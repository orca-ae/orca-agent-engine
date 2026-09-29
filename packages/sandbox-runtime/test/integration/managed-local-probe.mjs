// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Run explicitly inside the Linux Environment image (including gVisor in CI).
// node packages/sandbox-runtime/test/integration/managed-local-probe.mjs
// ORCA_SANDBOX_RUNTIME_ENTRY optionally selects the deployed package entry.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdir, mkdtemp, readFile, readlink, rm, stat, statfs, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

const {
  LocalSandboxRuntime,
  asLocalSandboxHandle,
  buildSandboxWritePolicy,
  createManagedToolSandboxManager,
  createPolicyEnforcedSandbox,
} = await import(
  process.env.ORCA_SANDBOX_RUNTIME_ENTRY ?? new URL('../../dist/index.js', import.meta.url).href
);

assert.equal(process.platform, 'linux', 'real managed isolation probe requires Linux');
const workerPidNamespace = await readlink('/proc/self/ns/pid');
const workDir = process.env.ORCA_MANAGED_PROBE_WORK_DIR ?? process.cwd();
const base = await mkdtemp(join(workDir, 'orca-managed-isolation-'));
const marker = `private-worker-${randomUUID()}`;
const privateState = join(base, 'worker-home/history/checkpoint');
await mkdir(join(base, 'worker-home/history'), { recursive: true });
await writeFile(privateState, marker);
process.env.ORCA_PRIVATE_WORKER_PROBE = marker;
const server = createServer((_request, response) => response.end('allowed-test-origin'));
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
const port = server.address().port;
const handles = [];
const quote = (value) => `'${value.replace(/'/g, `'"'"'`)}'`;

async function acquire(domains, unrestricted = false, directory = base) {
  const manager = createManagedToolSandboxManager();
  let raceDirectory;
  const runtime = new LocalSandboxRuntime({
    harnessWorkDir: join(directory, 'tool-handles'),
    allowedNetworkHosts: domains,
    networkUnrestricted: unrestricted,
    managedToolFilesystem: true,
    manager: {
      ...manager,
      async wrapWithSandbox(command, shell, config) {
        if (raceDirectory && command.includes('const chunks = [];')) {
          const directory = raceDirectory;
          raceDirectory = undefined;
          // Instrument the actual production helper only to announce that its
          // component checks completed, then hold stdin while another managed
          // Bash call swaps the directory. The helper's final open is unchanged
          // and the real generated SRT profile must deny the redirected write.
          command = command.replace(
            'const chunks = [];',
            'fs.writeFileSync(target + ".ready", ""); const chunks = [];',
          );
          command = `{ attempts=0; while [ ! -L ${quote(directory)} ]; do attempts=$((attempts + 1)); [ "$attempts" -lt 1000 ] || exit 96; sleep 0.01; done; cat; } | { ${command}; }`;
        }
        return await manager.wrapWithSandbox(command, shell, config);
      },
    },
  });
  const raw = await runtime.acquire({});
  handles.push(raw);
  const root = asLocalSandboxHandle(raw).rootDir();
  for (const dir of [
    'mnt/session/outputs',
    'mnt/memory',
    'mnt/reference',
    'workspace/repo',
    'workspace/skills/demo',
  ]) {
    await mkdir(join(root, dir), { recursive: true });
  }
  await writeFile(join(root, 'mnt/input.txt'), 'input-content', { mode: 0o444 });
  await writeFile(join(root, 'mnt/memory/notes.txt'), 'memory-content');
  await writeFile(join(root, 'mnt/reference/notes.txt'), 'readonly-memory-content');
  await writeFile(join(root, 'workspace/repo/README.md'), 'git-content');
  await writeFile(join(root, 'workspace/skills/demo/SKILL.md'), 'skill-content');
  const policy = buildSandboxWritePolicy(
    [
      { path: '/mnt/input.txt', kind: 'file', access: 'read_only' },
      { path: '/mnt/memory', kind: 'memory_store', access: 'read_write' },
      { path: '/mnt/reference', kind: 'memory_store', access: 'read_only' },
      { path: '/workspace/repo', kind: 'github_repository', access: 'read_write' },
    ],
    { includeSkillsRoot: true, networkAllowedDomains: domains, networkUnrestricted: unrestricted },
  );
  return {
    root,
    tool: await createPolicyEnforcedSandbox(raw, policy),
    armFileRace: () => {
      raceDirectory = join(root, 'mnt/session/outputs/race');
    },
  };
}

async function bash(tool, command) {
  const result = await tool.run({ tool: 'bash', args: { command, timeout_ms: 20_000 } });
  assert.equal(result.exit_code, 0, `command failed: ${command}\n${result.stderr}`);
  return result.stdout ?? '';
}

try {
  if (process.env.ORCA_EXPECT_TMPFS_REJECTION === '1') {
    assert.equal((await statfs(tmpdir())).type, 0x01021994, 'negative probe requires tmpfs');
    const tmpfsBase = await mkdtemp(join(tmpdir(), 'orca-unsupported-fs-'));
    try {
      await assert.rejects(acquire([], false, tmpfsBase), /read-only metadata changes/);
      console.log('PASS: unsafe tmpfs rejected before model tools are exposed');
    } finally {
      await rm(tmpfsBase, { recursive: true, force: true });
    }
  }
  const { root, tool, armFileRace } = await acquire(['127.0.0.1']);
  await bash(
    tool,
    `set -eu
    test "$(id -u)" = 1000
    caps=0
    while read -r key value rest; do
      case "$key" in
        CapInh:|CapPrm:|CapEff:|CapBnd:|CapAmb:)
          [[ "$value" =~ ^0+$ ]] || { echo "unexpected $key $value" >&2; exit 1; }
          caps=$((caps + 1)) ;;
      esac
    done < /proc/self/status
    test "$caps" = 5
    # gVisor omits NoNewPrivs from /proc/status; setpriv queries PR_GET_NO_NEW_PRIVS.
    privileges=$(setpriv --dump)
    printf '%s\\n' "$privileges"
    [[ "$privileges" =~ no_new_privs:[[:space:]]+1 ]]`,
  );
  assert.equal(await bash(tool, 'cat /mnt/input.txt'), 'input-content');
  assert.equal(await bash(tool, 'cat /workspace/skills/demo/SKILL.md'), 'skill-content');
  await tool.files.write('/mnt/session/outputs/from-file-api', Buffer.from('shared-output'));
  assert.equal(await bash(tool, 'cat /mnt/session/outputs/from-file-api'), 'shared-output');
  await bash(
    tool,
    'printf updated-memory > /mnt/memory/notes.txt; printf updated-git > /workspace/repo/README.md',
  );
  assert.equal((await tool.files.read('/mnt/memory/notes.txt')).toString(), 'updated-memory');
  assert.equal((await tool.files.read('/workspace/repo/README.md')).toString(), 'updated-git');
  const glob = await tool.run({ tool: 'glob', args: { root: '/mnt', pattern: '*.txt' } });
  assert.deepEqual(glob.output, ['input.txt']);
  const grep = await tool.run({
    tool: 'grep',
    args: { root: '/workspace/repo', pattern: 'updated-git' },
  });
  assert.match(grep.output, /README.md:1:updated-git/);
  await bash(
    tool,
    `set -eu
if (printf forbidden > /mnt/input.txt) 2>/dev/null; then exit 91; fi
if (printf forbidden > /workspace/skills/demo/SKILL.md) 2>/dev/null; then exit 92; fi
# Assert the protected state, not chmod's exit status: a successful no-op
# does not grant writes. Use an actual numeric mode change and verify both
# the tool view and (below) the worker view after the command exits.
mode_before=$(stat -c %a /mnt/input.txt)
test "$mode_before" = 444
chmod 0644 /mnt/input.txt 2>/dev/null || true
mode_after=$(stat -c %a /mnt/input.txt)
[ "$mode_after" = "$mode_before" ] || { echo "read-only mode changed: $mode_before -> $mode_after" >&2; exit 93; }
if (printf forbidden-after-chmod > /mnt/input.txt) 2>/dev/null; then exit 99; fi
test ! -e ${quote(privateState)}
test "$(readlink /proc/self/ns/pid)" != ${quote(workerPidNamespace)}
# PID numbers can be reused inside the nested namespace (including PID 1).
if [ -e /proc/${process.pid}/ns/pid ]; then
  test "$(readlink /proc/${process.pid}/ns/pid)" != ${quote(workerPidNamespace)}
fi
test -z "\${ORCA_PRIVATE_WORKER_PROBE:-}"
test "$HOME" = /tmp/home
test "$TMPDIR" = /tmp
printf scratch > /tmp/scratch
test -f /tmp/scratch`,
  );
  await bash(tool, 'test ! -e /tmp/scratch');
  assert.equal((await stat(join(root, 'mnt/input.txt'))).mode & 0o777, 0o444);
  assert.equal(await readFile(join(root, 'mnt/input.txt'), 'utf8'), 'input-content');
  assert.equal(await readFile(privateState, 'utf8'), marker);
  for (const [relativeDirectory, name, protectedPath, original] of [
    ['../../../mnt', 'input.txt', 'mnt/input.txt', 'input-content'],
    ['../../../mnt/reference', 'notes.txt', 'mnt/reference/notes.txt', 'readonly-memory-content'],
    [
      '../../../workspace/skills/demo',
      'SKILL.md',
      'workspace/skills/demo/SKILL.md',
      'skill-content',
    ],
  ]) {
    armFileRace();
    const result = tool.files
      .write(`/mnt/session/outputs/race/${name}`, Buffer.from('forbidden'))
      .then(
        () => ({ succeeded: true }),
        (error) => ({ succeeded: false, error }),
      );
    await bash(
      tool,
      `set -eu
attempts=0
while [ ! -f ${quote(`/mnt/session/outputs/race/${name}.ready`)} ]; do
  attempts=$((attempts + 1))
  [ "$attempts" -lt 1000 ] || exit 97
  sleep 0.01
done
mv /mnt/session/outputs/race /mnt/session/outputs/race-original
ln -s ${quote(relativeDirectory)} /mnt/session/outputs/race`,
    );
    const writeResult = await result;
    assert.equal(writeResult.succeeded, false, `raced write changed ${protectedPath}`);
    assert.match(writeResult.error.message, /EROFS|EACCES|[Rr]ead.only|[Pp]ermission denied/);
    assert.equal(await readFile(join(root, protectedPath), 'utf8'), original);
    await bash(tool, 'rm -rf /mnt/session/outputs/race /mnt/session/outputs/race-original');
  }
  assert.equal(
    await bash(tool, `NO_PROXY= no_proxy= curl -fsS --max-time 5 http://127.0.0.1:${port}`),
    'allowed-test-origin',
  );

  const unrestricted = await acquire([], true);
  assert.equal(
    await bash(unrestricted.tool, `curl --noproxy '*' -fsS --max-time 5 http://127.0.0.1:${port}`),
    'allowed-test-origin',
  );
  await bash(
    unrestricted.tool,
    'if (printf forbidden > /mnt/input.txt) 2>/dev/null; then exit 98; fi',
  );

  // The same process creates a second runtime with an empty host grant. It
  // must not inherit the first SRT proxy's domain list or the host loopback.
  const denied = await acquire([]);
  await bash(
    denied.tool,
    `if NO_PROXY= no_proxy= curl -fsS --max-time 5 http://127.0.0.1:${port}; then exit 94; fi`,
  );
  await bash(
    denied.tool,
    `if curl --noproxy '*' -fsS --max-time 2 http://127.0.0.1:${port}; then exit 95; fi`,
  );
  console.log(
    'PASS: unprivileged model tools, mapped filesystem, read-only policy, private worker state, and isolated SRT proxy grants',
  );
} finally {
  delete process.env.ORCA_PRIVATE_WORKER_PROBE;
  await Promise.allSettled(handles.map((handle) => handle.destroy()));
  await new Promise((resolve) => server.close(resolve));
  await rm(base, { recursive: true, force: true });
}
