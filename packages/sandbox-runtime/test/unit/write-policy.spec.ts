// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Mirrors services/harness-server/test/unit/write-policy.spec.ts, updated for
// this package's contract changes: a read_write file mount now throws instead
// of demoting to read-only.
import { mkdirSync, symlinkSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { asInMemoryHandle, InMemorySandboxRuntime } from '../../src/in-memory/runtime.js';
import {
  buildBubblewrapCommand,
  buildMappedToolBubblewrapCommand,
  buildSandboxFilesystemRootPreflightCommand,
  buildSandboxWritePolicy,
  buildSkillsAliasProbeCommand,
  createPolicyEnforcedSandbox,
  networkAllowedDomainsFromUrls,
  normalizeWritePolicyAccess,
  OUTPUT_WRITABLE_PATH,
  validateSandboxWritePolicy,
  readablePathFor,
  writablePathFor,
} from '../../src/write-policy.js';
import {
  hasWritePolicyEnforcement,
  type SandboxHandle,
  type WritePolicyCapableHandle,
} from '../../src/sandbox-runtime.js';

/** Narrow an acquired handle through the exported guard (compile-time API). */
function asWritePolicyCapable(handle: SandboxHandle): WritePolicyCapableHandle {
  if (!hasWritePolicyEnforcement(handle)) {
    throw new Error('runtime does not implement write-policy enforcement');
  }
  return handle;
}

describe('sandbox write policy', () => {
  it('maps only the private tool root and runtime bindings while retaining SRT network proxies', () => {
    const policy = buildSandboxWritePolicy(
      [
        { path: '/mnt/input.txt', kind: 'file', access: 'read_only' },
        { path: '/mnt/memory', kind: 'memory_store', access: 'read_write' },
        { path: '/workspace/repo', kind: 'github_repository', access: 'read_only' },
      ],
      { includeSkillsRoot: true },
    );
    const command = buildMappedToolBubblewrapCommand(
      'cat /mnt/input.txt',
      policy,
      '/private/tools/session',
      [
        { source: '/usr/bin', target: '/bin' },
        { source: '/usr/lib', target: '/lib' },
      ],
    );
    expect(command).toContain("'--ro-bind' '/private/tools/session' '/'");
    expect(command).toContain("'--ro-bind' '/usr/bin' '/bin'");
    expect(command).toContain("'--bind' '/private/tools/session/mnt/memory' '/mnt/memory'");
    expect(command).toContain(
      "'--bind' '/private/tools/session/mnt/session/outputs' '/mnt/session/outputs'",
    );
    expect(command).toContain("'--unshare-user' '--unshare-pid'");
    expect(command).toContain("'--cap-drop' 'ALL'");
    expect(command).toContain("'--tmpfs' '/tmp'");
    expect(command).toContain("'--dev' '/dev' '--proc' '/proc'");
    expect(command).toContain("'--chdir' '/mnt/session/outputs'");
    expect(command).toContain("'cat /mnt/input.txt'");
    expect(command).not.toContain("'--ro-bind' '/' '/'");
    expect(command).not.toContain('--unshare-net');
    expect(command).not.toContain('--clearenv');
    expect(command).not.toContain('HTTP_PROXY');
    expect(command).not.toContain("'--bind' '/private/tools/session/workspace/skills'");
    expect(command).not.toContain('/private/worker');
    expect(command).not.toContain('--dev-bind');
  });

  it.each(['/', '/usr', '/usr/bin', '/tmp', '/proc', '/etc/ssl'])(
    'rejects resource aliases at reserved runtime path %s',
    (root) => {
      expect(() =>
        buildMappedToolBubblewrapCommand(
          'true',
          buildSandboxWritePolicy([{ path: root, kind: 'file', access: 'read_only' }]),
          '/private/tools',
          [],
        ),
      ).toThrow();
    },
  );

  it('rejects writable Skill ancestors and a host-root binding', () => {
    expect(() =>
      buildMappedToolBubblewrapCommand(
        'true',
        buildSandboxWritePolicy([
          { path: '/workspace', kind: 'memory_store', access: 'read_write' },
        ]),
        '/private/tools',
        [],
      ),
    ).toThrow(/Skills writable/);
    expect(() =>
      buildMappedToolBubblewrapCommand('true', buildSandboxWritePolicy([]), '/', []),
    ).toThrow(/private root/);
    expect(() =>
      buildMappedToolBubblewrapCommand('true', buildSandboxWritePolicy([]), '/private/tools', [
        { source: '/', target: '/usr' },
      ]),
    ).toThrow(/non-root paths/);
  });
  it('rejects a read_write file mount instead of silently demoting it', () => {
    // The mount union makes this unrepresentable in TS; the runtime guard
    // covers untyped (wire-decoded) callers. Silent demotion would deny the
    // writes the caller asked for with no signal.
    expect(() =>
      buildSandboxWritePolicy([
        { path: '/mnt/inputs/input.txt', kind: 'file', access: 'read_write' },
      ] as never),
    ).toThrow(/cannot grant read_write to a file mount/);
  });

  it('rejects an UNKNOWN read_write mount kind — the allowlist must fail closed', () => {
    // A denylist here failed OPEN — a wire-decoded
    // {kind:'directory', access:'read_write'} became a writable root and a
    // bubblewrap --bind. Only memory_store/github_repository may be writable.
    expect(() =>
      buildSandboxWritePolicy([
        { path: '/mnt/inputs', kind: 'directory', access: 'read_write' },
      ] as never),
    ).toThrow(/cannot grant read_write to a directory mount/);
    expect(() =>
      validateSandboxWritePolicy({
        writablePaths: [{ path: '/mnt/x', kind: 'directory' as never }],
        readonlyPaths: [],
      }),
    ).toThrow(/unknown writable kind/);
  });

  it('allows outputs and only explicitly writable memory/repository mounts', () => {
    const policy = buildSandboxWritePolicy(
      [
        { path: '/mnt/memory/notes', kind: 'memory_store', access: 'read_write' },
        { path: '/workspace/repo', kind: 'github_repository', access: 'read_write' },
        { path: '/mnt/inputs/input.txt', kind: 'file', access: 'read_only' },
        { path: '/mnt/memory/reference', kind: 'memory_store', access: 'read_only' },
      ],
      {
        includeSkillsRoot: true,
        networkAllowedDomains: networkAllowedDomainsFromUrls([
          'https://github.com/orca-ae/orca-agent-engine.git',
          'https://registry.example.com/v1/git-creds',
          'https://github.com/duplicate',
        ]),
      },
    );

    expect(policy.writablePaths).toEqual([
      { path: '/mnt/memory/notes', kind: 'memory_store' },
      { path: OUTPUT_WRITABLE_PATH, kind: 'session_output' },
      { path: '/workspace/repo', kind: 'github_repository' },
    ]);
    expect(policy.readonlyPaths).toEqual([
      '/mnt/inputs/input.txt',
      '/mnt/memory/reference',
      '/workspace/skills',
    ]);
    expect(policy.networkAllowedDomains).toEqual(['github.com', 'registry.example.com']);
    expect(writablePathFor(policy, '/mnt/session/outputs/poem.txt')?.kind).toBe('session_output');
    expect(writablePathFor(policy, '/mnt/session/outputs-old/poem.txt')).toBeUndefined();
    expect(readablePathFor(policy, '/mnt/inputs/input.txt')).toBe('/mnt/inputs/input.txt');
    expect(readablePathFor(policy, '/workspace/repo/src/index.ts')).toBe('/workspace/repo');
    expect(readablePathFor(policy, '/proc/self/environ')).toBeUndefined();
  });

  it('omits the reserved Skill root unless a non-empty catalog is explicit', () => {
    const policy = buildSandboxWritePolicy([]);
    expect(policy.readonlyPaths).toEqual([]);
  });
  it('exposes scoped Git capabilities as a read-only root and rejects writable overlap', () => {
    const policy = buildSandboxWritePolicy([], { includeGitProxyRoot: true });
    expect(readablePathFor(policy, '/.orca/git/sesrsc_git.config')).toBe('/.orca/git');
    expect(writablePathFor(policy, '/.orca/git/sesrsc_git.config')).toBeUndefined();
    expect(() =>
      buildSandboxWritePolicy(
        [{ path: '/.orca', kind: 'github_repository', access: 'read_write' }],
        { includeGitProxyRoot: true },
      ),
    ).toThrow(/overlapping/);
  });

  it('rejects malformed network endpoints and domain entries', () => {
    expect(() => networkAllowedDomainsFromUrls(['ssh://github.com/orca/repo'])).toThrow(
      /must use http or https/,
    );
    expect(() =>
      buildSandboxWritePolicy([], { networkAllowedDomains: ['github.com/path'] }),
    ).toThrow(/domain is invalid/);
  });

  it('rejects ambiguous writable/read-only overlaps', () => {
    expect(() =>
      buildSandboxWritePolicy([
        { path: '/workspace/repo', kind: 'github_repository', access: 'read_write' },
        { path: '/workspace/repo/vendor', kind: 'file', access: 'read_only' },
      ]),
    ).toThrow(/overlapping/);
    expect(() =>
      buildSandboxWritePolicy([
        { path: '/mnt/session', kind: 'memory_store', access: 'read_write' },
      ]),
    ).toThrow(/overlapping writable roots/);
  });

  it('fails closed when a mounted resource has no pinned access', () => {
    expect(normalizeWritePolicyAccess('read_write')).toBe('read_write');
    expect(normalizeWritePolicyAccess('read_only')).toBe('read_only');
    expect(normalizeWritePolicyAccess(undefined)).toBe('read_only');
    expect(normalizeWritePolicyAccess('unexpected')).toBe('read_only');
  });

  it('guards direct writes, traversal, prefix collisions, and symlink escapes', async () => {
    const runtime = new InMemorySandboxRuntime();
    const raw = await runtime.acquire({});
    const denied = vi.fn();
    const policy = buildSandboxWritePolicy([]);
    const sandbox = await createPolicyEnforcedSandbox(asWritePolicyCapable(raw), policy, denied);
    try {
      await sandbox.files.write('/mnt/session/outputs/poem.txt', Buffer.from('allowed'));
      await expect(raw.files.read('/mnt/session/outputs/poem.txt')).resolves.toEqual(
        Buffer.from('allowed'),
      );

      await expect(
        sandbox.files.write('/mnt/ai_coding_poem.txt', Buffer.from('denied')),
      ).rejects.toThrow(/write denied/);
      await expect(
        sandbox.files.write('/mnt/session/outputs-old/poem.txt', Buffer.from('denied')),
      ).rejects.toThrow(/write denied/);
      await expect(
        sandbox.files.write('/mnt/session/outputs/../../poem.txt', Buffer.from('denied')),
      ).rejects.toThrow(/write denied/);
      await expect(sandbox.files.read('/proc/1/environ')).rejects.toThrow(/read denied/);
      await expect(sandbox.files.list('/dev')).rejects.toThrow(/read denied/);

      const root = asInMemoryHandle(raw).rootDir();
      mkdirSync(join(root, 'mnt/session/outputs'), { recursive: true });
      symlinkSync('/tmp', join(root, 'mnt/session/outputs/escape'));
      await expect(
        sandbox.files.write('/mnt/session/outputs/escape/poem.txt', Buffer.from('denied')),
      ).rejects.toThrow(/write denied/);
      expect(denied).toHaveBeenCalledTimes(4);
    } finally {
      await sandbox.destroy();
    }
  });

  it('reports a canonicalizer infra failure as such — never as a policy denial', async () => {
    // On the cloud runtimes canonicalizePathForPolicy is a REMOTE realpath; a
    // transport failure reclassified as a denial would discard the real error
    // and pollute the onDenied audit with violations that never happened.
    const denied = vi.fn();
    const raw = asWritePolicyCapable(await new InMemorySandboxRuntime().acquire({}));
    const policy = buildSandboxWritePolicy([]);
    const sandbox = await createPolicyEnforcedSandbox(raw, policy, denied);
    try {
      raw.canonicalizePathForPolicy = vi.fn().mockRejectedValue(new Error('execd 500'));
      await expect(
        sandbox.files.write('/mnt/session/outputs/poem.txt', Buffer.from('x')),
      ).rejects.toThrow(/canonicalization failed/);
      await expect(sandbox.files.read('/mnt/session/outputs/poem.txt')).rejects.toThrow(
        /canonicalization failed/,
      );
      // The audit stays clean: infra failures are not agent policy violations.
      expect(denied).not.toHaveBeenCalled();
    } finally {
      await sandbox.destroy();
    }
  });

  it('validateSandboxWritePolicy is the only gate to the branded type', () => {
    // Hand-rolled policies must pass the same invariants as builder output:
    // non-canonical roots would silently break containment
    // matching and bind unvetted paths into the bubblewrap namespace.
    expect(() =>
      validateSandboxWritePolicy({ writablePaths: [], readonlyPaths: ['/mnt/../etc'] }),
    ).toThrow(/not canonical/);
    expect(() =>
      validateSandboxWritePolicy({
        writablePaths: [{ path: '/mnt/data', kind: 'memory_store' }],
        readonlyPaths: ['/mnt/data/sub'],
      }),
    ).toThrow(/overlapping/);
    const branded = validateSandboxWritePolicy({
      writablePaths: [{ path: '/mnt/data', kind: 'memory_store' }],
      readonlyPaths: ['/workspace/skills'],
    });
    expect(Object.isFrozen(branded)).toBe(true);
    expect(writablePathFor(branded, '/mnt/data/notes.md')?.kind).toBe('memory_store');
  });

  it('limits reads and directory listings to mounted session roots', async () => {
    const runtime = new InMemorySandboxRuntime();
    const raw = await runtime.acquire({});
    const policy = buildSandboxWritePolicy([
      { path: '/mnt/inputs/input.txt', kind: 'file', access: 'read_only' },
      { path: '/workspace/repo', kind: 'github_repository', access: 'read_only' },
    ]);
    await raw.files.write('/mnt/inputs/input.txt', Buffer.from('allowed'));
    await raw.files.write('/workspace/repo/README.md', Buffer.from('repository'));
    await raw.files.write('/outside/secret.txt', Buffer.from('secret'));
    const sandbox = await createPolicyEnforcedSandbox(asWritePolicyCapable(raw), policy);
    try {
      await expect(sandbox.files.read('/mnt/inputs/input.txt')).resolves.toEqual(
        Buffer.from('allowed'),
      );
      await expect(sandbox.files.list('/workspace/repo')).resolves.toContain('README.md');
      await expect(sandbox.files.read('/outside/secret.txt')).rejects.toThrow(/read denied/);
      await expect(sandbox.files.read('/proc/self/environ')).rejects.toThrow(/read denied/);
      await expect(sandbox.files.list('/outside')).rejects.toThrow(/read denied/);
    } finally {
      await sandbox.destroy();
    }
  });

  it('routes paged reads through one policy-constrained runtime primitive', async () => {
    const raw = await new InMemorySandboxRuntime().acquire({});
    const policy = buildSandboxWritePolicy([
      { path: '/mnt/inputs/input.txt', kind: 'file', access: 'read_only' },
    ]);
    await raw.files.write('/mnt/inputs/input.txt', Buffer.from('allowed'));
    await raw.files.write('/outside/secret.txt', Buffer.from('secret'));
    const sandbox = await createPolicyEnforcedSandbox(asWritePolicyCapable(raw), policy);
    const wholeRead = vi.spyOn(raw.files, 'read').mockRejectedValue(new Error('whole read used'));
    const canonicalize = vi.fn().mockRejectedValue(new Error('legacy check used'));
    raw.canonicalizePathForPolicy = canonicalize;
    const atomicRead = vi.spyOn(raw.files, 'readUtf8Page');
    try {
      await expect(sandbox.files.readUtf8Page('/mnt/inputs/input.txt', {})).resolves.toMatchObject({
        content: 'allowed',
      });
      expect(atomicRead).toHaveBeenCalledWith(
        '/mnt/inputs/input.txt',
        {},
        expect.objectContaining({
          readableRoots: expect.arrayContaining(['/mnt/inputs/input.txt']),
        }),
      );
      expect(wholeRead).not.toHaveBeenCalled();
      expect(canonicalize).not.toHaveBeenCalled();

      await expect(sandbox.files.readUtf8Page('/outside/secret.txt', {})).rejects.toThrow(
        /outside session resource roots/,
      );
      expect(atomicRead).toHaveBeenCalledTimes(1);
    } finally {
      await sandbox.destroy();
    }
  });

  it('denies reads whose canonical target resolves into a control filesystem', async () => {
    const raw = await new InMemorySandboxRuntime().acquire({});
    const originalCanonicalize = raw.canonicalizePathForPolicy!.bind(raw);
    raw.canonicalizePathForPolicy = async (pathValue) =>
      pathValue === '/workspace/proc-link'
        ? '/proc/1/environ'
        : await originalCanonicalize(pathValue);
    // The policy deliberately makes /proc/1 a READABLE root: with the target
    // inside a readable root, the only check that can reject the read is the
    // control-filesystem denied-roots guard — deleting that guard fails this
    // test, whereas the previous setup (target outside every root) rejected
    // for the wrong reason and the guard was unfalsifiable.
    const policy = buildSandboxWritePolicy([
      { path: '/proc/1', kind: 'file', access: 'read_only' },
      { path: '/workspace/proc-link', kind: 'file', access: 'read_only' },
    ]);
    expect(readablePathFor(policy, '/proc/1/environ')).toBeDefined();
    const sandbox = await createPolicyEnforcedSandbox(asWritePolicyCapable(raw), policy);
    try {
      await expect(sandbox.files.read('/workspace/proc-link')).rejects.toThrow(/read denied/);
    } finally {
      await sandbox.destroy();
    }
  });

  it('denies Bash in the test runtime instead of pretending shell parsing is isolation', async () => {
    const raw = await new InMemorySandboxRuntime().acquire({});
    const sandbox = await createPolicyEnforcedSandbox(
      asWritePolicyCapable(raw),
      buildSandboxWritePolicy([]),
    );
    try {
      await expect(
        sandbox.run({ tool: 'bash', args: { command: 'touch /mnt/forbidden' } }),
      ).resolves.toMatchObject({ exit_code: 126 });
    } finally {
      await sandbox.destroy();
    }
  });

  it('builds a read-only root with narrow writable mounts and isolated scratch', () => {
    const command = buildBubblewrapCommand(
      'printf ok > poem.txt',
      buildSandboxWritePolicy([
        { path: '/mnt/memory/notes', kind: 'memory_store', access: 'read_write' },
      ]),
    );
    expect(command).toContain("'--ro-bind' '/' '/'");
    expect(command).toContain("'--bind' '/mnt/session/outputs' '/mnt/session/outputs'");
    expect(command).toContain("'--bind' '/mnt/memory/notes' '/mnt/memory/notes'");
    expect(command).toContain("'--perms' '1777' '--tmpfs' '/tmp'");
    expect(command).toContain("'--perms' '0700' '--dir' '/tmp/home'");
    expect(command).toContain("'--new-session'");
    expect(command).toContain("'--unshare-user'");
    expect(command).toContain("'--unshare-pid'");
    expect(command).toContain("'--uid' '1000' '--gid' '1000' '--cap-drop' 'ALL'");
    expect(command).toContain("'--dev' '/dev'");
    expect(command).not.toContain("'--dev-bind'");
    expect(command).toContain("'--chdir' '/mnt/session/outputs'");
    expect(command).toContain("'--setenv' 'HOME' '/tmp/home'");
    expect(command).toContain("'--setenv' 'TMPDIR' '/tmp'");
    expect(command).not.toContain("'setpriv'");
  });

  it('builds a Linux probe for Skill aliases and nested resource mounts', () => {
    const command = buildSkillsAliasProbeCommand(
      buildSandboxWritePolicy([
        { path: '/workspace/repo', kind: 'github_repository', access: 'read_write' },
        { path: '/mnt/inputs/reference', kind: 'file', access: 'read_only' },
      ]),
    );
    expect(command).toContain("stat -Lc '");
    expect(command).toContain('/proc/self/mountinfo');
    expect(command).toContain("'/workspace/skills'");
    expect(command).toContain("'/mnt/session/outputs'");
    expect(command).toContain("'/workspace/repo'");
    expect(command).toContain("'/mnt/inputs/reference'");
    expect(command).toContain('aliases reserved Skill root');
    expect(command).toContain('contains nested mount');
  });

  it('builds a pre-materialization probe for exact mounts and symlink aliases', () => {
    const command = buildSandboxFilesystemRootPreflightCommand([
      '/workspace/skills',
      '/mnt/session/outputs',
      '/mnt/inputs/reference',
      '/mnt/inputs/reference',
    ]);
    expect(command).toContain('realpath -m');
    expect(command).toContain('/proc/self/mountinfo');
    expect(command).toContain('contains a pre-existing mount');
    expect(command.split("'/mnt/inputs/reference'")).toHaveLength(2);
  });
});
