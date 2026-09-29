// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  LocalSandboxRuntime,
  asLocalSandboxHandle,
  type SandboxManagerInitConfig,
  type SandboxManagerLike,
} from '../../src/local/runtime.js';
import { buildSandboxWritePolicy } from '../../src/write-policy.js';

/**
 * In-process fake of the upstream `SandboxManager`. Records the init config
 * + every `wrapWithSandbox` call so the tests can assert the runtime built
 * the right sandbox profile and prepended the right wrapper.
 *
 * The fake's `wrapWithSandbox` returns the command verbatim, so the
 * subsequent `child_process.spawn` runs the bare command — that's how we get
 * to assert the runtime's stdout/stderr/exit_code path without needing a
 * real `srt` binary.
 */
class FakeSandboxManager implements SandboxManagerLike {
  initialized = 0;
  initConfig: SandboxManagerInitConfig | undefined;
  wrapped: Array<{
    command: string;
    binShell?: string;
    customConfig?: Partial<SandboxManagerInitConfig>;
  }> = [];

  async initialize(config: SandboxManagerInitConfig): Promise<void> {
    this.initialized += 1;
    this.initConfig = config;
  }

  async wrapWithSandbox(
    command: string,
    binShell?: string,
    customConfig?: Partial<SandboxManagerInitConfig>,
  ): Promise<string> {
    const entry: {
      command: string;
      binShell?: string;
      customConfig?: Partial<SandboxManagerInitConfig>;
    } = { command };
    if (binShell !== undefined) entry.binShell = binShell;
    if (customConfig !== undefined) entry.customConfig = customConfig;
    this.wrapped.push(entry);
    // Return the command verbatim — child_process.spawn will run it as-is.
    return command;
  }
}

describe('LocalSandboxRuntime', () => {
  let baseDir: string;
  let manager: FakeSandboxManager;

  beforeEach(() => {
    baseDir = mkdtempSync(join(tmpdir(), 'orca-local-rt-'));
    manager = new FakeSandboxManager();
  });

  afterEach(() => {
    rmSync(baseDir, { recursive: true, force: true });
  });

  it('advertises supportsFuse=false', () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: ['api.anthropic.com'],
      manager,
    });
    expect(rt.capabilities.supportsFuse).toBe(false);
    expect(rt.capabilities.supportsLocalMemory).toBe(true);
    expect(rt.capabilities.supportsWritePolicy).toBe(true);
  });

  it('accepts filesystem-root preparation inside its private workdir', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      await expect(
        sb.prepareFilesystemRoots!(['/mnt/inputs', '/workspace/skills']),
      ).resolves.toBeUndefined();
      expect(manager.wrapped).toHaveLength(0);
    } finally {
      await sb.destroy();
    }
  });

  it('creates fresh private workdirs and scratch dirs with mode 0700', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const first = await rt.acquire({});
    const second = await rt.acquire({});
    try {
      const firstRoot = asLocalSandboxHandle(first).rootDir();
      const secondRoot = asLocalSandboxHandle(second).rootDir();
      expect(firstRoot).not.toBe(secondRoot);
      expect(statSync(firstRoot).mode & 0o777).toBe(0o700);
      expect(statSync(join(firstRoot, 'tmp')).mode & 0o777).toBe(0o700);
      expect(statSync(join(baseDir, 'sessions')).mode & 0o777).toBe(0o700);
    } finally {
      await first.destroy();
      await second.destroy();
    }
  });

  it('rejects a symlinked sessions directory before initializing the sandbox', async () => {
    const target = join(baseDir, 'redirected-sessions');
    mkdirSync(target);
    symlinkSync(target, join(baseDir, 'sessions'), 'dir');
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });

    await expect(rt.acquire({})).rejects.toThrow(/not a real directory/);
    expect(manager.initialized).toBe(0);
  });

  it('narrows the agent subprocess profile to policy writable roots', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      await sb.files.write('/mnt/session/outputs/.keep', Buffer.alloc(0));
      const policy = buildSandboxWritePolicy([
        { path: '/mnt/memory/notes', kind: 'memory_store', access: 'read_write' },
      ]);
      await sb.prepareWritePolicy!(policy);

      const wrapped = manager.wrapped.at(-1);
      const config = wrapped?.customConfig as SandboxManagerInitConfig;
      const root = realpathSync(asLocalSandboxHandle(sb).rootDir());
      expect(config.filesystem.allowWrite).toEqual([
        join(root, 'tmp'),
        join(root, 'mnt', 'memory', 'notes'),
        join(root, 'mnt', 'session', 'outputs'),
      ]);
      expect(config.filesystem.allowWrite).not.toContain(root);
      expect(config.filesystem.allowRead).toEqual(
        expect.arrayContaining([root, realpathSync('/bin'), realpathSync('/usr')]),
      );

      const result = await sb.runWithWritePolicy!(
        { tool: 'bash', args: { command: 'printf %s "$TMPDIR"' } },
        policy,
      );
      expect(result.stdout).toBe(join(root, 'tmp'));
    } finally {
      await sb.destroy();
    }
  });

  it('initialize is called exactly once across multiple acquires', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: ['api.anthropic.com'],
      manager,
    });
    const sb1 = await rt.acquire({});
    const sb2 = await rt.acquire({});
    expect(manager.initialized).toBe(1);
    await sb1.destroy();
    await sb2.destroy();
  });

  it('builds a SandboxManagerInitConfig with the locked allow-list shape', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: ['api.anthropic.com', 'mcp.test', 'minio.test'],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      const cfg = manager.initConfig;
      expect(cfg).toBeDefined();
      expect(cfg!.network.allowedDomains).toEqual(['api.anthropic.com', 'mcp.test', 'minio.test']);
      expect(cfg!.network.deniedDomains).toEqual([]);
      expect(cfg!.network.allowLocalBinding).toBe(true);
      // Per-session work-dir + tmp/ are both writable; system /tmp is NOT
      // auto-allowed (the locked design swaps it for the session-scoped tmp).
      expect(cfg!.filesystem.allowWrite.length).toBe(2);
      expect(cfg!.filesystem.allowWrite[0]).toMatch(/sessions\//);
      expect(cfg!.filesystem.allowWrite[1]).toMatch(/sessions\/.*\/tmp$/);
      // Reads default-deny the host root, then re-open only the session and
      // the minimal OS runtime needed by shell commands.
      expect(cfg!.filesystem.allowRead).toEqual(
        expect.arrayContaining([realpathSync('/bin'), realpathSync('/usr')]),
      );
      const seccomp = cfg!.seccomp as { applyPath?: string };
      expect(seccomp.applyPath).toMatch(
        new RegExp(`vendor/seccomp/${process.arch}/apply-seccomp$`),
      );
      expect(cfg!.filesystem.allowRead).toContain(seccomp.applyPath);
      expect(cfg!.filesystem.denyRead).toEqual(['/', '~/.ssh', '~/.aws', '~/.config/gcloud']);
      expect(cfg!.filesystem.denyWrite).toEqual([
        '/tmp/claude',
        '/private/tmp/claude',
        '~/.npm/_logs',
        '~/.claude/debug',
      ]);
    } finally {
      await sb.destroy();
    }
  });

  it('respects custom extraReadPaths and extraDenyReadPaths', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      extraReadPaths: ['/opt/custom'],
      extraDenyReadPaths: ['~/private'],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      const cfg = manager.initConfig!;
      expect(cfg.filesystem.allowRead).toEqual(expect.arrayContaining(['/opt/custom']));
      expect(cfg.filesystem.allowRead).toContain('/usr');
      expect(cfg.filesystem.denyRead).toEqual(['/', '~/private']);

      await sb.files.write('/mnt/session/outputs/.keep', Buffer.alloc(0));
      await sb.prepareWritePolicy!(buildSandboxWritePolicy([]));
      const policyConfig = manager.wrapped.at(-1)?.customConfig as SandboxManagerInitConfig;
      expect(policyConfig.filesystem.allowRead).toEqual(expect.arrayContaining(['/opt/custom']));
      expect(policyConfig.filesystem.allowWrite).not.toContain('/opt/custom');
    } finally {
      await sb.destroy();
    }
  });

  it('acquire creates the per-session work-dir under harnessWorkDir', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      const root = realpathSync(asLocalSandboxHandle(sb).rootDir());
      expect(root.startsWith(join(realpathSync(baseDir), 'sessions'))).toBe(true);
      expect(existsSync(root)).toBe(true);
    } finally {
      await sb.destroy();
    }
  });

  it('destroy rm -rfs the per-session work-dir', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    const root = asLocalSandboxHandle(sb).rootDir();
    await sb.destroy();
    expect(existsSync(root)).toBe(false);
  });

  it('destroy is idempotent', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    await sb.destroy();
    await expect(sb.destroy()).resolves.toBeUndefined();
  });

  it('files.write + files.read round-trip bytes through the host work-dir', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      await sb.files.write('/data/hello.txt', Buffer.from('hi'));
      const back = await sb.files.read('/data/hello.txt');
      expect(back.toString('utf8')).toBe('hi');
      // The bytes really live on the host FS under the work-dir root.
      const root = asLocalSandboxHandle(sb).rootDir();
      expect(existsSync(join(root, 'data', 'hello.txt'))).toBe(true);
      await sb.files.chmod('/data/hello.txt', 0o444);
      expect(statSync(join(root, 'data', 'hello.txt')).mode & 0o777).toBe(0o444);
    } finally {
      await sb.destroy();
    }
  });

  it('runs every host file operation in a strict per-session sandbox', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: ['should-not-reach-file-helper.test'],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      await sb.files.write('/data/hello.bin', Buffer.from([0, 255, 1]));
      expect(await sb.files.read('/data/hello.bin')).toEqual(Buffer.from([0, 255, 1]));
      expect(await sb.files.list('/data')).toEqual(['hello.bin']);
      await sb.files.delete('/data/hello.bin');

      expect(manager.wrapped).toHaveLength(4);
      const root = realpathSync(asLocalSandboxHandle(sb).rootDir());
      for (const wrapped of manager.wrapped) {
        const config = wrapped.customConfig as SandboxManagerInitConfig;
        expect(config.network.allowedDomains).toEqual([]);
        expect(config.filesystem.denyRead).toEqual(['/']);
        expect(config.filesystem.allowRead).toContain(root);
        expect(config.filesystem.allowWrite).toEqual([root]);
        expect(config.filesystem.denyWrite).toContain('/tmp/claude');
      }
    } finally {
      await sb.destroy();
    }
  });

  it('rejects deleting the sandbox root and paths containing NUL bytes', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      await expect(sb.files.delete('/')).rejects.toThrow(/refusing to delete the sandbox root/);
      await expect(sb.files.read('/bad\0path')).rejects.toThrow(/NUL byte/);
    } finally {
      await sb.destroy();
    }
  });

  it('runs bash through wrapWithSandbox and reports stdout + exit code', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      const r = await sb.run({ tool: 'bash', args: { command: 'echo hello' } });
      expect(r.stdout?.trim()).toBe('hello');
      expect(r.exit_code).toBe(0);
      // The runtime DID call wrapWithSandbox, not spawn directly.
      expect(manager.wrapped.length).toBe(1);
      expect(manager.wrapped[0]?.command).toMatch(/export HOME=.*; echo hello$/);
    } finally {
      await sb.destroy();
    }
  });

  it('does not expose harness host secrets to agent subprocesses', async () => {
    const secretKey = 'ORCA_TEST_HOST_SECRET';
    const previous = process.env[secretKey];
    process.env[secretKey] = 'must-not-reach-agent';
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      const root = realpathSync(asLocalSandboxHandle(sb).rootDir());
      const r = await sb.run({
        tool: 'bash',
        args: {
          command: `printf '%s\n%s\n%s\n' "\${${secretKey}-unset}" "$HOME" "$TMPDIR"`,
        },
      });

      expect(r.exit_code).toBe(0);
      expect(r.stdout?.split('\n').slice(0, 3)).toEqual(['unset', root, join(root, 'tmp')]);
    } finally {
      await sb.destroy();
      if (previous === undefined) delete process.env[secretKey];
      else process.env[secretKey] = previous;
    }
  });

  it('reports non-zero exit codes', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      const r = await sb.run({ tool: 'bash', args: { command: 'exit 7' } });
      expect(r.exit_code).toBe(7);
    } finally {
      await sb.destroy();
    }
  });

  it('runPrivileged is absent — the local runtime has no privilege boundary', async () => {
    // `srt` blocks sudo and the FUSE strategies are never selected against
    // this runtime; absence of the optional method is the feature signal.
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      expect(sb.runPrivileged).toBeUndefined();
    } finally {
      await sb.destroy();
    }
  });

  it('passes binShell override through to wrapWithSandbox', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
      binShell: '/bin/bash',
    });
    const sb = await rt.acquire({});
    try {
      await sb.run({ tool: 'bash', args: { command: 'echo hi' } });
      expect(manager.wrapped[0]?.binShell).toBe('/bin/bash');
    } finally {
      await sb.destroy();
    }
  });

  it('rejects further run() calls after destroy', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    await sb.destroy();
    await expect(sb.run({ tool: 'bash', args: { command: 'echo hi' } })).rejects.toThrowError(
      /destroyed/,
    );
  });

  it('rejects sandbox-visible paths that traverse out of the work-dir (read)', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      await expect(sb.files.read('/../../etc/passwd')).rejects.toThrowError(
        /escapes the sandbox work-dir/,
      );
    } finally {
      await sb.destroy();
    }
  });

  it('rejects sandbox-visible paths that traverse out of the work-dir (write)', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      // Reviewer's example: `/data/../../../etc/passwd` collapses with
      // `path.join` to `/<harnessWorkDir-parent>/etc/passwd` — plainly
      // outside the work-dir.
      await expect(
        sb.files.write('/data/../../../etc/passwd', Buffer.from('x')),
      ).rejects.toThrowError(/escapes the sandbox work-dir/);
    } finally {
      await sb.destroy();
    }
  });

  it('rejects host-side reads through a symlink to process secrets', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      symlinkSync('/proc/self/environ', join(asLocalSandboxHandle(sb).rootDir(), 'leak'));
      await expect(sb.files.read('/leak')).rejects.toThrow(/symbolic link/);
    } finally {
      await sb.destroy();
    }
  });

  it('rejects read, list, write, and delete through an outside-root symlink', async () => {
    const outside = mkdtempSync(join(baseDir, 'outside-'));
    writeFileSync(join(outside, 'sentinel.txt'), 'keep');
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      symlinkSync(outside, join(asLocalSandboxHandle(sb).rootDir(), 'escape'));
      await expect(sb.files.read('/escape/sentinel.txt')).rejects.toThrow(/symbolic link/);
      await expect(sb.files.list('/escape')).rejects.toThrow(/symbolic link/);
      await expect(sb.files.write('/escape/pwned.txt', Buffer.from('x'))).rejects.toThrow(
        /symbolic link/,
      );
      await expect(sb.files.delete('/escape/sentinel.txt')).rejects.toThrow(/symbolic link/);
      expect(existsSync(join(outside, 'sentinel.txt'))).toBe(true);
      expect(existsSync(join(outside, 'pwned.txt'))).toBe(false);
    } finally {
      await sb.destroy();
    }
  });

  it('reports a non-zero exit code with diagnostic stderr when bash is killed by timeout', async () => {
    const rt = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: [],
      manager,
    });
    const sb = await rt.acquire({});
    try {
      // `sleep 1` (not 5) so even if the SIGKILL plumbing has overhead on
      // slow CI runners the parent process is reaped within the 10s
      // vitest budget. timeout_ms=100 still triggers the kill path
      // because 100ms < 1s.
      const r = await sb.run({
        tool: 'bash',
        args: { command: 'sleep 1', timeout_ms: 100 },
      });
      // GNU `timeout` convention: exit 124 means "timed out" — we use the
      // same so an LLM-driven loop can recognize the failure mode.
      expect(r.exit_code).toBe(124);
      expect(r.stderr ?? '').toMatch(/killed by SIG/);
      expect(r.stderr ?? '').toContain('100ms');
    } finally {
      await sb.destroy();
    }
  }, 10_000);

  it("forwards each session's own config to wrapWithSandbox (not just the first session's)", async () => {
    // Two separate runtimes (= two distinct work-dir bases + two distinct
    // extraReadPaths) sharing a single FakeSandboxManager. The upstream
    // SandboxManager.initialize is global+idempotent — without per-call
    // `customConfig`, every session would inherit session-1's allow-list.
    // We assert each session's own config makes it through to the wrap call,
    // proving the security fix.
    const baseDirA = mkdtempSync(join(tmpdir(), 'orca-rt-A-'));
    const baseDirB = mkdtempSync(join(tmpdir(), 'orca-rt-B-'));
    try {
      const rtA = new LocalSandboxRuntime({
        harnessWorkDir: baseDirA,
        allowedNetworkHosts: ['hostA.test'],
        extraReadPaths: ['/opt/A'],
        manager,
      });
      const rtB = new LocalSandboxRuntime({
        harnessWorkDir: baseDirB,
        allowedNetworkHosts: ['hostB.test'],
        extraReadPaths: ['/opt/B'],
        manager,
      });

      const sbA = await rtA.acquire({});
      const sbB = await rtB.acquire({});
      try {
        await sbA.run({ tool: 'bash', args: { command: ':' } });
        await sbB.run({ tool: 'bash', args: { command: ':' } });

        expect(manager.wrapped.length).toBe(2);
        const cfgA = manager.wrapped[0]!.customConfig as SandboxManagerInitConfig;
        const cfgB = manager.wrapped[1]!.customConfig as SandboxManagerInitConfig;

        // Per-session network allow-list flows through.
        expect(cfgA.network.allowedDomains).toEqual(['hostA.test']);
        expect(cfgB.network.allowedDomains).toEqual(['hostB.test']);

        // Per-session work-dir flows through (the writable paths differ
        // between A and B because they sit under different baseDirs).
        expect(cfgA.filesystem.allowWrite[0]!.startsWith(realpathSync(baseDirA))).toBe(true);
        expect(cfgB.filesystem.allowWrite[0]!.startsWith(realpathSync(baseDirB))).toBe(true);
        expect(cfgA.filesystem.allowWrite[0]).not.toBe(cfgB.filesystem.allowWrite[0]);

        // Per-session extra read paths flow through.
        expect(cfgA.filesystem.allowRead).toEqual(expect.arrayContaining(['/opt/A']));
        expect(cfgB.filesystem.allowRead).toEqual(expect.arrayContaining(['/opt/B']));
        expect(cfgA.filesystem.allowRead).not.toContain('/opt/B');
        expect(cfgB.filesystem.allowRead).not.toContain('/opt/A');
      } finally {
        await sbA.destroy();
        await sbB.destroy();
      }
    } finally {
      rmSync(baseDirA, { recursive: true, force: true });
      rmSync(baseDirB, { recursive: true, force: true });
    }
  });
});
