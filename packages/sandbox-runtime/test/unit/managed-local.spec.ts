// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// These inspect generated boundaries with a fake manager. Real SRT, seccomp,
// nested namespace and gVisor behavior require the Linux stack probe.
import * as fs from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  LocalSandboxRuntime,
  asLocalSandboxHandle,
  createManagedToolSandboxManager,
  type SandboxManagerInitConfig,
  type SandboxManagerLike,
} from '../../src/local/runtime.js';
import { buildSandboxWritePolicy, createPolicyEnforcedSandbox } from '../../src/write-policy.js';
import { hasWritePolicyEnforcement } from '../../src/sandbox-runtime.js';

vi.mock('node:fs', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs')>();
  return {
    ...actual,
    readFileSync: vi.fn(actual.readFileSync),
    readlinkSync: vi.fn(actual.readlinkSync),
    readdirSync: vi.fn(actual.readdirSync),
    realpathSync: vi.fn(actual.realpathSync),
  };
});

class RecordingManager implements SandboxManagerLike {
  config?: SandboxManagerInitConfig;
  calls: Array<{ command: string; config?: Partial<SandboxManagerInitConfig> | undefined }> = [];
  result = 'true';
  released: string[] = [];
  async initialize(config: SandboxManagerInitConfig): Promise<void> {
    this.config = config;
  }
  async wrapWithSandbox(
    command: string,
    _shell?: string,
    config?: Partial<SandboxManagerInitConfig>,
  ): Promise<string> {
    this.calls.push({ command, config });
    return this.result;
  }
  releaseSandboxCommand(wrapped: string): void {
    this.released.push(wrapped);
  }
}

describe('managed Local tool filesystem', () => {
  const platform = Object.getOwnPropertyDescriptor(process, 'platform')!;
  let baseDir: string;
  let manager: RecordingManager;
  beforeEach(async () => {
    baseDir = fs.mkdtempSync(join(tmpdir(), 'orca-managed-tools-'));
    manager = new RecordingManager();
    Object.defineProperty(process, 'platform', { ...platform, value: 'linux' });
    const actual = await vi.importActual<typeof fs>('node:fs');
    const originalRead = actual.readFileSync;
    vi.mocked(fs.readFileSync).mockImplementation(((
      ...args: Parameters<typeof fs.readFileSync>
    ) => {
      if (args[0] === '/proc/self/mountinfo') return '';
      return originalRead(...args);
    }) as typeof fs.readFileSync);
    const originalReadlink = actual.readlinkSync;
    vi.mocked(fs.readlinkSync).mockImplementation(((
      ...args: Parameters<typeof fs.readlinkSync>
    ) => {
      if (args[0] === '/proc/self/ns/pid') return 'pid:[host]';
      if (args[0] === '/proc/self/ns/net') return 'net:[host]';
      return originalReadlink(...args);
    }) as typeof fs.readlinkSync);
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    Object.defineProperty(process, 'platform', platform);
    fs.rmSync(baseDir, { recursive: true, force: true });
  });

  async function acquire(unrestricted = false) {
    const runtime = new LocalSandboxRuntime({
      harnessWorkDir: baseDir,
      allowedNetworkHosts: ['tools.example'],
      networkUnrestricted: unrestricted,
      manager,
      managedToolFilesystem: true,
    });
    const raw = await runtime.acquire({});
    if (!hasWritePolicyEnforcement(raw)) throw new Error('write policy missing');
    const root = asLocalSandboxHandle(raw).rootDir();
    fs.mkdirSync(join(root, 'mnt/session/outputs'), { recursive: true });
    fs.mkdirSync(join(root, 'workspace/skills'), { recursive: true });
    fs.mkdirSync(join(root, 'mnt/memory'), { recursive: true });
    fs.writeFileSync(join(root, 'mnt/input.txt'), 'input');
    const policy = buildSandboxWritePolicy(
      [
        { path: '/mnt/input.txt', kind: 'file', access: 'read_only' },
        { path: '/mnt/memory', kind: 'memory_store', access: 'read_write' },
      ],
      {
        includeSkillsRoot: true,
        networkAllowedDomains: ['tools.example'],
        networkUnrestricted: unrestricted,
      },
    );
    return { raw, root, policy };
  }

  it('honors unrestricted egress without weakening filesystem policy or file-helper isolation', async () => {
    const { raw, policy } = await acquire(true);
    try {
      const tools = await createPolicyEnforcedSandbox(raw, policy);
      expect(manager.calls[0]!.command).toContain('/proc/self/ns/net)" = ');
      expect(manager.calls[0]!.config!.network).toMatchObject({ unrestricted: true });
      await expect(tools.files.write('/mnt/input.txt', Buffer.from('forbidden'))).rejects.toThrow();
      await tools.files.read('/mnt/input.txt');
      expect(manager.calls.at(-1)!.config!.network).toMatchObject({
        allowedDomains: [],
        unrestricted: false,
      });
    } finally {
      await raw.destroy();
    }
    const limited = await acquire();
    try {
      await expect(
        createPolicyEnforcedSandbox(limited.raw, { ...limited.policy, networkUnrestricted: true }),
      ).rejects.toThrow('exceeds the runtime network allow-list');
    } finally {
      await limited.raw.destroy();
    }
  });

  it('is Linux-only and does not weaken the existing default Local runtime', () => {
    Object.defineProperty(process, 'platform', { ...platform, value: 'darwin' });
    const options = { harnessWorkDir: baseDir, allowedNetworkHosts: [], manager };
    expect(() => new LocalSandboxRuntime({ ...options, managedToolFilesystem: true })).toThrow(
      /requires Linux/,
    );
    expect(() => new LocalSandboxRuntime(options)).not.toThrow();
  });

  it('pre-creates real mount targets and maps shell/search paths to the same virtual tree', async () => {
    const { raw, root, policy } = await acquire();
    try {
      const sandbox = await createPolicyEnforcedSandbox(raw, policy);
      expect(manager.config?.network).toEqual({
        allowedDomains: ['tools.example'],
        deniedDomains: [],
        allowLocalBinding: false,
      });
      expect(manager.config).not.toHaveProperty('enableWeakerNestedSandbox');
      expect(manager.config?.network).not.toHaveProperty('allowAllUnixSockets');
      expect(fs.lstatSync(join(root, 'bin')).isDirectory()).toBe(true);
      expect(fs.lstatSync(join(root, 'bin')).isSymbolicLink()).toBe(false);
      const probe = manager.calls[0]!.command;
      expect(probe).toContain('stat -Lc');
      expect(probe).toContain('pid:[host]');
      expect(probe).toContain('net:[host]');
      expect(probe).toContain('mapped tool root is writable');
      expect(probe).toContain('mktemp /mnt/session/outputs');
      // The outer SRT stage supplies sources; the generated inner bwrap owns
      // their final read-only/writable mount policy.
      expect(manager.calls[0]?.config?.filesystem?.allowWrite).toEqual([fs.realpathSync(root)]);
      await sandbox.run({
        tool: 'bash',
        args: { command: 'cat /mnt/input.txt > /mnt/session/outputs/out.txt' },
      });
      await sandbox.run({ tool: 'glob', args: { root: '/mnt', pattern: '*.txt' } });
      await sandbox.run({ tool: 'grep', args: { root: '/mnt', pattern: 'input' } });
      const commands = manager.calls.slice(1).map((call) => call.command);
      expect(commands[0]).toContain('cat /mnt/input.txt > /mnt/session/outputs/out.txt');
      for (const command of commands.slice(1)) {
        expect(command).toContain('/mnt');
        expect(command).not.toContain(`cd '${root}`);
      }
      expect(commands[1]).toContain('compgen -G');
      expect(commands[2]).toContain('grep -rn');
      expect(sandbox).not.toHaveProperty('spawn');
      expect(sandbox).not.toHaveProperty('rootDir');
    } finally {
      await raw.destroy();
    }
  });

  it('fails closed when the actual mapped sandbox command fails', async () => {
    const { raw, policy } = await acquire();
    manager.result = 'echo nested-isolation-unavailable >&2; exit 73';
    try {
      await expect(createPolicyEnforcedSandbox(raw, policy)).rejects.toThrow(
        /isolation probe failed: nested-isolation-unavailable/,
      );
      await expect(
        raw.files.write('/mnt/session/outputs/file', Buffer.from('data')),
      ).rejects.toThrow(/prepared write policy/);
    } finally {
      await raw.destroy();
    }
  });

  it('rejects metadata mutation even if the sandbox command reports success and cleans its probe', async () => {
    const { raw, root, policy } = await acquire();
    const wrap = manager.wrapWithSandbox.bind(manager);
    vi.spyOn(manager, 'wrapWithSandbox').mockImplementation(async (...args) => {
      const sentinel = fs
        .readdirSync(root)
        .find((name) => name.startsWith('.orca-readonly-probe-'))!;
      fs.chmodSync(join(root, sentinel), 0o644);
      return await wrap(...args);
    });
    try {
      await expect(createPolicyEnforcedSandbox(raw, policy)).rejects.toThrow(
        /read-only metadata changes/,
      );
      expect(fs.readdirSync(root).some((name) => name.startsWith('.orca-readonly-probe-'))).toBe(
        false,
      );
      await expect(
        raw.files.write('/mnt/session/outputs/file', Buffer.from('data')),
      ).rejects.toThrow(/prepared write policy/);
    } finally {
      await raw.destroy();
    }
  });

  it('grants file helpers only prepared writable roots, and no writes for read-only operations', async () => {
    const { raw, root, policy } = await acquire();
    try {
      await expect(
        raw.files.write('/mnt/session/outputs/file', Buffer.from('data')),
      ).rejects.toThrow(/prepared write policy/);
      const sandbox = await createPolicyEnforcedSandbox(raw, policy);
      fs.writeFileSync(join(root, 'mnt/session/outputs/file'), 'data');
      manager.calls.length = 0;
      await sandbox.files.write('/mnt/session/outputs/file', Buffer.from('changed'));
      await sandbox.files.chmod('/mnt/session/outputs/file', 0o600);
      await sandbox.files.delete('/mnt/session/outputs/file');
      for (const call of manager.calls) {
        expect(call.config?.filesystem?.allowWrite).toEqual(
          policy.writablePaths.map((entry) => join(fs.realpathSync(root), entry.path)),
        );
        expect(call.config?.filesystem?.allowWrite).not.toContain(fs.realpathSync(root));
        expect(call.config?.filesystem?.allowRead).not.toContain(fs.realpathSync(root));
        expect(call.config?.filesystem?.allowRead).toEqual(
          expect.arrayContaining(
            policy.readonlyPaths.map((entry) => join(fs.realpathSync(root), entry)),
          ),
        );
      }
      manager.calls.length = 0;
      await sandbox.files.read('/mnt/input.txt');
      manager.result = 'printf "[]"';
      await sandbox.files.list('/mnt/memory');
      for (const call of manager.calls) expect(call.config?.filesystem?.allowWrite).toEqual([]);
      // Revalidate each call after prepare, not only once at policy activation.
      fs.rmSync(join(root, 'mnt/memory'), { recursive: true });
      fs.symlinkSync('../input.txt', join(root, 'mnt/memory'));
      await expect(
        sandbox.files.write('/mnt/session/outputs/file', Buffer.from('data')),
      ).rejects.toThrow(/symbolic link/);
    } finally {
      await raw.destroy();
    }
  });

  it('rejects symlink and hard-link aliases before invoking the model shell', async () => {
    const { raw, root, policy } = await acquire();
    try {
      fs.rmSync(join(root, 'mnt/input.txt'));
      const privateFile = join(baseDir, 'worker-checkpoint');
      fs.writeFileSync(privateFile, 'secret');
      fs.symlinkSync(privateFile, join(root, 'mnt/input.txt'));
      await expect(raw.prepareWritePolicy(policy)).rejects.toThrow(/symbolic link/);
      expect(manager.calls).toHaveLength(0);
      fs.rmSync(join(root, 'mnt/input.txt'));
      fs.linkSync(privateFile, join(root, 'mnt/input.txt'));
      await expect(raw.prepareWritePolicy(policy)).rejects.toThrow(/hard-linked/);
      expect(manager.calls).toHaveLength(0);
    } finally {
      await raw.destroy();
    }
  });

  it('rejects nested mounts and replaced runtime targets', async () => {
    const { raw, root, policy } = await acquire();
    try {
      vi.mocked(fs.readFileSync).mockReturnValue(
        `1 0 0:1 / ${fs.realpathSync(root)}/mnt/memory rw - tmpfs tmpfs rw\n`,
      );
      await expect(raw.prepareWritePolicy(policy)).rejects.toThrow(/pre-existing mount/);
      vi.mocked(fs.readFileSync).mockReturnValue('');
      fs.rmSync(join(root, 'bin'), { recursive: true });
      fs.symlinkSync('/bin', join(root, 'bin'));
      await expect(raw.prepareWritePolicy(policy)).rejects.toThrow(/symbolic link/);
      expect(manager.calls).toHaveLength(0);
    } finally {
      await raw.destroy();
    }
  });

  it('does not grant ambient hosts or pass worker secrets to the SRT process', async () => {
    vi.stubEnv('ANTHROPIC_API_KEY', 'worker-secret');
    vi.stubEnv('AI_GATEWAY_URL', 'https://ambient-gateway');
    vi.stubEnv('AWS_SECRET_ACCESS_KEY', 'object-store-secret');
    const { raw, policy } = await acquire();
    manager.result = 'env';
    try {
      const result = await raw.runWithWritePolicy(
        { tool: 'bash', args: { command: 'env' } },
        policy,
      );
      expect(result.stdout).not.toContain('worker-secret');
      expect(result.stdout).not.toContain('object-store-secret');
      expect(result.stdout).not.toContain('ambient-gateway');
      expect(manager.calls[0]!.config?.network?.allowedDomains).toEqual(['tools.example']);
      await expect(
        raw.runWithWritePolicy(
          { tool: 'bash', args: { command: 'true' } },
          buildSandboxWritePolicy([], { networkAllowedDomains: ['api.anthropic.com'] }),
        ),
      ).rejects.toThrow(/exceeds/);
    } finally {
      await raw.destroy();
    }
  });

  it('canonicalizes merged-/usr root masks without weakening private directory denial', async () => {
    const original = await vi.importActual<typeof fs>('node:fs');
    vi.mocked(fs.readdirSync).mockImplementation(((path, ...args) =>
      path === '/'
        ? ['bin', 'lib', 'lib64', 'usr', 'opt', 'home', 'tmp', 'proc', 'dev', 'sys']
        : original.readdirSync(path, ...args)) as typeof fs.readdirSync);
    vi.mocked(fs.realpathSync).mockImplementation(((path, ...args) => {
      const merged: Record<string, string> = {
        '/bin': '/usr/bin',
        '/lib': '/usr/lib',
        '/lib64': '/usr/lib64',
      };
      return (
        merged[String(path)] ??
        (['/usr', '/opt', '/home', '/tmp'].includes(String(path))
          ? String(path)
          : original.realpathSync(path, ...args))
      );
    }) as typeof fs.realpathSync);
    const manager = createManagedToolSandboxManager();
    const wrapped = await manager.wrapWithSandbox('true', '/bin/bash', {
      network: { allowedDomains: [], deniedDomains: [] },
      filesystem: { denyRead: ['/'], allowRead: ['/usr/bin'], allowWrite: [], denyWrite: [] },
    });
    try {
      const serialized = wrapped.slice(wrapped.lastIndexOf("'{") + 1, -1);
      const config = JSON.parse(serialized).config;
      expect(config.filesystem.denyRead).toEqual(['/usr', '/opt', '/home', '/tmp']);
      expect(config.filesystem.allowRead).toContain('/usr/bin');
    } finally {
      await manager.releaseSandboxCommand?.(wrapped);
    }
  });

  it('uses a fresh manager process for every command rather than singleton proxy grants', async () => {
    const isolated = createManagedToolSandboxManager();
    const config = {
      network: { allowedDomains: ['one.example'], deniedDomains: [] },
      filesystem: { denyRead: ['/'], allowWrite: [], denyWrite: [] },
    };
    const first = await isolated.wrapWithSandbox('true', '/bin/bash', config);
    const second = await isolated.wrapWithSandbox('true', '/bin/bash', {
      ...config,
      network: { allowedDomains: ['two.example'], deniedDomains: [] },
    });
    expect(first).toContain('SandboxManager.initialize(config)');
    expect(first).toContain('await SandboxManager.reset()');
    expect(first).toContain('one.example');
    expect(first).not.toContain('two.example');
    expect(second).toContain('two.example');
    expect(second).not.toContain('one.example');
    const temporaryPaths = [first, second].map((wrapped) => {
      const environment = wrapped.match(/Object\.assign\(process\.env, (\{[^\n]+\})\);/)?.[1];
      expect(environment).toBeDefined();
      const parsed = JSON.parse(environment!);
      const directory = parsed.TMPDIR as string;
      expect(directory).toMatch(/^\/tmp\/orca-srt-/);
      expect(parsed).toEqual({ TMPDIR: directory, TMP: directory, TEMP: directory });
      expect(fs.statSync(directory!).mode & 0o777).toBe(0o700);
      expect(Buffer.byteLength(join(directory!, 'claude-http-0123456789abcdef.sock'))).toBeLessThan(
        108,
      );
      return directory!;
    });
    expect(temporaryPaths[0]).not.toBe(temporaryPaths[1]);
    await isolated.releaseSandboxCommand!(first);
    await isolated.releaseSandboxCommand!(second);
    await isolated.releaseSandboxCommand!(second);
    for (const directory of temporaryPaths) expect(fs.existsSync(directory)).toBe(false);
  });

  it('releases trusted command state when the mapped subprocess times out', async () => {
    const { raw, policy } = await acquire();
    try {
      await createPolicyEnforcedSandbox(raw, policy);
      manager.released.length = 0;
      manager.result = 'sleep 5';
      await expect(
        raw.runWithWritePolicy({ tool: 'bash', args: { command: 'true', timeout_ms: 30 } }, policy),
      ).resolves.toMatchObject({ exit_code: 124 });
      expect(manager.released).toEqual(['sleep 5']);
    } finally {
      await raw.destroy();
    }
  });
});
