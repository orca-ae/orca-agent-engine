// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type { SessionS3Creds } from '../../src/auth/sts-creds.js';
import type {
  SandboxFiles,
  SandboxHandle,
  ToolCall,
  ToolResult,
} from '../../src/sandbox/sandbox-runtime.js';
import { MemoryFuseStrategy } from '../../src/sandbox/mounts/memory-fuse.js';
import type { MountResource } from '../../src/sandbox/mounts/mount-strategy.js';

interface RecordedRun {
  call: ToolCall;
}

interface RecordedPrivileged {
  cmd: string;
  envs?: Record<string, string>;
}

class FakeSandboxHandle implements SandboxHandle {
  readonly id = 'sbx_fake';
  readonly runs: RecordedRun[] = [];
  readonly privileged: RecordedPrivileged[] = [];
  runResult: ToolResult = { exit_code: 0, stdout: '', stderr: '' };
  privilegedResult: ToolResult = { exit_code: 0, stdout: '', stderr: '' };
  /** When set, runPrivileged THROWS instead of returning a result — exercises
   *  the deactivate best-effort umount path. */
  privilegedThrows: Error | null = null;

  readonly files: SandboxFiles = {
    async write() {
      throw new Error('files.write not used');
    },
    async read() {
      throw new Error('files.read not used');
    },
    async readUtf8Page() {
      throw new Error('files.readUtf8Page not used');
    },
    async list() {
      return [];
    },
    async chmod() {
      /* no-op */
    },
    async delete() {
      /* no-op */
    },
  };

  async run(call: ToolCall): Promise<ToolResult> {
    this.runs.push({ call });
    return this.runResult;
  }

  async runPrivileged(cmd: string, opts?: { envs?: Record<string, string> }): Promise<ToolResult> {
    if (this.privilegedThrows) throw this.privilegedThrows;
    const entry: RecordedPrivileged = { cmd };
    if (opts?.envs) entry.envs = opts.envs;
    this.privileged.push(entry);
    return this.privilegedResult;
  }

  async pause(): Promise<void> {
    /* no-op */
  }
  async resume(): Promise<void> {
    /* no-op */
  }
  async destroy(): Promise<void> {
    /* no-op */
  }
}

function fakeCreds(): SessionS3Creds {
  return {
    accessKeyId: 'ASIATESTKEY',
    secretAccessKey: 'testSecretKey',
    sessionToken: 'sts-session-token',
    expiresAt: Math.floor(Date.now() / 1000) + 3600,
  };
}

const baseConfig = () => ({
  workspaceId: 'ws_test',
  sessionId: 'ses_test',
  bucket: 'orca-files',
  endpoint: 'http://minio:9000',
  keyPrefix: 'memory/',
  creds: fakeCreds(),
});

const baseResource: MountResource = {
  type: 'memory_store',
  id: 'sesrsc_1',
  memoryStoreId: 'mems_a',
  storeName: 'prefs',
  mountPath: '/mnt/memory/prefs/',
  access: 'read_write',
};

describe('MemoryFuseStrategy', () => {
  it('first activate issues mkdir + s3fs runPrivileged with the per-store prefix', async () => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy(baseConfig());

    const handle = await strategy.activate(sb, baseResource);

    expect(sb.runs).toHaveLength(1);
    expect(sb.runs[0]!.call.tool).toBe('bash');
    expect((sb.runs[0]!.call.args as { command: string }).command).toBe(
      "mkdir -p -- '/mnt/memory/prefs/'",
    );

    expect(sb.privileged).toHaveLength(1);
    expect(sb.privileged[0]!.cmd).toContain(
      'orca-files:/memory/workspaces/ws_test/memory-stores/mems_a/live/',
    );
    expect(sb.privileged[0]!.cmd).toContain('/mnt/memory/prefs/');
    expect(sb.privileged[0]!.cmd).toContain(
      'allow_other,use_path_request_style,use_cache=,ensure_diskfree=0,compat_dir,uid=1000,gid=1000,umask=0022,url=http://minio:9000',
    );
    // RW mount: `-o ro,` MUST NOT appear — memory stores mount read-write.
    expect(sb.privileged[0]!.cmd).not.toContain('-o ro');

    expect(sb.privileged[0]!.envs).toMatchObject({
      ORCA_S3_ACCESS_KEY_ID: 'ASIATESTKEY',
      ORCA_S3_SECRET_ACCESS_KEY: 'testSecretKey',
      ORCA_S3_SESSION_TOKEN: 'sts-session-token',
    });
    expect(sb.privileged[0]!.cmd).toContain('/usr/local/bin/orca-s3fs-mount');
    // Creds MUST NOT leak via the cmdline.
    expect(sb.privileged[0]!.cmd).not.toContain('testSecretKey');
    expect(sb.privileged[0]!.cmd).not.toContain('ASIATESTKEY');
    expect(sb.privileged[0]!.cmd).not.toContain('sts-session-token');

    expect(handle.resourceType).toBe('memory_store');
    expect(handle.mountPath).toBe('/mnt/memory/prefs/');
    expect(handle.resourceId).toBe('sesrsc_1');
  });

  it('omits path-style addressing when virtual-hosted S3 addressing is configured', async () => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy({
      ...baseConfig(),
      endpoint: 'https://s3.us-west-1.amazonaws.com',
      region: 'us-west-1',
      forcePathStyle: false,
    });

    await strategy.activate(sb, baseResource);

    expect(sb.privileged[0]!.cmd).not.toContain('use_path_request_style');
    expect(sb.privileged[0]!.cmd).toContain('allow_other,use_cache=,ensure_diskfree=0,compat_dir');
    expect(sb.privileged[0]!.cmd).toContain(
      'url=https://s3.us-west-1.amazonaws.com,endpoint=us-west-1',
    );
  });

  it('idempotent: second activate of the same memoryStoreId does NOT re-issue the mount', async () => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy(baseConfig());

    await strategy.activate(sb, baseResource);
    await strategy.activate(sb, { ...baseResource, id: 'sesrsc_2' });

    expect(sb.runs).toHaveLength(1);
    expect(sb.privileged).toHaveLength(1);
  });

  it('different memoryStoreId triggers a new mount + mkdir', async () => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy(baseConfig());

    await strategy.activate(sb, baseResource);
    await strategy.activate(sb, {
      ...baseResource,
      id: 'sesrsc_2',
      memoryStoreId: 'mems_b',
      storeName: 'logs',
      mountPath: '/mnt/memory/logs/',
    });

    expect(sb.runs).toHaveLength(2);
    expect(sb.privileged).toHaveLength(2);
    expect(sb.privileged[1]!.cmd).toContain(
      'orca-files:/memory/workspaces/ws_test/memory-stores/mems_b/live/',
    );
    expect(sb.privileged[1]!.cmd).toContain('/mnt/memory/logs/');
  });

  it('throws on non-memory_store resource', async () => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy(baseConfig());

    await expect(
      strategy.activate(sb, {
        type: 'file',
        id: 'sesrsc_x',
        fileId: 'file_x',
        mountPath: '/mnt/x.bin',
        access: 'read_only',
      }),
    ).rejects.toThrow(/unsupported resource type/);

    expect(sb.runs).toHaveLength(0);
    expect(sb.privileged).toHaveLength(0);
  });

  it('adds a read-only FUSE option for read_only resources', async () => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy(baseConfig());

    await strategy.activate(sb, { ...baseResource, access: 'read_only' });

    expect(sb.privileged[0]!.cmd).toContain('ro,allow_other');
  });

  it('rejects a store id that could escape its object prefix', async () => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy(baseConfig());

    await expect(
      strategy.activate(sb, { ...baseResource, memoryStoreId: '../other' }),
    ).rejects.toThrow(/invalid storeId/);
    expect(sb.runs).toHaveLength(0);
    expect(sb.privileged).toHaveLength(0);
  });

  it.each(['_team', '-team'])('accepts auth-valid workspace id %s', async (workspaceId) => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy({ ...baseConfig(), workspaceId });

    await strategy.activate(sb, baseResource);

    expect(sb.privileged[0]!.cmd).toContain(`/workspaces/${workspaceId}/`);
  });

  it.each([
    '../mnt/memory/prefs/',
    '/mnt/memory/../escape/',
    '/mnt/memory//prefs/',
    '/mnt/memory/prefs;touch-pwned/',
    '/mnt/custom/',
    '/workspace/prefs/',
    '/mnt/',
    '/mnt/memory/prefs\nnext/',
  ])('rejects unsafe mount path %j before any sandbox side effect', async (mountPath) => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy(baseConfig());

    await expect(strategy.activate(sb, { ...baseResource, mountPath })).rejects.toThrow(
      /invalid mountPath/,
    );
    expect(sb.runs).toHaveLength(0);
    expect(sb.privileged).toHaveLength(0);
  });

  it('rejects invalid access before creating the mount directory', async () => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy(baseConfig());

    await expect(
      strategy.activate(sb, { ...baseResource, access: 'invalid' as 'read_write' }),
    ).rejects.toThrow(/unsupported access/);
    expect(sb.runs).toHaveLength(0);
    expect(sb.privileged).toHaveLength(0);
  });

  it('throws when mkdir fails (with stderr in the message)', async () => {
    const sb = new FakeSandboxHandle();
    sb.runResult = { exit_code: 1, stderr: 'permission denied' };
    const strategy = new MemoryFuseStrategy(baseConfig());

    await expect(strategy.activate(sb, baseResource)).rejects.toThrow(
      /mkdir.*\/mnt\/memory\/prefs\/.*failed.*permission denied/,
    );
    // s3fs MUST NOT have been attempted.
    expect(sb.privileged).toHaveLength(0);
  });

  it('throws when s3fs mount fails (operator hint includes CAP_SYS_ADMIN)', async () => {
    const sb = new FakeSandboxHandle();
    sb.privilegedResult = { exit_code: 1, stderr: 'CAP_SYS_ADMIN missing' };
    const strategy = new MemoryFuseStrategy(baseConfig());

    await expect(strategy.activate(sb, baseResource)).rejects.toThrow(/CAP_SYS_ADMIN/);
    // mkdir DID run; the failure is at the privileged mount step.
    expect(sb.runs).toHaveLength(1);
  });

  it('deactivate issues a best-effort umount and swallows errors', async () => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy(baseConfig());
    const handle = await strategy.activate(sb, baseResource);

    sb.privilegedThrows = new Error('umount: target busy');
    // Should NOT throw — swallowed.
    await expect(strategy.deactivate(sb, handle)).resolves.toBeUndefined();
  });

  it('snapshot hooks teardownForSnapshot + restoreAfterSnapshot are no-ops', async () => {
    const sb = new FakeSandboxHandle();
    const strategy = new MemoryFuseStrategy(baseConfig());
    const handle = await strategy.activate(sb, baseResource);

    const runsBefore = sb.runs.length;
    const privBefore = sb.privileged.length;

    const torn = await strategy.teardownForSnapshot(sb, handle);
    expect(torn.mountPath).toBe('/mnt/memory/prefs/');
    expect(torn.resourceType).toBe('memory_store');
    expect(torn.resourceId).toBe('sesrsc_1');

    const restored = await strategy.restoreAfterSnapshot(sb, torn);
    expect(restored.mountPath).toBe('/mnt/memory/prefs/');
    expect(restored.resourceId).toBe('sesrsc_1');
    expect(restored.resourceType).toBe('memory_store');

    // No additional sandbox calls.
    expect(sb.runs.length).toBe(runsBefore);
    expect(sb.privileged.length).toBe(privBefore);
  });
});
