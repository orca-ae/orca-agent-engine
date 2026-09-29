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
import {
  mountSessionOutputs,
  type MountSessionOutputsInput,
} from '../../src/sandbox/outputs/output-mount.js';

interface RecordedRun {
  call: ToolCall;
}

interface RecordedPrivileged {
  cmd: string;
  envs?: Record<string, string>;
}

/**
 * Minimal fake SandboxHandle recording run/privileged invocations. The
 * `runResult` / `privilegedResult` getters let individual tests override the
 * exit code without rebuilding the whole stub.
 */
class FakeSandboxHandle implements SandboxHandle {
  readonly id = 'sbx_fake';
  readonly runs: RecordedRun[] = [];
  readonly privileged: RecordedPrivileged[] = [];
  readonly fileWrites: string[] = [];
  readonly fileDeletes: string[] = [];
  runResult: ToolResult = { exit_code: 0, stdout: '', stderr: '' };
  privilegedResult: ToolResult = { exit_code: 0, stdout: '', stderr: '' };
  constructor(private readonly canonicalPaths = new Map<string, string>()) {}

  readonly files: SandboxFiles = {
    write: async (path) => {
      this.fileWrites.push(path);
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
    async chmod() {},
    delete: async (path) => {
      this.fileDeletes.push(path);
    },
  };

  async run(call: ToolCall): Promise<ToolResult> {
    this.runs.push({ call });
    return this.runResult;
  }

  async canonicalizePathForPolicy(path: string): Promise<string> {
    return this.canonicalPaths.get(path) ?? path;
  }

  async runPrivileged(cmd: string, opts?: { envs?: Record<string, string> }): Promise<ToolResult> {
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

function fuseInput(
  sandbox: SandboxHandle,
  overrides: Partial<MountSessionOutputsInput> = {},
): MountSessionOutputsInput {
  return {
    sandbox,
    workspaceId: 'ws_x',
    sessionId: 'ses_y',
    generationId: 'run_test',
    supportsFuse: true,
    bucket: 'orca-files',
    endpoint: 'http://minio:9000',
    outputsRoot: 'outputs/',
    creds: fakeCreds(),
    ...overrides,
  };
}

describe('mountSessionOutputs', () => {
  describe('InMemory path (supportsFuse=false)', () => {
    it('creates the output root through the Files API without issuing commands', async () => {
      const sb = new FakeSandboxHandle();

      const handle = await mountSessionOutputs({
        sandbox: sb,
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        generationId: 'run_test',
        supportsFuse: false,
      });

      expect(sb.runs).toHaveLength(0);
      expect(sb.privileged).toHaveLength(0);
      expect(sb.fileWrites).toEqual(['/mnt/session/outputs/.orca-output-mount']);
      expect(sb.fileDeletes).toEqual(['/mnt/session/outputs/.orca-output-mount']);

      expect(handle.kind).toBe('inmemory_local');
      expect(handle.sandboxPath).toBe('/mnt/session/outputs');
      expect(handle.s3).toBeUndefined();
    });

    it('rejects an output alias to the reserved Skill root before writing', async () => {
      const sb = new FakeSandboxHandle(
        new Map([['/mnt/session/outputs', '/workspace/skills/output-alias']]),
      );

      await expect(
        mountSessionOutputs({
          sandbox: sb,
          workspaceId: 'ws_x',
          sessionId: 'ses_y',
          generationId: 'run_test',
          supportsFuse: false,
        }),
      ).rejects.toThrow(/resolves across reserved Skill root/);

      expect(sb.fileWrites).toEqual([]);
      expect(sb.fileDeletes).toEqual([]);
      expect(sb.privileged).toEqual([]);
    });
  });

  it('creates a local output directory without invoking shell commands when forced', async () => {
    const sb = new FakeSandboxHandle();

    const handle = await mountSessionOutputs({
      sandbox: sb,
      workspaceId: 'ws_x',
      sessionId: 'ses_y',
      generationId: 'run_test',
      supportsFuse: true,
      forceLocal: true,
    });

    expect(sb.runs).toHaveLength(0);
    expect(sb.privileged).toHaveLength(0);
    expect(sb.fileWrites).toEqual(['/mnt/session/outputs/.orca-output-mount']);
    expect(sb.fileDeletes).toEqual(['/mnt/session/outputs/.orca-output-mount']);
    expect(handle.kind).toBe('inmemory_local');
  });

  describe('FUSE path (supportsFuse=true)', () => {
    it('issues mkdir run + s3fs runPrivileged with the expected substrings', async () => {
      const sb = new FakeSandboxHandle();

      const handle = await mountSessionOutputs({
        sandbox: sb,
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        generationId: 'run_test',
        supportsFuse: true,
        bucket: 'orca-files',
        endpoint: 'http://minio:9000',
        outputsRoot: 'outputs/',
        creds: fakeCreds(),
      });

      // mkdir on the non-privileged path; s3fs on the privileged path.
      expect(sb.runs).toHaveLength(1);
      expect((sb.runs[0]!.call.args as { command: string }).command).toBe(
        'mkdir -p /mnt/session/outputs',
      );
      expect(sb.privileged).toHaveLength(1);

      const mount = sb.privileged[0]!;
      expect(mount.cmd).toContain('/usr/local/bin/orca-s3fs-mount');
      expect(mount.cmd).toContain('orca-files');
      expect(mount.cmd).toContain(
        'outputs/workspaces/ws_x/sessions/ses_y/executions/run_test/outputs/',
      );
      expect(mount.cmd).toContain('/mnt/session/outputs');
      // Same opts as the template's start.sh::mount_outputs — keep them
      // consistent so an operator running a manual spike sees the same flags.
      expect(mount.cmd).toContain('use_path_request_style');
      expect(mount.cmd).toContain('use_cache=');
      expect(mount.cmd).toContain('ensure_diskfree=0');
      expect(mount.cmd).toContain('url=http://minio:9000');

      expect(mount.envs).toBeDefined();
      expect(mount.envs!['ORCA_S3_ACCESS_KEY_ID']).toBe('ASIATESTKEY');
      expect(mount.envs!['ORCA_S3_SECRET_ACCESS_KEY']).toBe('testSecretKey');
      expect(mount.envs!['ORCA_S3_SESSION_TOKEN']).toBe('sts-session-token');

      // The cmd MUST NOT leak the secret values — only the env passes them.
      expect(mount.cmd).not.toContain('testSecretKey');
      expect(mount.cmd).not.toContain('ASIATESTKEY');
      expect(mount.cmd).not.toContain('sts-session-token');

      expect(handle.kind).toBe('s3');
      expect(handle.sandboxPath).toBe('/mnt/session/outputs');
      expect(handle.s3).toEqual({
        bucket: 'orca-files',
        endpoint: 'http://minio:9000',
        prefix: 'outputs/workspaces/ws_x/sessions/ses_y/executions/run_test/outputs/',
      });
    });

    it('omits path-style addressing when virtual-hosted S3 addressing is configured', async () => {
      const sb = new FakeSandboxHandle();

      await mountSessionOutputs(
        fuseInput(sb, {
          endpoint: 'https://s3.us-west-1.amazonaws.com',
          region: 'us-west-1',
          forcePathStyle: false,
        }),
      );

      expect(sb.privileged[0]!.cmd).not.toContain('use_path_request_style');
      expect(sb.privileged[0]!.cmd).toContain(
        'allow_other,use_cache=,ensure_diskfree=0,compat_dir',
      );
      expect(sb.privileged[0]!.cmd).toContain(
        'url=https://s3.us-west-1.amazonaws.com,endpoint=us-west-1',
      );
    });

    it('returns s3.prefix ending with `/` (trailing-slash invariant)', async () => {
      const sb = new FakeSandboxHandle();

      const handle = await mountSessionOutputs({
        sandbox: sb,
        workspaceId: 'ws_alpha',
        sessionId: 'ses_beta',
        generationId: 'run_test',
        supportsFuse: true,
        bucket: 'orca-files',
        endpoint: 'http://minio:9000',
        outputsRoot: 'outputs/',
        creds: fakeCreds(),
      });

      expect(handle.s3?.prefix.endsWith('/')).toBe(true);
      expect(handle.s3?.prefix).toBe(
        'outputs/workspaces/ws_alpha/sessions/ses_beta/executions/run_test/outputs/',
      );
    });

    it('isolates different runner generations under the same session prefix', async () => {
      const first = await mountSessionOutputs({
        sandbox: new FakeSandboxHandle(),
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        generationId: 'run_first',
        supportsFuse: true,
        bucket: 'orca-files',
        endpoint: 'http://minio:9000',
        outputsRoot: 'outputs/',
        creds: fakeCreds(),
      });
      const second = await mountSessionOutputs({
        sandbox: new FakeSandboxHandle(),
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        generationId: 'run_second',
        supportsFuse: true,
        bucket: 'orca-files',
        endpoint: 'http://minio:9000',
        outputsRoot: 'outputs/',
        creds: fakeCreds(),
      });

      expect(first.s3?.prefix).toBe(
        'outputs/workspaces/ws_x/sessions/ses_y/executions/run_first/outputs/',
      );
      expect(second.s3?.prefix).toBe(
        'outputs/workspaces/ws_x/sessions/ses_y/executions/run_second/outputs/',
      );
    });

    it('includes region in handle.s3 when provided', async () => {
      const sb = new FakeSandboxHandle();

      const handle = await mountSessionOutputs({
        sandbox: sb,
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        generationId: 'run_test',
        supportsFuse: true,
        bucket: 'orca-files',
        endpoint: 'http://minio:9000',
        outputsRoot: 'outputs/',
        creds: fakeCreds(),
        region: 'us-east-1',
      });

      expect(handle.s3?.region).toBe('us-east-1');
    });

    it('allows an empty configured root without changing the canonical namespace', async () => {
      const handle = await mountSessionOutputs(
        fuseInput(new FakeSandboxHandle(), { outputsRoot: '' }),
      );

      expect(handle.s3?.prefix).toBe('workspaces/ws_x/sessions/ses_y/executions/run_test/outputs/');
    });

    it('rejects unsafe object-key segments before issuing sandbox commands', async () => {
      const invalidInputs: Array<Partial<MountSessionOutputsInput>> = [
        { workspaceId: 'ws/escape' },
        { sessionId: 'ses*' },
        { generationId: '../run' },
        { outputsRoot: 'outputs/*/' },
      ];

      for (const overrides of invalidInputs) {
        const sandbox = new FakeSandboxHandle();
        await expect(mountSessionOutputs(fuseInput(sandbox, overrides))).rejects.toThrow(
          /invalid .*object-key segment|invalid outputsRoot/,
        );
        expect(sandbox.runs).toHaveLength(0);
        expect(sandbox.privileged).toHaveLength(0);
      }
    });

    it('throws when supportsFuse=true but creds is missing', async () => {
      const sb = new FakeSandboxHandle();

      await expect(
        mountSessionOutputs({
          sandbox: sb,
          workspaceId: 'ws_x',
          sessionId: 'ses_y',
          generationId: 'run_test',
          supportsFuse: true,
          bucket: 'orca-files',
          endpoint: 'http://minio:9000',
          outputsRoot: 'outputs/',
          // creds intentionally absent
        }),
      ).rejects.toThrow(/supportsFuse=true requires bucket, endpoint, outputsRoot, creds/);
    });

    it('throws when supportsFuse=true but bucket is missing', async () => {
      const sb = new FakeSandboxHandle();

      await expect(
        mountSessionOutputs({
          sandbox: sb,
          workspaceId: 'ws_x',
          sessionId: 'ses_y',
          generationId: 'run_test',
          supportsFuse: true,
          endpoint: 'http://minio:9000',
          outputsRoot: 'outputs/',
          creds: fakeCreds(),
        }),
      ).rejects.toThrow(/supportsFuse=true requires bucket, endpoint, outputsRoot, creds/);
    });

    it('throws with the stderr when mkdir returns a non-zero exit_code (FUSE path)', async () => {
      const sb = new FakeSandboxHandle();
      sb.runResult = { exit_code: 1, stderr: 'mkdir: failed for some reason' };

      await expect(
        mountSessionOutputs({
          sandbox: sb,
          workspaceId: 'ws_x',
          sessionId: 'ses_y',
          generationId: 'run_test',
          supportsFuse: true,
          bucket: 'orca-files',
          endpoint: 'http://minio:9000',
          outputsRoot: 'outputs/',
          creds: fakeCreds(),
        }),
      ).rejects.toThrow(/mkdir: failed for some reason/);
    });

    it('throws with the stderr when runPrivileged returns a non-zero exit_code', async () => {
      const sb = new FakeSandboxHandle();
      sb.privilegedResult = {
        exit_code: 1,
        stderr: 's3fs: unable to access /mnt/session/outputs (Operation not permitted)',
      };

      await expect(
        mountSessionOutputs({
          sandbox: sb,
          workspaceId: 'ws_x',
          sessionId: 'ses_y',
          generationId: 'run_test',
          supportsFuse: true,
          bucket: 'orca-files',
          endpoint: 'http://minio:9000',
          outputsRoot: 'outputs/',
          creds: fakeCreds(),
        }),
      ).rejects.toThrow(/Operation not permitted/);
    });
  });
});
