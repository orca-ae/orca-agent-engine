// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// E2BSandboxRuntime — relocation coverage (moved from harness-server's
// src/sandbox/e2b/runtime.ts to @orca/cloud-sandbox by A2). No real E2B
// credentials are used: `acquire()` only calls the SDK's `Sandbox.create`,
// which these tests never invoke — mirrors harness-server's own
// sandbox-capabilities.spec.ts split (capability flag asserted without
// acquiring; the real-sandbox round-trip is a separately gated live test).

import { buildSandboxWritePolicy } from '@orca/sandbox-runtime';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  E2BSandboxRuntime,
  buildE2BEndpointUrl,
  buildE2BPrerequisiteProbeCommand,
  buildE2BPrivilegedCommand,
} from '../../src/e2b/runtime.js';

const e2bMocks = vi.hoisted(() => ({
  create: vi.fn(),
  run: vi.fn(),
  read: vi.fn(),
}));

vi.mock('@e2b/code-interpreter', () => ({
  Sandbox: {
    create: e2bMocks.create,
  },
}));

describe('E2BSandboxRuntime', () => {
  it('advertises supportsFuse=true on the runtime instance (no acquire required)', () => {
    // Constructing the runtime is safe — `Sandbox.create` is only called
    // inside `acquire()`, and we don't acquire here. This lets us assert the
    // static capability flag without an E2B API key.
    const rt = new E2BSandboxRuntime({ apiKey: 'unused-test-key' });
    expect(rt.capabilities.supportsFuse).toBe(true);
    expect(rt.capabilities.supportsWritePolicy).toBe(true);
  });

  it('accepts an optional templateId/baseURL without throwing', () => {
    const rt = new E2BSandboxRuntime({
      apiKey: 'unused-test-key',
      templateId: 'orca-default',
      baseURL: 'https://e2b.example.com',
    });
    expect(rt.capabilities.supportsFuse).toBe(true);
  });
});

describe('buildE2BEndpointUrl', () => {
  it('prefixes a bare host with https://', () => {
    expect(buildE2BEndpointUrl('abc-4096.e2b.dev')).toBe('https://abc-4096.e2b.dev');
  });

  it('leaves an already-schemed URL untouched', () => {
    expect(buildE2BEndpointUrl('https://abc-4096.e2b.dev')).toBe('https://abc-4096.e2b.dev');
    expect(buildE2BEndpointUrl('http://abc-4096.e2b.dev')).toBe('http://abc-4096.e2b.dev');
  });
});

describe('buildE2BPrivilegedCommand', () => {
  it('preserves only sudoers-approved S3 transport envs', () => {
    expect(
      buildE2BPrivilegedCommand(' sudo /usr/local/bin/orca-s3fs-mount bucket /mnt/memory/x opts', [
        'BASH_ENV',
        'ENV',
        'PATH',
        'ORCA_S3_ACCESS_KEY_ID',
        'ORCA_S3_SECRET_ACCESS_KEY',
      ]),
    ).toBe(
      'sudo --preserve-env=ORCA_S3_ACCESS_KEY_ID,ORCA_S3_SECRET_ACCESS_KEY /usr/local/bin/orca-s3fs-mount bucket /mnt/memory/x opts',
    );
  });

  it('rejects an unsupported privileged environment name', () => {
    expect(() => buildE2BPrivilegedCommand('true', ['BASH_ENV', 'LD_PRELOAD'])).toThrow(
      /unsupported E2B privileged environment: LD_PRELOAD/,
    );
  });
});

describe('buildE2BPrerequisiteProbeCommand', () => {
  it('fails E2B acquisition on broad sudo, arbitrary root shell, or missing FUSE prerequisites', () => {
    const probe = buildE2BPrerequisiteProbeCommand();
    expect(probe).toContain('test -c /dev/fuse');
    expect(probe).toContain('NOPASSWD:[[:space:]]*ALL|NOPASSWD:SETENV');
    expect(probe).toContain('sudo -n /bin/sh -c id');
    expect(probe).toContain('/usr/local/bin/orca-s3fs-mount');
    expect(probe).toContain('/mnt/custom');
    expect(probe).toContain('credlib=/tmp/forbidden.so');
    expect(probe).toContain('test "$helper_rc" = 64');
    expect(probe).toContain('test "$custom_mount_rc" = 64');
  });
});

/**
 * Build the duck-typed rejection the real SDK produces: `commands.run` THROWS
 * a CommandExitError carrying exitCode/stdout/stderr whenever the command
 * exits non-zero (verified against the pinned e2b SDK).
 * Fakes that resolve with a non-zero exitCode model a contract the SDK does
 * not have and masked the no-match regression.
 */
function sdkExitError(exitCode: number, stdout = '', stderr = ''): Error {
  return Object.assign(new Error(`exit status ${exitCode}`), { exitCode, stdout, stderr });
}

describe('E2B command normalization (throw-on-nonzero SDK contract)', () => {
  beforeEach(() => {
    e2bMocks.create.mockReset();
    e2bMocks.run.mockReset();
    e2bMocks.create.mockResolvedValue({
      sandboxId: 'sb_e2b_glob_test',
      commands: { run: e2bMocks.run },
      files: { write: vi.fn(), read: e2bMocks.read, list: vi.fn().mockResolvedValue([]) },
    });
  });

  it('a no-match glob (exit 1 → SDK throw) is an empty success, not a rejection', async () => {
    // The ordinary case: compgen exits 1
    // for zero matches, the SDK throws, and run() must still resolve {output: []}.
    e2bMocks.run
      // acquire()'s template prerequisite probe must pass first.
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 })
      .mockRejectedValue(sdkExitError(1));
    const handle = await new E2BSandboxRuntime({ apiKey: 'test-key' }).acquire({});
    await expect(handle.run({ tool: 'glob', args: { pattern: '*.nope' } })).resolves.toEqual({
      output: [],
    });
  });

  it('surfaces a bad glob/grep root (exit 2 → SDK throw) as an error result', async () => {
    e2bMocks.run
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 })
      .mockRejectedValue(
        sdkExitError(2, '', 'bash: line 1: cd: /no/such/dir: No such file or directory'),
      );
    const handle = await new E2BSandboxRuntime({ apiKey: 'test-key' }).acquire({});

    const glob = await handle.run({ tool: 'glob', args: { pattern: '*', root: '/no/such/dir' } });
    expect(glob.exit_code).toBe(2);
    expect(glob.stderr).toContain('No such file or directory');
    expect(glob.output).toEqual([]);
  });

  it('grep exit 2 with matches keeps the partial output alongside the fault', async () => {
    // grep exits 2 on ANY error even when it also matched:
    // the matches it printed must not be discarded.
    e2bMocks.run
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 })
      .mockRejectedValue(sdkExitError(2, 'src/a.ts:3:match', 'grep: ./secret: Permission denied'));
    const handle = await new E2BSandboxRuntime({ apiKey: 'test-key' }).acquire({});
    const grep = await handle.run({ tool: 'grep', args: { pattern: 'match' } });
    expect(grep.exit_code).toBe(2);
    expect(grep.output).toBe('src/a.ts:3:match');
    expect(grep.stderr).toContain('Permission denied');
  });

  it('a failing bash command (SDK throw) resolves with its real exit code and streams', async () => {
    e2bMocks.run
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 })
      .mockRejectedValue(sdkExitError(3, 'partial out', 'boom'));
    const handle = await new E2BSandboxRuntime({ apiKey: 'test-key' }).acquire({});
    await expect(handle.run({ tool: 'bash', args: { command: 'exit 3' } })).resolves.toEqual({
      stdout: 'partial out',
      stderr: 'boom',
      exit_code: 3,
    });
  });

  it('a throw without a numeric exitCode (transport fault) still propagates', async () => {
    e2bMocks.run
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 })
      .mockRejectedValue(new Error('fetch failed'));
    const handle = await new E2BSandboxRuntime({ apiKey: 'test-key' }).acquire({});
    await expect(handle.run({ tool: 'bash', args: { command: 'true' } })).rejects.toThrow(
      /fetch failed/,
    );
  });
});

describe('E2B bounded file reads', () => {
  beforeEach(() => {
    e2bMocks.create.mockReset();
    e2bMocks.run.mockReset();
    e2bMocks.read.mockReset();
    e2bMocks.create.mockResolvedValue({
      sandboxId: 'sb_e2b_read_test',
      commands: { run: e2bMocks.run },
      files: {
        write: vi.fn(),
        read: e2bMocks.read,
        list: vi.fn().mockResolvedValue([]),
      },
    });
  });

  it('uses one timed fd-helper command and never downloads the whole file', async () => {
    e2bMocks.run
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 })
      .mockResolvedValueOnce({
        stdout: JSON.stringify({
          data_base64: Buffer.from('hello', 'utf8').toString('base64'),
          total_bytes: 5,
        }),
        stderr: '',
        exitCode: 0,
      });
    const handle = await new E2BSandboxRuntime({ apiKey: 'test-key' }).acquire({});

    const page = await handle.files.readUtf8Page(
      '/mnt/inputs/document.txt',
      {},
      { readableRoots: ['/mnt/inputs'] },
    );

    expect(page.content).toBe('hello');
    expect(e2bMocks.run).toHaveBeenCalledTimes(2);
    expect(e2bMocks.read).not.toHaveBeenCalled();
  });

  it('fails sandbox setup when the ranged-read runtime prerequisite is unavailable', async () => {
    e2bMocks.run
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 })
      .mockResolvedValueOnce({
        stdout: '',
        stderr: '/bin/sh: node: not found',
        exitCode: 127,
      });
    const handle = await new E2BSandboxRuntime({ apiKey: 'test-key' }).acquire({});

    await expect(handle.prepareWritePolicy!(buildSandboxWritePolicy([]))).rejects.toThrow(
      /ranged-read prerequisite probe failed.*node: not found/,
    );
  });

  it('runs the filesystem-root preflight as one timed sandbox command', async () => {
    e2bMocks.run
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 })
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 });
    const handle = await new E2BSandboxRuntime({ apiKey: 'test-key' }).acquire({});

    await handle.prepareFilesystemRoots!(['/mnt/inputs', '/workspace/skills']);

    expect(e2bMocks.run).toHaveBeenCalledTimes(2);
    expect(e2bMocks.read).not.toHaveBeenCalled();
  });
});
