// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from 'vitest';
import { E2BSandboxRuntime } from '../../src/sandbox/e2b/runtime.js';

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
    expect(e2bMocks.run).toHaveBeenNthCalledWith(
      2,
      expect.stringMatching(/\/proc\/self\/fd\/.*\/mnt\/inputs\/document\.txt/s),
      {
        envs: {
          BASH_ENV: '',
          ENV: '',
          NODE_OPTIONS: '',
          PATH: '/usr/local/bin:/usr/bin',
        },
        timeoutMs: 10_000,
      },
    );
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

    await expect(
      handle.prepareWritePolicy!({ writablePaths: [], readonlyPaths: [] }),
    ).rejects.toThrow(/ranged-read prerequisite probe failed.*node: not found/);
    expect(e2bMocks.run).toHaveBeenCalledTimes(2);
    expect(e2bMocks.run).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('/proc/self/fd/'),
      expect.objectContaining({
        envs: expect.objectContaining({ NODE_OPTIONS: '', PATH: '/usr/local/bin:/usr/bin' }),
        timeoutMs: 10_000,
      }),
    );
  });

  it('runs the filesystem-root preflight as one timed sandbox command', async () => {
    e2bMocks.run
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 })
      .mockResolvedValueOnce({ stdout: '', stderr: '', exitCode: 0 });
    const handle = await new E2BSandboxRuntime({ apiKey: 'test-key' }).acquire({});

    await handle.prepareFilesystemRoots!(['/mnt/inputs', '/workspace/skills']);

    expect(e2bMocks.run).toHaveBeenCalledTimes(2);
    expect(e2bMocks.run).toHaveBeenNthCalledWith(
      2,
      expect.stringMatching(/\/proc\/self\/mountinfo.*\/mnt\/inputs.*\/workspace\/skills/s),
      {
        envs: {
          BASH_ENV: '',
          ENV: '',
          NODE_OPTIONS: '',
          PATH: '/usr/local/bin:/usr/bin',
        },
        timeoutMs: 10_000,
      },
    );
    expect(e2bMocks.read).not.toHaveBeenCalled();
  });
});
