// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import type { DshWorkerLaunchOptions } from '@orca/dsh-harness';
import { LocalSandboxRuntime } from '../../src/sandbox/seam.js';
import { parseSnapshotBody } from '../../src/snapshot.js';
import { probeDshSandboxBootstrap } from '../../src/harness/dsh-sdk/sandbox-launch.js';

const fixture = vi.hoisted(() => ({
  handle: vi.fn(),
  close: vi.fn(),
  options: undefined as DshWorkerLaunchOptions | undefined,
}));
vi.mock('node:child_process', async (original) => ({
  ...(await original<typeof import('node:child_process')>()),
  execFileSync: vi.fn(),
}));
vi.mock('@orca/dsh-harness', () => ({
  inspectDshInstallation: () => [],
  DshSdkWorker: class {
    constructor(_emit: unknown, options: DshWorkerLaunchOptions) {
      fixture.options = options;
    }
    handle = fixture.handle;
    close = fixture.close;
  },
}));

let baseDir: string;
let root: string;
let runtime: LocalSandboxRuntime;
let destroy: ReturnType<typeof vi.fn>;
let spawn: ReturnType<typeof vi.fn>;
const snapshot = parseSnapshotBody(
  Buffer.from(
    JSON.stringify({
      provider: 'dsh-sdk',
      model: { provider: 'deepseek', id: 'deepseek-v4-flash' },
      system: '',
      allowed_tool_names: [],
      allowed_mcp_server_names: [],
      tool_permissions: {},
      egress: {
        mode: 'gateway',
        gateway: { llm_base_url: 'http://127.0.0.1:1', llm_jwt: 'fixture' },
      },
    }),
  ),
);

beforeEach(() => {
  vi.resetAllMocks();
  // Exercise control flow only; these host/worker fixtures do not qualify Linux isolation.
  vi.stubGlobal('process', {
    ...process,
    platform: 'linux',
    arch: 'x64',
    versions: { ...process.versions, node: '22.23.2' },
    getuid: () => 1000,
  });
  baseDir = mkdtempSync(join(tmpdir(), 'dsh-probe-cleanup-'));
  root = baseDir;
  destroy = vi.fn(async () => {
    rmSync(root, { recursive: true, force: true });
  });
  spawn = vi.fn(async () => ({
    stdout: new PassThrough(),
    stdin: new PassThrough(),
    exited: Promise.resolve(),
    kill: vi.fn(),
  }));
  runtime = new LocalSandboxRuntime({
    harnessWorkDir: baseDir,
    allowedNetworkHosts: [],
    manager: { initialize: async () => {}, wrapWithSandbox: async (command) => command },
  });
  vi.spyOn(runtime, 'withReadOnlyRuntimePaths').mockReturnValue(runtime);
  vi.spyOn(runtime, 'acquire').mockResolvedValue({
    rootDir: () => root,
    spawn,
    destroy,
  } as unknown as Awaited<ReturnType<LocalSandboxRuntime['acquire']>>);
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  rmSync(baseDir, { recursive: true, force: true });
});

it('closes the worker before destroying the workdir on success', async () => {
  fixture.close.mockImplementation(async () => {
    expect(destroy).not.toHaveBeenCalled();
  });
  await probeDshSandboxBootstrap(snapshot, runtime, 'ses_probe');
  expect(fixture.close).toHaveBeenCalledOnce();
  expect(destroy).toHaveBeenCalledOnce();
  expect(existsSync(root)).toBe(false);
});

it.each([false, true])(
  'preserves the workdir on unconfirmed cleanup (startup failed=%s)',
  async (failed) => {
    const original = new Error('initialize failed');
    const cleanup = new Error('worker retirement failed');
    if (failed) fixture.handle.mockRejectedValue(original);
    fixture.close.mockRejectedValue(cleanup);
    const error = await probeDshSandboxBootstrap(snapshot, runtime, 'ses_probe').catch(
      (error) => error,
    );
    if (failed) {
      expect(error).toBeInstanceOf(AggregateError);
      expect(error.errors).toEqual([original, cleanup]);
    } else expect(error).toBe(cleanup);
    expect(destroy).not.toHaveBeenCalled();
    expect(existsSync(join(root, '.orca-dsh'))).toBe(true);
  },
);

it('preserves startup failure after successful cleanup', async () => {
  const original = new Error('initialize failed');
  fixture.handle.mockRejectedValue(original);
  await expect(probeDshSandboxBootstrap(snapshot, runtime, 'ses_probe')).rejects.toBe(original);
  expect(destroy).toHaveBeenCalledOnce();
});

it('aggregates startup and workdir removal failures after confirmed worker cleanup', async () => {
  const original = new Error('initialize failed');
  const cleanup = new Error('remove failed');
  fixture.handle.mockRejectedValue(original);
  destroy.mockRejectedValue(cleanup);
  const error = await probeDshSandboxBootstrap(snapshot, runtime, 'ses_probe').catch(
    (error) => error,
  );
  expect(error).toBeInstanceOf(AggregateError);
  expect(error.errors).toEqual([original, cleanup]);
  expect(fixture.close).toHaveBeenCalledOnce();
});

it.each([false, true])(
  'maps only the acquired root to virtual / (mismatch=%s)',
  async (mismatch) => {
    fixture.handle.mockImplementation(async () => {
      await fixture.options!.launch({
        executable: '/node',
        args: [],
        cwd: mismatch ? join(root, 'other') : root,
        env: {},
      });
    });
    const probe = probeDshSandboxBootstrap(snapshot, runtime, 'ses_probe');
    if (mismatch) {
      await expect(probe).rejects.toThrow('DSH bootstrap cwd must equal the sandbox root');
      expect(spawn).not.toHaveBeenCalled();
    } else {
      await probe;
      expect(spawn).toHaveBeenCalledWith(expect.any(String), {
        cwd: '/',
        env: { BASH_ENV: '/dev/null', ENV: '/dev/null' },
      });
    }
  },
);
