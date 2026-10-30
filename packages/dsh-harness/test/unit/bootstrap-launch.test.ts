// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect, it, vi } from 'vitest';
import { DshSdkWorker, type DshLaunchInput } from '../../src/worker.js';
import { inspectDshInstallation } from '../../src/installation.js';

it('checks the actual pinned installation before bootstrap', () => {
  const paths = inspectDshInstallation();
  expect(paths.some((path) => path.includes('@deepseek-ai'))).toBe(true);
  expect(paths.some((path) => path.endsWith('node_modules'))).toBe(true);
});

it('initializes the actual pinned profile through injected stdio, without a model/tool request', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-bootstrap-'));
  let requests = 0;
  const endpoint = createServer((_request, response) => {
    requests++;
    response.end('{}');
  });
  endpoint.listen(0, '127.0.0.1');
  await once(endpoint, 'listening');
  const address = endpoint.address();
  if (!address || typeof address === 'string') throw new Error('missing fixture port');
  const emit = vi.fn();
  const launch = vi.fn(async (input: DshLaunchInput) => {
    // Real pinned DSH process; this test supplies no OS isolation and is not qualification.
    const child = spawn(input.executable, input.args, {
      cwd: input.cwd,
      env: input.env,
      stdio: 'pipe',
    });
    child.stderr.resume();
    const exited = new Promise<void>((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', () => resolve());
    });
    return {
      stdout: child.stdout,
      stdin: child.stdin,
      exited,
      kill: (signal: NodeJS.Signals) => {
        child.kill(signal);
      },
    };
  });
  const worker = new DshSdkWorker(emit, { homeRoot: root, launch });
  try {
    await worker.handle({
      type: 'start',
      root,
      sessionId: 'ses_bootstrap',
      modelProvider: 'deepseek',
      model: 'deepseek-v4-flash',
      system: 'Bootstrap only.',
      apiKey: 'fixture-only',
      dshGatewayUrl: `http://127.0.0.1:${address.port}`,
      tools: [],
    });
    expect(emit).toHaveBeenCalledWith({ type: 'ready' });
    expect(emit.mock.calls.some(([event]) => event.type === 'tool_call')).toBe(false);
    expect(requests).toBe(0);
    const input = launch.mock.calls[0]![0];
    expect(input.args.slice(-2)).toEqual(['--profile', 'orca-managed']);
    expect(input.env.DSH_HOME!.startsWith(root + '/')).toBe(true);
    expect(Object.keys(input.env).sort()).toEqual([
      'DSH_HOME',
      'DSH_TELEMETRY_DISABLED',
      'HOME',
      'ORCA_DSH_SYSTEM',
      'PATH',
    ]);
    await worker.close();
    expect(await readdir(root)).toEqual([]);
  } finally {
    await worker.close();
    await new Promise<void>((resolve) => endpoint.close(() => resolve()));
    await rm(root, { recursive: true, force: true });
  }
}, 45000);

it('does not fall back to host spawn after an injected sandbox launch fails', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-bootstrap-failure-'));
  const emit = vi.fn();
  const launch = vi.fn(async () => {
    throw new Error('SRT launch failed');
  });
  const worker = new DshSdkWorker(emit, { homeRoot: root, launch });
  try {
    await expect(
      worker.handle({
        type: 'start',
        root,
        sessionId: 'ses_bootstrap',
        modelProvider: 'deepseek',
        model: 'deepseek-v4-flash',
        system: '',
        apiKey: 'fixture-only',
        dshGatewayUrl: 'http://127.0.0.1:1',
        tools: [],
      }),
    ).rejects.toThrow('SRT launch failed');
    expect(launch).toHaveBeenCalledOnce();
    expect(emit).not.toHaveBeenCalledWith({ type: 'ready' });
    expect(await readdir(root)).toEqual([]);
  } finally {
    await worker.close();
    await rm(root, { recursive: true, force: true });
  }
});
