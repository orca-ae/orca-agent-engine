// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { EventEmitter } from 'node:events';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';
import { JsonRpcLineTransport } from '@deepseek-ai/dsh-sdk-protocol';
import { afterEach, expect, it, vi } from 'vitest';
import { DshSdkWorker } from '../../src/worker.js';

vi.mock('node:child_process', async (original) => ({
  ...(await original<typeof import('node:child_process')>()),
  spawn: vi.fn(),
}));
afterEach(() => vi.restoreAllMocks());

it.each(['error', 'exit'] as const)(
  'default launch handles %s followed immediately by close without stale group signals',
  async (event) => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-lifecycle-'));
    const child = Object.assign(new EventEmitter(), {
      stdout: new PassThrough(),
      stdin: new PassThrough(),
      stderr: new PassThrough(),
      pid: 123456789,
      exitCode: null as number | null,
      signalCode: null,
      kill: vi.fn(),
    });
    const spawnError = Object.assign(new Error('spawn ENOENT'), { code: 'ENOENT' });
    vi.mocked(spawn).mockImplementation(() => {
      if (event === 'error') queueMicrotask(() => child.emit('error', spawnError));
      return child as unknown as ReturnType<typeof spawn>;
    });
    const kill = vi.spyOn(process, 'kill').mockReturnValue(true);
    const peer = new JsonRpcLineTransport(child.stdin, child.stdout);
    let initializing!: () => void;
    const requested = new Promise<void>((resolve) => {
      initializing = resolve;
    });
    peer.onRequest(async () => {
      initializing();
      return { threadId: 'root', threads: [] };
    });
    peer.start();
    const emit = vi.fn();
    const worker = new DshSdkWorker(emit);
    const starting = worker.handle({
      type: 'start',
      root,
      sessionId: 'ses_lifecycle',
      modelProvider: 'deepseek',
      model: 'deepseek-v4-flash',
      system: '',
      apiKey: 'fixture',
      dshGatewayUrl: 'http://127.0.0.1:1',
      tools: [],
    });
    const settled = starting.catch((error) => error);
    try {
      if (event === 'error') {
        expect(await settled).toBeInstanceOf(Error);
        expect(emit).not.toHaveBeenCalledWith({ type: 'ready' });
      } else {
        await requested;
        await starting;
        child.exitCode = 0;
        child.emit('exit', 0, null);
      }
      const closing = worker.close();
      child.emit('close', event === 'error' ? -2 : 0, null);
      await closing;
      await settled;
      expect(kill).not.toHaveBeenCalled();
      expect(child.kill).not.toHaveBeenCalled();
      expect(await readdir(root)).toEqual([]);
      await expect(worker.handle({ type: 'submit', text: 'never run' })).rejects.toThrow();
    } finally {
      await worker.close();
      peer.close();
      child.stdout.destroy();
      child.stdin.destroy();
      child.stderr.destroy();
      await rm(root, { recursive: true, force: true });
    }
  },
);
