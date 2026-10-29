// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { PassThrough } from 'node:stream';
import { JsonRpcLineTransport } from '@deepseek-ai/dsh-sdk-protocol';
import { expect, it, vi } from 'vitest';
import { DshSdkWorker } from '../../src/worker.js';
import { exerciseNativeTree } from '../support/native-tree.js';

it.each([undefined, 1_200_000])(
  'native submit respects remaining budget=%s and retains cancellation headroom',
  async (executionBudgetMs) => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
    vi.spyOn(AbortSignal, 'timeout').mockImplementation((ms) => {
      const controller = new AbortController();
      setTimeout(() => controller.abort(), ms);
      return controller.signal;
    });
    const emit = vi.fn();
    let signal!: AbortSignal;
    let finish!: () => void;
    const request = vi.fn((_method: string, _params: unknown, abort: AbortSignal) => {
      signal = abort;
      return new Promise<void>((resolve, reject) => {
        finish = resolve;
        abort.addEventListener('abort', () => reject(abort.reason), { once: true });
      });
    });
    const worker = new DshSdkWorker(emit);
    Object.assign(worker, { input: {}, rpc: { request }, reap: vi.fn(async () => {}) });
    try {
      const turn = worker.handle({
        type: 'submit',
        text: 'test',
        sourceEventId: 'evt_budget',
        ...(executionBudgetMs === undefined ? {} : { executionBudgetMs }),
      });
      await vi.advanceTimersByTimeAsync(600_000);
      if (executionBudgetMs === undefined) {
        await turn;
        expect(signal.aborted).toBe(true);
        expect(emit).toHaveBeenCalledWith(
          expect.objectContaining({ type: 'failure', fatal: true }),
        );
      } else {
        await vi.advanceTimersByTimeAsync(600_000);
        expect(signal.aborted).toBe(false);
        expect(emit).not.toHaveBeenCalled();
        expect(request.mock.calls[0]?.[1]).toMatchObject({ executionBudgetMs });
        finish();
        await turn;
      }
    } finally {
      finish();
      await worker.close();
      vi.restoreAllMocks();
      vi.useRealTimers();
    }
  },
);

it.each([0, -1, 1.5, 2_147_483_648])(
  'rejects budget %s before native admission',
  async (executionBudgetMs) => {
    const request = vi.fn();
    const worker = new DshSdkWorker(() => {});
    Object.assign(worker, { input: {}, rpc: { request } });
    await expect(
      worker.handle({
        type: 'submit',
        text: 'test',
        sourceEventId: 'evt_budget',
        executionBudgetMs,
      }),
    ).rejects.toThrow('Invalid SDK execution budget');
    expect(request).not.toHaveBeenCalled();
  },
);

it.each(['list_child_sessions', 'read_child_session'])(
  'reserves %s against managed tool collisions before spawning a worker',
  async (name) => {
    const worker = new DshSdkWorker(() => {});
    await expect(
      worker.handle({
        type: 'start',
        root: '/unused',
        sessionId: 'session',
        model: 'deepseek-v4-flash',
        system: '',
        apiKey: 'jwt',
        dshGatewayUrl: 'http://gateway.test',
        archivedThreadSubpaths: [],
        tools: [{ name, inputSchema: { type: 'object' } }],
      }),
    ).rejects.toThrow('Reserved DSH native tool name');
    await worker.close();
  },
);

it('official Claude adapter preserves scoped refresh, native children and cold restore', async () => {
  const failures: string[] = [];
  try {
    await exerciseNativeTree(
      (event) => {
        if (event.type === 'failure') failures.push(event.message);
      },
      { provider: 'anthropic', id: 'claude-sonnet-4-6' },
    );
  } catch (cause) {
    throw new Error('Official adapter fixture: ' + failures.join('; '), { cause });
  }
}, 45000);

it('published DSH runs parallel spawn/fork, control, scoped tools and a cold native tree', async () => {
  const timeout = vi.spyOn(AbortSignal, 'timeout');
  try {
    await exerciseNativeTree();
    expect(timeout).toHaveBeenCalledWith(15000);
  } finally {
    timeout.mockRestore();
  }
}, 45000);

it.each([false, true])(
  'settles interrupt before reaping a failed submit (reject=%s)',
  async (reject) => {
    let finishSubmit!: () => void;
    let acknowledgeInterrupt!: () => void;
    const timeout = new Error('interrupt deadline exceeded');
    const submitted = new Promise<void>((resolve) => {
      finishSubmit = resolve;
    });
    const interrupted = new Promise<void>((resolve, rejectInterrupt) => {
      acknowledgeInterrupt = () => (reject ? rejectInterrupt(timeout) : resolve());
    });
    const reap = vi.fn(async () => {});
    const worker = new DshSdkWorker(() => {});
    // Control response ordering without relying on OS pipe scheduling.
    Object.assign(worker, {
      input: {},
      rpc: { request: (method: string) => (method === 'submit' ? submitted : interrupted) },
      reap,
    });
    const turn = worker.handle({ type: 'submit', text: 'test', sourceEventId: 'evt_test' });
    const control = worker.handle({ type: 'interrupt' });
    const settled = Promise.allSettled([turn, control]);
    Object.assign(worker, { fatal: true });
    finishSubmit();
    try {
      await new Promise((resolve) => setImmediate(resolve));
      expect(reap).not.toHaveBeenCalled();
    } finally {
      acknowledgeInterrupt();
      await settled;
    }
    const [turnResult, controlResult] = await settled;
    expect(turnResult.status).toBe('fulfilled');
    expect(controlResult.status).toBe(reject ? 'rejected' : 'fulfilled');
    if (controlResult.status === 'rejected') expect(controlResult.reason.cause).toBe(timeout);
    expect(reap).toHaveBeenCalled();
  },
);

it('keeps the published RPC transport alive when submit replies before interrupt', async () => {
  const requests = new PassThrough();
  const responses = new PassThrough();
  const rpc = new JsonRpcLineTransport(responses, requests);
  const peer = new JsonRpcLineTransport(requests, responses);
  let finishSubmit!: () => void;
  let acknowledgeInterrupt!: () => void;
  peer.onRequest(
    (method) =>
      new Promise((resolve) => {
        if (method === 'submit') finishSubmit = () => resolve({});
        else acknowledgeInterrupt = () => resolve({});
      }),
  );
  rpc.start();
  peer.start();
  const worker = new DshSdkWorker(() => {});
  const reap = vi.fn(async () => rpc.close());
  Object.assign(worker, { input: {}, rpc, reap });
  const turn = worker.handle({ type: 'submit', text: 'test', sourceEventId: 'evt_test' });
  const control = worker.handle({ type: 'interrupt' });
  const settled = Promise.allSettled([turn, control]);
  Object.assign(worker, { fatal: true });
  finishSubmit();
  try {
    // Let submit's response retire the turn before the peer sends its interrupt ACK.
    await new Promise((resolve) => setImmediate(resolve));
    acknowledgeInterrupt();
    const [turnResult, controlResult] = await settled;
    expect(turnResult.status).toBe('fulfilled');
    // Immediate reap rejects this request with "JSON-RPC transport closed", not a timeout.
    expect(controlResult).toEqual({ status: 'fulfilled', value: undefined });
    expect(reap).toHaveBeenCalledOnce();
  } finally {
    acknowledgeInterrupt();
    await settled;
    rpc.close();
    peer.close();
    requests.destroy();
    responses.destroy();
  }
});
