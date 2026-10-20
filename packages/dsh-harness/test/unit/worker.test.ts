// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { PassThrough } from 'node:stream';
import { JsonRpcLineTransport } from '@deepseek-ai/dsh-sdk-protocol';
import { expect, it, vi } from 'vitest';
import { DshSdkWorker } from '../../src/worker.js';
import { exerciseNativeTree } from '../support/native-tree.js';

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
