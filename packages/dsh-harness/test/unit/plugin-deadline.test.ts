// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { PassThrough } from 'node:stream';
import type { Context } from '@deepseek-ai/cordis';
import { afterEach, expect, it, vi } from 'vitest';
import { apply } from '../../src/plugin.js';

const rpc = vi.hoisted(() => ({
  handler: undefined as
    | undefined
    | ((method: string, params: Record<string, unknown>) => Promise<unknown>),
  notify: vi.fn(),
}));
vi.mock('@deepseek-ai/dsh-sdk-protocol', () => ({
  JsonRpcLineTransport: class {
    onRequest(handler: NonNullable<typeof rpc.handler>) {
      rpc.handler = handler;
    }
    notify = rpc.notify;
    start() {}
    close() {}
  },
}));
vi.mock('../../src/pi-adapter.js', () => ({
  registerManagedPi: () => ({ api: 'anthropic-messages', baseUrl: 'http://gateway.test' }),
}));

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  rpc.notify.mockClear();
});

it('real plugin targeted submit survives 600s and bounds child prompt at budget plus drain headroom', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
  // Keep process stdio out of the fixture; execute apply and its actual registered RPC handler.
  vi.spyOn(process.stdin, 'pipe').mockReturnValue(new PassThrough());
  vi.spyOn(AbortSignal, 'timeout').mockImplementation((ms) => {
    const controller = new AbortController();
    setTimeout(() => controller.abort(), ms);
    return controller.signal;
  });
  const childId = '11111111-1111-4111-8111-111111111111';
  const agent = { id: '', whenIdle: vi.fn(async () => {}), cancel: vi.fn() };
  let signal!: AbortSignal;
  const prompt = vi.fn((_request: unknown, abort: AbortSignal) => {
    signal = abort;
    return new Promise<void>((_resolve, reject) => {
      abort.addEventListener('abort', () => reject(abort.reason), { once: true });
    });
  });
  let dispose!: () => Promise<void>;
  const ctx = {
    get: () => undefined,
    on: () => () => {},
    effect: (start: () => () => Promise<void>) => {
      dispose = start();
    },
    agents: {
      create: async ({ sessionId }: { sessionId: string }) => {
        agent.id = sessionId;
        return { agent, dispose: async () => {} };
      },
      get: (id: string) => (id === agent.id ? agent : undefined),
    },
    subagents: {
      listDescendants: async () => [
        { id: childId, parentId: agent.id, kind: 'child', mode: 'continuable' },
      ],
      listChildren: async () => [],
      drainChildren: vi.fn(async () => {}),
      drainDescendants: async () => {},
      prompt,
    },
  };
  apply(ctx as unknown as Context);
  try {
    await rpc.handler!('initialize', {
      sessionId: 'ses_deadline',
      modelProvider: 'anthropic',
      model: 'claude-sonnet-4-6',
      apiKey: 'fixture-token',
      dshGatewayUrl: 'http://gateway.test',
      archivedThreadSubpaths: [],
      system: '',
      tools: [],
    });
    const submitted = rpc.handler!('submit', {
      text: 'follow up',
      sourceEventId: 'evt_child_budget',
      threadId: childId,
      executionBudgetMs: 1_200_000,
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(prompt).toHaveBeenCalledWith(
      expect.objectContaining({
        childSessionId: childId,
        parentSessionId: agent.id,
        delivery: 'queue',
        mode: 'continuable',
      }),
      signal,
    );
    await vi.advanceTimersByTimeAsync(600_001);
    expect(signal.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(629_998);
    expect(signal.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(signal.aborted).toBe(true);
    await submitted;
    expect(agent.cancel).toHaveBeenCalledOnce();
    expect(ctx.subagents.drainChildren).toHaveBeenCalledOnce();
    expect(rpc.notify).toHaveBeenCalledWith(
      'orca.event',
      expect.objectContaining({ type: 'failure', fatal: true }),
    );
    expect(rpc.notify).toHaveBeenLastCalledWith('orca.event', { type: 'done' });
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    await dispose();
  }
});
