// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import {
  RetryableSessionEventError,
  PulsarSessionEventSource,
  type Event,
  type TranscriptStore,
} from '@orca/transcript-store';
import type { HarnessTurnRequest } from '@orca/harness-catalog';
import type { CodexCheckpoint } from '@orca/codex-harness';
import { Dispatcher } from '../../src/runner/dispatcher.js';
import { SessionRunner } from '../../src/runner/session-runner.js';
import type { RegistryClient, SessionRecord } from '../../src/clients/registry.js';
import type { CodexSdkHarnessOptions } from '../../src/harness/codex-sdk/index.js';
import { turnStoreFixture } from '../support/codex-turn-store.js';

const checkpoint = (text: string): CodexCheckpoint => ({
  version: 1,
  instructionsSha256: createHash('sha256').update('').digest('hex'),
  threadId: 'thread',
  files: { 'sessions/2026/09/20/rollout-thread.jsonl': Buffer.from(text).toString('base64') },
});
const first = { id: 'evt_first', kind: 'user.message', payload: { content: 'hello' } };
function gate() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
function fixture(
  options: Parameters<typeof turnStoreFixture>[0] = {},
  requiredAction = false,
  fatalFirst = false,
) {
  const callback = gate();
  const durable = turnStoreFixture({ checkpoint: checkpoint('previous'), ...options });
  const events: Event[] = [];
  let beforeRead = () => {};
  let beforeAppend: (event: Event) => Promise<void> = async () => {};
  let afterAppend: (event: Event) => Promise<void> = async () => {};
  const append = vi.fn(async (_ws: string, _id: string, batch: Event[]) => {
    for (const event of batch) {
      await beforeAppend(event);
      events.push({ ...event, seq: events.length });
      await afterAppend(event);
    }
    return {};
  });
  const store = {
    append,
    read: async function* () {
      beforeRead();
      yield* events;
    },
  } as unknown as TranscriptStore;
  const registry = {
    getExecutionOwner: async () => 'harness-server',
    harnessTurn: vi.fn(async ({ request }: { request: HarnessTurnRequest }) => {
      if (request.action.type === 'inspect') return structuredClone(durable.snapshot);
      if (request.action.type === 'claim') return durable.store.claim();
      return durable.store.update(request.action);
    }),
  } as unknown as RegistryClient;
  const dispatcher = new Dispatcher({
    groupId: 'receipt-test',
    store,
    registry,
    anthropicApiKey: '',
    openaiApiKey: 'fixture',
    modelDefault: 'unused',
  });
  const sdkSubmit = vi.fn();
  const start = async () => {
    const harness = dispatcher['buildCodexHarness']('ws_test', 'ses_test', {
      runtime_revision: 1,
      harness_state: durable.snapshot.state,
      harness_ownership_revision: durable.snapshot.ownershipRevision,
    } as SessionRecord);
    (harness as unknown as { opts: CodexSdkHarnessOptions }).opts.createWorker = (emit) => ({
      close: async () => {},
      refreshOptions: async () => {},
      handle: async (command) => {
        if (command.type === 'interrupt' || command.type === 'tool_result') callback.resolve();
        if (command.type !== 'submit') return;
        sdkSubmit();
        if (fatalFirst && sdkSubmit.mock.calls.length === 1) {
          emit({ type: 'failure', fatal: true, message: 'provider request failed' });
          emit({ type: 'done' });
          return;
        }
        if (requiredAction) {
          emit({ type: 'tool_call', id: 'native_call', name: 'lookup', arguments: {} });
          await callback.promise;
        }
        emit({
          type: 'event',
          event: {
            type: 'item.completed',
            item: { id: 'native_response', type: 'agent_message', text: 'answer' },
          },
        });
        emit({ type: 'checkpoint', checkpoint: checkpoint('finished') });
      },
    });
    const runner = new SessionRunner({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      harness,
      store,
      completionForTerminal: (ids) =>
        dispatcher['completionForTerminal']('ws_test', 'ses_test', undefined, ids),
    });
    await runner.start({
      agentSnapshot: {
        model_provider: 'openai',
        model_id: 'gpt-5.4-mini',
        allowed_tool_names: [],
        ...(requiredAction
          ? { custom_tools: [{ name: 'lookup', input_schema: { type: 'object' } }] }
          : {}),
      },
    });
    return { runner, harness };
  };
  return {
    durable,
    registry,
    events,
    append,
    dispatcher,
    sdkSubmit,
    start,
    readBefore: (hook: typeof beforeRead) => {
      beforeRead = hook;
    },
    before: (hook: typeof beforeAppend) => {
      beforeAppend = hook;
    },
    after: (hook: typeof afterAppend) => {
      afterAppend = hook;
    },
  };
}

describe('separate Codex durable response receipts', () => {
  it('does not commit native history while response append is blocked or failed', async () => {
    const h = fixture();
    const blocked = gate();
    h.before(async (event) => {
      if (event.kind === 'agent.message') await blocked.promise;
    });
    const { runner } = await h.start();
    try {
      const result = runner.submit(first).catch((error) => error);
      await vi.waitFor(() =>
        expect(h.append.mock.calls.some(([, , batch]) => batch[0]?.kind === 'agent.message')).toBe(
          true,
        ),
      );
      expect(h.durable.snapshot).toMatchObject({
        state: checkpoint('previous'),
        receipt: { phase: 'pending' },
      });
      blocked.reject(new Error('response append unavailable'));
      expect(await result).toBeInstanceOf(Error);
      expect(h.durable.snapshot.state).toEqual(checkpoint('previous'));
      expect(h.durable.snapshot.receipt?.phase).toBe('pending');
    } finally {
      await runner.stop('replica.shutting_down');
    }
  });

  it('recovers a lost commit acknowledgment without another SDK submission', async () => {
    let lose = true;
    const h = fixture({
      afterCommit: async () => {
        if (lose) {
          lose = false;
          throw new Error('commit ACK lost');
        }
      },
    });
    const a = await h.start();
    await expect(a.runner.submit(first)).rejects.toThrow('persistence');
    expect(h.durable.snapshot.receipt?.phase).toBe('ready');
    await a.runner.stop('replica.shutting_down');
    const b = await h.start();
    try {
      await b.runner.submit(first);
      expect(h.sdkSubmit).toHaveBeenCalledTimes(1);
      expect(h.durable.snapshot.receipt?.phase).toBe('settled');
      expect(h.events.filter((event) => event.kind === 'session.status_idle')).toHaveLength(1);
      expect(
        h.events.filter((event) => event.kind === 'session.user_event_completed'),
      ).toHaveLength(1);
    } finally {
      await b.runner.stop('replica.shutting_down');
    }
  });

  it('repairs only missing terminal-prefix events after a lost append acknowledgment', async () => {
    const h = fixture();
    let lost = false;
    h.after(async (event) => {
      if (!lost && event.kind === 'session.status_idle') {
        lost = true;
        throw new Error('terminal ACK lost');
      }
    });
    const a = await h.start();
    await expect(a.runner.submit(first)).rejects.toThrow();
    await a.runner.stop('replica.shutting_down');
    const terminal = h.events.find((event) => event.kind === 'session.status_idle')!;
    expect(h.events.filter((event) => event.kind === 'session.user_event_completed')).toHaveLength(
      0,
    );
    const b = await h.start();
    try {
      await b.runner.submit(first);
      expect(h.sdkSubmit).toHaveBeenCalledTimes(1);
      expect(h.events.filter((event) => event.id === terminal.id)).toEqual([terminal]);
      const marker = h.events.find((event) => event.kind === 'session.user_event_completed')!;
      expect(marker.seq).toBeGreaterThan(terminal.seq);
      expect(marker.producedAt).toBe(terminal.producedAt);
    } finally {
      await b.runner.stop('replica.shutting_down');
    }
  });

  it('keeps the next source out of the SDK until terminal and completion acknowledgments settle', async () => {
    const h = fixture();
    const blocked = gate();
    let firstTerminal = true;
    h.before(async (event) => {
      if (firstTerminal && event.kind === 'session.status_idle') {
        firstTerminal = false;
        await blocked.promise;
      }
    });
    const { runner } = await h.start();
    try {
      const a = runner.submit(first);
      await vi.waitFor(() => expect(h.durable.snapshot.receipt?.phase).toBe('ready'));
      const b = runner.submit({ ...first, id: 'evt_second' });
      await Promise.resolve();
      expect(h.sdkSubmit).toHaveBeenCalledTimes(1);
      blocked.resolve();
      await Promise.all([a, b]);
      expect(h.sdkSubmit).toHaveBeenCalledTimes(2);
      expect(h.durable.snapshot.receipt?.phase).toBe('settled');
    } finally {
      blocked.resolve();
      await runner.stop('replica.shutting_down');
    }
  });

  it('keeps required-action sources incomplete and abandons a crashed callback without replay', async () => {
    const h = fixture({}, true);
    const a = await h.start();
    await a.runner.submit(first);
    await vi.waitFor(() =>
      expect(h.events.some((event) => event.kind === 'session.status_idle')).toBe(true),
    );
    expect(h.durable.snapshot.receipt?.phase).toBe('pending');
    expect(h.events.filter((event) => event.kind === 'session.user_event_completed')).toHaveLength(
      0,
    );
    await a.runner.stop('replica.shutting_down');
    const b = await h.start();
    try {
      await b.runner.submit(first);
      expect(h.sdkSubmit).toHaveBeenCalledTimes(1);
      expect(h.durable.snapshot.state).toEqual(checkpoint('previous'));
      expect(h.events.filter((event) => event.kind === 'session.error')).toHaveLength(1);
      expect(
        h.events.filter((event) => event.kind === 'session.user_event_completed'),
      ).toHaveLength(1);
    } finally {
      await b.runner.stop('replica.shutting_down');
    }
  });

  it('completes the primary and accepted callback together without consuming a deferred user message', async () => {
    const h = fixture({}, true);
    const { runner } = await h.start();
    try {
      await runner.submit(first);
      expect(await runner.submit({ ...first, id: 'evt_later' })).toBe('deferred');
      const use = h.events.find((event) => event.kind === 'agent.custom_tool_use')!;
      await runner.submit({
        id: 'evt_callback',
        kind: 'user.custom_tool_result',
        payload: { custom_tool_use_id: use.id, content: [{ type: 'text', text: 'result' }] },
      });
      const markers = h.events.filter((event) => event.kind === 'session.user_event_completed');
      expect(
        markers.map((event) => JSON.parse(Buffer.from(event.payload).toString()).user_event_id),
      ).toEqual([first.id, 'evt_callback']);
      expect(h.durable.snapshot.receipt?.sourceIds).toEqual([first.id, 'evt_callback']);
    } finally {
      await runner.stop('replica.shutting_down');
    }
  });

  it('cold recovery completes all accepted sources and preserves later queued user messages', async () => {
    const h = fixture();
    h.events.push(
      ...h.dispatcher['completionForTerminal']('ws_test', 'ses_test', undefined, ['evt_previous'])!
        .events,
    );
    expect(await h.dispatcher['isCompletedUserEvent']('ws_test', 'ses_test', 'evt_previous')).toBe(
      true,
    );
    h.durable.snapshot.receipt = {
      turnId: first.id,
      sourceIds: [first.id, 'evt_callback', 'evt_interrupt'],
      usageEventId: 'evt_usage',
      guarded: true,
      phase: 'pending',
      terminalEventId: 'evt_terminal',
      errorEventId: 'evt_error',
      error: null,
      producedAt: new Date().toISOString(),
    };
    h.dispatcher['activeTurnUserEventIds'].set('ws_test/ses_test', [
      first.id,
      'evt_callback',
      'evt_later',
    ]);
    await h.dispatcher['recoverColdHarnessTurn'](
      'ws_test',
      'ses_test',
      h.dispatcher['lifecycleGeneration'],
    );
    expect(h.durable.snapshot).toMatchObject({
      state: checkpoint('previous'),
      receipt: { phase: 'settled', error: expect.stringContaining('abandoned') },
    });
    expect(h.sdkSubmit).not.toHaveBeenCalled();
    expect(await h.dispatcher['isCompletedUserEvent']('ws_test', 'ses_test', 'evt_previous')).toBe(
      true,
    );
    const markers = h.events.filter((event) => event.kind === 'session.user_event_completed');
    expect(
      markers.map((event) => JSON.parse(Buffer.from(event.payload).toString()).user_event_id),
    ).toEqual(['evt_previous', first.id, 'evt_callback', 'evt_interrupt']);
    expect(h.dispatcher['activeTurnUserEventIds'].get('ws_test/ses_test')).toEqual(['evt_later']);
    const next = h.dispatcher['completionForTerminal']('ws_test', 'ses_test')!;
    expect(JSON.parse(Buffer.from(next.events[0]!.payload).toString()).user_event_id).toBe(
      'evt_later',
    );
  });
  it('retires a fatal worker on the failing turn and runs the immediate follow-up in a fresh runner', async () => {
    const h = fixture({}, false, true);
    const { runner } = await h.start();
    const key = 'ws_test/ses_test';
    const generation = h.dispatcher['lifecycleGeneration'];
    h.dispatcher['runners'].set(key, runner);
    vi.spyOn(
      h.dispatcher as unknown as { refreshDeferredGuardrailUsageState(): Promise<boolean> },
      'refreshDeferredGuardrailUsageState',
    ).mockResolvedValue(true);
    try {
      await h.dispatcher['deferUserMessage'](
        'ws_test',
        'ses_test',
        key,
        first.id,
        first.payload,
        generation,
      );
      expect(
        await h.dispatcher['drainDeferredUserMessages'](
          'ws_test',
          'ses_test',
          key,
          runner,
          generation,
        ),
      ).toBe('failed');
      expect(h.dispatcher['runners'].has(key)).toBe(false);
      expect(h.durable.snapshot.receipt).toMatchObject({
        phase: 'settled',
        error: 'provider request failed',
      });
      expect(h.events.filter((e) => e.kind === 'session.error')).toHaveLength(1);
      expect(h.events.filter((e) => e.kind === 'session.status_idle')).toHaveLength(1);
      const fresh = await h.start();
      try {
        await fresh.runner.submit({ ...first, id: 'evt_immediate_follow_up' });
        expect(h.sdkSubmit).toHaveBeenCalledTimes(2);
        expect(h.durable.snapshot.receipt).toMatchObject({
          turnId: 'evt_immediate_follow_up',
          phase: 'settled',
          error: null,
        });
        expect(h.events.filter((e) => e.kind === 'agent.message')).toHaveLength(1);
      } finally {
        await fresh.runner.stop('replica.shutting_down');
      }
    } finally {
      await runner.stop('replica.shutting_down');
    }
  });

  it('retires a deferred turn with a lost commit ACK and leaves terminal repair to its receipt', async () => {
    const h = fixture({
      afterCommit: async () => {
        throw new Error('commit ACK lost');
      },
    });
    const { runner } = await h.start();
    const generation = h.dispatcher['lifecycleGeneration'];
    const key = 'ws_test/ses_test';
    h.dispatcher['runners'].set(key, runner);
    vi.spyOn(
      h.dispatcher as unknown as { refreshDeferredGuardrailUsageState(): Promise<boolean> },
      'refreshDeferredGuardrailUsageState',
    ).mockResolvedValue(true);
    try {
      await h.dispatcher['deferUserMessage'](
        'ws_test',
        'ses_test',
        key,
        first.id,
        first.payload,
        generation,
      );
      await expect(
        h.dispatcher['drainDeferredUserMessages']('ws_test', 'ses_test', key, runner, generation),
      ).rejects.toBeInstanceOf(RetryableSessionEventError);
      expect(h.dispatcher['runners'].has(key)).toBe(false);
      expect(h.durable.snapshot.receipt?.phase).toBe('ready');
      expect(h.sdkSubmit).toHaveBeenCalledOnce();
      expect(
        h.events.filter((event) =>
          ['session.error', 'session.status_idle', 'session.user_event_completed'].includes(
            event.kind,
          ),
        ),
      ).toEqual([]);
      await h.dispatcher['recoverColdHarnessTurn']('ws_test', 'ses_test', generation);
      expect(
        h.events.filter((event) => event.kind === 'session.status_idle').map((event) => event.id),
      ).toEqual([h.durable.snapshot.receipt!.terminalEventId]);
      expect(h.events.filter((event) => event.kind === 'session.error')).toEqual([]);
      expect(h.dispatcher['activeTurnUserEventIds'].get(key)).toBeUndefined();
    } finally {
      await runner.stop('replica.shutting_down');
    }
  });

  it.each(['cold', 'cached'] as const)(
    'does not replay a completed deferred source from a %s queue after its receipt has advanced',
    async (queueState) => {
      const h = fixture();
      const { runner } = await h.start();
      const generation = h.dispatcher['lifecycleGeneration'];
      const key = 'ws_test/ses_test';
      const refresh = vi
        .spyOn(
          h.dispatcher as unknown as { refreshDeferredGuardrailUsageState(): Promise<boolean> },
          'refreshDeferredGuardrailUsageState',
        )
        .mockResolvedValue(true);
      try {
        h.events.push({
          id: first.id,
          workspaceId: 'ws_test',
          sessionId: 'ses_test',
          subpath: '',
          seq: h.events.length,
          kind: first.kind,
          producedBy: 'client',
          producedAt: new Date().toISOString(),
          payload: Buffer.from(JSON.stringify(first.payload)),
          idempotencyKey: first.id,
        });
        await h.dispatcher['deferUserMessage'](
          'ws_test',
          'ses_test',
          key,
          first.id,
          first.payload,
          generation,
        );
        await runner.submit(first);
        await runner.submit({ ...first, id: 'evt_newer_turn' });
        expect(h.durable.snapshot.receipt?.turnId).toBe('evt_newer_turn');
        expect(
          h.events.some((event) => event.kind === 'session.deferred_user_message_submitted'),
        ).toBe(false);
        if (queueState === 'cold') h.dispatcher['clearDeferredUserMessages'](key);
        await expect(
          h.dispatcher['drainDeferredUserMessages']('ws_test', 'ses_test', key, runner, generation),
        ).resolves.toBe('complete');
        expect(h.sdkSubmit).toHaveBeenCalledTimes(2);
        expect(refresh).not.toHaveBeenCalled();
        expect(h.dispatcher['deferredUserMessageQueues'].get(key)?.items).toEqual([]);
      } finally {
        await runner.stop('replica.shutting_down');
      }
    },
  );
  it.each(['inspect', 'claim', 'abandon', 'read', 'append', 'settle'] as const)(
    'retains a source beyond the Pulsar poison limit when cold receipt %s fails',
    async (stage) => {
      const h = fixture();
      h.durable.snapshot.receipt = {
        turnId: first.id,
        sourceIds: [first.id],
        usageEventId: 'evt_usage',
        guarded: false,
        phase: stage === 'abandon' ? 'pending' : 'ready',
        terminalEventId: 'evt_terminal',
        errorEventId: 'evt_error',
        error: null,
        producedAt: new Date().toISOString(),
      };
      let failing = true;
      const original = h.registry.harnessTurn.bind(h.registry);
      vi.spyOn(h.registry, 'harnessTurn').mockImplementation(async (input) => {
        if (failing && input.request.action.type === stage) throw new Error('Registry unavailable');
        return original(input);
      });
      h.readBefore(() => {
        if (failing && stage === 'read') throw new Error('read unavailable');
      });
      h.before(async () => {
        if (failing && stage === 'append') throw new Error('append unavailable');
      });
      const deliver = () =>
        h.dispatcher['handleUserEvent']('ws_test', 'ses_test', first.kind, first.payload, {
          userEventId: first.id,
        });
      const failure = await deliver().catch((error: unknown) => error);
      expect(failure).toBeInstanceOf(RetryableSessionEventError);
      const source = new PulsarSessionEventSource({
        client: {} as never,
        subscription: 'receipt-retry',
      });
      const consumer = { acknowledge: vi.fn(async () => {}), negativeAcknowledge: vi.fn() };
      const message = {
        getRedeliveryCount: () => 11,
        getTopicName: () => 'receipt-topic',
        getProperties: () => ({ kind: first.kind }),
      };
      await source['handleDeliveryFailure'](consumer as never, message as never, failure);
      expect(consumer.negativeAcknowledge).toHaveBeenCalledWith(message);
      expect(consumer.acknowledge).not.toHaveBeenCalled();
      expect(h.durable.snapshot.receipt.phase).not.toBe('settled');
      failing = false;
      await expect(deliver()).resolves.toBeUndefined();
      expect(h.durable.snapshot.receipt.phase).toBe('settled');
      expect(h.events.filter((event) => event.id === 'evt_terminal')).toHaveLength(1);
      expect(
        h.events.filter((event) => event.kind === 'session.user_event_completed'),
      ).toHaveLength(1);
      expect(h.sdkSubmit).not.toHaveBeenCalled();
    },
  );

  it.each(['claim', 'read'] as const)(
    'keeps adapter startup %s failures retryable before worker creation',
    async (stage) => {
      const h = fixture();
      h.durable.snapshot.receipt = {
        turnId: first.id,
        sourceIds: [first.id],
        usageEventId: 'evt_usage',
        guarded: false,
        phase: 'ready',
        terminalEventId: 'evt_terminal',
        errorEventId: 'evt_error',
        error: null,
        producedAt: new Date().toISOString(),
      };
      if (stage === 'claim')
        vi.spyOn(h.registry, 'harnessTurn').mockRejectedValueOnce(new Error('claim unavailable'));
      else
        h.readBefore(() => {
          throw new Error('read unavailable');
        });
      await expect(h.start()).rejects.toBeInstanceOf(RetryableSessionEventError);
      expect(h.durable.snapshot.receipt.phase).toBe('ready');
      expect(h.sdkSubmit).not.toHaveBeenCalled();
      expect(h.events).toEqual([]);
    },
  );
});
