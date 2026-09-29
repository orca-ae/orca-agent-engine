// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';
import type {
  AgentEvent,
  AgentEventInput,
  AgentHarness,
  SessionStartInput,
  TerminationReason,
  UserEvent,
} from '../../src/harness/agent-harness.js';
import { withCanonicalAgentEventEnvelope } from '../../src/harness/agent-harness.js';
import { SessionRunner } from '../../src/runner/session-runner.js';
import type { SandboxHandle } from '../../src/sandbox/sandbox-runtime.js';

describe('SessionRunner event-pump failures', () => {
  it('rejects an outstanding response acknowledgment synchronously on stop', async () => {
    const harness = new QueueHarness();
    const store = new RecordingStore();
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    vi.spyOn(store, 'append').mockImplementation(async () => {
      await blocked;
      return [];
    });
    const runner = new SessionRunner({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      harness,
      store,
    });
    const resolve = vi.fn(),
      reject = vi.fn();
    await runner.start({ agentSnapshot: {} });
    harness.emit({ kind: 'agent.message', payload: {}, persistence: { resolve, reject } });
    await vi.waitFor(() => expect(store.append).toHaveBeenCalled());
    expect(resolve).not.toHaveBeenCalled();
    const stop = runner.stop('replica.shutting_down');
    expect(reject).toHaveBeenCalledOnce();
    release();
    await stop;
    expect(resolve).not.toHaveBeenCalled();
  });

  it.each([true, false])(
    'settles private usage acknowledgments when recording succeeds=%s',
    async (success) => {
      const harness = new QueueHarness();
      const runner = new SessionRunner({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        harness,
        store: new RecordingStore(),
        recordUsage: async () => {
          if (!success) throw new Error('usage unavailable');
          return {};
        },
      });
      const resolve = vi.fn(),
        reject = vi.fn();
      await runner.start({ agentSnapshot: {} });
      harness.emit({
        kind: 'agent.usage',
        payload: { usage: { input_tokens: 1, output_tokens: 1 } },
        persistence: { resolve, reject },
      });
      await vi.waitFor(() => expect(success ? resolve : reject).toHaveBeenCalledOnce());
      expect(runner['eventAcknowledgements'].size).toBe(0);
      await runner.stop('replica.shutting_down');
    },
  );

  it.each([
    ['missing', undefined],
    ['null', null],
    ['wildcard', '*'],
  ])(
    'does not coerce a runtime %s subpath to root and poisons before submit',
    async (_label, subpath) => {
      const harness = new QueueHarness({ pendingRequiredAction: true });
      const store = new RecordingStore();
      const runner = new SessionRunner({
        workspaceId: 'ws_pump_failure',
        sessionId: 'ses_pump_failure',
        harness,
        store,
      });

      await runner.start({ agentSnapshot: {} });
      harness.emitUnsafe({
        kind: 'agent.message',
        id: 'evt_invalid_subpath',
        payload: { content: 'invalid public subpath' },
        subpath: subpath as never,
      });
      await waitForPumpFailure(runner);

      expect(store.appendCalls).toBe(0);
      expect(runner.hasPendingRequiredAction()).toBe(false);
      await expect(runner.submit(userMessage())).rejects.toThrow(
        'invalid public AgentEvent subpath',
      );
      expect(harness.submitCalls).toBe(0);

      await runner.stop('error');
    },
  );

  it.each([
    ['missing', undefined],
    ['null', null],
    ['noncanonical', 'toolu_not_an_event'],
    ['empty suffix', 'evt_'],
  ])('does not replace a runtime %s ID from payload fields', async (_label, id) => {
    const harness = new QueueHarness();
    const store = new RecordingStore();
    const runner = new SessionRunner({
      workspaceId: 'ws_pump_failure',
      sessionId: 'ses_pump_failure',
      harness,
      store,
    });

    await runner.start({ agentSnapshot: {} });
    harness.emitUnsafe({
      kind: 'agent.message',
      id: id as never,
      subpath: '',
      payload: { id: 'evt_payload_fallback', uuid: 'evt_payload_uuid' },
    });
    await waitForPumpFailure(runner);

    expect(store.appendCalls).toBe(0);
    await expect(runner.submit(userMessage())).rejects.toThrow('invalid public AgentEvent id');
    expect(harness.submitCalls).toBe(0);

    await runner.stop('error');
  });

  it('races a pending submit against a store failure and rejects later submits before harness invocation', async () => {
    const harness = new BlockingSubmitHarness();
    const runner = new SessionRunner({
      workspaceId: 'ws_pump_failure',
      sessionId: 'ses_pump_failure',
      harness,
      store: new RecordingStore(new Error('transcript append failed')),
    });

    await runner.start({ agentSnapshot: {} });
    const pendingSubmit = runner.submit(userMessage());
    await harness.waitForSubmit();
    harness.emit({ kind: 'agent.message', payload: { content: 'store failure' } });

    await expect(pendingSubmit).rejects.toThrow('transcript append failed');
    await expect(runner.submit(userMessage())).rejects.toThrow('transcript append failed');
    expect(harness.submitCalls).toBe(1);

    await runner.stop('error');
  });

  it('poisons the runner when its event iterator rejects before submit', async () => {
    const harness = new RejectingEventsHarness(new Error('event stream disconnected'));
    const runner = new SessionRunner({
      workspaceId: 'ws_pump_failure',
      sessionId: 'ses_pump_failure',
      harness,
      store: new RecordingStore(),
    });

    await runner.start({ agentSnapshot: {} });
    await waitForPumpFailure(runner);

    await expect(runner.submit(userMessage())).rejects.toThrow('event stream disconnected');
    expect(harness.submitCalls).toBe(0);
    await runner.stop('error');
  });

  it('does not poison the runner when its event iterator ends normally', async () => {
    const harness = new FiniteEventsHarness();
    const runner = new SessionRunner({
      workspaceId: 'ws_pump_failure',
      sessionId: 'ses_pump_failure',
      harness,
      store: new RecordingStore(),
    });

    await runner.start({ agentSnapshot: {} });
    await vi.waitFor(() => {
      expect((runner as unknown as { pumping: boolean }).pumping).toBe(false);
    });

    await expect(runner.submit(userMessage())).resolves.toBeUndefined();
    expect(harness.submitCalls).toBe(1);
    await runner.stop('error');
  });

  it('removes a pump-failure race waiter after each successful submit', async () => {
    const runner = new SessionRunner({
      workspaceId: 'ws_pump_failure',
      sessionId: 'ses_pump_failure',
      harness: new QueueHarness(),
      store: new RecordingStore(),
    });

    await runner.start({ agentSnapshot: {} });
    await runner.submit(userMessage());
    await runner.submit(userMessage());

    expect(
      (runner as unknown as { pumpFailureWaiters: Set<unknown> }).pumpFailureWaiters.size,
    ).toBe(0);
    await runner.stop('error');
  });

  it('keeps teardown available after pump failure and runs it once', async () => {
    const harness = new QueueHarness();
    const destroy = vi.fn().mockResolvedValue(undefined);
    const runner = new SessionRunner({
      workspaceId: 'ws_pump_failure',
      sessionId: 'ses_pump_failure',
      harness,
      store: new RecordingStore(),
      sandbox: { id: 'sbx_pump_failure', destroy } as unknown as SandboxHandle,
    });

    await runner.start({ agentSnapshot: {} });
    harness.emitUnsafe({
      kind: 'agent.message',
      id: 'evt_invalid_subpath',
      payload: {},
      subpath: '*' as never,
    });
    await waitForPumpFailure(runner);

    await runner.stop('error');
    await runner.stop('error');

    expect(harness.stopReasons).toEqual(['error']);
    expect(destroy).toHaveBeenCalledTimes(1);
  });
});

class QueueHarness implements AgentHarness {
  private readonly queued: AgentEvent[] = [];
  private readonly resolvers: Array<(value: IteratorResult<AgentEvent>) => void> = [];
  private stopped = false;
  submitCalls = 0;
  readonly stopReasons: TerminationReason[] = [];

  constructor(private readonly options: { pendingRequiredAction?: boolean } = {}) {}

  async start(_input: SessionStartInput): Promise<void> {}

  async submit(_event: UserEvent): Promise<void> {
    this.submitCalls += 1;
  }

  hasPendingRequiredAction(): boolean {
    return this.options.pendingRequiredAction ?? false;
  }

  async stop(reason: TerminationReason): Promise<void> {
    this.stopReasons.push(reason);
    this.stopped = true;
    for (const resolve of this.resolvers.splice(0)) resolve({ value: undefined, done: true });
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.stopped || this.queued.length > 0) {
      const event = this.queued.shift();
      if (event) {
        yield event;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) =>
        this.resolvers.push(resolve),
      );
      if (next.done) return;
      yield next.value;
    }
  }

  emit(event: AgentEventInput): void {
    this.emitUnsafe(withCanonicalAgentEventEnvelope(event));
  }

  emitUnsafe(event: AgentEvent): void {
    const resolve = this.resolvers.shift();
    if (resolve) resolve({ value: event, done: false });
    else this.queued.push(event);
  }
}

class BlockingSubmitHarness extends QueueHarness {
  private resolveSubmit!: () => void;
  private releaseSubmit!: () => void;
  private readonly submitStarted = new Promise<void>((resolve) => {
    this.resolveSubmit = resolve;
  });
  private readonly submitBlocked = new Promise<void>((resolve) => {
    this.releaseSubmit = resolve;
  });

  override async submit(_event: UserEvent): Promise<void> {
    this.submitCalls += 1;
    this.resolveSubmit();
    await this.submitBlocked;
  }

  async waitForSubmit(): Promise<void> {
    await this.submitStarted;
  }

  override async stop(reason: TerminationReason): Promise<void> {
    this.releaseSubmit();
    await super.stop(reason);
  }
}

class RejectingEventsHarness extends QueueHarness {
  constructor(private readonly failure: Error) {
    super();
  }

  override events(): AsyncIterable<AgentEvent> {
    return {
      [Symbol.asyncIterator]: () => ({
        next: async (): Promise<IteratorResult<AgentEvent>> => {
          throw this.failure;
        },
      }),
    };
  }
}

class FiniteEventsHarness extends QueueHarness {
  override events(): AsyncIterable<AgentEvent> {
    return {
      [Symbol.asyncIterator]: () => ({
        next: async (): Promise<IteratorResult<AgentEvent>> => ({ value: undefined, done: true }),
      }),
    };
  }
}

class RecordingStore implements TranscriptStore {
  readonly appended: Event[] = [];
  appendCalls = 0;

  constructor(private readonly failure?: Error) {}

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    this.appendCalls += 1;
    if (this.failure) throw this.failure;
    this.appended.push(
      ...events.map((event, index) => ({
        ...event,
        workspaceId,
        sessionId,
        seq: this.appended.length + index + 1,
      })),
    );
    return events.map((event) => event.id);
  }

  async *read(_workspaceId: string, _sessionId: string, _opts: ReadOptions): AsyncIterable<Event> {}

  async *tail(_workspaceId: string, _sessionId: string, _opts: TailOptions): AsyncIterable<Event> {}

  async archive(): Promise<void> {}

  async close(): Promise<void> {}
}

function userMessage(): UserEvent {
  return { kind: 'user.message', payload: { content: [{ type: 'text', text: 'hello' }] } };
}

async function waitForPumpFailure(runner: SessionRunner): Promise<void> {
  await vi.waitFor(() => {
    expect((runner as unknown as { pumpFailure: unknown }).pumpFailure).not.toBeNull();
  });
}
