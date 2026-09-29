// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type { ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';
import {
  GuardrailUsageUnavailableError,
  type AgentEvent,
  type AgentHarness,
  type SessionStartInput,
  type TerminationReason,
  type UserEvent,
} from '../../src/harness/agent-harness.js';
import { SessionRunner } from '../../src/runner/session-runner.js';

function emptyAsyncIterable<T>(): AsyncIterable<T> {
  return { async *[Symbol.asyncIterator](): AsyncGenerator<T> {} };
}

function stubStore(): TranscriptStore {
  return {
    append: async () => [],
    read: (_workspaceId: string, _sessionId: string, _opts: ReadOptions) => emptyAsyncIterable(),
    tail: (_workspaceId: string, _sessionId: string, _opts: TailOptions) => emptyAsyncIterable(),
    archive: async () => {},
    close: async () => {},
  } satisfies TranscriptStore;
}

class UsageQueueHarness implements AgentHarness {
  readonly submitted: UserEvent[] = [];
  readonly appliedUsageStates: Array<Readonly<Record<string, unknown>>> = [];
  private readonly queue: AgentEvent[] = [];
  private readonly waiters: Array<(result: IteratorResult<AgentEvent>) => void> = [];
  private stopped = false;

  async start(_input: SessionStartInput): Promise<void> {}

  async submit(event: UserEvent): Promise<void> {
    this.submitted.push(event);
  }

  applyGuardrailUsageState(state: Readonly<Record<string, unknown>>): void {
    this.appliedUsageStates.push(state);
  }

  emit(event: AgentEvent): void {
    const waiter = this.waiters.shift();
    if (waiter) waiter({ done: false, value: event });
    else this.queue.push(event);
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.stopped || this.queue.length > 0) {
      const queued = this.queue.shift();
      if (queued) {
        yield queued;
        continue;
      }
      const next = await new Promise<IteratorResult<AgentEvent>>((resolve) => {
        this.waiters.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }

  async stop(_reason: TerminationReason): Promise<void> {
    this.stopped = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter({ done: true, value: undefined as unknown as AgentEvent });
    }
  }
}

describe('SessionRunner usage acknowledgement', () => {
  it('blocks actions, then retries the same idempotent delta before resuming', async () => {
    const harness = new UsageQueueHarness();
    const recordUsage = vi
      .fn()
      .mockRejectedValueOnce(new Error('registry unavailable'))
      .mockRejectedValueOnce(new Error('registry still unavailable'))
      .mockResolvedValueOnce({ total_tokens: 15, daily_cost_usd: 0.25 });
    const runner = new SessionRunner({
      workspaceId: 'ws_usage_lock',
      sessionId: 'ses_usage_lock',
      harness,
      store: stubStore(),
      recordUsage,
    });
    await runner.start({ agentSnapshot: {} });
    harness.emit({
      id: 'evt_usage_delta',
      kind: 'agent.usage',
      payload: {
        model: 'claude-test',
        subagent_id: 'agt_researcher',
        turn_event_id: 'evt_user_turn',
        usage: { input_tokens: 12, output_tokens: 3 },
      },
    });
    await vi.waitFor(() => expect(recordUsage).toHaveBeenCalledTimes(1));
    expect(runner.hasPendingGuardrailUsage()).toBe(true);

    const action = {
      id: 'evt_next_turn',
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'continue' }] },
    };
    await expect(runner.submit(action)).rejects.toBeInstanceOf(GuardrailUsageUnavailableError);
    expect(harness.submitted).toEqual([]);

    await expect(runner.submit(action)).resolves.toBeUndefined();
    expect(recordUsage).toHaveBeenCalledTimes(3);
    for (const call of recordUsage.mock.calls) {
      expect(call).toEqual([
        {
          cache_creation: {
            ephemeral_1h_input_tokens: 0,
            ephemeral_5m_input_tokens: 0,
          },
          cache_read_input_tokens: 0,
          input_tokens: 12,
          output_tokens: 3,
        },
        'claude-test',
        'agt_researcher',
        'evt_user_turn',
        'evt_usage_delta',
      ]);
    }
    expect(harness.appliedUsageStates).toEqual([{ total_tokens: 15, daily_cost_usd: 0.25 }]);
    expect(harness.submitted).toEqual([action]);
    expect(runner.hasPendingGuardrailUsage()).toBe(false);

    await runner.stop('client.archived');
  });
});
