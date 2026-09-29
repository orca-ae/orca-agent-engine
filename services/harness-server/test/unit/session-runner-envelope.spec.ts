// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';
import type {
  AgentEvent,
  AgentHarness,
  SessionStartInput,
} from '../../src/harness/agent-harness.js';
import { SessionRunner } from '../../src/runner/session-runner.js';

describe('SessionRunner AgentEvent envelope mapping', () => {
  it('hands the session-scoped MCP JWT source to the harness without resolving it', async () => {
    const harness = new FakeHarness([]);
    const start = vi.spyOn(harness, 'start');
    const mcpJwtProvider = { getValidToken: vi.fn(), close: vi.fn() };
    const runner = new SessionRunner({
      workspaceId: 'ws_envelope',
      sessionId: 'ses_envelope',
      store: new RecordingStore(),
      harness,
      mcpJwtProvider,
    });
    try {
      await runner.start({ agentSnapshot: {} });
      expect(start.mock.calls[0]![0].mcpJwtProvider).toBe(mcpJwtProvider);
      expect(mcpJwtProvider.getValidToken).not.toHaveBeenCalled();
    } finally {
      await runner.stop('idle.timeout');
    }
  });

  it('preserves strict canonical envelopes without deriving identity from payload fields', async () => {
    const store = new RecordingStore();
    const runner = new SessionRunner({
      workspaceId: 'ws_envelope',
      sessionId: 'ses_envelope',
      store,
      harness: new FakeHarness([
        {
          kind: 'agent.message',
          id: 'evt_envelope_message',
          subpath: 'subagents/reviewer',
          payload: {
            id: 'evt_payload_message',
            uuid: 'evt_payload_uuid',
            tool_use_id: 'evt_related_tool_use',
            content: 'review complete',
          },
        },
        {
          kind: 'agent.tool_result',
          id: 'evt_result_envelope',
          subpath: '',
          payload: {
            id: 'evt_payload_result',
            tool_use_id: 'evt_related_tool_use',
            content: 'result',
          },
        },
        {
          kind: 'agent.tool_result',
          id: 'evt_relation_envelope',
          subpath: '',
          payload: {
            tool_use_id: 'evt_related_tool_use_only',
            event_id: 'evt_related_event_only',
            event: { id: 'evt_nested_event_only' },
            model_request_start_id: 'evt_model_start_only',
            content: 'relation only',
          },
        },
      ]),
    });

    await runner.start({ agentSnapshot: {} });
    await vi.waitFor(() => expect(store.appended).toHaveLength(3));
    await runner.stop('idle.timeout');

    const [canonical, result, relationOnly] = store.appended;
    expect(canonical).toMatchObject({
      id: 'evt_envelope_message',
      subpath: 'subagents/reviewer',
      idempotencyKey: 'evt_envelope_message',
    });
    expect(payloadOf(canonical!)).toMatchObject({
      id: 'evt_payload_message',
      uuid: 'evt_payload_uuid',
      tool_use_id: 'evt_related_tool_use',
    });

    expect(result).toMatchObject({
      id: 'evt_result_envelope',
      subpath: '',
      idempotencyKey: 'evt_result_envelope',
    });
    expect(payloadOf(result!)).toMatchObject({
      id: 'evt_payload_result',
      tool_use_id: 'evt_related_tool_use',
    });

    expect(relationOnly).toMatchObject({
      id: 'evt_relation_envelope',
      subpath: '',
      idempotencyKey: 'evt_relation_envelope',
    });
    expect(payloadOf(relationOnly!)).toMatchObject({
      tool_use_id: 'evt_related_tool_use_only',
      event_id: 'evt_related_event_only',
      event: { id: 'evt_nested_event_only' },
      model_request_start_id: 'evt_model_start_only',
    });
  });
});

class FakeHarness implements AgentHarness {
  constructor(private readonly emitted: AgentEvent[]) {}

  async start(_input: SessionStartInput): Promise<void> {}

  async submit(): Promise<void> {}

  async stop(): Promise<void> {}

  async *events(): AsyncIterable<AgentEvent> {
    yield* this.emitted;
  }
}

class RecordingStore implements TranscriptStore {
  readonly appended: Event[] = [];

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
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

function payloadOf(event: Event): Record<string, unknown> {
  return JSON.parse(Buffer.from(event.payload).toString('utf8')) as Record<string, unknown>;
}
