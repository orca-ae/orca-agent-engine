// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type { Event } from '@orca/transcript-store-types';
import { ClaudeAgentSdkHarness } from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';
import { mapSandboxEvent } from '../../src/harness/in-sandbox/event-mapper.js';
import {
  initialCanonicalProjectionState,
  projectCanonicalTurns,
  reduceCanonicalEventBatch,
} from '../../../observability-exporter/src/projector.js';
import {
  event,
  SESSION_ID,
  TRANSCRIPT_SECRET,
  WORKSPACE_ID,
} from '../../../observability-exporter/test/support/events.js';

// Exercise the real separate producer's mapping methods, without starting an SDK query,
// network, or sandbox. The narrow private seam is deliberate: no copied mapper or VM.
function separateProducer() {
  const harness = new ClaudeAgentSdkHarness({
    apiKey: '',
    modelDefault: 'unused',
    workspaceId: WORKSPACE_ID,
    sessionId: SESSION_ID,
    adapter: new ClaudeAgentSdkAdapter(
      {
        append: async () => [],
        async *read() {},
        async *tail() {},
        archive: async () => {},
        close: async () => {},
      },
      WORKSPACE_ID,
    ),
  });
  const mapping = harness as unknown as {
    emitAssistantToolUse(block: unknown): void;
    emitToolResults(message: unknown): Promise<void>;
    outQueue: AgentEvent[];
  };
  return { harness, mapping };
}

function persisted(produced: readonly AgentEvent[], start = 4): Event[] {
  return produced.map((value, index) =>
    event(start + index, value.kind, value.payload as Record<string, unknown>, {
      id: value.id,
      subpath: value.subpath ?? '',
    }),
  );
}

function opened(): Event[] {
  return [
    event(1, 'user.message', {}, { id: 'evt_turn', producedBy: 'client' }),
    event(2, 'session.user_event_processed', { user_event_id: 'evt_turn' }),
    event(3, 'session.status_running'),
  ];
}

function terminal(seq = 30): Event {
  return event(seq, 'session.status_idle', { stop_reason: { type: 'end_turn' } });
}

function expectTool(events: Event[], family: string, outcome: string) {
  const traces = projectCanonicalTurns(events);
  expect(traces).toHaveLength(1);
  const trace = traces[0]!;
  expect(trace.spans).toHaveLength(1);
  expect(trace.spans[0]).toMatchObject({
    observationType: 'tool',
    name: 'orca.agent.tool',
    parentSpanId: trace.root.spanId,
    metadata: { 'orca.tool.family': family, 'orca.tool.outcome': outcome },
  });
  expect(JSON.stringify(traces)).not.toContain(TRANSCRIPT_SECRET);
  return trace;
}

describe('actual tool producers conform to canonical projection', () => {
  it.each(['local', 'mcp'] as const)(
    'separate %s uses envelope identity, not the SDK call ID',
    async (family) => {
      const { mapping } = separateProducer();
      mapping.emitAssistantToolUse({
        id: 'sdk_call',
        name: family === 'mcp' ? 'mcp__remote__lookup' : 'Read',
        input: { secret: TRANSCRIPT_SECRET },
      });
      await mapping.emitToolResults({
        message: {
          content: [{ type: 'tool_result', tool_use_id: 'sdk_call', content: TRANSCRIPT_SECRET }],
        },
      });
      const [use, result] = mapping.outQueue;
      expect(use!.id).not.toBe('sdk_call');
      expect(use!.payload).not.toHaveProperty('tool_use_id');
      expect(result!.payload).toHaveProperty('tool_use_id', use!.id);
      expect(result!.id).not.toBe(use!.id);
      expectTool([...opened(), ...persisted(mapping.outQueue), terminal()], family, 'success');
    },
  );

  it.each([false, true])('in-sandbox preserves explicit correlation and error=%s', (isError) => {
    const raw = { session_id: SESSION_ID, created_at: '2026-01-01T00:00:04.000Z' };
    const use = mapSandboxEvent({
      ...raw,
      type: 'agent.tool_use',
      id: 'evt_wire_use',
      tool_use_id: 'sdk_call',
      name: 'Read',
      input: { secret: TRANSCRIPT_SECRET },
    });
    const result = mapSandboxEvent({
      ...raw,
      type: 'agent.tool_result',
      id: 'evt_wire_result',
      tool_use_id: 'sdk_call',
      content: TRANSCRIPT_SECRET,
      is_error: isError,
    });
    expect(use[0]!.payload).toHaveProperty('tool_use_id', 'sdk_call');
    expect(result[0]!.id).toBe('evt_wire_result');
    expectTool(
      [...opened(), ...persisted([...use, ...result]), terminal()],
      'local',
      isError ? 'error' : 'success',
    );
  });

  it('in-sandbox fallback ID is the use envelope ID', () => {
    const raw = { session_id: SESSION_ID, created_at: '2026-01-01T00:00:04.000Z' };
    const use = mapSandboxEvent({
      ...raw,
      type: 'agent.tool_use',
      id: 'evt_fallback',
      name: 'Read',
    });
    const result = mapSandboxEvent({
      ...raw,
      type: 'agent.tool_result',
      id: 'evt_result',
      tool_use_id: 'evt_fallback',
    });
    expect(use[0]!.payload).toHaveProperty('tool_use_id', 'evt_fallback');
    expectTool([...opened(), ...persisted([...use, ...result]), terminal()], 'local', 'success');
  });

  it.each(['user.tool_result', 'user.tool_confirmation'])(
    'separate client-executed local tool distinguishes %s from completion',
    (kind) => {
      const { harness, mapping } = separateProducer();
      const request = harness.requestAgentToolUse('Read', { secret: TRANSCRIPT_SECRET });
      const source = event(
        10,
        kind,
        { tool_use_id: request.id, result: 'allow', content: TRANSCRIPT_SECRET },
        { id: 'evt_client_local', producedBy: 'client' },
      );
      const prefix = [...opened(), ...persisted(mapping.outQueue), source];
      expectTool([...prefix, terminal()], 'local', 'incomplete');
      const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), prefix);
      const tail = [
        event(11, 'session.user_event_processed', { user_event_id: source.id }),
        terminal(),
      ];
      const second = reduceCanonicalEventBatch(
        JSON.parse(JSON.stringify(first.state)),
        tail,
        new Set(first.acceptedSourceIds),
      );
      const expected = expectTool(
        [...prefix, ...tail],
        'local',
        kind === 'user.tool_result' ? 'success' : 'incomplete',
      );
      expect(second.issues).toEqual([]);
      expect(second.completedTraces).toEqual([expected]);
    },
  );

  it('accepts the exact custom result, not a nearer queued error result', () => {
    const { harness, mapping } = separateProducer();
    const request = harness.requestCustomToolUse('lookup', {});
    const good = event(
      10,
      'user.custom_tool_result',
      { custom_tool_use_id: request.id, is_error: false },
      { id: 'evt_good', producedBy: 'client' },
    );
    const queued = event(
      11,
      'user.custom_tool_result',
      { custom_tool_use_id: request.id, is_error: true },
      { id: 'evt_queued_error', producedBy: 'client' },
    );
    const prefix = [...opened(), ...persisted(mapping.outQueue), good, queued];
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), prefix);
    const tail = [
      event(12, 'session.user_event_processed', { user_event_id: good.id }),
      terminal(),
    ];
    const second = reduceCanonicalEventBatch(
      JSON.parse(JSON.stringify(first.state)),
      tail,
      new Set(first.acceptedSourceIds),
    );
    const expected = expectTool([...prefix, ...tail], 'custom', 'success');
    expect(second.issues).toEqual([]);
    expect(second.completedTraces).toEqual([expected]);
    expect(expected.spans[0]!.endedAt).toBe(good.producedAt);
  });

  for (const producer of ['separate', 'in-sandbox'] as const) {
    for (const response of [
      'accepted',
      'unaccepted',
      'wrong-field',
      'confirmation',
      'ordinary-result',
    ] as const) {
      it(`${producer} custom boundary requires exact accepted custom result: ${response}`, () => {
        let produced: AgentEvent[];
        if (producer === 'separate') {
          const { harness, mapping } = separateProducer();
          harness.requestCustomToolUse('lookup', { secret: TRANSCRIPT_SECRET });
          produced = mapping.outQueue;
        } else {
          produced = mapSandboxEvent({
            type: 'agent.custom_tool_use',
            id: 'evt_custom',
            session_id: SESSION_ID,
            created_at: '2026-01-01T00:00:04.000Z',
            name: 'lookup',
            input: { secret: TRANSCRIPT_SECRET },
          });
        }
        const use = produced[0]!;
        expect(use.kind).toBe('agent.custom_tool_use');
        expect(use.payload).toHaveProperty('id', use.id);
        expect(use.payload).not.toHaveProperty('tool_use_id');
        const kind =
          response === 'confirmation'
            ? 'user.tool_confirmation'
            : response === 'ordinary-result'
              ? 'user.tool_result'
              : 'user.custom_tool_result';
        const key = ['wrong-field', 'confirmation', 'ordinary-result'].includes(response)
          ? 'tool_use_id'
          : 'custom_tool_use_id';
        const source = event(
          10,
          kind,
          { [key]: use.id, content: TRANSCRIPT_SECRET, result: 'allow' },
          { id: 'evt_response', producedBy: 'client' },
        );
        const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
          ...opened(),
          ...persisted(produced),
          source,
        ]);
        expect(first.completedTraces).toEqual([]);
        expect(first.issues).toEqual([]);
        expect(JSON.stringify(first.state)).not.toContain(TRANSCRIPT_SECRET);
        // Persistence/restart boundary: acceptance arrives in a different reducer batch.
        const accepted = response !== 'unaccepted';
        const tail = [
          ...(accepted
            ? [event(11, 'session.user_event_processed', { user_event_id: source.id })]
            : []),
          terminal(),
        ];
        const second = reduceCanonicalEventBatch(
          JSON.parse(JSON.stringify(first.state)),
          tail,
          new Set(first.acceptedSourceIds),
        );
        expect(second.issues).toEqual([]);
        const expected = expectTool(
          [...opened(), ...persisted(produced), source, ...tail],
          'custom',
          response === 'accepted' ? 'success' : 'incomplete',
        );
        expect(second.completedTraces).toEqual([expected]);
        if (response === 'accepted') {
          expect(expected.spans[0]!.endedAt).toBe(source.producedAt);
        }
      });
    }
  }
});
