// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { parseProjectedTrace } from '../../src/canonical-validation.js';
import { encodeLangfuseOtlpJson } from '../../src/otlp-json.js';
import {
  CanonicalProjectionStateError,
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import { event } from '../support/events.js';

const start = () => [
  event(1, 'user.message', {}, { producedBy: 'client' }),
  event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
];
const end = () => event(2000, 'session.status_idle', { stop_reason: { type: 'end_turn' } });
const restore = (state: unknown) =>
  parseCanonicalProjectionState(JSON.parse(JSON.stringify(state)));
const thread = (i: number) => ({
  subpath: `subagents/${i % 2 === 0 ? '11111111-1111-4111-8111-111111111111' : '22222222-2222-4222-8222-222222222222'}`,
});

describe('bounded tool observation loss', () => {
  it.each(['metadata_only', 'raw_io'] as const)(
    'preserves first 256 tools across threads and late results in %s through recovery',
    (mode) => {
      const uses = Array.from({ length: 270 }, (_, i) =>
        event(i + 3, 'agent.tool_use', { tool_use_id: `call_${i}` }, thread(i)),
      );
      // Receipts for omitted calls fill pending capacity before admitted completions.
      const unmatched = Array.from({ length: 260 }, (_, i) =>
        event(i + 300, 'agent.tool_result', { tool_use_id: `call_${256 + i}` }, thread(i)),
      );
      const model = [
        event(280, 'span.model_request_start', { model_observation_kind: 'turn_model_summary' }),
        event(281, 'span.model_request_end', {
          model_observation_kind: 'turn_model_summary',
          model_request_start_id: 'evt_280',
          model_usage: { input_tokens: 19, output_tokens: 7 },
          total_cost_usd: 0.25,
        }),
      ];
      const first = reduceCanonicalEventBatch(
        initialCanonicalProjectionState(),
        [...start(), ...uses, ...model, ...unmatched],
        new Set(),
        undefined,
        mode,
      );
      expect(first.state.version).toBe(6);
      expect(first.state.activeTurn?.tools).toMatchObject({
        omittedCallEventCount: 14,
        omittedResultEventCount: 4,
      });
      const restored = restore(first.state);
      // Retained identities remain exact even after saturation.
      expect(
        reduceCanonicalEventBatch(
          restored,
          [uses[0]!, unmatched[0]!],
          new Set(first.acceptedSourceIds),
          undefined,
          mode,
        ).state,
      ).toEqual(restored);
      expect(() =>
        reduceCanonicalEventBatch(
          restored,
          [event(3, 'agent.tool_use', { tool_use_id: 'collision' }, thread(0))],
          new Set(first.acceptedSourceIds),
          undefined,
          mode,
        ),
      ).toThrow(CanonicalProjectionStateError);
      const results = Array.from({ length: 256 }, (_, i) =>
        event(
          i + 600,
          'agent.tool_result',
          { tool_use_id: `call_${i}`, content: 'output' },
          thread(i),
        ),
      );
      const completed = reduceCanonicalEventBatch(
        restored,
        results,
        new Set(first.acceptedSourceIds),
        undefined,
        mode,
      );
      expect(completed.state.activeTurn?.tools.results).toHaveLength(512);
      const downgraded = JSON.parse(JSON.stringify(completed.state));
      downgraded.version = 5;
      expect(() => restore(downgraded)).toThrow(CanonicalProjectionStateError);
      const malformed = JSON.parse(JSON.stringify(completed.state));
      for (const use of malformed.activeTurn.tools.uses) delete use.completed;
      expect(() => restore(malformed)).toThrow(CanonicalProjectionStateError);
      const final = reduceCanonicalEventBatch(
        restore(completed.state),
        [end()],
        new Set(first.acceptedSourceIds),
        undefined,
        mode,
      );
      const trace = final.completedTraces[0]!;
      expect(trace.spans.filter((span) => span.observationType === 'tool')).toHaveLength(256);
      expect(
        trace.spans.find((span) => span.observationType === 'turn_model_summary')?.modelSummary,
      ).toMatchObject({
        usage: { inputTokens: 19, outputTokens: 7 },
        totalCostUsd: 0.25,
      });
      expect(trace.spans.every((span) => span.status === 'ok')).toBe(true);
      expect(trace.root.status).toBe('ok');
      expect(trace.root.metadata).toMatchObject({
        'orca.turn.tool_observations_truncated': true,
        'orca.turn.omitted_tool_call_event_count': 14,
        'orca.turn.omitted_tool_result_event_count': 4,
      });
      expect(parseProjectedTrace(JSON.parse(JSON.stringify(trace)))).toEqual(trace);
      const attributes =
        encodeLangfuseOtlpJson(trace).resourceSpans[0]!.scopeSpans[0]!.spans[0]!.attributes;
      expect(attributes).toEqual(
        expect.arrayContaining([
          {
            key: 'langfuse.observation.metadata.orca.turn.tool_observations_truncated',
            value: { boolValue: true },
          },
          {
            key: 'langfuse.observation.metadata.orca.turn.omitted_tool_call_event_count',
            value: { intValue: '14' },
          },
          {
            key: 'langfuse.observation.metadata.orca.turn.omitted_tool_result_event_count',
            value: { intValue: '4' },
          },
        ]),
      );
      const replay = reduceCanonicalEventBatch(
        initialCanonicalProjectionState(),
        [...start(), ...uses, ...model, ...unmatched, ...results, end()],
        new Set(),
        undefined,
        mode,
      );
      expect(replay.completedTraces).toEqual(final.completedTraces);
    },
  );

  it('reconciles result-before-use at pending capacity and still admits a late completion', () => {
    const pending = Array.from({ length: 256 }, (_, i) =>
      event(i + 3, 'agent.tool_result', { tool_use_id: `pending_${i}` }),
    );
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...start(),
      ...pending,
    ]);
    const next = reduceCanonicalEventBatch(restore(first.state), [
      event(300, 'agent.tool_use', { tool_use_id: 'pending_0' }),
      event(301, 'agent.tool_result', { tool_use_id: 'new_pending' }),
      event(302, 'agent.tool_result', { tool_use_id: 'overflow' }),
      event(303, 'agent.tool_use', { tool_use_id: 'later' }),
      event(304, 'agent.tool_result', { tool_use_id: 'later' }),
    ]);
    expect(next.state.activeTurn?.tools).toMatchObject({
      unmatchedResultCount: 257,
      omittedResultEventCount: 1,
      omittedCallEventCount: 0,
    });
    const trace = reduceCanonicalEventBatch(restore(next.state), [end()]).completedTraces[0]!;
    expect(trace.spans.map((span) => span.status)).toEqual(['ok', 'ok']);
    expect(trace.spans[0]?.endedAt).toBe(trace.spans[0]?.startedAt);
  });

  it('counts accepted omitted client results but not approvals as omitted result events', () => {
    const uses = Array.from({ length: 257 }, (_, i) =>
      event(i + 3, 'agent.tool_use', { tool_use_id: `call_${i}` }),
    );
    const pending = Array.from({ length: 256 }, (_, i) =>
      event(i + 300, 'agent.tool_result', { tool_use_id: `orphan_${i}` }),
    );
    const trace = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      [
        ...start(),
        ...uses,
        ...pending,
        event(600, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
        event(
          601,
          'user.tool_confirmation',
          { tool_use_id: 'call_256', result: 'allow' },
          { producedBy: 'client' },
        ),
        event(602, 'session.user_event_processed', { user_event_id: 'evt_601' }),
        event(603, 'user.tool_result', { tool_use_id: 'call_256' }, { producedBy: 'client' }),
        event(604, 'session.user_event_processed', { user_event_id: 'evt_603' }),
        event(
          605,
          'user.tool_result',
          { tool_use_id: 'call_0', content: 'retained' },
          { producedBy: 'client' },
        ),
        event(606, 'session.user_event_processed', { user_event_id: 'evt_605' }),
        end(),
      ],
      new Set(),
      undefined,
      'raw_io',
    ).completedTraces[0]!;
    expect(trace.root.metadata['orca.turn.omitted_tool_result_event_count']).toBe(1);
    expect(trace.spans[0]?.status).toBe('ok');
    expect(trace.spans[0]?.io?.output?.json).toBe('"retained"');
  });

  it('reconciles legacy result-before-use checkpoints once, without retaining result content', () => {
    const state = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...start(),
      event(3, 'agent.tool_result', { tool_use_id: 'pending' }),
      event(4, 'agent.tool_use', { tool_use_id: 'pending' }),
    ]).state;
    const legacy = JSON.parse(JSON.stringify(state));
    delete legacy.activeTurn.tools.uses[0].completed;
    delete legacy.activeTurn.tools.omittedCallEventCount;
    delete legacy.activeTurn.tools.omittedResultEventCount;
    legacy.activeTurn.tools.unmatchedResultCount = 1;
    const repaired = restore(legacy);
    expect(repaired.activeTurn?.tools.unmatchedResultCount).toBe(0);
    expect(repaired.activeTurn?.tools.uses[0]?.completed?.status).toBe('ok');
    expect(restore(repaired)).toEqual(repaired);
    expect(reduceCanonicalEventBatch(repaired, [end()]).completedTraces[0]?.spans[0]?.status).toBe(
      'ok',
    );
  });

  it('rejects malformed overflow state and metadata, while reading legacy states/outbox', () => {
    const state = reduceCanonicalEventBatch(initialCanonicalProjectionState(), start()).state;
    for (const version of [3, 4, 5, 6]) {
      const fixture = JSON.parse(JSON.stringify(state));
      fixture.version = version;
      if (version >= 4) fixture.captureMode = 'metadata_only';
      if (version < 6) {
        delete fixture.activeTurn.tools.omittedCallEventCount;
        delete fixture.activeTurn.tools.omittedResultEventCount;
      }
      expect(restore(fixture).activeTurn?.tools.omittedCallEventCount).toBe(0);
      fixture.activeTurn.tools.omittedCallEventCount = -1;
      expect(() => restore(fixture)).toThrow(CanonicalProjectionStateError);
    }
    const current = JSON.parse(JSON.stringify(state));
    current.version = 6;
    current.captureMode = 'metadata_only';
    delete current.activeTurn.tools.omittedResultEventCount;
    expect(() => restore(current)).toThrow(CanonicalProjectionStateError);
    current.version = 7;
    expect(() => restore(current)).toThrow(CanonicalProjectionStateError);
    const trace = reduceCanonicalEventBatch(state, [end()]).completedTraces[0]!;
    expect(parseProjectedTrace(JSON.parse(JSON.stringify(trace)))).toEqual(trace);
    trace.root.metadata['orca.turn.tool_observations_truncated'] = true;
    expect(() => parseProjectedTrace(trace)).toThrow();
    trace.root.metadata['orca.turn.omitted_tool_call_event_count'] = 0;
    trace.root.metadata['orca.turn.omitted_tool_result_event_count'] = 0;
    expect(() => parseProjectedTrace(trace)).toThrow();
  });
});
