// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import { parseProjectedTrace } from '../../src/canonical-validation.js';
import { deterministicChildSpanId } from '../../src/ids.js';
import { event, completedPrimaryTurnEvents } from '../support/events.js';
import { encodeLangfuseOtlpJson } from '../../src/otlp-json.js';
import type { CaptureMode } from '../../src/types.js';

const children = [
  '11111111-1111-4111-8111-111111111111',
  '22222222-2222-4222-8222-222222222222',
].map((id) => 'subagents/' + id);
const label = { model_observation_kind: 'turn_model_summary' };
function treeEvents() {
  return [
    event(
      1,
      'user.message',
      { content: [{ type: 'text', text: 'root input' }] },
      { producedBy: 'client' },
    ),
    event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
    event(3, 'span.model_request_start', label),
    event(
      4,
      'session.thread_created',
      {
        parent_subpath: '',
        session_thread_id: 'sth_child1',
        parent_thread_id: 'sth_root',
        private: 'secret',
      },
      { subpath: children[0]! },
    ),
    event(5, 'session.thread_created', { parent_subpath: children[0]! }, { subpath: children[1]! }),
    event(
      6,
      'agent.tool_use',
      { tool_use_id: 'reused', name: 'read', input: { file: 'a' } },
      { subpath: children[0]! },
    ),
    event(
      7,
      'agent.tool_use',
      { tool_use_id: 'reused', name: 'read', input: { file: 'b' } },
      { subpath: children[1]! },
    ),
    event(8, 'span.model_request_start', label, { subpath: children[0]! }),
    event(9, 'span.model_request_start', label, { subpath: children[1]! }),
    event(10, 'span.model_request_end', {
      ...label,
      model_request_start_id: 'evt_3',
      model_usage: { input_tokens: 2 },
    }),
    event(
      11,
      'span.model_request_end',
      { ...label, model_request_start_id: 'evt_8', model_usage: { input_tokens: 99 } },
      { subpath: children[1]! },
    ),
    event(12, 'agent.thread_message_received', {
      content: [{ type: 'text', text: 'crosspost secret' }],
      tool_use_id: 'reused',
    }),
    event(
      13,
      'agent.message',
      { content: [{ type: 'text', text: 'child secret' }] },
      { subpath: children[0]! },
    ),
    event(14, 'session.thread_status_idle', {}, { subpath: children[0]! }),
    event(
      15,
      'agent.tool_result',
      { tool_use_id: 'reused', content: 'B', is_error: true },
      { subpath: children[1]! },
    ),
    event(
      16,
      'agent.tool_result',
      { tool_use_id: 'reused', content: 'A' },
      { subpath: children[0]! },
    ),
    event(
      17,
      'span.model_request_end',
      { ...label, model_request_start_id: 'evt_8', model_usage: { input_tokens: 3 } },
      { subpath: children[0]! },
    ),
    event(
      18,
      'span.model_request_end',
      { ...label, model_request_start_id: 'evt_9', model_usage: { input_tokens: 5 } },
      { subpath: children[1]! },
    ),
    event(19, 'agent.message', { content: [{ type: 'text', text: 'root output' }] }),
    event(20, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
  ];
}

describe('flat canonical native threads', () => {
  it.each<CaptureMode>(['metadata_only', 'raw_io'])(
    'correlates child-addressed input across restart and acceptance replay in %s',
    (mode) => {
      const events = completedPrimaryTurnEvents();
      const primary = reduceCanonicalEventBatch(
        initialCanonicalProjectionState(),
        events,
        new Set(),
        undefined,
        mode,
      );
      events[0] = { ...events[0]!, subpath: children[0]! };
      const pending = reduceCanonicalEventBatch(
        initialCanonicalProjectionState(),
        events.slice(0, 2),
        new Set(),
        undefined,
        mode,
      );
      const restored = parseCanonicalProjectionState(JSON.parse(JSON.stringify(pending.state)));
      const accepted = reduceCanonicalEventBatch(
        restored,
        [events[2]!],
        new Set(),
        undefined,
        mode,
      );
      const result = reduceCanonicalEventBatch(
        parseCanonicalProjectionState(JSON.parse(JSON.stringify(accepted.state))),
        events.slice(2),
        new Set(accepted.acceptedSourceIds),
        undefined,
        mode,
      );
      expect(accepted.issues).toEqual([]);
      expect(result.issues).toEqual([]);
      expect(result.completedTraces).toEqual(primary.completedTraces);
      expect(result.completedTraces).toHaveLength(1);
    },
  );

  it('does not treat non-client or noncanonical child messages as accepted inputs', () => {
    for (const override of [
      { subpath: children[0]!, producedBy: 'harness' as const },
      { subpath: 'subagents/not-a-native-uuid', producedBy: 'client' as const },
    ]) {
      const events = completedPrimaryTurnEvents();
      events[0] = { ...events[0]!, ...override };
      const result = reduceCanonicalEventBatch(initialCanonicalProjectionState(), events);
      expect(result.completedTraces).toEqual([]);
      expect(result.issues.map((issue) => issue.code)).toContain('unresolved_acceptance_source');
    }
  });

  it('retains 128 open tree summaries through restart, closes them, and rejects overflow', () => {
    const starts = Array.from({ length: 128 }, (_, index) =>
      event(index + 3, 'span.model_request_start', label, {
        subpath:
          index === 0
            ? ''
            : 'subagents/' + String(index).padStart(8, '0') + '-1111-4111-8111-111111111111',
      }),
    );
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      event(1, 'user.message', {}, { producedBy: 'client' }),
      event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
      ...starts,
    ]);
    const restored = parseCanonicalProjectionState(JSON.parse(JSON.stringify(first.state)));
    expect(restored.activeTurn!.openModelSummaries).toHaveLength(128);
    expect(() =>
      reduceCanonicalEventBatch(restored, [event(131, 'span.model_request_start', label)]),
    ).toThrow();
    const overflow = JSON.parse(JSON.stringify(restored));
    overflow.activeTurn.openModelSummaries.push({
      ...overflow.activeTurn.openModelSummaries[0],
      sourceEventId: 'evt_overflow',
    });
    expect(() => parseCanonicalProjectionState(overflow)).toThrow();
    const result = reduceCanonicalEventBatch(
      restored,
      [
        ...starts.map((start, index) =>
          event(
            index + 131,
            'span.model_request_end',
            {
              ...label,
              model_request_start_id: start.id,
              model_usage: { input_tokens: 1 },
            },
            { subpath: start.subpath },
          ),
        ),
        event(259, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
      ],
      new Set(first.acceptedSourceIds),
    );
    expect(result.issues).toEqual([]);
    expect(result.completedTraces).toHaveLength(1);
    const trace = result.completedTraces[0]!;
    expect(trace.spans).toHaveLength(128);
    expect(parseProjectedTrace(JSON.parse(JSON.stringify(trace)))).toEqual(trace);
  });

  it.each<CaptureMode>(['metadata_only', 'raw_io'])(
    'correlates concurrent tools and self-only summaries in %s, with restart/replay',
    (mode) => {
      const events = treeEvents();
      const first = reduceCanonicalEventBatch(
        initialCanonicalProjectionState(),
        events.slice(0, 14),
        new Set(),
        undefined,
        mode,
      );
      expect(first.completedTraces).toEqual([]);
      expect(first.state.version).toBe(5);
      const restored = parseCanonicalProjectionState(JSON.parse(JSON.stringify(first.state)));
      const next = reduceCanonicalEventBatch(
        restored,
        events.slice(14),
        new Set(first.acceptedSourceIds),
        undefined,
        mode,
      );
      const trace = next.completedTraces[0]!;
      expect(parseProjectedTrace(JSON.parse(JSON.stringify(trace)))).toEqual(trace);
      expect(trace.spans).toHaveLength(5);
      expect(trace.spans.every((span) => span.parentSpanId === trace.root.spanId)).toBe(true);
      const tools = trace.spans.filter((span) => span.observationType === 'tool');
      expect(tools.map((span) => [span.subpath, span.status])).toEqual([
        [children[0]!, 'ok'],
        [children[1]!, 'error'],
      ]);
      expect(tools[0]!.metadata).toMatchObject({
        'orca.thread.parent_subpath': '',
        'orca.thread.parent_thread_id': 'sth_root',
        'orca.thread.session_thread_id': 'sth_child1',
      });
      expect(tools[1]!.metadata['orca.thread.parent_subpath']).toBe(children[0]!);
      for (const span of trace.spans)
        expect(span.spanId).toBe(
          deterministicChildSpanId(
            trace.traceId,
            span.observationType,
            span.subpath,
            span.sourceEventId,
          ),
        );
      expect(
        trace.spans
          .filter((span) => span.modelSummary)
          .map((span) => span.modelSummary!.usage!.inputTokens),
      ).toEqual([2, 3, 5]);
      expect(JSON.stringify(trace)).not.toMatch(/child secret|crosspost secret|private/);
      if (mode === 'raw_io') {
        expect(tools.map((span) => span.io?.output?.json)).toEqual(['"A"', '"B"']);
        expect(trace.root.io?.output?.json).toContain('root output');
      } else expect(JSON.stringify(trace)).not.toMatch(/root input|root output/);
      const whole = reduceCanonicalEventBatch(
        initialCanonicalProjectionState(),
        events,
        new Set(),
        undefined,
        mode,
      );
      expect(whole.completedTraces).toEqual(next.completedTraces);
      const replay = reduceCanonicalEventBatch(
        next.state,
        events,
        new Set(['evt_1']),
        undefined,
        mode,
      );
      expect(replay.completedTraces).toEqual([]);
      const wire = JSON.stringify(encodeLangfuseOtlpJson(trace));
      expect(wire).toContain(children[0]!);
      expect(wire).toContain('orca.thread.parent_subpath');
    },
  );

  it('preserves old state/outbox output and rejects mislabeled tree records', () => {
    const events = completedPrimaryTurnEvents();
    const old = reduceCanonicalEventBatch(initialCanonicalProjectionState(), events.slice(0, 5));
    expect(old.state.version).toBe(3);
    const restored = parseCanonicalProjectionState(old.state);
    expect(restored).toEqual(old.state);
    const tail = reduceCanonicalEventBatch(restored, events.slice(5));
    const whole = reduceCanonicalEventBatch(initialCanonicalProjectionState(), events);
    expect(tail.completedTraces).toEqual(whole.completedTraces);
    expect(parseProjectedTrace(whole.completedTraces[0])).toEqual(whole.completedTraces[0]);
    const childState = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      treeEvents().slice(0, 10),
    ).state;
    expect(() => parseCanonicalProjectionState({ ...childState, version: 4 })).toThrow();
    const tree = reduceCanonicalEventBatch(initialCanonicalProjectionState(), treeEvents())
      .completedTraces[0]!;
    expect(() =>
      parseProjectedTrace({ ...tree, schemaVersion: 'orca.observability.projected-trace.v1' }),
    ).toThrow();
    expect(() =>
      parseProjectedTrace({
        ...tree,
        spans: tree.spans.map((span) => ({ ...span, parentSpanId: tree.spans[0]!.spanId })),
      }),
    ).toThrow();
  });

  it('joins primary-stream replies and approval facts to the originating child only', () => {
    const events = treeEvents().slice(0, 7);
    events.push(
      event(8, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
      event(
        9,
        'user.tool_confirmation',
        { subpath: children[1]!, tool_use_id: 'reused', result: 'allow' },
        { producedBy: 'client' },
      ),
      event(10, 'session.user_event_processed', { user_event_id: 'evt_9' }),
      event(
        11,
        'user.tool_result',
        { subpath: children[1]!, tool_use_id: 'reused', content: 'child B' },
        { producedBy: 'client' },
      ),
      event(12, 'session.user_event_processed', { user_event_id: 'evt_11' }),
      event(13, 'session.status_running'),
      event(14, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
    );
    const result = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events,
      new Set(),
      undefined,
      'raw_io',
    );
    const trace = result.completedTraces[0]!;
    const tools = trace.spans.filter((span) => span.observationType === 'tool');
    expect(tools).toHaveLength(2);
    expect(tools[0]!.status).toBe('unset');
    expect(tools[0]!.metadata['orca.tool.last_approval.result']).toBeUndefined();
    expect(tools[1]!.status).toBe('ok');
    expect(tools[1]!.metadata['orca.tool.last_approval.result']).toBe('allow');
    expect(tools[1]!.io?.output?.json).toBe('"child B"');
    expect(parseProjectedTrace(trace)).toEqual(trace);
  });

  it('keeps capture restriction sticky across child-aware restarts', () => {
    const events = treeEvents();
    const first = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events.slice(0, 16),
      new Set(),
      undefined,
      'raw_io',
    );
    const restricted = reduceCanonicalEventBatch(
      first.state,
      [],
      new Set(first.acceptedSourceIds),
      undefined,
      'metadata_only',
    );
    expect(restricted.state.version).toBe(5);
    const result = reduceCanonicalEventBatch(
      parseCanonicalProjectionState(restricted.state),
      events.slice(16),
      new Set(first.acceptedSourceIds),
      undefined,
      'raw_io',
    );
    expect(result.completedTraces[0]!.schemaVersion).toBe('orca.observability.projected-trace.v3');
    expect(JSON.stringify(result)).not.toMatch(/root input|root output|file|toolName/);
    expect(parseProjectedTrace(result.completedTraces[0])).toEqual(result.completedTraces[0]);
  });

  it('inherits the root sampling/capture pin for every child', () => {
    const result = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      treeEvents(),
      new Set(),
      {
        algorithmVersion: 'orca.observability.trace-sampling.v1',
        bindingId: 'aob_tree',
        bindingVersion: 1,
        sampleRate: 0,
      },
      'raw_io',
    );
    expect(result.completedTraces).toEqual([]);
    expect(JSON.stringify(result.state)).not.toMatch(/root input|file|sth_child1/);
  });
});
