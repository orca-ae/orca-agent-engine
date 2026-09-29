// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  CanonicalProjectionStateError,
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import {
  isTraceSampled,
  TRACE_SAMPLING_VERSION,
  type TraceSamplingPolicy,
} from '../../src/sampling.js';
import { deterministicTraceId } from '../../src/ids.js';
import { completedPrimaryTurnEvents, event, TRANSCRIPT_SECRET } from '../support/events.js';

const policy: TraceSamplingPolicy = {
  algorithmVersion: TRACE_SAMPLING_VERSION,
  bindingId: 'aob_sampling',
  bindingVersion: 1,
  sampleRate: 0,
};

describe('sampling at the reducer acceptance boundary', () => {
  it.each([0, 0.5, 1])('replays rate %s identically across every batch boundary', (sampleRate) => {
    const pinned = { ...policy, sampleRate };
    const events = completedPrimaryTurnEvents();
    const full = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events,
      new Set(),
      pinned,
    );
    const sampled = isTraceSampled(
      pinned,
      deterministicTraceId(events[0]!.workspaceId, events[0]!.sessionId, events[0]!.id),
    );
    expect(full.completedTraces).toHaveLength(sampled ? 1 : 0);
    expect(full.state.sampling?.suppressed?.reason).toBe(sampled ? undefined : 'sampled_out');
    if (sampled) {
      expect(full.completedTraces[0]?.spans).toHaveLength(1);
      expect(full.completedTraces[0]?.spans[0]?.parentSpanId).toBe(
        full.completedTraces[0]?.root.spanId,
      );
    }
    for (let split = 0; split <= events.length; split += 1) {
      const first = reduceCanonicalEventBatch(
        initialCanonicalProjectionState(),
        events.slice(0, split),
        new Set(),
        pinned,
      );
      const restored = parseCanonicalProjectionState(JSON.parse(JSON.stringify(first.state)));
      // No fresh policy is required to finish a pinned active turn after restart.
      const second = reduceCanonicalEventBatch(
        restored,
        events.slice(split),
        new Set(first.acceptedSourceIds),
      );
      expect(second.state).toEqual(full.state);
      expect([...first.completedTraces, ...second.completedTraces]).toEqual(full.completedTraces);
      expect(JSON.stringify([first, second])).not.toContain(TRANSCRIPT_SECRET);
    }
  });

  it('does not even read model payloads or construct summaries for sampled-out turns', () => {
    const events = completedPrimaryTurnEvents();
    for (const source of events.filter((source) => source.kind.startsWith('span.model_request_'))) {
      Object.defineProperty(source, 'payload', {
        get: () => {
          throw new Error('model payload accessed before sampling');
        },
      });
    }
    const first = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events.slice(0, 7),
      new Set(),
      policy,
    );
    expect(first.state.activeTurn).toMatchObject({
      openModelSummaries: [],
      completedModelSummaries: [],
    });
    expect(first.state.activeTurn?.userId).toBeUndefined();
    const second = reduceCanonicalEventBatch(
      first.state,
      events.slice(7),
      new Set(first.acceptedSourceIds),
    );
    expect(second.completedTraces).toEqual([]);
    expect(second.state.sampling).toMatchObject({
      policy,
      suppressed: { turnCount: '1', reason: 'sampled_out' },
    });
  });

  it('coalesces many turns into one bounded identity watermark and deduplicates acceptances', () => {
    let state = initialCanonicalProjectionState();
    const accepted = new Set<string>();
    for (let turn = 0; turn < 100; turn += 1) {
      const seq = turn * 3 + 1;
      const source = event(
        seq,
        'user.message',
        { content: TRANSCRIPT_SECRET },
        { producedBy: 'client' },
      );
      const result = reduceCanonicalEventBatch(
        state,
        [
          source,
          event(seq + 1, 'session.user_event_processed', { user_event_id: source.id }),
          event(seq + 2, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
        ],
        accepted,
        policy,
      );
      state = result.state;
      result.acceptedSourceIds.forEach((id) => accepted.add(id));
      expect(result.completedTraces).toEqual([]);
      expect(state.pendingInputs).toEqual([]);
    }
    expect(state.sampling?.suppressed).toMatchObject({
      turnCount: '100',
      firstSourceSeq: '3',
      lastSourceSeq: '300',
    });
    expect(JSON.stringify(state).length).toBeLessThan(800);
    const duplicate = reduceCanonicalEventBatch(
      state,
      [event(301, 'session.user_event_processed', { user_event_id: 'evt_1' })],
      accepted,
      policy,
    );
    expect(duplicate.state).toEqual(state);
    expect(duplicate.issues).toEqual([]);
  });

  it('keeps required-action continuations in the same sampled-out trace', () => {
    const events = completedPrimaryTurnEvents();
    const first = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events.slice(0, -1),
      new Set(),
      policy,
    );
    const result = reduceCanonicalEventBatch(
      first.state,
      [
        event(8, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
        event(9, 'user.tool_result', { content: TRANSCRIPT_SECRET }, { producedBy: 'client' }),
        event(10, 'session.user_event_processed', { user_event_id: 'evt_9' }),
        event(11, 'session.status_running'),
        event(12, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
      ],
      new Set(first.acceptedSourceIds),
    );
    expect(result.issues).toEqual([]);
    expect(result.completedTraces).toEqual([]);
    expect(result.state.sampling?.suppressed).toMatchObject({
      turnCount: '1',
      lastSourceSeq: '12',
    });
  });

  it.each(['session.status_terminated', 'session.archived', 'session.deleted'])(
    'suppresses terminal %s',
    (kind) => {
      const events = completedPrimaryTurnEvents();
      const result = reduceCanonicalEventBatch(
        initialCanonicalProjectionState(),
        [...events.slice(0, -1), event(8, kind)],
        new Set(),
        policy,
      );
      expect(result.completedTraces).toEqual([]);
      expect(result.state.sampling?.suppressed?.turnCount).toBe('1');
    },
  );

  it('adopts the Registry pin for a legacy metadata-only open turn and scrubs its summaries', () => {
    const events = completedPrimaryTurnEvents();
    const legacy = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events.slice(0, -1),
    );
    // Construct the pre-tool persisted shape explicitly; new reductions write v3.
    const legacyState = parseCanonicalProjectionState({
      ...legacy.state,
      version: 1,
      activeTurn: { ...legacy.state.activeTurn, tools: undefined },
    });
    expect(legacyState.version).toBe(1);
    expect(legacy.state.activeTurn?.completedModelSummaries).toHaveLength(1);
    const result = reduceCanonicalEventBatch(
      legacyState,
      events.slice(-1),
      new Set(legacy.acceptedSourceIds),
      policy,
    );
    expect(result.state.version).toBe(3);
    expect(result.completedTraces).toEqual([]);
    expect(JSON.stringify(result.state)).not.toContain('claude-test');
  });

  it('rejects changes to an existing Session pin instead of resampling it', () => {
    const state = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      [],
      new Set(),
      policy,
    ).state;
    for (const changed of [
      { ...policy, bindingVersion: 2 },
      { ...policy, bindingId: 'aob_new' },
      { ...policy, sampleRate: 1 },
    ]) {
      expect(() => reduceCanonicalEventBatch(state, [], new Set(), changed)).toThrow(
        CanonicalProjectionStateError,
      );
    }
  });

  it('allowlists durable suppression, rejects malformed versions/counts, and saturates the bounded counter', () => {
    const result = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      completedPrimaryTurnEvents(),
      new Set(),
      policy,
    );
    const state = result.state;
    const restored = structuredClone(state);
    Object.assign(restored.sampling!.policy, { secret: TRANSCRIPT_SECRET });
    Object.assign(restored.sampling!.suppressed!, { content: TRANSCRIPT_SECRET });
    expect(parseCanonicalProjectionState(restored)).toEqual(state);
    expect(() =>
      parseCanonicalProjectionState({ ...state, version: 2, sampling: undefined }),
    ).toThrow(CanonicalProjectionStateError);
    expect(() => parseCanonicalProjectionState({ ...state, version: 4 })).toThrow(
      CanonicalProjectionStateError,
    );
    for (const turnCount of ['-1', '0', '01', '9223372036854775808']) {
      expect(() =>
        parseCanonicalProjectionState({
          ...state,
          sampling: { ...state.sampling, suppressed: { ...state.sampling!.suppressed, turnCount } },
        }),
      ).toThrow(CanonicalProjectionStateError);
    }
    state.sampling!.suppressed!.turnCount = '9223372036854775807';
    const next = reduceCanonicalEventBatch(
      state,
      [
        event(9, 'user.message', {}, { producedBy: 'client' }),
        event(10, 'session.user_event_processed', { user_event_id: 'evt_9' }),
        event(11, 'session.status_idle'),
      ],
      new Set(result.acceptedSourceIds),
    );
    expect(next.state.sampling?.suppressed?.turnCount).toBe('9223372036854775807');
  });
});
