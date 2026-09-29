// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { deterministicChildSpanId } from '../../src/ids.js';
import { outcomeIdentityKey } from '../../src/event-identity.js';
import {
  CanonicalProjectionStateError,
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  projectCanonicalTurns,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import { encodeLangfuseOtlpJson } from '../../src/otlp-json.js';
import { TRACE_SAMPLING_VERSION } from '../../src/sampling.js';
import { event, TRANSCRIPT_SECRET } from '../support/events.js';

const start = (seq = 3, payload: Record<string, unknown> = {}, id = 'evt_eval') =>
  event(
    seq,
    'span.outcome_evaluation_start',
    { id, outcome_id: 'outcome_test', iteration: 0, ...payload },
    { id },
  );
const end = (seq = 4, payload: Record<string, unknown> = {}) =>
  event(seq, 'span.outcome_evaluation_end', {
    outcome_evaluation_start_id: 'evt_eval',
    outcome_id: 'outcome_test',
    iteration: 0,
    result: 'satisfied',
    ...payload,
  });
const prefix = () => [
  event(1, 'user.message', {}, { producedBy: 'client' }),
  event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
];
const terminal = () => event(999, 'session.status_idle', { stop_reason: { type: 'end_turn' } });

describe('metadata-only outcome evaluators', () => {
  it('projects a deterministic evaluator and restores at every batch boundary without content', () => {
    const events = [
      ...prefix(),
      start(),
      end(4, {
        explanation: TRANSCRIPT_SECRET,
        usage: { input_tokens: 0 },
        model: TRANSCRIPT_SECRET,
      }),
      terminal(),
    ];
    const [trace] = projectCanonicalTurns(events);
    expect(trace!.spans).toHaveLength(1);
    expect(trace!.spans[0]).toMatchObject({
      observationType: 'outcome_evaluation',
      name: 'orca.agent.outcome_evaluation',
      parentSpanId: trace!.root.spanId,
      sourceEventId: 'evt_eval',
      metadata: {
        'orca.outcome.iteration': 0,
        'orca.outcome.result': 'satisfied',
      },
    });
    expect(trace!.spans[0]!.spanId).toBe(
      deterministicChildSpanId(trace!.traceId, 'outcome_evaluation', '', 'evt_eval'),
    );
    for (let split = 0; split <= events.length; split++) {
      const first = reduceCanonicalEventBatch(
        initialCanonicalProjectionState(),
        events.slice(0, split),
      );
      const second = reduceCanonicalEventBatch(
        parseCanonicalProjectionState(JSON.parse(JSON.stringify(first.state))),
        events.slice(split),
        new Set(first.acceptedSourceIds),
      );
      expect([...first.completedTraces, ...second.completedTraces]).toEqual([trace]);
      expect(JSON.stringify([first, second])).not.toContain(TRANSCRIPT_SECRET);
    }
    const wire = encodeLangfuseOtlpJson(trace!);
    expect(JSON.stringify(trace)).not.toContain('outcome_test');
    expect(JSON.stringify(trace)).not.toContain('outcome_digest_');
    expect(JSON.stringify(wire)).not.toContain('orca.outcome.id');
    expect(wire.resourceSpans[0]!.scopeSpans[0]!.spans[1]!.attributes).toContainEqual({
      key: 'langfuse.observation.type',
      value: { stringValue: 'evaluator' },
    });
    expect(JSON.stringify(wire)).not.toMatch(
      /usage_details|gen_ai|cost_details|explanation|observation.input|observation.output/,
    );
  });

  it.each(['satisfied', 'needs_revision', 'max_iterations_reached', 'failed'])(
    'keeps the producer verdict %s (only failed is execution error)',
    (result) => {
      const [trace] = projectCanonicalTurns([...prefix(), start(), end(4, { result }), terminal()]);
      expect(trace!.spans[0]?.metadata['orca.outcome.result']).toBe(result);
      expect(trace!.spans[0]?.status).toBe(result === 'failed' ? 'error' : 'ok');
      expect(trace!.root.status).toBe('ok');
    },
  );

  it.each([
    { outcome_evaluation_start_id: undefined },
    { outcome_evaluation_start_id: 'evt_other' },
    { outcome_id: 'outcome_other' },
    { iteration: 1 },
    { iteration: -1 },
    { iteration: 0.5 },
    { result: 'not_satisfied' },
    { result: TRANSCRIPT_SECRET },
  ])('requires exact valid correlation and verdict %j', (payload) => {
    const [trace] = projectCanonicalTurns([...prefix(), start(), end(4, payload), terminal()]);
    expect(trace!.spans).toEqual([]);
  });

  it('does not consume a start on a mismatched end and deduplicates completed starts across restart', () => {
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...prefix(),
      start(),
      end(4, { iteration: 1 }),
      end(5),
    ]);
    const second = reduceCanonicalEventBatch(JSON.parse(JSON.stringify(first.state)), [
      start(6),
      end(7),
      terminal(),
    ]);
    expect(second.completedTraces[0]!.spans).toHaveLength(1);
  });

  it('does not infer starts, export open starts, or attach other subpaths/unaccepted events', () => {
    expect(projectCanonicalTurns([start(), end(), terminal()])).toEqual([]);
    for (const pair of [
      [end()],
      [start()],
      [{ ...start(), subpath: 'child' }, end()],
      [start(), { ...end(), subpath: 'child' }],
    ]) {
      expect(projectCanonicalTurns([...prefix(), ...pair, terminal()])[0]!.spans).toEqual([]);
    }
  });

  it('retains the contract interrupted verdict without inferring an execution failure', () => {
    const [trace] = projectCanonicalTurns([
      ...prefix(),
      start(),
      end(4, { result: 'interrupted' }),
      terminal(),
    ]);
    expect(trace!.spans[0]).toMatchObject({
      status: 'unset',
      metadata: { 'orca.outcome.result': 'interrupted' },
    });
    expect(
      encodeLangfuseOtlpJson(trace!).resourceSpans[0]!.scopeSpans[0]!.spans[1]!.status.code,
    ).toBe(0);
    const checkpoint = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...prefix(),
      start(),
      end(4, { result: 'interrupted' }),
    ]);
    expect(
      parseCanonicalProjectionState(checkpoint.state).activeTurn?.completedEvaluations?.[0]?.status,
    ).toBe('unset');
    for (const status of ['ok', 'error']) {
      const raw = JSON.parse(JSON.stringify(checkpoint.state));
      raw.activeTurn.completedEvaluations[0].status = status;
      expect(() => parseCanonicalProjectionState(raw)).toThrow(CanonicalProjectionStateError);
    }
  });

  it.each([
    { id: 'evt_conflict' },
    { outcome_id: '' },
    { outcome_id: 'x'.repeat(513) },
    { iteration: Number.MAX_SAFE_INTEGER + 1 },
  ])('ignores malformed start %j', (payload) => {
    expect(
      projectCanonicalTurns([...prefix(), start(3, payload), end(), terminal()])[0]!.spans,
    ).toEqual([]);
  });

  it('distinguishes concurrent evaluations of the same outcome and iteration by start ID', () => {
    const [trace] = projectCanonicalTurns([
      ...prefix(),
      start(),
      start(4, {}, 'evt_other'),
      end(5, { outcome_evaluation_start_id: 'evt_other' }),
      end(6),
      terminal(),
    ]);
    expect(trace!.spans.map((span) => span.sourceEventId)).toEqual(['evt_other', 'evt_eval']);
    expect(new Set(trace!.spans.map((span) => span.spanId)).size).toBe(2);
  });

  it('bounds live reducer growth and leaves the supplied checkpoint unchanged on overflow', () => {
    const base = reduceCanonicalEventBatch(initialCanonicalProjectionState(), prefix()).state;
    expect(() =>
      reduceCanonicalEventBatch(
        base,
        Array.from({ length: 65 }, (_, i) => start(3 + i, {}, `evt_open_${i}`)),
      ),
    ).toThrow(CanonicalProjectionStateError);
    expect(() =>
      reduceCanonicalEventBatch(
        base,
        Array.from({ length: 257 }, (_, i) => [
          start(3 + 2 * i, {}, `evt_done_${i}`),
          end(4 + 2 * i, { outcome_evaluation_start_id: `evt_done_${i}` }),
        ]).flat(),
      ),
    ).toThrow(CanonicalProjectionStateError);
    expect(base.activeTurn?.openEvaluations).toBeUndefined();
  });

  it('scrubs arbitrary checkpoint fields and rejects invalid evaluator metadata', () => {
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...prefix(),
      start(),
    ]);
    const raw = JSON.parse(JSON.stringify(first.state));
    raw.activeTurn.openEvaluations[0].explanation = TRANSCRIPT_SECRET;
    expect(JSON.stringify(parseCanonicalProjectionState(raw))).not.toContain(TRANSCRIPT_SECRET);
    const completed = reduceCanonicalEventBatch(first.state, [end()]);
    for (const patch of [
      { modelSummary: { usage: { inputTokens: 0 } } },
      {
        metadata: {
          ...completed.state.activeTurn!.completedEvaluations![0]!.metadata,
          explanation: TRANSCRIPT_SECRET,
        },
      },
    ]) {
      const restored = JSON.parse(JSON.stringify(completed.state));
      Object.assign(restored.activeTurn.completedEvaluations[0], patch);
      expect(() => parseCanonicalProjectionState(restored)).toThrow(CanonicalProjectionStateError);
    }
  });

  it('accepts pre-evaluator checkpoints but rejects raw legacy evaluator state and malformed keys', () => {
    const legacy = reduceCanonicalEventBatch(initialCanonicalProjectionState(), prefix()).state;
    expect(parseCanonicalProjectionState(JSON.parse(JSON.stringify(legacy)))).toEqual(legacy);
    const open = reduceCanonicalEventBatch(legacy, [start()]).state;
    for (const key of [
      undefined,
      'outcome_test',
      'outcome_digest_short',
      outcomeIdentityKey('test')!.toUpperCase(),
    ]) {
      const raw = JSON.parse(JSON.stringify(open));
      raw.activeTurn.openEvaluations[0].outcomeKey = key;
      expect(() => parseCanonicalProjectionState(raw)).toThrow(CanonicalProjectionStateError);
    }
    const raw = JSON.parse(JSON.stringify(open));
    delete raw.activeTurn.openEvaluations[0].outcomeKey;
    raw.activeTurn.openEvaluations[0].outcomeId = 'outcome_test';
    expect(() => parseCanonicalProjectionState(raw)).toThrow(CanonicalProjectionStateError);
    const completed = reduceCanonicalEventBatch(open, [end()]).state;
    for (const field of ['outcomeId', 'outcomeKey']) {
      const restored = JSON.parse(JSON.stringify(completed));
      restored.activeTurn.completedEvaluations[0][field] = outcomeIdentityKey('outcome_test');
      expect(() => parseCanonicalProjectionState(restored)).toThrow(CanonicalProjectionStateError);
    }
  });

  it('gates evaluator payload access on sampling and scrubs an adopted old turn', () => {
    const first = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...prefix(),
      start(),
    ]);
    const source = end();
    Object.defineProperty(source, 'payload', {
      get() {
        throw new Error('payload accessed');
      },
    });
    const second = reduceCanonicalEventBatch(first.state, [source], new Set(), {
      algorithmVersion: TRACE_SAMPLING_VERSION,
      bindingId: 'aob_test',
      bindingVersion: 1,
      sampleRate: 0,
    });
    expect(second.state.activeTurn?.openEvaluations ?? []).toEqual([]);
    expect(second.state.activeTurn?.completedEvaluations ?? []).toEqual([]);
    const sampledOutStart = start(5);
    Object.defineProperty(sampledOutStart, 'payload', {
      get() {
        throw new Error('start payload accessed');
      },
    });
    expect(() => reduceCanonicalEventBatch(second.state, [sampledOutStart])).not.toThrow();
  });

  it('bounds persisted open and completed evaluators', () => {
    const initial = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...prefix(),
      start(),
    ]);
    const state = JSON.parse(JSON.stringify(initial.state));
    state.activeTurn.openEvaluations = Array(65).fill(state.activeTurn.openEvaluations?.[0]);
    expect(() => parseCanonicalProjectionState(state)).toThrow(CanonicalProjectionStateError);
    const completed = reduceCanonicalEventBatch(initial.state, [end()]);
    const restored = JSON.parse(JSON.stringify(completed.state));
    restored.activeTurn.completedEvaluations = Array(257).fill(
      restored.activeTurn.completedEvaluations?.[0],
    );
    expect(() => parseCanonicalProjectionState(restored)).toThrow(CanonicalProjectionStateError);
  });

  it.each([
    'outcome_customer_email_alice_example_com',
    'outc_customer_secret_token',
    'outcome_prose with spaces',
    'outcome_secret\n',
    'outcome_中文',
    'Bearer secret',
    'sk-ant-api03-secret',
    'outcome_secret=value',
    'outcome_secret/path',
  ])('hashes private outcome identity %s and rejects raw checkpoints', (outcomeId) => {
    const inputs = [
      ...prefix(),
      start(3, { outcome_id: outcomeId }),
      end(4, { outcome_id: outcomeId }),
    ];
    const checkpoint = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      inputs.slice(0, -1),
    );
    expect(checkpoint.state.activeTurn?.openEvaluations?.[0]).toMatchObject({
      outcomeKey: outcomeIdentityKey(outcomeId),
    });
    expect(JSON.stringify(checkpoint.state)).not.toContain(outcomeId);
    const reduced = reduceCanonicalEventBatch(
      parseCanonicalProjectionState(checkpoint.state),
      inputs.slice(-1),
    );
    expect(reduced.state.activeTurn?.openEvaluations ?? []).toEqual([]);
    expect(reduced.state.activeTurn?.completedEvaluations).toHaveLength(1);
    expect(JSON.stringify(reduced.state)).not.toContain(outcomeId);
    expect(JSON.stringify(reduced.state)).not.toContain('outcome_digest_');
    const open = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...prefix(),
      start(),
    ]);
    const rawOpen = JSON.parse(JSON.stringify(open.state));
    rawOpen.activeTurn.openEvaluations[0].outcomeId = outcomeId;
    expect(() => parseCanonicalProjectionState(rawOpen)).toThrow(CanonicalProjectionStateError);
    delete rawOpen.activeTurn.openEvaluations[0].outcomeId;
    rawOpen.activeTurn.openEvaluations[0].outcomeKey = outcomeId;
    expect(() => parseCanonicalProjectionState(rawOpen)).toThrow(CanonicalProjectionStateError);
    const completed = reduceCanonicalEventBatch(open.state, [end()]);
    const rawCompleted = JSON.parse(JSON.stringify(completed.state));
    rawCompleted.activeTurn.completedEvaluations[0].metadata['orca.outcome.id'] = outcomeId;
    expect(() => parseCanonicalProjectionState(rawCompleted)).toThrow(
      CanonicalProjectionStateError,
    );
  });

  it.each([
    'spanId',
    'parentSpanId',
    'workspaceId',
    'sessionId',
    'anchorEventId',
    'duplicateOpen',
    'duplicateCompleted',
    'overlap',
  ])('rejects tampered evaluator checkpoint %s', (mutation) => {
    const open = reduceCanonicalEventBatch(initialCanonicalProjectionState(), [
      ...prefix(),
      start(),
    ]);
    const completed = reduceCanonicalEventBatch(open.state, [end()]);
    const raw = JSON.parse(JSON.stringify(completed.state));
    const active = raw.activeTurn;
    if (mutation === 'spanId' || mutation === 'parentSpanId')
      active.completedEvaluations[0][mutation] = '1234567890abcdef';
    else if (mutation === 'workspaceId') active.workspaceId = 'ws_other';
    else if (mutation === 'sessionId') active.sessionId = 'ses_other';
    else if (mutation === 'anchorEventId') active.anchorEventId = 'evt_other';
    else if (mutation === 'duplicateOpen')
      active.openEvaluations = [
        open.state.activeTurn!.openEvaluations![0],
        open.state.activeTurn!.openEvaluations![0],
      ];
    else if (mutation === 'duplicateCompleted')
      active.completedEvaluations.push(active.completedEvaluations[0]);
    else active.openEvaluations = open.state.activeTurn!.openEvaluations;
    expect(() => parseCanonicalProjectionState(raw)).toThrow(CanonicalProjectionStateError);
  });
});
