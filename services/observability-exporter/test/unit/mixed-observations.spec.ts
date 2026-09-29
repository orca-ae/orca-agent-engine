// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Pool } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import { outcomeIdentityKey } from '../../src/event-identity.js';
import { deterministicChildSpanId, deterministicTraceId } from '../../src/ids.js';
import { encodeLangfuseOtlpJson } from '../../src/otlp-json.js';
import { ObservabilityExporterRepository } from '../../src/persistence.js';
import {
  initialCanonicalProjectionState,
  parseCanonicalProjectionState,
  projectCanonicalTurns,
  reduceCanonicalEventBatch,
} from '../../src/projector.js';
import { isTraceSampled, TRACE_SAMPLING_VERSION } from '../../src/sampling.js';
import { MAX_PROJECTED_TOOLS_PER_TURN } from '../../src/tool-projection.js';
import {
  MAX_PROJECTED_EVALUATIONS_PER_TURN,
  MAX_PROJECTED_MODEL_SUMMARIES_PER_TURN,
  type ProjectedTrace,
} from '../../src/types.js';
import { SESSION_ID, TRANSCRIPT_SECRET, WORKSPACE_ID } from '../support/events.js';
import {
  mixedDeliveryContext,
  mixedObservationEvents,
  PRIVATE_OUTCOME,
} from '../support/mixed-observations.js';

const roundTrip = <T>(value: T): T => JSON.parse(JSON.stringify(value));
const policy = (sampleRate: number) => ({
  algorithmVersion: TRACE_SAMPLING_VERSION,
  bindingId: 'aob_mixed',
  bindingVersion: 1,
  sampleRate,
});
const mixedTrace = () => projectCanonicalTurns(mixedObservationEvents())[0]!;

function assertPublic(value: unknown) {
  expect(JSON.stringify(value)).not.toContain(PRIVATE_OUTCOME);
  expect(JSON.stringify(value)).not.toContain(TRANSCRIPT_SECRET);
  expect(JSON.stringify(value)).not.toMatch(/outcomeKey|outcome_digest_|orca.outcome.id/);
}

function outbox(trace: ProjectedTrace) {
  const row = {
    id: '1',
    organization_id: mixedDeliveryContext.organizationId,
    workspace_id: trace.workspaceId,
    session_id: trace.sessionId,
    binding_id: mixedDeliveryContext.bindingId,
    binding_version: 1,
    trace_id: trace.traceId,
    canonical_trace: trace,
    delivery_context: mixedDeliveryContext,
    delivery_attempt_count: '0',
    lease_owner: 'mixed',
    lease_generation: '1',
  };
  const query = vi.fn(async (sql: string, _values?: unknown[]) => ({
    rows: sql.includes('SELECT o.binding_id')
      ? [{ binding_id: mixedDeliveryContext.bindingId }]
      : sql.includes('WITH candidate AS')
        ? [row]
        : [],
    rowCount: 1,
  }));
  const repository = new ObservabilityExporterRepository({
    query,
    connect: async () => ({ query, release: vi.fn() }),
  } as unknown as Pool);
  const state = initialCanonicalProjectionState();
  return {
    query,
    claim: () => repository.claimOutbox('mixed', 30_000),
    write: () =>
      repository.completeProjection(
        {
          workspaceId: trace.workspaceId,
          sessionId: trace.sessionId,
          nextSeq: '0',
          firstPendingSeq: '1',
          state,
          leaseOwner: 'mixed',
          leaseGeneration: '1',
        },
        '18',
        state,
        [{ trace, deliveryContext: mixedDeliveryContext }],
      ),
  };
}

describe('combined model, tool and evaluator boundaries', () => {
  it('retains all families and the pending receipt across every pair of serialized batch boundaries', () => {
    const events = mixedObservationEvents();
    const expected = mixedTrace();
    expect(expected.spans.map((span) => span.observationType).sort()).toEqual([
      'outcome_evaluation',
      'outcome_evaluation',
      'tool',
      'turn_model_summary',
      'turn_model_summary',
    ]);
    const checkpoint = reduceCanonicalEventBatch(
      initialCanonicalProjectionState(),
      events.slice(0, 12),
    );
    const restored = parseCanonicalProjectionState(roundTrip(checkpoint.state));
    expect(restored.pendingInputs[0]).toMatchObject({
      eventId: 'evt_11',
      toolTraceId: expected.traceId,
      toolResult: { family: 'local', outcome: 'error' },
    });
    expect(restored.activeTurn?.tools.uses).toHaveLength(1);
    expect(restored.activeTurn?.openModelSummaries).toHaveLength(1);
    expect(restored.activeTurn?.completedModelSummaries).toHaveLength(1);
    expect(restored.activeTurn?.completedEvaluations).toHaveLength(1);
    expect(restored.activeTurn?.openEvaluations).toEqual([
      expect.objectContaining({
        sourceEventId: 'evt_8',
        outcomeKey: outcomeIdentityKey(PRIVATE_OUTCOME),
      }),
    ]);
    assertPublic(restored.activeTurn?.completedEvaluations);
    const contaminated = roundTrip(restored);
    Object.assign(contaminated.activeTurn!.completedEvaluations![0]!, {
      outcomeKey: outcomeIdentityKey(PRIVATE_OUTCOME),
    });
    expect(() => parseCanonicalProjectionState(contaminated)).toThrow();
    for (let first = 0; first <= events.length; first++) {
      for (let second = first; second <= events.length; second++) {
        let state = initialCanonicalProjectionState();
        const accepted = new Set<string>();
        const traces: ProjectedTrace[] = [];
        for (const batch of [
          events.slice(0, first),
          events.slice(first, second),
          events.slice(second),
        ]) {
          const reduced = reduceCanonicalEventBatch(state, batch, accepted);
          expect(reduced.issues).toEqual([]);
          reduced.acceptedSourceIds.forEach((id) => accepted.add(id));
          traces.push(...reduced.completedTraces);
          state = parseCanonicalProjectionState(roundTrip(reduced.state));
          expect(JSON.stringify(state)).not.toContain(PRIVATE_OUTCOME);
          expect(JSON.stringify(state)).not.toContain(TRANSCRIPT_SECRET);
        }
        expect(traces).toEqual([expected]);
        assertPublic(traces);
      }
    }
  });

  it.each([0, 0.5, 1])(
    'reads client control linkage but gates agent tool/evaluator payloads at sample rate %s',
    (sampleRate) => {
      const decisions = new Set<boolean>();
      for (let turn = 0; turn < 12; turn++) {
        const events = mixedObservationEvents().map((source) => ({
          ...source,
          sessionId: `${SESSION_ID}_${turn}`,
        }));
        const sampled = isTraceSampled(
          policy(sampleRate),
          deterministicTraceId(WORKSPACE_ID, events[0]!.sessionId, 'evt_1'),
        );
        decisions.add(sampled);
        const reads = new Map<string, number>();
        for (const source of events.filter(
          (e) => e.kind.includes('tool') || /outcome_evaluation_(start|end)$/.test(e.kind),
        )) {
          const payload = source.payload;
          reads.set(source.id, 0);
          Object.defineProperty(source, 'payload', {
            get() {
              reads.set(source.id, reads.get(source.id)! + 1);
              return payload;
            },
          });
        }
        let state = initialCanonicalProjectionState();
        const accepted = new Set<string>();
        const traces: ProjectedTrace[] = [];
        for (const source of events) {
          const reduced = reduceCanonicalEventBatch(state, [source], accepted, policy(sampleRate));
          expect(reduced.issues).toEqual([]);
          reduced.acceptedSourceIds.forEach((id) => accepted.add(id));
          traces.push(...reduced.completedTraces);
          state = parseCanonicalProjectionState(roundTrip(reduced.state));
          expect(JSON.stringify(state)).not.toContain(PRIVATE_OUTCOME);
          expect(JSON.stringify(state)).not.toContain(TRANSCRIPT_SECRET);
          if (!sampled) {
            expect(JSON.stringify(state)).not.toMatch(
              /correlationKey|toolResult|toolTraceId|outcomeKey/,
            );
            if (state.activeTurn !== null) {
              expect(state.activeTurn.tools.uses).toEqual([]);
              expect(state.activeTurn.tools.results).toEqual([]);
              expect(state.activeTurn.openEvaluations ?? []).toEqual([]);
              expect(state.activeTurn.completedEvaluations ?? []).toEqual([]);
            }
          }
        }
        expect(traces).toHaveLength(sampled ? 1 : 0);
        for (const [id, count] of reads) {
          const clientControl = events.find((source) => source.id === id)!.producedBy === 'client';
          expect(sampled || clientControl ? count > 0 : count === 0).toBe(true);
        }
        if (sampled) expect(traces[0]!.spans).toHaveLength(5);
        else expect(state.sampling?.suppressed?.turnCount).toBe('1');
      }
      expect(decisions).toEqual(
        sampleRate === 0.5 ? new Set([true, false]) : new Set([sampleRate === 1]),
      );
    },
  );

  it.each([1, 2, 3] as const)(
    'adopts v%s with optional evaluators without erasing v3 tool receipts',
    (version) => {
      for (const withEvaluators of [false, true]) {
        for (const sampleRate of [0, 1]) {
          const events = mixedObservationEvents();
          const before = reduceCanonicalEventBatch(
            initialCanonicalProjectionState(),
            events.slice(0, 12),
          );
          const raw = roundTrip(before.state);
          raw.version = version;
          if (version !== 3) {
            Reflect.deleteProperty(raw.activeTurn!, 'tools');
            raw.pendingInputs.forEach((input) => {
              delete input.toolResult;
              delete input.toolTraceId;
            });
          }
          if (!withEvaluators) {
            delete raw.activeTurn!.openEvaluations;
            delete raw.activeTurn!.completedEvaluations;
          }
          if (version === 2) raw.sampling = { policy: policy(sampleRate), suppressed: null };
          const adopted = reduceCanonicalEventBatch(
            parseCanonicalProjectionState(raw),
            [],
            new Set(before.acceptedSourceIds),
            policy(sampleRate),
          );
          expect(adopted.state.version).toBe(3);
          if (sampleRate === 0) {
            expect(adopted.state.activeTurn?.openModelSummaries).toEqual([]);
            expect(adopted.state.activeTurn?.completedModelSummaries).toEqual([]);
            expect(adopted.state.activeTurn?.openEvaluations ?? []).toEqual([]);
            expect(adopted.state.activeTurn?.completedEvaluations ?? []).toEqual([]);
            expect(adopted.state.activeTurn?.tools.uses).toEqual([]);
            expect(adopted.state.pendingInputs[0]).not.toHaveProperty('toolResult');
            expect(adopted.state.pendingInputs[0]).not.toHaveProperty('toolTraceId');
          } else if (version === 3) {
            expect(adopted.state.pendingInputs).toEqual(before.state.pendingInputs);
            expect(adopted.state.activeTurn?.tools).toEqual(before.state.activeTurn?.tools);
          }
          const after = reduceCanonicalEventBatch(
            parseCanonicalProjectionState(roundTrip(adopted.state)),
            events.slice(12),
            new Set(before.acceptedSourceIds),
          );
          expect(after.completedTraces).toHaveLength(sampleRate);
          if (sampleRate) {
            const spans = after.completedTraces[0]!.spans;
            expect(spans.filter((span) => span.observationType === 'tool')).toHaveLength(
              version === 3 ? 1 : 0,
            );
            expect(
              spans.filter((span) => span.observationType === 'outcome_evaluation'),
            ).toHaveLength(withEvaluators ? 2 : 0);
            expect(
              spans.filter((span) => span.observationType === 'turn_model_summary'),
            ).toHaveLength(2);
          }
        }
      }
    },
  );

  it('round-trips the mixed outbox and OTLP without exporting outcome identity or evaluator usage', async () => {
    const trace = mixedTrace();
    const contaminated = roundTrip(trace);
    Object.assign(
      contaminated.spans.find((span) => span.observationType === 'outcome_evaluation')!,
      {
        explanation: TRANSCRIPT_SECRET,
      },
    );
    // Generic unknown fields are scrubbed; known private identity fields are rejected below.
    const fixture = outbox(contaminated);
    await fixture.write();
    const insert = fixture.query.mock.calls.find(([sql]) =>
      sql.includes('INSERT INTO observability_exporter_trace_outbox'),
    );
    expect(JSON.parse(insert![1]![7] as string)).toEqual(trace);
    assertPublic(fixture.query.mock.calls);
    const item = await fixture.claim();
    expect(item?.trace).toEqual(trace);
    assertPublic(item);
    const wire = encodeLangfuseOtlpJson(item!.trace);
    assertPublic(wire);
    const children = wire.resourceSpans[0]!.scopeSpans[0]!.spans.slice(1);
    expect(children).toHaveLength(5);
    for (const child of children) {
      expect(child.parentSpanId).toBe(trace.root.spanId);
      const canonical = trace.spans.find((span) => span.spanId === child.spanId)!;
      expect(child.status.code).toBe(
        canonical.status === 'unset' ? 0 : canonical.status === 'error' ? 2 : 1,
      );
      if (canonical.observationType === 'outcome_evaluation') {
        expect(child.attributes).toContainEqual({
          key: 'langfuse.observation.type',
          value: { stringValue: 'evaluator' },
        });
        expect(JSON.stringify(child)).not.toMatch(/gen_ai|usage_details|cost_details|explanation/);
      }
    }
  });

  it.each([
    ['outcomeId', 'claim'],
    ['outcomeId', 'write'],
    ['outcomeKey', 'claim'],
    ['outcomeKey', 'write'],
  ] as const)('rejects top-level evaluator identity %s on outbox %s', async (field, boundary) => {
    const trace = mixedTrace();
    Object.assign(trace.spans.find((span) => span.observationType === 'outcome_evaluation')!, {
      [field]: field === 'outcomeId' ? PRIVATE_OUTCOME : outcomeIdentityKey(PRIVATE_OUTCOME),
    });
    const fixture = outbox(trace);
    await expect(fixture[boundary]()).rejects.toThrow('outbox trace is invalid');
    expect(
      fixture.query.mock.calls.some(([sql]) =>
        sql.includes('INSERT INTO observability_exporter_trace_outbox'),
      ),
    ).toBe(false);
  });

  it.each([
    [
      'tool status',
      (trace: ProjectedTrace) => {
        trace.spans.find((s) => s.observationType === 'tool')!.status = 'ok';
      },
    ],
    [
      'evaluator status',
      (trace: ProjectedTrace) => {
        trace.spans.find((s) => s.observationType === 'outcome_evaluation')!.status = 'ok';
      },
    ],
    [
      'model unset',
      (trace: ProjectedTrace) => {
        trace.spans.find((s) => s.observationType === 'turn_model_summary')!.status = 'unset';
      },
    ],
    [
      'root unset',
      (trace: ProjectedTrace) => {
        trace.root.status = 'unset';
      },
    ],
    [
      'tool as evaluator',
      (trace: ProjectedTrace) => {
        trace.spans.find((s) => s.observationType === 'tool')!.observationType =
          'outcome_evaluation';
      },
    ],
    [
      'evaluator as model',
      (trace: ProjectedTrace) => {
        trace.spans.find((s) => s.observationType === 'outcome_evaluation')!.observationType =
          'turn_model_summary';
      },
    ],
    [
      'unknown type',
      (trace: ProjectedTrace) => {
        Object.assign(trace.spans[0]!, { observationType: 'generation' });
      },
    ],
    [
      'outcome metadata',
      (trace: ProjectedTrace) => {
        trace.spans.find((s) => s.observationType === 'outcome_evaluation')!.metadata[
          'orca.outcome.id'
        ] = PRIVATE_OUTCOME;
      },
    ],
  ] as const)('rejects mixed outbox %s on read and before SQL insert', async (_label, mutate) => {
    const trace = mixedTrace();
    mutate(trace);
    const fixture = outbox(trace);
    await expect(fixture.claim()).rejects.toThrow();
    await expect(fixture.write()).rejects.toThrow();
    expect(
      fixture.query.mock.calls.some(([sql]) =>
        sql.includes('INSERT INTO observability_exporter_trace_outbox'),
      ),
    ).toBe(false);
  });

  it('allows all three family maxima together, but rejects each independent cap and the total cap', async () => {
    const trace = mixedTrace();
    const families = [
      ['turn_model_summary', MAX_PROJECTED_MODEL_SUMMARIES_PER_TURN],
      ['tool', MAX_PROJECTED_TOOLS_PER_TURN],
      ['outcome_evaluation', MAX_PROJECTED_EVALUATIONS_PER_TURN],
    ] as const;
    const expand = (type: (typeof families)[number][0], count: number) =>
      Array.from({ length: count }, (_, index) => {
        const span = roundTrip(trace.spans.find((s) => s.observationType === type)!);
        span.sourceEventId = `evt_${type}_${index}`;
        span.spanId = deterministicChildSpanId(trace.traceId, type, '', span.sourceEventId);
        span.metadata['orca.source.start_event_id'] = span.sourceEventId;
        return span;
      });
    const maximal = { ...trace, spans: families.flatMap(([type, cap]) => expand(type, cap)) };
    await expect(outbox(maximal).write()).resolves.toBeUndefined();
    expect((await outbox(maximal).claim())?.trace).toEqual(maximal);
    for (const [type, cap] of families) {
      const overFamily = {
        ...trace,
        spans: [...expand(type, cap + 1), ...trace.spans.filter((s) => s.observationType !== type)],
      };
      await expect(outbox(overFamily).claim()).rejects.toThrow();
      await expect(outbox(overFamily).write()).rejects.toThrow();
    }
    const overTotal = {
      ...maximal,
      spans: [...maximal.spans, expand('tool', MAX_PROJECTED_TOOLS_PER_TURN + 1).at(-1)!],
    };
    await expect(outbox(overTotal).claim()).rejects.toThrow();
    await expect(outbox(overTotal).write()).rejects.toThrow();
  });
});
