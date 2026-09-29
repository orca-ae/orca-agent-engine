// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Pool } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import { ObservabilityExporterRepository } from '../../src/persistence.js';
import { projectCanonicalTurns } from '../../src/projector.js';
import type { ProjectedTrace } from '../../src/types.js';
import { completedProjectedTrace, event, TRANSCRIPT_SECRET } from '../support/events.js';

describe('evaluator outbox reconstruction', () => {
  it('claims a metadata-only evaluator and drops unknown top-level content', async () => {
    const trace = evaluatorTrace();
    Object.assign(trace.spans[0]!, { explanation: TRANSCRIPT_SECRET, usage: { input_tokens: 0 } });
    const claimed = await repositoryFor(trace).claimOutbox('evaluator-delivery', 30_000);
    expect(claimed?.trace.spans[0]?.observationType).toBe('outcome_evaluation');
    expect(JSON.stringify(claimed)).not.toContain(TRANSCRIPT_SECRET);
    expect(claimed?.trace.spans[0]).not.toHaveProperty('usage');
  });

  it('keeps legacy traces readable without inventing evaluator observations', async () => {
    const trace = completedProjectedTrace();
    const claimed = await repositoryFor(trace).claimOutbox('legacy-delivery', 30_000);
    expect(claimed?.trace).toEqual(trace);
  });

  it('preserves an interrupted evaluation as unset, not success or failure', async () => {
    const trace = evaluatorTrace('interrupted');
    const claimed = await repositoryFor(trace).claimOutbox('interrupted-delivery', 30_000);
    expect(claimed?.trace.spans[0]?.status).toBe('unset');
    expect(claimed?.trace.root.status).toBe('ok');
  });

  it.each([
    [
      'description-derived slug outcome ID',
      (span: ProjectedTrace['spans'][number]) => {
        span.metadata['orca.outcome.id'] = 'outcome_patient_alice_hiv_positive';
      },
    ],
    [
      'unknown metadata',
      (span: ProjectedTrace['spans'][number]) => {
        span.metadata.explanation = TRANSCRIPT_SECRET;
      },
    ],
    [
      'fabricated model summary',
      (span: ProjectedTrace['spans'][number]) => {
        span.modelSummary = { provider: 'fabricated' };
      },
    ],
    [
      'zero span ID',
      (span: ProjectedTrace['spans'][number]) => {
        span.spanId = '0'.repeat(16);
      },
    ],
    [
      'unrelated span ID',
      (span: ProjectedTrace['spans'][number]) => {
        span.spanId = 'a'.repeat(16);
      },
    ],
    [
      'unrelated parent',
      (span: ProjectedTrace['spans'][number]) => {
        span.parentSpanId = 'a'.repeat(16);
      },
    ],
    [
      'child path',
      (span: ProjectedTrace['spans'][number]) => {
        span.subpath = 'subagents/child';
      },
    ],
    [
      'prose outcome ID',
      (span: ProjectedTrace['spans'][number]) => {
        span.metadata['orca.outcome.id'] = 'the user secret outcome';
      },
    ],
    [
      'credential outcome ID',
      (span: ProjectedTrace['spans'][number]) => {
        span.metadata['orca.outcome.id'] = 'Bearer sk-secret';
      },
    ],
    [
      'unknown result',
      (span: ProjectedTrace['spans'][number]) => {
        span.metadata['orca.outcome.result'] = 'arbitrary';
      },
    ],
    [
      'contradictory failed status',
      (span: ProjectedTrace['spans'][number]) => {
        span.metadata['orca.outcome.result'] = 'failed';
      },
    ],
    [
      'contradictory interrupted status',
      (span: ProjectedTrace['spans'][number]) => {
        span.metadata['orca.outcome.result'] = 'interrupted';
      },
    ],
  ] as const)('rejects %s', async (_name, mutate) => {
    const trace = evaluatorTrace();
    mutate(trace.spans[0]!);
    await expect(repositoryFor(trace).claimOutbox('invalid-delivery', 30_000)).rejects.toThrow(
      'outbox trace is invalid',
    );
  });
});

function evaluatorTrace(result = 'satisfied'): ProjectedTrace {
  const trace = projectCanonicalTurns([
    event(1, 'user.message', {}, { id: 'evt_user', producedBy: 'client' }),
    event(2, 'session.user_event_processed', { user_event_id: 'evt_user' }),
    event(
      3,
      'span.outcome_evaluation_start',
      { outcome_id: 'outc_test', iteration: 0 },
      { id: 'evt_eval' },
    ),
    event(4, 'span.outcome_evaluation_end', {
      outcome_evaluation_start_id: 'evt_eval',
      outcome_id: 'outc_test',
      iteration: 0,
      result,
      explanation: TRANSCRIPT_SECRET,
      usage: { input_tokens: 0 },
    }),
    event(5, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
  ])[0]!;
  expect(trace.spans).toHaveLength(1);
  return trace;
}

function repositoryFor(trace: ProjectedTrace): ObservabilityExporterRepository {
  const deliveryContext = {
    organizationId: 'org_test',
    bindingId: 'aob_test',
    bindingVersion: 1,
    adapterType: 'otlp_http',
    endpointKind: 'traces_endpoint',
    endpointClass: 'public',
    endpointUrl: 'https://collector.example/api/public/otel/v1/traces',
    semanticProfile: 'langfuse',
    protocol: 'http/json',
    compression: 'none',
    timeoutMs: 1_000,
    captureMode: 'metadata_only',
    sampleRate: 1,
    configSchemaVersion: 1,
  };
  const row = {
    id: '1',
    organization_id: deliveryContext.organizationId,
    workspace_id: trace.workspaceId,
    session_id: trace.sessionId,
    binding_id: deliveryContext.bindingId,
    binding_version: 1,
    trace_id: trace.traceId,
    canonical_trace: trace,
    delivery_context: deliveryContext,
    delivery_attempt_count: '0',
    lease_owner: 'evaluator-delivery',
    lease_generation: '1',
  };
  const query = vi.fn(async (sql: string) => ({
    rows: sql.includes('SELECT o.binding_id')
      ? [{ binding_id: deliveryContext.bindingId }]
      : sql.includes('WITH candidate AS')
        ? [row]
        : [],
    rowCount: 1,
  }));
  return new ObservabilityExporterRepository({
    query,
    connect: async () => ({ query, release: () => undefined }),
  } as unknown as Pool);
}
