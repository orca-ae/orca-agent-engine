// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import type { Event } from '@orca/transcript-store-types';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  applyObservabilityExporterMigrations,
  ObservabilityExporterRepository,
} from '../../../src/persistence.js';
import { RegistryObservabilityClient } from '../../../src/registry-client.js';
import { KafkaObservabilityExporterRuntime } from '../../../src/runtime.js';
import type { ProjectedTrace } from '../../../src/types.js';
import type { OtlpExportRequest } from '../../../src/otlp-json.js';
import { event, WORKSPACE_ID, SESSION_ID, TRANSCRIPT_SECRET } from '../../support/events.js';
import { basicRegistrySecret, enabledRegistryContext } from '../../support/registry.js';

const adminUrl =
  process.env['OBSERVABILITY_EXPORTER_TEST_ADMIN_DATABASE_URL'] ??
  'postgres://orca:orca@127.0.0.1:5432/postgres';

describe('evaluator runtime with durable Postgres checkpoints', () => {
  let admin: Pool;
  let pool: Pool;
  let databaseName: string;

  beforeAll(async () => {
    admin = new Pool({ connectionString: adminUrl, max: 1, connectionTimeoutMillis: 2_000 });
    databaseName = `obs_evaluator_${process.pid}_${randomBytes(4).toString('hex')}`;
    await admin.query(`CREATE DATABASE "${databaseName}"`);
    const url = new URL(adminUrl);
    url.pathname = `/${databaseName}`;
    pool = new Pool({ connectionString: url.toString(), max: 4, connectionTimeoutMillis: 2_000 });
    await applyObservabilityExporterMigrations(pool);
  });

  beforeEach(async () => {
    await pool.query(`TRUNCATE observability_exporter_session_state, observability_exporter_event_inbox,
      observability_exporter_accepted_sources, observability_exporter_trace_outbox,
      observability_exporter_conflicts, observability_exporter_binding_cooldowns`);
  });

  afterAll(async () => {
    await pool?.end();
    try {
      if (pool !== undefined) await admin.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
    } finally {
      await admin?.end();
    }
  });

  function runtimeFor(events: Event[], sampleRate = 1) {
    // Each invocation constructs both repository and runtime anew. Only SQL state survives.
    const repository = new ObservabilityExporterRepository(pool);
    const secretResolve = vi.fn();
    const providerFetch = vi.fn<typeof fetch>(async () => Response.json({}));
    const registryClient = new RegistryObservabilityClient({
      internalBaseUrl: 'http://registry.example',
      tokenProvider: async () => 'internal-test-token-at-least-32-bytes',
      fetchImpl: async (input) => {
        if (String(input).endsWith('/context/resolve')) {
          const context = enabledRegistryContext(WORKSPACE_ID, SESSION_ID);
          const binding = context.binding as Record<string, unknown>;
          (binding.config as Record<string, unknown>).sample_rate = sampleRate;
          return Response.json(context);
        }
        secretResolve();
        return Response.json(basicRegistrySecret());
      },
    });
    const runtime = new KafkaObservabilityExporterRuntime({
      repository,
      transcriptStore: {
        read: async function* (_workspaceId, _sessionId, options) {
          yield* events.filter((source) => source.seq >= Number(options?.fromCursor ?? '0'));
        },
      },
      eventSource: { start: async () => undefined, stop: async () => undefined },
      registryClient,
      workerId: 'evaluator-runtime',
      projectorLeaseMs: 30_000,
      projectorBatchSize: 1_000,
      projectorBatchBytes: 8 * 1024 * 1024,
      deliveryLeaseMs: 180_000,
      registryRequestTimeoutMs: 1_000,
      projectorPollMs: 1,
      deliveryPollMs: 1,
      otlpFetchImpl: providerFetch,
    });
    return { runtime, repository, providerFetch, secretResolve };
  }

  async function assertPrivateContentAbsent() {
    const snapshot = await pool.query(`SELECT jsonb_build_object(
      'state', (SELECT jsonb_agg(to_jsonb(s)) FROM observability_exporter_session_state s),
      'inbox', (SELECT jsonb_agg(to_jsonb(i)) FROM observability_exporter_event_inbox i),
      'outbox', (SELECT jsonb_agg(to_jsonb(o)) FROM observability_exporter_trace_outbox o)
    ) AS snapshot`);
    expect(JSON.stringify(snapshot.rows)).not.toContain(TRANSCRIPT_SECRET);
    expect(JSON.stringify(snapshot.rows)).not.toContain('patient_alice_hiv_positive');
  }

  it.each(['satisfied', 'needs_revision', 'failed', 'max_iterations_reached', 'interrupted'])(
    'restarts between start/end and delivers one metadata-only evaluator for %s',
    async (result) => {
      const events = evaluatorTurn(result);
      const before = runtimeFor(events.slice(0, 5));
      for (const source of events.slice(0, 5)) await before.repository.acceptEvent(source);
      await expect(before.runtime.projectOnce()).resolves.toBe(true);
      const checkpoint = await pool.query(
        'SELECT next_seq::text FROM observability_exporter_session_state',
      );
      expect(checkpoint.rows[0].next_seq).toBe('6');
      expect((await pool.query('SELECT * FROM observability_exporter_trace_outbox')).rows).toEqual(
        [],
      );
      await assertPrivateContentAbsent();

      const after = runtimeFor(events);
      for (const source of events.slice(5)) await after.repository.acceptEvent(source);
      await expect(after.runtime.projectOnce()).resolves.toBe(true);
      const outbox = await pool.query<{ canonical_trace: ProjectedTrace; status: string }>(
        'SELECT canonical_trace, status FROM observability_exporter_trace_outbox',
      );
      expect(outbox.rows).toHaveLength(1);
      const trace = outbox.rows[0]!.canonical_trace;
      expect(trace.spans).toHaveLength(1);
      expect(trace.spans[0]).toMatchObject({
        observationType: 'outcome_evaluation',
        sourceEventId: 'evt_eval_start',
        parentSpanId: trace.root.spanId,
        status: result === 'interrupted' ? 'unset' : result === 'failed' ? 'error' : 'ok',
        startedAt: events[3]!.producedAt,
        endedAt: events[5]!.producedAt,
        metadata: {
          'orca.outcome.iteration': 1,
          'orca.outcome.result': result,
        },
      });
      expect(trace.spans[0]!.modelSummary).toBeUndefined();
      expect(trace.spans[0]!.metadata).not.toHaveProperty('orca.outcome.id');
      expect(JSON.stringify(trace)).not.toMatch(
        /explanation|input_tokens|output_tokens|total_cost_usd/,
      );
      await assertPrivateContentAbsent();

      // Delivery rehydrates the canonical trace through the actual persistence parser.
      const delivery = runtimeFor([]);
      await expect(delivery.runtime.deliverOnce()).resolves.toBe(true);
      expect(delivery.secretResolve).toHaveBeenCalledOnce();
      expect(delivery.providerFetch).toHaveBeenCalledOnce();
      const body = String(delivery.providerFetch.mock.calls[0]![1]!.body);
      const wire = JSON.parse(body) as OtlpExportRequest;
      const spans = wire.resourceSpans.flatMap((resource) =>
        resource.scopeSpans.flatMap((scope) => scope.spans),
      );
      expect(spans).toHaveLength(2);
      expect(spans[1]).toMatchObject({
        traceId: trace.traceId,
        spanId: trace.spans[0]!.spanId,
        parentSpanId: trace.root.spanId,
        status: { code: result === 'interrupted' ? 0 : result === 'failed' ? 2 : 1 },
      });
      expect(spans[1]!.attributes).toContainEqual({
        key: 'langfuse.observation.type',
        value: { stringValue: 'evaluator' },
      });
      expect(body).not.toContain(TRANSCRIPT_SECRET);
      expect(body).not.toMatch(/gen_ai\.usage|usage_details|cost_details|explanation/);
      expect(
        (await pool.query('SELECT status FROM observability_exporter_trace_outbox')).rows[0].status,
      ).toBe('delivered');
      await expect(delivery.runtime.deliverOnce()).resolves.toBe(false);
      await expect(after.runtime.projectOnce()).resolves.toBe(false);
      expect(delivery.providerFetch).toHaveBeenCalledOnce();
    },
  );

  it('advances a sampled-out evaluator turn across restart without outbox or provider work', async () => {
    const events = evaluatorTurn('satisfied');
    const first = runtimeFor(events.slice(0, 5), 0);
    for (const source of events.slice(0, 5)) await first.repository.acceptEvent(source);
    await expect(first.runtime.projectOnce()).resolves.toBe(true);
    await assertPrivateContentAbsent();
    const second = runtimeFor(events, 0);
    for (const source of events.slice(5)) await second.repository.acceptEvent(source);
    await expect(second.runtime.projectOnce()).resolves.toBe(true);
    const state = await pool.query(
      'SELECT next_seq::text, projection_state FROM observability_exporter_session_state',
    );
    expect(state.rows[0].next_seq).toBe('8');
    expect(state.rows[0].projection_state.activeTurn).toBeNull();
    expect(state.rows[0].projection_state.sampling.suppressed).toMatchObject({
      reason: 'sampled_out',
      turnCount: '1',
    });
    expect((await pool.query('SELECT * FROM observability_exporter_trace_outbox')).rows).toEqual(
      [],
    );
    await expect(second.runtime.deliverOnce()).resolves.toBe(false);
    expect(second.secretResolve).not.toHaveBeenCalled();
    expect(second.providerFetch).not.toHaveBeenCalled();
    await assertPrivateContentAbsent();
  });
});

function evaluatorTurn(result: string): Event[] {
  return [
    event(
      1,
      'user.message',
      { content: TRANSCRIPT_SECRET },
      { id: 'evt_turn', producedBy: 'client' },
    ),
    event(2, 'session.user_event_processed', { user_event_id: 'evt_turn' }),
    event(3, 'session.status_running'),
    event(
      4,
      'span.outcome_evaluation_start',
      { outcome_id: 'outcome_patient_alice_hiv_positive', iteration: 1 },
      { id: 'evt_eval_start' },
    ),
    event(5, 'span.outcome_evaluation_ongoing', {
      outcome_id: 'outcome_patient_alice_hiv_positive',
      iteration: 1,
      outcome_evaluation_start_id: 'evt_eval_start',
      explanation: TRANSCRIPT_SECRET,
    }),
    event(6, 'span.outcome_evaluation_end', {
      outcome_id: 'outcome_patient_alice_hiv_positive',
      iteration: 1,
      outcome_evaluation_start_id: 'evt_eval_start',
      result,
      explanation: TRANSCRIPT_SECRET,
      usage: { input_tokens: 913, output_tokens: 517 },
      total_cost_usd: 37,
    }),
    event(7, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
  ];
}
