// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  applyObservabilityExporterMigrations,
  ExporterLeaseLostError,
  ObservabilityExporterRepository,
} from '../../../src/persistence.js';
import { initialCanonicalProjectionState } from '../../../src/projector.js';
import { KafkaObservabilityExporterRuntime } from '../../../src/runtime.js';
import type { PinnedDeliveryContext } from '../../../src/types.js';
import { completedProjectedTrace, event, TRANSCRIPT_SECRET } from '../../support/events.js';

const ADMIN_DATABASE_URL =
  process.env['OBSERVABILITY_EXPORTER_TEST_ADMIN_DATABASE_URL'] ??
  'postgres://orca:orca@localhost:5432/postgres';
const PROVIDER_MESSAGE = 'collector-secret-sentinel: sk-outcome\n密钥😀';
const MESSAGE_SHA256 = 'acc2b6bffe2f543fa46cfb4f5187fdc6749ca28cf938b0d5788284077867377c';
const deliveryContext: PinnedDeliveryContext = {
  organizationId: 'org_outcomes',
  bindingId: 'aob_outcomes',
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

describe('runtime terminal delivery outcomes (real Postgres)', () => {
  let adminPool: Pool;
  let pool: Pool;
  let repository: ObservabilityExporterRepository;
  let databaseName: string;

  beforeAll(async () => {
    adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    databaseName = `observability_outcomes_${process.pid}_${randomBytes(4).toString('hex')}`;
    await adminPool.query(`CREATE DATABASE "${databaseName}"`);
    const databaseUrl = new URL(ADMIN_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    pool = new Pool({ connectionString: databaseUrl.toString(), max: 4 });
    await applyObservabilityExporterMigrations(pool);
    repository = new ObservabilityExporterRepository(pool);
  });

  afterAll(async () => {
    await pool?.end().catch(() => undefined);
    await adminPool?.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => undefined);
    await adminPool?.end().catch(() => undefined);
  });

  beforeEach(async () => {
    await pool.query(`TRUNCATE observability_exporter_trace_outbox,
      observability_exporter_session_state, observability_exporter_event_inbox`);
    await repository.acceptEvent(event(1, 'user.message', { content: TRANSCRIPT_SECRET }));
    const claim = await repository.claimSession('outcomes-projector', 30_000);
    await repository.completeProjection(claim!, '2', initialCanonicalProjectionState(), [
      { trace: completedProjectedTrace(), deliveryContext },
    ]);
  });

  const cases = [
    { kind: 'accepted', rejectedSpans: '0', body: '{}' },
    { kind: 'accepted_with_warning', rejectedSpans: '0', body: partialResponse('0') },
    {
      kind: 'partial_rejection',
      rejectedSpans: '9223372036854775807',
      body: partialResponse('9223372036854775807'),
    },
  ];
  it.each(
    cases.flatMap((entry) => [
      { ...entry, shutdownAtEof: false },
      { ...entry, shutdownAtEof: true },
    ]),
  )(
    'atomically persists $kind, shutdown at EOF=$shutdownAtEof, without a whole-row resend',
    async ({ kind, rejectedSpans, body, shutdownAtEof }) => {
      const controller = new AbortController();
      const fetchImpl = vi.fn(
        async () =>
          new Response(
            new ReadableStream({
              start(stream) {
                stream.enqueue(Buffer.from(body));
              },
              pull(stream) {
                stream.close();
                if (shutdownAtEof) controller.abort();
              },
            }),
            { headers: { 'content-type': 'application/json' } },
          ),
      );
      const runtime = runtimeFor(fetchImpl);

      await expect(runtime.deliverOnce(controller.signal)).resolves.toBe(true);
      const row = await outboxRow();
      const partial = kind === 'partial_rejection';
      expect(row).toMatchObject({
        status: partial ? 'suppressed' : 'delivered',
        suppression_reason: partial ? 'partial_rejection' : null,
        delivery_outcome: kind,
        delivery_rejected_spans: rejectedSpans,
        delivery_message_bytes: kind === 'accepted' ? null : 48,
        delivery_message_sha256: kind === 'accepted' ? null : MESSAGE_SHA256,
        delivery_attempt_count: '0',
        last_error_code: null,
        lease_owner: null,
        lease_until: null,
      });
      expect(row[partial ? 'suppressed_at' : 'delivered_at']).toBeInstanceOf(Date);
      expect(row[partial ? 'delivered_at' : 'suppressed_at']).toBeNull();
      await expect(runtime.deliverOnce()).resolves.toBe(false);
      await expect(repository.claimOutbox('another-delivery', 30_000)).resolves.toBeNull();
      expect(fetchImpl).toHaveBeenCalledOnce();
      for (const table of [
        'trace_outbox',
        'session_state',
        'event_inbox',
        'accepted_sources',
        'conflicts',
      ]) {
        const stored = await pool.query(`SELECT * FROM observability_exporter_${table}`);
        const serialized = JSON.stringify(stored.rows);
        expect(serialized).not.toContain('collector-secret-sentinel');
        expect(serialized).not.toContain('sk-outcome');
        expect(serialized).not.toContain(TRANSCRIPT_SECRET);
      }
    },
  );

  it('retains a pending row without outcome metadata if the partial body is cancelled before EOF', async () => {
    const controller = new AbortController();
    const runtime = runtimeFor(
      async () =>
        new Response(
          new ReadableStream({
            start(stream) {
              stream.enqueue(Buffer.from(partialResponse('1')));
            },
            pull(stream) {
              controller.abort();
              stream.error(new Error('cancelled before response completion'));
            },
          }),
          { headers: { 'content-type': 'application/json' } },
        ),
    );

    await expect(runtime.deliverOnce(controller.signal)).rejects.toMatchObject({
      kind: 'cancelled',
    });
    expect(await outboxRow()).toMatchObject({
      ...pendingOutcome(),
      lease_owner: null,
      lease_until: null,
      last_error_code: 'delivery_failed',
    });
    // Cancellation still releases work: unlike a confirmed partial rejection, it can be reclaimed.
    expect(await repository.claimOutbox('next-delivery', 30_000)).not.toBeNull();
  });

  it('cannot complete or release an expired lease after a confirmed partial response and shutdown', async () => {
    const controller = new AbortController();
    const runtime = runtimeFor(async () => {
      await pool.query(`UPDATE observability_exporter_trace_outbox
        SET lease_until = now() - interval '1 second'`);
      return new Response(
        new ReadableStream({
          start(stream) {
            stream.enqueue(Buffer.from(partialResponse('1')));
          },
          pull(stream) {
            stream.close();
            controller.abort();
          },
        }),
        { headers: { 'content-type': 'application/json' } },
      );
    });

    await expect(runtime.deliverOnce(controller.signal)).rejects.toBeInstanceOf(
      ExporterLeaseLostError,
    );
    expect(await outboxRow()).toMatchObject({
      ...pendingOutcome(),
      lease_owner: 'outcomes-delivery',
      last_error_code: null,
    });
  });

  async function outboxRow() {
    const result = await pool.query(`SELECT * FROM observability_exporter_trace_outbox`);
    expect(result.rows).toHaveLength(1);
    return result.rows[0] as Record<string, unknown>;
  }

  function runtimeFor(otlpFetchImpl: typeof fetch) {
    return new KafkaObservabilityExporterRuntime({
      repository,
      transcriptStore: { read: async function* () {} },
      eventSource: { start: async () => undefined, stop: async () => undefined },
      registryClient: {
        resolveContext: async () => ({ status: 'enabled' as const, deliveryContext }),
        resolveSecret: async () => ({
          bindingId: deliveryContext.bindingId,
          bindingVersion: deliveryContext.bindingVersion,
          effectiveCaptureMode: 'metadata_only' as const,
          auth: { type: 'basic' as const, username: 'pk-outcome', password: 'sk-outcome' },
        }),
      },
      workerId: 'outcomes',
      projectorLeaseMs: 30_000,
      projectorBatchSize: 1_000,
      projectorBatchBytes: 8 * 1024 * 1024,
      deliveryLeaseMs: 30_000,
      registryRequestTimeoutMs: 1_000,
      projectorPollMs: 1,
      deliveryPollMs: 1,
      otlpFetchImpl,
    });
  }
});

function partialResponse(rejectedSpans: string): string {
  return `{ "partialSuccess": { "rejectedSpans": ${rejectedSpans}, "errorMessage": ${JSON.stringify(PROVIDER_MESSAGE)} } }`;
}

function pendingOutcome() {
  return {
    status: 'pending',
    delivery_outcome: null,
    delivery_rejected_spans: null,
    delivery_message_bytes: null,
    delivery_message_sha256: null,
    delivery_attempt_count: '0',
    delivered_at: null,
    suppressed_at: null,
    suppression_reason: null,
  };
}
