// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import type { Event } from '@orca/transcript-store-types';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { outcomeIdentityKey } from '../../../src/event-identity.js';
import {
  applyObservabilityExporterMigrations,
  ObservabilityExporterRepository,
} from '../../../src/persistence.js';
import { projectCanonicalTurns } from '../../../src/projector.js';
import { RegistryObservabilityClient } from '../../../src/registry-client.js';
import { KafkaObservabilityExporterRuntime } from '../../../src/runtime.js';
import { SESSION_ID, TRANSCRIPT_SECRET, WORKSPACE_ID } from '../../support/events.js';
import { mixedObservationEvents, PRIVATE_OUTCOME } from '../../support/mixed-observations.js';
import { basicRegistrySecret, enabledRegistryContext } from '../../support/registry.js';

const adminUrl =
  process.env['OBSERVABILITY_EXPORTER_TEST_ADMIN_DATABASE_URL'] ??
  'postgres://orca:orca@127.0.0.1:5432/postgres';

describe('mixed observations across real Postgres runtime restarts', () => {
  let admin: Pool;
  let pool: Pool;
  let database: string;
  beforeAll(async () => {
    admin = new Pool({ connectionString: adminUrl, max: 1, connectionTimeoutMillis: 2000 });
    database = `obs_mixed_${process.pid}_${randomBytes(4).toString('hex')}`;
    await admin.query(`CREATE DATABASE "${database}"`);
    const url = new URL(adminUrl);
    url.pathname = `/${database}`;
    pool = new Pool({ connectionString: url.toString(), max: 4, connectionTimeoutMillis: 2000 });
    await applyObservabilityExporterMigrations(pool);
  });
  afterAll(async () => {
    await pool?.end();
    try {
      if (pool !== undefined) await admin.query(`DROP DATABASE IF EXISTS "${database}"`);
    } finally {
      await admin?.end();
    }
  });

  function runtimeFor(events: Event[], sampleRate: number) {
    const repository = new ObservabilityExporterRepository(pool);
    const providerFetch = vi.fn<typeof fetch>(async () => Response.json({}));
    const secretResolve = vi.fn();
    const registryClient = new RegistryObservabilityClient({
      internalBaseUrl: 'http://registry.example',
      tokenProvider: async () => 'internal-test-token-at-least-32-bytes',
      fetchImpl: async (input) => {
        if (String(input).endsWith('/context/resolve')) {
          const context = enabledRegistryContext(WORKSPACE_ID, events[0]?.sessionId ?? SESSION_ID);
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
      registryClient,
      transcriptStore: {
        read: async function* (_workspace, _session, options) {
          yield* events.filter((source) => source.seq >= Number(options?.fromCursor ?? '0'));
        },
      },
      eventSource: { start: async () => undefined, stop: async () => undefined },
      workerId: 'mixed-runtime',
      projectorLeaseMs: 30_000,
      projectorBatchSize: 1000,
      projectorBatchBytes: 8 * 1024 * 1024,
      deliveryLeaseMs: 180_000,
      registryRequestTimeoutMs: 1000,
      projectorPollMs: 1,
      deliveryPollMs: 1,
      otlpFetchImpl: providerFetch,
    });
    return { runtime, repository, providerFetch, secretResolve };
  }

  it.each([0, 1])(
    'persists receipts and all families, then delivers only at rate %s',
    async (sampleRate) => {
      await pool.query(`TRUNCATE observability_exporter_session_state, observability_exporter_event_inbox,
      observability_exporter_accepted_sources, observability_exporter_trace_outbox,
      observability_exporter_conflicts, observability_exporter_binding_cooldowns`);
      const events = mixedObservationEvents();
      const before = runtimeFor(events.slice(0, 12), sampleRate);
      for (const source of events.slice(0, 12)) await before.repository.acceptEvent(source);
      await expect(before.runtime.projectOnce()).resolves.toBe(true);
      const checkpoint = (
        await pool.query(
          'SELECT next_seq::text, projection_state FROM observability_exporter_session_state',
        )
      ).rows[0];
      expect(checkpoint.next_seq).toBe('13');
      const state = checkpoint.projection_state;
      expect(JSON.stringify(state)).not.toContain(PRIVATE_OUTCOME);
      expect(JSON.stringify(state)).not.toContain(TRANSCRIPT_SECRET);
      if (sampleRate) {
        expect(state.pendingInputs[0].toolResult).toMatchObject({
          family: 'local',
          outcome: 'error',
        });
        expect(state.activeTurn.tools.uses).toHaveLength(1);
        expect(state.activeTurn.openModelSummaries).toHaveLength(1);
        expect(state.activeTurn.completedModelSummaries).toHaveLength(1);
        expect(state.activeTurn.completedEvaluations).toHaveLength(1);
        expect(state.activeTurn.openEvaluations[0].outcomeKey).toBe(
          outcomeIdentityKey(PRIVATE_OUTCOME),
        );
      } else {
        expect(state.pendingInputs[0]).not.toHaveProperty('toolResult');
        expect(JSON.stringify(state)).not.toContain('outcome_digest_');
      }
      expect((await pool.query('SELECT * FROM observability_exporter_trace_outbox')).rows).toEqual(
        [],
      );

      const after = runtimeFor(events, sampleRate);
      for (const source of events.slice(12)) await after.repository.acceptEvent(source);
      await expect(after.runtime.projectOnce()).resolves.toBe(true);
      const finalCheckpoint = (
        await pool.query(
          'SELECT next_seq::text, projection_state FROM observability_exporter_session_state',
        )
      ).rows[0];
      expect(finalCheckpoint.next_seq).toBe('18');
      expect(finalCheckpoint.projection_state.activeTurn).toBeNull();
      expect(finalCheckpoint.projection_state.pendingInputs).toEqual([]);
      expect(finalCheckpoint.projection_state.sampling.suppressed).toEqual(
        sampleRate
          ? null
          : expect.objectContaining({
              reason: 'sampled_out',
              turnCount: '1',
              firstSourceSeq: '17',
              lastSourceSeq: '17',
            }),
      );
      const rows = (
        await pool.query('SELECT canonical_trace FROM observability_exporter_trace_outbox')
      ).rows;
      expect(rows).toHaveLength(sampleRate);
      if (sampleRate) expect(rows[0].canonical_trace).toEqual(projectCanonicalTurns(events)[0]);
      expect(JSON.stringify(rows)).not.toMatch(/outcomeKey|outcome_digest_|orca.outcome.id/);
      expect(JSON.stringify(rows)).not.toContain(PRIVATE_OUTCOME);
      expect(JSON.stringify(rows)).not.toContain(TRANSCRIPT_SECRET);
      const delivery = runtimeFor([], sampleRate);
      await expect(delivery.runtime.deliverOnce()).resolves.toBe(Boolean(sampleRate));
      expect(delivery.providerFetch).toHaveBeenCalledTimes(sampleRate);
      expect(delivery.secretResolve).toHaveBeenCalledTimes(sampleRate);
      if (sampleRate) {
        const body = String(delivery.providerFetch.mock.calls[0]![1]!.body);
        const spans = JSON.parse(body).resourceSpans[0].scopeSpans[0].spans;
        expect(spans).toHaveLength(6);
        expect(body).not.toMatch(/outcomeKey|outcome_digest_|orca.outcome.id/);
        expect(body).not.toContain(PRIVATE_OUTCOME);
        expect(body).not.toContain(TRANSCRIPT_SECRET);
      }
      await expect(delivery.runtime.deliverOnce()).resolves.toBe(false);
      await expect(after.runtime.projectOnce()).resolves.toBe(false);
    },
  );
});
