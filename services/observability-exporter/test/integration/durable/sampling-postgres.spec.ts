// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomBytes } from 'node:crypto';
import type { Event } from '@orca/transcript-store-types';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  applyObservabilityExporterMigrations,
  ExporterLeaseLostError,
  ObservabilityExporterRepository,
} from '../../../src/persistence.js';
import {
  initialCanonicalProjectionState,
  reduceCanonicalEventBatch,
  type CanonicalProjectionState,
} from '../../../src/projector.js';
import { RegistryObservabilityClient } from '../../../src/registry-client.js';
import { KafkaObservabilityExporterRuntime } from '../../../src/runtime.js';
import { deterministicTraceId } from '../../../src/ids.js';
import { TRACE_SAMPLING_VERSION, type TraceSamplingPolicy } from '../../../src/sampling.js';
import { basicRegistrySecret, enabledRegistryContext } from '../../support/registry.js';
import { completedPrimaryTurnEvents, event, TRANSCRIPT_SECRET } from '../../support/events.js';

const ADMIN_DATABASE_URL =
  process.env['OBSERVABILITY_EXPORTER_TEST_ADMIN_DATABASE_URL'] ??
  'postgres://orca:orca@localhost:5432/postgres';
const workspaceId = 'ws_sampling';
const sessionId = 'ses_sampling';
const basePolicy: TraceSamplingPolicy = {
  algorithmVersion: TRACE_SAMPLING_VERSION,
  bindingId: 'aob_sampling',
  bindingVersion: 1,
  sampleRate: 0,
};

describe('sampling with real Postgres checkpoints and runtime delivery', () => {
  let admin: Pool;
  let pool: Pool;
  let repository: ObservabilityExporterRepository;
  let databaseName: string;

  beforeAll(async () => {
    admin = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    databaseName = `obs_sampling_${process.pid}_${randomBytes(4).toString('hex')}`;
    await admin.query(`CREATE DATABASE "${databaseName}"`);
    const url = new URL(ADMIN_DATABASE_URL);
    url.pathname = `/${databaseName}`;
    pool = new Pool({ connectionString: url.toString(), max: 4 });
    await applyObservabilityExporterMigrations(pool);
    repository = new ObservabilityExporterRepository(pool);
  });

  beforeEach(async () => {
    await pool.query(`TRUNCATE observability_exporter_session_state, observability_exporter_event_inbox,
      observability_exporter_accepted_sources, observability_exporter_trace_outbox,
      observability_exporter_conflicts, observability_exporter_binding_cooldowns`);
  });

  afterAll(async () => {
    await pool?.end();
    await admin?.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
    await admin?.end();
  });

  function runtimeFor(events: Event[], policy: TraceSamplingPolicy, batchSize = 1_000) {
    const secretResolve = vi.fn();
    const providerFetch = vi.fn<typeof fetch>(
      async () =>
        new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } }),
    );
    const registry = new RegistryObservabilityClient({
      internalBaseUrl: 'http://registry.example',
      tokenProvider: async () => 'internal-test-token-at-least-32-bytes',
      fetchImpl: async (input) => {
        if (String(input).endsWith('/context/resolve')) {
          const response = enabledRegistryContext(workspaceId, sessionId);
          const binding = response.binding as Record<string, unknown>;
          binding.id = policy.bindingId;
          binding.version = policy.bindingVersion;
          (binding.config as Record<string, unknown>).sample_rate = policy.sampleRate;
          return Response.json(response);
        }
        secretResolve();
        return Response.json({
          ...basicRegistrySecret(),
          binding_id: policy.bindingId,
          binding_version: policy.bindingVersion,
        });
      },
    });
    const runtime = new KafkaObservabilityExporterRuntime({
      repository,
      transcriptStore: {
        read: async function* (_workspaceId, _sessionId, options) {
          yield* events
            .filter((source) => source.seq >= Number(options?.fromCursor ?? '0'))
            .slice(0, options?.maxEvents);
        },
      },
      eventSource: { start: async () => undefined, stop: async () => undefined },
      registryClient: registry,
      workerId: 'sampling-runtime',
      projectorLeaseMs: 30_000,
      projectorBatchSize: batchSize,
      projectorBatchBytes: 8 * 1024 * 1024,
      deliveryLeaseMs: 180_000,
      registryRequestTimeoutMs: 1_000,
      projectorPollMs: 1,
      deliveryPollMs: 1,
      otlpFetchImpl: providerFetch,
    });
    return { runtime, secretResolve, providerFetch };
  }

  async function stateRow() {
    const result = await pool.query<{
      next_seq: string;
      projection_state: CanonicalProjectionState;
    }>(
      'SELECT next_seq::text, projection_state FROM observability_exporter_session_state WHERE workspace_id=$1 AND session_id=$2',
      [workspaceId, sessionId],
    );
    return result.rows[0]!;
  }

  async function count(table: string) {
    const result = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM ${table}`,
    );
    return Number(result.rows[0]!.count);
  }

  it.each([
    { sampleRate: 0, bindingVersion: 1 },
    { sampleRate: 1, bindingVersion: 1 },
    { sampleRate: 0.5, bindingVersion: 1 },
    { sampleRate: 0.5, bindingVersion: 2 },
  ])(
    'persists and replays rate $sampleRate / config $bindingVersion without unsampled outbox/provider work',
    async (parameters) => {
      const policy = { ...basePolicy, ...parameters };
      const events = turns(12);
      for (const source of events) await repository.acceptEvent(source);
      const expectedIds = events
        .filter((source) => source.kind === 'user.message')
        .map((source) => deterministicTraceId(workspaceId, sessionId, source.id));
      const selected = expectedIds.filter((traceId) => {
        // Independent fixed-rate reference: no sampler implementation in the oracle.
        const hash = createHash('sha256')
          .update('orca.observability.trace-sampling.v1\0')
          .update(JSON.stringify([policy.bindingId, policy.bindingVersion, traceId]))
          .digest()
          .readBigUInt64BE();
        return policy.sampleRate === 1 || (policy.sampleRate === 0.5 && hash < 1n << 63n);
      });
      if (policy.sampleRate === 0.5) {
        expect(selected.length).toBeGreaterThan(0);
        expect(selected.length).toBeLessThan(expectedIds.length);
      }
      // Recreate the runtime after every small batch; durable state owns the decision.
      for (let batch = 0; batch < events.length; batch += 4) {
        await expect(runtimeFor(events, policy, 4).runtime.projectOnce()).resolves.toBe(true);
        const state = (await stateRow()).projection_state;
        expect(state.sampling?.policy).toEqual(policy);
        expect(JSON.stringify(state)).not.toContain(TRANSCRIPT_SECRET);
        if (policy.sampleRate === 0) {
          expect(JSON.stringify(state)).not.toContain('claude-test');
          expect(state.activeTurn?.completedModelSummaries ?? []).toEqual([]);
        }
      }
      const final = await stateRow();
      expect(final.next_seq).toBe(String(events.at(-1)!.seq + 1));
      expect(final.projection_state.activeTurn).toBeNull();
      expect(final.projection_state.pendingInputs).toEqual([]);
      const watermark = final.projection_state.sampling?.suppressed;
      if (selected.length === expectedIds.length) expect(watermark).toBeNull();
      else
        expect(watermark).toMatchObject({
          reason: 'sampled_out',
          turnCount: String(expectedIds.length - selected.length),
          workspaceId,
          sessionId,
        });
      expect(JSON.stringify(final.projection_state).length).toBeLessThan(800);
      expect(await count('observability_exporter_session_state')).toBe(1);
      expect(await count('observability_exporter_accepted_sources')).toBe(12);
      const rows = await pool.query<{ trace_id: string }>(
        'SELECT trace_id FROM observability_exporter_trace_outbox ORDER BY trace_id',
      );
      expect(rows.rows.map((row) => row.trace_id)).toEqual([...selected].sort());
      const inbox = await pool.query(
        'SELECT 1 FROM observability_exporter_event_inbox WHERE processed_at IS NULL',
      );
      expect(inbox.rowCount).toBe(0);
      // Late ACK/replayed notifications do not reproject or increment the watermark.
      for (const source of events)
        await expect(repository.acceptEvent(source)).resolves.toBe('duplicate');
      const restarted = runtimeFor(events, policy);
      await expect(restarted.runtime.projectOnce()).resolves.toBe(false);
      expect(await stateRow()).toEqual(final);
      for (const _traceId of selected)
        await expect(restarted.runtime.deliverOnce()).resolves.toBe(true);
      await expect(restarted.runtime.deliverOnce()).resolves.toBe(false);
      expect(restarted.secretResolve).toHaveBeenCalledTimes(selected.length);
      expect(restarted.providerFetch).toHaveBeenCalledTimes(selected.length);
      expect(JSON.stringify(restarted.providerFetch.mock.calls)).not.toContain(TRANSCRIPT_SECRET);
    },
    20_000,
  );

  it.each([0, 0.5, 1])(
    'checkpoints accepted client tool results at rate %s before delivery',
    async (sampleRate) => {
      const policy = { ...basePolicy, sampleRate };
      const events = [
        event(1, 'user.message', {}, { producedBy: 'client' }),
        event(2, 'session.user_event_processed', { user_event_id: 'evt_1' }),
        event(3, 'session.status_running'),
        event(4, 'agent.tool_use', {
          tool_use_id: 'private-native-tool-id',
          input: TRANSCRIPT_SECRET,
        }),
        event(5, 'agent.custom_tool_use', { name: TRANSCRIPT_SECRET }),
        event(6, 'session.status_idle', { stop_reason: { type: 'requires_action' } }),
        event(
          7,
          'user.tool_result',
          { tool_use_id: 'private-native-tool-id', content: TRANSCRIPT_SECRET },
          { producedBy: 'client' },
        ),
        event(
          8,
          'user.custom_tool_result',
          { custom_tool_use_id: 'evt_5', content: TRANSCRIPT_SECRET, is_error: true },
          { producedBy: 'client' },
        ),
        event(9, 'session.user_event_processed', { user_event_id: 'evt_8' }),
        event(10, 'session.user_event_processed', { user_event_id: 'evt_7' }),
        event(11, 'session.status_running'),
        event(12, 'agent.tool_result', { content: TRANSCRIPT_SECRET }),
        event(13, 'session.status_idle', { stop_reason: { type: 'end_turn' } }),
      ].map((source) => ({ ...source, workspaceId, sessionId }));
      const traceId = deterministicTraceId(workspaceId, sessionId, 'evt_1');
      const hash = createHash('sha256')
        .update('orca.observability.trace-sampling.v1\0')
        .update(JSON.stringify([policy.bindingId, policy.bindingVersion, traceId]))
        .digest()
        .readBigUInt64BE();
      const sampled = sampleRate === 1 || (sampleRate === 0.5 && hash < 1n << 63n);
      for (const source of events) await repository.acceptEvent(source);
      for (const source of events) {
        await expect(runtimeFor(events, policy, 1).runtime.projectOnce()).resolves.toBe(true);
        const row = await stateRow();
        expect(row.next_seq).toBe(String(source.seq + 1));
        expect(JSON.stringify(row)).not.toContain(TRANSCRIPT_SECRET);
        expect(JSON.stringify(row)).not.toContain('private-native-tool-id');
        if (source.seq === 8 && sampled) {
          expect(row.projection_state.pendingInputs).toHaveLength(2);
          expect(
            row.projection_state.activeTurn?.tools.uses.every((use) => use.completed === undefined),
          ).toBe(true);
        }
      }
      expect(await count('observability_exporter_accepted_sources')).toBe(3);
      expect(await count('observability_exporter_trace_outbox')).toBe(sampled ? 1 : 0);
      const delivery = runtimeFor(events, policy);
      await expect(delivery.runtime.deliverOnce()).resolves.toBe(sampled);
      expect(delivery.secretResolve).toHaveBeenCalledTimes(sampled ? 1 : 0);
      expect(delivery.providerFetch).toHaveBeenCalledTimes(sampled ? 1 : 0);
      if (sampled) {
        const rows = await pool.query(
          'SELECT canonical_trace FROM observability_exporter_trace_outbox',
        );
        const trace = rows.rows[0]!.canonical_trace;
        expect(trace.spans).toHaveLength(2);
        expect(trace.root.status).toBe('ok');
        expect(trace.root.metadata['orca.turn.unmatched_tool_result_count']).toBe(1);
        expect(trace.spans.map((span: { status: string }) => span.status)).toEqual(['ok', 'error']);
        expect(JSON.stringify(delivery.providerFetch.mock.calls)).not.toContain(TRANSCRIPT_SECRET);
        expect(JSON.stringify(delivery.providerFetch.mock.calls)).not.toContain(
          'private-native-tool-id',
        );
      }
    },
  );

  it.each([
    { bindingVersion: 1, sampleRate: 0.5491406037789095, sampled: false },
    { bindingVersion: 1, sampleRate: 0.5491406037789095 + 2 ** -53, sampled: true },
    { bindingVersion: 2, sampleRate: 0.9451945012196389, sampled: true },
    { bindingVersion: 2, sampleRate: 0.9451945012196389 - 2 ** -53, sampled: false },
  ])(
    'preserves the exact fractional hash boundary through Postgres for $bindingVersion/$sampleRate',
    async ({ bindingVersion, sampleRate, sampled }) => {
      // Independent vectors for trace 65200e5c658ec8b0699112fef18ee2e7:
      // config 1 -> 8c947a8622d82338; config 2 -> f1f8444f18edf408.
      const events = turns(1);
      for (const source of events) await repository.acceptEvent(source);
      const { runtime, secretResolve, providerFetch } = runtimeFor(events, {
        ...basePolicy,
        bindingVersion,
        sampleRate,
      });
      await expect(runtime.projectOnce()).resolves.toBe(true);
      expect((await stateRow()).next_seq).toBe('9');
      expect(await count('observability_exporter_trace_outbox')).toBe(sampled ? 1 : 0);
      expect((await stateRow()).projection_state.sampling?.suppressed?.reason).toBe(
        sampled ? undefined : 'sampled_out',
      );
      await expect(runtime.deliverOnce()).resolves.toBe(sampled);
      expect(secretResolve).toHaveBeenCalledTimes(sampled ? 1 : 0);
      expect(providerFetch).toHaveBeenCalledTimes(sampled ? 1 : 0);
    },
  );

  it('rolls back suppression, accepted identities, and checkpoint if inbox completion fails', async () => {
    const events = turns(2);
    for (const source of events) await repository.acceptEvent(source);
    const originalCheckpoint = await stateRow();
    await pool.query(`CREATE FUNCTION reject_sampling_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'injected checkpoint failure'; END $$;
      CREATE TRIGGER reject_sampling_checkpoint BEFORE UPDATE OF processed_at ON observability_exporter_event_inbox
      FOR EACH ROW EXECUTE FUNCTION reject_sampling_checkpoint()`);
    const { runtime, secretResolve, providerFetch } = runtimeFor(events, basePolicy);
    try {
      await expect(runtime.projectOnce()).rejects.toThrow('injected checkpoint failure');
      // Rollback preserves the stored checkpoint, including its legacy schema
      // version; it does not persist the reducer upgrade attempted in this batch.
      expect(await stateRow()).toEqual(originalCheckpoint);
      expect(await count('observability_exporter_accepted_sources')).toBe(0);
      expect(await count('observability_exporter_trace_outbox')).toBe(0);
      expect(
        (
          await pool.query(
            'SELECT 1 FROM observability_exporter_event_inbox WHERE processed_at IS NOT NULL',
          )
        ).rowCount,
      ).toBe(0);
    } finally {
      await pool.query(
        'DROP TRIGGER reject_sampling_checkpoint ON observability_exporter_event_inbox; DROP FUNCTION reject_sampling_checkpoint()',
      );
    }
    await expect(runtime.projectOnce()).resolves.toBe(true);
    expect((await stateRow()).projection_state.sampling?.suppressed?.turnCount).toBe('2');
    expect(await count('observability_exporter_accepted_sources')).toBe(2);
    await expect(runtime.deliverOnce()).resolves.toBe(false);
    expect(secretResolve).not.toHaveBeenCalled();
    expect(providerFetch).not.toHaveBeenCalled();
  });

  it('fences stale projection commits without double-counting sampled-out turns', async () => {
    const events = turns(1);
    for (const source of events) await repository.acceptEvent(source);
    const stale = (await repository.claimSession('stale', 30_000))!;
    const result = reduceCanonicalEventBatch(stale.state, events, new Set(), basePolicy);
    await pool.query(
      "UPDATE observability_exporter_session_state SET lease_until = now() - interval '1 second'",
    );
    const current = (await repository.claimSession('current', 30_000))!;
    await repository.completeProjection(current, '9', result.state, [], result.acceptedSourceIds);
    await expect(
      repository.completeProjection(stale, '9', result.state, [], result.acceptedSourceIds),
    ).rejects.toBeInstanceOf(ExporterLeaseLostError);
    expect((await stateRow()).projection_state.sampling?.suppressed?.turnCount).toBe('1');
    expect(await count('observability_exporter_trace_outbox')).toBe(0);
  });

  it('fails closed when Registry changes an existing Session pin between batches', async () => {
    const events = turns(2);
    for (const source of events) await repository.acceptEvent(source);
    await runtimeFor(events, basePolicy, 7).runtime.projectOnce();
    const first = await stateRow();
    await expect(
      runtimeFor(events, { ...basePolicy, bindingVersion: 2, sampleRate: 1 }).runtime.projectOnce(),
    ).rejects.toThrow('projection state is invalid');
    expect(await stateRow()).toEqual(first);
    expect(await count('observability_exporter_trace_outbox')).toBe(0);
    const quarantined = await pool.query(
      'SELECT last_error_code FROM observability_exporter_session_state WHERE quarantined_at IS NOT NULL',
    );
    expect(quarantined.rows).toEqual([{ last_error_code: 'canonical_projection_error' }]);
  });

  it('rejects a sampled-out canonical row at the repository boundary before any outbox write', async () => {
    const events = turns(1);
    for (const source of events) await repository.acceptEvent(source);
    const claim = (await repository.claimSession('boundary', 30_000))!;
    const trace = reduceCanonicalEventBatch(claim.state, events).completedTraces[0]!;
    const context = await new RegistryObservabilityClient({
      internalBaseUrl: 'http://registry.example',
      tokenProvider: async () => 'internal-test-token-at-least-32-bytes',
      fetchImpl: async () => {
        const response = enabledRegistryContext(workspaceId, sessionId);
        (
          (response.binding as Record<string, unknown>).config as Record<string, unknown>
        ).sample_rate = 0;
        return Response.json(response);
      },
    }).resolveContext({ workspaceId, sessionId });
    if (context.status !== 'enabled') throw new Error('expected enabled context');
    await expect(
      repository.completeProjection(claim, '9', initialCanonicalProjectionState(), [
        { trace, deliveryContext: context.deliveryContext },
      ]),
    ).rejects.toThrow('sampled-out trace cannot enter');
    expect(await count('observability_exporter_trace_outbox')).toBe(0);
    expect((await stateRow()).next_seq).toBe('0');
  });
});

function turns(count: number): Event[] {
  return Array.from({ length: count }, (_, index) =>
    completedPrimaryTurnEvents('model_observation_kind', `_sampling_${index}`)
      .filter((source) => !source.id.startsWith('evt_queued'))
      .map((source) => ({
        ...source,
        workspaceId,
        sessionId,
        seq: source.seq + index * 10,
        id: /^evt_[0-9]+$/u.test(source.id) ? `${source.id}_sampling_${index}` : source.id,
      })),
  ).flat();
}
