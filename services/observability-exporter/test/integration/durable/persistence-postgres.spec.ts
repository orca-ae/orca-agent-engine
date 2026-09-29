// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  applyObservabilityExporterMigrations,
  ObservabilityExporterRepository,
  type ClaimedOutboxItem,
} from '../../../src/persistence.js';
import { selectCanonicalReplayEvents } from '../../../src/runtime.js';
import {
  initialCanonicalProjectionState,
  reduceCanonicalEventBatch,
} from '../../../src/projector.js';
import { boundedAgentEventId } from '../../../src/event-identity.js';
import type {
  OtlpDeliveryOutcome,
  PinnedDeliveryContext,
  ProjectedTrace,
} from '../../../src/types.js';
import {
  TRANSCRIPT_SECRET,
  completedPrimaryTurnEvents,
  completedProjectedTrace,
  event,
} from '../../support/events.js';

const ADMIN_DATABASE_URL =
  process.env['OBSERVABILITY_EXPORTER_TEST_ADMIN_DATABASE_URL'] ??
  'postgres://orca:orca@localhost:5432/postgres';

const deliveryContext: PinnedDeliveryContext = {
  organizationId: 'org_persistence',
  bindingId: 'aob_persistence',
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

describe('ObservabilityExporterRepository (real Postgres)', () => {
  let adminPool: Pool;
  let pool: Pool;
  let repository: ObservabilityExporterRepository;
  let databaseName: string;

  beforeAll(async () => {
    adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    databaseName = `observability_exporter_${process.pid}_${randomBytes(4).toString('hex')}`;
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

  it('idempotently restores the durable attempt-count schema before inserts', async () => {
    await pool.query(`
      ALTER TABLE observability_exporter_event_inbox
        ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'legacy'
    `);
    await pool.query(`
      ALTER TABLE observability_exporter_trace_outbox
        DROP COLUMN delivery_attempt_count,
        ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
        ADD COLUMN trace_schema_version TEXT NOT NULL DEFAULT 'legacy'
    `);
    await pool.query(`
      ALTER TABLE observability_exporter_event_inbox
        ALTER COLUMN source_kind DROP DEFAULT
    `);
    await pool.query(`
      ALTER TABLE observability_exporter_trace_outbox
        ALTER COLUMN trace_schema_version DROP DEFAULT
    `);

    await applyObservabilityExporterMigrations(pool);
    await applyObservabilityExporterMigrations(pool);

    const removed = await pool.query<{ column_name: string }>(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name IN (
          'observability_exporter_event_inbox',
          'observability_exporter_trace_outbox'
        )
        AND column_name IN ('source_kind', 'trace_schema_version', 'attempt_count')
    `);
    expect(removed.rows).toEqual([]);
    const attemptColumn = await pool.query<{
      data_type: string;
      is_nullable: string;
      column_default: string | null;
    }>(`
      SELECT data_type, is_nullable, column_default
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'observability_exporter_trace_outbox'
        AND column_name = 'delivery_attempt_count'
    `);
    expect(attemptColumn.rows).toEqual([
      expect.objectContaining({ data_type: 'bigint', is_nullable: 'NO' }),
    ]);
    expect(attemptColumn.rows[0]?.column_default).toContain('0');
    const attemptConstraint = await pool.query<{ definition: string }>(`
      SELECT pg_get_constraintdef(oid) AS definition
      FROM pg_constraint
      WHERE conrelid = 'observability_exporter_trace_outbox'::regclass
        AND conname = 'observability_exporter_outbox_delivery_attempt_nonnegative'
    `);
    expect(attemptConstraint.rows).toHaveLength(1);
    expect(attemptConstraint.rows[0]?.definition).toContain('delivery_attempt_count >= 0');
    const bindingLeaseIndex = await pool.query<{ indexdef: string }>(`
      SELECT indexdef
      FROM pg_indexes
      WHERE schemaname = 'public'
        AND tablename = 'observability_exporter_trace_outbox'
        AND indexname = 'observability_exporter_outbox_binding_lease_idx'
    `);
    expect(bindingLeaseIndex.rows).toHaveLength(1);
    expect(bindingLeaseIndex.rows[0]?.indexdef).toContain('(binding_id, lease_until)');

    // The prior runtime's startup DDL drops only its legacy attempt_count
    // column. Starting it after this migration must not remove the new state.
    await pool.query(`
      ALTER TABLE observability_exporter_trace_outbox
        DROP COLUMN IF EXISTS attempt_count
    `);
    const mixedVersionColumn = await pool.query<{ exists: boolean }>(`
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'observability_exporter_trace_outbox'
          AND column_name = 'delivery_attempt_count'
      ) AS exists
    `);
    expect(mixedVersionColumn.rows).toEqual([{ exists: true }]);

    const workspaceId = 'ws_schema_upgrade';
    const sessionId = 'ses_schema_upgrade';
    await repository.acceptEvent(
      event(1, 'session.status_running', {}, { workspaceId, sessionId }),
    );
    const claim = await repository.claimSession('schema-upgrade-projector', 30_000);
    await repository.completeProjection(claim!, '2', initialCanonicalProjectionState(), [
      {
        trace: traceFor(workspaceId, sessionId, '1'.repeat(32), '2'.repeat(16)),
        deliveryContext,
      },
    ]);
    const outbox = await repository.claimOutbox('schema-upgrade-delivery', 30_000);
    expect(outbox).toMatchObject({
      trace: { workspaceId, sessionId },
      attemptCount: 0,
    });
    await repository.markOutboxDelivered(outbox!);
  });

  it('adds nullable no-default delivery outcome columns twice without backfilling legacy terminal rows', async () => {
    await pool.query(`
      ALTER TABLE observability_exporter_trace_outbox
        DROP COLUMN IF EXISTS delivery_outcome,
        DROP COLUMN IF EXISTS delivery_rejected_spans,
        DROP COLUMN IF EXISTS delivery_message_bytes,
        DROP COLUMN IF EXISTS delivery_message_sha256
    `);
    await pool.query(`
      INSERT INTO observability_exporter_trace_outbox (
        organization_id, workspace_id, session_id, binding_id, binding_version,
        trace_id, payload_hash, canonical_trace, delivery_context, status,
        delivered_at, suppressed_at, suppression_reason
      )
      SELECT 'org_legacy_outcome', 'ws_legacy_outcome', status, 'aob_legacy_outcome', 1,
             repeat('a', 32), repeat('b', 64), '{}'::jsonb, '{}'::jsonb, status,
             CASE WHEN status = 'delivered' THEN now() END,
             CASE WHEN status = 'suppressed' THEN now() END,
             CASE WHEN status = 'suppressed' THEN 'partial_rejection' END
      FROM unnest(ARRAY['pending', 'delivered', 'suppressed']) AS statuses(status)
    `);

    await applyObservabilityExporterMigrations(pool);
    await applyObservabilityExporterMigrations(pool);

    const columns = await pool.query(`
      SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'observability_exporter_trace_outbox'
        AND column_name IN (
          'delivery_outcome', 'delivery_rejected_spans',
          'delivery_message_bytes', 'delivery_message_sha256'
        )
      ORDER BY column_name
    `);
    expect(columns.rows).toEqual([
      {
        column_name: 'delivery_message_bytes',
        data_type: 'integer',
        is_nullable: 'YES',
        column_default: null,
      },
      {
        column_name: 'delivery_message_sha256',
        data_type: 'text',
        is_nullable: 'YES',
        column_default: null,
      },
      {
        column_name: 'delivery_outcome',
        data_type: 'text',
        is_nullable: 'YES',
        column_default: null,
      },
      {
        column_name: 'delivery_rejected_spans',
        data_type: 'bigint',
        is_nullable: 'YES',
        column_default: null,
      },
    ]);
    const legacyRows = await pool.query(`
      SELECT status, delivery_outcome, delivery_rejected_spans,
             delivery_message_bytes, delivery_message_sha256
      FROM observability_exporter_trace_outbox
      WHERE workspace_id = 'ws_legacy_outcome'
      ORDER BY status
    `);
    expect(legacyRows.rows).toEqual(
      ['delivered', 'pending', 'suppressed'].map((status) => ({
        status,
        delivery_outcome: null,
        delivery_rejected_spans: null,
        delivery_message_bytes: null,
        delivery_message_sha256: null,
      })),
    );
    // Leave the shared fixture with no pending migration-only payload.
    await pool.query(`
      UPDATE observability_exporter_trace_outbox
      SET status = 'suppressed', suppressed_at = now(), suppression_reason = 'test_complete'
      WHERE workspace_id = 'ws_legacy_outcome' AND status = 'pending'
    `);
  });

  it('keeps inbox content-free while atomically advancing state and writing a fenced canonical outbox row', async () => {
    const events = completedPrimaryTurnEvents();
    for (const source of events) {
      await expect(repository.acceptEvent(source)).resolves.toBe('accepted');
    }
    await expect(repository.acceptEvent(events[0]!)).resolves.toBe('duplicate');

    const claim = await repository.claimSession('persistence-projector', 30_000);
    expect(claim).not.toBeNull();
    const reduced = reduceCanonicalEventBatch(claim!.state, events);
    expect(reduced.issues).toEqual([]);
    const stateWithUntrustedFields = Object.assign(reduced.state, {
      transcriptContent: TRANSCRIPT_SECRET,
    });
    const traceWithUntrustedFields = Object.assign(completedProjectedTrace(), {
      transcriptContent: TRANSCRIPT_SECRET,
    });
    const traceWithUnsafeRootMetadata: ProjectedTrace = {
      ...completedProjectedTrace(),
      root: {
        ...completedProjectedTrace().root,
        metadata: {
          ...completedProjectedTrace().root.metadata,
          transcript_content: TRANSCRIPT_SECRET,
        },
      },
    };
    await expect(
      repository.completeProjection(
        claim!,
        '9',
        reduced.state,
        [{ trace: traceWithUnsafeRootMetadata, deliveryContext }],
        reduced.acceptedSourceIds,
      ),
    ).rejects.toThrow('outbox trace is invalid');
    const baseTrace = completedProjectedTrace();
    const traceWithUnsafeChildMetadata: ProjectedTrace = {
      ...baseTrace,
      spans: baseTrace.spans.map((span, index) =>
        index === 0
          ? {
              ...span,
              metadata: { ...span.metadata, transcript_content: TRANSCRIPT_SECRET },
            }
          : span,
      ),
    };
    await expect(
      repository.completeProjection(
        claim!,
        '9',
        reduced.state,
        [{ trace: traceWithUnsafeChildMetadata, deliveryContext }],
        reduced.acceptedSourceIds,
      ),
    ).rejects.toThrow('outbox trace is invalid');
    const traceWithUnboundedModel: ProjectedTrace = {
      ...baseTrace,
      spans: baseTrace.spans.map((span, index) =>
        index === 0
          ? {
              ...span,
              modelSummary: { ...span.modelSummary, requestedModel: 'x'.repeat(257) },
            }
          : span,
      ),
    };
    await expect(
      repository.completeProjection(
        claim!,
        '9',
        reduced.state,
        [{ trace: traceWithUnboundedModel, deliveryContext }],
        reduced.acceptedSourceIds,
      ),
    ).rejects.toThrow('outbox trace is invalid');

    const contextWithUntrustedFields = {
      ...deliveryContext,
      deliverySecretRef: 'sk-persistence-secret',
    };
    await repository.completeProjection(
      claim!,
      '9',
      stateWithUntrustedFields,
      [{ trace: traceWithUntrustedFields, deliveryContext: contextWithUntrustedFields }],
      reduced.acceptedSourceIds,
    );
    await expect(
      repository.loadAcceptedSourceIds(claim!, ['evt_user_turn', 'evt_not_accepted']),
    ).resolves.toEqual(new Set(['evt_user_turn']));

    const claimedOutbox = await repository.claimOutbox('persistence-delivery', 30_000);
    expect(claimedOutbox).toMatchObject({
      trace: {
        workspaceId: events[0]!.workspaceId,
        sessionId: events[0]!.sessionId,
        traceId: completedProjectedTrace().traceId,
      },
      deliveryContext: { bindingId: deliveryContext.bindingId },
    });
    await repository.markOutboxDelivered(claimedOutbox!);

    const persisted = await pool.query(`
      SELECT jsonb_build_object(
        'inbox', (SELECT jsonb_agg(to_jsonb(i)) FROM observability_exporter_event_inbox i),
        'accepted', (SELECT jsonb_agg(to_jsonb(a)) FROM observability_exporter_accepted_sources a),
        'state', (SELECT jsonb_agg(to_jsonb(s)) FROM observability_exporter_session_state s),
        'outbox', (SELECT jsonb_agg(to_jsonb(o)) FROM observability_exporter_trace_outbox o)
      ) AS state
    `);
    const encoded = JSON.stringify(persisted.rows[0]!.state);
    expect(encoded).not.toContain(TRANSCRIPT_SECRET);
    expect(encoded).not.toContain('queued and unaccepted');
    expect(encoded).not.toContain('sk-persistence-secret');
    expect(encoded).toContain('payload_hash');
    expect(encoded).toContain('source_hash');
  });

  it('allows authoritative Kafka replay to read ahead of committed inbox identities', async () => {
    const suffix = randomBytes(4).toString('hex');
    const workspaceId = `ws_read_ahead_${suffix}`;
    const sessionId = `ses_read_ahead_${suffix}`;
    const events = completedPrimaryTurnEvents('model_observation_kind', `_${suffix}`);
    for (const source of events) {
      source.workspaceId = workspaceId;
      source.sessionId = sessionId;
    }

    // Event-source ACK has committed only the work notification that made this
    // Session claimable. Authoritative Kafka replay can already see later rows.
    await repository.acceptEvent(events[0]!);
    const claim = await repository.claimSession('read-ahead-projector', 30_000);
    expect(claim).not.toBeNull();
    const inboxIdentities = await repository.loadInboxEventIdentities(
      claim!,
      events.map((source) => source.id),
    );
    expect(inboxIdentities).toHaveLength(1);

    const replay = selectCanonicalReplayEvents(events, inboxIdentities);
    expect(replay.conflict).toBeUndefined();
    expect(replay.events).toHaveLength(events.length);
    const reduced = reduceCanonicalEventBatch(claim!.state, replay.events);
    expect(reduced.issues).toEqual([]);
    await repository.completeProjection(
      claim!,
      String(events.at(-1)!.seq + 1),
      reduced.state,
      reduced.completedTraces.map((trace) => ({ trace, deliveryContext })),
      reduced.acceptedSourceIds,
    );

    const projectedBeforeAck = await pool.query<{ inbox_count: string; outbox_count: string }>(
      `SELECT
         (SELECT count(*)::text FROM observability_exporter_event_inbox
           WHERE workspace_id = $1 AND session_id = $2) AS inbox_count,
         (SELECT count(*)::text FROM observability_exporter_trace_outbox
           WHERE workspace_id = $1 AND session_id = $2) AS outbox_count`,
      [workspaceId, sessionId],
    );
    expect(projectedBeforeAck.rows).toEqual([{ inbox_count: '1', outbox_count: '1' }]);

    for (const source of events.slice(1)) await repository.acceptEvent(source);
    const lateInbox = await pool.query<{ pending_count: string }>(
      `SELECT count(*) FILTER (WHERE processed_at IS NULL)::text AS pending_count
         FROM observability_exporter_event_inbox
        WHERE workspace_id = $1 AND session_id = $2`,
      [workspaceId, sessionId],
    );
    expect(lateInbox.rows).toEqual([{ pending_count: '0' }]);
    expect(await repository.claimSession('read-ahead-reclaim', 30_000)).toBeNull();

    await pool.query(
      `UPDATE observability_exporter_trace_outbox
          SET status = 'delivered', delivered_at = now()
        WHERE workspace_id = $1 AND session_id = $2`,
      [workspaceId, sessionId],
    );
  });

  it('digests oversized contract-valid event IDs before indexed or reducer persistence', async () => {
    const oversizedEventId = `evt_${'x'.repeat(4_000)}`;
    const events = completedPrimaryTurnEvents('model_observation_kind', '_oversized');
    events[0] = { ...events[0]!, id: oversizedEventId };
    events[2] = {
      ...events[2]!,
      payload: Buffer.from(
        JSON.stringify({ type: 'session.user_event_processed', user_event_id: oversizedEventId }),
      ),
    };
    for (const source of events) {
      await expect(repository.acceptEvent(source)).resolves.toBe('accepted');
    }
    await expect(repository.acceptEvent(events[0]!)).resolves.toBe('duplicate');

    const claim = await repository.claimSession('oversized-projector', 30_000);
    expect(claim).toMatchObject({ sessionId: events[0]!.sessionId });
    const inboxIdentities = await repository.loadInboxEventIdentities(
      claim!,
      events.map((source) => source.id),
    );
    expect(inboxIdentities).toHaveLength(events.length);
    expect(inboxIdentities.every(({ eventKey }) => /^[0-9a-f]{64}$/u.test(eventKey))).toBe(true);
    const reduced = reduceCanonicalEventBatch(claim!.state, events);
    const boundedEventId = boundedAgentEventId(oversizedEventId)!;
    expect(reduced.completedTraces[0]!.anchorEventId).toBe(boundedEventId);
    expect(reduced.acceptedSourceIds).toEqual([boundedEventId]);
    await repository.completeProjection(
      claim!,
      '9',
      reduced.state,
      reduced.completedTraces.map((trace) => ({ trace, deliveryContext })),
      reduced.acceptedSourceIds,
    );
    await expect(repository.loadAcceptedSourceIds(claim!, [boundedEventId])).resolves.toEqual(
      new Set([boundedEventId]),
    );

    const stored = await pool.query<{ encoded: string; key_length: number }>(
      `
        SELECT jsonb_build_object(
          'inbox', (SELECT jsonb_agg(to_jsonb(i)) FROM observability_exporter_event_inbox i
                    WHERE workspace_id = $1 AND session_id = $2),
          'accepted', (SELECT jsonb_agg(to_jsonb(a)) FROM observability_exporter_accepted_sources a
                       WHERE workspace_id = $1 AND session_id = $2),
          'state', (SELECT projection_state FROM observability_exporter_session_state
                    WHERE workspace_id = $1 AND session_id = $2),
          'outbox', (SELECT jsonb_agg(to_jsonb(o)) FROM observability_exporter_trace_outbox o
                     WHERE workspace_id = $1 AND session_id = $2)
        )::text AS encoded,
        (SELECT length(event_key) FROM observability_exporter_event_inbox
         WHERE workspace_id = $1 AND session_id = $2 LIMIT 1) AS key_length
      `,
      [events[0]!.workspaceId, events[0]!.sessionId],
    );
    expect(stored.rows[0]!.key_length).toBe(64);
    expect(stored.rows[0]!.encoded).toContain(boundedEventId);
    expect(stored.rows[0]!.encoded).not.toContain(oversizedEventId);
  });

  it('quarantines a conflicting Kafka envelope identity after recording only hashes', async () => {
    const source = event(
      1,
      'user.message',
      { content: TRANSCRIPT_SECRET },
      {
        id: 'evt_conflict',
        workspaceId: 'ws_conflict',
        sessionId: 'ses_conflict',
        producedBy: 'client',
      },
    );
    await expect(repository.acceptEvent(source)).resolves.toBe('accepted');
    await expect(repository.acceptEvent({ ...source, seq: 2 })).resolves.toBe('duplicate');
    await expect(
      repository.acceptEvent({ ...source, payload: Buffer.from('changed but never persisted') }),
    ).resolves.toBe('conflict');
    expect(await repository.claimSession('conflict-projector', 30_000)).toBeNull();

    const stored = await pool.query<{ existing_hash: string; incoming_hash: string }>(`
      SELECT existing_hash, incoming_hash
      FROM observability_exporter_conflicts
      WHERE workspace_id = 'ws_conflict' AND session_id = 'ses_conflict'
    `);
    expect(stored.rows).toHaveLength(1);
    expect(stored.rows[0]).toMatchObject({
      existing_hash: expect.stringMatching(/^[0-9a-f]{64}$/),
      incoming_hash: expect.stringMatching(/^[0-9a-f]{64}$/),
    });
  });

  it('continues inserting later traces when an earlier trace identity conflicts', async () => {
    const workspaceId = 'ws_trace_conflict';
    const sessionId = 'ses_trace_conflict';
    const firstEvent = event(1, 'session.status_running', {}, { workspaceId, sessionId });
    await repository.acceptEvent(firstEvent);
    const firstClaim = await repository.claimSession('trace-conflict-one', 30_000);
    const firstTrace = traceFor(workspaceId, sessionId, 'a'.repeat(32), 'b'.repeat(16));
    await repository.completeProjection(firstClaim!, '2', initialCanonicalProjectionState(), [
      { trace: firstTrace, deliveryContext },
    ]);

    await repository.acceptEvent(event(2, 'session.status_idle', {}, { workspaceId, sessionId }));
    const secondClaim = await repository.claimSession('trace-conflict-two', 30_000);
    const conflictingTrace: ProjectedTrace = {
      ...firstTrace,
      root: {
        ...firstTrace.root,
        status: 'error',
        metadata: {
          ...firstTrace.root.metadata,
          'orca.turn.terminal_reason': 'retries_exhausted',
        },
      },
    };
    const laterTrace = traceFor(workspaceId, sessionId, 'c'.repeat(32), 'd'.repeat(16));
    await repository.completeProjection(secondClaim!, '3', initialCanonicalProjectionState(), [
      { trace: conflictingTrace, deliveryContext },
      { trace: laterTrace, deliveryContext },
    ]);

    const outbox = await pool.query<{ trace_id: string }>(`
      SELECT trace_id
      FROM observability_exporter_trace_outbox
      WHERE workspace_id = 'ws_trace_conflict' AND session_id = 'ses_trace_conflict'
      ORDER BY trace_id
    `);
    expect(outbox.rows.map((row) => row.trace_id)).toEqual(['a'.repeat(32), 'c'.repeat(32)]);
    const conflict = await pool.query<{ error: string; quarantined: boolean; conflicts: string }>(`
      SELECT s.last_error_code AS error,
             s.quarantined_at IS NOT NULL AS quarantined,
             count(c.id)::text AS conflicts
      FROM observability_exporter_session_state s
      JOIN observability_exporter_conflicts c USING (workspace_id, session_id)
      WHERE s.workspace_id = 'ws_trace_conflict' AND s.session_id = 'ses_trace_conflict'
      GROUP BY s.last_error_code, s.quarantined_at
    `);
    expect(conflict.rows).toEqual([{ error: 'trace_conflict', quarantined: true, conflicts: '1' }]);
  });

  it('rejects malformed persisted reducer state rather than replaying it as Transcript data', async () => {
    const source = event(
      1,
      'user.message',
      {},
      {
        id: 'evt_bad_state',
        workspaceId: 'ws_bad_state',
        sessionId: 'ses_bad_state',
        producedBy: 'client',
      },
    );
    await repository.acceptEvent(source);
    await pool.query(
      `UPDATE observability_exporter_session_state
         SET projection_state = $3::jsonb
       WHERE workspace_id = $1 AND session_id = $2`,
      [source.workspaceId, source.sessionId, JSON.stringify({ version: 1, pendingInputs: [] })],
    );
    await expect(repository.claimSession('bad-state-projector', 30_000)).rejects.toThrow(
      'canonical observability projection state is invalid',
    );
    const quarantined = await pool.query<{ last_error_code: string; quarantined: boolean }>(`
      SELECT last_error_code, quarantined_at IS NOT NULL AS quarantined
      FROM observability_exporter_session_state
      WHERE workspace_id = 'ws_bad_state' AND session_id = 'ses_bad_state'
    `);
    expect(quarantined.rows).toEqual([
      { last_error_code: 'invalid_projection_state', quarantined: true },
    ]);
  });

  it('durably delays retries while shutdown release preserves the attempt count', async () => {
    await pool.query(`
      UPDATE observability_exporter_trace_outbox
      SET status = 'delivered', delivered_at = now()
      WHERE status = 'pending'
    `);
    const suffix = randomBytes(4).toString('hex');
    const workspaceId = `ws_retry_${suffix}`;
    const sessionId = `ses_retry_${suffix}`;
    await repository.acceptEvent(
      event(1, 'session.status_running', {}, { workspaceId, sessionId }),
    );
    const projection = await repository.claimSession('retry-projector', 30_000);
    expect(projection).toMatchObject({ workspaceId, sessionId });
    await repository.completeProjection(projection!, '2', initialCanonicalProjectionState(), [
      {
        trace: traceFor(workspaceId, sessionId, 'a'.repeat(32), 'b'.repeat(16)),
        deliveryContext,
      },
    ]);

    const firstClaim = await repository.claimOutbox('retry-delivery-a', 30_000);
    expect(firstClaim).toMatchObject({ attemptCount: 0 });
    await repository.scheduleOutboxRetry(firstClaim!, 5_000, 'otlp_transport');

    const scheduled = await pool.query<{
      attempt_count: string;
      last_error_code: string;
      delayed: boolean;
      released: boolean;
    }>(
      `SELECT delivery_attempt_count::text AS attempt_count,
              last_error_code,
              available_at > now() + interval '4 seconds' AS delayed,
              lease_owner IS NULL AND lease_until IS NULL AS released
         FROM observability_exporter_trace_outbox
        WHERE id = $1::bigint`,
      [firstClaim!.id],
    );
    expect(scheduled.rows).toEqual([
      {
        attempt_count: '1',
        last_error_code: 'otlp_transport',
        delayed: true,
        released: true,
      },
    ]);
    await expect(repository.claimOutbox('retry-too-soon', 30_000)).resolves.toBeNull();

    await pool.query(
      `UPDATE observability_exporter_trace_outbox SET available_at = now() WHERE id = $1::bigint`,
      [firstClaim!.id],
    );
    await pool.query(
      `UPDATE observability_exporter_binding_cooldowns
          SET retry_not_before = now()
        WHERE binding_id = $1`,
      [deliveryContext.bindingId],
    );
    const retryClaim = await repository.claimOutbox('retry-delivery-b', 30_000);
    expect(retryClaim).toMatchObject({ attemptCount: 1 });
    await repository.releaseOutboxClaim(retryClaim!);

    const released = await pool.query<{
      attempt_count: string;
      last_error_code: string;
      available: boolean;
    }>(
      `SELECT delivery_attempt_count::text AS attempt_count,
              last_error_code,
              available_at <= now() AS available
         FROM observability_exporter_trace_outbox
        WHERE id = $1::bigint`,
      [firstClaim!.id],
    );
    expect(released.rows).toEqual([
      { attempt_count: '1', last_error_code: 'delivery_failed', available: true },
    ]);

    const shutdownReleased = await repository.claimOutbox('retry-delivery-c', 30_000);
    expect(shutdownReleased).toMatchObject({ attemptCount: 1 });
    await repository.markOutboxSuppressed(shutdownReleased!, 'test_complete');
  });

  it('shares retry cooldown within one binding without blocking another binding', async () => {
    await pool.query(`
      UPDATE observability_exporter_trace_outbox
      SET status = 'delivered', delivered_at = now()
      WHERE status = 'pending'
    `);
    const suffix = randomBytes(4).toString('hex');
    const bindingA = `aob_cooldown_a_${suffix}`;
    const bindingB = `aob_cooldown_b_${suffix}`;
    const contextA = { ...deliveryContext, bindingId: bindingA };
    const contextB = { ...deliveryContext, bindingId: bindingB };
    await enqueueOutboxTrace(
      repository,
      `ws_cooldown_a1_${suffix}`,
      `ses_cooldown_a1_${suffix}`,
      contextA,
      '1'.repeat(32),
      'a'.repeat(16),
    );
    await enqueueOutboxTrace(
      repository,
      `ws_cooldown_a2_${suffix}`,
      `ses_cooldown_a2_${suffix}`,
      contextA,
      '2'.repeat(32),
      'b'.repeat(16),
    );
    await enqueueOutboxTrace(
      repository,
      `ws_cooldown_b_${suffix}`,
      `ses_cooldown_b_${suffix}`,
      contextB,
      '3'.repeat(32),
      'c'.repeat(16),
    );

    const firstA = await repository.claimOutbox('cooldown-delivery-a', 30_000);
    expect(firstA?.deliveryContext.bindingId).toBe(bindingA);
    await repository.scheduleOutboxRetry(firstA!, 30_000, 'otlp_http_429');

    const otherBinding = await repository.claimOutbox('cooldown-delivery-b', 30_000);
    expect(otherBinding?.deliveryContext.bindingId).toBe(bindingB);
    await repository.markOutboxSuppressed(otherBinding!, 'test_complete');
    await expect(repository.claimOutbox('cooldown-blocked-a', 30_000)).resolves.toBeNull();
  });

  it('reselects another ready binding after losing a concurrent delivery claim', async () => {
    await pool.query(`
      UPDATE observability_exporter_trace_outbox
      SET status = 'delivered', delivered_at = now()
      WHERE status = 'pending'
    `);
    const suffix = randomBytes(4).toString('hex');
    const bindingIds = [`aob_reselect_a_${suffix}`, `aob_reselect_b_${suffix}`];
    const queuedBindings = [bindingIds[0]!, bindingIds[0]!, bindingIds[1]!];
    for (const [index, bindingId] of queuedBindings.entries()) {
      await enqueueOutboxTrace(
        repository,
        `ws_reselect_${index}_${suffix}`,
        `ses_reselect_${index}_${suffix}`,
        { ...deliveryContext, bindingId },
        String(index + 1).repeat(32),
        String.fromCharCode(97 + index).repeat(16),
      );
    }

    // Bound failure cleanup even if a claimant never gets past the held lock.
    const claimPool = new Pool({
      connectionString: pool.options.connectionString,
      max: 2,
      connectionTimeoutMillis: 3_000,
      statement_timeout: 3_000,
    });
    const claimingRepository = new ObservabilityExporterRepository(claimPool);
    const blocker = await pool.connect();
    const claims: ReturnType<ObservabilityExporterRepository['claimOutbox']>[] = [];
    try {
      await blocker.query('BEGIN');
      await blocker.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [bindingIds[0]]);
      for (let index = 0; index < 2; index += 1) {
        const claim = claimingRepository.claimOutbox(`reselect-delivery-${index}`, 30_000);
        claims.push(claim);
        // Observe errors immediately; the assertions below still await the original promises.
        void claim.catch(() => undefined);
      }
      // Both replicas must have selected A before either can claim it. No timing sleeps.
      await expect
        .poll(
          async () => {
            const waiting = await blocker.query<{ count: number }>(`
              SELECT count(*)::int AS count
              FROM pg_locks waiting
              JOIN pg_locks held USING (locktype, database, classid, objid, objsubid)
              WHERE held.pid = pg_backend_pid() AND held.locktype = 'advisory'
                AND held.granted AND NOT waiting.granted
            `);
            return waiting.rows[0]?.count;
          },
          { timeout: 2_000, interval: 10 },
        )
        .toBe(2);
      await blocker.query('COMMIT');

      const claimed = await Promise.all(claims);
      expect(claimed.map((claim) => claim?.deliveryContext.bindingId).sort()).toEqual(bindingIds);
      await expect(repository.claimOutbox('reselect-exhausted', 30_000)).resolves.toBeNull();
    } finally {
      await blocker.query('ROLLBACK');
      blocker.release();
      await Promise.allSettled(claims);
      await claimPool.end();
    }
  });

  it('serializes concurrent delivery claims for one binding', async () => {
    await pool.query(`
      UPDATE observability_exporter_trace_outbox
      SET status = 'delivered', delivered_at = now()
      WHERE status = 'pending'
    `);
    const suffix = randomBytes(4).toString('hex');
    const bindingId = `aob_serialized_${suffix}`;
    const context = { ...deliveryContext, bindingId };
    for (let index = 0; index < 4; index += 1) {
      await enqueueOutboxTrace(
        repository,
        `ws_serialized_${index}_${suffix}`,
        `ses_serialized_${index}_${suffix}`,
        context,
        String(index + 1).repeat(32),
        String.fromCharCode(97 + index).repeat(16),
      );
    }

    const claims = await Promise.all(
      Array.from({ length: 4 }, (_value, index) =>
        repository.claimOutbox(`serialized-delivery-${index}`, 30_000),
      ),
    );
    const claimed = claims.filter((claim) => claim !== null);
    expect(claimed).toHaveLength(1);
    expect(claimed[0]?.deliveryContext.bindingId).toBe(bindingId);
    await repository.markOutboxSuppressed(claimed[0]!, 'test_complete');

    const next = await repository.claimOutbox('serialized-delivery-next', 30_000);
    expect(next?.deliveryContext.bindingId).toBe(bindingId);
    await repository.markOutboxSuppressed(next!, 'test_complete');
  });

  it.each([
    {
      corruption: "organization_id = 'org_corrupt'",
      expectedError: 'outbox identity is invalid',
    },
    {
      corruption: `delivery_context = jsonb_set(delivery_context, '{protocol}', '"http/protobuf"')`,
      expectedError: 'delivery context is invalid',
    },
  ])('suppresses corrupted persisted outbox state: $expectedError', async (testCase) => {
    await pool.query(
      `UPDATE observability_exporter_trace_outbox
          SET status = 'delivered', delivered_at = now()
        WHERE status = 'pending'`,
    );
    const suffix = randomBytes(4).toString('hex');
    const workspaceId = `ws_outbox_identity_${suffix}`;
    const sessionId = `ses_outbox_identity_${suffix}`;
    const source = event(1, 'session.status_running', {}, { workspaceId, sessionId });
    await repository.acceptEvent(source);
    const claim = await repository.claimSession('outbox-identity-projector', 30_000);
    const trace = traceFor(workspaceId, sessionId, 'e'.repeat(32), 'f'.repeat(16));
    await repository.completeProjection(claim!, '2', initialCanonicalProjectionState(), [
      { trace, deliveryContext },
    ]);
    await pool.query(
      `UPDATE observability_exporter_trace_outbox
          SET ${testCase.corruption}
        WHERE workspace_id = $1 AND session_id = $2`,
      [workspaceId, sessionId],
    );

    await expect(repository.claimOutbox('outbox-identity-delivery', 30_000)).rejects.toThrow(
      testCase.expectedError,
    );
    const status = await pool.query<{ status: string; suppression_reason: string }>(
      `SELECT status, suppression_reason
         FROM observability_exporter_trace_outbox
        WHERE workspace_id = $1 AND session_id = $2`,
      [workspaceId, sessionId],
    );
    expect(status.rows).toEqual([
      { status: 'suppressed', suppression_reason: 'invalid_outbox_state' },
    ]);
  });

  it('fences expired projection and outbox owners after worker takeover', async () => {
    const suffix = randomBytes(4).toString('hex');
    const workspaceId = `ws_takeover_${suffix}`;
    const sessionId = `ses_takeover_${suffix}`;
    const events = completedPrimaryTurnEvents('model_observation_kind', `_${suffix}`);
    for (const source of events) {
      source.workspaceId = workspaceId;
      source.sessionId = sessionId;
      await repository.acceptEvent(source);
    }

    const projectionA = await repository.claimSession('takeover-projector-a', 20);
    expect(projectionA).not.toBeNull();
    await sleep(50);
    const projectionB = await repository.claimSession('takeover-projector-b', 30_000);
    expect(projectionB).not.toBeNull();
    await repository.renewProjectionClaim(projectionB!, 1_000);
    const projectionLease = await pool.query<{ healthy: boolean }>(
      `SELECT lease_until > now() + interval '20 seconds' AS healthy
         FROM observability_exporter_session_state
        WHERE workspace_id = $1 AND session_id = $2`,
      [workspaceId, sessionId],
    );
    expect(projectionLease.rows[0]?.healthy).toBe(true);
    const reduced = reduceCanonicalEventBatch(projectionB!.state, events);
    expect(reduced.issues).toEqual([]);

    await expect(
      repository.quarantineProjectionClaim(projectionA!, 'stale_projection_error'),
    ).rejects.toThrow('lease lost');
    await expect(
      repository.completeProjection(
        projectionA!,
        '9',
        reduced.state,
        reduced.completedTraces.map((trace) => ({ trace, deliveryContext })),
        reduced.acceptedSourceIds,
      ),
    ).rejects.toThrow('lease lost');
    await repository.completeProjection(
      projectionB!,
      '9',
      reduced.state,
      reduced.completedTraces.map((trace) => ({ trace, deliveryContext })),
      reduced.acceptedSourceIds,
    );

    const outboxA = await repository.claimOutbox('takeover-delivery-a', 20);
    expect(outboxA).not.toBeNull();
    await sleep(50);
    const outboxB = await repository.claimOutbox('takeover-delivery-b', 30_000);
    expect(outboxB).not.toBeNull();
    await repository.renewOutboxClaim(outboxB!, 1_000);
    const outboxLease = await pool.query<{ healthy: boolean }>(
      `SELECT lease_until > now() + interval '20 seconds' AS healthy
         FROM observability_exporter_trace_outbox
        WHERE id = $1::bigint`,
      [outboxB!.id],
    );
    expect(outboxLease.rows[0]?.healthy).toBe(true);
    await expect(
      repository.scheduleOutboxRetry(outboxA!, 1_000, 'stale_delivery_retry'),
    ).rejects.toThrow('lease lost');
    await expect(repository.markOutboxDelivered(outboxA!)).rejects.toThrow('lease lost');
    await repository.markOutboxDelivered(outboxB!);
  });

  describe('delivery outcomes', () => {
    const digest = 'a'.repeat(64);
    const acceptedColumns = {
      status: 'delivered',
      reason: null,
      kind: 'accepted',
      count: '0',
      bytes: null,
      digest: null,
    };
    const warningColumns = { ...acceptedColumns, kind: 'accepted_with_warning', bytes: 1, digest };
    const partialColumns = {
      ...warningColumns,
      status: 'suppressed',
      reason: 'partial_rejection',
      kind: 'partial_rejection',
      count: '1',
      bytes: 0,
    };
    const outcomeCases: {
      name: string;
      outcome: OtlpDeliveryOutcome;
      expected: typeof acceptedColumns | typeof warningColumns | typeof partialColumns;
    }[] = [
      { name: 'accepted', outcome: { kind: 'accepted' }, expected: acceptedColumns },
      {
        name: 'warning with one byte',
        outcome: {
          kind: 'accepted_with_warning',
          rejectedSpans: '0',
          messageBytes: 1,
          messageSha256: digest,
        },
        expected: warningColumns,
      },
      {
        name: 'warning at message byte limit',
        outcome: {
          kind: 'accepted_with_warning',
          rejectedSpans: '0',
          messageBytes: 65_536,
          messageSha256: digest,
        },
        expected: { ...warningColumns, bytes: 65_536 },
      },
      {
        name: 'partial rejection with exact maximum int64 and no message',
        outcome: {
          kind: 'partial_rejection',
          rejectedSpans: '9223372036854775807',
          messageBytes: 0,
          messageSha256: digest,
        },
        expected: { ...partialColumns, count: '9223372036854775807' },
      },
      {
        name: 'partial rejection at message byte limit',
        outcome: {
          kind: 'partial_rejection',
          rejectedSpans: '1',
          messageBytes: 65_536,
          messageSha256: digest,
        },
        expected: { ...partialColumns, bytes: 65_536 },
      },
    ];

    async function claimOutcome(): Promise<ClaimedOutboxItem> {
      const suffix = randomBytes(4).toString('hex');
      await enqueueOutboxTrace(
        repository,
        `ws_outcome_${suffix}`,
        `ses_outcome_${suffix}`,
        { ...deliveryContext, bindingId: `aob_outcome_${suffix}` },
        randomBytes(16).toString('hex'),
        randomBytes(8).toString('hex'),
      );
      const item = await repository.claimOutbox('outcome-delivery', 30_000);
      expect(item?.trace.sessionId).toBe(`ses_outcome_${suffix}`);
      return item!;
    }

    async function readOutcome(item: ClaimedOutboxItem) {
      const result = await pool.query(
        `SELECT status, suppression_reason AS reason, delivery_outcome AS kind,
                delivery_rejected_spans::text AS count, delivery_message_bytes AS bytes,
                delivery_message_sha256 AS digest, lease_owner, lease_until, last_error_code,
                delivered_at IS NOT NULL AS delivered, suppressed_at IS NOT NULL AS suppressed,
                delivery_attempt_count::text AS attempts
         FROM observability_exporter_trace_outbox WHERE id = $1::bigint`,
        [item.id],
      );
      return result.rows[0];
    }

    it.each(outcomeCases)(
      'atomically completes $name without raw content or terminal reclaims',
      async ({ outcome, expected }) => {
        const item = await claimOutcome();
        await pool.query(
          "UPDATE observability_exporter_trace_outbox SET last_error_code = 'delivery_failed' WHERE id = $1::bigint",
          [item.id],
        );
        await repository.completeOutboxDelivery(
          item,
          Object.assign({}, outcome, {
            errorMessage: TRANSCRIPT_SECRET,
            rawMessage: TRANSCRIPT_SECRET,
            credential: 'sk-outcome-secret',
            headers: { authorization: 'sk-outcome-secret' },
          }),
        );

        expect(await readOutcome(item)).toEqual({
          ...expected,
          delivered: expected.status === 'delivered',
          suppressed: expected.status === 'suppressed',
          lease_owner: null,
          lease_until: null,
          last_error_code: null,
          attempts: '0',
        });
        const persisted = await pool.query(
          'SELECT to_jsonb(o) AS row FROM observability_exporter_trace_outbox o WHERE id = $1::bigint',
          [item.id],
        );
        expect(JSON.stringify(persisted.rows)).not.toContain(TRANSCRIPT_SECRET);
        expect(JSON.stringify(persisted.rows)).not.toContain('sk-outcome-secret');
        await expect(repository.claimOutbox('terminal-reclaim', 30_000)).resolves.toBeNull();
        await expect(repository.completeOutboxDelivery(item, { kind: 'accepted' })).rejects.toThrow(
          'lease lost',
        );
        expect(await readOutcome(item)).toMatchObject(expected);
      },
    );

    it('rejects invalid repository metadata without retaining it or changing the active lease', async () => {
      const item = await claimOutcome();
      const before = await readOutcome(item);
      const partial = {
        kind: 'partial_rejection',
        rejectedSpans: '1',
        messageBytes: 0,
        messageSha256: digest,
      };
      for (const outcome of [
        { ...partial, rejectedSpans: '9223372036854775808' },
        { ...partial, rejectedSpans: Number.MAX_SAFE_INTEGER },
        { ...partial, rejectedSpans: '01' },
        { ...partial, messageBytes: 65_537 },
        { ...partial, messageSha256: TRANSCRIPT_SECRET },
        { ...partial, messageBytes: { toPostgres: () => 'sk-outcome-secret' } },
        { ...partial, kind: 'accepted_with_warning', rejectedSpans: '0' },
      ]) {
        await expect(
          repository.completeOutboxDelivery(item, outcome as OtlpDeliveryOutcome),
        ).rejects.toThrow(new Error('observability exporter delivery outcome is invalid'));
        expect(await readOutcome(item)).toEqual(before);
      }
      await repository.completeOutboxDelivery(item, { kind: 'accepted' });
    });

    it('fences expired, stale-generation and wrong-owner outcomes without metadata writes', async () => {
      const expired = await claimOutcome();
      await pool.query(
        "UPDATE observability_exporter_trace_outbox SET lease_until = now() - interval '1 second' WHERE id = $1::bigint",
        [expired.id],
      );
      const beforeExpiry = await readOutcome(expired);
      for (const { outcome } of outcomeCases) {
        await expect(repository.completeOutboxDelivery(expired, outcome)).rejects.toThrow(
          'lease lost',
        );
        expect(await readOutcome(expired)).toEqual(beforeExpiry);
      }
      // Reuse the owner so the stale-claim rejection specifically proves generation fencing.
      const current = await repository.claimOutbox(expired.leaseOwner, 30_000);
      expect(current?.id).toBe(expired.id);
      expect(current?.leaseGeneration).not.toBe(expired.leaseGeneration);
      const beforeTakeover = await readOutcome(current!);
      for (const claim of [expired, { ...current!, leaseOwner: 'wrong-owner' }]) {
        for (const { outcome } of outcomeCases) {
          await expect(repository.completeOutboxDelivery(claim, outcome)).rejects.toThrow(
            'lease lost',
          );
          expect(await readOutcome(current!)).toEqual(beforeTakeover);
        }
      }
      await repository.completeOutboxDelivery(current!, outcomeCases[3]!.outcome);
    });

    it('preserves legacy terminal APIs with all outcome columns null', async () => {
      for (const status of ['delivered', 'suppressed']) {
        const item = await claimOutcome();
        if (status === 'delivered') await repository.markOutboxDelivered(item);
        else await repository.markOutboxSuppressed(item, 'partial_rejection');
        expect(await readOutcome(item)).toMatchObject({
          status,
          kind: null,
          count: null,
          bytes: null,
          digest: null,
          lease_owner: null,
          lease_until: null,
          last_error_code: null,
        });
      }
    });

    it.each([
      { name: 'null outcome with populated count', values: { ...acceptedColumns, kind: null } },
      { name: 'unknown outcome', values: { ...acceptedColumns, kind: 'unknown' } },
      { name: 'pending outcome', values: { ...acceptedColumns, status: 'pending' } },
      { name: 'accepted without rejected count', values: { ...acceptedColumns, count: null } },
      { name: 'accepted with rejected spans', values: { ...acceptedColumns, count: '1' } },
      { name: 'accepted with message bytes', values: { ...acceptedColumns, bytes: 0 } },
      { name: 'accepted with digest', values: { ...acceptedColumns, digest } },
      {
        name: 'accepted with suppression reason',
        values: { ...acceptedColumns, reason: 'partial_rejection' },
      },
      { name: 'suppressed accepted outcome', values: { ...acceptedColumns, status: 'suppressed' } },
      { name: 'warning without count', values: { ...warningColumns, count: null } },
      { name: 'warning with rejected spans', values: { ...warningColumns, count: '1' } },
      { name: 'warning without bytes', values: { ...warningColumns, bytes: null } },
      { name: 'warning with empty message', values: { ...warningColumns, bytes: 0 } },
      { name: 'warning with oversized message', values: { ...warningColumns, bytes: 65_537 } },
      { name: 'warning without digest', values: { ...warningColumns, digest: null } },
      { name: 'partial without count', values: { ...partialColumns, count: null } },
      { name: 'partial with zero rejected spans', values: { ...partialColumns, count: '0' } },
      { name: 'partial with negative rejected spans', values: { ...partialColumns, count: '-1' } },
      { name: 'partial without message bytes', values: { ...partialColumns, bytes: null } },
      { name: 'partial with negative message bytes', values: { ...partialColumns, bytes: -1 } },
      { name: 'partial with oversized message', values: { ...partialColumns, bytes: 65_537 } },
      { name: 'partial without digest', values: { ...partialColumns, digest: null } },
      {
        name: 'partial with uppercase digest',
        values: { ...partialColumns, digest: 'A'.repeat(64) },
      },
      { name: 'partial with short digest', values: { ...partialColumns, digest: 'a'.repeat(63) } },
      { name: 'partial with newline digest', values: { ...partialColumns, digest: digest + '\n' } },
      { name: 'partial without suppression reason', values: { ...partialColumns, reason: null } },
      {
        name: 'partial with wrong suppression reason',
        values: { ...partialColumns, reason: 'other' },
      },
      { name: 'delivered partial outcome', values: { ...partialColumns, status: 'delivered' } },
    ])('rejects inconsistent SQL metadata: $name', async ({ values }) => {
      const item = await claimOutcome();
      const before = await readOutcome(item);
      await expect(
        pool.query(
          `UPDATE observability_exporter_trace_outbox
         SET status = $2, suppression_reason = $3, delivery_outcome = $4,
             delivery_rejected_spans = $5::bigint, delivery_message_bytes = $6::integer,
             delivery_message_sha256 = $7,
             delivered_at = CASE WHEN $2 = 'delivered' THEN now() END,
             suppressed_at = CASE WHEN $2 = 'suppressed' THEN now() END
         WHERE id = $1::bigint`,
          [
            item.id,
            values.status,
            values.reason,
            values.kind,
            values.count,
            values.bytes,
            values.digest,
          ],
        ),
      ).rejects.toMatchObject({ code: '23514' });
      expect(await readOutcome(item)).toEqual(before);
      await repository.markOutboxSuppressed(item, 'test_complete');
    });

    it('rejects int64 overflow at the SQL boundary', async () => {
      const item = await claimOutcome();
      const before = await readOutcome(item);
      await expect(
        pool.query(
          'UPDATE observability_exporter_trace_outbox SET delivery_rejected_spans = $2::bigint WHERE id = $1::bigint',
          [item.id, '9223372036854775808'],
        ),
      ).rejects.toMatchObject({ code: '22003' });
      expect(await readOutcome(item)).toEqual(before);
      await repository.markOutboxSuppressed(item, 'test_complete');
    });
  });
});

function traceFor(
  workspaceId: string,
  sessionId: string,
  traceId: string,
  rootSpanId: string,
): ProjectedTrace {
  const trace = completedProjectedTrace();
  return {
    ...trace,
    traceId,
    workspaceId,
    sessionId,
    root: {
      ...trace.root,
      spanId: rootSpanId,
      metadata: {
        ...trace.root.metadata,
        'orca.workspace.id': workspaceId,
        'orca.session.id': sessionId,
      },
    },
    spans: trace.spans.map((span) => ({ ...span, parentSpanId: rootSpanId })),
  };
}

async function enqueueOutboxTrace(
  repository: ObservabilityExporterRepository,
  workspaceId: string,
  sessionId: string,
  context: PinnedDeliveryContext,
  traceId: string,
  rootSpanId: string,
): Promise<void> {
  await repository.acceptEvent(event(1, 'session.status_running', {}, { workspaceId, sessionId }));
  const claim = await repository.claimSession(`cooldown-projector-${sessionId}`, 30_000);
  expect(claim).toMatchObject({ workspaceId, sessionId });
  await repository.completeProjection(claim!, '2', initialCanonicalProjectionState(), [
    { trace: traceFor(workspaceId, sessionId, traceId, rootSpanId), deliveryContext: context },
  ]);
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
