// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import type { Event } from '@orca/transcript-store-types';
import type { Pool, PoolClient } from 'pg';
import { parsePinnedDeliveryContext, parseProjectedTrace } from './canonical-validation.js';
import { MAX_DELIVERY_RETRY_DELAY_MS } from './delivery-retry.js';
import { eventIdentityKey, isBoundedAgentEventId } from './event-identity.js';
import { MAX_LEASE_OWNER_LENGTH } from './lease-owner.js';
import { parseCanonicalProjectionState, type CanonicalProjectionState } from './projector.js';
import { isTraceSampled, TRACE_SAMPLING_VERSION } from './sampling.js';
import type { OtlpDeliveryOutcome, PinnedDeliveryContext, ProjectedTrace } from './types.js';

export type InboxAcceptance = 'accepted' | 'duplicate' | 'conflict';

// Bound selection races (including repeated SKIP LOCKED misses), not delivery retries.
const MAX_OUTBOX_SELECTION_ATTEMPTS = 8;

export interface ClaimedProjectionSession {
  workspaceId: string;
  sessionId: string;
  nextSeq: string;
  firstPendingSeq: string;
  state: CanonicalProjectionState;
  leaseOwner: string;
  leaseGeneration: string;
}

export interface PendingCanonicalTrace {
  trace: ProjectedTrace;
  deliveryContext: PinnedDeliveryContext;
}

export interface InboxEventIdentity {
  eventKey: string;
  sourceSeq: string;
  sourceHash: string;
}

export interface ClaimedOutboxItem {
  id: string;
  trace: ProjectedTrace;
  deliveryContext: PinnedDeliveryContext;
  attemptCount: number;
  leaseOwner: string;
  leaseGeneration: string;
}

export class ExporterLeaseLostError extends Error {
  constructor(resource: string) {
    super(`observability exporter lease lost for ${resource}`);
    this.name = 'ExporterLeaseLostError';
  }
}

export class ExporterReplayConflictError extends Error {
  constructor() {
    super('observability exporter replay identity conflict');
    this.name = 'ExporterReplayConflictError';
  }
}

interface SessionStateRow {
  workspace_id: string;
  session_id: string;
  next_seq: string;
  first_pending_seq: string;
  projection_state: unknown;
  lease_owner: string;
  lease_generation: string;
}

interface OutboxRow {
  id: string;
  organization_id: string;
  workspace_id: string;
  session_id: string;
  binding_id: string;
  binding_version: number;
  trace_id: string;
  canonical_trace: unknown;
  delivery_context: unknown;
  delivery_attempt_count: string;
  lease_owner: string;
  lease_generation: string;
}

/**
 * Creates only exporter-owned tables. This service never opens Registry's
 * schema or transaction; deployment may share a Postgres server but not a DB
 * ownership boundary.
 */
export async function applyObservabilityExporterMigrations(pool: Pool): Promise<void> {
  const client = await pool.connect();
  let inTransaction = false;
  try {
    await client.query('BEGIN');
    inTransaction = true;
    await client.query(
      "SELECT pg_advisory_xact_lock(hashtext('orca_observability_exporter_schema_v1'))",
    );
    await client.query(`
      CREATE TABLE IF NOT EXISTS observability_exporter_session_state (
        workspace_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        next_seq BIGINT NOT NULL DEFAULT 0,
        projection_state JSONB NOT NULL DEFAULT
          '{"version":1,"pendingInputs":[],"activeTurn":null}'::jsonb,
        lease_owner TEXT,
        lease_generation BIGINT NOT NULL DEFAULT 0,
        lease_until TIMESTAMPTZ,
        quarantined_at TIMESTAMPTZ,
        last_error_code TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (workspace_id, session_id),
        CHECK (next_seq >= 0),
        CHECK (lease_generation >= 0)
      )
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS observability_exporter_accepted_sources (
        workspace_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        source_event_key CHAR(64) NOT NULL,
        accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (workspace_id, session_id, source_event_key)
      )
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS observability_exporter_event_inbox (
        workspace_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        event_key CHAR(64) NOT NULL,
        source_seq BIGINT NOT NULL,
        source_hash CHAR(64) NOT NULL,
        received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        processed_at TIMESTAMPTZ,
        PRIMARY KEY (workspace_id, session_id, event_key),
        CHECK (source_seq >= 0)
      )
    `);
    await client.query(`
      ALTER TABLE observability_exporter_event_inbox
        DROP COLUMN IF EXISTS source_kind
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS observability_exporter_inbox_pending_idx
        ON observability_exporter_event_inbox (workspace_id, session_id, source_seq)
        WHERE processed_at IS NULL
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS observability_exporter_trace_outbox (
        id BIGSERIAL PRIMARY KEY,
        organization_id TEXT NOT NULL,
        workspace_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        binding_id TEXT NOT NULL,
        binding_version INTEGER NOT NULL,
        trace_id CHAR(32) NOT NULL,
        payload_hash CHAR(64) NOT NULL,
        canonical_trace JSONB NOT NULL,
        delivery_context JSONB NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        lease_owner TEXT,
        lease_generation BIGINT NOT NULL DEFAULT 0,
        lease_until TIMESTAMPTZ,
        delivered_at TIMESTAMPTZ,
        suppressed_at TIMESTAMPTZ,
        suppression_reason TEXT,
        delivery_attempt_count BIGINT NOT NULL DEFAULT 0,
        last_error_code TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (workspace_id, session_id, trace_id),
        CHECK (binding_version > 0),
        CHECK (lease_generation >= 0),
        CONSTRAINT observability_exporter_outbox_delivery_attempt_nonnegative
          CHECK (delivery_attempt_count >= 0),
        CHECK (status IN ('pending', 'delivered', 'suppressed'))
      )
    `);
    await client.query(`
      ALTER TABLE observability_exporter_trace_outbox
        DROP COLUMN IF EXISTS trace_schema_version,
        DROP COLUMN IF EXISTS attempt_count,
        ADD COLUMN IF NOT EXISTS delivery_attempt_count BIGINT NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS delivery_outcome TEXT,
        ADD COLUMN IF NOT EXISTS delivery_rejected_spans BIGINT,
        ADD COLUMN IF NOT EXISTS delivery_message_bytes INTEGER,
        ADD COLUMN IF NOT EXISTS delivery_message_sha256 TEXT
    `);
    await client.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1
          FROM pg_constraint
          WHERE conrelid = 'observability_exporter_trace_outbox'::regclass
            AND conname = 'observability_exporter_outbox_delivery_attempt_nonnegative'
        ) THEN
          ALTER TABLE observability_exporter_trace_outbox
            ADD CONSTRAINT observability_exporter_outbox_delivery_attempt_nonnegative
            CHECK (delivery_attempt_count >= 0);
        END IF;
      END;
      $$
    `);
    await client.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1
          FROM pg_constraint
          WHERE conrelid = 'observability_exporter_trace_outbox'::regclass
            AND conname = 'observability_exporter_outbox_delivery_outcome_consistent'
        ) THEN
          ALTER TABLE observability_exporter_trace_outbox
            ADD CONSTRAINT observability_exporter_outbox_delivery_outcome_consistent
            CHECK (
              (delivery_outcome IS NULL AND delivery_rejected_spans IS NULL
                AND delivery_message_bytes IS NULL AND delivery_message_sha256 IS NULL)
              OR (
                (delivery_outcome = 'accepted' AND status = 'delivered'
                  AND delivered_at IS NOT NULL AND suppressed_at IS NULL AND suppression_reason IS NULL
                  AND delivery_rejected_spans = 0
                  AND delivery_message_bytes IS NULL AND delivery_message_sha256 IS NULL)
                OR (delivery_outcome = 'accepted_with_warning' AND status = 'delivered'
                  AND delivered_at IS NOT NULL AND suppressed_at IS NULL AND suppression_reason IS NULL
                  AND delivery_rejected_spans = 0
                  AND delivery_message_bytes BETWEEN 1 AND 65536
                  AND length(delivery_message_sha256) = 64
                  AND delivery_message_sha256 ~ '^[0-9a-f]{64}$')
                OR (delivery_outcome = 'partial_rejection' AND status = 'suppressed'
                  AND suppressed_at IS NOT NULL AND delivered_at IS NULL
                  AND suppression_reason = 'partial_rejection'
                  AND delivery_rejected_spans > 0
                  AND delivery_message_bytes BETWEEN 0 AND 65536
                  AND length(delivery_message_sha256) = 64
                  AND delivery_message_sha256 ~ '^[0-9a-f]{64}$')
              ) IS TRUE
            );
        END IF;
      END;
      $$
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS observability_exporter_binding_cooldowns (
        binding_id TEXT PRIMARY KEY,
        retry_not_before TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS observability_exporter_outbox_ready_idx
        ON observability_exporter_trace_outbox (available_at, id)
        WHERE status = 'pending'
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS observability_exporter_outbox_binding_lease_idx
        ON observability_exporter_trace_outbox (binding_id, lease_until)
        WHERE status = 'pending' AND lease_until IS NOT NULL
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS observability_exporter_conflicts (
        id BIGSERIAL PRIMARY KEY,
        workspace_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        identity_type TEXT NOT NULL,
        identity_value TEXT NOT NULL,
        existing_hash CHAR(64) NOT NULL,
        incoming_hash CHAR(64) NOT NULL,
        detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (
          workspace_id,
          session_id,
          identity_type,
          identity_value,
          existing_hash,
          incoming_hash
        )
      )
    `);
    await client.query('COMMIT');
    inTransaction = false;
  } catch (error) {
    if (inTransaction) await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

/** Durable ACK/work-notification identity ledger, Session cursor, and canonical trace outbox. */
export class ObservabilityExporterRepository {
  constructor(private readonly pool: Pool) {}

  /**
   * Event-source ACK boundary: this resolves only after the content-free inbox
   * transaction has committed. Payload bytes influence an irreversible hash
   * but never enter a table. Kafka replay is authoritative and can advance the
   * projection cursor before this notification row arrives; such a late row is
   * inserted as already processed.
   */
  async acceptEvent(event: Event): Promise<InboxAcceptance> {
    const sourceSeq = eventSequence(event);
    const sourceHash = hashTranscriptEnvelope(event);
    const eventKey = eventIdentityKey(event.id);
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `
          INSERT INTO observability_exporter_session_state (workspace_id, session_id)
          VALUES ($1, $2)
          ON CONFLICT (workspace_id, session_id) DO NOTHING
        `,
        [event.workspaceId, event.sessionId],
      );
      const cursor = await client.query<{ next_seq: string }>(
        `
          SELECT next_seq::text
          FROM observability_exporter_session_state
          WHERE workspace_id = $1 AND session_id = $2
          FOR UPDATE
        `,
        [event.workspaceId, event.sessionId],
      );
      const nextSeq = cursor.rows[0]?.next_seq;
      if (nextSeq === undefined) throw new Error('observability exporter session state is missing');
      const alreadyProjected = BigInt(sourceSeq) < BigInt(nextSeq);
      const inserted = await client.query<{ event_key: string }>(
        `
          INSERT INTO observability_exporter_event_inbox (
            workspace_id, session_id, event_key, source_seq, source_hash, processed_at
          ) VALUES ($1, $2, $3, $4::bigint, $5, CASE WHEN $6::boolean THEN now() ELSE NULL END)
          ON CONFLICT (workspace_id, session_id, event_key) DO NOTHING
          RETURNING event_key
        `,
        [event.workspaceId, event.sessionId, eventKey, sourceSeq, sourceHash, alreadyProjected],
      );
      if (inserted.rowCount === 1) {
        await client.query('COMMIT');
        return 'accepted';
      }

      const existing = await client.query<{ source_hash: string }>(
        `
          SELECT source_hash
          FROM observability_exporter_event_inbox
          WHERE workspace_id = $1 AND session_id = $2 AND event_key = $3
        `,
        [event.workspaceId, event.sessionId, eventKey],
      );
      const existingHash = existing.rows[0]?.source_hash;
      if (existingHash === sourceHash) {
        await client.query('COMMIT');
        return 'duplicate';
      }
      await insertConflict(client, {
        workspaceId: event.workspaceId,
        sessionId: event.sessionId,
        identityType: 'transcript_event',
        identityValue: eventKey,
        existingHash: existingHash ?? missingHash(),
        incomingHash: sourceHash,
      });
      await client.query(
        `
          UPDATE observability_exporter_session_state
          SET quarantined_at = COALESCE(quarantined_at, now()),
              last_error_code = 'source_conflict',
              updated_at = now()
          WHERE workspace_id = $1 AND session_id = $2
        `,
        [event.workspaceId, event.sessionId],
      );
      await client.query('COMMIT');
      return 'conflict';
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async claimSession(
    leaseOwner: string,
    leaseMs: number,
  ): Promise<ClaimedProjectionSession | null> {
    assertLeaseInput(leaseOwner, leaseMs);
    const result = await this.pool.query<SessionStateRow>(
      `
        WITH candidate AS (
          SELECT s.workspace_id, s.session_id, pending.first_pending_seq
          FROM observability_exporter_session_state s
          JOIN LATERAL (
            SELECT min(i.source_seq) AS first_pending_seq
            FROM observability_exporter_event_inbox i
            WHERE i.workspace_id = s.workspace_id
              AND i.session_id = s.session_id
              AND i.processed_at IS NULL
          ) pending ON pending.first_pending_seq IS NOT NULL
          WHERE s.quarantined_at IS NULL
            AND (s.lease_until IS NULL OR s.lease_until < now())
          ORDER BY pending.first_pending_seq, s.workspace_id, s.session_id
          FOR UPDATE OF s SKIP LOCKED
          LIMIT 1
        ), claimed AS (
          UPDATE observability_exporter_session_state s
          SET lease_owner = $1,
              lease_generation = s.lease_generation + 1,
              lease_until = now() + ($2::bigint * interval '1 millisecond'),
              updated_at = now()
          FROM candidate c
          WHERE s.workspace_id = c.workspace_id AND s.session_id = c.session_id
          RETURNING s.*, c.first_pending_seq
        )
        SELECT workspace_id, session_id, next_seq::text,
               first_pending_seq::text, projection_state,
               lease_owner, lease_generation::text
        FROM claimed
      `,
      [leaseOwner, leaseMs],
    );
    const row = result.rows[0];
    if (row === undefined) return null;
    let state: CanonicalProjectionState;
    try {
      state = parseCanonicalProjectionState(row.projection_state);
    } catch (error) {
      await this.pool
        .query(
          `
            UPDATE observability_exporter_session_state
            SET lease_owner = NULL,
                lease_until = NULL,
                quarantined_at = COALESCE(quarantined_at, now()),
                last_error_code = 'invalid_projection_state',
                updated_at = now()
            WHERE workspace_id = $1 AND session_id = $2
              AND lease_owner = $3 AND lease_generation = $4::bigint
              AND lease_until > now()
              AND quarantined_at IS NULL
          `,
          [row.workspace_id, row.session_id, row.lease_owner, row.lease_generation],
        )
        .catch(() => undefined);
      throw error;
    }
    return {
      workspaceId: row.workspace_id,
      sessionId: row.session_id,
      nextSeq: row.next_seq,
      firstPendingSeq: row.first_pending_seq,
      state,
      leaseOwner: row.lease_owner,
      leaseGeneration: row.lease_generation,
    };
  }

  async completeProjection(
    claim: ClaimedProjectionSession,
    nextSeq: string,
    state: CanonicalProjectionState,
    traces: readonly PendingCanonicalTrace[],
    acceptedSourceIds: readonly string[] = [],
  ): Promise<void> {
    if (BigInt(nextSeq) <= BigInt(claim.nextSeq)) {
      throw new Error('projection cursor must advance');
    }
    // Treat caller-supplied reducer state as untrusted at this boundary too.
    // The parser reconstructs its content-free shape and drops unknown fields.
    const canonicalState = parseCanonicalProjectionState(state);
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      // This SELECT verifies an unexpired owner/generation and locks the row.
      // Once acquired, no takeover can occur until this transaction commits,
      // so later inserts need not race the wall clock while holding that lock.
      await assertProjectionLease(client, claim);
      await insertAcceptedSources(client, claim, acceptedSourceIds);
      let conflicted = false;
      for (const pending of traces) {
        assertPendingTraceMatchesClaim(pending, claim);
        const traceConflicted = await insertTrace(client, claim, pending);
        conflicted ||= traceConflicted;
      }
      const updated = await client.query(
        `
          UPDATE observability_exporter_session_state
          SET next_seq = $5::bigint,
              projection_state = $6::jsonb,
              lease_owner = NULL,
              lease_until = NULL,
              quarantined_at = CASE WHEN $7::boolean THEN COALESCE(quarantined_at, now()) ELSE quarantined_at END,
              last_error_code = CASE WHEN $7::boolean THEN 'trace_conflict' ELSE NULL END,
              updated_at = now()
          WHERE workspace_id = $1 AND session_id = $2
            AND lease_owner = $3 AND lease_generation = $4::bigint
        `,
        [
          claim.workspaceId,
          claim.sessionId,
          claim.leaseOwner,
          claim.leaseGeneration,
          nextSeq,
          stableJson(canonicalState),
          conflicted,
        ],
      );
      if (updated.rowCount !== 1) throw new ExporterLeaseLostError(`session ${claim.sessionId}`);
      await client.query(
        `
          UPDATE observability_exporter_event_inbox
          SET processed_at = COALESCE(processed_at, now())
          WHERE workspace_id = $1 AND session_id = $2
            AND processed_at IS NULL AND source_seq < $3::bigint
        `,
        [claim.workspaceId, claim.sessionId, nextSeq],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async releaseProjectionClaim(
    claim: ClaimedProjectionSession,
    errorCode = 'projection_failed',
  ): Promise<void> {
    assertErrorCode(errorCode);
    const result = await this.pool.query(
      `
        UPDATE observability_exporter_session_state
        SET lease_owner = NULL, lease_until = NULL, last_error_code = $5, updated_at = now()
        WHERE workspace_id = $1 AND session_id = $2
          AND lease_owner = $3 AND lease_generation = $4::bigint
          AND lease_until > now()
          AND quarantined_at IS NULL
      `,
      [claim.workspaceId, claim.sessionId, claim.leaseOwner, claim.leaseGeneration, errorCode],
    );
    if (result.rowCount !== 1) throw new ExporterLeaseLostError(`session ${claim.sessionId}`);
  }

  async loadAcceptedSourceIds(
    claim: ClaimedProjectionSession,
    candidateSourceIds: readonly string[],
  ): Promise<Set<string>> {
    const unique = [...new Set(candidateSourceIds)];
    if (unique.length === 0) return new Set();
    unique.forEach(assertSourceEventId);
    const sourceIdByKey = new Map(unique.map((sourceId) => [eventIdentityKey(sourceId), sourceId]));
    const result = await this.pool.query<{ source_event_key: string }>(
      `
        SELECT source_event_key
        FROM observability_exporter_accepted_sources
        WHERE workspace_id = $1 AND session_id = $2
          AND source_event_key = ANY($3::text[])
      `,
      [claim.workspaceId, claim.sessionId, [...sourceIdByKey.keys()]],
    );
    return new Set(
      result.rows.flatMap((row) => {
        const sourceId = sourceIdByKey.get(row.source_event_key);
        return sourceId === undefined ? [] : [sourceId];
      }),
    );
  }

  async loadInboxEventIdentities(
    claim: ClaimedProjectionSession,
    eventIds: readonly string[],
  ): Promise<InboxEventIdentity[]> {
    const eventKeys = [...new Set(eventIds.map(eventIdentityKey))];
    if (eventKeys.length === 0) return [];
    const result = await this.pool.query<{
      event_key: string;
      source_seq: string;
      source_hash: string;
    }>(
      `
        SELECT event_key, source_seq::text, source_hash
        FROM observability_exporter_event_inbox
        WHERE workspace_id = $1 AND session_id = $2
          AND event_key = ANY($3::text[])
      `,
      [claim.workspaceId, claim.sessionId, eventKeys],
    );
    return result.rows.map((row) => ({
      eventKey: row.event_key,
      sourceSeq: row.source_seq,
      sourceHash: row.source_hash,
    }));
  }

  async renewProjectionClaim(claim: ClaimedProjectionSession, leaseMs: number): Promise<void> {
    assertLeaseInput(claim.leaseOwner, leaseMs);
    const result = await this.pool.query(
      `
        UPDATE observability_exporter_session_state
        SET lease_until = GREATEST(
              lease_until,
              now() + ($5::bigint * interval '1 millisecond')
            ),
            updated_at = now()
        WHERE workspace_id = $1 AND session_id = $2
          AND lease_owner = $3 AND lease_generation = $4::bigint
          AND lease_until > now() AND quarantined_at IS NULL
      `,
      [claim.workspaceId, claim.sessionId, claim.leaseOwner, claim.leaseGeneration, leaseMs],
    );
    if (result.rowCount !== 1) throw new ExporterLeaseLostError(`session ${claim.sessionId}`);
  }

  async quarantineProjectionClaim(
    claim: ClaimedProjectionSession,
    errorCode: string,
  ): Promise<void> {
    assertErrorCode(errorCode);
    const result = await this.pool.query(
      `
        UPDATE observability_exporter_session_state
        SET lease_owner = NULL,
            lease_until = NULL,
            quarantined_at = COALESCE(quarantined_at, now()),
            last_error_code = $5,
            updated_at = now()
        WHERE workspace_id = $1 AND session_id = $2
          AND lease_owner = $3 AND lease_generation = $4::bigint
          AND lease_until > now()
          AND quarantined_at IS NULL
      `,
      [claim.workspaceId, claim.sessionId, claim.leaseOwner, claim.leaseGeneration, errorCode],
    );
    if (result.rowCount !== 1) throw new ExporterLeaseLostError(`session ${claim.sessionId}`);
  }

  async quarantineReplayConflict(
    claim: ClaimedProjectionSession,
    conflict: {
      eventKey: string;
      existingHash: string;
      incomingHash: string;
    },
  ): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await assertProjectionLease(client, claim);
      await insertConflict(client, {
        workspaceId: claim.workspaceId,
        sessionId: claim.sessionId,
        identityType: 'transcript_event',
        identityValue: conflict.eventKey,
        existingHash: conflict.existingHash,
        incomingHash: conflict.incomingHash,
      });
      const updated = await client.query(
        `
          UPDATE observability_exporter_session_state
          SET lease_owner = NULL,
              lease_until = NULL,
              quarantined_at = COALESCE(quarantined_at, now()),
              last_error_code = 'source_conflict',
              updated_at = now()
          WHERE workspace_id = $1 AND session_id = $2
            AND lease_owner = $3 AND lease_generation = $4::bigint
        `,
        [claim.workspaceId, claim.sessionId, claim.leaseOwner, claim.leaseGeneration],
      );
      if (updated.rowCount !== 1) {
        throw new ExporterLeaseLostError(`session ${claim.sessionId}`);
      }
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async claimOutbox(
    leaseOwner: string,
    leaseMs: number,
    signal?: AbortSignal,
  ): Promise<ClaimedOutboxItem | null> {
    assertLeaseInput(leaseOwner, leaseMs);
    let row: OutboxRow | undefined;
    for (let attempt = 0; attempt < MAX_OUTBOX_SELECTION_ATTEMPTS; attempt += 1) {
      signal?.throwIfAborted();
      const client = await this.pool.connect();
      let bindingId: string | undefined;
      try {
        signal?.throwIfAborted();
        await client.query('BEGIN');
        const binding = await client.query<{ binding_id: string }>(
          `
          SELECT o.binding_id
          FROM observability_exporter_trace_outbox o
          LEFT JOIN observability_exporter_binding_cooldowns c
            ON c.binding_id = o.binding_id
          WHERE o.status = 'pending'
            AND o.available_at <= now()
            AND (o.lease_until IS NULL OR o.lease_until < now())
            AND (c.retry_not_before IS NULL OR c.retry_not_before <= now())
            AND NOT EXISTS (
              SELECT 1
              FROM observability_exporter_trace_outbox active
              WHERE active.binding_id = o.binding_id
                AND active.status = 'pending'
                AND active.lease_until >= now()
            )
          ORDER BY o.available_at, o.id
          LIMIT 1
        `,
        );
        bindingId = binding.rows[0]?.binding_id;
        if (bindingId !== undefined) {
          await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [bindingId]);
          signal?.throwIfAborted();
          const result = await client.query<OutboxRow>(
            `
            WITH candidate AS (
              SELECT o.id
              FROM observability_exporter_trace_outbox o
              LEFT JOIN observability_exporter_binding_cooldowns c
                ON c.binding_id = o.binding_id
              WHERE o.binding_id = $3
                AND o.status = 'pending'
                AND o.available_at <= now()
                AND (o.lease_until IS NULL OR o.lease_until < now())
                AND (c.retry_not_before IS NULL OR c.retry_not_before <= now())
                AND NOT EXISTS (
                  SELECT 1
                  FROM observability_exporter_trace_outbox active
                  WHERE active.binding_id = o.binding_id
                    AND active.status = 'pending'
                    AND active.lease_until >= now()
                )
              ORDER BY o.available_at, o.id
              FOR UPDATE OF o SKIP LOCKED
              LIMIT 1
            ), claimed AS (
              UPDATE observability_exporter_trace_outbox o
              SET lease_owner = $1,
                  lease_generation = o.lease_generation + 1,
                  lease_until = now() + ($2::bigint * interval '1 millisecond')
              FROM candidate c
              WHERE o.id = c.id
              RETURNING o.*
            )
            SELECT id::text, organization_id, workspace_id, session_id, binding_id,
                   binding_version, trace_id, canonical_trace, delivery_context,
                   delivery_attempt_count::text, lease_owner, lease_generation::text
            FROM claimed
          `,
            [leaseOwner, leaseMs, bindingId],
          );
          row = result.rows[0];
        }
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw error;
      } finally {
        client.release();
      }
      if (bindingId === undefined) return null;
      if (row !== undefined) break;
      // A replica may have claimed this binding while we waited. Reselect only
      // after releasing the transaction/lock, with a fresh snapshot and now().
    }
    if (row === undefined) return null;
    let trace: ProjectedTrace;
    let deliveryContext: PinnedDeliveryContext;
    let attemptCount: number;
    try {
      trace = parseProjectedTrace(row.canonical_trace);
      deliveryContext = parsePinnedDeliveryContext(row.delivery_context);
      assertOutboxRowIdentity(row, trace, deliveryContext);
      assertTraceSampled(trace, deliveryContext);
      attemptCount = parseOutboxAttemptCount(row.delivery_attempt_count);
    } catch (error) {
      await this.pool
        .query(
          `
            UPDATE observability_exporter_trace_outbox
            SET status = 'suppressed',
                suppressed_at = now(),
                suppression_reason = 'invalid_outbox_state',
                lease_owner = NULL,
                lease_until = NULL,
                last_error_code = 'invalid_outbox_state'
            WHERE id = $1::bigint
              AND status = 'pending'
              AND lease_owner = $2 AND lease_generation = $3::bigint
              AND lease_until > now()
          `,
          [row.id, row.lease_owner, row.lease_generation],
        )
        .catch(() => undefined);
      throw error;
    }
    return {
      id: row.id,
      trace,
      deliveryContext,
      attemptCount,
      leaseOwner: row.lease_owner,
      leaseGeneration: row.lease_generation,
    };
  }

  async completeOutboxDelivery(
    item: ClaimedOutboxItem,
    outcome: OtlpDeliveryOutcome,
  ): Promise<void> {
    // Reconstruct the scalar allowlist; neither raw responses nor caller objects
    // (including pg serialization hooks) may cross the persistence boundary.
    const safeOutcome = parseDeliveryOutcome(outcome);
    await this.completeOutboxLease(
      item,
      `
      status = CASE WHEN $4 = 'partial_rejection' THEN 'suppressed' ELSE 'delivered' END,
      delivered_at = CASE WHEN $4 = 'partial_rejection' THEN NULL ELSE now() END,
      suppressed_at = CASE WHEN $4 = 'partial_rejection' THEN now() ELSE NULL END,
      suppression_reason = CASE WHEN $4 = 'partial_rejection' THEN 'partial_rejection' ELSE NULL END,
      delivery_outcome = $4,
      delivery_rejected_spans = $5::bigint,
      delivery_message_bytes = $6::integer,
      delivery_message_sha256 = $7,
      lease_owner = NULL,
      lease_until = NULL,
      last_error_code = NULL
    `,
      safeOutcome.kind,
      safeOutcome.kind === 'accepted' ? '0' : safeOutcome.rejectedSpans,
      safeOutcome.kind === 'accepted' ? null : safeOutcome.messageBytes,
      safeOutcome.kind === 'accepted' ? null : safeOutcome.messageSha256,
    );
  }

  async markOutboxDelivered(item: ClaimedOutboxItem): Promise<void> {
    await this.completeOutboxLease(
      item,
      `
      status = 'delivered',
      delivered_at = now(),
      lease_owner = NULL,
      lease_until = NULL,
      last_error_code = NULL
    `,
    );
  }

  async markOutboxSuppressed(item: ClaimedOutboxItem, reason: string): Promise<void> {
    assertErrorCode(reason);
    await this.completeOutboxLease(
      item,
      `
      status = 'suppressed',
      suppressed_at = now(),
      suppression_reason = $4,
      lease_owner = NULL,
      lease_until = NULL,
      last_error_code = NULL
    `,
      reason,
    );
  }

  async releaseOutboxClaim(item: ClaimedOutboxItem, errorCode = 'delivery_failed'): Promise<void> {
    assertErrorCode(errorCode);
    await this.completeOutboxLease(
      item,
      `
      available_at = now(),
      lease_owner = NULL,
      lease_until = NULL,
      last_error_code = $4
    `,
      errorCode,
    );
  }

  async scheduleOutboxRetry(
    item: ClaimedOutboxItem,
    delayMs: number,
    errorCode: string,
  ): Promise<void> {
    assertRetryDelay(delayMs);
    assertErrorCode(errorCode);
    const result = await this.pool.query<{ scheduled: boolean }>(
      `
        WITH scheduled AS (
          UPDATE observability_exporter_trace_outbox
          SET delivery_attempt_count = delivery_attempt_count + 1,
              available_at = now() + ($4::bigint * interval '1 millisecond'),
              lease_owner = NULL,
              lease_until = NULL,
              last_error_code = $5
          WHERE id = $1::bigint
            AND status = 'pending'
            AND lease_owner = $2 AND lease_generation = $3::bigint
            AND lease_until > now()
          RETURNING binding_id, available_at
        ), cooldown AS (
          INSERT INTO observability_exporter_binding_cooldowns (
            binding_id, retry_not_before, updated_at
          )
          SELECT binding_id, available_at, now()
          FROM scheduled
          ON CONFLICT (binding_id) DO UPDATE
          SET retry_not_before = GREATEST(
                observability_exporter_binding_cooldowns.retry_not_before,
                EXCLUDED.retry_not_before
              ),
              updated_at = now()
          RETURNING binding_id
        )
        SELECT EXISTS (SELECT 1 FROM scheduled)
           AND EXISTS (SELECT 1 FROM cooldown) AS scheduled
      `,
      [item.id, item.leaseOwner, item.leaseGeneration, delayMs, errorCode],
    );
    if (result.rows[0]?.scheduled !== true) {
      throw new ExporterLeaseLostError(`outbox ${item.id}`);
    }
  }

  async renewOutboxClaim(item: ClaimedOutboxItem, leaseMs: number): Promise<void> {
    assertLeaseInput(item.leaseOwner, leaseMs);
    const result = await this.pool.query(
      `
        UPDATE observability_exporter_trace_outbox
        SET lease_until = GREATEST(
              lease_until,
              now() + ($4::bigint * interval '1 millisecond')
            )
        WHERE id = $1::bigint
          AND status = 'pending'
          AND lease_owner = $2 AND lease_generation = $3::bigint
          AND lease_until > now()
      `,
      [item.id, item.leaseOwner, item.leaseGeneration, leaseMs],
    );
    if (result.rowCount !== 1) throw new ExporterLeaseLostError(`outbox ${item.id}`);
  }

  private async completeOutboxLease(
    item: ClaimedOutboxItem,
    setClause: string,
    ...values: readonly (string | number | null)[]
  ): Promise<void> {
    const result = await this.pool.query(
      `
        UPDATE observability_exporter_trace_outbox
        SET ${setClause}
        WHERE id = $1::bigint
          AND status = 'pending'
          AND lease_owner = $2 AND lease_generation = $3::bigint
          AND lease_until > now()
      `,
      [item.id, item.leaseOwner, item.leaseGeneration, ...values],
    );
    if (result.rowCount !== 1) throw new ExporterLeaseLostError(`outbox ${item.id}`);
  }
}

async function assertProjectionLease(
  client: PoolClient,
  claim: ClaimedProjectionSession,
): Promise<void> {
  const result = await client.query(
    `
      SELECT 1
      FROM observability_exporter_session_state
      WHERE workspace_id = $1 AND session_id = $2
        AND lease_owner = $3 AND lease_generation = $4::bigint
        AND lease_until > now()
        AND quarantined_at IS NULL
      FOR UPDATE
    `,
    [claim.workspaceId, claim.sessionId, claim.leaseOwner, claim.leaseGeneration],
  );
  if (result.rowCount !== 1) throw new ExporterLeaseLostError(`session ${claim.sessionId}`);
}

async function insertAcceptedSources(
  client: PoolClient,
  claim: ClaimedProjectionSession,
  sourceEventIds: readonly string[],
): Promise<void> {
  const unique = [...new Set(sourceEventIds)];
  if (unique.length === 0) return;
  unique.forEach(assertSourceEventId);
  const sourceEventKeys = unique.map(eventIdentityKey);
  await client.query(
    `
      INSERT INTO observability_exporter_accepted_sources (
        workspace_id, session_id, source_event_key
      )
      SELECT $1, $2, source_event_key
      FROM unnest($3::text[]) AS accepted(source_event_key)
      ON CONFLICT DO NOTHING
    `,
    [claim.workspaceId, claim.sessionId, sourceEventKeys],
  );
}

async function insertTrace(
  client: PoolClient,
  claim: ClaimedProjectionSession,
  pending: PendingCanonicalTrace,
): Promise<boolean> {
  // Rebuild both values from their allowlists before the first database write.
  // Static types are not a persistence boundary: this keeps accidental caller
  // fields, including Transcript-shaped content or a secret reference, out.
  const trace = parseProjectedTrace(pending.trace);
  const context = parsePinnedDeliveryContext(pending.deliveryContext);
  assertTraceSampled(trace, context);
  const canonicalTrace = stableJson(trace);
  const payloadHash = sha256Hex(canonicalTrace);
  const deliveryContext = stableJson(context);
  const inserted = await client.query<{ id: string }>(
    `
      INSERT INTO observability_exporter_trace_outbox (
        organization_id, workspace_id, session_id, binding_id, binding_version,
        trace_id, payload_hash, canonical_trace, delivery_context
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb)
      ON CONFLICT (workspace_id, session_id, trace_id) DO NOTHING
      RETURNING id::text
    `,
    [
      context.organizationId,
      claim.workspaceId,
      claim.sessionId,
      context.bindingId,
      context.bindingVersion,
      trace.traceId,
      payloadHash,
      canonicalTrace,
      deliveryContext,
    ],
  );
  if (inserted.rowCount === 1) return false;

  const existing = await client.query<{ payload_hash: string }>(
    `
      SELECT payload_hash
      FROM observability_exporter_trace_outbox
      WHERE workspace_id = $1 AND session_id = $2 AND trace_id = $3
    `,
    [claim.workspaceId, claim.sessionId, trace.traceId],
  );
  const existingHash = existing.rows[0]?.payload_hash;
  if (existingHash === payloadHash) return false;
  await insertConflict(client, {
    workspaceId: claim.workspaceId,
    sessionId: claim.sessionId,
    identityType: 'trace_payload',
    identityValue: trace.traceId,
    existingHash: existingHash ?? missingHash(),
    incomingHash: payloadHash,
  });
  return true;
}

function assertTraceSampled(trace: ProjectedTrace, context: PinnedDeliveryContext): void {
  if (
    !isTraceSampled(
      {
        algorithmVersion: TRACE_SAMPLING_VERSION,
        bindingId: context.bindingId,
        bindingVersion: context.bindingVersion,
        sampleRate: context.sampleRate,
      },
      trace.traceId,
    )
  ) {
    throw new Error('sampled-out trace cannot enter the canonical outbox');
  }
}

function assertPendingTraceMatchesClaim(
  pending: PendingCanonicalTrace,
  claim: ClaimedProjectionSession,
): void {
  if (
    pending.trace.workspaceId !== claim.workspaceId ||
    pending.trace.sessionId !== claim.sessionId ||
    pending.deliveryContext.organizationId.length === 0 ||
    pending.deliveryContext.bindingId.length === 0 ||
    !Number.isSafeInteger(pending.deliveryContext.bindingVersion) ||
    pending.deliveryContext.bindingVersion <= 0
  ) {
    throw new Error('canonical trace does not match claimed Session');
  }
}

function assertOutboxRowIdentity(
  row: OutboxRow,
  trace: ProjectedTrace,
  context: PinnedDeliveryContext,
): void {
  if (
    trace.workspaceId !== row.workspace_id ||
    trace.sessionId !== row.session_id ||
    trace.traceId !== row.trace_id ||
    context.organizationId !== row.organization_id ||
    context.bindingId !== row.binding_id ||
    context.bindingVersion !== row.binding_version
  ) {
    throw new Error('observability exporter outbox identity is invalid');
  }
}

async function insertConflict(
  client: PoolClient,
  input: {
    workspaceId: string;
    sessionId: string;
    identityType: string;
    identityValue: string;
    existingHash: string;
    incomingHash: string;
  },
): Promise<void> {
  await client.query(
    `
      INSERT INTO observability_exporter_conflicts (
        workspace_id, session_id, identity_type, identity_value, existing_hash, incoming_hash
      ) VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT DO NOTHING
    `,
    [
      input.workspaceId,
      input.sessionId,
      input.identityType,
      input.identityValue,
      input.existingHash,
      input.incomingHash,
    ],
  );
}

function eventSequence(event: Event): string {
  if (!Number.isSafeInteger(event.seq) || event.seq < 0) {
    throw new Error('observability exporter received an invalid transcript sequence');
  }
  return BigInt(event.seq).toString();
}

function hashTranscriptEnvelope(event: Event): string {
  return sha256Hex(
    stableJson({
      hashSchema: 'orca.observability.transcript-envelope-hash.v1',
      id: event.id,
      workspaceId: event.workspaceId,
      sessionId: event.sessionId,
      subpath: event.subpath,
      producedAt: event.producedAt,
      producedBy: event.producedBy,
      kind: event.kind,
      idempotencyKey: event.idempotencyKey,
      ...(event.userId === undefined ? {} : { userId: event.userId }),
      payloadSha256: sha256Hex(Buffer.from(event.payload)),
    }),
  );
}

function parseDeliveryOutcome(value: unknown): OtlpDeliveryOutcome {
  if (!isRecord(value)) throw new Error('observability exporter delivery outcome is invalid');
  const { kind, rejectedSpans, messageBytes, messageSha256 } = value;
  if (kind === 'accepted') {
    if (rejectedSpans !== undefined || messageBytes !== undefined || messageSha256 !== undefined) {
      throw new Error('observability exporter delivery outcome is invalid');
    }
    return { kind };
  }
  if (
    (kind !== 'accepted_with_warning' && kind !== 'partial_rejection') ||
    typeof messageBytes !== 'number' ||
    !Number.isInteger(messageBytes) ||
    messageBytes < (kind === 'accepted_with_warning' ? 1 : 0) ||
    messageBytes > 65_536 ||
    typeof messageSha256 !== 'string' ||
    messageSha256.length !== 64 ||
    !/^[0-9a-f]{64}$/u.test(messageSha256)
  ) {
    throw new Error('observability exporter delivery outcome is invalid');
  }
  if (kind === 'accepted_with_warning') {
    if (rejectedSpans !== '0')
      throw new Error('observability exporter delivery outcome is invalid');
    return { kind, rejectedSpans, messageBytes, messageSha256 };
  }
  // Keep the canonical int64 decimal string exact, never routing it through Number.
  if (
    typeof rejectedSpans !== 'string' ||
    rejectedSpans.length === 0 ||
    rejectedSpans.length > 19 ||
    rejectedSpans[0] === '0' ||
    /[^0-9]/u.test(rejectedSpans) ||
    BigInt(rejectedSpans) > 9_223_372_036_854_775_807n
  ) {
    throw new Error('observability exporter delivery outcome is invalid');
  }
  return { kind, rejectedSpans, messageBytes, messageSha256 };
}

function assertLeaseInput(owner: string, leaseMs: number): void {
  if (owner.length === 0 || owner.length > MAX_LEASE_OWNER_LENGTH || /[\r\n]/u.test(owner)) {
    throw new Error('observability exporter lease owner is invalid');
  }
  if (!Number.isSafeInteger(leaseMs) || leaseMs <= 0 || leaseMs > 300_000) {
    throw new Error('observability exporter lease duration is invalid');
  }
}

function assertErrorCode(value: string): void {
  if (!/^[a-z0-9_]{1,128}$/u.test(value)) {
    throw new Error('observability exporter error code is invalid');
  }
}

function parseOutboxAttemptCount(value: string): number {
  let attemptCount: bigint;
  try {
    attemptCount = BigInt(value);
  } catch {
    throw new Error('observability exporter outbox retry state is invalid');
  }
  if (attemptCount < 0n) {
    throw new Error('observability exporter outbox retry state is invalid');
  }
  const maximum = BigInt(Number.MAX_SAFE_INTEGER);
  return attemptCount > maximum ? Number.MAX_SAFE_INTEGER : Number(attemptCount);
}

function assertRetryDelay(delayMs: number): void {
  if (!Number.isSafeInteger(delayMs) || delayMs <= 0 || delayMs > MAX_DELIVERY_RETRY_DELAY_MS) {
    throw new Error('observability exporter retry delay is invalid');
  }
}

function assertSourceEventId(value: string): void {
  if (!isBoundedAgentEventId(value)) {
    throw new Error('observability exporter accepted source event id is invalid');
  }
}

function stableJson(value: unknown): string {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') {
    return JSON.stringify(value);
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new Error('cannot persist a non-finite number');
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`)
      .join(',')}}`;
  }
  throw new Error('cannot persist a non-JSON value');
}

function sha256Hex(value: string | Uint8Array): string {
  return createHash('sha256').update(value).digest('hex');
}

function missingHash(): string {
  return sha256Hex('missing');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export { hashTranscriptEnvelope, stableJson };
