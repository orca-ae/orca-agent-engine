// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Pool, PoolClient } from 'pg';
import { setTimeout as abortableDelay } from 'node:timers/promises';
import { v7 as uuidv7 } from 'uuid';
import {
  appendLatency,
  appendTotal,
  archiveTotal,
  dedupHits,
  readFirstByteLatency,
  readTotal,
  subagentMessageRate,
  tailTotal,
} from './metrics.js';
import type {
  ReadOptions,
  SessionEventSourceStatus,
  TailOptions,
  TranscriptStore,
} from './store.js';
import { SessionEventBarrierError, type Event } from './types.js';
import { assertEventsMatchRoute } from './route.js';
import { terminalSourceFailureFields } from './source-failure.js';

const DEFAULT_TAIL_POLL_INTERVAL_MS = 500;

export interface PostgresTranscriptStoreOptions {
  pool: Pool;
  closePool?: boolean;
  tailPollIntervalMs?: number;
}

interface EventRow {
  seq: string;
  workspace_id: string;
  session_id: string;
  event_id: string;
  subpath: string;
  produced_at: string;
  produced_by: string;
  kind: string;
  payload: Buffer | null;
  idempotency_key: string;
  user_id: string | null;
}

/**
 * Create the tables used by the Postgres transcript backend.
 *
 * Services call this during boot before constructing `PostgresTranscriptStore`.
 * The migration is intentionally raw SQL so the package stays independent from
 * the registry/file/memory Drizzle schemas.
 */
export async function applyPostgresTranscriptMigrations(pool: Pool): Promise<void> {
  const client = await pool.connect();
  let inTransaction = false;

  try {
    await client.query('BEGIN');
    inTransaction = true;
    await client.query("SELECT pg_advisory_xact_lock(hashtext('orca_transcript_store_schema'))");
    await client.query(`
      CREATE TABLE IF NOT EXISTS transcript_events (
        seq BIGSERIAL PRIMARY KEY,
        workspace_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        event_id TEXT NOT NULL,
        subpath TEXT NOT NULL DEFAULT '',
        produced_at TEXT NOT NULL,
        produced_by TEXT NOT NULL,
        kind TEXT NOT NULL,
        payload BYTEA NOT NULL DEFAULT ''::bytea,
        idempotency_key TEXT NOT NULL DEFAULT '',
        user_id TEXT,
        inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (workspace_id, session_id, event_id)
      )
    `);
    await client.query('ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS user_id TEXT');
    await client.query(`
      CREATE INDEX IF NOT EXISTS transcript_events_session_seq_idx
        ON transcript_events (workspace_id, session_id, seq)
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS transcript_events_client_user_seq_idx
        ON transcript_events (seq)
        WHERE produced_by = 'client' AND kind LIKE 'user.%'
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS transcript_events_dispatch_lifecycle_seq_idx
        ON transcript_events (seq)
        WHERE (produced_by = 'client' AND kind LIKE 'user.%')
           OR kind IN ('session.archived', 'session.deleted')
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS transcript_event_claims (
        group_id TEXT NOT NULL,
        event_seq BIGINT NOT NULL REFERENCES transcript_events(seq) ON DELETE CASCADE,
        claimed_by TEXT NOT NULL,
        lease_until TIMESTAMPTZ NOT NULL,
        processed_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (group_id, event_seq)
      )
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS transcript_event_claims_ready_idx
        ON transcript_event_claims (group_id, processed_at, lease_until)
    `);
    await client.query('COMMIT');
    inTransaction = false;
  } catch (err) {
    if (inTransaction) {
      await client.query('ROLLBACK').catch(() => undefined);
    }
    throw err;
  } finally {
    client.release();
  }
}

export class PostgresTranscriptStore implements TranscriptStore {
  private readonly pollIntervalMs: number;

  constructor(private readonly opts: PostgresTranscriptStoreOptions) {
    this.pollIntervalMs = opts.tailPollIntervalMs ?? DEFAULT_TAIL_POLL_INTERVAL_MS;
  }

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    assertEventsMatchRoute(workspaceId, sessionId, events);
    if (events.length === 0) return [];
    const stop = appendLatency.startTimer();
    try {
      const result = await this.opts.pool.query<{ event_id: string; seq: string }>(
        `
          INSERT INTO transcript_events (
            workspace_id,
            session_id,
            event_id,
            subpath,
            produced_at,
            produced_by,
            kind,
            payload,
            idempotency_key,
            user_id
          )
          SELECT *
          FROM unnest(
            $1::text[],
            $2::text[],
            $3::text[],
            $4::text[],
            $5::text[],
            $6::text[],
            $7::text[],
            $8::bytea[],
            $9::text[],
            $10::text[]
          )
          ON CONFLICT (workspace_id, session_id, event_id) DO NOTHING
          RETURNING event_id, seq::text
        `,
        [
          events.map(() => workspaceId),
          events.map(() => sessionId),
          events.map((event) => event.id),
          events.map((event) => event.subpath ?? ''),
          events.map((event) => event.producedAt),
          events.map((event) => event.producedBy),
          events.map((event) => event.kind),
          events.map((event) => Buffer.from(event.payload)),
          events.map((event) => event.idempotencyKey ?? ''),
          events.map((event) => optionalUserId(event.userId) ?? null),
        ],
      );

      const seqById = new Map(result.rows.map((row) => [row.event_id, Number(row.seq)]));
      if (seqById.size < events.length) {
        const existing = await this.opts.pool.query<{ event_id: string; seq: string }>(
          `
            SELECT event_id, seq::text
            FROM transcript_events
            WHERE workspace_id = $1 AND session_id = $2 AND event_id = ANY($3::text[])
          `,
          [workspaceId, sessionId, events.map((event) => event.id)],
        );
        for (const row of existing.rows) seqById.set(row.event_id, Number(row.seq));
      }

      const inserted = new Set(result.rows.map((row) => row.event_id));
      const deduped = events.length - inserted.size;
      for (let i = 0; i < deduped; i++) dedupHits.inc();
      for (const e of events) {
        if (inserted.has(e.id) && e.subpath && e.subpath.length > 0) {
          subagentMessageRate.inc({ workspace_id: workspaceId, produced_by: e.producedBy });
        }
        const seq = seqById.get(e.id);
        if (seq !== undefined) e.seq = seq;
      }

      appendTotal.inc({ status: 'ok' });
      return events.map((event) => event.id);
    } catch (err) {
      appendTotal.inc({ status: 'error' });
      throw err;
    } finally {
      stop();
    }
  }

  async *read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event> {
    let firstSeen = false;
    const startedAt = Date.now();
    try {
      const highWatermark = await this.highWatermark(workspaceId, sessionId);
      if (highWatermark === null) {
        readTotal.inc({ status: 'ok' });
        return;
      }
      const fromSeq = parseCursor(opts.fromCursor) ?? 0n;
      if (fromSeq > highWatermark) {
        readTotal.inc({ status: 'ok' });
        return;
      }

      let remaining = opts.maxEvents > 0 ? opts.maxEvents : Number.POSITIVE_INFINITY;
      let nextSeq = fromSeq;
      while (remaining > 0) {
        const limit = Math.min(remaining, 500);
        const rows = await this.fetchRows({
          workspaceId,
          sessionId,
          fromSeq: nextSeq,
          toSeq: highWatermark,
          subpath: opts.subpath,
          limit,
        });
        if (rows.length === 0) break;
        for (const row of rows) {
          if (!firstSeen) {
            firstSeen = true;
            readFirstByteLatency.observe((Date.now() - startedAt) / 1000);
          }
          const event = rowToEvent(row);
          nextSeq = BigInt(event.seq) + 1n;
          remaining -= 1;
          yield event;
        }
      }
      readTotal.inc({ status: 'ok' });
    } catch (err) {
      readTotal.inc({ status: 'error' });
      throw err;
    }
  }

  async *tail(workspaceId: string, sessionId: string, opts: TailOptions): AsyncIterable<Event> {
    try {
      const parsedCursor = parseCursor(opts.fromCursor);
      let nextSeq =
        parsedCursor ?? ((await this.highWatermark(workspaceId, sessionId)) ?? -1n) + 1n;
      opts.onReady?.();

      while (!opts.signal?.aborted) {
        const rows = await this.fetchRows({
          workspaceId,
          sessionId,
          fromSeq: nextSeq,
          subpath: opts.subpath,
          limit: 100,
        });
        if (rows.length === 0) {
          await delay(this.pollIntervalMs, opts.signal);
          continue;
        }
        for (const row of rows) {
          const event = rowToEvent(row);
          nextSeq = BigInt(event.seq) + 1n;
          yield event;
        }
      }
      tailTotal.inc({ status: 'ok' });
    } catch (err) {
      if (!opts.signal?.aborted) {
        tailTotal.inc({ status: 'error' });
        throw err;
      }
    }
  }

  async archive(workspaceId: string, sessionId: string): Promise<void> {
    try {
      const sentinel: Event = {
        id: uuidv7(),
        workspaceId,
        sessionId,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'transcript-store',
        kind: 'session.archived',
        payload: new Uint8Array(),
        idempotencyKey: '',
      };
      await this.append(workspaceId, sessionId, [sentinel]);
      archiveTotal.inc({ status: 'ok' });
    } catch (err) {
      archiveTotal.inc({ status: 'error' });
      throw err;
    }
  }

  async close(): Promise<void> {
    if (this.opts.closePool === false) return;
    await this.opts.pool.end();
  }

  private async highWatermark(workspaceId: string, sessionId: string): Promise<bigint | null> {
    const result = await this.opts.pool.query<{ max_seq: string | null }>(
      `
        SELECT max(seq)::text AS max_seq
        FROM transcript_events
        WHERE workspace_id = $1 AND session_id = $2
      `,
      [workspaceId, sessionId],
    );
    const value = result.rows[0]?.max_seq;
    return value === null || value === undefined ? null : BigInt(value);
  }

  private async fetchRows(input: {
    workspaceId: string;
    sessionId: string;
    fromSeq: bigint;
    toSeq?: bigint;
    subpath: string;
    limit: number;
  }): Promise<EventRow[]> {
    const params: Array<string | number> = [
      input.workspaceId,
      input.sessionId,
      input.fromSeq.toString(),
      input.limit,
    ];
    const predicates = ['workspace_id = $1', 'session_id = $2', 'seq >= $3::bigint'];
    const subpath = subpathPredicate(input.subpath, params);
    if (subpath !== null) predicates.push(subpath);
    if (input.toSeq !== undefined) {
      params.push(input.toSeq.toString());
      predicates.push(`seq <= $${params.length}::bigint`);
    }
    const result = await this.opts.pool.query<EventRow>(
      `
        SELECT
          seq::text,
          workspace_id,
          session_id,
          event_id,
          subpath,
          produced_at,
          produced_by,
          kind,
          payload,
          idempotency_key,
          user_id
        FROM transcript_events
        WHERE ${predicates.join(' AND ')}
        ORDER BY transcript_events.seq ASC
        LIMIT $4
      `,
      params,
    );
    return result.rows;
  }
}

export interface PostgresSessionEventSourceOptions {
  pool: Pool;
  groupId: string;
  pollIntervalMs?: number;
  leaseMs?: number;
  batchSize?: number;
  workerId?: string;
  includeAllEvents?: boolean;
  signal?: AbortSignal;
}

export type PostgresSessionEventHandler = (event: Event) => Promise<void>;

/**
 * At-least-once Postgres consumer for transcript events.
 *
 * `transcript_event_claims` stores one row per `(consumer group, event)`.
 * A failed handler leaves the claim unprocessed; another replica can pick it
 * up after the lease expires. By default this preserves the harness
 * dispatcher behavior and only claims client `user.*` events plus
 * `session.archived` / `session.deleted` lifecycle sentinels; set
 * `includeAllEvents` for read-model projectors.
 */
export class PostgresSessionEventSource {
  private readonly pollIntervalMs: number;
  private readonly leaseMs: number;
  private readonly batchSize: number;
  private readonly workerId: string;
  private readonly ac = new AbortController();
  private running = false;
  private stopped = true;
  private terminalRunLoopError = false;
  private runPromise: Promise<void> | null = null;
  private readonly failed: Promise<void>;
  private resolveFailed!: () => void;

  constructor(private readonly opts: PostgresSessionEventSourceOptions) {
    this.pollIntervalMs = opts.pollIntervalMs ?? 500;
    this.leaseMs = opts.leaseMs ?? 30_000;
    this.batchSize = opts.batchSize ?? 100;
    this.workerId =
      opts.workerId ?? `pg-event-source-${process.pid}-${Math.random().toString(36).slice(2)}`;
    this.failed = new Promise<void>((resolve) => {
      this.resolveFailed = resolve;
    });
    if (opts.signal) {
      const stop = (): void => {
        this.stopped = true;
        this.ac.abort();
      };
      if (opts.signal.aborted) stop();
      else opts.signal.addEventListener('abort', stop, { once: true });
    }
  }

  async start(handler: PostgresSessionEventHandler): Promise<void> {
    if (this.runPromise) return;
    if (this.ac.signal.aborted) return;
    this.stopped = false;
    this.terminalRunLoopError = false;
    this.running = true;
    this.runPromise = this.run(handler).then(
      () => {
        this.running = false;
      },
      (error: unknown) => {
        this.running = false;
        if (this.stopped) return;
        this.terminalRunLoopError = true;
        console.error(
          'PostgresSessionEventSource: run loop crashed',
          terminalSourceFailureFields(error),
        );
        this.resolveFailed();
      },
    );
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.ac.abort();
    await this.runPromise;
  }

  status(): SessionEventSourceStatus {
    if (this.stopped) return { ready: false, state: 'stopped' };
    if (this.terminalRunLoopError) return { ready: false, state: 'failed' };
    return { ready: this.running, state: this.running ? 'running' : 'stopped' };
  }

  /** Resolves only when an unintentional run-loop failure escapes. */
  whenFailed(): Promise<void> {
    return this.failed;
  }

  private async run(handler: PostgresSessionEventHandler): Promise<void> {
    while (!this.ac.signal.aborted) {
      let claimed: EventRow[];
      try {
        claimed = await this.claimBatch();
      } catch (err) {
        console.error('postgres session event claim failed', err);
        await delay(this.pollIntervalMs, this.ac.signal);
        continue;
      }
      if (claimed.length === 0) {
        await delay(this.pollIntervalMs, this.ac.signal);
        continue;
      }
      for (const row of claimed) {
        if (this.ac.signal.aborted) return;
        try {
          await this.handleOwnedClaim(row, handler);
        } catch (err) {
          console.error('postgres session event handler failed', err);
        }
      }
    }
  }

  private async handleOwnedClaim(
    row: EventRow,
    handler: PostgresSessionEventHandler,
  ): Promise<void> {
    const ac = new AbortController();
    const stop = (): void => ac.abort();
    this.ac.signal.addEventListener('abort', stop, { once: true });
    let renewal: Promise<boolean> | undefined;
    let leaseError: unknown;
    const renew = (): Promise<boolean> => {
      if (ac.signal.aborted || this.ac.signal.aborted) return Promise.resolve(false);
      if (renewal) return renewal;
      renewal = this.renewClaim(row.seq)
        .then((owned) => {
          if (!owned) ac.abort();
          return owned;
        })
        .catch((error: unknown) => {
          leaseError = error;
          ac.abort();
          return false;
        })
        .finally(() => {
          renewal = undefined;
        });
      return renewal;
    };
    // A prior row's repair may outlive leases for the rest of its batch.
    // Revalidate ownership before dispatch, and never revive an expired claim.
    const intervalMs = Math.max(1, Math.min(5_000, Math.floor(this.leaseMs / 3)));
    const timer = setInterval(() => {
      void renew();
    }, intervalMs);
    timer.unref();
    try {
      let attempt = async (): Promise<void> => {
        await handler(rowToEvent(row));
      };
      while (!ac.signal.aborted && !this.ac.signal.aborted) {
        if (!(await renew()) || ac.signal.aborted || this.ac.signal.aborted) break;
        try {
          await attempt();
        } catch (error) {
          if (!(error instanceof SessionEventBarrierError)) throw error;
          // Keep the session head unprocessed and leased while repairing the
          // original outcome. Other workers cannot claim a later session row.
          attempt = error.retry;
          console.error('postgres session event outcome repair failed', error);
          await delay(this.pollIntervalMs, ac.signal);
          continue;
        }
        if ((await renew()) && !ac.signal.aborted && !this.ac.signal.aborted) {
          await this.markProcessed(row.seq);
        }
        break;
      }
    } finally {
      clearInterval(timer);
      ac.abort();
      this.ac.signal.removeEventListener('abort', stop);
      await renewal;
    }
    if (leaseError) throw leaseError;
  }

  private async renewClaim(seq: string): Promise<boolean> {
    const result = await this.opts.pool.query(
      `
        UPDATE transcript_event_claims
        SET lease_until = clock_timestamp() + ($4::text || ' milliseconds')::interval,
            updated_at = clock_timestamp()
        WHERE group_id = $1 AND event_seq = $2::bigint AND claimed_by = $3
          AND processed_at IS NULL AND lease_until > clock_timestamp()
      `,
      [this.opts.groupId, seq, this.workerId, this.leaseMs],
    );
    return result.rowCount === 1;
  }

  private async claimBatch(): Promise<EventRow[]> {
    const client = await this.opts.pool.connect();
    try {
      await client.query('BEGIN');
      const claimed = await claimClientEvents(client, {
        groupId: this.opts.groupId,
        workerId: this.workerId,
        leaseMs: this.leaseMs,
        batchSize: this.batchSize,
        includeAllEvents: this.opts.includeAllEvents ?? false,
      });
      await client.query('COMMIT');
      return claimed;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  private async markProcessed(seq: string): Promise<void> {
    await this.opts.pool.query(
      `
        UPDATE transcript_event_claims
        SET processed_at = now(), updated_at = now()
        WHERE group_id = $1 AND event_seq = $2::bigint AND claimed_by = $3
          AND processed_at IS NULL AND lease_until > clock_timestamp()
      `,
      [this.opts.groupId, seq, this.workerId],
    );
  }
}

async function claimClientEvents(
  client: PoolClient,
  input: {
    groupId: string;
    workerId: string;
    leaseMs: number;
    batchSize: number;
    includeAllEvents: boolean;
  },
): Promise<EventRow[]> {
  const result = await client.query<EventRow>(
    `
      WITH candidates AS (
        SELECT e.seq
        FROM transcript_events e
        LEFT JOIN transcript_event_claims c
          ON c.group_id = $1 AND c.event_seq = e.seq
        WHERE (
          $5::boolean
          OR (e.produced_by = 'client' AND e.kind LIKE 'user.%')
          OR e.kind IN ('session.archived', 'session.deleted')
        )
          AND (
            c.event_seq IS NULL
            OR (c.processed_at IS NULL AND c.lease_until < now())
          )
          AND NOT EXISTS (
            SELECT 1
            FROM transcript_events earlier
            LEFT JOIN transcript_event_claims earlier_claim
              ON earlier_claim.group_id = $1 AND earlier_claim.event_seq = earlier.seq
            WHERE earlier.workspace_id = e.workspace_id
              AND earlier.session_id = e.session_id
              AND earlier.seq < e.seq
              AND (
                $5::boolean
                OR (earlier.produced_by = 'client' AND earlier.kind LIKE 'user.%')
                OR earlier.kind IN ('session.archived', 'session.deleted')
              )
              AND earlier_claim.processed_at IS NULL
          )
        ORDER BY e.seq ASC
        LIMIT $4
      ),
      claimed AS (
        INSERT INTO transcript_event_claims (
          group_id,
          event_seq,
          claimed_by,
          lease_until,
          updated_at
        )
        SELECT
          $1,
          seq,
          $2,
          now() + ($3::text || ' milliseconds')::interval,
          now()
        FROM candidates
        ON CONFLICT (group_id, event_seq) DO UPDATE
          SET claimed_by = EXCLUDED.claimed_by,
              lease_until = EXCLUDED.lease_until,
              updated_at = now()
          WHERE transcript_event_claims.processed_at IS NULL
            AND transcript_event_claims.lease_until < now()
        RETURNING event_seq
      )
      SELECT
        e.seq::text,
        e.workspace_id,
        e.session_id,
        e.event_id,
        e.subpath,
        e.produced_at,
        e.produced_by,
        e.kind,
        e.payload,
        e.idempotency_key,
        e.user_id
      FROM transcript_events e
      JOIN claimed c ON c.event_seq = e.seq
      ORDER BY e.seq ASC
    `,
    [input.groupId, input.workerId, input.leaseMs, input.batchSize, input.includeAllEvents],
  );
  return result.rows;
}

function rowToEvent(row: EventRow): Event {
  const userId = optionalUserId(row.user_id);
  return {
    id: row.event_id,
    workspaceId: row.workspace_id,
    sessionId: row.session_id,
    subpath: row.subpath,
    seq: Number(row.seq),
    producedAt: row.produced_at,
    producedBy: row.produced_by,
    kind: row.kind,
    payload: row.payload ? new Uint8Array(row.payload) : new Uint8Array(),
    idempotencyKey: row.idempotency_key,
    ...(userId !== undefined ? { userId } : {}),
  };
}

function optionalUserId(value: string | null | undefined): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function parseCursor(cursor: string): bigint | null {
  if (cursor === '') return null;
  try {
    return BigInt(cursor);
  } catch {
    return null;
  }
}

function subpathPredicate(subpath: string, params: Array<string | number>): string | null {
  if (subpath === '*') return null;
  params.push(subpath);
  return `subpath = $${params.length}`;
}

async function delay(ms: number, signal?: AbortSignal): Promise<void> {
  try {
    await abortableDelay(ms, undefined, { signal });
  } catch (error) {
    if (!signal?.aborted) throw error;
  }
}
