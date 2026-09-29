// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { Pool } from 'pg';
import type { Event } from '../../src/types.js';
import { SessionEventBarrierError } from '../../src/types.js';
import {
  PostgresSessionEventSource,
  applyPostgresTranscriptMigrations,
  PostgresTranscriptStore,
} from '../../src/postgres-store.js';
import { deletePostgresRowsForWorkspace, makePostgresStore, uniqueIds } from './setup.js';

describe('PostgresTranscriptStore (library, integration)', () => {
  let pool: Pool;
  let store: PostgresTranscriptStore;

  beforeAll(async () => {
    const setup = await makePostgresStore();
    pool = setup.pool;
    store = setup.store;
  });

  afterAll(async () => {
    await store?.close();
  });

  it.each([1, 100])(
    'holds the session head across workers during repair with batch size %s',
    async (batchSize) => {
      const { ws, ses } = uniqueIds('pg_barrier');
      const otherWs = `${ws}_other`;
      const groupId = `barrier-${ws}`;
      // A 150ms lease can legitimately expire during a loaded CI scheduling pause.
      // Keep this shorter than production (30s), but long enough for real DB I/O.
      const leaseMs = 2_000;
      const options = { pool, groupId, pollIntervalMs: 10, leaseMs, batchSize };
      const sourceA = new PostgresSessionEventSource({ ...options, workerId: 'barrier-a' });
      const sourceB = new PostgresSessionEventSource({ ...options, workerId: 'barrier-b' });
      const first = makeEvents(ws, ses, [1], 'client', 'user.message')[0]!;
      const interrupt = makeEvents(ws, ses, [2], 'client', 'user.interrupt')[0]!;
      const other = makeEvents(otherWs, ses, [3], 'client', 'user.message')[0]!;
      const processed = {
        ...first,
        id: `evt_processed_${ws}`,
        producedBy: 'harness',
        kind: 'session.user_event_processed',
      };
      const failure = {
        ...first,
        id: `evt_failure_${ws}`,
        producedBy: 'harness',
        kind: 'session.error',
      };
      const idle = {
        ...first,
        id: `evt_idle_${ws}`,
        producedBy: 'harness',
        kind: 'session.status_idle',
      };
      const seen: string[] = [];
      let available = false;
      let attempts = 0;
      const repair = async (): Promise<void> => {
        attempts += 1;
        if (!available) throw new SessionEventBarrierError('partial outcome unavailable', repair);
        await store.append(ws, ses, [failure]);
      };
      const handler = async (event: Event): Promise<void> => {
        seen.push(event.id);
        if (event.id === first.id) {
          await store.append(ws, ses, [processed]);
          await repair();
        } else if (event.id === interrupt.id) {
          await store.append(ws, ses, [idle]);
        }
      };
      try {
        await store.append(ws, ses, [first, interrupt]);
        // A large batch also claims this other head. A must skip its stale
        // entry if B processes it after its lease expires during A's repair.
        await store.append(otherWs, ses, [other]);
        await sourceA.start(handler);
        await vi.waitFor(() => expect(attempts).toBeGreaterThan(0));
        // The same session id in another workspace remains independently claimable.
        await sourceB.start(handler);
        await vi.waitFor(() => expect(seen).toContain(other.id), { timeout: leaseMs * 2 });
        expect(seen).not.toContain(interrupt.id);
        // Keep repairing for several leases: B cannot take over the active head.
        await new Promise((resolve) => setTimeout(resolve, leaseMs * 2 + 200));
        expect(seen.filter((id) => id === first.id)).toHaveLength(1);
        expect(seen).not.toContain(interrupt.id);
        expect(attempts).toBeGreaterThan(5);
        const claim = await pool.query<{ claimed_by: string; live: boolean }>(
          'SELECT claimed_by, lease_until > clock_timestamp() AS live FROM transcript_event_claims WHERE group_id = $1 AND event_seq = $2',
          [groupId, first.seq],
        );
        expect(claim.rows).toEqual([{ claimed_by: 'barrier-a', live: true }]);
        available = true;
        await vi.waitFor(() => expect(seen).toContain(interrupt.id));
        await sourceA.stop();
        await sourceB.stop();
        const events: Event[] = [];
        for await (const event of store.read(ws, ses, {
          fromCursor: '',
          maxEvents: 0,
          subpath: '*',
        }))
          events.push(event);
        expect(events.map(({ id }) => id)).toEqual([
          first.id,
          interrupt.id,
          processed.id,
          failure.id,
          idle.id,
        ]);
        expect(seen).toHaveLength(3);
      } finally {
        available = true;
        await sourceA.stop();
        await sourceB.stop();
        await deletePostgresRowsForWorkspace(pool, ws);
        await deletePostgresRowsForWorkspace(pool, otherWs);
      }
    },
    20_000,
  );

  it('cancels repair backoff without marking the head processed and recovers on another worker', async () => {
    const { ws, ses } = uniqueIds('pg_barrier_stop');
    const groupId = `barrier-stop-${ws}`;
    const first = makeEvents(ws, ses, [1], 'client', 'user.message')[0]!;
    const next = makeEvents(ws, ses, [2], 'client', 'user.interrupt')[0]!;
    const sourceA = new PostgresSessionEventSource({
      pool,
      groupId,
      pollIntervalMs: 10_000,
      leaseMs: 150,
      workerId: 'stop-a',
    });
    const sourceB = new PostgresSessionEventSource({
      pool,
      groupId,
      pollIntervalMs: 10,
      leaseMs: 150,
      workerId: 'stop-b',
    });
    let attempts = 0;
    const repair = async (): Promise<void> => {
      attempts += 1;
      throw new SessionEventBarrierError('storage unavailable', repair);
    };
    try {
      await store.append(ws, ses, [first, next]);
      await sourceA.start(repair);
      await vi.waitFor(() => expect(attempts).toBe(1));
      let stopped = false;
      const stopping = sourceA.stop().then(() => {
        stopped = true;
      });
      await vi.waitFor(() => expect(stopped).toBe(true), { timeout: 500 });
      await stopping;
      const result = await pool.query<{ processed_at: string | null }>(
        'SELECT processed_at FROM transcript_event_claims WHERE group_id = $1 AND event_seq = $2',
        [groupId, first.seq],
      );
      expect(result.rows).toEqual([{ processed_at: null }]);
      const seen: string[] = [];
      await sourceB.start(async (event) => {
        seen.push(event.id);
      });
      await vi.waitFor(() => expect(seen).toEqual([first.id, next.id]));
      expect(attempts).toBe(1);
    } finally {
      await sourceA.stop();
      await sourceB.stop();
      await deletePostgresRowsForWorkspace(pool, ws);
    }
  });

  it('does not settle a claim whose ownership changed during its handler', async () => {
    const { ws, ses } = uniqueIds('pg_claim_fence');
    const groupId = `claim-fence-${ws}`;
    const first = makeEvents(ws, ses, [1], 'client', 'user.message')[0]!;
    const next = makeEvents(ws, ses, [2], 'client', 'user.interrupt')[0]!;
    const source = new PostgresSessionEventSource({
      pool,
      groupId,
      pollIntervalMs: 10,
      leaseMs: 150,
      workerId: 'old-owner',
    });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const seen: string[] = [];
    try {
      await store.append(ws, ses, [first, next]);
      await source.start(async (event) => {
        seen.push(event.id);
        await gate;
      });
      await vi.waitFor(() => expect(seen).toEqual([first.id]));
      await pool.query(
        "UPDATE transcript_event_claims SET claimed_by = 'replacement', lease_until = clock_timestamp() + interval '10 seconds' WHERE group_id = $1 AND event_seq = $2",
        [groupId, first.seq],
      );
      release();
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(seen).toEqual([first.id]);
      const result = await pool.query<{ claimed_by: string; processed_at: string | null }>(
        'SELECT claimed_by, processed_at FROM transcript_event_claims WHERE group_id = $1 AND event_seq = $2',
        [groupId, first.seq],
      );
      expect(result.rows).toEqual([{ claimed_by: 'replacement', processed_at: null }]);
    } finally {
      release();
      await source.stop();
      await deletePostgresRowsForWorkspace(pool, ws);
    }
  });

  it('idempotently adds nullable user attribution to legacy tables', async () => {
    const schema = `transcript_store_user_id_${Date.now().toString(36)}${Math.random()
      .toString(36)
      .slice(2, 8)}`;
    await pool.query(`CREATE SCHEMA ${schema}`);
    const legacyPool = new Pool({
      connectionString: transcriptStoreDatabaseUrl(),
      options: `-c search_path=${schema}`,
    });
    try {
      await legacyPool.query(`
        CREATE TABLE transcript_events (
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
          inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (workspace_id, session_id, event_id)
        )
      `);
      await applyPostgresTranscriptMigrations(legacyPool);
      await applyPostgresTranscriptMigrations(legacyPool);

      const result = await legacyPool.query<{ is_nullable: string }>(`
        SELECT is_nullable
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'transcript_events'
          AND column_name = 'user_id'
      `);
      expect(result.rows).toEqual([{ is_nullable: 'YES' }]);
    } finally {
      await legacyPool.end();
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    }
  });

  it('appends, dedupes across subpaths, and reads session events in order', async () => {
    const { ws, ses } = uniqueIds('pg_read');
    try {
      const events = makeEvents(ws, ses, [1, 2, 3]);
      expect(await store.append(ws, ses, events)).toEqual(events.map((event) => event.id));
      expect(events.every((event) => event.seq > 0)).toBe(true);
      const duplicate = { ...events[1]!, subpath: 'subagents/dedup-child', seq: 0 };
      expect(await store.append(ws, ses, [duplicate])).toEqual([events[1]!.id]);
      expect(duplicate.seq).toBe(events[1]!.seq);

      const persisted = await pool.query<{ subpath: string; seq: string }>(
        `
          SELECT subpath, seq::text
          FROM transcript_events
          WHERE workspace_id = $1 AND session_id = $2 AND event_id = $3
        `,
        [ws, ses, events[1]!.id],
      );
      expect(persisted.rows).toEqual([{ subpath: '', seq: String(events[1]!.seq) }]);

      const seen: Event[] = [];
      for await (const event of store.read(ws, ses, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '*',
      })) {
        seen.push(event);
      }

      expect(seen.map((event) => event.id)).toEqual(events.map((event) => event.id));
      expect(seen.map((event) => event.seq)).toEqual(events.map((event) => event.seq));
      expect(seen.map((event) => event.subpath)).toEqual(['', '', '']);
      expect(seen.map((event) => Buffer.from(event.payload)[0])).toEqual([1, 2, 3]);
    } finally {
      await deletePostgresRowsForWorkspace(pool, ws);
    }
  }, 30000);

  it('reads and tails numeric sequence order across digit and page boundaries', async () => {
    const schema = `transcript_numeric_order_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    await pool.query(`CREATE SCHEMA ${schema}`);
    const isolatedPool = new Pool({
      connectionString: transcriptStoreDatabaseUrl(),
      options: `-c search_path=${schema}`,
    });
    const isolatedStore = new PostgresTranscriptStore({ pool: isolatedPool });
    const { ws, ses } = uniqueIds('pg_numeric_order');
    try {
      await applyPostgresTranscriptMigrations(isolatedPool);
      const events = makeEvents(
        ws,
        ses,
        Array.from({ length: 1001 }, (_, i) => i + 1),
      );
      await isolatedStore.append(ws, ses, events);
      expect(events.map(({ seq }) => seq)).toEqual(Array.from({ length: 1001 }, (_, i) => i + 1));
      const seen: Event[] = [];
      for await (const event of isolatedStore.read(ws, ses, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '*',
      })) {
        seen.push(event);
        if (seen.length > events.length) break;
      }
      expect(seen.map(({ id }) => id)).toEqual(events.map(({ id }) => id));
      const tailed: Event[] = [];
      for await (const event of isolatedStore.tail(ws, ses, { fromCursor: '9', subpath: '*' })) {
        tailed.push(event);
        if (tailed.length === events.length - 8) break;
      }
      expect(tailed.map(({ id }) => id)).toEqual(events.slice(8).map(({ id }) => id));
    } finally {
      await isolatedStore.close();
      await pool.query(`DROP SCHEMA ${schema} CASCADE`);
    }
  }, 10_000);

  it('tails events appended after subscription', async () => {
    const { ws, ses } = uniqueIds('pg_tail');
    const ac = new AbortController();
    try {
      const received: Event[] = [];
      const done = (async () => {
        for await (const event of store.tail(ws, ses, {
          fromCursor: '',
          subpath: '',
          signal: ac.signal,
        })) {
          received.push(event);
          if (received.length === 2) ac.abort();
        }
      })();

      await new Promise((resolve) => setTimeout(resolve, 100));
      const events = makeEvents(ws, ses, [7, 8]);
      await store.append(ws, ses, events);
      await done;

      expect(received.map((event) => event.id)).toEqual(events.map((event) => event.id));
    } finally {
      ac.abort();
      await deletePostgresRowsForWorkspace(pool, ws);
    }
  }, 30000);

  it('claims user events once per consumer group', async () => {
    const { ws, ses } = uniqueIds('pg_claim');
    const groupId = `pg_claim_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const sourceA = new PostgresSessionEventSource({
      pool,
      groupId,
      pollIntervalMs: 50,
      leaseMs: 1000,
      workerId: 'a',
    });
    const sourceB = new PostgresSessionEventSource({
      pool,
      groupId,
      pollIntervalMs: 50,
      leaseMs: 1000,
      workerId: 'b',
    });
    try {
      const seen: string[] = [];
      const handler = async (event: Event): Promise<void> => {
        seen.push(event.id);
      };
      await sourceA.start(handler);
      await sourceB.start(handler);

      const events = makeEvents(ws, ses, [1, 2, 3], 'client', 'user.message');
      await store.append(ws, ses, events);

      await waitFor(() => seen.length === 3);
      expect(new Set(seen)).toEqual(new Set(events.map((event) => event.id)));
    } finally {
      await sourceA.stop();
      await sourceB.stop();
      await deletePostgresRowsForWorkspace(pool, ws);
    }
  }, 30000);

  it('preserves optional user attribution through append, read, and claims', async () => {
    const { ws, ses } = uniqueIds('pg_user_id');
    const source = new PostgresSessionEventSource({
      pool,
      groupId: `pg_user_id_${Date.now()}_${Math.random().toString(36).slice(2)}`,
      pollIntervalMs: 50,
      leaseMs: 1000,
    });
    try {
      const events = makeEvents(ws, ses, [1, 2, 3], 'client', 'user.message');
      events[0]!.userId = 'user_pg_1';
      events[1]!.userId = '';
      await store.append(ws, ses, events);
      await pool.query(
        `
          UPDATE transcript_events
          SET user_id = ''
          WHERE workspace_id = $1 AND session_id = $2 AND event_id = $3
        `,
        [ws, ses, events[1]!.id],
      );

      const read: Event[] = [];
      for await (const event of store.read(ws, ses, {
        fromCursor: '',
        maxEvents: 0,
        subpath: '',
      })) {
        read.push(event);
      }
      expect(read.map((event) => event.userId)).toEqual(['user_pg_1', undefined, undefined]);

      const claimed: Event[] = [];
      await source.start(async (event) => {
        claimed.push(event);
      });
      await waitFor(() => claimed.length === events.length);
      expect(claimed.map((event) => event.userId)).toEqual(['user_pg_1', undefined, undefined]);
    } finally {
      await source.stop();
      await deletePostgresRowsForWorkspace(pool, ws);
    }
  }, 30000);

  it.each(['session.archived', 'session.deleted'] as const)(
    'claims %s sentinels with the default event filter',
    async (kind) => {
      const { ws, ses } = uniqueIds(`pg_lifecycle_claim_${kind}`);
      const source = new PostgresSessionEventSource({
        pool,
        groupId: `pg_lifecycle_claim_${Date.now()}_${Math.random().toString(36).slice(2)}`,
        pollIntervalMs: 50,
        leaseMs: 1000,
      });
      try {
        const seen: Event[] = [];
        await source.start(async (event) => {
          if (event.workspaceId === ws && event.sessionId === ses) seen.push(event);
        });

        if (kind === 'session.archived') {
          await store.archive(ws, ses);
        } else {
          await store.append(ws, ses, makeEvents(ws, ses, [0], 'registry-service', kind));
        }
        await waitFor(() => seen.length === 1);
        expect(seen[0]).toMatchObject({
          workspaceId: ws,
          sessionId: ses,
          producedBy: kind === 'session.archived' ? 'transcript-store' : 'registry-service',
          kind,
        });
      } finally {
        await source.stop();
        await deletePostgresRowsForWorkspace(pool, ws);
      }
    },
    30000,
  );
});

function makeEvents(
  workspaceId: string,
  sessionId: string,
  payloads: number[],
  producedBy = 'harness',
  kind = 'agent.message',
): Event[] {
  return payloads.map((payload) => ({
    id: `evt_pg_${payload}_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    workspaceId,
    sessionId,
    subpath: '',
    seq: 0,
    producedAt: new Date().toISOString(),
    producedBy,
    kind,
    payload: new Uint8Array([payload]),
    idempotencyKey: '',
  }));
}

function transcriptStoreDatabaseUrl(): string {
  return (
    process.env['TRANSCRIPT_STORE_DATABASE_URL'] ??
    process.env['DATABASE_URL'] ??
    'postgres://orca:orca@localhost:5432/transcriptstore'
  );
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < 5000) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error('condition not met');
}
