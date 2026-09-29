// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { getEventListeners } from 'node:events';
import {
  PostgresSessionEventSource,
  PostgresTranscriptStore,
  applyPostgresTranscriptMigrations,
  transcriptStoreMetricsRegistry,
  SessionEventBarrierError,
  type Event,
} from '../../src/index.js';

describe('PostgresTranscriptStore', () => {
  it.each(['repair', 'idle'])(
    'releases expired abort listeners during repeated %s waits',
    async (mode) => {
      const listeners = vi.spyOn(AbortSignal.prototype, 'addEventListener');
      const log = vi.spyOn(console, 'error').mockImplementation(() => undefined);
      let attempts = 0;
      const row = {
        seq: '1',
        workspace_id: 'ws_listeners',
        session_id: 'ses_listeners',
        event_id: 'evt_listeners',
        subpath: '',
        produced_at: new Date().toISOString(),
        produced_by: 'client',
        kind: 'user.message',
        payload: Buffer.from('{}'),
        idempotency_key: '',
        user_id: null,
      };
      const query = vi.fn(async () => ({ rows: [], rowCount: 1 }));
      const source = new PostgresSessionEventSource({
        pool: {
          query,
          connect: async () => ({
            query: async (sql: string) => {
              if (!sql.includes('WITH candidates')) return { rows: [] };
              if (mode === 'idle') attempts += 1;
              return { rows: mode === 'repair' ? [row] : [] };
            },
            release: () => {},
          }),
        } as never,
        groupId: 'listener-cleanup',
        pollIntervalMs: 1,
      });
      const repair = async (): Promise<void> => {
        attempts += 1;
        throw new SessionEventBarrierError('persistent storage outage', repair);
      };
      try {
        await source.start(repair);
        await vi.waitFor(() => expect(attempts).toBeGreaterThanOrEqual(30));
        const signals = new Set(
          listeners.mock.contexts.filter(
            (context): context is AbortSignal => context instanceof AbortSignal,
          ),
        );
        expect(signals.size).toBeGreaterThan(0);
        for (const signal of signals)
          expect(getEventListeners(signal, 'abort').length).toBeLessThanOrEqual(2);
        await source.stop();
        for (const signal of signals) expect(getEventListeners(signal, 'abort')).toHaveLength(0);
        expect(source.status()).toEqual({ ready: false, state: 'stopped' });
      } finally {
        await source.stop();
        listeners.mockRestore();
        log.mockRestore();
      }
    },
  );

  it('does not start a handler when shutdown wins an in-flight claim renewal', async () => {
    let release!: () => void;
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    let renewals = 0;
    const row = {
      seq: '1',
      workspace_id: 'ws_stop',
      session_id: 'ses_stop',
      event_id: 'evt_stop',
      subpath: '',
      produced_at: new Date().toISOString(),
      produced_by: 'client',
      kind: 'user.message',
      payload: Buffer.from('{}'),
      idempotency_key: '',
      user_id: null,
    };
    const query = vi.fn(async (sql: string) => {
      if (sql.includes('SET lease_until')) {
        renewals += 1;
        await pending;
      }
      return { rows: [], rowCount: 1 };
    });
    const source = new PostgresSessionEventSource({
      pool: {
        query,
        connect: async () => ({
          query: async (sql: string) => ({ rows: sql.includes('WITH candidates') ? [row] : [] }),
          release: () => {},
        }),
      } as never,
      groupId: 'stop-renewal',
      pollIntervalMs: 1,
    });
    const handler = vi.fn(async () => {});
    try {
      await source.start(handler);
      await vi.waitFor(() => expect(renewals).toBe(1));
      const stopping = source.stop();
      release();
      await stopping;
      expect(handler).not.toHaveBeenCalled();
      expect(query.mock.calls.some(([sql]) => sql.includes('SET processed_at'))).toBe(false);
    } finally {
      release();
      await source.stop();
    }
  });

  it('stays ready while ordinary claim retries continue, then reports stopped', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const connect = vi.fn(async () => {
      throw new Error('transient database failure');
    });
    const source = new PostgresSessionEventSource({
      pool: { connect } as never,
      groupId: 'pg-source-status-retry-unit',
      pollIntervalMs: 1,
    });

    try {
      await source.start(async () => undefined);
      await waitFor(() => connect.mock.calls.length > 0);

      expect(source.status()).toEqual({ ready: true, state: 'running' });
      await source.stop();
      expect(source.status()).toEqual({ ready: false, state: 'stopped' });
    } finally {
      errorSpy.mockRestore();
    }
  });

  it('reports failed after a terminal post-start run-loop rejection without exposing its error', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const source = new PostgresSessionEventSource({
      pool: {} as never,
      groupId: 'pg-source-status-failure-unit',
    });
    (
      source as unknown as {
        run: (_handler: (event: Event) => Promise<void>) => Promise<void>;
      }
    ).run = async () => {
      await Promise.resolve();
      throw Object.assign(new Error('postgres://user:secret@host/database'), { code: '57P01' });
    };

    try {
      await source.start(async () => undefined);
      await waitFor(() => source.status().state === 'failed');
      await expect(source.whenFailed()).resolves.toBeUndefined();

      expect(source.status()).toEqual({ ready: false, state: 'failed' });
      expect(errorSpy).toHaveBeenCalledWith('PostgresSessionEventSource: run loop crashed', {
        name: 'Error',
        code: '57P01',
      });
      expect(JSON.stringify(source.status())).not.toContain('secret');
      expect(JSON.stringify(errorSpy.mock.calls)).not.toContain('secret');
      await source.stop();
    } finally {
      errorSpy.mockRestore();
    }
  });

  it('does not resolve whenFailed for intentional stop', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const source = new PostgresSessionEventSource({
      pool: { connect: vi.fn(async () => ({ query: vi.fn(), release: vi.fn() })) } as never,
      groupId: 'pg-source-intentional-stop-unit',
      pollIntervalMs: 1,
    });
    const failed = vi.fn();
    void source.whenFailed().then(failed);

    try {
      await source.start(async () => undefined);
      await source.stop();
      await Promise.resolve();

      expect(failed).not.toHaveBeenCalled();
    } finally {
      errorSpy.mockRestore();
    }
  });

  it('serializes migrations with a Postgres advisory transaction lock', async () => {
    const queries: string[] = [];
    const release = vi.fn();
    const client = {
      query: vi.fn(async (sql: string) => {
        queries.push(sql);
        return { rows: [] };
      }),
      release,
    };
    const pool = {
      connect: vi.fn(async () => client),
    };

    await applyPostgresTranscriptMigrations(pool as never);

    expect(pool.connect).toHaveBeenCalledOnce();
    expect(release).toHaveBeenCalledOnce();
    expect(queries[0]).toBe('BEGIN');
    expect(queries[1]).toContain("pg_advisory_xact_lock(hashtext('orca_transcript_store_schema'))");
    expect(queries).toContain(
      'ALTER TABLE transcript_events ADD COLUMN IF NOT EXISTS user_id TEXT',
    );
    expect(queries).toContain('COMMIT');
  });

  it('rolls back and releases the migration connection on DDL failure', async () => {
    const queries: string[] = [];
    const release = vi.fn();
    const client = {
      query: vi.fn(async (sql: string) => {
        queries.push(sql);
        if (sql.includes('CREATE TABLE IF NOT EXISTS transcript_events')) {
          throw new Error('ddl failed');
        }
        return { rows: [] };
      }),
      release,
    };
    const pool = {
      connect: vi.fn(async () => client),
    };

    await expect(applyPostgresTranscriptMigrations(pool as never)).rejects.toThrow('ddl failed');

    expect(queries).toContain('ROLLBACK');
    expect(queries).not.toContain('COMMIT');
    expect(release).toHaveBeenCalledOnce();
  });

  it('does not count an active tail as ok before it exits normally', async () => {
    const queries: string[] = [];
    const pool = {
      query: async (sql: string) => {
        queries.push(sql);
        if (sql.includes('max(seq)::text')) return { rows: [{ max_seq: null }] };
        return { rows: [] };
      },
      end: async () => undefined,
    };
    const store = new PostgresTranscriptStore({
      pool: pool as never,
      tailPollIntervalMs: 10_000,
    });
    const ac = new AbortController();
    const before = await tailOkCount();
    const iterator = store
      .tail('ws_pg_metric', 'ses_pg_metric', {
        fromCursor: '',
        subpath: '',
        signal: ac.signal,
      })
      [Symbol.asyncIterator]();

    const next = iterator.next();
    await waitFor(() => queries.length >= 2);

    expect(await tailOkCount()).toBe(before);

    ac.abort();
    await next;
  });

  it('can leave a caller-owned pool open on close', async () => {
    const end = vi.fn(async () => undefined);
    const store = new PostgresTranscriptStore({
      pool: { end } as never,
      closePool: false,
    });

    await store.close();

    expect(end).not.toHaveBeenCalled();
  });

  it('rejects an event whose embedded route differs from the append target before querying', async () => {
    const query = vi.fn();
    const store = new PostgresTranscriptStore({ pool: { query } as never });
    const event: Event = {
      id: 'evt_route_mismatch',
      workspaceId: 'ws_forged',
      sessionId: 'ses_target',
      subpath: '',
      seq: 0,
      producedAt: new Date().toISOString(),
      producedBy: 'client',
      kind: 'user.message',
      payload: new Uint8Array(),
      idempotencyKey: '',
    };

    await expect(store.append('ws_target', 'ses_target', [event])).rejects.toThrow(
      'event route mismatch: expected ws_target/ses_target, got ws_forged/ses_target',
    );
    expect(query).not.toHaveBeenCalled();
  });
});

async function tailOkCount(): Promise<number> {
  const metric = transcriptStoreMetricsRegistry.getSingleMetric('transcript_store_tail_total');
  const values = ((await metric?.get())?.values ?? []) as Array<{
    labels?: Record<string, string>;
    value: number;
  }>;
  return values.find((value) => value.labels?.['status'] === 'ok')?.value ?? 0;
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < 1000) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error('condition not met');
}
