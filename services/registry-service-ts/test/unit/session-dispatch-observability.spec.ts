// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import type { SQL } from 'drizzle-orm';
import { PgDialect } from 'drizzle-orm/pg-core';
import { describe, expect, it, vi } from 'vitest';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  ENSURE_SESSION_EVENTS_INDEX_UNPROCESSED_CLIENT_USER_SQL,
  ensureSessionEventsIndexUnprocessedClientUser,
} from '../../src/migrate.js';
import {
  buildSessionDispatchObservabilityReader,
  SessionDispatchObservabilityRefresher,
  type SessionDispatchObservabilityMetrics,
  type SessionDispatchObservabilitySnapshot,
} from '../../src/observability/session-dispatch.js';

describe('session dispatch observability', () => {
  it('creates its partial index concurrently outside Drizzle journal transaction', async () => {
    const migration = readFileSync(
      new URL(
        '../../src/persistence/postgres/migrations/0052_session_events_unprocessed_client_user_index.sql',
        import.meta.url,
      ),
      'utf8',
    );

    expect(migration).toContain(
      'creates this index concurrently after Drizzle commits its transaction.',
    );
    expect(migration).toContain('SELECT 1;');
    expect(migration).not.toContain('CREATE INDEX');

    const query = vi.fn().mockResolvedValue({ rows: [] });
    await ensureSessionEventsIndexUnprocessedClientUser({ query });

    expect(query).toHaveBeenCalledTimes(2);
    expect(query).toHaveBeenNthCalledWith(
      2,
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS "session_events_index_unprocessed_client_user_idx" ON "session_events_index" USING btree ("workspace_id","produced_at") WHERE "session_events_index"."visibility" = \'public\' AND "session_events_index"."produced_by" = \'client\' AND "session_events_index"."kind" LIKE \'user.%\' AND "session_events_index"."processed_at" IS NULL;',
    );
    expect(query).toHaveBeenNthCalledWith(
      2,
      ENSURE_SESSION_EVENTS_INDEX_UNPROCESSED_CLIENT_USER_SQL,
    );
  });

  it('drops an invalid interrupted concurrent index before unconditionally ensuring it', async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ exists: true }] })
      .mockResolvedValue({ rows: [] });

    await ensureSessionEventsIndexUnprocessedClientUser({ query });

    expect(query).toHaveBeenCalledTimes(3);
    expect(query).toHaveBeenNthCalledWith(
      2,
      'DROP INDEX CONCURRENTLY IF EXISTS "session_events_index_unprocessed_client_user_idx";',
    );
    expect(query).toHaveBeenNthCalledWith(
      3,
      ENSURE_SESSION_EVENTS_INDEX_UNPROCESSED_CLIENT_USER_SQL,
    );
  });

  it('maps aggregate rows to gauges and removes workspace labels that drained', async () => {
    const snapshots: SessionDispatchObservabilitySnapshot[] = [
      {
        unprocessedClientUserEvents: [
          { workspaceId: 'ws_a', eventCount: 2, oldestAgeSeconds: 12.5 },
          { workspaceId: 'ws_b', eventCount: 1, oldestAgeSeconds: 4 },
        ],
        pendingSessionLifecycleOutbox: { rowCount: 3, oldestAgeSeconds: 30, attemptCount: 7 },
      },
      {
        unprocessedClientUserEvents: [{ workspaceId: 'ws_b', eventCount: 4, oldestAgeSeconds: 9 }],
        pendingSessionLifecycleOutbox: { rowCount: 0, oldestAgeSeconds: 0, attemptCount: 0 },
      },
    ];
    const metricState = fakeMetrics();
    const refresher = new SessionDispatchObservabilityRefresher(
      { read: async () => snapshots.shift()! },
      metricState.metrics,
    );

    await refresher.refresh();
    expect(metricState.unprocessed).toEqual(
      new Map([
        ['ws_a', 2],
        ['ws_b', 1],
      ]),
    );
    expect(metricState.oldestUnprocessedAge).toEqual(
      new Map([
        ['ws_a', 12.5],
        ['ws_b', 4],
      ]),
    );
    expect(metricState.outbox).toEqual({ rows: 3, oldestAgeSeconds: 30, attempts: 7 });

    await refresher.refresh();
    expect(metricState.unprocessed).toEqual(new Map([['ws_b', 4]]));
    expect(metricState.oldestUnprocessedAge).toEqual(new Map([['ws_b', 9]]));
    expect(metricState.outbox).toEqual({ rows: 0, oldestAgeSeconds: 0, attempts: 0 });
    expect(metricState.refreshOutcomes).toEqual(['success', 'success']);
  });

  it('reads aggregate query rows in a transaction with a local statement timeout', async () => {
    const execute = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({
        rows: [
          { workspace_id: 'ws_a', event_count: '2', oldest_age_seconds: '12.5' },
          { workspace_id: 'ws_b', event_count: 1, oldest_age_seconds: 4 },
        ],
      })
      .mockResolvedValueOnce({
        rows: [{ pending_row_count: '3', oldest_age_seconds: '30', attempt_count: '7' }],
      });
    const transaction = vi.fn(
      async (callback: (tx: Pick<DbClient, 'execute'>) => Promise<unknown>) =>
        callback({ execute } as unknown as Pick<DbClient, 'execute'>),
    );
    const reader = buildSessionDispatchObservabilityReader({ transaction } as unknown as Pick<
      DbClient,
      'transaction'
    >);

    await expect(reader.read()).resolves.toEqual({
      unprocessedClientUserEvents: [
        { workspaceId: 'ws_a', eventCount: 2, oldestAgeSeconds: 12.5 },
        { workspaceId: 'ws_b', eventCount: 1, oldestAgeSeconds: 4 },
      ],
      pendingSessionLifecycleOutbox: { rowCount: 3, oldestAgeSeconds: 30, attemptCount: 7 },
    });
    expect(transaction).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(3);
    expect(sqlText(execute.mock.calls[0]![0] as { getSQL(): SQL })).toBe(
      "SET LOCAL statement_timeout = '4000ms'",
    );
    expect(sqlText(execute.mock.calls[1]![0] as { getSQL(): SQL })).toContain(
      'FROM session_events_index',
    );
    expect(sqlText(execute.mock.calls[2]![0] as { getSQL(): SQL })).toContain(
      'FROM session_lifecycle_outbox',
    );
  });

  it('retries after a timed-out aggregate refresh', async () => {
    const execute = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockRejectedValueOnce(new Error('canceling statement due to statement timeout'))
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({
        rows: [{ pending_row_count: 0, oldest_age_seconds: 0, attempt_count: 0 }],
      });
    const transaction = vi.fn(
      async (callback: (tx: Pick<DbClient, 'execute'>) => Promise<unknown>) =>
        callback({ execute } as unknown as Pick<DbClient, 'execute'>),
    );
    const metricState = fakeMetrics();
    const warn = vi.fn();
    const refresher = new SessionDispatchObservabilityRefresher(
      buildSessionDispatchObservabilityReader({ transaction } as unknown as Pick<
        DbClient,
        'transaction'
      >),
      metricState.metrics,
      { warn },
    );

    await refresher.refresh();
    await refresher.refresh();

    expect(transaction).toHaveBeenCalledTimes(2);
    expect(metricState.refreshOutcomes).toEqual(['error', 'success']);
    expect(warn).toHaveBeenCalledWith(
      'registry-service-ts failed to refresh session dispatch observability',
    );
  });

  it('skips overlap and preserves last-success timestamp on refresh failures', async () => {
    let completeRead: ((snapshot: SessionDispatchObservabilitySnapshot) => void) | undefined;
    const blockedRead = new Promise<SessionDispatchObservabilitySnapshot>((resolve) => {
      completeRead = resolve;
    });
    const metricState = fakeMetrics();
    const snapshot: SessionDispatchObservabilitySnapshot = {
      unprocessedClientUserEvents: [],
      pendingSessionLifecycleOutbox: { rowCount: 0, oldestAgeSeconds: 0, attemptCount: 0 },
    };
    const reader = { read: vi.fn(() => blockedRead).mockResolvedValueOnce(snapshot) };
    const refresher = new SessionDispatchObservabilityRefresher(reader, metricState.metrics);

    await refresher.refresh();
    const lastSuccessBeforeOverlap = metricState.lastSuccessTimestampSeconds;
    expect(lastSuccessBeforeOverlap).toEqual(expect.any(Number));

    const firstRefresh = refresher.refresh();
    await Promise.resolve();
    await refresher.refresh();
    expect(reader.read).toHaveBeenCalledTimes(2);
    expect(metricState.refreshOutcomes).toEqual(['success', 'overlap']);
    expect(metricState.lastSuccessTimestampSeconds).toBe(lastSuccessBeforeOverlap);

    completeRead?.(snapshot);
    await firstRefresh;
    expect(metricState.refreshOutcomes).toEqual(['success', 'overlap', 'success']);
    const lastSuccessTimestampSeconds = metricState.lastSuccessTimestampSeconds;
    expect(lastSuccessTimestampSeconds).toEqual(expect.any(Number));

    const warn = vi.fn();
    await new SessionDispatchObservabilityRefresher(
      { read: async () => Promise.reject(new Error('payload must not reach logs')) },
      metricState.metrics,
      { warn },
    ).refresh();
    expect(metricState.refreshOutcomes).toEqual(['success', 'overlap', 'success', 'error']);
    expect(metricState.lastSuccessTimestampSeconds).toBe(lastSuccessTimestampSeconds);
    expect(warn).toHaveBeenCalledWith(
      'registry-service-ts failed to refresh session dispatch observability',
    );
  });
});

function fakeMetrics(): {
  metrics: SessionDispatchObservabilityMetrics;
  unprocessed: Map<string, number>;
  oldestUnprocessedAge: Map<string, number>;
  outbox: { rows: number; oldestAgeSeconds: number; attempts: number };
  refreshOutcomes: Array<'success' | 'error' | 'overlap'>;
  lastSuccessTimestampSeconds: number | undefined;
} {
  const unprocessed = new Map<string, number>();
  const oldestUnprocessedAge = new Map<string, number>();
  const outbox = { rows: -1, oldestAgeSeconds: -1, attempts: -1 };
  const refreshOutcomes: Array<'success' | 'error' | 'overlap'> = [];
  let lastSuccessTimestampSeconds: number | undefined;

  return {
    metrics: {
      unprocessedClientUserEvents: {
        reset: () => unprocessed.clear(),
        set: ({ workspace_id }, value) => unprocessed.set(workspace_id, value),
      },
      oldestUnprocessedClientUserEventAgeSeconds: {
        reset: () => oldestUnprocessedAge.clear(),
        set: ({ workspace_id }, value) => oldestUnprocessedAge.set(workspace_id, value),
      },
      sessionLifecycleOutboxPending: { set: (value) => (outbox.rows = value) },
      sessionLifecycleOutboxOldestPendingAgeSeconds: {
        set: (value) => (outbox.oldestAgeSeconds = value),
      },
      sessionLifecycleOutboxPendingAttempts: { set: (value) => (outbox.attempts = value) },
      lastSuccessTimestampSeconds: {
        set: (value) => (lastSuccessTimestampSeconds = value),
      },
      refreshTotal: { inc: ({ result }) => refreshOutcomes.push(result) },
    },
    unprocessed,
    oldestUnprocessedAge,
    outbox,
    refreshOutcomes,
    get lastSuccessTimestampSeconds() {
      return lastSuccessTimestampSeconds;
    },
  };
}

function sqlText(query: { getSQL(): SQL }): string {
  return new PgDialect().sqlToQuery(query.getSQL()).sql;
}
