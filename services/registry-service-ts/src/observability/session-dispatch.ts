// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { sql } from 'drizzle-orm';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  oldestUnprocessedClientUserEventAgeSeconds,
  sessionDispatchObservabilityLastSuccessTimestampSeconds,
  sessionDispatchObservabilityRefreshTotal,
  sessionLifecycleOutboxOldestPendingAgeSeconds,
  sessionLifecycleOutboxPending,
  sessionLifecycleOutboxPendingAttempts,
  unprocessedClientUserEvents,
} from '../metrics.js';

export const SESSION_DISPATCH_OBSERVABILITY_REFRESH_INTERVAL_MS = 5_000;

interface UnprocessedClientUserEventRow {
  workspaceId: string;
  eventCount: number;
  oldestAgeSeconds: number;
}

interface PendingSessionLifecycleOutbox {
  rowCount: number;
  oldestAgeSeconds: number;
  attemptCount: number;
}

export interface SessionDispatchObservabilitySnapshot {
  unprocessedClientUserEvents: UnprocessedClientUserEventRow[];
  pendingSessionLifecycleOutbox: PendingSessionLifecycleOutbox;
}

export interface SessionDispatchObservabilityReader {
  read(): Promise<SessionDispatchObservabilitySnapshot>;
}

interface WorkspaceGauge {
  reset(): void;
  set(labels: { workspace_id: string }, value: number): void;
}

interface Gauge {
  set(value: number): void;
}

interface OutcomeCounter {
  inc(labels: { result: 'success' | 'error' | 'overlap' }): void;
}

export interface SessionDispatchObservabilityMetrics {
  unprocessedClientUserEvents: WorkspaceGauge;
  oldestUnprocessedClientUserEventAgeSeconds: WorkspaceGauge;
  sessionLifecycleOutboxPending: Gauge;
  sessionLifecycleOutboxOldestPendingAgeSeconds: Gauge;
  sessionLifecycleOutboxPendingAttempts: Gauge;
  lastSuccessTimestampSeconds: Gauge;
  refreshTotal: OutcomeCounter;
}

interface Logger {
  warn(message: string): void;
}

const metrics: SessionDispatchObservabilityMetrics = {
  unprocessedClientUserEvents,
  oldestUnprocessedClientUserEventAgeSeconds,
  sessionLifecycleOutboxPending,
  sessionLifecycleOutboxOldestPendingAgeSeconds,
  sessionLifecycleOutboxPendingAttempts,
  lastSuccessTimestampSeconds: sessionDispatchObservabilityLastSuccessTimestampSeconds,
  refreshTotal: sessionDispatchObservabilityRefreshTotal,
};

const logger: Logger = {
  warn: (message) => console.warn(message),
};

/**
 * Build the metadata-only read model query used for dispatch backlog metrics.
 * It intentionally selects aggregate state only: never event payloads, secret
 * references, or MCP request bodies.
 */
export function buildSessionDispatchObservabilityReader(
  db: Pick<DbClient, 'transaction'>,
): SessionDispatchObservabilityReader {
  return {
    async read(): Promise<SessionDispatchObservabilitySnapshot> {
      return db.transaction(async (tx) => {
        // The refresh interval is five seconds. Bound each aggregate query below
        // that interval so a blocked query cannot permanently hold the overlap guard.
        await tx.execute(sql`SET LOCAL statement_timeout = '4000ms'`);

        const eventsResult = await tx.execute(sql`
          SELECT
            workspace_id,
            COUNT(*)::double precision AS event_count,
            GREATEST(
              0,
              EXTRACT(EPOCH FROM clock_timestamp() - MIN(produced_at)::timestamptz)
            ) AS oldest_age_seconds
          FROM session_events_index
          WHERE visibility = 'public'
            AND produced_by = 'client'
            AND kind LIKE 'user.%'
            AND processed_at IS NULL
          GROUP BY workspace_id
        `);
        const outboxResult = await tx.execute(sql`
          SELECT
            COUNT(*)::double precision AS pending_row_count,
            GREATEST(
              0,
              COALESCE(
                EXTRACT(EPOCH FROM clock_timestamp() - MIN(created_at)),
                0
              )
            ) AS oldest_age_seconds,
            COALESCE(SUM(attempt_count), 0)::double precision AS attempt_count
          FROM session_lifecycle_outbox
          WHERE published_at IS NULL
        `);

        const unprocessedClientUserEvents = rowsOf(eventsResult).map((row) => ({
          workspaceId: stringColumn(row, 'workspace_id'),
          eventCount: nonNegativeNumberColumn(row, 'event_count'),
          oldestAgeSeconds: nonNegativeNumberColumn(row, 'oldest_age_seconds'),
        }));
        const outboxRow = rowsOf(outboxResult)[0];
        if (!outboxRow)
          throw new Error('session dispatch observability outbox query returned no row');

        return {
          unprocessedClientUserEvents,
          pendingSessionLifecycleOutbox: {
            rowCount: nonNegativeNumberColumn(outboxRow, 'pending_row_count'),
            oldestAgeSeconds: nonNegativeNumberColumn(outboxRow, 'oldest_age_seconds'),
            attemptCount: nonNegativeNumberColumn(outboxRow, 'attempt_count'),
          },
        };
      });
    },
  };
}

export class SessionDispatchObservabilityRefresher {
  private running = false;

  constructor(
    private readonly reader: SessionDispatchObservabilityReader,
    private readonly metricSinks: SessionDispatchObservabilityMetrics = metrics,
    private readonly refreshLogger: Logger = logger,
  ) {}

  async refresh(): Promise<void> {
    if (this.running) {
      this.metricSinks.refreshTotal.inc({ result: 'overlap' });
      return;
    }

    this.running = true;
    try {
      const snapshot = await this.reader.read();
      updateSessionDispatchObservabilityMetrics(snapshot, this.metricSinks);
      this.metricSinks.lastSuccessTimestampSeconds.set(Date.now() / 1_000);
      this.metricSinks.refreshTotal.inc({ result: 'success' });
    } catch {
      this.metricSinks.refreshTotal.inc({ result: 'error' });
      // Queries select aggregate metadata only. Keep errors out of the log so
      // no driver-provided text can accidentally expose sensitive values.
      this.refreshLogger.warn(
        'registry-service-ts failed to refresh session dispatch observability',
      );
    } finally {
      this.running = false;
    }
  }
}

export function updateSessionDispatchObservabilityMetrics(
  snapshot: SessionDispatchObservabilitySnapshot,
  metricSinks: SessionDispatchObservabilityMetrics = metrics,
): void {
  // A successful refresh replaces the complete workspace view. reset() drops
  // workspaces that drained since the prior refresh instead of preserving a
  // stale labeled series.
  metricSinks.unprocessedClientUserEvents.reset();
  metricSinks.oldestUnprocessedClientUserEventAgeSeconds.reset();
  for (const row of snapshot.unprocessedClientUserEvents) {
    metricSinks.unprocessedClientUserEvents.set({ workspace_id: row.workspaceId }, row.eventCount);
    metricSinks.oldestUnprocessedClientUserEventAgeSeconds.set(
      { workspace_id: row.workspaceId },
      row.oldestAgeSeconds,
    );
  }
  metricSinks.sessionLifecycleOutboxPending.set(snapshot.pendingSessionLifecycleOutbox.rowCount);
  metricSinks.sessionLifecycleOutboxOldestPendingAgeSeconds.set(
    snapshot.pendingSessionLifecycleOutbox.oldestAgeSeconds,
  );
  metricSinks.sessionLifecycleOutboxPendingAttempts.set(
    snapshot.pendingSessionLifecycleOutbox.attemptCount,
  );
}

function rowsOf(result: { rows: unknown }): Record<string, unknown>[] {
  if (!Array.isArray(result.rows))
    throw new Error('session dispatch observability query returned invalid rows');
  return result.rows.map((row) => {
    if (row === null || typeof row !== 'object' || Array.isArray(row)) {
      throw new Error('session dispatch observability query returned an invalid row');
    }
    return row as Record<string, unknown>;
  });
}

function stringColumn(row: Record<string, unknown>, name: string): string {
  const value = row[name];
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error('session dispatch observability query returned an invalid workspace');
  }
  return value;
}

function nonNegativeNumberColumn(row: Record<string, unknown>, name: string): number {
  const value = Number(row[name]);
  if (!Number.isFinite(value) || value < 0) {
    throw new Error('session dispatch observability query returned an invalid metric value');
  }
  return value;
}
