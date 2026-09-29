// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, asc, eq, inArray, isNull, sql } from 'drizzle-orm';
import { v7 as uuidv7 } from 'uuid';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import type { DbClient } from '../persistence/postgres/client.js';
import { sessionLifecycleOutbox } from '../persistence/postgres/schema.js';

export const SESSION_LIFECYCLE_RECONCILE_INTERVAL_MS = 5_000;

export interface SessionLifecycleOutboxRow {
  id: string;
  workspaceId: string;
  sessionId: string;
  kind: 'session.initial_events' | 'session.archived' | 'session.deleted';
  events?: SerializedSessionEvent[];
  createdAt: Date;
}

interface SerializedSessionEvent {
  id: string;
  subpath: string;
  seq: number;
  producedAt: string;
  producedBy: string;
  kind: string;
  payloadBase64: string;
  idempotencyKey: string;
  userId?: string;
}

export interface SessionLifecycleReconcileResult {
  processed: number;
  published: number;
  failed: number;
}

export function newSessionArchiveOutboxRow(
  workspaceId: string,
  sessionId: string,
  now = new Date(),
): SessionLifecycleOutboxRow {
  return {
    id: `evt_${uuidv7()}`,
    workspaceId,
    sessionId,
    kind: 'session.archived',
    createdAt: now,
  };
}

export function newSessionDeleteOutboxRow(
  workspaceId: string,
  sessionId: string,
  now = new Date(),
): SessionLifecycleOutboxRow {
  return {
    id: `evt_${uuidv7()}`,
    workspaceId,
    sessionId,
    kind: 'session.deleted',
    createdAt: now,
  };
}

export function newSessionInitialEventsOutboxRow(
  workspaceId: string,
  sessionId: string,
  events: Event[],
  now = new Date(),
): SessionLifecycleOutboxRow {
  const firstEvent = events[0];
  if (!firstEvent) throw new Error('initial session events outbox row requires at least one event');
  return {
    id: firstEvent.id,
    workspaceId,
    sessionId,
    kind: 'session.initial_events',
    events: events.map((event) => {
      const userId = nonEmptyUserId(event.userId);
      return {
        id: event.id,
        subpath: event.subpath,
        seq: event.seq,
        producedAt: event.producedAt,
        producedBy: event.producedBy,
        kind: event.kind,
        payloadBase64: Buffer.from(event.payload).toString('base64'),
        idempotencyKey: event.idempotencyKey,
        ...(userId !== undefined ? { userId } : {}),
      };
    }),
    createdAt: now,
  };
}

/**
 * Publish pending Session transcript events in bounded batches.
 *
 * Each candidate is locked before publication so Registry replicas cannot
 * publish the same row concurrently. Initial-event retries also reconcile
 * stable event ids against the transcript before appending, which closes the
 * crash window between a successful append and the publishedAt update.
 * Harness session termination remains idempotent for lifecycle sentinels.
 */
export async function reconcileSessionLifecycleOutbox(
  db: DbClient,
  store: TranscriptStore,
  options: { eventIds?: string[]; batchSize?: number; now?: Date } = {},
): Promise<SessionLifecycleReconcileResult> {
  const result: SessionLifecycleReconcileResult = { processed: 0, published: 0, failed: 0 };
  if (options.eventIds?.length === 0) return result;

  const filters = [isNull(sessionLifecycleOutbox.publishedAt)];
  if (options.eventIds) filters.push(inArray(sessionLifecycleOutbox.id, options.eventIds));
  const rows = await db
    .select({
      id: sessionLifecycleOutbox.id,
      workspaceId: sessionLifecycleOutbox.workspaceId,
      sessionId: sessionLifecycleOutbox.sessionId,
      kind: sessionLifecycleOutbox.kind,
      events: sessionLifecycleOutbox.events,
      createdAt: sessionLifecycleOutbox.createdAt,
    })
    .from(sessionLifecycleOutbox)
    .where(and(...filters))
    .orderBy(asc(sessionLifecycleOutbox.createdAt))
    .limit(options.batchSize ?? 100);

  for (const candidate of rows) {
    const attemptedAt = options.now ?? new Date();
    const outcome = await db.transaction(async (tx) => {
      const lockedRows = await tx
        .select({
          id: sessionLifecycleOutbox.id,
          workspaceId: sessionLifecycleOutbox.workspaceId,
          sessionId: sessionLifecycleOutbox.sessionId,
          kind: sessionLifecycleOutbox.kind,
          events: sessionLifecycleOutbox.events,
          createdAt: sessionLifecycleOutbox.createdAt,
        })
        .from(sessionLifecycleOutbox)
        .where(
          and(
            eq(sessionLifecycleOutbox.id, candidate.id),
            isNull(sessionLifecycleOutbox.publishedAt),
          ),
        )
        .for('update')
        .limit(1);
      const row = lockedRows[0];
      if (!row) return 'skipped' as const;

      try {
        const events =
          row.kind === 'session.initial_events'
            ? deserializeInitialEvents(row)
            : [
                {
                  id: row.id,
                  workspaceId: row.workspaceId,
                  sessionId: row.sessionId,
                  subpath: '',
                  seq: 0,
                  producedAt: row.createdAt.toISOString(),
                  producedBy: 'registry-service',
                  kind: row.kind,
                  payload: new Uint8Array(),
                  idempotencyKey: row.id,
                } satisfies Event,
              ];
        const eventsToAppend =
          row.kind === 'session.initial_events'
            ? await missingInitialEvents(store, row.workspaceId, row.sessionId, events)
            : events;
        if (eventsToAppend.length > 0) {
          await store.append(row.workspaceId, row.sessionId, eventsToAppend);
        }
        await tx
          .update(sessionLifecycleOutbox)
          .set({
            publishedAt: attemptedAt,
            lastAttemptAt: attemptedAt,
            attemptCount: sql`${sessionLifecycleOutbox.attemptCount} + 1`,
          })
          .where(
            and(eq(sessionLifecycleOutbox.id, row.id), isNull(sessionLifecycleOutbox.publishedAt)),
          );
        return 'published' as const;
      } catch {
        await tx
          .update(sessionLifecycleOutbox)
          .set({
            lastAttemptAt: attemptedAt,
            attemptCount: sql`${sessionLifecycleOutbox.attemptCount} + 1`,
          })
          .where(
            and(eq(sessionLifecycleOutbox.id, row.id), isNull(sessionLifecycleOutbox.publishedAt)),
          );
        return 'failed' as const;
      }
    });
    if (outcome === 'skipped') continue;
    result.processed += 1;
    if (outcome === 'published') {
      result.published += 1;
    } else {
      result.failed += 1;
    }
  }

  return result;
}

async function missingInitialEvents(
  store: TranscriptStore,
  workspaceId: string,
  sessionId: string,
  events: Event[],
): Promise<Event[]> {
  const missingIds = new Set(events.map((event) => event.id));
  for await (const event of store.read(workspaceId, sessionId, {
    fromCursor: '',
    maxEvents: 0,
    subpath: '*',
  })) {
    missingIds.delete(event.id);
    if (missingIds.size === 0) break;
  }
  return events.filter((event) => missingIds.has(event.id));
}

function deserializeInitialEvents(row: {
  workspaceId: string;
  sessionId: string;
  events: unknown;
}): Event[] {
  if (!Array.isArray(row.events) || row.events.length === 0) {
    throw new Error('initial session events outbox row has no events');
  }
  return row.events.map((value) => {
    const event = value as Partial<SerializedSessionEvent>;
    if (
      typeof event.id !== 'string' ||
      typeof event.subpath !== 'string' ||
      typeof event.seq !== 'number' ||
      typeof event.producedAt !== 'string' ||
      typeof event.producedBy !== 'string' ||
      typeof event.kind !== 'string' ||
      typeof event.payloadBase64 !== 'string' ||
      typeof event.idempotencyKey !== 'string' ||
      (event.userId !== undefined && typeof event.userId !== 'string')
    ) {
      throw new Error('initial session events outbox row contains an invalid event');
    }
    const userId = nonEmptyUserId(event.userId);
    return {
      id: event.id,
      workspaceId: row.workspaceId,
      sessionId: row.sessionId,
      subpath: event.subpath,
      seq: event.seq,
      producedAt: event.producedAt,
      producedBy: event.producedBy,
      kind: event.kind,
      payload: Buffer.from(event.payloadBase64, 'base64'),
      idempotencyKey: event.idempotencyKey,
      ...(userId !== undefined ? { userId } : {}),
    };
  });
}

function nonEmptyUserId(userId: string | undefined): string | undefined {
  return typeof userId === 'string' && userId.length > 0 ? userId : undefined;
}
