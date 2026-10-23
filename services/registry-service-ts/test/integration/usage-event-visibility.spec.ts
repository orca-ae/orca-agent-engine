// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { and, eq, inArray } from 'drizzle-orm';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  sessionEventsIndex,
  sessionUsageEvents,
  sessions,
} from '../../src/persistence/postgres/schema.js';
import {
  backfillUsageEventVisibility,
  listSessionEventsFromIndex,
} from '../../src/events/session-events-index.js';
import { runUsageVisibilityBackfill } from '../../src/backfill-usage-visibility.js';
import { httpEventToProto } from '../../src/domain/events.js';
import {
  buildTestPostgresStore,
  closeTestDb,
  deleteTranscriptRowsForWorkspace,
  getTestDb,
} from './setup.js';
import { createTestWorkspace, uniqueWorkspace } from './fixtures.js';

describe('runtime usage visibility (PostgreSQL)', () => {
  let db: DbClient;
  let transcript: Awaited<ReturnType<typeof buildTestPostgresStore>>;
  const workspaceId = uniqueWorkspace('usage_visibility');
  const nextWorkspaceId = `${workspaceId}_z`;
  const sessionId = 'ses_usage_visibility';
  beforeAll(async () => {
    ({ db } = await getTestDb());
    await createTestWorkspace(db, workspaceId);
    await createTestWorkspace(db, nextWorkspaceId);
    transcript = await buildTestPostgresStore();
    await transcript.store.append(workspaceId, sessionId, [
      httpEventToProto({
        workspaceId,
        sessionId,
        producedBy: 'harness',
        idempotencyKey: 'historical_usage',
        input: { id: 'evt_raw_usage', type: 'agent.usage', usage: { input_tokens: 17 } },
      }),
    ]);
    for (let seq = 1; seq <= 7; seq++)
      await insert(`evt_page_${seq}`, seq, seq % 2 ? 'agent.usage' : 'agent.message');
    await insert('evt_thread_usage', 8, 'agent.usage', 'threads/sth_worker');
  });
  afterAll(async () => {
    if (db)
      await db
        .delete(sessionEventsIndex)
        .where(inArray(sessionEventsIndex.workspaceId, [workspaceId, nextWorkspaceId]));
    if (transcript) {
      await deleteTranscriptRowsForWorkspace(transcript.pool, workspaceId);
      await transcript.store.close();
    }
    await closeTestDb();
  });

  async function insert(eventId: string, seq: number, kind: string, subpath = '') {
    await db.insert(sessionEventsIndex).values({
      workspaceId,
      sessionId,
      eventId,
      seq,
      kind,
      subpath,
      visibility: 'public',
      producedBy: 'harness',
      producedAt: '2026-10-20T00:00:00.000Z',
      payload: { type: kind, usage: { input_tokens: 17 } },
    });
  }

  it('excludes old public rows before ascending/descending limits, type filters and cursors', async () => {
    for (const order of ['asc', 'desc'] as const) {
      const ids: string[] = [];
      let fromCursor = '';
      do {
        const page = await listSessionEventsFromIndex(db, {
          workspaceId,
          sessionId,
          fromCursor,
          limit: 2,
          subpath: '*',
          order,
        });
        expect(page.events.length).toBe(page.nextCursor ? 2 : 1);
        ids.push(...page.events.map((event) => event.id));
        fromCursor = page.nextCursor ?? '';
      } while (fromCursor);
      expect(ids).toEqual(
        order === 'asc'
          ? ['evt_page_2', 'evt_page_4', 'evt_page_6']
          : ['evt_page_6', 'evt_page_4', 'evt_page_2'],
      );
      // A cursor issued by an old writer can itself point at a now-hidden row.
      const resumed = await listSessionEventsFromIndex(db, {
        workspaceId,
        sessionId,
        fromCursor: '3:0:evt_page_3',
        limit: 2,
        subpath: '*',
        order,
        types: ['agent.message', 'agent.usage'],
      });
      expect(resumed.events.map((event) => event.id)).toEqual(
        order === 'asc' ? ['evt_page_4', 'evt_page_6'] : ['evt_page_2'],
      );
      expect(resumed.nextCursor).toBeNull();
    }
    for (const subpath of ['', '*', 'threads/sth_worker']) {
      const page = await listSessionEventsFromIndex(db, {
        workspaceId,
        sessionId,
        fromCursor: '',
        limit: 1,
        subpath,
        types: ['agent.usage'],
      });
      expect(page).toEqual({ events: [], nextCursor: null });
    }
  });

  it('resumes bounded batches, verifies the final pass, and repairs a drained old writer without accounting replay', async () => {
    const before = await db
      .select()
      .from(sessionEventsIndex)
      .where(eq(sessionEventsIndex.workspaceId, workspaceId));
    const usageBefore = await db
      .select()
      .from(sessionUsageEvents)
      .where(eq(sessionUsageEvents.workspaceId, workspaceId));
    const sessionsBefore = await db
      .select()
      .from(sessions)
      .where(eq(sessions.workspaceId, workspaceId));
    const rawBefore = await transcript.pool.query(
      'SELECT * FROM transcript_events WHERE workspace_id = $1',
      [workspaceId],
    );
    await expect(runUsageVisibilityBackfill(db, { verifyOnly: true })).rejects.toThrow(
      'verification failed',
    );
    expect((await backfillUsageEventVisibility(db, 2)).changed).toBe(2);
    // Simulate maintenance stopping after a committed batch, then restarting.
    expect(await runUsageVisibilityBackfill(db, { batchSize: 2 })).toBe(3);
    expect(await runUsageVisibilityBackfill(db, { batchSize: 2 })).toBe(0);
    expect(await runUsageVisibilityBackfill(db, { verifyOnly: true })).toBe(0);
    const after = await db
      .select()
      .from(sessionEventsIndex)
      .where(eq(sessionEventsIndex.workspaceId, workspaceId));
    for (const original of before) {
      expect(after.find((row) => row.eventId === original.eventId)).toEqual({
        ...original,
        visibility: original.kind === 'agent.usage' ? 'internal' : original.visibility,
      });
    }
    // An old writer can still reintroduce a public index row during rollout.
    await insert('evt_old_writer', 9, 'agent.usage');
    await expect(runUsageVisibilityBackfill(db, { verifyOnly: true })).rejects.toThrow(
      'verification failed',
    );
    expect(
      (
        await listSessionEventsFromIndex(db, {
          workspaceId,
          sessionId,
          fromCursor: '',
          limit: 10,
          subpath: '*',
        })
      ).events.map((event) => event.type),
    ).not.toContain('agent.usage');
    expect(await runUsageVisibilityBackfill(db, { batchSize: 1 })).toBe(1);
    expect(
      await db
        .select()
        .from(sessionUsageEvents)
        .where(eq(sessionUsageEvents.workspaceId, workspaceId)),
    ).toEqual(usageBefore);
    expect(await db.select().from(sessions).where(eq(sessions.workspaceId, workspaceId))).toEqual(
      sessionsBefore,
    );
    expect(
      (
        await transcript.pool.query('SELECT * FROM transcript_events WHERE workspace_id = $1', [
          workspaceId,
        ])
      ).rows,
    ).toEqual(rawBefore.rows);
    expect(rawBefore.rows).toHaveLength(1);
    expect(
      await db
        .select()
        .from(sessionEventsIndex)
        .where(
          and(
            eq(sessionEventsIndex.workspaceId, workspaceId),
            eq(sessionEventsIndex.kind, 'agent.usage'),
          ),
        ),
    ).toHaveLength(6);
  });

  it.each([false, true])(
    'verifies skipped earlier rows after a partial batch (released during scan=%s)',
    async (releaseDuringScan) => {
      const lockedId = `evt_locked_${releaseDuringScan}`;
      await insert(lockedId, 10, 'agent.usage');
      await insert(`${lockedId}_z1`, 11, 'agent.usage');
      await insert(`${lockedId}_z2`, 12, 'agent.usage');
      let release!: () => void;
      let acquired!: () => void;
      const ready = new Promise<void>((resolve) => {
        acquired = resolve;
      });
      const hold = db.transaction(async (tx) => {
        await tx
          .select()
          .from(sessionEventsIndex)
          .where(
            and(
              eq(sessionEventsIndex.workspaceId, workspaceId),
              eq(sessionEventsIndex.eventId, lockedId),
            ),
          )
          .for('update');
        acquired();
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      });
      await ready;
      let batches = 0;
      const progressingDb = new Proxy(db, {
        get(target, key) {
          if (key !== 'transaction') return Reflect.get(target, key);
          return async (run: Parameters<DbClient['transaction']>[0]) => {
            const result = await target.transaction(run);
            if (++batches === 1 && releaseDuringScan) {
              release();
              await hold;
            }
            return result;
          };
        },
      });
      try {
        // The first batch is partial because its earlier row is locked. Once
        // released, that row remains behind this invocation's scan position.
        await expect(runUsageVisibilityBackfill(progressingDb, { batchSize: 3 })).rejects.toThrow(
          'verification failed',
        );
        expect(batches).toBe(2);
      } finally {
        release();
        await hold;
      }
      expect(await runUsageVisibilityBackfill(db)).toBe(1);
    },
  );

  it('advances across all composite key components and leaves earlier arrivals for verified restart', async () => {
    const keys = [
      { workspaceId, sessionId: 'ses_keyset_a', eventId: 'evt_z' },
      { workspaceId, sessionId: 'ses_keyset_b', eventId: 'evt_a' },
      { workspaceId, sessionId: 'ses_keyset_b', eventId: 'evt_z' },
      { workspaceId: nextWorkspaceId, sessionId: 'ses_keyset_a', eventId: 'evt_a' },
    ];
    await db.insert(sessionEventsIndex).values(
      keys.map((key, seq) => ({
        ...key,
        seq,
        kind: 'agent.usage',
        visibility: 'public',
        producedBy: 'harness',
        producedAt: '2026-10-20T00:00:00.000Z',
      })),
    );
    const first = await backfillUsageEventVisibility(db, 2);
    expect(first.changed).toBe(2);
    expect(first.nextCursor).toEqual(keys[1]);
    await db.insert(sessionEventsIndex).values({
      ...keys[0]!,
      eventId: 'evt_earlier_arrival',
      seq: 4,
      kind: 'agent.usage',
      visibility: 'public',
      producedBy: 'harness',
      producedAt: '2026-10-20T00:00:00.000Z',
    });
    const second = await backfillUsageEventVisibility(db, 2, first.nextCursor!);
    expect(second.changed).toBe(2);
    expect(second.nextCursor).toEqual(keys[3]);
    expect(await backfillUsageEventVisibility(db, 2, second.nextCursor!)).toEqual({
      changed: 0,
      nextCursor: null,
    });
    await expect(runUsageVisibilityBackfill(db, { verifyOnly: true })).rejects.toThrow(
      'verification failed',
    );
    expect(await runUsageVisibilityBackfill(db, { batchSize: 1 })).toBe(1);
    expect(await runUsageVisibilityBackfill(db, { verifyOnly: true })).toBe(0);
  });

  it('bounds the batch size before opening a transaction', async () => {
    for (const size of [0, -1, 1.5, 1_001])
      await expect(backfillUsageEventVisibility(db, size)).rejects.toThrow('batch size');
  });
});
