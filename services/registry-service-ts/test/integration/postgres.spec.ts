// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, afterAll } from 'vitest';
import { sql } from 'drizzle-orm';
import { stableSessionThreadId } from '../../src/domain/thread-projection.js';
import { getTestDb, closeTestDb } from './setup.js';

describe('Postgres connectivity (integration)', () => {
  afterAll(async () => {
    await closeTestDb();
  });

  it('connects, runs migrations, and queries SELECT 1', async () => {
    const { db } = await getTestDb();
    const result = await db.execute(sql`select 1 as one`);
    expect(result.rows[0]?.['one']).toBe(1);
  });

  it('the agents table exists after migration', async () => {
    const { db } = await getTestDb();
    const result = await db.execute(
      sql`select 1 from information_schema.tables where table_name = 'agents'`,
    );
    expect(result.rowCount).toBe(1);
  });

  it('migration UUIDv5 produces the runtime-stable primary thread id', async () => {
    const { db } = await getTestDb();
    const workspaceId = 'ws_migration_thread_id';
    const sessionId = 'ses_migration_thread_id';
    const result = await db.execute(sql`
      select 'sth_' || uuid_generate_v5(
        'ea82f5c2-b759-4717-9727-51b3ef079bef'::uuid,
        ${`${workspaceId}:${sessionId}:`}
      )::text as id
    `);

    expect(result.rows[0]?.['id']).toBe(stableSessionThreadId(workspaceId, sessionId, ''));
  });
});
