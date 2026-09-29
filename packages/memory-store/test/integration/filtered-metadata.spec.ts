// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { Pool } from 'pg';
import { applyMigrations, PostgresMemoryMetadataStore } from '../../src/metadata/postgres.js';

describe('page-scoped Memory metadata reads (Postgres only)', () => {
  let pool: Pool;
  let store: PostgresMemoryMetadataStore;
  const workspaceId = `ws_${randomUUID()}`;
  const storeId = `mems_${randomUUID()}`;
  const target = `mem_${randomUUID()}`;
  beforeAll(async () => {
    pool = new Pool({
      connectionString:
        process.env['MEMORYSTORE_DATABASE_URL'] ??
        'postgres://orca:orca@localhost:5432/memorystore',
      max: 2,
    });
    await applyMigrations(pool);
    store = new PostgresMemoryMetadataStore(pool);
    await store.insertStore({
      id: storeId,
      workspaceId,
      name: 'filtered metadata',
      description: null,
      archivedAt: null,
    });
    for (let i = 0; i < 40; i++) {
      await store.upsertMemory({
        id: i < 3 ? target : `mem_${randomUUID()}`,
        versionId: `memver_${randomUUID()}`,
        workspaceId,
        storeId,
        path: i < 3 ? 'target.txt' : `other-${i}.txt`,
        sha256: 'a'.repeat(64),
        sizeBytes: 1,
      });
    }
  });
  afterAll(async () => {
    await pool?.end();
  });

  it('returns exactly the target history while avoiding unrelated version rows', async () => {
    const all = await store.listAllVersions(workspaceId, storeId);
    const filtered = await store.listAllVersions(workspaceId, storeId, {
      memoryIds: [target, target],
    });
    expect(all).toHaveLength(40);
    expect(filtered).toEqual(all.filter((version) => version.memoryId === target));
    expect(filtered).toHaveLength(3);
    expect(await store.listMemories(workspaceId, storeId, { memoryIds: [target] })).toHaveLength(1);
    expect(await store.listAllVersions('ws_other', storeId, { memoryIds: [target] })).toEqual([]);
    expect(await store.listMemories('ws_other', storeId, { memoryIds: [target] })).toEqual([]);
    expect(await store.listAllVersions(workspaceId, 'mems_other', { memoryIds: [target] })).toEqual(
      [],
    );
  });

  it('short-circuits empty ID sets and retains deletion history', async () => {
    const query = vi.spyOn(pool, 'query');
    expect(await store.listAllVersions(workspaceId, storeId, { memoryIds: [] })).toEqual([]);
    expect(await store.listMemories(workspaceId, storeId, { memoryIds: [] })).toEqual([]);
    expect(query).not.toHaveBeenCalled();
    query.mockRestore();
    await store.deleteMemory({
      workspaceId,
      storeId,
      memoryId: target,
      versionId: `memver_${randomUUID()}`,
    });
    expect(await store.listMemories(workspaceId, storeId, { memoryIds: [target] })).toEqual([]);
    expect(await store.listAllVersions(workspaceId, storeId, { memoryIds: [target] })).toHaveLength(
      4,
    );
  });
});
