// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import { Pool } from 'pg';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { InMemoryMemoryBlobStore } from '../../src/blob/in-memory.js';
import { LocalMemoryStore } from '../../src/local-memory-store.js';
import { applyMigrations, PostgresMemoryMetadataStore } from '../../src/metadata/postgres.js';

const pool = new Pool({
  connectionString:
    process.env['MEMORYSTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/memorystore',
});
beforeAll(() => applyMigrations(pool));
afterAll(() => pool.end());

it('retains the entire store graph and bytes, hides children, and rejects later writes', async () => {
  const blobs = new InMemoryMemoryBlobStore();
  const metadata = new PostgresMemoryMetadataStore(pool);
  const store = new LocalMemoryStore({ blobStore: blobs, metadataStore: metadata });
  const workspaceId = `ws_${randomUUID()}`;
  const parent = await store.createStore({ workspaceId, name: 'retained' });
  const content = Buffer.from('retained');
  const sha256 = createHash('sha256').update(content).digest('hex');
  const input = {
    workspaceId,
    storeId: parent.id,
    path: 'a.md',
    sha256,
    sizeBytes: content.length,
  };
  const { memory, version } = await store.writeMemory({
    ...input,
    content: Readable.from(content),
  });
  await store.deleteStore(`foreign_${workspaceId}`, parent.id);
  expect(await store.getStore(workspaceId, parent.id)).not.toBeNull();
  await store.deleteStore(workspaceId, parent.id);
  expect(await store.getStore(workspaceId, parent.id)).toBeNull();
  expect((await store.listStores(workspaceId, { includeArchived: true })).items).toEqual([]);
  expect(await metadata.listMemories(workspaceId, parent.id)).toEqual([]);
  expect(await metadata.getMemory(workspaceId, parent.id, memory.id)).toBeNull();
  expect(await metadata.getVersion(workspaceId, parent.id, version.id)).toBeNull();
  expect(await metadata.listAllVersions(workspaceId, parent.id)).toEqual([]);
  expect(await blobs.listLive(workspaceId, parent.id)).toEqual(['a.md']);
  for (const [table, id] of [
    ['memory_stores', parent.id],
    ['memories', memory.id],
    ['memory_versions', version.id],
  ]) {
    const { rows } = await pool.query(`SELECT deleted_at FROM ${table} WHERE id = $1`, [id]);
    expect(rows).toEqual([{ deleted_at: expect.any(Date) }]);
  }
  await expect(
    metadata.upsertMemory({ ...input, id: 'mem_rejected', versionId: 'memver_rejected' }),
  ).rejects.toThrow('not found');
  expect(await store.updateStore({ workspaceId, storeId: parent.id, name: 'revived' })).toBeNull();
});

it('serializes store deletion with child deletion without leaving a live audit version', async () => {
  const workspaceId = `ws_${randomUUID()}`;
  const metadata = new PostgresMemoryMetadataStore(pool);
  const store = new LocalMemoryStore({
    blobStore: new InMemoryMemoryBlobStore(),
    metadataStore: metadata,
  });
  const parent = await store.createStore({ workspaceId, name: 'concurrent' });
  const content = Buffer.from('concurrent');
  const { memory } = await store.writeMemory({
    workspaceId,
    storeId: parent.id,
    path: 'a.md',
    content: Readable.from(content),
    sizeBytes: content.length,
    sha256: createHash('sha256').update(content).digest('hex'),
  });
  await Promise.all([
    metadata.deleteMemory({
      workspaceId,
      storeId: parent.id,
      memoryId: memory.id,
      versionId: `memver_${randomUUID()}`,
    }),
    metadata.deleteStore(workspaceId, parent.id),
  ]);
  expect(await metadata.getStore(workspaceId, parent.id)).toBeNull();
  expect(await metadata.listAllVersions(workspaceId, parent.id)).toEqual([]);
  const { rows } = await pool.query('SELECT deleted_at FROM memory_versions WHERE store_id = $1', [
    parent.id,
  ]);
  expect(rows.length).toBeGreaterThan(0);
  expect(rows.every((row) => row.deleted_at !== null)).toBe(true);
});
