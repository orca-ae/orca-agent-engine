// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import { Pool } from 'pg';
import { afterAll, beforeAll, expect, it, vi } from 'vitest';
import { InMemoryBlobStore } from '../../src/blob/in-memory.js';
import { LocalFileStore } from '../../src/file-store.js';
import { applyMigrations, PostgresFileMetadataStore } from '../../src/metadata/postgres.js';

const pool = new Pool({
  connectionString:
    process.env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore',
});
beforeAll(() => applyMigrations(pool));
afterAll(() => pool.end());

it('retains deleted metadata and bytes, hides reads, and releases the active digest', async () => {
  const blobs = new InMemoryBlobStore();
  const store = new LocalFileStore({ pool, blobStore: blobs });
  const workspaceId = `ws_${randomUUID()}`;
  const create = () =>
    store.create({
      workspaceId,
      filename: 'retained.txt',
      mimeType: 'text/plain',
      content: Readable.from('retained'),
    });
  const file = await create();
  await store.delete(workspaceId, file.id);
  expect(await store.get(workspaceId, file.id)).toBeNull();
  expect(await store.open(workspaceId, file.id)).toBeNull();
  expect((await store.list(workspaceId)).items).toEqual([]);
  const { rows } = await pool.query('SELECT deleted_at, sha256 FROM files WHERE id = $1', [
    file.id,
  ]);
  expect(rows[0]).toMatchObject({ deleted_at: expect.any(Date), sha256: file.sha256 });
  expect(blobs.size()).toBe(1);
  const replacement = await create();
  expect(replacement.id).not.toBe(file.id);
  expect((await store.list(workspaceId)).items.map((row) => row.id)).toEqual([replacement.id]);
  await expect(store.delete(workspaceId, file.id)).rejects.toThrow();
  expect(
    (await pool.query('SELECT deleted_at FROM files WHERE id = $1', [file.id])).rows[0].deleted_at,
  ).toEqual(rows[0].deleted_at);
  await store.delete(`foreign_${workspaceId}`, replacement.id).catch(() => {});
  expect(await store.get(workspaceId, replacement.id)).not.toBeNull();
});

function signal() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it.each([false, true])(
  'preserves history and active digest uniqueness when delete wins before lookup=%s',
  async (deleteBeforeLookup) => {
    const metadata = new PostgresFileMetadataStore(pool);
    const blobs = new InMemoryBlobStore();
    const store = new LocalFileStore({ metadataStore: metadata, blobStore: blobs });
    const workspaceId = `ws_${randomUUID()}`;
    const create = () =>
      store.create({
        workspaceId,
        filename: 'racing.txt',
        mimeType: 'text/plain',
        content: Readable.from('retained during delete'),
      });
    const original = await create();
    const lookupStarted = signal();
    const deletionFinished = signal();
    const findBySha = metadata.findBySha.bind(metadata);
    // Only control interleaving: both lookup and deletion use real PostgreSQL.
    // Cover create linearizing on either side of the committed tombstone.
    const lookup = vi.spyOn(metadata, 'findBySha').mockImplementationOnce(async (...args) => {
      const before = deleteBeforeLookup ? null : await findBySha(...args);
      lookupStarted.resolve();
      await deletionFinished.promise;
      return deleteBeforeLookup ? findBySha(...args) : before;
    });
    try {
      const [racing] = await Promise.all([
        create(),
        (async () => {
          await lookupStarted.promise;
          try {
            await store.delete(workspaceId, original.id);
          } finally {
            deletionFinished.resolve();
          }
        })(),
      ]);
      if (deleteBeforeLookup) expect(racing.id).not.toBe(original.id);
      else expect(racing.id).toBe(original.id);
      expect(await store.get(workspaceId, original.id)).toBeNull();

      const replacement = await create();
      expect(replacement.id).not.toBe(original.id);
      if (deleteBeforeLookup) expect(replacement.id).toBe(racing.id);
      expect((await store.list(workspaceId)).items.map((row) => row.id)).toEqual([replacement.id]);
      const { rows } = await pool.query(
        'SELECT id, deleted_at FROM files WHERE workspace_id = $1 AND sha256 = $2',
        [workspaceId, original.sha256],
      );
      expect(rows).toHaveLength(2);
      expect(rows.find((row) => row.id === original.id)?.deleted_at).toBeInstanceOf(Date);
      expect(rows.find((row) => row.id === replacement.id)?.deleted_at).toBeNull();
      const opened = await store.open(workspaceId, replacement.id);
      expect(opened).not.toBeNull();
      const chunks: Buffer[] = [];
      for await (const chunk of opened!.stream) chunks.push(Buffer.from(chunk));
      expect(Buffer.concat(chunks).toString()).toBe('retained during delete');
      expect(blobs.size()).toBe(1);
    } finally {
      deletionFinished.resolve();
      lookup.mockRestore();
    }
  },
);

it('retains one tombstone and its bytes when two deletes overlap', async () => {
  const metadata = new PostgresFileMetadataStore(pool);
  const blobs = new InMemoryBlobStore();
  const store = new LocalFileStore({ metadataStore: metadata, blobStore: blobs });
  const workspaceId = `ws_${randomUUID()}`;
  const file = await store.create({
    workspaceId,
    filename: 'double-delete.txt',
    mimeType: 'text/plain',
    content: Readable.from('retained'),
  });
  const bothLookedUp = signal();
  const findById = metadata.findById.bind(metadata);
  let lookups = 0;
  const lookup = vi.spyOn(metadata, 'findById').mockImplementation(async (...args) => {
    const row = await findById(...args);
    if (++lookups === 2) bothLookedUp.resolve();
    await bothLookedUp.promise;
    return row;
  });
  try {
    await Promise.all([store.delete(workspaceId, file.id), store.delete(workspaceId, file.id)]);
  } finally {
    bothLookedUp.resolve();
    lookup.mockRestore();
  }
  const read = () => pool.query('SELECT deleted_at FROM files WHERE id = $1', [file.id]);
  const { rows } = await read();
  expect(rows).toEqual([{ deleted_at: expect.any(Date) }]);
  await expect(store.delete(workspaceId, file.id)).rejects.toThrow();
  expect((await read()).rows).toEqual(rows);
  expect(await store.get(workspaceId, file.id)).toBeNull();
  expect((await store.list(workspaceId)).items).toEqual([]);
  expect(blobs.size()).toBe(1);
});
