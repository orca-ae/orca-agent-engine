// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { randomBytes } from 'node:crypto';
import { Readable } from 'node:stream';
import { Pool } from 'pg';
import { InMemoryBlobStore } from '../../src/blob/in-memory.js';
import { LocalFileStore } from '../../src/file-store.js';
import { applyMigrations } from '../../src/metadata/postgres.js';

const DATABASE_URL =
  process.env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore';

describe('LocalFileStore archived-file dedup (integration)', () => {
  let pool: Pool;
  let store: LocalFileStore;

  beforeAll(async () => {
    pool = new Pool({ connectionString: DATABASE_URL, max: 2 });
    await applyMigrations(pool);
    store = new LocalFileStore({ pool, blobStore: new InMemoryBlobStore() });
  });

  afterAll(async () => {
    await pool.end();
  });

  it('creates a fresh active file when archived agent content is uploaded again', async () => {
    const workspaceId = `ws_reupload_${Date.now()}_${randomBytes(3).toString('hex')}`;
    const payload = Buffer.from(`reupload-${randomBytes(8).toString('hex')}`);
    const archived = await store.create({
      workspaceId,
      filename: 'archived.txt',
      mimeType: 'text/plain',
      content: Readable.from(payload),
    });
    await store.archive(workspaceId, archived.id);

    const reuploaded = await store.create({
      workspaceId,
      filename: 'active.txt',
      mimeType: 'text/plain',
      content: Readable.from(payload),
    });

    expect(reuploaded.id).not.toBe(archived.id);
    expect(reuploaded.archivedAt).toBeNull();
    expect(reuploaded.sha256).toBe(archived.sha256);
    const page = await store.list(workspaceId);
    expect(page.items.map((file) => file.id)).toContain(reuploaded.id);
    expect(page.items.map((file) => file.id)).not.toContain(archived.id);
  });
});
