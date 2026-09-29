// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { InMemoryBlobStore } from '../../src/blob/in-memory.js';
import { LocalFileStore } from '../../src/file-store.js';
import { applyMigrations } from '../../src/metadata/postgres.js';
import type { CreateFileInput } from '../../src/types.js';

const databaseUrl =
  process.env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore';

describe('Postgres output replay identity', () => {
  let pool: Pool;
  beforeAll(async () => {
    pool = new Pool({ connectionString: databaseUrl, max: 4 });
    await applyMigrations(pool);
  });
  afterAll(async () => {
    await pool.end();
  });

  it('arbitrates concurrent retries through the primary key and survives reconstruction', async () => {
    const workspaceId = `ws_${randomUUID()}`;
    const id = `file_${randomUUID()}`;
    const blobStore = new InMemoryBlobStore();
    const input = (content = 'answer'): CreateFileInput => ({
      id,
      workspaceId,
      scopeId: 'ses_output',
      purpose: 'agent_output',
      filename: 'answer.txt',
      mimeType: 'text/plain',
      content: Readable.from(content),
    });
    const stores = [
      new LocalFileStore({ pool, blobStore }),
      new LocalFileStore({ pool, blobStore }),
    ];
    const records = await Promise.all(stores.map((store) => store.create(input())));
    expect(records[0]).toEqual(records[1]);
    const rebuilt = new LocalFileStore({ pool, blobStore });
    expect(await rebuilt.create(input())).toEqual(records[0]);
    await expect(rebuilt.create(input('changed'))).rejects.toThrow('identity conflicts');
    expect((await rebuilt.list(workspaceId)).items).toHaveLength(1);
    await rebuilt.delete(workspaceId, id);
    await expect(rebuilt.create(input())).rejects.toThrow();
    expect((await rebuilt.list(workspaceId)).items).toEqual([]);
  });
});
