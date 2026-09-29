// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { S3Client, CreateBucketCommand, HeadBucketCommand } from '@aws-sdk/client-s3';
import { Readable } from 'node:stream';
import { createHash, randomBytes } from 'node:crypto';
import { S3BlobStore } from '../../src/blob/s3.js';
import { LocalFileStore } from '../../src/file-store.js';
import { applyMigrations } from '../../src/metadata/postgres.js';

const DATABASE_URL =
  process.env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore';
const ENDPOINT = process.env['S3_ENDPOINT'] ?? 'http://localhost:9000';
const ACCESS_KEY = process.env['S3_ACCESS_KEY'] ?? 'minioadmin';
const SECRET_KEY = process.env['S3_SECRET_KEY'] ?? 'minioadmin';
const BUCKET = process.env['S3_BUCKET'] ?? 'orca-files';
const REGION = process.env['S3_REGION'] ?? 'us-east-1';

async function readAll(s: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of s) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as Uint8Array));
  }
  return Buffer.concat(chunks);
}

describe('LocalFileStore (integration)', () => {
  let pool: Pool;
  let store: LocalFileStore;
  let s3Client: S3Client;

  beforeAll(async () => {
    pool = new Pool({ connectionString: DATABASE_URL, max: 5 });
    await applyMigrations(pool);

    s3Client = new S3Client({
      endpoint: ENDPOINT,
      region: REGION,
      credentials: { accessKeyId: ACCESS_KEY, secretAccessKey: SECRET_KEY },
      forcePathStyle: true,
    });
    try {
      await s3Client.send(new HeadBucketCommand({ Bucket: BUCKET }));
    } catch {
      try {
        await s3Client.send(new CreateBucketCommand({ Bucket: BUCKET }));
      } catch {
        /* race; ignore */
      }
    }

    store = new LocalFileStore({
      pool,
      blobStore: new S3BlobStore({ client: s3Client, bucket: BUCKET, keyPrefix: 'test/files/' }),
    });
  });

  afterAll(async () => {
    s3Client.destroy();
    await pool.end();
  });

  it('round-trips a small file', async () => {
    const ws = `ws_${Date.now()}_${randomBytes(3).toString('hex')}`;
    const payload = Buffer.from('hello phase 5');
    const created = await store.create({
      workspaceId: ws,
      filename: 'hello.txt',
      mimeType: 'text/plain',
      content: Readable.from(payload),
      expectedSizeBytes: payload.length,
    });
    expect(created.id).toMatch(/^file_/);
    expect(created.sha256).toBe(createHash('sha256').update(payload).digest('hex'));
    expect(created.sizeBytes).toBe(payload.length);

    const fetched = await store.get(ws, created.id);
    expect(fetched?.id).toBe(created.id);

    const opened = await store.open(ws, created.id);
    expect(opened).not.toBeNull();
    const back = await readAll(opened!.stream);
    expect(back).toEqual(payload);
  });

  it('dedups by sha256: same content twice in one workspace returns the same id', async () => {
    const ws = `ws_${Date.now()}_${randomBytes(3).toString('hex')}`;
    const payload = randomBytes(512);
    const a = await store.create({
      workspaceId: ws,
      filename: 'a.bin',
      mimeType: 'application/octet-stream',
      content: Readable.from(payload),
    });
    const b = await store.create({
      workspaceId: ws,
      filename: 'b.bin',
      mimeType: 'application/octet-stream',
      content: Readable.from(payload),
    });
    expect(b.id).toBe(a.id);
    expect(b.filename).toBe('a.bin'); // dedup returns the original record verbatim
  });

  it('rejects content with mismatched expectedSizeBytes', async () => {
    const ws = `ws_${Date.now()}_${randomBytes(3).toString('hex')}`;
    const payload = Buffer.from('exactly twelve');
    await expect(
      store.create({
        workspaceId: ws,
        filename: 'x.txt',
        mimeType: 'text/plain',
        content: Readable.from(payload),
        expectedSizeBytes: 999,
      }),
    ).rejects.toThrow(/size mismatch/i);
  });

  it('list returns archived=null records, paginated', async () => {
    const ws = `ws_${Date.now()}_${randomBytes(3).toString('hex')}`;
    for (let i = 0; i < 3; i++) {
      await store.create({
        workspaceId: ws,
        filename: `f${i}.txt`,
        mimeType: 'text/plain',
        content: Readable.from(Buffer.from(`payload-${i}-${randomBytes(8).toString('hex')}`)),
      });
    }
    const page = await store.list(ws);
    expect(page.items.length).toBe(3);
    expect(page.nextCursor).toBeNull();
  });

  it('archive marks the record; get still returns it (raw); list excludes it', async () => {
    const ws = `ws_${Date.now()}_${randomBytes(3).toString('hex')}`;
    const created = await store.create({
      workspaceId: ws,
      filename: 'gone.txt',
      mimeType: 'text/plain',
      content: Readable.from(Buffer.from(`gone-${randomBytes(4).toString('hex')}`)),
    });
    await store.archive(ws, created.id);
    const after = await store.get(ws, created.id);
    expect(after?.archivedAt).not.toBeNull();
    const page = await store.list(ws);
    expect(page.items.find((f) => f.id === created.id)).toBeUndefined();
  });
});

describe('applyMigrations (integration)', () => {
  it('serializes concurrent callers without deadlocking or leaking the advisory lock', async () => {
    // registry and harness both migrate this DB at boot. Fire several
    // applyMigrations concurrently on independent pools: the advisory lock must
    // serialize them (no duplicate-DDL / duplicate-journal errors), each caller
    // must release its lock, and a follow-up call must still acquire it.
    const pools = Array.from(
      { length: 4 },
      () => new Pool({ connectionString: DATABASE_URL, max: 1 }),
    );
    try {
      await Promise.all(pools.map((p) => applyMigrations(p)));

      // A follow-up call must still acquire the lock: it would block until the
      // test timed out if any prior caller had failed to release it. Reaching
      // the assertion proves the lock was released and migrations are recorded.
      const probe = new Pool({ connectionString: DATABASE_URL, max: 1 });
      try {
        await applyMigrations(probe);
        const { rows } = await probe.query(
          'SELECT count(*)::int AS applied FROM drizzle.__drizzle_migrations',
        );
        expect(rows[0].applied).toBeGreaterThan(0);
      } finally {
        await probe.end();
      }
    } finally {
      await Promise.all(pools.map((p) => p.end()));
    }
  });
});
