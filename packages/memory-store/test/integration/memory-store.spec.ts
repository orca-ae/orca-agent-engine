// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomBytes } from 'node:crypto';
import { Readable } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { CreateBucketCommand, HeadBucketCommand, S3Client } from '@aws-sdk/client-s3';
import { Pool } from 'pg';
import { S3MemoryBlobStore } from '../../src/blob/s3.js';
import { LocalMemoryStore } from '../../src/local-memory-store.js';
import { applyMigrations } from '../../src/metadata/postgres.js';
import { PostgresMemoryMetadataStore } from '../../src/metadata/postgres.js';
import { MemoryConflictError } from '../../src/store.js';

const ADMIN_DATABASE_URL =
  process.env['MEMORYSTORE_ADMIN_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/postgres';
const DATABASE_URL =
  process.env['MEMORYSTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/memorystore';
const ENDPOINT = process.env['S3_ENDPOINT'] ?? 'http://localhost:9000';
const ACCESS_KEY = process.env['S3_ACCESS_KEY'] ?? 'minioadmin';
const SECRET_KEY = process.env['S3_SECRET_KEY'] ?? 'minioadmin';
const BUCKET = process.env['S3_BUCKET'] ?? 'orca-files';
const REGION = process.env['S3_REGION'] ?? 'us-east-1';

async function ensureDatabase(): Promise<void> {
  // Connect to the admin DB (the default `postgres` database) and create
  // `memorystore` if it doesn't exist. Mirrors the file-store integration
  // setup so local dev doesn't need a manual bootstrap step.
  const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
  try {
    const dbName = new URL(DATABASE_URL).pathname.replace(/^\//, '');
    if (!dbName) throw new Error(`MEMORYSTORE_DATABASE_URL has no path: ${DATABASE_URL}`);
    const exists = await adminPool.query('SELECT 1 FROM pg_database WHERE datname = $1', [dbName]);
    if (exists.rowCount === 0) {
      // CREATE DATABASE can't run in a transaction or with a parameter
      // placeholder; concatenate the (validated) name.
      if (!/^[a-z_][a-z0-9_]*$/i.test(dbName)) {
        throw new Error(`refusing to create db with non-identifier name: ${dbName}`);
      }
      await adminPool.query(`CREATE DATABASE "${dbName}"`);
    }
  } finally {
    await adminPool.end().catch(() => {});
  }
}

async function readAll(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const c of stream) {
    chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c as unknown as Uint8Array));
  }
  return Buffer.concat(chunks);
}

function digest(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

function uniqueWorkspaceId(): string {
  return `ws_${Date.now()}_${randomBytes(4).toString('hex')}`;
}

describe('LocalMemoryStore (integration: Postgres + S3)', () => {
  let pool: Pool;
  let s3Client: S3Client;
  let blobs: S3MemoryBlobStore;
  let local: LocalMemoryStore;

  beforeAll(async () => {
    await ensureDatabase();

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
        /* race or already exists */
      }
    }
    // Single workspace-agnostic blob-store instance shared across tests; the
    // test passes per-call workspaceId.
    blobs = new S3MemoryBlobStore({
      client: s3Client,
      bucket: BUCKET,
      keyPrefix: 'test/memory-int/',
    });
    local = new LocalMemoryStore({
      blobStore: blobs,
      metadataStore: new PostgresMemoryMetadataStore(pool),
    });
  });

  afterAll(async () => {
    s3Client?.destroy();
    await pool?.end().catch(() => {});
  });

  it('createStore + listStores + getStore round-trip', async () => {
    const ws = uniqueWorkspaceId();
    const created = await local.createStore({ workspaceId: ws, name: 'docs' });
    expect(created.id).toMatch(/^mems_/);
    expect(created.workspaceId).toBe(ws);

    const fetched = await local.getStore(ws, created.id);
    expect(fetched?.id).toBe(created.id);

    const page = await local.listStores(ws);
    expect(page.items.map((s) => s.id)).toContain(created.id);
  });

  it('writeMemory + openMemory round-trip the bytes via S3', async () => {
    const ws = uniqueWorkspaceId();
    const created = await local.createStore({ workspaceId: ws, name: 'docs' });
    const payload = Buffer.from('integration ok');
    const sha = digest(payload);
    const { memory } = await local.writeMemory({
      workspaceId: ws,
      storeId: created.id,
      path: 'note.md',
      content: Readable.from(payload),
      sizeBytes: payload.length,
      sha256: sha,
    });
    const opened = await local.openMemory(ws, created.id, memory.id);
    expect(opened).not.toBeNull();
    const back = await readAll(opened!.stream);
    expect(back.equals(payload)).toBe(true);
    expect(opened!.sha256).toBe(sha);
    expect(opened!.sizeBytes).toBe(payload.length);
  });

  it('CAS happy path + CAS conflict throws MemoryConflictError', async () => {
    const ws = uniqueWorkspaceId();
    const created = await local.createStore({ workspaceId: ws, name: 'docs' });

    const v1 = Buffer.from('one');
    const sha1 = digest(v1);
    const first = await local.writeMemory({
      workspaceId: ws,
      storeId: created.id,
      path: 'note.md',
      content: Readable.from(v1),
      sizeBytes: v1.length,
      sha256: sha1,
    });

    // CAS happy: matching previousSha256.
    const v2 = Buffer.from('two');
    const sha2 = digest(v2);
    const second = await local.writeMemory({
      workspaceId: ws,
      storeId: created.id,
      path: 'note.md',
      content: Readable.from(v2),
      sizeBytes: v2.length,
      sha256: sha2,
      previousSha256: sha1,
    });
    expect(second.memory.id).toBe(first.memory.id);
    expect(second.memory.currentSha256).toBe(sha2);

    // CAS conflict: caller supplies a stale sha.
    const v3 = Buffer.from('three');
    const sha3 = digest(v3);
    let thrown: unknown;
    try {
      await local.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(v3),
        sizeBytes: v3.length,
        sha256: sha3,
        previousSha256: sha1, // stale: current is sha2
      });
    } catch (err) {
      thrown = err;
    }
    expect(thrown).toBeInstanceOf(MemoryConflictError);
    const conflict = thrown as MemoryConflictError;
    expect(conflict.observedSha).toBe(sha2);
    expect(conflict.expectedSha).toBe(sha1);
  });

  it('listVersions DESC + redactVersion masks only the selected version', async () => {
    const ws = uniqueWorkspaceId();
    const created = await local.createStore({ workspaceId: ws, name: 'docs' });

    const v1 = Buffer.from('one');
    const sha1 = digest(v1);
    const first = await local.writeMemory({
      workspaceId: ws,
      storeId: created.id,
      path: 'note.md',
      content: Readable.from(v1),
      sizeBytes: v1.length,
      sha256: sha1,
    });
    const v2 = Buffer.from('two');
    const sha2 = digest(v2);
    const second = await local.writeMemory({
      workspaceId: ws,
      storeId: created.id,
      path: 'note.md',
      content: Readable.from(v2),
      sizeBytes: v2.length,
      sha256: sha2,
      previousSha256: sha1,
    });

    const versions = await local.listVersions(ws, created.id, first.memory.id);
    expect(versions.map((v) => v.id)).toEqual([second.version.id, first.version.id]);

    // Version blobs are reachable up front.
    await expect(blobs.openVersion(ws, created.id, sha1)).resolves.toBeDefined();
    await expect(blobs.openVersion(ws, created.id, sha2)).resolves.toBeDefined();

    const redacted = await local.redactVersion(ws, created.id, first.version.id);
    expect(redacted.redactedAt).toBeInstanceOf(Date);
    await expect(local.openVersion(ws, created.id, first.version.id)).resolves.toBeNull();
    // SHA-addressed blobs may be shared by multiple version rows. Redaction is
    // therefore a per-version metadata overlay, not a destructive blob delete.
    await expect(blobs.openVersion(ws, created.id, sha1)).resolves.toBeDefined();
    await expect(blobs.openVersion(ws, created.id, sha2)).resolves.toBeDefined();
  });
});
