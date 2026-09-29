// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import fs from 'node:fs';
import { Pool } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import * as schema from '../../src/persistence/postgres/schema.js';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Kafka } from 'kafkajs';
import {
  KafkaTranscriptStore,
  PostgresTranscriptStore,
  applyPostgresTranscriptMigrations,
  type TranscriptStore,
} from '@orca/transcript-store';
import { S3Client } from '@aws-sdk/client-s3';
import {
  LocalFileStore,
  S3BlobStore,
  applyMigrations as applyFileStoreMigrations,
  type FileStore,
} from '@orca/file-store';
import {
  LocalMemoryStore,
  S3MemoryBlobStore,
  PostgresMemoryMetadataStore,
  applyMigrations as applyMemoryStoreMigrations,
} from '@orca/memory-store';
import type { SseConfig } from '../../src/server.js';
import { SessionJwtMinter } from '../../src/auth/session-jwt.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const TEST_DB_URL = process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
// Same precedence `config.ts` uses for `transcriptStoreDatabaseUrl`: an explicit
// `TRANSCRIPT_STORE_DATABASE_URL` wins, otherwise fall back to the registry's own
// `DATABASE_URL` (a single-Postgres self-hosted deployment; see
// `buildTestPostgresStore` below) — CI's registry-service-ts integration job sets
// only `DATABASE_URL`, so this resolves to the same database `getTestDb` uses.
const TRANSCRIPT_STORE_TEST_DB_URL = process.env['TRANSCRIPT_STORE_DATABASE_URL'] ?? TEST_DB_URL;

let pool: Pool | null = null;
let migrated = false;

export async function getTestDb() {
  if (!pool) {
    pool = new Pool({ connectionString: TEST_DB_URL, max: 5 });
  }
  const db = drizzle(pool, { schema });
  if (!migrated) {
    await migrate(db, {
      migrationsFolder: resolve(__dirname, '../../src/persistence/postgres/migrations'),
    });
    migrated = true;
  }
  return { db, pool };
}

export async function closeTestDb() {
  if (pool) {
    await pool.end();
    pool = null;
    migrated = false;
  }
}

/**
 * Minimal in-memory `TranscriptStore` stub for tests that don't exercise the
 * events endpoints. Tests that do use `buildTestStore()`, which talks to a
 * real Kafka broker.
 */
export function buildStubStore(): TranscriptStore {
  return {
    async append() {
      return [];
    },
    async *read() {
      /* nothing */
    },
    async *tail() {
      /* nothing */
    },
    async archive() {
      /* no-op */
    },
    async close() {
      /* no-op */
    },
  };
}

export const STUB_SSE_CONFIG: SseConfig = {
  bufferSize: 256,
  dropAgeMs: 5000,
  heartbeatMs: 15000,
};

/**
 * Real Kafka-backed `TranscriptStore` for integration tests that exercise the
 * `/v1/sessions/:id/events` endpoints end-to-end.  Caller is responsible for
 * `await store.close()` in `afterAll`.
 */
export function buildTestStore(): KafkaTranscriptStore {
  const kafka = new Kafka({
    clientId: 'registry-it',
    brokers: [process.env['KAFKA_BROKERS'] ?? 'localhost:9092'],
  });
  return new KafkaTranscriptStore({ kafka });
}

/**
 * Real Postgres-backed `TranscriptStore` for integration tests that exercise the
 * `colocated` single-writer bridge path bus-free (the registry as in-process
 * shared server, no Kafka). Applies the transcript-store package's
 * own raw-SQL migrations (`transcript_events` / `transcript_event_claims` —
 * table names distinct from the registry's Drizzle schema), so this safely
 * shares the registry's own test Postgres database; no dedicated
 * `transcriptstore` database is required, mirroring a single-Postgres
 * self-hosted deployment (`config.ts`'s
 * `transcriptStoreDatabaseUrl` falls back to `DATABASE_URL`). A short
 * `tailPollIntervalMs` keeps the bridge's live-tail loop fast in tests — no
 * broker consumer-group join to wait out. Caller is responsible for
 * `await store.close()` in `afterAll` (closes the pool too; `closePool`
 * defaults to `true`).
 */
export async function buildTestPostgresStore(): Promise<{
  pool: Pool;
  store: PostgresTranscriptStore;
}> {
  const pool = new Pool({ connectionString: TRANSCRIPT_STORE_TEST_DB_URL, max: 5 });
  await applyPostgresTranscriptMigrations(pool);
  return { pool, store: new PostgresTranscriptStore({ pool, tailPollIntervalMs: 50 }) };
}

/** Delete every transcript row for a workspace — Postgres bridge-test cleanup. */
export async function deleteTranscriptRowsForWorkspace(
  pool: Pool,
  workspaceId: string,
): Promise<void> {
  await pool.query('DELETE FROM transcript_events WHERE workspace_id = $1', [workspaceId]);
}

/**
 * Build a `SessionJwtMinter` from the integration-test PEM fixture.  Resolves
 * the fixture path relative to this file so callers don't depend on cwd.
 */
export function buildTestJwtMinter(): SessionJwtMinter {
  const pemPath = resolve(__dirname, '../fixtures/session-jwt-private.pem');
  const pem = fs.readFileSync(pemPath, 'utf8');
  return new SessionJwtMinter({
    privateKeyPem: pem,
    issuer: 'orca-registry',
    audience: 'ai-gateway',
    ttlSecs: 300,
  });
}

let fileStorePool: Pool | null = null;
let fileStoreInstance: LocalFileStore | null = null;

/**
 * Build a `FileStore` against RustFS + the dev `filestore` Postgres database.
 * Use this for tests that exercise the actual upload path. Singleton across
 * tests in the same vitest worker; close via `closeTestFileStore()`.
 */
export async function buildTestFileStore(): Promise<LocalFileStore> {
  if (fileStoreInstance) return fileStoreInstance;
  const url =
    process.env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore';
  fileStorePool = new Pool({ connectionString: url });
  await applyFileStoreMigrations(fileStorePool);
  const s3 = new S3Client({
    endpoint: process.env['S3_ENDPOINT'] ?? 'http://localhost:9000',
    region: process.env['S3_REGION'] ?? 'us-east-1',
    credentials: {
      accessKeyId: process.env['S3_ACCESS_KEY'] ?? 'minioadmin',
      secretAccessKey: process.env['S3_SECRET_KEY'] ?? 'minioadmin',
    },
    forcePathStyle: true,
  });
  fileStoreInstance = new LocalFileStore({
    pool: fileStorePool,
    blobStore: new S3BlobStore({
      client: s3,
      bucket: process.env['S3_BUCKET'] ?? 'orca-files',
      keyPrefix: 'test/registry/',
    }),
  });
  return fileStoreInstance;
}

/**
 * In-memory `FileStore` stub for tests that don't exercise upload/download —
 * just satisfies the `BuildAppOptions.fileStore` typing.
 */
export function buildStubFileStore(): FileStore {
  return {
    async create() {
      throw new Error('stub fileStore');
    },
    async get() {
      return null;
    },
    async list() {
      return { items: [], nextCursor: null };
    },
    async open() {
      return null;
    },
    async archive() {
      /* no-op */
    },
    async delete() {
      /* no-op */
    },
    async close() {
      /* no-op */
    },
  };
}

export async function closeTestFileStore(): Promise<void> {
  if (fileStoreInstance) {
    await fileStoreInstance.close();
    fileStoreInstance = null;
    fileStorePool = null;
  }
}

let memoryStorePool: Pool | null = null;
let memoryStoreInstance: LocalMemoryStore | null = null;

/**
 * Build a `MemoryStore` against RustFS + the dev `memorystore` Postgres
 * database. Use this for tests that exercise the actual `/v1/memory_stores/*`
 * paths. Singleton across tests in the same vitest worker; close via
 * `closeTestMemoryStore()`.
 *
 * Mirrors `buildTestFileStore` for consistency with the file-store wiring.
 */
export async function buildTestMemoryStore(): Promise<LocalMemoryStore> {
  if (memoryStoreInstance) return memoryStoreInstance;
  const adminUrl =
    process.env['MEMORYSTORE_ADMIN_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/postgres';
  const url =
    process.env['MEMORYSTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/memorystore';

  // Best-effort `CREATE DATABASE` so local dev doesn't need a manual bootstrap.
  // The library tests follow the same pattern (see
  // packages/memory-store/test/integration/memory-store.spec.ts).
  const adminPool = new Pool({ connectionString: adminUrl, max: 1 });
  try {
    const dbName = new URL(url).pathname.replace(/^\//, '');
    if (dbName) {
      const exists = await adminPool.query('SELECT 1 FROM pg_database WHERE datname = $1', [
        dbName,
      ]);
      if (exists.rowCount === 0 && /^[a-z_][a-z0-9_]*$/i.test(dbName)) {
        await adminPool.query(`CREATE DATABASE "${dbName}"`);
      }
    }
  } finally {
    await adminPool.end().catch(() => {});
  }

  memoryStorePool = new Pool({ connectionString: url });
  await applyMemoryStoreMigrations(memoryStorePool);
  const s3 = new S3Client({
    endpoint: process.env['S3_ENDPOINT'] ?? 'http://localhost:9000',
    region: process.env['S3_REGION'] ?? 'us-east-1',
    credentials: {
      accessKeyId: process.env['S3_ACCESS_KEY'] ?? 'minioadmin',
      secretAccessKey: process.env['S3_SECRET_KEY'] ?? 'minioadmin',
    },
    forcePathStyle: true,
  });
  memoryStoreInstance = new LocalMemoryStore({
    blobStore: new S3MemoryBlobStore({
      client: s3,
      bucket: process.env['S3_BUCKET'] ?? 'orca-files',
      keyPrefix: 'test/registry-memory/',
    }),
    metadataStore: new PostgresMemoryMetadataStore(memoryStorePool),
  });
  return memoryStoreInstance;
}

export async function closeTestMemoryStore(): Promise<void> {
  if (memoryStoreInstance) {
    await memoryStoreInstance.close();
    memoryStoreInstance = null;
  }
  if (memoryStorePool) {
    await memoryStorePool.end().catch(() => {});
    memoryStorePool = null;
  }
}
