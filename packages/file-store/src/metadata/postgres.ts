// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { eq, and, isNull, desc, asc, sql } from 'drizzle-orm';
import { drizzle, type NodePgDatabase } from 'drizzle-orm/node-postgres';
import {
  bigint,
  boolean,
  index,
  jsonb,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
} from 'drizzle-orm/pg-core';
import type { Pool } from 'pg';
import type { FilePurpose, FileRecord } from '../types.js';
import type { FileMetadataStore } from './store.js';
import type { ListFilesOptions } from '../store.js';

export const files = pgTable(
  'files',
  {
    id: text('id').primaryKey(),
    workspaceId: text('workspace_id').notNull(),
    filename: text('filename').notNull(),
    mimeType: text('mime_type').notNull(),
    sizeBytes: bigint('size_bytes', { mode: 'number' }).notNull(),
    sha256: text('sha256').notNull(),
    blobUri: text('blob_uri').notNull(),
    metadata: jsonb('metadata').notNull().default({}),
    purpose: text('purpose').notNull().default('agent'),
    scopeId: text('scope_id'),
    downloadable: boolean('downloadable').notNull().default(false),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    // Partial unique index: only active `purpose='agent'` rows participate in
    // dedup. Archiving releases the digest so the same bytes can be uploaded as
    // a fresh active File. `purpose='agent_output'` rows always insert (each
    // session output is distinct). See `LocalFileStore.create` for the matching
    // application-side lookup.
    uniqueIndex('files_ws_sha256_agent_idx')
      .on(t.workspaceId, t.sha256)
      .where(sql`purpose = 'agent' AND archived_at IS NULL AND deleted_at IS NULL`),
    index('files_ws_archived_idx').on(t.workspaceId, t.archivedAt),
    index('files_ws_scope_idx').on(t.workspaceId, t.scopeId),
  ],
);

export type FileRow = typeof files.$inferSelect;
export type FileInsert = typeof files.$inferInsert;

export const schema = { files };
export type Db = NodePgDatabase<typeof schema>;

export function buildDb(pool: Pool): Db {
  return drizzle(pool, { schema });
}

function rowToRecord(r: FileRow): FileRecord {
  return {
    id: r.id,
    workspaceId: r.workspaceId,
    filename: r.filename,
    mimeType: r.mimeType,
    sizeBytes: r.sizeBytes,
    sha256: r.sha256,
    metadata: (r.metadata as Record<string, string>) ?? {},
    // The DB stores purpose as TEXT (Drizzle has no native string-union enum
    // here), but the column has a CHECK-equivalent default + we only ever
    // write `'agent' | 'agent_output'` from the application layer. Cast.
    purpose: r.purpose as FilePurpose,
    scopeId: r.scopeId ?? null,
    downloadable: r.downloadable,
    archivedAt: r.archivedAt,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  };
}

/**
 * The metadata-side of the file store: pure CRUD against Postgres. Composed
 * with a `BlobStore` by `LocalFileStore`.
 */
export class PostgresFileMetadataStore implements FileMetadataStore {
  private readonly db: Db;
  constructor(pool: Pool) {
    this.db = buildDb(pool);
  }

  async findBySha(workspaceId: string, sha256: string): Promise<FileRecord | null> {
    const rows = await this.db
      .select()
      .from(files)
      .where(
        and(
          isNull(files.deletedAt),
          eq(files.workspaceId, workspaceId),
          eq(files.sha256, sha256),
          eq(files.purpose, 'agent'),
          isNull(files.archivedAt),
        ),
      )
      .limit(1);
    return rows[0] ? rowToRecord(rows[0]) : null;
  }

  async insert(record: FileInsert): Promise<FileRecord> {
    const [row] = await this.db.insert(files).values(record).returning();
    if (!row) throw new Error(`insert returned no row for ${record.id}`);
    return rowToRecord(row);
  }

  async findById(workspaceId: string, fileId: string): Promise<FileRecord | null> {
    const rows = await this.db
      .select()
      .from(files)
      .where(and(isNull(files.deletedAt), eq(files.id, fileId), eq(files.workspaceId, workspaceId)))
      .limit(1);
    return rows[0] ? rowToRecord(rows[0]) : null;
  }

  async list(
    workspaceId: string,
    opts?: ListFilesOptions,
  ): Promise<{ items: FileRecord[]; nextCursor: string | null }> {
    const limit = Math.min(opts?.limit ?? 100, 1000);
    const cursor = opts?.cursor;
    const beforeCursor = opts?.beforeCursor;
    const scopeId = opts?.scopeId;
    const purpose = opts?.purpose;
    // Build the predicate explicitly. `scopeId` is matched as exact equality;
    // an empty string would NOT match a NULL row (Postgres equality semantics)
    // — callers must omit `scopeId` if they want all rows.
    const baseWhere = and(
      eq(files.workspaceId, workspaceId),
      isNull(files.archivedAt),
      scopeId ? eq(files.scopeId, scopeId) : undefined,
      purpose ? eq(files.purpose, purpose) : undefined,
    );
    if (beforeCursor) {
      const rows = await this.db
        .select()
        .from(files)
        .where(and(isNull(files.deletedAt), baseWhere, sql`${files.id} > ${beforeCursor}`))
        .orderBy(asc(files.id))
        .limit(limit + 1);
      const window = rows.slice(0, limit).reverse();
      const items = window.map(rowToRecord);
      const nextCursor = rows.length > limit && items.length > 0 ? items[0]!.id : null;
      return { items, nextCursor };
    }

    const where = cursor ? and(baseWhere, sql`${files.id} < ${cursor}`) : baseWhere;
    const rows = await this.db
      .select()
      .from(files)
      .where(and(isNull(files.deletedAt), where))
      .orderBy(desc(files.id))
      .limit(limit + 1);
    const items = rows.slice(0, limit).map(rowToRecord);
    const nextCursor = rows.length > limit ? items[items.length - 1]!.id : null;
    return { items, nextCursor };
  }

  async archive(workspaceId: string, fileId: string): Promise<void> {
    await this.db
      .update(files)
      .set({ archivedAt: new Date(), updatedAt: new Date() })
      .where(
        and(isNull(files.deletedAt), eq(files.id, fileId), eq(files.workspaceId, workspaceId)),
      );
  }

  async delete(workspaceId: string, fileId: string): Promise<void> {
    await this.db
      .update(files)
      .set({ deletedAt: new Date(), updatedAt: new Date() })
      .where(
        and(isNull(files.deletedAt), eq(files.id, fileId), eq(files.workspaceId, workspaceId)),
      );
  }
}

/**
 * Apply Drizzle migrations from the package's `migrations` directory.
 * Used by tests and by service startup. Idempotent.
 */
export async function applyMigrations(pool: Pool): Promise<void> {
  const { migrate } = await import('drizzle-orm/node-postgres/migrator');
  const { fileURLToPath } = await import('node:url');
  const { dirname, resolve } = await import('node:path');
  const { existsSync } = await import('node:fs');
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = dirname(__filename);
  // When loading from src/ (vitest), this file lives at src/metadata/postgres.ts
  // and migrations are at src/metadata/migrations (sibling subdirectory).
  // When loading from dist/ (built), index.js lives at dist/ and migrations
  // are at src/metadata/migrations relative to the package root. Try both.
  const candidates = [
    resolve(__dirname, 'migrations'), // src/metadata/{this file} -> src/metadata/migrations
    resolve(__dirname, '../src/metadata/migrations'), // dist/ -> src/metadata/migrations
    resolve(__dirname, '../metadata/migrations'), // legacy fallback
  ];
  const migrationsFolder = candidates.find((p) => existsSync(p));
  if (!migrationsFolder) {
    throw new Error(
      `applyMigrations: cannot find migrations folder; tried: ${candidates.join(', ')}`,
    );
  }
  // Both registry and harness open this database and run migrations at boot, so
  // serialize the DDL with a session-scoped advisory lock held on a dedicated
  // client for the whole migrate() span (drizzle's node-postgres migrator takes
  // no lock of its own). Binding drizzle to that same client keeps the lock and
  // every migration statement on one session; the lock is per-database, so it
  // only contends within this `filestore` DB.
  const client = await pool.connect();
  try {
    await client.query("SELECT pg_advisory_lock(hashtext('orca_file_store_migrations'))");
    await migrate(drizzle(client), { migrationsFolder });
  } finally {
    try {
      await client.query("SELECT pg_advisory_unlock(hashtext('orca_file_store_migrations'))");
    } finally {
      client.release();
    }
  }
}
