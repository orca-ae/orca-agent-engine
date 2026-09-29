// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { existsSync } from 'node:fs';
import { AsyncLocalStorage } from 'node:async_hooks';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { and, asc, desc, eq, gte, inArray, isNull, lte, sql } from 'drizzle-orm';
import { drizzle, type NodePgDatabase } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import {
  bigint,
  foreignKey,
  index,
  jsonb,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
} from 'drizzle-orm/pg-core';
import type { Pool } from 'pg';
import type { MemoryRecord, MemoryStoreRecord, MemoryVersionRecord } from '../types.js';
import type { ListStoresOptions, MemoryIdsFilter } from '../store.js';
import { MemoryConflictError } from '../store.js';
import type {
  DeleteMemoryInput,
  InsertStoreInput,
  MemoryMetadataStore,
  RedactMemoryVersionInput,
  UpsertMemoryInput,
  UpsertMemoryResult,
} from './store.js';

export const memoryStores = pgTable(
  'memory_stores',
  {
    id: text('id').primaryKey(),
    workspaceId: text('workspace_id').notNull(),
    name: text('name').notNull(),
    description: text('description'),
    metadata: jsonb('metadata').notNull().default({}),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    archivedAt: timestamp('archived_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('memory_stores_ws_idx').on(t.workspaceId),
    uniqueIndex('memory_stores_workspace_id_idx').on(t.workspaceId, t.id),
  ],
);

export const memories = pgTable(
  'memories',
  {
    id: text('id').primaryKey(),
    workspaceId: text('workspace_id').notNull(),
    storeId: text('store_id').notNull(),
    path: text('path').notNull(),
    currentSha256: text('current_sha256').notNull(),
    sizeBytes: bigint('size_bytes', { mode: 'number' }).notNull(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
    updatedBySessionId: text('updated_by_session_id'),
    updatedByEventId: text('updated_by_event_id'),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
  },
  (t) => [
    uniqueIndex('memories_workspace_store_path_idx')
      .on(t.workspaceId, t.storeId, t.path)
      .where(sql`${t.deletedAt} is null`),
    uniqueIndex('memories_workspace_store_id_idx').on(t.workspaceId, t.storeId, t.id),
    foreignKey({
      name: 'memories_workspace_store_fk',
      columns: [t.workspaceId, t.storeId],
      foreignColumns: [memoryStores.workspaceId, memoryStores.id],
    }).onDelete('cascade'),
  ],
);

export const memoryVersions = pgTable(
  'memory_versions',
  {
    id: text('id').primaryKey(),
    workspaceId: text('workspace_id').notNull(),
    storeId: text('store_id').notNull(),
    memoryId: text('memory_id').notNull(),
    path: text('path').notNull(),
    sha256: text('sha256').notNull(),
    sizeBytes: bigint('size_bytes', { mode: 'number' }).notNull(),
    writtenBySessionId: text('written_by_session_id'),
    writtenByApiKeyId: text('written_by_api_key_id'),
    writtenByUserId: text('written_by_user_id'),
    writtenByEventId: text('written_by_event_id'),
    writtenAt: timestamp('written_at', { withTimezone: true }).notNull().defaultNow(),
    deletedAt: timestamp('deleted_at', { withTimezone: true }),
    redactedAt: timestamp('redacted_at', { withTimezone: true }),
    redactedBySessionId: text('redacted_by_session_id'),
    redactedByApiKeyId: text('redacted_by_api_key_id'),
    redactedByUserId: text('redacted_by_user_id'),
  },
  (t) => [
    index('memory_versions_store_idx').on(t.workspaceId, t.storeId, t.writtenAt),
    index('memory_versions_memory_idx').on(t.workspaceId, t.storeId, t.memoryId, t.writtenAt),
    foreignKey({
      name: 'memory_versions_workspace_store_fk',
      columns: [t.workspaceId, t.storeId],
      foreignColumns: [memoryStores.workspaceId, memoryStores.id],
    }).onDelete('cascade'),
    foreignKey({
      name: 'memory_versions_workspace_memory_fk',
      columns: [t.workspaceId, t.storeId, t.memoryId],
      foreignColumns: [memories.workspaceId, memories.storeId, memories.id],
    }).onDelete('cascade'),
  ],
);

export const schema = { memoryStores, memories, memoryVersions };
export type Db = NodePgDatabase<typeof schema>;

export function buildDb(pool: Pool): Db {
  return drizzle(pool, { schema });
}

type MemoryStoreRow = typeof memoryStores.$inferSelect;
type MemoryRow = typeof memories.$inferSelect;
type MemoryVersionRow = typeof memoryVersions.$inferSelect;

function rowToStore(r: MemoryStoreRow): MemoryStoreRecord {
  return {
    id: r.id,
    workspaceId: r.workspaceId,
    name: r.name,
    description: r.description ?? null,
    metadata: normalizeMetadata(r.metadata),
    archivedAt: r.archivedAt ?? null,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  };
}

function normalizeMetadata(value: unknown): Record<string, string> {
  const metadata = Object.create(null) as Record<string, string>;
  if (!value || typeof value !== 'object' || Array.isArray(value)) return metadata;
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    if (typeof item === 'string') metadata[key] = item;
  }
  return metadata;
}

function rowToMemory(r: MemoryRow): MemoryRecord {
  return {
    id: r.id,
    storeId: r.storeId,
    path: r.path,
    currentSha256: r.currentSha256,
    sizeBytes: r.sizeBytes,
    updatedAt: r.updatedAt,
    updatedBySessionId: r.updatedBySessionId ?? null,
    updatedByEventId: r.updatedByEventId ?? null,
  };
}

function rowToVersion(r: MemoryVersionRow): MemoryVersionRecord {
  return {
    id: r.id,
    storeId: r.storeId,
    memoryId: r.memoryId,
    path: r.path,
    sha256: r.sha256,
    sizeBytes: r.sizeBytes,
    writtenBySessionId: r.writtenBySessionId ?? null,
    writtenByApiKeyId: r.writtenByApiKeyId ?? null,
    writtenByUserId: r.writtenByUserId ?? null,
    writtenByEventId: r.writtenByEventId ?? null,
    writtenAt: r.writtenAt,
    redactedAt: r.redactedAt ?? null,
    redactedBySessionId: r.redactedBySessionId ?? null,
    redactedByApiKeyId: r.redactedByApiKeyId ?? null,
    redactedByUserId: r.redactedByUserId ?? null,
  };
}

function isUniqueViolation(err: unknown): boolean {
  return typeof err === 'object' && err !== null && (err as { code?: unknown }).code === '23505';
}

function toJsonMetadata(source: Record<string, string>): Record<string, string> {
  const metadata: Record<string, string> = {};
  for (const [key, value] of Object.entries(source)) {
    Object.defineProperty(metadata, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  }
  return metadata;
}

/**
 * Postgres-backed metadata store. Composes with `MemoryBlobStore` via
 * `LocalMemoryStore`. The `upsertMemory` path is transactional: the CAS check
 * + memory upsert + version insert happen in a single SQL transaction so the
 * caller never sees a half-written write.
 */
export class PostgresMemoryMetadataStore implements MemoryMetadataStore {
  private readonly rootDb: Db;
  private readonly writeContext = new AsyncLocalStorage<{ key: string; db: Db }>();

  private get db(): Db {
    return this.writeContext.getStore()?.db ?? this.rootDb;
  }

  constructor(private readonly pool: Pool) {
    this.rootDb = buildDb(pool);
  }

  async withStoreWriteLock<T>(
    workspaceId: string,
    storeId: string,
    work: () => Promise<T>,
  ): Promise<T> {
    const key = JSON.stringify(['orca.memory-store.write', workspaceId, storeId]);
    const current = this.writeContext.getStore();
    if (current?.key === key) return work();
    if (current) throw new Error('nested Memory store lock differs');
    const client = await this.pool.connect();
    let locked = false;
    let result: { ok: true; value: T } | { ok: false; error: unknown };
    try {
      await client.query('SELECT pg_advisory_lock(hashtextextended($1, 0))', [key]);
      locked = true;
      // The callback's metadata queries use this same connection. Waiting
      // writers cannot exhaust the pool needed by the current lock holder.
      result = {
        ok: true,
        value: await this.writeContext.run({ key, db: drizzle(client, { schema }) }, work),
      };
    } catch (error) {
      result = { ok: false, error };
    }
    try {
      if (locked) await client.query('SELECT pg_advisory_unlock(hashtextextended($1, 0))', [key]);
    } catch (error) {
      // Destroy the connection so a failed unlock cannot return a held session
      // lock to the pool. Keep the original work error when both operations fail.
      client.release(
        error instanceof Error ? error : new Error('failed to release Memory write lock'),
      );
      throw result.ok ? error : result.error;
    }
    client.release();
    if (!result.ok) throw result.error;
    return result.value;
  }

  async insertStore(record: InsertStoreInput): Promise<MemoryStoreRecord> {
    const rows = await this.db
      .insert(memoryStores)
      .values({
        id: record.id,
        workspaceId: record.workspaceId,
        name: record.name,
        description: record.description,
        metadata: toJsonMetadata(record.metadata ?? {}),
        archivedAt: record.archivedAt,
      })
      .returning();
    const row = rows[0];
    if (!row) throw new Error(`insertStore returned no row for ${record.id}`);
    return rowToStore(row);
  }

  async listStores(
    workspaceId: string,
    opts?: ListStoresOptions,
  ): Promise<{ items: MemoryStoreRecord[]; nextCursor: string | null }> {
    const limit = opts?.limit ?? 100;
    const rows = await this.db
      .select()
      .from(memoryStores)
      .where(
        and(
          isNull(memoryStores.deletedAt),
          eq(memoryStores.workspaceId, workspaceId),
          opts?.cursor ? sql`${memoryStores.id} > ${opts.cursor}` : undefined,
          opts?.includeArchived === false ? isNull(memoryStores.archivedAt) : undefined,
          opts?.createdAtGte ? gte(memoryStores.createdAt, opts.createdAtGte) : undefined,
          opts?.createdAtLte ? lte(memoryStores.createdAt, opts.createdAtLte) : undefined,
        ),
      )
      .orderBy(asc(memoryStores.id))
      .offset(opts?.offset ?? 0)
      .limit(limit + 1);
    const items = rows.slice(0, limit).map(rowToStore);
    const nextCursor = rows.length > limit ? items[items.length - 1]!.id : null;
    return { items, nextCursor };
  }

  async getStore(workspaceId: string, storeId: string): Promise<MemoryStoreRecord | null> {
    const rows = await this.db
      .select()
      .from(memoryStores)
      .where(
        and(
          isNull(memoryStores.deletedAt),
          eq(memoryStores.workspaceId, workspaceId),
          eq(memoryStores.id, storeId),
        ),
      )
      .limit(1);
    return rows[0] ? rowToStore(rows[0]) : null;
  }

  async updateStore(
    workspaceId: string,
    storeId: string,
    updates: { name?: string; description?: string | null; metadata?: Record<string, string> },
  ): Promise<MemoryStoreRecord | null> {
    const rows = await this.db
      .update(memoryStores)
      .set({
        ...(updates.name !== undefined ? { name: updates.name } : {}),
        ...(updates.description !== undefined ? { description: updates.description } : {}),
        ...(updates.metadata !== undefined ? { metadata: toJsonMetadata(updates.metadata) } : {}),
        updatedAt: new Date(),
      })
      .where(
        and(
          isNull(memoryStores.deletedAt),
          eq(memoryStores.workspaceId, workspaceId),
          eq(memoryStores.id, storeId),
        ),
      )
      .returning();
    return rows[0] ? rowToStore(rows[0]) : null;
  }

  async archiveStore(workspaceId: string, storeId: string): Promise<void> {
    return this.withStoreWriteLock(workspaceId, storeId, () =>
      this.archiveStoreLocked(workspaceId, storeId),
    );
  }

  private async archiveStoreLocked(workspaceId: string, storeId: string): Promise<void> {
    const now = new Date();
    await this.db
      .update(memoryStores)
      .set({ archivedAt: now, updatedAt: now })
      .where(
        and(
          isNull(memoryStores.deletedAt),
          eq(memoryStores.workspaceId, workspaceId),
          eq(memoryStores.id, storeId),
        ),
      );
  }

  async deleteStore(workspaceId: string, storeId: string): Promise<void> {
    return this.withStoreWriteLock(workspaceId, storeId, () =>
      this.deleteStoreLocked(workspaceId, storeId),
    );
  }

  private async deleteStoreLocked(workspaceId: string, storeId: string): Promise<void> {
    // Retain the complete metadata graph and its blobs behind one deletion fence.
    await this.db.transaction(async (tx) => {
      const [parent] = await tx
        .select({ id: memoryStores.id })
        .from(memoryStores)
        .where(
          and(
            eq(memoryStores.workspaceId, workspaceId),
            eq(memoryStores.id, storeId),
            isNull(memoryStores.deletedAt),
          ),
        )
        .for('update');
      if (!parent) return;
      const now = new Date();
      await tx
        .update(memoryVersions)
        .set({ deletedAt: now })
        .where(
          and(
            isNull(memoryVersions.deletedAt),
            eq(memoryVersions.workspaceId, workspaceId),
            eq(memoryVersions.storeId, storeId),
          ),
        );
      await tx
        .update(memories)
        .set({ deletedAt: sql`coalesce(${memories.deletedAt}, ${now})` })
        .where(and(eq(memories.workspaceId, workspaceId), eq(memories.storeId, storeId)));
      await tx
        .update(memoryStores)
        .set({ deletedAt: now })
        .where(
          and(
            isNull(memoryStores.deletedAt),
            eq(memoryStores.workspaceId, workspaceId),
            eq(memoryStores.id, storeId),
          ),
        );
    });
  }

  async upsertMemory(input: UpsertMemoryInput): Promise<UpsertMemoryResult> {
    return this.withStoreWriteLock(input.workspaceId, input.storeId, () =>
      this.upsertMemoryLocked(input),
    );
  }

  private async upsertMemoryLocked(input: UpsertMemoryInput): Promise<UpsertMemoryResult> {
    try {
      return await this.db.transaction(async (tx) => {
        const [parent] = await tx
          .select({ id: memoryStores.id })
          .from(memoryStores)
          .where(
            and(
              eq(memoryStores.workspaceId, input.workspaceId),
              eq(memoryStores.id, input.storeId),
              isNull(memoryStores.deletedAt),
            ),
          )
          .for('share');
        if (!parent)
          throw new Error(
            `upsertMemory: store ${input.storeId} not found in workspace ${input.workspaceId}`,
          );
        const existingVersionRows = await tx
          .select()
          .from(memoryVersions)
          .where(
            and(
              isNull(memoryVersions.deletedAt),
              eq(memoryVersions.workspaceId, input.workspaceId),
              eq(memoryVersions.storeId, input.storeId),
              eq(memoryVersions.id, input.versionId),
            ),
          )
          .limit(1);
        const existingVersion = existingVersionRows[0];
        if (existingVersion) {
          const existingMemoryRows = await tx
            .select()
            .from(memories)
            .where(
              and(
                eq(memories.workspaceId, input.workspaceId),
                eq(memories.storeId, input.storeId),
                eq(memories.id, existingVersion.memoryId),
              ),
            )
            .limit(1);
          const existingMemory = existingMemoryRows[0];
          if (!existingMemory) {
            throw new Error(`upsertMemory: version ${input.versionId} has no memory row`);
          }
          return {
            ok: true,
            memory: rowToMemory(existingMemory),
            version: rowToVersion(existingVersion),
          };
        }

        const existing = await tx
          .select()
          .from(memories)
          .where(
            and(
              eq(memories.workspaceId, input.workspaceId),
              eq(memories.storeId, input.storeId),
              input.memoryId ? eq(memories.id, input.memoryId) : eq(memories.path, input.path),
              isNull(memories.deletedAt),
            ),
          )
          .limit(1);

        const prior = existing[0];
        if (input.memoryId && !prior) {
          throw new Error(`upsertMemory: memory ${input.memoryId} not found`);
        }
        if (prior) {
          if (input.createOnly) {
            const error = new Error(`memory path already exists: ${input.path}`) as Error & {
              code?: string;
              conflictingMemoryId?: string;
            };
            error.code = '23505';
            error.conflictingMemoryId = prior.id;
            throw error;
          }
          // CAS: caller asserted what the current sha is; abort if it drifted.
          if (input.previousSha256 !== undefined && input.previousSha256 !== prior.currentSha256) {
            return {
              ok: false,
              observedSha: prior.currentSha256,
              expectedSha: input.previousSha256,
            };
          }
          const now = new Date();
          const updatedRows = await tx
            .update(memories)
            .set({
              currentSha256: input.sha256,
              path: input.path,
              sizeBytes: input.sizeBytes,
              updatedAt: now,
              updatedBySessionId: input.writtenBySessionId ?? null,
              updatedByEventId: input.writtenByEventId ?? null,
              deletedAt: null,
            })
            .where(
              and(
                eq(memories.workspaceId, input.workspaceId),
                eq(memories.storeId, input.storeId),
                eq(memories.id, prior.id),
              ),
            )
            .returning();
          const versionRows = await tx
            .insert(memoryVersions)
            .values({
              id: input.versionId,
              workspaceId: input.workspaceId,
              storeId: input.storeId,
              memoryId: prior.id,
              path: input.path,
              sha256: input.sha256,
              sizeBytes: input.sizeBytes,
              writtenBySessionId: input.writtenBySessionId ?? null,
              writtenByApiKeyId: input.writtenByApiKeyId ?? null,
              writtenByUserId: input.writtenByUserId ?? null,
              writtenByEventId: input.writtenByEventId ?? null,
            })
            .returning();
          const memoryRow = updatedRows[0];
          const versionRow = versionRows[0];
          if (!memoryRow || !versionRow) {
            throw new Error(`upsertMemory: update returned no rows for ${input.path}`);
          }
          return {
            ok: true,
            memory: rowToMemory(memoryRow),
            version: rowToVersion(versionRow),
          };
        }

        // First write to this path. CAS with previousSha set is a mismatch
        // because there is no current sha to match against — surface that
        // explicitly with an empty observedSha so the caller can distinguish
        // "expected a sha but the path was empty".
        if (input.previousSha256 !== undefined) {
          return {
            ok: false,
            observedSha: '',
            expectedSha: input.previousSha256,
          };
        }
        const memoryRows = await tx
          .insert(memories)
          .values({
            id: input.id,
            workspaceId: input.workspaceId,
            storeId: input.storeId,
            path: input.path,
            currentSha256: input.sha256,
            sizeBytes: input.sizeBytes,
            updatedBySessionId: input.writtenBySessionId ?? null,
            updatedByEventId: input.writtenByEventId ?? null,
          })
          .returning();
        const versionRows = await tx
          .insert(memoryVersions)
          .values({
            id: input.versionId,
            workspaceId: input.workspaceId,
            storeId: input.storeId,
            memoryId: input.id,
            path: input.path,
            sha256: input.sha256,
            sizeBytes: input.sizeBytes,
            writtenBySessionId: input.writtenBySessionId ?? null,
            writtenByApiKeyId: input.writtenByApiKeyId ?? null,
            writtenByUserId: input.writtenByUserId ?? null,
            writtenByEventId: input.writtenByEventId ?? null,
          })
          .returning();
        const memoryRow = memoryRows[0];
        const versionRow = versionRows[0];
        if (!memoryRow || !versionRow) {
          throw new Error(`upsertMemory: insert returned no rows for ${input.path}`);
        }
        return {
          ok: true,
          memory: rowToMemory(memoryRow),
          version: rowToVersion(versionRow),
        };
      });
    } catch (e) {
      if (!isUniqueViolation(e)) throw e;
      if (input.createOnly || input.memoryId) throw e;
      const existingVersionRows = await this.db
        .select()
        .from(memoryVersions)
        .where(
          and(
            isNull(memoryVersions.deletedAt),
            eq(memoryVersions.workspaceId, input.workspaceId),
            eq(memoryVersions.storeId, input.storeId),
            eq(memoryVersions.id, input.versionId),
          ),
        )
        .limit(1);
      const existingVersion = existingVersionRows[0];
      if (!existingVersion) throw e;
      const existingMemoryRows = await this.db
        .select()
        .from(memories)
        .where(
          and(
            eq(memories.workspaceId, input.workspaceId),
            eq(memories.storeId, input.storeId),
            eq(memories.id, existingVersion.memoryId),
          ),
        )
        .limit(1);
      const existingMemory = existingMemoryRows[0];
      if (!existingMemory) {
        throw new Error(`upsertMemory: version ${input.versionId} has no memory row`);
      }
      return {
        ok: true,
        memory: rowToMemory(existingMemory),
        version: rowToVersion(existingVersion),
      };
    }
  }

  async getMemory(
    workspaceId: string,
    storeId: string,
    memoryId: string,
  ): Promise<MemoryRecord | null> {
    const rows = await this.db
      .select()
      .from(memories)
      .where(
        and(
          eq(memories.workspaceId, workspaceId),
          eq(memories.storeId, storeId),
          eq(memories.id, memoryId),
          isNull(memories.deletedAt),
        ),
      )
      .limit(1);
    return rows[0] ? rowToMemory(rows[0]) : null;
  }

  async getMemoryByPath(
    workspaceId: string,
    storeId: string,
    path: string,
  ): Promise<MemoryRecord | null> {
    const rows = await this.db
      .select()
      .from(memories)
      .where(
        and(
          eq(memories.workspaceId, workspaceId),
          eq(memories.storeId, storeId),
          eq(memories.path, path),
          isNull(memories.deletedAt),
        ),
      )
      .limit(1);
    return rows[0] ? rowToMemory(rows[0]) : null;
  }

  async listMemories(
    workspaceId: string,
    storeId: string,
    opts?: MemoryIdsFilter,
  ): Promise<MemoryRecord[]> {
    if (opts?.memoryIds?.length === 0) return [];
    const rows = await this.db
      .select()
      .from(memories)
      .where(
        and(
          eq(memories.workspaceId, workspaceId),
          eq(memories.storeId, storeId),
          opts?.memoryIds === undefined ? undefined : inArray(memories.id, [...opts.memoryIds]),
          isNull(memories.deletedAt),
        ),
      )
      .orderBy(asc(memories.path));
    return rows.map(rowToMemory);
  }

  async deleteMemory(input: DeleteMemoryInput): Promise<boolean> {
    return this.withStoreWriteLock(input.workspaceId, input.storeId, () =>
      this.deleteMemoryLocked(input),
    );
  }

  private async deleteMemoryLocked(input: DeleteMemoryInput): Promise<boolean> {
    return this.db.transaction(async (tx) => {
      // Match store deletion and writes: lock the parent before its children.
      const [parent] = await tx
        .select({ id: memoryStores.id })
        .from(memoryStores)
        .where(
          and(
            eq(memoryStores.workspaceId, input.workspaceId),
            eq(memoryStores.id, input.storeId),
            isNull(memoryStores.deletedAt),
          ),
        )
        .for('share');
      if (!parent) return false;
      const [prior] = await tx
        .select()
        .from(memories)
        .where(
          and(
            eq(memories.workspaceId, input.workspaceId),
            eq(memories.storeId, input.storeId),
            eq(memories.id, input.memoryId),
          ),
        )
        .for('update');
      if (!prior) return false;
      const [receipt] = await tx
        .select()
        .from(memoryVersions)
        .where(
          and(
            eq(memoryVersions.workspaceId, input.workspaceId),
            eq(memoryVersions.storeId, input.storeId),
            eq(memoryVersions.id, input.versionId),
          ),
        );
      if (receipt) {
        if (
          receipt.workspaceId !== input.workspaceId ||
          receipt.storeId !== input.storeId ||
          receipt.memoryId !== input.memoryId ||
          prior.deletedAt === null ||
          receipt.writtenBySessionId !== (input.writtenBySessionId ?? null) ||
          receipt.writtenByEventId !== (input.writtenByEventId ?? null) ||
          receipt.writtenByApiKeyId !== (input.writtenByApiKeyId ?? null) ||
          receipt.writtenByUserId !== (input.writtenByUserId ?? null) ||
          (input.previousSha256 !== undefined && receipt.sha256 !== input.previousSha256)
        )
          throw new Error('memory deletion version identity differs');
        return false;
      }
      if (prior.deletedAt !== null) return false;
      if (input.previousSha256 !== undefined && prior.currentSha256 !== input.previousSha256)
        throw new MemoryConflictError(prior.currentSha256, input.previousSha256);
      if (input.previousPath !== undefined && prior.path !== input.previousPath)
        throw new Error('memory path changed before deletion');
      const rows = await tx
        .update(memories)
        .set({ deletedAt: new Date(), updatedAt: new Date() })
        .where(
          and(
            eq(memories.workspaceId, input.workspaceId),
            eq(memories.storeId, input.storeId),
            eq(memories.id, input.memoryId),
            isNull(memories.deletedAt),
          ),
        )
        .returning();
      const memory = rows[0];
      if (!memory) return false;
      await tx.insert(memoryVersions).values({
        id: input.versionId,
        workspaceId: input.workspaceId,
        storeId: input.storeId,
        memoryId: input.memoryId,
        path: memory.path,
        sha256: memory.currentSha256,
        sizeBytes: memory.sizeBytes,
        writtenBySessionId: input.writtenBySessionId ?? null,
        writtenByApiKeyId: input.writtenByApiKeyId ?? null,
        writtenByUserId: input.writtenByUserId ?? null,
        writtenByEventId: input.writtenByEventId ?? null,
      });
      return true;
    });
  }

  async listVersions(
    workspaceId: string,
    storeId: string,
    memoryId: string,
  ): Promise<MemoryVersionRecord[]> {
    const rows = await this.db
      .select()
      .from(memoryVersions)
      .where(
        and(
          isNull(memoryVersions.deletedAt),
          eq(memoryVersions.workspaceId, workspaceId),
          eq(memoryVersions.storeId, storeId),
          eq(memoryVersions.memoryId, memoryId),
        ),
      )
      .orderBy(desc(memoryVersions.writtenAt));
    return rows.map(rowToVersion);
  }

  async listAllVersions(
    workspaceId: string,
    storeId: string,
    opts?: MemoryIdsFilter,
  ): Promise<MemoryVersionRecord[]> {
    if (opts?.memoryIds?.length === 0) return [];
    const rows = await this.db
      .select()
      .from(memoryVersions)
      .where(
        and(
          isNull(memoryVersions.deletedAt),
          eq(memoryVersions.workspaceId, workspaceId),
          eq(memoryVersions.storeId, storeId),
          opts?.memoryIds === undefined
            ? undefined
            : inArray(memoryVersions.memoryId, [...opts.memoryIds]),
        ),
      )
      .orderBy(desc(memoryVersions.writtenAt));
    return rows.map(rowToVersion);
  }

  async getVersion(
    workspaceId: string,
    storeId: string,
    versionId: string,
  ): Promise<MemoryVersionRecord | null> {
    const rows = await this.db
      .select()
      .from(memoryVersions)
      .where(
        and(
          isNull(memoryVersions.deletedAt),
          eq(memoryVersions.workspaceId, workspaceId),
          eq(memoryVersions.storeId, storeId),
          eq(memoryVersions.id, versionId),
        ),
      )
      .limit(1);
    return rows[0] ? rowToVersion(rows[0]) : null;
  }

  async markVersionRedacted(input: RedactMemoryVersionInput): Promise<MemoryVersionRecord | null> {
    return await this.db.transaction(async (tx) => {
      const existing = await tx
        .select()
        .from(memoryVersions)
        .where(
          and(
            isNull(memoryVersions.deletedAt),
            eq(memoryVersions.workspaceId, input.workspaceId),
            eq(memoryVersions.storeId, input.storeId),
            eq(memoryVersions.id, input.versionId),
          ),
        )
        .limit(1);
      const row = existing[0];
      if (!row) return null;
      // Idempotent: don't re-stamp redactedAt; preserve the original timestamp.
      if (row.redactedAt) return rowToVersion(row);
      const updated = await tx
        .update(memoryVersions)
        .set({
          redactedAt: new Date(),
          redactedBySessionId: input.redactedBySessionId ?? null,
          redactedByApiKeyId: input.redactedByApiKeyId ?? null,
          redactedByUserId: input.redactedByUserId ?? null,
        })
        .where(
          and(
            isNull(memoryVersions.deletedAt),
            eq(memoryVersions.workspaceId, input.workspaceId),
            eq(memoryVersions.storeId, input.storeId),
            eq(memoryVersions.id, input.versionId),
          ),
        )
        .returning();
      const updatedRow = updated[0];
      if (!updatedRow) {
        throw new Error(`markVersionRedacted: update returned no row for ${input.versionId}`);
      }
      return rowToVersion(updatedRow);
    });
  }

  async close(): Promise<void> {
    // Pool is owned by the caller (matches PostgresFileMetadataStore); we
    // don't end it here.
  }
}

/**
 * Apply Drizzle migrations from the package's `migrations` directory.
 * Used by tests + service startup. Idempotent: drizzle tracks applied
 * migrations in `__drizzle_migrations`.
 *
 * The candidate-list mirrors `@orca/file-store`'s `applyMigrations` so the
 * helper works identically whether the caller imports from `src/` (vitest)
 * or `dist/` (built).
 */
export async function applyMigrations(pool: Pool): Promise<void> {
  const here = dirname(fileURLToPath(import.meta.url));
  const candidates = [
    resolve(here, 'migrations'), // src/metadata/{this file} -> src/metadata/migrations
    resolve(here, '../src/metadata/migrations'), // dist/ -> src/metadata/migrations
    resolve(here, '../metadata/migrations'), // legacy fallback
  ];
  const migrationsFolder = candidates.find((p) => existsSync(p));
  if (!migrationsFolder) {
    throw new Error(
      `applyMigrations: cannot find migrations folder; tried: ${candidates.join(', ')}`,
    );
  }
  // registry runs this at boot with replicaCount >= 2, so two instances race the
  // same DDL. Serialize with a session-scoped advisory lock held on a dedicated
  // client across the whole migrate() span (drizzle's migrator takes no lock of
  // its own); binding drizzle to that client keeps lock + DDL on one session.
  // The lock is per-database, contending only within this `memorystore` DB.
  const client = await pool.connect();
  try {
    await client.query("SELECT pg_advisory_lock(hashtext('orca_memory_store_migrations'))");
    await migrate(drizzle(client), { migrationsFolder });
  } finally {
    try {
      await client.query("SELECT pg_advisory_unlock(hashtext('orca_memory_store_migrations'))");
    } finally {
      client.release();
    }
  }
}
