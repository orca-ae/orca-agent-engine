// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { MemoryRecord, MemoryStoreRecord, MemoryVersionRecord } from '../types.js';
import { AsyncLocalStorage } from 'node:async_hooks';
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

/**
 * Test double + harness InMemory fallback. Mirrors the `PostgresMemoryMetadataStore`
 * semantics exactly — the upsertMemory CAS check is internally locked via
 * synchronous JS execution (single-threaded), so the equivalent of the
 * Postgres transaction is just running the read + write + version-insert in
 * one async tick without awaiting between the read and the writes.
 *
 * Not safe for production: rows live in heap memory, no persistence, and
 * cross-process concurrency is not handled.
 */
export class InMemoryMemoryMetadataStore implements MemoryMetadataStore {
  private readonly writeLocks = new Map<string, Promise<void>>();
  private readonly writeContext = new AsyncLocalStorage<string>();

  async withStoreWriteLock<T>(
    workspaceId: string,
    storeId: string,
    work: () => Promise<T>,
  ): Promise<T> {
    const key = JSON.stringify([workspaceId, storeId]);
    if (this.writeContext.getStore() === key) return work();
    if (this.writeContext.getStore() !== undefined)
      throw new Error('nested Memory store lock differs');
    const previous = this.writeLocks.get(key) ?? Promise.resolve();
    let release!: () => void;
    const next = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.writeLocks.set(key, next);
    await previous;
    try {
      return await this.writeContext.run(key, work);
    } finally {
      release();
      if (this.writeLocks.get(key) === next) this.writeLocks.delete(key);
    }
  }
  private readonly deletedStoreAt = new Map<string, Date>();
  private readonly deletedVersionAt = new Map<string, Date>();
  private readonly stores = new Map<string, MemoryStoreRecord>();
  private readonly memoryRows = new Map<string, MemoryRecord>();
  private readonly versionRows = new Map<string, MemoryVersionRecord>();
  private readonly memoryWorkspaces = new Map<string, string>();
  private readonly versionWorkspaces = new Map<string, string>();
  private readonly deletedMemoryIds = new Set<string>();
  /** Monotonic counter for ordering version rows when their writtenAt
   * timestamps collide (Date.now() resolution is millisecond-coarse). */
  private versionSeq = 0;
  /** Per-version sequence number to enable deterministic descending order. */
  private readonly versionOrder = new Map<string, number>();

  async insertStore(record: InsertStoreInput): Promise<MemoryStoreRecord> {
    const now = new Date();
    const stored: MemoryStoreRecord = {
      id: record.id,
      workspaceId: record.workspaceId,
      name: record.name,
      description: record.description,
      metadata: cloneMetadata(record.metadata),
      archivedAt: record.archivedAt,
      createdAt: now,
      updatedAt: now,
    };
    this.stores.set(record.id, stored);
    return cloneStore(stored);
  }

  async listStores(
    workspaceId: string,
    opts?: ListStoresOptions,
  ): Promise<{ items: MemoryStoreRecord[]; nextCursor: string | null }> {
    const limit = opts?.limit ?? 100;
    const cursor = opts?.cursor;
    const offset = opts?.offset ?? 0;
    // Sort ascending by id for stable cursor pagination — matches the
    // Postgres path's `ORDER BY id ASC`.
    const candidates = Array.from(this.stores.values())
      .filter(
        (s) =>
          s.workspaceId === workspaceId &&
          !this.deletedStoreAt.has(s.id) &&
          (!cursor || s.id > cursor) &&
          (opts?.includeArchived !== false || s.archivedAt === null) &&
          (opts?.createdAtGte === undefined || s.createdAt >= opts.createdAtGte) &&
          (opts?.createdAtLte === undefined || s.createdAt <= opts.createdAtLte),
      )
      .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    const window = candidates.slice(offset, offset + limit + 1);
    const items = window.slice(0, limit).map(cloneStore);
    const nextCursor = window.length > limit ? items[items.length - 1]!.id : null;
    return { items, nextCursor };
  }

  async getStore(workspaceId: string, storeId: string): Promise<MemoryStoreRecord | null> {
    const row = this.stores.get(storeId);
    if (!row || row.workspaceId !== workspaceId || this.deletedStoreAt.has(storeId)) return null;
    return cloneStore(row);
  }

  async updateStore(
    workspaceId: string,
    storeId: string,
    updates: { name?: string; description?: string | null; metadata?: Record<string, string> },
  ): Promise<MemoryStoreRecord | null> {
    const row = this.stores.get(storeId);
    if (!row || row.workspaceId !== workspaceId || this.deletedStoreAt.has(storeId)) return null;
    const updated: MemoryStoreRecord = {
      ...row,
      ...(updates.name !== undefined ? { name: updates.name } : {}),
      ...(updates.description !== undefined ? { description: updates.description } : {}),
      ...(updates.metadata !== undefined ? { metadata: cloneMetadata(updates.metadata) } : {}),
      updatedAt: new Date(),
    };
    this.stores.set(storeId, updated);
    return cloneStore(updated);
  }

  async archiveStore(workspaceId: string, storeId: string): Promise<void> {
    return this.withStoreWriteLock(workspaceId, storeId, () =>
      this.archiveStoreLocked(workspaceId, storeId),
    );
  }

  private async archiveStoreLocked(workspaceId: string, storeId: string): Promise<void> {
    const row = this.stores.get(storeId);
    if (!row || row.workspaceId !== workspaceId || this.deletedStoreAt.has(storeId)) return;
    const now = new Date();
    this.stores.set(storeId, { ...row, archivedAt: now, updatedAt: now });
  }

  async deleteStore(workspaceId: string, storeId: string): Promise<void> {
    return this.withStoreWriteLock(workspaceId, storeId, () =>
      this.deleteStoreLocked(workspaceId, storeId),
    );
  }

  private async deleteStoreLocked(workspaceId: string, storeId: string): Promise<void> {
    const row = this.stores.get(storeId);
    if (!row || row.workspaceId !== workspaceId || this.deletedStoreAt.has(storeId)) return;
    const now = new Date();
    this.deletedStoreAt.set(storeId, now);
    for (const memory of this.memoryRows.values()) {
      if (this.memoryWorkspaces.get(memory.id) === workspaceId && memory.storeId === storeId) {
        this.deletedMemoryIds.add(memory.id);
      }
    }
    for (const version of this.versionRows.values()) {
      if (this.versionWorkspaces.get(version.id) === workspaceId && version.storeId === storeId) {
        this.deletedVersionAt.set(version.id, now);
      }
    }
  }

  async upsertMemory(input: UpsertMemoryInput): Promise<UpsertMemoryResult> {
    return this.withStoreWriteLock(input.workspaceId, input.storeId, () =>
      this.upsertMemoryLocked(input),
    );
  }

  private async upsertMemoryLocked(input: UpsertMemoryInput): Promise<UpsertMemoryResult> {
    const parent = this.stores.get(input.storeId);
    if (
      !parent ||
      parent.workspaceId !== input.workspaceId ||
      this.deletedStoreAt.has(input.storeId)
    ) {
      throw new Error(
        `upsertMemory: store ${input.storeId} not found in workspace ${input.workspaceId}`,
      );
    }
    const existingVersion = this.versionRows.get(input.versionId);
    if (
      existingVersion?.storeId === input.storeId &&
      this.versionWorkspaces.get(input.versionId) === input.workspaceId
    ) {
      const existingMemory = this.memoryRows.get(existingVersion.memoryId);
      if (
        !existingMemory ||
        this.memoryWorkspaces.get(existingVersion.memoryId) !== input.workspaceId
      ) {
        throw new Error(`upsertMemory: version ${input.versionId} has no memory row`);
      }
      return {
        ok: true,
        memory: cloneMemory(existingMemory),
        version: cloneVersion(existingVersion),
      };
    }
    if (existingVersion) {
      throw new Error(
        `upsertMemory: version ${input.versionId} already exists in store ${existingVersion.storeId}`,
      );
    }

    // Find existing memory by (storeId, path). Tied path-lookup mirrors the
    // unique index in Postgres.
    const existing = input.memoryId
      ? this.memoryRows.get(input.memoryId)
      : this.findMemoryByPath(input.workspaceId, input.storeId, input.path, false);
    const now = new Date();

    if (input.memoryId && !existing) {
      throw new Error(`upsertMemory: memory ${input.memoryId} not found`);
    }

    if (existing) {
      if (
        this.memoryWorkspaces.get(existing.id) !== input.workspaceId ||
        existing.storeId !== input.storeId ||
        this.deletedMemoryIds.has(existing.id)
      ) {
        throw new Error(`upsertMemory: memory ${existing.id} not found`);
      }
      if (input.createOnly) {
        const error = new Error(`memory path already exists: ${input.path}`) as Error & {
          code?: string;
          conflictingMemoryId?: string;
        };
        error.code = '23505';
        error.conflictingMemoryId = existing.id;
        throw error;
      }
      const pathOwner = this.findMemoryByPath(input.workspaceId, input.storeId, input.path, false);
      if (pathOwner && pathOwner.id !== existing.id) {
        const error = new Error(`memory path already exists: ${input.path}`) as Error & {
          code?: string;
        };
        error.code = '23505';
        throw error;
      }
      if (input.previousSha256 !== undefined && input.previousSha256 !== existing.currentSha256) {
        return {
          ok: false,
          observedSha: existing.currentSha256,
          expectedSha: input.previousSha256,
        };
      }
      const updated: MemoryRecord = {
        ...existing,
        path: input.path,
        currentSha256: input.sha256,
        sizeBytes: input.sizeBytes,
        updatedAt: now,
        updatedBySessionId: input.writtenBySessionId ?? null,
        updatedByEventId: input.writtenByEventId ?? null,
      };
      this.memoryRows.set(existing.id, updated);
      this.deletedMemoryIds.delete(existing.id);
      const version: MemoryVersionRecord = {
        id: input.versionId,
        storeId: input.storeId,
        memoryId: existing.id,
        path: input.path,
        sha256: input.sha256,
        sizeBytes: input.sizeBytes,
        writtenBySessionId: input.writtenBySessionId ?? null,
        writtenByApiKeyId: input.writtenByApiKeyId ?? null,
        writtenByUserId: input.writtenByUserId ?? null,
        writtenByEventId: input.writtenByEventId ?? null,
        writtenAt: now,
        redactedAt: null,
        redactedBySessionId: null,
        redactedByApiKeyId: null,
        redactedByUserId: null,
      };
      this.versionRows.set(version.id, version);
      this.versionWorkspaces.set(version.id, input.workspaceId);
      this.versionOrder.set(version.id, ++this.versionSeq);
      return {
        ok: true,
        memory: cloneMemory(updated),
        version: cloneVersion(version),
      };
    }

    if (input.previousSha256 !== undefined) {
      // Mirrors the Postgres path: first-write with previousSha is a
      // mismatch, observedSha is empty.
      return { ok: false, observedSha: '', expectedSha: input.previousSha256 };
    }

    const memory: MemoryRecord = {
      id: input.id,
      storeId: input.storeId,
      path: input.path,
      currentSha256: input.sha256,
      sizeBytes: input.sizeBytes,
      updatedAt: now,
      updatedBySessionId: input.writtenBySessionId ?? null,
      updatedByEventId: input.writtenByEventId ?? null,
    };
    this.memoryRows.set(memory.id, memory);
    this.memoryWorkspaces.set(memory.id, input.workspaceId);
    const version: MemoryVersionRecord = {
      id: input.versionId,
      storeId: input.storeId,
      memoryId: memory.id,
      path: input.path,
      sha256: input.sha256,
      sizeBytes: input.sizeBytes,
      writtenBySessionId: input.writtenBySessionId ?? null,
      writtenByApiKeyId: input.writtenByApiKeyId ?? null,
      writtenByUserId: input.writtenByUserId ?? null,
      writtenByEventId: input.writtenByEventId ?? null,
      writtenAt: now,
      redactedAt: null,
      redactedBySessionId: null,
      redactedByApiKeyId: null,
      redactedByUserId: null,
    };
    this.versionRows.set(version.id, version);
    this.versionWorkspaces.set(version.id, input.workspaceId);
    this.versionOrder.set(version.id, ++this.versionSeq);
    return {
      ok: true,
      memory: cloneMemory(memory),
      version: cloneVersion(version),
    };
  }

  async getMemory(
    workspaceId: string,
    storeId: string,
    memoryId: string,
  ): Promise<MemoryRecord | null> {
    const row = this.memoryRows.get(memoryId);
    if (
      !row ||
      this.memoryWorkspaces.get(memoryId) !== workspaceId ||
      row.storeId !== storeId ||
      this.deletedMemoryIds.has(memoryId)
    ) {
      return null;
    }
    return cloneMemory(row);
  }

  async getMemoryByPath(
    workspaceId: string,
    storeId: string,
    path: string,
  ): Promise<MemoryRecord | null> {
    const row = this.findMemoryByPath(workspaceId, storeId, path, false);
    return row ? cloneMemory(row) : null;
  }

  async listMemories(
    workspaceId: string,
    storeId: string,
    opts?: MemoryIdsFilter,
  ): Promise<MemoryRecord[]> {
    const ids = opts?.memoryIds === undefined ? null : new Set(opts.memoryIds);
    return Array.from(this.memoryRows.values())
      .filter(
        (r) =>
          this.memoryWorkspaces.get(r.id) === workspaceId &&
          r.storeId === storeId &&
          (ids === null || ids.has(r.id)) &&
          !this.deletedMemoryIds.has(r.id),
      )
      .sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0))
      .map(cloneMemory);
  }

  async deleteMemory(input: DeleteMemoryInput): Promise<boolean> {
    return this.withStoreWriteLock(input.workspaceId, input.storeId, () =>
      this.deleteMemoryLocked(input),
    );
  }

  private async deleteMemoryLocked(input: DeleteMemoryInput): Promise<boolean> {
    const receipt = this.versionRows.get(input.versionId);
    if (receipt) {
      if (
        this.versionWorkspaces.get(input.versionId) !== input.workspaceId ||
        receipt.storeId !== input.storeId ||
        receipt.memoryId !== input.memoryId ||
        receipt.writtenBySessionId !== (input.writtenBySessionId ?? null) ||
        receipt.writtenByEventId !== (input.writtenByEventId ?? null) ||
        receipt.writtenByApiKeyId !== (input.writtenByApiKeyId ?? null) ||
        receipt.writtenByUserId !== (input.writtenByUserId ?? null) ||
        (input.previousSha256 !== undefined && receipt.sha256 !== input.previousSha256) ||
        !this.deletedMemoryIds.has(input.memoryId)
      )
        throw new Error('memory deletion version identity differs');
      return false;
    }
    const row = this.memoryRows.get(input.memoryId);
    if (
      !row ||
      this.memoryWorkspaces.get(input.memoryId) !== input.workspaceId ||
      row.storeId !== input.storeId ||
      this.deletedMemoryIds.has(input.memoryId)
    ) {
      return false;
    }
    if (input.previousSha256 !== undefined && row.currentSha256 !== input.previousSha256)
      throw new MemoryConflictError(row.currentSha256, input.previousSha256);
    if (input.previousPath !== undefined && row.path !== input.previousPath)
      throw new Error('memory path changed before deletion');
    const version: MemoryVersionRecord = {
      id: input.versionId,
      storeId: input.storeId,
      memoryId: input.memoryId,
      path: row.path,
      sha256: row.currentSha256,
      sizeBytes: row.sizeBytes,
      writtenBySessionId: input.writtenBySessionId ?? null,
      writtenByApiKeyId: input.writtenByApiKeyId ?? null,
      writtenByUserId: input.writtenByUserId ?? null,
      writtenByEventId: input.writtenByEventId ?? null,
      writtenAt: new Date(),
      redactedAt: null,
      redactedBySessionId: null,
      redactedByApiKeyId: null,
      redactedByUserId: null,
    };
    this.versionRows.set(version.id, version);
    this.versionWorkspaces.set(version.id, input.workspaceId);
    this.versionOrder.set(version.id, ++this.versionSeq);
    this.deletedMemoryIds.add(input.memoryId);
    return true;
  }

  async listVersions(
    workspaceId: string,
    storeId: string,
    memoryId: string,
  ): Promise<MemoryVersionRecord[]> {
    return Array.from(this.versionRows.values())
      .filter(
        (v) =>
          this.versionWorkspaces.get(v.id) === workspaceId &&
          !this.deletedVersionAt.has(v.id) &&
          v.storeId === storeId &&
          v.memoryId === memoryId,
      )
      .sort((a, b) => {
        // DESC by writtenAt, falling back to the per-version sequence number
        // so siblings inserted within the same millisecond still order
        // last-write-first deterministically.
        const tDelta = b.writtenAt.getTime() - a.writtenAt.getTime();
        if (tDelta !== 0) return tDelta;
        return (this.versionOrder.get(b.id) ?? 0) - (this.versionOrder.get(a.id) ?? 0);
      })
      .map(cloneVersion);
  }

  async listAllVersions(
    workspaceId: string,
    storeId: string,
    opts?: MemoryIdsFilter,
  ): Promise<MemoryVersionRecord[]> {
    const ids = opts?.memoryIds === undefined ? null : new Set(opts.memoryIds);
    return Array.from(this.versionRows.values())
      .filter(
        (version) =>
          this.versionWorkspaces.get(version.id) === workspaceId &&
          version.storeId === storeId &&
          (ids === null || ids.has(version.memoryId)) &&
          !this.deletedVersionAt.has(version.id),
      )
      .sort((a, b) => {
        const time = b.writtenAt.getTime() - a.writtenAt.getTime();
        return time || (this.versionOrder.get(b.id) ?? 0) - (this.versionOrder.get(a.id) ?? 0);
      })
      .map(cloneVersion);
  }

  async getVersion(
    workspaceId: string,
    storeId: string,
    versionId: string,
  ): Promise<MemoryVersionRecord | null> {
    const row = this.versionRows.get(versionId);
    if (
      !row ||
      this.deletedVersionAt.has(versionId) ||
      this.versionWorkspaces.get(versionId) !== workspaceId ||
      row.storeId !== storeId
    ) {
      return null;
    }
    return cloneVersion(row);
  }

  async markVersionRedacted(input: RedactMemoryVersionInput): Promise<MemoryVersionRecord | null> {
    const row = this.versionRows.get(input.versionId);
    if (
      !row ||
      this.versionWorkspaces.get(input.versionId) !== input.workspaceId ||
      this.deletedVersionAt.has(input.versionId) ||
      row.storeId !== input.storeId
    ) {
      return null;
    }
    if (row.redactedAt) return cloneVersion(row);
    const updated: MemoryVersionRecord = {
      ...row,
      redactedAt: new Date(),
      redactedBySessionId: input.redactedBySessionId ?? null,
      redactedByApiKeyId: input.redactedByApiKeyId ?? null,
      redactedByUserId: input.redactedByUserId ?? null,
    };
    this.versionRows.set(input.versionId, updated);
    return cloneVersion(updated);
  }

  async close(): Promise<void> {
    /* no-op */
  }

  /** Test helper: drop all rows. */
  clear(): void {
    this.stores.clear();
    this.deletedStoreAt.clear();
    this.deletedVersionAt.clear();
    this.memoryRows.clear();
    this.versionRows.clear();
    this.memoryWorkspaces.clear();
    this.versionWorkspaces.clear();
    this.deletedMemoryIds.clear();
    this.versionOrder.clear();
    this.versionSeq = 0;
  }

  private findMemoryByPath(
    workspaceId: string,
    storeId: string,
    path: string,
    includeDeleted: boolean,
  ): MemoryRecord | undefined {
    for (const row of this.memoryRows.values()) {
      if (
        this.memoryWorkspaces.get(row.id) === workspaceId &&
        row.storeId === storeId &&
        row.path === path &&
        (includeDeleted || !this.deletedMemoryIds.has(row.id))
      ) {
        return row;
      }
    }
    return undefined;
  }
}

function cloneStore(r: MemoryStoreRecord): MemoryStoreRecord {
  return { ...r, metadata: cloneMetadata(r.metadata) };
}

function cloneMetadata(source?: Record<string, string>): Record<string, string> {
  const metadata = Object.create(null) as Record<string, string>;
  if (!source) return metadata;
  for (const [key, value] of Object.entries(source)) {
    metadata[key] = value;
  }
  return metadata;
}

function cloneMemory(r: MemoryRecord): MemoryRecord {
  return { ...r };
}

function cloneVersion(r: MemoryVersionRecord): MemoryVersionRecord {
  return { ...r };
}
