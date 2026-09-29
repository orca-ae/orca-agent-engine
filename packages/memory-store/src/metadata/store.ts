// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { MemoryStoreRecord, MemoryRecord, MemoryVersionRecord } from '../types.js';
import type { ListStoresOptions, MemoryIdsFilter } from '../store.js';

/**
 * Metadata layer interface. Implementations include Postgres (production /
 * CI) and InMemory (unit tests + harness's LocalMemoryStrategy fallback).
 *
 * The interface is transactional at the upsertMemory boundary: each successful
 * call atomically updates the memories row AND inserts a memory_versions row.
 * Implementations must enforce this in a single SQL transaction.
 */
export interface UpsertMemoryInput {
  /** Pre-generated memory id (mem_…). Implementations MUST NOT generate ids;
   * callers (LocalMemoryStore) own the id-generation policy. */
  id: string;
  /** Pre-generated version id (memver_…). */
  versionId: string;
  workspaceId: string;
  storeId: string;
  memoryId?: string;
  createOnly?: boolean;
  path: string;
  sha256: string;
  sizeBytes: number;
  /** When set, upsert fails (returns null + the observed sha) if the current
   * memories.current_sha256 doesn't match. Null on the first write to a path. */
  previousSha256?: string;
  writtenBySessionId?: string;
  writtenByApiKeyId?: string;
  writtenByUserId?: string;
  writtenByEventId?: string;
}

export interface DeleteMemoryInput {
  workspaceId: string;
  storeId: string;
  memoryId: string;
  versionId: string;
  previousSha256?: string;
  previousPath?: string;
  writtenBySessionId?: string;
  writtenByApiKeyId?: string;
  writtenByUserId?: string;
  writtenByEventId?: string;
}

export interface RedactMemoryVersionInput {
  workspaceId: string;
  storeId: string;
  versionId: string;
  redactedBySessionId?: string;
  redactedByApiKeyId?: string;
  redactedByUserId?: string;
}

export type UpsertMemoryResult =
  | { ok: true; memory: MemoryRecord; version: MemoryVersionRecord }
  | { ok: false; observedSha: string; expectedSha: string };

export type InsertStoreInput = Omit<MemoryStoreRecord, 'createdAt' | 'updatedAt' | 'metadata'> & {
  metadata?: Record<string, string>;
};

export interface MemoryMetadataStore {
  /** Coordinate metadata and blob mutations across writers for one Memory store. */
  withStoreWriteLock<T>(workspaceId: string, storeId: string, work: () => Promise<T>): Promise<T>;
  // store CRUD
  insertStore(record: InsertStoreInput): Promise<MemoryStoreRecord>;
  listStores(
    workspaceId: string,
    opts?: ListStoresOptions,
  ): Promise<{ items: MemoryStoreRecord[]; nextCursor: string | null }>;
  getStore(workspaceId: string, storeId: string): Promise<MemoryStoreRecord | null>;
  updateStore(
    workspaceId: string,
    storeId: string,
    updates: { name?: string; description?: string | null; metadata?: Record<string, string> },
  ): Promise<MemoryStoreRecord | null>;
  archiveStore(workspaceId: string, storeId: string): Promise<void>;
  deleteStore(workspaceId: string, storeId: string): Promise<void>;

  // memory + version (transactional)
  upsertMemory(input: UpsertMemoryInput): Promise<UpsertMemoryResult>;

  // memory queries
  getMemory(workspaceId: string, storeId: string, memoryId: string): Promise<MemoryRecord | null>;
  getMemoryByPath(workspaceId: string, storeId: string, path: string): Promise<MemoryRecord | null>;
  listMemories(
    workspaceId: string,
    storeId: string,
    opts?: MemoryIdsFilter,
  ): Promise<MemoryRecord[]>;
  /** Deletes the live memory row while preserving historical version rows. */
  deleteMemory(input: DeleteMemoryInput): Promise<boolean>;

  // version queries
  listVersions(
    workspaceId: string,
    storeId: string,
    memoryId: string,
  ): Promise<MemoryVersionRecord[]>;
  listAllVersions(
    workspaceId: string,
    storeId: string,
    opts?: MemoryIdsFilter,
  ): Promise<MemoryVersionRecord[]>;
  getVersion(
    workspaceId: string,
    storeId: string,
    versionId: string,
  ): Promise<MemoryVersionRecord | null>;
  /** Idempotent: returns the existing record if already redacted (with the
   * earlier `redactedAt` timestamp preserved). */
  markVersionRedacted(input: RedactMemoryVersionInput): Promise<MemoryVersionRecord | null>;

  close?(): Promise<void>;
}
