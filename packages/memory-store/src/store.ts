// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type {
  MemoryStoreRecord,
  MemoryRecord,
  MemoryVersionRecord,
  CreateStoreInput,
  MemoryActorAttribution,
  DeleteMemoryOptions,
  UpdateStoreInput,
  WriteMemoryInput,
} from './types.js';

/**
 * Thrown by `MemoryStore.writeMemory` (and the underlying metadata layer) when
 * the caller's `previousSha256` doesn't match the current memory's
 * `current_sha256`. The harness's version watcher catches this and retries
 * without precondition (last-writer-wins); a
 * `session.memory_conflict` event is emitted to the transcript stream so the
 * SDK can surface conflicts to client code. See
 * docs/managed-agents/memory-conflict-semantics.md for the full semantics.
 */
export class MemoryConflictError extends Error {
  constructor(
    public readonly observedSha: string,
    public readonly expectedSha: string,
  ) {
    super(`memory CAS failed: observed=${observedSha} expected=${expectedSha}`);
    this.name = 'MemoryConflictError';
  }
}

export interface OpenMemory {
  stream: NodeJS.ReadableStream;
  sizeBytes: number;
  sha256: string;
}

export interface ListStoresOptions {
  limit?: number;
  cursor?: string;
  offset?: number;
  includeArchived?: boolean;
  createdAtGte?: Date;
  createdAtLte?: Date;
}

/** Optional metadata narrowing; an empty ID list returns no rows. */
export interface MemoryIdsFilter {
  memoryIds?: readonly string[];
}

export interface MemoryStore {
  // ---- store-level CRUD ---------------------------------------------------
  createStore(input: CreateStoreInput): Promise<MemoryStoreRecord>;
  listStores(
    workspaceId: string,
    opts?: ListStoresOptions,
  ): Promise<{ items: MemoryStoreRecord[]; nextCursor: string | null }>;
  getStore(workspaceId: string, storeId: string): Promise<MemoryStoreRecord | null>;
  updateStore(input: UpdateStoreInput): Promise<MemoryStoreRecord | null>;
  archiveStore(workspaceId: string, storeId: string): Promise<void>;
  deleteStore(workspaceId: string, storeId: string): Promise<void>;

  // ---- memory-level CRUD --------------------------------------------------
  getMemory(workspaceId: string, storeId: string, memoryId: string): Promise<MemoryRecord | null>;
  /** Path-based lookup. Workspace scope is enforced via storeId membership. */
  getMemoryByPath(workspaceId: string, storeId: string, path: string): Promise<MemoryRecord | null>;
  listMemories(
    workspaceId: string,
    storeId: string,
    opts?: MemoryIdsFilter,
  ): Promise<MemoryRecord[]>;
  /** Removes the current memory and live bytes. Historical versions remain addressable. */
  deleteMemory(
    workspaceId: string,
    storeId: string,
    memoryId: string,
    actor?: DeleteMemoryOptions,
  ): Promise<boolean>;
  /** Returns the bytes of the current version. Returns null when the path
   * doesn't exist. */
  openMemory(workspaceId: string, storeId: string, memoryId: string): Promise<OpenMemory | null>;
  /**
   * Persists a new memory version. Throws MemoryConflictError when
   * `previousSha256` is set and doesn't match the current state. On success
   * returns the updated memory + the new version row.
   */
  writeMemory(
    input: WriteMemoryInput,
  ): Promise<{ memory: MemoryRecord; version: MemoryVersionRecord }>;

  // ---- version-level operations ------------------------------------------
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
  openVersion(workspaceId: string, storeId: string, versionId: string): Promise<OpenMemory | null>;
  /** Marks the version redacted with actor attribution. Idempotent against
   * already-redacted rows: returns the original timestamp and actor. */
  redactVersion(
    workspaceId: string,
    storeId: string,
    versionId: string,
    actor?: MemoryActorAttribution,
  ): Promise<MemoryVersionRecord>;

  close(): Promise<void>;
}
