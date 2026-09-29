// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export interface MemoryStoreRecord {
  id: string; // mems_…
  workspaceId: string;
  name: string;
  description: string | null;
  metadata: Record<string, string>;
  archivedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface MemoryRecord {
  id: string; // mem_…
  storeId: string;
  path: string;
  currentSha256: string;
  sizeBytes: number;
  updatedAt: Date;
  updatedBySessionId: string | null;
  updatedByEventId: string | null;
}

export interface MemoryVersionRecord {
  id: string; // memver_…
  storeId: string;
  memoryId: string;
  /** Denormalized from `memories.path` so redact-by-path queries don't need a join. */
  path: string;
  sha256: string;
  sizeBytes: number;
  writtenBySessionId: string | null;
  writtenByApiKeyId: string | null;
  writtenByUserId: string | null;
  writtenByEventId: string | null;
  writtenAt: Date;
  /** Null = not redacted. Set by the redact API. */
  redactedAt: Date | null;
  redactedBySessionId: string | null;
  redactedByApiKeyId: string | null;
  redactedByUserId: string | null;
}

export interface MemoryActorAttribution {
  sessionId?: string;
  apiKeyId?: string;
  userId?: string;
}

/** Internal checkpoint identity and CAS for retry-safe filesystem deletions. */
export interface DeleteMemoryOptions extends MemoryActorAttribution {
  versionId?: string;
  previousSha256?: string;
  previousPath?: string;
  writtenByEventId?: string;
}

export interface CreateStoreInput {
  workspaceId: string;
  name: string;
  description?: string;
  metadata?: Record<string, string>;
}

export interface UpdateStoreInput {
  workspaceId: string;
  storeId: string;
  name?: string;
  description?: string | null;
  metadata?: Record<string, string>;
}

export interface WriteMemoryInput {
  /** Workspace owning `storeId`. Threaded into blob keys so a single
   * blob-store instance can serve every workspace in the registry. */
  workspaceId: string;
  storeId: string;
  /** Existing memory to update. When set, path changes are treated as a
   * rename and preserve this memory id. Omit for create/path-upsert writes
   * from the internal watcher. */
  memoryId?: string;
  /** Fail if an active memory already owns `path`. Public creates set this;
   * internal filesystem writes retain their path-upsert behavior. */
  createOnly?: boolean;
  path: string;
  /** Streaming bytes; the store re-hashes for safety. */
  content: NodeJS.ReadableStream;
  sizeBytes: number;
  /** Caller-supplied digest. The store verifies on the wire. */
  sha256: string;
  /** Optimistic CAS: when set, write fails (MemoryConflictError) if memories.current_sha256 ≠ this. Null on first write of a path. */
  previousSha256?: string;
  /** Optional caller-owned idempotency key for internal retry-safe version writes. */
  versionId?: string;
  writtenBySessionId?: string;
  writtenByApiKeyId?: string;
  writtenByUserId?: string;
  writtenByEventId?: string;
}
