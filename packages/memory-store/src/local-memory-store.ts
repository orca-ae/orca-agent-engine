// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import {
  assertMemoryStoreId,
  assertMemoryWorkspaceId,
  normalizeMemoryRelativePath,
} from './blob/path.js';
import type { MemoryBlobStore } from './blob/blob-store.js';
import { newId } from './ids.js';
import type { MemoryMetadataStore, UpsertMemoryInput } from './metadata/store.js';
import type { ListStoresOptions, MemoryIdsFilter, MemoryStore, OpenMemory } from './store.js';
import { MemoryConflictError } from './store.js';
import type {
  CreateStoreInput,
  DeleteMemoryOptions,
  MemoryActorAttribution,
  MemoryRecord,
  MemoryStoreRecord,
  MemoryVersionRecord,
  UpdateStoreInput,
  WriteMemoryInput,
} from './types.js';

/**
 * Anthropic's Memory Tool API caps each individual memory at ~100 KB. The
 * library enforces the cap at the API boundary so callers (registry, harness
 * watcher) don't have to duplicate the check. Larger writes are rejected
 * before we touch metadata or blob stores.
 */
const MAX_MEMORY_BYTES = 100 * 1024;

export interface LocalMemoryStoreOptions {
  blobStore: MemoryBlobStore;
  metadataStore: MemoryMetadataStore;
}

/**
 * Default `MemoryStore` implementation: composes a `MemoryBlobStore`
 * (live + sha-addressed versions) with a `MemoryMetadataStore` (Postgres
 * or InMemory). Owns the id-generation policy and the write ordering
 * (metadata commits before blob writes so a CAS failure leaves no
 * orphan blobs behind).
 */
export class LocalMemoryStore implements MemoryStore {
  constructor(private readonly opts: LocalMemoryStoreOptions) {}

  async createStore(input: CreateStoreInput): Promise<MemoryStoreRecord> {
    return this.opts.metadataStore.insertStore({
      id: newId('mems'),
      workspaceId: input.workspaceId,
      name: input.name,
      description: input.description ?? null,
      metadata: input.metadata ?? {},
      archivedAt: null,
    });
  }

  async listStores(
    workspaceId: string,
    opts?: ListStoresOptions,
  ): Promise<{ items: MemoryStoreRecord[]; nextCursor: string | null }> {
    return this.opts.metadataStore.listStores(workspaceId, {
      ...opts,
      limit: opts?.limit ?? 100,
    });
  }

  async getStore(workspaceId: string, storeId: string): Promise<MemoryStoreRecord | null> {
    return this.opts.metadataStore.getStore(workspaceId, storeId);
  }

  async updateStore(input: UpdateStoreInput): Promise<MemoryStoreRecord | null> {
    return this.opts.metadataStore.updateStore(input.workspaceId, input.storeId, {
      ...(input.name !== undefined ? { name: input.name } : {}),
      ...(input.description !== undefined ? { description: input.description } : {}),
      ...(input.metadata !== undefined ? { metadata: input.metadata } : {}),
    });
  }

  async archiveStore(workspaceId: string, storeId: string): Promise<void> {
    await this.opts.metadataStore.archiveStore(workspaceId, storeId);
  }

  async deleteStore(workspaceId: string, storeId: string): Promise<void> {
    const store = await this.opts.metadataStore.getStore(workspaceId, storeId);
    if (!store) return;
    await this.opts.metadataStore.deleteStore(workspaceId, storeId);
  }

  async getMemory(
    workspaceId: string,
    storeId: string,
    memoryId: string,
  ): Promise<MemoryRecord | null> {
    const store = await this.opts.metadataStore.getStore(workspaceId, storeId);
    if (!store) return null;
    return this.opts.metadataStore.getMemory(workspaceId, storeId, memoryId);
  }

  async getMemoryByPath(
    workspaceId: string,
    storeId: string,
    path: string,
  ): Promise<MemoryRecord | null> {
    const store = await this.opts.metadataStore.getStore(workspaceId, storeId);
    if (!store) return null;
    return this.opts.metadataStore.getMemoryByPath(workspaceId, storeId, path);
  }

  async listMemories(
    workspaceId: string,
    storeId: string,
    opts?: MemoryIdsFilter,
  ): Promise<MemoryRecord[]> {
    const store = await this.opts.metadataStore.getStore(workspaceId, storeId);
    if (!store) return [];
    return this.opts.metadataStore.listMemories(workspaceId, storeId, opts);
  }

  async deleteMemory(
    workspaceId: string,
    storeId: string,
    memoryId: string,
    actor: DeleteMemoryOptions = {},
  ): Promise<boolean> {
    return this.opts.metadataStore.withStoreWriteLock(workspaceId, storeId, () =>
      this.deleteMemoryLocked(workspaceId, storeId, memoryId, actor),
    );
  }

  private async deleteMemoryLocked(
    workspaceId: string,
    storeId: string,
    memoryId: string,
    actor: DeleteMemoryOptions,
  ): Promise<boolean> {
    const memory = await this.getMemory(workspaceId, storeId, memoryId);
    // A checkpoint may have committed its tombstone before live-blob cleanup
    // failed. Keep processing its deterministic receipt even without a live row.
    if (!memory && actor.versionId === undefined) return false;
    const versionId = actor.versionId ?? newId('memver');
    const deleted = await this.opts.metadataStore.deleteMemory({
      workspaceId,
      storeId,
      memoryId,
      versionId,
      ...(actor.previousSha256 !== undefined ? { previousSha256: actor.previousSha256 } : {}),
      ...(actor.previousPath !== undefined
        ? { previousPath: normalizeMemoryRelativePath(actor.previousPath) }
        : {}),
      ...(actor.writtenByEventId !== undefined ? { writtenByEventId: actor.writtenByEventId } : {}),
      ...(actor.sessionId !== undefined ? { writtenBySessionId: actor.sessionId } : {}),
      ...(actor.apiKeyId !== undefined ? { writtenByApiKeyId: actor.apiKeyId } : {}),
      ...(actor.userId !== undefined ? { writtenByUserId: actor.userId } : {}),
    });
    const receipt = memory ? null : await this.getVersion(workspaceId, storeId, versionId);
    const path = memory?.path ?? (receipt?.memoryId === memoryId ? receipt.path : undefined);
    if (path === undefined) return deleted;
    // The store write lock covers both this ownership check and blob cleanup.
    // An old receipt must never remove a new occurrence at the same path,
    // including when that occurrence contains exactly the same bytes.
    if (await this.opts.metadataStore.getMemoryByPath(workspaceId, storeId, path)) return deleted;
    try {
      await this.opts.blobStore.deleteLive(workspaceId, storeId, path);
    } catch (error) {
      // Checkpoints retry with the same receipt until cleanup completes. Public
      // deletion retains its existing best-effort live-blob cleanup behavior.
      if (actor.versionId !== undefined) throw error;
    }
    return deleted;
  }

  async openMemory(
    workspaceId: string,
    storeId: string,
    memoryId: string,
  ): Promise<OpenMemory | null> {
    const memory = await this.getMemory(workspaceId, storeId, memoryId);
    if (!memory) return null;
    const stream = await this.opts.blobStore.openLive(workspaceId, storeId, memory.path);
    return {
      stream,
      sizeBytes: memory.sizeBytes,
      sha256: memory.currentSha256,
    };
  }

  async writeMemory(
    input: WriteMemoryInput,
  ): Promise<{ memory: MemoryRecord; version: MemoryVersionRecord }> {
    return this.opts.metadataStore.withStoreWriteLock(input.workspaceId, input.storeId, () =>
      this.writeMemoryLocked(input),
    );
  }

  private async writeMemoryLocked(
    input: WriteMemoryInput,
  ): Promise<{ memory: MemoryRecord; version: MemoryVersionRecord }> {
    assertMemoryWorkspaceId(input.workspaceId);
    assertMemoryStoreId(input.storeId);
    const path = normalizeMemoryRelativePath(input.path);
    const previousMemory = input.memoryId
      ? await this.opts.metadataStore.getMemory(input.workspaceId, input.storeId, input.memoryId)
      : null;
    if (input.memoryId && !previousMemory) {
      throw new Error(`LocalMemoryStore.writeMemory: memory ${input.memoryId} not found`);
    }
    const store = await this.opts.metadataStore.getStore(input.workspaceId, input.storeId);
    if (!store || store.archivedAt !== null) {
      throw new Error(
        `LocalMemoryStore.writeMemory: store ${input.storeId} not found in workspace ${input.workspaceId}`,
      );
    }
    if (input.sizeBytes > MAX_MEMORY_BYTES) {
      throw new Error(
        `LocalMemoryStore.writeMemory: ${input.sizeBytes} bytes exceeds the ${MAX_MEMORY_BYTES} byte (100 KB) per-memory cap`,
      );
    }
    // Buffer + verify the sha. We need the full bytes for both the live PUT
    // and the .versions/{sha} PUT, so streaming twice from the caller isn't
    // viable — and at 100 KB the buffer is cheap.
    const buf = await streamToBuffer(input.content);
    if (buf.length !== input.sizeBytes) {
      throw new Error(
        `LocalMemoryStore.writeMemory: content length ${buf.length} != caller-supplied sizeBytes ${input.sizeBytes}`,
      );
    }
    if (buf.length > MAX_MEMORY_BYTES) {
      // Defensive: caller could supply a stream that doesn't match
      // sizeBytes upward. This is the same error as above but reached via
      // the actual byte count.
      throw new Error(
        `LocalMemoryStore.writeMemory: content (${buf.length} bytes) exceeds the ${MAX_MEMORY_BYTES} byte (100 KB) per-memory cap`,
      );
    }
    const observed = createHash('sha256').update(buf).digest('hex');
    if (observed !== input.sha256) {
      throw new Error(
        `LocalMemoryStore.writeMemory: content sha256 ${observed} != caller-supplied sha256 ${input.sha256}`,
      );
    }

    // Atomically reserve memory + version rows BEFORE writing blobs. A CAS
    // failure throws here so we never leak orphan blobs.
    const upsertInput: UpsertMemoryInput = {
      id: newId('mem'),
      versionId: input.versionId ?? newId('memver'),
      workspaceId: input.workspaceId,
      storeId: input.storeId,
      ...(input.memoryId !== undefined ? { memoryId: input.memoryId } : {}),
      ...(input.createOnly !== undefined ? { createOnly: input.createOnly } : {}),
      path,
      sha256: input.sha256,
      sizeBytes: input.sizeBytes,
      ...(input.previousSha256 !== undefined ? { previousSha256: input.previousSha256 } : {}),
      ...(input.writtenBySessionId !== undefined
        ? { writtenBySessionId: input.writtenBySessionId }
        : {}),
      ...(input.writtenByApiKeyId !== undefined
        ? { writtenByApiKeyId: input.writtenByApiKeyId }
        : {}),
      ...(input.writtenByUserId !== undefined ? { writtenByUserId: input.writtenByUserId } : {}),
      ...(input.writtenByEventId !== undefined ? { writtenByEventId: input.writtenByEventId } : {}),
    };
    const result = await this.opts.metadataStore.upsertMemory(upsertInput);
    if (!result.ok) {
      throw new MemoryConflictError(result.observedSha, result.expectedSha);
    }
    this.assertVersionMatchesWrite(result.version, { ...input, path });
    if (result.version.redactedAt) {
      return { memory: result.memory, version: result.version };
    }

    // Persist blobs only after metadata commits. Write the version first so
    // a redact operation has a durable target; the live overwrite is the
    // visible "commit point" for FUSE readers.
    await this.opts.blobStore.putVersion(
      input.workspaceId,
      input.storeId,
      input.sha256,
      Readable.from(buf),
      input.sizeBytes,
    );
    // A replay can return the historical (now deleted) memory occurrence. Only
    // repair live bytes while that exact occurrence still owns this path and
    // version; otherwise the immutable version repair above is sufficient.
    const current = await this.opts.metadataStore.getMemoryByPath(
      input.workspaceId,
      input.storeId,
      path,
    );
    if (current?.id === result.memory.id && current.currentSha256 === input.sha256) {
      await this.opts.blobStore.putLive(
        input.workspaceId,
        input.storeId,
        path,
        Readable.from(buf),
        input.sizeBytes,
      );
    }
    if (previousMemory && previousMemory.path !== path) {
      try {
        await this.opts.blobStore.deleteLive(input.workspaceId, input.storeId, previousMemory.path);
      } catch {
        // The new path is already committed; tolerate a missing old live blob.
      }
    }
    return { memory: result.memory, version: result.version };
  }

  async listVersions(
    workspaceId: string,
    storeId: string,
    memoryId: string,
  ): Promise<MemoryVersionRecord[]> {
    const store = await this.opts.metadataStore.getStore(workspaceId, storeId);
    if (!store) return [];
    return this.opts.metadataStore.listVersions(workspaceId, storeId, memoryId);
  }

  async listAllVersions(
    workspaceId: string,
    storeId: string,
    opts?: MemoryIdsFilter,
  ): Promise<MemoryVersionRecord[]> {
    const store = await this.opts.metadataStore.getStore(workspaceId, storeId);
    if (!store) return [];
    return this.opts.metadataStore.listAllVersions(workspaceId, storeId, opts);
  }

  async getVersion(
    workspaceId: string,
    storeId: string,
    versionId: string,
  ): Promise<MemoryVersionRecord | null> {
    const store = await this.opts.metadataStore.getStore(workspaceId, storeId);
    if (!store) return null;
    return this.opts.metadataStore.getVersion(workspaceId, storeId, versionId);
  }

  async openVersion(
    workspaceId: string,
    storeId: string,
    versionId: string,
  ): Promise<OpenMemory | null> {
    const version = await this.getVersion(workspaceId, storeId, versionId);
    if (!version || version.redactedAt) return null;
    const stream = await this.opts.blobStore.openVersion(workspaceId, storeId, version.sha256);
    return { stream, sizeBytes: version.sizeBytes, sha256: version.sha256 };
  }

  async redactVersion(
    workspaceId: string,
    storeId: string,
    versionId: string,
    actor: MemoryActorAttribution = {},
  ): Promise<MemoryVersionRecord> {
    const store = await this.opts.metadataStore.getStore(workspaceId, storeId);
    if (!store) {
      throw new Error(`redactVersion: store ${storeId} not found in workspace ${workspaceId}`);
    }
    const updated = await this.opts.metadataStore.markVersionRedacted({
      workspaceId,
      storeId,
      versionId,
      ...(actor.sessionId !== undefined ? { redactedBySessionId: actor.sessionId } : {}),
      ...(actor.apiKeyId !== undefined ? { redactedByApiKeyId: actor.apiKeyId } : {}),
      ...(actor.userId !== undefined ? { redactedByUserId: actor.userId } : {}),
    });
    if (!updated) {
      throw new Error(`redactVersion: version ${versionId} not found in store ${storeId}`);
    }
    // Redaction is represented by the per-Version metadata overlay. Version
    // blobs are SHA-addressed and may be shared by another non-redacted
    // Version in this store, so deleting the shared blob here would corrupt
    // that Version's `view=full` response.
    return updated;
  }

  async close(): Promise<void> {
    if (this.opts.metadataStore.close) {
      await this.opts.metadataStore.close();
    }
  }

  private assertVersionMatchesWrite(version: MemoryVersionRecord, input: WriteMemoryInput): void {
    if (
      version.storeId !== input.storeId ||
      version.path !== input.path ||
      version.sha256 !== input.sha256 ||
      version.sizeBytes !== input.sizeBytes
    ) {
      throw new Error(
        `LocalMemoryStore.writeMemory: versionId ${version.id} already belongs to a different memory write`,
      );
    }
  }
}

async function streamToBuffer(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as Uint8Array));
  }
  return Buffer.concat(chunks);
}
