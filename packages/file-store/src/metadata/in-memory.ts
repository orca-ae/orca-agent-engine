// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FileRecord } from '../types.js';
import type { ListFilesOptions } from '../store.js';
import type { FileMetadataInsert, FileMetadataStore } from './store.js';

/**
 * Test double for `FileMetadataStore`. Backed by a Map keyed by file id;
 * mirrors the Postgres implementation's behavior for `findBySha` (only
 * returns non-archived rows) and `list` (excludes archived rows, sorts by
 * id descending, supports the `scopeId` filter). Not safe for production —
 * data lives in heap memory and is not concurrency-safe across processes.
 */
export class InMemoryFileMetadataStore implements FileMetadataStore {
  private readonly deletedAt = new Map<string, Date>();
  private readonly rows = new Map<string, FileRecord>();

  async findBySha(workspaceId: string, sha256: string): Promise<FileRecord | null> {
    // Mirror the Postgres partial unique index: dedup only applies within
    // `purpose='agent'` rows. `agent_output` rows (and any future non-
    // `agent` purpose) are excluded so that `LocalFileStore.create` for an
    // `agent_output` upload does NOT silently collapse onto an existing
    // `agent` row with the same sha256, and vice-versa.
    for (const row of this.rows.values()) {
      if (
        row.workspaceId === workspaceId &&
        row.sha256 === sha256 &&
        row.archivedAt === null &&
        !this.deletedAt.has(row.id) &&
        row.purpose === 'agent'
      ) {
        return clone(row);
      }
    }
    return null;
  }

  async insert(record: FileMetadataInsert): Promise<FileRecord> {
    if (this.rows.has(record.id)) {
      throw Object.assign(new Error('duplicate File id'), { code: '23505' });
    }
    // Mirror the Postgres partial unique index: (workspace_id, sha256)
    // WHERE purpose = 'agent' AND archived_at IS NULL. We do NOT enforce it
    // here because the call site (`LocalFileStore.create`) only consults
    // `findBySha` when `purpose === 'agent'`; tests exercise the dedup path
    // through that check. All inserts still enforce the file-id primary key.
    const stored: FileRecord = {
      id: record.id,
      workspaceId: record.workspaceId,
      filename: record.filename,
      mimeType: record.mimeType,
      sizeBytes: record.sizeBytes,
      sha256: record.sha256,
      metadata: { ...record.metadata },
      purpose: record.purpose,
      scopeId: record.scopeId,
      downloadable: record.downloadable,
      archivedAt: record.archivedAt,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
    };
    this.rows.set(record.id, stored);
    return clone(stored);
  }

  async findById(workspaceId: string, fileId: string): Promise<FileRecord | null> {
    const row = this.rows.get(fileId);
    if (!row || row.workspaceId !== workspaceId || this.deletedAt.has(fileId)) return null;
    return clone(row);
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
    const candidates = Array.from(this.rows.values())
      .filter(
        (r) =>
          r.workspaceId === workspaceId &&
          r.archivedAt === null &&
          !this.deletedAt.has(r.id) &&
          // Exact equality. An empty-string filter would NOT match a null
          // row — same semantics as the Postgres path.
          (scopeId === undefined || r.scopeId === scopeId) &&
          (purpose === undefined || r.purpose === purpose),
      )
      .sort((a, b) => (a.id < b.id ? 1 : a.id > b.id ? -1 : 0));
    if (beforeCursor) {
      const before = candidates.filter((r) => r.id > beforeCursor).reverse();
      const window = before.slice(0, limit + 1);
      const items = window.slice(0, limit).reverse().map(clone);
      const nextCursor = window.length > limit && items.length > 0 ? items[0]!.id : null;
      return { items, nextCursor };
    }
    const start = cursor ? candidates.findIndex((r) => r.id < cursor) : 0;
    const sliceStart = start === -1 ? candidates.length : start;
    const window = candidates.slice(sliceStart, sliceStart + limit + 1);
    const items = window.slice(0, limit).map(clone);
    const nextCursor = window.length > limit ? items[items.length - 1]!.id : null;
    return { items, nextCursor };
  }

  async archive(workspaceId: string, fileId: string): Promise<void> {
    const row = this.rows.get(fileId);
    if (!row || row.workspaceId !== workspaceId || this.deletedAt.has(fileId)) return;
    const now = new Date();
    this.rows.set(fileId, { ...row, archivedAt: now, updatedAt: now });
  }

  async delete(workspaceId: string, fileId: string): Promise<void> {
    const row = this.rows.get(fileId);
    if (!row || row.workspaceId !== workspaceId || this.deletedAt.has(fileId)) return;
    this.deletedAt.set(fileId, new Date());
  }

  /** Test helper: how many rows are stored (including archived). */
  size(): number {
    return this.rows.size;
  }

  /** Test helper: drop all rows. */
  clear(): void {
    this.rows.clear();
    this.deletedAt.clear();
  }
}

function clone(r: FileRecord): FileRecord {
  return {
    ...r,
    metadata: { ...r.metadata },
  };
}
