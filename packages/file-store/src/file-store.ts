// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomBytes } from 'node:crypto';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { tmpdir } from 'node:os';
import { isDeepStrictEqual } from 'node:util';
import {
  mkdtempSync,
  createReadStream,
  createWriteStream,
  statSync,
  unlinkSync,
  rmdirSync,
} from 'node:fs';
import { join } from 'node:path';
import type { BlobStore } from './blob/blob-store.js';
import type { FileStore, ListFilesOptions, OpenStream } from './store.js';
import {
  FileNotFoundError,
  FileSizeMismatchError,
  defaultDownloadable,
  type CreateFileInput,
  type FileRecord,
} from './types.js';
import { PostgresFileMetadataStore } from './metadata/postgres.js';
import type { FileMetadataStore } from './metadata/store.js';
import type { Pool } from 'pg';
import { blobBytesTotal, createTotal, dedupHits } from './metrics.js';

/**
 * Construction options for `LocalFileStore`. Callers must provide either a
 * `pool` (for Postgres-backed metadata) or a pre-built `metadataStore`
 * (typically the in-memory test double).
 */
export type LocalFileStoreOptions = LocalFileStoreOptionsBase &
  ({ pool: Pool; metadataStore?: never } | { pool?: never; metadataStore: FileMetadataStore });

interface LocalFileStoreOptionsBase {
  blobStore: BlobStore;
  /** Override `file_…` ID generator for tests; defaults to crypto random. */
  generateId?: () => string;
}

function defaultId(): string {
  return `file_${randomBytes(12).toString('base64url')}`;
}

/**
 * Default `FileStore` implementation: streams content to a tmp file while
 * computing the SHA-256 + size, dedups active user uploads against
 * `(workspace_id, sha256)`, uploads the blob if new, and inserts the metadata
 * row. Reads stream directly from the blob store.
 */
export class LocalFileStore implements FileStore {
  private readonly meta: FileMetadataStore;
  private readonly blobs: BlobStore;
  private readonly pool: Pool | null;
  private readonly genId: () => string;

  constructor(opts: LocalFileStoreOptions) {
    if ('metadataStore' in opts && opts.metadataStore) {
      this.meta = opts.metadataStore;
      this.pool = null;
    } else if (opts.pool) {
      this.meta = new PostgresFileMetadataStore(opts.pool);
      this.pool = opts.pool;
    } else {
      throw new Error('LocalFileStore requires either pool or metadataStore');
    }
    this.blobs = opts.blobStore;
    this.genId = opts.generateId ?? defaultId;
  }

  async create(input: CreateFileInput): Promise<FileRecord> {
    if (
      input.id !== undefined &&
      (!/^file_[A-Za-z0-9_-]{16,128}$/.test(input.id) ||
        input.purpose !== 'agent_output' ||
        !input.scopeId)
    ) {
      throw new Error('an explicit File id requires a valid session output identity');
    }
    // Stream content -> tmp file while hashing + counting bytes. We need both
    // before we can do dedup lookup OR a sized BlobStore.put.
    const tmpDir = mkdtempSync(join(tmpdir(), 'orca-files-'));
    const tmpPath = join(tmpDir, 'blob');
    const hasher = createHash('sha256');
    let size = 0;

    const counter = new Transform({
      transform(chunk: Buffer, _enc, cb) {
        hasher.update(chunk);
        size += chunk.length;
        cb(null, chunk);
      },
    });

    try {
      await pipeline(input.content, counter, createWriteStream(tmpPath));
    } catch (err) {
      cleanupTmp(tmpPath, tmpDir);
      createTotal.inc({ status: 'error' });
      throw err;
    }

    const sha256 = hasher.digest('hex');

    if (input.expectedSizeBytes !== undefined && input.expectedSizeBytes !== size) {
      cleanupTmp(tmpPath, tmpDir);
      createTotal.inc({ status: 'error' });
      throw new FileSizeMismatchError(input.expectedSizeBytes, size);
    }

    // Resolve purpose / downloadable / scopeId. Dedup is *purpose-aware*:
    //   - `purpose='agent'`        → dedup within `(workspace_id, sha256)`.
    //     User uploads of the same content collapse to one row (saves storage
    //     + simplifies SDK round-trips).
    //   - `purpose='agent_output'` → no content dedup. Each session output is its
    //     own File record even when bytes match a prior upload/output, to
    //     match Anthropic's Files API semantics where every output gets a
    //     fresh `file_id`. The Postgres unique index is partial
    //     (`WHERE purpose = 'agent'`) so non-`agent` rows are not constrained.
    const purpose = input.purpose ?? 'agent';
    const downloadable = input.downloadable ?? defaultDownloadable(purpose);
    const scopeId = input.scopeId ?? null;
    const sameOutput = (existing: FileRecord): FileRecord => {
      if (
        existing.workspaceId !== input.workspaceId ||
        existing.purpose !== purpose ||
        existing.scopeId !== scopeId ||
        existing.sha256 !== sha256 ||
        existing.sizeBytes !== size ||
        existing.filename !== input.filename ||
        existing.mimeType !== input.mimeType ||
        existing.downloadable !== downloadable ||
        existing.archivedAt !== null ||
        !isDeepStrictEqual(existing.metadata, input.metadata ?? {})
      ) {
        throw new Error('session output identity conflicts with an existing File');
      }
      return existing;
    };

    if (input.id !== undefined) {
      let existing: FileRecord | null;
      try {
        existing = await this.meta.findById(input.workspaceId, input.id);
      } catch (err) {
        cleanupTmp(tmpPath, tmpDir);
        createTotal.inc({ status: 'error' });
        throw err;
      }
      if (existing) {
        cleanupTmp(tmpPath, tmpDir);
        return sameOutput(existing);
      }
    }

    // Dedup check — only for `purpose='agent'`. Skipping `findBySha` for
    // `agent_output` (and any future non-`agent` purpose) avoids the bug
    // where an output silently collapses into a prior `agent` row with the
    // wrong scope_id / downloadable / purpose.
    if (purpose === 'agent') {
      const existing = await this.meta.findBySha(input.workspaceId, sha256);
      if (existing) {
        cleanupTmp(tmpPath, tmpDir);
        dedupHits.inc();
        createTotal.inc({ status: 'dedup' });
        return existing;
      }
    }

    // Persist blob. We re-stream from the tmp file so the BlobStore gets a
    // fresh Readable (the hash transform above consumed the input stream).
    const blobUri = `sha256://${sha256}`;
    try {
      await this.blobs.put(input.workspaceId, sha256, createReadStream(tmpPath), size);
      blobBytesTotal.inc({ op: 'put' }, size);
    } catch (err) {
      cleanupTmp(tmpPath, tmpDir);
      createTotal.inc({ status: 'error' });
      throw err;
    }

    const id = input.id ?? this.genId();
    const now = new Date();
    let inserted: FileRecord;
    try {
      inserted = await this.meta.insert({
        id,
        workspaceId: input.workspaceId,
        filename: input.filename,
        mimeType: input.mimeType,
        sizeBytes: size,
        sha256,
        blobUri,
        metadata: input.metadata ?? {},
        purpose,
        scopeId,
        downloadable,
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      });
    } catch (err) {
      // If the insert failed and we're in the `purpose='agent'` path, it may
      // be a unique-index race: a concurrent dedup write won. Re-check
      // `findBySha` so we return a consistent record rather than 500ing.
      // Session outputs may also race on an explicit replay identity. Only
      // an exact identity match may join that write; other failures propagate.
      cleanupTmp(tmpPath, tmpDir);
      if (input.id !== undefined && isUniqueViolation(err)) {
        const racing = await this.meta.findById(input.workspaceId, input.id);
        if (racing) return sameOutput(racing);
      }
      if (purpose === 'agent' && isUniqueViolation(err)) {
        const racing = await this.meta.findBySha(input.workspaceId, sha256);
        if (racing) {
          dedupHits.inc();
          createTotal.inc({ status: 'dedup' });
          return racing;
        }
      }
      createTotal.inc({ status: 'error' });
      throw err;
    }

    cleanupTmp(tmpPath, tmpDir);
    createTotal.inc({ status: 'ok' });
    return inserted;
  }

  async get(workspaceId: string, fileId: string): Promise<FileRecord | null> {
    return this.meta.findById(workspaceId, fileId);
  }

  async list(
    workspaceId: string,
    opts?: ListFilesOptions,
  ): Promise<{ items: FileRecord[]; nextCursor: string | null }> {
    return this.meta.list(workspaceId, opts);
  }

  async open(workspaceId: string, fileId: string): Promise<OpenStream | null> {
    const record = await this.meta.findById(workspaceId, fileId);
    if (!record) return null;
    const stream = await this.blobs.open(record.workspaceId, record.sha256);
    blobBytesTotal.inc({ op: 'open' }, record.sizeBytes);
    return { stream, sizeBytes: record.sizeBytes, sha256: record.sha256 };
  }

  async archive(workspaceId: string, fileId: string): Promise<void> {
    const existing = await this.meta.findById(workspaceId, fileId);
    if (!existing) throw new FileNotFoundError(fileId);
    await this.meta.archive(workspaceId, fileId);
  }

  async delete(workspaceId: string, fileId: string): Promise<void> {
    const existing = await this.meta.findById(workspaceId, fileId);
    if (!existing) throw new FileNotFoundError(fileId);
    await this.meta.delete(workspaceId, fileId);
    // Keep the blob with the retained metadata; other rows may share its digest.
  }

  async close(): Promise<void> {
    if (this.pool) await this.pool.end().catch(() => {});
  }
}

function isUniqueViolation(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    (error as { code?: unknown }).code === '23505'
  );
}

function cleanupTmp(tmpPath: string, tmpDir: string): void {
  try {
    if (statSync(tmpPath, { throwIfNoEntry: false })) {
      unlinkSync(tmpPath);
    }
  } catch {
    /* ignore */
  }
  try {
    rmdirSync(tmpDir);
  } catch {
    /* ignore */
  }
}
