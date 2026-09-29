// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FileRecord, CreateFileInput, FilePurpose } from './types.js';

export interface ListFilesOptions {
  limit?: number;
  cursor?: string;
  beforeCursor?: string;
  scopeId?: string;
  purpose?: FilePurpose;
}

/**
 * Streaming-read result. The store guarantees `sizeBytes` and `sha256` are
 * stable for the lifetime of the open call; callers may use them for
 * defensive checks against `FileRecord.sizeBytes` / `.sha256`.
 */
export interface OpenStream {
  stream: NodeJS.ReadableStream;
  sizeBytes: number;
  sha256: string;
}

/**
 * The public file-store interface. Backends compose:
 *   metadata (Postgres) + blobs (object storage).
 *
 * SHA-256 dedup is enforced for active user uploads via the underlying
 * metadata store's partial UNIQUE (workspace_id, sha256) index. Repeated
 * active uploads return the same `FileRecord.id`; archiving releases the
 * digest for a fresh row, and agent outputs never participate.
 */
export interface FileStore {
  create(input: CreateFileInput): Promise<FileRecord>;
  get(workspaceId: string, fileId: string): Promise<FileRecord | null>;
  list(
    workspaceId: string,
    opts?: ListFilesOptions,
  ): Promise<{ items: FileRecord[]; nextCursor: string | null }>;
  open(workspaceId: string, fileId: string): Promise<OpenStream | null>;
  archive(workspaceId: string, fileId: string): Promise<void>;
  delete(workspaceId: string, fileId: string): Promise<void>;
  close(): Promise<void>;
}

export type { FileRecord, CreateFileInput };
