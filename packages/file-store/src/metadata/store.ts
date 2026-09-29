// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { ListFilesOptions } from '../store.js';
import type { FileRecord } from '../types.js';

/**
 * The metadata-side of the file store: pure CRUD against whatever backing
 * store you compose with. The Postgres implementation is `PostgresFileMetadataStore`;
 * the in-memory test double is `InMemoryFileMetadataStore`.
 *
 * Implementations are responsible for enforcing active user-upload uniqueness
 * by `(workspace_id, sha256)`. `LocalFileStore` relies on `findBySha` returning
 * the active row when the same content is uploaded twice in one workspace.
 */
export interface FileMetadataStore {
  findBySha(workspaceId: string, sha256: string): Promise<FileRecord | null>;
  insert(record: FileMetadataInsert): Promise<FileRecord>;
  findById(workspaceId: string, fileId: string): Promise<FileRecord | null>;
  list(
    workspaceId: string,
    opts?: ListFilesOptions,
  ): Promise<{ items: FileRecord[]; nextCursor: string | null }>;
  archive(workspaceId: string, fileId: string): Promise<void>;
  delete(workspaceId: string, fileId: string): Promise<void>;
}

/**
 * The shape passed to `FileMetadataStore.insert`. This is what the
 * `LocalFileStore` builds after streaming + hashing the bytes; the store
 * persists it verbatim.
 */
export interface FileMetadataInsert {
  id: string;
  workspaceId: string;
  filename: string;
  mimeType: string;
  sizeBytes: number;
  sha256: string;
  blobUri: string;
  metadata: Record<string, string>;
  purpose: FileRecord['purpose'];
  scopeId: string | null;
  downloadable: boolean;
  archivedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}
