// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Two-namespace blob plane for memory-store.
 *
 * - **Live state** is path-addressed under
 *   `{root}workspaces/{workspaceId}/memory-stores/{storeId}/live/{path}`.
 *   The agent's FUSE mount writes here directly via s3fs.
 * - **Versions** are sha-addressed under
 *   `{root}workspaces/{workspaceId}/memory-stores/{storeId}/versions/{sha256}`.
 *   The watcher copies each new version here so redact/rollback is durable.
 *
 * Implementations include S3 (production / dev RustFS) and InMemory (tests).
 *
 * Note: workspaceId is passed on every call (not bound at construction time)
 * so the registry can reuse a single blob-store instance across all
 * workspaces. The store's per-workspace key prefix is derived inside the
 * implementation. The blob plane repeats the tenant identity so bucket and
 * STS policies can independently enforce the same workspace boundary.
 */
export interface MemoryBlobStore {
  // live namespace
  putLive(
    workspaceId: string,
    storeId: string,
    path: string,
    content: NodeJS.ReadableStream,
    sizeBytes: number,
  ): Promise<void>;
  openLive(workspaceId: string, storeId: string, path: string): Promise<NodeJS.ReadableStream>;
  deleteLive(workspaceId: string, storeId: string, path: string): Promise<void>;
  /** Returns relative paths under the store's dedicated `live/` prefix. */
  listLive(workspaceId: string, storeId: string): Promise<string[]>;

  // version namespace
  putVersion(
    workspaceId: string,
    storeId: string,
    sha256: string,
    content: NodeJS.ReadableStream,
    sizeBytes: number,
  ): Promise<void>;
  openVersion(workspaceId: string, storeId: string, sha256: string): Promise<NodeJS.ReadableStream>;
  /** Used by the redact path. Best-effort: may already be missing (no-op).*/
  deleteVersion(workspaceId: string, storeId: string, sha256: string): Promise<void>;
}
