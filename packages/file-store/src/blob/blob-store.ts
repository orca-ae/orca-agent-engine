// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Pluggable blob backend. Implementations: `S3BlobStore` and
 * `InMemoryBlobStore` (a test double).
 *
 * Blobs are physically isolated by workspace. Backends are responsible for
 * combining the explicit workspace id with the hex-encoded SHA-256 digest.
 */
export interface BlobStore {
  put(
    workspaceId: string,
    sha256: string,
    content: NodeJS.ReadableStream,
    sizeBytes: number,
  ): Promise<void>;
  open(workspaceId: string, sha256: string): Promise<NodeJS.ReadableStream>;
  delete(workspaceId: string, sha256: string): Promise<void>;
}
