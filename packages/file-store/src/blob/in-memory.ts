// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Readable } from 'node:stream';
import type { BlobStore } from './blob-store.js';
import { fileBlobKey } from './key.js';

/**
 * Test double. Stores blob bytes under the same workspace-owned logical key
 * shape as the S3 backend. Not safe for production — backed by heap memory.
 */
export class InMemoryBlobStore implements BlobStore {
  private readonly map = new Map<string, Buffer>();

  async put(
    workspaceId: string,
    sha256: string,
    content: NodeJS.ReadableStream,
    _sizeBytes: number,
  ): Promise<void> {
    const chunks: Buffer[] = [];
    for await (const chunk of content) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as Uint8Array));
    }
    this.map.set(fileBlobKey('', workspaceId, sha256), Buffer.concat(chunks));
  }

  async open(workspaceId: string, sha256: string): Promise<NodeJS.ReadableStream> {
    const key = fileBlobKey('', workspaceId, sha256);
    const buf = this.map.get(key);
    if (!buf) throw new Error(`blob not found in workspace ${workspaceId}: ${sha256}`);
    return Readable.from(buf);
  }

  async delete(workspaceId: string, sha256: string): Promise<void> {
    this.map.delete(fileBlobKey('', workspaceId, sha256));
  }

  /** Test helper: how many blobs are stored. */
  size(): number {
    return this.map.size;
  }

  /** Test helper: clear all blobs. */
  clear(): void {
    this.map.clear();
  }
}
