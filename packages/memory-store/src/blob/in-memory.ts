// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Readable } from 'node:stream';
import type { MemoryBlobStore } from './blob-store.js';
import {
  assertMemoryStoreId,
  assertMemoryVersionSha256,
  assertMemoryWorkspaceId,
  normalizeMemoryRelativePath,
} from './path.js';

/**
 * Test double. Backs blobs with two Maps keyed by
 * `${workspaceId}|${storeId}|${path}` (live) and
 * `${workspaceId}|${storeId}|${sha256}` (versions). Used by unit tests + the
 * harness's LocalMemoryStrategy fallback.
 */
export class InMemoryMemoryBlobStore implements MemoryBlobStore {
  private readonly live = new Map<string, Buffer>();
  private readonly versions = new Map<string, Buffer>();

  private liveKey(workspaceId: string, storeId: string, path: string): string {
    assertMemoryWorkspaceId(workspaceId);
    assertMemoryStoreId(storeId);
    const cleanPath = normalizeMemoryRelativePath(path);
    return `${workspaceId}|${storeId}|${cleanPath}`;
  }

  private versionKey(workspaceId: string, storeId: string, sha256: string): string {
    assertMemoryWorkspaceId(workspaceId);
    assertMemoryStoreId(storeId);
    assertMemoryVersionSha256(sha256);
    return `${workspaceId}|${storeId}|${sha256}`;
  }

  async putLive(
    workspaceId: string,
    storeId: string,
    path: string,
    content: NodeJS.ReadableStream,
    _sizeBytes: number,
  ): Promise<void> {
    const buf = await streamToBuffer(content);
    this.live.set(this.liveKey(workspaceId, storeId, path), buf);
  }

  async openLive(
    workspaceId: string,
    storeId: string,
    path: string,
  ): Promise<NodeJS.ReadableStream> {
    const buf = this.live.get(this.liveKey(workspaceId, storeId, path));
    if (!buf) throw new Error(`InMemoryMemoryBlobStore.openLive: missing ${storeId}/${path}`);
    return Readable.from(buf);
  }

  async deleteLive(workspaceId: string, storeId: string, path: string): Promise<void> {
    this.live.delete(this.liveKey(workspaceId, storeId, path));
  }

  async listLive(workspaceId: string, storeId: string): Promise<string[]> {
    assertMemoryWorkspaceId(workspaceId);
    assertMemoryStoreId(storeId);
    const prefix = `${workspaceId}|${storeId}|`;
    const out: string[] = [];
    for (const key of this.live.keys()) {
      if (key.startsWith(prefix)) out.push(key.substring(prefix.length));
    }
    return out;
  }

  async putVersion(
    workspaceId: string,
    storeId: string,
    sha256: string,
    content: NodeJS.ReadableStream,
    _sizeBytes: number,
  ): Promise<void> {
    const buf = await streamToBuffer(content);
    this.versions.set(this.versionKey(workspaceId, storeId, sha256), buf);
  }

  async openVersion(
    workspaceId: string,
    storeId: string,
    sha256: string,
  ): Promise<NodeJS.ReadableStream> {
    const buf = this.versions.get(this.versionKey(workspaceId, storeId, sha256));
    if (!buf) throw new Error(`InMemoryMemoryBlobStore.openVersion: missing ${storeId}/${sha256}`);
    return Readable.from(buf);
  }

  async deleteVersion(workspaceId: string, storeId: string, sha256: string): Promise<void> {
    this.versions.delete(this.versionKey(workspaceId, storeId, sha256));
  }
}

async function streamToBuffer(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as Uint8Array));
  }
  return Buffer.concat(chunks);
}
