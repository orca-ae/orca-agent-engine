// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  DeleteObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import { Readable } from 'node:stream';
import type { MemoryBlobStore } from './blob-store.js';
import {
  assertMemoryStoreId,
  assertMemoryVersionSha256,
  assertMemoryWorkspaceId,
  normalizeMemoryRelativePath,
} from './path.js';

export interface S3MemoryBlobStoreOptions {
  client: S3Client;
  bucket: string;
  /** Canonical object-key root common to all workspaces; empty or slash-terminated. */
  keyPrefix: string;
}

const SAFE_ROOT_SEGMENT = /^[A-Za-z0-9._-]+$/;

/**
 * S3-compatible memory blob backend. Layout:
 *
 *   live:    `{root}workspaces/{workspaceId}/memory-stores/{storeId}/live/{path}`
 *   version: `{root}workspaces/{workspaceId}/memory-stores/{storeId}/versions/{sha256}`
 *
 * Live keys are path-addressed so the harness's FUSE mount can write directly
 * via s3fs. Version keys are sha-addressed under a sibling `versions/`
 * subprefix, outside the mounted `live/` namespace.
 *
 * The instance is workspace-agnostic: the registry constructs a single
 * `S3MemoryBlobStore` and threads the per-request `workspaceId` into every
 * method call. (Earlier drafts bound `workspaceId` at construction time;
 * that doesn't compose with a multi-tenant registry.)
 */
export class S3MemoryBlobStore implements MemoryBlobStore {
  constructor(private readonly opts: S3MemoryBlobStoreOptions) {
    validateRootPrefix(opts.keyPrefix);
  }

  private liveKey(workspaceId: string, storeId: string, path: string): string {
    const cleanPath = normalizeMemoryRelativePath(path);
    return `${this.storePrefix(workspaceId, storeId)}live/${cleanPath}`;
  }

  private versionKey(workspaceId: string, storeId: string, sha256: string): string {
    assertMemoryVersionSha256(sha256);
    return `${this.storePrefix(workspaceId, storeId)}versions/${sha256}`;
  }

  private storePrefix(workspaceId: string, storeId: string): string {
    assertMemoryWorkspaceId(workspaceId);
    assertMemoryStoreId(storeId);
    return `${this.opts.keyPrefix}workspaces/${workspaceId}/memory-stores/${storeId}/`;
  }

  private livePrefix(workspaceId: string, storeId: string): string {
    return `${this.storePrefix(workspaceId, storeId)}live/`;
  }

  async putLive(
    workspaceId: string,
    storeId: string,
    path: string,
    content: NodeJS.ReadableStream,
    sizeBytes: number,
  ): Promise<void> {
    await this.opts.client.send(
      new PutObjectCommand({
        Bucket: this.opts.bucket,
        Key: this.liveKey(workspaceId, storeId, path),
        Body: content as unknown as Readable,
        ContentLength: sizeBytes,
      }),
    );
  }

  async openLive(
    workspaceId: string,
    storeId: string,
    path: string,
  ): Promise<NodeJS.ReadableStream> {
    const res = await this.opts.client.send(
      new GetObjectCommand({
        Bucket: this.opts.bucket,
        Key: this.liveKey(workspaceId, storeId, path),
      }),
    );
    if (!res.Body) {
      throw new Error(`S3MemoryBlobStore.openLive(${storeId}, ${path}): empty body`);
    }
    return res.Body as unknown as NodeJS.ReadableStream;
  }

  async deleteLive(workspaceId: string, storeId: string, path: string): Promise<void> {
    await this.opts.client.send(
      new DeleteObjectCommand({
        Bucket: this.opts.bucket,
        Key: this.liveKey(workspaceId, storeId, path),
      }),
    );
  }

  async listLive(workspaceId: string, storeId: string): Promise<string[]> {
    const prefix = this.livePrefix(workspaceId, storeId);
    const out: string[] = [];
    let continuationToken: string | undefined;
    do {
      const res = await this.opts.client.send(
        new ListObjectsV2Command({
          Bucket: this.opts.bucket,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        }),
      );
      for (const obj of res.Contents ?? []) {
        if (!obj.Key) continue;
        // strip the prefix to return the relative path
        out.push(obj.Key.substring(prefix.length));
      }
      continuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
    } while (continuationToken);
    return out;
  }

  async putVersion(
    workspaceId: string,
    storeId: string,
    sha256: string,
    content: NodeJS.ReadableStream,
    sizeBytes: number,
  ): Promise<void> {
    await this.opts.client.send(
      new PutObjectCommand({
        Bucket: this.opts.bucket,
        Key: this.versionKey(workspaceId, storeId, sha256),
        Body: content as unknown as Readable,
        ContentLength: sizeBytes,
      }),
    );
  }

  async openVersion(
    workspaceId: string,
    storeId: string,
    sha256: string,
  ): Promise<NodeJS.ReadableStream> {
    const res = await this.opts.client.send(
      new GetObjectCommand({
        Bucket: this.opts.bucket,
        Key: this.versionKey(workspaceId, storeId, sha256),
      }),
    );
    if (!res.Body) {
      throw new Error(`S3MemoryBlobStore.openVersion(${storeId}, ${sha256}): empty body`);
    }
    return res.Body as unknown as NodeJS.ReadableStream;
  }

  async deleteVersion(workspaceId: string, storeId: string, sha256: string): Promise<void> {
    // Best-effort: S3 DeleteObject is idempotent (200 even if missing), so no try/catch.
    await this.opts.client.send(
      new DeleteObjectCommand({
        Bucket: this.opts.bucket,
        Key: this.versionKey(workspaceId, storeId, sha256),
      }),
    );
  }
}

function validateRootPrefix(prefix: string): void {
  if (prefix === '') return;
  if (
    !prefix.endsWith('/') ||
    prefix.startsWith('/') ||
    prefix.includes('\\') ||
    prefix.includes('\0')
  ) {
    throw new Error(`S3MemoryBlobStore: invalid keyPrefix "${prefix}"`);
  }
  const segments = prefix.slice(0, -1).split('/');
  if (
    segments.some(
      (segment) =>
        segment.length === 0 ||
        segment === '.' ||
        segment === '..' ||
        !SAFE_ROOT_SEGMENT.test(segment),
    )
  ) {
    throw new Error(`S3MemoryBlobStore: invalid keyPrefix "${prefix}"`);
  }
}
