// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import { Readable } from 'node:stream';
import type { BlobStore } from './blob-store.js';
import { fileBlobKey, normalizeRootPrefix } from './key.js';

export interface S3BlobStoreOptions {
  client: S3Client;
  bucket: string;
  /** Optional storage root; default '' (bucket root). */
  keyPrefix?: string;
}

/**
 * S3-compatible blob backend. Works against AWS S3, MinIO, Cloudflare R2,
 * and GCS via S3-interop. Object key layout:
 *
 *   `{root}workspaces/{workspaceId}/files/blobs/{aa}/{bb}/{sha256}/content`
 *
 * The two-level fanout matches Git's loose-object layout — keeps any single
 * S3 prefix below ~65k objects without manual partitioning.
 */
export class S3BlobStore implements BlobStore {
  private readonly client: S3Client;
  private readonly bucket: string;
  private readonly keyPrefix: string;

  constructor(opts: S3BlobStoreOptions) {
    this.client = opts.client;
    this.bucket = opts.bucket;
    this.keyPrefix = normalizeRootPrefix(opts.keyPrefix);
  }

  private keyFor(workspaceId: string, sha256: string): string {
    return fileBlobKey(this.keyPrefix, workspaceId, sha256);
  }

  async put(
    workspaceId: string,
    sha256: string,
    content: NodeJS.ReadableStream,
    sizeBytes: number,
  ): Promise<void> {
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: this.keyFor(workspaceId, sha256),
        Body: content as Readable,
        ContentLength: sizeBytes,
      }),
    );
  }

  async open(workspaceId: string, sha256: string): Promise<NodeJS.ReadableStream> {
    const resp = await this.client.send(
      new GetObjectCommand({ Bucket: this.bucket, Key: this.keyFor(workspaceId, sha256) }),
    );
    if (!resp.Body) {
      throw new Error(`empty body for ${sha256}`);
    }
    // The AWS SDK v3 returns `StreamingBlobPayloadOutputTypes` which on Node
    // is a `Readable`. Cast through unknown to satisfy the strict TS
    // structural check.
    return resp.Body as unknown as NodeJS.ReadableStream;
  }

  async delete(workspaceId: string, sha256: string): Promise<void> {
    await this.client.send(
      new DeleteObjectCommand({ Bucket: this.bucket, Key: this.keyFor(workspaceId, sha256) }),
    );
  }
}
