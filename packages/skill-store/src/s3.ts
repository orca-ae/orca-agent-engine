// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import { cloneBundleRecord, decodeSkillBundle, encodeSkillBundle } from './codec.js';
import { normalizeRootPrefix, skillBundleKey } from './key.js';
import type { SkillStore } from './store.js';
import {
  SkillBundleIntegrityError,
  type SkillBundle,
  type SkillBundleInputFile,
  SkillBundleNotFoundError,
  type SkillBundleRecord,
} from './types.js';

const MAX_STORED_BUNDLE_BYTES = 64 * 1024 * 1024;
const MAX_CONDITIONAL_PUT_ATTEMPTS = 3;

export interface S3SkillStoreOptions {
  client: S3Client;
  bucket: string;
  /** Optional operator-owned root prefix. Default: bucket root. */
  keyPrefix?: string;
}

/**
 * S3-compatible immutable bundle store.
 *
 * Object layout:
 * `{root}workspaces/{workspaceId}/skill-versions/{versionId}/bundles/{sha256}/bundle.json`
 */
export class S3SkillStore implements SkillStore {
  private readonly client: S3Client;
  private readonly bucket: string;
  private readonly keyPrefix: string;

  constructor(options: S3SkillStoreOptions) {
    if (options.bucket.length === 0) throw new Error('skill bundle S3 bucket is required');
    this.client = options.client;
    this.bucket = options.bucket;
    this.keyPrefix = normalizeRootPrefix(options.keyPrefix);
  }

  async put(
    workspaceId: string,
    versionId: string,
    files: SkillBundleInputFile[],
  ): Promise<SkillBundleRecord> {
    const encoded = encodeSkillBundle(files);
    const digest = encoded.bundle.record.sha256;
    const key = this.keyFor(workspaceId, versionId, digest);
    for (let attempt = 1; attempt <= MAX_CONDITIONAL_PUT_ATTEMPTS; attempt += 1) {
      try {
        await this.client.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: key,
            Body: encoded.bytes,
            ContentLength: encoded.bytes.length,
            ContentType: 'application/vnd.orca.skill-bundle+json',
            IfNoneMatch: '*',
          }),
        );
        break;
      } catch (error) {
        if (isPreconditionFailed(error)) {
          // A concurrent/repeated writer won. Validate that the immutable object at
          // the digest-derived key is exactly the bundle we intended to store.
          await this.open(workspaceId, versionId, digest);
          break;
        }
        if (isConditionalConflict(error) && attempt < MAX_CONDITIONAL_PUT_ATTEMPTS) {
          continue;
        }
        throw error;
      }
    }
    return cloneBundleRecord(encoded.bundle.record);
  }

  async open(workspaceId: string, versionId: string, sha256: string): Promise<SkillBundle> {
    const key = this.keyFor(workspaceId, versionId, sha256);
    let response;
    try {
      response = await this.client.send(new GetObjectCommand({ Bucket: this.bucket, Key: key }));
    } catch (error) {
      if (isNotFound(error)) {
        throw new SkillBundleNotFoundError(workspaceId, versionId, sha256);
      }
      throw error;
    }
    if (!response.Body) {
      throw new SkillBundleNotFoundError(workspaceId, versionId, sha256);
    }
    if (response.ContentLength !== undefined && response.ContentLength > MAX_STORED_BUNDLE_BYTES) {
      throw new SkillBundleIntegrityError(
        `skill bundle exceeds ${MAX_STORED_BUNDLE_BYTES} encoded byte limit`,
      );
    }
    return decodeSkillBundle(await bodyToBuffer(response.Body, MAX_STORED_BUNDLE_BYTES), sha256);
  }

  async delete(workspaceId: string, versionId: string, sha256: string): Promise<void> {
    await this.client.send(
      new DeleteObjectCommand({
        Bucket: this.bucket,
        Key: this.keyFor(workspaceId, versionId, sha256),
      }),
    );
  }

  async close(): Promise<void> {
    // The S3 client is injected and may be shared with other workspace stores;
    // its owner controls destroy(). This store itself holds no open resources.
  }

  private keyFor(workspaceId: string, versionId: string, sha256: string): string {
    return skillBundleKey(this.keyPrefix, workspaceId, versionId, sha256);
  }
}

async function bodyToBuffer(body: unknown, maxBytes: number): Promise<Buffer> {
  if (Buffer.isBuffer(body) || body instanceof Uint8Array) {
    if (body.byteLength > maxBytes) {
      throw new SkillBundleIntegrityError(`skill bundle exceeds ${maxBytes} encoded byte limit`);
    }
    return Buffer.from(body);
  }
  // AWS SDK streaming bodies also expose transformToByteArray(). Prefer the
  // async iterator so an untrusted/misreported ContentLength cannot make the
  // SDK allocate the entire object before we enforce the encoded-byte limit.
  if (isAsyncIterable(body)) {
    const chunks: Buffer[] = [];
    let sizeBytes = 0;
    for await (const chunk of body) {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
      sizeBytes += bytes.length;
      if (sizeBytes > maxBytes) {
        throw new SkillBundleIntegrityError(`skill bundle exceeds ${maxBytes} encoded byte limit`);
      }
      chunks.push(bytes);
    }
    return Buffer.concat(chunks, sizeBytes);
  }
  if (
    typeof body === 'object' &&
    body !== null &&
    'transformToByteArray' in body &&
    typeof body.transformToByteArray === 'function'
  ) {
    const bytes = await body.transformToByteArray();
    if (bytes.byteLength > maxBytes) {
      throw new SkillBundleIntegrityError(`skill bundle exceeds ${maxBytes} encoded byte limit`);
    }
    return Buffer.from(bytes);
  }
  throw new Error('unsupported S3 skill bundle body');
}

function isAsyncIterable(value: unknown): value is AsyncIterable<unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    Symbol.asyncIterator in value &&
    typeof value[Symbol.asyncIterator] === 'function'
  );
}

function isPreconditionFailed(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false;
  const candidate = error as { name?: unknown; $metadata?: { httpStatusCode?: unknown } };
  return candidate.name === 'PreconditionFailed' || candidate.$metadata?.httpStatusCode === 412;
}

function isConditionalConflict(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false;
  const candidate = error as { name?: unknown; $metadata?: { httpStatusCode?: unknown } };
  return (
    candidate.name === 'ConditionalRequestConflict' || candidate.$metadata?.httpStatusCode === 409
  );
}

function isNotFound(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false;
  const candidate = error as { name?: unknown; $metadata?: { httpStatusCode?: unknown } };
  return (
    candidate.name === 'NoSuchKey' ||
    candidate.name === 'NotFound' ||
    candidate.$metadata?.httpStatusCode === 404
  );
}
