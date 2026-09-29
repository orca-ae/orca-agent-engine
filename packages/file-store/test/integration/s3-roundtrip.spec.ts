// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { CreateBucketCommand, HeadBucketCommand, S3Client } from '@aws-sdk/client-s3';
import { Readable } from 'node:stream';
import { createHash, randomBytes } from 'node:crypto';
import { S3BlobStore } from '../../src/blob/s3.js';

const ENDPOINT = process.env['S3_ENDPOINT'] ?? 'http://localhost:9000';
const ACCESS_KEY = process.env['S3_ACCESS_KEY'] ?? 'minioadmin';
const SECRET_KEY = process.env['S3_SECRET_KEY'] ?? 'minioadmin';
const BUCKET = process.env['S3_BUCKET'] ?? 'orca-files';
const REGION = process.env['S3_REGION'] ?? 'us-east-1';

async function readAll(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
  }
  return Buffer.concat(chunks);
}

describe('S3BlobStore (integration)', () => {
  let client: S3Client;
  let store: S3BlobStore;

  beforeAll(async () => {
    client = new S3Client({
      endpoint: ENDPOINT,
      region: REGION,
      credentials: { accessKeyId: ACCESS_KEY, secretAccessKey: SECRET_KEY },
      forcePathStyle: true,
    });
    // Ensure bucket exists; ignore "already-exists" errors.
    try {
      await client.send(new HeadBucketCommand({ Bucket: BUCKET }));
    } catch {
      try {
        await client.send(new CreateBucketCommand({ Bucket: BUCKET }));
      } catch {
        /* race or already exists; the put-test below catches real failures */
      }
    }
    store = new S3BlobStore({ client, bucket: BUCKET, keyPrefix: 'test/blobs/' });
  });

  afterAll(async () => {
    client?.destroy();
  });

  it('round-trips a 1 KiB blob', async () => {
    const workspaceId = `ws_s3_${Date.now()}_1k`;
    const payload = randomBytes(1024);
    const sha = createHash('sha256').update(payload).digest('hex');
    await store.put(workspaceId, sha, Readable.from(payload), payload.length);
    const back = await readAll(await store.open(workspaceId, sha));
    expect(back.equals(payload)).toBe(true);
    await store.delete(workspaceId, sha);
  });

  it('round-trips a 1 MiB blob', async () => {
    const workspaceId = `ws_s3_${Date.now()}_1m`;
    const payload = randomBytes(1024 * 1024);
    const sha = createHash('sha256').update(payload).digest('hex');
    await store.put(workspaceId, sha, Readable.from(payload), payload.length);
    const back = await readAll(await store.open(workspaceId, sha));
    expect(back.equals(payload)).toBe(true);
    await store.delete(workspaceId, sha);
  });
});
