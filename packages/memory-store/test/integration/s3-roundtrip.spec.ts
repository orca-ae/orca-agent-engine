// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { CreateBucketCommand, HeadBucketCommand, S3Client } from '@aws-sdk/client-s3';
import { Readable } from 'node:stream';
import { randomBytes } from 'node:crypto';
import { S3MemoryBlobStore } from '../../src/blob/s3.js';

const ENDPOINT = process.env['S3_ENDPOINT'] ?? 'http://localhost:9000';
const ACCESS_KEY = process.env['S3_ACCESS_KEY_ID'] ?? 'minioadmin';
const SECRET_KEY = process.env['S3_SECRET_ACCESS_KEY'] ?? 'minioadmin';
const BUCKET = process.env['S3_BUCKET'] ?? 'orca-files';
const REGION = process.env['S3_REGION'] ?? 'us-east-1';

async function readAll(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as Uint8Array));
  }
  return Buffer.concat(chunks);
}

function uniqueWorkspaceId(): string {
  return `ws_${Date.now()}_${randomBytes(4).toString('hex')}`;
}

describe('S3MemoryBlobStore (integration)', () => {
  let client: S3Client;
  let store: S3MemoryBlobStore;

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
    // A single workspace-agnostic instance; tests pass the per-call
    // workspaceId instead.
    store = new S3MemoryBlobStore({
      client,
      bucket: BUCKET,
      keyPrefix: 'test/memory/',
    });
  });

  afterAll(async () => {
    client?.destroy();
  });

  it('round-trips a 1 KiB live blob', async () => {
    const ws = uniqueWorkspaceId();
    const payload = randomBytes(1024);
    await store.putLive(ws, 'mems_a', 'docs/note.txt', Readable.from(payload), payload.length);
    const back = await readAll(await store.openLive(ws, 'mems_a', 'docs/note.txt'));
    expect(back.equals(payload)).toBe(true);
    await store.deleteLive(ws, 'mems_a', 'docs/note.txt');
  });

  it('round-trips a 1 MiB live blob', async () => {
    const ws = uniqueWorkspaceId();
    const payload = randomBytes(1024 * 1024);
    await store.putLive(ws, 'mems_a', 'big.bin', Readable.from(payload), payload.length);
    const back = await readAll(await store.openLive(ws, 'mems_a', 'big.bin'));
    expect(back.equals(payload)).toBe(true);
    await store.deleteLive(ws, 'mems_a', 'big.bin');
  });

  it('round-trips a 1 KiB version blob', async () => {
    const ws = uniqueWorkspaceId();
    const payload = randomBytes(1024);
    const sha = randomBytes(32).toString('hex');
    await store.putVersion(ws, 'mems_a', sha, Readable.from(payload), payload.length);
    const back = await readAll(await store.openVersion(ws, 'mems_a', sha));
    expect(back.equals(payload)).toBe(true);
    await store.deleteVersion(ws, 'mems_a', sha);
  });

  it('round-trips a 1 MiB version blob', async () => {
    const ws = uniqueWorkspaceId();
    const payload = randomBytes(1024 * 1024);
    const sha = randomBytes(32).toString('hex');
    await store.putVersion(ws, 'mems_a', sha, Readable.from(payload), payload.length);
    const back = await readAll(await store.openVersion(ws, 'mems_a', sha));
    expect(back.equals(payload)).toBe(true);
    await store.deleteVersion(ws, 'mems_a', sha);
  });

  it('listLive returns the live paths and excludes the .versions/ subprefix', async () => {
    const ws = uniqueWorkspaceId();
    await store.putLive(ws, 'mems_a', 'a.txt', Readable.from(Buffer.from('a')), 1);
    await store.putLive(ws, 'mems_a', 'sub/b.txt', Readable.from(Buffer.from('b')), 1);
    const sha = randomBytes(32).toString('hex');
    await store.putVersion(ws, 'mems_a', sha, Readable.from(Buffer.from('v')), 1);

    const live = await store.listLive(ws, 'mems_a');
    expect(live.sort()).toEqual(['a.txt', 'sub/b.txt']);

    await store.deleteLive(ws, 'mems_a', 'a.txt');
    await store.deleteLive(ws, 'mems_a', 'sub/b.txt');
    await store.deleteVersion(ws, 'mems_a', sha);
  });

  it('listLive on an empty store returns []', async () => {
    const ws = uniqueWorkspaceId();
    const live = await store.listLive(ws, 'mems_empty');
    expect(live).toEqual([]);
  });

  it('per-call workspaceId isolates writes across workspaces', async () => {
    const wsA = uniqueWorkspaceId();
    const wsB = uniqueWorkspaceId();
    await store.putLive(wsA, 'mems_shared', 'x.txt', Readable.from(Buffer.from('A')), 1);
    await store.putLive(wsB, 'mems_shared', 'y.txt', Readable.from(Buffer.from('B')), 1);
    const liveA = await store.listLive(wsA, 'mems_shared');
    const liveB = await store.listLive(wsB, 'mems_shared');
    expect(liveA).toEqual(['x.txt']);
    expect(liveB).toEqual(['y.txt']);
    await store.deleteLive(wsA, 'mems_shared', 'x.txt');
    await store.deleteLive(wsB, 'mems_shared', 'y.txt');
  });
});
