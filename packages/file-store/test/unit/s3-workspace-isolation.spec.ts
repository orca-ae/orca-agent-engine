// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { describe, expect, it } from 'vitest';
import { S3BlobStore } from '../../src/blob/s3.js';

class FakeS3Client {
  readonly objects = new Map<string, Buffer>();

  async send(command: unknown): Promise<unknown> {
    if (command instanceof PutObjectCommand) {
      const key = command.input.Key!;
      this.objects.set(key, await readAll(command.input.Body as unknown as NodeJS.ReadableStream));
      return {};
    }
    if (command instanceof GetObjectCommand) {
      const key = command.input.Key!;
      const value = this.objects.get(key);
      if (!value) throw new Error(`NoSuchKey: ${key}`);
      return { Body: Readable.from(value) };
    }
    if (command instanceof DeleteObjectCommand) {
      this.objects.delete(command.input.Key!);
      return {};
    }
    throw new Error('unsupported S3 command');
  }
}

async function readAll(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
  }
  return Buffer.concat(chunks);
}

describe('S3BlobStore workspace namespace', () => {
  it('stores the same sha under distinct workspace object keys', async () => {
    const client = new FakeS3Client();
    const store = new S3BlobStore({
      client: client as unknown as S3Client,
      bucket: 'orca-files',
      keyPrefix: 'configured/root',
    });
    const sha = createHash('sha256').update('same logical digest').digest('hex');

    await store.put('ws_alpha', sha, Readable.from(Buffer.from('alpha')), 5);
    await store.put('ws_beta', sha, Readable.from(Buffer.from('beta')), 4);

    const suffix = `files/blobs/${sha.slice(0, 2)}/${sha.slice(2, 4)}/${sha}/content`;
    expect([...client.objects.keys()].sort()).toEqual(
      [
        `configured/root/workspaces/ws_alpha/${suffix}`,
        `configured/root/workspaces/ws_beta/${suffix}`,
      ].sort(),
    );
    expect(await readAll(await store.open('ws_alpha', sha))).toEqual(Buffer.from('alpha'));
    expect(await readAll(await store.open('ws_beta', sha))).toEqual(Buffer.from('beta'));
  });

  it('cannot read a digest that only exists in another workspace', async () => {
    const client = new FakeS3Client();
    const store = new S3BlobStore({
      client: client as unknown as S3Client,
      bucket: 'orca-files',
      keyPrefix: 'configured/root/',
    });
    const payload = Buffer.from('workspace secret');
    const sha = createHash('sha256').update(payload).digest('hex');
    await store.put('ws_alpha', sha, Readable.from(payload), payload.length);

    await expect(store.open('ws_beta', sha)).rejects.toThrow(/NoSuchKey/);
  });

  it.each(['', '../ws_beta', 'ws/alpha', 'ws*alpha', 'ws\\alpha'])(
    'rejects unsafe workspace id %j before issuing S3 calls',
    async (workspaceId) => {
      const client = new FakeS3Client();
      const store = new S3BlobStore({
        client: client as unknown as S3Client,
        bucket: 'orca-files',
      });
      const sha = 'a'.repeat(64);

      await expect(store.put(workspaceId, sha, Readable.from(Buffer.from('x')), 1)).rejects.toThrow(
        /invalid workspaceId/,
      );
      expect(client.objects.size).toBe(0);
    },
  );
});
