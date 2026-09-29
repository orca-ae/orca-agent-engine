// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import { InMemoryBlobStore } from '../../src/blob/in-memory.js';

const SHA_A = 'a'.repeat(64);

async function readAll(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array));
  }
  return Buffer.concat(chunks);
}

describe('InMemoryBlobStore', () => {
  it('round-trips bytes', async () => {
    const store = new InMemoryBlobStore();
    const payload = Buffer.from('hello world');
    await store.put('ws_alpha', SHA_A, Readable.from(payload), payload.length);
    const back = await readAll(await store.open('ws_alpha', SHA_A));
    expect(back).toEqual(payload);
    expect(store.size()).toBe(1);
  });

  it('delete removes the blob', async () => {
    const store = new InMemoryBlobStore();
    await store.put('ws_alpha', SHA_A, Readable.from(Buffer.from('x')), 1);
    await store.delete('ws_alpha', SHA_A);
    expect(store.size()).toBe(0);
    await expect(store.open('ws_alpha', SHA_A)).rejects.toThrow(/not found/);
  });

  it('open of unknown blob throws', async () => {
    const store = new InMemoryBlobStore();
    await expect(store.open('ws_alpha', SHA_A)).rejects.toThrow(/not found/);
  });

  it('keeps the same sha physically separate across workspaces', async () => {
    const store = new InMemoryBlobStore();
    await store.put('ws_alpha', SHA_A, Readable.from(Buffer.from('alpha')), 5);
    await store.put('ws_beta', SHA_A, Readable.from(Buffer.from('beta')), 4);

    expect(store.size()).toBe(2);
    expect(await readAll(await store.open('ws_alpha', SHA_A))).toEqual(Buffer.from('alpha'));
    expect(await readAll(await store.open('ws_beta', SHA_A))).toEqual(Buffer.from('beta'));
  });

  it('does not read another workspace blob by sha', async () => {
    const store = new InMemoryBlobStore();
    await store.put('ws_alpha', SHA_A, Readable.from(Buffer.from('secret')), 6);

    await expect(store.open('ws_beta', SHA_A)).rejects.toThrow(/not found in workspace ws_beta/);
  });
});
