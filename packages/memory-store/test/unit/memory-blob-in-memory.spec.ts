// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { Readable } from 'node:stream';
import { InMemoryMemoryBlobStore } from '../../src/blob/in-memory.js';

const WS = 'ws_test';

async function streamToBuffer(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const c of stream) {
    chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c as unknown as Uint8Array));
  }
  return Buffer.concat(chunks);
}

describe('InMemoryMemoryBlobStore', () => {
  it('round-trips live bytes', async () => {
    const s = new InMemoryMemoryBlobStore();
    const payload = Buffer.from('hello-live');
    await s.putLive(WS, 'mems_x', 'foo/bar.txt', Readable.from(payload), payload.length);
    const got = await streamToBuffer(await s.openLive(WS, 'mems_x', 'foo/bar.txt'));
    expect(got.toString()).toBe('hello-live');
  });

  it('round-trips version bytes', async () => {
    const s = new InMemoryMemoryBlobStore();
    const payload = Buffer.from('v1');
    const sha = 'a'.repeat(64);
    await s.putVersion(WS, 'mems_x', sha, Readable.from(payload), payload.length);
    const got = await streamToBuffer(await s.openVersion(WS, 'mems_x', sha));
    expect(got.toString()).toBe('v1');
  });

  it('listLive returns relative paths and excludes versions', async () => {
    const s = new InMemoryMemoryBlobStore();
    await s.putLive(WS, 'mems_x', 'a.txt', Readable.from(Buffer.from('a')), 1);
    await s.putLive(WS, 'mems_x', 'sub/b.txt', Readable.from(Buffer.from('b')), 1);
    await s.putVersion(WS, 'mems_x', 'b'.repeat(64), Readable.from(Buffer.from('v')), 1);

    const live = await s.listLive(WS, 'mems_x');
    expect(live.sort()).toEqual(['a.txt', 'sub/b.txt']);
  });

  it('listLive scopes to the requested store', async () => {
    const s = new InMemoryMemoryBlobStore();
    await s.putLive(WS, 'mems_x', 'a.txt', Readable.from(Buffer.from('1')), 1);
    await s.putLive(WS, 'mems_y', 'a.txt', Readable.from(Buffer.from('2')), 1);
    expect(await s.listLive(WS, 'mems_x')).toEqual(['a.txt']);
    expect(await s.listLive(WS, 'mems_y')).toEqual(['a.txt']);
  });

  it('listLive scopes to the requested workspace', async () => {
    const s = new InMemoryMemoryBlobStore();
    await s.putLive('ws_a', 'mems_shared', 'a.txt', Readable.from(Buffer.from('1')), 1);
    await s.putLive('ws_b', 'mems_shared', 'b.txt', Readable.from(Buffer.from('2')), 1);
    expect(await s.listLive('ws_a', 'mems_shared')).toEqual(['a.txt']);
    expect(await s.listLive('ws_b', 'mems_shared')).toEqual(['b.txt']);
  });

  it('deleteLive on missing path is a no-op', async () => {
    const s = new InMemoryMemoryBlobStore();
    await expect(s.deleteLive(WS, 'mems_x', 'missing.txt')).resolves.toBeUndefined();
  });

  it('openLive on missing path throws', async () => {
    const s = new InMemoryMemoryBlobStore();
    await expect(s.openLive(WS, 'mems_x', 'nope.txt')).rejects.toThrow(/missing/);
  });

  it('openVersion on missing sha throws', async () => {
    const s = new InMemoryMemoryBlobStore();
    await expect(s.openVersion(WS, 'mems_x', 'c'.repeat(64))).rejects.toThrow(/missing/);
  });

  it('deleteVersion is idempotent', async () => {
    const s = new InMemoryMemoryBlobStore();
    const sha = 'd'.repeat(64);
    await s.putVersion(WS, 'mems_x', sha, Readable.from(Buffer.from('v')), 1);
    await s.deleteVersion(WS, 'mems_x', sha);
    await s.deleteVersion(WS, 'mems_x', sha); // second delete is a no-op
    await expect(s.openVersion(WS, 'mems_x', sha)).rejects.toThrow();
  });

  it('strips leading slash in live path keys', async () => {
    const s = new InMemoryMemoryBlobStore();
    await s.putLive(WS, 'mems_x', '/abs/path.txt', Readable.from(Buffer.from('y')), 1);
    expect(await s.listLive(WS, 'mems_x')).toEqual(['abs/path.txt']);
    const got = await streamToBuffer(await s.openLive(WS, 'mems_x', 'abs/path.txt'));
    expect(got.toString()).toBe('y');
  });

  it('rejects namespace collisions and invalid live paths', async () => {
    const s = new InMemoryMemoryBlobStore();

    await expect(s.listLive('ws_a|mems_b', 'mems_c')).rejects.toThrow(/invalid workspaceId/);
    await expect(s.listLive('ws_a', 'mems_b|mems_c')).rejects.toThrow(/invalid storeId/);
    await expect(s.putLive('ws_a', 'mems_b', '../secret', Readable.from('x'), 1)).rejects.toThrow(
      /invalid relative path/,
    );
  });
});
