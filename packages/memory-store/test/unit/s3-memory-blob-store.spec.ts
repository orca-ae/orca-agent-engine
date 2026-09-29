// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import { Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import { S3MemoryBlobStore } from '../../src/blob/s3.js';

function makeStore() {
  const send = vi
    .fn()
    .mockImplementation((command: unknown) =>
      Promise.resolve(
        command instanceof GetObjectCommand ? { Body: Readable.from('x') } : { Contents: [] },
      ),
    );
  const client = { send } as unknown as S3Client;
  return {
    send,
    store: new S3MemoryBlobStore({ client, bucket: 'orca', keyPrefix: 'tenant-data/' }),
  };
}

describe('S3MemoryBlobStore workspace keyspace', () => {
  it('keeps live and version data in separate workspace/store namespaces', async () => {
    const { send, store } = makeStore();

    await store.putLive('ws_a', 'mems_1', 'notes/a.txt', Readable.from('a'), 1);
    await store.putVersion('ws_a', 'mems_1', 'a'.repeat(64), Readable.from('a'), 1);

    const live = send.mock.calls[0]![0] as PutObjectCommand;
    const version = send.mock.calls[1]![0] as PutObjectCommand;
    expect(live.input.Key).toBe(
      'tenant-data/workspaces/ws_a/memory-stores/mems_1/live/notes/a.txt',
    );
    expect(version.input.Key).toBe(
      `tenant-data/workspaces/ws_a/memory-stores/mems_1/versions/${'a'.repeat(64)}`,
    );
  });

  it('lists only the mounted live prefix for the requested workspace', async () => {
    const send = vi.fn().mockResolvedValue({
      Contents: [
        {
          Key: 'tenant-data/workspaces/ws_b/memory-stores/mems_1/live/notes/b.txt',
        },
      ],
      IsTruncated: false,
    });
    const store = new S3MemoryBlobStore({
      client: { send } as unknown as S3Client,
      bucket: 'orca',
      keyPrefix: 'tenant-data/',
    });

    await expect(store.listLive('ws_b', 'mems_1')).resolves.toEqual(['notes/b.txt']);
    const command = send.mock.calls[0]![0] as ListObjectsV2Command;
    expect(command.input.Prefix).toBe('tenant-data/workspaces/ws_b/memory-stores/mems_1/live/');
  });

  it('uses a different object key for the same store/path in another workspace', async () => {
    const { send, store } = makeStore();

    await store.openLive('ws_a', 'mems_shared', 'x.txt');
    await store.openLive('ws_b', 'mems_shared', 'x.txt');

    const first = send.mock.calls[0]![0] as GetObjectCommand;
    const second = send.mock.calls[1]![0] as GetObjectCommand;
    expect(first.input.Key).not.toBe(second.input.Key);
    expect(first.input.Key).toContain('/workspaces/ws_a/');
    expect(second.input.Key).toContain('/workspaces/ws_b/');
  });

  it.each(['_team', '-team'])('accepts auth-valid workspace id %s', async (workspaceId) => {
    const { send, store } = makeStore();

    await store.openLive(workspaceId, 'mems_shared', 'x.txt');

    const command = send.mock.calls[0]![0] as GetObjectCommand;
    expect(command.input.Key).toContain(`/workspaces/${workspaceId}/`);
  });

  it('rejects tenant identifiers and paths that can escape their namespace', async () => {
    const { store } = makeStore();

    await expect(store.listLive('../ws_b', 'mems_1')).rejects.toThrow(/invalid workspaceId/);
    await expect(
      store.putLive('ws_a', 'mems_1', '../secret', Readable.from('x'), 1),
    ).rejects.toThrow(/invalid relative path/);
  });

  it.each(['a'.repeat(63), 'A'.repeat(64), `${'a'.repeat(63)}/`])(
    'rejects invalid version digest %s',
    async (sha256) => {
      const { store } = makeStore();

      await expect(
        store.putVersion('ws_a', 'mems_1', sha256, Readable.from('x'), 1),
      ).rejects.toThrow(/invalid sha256/);
    },
  );

  it('rejects an object-store root containing traversal or IAM wildcard segments', () => {
    const client = { send: vi.fn() } as unknown as S3Client;

    expect(
      () => new S3MemoryBlobStore({ client, bucket: 'orca', keyPrefix: '../private/' }),
    ).toThrow(/invalid keyPrefix/);
    expect(() => new S3MemoryBlobStore({ client, bucket: 'orca', keyPrefix: 'tenant/*/' })).toThrow(
      /invalid keyPrefix/,
    );
  });
});
