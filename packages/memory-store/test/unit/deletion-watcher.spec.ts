// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { GetObjectCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { expect, it, vi } from 'vitest';
import { MemoryVersionWatcher } from '../../../../services/harness-server/src/sandbox/memory/version-watcher.js';
import { InMemoryMemoryBlobStore } from '../../src/blob/in-memory.js';
import { InMemoryMemoryMetadataStore } from '../../src/metadata/in-memory.js';
import { LocalMemoryStore } from '../../src/local-memory-store.js';

type WatcherOptions = ConstructorParameters<typeof MemoryVersionWatcher>[0];

it('does not resurrect a checkpoint-deleted memory when a fresh watcher scans the live prefix', async () => {
  const workspaceId = 'ws_delete_watcher';
  const blobs = new InMemoryMemoryBlobStore();
  const store = new LocalMemoryStore({
    blobStore: blobs,
    metadataStore: new InMemoryMemoryMetadataStore(),
  });
  const parent = await store.createStore({ workspaceId, name: 'notes' });
  const { memory } = await store.writeMemory({
    workspaceId,
    storeId: parent.id,
    path: 'notes.txt',
    content: Readable.from('old'),
    sha256: createHash('sha256').update('old').digest('hex'),
    sizeBytes: 3,
  });
  await store.deleteMemory(workspaceId, parent.id, memory.id, {
    versionId: 'memver_delete_watcher',
  });

  const recordVersion = vi.fn(
    async (input: Parameters<WatcherOptions['registry']['recordSessionMemoryVersion']>[0]) => {
      const bytes = Buffer.from(input.contentBase64, 'base64');
      const result = await store.writeMemory({
        workspaceId,
        storeId: parent.id,
        path: input.path,
        content: Readable.from(bytes),
        sizeBytes: bytes.length,
        sha256: input.contentSha256,
      });
      return { ...result, conflict: false };
    },
  );
  const prefix = `memory/workspaces/${workspaceId}/memory-stores/${parent.id}/live/`;
  const s3 = new S3Client({ region: 'us-east-1' });
  const send = vi.spyOn(s3, 'send').mockImplementation(async (command) => {
    if (command instanceof ListObjectsV2Command) {
      expect(command.input.Prefix).toBe(prefix);
      const paths = await blobs.listLive(workspaceId, parent.id);
      return { Contents: paths.map((path) => ({ Key: `${prefix}${path}` })) };
    }
    if (command instanceof GetObjectCommand) {
      const path = command.input.Key!.slice(prefix.length);
      return { Body: await blobs.openLive(workspaceId, parent.id, path) };
    }
    throw new Error('unexpected S3 command');
  });
  const watcher = new MemoryVersionWatcher({
    workspaceId,
    sessionId: 'ses_delete_watcher',
    stores: [{ storeId: parent.id, storeName: 'notes', mountPath: '/mnt/memory/notes' }],
    registry: {
      listSessionMemories: () => store.listMemories(workspaceId, parent.id),
      recordSessionMemoryVersion: recordVersion,
    } as unknown as WatcherOptions['registry'],
    store: { append: vi.fn() } as unknown as WatcherOptions['store'],
    s3,
    bucket: 'memory-test',
    intervalMs: 0,
  });
  try {
    await watcher.start();
    expect(await watcher.tick()).toEqual({
      storesScanned: 1,
      versionsRecorded: 0,
      conflicts: 0,
      errors: 0,
    });
    expect(send).toHaveBeenCalledTimes(1);
    expect(recordVersion).not.toHaveBeenCalled();
    expect(await store.listMemories(workspaceId, parent.id)).toEqual([]);
    expect(await store.listVersions(workspaceId, parent.id, memory.id)).toHaveLength(2);
  } finally {
    await watcher.stop();
    send.mockRestore();
    s3.destroy();
  }
});
