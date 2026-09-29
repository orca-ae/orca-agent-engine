// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import { setTimeout as delay } from 'node:timers/promises';
import { expect, it, vi } from 'vitest';
import { InMemoryMemoryBlobStore } from '../../src/blob/in-memory.js';
import { LocalMemoryStore } from '../../src/local-memory-store.js';
import { MemoryConflictError } from '../../src/store.js';
import type { MemoryMetadataStore } from '../../src/metadata/store.js';

export function deletionContract(
  metadata: () => MemoryMetadataStore,
  peerMetadata = metadata,
): void {
  const hash = (value: string) => createHash('sha256').update(value).digest('hex');
  const workspaceId = 'ws_delete_checkpoint';
  const setup = async () => {
    const blobs = new InMemoryMemoryBlobStore();
    const store = new LocalMemoryStore({ blobStore: blobs, metadataStore: metadata() });
    const peer = new LocalMemoryStore({ blobStore: blobs, metadataStore: peerMetadata() });
    const parent = await store.createStore({ workspaceId, name: randomUUID() });
    const write = (value: string) =>
      store.writeMemory({
        workspaceId,
        storeId: parent.id,
        path: 'notes.txt',
        content: Readable.from(value),
        sha256: hash(value),
        sizeBytes: Buffer.byteLength(value),
      });
    return { store, peer, blobs, storeId: parent.id, write };
  };

  it('atomically refuses a stale deletion precondition', async () => {
    const f = await setup();
    const { memory } = await f.write('latest');
    await expect(
      f.store.deleteMemory(workspaceId, f.storeId, memory.id, {
        previousSha256: hash('old'),
        versionId: `memver_${randomUUID()}`,
      }),
    ).rejects.toBeInstanceOf(MemoryConflictError);
    expect((await f.store.getMemory(workspaceId, f.storeId, memory.id))!.currentSha256).toBe(
      hash('latest'),
    );
    expect(await f.store.listVersions(workspaceId, f.storeId, memory.id)).toHaveLength(1);
  });

  it('records one deterministic deletion receipt across concurrent retries', async () => {
    const f = await setup();
    const { memory } = await f.write('old');
    const versionId = `memver_${randomUUID()}`;
    const opts = {
      versionId,
      previousSha256: hash('old'),
      sessionId: 'ses_checkpoint',
      writtenByEventId: 'rchk_1',
    };
    const result = await Promise.all([
      f.store.deleteMemory(workspaceId, f.storeId, memory.id, opts),
      f.store.deleteMemory(workspaceId, f.storeId, memory.id, opts),
    ]);
    expect(result.sort()).toEqual([false, true]);
    expect(await f.store.getVersion(workspaceId, f.storeId, versionId)).toMatchObject({
      memoryId: memory.id,
      path: 'notes.txt',
      writtenBySessionId: 'ses_checkpoint',
      writtenByEventId: 'rchk_1',
    });
    expect(await f.store.listVersions(workspaceId, f.storeId, memory.id)).toHaveLength(2);
    expect(await f.blobs.listLive(workspaceId, f.storeId)).toEqual([]);
    await expect(f.blobs.openLive(workspaceId, f.storeId, 'notes.txt')).rejects.toThrow('missing');
    const historical = await f.store.openVersion(workspaceId, f.storeId, versionId);
    let content = '';
    for await (const chunk of historical!.stream) content += String(chunk);
    expect(content).toBe('old');
  });

  it('retries failed live cleanup after the deletion receipt commits', async () => {
    const f = await setup();
    const { memory } = await f.write('old');
    const opts = {
      versionId: `memver_${randomUUID()}`,
      previousSha256: hash('old'),
      previousPath: 'notes.txt',
      sessionId: 'ses_checkpoint',
      writtenByEventId: 'rchk_cleanup',
    };
    const hook = vi
      .spyOn(f.blobs, 'deleteLive')
      .mockRejectedValueOnce(new Error('blob unavailable'));
    try {
      await expect(f.store.deleteMemory(workspaceId, f.storeId, memory.id, opts)).rejects.toThrow(
        'blob unavailable',
      );
      expect(await f.store.getMemory(workspaceId, f.storeId, memory.id)).toBeNull();
      const receipt = await f.store.getVersion(workspaceId, f.storeId, opts.versionId);
      expect(receipt).not.toBeNull();
      expect(await f.blobs.listLive(workspaceId, f.storeId)).toEqual(['notes.txt']);

      await expect(f.peer.deleteMemory(workspaceId, f.storeId, memory.id, opts)).resolves.toBe(
        false,
      );
      expect(hook).toHaveBeenCalledTimes(2);
      expect(await f.blobs.listLive(workspaceId, f.storeId)).toEqual([]);
      expect(await f.store.getVersion(workspaceId, f.storeId, opts.versionId)).toEqual(receipt);
      expect(await f.store.listVersions(workspaceId, f.storeId, memory.id)).toHaveLength(2);
    } finally {
      hook.mockRestore();
    }
  });

  it.each(['old', 'replacement'])(
    'preserves recreated %s bytes when retrying failed live cleanup',
    async (value) => {
      const f = await setup();
      const { memory } = await f.write('old');
      const opts = { versionId: `memver_${randomUUID()}`, previousSha256: hash('old') };
      const hook = vi
        .spyOn(f.blobs, 'deleteLive')
        .mockRejectedValueOnce(new Error('blob unavailable'));
      try {
        await expect(f.store.deleteMemory(workspaceId, f.storeId, memory.id, opts)).rejects.toThrow(
          'blob unavailable',
        );
        const recreated = await f.peer.writeMemory({
          workspaceId,
          storeId: f.storeId,
          path: 'notes.txt',
          content: Readable.from(value),
          sha256: hash(value),
          sizeBytes: Buffer.byteLength(value),
        });
        await expect(f.store.deleteMemory(workspaceId, f.storeId, memory.id, opts)).resolves.toBe(
          false,
        );
        expect(hook).toHaveBeenCalledTimes(1);
        expect(recreated.memory.id).not.toBe(memory.id);
        const opened = await f.store.openMemory(workspaceId, f.storeId, recreated.memory.id);
        let content = '';
        for await (const chunk of opened!.stream) content += String(chunk);
        expect(content).toBe(value);
        expect(await f.store.listVersions(workspaceId, f.storeId, memory.id)).toHaveLength(2);
      } finally {
        hook.mockRestore();
      }
    },
  );

  it('holds the write lock through live cleanup before another writer recreates the path', async () => {
    const f = await setup();
    const { memory } = await f.write('old');
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const remove = f.blobs.deleteLive.bind(f.blobs);
    const hook = vi.spyOn(f.blobs, 'deleteLive').mockImplementationOnce(async (...args) => {
      entered();
      await gate;
      await remove(...args);
    });
    const deletion = f.store.deleteMemory(workspaceId, f.storeId, memory.id, {
      versionId: `memver_${randomUUID()}`,
    });
    await started;
    const replacement = f.peer.writeMemory({
      workspaceId,
      storeId: f.storeId,
      path: 'notes.txt',
      content: Readable.from('replacement'),
      sha256: hash('replacement'),
      sizeBytes: 11,
    });
    try {
      expect(await Promise.race([replacement.then(() => true), delay(100, false)])).toBe(false);
    } finally {
      release();
    }
    try {
      await expect(deletion).resolves.toBe(true);
      const recreated = await replacement;
      const opened = await f.store.openMemory(workspaceId, f.storeId, recreated.memory.id);
      let content = '';
      for await (const chunk of opened!.stream) content += String(chunk);
      expect(content).toBe('replacement');
    } finally {
      hook.mockRestore();
    }
  });

  it('leaves a recreated path untouched when retrying the old occurrence', async () => {
    const f = await setup();
    const { memory } = await f.write('old');
    const opts = { versionId: `memver_${randomUUID()}`, previousSha256: hash('old') };
    await f.store.deleteMemory(workspaceId, f.storeId, memory.id, opts);
    const recreated = await f.write('old');
    await f.store.deleteMemory(workspaceId, f.storeId, memory.id, opts);
    expect(await f.store.getMemory(workspaceId, f.storeId, recreated.memory.id)).not.toBeNull();
    const opened = await f.store.openMemory(workspaceId, f.storeId, recreated.memory.id);
    let content = '';
    for await (const chunk of opened!.stream) content += String(chunk);
    expect(content).toBe('old');
  });

  it('rejects reuse of an existing version identity without deleting the current memory', async () => {
    const f = await setup();
    const { memory, version } = await f.write('old');
    await expect(
      f.store.deleteMemory(workspaceId, f.storeId, memory.id, { versionId: version.id }),
    ).rejects.toThrow('identity');
    expect(await f.store.getMemory(workspaceId, f.storeId, memory.id)).not.toBeNull();
  });

  it('does not erase a replacement live blob after the tombstone commits', async () => {
    const f = await setup();
    const { memory } = await f.write('old');
    const backend = metadata();
    const remove = backend.deleteMemory.bind(backend);
    const hook = vi.spyOn(backend, 'deleteMemory').mockImplementationOnce(async (input) => {
      const result = await remove(input);
      await f.write('replacement');
      return result;
    });
    try {
      await f.store.deleteMemory(workspaceId, f.storeId, memory.id, {
        versionId: `memver_${randomUUID()}`,
      });
      const replacement = await f.store.getMemoryByPath(workspaceId, f.storeId, 'notes.txt');
      const opened = await f.store.openMemory(workspaceId, f.storeId, replacement!.id);
      let content = '';
      for await (const chunk of opened!.stream) content += String(chunk);
      expect(content).toBe('replacement');
    } finally {
      hook.mockRestore();
    }
  });

  it('repairs an old immutable version without overwriting a recreated live path', async () => {
    const f = await setup();
    const original = await f.write('old');
    await f.store.deleteMemory(workspaceId, f.storeId, original.memory.id);
    const replacement = await f.write('replacement');
    await f.store.writeMemory({
      workspaceId,
      storeId: f.storeId,
      path: 'notes.txt',
      versionId: original.version.id,
      content: Readable.from('old'),
      sizeBytes: 3,
      sha256: hash('old'),
    });
    const opened = await f.store.openMemory(workspaceId, f.storeId, replacement.memory.id);
    let content = '';
    for await (const chunk of opened!.stream) content += String(chunk);
    expect(content).toBe('replacement');
  });

  it('serializes an in-flight live repair with a different writer deleting and recreating the path', async () => {
    const f = await setup();
    const original = await f.write('old');
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const put = f.blobs.putLive.bind(f.blobs);
    const hook = vi.spyOn(f.blobs, 'putLive').mockImplementationOnce(async (...args) => {
      entered();
      await gate;
      await put(...args);
    });
    const replay = f.store.writeMemory({
      workspaceId,
      storeId: f.storeId,
      path: 'notes.txt',
      versionId: original.version.id,
      content: Readable.from('old'),
      sizeBytes: 3,
      sha256: hash('old'),
    });
    await started;
    const replace = (async () => {
      await f.peer.deleteMemory(workspaceId, f.storeId, original.memory.id);
      return f.peer.writeMemory({
        workspaceId,
        storeId: f.storeId,
        path: 'notes.txt',
        content: Readable.from('replacement'),
        sizeBytes: 11,
        sha256: hash('replacement'),
      });
    })();
    try {
      expect(await Promise.race([replace.then(() => true), delay(100, false)])).toBe(false);
    } finally {
      release();
    }
    await replay;
    const replacement = await replace;
    hook.mockRestore();
    const opened = await f.peer.openMemory(workspaceId, f.storeId, replacement.memory.id);
    let content = '';
    for await (const chunk of opened!.stream) content += String(chunk);
    expect(content).toBe('replacement');
  });

  it('checks original path atomically and releases locks after a failed delete', async () => {
    const f = await setup();
    const original = await f.write('old');
    await f.store.writeMemory({
      workspaceId,
      storeId: f.storeId,
      memoryId: original.memory.id,
      path: 'renamed.txt',
      content: Readable.from('old'),
      sizeBytes: 3,
      sha256: hash('old'),
    });
    await expect(
      f.store.deleteMemory(workspaceId, f.storeId, original.memory.id, {
        previousPath: 'notes.txt',
        previousSha256: hash('old'),
        versionId: `memver_${randomUUID()}`,
      }),
    ).rejects.toThrow('path changed');
    expect(await f.store.getMemoryByPath(workspaceId, f.storeId, 'renamed.txt')).not.toBeNull();
    await expect(f.write('after failure')).resolves.toBeDefined();
  });
}
