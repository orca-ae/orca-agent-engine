// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import type { FileStore, FileRecord, OpenStream } from '@orca/file-store';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import { TarballPrefetchStrategy } from '../../src/sandbox/mounts/tarball-prefetch.js';

function fakeFileStore(content: Buffer): FileStore {
  const record: FileRecord = {
    id: 'file_test',
    workspaceId: 'ws_x',
    filename: 'data.txt',
    mimeType: 'text/plain',
    sizeBytes: content.length,
    sha256: 'a'.repeat(64),
    metadata: {},
    archivedAt: null,
    createdAt: new Date(),
    updatedAt: new Date(),
  };
  return {
    async create() {
      throw new Error('not used in this test');
    },
    async get(_ws, id) {
      return id === record.id ? record : null;
    },
    async list() {
      return { items: [record], nextCursor: null };
    },
    async open(_ws, id): Promise<OpenStream | null> {
      if (id !== record.id) return null;
      return {
        stream: Readable.from(content),
        sizeBytes: content.length,
        sha256: record.sha256,
      };
    },
    async archive() {
      /* no-op */
    },
    async delete() {
      /* no-op */
    },
    async close() {
      /* no-op */
    },
  };
}

describe('TarballPrefetchStrategy', () => {
  it('activates a file resource by writing bytes at mountPath', async () => {
    const payload = Buffer.from('hello phase 5');
    const strategy = new TarballPrefetchStrategy('ws_x', fakeFileStore(payload));
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      const handle = await strategy.activate(sb, {
        id: 'sesrsc_1',
        type: 'file',
        fileId: 'file_test',
        mountPath: '/mnt/data.txt',
        access: 'read_only',
      });
      expect(handle.mountPath).toBe('/mnt/data.txt');
      expect(handle.resourceType).toBe('file');
      const back = await sb.files.read('/mnt/data.txt');
      expect(back.toString('utf8')).toBe('hello phase 5');
    } finally {
      await sb.destroy();
    }
  });

  it('deactivate removes the file at mountPath', async () => {
    const strategy = new TarballPrefetchStrategy('ws_x', fakeFileStore(Buffer.from('x')));
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      const handle = await strategy.activate(sb, {
        id: 'sesrsc_2',
        type: 'file',
        fileId: 'file_test',
        mountPath: '/mnt/d.txt',
        access: 'read_only',
      });
      await strategy.deactivate(sb, handle);
      await expect(sb.files.read('/mnt/d.txt')).rejects.toThrow();
    } finally {
      await sb.destroy();
    }
  });

  it('teardown + restore are no-ops for stateless strategy (snapshot hooks present)', async () => {
    const strategy = new TarballPrefetchStrategy('ws_x', fakeFileStore(Buffer.from('persist me')));
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      const handle = await strategy.activate(sb, {
        id: 'sesrsc_3',
        type: 'file',
        fileId: 'file_test',
        mountPath: '/mnt/p.txt',
        access: 'read_only',
      });
      const torn = await strategy.teardownForSnapshot(sb, handle);
      expect(torn.serializedState).toBeUndefined();
      await sb.pause();
      await sb.resume();
      const restored = await strategy.restoreAfterSnapshot(sb, torn);
      expect(restored.mountPath).toBe('/mnt/p.txt');
      // File still readable: tarball strategy is stateless across snapshot.
      expect((await sb.files.read('/mnt/p.txt')).toString('utf8')).toBe('persist me');
    } finally {
      await sb.destroy();
    }
  });

  it('rejects unsupported resource types', async () => {
    const strategy = new TarballPrefetchStrategy('ws_x', fakeFileStore(Buffer.alloc(0)));
    const rt = new InMemorySandboxRuntime();
    const sb = await rt.acquire({});
    try {
      await expect(
        strategy.activate(sb, {
          id: 'sesrsc_x',
          type: 'memory_store',
          memoryStoreId: 'mems_1',
          storeName: 'prefs',
          mountPath: '/mnt/mem',
          access: 'read_write',
        }),
      ).rejects.toThrow(/unsupported/);
    } finally {
      await sb.destroy();
    }
  });
});
