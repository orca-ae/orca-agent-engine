// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import type { FileStore, FileRecord, OpenStream } from '@orca/file-store';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import { TarballPrefetchStrategy } from '../../src/sandbox/mounts/tarball-prefetch.js';
import type {
  MountHandle,
  MountResource,
  MountStrategy,
  TornDownState,
} from '../../src/sandbox/mounts/mount-strategy.js';
import type { SandboxHandle } from '../../src/sandbox/sandbox-runtime.js';

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
      throw new Error('not used');
    },
    async get(_ws, id) {
      return id === record.id ? record : null;
    },
    async list() {
      return { items: [record], nextCursor: null };
    },
    async open(_ws, id): Promise<OpenStream | null> {
      if (id !== record.id) return null;
      return { stream: Readable.from(content), sizeBytes: content.length, sha256: record.sha256 };
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

/**
 * Wraps a MountStrategy and counts each lifecycle invocation. Used to assert
 * the snapshot invariant: each phase runs exactly once per snapshot cycle.
 */
class CountingStrategy implements MountStrategy {
  readonly supports: ReadonlyArray<MountResource['type']>;
  counts = { activate: 0, deactivate: 0, teardown: 0, restore: 0 };

  constructor(private readonly inner: MountStrategy) {
    this.supports = inner.supports;
  }

  async activate(sandbox: SandboxHandle, resource: MountResource): Promise<MountHandle> {
    this.counts.activate += 1;
    return this.inner.activate(sandbox, resource);
  }

  async deactivate(sandbox: SandboxHandle, handle: MountHandle): Promise<void> {
    this.counts.deactivate += 1;
    return this.inner.deactivate(sandbox, handle);
  }

  async teardownForSnapshot(sandbox: SandboxHandle, handle: MountHandle): Promise<TornDownState> {
    this.counts.teardown += 1;
    return this.inner.teardownForSnapshot(sandbox, handle);
  }

  async restoreAfterSnapshot(sandbox: SandboxHandle, torn: TornDownState): Promise<MountHandle> {
    this.counts.restore += 1;
    return this.inner.restoreAfterSnapshot(sandbox, torn);
  }
}

describe('sandbox snapshot lifecycle', () => {
  it('teardown -> pause -> resume -> restore preserves the mount; counts = 1 each', async () => {
    const payload = Buffer.from('survive the snapshot');
    const strategy = new CountingStrategy(
      new TarballPrefetchStrategy('ws_x', fakeFileStore(payload)),
    );
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});

    try {
      // 1. Activate the mount.
      const handle = await strategy.activate(sandbox, {
        id: 'sesrsc_test',
        type: 'file',
        fileId: 'file_test',
        mountPath: '/mnt/snap.txt',
        access: 'read_only',
      });
      expect(strategy.counts.activate).toBe(1);
      expect((await sandbox.files.read('/mnt/snap.txt')).toString('utf8')).toBe(
        'survive the snapshot',
      );

      // 2. Teardown for snapshot — MUST run BEFORE pause().
      const torn = await strategy.teardownForSnapshot(sandbox, handle);
      expect(strategy.counts.teardown).toBe(1);
      expect(torn.resourceId).toBe(handle.resourceId);

      // 3. Pause the sandbox.
      await sandbox.pause();

      // 4. Resume.
      await sandbox.resume();

      // 5. Restore after snapshot — MUST run BEFORE next tool dispatch.
      const restored = await strategy.restoreAfterSnapshot(sandbox, torn);
      expect(strategy.counts.restore).toBe(1);
      expect(restored.mountPath).toBe('/mnt/snap.txt');

      // 6. Tarball strategy is stateless across snapshot — file is still readable.
      expect((await sandbox.files.read('/mnt/snap.txt')).toString('utf8')).toBe(
        'survive the snapshot',
      );

      // Sanity: teardown + restore each ran exactly once for this cycle.
      expect(strategy.counts).toMatchObject({
        activate: 1,
        teardown: 1,
        restore: 1,
        deactivate: 0,
      });
    } finally {
      await sandbox.destroy();
    }
  }, 30_000);

  it('multiple mounts: each has teardown + restore called exactly once', async () => {
    const strategy = new CountingStrategy(
      new TarballPrefetchStrategy('ws_x', fakeFileStore(Buffer.from('multi'))),
    );
    const runtime = new InMemorySandboxRuntime();
    const sandbox = await runtime.acquire({});

    try {
      const h1 = await strategy.activate(sandbox, {
        id: 'sesrsc_a',
        type: 'file',
        fileId: 'file_test',
        mountPath: '/mnt/a.txt',
        access: 'read_only',
      });
      const h2 = await strategy.activate(sandbox, {
        id: 'sesrsc_b',
        type: 'file',
        fileId: 'file_test',
        mountPath: '/mnt/b.txt',
        access: 'read_only',
      });
      expect(strategy.counts.activate).toBe(2);

      // Snapshot dance, both handles.
      const t1 = await strategy.teardownForSnapshot(sandbox, h1);
      const t2 = await strategy.teardownForSnapshot(sandbox, h2);
      expect(strategy.counts.teardown).toBe(2);

      await sandbox.pause();
      await sandbox.resume();

      await strategy.restoreAfterSnapshot(sandbox, t1);
      await strategy.restoreAfterSnapshot(sandbox, t2);
      expect(strategy.counts.restore).toBe(2);

      // Both files still readable.
      expect((await sandbox.files.read('/mnt/a.txt')).toString('utf8')).toBe('multi');
      expect((await sandbox.files.read('/mnt/b.txt')).toString('utf8')).toBe('multi');
    } finally {
      await sandbox.destroy();
    }
  }, 30_000);
});
