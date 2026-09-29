// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it } from 'vitest';
import { InMemoryMemoryMetadataStore } from '../../src/metadata/in-memory.js';

const ws = 'ws_unit';

function shaA(): string {
  return 'a'.repeat(64);
}
function shaB(): string {
  return 'b'.repeat(64);
}
function shaC(): string {
  return 'c'.repeat(64);
}

describe('InMemoryMemoryMetadataStore', () => {
  let store: InMemoryMemoryMetadataStore;
  beforeEach(() => {
    store = new InMemoryMemoryMetadataStore();
  });

  it('narrows page histories and active memories by IDs without changing ordering or tenant scope', async () => {
    await store.insertStore({
      id: 'mems_ids',
      workspaceId: ws,
      name: 'ids',
      description: null,
      archivedAt: null,
    });
    for (const [id, versionId] of [
      ['mem_a', 'memver_a1'],
      ['mem_b', 'memver_b1'],
      ['mem_a', 'memver_a2'],
    ]) {
      await store.upsertMemory({
        id: id!,
        versionId: versionId!,
        workspaceId: ws,
        storeId: 'mems_ids',
        path: `${id}.txt`,
        sha256: shaA(),
        sizeBytes: 1,
      });
    }
    const all = await store.listAllVersions(ws, 'mems_ids');
    expect(await store.listAllVersions(ws, 'mems_ids', { memoryIds: ['mem_a', 'mem_a'] })).toEqual(
      all.filter((v) => v.memoryId === 'mem_a'),
    );
    expect(await store.listMemories(ws, 'mems_ids', { memoryIds: ['mem_a'] })).toEqual(
      (await store.listMemories(ws, 'mems_ids')).filter((m) => m.id === 'mem_a'),
    );
    for (const opts of [{ memoryIds: [] }, { memoryIds: ['mem_missing'] }]) {
      expect(await store.listAllVersions(ws, 'mems_ids', opts)).toEqual([]);
      expect(await store.listMemories(ws, 'mems_ids', opts)).toEqual([]);
    }
    expect(await store.listAllVersions('ws_foreign', 'mems_ids', { memoryIds: ['mem_a'] })).toEqual(
      [],
    );
    expect(await store.listMemories('ws_foreign', 'mems_ids', { memoryIds: ['mem_a'] })).toEqual(
      [],
    );
    await store.deleteMemory({
      workspaceId: ws,
      storeId: 'mems_ids',
      memoryId: 'mem_a',
      versionId: 'memver_deleted',
    });
    expect(await store.listMemories(ws, 'mems_ids', { memoryIds: ['mem_a'] })).toEqual([]);
    expect(await store.listAllVersions(ws, 'mems_ids', { memoryIds: ['mem_a'] })).toHaveLength(3);
  });

  describe('store CRUD', () => {
    it('insertStore + getStore round-trip', async () => {
      const inserted = await store.insertStore({
        id: 'mems_001',
        workspaceId: ws,
        name: 'docs',
        description: 'design notes',
        metadata: { owner: 'platform' },
        archivedAt: null,
      });
      expect(inserted.id).toBe('mems_001');
      expect(inserted.workspaceId).toBe(ws);
      expect(inserted.createdAt).toBeInstanceOf(Date);
      expect(inserted.updatedAt).toBeInstanceOf(Date);

      const fetched = await store.getStore(ws, 'mems_001');
      expect(fetched?.name).toBe('docs');
      expect(fetched?.description).toBe('design notes');
      expect(fetched?.metadata).toEqual({ owner: 'platform' });

      // Cross-workspace lookup must miss.
      const wrongWs = await store.getStore('ws_other', 'mems_001');
      expect(wrongWs).toBeNull();
    });

    it('listStores cursor-paginates ascending by id', async () => {
      for (const id of ['mems_a', 'mems_b', 'mems_c', 'mems_d', 'mems_e']) {
        await store.insertStore({
          id,
          workspaceId: ws,
          name: id,
          description: null,
          archivedAt: null,
        });
      }
      const page1 = await store.listStores(ws, { limit: 2 });
      expect(page1.items.map((s) => s.id)).toEqual(['mems_a', 'mems_b']);
      expect(page1.nextCursor).toBe('mems_b');

      const page2 = await store.listStores(ws, { limit: 2, cursor: page1.nextCursor! });
      expect(page2.items.map((s) => s.id)).toEqual(['mems_c', 'mems_d']);
      expect(page2.nextCursor).toBe('mems_d');

      const page3 = await store.listStores(ws, { limit: 2, cursor: page2.nextCursor! });
      expect(page3.items.map((s) => s.id)).toEqual(['mems_e']);
      expect(page3.nextCursor).toBeNull();
    });

    it('listStores filters by workspace', async () => {
      await store.insertStore({
        id: 'mems_a',
        workspaceId: 'ws_a',
        name: 'a',
        description: null,
        archivedAt: null,
      });
      await store.insertStore({
        id: 'mems_b',
        workspaceId: 'ws_b',
        name: 'b',
        description: null,
        archivedAt: null,
      });
      const a = await store.listStores('ws_a', { limit: 100 });
      expect(a.items.map((s) => s.id)).toEqual(['mems_a']);
      const b = await store.listStores('ws_b', { limit: 100 });
      expect(b.items.map((s) => s.id)).toEqual(['mems_b']);
    });

    it('listStores applies archive filters before offset pagination', async () => {
      for (const id of ['mems_a', 'mems_b', 'mems_c', 'mems_d']) {
        await store.insertStore({
          id,
          workspaceId: ws,
          name: id,
          description: null,
          archivedAt: null,
        });
      }
      await store.archiveStore(ws, 'mems_a');

      const page = await store.listStores(ws, {
        limit: 2,
        offset: 1,
        includeArchived: false,
      });
      expect(page.items.map((item) => item.id)).toEqual(['mems_c', 'mems_d']);
      expect(page.nextCursor).toBeNull();
    });

    it('archiveStore sets archivedAt', async () => {
      await store.insertStore({
        id: 'mems_a',
        workspaceId: ws,
        name: 'docs',
        description: null,
        archivedAt: null,
      });
      await store.archiveStore(ws, 'mems_a');
      const after = await store.getStore(ws, 'mems_a');
      expect(after?.archivedAt).toBeInstanceOf(Date);
    });

    it('updateStore changes name and description within workspace', async () => {
      await store.insertStore({
        id: 'mems_a',
        workspaceId: ws,
        name: 'docs',
        description: 'before',
        metadata: { owner: 'platform' },
        archivedAt: null,
      });
      const updated = await store.updateStore(ws, 'mems_a', {
        name: 'notes',
        description: null,
        metadata: { owner: 'runtime' },
      });
      expect(updated).toMatchObject({
        id: 'mems_a',
        name: 'notes',
        description: null,
        metadata: { owner: 'runtime' },
      });
      expect(await store.updateStore('ws_other', 'mems_a', { name: 'x' })).toBeNull();
    });

    it('preserves reserved metadata keys as data properties', async () => {
      const inserted = await store.insertStore({
        id: 'mems_reserved',
        workspaceId: ws,
        name: 'reserved',
        description: null,
        metadata: metadataWithReservedKeys('literal-prototype'),
        archivedAt: null,
      });
      expect(Object.getPrototypeOf(inserted.metadata)).toBeNull();
      expect(Object.hasOwn(inserted.metadata, '__proto__')).toBe(true);
      expect(inserted.metadata['__proto__']).toBe('literal-prototype');
      expect(inserted.metadata.constructor).toBe('literal-constructor');

      const updated = await store.updateStore(ws, 'mems_reserved', {
        metadata: metadataWithReservedKeys('updated-prototype'),
      });
      expect(updated).not.toBeNull();
      expect(Object.getPrototypeOf(updated!.metadata)).toBeNull();
      expect(Object.hasOwn(updated!.metadata, '__proto__')).toBe(true);
      expect(updated!.metadata['__proto__']).toBe('updated-prototype');
      expect(updated!.metadata.constructor).toBe('literal-constructor');
    });

    it('deleteStore clears stores + memories + versions', async () => {
      await store.insertStore({
        id: 'mems_a',
        workspaceId: ws,
        name: 'docs',
        description: null,
        archivedAt: null,
      });
      const upsert1 = await store.upsertMemory({
        id: 'mem_1',
        versionId: 'memver_1',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'a.md',
        sha256: shaA(),
        sizeBytes: 10,
      });
      expect(upsert1.ok).toBe(true);

      await store.deleteStore(ws, 'mems_a');
      expect(await store.getStore(ws, 'mems_a')).toBeNull();
      expect(await store.listMemories(ws, 'mems_a')).toEqual([]);
      expect(await store.listVersions(ws, 'mems_a', 'mem_1')).toEqual([]);
    });
  });

  describe('upsertMemory', () => {
    beforeEach(async () => {
      await store.insertStore({
        id: 'mems_a',
        workspaceId: ws,
        name: 'docs',
        description: null,
        archivedAt: null,
      });
    });

    it('first write inserts memory + version (no CAS)', async () => {
      const r = await store.upsertMemory({
        id: 'mem_1',
        versionId: 'memver_1',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaA(),
        sizeBytes: 12,
      });
      expect(r.ok).toBe(true);
      if (!r.ok) return;
      expect(r.memory.id).toBe('mem_1');
      expect(r.memory.currentSha256).toBe(shaA());
      expect(r.version.id).toBe('memver_1');
      expect(r.version.memoryId).toBe('mem_1');
      expect(r.version.sha256).toBe(shaA());
      expect(r.version.redactedAt).toBeNull();

      const versions = await store.listVersions(ws, 'mems_a', 'mem_1');
      expect(versions).toHaveLength(1);
      await expect(store.getVersion(ws, 'mems_a', 'memver_1')).resolves.toMatchObject({
        id: 'memver_1',
        sha256: shaA(),
      });
      await expect(store.getVersion(ws, 'mems_b', 'memver_1')).resolves.toBeNull();
    });

    it('does not create a new memory when an explicit memoryId is missing', async () => {
      await expect(
        store.upsertMemory({
          id: 'mem_generated',
          memoryId: 'mem_missing',
          versionId: 'memver_missing',
          workspaceId: ws,
          storeId: 'mems_a',
          path: 'note.md',
          sha256: shaA(),
          sizeBytes: 12,
        }),
      ).rejects.toThrow('upsertMemory: memory mem_missing not found');
      expect(await store.getMemoryByPath(ws, 'mems_a', 'note.md')).toBeNull();
    });

    it('subsequent write with matching previousSha updates memory + appends version', async () => {
      await store.upsertMemory({
        id: 'mem_1',
        versionId: 'memver_1',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaA(),
        sizeBytes: 10,
      });
      const r = await store.upsertMemory({
        id: 'mem_2', // ignored — existing memory id is reused
        versionId: 'memver_2',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaB(),
        sizeBytes: 20,
        previousSha256: shaA(),
      });
      expect(r.ok).toBe(true);
      if (!r.ok) return;
      expect(r.memory.id).toBe('mem_1'); // memory id is stable
      expect(r.memory.currentSha256).toBe(shaB());
      expect(r.memory.sizeBytes).toBe(20);
      expect(r.version.id).toBe('memver_2');
      expect(r.version.memoryId).toBe('mem_1');

      const versions = await store.listVersions(ws, 'mems_a', 'mem_1');
      expect(versions).toHaveLength(2);
    });

    it('deleteMemory removes the live memory and appends a tombstone version', async () => {
      await store.upsertMemory({
        id: 'mem_1',
        versionId: 'memver_1',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaA(),
        sizeBytes: 10,
      });

      await expect(
        store.deleteMemory({
          workspaceId: ws,
          storeId: 'mems_a',
          memoryId: 'mem_1',
          versionId: 'memver_deleted',
          writtenByApiKeyId: 'key_delete',
        }),
      ).resolves.toBe(true);
      await expect(store.getMemory(ws, 'mems_a', 'mem_1')).resolves.toBeNull();
      await expect(store.listVersions(ws, 'mems_a', 'mem_1')).resolves.toHaveLength(2);
      await expect(store.getVersion(ws, 'mems_a', 'memver_deleted')).resolves.toMatchObject({
        writtenByApiKeyId: 'key_delete',
      });
      await expect(
        store.deleteMemory({
          workspaceId: ws,
          storeId: 'mems_a',
          memoryId: 'mem_1',
          versionId: 'memver_deleted_again',
        }),
      ).resolves.toBe(false);
    });

    it('subsequent write with mismatched previousSha returns ok=false', async () => {
      await store.upsertMemory({
        id: 'mem_1',
        versionId: 'memver_1',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaA(),
        sizeBytes: 10,
      });
      const r = await store.upsertMemory({
        id: 'mem_X',
        versionId: 'memver_X',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaB(),
        sizeBytes: 20,
        previousSha256: shaC(), // ≠ shaA()
      });
      expect(r.ok).toBe(false);
      if (r.ok) return;
      expect(r.observedSha).toBe(shaA());
      expect(r.expectedSha).toBe(shaC());

      // Memory unchanged.
      const m = await store.getMemoryByPath(ws, 'mems_a', 'note.md');
      expect(m?.currentSha256).toBe(shaA());
      // No second version row was inserted.
      expect(await store.listVersions(ws, 'mems_a', 'mem_1')).toHaveLength(1);
    });

    it('first-write with previousSha set returns ok=false with empty observedSha', async () => {
      const r = await store.upsertMemory({
        id: 'mem_X',
        versionId: 'memver_X',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'fresh.md',
        sha256: shaA(),
        sizeBytes: 10,
        previousSha256: shaB(),
      });
      expect(r.ok).toBe(false);
      if (r.ok) return;
      expect(r.observedSha).toBe('');
      expect(r.expectedSha).toBe(shaB());
      expect(await store.getMemoryByPath(ws, 'mems_a', 'fresh.md')).toBeNull();
    });

    it('rejects versionId collisions across stores', async () => {
      await store.insertStore({
        id: 'mems_b',
        workspaceId: ws,
        name: 'other-docs',
        description: null,
        archivedAt: null,
      });
      await store.upsertMemory({
        id: 'mem_1',
        versionId: 'memver_shared',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaA(),
        sizeBytes: 10,
      });

      await expect(
        store.upsertMemory({
          id: 'mem_2',
          versionId: 'memver_shared',
          workspaceId: ws,
          storeId: 'mems_b',
          path: 'note.md',
          sha256: shaB(),
          sizeBytes: 20,
        }),
      ).rejects.toThrow(/already exists in store mems_a/);
      expect(await store.listMemories(ws, 'mems_b')).toEqual([]);
    });
  });

  describe('listVersions', () => {
    beforeEach(async () => {
      await store.insertStore({
        id: 'mems_a',
        workspaceId: ws,
        name: 'docs',
        description: null,
        archivedAt: null,
      });
    });

    it('returns versions ordered DESC by writtenAt', async () => {
      await store.upsertMemory({
        id: 'mem_1',
        versionId: 'memver_1',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaA(),
        sizeBytes: 10,
      });
      await store.upsertMemory({
        id: 'mem_X',
        versionId: 'memver_2',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaB(),
        sizeBytes: 11,
        previousSha256: shaA(),
      });
      await store.upsertMemory({
        id: 'mem_X',
        versionId: 'memver_3',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaC(),
        sizeBytes: 12,
        previousSha256: shaB(),
      });
      const versions = await store.listVersions(ws, 'mems_a', 'mem_1');
      expect(versions.map((v) => v.id)).toEqual(['memver_3', 'memver_2', 'memver_1']);
    });
  });

  describe('markVersionRedacted', () => {
    beforeEach(async () => {
      await store.insertStore({
        id: 'mems_a',
        workspaceId: ws,
        name: 'docs',
        description: null,
        archivedAt: null,
      });
      await store.upsertMemory({
        id: 'mem_1',
        versionId: 'memver_1',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'note.md',
        sha256: shaA(),
        sizeBytes: 10,
      });
    });

    it('returns null on missing version', async () => {
      const r = await store.markVersionRedacted({
        workspaceId: ws,
        storeId: 'mems_a',
        versionId: 'memver_does_not_exist',
      });
      expect(r).toBeNull();
    });

    it('flips redactedAt on first call', async () => {
      const r = await store.markVersionRedacted({
        workspaceId: ws,
        storeId: 'mems_a',
        versionId: 'memver_1',
        redactedByUserId: 'user_redactor',
      });
      expect(r).not.toBeNull();
      expect(r?.redactedAt).toBeInstanceOf(Date);
      expect(r?.redactedByUserId).toBe('user_redactor');
    });

    it('is idempotent: second call returns the same record without re-stamping', async () => {
      const first = await store.markVersionRedacted({
        workspaceId: ws,
        storeId: 'mems_a',
        versionId: 'memver_1',
        redactedByApiKeyId: 'key_first',
      });
      expect(first?.redactedAt).toBeInstanceOf(Date);
      const firstAt = first!.redactedAt!.getTime();

      // Second call should NOT update the timestamp or original actor.
      const second = await store.markVersionRedacted({
        workspaceId: ws,
        storeId: 'mems_a',
        versionId: 'memver_1',
        redactedByApiKeyId: 'key_second',
      });
      expect(second?.redactedAt?.getTime()).toBe(firstAt);
      expect(second?.redactedByApiKeyId).toBe('key_first');
    });
  });

  describe('cross-store isolation', () => {
    it('listMemories scopes to a single store', async () => {
      await store.insertStore({
        id: 'mems_a',
        workspaceId: ws,
        name: 'a',
        description: null,
        archivedAt: null,
      });
      await store.insertStore({
        id: 'mems_b',
        workspaceId: ws,
        name: 'b',
        description: null,
        archivedAt: null,
      });
      await store.upsertMemory({
        id: 'mem_a1',
        versionId: 'memver_a1',
        workspaceId: ws,
        storeId: 'mems_a',
        path: 'x.md',
        sha256: shaA(),
        sizeBytes: 1,
      });
      await store.upsertMemory({
        id: 'mem_b1',
        versionId: 'memver_b1',
        workspaceId: ws,
        storeId: 'mems_b',
        path: 'x.md',
        sha256: shaB(),
        sizeBytes: 1,
      });
      const a = await store.listMemories(ws, 'mems_a');
      expect(a.map((m) => m.id)).toEqual(['mem_a1']);
      const b = await store.listMemories(ws, 'mems_b');
      expect(b.map((m) => m.id)).toEqual(['mem_b1']);
    });

    it('requires workspace scope for every child metadata operation', async () => {
      await store.insertStore({
        id: 'mems_owner',
        workspaceId: ws,
        name: 'owner',
        description: null,
        archivedAt: null,
      });
      const result = await store.upsertMemory({
        id: 'mem_owner',
        versionId: 'memver_owner',
        workspaceId: ws,
        storeId: 'mems_owner',
        path: 'private.md',
        sha256: shaA(),
        sizeBytes: 1,
      });
      expect(result.ok).toBe(true);

      expect(await store.listMemories('ws_other', 'mems_owner')).toEqual([]);
      expect(await store.getMemory('ws_other', 'mems_owner', 'mem_owner')).toBeNull();
      expect(await store.getVersion('ws_other', 'mems_owner', 'memver_owner')).toBeNull();
      expect(await store.deleteMemory('ws_other', 'mems_owner', 'mem_owner')).toBe(false);
      await expect(
        store.upsertMemory({
          id: 'mem_attack',
          versionId: 'memver_attack',
          workspaceId: 'ws_other',
          storeId: 'mems_owner',
          path: 'private.md',
          sha256: shaB(),
          sizeBytes: 1,
        }),
      ).rejects.toThrow(/not found in workspace ws_other/);
    });
  });
});

function metadataWithReservedKeys(protoValue: string): Record<string, string> {
  const metadata = Object.create(null) as Record<string, string>;
  metadata['__proto__'] = protoValue;
  metadata['constructor'] = 'literal-constructor';
  return metadata;
}
