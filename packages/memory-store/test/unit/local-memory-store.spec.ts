// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomBytes } from 'node:crypto';
import { Readable } from 'node:stream';
import { beforeEach, describe, expect, it } from 'vitest';
import { InMemoryMemoryBlobStore } from '../../src/blob/in-memory.js';
import { LocalMemoryStore } from '../../src/local-memory-store.js';
import { InMemoryMemoryMetadataStore } from '../../src/metadata/in-memory.js';
import { MemoryConflictError } from '../../src/store.js';

const ws = 'ws_local';

async function readAll(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as unknown as Uint8Array));
  }
  return Buffer.concat(chunks);
}

function digest(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

describe('LocalMemoryStore', () => {
  let blobs: InMemoryMemoryBlobStore;
  let meta: InMemoryMemoryMetadataStore;
  let store: LocalMemoryStore;

  beforeEach(() => {
    blobs = new InMemoryMemoryBlobStore();
    meta = new InMemoryMemoryMetadataStore();
    store = new LocalMemoryStore({ blobStore: blobs, metadataStore: meta });
  });

  describe('store CRUD', () => {
    it('createStore + listStores + getStore round-trip', async () => {
      const created = await store.createStore({
        workspaceId: ws,
        name: 'design notes',
        description: 'shared scratchpad',
        metadata: { owner: 'platform' },
      });
      expect(created.id).toMatch(/^mems_/);
      expect(created.workspaceId).toBe(ws);
      expect(created.description).toBe('shared scratchpad');
      expect(created.metadata).toEqual({ owner: 'platform' });

      const fetched = await store.getStore(ws, created.id);
      expect(fetched?.id).toBe(created.id);
      expect(fetched?.name).toBe('design notes');

      const listed = await store.listStores(ws);
      expect(listed.items.map((s) => s.id)).toContain(created.id);
    });

    it('createStore omits description gracefully', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'plain' });
      expect(created.description).toBeNull();
    });

    it('updateStore changes name and clears description', async () => {
      const created = await store.createStore({
        workspaceId: ws,
        name: 'before',
        description: 'desc',
      });
      const updated = await store.updateStore({
        workspaceId: ws,
        storeId: created.id,
        name: 'after',
        description: null,
        metadata: { owner: 'runtime' },
      });
      expect(updated).toMatchObject({
        id: created.id,
        name: 'after',
        description: null,
        metadata: { owner: 'runtime' },
      });
      expect(await store.updateStore({ workspaceId: 'ws_other', storeId: created.id })).toBeNull();
    });
  });

  describe('writeMemory', () => {
    it('rejects invalid paths before creating metadata or blobs', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'safe-paths' });
      const payload = Buffer.from('secret');

      await expect(
        store.writeMemory({
          workspaceId: ws,
          storeId: created.id,
          path: '../secret',
          content: Readable.from(payload),
          sizeBytes: payload.length,
          sha256: digest(payload),
        }),
      ).rejects.toThrow(/invalid relative path/);

      await expect(store.listMemories(ws, created.id)).resolves.toEqual([]);
      await expect(blobs.listLive(ws, created.id)).resolves.toEqual([]);
    });

    it('rejects a store owned by another workspace before writing metadata or blobs', async () => {
      const created = await store.createStore({ workspaceId: 'ws_owner', name: 'private' });
      const payload = Buffer.from('secret');

      await expect(
        store.writeMemory({
          workspaceId: 'ws_attacker',
          storeId: created.id,
          path: 'note.md',
          content: Readable.from(payload),
          sizeBytes: payload.length,
          sha256: digest(payload),
        }),
      ).rejects.toThrow(/not found in workspace ws_attacker/);

      await expect(store.listMemories('ws_owner', created.id)).resolves.toEqual([]);
      await expect(blobs.listLive('ws_attacker', created.id)).resolves.toEqual([]);
    });

    it('happy path: writes live + version blobs and the metadata rows', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const payload = Buffer.from('hello memory');
      const sha = digest(payload);
      const { memory, version } = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(payload),
        sizeBytes: payload.length,
        sha256: sha,
      });

      expect(memory.id).toMatch(/^mem_/);
      expect(memory.currentSha256).toBe(sha);
      expect(memory.sizeBytes).toBe(payload.length);
      expect(version.id).toMatch(/^memver_/);
      expect(version.sha256).toBe(sha);
      await expect(store.getVersion(ws, created.id, version.id)).resolves.toMatchObject({
        id: version.id,
        sha256: sha,
      });
      await expect(store.getVersion('ws_other', created.id, version.id)).resolves.toBeNull();

      // Live blob is reachable.
      const liveBytes = await readAll(await blobs.openLive(ws, created.id, 'note.md'));
      expect(liveBytes.equals(payload)).toBe(true);
      // Version blob is reachable.
      const verBytes = await readAll(await blobs.openVersion(ws, created.id, sha));
      expect(verBytes.equals(payload)).toBe(true);

      // openMemory returns the same bytes via the high-level API.
      const opened = await store.openMemory(ws, created.id, memory.id);
      expect(opened).not.toBeNull();
      const openedBytes = await readAll(opened!.stream);
      expect(openedBytes.equals(payload)).toBe(true);
    });

    it('rejects content larger than 100 KB', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const payload = randomBytes(100 * 1024 + 1);
      const sha = digest(payload);
      await expect(
        store.writeMemory({
          workspaceId: ws,
          storeId: created.id,
          path: 'big.bin',
          content: Readable.from(payload),
          sizeBytes: payload.length,
          sha256: sha,
        }),
      ).rejects.toThrow(/100 KB|per-memory cap/i);
    });

    it('rejects content whose sha256 does not match caller-supplied sha', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const payload = Buffer.from('hello');
      await expect(
        store.writeMemory({
          workspaceId: ws,
          storeId: created.id,
          path: 'a.md',
          content: Readable.from(payload),
          sizeBytes: payload.length,
          sha256: 'f'.repeat(64), // wrong
        }),
      ).rejects.toThrow(/sha256/);

      // No metadata row was written.
      expect(await store.getMemoryByPath(ws, created.id, 'a.md')).toBeNull();
    });

    it('rejects content whose length does not match sizeBytes', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const payload = Buffer.from('actually six chars'); // length 18
      await expect(
        store.writeMemory({
          workspaceId: ws,
          storeId: created.id,
          path: 'a.md',
          content: Readable.from(payload),
          sizeBytes: 6, // wrong
          sha256: digest(payload),
        }),
      ).rejects.toThrow(/sizeBytes/);
    });

    it('CAS happy path: matching previousSha256 succeeds', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const v1 = Buffer.from('one');
      const sha1 = digest(v1);
      const first = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(v1),
        sizeBytes: v1.length,
        sha256: sha1,
      });

      const v2 = Buffer.from('two');
      const sha2 = digest(v2);
      const second = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(v2),
        sizeBytes: v2.length,
        sha256: sha2,
        previousSha256: sha1,
      });
      // Memory id remains stable; version id differs.
      expect(second.memory.id).toBe(first.memory.id);
      expect(second.memory.currentSha256).toBe(sha2);
      expect(second.version.id).not.toBe(first.version.id);
    });

    it('CAS failure: throws MemoryConflictError with observed/expected', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const v1 = Buffer.from('one');
      const sha1 = digest(v1);
      await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(v1),
        sizeBytes: v1.length,
        sha256: sha1,
      });

      const v2 = Buffer.from('two');
      const sha2 = digest(v2);
      // Caller asserts a stale precondition.
      const stalePrecond = 'd'.repeat(64);
      let thrown: unknown;
      try {
        await store.writeMemory({
          workspaceId: ws,
          storeId: created.id,
          path: 'note.md',
          content: Readable.from(v2),
          sizeBytes: v2.length,
          sha256: sha2,
          previousSha256: stalePrecond,
        });
      } catch (err) {
        thrown = err;
      }
      expect(thrown).toBeInstanceOf(MemoryConflictError);
      const conflict = thrown as MemoryConflictError;
      expect(conflict.observedSha).toBe(sha1);
      expect(conflict.expectedSha).toBe(stalePrecond);

      // No orphan blob: the live + version blobs must reflect v1 only.
      const live = await readAll(await blobs.openLive(ws, created.id, 'note.md'));
      expect(live.equals(v1)).toBe(true);
      // The version blob for sha2 must NOT have been created.
      await expect(blobs.openVersion(ws, created.id, sha2)).rejects.toThrow();
    });

    it('first-write with previousSha set throws MemoryConflictError', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const v1 = Buffer.from('one');
      const sha1 = digest(v1);
      const stalePrecond = 'd'.repeat(64);
      let thrown: unknown;
      try {
        await store.writeMemory({
          workspaceId: ws,
          storeId: created.id,
          path: 'fresh.md',
          content: Readable.from(v1),
          sizeBytes: v1.length,
          sha256: sha1,
          previousSha256: stalePrecond,
        });
      } catch (err) {
        thrown = err;
      }
      expect(thrown).toBeInstanceOf(MemoryConflictError);
      const conflict = thrown as MemoryConflictError;
      expect(conflict.observedSha).toBe('');
      expect(conflict.expectedSha).toBe(stalePrecond);
    });

    it('deleteMemory removes live memory and keeps version metadata', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const payload = Buffer.from('delete me');
      const sha = digest(payload);
      const { memory, version } = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'gone.md',
        content: Readable.from(payload),
        sizeBytes: payload.length,
        sha256: sha,
      });

      await expect(store.deleteMemory(ws, created.id, memory.id)).resolves.toBe(true);
      await expect(store.getMemory(ws, created.id, memory.id)).resolves.toBeNull();
      await expect(store.openMemory(ws, created.id, memory.id)).resolves.toBeNull();
      const versions = await store.listVersions(ws, created.id, memory.id);
      expect(versions).toHaveLength(2);
      expect(versions[0]).toMatchObject({ memoryId: memory.id, sha256: sha });
      await expect(store.getVersion(ws, created.id, version.id)).resolves.toMatchObject({
        id: version.id,
        sha256: sha,
      });
    });

    it('allows a deleted path to be recreated with a new memory id', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'recreated-paths' });
      const firstBytes = Buffer.from('first');
      const first = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(firstBytes),
        sizeBytes: firstBytes.length,
        sha256: digest(firstBytes),
      });

      await store.deleteMemory(ws, created.id, first.memory.id);

      const secondBytes = Buffer.from('second');
      const second = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(secondBytes),
        sizeBytes: secondBytes.length,
        sha256: digest(secondBytes),
        createOnly: true,
      });

      expect(second.memory.id).not.toBe(first.memory.id);
      expect(await store.getMemoryByPath(ws, created.id, 'note.md')).toMatchObject({
        id: second.memory.id,
        currentSha256: digest(secondBytes),
      });
      expect(await store.listVersions(ws, created.id, first.memory.id)).toHaveLength(2);
    });
  });

  describe('redactVersion', () => {
    it('masks one version without deleting a shared sha-addressed blob', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const payload = Buffer.from('secret');
      const sha = digest(payload);
      const { version } = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'a.md',
        content: Readable.from(payload),
        sizeBytes: payload.length,
        sha256: sha,
      });
      // Version blob exists before redact.
      await expect(blobs.openVersion(ws, created.id, sha)).resolves.toBeDefined();

      const redacted = await store.redactVersion(ws, created.id, version.id);
      expect(redacted.redactedAt).toBeInstanceOf(Date);
      expect(redacted.id).toBe(version.id);

      await expect(store.openVersion(ws, created.id, version.id)).resolves.toBeNull();
      await expect(blobs.openVersion(ws, created.id, sha)).resolves.toBeDefined();
    });

    it('is idempotent: second call returns the same record without re-stamping', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const payload = Buffer.from('secret');
      const sha = digest(payload);
      const { version } = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'a.md',
        content: Readable.from(payload),
        sizeBytes: payload.length,
        sha256: sha,
      });
      const first = await store.redactVersion(ws, created.id, version.id);
      const firstAt = first.redactedAt!.getTime();
      const second = await store.redactVersion(ws, created.id, version.id);
      expect(second.redactedAt?.getTime()).toBe(firstAt);
    });

    it('throws when the store does not exist', async () => {
      await expect(store.redactVersion(ws, 'mems_does_not_exist', 'memver_x')).rejects.toThrow(
        /store/,
      );
    });

    it('throws when the version does not exist', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      await expect(store.redactVersion(ws, created.id, 'memver_does_not_exist')).rejects.toThrow(
        /version/,
      );
    });
  });

  describe('listMemories isolation', () => {
    it('scopes to the requested store', async () => {
      const a = await store.createStore({ workspaceId: ws, name: 'a' });
      const b = await store.createStore({ workspaceId: ws, name: 'b' });
      const payload = Buffer.from('x');
      const sha = digest(payload);
      await store.writeMemory({
        workspaceId: ws,
        storeId: a.id,
        path: 'one.md',
        content: Readable.from(payload),
        sizeBytes: payload.length,
        sha256: sha,
      });
      await store.writeMemory({
        workspaceId: ws,
        storeId: b.id,
        path: 'two.md',
        content: Readable.from(payload),
        sizeBytes: payload.length,
        sha256: sha,
      });

      const aRows = await store.listMemories(ws, a.id);
      expect(aRows.map((m) => m.path)).toEqual(['one.md']);
      const bRows = await store.listMemories(ws, b.id);
      expect(bRows.map((m) => m.path)).toEqual(['two.md']);
    });

    it('returns [] when the store does not exist', async () => {
      const rows = await store.listMemories(ws, 'mems_nope');
      expect(rows).toEqual([]);
    });

    it('returns null on getMemory for a missing store', async () => {
      const m = await store.getMemory(ws, 'mems_nope', 'mem_nope');
      expect(m).toBeNull();
    });

    it('listVersions returns DESC order', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const v1 = Buffer.from('one');
      const sha1 = digest(v1);
      const w1 = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(v1),
        sizeBytes: v1.length,
        sha256: sha1,
      });
      const v2 = Buffer.from('two');
      const sha2 = digest(v2);
      const w2 = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(v2),
        sizeBytes: v2.length,
        sha256: sha2,
        previousSha256: sha1,
      });
      const versions = await store.listVersions(ws, created.id, w1.memory.id);
      expect(versions.map((v) => v.id)).toEqual([w2.version.id, w1.version.id]);
    });

    it('deduplicates caller-supplied versionId retries without rolling back live content', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'retry-docs' });
      const bytesA = Buffer.from('one');
      const shaA = digest(bytesA);
      const versionId = 'memver_retry000000000000';
      const first = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(bytesA),
        sizeBytes: bytesA.length,
        sha256: shaA,
        versionId,
      });
      const retry = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(bytesA),
        sizeBytes: bytesA.length,
        sha256: shaA,
        versionId,
      });
      expect(retry.version.id).toBe(first.version.id);
      expect(await store.listVersions(ws, created.id, first.memory.id)).toHaveLength(1);

      const bytesB = Buffer.from('two');
      const shaB = digest(bytesB);
      const second = await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(bytesB),
        sizeBytes: bytesB.length,
        sha256: shaB,
        previousSha256: shaA,
      });
      await store.redactVersion(ws, created.id, versionId);
      await expect(store.openVersion(ws, created.id, versionId)).resolves.toBeNull();
      await expect(blobs.openVersion(ws, created.id, shaA)).resolves.toBeDefined();
      await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(bytesA),
        sizeBytes: bytesA.length,
        sha256: shaA,
        versionId,
      });

      const live = await store.openMemory(ws, created.id, second.memory.id);
      expect(live?.sha256).toBe(shaB);
      expect(live ? await readAll(live.stream) : null).toEqual(bytesB);
      await expect(store.openVersion(ws, created.id, versionId)).resolves.toBeNull();
      await expect(blobs.openVersion(ws, created.id, shaA)).resolves.toBeDefined();
      const versions = await store.listVersions(ws, created.id, first.memory.id);
      expect(versions.map((v) => v.sha256).sort()).toEqual([shaA, shaB].sort());
    });

    it('rejects caller-supplied versionId reuse for different bytes before writing blobs', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'bad-retry-docs' });
      const bytesA = Buffer.from('one');
      const shaA = digest(bytesA);
      const versionId = 'memver_retry_mismatch';
      await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'note.md',
        content: Readable.from(bytesA),
        sizeBytes: bytesA.length,
        sha256: shaA,
        versionId,
      });

      const bytesB = Buffer.from('two');
      const shaB = digest(bytesB);
      await expect(
        store.writeMemory({
          workspaceId: ws,
          storeId: created.id,
          path: 'note.md',
          content: Readable.from(bytesB),
          sizeBytes: bytesB.length,
          sha256: shaB,
          versionId,
        }),
      ).rejects.toThrow(/already belongs to a different memory write/);

      await expect(blobs.openVersion(ws, created.id, shaB)).rejects.toThrow(/missing/);
      const memory = await store.getMemoryByPath(ws, created.id, 'note.md');
      expect(memory?.currentSha256).toBe(shaA);
    });
  });

  describe('deleteStore', () => {
    it('hides the store and its children while retaining the stored bytes', async () => {
      const created = await store.createStore({ workspaceId: ws, name: 'docs' });
      const payload = Buffer.from('x');
      await store.writeMemory({
        workspaceId: ws,
        storeId: created.id,
        path: 'a.md',
        content: Readable.from(payload),
        sizeBytes: payload.length,
        sha256: digest(payload),
      });
      await store.deleteStore(ws, created.id);
      expect(await store.getStore(ws, created.id)).toBeNull();
      expect(await blobs.listLive(ws, created.id)).toEqual(['a.md']);
      expect(await store.listMemories(ws, created.id)).toEqual([]);
      expect((await store.listStores(ws, { includeArchived: true })).items).toEqual([]);
      expect(
        await store.updateStore({ workspaceId: ws, storeId: created.id, name: 'revived' }),
      ).toBeNull();
    });
  });
});
