// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import { InMemoryBlobStore, InMemoryFileMetadataStore, LocalFileStore } from '@orca/file-store';
import {
  InMemoryMemoryBlobStore,
  InMemoryMemoryMetadataStore,
  LocalMemoryStore,
} from '@orca/memory-store';
import {
  OUTPUT_RESOURCE_ID,
  RESOURCE_CHUNK_BYTES,
  resourceCheckpointDigest,
  type ResourceCheckpoint,
  type ResourceManifest,
} from '@orca/sandbox-runtime';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import {
  SessionResourcesDelivery,
  RUNNER_RESOURCES_PATH,
  RUNNER_RESOURCE_CHANGES_PATH,
  RUNNER_RESOURCE_ACK_PATH,
  type PreparedRunnerResources,
} from '../../src/tunnel/session-resources-delivery.js';

const workspaceId = 'ws_resources';
const sessionId = 'ses_resources';
const hash = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');

async function fixture() {
  const files = new LocalFileStore({
    blobStore: new InMemoryBlobStore(),
    metadataStore: new InMemoryFileMetadataStore(),
  });
  const memories = new LocalMemoryStore({
    blobStore: new InMemoryMemoryBlobStore(),
    metadataStore: new InMemoryMemoryMetadataStore(),
  });
  const memoryStore = await memories.createStore({ workspaceId, name: 'notes' });
  const events: Event[] = [];
  const store = {
    read: vi.fn(async function* () {
      yield* events;
    }),
    append: vi.fn(async (_workspace: string, _session: string, batch: Event[]) => {
      events.push(...batch);
      return [];
    }),
  } as unknown as TranscriptStore;
  const manifest: ResourceManifest = {
    version: 1,
    revision: hash('bindings'),
    resources: [
      {
        resource_id: 'sesrsc_memory',
        kind: 'memory_store',
        mount_path: '/mnt/memory',
        access: 'read_write',
        files: [],
      },
      {
        resource_id: 'sesrsc_ro',
        kind: 'memory_store',
        mount_path: '/mnt/readonly',
        access: 'read_only',
        files: [],
      },
    ],
  };
  const incoming = new Map<string, Buffer>();
  const outputBytes = new Map<string, Buffer>();
  let checkpoint: ResourceCheckpoint = {
    version: 1,
    checkpoint_id: 'rchk_test',
    revision: manifest.revision,
    files: [],
    deleted: [],
  };
  let committed = false;
  let pending = false;
  let failAck = false;
  let corrupt = false;
  const request = vi.fn(async (path: string, raw: unknown): Promise<unknown> => {
    const body = raw as Record<string, unknown>;
    if (path === RUNNER_RESOURCES_PATH) {
      if (body.type === 'commit') committed = true;
      return { committed };
    }
    if (path === RUNNER_RESOURCE_ACK_PATH) {
      if (failAck) throw new Error('ACK lost');
      pending = false;
      return { ok: true };
    }
    expect(path).toBe(RUNNER_RESOURCE_CHANGES_PATH);
    if (body.type === 'pending')
      return pending
        ? { checkpoint, manifest_sha256: resourceCheckpointDigest(checkpoint) }
        : { checkpoint: null };
    if (body.type === 'manifest')
      return { ...checkpoint, manifest_sha256: resourceCheckpointDigest(checkpoint) };
    const bytes = outputBytes.get(`${body.resource_id}:${body.path}`)!;
    const offset = body.offset as number;
    return {
      offset,
      content_base64: (corrupt
        ? Buffer.from('corrupt')
        : bytes.subarray(offset, offset + RESOURCE_CHUNK_BYTES)
      ).toString('base64'),
    };
  });
  const prepared: PreparedRunnerResources = {
    manifest,
    memoryStores: new Map([
      ['sesrsc_memory', memoryStore.id],
      ['sesrsc_ro', memoryStore.id],
    ]),
    networkAllowedDomains: [],
    close: vi.fn(async () => {}),
    open: vi.fn(async (resource, path) => Readable.from(incoming.get(`${resource}:${path}`)!)),
  };
  const current = vi.fn(async () => true);
  const prepare = vi.fn(async () => prepared);
  const create = () =>
    new SessionResourcesDelivery({
      workspaceId,
      sessionId,
      fileStore: files,
      memoryStore: memories,
      store,
      request,
      isCurrent: current,
      prepare,
    });
  function change(resource: string, path: string, content: string, previous?: string) {
    const bytes = Buffer.from(content);
    outputBytes.set(`${resource}:${path}`, bytes);
    checkpoint.files.push({
      resource_id: resource,
      path,
      sha256: hash(bytes),
      size_bytes: bytes.length,
      mode: 0o644,
      ...(previous ? { previous_sha256: previous } : {}),
    });
    return bytes;
  }
  const commit = (delivery: SessionResourcesDelivery) =>
    delivery.commit(checkpoint.checkpoint_id, resourceCheckpointDigest(checkpoint));
  const writeMemory = (path: string, content: string) =>
    memories.writeMemory({
      workspaceId,
      storeId: memoryStore.id,
      path,
      content: Readable.from(content),
      sizeBytes: Buffer.byteLength(content),
      sha256: hash(content),
    });
  return {
    files,
    memories,
    memoryStore,
    events,
    store,
    manifest,
    incoming,
    prepared,
    prepare,
    request,
    current,
    create,
    change,
    commit,
    writeMemory,
    setCheckpoint: (value: ResourceCheckpoint) => {
      checkpoint = value;
    },
    checkpoint: () => checkpoint,
    setPending: (value: boolean) => {
      pending = value;
    },
    setFailAck: (value: boolean) => {
      failAck = value;
    },
    setCorrupt: () => {
      corrupt = true;
    },
  };
}

describe('Registry resource delivery and durable checkpoint ACK', () => {
  it('refreshes capabilities on every delivery even when the live resource revision is unchanged', async () => {
    const f = await fixture();
    f.prepared.refreshGitCapabilities = vi.fn(async () => []);
    await f.create().deliver();
    await f.create().deliver();
    const refreshes = f.request.mock.calls.filter(
      ([, raw]) => (raw as { type: string }).type === 'git_capabilities',
    );
    expect(refreshes).toHaveLength(2);
    const original = f.request.getMockImplementation()!;
    f.request.mockImplementation(async (path, raw) =>
      (raw as { type: string }).type === 'git_capabilities'
        ? { committed: false }
        : original(path, raw),
    );
    await expect(f.create().deliver()).rejects.toThrow('runner rejected Git capabilities');
  });
  it('mints grants after a slow byte transfer and refreshes again after later turn preparation', async () => {
    const f = await fixture();
    let now = 0;
    const mintedAt: number[] = [];
    f.prepared.refreshGitCapabilities = async () => {
      mintedAt.push(now);
      return [];
    };
    f.manifest.resources[0]!.files.push({
      path: 'large.txt',
      sha256: hash('data'),
      size_bytes: 4,
      mode: 0o644,
    });
    f.incoming.set('sesrsc_memory:large.txt', Buffer.from('data'));
    const original = f.request.getMockImplementation()!;
    f.request.mockImplementation(async (path, raw) => {
      if ((raw as { type: string }).type === 'file_chunk') now += 1_200_000;
      return original(path, raw);
    });
    const d = f.create();
    await d.deliver();
    expect(mintedAt).toEqual([1_200_000]);
    now += 1_200_000; // Skills/snapshot/accounting work before dispatch.
    await d.refreshGitCapabilities();
    expect(mintedAt).toEqual([1_200_000, 2_400_000]);
  });

  it('pushes bounded verified source chunks and preserves a committed live runner on reconnect', async () => {
    const f = await fixture();
    const bytes = Buffer.alloc(RESOURCE_CHUNK_BYTES + 5, 'a');
    f.manifest.resources[0]!.files.push({
      path: 'large.txt',
      sha256: hash(bytes),
      size_bytes: bytes.length,
      mode: 0o444,
    });
    f.incoming.set('sesrsc_memory:large.txt', bytes);
    await f.create().deliver();
    const chunkCalls = f.request.mock.calls.filter(
      ([, body]) => (body as { type: string }).type === 'file_chunk',
    );
    expect(
      chunkCalls.map(
        ([, body]) =>
          Buffer.from((body as { content_base64: string }).content_base64, 'base64').length,
      ),
    ).toEqual([RESOURCE_CHUNK_BYTES, 5]);
    await f.create().deliver();
    expect(f.prepared.open).toHaveBeenCalledTimes(1);
    expect(f.prepared.close).toHaveBeenCalledTimes(2);
  });

  it('does not commit a corrupted source or continue after losing binding ownership', async () => {
    const f = await fixture();
    f.manifest.resources[0]!.files.push({
      path: 'note',
      sha256: hash('right'),
      size_bytes: 5,
      mode: 0o444,
    });
    f.incoming.set('sesrsc_memory:note', Buffer.from('wrong'));
    await expect(f.create().deliver()).rejects.toThrow('integrity');
    expect(
      f.request.mock.calls.some(([, body]) => (body as { type: string }).type === 'commit'),
    ).toBe(false);
    f.current.mockResolvedValue(false);
    await expect(f.create().deliver()).rejects.toThrow('ownership');
  });

  it('rebuilds Memory descriptors after acknowledging a pending checkpoint', async () => {
    const f = await fixture();
    await f.create().deliver();
    f.change('sesrsc_memory', 'note.txt', 'acknowledged');
    f.setPending(true);
    f.prepare.mockClear();
    f.prepare.mockImplementation(async () => {
      const memory = await f.memories.getMemoryByPath(workspaceId, f.memoryStore.id, 'note.txt');
      return {
        ...f.prepared,
        manifest: {
          ...f.manifest,
          resources: [
            {
              ...f.manifest.resources[0]!,
              files: memory
                ? [{ path: 'note.txt', sha256: memory.currentSha256, size_bytes: 12, mode: 0o644 }]
                : [],
            },
          ],
        },
      };
    });
    await f.create().deliver();
    expect(f.prepare).toHaveBeenCalledTimes(2);
    const manifests = f.request.mock.calls.filter(
      ([path, raw]) =>
        path === RUNNER_RESOURCES_PATH && (raw as { type: string }).type === 'manifest',
    );
    expect(manifests.at(-1)![1]).toMatchObject({
      manifest: { resources: [{ files: [{ path: 'note.txt', sha256: hash('acknowledged') }] }] },
    });
  });

  it('registers downloadable output bytes and readiness before ACK, replaying after File insert/append failure', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    f.change(OUTPUT_RESOURCE_ID, 'reports/answer.txt', 'answer');
    vi.mocked(f.store.append).mockRejectedValueOnce(new Error('append unavailable'));
    await expect(f.commit(d)).rejects.toThrow('append unavailable');
    expect((await f.files.list(workspaceId)).items).toHaveLength(1);
    expect(f.request.mock.calls.some(([path]) => path === RUNNER_RESOURCE_ACK_PATH)).toBe(false);
    f.setPending(true);
    await f.create().deliver();
    const [file] = (await f.files.list(workspaceId)).items;
    expect(file).toMatchObject({
      purpose: 'agent_output',
      downloadable: true,
      scopeId: sessionId,
      sha256: hash('answer'),
    });
    expect(f.events).toHaveLength(1);
    expect(JSON.parse(Buffer.from(f.events[0]!.payload).toString())).toMatchObject({
      file_id: file!.id,
      key: 'reports/answer.txt',
    });
    const ackIndex = f.request.mock.calls.findIndex(([path]) => path === RUNNER_RESOURCE_ACK_PATH);
    expect(ackIndex).toBeGreaterThanOrEqual(0);
    expect(
      f.request.mock.calls.slice(ackIndex + 1).some(([path]) => path === RUNNER_RESOURCES_PATH),
    ).toBe(true);
  });

  it('repairs append-success/ACK-loss without duplicating File or logical readiness', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    f.change(OUTPUT_RESOURCE_ID, 'answer.txt', 'answer');
    vi.mocked(f.store.append).mockImplementationOnce(async (_w, _s, events) => {
      f.events.push(...events);
      throw new Error('append ACK lost');
    });
    await expect(f.commit(d)).rejects.toThrow('append ACK lost');
    await f.commit(d);
    expect((await f.files.list(workspaceId)).items).toHaveLength(1);
    expect(f.events).toHaveLength(1);
  });

  it('rejects forged store targets and corrupt frozen bytes without ACK', async () => {
    for (const target of ['sesrsc_other', 'sesrsc_ro']) {
      const f = await fixture();
      const d = f.create();
      await d.deliver();
      f.change(target, 'note.txt', 'unsafe');
      await expect(f.commit(d)).rejects.toThrow('undeclared writable');
      expect(f.request.mock.calls.some(([path]) => path === RUNNER_RESOURCE_ACK_PATH)).toBe(false);
    }
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    f.change(OUTPUT_RESOURCE_ID, 'answer.txt', 'changed');
    f.setCorrupt();
    await expect(f.commit(d)).rejects.toThrow('integrity');
    expect((await f.files.list(workspaceId)).items).toHaveLength(0);
    expect(f.request.mock.calls.some(([path]) => path === RUNNER_RESOURCE_ACK_PATH)).toBe(false);
  });

  it('persists one memory conflict version across ACK-loss retries', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    await f.writeMemory('note.txt', 'concurrent');
    f.change('sesrsc_memory', 'note.txt', 'tool value', hash('old'));
    f.setFailAck(true);
    await expect(f.commit(d)).rejects.toThrow('ACK lost');
    f.setFailAck(false);
    await f.commit(d);
    const memory = await f.memories.getMemoryByPath(workspaceId, f.memoryStore.id, 'note.txt');
    expect(memory!.currentSha256).toBe(hash('tool value'));
    const conflicts = f.events.filter((event) => event.kind === 'session.memory_conflict');
    expect(conflicts).toHaveLength(1);
    expect(JSON.parse(Buffer.from(conflicts[0]!.payload).toString())).toMatchObject({
      store_id: f.memoryStore.id,
      path: 'note.txt',
      expected_sha256: hash('old'),
      observed_sha256: hash('concurrent'),
      written_by_session_id: sessionId,
    });
    expect(await f.memories.listVersions(workspaceId, f.memoryStore.id, memory!.id)).toHaveLength(
      2,
    );
  });

  it.each([true, false])(
    'reuses trusted Git descriptors only while the runner retains the revision (%s)',
    async (active) => {
      const f = await fixture();
      const d = f.create();
      if (active) {
        f.manifest.resources.push({
          resource_id: 'sesrsc_git',
          kind: 'github_repository',
          mount_path: '/workspace/repo',
          access: 'read_write',
          files: [{ path: 'README', sha256: hash('git'), size_bytes: 3, mode: 0o644 }],
        });
        f.incoming.set('sesrsc_git:README', Buffer.from('git'));
      }
      await d.deliver();
      vi.mocked(f.prepared.open).mockClear();
      const original = f.request.getMockImplementation()!;
      f.request.mockImplementation(async (path, raw) => {
        const body = raw as Record<string, unknown>;
        if (path === RUNNER_RESOURCES_PATH && body.type === 'status')
          return { committed: active, revision: active ? f.manifest.revision : undefined };
        // Force the Memory refresh staging path, even with an already live sandbox.
        if (path === RUNNER_RESOURCES_PATH && body.type === 'manifest') return { committed: false };
        return original(path, raw);
      });
      if (active) f.prepared.retainedGitResourceIds = ['sesrsc_git'];
      await d.deliver();
      expect(f.prepare).toHaveBeenLastCalledWith(active ? f.manifest : undefined);
      expect(f.prepared.open).not.toHaveBeenCalled();
      expect(f.request).toHaveBeenCalledWith(
        RUNNER_RESOURCES_PATH,
        expect.objectContaining({
          type: 'manifest',
          retained_git_resource_ids: active ? ['sesrsc_git'] : [],
        }),
      );
    },
  );

  it('refuses a stale conflict receipt after the overwrite fails and another writer advances Memory', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    await f.writeMemory('note.txt', 'concurrent');
    f.change('sesrsc_memory', 'note.txt', 'tool value', hash('old'));
    const write = f.memories.writeMemory.bind(f.memories);
    const spy = vi.spyOn(f.memories, 'writeMemory').mockImplementation(async (input) => {
      if (input.versionId?.endsWith('_conflict')) {
        (input.content as Readable).destroy();
        throw new Error('overwrite unavailable');
      }
      return write(input);
    });
    await expect(f.commit(d)).rejects.toThrow('overwrite unavailable');
    expect(f.events.filter((event) => event.kind === 'session.memory_conflict')).toHaveLength(1);
    spy.mockRestore();
    await f.writeMemory('note.txt', 'newer concurrent');
    await expect(f.commit(d)).rejects.toThrow('memory conflict event identity differs');
    expect(
      (await f.memories.getMemoryByPath(workspaceId, f.memoryStore.id, 'note.txt'))!.currentSha256,
    ).toBe(hash('newer concurrent'));
    expect(f.request.mock.calls.some(([path]) => path === RUNNER_RESOURCE_ACK_PATH)).toBe(false);
  });

  it.each([false, true])(
    'persists conflict notification before overwrite and recovers append ACK loss (%s)',
    async (lostAck) => {
      const f = await fixture();
      const d = f.create();
      await d.deliver();
      await f.writeMemory('note.txt', 'concurrent');
      f.change('sesrsc_memory', 'note.txt', 'tool value', hash('old'));
      vi.mocked(f.store.append).mockImplementationOnce(async (_ws, _session, batch) => {
        if (lostAck) f.events.push(...batch);
        throw new Error('conflict append unavailable');
      });
      await expect(f.commit(d)).rejects.toThrow('conflict append unavailable');
      expect(
        (await f.memories.getMemoryByPath(workspaceId, f.memoryStore.id, 'note.txt'))!
          .currentSha256,
      ).toBe(hash('concurrent'));
      expect(f.request.mock.calls.some(([path]) => path === RUNNER_RESOURCE_ACK_PATH)).toBe(false);
      const replacement = f.create();
      await replacement.deliver();
      await f.commit(replacement);
      expect(f.events.filter((event) => event.kind === 'session.memory_conflict')).toHaveLength(1);
      expect(
        (await f.memories.getMemoryByPath(workspaceId, f.memoryStore.id, 'note.txt'))!
          .currentSha256,
      ).toBe(hash('tool value'));
    },
  );

  it('does not delete a recreated memory when replaying an acknowledged deletion checkpoint', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    await f.writeMemory('note.txt', 'old');
    f.checkpoint().deleted.push({
      resource_id: 'sesrsc_memory',
      path: 'note.txt',
      previous_sha256: hash('old'),
    });
    f.setFailAck(true);
    await expect(f.commit(d)).rejects.toThrow('ACK lost');
    await f.writeMemory('note.txt', 'old'); // even equal bytes belong to a new memory occurrence
    f.setFailAck(false);
    await f.commit(d);
    expect(
      await f.memories.getMemoryByPath(workspaceId, f.memoryStore.id, 'note.txt'),
    ).not.toBeNull();
  });

  it('fails closed when a memory changes before its deletion or its store is archived', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    await f.writeMemory('note.txt', 'new');
    f.checkpoint().deleted.push({
      resource_id: 'sesrsc_memory',
      path: 'note.txt',
      previous_sha256: hash('old'),
    });
    await expect(f.commit(d)).rejects.toThrow('changed before');
    f.checkpoint().deleted = [];
    f.change('sesrsc_memory', 'note.txt', 'tool value');
    await f.memories.archiveStore(workspaceId, f.memoryStore.id);
    await expect(f.commit(d)).rejects.toThrow('unavailable');
    expect(f.request.mock.calls.some(([path]) => path === RUNNER_RESOURCE_ACK_PATH)).toBe(false);
  });

  it('records an absent deletion target before ACK so recreation survives retry', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    f.checkpoint().deleted.push({
      resource_id: 'sesrsc_memory',
      path: 'absent.txt',
      previous_sha256: hash('old'),
    });
    f.setFailAck(true);
    await expect(f.commit(d)).rejects.toThrow('ACK lost');
    expect(f.events[0]!.kind).toBe('harness.resource_deletions');
    await f.writeMemory('absent.txt', 'old');
    f.setFailAck(false);
    await f.commit(d);
    expect(
      await f.memories.getMemoryByPath(workspaceId, f.memoryStore.id, 'absent.txt'),
    ).not.toBeNull();
  });

  it('stops before creating a File if resource download loses ownership', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    f.change(OUTPUT_RESOURCE_ID, 'answer.txt', 'answer');
    const original = f.request.getMockImplementation()!;
    f.request.mockImplementation(async (path, body) => {
      const result = await original(path, body);
      if (path === RUNNER_RESOURCE_CHANGES_PATH && (body as { type: string }).type === 'file_chunk')
        f.current.mockResolvedValue(false);
      return result;
    });
    await expect(f.commit(d)).rejects.toThrow('ownership');
    expect((await f.files.list(workspaceId)).items).toHaveLength(0);
  });

  it('keeps recreated Memory content on write-checkpoint replay after ACK loss', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    f.change('sesrsc_memory', 'note.txt', 'old');
    f.setFailAck(true);
    await expect(f.commit(d)).rejects.toThrow('ACK lost');
    const old = await f.memories.getMemoryByPath(workspaceId, f.memoryStore.id, 'note.txt');
    await f.memories.deleteMemory(workspaceId, f.memoryStore.id, old!.id);
    const replacement = await f.writeMemory('note.txt', 'replacement');
    f.setFailAck(false);
    await f.commit(d);
    const opened = await f.memories.openMemory(
      workspaceId,
      f.memoryStore.id,
      replacement.memory.id,
    );
    let content = '';
    for await (const chunk of opened!.stream) content += String(chunk);
    expect(content).toBe('replacement');
  });

  it('does not delete a Memory renamed after the deletion intent was persisted', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    const original = await f.writeMemory('old-path.txt', 'old');
    f.checkpoint().deleted.push({
      resource_id: 'sesrsc_memory',
      path: 'old-path.txt',
      previous_sha256: hash('old'),
    });
    vi.mocked(f.store.append).mockImplementationOnce(async (_w, _s, events) => {
      f.events.push(...events);
      throw new Error('intent ACK lost');
    });
    await expect(f.commit(d)).rejects.toThrow('intent ACK lost');
    await f.memories.writeMemory({
      workspaceId,
      storeId: f.memoryStore.id,
      memoryId: original.memory.id,
      path: 'renamed.txt',
      content: Readable.from('old'),
      sizeBytes: 3,
      sha256: hash('old'),
    });
    await expect(f.commit(d)).rejects.toThrow('path changed');
    expect(
      await f.memories.getMemoryByPath(workspaceId, f.memoryStore.id, 'renamed.txt'),
    ).not.toBeNull();
  });

  it('does not append readiness when ownership changes during transcript reconciliation', async () => {
    const f = await fixture();
    const d = f.create();
    await d.deliver();
    f.change(OUTPUT_RESOURCE_ID, 'answer.txt', 'answer');
    vi.mocked(f.store.read).mockImplementationOnce(async function* () {
      f.current.mockResolvedValue(false);
      yield* [];
    });
    await expect(f.commit(d)).rejects.toThrow('ownership');
    expect(f.events).toHaveLength(0);
    expect(f.request.mock.calls.some(([path]) => path === RUNNER_RESOURCE_ACK_PATH)).toBe(false);
  });
});

describe('managed File mount events', () => {
  async function mountedFixture() {
    const f = await fixture();
    f.manifest.resources.push({
      resource_id: 'sesrsc_file',
      kind: 'file',
      mount_path: '/mnt/input.txt',
      access: 'read_only',
      files: [{ path: '', sha256: hash('input'), size_bytes: 5, mode: 0o444 }],
    });
    f.incoming.set('sesrsc_file:', Buffer.from('input'));
    f.prepared.fileIds = new Map([['sesrsc_file', 'file_input']]);
    return f;
  }

  it('publishes File mount events only after the manager confirms full preparation', async () => {
    const f = await mountedFixture();
    const delivery = f.create();
    await expect(delivery.publishMounted()).rejects.toThrow('resources were not delivered');
    await delivery.deliver();
    expect(f.events).toHaveLength(0);
    await delivery.publishMounted();
    expect(f.events).toHaveLength(1);
    expect(f.events[0]).toMatchObject({ kind: 'session.resource_mounted', producedBy: 'harness' });
    expect(JSON.parse(Buffer.from(f.events[0]!.payload).toString())).toEqual({
      type: 'session.resource_mounted',
      resource_id: 'sesrsc_file',
      file_id: 'file_input',
      mount_path: '/mnt/input.txt',
      mount_strategy: 'tarball_prefetch',
    });
    await delivery.deliver();
    await delivery.publishMounted();
    const reconnected = f.create();
    await reconnected.deliver();
    await reconnected.publishMounted();
    expect(f.events).toHaveLength(1);
  });

  it('reconciles a lost mount-event append ACK without a duplicate', async () => {
    const f = await mountedFixture();
    const delivery = f.create();
    await delivery.deliver();
    vi.mocked(f.store.append).mockImplementationOnce(async (_ws, _ses, events) => {
      f.events.push(...events);
      throw new Error('append ACK lost');
    });
    await expect(delivery.publishMounted()).rejects.toThrow('append ACK lost');
    await delivery.publishMounted();
    expect(f.events).toHaveLength(1);
  });

  it('does not publish for a retired generation or failed resource delivery', async () => {
    const f = await mountedFixture();
    const delivery = f.create();
    await delivery.deliver();
    f.current.mockResolvedValue(false);
    await expect(delivery.publishMounted()).rejects.toThrow();
    expect(f.events).toHaveLength(0);
    await expect(delivery.deliver()).rejects.toThrow();
    f.current.mockResolvedValue(true);
    await expect(delivery.publishMounted()).rejects.toThrow('resources were not delivered');
  });
});
