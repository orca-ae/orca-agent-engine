// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Unit tests for MemoryVersionWatcher.
 *
 * Two scan paths under test:
 *   - S3 path via `aws-sdk-client-mock` — mirrors the FUSE-mount runtime.
 *   - InMemory path via a `Map`-backed `SandboxHandle` test double — same
 *     pattern the output-indexer tests use.
 *
 * The registry client + TranscriptStore are stubbed inline so we can assert
 * exact `recordSessionMemoryVersion` payloads and any emitted
 * `session.memory_conflict` events.
 */

import { Readable } from 'node:stream';
import { createHash } from 'node:crypto';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { GetObjectCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { mockClient } from 'aws-sdk-client-mock';
import type { MemoryRecord, MemoryVersionRecord } from '@orca/memory-store';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import type { RegistryClient } from '../../src/clients/registry.js';
import { registry as metricsRegistry } from '../../src/metrics.js';
import {
  MemoryVersionWatcher,
  type WatchedStore,
} from '../../src/sandbox/memory/version-watcher.js';
import type {
  SandboxFiles,
  SandboxHandle,
  ToolCall,
  ToolResult,
} from '../../src/sandbox/sandbox-runtime.js';

const s3Mock = mockClient(S3Client);

interface RecordedRegister {
  workspaceId: string;
  storeId: string;
  path: string;
  contentSha256: string;
  previousSha256: string | null;
  writtenBySessionId: string;
  contentLength: number;
}

interface FakeRegistryOpts {
  conflictPaths?: Set<string>;
  failOnPath?: string;
  /** Memories returned by listMemories — keyed by storeId. */
  seed?: Map<string, MemoryRecord[]>;
}

interface FakeRegistryClientResult {
  client: RegistryClient;
  registerCalls: RecordedRegister[];
  listCalls: Array<{ workspaceId: string; sessionId: string; storeId: string }>;
}

/** Build a registry double that records what the watcher invokes. */
function fakeRegistryClient(opts: FakeRegistryOpts = {}): FakeRegistryClientResult {
  const registerCalls: RecordedRegister[] = [];
  const listCalls: Array<{ workspaceId: string; sessionId: string; storeId: string }> = [];

  const client = {
    async listSessionMemories(input: {
      workspaceId: string;
      sessionId: string;
      storeId: string;
    }): Promise<MemoryRecord[]> {
      listCalls.push(input);
      return opts.seed?.get(input.storeId) ?? [];
    },
    async recordSessionMemoryVersion(input: {
      workspaceId: string;
      sessionId: string;
      storeId: string;
      path: string;
      contentBase64: string;
      contentSha256: string;
      previousSha256: string | null;
    }): Promise<{ memory: MemoryRecord; version: MemoryVersionRecord; conflict: boolean }> {
      const decoded = Buffer.from(input.contentBase64, 'base64');
      const call: RecordedRegister = {
        workspaceId: input.workspaceId,
        storeId: input.storeId,
        path: input.path,
        contentSha256: input.contentSha256,
        previousSha256: input.previousSha256,
        writtenBySessionId: input.sessionId,
        contentLength: decoded.length,
      };
      registerCalls.push(call);
      if (opts.failOnPath && input.path === opts.failOnPath) {
        throw new Error(`forced failure for ${input.path}`);
      }
      const conflict = opts.conflictPaths?.has(input.path) ?? false;
      const now = new Date();
      return {
        memory: {
          id: `mem_${Math.random().toString(36).slice(2, 10)}`,
          storeId: input.storeId,
          path: input.path,
          currentSha256: input.contentSha256,
          sizeBytes: decoded.length,
          updatedAt: now,
          updatedBySessionId: input.sessionId,
          updatedByEventId: null,
        },
        version: {
          id: `memver_${Math.random().toString(36).slice(2, 10)}`,
          storeId: input.storeId,
          memoryId: `mem_${Math.random().toString(36).slice(2, 10)}`,
          path: input.path,
          sha256: input.contentSha256,
          sizeBytes: decoded.length,
          writtenBySessionId: input.sessionId,
          writtenByEventId: null,
          writtenAt: now,
          redactedAt: null,
        },
        conflict,
      };
    },
  } as unknown as RegistryClient;

  return { client, registerCalls, listCalls };
}

function fakeStore(): { store: TranscriptStore; appended: Event[] } {
  const appended: Event[] = [];
  const store: TranscriptStore = {
    async append(_ws: string, _ses: string, events: Event[]): Promise<string[]> {
      for (const e of events) appended.push(e);
      return events.map((e) => e.id);
    },
    async *read() {
      /* nothing */
    },
    async *tail() {
      /* nothing */
    },
    async archive() {
      /* no-op */
    },
    async close() {
      /* no-op */
    },
  };
  return { store, appended };
}

/**
 * Map-backed `SandboxHandle` test double matching the output-indexer pattern.
 * Keys are full paths; values: Buffer = file, null = directory.
 */
function fakeInMemorySandbox(tree: Map<string, Buffer | null>): SandboxHandle {
  const files: SandboxFiles = {
    async write() {
      throw new Error('not used');
    },
    async read(path: string): Promise<Buffer> {
      const v = tree.get(path);
      if (v === null) {
        const err = new Error(`EISDIR: illegal operation on a directory, read '${path}'`);
        (err as { code?: string }).code = 'EISDIR';
        throw err;
      }
      if (v === undefined) {
        const err = new Error(`ENOENT: ${path}`);
        (err as { code?: string }).code = 'ENOENT';
        throw err;
      }
      return v;
    },
    async readUtf8Page() {
      throw new Error('not used');
    },
    async list(path: string): Promise<string[]> {
      const v = tree.get(path);
      if (v === undefined) {
        const err = new Error(`ENOENT: ${path}`);
        (err as { code?: string }).code = 'ENOENT';
        throw err;
      }
      if (v !== null) throw new Error(`ENOTDIR: ${path}`);
      const prefix = `${path}/`;
      const direct = new Set<string>();
      for (const key of tree.keys()) {
        if (key === path || !key.startsWith(prefix)) continue;
        const rest = key.slice(prefix.length);
        const slashIdx = rest.indexOf('/');
        direct.add(slashIdx === -1 ? rest : rest.slice(0, slashIdx));
      }
      return Array.from(direct);
    },
    async chmod() {
      /* no-op */
    },
    async delete() {
      /* no-op */
    },
  };
  return {
    id: 'sbx_inmem_fake',
    files,
    async run(call: ToolCall): Promise<ToolResult> {
      void call;
      return { exit_code: 0 };
    },
    async runPrivileged(cmd: string): Promise<ToolResult> {
      void cmd;
      throw new Error('not used');
    },
    async pause(): Promise<void> {
      /* no-op */
    },
    async resume(): Promise<void> {
      /* no-op */
    },
    async destroy(): Promise<void> {
      /* no-op */
    },
  };
}

function s3Body(buf: Buffer): NodeJS.ReadableStream {
  return Readable.from(buf);
}

function sha256Hex(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

function watchedStore(overrides: Partial<WatchedStore> = {}): WatchedStore {
  return {
    storeId: overrides.storeId ?? 'mems_test',
    storeName: overrides.storeName ?? 'notes',
    mountPath: overrides.mountPath ?? '/mnt/memory/notes',
  };
}

/**
 * Read a single counter label-cell out of the harness metrics registry.
 * Returns 0 when no matching cell exists yet — `prom-client` doesn't
 * materialize a counter cell until the first `.inc()` for that label combo.
 * Mirrors the helper in `metrics-phase5.1.spec.ts`.
 */
async function readCounterCell(opts: {
  name: string;
  labels: Record<string, string>;
}): Promise<number> {
  const json = await metricsRegistry.getMetricsAsJSON();
  const metric = json.find((m) => m.name === opts.name);
  if (!metric) return 0;
  const values = (metric as { values: Array<{ labels: Record<string, string>; value: number }> })
    .values;
  const cell = values.find((v) =>
    Object.entries(opts.labels).every(([k, val]) => v.labels[k] === val),
  );
  return cell?.value ?? 0;
}

function memoryRecord(path: string, sha: string, storeId: string): MemoryRecord {
  return {
    id: `mem_${path}`,
    storeId,
    path,
    currentSha256: sha,
    sizeBytes: 0,
    updatedAt: new Date(),
    updatedBySessionId: null,
    updatedByEventId: null,
  };
}

describe('MemoryVersionWatcher', () => {
  beforeEach(() => {
    s3Mock.reset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('cache seeding (S3 path)', () => {
    it('seeds the cache from listMemories so a same-bytes tick is a no-op', async () => {
      const aBytes = Buffer.from('hello A');
      const bBytes = Buffer.from('hello B!');
      const aSha = sha256Hex(aBytes);
      const bSha = sha256Hex(bBytes);

      const store = watchedStore({ storeId: 'mems_seed' });
      const seed = new Map<string, MemoryRecord[]>([
        [
          'mems_seed',
          [memoryRecord('a.txt', aSha, 'mems_seed'), memoryRecord('b.txt', bSha, 'mems_seed')],
        ],
      ]);

      const { client, registerCalls, listCalls } = fakeRegistryClient({ seed });
      const { store: ts } = fakeStore();

      const prefix = 'memory/workspaces/ws_x/memory-stores/mems_seed/live/';
      s3Mock.on(ListObjectsV2Command, { Bucket: 'orca-mem', Prefix: prefix }).resolves({
        Contents: [
          { Key: `${prefix}a.txt`, Size: aBytes.length },
          { Key: `${prefix}b.txt`, Size: bBytes.length },
        ],
        IsTruncated: false,
      });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}a.txt` })
        .resolves({ Body: s3Body(aBytes) as never });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}b.txt` })
        .resolves({ Body: s3Body(bBytes) as never });

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        s3: new S3Client({}),
        bucket: 'orca-mem',
        memoryKeyPrefix: 'memory/',
        intervalMs: 0, // disable timer; drive manually
      });

      await watcher.start();
      expect(listCalls).toHaveLength(1);
      expect(listCalls[0]).toEqual({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        storeId: 'mems_seed',
      });

      const result = await watcher.tick();
      expect(result.storesScanned).toBe(1);
      expect(result.versionsRecorded).toBe(0);
      expect(result.errors).toBe(0);
      expect(registerCalls).toHaveLength(0);
    });
  });

  describe('S3 path', () => {
    it('detects a first write and registers it with previousSha256=null', async () => {
      const newBytes = Buffer.from('new content');
      const newSha = sha256Hex(newBytes);
      const store = watchedStore({ storeId: 'mems_first' });

      const { client, registerCalls } = fakeRegistryClient();
      const { store: ts } = fakeStore();

      const prefix = 'memory/workspaces/ws_x/memory-stores/mems_first/live/';
      s3Mock.on(ListObjectsV2Command, { Bucket: 'orca-mem', Prefix: prefix }).resolves({
        Contents: [{ Key: `${prefix}foo.txt`, Size: newBytes.length }],
        IsTruncated: false,
      });
      // Use callsFake so the Body stream is recreated on every GetObject call
      // — `aws-sdk-client-mock`'s resolves() binds the resolved value once,
      // but a Readable can only be consumed once, so multi-tick tests need a
      // fresh stream per invocation.
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}foo.txt` })
        .callsFake(() => ({ Body: s3Body(newBytes) as never }));

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        s3: new S3Client({}),
        bucket: 'orca-mem',
        memoryKeyPrefix: 'memory/',
        intervalMs: 0,
      });

      // Snapshot the OK-counter cell before the tick — other tests in this
      // run may have already incremented it for the same workspace label.
      const okBefore = await readCounterCell({
        name: 'harness_memory_versions_recorded_total',
        labels: { workspace_id: 'ws_x', result: 'ok' },
      });

      await watcher.start();
      const result = await watcher.tick();

      expect(result.versionsRecorded).toBe(1);
      expect(result.errors).toBe(0);
      expect(registerCalls).toHaveLength(1);
      const call = registerCalls[0]!;
      expect(call.storeId).toBe('mems_first');
      expect(call.path).toBe('foo.txt');
      expect(call.previousSha256).toBeNull();
      expect(call.contentSha256).toBe(newSha);
      expect(call.contentLength).toBe(newBytes.length);
      expect(call.workspaceId).toBe('ws_x');
      expect(call.writtenBySessionId).toBe('ses_y');

      // A successful registration must bump the OK
      // versions-recorded counter by exactly one.
      const okAfter = await readCounterCell({
        name: 'harness_memory_versions_recorded_total',
        labels: { workspace_id: 'ws_x', result: 'ok' },
      });
      expect(okAfter - okBefore).toBe(1);

      // Cache should now be hot — second tick is a no-op for the same bytes.
      const result2 = await watcher.tick();
      expect(result2.versionsRecorded).toBe(0);
      expect(registerCalls).toHaveLength(1);
    });

    it('detects an update and registers with previousSha256=A', async () => {
      const aBytes = Buffer.from('version A');
      const bBytes = Buffer.from('version B');
      const aSha = sha256Hex(aBytes);
      const bSha = sha256Hex(bBytes);

      const store = watchedStore({ storeId: 'mems_upd' });
      const seed = new Map<string, MemoryRecord[]>([
        ['mems_upd', [memoryRecord('foo.txt', aSha, 'mems_upd')]],
      ]);

      const { client, registerCalls } = fakeRegistryClient({ seed });
      const { store: ts } = fakeStore();

      const prefix = 'memory/workspaces/ws_x/memory-stores/mems_upd/live/';
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [{ Key: `${prefix}foo.txt`, Size: bBytes.length }],
        IsTruncated: false,
      });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}foo.txt` })
        .resolves({ Body: s3Body(bBytes) as never });

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        s3: new S3Client({}),
        bucket: 'orca-mem',
        memoryKeyPrefix: 'memory/',
        intervalMs: 0,
      });

      await watcher.start();
      const result = await watcher.tick();

      expect(result.versionsRecorded).toBe(1);
      expect(registerCalls).toHaveLength(1);
      const call = registerCalls[0]!;
      expect(call.path).toBe('foo.txt');
      expect(call.previousSha256).toBe(aSha);
      expect(call.contentSha256).toBe(bSha);
    });

    it('registers all changed paths in a single tick', async () => {
      const aBytes = Buffer.from('alpha');
      const bBytes = Buffer.from('beta-bytes');
      const cBytes = Buffer.from('gamma!!!');

      const store = watchedStore({ storeId: 'mems_multi' });
      const { client, registerCalls } = fakeRegistryClient();
      const { store: ts } = fakeStore();

      const prefix = 'memory/workspaces/ws_x/memory-stores/mems_multi/live/';
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [
          { Key: `${prefix}a.txt`, Size: aBytes.length },
          { Key: `${prefix}b.txt`, Size: bBytes.length },
          { Key: `${prefix}sub/c.txt`, Size: cBytes.length },
        ],
        IsTruncated: false,
      });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}a.txt` })
        .resolves({ Body: s3Body(aBytes) as never });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}b.txt` })
        .resolves({ Body: s3Body(bBytes) as never });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}sub/c.txt` })
        .resolves({ Body: s3Body(cBytes) as never });

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        s3: new S3Client({}),
        bucket: 'orca-mem',
        memoryKeyPrefix: 'memory/',
        intervalMs: 0,
      });

      await watcher.start();
      const result = await watcher.tick();
      expect(result.versionsRecorded).toBe(3);
      expect(registerCalls).toHaveLength(3);
      const paths = registerCalls.map((c) => c.path).sort();
      expect(paths).toEqual(['a.txt', 'b.txt', 'sub/c.txt']);
    });

    it('emits a session.memory_conflict event when the registry returns conflict=true', async () => {
      const newBytes = Buffer.from('conflicted!');
      const newSha = sha256Hex(newBytes);

      const store = watchedStore({ storeId: 'mems_conf' });
      const { client, registerCalls } = fakeRegistryClient({
        conflictPaths: new Set(['shared.txt']),
      });
      const { store: ts, appended } = fakeStore();

      const prefix = 'memory/workspaces/ws_x/memory-stores/mems_conf/live/';
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [{ Key: `${prefix}shared.txt`, Size: newBytes.length }],
        IsTruncated: false,
      });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}shared.txt` })
        .resolves({ Body: s3Body(newBytes) as never });

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        s3: new S3Client({}),
        bucket: 'orca-mem',
        memoryKeyPrefix: 'memory/',
        intervalMs: 0,
      });

      await watcher.start();
      const result = await watcher.tick();

      expect(result.versionsRecorded).toBe(1);
      expect(result.conflicts).toBe(1);
      expect(registerCalls).toHaveLength(1);
      expect(appended).toHaveLength(1);
      const ev = appended[0]!;
      expect(ev.kind).toBe('session.memory_conflict');
      expect(ev.producedBy).toBe('harness');
      expect(ev.workspaceId).toBe('ws_x');
      expect(ev.sessionId).toBe('ses_y');
      const payload = JSON.parse(Buffer.from(ev.payload).toString('utf8')) as {
        type: string;
        store_id: string;
        path: string;
        observed_sha256: string;
        expected_sha256: string | null;
        written_by_session_id: string;
      };
      expect(payload.type).toBe('session.memory_conflict');
      expect(payload.store_id).toBe('mems_conf');
      expect(payload.path).toBe('shared.txt');
      expect(payload.observed_sha256).toBe(newSha);
      expect(payload.expected_sha256).toBeNull();
      expect(payload.written_by_session_id).toBe('ses_y');
    });

    it('logs and continues when one path fails; other paths still register', async () => {
      const aBytes = Buffer.from('alpha');
      const bBytes = Buffer.from('beta-bytes');
      const cBytes = Buffer.from('gamma');

      const store = watchedStore({ storeId: 'mems_err' });
      const { client, registerCalls } = fakeRegistryClient({ failOnPath: 'b.txt' });
      const { store: ts } = fakeStore();

      const prefix = 'memory/workspaces/ws_x/memory-stores/mems_err/live/';
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [
          { Key: `${prefix}a.txt`, Size: aBytes.length },
          { Key: `${prefix}b.txt`, Size: bBytes.length },
          { Key: `${prefix}c.txt`, Size: cBytes.length },
        ],
        IsTruncated: false,
      });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}a.txt` })
        .resolves({ Body: s3Body(aBytes) as never });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}b.txt` })
        .resolves({ Body: s3Body(bBytes) as never });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}c.txt` })
        .resolves({ Body: s3Body(cBytes) as never });

      const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        s3: new S3Client({}),
        bucket: 'orca-mem',
        memoryKeyPrefix: 'memory/',
        intervalMs: 0,
      });

      await watcher.start();
      const result = await watcher.tick();

      expect(result.versionsRecorded).toBe(2); // a.txt + c.txt succeeded
      expect(result.errors).toBe(1);
      expect(registerCalls).toHaveLength(3); // all three attempted
      const successfulPaths = registerCalls
        .filter((c) => c.path !== 'b.txt')
        .map((c) => c.path)
        .sort();
      expect(successfulPaths).toEqual(['a.txt', 'c.txt']);
      expect(errSpy).toHaveBeenCalled();
    });

    it('scans only the canonical live prefix', async () => {
      const liveBytes = Buffer.from('live data');

      const store = watchedStore({ storeId: 'mems_ver' });
      const { client, registerCalls } = fakeRegistryClient();
      const { store: ts } = fakeStore();

      const prefix = 'memory/workspaces/ws_x/memory-stores/mems_ver/live/';
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [{ Key: `${prefix}live.txt`, Size: liveBytes.length }],
        IsTruncated: false,
      });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}live.txt` })
        .resolves({ Body: s3Body(liveBytes) as never });

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        s3: new S3Client({}),
        bucket: 'orca-mem',
        memoryKeyPrefix: 'memory/',
        intervalMs: 0,
      });

      await watcher.start();
      const result = await watcher.tick();

      expect(result.versionsRecorded).toBe(1);
      expect(result.errors).toBe(0);
      expect(registerCalls).toHaveLength(1);
      expect(registerCalls[0]!.path).toBe('live.txt');
      expect(s3Mock.commandCalls(ListObjectsV2Command)[0]!.args[0].input).toMatchObject({
        Bucket: 'orca-mem',
        Prefix: prefix,
      });
    });
  });

  describe('lifecycle', () => {
    it('stop() cancels the timer and waits for in-flight tick to drain', async () => {
      const store = watchedStore({ storeId: 'mems_life' });
      const { client } = fakeRegistryClient();
      const { store: ts } = fakeStore();

      // S3 ListObjects returns nothing — keep ticks fast.
      s3Mock.on(ListObjectsV2Command).resolves({ Contents: [], IsTruncated: false });

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        s3: new S3Client({}),
        bucket: 'orca-mem',
        memoryKeyPrefix: 'memory/',
        intervalMs: 50, // active timer
      });

      await watcher.start();
      // Let the interval fire at least once.
      await new Promise((r) => setTimeout(r, 120));

      await watcher.stop();
      // After stop returns, no inflight + no timer → another wait shouldn't
      // see new list calls.
      const before = s3Mock.commandCalls(ListObjectsV2Command).length;
      await new Promise((r) => setTimeout(r, 120));
      const after = s3Mock.commandCalls(ListObjectsV2Command).length;
      expect(after).toBe(before);
    });

    it('stop() returns when an in-flight local scan is hung', async () => {
      const store = watchedStore({ storeId: 'mems_hung', mountPath: '/mnt/memory/hung' });
      const { client } = fakeRegistryClient();
      const { store: ts } = fakeStore();
      let listStarted: (() => void) | null = null;
      const listStartedPromise = new Promise<void>((resolve) => {
        listStarted = resolve;
      });
      const sandbox = {
        ...fakeInMemorySandbox(new Map([['/mnt/memory/hung', null]])),
        files: {
          ...fakeInMemorySandbox(new Map([['/mnt/memory/hung', null]])).files,
          async list(): Promise<string[]> {
            listStarted?.();
            return await new Promise<string[]>(() => {});
          },
        },
      };

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        sandbox,
        intervalMs: 0,
        stopTimeoutMs: 50,
      });

      await watcher.start();
      void watcher.tick();
      await listStartedPromise;

      const stopResult = await Promise.race([
        watcher.stop().then(() => 'stopped'),
        new Promise<'timeout'>((resolve) => setTimeout(() => resolve('timeout'), 150)),
      ]);

      expect(stopResult).toBe('stopped');
    });

    it('lets an in-flight memory version registration finish during stop', async () => {
      const bytes = Buffer.from('registration during stop');
      const tree = new Map<string, Buffer | null>([
        ['/mnt/memory/drain-register', null],
        ['/mnt/memory/drain-register/live.txt', bytes],
      ]);
      let recordStarted: (() => void) | null = null;
      let finishRecord: (() => void) | null = null;
      const recordStartedPromise = new Promise<void>((resolve) => {
        recordStarted = resolve;
      });
      const finishRecordPromise = new Promise<void>((resolve) => {
        finishRecord = resolve;
      });
      const client = {
        async listSessionMemories(): Promise<MemoryRecord[]> {
          return [];
        },
        async recordSessionMemoryVersion(): Promise<{
          memory: MemoryRecord;
          version: MemoryVersionRecord;
          conflict: boolean;
        }> {
          recordStarted?.();
          await finishRecordPromise;
          const now = new Date();
          const sha = sha256Hex(bytes);
          return {
            memory: {
              id: 'mem_drain',
              storeId: 'mems_drain',
              path: 'live.txt',
              currentSha256: sha,
              sizeBytes: bytes.length,
              updatedAt: now,
              updatedBySessionId: 'ses_y',
              updatedByEventId: null,
            },
            version: {
              id: 'memver_drain',
              storeId: 'mems_drain',
              memoryId: 'mem_drain',
              path: 'live.txt',
              sha256: sha,
              sizeBytes: bytes.length,
              writtenBySessionId: 'ses_y',
              writtenByEventId: null,
              writtenAt: now,
              redactedAt: null,
            },
            conflict: false,
          };
        },
      } as unknown as RegistryClient;
      const { store: ts } = fakeStore();

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [watchedStore({ storeId: 'mems_drain', mountPath: '/mnt/memory/drain-register' })],
        registry: client,
        store: ts,
        sandbox: fakeInMemorySandbox(tree),
        intervalMs: 0,
        operationTimeoutMs: 10,
        stopTimeoutMs: 200,
      });

      await watcher.start();
      const tickPromise = watcher.tick();
      await recordStartedPromise;
      const stopPromise = watcher.stop();
      await new Promise((r) => setTimeout(r, 30));
      finishRecord?.();
      const result = await tickPromise;
      await stopPromise;

      expect(result.versionsRecorded).toBe(1);
      expect(result.errors).toBe(0);
    });

    it('bounds active memory version registrations and reconciles before retry', async () => {
      const bytes = Buffer.from('active registration timeout');
      const sha = sha256Hex(bytes);
      const tree = new Map<string, Buffer | null>([
        ['/mnt/memory/active-timeout', null],
        ['/mnt/memory/active-timeout/live.txt', bytes],
      ]);
      const now = new Date();
      const committedMemory: MemoryRecord = {
        id: 'mem_active_timeout',
        storeId: 'mems_active_timeout',
        path: 'live.txt',
        currentSha256: sha,
        sizeBytes: bytes.length,
        updatedAt: now,
        updatedBySessionId: 'ses_y',
        updatedByEventId: null,
      };
      let memories: MemoryRecord[] = [];
      let attemptedVersionId: string | undefined;
      let attempts = 0;
      let sawAbort = false;
      const client = {
        async listSessionMemories(): Promise<MemoryRecord[]> {
          return memories;
        },
        async listSessionMemoryVersions(): Promise<MemoryVersionRecord[]> {
          if (!attemptedVersionId) return [];
          return [
            {
              id: `${attemptedVersionId}_conflict`,
              storeId: 'mems_active_timeout',
              memoryId: committedMemory.id,
              path: 'live.txt',
              sha256: sha,
              sizeBytes: bytes.length,
              writtenBySessionId: 'ses_y',
              writtenByEventId: null,
              writtenAt: now,
              redactedAt: null,
            },
          ];
        },
        async recordSessionMemoryVersion(input: {
          versionId?: string;
          signal?: AbortSignal;
        }): Promise<never> {
          attempts += 1;
          attemptedVersionId = input.versionId;
          return await new Promise<never>((_resolve, reject) => {
            input.signal?.addEventListener(
              'abort',
              () => {
                sawAbort = true;
                reject(new Error('registration aborted'));
              },
              { once: true },
            );
          });
        },
      } as unknown as RegistryClient;
      const { store: ts } = fakeStore();

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [
          watchedStore({
            storeId: 'mems_active_timeout',
            mountPath: '/mnt/memory/active-timeout',
          }),
        ],
        registry: client,
        store: ts,
        sandbox: fakeInMemorySandbox(tree),
        intervalMs: 0,
        operationTimeoutMs: 10,
      });

      await watcher.start();
      const first = await watcher.tick();
      memories = [committedMemory];
      const second = await watcher.tick();

      expect(first.versionsRecorded).toBe(0);
      expect(first.errors).toBe(1);
      expect(second.versionsRecorded).toBe(0);
      expect(second.errors).toBe(0);
      expect(attempts).toBe(1);
      expect(sawAbort).toBe(true);
    });

    it('supersedes an abandoned pending registration when live bytes change', async () => {
      const bytesA = Buffer.from('pending A');
      const bytesB = Buffer.from('pending B');
      const shaA = sha256Hex(bytesA);
      const shaB = sha256Hex(bytesB);
      const tree = new Map<string, Buffer | null>([
        ['/mnt/memory/pending-order', null],
        ['/mnt/memory/pending-order/live.txt', bytesA],
      ]);
      const now = new Date();
      const attempts: Array<{ sha: string; versionId: string | undefined }> = [];
      const client = {
        async listSessionMemories(): Promise<MemoryRecord[]> {
          return [];
        },
        async listSessionMemoryVersions(): Promise<MemoryVersionRecord[]> {
          return [];
        },
        async recordSessionMemoryVersion(input: {
          contentSha256: string;
          versionId?: string;
          signal?: AbortSignal;
        }): Promise<{
          memory: MemoryRecord;
          version: MemoryVersionRecord;
          conflict: boolean;
        }> {
          attempts.push({ sha: input.contentSha256, versionId: input.versionId });
          if (attempts.length === 1) {
            return await new Promise<never>((_resolve, reject) => {
              input.signal?.addEventListener('abort', () => reject(new Error('aborted')), {
                once: true,
              });
            });
          }
          return {
            memory: {
              id: 'mem_pending_order',
              storeId: 'mems_pending_order',
              path: 'live.txt',
              currentSha256: input.contentSha256,
              sizeBytes: input.contentSha256 === shaA ? bytesA.length : bytesB.length,
              updatedAt: now,
              updatedBySessionId: 'ses_y',
              updatedByEventId: null,
            },
            version: {
              id: input.versionId ?? 'memver_fallback',
              storeId: 'mems_pending_order',
              memoryId: 'mem_pending_order',
              path: 'live.txt',
              sha256: input.contentSha256,
              sizeBytes: input.contentSha256 === shaA ? bytesA.length : bytesB.length,
              writtenBySessionId: 'ses_y',
              writtenByEventId: null,
              writtenAt: now,
              redactedAt: null,
            },
            conflict: false,
          };
        },
      } as unknown as RegistryClient;
      const { store: ts } = fakeStore();

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [
          watchedStore({
            storeId: 'mems_pending_order',
            mountPath: '/mnt/memory/pending-order',
          }),
        ],
        registry: client,
        store: ts,
        sandbox: fakeInMemorySandbox(tree),
        intervalMs: 0,
        operationTimeoutMs: 10,
      });

      await watcher.start();
      const first = await watcher.tick();
      tree.set('/mnt/memory/pending-order/live.txt', bytesB);
      await new Promise((r) => setTimeout(r, 15));
      const second = await watcher.tick();

      expect(first.errors).toBe(1);
      expect(second.errors).toBe(0);
      expect(second.versionsRecorded).toBe(1);
      expect(attempts.map((a) => a.sha)).toEqual([shaA, shaB]);
      expect(attempts[1]!.versionId).not.toBe(attempts[0]!.versionId);
    });

    it('flushes a pending registration before stop returns', async () => {
      const bytes = Buffer.from('pending before stop');
      const tree = new Map<string, Buffer | null>([
        ['/mnt/memory/stop-flush', null],
        ['/mnt/memory/stop-flush/live.txt', bytes],
      ]);
      const now = new Date();
      let attempts = 0;
      const client = {
        async listSessionMemories(): Promise<MemoryRecord[]> {
          return [];
        },
        async recordSessionMemoryVersion(input: {
          contentSha256: string;
          versionId?: string;
          signal?: AbortSignal;
        }): Promise<{
          memory: MemoryRecord;
          version: MemoryVersionRecord;
          conflict: boolean;
        }> {
          attempts += 1;
          if (attempts === 1) {
            return await new Promise<never>((_resolve, reject) => {
              input.signal?.addEventListener('abort', () => reject(new Error('aborted')), {
                once: true,
              });
            });
          }
          return {
            memory: {
              id: 'mem_stop_flush',
              storeId: 'mems_stop_flush',
              path: 'live.txt',
              currentSha256: input.contentSha256,
              sizeBytes: bytes.length,
              updatedAt: now,
              updatedBySessionId: 'ses_y',
              updatedByEventId: null,
            },
            version: {
              id: input.versionId ?? 'memver_stop_flush',
              storeId: 'mems_stop_flush',
              memoryId: 'mem_stop_flush',
              path: 'live.txt',
              sha256: input.contentSha256,
              sizeBytes: bytes.length,
              writtenBySessionId: 'ses_y',
              writtenByEventId: null,
              writtenAt: now,
              redactedAt: null,
            },
            conflict: false,
          };
        },
      } as unknown as RegistryClient;
      const { store: ts } = fakeStore();

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [watchedStore({ storeId: 'mems_stop_flush', mountPath: '/mnt/memory/stop-flush' })],
        registry: client,
        store: ts,
        sandbox: fakeInMemorySandbox(tree),
        intervalMs: 0,
        operationTimeoutMs: 10,
        stopTimeoutMs: 200,
      });

      await watcher.start();
      const first = await watcher.tick();
      await watcher.stop();

      expect(first.errors).toBe(1);
      expect(first.versionsRecorded).toBe(0);
      expect(attempts).toBe(2);
    });

    it('aborts a hung memory version registration after the stop timeout', async () => {
      const bytes = Buffer.from('hung registration');
      const tree = new Map<string, Buffer | null>([
        ['/mnt/memory/hung-register', null],
        ['/mnt/memory/hung-register/live.txt', bytes],
      ]);
      let sawAbort = false;
      let recordStarted: (() => void) | null = null;
      const recordStartedPromise = new Promise<void>((resolve) => {
        recordStarted = resolve;
      });
      const client = {
        async listSessionMemories(): Promise<MemoryRecord[]> {
          return [];
        },
        async recordSessionMemoryVersion(input: { signal?: AbortSignal }): Promise<never> {
          recordStarted?.();
          return await new Promise<never>((_resolve, reject) => {
            input.signal?.addEventListener(
              'abort',
              () => {
                sawAbort = true;
                reject(new Error('registration aborted'));
              },
              { once: true },
            );
          });
        },
      } as unknown as RegistryClient;
      const { store: ts } = fakeStore();

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [watchedStore({ storeId: 'mems_timeout', mountPath: '/mnt/memory/hung-register' })],
        registry: client,
        store: ts,
        sandbox: fakeInMemorySandbox(tree),
        intervalMs: 0,
        stopTimeoutMs: 25,
      });

      await watcher.start();
      const tickPromise = watcher.tick();
      await recordStartedPromise;
      await watcher.stop();
      await tickPromise;

      expect(sawAbort).toBe(true);
    });
  });

  describe('InMemory path', () => {
    it('walks the sandbox FS recursively and registers each file', async () => {
      const fooBytes = Buffer.from('foo content');
      const barBytes = Buffer.from('bar content');
      const nestedBytes = Buffer.from('nested!');

      const tree = new Map<string, Buffer | null>([
        ['/mnt/memory/notes', null],
        ['/mnt/memory/notes/foo.txt', fooBytes],
        ['/mnt/memory/notes/bar.txt', barBytes],
        ['/mnt/memory/notes/sub', null],
        ['/mnt/memory/notes/sub/deep.txt', nestedBytes],
      ]);
      const sandbox = fakeInMemorySandbox(tree);

      const store = watchedStore({ storeId: 'mems_lcl', mountPath: '/mnt/memory/notes' });
      const { client, registerCalls } = fakeRegistryClient();
      const { store: ts } = fakeStore();

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        sandbox,
        intervalMs: 0,
      });

      await watcher.start();
      const result = await watcher.tick();

      expect(result.errors).toBe(0);
      expect(result.versionsRecorded).toBe(3);
      const paths = registerCalls.map((c) => c.path).sort();
      expect(paths).toEqual(['bar.txt', 'foo.txt', 'sub/deep.txt']);
      const fooCall = registerCalls.find((c) => c.path === 'foo.txt')!;
      expect(fooCall.contentSha256).toBe(sha256Hex(fooBytes));
      expect(fooCall.previousSha256).toBeNull();
      expect(fooCall.contentLength).toBe(fooBytes.length);

      // Same poll twice → cache hit → no re-registration.
      const result2 = await watcher.tick();
      expect(result2.versionsRecorded).toBe(0);
      expect(registerCalls).toHaveLength(3);
    });
  });

  /**
   * Observability hooks.
   *
   * The watcher's metrics flow through the harness-wide registry, so other
   * tests in this run share the same counter cells. Each test snapshots the
   * relevant cell before the action and asserts the delta — that way ordering
   * between specs doesn't matter.
   */
  describe('metrics', () => {
    it('increments harness_memory_watcher_poll_total{result="ok"} after a successful tick', async () => {
      const store = watchedStore({ storeId: 'mems_pollok' });
      const { client } = fakeRegistryClient();
      const { store: ts } = fakeStore();

      // Empty list → no registrations, but the tick still completes
      // successfully and must increment the OK poll counter.
      s3Mock.on(ListObjectsV2Command).resolves({ Contents: [], IsTruncated: false });

      const watcher = new MemoryVersionWatcher({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        s3: new S3Client({}),
        bucket: 'orca-mem',
        memoryKeyPrefix: 'memory/',
        intervalMs: 0,
      });

      const before = await readCounterCell({
        name: 'harness_memory_watcher_poll_total',
        labels: { result: 'ok' },
      });

      await watcher.start();
      const result = await watcher.tick();

      expect(result.errors).toBe(0);

      const after = await readCounterCell({
        name: 'harness_memory_watcher_poll_total',
        labels: { result: 'ok' },
      });
      expect(after - before).toBe(1);
    });

    it('increments harness_memory_versions_recorded_total{result="conflict"} when the registry flags a conflict', async () => {
      const newBytes = Buffer.from('conflicted bytes');

      const store = watchedStore({ storeId: 'mems_metconf' });
      const { client } = fakeRegistryClient({
        conflictPaths: new Set(['shared.txt']),
      });
      const { store: ts } = fakeStore();

      const prefix = 'memory/workspaces/ws_metconf/memory-stores/mems_metconf/live/';
      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [{ Key: `${prefix}shared.txt`, Size: newBytes.length }],
        IsTruncated: false,
      });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-mem', Key: `${prefix}shared.txt` })
        .resolves({ Body: s3Body(newBytes) as never });

      const watcher = new MemoryVersionWatcher({
        // Use a workspace label distinct from the OK-path test so the conflict
        // cell is unambiguous regardless of test ordering.
        workspaceId: 'ws_metconf',
        sessionId: 'ses_y',
        stores: [store],
        registry: client,
        store: ts,
        s3: new S3Client({}),
        bucket: 'orca-mem',
        memoryKeyPrefix: 'memory/',
        intervalMs: 0,
      });

      const conflictBefore = await readCounterCell({
        name: 'harness_memory_versions_recorded_total',
        labels: { workspace_id: 'ws_metconf', result: 'conflict' },
      });

      await watcher.start();
      const result = await watcher.tick();
      expect(result.conflicts).toBe(1);

      const conflictAfter = await readCounterCell({
        name: 'harness_memory_versions_recorded_total',
        labels: { workspace_id: 'ws_metconf', result: 'conflict' },
      });
      expect(conflictAfter - conflictBefore).toBe(1);
    });
  });
});
