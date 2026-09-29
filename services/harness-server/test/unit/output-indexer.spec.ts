// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Readable } from 'node:stream';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { GetObjectCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { mockClient } from 'aws-sdk-client-mock';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import type { RegistryClient, RegistryFileRecord } from '../../src/clients/registry.js';
import type {
  SandboxFiles,
  SandboxHandle,
  ToolCall,
  ToolResult,
} from '../../src/sandbox/sandbox-runtime.js';
import { OutputIndexer } from '../../src/sandbox/outputs/output-indexer.js';
import type { OutputMountHandle } from '../../src/sandbox/outputs/output-mount.js';

const s3Mock = mockClient(S3Client);

interface RecordedCreate {
  workspaceId: string;
  sessionId: string;
  filename: string;
  mimeType: string;
  contentLength: number;
}

function fakeFileRecord(overrides: Partial<RegistryFileRecord> = {}): RegistryFileRecord {
  return {
    id: overrides.id ?? `file_${Math.random().toString(36).slice(2, 10)}`,
    filename: overrides.filename ?? 'output.txt',
    mime_type: overrides.mime_type ?? 'application/octet-stream',
    size_bytes: overrides.size_bytes ?? 4,
    sha256: overrides.sha256 ?? 'a'.repeat(64),
    metadata: overrides.metadata ?? {},
    purpose: overrides.purpose ?? 'agent_output',
    scope_id: overrides.scope_id ?? 'ses_test',
    downloadable: overrides.downloadable ?? true,
    archived_at: overrides.archived_at ?? null,
    created_at: overrides.created_at ?? new Date().toISOString(),
    updated_at: overrides.updated_at ?? new Date().toISOString(),
  };
}

interface FakeRegistryClientResult {
  client: RegistryClient;
  calls: RecordedCreate[];
}

function fakeRegistryClient(opts?: {
  failOnFilename?: string;
  recordFactory?: (call: RecordedCreate) => RegistryFileRecord;
}): FakeRegistryClientResult {
  const calls: RecordedCreate[] = [];
  const client = {
    async createFile(input: {
      workspaceId: string;
      sessionId: string;
      content: Buffer;
      filename: string;
      mimeType: string;
    }): Promise<RegistryFileRecord> {
      const call: RecordedCreate = {
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        filename: input.filename,
        mimeType: input.mimeType,
        contentLength: input.content.length,
      };
      calls.push(call);
      if (opts?.failOnFilename && input.filename === opts.failOnFilename) {
        throw new Error(`forced failure for ${input.filename}`);
      }
      return opts?.recordFactory
        ? opts.recordFactory(call)
        : fakeFileRecord({ filename: input.filename });
    },
  } as unknown as RegistryClient;
  return { client, calls };
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
 * Map-backed `SandboxHandle` test double for the InMemory output-indexer
 * path. Keys are full paths; values:
 *   - `Buffer` — file contents.
 *   - `null`   — "this path is a directory" (so `files.list(path)` walks it
 *                and `files.read(path)` throws EISDIR like the real impl).
 *   - `Error`  — synthetic I/O failure for negative cases.
 */
function fakeInMemorySandbox(tree: Map<string, Buffer | null | Error>): SandboxHandle {
  const files: SandboxFiles = {
    async write() {
      throw new Error('not used');
    },
    async read(path: string): Promise<Buffer> {
      const v = tree.get(path);
      if (v instanceof Error) throw v;
      if (v === null) {
        const err = new Error(`EISDIR: illegal operation on a directory, read '${path}'`);
        (err as { code?: string }).code = 'EISDIR';
        throw err;
      }
      if (v === undefined) throw new Error(`ENOENT: ${path}`);
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

function s3Mount(): OutputMountHandle {
  return {
    kind: 's3',
    sandboxPath: '/mnt/session/outputs',
    s3: {
      bucket: 'orca-files',
      endpoint: 'http://minio:9000',
      prefix: 'outputs/ws_x/ses_y/',
    },
  };
}

function inMemoryMount(): OutputMountHandle {
  return { kind: 'inmemory_local', sandboxPath: '/mnt/session/outputs' };
}

function s3Body(buf: Buffer): NodeJS.ReadableStream {
  // The indexer casts `Body` to `NodeJS.ReadableStream` and drains it with
  // `for await`. A plain `Readable.from(buf)` satisfies that contract — we
  // don't need the full `StreamingBlobPayloadOutputTypes` shape because our
  // collector only iterates.
  return Readable.from(buf);
}

describe('OutputIndexer', () => {
  beforeEach(() => {
    s3Mock.reset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('S3 path', () => {
    it('registers each object in the prefix as a file_… record', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store, appended } = fakeStore();
      const indexer = new OutputIndexer();

      const aBytes = Buffer.from('hello A');
      const bBytes = Buffer.from('hello B!');

      s3Mock
        .on(ListObjectsV2Command, { Bucket: 'orca-files', Prefix: 'outputs/ws_x/ses_y/' })
        .resolves({
          Contents: [
            { Key: 'outputs/ws_x/ses_y/a.txt', Size: aBytes.length },
            { Key: 'outputs/ws_x/ses_y/b.txt', Size: bBytes.length },
          ],
          IsTruncated: false,
        });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-files', Key: 'outputs/ws_x/ses_y/a.txt' })
        .resolves({ Body: s3Body(aBytes) as never, ContentType: 'text/plain' });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-files', Key: 'outputs/ws_x/ses_y/b.txt' })
        .resolves({ Body: s3Body(bBytes) as never, ContentType: 'text/plain' });

      const result = await indexer.indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: s3Mount(),
        s3: new S3Client({}),
        registry: client,
        store,
      });

      expect(result.count).toBe(2);
      expect(result.skipped).toBe(0);
      expect(result.errors).toEqual([]);
      expect(calls).toHaveLength(2);
      const filenames = calls.map((c) => c.filename).sort();
      expect(filenames).toEqual(['a.txt', 'b.txt']);
      for (const call of calls) {
        expect(call.workspaceId).toBe('ws_x');
        expect(call.sessionId).toBe('ses_y');
        expect(call.mimeType).toBe('text/plain');
      }
      expect(appended).toHaveLength(2);
      const outputPaths: string[] = [];
      for (const e of appended) {
        expect(e.kind).toBe('session.output_indexed');
        const payload = JSON.parse(Buffer.from(e.payload).toString('utf8')) as {
          file_id: string;
          key: string;
        };
        expect(payload.file_id).toMatch(/^file_/);
        expect(payload.key).not.toContain('ws_x');
        expect(payload.key).not.toContain('outputs/');
        outputPaths.push(payload.key);
      }
      expect(outputPaths.sort()).toEqual(['a.txt', 'b.txt']);
    });

    it('skips objects that exceed maxBytesPerFile (counted in skipped, not count)', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store } = fakeStore();
      const indexer = new OutputIndexer();

      const small = Buffer.from('small');
      const big = Buffer.alloc(1024 * 1024); // 1 MB

      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [
          { Key: 'outputs/ws_x/ses_y/small.txt', Size: small.length },
          { Key: 'outputs/ws_x/ses_y/big.bin', Size: big.length },
        ],
        IsTruncated: false,
      });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-files', Key: 'outputs/ws_x/ses_y/small.txt' })
        .resolves({ Body: s3Body(small) as never });

      const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

      const result = await indexer.indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: s3Mount(),
        s3: new S3Client({}),
        registry: client,
        store,
        maxBytesPerFile: 1024, // 1 KB cap; only `small` fits.
      });

      expect(result.count).toBe(1);
      expect(result.skipped).toBe(1);
      expect(result.errors).toEqual([]);
      expect(calls).toHaveLength(1);
      expect(calls[0]!.filename).toBe('small.txt');
      expect(warn).toHaveBeenCalled();
    });

    it('records a per-key error when createFile throws but continues with other keys', async () => {
      const { client, calls } = fakeRegistryClient({ failOnFilename: 'b.txt' });
      const { store } = fakeStore();
      const indexer = new OutputIndexer();

      const aBytes = Buffer.from('hello A');
      const bBytes = Buffer.from('hello B');
      const cBytes = Buffer.from('hello C');

      s3Mock.on(ListObjectsV2Command).resolves({
        Contents: [
          { Key: 'outputs/ws_x/ses_y/a.txt', Size: aBytes.length },
          { Key: 'outputs/ws_x/ses_y/b.txt', Size: bBytes.length },
          { Key: 'outputs/ws_x/ses_y/c.txt', Size: cBytes.length },
        ],
        IsTruncated: false,
      });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-files', Key: 'outputs/ws_x/ses_y/a.txt' })
        .resolves({ Body: s3Body(aBytes) as never });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-files', Key: 'outputs/ws_x/ses_y/b.txt' })
        .resolves({ Body: s3Body(bBytes) as never });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-files', Key: 'outputs/ws_x/ses_y/c.txt' })
        .resolves({ Body: s3Body(cBytes) as never });

      vi.spyOn(console, 'error').mockImplementation(() => {});

      const result = await indexer.indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: s3Mount(),
        s3: new S3Client({}),
        registry: client,
        store,
      });

      expect(result.count).toBe(2);
      expect(result.skipped).toBe(0);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]!.key).toBe('outputs/ws_x/ses_y/b.txt');
      expect(result.errors[0]!.error).toMatch(/forced failure/);
      expect(calls).toHaveLength(3);
    });

    it('returns count=0 for an empty prefix', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store, appended } = fakeStore();
      const indexer = new OutputIndexer();

      s3Mock.on(ListObjectsV2Command).resolves({ Contents: [], IsTruncated: false });

      const result = await indexer.indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: s3Mount(),
        s3: new S3Client({}),
        registry: client,
        store,
      });

      expect(result.count).toBe(0);
      expect(result.skipped).toBe(0);
      expect(result.errors).toEqual([]);
      expect(calls).toHaveLength(0);
      expect(appended).toHaveLength(0);
    });

    it('paginates ListObjectsV2 via NextContinuationToken', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store } = fakeStore();
      const indexer = new OutputIndexer();

      const a = Buffer.from('aaa');
      const b = Buffer.from('bbbb');

      s3Mock
        .on(ListObjectsV2Command)
        .resolvesOnce({
          Contents: [{ Key: 'outputs/ws_x/ses_y/a.txt', Size: a.length }],
          IsTruncated: true,
          NextContinuationToken: 'tok-1',
        })
        .resolvesOnce({
          Contents: [{ Key: 'outputs/ws_x/ses_y/b.txt', Size: b.length }],
          IsTruncated: false,
        });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-files', Key: 'outputs/ws_x/ses_y/a.txt' })
        .resolves({ Body: s3Body(a) as never });
      s3Mock
        .on(GetObjectCommand, { Bucket: 'orca-files', Key: 'outputs/ws_x/ses_y/b.txt' })
        .resolves({ Body: s3Body(b) as never });

      const result = await indexer.indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: s3Mount(),
        s3: new S3Client({}),
        registry: client,
        store,
      });

      expect(result.count).toBe(2);
      expect(calls.map((c) => c.filename).sort()).toEqual(['a.txt', 'b.txt']);
      // Two list calls: one initial + one continuation.
      const listCalls = s3Mock.commandCalls(ListObjectsV2Command);
      expect(listCalls).toHaveLength(2);
      expect(listCalls[1]!.args[0].input.ContinuationToken).toBe('tok-1');
    });
  });

  describe('InMemory path', () => {
    it('walks the sandbox FS and registers each file', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store, appended } = fakeStore();
      const indexer = new OutputIndexer();

      const tree = new Map<string, Buffer | null | Error>([
        ['/mnt/session/outputs', null],
        ['/mnt/session/outputs/file1.txt', Buffer.from('file 1 content')],
        ['/mnt/session/outputs/file2.txt', Buffer.from('file 2 content here')],
      ]);
      const sandbox = fakeInMemorySandbox(tree);

      const result = await indexer.indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox,
        registry: client,
        store,
      });

      expect(result.count).toBe(2);
      expect(result.skipped).toBe(0);
      expect(result.errors).toEqual([]);
      const filenames = calls.map((c) => c.filename).sort();
      expect(filenames).toEqual(['file1.txt', 'file2.txt']);
      for (const call of calls) {
        expect(call.workspaceId).toBe('ws_x');
        expect(call.sessionId).toBe('ses_y');
      }
      expect(appended).toHaveLength(2);
    });

    it('does not register unchanged output bytes twice across scans', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store, appended } = fakeStore();
      const indexer = new OutputIndexer();
      const tree = new Map<string, Buffer | null | Error>([
        ['/mnt/session/outputs', null],
        ['/mnt/session/outputs/poem.txt', Buffer.from('same poem')],
      ]);
      const input = {
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox: fakeInMemorySandbox(tree),
        registry: client,
        store,
      };

      expect(await indexer.indexSession(input)).toMatchObject({ count: 1, errors: [] });
      expect(await indexer.indexSession(input)).toMatchObject({ count: 0, errors: [] });
      expect(calls).toHaveLength(1);
      expect(appended).toHaveLength(1);
    });

    it('registers the same filename and bytes again for a new runner generation', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store } = fakeStore();
      const tree = new Map<string, Buffer | null | Error>([
        ['/mnt/session/outputs', null],
        ['/mnt/session/outputs/poem.txt', Buffer.from('same poem')],
      ]);
      const input = {
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox: fakeInMemorySandbox(tree),
        registry: client,
        store,
      };

      expect(await new OutputIndexer().indexSession(input)).toMatchObject({ count: 1 });
      expect(await new OutputIndexer().indexSession(input)).toMatchObject({ count: 1 });
      expect(calls).toHaveLength(2);
      expect(calls[0]?.filename).toBe('poem.txt');
      expect(calls[1]?.filename).toBe('poem.txt');
    });

    it('registers a new file record when an output path changes content', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store } = fakeStore();
      const indexer = new OutputIndexer();
      const tree = new Map<string, Buffer | null | Error>([
        ['/mnt/session/outputs', null],
        ['/mnt/session/outputs/poem.txt', Buffer.from('first draft')],
      ]);
      const input = {
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox: fakeInMemorySandbox(tree),
        registry: client,
        store,
      };

      expect(await indexer.indexSession(input)).toMatchObject({ count: 1 });
      tree.set('/mnt/session/outputs/poem.txt', Buffer.from('final draft'));
      expect(await indexer.indexSession(input)).toMatchObject({ count: 1 });
      expect(calls).toHaveLength(2);
    });

    it('registers different paths independently even when their bytes match', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store, appended } = fakeStore();
      const indexer = new OutputIndexer();
      const sameBytes = Buffer.from('shared bytes');
      const tree = new Map<string, Buffer | null | Error>([
        ['/mnt/session/outputs', null],
        ['/mnt/session/outputs/a.txt', sameBytes],
        ['/mnt/session/outputs/b.txt', sameBytes],
      ]);

      const result = await indexer.indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox: fakeInMemorySandbox(tree),
        registry: client,
        store,
      });

      expect(result).toMatchObject({ count: 2, errors: [] });
      expect(calls.map((call) => call.filename).sort()).toEqual(['a.txt', 'b.txt']);
      const eventPaths = appended.map((event) => {
        const payload = JSON.parse(Buffer.from(event.payload).toString('utf8')) as { key: string };
        expect(payload.key).not.toContain('/mnt/session');
        return payload.key;
      });
      expect(eventPaths.sort()).toEqual(['a.txt', 'b.txt']);
    });

    it('retries a path after registry registration fails', async () => {
      const { client, calls } = fakeRegistryClient({ failOnFilename: 'poem.txt' });
      const { store } = fakeStore();
      const indexer = new OutputIndexer();
      const tree = new Map<string, Buffer | null | Error>([
        ['/mnt/session/outputs', null],
        ['/mnt/session/outputs/poem.txt', Buffer.from('retry me')],
      ]);
      const input = {
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox: fakeInMemorySandbox(tree),
        registry: client,
        store,
      };
      vi.spyOn(console, 'error').mockImplementation(() => {});

      expect(await indexer.indexSession(input)).toMatchObject({ count: 0 });
      expect(await indexer.indexSession(input)).toMatchObject({ count: 0 });
      expect(calls).toHaveLength(2);
    });

    it('retries a failed readiness event without creating another File row', async () => {
      const { client, calls } = fakeRegistryClient();
      const recording = fakeStore();
      const append = recording.store.append.bind(recording.store);
      let failNextAppend = true;
      recording.store.append = async (workspaceId, sessionId, events) => {
        if (failNextAppend) {
          failNextAppend = false;
          throw new Error('transcript unavailable');
        }
        return await append(workspaceId, sessionId, events);
      };
      const indexer = new OutputIndexer();
      const input = {
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox: fakeInMemorySandbox(
          new Map<string, Buffer | null | Error>([
            ['/mnt/session/outputs', null],
            ['/mnt/session/outputs/poem.txt', Buffer.from('event retry')],
          ]),
        ),
        registry: client,
        store: recording.store,
      };
      vi.spyOn(console, 'error').mockImplementation(() => {});

      const first = await indexer.indexSession(input);
      expect(first).toMatchObject({ count: 1 });
      expect(first.errors[0]?.error).toMatch(/session.output_indexed/);
      expect(calls).toHaveLength(1);
      expect(recording.appended).toHaveLength(0);

      expect(await indexer.indexSession(input)).toMatchObject({ count: 0, errors: [] });
      expect(calls).toHaveLength(1);
      expect(recording.appended).toHaveLength(1);
      expect(recording.appended[0]?.kind).toBe('session.output_indexed');
    });

    it('preserves the full pending readiness-event batch when a retry is aborted', async () => {
      const { client, calls } = fakeRegistryClient();
      const recording = fakeStore();
      const append = recording.store.append.bind(recording.store);
      let appendMode: 'fail' | 'hang' | 'succeed' = 'fail';
      let appendCalls = 0;
      recording.store.append = async (workspaceId, sessionId, events) => {
        appendCalls += 1;
        if (appendMode === 'fail') throw new Error('transcript unavailable');
        if (appendMode === 'hang') return await new Promise<string[]>(() => {});
        return await append(workspaceId, sessionId, events);
      };
      const indexer = new OutputIndexer();
      const input = {
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox: fakeInMemorySandbox(
          new Map<string, Buffer | null | Error>([
            ['/mnt/session/outputs', null],
            ['/mnt/session/outputs/first.txt', Buffer.from('first')],
            ['/mnt/session/outputs/second.txt', Buffer.from('second')],
          ]),
        ),
        registry: client,
        store: recording.store,
      };
      vi.spyOn(console, 'error').mockImplementation(() => {});

      expect(await indexer.indexSession(input)).toMatchObject({ count: 2 });
      expect(calls).toHaveLength(2);
      expect(recording.appended).toHaveLength(0);

      appendMode = 'hang';
      const controller = new AbortController();
      const abortedRetry = indexer.indexSession({ ...input, signal: controller.signal });
      await vi.waitFor(() => expect(appendCalls).toBe(3));
      const abort = new Error('shutdown');
      abort.name = 'AbortError';
      controller.abort(abort);
      await expect(abortedRetry).rejects.toMatchObject({ name: 'AbortError' });

      appendMode = 'succeed';
      expect(await indexer.indexSession(input)).toMatchObject({ count: 0, errors: [] });
      expect(calls).toHaveLength(2);
      expect(recording.appended).toHaveLength(2);
      expect(new Set(recording.appended.map((event) => event.id)).size).toBe(2);
    });

    it('recurses into subdirectories', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store } = fakeStore();
      const indexer = new OutputIndexer();

      const tree = new Map<string, Buffer | null | Error>([
        ['/mnt/session/outputs', null],
        ['/mnt/session/outputs/charts', null],
        ['/mnt/session/outputs/charts/foo.png', Buffer.from('PNG')],
      ]);
      const sandbox = fakeInMemorySandbox(tree);

      const result = await indexer.indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox,
        registry: client,
        store,
      });

      expect(result.count).toBe(1);
      expect(calls).toHaveLength(1);
      expect(calls[0]!.filename).toBe('charts/foo.png');
    });

    it('returns gracefully (count=0) when the outputs dir does not exist', async () => {
      const { client, calls } = fakeRegistryClient();
      const { store } = fakeStore();
      const indexer = new OutputIndexer();

      const sandbox = fakeInMemorySandbox(new Map());
      vi.spyOn(console, 'warn').mockImplementation(() => {});

      const result = await indexer.indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox,
        registry: client,
        store,
      });

      expect(result.count).toBe(0);
      expect(calls).toHaveLength(0);
    });

    it('aborts a stuck local filesystem operation before teardown', async () => {
      const { client } = fakeRegistryClient();
      const { store } = fakeStore();
      const sandbox = fakeInMemorySandbox(
        new Map<string, Buffer | null | Error>([['/mnt/session/outputs', null]]),
      );
      sandbox.files.list = async () => await new Promise<string[]>(() => {});
      const controller = new AbortController();
      const pending = new OutputIndexer().indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        sandbox,
        registry: client,
        store,
        signal: controller.signal,
      });
      const error = new Error('shutdown');
      error.name = 'AbortError';
      controller.abort(error);

      await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    });

    it('records errors in result.errors when sandbox missing for inmemory_local', async () => {
      const { client } = fakeRegistryClient();
      const { store } = fakeStore();
      const indexer = new OutputIndexer();

      const result = await indexer.indexSession({
        workspaceId: 'ws_x',
        sessionId: 'ses_y',
        mount: inMemoryMount(),
        registry: client,
        store,
      });

      expect(result.count).toBe(0);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]!.error).toMatch(/requires sandbox/);
    });
  });
});
