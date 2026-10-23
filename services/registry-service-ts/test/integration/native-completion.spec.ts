// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeAll, afterAll, expect, it, vi } from 'vitest';
import { and, eq } from 'drizzle-orm';
import {
  NATIVE_USAGE_SOURCE,
  OUTPUT_RESOURCE_ID,
  resourceCheckpointDigest,
  type ResourceCheckpoint,
} from '@orca/sandbox-runtime';
import { createKafkaTranscriptCodec, type Event } from '@orca/transcript-store';
import { createRequire } from 'node:module';
import {
  NativeCompletionCoordinator,
  hasNativeUsageAuthority,
} from '../../src/domain/native-completion.js';
import {
  agents,
  agentVersions,
  environments,
  sessions,
  sessionNativeCompletions,
  sessionUsageEvents,
} from '../../src/persistence/postgres/schema.js';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { buildCombinedTestApp } from '../../src/server.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';
import { newId } from '../../src/domain/versioning.js';
import { createServer } from 'node:http';
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { createHash } from 'node:crypto';
import { InMemoryBlobStore, InMemoryFileMetadataStore, LocalFileStore } from '@orca/file-store';
import { tokenBoundRunnerId } from '@orca/harness-tunnel';
import { SessionResourcesDelivery } from '../../src/tunnel/session-resources-delivery.js';
import { SessionLoop } from '../../../session-runner/src/session-loop.js';
import { ProviderRegistry } from '../../../session-runner/src/harness/provider.js';
import { registerPiProvider } from '../../../session-runner/src/harness/pi/provider.js';
import { LocalSandboxRuntime, SandboxManager } from '@orca/sandbox-runtime';
import { registerSessionHandlers } from '../../../session-runner/src/register-handlers.js';
import { RouteDispatcher } from '../../../session-runner/src/tunnel/request-dispatch.js';
import { serveTunnel } from '../../../session-runner/src/tunnel/serve.js';
import { FakeRegistryRunnerTunnel } from '../../../session-runner/test/unit/support/fake-registry-runner-tunnel.js';

// Resolve the controlled installation in Node, whose import.meta.resolve Vitest does not emulate.
vi.mock('../../../../packages/pi-harness/dist/index.js', async () => {
  const { createRequire } = await import('node:module');
  return createRequire(new URL('../../../session-runner/package.json', import.meta.url))(
    '@orca/pi-harness',
  );
});

// Use the installed KafkaJS encoder so payload-only sizing cannot hide metadata/framing overflow.
const kafkaRequire = createRequire(
  new URL('../../../../packages/transcript-store/package.json', import.meta.url),
);
const encodeRecord = kafkaRequire('kafkajs/src/protocol/recordBatch/record/v0') as (message: {
  key: Buffer;
  value: Buffer;
  headers: Record<string, Buffer>;
  offsetDelta: number;
}) => { buffer: Buffer };
const { RecordBatch: encodeBatch } = kafkaRequire('kafkajs/src/protocol/recordBatch/v0') as {
  RecordBatch(input: { records: Array<{ buffer: Buffer }> }): Promise<{ buffer: Buffer }>;
};
const rawCodec = createKafkaTranscriptCodec();
const BROKER_BATCH_BYTES = 1024 * 1024;
async function checkedBatchBytes(events: Event[]): Promise<number> {
  const records = await Promise.all(
    events.map(async (event, offsetDelta) => {
      const message = await rawCodec.encode(event);
      return encodeRecord({
        key: Buffer.from(message.key ?? ''),
        value: Buffer.from(message.value ?? ''),
        headers: message.headers as Record<string, Buffer>,
        offsetDelta,
      });
    }),
  );
  const bytes = (await encodeBatch({ records })).buffer.byteLength;
  if (bytes > BROKER_BATCH_BYTES) throw new Error('Kafka record batch exceeds 1 MiB');
  return bytes;
}

function replayEvents(count: number, bytes = 1024) {
  return Array.from({ length: count }, (_, index) => ({
    id: 'evt_chunk_' + index,
    type: 'agent.message',
    content: [{ type: 'text', text: 'x'.repeat(bytes) }],
  }));
}

let db: Awaited<ReturnType<typeof getTestDb>>['db'];
const workspaceId = uniqueWorkspace('native_completion');
beforeAll(async () => {
  ({ db } = await getTestDb());
  await createTestApiKey(db, workspaceId);
});
afterAll(async () => {
  await rawCodec.close();
  await closeTestDb();
});

async function fixture(begin = true, claim = true) {
  const sessionId = newId('ses'),
    agentId = newId('agt'),
    environmentId = newId('env'),
    runnerId = 'runner_fixture';
  const metadata = { harness: 'pi', mode: 'colocated' };
  const model = { provider: 'anthropic', id: 'claude-sonnet-4' };
  await db.insert(agents).values({
    id: agentId,
    workspaceId,
    name: 'native',
    harnessType: 'pi',
    modelProvider: model.provider,
    modelId: model.id,
    metadata,
  });
  await db.insert(agentVersions).values({
    id: newId('agtv'),
    workspaceId,
    agentId,
    version: 1,
    snapshot: {
      id: agentId,
      name: 'native',
      version: 1,
      harness_type: 'pi',
      model,
      system: '',
      tools: [],
      mcp_servers: [],
      skills: [],
      metadata,
      multiagent: null,
    },
  });
  await db
    .insert(environments)
    .values({ id: environmentId, workspaceId, name: environmentId, target: 'self_hosted' });
  await db.insert(sessions).values({
    id: sessionId,
    workspaceId,
    agentId,
    agentVersion: 1,
    environmentId,
    runnerId,
    distributionState: 'assigned',
  });
  const store = buildStubStore();
  const events: Event[] = [];
  vi.spyOn(store, 'read').mockImplementation(async function* () {
    yield* events;
  });
  const append = vi.spyOn(store, 'append').mockImplementation(async (_ws, _session, batch) => {
    events.push(...batch);
    return [];
  });
  let current = true;
  const options = { db, workspaceId, sessionId, runnerId, store, isCurrent: async () => current };
  const coordinator = new NativeCompletionCoordinator(options);
  const binding = {
    executionId: 'exec_fixture',
    placementId: 'placement_fixture',
    revision: 'a'.repeat(64),
  };
  if (claim) await coordinator.claim(binding);
  const turnId = newId('evt');
  const event: Event = {
    id: turnId,
    workspaceId,
    sessionId,
    subpath: '',
    seq: 1,
    kind: 'user.message',
    producedBy: 'client',
    producedAt: new Date().toISOString(),
    idempotencyKey: '',
    payload: Buffer.from('{}'),
  };
  if (begin) await coordinator.begin(event, binding);
  const checkpoint: ResourceCheckpoint = {
    version: 1,
    checkpoint_id: 'rchk_fixture',
    revision: binding.revision,
    files: [],
    deleted: [],
    completion: {
      version: 1,
      source: NATIVE_USAGE_SOURCE,
      turn_id: turnId,
      execution_id: binding.executionId,
      placement_id: binding.placementId,
      outcome: 'success',
      report_set_sealed: true,
      expected_reports: ['report_1', 'report_2'],
      reports: [13, 17].map((input, index) => ({
        id: 'report_' + (index + 1),
        provider: 'anthropic',
        model: 'unpriced-fixture-model',
        semantics: 'delta',
        complete: true,
        usage: {
          input_tokens: input,
          output_tokens: index ? 11 : 7,
          cache_read_input_tokens: 0,
          cache_creation: { ephemeral_5m_input_tokens: 0, ephemeral_1h_input_tokens: 0 },
        },
      })),
    },
  };
  const scope = and(
    eq(sessionNativeCompletions.workspaceId, workspaceId),
    eq(sessionNativeCompletions.sessionId, sessionId),
    eq(sessionNativeCompletions.turnId, turnId),
  );
  const row = async () => (await db.select().from(sessionNativeCompletions).where(scope))[0]!;
  const session = async () =>
    (
      await db
        .select()
        .from(sessions)
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)))
    )[0]!;
  return {
    coordinator,
    binding,
    checkpoint,
    event,
    store,
    events,
    append,
    row,
    session,
    sessionId,
    replace: (over: Partial<typeof options> = {}) =>
      new NativeCompletionCoordinator({ ...options, ...over }),
    retire: () => {
      current = false;
    },
  };
}

it('rejects a shared usage writer that passed the outer check while native authority was uncommitted', async () => {
  const f = await fixture(false, false);
  expect(await f.coordinator.hasAuthority()).toBe(false);
  const app = buildCombinedTestApp({
    db,
    oidc: { allowedIssuers: [], audience: 'orca-agent-engine' },
    store: f.store,
    sse: STUB_SSE_CONFIG,
    jwtMinter: buildTestJwtMinter(),
    fileStore: buildStubFileStore(),
  });
  await app.ready();
  let release!: () => void;
  let inserted!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const uncommitted = new Promise<void>((resolve) => {
    inserted = resolve;
  });
  let checks = 0;
  const coordinator = f.replace({
    isCurrent: async () => {
      if (++checks === 2) {
        inserted();
        await gate;
      }
      return true;
    },
  });
  const claim = coordinator.claim(f.binding);
  await uncommitted;
  let entered!: () => void;
  const usageTransaction = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const original = db.transaction.bind(db);
  const transaction = vi.spyOn(db, 'transaction').mockImplementation((action, config) => {
    entered();
    return original(action, config);
  });
  try {
    const response = Promise.resolve(
      app.inject({
        method: 'POST',
        url: '/internal/v1/workspaces/' + workspaceId + '/sessions/' + f.sessionId + '/usage',
        payload: { usage: { input_tokens: 7, output_tokens: 2 } },
      }),
    );
    await usageTransaction;
    release();
    await claim;
    const result = await response;
    expect(result.statusCode).toBe(403);
    expect(result.json()).toEqual({ error: 'Registry owns native usage accounting' });
    expect((await f.session()).usageInputTokens).toBe(0);
  } finally {
    release();
    transaction.mockRestore();
    await app.close();
  }
});

it('replays 10,000 stable events with one transcript scan and bounded lock transactions, including concurrent finish', async () => {
  const f = await fixture();
  f.checkpoint.completion!.events = Array.from({ length: 10000 }, (_, index) => ({
    id: 'evt_long_' + index,
    type: 'agent.message',
    content: [],
  }));
  await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
  vi.mocked(f.store.read).mockClear();
  const transactions = vi.spyOn(db, 'transaction');
  const batchBytes: number[] = [];
  f.append.mockImplementation(async (_w, _s, batch) => {
    batchBytes.push(await checkedBatchBytes(batch));
    f.events.push(...batch);
    return [];
  });
  try {
    await Promise.all([
      f.coordinator.finish(f.event.id, f.binding.revision),
      f.coordinator.finish(f.event.id, f.binding.revision),
    ]);
    expect(f.store.read).toHaveBeenCalledTimes(1);
    expect(f.append.mock.calls.length).toBeGreaterThan(4);
    expect(f.append.mock.calls.length).toBeLessThanOrEqual(64);
    expect(Math.max(...batchBytes)).toBeLessThanOrEqual(BROKER_BATCH_BYTES);
    expect(transactions.mock.calls.length).toBeLessThanOrEqual(10);
    expect(f.events).toHaveLength(10003);
    expect(new Set(f.events.map((event) => event.id)).size).toBe(10003);
    expect((await f.row()).phase).toBe('completed');
  } finally {
    transactions.mockRestore();
  }
});

it('splits replay larger than the Kafka batch limit while preserving order and one history scan', async () => {
  const f = await fixture();
  const events = replayEvents(2000);
  f.checkpoint.completion!.events = events;
  await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
  vi.mocked(f.store.read).mockClear();
  const batchBytes: number[] = [];
  f.append.mockImplementation(async (_w, _s, batch) => {
    batchBytes.push(await checkedBatchBytes(batch));
    f.events.push(...batch);
    return [];
  });
  const phases: string[] = [];
  await f.coordinator.finish(f.event.id, f.binding.revision, async (phase) => {
    phases.push(phase);
    if (phase === 'prepared')
      expect(f.events.map((event) => event.id)).toEqual(events.map((event) => event.id));
  });
  expect(f.store.read).toHaveBeenCalledTimes(1);
  expect(batchBytes.length).toBeGreaterThan(4);
  expect(batchBytes.reduce((total, bytes) => total + bytes, 0)).toBeGreaterThan(BROKER_BATCH_BYTES);
  expect(Math.max(...batchBytes)).toBeLessThanOrEqual(BROKER_BATCH_BYTES);
  expect(f.events.slice(0, events.length).map((event) => event.id)).toEqual(
    events.map((event) => event.id),
  );
  expect(phases).toEqual(['prepared', 'completed']);
  expect((await f.row()).phase).toBe('completed');
});

it.each([false, true])(
  'reconciles partial multi-chunk replay and lost ACK without duplicate IDs (%s)',
  async (lostAck) => {
    const f = await fixture();
    const events = replayEvents(2000);
    f.checkpoint.completion!.events = events;
    await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
    vi.mocked(f.store.read).mockClear();
    let appends = 0;
    f.append.mockImplementation(async (_w, _s, batch) => {
      await checkedBatchBytes(batch);
      if (++appends === 2) {
        if (lostAck) f.events.push(...batch);
        throw new Error('middle chunk ACK unavailable');
      }
      f.events.push(...batch);
      return [];
    });
    const ack = vi.fn(async () => {});
    await expect(f.coordinator.finish(f.event.id, f.binding.revision, ack)).rejects.toThrow(
      'middle chunk ACK unavailable',
    );
    expect(f.events.length).toBeGreaterThan(0);
    expect(f.events.length).toBeLessThan(events.length);
    expect(ack).not.toHaveBeenCalled();
    expect((await f.row()).phase).toBe('prepared');
    const committedIds = f.events.map((event) => event.id);
    const callsBeforeRetry = f.append.mock.calls.length;
    await f.coordinator.finish(f.event.id, f.binding.revision, ack);
    expect(f.store.read).toHaveBeenCalledTimes(2);
    expect(f.events.slice(0, events.length).map((event) => event.id)).toEqual(
      events.map((event) => event.id),
    );
    expect(new Set(f.events.map((event) => event.id)).size).toBe(events.length + 3);
    const retriedIds = f.append.mock.calls
      .slice(callsBeforeRetry)
      .flatMap(([, , batch]) => batch.map((event) => event.id));
    expect(retriedIds.some((id) => committedIds.includes(id))).toBe(false);
    expect((await f.row()).phase).toBe('completed');
    expect((await f.session()).usageInputTokens).toBe(30);
  },
);

it('fences every replay chunk and lets a replacement reconcile the already committed prefix', async () => {
  const f = await fixture();
  const events = replayEvents(2000);
  f.checkpoint.completion!.events = events;
  await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
  vi.mocked(f.store.read).mockClear();
  f.append.mockImplementation(async (_w, _s, batch) => {
    await checkedBatchBytes(batch);
    f.events.push(...batch);
    f.retire();
    return [];
  });
  const ack = vi.fn(async () => {});
  await expect(f.coordinator.finish(f.event.id, f.binding.revision, ack)).rejects.toThrow(
    'generation retired',
  );
  expect(f.append).toHaveBeenCalledTimes(1);
  expect(f.events.length).toBeGreaterThan(0);
  expect(f.events.length).toBeLessThan(events.length);
  expect(ack).not.toHaveBeenCalled();
  expect((await f.row()).phase).toBe('prepared');
  const replacement = f.replace({ isCurrent: async () => true });
  await replacement.claim(f.binding);
  await replacement.finish(f.event.id, f.binding.revision, ack);
  expect(f.store.read).toHaveBeenCalledTimes(2);
  expect(f.events.slice(0, events.length).map((event) => event.id)).toEqual(
    events.map((event) => event.id),
  );
  expect(new Set(f.events.map((event) => event.id)).size).toBe(events.length + 3);
  expect((await f.row()).phase).toBe('completed');
});

it.each([900, 1100])(
  'keeps a single large event subject to the existing broker limit (%s KiB)',
  async (kib) => {
    const f = await fixture();
    f.checkpoint.completion!.events = replayEvents(1, kib * 1024);
    await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
    f.append.mockImplementation(async (_w, _s, batch) => {
      await checkedBatchBytes(batch);
      f.events.push(...batch);
      return [];
    });
    const ack = vi.fn(async () => {});
    if (kib > 1024) {
      await expect(f.coordinator.finish(f.event.id, f.binding.revision, ack)).rejects.toThrow(
        'Kafka record batch exceeds 1 MiB',
      );
      expect(ack).not.toHaveBeenCalled();
      expect(f.events).toEqual([]);
      expect((await f.row()).phase).toBe('prepared');
      await expect(
        f.coordinator.begin({ ...f.event, id: newId('evt') }, f.binding),
      ).rejects.toThrow('blocks');
    } else {
      await f.coordinator.finish(f.event.id, f.binding.revision, ack);
      expect(f.events[0]!.payload.byteLength).toBeGreaterThan(kib * 1024);
      expect(JSON.parse(Buffer.from(f.events[0]!.payload).toString()).content[0].text).toHaveLength(
        kib * 1024,
      );
      expect((await f.row()).phase).toBe('completed');
    }
  },
);

it('refuses a broker append if its generation retires during the initial ID scan', async () => {
  const f = await fixture();
  await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
  vi.mocked(f.store.read).mockImplementationOnce(async function* () {
    f.retire();
    yield* [];
  });
  await expect(f.coordinator.finish(f.event.id, f.binding.revision)).rejects.toThrow(
    'generation retired',
  );
  expect(f.append).not.toHaveBeenCalled();
  expect((await f.row()).phase).toBe('prepared');
});

it('serializes concurrent finish after a lost append ACK and rescans only on ambiguity', async () => {
  const f = await fixture();
  await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
  f.append.mockImplementationOnce(async (_w, _s, batch) => {
    f.events.push(...batch);
    throw new Error('lost append ACK');
  });
  vi.mocked(f.store.read).mockClear();
  const results = await Promise.allSettled([
    f.coordinator.finish(f.event.id, f.binding.revision),
    f.coordinator.finish(f.event.id, f.binding.revision),
  ]);
  expect(results.map((result) => result.status)).toEqual(['rejected', 'fulfilled']);
  expect(f.store.read).toHaveBeenCalledTimes(2);
  expect(f.events.filter((event) => event.kind === 'agent.turn_completed')).toHaveLength(1);
  expect((await f.row()).phase).toBe('completed');
});

it('holds authority takeover behind a live broker append and reconciles that event during finish', async () => {
  const f = await fixture();
  const live = {
    ...f.event,
    id: 'evt_live_fenced',
    kind: 'agent.message',
    payload: Buffer.from(JSON.stringify({ type: 'agent.message', source_event_id: f.event.id })),
  };
  f.checkpoint.completion!.events = [{ id: live.id, type: live.kind, content: [] }];
  await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
  let release!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const appending = new Promise<void>((resolve) => {
    entered = resolve;
  });
  f.append.mockImplementationOnce(async (_w, _s, batch) => {
    entered();
    await gate;
    f.events.push(...batch);
    return [];
  });
  const liveAppend = f.coordinator.appendEvents(f.event.id, f.binding.revision, [live]);
  await appending;
  const replacement = f.replace();
  const takeover = replacement.claim(f.binding);
  release();
  await Promise.all([liveAppend, takeover]);
  await expect(f.coordinator.appendEvents(f.event.id, f.binding.revision, [live])).rejects.toThrow(
    'superseded',
  );
  await replacement.finish(f.event.id, f.binding.revision);
  expect(f.events.filter((event) => event.id === live.id)).toHaveLength(1);
  expect((await f.row()).phase).toBe('completed');
});

it('commits both stable reports once before no-output completion becomes visible', async () => {
  const f = await fixture(),
    digest = resourceCheckpointDigest(f.checkpoint);
  expect(await hasNativeUsageAuthority(db, workspaceId, f.sessionId)).toBe(true);
  await f.coordinator.stage(f.checkpoint, digest);
  await f.coordinator.prepare(f.checkpoint, digest);
  expect(f.events).toEqual([]);
  expect((await f.row()).phase).toBe('prepared');
  expect((await f.session()).usageInputTokens).toBe(30);
  const phases: string[] = [];
  await f.coordinator.finish(f.event.id, f.binding.revision, async (phase) => {
    phases.push(phase);
    if (phase === 'prepared') expect(f.events).toEqual([]);
    else expect(f.events.some((e) => e.kind === 'agent.turn_completed')).toBe(true);
  });
  await f.coordinator.prepare(f.checkpoint, digest);
  await f.coordinator.finish(f.event.id, f.binding.revision);
  expect(phases).toEqual(['prepared', 'completed']);
  expect((await f.row()).phase).toBe('completed');
  expect((await f.session()).usageInputTokens).toBe(30);
  expect(f.events.filter((e) => e.kind === 'agent.turn_completed')).toHaveLength(1);
  expect(
    await db
      .select()
      .from(sessionUsageEvents)
      .where(
        and(
          eq(sessionUsageEvents.workspaceId, workspaceId),
          eq(sessionUsageEvents.sessionId, f.sessionId),
        ),
      ),
  ).toHaveLength(2);
});

it('retains prepared evidence through ACK loss, rejects changed payload and fences the old owner', async () => {
  const f = await fixture(),
    digest = resourceCheckpointDigest(f.checkpoint);
  await f.coordinator.prepare(f.checkpoint, digest);
  await expect(
    f.coordinator.finish(f.event.id, f.binding.revision, async () => {
      throw new Error('ACK lost');
    }),
  ).rejects.toThrow('ACK lost');
  expect((await f.row()).phase).toBe('prepared');
  const changed = structuredClone(f.checkpoint);
  changed.completion!.reports[0]!.usage!.input_tokens++;
  await expect(f.coordinator.prepare(changed, resourceCheckpointDigest(changed))).rejects.toThrow(
    'payload differs',
  );
  const next = f.replace();
  await next.claim(f.binding);
  await expect(f.coordinator.finish(f.event.id, f.binding.revision)).rejects.toThrow('superseded');
  await next.finish(f.event.id, f.binding.revision);
  expect((await f.row()).phase).toBe('completed');
});

it('repairs ACK-success/terminal-append-loss from the durable receipt without running a model', async () => {
  const f = await fixture();
  await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
  f.append.mockImplementationOnce(async () => {
    throw new Error('terminal persistence failed');
  });
  await expect(
    f.coordinator.finish(f.event.id, f.binding.revision, async () => {}),
  ).rejects.toThrow('terminal persistence');
  expect((await f.row()).phase).toBe('prepared');
  const next = f.replace();
  await next.claim(f.binding);
  await next.finish(f.event.id, f.binding.revision);
  expect(f.events.filter((e) => e.kind === 'agent.turn_completed')).toHaveLength(1);
  expect((await f.session()).usageInputTokens).toBe(30);
});

it('reconciles a lost terminal append ACK by stable ID', async () => {
  const f = await fixture();
  await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
  f.append.mockImplementationOnce(async (_w, _s, batch) => {
    f.events.push(...batch);
    throw new Error('append ACK lost');
  });
  await expect(f.coordinator.finish(f.event.id, f.binding.revision)).rejects.toThrow(
    'append ACK lost',
  );
  const next = f.replace();
  await next.claim(f.binding);
  await next.finish(f.event.id, f.binding.revision);
  expect(f.events.filter((e) => e.kind === 'agent.turn_completed')).toHaveLength(1);
});

it('keeps committed partial usage but never ACKs or completes missing report evidence', async () => {
  const f = await fixture();
  f.checkpoint.completion!.expected_reports.push('missing_report');
  await expect(
    f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint)),
  ).rejects.toThrow('incomplete');
  expect((await f.row()).phase).toBe('incomplete');
  expect((await f.session()).usageInputTokens).toBe(30);
  await expect(f.coordinator.finish(f.event.id, f.binding.revision)).rejects.toThrow(
    'not prepared',
  );
  expect(f.events.some((e) => e.kind === 'agent.turn_completed')).toBe(false);
  await expect(f.coordinator.begin({ ...f.event, id: newId('evt') }, f.binding)).rejects.toThrow(
    'blocks',
  );
});

it('marks process loss incomplete and rejects replay on another execution', async () => {
  const f = await fixture();
  const next = f.replace();
  await next.claim({ ...f.binding, executionId: 'replacement' });
  await expect(next.begin(f.event, { ...f.binding, executionId: 'replacement' })).rejects.toThrow(
    'new execution',
  );
  await expect(next.markLost(f.event.id, f.binding.revision)).rejects.toThrow('operator recovery');
  expect((await f.row()).phase).toBe('incomplete');
  expect(f.events.some((e) => e.kind === 'agent.turn_completed')).toBe(false);
});

it.each(['failed', 'interrupted'] as const)(
  'preserves %s instead of publishing a clean success',
  async (outcome) => {
    const f = await fixture();
    f.checkpoint.completion!.outcome = outcome;
    await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
    await f.coordinator.finish(f.event.id, f.binding.revision);
    expect(f.events.map((e) => e.kind).slice(0, 2)).toEqual([
      'agent.error',
      'agent.turn_completed',
    ]);
    expect((await f.row()).phase).toBe('completed');
  },
);

it('repairs missing public events before ACK with their live IDs and deduplicates a lost append ACK', async () => {
  const f = await fixture();
  f.checkpoint.completion!.events = [
    { id: 'evt_live_approval', type: 'agent.requires_action', tool_use_id: 'call_1' },
    { id: 'evt_live_answer', type: 'agent.message', content: [{ type: 'text', text: 'answer' }] },
  ];
  await f.coordinator.prepare(f.checkpoint, resourceCheckpointDigest(f.checkpoint));
  f.append.mockImplementationOnce(async (_w, _s, batch) => {
    f.events.push(...batch);
    throw new Error('public append ACK lost');
  });
  await expect(f.coordinator.finish(f.event.id, f.binding.revision)).rejects.toThrow(
    'public append ACK lost',
  );
  const next = f.replace();
  await next.claim(f.binding);
  await next.finish(f.event.id, f.binding.revision, async (phase) => {
    if (phase === 'prepared')
      expect(f.events.map((event) => event.id)).toEqual(['evt_live_approval', 'evt_live_answer']);
  });
  expect(f.events.filter((event) => event.id === 'evt_live_approval')).toHaveLength(1);
  expect(f.events.map((event) => event.kind).slice(0, 3)).toEqual([
    'agent.requires_action',
    'agent.message',
    'agent.turn_completed',
  ]);
});

it('commits surviving reports after process loss but preserves incomplete and blocks the next model call', async () => {
  const f = await fixture();
  const evidence = {
    ...f.checkpoint.completion!,
    report_set_sealed: false,
    expected_reports: ['report_1', 'report_2'],
    reports: [f.checkpoint.completion!.reports[0]!],
  };
  await expect(f.coordinator.markLost(f.event.id, f.binding.revision, evidence)).rejects.toThrow(
    'operator recovery',
  );
  expect((await f.row()).phase).toBe('incomplete');
  expect((await f.session()).usageInputTokens).toBe(13);
  const next = f.replace();
  await next.claim(f.binding);
  await expect(next.markLost(f.event.id, f.binding.revision, evidence)).rejects.toThrow(
    'operator recovery',
  );
  expect((await f.session()).usageInputTokens).toBe(13);
  const changed = structuredClone(evidence);
  changed.reports[0]!.usage!.input_tokens++;
  await expect(next.markLost(f.event.id, f.binding.revision, changed)).rejects.toThrow(
    'reports changed',
  );
  await expect(next.begin({ ...f.event, id: newId('evt') }, f.binding)).rejects.toThrow('blocks');
  expect(f.events.some((event) => event.kind === 'agent.turn_completed')).toBe(false);
});

it('rejects retained accounting attributed to a different execution', async () => {
  const f = await fixture();
  await expect(
    f.coordinator.markLost(f.event.id, f.binding.revision, {
      ...f.checkpoint.completion!,
      execution_id: 'forged_execution',
    }),
  ).rejects.toThrow('ownership');
  expect((await f.session()).usageInputTokens).toBe(0);
  expect((await f.row()).phase).toBe('running');
});

it.each([false, true, 'missing', 'incomplete'] as const)(
  'reconciles a running-to-frozen reconnect with outputs=%s without replay',
  async (evidence) => {
    const output = evidence === true;
    const f = await fixture();
    const checkpoint = f.checkpoint;
    const bytes = Buffer.from('answer');
    if (output)
      checkpoint.files.push({
        resource_id: OUTPUT_RESOURCE_ID,
        path: 'answer.txt',
        size_bytes: bytes.length,
        sha256: createHash('sha256').update(bytes).digest('hex'),
        mode: 0o644,
      });
    const digest = resourceCheckpointDigest(checkpoint);
    const lost = vi.spyOn(f.coordinator, 'markLost');
    const files = new LocalFileStore({
      blobStore: new InMemoryBlobStore(),
      metadataStore: new InMemoryFileMetadataStore(),
    });
    let pendingReads = 0,
      completed = false;
    const acks: string[] = [];
    const delivery = new SessionResourcesDelivery({
      workspaceId,
      sessionId: f.sessionId,
      fileStore: files,
      store: f.store,
      isCurrent: async () => true,
      prepare: async () => ({
        manifest: { version: 1, revision: f.binding.revision, resources: [] },
        memoryStores: new Map(),
        networkAllowedDomains: [],
        close: async () => {},
        open: async () => {
          throw new Error('unused');
        },
      }),
      native: {
        coordinator: f.coordinator,
        probe: async () => ({
          ...f.binding,
          resolvedPaths: {},
          outputs: '/verified/outputs',
          ...(!completed
            ? {
                turn: {
                  turnId: f.event.id,
                  phase: 'frozen',
                  checkpointId: checkpoint.checkpoint_id,
                  digest,
                },
              }
            : {}),
        }),
      },
      request: async (path, raw) => {
        const body = raw as Record<string, unknown>;
        if (path === '/v1/runner/resources')
          return { committed: true, revision: f.binding.revision };
        if (path.endsWith('/ack')) {
          acks.push(body.completion_phase as string);
          if (body.completion_phase === 'prepared') {
            expect((await f.row()).phase).toBe('prepared');
            expect((await f.session()).usageInputTokens).toBe(30);
            expect((await files.list(workspaceId)).items).toHaveLength(output ? 1 : 0);
            expect(f.events.some((event) => event.kind === 'agent.turn_completed')).toBe(false);
            await expect(
              f.coordinator.begin({ ...f.event, id: newId('evt') }, f.binding),
            ).rejects.toThrow('blocks');
          } else {
            expect((await f.row()).phase).toBe('completed');
            completed = true;
          }
          return { ok: true };
        }
        if (body.type === 'pending') {
          if (pendingReads > 0 && evidence === 'incomplete')
            return {
              checkpoint: null,
              incomplete_completion: { ...checkpoint.completion!, report_set_sealed: false },
            };
          return ++pendingReads === 1 || completed || evidence === 'missing'
            ? { checkpoint: null }
            : { checkpoint, manifest_sha256: digest };
        }
        if (body.type === 'manifest') return { ...checkpoint, manifest_sha256: digest };
        return { offset: 0, content_base64: bytes.toString('base64') };
      },
    });
    try {
      if (typeof evidence === 'string') {
        await expect(delivery.deliver()).rejects.toThrow(
          evidence === 'missing' ? 'checkpoint is unavailable' : 'operator recovery',
        );
        expect(lost).toHaveBeenCalledTimes(evidence === 'incomplete' ? 1 : 0);
        expect((await f.row()).phase).toBe(evidence === 'incomplete' ? 'incomplete' : 'running');
        expect((await f.session()).usageInputTokens).toBe(evidence === 'incomplete' ? 30 : 0);
        expect(acks).toEqual([]);
        expect(f.events.some((event) => event.kind === 'agent.turn_completed')).toBe(false);
        await expect(
          f.coordinator.begin({ ...f.event, id: newId('evt') }, f.binding),
        ).rejects.toThrow('blocks');
        return;
      }
      await delivery.deliver();
      expect(lost).not.toHaveBeenCalled();
      expect((await f.row()).phase).toBe('completed');
      expect(acks).toEqual(['prepared', 'completed']);
      const ids = f.events.map((event) => event.id);
      await delivery.deliver();
      expect(f.events.map((event) => event.id)).toEqual(ids);
      expect(f.events.filter((event) => event.kind === 'agent.turn_completed')).toHaveLength(1);
      if (output) expect(f.events[0]!.kind).toBe('session.output_indexed');
      await f.coordinator.begin({ ...f.event, id: newId('evt') }, f.binding);
    } finally {
      await files.close();
    }
  },
);

it
  .skipIf(
    !['darwin', 'linux'].includes(process.platform) || process.versions.node.split('.')[0] !== '22',
  )
  .each(['gateway', 'sidecar'] as const)(
  'consumes an attached File with controlled Pi over a real tunnel (%s) and commits outputs and all usage before terminal',
  async (route) => {
    const f = await fixture(false);
    const workspaceDir = await mkdtemp(join(tmpdir(), 'orca-native-tunnel-'));
    const marker = 'private-attached-' + f.event.id;
    let inputPath = '',
      outputPath = '',
      calls = 0;
    const modelErrors: unknown[] = [];
    const server = createServer(async (req, res) => {
      try {
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(Buffer.from(chunk));
        const body = JSON.parse(Buffer.concat(chunks).toString());
        if (route === 'gateway') {
          expect(req.headers.authorization).toBe(
            calls < 3 ? 'Bearer fixture-scoped' : 'Bearer refreshed-scoped',
          );
          expect(req.headers['x-api-key']).toBeUndefined();
        } else {
          expect(req.headers['x-api-key']).toBe('fixture-direct-key');
          expect(req.headers.authorization).toBeUndefined();
        }
        expect(body.tools.map((tool: { name: string }) => tool.name).sort()).toEqual([
          'read',
          'write',
        ]);
        expect(JSON.stringify(body.system)).toContain(inputPath);
        expect(JSON.stringify(body.system)).toContain(outputPath);
        calls++;
        if (calls >= 2) expect(JSON.stringify(body.messages)).toContain(marker);
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        const emit = (value: Record<string, unknown>) =>
          res.write('event: ' + value.type + '\ndata: ' + JSON.stringify(value) + '\n\n');
        emit({
          type: 'message_start',
          message: {
            id: 'msg_' + calls,
            type: 'message',
            role: 'assistant',
            model: 'actual-fixture-model',
            content: [],
            stop_reason: null,
            stop_sequence: null,
            usage: { input_tokens: 13, output_tokens: 0 },
          },
        });
        if (calls < 3) {
          emit({
            type: 'content_block_start',
            index: 0,
            content_block: {
              type: 'tool_use',
              id: 'call_' + calls,
              name: calls === 1 ? 'read' : 'write',
              input: {},
            },
          });
          emit({
            type: 'content_block_delta',
            index: 0,
            delta: {
              type: 'input_json_delta',
              partial_json: JSON.stringify(
                calls === 1
                  ? { path: inputPath }
                  : { path: outputPath + '/report.txt', content: marker },
              ),
            },
          });
        } else {
          emit({
            type: 'content_block_start',
            index: 0,
            content_block: { type: 'text', text: '' },
          });
          emit({
            type: 'content_block_delta',
            index: 0,
            delta: { type: 'text_delta', text: 'Saved ' + outputPath + '/report.txt' },
          });
        }
        emit({ type: 'content_block_stop', index: 0 });
        emit({
          type: 'message_delta',
          delta: { stop_reason: calls < 3 ? 'tool_use' : 'end_turn', stop_sequence: null },
          usage: { output_tokens: 7 },
        });
        emit({ type: 'message_stop' });
        res.end();
      } catch (error) {
        modelErrors.push(error);
        res.writeHead(500).end();
      }
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const providers = registerPiProvider(new ProviderRegistry(), {
      modelDefault: 'claude-sonnet-4-20250514',
      fallbackApiKey: 'fixture-direct-key',
    });
    expect(providers.sessionSandboxReadPaths().length).toBeGreaterThan(0);
    const runtime = new LocalSandboxRuntime({
      harnessWorkDir: workspaceDir,
      allowedNetworkHosts: ['127.0.0.1'],
      manager: SandboxManager,
      extraReadPaths: [process.execPath],
    });
    let launches = 0;
    const acquire = LocalSandboxRuntime.prototype.acquire;
    const acquireSpy = vi
      .spyOn(LocalSandboxRuntime.prototype, 'acquire')
      .mockImplementation(async function (this: LocalSandboxRuntime, env) {
        const sandbox = await acquire.call(this, env);
        const spawn = sandbox.spawn!.bind(sandbox);
        sandbox.spawn = async (command, options) => {
          if (options?.env?.PI_CODING_AGENT_DIR) {
            launches++;
            if (route === 'sidecar') {
              // Verify production config, then substitute only the fixture's HTTP endpoint before boot.
              const path = join(options.env.PI_CODING_AGENT_DIR, 'models.json');
              const config = JSON.parse(await readFile(path, 'utf8'));
              expect(config.providers.orca).toMatchObject({
                baseUrl: 'https://api.anthropic.com',
                apiKey: 'fixture-direct-key',
                authHeader: false,
                models: [{ id: 'claude-sonnet-4-20250514' }],
              });
              config.providers.orca.baseUrl =
                'http://127.0.0.1:' + (server.address() as { port: number }).port;
              await writeFile(path, JSON.stringify(config));
            }
          }
          return spawn(command, options);
        };
        return sandbox;
      });
    const loop = new SessionLoop({
      workspaceId,
      providers,
      sandboxRuntime: runtime,
      skillsWorkspaceDir: workspaceDir,
    });
    const token = 'native-tunnel-fixture',
      runnerId = tokenBoundRunnerId(token);
    const peer = new FakeRegistryRunnerTunnel({ runnerId, bindingToken: token });
    await peer.listen();
    const tunnel = serveTunnel({
      dispatcher: registerSessionHandlers(new RouteDispatcher(), loop, { logger: console }),
      registryRunnerUrl: peer.baseUrl(),
      runnerId,
      bindingToken: token,
      runnerVersion: 'fixture',
      providers: ['pi'],
    });
    const files = new LocalFileStore({
      blobStore: new InMemoryBlobStore(),
      metadataStore: new InMemoryFileMetadataStore(),
    });
    try {
      const runner = await peer.nextRunner();
      let hideNextPending = false;
      const request = async (path: string, body: unknown) => {
        if (
          hideNextPending &&
          path === '/v1/runner/resource-changes' &&
          (body as { type: string }).type === 'pending'
        ) {
          hideNextPending = false;
          return { checkpoint: null };
        }
        const response = await runner.request({
          method: 'POST',
          path,
          headers: [['X-Orca-Session-Id', f.sessionId]],
          body: JSON.stringify(body),
        });
        expect(response.status, response.body).toBe(200);
        return JSON.parse(response.body);
      };
      const bytes = Buffer.from(marker);
      expect(await request('/v1/runner/resources', { type: 'status' })).toMatchObject({
        supported_placements: ['session_sandbox'],
      });
      const delivery = new SessionResourcesDelivery({
        workspaceId,
        sessionId: f.sessionId,
        fileStore: files,
        store: f.store,
        isCurrent: async () => true,
        request,
        prepare: async () => ({
          manifest: {
            version: 1,
            revision: f.binding.revision,
            resources: [
              {
                resource_id: 'sesrsc_fixture',
                kind: 'file',
                mount_path: '/mnt/context.txt',
                access: 'read_only',
                files: [
                  {
                    path: '',
                    size_bytes: bytes.length,
                    sha256: createHash('sha256').update(bytes).digest('hex'),
                    mode: 0o444,
                  },
                ],
              },
            ],
          },
          memoryStores: new Map(),
          fileIds: new Map([['sesrsc_fixture', 'file_fixture']]),
          networkAllowedDomains: [],
          open: async () => Readable.from([bytes]),
          close: async () => {},
        }),
        native: {
          coordinator: f.coordinator,
          probe: async () => {
            const status = await request('/v1/runner/resources', { type: 'status' });
            return {
              executionId: status.execution_id,
              placementId: status.placement_id,
              revision: status.revision,
              resolvedPaths: status.resolved_paths,
              outputs: status.outputs_directory,
              ...(status.turn
                ? {
                    turn: {
                      turnId: status.turn.turn_id,
                      phase: status.turn.phase,
                      checkpointId: status.turn.checkpoint_id,
                      digest: status.turn.manifest_sha256,
                    },
                  }
                : {}),
            };
          },
        },
      });
      await delivery.deliver();
      const status = await request('/v1/runner/resources', { type: 'status' });
      inputPath = status.resolved_paths.sesrsc_fixture;
      outputPath = status.outputs_directory;
      expect(inputPath).not.toBe('/mnt/context.txt');
      const snapshot = {
        provider: 'pi',
        model: { provider: 'anthropic', id: 'claude-sonnet-4-20250514' },
        system: 'Read the attached File then write report.txt.',
        allowed_tool_names: ['read', 'write'],
        allowed_mcp_server_names: [],
        tool_permissions: {
          'mcp__orca__*': 'always_deny',
          mcp__orca__read: 'always_allow',
          mcp__orca__write: 'always_allow',
        },
        default_tool_permission: 'always_ask',
        egress:
          route === 'sidecar'
            ? { mode: 'sidecar' }
            : {
                mode: 'gateway',
                gateway: {
                  llm_base_url: 'http://127.0.0.1:' + (server.address() as { port: number }).port,
                  llm_jwt: 'fixture-scoped',
                },
              },
        ...delivery.snapshotResources(),
      };
      await request('/v1/runner/snapshot', snapshot);
      const configPath = join(inputPath, '../../.pi-agent-home/models.json');
      await delivery.beginTurn(f.event);
      await delivery.publishMounted();
      expect(
        JSON.parse(
          f.events.find((event) => event.kind === 'session.resource_mounted')!.payload.toString(),
        ).resolved_path,
      ).toBe(inputPath);
      const response = await runner.request({
        method: 'POST',
        path: '/v1/runner/turn',
        headers: [['X-Orca-Session-Id', f.sessionId]],
        body: JSON.stringify({
          type: 'user.message',
          native_turn_id: f.event.id,
          content: [
            { type: 'text', text: 'Read context, save its contents as report.txt, then confirm.' },
          ],
        }),
      });
      expect(response.status, response.body).toBe(200);
      expect(modelErrors).toEqual([]);
      expect(calls, response.body).toBe(3);
      const events = response.body
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line));
      expect(response.body).not.toContain(outputPath);
      expect(events.map((event) => event.type)).not.toContain('agent.turn_completed');
      expect(events.map((event) => event.type)).not.toContain('agent.usage');
      const completion = events.find((event) => event.type === 'orca.native_completion');
      expect(completion, response.body).toBeDefined();
      expect((await files.list(workspaceId, { scopeId: f.sessionId })).items).toEqual([]);
      await delivery.commit(completion.checkpoint_id, completion.manifest_sha256);
      const outputs = (await files.list(workspaceId, { scopeId: f.sessionId })).items;
      expect(outputs).toHaveLength(1);
      expect(outputs[0]!.filename).toBe('report.txt');
      const opened = await files.open(workspaceId, outputs[0]!.id);
      const content: Buffer[] = [];
      for await (const chunk of opened!.stream) content.push(Buffer.from(chunk));
      expect(Buffer.concat(content).toString()).toBe(marker);
      expect((await f.session()).usageInputTokens).toBe(39);
      expect((await f.session()).usageOutputTokens).toBe(21);
      expect((await f.row()).phase).toBe('completed');
      expect(f.events.findIndex((event) => event.kind === 'session.output_indexed')).toBeLessThan(
        f.events.findIndex((event) => event.kind === 'agent.turn_completed'),
      );
      await delivery.commit(completion.checkpoint_id, completion.manifest_sha256);
      expect((await f.session()).usageInputTokens).toBe(39);
      expect(f.events.filter((event) => event.kind === 'agent.turn_completed')).toHaveLength(1);
      const nextEvent = { ...f.event, id: newId('evt') };
      const nextSnapshot =
        route === 'sidecar'
          ? snapshot
          : {
              ...snapshot,
              egress: {
                mode: 'gateway',
                gateway: {
                  llm_base_url: 'http://127.0.0.1:' + (server.address() as { port: number }).port,
                  llm_jwt: 'refreshed-scoped',
                },
              },
            };
      await request('/v1/runner/snapshot', nextSnapshot);
      if (route === 'sidecar')
        expect(JSON.parse(await readFile(configPath, 'utf8')).providers.orca).toMatchObject({
          apiKey: 'fixture-direct-key',
          authHeader: false,
        });
      expect(launches).toBe(1);
      expect(await request('/v1/runner/resources', { type: 'status' })).toMatchObject({
        execution_id: status.execution_id,
        placement_id: status.placement_id,
      });
      await delivery.beginTurn(nextEvent);
      const firstTurnCalls = calls;
      const second = await runner.request({
        method: 'POST',
        path: '/v1/runner/turn',
        headers: [['X-Orca-Session-Id', f.sessionId]],
        body: JSON.stringify({
          type: 'user.message',
          native_turn_id: nextEvent.id,
          content: [{ type: 'text', text: 'Confirm again.' }],
        }),
      });
      expect(second.status, second.body).toBe(200);
      const secondCompletion = second.body
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
        .find((event) => event.type === 'orca.native_completion');
      expect(secondCompletion, second.body).toBeDefined();
      const markLost = vi.spyOn(f.coordinator, 'markLost');
      // Reproduce the non-atomic pending/status crossing on the single-call, no-output turn.
      hideNextPending = true;
      await delivery.deliver();
      expect(hideNextPending).toBe(false);
      expect(markLost).not.toHaveBeenCalled();
      await delivery.deliver();
      expect(calls - firstTurnCalls).toBe(1);
      expect(calls).toBe(4);
      expect(modelErrors).toEqual([]);
      expect((await f.session()).usageInputTokens).toBe(52);
      expect(f.events.filter((event) => event.kind === 'agent.turn_completed')).toHaveLength(2);
    } finally {
      acquireSpy.mockRestore();
      await tunnel.stop();
      await loop.stop();
      await peer.close();
      await files.close();
      await SandboxManager.reset();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(workspaceDir, { recursive: true, force: true });
    }
  },
  60000,
);
