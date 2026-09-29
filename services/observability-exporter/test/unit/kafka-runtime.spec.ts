// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Event } from '@orca/transcript-store-types';
import { createKafkaTranscriptCodec, type KafkaTranscriptCodec } from '@orca/transcript-store';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  KafkaJSProtocolError,
  type ConsumerRunConfig,
  type EachBatchPayload,
  type Kafka,
  type RecordBatchEntry,
  type Transaction,
  type Producer,
} from 'kafkajs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { internalContract } from '../../../registry-service-ts/src/contracts/internal.contract.js';
import type { KafkaDiskIndex } from '../../src/kafka-disk-index.js';
import {
  migrateKafkaCheckpoint,
  parseKafkaStateHead,
  kafkaStateKeys,
} from '../../src/kafka-state-v2.js';
import { deliverKafkaTrace } from '../../src/kafka-delivery.js';
import {
  KafkaOnlyObservabilityExporterRuntime,
  kafkaDeliveryProgressKey,
  kafkaSourceGroupId,
  kafkaSourceTransactionId,
  withKafkaDeliveryHeartbeat,
  type KafkaOnlyExporterRuntimeOptions,
} from '../../src/kafka-runtime.js';
import {
  initialKafkaCheckpoint,
  projectKafkaEvents,
  type KafkaCheckpoint,
  type KafkaSessionRoute,
} from '../../src/kafka-state.js';
import { RegistryObservabilityClient } from '../../src/registry-client.js';
import { KafkaSharedState } from '../../src/kafka-shared-state.js';
import { encodeKafkaDelivery } from '../../src/kafka-state-io.js';
import type { PinnedDeliveryContext } from '../../src/types.js';
import { completedPrimaryTurnEvents, SESSION_ID, WORKSPACE_ID } from '../support/events.js';
import { enabledRegistryContext } from '../support/registry.js';

vi.mock('../../src/kafka-shared-state.js', () => ({
  KafkaSharedState: vi.fn().mockImplementation(({ kafka }) => kafka.sharedState),
}));
// Historical fixture inputs only: runtime restoration executes the real v2 readers against the index below.
const restoreKafkaCheckpoint =
  vi.fn<
    (input: {
      producer: Producer;
      key: string;
      topic: string;
      route: KafkaSessionRoute;
      signal: AbortSignal;
    }) => Promise<KafkaCheckpoint | null>
  >();
const restoreKafkaValue =
  vi.fn<
    (input: {
      producer: Producer;
      key: string;
      topic: string;
      signal: AbortSignal;
    }) => Promise<{ nextOffset: string } | null>
  >();
vi.mock('../../src/kafka-delivery.js', () => ({ deliverKafkaTrace: vi.fn() }));
vi.mock('@orca/transcript-store', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@orca/transcript-store')>();
  return {
    ...actual,
    createKafkaTranscriptCodec: vi.fn(() => {
      const codec = actual.createKafkaTranscriptCodec();
      vi.spyOn(codec, 'prepareWriter');
      vi.spyOn(codec, 'close');
      return codec;
    }),
  };
});

const route = {
  topic: `orca.${WORKSPACE_ID}.sessions.${SESSION_ID}.events`,
  workspaceId: WORKSPACE_ID,
  sessionId: SESSION_ID,
};
const namespace = 'exporter-test';
const checkpointTopic = 'exporter.checkpoints';
const deliveryTopic = 'exporter.deliveries';
const context: PinnedDeliveryContext = {
  organizationId: 'org_test',
  bindingId: 'aob_test',
  bindingVersion: 1,
  adapterType: 'otlp_http',
  endpointKind: 'traces_endpoint',
  endpointClass: 'public',
  endpointUrl: 'https://collector.example/api/public/otel/v1/traces',
  semanticProfile: 'langfuse',
  protocol: 'http/json',
  compression: 'none',
  timeoutMs: 1000,
  captureMode: 'metadata_only',
  sampleRate: 1,
  configSchemaVersion: 1,
};

function wire(source: Event): RecordBatchEntry {
  return {
    key: Buffer.from(source.id),
    value: Buffer.from(source.payload),
    offset: String(source.seq),
    timestamp: '0',
    attributes: 0,
    headers: {
      id: source.id,
      workspace_id: source.workspaceId,
      session_id: source.sessionId,
      subpath: source.subpath,
      produced_at: source.producedAt,
      produced_by: source.producedBy,
      kind: source.kind,
      idempotency_key: source.idempotencyKey,
      ...(source.userId === undefined ? {} : { user_id: source.userId }),
    },
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function batchPayload(messages = completedPrimaryTurnEvents().map(wire), topic = route.topic) {
  return {
    batch: {
      topic,
      partition: 0,
      highWatermark: '100',
      messages,
      isEmpty: () => messages.length === 0,
      firstOffset: () => messages[0]?.offset ?? null,
      lastOffset: () => messages.at(-1)?.offset ?? '0',
      offsetLag: () => '0',
      offsetLagLow: () => '0',
    },
    heartbeat: vi.fn(async () => {}),
    isRunning: vi.fn(() => true),
    isStale: vi.fn(() => false),
    resolveOffset: vi.fn<(offset: string) => void>(),
    pause: vi.fn(() => vi.fn()),
    commitOffsetsIfNecessary: vi.fn(async () => {}),
    uncommittedOffsets: vi.fn(() => ({ topics: [] })),
  } satisfies EachBatchPayload;
}

function deliveryBatch(offsets = ['0']) {
  const record = projectKafkaEvents(
    initialKafkaCheckpoint(route, context),
    completedPrimaryTurnEvents().map((event) => ({ offset: String(event.seq), event })),
  ).deliveries[0]!;
  return batchPayload(
    offsets.map((offset) => ({
      ...wire(completedPrimaryTurnEvents()[0]!),
      offset,
      value: Buffer.from(JSON.stringify(record)),
    })),
    deliveryTopic,
  );
}

function mockConsumer() {
  const listeners = new Map<string, (event: { payload: { error: Error } }) => void>();
  let config: ConsumerRunConfig | undefined;
  return {
    events: { CRASH: 'crash', GROUP_JOIN: 'group_join' },
    on: vi.fn((event: string, listener: (event: { payload: { error: Error } }) => void) => {
      listeners.set(event, listener);
    }),
    connect: vi.fn(async () => {}),
    disconnect: vi.fn(async () => {}),
    subscribe: vi.fn<(input: { topic: string; fromBeginning: boolean }) => Promise<void>>(
      async () => {},
    ),
    commitOffsets: vi.fn(async () => {}),
    run: vi.fn(async (value: ConsumerRunConfig) => {
      config = value;
    }),
    join: () => listeners.get('group_join')?.({ payload: { error: new Error('join') } }),
    crash: (error: Error) => listeners.get('crash')?.({ payload: { error } }),
    async process(batch: EachBatchPayload) {
      if (!config?.eachBatch) throw new Error('consumer not started');
      await config.eachBatch(batch);
    },
  };
}

const runtimes: KafkaOnlyObservabilityExporterRuntime[] = [];
function setup(
  otlpFetchImpl?: typeof fetch,
  codec?: KafkaTranscriptCodec,
  sessionRoute = route,
  options: Partial<KafkaOnlyExporterRuntimeOptions> = {},
) {
  const suffix = codec?.encoding === 'avro' ? '-avro' : '';
  const checkpointTopic = 'exporter.checkpoints' + suffix;
  const deliveryTopic = 'exporter.deliveries' + suffix;
  const order: string[] = [];
  const rows = new Map<string, string>();
  let nextStateOffset = 0n;
  let appliedOffset = -1n;
  const pendingOffsets = new Map<bigint, () => void>();
  let applyImmediately = true;
  const committedRows: Array<{ key: string; value: string; offset: bigint }> = [];
  const applyCommitted = (): void => {
    for (const record of committedRows.splice(0)) {
      rows.set(record.key, record.value);
      if (record.offset > appliedOffset) appliedOffset = record.offset;
    }
    for (const [offset, resolve] of pendingOffsets) {
      if (offset <= appliedOffset) {
        pendingOffsets.delete(offset);
        resolve();
      }
    }
  };
  const index = {
    read: vi.fn(async (keys: readonly string[], expected?: { key: string; value: string }) => {
      if (expected !== undefined && rows.get(expected.key) !== expected.value)
        throw new Error('Kafka index expected head mismatch');
      return keys.map((key) => rows.get(key) ?? null);
    }),
    countPrefix: vi.fn(
      async (prefix: string) => [...rows.keys()].filter((key) => key.startsWith(prefix)).length,
    ),
  } as unknown as KafkaDiskIndex;
  const sharedState = {
    diagnostics: vi.fn(async () => ({
      scannedBytes: 42,
      scannedRecords: 2,
      waiters: 0,
      databaseBytes: 4096,
      databaseLimitBytes: 8192,
      diskQuotaBytes: 131072,
    })),
    start: vi.fn(async () => {}),
    close: vi.fn(async () => {}),
    read: vi.fn(async <T>(work: (index: KafkaDiskIndex) => Promise<T>) => work(index)),
    waitForOffset: vi.fn(async (offset: string, signal: AbortSignal) => {
      signal.throwIfAborted();
      if (BigInt(offset) <= appliedOffset) return;
      await new Promise<void>((resolve, reject) => {
        const abort = () => {
          pendingOffsets.delete(BigInt(offset));
          reject(signal.reason);
        };
        signal.addEventListener('abort', abort, { once: true });
        pendingOffsets.set(BigInt(offset), () => {
          signal.removeEventListener('abort', abort);
          resolve();
        });
      });
    }),
    barrier: vi.fn(
      async <T>(
        producer: Producer,
        key: string,
        signal: AbortSignal,
        snapshot: (index: KafkaDiskIndex) => Promise<T>,
      ) => {
        signal.throwIfAborted();
        if (key.startsWith('delivery:')) {
          const progress = await restoreKafkaValue({
            producer,
            key,
            topic: checkpointTopic,
            signal,
          });
          signal.throwIfAborted();
          if (!rows.has(key) && progress !== null)
            rows.set(
              key,
              JSON.stringify({
                version: 1,
                topic: deliveryTopic,
                partition: Number(key.split(':').at(-2)),
                groupId: namespace + suffix + '-delivery-v1',
                ...progress,
              }),
            );
        } else {
          const consumerIndex = kafka.consumer.mock.calls.findIndex(
            ([input]) => input.groupId === key,
          );
          const consumer = kafka.consumer.mock.results[consumerIndex]!.value as ReturnType<
            typeof mockConsumer
          >;
          const topic = consumer.subscribe.mock.calls[0]![0].topic;
          const match = /orca[.]([^.]+)[.]sessions[.]([^.]+)[.]events(?:-avro)?$/u.exec(topic)!;
          const restoredRoute = { topic, workspaceId: match[1]!, sessionId: match[2]! };
          const checkpoint = await restoreKafkaCheckpoint({
            producer,
            key,
            topic: checkpointTopic,
            route: restoredRoute,
            signal,
          });
          signal.throwIfAborted();
          if (!rows.has(key) && checkpoint !== null) {
            const seeded = migrateKafkaCheckpoint(JSON.stringify(checkpoint), restoredRoute, key);
            for (const record of seeded.records) rows.set(record.key, record.value);
          }
        }
        return snapshot(index);
      },
    ),
  };
  const transaction = {
    sendBatch: vi.fn<Transaction['sendBatch']>(async () => []),
    send: vi.fn<Transaction['send']>(async ({ topic, messages }) => {
      order.push(`send:${topic}`);
      const baseOffset = nextStateOffset.toString();
      if (topic === checkpointTopic) nextStateOffset += BigInt(messages.length);
      return [{ topicName: topic, partition: 0, errorCode: 0, baseOffset }];
    }),
    sendOffsets: vi.fn<Transaction['sendOffsets']>(async () => {
      order.push('sendOffsets');
    }),
    commit: vi.fn(async () => {
      order.push('commit');
    }),
    abort: vi.fn(async () => {
      order.push('abort');
    }),
    isActive: () => true,
  } satisfies Transaction;
  const producers: Array<ReturnType<typeof makeProducer>> = [];
  function makeProducer() {
    const producer = {
      connect: vi.fn(async () => {}),
      disconnect: vi.fn(async () => {}),
      transaction: vi.fn(async () => {
        order.push('transaction');
        const staged: Array<{ key: string; value: string; offset: bigint }> = [];
        return {
          ...transaction,
          send: async (input: Parameters<Transaction['send']>[0]) => {
            const result = await transaction.send(input);
            if (input.topic === checkpointTopic && result[0]?.baseOffset !== undefined) {
              input.messages.forEach((message, i) =>
                staged.push({
                  key: String(message.key),
                  value: String(message.value),
                  offset: BigInt(result[0]!.baseOffset!) + BigInt(i),
                }),
              );
            }
            return result;
          },
          commit: async () => {
            await transaction.commit();
            committedRows.push(...staged);
            if (applyImmediately) applyCommitted();
          },
          abort: async () => {
            staged.length = 0;
            await transaction.abort();
          },
        };
      }),
    };
    return producer;
  }
  const delivery = mockConsumer();
  const source = mockConsumer();
  const admin = {
    connect: vi.fn(async () => {}),
    disconnect: vi.fn(async () => {}),
    fetchTopicMetadata: vi.fn(async ({ topics }: { topics: string[] }) => ({
      topics: topics.map((name) => ({
        name,
        partitions: [{ partitionId: 0 }],
      })),
    })),
    describeConfigs: vi.fn(async () => ({
      resources: [checkpointTopic, deliveryTopic].map((resourceName) => ({
        resourceName,
        configEntries: [
          {
            configName: 'cleanup.policy',
            configValue: resourceName === checkpointTopic ? 'compact' : 'delete',
          },
          { configName: 'retention.ms', configValue: '-1' },
          { configName: 'retention.bytes', configValue: '-1' },
        ],
      })),
    })),
    fetchTopicOffsets: vi.fn(async () => [{ partition: 0, low: '0', high: '100' }]),
    fetchOffsets: vi.fn(async ({ topics }: { topics: string[] }) =>
      topics.map((topic) => ({ topic, partitions: [{ partition: 0, offset: '-1' }] })),
    ),
  };
  const kafka = {
    sharedState,
    admin: vi.fn(() => admin),
    consumer: vi.fn().mockReturnValueOnce(delivery).mockReturnValueOnce(source),
    producer: vi.fn(() => {
      const producer = makeProducer();
      producers.push(producer);
      return producer;
    }),
  };
  const registryClient = { resolveContext: vi.fn(), resolveSecret: vi.fn() };
  const runtime = new KafkaOnlyObservabilityExporterRuntime({
    kafka: kafka as unknown as Kafka,
    groupId: namespace + suffix,
    sessions: [sessionRoute],
    checkpointTopic,
    deliveryTopic,
    registryClient,
    ...(codec === undefined ? {} : { codec }),
    ...(otlpFetchImpl === undefined ? {} : { otlpFetchImpl }),
    ...options,
  });
  runtimes.push(runtime);
  return {
    runtime,
    kafka,
    source,
    delivery,
    transaction,
    producers,
    order,
    registryClient,
    admin,
    rows,
    index,
    sharedState,
    holdIndex: () => {
      applyImmediately = false;
    },
    applyCommitted,
    head: (key = kafkaSourceGroupId(namespace + suffix, sessionRoute.topic)) =>
      parseKafkaStateHead(rows.get(key)!, sessionRoute),
  };
}

function expectV2Head(
  head: ReturnType<typeof parseKafkaStateHead>,
  expected: KafkaCheckpoint,
): void {
  expect(head).toMatchObject({
    version: 2,
    route: expected.route,
    nextOffset: expected.nextOffset,
    identityCount: expected.identities.length,
    acceptedCount: expected.acceptedSourceIds.length,
    deliveryContext: expected.deliveryContext,
    reducer: { kind: 'inline', value: expected.reducer },
  });
  expect(head).not.toHaveProperty('identities');
  expect(head).not.toHaveProperty('acceptedSourceIds');
}

function expectV2State(s: ReturnType<typeof setup>, expected: KafkaCheckpoint): void {
  const key = kafkaSourceGroupId(namespace, expected.route.topic);
  expectV2Head(parseKafkaStateHead(s.rows.get(key)!, expected.route), expected);
  const keys = kafkaStateKeys(key);
  expect(
    [...s.rows.keys()].filter((rowKey) => rowKey.startsWith(keys.identityPrefix)),
  ).toHaveLength(expected.identities.length);
  expect(
    [...s.rows.keys()].filter((rowKey) => rowKey.startsWith(keys.acceptedPrefix)),
  ).toHaveLength(expected.acceptedSourceIds.length);
  for (const identity of expected.identities) {
    expect(JSON.parse(s.rows.get(keys.identity(identity.key))!)).toMatchObject(identity);
  }
  for (const id of expected.acceptedSourceIds) {
    expect(JSON.parse(s.rows.get(keys.accepted(id))!)).toMatchObject({ id });
  }
}

beforeEach(() => {
  vi.mocked(restoreKafkaValue).mockReset().mockResolvedValue(null);
  vi.mocked(restoreKafkaCheckpoint)
    .mockReset()
    .mockImplementation(async ({ route: restoredRoute }) =>
      initialKafkaCheckpoint(restoredRoute, context),
    );
  vi.mocked(deliverKafkaTrace)
    .mockReset()
    .mockResolvedValue({ kind: 'terminal', reason: 'accepted' });
});

describe('raw capture batch authority', () => {
  const raw = { ...context, captureMode: 'raw_io' };
  function rawSetup(options: Partial<KafkaOnlyExporterRuntimeOptions> = {}) {
    vi.mocked(restoreKafkaCheckpoint).mockResolvedValue(initialKafkaCheckpoint(route, raw));
    const s = setup(undefined, undefined, route, options);
    s.registryClient.resolveContext.mockResolvedValue({
      status: 'enabled',
      deliveryContext: raw,
    });
    return s;
  }

  it('resolves once per projection chunk without changing the pin', async () => {
    const s = rawSetup({ batchSize: 3 });
    await s.runtime.start();
    await s.source.process(batchPayload());
    expect(s.registryClient.resolveContext).toHaveBeenCalledTimes(3);
    expect(s.head().deliveryContext).toEqual(raw);
    const sends = s.transaction.send.mock.calls.filter(([entry]) => entry.topic === deliveryTopic);
    expect(JSON.stringify(sends)).toContain('orca.observability.projected-trace.v2');
    expect(JSON.stringify(sends)).toContain('content-that-must-not-be-exported');
  });

  it('allows a fresh Kafka raw pin, with separate initialization and batch authorization', async () => {
    const s = rawSetup();
    vi.mocked(restoreKafkaCheckpoint).mockResolvedValue(null);
    await s.runtime.start();
    await s.source.process(batchPayload());
    expect(s.registryClient.resolveContext).toHaveBeenCalledTimes(2);
    expect(s.head().deliveryContext).toEqual(raw);
    const sends = s.transaction.send.mock.calls.filter(([entry]) => entry.topic === deliveryTopic);
    expect(JSON.stringify(sends)).toContain('orca.observability.projected-trace.v2');
  });

  it('never upgrades restored metadata pins to content collection', async () => {
    const s = setup();
    s.registryClient.resolveContext.mockResolvedValue({
      status: 'enabled',
      deliveryContext: raw,
    });
    await s.runtime.start();
    await s.source.process(batchPayload());
    expect(s.registryClient.resolveContext).not.toHaveBeenCalled();
    expect(s.head().deliveryContext).toEqual(context);
    const sends = s.transaction.send.mock.calls.filter(([entry]) => entry.topic === deliveryTopic);
    expect(JSON.stringify(sends)).not.toContain('orca.observability.projected-trace.v2');
  });

  it.each([1, 5])(
    'scrubs pending/active content at split %s and never re-expands',
    async (split) => {
      const s = rawSetup();
      await s.runtime.start();
      const messages = completedPrimaryTurnEvents().map(wire);
      await s.source.process(batchPayload(messages.slice(0, split)));
      expect(JSON.stringify(s.head().reducer)).toContain('content-that-must-not-be-exported');
      s.registryClient.resolveContext.mockResolvedValueOnce({
        status: 'enabled',
        deliveryContext: context,
      });
      await s.source.process(batchPayload(messages.slice(split, split + 1)));
      expect(JSON.stringify(s.head().reducer)).not.toContain('content-that-must-not-be-exported');
      expect(s.head().deliveryContext).toEqual(raw);
      await s.source.process(batchPayload(messages.slice(split + 1)));
      const sends = s.transaction.send.mock.calls.filter(
        ([entry]) => entry.topic === deliveryTopic,
      );
      expect(JSON.stringify(sends)).not.toContain('content-that-must-not-be-exported');
      expect(JSON.stringify(sends)).not.toContain('orca.observability.projected-trace.v2');
    },
  );

  it.each([
    { bindingId: 'aob_other' },
    { bindingVersion: 2 },
    { sampleRate: 0.5 },
    { endpointUrl: 'https://other.example/api/public/otel/v1/traces' },
  ])('does not capture or repin after a fresh immutable-context mismatch: %j', async (change) => {
    const s = rawSetup();
    s.registryClient.resolveContext.mockResolvedValue({
      status: 'enabled',
      deliveryContext: { ...raw, ...change },
    });
    await s.runtime.start();
    await s.source.process(batchPayload());
    expect(s.head().deliveryContext).toEqual(raw);
    const sends = s.transaction.send.mock.calls.filter(([entry]) => entry.topic === deliveryTopic);
    expect(JSON.stringify(sends)).not.toContain('orca.observability.projected-trace.v2');
  });

  it('does not collect content from cached authority when Registry is unavailable', async () => {
    const s = rawSetup();
    s.registryClient.resolveContext.mockRejectedValue(new Error('unavailable'));
    await s.runtime.start();
    await expect(s.source.process(batchPayload())).rejects.toThrow('unavailable');
    expect(s.head().nextOffset).toBe('0');
    expect(
      s.transaction.send.mock.calls.filter(([entry]) => entry.topic === deliveryTopic),
    ).toHaveLength(0);
  });

  it('bounds continuous Registry expirations and resumes only with fresh authority on redelivery', async () => {
    const s = rawSetup({ captureBatchTimeoutMs: 100, batchSize: 1 });
    await s.runtime.start();
    let time = 0;
    const clock = vi.spyOn(performance, 'now').mockImplementation(() => time);
    s.registryClient.resolveContext
      .mockResolvedValueOnce({ status: 'enabled', deliveryContext: raw })
      .mockImplementation(async () => {
        time += 200;
        return { status: 'enabled', deliveryContext: raw };
      });
    try {
      const first = batchPayload();
      await s.source.process(first);
      expect(s.registryClient.resolveContext).toHaveBeenCalledTimes(3);
      expect(first.resolveOffset.mock.calls).toEqual([[first.batch.messages[0]!.offset]]);
      const nextOffset = String(BigInt(first.batch.messages[0]!.offset) + 1n);
      expect(s.head().nextOffset).toBe(nextOffset);
      expect(
        s.transaction.send.mock.calls.filter(([entry]) => entry.topic === deliveryTopic),
      ).toHaveLength(0);
      const unresolved = first.batch.messages.filter(
        (message) => BigInt(message.offset) >= BigInt(nextOffset),
      );
      s.registryClient.resolveContext.mockResolvedValue({
        status: 'enabled',
        deliveryContext: raw,
      });
      const redelivery = batchPayload(unresolved);
      await s.source.process(redelivery);
      expect(s.registryClient.resolveContext).toHaveBeenCalledTimes(3 + unresolved.length);
      expect(redelivery.resolveOffset.mock.calls).toEqual(
        unresolved.map((message) => [message.offset]),
      );
      expect(s.head().nextOffset).toBe(String(BigInt(unresolved.at(-1)!.offset) + 1n));
      expect(
        s.transaction.send.mock.calls.filter(([entry]) => entry.topic === deliveryTopic),
      ).toHaveLength(1);
    } finally {
      clock.mockRestore();
    }
  });

  it('bounds continuous projection expirations, shrinking without committing either candidate', async () => {
    const s = rawSetup({ captureBatchTimeoutMs: 100 });
    await s.runtime.start();
    await s.source.process(batchPayload([]));
    s.transaction.send.mockClear();
    const original = s.sharedState.read.getMockImplementation()!;
    let time = 0;
    const candidateOffsets: string[] = [];
    const clock = vi.spyOn(performance, 'now').mockImplementation(() => time);
    s.sharedState.read.mockImplementation(async (work) => {
      const result = await original(work);
      candidateOffsets.push((result as { head: { nextOffset: string } }).head.nextOffset);
      time += 101;
      return result;
    });
    try {
      const batch = batchPayload();
      await s.source.process(batch);
      expect(s.registryClient.resolveContext).toHaveBeenCalledTimes(2);
      expect(candidateOffsets).toHaveLength(2);
      expect(BigInt(candidateOffsets[1]!)).toBeLessThan(BigInt(candidateOffsets[0]!));
      expect(s.head().nextOffset).toBe('0');
      expect(batch.resolveOffset).not.toHaveBeenCalled();
      expect(s.transaction.send).not.toHaveBeenCalled();
      // Redelivery keeps the reduced cap rather than repeating the two oversized candidates.
      s.sharedState.read.mockImplementation(original).mockImplementationOnce(async (work) => {
        const result = await original(work);
        const nextOffset = (result as { head: { nextOffset: string } }).head.nextOffset;
        expect(BigInt(nextOffset)).toBeLessThan(BigInt(candidateOffsets[1]!));
        return result;
      });
      const redelivery = batchPayload();
      await s.source.process(redelivery);
      expect(redelivery.resolveOffset).toHaveBeenCalledTimes(redelivery.batch.messages.length);
      expect(
        s.transaction.send.mock.calls.filter(([entry]) => entry.topic === deliveryTopic),
      ).toHaveLength(1);
    } finally {
      clock.mockRestore();
    }
  });

  it('releases both projector permits under continuous expiry so queued metadata work progresses', async () => {
    const s = rawSetup({ captureBatchTimeoutMs: 100, projectorConcurrency: 2 });
    const rawRoute = {
      ...route,
      sessionId: 'ses_raw2',
      topic: `orca.${WORKSPACE_ID}.sessions.ses_raw2.events`,
    };
    const metadataRoute = {
      ...route,
      sessionId: 'ses_metadata',
      topic: `orca.${WORKSPACE_ID}.sessions.ses_metadata.events`,
    };
    vi.mocked(restoreKafkaCheckpoint).mockImplementation(async ({ route: restoredRoute }) =>
      initialKafkaCheckpoint(
        restoredRoute,
        restoredRoute.sessionId === metadataRoute.sessionId ? context : raw,
      ),
    );
    await s.runtime.start();
    const second = mockConsumer();
    const metadata = mockConsumer();
    s.kafka.consumer.mockReturnValueOnce(second).mockReturnValueOnce(metadata);
    await s.runtime.addSessions([rawRoute, metadataRoute]);
    // Restore owners before timing collection; no migration commit is attributed to capture.
    await s.source.process(batchPayload([]));
    await second.process(batchPayload([], rawRoute.topic));
    await metadata.process(batchPayload([], metadataRoute.topic));
    s.transaction.send.mockClear();
    let time = 0;
    const clock = vi.spyOn(performance, 'now').mockImplementation(() => time);
    const gate = deferred<void>();
    s.registryClient.resolveContext.mockImplementation(async () => {
      await gate.promise;
      time += 200;
      return { status: 'enabled', deliveryContext: raw };
    });
    const rawBatch = batchPayload();
    const secondBatch = batchPayload(
      completedPrimaryTurnEvents().map((event) =>
        wire({ ...event, sessionId: rawRoute.sessionId }),
      ),
      rawRoute.topic,
    );
    const metadataBatch = batchPayload(
      completedPrimaryTurnEvents().map((event) =>
        wire({ ...event, sessionId: metadataRoute.sessionId }),
      ),
      metadataRoute.topic,
    );
    try {
      const firstWork = s.source.process(rawBatch);
      const secondWork = second.process(secondBatch);
      await vi.waitFor(() => expect(s.registryClient.resolveContext).toHaveBeenCalledTimes(2));
      const metadataWork = metadata.process(metadataBatch);
      expect(metadataBatch.resolveOffset).not.toHaveBeenCalled();
      gate.resolve();
      await Promise.all([firstWork, secondWork, metadataWork]);
      expect(s.registryClient.resolveContext).toHaveBeenCalledTimes(4);
      expect(rawBatch.resolveOffset).not.toHaveBeenCalled();
      expect(secondBatch.resolveOffset).not.toHaveBeenCalled();
      expect(metadataBatch.resolveOffset).toHaveBeenCalledTimes(
        metadataBatch.batch.messages.length,
      );
      const sends = s.transaction.send.mock.calls.filter(
        ([entry]) => entry.topic === deliveryTopic,
      );
      expect(sends).toHaveLength(1);
      expect(JSON.stringify(sends)).not.toContain('orca.observability.projected-trace.v2');
    } finally {
      gate.resolve();
      clock.mockRestore();
    }
  });

  it('discards an expired projection before commit, shrinks the chunk and reacquires context', async () => {
    const s = rawSetup({ captureBatchTimeoutMs: 100 });
    await s.runtime.start();
    const original = s.sharedState.read.getMockImplementation()!;
    const clock = vi.spyOn(performance, 'now');
    let time = 0;
    clock.mockImplementation(() => time);
    s.sharedState.read.mockImplementationOnce(async (work) => {
      const result = await original(work);
      time = 101;
      return result;
    });
    s.registryClient.resolveContext
      .mockResolvedValueOnce({ status: 'enabled', deliveryContext: raw })
      .mockResolvedValueOnce({ status: 'enabled', deliveryContext: context });
    try {
      await s.source.process(batchPayload());
      expect(s.registryClient.resolveContext).toHaveBeenCalledTimes(3);
      const sends = s.transaction.send.mock.calls.filter(
        ([entry]) => entry.topic === deliveryTopic,
      );
      expect(sends).toHaveLength(1);
      expect(JSON.stringify(sends)).not.toContain('orca.observability.projected-trace.v2');
    } finally {
      clock.mockRestore();
    }
  });
});

it('emits fixed numeric diagnostics every minute, single-flight, and clears the timer on shutdown', async () => {
  vi.useFakeTimers();
  const onDiagnostic = vi.fn<NonNullable<KafkaOnlyExporterRuntimeOptions['onDiagnostic']>>();
  const log = vi.spyOn(console, 'info').mockImplementation(() => {});
  const s = setup(undefined, undefined, route, { onDiagnostic });
  try {
    await s.runtime.start();
    await s.source.process(batchPayload());
    expect(onDiagnostic.mock.calls.map(([summary]) => summary.phase)).toEqual(['start']);
    await vi.advanceTimersByTimeAsync(59_999);
    expect(s.sharedState.diagnostics).not.toHaveBeenCalled();
    const gate = deferred<Awaited<ReturnType<typeof s.sharedState.diagnostics>>>();
    s.sharedState.diagnostics.mockImplementationOnce(() => gate.promise);
    await vi.advanceTimersByTimeAsync(1);
    await vi.advanceTimersByTimeAsync(120_000);
    expect(s.sharedState.diagnostics).toHaveBeenCalledOnce();
    gate.resolve({
      scannedBytes: 42,
      scannedRecords: 2,
      waiters: 0,
      databaseBytes: 4096,
      databaseLimitBytes: 8192,
      diskQuotaBytes: 131072,
    });
    await vi.advanceTimersByTimeAsync(0);
    const summary = onDiagnostic.mock.calls.at(-1)![0];
    expect(summary.phase).toBe('summary');
    expect(summary.transactionPeakBytes).toBeGreaterThan(0);
    expect(summary.assemblyPeakBytes).toBeGreaterThan(0);
    expect(summary.consumerCount).toBe(2);
    expect(summary.producerCount).toBe(1);
    expect(summary.sharedReader.scannedBytes).toBe(42);
    const checkNumbers = (value: object): void => {
      for (const [key, field] of Object.entries(value)) {
        if (key === 'phase') continue;
        if (typeof field === 'object') checkNumbers(field);
        else expect(Number.isFinite(field) && field >= 0).toBe(true);
      }
    };
    checkNumbers(summary);
    const serialized = JSON.stringify(summary);
    for (const secret of [
      route.topic,
      route.sessionId,
      route.workspaceId,
      context.endpointUrl,
      namespace,
    ])
      expect(serialized).not.toContain(secret);
    expect(JSON.parse(log.mock.calls.at(-1)![0])).toEqual(summary);
    await s.runtime.stop();
    const stopped = onDiagnostic.mock.calls.at(-1)![0];
    expect(stopped.phase).toBe('stop');
    expect(stopped.consumerCount).toBe(0);
    expect(stopped.producerCount).toBe(0);
    expect(stopped.durationMs).toBeGreaterThanOrEqual(stopped.shutdownDrainMs);
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(120_000);
    expect(s.sharedState.diagnostics).toHaveBeenCalledTimes(2); // Periodic sample plus final sample.
    expect(onDiagnostic.mock.calls.at(-1)![0]).toBe(stopped);
  } finally {
    await s.runtime.stop();
    log.mockRestore();
  }
});

it('drains a pending diagnostic query before closing the index without logging a late summary', async () => {
  vi.useFakeTimers();
  const onDiagnostic = vi.fn<NonNullable<KafkaOnlyExporterRuntimeOptions['onDiagnostic']>>();
  const s = setup(undefined, undefined, route, { onDiagnostic });
  const gate = deferred<Awaited<ReturnType<typeof s.sharedState.diagnostics>>>();
  s.sharedState.diagnostics.mockImplementationOnce(() => gate.promise);
  await s.runtime.start();
  await vi.advanceTimersByTimeAsync(60_000);
  const stop = s.runtime.stop();
  await vi.advanceTimersByTimeAsync(120_000);
  expect(vi.getTimerCount()).toBe(0);
  expect(s.sharedState.close).not.toHaveBeenCalled();
  gate.resolve({
    scannedBytes: 1,
    scannedRecords: 1,
    waiters: 0,
    databaseBytes: 4096,
    databaseLimitBytes: 8192,
    diskQuotaBytes: 131072,
  });
  await stop;
  expect(s.sharedState.close).toHaveBeenCalledOnce();
  expect(onDiagnostic.mock.calls.map(([summary]) => summary.phase)).toEqual(['start', 'stop']);
});

it('isolates diagnostic query, logger and callback failures from readiness and shutdown', async () => {
  vi.useFakeTimers();
  const log = vi.spyOn(console, 'info').mockImplementation(() => {
    throw new Error('secret');
  });
  const onDiagnostic = vi.fn(() => {
    throw new Error('secret');
  });
  const s = setup(undefined, undefined, route, { onDiagnostic });
  try {
    s.sharedState.diagnostics.mockRejectedValueOnce(new Error('secret'));
    await s.runtime.start();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(s.runtime.status().ready).toBe(true);
    expect(onDiagnostic).toHaveBeenLastCalledWith(
      expect.objectContaining({ phase: 'summary', diagnosticErrors: 1 }),
    );
    await s.runtime.stop();
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    await s.runtime.stop();
    log.mockRestore();
  }
});

it('reports restored assembly size even without a new projection transaction', async () => {
  const onDiagnostic = vi.fn<NonNullable<KafkaOnlyExporterRuntimeOptions['onDiagnostic']>>();
  const s = setup(undefined, undefined, route, { onDiagnostic });
  await s.runtime.start();
  await s.source.process(batchPayload([]));
  await s.runtime.stop();
  const summary = onDiagnostic.mock.calls.at(-1)![0];
  expect(summary.assemblyPeakBytes).toBeGreaterThan(0);
  expect(summary.transactionPeakBytes).toBe(0);
  expect(summary.sharedReader.scannedBytes).toBe(42); // Final sample even before the first minute.
});

it('reports actual transaction bytes and configured limit without source identity', async () => {
  const onDiagnostic = vi.fn<NonNullable<KafkaOnlyExporterRuntimeOptions['onDiagnostic']>>();
  const s = setup(undefined, undefined, route, { maxTransactionBytes: 1, onDiagnostic });
  await s.runtime.start();
  await expect(s.source.process(batchPayload())).rejects.toThrow(
    /^Kafka exporter transaction size budget exceeded: kind=transaction actual=[0-9]+ limit=1$/,
  );
  await s.runtime.stop();
  expect(onDiagnostic.mock.calls.at(-1)![0].transactionPeakBytes).toBeGreaterThan(1);
  expect(s.transaction.send).not.toHaveBeenCalled();
});

it('starts discovery through a work-conserving pool rather than serial joins or fixed waves', async () => {
  const s = setup(undefined, undefined, route, { startupConcurrency: 2 });
  await s.runtime.start();
  const consumers = Array.from({ length: 4 }, () => mockConsumer());
  const gates = consumers.map(() => deferred<void>());
  consumers.forEach((consumer, index) => {
    consumer.connect.mockImplementation(() => gates[index]!.promise);
    s.kafka.consumer.mockReturnValueOnce(consumer);
  });
  const routes = consumers.map((_, index) => ({
    ...route,
    sessionId: `ses_pool${index}`,
    topic: `orca.${WORKSPACE_ID}.sessions.ses_pool${index}.events`,
  }));
  const update = s.runtime.addSessions(routes);
  try {
    await vi.waitFor(() => expect(consumers[1]!.connect).toHaveBeenCalledOnce());
    expect(consumers[2]!.connect).not.toHaveBeenCalled();
    gates[1]!.resolve();
    await vi.waitFor(() => expect(consumers[2]!.connect).toHaveBeenCalledOnce());
    expect(consumers[0]!.run).not.toHaveBeenCalled();
  } finally {
    gates.forEach((gate) => gate.resolve());
    await update;
  }
});

it('drains late delivery connect on shutdown without subscribing or running', async () => {
  const s = setup();
  const gate = deferred<void>();
  s.delivery.connect.mockImplementation(() => gate.promise);
  const start = s.runtime.start().catch(() => undefined);
  await vi.waitFor(() => expect(s.delivery.connect).toHaveBeenCalledOnce());
  const stop = s.runtime.stop();
  expect(s.delivery.disconnect).not.toHaveBeenCalled();
  gate.resolve();
  await Promise.all([start, stop]);
  expect(s.delivery.subscribe).not.toHaveBeenCalled();
  expect(s.delivery.run).not.toHaveBeenCalled();
  expect(s.delivery.disconnect).toHaveBeenCalledOnce();
});

it('shares two lazy restore permits between sources and delivery and heartbeats queued work', async () => {
  vi.useFakeTimers();
  const s = setup();
  await s.runtime.start();
  const secondRoute = {
    ...route,
    sessionId: 'ses_second',
    topic: `orca.${WORKSPACE_ID}.sessions.ses_second.events`,
  };
  const second = mockConsumer();
  s.kafka.consumer.mockReturnValueOnce(second);
  await s.runtime.addSessions([secondRoute]);
  const gates = [deferred<void>(), deferred<void>()];
  vi.mocked(restoreKafkaCheckpoint).mockImplementation(async ({ route: restoredRoute }) => {
    await gates[restoredRoute.topic === route.topic ? 0 : 1]!.promise;
    return initialKafkaCheckpoint(restoredRoute, context);
  });
  const batches = [batchPayload([]), batchPayload([], secondRoute.topic), deliveryBatch()];
  const firstWork = s.source.process(batches[0]!);
  const secondWork = second.process(batches[1]!);
  await vi.advanceTimersByTimeAsync(0);
  expect(restoreKafkaCheckpoint).toHaveBeenCalledTimes(2);
  const deliveryWork = s.delivery.process(batches[2]!);
  await vi.advanceTimersByTimeAsync(3_000);
  expect(restoreKafkaValue).not.toHaveBeenCalled();
  expect(batches[2]!.heartbeat.mock.calls.length).toBeGreaterThan(1);
  expect(s.producers).toHaveLength(2);
  gates[0]!.resolve();
  await vi.advanceTimersByTimeAsync(0);
  expect(restoreKafkaValue).toHaveBeenCalledOnce();
  gates[1]!.resolve();
  await Promise.all([firstWork, secondWork, deliveryWork]);
});

it('uses the default pool of eight for initial startup and does not require lazy restore for readiness', async () => {
  const routes = Array.from({ length: 9 }, (_, i) => ({
    ...route,
    sessionId: `ses_initial${i}`,
    topic: `orca.${WORKSPACE_ID}.sessions.ses_initial${i}.events`,
  }));
  const s = setup(undefined, undefined, route, { sessions: routes });
  const consumers = [s.delivery, s.source, ...Array.from({ length: 8 }, () => mockConsumer())];
  const gate = deferred<void>();
  consumers.forEach((consumer, i) => {
    consumer.connect.mockImplementation(() => gate.promise);
    if (i > 1) s.kafka.consumer.mockReturnValueOnce(consumer);
  });
  const start = s.runtime.start();
  try {
    await vi.waitFor(() => expect(s.kafka.consumer).toHaveBeenCalledTimes(8));
    expect(s.runtime.status()).toEqual({ ready: false, state: 'starting' });
    expect(restoreKafkaCheckpoint).not.toHaveBeenCalled();
  } finally {
    gate.resolve();
    await start;
  }
  expect(s.kafka.consumer).toHaveBeenCalledTimes(10);
  expect(s.runtime.status()).toEqual({ ready: true, state: 'running' });
  expect(restoreKafkaCheckpoint).not.toHaveBeenCalled();
  expect(restoreKafkaValue).not.toHaveBeenCalled();
});

it.each(['shutdown', 'generation changed'] as const)(
  'cancels queued delivery restore on %s without creating its producer',
  async (reason) => {
    vi.useFakeTimers();
    const s = setup(undefined, undefined, route, { restoreConcurrency: 1 });
    await s.runtime.start();
    const restore = deferred<void>();
    vi.mocked(restoreKafkaCheckpoint).mockImplementation(async () => {
      await restore.promise;
      return initialKafkaCheckpoint(route, context);
    });
    const sourceWork = s.source.process(batchPayload([])).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(0);
    const batch = deliveryBatch();
    const deliveryWork = s.delivery.process(batch);
    await vi.advanceTimersByTimeAsync(0);
    expect(s.producers).toHaveLength(1);
    if (reason === 'shutdown') void s.runtime.stop();
    else s.delivery.join();
    await vi.advanceTimersByTimeAsync(1_000);
    await deliveryWork;
    expect(restoreKafkaValue).not.toHaveBeenCalled();
    restore.resolve();
    await sourceWork;
    expect(s.producers).toHaveLength(1);
    if (reason === 'generation changed') {
      await s.delivery.process(deliveryBatch());
      expect(restoreKafkaValue).toHaveBeenCalledOnce();
      expect(s.runtime.status().ready).toBe(true);
    }
    await s.runtime.stop();
  },
);

it('releases the restore permit before source projection finishes', async () => {
  const decode = deferred<Event | null>();
  const entered = deferred<void>();
  const codec: KafkaTranscriptCodec = {
    prepareWriter: vi.fn(),
    encode: vi.fn(),
    close: vi.fn(),
    decode: vi.fn(() => {
      entered.resolve();
      return decode.promise;
    }),
  };
  const s = setup(undefined, codec, route, { restoreConcurrency: 1 });
  await s.runtime.start();
  const sourceWork = s.source.process(batchPayload([wire(completedPrimaryTurnEvents()[0]!)]));
  await entered.promise;
  await s.delivery.process(deliveryBatch());
  expect(restoreKafkaValue).toHaveBeenCalledOnce();
  expect(deliverKafkaTrace).toHaveBeenCalledOnce();
  decode.resolve(null);
  await sourceWork;
});

it('aborts queued restores before releasing the permit of a fatal restore', async () => {
  const s = setup(undefined, undefined, route, { restoreConcurrency: 1 });
  await s.runtime.start();
  const entered = deferred<void>();
  const gate = deferred<void>();
  vi.mocked(restoreKafkaCheckpoint).mockImplementation(async () => {
    entered.resolve();
    await gate.promise;
    throw new Error('restore corrupt');
  });
  const sourceWork = s.source.process(batchPayload()).catch(() => undefined);
  await entered.promise;
  const deliveryWork = s.delivery.process(deliveryBatch()).catch(() => undefined);
  await new Promise<void>((resolve) => setImmediate(resolve));
  gate.resolve();
  await Promise.all([sourceWork, deliveryWork]);
  await s.runtime.stop();
  expect(s.producers).toHaveLength(1);
  expect(restoreKafkaValue).not.toHaveBeenCalled();
  expect(s.runtime.status().state).toBe('failed');
});

it.each(['source', 'delivery'] as const)(
  'drains late %s producer connect and never restores after shutdown',
  async (kind) => {
    const s = setup();
    await s.runtime.start();
    const connected = deferred<void>();
    const entered = deferred<void>();
    const producer = {
      connect: vi.fn(() => {
        entered.resolve();
        return connected.promise;
      }),
      disconnect: vi.fn(async () => {}),
      transaction: vi.fn(),
    };
    s.kafka.producer.mockReturnValueOnce(producer as never);
    const work = (
      kind === 'source' ? s.source.process(batchPayload()) : s.delivery.process(deliveryBatch())
    ).catch(() => undefined);
    await entered.promise;
    let stopped = false;
    const stop = s.runtime.stop().then(() => {
      stopped = true;
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(stopped).toBe(false);
    expect(producer.disconnect).not.toHaveBeenCalled();
    expect(s.runtime.status().ready).toBe(false);
    connected.resolve();
    await Promise.all([work, stop]);
    expect(producer.disconnect).toHaveBeenCalledOnce();
    expect(restoreKafkaCheckpoint).not.toHaveBeenCalled();
    expect(restoreKafkaValue).not.toHaveBeenCalled();
  },
);

it('stops claiming joins at the first failure and drains peers without self-waiting cleanup', async () => {
  const s = setup(undefined, undefined, route, { startupConcurrency: 2 });
  await s.runtime.start();
  const consumers = Array.from({ length: 3 }, () => mockConsumer());
  const late = deferred<void>();
  const fail = deferred<void>();
  consumers[0]!.connect.mockImplementation(async () => {
    await fail.promise;
    throw new Error('join failed');
  });
  consumers[1]!.connect.mockImplementation(() => late.promise);
  consumers.forEach((consumer) => s.kafka.consumer.mockReturnValueOnce(consumer));
  const routes = consumers.map((_, i) => ({
    ...route,
    sessionId: `ses_fail${i}`,
    topic: `orca.${WORKSPACE_ID}.sessions.ses_fail${i}.events`,
  }));
  const update = s.runtime.addSessions(routes).catch((error: unknown) => error);
  await vi.waitFor(() => expect(consumers[1]!.connect).toHaveBeenCalledOnce());
  fail.resolve();
  await vi.waitFor(() => expect(s.runtime.status().state).toBe('failed'));
  expect(consumers[2]!.connect).not.toHaveBeenCalled();
  expect(consumers[1]!.disconnect).not.toHaveBeenCalled();
  late.resolve();
  expect(await update).toEqual(new Error('join failed'));
  await s.runtime.stop();
  expect(consumers[1]!.subscribe).not.toHaveBeenCalled();
  expect(consumers[1]!.disconnect).toHaveBeenCalledOnce();
});

it('accepts only authoritative Avro routes in initial and manually added sessions', async () => {
  const codec: KafkaTranscriptCodec = {
    encoding: 'avro',
    prepareWriter: vi.fn(),
    encode: vi.fn(),
    decode: vi.fn(),
    close: vi.fn(),
  };
  const avroRoute = { ...route, topic: 'public.default.' + route.topic + '-avro' };
  expect(() => setup(undefined, codec)).toThrow('invalid Kafka exporter Session route');
  expect(() => setup(undefined, undefined, avroRoute)).toThrow(
    'invalid Kafka exporter Session route',
  );
  expect(() => setup(undefined, codec, { ...avroRoute, workspaceId: 'other' })).toThrow(
    'invalid Kafka exporter Session route',
  );
  expect(() => setup(undefined, codec, { ...avroRoute, sessionId: 'ses_other' })).toThrow(
    'invalid Kafka exporter Session route',
  );
  const s = setup(undefined, codec, avroRoute);
  await s.runtime.start();
  expect(s.source.subscribe).toHaveBeenCalledWith({ topic: avroRoute.topic, fromBeginning: true });
  expect(s.delivery.subscribe).toHaveBeenCalledWith({
    topic: deliveryTopic + '-avro',
    fromBeginning: true,
  });
  expect(s.kafka.consumer.mock.calls.map(([options]) => options.groupId)).toEqual([
    namespace + '-avro-delivery-v1',
    kafkaSourceGroupId(namespace + '-avro', avroRoute.topic),
  ]);
  vi.mocked(restoreKafkaCheckpoint).mockResolvedValue(initialKafkaCheckpoint(avroRoute, context));
  await s.source.process(batchPayload([], avroRoute.topic));
  expect(restoreKafkaCheckpoint).toHaveBeenCalledWith(
    expect.objectContaining({
      topic: checkpointTopic + '-avro',
      route: avroRoute,
      key: kafkaSourceGroupId(namespace + '-avro', avroRoute.topic),
    }),
  );
  await expect(s.runtime.addSessions([avroRoute])).resolves.toBeUndefined();
  await expect(s.runtime.addSessions([route])).rejects.toThrow(
    'invalid Kafka exporter Session route',
  );
  const other = setup(undefined, codec, avroRoute);
  await expect(other.runtime.addSessions([{ ...avroRoute, workspaceId: 'other' }])).rejects.toThrow(
    'invalid Kafka exporter Session route',
  );
  await s.runtime.stop();
  expect(s.source.disconnect).toHaveBeenCalledOnce();
  expect(codec.prepareWriter).not.toHaveBeenCalled();
  expect(codec.close).not.toHaveBeenCalled();
});
afterEach(async () => {
  await Promise.all(runtimes.splice(0).map((runtime) => runtime.stop()));
  vi.useRealTimers();
});

it.each([{ protocol: 'http/protobuf' }, { compression: 'gzip' }])(
  'persists unsupported Registry capability %j as Session suppression across restart',
  async (mode) => {
    const schema = internalContract.resolveAgentObservabilityContext.responses[200];
    const unsupported = schema.parse(enabledRegistryContext(WORKSPACE_ID, SESSION_ID));
    Object.assign(unsupported.binding!.config, mode);
    const supportedSessionId = 'ses_supported';
    const supportedRoute = {
      ...route,
      sessionId: supportedSessionId,
      topic: `orca.${WORKSPACE_ID}.sessions.${supportedSessionId}.events`,
    };
    const fetchImpl = vi.fn(async (input: string | URL | Request) =>
      Response.json(
        schema.parse(
          String(input).includes(`/sessions/${SESSION_ID}/`)
            ? unsupported
            : enabledRegistryContext(WORKSPACE_ID, supportedSessionId),
        ),
      ),
    );
    const client = new RegistryObservabilityClient({
      internalBaseUrl: 'https://registry.internal',
      tokenProvider: async () => 'test-token',
      fetchImpl,
    });
    vi.mocked(restoreKafkaCheckpoint).mockResolvedValue(null);
    const first = setup();
    first.registryClient.resolveContext.mockImplementation((input) => client.resolveContext(input));
    await first.runtime.start();
    const messages = completedPrimaryTurnEvents().map(wire);
    const split = 5;
    const batch = batchPayload(messages.slice(0, split));
    await expect(first.source.process(batch)).resolves.toBeUndefined();
    const checkpoint = first.head();
    const savedRows = new Map(first.rows);
    const nextOffset = (BigInt(messages[split - 1]!.offset) + 1n).toString();
    expect(checkpoint.deliveryContext).toBeNull();
    expect(checkpoint.nextOffset).toBe(nextOffset);
    expect(first.transaction.sendOffsets).toHaveBeenCalledWith({
      consumerGroupId: kafkaSourceGroupId(namespace, route.topic),
      topics: [{ topic: route.topic, partitions: [{ partition: 0, offset: nextOffset }] }],
    });
    // Fresh absent state is initialized durably before the first projection transaction.
    expect(first.transaction.commit).toHaveBeenCalledTimes(2);
    expect(
      first.transaction.sendOffsets.mock.calls.map(
        ([request]) => request.topics[0]!.partitions[0]!.offset,
      ),
    ).toEqual(['0', nextOffset]);
    expect(batch.resolveOffset.mock.calls.flat()).toEqual(
      messages.slice(0, split).map((entry) => entry.offset),
    );
    expect(first.transaction.send.mock.calls.some(([entry]) => entry.topic === deliveryTopic)).toBe(
      false,
    );
    expect(first.registryClient.resolveSecret).not.toHaveBeenCalled();

    const supportedSource = mockConsumer();
    first.kafka.consumer.mockReturnValueOnce(supportedSource);
    await first.runtime.addSessions([supportedRoute]);
    await supportedSource.process(
      batchPayload(
        completedPrimaryTurnEvents().map((event) =>
          wire({ ...event, sessionId: supportedSessionId }),
        ),
        supportedRoute.topic,
      ),
    );
    expect(
      first.transaction.send.mock.calls.filter(([entry]) => entry.topic === deliveryTopic),
    ).toHaveLength(1);
    expect(first.runtime.status()).toEqual({ ready: true, state: 'running' });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    await first.runtime.stop();

    // Restore through the real persisted-state parser; a changed Registry must not unpin suppression.
    const restarted = setup();
    for (const [key, value] of savedRows) restarted.rows.set(key, value);
    restarted.admin.fetchOffsets.mockResolvedValue([
      {
        topic: route.topic,
        partitions: [{ partition: 0, offset: nextOffset }],
      },
    ]);
    await restarted.runtime.start();
    await restarted.source.process(batchPayload(messages.slice(split)));
    expect(restarted.registryClient.resolveContext).not.toHaveBeenCalled();
    expect(restarted.registryClient.resolveSecret).not.toHaveBeenCalled();
    expect(
      restarted.transaction.send.mock.calls.every(([entry]) => entry.topic === checkpointTopic),
    ).toBe(true);
    const restored = restarted.head();
    expect(restored.deliveryContext).toBeNull();
    expect(restored.nextOffset).toBe((BigInt(messages.at(-1)!.offset) + 1n).toString());
    expect(restarted.transaction.sendOffsets).toHaveBeenCalledWith({
      consumerGroupId: kafkaSourceGroupId(namespace, route.topic),
      topics: [{ topic: route.topic, partitions: [{ partition: 0, offset: restored.nextOffset }] }],
    });
    expect(restarted.transaction.commit).toHaveBeenCalledOnce();
    expect(restarted.runtime.status().ready).toBe(true);
  },
);

it.each([{ protocol: 'http/protobuf' }, { compression: 'gzip' }, { timeoutMs: 'invalid' }])(
  'fails closed on invalid persisted delivery context %j instead of suppressing it',
  async (invalid) => {
    const { runtime, source, registryClient, transaction, rows } = setup();
    const key = kafkaSourceGroupId(namespace, route.topic);
    const seeded = migrateKafkaCheckpoint(
      JSON.stringify(initialKafkaCheckpoint(route, context)),
      route,
      key,
    );
    for (const record of seeded.records) rows.set(record.key, record.value);
    rows.set(key, JSON.stringify({ ...seeded.head, deliveryContext: { ...context, ...invalid } }));
    await runtime.start();
    await expect(source.process(batchPayload())).rejects.toThrow();
    expect(runtime.status().state).toBe('failed');
    expect(registryClient.resolveContext).not.toHaveBeenCalled();
    expect(transaction.sendOffsets).not.toHaveBeenCalled();
    expect(transaction.commit).not.toHaveBeenCalled();
  },
);

it('fails closed when source retention overtakes an already active owner', async () => {
  const { runtime, source, admin, transaction } = setup();
  await runtime.start();
  const messages = completedPrimaryTurnEvents().map(wire);
  await source.process(batchPayload(messages.slice(0, 5)));
  transaction.sendOffsets.mockClear();
  transaction.commit.mockClear();
  admin.fetchTopicOffsets.mockResolvedValue([{ partition: 0, low: '7', high: '100' }]);
  await expect(source.process(batchPayload(messages.slice(6)))).rejects.toThrow(
    'Kafka source retention overtook the projection checkpoint',
  );
  expect(transaction.sendOffsets).not.toHaveBeenCalled();
  expect(transaction.commit).not.toHaveBeenCalled();
  expect(runtime.status().state).toBe('failed');
});

it('fails closed when an active source log is truncated behind its checkpoint', async () => {
  const { runtime, source, admin, transaction } = setup();
  await runtime.start();
  const messages = completedPrimaryTurnEvents().map(wire);
  await source.process(batchPayload(messages.slice(0, 5)));
  transaction.sendOffsets.mockClear();
  admin.fetchTopicOffsets.mockResolvedValue([{ partition: 0, low: '0', high: '5' }]);
  await expect(source.process(batchPayload(messages.slice(5)))).rejects.toThrow(
    'Kafka source log end precedes the projection checkpoint',
  );
  expect(transaction.sendOffsets).not.toHaveBeenCalled();
  expect(runtime.status().state).toBe('failed');
});

describe('Kafka-only runtime transactions', () => {
  it.each(['checkpoint replay', 'foreign workspace', 'foreign session'] as const)(
    'skips ambiguous headers for %s before codec I/O using legacy access order',
    async (reason) => {
      const actual =
        await vi.importActual<typeof import('@orca/transcript-store')>('@orca/transcript-store');
      const codec = actual.createKafkaTranscriptCodec();
      const decode = vi.spyOn(codec, 'decode');
      vi.mocked(createKafkaTranscriptCodec).mockReturnValueOnce(codec);
      const checkpoint = initialKafkaCheckpoint(route, context);
      const message = wire(completedPrimaryTurnEvents()[0]!);
      const nextOffset = (BigInt(message.offset) + 1n).toString();
      if (reason === 'checkpoint replay') checkpoint.nextOffset = nextOffset;
      vi.mocked(restoreKafkaCheckpoint).mockResolvedValue(checkpoint);
      const s = setup();
      await s.runtime.start();
      const batch = batchPayload([
        {
          ...message,
          headers: {
            ...message.headers,
            workspace_id:
              reason === 'checkpoint replay'
                ? [route.workspaceId, route.workspaceId]
                : reason === 'foreign workspace'
                  ? 'ws_foreign'
                  : route.workspaceId,
            session_id:
              reason === 'foreign workspace' ? [route.sessionId, route.sessionId] : 'ses_foreign',
            kind: ['user.message', 'user.message'],
          },
        },
      ]);
      await s.source.process(batch);
      expect(decode).not.toHaveBeenCalled();
      expect(s.runtime.status().state).toBe('running');
      expect(
        s.transaction.send.mock.calls.every(([entry]) => entry.topic === checkpointTopic),
      ).toBe(true);
      const saved = s.head();
      expect(saved.nextOffset).toBe(nextOffset);
      expect(saved.identityCount).toBe(0);
      expect(batch.resolveOffset).toHaveBeenCalledWith(message.offset);
    },
  );

  it.each([
    'kind',
    'workspace_id',
    'session_id',
    'user_id',
    'id',
    'subpath',
    'produced_at',
    'produced_by',
    'idempotency_key',
  ])(
    'rejects ambiguous raw %s headers with the real default codec before projection progress',
    async (name) => {
      const actual =
        await vi.importActual<typeof import('@orca/transcript-store')>('@orca/transcript-store');
      vi.mocked(createKafkaTranscriptCodec).mockReturnValueOnce(
        actual.createKafkaTranscriptCodec(),
      );
      const checkpoint = initialKafkaCheckpoint(route, context);
      vi.mocked(restoreKafkaCheckpoint).mockResolvedValue(checkpoint);
      const before = JSON.stringify(checkpoint);
      const s = setup();
      await s.runtime.start();
      const message = wire(completedPrimaryTurnEvents()[0]!);
      const value = message.headers?.[name]?.toString() ?? '';
      const batch = batchPayload([
        { ...message, headers: { ...message.headers, [name]: [value, value] } },
      ]);
      await expect(s.source.process(batch)).rejects.toThrow('ambiguous Kafka Transcript header');
      expect(s.runtime.status().state).toBe('failed');
      expect(s.order).not.toContain('transaction');
      expect(s.transaction.send).not.toHaveBeenCalled();
      expect(s.transaction.sendOffsets).not.toHaveBeenCalled();
      expect(s.transaction.commit).not.toHaveBeenCalled();
      expect(batch.resolveOffset).not.toHaveBeenCalled();
      expect(JSON.stringify(checkpoint)).toBe(before);
    },
  );

  it.each(['mixed', 'avro'] as const)(
    'projects real %s codec records with the same hashes and OTLP as raw',
    async (mode) => {
      const actual =
        await vi.importActual<typeof import('@orca/transcript-store')>('@orca/transcript-store');
      let schema: unknown;
      const requests: string[] = [];
      const server = createServer(async (request, response) => {
        requests.push(`${request.method} ${request.url}`);
        response.setHeader('content-type', 'application/json');
        if (request.method === 'POST') {
          const chunks: Buffer[] = [];
          for await (const chunk of request) chunks.push(Buffer.from(chunk));
          schema = JSON.parse(Buffer.concat(chunks).toString()).schema;
          response.end(JSON.stringify({ id: 17 }));
        } else {
          response.end(JSON.stringify({ schemaType: 'AVRO', schema }));
        }
      });
      await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
      const schemaRegistry = { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}` };
      const writer = actual.createKafkaTranscriptCodec({ encoding: 'avro', schemaRegistry });
      const reader = actual.createKafkaTranscriptCodec({ schemaRegistry });
      const s = setup(undefined, reader);
      try {
        const events = completedPrimaryTurnEvents();
        const messages = await Promise.all(
          events.map(async (event, index) => {
            if (mode === 'mixed' && index % 2 === 0) return wire(event);
            const encoded = await writer.encode(event);
            return {
              key: Buffer.from(event.id),
              offset: String(event.seq),
              timestamp: '0',
              attributes: 0,
              headers: encoded.headers!,
              value: Buffer.from(encoded.value!),
            };
          }),
        );
        requests.length = 0;
        await s.runtime.start();
        expect(requests).toEqual([]);
        await s.source.process(batchPayload(messages));
        expect(requests).toEqual(['GET /schemas/ids/17']);
        const expected = projectKafkaEvents(
          initialKafkaCheckpoint(route, context),
          events.map((event) => ({ offset: String(event.seq), event })),
        );
        expectV2State(s, expected.checkpoint);
        expect(s.transaction.send).toHaveBeenCalledWith({
          topic: deliveryTopic,
          messages: expected.deliveries.map((record) => ({
            key: record.trace.traceId,
            value: JSON.stringify(record),
          })),
        });
      } finally {
        await s.runtime.stop();
        await Promise.all([writer.close(), reader.close()]);
        await new Promise<void>((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        );
      }
    },
  );

  it('discards a schema failure returned after the source generation changes', async () => {
    const codec: KafkaTranscriptCodec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn(async () => {
        s.source.join();
        throw new Error('old owner schema failure');
      }),
    };
    const s = setup(undefined, codec);
    await s.runtime.start();
    const batch = batchPayload();
    await expect(s.source.process(batch)).resolves.toBeUndefined();
    expect(s.runtime.status().state).toBe('running');
    expect(s.transaction.sendOffsets).not.toHaveBeenCalled();
    expect(batch.resolveOffset).not.toHaveBeenCalled();
  });

  it('skips cold-cache committed offsets before decoding and retains scanned poison-route progress', async () => {
    const events = completedPrimaryTurnEvents();
    const checkpoint = projectKafkaEvents(
      initialKafkaCheckpoint(route, context),
      events.slice(0, 4).map((event) => ({ offset: String(event.seq), event })),
    ).checkpoint;
    const before = JSON.stringify(checkpoint);
    vi.mocked(restoreKafkaCheckpoint).mockResolvedValue(checkpoint);
    const codec: KafkaTranscriptCodec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn(async (message) => {
        if (BigInt(message.offset) < BigInt(checkpoint.nextOffset))
          throw new Error('old schema unavailable');
        return null;
      }),
    };
    const s = setup(undefined, codec);
    await s.runtime.start();
    const batch = batchPayload(events.map(wire));
    await s.source.process(batch);
    expect(codec.decode).toHaveBeenCalledTimes(events.length - 4);
    expect(JSON.stringify(checkpoint)).toBe(before);
    const saved = s.head();
    expectV2State(s, { ...checkpoint, nextOffset: String(events.at(-1)!.seq + 1) });
    expect(saved.nextOffset).toBe(String(events.at(-1)!.seq + 1));
    expect(s.transaction.send.mock.calls.every(([entry]) => entry.topic === checkpointTopic)).toBe(
      true,
    );
  });

  it.each(['-1', '01', '9007199254740992', '1'])(
    'validates complete batch ordering before any codec lookup for offset %s',
    async (offset) => {
      const codec: KafkaTranscriptCodec = {
        prepareWriter: vi.fn(),
        encode: vi.fn(),
        close: vi.fn(),
        decode: vi.fn(),
      };
      const s = setup(undefined, codec);
      await s.runtime.start();
      const message = wire(completedPrimaryTurnEvents()[0]!);
      const batch = batchPayload([
        { ...message, offset: '1' },
        { ...message, offset },
      ]);
      await expect(s.source.process(batch)).rejects.toThrow('ordering');
      expect(codec.decode).not.toHaveBeenCalled();
      expect(s.transaction.send).not.toHaveBeenCalled();
      expect(batch.resolveOffset).not.toHaveBeenCalled();
    },
  );

  it('fails the whole instance on a late decode error before any chunk commits and restarts from the checkpoint', async () => {
    const checkpoint = initialKafkaCheckpoint(route, context);
    const before = JSON.stringify(checkpoint);
    vi.mocked(restoreKafkaCheckpoint).mockResolvedValue(checkpoint);
    const codec: KafkaTranscriptCodec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn(async (message) => {
        if (message.offset === '101') throw new Error('schema unavailable');
        return null;
      }),
    };
    const s = setup(undefined, codec);
    await s.runtime.start();
    const message = wire(completedPrimaryTurnEvents()[0]!);
    const batch = batchPayload(
      Array.from({ length: 102 }, (_, offset) => ({ ...message, offset: String(offset) })),
    );
    await expect(s.source.process(batch)).rejects.toThrow('schema unavailable');
    await s.runtime.stop();
    expect(s.order).not.toContain('transaction');
    expect(s.transaction.sendOffsets).not.toHaveBeenCalled();
    expect(batch.resolveOffset).not.toHaveBeenCalled();
    expect(JSON.stringify(checkpoint)).toBe(before);
    expect(s.runtime.status().state).toBe('failed');
    expect(s.delivery.disconnect).toHaveBeenCalledOnce();
    expect(codec.close).not.toHaveBeenCalled();
    const restarted = setup();
    await restarted.runtime.start();
    await restarted.source.process(batchPayload());
    expect(restarted.transaction.commit).toHaveBeenCalledOnce();
    expect(
      restarted.transaction.send.mock.calls.some(([entry]) => entry.topic === deliveryTopic),
    ).toBe(true);
  });

  it('closes its default read-only codec exactly once without preparing a writer', async () => {
    const s = setup();
    const codec = vi.mocked(createKafkaTranscriptCodec).mock.results.at(-1)!.value;
    await s.runtime.start();
    await s.source.process(batchPayload());
    await Promise.all([s.runtime.stop(), s.runtime.stop()]);
    expect(codec.close).toHaveBeenCalledOnce();
    expect(codec.prepareWriter).not.toHaveBeenCalled();
  });

  it.each(['heartbeat failure', 'generation changed', 'shutdown'] as const)(
    'keeps schema waits alive with heartbeats and cancels on %s without transaction progress',
    async (reason) => {
      vi.useFakeTimers();
      const entered = deferred<AbortSignal>();
      const codec: KafkaTranscriptCodec = {
        prepareWriter: vi.fn(),
        encode: vi.fn(),
        close: vi.fn(),
        decode: vi.fn((_message, _route, signal) => {
          entered.resolve(signal!);
          return new Promise<Event | null>((_resolve, reject) => {
            signal!.addEventListener('abort', () => reject(signal!.reason), { once: true });
          });
        }),
      };
      const s = setup(undefined, codec);
      await s.runtime.start();
      const batch = batchPayload();
      const processing = s.source.process(batch);
      const result = processing.catch((error: unknown) => error);
      const signal = await entered.promise;
      batch.heartbeat.mockClear();
      await vi.advanceTimersByTimeAsync(9_000);
      expect(batch.heartbeat.mock.calls.length).toBeGreaterThanOrEqual(3);
      expect(s.order).not.toContain('transaction');
      if (reason === 'heartbeat failure')
        batch.heartbeat.mockRejectedValue(new Error('heartbeat failed'));
      else if (reason === 'generation changed') s.source.join();
      else void s.runtime.stop();
      await vi.advanceTimersByTimeAsync(3_000);
      await result;
      if (reason !== 'generation changed') await s.runtime.stop();
      expect(signal.aborted).toBe(true);
      expect(s.transaction.send).not.toHaveBeenCalled();
      expect(s.transaction.commit).not.toHaveBeenCalled();
      expect(batch.resolveOffset).not.toHaveBeenCalled();
      expect(s.runtime.status().state).toBe(
        reason === 'heartbeat failure' ? 'failed' : reason === 'shutdown' ? 'stopped' : 'running',
      );
      if (reason === 'heartbeat failure') expect(s.delivery.disconnect).toHaveBeenCalledOnce();
      // A live runtime retains only its fixed diagnostic interval.
      expect(vi.getTimerCount()).toBe(reason === 'generation changed' ? 1 : 0);
    },
  );

  it('projects restored codec Events rather than their wire frames before opening a transaction', async () => {
    const events = completedPrimaryTurnEvents();
    const messages = events.map((event) => ({ ...wire(event), value: Buffer.from('avro frame') }));
    const codec: KafkaTranscriptCodec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn(async (message) => {
        expect(s.transaction.send).not.toHaveBeenCalled();
        expect(s.order).not.toContain('transaction');
        return events.find((event) => String(event.seq) === message.offset)!;
      }),
    };
    const s = setup(undefined, codec);
    await s.runtime.start();
    await s.source.process(batchPayload(messages));
    const expected = projectKafkaEvents(
      initialKafkaCheckpoint(route, context),
      events.map((event) => ({ offset: String(event.seq), event })),
    );
    expectV2State(s, expected.checkpoint);
    expect(s.transaction.send).toHaveBeenCalledWith({
      topic: deliveryTopic,
      messages: expected.deliveries.map((record) => ({
        key: record.trace.traceId,
        value: JSON.stringify(record),
      })),
    });
    expect(codec.decode).toHaveBeenCalledTimes(messages.length);
    await s.runtime.stop();
    expect(codec.prepareWriter).not.toHaveBeenCalled();
    expect(codec.close).not.toHaveBeenCalled();
  });

  it.each([checkpointTopic, deliveryTopic, route.topic])(
    'rejects metadata missing expected topic %s before starting consumers',
    async (missing) => {
      const s = setup();
      s.admin.fetchTopicMetadata.mockImplementation(async ({ topics }) => ({
        topics: topics
          .filter((name) => name !== missing)
          .map((name) => ({ name, partitions: [{ partitionId: 0 }] })),
      }));
      await expect(s.runtime.start()).rejects.toThrow('every expected topic exactly once');
      expect(s.kafka.consumer).not.toHaveBeenCalled();
      expect(s.runtime.status()).toEqual({ ready: false, state: 'failed' });
    },
  );

  it.each(['empty', 'duplicate', 'unexpected'])('rejects %s topic metadata', async (kind) => {
    const s = setup();
    s.admin.fetchTopicMetadata.mockResolvedValue({
      topics:
        kind === 'empty'
          ? []
          : [checkpointTopic, deliveryTopic, kind === 'duplicate' ? deliveryTopic : 'other'].map(
              (name) => ({ name, partitions: [{ partitionId: 0 }] }),
            ),
    });
    await expect(s.runtime.start()).rejects.toThrow('every expected topic exactly once');
    expect(s.kafka.consumer).not.toHaveBeenCalled();
  });

  it('rejects a delivery topic with no partitions', async () => {
    const s = setup();
    s.admin.fetchTopicMetadata.mockImplementation(async ({ topics }) => ({
      topics: topics.map((name) => ({
        name,
        partitions: name === deliveryTopic ? [] : [{ partitionId: 0 }],
      })),
    }));
    await expect(s.runtime.start()).rejects.toThrow('single-partition');
    expect(s.kafka.consumer).not.toHaveBeenCalled();
  });

  it('serializes dynamic discovery and deduplicates initial and newly discovered Sessions', async () => {
    const s = setup();
    await s.runtime.start();
    const discovered = {
      ...route,
      sessionId: 'ses_discovered',
      topic: `orca.${route.workspaceId}.sessions.ses_discovered.events`,
    };
    const added = mockConsumer();
    s.kafka.consumer.mockReturnValueOnce(added);
    await Promise.all([
      s.runtime.addSessions([route, discovered, discovered]),
      s.runtime.addSessions([discovered]),
    ]);
    expect(s.kafka.consumer).toHaveBeenCalledTimes(3);
    expect(added.subscribe).toHaveBeenCalledWith({ topic: discovered.topic, fromBeginning: true });
    expect(s.admin.fetchTopicMetadata).toHaveBeenCalledTimes(2);
    await s.runtime.stop();
    expect(added.disconnect).toHaveBeenCalledOnce();
  });

  it('rejects discovered multi-partition topics before starting a consumer', async () => {
    const s = setup();
    await s.runtime.start();
    const discovered = {
      ...route,
      sessionId: 'ses_multi',
      topic: `orca.${route.workspaceId}.sessions.ses_multi.events`,
    };
    s.admin.fetchTopicMetadata.mockResolvedValueOnce({
      topics: [{ name: discovered.topic, partitions: [{ partitionId: 0 }, { partitionId: 1 }] }],
    });
    await expect(s.runtime.addSessions([discovered])).rejects.toThrow('single-partition');
    expect(s.kafka.consumer).toHaveBeenCalledTimes(2);
    expect(s.runtime.status().state).toBe('failed');
  });

  it('drains a Session connection that finishes after shutdown begins', async () => {
    const s = setup();
    await s.runtime.start();
    const discovered = {
      ...route,
      sessionId: 'ses_late',
      topic: `orca.${route.workspaceId}.sessions.ses_late.events`,
    };
    const added = mockConsumer();
    const connected = deferred<void>();
    added.connect.mockImplementation(() => connected.promise);
    s.kafka.consumer.mockReturnValueOnce(added);
    const addition = s.runtime.addSessions([discovered]);
    await vi.waitFor(() => expect(added.connect).toHaveBeenCalledOnce());
    const failed = expect(addition).rejects.toThrow();
    const stopped = s.runtime.stop();
    connected.resolve();
    await failed;
    await stopped;
    expect(added.subscribe).not.toHaveBeenCalled();
    expect(added.disconnect).toHaveBeenCalledOnce();
    expect(s.runtime.status().state).toBe('stopped');
  });

  it('atomically sends delivery, checkpoint and source offsets before commit and only then resolves source messages', async () => {
    const s = setup();
    await s.runtime.start();
    const batch = batchPayload();
    const expected = projectKafkaEvents(
      initialKafkaCheckpoint(route, context),
      completedPrimaryTurnEvents().map((event) => ({ offset: String(event.seq), event })),
    );
    const commitEntered = deferred<void>();
    const commit = deferred<void>();
    s.transaction.commit.mockImplementation(async () => {
      s.order.push('commit');
      commitEntered.resolve();
      await commit.promise;
    });
    batch.resolveOffset.mockImplementation((offset) => {
      s.order.push(`resolve:${offset}`);
    });
    const processing = s.source.process(batch);
    await commitEntered.promise;
    try {
      expect(s.order).toEqual([
        'transaction',
        `send:${deliveryTopic}`,
        `send:${checkpointTopic}`,
        'sendOffsets',
        'commit',
      ]);
      expect(batch.resolveOffset).not.toHaveBeenCalled();
      expect(expected.deliveries).toHaveLength(1);
      expect(s.transaction.send).toHaveBeenNthCalledWith(1, {
        topic: deliveryTopic,
        messages: expected.deliveries.map((record) => ({
          key: record.trace.traceId,
          value: JSON.stringify(record),
        })),
      });
      const stateSend = s.transaction.send.mock.calls[1]![0];
      const checkpointKey = kafkaSourceGroupId(namespace, route.topic);
      expect(stateSend.topic).toBe(checkpointTopic);
      expect(stateSend.messages.every((message) => message.partition === 0)).toBe(true);
      expect(stateSend.messages.at(-1)!.key).toBe(checkpointKey);
      expectV2Head(
        parseKafkaStateHead(String(stateSend.messages.at(-1)!.value), route),
        expected.checkpoint,
      );
      const keys = kafkaStateKeys(checkpointKey);
      for (const identity of expected.checkpoint.identities) {
        const message = stateSend.messages.find(
          (entry) => entry.key === keys.identity(identity.key),
        );
        expect(JSON.parse(String(message!.value))).toEqual({
          ...identity,
          visibleOffset: (BigInt(identity.offset) + 1n).toString(),
          imported: false,
        });
      }
      expect(s.head().nextOffset).toBe('0');
      expect(s.sharedState.waitForOffset).not.toHaveBeenCalled();
      expect(s.transaction.sendOffsets).toHaveBeenCalledWith({
        consumerGroupId: kafkaSourceGroupId(namespace, route.topic),
        topics: [
          {
            topic: route.topic,
            partitions: [{ partition: 0, offset: expected.checkpoint.nextOffset }],
          },
        ],
      });
    } finally {
      commit.resolve();
    }
    await processing;
    expectV2State(s, expected.checkpoint);
    expect(s.sharedState.waitForOffset).toHaveBeenCalledOnce();
    expect(s.order.slice(5)).toEqual(
      batch.batch.messages.map((message) => `resolve:${message.offset}`),
    );
    expect(s.transaction.abort).not.toHaveBeenCalled();
    expect(s.source.commitOffsets).not.toHaveBeenCalled();
    expect(s.registryClient.resolveContext).not.toHaveBeenCalled();
    expect(s.source.run).toHaveBeenCalledWith(
      expect.objectContaining({ autoCommit: false, eachBatchAutoResolve: false }),
    );
    expect(s.kafka.consumer).toHaveBeenCalledWith(
      expect.objectContaining({ readUncommitted: false }),
    );
  });

  it('waits for committed v2 rows to reach the shared index before resolving source offsets', async () => {
    const s = setup();
    await s.runtime.start();
    s.holdIndex();
    const batch = batchPayload();
    const work = s.source.process(batch);
    try {
      await vi.waitFor(() => expect(s.sharedState.waitForOffset).toHaveBeenCalledOnce());
      expect(s.transaction.commit).toHaveBeenCalledOnce();
      expect(s.head().nextOffset).toBe('0');
      expect(s.head().identityCount).toBe(0);
      expect(batch.resolveOffset).not.toHaveBeenCalled();
    } finally {
      s.applyCommitted();
      await work;
    }
    expectV2State(
      s,
      projectKafkaEvents(
        initialKafkaCheckpoint(route, context),
        completedPrimaryTurnEvents().map((event) => ({ offset: String(event.seq), event })),
      ).checkpoint,
    );
    expect(batch.resolveOffset.mock.calls.flat()).toEqual(
      batch.batch.messages.map((message) => message.offset),
    );
  });

  it.each([deliveryTopic, checkpointTopic])(
    'aborts a failed send to %s without resolving source offsets',
    async (failedTopic) => {
      const s = setup();
      await s.runtime.start();
      const failure = new Error('injected send failure');
      s.transaction.send.mockImplementation(async ({ topic }) => {
        s.order.push(`send:${topic}`);
        if (topic === failedTopic) throw failure;
        return [];
      });
      const batch = batchPayload();
      await expect(s.source.process(batch)).rejects.toBe(failure);
      expect(s.transaction.abort).toHaveBeenCalledTimes(1);
      expect(s.order.at(-1)).toBe('abort');
      expect(s.transaction.sendOffsets).not.toHaveBeenCalled();
      expect(s.transaction.commit).not.toHaveBeenCalled();
      expect(batch.resolveOffset).not.toHaveBeenCalled();
      expect(s.source.commitOffsets).not.toHaveBeenCalled();
      expect(s.head().nextOffset).toBe('0');
      expect(s.head().identityCount).toBe(0);
      expect(
        await s.index.countPrefix(
          kafkaStateKeys(kafkaSourceGroupId(namespace, route.topic)).identityPrefix,
        ),
      ).toBe(0);
      expect(s.runtime.status()).toEqual({ ready: false, state: 'failed' });
    },
  );

  it.each(['REBALANCE_IN_PROGRESS', 'ILLEGAL_GENERATION', 'UNKNOWN_MEMBER_ID'])(
    'lets KafkaJS rejoin after %s without failing the runtime',
    async (type) => {
      const s = setup();
      await s.runtime.start();
      const error = new KafkaJSProtocolError(
        Object.assign(new Error('assignment changed'), { type, code: 27 }),
      );
      s.transaction.sendOffsets.mockRejectedValueOnce(error);
      const batch = batchPayload();
      await expect(s.source.process(batch)).rejects.toBe(error);
      expect(s.transaction.abort).toHaveBeenCalledTimes(1);
      expect(s.transaction.commit).not.toHaveBeenCalled();
      expect(batch.resolveOffset).not.toHaveBeenCalled();
      expect(s.producers[0]!.disconnect).toHaveBeenCalledTimes(1);
      expect(s.runtime.status()).toEqual({ ready: true, state: 'running' });
      expect(s.delivery.disconnect).not.toHaveBeenCalled();
      s.source.join();
      await s.source.process(batchPayload());
      expect(restoreKafkaCheckpoint).toHaveBeenCalledTimes(2);
      expect(s.kafka.producer).toHaveBeenCalledTimes(2);
      expect(s.kafka.producer.mock.calls[0]).toEqual(s.kafka.producer.mock.calls[1]);
      expect(s.transaction.commit).toHaveBeenCalledTimes(1);
    },
  );

  it('reuses deterministic transactional IDs across restart while isolating topics and namespaces', async () => {
    const first = setup();
    await first.runtime.start();
    await first.source.process(batchPayload());
    await first.runtime.stop();
    const second = setup();
    await second.runtime.start();
    await second.source.process(batchPayload());
    expect(first.kafka.producer.mock.calls).toEqual(second.kafka.producer.mock.calls);
    expect(second.kafka.producer).toHaveBeenCalledWith({
      transactionalId: kafkaSourceTransactionId(namespace, route.topic),
      idempotent: true,
      maxInFlightRequests: 1,
      allowAutoTopicCreation: false,
      transactionTimeout: 60_000,
    });
    expect(kafkaSourceTransactionId(namespace, route.topic)).toBe(
      `${kafkaSourceGroupId(namespace, route.topic)}-partition-0-v1`,
    );
    expect(kafkaSourceTransactionId('other', route.topic)).not.toBe(
      kafkaSourceTransactionId(namespace, route.topic),
    );
    expect(kafkaSourceTransactionId(namespace, `${route.topic}-other`)).not.toBe(
      kafkaSourceTransactionId(namespace, route.topic),
    );
  });

  it('cancels delivery on assignment loss without completing HTTP or failing the instance', async () => {
    vi.useFakeTimers();
    const s = setup();
    await s.runtime.start();
    const record = projectKafkaEvents(
      initialKafkaCheckpoint(route, context),
      completedPrimaryTurnEvents().map((event) => ({ offset: String(event.seq), event })),
    ).deliveries[0]!;
    const batch = batchPayload(
      [{ ...wire(completedPrimaryTurnEvents()[0]!), value: Buffer.from(JSON.stringify(record)) }],
      deliveryTopic,
    );
    const started = deferred<AbortSignal>();
    vi.mocked(deliverKafkaTrace).mockImplementation((_record, _options, signal) => {
      started.resolve(signal!);
      return new Promise((_resolve, reject) => {
        signal!.addEventListener('abort', () => reject(signal!.reason), { once: true });
      });
    });
    const processing = s.delivery.process(batch);
    const signal = await started.promise;
    expect(s.transaction.commit).toHaveBeenCalledOnce();
    s.transaction.send.mockClear();
    s.transaction.sendOffsets.mockClear();
    s.transaction.commit.mockClear();
    batch.isStale.mockReturnValue(true);
    await vi.advanceTimersByTimeAsync(1000);
    await processing;
    expect(signal.aborted).toBe(true);
    expect(s.transaction.send).not.toHaveBeenCalled();
    expect(s.transaction.sendOffsets).not.toHaveBeenCalled();
    expect(s.transaction.commit).not.toHaveBeenCalled();
    expect(s.delivery.commitOffsets).not.toHaveBeenCalled();
    expect(batch.resolveOffset).not.toHaveBeenCalled();
    expect(s.runtime.status()).toEqual({ ready: true, state: 'running' });
    expect(vi.getTimerCount()).toBe(1); // Runtime diagnostic interval, not a delivery retry.
  });
});

describe('Kafka delivery progress', () => {
  it.each(['terminal', 'retry', 'assignment lost'] as const)(
    'isolates concurrent partitions while slow HTTP settles as %s',
    async (outcome) => {
      const s = setup();
      // Model independent Kafka producers: no transaction object is shared across partitions.
      const makeProducer = s.kafka.producer.getMockImplementation()!;
      const transactions: (typeof s.transaction)[] = [];
      s.kafka.producer.mockImplementation(() => {
        const producer = makeProducer();
        const transaction = {
          ...s.transaction,
          send: vi.fn(s.transaction.send),
          sendOffsets: vi.fn(s.transaction.sendOffsets),
          commit: vi.fn(s.transaction.commit),
        };
        transactions.push(transaction);
        producer.transaction.mockResolvedValue(transaction);
        return producer;
      });
      s.admin.fetchTopicOffsets.mockResolvedValue([
        { partition: 0, low: '0', high: '100' },
        { partition: 1, low: '0', high: '100' },
      ]);
      s.admin.fetchTopicMetadata.mockImplementation(async ({ topics }) => ({
        topics: topics.map((name) => ({
          name,
          partitions:
            name === deliveryTopic
              ? [{ partitionId: 0 }, { partitionId: 1 }]
              : [{ partitionId: 0 }],
        })),
      }));
      await s.runtime.start();
      expect(s.delivery.run).toHaveBeenCalledWith(
        expect.objectContaining({ partitionsConsumedConcurrently: 8 }),
      );
      expect(s.source.run.mock.calls[0]![0].partitionsConsumedConcurrently).toBeUndefined();

      const entered = deferred<void>();
      const response = deferred<Awaited<ReturnType<typeof deliverKafkaTrace>>>();
      vi.mocked(deliverKafkaTrace).mockImplementationOnce(() => {
        entered.resolve();
        return response.promise;
      });
      const slow = deliveryBatch(['0', '1']);
      const fast = deliveryBatch();
      fast.batch.partition = 1;
      const pending = s.delivery.process(slow);
      try {
        await entered.promise;
        await s.delivery.process(fast);
        expect(fast.resolveOffset).toHaveBeenCalledWith('0');
        expect(slow.resolveOffset).not.toHaveBeenCalled();
        expect(deliverKafkaTrace).toHaveBeenCalledTimes(2);
        expect(s.kafka.producer).toHaveBeenCalledTimes(2);
        for (const partition of [0, 1]) {
          expect(s.kafka.producer).toHaveBeenNthCalledWith(
            partition + 1,
            expect.objectContaining({
              transactionalId: kafkaDeliveryProgressKey(namespace, deliveryTopic, partition),
            }),
          );
        }
        expect(s.producers[0]!.transaction).toHaveBeenCalledTimes(1);
        expect(s.producers[1]!.transaction).toHaveBeenCalledTimes(2);
        expect(transactions[0]!.commit).toHaveBeenCalledTimes(1);
        expect(transactions[1]!.commit).toHaveBeenCalledTimes(2);
        for (const partition of [0, 1]) {
          expect(transactions[partition]!.send).toHaveBeenLastCalledWith({
            topic: checkpointTopic,
            messages: [
              {
                partition: 0,
                key: kafkaDeliveryProgressKey(namespace, deliveryTopic, partition),
                value: JSON.stringify({
                  version: 1,
                  topic: deliveryTopic,
                  partition,
                  groupId: namespace + '-delivery-v1',
                  nextOffset: String(partition),
                }),
              },
            ],
          });
        }
        expect(s.transaction.sendOffsets).toHaveBeenLastCalledWith({
          consumerGroupId: namespace + '-delivery-v1',
          topics: [{ topic: deliveryTopic, partitions: [{ partition: 1, offset: '1' }] }],
        });
        if (outcome === 'assignment lost') slow.isStale.mockReturnValue(true);
      } finally {
        response.resolve(
          outcome === 'retry'
            ? { kind: 'retry', retryAfterMs: 1000 }
            : { kind: 'terminal', reason: 'accepted' },
        );
        await pending;
      }
      expect(slow.resolveOffset.mock.calls).toEqual(outcome === 'terminal' ? [['0'], ['1']] : []);
      expect(slow.pause).toHaveBeenCalledTimes(outcome === 'retry' ? 1 : 0);
      expect(fast.pause).not.toHaveBeenCalled();
      expect(s.producers[1]!.disconnect).not.toHaveBeenCalled();
      expect(s.runtime.status()).toEqual({ ready: true, state: 'running' });
    },
  );

  const key = kafkaDeliveryProgressKey(namespace, deliveryTopic, 0);
  const groupId = namespace + '-delivery-v1';
  const progress = (nextOffset: string) => ({
    version: 1,
    topic: deliveryTopic,
    partition: 0,
    groupId,
    nextOffset,
  });

  it('initializes progress and offset zero before HTTP, then commits terminal progress before resolving', async () => {
    const s = setup();
    await s.runtime.start();
    const batch = deliveryBatch();
    const entered = deferred<void>();
    const outcome = deferred<Awaited<ReturnType<typeof deliverKafkaTrace>>>();
    vi.mocked(deliverKafkaTrace).mockImplementation(() => {
      s.order.push('http');
      entered.resolve();
      return outcome.promise;
    });
    batch.resolveOffset.mockImplementation((offset) => s.order.push(`resolve:${offset}`));
    const processing = s.delivery.process(batch);
    await entered.promise;
    expect(s.kafka.producer).toHaveBeenCalledWith({
      transactionalId: key,
      idempotent: true,
      maxInFlightRequests: 1,
      allowAutoTopicCreation: false,
      transactionTimeout: 60_000,
    });
    expect(restoreKafkaValue).toHaveBeenCalledWith(
      expect.objectContaining({
        key,
        topic: checkpointTopic,
        producer: s.producers[0],
      }),
    );
    expect(s.order).toEqual([
      'transaction',
      `send:${checkpointTopic}`,
      'sendOffsets',
      'commit',
      'http',
    ]);
    expect(s.transaction.send).toHaveBeenLastCalledWith({
      topic: checkpointTopic,
      messages: [{ partition: 0, key, value: JSON.stringify(progress('0')) }],
    });
    expect(s.transaction.sendOffsets).toHaveBeenLastCalledWith({
      consumerGroupId: groupId,
      topics: [{ topic: deliveryTopic, partitions: [{ partition: 0, offset: '0' }] }],
    });
    expect(batch.resolveOffset).not.toHaveBeenCalled();
    const committing = deferred<void>();
    const committed = deferred<void>();
    s.transaction.commit.mockImplementation(async () => {
      s.order.push('commit');
      committing.resolve();
      await committed.promise;
    });
    outcome.resolve({ kind: 'terminal', reason: 'partial_rejection' });
    await committing.promise;
    expect(batch.resolveOffset).not.toHaveBeenCalled();
    expect(s.transaction.send).toHaveBeenLastCalledWith({
      topic: checkpointTopic,
      messages: [{ partition: 0, key, value: JSON.stringify(progress('1')) }],
    });
    expect(s.transaction.sendOffsets).toHaveBeenLastCalledWith({
      consumerGroupId: groupId,
      topics: [{ topic: deliveryTopic, partitions: [{ partition: 0, offset: '1' }] }],
    });
    committed.resolve();
    await processing;
    expect(s.order.slice(5)).toEqual([
      'transaction',
      `send:${checkpointTopic}`,
      'sendOffsets',
      'commit',
      'resolve:0',
    ]);
    expect(s.delivery.commitOffsets).not.toHaveBeenCalled();
  });

  it('repairs a missing group from advanced progress and skips already delivered messages', async () => {
    vi.mocked(restoreKafkaValue).mockResolvedValue({ nextOffset: '2' });
    const s = setup();
    await s.runtime.start();
    const batch = deliveryBatch(['0', '1', '2']);
    vi.mocked(deliverKafkaTrace).mockImplementation(async () => {
      expect(batch.resolveOffset.mock.calls).toEqual([['0'], ['1']]);
      expect(s.transaction.sendOffsets).toHaveBeenLastCalledWith({
        consumerGroupId: groupId,
        topics: [{ topic: deliveryTopic, partitions: [{ partition: 0, offset: '2' }] }],
      });
      return { kind: 'terminal', reason: 'accepted' };
    });
    await s.delivery.process(batch);
    expect(deliverKafkaTrace).toHaveBeenCalledOnce();
    expect(batch.resolveOffset.mock.calls).toEqual([['0'], ['1'], ['2']]);
    expect(s.transaction.send).toHaveBeenLastCalledWith({
      topic: checkpointTopic,
      messages: [{ partition: 0, key, value: JSON.stringify(progress('3')) }],
    });
  });

  it('fails closed when group offsets advanced but durable progress is missing', async () => {
    const s = setup();
    s.admin.fetchOffsets.mockResolvedValue([
      {
        topic: deliveryTopic,
        partitions: [{ partition: 0, offset: '2' }],
      },
    ]);
    await s.runtime.start();
    const batch = deliveryBatch(['2']);
    await expect(s.delivery.process(batch)).rejects.toThrow(
      'Kafka delivery progress/source offset mismatch',
    );
    expect(deliverKafkaTrace).not.toHaveBeenCalled();
    expect(s.transaction.sendOffsets).not.toHaveBeenCalled();
    expect(s.transaction.commit).not.toHaveBeenCalled();
    expect(batch.resolveOffset).not.toHaveBeenCalled();
    expect(s.runtime.status().state).toBe('failed');
  });

  it.each(
    (['valid', 'stale', 'generation changed'] as const).flatMap((ownership) =>
      [true, false].map((partial) => ({ ownership, partial })),
    ),
  )(
    'handles real HTTP EOF shutdown with ownership $ownership and partial=$partial',
    async ({ ownership, partial }) => {
      const actual = await vi.importActual<typeof import('../../src/kafka-delivery.js')>(
        '../../src/kafka-delivery.js',
      );
      vi.mocked(deliverKafkaTrace).mockImplementation(actual.deliverKafkaTrace);
      let stopped: Promise<void> | undefined;
      const fetchImpl = vi.fn<typeof fetch>(
        async () =>
          new Response(
            new ReadableStream({
              start(stream) {
                stream.enqueue(
                  new TextEncoder().encode(
                    partial ? '{"partialSuccess":{"rejectedSpans":"1"}}' : '{}',
                  ),
                );
              },
              pull(stream) {
                if (ownership === 'stale') batch.isStale.mockReturnValue(true);
                if (ownership === 'generation changed') s.delivery.join();
                stopped = s.runtime.stop();
                stream.close();
              },
            }),
            { headers: { 'content-type': 'application/json' } },
          ),
      );
      const s = setup(fetchImpl);
      const batch = deliveryBatch();
      s.registryClient.resolveSecret.mockResolvedValue({
        bindingId: context.bindingId,
        bindingVersion: 1,
        effectiveCaptureMode: 'metadata_only',
        auth: { type: 'basic', username: 'public-key', password: 'secret-key' },
      });
      await s.runtime.start();
      await s.delivery.process(batch);
      expect(stopped).toBeDefined();
      await stopped;
      expect(fetchImpl).toHaveBeenCalledOnce();
      await expect(vi.mocked(deliverKafkaTrace).mock.results[0]!.value).resolves.toEqual({
        kind: 'terminal',
        reason: partial ? 'partial_rejection' : 'accepted',
      });
      expect(s.transaction.commit).toHaveBeenCalledTimes(ownership === 'valid' ? 2 : 1);
      expect(s.transaction.send).toHaveBeenLastCalledWith({
        topic: checkpointTopic,
        messages: [
          { partition: 0, key, value: JSON.stringify(progress(ownership === 'valid' ? '1' : '0')) },
        ],
      });
      expect(batch.resolveOffset.mock.calls).toEqual(ownership === 'valid' ? [['0']] : []);
      expect(s.runtime.status().state).toBe('stopped');
    },
  );
});

describe('shutdown failure classification and drain', () => {
  it.each(['reader', 'consumer'] as const)(
    'classifies a late %s callback after intentional consumer disconnect',
    async (kind) => {
      const onDiagnostic = vi.fn();
      const s = setup(undefined, undefined, route, { onDiagnostic });
      await s.runtime.start();
      const gate = deferred<void>();
      s.sharedState.close.mockImplementationOnce(() => gate.promise);
      const stopped = s.runtime.stop();
      await vi.waitFor(() => expect(s.sharedState.close).toHaveBeenCalledOnce());
      const error = new Error('private credentials must not be logged');
      if (kind === 'reader') {
        vi.mocked(KafkaSharedState).mock.calls.at(-1)![0].onFailure!(error);
      } else {
        // The source has already intentionally disconnected, so this late callback is harmless.
        s.source.crash(error);
      }
      gate.resolve();
      await stopped;
      expect(s.runtime.status().state).toBe(kind === 'reader' ? 'failed' : 'stopped');
      expect(JSON.stringify(onDiagnostic.mock.calls)).not.toContain(error.message);
    },
  );

  it.each(['AbortError', 'Error'])(
    'classifies late startup rejection %s during shutdown',
    async (name) => {
      const s = setup();
      const connected = deferred<void>();
      s.source.connect.mockImplementationOnce(() => connected.promise);
      const start = s.runtime.start().catch((error: unknown) => error);
      await vi.waitFor(() => expect(s.source.connect).toHaveBeenCalledOnce());
      const stopped = s.runtime.stop();
      const error = new Error('private startup failure');
      error.name = name;
      connected.reject(error);
      expect(await start).toBe(error);
      await stopped;
      expect(s.runtime.status().state).toBe(name === 'AbortError' ? 'stopped' : 'failed');
      expect(s.admin.disconnect).toHaveBeenCalledOnce();
    },
  );

  it('drains real inline credential resolution that ignores cancellation', async () => {
    const actual = await vi.importActual<typeof import('../../src/kafka-delivery.js')>(
      '../../src/kafka-delivery.js',
    );
    vi.mocked(deliverKafkaTrace).mockImplementation(actual.deliverKafkaTrace);
    const fetchImpl = vi.fn<typeof fetch>();
    const s = setup(fetchImpl);
    const credentials = deferred<never>();
    s.registryClient.resolveSecret.mockImplementationOnce(() => credentials.promise);
    await s.runtime.start();
    const batch = deliveryBatch();
    const work = s.delivery.process(batch);
    await vi.waitFor(() => expect(s.registryClient.resolveSecret).toHaveBeenCalledOnce());
    vi.useFakeTimers();
    let done = false;
    const stopped = s.runtime.stop().then(() => {
      done = true;
    });
    await vi.advanceTimersByTimeAsync(1_001);
    expect(done).toBe(false);
    expect(s.delivery.disconnect).not.toHaveBeenCalled();
    expect(s.sharedState.close).not.toHaveBeenCalled();
    credentials.reject(new Error('credential request settled after abort'));
    await Promise.all([work, stopped]);
    expect(fetchImpl).not.toHaveBeenCalled();
    expect(batch.resolveOffset).not.toHaveBeenCalled();
    expect(s.runtime.status().state).toBe('stopped');
    vi.useRealTimers();
  });

  it('closes admin even if shared-state cleanup rejects, without an unhandled background stop', async () => {
    const s = setup();
    await s.runtime.start();
    const error = new Error('private cleanup failure');
    s.sharedState.close.mockRejectedValue(error);
    s.source.crash(new Error('fatal reader'));
    await expect(s.runtime.stop()).rejects.toBe(error);
    expect(s.admin.disconnect).toHaveBeenCalledOnce();
    expect(s.runtime.status().state).toBe('failed');
    s.sharedState.close.mockResolvedValue(undefined);
    runtimes.splice(runtimes.indexOf(s.runtime), 1);
  });

  it.each(
    (['inline', 'manifest'] as const).flatMap((format) =>
      (['terminal', 'fatal', 'consumer fatal'] as const).map((outcome) => ({ format, outcome })),
    ),
  )(
    'drains $format signal-ignoring delivery work ending in $outcome',
    async ({ format, outcome }) => {
      const s = setup();
      await s.runtime.start();
      const gate = deferred<{ kind: 'terminal'; reason: 'delivered' }>();
      vi.mocked(deliverKafkaTrace).mockImplementationOnce(() => gate.promise);
      const batch = deliveryBatch();
      if (format === 'manifest') {
        const checkpointKey = kafkaSourceGroupId(namespace, route.topic);
        s.rows.set(checkpointKey, JSON.stringify(initialKafkaCheckpoint(route, context)));
        const encoded = encodeKafkaDelivery(
          JSON.parse(batch.batch.messages[0]!.value!.toString()),
          checkpointKey,
          { maxRecordBytes: 2_048 },
        );
        expect(encoded.chunks.length).toBeGreaterThan(0);
        for (const chunk of encoded.chunks) s.rows.set(chunk.key, chunk.value);
        batch.batch.messages[0]!.value = Buffer.from(encoded.delivery.value);
      }
      const work = s.delivery.process(batch).catch((error: unknown) => error);
      await vi.waitFor(() => expect(deliverKafkaTrace).toHaveBeenCalledOnce());
      vi.useFakeTimers();
      let done = false;
      const stopped = s.runtime.stop().then(() => {
        done = true;
      });
      await vi.advanceTimersByTimeAsync(1_001);
      expect(done).toBe(false);
      expect(s.sharedState.close).not.toHaveBeenCalled();
      expect(s.delivery.disconnect).not.toHaveBeenCalled();
      if (outcome === 'consumer fatal') s.delivery.crash(new Error('late consumer fatal'));
      if (outcome === 'fatal') gate.reject(new Error('late private transport failure'));
      else gate.resolve({ kind: 'terminal', reason: 'delivered' });
      await Promise.all([work, stopped]);
      expect(s.runtime.status().state).toBe(outcome === 'terminal' ? 'stopped' : 'failed');
      // Beyond the classification grace period no offset is completed, even if I/O later succeeds.
      expect(batch.resolveOffset).not.toHaveBeenCalled();
      vi.useRealTimers();
    },
  );
});

describe('withKafkaDeliveryHeartbeat', () => {
  it('preserves terminal settlement after shutdown within the one-second grace period', async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const work = deferred<string>();
    const result = withKafkaDeliveryHeartbeat(
      batchPayload([]),
      controller.signal,
      () => work.promise,
      (outcome) => outcome === 'terminal',
    );
    controller.abort();
    await vi.advanceTimersByTimeAsync(999);
    work.resolve('terminal');
    await expect(result).resolves.toBe('terminal');
    expect(vi.getTimerCount()).toBe(0);
  });

  it('bounds shutdown settlement to one second even if the operation ignores abort', async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const reason = new Error('shutdown');
    const result = withKafkaDeliveryHeartbeat(
      batchPayload([]),
      controller.signal,
      () => new Promise<never>(() => {}),
      () => true,
    );
    const rejected = expect(result).rejects.toBe(reason);
    controller.abort(reason);
    await vi.advanceTimersByTimeAsync(1000);
    await rejected;
    expect(vi.getTimerCount()).toBe(0);
  });

  it('does not preserve a retry outcome after shutdown', async () => {
    const controller = new AbortController();
    const reason = new Error('shutdown');
    const work = deferred<string>();
    const result = withKafkaDeliveryHeartbeat(
      batchPayload([]),
      controller.signal,
      () => work.promise,
      (outcome) => outcome === 'terminal',
    );
    const rejected = expect(result).rejects.toBe(reason);
    controller.abort(reason);
    work.resolve('retry');
    await rejected;
  });

  it.each(['stale', 'stopped', 'heartbeat failure'] as const)(
    'never preserves terminal settlement after lease loss: %s',
    async (loss) => {
      vi.useFakeTimers();
      const batch = batchPayload([]);
      const work = deferred<string>();
      const result = withKafkaDeliveryHeartbeat(
        batch,
        new AbortController().signal,
        (signal) => {
          signal.addEventListener('abort', () => work.resolve('terminal'), { once: true });
          return work.promise;
        },
        () => true,
      );
      if (loss === 'stale') batch.isStale.mockReturnValue(true);
      if (loss === 'stopped') batch.isRunning.mockReturnValue(false);
      if (loss === 'heartbeat failure') batch.heartbeat.mockRejectedValue(new Error('rebalance'));
      const rejected = expect(result).rejects.toThrow(
        loss === 'heartbeat failure' ? 'rebalance' : 'assignment lost',
      );
      await vi.advanceTimersByTimeAsync(1000);
      await rejected;
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it('heartbeats every second during a slow operation and clears its timer on success', async () => {
    vi.useFakeTimers();
    const batch = batchPayload([]);
    const work = deferred<string>();
    const operation = vi.fn(() => work.promise);
    const result = withKafkaDeliveryHeartbeat(batch, new AbortController().signal, operation);
    await vi.advanceTimersByTimeAsync(3500);
    expect(batch.heartbeat).toHaveBeenCalledTimes(3);
    expect(operation).toHaveBeenCalledTimes(1);
    work.resolve('accepted');
    await expect(result).resolves.toBe('accepted');
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(5000);
    expect(batch.heartbeat).toHaveBeenCalledTimes(3);
  });

  it.each(['stale', 'stopped', 'heartbeat failure'] as const)(
    'cancels a pending operation on %s even if it ignores cancellation',
    async (loss) => {
      vi.useFakeTimers();
      const batch = batchPayload([]);
      let signal!: AbortSignal;
      const result = withKafkaDeliveryHeartbeat(batch, new AbortController().signal, (value) => {
        signal = value;
        return new Promise(() => {});
      });
      const failure = new KafkaJSProtocolError(
        Object.assign(new Error('rebalance'), { type: 'REBALANCE_IN_PROGRESS', code: 27 }),
      );
      if (loss === 'stale') batch.isStale.mockReturnValue(true);
      if (loss === 'stopped') batch.isRunning.mockReturnValue(false);
      if (loss === 'heartbeat failure') batch.heartbeat.mockRejectedValue(failure);
      const rejected = expect(result).rejects.toThrow(
        loss === 'heartbeat failure' ? 'rebalance' : 'assignment lost',
      );
      await vi.advanceTimersByTimeAsync(1000);
      await rejected;
      expect(signal.aborted).toBe(true);
      if (loss === 'heartbeat failure') expect(signal.reason).toBe(failure);
      else expect(batch.heartbeat).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it('propagates shutdown immediately and skips work when already aborted', async () => {
    vi.useFakeTimers();
    const batch = batchPayload([]);
    const controller = new AbortController();
    const reason = new Error('shutdown');
    let signal!: AbortSignal;
    const operation = vi.fn((value: AbortSignal) => {
      signal = value;
      return new Promise<never>(() => {});
    });
    const result = withKafkaDeliveryHeartbeat(batch, controller.signal, operation);
    const rejected = expect(result).rejects.toBe(reason);
    controller.abort(reason);
    await rejected;
    expect(signal.reason).toBe(reason);
    await expect(withKafkaDeliveryHeartbeat(batch, controller.signal, operation)).rejects.toBe(
      reason,
    );
    expect(operation).toHaveBeenCalledTimes(1);
    expect(batch.heartbeat).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('clears the heartbeat timer when the operation fails', async () => {
    vi.useFakeTimers();
    const failure = new Error('delivery failed');
    await expect(
      withKafkaDeliveryHeartbeat(batchPayload([]), new AbortController().signal, async () => {
        throw failure;
      }),
    ).rejects.toBe(failure);
    expect(vi.getTimerCount()).toBe(0);
  });
});
