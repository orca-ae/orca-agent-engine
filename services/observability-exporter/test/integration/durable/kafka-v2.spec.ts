// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Event } from '@orca/transcript-store-types';
import {
  Kafka,
  KafkaJSDeleteGroupsError,
  logLevel,
  type Admin,
  type Consumer,
  type Producer,
  type EachBatchPayload,
} from 'kafkajs';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  KafkaOnlyObservabilityExporterRuntime,
  kafkaDeliveryProgressKey,
  kafkaSourceGroupId,
  kafkaSourceTransactionId,
} from '../../../src/kafka-runtime.js';
import {
  initialKafkaCheckpoint,
  projectKafkaEvents,
  type KafkaCheckpoint,
  type KafkaDeliveryRecord,
} from '../../../src/kafka-state.js';
import { kafkaStateKeys, type KafkaStateHead } from '../../../src/kafka-state-v2.js';
import { decodeKafkaBlob, kafkaBlobKeys, type KafkaBlobValue } from '../../../src/kafka-blob.js';
import { encodeLangfuseOtlpJson, type OtlpExportRequest } from '../../../src/otlp-json.js';
import { encodeKafkaDelivery, sendKafkaStateRecords } from '../../../src/kafka-state-io.js';
import { RegistryObservabilityClient } from '../../../src/registry-client.js';
import { TRANSCRIPT_SECRET, completedPrimaryTurnEvents, event } from '../../support/events.js';
import { waitForKafkaTopics } from '../../support/kafka.js';
import { basicRegistrySecret, enabledRegistryContext } from '../../support/registry.js';

const brokers = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);

// Real Kafka only; HTTP is the external boundary, never the state/projection implementation.
// Every restart uses a new scratch directory and the SAME source group/transactional ID.
describe('Kafka v2 migration and committed recovery', () => {
  let fixture: KafkaV2Fixture | undefined;
  afterEach(async () => {
    await fixture?.close();
    fixture = undefined;
  }, 60_000);

  it('migrates the persisted suppressed 3151 checkpoint atomically at the same offset and reaches 3459', async () => {
    const f = (fixture = new KafkaV2Fixture());
    await f.open();
    const events = Array.from({ length: 3459 }, (_, i) =>
      event(
        i,
        'session.status_running',
        {},
        {
          id: 'evt_regression_' + i,
          workspaceId: f.workspaceId,
          sessionId: f.sessionId,
        },
      ),
    );
    const route = { topic: f.topic, workspaceId: f.workspaceId, sessionId: f.sessionId };
    const legacy = projectKafkaEvents(
      initialKafkaCheckpoint(route, null),
      events.slice(0, 3151).map((event, i) => ({ event, offset: String(i) })),
    ).checkpoint;
    const raw = JSON.stringify(legacy);
    expect(Buffer.byteLength(raw)).toBeGreaterThan(510_000);
    expect(Buffer.byteLength(raw)).toBeLessThanOrEqual(524_288);
    // Persist source prefix AND suffix before seeding the old writer's checkpoint.
    // Never manufacture source offsets with admin.setOffsets or mutate broker limits.
    for (let i = 0; i < events.length; i += 100) await f.append(events.slice(i, i + 100));
    await seedLegacy(f, legacy);
    expect(await f.offset(f.sourceGroupId, f.topic)).toBe('3151');
    const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
    const producerSpy = vi.spyOn(f.kafka, 'producer');
    try {
      const runtime = f.runtime(send);
      await runtime.start();
      await f.waitForCheckpoint('3459');
      expect(f.checkpoint).toMatchObject({
        version: 2,
        identityCount: 3459,
        deliveryContext: null,
      });
      const imported = f.heads.find((head) => head.version === 2 && head.nextOffset === '3151');
      expect(imported).toMatchObject({
        identityCount: 3151,
        deliveryContext: null,
        importBaseline: { nextOffset: '3151', identityCount: 3151 },
      });
      expect(producerSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          transactionalId: kafkaSourceTransactionId(f.groupId, f.topic),
        }),
      );
      expect(Buffer.byteLength(JSON.stringify(f.checkpoint))).toBeLessThan(524_288);
      const prefix = kafkaStateKeys(f.sourceGroupId).identityPrefix;
      expect([...f.stateRecords.keys()].filter((key) => key.startsWith(prefix))).toHaveLength(3459);
      expect(f.contextRequests).not.toHaveBeenCalled();
      expect(f.secretRequests).not.toHaveBeenCalled();
      expect(send).not.toHaveBeenCalled();
      expect(f.deliveries).toHaveLength(0);
    } finally {
      producerSpy.mockRestore();
    }
  }, 120_000);

  it('cold rebuilds exact identity and accepted ledgers, ignores replay, and fails closed on a changed hash', async () => {
    const f = (fixture = new KafkaV2Fixture());
    await f.open();
    const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
    const first = f.runtime(send);
    await first.start();
    await f.append(f.events);
    await f.waitForCheckpoint('8');
    await f.waitForDelivery();
    expect(f.checkpoint).toMatchObject({ version: 2, identityCount: 8, acceptedCount: 1 });
    await first.stop();
    const restarted = f.runtime(send);
    await restarted.start();
    await f.append(f.events);
    await f.waitForCheckpoint('16');
    await restarted.stop();
    await f.waitForObservedTail(f.deliveryTopic);
    expect(f.runtimeDirectories[1]).not.toBe(f.runtimeDirectories[0]);
    expect(f.checkpoint).toMatchObject({ version: 2, identityCount: 8, acceptedCount: 1 });
    expect(f.contextRequests).toHaveBeenCalledTimes(1);
    expect(f.deliveries).toHaveLength(1);
    expect(send).toHaveBeenCalledTimes(1);
    expect(String(send.mock.calls[0]?.[1]?.body)).not.toContain(TRANSCRIPT_SECRET);
    const conflict = f.runtime(send);
    f.expectedFailures.add(conflict);
    await conflict.start();
    await f.append([{ ...f.events[0]!, payload: Buffer.from('{"content":"different hash"}') }]);
    await waitFor(
      'identity conflict',
      async () => conflict.status().state === 'failed',
      () => f.assertHealthy(),
    );
    await conflict.stop();
    await f.waitForObservedTail(f.checkpointTopic);
    expect(await f.offset(f.sourceGroupId, f.topic)).toBe('16');
    expect(f.checkpoint?.nextOffset).toBe('16');
    expect(send).toHaveBeenCalledTimes(1);
  }, 120_000);

  it('reloads committed v2 state after both source and delivery consumer groups are deleted', async () => {
    const f = (fixture = new KafkaV2Fixture());
    await f.open();
    const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
    const first = f.runtime(send);
    await first.start();
    await f.append(f.events);
    await f.waitForCheckpoint('8');
    await f.waitForDelivery();
    expect(f.checkpoint?.version).toBe(2);
    await first.stop();
    await f.admin.deleteGroups([f.sourceGroupId, f.deliveryGroupId]);
    expect(await f.offset(f.sourceGroupId, f.topic)).toBe('-1');
    expect(await f.offset(f.deliveryGroupId, f.deliveryTopic)).toBe('-1');
    const restarted = f.runtime(send);
    await restarted.start();
    const namespace = '_' + f.suffix + '_next';
    const next = completedPrimaryTurnEvents('model_observation_kind', namespace).map((event) => ({
      ...event,
      id: event.id.endsWith(namespace) ? event.id : event.id + namespace,
      idempotencyKey: event.idempotencyKey + namespace,
      workspaceId: f.workspaceId,
      sessionId: f.sessionId,
    }));
    await f.append(next);
    await f.waitForCheckpoint('16');
    await f.waitForDelivery(1);
    await restarted.stop();
    await f.waitForObservedTail(f.deliveryTopic);
    expect(f.checkpoint).toMatchObject({ version: 2, identityCount: 16, acceptedCount: 2 });
    expect(f.contextRequests).toHaveBeenCalledTimes(1);
    expect(f.deliveries).toHaveLength(2);
    expect(new Set(f.deliveries.map(({ record }) => record.trace.traceId)).size).toBe(2);
    expect(send).toHaveBeenCalledTimes(2);
  }, 120_000);

  it('aborts broker-written v2 ledger, delivery and head when source-offset send fails, then recovers', async () => {
    const f = (fixture = new KafkaV2Fixture());
    await f.open();
    const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
    const createProducer = f.kafka.producer.bind(f.kafka);
    const writtenKeys = new Set<string>();
    let aborted = false;
    let wroteHead = false;
    let deliveryOffset: string | undefined;
    const injection = vi.spyOn(f.kafka, 'producer').mockImplementation((config) => {
      const producer = createProducer(config);
      if (config?.transactionalId !== kafkaSourceTransactionId(f.groupId, f.topic)) return producer;
      const begin = producer.transaction.bind(producer);
      vi.spyOn(producer, 'transaction').mockImplementation(async () => {
        const tx = await begin();
        let hasDelivery = false;
        return {
          ...tx,
          send: async (record) => {
            const metadata = await tx.send(record);
            if (record.topic === f.checkpointTopic)
              for (const message of record.messages) {
                const key = String(message.key);
                if (/^[IARD]\//u.test(key)) writtenKeys.add(key);
                if (key === f.sourceGroupId) {
                  const head = JSON.parse(String(message.value)) as KafkaStateHead;
                  if (head.version === 2 && head.nextOffset === '8') wroteHead = true;
                }
              }
            if (record.topic === f.deliveryTopic) {
              hasDelivery = true;
              deliveryOffset = metadata[0]?.baseOffset;
            }
            return metadata;
          },
          sendOffsets: async (offsets) => {
            if (hasDelivery)
              throw new Error('injected offset failure after broker-written v2 state');
            await tx.sendOffsets(offsets);
          },
          abort: async () => {
            await tx.abort();
            if (hasDelivery) aborted = true;
          },
        };
      });
      return producer;
    });
    try {
      await f.append(f.events);
      // Force the otherwise small, real projected trace through the blob path.
      // The broker remains unchanged; this is a stricter exporter record budget.
      const failed = f.runtime(send, true, { maxDeliveryBytes: 2048 });
      f.expectedFailures.add(failed);
      await failed.start();
      await waitFor(
        'transaction abort',
        async () => failed.status().state === 'failed',
        () => f.assertHealthy(),
      );
      await failed.stop();
      expect(aborted).toBe(true);
      expect(wroteHead).toBe(true);
      expect(writtenKeys.size).toBeGreaterThanOrEqual(9);
      expect([...writtenKeys].some((key) => key.startsWith('D/'))).toBe(true);
      expect(deliveryOffset).toBeDefined();
      await f.waitForObservedTail(f.checkpointTopic);
      await f.waitForObservedTail(f.deliveryTopic);
      expect([...writtenKeys].filter((key) => f.stateRecords.has(key))).toEqual([]);
      // The separately committed initial head pins the session at offset zero.
      // The failed projection must leave that baseline (not its source batch) visible.
      expect(f.checkpoint).toMatchObject({ version: 2, nextOffset: '0', identityCount: 0 });
      expect(await f.offset(f.sourceGroupId, f.topic)).toBe('0');
      expect(f.deliveries).toHaveLength(0);
      expect(send).not.toHaveBeenCalled();
      injection.mockRestore();
      const restarted = f.runtime(send, true, { maxDeliveryBytes: 2048 });
      await restarted.start();
      await f.waitForCheckpoint('8');
      await f.waitForDelivery();
      expect(f.checkpoint).toMatchObject({ version: 2, identityCount: 8, acceptedCount: 1 });
      expect(BigInt(f.deliveries[0]!.offset)).toBeGreaterThan(BigInt(deliveryOffset!));
      expect(send).toHaveBeenCalledTimes(1);
    } finally {
      injection.mockRestore();
    }
  }, 120_000);

  it('restores a genuinely large reducer and delivers its chunked terminal trace after a cold restart', async () => {
    const f = (fixture = new KafkaV2Fixture());
    await f.open();
    const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
    const events = largeTurnEvents(f);
    const prefix = events.slice(0, -1);
    // Validate the generated fixture with the real pure projector before exercising
    // Kafka. This resolver call belongs to fixture construction, not the runtime.
    const context = await f.registryClient.resolveContext({
      workspaceId: f.workspaceId,
      sessionId: f.sessionId,
    });
    if (context.status !== 'enabled') throw new Error('fixture context must be enabled');
    f.contextRequests.mockClear();
    const oracle = projectKafkaEvents(
      initialKafkaCheckpoint(
        {
          topic: f.topic,
          workspaceId: f.workspaceId,
          sessionId: f.sessionId,
        },
        context.deliveryContext,
      ),
      events.map((event, i) => ({ event, offset: String(i) })),
    );
    expect(oracle.deliveries).toHaveLength(1);
    expect(Buffer.byteLength(JSON.stringify(oracle.deliveries[0]))).toBeGreaterThan(524_288);
    const first = f.runtime(send);
    await first.start();
    // Bound Produce batches rather than changing the broker's default record-batch limit.
    for (let i = 0; i < prefix.length; i += 100) await f.append(prefix.slice(i, i + 100));
    await f.waitForCheckpoint(String(prefix.length));
    expect(f.checkpoint?.version).toBe(2);
    expect(f.checkpoint?.reducer).toMatchObject({ kind: 'chunks' });
    expect(send).not.toHaveBeenCalled();
    expect(f.checkpoint!.deliveryContext).toEqual(context.deliveryContext);
    const stateBlob = f.checkpoint!.reducer;
    const reducer = decodeKafkaBlob(
      stateBlob,
      kafkaBlobKeys(stateBlob, kafkaStateKeys(f.sourceGroupId).reducerPrefix).map(
        (key) => f.stateRecords.get(key) ?? null,
      ),
    );
    expect(Buffer.byteLength(JSON.stringify(reducer))).toBeGreaterThan(524_288);
    await first.stop();
    const restarted = f.runtime(send);
    await restarted.start();
    await f.append(events.slice(-1));
    await f.waitForCheckpoint(String(events.length));
    await f.waitForDelivery();
    await restarted.stop();
    await f.waitForObservedTail(f.checkpointTopic);
    const queued = f.deliveries[0]!.record as unknown as {
      version: number;
      digest: string;
      blob: KafkaBlobValue;
    };
    expect(queued.version).toBe(2);
    const deliveryKeys = kafkaBlobKeys(
      queued.blob,
      kafkaStateKeys(f.sourceGroupId).deliveryPrefix + queued.digest,
    );
    expect(deliveryKeys.length).toBeGreaterThan(1);
    expect(
      decodeKafkaBlob(
        queued.blob,
        deliveryKeys.map((key) => f.stateRecords.get(key) ?? null),
      ),
    ).toEqual(oracle.deliveries[0]);
    expect(f.checkpoint).toMatchObject({ version: 2, identityCount: events.length });
    expect(f.contextRequests).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledTimes(1);
    const sent = JSON.parse(String(send.mock.calls[0]?.[1]?.body)) as OtlpExportRequest;
    expect(sent).toEqual(
      encodeLangfuseOtlpJson(oracle.deliveries[0]!.trace, oracle.deliveries[0]!.deliveryContext),
    );
    for (const span of sent.resourceSpans[0]!.scopeSpans[0]!.spans) {
      expect(span.attributes).toContainEqual({
        key: 'langfuse.observation.metadata.orca.agent.id',
        value: { stringValue: 'agt_registry' },
      });
      expect(span.attributes).toContainEqual({
        key: 'langfuse.observation.metadata.orca.agent.version',
        value: { intValue: '1' },
      });
    }
  }, 180_000);

  it('waits for an authenticated shared-reader barrier when a committed manifest overtakes its chunks', async () => {
    const f = (fixture = new KafkaV2Fixture());
    await f.open();
    const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
    let release = false;
    let blocked = false;
    f.beforeStateBatch = async (payload) => {
      if (!payload.batch.messages.some((message) => message.key?.toString().startsWith('D/')))
        return;
      blocked = true;
      await waitFor(
        'release delayed chunk application',
        async () => {
          await payload.heartbeat();
          return release;
        },
        () => f.assertHealthy(),
      );
    };
    const writer = f.kafka.producer({
      transactionalId: 'obs-v2-fixture-' + f.suffix,
      idempotent: true,
      maxInFlightRequests: 1,
      allowAutoTopicCreation: false,
    });
    try {
      const runtime = f.runtime(send);
      await runtime.start();
      await f.append(f.events);
      await f.waitForCheckpoint('8');
      await f.waitForDelivery();
      expect(f.checkpoint?.version).toBe(2);
      const acknowledged = await f.offset(f.deliveryGroupId, f.deliveryTopic);
      // The delivery owner is already restored. Only a NEW missing-chunks barrier
      // can advance this marker; source projection is idle throughout the injection.
      const barriers = () =>
        JSON.stringify([...f.stateRecords].filter(([key]) => key.startsWith('barrier:')));
      const before = barriers();
      const oracle = projectKafkaEvents(
        initialKafkaCheckpoint(
          {
            topic: f.topic,
            workspaceId: f.workspaceId,
            sessionId: f.sessionId,
          },
          f.checkpoint!.deliveryContext,
        ),
        largeTurnEvents(f).map((event, i) => ({ event, offset: String(i) })),
      );
      expect(oracle.deliveries).toHaveLength(1);
      const encoded = encodeKafkaDelivery(oracle.deliveries[0]!, f.sourceGroupId);
      expect(encoded.chunks.length).toBeGreaterThan(1);
      // A real transaction publishes a valid historical delivery fixture. Its
      // immutable content reference is independent of the current source head.
      await writer.connect();
      const tx = await writer.transaction();
      try {
        await sendKafkaStateRecords(tx, f.checkpointTopic, encoded.chunks, () => {});
        await tx.send({
          topic: f.deliveryTopic,
          messages: [{ partition: 0, ...encoded.delivery }],
        });
        await tx.commit();
      } catch (error) {
        await tx.abort();
        throw error;
      }
      await waitFor(
        'manifest observed and catch-up barrier committed',
        async () => blocked && f.deliveries.length === 2 && barriers() !== before,
        () => f.assertHealthy(),
      );
      expect(send).toHaveBeenCalledTimes(1);
      expect(await f.offset(f.deliveryGroupId, f.deliveryTopic)).toBe(acknowledged);
      expect(runtime.status()).toEqual({ ready: true, state: 'running' });
      release = true;
      await f.waitForDelivery(1);
      expect(send).toHaveBeenCalledTimes(2);
      const sent = JSON.parse(String(send.mock.calls[1]?.[1]?.body)) as OtlpExportRequest;
      expect(sent).toEqual(
        encodeLangfuseOtlpJson(oracle.deliveries[0]!.trace, oracle.deliveries[0]!.deliveryContext),
      );
      for (const span of sent.resourceSpans[0]!.scopeSpans[0]!.spans) {
        expect(span.attributes).toContainEqual({
          key: 'langfuse.observation.metadata.orca.agent.id',
          value: { stringValue: 'agt_registry' },
        });
        expect(span.attributes).toContainEqual({
          key: 'langfuse.observation.metadata.orca.agent.version',
          value: { intValue: '1' },
        });
      }
    } finally {
      release = true;
      await writer.disconnect();
    }
  }, 120_000);

  it('fails closed on a malformed UTF-8 checkpoint key instead of treating a broken shared log as empty', async () => {
    const f = (fixture = new KafkaV2Fixture());
    await f.open();
    await f.producer.send({
      topic: f.checkpointTopic,
      messages: [{ partition: 0, key: Buffer.from([0xff]), value: '{}' }],
    });
    await f.append(f.events);
    const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
    const failed = f.runtime(send);
    f.expectedFailures.add(failed);
    await failed.start().catch(() => undefined);
    await waitFor(
      'malformed key rejection',
      async () => failed.status().state === 'failed',
      () => f.assertHealthy(),
    );
    await failed.stop();
    expect(await f.offset(f.sourceGroupId, f.topic)).toBe('-1');
    expect(f.contextRequests).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  }, 90_000);

  it.each(['missing delivery pin', 'wrong route'] as const)(
    'rejects a persisted v1 checkpoint with %s without resolving a new pin or advancing offsets',
    async (damage) => {
      const f = (fixture = new KafkaV2Fixture());
      await f.open();
      const route = { topic: f.topic, workspaceId: f.workspaceId, sessionId: f.sessionId };
      const legacy = initialKafkaCheckpoint(route, null);
      legacy.nextOffset = '1';
      await f.append(f.events.slice(0, 2));
      const malformed: Record<string, unknown> = { ...legacy };
      if (damage === 'missing delivery pin') delete malformed.deliveryContext;
      else malformed.route = { ...route, sessionId: 'ses_wrong' };
      await seedLegacy(f, malformed, '1');
      const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
      const failed = f.runtime(send);
      f.expectedFailures.add(failed);
      await failed.start().catch(() => undefined);
      await waitFor(
        'malformed checkpoint rejection',
        async () => failed.status().state === 'failed',
        () => f.assertHealthy(),
      );
      await failed.stop();
      expect(await f.offset(f.sourceGroupId, f.topic)).toBe('1');
      expect(f.contextRequests).not.toHaveBeenCalled();
      expect(f.secretRequests).not.toHaveBeenCalled();
      expect(send).not.toHaveBeenCalled();
      expect(f.deliveries).toHaveLength(0);
    },
    120_000,
  );
});

// Reachable projector state, not hand-edited arrays: wide legal IDs, 256 completed
// model/tool/evaluation triples, and 1024 queued inputs. No /tmp fixture dependency.
function largeTurnEvents(f: KafkaV2Fixture): Event[] {
  const events: Event[] = [];
  const wideId = (seq: number) => ('evt_' + String(seq).padStart(8, '0') + '_').padEnd(512, 'x');
  const add = (
    kind: string,
    payload: Record<string, unknown> = {},
    options: Parameters<typeof event>[3] = {},
  ) => {
    const seq = events.length;
    const next = event(seq, kind, payload, {
      id: wideId(seq),
      workspaceId: f.workspaceId,
      sessionId: f.sessionId,
      ...options,
    });
    events.push(next);
    return next.id;
  };
  const user = add('user.message', {}, { producedBy: 'client', userId: 'u'.repeat(512) });
  add('session.user_event_processed', { user_event_id: user });
  add('session.status_running');
  for (let i = 0; i < 256; i++) {
    const model = add('span.model_request_start', {
      model_observation_kind: 'turn_model_summary',
      provider: 'p'.repeat(256),
      model: 'm'.repeat(256),
    });
    add('span.model_request_end', {
      model_observation_kind: 'turn_model_summary',
      model_request_start_id: model,
      is_error: false,
      model_usage: { input_tokens: 11, output_tokens: 7 },
    });
    const tool = add('agent.custom_tool_use');
    const result = add(
      'user.custom_tool_result',
      { custom_tool_use_id: tool },
      { producedBy: 'client' },
    );
    add('session.status_idle', { stop_reason: { type: 'requires_action' } });
    add('session.user_event_processed', { user_event_id: result });
    add('session.status_running');
    const evaluation = add('span.outcome_evaluation_start', {
      outcome_id: 'private_' + i,
      iteration: i,
    });
    add('span.outcome_evaluation_end', {
      outcome_id: 'private_' + i,
      iteration: i,
      outcome_evaluation_start_id: evaluation,
      result: 'satisfied',
    });
  }
  for (let i = 0; i < 1024; i++)
    add('user.message', {}, { producedBy: 'client', userId: 'u'.repeat(512) });
  add('session.status_idle', { stop_reason: { type: 'end_turn' } });
  return events;
}

async function seedLegacy(
  f: KafkaV2Fixture,
  checkpoint: KafkaCheckpoint | Record<string, unknown>,
  nextOffset = String(checkpoint.nextOffset),
): Promise<void> {
  const producer = f.kafka.producer({
    transactionalId: kafkaSourceTransactionId(f.groupId, f.topic),
    idempotent: true,
    maxInFlightRequests: 1,
    allowAutoTopicCreation: false,
  });
  await producer.connect();
  try {
    const tx = await producer.transaction();
    try {
      await tx.send({
        topic: f.checkpointTopic,
        acks: -1,
        messages: [{ partition: 0, key: f.sourceGroupId, value: JSON.stringify(checkpoint) }],
      });
      await tx.sendOffsets({
        consumerGroupId: f.sourceGroupId,
        topics: [{ topic: f.topic, partitions: [{ partition: 0, offset: nextOffset }] }],
      });
      await tx.commit();
    } catch (error) {
      await tx.abort();
      throw error;
    }
  } finally {
    await producer.disconnect();
  }
  // EndTxn acknowledgement can precede the coordinator applying its offset marker.
  await f.waitForCheckpoint(nextOffset);
}

class KafkaV2Fixture {
  readonly suffix = randomUUID().replaceAll('-', '');
  readonly workspaceId = `ws_kafka_only_${this.suffix}`;
  readonly sessionId = `ses_kafka_only_${this.suffix}`;
  readonly topic = `orca.${this.workspaceId}.sessions.${this.sessionId}.events`;
  readonly checkpointTopic = `obs-checkpoint-${this.suffix}`;
  readonly deliveryTopic = `obs-delivery-${this.suffix}`;
  readonly groupId = `obs-kafka-only-${this.suffix}`;
  readonly sourceGroupId = kafkaSourceGroupId(this.groupId, this.topic);
  readonly deliveryGroupId = `${this.groupId}-delivery-v1`;
  readonly deliveryProgressKey = kafkaDeliveryProgressKey(this.groupId, this.deliveryTopic, 0);
  readonly observerGroupId = `obs-test-reader-${this.suffix}`;
  readonly kafka = new Kafka({
    clientId: this.groupId,
    brokers,
    logLevel: logLevel.ERROR,
    connectionTimeout: 3_000,
  });
  readonly admin: Admin = this.kafka.admin();
  readonly producer: Producer = this.kafka.producer({ allowAutoTopicCreation: false });
  readonly observer: Consumer = this.kafka.consumer({
    groupId: this.observerGroupId,
    readUncommitted: false,
    allowAutoTopicCreation: false,
    retry: { restartOnFailure: async () => false },
  });
  readonly events = completedPrimaryTurnEvents('model_observation_kind', `_${this.suffix}`).map(
    (event) => ({
      ...event,
      workspaceId: this.workspaceId,
      sessionId: this.sessionId,
    }),
  );
  readonly contextRequests = vi.fn(() => enabledRegistryContext(this.workspaceId, this.sessionId));
  readonly secretRequests = vi.fn(() => basicRegistrySecret());
  readonly registryClient = new RegistryObservabilityClient({
    internalBaseUrl: 'http://registry.test',
    tokenProvider: async () => 'x'.repeat(32),
    fetchImpl: async (input) => {
      const url = String(input);
      if (url.endsWith('/agent-observability/context/resolve'))
        return jsonResponse(this.contextRequests());
      if (url.endsWith('/agent-observability/secret/resolve'))
        return jsonResponse(this.secretRequests());
      throw new Error(`unexpected Registry request: ${url}`);
    },
  });
  readonly runtimes: KafkaOnlyObservabilityExporterRuntime[] = [];
  readonly expectedFailures = new Set<KafkaOnlyObservabilityExporterRuntime>();
  readonly deliveries: Array<{ offset: string; record: KafkaDeliveryRecord }> = [];
  private readonly observedOffsets = new Map<string, bigint>();
  checkpoint: KafkaStateHead | undefined;
  readonly heads: KafkaStateHead[] = [];
  readonly stateRecords = new Map<string, string>();
  readonly directory = mkdtempSync(join(tmpdir(), 'orca-kafka-v2-'));
  readonly runtimeDirectories: string[] = [];
  readonly consumerGroups = new Set<string>();
  private consumerSpy: { mockRestore(): void } | undefined;
  beforeStateBatch: ((payload: EachBatchPayload) => Promise<void>) | undefined;
  deliveryProgress: unknown;
  private observerError: Error | undefined;
  private createdTopics = false;

  async open(): Promise<void> {
    const createConsumer = this.kafka.consumer.bind(this.kafka);
    this.consumerSpy = vi.spyOn(this.kafka, 'consumer').mockImplementation((options) => {
      this.consumerGroups.add(options.groupId);
      const consumer = createConsumer(options);
      if (options.groupId.startsWith('orca-exporter-restore-')) {
        const run = consumer.run.bind(consumer);
        vi.spyOn(consumer, 'run').mockImplementation(async (config) => {
          const eachBatch = config?.eachBatch;
          if (eachBatch === undefined) return run(config);
          return run({
            ...config,
            eachBatch: async (payload) => {
              await this.beforeStateBatch?.(payload);
              await eachBatch(payload);
            },
          });
        });
      }
      return consumer;
    });
    await this.admin.connect();
    await this.admin.createTopics({
      waitForLeaders: false,
      topics: [
        { topic: this.topic, numPartitions: 1, replicationFactor: 1 },
        {
          topic: this.checkpointTopic,
          numPartitions: 1,
          replicationFactor: 1,
          configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
        },
        {
          topic: this.deliveryTopic,
          numPartitions: 1,
          replicationFactor: 1,
          configEntries: [
            { name: 'cleanup.policy', value: 'delete' },
            { name: 'retention.ms', value: '-1' },
            { name: 'retention.bytes', value: '-1' },
          ],
        },
      ],
    });
    this.createdTopics = true;
    await waitForKafkaTopics(this.admin, [this.topic, this.checkpointTopic, this.deliveryTopic]);
    await this.producer.connect();
    this.observer.on(this.observer.events.CRASH, ({ payload }) => {
      this.observerError = payload.error;
    });
    this.observer.on(this.observer.events.END_BATCH_PROCESS, ({ payload }) => {
      this.observedOffsets.set(payload.topic, BigInt(payload.lastOffset));
    });
    await this.observer.connect();
    await this.observer.subscribe({
      topics: [this.checkpointTopic, this.deliveryTopic],
      fromBeginning: true,
    });
    await this.observer.run({
      autoCommit: false,
      eachMessage: async ({ topic, message }) => {
        if (topic === this.checkpointTopic && message.key !== null) {
          const key = message.key.toString();
          if (message.value === null) this.stateRecords.delete(key);
          else this.stateRecords.set(key, message.value.toString());
        }
        if (topic === this.checkpointTopic && message.key?.toString() === this.sourceGroupId) {
          const head = JSON.parse(message.value!.toString()) as KafkaStateHead;
          this.checkpoint = head;
          this.heads.push(head);
        } else if (
          topic === this.checkpointTopic &&
          message.key?.toString() === this.deliveryProgressKey
        ) {
          this.deliveryProgress =
            message.value === null ? null : JSON.parse(message.value.toString());
        } else if (topic === this.deliveryTopic) {
          this.deliveries.push({
            offset: message.offset,
            record: JSON.parse(message.value!.toString()) as KafkaDeliveryRecord,
          });
        }
      },
    });
  }

  runtime(
    otlpFetchImpl: typeof fetch,
    initialSession = true,
    limits: { maxDeliveryBytes?: number } = {},
  ): KafkaOnlyObservabilityExporterRuntime {
    const stateDirectory = join(this.directory, String(this.runtimes.length));
    mkdirSync(stateDirectory);
    this.runtimeDirectories.push(stateDirectory);
    const runtime = new KafkaOnlyObservabilityExporterRuntime({
      ...{ stateDirectory },
      ...limits,
      kafka: this.kafka,
      groupId: this.groupId,
      sessions: initialSession
        ? [{ topic: this.topic, workspaceId: this.workspaceId, sessionId: this.sessionId }]
        : [],
      checkpointTopic: this.checkpointTopic,
      deliveryTopic: this.deliveryTopic,
      registryClient: this.registryClient,
      otlpFetchImpl,
    });
    this.runtimes.push(runtime);
    return runtime;
  }

  async append(events: readonly Event[]): Promise<void> {
    // Match the Transcript wire directly: payload is the value, envelope is headers;
    // Kafka assigns seq. Do not route this through the Postgres-backed runtime/store.
    await this.producer.send({
      topic: this.topic,
      acks: -1,
      messages: events.map((event) => ({
        partition: 0,
        key: event.id,
        value: Buffer.from(event.payload),
        headers: {
          id: event.id,
          workspace_id: event.workspaceId,
          session_id: event.sessionId,
          subpath: event.subpath,
          produced_at: event.producedAt,
          produced_by: event.producedBy,
          kind: event.kind,
          idempotency_key: event.idempotencyKey,
          ...(event.userId === undefined ? {} : { user_id: event.userId }),
        },
      })),
    });
  }

  async offset(groupId: string, topic: string): Promise<string> {
    const offsets = await this.admin.fetchOffsets({
      groupId,
      topics: [topic],
      resolveOffsets: false,
    });
    return offsets[0]?.partitions.find((partition) => partition.partition === 0)?.offset ?? '-1';
  }

  async waitForCheckpoint(nextOffset: string): Promise<void> {
    await waitFor(
      `committed checkpoint and source offset ${nextOffset}`,
      async () =>
        this.checkpoint?.nextOffset === nextOffset &&
        (await this.offset(this.sourceGroupId, this.topic)) === nextOffset,
      () => this.assertHealthy(),
    );
  }

  async waitForDelivery(index = 0): Promise<void> {
    await waitFor(
      'committed delivery offset',
      async () => {
        const delivery = this.deliveries[index];
        return (
          delivery !== undefined &&
          (await this.offset(this.deliveryGroupId, this.deliveryTopic)) ===
            (BigInt(delivery.offset) + 1n).toString()
        );
      },
      () => this.assertHealthy(),
    );
  }

  async waitForObservedTail(topic: string): Promise<void> {
    const offsets = await this.admin.fetchTopicOffsets(topic);
    const high = offsets.find((partition) => partition.partition === 0)?.high;
    expect(high).toBeDefined();
    const tail = BigInt(high!) - 1n;
    await waitFor(
      `read_committed observer through ${topic}:${tail}`,
      async () => (this.observedOffsets.get(topic) ?? -1n) >= tail,
      () => this.assertHealthy(),
    );
  }

  assertHealthy(): void {
    if (this.observerError !== undefined) throw this.observerError;
    for (const runtime of this.runtimes) {
      if (!this.expectedFailures.has(runtime)) expect(runtime.status().state).not.toBe('failed');
    }
  }

  async close(): Promise<void> {
    await Promise.all(this.runtimes.map((runtime) => runtime.stop()));
    await Promise.all([this.observer.disconnect(), this.producer.disconnect()]);
    try {
      if (this.createdTopics) {
        let groups = [
          ...new Set([
            ...this.consumerGroups,
            this.sourceGroupId,
            this.deliveryGroupId,
            this.observerGroupId,
          ]),
        ];
        try {
          await waitFor(
            'fixture groups to leave and delete',
            async () => {
              try {
                await this.admin.deleteGroups(groups);
                return true;
              } catch (error) {
                if (!(error instanceof KafkaJSDeleteGroupsError)) throw error;
                // An early startup failure need not create every group. A completed
                // LeaveGroup can briefly still be visible as NON_EMPTY_GROUP.
                if (error.groups.some((group) => group.errorCode !== 68 && group.errorCode !== 69))
                  throw new Error('fixture group cleanup failed: ' + JSON.stringify(error.groups), {
                    cause: error,
                  });
                groups = error.groups
                  .filter((group) => group.errorCode === 68)
                  .map((group) => group.groupId);
                return groups.length === 0;
              }
            },
            () => {},
          );
        } finally {
          await this.admin.deleteTopics({
            topics: [this.topic, this.checkpointTopic, this.deliveryTopic],
          });
        }
      }
    } finally {
      await this.admin.disconnect();
      this.consumerSpy?.mockRestore();
      rmSync(this.directory, { recursive: true, force: true });
    }
  }
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), { headers: { 'content-type': 'application/json' } });
}

async function waitFor(
  description: string,
  condition: () => Promise<boolean>,
  assertHealthy: () => void,
): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    assertHealthy();
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`timed out waiting for ${description}`);
}
