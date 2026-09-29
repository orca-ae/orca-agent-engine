// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Event } from '@orca/transcript-store-types';
import { Kafka, logLevel, type Admin, type Consumer, type Producer } from 'kafkajs';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  KafkaOnlyObservabilityExporterRuntime,
  kafkaDeliveryProgressKey,
  kafkaSourceGroupId,
} from '../../../src/kafka-runtime.js';
import { decodeKafkaBlob, kafkaBlobKeys } from '../../../src/kafka-blob.js';
import type { KafkaDeliveryRecord } from '../../../src/kafka-state.js';
import type { OtlpExportRequest } from '../../../src/otlp-json.js';
import {
  kafkaStateKeys,
  parseKafkaStateHead,
  type KafkaStateHead,
} from '../../../src/kafka-state-v2.js';
import { parseCanonicalProjectionState } from '../../../src/projector.js';
import { RegistryObservabilityClient } from '../../../src/registry-client.js';
import { TRANSCRIPT_SECRET, completedPrimaryTurnEvents } from '../../support/events.js';
import {
  IO_CANARY,
  IO_INPUT,
  IO_OUTPUT,
  IO_TOOL_NAME,
  rawPrimaryTurnEvents,
} from '../../support/raw-events.js';
import { waitForKafkaTopics } from '../../support/kafka.js';
import { basicRegistrySecret, enabledRegistryContext } from '../../support/registry.js';

const brokers = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);

// Runs in the normal integration gate. This suite needs Kafka only, never SQL.
describe('Kafka-only exporter durability', () => {
  let fixture: KafkaFixture | undefined;

  afterEach(async () => {
    await fixture?.close();
    fixture = undefined;
  }, 60_000);

  it('restores raw user/tool content across processes and sends usable observation I/O', async () => {
    fixture = new KafkaFixture();
    const current = fixture;
    const events = rawPrimaryTurnEvents('_' + current.suffix).map((entry) => ({
      ...entry,
      workspaceId: current.workspaceId,
      sessionId: current.sessionId,
    }));
    enableRawCapture(current);
    await current.open();
    const requests: string[] = [];
    const send: typeof fetch = async (_input, init) => {
      requests.push(String(init?.body));
      return jsonResponse({});
    };
    const first = current.runtime(send);
    await first.start();
    // Checkpoint the accepted input and tool use, before the tool result arrives.
    await current.append(events.slice(0, 5));
    await current.waitForCheckpoint('5');
    const pending = JSON.stringify(current.reducer);
    expect(pending).toContain(IO_INPUT);
    expect(pending).toContain(IO_TOOL_NAME);
    expect(pending).toContain(IO_CANARY);
    await first.stop();

    const second = current.runtime(send);
    await second.start();
    await current.append(events.slice(5));
    await current.waitForCheckpoint(String(events.length));
    await current.waitForDelivery();
    expect(requests).toHaveLength(1);
    expect(requests[0]).toContain('langfuse.observation.input');
    expect(requests[0]).toContain('langfuse.observation.output');
    expect(requests[0]).toContain(IO_INPUT);
    expect(requests[0]).toContain(IO_OUTPUT);
    expect(requests[0]).toContain(IO_TOOL_NAME);
    expect(requests[0]).toContain(IO_CANARY);
    expect(requests[0]).not.toContain('private-thinking-canary');
    expect(requests[0]).not.toContain('unfinished-private-delta');
    const wire = JSON.parse(requests[0]!) as OtlpExportRequest;
    const spans = wire.resourceSpans[0]!.scopeSpans[0]!.spans;
    for (const span of spans) {
      expect(span.attributes).toEqual(
        expect.arrayContaining([
          {
            key: 'langfuse.observation.metadata.orca.agent.id',
            value: { stringValue: 'agt_registry' },
          },
          { key: 'langfuse.observation.metadata.orca.agent.version', value: { intValue: '1' } },
          { key: 'langfuse.environment', value: { stringValue: 'test' } },
          { key: 'langfuse.release', value: { stringValue: 'rio-attribution-approval-test' } },
        ]),
      );
    }
    const tool = spans.find((span) => span.name === IO_TOOL_NAME);
    expect(tool?.attributes).toEqual(
      expect.arrayContaining([
        {
          key: 'langfuse.observation.metadata.orca.tool.last_approval.result',
          value: { stringValue: 'allow' },
        },
      ]),
    );
    expect(tool?.status.code).toBe(2);
    expect(spans[0]?.status.code).toBe(1);
    expect(second.status().state).toBe('running');
  }, 120_000);

  it('suppresses queued raw content when fresh authorization is restricted', async () => {
    fixture = new KafkaFixture();
    const current = fixture;
    enableRawCapture(current);
    await current.open();
    let restricted = false;
    const requests: string[] = [];
    current.secretRequests.mockImplementation(() => ({
      ...basicRegistrySecret(),
      effective_capture_mode: restricted ? 'metadata_only' : 'raw_io',
    }));
    const runtime = current.runtime(async (_input, init) => {
      requests.push(String(init?.body));
      restricted = true;
      return new Response('temporarily unavailable', { status: 503 });
    });
    await runtime.start();
    await current.append(
      rawPrimaryTurnEvents('_' + current.suffix).map((entry) => ({
        ...entry,
        workspaceId: current.workspaceId,
        sessionId: current.sessionId,
      })),
    );
    await current.waitForDelivery();
    expect(requests).toHaveLength(1);
    expect(requests[0]).toContain(IO_INPUT);
    // The first, explicitly authorized raw attempt preserves the original value.
    // Restriction suppresses subsequent sends; it does not retroactively redact it.
    expect(requests[0]).toContain(IO_CANARY);
    expect(current.secretRequests.mock.calls.length).toBeGreaterThanOrEqual(2);
    expect(runtime.status().state).toBe('running');
  }, 120_000);

  it('restores an unfinished turn from a committed checkpoint and deduplicates replayed events', async () => {
    fixture = new KafkaFixture();
    await fixture.open();
    const requests: string[] = [];
    const send: typeof fetch = async (_input, init) => {
      requests.push(String(init?.body));
      expect(new Headers(init?.headers).get('authorization')).toBe(
        `Basic ${Buffer.from('pk-registry:sk-registry').toString('base64')}`,
      );
      return jsonResponse({});
    };
    const first = fixture.runtime(send, false);
    await first.start();
    await first.addSessions([
      { topic: fixture.topic, workspaceId: fixture.workspaceId, sessionId: fixture.sessionId },
    ]);
    // Stop with the accepted user event and model start persisted, but no model end.
    await fixture.append(fixture.events.slice(0, 5));
    await fixture.waitForCheckpoint('5');
    expect(fixture.reducer.activeTurn).not.toBeNull();
    expect(fixture.deliveries).toHaveLength(0);
    expect(requests).toHaveLength(0);
    await first.stop();

    const restarted = fixture.runtime(send);
    await restarted.start();
    await fixture.append(fixture.events.slice(5));
    await fixture.waitForCheckpoint(String(fixture.events.length));
    await fixture.waitForDelivery();
    expect(fixture.deliveries).toHaveLength(1);
    expect(fixture.reducer.activeTurn).toBeNull();
    expect(fixture.checkpoint?.identityCount).toBe(fixture.events.length);
    expect(fixture.deliveries[0]?.record.trace).toMatchObject({
      workspaceId: fixture.workspaceId,
      sessionId: fixture.sessionId,
    });
    // Context must be restored from Kafka, not repinned on the second process.
    expect(fixture.contextRequests).toHaveBeenCalledTimes(1);
    expect(requests).toHaveLength(1);
    expect(requests[0]).toContain('orca.agent.turn');
    expect(requests[0]).toContain('claude-test');
    expect(requests[0]).not.toContain(TRANSCRIPT_SECRET);
    expect(requests[0]).not.toContain('queued and unaccepted');

    // A third owner exercises the persisted identity ledger after turn completion.
    await restarted.stop();
    const replay = fixture.runtime(send);
    await replay.start();
    await fixture.append(fixture.events);
    await fixture.waitForCheckpoint(String(fixture.events.length * 2));
    expect(fixture.checkpoint?.identityCount).toBe(fixture.events.length);
    expect(fixture.contextRequests).toHaveBeenCalledTimes(1);
    expect(fixture.deliveries).toHaveLength(1);
    expect(requests).toHaveLength(1);
    expect(replay.status()).toEqual({ ready: true, state: 'running' });
  }, 120_000);

  it('restores acknowledged delivery progress after its consumer group is deleted without resending history', async () => {
    fixture = new KafkaFixture();
    const current = fixture;
    await current.open();
    const requests: string[] = [];
    const send: typeof fetch = async (_input, init) => {
      requests.push(String(init?.body));
      return jsonResponse({});
    };
    const first = current.runtime(send);
    await first.start();
    await current.append(current.events);
    await current.waitForCheckpoint(String(current.events.length));
    await current.waitForDelivery();
    await first.stop();
    await current.waitForObservedTail(current.checkpointTopic);
    const acknowledgedOffset = await current.offset(current.deliveryGroupId, current.deliveryTopic);
    expect(current.deliveryProgress).toEqual({
      version: 1,
      topic: current.deliveryTopic,
      partition: 0,
      groupId: current.deliveryGroupId,
      nextOffset: acknowledgedOffset,
    });
    expect(requests).toHaveLength(1);

    await current.admin.deleteGroups([current.deliveryGroupId]);
    expect(await current.offset(current.deliveryGroupId, current.deliveryTopic)).toBe('-1');
    const restarted = current.runtime(send);
    await restarted.start();
    const namespace = `_${current.suffix}_next`;
    const nextTurn = completedPrimaryTurnEvents('model_observation_kind', namespace).map(
      (event) => ({
        ...event,
        // The support fixture namespaces referenced IDs, but not generic status IDs.
        id: event.id.endsWith(namespace) ? event.id : `${event.id}${namespace}`,
        idempotencyKey: `${event.idempotencyKey}${namespace}`,
        workspaceId: current.workspaceId,
        sessionId: current.sessionId,
      }),
    );
    await current.append(nextTurn);
    await current.waitForCheckpoint(String(current.events.length + nextTurn.length));
    await current.waitForDelivery(1);
    // A newly acknowledged, distinct turn proves the restarted consumer passed history.
    await restarted.stop();
    await current.waitForObservedTail(current.deliveryTopic);
    expect(current.deliveries).toHaveLength(2);
    expect(current.deliveries[1]?.record.trace.traceId).not.toBe(
      current.deliveries[0]?.record.trace.traceId,
    );
    expect(requests).toHaveLength(2);
    expect(new Set(requests).size).toBe(2);
  }, 120_000);

  it('fails closed before sending a subsequent delivery when acknowledged progress is tombstoned', async () => {
    fixture = new KafkaFixture();
    const current = fixture;
    await current.open();
    const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
    const first = current.runtime(send);
    await first.start();
    await current.append(current.events);
    await current.waitForCheckpoint(String(current.events.length));
    await current.waitForDelivery();
    await first.stop();
    await current.waitForObservedTail(current.checkpointTopic);
    const acknowledgedOffset = await current.offset(current.deliveryGroupId, current.deliveryTopic);
    expect(BigInt(acknowledgedOffset)).toBeGreaterThan(0n);
    expect(current.deliveryProgress).toMatchObject({ nextOffset: acknowledgedOffset });
    expect(send).toHaveBeenCalledTimes(1);

    await current.producer.send({
      topic: current.checkpointTopic,
      acks: -1,
      messages: [{ partition: 0, key: current.deliveryProgressKey, value: null }],
    });
    await current.waitForObservedTail(current.checkpointTopic);
    expect(current.deliveryProgress).toBeNull();
    // Preserve the source checkpoint and the advanced group offset. Queue a valid
    // record at a new physical offset so a consumer cannot merely idle past the test.
    const record = current.deliveries[0]!.record;
    await current.producer.send({
      topic: current.deliveryTopic,
      acks: -1,
      messages: [{ partition: 0, key: record.trace.traceId, value: JSON.stringify(record) }],
    });
    await current.waitForObservedTail(current.deliveryTopic);
    expect(current.deliveries).toHaveLength(2);
    expect(BigInt(current.deliveries[1]!.offset)).toBeGreaterThanOrEqual(
      BigInt(acknowledgedOffset),
    );
    send.mockClear();
    const restarted = current.runtime(send);
    current.expectedFailures.add(restarted);
    // Validation may fail during startup or when the first assigned batch arrives.
    await restarted.start().catch(() => undefined);
    await waitFor(
      'fail-closed delivery progress validation',
      async () => restarted.status().state === 'failed',
      () => current.assertHealthy(),
    );
    await restarted.stop();
    expect(restarted.status()).toEqual({ ready: false, state: 'failed' });
    expect(send).not.toHaveBeenCalled();
    expect(await current.offset(current.deliveryGroupId, current.deliveryTopic)).toBe(
      acknowledgedOffset,
    );
    expect(current.checkpoint?.nextOffset).toBe(String(current.events.length));
  }, 120_000);

  it('aborts broker-written delivery when checkpoint send fails, then replays atomically on restart', async () => {
    fixture = new KafkaFixture();
    const current = fixture;
    await current.open();
    const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
    const createProducer = current.kafka.producer.bind(current.kafka);
    let writtenOffset: string | undefined;
    let writtenRecord: KafkaDeliveryRecord | undefined;
    let aborted = false;
    const injection = vi.spyOn(current.kafka, 'producer').mockImplementation((config) => {
      const producer = createProducer(config);
      const begin = producer.transaction.bind(producer);
      vi.spyOn(producer, 'transaction').mockImplementation(async () => {
        const transaction = await begin();
        let hasDelivery = false;
        return {
          ...transaction,
          send: async (record) => {
            if (hasDelivery && record.topic === current.checkpointTopic) {
              throw new Error('injected checkpoint send failure after broker-written delivery');
            }
            const metadata = await transaction.send(record);
            if (record.topic === current.deliveryTopic) {
              hasDelivery = true;
              writtenOffset = metadata[0]?.baseOffset;
              writtenRecord = JSON.parse(String(record.messages[0]?.value)) as KafkaDeliveryRecord;
            }
            return metadata;
          },
          abort: async () => {
            await transaction.abort();
            if (hasDelivery) aborted = true;
          },
        };
      });
      return producer;
    });
    try {
      // Preload one complete source batch: only the v2 initialization head can
      // precede the aborted projection, never a partially consumed source batch.
      await current.append(current.events);
      const failed = current.runtime(send);
      await failed.start();
      await waitFor(
        'injected transaction failure',
        async () => failed.status().state === 'failed',
        () => {},
      );
      await failed.stop();
      current.expectedFailures.add(failed);
      expect(aborted).toBe(true);
      expect(writtenOffset).toBeDefined();
      expect(writtenRecord?.trace.sessionId).toBe(current.sessionId);
      expect(await current.offset(current.sourceGroupId, current.topic)).toBe('0');
      expect(await current.offset(current.deliveryGroupId, current.deliveryTopic)).toBe('-1');
      // KafkaJS emits END_BATCH_PROCESS even for filtered aborted/control batches.
      // Wait for the observer to pass the physical log tails, not a guessed delay.
      await current.waitForObservedTail(current.deliveryTopic);
      await current.waitForObservedTail(current.checkpointTopic);
      expect(current.deliveries).toHaveLength(0);
      expect(current.checkpoint).toMatchObject({
        version: 2,
        nextOffset: '0',
        identityCount: 0,
        acceptedCount: 0,
      });
      expect(current.reducer.activeTurn).toBeNull();
      expect(current.observedLedgerCounts()).toEqual({ identities: 0, accepted: 0 });
      expect(send).not.toHaveBeenCalled();

      injection.mockRestore();
      const restarted = current.runtime(send);
      await restarted.start();
      await current.waitForCheckpoint(String(current.events.length));
      await current.waitForDelivery();
      await current.waitForObservedTail(current.deliveryTopic);
      expect(current.deliveries).toHaveLength(1);
      expect(current.deliveries[0]?.record).toEqual(writtenRecord);
      expect(BigInt(current.deliveries[0]!.offset)).toBeGreaterThan(BigInt(writtenOffset!));
      expect(send).toHaveBeenCalledTimes(1);
      expect(restarted.status()).toEqual({ ready: true, state: 'running' });
    } finally {
      injection.mockRestore();
    }
  }, 120_000);

  it('survives overlapping owners rebalancing and completes the checkpointed turn after the first stops', async () => {
    fixture = new KafkaFixture();
    const current = fixture;
    await current.open();
    const send = vi.fn<typeof fetch>(async () => jsonResponse({}));
    const first = current.runtime(send);
    await first.start();
    await current.append(current.events.slice(0, 5));
    await current.waitForCheckpoint('5');
    const second = current.runtime(send);
    await second.start();
    await waitFor(
      'two stable source group members',
      async () => {
        const { groups } = await current.admin.describeGroups([current.sourceGroupId]);
        return groups[0]?.state === 'Stable' && groups[0].members.length === 2;
      },
      () => current.assertHealthy(),
    );
    expect(send).not.toHaveBeenCalled();
    await first.stop();
    await current.append(current.events.slice(5));
    await current.waitForCheckpoint(String(current.events.length));
    await current.waitForDelivery();
    await current.waitForObservedTail(current.deliveryTopic);
    expect(current.deliveries).toHaveLength(1);
    expect(current.contextRequests).toHaveBeenCalledTimes(1);
    expect(current.reducer.activeTurn).toBeNull();
    expect(send).toHaveBeenCalledTimes(1);
    expect(first.status()).toEqual({ ready: false, state: 'stopped' });
    expect(second.status()).toEqual({ ready: true, state: 'running' });
  }, 150_000);

  it('keeps failed delivery uncommitted, then retries the same trace and commits only after success', async () => {
    fixture = new KafkaFixture();
    await fixture.open();
    let accept = false;
    let successes = 0;
    const requests: string[] = [];
    const runtime = fixture.runtime(async (_input, init) => {
      requests.push(String(init?.body));
      if (!accept) return new Response('unavailable', { status: 503 });
      successes += 1;
      return jsonResponse({});
    });
    await runtime.start();
    await fixture.append(fixture.events);
    await fixture.waitForCheckpoint(String(fixture.events.length));
    await waitFor(
      'two failed HTTP attempts',
      async () => requests.length >= 2,
      () => fixture!.assertHealthy(),
    );
    // Projection/source acknowledgement is atomic in Kafka and independent of HTTP.
    expect(fixture.deliveries).toHaveLength(1);
    // The initialized progress marker commits offset zero, not this failed record.
    expect(await fixture.offset(fixture.deliveryGroupId, fixture.deliveryTopic)).toBe('0');
    expect(successes).toBe(0);
    accept = true;
    await fixture.waitForDelivery();
    expect(successes).toBe(1);
    expect(requests.length).toBeGreaterThanOrEqual(3);
    expect(new Set(requests).size).toBe(1);
    expect(fixture.secretRequests).toHaveBeenCalledTimes(requests.length);
    expect(fixture.deliveries).toHaveLength(1);
    expect(runtime.status()).toEqual({ ready: true, state: 'running' });
  }, 90_000);
});

class KafkaFixture {
  readonly suffix = randomBytes(6).toString('hex');
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
  // Test oracle only: production restores through its disk-backed index. Keep
  // the head at its original key; I/A/R records are auxiliary state, not heads.
  private readonly stateValues = new Map<string, string>();
  private stateDirectory: string | undefined;

  get checkpoint(): KafkaStateHead | undefined {
    const raw = this.stateValues.get(this.sourceGroupId);
    return raw === undefined
      ? undefined
      : parseKafkaStateHead(raw, {
          topic: this.topic,
          workspaceId: this.workspaceId,
          sessionId: this.sessionId,
        });
  }

  get reducer() {
    const head = this.checkpoint;
    if (head === undefined) throw new Error('missing observed v2 head');
    const keys = kafkaBlobKeys(head.reducer, kafkaStateKeys(this.sourceGroupId).reducerPrefix);
    return parseCanonicalProjectionState(
      decodeKafkaBlob(
        head.reducer,
        keys.map((key) => this.stateValues.get(key) ?? null),
      ),
    );
  }

  observedLedgerCounts(): { identities: number; accepted: number } {
    const prefixes = kafkaStateKeys(this.sourceGroupId);
    const keys = [...this.stateValues.keys()];
    return {
      identities: keys.filter((key) => key.startsWith(prefixes.identityPrefix)).length,
      accepted: keys.filter((key) => key.startsWith(prefixes.acceptedPrefix)).length,
    };
  }
  deliveryProgress: unknown;
  private observerError: Error | undefined;
  private createdTopics = false;

  async open(): Promise<void> {
    this.stateDirectory = await mkdtemp(join(tmpdir(), 'orca-kafka-only-spec-'));
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
        if (topic === this.checkpointTopic) {
          const key = message.key!.toString();
          if (message.value === null) this.stateValues.delete(key);
          else this.stateValues.set(key, message.value.toString());
          if (key === this.deliveryProgressKey) {
            this.deliveryProgress =
              message.value === null ? null : JSON.parse(message.value.toString());
          }
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
  ): KafkaOnlyObservabilityExporterRuntime {
    const runtime = new KafkaOnlyObservabilityExporterRuntime({
      kafka: this.kafka,
      groupId: this.groupId,
      sessions: initialSession
        ? [{ topic: this.topic, workspaceId: this.workspaceId, sessionId: this.sessionId }]
        : [],
      checkpointTopic: this.checkpointTopic,
      deliveryTopic: this.deliveryTopic,
      stateDirectory: this.stateDirectory!,
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
      async () => {
        const head = this.checkpoint;
        if (head?.nextOffset !== nextOffset) return false;
        const counts = this.observedLedgerCounts();
        if (counts.identities !== head.identityCount || counts.accepted !== head.acceptedCount)
          return false;
        // Do not decode historical heads during cold replay: compacted chunks
        // may only reconstruct the latest head once the observer catches up.
        const keys = kafkaBlobKeys(head.reducer, kafkaStateKeys(this.sourceGroupId).reducerPrefix);
        if (keys.some((key) => !this.stateValues.has(key))) return false;
        expect(this.reducer).toBeDefined();
        return (await this.offset(this.sourceGroupId, this.topic)) === nextOffset;
      },
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
    try {
      await Promise.all(this.runtimes.map((runtime) => runtime.stop()));
      await Promise.all([this.observer.disconnect(), this.producer.disconnect()]);
      if (this.createdTopics) {
        try {
          const { groups } = await this.admin.listGroups();
          const owned = new Set([this.sourceGroupId, this.deliveryGroupId, this.observerGroupId]);
          const existing = groups.map((group) => group.groupId).filter((id) => owned.has(id));
          if (existing.length > 0) await this.admin.deleteGroups(existing);
        } finally {
          await this.admin.deleteTopics({
            topics: [this.topic, this.checkpointTopic, this.deliveryTopic],
          });
        }
      }
    } finally {
      try {
        await this.admin.disconnect();
      } finally {
        if (this.stateDirectory !== undefined)
          await rm(this.stateDirectory, { recursive: true, force: true });
      }
    }
  }
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), { headers: { 'content-type': 'application/json' } });
}

function enableRawCapture(fixture: KafkaFixture): void {
  fixture.contextRequests.mockImplementation(() => {
    const context = enabledRegistryContext(fixture.workspaceId, fixture.sessionId);
    const binding = context.binding as { config: Record<string, unknown> };
    binding.config.capture_mode = 'raw_io';
    binding.config.environment = 'test';
    binding.config.release = 'rio-attribution-approval-test';
    context.capture = {
      pinned_mode: 'raw_io',
      effective_mode: 'raw_io',
      current_ceilings: {
        platform: 'raw_io',
        organization: 'raw_io',
        workspace: 'raw_io',
      },
    };
    const epochs = context.epochs as { pinned: unknown; current: unknown };
    epochs.current = epochs.pinned;
    return context;
  });
  fixture.secretRequests.mockImplementation(() => ({
    ...basicRegistrySecret(),
    effective_capture_mode: 'raw_io',
  }));
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
