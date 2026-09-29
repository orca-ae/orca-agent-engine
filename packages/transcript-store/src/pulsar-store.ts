// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { v7 as uuidv7 } from 'uuid';
import { setTimeout as abortableDelay } from 'node:timers/promises';
import { setMaxListeners } from 'node:events';
import { LruDedup } from './kafka/dedup.js';
import {
  appendLatency,
  appendTotal,
  archiveTotal,
  dedupHits,
  readFirstByteLatency,
  readTotal,
  subagentMessageRate,
  tailTotal,
} from './metrics.js';
import type {
  ReadOptions,
  SessionEventSourceStatus,
  TailOptions,
  TranscriptStore,
} from './store.js';
import { RetryableSessionEventError, SessionEventBarrierError, type Event } from './types.js';
import { assertEventsMatchRoute, eventMatchesRoute, type SessionRoute } from './route.js';
import { terminalSourceFailureFields } from './source-failure.js';

const DEFAULT_TENANT = 'public';
const DEFAULT_NAMESPACE = 'default';
const DEFAULT_TOPIC_PREFIX = 'orca';
// hasNext already established backlog; allow broker delivery time under load.
// A timeout still rejects the scan and never means end-of-transcript.
const DEFAULT_READ_TIMEOUT_MS = 5_000;
const DEFAULT_TAIL_READ_TIMEOUT_MS = 500;
const DEFAULT_EVENT_SOURCE_RECEIVE_TIMEOUT_MS = 500;
const DEFAULT_EVENT_SOURCE_MAX_CONCURRENT_HANDLERS = 8;
const DEFAULT_EVENT_SOURCE_MAX_PENDING_DELIVERIES = 100;
const DEFAULT_EVENT_SOURCE_TOPIC_REDISCOVER_INTERVAL_MS = 5_000;
const DEFAULT_EVENT_SOURCE_PATTERN_AUTO_DISCOVERY_SECONDS = 1;
// Negative-ack redelivery delay set explicitly on the event-source consumer.
// 10s keeps a persistently-failing message cycling through the
// MAX_HANDLER_REDELIVERIES backstop quickly (dropped in under a minute) while
// keeping the nack quiescence window that defers idle consumer refresh
// (see refreshIdleConsumerIfDue) modest.
const DEFAULT_EVENT_SOURCE_NACK_REDELIVER_TIMEOUT_MS = 10_000;
// Poison backstop: once the broker reports this many redeliveries for a
// message whose handler keeps throwing, the message is acked and dropped.
// RetryableSessionEventError is exempt because it represents infrastructure
// unavailability rather than a poison payload.
// Without a cap, a single unprocessable event (e.g. a stray client
// tool_confirmation no runner state can apply) wedges the subscription in an
// endless nack/redeliver loop — an easy DoS on the whole pipeline.
const MAX_HANDLER_REDELIVERIES = 5;
// The native NegativeAcksTracker redelivers nacked messages on an internal
// timer tick (a fraction of the nack delay), so it can stay armed for up to
// ~4/3 of the delay after the last nack. Twice the delay plus this fixed
// slack gives a comfortable quiescence margin.
const NACK_QUIESCENCE_SLACK_MS = 500;
// pulsar-client 1.17 does not expose patternAutoDiscoveryPeriod to the native
// ConsumerConfiguration, whose default period is 60s. Pattern and nack timer
// callbacks can therefore outlive close(); keep the closed Node wrapper (and
// its shared pulsar_consumer_t) alive for two native discovery periods before
// allowing GC to free it.
const NATIVE_PATTERN_AUTO_DISCOVERY_PERIOD_MS = 60_000;
const RETIRED_CONSUMER_MIN_HOLD_MS =
  2 * NATIVE_PATTERN_AUTO_DISCOVERY_PERIOD_MS + NACK_QUIESCENCE_SLACK_MS;
// stop() gives an armed nack timer a bounded chance to fire before closing
// the consumer. Capped so shutdown stays fast; a timer still armed past the
// cap is a residual risk we accept on the exit path.
const STOP_NACK_DRAIN_MAX_WAIT_MS = 3_000;
const DEFAULT_DEDUP_CAPACITY = 1024;
const DEFAULT_PRODUCER_CACHE_CAPACITY = 256;
const APPEND_BATCH_MAX_MESSAGES = 1_000;
const APPEND_BATCH_MAX_BYTES = 2 * 1024 * 1024;
const APPEND_BATCH_MAX_PUBLISH_DELAY_MS = 60_000;

interface PulsarModule {
  Client: new (config: PulsarClientConfig) => PulsarClient;
  AuthenticationToken: new (params: { token: string }) => unknown;
  AuthenticationOauth2: new (params: PulsarOauth2Params) => unknown;
  MessageId: {
    earliest(): PulsarMessageId;
    latest(): PulsarMessageId;
    deserialize(data: Buffer): PulsarMessageId;
  };
}

interface PulsarClientConfig {
  serviceUrl: string;
  authentication?: unknown;
  operationTimeoutSeconds?: number;
  [key: string]: unknown;
}

interface PulsarOauth2Params {
  type: string;
  issuer_url: string;
  client_id?: string;
  client_secret?: string;
  private_key?: string;
  audience?: string;
  scope?: string;
}

interface PulsarClient {
  createProducer(config: PulsarProducerConfig): Promise<PulsarProducer>;
  createReader(config: PulsarReaderConfig): Promise<PulsarReader>;
  subscribe(config: PulsarConsumerConfig): Promise<PulsarConsumer>;
  close(): Promise<null>;
}

interface PulsarProducerConfig {
  topic: string;
  sendTimeoutMs?: number;
  batchingEnabled?: boolean;
  batchingType?: 'DefaultBatching' | 'KeyBasedBatching';
  batchingMaxPublishDelayMs?: number;
  batchingMaxMessages?: number;
  batchingMaxAllowedSizeInBytes?: number;
  blockIfQueueFull?: boolean;
}

interface PulsarProducer {
  send(message: PulsarProducerMessage): Promise<PulsarMessageId>;
  flush(): Promise<null>;
  close(): Promise<null>;
}

interface PulsarProducerMessage {
  data: Buffer;
  properties?: Record<string, string>;
  eventTimestamp?: number;
  partitionKey?: string;
}

interface PulsarReaderConfig {
  topic: string;
  startMessageId: PulsarMessageId;
  receiverQueueSize?: number;
}

interface PulsarReader {
  hasNext(): boolean | Promise<boolean>;
  readNext(timeout?: number): Promise<PulsarMessage>;
  close(): Promise<null>;
}

interface PulsarConsumerConfig {
  topic?: string;
  topicsPattern?: string;
  subscription: string;
  subscriptionType?: 'Shared' | 'Exclusive' | 'Failover' | 'KeyShared';
  keySharedPolicy?: { keyShareMode: 'AutoSplit'; allowOutOfOrderDelivery: false };
  subscriptionInitialPosition?: 'Earliest' | 'Latest';
  ackTimeoutMs?: number;
  nAckRedeliverTimeoutMs?: number;
  patternAutoDiscoveryPeriod?: number;
  receiverQueueSize?: number;
}

interface PulsarConsumer {
  receive(timeout?: number): Promise<PulsarMessage>;
  acknowledge(message: PulsarMessage): Promise<null>;
  negativeAcknowledge(message: PulsarMessage): void;
  close(): Promise<null>;
  unsubscribe(): Promise<null>;
}

interface PulsarMessage {
  getData(): Buffer;
  getProperties(): Record<string, string>;
  getMessageId(): PulsarMessageId;
  getPublishTimestamp(): number;
  getRedeliveryCount(): number;
  getTopicName(): string;
}

interface PulsarMessageId {
  serialize(): Buffer;
  toString(): string;
}

const PULSAR_LEDGER_SEQ_MULTIPLIER = 1_000_000_000n;
const PULSAR_BATCH_SEQ_MULTIPLIER = 10_000n;
const POSTGRES_BIGINT_MAX = 9_223_372_036_854_775_807n;

export interface PulsarTranscriptStoreOptions {
  serviceUrl?: string;
  auth?: PulsarAuthConfig;
  client?: PulsarClient;
  pulsar?: PulsarModule;
  tenant?: string;
  namespace?: string;
  topicPrefix?: string;
  producerSendTimeoutMs?: number;
  readTimeoutMs?: number;
  tailReadTimeoutMs?: number;
  dedupCapacity?: number;
  producerCacheCapacity?: number;
}

export interface PulsarSessionEventSourceOptions {
  serviceUrl?: string;
  auth?: PulsarAuthConfig;
  client?: PulsarClient;
  tenant?: string;
  namespace?: string;
  topicPrefix?: string;
  subscription: string;
  receiveTimeoutMs?: number;
  ackTimeoutMs?: number;
  /** Maximum handler/repair calls in flight across all sessions. Defaults to 8. */
  maxConcurrentHandlers?: number;
  /** Maximum active plus queued deliveries; receiving pauses at this limit. Defaults to 100. */
  maxPendingDeliveries?: number;
  /**
   * Negative-ack redelivery delay (`nAckRedeliverTimeoutMs` on the consumer).
   * Always set explicitly so the nack quiescence window that gates idle
   * consumer refresh is deterministic. Defaults to
   * `DEFAULT_EVENT_SOURCE_NACK_REDELIVER_TIMEOUT_MS`.
   */
  nAckRedeliverTimeoutMs?: number;
  /**
   * When > 0, an idle pattern consumer is torn down and re-subscribed every
   * `topicRediscoverIntervalMs` ms so newly-created session topics are picked
   * up without waiting for Pulsar's native pattern auto-discovery period.
   * Defaults to 5s because the Node pulsar-client binding does not reliably
   * honor `patternAutoDiscoveryPeriod`; callers can explicitly pass 0 only for
   * pre-provisioned-topic environments. Refresh is gated by nack quiescence to
   * avoid the native close/re-subscribe use-after-free path.
   */
  topicRediscoverIntervalMs?: number;
  /**
   * Native pattern auto-discovery period passed to the Pulsar client. Pulsar's
   * regex consumers may discover newly-created matching topics without
   * recreating the consumer, but this is best-effort in the Node binding.
   */
  patternAutoDiscoveryPeriodSeconds?: number;
  includeAllEvents?: boolean;
}

export type PulsarAuthConfig =
  | {
      type: 'token';
      token: string;
    }
  | {
      type: 'oauth2';
      issuerUrl: string;
      clientId?: string;
      clientSecret?: string;
      privateKey?: string;
      audience?: string;
      scope?: string;
      oauth2Type?: string;
    };

export type PulsarSessionEventHandler = (event: Event) => Promise<void>;

export class PulsarTranscriptStore implements TranscriptStore {
  private readonly tenant: string;
  private readonly namespace: string;
  private readonly topicPrefix: string;
  private readonly readTimeoutMs: number;
  private readonly tailReadTimeoutMs: number;
  private readonly dedupCapacity: number;
  private readonly producerCacheCapacity: number;
  private client: PulsarClient | null;
  private pulsar: PulsarModule | null;
  private readonly producerByTopic = new Map<string, PulsarProducer>();
  private readonly seenIdsByTopic = new Map<string, LruDedup<number>>();

  constructor(private readonly opts: PulsarTranscriptStoreOptions) {
    this.tenant = opts.tenant ?? DEFAULT_TENANT;
    this.namespace = opts.namespace ?? DEFAULT_NAMESPACE;
    this.topicPrefix = opts.topicPrefix ?? DEFAULT_TOPIC_PREFIX;
    this.readTimeoutMs = opts.readTimeoutMs ?? DEFAULT_READ_TIMEOUT_MS;
    this.tailReadTimeoutMs = opts.tailReadTimeoutMs ?? DEFAULT_TAIL_READ_TIMEOUT_MS;
    this.dedupCapacity = opts.dedupCapacity ?? DEFAULT_DEDUP_CAPACITY;
    this.producerCacheCapacity = opts.producerCacheCapacity ?? DEFAULT_PRODUCER_CACHE_CAPACITY;
    if (this.dedupCapacity <= 0) throw new Error('dedupCapacity must be > 0');
    if (this.producerCacheCapacity <= 0) throw new Error('producerCacheCapacity must be > 0');
    this.client = opts.client ?? null;
    this.pulsar = opts.pulsar ?? null;
  }

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    if (events.length === 0) return [];
    assertEventsMatchRoute(workspaceId, sessionId, events);
    const stop = appendLatency.startTimer();
    try {
      const topic = this.topicName(workspaceId, sessionId);
      const producer = await this.producerFor(topic);
      const seenIds = this.seenIdsFor(topic);
      const newEvents: Event[] = [];
      const pendingEventsById = new Map<string, Event[]>();
      for (const event of events) {
        const dedupSeq = seenIds.get(event.id);
        if (dedupSeq !== undefined) {
          event.seq = dedupSeq;
          dedupHits.inc();
          continue;
        }
        const pendingEvents = pendingEventsById.get(event.id);
        if (pendingEvents) {
          pendingEvents.push(event);
          dedupHits.inc();
          continue;
        }
        if (event.subpath && event.subpath.length > 0) {
          subagentMessageRate.inc({ workspace_id: workspaceId, produced_by: event.producedBy });
        }
        pendingEventsById.set(event.id, [event]);
        newEvents.push(event);
      }
      if (newEvents.length > 0) {
        // Queue the whole append before flushing: awaiting each send would let
        // consumers observe the first event before its companion is published.
        const sendPromises = newEvents.map((event) =>
          producer.send({
            data: Buffer.from(event.payload),
            properties: eventToProperties(event),
            eventTimestamp: Date.parse(event.producedAt),
            partitionKey: sessionId,
          }),
        );
        const results = await Promise.allSettled([producer.flush(), ...sendPromises]);
        const flushResult = results[0] as PromiseSettledResult<void>;
        const sendResults = results.slice(1) as PromiseSettledResult<PulsarMessageId>[];
        for (let i = 0; i < sendResults.length; i++) {
          const sendResult = sendResults[i]!;
          if (sendResult.status === 'rejected') continue;
          const event = newEvents[i]!;
          const seq = pulsarMessageIdSeq(sendResult.value, event.seq);
          for (const pendingEvent of pendingEventsById.get(event.id)!) {
            pendingEvent.seq = seq;
          }
          seenIds.add(event.id, seq);
        }
        if (flushResult.status === 'rejected') throw flushResult.reason;
        const failedSend = sendResults.find((result) => result.status === 'rejected');
        if (failedSend?.status === 'rejected') throw failedSend.reason;
      }
      appendTotal.inc({ status: 'ok' });
      return events.map((event) => event.id);
    } catch (err) {
      appendTotal.inc({ status: 'error' });
      throw err;
    } finally {
      stop();
    }
  }

  async *read(workspaceId: string, sessionId: string, opts: ReadOptions): AsyncIterable<Event> {
    let reader: PulsarReader | null = null;
    const startedAt = Date.now();
    let firstSeen = false;
    try {
      const module = await this.module();
      const topic = this.topicName(workspaceId, sessionId);
      const expectedRoute = { workspaceId, sessionId };
      // Only the broker-backed hasNext check can establish the end of the
      // available transcript across all writers. A read timeout (or other
      // failure) after it reports more messages must reject the scan: recovery
      // uses a completed scan as proof that a stable event ID is absent.
      reader = await this.createReader(topic, module.MessageId.earliest());
      const fromSeq = parseCursor(opts.fromCursor) ?? 0;
      const maxEvents = opts.maxEvents > 0 ? opts.maxEvents : Number.POSITIVE_INFINITY;
      let delivered = 0;
      let fallbackSeq = 0;

      while (delivered < maxEvents) {
        if (!(await reader.hasNext())) break;
        const message = await reader.readNext(this.readTimeoutMs);
        const event = messageToEventForRoute(
          message,
          fallbackSeq,
          {
            tenant: this.tenant,
            namespace: this.namespace,
            topicPrefix: this.topicPrefix,
          },
          expectedRoute,
        );
        fallbackSeq += 1;
        if (!event) continue;
        if (event.seq < fromSeq || !matchesSubpath(event.subpath, opts.subpath)) continue;
        if (!firstSeen) {
          firstSeen = true;
          readFirstByteLatency.observe((Date.now() - startedAt) / 1000);
        }
        delivered += 1;
        yield event;
      }
      readTotal.inc({ status: 'ok' });
    } catch (err) {
      readTotal.inc({ status: 'error' });
      throw err;
    } finally {
      if (reader) await reader.close().catch(() => null);
    }
  }

  async *tail(workspaceId: string, sessionId: string, opts: TailOptions): AsyncIterable<Event> {
    let consumer: PulsarConsumer | null = null;
    try {
      const parsedCursor = parseCursor(opts.fromCursor);
      const expectedRoute = { workspaceId, sessionId };
      const client = await this.ensureClient();
      consumer = await client.subscribe({
        topic: pulsarTopicName({
          tenant: this.tenant,
          namespace: this.namespace,
          topicPrefix: this.topicPrefix,
          workspaceId,
          sessionId,
        }),
        subscription: `orca-tail-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        subscriptionType: 'Shared',
        subscriptionInitialPosition: parsedCursor === null ? 'Latest' : 'Earliest',
        receiverQueueSize: 100,
      });
      opts.onReady?.();
      let fallbackSeq = 0;

      while (!opts.signal?.aborted) {
        const message = await readConsumerMessageOrNull(consumer, this.tailReadTimeoutMs);
        if (!message) continue;
        const event = messageToEventForRoute(
          message,
          fallbackSeq,
          {
            tenant: this.tenant,
            namespace: this.namespace,
            topicPrefix: this.topicPrefix,
          },
          expectedRoute,
        );
        fallbackSeq += 1;
        await consumer.acknowledge(message).catch(() => null);
        if (!event) continue;
        if (parsedCursor !== null && event.seq < parsedCursor) continue;
        if (!matchesSubpath(event.subpath, opts.subpath)) continue;
        yield event;
      }
      tailTotal.inc({ status: 'ok' });
    } catch (err) {
      if (!opts.signal?.aborted) {
        tailTotal.inc({ status: 'error' });
        throw err;
      }
    } finally {
      if (consumer) {
        await consumer.unsubscribe().catch(() => null);
        await consumer.close().catch(() => null);
      }
    }
  }

  async archive(workspaceId: string, sessionId: string): Promise<void> {
    try {
      const sentinel: Event = {
        id: uuidv7(),
        workspaceId,
        sessionId,
        subpath: '',
        seq: 0,
        producedAt: new Date().toISOString(),
        producedBy: 'transcript-store',
        kind: 'session.archived',
        payload: new Uint8Array(),
        idempotencyKey: '',
      };
      await this.append(workspaceId, sessionId, [sentinel]);
      archiveTotal.inc({ status: 'ok' });
    } catch (err) {
      archiveTotal.inc({ status: 'error' });
      throw err;
    }
  }

  async close(): Promise<void> {
    const producers = [...this.producerByTopic.values()];
    this.producerByTopic.clear();
    this.seenIdsByTopic.clear();
    await Promise.all(producers.map((producer) => producer.close().catch(() => null)));
    if (this.client) {
      await this.client.close().catch(() => null);
      this.client = null;
    }
  }

  private async createReader(
    topic: string,
    startMessageId: PulsarMessageId,
  ): Promise<PulsarReader> {
    const client = await this.ensureClient();
    return client.createReader({
      topic,
      startMessageId,
      receiverQueueSize: 100,
    });
  }

  private topicName(workspaceId: string, sessionId: string): string {
    return pulsarTopicName({
      tenant: this.tenant,
      namespace: this.namespace,
      topicPrefix: this.topicPrefix,
      workspaceId,
      sessionId,
    });
  }

  private async producerFor(topic: string): Promise<PulsarProducer> {
    const existing = this.producerByTopic.get(topic);
    if (existing) {
      this.producerByTopic.delete(topic);
      this.producerByTopic.set(topic, existing);
      return existing;
    }
    const client = await this.ensureClient();
    const producerConfig: PulsarProducerConfig = {
      topic,
      batchingEnabled: true,
      batchingType: 'KeyBasedBatching',
      // append() flushes explicitly; keep the native timer from splitting a
      // batch while its send calls are still being enqueued.
      batchingMaxPublishDelayMs: APPEND_BATCH_MAX_PUBLISH_DELAY_MS,
      batchingMaxMessages: APPEND_BATCH_MAX_MESSAGES,
      batchingMaxAllowedSizeInBytes: APPEND_BATCH_MAX_BYTES,
      blockIfQueueFull: true,
    };
    if (this.opts.producerSendTimeoutMs !== undefined) {
      producerConfig.sendTimeoutMs = this.opts.producerSendTimeoutMs;
    }
    const producer = await client.createProducer(producerConfig);
    this.producerByTopic.set(topic, producer);
    await this.evictOldestProducers();
    return producer;
  }

  private seenIdsFor(topic: string): LruDedup<number> {
    let seen = this.seenIdsByTopic.get(topic);
    if (!seen) {
      seen = new LruDedup<number>(this.dedupCapacity);
      this.seenIdsByTopic.set(topic, seen);
    }
    return seen;
  }

  private async evictOldestProducers(): Promise<void> {
    while (this.producerByTopic.size > this.producerCacheCapacity) {
      const oldestTopic = this.producerByTopic.keys().next().value;
      if (oldestTopic === undefined) return;
      const producer = this.producerByTopic.get(oldestTopic);
      this.producerByTopic.delete(oldestTopic);
      this.seenIdsByTopic.delete(oldestTopic);
      await producer?.close().catch(() => null);
    }
  }

  private async ensureClient(): Promise<PulsarClient> {
    if (this.client) return this.client;
    const module = await this.module();
    // Another first read/append can finish the import while this call waits.
    // Keep one owned client: overwriting it lets GC destroy a native client
    // whose producers/readers are still in use.
    if (this.client) return this.client;
    this.client = new module.Client(
      buildPulsarClientConfig(module, this.opts.serviceUrl, this.opts.auth),
    );
    return this.client;
  }

  private async module(): Promise<PulsarModule> {
    if (this.pulsar) return this.pulsar;
    this.pulsar = await loadPulsarModule();
    return this.pulsar;
  }
}

export class PulsarSessionEventSource {
  private readonly tenant: string;
  private readonly namespace: string;
  private readonly topicPrefix: string;
  private readonly receiveTimeoutMs: number;
  private readonly topicRediscoverIntervalMs: number;
  private readonly patternAutoDiscoveryPeriodSeconds: number;
  private readonly nAckRedeliverTimeoutMs: number;
  private readonly nackQuiescenceMs: number;
  private readonly retiredConsumerHoldMs: number;
  private readonly maxConcurrentHandlers: number;
  private readonly maxPendingDeliveries: number;
  private client: PulsarClient | null;
  private consumer: PulsarConsumer | null = null;
  private retiredConsumers: Array<{ consumer: PulsarConsumer; releaseAt: number }> = [];
  private consumerStartedAt = 0;
  private lastNackAt = 0;
  /**
   * Nacked messages whose redelivery has not been observed yet. Incremented
   * on every negativeAcknowledge, decremented when a message with a non-zero
   * redelivery count is received (the tracker timer fired and released its
   * entry). Zero means the native NegativeAcksTracker is empirically drained,
   * so an idle consumer refresh is safe without waiting out the time window.
   */
  private outstandingNacks = 0;
  private running = false;
  private stopped = true;
  private terminalRunLoopError = false;
  private runPromise: Promise<void> | null = null;
  private retryAbort = new AbortController();
  private readonly sessionLanes = new Map<string, Promise<void>>();
  private activeHandlers = 0;
  private pendingDeliveries = 0;
  private resolveDispatchChange!: () => void;
  private dispatchChanged = new Promise<void>((resolve) => {
    this.resolveDispatchChange = resolve;
  });
  private readonly failed: Promise<void>;
  private resolveFailed!: () => void;

  constructor(private readonly opts: PulsarSessionEventSourceOptions) {
    this.tenant = opts.tenant ?? DEFAULT_TENANT;
    this.namespace = opts.namespace ?? DEFAULT_NAMESPACE;
    this.topicPrefix = opts.topicPrefix ?? DEFAULT_TOPIC_PREFIX;
    this.receiveTimeoutMs = opts.receiveTimeoutMs ?? DEFAULT_EVENT_SOURCE_RECEIVE_TIMEOUT_MS;
    this.topicRediscoverIntervalMs =
      opts.topicRediscoverIntervalMs ?? DEFAULT_EVENT_SOURCE_TOPIC_REDISCOVER_INTERVAL_MS;
    this.patternAutoDiscoveryPeriodSeconds =
      opts.patternAutoDiscoveryPeriodSeconds ?? DEFAULT_EVENT_SOURCE_PATTERN_AUTO_DISCOVERY_SECONDS;
    this.nAckRedeliverTimeoutMs =
      opts.nAckRedeliverTimeoutMs ?? DEFAULT_EVENT_SOURCE_NACK_REDELIVER_TIMEOUT_MS;
    this.nackQuiescenceMs = 2 * this.nAckRedeliverTimeoutMs + NACK_QUIESCENCE_SLACK_MS;
    this.retiredConsumerHoldMs = Math.max(this.nackQuiescenceMs, RETIRED_CONSUMER_MIN_HOLD_MS);
    this.maxConcurrentHandlers =
      opts.maxConcurrentHandlers ?? DEFAULT_EVENT_SOURCE_MAX_CONCURRENT_HANDLERS;
    this.maxPendingDeliveries =
      opts.maxPendingDeliveries ?? DEFAULT_EVENT_SOURCE_MAX_PENDING_DELIVERIES;
    for (const [name, value] of [
      ['maxConcurrentHandlers', this.maxConcurrentHandlers],
      ['maxPendingDeliveries', this.maxPendingDeliveries],
    ] as const) {
      if (!Number.isSafeInteger(value) || value < 1)
        throw new Error(`${name} must be a positive safe integer`);
    }
    this.client = opts.client ?? null;
    this.failed = new Promise<void>((resolve) => {
      this.resolveFailed = resolve;
    });
  }

  async start(handler: PulsarSessionEventHandler): Promise<void> {
    if (this.runPromise) return;
    this.stopped = false;
    this.retryAbort = new AbortController();
    // Each retained delivery can have one abortable repair wait. Their count
    // is bounded by admission, so allow that many legitimate signal listeners.
    setMaxListeners(this.maxPendingDeliveries, this.retryAbort.signal);
    this.terminalRunLoopError = false;
    this.running = true;
    // run()'s per-iteration try/catch can itself throw (e.g.
    // consumer.negativeAcknowledge in the catch branch). Consume that
    // rejection here so Node does not turn it into an unhandled rejection,
    // and expose only a stable failed state to readiness callers.
    this.runPromise = this.run(handler).then(
      () => {
        this.running = false;
      },
      (error: unknown) => this.failRunLoop(error),
    );
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.retryAbort.abort();
    this.notifyDispatchChange();
    // A receive(timeout) runs on a native worker. Closing first races that
    // worker and reproduces apache/pulsar-client-cpp#453; the bounded receive
    // timeout lets the loop observe `stopped` and settle before teardown.
    await this.runPromise;
    // No new deliveries can be queued now. Finish active handlers before
    // closing their consumer; queued work observes the abort and stays unacked.
    await Promise.all(this.sessionLanes.values());
    if (this.consumer) {
      // Give an armed nack timer a bounded chance to fire before close() — the
      // same use-after-free refreshIdleConsumerIfDue defers around, but capped
      // at STOP_NACK_DRAIN_MAX_WAIT_MS so shutdown stays fast. With the
      // default 10s redelivery delay the timer can outlive the cap; that
      // residual race is accepted on the exit path.
      if (this.outstandingNacks > 0 && this.lastNackAt > 0) {
        const remaining = this.nackQuiescenceMs - (Date.now() - this.lastNackAt);
        const wait = Math.min(Math.max(remaining, 0), STOP_NACK_DRAIN_MAX_WAIT_MS);
        if (wait > 0) await delay(wait);
      }
      const consumer = this.consumer;
      this.consumer = null;
      await this.closeAndRetainConsumer(consumer);
    }
    if (this.client) {
      await this.client.close().catch(() => null);
      this.client = null;
    }
  }

  status(): SessionEventSourceStatus {
    if (this.stopped) return { ready: false, state: 'stopped' };
    if (this.terminalRunLoopError) return { ready: false, state: 'failed' };
    return { ready: this.running, state: this.running ? 'running' : 'stopped' };
  }

  /** Resolves only when an unintentional run-loop failure escapes. */
  whenFailed(): Promise<void> {
    return this.failed;
  }

  private async run(handler: PulsarSessionEventHandler): Promise<void> {
    let fallbackSeq = 0;
    while (!this.retryAbort.signal.aborted) {
      while (
        this.pendingDeliveries >= this.maxPendingDeliveries &&
        !this.retryAbort.signal.aborted
      ) {
        await this.dispatchChanged;
      }
      if (this.retryAbort.signal.aborted) break;
      const consumer = await this.ensureConsumerOrNull();
      if (!consumer) {
        await delay(this.receiveTimeoutMs);
        continue;
      }
      let message: PulsarMessage | null = null;
      try {
        message = await readConsumerMessageOrNull(consumer, this.receiveTimeoutMs);
        if (this.retryAbort.signal.aborted) break;
        if (!message) {
          // Idle tick: the receiver queue is drained, but that alone is NOT
          // enough to tear the consumer down — a recently nacked message may
          // still sit in the native NegativeAcksTracker, whose redelivery
          // timer survives close() (use-after-free / SIGSEGV in
          // pulsar-client-cpp, see apache/pulsar-client-cpp#453).
          // refreshIdleConsumerIfDue additionally enforces the nack
          // quiescence window before re-subscribing to re-evaluate the
          // session-topic pattern.
          await this.refreshIdleConsumerIfDue();
          continue;
        }
        if (message.getRedeliveryCount() > 0) {
          // A redelivery means the native tracker fired and released the timer
          // entry of an earlier nack (or an ack-timeout fired — see the caveat
          // in refreshIdleConsumerIfDue), retiring one outstanding nack.
          this.outstandingNacks = Math.max(0, this.outstandingNacks - 1);
        }
        const event = messageToEventForRoute(message, fallbackSeq, {
          tenant: this.tenant,
          namespace: this.namespace,
          topicPrefix: this.topicPrefix,
        });
        fallbackSeq += 1;
        if (!event) {
          // Route mismatches are poison input. Ack/drop immediately so they
          // cannot reach a handler or consume the retry budget.
          await consumer.acknowledge(message);
          continue;
        }
        if (
          !this.opts.includeAllEvents &&
          event.kind !== 'session.archived' &&
          event.kind !== 'session.deleted' &&
          (event.producedBy !== 'client' || !event.kind.startsWith('user.'))
        ) {
          await consumer.acknowledge(message);
          continue;
        }
        this.enqueueSessionDelivery(consumer, message, event, handler);
      } catch (err) {
        await this.handleDeliveryFailure(consumer, message, err);
      }
    }
  }

  private enqueueSessionDelivery(
    consumer: PulsarConsumer,
    message: PulsarMessage,
    event: Event,
    handler: PulsarSessionEventHandler,
  ): void {
    this.pendingDeliveries += 1;
    const key = `${event.workspaceId}\0${event.sessionId}`;
    const previous = this.sessionLanes.get(key) ?? Promise.resolve();
    const lane = previous
      .then(async () => {
        if (this.retryAbort.signal.aborted) return;
        try {
          if (await this.handleBeforeNextDelivery(() => handler(event)))
            await consumer.acknowledge(message);
        } catch (err) {
          await this.handleDeliveryFailure(consumer, message, err);
        }
      })
      .catch((error: unknown) => this.failRunLoop(error))
      .finally(() => {
        if (this.sessionLanes.get(key) === lane) this.sessionLanes.delete(key);
        this.pendingDeliveries -= 1;
        this.notifyDispatchChange();
      });
    this.sessionLanes.set(key, lane);
  }

  private failRunLoop(error: unknown): void {
    this.running = false;
    if (this.stopped || this.terminalRunLoopError) return;
    this.terminalRunLoopError = true;
    this.retryAbort.abort();
    this.notifyDispatchChange();
    console.error('PulsarSessionEventSource: run loop crashed', terminalSourceFailureFields(error));
    this.resolveFailed();
  }

  private async handleDeliveryFailure(
    consumer: PulsarConsumer,
    message: PulsarMessage | null,
    err: unknown,
  ): Promise<void> {
    if (this.retryAbort.signal.aborted) return;
    if (
      message &&
      message.getRedeliveryCount() >= MAX_HANDLER_REDELIVERIES &&
      !(err instanceof RetryableSessionEventError)
    ) {
      // Poison backstop: the handler failed on every redelivery, so
      // retrying can no longer help. Ack and drop the message instead of
      // nacking it again, otherwise one bad event loops forever.
      console.error(
        'pulsar session event source dropping poison message after ' +
          `${message.getRedeliveryCount()} redeliveries ` +
          `(topic=${message.getTopicName()}, kind=${message.getProperties()['kind'] ?? ''})`,
        err,
      );
      await consumer.acknowledge(message).catch(() => null);
      return;
    }
    if (message) {
      // Record the nack before the native call: even if it throws we must
      // assume the tracker saw the message.
      this.lastNackAt = Date.now();
      this.outstandingNacks += 1;
      consumer.negativeAcknowledge(message);
    }
    console.error('pulsar session event handler failed', err);
  }

  private async handleBeforeNextDelivery(handler: () => Promise<void>): Promise<boolean> {
    let attempt = handler;
    while (!this.retryAbort.signal.aborted) {
      while (this.activeHandlers >= this.maxConcurrentHandlers && !this.retryAbort.signal.aborted) {
        await this.dispatchChanged;
      }
      if (this.retryAbort.signal.aborted) return false;
      // Reserve synchronously after checking capacity. Returning from a
      // separate async waiter first would let multiple callers claim one slot.
      this.activeHandlers += 1;
      try {
        await attempt();
        return !this.retryAbort.signal.aborted;
      } catch (error) {
        if (this.retryAbort.signal.aborted) return false;
        if (!(error instanceof SessionEventBarrierError)) throw error;
        // A nack allows later queued events to overtake a partially committed
        // terminal outcome during broker redelivery backoff. Retain this
        // delivery and repair that exact outcome before dispatching the next
        // event in this session lane. Other lanes keep receiving and handling.
        attempt = error.retry;
        console.error('pulsar session event outcome repair failed', error);
      } finally {
        this.activeHandlers -= 1;
        this.notifyDispatchChange();
      }
      // A session's backoff retains its delivery but releases execution
      // capacity so unrelated sessions can progress even with a limit of one.
      await abortableDelay(this.nAckRedeliverTimeoutMs, undefined, {
        signal: this.retryAbort.signal,
      }).catch(() => undefined);
    }
    return false;
  }

  private notifyDispatchChange(): void {
    const resolve = this.resolveDispatchChange;
    this.dispatchChanged = new Promise<void>((next) => {
      this.resolveDispatchChange = next;
    });
    resolve();
  }

  private async ensureConsumerOrNull(): Promise<PulsarConsumer | null> {
    if (this.consumer) return this.consumer;
    try {
      const client = await this.ensureClient();
      const consumerConfig: PulsarConsumerConfig = {
        topicsPattern: pulsarTopicPattern({
          tenant: this.tenant,
          namespace: this.namespace,
          topicPrefix: this.topicPrefix,
        }),
        subscription: this.opts.subscription,
        subscriptionType: 'KeyShared',
        keySharedPolicy: { keyShareMode: 'AutoSplit', allowOutOfOrderDelivery: false },
        subscriptionInitialPosition: 'Earliest',
        receiverQueueSize: 100,
        patternAutoDiscoveryPeriod: this.patternAutoDiscoveryPeriodSeconds,
        // Explicit so the nack quiescence window in refreshIdleConsumerIfDue
        // is deterministic instead of relying on the library default.
        nAckRedeliverTimeoutMs: this.nAckRedeliverTimeoutMs,
      };
      if (this.opts.ackTimeoutMs !== undefined)
        consumerConfig.ackTimeoutMs = this.opts.ackTimeoutMs;
      this.consumer = await client.subscribe(consumerConfig);
      this.consumerStartedAt = Date.now();
      return this.consumer;
    } catch (err) {
      await this.consumer?.close().catch(() => null);
      this.consumer = null;
      if (!this.stopped) console.error('pulsar session event source subscribe failed', err);
      return null;
    }
  }

  /**
   * Periodically drop idle pattern consumer so next `ensureConsumerOrNull`
   * re-subscribes and re-evaluates topic pattern. Invariant for closing here:
   * the receiver queue is drained (idle branch only) AND the native
   * NegativeAcksTracker holds no armed timer. The tracker's redelivery timer
   * survives `close()`; closing while a nacked message is still tracked is a
   * use-after-free (SIGSEGV) in pulsar-client-cpp
   * (apache/pulsar-client-cpp#453).
   *
   * Two gates decide when the tracker is quiescent:
   * - `outstandingNacks === 0`: every nack was observed coming back as a
   *   redelivery, so the tracker is empirically drained — refresh immediately.
   *   This keeps topic discovery responsive even while some handler keeps
   *   failing (each redelivery cycle re-opens a refresh window), instead of a
   *   persistent failure deferring discovery indefinitely.
   * - the quiescence time window: bounds the deferral when the counter is
   *   stale — after a key ownership change another replica may receive the
   *   redelivery, so the counter here never drains. Caveat in the other
   *   direction: with `ackTimeoutMs` configured, an ack-timeout redelivery can
   *   retire the counter before a distinct nack timer has fired; the residual
   *   race is accepted for that opt-in configuration.
   */
  private async refreshIdleConsumerIfDue(): Promise<void> {
    if (this.stopped || !this.consumer || this.topicRediscoverIntervalMs <= 0) return;
    // Retained deliveries and their queued successors still belong to this
    // consumer. Closing it would allow broker redelivery during local repair.
    if (this.sessionLanes.size > 0) return;
    if (Date.now() - this.consumerStartedAt < this.topicRediscoverIntervalMs) return;
    if (
      this.outstandingNacks > 0 &&
      this.lastNackAt > 0 &&
      Date.now() - this.lastNackAt < this.nackQuiescenceMs
    ) {
      return;
    }
    const consumer = this.consumer;
    this.consumer = null;
    this.consumerStartedAt = 0;
    // The nack bookkeeping belonged to the consumer just closed; the next
    // consumer starts with a fresh (empty) tracker.
    this.lastNackAt = 0;
    this.outstandingNacks = 0;
    await this.closeAndRetainConsumer(consumer);
  }

  /**
   * Close the broker-side consumer but retain the Node wrapper while native
   * pattern/nack callbacks drain. `pulsar_consumer_close_async` does not free
   * the C handle; the wrapper's finalizer does. Dropping the last JS reference
   * immediately after close therefore reintroduces the use-after-free that the
   * pre-close quiescence gate alone cannot prevent.
   */
  private async closeAndRetainConsumer(consumer: PulsarConsumer): Promise<void> {
    const now = Date.now();
    this.retiredConsumers = this.retiredConsumers.filter(({ releaseAt }) => releaseAt > now);
    this.retiredConsumers.push({ consumer, releaseAt: now + this.retiredConsumerHoldMs });
    await consumer.close().catch(() => null);
  }

  private async ensureClient(): Promise<PulsarClient> {
    if (this.client) return this.client;
    const module = await loadPulsarModule();
    this.client = new module.Client(
      buildPulsarClientConfig(module, this.opts.serviceUrl, this.opts.auth),
    );
    return this.client;
  }
}

export function pulsarTopicName(input: {
  tenant?: string;
  namespace?: string;
  topicPrefix?: string;
  workspaceId: string;
  sessionId: string;
}): string {
  const tenant = input.tenant ?? DEFAULT_TENANT;
  const namespace = input.namespace ?? DEFAULT_NAMESPACE;
  const topicPrefix = input.topicPrefix ?? DEFAULT_TOPIC_PREFIX;
  assertPulsarNameSegment('tenant', tenant);
  assertPulsarNameSegment('namespace', namespace);
  assertPulsarNameSegment('topicPrefix', topicPrefix);
  assertPulsarRouteId('workspaceId', input.workspaceId);
  assertPulsarRouteId('sessionId', input.sessionId);
  return `persistent://${tenant}/${namespace}/${topicPrefix}.${input.workspaceId}.sessions.${input.sessionId}.events`;
}

function pulsarTopicPattern(input: {
  tenant: string;
  namespace: string;
  topicPrefix: string;
}): string {
  assertPulsarNameSegment('tenant', input.tenant);
  assertPulsarNameSegment('namespace', input.namespace);
  assertPulsarNameSegment('topicPrefix', input.topicPrefix);
  return (
    `persistent://${escapeRegex(input.tenant)}/${escapeRegex(input.namespace)}/` +
    `${escapeRegex(input.topicPrefix)}\\.([A-Za-z0-9_-]+)\\.sessions\\.` +
    `([A-Za-z0-9_-]+)\\.events(?:-partition-\\d+)?`
  );
}

function matchPulsarSessionTopic(
  topic: string,
  input: { tenant: string; namespace: string; topicPrefix: string },
): SessionRoute | null {
  assertPulsarNameSegment('tenant', input.tenant);
  assertPulsarNameSegment('namespace', input.namespace);
  assertPulsarNameSegment('topicPrefix', input.topicPrefix);
  const pattern = new RegExp(
    `^persistent://${escapeRegex(input.tenant)}/${escapeRegex(input.namespace)}/` +
      `${escapeRegex(input.topicPrefix)}\\.([A-Za-z0-9_-]+)\\.sessions\\.` +
      `([A-Za-z0-9_-]+)\\.events(?:-partition-\\d+)?$`,
  );
  const match = pattern.exec(topic);
  return match ? { workspaceId: match[1]!, sessionId: match[2]! } : null;
}

const PULSAR_NAME_SEGMENT = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const PULSAR_ROUTE_ID = /^[A-Za-z0-9_-]+$/;

function assertPulsarNameSegment(label: string, value: string): void {
  if (!PULSAR_NAME_SEGMENT.test(value) || value === '.' || value === '..') {
    throw new Error(`invalid Pulsar ${label}: ${value}`);
  }
}

function assertPulsarRouteId(label: string, value: string): void {
  if (!PULSAR_ROUTE_ID.test(value)) {
    throw new Error(`invalid Pulsar ${label}: ${value}`);
  }
}

function buildPulsarClientConfig(
  module: PulsarModule,
  serviceUrl: string | undefined,
  auth: PulsarAuthConfig | undefined,
): PulsarClientConfig {
  const config: PulsarClientConfig = {
    serviceUrl: serviceUrl ?? 'pulsar://localhost:6650',
  };
  if (!auth) return config;

  if (auth.type === 'token') {
    config.authentication = new module.AuthenticationToken({ token: auth.token });
    return config;
  }

  const params: PulsarOauth2Params = {
    type: auth.oauth2Type ?? 'client_credentials',
    issuer_url: auth.issuerUrl,
  };
  if (auth.clientId !== undefined) params.client_id = auth.clientId;
  if (auth.clientSecret !== undefined) params.client_secret = auth.clientSecret;
  if (auth.privateKey !== undefined) params.private_key = auth.privateKey;
  if (auth.audience !== undefined) params.audience = auth.audience;
  if (auth.scope !== undefined) params.scope = auth.scope;
  config.authentication = new module.AuthenticationOauth2(params);
  return config;
}

export function normalizePulsarModule(imported: unknown): PulsarModule {
  const candidates = [
    imported,
    (imported as { default?: unknown } | null)?.default,
    (imported as { 'module.exports'?: unknown } | null)?.['module.exports'],
  ];
  for (const candidate of candidates) {
    const module = candidate as Partial<PulsarModule> | null | undefined;
    if (
      typeof module?.Client === 'function' &&
      module.MessageId &&
      typeof module.AuthenticationToken === 'function' &&
      typeof module.AuthenticationOauth2 === 'function'
    ) {
      return module as PulsarModule;
    }
  }
  throw new Error('pulsar-client module does not expose Client/MessageId/authentication classes');
}

async function loadPulsarModule(): Promise<PulsarModule> {
  return normalizePulsarModule(await import('pulsar-client'));
}

function eventToProperties(event: Event): Record<string, string> {
  const userId = event.userId;
  return {
    id: event.id,
    workspace_id: event.workspaceId,
    session_id: event.sessionId,
    subpath: event.subpath,
    produced_at: event.producedAt,
    produced_by: event.producedBy,
    kind: event.kind,
    idempotency_key: event.idempotencyKey,
    ...(userId !== undefined && userId.length > 0 ? { user_id: userId } : {}),
  };
}

function messageToEvent(message: PulsarMessage, fallbackSeq: number): Event {
  const props = message.getProperties();
  const userId = props['user_id'];
  return {
    id: props['id'] ?? '',
    workspaceId: props['workspace_id'] ?? '',
    sessionId: props['session_id'] ?? '',
    subpath: props['subpath'] ?? '',
    seq: pulsarMessageSeq(message, fallbackSeq),
    producedAt: props['produced_at'] ?? new Date(message.getPublishTimestamp()).toISOString(),
    producedBy: props['produced_by'] ?? '',
    kind: props['kind'] ?? '',
    payload: new Uint8Array(message.getData()),
    idempotencyKey: props['idempotency_key'] ?? '',
    ...(userId !== undefined && userId.length > 0 ? { userId } : {}),
  };
}

function messageToEventForRoute(
  message: PulsarMessage,
  fallbackSeq: number,
  topicConfig: { tenant: string; namespace: string; topicPrefix: string },
  expectedRoute?: SessionRoute,
): Event | null {
  const actualRoute = matchPulsarSessionTopic(message.getTopicName(), topicConfig);
  if (!actualRoute) return null;
  if (
    expectedRoute &&
    (actualRoute.workspaceId !== expectedRoute.workspaceId ||
      actualRoute.sessionId !== expectedRoute.sessionId)
  ) {
    return null;
  }
  const event = messageToEvent(message, fallbackSeq);
  if (!eventMatchesRoute(event, actualRoute)) return null;
  return event;
}

function pulsarMessageSeq(message: PulsarMessage, fallbackSeq: number): number {
  return pulsarMessageIdSeq(message.getMessageId(), fallbackSeq);
}

function pulsarMessageIdSeq(messageId: { toString(): string }, fallbackSeq: number): number {
  const raw = messageId.toString().trim();
  const direct = parseSafeNonNegativeInteger(raw);
  if (direct !== null) return direct;

  const parts = pulsarMessageIdParts(raw);
  if (parts.length >= 2) {
    const ledger = parseBigInt(parts[0]);
    const entry = parseBigInt(parts[1]);
    if (ledger !== null && entry !== null) {
      const batch = pulsarMessageIdBatch(parts);
      const normalizedBatch = batch > 0n ? batch : 0n;
      const composite =
        ledger * PULSAR_LEDGER_SEQ_MULTIPLIER +
        entry * PULSAR_BATCH_SEQ_MULTIPLIER +
        normalizedBatch;
      if (composite <= POSTGRES_BIGINT_MAX) return Number(composite);
    }
  }

  return fallbackSeq;
}

function pulsarMessageIdParts(raw: string): string[] {
  const parenthesized = /^\((.*)\)$/.exec(raw);
  const value = parenthesized ? parenthesized[1]! : raw;
  const delimiter = value.includes(',') ? ',' : ':';
  return value.split(delimiter).map((part) => part.trim());
}

function pulsarMessageIdBatch(parts: string[]): bigint {
  return parts.length >= 4 ? (parseBigInt(parts[3]) ?? 0n) : 0n;
}

function parseSafeNonNegativeInteger(value: string): number | null {
  if (!/^\d+$/.test(value)) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function parseBigInt(value: string | undefined): bigint | null {
  if (value === undefined || !/^-?\d+$/.test(value)) return null;
  return BigInt(value);
}

function parseCursor(cursor: string): number | null {
  if (cursor === '') return null;
  const parsed = Number(cursor);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : null;
}

async function readConsumerMessageOrNull(
  consumer: PulsarConsumer,
  timeoutMs: number,
): Promise<PulsarMessage | null> {
  try {
    return await consumer.receive(timeoutMs);
  } catch {
    return null;
  }
}

async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function matchesSubpath(eventSubpath: string, filter: string): boolean {
  if (filter === '') return eventSubpath === '';
  if (filter === '*') return true;
  return eventSubpath === filter;
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
