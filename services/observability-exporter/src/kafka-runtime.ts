// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { createKafkaTranscriptCodec, type KafkaTranscriptCodec } from '@orca/transcript-store';
import kafkajs, {
  type Admin,
  type Consumer,
  type EachBatchPayload,
  type Kafka,
  type Producer,
} from 'kafkajs';
import { KafkaSharedState } from './kafka-shared-state.js';
import {
  initialKafkaState,
  migrateKafkaCheckpoint,
  loadKafkaState,
  validateKafkaState,
  projectKafkaState,
  kafkaStateKeys,
  type LoadedKafkaState,
  type KafkaStateProjection,
} from './kafka-state-v2.js';
import {
  decodeKafkaDelivery,
  encodeKafkaDelivery,
  kafkaRecordBytes,
  sendKafkaStateRecords,
} from './kafka-state-io.js';
import type { KafkaStateRecord } from './kafka-blob.js';
import { deliverKafkaTrace } from './kafka-delivery.js';
import { deliveryRetryDelayMs } from './delivery-retry.js';
import { isLangfuseHttpJsonContext } from './delivery-capabilities.js';
import { parseKafkaDeliveryContext } from './kafka-validation.js';
import {
  validateKafkaTranscriptHeaders,
  validateKafkaTranscriptOffsets,
  type DecodedKafkaTranscript,
  type KafkaSessionRoute,
} from './kafka-state.js';
import type { RegistryObservabilityClient } from './registry-client.js';
import type { CaptureMode } from './types.js';
import { StartupAdmission } from './startup-admission.js';

// KafkaJS is CommonJS; Node's native ESM loader does not expose this as a named export.
const { ConfigResourceTypes } = kafkajs;

/** Only raised before opening a transaction; its candidate can safely be discarded. */
class CaptureBatchExpiredError extends Error {}

export interface KafkaOnlyExporterRuntimeOptions {
  kafka: Kafka;
  /** Injected codecs are shared and remain caller-owned; the default is runtime-owned. */
  codec?: KafkaTranscriptCodec;
  /** Stable deployment namespace: never use a process/Pod ID. */
  groupId: string;
  /** Initial authoritative Session topics; one partition each. More may be added after start. */
  sessions: readonly KafkaSessionRoute[];
  /** Provisioned single-partition topic with cleanup.policy=compact (not delete). */
  checkpointTopic: string;
  /** Provisioned non-compacted topic with unlimited retention. */
  deliveryTopic: string;
  registryClient: Pick<RegistryObservabilityClient, 'resolveContext' | 'resolveSecret'>;
  registryRequestTimeoutMs?: number;
  /** Maximum age of a fresh content authorization before projection transaction admission. */
  captureBatchTimeoutMs?: number;
  snapshotTimeoutMs?: number;
  stateDirectory?: string;
  stateMaxBytes?: number;
  stateCatchupTimeoutMs?: number;
  maxAssemblyBytes?: number;
  maxTransactionBytes?: number;
  projectorConcurrency?: number;
  maxCheckpointBytes?: number;
  maxDeliveryBytes?: number;
  batchSize?: number;
  startupConcurrency?: number;
  restoreConcurrency?: number;
  otlpFetchImpl?: typeof fetch;
  /** Internal diagnostic hook; never carries Session identity or payloads. */
  onDiagnostic?: (summary: KafkaRuntimeDiagnostic) => void;
}

interface KafkaRuntimeDiagnostic {
  phase: 'summary' | 'start' | 'stop';
  durationMs: number;
  shutdownDrainMs: number;
  /** Runtime-owned source/delivery handles; excludes the single shared state reader. */
  consumerCount: number;
  producerCount: number;
  startup: ReturnType<StartupAdmission['snapshot']>;
  restore: ReturnType<StartupAdmission['snapshot']>;
  projector: ReturnType<StartupAdmission['snapshot']>;
  largeDelivery: ReturnType<StartupAdmission['snapshot']>;
  sharedReader: Awaited<ReturnType<KafkaSharedState['diagnostics']>>;
  diagnosticErrors: number;
  transactionPeakBytes: number;
  assemblyPeakBytes: number;
}

interface Source {
  route: KafkaSessionRoute;
  consumer: Consumer;
  groupId: string;
  generation: number;
  // Transient projection-size cap survives callback retries, never changes authority.
  captureChunkSize?: number;
  pendingProducer?: Producer;
  owner?: { generation: number; producer: Producer; checkpoint: LoadedKafkaState };
}

const MAX_CAPTURE_EXPIRATIONS_PER_BATCH = 2;

/**
 * Opt-in Kafka-only runtime. No SQL/inbox/claim path is imported. Projection is
 * exactly-once within Kafka; external HTTP delivery is necessarily at-least-once.
 * Retries pause their delivery partition (at most one hour); attempt counters and
 * delay deadlines reset on restart, but the uncommitted record remains durable.
 * Fatal failures stop this instance rather than letting a fenced producer resurrect;
 * ordinary rebalances release local state and rejoin.
 */
export class KafkaOnlyObservabilityExporterRuntime {
  private readonly abort = new AbortController();
  private readonly sources: Source[] = [];
  private readonly producers = new Set<Producer>();
  private readonly closingConsumers = new WeakSet<Consumer>();
  private readonly timers = new Set<ReturnType<typeof setTimeout>>();
  private readonly admin: Admin;
  private delivery: Consumer | undefined;
  private readonly deliveryWork = new Set<Promise<void>>();
  // Track underlying operations, not only heartbeat races which can reject before I/O settles.
  private readonly ownedWork = new Set<Promise<unknown>>();
  private readonly startupAdmission: StartupAdmission;
  private readonly restoreAdmission: StartupAdmission;
  private readonly projectorAdmission: StartupAdmission;
  private readonly largeDeliveryAdmission = new StartupAdmission(1);
  private readonly sharedState: KafkaSharedState;
  private state: 'stopped' | 'starting' | 'running' | 'failed' = 'stopped';
  private startPromise: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;
  private sessionUpdates: Promise<void> = Promise.resolve();
  private readonly registryTimeout: number;
  private readonly captureBatchTimeout: number;
  private readonly codec: KafkaTranscriptCodec;
  private diagnosticTimer: ReturnType<typeof setInterval> | undefined;
  private diagnosticWork: Promise<void> | undefined;
  private diagnosticErrors = 0;
  private consumerCount = 0;
  private transactionPeakBytes = 0;
  private assemblyPeakBytes = 0;
  private shutdownDrainMs = 0;
  private readerDiagnostic: KafkaRuntimeDiagnostic['sharedReader'];

  constructor(private readonly options: KafkaOnlyExporterRuntimeOptions) {
    if (
      !/^[A-Za-z0-9._-]{1,128}$/u.test(options.groupId) ||
      options.checkpointTopic === options.deliveryTopic
    ) {
      throw new Error('invalid Kafka exporter namespace');
    }
    this.registryTimeout = bounded(options.registryRequestTimeoutMs ?? 10_000, 1, 15_000);
    this.captureBatchTimeout = bounded(options.captureBatchTimeoutMs ?? 30_000, 1, 60_000);
    this.startupAdmission = new StartupAdmission(options.startupConcurrency ?? 8);
    this.restoreAdmission = new StartupAdmission(options.restoreConcurrency ?? 2);
    this.projectorAdmission = new StartupAdmission(options.projectorConcurrency ?? 2);
    this.readerDiagnostic = {
      scannedBytes: 0,
      scannedRecords: 0,
      waiters: 0,
      databaseBytes: 0,
      databaseLimitBytes: 0,
      diskQuotaBytes: options.stateMaxBytes ?? 1024 * 1024 * 1024,
    };
    bounded(options.stateCatchupTimeoutMs ?? options.snapshotTimeoutMs ?? 120_000, 1, 600_000);
    bounded(options.stateMaxBytes ?? 1024 * 1024 * 1024, 128 * 1024, Number.MAX_SAFE_INTEGER);
    bounded(options.maxAssemblyBytes ?? 32 * 1024 * 1024, 1, 64 * 1024 * 1024);
    bounded(options.maxTransactionBytes ?? 64 * 1024 * 1024, 1, 128 * 1024 * 1024);
    bounded(options.batchSize ?? 100, 1, 1_000);
    bounded(options.maxCheckpointBytes ?? 512 * 1024, 1, 16 * 1024 * 1024);
    bounded(options.maxDeliveryBytes ?? 512 * 1024, 1, 16 * 1024 * 1024);

    const topics = new Set<string>();
    for (const route of options.sessions) {
      validateSessionRoute(route, options.codec?.encoding ?? 'raw');
      if (
        topics.has(route.topic) ||
        [options.checkpointTopic, options.deliveryTopic].includes(route.topic)
      ) {
        throw new Error('invalid Kafka exporter Session route');
      }
      topics.add(route.topic);
    }
    this.admin = options.kafka.admin();
    this.codec = options.codec ?? createKafkaTranscriptCodec();
    this.sharedState = new KafkaSharedState({
      kafka: options.kafka,
      topic: options.checkpointTopic,
      ...(options.stateDirectory === undefined ? {} : { directory: options.stateDirectory }),
      maxDiskBytes: options.stateMaxBytes ?? 1024 * 1024 * 1024,
      timeoutMs: options.stateCatchupTimeoutMs ?? options.snapshotTimeoutMs ?? 120_000,
      maxRecordBytes: options.maxCheckpointBytes ?? 512 * 1024,
      onFailure: (error) => this.fail(error),
    });
  }

  status(): { ready: boolean; state: 'stopped' | 'starting' | 'running' | 'failed' } {
    return { ready: this.state === 'running' && !this.abort.signal.aborted, state: this.state };
  }

  start(): Promise<void> {
    if (this.startPromise === undefined) {
      const started = performance.now();
      this.startPromise = this.startOnce().finally(() =>
        this.emitDiagnostic('start', performance.now() - started),
      );
    }
    return this.startPromise;
  }

  private emitDiagnostic(phase: KafkaRuntimeDiagnostic['phase'], durationMs = 0): void {
    const summary: KafkaRuntimeDiagnostic = {
      phase,
      durationMs,
      shutdownDrainMs: this.shutdownDrainMs,
      consumerCount: this.consumerCount,
      producerCount: this.producers.size,
      startup: this.startupAdmission.snapshot(),
      restore: this.restoreAdmission.snapshot(),
      projector: this.projectorAdmission.snapshot(),
      largeDelivery: this.largeDeliveryAdmission.snapshot(),
      sharedReader: { ...this.readerDiagnostic },
      diagnosticErrors: this.diagnosticErrors,
      transactionPeakBytes: this.transactionPeakBytes,
      assemblyPeakBytes: this.assemblyPeakBytes,
    };
    // Logging/hook failures cannot change admission, readiness or shutdown.
    try {
      console.info(JSON.stringify(summary));
    } catch {
      /* best effort */
    }
    try {
      this.options.onDiagnostic?.(summary);
    } catch {
      /* best effort */
    }
  }

  private sampleDiagnostics(): void {
    if (this.diagnosticWork !== undefined || this.abort.signal.aborted) return;
    this.diagnosticWork = (async () => {
      try {
        this.readerDiagnostic = await this.sharedState.diagnostics();
      } catch {
        this.diagnosticErrors += 1;
      }
      if (!this.abort.signal.aborted) this.emitDiagnostic('summary');
    })().finally(() => {
      this.diagnosticWork = undefined;
    });
  }

  /** Add-only discovery seam. Concurrent/repeated discoveries never create duplicate consumers. */
  addSessions(routes: readonly KafkaSessionRoute[]): Promise<void> {
    const snapshot = routes.map((route) => ({ ...route }));
    const update = this.sessionUpdates
      .then(async () => {
        this.abort.signal.throwIfAborted();
        await this.start();
        this.abort.signal.throwIfAborted();
        const known = new Set(this.sources.map((source) => source.route.topic));
        const additions: KafkaSessionRoute[] = [];
        for (const route of snapshot) {
          validateSessionRoute(route, this.codec.encoding ?? 'raw');
          if ([this.options.checkpointTopic, this.options.deliveryTopic].includes(route.topic)) {
            throw new Error('invalid Kafka exporter Session route');
          }
          if (known.has(route.topic)) continue;
          known.add(route.topic);
          additions.push(route);
        }
        if (additions.length === 0) return;
        const metadata = await this.admin.fetchTopicMetadata({
          topics: additions.map((route) => route.topic),
        });
        this.abort.signal.throwIfAborted();
        const expected = new Set(additions.map((route) => route.topic));
        if (
          metadata.topics.length !== additions.length ||
          metadata.topics.some((topic) => !expected.delete(topic.name)) ||
          metadata.topics.some((topic) => topic.partitions.length !== 1)
        ) {
          throw new Error('Kafka exporter requires single-partition source topics');
        }
        await this.startupAdmission.runAll(
          additions,
          this.abort.signal,
          (route) =>
            this.startSource(route).catch((error: unknown) => {
              this.fail(error);
              throw error;
            }),
          () => {},
        );
        this.abort.signal.throwIfAborted();
      })
      .catch((error: unknown) => {
        this.fail(error);
        throw error;
      });
    this.sessionUpdates = update.catch(() => undefined);
    return update;
  }

  private async startOnce(): Promise<void> {
    this.abort.signal.throwIfAborted();
    this.state = 'starting';
    this.diagnosticTimer = setInterval(() => this.sampleDiagnostics(), 60_000);
    this.diagnosticTimer.unref();
    try {
      await this.admin.connect();
      this.abort.signal.throwIfAborted();
      await this.validateTopics();
      this.abort.signal.throwIfAborted();
      await this.startupAdmission.runAll(
        [
          () => this.startDelivery(),
          ...this.options.sessions.map((route) => () => this.startSource(route)),
        ],
        this.abort.signal,
        (start) =>
          start().catch((error: unknown) => {
            this.fail(error);
            throw error;
          }),
        () => {},
      );
      this.abort.signal.throwIfAborted();
      this.state = 'running';
    } catch (error) {
      this.fail(error);
      throw error;
    }
  }

  stop(): Promise<void> {
    this.abort.abort();
    clearInterval(this.diagnosticTimer);
    this.diagnosticTimer = undefined;
    if (this.stopPromise === undefined) {
      const started = performance.now();
      this.stopPromise = this.stopOnce().finally(() =>
        this.emitDiagnostic('stop', performance.now() - started),
      );
    }
    return this.stopPromise;
  }

  private async stopOnce(): Promise<void> {
    const drainStarted = performance.now();
    if (this.options.codec === undefined) await this.codec.close().catch(() => undefined);
    await this.startPromise?.catch(() => undefined);
    await this.sessionUpdates;
    // Keep consumer membership alive until classified HTTP outcomes have attempted completion.
    await Promise.allSettled([...this.deliveryWork]);
    await Promise.allSettled([...this.ownedWork]);
    this.shutdownDrainMs = performance.now() - drainStarted;
    for (const timer of this.timers) clearTimeout(timer);
    this.timers.clear();
    await Promise.all(
      [
        ...this.sources.map((source) => source.consumer),
        ...(this.delivery === undefined ? [] : [this.delivery]),
      ].map((consumer) => {
        this.closingConsumers.add(consumer);
        return consumer.disconnect().catch(() => undefined);
      }),
    );
    this.consumerCount = 0;
    await Promise.all(
      [...this.producers].map((producer) => producer.disconnect().catch(() => undefined)),
    );
    this.producers.clear();
    await this.diagnosticWork;
    try {
      this.readerDiagnostic = await this.sharedState.diagnostics();
    } catch {
      this.diagnosticErrors += 1;
    }
    try {
      await this.sharedState.close();
    } catch (error) {
      this.fail(error);
      throw error;
    } finally {
      await this.admin.disconnect().catch(() => undefined);
    }
    if (this.state !== 'failed') this.state = 'stopped';
  }

  private isShutdownCancellation(error: unknown): boolean {
    return (
      this.abort.signal.aborted &&
      (error === this.abort.signal.reason ||
        (error instanceof Error && error.name === 'AbortError'))
    );
  }

  private fail(error: unknown): void {
    if (this.isShutdownCancellation(error)) return;
    this.state = 'failed';
    // The foreground stop still observes cleanup rejection; event callbacks must not leak it.
    void this.stop().catch(() => undefined);
  }

  private async trackOwnedWork<T>(work: Promise<T>): Promise<T> {
    this.ownedWork.add(work);
    try {
      return await work;
    } catch (error) {
      // Heartbeat races may have already returned; late underlying failures still fail closed.
      if (!isKafkaAssignmentChange(error)) this.fail(error);
      throw error;
    } finally {
      this.ownedWork.delete(work);
    }
  }

  private admitRestore<T>(signal: AbortSignal, operation: () => Promise<T>): Promise<T> {
    return this.restoreAdmission.run(signal, async () => {
      try {
        return await operation();
      } catch (error) {
        // Abort queued work before releasing this permit; ordinary rebalances remain local.
        if (!isKafkaAssignmentChange(error)) this.fail(error);
        throw error;
      }
    });
  }

  private withOwnedHeartbeat<T>(
    batch: Pick<EachBatchPayload, 'heartbeat' | 'isRunning' | 'isStale'>,
    operation: (signal: AbortSignal) => Promise<T>,
    intervalMs = 1_000,
  ): Promise<T> {
    return this.trackOwnedWork(
      (async () => {
        let work: Promise<T> | undefined;
        try {
          return await withKafkaDeliveryHeartbeat(
            batch,
            this.abort.signal,
            (signal) => {
              work = operation(signal);
              return work;
            },
            undefined,
            intervalMs,
          );
        } finally {
          // A lost heartbeat races I/O. Do not release its producer before late connect settles.
          // Preserve the initiating failure when its admission abort wins the heartbeat race.
          await work;
        }
      })(),
    );
  }

  private consumer(groupId: string): Consumer {
    const consumer = this.options.kafka.consumer({
      groupId,
      readUncommitted: false,
      allowAutoTopicCreation: false,
      sessionTimeout: 60_000,
      heartbeatInterval: 3_000,
      retry: { restartOnFailure: async () => false },
    });
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      // KafkaJS may emit a delayed CRASH while intentional disconnect settles.
      if (!this.closingConsumers.has(consumer)) this.fail(payload.error);
    });
    this.consumerCount += 1;
    return consumer;
  }

  private async startSource(route: KafkaSessionRoute): Promise<void> {
    this.abort.signal.throwIfAborted();
    const groupId = kafkaSourceGroupId(this.options.groupId, route.topic);
    const consumer = this.consumer(groupId);
    const source: Source = { route: { ...route }, groupId, consumer, generation: 0 };
    this.sources.push(source);
    consumer.on(consumer.events.GROUP_JOIN, () => {
      source.generation += 1;
    });
    await consumer.connect();
    this.abort.signal.throwIfAborted();
    await consumer.subscribe({ topic: route.topic, fromBeginning: true });
    this.abort.signal.throwIfAborted();
    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      eachBatch: async (batch) => {
        try {
          await this.projectBatch(source, batch);
        } catch (error) {
          if (isKafkaAssignmentChange(error)) {
            const producer = source.owner?.producer ?? source.pendingProducer;
            delete source.owner;
            delete source.pendingProducer;
            if (producer !== undefined) {
              await producer.disconnect().catch(() => undefined);
              this.producers.delete(producer);
            }
            if (error instanceof KafkaAssignmentLostError) return;
            // Let KafkaJS rejoin its group; this is not an instance failure.
            throw error;
          }
          this.fail(error);
          throw error;
        }
      },
    });
    this.abort.signal.throwIfAborted();
  }

  private async projectBatch(source: Source, batch: EachBatchPayload): Promise<void> {
    const generation = source.generation;
    await this.withOwnedHeartbeat(
      {
        heartbeat: batch.heartbeat,
        isRunning: () => batch.isRunning() && source.generation === generation,
        isStale: batch.isStale,
      },
      (signal) =>
        this.projectorAdmission.run(signal, () => this.projectOwnedBatch(source, batch, signal)),
      3_000,
    );
  }

  private stateBlobOptions() {
    return {
      maxRecordBytes: this.options.maxCheckpointBytes ?? 512 * 1024,
      maxAssemblyBytes: this.options.maxAssemblyBytes ?? 32 * 1024 * 1024,
    };
  }

  private observeStateAssembly(head: LoadedKafkaState['head']): void {
    for (const blob of [head.reducer, head.importBaseline?.checkpoint]) {
      if (blob === undefined) continue;
      // Inline values are bounded; large payloads reuse validated descriptor bytes.
      const bytes =
        blob.kind === 'chunks' ? blob.bytes : Buffer.byteLength(JSON.stringify(blob.value));
      this.assemblyPeakBytes = Math.max(this.assemblyPeakBytes, bytes);
    }
  }

  private async commitProjection(
    source: Source,
    producer: Producer,
    projected: KafkaStateProjection,
    signal: AbortSignal,
    assertOwner: () => void,
    captureDeadline = Infinity,
  ): Promise<LoadedKafkaState> {
    const encoded = projected.deliveries.map((record) =>
      encodeKafkaDelivery(record, source.groupId, {
        maxRecordBytes: this.options.maxDeliveryBytes ?? 512 * 1024,
        maxChunkBytes: Math.min(this.options.maxCheckpointBytes ?? 512 * 1024, 512 * 1024),
        maxAssemblyBytes: this.options.maxAssemblyBytes ?? 32 * 1024 * 1024,
      }),
    );
    // The authoritative head is always last in the state partition, after all content/ledger rows.
    const records: KafkaStateRecord[] = [
      ...projected.records.slice(0, -1),
      ...encoded.flatMap((entry) => entry.chunks),
      projected.records.at(-1)!,
    ];
    const deliveries = encoded.map((entry) => entry.delivery);
    const total = [...records, ...deliveries].reduce(
      (bytes, record) => bytes + kafkaRecordBytes(record),
      0,
    );
    this.transactionPeakBytes = Math.max(this.transactionPeakBytes, total);
    this.observeStateAssembly(projected.head);
    this.assemblyPeakBytes = Math.max(
      this.assemblyPeakBytes,
      ...encoded.map((entry) => entry.assemblyBytes),
    );
    const transactionLimit = this.options.maxTransactionBytes ?? 64 * 1024 * 1024;
    if (total > transactionLimit) throw new KafkaTransactionBudgetError(total, transactionLimit);
    assertOwner();
    if (performance.now() >= captureDeadline) throw new CaptureBatchExpiredError();
    const transaction = await producer.transaction();
    let offset: string | undefined;
    try {
      await sendKafkaStateRecords(
        transaction,
        this.options.deliveryTopic,
        deliveries,
        assertOwner,
        Math.min(this.options.maxDeliveryBytes ?? 512 * 1024, 512 * 1024),
      );
      offset = await sendKafkaStateRecords(
        transaction,
        this.options.checkpointTopic,
        records,
        assertOwner,
        Math.min(this.options.maxCheckpointBytes ?? 512 * 1024, 512 * 1024),
        0,
      );
      assertOwner();
      await transaction.sendOffsets({
        consumerGroupId: source.groupId,
        topics: [
          {
            topic: source.route.topic,
            partitions: [{ partition: 0, offset: projected.head.nextOffset }],
          },
        ],
      });
      assertOwner();
      await transaction.commit();
      assertOwner();
    } catch (error) {
      await transaction.abort().catch(() => undefined);
      throw error;
    }
    if (offset === undefined) throw new Error('Kafka state commit offset missing');
    await this.sharedState.waitForOffset(offset, signal);
    assertOwner();
    return this.sharedState.read((index) =>
      loadKafkaState(
        projected.headValue,
        source.route,
        source.groupId,
        index,
        this.stateBlobOptions(),
      ),
    );
  }

  private async projectOwnedBatch(
    source: Source,
    batch: EachBatchPayload,
    signal: AbortSignal,
  ): Promise<void> {
    if (batch.batch.partition !== 0 || batch.batch.topic !== source.route.topic)
      throw new Error('Kafka Transcript must have one authoritative partition');
    const generation = source.generation;
    const assertOwner = (): void => {
      signal.throwIfAborted();
      if (!batch.isRunning() || batch.isStale() || source.generation !== generation)
        throw new KafkaAssignmentLostError();
    };
    assertOwner();
    await batch.heartbeat();
    assertOwner();
    if (source.owner?.generation !== generation) {
      await this.admitRestore(signal, async () => {
        assertOwner();
        if (source.owner !== undefined) {
          await source.owner.producer.disconnect();
          assertOwner();
          this.producers.delete(source.owner.producer);
          delete source.owner;
        }
        const producer = this.options.kafka.producer({
          transactionalId: kafkaSourceTransactionId(this.options.groupId, source.route.topic),
          idempotent: true,
          maxInFlightRequests: 1,
          allowAutoTopicCreation: false,
          transactionTimeout: 60_000,
        });
        this.producers.add(producer);
        source.pendingProducer = producer;
        await producer.connect();
        assertOwner();
        const restored = await this.sharedState.barrier(
          producer,
          source.groupId,
          signal,
          async (index) => {
            assertOwner();
            const [raw] = await index.read([source.groupId]);
            if (raw === null) {
              const keys = kafkaStateKeys(source.groupId);
              if (
                (await index.countPrefix(keys.identityPrefix)) ||
                (await index.countPrefix(keys.acceptedPrefix))
              )
                throw new Error('Kafka state ledger has no head');
              return null;
            }
            const version = (JSON.parse(raw!) as { version?: unknown }).version;
            if (version === 1) {
              const migration = migrateKafkaCheckpoint(
                raw!,
                source.route,
                source.groupId,
                this.stateBlobOptions(),
              );
              return { migration, state: undefined };
            }
            const state = await loadKafkaState(
              raw!,
              source.route,
              source.groupId,
              index,
              this.stateBlobOptions(),
            );
            await validateKafkaState(state, source.groupId, index, this.stateBlobOptions());
            this.observeStateAssembly(state.head);
            return { state, migration: undefined };
          },
        );
        assertOwner();
        const nextOffset =
          restored?.state?.head.nextOffset ?? restored?.migration?.head.nextOffset ?? '0';
        const offsets = await this.admin.fetchTopicOffsets(source.route.topic);
        assertOwner();
        const low = offsets.find((entry) => entry.partition === 0)?.low;
        const high = offsets.find((entry) => entry.partition === 0)?.high;
        const committed = await this.admin.fetchOffsets({
          groupId: source.groupId,
          topics: [source.route.topic],
          resolveOffsets: false,
        });
        assertOwner();
        const groupOffset =
          committed[0]?.partitions.find((entry) => entry.partition === 0)?.offset ?? '-1';
        if (
          low === undefined ||
          high === undefined ||
          BigInt(low) > BigInt(nextOffset) ||
          BigInt(high) < BigInt(nextOffset) ||
          (groupOffset !== '-1' && BigInt(groupOffset) > BigInt(nextOffset))
        )
          throw new Error('Kafka checkpoint/source retention or offset mismatch');
        let state = restored?.state;
        if (restored?.migration !== undefined) {
          state = await this.commitProjection(
            source,
            producer,
            restored.migration,
            signal,
            assertOwner,
          );
        } else if (state === undefined) {
          const context = await this.options.registryClient.resolveContext({
            workspaceId: source.route.workspaceId,
            sessionId: source.route.sessionId,
            signal: AbortSignal.any([signal, AbortSignal.timeout(this.registryTimeout)]),
          });
          assertOwner();
          const initial = initialKafkaState(
            source.route,
            context.status === 'enabled' && isLangfuseHttpJsonContext(context.deliveryContext, true)
              ? parseKafkaDeliveryContext(context.deliveryContext)
              : null,
            source.groupId,
            this.stateBlobOptions(),
          );
          state = await this.commitProjection(source, producer, initial, signal, assertOwner);
        }
        assertOwner();
        source.owner = { generation, producer, checkpoint: state };
        delete source.pendingProducer;
      });
    }
    assertOwner();
    const owner = source.owner!;
    const retained = await this.admin.fetchTopicOffsets(source.route.topic);
    assertOwner();
    const lowOffset = retained.find((entry) => entry.partition === 0)?.low;
    const highOffset = retained.find((entry) => entry.partition === 0)?.high;
    if (lowOffset === undefined || BigInt(lowOffset) > BigInt(owner.checkpoint.head.nextOffset))
      throw new Error('Kafka source retention overtook the projection checkpoint');
    if (highOffset === undefined || BigInt(highOffset) < BigInt(owner.checkpoint.head.nextOffset))
      throw new Error('Kafka source log end precedes the projection checkpoint');
    const size = this.options.batchSize ?? 100;
    validateKafkaTranscriptOffsets(batch.batch.messages);
    const decoded = new Map<string, DecodedKafkaTranscript>();
    let decodedBytes = 0;
    // Decode the broker batch before any chunk commits, retaining the existing late-codec-error contract.
    for (const message of batch.batch.messages) {
      if (BigInt(message.offset) < BigInt(owner.checkpoint.head.nextOffset)) continue;
      if (!validateKafkaTranscriptHeaders(message, source.route)) {
        decoded.set(message.offset, { offset: message.offset, event: null });
        continue;
      }
      const event = await this.codec
        .decode(message, source.route, signal)
        .catch((error: unknown) => {
          if (this.abort.signal.aborted) this.fail(error);
          assertOwner();
          throw error;
        });
      assertOwner();
      decodedBytes += event?.payload.byteLength ?? 0;
      if (decodedBytes > 64 * 1024 * 1024)
        throw new Error(
          `Kafka decoded batch budget exceeded: kind=decoded-bytes actual=${decodedBytes} limit=${64 * 1024 * 1024}`,
        );
      if (decoded.size >= 100_000)
        throw new Error(
          `Kafka decoded batch budget exceeded: kind=decoded-records actual=${decoded.size + 1} limit=100000`,
        );
      decoded.set(message.offset, { offset: message.offset, event });
    }
    // Bound expired-authority retries across this callback, not just each chunk.
    // Returning unresolved offsets releases projector admission; KafkaJS redelivers
    // from the last resolved offset (eachBatchAutoResolve is disabled).
    let captureExpirations = 0;
    for (let offset = 0; offset < batch.batch.messages.length; ) {
      let chunkSize = Math.min(
        size,
        source.captureChunkSize ?? size,
        batch.batch.messages.length - offset,
      );
      for (;;) {
        assertOwner();
        await batch.heartbeat();
        assertOwner();
        const messages = batch.batch.messages.slice(offset, offset + chunkSize);
        // Only content-capable pins need collection authority. Metadata/null pins never
        // expand, including after restart. Routing and sampling always remain pinned.
        const pinned = owner.checkpoint.head.deliveryContext;
        let captureMode: CaptureMode = 'metadata_only';
        let captureDeadline = Infinity;
        if (pinned?.captureMode === 'raw_io') {
          captureDeadline = performance.now() + this.captureBatchTimeout;
          const fresh = await this.options.registryClient.resolveContext({
            workspaceId: source.route.workspaceId,
            sessionId: source.route.sessionId,
            signal: AbortSignal.any([signal, AbortSignal.timeout(this.registryTimeout)]),
          });
          assertOwner();
          if (fresh.status === 'enabled') {
            const current = parseKafkaDeliveryContext(fresh.deliveryContext);
            const matchesPin = Object.keys(pinned).every(
              (key) =>
                key === 'captureMode' ||
                pinned[key as keyof typeof pinned] === current[key as keyof typeof current],
            );
            if (matchesPin && current.captureMode === 'raw_io') captureMode = 'raw_io';
          }
          if (performance.now() >= captureDeadline) {
            if (++captureExpirations >= MAX_CAPTURE_EXPIRATIONS_PER_BATCH) return;
            continue;
          }
        }
        const projected = await this.sharedState.read((index) =>
          projectKafkaState(
            owner.checkpoint,
            messages.flatMap((message) => {
              const entry = decoded.get(message.offset);
              return entry === undefined ? [] : [entry];
            }),
            source.groupId,
            index,
            this.stateBlobOptions(),
            captureMode,
          ),
        );
        assertOwner();
        // Discard an expired candidate, then collect again under a new bounded batch.
        // No second precommit HTTP query, lease, or change to Kafka ownership fencing.
        if (performance.now() >= captureDeadline) {
          chunkSize = Math.max(1, Math.floor(chunkSize / 2));
          source.captureChunkSize = chunkSize;
          if (++captureExpirations >= MAX_CAPTURE_EXPIRATIONS_PER_BATCH) return;
          continue;
        }
        try {
          owner.checkpoint = await this.commitProjection(
            source,
            owner.producer,
            projected,
            signal,
            assertOwner,
            captureDeadline,
          );
        } catch (error) {
          if (error instanceof CaptureBatchExpiredError) {
            chunkSize = Math.max(1, Math.floor(chunkSize / 2));
            source.captureChunkSize = chunkSize;
            if (++captureExpirations >= MAX_CAPTURE_EXPIRATIONS_PER_BATCH) return;
            continue;
          }
          // Only this pre-transaction admission failure is safe to split. Never retry an
          // ambiguous commit, fencing error, or partial source transition with live state.
          if (error instanceof KafkaTransactionBudgetError && chunkSize > 1) {
            chunkSize = Math.max(1, Math.floor(chunkSize / 2));
            continue;
          }
          throw error;
        }
        assertOwner();
        for (const message of messages) batch.resolveOffset(message.offset);
        offset += chunkSize;
        break;
      }
    }
  }

  private async startDelivery(): Promise<void> {
    this.abort.signal.throwIfAborted();
    const groupId = this.options.groupId + '-delivery-v1';
    const consumer = this.consumer(groupId);
    this.delivery = consumer;
    let generation = 0;
    consumer.on(consumer.events.GROUP_JOIN, () => {
      generation += 1;
    });
    const owners = new Map<
      number,
      { generation: number; producer: Producer; nextOffset: string | null }
    >();
    const attempts = new Map<number, number>();
    const release = async (partition: number): Promise<void> => {
      const owner = owners.get(partition);
      owners.delete(partition);
      if (owner !== undefined) {
        await owner.producer.disconnect().catch(() => undefined);
        this.producers.delete(owner.producer);
      }
    };
    await consumer.connect();
    this.abort.signal.throwIfAborted();
    await consumer.subscribe({ topic: this.options.deliveryTopic, fromBeginning: true });
    this.abort.signal.throwIfAborted();
    const process = async (batch: EachBatchPayload): Promise<void> => {
      if (this.abort.signal.aborted) return;
      const partition = batch.batch.partition;
      const assignedGeneration = generation;
      const assertOwner = (): void => {
        // Shutdown alone is not lease loss: a complete HTTP outcome may still commit.
        if (!batch.isRunning() || batch.isStale() || generation !== assignedGeneration) {
          throw new KafkaAssignmentLostError();
        }
      };
      const key = kafkaDeliveryProgressKey(
        this.options.groupId,
        this.options.deliveryTopic,
        partition,
      );
      try {
        assertOwner();
        await batch.heartbeat();
        this.abort.signal.throwIfAborted();
        let owner = owners.get(partition);
        if (owner?.generation !== generation || owner.nextOffset === null) {
          owner = await this.withOwnedHeartbeat(
            {
              heartbeat: batch.heartbeat,
              isRunning: () => batch.isRunning() && generation === assignedGeneration,
              isStale: batch.isStale,
            },
            (signal) =>
              this.admitRestore(signal, async () => {
                const assertRestoreOwner = (): void => {
                  signal.throwIfAborted();
                  assertOwner();
                };
                assertRestoreOwner();
                await release(partition);
                assertRestoreOwner();
                const producer = this.options.kafka.producer({
                  transactionalId: key,
                  idempotent: true,
                  maxInFlightRequests: 1,
                  allowAutoTopicCreation: false,
                  transactionTimeout: 60_000,
                });
                this.producers.add(producer);
                const restoredOwner: {
                  generation: number;
                  producer: Producer;
                  nextOffset: string | null;
                } = { generation: assignedGeneration, producer, nextOffset: null };
                owners.set(partition, restoredOwner);
                await producer.connect();
                assertRestoreOwner();
                const progress = await this.sharedState.barrier(
                  producer,
                  key,
                  signal,
                  async (index) => {
                    assertOwner();
                    const [raw] = await index.read([key]);
                    return raw === null
                      ? null
                      : parseDeliveryProgress(
                          JSON.parse(raw!),
                          this.options.deliveryTopic,
                          partition,
                          groupId,
                        );
                  },
                );
                assertRestoreOwner();
                const committed = await this.admin.fetchOffsets({
                  groupId,
                  topics: [this.options.deliveryTopic],
                  resolveOffsets: false,
                });
                assertRestoreOwner();
                const groupOffset =
                  committed[0]?.partitions.find((entry) => entry.partition === partition)?.offset ??
                  '-1';
                const offsets = await this.admin.fetchTopicOffsets(this.options.deliveryTopic);
                assertRestoreOwner();
                const bounds = offsets.find((entry) => entry.partition === partition);
                if (
                  bounds === undefined ||
                  BigInt(bounds.low) > BigInt(progress?.nextOffset ?? '0') ||
                  BigInt(bounds.high) < BigInt(progress?.nextOffset ?? '0') ||
                  (progress === null && groupOffset !== '-1') ||
                  (progress !== null &&
                    groupOffset !== '-1' &&
                    BigInt(groupOffset) > BigInt(progress.nextOffset))
                ) {
                  throw new Error('Kafka delivery progress/source offset mismatch');
                }
                restoredOwner.nextOffset = progress?.nextOffset ?? '0';
                return restoredOwner;
              }),
          );
        }
        this.abort.signal.throwIfAborted();
        assertOwner();
        const complete = async (nextOffset: string): Promise<void> => {
          await batch.heartbeat();
          assertOwner();
          const transaction = await owner.producer.transaction();
          try {
            await transaction.send({
              topic: this.options.checkpointTopic,
              messages: [
                {
                  partition: 0,
                  key,
                  value: JSON.stringify({
                    version: 1,
                    topic: this.options.deliveryTopic,
                    partition,
                    groupId,
                    nextOffset,
                  }),
                },
              ],
            });
            await transaction.sendOffsets({
              consumerGroupId: groupId,
              topics: [
                {
                  topic: this.options.deliveryTopic,
                  partitions: [{ partition, offset: nextOffset }],
                },
              ],
            });
            await batch.heartbeat();
            assertOwner();
            await transaction.commit();
            owner.nextOffset = nextOffset;
          } catch (error) {
            await transaction.abort().catch(() => undefined);
            throw error;
          }
        };
        // Persist initialization before HTTP and repair expired/deleted group offsets.
        // Recovery skips already completed records using the compacted progress, not the group.
        await complete(owner.nextOffset!);
        for (const message of batch.batch.messages) {
          if (this.abort.signal.aborted) return;
          assertOwner();
          if (BigInt(message.offset) < BigInt(owner.nextOffset!)) {
            batch.resolveOffset(message.offset);
            continue;
          }
          await batch.heartbeat();
          if (
            message.value === null ||
            message.value.length > (this.options.maxDeliveryBytes ?? 512 * 1024)
          ) {
            throw new Error('invalid Kafka delivery record size');
          }
          const raw = message.value.toString();
          const isManifest = (JSON.parse(raw) as { version?: unknown }).version === 2;
          const result = await withKafkaDeliveryHeartbeat(
            batch,
            this.abort.signal,
            (signal) => {
              const work = async () => {
                let record = await this.sharedState.read((index) =>
                  decodeKafkaDelivery(raw, index, this.options.groupId, {
                    maxRecordBytes: this.options.maxDeliveryBytes ?? 512 * 1024,
                    maxAssemblyBytes: this.options.maxAssemblyBytes ?? 32 * 1024 * 1024,
                    onAssemblyBytes: (bytes) => {
                      this.assemblyPeakBytes = Math.max(this.assemblyPeakBytes, bytes);
                    },
                  }),
                );
                assertOwner();
                signal.throwIfAborted();
                if (record === null) {
                  record = await this.sharedState.barrier(owner.producer, key, signal, (index) =>
                    decodeKafkaDelivery(raw, index, this.options.groupId, {
                      maxRecordBytes: this.options.maxDeliveryBytes ?? 512 * 1024,
                      maxAssemblyBytes: this.options.maxAssemblyBytes ?? 32 * 1024 * 1024,
                      onAssemblyBytes: (bytes) => {
                        this.assemblyPeakBytes = Math.max(this.assemblyPeakBytes, bytes);
                      },
                    }),
                  );
                  assertOwner();
                  signal.throwIfAborted();
                  if (record === null)
                    throw new Error('Kafka delivery chunks missing after barrier');
                }
                return deliverKafkaTrace(
                  record,
                  {
                    registryClient: this.options.registryClient,
                    registryRequestTimeoutMs: this.registryTimeout,
                    ...(this.options.otlpFetchImpl === undefined
                      ? {}
                      : { otlpFetchImpl: this.options.otlpFetchImpl }),
                  },
                  signal,
                );
              };
              return isManifest
                ? this.largeDeliveryAdmission.run(signal, () => this.trackOwnedWork(work()))
                : this.trackOwnedWork(work());
            },
            (outcome) => outcome.kind === 'terminal',
          );
          assertOwner();
          if (result.kind === 'retry') {
            this.abort.signal.throwIfAborted();
            const attempt = (attempts.get(partition) ?? 0) + 1;
            attempts.set(partition, attempt);
            const resume = batch.pause();
            const timer = setTimeout(
              () => {
                this.timers.delete(timer);
                if (!this.abort.signal.aborted) resume();
              },
              deliveryRetryDelayMs(attempt, result.retryAfterMs),
            );
            this.timers.add(timer);
            return;
          }
          // Complete terminal HTTP results even during shutdown, while the same owner is valid.
          await complete((BigInt(message.offset) + 1n).toString());
          batch.resolveOffset(message.offset);
          attempts.delete(partition);
        }
      } catch (error) {
        if (isKafkaAssignmentChange(error)) {
          await release(partition);
          if (error instanceof KafkaAssignmentLostError) return;
          throw error;
        }
        if (this.isShutdownCancellation(error)) return;
        this.fail(error);
        throw error;
      }
    };
    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      // KafkaJS preserves partition order. Owners/producers and retry state are
      // partition-local, so slow HTTP need not serialize the entire consumer.
      partitionsConsumedConcurrently: 8,
      eachBatch: async (batch) => {
        const work = process(batch);
        this.deliveryWork.add(work);
        try {
          await work;
        } finally {
          this.deliveryWork.delete(work);
        }
      },
    });
    this.abort.signal.throwIfAborted();
  }

  private async validateTopics(): Promise<void> {
    const expectedTopics = new Set([
      this.options.checkpointTopic,
      this.options.deliveryTopic,
      ...this.options.sessions.map((route) => route.topic),
    ]);
    const metadata = await this.admin.fetchTopicMetadata({
      topics: [...expectedTopics],
    });
    this.abort.signal.throwIfAborted();
    if (
      metadata.topics.length !== expectedTopics.size ||
      metadata.topics.some((topic) => !expectedTopics.delete(topic.name))
    ) {
      throw new Error(
        'Kafka exporter topic metadata must contain every expected topic exactly once',
      );
    }
    for (const topic of metadata.topics) {
      if (
        topic.partitions.length === 0 ||
        (topic.name !== this.options.deliveryTopic && topic.partitions.length !== 1)
      ) {
        throw new Error('Kafka exporter requires single-partition source/checkpoint topics');
      }
    }
    const configs = await this.admin.describeConfigs({
      includeSynonyms: false,
      resources: [this.options.checkpointTopic, this.options.deliveryTopic].map((name) => ({
        type: ConfigResourceTypes.TOPIC,
        name,
        configNames: ['cleanup.policy', 'retention.ms', 'retention.bytes'],
      })),
    });
    this.abort.signal.throwIfAborted();
    for (const name of [this.options.checkpointTopic, this.options.deliveryTopic]) {
      const resource = configs.resources.find((entry) => entry.resourceName === name);
      const values = new Map(
        resource?.configEntries.map((entry) => [entry.configName, entry.configValue]),
      );
      if (
        name === this.options.checkpointTopic
          ? values.get('cleanup.policy') !== 'compact'
          : values.get('cleanup.policy') !== 'delete' ||
            values.get('retention.ms') !== '-1' ||
            values.get('retention.bytes') !== '-1'
      ) {
        throw new Error('Kafka exporter state/delivery topic retention policy is unsafe');
      }
    }
  }
}

export function kafkaSourceGroupId(namespace: string, topic: string): string {
  return `${namespace}-source-${createHash('sha256').update(topic).digest('hex')}`;
}
export function kafkaDeliveryProgressKey(
  namespace: string,
  topic: string,
  partition: number,
): string {
  return `delivery:${namespace}:${createHash('sha256').update(topic).digest('hex')}:${partition}:v1`;
}
function parseDeliveryProgress(
  value: unknown,
  topic: string,
  partition: number,
  groupId: string,
): { nextOffset: string } {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new Error('invalid Kafka delivery progress');
  const progress = value as Record<string, unknown>;
  if (
    progress.version !== 1 ||
    progress.topic !== topic ||
    progress.partition !== partition ||
    progress.groupId !== groupId ||
    typeof progress.nextOffset !== 'string' ||
    !/^(0|[1-9][0-9]{0,18})$/u.test(progress.nextOffset) ||
    BigInt(progress.nextOffset) >= 1n << 63n
  )
    throw new Error('invalid Kafka delivery progress');
  return { nextOffset: progress.nextOffset };
}
function validateSessionRoute(route: KafkaSessionRoute, encoding: 'raw' | 'avro'): void {
  const suffix = `orca.${route.workspaceId}.sessions.${route.sessionId}.events${encoding === 'avro' ? '-avro' : ''}`;
  if (
    !/^[A-Za-z0-9_-]{1,128}$/u.test(route.workspaceId) ||
    !/^ses_[A-Za-z0-9_-]+$/u.test(route.sessionId) ||
    route.sessionId.length > 128 ||
    !route.topic.endsWith(suffix)
  )
    throw new Error('invalid Kafka exporter Session route');
  const prefix = route.topic.slice(0, -suffix.length);
  if (prefix !== '' && !/^([A-Za-z0-9_-]+\.)+$/u.test(prefix))
    throw new Error('invalid Kafka topic prefix');
}
export function kafkaSourceTransactionId(namespace: string, topic: string): string {
  return `${kafkaSourceGroupId(namespace, topic)}-partition-0-v1`;
}
function bounded(value: number, min: number, max: number): number {
  if (!Number.isSafeInteger(value) || value < min || value > max)
    throw new Error('invalid Kafka exporter limit');
  return value;
}

class KafkaTransactionBudgetError extends Error {
  constructor(actual: number, limit: number) {
    super(
      `Kafka exporter transaction size budget exceeded: kind=transaction actual=${actual} limit=${limit}`,
    );
  }
}

class KafkaAssignmentLostError extends Error {
  constructor() {
    super('Kafka exporter assignment lost');
  }
}

function isKafkaAssignmentChange(error: unknown): boolean {
  return (
    error instanceof KafkaAssignmentLostError ||
    (error !== null &&
      typeof error === 'object' &&
      'type' in error &&
      ['REBALANCE_IN_PROGRESS', 'ILLEGAL_GENERATION', 'UNKNOWN_MEMBER_ID'].includes(
        String(error.type),
      ))
  );
}

/** Maintain membership during slow HTTP/Registry calls and cancel on lease loss. */
export async function withKafkaDeliveryHeartbeat<T>(
  batch: Pick<EachBatchPayload, 'heartbeat' | 'isRunning' | 'isStale'>,
  signal: AbortSignal,
  operation: (signal: AbortSignal) => Promise<T>,
  preserveTerminal?: (result: T) => boolean,
  heartbeatIntervalMs = 1_000,
): Promise<T> {
  signal.throwIfAborted();
  const lease = new AbortController();
  const combined = AbortSignal.any([signal, lease.signal]);
  let timer: ReturnType<typeof setTimeout> | undefined;
  let shutdownTimer: ReturnType<typeof setTimeout> | undefined;
  let done = false;
  let reject!: (error: unknown) => void;
  const lost = new Promise<never>((_, fail) => {
    reject = fail;
  });
  const onShutdown = (): void => {
    // Abort incomplete HTTP immediately, but let EOF classification settle before discarding it.
    if (preserveTerminal === undefined) reject(signal.reason);
    else shutdownTimer = setTimeout(() => reject(signal.reason), 1_000);
  };
  const onLeaseLost = (): void => reject(lease.signal.reason);
  signal.addEventListener('abort', onShutdown, { once: true });
  lease.signal.addEventListener('abort', onLeaseLost, { once: true });
  const tick = async (): Promise<void> => {
    try {
      if (!batch.isRunning() || batch.isStale()) throw new KafkaAssignmentLostError();
      await batch.heartbeat();
      if (!done)
        timer = setTimeout(() => {
          void tick();
        }, heartbeatIntervalMs);
    } catch (error) {
      if (!done) lease.abort(error);
    }
  };
  timer = setTimeout(() => {
    void tick();
  }, heartbeatIntervalMs);
  try {
    const result = await Promise.race([operation(combined), lost]);
    lease.signal.throwIfAborted();
    if (signal.aborted && !preserveTerminal?.(result)) signal.throwIfAborted();
    return result;
  } finally {
    done = true;
    if (timer !== undefined) clearTimeout(timer);
    if (shutdownTimer !== undefined) clearTimeout(shutdownTimer);
    signal.removeEventListener('abort', onShutdown);
    lease.signal.removeEventListener('abort', onLeaseLost);
  }
}
