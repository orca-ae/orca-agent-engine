// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import type { Consumer, EachBatchPayload, Kafka, Producer } from 'kafkajs';
import { TextDecoder } from 'node:util';
import { KafkaDiskIndex } from './kafka-disk-index.js';

interface Waiter {
  key?: string;
  nonce?: string;
  offset?: bigint;
  snapshot: (index: KafkaDiskIndex) => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
}

/** One read-committed stream and fresh scratch index per runtime, never per Session. */
export class KafkaSharedState {
  private index: KafkaDiskIndex | undefined;
  private reader: Consumer | undefined;
  private startPromise: Promise<void> | undefined;
  private closePromise: Promise<void> | undefined;
  private failure: Error | undefined;
  private closed = false;
  private appliedOffset = -1n;
  private scannedBytes = 0;
  private scannedRecords = 0;
  private readonly waiters = new Set<Waiter>();
  private queries: Promise<unknown> = Promise.resolve();
  private readonly heartbeatStops = new Set<() => Promise<void>>();

  constructor(
    private readonly options: {
      kafka: Kafka;
      topic: string;
      directory?: string;
      maxDiskBytes?: number;
      timeoutMs?: number;
      maxRecordBytes?: number;
      onFailure: (error: Error) => void;
    },
  ) {}

  start(): Promise<void> {
    this.startPromise ??= this.startOnce().catch((error: unknown) => {
      this.fail(error);
      throw error;
    });
    return this.startPromise;
  }

  private async startOnce(): Promise<void> {
    this.assertOpen();
    this.index = await KafkaDiskIndex.open({
      onFailure: (error) => this.fail(error),
      ...(this.options.directory === undefined ? {} : { directory: this.options.directory }),
      ...(this.options.maxDiskBytes === undefined ? {} : { maxBytes: this.options.maxDiskBytes }),
    });
    this.assertOpen();
    const reader = this.options.kafka.consumer({
      groupId: `orca-exporter-restore-${randomUUID()}`,
      readUncommitted: false,
      allowAutoTopicCreation: false,
      sessionTimeout: 60_000,
      heartbeatInterval: 3_000,
      maxBytesPerPartition: 1024 * 1024,
      maxBytes: 4 * 1024 * 1024,
      retry: { restartOnFailure: async () => false },
    });
    this.reader = reader;
    reader.on(reader.events.CRASH, () => this.fail(new Error('Kafka state reader failed')));
    await reader.connect();
    this.assertOpen();
    await reader.subscribe({ topic: this.options.topic, fromBeginning: true });
    this.assertOpen();
    await reader.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      eachBatch: async (batch) => {
        const heartbeat = this.maintainHeartbeat(batch);
        const active = (): boolean => {
          this.assertOpen();
          return batch.isRunning() && !batch.isStale();
        };
        try {
          this.assertOpen();
          if (batch.batch.topic !== this.options.topic || batch.batch.partition !== 0)
            throw new Error('Kafka state reader partition mismatch');
          let pending: Array<{ key: string; value: string | null; offset: string }> = [];
          let bytes = 0;
          const flush = async (): Promise<boolean> => {
            if (!active()) return false;
            if (pending.length === 0) return true;
            await this.index!.apply(pending);
            if (!active()) return false;
            this.appliedOffset = BigInt(pending.at(-1)!.offset);
            for (const record of pending) batch.resolveOffset(record.offset);
            pending = [];
            bytes = 0;
            await heartbeat.beat();
            if (!active()) return false;
            for (const waiter of [...this.waiters]) {
              if (waiter.key === undefined && waiter.offset! <= this.appliedOffset) {
                this.waiters.delete(waiter);
                waiter.resolve(undefined);
              }
            }
            return true;
          };
          for (const message of batch.batch.messages) {
            this.assertOpen();
            if (!batch.isRunning() || batch.isStale()) return;
            const size = (message.key?.length ?? 0) + (message.value?.length ?? 0);
            this.scannedBytes += size;
            this.scannedRecords++;
            if (BigInt(message.offset) <= this.appliedOffset) {
              batch.resolveOffset(message.offset);
              continue;
            }
            const limit = (this.options.maxRecordBytes ?? 512 * 1024) + 4096;
            if (size > limit)
              throw new Error(
                `Kafka state record size exceeded kind=state-record actual=${size} limit=${limit}`,
              );
            if (message.key === null) throw new Error('Kafka state record key missing');
            const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
            const key = decoder.decode(message.key);
            const value = message.value === null ? null : decoder.decode(message.value);
            if (pending.length >= 500 || bytes + size > 2 * 1024 * 1024) {
              if (!(await flush())) return;
            }
            pending.push({ key, value, offset: message.offset });
            bytes += size;
            const barrier = [...this.waiters].find(
              (waiter) => waiter.key === key && waiter.nonce === value,
            );
            if (barrier !== undefined) {
              if (!(await flush())) return;
              if (barrier.offset !== BigInt(message.offset))
                throw new Error('Kafka state barrier mismatch');
              // Pin the scoped snapshot BEFORE applying later records in this fetch.
              try {
                const value = await barrier.snapshot(this.index!);
                // A stale snapshot cannot authenticate this barrier. Keep its
                // waiter reachable by failure/close until its caller cancels.
                if (!active()) return;
                barrier.resolve(value);
                this.waiters.delete(barrier);
              } catch (error) {
                barrier.reject(error);
                this.waiters.delete(barrier);
                this.assertOpen();
              }
            }
          }
          await flush();
        } catch (error) {
          if (!this.closed) this.fail(error);
          throw error;
        } finally {
          await heartbeat.stop();
        }
      },
    });
    this.assertOpen();
  }

  /** Keep this reader's group alive during index I/O, not the source consumer's group. */
  private maintainHeartbeat(batch: EachBatchPayload): {
    beat: () => Promise<void>;
    stop: () => Promise<void>;
  } {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let inFlight: Promise<void> | undefined;
    const stop = async (): Promise<void> => {
      stopped = true;
      clearTimeout(timer);
      await inFlight;
      this.heartbeatStops.delete(stop);
    };
    const schedule = (): void => {
      if (!stopped)
        timer = setTimeout(() => {
          void beat();
        }, 3_000);
    };
    const beat = (): Promise<void> => {
      if (inFlight) return inFlight;
      clearTimeout(timer);
      if (stopped || this.closed || this.failure || !batch.isRunning() || batch.isStale()) {
        stopped = true;
        return Promise.resolve();
      }
      inFlight = Promise.resolve()
        .then(() => batch.heartbeat())
        .catch((error: unknown) => {
          this.fail(error);
        })
        .finally(() => {
          inFlight = undefined;
          schedule();
        });
      return inFlight;
    };
    this.heartbeatStops.add(stop);
    schedule();
    return { beat, stop };
  }

  /** Producer is caller-owned; beginning the transaction, not connect(), fences the old writer. */
  async barrier<T>(
    producer: Producer,
    ownerKey: string,
    signal: AbortSignal,
    snapshot: (index: KafkaDiskIndex) => Promise<T>,
  ): Promise<T> {
    signal = AbortSignal.any([signal, AbortSignal.timeout(this.options.timeoutMs ?? 120_000)]);
    signal.throwIfAborted();
    await this.start();
    signal.throwIfAborted();
    this.assertOpen();
    const nonce = randomUUID();
    const key = `barrier:${ownerKey}`;
    const pending = this.wait<T>({ key, nonce, snapshot }, signal);
    let transaction;
    try {
      transaction = await producer.transaction();
      signal.throwIfAborted();
      this.assertOpen();
      const sent = await transaction.send({
        topic: this.options.topic,
        messages: [{ partition: 0, key, value: nonce }],
      });
      const offset = sent[0]?.baseOffset;
      if (offset === undefined || !/^(0|[1-9][0-9]*)$/u.test(offset))
        throw new Error('Kafka state barrier offset missing');
      pending.waiter.offset = BigInt(offset);
      signal.throwIfAborted();
      this.assertOpen();
      await transaction.commit();
      return await pending.promise;
    } catch (error) {
      pending.waiter.reject(error);
      await transaction?.abort().catch(() => undefined);
      throw error;
    } finally {
      pending.cancel();
    }
  }

  async waitForOffset(offset: string, signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    this.assertOpen();
    if (!/^(0|[1-9][0-9]*)$/u.test(offset)) throw new Error('invalid Kafka state offset');
    if (BigInt(offset) <= this.appliedOffset) return;
    const pending = this.wait<void>(
      { offset: BigInt(offset), snapshot: async () => undefined },
      signal,
    );
    try {
      await pending.promise;
    } finally {
      pending.cancel();
    }
  }

  async read<T>(work: (index: KafkaDiskIndex) => Promise<T>): Promise<T> {
    const query = this.queries.then(async () => {
      this.assertOpen();
      if (this.index === undefined) throw new Error('Kafka state index is not started');
      return work(this.index);
    });
    // At most one foreground query and one reader apply/snapshot enter the worker.
    this.queries = query.catch(() => undefined);
    return query;
  }

  async diagnostics(): Promise<{
    scannedBytes: number;
    scannedRecords: number;
    waiters: number;
    databaseBytes: number;
    databaseLimitBytes: number;
    diskQuotaBytes: number;
  }> {
    const query = this.queries.then(async () => {
      const stats =
        this.index === undefined
          ? {
              databaseBytes: 0,
              databaseLimitBytes: 0,
              diskQuotaBytes: this.options.maxDiskBytes ?? 1024 * 1024 * 1024,
            }
          : await this.index.stats();
      return {
        scannedBytes: this.scannedBytes,
        scannedRecords: this.scannedRecords,
        waiters: this.waiters.size,
        ...stats,
      };
    });
    this.queries = query.catch(() => undefined);
    return query;
  }

  private wait<T>(
    input: Pick<Waiter, 'key' | 'nonce' | 'offset' | 'snapshot'>,
    signal: AbortSignal,
  ): { promise: Promise<T>; waiter: Waiter; cancel: () => void } {
    let waiter!: Waiter;
    const promise = new Promise<T>((resolve, reject) => {
      waiter = { ...input, resolve: (value) => resolve(value as T), reject };
      this.waiters.add(waiter);
    });
    void promise.catch(() => undefined);
    const timer = setTimeout(
      () => waiter.reject(new Error('Kafka state catch-up timed out')),
      this.options.timeoutMs ?? 120_000,
    );
    // Preserve assignment-loss identity so an ordinary rebalance is not promoted to instance failure.
    const abort = (): void => waiter.reject(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) abort();
    return {
      promise,
      waiter,
      cancel: () => {
        this.waiters.delete(waiter);
        clearTimeout(timer);
        signal.removeEventListener('abort', abort);
      },
    };
  }

  private assertOpen(): void {
    if (this.failure !== undefined) throw this.failure;
    if (this.closed) throw new Error('Kafka state reader closed');
  }

  private fail(error: unknown): void {
    if (this.failure !== undefined || this.closed) return;
    this.failure = error instanceof Error ? error : new Error('Kafka state reader failed');
    for (const stop of this.heartbeatStops) void stop();
    for (const waiter of this.waiters) waiter.reject(this.failure);
    try {
      this.options.onFailure(this.failure);
    } catch {
      // Observers cannot prevent waiter rejection or resource cleanup.
    }
  }

  close(): Promise<void> {
    this.closed = true;
    const heartbeats = [...this.heartbeatStops].map((stop) => stop());
    for (const waiter of this.waiters) waiter.reject(new Error('Kafka state reader closed'));
    this.closePromise ??= (async () => {
      await Promise.all(heartbeats);
      await this.startPromise?.catch(() => undefined);
      await this.queries;
      try {
        await this.reader?.disconnect();
      } finally {
        await this.index?.close();
      }
    })();
    return this.closePromise;
  }
}
