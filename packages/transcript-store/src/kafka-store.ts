// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Kafka } from 'kafkajs';
import { v7 as uuidv7 } from 'uuid';
import { TranscriptProducer } from './kafka/producer.js';
import { consumeSession } from './kafka/consumer.js';
import { createKafkaTranscriptCodec, type KafkaTranscriptCodec } from './kafka/codec.js';
import {
  appendTotal,
  appendLatency,
  readTotal,
  readFirstByteLatency,
  tailTotal,
  archiveTotal,
  subagentMessageRate,
} from './metrics.js';
import type { Event } from './types.js';
import type { ReadOptions, TailOptions, TranscriptStore } from './store.js';
import { assertEventsMatchRoute } from './route.js';

export interface KafkaTranscriptStoreOptions {
  kafka: Kafka;
  /** Shared injected codecs remain caller-owned. */
  codec?: KafkaTranscriptCodec;
  /**
   * Optional topic prefix (dot-terminated segments, e.g. `public.default.`).
   * Prepended to every session topic name on produce + consume. Required for
   * Kafka-on-Pulsar (KoP) endpoints, where dotted Kafka topic names map to
   * `<tenant>.<namespace>.<local-topic>`. Defaults to '' (bare names,
   * plain Kafka behavior).
   */
  topicPrefix?: string;
}

/** Kafka-only controls for one bounded read. Shared backend semantics stay unchanged. */
export interface KafkaReadOptions extends ReadOptions {
  /** Cancel the short-lived Kafka consumer backing this read. */
  signal?: AbortSignal;
  /**
   * Cumulative Kafka broker message bytes. KafkaJS `size` is used when
   * present; record-batch messages fall back to key/value/header bytes.
   * Zero or undefined disables the cap. One first raw record is admitted even
   * when it alone exceeds the cap.
   */
  maxBytes?: number;
  /** Next offset after the latest admitted record successfully decoded or explicitly route-skipped. */
  onScannedCursor?: (nextCursor: string) => void;
}

/**
 * Kafka-backed `TranscriptStore` implementation.  Uses a single shared
 * `TranscriptProducer` (idempotent producer with per-session LRU dedup) for
 * appends, and one short-lived consumer per Read/Tail call.
 *
 * The store is lazily connected on first append/archive; callers should call
 * `close()` on shutdown.
 */
export class KafkaTranscriptStore implements TranscriptStore {
  private readonly kafka: Kafka;
  private readonly producer: TranscriptProducer;
  private readonly topicPrefix: string;
  private readonly codec: KafkaTranscriptCodec;
  private readonly ownsCodec: boolean;
  private connected = false;

  constructor(opts: KafkaTranscriptStoreOptions) {
    this.kafka = opts.kafka;
    this.topicPrefix = opts.topicPrefix ?? '';
    this.codec = opts.codec ?? createKafkaTranscriptCodec();
    this.ownsCodec = opts.codec === undefined;
    this.producer = new TranscriptProducer(opts.kafka, this.topicPrefix, { codec: this.codec });
  }

  async ensureConnected(): Promise<void> {
    if (!this.connected) {
      await this.producer.connect();
      this.connected = true;
    }
  }

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    assertEventsMatchRoute(workspaceId, sessionId, events);
    const stop = appendLatency.startTimer();
    await this.ensureConnected();
    try {
      for (const e of events) {
        if (e.subpath && e.subpath.length > 0) {
          subagentMessageRate.inc({ workspace_id: workspaceId, produced_by: e.producedBy });
        }
      }
      const ids = await this.producer.append(workspaceId, sessionId, events);
      appendTotal.inc({ status: 'ok' });
      return ids;
    } catch (err) {
      appendTotal.inc({ status: 'error' });
      throw err;
    } finally {
      stop();
    }
  }

  async *read(
    workspaceId: string,
    sessionId: string,
    opts: KafkaReadOptions,
  ): AsyncIterable<Event> {
    yield* iterateConsume(
      this.kafka,
      {
        workspaceId,
        sessionId,
        fromCursor: opts.fromCursor,
        subpath: opts.subpath,
        maxEvents: opts.maxEvents,
        mode: 'bounded',
        topicPrefix: this.topicPrefix,
        codec: this.codec,
        ...(opts.maxBytes === undefined ? {} : { maxBytes: opts.maxBytes }),
        ...(opts.onScannedCursor === undefined ? {} : { onScannedCursor: opts.onScannedCursor }),
        ...(opts.signal === undefined ? {} : { signal: opts.signal }),
      },
      {
        onOk: () => readTotal.inc({ status: 'ok' }),
        onError: () => readTotal.inc({ status: 'error' }),
      },
    );
  }

  async *tail(workspaceId: string, sessionId: string, opts: TailOptions): AsyncIterable<Event> {
    tailTotal.inc({ status: 'ok' });
    yield* iterateConsume(
      this.kafka,
      {
        workspaceId,
        sessionId,
        fromCursor: opts.fromCursor,
        subpath: opts.subpath,
        maxEvents: 0,
        mode: 'unbounded',
        topicPrefix: this.topicPrefix,
        codec: this.codec,
        ...(opts.signal !== undefined ? { signal: opts.signal } : {}),
      },
      {
        onError: () => tailTotal.inc({ status: 'error' }),
        ...(opts.onReady !== undefined ? { onReady: opts.onReady } : {}),
      },
    );
  }

  async archive(workspaceId: string, sessionId: string): Promise<void> {
    await this.ensureConnected();
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
      await this.producer.append(workspaceId, sessionId, [sentinel]);
      archiveTotal.inc({ status: 'ok' });
    } catch (err) {
      archiveTotal.inc({ status: 'error' });
      throw err;
    }
  }

  async close(): Promise<void> {
    try {
      await this.producer.disconnect();
      this.connected = false;
    } finally {
      if (this.ownsCodec) await this.codec.close();
    }
  }
}

interface ConsumeArgs {
  codec: KafkaTranscriptCodec;
  workspaceId: string;
  sessionId: string;
  fromCursor: string;
  subpath: string;
  maxEvents: number;
  maxBytes?: number;
  onScannedCursor?: (nextCursor: string) => void;
  mode: 'bounded' | 'unbounded';
  topicPrefix: string;
  signal?: AbortSignal;
}

interface IterateHooks {
  onOk?: () => void;
  onReady?: () => void;
  onError?: (error: Error) => void;
}

/**
 * Adapt the callback-style `consumeSession()` into an async iterable.
 * Always passes an `AbortSignal` to `consumeSession`; if the caller didn't
 * supply one, we create our own and abort it in `finally` so the underlying
 * consumer is cleaned up on early break / throw.
 */
async function* iterateConsume(
  kafka: Kafka,
  args: ConsumeArgs,
  hooks: IterateHooks = {},
): AsyncIterable<Event> {
  const queue: Event[] = [];
  const resolvers: Array<(v: IteratorResult<Event>) => void> = [];
  let ended = false;
  let terminalError: Error | null = null;
  let firstSeen = false;
  const startedAt = Date.now();

  const ourAc = args.signal ? null : new AbortController();
  const signal = args.signal ?? ourAc!.signal;

  const onAbort = (): void => {
    queue.length = 0;
    if (ended) return;
    ended = true;
    while (resolvers.length) resolvers.shift()!({ value: undefined, done: true });
  };
  if (signal.aborted) {
    // Abort already fired before we could wire up; flag ended now so the
    // consume loop returns immediately after handle.stop() in finally.
    ended = true;
  } else {
    signal.addEventListener('abort', onAbort, { once: true });
  }

  const handle = consumeSession(
    kafka,
    {
      workspaceId: args.workspaceId,
      sessionId: args.sessionId,
      fromCursor: args.fromCursor,
      subpath: args.subpath,
      maxEvents: args.maxEvents,
      mode: args.mode,
      topicPrefix: args.topicPrefix,
      codec: args.codec,
      signal,
      ...(args.maxBytes === undefined ? {} : { maxBytes: args.maxBytes }),
      ...(args.onScannedCursor === undefined ? {} : { onScannedCursor: args.onScannedCursor }),
    },
    {
      onEvent: (e) => {
        if (!firstSeen) {
          firstSeen = true;
          if (args.mode === 'bounded') {
            readFirstByteLatency.observe((Date.now() - startedAt) / 1000);
          }
        }
        if (resolvers.length) resolvers.shift()!({ value: e, done: false });
        else queue.push(e);
      },
      onEnd: () => {
        ended = true;
        if (hooks.onOk) hooks.onOk();
        while (resolvers.length) resolvers.shift()!({ value: undefined, done: true });
      },
      ...(hooks.onReady !== undefined ? { onReady: hooks.onReady } : {}),
      onError: (error) => {
        terminalError = error;
        ended = true;
        if (hooks.onError) hooks.onError(error);
        while (resolvers.length) resolvers.shift()!({ value: undefined, done: true });
      },
    },
  );

  try {
    while (true) {
      if (queue.length) {
        yield queue.shift()!;
        continue;
      }
      if (ended) {
        if (terminalError) throw terminalError;
        return;
      }
      const next = await new Promise<IteratorResult<Event>>((resolve) => resolvers.push(resolve));
      if (next.done) {
        if (terminalError) throw terminalError;
        return;
      }
      yield next.value;
    }
  } finally {
    signal.removeEventListener('abort', onAbort);
    await handle.stop().catch(() => {});
    if (ourAc) ourAc.abort();
  }
}
