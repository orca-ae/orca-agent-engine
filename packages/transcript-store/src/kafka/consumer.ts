// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { setTimeout as delay } from 'node:timers/promises';
import type { Kafka, EachMessagePayload, KafkaMessage } from 'kafkajs';
import { matchSessionTopic, sessionTopicName, parseCursor } from './topic.js';
import { createKafkaTranscriptCodec, type KafkaTranscriptCodec } from './codec.js';
import { withHeartbeat, KafkaTranscriptHeartbeatError } from './with-heartbeat.js';
import { KafkaTranscriptCodecError } from './codec-error.js';
import type { Event } from '../types.js';

const TAIL_SUBSCRIBE_RETRY_DELAYS_MS = [250, 500, 1000] as const;

export interface ConsumeOptions {
  /** Injected codecs remain caller-owned. */
  codec?: KafkaTranscriptCodec;
  workspaceId: string;
  sessionId: string;
  fromCursor: string;
  subpath: string;
  maxEvents?: number; // 0 / undefined = no caller-imposed cap; see mode
  /** Bounded-mode cap over cumulative broker message bytes; 0 / undefined = unbounded. */
  maxBytes?: number;
  /** Reports the next offset only after successful decode or an explicit route skip. */
  onScannedCursor?: (nextCursor: string) => void;
  signal: AbortSignal;
  /**
   * 'bounded'   — drain to the high-watermark at call time, then end.
   *               Used by Read RPC.
   * 'unbounded' — run indefinitely until cancelled.
   *               Used by Tail RPC.
   */
  mode: 'bounded' | 'unbounded';
  /**
   * Optional topic prefix (dot-terminated segments, e.g. `public.default.`).
   * Prepended to the bare session topic name for both the high-watermark
   * lookup and the subscription. Defaults to '' (bare names).
   */
  topicPrefix?: string;
}

export interface ConsumeCallbacks {
  onEvent: (event: Event) => Promise<void> | void;
  onEnd: () => void;
  onReady?: () => void;
  onError?: (err: Error) => void;
}

/**
 * Fetch the high-watermark (next-to-be-written offset) for partition 0 of a
 * topic.  Returns `0n` if the topic doesn't exist or has no messages.
 */
async function getHighWatermark(kafka: Kafka, topic: string, signal: AbortSignal): Promise<bigint> {
  const admin = kafka.admin();
  const onAbort = (): void => {
    void admin.disconnect().catch(() => undefined);
  };
  signal.throwIfAborted();
  signal.addEventListener('abort', onAbort, { once: true });
  try {
    await admin.connect();
    signal.throwIfAborted();
    const offsets = await admin.fetchTopicOffsets(topic);
    signal.throwIfAborted();
    return BigInt(offsets[0]?.high ?? '0');
  } finally {
    signal.removeEventListener('abort', onAbort);
    await admin.disconnect();
  }
}

function isUnknownTopicError(err: unknown): boolean {
  const seen = new Set<unknown>();
  let current = err;

  while (typeof current === 'object' && current !== null && !seen.has(current)) {
    seen.add(current);
    const kafkaError = current as {
      type?: unknown;
      code?: unknown;
      message?: unknown;
      cause?: unknown;
    };
    if (
      kafkaError.type === 'UNKNOWN_TOPIC_OR_PARTITION' ||
      kafkaError.code === 3 ||
      (typeof kafkaError.message === 'string' &&
        (/UNKNOWN_TOPIC/i.test(kafkaError.message) ||
          /unknown topic/i.test(kafkaError.message) ||
          /does not host this topic-partition/i.test(kafkaError.message)))
    ) {
      return true;
    }
    current = kafkaError.cause;
  }

  return false;
}

/**
 * Drive a kafkajs consumer with `seek()` for cursor positioning.  No consumer
 * group commit (groupId is a unique throwaway string).  Returns a `stop()`
 * function that disconnects the consumer; idempotent.
 *
 * Bounded mode: fetches the high-watermark before subscribing, then ends the
 * stream once that offset is reached, maxEvents is delivered, or maxBytes of
 * broker-reported message data is scanned. The first raw record is admitted
 * even when it alone exceeds maxBytes; onScannedCursor reports that progress
 * after successful decode or an explicit route skip, never after decode failure.
 *
 * Unbounded mode: runs indefinitely until the AbortSignal fires or stop() is
 * called explicitly.
 */
export function consumeSession(
  kafka: Kafka,
  opts: ConsumeOptions,
  cb: ConsumeCallbacks,
): { stop: () => Promise<void> } {
  const codec = opts.codec ?? createKafkaTranscriptCodec();
  const encoding = codec.encoding ?? 'raw';
  const topic = sessionTopicName(
    opts.workspaceId,
    opts.sessionId,
    opts.topicPrefix ?? '',
    encoding,
  );
  const groupId = `transcript-store-${Math.random().toString(36).slice(2)}-${Date.now()}`;
  let stopped = false;
  const stopController = new AbortController();
  const lifecycleSignal = AbortSignal.any([opts.signal, stopController.signal]);
  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: 10000,
    retry: { restartOnFailure: async () => !stopped && !lifecycleSignal.aborted },
  });
  let disconnectPromise: Promise<void> | undefined;
  let stopPromise: Promise<void> | undefined;
  let removeLifecycleCrashListener: (() => void) | undefined;
  let generation = 0;
  const removeGenerationListener = consumer.on(consumer.events.GROUP_JOIN, () => {
    generation += 1;
  });
  let delivered = 0;
  let endSignalled = false;
  const subpathFilter = opts.subpath;

  const disconnectConsumer = async (): Promise<void> => {
    try {
      await consumer.disconnect();
    } catch {
      /* ignore disconnect errors */
    }
  };

  function requestStop(): void {
    stopped = true;
    stopController.abort();
    opts.signal.removeEventListener('abort', onAbort);
    disconnectPromise ??= disconnectConsumer().then(async () => {
      if (opts.codec === undefined) await codec.close();
    });
  }

  async function stop(): Promise<void> {
    requestStop();
    stopPromise ??= (async () => {
      await disconnectPromise;
      await lifecyclePromise.catch(() => undefined);
      // A connect or KafkaJS restart already in flight can settle after the
      // first disconnect. The second pass makes stop() a quiescence boundary.
      await disconnectConsumer();
      removeLifecycleCrashListener?.();
      removeGenerationListener();
    })();
    await stopPromise;
  }

  function onAbort(): void {
    void stop();
  }

  function signalEnd(): void {
    if (!endSignalled) {
      endSignalled = true;
      cb.onEnd();
    }
    requestStop();
  }

  const run = async () => {
    if (stopped || lifecycleSignal.aborted) return;
    // --- Bounded: fetch high-watermark before subscribing ---
    let lastOffsetToDeliver: bigint | null = null; // inclusive last offset to deliver
    let startOffsetToSeek: bigint | null = null;
    const callerMaxEvents: number =
      opts.maxEvents === undefined || opts.maxEvents === 0 ? 0 : opts.maxEvents;
    const callerMaxBytes =
      opts.mode === 'bounded' && opts.maxBytes !== undefined && opts.maxBytes > 0
        ? opts.maxBytes
        : 0;
    let consumedBytes = 0;
    let scannedMessages = 0;
    const parsedStartOffset = parseCursor(opts.fromCursor);

    // The offset we seek to after run() joins the group.  An explicit cursor
    // applies in BOTH modes.  In unbounded (Tail) mode with an EMPTY cursor the
    // contract is "from-now" (TailOptions.fromCursor "" = current head), so the
    // start offset is the high-watermark at subscribe time — NOT offset 0.  This
    // makes Kafka match the Pulsar ('Latest') and Postgres (highWatermark+1)
    // backends and the documented TailOptions contract; without it a tail would
    // replay the whole topic from the beginning.

    if (opts.mode === 'bounded') {
      let hwm: bigint;
      try {
        hwm = await getHighWatermark(kafka, topic, lifecycleSignal);
      } catch (err: unknown) {
        if (stopped || lifecycleSignal.aborted) return;
        if (isUnknownTopicError(err)) {
          signalEnd();
          return;
        }
        if (cb.onError) cb.onError(err as Error);
        else throw err;
        requestStop();
        return;
      }
      if (stopped || lifecycleSignal.aborted) return;

      if (hwm === 0n) {
        // Topic is empty (or doesn't exist yet via auto-create path)
        signalEnd();
        return;
      }

      // Apply cursor: if fromCursor is set, we start at that offset; the
      // effective end is still hwm-1.
      const startOff = parsedStartOffset !== null ? parsedStartOffset : 0n;
      startOffsetToSeek = parsedStartOffset;

      if (startOff >= hwm) {
        // Start is already past the end; nothing to deliver.
        signalEnd();
        return;
      }

      // last offset we should deliver (inclusive)
      lastOffsetToDeliver = hwm - 1n;

      // If caller specified a maxEvents cap, tighten the window.
      if (callerMaxEvents > 0) {
        const cappedLast = startOff + BigInt(callerMaxEvents) - 1n;
        if (cappedLast < lastOffsetToDeliver) {
          lastOffsetToDeliver = cappedLast;
        }
      }
    } else if (parsedStartOffset !== null) {
      startOffsetToSeek = parsedStartOffset;
    } else {
      // --- Unbounded "from-now": seek to the current head ---
      // Resolve the high-watermark (next-to-be-written offset) and start there,
      // so only events appended AFTER this tail subscribed are delivered.
      try {
        startOffsetToSeek = await getHighWatermark(kafka, topic, lifecycleSignal);
      } catch (err: unknown) {
        if (stopped || lifecycleSignal.aborted) return;
        if (isUnknownTopicError(err)) {
          // For a new session on an auto-create Kafka cluster, subscribe below
          // creates the topic and the tail starts at offset 0. A topic that
          // stays unavailable fails the tail after bounded subscribe retries.
          startOffsetToSeek = 0n;
        } else if (cb.onError) {
          cb.onError(err as Error);
          requestStop();
          return;
        } else {
          throw err;
        }
      }
    }

    if (stopped || lifecycleSignal.aborted) return;
    // --- Subscribe ---
    try {
      await consumer.connect();
      if (stopped || lifecycleSignal.aborted) {
        await disconnectConsumer();
        return;
      }
      for (let attempt = 0; ; attempt++) {
        if (stopped || lifecycleSignal.aborted) return;
        try {
          await consumer.subscribe({
            topic,
            fromBeginning: opts.mode === 'bounded' || parsedStartOffset !== null,
          });
          break;
        } catch (err: unknown) {
          // Auto-creation can return UNKNOWN_TOPIC_OR_PARTITION before the
          // topic becomes visible. Keep this tail and its pinned start offset
          // alive, rather than turning the creation window into normal EOF.
          const retryDelay =
            opts.mode === 'unbounded' && isUnknownTopicError(err)
              ? TAIL_SUBSCRIBE_RETRY_DELAYS_MS[attempt]
              : undefined;
          if (retryDelay === undefined) throw err;
          await delay(retryDelay, undefined, { signal: lifecycleSignal });
        }
      }
      if (stopped || lifecycleSignal.aborted) {
        await disconnectConsumer();
        return;
      }
    } catch (err: unknown) {
      if (stopped || lifecycleSignal.aborted) {
        await disconnectConsumer();
        return;
      }
      if (opts.mode === 'bounded' && isUnknownTopicError(err)) {
        signalEnd();
        return;
      }
      if (cb.onError) cb.onError(err as Error);
      else throw err;
      requestStop();
      return;
    }

    // --- Run ---
    // KafkaJS can resolve consumer.run() after an initial retriable group-join
    // failure and restart the runner in the background. Wait for the actual
    // GROUP_JOIN event before seeking or declaring the tail ready.
    let removeGroupJoinListener: (() => void) | undefined;
    let removeCrashListener: (() => void) | undefined;
    let removeAbortListener: (() => void) | undefined;
    let joined = false;
    let terminalCrashReported = false;
    const reportTerminalCrash = (error: Error): void => {
      if (terminalCrashReported || stopped || lifecycleSignal.aborted) return;
      terminalCrashReported = true;
      cb.onError?.(error);
      requestStop();
    };
    removeLifecycleCrashListener = consumer.on(consumer.events.CRASH, ({ payload }) => {
      if (payload.restart) {
        if (stopped || lifecycleSignal.aborted) {
          queueMicrotask(() => {
            void disconnectConsumer();
          });
        }
        return;
      }
      if (joined) reportTerminalCrash(payload.error);
    });
    const groupJoined = new Promise<void>((resolve, reject) => {
      let settled = false;
      const settle = (complete: () => void) => {
        if (settled) return;
        settled = true;
        removeGroupJoinListener?.();
        removeCrashListener?.();
        removeAbortListener?.();
        complete();
      };
      removeGroupJoinListener = consumer.on(consumer.events.GROUP_JOIN, () => {
        joined = true;
        settle(resolve);
      });
      removeCrashListener = consumer.on(consumer.events.CRASH, ({ payload }) => {
        if (!payload.restart) settle(() => reject(payload.error));
      });
      const onAbort = () => settle(resolve);
      lifecycleSignal.addEventListener('abort', onAbort, { once: true });
      removeAbortListener = () => lifecycleSignal.removeEventListener('abort', onAbort);
      if (lifecycleSignal.aborted) onAbort();
    });

    const runPromise = consumer.run({
      eachMessage: async ({ topic: messageTopic, message, heartbeat }: EachMessagePayload) => {
        if (stopped || lifecycleSignal.aborted) return;
        const deliveryGeneration = generation;
        const assertOwned = (): void => {
          lifecycleSignal.throwIfAborted();
          if (generation !== deliveryGeneration) throw new KafkaTranscriptHeartbeatError();
        };
        const messageBytes = kafkaMessageBytes(message);
        if (
          callerMaxBytes > 0 &&
          scannedMessages > 0 &&
          consumedBytes + messageBytes > callerMaxBytes
        ) {
          signalEnd();
          return;
        }
        consumedBytes += messageBytes;
        scannedMessages += 1;
        const byteLimitReached = callerMaxBytes > 0 && consumedBytes >= callerMaxBytes;
        const actualRoute = matchSessionTopic(messageTopic, opts.topicPrefix ?? '', encoding);
        let event: Event | null = null;
        try {
          if (actualRoute?.canonicalTopic === topic) {
            event = await withHeartbeat(heartbeat, lifecycleSignal, assertOwned, (signal) =>
              codec.decode(message, actualRoute, signal),
            );
          }
          assertOwned();
        } catch (cause) {
          if (stopped || lifecycleSignal.aborted) return;
          // Do not expose transport responses or allow KafkaJS to retry past
          // the failing record behind a read/tail caller's back.
          const error =
            cause instanceof KafkaTranscriptHeartbeatError
              ? new KafkaTranscriptHeartbeatError()
              : cause instanceof KafkaTranscriptCodecError
                ? new KafkaTranscriptCodecError(cause.code, cause.retryable)
                : new Error('Kafka transcript decode failed');
          reportTerminalCrash(error);
          throw error;
        }
        if (stopped || lifecycleSignal.aborted) return;
        opts.onScannedCursor?.((BigInt(message.offset) + 1n).toString());
        if (!event) {
          // Route mismatches are poison input, not transient delivery errors.
          // Return successfully so Kafka advances past the forged message.
          if (
            byteLimitReached ||
            (lastOffsetToDeliver !== null && BigInt(message.offset) >= lastOffsetToDeliver)
          ) {
            signalEnd();
          }
          return;
        }
        if (!matchesSubpath(event.subpath, subpathFilter)) {
          // Still need to check if this was the last offset for bounded mode
          if (
            byteLimitReached ||
            (lastOffsetToDeliver !== null && BigInt(message.offset) >= lastOffsetToDeliver)
          ) {
            signalEnd();
          }
          return;
        }
        await cb.onEvent(event);
        if (stopped || lifecycleSignal.aborted) return;
        delivered++;

        if (byteLimitReached) {
          signalEnd();
          return;
        }

        if (opts.mode === 'bounded' && lastOffsetToDeliver !== null) {
          if (BigInt(message.offset) >= lastOffsetToDeliver) {
            signalEnd();
            return;
          }
        }

        // Unbounded mode with explicit maxEvents cap
        if (opts.mode === 'unbounded' && callerMaxEvents > 0 && delivered >= callerMaxEvents) {
          signalEnd();
        }
      },
    });
    try {
      await runPromise;
      await groupJoined;
    } finally {
      removeGroupJoinListener?.();
      removeCrashListener?.();
      removeAbortListener?.();
    }

    if (stopped || lifecycleSignal.aborted) return;

    // --- Seek to the start offset ---
    // Must be called after GROUP_JOIN, when consumerGroup is initialized.
    // `startOffsetToSeek` is the
    // explicit cursor (both modes) or, for an empty-cursor unbounded tail, the
    // from-now high-watermark resolved above.  `null` means start from the
    // subscription default (offset 0 / fromBeginning).
    if (startOffsetToSeek !== null) {
      consumer.seek({ topic, partition: 0, offset: startOffsetToSeek.toString() });
    }
    cb.onReady?.();
  };

  const lifecyclePromise = run().catch((err) => {
    if (stopped || lifecycleSignal.aborted) return;
    cb.onError?.(err);
    requestStop();
  });
  if (opts.signal.aborted) onAbort();
  else opts.signal.addEventListener('abort', onAbort, { once: true });

  return { stop };
}

function kafkaMessageBytes(message: KafkaMessage): number {
  if (typeof message.size === 'number' && Number.isSafeInteger(message.size) && message.size >= 0) {
    return message.size;
  }

  let bytes = (message.key?.byteLength ?? 0) + (message.value?.byteLength ?? 0);
  for (const [name, value] of Object.entries(message.headers ?? {})) {
    bytes += Buffer.byteLength(name, 'utf8') + kafkaHeaderValueBytes(value);
  }
  return bytes;
}

function kafkaHeaderValueBytes(value: Buffer | string | (Buffer | string)[] | undefined): number {
  if (value === undefined) return 0;
  if (Array.isArray(value)) {
    return value.reduce((bytes, entry) => bytes + Buffer.byteLength(entry), 0);
  }
  return Buffer.byteLength(value);
}

function matchesSubpath(eventSubpath: string, filter: string): boolean {
  if (filter === '') return eventSubpath === '';
  if (filter === '*') return true;
  return eventSubpath === filter;
}
