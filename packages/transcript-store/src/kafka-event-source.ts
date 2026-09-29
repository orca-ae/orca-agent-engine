// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import type { Admin, Consumer, Kafka } from 'kafkajs';
import { matchSessionTopic, validateTopicPrefix } from './kafka/topic.js';
import { createKafkaTranscriptCodec, type KafkaTranscriptCodec } from './kafka/codec.js';
import { withHeartbeat, KafkaTranscriptHeartbeatError } from './kafka/with-heartbeat.js';
import { KafkaTranscriptCodecError } from './kafka/codec-error.js';
import type { SessionEventSourceStatus } from './store.js';
import type { Event } from './types.js';

export interface KafkaSessionEventSourceOptions {
  kafka: Kafka;
  /** Injected codecs remain caller-owned; readers never prepare a writer. */
  codec?: KafkaTranscriptCodec;
  groupId: string;
  topicPattern?: RegExp;
  /**
   * Optional topic prefix (dot-terminated segments, e.g. `public.default.`).
   * Discovery tolerates both bare and prefixed listings (Kafka-on-Pulsar (KoP)
   * strips the default tenant/namespace prefix from `listTopics()` output,
   * plain Kafka lists the full name) and always subscribes to the canonical
   * (prefixed) name. Defaults to '' (bare names).
   */
  topicPrefix?: string;
  topicDiscoveryIntervalMs?: number;
  signal?: AbortSignal;
}

export type KafkaSessionEventHandler = (event: Event) => Promise<void>;

class KafkaSessionEventSourceStoppingError extends Error {
  constructor() {
    super('Kafka session event source stopped before event acknowledgement');
    this.name = 'KafkaSessionEventSourceStoppingError';
  }
}

/**
 * At-least-once Kafka consumer for all session transcript topics.
 *
 * The consumer group owns offsets; if the handler throws, KafkaJS does not
 * commit that message and another poll/rebalance can retry it.
 */
export class KafkaSessionEventSource {
  private readonly ac = new AbortController();
  private readonly consumers = new Map<string, Consumer>();
  private readonly startingTopics = new Set<string>();
  private readonly decodeFailures = new Map<string, { delayMs: number; retryAfter: number }>();
  private readonly codec: KafkaTranscriptCodec;
  private admin: Admin | null = null;
  private runPromise: Promise<void> | null = null;
  private stopPromise: Promise<void> | null = null;
  private sourceStatus: SessionEventSourceStatus = { ready: false, state: 'stopped' };

  constructor(private readonly opts: KafkaSessionEventSourceOptions) {
    validateTopicPrefix(opts.topicPrefix ?? '');
    this.codec = opts.codec ?? createKafkaTranscriptCodec();
    if (opts.signal) {
      if (opts.signal.aborted) {
        this.ac.abort();
      } else {
        opts.signal.addEventListener(
          'abort',
          () => {
            void this.stop();
          },
          { once: true },
        );
      }
    }
  }

  async start(handler: KafkaSessionEventHandler): Promise<void> {
    if (this.runPromise) return;
    if (this.ac.signal.aborted) return;
    this.sourceStatus = { ready: false, state: 'running' };
    this.runPromise = this.run(handler).catch(() => {
      if (!this.ac.signal.aborted) {
        this.sourceStatus = { ready: false, state: 'failed' };
        console.error('kafka session event source failed');
      }
    });
  }

  async stop(): Promise<void> {
    this.stopPromise ??= this.stopOnce();
    await this.stopPromise;
  }

  private async stopOnce(): Promise<void> {
    this.ac.abort();
    await this.disconnectResources();
    await this.runPromise;
    // A connect/list/subscribe call already in flight when shutdown began can
    // settle after the first snapshot. Drain that late resource before return.
    await this.disconnectResources();
    if (this.opts.codec === undefined) await this.codec.close();
    this.sourceStatus = { ready: false, state: 'stopped' };
  }

  private async disconnectResources(): Promise<void> {
    const consumers = [...this.consumers.values()];
    this.consumers.clear();
    await Promise.all(consumers.map((consumer) => consumer.disconnect().catch(() => null)));
    const admin = this.admin;
    this.admin = null;
    await admin?.disconnect().catch(() => null);
  }

  status(): SessionEventSourceStatus {
    return { ...this.sourceStatus };
  }

  private async run(handler: KafkaSessionEventHandler): Promise<void> {
    const discoveryIntervalMs = this.opts.topicDiscoveryIntervalMs ?? 1_000;

    while (!this.ac.signal.aborted) {
      if (!this.admin) {
        const connected = await this.connectAdmin().catch(() => {
          if (!this.ac.signal.aborted) {
            console.error('kafka session event source admin connect failed');
          }
          return false;
        });
        if (!connected) {
          await sleep(discoveryIntervalMs, this.ac.signal);
          continue;
        }
      }

      await this.discoverTopics(handler).catch(async () => {
        this.sourceStatus = { ready: false, state: 'running' };
        if (!this.ac.signal.aborted) {
          console.error('kafka session event source discovery failed');
        }
        await this.admin?.disconnect().catch(() => null);
        this.admin = null;
      });
      await sleep(discoveryIntervalMs, this.ac.signal);
    }
  }

  private async connectAdmin(): Promise<boolean> {
    const admin = this.opts.kafka.admin();
    await admin.connect();
    if (this.ac.signal.aborted) {
      await admin.disconnect().catch(() => null);
      return false;
    }
    this.admin = admin;
    this.sourceStatus = { ready: this.decodeFailures.size === 0, state: 'running' };
    return true;
  }

  private async discoverTopics(handler: KafkaSessionEventHandler): Promise<void> {
    if (!this.admin) return;
    const topics = await this.admin.listTopics();
    if (this.ac.signal.aborted) return;
    // De-dup by canonical (prefixed) topic: KoP can list the same topic under
    // its bare local name while a plain Kafka broker lists the full name —
    // both resolve to one canonical subscription.
    const canonicalTopics = new Set<string>();
    for (const topic of topics) {
      const canonical = this.canonicalSessionTopic(topic);
      if (canonical !== null) canonicalTopics.add(canonical);
    }
    // Keep every startup owned by the run loop even when a sibling fails, so
    // stop() cannot finish before late connections and their cleanup settle.
    const startups = await Promise.allSettled(
      [...canonicalTopics].map((topic) => this.ensureTopicConsumer(topic, handler)),
    );
    const failure = startups.find((startup) => startup.status === 'rejected');
    if (failure) throw failure.reason;
  }

  /**
   * Resolve a listed topic name to the canonical (prefixed) name to subscribe
   * to, or `null` when it is not a session topic. When `opts.topicPattern` is
   * provided it further filters the prefix-stripped bare session topic name.
   */
  private canonicalSessionTopic(topic: string): string | null {
    const topicPrefix = this.opts.topicPrefix ?? '';
    const match = matchSessionTopic(topic, topicPrefix, this.codec.encoding ?? 'raw');
    if (!match) return null;
    if (this.opts.topicPattern) {
      const bare =
        topicPrefix !== '' && topic.startsWith(topicPrefix)
          ? topic.slice(topicPrefix.length)
          : topic;
      return matchesTopic(this.opts.topicPattern, bare) ? `${topicPrefix}${bare}` : null;
    }
    return match.canonicalTopic;
  }

  private async ensureTopicConsumer(
    topic: string,
    handler: KafkaSessionEventHandler,
  ): Promise<void> {
    if (this.ac.signal.aborted) return;
    if (this.consumers.has(topic) || this.startingTopics.has(topic)) return;
    const retry = this.decodeFailures.get(topic);
    if (retry && Date.now() < retry.retryAfter) return;
    this.startingTopics.add(topic);
    const consumer = this.opts.kafka.consumer({
      groupId: topicGroupId(this.opts.groupId, topic),
      retry: {
        // Topic discovery is the sole restart owner. KafkaJS restarting the
        // same consumer races stop() because its internal start promise is not
        // exposed for lifecycle fencing.
        restartOnFailure: async () => false,
      },
    });
    this.consumers.set(topic, consumer);
    let generation = 0;
    consumer.on(consumer.events.GROUP_JOIN, () => {
      generation += 1;
    });
    consumer.on(consumer.events.CRASH, () => {
      if (this.ac.signal.aborted || this.consumers.get(topic) !== consumer) return;
      this.consumers.delete(topic);
      void consumer.disconnect().catch(() => null);
    });
    const encoding = this.codec.encoding ?? 'raw';
    const expectedRoute = matchSessionTopic(topic, this.opts.topicPrefix ?? '', encoding);
    try {
      await consumer.connect();
      if (this.ac.signal.aborted) {
        this.consumers.delete(topic);
        await consumer.disconnect().catch(() => null);
        return;
      }
      await consumer.subscribe({ topic, fromBeginning: true });
      if (this.ac.signal.aborted) {
        this.consumers.delete(topic);
        await consumer.disconnect().catch(() => null);
        return;
      }
      await consumer.run({
        eachMessage: async ({ topic: messageTopic, message, heartbeat }) => {
          const deliveryGeneration = generation;
          const assertOwned = (): void => {
            if (
              this.ac.signal.aborted ||
              this.consumers.get(topic) !== consumer ||
              generation !== deliveryGeneration
            ) {
              throw new KafkaSessionEventSourceStoppingError();
            }
          };
          assertOwned();
          const actualRoute = matchSessionTopic(
            messageTopic,
            this.opts.topicPrefix ?? '',
            encoding,
          );
          if (
            !expectedRoute ||
            !actualRoute ||
            actualRoute.canonicalTopic !== expectedRoute.canonicalTopic
          ) {
            return;
          }
          let event: Event | null;
          try {
            event = await withHeartbeat(heartbeat, this.ac.signal, assertOwned, (signal) =>
              this.codec.decode(message, actualRoute, signal),
            );
          } catch (cause) {
            assertOwned();
            const retry = this.decodeFailures.get(topic) ?? { delayMs: 0, retryAfter: 0 };
            this.sourceStatus = { ready: false, state: 'running' };
            // Readiness also tracks heartbeat failures, but only codec failures
            // throttle discovery. Reconnect/join alone cannot prove decode recovery.
            if (!(cause instanceof KafkaTranscriptHeartbeatError)) {
              retry.delayMs = Math.min(Math.max(retry.delayMs * 2, 1_000), 30_000);
              retry.retryAfter = Date.now() + retry.delayMs;
            }
            this.decodeFailures.set(topic, retry);
            // KafkaJS may log thrown errors: never attach a transport cause.
            throw cause instanceof KafkaTranscriptHeartbeatError
              ? new KafkaTranscriptHeartbeatError()
              : cause instanceof KafkaTranscriptCodecError
                ? new KafkaTranscriptCodecError(cause.code, cause.retryable)
                : new Error('Kafka transcript decode failed');
          }
          assertOwned();
          this.decodeFailures.delete(topic);
          this.sourceStatus = { ready: this.decodeFailures.size === 0, state: 'running' };
          // A forged route is poison input. Returning successfully lets the
          // consumer group commit/drop it without invoking tenant code.
          if (!event) return;
          if (this.ac.signal.aborted) throw new KafkaSessionEventSourceStoppingError();
          await handler(event);
          assertOwned();
        },
      });
      if (this.ac.signal.aborted) {
        this.consumers.delete(topic);
        await consumer.disconnect().catch(() => null);
      }
    } catch (err) {
      this.consumers.delete(topic);
      await consumer.disconnect().catch(() => null);
      if (this.ac.signal.aborted) return;
      throw err;
    } finally {
      this.startingTopics.delete(topic);
    }
  }
}

function matchesTopic(pattern: RegExp, topic: string): boolean {
  pattern.lastIndex = 0;
  return pattern.test(topic);
}

function topicGroupId(baseGroupId: string, topic: string): string {
  const digest = createHash('sha1').update(topic).digest('hex').slice(0, 16);
  return `${baseGroupId}-${digest}`;
}

async function sleep(ms: number, signal: AbortSignal): Promise<void> {
  await new Promise<void>((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const done = () => {
      clearTimeout(timeout);
      signal.removeEventListener('abort', done);
      resolve();
    };
    const timeout = setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
  });
}
