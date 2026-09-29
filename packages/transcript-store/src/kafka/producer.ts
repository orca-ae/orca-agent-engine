// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Kafka, Producer } from 'kafkajs';
import { LruDedup } from './dedup.js';
import { sessionTopicName, validateTopicPrefix } from './topic.js';
import { createKafkaTranscriptCodec, type KafkaTranscriptCodec } from './codec.js';
import type { Event } from '../types.js';
import { dedupHits } from '../metrics.js';
import { assertEventsMatchRoute } from '../route.js';

const DEDUP_CAPACITY = 1024;

export interface TranscriptProducerOptions {
  codec?: KafkaTranscriptCodec;
}

export class TranscriptProducer {
  private producer: Producer;
  private connected = false;
  private dedup = new Map<string, LruDedup<number>>(); // workspace/session -> LRU<event_id, seq>
  private readonly pendingAppends = new Map<string, Promise<void>>();
  private readonly topicPrefix: string;
  private readonly codec: KafkaTranscriptCodec;
  private readonly ownsCodec: boolean;

  constructor(kafka: Kafka, topicPrefix = '', opts: TranscriptProducerOptions = {}) {
    validateTopicPrefix(topicPrefix);
    this.topicPrefix = topicPrefix;
    this.codec = opts.codec ?? createKafkaTranscriptCodec();
    this.ownsCodec = opts.codec === undefined;
    this.producer = kafka.producer({ idempotent: true, maxInFlightRequests: 5 });
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    await this.codec.prepareWriter();
    await this.producer.connect();
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    try {
      if (this.connected) await this.producer.disconnect();
      this.connected = false;
    } finally {
      if (this.ownsCodec) await this.codec.close();
    }
  }

  /**
   * Append events to a session topic.
   * Returns the list of event IDs corresponding to each input position
   * (retries with the same event.id return the cached id; new events return their own id).
   */
  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    if (events.length === 0) return [];
    assertEventsMatchRoute(workspaceId, sessionId, events);
    // Schema cache misses must not reorder same-session appends. A rejected
    // append releases the queue without poisoning retries or other sessions.
    const key = `${workspaceId}\0${sessionId}`;
    const previous = this.pendingAppends.get(key) ?? Promise.resolve();
    const append = previous.then(() => this.appendBatch(workspaceId, sessionId, events));
    const settled = append.then(
      () => undefined,
      () => undefined,
    );
    this.pendingAppends.set(key, settled);
    try {
      return await append;
    } finally {
      if (this.pendingAppends.get(key) === settled) this.pendingAppends.delete(key);
    }
  }

  private async appendBatch(
    workspaceId: string,
    sessionId: string,
    events: Event[],
  ): Promise<string[]> {
    const topic = sessionTopicName(
      workspaceId,
      sessionId,
      this.topicPrefix,
      this.codec.encoding ?? 'raw',
    );
    const lru = this.lruFor(workspaceId, sessionId);

    const newEvents: Event[] = [];
    const result: string[] = new Array(events.length);
    for (let i = 0; i < events.length; i++) {
      const event = events[i]!;
      const dedupSeq = lru.get(event.id);
      if (dedupSeq !== undefined) {
        event.seq = dedupSeq;
        result[i] = event.id;
        dedupHits.inc();
      } else {
        newEvents.push(event);
        result[i] = event.id;
      }
    }

    if (newEvents.length > 0) {
      const messages = await Promise.all(newEvents.map((event) => this.codec.encode(event)));
      const metadata = await this.producer.send({
        topic,
        messages,
        acks: -1,
      });
      const baseOffset = producerBatchBaseOffset(metadata);
      if (baseOffset !== null) {
        for (let i = 0; i < newEvents.length; i++) {
          newEvents[i]!.seq = baseOffset + i;
        }
      }
      for (const e of newEvents) lru.add(e.id, e.seq);
    }

    return result;
  }

  private lruFor(workspaceId: string, sessionId: string): LruDedup<number> {
    const key = `${workspaceId}\0${sessionId}`;
    let lru = this.dedup.get(key);
    if (!lru) {
      lru = new LruDedup<number>(DEDUP_CAPACITY);
      this.dedup.set(key, lru);
    }
    return lru;
  }
}

function producerBatchBaseOffset(metadata: Awaited<ReturnType<Producer['send']>>): number | null {
  const raw = metadata[0]?.baseOffset ?? metadata[0]?.offset;
  if (raw === undefined) return null;
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}
