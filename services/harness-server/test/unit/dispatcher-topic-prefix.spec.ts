// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { createKafkaTranscriptCodec } from '@orca/transcript-store';
import { Dispatcher } from '../../src/runner/dispatcher.js';

describe('Dispatcher topic-prefix discovery', () => {
  it.each(['', 'test.', 'public.default.'])(
    'subscribes only the Avro topic set under %j even with a broad custom filter',
    async (prefix) => {
      const raw = 'orca.ws_snapshot.sessions.ses_snapshot.events';
      const avro = `${raw}-avro`;
      const listedPrefix = prefix === 'public.default.' ? '' : prefix;
      const kafka = new SubscriptionRecordingKafka([
        `${listedPrefix}${raw}`,
        `${listedPrefix}${avro}`,
        `${listedPrefix}unrelated`,
      ]);
      const codec = createKafkaTranscriptCodec({
        encoding: 'avro',
        schemaRegistry: { url: 'http://127.0.0.1:1' },
      });
      const decode = vi.spyOn(codec, 'decode');
      const dispatcher = new Dispatcher({
        kafka: kafka as never,
        codec,
        topicPrefix: prefix,
        topicPattern: /.*/,
        groupId: 'dispatcher-avro-topic-set',
        store: {},
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        topicRediscoverIntervalMs: 60_000,
      } as ConstructorParameters<typeof Dispatcher>[0]);
      try {
        await dispatcher.start();
        expect(kafka.consumers).toHaveLength(1);
        expect(kafka.consumers[0]?.subscriptions).toEqual([
          { topics: [`${prefix}${avro}`], fromBeginning: true },
        ]);
        // Even a direct delivery bypassing broker subscription cannot decode old history.
        await (dispatcher as unknown as { onMessage(delivery: unknown): Promise<void> }).onMessage({
          topic: `${prefix}${raw}`,
          partition: 0,
          message: {
            offset: '0',
            value: Buffer.from('{}'),
            headers: {
              workspace_id: 'ws_snapshot',
              session_id: 'ses_snapshot',
              produced_by: 'client',
              kind: 'user.message',
            },
          },
        });
        expect(decode).not.toHaveBeenCalled();
      } finally {
        await dispatcher.stop();
        await codec.close();
      }
    },
  );

  it.each([undefined, 'raw'] as const)(
    'keeps the raw topic set with encoding %s and a registry URL',
    async (encoding) => {
      const raw = 'orca.ws_snapshot.sessions.ses_snapshot.events';
      const kafka = new SubscriptionRecordingKafka([raw, `${raw}-avro`, 'unrelated']);
      const codec = createKafkaTranscriptCodec({
        ...(encoding === undefined ? {} : { encoding }),
        schemaRegistry: { url: 'http://127.0.0.1:1' },
      });
      const dispatcher = new Dispatcher({
        kafka: kafka as never,
        codec,
        topicPattern: /.*/,
        groupId: 'dispatcher-raw-topic-set',
        store: {},
        anthropicApiKey: 'unused',
        modelDefault: 'fake',
        topicRediscoverIntervalMs: 60_000,
      } as ConstructorParameters<typeof Dispatcher>[0]);
      try {
        await dispatcher.start();
        expect(kafka.consumers).toHaveLength(1);
        expect(kafka.consumers[0]?.subscriptions).toEqual([{ topics: [raw], fromBeginning: true }]);
      } finally {
        await dispatcher.stop();
        await codec.close();
      }
    },
  );

  it('prefers real prefixed topics over unrelated bare topics on plain Kafka', async () => {
    const dispatcher = new Dispatcher({
      kafka: fakeKafka([
        'orca.ws_stale.sessions.ses_stale.events',
        'phase51.output.orca.ws_live.sessions.ses_live.events',
      ]),
      topicPrefix: 'phase51.output.',
      groupId: 'dispatcher-topic-prefix-unit',
      store: {},
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
    } as ConstructorParameters<typeof Dispatcher>[0]);

    const topics = await (
      dispatcher as unknown as { listMatchingSessionTopics(): Promise<string[] | null> }
    ).listMatchingSessionTopics();

    expect(topics).toEqual(['phase51.output.orca.ws_live.sessions.ses_live.events']);
  });

  it('falls back to bare topic listings for KoP-style prefixed topics', async () => {
    const dispatcher = new Dispatcher({
      kafka: fakeKafka(['orca.ws_live.sessions.ses_live.events']),
      topicPrefix: 'public.default.',
      groupId: 'dispatcher-topic-prefix-kop-unit',
      store: {},
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
    } as ConstructorParameters<typeof Dispatcher>[0]);

    const topics = await (
      dispatcher as unknown as { listMatchingSessionTopics(): Promise<string[] | null> }
    ).listMatchingSessionTopics();

    expect(topics).toEqual(['public.default.orca.ws_live.sessions.ses_live.events']);
  });

  it('applies custom topicPattern to bare names before canonicalizing prefixes', async () => {
    const dispatcher = new Dispatcher({
      kafka: fakeKafka([
        'orca.ws_skip.sessions.ses_skip.events',
        'public.default.orca.ws_live.sessions.ses_live.events',
      ]),
      topicPrefix: 'public.default.',
      topicPattern: /^orca\.ws_live\.sessions\.ses_live\.events$/,
      groupId: 'dispatcher-topic-pattern-unit',
      store: {},
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
    } as ConstructorParameters<typeof Dispatcher>[0]);

    const topics = await (
      dispatcher as unknown as { listMatchingSessionTopics(): Promise<string[] | null> }
    ).listMatchingSessionTopics();

    expect(topics).toEqual(['public.default.orca.ws_live.sessions.ses_live.events']);
  });

  it('subscribes bare Kafka from the exact listTopics snapshot', async () => {
    const topic = 'orca.ws_snapshot.sessions.ses_snapshot.events';
    const kafka = new SubscriptionRecordingKafka([topic]);
    const dispatcher = new Dispatcher({
      kafka: kafka as never,
      groupId: 'dispatcher-bare-topic-list-unit',
      store: {},
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      topicRediscoverIntervalMs: 60_000,
    } as ConstructorParameters<typeof Dispatcher>[0]);

    try {
      await dispatcher.start();

      expect(kafka.consumers[0]?.subscriptions).toEqual([{ topics: [topic], fromBeginning: true }]);
    } finally {
      await dispatcher.stop();
    }
  });

  it('subscribes prefixed Kafka from the same canonical snapshot', async () => {
    const localTopic = 'orca.ws_snapshot.sessions.ses_snapshot.events';
    const canonicalTopic = `public.default.${localTopic}`;
    const kafka = new SubscriptionRecordingKafka([localTopic]);
    const dispatcher = new Dispatcher({
      kafka: kafka as never,
      topicPrefix: 'public.default.',
      groupId: 'dispatcher-prefixed-topic-list-unit',
      store: {},
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
      topicRediscoverIntervalMs: 60_000,
    } as ConstructorParameters<typeof Dispatcher>[0]);

    try {
      await dispatcher.start();

      expect(kafka.consumers[0]?.subscriptions).toEqual([
        { topics: [canonicalTopic], fromBeginning: true },
      ]);
    } finally {
      await dispatcher.stop();
    }
  });
});

function fakeKafka(topics: string[]): unknown {
  return {
    admin: () => ({
      connect: async () => {},
      listTopics: async () => topics,
      disconnect: async () => {},
    }),
  };
}

class SubscriptionRecordingKafka {
  readonly consumers: SubscriptionRecordingConsumer[] = [];

  constructor(private readonly topics: string[]) {}

  admin() {
    return {
      connect: async () => {},
      listTopics: async () => this.topics,
      disconnect: async () => {},
    };
  }

  consumer() {
    const consumer = new SubscriptionRecordingConsumer();
    this.consumers.push(consumer);
    return consumer;
  }
}

class SubscriptionRecordingConsumer {
  readonly events = {
    GROUP_JOIN: 'consumer.group_join',
    CRASH: 'consumer.crash',
    REBALANCING: 'consumer.rebalancing',
  };
  readonly subscriptions: unknown[] = [];
  private readonly listeners = new Map<string, Array<(event: unknown) => void>>();

  async connect(): Promise<void> {}

  async disconnect(): Promise<void> {}

  async subscribe(subscription: unknown): Promise<void> {
    this.subscriptions.push(subscription);
  }

  async run(): Promise<void> {
    this.emit(this.events.GROUP_JOIN, { payload: { memberAssignment: {} } });
  }

  on(eventName: string, listener: (event: unknown) => void): () => void {
    const listeners = this.listeners.get(eventName) ?? [];
    listeners.push(listener);
    this.listeners.set(eventName, listeners);
    return () => {};
  }

  private emit(eventName: string, event: unknown): void {
    for (const listener of this.listeners.get(eventName) ?? []) listener(event);
  }
}
