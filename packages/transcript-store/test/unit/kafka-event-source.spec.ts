// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import type { EachMessagePayload, Kafka, KafkaMessage } from 'kafkajs';
import { KafkaSessionEventSource } from '../../src/kafka-event-source.js';
import { kafkaMessageToEventForRoute } from '../../src/kafka/serialize.js';
import { KafkaTranscriptCodecError } from '../../src/kafka/codec-error.js';
import { KafkaTranscriptHeartbeatError } from '../../src/kafka/with-heartbeat.js';

interface SubscribeCall {
  topic: string;
  fromBeginning?: boolean;
}

class FakeConsumer {
  readonly events = { CRASH: 'consumer.crash', GROUP_JOIN: 'consumer.group_join' } as const;
  subscribeCalls: SubscribeCall[] = [];
  groupId: string;
  connected = false;
  disconnectCalls = 0;
  private eachMessage?: (payload: EachMessagePayload) => Promise<void>;
  private readonly listeners = new Map<string, Set<(event: unknown) => void>>();

  constructor(
    groupId: string,
    private readonly connectGate?: Promise<void>,
    readonly restartOnFailure?: () => Promise<boolean>,
  ) {
    this.groupId = groupId;
  }

  async connect(): Promise<void> {
    await this.connectGate;
    this.connected = true;
  }
  async disconnect(): Promise<void> {
    this.disconnectCalls += 1;
    this.connected = false;
  }
  async subscribe(opts: SubscribeCall): Promise<void> {
    this.subscribeCalls.push(opts);
  }
  async run(opts: { eachMessage: (payload: EachMessagePayload) => Promise<void> }): Promise<void> {
    this.eachMessage = opts.eachMessage;
  }

  on(eventName: string, listener: (event: unknown) => void): () => void {
    const listeners = this.listeners.get(eventName) ?? new Set();
    listeners.add(listener);
    this.listeners.set(eventName, listeners);
    return () => listeners.delete(listener);
  }

  crash(error: Error, restart: boolean): void {
    for (const listener of this.listeners.get(this.events.CRASH) ?? []) {
      listener({ payload: { error, restart } });
    }
  }

  rejoin(): void {
    for (const listener of this.listeners.get(this.events.GROUP_JOIN) ?? []) {
      listener({ payload: {} });
    }
  }

  async emit(
    topic: string,
    message: KafkaMessage,
    heartbeat = async () => undefined,
  ): Promise<void> {
    if (!this.eachMessage) throw new Error('consumer is not running');
    await this.eachMessage({
      topic,
      partition: 0,
      message,
      heartbeat,
      pause: () => () => undefined,
    });
  }
}

class FakeAdmin {
  connected = false;
  disconnectCalls = 0;

  constructor(
    private readonly topics: string[],
    private readonly connectGate?: Promise<void>,
  ) {}
  async connect(): Promise<void> {
    await this.connectGate;
    this.connected = true;
  }
  async disconnect(): Promise<void> {
    this.disconnectCalls += 1;
    this.connected = false;
  }
  async listTopics(): Promise<string[]> {
    return this.topics;
  }
}

function fakeKafka(
  listedTopics: string[],
  options: {
    adminConnectGate?: Promise<void>;
    consumerConnectGate?: Promise<void>;
    consumerConnectGates?: Promise<void>[];
  } = {},
): { kafka: Kafka; consumers: FakeConsumer[]; admins: FakeAdmin[] } {
  const consumers: FakeConsumer[] = [];
  const admins: FakeAdmin[] = [];
  const kafka = {
    admin: () => {
      const admin = new FakeAdmin(listedTopics, options.adminConnectGate);
      admins.push(admin);
      return admin;
    },
    consumer: ({
      groupId,
      retry,
    }: {
      groupId: string;
      retry?: { restartOnFailure?: () => Promise<boolean> };
    }) => {
      const consumer = new FakeConsumer(
        groupId,
        options.consumerConnectGates?.[consumers.length] ?? options.consumerConnectGate,
        retry?.restartOnFailure,
      );
      consumers.push(consumer);
      return consumer;
    },
  } as unknown as Kafka;
  return { kafka, consumers, admins };
}

async function waitFor(cond: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error('timeout waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

const noopHandler = async (): Promise<void> => {};

describe('KafkaSessionEventSource topic discovery', () => {
  it.each([
    { encoding: 'avro', topicPattern: undefined, suffix: '-avro' },
    { encoding: 'avro', topicPattern: /.*/g, suffix: '-avro' },
    { encoding: 'raw', topicPattern: undefined, suffix: '' },
    { encoding: 'raw', topicPattern: /.*/g, suffix: '' },
    { encoding: undefined, topicPattern: undefined, suffix: '' },
    { encoding: undefined, topicPattern: /.*/g, suffix: '' },
  ] as const)(
    'discovers only $encoding topics with pattern $topicPattern and deduplicates KoP aliases',
    async ({ encoding, topicPattern, suffix }) => {
      vi.useFakeTimers();
      const { kafka, consumers } = fakeKafka([
        'orca.ws_a.sessions.ses_a.events',
        'public.default.orca.ws_a.sessions.ses_a.events',
        'orca.ws_a.sessions.ses_a.events-avro',
        'public.default.orca.ws_a.sessions.ses_a.events-avro',
        'foreign.namespace.orca.ws_a.sessions.ses_a.events-avro',
        'orca.ws_a.sessions.ses_a.events-avro.extra',
        'unrelated.topic',
      ]);
      const codec = {
        ...(encoding === undefined ? {} : { encoding }),
        prepareWriter: vi.fn(),
        encode: vi.fn(),
        decode: vi.fn(),
        close: vi.fn(),
      };
      const source = new KafkaSessionEventSource({
        kafka,
        codec,
        groupId: 'format-discovery',
        topicPrefix: 'public.default.',
        ...(topicPattern === undefined ? {} : { topicPattern }),
        topicDiscoveryIntervalMs: 100,
      });
      try {
        await source.start(noopHandler);
        await vi.advanceTimersByTimeAsync(300);
        expect(consumers).toHaveLength(1);
        expect(consumers[0]!.subscribeCalls).toEqual([
          { topic: `public.default.orca.ws_a.sessions.ses_a.events${suffix}`, fromBeginning: true },
        ]);
        expect(codec.prepareWriter).not.toHaveBeenCalled();
      } finally {
        await source.stop();
        vi.useRealTimers();
      }
      expect(codec.close).not.toHaveBeenCalled();
    },
  );

  it('dispatches only the selected Avro route, accepting KoP aliases but rejecting stale raw delivery', async () => {
    const topic = 'public.default.orca.ws_a.sessions.ses_a.events-avro';
    const { kafka, consumers } = fakeKafka([topic]);
    const message = kafkaMessage('ws_a', 'ses_a');
    const decoded = kafkaMessageToEventForRoute(message, {
      workspaceId: 'ws_a',
      sessionId: 'ses_a',
    });
    const codec = {
      encoding: 'avro' as const,
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      decode: vi.fn().mockResolvedValue(decoded),
      close: vi.fn(),
    };
    const handler = vi.fn().mockResolvedValue(undefined);
    const source = new KafkaSessionEventSource({
      kafka,
      codec,
      groupId: 'avro-dispatch',
      topicPrefix: 'public.default.',
    });
    try {
      await source.start(handler);
      await waitFor(() => consumers[0]?.subscribeCalls.length === 1);
      for (const staleTopic of [
        'orca.ws_a.sessions.ses_a.events',
        'public.default.orca.ws_a.sessions.ses_a.events',
        'orca.ws_b.sessions.ses_a.events-avro',
        'orca.ws_a.sessions.ses_b.events-avro',
        'foreign.namespace.orca.ws_a.sessions.ses_a.events-avro',
      ]) {
        await consumers[0]!.emit(staleTopic, message);
      }
      expect(codec.decode).not.toHaveBeenCalled();
      expect(handler).not.toHaveBeenCalled();
      for (const actualTopic of [topic, 'orca.ws_a.sessions.ses_a.events-avro']) {
        await consumers[0]!.emit(actualTopic, message);
      }
      expect(codec.decode).toHaveBeenCalledTimes(2);
      expect(codec.decode).toHaveBeenCalledWith(
        message,
        { workspaceId: 'ws_a', sessionId: 'ses_a', canonicalTopic: topic },
        expect.any(AbortSignal),
      );
      expect(handler).toHaveBeenCalledTimes(2);
      expect(handler).toHaveBeenCalledWith(decoded);
    } finally {
      await source.stop();
    }
  });

  it('backs off sustained decode failures exponentially to 30 seconds without acknowledging them', async () => {
    vi.useFakeTimers();
    const topic = 'orca.ws_a.sessions.ses_a.events';
    const { kafka, consumers } = fakeKafka([topic]);
    const codec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn().mockRejectedValue(new KafkaTranscriptCodecError('schema', false)),
    };
    const handler = vi.fn().mockResolvedValue(undefined);
    const source = new KafkaSessionEventSource({
      kafka,
      codec,
      groupId: 'decode-backoff',
      topicDiscoveryIntervalMs: 100,
    });
    try {
      await source.start(handler);
      await vi.advanceTimersByTimeAsync(0);
      for (const cooldown of [1000, 2000, 4000, 8000, 16000, 30000, 30000]) {
        const count = consumers.length;
        const consumer = consumers[count - 1]!;
        consumer.rejoin();
        await expect(consumer.emit(topic, kafkaMessage('ws_a', 'ses_a'))).rejects.toMatchObject({
          code: 'schema',
          retryable: false,
        });
        // FakeConsumer exposes callback rejection separately from KafkaJS's subsequent CRASH.
        consumer.crash(new Error('decode failed'), false);
        await vi.advanceTimersByTimeAsync(cooldown - 1);
        expect(consumers).toHaveLength(count);
        expect(codec.decode).toHaveBeenCalledTimes(count);
        expect(handler).not.toHaveBeenCalled();
        expect(source.status()).toEqual({ ready: false, state: 'running' });
        await vi.advanceTimersByTimeAsync(1);
        expect(consumers).toHaveLength(count + 1);
        expect(consumers[count]?.subscribeCalls).toEqual([{ topic, fromBeginning: true }]);
        expect(source.status().ready).toBe(false);
      }
    } finally {
      await source.stop();
      vi.useRealTimers();
    }
  });

  it('preserves safe codec error classification without copying message or cause', async () => {
    const topic = 'orca.ws_a.sessions.ses_a.events';
    const { kafka, consumers } = fakeKafka([topic]);
    const failure = Object.assign(new KafkaTranscriptCodecError('registry_unavailable', true), {
      message: 'secret',
      cause: new Error('secret'),
    });
    const codec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn().mockRejectedValue(failure),
    };
    const source = new KafkaSessionEventSource({ kafka, codec, groupId: 'classification' });
    try {
      await source.start(noopHandler);
      await waitFor(() => consumers[0]?.subscribeCalls.length === 1);
      const error = await consumers[0]!
        .emit(topic, kafkaMessage('ws_a', 'ses_a'))
        .catch((error: unknown) => error);
      expect(error).toMatchObject({
        code: 'registry_unavailable',
        retryable: true,
        message: 'Kafka transcript codec: registry_unavailable',
      });
      expect((error as Error).cause).toBeUndefined();
    } finally {
      await source.stop();
    }
  });

  it.each(['slow', 'lost membership'] as const)(
    'guards decode with heartbeats: %s',
    async (scenario) => {
      vi.useFakeTimers();
      const topic = 'orca.ws_a.sessions.ses_a.events';
      const { kafka, consumers } = fakeKafka([topic]);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      let waiterSignal: AbortSignal | undefined;
      const decoded = kafkaMessageToEventForRoute(kafkaMessage('ws_a', 'ses_a'), {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
      });
      const codec = {
        prepareWriter: vi.fn(),
        encode: vi.fn(),
        close: vi.fn(),
        decode: vi.fn(async (_message: KafkaMessage, _route: unknown, signal?: AbortSignal) => {
          waiterSignal = signal;
          await gate;
          return decoded;
        }),
      };
      const heartbeat = vi.fn<() => Promise<undefined>>().mockResolvedValue(undefined);
      if (scenario === 'lost membership')
        heartbeat
          .mockResolvedValueOnce(undefined)
          .mockRejectedValueOnce(new Error('secret broker response'));
      const handler = vi.fn().mockResolvedValue(undefined);
      const source = new KafkaSessionEventSource({
        kafka,
        codec,
        groupId: 'heartbeat',
        topicDiscoveryIntervalMs: 1000,
      });
      let delivery: Promise<unknown> | undefined;
      try {
        await source.start(handler);
        await vi.advanceTimersByTimeAsync(0);
        delivery = consumers[0]!
          .emit(topic, kafkaMessage('ws_a', 'ses_a'), heartbeat)
          .catch((error: unknown) => error);
        await vi.advanceTimersByTimeAsync(scenario === 'slow' ? 12000 : 3000);
        if (scenario === 'slow') {
          expect(heartbeat.mock.calls.length).toBeGreaterThanOrEqual(5);
          expect(handler).not.toHaveBeenCalled();
          release();
          await delivery;
          expect(heartbeat.mock.calls.length).toBeGreaterThanOrEqual(6);
          expect(handler).toHaveBeenCalledWith(decoded);
        } else {
          expect(waiterSignal?.aborted).toBe(true);
          expect(await delivery).toEqual(
            expect.objectContaining({ message: 'Kafka transcript heartbeat failed' }),
          );
          expect(handler).not.toHaveBeenCalled();
          expect(source.status().ready).toBe(false);
        }
      } finally {
        release();
        await delivery;
        await source.stop();
        vi.useRealTimers();
      }
    },
  );

  it.each(['decode', 'handler'] as const)(
    'rejects old ownership when a rebalance occurs during %s',
    async (stage) => {
      const topic = 'orca.ws_a.sessions.ses_a.events';
      const { kafka, consumers } = fakeKafka([topic]);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const decoded = kafkaMessageToEventForRoute(kafkaMessage('ws_a', 'ses_a'), {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
      });
      const codec = {
        prepareWriter: vi.fn(),
        encode: vi.fn(),
        close: vi.fn(),
        decode: vi.fn(async () => {
          if (stage === 'decode') await gate;
          return decoded;
        }),
      };
      const handler = vi.fn(async () => {
        if (stage === 'handler') await gate;
      });
      const source = new KafkaSessionEventSource({
        kafka,
        codec,
        groupId: 'codec',
        topicDiscoveryIntervalMs: 10,
      });
      try {
        await source.start(handler);
        await waitFor(() => consumers[0]?.subscribeCalls.length === 1);
        const delivery = consumers[0]!.emit(topic, kafkaMessage('ws_a', 'ses_a'));
        await waitFor(() => (stage === 'decode' ? codec.decode : handler).mock.calls.length === 1);
        consumers[0]!.rejoin();
        release();
        await expect(delivery).rejects.toThrow();
        if (stage === 'decode') expect(handler).not.toHaveBeenCalled();
      } finally {
        release();
        await source.stop();
      }
    },
  );

  it('fences a replaced consumer after async decode and does not clear the new owner failure', async () => {
    const topic = 'orca.ws_a.sessions.ses_a.events';
    const { kafka, consumers } = fakeKafka([topic]);
    const decoded = kafkaMessageToEventForRoute(kafkaMessage('ws_a', 'ses_a'), {
      workspaceId: 'ws_a',
      sessionId: 'ses_a',
    });
    let resolveDecode!: (event: typeof decoded) => void;
    const pending = new Promise<typeof decoded>((resolve) => {
      resolveDecode = resolve;
    });
    const codec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi
        .fn()
        .mockReturnValueOnce(pending)
        .mockRejectedValueOnce(new Error('schema failure')),
    };
    const handler = vi.fn().mockResolvedValue(undefined);
    const source = new KafkaSessionEventSource({
      kafka,
      codec,
      groupId: 'codec',
      topicDiscoveryIntervalMs: 10,
    });
    try {
      await source.start(handler);
      await waitFor(() => consumers[0]?.subscribeCalls.length === 1);
      const oldDelivery = consumers[0]!.emit(topic, kafkaMessage('ws_a', 'ses_a'));
      const oldRejected = expect(oldDelivery).rejects.toThrow();
      await waitFor(() => codec.decode.mock.calls.length === 1);
      consumers[0]!.crash(new Error('lost ownership'), false);
      await waitFor(() => consumers[1]?.subscribeCalls.length === 1);
      await expect(consumers[1]!.emit(topic, kafkaMessage('ws_a', 'ses_a'))).rejects.toThrow();
      resolveDecode(decoded);
      await oldRejected;
      expect(handler).not.toHaveBeenCalled();
      expect(source.status().ready).toBe(false);
    } finally {
      resolveDecode(decoded);
      await source.stop();
    }
  });

  it('blocks acknowledgement and readiness on decode failure until successful rediscovery delivery', async () => {
    vi.useFakeTimers();
    const topic = 'orca.ws_a.sessions.ses_a.events';
    const { kafka, consumers } = fakeKafka([topic]);
    const decoded = kafkaMessageToEventForRoute(kafkaMessage('ws_a', 'ses_a'), {
      workspaceId: 'ws_a',
      sessionId: 'ses_a',
    });
    const codec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi
        .fn()
        .mockRejectedValueOnce(new Error('secret response body'))
        .mockResolvedValue(decoded),
    };
    const handler = vi.fn().mockResolvedValue(undefined);
    const source = new KafkaSessionEventSource({
      kafka,
      codec,
      groupId: 'codec',
      topicDiscoveryIntervalMs: 100,
    });
    try {
      await source.start(handler);
      await vi.advanceTimersByTimeAsync(50);
      await expect(consumers[0]!.emit(topic, kafkaMessage('ws_a', 'ses_a'))).rejects.toThrow(
        'Kafka transcript decode failed',
      );
      expect(handler).not.toHaveBeenCalled();
      expect(source.status().ready).toBe(false);
      consumers[0]!.crash(new Error('decode failed'), false);
      await vi.advanceTimersByTimeAsync(999);
      expect(consumers).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      // The minimum cooldown expires between scans; no dedicated retry timer starts work.
      expect(consumers).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(50);
      expect(consumers).toHaveLength(2);
      expect(source.status().ready).toBe(false);
      await consumers[1]!.emit(topic, kafkaMessage('ws_a', 'ses_a'));
      expect(handler).toHaveBeenCalledWith(decoded);
      expect(source.status().ready).toBe(true);

      codec.decode.mockRejectedValueOnce(new Error('schema fails again'));
      await expect(consumers[1]!.emit(topic, kafkaMessage('ws_a', 'ses_a'))).rejects.toThrow(
        'Kafka transcript decode failed',
      );
      consumers[1]!.crash(new Error('decode failed again'), false);
      expect(source.status().ready).toBe(false);
      await vi.advanceTimersByTimeAsync(999);
      expect(consumers).toHaveLength(2);
      await vi.advanceTimersByTimeAsync(1);
      expect(consumers).toHaveLength(3);
      await consumers[2]!.emit(topic, kafkaMessage('ws_a', 'ses_a'));
      expect(handler).toHaveBeenCalledTimes(2);
      expect(source.status().ready).toBe(true);
      expect(codec.prepareWriter).not.toHaveBeenCalled();
    } finally {
      await source.stop();
      vi.useRealTimers();
    }
    expect(codec.close).not.toHaveBeenCalled();
  });

  it('keeps new topics consumable during cooldown and stops without pending retry work', async () => {
    vi.useFakeTimers();
    const topic = 'orca.ws_a.sessions.ses_a.events';
    const otherTopic = 'orca.ws_b.sessions.ses_b.events';
    const topics = [topic];
    const { kafka, consumers, admins } = fakeKafka(topics);
    const decoded = kafkaMessageToEventForRoute(kafkaMessage('ws_b', 'ses_b'), {
      workspaceId: 'ws_b',
      sessionId: 'ses_b',
    });
    const codec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn().mockRejectedValueOnce(new Error('bad schema')).mockResolvedValue(decoded),
    };
    const handler = vi.fn().mockResolvedValue(undefined);
    const source = new KafkaSessionEventSource({
      kafka,
      codec,
      groupId: 'isolated-backoff',
      topicDiscoveryIntervalMs: 100,
    });
    try {
      await source.start(handler);
      await vi.advanceTimersByTimeAsync(0);
      await expect(consumers[0]!.emit(topic, kafkaMessage('ws_a', 'ses_a'))).rejects.toThrow();
      consumers[0]!.crash(new Error('decode failed'), false);
      topics.push(otherTopic);
      await vi.advanceTimersByTimeAsync(100);
      expect(consumers).toHaveLength(2);
      expect(consumers[1]?.subscribeCalls).toEqual([{ topic: otherTopic, fromBeginning: true }]);
      await consumers[1]!.emit(otherTopic, kafkaMessage('ws_b', 'ses_b'));
      expect(handler).toHaveBeenCalledOnce();
      expect(handler).toHaveBeenCalledWith(decoded);
      expect(source.status()).toEqual({ ready: false, state: 'running' });

      await source.stop();
      expect(source.status()).toEqual({ ready: false, state: 'stopped' });
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(60_000);
      expect(consumers).toHaveLength(2);
      expect(codec.decode).toHaveBeenCalledTimes(2);
      expect(consumers.every((consumer) => !consumer.connected)).toBe(true);
      expect(admins.every((admin) => !admin.connected)).toBe(true);
      expect(codec.prepareWriter).not.toHaveBeenCalled();
      expect(codec.close).not.toHaveBeenCalled();
    } finally {
      await source.stop();
      vi.useRealTimers();
    }
  });

  it('retries heartbeat failures on the next discovery scan without accumulating codec cooldown', async () => {
    vi.useFakeTimers();
    const topic = 'orca.ws_a.sessions.ses_a.events';
    const { kafka, consumers } = fakeKafka([topic]);
    const codec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn().mockRejectedValue(new KafkaTranscriptCodecError('schema', false)),
    };
    const heartbeat = vi.fn().mockRejectedValue(new Error('secret member-loss response'));
    const handler = vi.fn().mockResolvedValue(undefined);
    const source = new KafkaSessionEventSource({
      kafka,
      codec,
      groupId: 'heartbeat-no-backoff',
      topicDiscoveryIntervalMs: 100,
    });
    try {
      await source.start(handler);
      await vi.advanceTimersByTimeAsync(0);
      for (let index = 0; index < 3; index += 1) {
        const failure = await consumers[index]!.emit(
          topic,
          kafkaMessage('ws_a', 'ses_a'),
          heartbeat,
        ).catch((error: unknown) => error);
        expect(failure).toBeInstanceOf(KafkaTranscriptHeartbeatError);
        expect(failure).toMatchObject({ message: 'Kafka transcript heartbeat failed' });
        expect((failure as Error).cause).toBeUndefined();
        consumers[index]!.crash(failure as Error, false);
        await vi.advanceTimersByTimeAsync(100);
        expect(consumers).toHaveLength(index + 2);
        expect(source.status().ready).toBe(false);
      }
      expect(codec.decode).not.toHaveBeenCalled();
      await expect(consumers[3]!.emit(topic, kafkaMessage('ws_a', 'ses_a'))).rejects.toThrow();
      consumers[3]!.crash(new Error('decode failed'), false);
      await vi.advanceTimersByTimeAsync(999);
      expect(consumers).toHaveLength(4);
      await vi.advanceTimersByTimeAsync(1);
      expect(consumers).toHaveLength(5);
      expect(handler).not.toHaveBeenCalled();

      // Membership loss after a codec failure must retain its retry history.
      await expect(
        consumers[4]!.emit(topic, kafkaMessage('ws_a', 'ses_a'), heartbeat),
      ).rejects.toBeInstanceOf(KafkaTranscriptHeartbeatError);
      consumers[4]!.crash(new Error('heartbeat failed'), false);
      await vi.advanceTimersByTimeAsync(100);
      expect(consumers).toHaveLength(6);
      expect(source.status().ready).toBe(false);
      await expect(consumers[5]!.emit(topic, kafkaMessage('ws_a', 'ses_a'))).rejects.toThrow();
      consumers[5]!.crash(new Error('decode failed again'), false);
      await vi.advanceTimersByTimeAsync(1999);
      expect(consumers).toHaveLength(6);
      await vi.advanceTimersByTimeAsync(1);
      expect(consumers).toHaveLength(7);
      expect(source.status().ready).toBe(false);
      expect(handler).not.toHaveBeenCalled();
    } finally {
      await source.stop();
      vi.useRealTimers();
    }
  });

  it('reports stopped, ready-running, and stopped source states', async () => {
    const { kafka } = fakeKafka([]);
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-status',
      topicDiscoveryIntervalMs: 10,
    });
    expect(source.status()).toEqual({ ready: false, state: 'stopped' });
    await source.start(noopHandler);
    await waitFor(() => source.status().ready);
    expect(source.status()).toEqual({ ready: true, state: 'running' });
    await source.stop();
    expect(source.status()).toEqual({ ready: false, state: 'stopped' });
  });

  it('stops and disconnects when its external signal aborts', async () => {
    const controller = new AbortController();
    const { kafka, consumers } = fakeKafka(['orca.ws_a.sessions.s_1.events']);
    const handler = vi.fn(async () => undefined);
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-external-abort',
      topicDiscoveryIntervalMs: 10,
      signal: controller.signal,
    });

    await source.start(handler);
    await waitFor(() => consumers[0]?.connected === true);
    controller.abort();
    await waitFor(() => source.status().state === 'stopped');

    expect(consumers[0]?.connected).toBe(false);
    expect(consumers[0]?.disconnectCalls).toBeGreaterThan(0);
    await expect(
      consumers[0]!.emit('orca.ws_a.sessions.s_1.events', kafkaMessage('ws_a', 's_1')),
    ).rejects.toThrow('stopped before event acknowledgement');
    expect(handler).not.toHaveBeenCalled();
  });

  it('does not start when its external signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const { kafka, consumers, admins } = fakeKafka(['orca.ws_a.sessions.s_1.events']);
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-pre-aborted',
      topicDiscoveryIntervalMs: 10,
      signal: controller.signal,
    });

    await source.start(noopHandler);

    expect(source.status()).toEqual({ ready: false, state: 'stopped' });
    expect(admins).toEqual([]);
    expect(consumers).toEqual([]);
  });

  it('disconnects a consumer whose connect settles after stop begins', async () => {
    let releaseConnect!: () => void;
    const connectGate = new Promise<void>((resolve) => {
      releaseConnect = resolve;
    });
    const { kafka, consumers } = fakeKafka(['orca.ws_a.sessions.s_1.events'], {
      consumerConnectGate: connectGate,
    });
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-late-consumer',
      topicDiscoveryIntervalMs: 10,
    });

    await source.start(noopHandler);
    await waitFor(() => consumers.length === 1);
    const stopping = source.stop();
    releaseConnect();
    await stopping;

    expect(consumers[0]?.connected).toBe(false);
    expect(consumers[0]?.disconnectCalls).toBeGreaterThan(0);
  });

  it('drains every consumer startup on stop after another startup fails', async () => {
    let rejectConnect!: (error: Error) => void;
    const failingConnect = new Promise<void>((_, reject) => {
      rejectConnect = reject;
    });
    let releaseConnect!: () => void;
    const pendingConnect = new Promise<void>((resolve) => {
      releaseConnect = resolve;
    });
    const { kafka, consumers, admins } = fakeKafka(
      ['orca.ws_a.sessions.s_1.events', 'orca.ws_a.sessions.s_2.events'],
      { consumerConnectGates: [failingConnect, pendingConnect] },
    );
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-partial-startup-failure',
      topicDiscoveryIntervalMs: 60_000,
    });
    const logError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const stopped = vi.fn();

    try {
      await source.start(noopHandler);
      await waitFor(() => consumers.length === 2);
      const run = vi.spyOn(consumers[1]!, 'run');
      rejectConnect(new Error('consumer connect failed'));
      // Flush the rejected startup and discovery continuations without a timed sleep.
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(consumers[0]?.disconnectCalls).toBe(1);

      const stopping = source.stop().then(stopped);
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(consumers[1]?.disconnectCalls).toBe(1);
      expect(stopped).not.toHaveBeenCalled();

      releaseConnect();
      await stopping;

      expect(stopped).toHaveBeenCalledOnce();
      expect(consumers[1]?.disconnectCalls).toBe(2);
      expect(consumers.every((consumer) => !consumer.connected)).toBe(true);
      expect(admins.every((admin) => !admin.connected)).toBe(true);
      expect(consumers[1]?.subscribeCalls).toEqual([]);
      expect(run).not.toHaveBeenCalled();
      expect(source.status()).toEqual({ ready: false, state: 'stopped' });
    } finally {
      rejectConnect(new Error('test cleanup'));
      releaseConnect();
      await source.stop();
      logError.mockRestore();
    }
  });

  it('disconnects an admin whose connect settles after stop begins', async () => {
    let releaseConnect!: () => void;
    const connectGate = new Promise<void>((resolve) => {
      releaseConnect = resolve;
    });
    const { kafka, admins } = fakeKafka([], { adminConnectGate: connectGate });
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-late-admin',
      topicDiscoveryIntervalMs: 10,
    });

    await source.start(noopHandler);
    await waitFor(() => admins.length === 1);
    const stopping = source.stop();
    releaseConnect();
    await stopping;

    expect(admins[0]?.connected).toBe(false);
    expect(admins[0]?.disconnectCalls).toBeGreaterThan(0);
  });

  it('removes and recreates a topic consumer after a terminal crash', async () => {
    const { kafka, consumers } = fakeKafka(['orca.ws_a.sessions.s_1.events']);
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-terminal-crash',
      topicDiscoveryIntervalMs: 10,
    });

    try {
      await source.start(noopHandler);
      await waitFor(() => consumers[0]?.connected === true);
      consumers[0]!.crash(new Error('terminal'), false);
      await waitFor(() => consumers[1]?.connected === true);

      expect(consumers[0]?.connected).toBe(false);
      expect(consumers[0]?.disconnectCalls).toBeGreaterThan(0);
      expect(consumers[1]?.groupId).toBe(consumers[0]?.groupId);
    } finally {
      await source.stop();
    }
  });

  it('keeps topic discovery as the sole consumer restart owner', async () => {
    const { kafka, consumers } = fakeKafka(['orca.ws_a.sessions.s_1.events']);
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-restart-fence',
      topicDiscoveryIntervalMs: 10,
    });

    await source.start(noopHandler);
    await waitFor(() => consumers[0]?.connected === true);
    await expect(consumers[0]?.restartOnFailure?.()).resolves.toBe(false);
    await source.stop();
  });

  it('drops messages whose route headers disagree with the actual session topic', async () => {
    const { kafka, consumers } = fakeKafka(['orca.ws_a.sessions.s_1.events']);
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-src',
      topicDiscoveryIntervalMs: 10,
    });
    const handler = vi.fn(async () => undefined);
    try {
      await source.start(handler);
      await waitFor(() => consumers.length >= 1);

      const message = kafkaMessage('ws_forged', 's_1');
      await consumers[0]!.emit('orca.ws_a.sessions.s_1.events', message);
      expect(handler).not.toHaveBeenCalled();

      await consumers[0]!.emit(
        'orca.ws_other.sessions.s_1.events',
        kafkaMessage('ws_other', 's_1'),
      );
      expect(handler).not.toHaveBeenCalled();

      await consumers[0]!.emit('orca.ws_a.sessions.s_1.events', kafkaMessage('ws_a', 's_1'));
      expect(handler).toHaveBeenCalledOnce();
    } finally {
      await source.stop();
    }
  });

  it('subscribes to bare session topics when no prefix is configured', async () => {
    const { kafka, consumers } = fakeKafka(['orca.ws_a.sessions.s_1.events', 'other.topic']);
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-src',
      topicDiscoveryIntervalMs: 10,
    });
    try {
      await source.start(noopHandler);
      await waitFor(() => consumers.length >= 1);
      expect(consumers[0]!.subscribeCalls).toEqual([
        { topic: 'orca.ws_a.sessions.s_1.events', fromBeginning: true },
      ]);
    } finally {
      await source.stop();
    }
  });

  it('subscribes to the canonical prefixed name when KoP lists the BARE name', async () => {
    // Kafka-on-Pulsar (KoP) strips `public.default.` from listTopics() output for
    // the default tenant/namespace; fetches still require the prefixed name.
    const { kafka, consumers } = fakeKafka(['orca.ws_a.sessions.s_1.events', 'other.topic']);
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-src',
      topicPrefix: 'public.default.',
      topicDiscoveryIntervalMs: 10,
    });
    try {
      await source.start(noopHandler);
      await waitFor(() => consumers.length >= 1);
      expect(consumers[0]!.subscribeCalls).toEqual([
        { topic: 'public.default.orca.ws_a.sessions.s_1.events', fromBeginning: true },
      ]);
      // Subsequent discovery ticks must de-dup on the canonical topic and not
      // spin up extra consumers for the same session topic.
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(consumers.length).toBe(1);
    } finally {
      await source.stop();
    }
  });

  it('de-dups bare and fully prefixed listings of the same topic', async () => {
    const { kafka, consumers } = fakeKafka([
      'orca.ws_a.sessions.s_1.events',
      'public.default.orca.ws_a.sessions.s_1.events',
    ]);
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-src',
      topicPrefix: 'public.default.',
      topicDiscoveryIntervalMs: 10,
    });
    try {
      await source.start(noopHandler);
      await waitFor(() => consumers.length >= 1);
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(consumers.length).toBe(1);
      expect(consumers[0]!.subscribeCalls).toEqual([
        { topic: 'public.default.orca.ws_a.sessions.s_1.events', fromBeginning: true },
      ]);
    } finally {
      await source.stop();
    }
  });

  it('applies a custom topicPattern only within valid prefix-stripped session topics', async () => {
    const { kafka, consumers } = fakeKafka([
      'custom.ws_a.topic',
      'orca.ws_a.sessions.s_1.events',
      'public.default.orca.ws_b.sessions.s_2.events',
      'orca.ws_b.sessions.s_2.events-avro',
    ]);
    const source = new KafkaSessionEventSource({
      kafka,
      groupId: 'test-src',
      topicPrefix: 'public.default.',
      topicPattern: /^(?:custom\.|orca\.ws_b\.)/g,
      topicDiscoveryIntervalMs: 10,
    });
    try {
      await source.start(noopHandler);
      await waitFor(() => consumers.length >= 1);
      expect(consumers[0]!.subscribeCalls).toEqual([
        { topic: 'public.default.orca.ws_b.sessions.s_2.events', fromBeginning: true },
      ]);
      expect(consumers).toHaveLength(1);
    } finally {
      await source.stop();
    }
  });

  it('rejects an invalid topic prefix at construction', () => {
    const { kafka } = fakeKafka([]);
    expect(
      () =>
        new KafkaSessionEventSource({
          kafka,
          groupId: 'test-src',
          topicPrefix: 'public.default',
        }),
    ).toThrow(/invalid topic prefix/);
  });
});

function kafkaMessage(workspaceId: string, sessionId: string): KafkaMessage {
  return {
    key: Buffer.from('evt_route'),
    value: Buffer.from('payload'),
    timestamp: '0',
    attributes: 0,
    offset: '0',
    size: 7,
    headers: {
      id: Buffer.from('evt_route'),
      workspace_id: Buffer.from(workspaceId),
      session_id: Buffer.from(sessionId),
      produced_by: Buffer.from('client'),
      kind: Buffer.from('user.message'),
    },
  };
}
