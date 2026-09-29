// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type { EachMessagePayload, Kafka, KafkaMessage } from 'kafkajs';
import { consumeSession } from '../../src/kafka/consumer.js';
import { KafkaTranscriptStore } from '../../src/kafka-store.js';
import { kafkaMessageToEventForRoute } from '../../src/kafka/serialize.js';
import * as codecModule from '../../src/kafka/codec.js';
import { KafkaTranscriptCodecError } from '../../src/kafka/codec-error.js';

class FakeConsumer {
  readonly events = {
    GROUP_JOIN: 'consumer.group_join',
    CRASH: 'consumer.crash',
  } as const;
  subscribeCalls: Array<{ topic: string; fromBeginning?: boolean }> = [];
  seekCalls: Array<{ topic: string; partition: number; offset: string }> = [];
  private eachMessage?: (payload: EachMessagePayload) => Promise<void>;
  private activeEachMessage: Promise<void> | null = null;
  private readonly listeners = new Map<string, Set<(event: unknown) => void>>();

  constructor(private readonly joinOnRun = true) {}

  async connect(): Promise<void> {}
  async disconnect(): Promise<void> {
    await this.activeEachMessage;
  }
  async subscribe(opts: { topic: string; fromBeginning?: boolean }): Promise<void> {
    this.subscribeCalls.push(opts);
  }
  async run(opts: { eachMessage: (payload: EachMessagePayload) => Promise<void> }): Promise<void> {
    this.eachMessage = opts.eachMessage;
    if (this.joinOnRun) this.joinGroup();
  }
  seek(opts: { topic: string; partition: number; offset: string }): void {
    this.seekCalls.push(opts);
  }

  on(eventName: string, listener: (event: unknown) => void): () => void {
    const listeners = this.listeners.get(eventName) ?? new Set();
    listeners.add(listener);
    this.listeners.set(eventName, listeners);
    return () => listeners.delete(listener);
  }

  joinGroup(): void {
    for (const listener of this.listeners.get(this.events.GROUP_JOIN) ?? []) {
      listener({ payload: {} });
    }
  }

  crash(error: Error, restart: boolean): void {
    for (const listener of this.listeners.get(this.events.CRASH) ?? []) {
      listener({ payload: { error, restart } });
    }
  }

  async emit(
    topic: string,
    message: KafkaMessage,
    heartbeat = async () => undefined,
  ): Promise<void> {
    if (!this.eachMessage) throw new Error('consumer is not running');
    let finishActive!: () => void;
    const active = new Promise<void>((resolve) => {
      finishActive = resolve;
    });
    this.activeEachMessage = active;
    try {
      await this.eachMessage({
        topic,
        partition: 0,
        message,
        heartbeat,
        pause: () => () => undefined,
      });
    } finally {
      finishActive();
      if (this.activeEachMessage === active) this.activeEachMessage = null;
    }
  }

  get running(): boolean {
    return this.eachMessage !== undefined;
  }
}

class FakeAdmin {
  fetchTopicOffsetsCalls: string[] = [];

  constructor(private readonly highWatermark: string | Error) {}

  async connect(): Promise<void> {}
  async disconnect(): Promise<void> {}
  async fetchTopicOffsets(topic: string): Promise<Array<{ high: string }>> {
    this.fetchTopicOffsetsCalls.push(topic);
    if (this.highWatermark instanceof Error) throw this.highWatermark;
    return [{ high: this.highWatermark }];
  }
}

function fakeKafka(
  highWatermark: string | Error,
  joinOnRun = true,
): {
  kafka: Kafka;
  admin: FakeAdmin;
  consumer: FakeConsumer;
} {
  const admin = new FakeAdmin(highWatermark);
  const consumer = new FakeConsumer(joinOnRun);
  const kafka = {
    admin: () => admin,
    consumer: () => consumer,
  } as unknown as Kafka;
  return { kafka, admin, consumer };
}

async function waitFor(predicate: () => boolean, timeoutMs = 1000): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error('condition not met');
}

describe('consumeSession', () => {
  it.each(['read', 'tail'] as const)(
    'store %s uses the avro topic set and skips stale raw routes without decoding',
    async (operation) => {
      const { kafka, admin, consumer } = fakeKafka('2');
      Object.assign(kafka, {
        producer: () => ({ connect: vi.fn(), disconnect: vi.fn(), send: vi.fn() }),
      });
      const codec = {
        encoding: 'avro' as const,
        prepareWriter: vi.fn(),
        encode: vi.fn(),
        decode: vi.fn<codecModule.KafkaTranscriptCodec['decode']>(async (message, route) =>
          kafkaMessageToEventForRoute(message, route),
        ),
        close: vi.fn(),
      };
      const store = new KafkaTranscriptStore({ kafka, codec, topicPrefix: 'public.default.' });
      const controller = new AbortController();
      const received: string[] = [];
      const reading = (async () => {
        for await (const event of store[operation]('ws_a', 'ses_a', {
          fromCursor: '',
          subpath: '*',
          maxEvents: 0,
          signal: controller.signal,
        })) {
          received.push(event.id);
          if (operation === 'tail') break;
        }
      })();
      try {
        await waitFor(() => consumer.running);
        const topic = 'public.default.orca.ws_a.sessions.ses_a.events-avro';
        expect(admin.fetchTopicOffsetsCalls).toEqual([topic]);
        expect(consumer.subscribeCalls).toEqual([{ topic, fromBeginning: operation === 'read' }]);
        if (operation === 'tail') {
          expect(consumer.seekCalls).toEqual([{ topic, partition: 0, offset: '2' }]);
        }
        await consumer.emit(
          'public.default.orca.ws_a.sessions.ses_a.events',
          kafkaMessage('ws_a', 'ses_a', { offset: operation === 'read' ? 0 : 2 }),
        );
        expect(codec.decode).not.toHaveBeenCalled();
        await consumer.emit(
          topic,
          kafkaMessage('ws_a', 'ses_a', { offset: operation === 'read' ? 1 : 3 }),
        );
        await reading;
        expect(received).toEqual(['evt_route']);
        expect(codec.decode).toHaveBeenCalledTimes(1);
        expect(codec.prepareWriter).not.toHaveBeenCalled();
      } finally {
        controller.abort();
        await reading;
        await store.close();
      }
    },
  );

  it('cleans up an in-flight heartbeat immediately when decode fails', async () => {
    vi.useFakeTimers();
    const { kafka, consumer } = fakeKafka('1');
    let fail!: (error: Error) => void;
    const pending = new Promise<null>((_resolve, reject) => {
      fail = reject;
    });
    const codec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn().mockReturnValue(pending),
    };
    const heartbeat = vi
      .fn<() => Promise<undefined>>()
      .mockResolvedValueOnce(undefined)
      .mockReturnValue(new Promise(() => {}));
    const onError = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '0',
        subpath: '*',
        mode: 'bounded',
        codec,
        signal: new AbortController().signal,
      },
      { onEvent: vi.fn(), onError, onEnd: vi.fn() },
    );
    let delivery: Promise<unknown> | undefined;
    try {
      await vi.advanceTimersByTimeAsync(0);
      delivery = consumer
        .emit('orca.ws_a.sessions.ses_a.events', kafkaMessage('ws_a', 'ses_a'), heartbeat)
        .catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(3000);
      expect(heartbeat).toHaveBeenCalledTimes(2);
      fail(new Error('schema failure'));
      await vi.advanceTimersByTimeAsync(0);
      expect(onError).toHaveBeenCalledTimes(1);
    } finally {
      fail(new Error('cleanup'));
      await vi.advanceTimersByTimeAsync(3000);
      await delivery;
      await handle.stop();
      vi.useRealTimers();
    }
  });

  it('preserves safe codec error classification without copying message or cause', async () => {
    const { kafka, consumer } = fakeKafka('1');
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
    const onError = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '0',
        subpath: '*',
        mode: 'bounded',
        codec,
        signal: new AbortController().signal,
      },
      { onEvent: vi.fn(), onError, onEnd: vi.fn() },
    );
    try {
      await waitFor(() => consumer.running);
      await expect(
        consumer.emit('orca.ws_a.sessions.ses_a.events', kafkaMessage('ws_a', 'ses_a')),
      ).rejects.toMatchObject({
        code: 'registry_unavailable',
        retryable: true,
        message: 'Kafka transcript codec: registry_unavailable',
      });
      expect(onError.mock.calls[0]?.[0].cause).toBeUndefined();
    } finally {
      await handle.stop();
    }
  });

  it('does not scan a decode result from a previous group generation', async () => {
    const { kafka, consumer } = fakeKafka('1');
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const codec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn(async () => {
        await gate;
        return null;
      }),
    };
    const onScannedCursor = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '0',
        subpath: '*',
        mode: 'bounded',
        codec,
        signal: new AbortController().signal,
        onScannedCursor,
      },
      { onEvent: vi.fn(), onError: vi.fn(), onEnd: vi.fn() },
    );
    try {
      await waitFor(() => consumer.running);
      const delivery = consumer.emit(
        'orca.ws_a.sessions.ses_a.events',
        kafkaMessage('ws_a', 'ses_a'),
      );
      await waitFor(() => codec.decode.mock.calls.length === 1);
      consumer.joinGroup();
      release();
      await expect(delivery).rejects.toThrow();
      expect(onScannedCursor).not.toHaveBeenCalled();
    } finally {
      release();
      await handle.stop();
    }
  });

  it.each(['slow', 'lost membership'] as const)(
    'guards decode with heartbeats: %s',
    async (scenario) => {
      vi.useFakeTimers();
      const { kafka, consumer } = fakeKafka('1');
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      let waiterSignal: AbortSignal | undefined;
      const codec = {
        prepareWriter: vi.fn(),
        encode: vi.fn(),
        close: vi.fn(),
        decode: vi.fn(async (_message: KafkaMessage, _route: unknown, signal?: AbortSignal) => {
          waiterSignal = signal;
          await gate;
          return null;
        }),
      };
      const heartbeat = vi.fn<() => Promise<undefined>>().mockResolvedValue(undefined);
      if (scenario === 'lost membership')
        heartbeat
          .mockResolvedValueOnce(undefined)
          .mockRejectedValueOnce(new Error('secret broker response'));
      const onScannedCursor = vi.fn();
      const onEvent = vi.fn();
      const onError = vi.fn();
      const handle = consumeSession(
        kafka,
        {
          workspaceId: 'ws_a',
          sessionId: 'ses_a',
          fromCursor: '0',
          subpath: '*',
          mode: 'bounded',
          codec,
          signal: new AbortController().signal,
          onScannedCursor,
        },
        { onEvent, onError, onEnd: vi.fn() },
      );
      let delivery: Promise<unknown> | undefined;
      try {
        await vi.advanceTimersByTimeAsync(0);
        delivery = consumer
          .emit('orca.ws_a.sessions.ses_a.events', kafkaMessage('ws_a', 'ses_a'), heartbeat)
          .catch((error: unknown) => error);
        await vi.advanceTimersByTimeAsync(scenario === 'slow' ? 12000 : 3000);
        if (scenario === 'slow') {
          expect(heartbeat.mock.calls.length).toBeGreaterThanOrEqual(5);
          expect(onScannedCursor).not.toHaveBeenCalled();
          release();
          await delivery;
          expect(heartbeat.mock.calls.length).toBeGreaterThanOrEqual(6);
          expect(onScannedCursor).toHaveBeenCalledWith('1');
        } else {
          expect(waiterSignal?.aborted).toBe(true);
          expect(await delivery).toEqual(
            expect.objectContaining({ message: 'Kafka transcript heartbeat failed' }),
          );
          expect(onScannedCursor).not.toHaveBeenCalled();
          expect(onEvent).not.toHaveBeenCalled();
          expect(onError).toHaveBeenCalledTimes(1);
        }
      } finally {
        release();
        await delivery;
        await handle.stop();
        vi.useRealTimers();
      }
    },
  );

  it.each(['bounded', 'unbounded'] as const)(
    'does not deliver or scan after cancellation during %s decode',
    async (mode) => {
      const { kafka, consumer } = fakeKafka('1');
      const ac = new AbortController();
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
          await gate;
          return decoded;
        }),
      };
      const onEvent = vi.fn();
      const onError = vi.fn();
      const onScannedCursor = vi.fn();
      const handle = consumeSession(
        kafka,
        {
          workspaceId: 'ws_a',
          sessionId: 'ses_a',
          fromCursor: '0',
          subpath: '*',
          mode,
          signal: ac.signal,
          codec,
          onScannedCursor,
        },
        { onEvent, onError, onEnd: vi.fn() },
      );
      try {
        await waitFor(() => consumer.running);
        const delivery = consumer.emit(
          'orca.ws_a.sessions.ses_a.events',
          kafkaMessage('ws_a', 'ses_a'),
        );
        await waitFor(() => codec.decode.mock.calls.length === 1);
        ac.abort();
        release();
        await delivery;
        expect(onEvent).not.toHaveBeenCalled();
        expect(onScannedCursor).not.toHaveBeenCalled();
        expect(onError).not.toHaveBeenCalled();
      } finally {
        release();
        await handle.stop();
      }
    },
  );

  it('charges the raw broker record size rather than the decoded payload', async () => {
    const { kafka, consumer } = fakeKafka('3');
    const decoded = kafkaMessageToEventForRoute(kafkaMessage('ws_a', 'ses_a'), {
      workspaceId: 'ws_a',
      sessionId: 'ses_a',
    })!;
    decoded.payload = Buffer.alloc(1000);
    const codec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi.fn().mockResolvedValue(decoded),
    };
    const onEvent = vi.fn();
    const onEnd = vi.fn();
    const onScannedCursor = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '0',
        subpath: '*',
        mode: 'bounded',
        signal: new AbortController().signal,
        codec,
        maxBytes: 14,
        onScannedCursor,
      },
      { onEvent, onEnd },
    );
    try {
      await waitFor(() => consumer.running);
      await consumer.emit('orca.ws_a.sessions.ses_a.events', kafkaMessage('ws_a', 'ses_a'));
      expect(onEnd).not.toHaveBeenCalled();
      await consumer.emit(
        'orca.ws_a.sessions.ses_a.events',
        kafkaMessage('ws_a', 'ses_a', { offset: 1 }),
      );
      expect(onEvent).toHaveBeenCalledTimes(2);
      expect(onScannedCursor.mock.calls).toEqual([['1'], ['2']]);
      expect(onEnd).toHaveBeenCalledTimes(1);
    } finally {
      await handle.stop();
    }
  });

  it.each(['UNKNOWN_TOPIC_OR_PARTITION', 'authorization failed'])(
    'closes its default codec after startup failure: %s',
    async (message) => {
      const { kafka } = fakeKafka(new Error(message));
      const codec = {
        prepareWriter: vi.fn(),
        encode: vi.fn(),
        decode: vi.fn(),
        close: vi.fn().mockResolvedValue(undefined),
      };
      const factory = vi.spyOn(codecModule, 'createKafkaTranscriptCodec').mockReturnValue(codec);
      const done = vi.fn();
      const handle = consumeSession(
        kafka,
        {
          workspaceId: 'ws_a',
          sessionId: 'ses_a',
          fromCursor: '',
          subpath: '*',
          mode: 'bounded',
          signal: new AbortController().signal,
        },
        { onEvent: vi.fn(), onError: done, onEnd: done },
      );
      try {
        await waitFor(() => done.mock.calls.length === 1);
        expect(codec.close).toHaveBeenCalledTimes(1);
      } finally {
        await handle.stop();
        factory.mockRestore();
      }
    },
  );

  it.each(['read', 'tail'] as const)(
    'shares the store codec with %s without preparing a writer',
    async (method) => {
      const { kafka, consumer } = fakeKafka('1');
      Object.assign(kafka, { producer: () => ({ disconnect: vi.fn() }) });
      const decoded = kafkaMessageToEventForRoute(kafkaMessage('ws_a', 'ses_a'), {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
      })!;
      decoded.payload = Buffer.from('decoded bytes');
      const codec = {
        prepareWriter: vi.fn(),
        encode: vi.fn(),
        close: vi.fn(),
        decode: vi.fn().mockResolvedValue(decoded),
      };
      const store = new KafkaTranscriptStore({ kafka, codec });
      const iterator = store[method]('ws_a', 'ses_a', {
        fromCursor: '0',
        subpath: '*',
        maxEvents: 1,
      })[Symbol.asyncIterator]();
      const next = iterator.next();
      await waitFor(() => consumer.running);
      await consumer.emit('orca.ws_a.sessions.ses_a.events', kafkaMessage('ws_a', 'ses_a'));
      expect((await next).value?.payload).toEqual(Buffer.from('decoded bytes'));
      await iterator.return?.();
      await store.close();
      expect(codec.prepareWriter).not.toHaveBeenCalled();
      expect(codec.close).not.toHaveBeenCalled();
    },
  );

  it('does not scan past a schema failure and reports a sanitized terminal error', async () => {
    const { kafka, consumer } = fakeKafka('2');
    const codec = {
      prepareWriter: vi.fn(),
      encode: vi.fn(),
      close: vi.fn(),
      decode: vi
        .fn()
        .mockResolvedValueOnce(null)
        .mockRejectedValueOnce(new Error('secret HTTP response')),
    };
    const onScannedCursor = vi.fn();
    const onError = vi.fn();
    const onEvent = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        mode: 'bounded',
        signal: new AbortController().signal,
        codec,
        onScannedCursor,
      },
      { onEvent, onError, onEnd: vi.fn() },
    );
    try {
      await waitFor(() => consumer.running);
      await consumer.emit('orca.ws_a.sessions.ses_a.events', kafkaMessage('ws_a', 'ses_a'));
      await expect(
        consumer.emit(
          'orca.ws_a.sessions.ses_a.events',
          kafkaMessage('ws_a', 'ses_a', { offset: 1 }),
        ),
      ).rejects.toThrow('Kafka transcript decode failed');
      expect(onScannedCursor.mock.calls).toEqual([['1']]);
      expect(onError).toHaveBeenCalledTimes(1);
      expect(onError.mock.calls[0]?.[0].message).not.toContain('secret');
      expect(onEvent).not.toHaveBeenCalled();
    } finally {
      await handle.stop();
    }
    expect(codec.close).not.toHaveBeenCalled();
  });

  it('treats a wrapped unknown-topic error as an empty bounded read', async () => {
    const protocolError = Object.assign(
      new Error('This server does not host this topic-partition'),
      {
        name: 'KafkaJSProtocolError',
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
        code: 3,
      },
    );
    const wrappedError = Object.assign(new Error(protocolError.message), {
      name: 'KafkaJSNumberOfRetriesExceeded',
      cause: protocolError,
    });
    const { kafka, consumer } = fakeKafka(wrappedError);
    const ac = new AbortController();
    const onEnd = vi.fn();
    const onError = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_missing',
        fromCursor: '',
        subpath: '',
        mode: 'bounded',
        signal: ac.signal,
      },
      { onEvent: () => {}, onEnd, onError },
    );

    try {
      await waitFor(() => onEnd.mock.calls.length === 1);

      expect(onError).not.toHaveBeenCalled();
      expect(consumer.subscribeCalls).toEqual([]);
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it('reports wrapped non-topic errors from a bounded read', async () => {
    const protocolError = Object.assign(new Error('Not authorized to access topics'), {
      name: 'KafkaJSProtocolError',
      type: 'TOPIC_AUTHORIZATION_FAILED',
      code: 29,
    });
    const wrappedError = Object.assign(new Error(protocolError.message), {
      name: 'KafkaJSNumberOfRetriesExceeded',
      cause: protocolError,
    });
    const { kafka, consumer } = fakeKafka(wrappedError);
    const ac = new AbortController();
    const onEnd = vi.fn();
    const onError = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_denied',
        fromCursor: '',
        subpath: '',
        mode: 'bounded',
        signal: ac.signal,
      },
      { onEvent: () => {}, onEnd, onError },
    );

    try {
      await waitFor(() => onError.mock.calls.length === 1);

      expect(onError).toHaveBeenCalledWith(wrappedError);
      expect(onEnd).not.toHaveBeenCalled();
      expect(consumer.subscribeCalls).toEqual([]);
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it('drops a forged route and still completes a bounded read at its watermark', async () => {
    const { kafka, consumer } = fakeKafka('1');
    const ac = new AbortController();
    const onEvent = vi.fn();
    const onEnd = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        mode: 'bounded',
        signal: ac.signal,
      },
      { onEvent, onEnd },
    );

    try {
      await waitFor(() => consumer.running);
      await consumer.emit('orca.ws_a.sessions.ses_a.events', kafkaMessage('ws_forged', 'ses_a'));

      expect(onEvent).not.toHaveBeenCalled();
      expect(onEnd).toHaveBeenCalledOnce();
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it('stops a bounded read before the next event would exceed maxBytes', async () => {
    const { kafka, consumer } = fakeKafka('3');
    const ac = new AbortController();
    const onEvent = vi.fn();
    const onEnd = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        maxBytes: 10,
        mode: 'bounded',
        signal: ac.signal,
      },
      { onEvent, onEnd },
    );

    try {
      await waitFor(() => consumer.running);
      await consumer.emit(
        'orca.ws_a.sessions.ses_a.events',
        kafkaMessage('ws_a', 'ses_a', { offset: 0, size: 6 }),
      );
      expect(onEvent).toHaveBeenCalledOnce();
      expect(onEnd).not.toHaveBeenCalled();

      await consumer.emit(
        'orca.ws_a.sessions.ses_a.events',
        kafkaMessage('ws_a', 'ses_a', { offset: 1, size: 5 }),
      );
      expect(onEvent).toHaveBeenCalledOnce();
      expect(onEnd).toHaveBeenCalledOnce();
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it('delivers one event when that event alone exceeds maxBytes', async () => {
    const { kafka, consumer } = fakeKafka('2');
    const ac = new AbortController();
    const onEvent = vi.fn();
    const onEnd = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        maxBytes: 5,
        mode: 'bounded',
        signal: ac.signal,
      },
      { onEvent, onEnd },
    );

    try {
      await waitFor(() => consumer.running);
      await consumer.emit(
        'orca.ws_a.sessions.ses_a.events',
        kafkaMessage('ws_a', 'ses_a', { offset: 0, size: 6 }),
      );

      expect(onEvent).toHaveBeenCalledOnce();
      expect(onEnd).toHaveBeenCalledOnce();
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it('stops after one oversized poison record without waiting for a matching event', async () => {
    const { kafka, consumer } = fakeKafka('2');
    const ac = new AbortController();
    const onEvent = vi.fn();
    const onEnd = vi.fn();
    const onScannedCursor = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        maxBytes: 5,
        onScannedCursor,
        mode: 'bounded',
        signal: ac.signal,
      },
      { onEvent, onEnd },
    );

    try {
      await waitFor(() => consumer.running);
      await consumer.emit(
        'orca.ws_a.sessions.ses_a.events',
        kafkaMessage('ws_forged', 'ses_a', { offset: 0, size: 6 }),
      );

      expect(onEvent).not.toHaveBeenCalled();
      expect(onEnd).toHaveBeenCalledOnce();
      expect(onScannedCursor).toHaveBeenCalledWith('1');
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it('accounts record-batch messages when KafkaMessage.size is absent', async () => {
    const { kafka, consumer } = fakeKafka('2');
    const ac = new AbortController();
    const onEvent = vi.fn();
    const onEnd = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        maxBytes: 1,
        mode: 'bounded',
        signal: ac.signal,
      },
      { onEvent, onEnd },
    );

    try {
      await waitFor(() => consumer.running);
      await consumer.emit(
        'orca.ws_a.sessions.ses_a.events',
        kafkaMessage('ws_a', 'ses_a', { offset: 0, size: null }),
      );

      expect(onEvent).toHaveBeenCalledOnce();
      expect(onEnd).toHaveBeenCalledOnce();
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it.each([
    { fromCursor: '', head: 'missing', expectedOffset: '0' },
    { fromCursor: '', head: '7', expectedOffset: '7' },
    { fromCursor: '5', head: '9', expectedOffset: '5' },
  ])('retries cold-topic tail subscription without moving its start: %j', async (scenario) => {
    const unknownTopic = Object.assign(
      new Error('This server does not host this topic-partition'),
      {
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
        code: 3,
      },
    );
    const { kafka, admin, consumer } = fakeKafka(
      scenario.head === 'missing'
        ? new Error('KafkaJSNumberOfRetriesExceeded', { cause: unknownTopic })
        : scenario.head,
    );
    const subscribe = vi.spyOn(consumer, 'subscribe').mockRejectedValueOnce(unknownTopic);
    const onReady = vi.fn();
    const onEnd = vi.fn();
    const onError = vi.fn();
    const onEvent = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: scenario.fromCursor,
        subpath: '*',
        mode: 'unbounded',
        signal: new AbortController().signal,
      },
      { onEvent, onReady, onEnd, onError },
    );
    try {
      await waitFor(() => onReady.mock.calls.length === 1);
      expect(subscribe).toHaveBeenCalledTimes(2);
      expect(admin.fetchTopicOffsetsCalls).toHaveLength(scenario.fromCursor ? 0 : 1);
      expect(consumer.seekCalls).toEqual([
        {
          topic: 'orca.ws_a.sessions.ses_a.events',
          partition: 0,
          offset: scenario.expectedOffset,
        },
      ]);
      // A record appended while subscribe retried is still at the pinned start.
      await consumer.emit(
        'orca.ws_a.sessions.ses_a.events',
        kafkaMessage('ws_a', 'ses_a', { offset: Number(scenario.expectedOffset) }),
      );
      expect(onEvent).toHaveBeenCalledTimes(1);
      expect(onEnd).not.toHaveBeenCalled();
      expect(onError).not.toHaveBeenCalled();
    } finally {
      await handle.stop();
    }
  });

  it('rejects a tail after three cold-topic retries instead of returning normal EOF', async () => {
    const { kafka, consumer } = fakeKafka('0');
    Object.assign(kafka, { producer: () => ({ disconnect: vi.fn() }) });
    const unknownTopic = Object.assign(new Error('topic is unavailable'), { code: 3 });
    const subscribe = vi.spyOn(consumer, 'subscribe').mockRejectedValue(unknownTopic);
    const disconnect = vi.spyOn(consumer, 'disconnect');
    const store = new KafkaTranscriptStore({ kafka });
    const ac = new AbortController();
    try {
      const iterator = store
        .tail('ws_a', 'ses_a', {
          fromCursor: '',
          subpath: '*',
          signal: ac.signal,
        })
        [Symbol.asyncIterator]();
      await expect(iterator.next()).rejects.toBe(unknownTopic);
      expect(subscribe).toHaveBeenCalledTimes(4);
      expect(consumer.running).toBe(false);
      expect(disconnect).toHaveBeenCalled();
    } finally {
      ac.abort();
      await store.close();
    }
  });

  it.each(['abort', 'stop'] as const)('cancels cold-topic backoff on %s', async (cancel) => {
    const { kafka, consumer } = fakeKafka('0');
    const subscribe = vi
      .spyOn(consumer, 'subscribe')
      .mockRejectedValue(Object.assign(new Error('unknown topic'), { code: 3 }));
    const disconnect = vi.spyOn(consumer, 'disconnect');
    const ac = new AbortController();
    const onReady = vi.fn();
    const onEnd = vi.fn();
    const onError = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        mode: 'unbounded',
        signal: ac.signal,
      },
      { onEvent: vi.fn(), onReady, onEnd, onError },
    );
    try {
      await waitFor(() => subscribe.mock.calls.length === 1);
      if (cancel === 'abort') ac.abort();
      await handle.stop();
      expect(subscribe).toHaveBeenCalledTimes(1);
      expect(disconnect).toHaveBeenCalled();
      expect(onReady).not.toHaveBeenCalled();
      expect(onEnd).not.toHaveBeenCalled();
      expect(onError).not.toHaveBeenCalled();
    } finally {
      await handle.stop();
    }
  });

  it.each(['bounded', 'unbounded'] as const)(
    'does not retry subscription authorization failures in %s mode',
    async (mode) => {
      const { kafka, consumer } = fakeKafka('1');
      const denied = Object.assign(new Error('not authorized'), {
        type: 'TOPIC_AUTHORIZATION_FAILED',
      });
      const subscribe = vi.spyOn(consumer, 'subscribe').mockRejectedValue(denied);
      const onError = vi.fn();
      const onEnd = vi.fn();
      const handle = consumeSession(
        kafka,
        {
          workspaceId: 'ws_a',
          sessionId: 'ses_a',
          fromCursor: '',
          subpath: '*',
          mode,
          signal: new AbortController().signal,
        },
        { onEvent: vi.fn(), onError, onEnd },
      );
      try {
        await waitFor(() => onError.mock.calls.length === 1);
        expect(onError).toHaveBeenCalledWith(denied);
        expect(subscribe).toHaveBeenCalledTimes(1);
        expect(onEnd).not.toHaveBeenCalled();
      } finally {
        await handle.stop();
      }
    },
  );

  it('still ends a bounded read when its topic disappears before subscribe', async () => {
    const { kafka, consumer } = fakeKafka('1');
    const subscribe = vi
      .spyOn(consumer, 'subscribe')
      .mockRejectedValue(Object.assign(new Error('unknown topic'), { code: 3 }));
    const onEnd = vi.fn();
    const onError = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        mode: 'bounded',
        signal: new AbortController().signal,
      },
      { onEvent: vi.fn(), onEnd, onError },
    );
    try {
      await waitFor(() => onEnd.mock.calls.length === 1);
      expect(subscribe).toHaveBeenCalledTimes(1);
      expect(onError).not.toHaveBeenCalled();
    } finally {
      await handle.stop();
    }
  });

  it('seeks an empty-cursor unbounded tail to the current topic head', async () => {
    const { kafka, admin, consumer } = fakeKafka('7');
    const ac = new AbortController();
    let ready = false;
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '',
        mode: 'unbounded',
        signal: ac.signal,
      },
      {
        onEvent: () => {},
        onEnd: () => {},
        onReady: () => {
          ready = true;
        },
      },
    );

    try {
      await waitFor(() => consumer.seekCalls.length === 1);

      expect(admin.fetchTopicOffsetsCalls).toEqual(['orca.ws_a.sessions.ses_a.events']);
      expect(consumer.subscribeCalls).toEqual([
        { topic: 'orca.ws_a.sessions.ses_a.events', fromBeginning: false },
      ]);
      expect(consumer.seekCalls).toEqual([
        { topic: 'orca.ws_a.sessions.ses_a.events', partition: 0, offset: '7' },
      ]);
      expect(ready).toBe(true);
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it('waits for a restarted consumer to join its group before seeking', async () => {
    const { kafka, consumer } = fakeKafka('7', false);
    const ac = new AbortController();
    const onReady = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '',
        mode: 'unbounded',
        signal: ac.signal,
      },
      { onEvent: () => {}, onEnd: () => {}, onReady },
    );

    try {
      await waitFor(() => consumer.running);
      expect(consumer.seekCalls).toEqual([]);
      expect(onReady).not.toHaveBeenCalled();

      consumer.joinGroup();
      await waitFor(() => consumer.seekCalls.length === 1);

      expect(consumer.seekCalls).toEqual([
        { topic: 'orca.ws_a.sessions.ses_a.events', partition: 0, offset: '7' },
      ]);
      expect(onReady).toHaveBeenCalledOnce();
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it('reports a terminal group-join crash without seeking or signaling ready', async () => {
    const { kafka, consumer } = fakeKafka('7', false);
    const ac = new AbortController();
    const onReady = vi.fn();
    const onError = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '',
        mode: 'unbounded',
        signal: ac.signal,
      },
      { onEvent: () => {}, onEnd: () => {}, onReady, onError },
    );

    try {
      await waitFor(() => consumer.running);
      const error = new Error('group join failed');
      consumer.crash(error, false);
      await waitFor(() => onError.mock.calls.length === 1);

      expect(onError).toHaveBeenCalledWith(error);
      expect(consumer.seekCalls).toEqual([]);
      expect(onReady).not.toHaveBeenCalled();
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it('reports a terminal consumer crash after the bounded read joined', async () => {
    const { kafka, consumer } = fakeKafka('2');
    const ac = new AbortController();
    const onError = vi.fn();
    const onReady = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        mode: 'bounded',
        signal: ac.signal,
      },
      { onEvent: () => {}, onEnd: () => {}, onError, onReady },
    );

    try {
      await waitFor(() => onReady.mock.calls.length === 1);
      const error = new Error('post-join crash');
      consumer.crash(error, false);
      await waitFor(() => onError.mock.calls.length === 1);

      expect(onError).toHaveBeenCalledWith(error);
    } finally {
      ac.abort();
      await handle.stop();
    }
  });

  it('does not seek or signal ready when cancelled before the group joins', async () => {
    const { kafka, consumer } = fakeKafka('7', false);
    const ac = new AbortController();
    const onReady = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '',
        mode: 'unbounded',
        signal: ac.signal,
      },
      { onEvent: () => {}, onEnd: () => {}, onReady },
    );

    await waitFor(() => consumer.running);
    ac.abort();
    await handle.stop();

    expect(consumer.seekCalls).toEqual([]);
    expect(onReady).not.toHaveBeenCalled();
  });

  it('does not seek a bounded read when cancelled before the group joins', async () => {
    const { kafka, consumer } = fakeKafka('1', false);
    const ac = new AbortController();
    const onReady = vi.fn();
    const onError = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        maxBytes: 10,
        mode: 'bounded',
        signal: ac.signal,
      },
      { onEvent: () => {}, onEnd: () => {}, onReady, onError },
    );

    await waitFor(() => consumer.running);
    ac.abort();
    await handle.stop();

    expect(consumer.seekCalls).toEqual([]);
    expect(onReady).not.toHaveBeenCalled();
    expect(onError).not.toHaveBeenCalled();
  });

  it('settles stop before a bounded read joins without requiring caller abort', async () => {
    const { kafka, consumer } = fakeKafka('1', false);
    const ac = new AbortController();
    const onReady = vi.fn();
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '',
        subpath: '*',
        mode: 'bounded',
        signal: ac.signal,
      },
      { onEvent: () => {}, onEnd: () => {}, onReady },
    );

    await waitFor(() => consumer.running);
    await handle.stop();

    expect(consumer.seekCalls).toEqual([]);
    expect(onReady).not.toHaveBeenCalled();
  });

  it('seeks an explicit-cursor unbounded tail without reading the topic head', async () => {
    const { kafka, admin, consumer } = fakeKafka('7');
    const ac = new AbortController();
    let ready = false;
    const handle = consumeSession(
      kafka,
      {
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
        fromCursor: '3',
        subpath: '',
        mode: 'unbounded',
        signal: ac.signal,
      },
      {
        onEvent: () => {},
        onEnd: () => {},
        onReady: () => {
          ready = true;
        },
      },
    );

    try {
      await waitFor(() => consumer.seekCalls.length === 1);

      expect(admin.fetchTopicOffsetsCalls).toEqual([]);
      expect(consumer.subscribeCalls).toEqual([
        { topic: 'orca.ws_a.sessions.ses_a.events', fromBeginning: true },
      ]);
      expect(consumer.seekCalls).toEqual([
        { topic: 'orca.ws_a.sessions.ses_a.events', partition: 0, offset: '3' },
      ]);
      expect(ready).toBe(true);
    } finally {
      ac.abort();
      await handle.stop();
    }
  });
});

function kafkaMessage(
  workspaceId: string,
  sessionId: string,
  options: { offset?: number; size?: number | null } = {},
): KafkaMessage {
  const message = {
    key: Buffer.from('evt_route'),
    value: Buffer.from('payload'),
    timestamp: '0',
    attributes: 0,
    offset: String(options.offset ?? 0),
    headers: {
      id: Buffer.from('evt_route'),
      workspace_id: Buffer.from(workspaceId),
      session_id: Buffer.from(sessionId),
    },
  };
  return options.size === null
    ? (message as KafkaMessage)
    : ({ ...message, size: options.size ?? 7 } as KafkaMessage);
}
