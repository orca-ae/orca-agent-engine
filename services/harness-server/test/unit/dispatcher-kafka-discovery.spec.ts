// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { createServer } from 'node:http';
import type { ConsumerConfig } from 'kafkajs';
import { createKafkaTranscriptCodec, type KafkaTranscriptCodec } from '@orca/transcript-store';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';
import { harnessKafkaOffsetLag } from '../../src/metrics.js';
import { Dispatcher } from '../../src/runner/dispatcher.js';
import {
  RegistryInvalidRuntimeBindingError,
  type RegistryClient,
} from '../../src/clients/registry.js';
import {
  GuardrailPolicyDeniedError,
  GuardrailUsageUnavailableError,
  SessionEventKind,
  type AgentEvent,
  type AgentHarness,
  type SessionStartInput,
  type SubmitHooks,
  type TerminationReason,
  type UserEvent,
} from '../../src/harness/agent-harness.js';

const FIRST_TOPIC = 'orca.ws_dispatcher.sessions.ses_first.events';
const SECOND_TOPIC = 'orca.ws_dispatcher.sessions.ses_second.events';

describe('Dispatcher Kafka discovery and replacement', () => {
  it('commits a permanent preparation failure and the following interrupt without seeking or spawning', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const store = new TerminalGateStore();
    const prepareExecution = vi
      .fn()
      .mockRejectedValue(new RegistryInvalidRuntimeBindingError('agent_version', 'agt_archived@1'));
    const updateSessionStateInternal = vi.fn().mockResolvedValue({ status: 'idle' });
    const harnessFactory = vi.fn(() => new TerminalHarness());
    const dispatcher = makeDispatcher(kafka, {
      store,
      harnessFactory,
      registry: {
        getExecutionOwner: vi.fn().mockResolvedValue('harness-server'),
        prepareExecution,
        updateSessionStateInternal,
      } as unknown as RegistryClient,
    });
    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      const message = userMessageDelivery(FIRST_TOPIC, 'evt_archived_agent_message');
      await consumer.deliver(message.payload);
      await waitFor(() => store.terminalAppendStarted);
      expect(consumer.committedOffsets).toEqual([]);
      store.releaseTerminalAppend();
      await waitFor(() => consumer.committedOffsets.length === 1);
      const interrupt = userMessageDelivery(FIRST_TOPIC, 'evt_archived_agent_interrupt');
      const payload = interrupt.payload as unknown as {
        message: { offset: string; headers: Record<string, Buffer>; value: Buffer };
      };
      payload.message.offset = '1';
      payload.message.headers.kind = Buffer.from('user.interrupt');
      payload.message.value = Buffer.from('{}');
      await consumer.deliver(interrupt.payload);
      await waitFor(() => consumer.committedOffsets.length === 2);
      expect(consumer.committedOffsets).toEqual([
        [{ topic: FIRST_TOPIC, partition: 0, offset: '1' }],
        [{ topic: FIRST_TOPIC, partition: 0, offset: '2' }],
      ]);
      expect(consumer.seeks).toEqual([]);
      expect(prepareExecution).toHaveBeenCalledTimes(1);
      expect(harnessFactory).not.toHaveBeenCalled();
      expect(updateSessionStateInternal).toHaveBeenCalledTimes(2);
      expect(store.appended.filter(({ kind }) => kind === 'session.error')).toHaveLength(1);
      expect(
        store.appended.filter(({ kind }) => kind === 'session.user_event_completed'),
      ).toHaveLength(2);
    } finally {
      store.releaseTerminalAppend();
      await dispatcher.stop();
    }
  });

  it.each(['user.message', 'user.interrupt'])(
    'leaves Registry-owned %s to the Registry without preparing or fabricating completion',
    async (kind) => {
      const kafka = new FakeKafka([FIRST_TOPIC]);
      const store = new TerminalGateStore();
      const getExecutionOwner = vi.fn().mockResolvedValue('registry');
      const prepareExecution = vi.fn();
      const updateSessionStateInternal = vi.fn();
      const dispatcher = makeDispatcher(kafka, {
        store,
        registry: {
          getExecutionOwner,
          prepareExecution,
          updateSessionStateInternal,
        } as unknown as RegistryClient,
      });
      try {
        await dispatcher.start();
        const consumer = kafka.consumers[0]!;
        const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_codex_interrupt');
        const wire = delivery.payload as unknown as {
          message: { headers: Record<string, Buffer>; value: Buffer };
        };
        wire.message.headers.kind = Buffer.from(kind);
        wire.message.value = Buffer.from('{}');
        await consumer.deliver(delivery.payload);
        await waitFor(() => consumer.committedOffsets.length === 1);
        expect(prepareExecution).not.toHaveBeenCalled();
        expect(updateSessionStateInternal).not.toHaveBeenCalled();
        expect(store.appended).toEqual([]);
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it('preserves accepted turn settlement and handoff after a dispatch-time heartbeat failure', async () => {
    vi.useFakeTimers();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    class AcceptedHarness extends TerminalHarness {
      override async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
        await super.submit(event, hooks);
        await gate;
      }
    }
    const harness = new AcceptedHarness();
    const submitted = vi.spyOn(harness, 'submit');
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const store = new TerminalGateStore();
    const dispatcher = makeTurnDispatcher(kafka, store, { harnessFactory: () => harness });
    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_accepted_heartbeat');
      const heartbeat = vi
        .fn(async () => {})
        .mockResolvedValueOnce(undefined)
        .mockResolvedValueOnce(undefined)
        .mockRejectedValueOnce(new Error('ILLEGAL_GENERATION'));
      (delivery.payload as unknown as { heartbeat: () => Promise<void> }).heartbeat = heartbeat;
      await consumer.deliver(delivery.payload);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(heartbeat).toHaveBeenCalledTimes(3);
      expect(store.terminalAppendStarted).toBe(true);
      expect(submitted).toHaveBeenCalledTimes(1);
      expect(consumer.seeks).toEqual([]);
      expect(consumer.committedOffsets).toEqual([]);
      consumer.emitRebalancing();
      consumer.emitGroupJoin();
      await consumer.deliver(userMessageDelivery(FIRST_TOPIC, 'evt_accepted_heartbeat').payload);
      release();
      store.releaseTerminalAppend();
      await vi.advanceTimersByTimeAsync(0);
      expect(submitted).toHaveBeenCalledTimes(1);
      expect(consumer.committedOffsets).toHaveLength(1);
      expect(consumer.seeks).toEqual([]);
      expect(delivery.resumed()).toBe(false);
    } finally {
      release();
      store.releaseTerminalAppend();
      await dispatcher.stop();
      vi.useRealTimers();
    }
  });

  it.each([
    ['post-decode', 'user.message'],
    ['post-decode', 'session.archived'],
    ['periodic-in-flight', 'user.message'],
    ['periodic-in-flight', 'session.archived'],
  ] as const)(
    'blocks %s heartbeat failure before %s effects with unchanged assignment',
    async (window, kind) => {
      vi.useFakeTimers();
      const kafka = new FakeKafka([FIRST_TOPIC]);
      const store = new TerminalGateStore();
      store.releaseTerminalAppend();
      const harness = new TerminalHarness();
      const submitted = vi.spyOn(harness, 'submit');
      const stopped = vi.spyOn(harness, 'stop');
      const raw = createKafkaTranscriptCodec();
      const codec = { ...raw, decode: vi.fn(raw.decode) };
      const dispatcher = makeTurnDispatcher(kafka, store, { codec, harnessFactory: () => harness });
      let releaseDecode!: () => void;
      const decoding = new Promise<void>((resolve) => {
        releaseDecode = resolve;
      });
      let rejectHeartbeat!: (error: Error) => void;
      const beat = new Promise<void>((_resolve, reject) => {
        rejectHeartbeat = reject;
      });
      // The rejection is also observed if an assertion fails before Kafka starts the beat.
      void beat.catch(() => {});
      try {
        await dispatcher.start();
        const consumer = kafka.consumers[0]!;
        await consumer.deliver(userMessageDelivery(FIRST_TOPIC, 'evt_warm').payload);
        await vi.advanceTimersByTimeAsync(0);
        expect(consumer.committedOffsets).toHaveLength(1);
        submitted.mockClear();
        const appended = store.appended.length;
        const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_heartbeat_failure');
        const payload = delivery.payload as unknown as {
          message: { headers: Record<string, Buffer>; offset: string };
          heartbeat: () => Promise<void>;
        };
        payload.message.headers['kind'] = Buffer.from(kind);
        payload.message.offset = '1';
        const heartbeat = vi.fn(async () => {}).mockResolvedValueOnce(undefined);
        if (window === 'post-decode') {
          heartbeat.mockRejectedValueOnce(new Error('ILLEGAL_GENERATION'));
        } else {
          heartbeat.mockImplementationOnce(async () => await beat);
          codec.decode.mockImplementationOnce(async (message, route, signal) => {
            await decoding;
            return await raw.decode(message, route, signal);
          });
        }
        payload.heartbeat = heartbeat;
        const handling = consumer.deliver(delivery.payload).catch(() => {});
        if (window === 'periodic-in-flight') {
          await vi.advanceTimersByTimeAsync(5_000);
          expect(heartbeat).toHaveBeenCalledTimes(2);
          releaseDecode();
          await vi.advanceTimersByTimeAsync(0);
          // Decode completion must not outrun the still-pending membership check.
          expect(submitted).not.toHaveBeenCalled();
          expect(stopped).not.toHaveBeenCalled();
          expect(consumer.committedOffsets).toHaveLength(1);
          rejectHeartbeat(new Error('ILLEGAL_GENERATION'));
        }
        await vi.advanceTimersByTimeAsync(250);
        await handling;
        expect(submitted).not.toHaveBeenCalled();
        expect(stopped).not.toHaveBeenCalled();
        expect(store.appended).toHaveLength(appended);
        expect(consumer.committedOffsets).toHaveLength(1);
      } finally {
        releaseDecode();
        rejectHeartbeat(new Error('test cleanup'));
        await dispatcher.stop();
        await raw.close();
        vi.useRealTimers();
      }
    },
  );

  it('consumes real Avro turns and a validated Avro archive only from the selected topic set', async () => {
    let schema = '';
    const server = createServer(async (request, response) => {
      response.setHeader('content-type', 'application/json');
      if (request.method === 'POST') {
        const chunks: Buffer[] = [];
        for await (const chunk of request) chunks.push(Buffer.from(chunk));
        schema = (JSON.parse(Buffer.concat(chunks).toString()) as { schema: string }).schema;
        response.end(JSON.stringify({ id: 17 }));
      } else {
        response.end(JSON.stringify({ schemaType: 'AVRO', schema }));
      }
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address() as { port: number };
    const codec = createKafkaTranscriptCodec({
      encoding: 'avro',
      schemaRegistry: { url: `http://127.0.0.1:${address.port}` },
    });
    const topic = `${FIRST_TOPIC}-avro`;
    const kafka = new FakeKafka([FIRST_TOPIC, topic]);
    const store = new TerminalGateStore();
    const harness = new TerminalHarness();
    const stopped = vi.spyOn(harness, 'stop');
    const dispatcher = makeTurnDispatcher(kafka, store, { codec, harnessFactory: () => harness });
    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      expect(kafka.consumers).toHaveLength(1);
      expect(consumer.subscriptions).toEqual([[topic]]);
      for (const offset of [0, 1, 2]) {
        const archive = offset === 2;
        const wire = await codec.encode({
          id: `evt_mixed_${offset}`,
          workspaceId: 'ws_dispatcher',
          sessionId: 'ses_first',
          subpath: '',
          seq: 0,
          producedAt: '2026-09-11T00:00:00Z',
          producedBy: archive ? 'harness' : 'client',
          kind: archive ? 'session.archived' : 'user.message',
          idempotencyKey: '',
          payload: Buffer.from(archive ? '' : '{"content":[{"type":"text","text":"mixed"}]}'),
        });
        const delivery = fakeDelivery(topic, `evt_mixed_${offset}`);
        (delivery.payload as unknown as { message: unknown }).message = {
          ...wire,
          key: Buffer.from(wire.key!),
          value: Buffer.from(wire.value!),
          offset: String(offset),
        };
        // Valid Avro frames and headers do not authorize reading the old topic set.
        const decode = vi.spyOn(codec, 'decode');
        await (dispatcher as unknown as { onMessage(payload: unknown): Promise<void> }).onMessage({
          ...(delivery.payload as object),
          topic: FIRST_TOPIC,
        });
        expect(decode).not.toHaveBeenCalled();
        decode.mockRestore();
        if (archive) {
          const message = (
            delivery.payload as unknown as { message: { headers: Record<string, Buffer> } }
          ).message;
          const headers = message.headers;
          message.headers = { ...headers, kind: Buffer.from('session.deleted') };
          await consumer.deliver(delivery.payload);
          expect(consumer.committedOffsets).toHaveLength(2);
          expect(consumer.seeks).toHaveLength(1);
          expect(stopped).not.toHaveBeenCalled();
          message.headers = headers;
        }
        await consumer.deliver(delivery.payload);
        if (offset === 0) {
          await waitFor(() => store.terminalAppendStarted);
          expect(consumer.committedOffsets).toHaveLength(0);
          store.releaseTerminalAppend();
        }
        await waitFor(() => consumer.committedOffsets.length === offset + 1);
      }
      expect(
        store.appended.filter((event) => event.kind === 'session.user_event_completed'),
      ).toHaveLength(2);
      expect(stopped).toHaveBeenCalledWith('client.archived');
    } finally {
      store.releaseTerminalAppend();
      await dispatcher.stop();
      await codec.close();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }
  });

  it('fences dispatch if assignment changes during the post-decode heartbeat', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const codec = codecFixture();
    const factory = vi.fn(() => new TerminalHarness());
    const store = new TerminalGateStore();
    const dispatcher = makeTurnDispatcher(kafka, store, { codec, harnessFactory: factory });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_codec');
      const heartbeat = vi.fn(async () => await gate).mockResolvedValueOnce(undefined);
      (delivery.payload as unknown as { heartbeat: () => Promise<void> }).heartbeat = heartbeat;
      await consumer.deliver(delivery.payload);
      expect(heartbeat).toHaveBeenCalledTimes(2);
      consumer.emitRebalancing();
      consumer.emitGroupJoin();
      release();
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(factory).not.toHaveBeenCalled();
      expect(consumer.committedOffsets).toEqual([]);
      expect(consumer.seeks).toEqual([]);
      expect(delivery.resumed()).toBe(false);
    } finally {
      release();
      store.releaseTerminalAppend();
      await dispatcher.stop();
    }
  });

  it('heartbeats throughout schema lookup and aborts lookup when heartbeat fails', async () => {
    vi.useFakeTimers();
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const codec = codecFixture();
    let signal: AbortSignal | undefined;
    vi.mocked(codec.decode).mockImplementation(async (_message, _route, abort) => {
      signal = abort;
      return await new Promise<Event | null>(() => {});
    });
    const factory = vi.fn(() => new TerminalHarness());
    const dispatcher = makeTurnDispatcher(kafka, new TerminalGateStore(), {
      codec,
      harnessFactory: factory,
    });
    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_pending');
      const heartbeat = vi.fn(async () => {});
      heartbeat
        .mockResolvedValueOnce(undefined)
        .mockResolvedValueOnce(undefined)
        .mockRejectedValue(new Error('lost membership'));
      (delivery.payload as unknown as { heartbeat: () => Promise<void> }).heartbeat = heartbeat;
      const handling = consumer.deliver(delivery.payload);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(heartbeat).toHaveBeenCalledTimes(2);
      expect(factory).not.toHaveBeenCalled();
      expect(consumer.committedOffsets).toEqual([]);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(signal?.aborted).toBe(true);
      await vi.advanceTimersByTimeAsync(250);
      await handling;
      expect(consumer.committedOffsets).toEqual([]);
      expect(consumer.seeks).toHaveLength(1);
      const count = heartbeat.mock.calls.length;
      await vi.advanceTimersByTimeAsync(5_000);
      expect(heartbeat).toHaveBeenCalledTimes(count);
    } finally {
      await dispatcher.stop();
      vi.useRealTimers();
    }
  });

  it.each(['user.message', 'session.archived', 'session.deleted', 'session.status_idle'])(
    'retries codec failure for %s without executing control effects or committing',
    async (kind) => {
      const kafka = new FakeKafka([FIRST_TOPIC]);
      const store = new TerminalGateStore();
      store.releaseTerminalAppend();
      const harness = new TerminalHarness();
      const stopped = vi.spyOn(harness, 'stop');
      const codec = codecFixture();
      const dispatcher = makeTurnDispatcher(kafka, store, { codec, harnessFactory: () => harness });
      try {
        await dispatcher.start();
        const consumer = kafka.consumers[0]!;
        await consumer.deliver(userMessageDelivery(FIRST_TOPIC, 'evt_codec').payload);
        await waitFor(() => consumer.committedOffsets.length === 1);
        const priorAppends = store.appended.length;
        vi.mocked(codec.decode).mockRejectedValueOnce(new Error('schema unavailable'));
        const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_invalid');
        const message = (
          delivery.payload as unknown as {
            message: { offset: string; headers: Record<string, Buffer>; value: Buffer };
          }
        ).message;
        message.offset = '1';
        message.headers['kind'] = Buffer.from(kind);
        message.value = Buffer.from([0, 0, 0, 0, 7]);
        await consumer.deliver(delivery.payload);
        await waitFor(() => consumer.seeks.length === 1);
        expect(consumer.committedOffsets).toHaveLength(1);
        expect(store.appended).toHaveLength(priorAppends);
        expect(stopped).not.toHaveBeenCalled();
        expect(delivery.paused()).toBe(true);
        expect(delivery.resumed()).toBe(true);
      } finally {
        await dispatcher.stop();
      }
    },
  );

  it.each(['rebalance', 'shutdown', 'crash', 'replacement'] as const)(
    'cancels a pending schema lookup on %s before any turn effects',
    async (action) => {
      const kafka = new FakeKafka([FIRST_TOPIC]);
      const store = new TerminalGateStore();
      let signal: AbortSignal | undefined;
      let release!: (event: Event | null) => void;
      const pending = new Promise<Event | null>((resolve) => {
        release = resolve;
      });
      const codec: KafkaTranscriptCodec = {
        prepareWriter: vi.fn(async () => {}),
        encode: vi.fn(async () => ({ value: null })),
        close: vi.fn(async () => {}),
        decode: vi.fn(async (_message, _route, abort) => {
          signal = abort;
          return await pending;
        }),
      };
      const harnessFactory = vi.fn(() => new TerminalHarness());
      const dispatcher = makeTurnDispatcher(kafka, store, { codec, harnessFactory });
      try {
        await dispatcher.start();
        const consumer = kafka.consumers[0]!;
        const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_pending_decode');
        const handling = consumer.deliver(delivery.payload);
        await waitFor(() => vi.mocked(codec.decode).mock.calls.length === 1);
        if (action === 'shutdown') await dispatcher.stop();
        else if (action === 'crash') consumer.emitCrash();
        else if (action === 'replacement') {
          kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];
          await rediscover(dispatcher);
        } else {
          consumer.emitRebalancing();
          consumer.emitGroupJoin();
        }
        expect(signal?.aborted).toBe(true);
        await handling;
        release(null);
        expect(harnessFactory).not.toHaveBeenCalled();
        expect(store.appended).toEqual([]);
        expect(consumer.committedOffsets).toEqual([]);
        expect(consumer.seeks).toEqual([]);
        expect(delivery.resumed()).toBe(false);
        expect(codec.close).not.toHaveBeenCalled();
      } finally {
        release(null);
        await dispatcher.stop();
      }
    },
  );

  it('dispatches decoded payload bytes and commits only after durable turn completion', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const store = new TerminalGateStore();
    const codec: KafkaTranscriptCodec = {
      prepareWriter: vi.fn(async () => {}),
      encode: vi.fn(async () => ({ value: null })),
      close: vi.fn(async () => {}),
      decode: vi.fn(async () => ({
        id: 'evt_codec',
        workspaceId: 'ws_dispatcher',
        sessionId: 'ses_first',
        subpath: '',
        seq: 0,
        producedAt: '2026-09-11T00:00:00Z',
        producedBy: 'client',
        kind: 'user.message',
        idempotencyKey: '',
        payload: Buffer.from('{"content":[{"type":"text","text":"decoded"}]}'),
      })),
    };
    const dispatcher = makeTurnDispatcher(kafka, store, { codec });
    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_codec');
      (delivery.payload as unknown as { message: { value: Buffer } }).message.value = Buffer.from([
        0, 0, 0, 0, 1, 255,
      ]);
      await consumer.deliver(delivery.payload);
      await waitFor(() => store.terminalAppendStarted);
      expect(codec.decode).toHaveBeenCalled();
      expect(consumer.committedOffsets).toEqual([]);
      store.releaseTerminalAppend();
      await waitFor(() => consumer.committedOffsets.length === 1);
    } finally {
      store.releaseTerminalAppend();
      await dispatcher.stop();
    }
  });

  it('discovers a topic created after the subscription snapshot on the next tick', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    kafka.onSubscribe = (consumer) => {
      if (consumer.index === 0) kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];
    };
    const dispatcher = makeDispatcher(kafka);

    try {
      await dispatcher.start();

      expect(kafka.consumers[0]?.subscriptions).toEqual([[FIRST_TOPIC]]);
      expect(activeTopics(dispatcher)).toEqual([FIRST_TOPIC]);

      await rediscover(dispatcher);

      expect(kafka.consumers[1]?.subscriptions).toEqual([[FIRST_TOPIC, SECOND_TOPIC]]);
      expect(activeTopics(dispatcher)).toEqual([FIRST_TOPIC, SECOND_TOPIC]);
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });
    } finally {
      await dispatcher.stop();
    }
  });

  it.each([
    {
      label: 'subscribe fails',
      candidate: { subscribeError: new Error('candidate subscribe failed') } satisfies ConsumerPlan,
    },
    {
      label: 'GROUP_JOIN times out',
      candidate: { join: 'never' } satisfies ConsumerPlan,
    },
  ])(
    'preserves prior topics and retries desired topics when candidate $label',
    async ({ candidate }) => {
      const kafka = new FakeKafka([FIRST_TOPIC]);
      kafka.plans = [{}, candidate, {}, {}];
      const dispatcher = makeDispatcher(kafka, { kafkaGroupJoinTimeoutMs: 5 });
      const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

      try {
        await dispatcher.start();
        kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];

        await rediscover(dispatcher);

        expect(kafka.consumers[1]?.disconnected).toBe(true);
        if (candidate.subscribeError) {
          // Candidate preparation failed before old retirement. Keep the
          // joined old consumer active; a second fallback would cause an
          // unnecessary availability gap and another transition.
          expect(kafka.consumers[0]?.disconnected).toBe(false);
          expect(kafka.consumers).toHaveLength(2);
        } else {
          expect(kafka.consumers[2]?.subscriptions).toEqual([[FIRST_TOPIC]]);
        }
        expect(activeTopics(dispatcher)).toEqual([FIRST_TOPIC]);
        expect(knownTopics(dispatcher)).toEqual([FIRST_TOPIC, SECOND_TOPIC]);
        expect(dispatcher.readiness()).toMatchObject({ ready: false });
        expect(dispatcher.readiness().reasons).toContain('kafka_topics_unjoined');

        await rediscover(dispatcher);

        const replacementIndex = candidate.subscribeError ? 2 : 3;
        expect(kafka.consumers[replacementIndex]?.subscriptions).toEqual([
          [FIRST_TOPIC, SECOND_TOPIC],
        ]);
        expect(activeTopics(dispatcher)).toEqual([FIRST_TOPIC, SECOND_TOPIC]);
        expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });
      } finally {
        consoleError.mockRestore();
        await dispatcher.stop();
      }
    },
  );

  it('clears readiness during rebalance or crash and restores it at GROUP_JOIN', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka);

    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });

      consumer.emitRebalancing();
      expect(dispatcher.readiness()).toMatchObject({ ready: false });
      expect(dispatcher.readiness().reasons).toContain('kafka_topics_unjoined');

      await rediscover(dispatcher);
      expect(kafka.consumers).toHaveLength(1);
      expect(consumer.disconnected).toBe(false);
      consumer.emitGroupJoin();
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });

      consumer.emitCrash();
      expect(dispatcher.readiness()).toMatchObject({ ready: false });
      expect(dispatcher.readiness().reasons).toContain('kafka_transition_failed');

      consumer.emitGroupJoin();
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });
    } finally {
      await dispatcher.stop();
    }
  });

  it('replaces a crashed consumer even when discovery finds unchanged topics', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka);

    try {
      await dispatcher.start();
      const crashed = kafka.consumers[0]!;
      // KafkaJS must not restart the retired instance alongside our replacement.
      expect(
        await kafka.consumerConfigs[0]?.retry?.restartOnFailure?.(new Error('broker unavailable')),
      ).toBe(false);
      crashed.emitCrash();

      await rediscover(dispatcher);

      expect(crashed.disconnected).toBe(true);
      expect(kafka.consumers[1]?.subscriptions).toEqual([[FIRST_TOPIC]]);
      expect(
        await kafka.consumerConfigs[1]?.retry?.restartOnFailure?.(new Error('broker unavailable')),
      ).toBe(false);
      expect(activeTopics(dispatcher)).toEqual([FIRST_TOPIC]);
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });
    } finally {
      await dispatcher.stop();
    }
  });

  it('retries an initial join crash even when KafkaJS run resolves without joining', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    kafka.plans = [{ join: 'crash' }, { join: 'crash' }, {}];
    const dispatcher = makeDispatcher(kafka);
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await dispatcher.start();
      expect(kafka.consumers[0]?.disconnected).toBe(true);
      expect(dispatcher.readiness().ready).toBe(false);

      await rediscover(dispatcher);
      expect(kafka.consumers[1]?.disconnected).toBe(true);
      expect(dispatcher.readiness().ready).toBe(false);

      await rediscover(dispatcher);
      expect(kafka.consumers[2]?.subscriptions).toEqual([[FIRST_TOPIC]]);
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });
    } finally {
      await dispatcher.stop();
      errorSpy.mockRestore();
    }
  });

  it('is ready after GROUP_JOIN with an empty or subset assignment', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka);

    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;

      consumer.emitGroupJoin({});
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });

      consumer.emitGroupJoin({ [FIRST_TOPIC]: [] });
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });
    } finally {
      await dispatcher.stop();
    }
  });

  it('exports END_BATCH_PROCESS offset lag and clears it without an assignment or on stop', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka);

    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;

      consumer.emitEndBatchProcess('17');
      await expectKafkaOffsetLag(17);

      consumer.emitGroupJoin({});
      await expectKafkaOffsetLag(0);

      consumer.emitEndBatchProcess('99');
      await expectKafkaOffsetLag(0);

      await dispatcher.stop();
      await expectKafkaOffsetLag(0);
    } finally {
      await dispatcher.stop();
    }
  });

  it('joins existing topics on every process start', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const first = makeDispatcher(kafka);
    const second = makeDispatcher(kafka);

    try {
      await first.start();
      expect(first.readiness()).toEqual({ ready: true, reasons: [] });
      await first.stop();

      await second.start();
      expect(kafka.consumers[1]?.subscriptions).toEqual([[FIRST_TOPIC]]);
      expect(second.readiness()).toEqual({ ready: true, reasons: [] });
    } finally {
      await second.stop();
    }
  });

  it('retries metadata discovery after startup failure and treats a fresh empty snapshot as ready', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    kafka.listTopicFailures = 1;
    const dispatcher = makeDispatcher(kafka);
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await dispatcher.start();
      expect(kafka.consumers).toHaveLength(0);
      expect(dispatcher.readiness()).toMatchObject({ ready: false });
      expect(dispatcher.readiness().reasons).toContain('kafka_discovery_stale');

      await rediscover(dispatcher);
      expect(kafka.consumers[0]?.subscriptions).toEqual([[FIRST_TOPIC]]);
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });
      await dispatcher.stop();

      const emptyKafka = new FakeKafka([]);
      const emptyDispatcher = makeDispatcher(emptyKafka);
      try {
        await emptyDispatcher.start();
        expect(emptyKafka.consumers).toHaveLength(0);
        expect(emptyDispatcher.readiness()).toEqual({ ready: true, reasons: [] });
      } finally {
        await emptyDispatcher.stop();
      }
    } finally {
      consoleError.mockRestore();
      await dispatcher.stop();
    }
  });

  it('does not run a replacement while old consumer still has an in-flight handler', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka);

    try {
      await dispatcher.start();
      const oldConsumer = kafka.consumers[0]!;
      oldConsumer.holdInFlightHandler();
      kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];

      const transition = rediscover(dispatcher);
      await waitFor(() => oldConsumer.disconnectCalls === 1);
      expect(kafka.consumers[1]?.runCalls).toBe(0);

      oldConsumer.releaseInFlightHandler();
      await transition;

      expect(kafka.consumers[1]?.runCalls).toBe(1);
      expect(kafka.duplicateRunAttempts).toBe(0);
    } finally {
      await dispatcher.stop();
    }
  });

  it('replaces consumers while turn work runs and reuses that work on redelivery', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka);
    let releaseWork: () => void = () => {};
    const workGate = new Promise<void>((resolve) => {
      releaseWork = resolve;
    });
    const onMessage = vi.fn(async () => await workGate);
    (
      dispatcher as unknown as {
        onMessage(payload: unknown): Promise<void>;
      }
    ).onMessage = onMessage;

    try {
      await dispatcher.start();
      const oldConsumer = kafka.consumers[0]!;
      const oldDelivery = userMessageDelivery(FIRST_TOPIC, 'evt_long_turn');
      await oldConsumer.deliver(oldDelivery.payload);
      expect(oldDelivery.paused()).toBe(true);
      expect(onMessage).toHaveBeenCalledTimes(1);
      expect(oldConsumer.committedOffsets).toEqual([]);

      kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];
      await rediscover(dispatcher);
      const replacement = kafka.consumers[1]!;
      expect(replacement.runCalls).toBe(1);

      const retryDelivery = userMessageDelivery(FIRST_TOPIC, 'evt_long_turn');
      await replacement.deliver(retryDelivery.payload);
      expect(onMessage).toHaveBeenCalledTimes(1);

      releaseWork();
      (
        dispatcher as unknown as {
          settleKafkaSourceEvent(sourceEventId: string): void;
        }
      ).settleKafkaSourceEvent('evt_long_turn');
      await waitFor(() => replacement.committedOffsets.length === 1);
      expect(replacement.committedOffsets[0]).toEqual([
        { topic: FIRST_TOPIC, partition: 0, offset: '1' },
      ]);
      expect(retryDelivery.resumed()).toBe(true);
    } finally {
      releaseWork();
      await dispatcher.stop();
    }
  });

  it('fences detached commit and resume across an assignment epoch change', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka);
    let releaseWork: () => void = () => {};
    const workGate = new Promise<void>((resolve) => {
      releaseWork = resolve;
    });
    const onMessage = vi.fn(async () => await workGate);
    (
      dispatcher as unknown as {
        onMessage(payload: unknown): Promise<void>;
      }
    ).onMessage = onMessage;

    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      const staleDelivery = userMessageDelivery(FIRST_TOPIC, 'evt_assignment_epoch');
      await consumer.deliver(staleDelivery.payload);

      consumer.emitRebalancing();
      consumer.emitGroupJoin({ [FIRST_TOPIC]: [0] });
      releaseWork();
      (
        dispatcher as unknown as {
          settleKafkaSourceEvent(sourceEventId: string): void;
        }
      ).settleKafkaSourceEvent('evt_assignment_epoch');
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(consumer.committedOffsets).toEqual([]);
      expect(consumer.seeks).toEqual([]);
      expect(staleDelivery.resumed()).toBe(false);

      const ownedDelivery = userMessageDelivery(FIRST_TOPIC, 'evt_assignment_epoch');
      await consumer.deliver(ownedDelivery.payload);
      await waitFor(() => consumer.committedOffsets.length === 1 && ownedDelivery.resumed());
      expect(onMessage).toHaveBeenCalledTimes(1);
      expect(ownedDelivery.resumed()).toBe(true);
    } finally {
      releaseWork();
      await dispatcher.stop();
    }
  });

  it('retains shared work when commit resolves after ownership is lost', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka);
    const onMessage = vi.fn(async () => {});
    (
      dispatcher as unknown as {
        onMessage(payload: unknown): Promise<void>;
        settleKafkaSourceEvent(sourceEventId: string): void;
      }
    ).onMessage = onMessage;

    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      consumer.beforeCommit = () => {
        consumer.beforeCommit = null;
        consumer.emitRebalancing();
        return false;
      };
      const staleDelivery = userMessageDelivery(FIRST_TOPIC, 'evt_commit_ownership_loss');
      await consumer.deliver(staleDelivery.payload);
      (
        dispatcher as unknown as {
          settleKafkaSourceEvent(sourceEventId: string): void;
        }
      ).settleKafkaSourceEvent('evt_commit_ownership_loss');
      await waitFor(() => consumer.beforeCommit === null);
      expect(consumer.committedOffsets).toEqual([]);

      consumer.emitGroupJoin({ [FIRST_TOPIC]: [0] });
      const ownedDelivery = userMessageDelivery(FIRST_TOPIC, 'evt_commit_ownership_loss');
      await consumer.deliver(ownedDelivery.payload);
      await waitFor(() => consumer.committedOffsets.length === 1);
      expect(onMessage).toHaveBeenCalledTimes(1);
    } finally {
      await dispatcher.stop();
    }
  });

  it('does not idle out or commit before terminal status and completion marker are durable', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const store = new TerminalGateStore();
    const dispatcher = makeTurnDispatcher(kafka, store, { sessionIdleTimeoutMs: 5 });

    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_durable_terminal');
      await consumer.deliver(delivery.payload);
      await waitFor(() => store.terminalAppendStarted);
      expect(consumer.committedOffsets).toEqual([]);
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(
        (
          dispatcher as unknown as {
            runners: Map<string, unknown>;
          }
        ).runners.has('ws_dispatcher/ses_first'),
      ).toBe(true);

      store.releaseTerminalAppend();
      await waitFor(() => consumer.committedOffsets.length === 1);
      expect(store.appended.some((event) => event.kind === 'session.user_event_completed')).toBe(
        true,
      );
    } finally {
      store.releaseTerminalAppend();
      await dispatcher.stop();
    }
  });

  it.each([
    {
      label: 'usage failure after acceptance',
      accepted: true,
      error: new GuardrailUsageUnavailableError('ACK failed'),
    },
    {
      label: 'pending usage retry before acceptance',
      accepted: false,
      error: new GuardrailUsageUnavailableError('ACK still unavailable'),
    },
    {
      label: 'request policy denial before acceptance',
      accepted: false,
      error: new GuardrailPolicyDeniedError([], 'budget exceeded'),
    },
  ])(
    'settles $label only after the terminal append and allows session deletion',
    async ({ accepted, error }) => {
      const kafka = new FakeKafka([FIRST_TOPIC]);
      const store = new TerminalGateStore();
      const harness = new GuardrailFailureHarness(error, accepted);
      const stopped = vi.spyOn(harness, 'stop');
      const dispatcher = makeTurnDispatcher(kafka, store, {
        harnessFactory: () => harness,
        sessionIdleTimeoutMs: 60_000,
      });

      try {
        await dispatcher.start();
        const consumer = kafka.consumers[0]!;
        const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_guardrail_failure');
        await consumer.deliver(delivery.payload);
        await waitFor(() => store.terminalAppendStarted);
        expect(delivery.paused()).toBe(true);
        expect(consumer.committedOffsets).toEqual([]);
        expect(delivery.resumed()).toBe(false);

        store.releaseTerminalAppend();
        await waitFor(() => consumer.committedOffsets.length === 1 && delivery.resumed());
        expect(consumer.committedOffsets).toEqual([
          [{ topic: FIRST_TOPIC, partition: 0, offset: '1' }],
        ]);
        const completed = store.appended.filter(
          (event) => event.kind === 'session.user_event_completed',
        );
        expect(completed).toHaveLength(accepted ? 1 : 0);
        if (accepted) {
          expect(JSON.parse(Buffer.from(completed[0]!.payload).toString('utf8'))).toEqual({
            user_event_id: 'evt_guardrail_failure',
          });
        }
        expect(harness.hasPendingGuardrailUsage()).toBe(
          error instanceof GuardrailUsageUnavailableError,
        );
        expect(stopped).not.toHaveBeenCalled();

        // Kafka cannot deliver the queued lifecycle sentinel until this session
        // partition resumes. This is the cleanup that frees the sandbox capacity.
        const deletion = userMessageDelivery(FIRST_TOPIC, 'evt_session_deleted');
        const payload = deletion.payload as unknown as {
          message: { offset: string; headers: Record<string, Buffer> };
        };
        payload.message.offset = '1';
        payload.message.headers.kind = Buffer.from('session.deleted');
        payload.message.headers.produced_by = Buffer.from('transcript-store');
        await consumer.deliver(deletion.payload);
        expect(stopped).toHaveBeenCalledWith('client.archived');
        expect(consumer.committedOffsets.at(-1)).toEqual([
          { topic: FIRST_TOPIC, partition: 0, offset: '2' },
        ]);
      } finally {
        store.releaseTerminalAppend();
        await dispatcher.stop();
      }
    },
  );

  it('retries a guardrail terminal append failure without committing the source', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const store = new TerminalGateStore(true);
    const harness = new GuardrailFailureHarness(
      new GuardrailUsageUnavailableError('ACK failed'),
      true,
    );
    const dispatcher = makeTurnDispatcher(kafka, store, { harnessFactory: () => harness });
    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_guardrail_terminal_retry');
      await consumer.deliver(delivery.payload);
      await waitFor(() => store.terminalAppendStarted);
      store.releaseTerminalAppend();
      await waitFor(() => consumer.seeks.length === 1 && delivery.resumed());
      expect(consumer.committedOffsets).toEqual([]);
      expect(store.appended.some((event) => event.kind === 'session.user_event_completed')).toBe(
        false,
      );

      const retry = userMessageDelivery(FIRST_TOPIC, 'evt_guardrail_terminal_retry');
      await consumer.deliver(retry.payload);
      await waitFor(() => consumer.committedOffsets.length === 1 && retry.resumed());
      expect(
        store.appended.filter((event) => event.kind === 'session.user_event_completed'),
      ).toHaveLength(1);
    } finally {
      store.releaseTerminalAppend();
      await dispatcher.stop();
    }
  });

  it('commits Harness loopback records without pausing the partition', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka);
    const onMessage = vi.fn(async () => undefined);
    (
      dispatcher as unknown as {
        onMessage(payload: unknown): Promise<void>;
      }
    ).onMessage = onMessage;

    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      const delivery = fakeDelivery(FIRST_TOPIC, 'evt_harness_loopback');

      await consumer.deliver(delivery.payload);

      expect(onMessage).toHaveBeenCalledTimes(1);
      expect(delivery.paused()).toBe(false);
      expect(delivery.resumed()).toBe(false);
      expect(consumer.committedOffsets).toEqual([
        [{ topic: FIRST_TOPIC, partition: 0, offset: '1' }],
      ]);
    } finally {
      await dispatcher.stop();
    }
  });

  it('ignores a late persistence failure callback from a retired runner', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const store = new TerminalGateStore();
    const dispatcher = makeTurnDispatcher(kafka, store);
    const internals = dispatcher as unknown as {
      lifecycleGeneration: number;
      runners: Map<string, { stop(reason: TerminationReason): Promise<void> }>;
      spawnRunnerWithMetrics(
        workspaceId: string,
        sessionId: string,
      ): Promise<{ stop(reason: TerminationReason): Promise<void> }>;
      handleRunnerEventPersistenceFailure(
        workspaceId: string,
        sessionId: string,
        lifecycleGeneration: number,
        originatingRunner: { stop(reason: TerminationReason): Promise<void> },
        error: unknown,
      ): void;
    };
    let retiredRunner: { stop(reason: TerminationReason): Promise<void> } | null = null;

    try {
      await dispatcher.start();
      retiredRunner = await internals.spawnRunnerWithMetrics('ws_dispatcher', 'ses_first');
      const runnerKey = 'ws_dispatcher/ses_first';
      internals.runners.set(runnerKey, retiredRunner);
      internals.runners.delete(runnerKey);

      const consumer = kafka.consumers[0]!;
      await consumer.deliver(userMessageDelivery(FIRST_TOPIC, 'evt_fresh_runner').payload);
      await waitFor(() => store.terminalAppendStarted);
      const freshRunner = internals.runners.get(runnerKey);
      expect(freshRunner).toBeDefined();
      expect(freshRunner).not.toBe(retiredRunner);

      internals.handleRunnerEventPersistenceFailure(
        'ws_dispatcher',
        'ses_first',
        internals.lifecycleGeneration,
        retiredRunner,
        new Error('late retired append failure'),
      );
      expect(internals.runners.get(runnerKey)).toBe(freshRunner);

      store.releaseTerminalAppend();
      await waitFor(() => consumer.committedOffsets.length === 1);
    } finally {
      store.releaseTerminalAppend();
      await retiredRunner?.stop('replica.shutting_down');
      await dispatcher.stop();
    }
  });

  it('seeks with backoff and never commits when terminal persistence fails', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const store = new TerminalGateStore(true);
    const dispatcher = makeTurnDispatcher(kafka, store);

    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      const delivery = userMessageDelivery(FIRST_TOPIC, 'evt_terminal_failure');
      await consumer.deliver(delivery.payload);
      await waitFor(() => store.terminalAppendStarted);
      store.releaseTerminalAppend();

      expect(consumer.committedOffsets).toEqual([]);
      await waitFor(() => consumer.seeks.length === 1);
      expect(consumer.committedOffsets).toEqual([]);
      expect(consumer.seeks[0]).toEqual({ topic: FIRST_TOPIC, partition: 0, offset: '0' });

      await consumer.deliver(userMessageDelivery(FIRST_TOPIC, 'evt_terminal_failure').payload);
      await waitFor(() => consumer.committedOffsets.length === 1);
      expect(store.appended.some((event) => event.kind === 'session.user_event_completed')).toBe(
        true,
      );
    } finally {
      store.releaseTerminalAppend();
      await dispatcher.stop();
    }
  });

  it('uses interruptible exponential backoff for repeated poison delivery failures', async () => {
    vi.useFakeTimers();
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka);
    (
      dispatcher as unknown as {
        onMessage(payload: unknown): Promise<void>;
      }
    ).onMessage = vi.fn(async () => {
      throw new Error('poison event');
    });

    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;

      await consumer.deliver(userMessageDelivery(FIRST_TOPIC, 'evt_poison').payload);
      await vi.advanceTimersByTimeAsync(249);
      expect(consumer.seeks).toHaveLength(0);
      await vi.advanceTimersByTimeAsync(1);
      expect(consumer.seeks).toHaveLength(1);

      await consumer.deliver(userMessageDelivery(FIRST_TOPIC, 'evt_poison').payload);
      await vi.advanceTimersByTimeAsync(499);
      expect(consumer.seeks).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(consumer.seeks).toHaveLength(2);

      const staleDelivery = userMessageDelivery(FIRST_TOPIC, 'evt_poison');
      await consumer.deliver(staleDelivery.payload);
      await vi.advanceTimersByTimeAsync(0);
      consumer.emitRebalancing();
      await vi.advanceTimersByTimeAsync(0);
      expect(consumer.seeks).toHaveLength(2);
      expect(staleDelivery.resumed()).toBe(false);
    } finally {
      await dispatcher.stop();
      vi.useRealTimers();
    }
  });

  it('keeps old consumer tracked when its disconnect fails before a replacement can run', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    kafka.plans = [{ disconnectFailures: 1 }, {}, {}];
    const dispatcher = makeDispatcher(kafka);
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await dispatcher.start();
      const oldConsumer = kafka.consumers[0]!;
      kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];

      await rediscover(dispatcher);

      const candidate = kafka.consumers[1]!;
      expect(oldConsumer.disconnectCalls).toBe(1);
      expect(oldConsumer.running).toBe(true);
      expect(candidate.runCalls).toBe(0);
      expect(candidate.disconnected).toBe(true);
      expect(activeTopics(dispatcher)).toEqual([FIRST_TOPIC]);
      expect(kafka.duplicateRunAttempts).toBe(0);

      await rediscover(dispatcher);
      expect(kafka.consumers[2]?.runCalls).toBe(1);
      expect(kafka.duplicateRunAttempts).toBe(0);
    } finally {
      consoleError.mockRestore();
      await dispatcher.stop();
    }
  });

  it('keeps a failed running candidate tracked until a retry retires it', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    kafka.plans = [{}, { join: 'never', disconnectFailures: 1 }, {}];
    const dispatcher = makeDispatcher(kafka, { kafkaGroupJoinTimeoutMs: 5 });
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await dispatcher.start();
      kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];

      await rediscover(dispatcher);

      const failedCandidate = kafka.consumers[1]!;
      expect(failedCandidate.running).toBe(true);
      expect(failedCandidate.disconnected).toBe(false);
      expect(consumerPointer(dispatcher)).toBe(failedCandidate);
      expect(kafka.consumers).toHaveLength(2);

      await rediscover(dispatcher);

      expect(failedCandidate.disconnected).toBe(true);
      expect(kafka.consumers[2]?.runCalls).toBe(1);
      expect(kafka.duplicateRunAttempts).toBe(0);
    } finally {
      consoleError.mockRestore();
      await dispatcher.stop();
    }
  });

  it('bounds shutdown when candidate connect never resolves and ignores late GROUP_JOIN', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    kafka.plans = [{}, { connect: 'blocked' }];
    const dispatcher = makeDispatcher(kafka, { kafkaShutdownGraceMs: 20 });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await dispatcher.start();
      kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];
      const transition = rediscover(dispatcher);
      await waitFor(() => kafka.consumers.length === 2);
      const candidate = kafka.consumers[1]!;
      await waitFor(() => candidate.connectCalls === 1);

      const startedAt = Date.now();
      await dispatcher.stop();

      expect(Date.now() - startedAt).toBeLessThan(500);
      expect(errorSpy).toHaveBeenCalledWith(
        'dispatcher: Kafka shutdown grace exceeded; continuing cleanup',
      );
      expect(errorSpy).toHaveBeenCalledTimes(1);
      expect(dispatcher.readiness()).toEqual({ ready: false, reasons: ['dispatcher_stopping'] });
      expect(consumerPointer(dispatcher)).toBeNull();
      expect(activeTopics(dispatcher)).toEqual([]);

      candidate.releaseConnect();
      await transition;
      expect(candidate.subscriptions).toEqual([]);
      expect(candidate.runCalls).toBe(0);
      candidate.emitGroupJoin({ [FIRST_TOPIC]: [0] });
      expect(consumerPointer(dispatcher)).toBeNull();
      expect(dispatcher.readiness()).toEqual({ ready: false, reasons: ['dispatcher_stopping'] });
    } finally {
      errorSpy.mockRestore();
      await dispatcher.stop();
    }
  });

  it('ignores a late metadata snapshot after the same dispatcher restarts', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka, { shutdownGraceMs: 20 });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await dispatcher.start();
      kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];
      const blocked = kafka.blockNextListTopics();
      const oldTransition = rediscover(dispatcher);
      await waitFor(() => blocked.started());

      await dispatcher.stop();
      kafka.topics = [FIRST_TOPIC];
      await dispatcher.start();
      expect(knownTopics(dispatcher)).toEqual([FIRST_TOPIC]);
      expect(activeTopics(dispatcher)).toEqual([FIRST_TOPIC]);

      blocked.release();
      await oldTransition;

      expect(knownTopics(dispatcher)).toEqual([FIRST_TOPIC]);
      expect(activeTopics(dispatcher)).toEqual([FIRST_TOPIC]);
      expect(kafka.consumers).toHaveLength(2);
    } finally {
      errorSpy.mockRestore();
      await dispatcher.stop();
    }
  });

  it('ignores a late empty-snapshot disconnect failure after restart', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka, { shutdownGraceMs: 20 });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await dispatcher.start();
      const oldConsumer = kafka.consumers[0]!;
      oldConsumer.holdInFlightHandler();
      kafka.topics = [];
      const oldTransition = rediscover(dispatcher);
      await waitFor(() => oldConsumer.disconnectCalls === 1);

      await dispatcher.stop();
      kafka.topics = [FIRST_TOPIC];
      await dispatcher.start();
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });

      oldConsumer.releaseInFlightHandler();
      await oldTransition;

      expect(knownTopics(dispatcher)).toEqual([FIRST_TOPIC]);
      expect(activeTopics(dispatcher)).toEqual([FIRST_TOPIC]);
      expect(dispatcher.readiness()).toEqual({ ready: true, reasons: [] });
    } finally {
      errorSpy.mockRestore();
      await dispatcher.stop();
    }
  });

  it('bounds shutdown when old consumer disconnect waits for an in-flight handler', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka, { kafkaShutdownGraceMs: 20 });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await dispatcher.start();
      const oldConsumer = kafka.consumers[0]!;
      oldConsumer.holdInFlightHandler();
      kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];
      const transition = rediscover(dispatcher);
      await waitFor(() => oldConsumer.disconnectCalls === 1);
      const candidate = kafka.consumers[1]!;

      const startedAt = Date.now();
      await dispatcher.stop();

      expect(Date.now() - startedAt).toBeLessThan(500);
      expect(oldConsumer.disconnectCalls).toBe(1);
      await waitFor(() => candidate.disconnected);
      expect(dispatcher.readiness()).toEqual({ ready: false, reasons: ['dispatcher_stopping'] });
      expect(consumerPointer(dispatcher)).toBeNull();

      oldConsumer.releaseInFlightHandler();
      await transition;
      expect(candidate.runCalls).toBe(0);
      candidate.emitGroupJoin({ [SECOND_TOPIC]: [0] });
      expect(consumerPointer(dispatcher)).toBeNull();
      expect(activeTopics(dispatcher)).toEqual([]);
    } finally {
      errorSpy.mockRestore();
      await dispatcher.stop();
    }
  });

  it('bounds final consumer disconnect when its in-flight handler never settles', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    const dispatcher = makeDispatcher(kafka, { kafkaShutdownGraceMs: 20 });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await dispatcher.start();
      const consumer = kafka.consumers[0]!;
      consumer.holdInFlightHandler();

      const startedAt = Date.now();
      await dispatcher.stop();

      expect(Date.now() - startedAt).toBeLessThan(500);
      expect(dispatcher.readiness()).toEqual({ ready: false, reasons: ['dispatcher_stopping'] });
      expect(consumerPointer(dispatcher)).toBeNull();
      consumer.releaseInFlightHandler();
      consumer.emitGroupJoin({ [FIRST_TOPIC]: [0] });
      expect(consumerPointer(dispatcher)).toBeNull();
      expect(dispatcher.readiness()).toEqual({ ready: false, reasons: ['dispatcher_stopping'] });
    } finally {
      errorSpy.mockRestore();
      await dispatcher.stop();
    }
  });

  it('cancels an in-flight candidate GROUP_JOIN wait during shutdown', async () => {
    const kafka = new FakeKafka([FIRST_TOPIC]);
    kafka.plans = [{}, { join: 'never' }];
    const dispatcher = makeDispatcher(kafka, { kafkaGroupJoinTimeoutMs: 10_000 });

    await dispatcher.start();
    kafka.topics = [FIRST_TOPIC, SECOND_TOPIC];
    const transition = rediscover(dispatcher);
    await waitFor(() => kafka.consumers.length === 2);
    const candidate = kafka.consumers[1]!;
    await waitFor(() => candidate.runCalls === 1);

    await dispatcher.stop();
    await transition;

    expect(candidate.disconnected).toBe(true);
    expect(consumerPointer(dispatcher)).toBeNull();
  });
});

function codecFixture(): KafkaTranscriptCodec {
  return {
    prepareWriter: vi.fn(async () => {}),
    encode: vi.fn(async () => ({ value: null })),
    close: vi.fn(async () => {}),
    decode: vi.fn(async () => ({
      id: 'evt_codec',
      workspaceId: 'ws_dispatcher',
      sessionId: 'ses_first',
      subpath: '',
      seq: 0,
      producedAt: '2026-09-11T00:00:00Z',
      producedBy: 'client',
      kind: 'user.message',
      idempotencyKey: '',
      payload: Buffer.from('{"content":[{"type":"text","text":"decoded"}]}'),
    })),
  };
}

interface ConsumerPlan {
  subscribeError?: Error;
  connect?: 'immediate' | 'blocked';
  disconnectFailures?: number;
  join?: 'immediate' | 'never' | 'crash';
}

function makeDispatcher(
  kafka: FakeKafka,
  extra: Partial<ConstructorParameters<typeof Dispatcher>[0]> = {},
): Dispatcher {
  return new Dispatcher({
    kafka: kafka as never,
    groupId: 'dispatcher-kafka-discovery-unit',
    store: {},
    anthropicApiKey: 'unused',
    modelDefault: 'fake',
    topicRediscoverIntervalMs: 60_000,
    ...extra,
  } as ConstructorParameters<typeof Dispatcher>[0]);
}

function makeTurnDispatcher(
  kafka: FakeKafka,
  store: TranscriptStore,
  options: Pick<
    ConstructorParameters<typeof Dispatcher>[0],
    'sessionIdleTimeoutMs' | 'harnessFactory' | 'codec'
  > = {},
): Dispatcher {
  return new Dispatcher({
    kafka: kafka as never,
    groupId: 'dispatcher-kafka-turn-unit',
    store,
    anthropicApiKey: 'unused',
    modelDefault: 'fake',
    topicRediscoverIntervalMs: 60_000,
    harnessFactory: () => new TerminalHarness(),
    ...options,
  });
}

async function rediscover(dispatcher: Dispatcher): Promise<void> {
  await (dispatcher as unknown as { rediscoverTopics(): Promise<void> }).rediscoverTopics();
}

function activeTopics(dispatcher: Dispatcher): string[] {
  return [
    ...(
      dispatcher as unknown as {
        subscribedTopics: Set<string>;
      }
    ).subscribedTopics,
  ].sort();
}

function knownTopics(dispatcher: Dispatcher): string[] {
  return [
    ...(
      dispatcher as unknown as {
        knownTopics: Set<string>;
      }
    ).knownTopics,
  ].sort();
}

function consumerPointer(dispatcher: Dispatcher): unknown {
  return (dispatcher as unknown as { consumer: unknown }).consumer;
}

async function expectKafkaOffsetLag(expected: number): Promise<void> {
  const values = (await harnessKafkaOffsetLag.get()).values;
  expect(values).toEqual([expect.objectContaining({ value: expected })]);
}

class FakeKafka {
  readonly consumers: FakeConsumer[] = [];
  readonly consumerConfigs: ConsumerConfig[] = [];
  topics: string[];
  plans: ConsumerPlan[] = [];
  onSubscribe: ((consumer: FakeConsumer, topics: string[]) => void) | undefined;
  duplicateRunAttempts = 0;
  listTopicFailures = 0;
  private nextListTopicsBlock:
    | {
        captured: string[];
        started: boolean;
        promise: Promise<void>;
        release(): void;
      }
    | undefined;

  constructor(topics: string[]) {
    this.topics = topics;
  }

  admin() {
    return {
      connect: async () => {},
      listTopics: async () => {
        if (this.listTopicFailures > 0) {
          this.listTopicFailures -= 1;
          throw new Error('metadata unavailable');
        }
        const block = this.nextListTopicsBlock;
        if (block) {
          this.nextListTopicsBlock = undefined;
          block.started = true;
          await block.promise;
          return [...block.captured];
        }
        return [...this.topics];
      },
      disconnect: async () => {},
    };
  }

  consumer(config: ConsumerConfig) {
    this.consumerConfigs.push(config);
    const consumer = new FakeConsumer(
      this,
      this.consumers.length,
      this.plans[this.consumers.length],
    );
    this.consumers.push(consumer);
    return consumer;
  }

  blockNextListTopics(): { started(): boolean; release(): void } {
    let releasePromise: () => void = () => {};
    const block = {
      captured: [...this.topics],
      started: false,
      promise: new Promise<void>((resolve) => {
        releasePromise = resolve;
      }),
      release: () => releasePromise(),
    };
    this.nextListTopicsBlock = block;
    return {
      started: () => block.started,
      release: block.release,
    };
  }
}

class FakeConsumer {
  readonly events = {
    GROUP_JOIN: 'consumer.group_join',
    REBALANCING: 'consumer.rebalancing',
    CRASH: 'consumer.crash',
    END_BATCH_PROCESS: 'consumer.end_batch_process',
  };
  readonly subscriptions: string[][] = [];
  readonly index: number;
  disconnected = false;
  running = false;
  runCalls = 0;
  connectCalls = 0;
  disconnectCalls = 0;
  readonly committedOffsets: Array<Array<{ topic: string; partition: number; offset: string }>> =
    [];
  readonly seeks: Array<{ topic: string; partition: number; offset: string }> = [];
  beforeCommit: (() => boolean | void) | null = null;
  private eachMessage: ((payload: never) => Promise<void>) | undefined;
  private readonly listeners = new Map<string, Array<(event: { payload: unknown }) => void>>();
  private inFlightHandler: Promise<void> | null = null;
  private releaseHandler: (() => void) | null = null;
  private releaseConnectCallback: (() => void) | null = null;
  private disconnectFailures: number;

  constructor(
    private readonly kafka: FakeKafka,
    index: number,
    private readonly plan: ConsumerPlan = {},
  ) {
    this.index = index;
    this.disconnectFailures = plan.disconnectFailures ?? 0;
  }

  async connect(): Promise<void> {
    this.connectCalls += 1;
    if (this.plan.connect !== 'blocked') return;
    await new Promise<void>((resolve) => {
      this.releaseConnectCallback = resolve;
    });
  }

  async subscribe(subscription: { topics?: string[] }): Promise<void> {
    if (this.plan.subscribeError) throw this.plan.subscribeError;
    const topics = [...(subscription.topics ?? [])];
    this.subscriptions.push(topics);
    this.kafka.onSubscribe?.(this, topics);
  }

  async run(options?: { eachMessage?: (payload: never) => Promise<void> }): Promise<void> {
    this.runCalls += 1;
    this.eachMessage = options?.eachMessage;
    if (this.kafka.consumers.some((consumer) => consumer !== this && consumer.running)) {
      this.kafka.duplicateRunAttempts += 1;
    }
    this.running = true;
    if (this.plan.join === 'crash') {
      await this.disconnect();
      this.emitCrash();
      return;
    }
    if (this.plan.join !== 'never') this.emitGroupJoin();
  }

  async disconnect(): Promise<void> {
    this.disconnectCalls += 1;
    if (this.disconnectFailures > 0) {
      this.disconnectFailures -= 1;
      throw new Error('planned disconnect failure');
    }
    await this.inFlightHandler;
    this.running = false;
    this.disconnected = true;
  }

  async deliver(payload: never): Promise<void> {
    if (!this.eachMessage) throw new Error('consumer eachMessage handler is not configured');
    await this.eachMessage(payload);
  }

  async commitOffsets(
    offsets: Array<{ topic: string; partition: number; offset: string }>,
  ): Promise<void> {
    if (this.beforeCommit?.() === false) return;
    this.committedOffsets.push(offsets.map((offset) => ({ ...offset })));
  }

  seek(offset: { topic: string; partition: number; offset: string }): void {
    this.seeks.push({ ...offset });
  }

  on(eventName: string, listener: (event: { payload: unknown }) => void): () => void {
    const listeners = this.listeners.get(eventName) ?? [];
    listeners.push(listener);
    this.listeners.set(eventName, listeners);
    return () => {
      const remaining = this.listeners
        .get(eventName)
        ?.filter((candidate) => candidate !== listener);
      if (remaining) this.listeners.set(eventName, remaining);
    };
  }

  emitGroupJoin(assignment?: Record<string, number[]>): void {
    const effectiveAssignment =
      assignment ??
      Object.fromEntries(this.subscriptions.at(-1)?.map((topic) => [topic, [0]]) ?? []);
    this.emit(this.events.GROUP_JOIN, { memberAssignment: effectiveAssignment });
  }

  emitRebalancing(): void {
    this.emit(this.events.REBALANCING, {});
  }

  emitCrash(): void {
    this.emit(this.events.CRASH, { error: new Error('consumer crashed'), restart: true });
  }

  emitEndBatchProcess(offsetLag: string): void {
    this.emit(this.events.END_BATCH_PROCESS, { offsetLag });
  }

  releaseConnect(): void {
    this.releaseConnectCallback?.();
    this.releaseConnectCallback = null;
  }

  holdInFlightHandler(): void {
    this.inFlightHandler = new Promise<void>((resolve) => {
      this.releaseHandler = resolve;
    });
  }

  releaseInFlightHandler(): void {
    this.releaseHandler?.();
    this.releaseHandler = null;
    this.inFlightHandler = null;
  }

  private emit(eventName: string, payload: unknown): void {
    for (const listener of this.listeners.get(eventName) ?? []) listener({ payload });
  }
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < 1_000) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error('condition not met');
}

function fakeDelivery(
  topic: string,
  eventId: string,
): {
  payload: never;
  paused(): boolean;
  resumed(): boolean;
} {
  let paused = false;
  let resumed = false;
  return {
    payload: {
      topic,
      partition: 0,
      message: {
        offset: '0',
        headers: { id: Buffer.from(eventId) },
        value: Buffer.from('{}'),
      },
      heartbeat: async () => {},
      pause: () => {
        paused = true;
        return () => {
          resumed = true;
        };
      },
    } as never,
    paused: () => paused,
    resumed: () => resumed,
  };
}

function userMessageDelivery(topic: string, eventId: string): ReturnType<typeof fakeDelivery> {
  const delivery = fakeDelivery(topic, eventId);
  const payload = delivery.payload as unknown as {
    message: { headers: Record<string, Buffer>; value: Buffer };
  };
  payload.message.headers = {
    id: Buffer.from(eventId),
    workspace_id: Buffer.from('ws_dispatcher'),
    session_id: Buffer.from('ses_first'),
    produced_by: Buffer.from('client'),
    produced_at: Buffer.from('2026-08-27T00:00:00.000Z'),
    kind: Buffer.from('user.message'),
  };
  payload.message.value = Buffer.from(JSON.stringify({ content: [{ type: 'text', text: 'go' }] }));
  return delivery;
}

class TerminalGateStore implements TranscriptStore {
  readonly appended: Event[] = [];
  terminalAppendStarted = false;
  private release: (() => void) | null = null;
  private readonly released = new Promise<void>((resolve) => {
    this.release = resolve;
  });

  private terminalFailuresRemaining: number;

  constructor(failTerminal = false) {
    this.terminalFailuresRemaining = failTerminal ? 1 : 0;
  }

  async append(_workspaceId: string, _sessionId: string, events: Event[]): Promise<string[]> {
    if (events.some((event) => event.kind === SessionEventKind.statusIdle)) {
      this.terminalAppendStarted = true;
      await this.released;
      if (this.terminalFailuresRemaining > 0) {
        this.terminalFailuresRemaining -= 1;
        throw new Error('terminal append failed');
      }
    }
    this.appended.push(...events.map((event) => ({ ...event })));
    return events.map((event) => event.id);
  }

  read(_workspaceId: string, _sessionId: string, _opts: ReadOptions): AsyncIterable<Event> {
    return emptyEvents();
  }

  tail(_workspaceId: string, _sessionId: string, _opts: TailOptions): AsyncIterable<Event> {
    return emptyEvents();
  }

  async archive(): Promise<void> {}
  async close(): Promise<void> {}

  releaseTerminalAppend(): void {
    this.release?.();
  }
}

class TerminalHarness implements AgentHarness {
  private readonly queue: AgentEvent[] = [];
  private readonly waiters: Array<(event: AgentEvent | null) => void> = [];
  private eventSequence = 0;
  private stopped = false;

  async start(_input: SessionStartInput): Promise<void> {
    void _input;
  }

  async submit(event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (event.kind !== 'user.message') return;
    await hooks?.onAccepted();
    this.emit({
      id: `evt_terminal_harness_${++this.eventSequence}`,
      subpath: '',
      kind: SessionEventKind.statusRunning,
      payload: {},
    });
    this.emit({
      id: `evt_terminal_harness_${++this.eventSequence}`,
      subpath: '',
      kind: SessionEventKind.statusIdle,
      payload: { stop_reason: { type: 'end_turn' } },
    });
  }

  async stop(_reason: TerminationReason): Promise<void> {
    void _reason;
    this.stopped = true;
    for (const resolve of this.waiters.splice(0)) resolve(null);
  }

  async *events(): AsyncIterable<AgentEvent> {
    while (!this.stopped || this.queue.length > 0) {
      const queued = this.queue.shift();
      if (queued) {
        yield queued;
        continue;
      }
      const next = await new Promise<AgentEvent | null>((resolve) => this.waiters.push(resolve));
      if (next === null) return;
      yield next;
    }
  }

  private emit(event: AgentEvent): void {
    const waiter = this.waiters.shift();
    if (waiter) waiter(event);
    else this.queue.push(event);
  }
}

class GuardrailFailureHarness extends TerminalHarness {
  constructor(
    private readonly failure: Error,
    private readonly accept: boolean,
  ) {
    super();
  }

  override async submit(_event: UserEvent, hooks?: SubmitHooks): Promise<void> {
    if (this.accept) await hooks?.onAccepted();
    throw this.failure;
  }

  hasPendingGuardrailUsage(): boolean {
    return this.failure instanceof GuardrailUsageUnavailableError;
  }
}

function emptyEvents(): AsyncIterable<Event> {
  return {
    [Symbol.asyncIterator]: () => ({
      next: async (): Promise<IteratorResult<Event>> => ({ done: true, value: undefined as never }),
    }),
  };
}
