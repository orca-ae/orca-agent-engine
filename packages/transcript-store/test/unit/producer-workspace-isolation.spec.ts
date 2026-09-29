// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Kafka, Producer } from 'kafkajs';
import { describe, expect, it, vi } from 'vitest';
import { TranscriptProducer } from '../../src/kafka/producer.js';
import { KafkaTranscriptStore } from '../../src/kafka-store.js';
import type { Event } from '../../src/types.js';

function event(workspaceId: string, sessionId: string, id: string): Event {
  return {
    id,
    workspaceId,
    sessionId,
    subpath: '',
    seq: 0,
    producedAt: new Date(0).toISOString(),
    producedBy: 'client',
    kind: 'user.message',
    payload: new Uint8Array([1]),
    idempotencyKey: '',
  };
}

describe('TranscriptProducer workspace isolation', () => {
  it.each([
    ['avro', 'public.default.orca.ws_a.sessions.ses_a.events-avro'],
    ['raw', 'public.default.orca.ws_a.sessions.ses_a.events'],
    [undefined, 'public.default.orca.ws_a.sessions.ses_a.events'],
  ] as const)(
    'routes append and archive using actual codec encoding %s',
    async (encoding, topic) => {
      const send = vi.fn().mockResolvedValue([{ baseOffset: '0' }]);
      const kafka = {
        producer: () => ({ connect: vi.fn(), disconnect: vi.fn(), send }),
      } as unknown as Kafka;
      const codec = {
        ...(encoding === undefined ? {} : { encoding }),
        prepareWriter: vi.fn(),
        encode: vi.fn().mockResolvedValue({ value: Buffer.from('encoded') }),
        decode: vi.fn(),
        close: vi.fn(),
      };
      const store = new KafkaTranscriptStore({ kafka, codec, topicPrefix: 'public.default.' });
      try {
        await store.append('ws_a', 'ses_a', [event('ws_a', 'ses_a', 'first')]);
        await store.archive('ws_a', 'ses_a');
        expect(send.mock.calls.map((call) => call[0].topic)).toEqual([topic, topic]);
      } finally {
        await store.close();
      }
    },
  );

  it('preserves append order when the first encoding waits for a schema', async () => {
    const send = vi.fn().mockResolvedValue([{ baseOffset: '0' }]);
    const kafka = {
      producer: () => ({ connect: vi.fn(), disconnect: vi.fn(), send }),
    } as unknown as Kafka;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const codec = {
      prepareWriter: vi.fn(),
      decode: vi.fn(),
      close: vi.fn(),
      encode: vi.fn(async (e: Event) => {
        if (e.id === 'first') await gate;
        return { key: e.id, value: Buffer.from(e.id) };
      }),
    };
    const producer = new TranscriptProducer(kafka, '', { codec });
    const first = producer.append('ws_a', 'ses_a', [event('ws_a', 'ses_a', 'first')]);
    const second = producer.append('ws_a', 'ses_a', [event('ws_a', 'ses_a', 'second')]);
    await new Promise((resolve) => setTimeout(resolve, 0));
    release();
    await Promise.all([first, second]);
    expect(send.mock.calls.map((call) => call[0].messages[0].key)).toEqual(['first', 'second']);
  });

  it('prepares the store writer and encodes archive through the shared caller-owned codec', async () => {
    const send = vi.fn().mockResolvedValue([{ baseOffset: '0' }]);
    const connect = vi.fn();
    const kafka = { producer: () => ({ connect, disconnect: vi.fn(), send }) } as unknown as Kafka;
    const codec = {
      prepareWriter: vi
        .fn()
        .mockRejectedValueOnce(new Error('writer unavailable'))
        .mockResolvedValue(undefined),
      encode: vi.fn().mockResolvedValue({ value: Buffer.from('archive') }),
      decode: vi.fn(),
      close: vi.fn(),
    };
    const store = new KafkaTranscriptStore({ kafka, codec });
    await expect(store.ensureConnected()).rejects.toThrow('writer unavailable');
    expect(connect).not.toHaveBeenCalled();
    await store.archive('ws_a', 'ses_a');
    expect(codec.encode).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: 'session.archived',
        payload: new Uint8Array(),
        workspaceId: 'ws_a',
        sessionId: 'ses_a',
      }),
    );
    expect(send.mock.calls[0]?.[0].messages).toEqual([{ value: Buffer.from('archive') }]);
    await store.close();
    expect(codec.close).not.toHaveBeenCalled();
  });

  it('encodes every fresh event before send and permits retry after encoding failure', async () => {
    const send = vi.fn().mockResolvedValue([{ baseOffset: '12' }]);
    const kafka = {
      producer: () => ({ connect: vi.fn(), disconnect: vi.fn(), send }),
    } as unknown as Kafka;
    const codec = {
      prepareWriter: vi.fn().mockResolvedValue(undefined),
      encode: vi
        .fn()
        .mockResolvedValueOnce({ value: Buffer.from('encoded-first') })
        .mockRejectedValueOnce(new Error('schema unavailable'))
        .mockResolvedValue({ value: Buffer.from('encoded-retry') }),
      decode: vi.fn(),
      close: vi.fn(),
    };
    const producer = new TranscriptProducer(kafka, '', { codec });
    const events = [event('ws_a', 'ses_a', 'first'), event('ws_a', 'ses_a', 'second')];
    await expect(producer.append('ws_a', 'ses_a', events)).rejects.toThrow('schema unavailable');
    expect(send).not.toHaveBeenCalled();
    expect(events.map((e) => e.seq)).toEqual([0, 0]);
    await producer.append('ws_a', 'ses_a', events);
    expect(send.mock.calls[0]?.[0].messages).toEqual([
      { value: Buffer.from('encoded-retry') },
      { value: Buffer.from('encoded-retry') },
    ]);
    expect(events.map((e) => e.seq)).toEqual([12, 13]);
    await producer.append('ws_a', 'ses_a', events);
    expect(send).toHaveBeenCalledTimes(1);
    await producer.disconnect();
    expect(codec.close).not.toHaveBeenCalled();
  });

  it('rejects events whose embedded route differs from the append target', async () => {
    const send = vi.fn();
    const producer = {
      connect: vi.fn(),
      disconnect: vi.fn(),
      send,
    } as unknown as Producer;
    const kafka = {
      producer: vi.fn(() => producer),
    } as unknown as Kafka;
    const transcriptProducer = new TranscriptProducer(kafka);

    await expect(
      transcriptProducer.append('ws_alpha', 'session_alpha', [
        event('ws_beta', 'session_alpha', 'event_forged_workspace'),
      ]),
    ).rejects.toThrow(/event route mismatch/);
    await expect(
      transcriptProducer.append('ws_alpha', 'session_alpha', [
        event('ws_alpha', 'session_beta', 'event_forged_session'),
      ]),
    ).rejects.toThrow(/event route mismatch/);
    expect(send).not.toHaveBeenCalled();
  });

  it('does not deduplicate matching session and event IDs across workspaces', async () => {
    const send = vi.fn().mockResolvedValue([{ baseOffset: '1' }]);
    const producer = {
      connect: vi.fn(),
      disconnect: vi.fn(),
      send,
    } as unknown as Producer;
    const kafka = {
      producer: vi.fn(() => producer),
    } as unknown as Kafka;
    const transcriptProducer = new TranscriptProducer(kafka);

    await transcriptProducer.append('ws_alpha', 'session_shared', [
      event('ws_alpha', 'session_shared', 'event_shared'),
    ]);
    await transcriptProducer.append('ws_beta', 'session_shared', [
      event('ws_beta', 'session_shared', 'event_shared'),
    ]);

    expect(send).toHaveBeenCalledTimes(2);
    expect(send.mock.calls[0]?.[0].topic).not.toBe(send.mock.calls[1]?.[0].topic);
  });
});
