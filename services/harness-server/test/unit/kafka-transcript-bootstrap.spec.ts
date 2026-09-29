// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Kafka } from 'kafkajs';
import type { KafkaTranscriptCodec } from '@orca/transcript-store';
import { buildKafkaTranscriptBackend } from '../../src/kafka-transcript-bootstrap.js';

const dependencies = vi.hoisted(() => ({
  createCodec: vi.fn(),
  store: vi.fn(),
  source: vi.fn(),
}));
vi.mock('@orca/transcript-store', () => ({
  createKafkaTranscriptCodec: dependencies.createCodec,
  KafkaTranscriptStore: dependencies.store,
  KafkaSessionEventSource: dependencies.source,
}));

const kafka = new Kafka({ brokers: ['localhost:9092'] });
let codec: KafkaTranscriptCodec;
beforeEach(() => {
  vi.resetAllMocks();
  codec = {
    prepareWriter: vi.fn().mockResolvedValue(undefined),
    encode: vi.fn(),
    decode: vi.fn(),
    close: vi.fn().mockResolvedValue(undefined),
  };
  dependencies.createCodec.mockReturnValue(codec);
});

describe('Kafka transcript bootstrap', () => {
  it('given pending writer preparation, does not expose a backend or construct consumers', async () => {
    let ready!: () => void;
    vi.mocked(codec.prepareWriter).mockReturnValue(
      new Promise<void>((resolve) => {
        ready = resolve;
      }),
    );
    let exposed = false;
    const pending = buildKafkaTranscriptBackend(kafka, 'test.').then((backend) => {
      exposed = true;
      return backend;
    });
    await Promise.resolve();
    expect(exposed).toBe(false);
    expect(dependencies.store).not.toHaveBeenCalled();
    expect(dependencies.source).not.toHaveBeenCalled();
    ready();
    await pending;
    expect(exposed).toBe(true);
  });

  it('shares one configured codec and leaves its shutdown to the caller', async () => {
    const options = { encoding: 'avro' as const };
    const backend = await buildKafkaTranscriptBackend(kafka, 'test.', options);
    expect(dependencies.createCodec).toHaveBeenCalledOnce();
    expect(dependencies.createCodec).toHaveBeenCalledWith(options);
    expect(codec.prepareWriter).toHaveBeenCalledOnce();
    expect(dependencies.store).toHaveBeenCalledOnce();
    expect(dependencies.store).toHaveBeenCalledWith({ kafka, topicPrefix: 'test.', codec });
    expect(backend.codec).toBe(codec);
    expect(backend.kafka).toBe(kafka);
    expect(codec.close).not.toHaveBeenCalled();
    await backend.close();
    expect(codec.close).toHaveBeenCalledOnce();
  });

  it('delegates absent configuration to the factory raw default', async () => {
    await buildKafkaTranscriptBackend(kafka, '');
    expect(dependencies.createCodec).toHaveBeenCalledOnce();
    expect(dependencies.createCodec).toHaveBeenCalledWith({});
    expect(codec.prepareWriter).toHaveBeenCalledOnce();
  });

  it('given preparation failure, closes the codec and rejects without creating a backend', async () => {
    const failure = new Error('writer unavailable');
    vi.mocked(codec.prepareWriter).mockRejectedValue(failure);
    await expect(buildKafkaTranscriptBackend(kafka, '')).rejects.toBe(failure);
    expect(codec.close).toHaveBeenCalledOnce();
    expect(dependencies.store).not.toHaveBeenCalled();
    expect(dependencies.source).not.toHaveBeenCalled();
  });

  it('preserves the preparation failure even if cleanup also fails', async () => {
    const failure = new Error('writer unavailable');
    vi.mocked(codec.prepareWriter).mockRejectedValue(failure);
    vi.mocked(codec.close).mockRejectedValue(new Error('cleanup failed'));
    await expect(buildKafkaTranscriptBackend(kafka, '')).rejects.toBe(failure);
    expect(codec.close).toHaveBeenCalledOnce();
  });
});
