// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Kafka } from 'kafkajs';
import {
  createKafkaTranscriptCodec,
  KafkaTranscriptStore,
  type KafkaTranscriptCodec,
  type KafkaTranscriptCodecOptions,
} from '@orca/transcript-store';

/** Prepare before exposing writers; the composition root owns the shared codec. */
export async function buildKafkaTranscriptBackend(
  kafka: Kafka,
  topicPrefix: string,
  options?: KafkaTranscriptCodecOptions,
): Promise<{
  kafka: Kafka;
  store: KafkaTranscriptStore;
  codec: KafkaTranscriptCodec;
  close(): Promise<void>;
}> {
  const codec = createKafkaTranscriptCodec(options ?? {});
  try {
    await codec.prepareWriter();
    return {
      kafka,
      store: new KafkaTranscriptStore({ kafka, topicPrefix, codec }),
      codec,
      // Dispatcher/store borrow the codec; close it after stopping both.
      close: () => codec.close(),
    };
  } catch (error) {
    await codec.close().catch(() => {});
    throw error;
  }
}
