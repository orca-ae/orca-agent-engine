// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Kafka } from 'kafkajs';
import { KafkaTranscriptStore } from '@orca/transcript-store';

const KAFKA_BROKER = process.env['KAFKA_BROKERS'] ?? 'localhost:9092';

export function buildTestStore(): KafkaTranscriptStore {
  const kafka = new Kafka({
    clientId: 'harness-it',
    brokers: [KAFKA_BROKER],
  });
  return new KafkaTranscriptStore({ kafka });
}

export function uniqueIds(prefix: string): { ws: string; ses: string } {
  const suffix = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  return { ws: `ws_${prefix}_${suffix}`, ses: `ses_${prefix}_${suffix}` };
}
