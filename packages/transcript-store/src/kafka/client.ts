// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Kafka, logLevel, type KafkaConfig } from 'kafkajs';

let singleton: Kafka | null = null;

export interface KafkaClientOptions {
  brokers: string[];
  clientId: string;
  ssl?: KafkaConfig['ssl'];
  sasl?: KafkaConfig['sasl'];
}

export function getKafka(opts: KafkaClientOptions): Kafka {
  if (singleton) return singleton;
  const config: KafkaConfig = {
    brokers: opts.brokers,
    clientId: opts.clientId,
    logLevel: logLevel.WARN,
  };
  if (opts.ssl !== undefined) config.ssl = opts.ssl;
  if (opts.sasl !== undefined) config.sasl = opts.sasl;
  singleton = new Kafka(config);
  return singleton;
}

export function resetKafkaForTests(): void {
  singleton = null;
}
