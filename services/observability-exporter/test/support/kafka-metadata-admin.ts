// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createRequire } from 'node:module';
import kafkaJs, { type Admin, type Logger } from 'kafkajs';
import { vi } from 'vitest';

const require = createRequire(import.meta.url);
// Exercise the installed Admin -> Cluster -> BrokerPool path; only broker I/O is fake.
const Cluster = require('kafkajs/src/cluster') as new (options: {
  logger: Logger;
  brokers: string[];
  retry: { retries: number };
}) => {
  brokerPool: {
    seedBroker: unknown;
    brokers: Record<string, unknown>;
    metadata: unknown;
    metadataExpireAt: number | null;
  };
};
const createAdmin = require('kafkajs/src/admin') as (options: {
  cluster: InstanceType<typeof Cluster>;
  logger: Logger;
  retry: { retries: number };
}) => Admin;

export function metadataAdmin(topics: string[], warm = false) {
  const logger = new kafkaJs.Kafka({
    brokers: ['broker:9092'],
    logLevel: kafkaJs.logLevel.NOTHING,
  }).logger();
  const retry = { retries: 0 };
  const cluster = new Cluster({ logger, brokers: ['broker:9092'], retry });
  const response = {
    brokers: [{ nodeId: 0, host: 'broker', port: 9092 }],
    topicMetadata: topics.map((topic) => ({ topic, partitionMetadata: [] })),
  };
  const broker = {
    nodeId: 0,
    connectionPool: { host: 'broker', port: 9092 },
    isConnected: () => true,
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    metadata: vi.fn().mockResolvedValue(response),
  };
  cluster.brokerPool.seedBroker = broker;
  cluster.brokerPool.brokers = { 0: broker };
  if (warm) {
    cluster.brokerPool.metadata = response;
    cluster.brokerPool.metadataExpireAt = Date.now() + 60_000;
  }
  const admin = createAdmin({ cluster, logger, retry });
  return { admin, broker, response };
}
