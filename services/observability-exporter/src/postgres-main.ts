// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import {
  createKafkaTranscriptCodec,
  KafkaSessionEventSource,
  KafkaTranscriptStore,
} from '@orca/transcript-store';
import { buildExporterPoolConfig, type ExporterConfig } from './config.js';
import { buildInternalServiceTokenProvider } from './internal-service-token.js';
import {
  applyObservabilityExporterMigrations,
  ObservabilityExporterRepository,
} from './persistence.js';
import { RegistryObservabilityClient } from './registry-client.js';
import { KafkaObservabilityExporterRuntime } from './runtime.js';
import { createExporterHealthServer } from './health.js';

/** Explicit migration compatibility path; the broker runtime never loads this module. */
export async function runPostgresExporter(config: ExporterConfig): Promise<void> {
  const pool = new Pool(buildExporterPoolConfig(config));
  const kafka = new Kafka(config.kafka);
  const codec = createKafkaTranscriptCodec(config.kafkaTranscript ?? {});
  const encoding = codec.encoding ?? 'raw';
  const transcriptStore = new KafkaTranscriptStore({
    kafka,
    topicPrefix: config.kafkaTopicPrefix,
    codec,
  });
  const eventSource = new KafkaSessionEventSource({
    kafka,
    codec,
    groupId: 'observability-exporter-inbox' + (encoding === 'avro' ? '-avro' : ''),
    topicPrefix: config.kafkaTopicPrefix,
    topicDiscoveryIntervalMs: config.kafkaTopicDiscoveryIntervalMs,
  });
  const tokenProvider = buildInternalServiceTokenProvider({
    ...(config.internalServiceToken === undefined ? {} : { token: config.internalServiceToken }),
    ...(config.internalServiceTokenFile === undefined
      ? {}
      : { tokenFile: config.internalServiceTokenFile }),
  });
  const runtime = new KafkaObservabilityExporterRuntime({
    repository: new ObservabilityExporterRepository(pool),
    transcriptStore,
    eventSource,
    registryClient: new RegistryObservabilityClient({
      internalBaseUrl: config.registryInternalBaseUrl,
      tokenProvider,
    }),
    workerId: config.workerId,
    projectorLeaseMs: config.projectorLeaseMs,
    projectorBatchSize: config.projectorBatchSize,
    projectorBatchBytes: config.projectorBatchBytes,
    deliveryLeaseMs: config.deliveryLeaseMs,
    registryRequestTimeoutMs: config.registryRequestTimeoutMs,
    projectorPollMs: config.projectorPollMs,
    deliveryPollMs: config.deliveryPollMs,
  });
  const controller = new AbortController();
  let initialized = false;
  const health = createExporterHealthServer({
    isLive: () => !controller.signal.aborted && eventSource.status().state !== 'failed',
    checkReady: async () => {
      if (!initialized || !eventSource.status().ready) return false;
      await pool.query('SELECT 1');
      return eventSource.status().ready;
    },
  });
  let stoppingIngestion: Promise<void> | undefined;

  const stopIngestion = (): Promise<void> => {
    stoppingIngestion ??= runtime.stopIngestion().catch(() => undefined);
    return stoppingIngestion;
  };
  const onSignal = (): void => {
    controller.abort();
    void stopIngestion();
  };
  process.once('SIGTERM', onSignal);
  process.once('SIGINT', onSignal);

  try {
    await new Promise<void>((resolve, reject) => {
      health.once('error', reject);
      health.listen(8080, '0.0.0.0', resolve);
    });
    await applyObservabilityExporterMigrations(pool);
    if (controller.signal.aborted) return;
    await runtime.startIngestion();
    initialized = true;
    console.info('observability-exporter Kafka ingestion loop started');
    await runtime.run(controller.signal);
  } finally {
    process.off('SIGTERM', onSignal);
    process.off('SIGINT', onSignal);
    controller.abort();
    health.close();
    health.closeAllConnections();
    await stopIngestion();
    await transcriptStore.close().catch(() => undefined);
    try {
      await codec.close();
    } finally {
      await pool.end();
    }
  }
}
