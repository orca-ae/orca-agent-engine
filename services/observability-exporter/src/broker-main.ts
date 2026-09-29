// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { createKafkaTranscriptCodec, matchSessionTopic } from '@orca/transcript-store';
import kafkaJs, { Kafka } from 'kafkajs';
import type { ExporterConfig } from './config.js';
import { createExporterHealthServer } from './health.js';
import { buildInternalServiceTokenProvider } from './internal-service-token.js';
import { KafkaOnlyObservabilityExporterRuntime } from './kafka-runtime.js';
import { RegistryObservabilityClient } from './registry-client.js';

// KafkaJS's dynamically exported errors are only available on the CJS default in native ESM.
const {
  KafkaJSConnectionError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
  KafkaJSRequestTimeoutError,
} = kafkaJs;

/** Only metadata availability failures, never KafkaJS's broad `retriable` flag. */
function metadataRetryType(error: unknown): string | undefined {
  const cause = error instanceof KafkaJSNumberOfRetriesExceeded ? error.cause : error;
  if (
    cause instanceof KafkaJSProtocolError &&
    [
      'LEADER_NOT_AVAILABLE',
      'NOT_LEADER_FOR_PARTITION',
      'UNKNOWN_TOPIC_OR_PARTITION',
      'REQUEST_TIMED_OUT',
      'BROKER_NOT_AVAILABLE',
      'REPLICA_NOT_AVAILABLE',
      'NETWORK_EXCEPTION',
    ].includes(cause.type)
  )
    return cause.type;
  if (cause instanceof KafkaJSRequestTimeoutError) return 'KafkaJSRequestTimeoutError';
  // KafkaJS exports this subclass at runtime but omits it from its TypeScript declarations.
  if (cause instanceof KafkaJSConnectionError && cause.name === 'KafkaJSConnectionClosedError')
    return cause.name;
  if (
    cause instanceof KafkaJSConnectionError &&
    'code' in cause &&
    typeof cause.code === 'string' &&
    [
      'ECONNRESET',
      'ECONNREFUSED',
      'ETIMEDOUT',
      'EPIPE',
      'EAI_AGAIN',
      'ENETUNREACH',
      'EHOSTUNREACH',
    ].includes(cause.code)
  )
    return cause.code;
  return undefined;
}

function isKafkaJsNullMetadata(error: unknown): error is TypeError {
  return (
    error instanceof TypeError &&
    error.message ===
      "Cannot destructure property 'topicMetadata' of '(intermediate value)' as it is null." &&
    /[/\\]kafkajs[/\\]src[/\\]admin[/\\]index\.js:\d+:\d+/.test(error.stack?.split('\n')[1] ?? '')
  );
}

/** Broker-backed process bootstrap. No SQL connection, migration, or repository. */
export async function runBrokerExporter(
  config: ExporterConfig,
  signal?: AbortSignal,
): Promise<void> {
  const kafka = new Kafka(config.kafka);
  const codec = createKafkaTranscriptCodec(config.kafkaTranscript ?? {});
  const encoding = codec.encoding ?? 'raw';
  const suffix = encoding === 'avro' ? '-avro' : '';
  const admin = kafka.admin();
  const prefix = config.kafkaTopicPrefix;
  const supportsBareAliases = config.kafkaTopicListingMode === 'bare-alias';
  const checkpointTopic = `${prefix}orca.observability.v1.checkpoints${suffix}`;
  const deliveryTopic = `${prefix}orca.observability.v1.delivery${suffix}`;
  const groupId = `observability-exporter-kafka-v1-${createHash('sha256').update(prefix).digest('hex').slice(0, 16)}${suffix}`;
  const tokenProvider = buildInternalServiceTokenProvider({
    ...(config.internalServiceToken === undefined ? {} : { token: config.internalServiceToken }),
    ...(config.internalServiceTokenFile === undefined
      ? {}
      : { tokenFile: config.internalServiceTokenFile }),
  });
  const runtime = new KafkaOnlyObservabilityExporterRuntime({
    kafka,
    codec,
    groupId,
    sessions: [],
    checkpointTopic,
    deliveryTopic,
    registryClient: new RegistryObservabilityClient({
      internalBaseUrl: config.registryInternalBaseUrl,
      tokenProvider,
    }),
    registryRequestTimeoutMs: config.registryRequestTimeoutMs,
    batchSize: config.projectorBatchSize,
    startupConcurrency: config.kafkaStartupConcurrency,
    restoreConcurrency: config.kafkaRestoreConcurrency,
    projectorConcurrency: config.kafkaProjectorConcurrency,
    ...(config.kafkaStateDirectory === undefined
      ? {}
      : { stateDirectory: config.kafkaStateDirectory }),
    stateMaxBytes: config.kafkaStateMaxBytes,
    stateCatchupTimeoutMs: config.kafkaStateCatchupTimeoutMs,
    maxAssemblyBytes: config.kafkaMaxAssemblyBytes,
    maxTransactionBytes: config.kafkaMaxTransactionBytes,
  });
  const controller = new AbortController();
  let runtimeStop: Promise<void> | undefined;
  const stopRuntime = (): Promise<void> => (runtimeStop ??= runtime.stop());
  const onSignal = (): void => {
    controller.abort();
    // Do not wait for discovery joins: stop immediately cancels queued runtime work.
    void stopRuntime().catch(() => undefined);
  };
  signal?.addEventListener('abort', onSignal, { once: true });
  if (signal?.aborted) onSignal();
  process.once('SIGTERM', onSignal);
  process.once('SIGINT', onSignal);
  let discovered = false;
  let propagatingFailure = false;
  const listTopicsOnce = async (deadline: number): Promise<string[]> => {
    try {
      return await admin.listTopics();
    } catch (error) {
      // KafkaJS 2.2.4 withBroker() swallows ALL broker errors and returns null.
      // Probe with an empty cache to recover an actual result/error; the TypeError
      // itself is not retriable, since it can also mask authorization failures.
      if (!isKafkaJsNullMetadata(error) || performance.now() >= deadline) throw error;
      controller.signal.throwIfAborted();
      if (runtime.status().state === 'failed') throw new Error('Kafka exporter runtime failed');
      const probe = kafka.admin();
      let failed = false;
      try {
        await probe.connect();
        controller.signal.throwIfAborted();
        if (runtime.status().state === 'failed') throw new Error('Kafka exporter runtime failed');
        if (performance.now() >= deadline) throw error;
        return await probe.listTopics();
      } catch (probeError) {
        failed = true;
        throw probeError;
      } finally {
        await probe.disconnect().catch((cleanupError: unknown) => {
          if (!failed) throw cleanupError;
        });
      }
    }
  };
  const listTopics = async (phase: 'startup' | 'discovery'): Promise<string[]> => {
    // KafkaJS calls have their own timeouts/retries and cannot take this signal.
    // Count their elapsed time, but do not abandon an in-flight admin operation.
    const deadline = performance.now() + 15 * 60_000;
    let backoffMs = 1000;
    let lastError: unknown;
    for (;;) {
      controller.signal.throwIfAborted();
      if (phase === 'discovery' && runtime.status().state === 'failed') {
        throw new Error('Kafka exporter runtime failed');
      }
      if (performance.now() >= deadline) throw lastError;
      try {
        return await listTopicsOnce(deadline);
      } catch (error) {
        const type = metadataRetryType(error);
        const remainingMs = deadline - performance.now();
        if (type === undefined || remainingMs <= 0) throw error;
        lastError = error;
        const retryMs = Math.min(backoffMs, remainingMs);
        console.warn(
          JSON.stringify({
            component: 'observability-exporter',
            code: 'metadata_retry',
            phase,
            type,
            delayMs: Math.ceil(retryMs),
          }),
        );
        await delay(retryMs, undefined, { signal: controller.signal });
        backoffMs = Math.min(backoffMs * 2, 30_000);
      }
    }
  };
  const health = createExporterHealthServer({
    isLive: () => !controller.signal.aborted && runtime.status().state !== 'failed',
    checkReady: async () => {
      if (!discovered || !runtime.status().ready) return false;
      await admin.listTopics();
      return runtime.status().ready && !controller.signal.aborted;
    },
  });
  try {
    if (controller.signal.aborted) return;
    await new Promise<void>((resolve, reject) => {
      health.once('error', reject);
      health.listen(8080, '0.0.0.0', resolve);
    });
    await admin.connect();
    if (controller.signal.aborted) return;
    const listed = new Set(await listTopics('startup'));
    if (controller.signal.aborted) return;
    const topics = [
      {
        topic: checkpointTopic,
        numPartitions: 1,
        configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
      },
      {
        topic: deliveryTopic,
        numPartitions: 8,
        configEntries: [
          { name: 'cleanup.policy', value: 'delete' },
          { name: 'retention.ms', value: '-1' },
          { name: 'retention.bytes', value: '-1' },
        ],
      },
    ].filter(
      ({ topic }) =>
        !listed.has(topic) &&
        !(supportsBareAliases && prefix && listed.has(topic.slice(prefix.length))),
    );
    if (topics.length > 0) await admin.createTopics({ waitForLeaders: true, topics });
    if (controller.signal.aborted) return;
    await runtime.start();
    while (!controller.signal.aborted) {
      if (runtime.status().state === 'failed') throw new Error('Kafka exporter runtime failed');
      const routes = (await listTopics('discovery')).flatMap((topic) => {
        // Listings may omit the namespace prefix only with explicit alias support (e.g. KoP).
        if (!supportsBareAliases && !topic.startsWith(prefix)) return [];
        const match = matchSessionTopic(topic, prefix, encoding);
        return match === null
          ? []
          : [
              {
                topic: match.canonicalTopic,
                workspaceId: match.workspaceId,
                sessionId: match.sessionId,
              },
            ];
      });
      if (controller.signal.aborted) break;
      await runtime.addSessions(routes);
      if (controller.signal.aborted) break;
      discovered = true;
      await delay(config.kafkaTopicDiscoveryIntervalMs, undefined, {
        signal: controller.signal,
      }).catch((error: unknown) => {
        if (!controller.signal.aborted) throw error;
      });
    }
  } catch (error) {
    // Runtime cancellation during connect/join is an ordinary signal shutdown.
    if (!controller.signal.aborted || runtime.status().state === 'failed') {
      propagatingFailure = true;
      throw error;
    }
  } finally {
    process.off('SIGTERM', onSignal);
    process.off('SIGINT', onSignal);
    signal?.removeEventListener('abort', onSignal);
    controller.abort();
    health.close();
    health.closeAllConnections();
    const cleanup = await Promise.allSettled([
      stopRuntime().finally(() => codec.close()),
      admin.disconnect(),
    ]);
    // Check after the drain, inside finally so early returns cannot hide a late
    // runtime failure. Never replace an initiating try failure with cleanup status.
    if (!propagatingFailure) {
      // eslint-disable-next-line no-unsafe-finally -- Only overrides a successful return, never a thrown failure.
      if (runtime.status().state === 'failed') throw new Error('Kafka exporter runtime failed');
      if (cleanup.some((result) => result.status === 'rejected')) {
        // eslint-disable-next-line no-unsafe-finally -- Only overrides a successful return, never a thrown failure.
        throw new Error('Kafka exporter resource cleanup failed');
      }
    }
  }
}
