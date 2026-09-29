// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import { isAbsolute } from 'node:path';
import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import {
  validateTopicPrefix,
  parseKafkaTranscriptConfig,
  type KafkaTranscriptCodecOptions,
} from '@orca/transcript-store';
import type { KafkaConfig } from 'kafkajs';
import type { PoolConfig } from 'pg';
import { MAX_WORKER_ID_LENGTH } from './lease-owner.js';

type KafkaConnectionMode = 'plaintext' | 'sasl-plain-token-tls' | 'sasl-plain-tls' | 'custom';

const KAFKA_CONNECTION_MODES: readonly KafkaConnectionMode[] = [
  'plaintext',
  'sasl-plain-token-tls',
  'sasl-plain-tls',
  'custom',
];
const TLS_KAFKA_TIMEOUT_MS = 10_000;
const MIN_PROJECTOR_LEASE_MS = 30_000;
const LEASE_COMPLETION_MARGIN_MS = 5_000;
const DEFAULT_PROJECTOR_BATCH_BYTES = 8 * 1024 * 1024;
const MAX_PROJECTOR_BATCH_BYTES = 64 * 1024 * 1024;

/** Shared configuration for broker-native and legacy Postgres exporter runtimes. */
export interface ExporterConfig {
  stateBackend: 'kafka' | 'postgres';
  databaseUrl?: string;
  databasePoolMax: number;
  databaseTimeoutMs: number;
  kafka: KafkaConfig;
  kafkaTranscript?: KafkaTranscriptCodecOptions;
  kafkaTopicListingMode: 'canonical' | 'bare-alias';
  kafkaTopicPrefix: string;
  kafkaTopicDiscoveryIntervalMs: number;
  kafkaStartupConcurrency: number;
  kafkaRestoreConcurrency: number;
  kafkaProjectorConcurrency: number;
  kafkaStateDirectory?: string;
  kafkaStateMaxBytes: number;
  kafkaStateCatchupTimeoutMs: number;
  kafkaMaxAssemblyBytes: number;
  kafkaMaxTransactionBytes: number;
  registryInternalBaseUrl: string;
  internalServiceToken?: string;
  internalServiceTokenFile?: string;
  workerId: string;
  projectorPollMs: number;
  projectorLeaseMs: number;
  projectorBatchSize: number;
  projectorBatchBytes: number;
  deliveryPollMs: number;
  deliveryLeaseMs: number;
  registryRequestTimeoutMs: number;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): ExporterConfig {
  assertKafkaTranscriptBackend(env['TRANSCRIPT_STORE_BACKEND']);
  if (
    optionalEnv(env, 'OBSERVABILITY_EXPORTER_STATE_BACKEND') === undefined &&
    optionalEnv(env, 'OBSERVABILITY_EXPORTER_DATABASE_URL') !== undefined
  ) {
    throw new Error(
      'Existing exporter database requires explicit OBSERVABILITY_EXPORTER_STATE_BACKEND=postgres to retain SQL progress, or kafka after controlled cutover',
    );
  }
  const stateBackend = optionalEnv(env, 'OBSERVABILITY_EXPORTER_STATE_BACKEND') ?? 'kafka';
  if (stateBackend !== 'kafka' && stateBackend !== 'postgres') {
    throw new Error('OBSERVABILITY_EXPORTER_STATE_BACKEND must be kafka or postgres');
  }
  // Keep the shared config shape, but never read stale SQL-runtime settings in
  // Kafka mode. Its unused legacy fields retain defaults for compatibility.
  const legacyEnv = stateBackend === 'postgres' ? env : {};
  const brokerEnv = stateBackend === 'kafka' ? env : {};
  const stateDirectory = optionalEnv(brokerEnv, 'OBSERVABILITY_KAFKA_STATE_DIRECTORY');
  if (
    stateDirectory !== undefined &&
    (!isAbsolute(stateDirectory) || stateDirectory.includes('\0'))
  )
    throw new Error('OBSERVABILITY_KAFKA_STATE_DIRECTORY must be an absolute path');
  const databaseUrl =
    stateBackend === 'postgres'
      ? requiredEnv(env, 'OBSERVABILITY_EXPORTER_DATABASE_URL')
      : undefined;
  const registryRequestTimeoutMs = positiveInteger(
    env,
    'OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS',
    10_000,
    stateBackend === 'kafka' ? 15_000 : 120_000,
  );
  const projectorLeaseMs = positiveInteger(
    legacyEnv,
    'OBSERVABILITY_PROJECTOR_LEASE_MS',
    30_000,
    300_000,
    MIN_PROJECTOR_LEASE_MS,
  );

  const token = optionalEnv(env, 'INTERNAL_SERVICE_TOKEN');
  const tokenFile = optionalEnv(env, 'INTERNAL_SERVICE_TOKEN_FILE');
  if ((token === undefined ? 0 : 1) + (tokenFile === undefined ? 0 : 1) !== 1) {
    throw new Error(
      'observability-exporter requires exactly one of INTERNAL_SERVICE_TOKEN or INTERNAL_SERVICE_TOKEN_FILE',
    );
  }

  const kafkaConnectionMode = parseKafkaConnectionMode(env);
  return {
    stateBackend,
    ...(stateDirectory === undefined ? {} : { kafkaStateDirectory: stateDirectory }),
    kafkaProjectorConcurrency: positiveInteger(
      brokerEnv,
      'OBSERVABILITY_KAFKA_PROJECTOR_CONCURRENCY',
      2,
      32,
    ),
    kafkaStateMaxBytes: positiveInteger(
      brokerEnv,
      'OBSERVABILITY_KAFKA_STATE_MAX_BYTES',
      1024 * 1024 * 1024,
      1024 ** 4,
      128 * 1024,
    ),
    kafkaStateCatchupTimeoutMs: positiveInteger(
      brokerEnv,
      'OBSERVABILITY_KAFKA_STATE_CATCHUP_TIMEOUT_MS',
      120_000,
      600_000,
    ),
    kafkaMaxAssemblyBytes: positiveInteger(
      brokerEnv,
      'OBSERVABILITY_KAFKA_MAX_ASSEMBLY_BYTES',
      32 * 1024 * 1024,
      64 * 1024 * 1024,
    ),
    kafkaMaxTransactionBytes: positiveInteger(
      brokerEnv,
      'OBSERVABILITY_KAFKA_MAX_TRANSACTION_BYTES',
      64 * 1024 * 1024,
      128 * 1024 * 1024,
    ),
    ...(databaseUrl === undefined ? {} : { databaseUrl }),
    databasePoolMax: positiveInteger(legacyEnv, 'OBSERVABILITY_EXPORTER_DATABASE_POOL_MAX', 5, 50),
    databaseTimeoutMs: positiveInteger(
      legacyEnv,
      'OBSERVABILITY_EXPORTER_DATABASE_TIMEOUT_MS',
      10_000,
      120_000,
    ),
    kafka: parseKafkaConfig(env, kafkaConnectionMode),
    kafkaTranscript: parseKafkaTranscriptConfig(env, 'kafka'),
    kafkaTopicListingMode: parseKafkaTopicListingMode(env),
    kafkaTopicPrefix: parseKafkaTopicPrefix(env),
    kafkaStartupConcurrency: positiveInteger(
      stateBackend === 'kafka' ? env : {},
      'OBSERVABILITY_KAFKA_STARTUP_CONCURRENCY',
      8,
      32,
    ),
    kafkaRestoreConcurrency: positiveInteger(
      stateBackend === 'kafka' ? env : {},
      'OBSERVABILITY_KAFKA_RESTORE_CONCURRENCY',
      2,
      32,
    ),
    kafkaTopicDiscoveryIntervalMs: positiveInteger(
      env,
      'KAFKA_TOPIC_REDISCOVER_INTERVAL_MS',
      1_000,
      60_000,
    ),
    registryInternalBaseUrl: parseRegistryInternalBaseUrl(
      requiredEnv(env, 'REGISTRY_INTERNAL_BASE_URL'),
    ),
    ...(token === undefined ? {} : { internalServiceToken: token }),
    ...(tokenFile === undefined ? {} : { internalServiceTokenFile: tokenFile }),
    workerId: parseWorkerId(legacyEnv),
    projectorPollMs: positiveInteger(legacyEnv, 'OBSERVABILITY_PROJECTOR_POLL_MS', 250, 60_000),
    projectorLeaseMs,
    projectorBatchSize: positiveInteger(
      env,
      'OBSERVABILITY_PROJECTOR_BATCH_SIZE',
      stateBackend === 'kafka' ? 100 : 1_000,
      stateBackend === 'kafka' ? 1_000 : 10_000,
    ),
    projectorBatchBytes: positiveInteger(
      legacyEnv,
      'OBSERVABILITY_PROJECTOR_BATCH_BYTES',
      DEFAULT_PROJECTOR_BATCH_BYTES,
      MAX_PROJECTOR_BATCH_BYTES,
    ),
    deliveryPollMs: positiveInteger(legacyEnv, 'OBSERVABILITY_DELIVERY_POLL_MS', 250, 60_000),
    // The initial delivery lease covers fresh Registry authorization. Runtime
    // renews it again from the pre-send boundary using the pinned target timeout.
    deliveryLeaseMs: parseDeliveryLeaseMs(legacyEnv, registryRequestTimeoutMs),
    registryRequestTimeoutMs,
  };
}

export function buildExporterPoolConfig(
  config: Pick<ExporterConfig, 'databasePoolMax' | 'databaseTimeoutMs' | 'databaseUrl'>,
): PoolConfig {
  if (config.databaseUrl === undefined) {
    throw new Error('Postgres exporter state requires OBSERVABILITY_EXPORTER_DATABASE_URL');
  }
  return {
    connectionString: config.databaseUrl,
    max: config.databasePoolMax,
    connectionTimeoutMillis: config.databaseTimeoutMs,
    statement_timeout: config.databaseTimeoutMs,
    query_timeout: config.databaseTimeoutMs + 1_000,
  };
}

function parseDeliveryLeaseMs(env: NodeJS.ProcessEnv, registryRequestTimeoutMs: number): number {
  const value = positiveInteger(env, 'OBSERVABILITY_DELIVERY_LEASE_MS', 180_000, 300_000);
  if (value <= registryRequestTimeoutMs + LEASE_COMPLETION_MARGIN_MS) {
    throw new Error(
      'OBSERVABILITY_DELIVERY_LEASE_MS must exceed OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS plus 5000ms',
    );
  }
  return value;
}

function assertKafkaTranscriptBackend(raw: string | undefined): void {
  const normalized = (raw ?? 'kafka').toLowerCase();
  if (normalized === 'kafka') return;
  throw new Error(
    `TRANSCRIPT_STORE_BACKEND=${raw ?? ''} is unsupported by observability-exporter; expected kafka`,
  );
}

function parseKafkaConnectionMode(env: NodeJS.ProcessEnv): KafkaConnectionMode {
  const rawMode = (optionalEnv(env, 'KAFKA_CONNECTION_MODE') ?? 'plaintext').toLowerCase();
  if (!KAFKA_CONNECTION_MODES.includes(rawMode as KafkaConnectionMode)) {
    throw new Error(
      `KAFKA_CONNECTION_MODE=${rawMode} is not recognized; expected ${KAFKA_CONNECTION_MODES.join(', ')}`,
    );
  }

  return rawMode as KafkaConnectionMode;
}

function parseKafkaTopicListingMode(
  env: NodeJS.ProcessEnv,
): ExporterConfig['kafkaTopicListingMode'] {
  const mode = (optionalEnv(env, 'KAFKA_TOPIC_LISTING_MODE') ?? 'canonical').trim().toLowerCase();
  if (mode !== 'canonical' && mode !== 'bare-alias') {
    throw new Error(
      `KAFKA_TOPIC_LISTING_MODE=${mode} is not recognized; expected canonical or bare-alias`,
    );
  }
  return mode;
}

function parseKafkaConfig(env: NodeJS.ProcessEnv, mode: KafkaConnectionMode): KafkaConfig {
  const brokers = (env['KAFKA_BROKERS'] ?? 'localhost:9092')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  if (brokers.length === 0) throw new Error('KAFKA_BROKERS must include at least one broker');

  const clientId = env['KAFKA_CLIENT_ID'] ?? 'observability-exporter';
  const base: KafkaConfig = { brokers, clientId };
  switch (mode) {
    case 'plaintext':
      return base;
    case 'sasl-plain-token-tls':
      return {
        ...base,
        ssl: true,
        connectionTimeout: TLS_KAFKA_TIMEOUT_MS,
        authenticationTimeout: TLS_KAFKA_TIMEOUT_MS,
        sasl: {
          mechanism: 'plain',
          username: optionalEnv(env, 'KAFKA_SASL_USERNAME') ?? 'public',
          password: `token:${requiredEnv(env, 'KAFKA_AUTH_TOKEN')}`,
        },
      };
    case 'sasl-plain-tls':
      return {
        ...base,
        ssl: true,
        connectionTimeout: TLS_KAFKA_TIMEOUT_MS,
        authenticationTimeout: TLS_KAFKA_TIMEOUT_MS,
        sasl: {
          mechanism: 'plain',
          username: requiredEnv(env, 'KAFKA_SASL_USERNAME'),
          password: requiredEnv(env, 'KAFKA_AUTH_TOKEN'),
        },
      };
    case 'custom': {
      const config: KafkaConfig = { ...base };
      const ssl = parseCustomKafkaSsl(env);
      if (ssl !== undefined) {
        config.ssl = ssl;
        config.connectionTimeout = TLS_KAFKA_TIMEOUT_MS;
        config.authenticationTimeout = TLS_KAFKA_TIMEOUT_MS;
      }
      const mechanism = optionalEnv(env, 'KAFKA_SASL_MECHANISM');
      if (mechanism !== undefined) {
        if (mechanism.toLowerCase() !== 'plain') {
          throw new Error(`KAFKA_SASL_MECHANISM=${mechanism} is not supported; expected plain`);
        }
        config.sasl = {
          mechanism: 'plain',
          username: requiredEnv(env, 'KAFKA_SASL_USERNAME'),
          password: requiredEnv(env, 'KAFKA_SASL_PASSWORD'),
        };
      }
      return config;
    }
  }
}

function parseCustomKafkaSsl(env: NodeJS.ProcessEnv): boolean | TlsConnectionOptions | undefined {
  const ca = readKafkaTlsFile(env, 'KAFKA_SSL_CA_FILE');
  const cert = readKafkaTlsFile(env, 'KAFKA_SSL_CERT_FILE');
  const key = readKafkaTlsFile(env, 'KAFKA_SSL_KEY_FILE');
  const rejectUnauthorized = optionalEnv(env, 'KAFKA_SSL_REJECT_UNAUTHORIZED');
  const enabled = booleanEnv(env, 'KAFKA_SSL', false);
  if (
    ca === undefined &&
    cert === undefined &&
    key === undefined &&
    rejectUnauthorized === undefined
  ) {
    return enabled ? true : undefined;
  }

  const ssl: TlsConnectionOptions = {};
  if (ca !== undefined) ssl.ca = ca;
  if (cert !== undefined) ssl.cert = cert;
  if (key !== undefined) ssl.key = key;
  if (rejectUnauthorized !== undefined) {
    ssl.rejectUnauthorized = booleanEnv(env, 'KAFKA_SSL_REJECT_UNAUTHORIZED', true);
  }
  return ssl;
}

function readKafkaTlsFile(env: NodeJS.ProcessEnv, name: string): Buffer | undefined {
  const path = optionalEnv(env, name);
  if (path === undefined) return undefined;
  try {
    return fs.readFileSync(path);
  } catch {
    throw new Error(`${name} points to an unreadable file`);
  }
}

function parseKafkaTopicPrefix(env: NodeJS.ProcessEnv): string {
  const value = optionalEnv(env, 'KAFKA_TOPIC_PREFIX') ?? '';
  try {
    validateTopicPrefix(value);
  } catch {
    throw new Error('KAFKA_TOPIC_PREFIX must be dot-terminated segments');
  }
  return value;
}

function parseRegistryInternalBaseUrl(raw: string): string {
  let value: URL;
  try {
    value = new URL(raw);
  } catch {
    throw new Error('REGISTRY_INTERNAL_BASE_URL must be an absolute HTTP(S) origin');
  }
  if (
    (value.protocol !== 'http:' && value.protocol !== 'https:') ||
    value.username ||
    value.password ||
    value.search ||
    value.hash ||
    (value.pathname !== '' && value.pathname !== '/')
  ) {
    throw new Error('REGISTRY_INTERNAL_BASE_URL must be an HTTP(S) origin');
  }
  return value.origin;
}

function parseWorkerId(env: NodeJS.ProcessEnv): string {
  const value =
    optionalEnv(env, 'OBSERVABILITY_EXPORTER_WORKER_ID') ??
    `observability-exporter-${process.pid}-${randomUUID()}`;
  if (value.length > MAX_WORKER_ID_LENGTH || value.trim() === '' || /[\r\n]/u.test(value)) {
    throw new Error(
      `OBSERVABILITY_EXPORTER_WORKER_ID must be a non-empty single-line value of at most ${MAX_WORKER_ID_LENGTH} characters`,
    );
  }
  return value;
}

function requiredEnv(env: NodeJS.ProcessEnv, name: string): string {
  const value = optionalEnv(env, name);
  if (value === undefined) throw new Error(`${name} is required`);
  return value;
}

function optionalEnv(env: NodeJS.ProcessEnv, name: string): string | undefined {
  const value = env[name];
  return value === undefined || value === '' ? undefined : value;
}

function positiveInteger(
  env: NodeJS.ProcessEnv,
  name: string,
  fallback: number,
  maximum: number,
  minimum = 1,
): number {
  const raw = optionalEnv(env, name);
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

function booleanEnv(env: NodeJS.ProcessEnv, name: string, fallback: boolean): boolean {
  const raw = optionalEnv(env, name);
  if (raw === undefined) return fallback;
  switch (raw.toLowerCase()) {
    case 'true':
    case '1':
    case 'yes':
      return true;
    case 'false':
    case '0':
    case 'no':
      return false;
    default:
      throw new Error(`${name} must be true or false`);
  }
}
