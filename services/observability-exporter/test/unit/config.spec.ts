// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import { buildExporterPoolConfig, loadConfig } from '../../src/config.js';
import {
  MAX_LEASE_OWNER_LENGTH,
  MAX_WORKER_ID_LENGTH,
  PROJECTOR_LEASE_OWNER_SUFFIX,
} from '../../src/lease-owner.js';

const baseEnv: NodeJS.ProcessEnv = {
  REGISTRY_INTERNAL_BASE_URL: 'http://registry.internal:8081',
  INTERNAL_SERVICE_TOKEN: 'x'.repeat(32),
};
const postgresEnv: NodeJS.ProcessEnv = {
  ...baseEnv,
  OBSERVABILITY_EXPORTER_STATE_BACKEND: 'postgres',
  OBSERVABILITY_EXPORTER_DATABASE_URL: 'postgres://legacy',
};

describe('observability exporter configuration', () => {
  it('bounds Kafka startup/restore concurrency independently and ignores them in Postgres', () => {
    expect(loadConfig(baseEnv)).toMatchObject({
      kafkaStartupConcurrency: 8,
      kafkaRestoreConcurrency: 2,
    });
    for (const name of [
      'OBSERVABILITY_KAFKA_STARTUP_CONCURRENCY',
      'OBSERVABILITY_KAFKA_RESTORE_CONCURRENCY',
    ]) {
      for (const value of ['0', '33', '1.5', 'invalid']) {
        expect(() => loadConfig({ ...baseEnv, [name]: value })).toThrow(name);
        expect(() => loadConfig({ ...postgresEnv, [name]: value })).not.toThrow();
      }
    }
    expect(
      loadConfig({
        ...baseEnv,
        OBSERVABILITY_KAFKA_STARTUP_CONCURRENCY: '32',
        OBSERVABILITY_KAFKA_RESTORE_CONCURRENCY: '1',
      }),
    ).toMatchObject({ kafkaStartupConcurrency: 32, kafkaRestoreConcurrency: 1 });
  });

  it.each([
    undefined,
    'plaintext',
    'custom',
    'sasl-plain-tls',
    'sasl-plain-token-tls',
    'SASL-PLAIN-TOKEN-TLS',
  ])('selects topic listing independently of connection mode %s', (mode) => {
    const env = {
      ...baseEnv,
      KAFKA_CONNECTION_MODE: mode,
      KAFKA_TOPIC_PREFIX: 'public.default.',
      KAFKA_AUTH_TOKEN: 'test-token',
      KAFKA_SASL_USERNAME: 'public',
      KAFKA_SSL: 'true',
      KAFKA_SASL_MECHANISM: 'plain',
      KAFKA_SASL_PASSWORD: 'test-password',
    };
    expect(loadConfig(env).kafkaTopicListingMode).toBe('canonical');
    expect(
      loadConfig({ ...env, KAFKA_TOPIC_LISTING_MODE: 'canonical' }).kafkaTopicListingMode,
    ).toBe('canonical');
    expect(
      loadConfig({ ...env, KAFKA_TOPIC_LISTING_MODE: 'bare-alias' }).kafkaTopicListingMode,
    ).toBe('bare-alias');
  });

  it('normalizes topic listing mode and rejects unsupported values', () => {
    expect(
      loadConfig({ ...baseEnv, KAFKA_TOPIC_LISTING_MODE: ' BARE-ALIAS ' }).kafkaTopicListingMode,
    ).toBe('bare-alias');
    for (const mode of ['invalid', 'kop-bare-alias']) {
      expect(() => loadConfig({ ...baseEnv, KAFKA_TOPIC_LISTING_MODE: mode })).toThrow(
        /KAFKA_TOPIC_LISTING_MODE=.*is not recognized; expected canonical or bare-alias/,
      );
    }
  });

  it('rejects the removed connection mode without an alias', () => {
    expect(() =>
      loadConfig({
        ...baseEnv,
        KAFKA_CONNECTION_MODE: 'kop-token',
        KAFKA_AUTH_TOKEN: 'test-token',
      }),
    ).toThrow(/KAFKA_CONNECTION_MODE=kop-token is not recognized/);
  });

  it('preserves token-prefixed SASL/PLAIN over TLS credentials and timeouts', () => {
    const env = {
      ...baseEnv,
      KAFKA_CONNECTION_MODE: 'sasl-plain-token-tls',
      KAFKA_AUTH_TOKEN: 'jwt',
    };
    expect(loadConfig(env).kafka).toMatchObject({
      ssl: true,
      connectionTimeout: 10_000,
      authenticationTimeout: 10_000,
      sasl: { mechanism: 'plain', username: 'public', password: 'token:jwt' },
    });
    expect(loadConfig({ ...env, KAFKA_SASL_USERNAME: 'tenant/user' }).kafka.sasl).toEqual({
      mechanism: 'plain',
      username: 'tenant/user',
      password: 'token:jwt',
    });
    expect(() => loadConfig({ ...env, KAFKA_AUTH_TOKEN: undefined })).toThrow(
      'KAFKA_AUTH_TOKEN is required',
    );
  });

  it('defaults to broker state without accepting a database dependency', () => {
    const env = baseEnv;
    expect(loadConfig(env)).toMatchObject({ stateBackend: 'kafka' });
    expect(loadConfig(env).databaseUrl).toBeUndefined();
    // Existing SQL progress must never be silently replaced with a fresh group.
    expect(() =>
      loadConfig({ ...env, OBSERVABILITY_EXPORTER_DATABASE_URL: 'postgres://old' }),
    ).toThrow('requires explicit OBSERVABILITY_EXPORTER_STATE_BACKEND');
    expect(
      loadConfig({
        ...env,
        OBSERVABILITY_EXPORTER_DATABASE_URL: 'postgres://old',
        OBSERVABILITY_EXPORTER_STATE_BACKEND: 'kafka',
      }).databaseUrl,
    ).toBeUndefined();
    expect(() => buildExporterPoolConfig(loadConfig(env))).toThrow(
      'Postgres exporter state requires OBSERVABILITY_EXPORTER_DATABASE_URL',
    );
  });

  it('requires an explicit legacy Postgres selection and database URL', () => {
    expect(
      loadConfig({
        ...baseEnv,
        OBSERVABILITY_EXPORTER_STATE_BACKEND: 'postgres',
        OBSERVABILITY_EXPORTER_DATABASE_URL: 'postgres://legacy',
      }),
    ).toMatchObject({ stateBackend: 'postgres', databaseUrl: 'postgres://legacy' });
    const env = baseEnv;
    expect(() => loadConfig({ ...env, OBSERVABILITY_EXPORTER_STATE_BACKEND: 'postgres' })).toThrow(
      'OBSERVABILITY_EXPORTER_DATABASE_URL is required',
    );
    expect(() => loadConfig({ ...env, OBSERVABILITY_EXPORTER_STATE_BACKEND: 'memory' })).toThrow(
      'OBSERVABILITY_EXPORTER_STATE_BACKEND must be kafka or postgres',
    );
  });

  it('accepts a controlled Kafka cutover retaining a valid SQL delivery lease', () => {
    const legacyEnv = {
      ...postgresEnv,
      OBSERVABILITY_DELIVERY_LEASE_MS: '20000',
      OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS: '10000',
    };
    expect(loadConfig(legacyEnv).deliveryLeaseMs).toBe(20_000);
    expect(
      loadConfig({ ...baseEnv, OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS: '15000' })
        .registryRequestTimeoutMs,
    ).toBe(15_000);
    expect(
      loadConfig({
        ...legacyEnv,
        OBSERVABILITY_EXPORTER_STATE_BACKEND: 'kafka',
        OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS: '15000',
      }),
    ).toMatchObject({ stateBackend: 'kafka', registryRequestTimeoutMs: 15_000 });
    expect(() =>
      loadConfig({ ...legacyEnv, OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS: '15000' }),
    ).toThrow('must exceed OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS plus 5000ms');
  });

  it.each([
    ['OBSERVABILITY_EXPORTER_DATABASE_POOL_MAX', '51', 'databasePoolMax', 5],
    ['OBSERVABILITY_EXPORTER_DATABASE_TIMEOUT_MS', '120001', 'databaseTimeoutMs', 10_000],
    ['OBSERVABILITY_PROJECTOR_POLL_MS', '0', 'projectorPollMs', 250],
    ['OBSERVABILITY_PROJECTOR_LEASE_MS', '29999', 'projectorLeaseMs', 30_000],
    ['OBSERVABILITY_PROJECTOR_BATCH_BYTES', '67108865', 'projectorBatchBytes', 8 * 1024 * 1024],
    ['OBSERVABILITY_DELIVERY_POLL_MS', '60001', 'deliveryPollMs', 250],
    ['OBSERVABILITY_DELIVERY_LEASE_MS', '0', 'deliveryLeaseMs', 180_000],
    [
      'OBSERVABILITY_EXPORTER_WORKER_ID',
      'w'.repeat(MAX_WORKER_ID_LENGTH + 1),
      'workerId',
      undefined,
    ],
  ])(
    'ignores legacy-only %s in Kafka but validates it in Postgres',
    (name, invalid, field, fallback) => {
      for (const value of [invalid, 'bad\nvalue']) {
        // Both the default backend and an explicit migration selection ignore SQL knobs.
        for (const env of [
          baseEnv,
          { ...postgresEnv, OBSERVABILITY_EXPORTER_STATE_BACKEND: 'kafka' },
        ]) {
          const config = loadConfig({ ...env, [name]: value });
          if (fallback === undefined) {
            expect(config.workerId).toMatch(/^observability-exporter-/u);
            expect(config.workerId).not.toBe(value);
          } else {
            expect(config).toHaveProperty(field, fallback);
          }
        }
        expect(() => loadConfig({ ...postgresEnv, [name]: value })).toThrow(name);
      }
    },
  );

  it('constructs only a Kafka Transcript runtime', () => {
    const config = loadConfig({
      ...baseEnv,
      KAFKA_BROKERS: ' kafka-a:9092, kafka-b:9092 ',
      KAFKA_TOPIC_PREFIX: 'public.default.',
    });

    expect(config.kafka.brokers).toEqual(['kafka-a:9092', 'kafka-b:9092']);
    expect(config.kafkaTopicPrefix).toBe('public.default.');
    expect(config.databaseTimeoutMs).toBe(10_000);
    expect(config.projectorBatchBytes).toBe(8 * 1024 * 1024);
    expect(config.deliveryLeaseMs).toBeGreaterThan(120_000);
  });

  it.each(['postgres', 'pulsar', 'memory'])(
    'rejects unsupported Transcript backend %s',
    (backend) => {
      expect(() => loadConfig({ ...baseEnv, TRANSCRIPT_STORE_BACKEND: backend })).toThrow(
        `TRANSCRIPT_STORE_BACKEND=${backend} is unsupported by observability-exporter; expected kafka`,
      );
    },
  );

  it('requires one internal Registry credential source and a root HTTP origin', () => {
    expect(() => {
      const { INTERNAL_SERVICE_TOKEN: _token, ...withoutToken } = baseEnv;
      return loadConfig(withoutToken);
    }).toThrow('exactly one of INTERNAL_SERVICE_TOKEN or INTERNAL_SERVICE_TOKEN_FILE');
    expect(() => loadConfig({ ...baseEnv, INTERNAL_SERVICE_TOKEN_FILE: '/var/run/token' })).toThrow(
      'exactly one of INTERNAL_SERVICE_TOKEN or INTERNAL_SERVICE_TOKEN_FILE',
    );
    expect(() =>
      loadConfig({ ...baseEnv, REGISTRY_INTERNAL_BASE_URL: 'https://registry.example/internal' }),
    ).toThrow('REGISTRY_INTERNAL_BASE_URL must be an HTTP(S) origin');
  });

  it('rejects leases that cannot cover their bounded external-I/O stage', () => {
    expect(() => loadConfig({ ...postgresEnv, OBSERVABILITY_PROJECTOR_LEASE_MS: '29999' })).toThrow(
      'OBSERVABILITY_PROJECTOR_LEASE_MS must be an integer between 30000 and 300000',
    );
    expect(() =>
      loadConfig({
        ...baseEnv,
        OBSERVABILITY_EXPORTER_STATE_BACKEND: 'postgres',
        OBSERVABILITY_EXPORTER_DATABASE_URL: 'postgres://legacy',
        OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS: '20000',
        OBSERVABILITY_DELIVERY_LEASE_MS: '25000',
      }),
    ).toThrow('must exceed OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS plus 5000ms');
  });

  it('bounds broker batch and Registry work within the Kafka ownership window', () => {
    expect(loadConfig(baseEnv).projectorBatchSize).toBe(100);
    expect(() =>
      loadConfig({ ...baseEnv, OBSERVABILITY_REGISTRY_REQUEST_TIMEOUT_MS: '15001' }),
    ).toThrow('between 1 and 15000');
    expect(() => loadConfig({ ...baseEnv, OBSERVABILITY_PROJECTOR_BATCH_SIZE: '1001' })).toThrow(
      'between 1 and 1000',
    );
  });

  it('bounds the legacy Postgres projector replay byte budget', () => {
    expect(
      loadConfig({ ...postgresEnv, OBSERVABILITY_PROJECTOR_BATCH_BYTES: String(64 * 1024 * 1024) })
        .projectorBatchBytes,
    ).toBe(64 * 1024 * 1024);
    for (const value of ['0', String(64 * 1024 * 1024 + 1), '1.5']) {
      expect(() =>
        loadConfig({ ...postgresEnv, OBSERVABILITY_PROJECTOR_BATCH_BYTES: value }),
      ).toThrow('OBSERVABILITY_PROJECTOR_BATCH_BYTES must be an integer between 1 and 67108864');
    }
  });

  it('bounds exporter database connect and query time', () => {
    const config = loadConfig({
      ...baseEnv,
      OBSERVABILITY_EXPORTER_STATE_BACKEND: 'postgres',
      OBSERVABILITY_EXPORTER_DATABASE_URL: 'postgres://legacy',
      OBSERVABILITY_EXPORTER_DATABASE_TIMEOUT_MS: '120000',
    });
    expect(config.databaseTimeoutMs).toBe(120_000);
    expect(buildExporterPoolConfig(config)).toMatchObject({
      connectionTimeoutMillis: 120_000,
      statement_timeout: 120_000,
      query_timeout: 121_000,
    });
    for (const value of ['0', '120001', '1.5']) {
      expect(() =>
        loadConfig({ ...postgresEnv, OBSERVABILITY_EXPORTER_DATABASE_TIMEOUT_MS: value }),
      ).toThrow(
        'OBSERVABILITY_EXPORTER_DATABASE_TIMEOUT_MS must be an integer between 1 and 120000',
      );
    }
  });

  it('bounds the worker prefix so the longest runtime lease owner fits persistence', () => {
    const workerId = 'w'.repeat(MAX_WORKER_ID_LENGTH);
    const config = loadConfig({ ...postgresEnv, OBSERVABILITY_EXPORTER_WORKER_ID: workerId });

    expect(MAX_WORKER_ID_LENGTH).toBe(246);
    expect(config.workerId).toBe(workerId);
    expect(`${config.workerId}${PROJECTOR_LEASE_OWNER_SUFFIX}`).toHaveLength(
      MAX_LEASE_OWNER_LENGTH,
    );
    expect(() =>
      loadConfig({
        ...postgresEnv,
        OBSERVABILITY_EXPORTER_WORKER_ID: 'w'.repeat(MAX_WORKER_ID_LENGTH + 1),
      }),
    ).toThrow('at most 246 characters');
  });
});
describe('Kafka transcript schema configuration', () => {
  it('passes reader-first Registry configuration independently of broker authentication', () => {
    const config = loadConfig({
      ...baseEnv,
      KAFKA_SCHEMA_REGISTRY_URL: 'https://schemas.example/prefix',
      KAFKA_SCHEMA_REGISTRY_AUTH_MODE: 'basic',
      KAFKA_SCHEMA_REGISTRY_USERNAME: 'public',
      KAFKA_SCHEMA_REGISTRY_PASSWORD: 'registry-jwt',
      KAFKA_CONNECTION_MODE: 'sasl-plain-token-tls',
      KAFKA_AUTH_TOKEN: 'broker-jwt',
    });
    expect(config.kafkaTranscript).toMatchObject({
      encoding: 'raw',
      schemaRegistry: {
        url: 'https://schemas.example/prefix',
        auth: { username: 'public', password: 'registry-jwt' },
      },
    });
    expect(config.kafka.sasl).toMatchObject({ password: 'token:broker-jwt' });
  });

  it('rejects enabled Schema Registry settings on a non-Kafka transcript backend', () => {
    expect(() =>
      loadConfig({
        ...baseEnv,
        TRANSCRIPT_STORE_BACKEND: 'postgres',
        KAFKA_SCHEMA_REGISTRY_URL: 'http://schemas.local',
      }),
    ).toThrow(/kafka/i);
  });

  it('keeps schema integration disabled by default', () => {
    const config = loadConfig(baseEnv);
    expect(config.kafkaTranscript).toBeDefined();
    expect(config.kafkaTranscript?.encoding ?? 'raw').toBe('raw');
    expect(config.kafkaTranscript?.schemaRegistry).toBeUndefined();
  });

  it('rejects insecure Registry credentials without exposing them', () => {
    let failure: unknown;
    try {
      loadConfig({
        ...baseEnv,
        KAFKA_SCHEMA_REGISTRY_URL: 'http://registry.example',
        KAFKA_SCHEMA_REGISTRY_AUTH_MODE: 'basic',
        KAFKA_SCHEMA_REGISTRY_USERNAME: 'registry-user',
        KAFKA_SCHEMA_REGISTRY_PASSWORD: 'private-registry-password',
      });
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(Error);
    expect(String(failure)).not.toContain('private-registry-password');
  });

  it('rejects Avro without a Registry URL before startup', () => {
    expect(() => loadConfig({ ...baseEnv, KAFKA_TRANSCRIPT_ENCODING: 'avro' })).toThrow(
      /KAFKA_SCHEMA_REGISTRY_URL/,
    );
  });
});
