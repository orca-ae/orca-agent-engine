// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

import { loadConfig } from '../../src/config.js';

const baseEnv = { SANDBOX_RUNTIME: 'local' };

describe('loadConfig — Kafka connection parsing', () => {
  it('defaults to plaintext Kafka with existing local-stack values', () => {
    const config = loadConfig(baseEnv);

    expect(config.kafka).toEqual({ brokers: ['localhost:9092'], clientId: 'harness-server' });
    expect(config.kafkaBrokers).toEqual(config.kafka.brokers);
    expect(config.clientId).toBe(config.kafka.clientId);
  });

  it('trims and ignores empty broker entries', () => {
    const config = loadConfig({ ...baseEnv, KAFKA_BROKERS: ' kafka-1:9092, ,kafka-2:9092, ' });

    expect(config.kafka.brokers).toEqual(['kafka-1:9092', 'kafka-2:9092']);
  });

  it('parses token-prefixed SASL/PLAIN over TLS config', () => {
    const config = loadConfig({
      ...baseEnv,
      KAFKA_CONNECTION_MODE: 'sasl-plain-token-tls',
      KAFKA_AUTH_TOKEN: 'jwt-token',
    });

    expect(config.kafka).toMatchObject({
      ssl: true,
      connectionTimeout: 10_000,
      authenticationTimeout: 10_000,
      sasl: { mechanism: 'plain', username: 'public', password: 'token:jwt-token' },
    });
  });

  it('preserves an explicit username and accepts an uppercase token mode', () => {
    const config = loadConfig({
      ...baseEnv,
      KAFKA_CONNECTION_MODE: 'SASL-PLAIN-TOKEN-TLS',
      KAFKA_AUTH_TOKEN: 'jwt-token',
      KAFKA_SASL_USERNAME: 'tenant/user',
    });
    expect(config.kafka.sasl).toEqual({
      mechanism: 'plain',
      username: 'tenant/user',
      password: 'token:jwt-token',
    });
  });

  it('rejects the removed connection mode without an alias', () => {
    expect(() =>
      loadConfig({
        ...baseEnv,
        KAFKA_CONNECTION_MODE: 'kop-token',
        KAFKA_AUTH_TOKEN: 'jwt-token',
      }),
    ).toThrow(/KAFKA_CONNECTION_MODE=kop-token is not recognized/);
  });

  it('parses SASL/PLAIN over TLS config', () => {
    const config = loadConfig({
      ...baseEnv,
      KAFKA_CONNECTION_MODE: 'sasl-plain-tls',
      KAFKA_SASL_USERNAME: 'tenant/user',
      KAFKA_AUTH_TOKEN: 'jwt-token',
    });

    expect(config.kafka).toMatchObject({
      ssl: true,
      sasl: { mechanism: 'plain', username: 'tenant/user', password: 'jwt-token' },
    });
  });

  it('parses custom plain SASL and TLS files', () => {
    const dir = mkdtempSync(join(tmpdir(), 'orca-kafka-tls-'));
    try {
      const caFile = join(dir, 'ca.pem');
      writeFileSync(caFile, 'ca-bytes');

      const config = loadConfig({
        ...baseEnv,
        KAFKA_CONNECTION_MODE: 'custom',
        KAFKA_SSL_CA_FILE: caFile,
        KAFKA_SSL_REJECT_UNAUTHORIZED: 'false',
        KAFKA_SASL_MECHANISM: 'plain',
        KAFKA_SASL_USERNAME: 'user',
        KAFKA_SASL_PASSWORD: 'pass',
      });

      expect(config.kafka.ssl).toMatchObject({
        ca: Buffer.from('ca-bytes'),
        rejectUnauthorized: false,
      });
      expect(config.kafka.sasl).toEqual({ mechanism: 'plain', username: 'user', password: 'pass' });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('rejects invalid Kafka modes and missing mode-required fields without leaking secrets', () => {
    expect(() => loadConfig({ ...baseEnv, KAFKA_CONNECTION_MODE: 'bad-mode' })).toThrowError(
      /KAFKA_CONNECTION_MODE=bad-mode is not recognized/,
    );
    expect(() =>
      loadConfig({ ...baseEnv, KAFKA_CONNECTION_MODE: 'sasl-plain-token-tls' }),
    ).toThrowError(/KAFKA_AUTH_TOKEN is required/);
    expect(() =>
      loadConfig({
        ...baseEnv,
        KAFKA_CONNECTION_MODE: 'sasl-plain-tls',
        KAFKA_AUTH_TOKEN: 'jwt-token',
      }),
    ).toThrowError(/KAFKA_SASL_USERNAME is required/);
    expect(() =>
      loadConfig({
        ...baseEnv,
        KAFKA_CONNECTION_MODE: 'custom',
        KAFKA_SASL_MECHANISM: 'plain',
        KAFKA_SASL_PASSWORD: 'secret',
      }),
    ).toThrowError(/KAFKA_SASL_USERNAME is required/);
  });

  it('rejects missing TLS files without exposing credential values', () => {
    let message = '';
    try {
      loadConfig({
        ...baseEnv,
        KAFKA_CONNECTION_MODE: 'custom',
        KAFKA_SSL_CA_FILE: '/missing/ca.pem',
        KAFKA_SASL_MECHANISM: 'plain',
        KAFKA_SASL_USERNAME: 'user',
        KAFKA_SASL_PASSWORD: 'super-secret-password',
      });
    } catch (error) {
      message = (error as Error).message;
    }

    expect(message).toMatch(/KAFKA_SSL_CA_FILE points to an unreadable file: \/missing\/ca\.pem/);
    expect(message).not.toContain('super-secret-password');
  });
});

describe('loadConfig — Kafka topic prefix parsing', () => {
  it('defaults to an empty prefix (bare topic names)', () => {
    expect(loadConfig(baseEnv).kafkaTopicPrefix).toBe('');
    expect(loadConfig({ ...baseEnv, KAFKA_TOPIC_PREFIX: '' }).kafkaTopicPrefix).toBe('');
  });

  it('accepts dot-terminated prefixes with the default required rediscovery interval', () => {
    expect(
      loadConfig({
        ...baseEnv,
        KAFKA_TOPIC_PREFIX: 'public.default.',
      }).kafkaTopicPrefix,
    ).toBe('public.default.');
    expect(
      loadConfig({
        ...baseEnv,
        KAFKA_TOPIC_PREFIX: 'tenant-1.ns_2.',
      }).kafkaTopicPrefix,
    ).toBe('tenant-1.ns_2.');
  });

  it('defaults Kafka rediscovery to 30 seconds and rejects disabled or invalid values', () => {
    expect(loadConfig(baseEnv).kafkaTopicRediscoverIntervalMs).toBe(30_000);
    for (const value of ['0', '-1', 'not-a-number', '1.5']) {
      expect(() =>
        loadConfig({ ...baseEnv, KAFKA_TOPIC_REDISCOVER_INTERVAL_MS: value }),
      ).toThrowError(/KAFKA_TOPIC_REDISCOVER_INTERVAL_MS=.*must be a positive integer/);
    }
    expect(
      loadConfig({
        ...baseEnv,
        TRANSCRIPT_STORE_BACKEND: 'postgres',
        KAFKA_TOPIC_REDISCOVER_INTERVAL_MS: '0',
      }).kafkaTopicRediscoverIntervalMs,
    ).toBe(0);
  });

  it('rejects prefixes that are not dot-terminated segments', () => {
    expect(() => loadConfig({ ...baseEnv, KAFKA_TOPIC_PREFIX: 'public.default' })).toThrowError(
      /KAFKA_TOPIC_PREFIX=public\.default is invalid/,
    );
    expect(() => loadConfig({ ...baseEnv, KAFKA_TOPIC_PREFIX: '.public.' })).toThrowError(
      /KAFKA_TOPIC_PREFIX/,
    );
    expect(() => loadConfig({ ...baseEnv, KAFKA_TOPIC_PREFIX: 'pub lic.' })).toThrowError(
      /KAFKA_TOPIC_PREFIX/,
    );
  });
});

describe('loadConfig — Pulsar auth parsing', () => {
  it('defaults the Postgres transcript backend to the transcriptstore database', () => {
    const config = loadConfig({
      SANDBOX_RUNTIME: 'local',
      TRANSCRIPT_STORE_BACKEND: 'postgres',
    });

    expect(config.transcriptStoreDatabaseUrl).toBe(
      'postgres://orca:orca@localhost:5432/transcriptstore',
    );
  });

  it('parses token auth from env', () => {
    const config = loadConfig({
      SANDBOX_RUNTIME: 'local',
      PULSAR_AUTH_TYPE: 'token',
      PULSAR_AUTH_TOKEN: 'pulsar-token',
    });

    expect(config.pulsarAuth).toEqual({ type: 'token', token: 'pulsar-token' });
  });

  it('parses OAuth2 auth from env', () => {
    const config = loadConfig({
      SANDBOX_RUNTIME: 'local',
      PULSAR_AUTH_TYPE: 'oauth2',
      PULSAR_OAUTH2_ISSUER_URL: 'https://issuer.example',
      PULSAR_OAUTH2_CLIENT_ID: 'client-id',
      PULSAR_OAUTH2_CLIENT_SECRET: 'client-secret',
      PULSAR_OAUTH2_AUDIENCE: 'pulsar-audience',
      PULSAR_OAUTH2_SCOPE: 'produce consume',
    });

    expect(config.pulsarAuth).toEqual({
      type: 'oauth2',
      issuerUrl: 'https://issuer.example',
      clientId: 'client-id',
      clientSecret: 'client-secret',
      audience: 'pulsar-audience',
      scope: 'produce consume',
    });
  });

  it('rejects OAuth2 auth without an issuer URL', () => {
    expect(() =>
      loadConfig({
        SANDBOX_RUNTIME: 'local',
        PULSAR_AUTH_TYPE: 'oauth2',
      }),
    ).toThrowError(/PULSAR_OAUTH2_ISSUER_URL is required/);
  });
});

describe('loadConfig — workspace object root', () => {
  it('uses one canonical root and the internal Registry listener by default', () => {
    const config = loadConfig(baseEnv);

    expect(config.s3KeyPrefix).toBe('managed-agents/');
    expect(config.s3ForcePathStyle).toBe(true);
    expect(config.registryInternalBaseUrl).toBe('http://localhost:8081');
    expect(config).not.toHaveProperty('outputsKeyPrefix');
    expect(config).not.toHaveProperty('memoryKeyPrefix');
  });

  it('normalizes a configured root and rejects ambiguous or wildcard segments', () => {
    expect(loadConfig({ ...baseEnv, S3_KEY_PREFIX: 'tenant-data' }).s3KeyPrefix).toBe(
      'tenant-data/',
    );
    expect(() => loadConfig({ ...baseEnv, S3_KEY_PREFIX: '../other' })).toThrowError(
      /S3_KEY_PREFIX=.*invalid/,
    );
    expect(() => loadConfig({ ...baseEnv, S3_KEY_PREFIX: 'tenant/*' })).toThrowError(
      /S3_KEY_PREFIX=.*invalid/,
    );
    expect(loadConfig({ ...baseEnv, S3_FORCE_PATH_STYLE: 'false' }).s3ForcePathStyle).toBe(false);
  });

  it('keeps the S3 data-plane and STS endpoints independent', () => {
    const withS3Only = loadConfig({
      ...baseEnv,
      S3_ENDPOINT: 'http://minio:9000',
    });
    expect(withS3Only.s3Endpoint).toBe('http://minio:9000');
    expect(withS3Only.s3StsEndpoint).toBeUndefined();

    const withStsOverride = loadConfig({
      ...baseEnv,
      S3_ENDPOINT: 'http://minio:9000',
      S3_STS_ENDPOINT: 'http://minio-sts:9000',
    });
    expect(withStsOverride.s3Endpoint).toBe('http://minio:9000');
    expect(withStsOverride.s3StsEndpoint).toBe('http://minio-sts:9000');
    expect(loadConfig({ ...baseEnv, S3_STS_ENDPOINT: '' }).s3StsEndpoint).toBeUndefined();
  });

  it('treats empty static S3 credential variables as unset', () => {
    expect(loadConfig(baseEnv)).toMatchObject({
      s3AccessKeyId: undefined,
      s3SecretAccessKey: undefined,
    });
    expect(
      loadConfig({
        ...baseEnv,
        S3_ACCESS_KEY_ID: '',
        S3_SECRET_ACCESS_KEY: '',
      }),
    ).toMatchObject({
      s3AccessKeyId: undefined,
      s3SecretAccessKey: undefined,
    });
  });

  it('requires an explicit non-production opt-in for static sandbox S3 credentials', () => {
    expect(loadConfig(baseEnv).allowInsecureStaticS3Creds).toBe(false);
    expect(
      loadConfig({
        ...baseEnv,
        NODE_ENV: 'development',
        ALLOW_INSECURE_STATIC_S3_CREDS: 'true',
      }).allowInsecureStaticS3Creds,
    ).toBe(true);
    expect(() =>
      loadConfig({
        ...baseEnv,
        NODE_ENV: 'production',
        ALLOW_INSECURE_STATIC_S3_CREDS: 'true',
      }),
    ).toThrowError(/permitted only when NODE_ENV is development or test/);
    expect(() =>
      loadConfig({
        ...baseEnv,
        ALLOW_INSECURE_STATIC_S3_CREDS: 'true',
      }),
    ).toThrowError(/permitted only when NODE_ENV is development or test/);
  });

  it('uses the AWS default credential chain unless a complete static pair is configured', () => {
    expect(loadConfig(baseEnv)).toMatchObject({
      s3AccessKeyId: undefined,
      s3SecretAccessKey: undefined,
    });
    expect(
      loadConfig({
        ...baseEnv,
        S3_ACCESS_KEY_ID: 'static-access',
        S3_SECRET_ACCESS_KEY: 'static-secret',
      }),
    ).toMatchObject({
      s3AccessKeyId: 'static-access',
      s3SecretAccessKey: 'static-secret',
    });
    expect(
      loadConfig({
        ...baseEnv,
        S3_ACCESS_KEY: 'legacy-access',
        S3_SECRET_KEY: 'legacy-secret',
      }),
    ).toMatchObject({
      s3AccessKeyId: 'legacy-access',
      s3SecretAccessKey: 'legacy-secret',
    });
    expect(
      loadConfig({
        ...baseEnv,
        S3_ACCESS_KEY_ID: 'same-access',
        S3_SECRET_ACCESS_KEY: 'same-secret',
        S3_ACCESS_KEY: 'same-access',
        S3_SECRET_KEY: 'same-secret',
      }),
    ).toMatchObject({
      s3AccessKeyId: 'same-access',
      s3SecretAccessKey: 'same-secret',
    });
    expect(() => loadConfig({ ...baseEnv, S3_SECRET_KEY: 'partial-secret' })).toThrowError(
      /must configure both access-key and secret-key/,
    );
    expect(() =>
      loadConfig({
        ...baseEnv,
        S3_ACCESS_KEY_ID: 'canonical-access',
        S3_SECRET_ACCESS_KEY: 'canonical-secret',
        S3_ACCESS_KEY: 'legacy-access',
        S3_SECRET_KEY: 'legacy-secret',
      }),
    ).toThrowError(/alias pairs must match/);
    expect(() =>
      loadConfig({
        ...baseEnv,
        S3_ACCESS_KEY_ID: 'canonical-access',
        S3_SECRET_KEY: 'legacy-secret',
      }),
    ).toThrowError(/must configure both access-key and secret-key/);
    expect(() =>
      loadConfig({
        ...baseEnv,
        S3_ACCESS_KEY_ID: 'canonical-access',
        S3_SECRET_ACCESS_KEY: 'canonical-secret',
        S3_ACCESS_KEY: 'partial-legacy-access',
      }),
    ).toThrowError(/must configure both access-key and secret-key/);
  });
});

describe('loadConfig — Registry internal auth', () => {
  it('parses static token and projected token file sources', () => {
    expect(
      loadConfig({ ...baseEnv, INTERNAL_SERVICE_TOKEN: 'static-token' }).internalServiceToken,
    ).toBe('static-token');
    expect(
      loadConfig({ ...baseEnv, INTERNAL_SERVICE_TOKEN_FILE: '/var/run/secrets/orca/token' })
        .internalServiceTokenFile,
    ).toBe('/var/run/secrets/orca/token');
  });
});

describe('loadConfig — sandbox-harness default image override', () => {
  it('defaults to undefined, leaving the harness-catalog default in effect', () => {
    expect(loadConfig(baseEnv).sandboxHarnessClaudeCodeImage).toBeUndefined();
  });

  it('reads the runtime override from SANDBOX_HARNESS_CLAUDE_CODE_IMAGE', () => {
    const config = loadConfig({
      ...baseEnv,
      SANDBOX_HARNESS_CLAUDE_CODE_IMAGE: 'docker.io/orcaae/sandbox-harness-claude-code:0.3.0',
    });

    expect(config.sandboxHarnessClaudeCodeImage).toBe(
      'docker.io/orcaae/sandbox-harness-claude-code:0.3.0',
    );
  });
});

describe('loadConfig — database pool sizing', () => {
  it('defaults the transcript and file-store pools to 10', () => {
    const config = loadConfig(baseEnv);

    expect(config.transcriptStorePoolMax).toBe(10);
    expect(config.fileStorePoolMax).toBe(10);
  });

  it('reads per-pool overrides and the file-store DSN from the environment', () => {
    const config = loadConfig({
      ...baseEnv,
      TRANSCRIPT_STORE_POOL_MAX: '4',
      FILESTORE_POOL_MAX: '6',
      FILESTORE_DATABASE_URL: 'postgres://orca_a:pw@pg:5432/orca_a_filestore',
    });

    expect(config.transcriptStorePoolMax).toBe(4);
    expect(config.fileStorePoolMax).toBe(6);
    expect(config.fileStoreDatabaseUrl).toBe('postgres://orca_a:pw@pg:5432/orca_a_filestore');
  });

  it('rejects non-positive or non-integer pool sizes', () => {
    expect(() => loadConfig({ ...baseEnv, TRANSCRIPT_STORE_POOL_MAX: '0' })).toThrowError(
      /TRANSCRIPT_STORE_POOL_MAX=0 is not a positive integer/,
    );
    expect(() => loadConfig({ ...baseEnv, FILESTORE_POOL_MAX: '1.5' })).toThrowError(
      /FILESTORE_POOL_MAX=1.5 is not a positive integer/,
    );
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
