// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

import { loadConfig } from '../../src/config.js';

describe('loadConfig — API performance rollback switches', () => {
  it('keeps admission off until explicitly enabled and validates queue bounds', () => {
    expect(loadConfig({}).heavyReadAdmission.maxConcurrent).toBe(0);
    expect(
      loadConfig({ REGISTRY_HEAVY_READ_ADMISSION_ENABLED: 'true' }).heavyReadAdmission,
    ).toEqual({
      maxConcurrent: 4,
      maxPerWorkspace: 2,
      maxQueued: 32,
      maxQueuedPerWorkspace: 8,
      queueTimeoutMs: 5000,
    });
    for (const key of [
      'REGISTRY_HEAVY_READ_MAX_CONCURRENT',
      'REGISTRY_HEAVY_READ_MAX_PER_WORKSPACE',
      'REGISTRY_HEAVY_READ_MAX_QUEUED',
      'REGISTRY_HEAVY_READ_MAX_QUEUED_PER_WORKSPACE',
      'REGISTRY_HEAVY_READ_QUEUE_TIMEOUT_MS',
    ]) {
      expect(() =>
        loadConfig({ REGISTRY_HEAVY_READ_ADMISSION_ENABLED: 'true', [key]: '0' }),
      ).toThrow(key);
    }
  });

  it('enables batching and proof reuse independently and validates boolean values', () => {
    expect(loadConfig({})).toMatchObject({
      batchReadsEnabled: true,
      apiKeyProofCacheEnabled: true,
    });
    expect(loadConfig({ REGISTRY_BATCH_READS_ENABLED: 'false' })).toMatchObject({
      batchReadsEnabled: false,
      apiKeyProofCacheEnabled: true,
    });
    expect(loadConfig({ REGISTRY_API_KEY_PROOF_CACHE_ENABLED: 'false' })).toMatchObject({
      batchReadsEnabled: true,
      apiKeyProofCacheEnabled: false,
    });
    for (const key of ['REGISTRY_BATCH_READS_ENABLED', 'REGISTRY_API_KEY_PROOF_CACHE_ENABLED']) {
      expect(() => loadConfig({ [key]: 'invalid' })).toThrow(key);
    }
  });
});

describe('loadConfig — cron Trigger worker', () => {
  it('enables the in-process planner and dispatcher with bounded defaults', () => {
    expect(loadConfig({})).toMatchObject({
      triggerSchedulerEnabled: true,
      triggerReconcileIntervalMs: 5_000,
      triggerReconcileBatchSize: 100,
    });
  });

  it('accepts an API-only deployment configuration and validates numeric knobs', () => {
    expect(
      loadConfig({
        TRIGGER_SCHEDULER_ENABLED: 'false',
        TRIGGER_RECONCILE_INTERVAL_MS: '2500',
        TRIGGER_RECONCILE_BATCH_SIZE: '25',
      }),
    ).toMatchObject({
      triggerSchedulerEnabled: false,
      triggerReconcileIntervalMs: 2_500,
      triggerReconcileBatchSize: 25,
    });
    expect(() => loadConfig({ TRIGGER_RECONCILE_BATCH_SIZE: '0' })).toThrowError(
      /positive integer/,
    );
  });
});

describe('loadConfig — Gateway Registry usage sink', () => {
  it('keeps Harness authoritative by default and accepts explicit enablement', () => {
    expect(loadConfig({}).gatewayRegistryUsageEnabled).toBe(false);
    expect(
      loadConfig({ AI_GATEWAY_REGISTRY_USAGE_ENABLED: 'true' }).gatewayRegistryUsageEnabled,
    ).toBe(true);
  });

  it('rejects an invalid boolean', () => {
    expect(() => loadConfig({ AI_GATEWAY_REGISTRY_USAGE_ENABLED: 'sometimes' })).toThrowError(
      /AI_GATEWAY_REGISTRY_USAGE_ENABLED/,
    );
  });
});

describe('loadConfig — session JWT LLM policy', () => {
  it('omits the policy when both variables are unset', () => {
    expect(loadConfig({}).sessionJwtLlmPolicy).toBeUndefined();
  });

  it('trims and deduplicates configured routes and models', () => {
    expect(
      loadConfig({
        SESSION_JWT_LLM_ROUTES: ' llm-messages,managed-openai,llm-messages ',
        SESSION_JWT_LLM_MODELS: ' claude-*,gpt-4o*,claude-* ',
      }).sessionJwtLlmPolicy,
    ).toEqual({
      routes: ['llm-messages', 'managed-openai'],
      models: ['claude-*', 'gpt-4o*'],
    });
  });

  it('rejects partial or empty-entry configuration', () => {
    expect(() => loadConfig({ SESSION_JWT_LLM_ROUTES: 'llm-messages' })).toThrowError(
      /must be configured together/,
    );
    expect(() =>
      loadConfig({
        SESSION_JWT_LLM_ROUTES: 'llm-messages,',
        SESSION_JWT_LLM_MODELS: 'claude-*',
      }),
    ).toThrowError(/SESSION_JWT_LLM_ROUTES contains an empty entry/);
    expect(() =>
      loadConfig({
        SESSION_JWT_LLM_ROUTES: 'llm-messages',
        SESSION_JWT_LLM_MODELS: 'claude-*,,gpt-4o*',
      }),
    ).toThrowError(/SESSION_JWT_LLM_MODELS contains an empty entry/);
  });
});

describe('loadConfig — Kafka connection parsing', () => {
  it('defaults to plaintext Kafka with existing local-stack values', () => {
    const config = loadConfig({});

    expect(config.kafka).toEqual({ brokers: ['localhost:9092'], clientId: 'registry-service-ts' });
    expect(config.kafkaBrokers).toEqual(config.kafka.brokers);
    expect(config.kafkaClientId).toBe(config.kafka.clientId);
  });

  it('trims and ignores empty broker entries', () => {
    const config = loadConfig({ KAFKA_BROKERS: ' kafka-1:9092, ,kafka-2:9092, ' });

    expect(config.kafka.brokers).toEqual(['kafka-1:9092', 'kafka-2:9092']);
  });

  it('parses token-prefixed SASL/PLAIN over TLS config', () => {
    const config = loadConfig({
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
        KAFKA_CONNECTION_MODE: 'kop-token',
        KAFKA_AUTH_TOKEN: 'jwt-token',
      }),
    ).toThrow(/KAFKA_CONNECTION_MODE=kop-token is not recognized/);
  });

  it('parses SASL/PLAIN over TLS config', () => {
    const config = loadConfig({
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
    expect(() => loadConfig({ KAFKA_CONNECTION_MODE: 'bad-mode' })).toThrowError(
      /KAFKA_CONNECTION_MODE=bad-mode is not recognized/,
    );
    expect(() => loadConfig({ KAFKA_CONNECTION_MODE: 'sasl-plain-token-tls' })).toThrowError(
      /KAFKA_AUTH_TOKEN is required/,
    );
    expect(() =>
      loadConfig({ KAFKA_CONNECTION_MODE: 'sasl-plain-tls', KAFKA_AUTH_TOKEN: 'jwt-token' }),
    ).toThrowError(/KAFKA_SASL_USERNAME is required/);
    expect(() =>
      loadConfig({
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

describe('loadConfig — listener and object-store isolation', () => {
  it('uses distinct public/internal/admin ports and one canonical object root', () => {
    const config = loadConfig({});
    expect(config.httpPort).toBe(8080);
    expect(config.internalHttpPort).toBe(8081);
    expect(config.adminHttpPort).toBe(8082);
    expect(config.platformOidcAllowedIssuers).toEqual([]);
    expect(config.platformOidcAudience).toBe('orca-managed-agents-platform');
    expect(config.trustedProxyCidrs).toEqual([]);
    expect(config.s3KeyPrefix).toBe('managed-agents/');
    expect(config.s3ForcePathStyle).toBe(true);
  });

  it('accepts explicit listener ports and object root', () => {
    const config = loadConfig({
      HTTP_PORT: '9080',
      INTERNAL_HTTP_PORT: '9081',
      ADMIN_HTTP_PORT: '9082',
      PLATFORM_OIDC_ALLOWED_ISSUERS: 'https://idp.example/platform',
      PLATFORM_OIDC_AUDIENCE: 'platform-audience',
      TRUST_PROXY_CIDRS: '10.42.0.0/16, 2001:db8::1,10.42.0.0/16',
      S3_KEY_PREFIX: 'orca-data/',
    });
    expect(config.httpPort).toBe(9080);
    expect(config.internalHttpPort).toBe(9081);
    expect(config.adminHttpPort).toBe(9082);
    expect(config.platformOidcAllowedIssuers).toEqual(['https://idp.example/platform']);
    expect(config.platformOidcAudience).toBe('platform-audience');
    expect(config.trustedProxyCidrs).toEqual(['10.42.0.0/16', '2001:db8::1']);
    expect(config.s3KeyPrefix).toBe('orca-data/');
    expect(loadConfig({ S3_FORCE_PATH_STYLE: 'false' }).s3ForcePathStyle).toBe(false);
  });

  it('normalizes the object root and rejects traversal or wildcard segments', () => {
    expect(loadConfig({ S3_KEY_PREFIX: 'orca-data' }).s3KeyPrefix).toBe('orca-data/');
    expect(() => loadConfig({ S3_KEY_PREFIX: '../private' })).toThrowError(
      /S3_KEY_PREFIX=.*invalid/,
    );
    expect(() => loadConfig({ S3_KEY_PREFIX: 'orca/*' })).toThrowError(/S3_KEY_PREFIX=.*invalid/);
  });

  it('uses the AWS default credential chain unless a complete static pair is configured', () => {
    expect(loadConfig({})).toMatchObject({
      s3AccessKeyId: undefined,
      s3SecretAccessKey: undefined,
    });
    expect(
      loadConfig({
        S3_ACCESS_KEY_ID: 'static-access',
        S3_SECRET_ACCESS_KEY: 'static-secret',
      }),
    ).toMatchObject({
      s3AccessKeyId: 'static-access',
      s3SecretAccessKey: 'static-secret',
    });
    expect(
      loadConfig({
        S3_ACCESS_KEY: 'legacy-access',
        S3_SECRET_KEY: 'legacy-secret',
      }),
    ).toMatchObject({
      s3AccessKeyId: 'legacy-access',
      s3SecretAccessKey: 'legacy-secret',
    });
    expect(
      loadConfig({
        S3_ACCESS_KEY_ID: 'same-access',
        S3_SECRET_ACCESS_KEY: 'same-secret',
        S3_ACCESS_KEY: 'same-access',
        S3_SECRET_KEY: 'same-secret',
      }),
    ).toMatchObject({
      s3AccessKeyId: 'same-access',
      s3SecretAccessKey: 'same-secret',
    });
    expect(() => loadConfig({ S3_ACCESS_KEY: 'partial-access' })).toThrowError(
      /must configure both access-key and secret-key/,
    );
    expect(() =>
      loadConfig({
        S3_ACCESS_KEY_ID: 'canonical-access',
        S3_SECRET_ACCESS_KEY: 'canonical-secret',
        S3_ACCESS_KEY: 'legacy-access',
        S3_SECRET_KEY: 'legacy-secret',
      }),
    ).toThrowError(/alias pairs must match/);
    expect(() =>
      loadConfig({
        S3_ACCESS_KEY_ID: 'canonical-access',
        S3_SECRET_KEY: 'legacy-secret',
      }),
    ).toThrowError(/must configure both access-key and secret-key/);
    expect(() =>
      loadConfig({
        S3_ACCESS_KEY_ID: 'canonical-access',
        S3_SECRET_ACCESS_KEY: 'canonical-secret',
        S3_ACCESS_KEY: 'partial-legacy-access',
      }),
    ).toThrowError(/must configure both access-key and secret-key/);
  });

  it('rejects invalid or trust-all proxy ranges', () => {
    expect(() => loadConfig({ TRUST_PROXY_CIDRS: 'ingress-nginx' })).toThrowError(
      /invalid IP or CIDR/,
    );
    expect(() => loadConfig({ TRUST_PROXY_CIDRS: '10.0.0.0/33' })).toThrowError(
      /invalid or trust-all CIDR prefix/,
    );
    expect(() => loadConfig({ TRUST_PROXY_CIDRS: '0.0.0.0/0' })).toThrowError(
      /invalid or trust-all CIDR prefix/,
    );
    expect(() => loadConfig({ TRUST_PROXY_CIDRS: '::/0' })).toThrowError(
      /invalid or trust-all CIDR prefix/,
    );
  });
});

describe('loadConfig — Registry internal auth', () => {
  it('defaults by environment and accepts explicit modes', () => {
    expect(loadConfig({}).internalAuthMode).toBe('static_token');
    expect(loadConfig({}).internalAuthAudience).toBe('orca-registry-internal');
    expect(loadConfig({ INTERNAL_AUTH_AUDIENCE: '' }).internalAuthAudience).toBe(
      'orca-registry-internal',
    );
    expect(loadConfig({ KUBERNETES_SERVICE_HOST: '10.0.0.1' }).internalAuthMode).toBe(
      'kubernetes_service_account',
    );
    expect(loadConfig({ INTERNAL_AUTH_MODE: 'static_token' }).internalAuthMode).toBe(
      'static_token',
    );
  });

  it('parses token sources and Kubernetes caller identities', () => {
    const config = loadConfig({
      INTERNAL_AUTH_MODE: 'kubernetes_service_account',
      INTERNAL_AUTH_AUDIENCE: 'custom-audience',
      INTERNAL_AUTH_HARNESS_SUBJECT: 'system:serviceaccount:orca:harness',
      INTERNAL_AUTH_AI_GATEWAY_SUBJECT: 'system:serviceaccount:orca:gateway',
      INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT:
        'system:serviceaccount:orca:observability-exporter',
      INTERNAL_SERVICE_TOKEN_FILE: '/tmp/internal-token',
    });
    expect(config).toMatchObject({
      internalAuthMode: 'kubernetes_service_account',
      internalAuthAudience: 'custom-audience',
      internalAuthHarnessSubject: 'system:serviceaccount:orca:harness',
      internalAuthAiGatewaySubject: 'system:serviceaccount:orca:gateway',
      internalAuthObservabilityExporterSubject: 'system:serviceaccount:orca:observability-exporter',
      internalServiceTokenFile: '/tmp/internal-token',
    });
  });

  it('rejects unknown modes', () => {
    expect(() => loadConfig({ INTERNAL_AUTH_MODE: 'none' })).toThrow(/INTERNAL_AUTH_MODE=none/);
  });

  it('permits shared-token auth in non-Kubernetes deployments', () => {
    expect(loadConfig({ INTERNAL_AUTH_MODE: 'static_token' })).toMatchObject({
      internalAuthMode: 'static_token',
    });
    expect(
      loadConfig({ NODE_ENV: 'production', INTERNAL_AUTH_MODE: 'static_token' }),
    ).toMatchObject({
      internalAuthMode: 'static_token',
    });
  });
});

describe('loadConfig — Kafka topic prefix parsing', () => {
  it('defaults to an empty prefix (bare topic names)', () => {
    expect(loadConfig({}).kafkaTopicPrefix).toBe('');
    expect(loadConfig({ KAFKA_TOPIC_PREFIX: '' }).kafkaTopicPrefix).toBe('');
  });

  it('accepts dot-terminated prefixes', () => {
    expect(loadConfig({ KAFKA_TOPIC_PREFIX: 'public.default.' }).kafkaTopicPrefix).toBe(
      'public.default.',
    );
    expect(loadConfig({ KAFKA_TOPIC_PREFIX: 'tenant-1.ns_2.' }).kafkaTopicPrefix).toBe(
      'tenant-1.ns_2.',
    );
  });

  it('rejects prefixes that are not dot-terminated segments', () => {
    expect(() => loadConfig({ KAFKA_TOPIC_PREFIX: 'public.default' })).toThrowError(
      /KAFKA_TOPIC_PREFIX=public\.default is invalid/,
    );
    expect(() => loadConfig({ KAFKA_TOPIC_PREFIX: '.public.' })).toThrowError(/KAFKA_TOPIC_PREFIX/);
    expect(() => loadConfig({ KAFKA_TOPIC_PREFIX: 'pub lic.' })).toThrowError(/KAFKA_TOPIC_PREFIX/);
  });
});

describe('loadConfig — public runner-tunnel posture', () => {
  it('defaults to no binding-token allow-list (loopback-only remote posture)', () => {
    const config = loadConfig({});
    expect(config.runnerTunnelTokens.size).toBe(0);
    expect(config.runnerTunnelAllowedOrigins.size).toBe(0);
    // Self-hosted single-user default: CSWSH local mode on.
    expect(config.runnerTunnelLocalMode).toBe(true);
  });

  it('parses a comma-separated binding-token allow-list, trimming + de-duping', () => {
    const config = loadConfig({ RUNNER_TUNNEL_TOKENS: ' tok-a, tok-b ,tok-a, ,tok-c ' });
    expect([...config.runnerTunnelTokens].sort()).toEqual(['tok-a', 'tok-b', 'tok-c']);
  });

  it('parses an extra-allowed-origins list', () => {
    const config = loadConfig({
      RUNNER_TUNNEL_ALLOWED_ORIGINS: 'https://app.example.com, https://ops.example.com',
    });
    expect([...config.runnerTunnelAllowedOrigins].sort()).toEqual([
      'https://app.example.com',
      'https://ops.example.com',
    ]);
  });

  it('honors RUNNER_TUNNEL_LOCAL_MODE=false', () => {
    expect(loadConfig({ RUNNER_TUNNEL_LOCAL_MODE: 'false' }).runnerTunnelLocalMode).toBe(false);
    expect(loadConfig({ RUNNER_TUNNEL_LOCAL_MODE: 'true' }).runnerTunnelLocalMode).toBe(true);
  });

  it('rejects a non-boolean RUNNER_TUNNEL_LOCAL_MODE', () => {
    expect(() => loadConfig({ RUNNER_TUNNEL_LOCAL_MODE: 'maybe' })).toThrowError(
      /RUNNER_TUNNEL_LOCAL_MODE=maybe is not recognized/,
    );
  });
});

describe('loadConfig — Pulsar auth parsing', () => {
  it('parses token auth from env', () => {
    const config = loadConfig({
      PULSAR_AUTH_TYPE: 'token',
      PULSAR_AUTH_TOKEN: 'pulsar-token',
    });

    expect(config.pulsarAuth).toEqual({ type: 'token', token: 'pulsar-token' });
  });

  it('parses OAuth2 auth from env', () => {
    const config = loadConfig({
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

  it('rejects token auth without a token', () => {
    expect(() => loadConfig({ PULSAR_AUTH_TYPE: 'token' })).toThrowError(
      /PULSAR_AUTH_TOKEN is required/,
    );
  });
});

describe('loadConfig — SecretStore selection', () => {
  it('allows the in-memory store only for local development and tests', () => {
    expect(
      loadConfig({ ORCA_SECRET_STORE_MODE: 'local', NODE_ENV: 'development' }).secretStoreMode,
    ).toBe('local');
    expect(loadConfig({ ORCA_SECRET_STORE_MODE: 'local', NODE_ENV: 'test' }).secretStoreMode).toBe(
      'local',
    );
  });

  it('rejects the in-memory store without an explicit dev/test runtime', () => {
    expect(() => loadConfig({ ORCA_SECRET_STORE_MODE: 'local' })).toThrowError(
      /requires NODE_ENV=development or test/,
    );
    expect(() =>
      loadConfig({ ORCA_SECRET_STORE_MODE: 'local', NODE_ENV: 'production' }),
    ).toThrowError(/forbidden in deployed registry instances/);
    expect(() =>
      loadConfig({
        ORCA_SECRET_STORE_MODE: 'local',
        NODE_ENV: 'test',
        KUBERNETES_SERVICE_HOST: '10.0.0.1',
      }),
    ).toThrowError(/forbidden in deployed registry instances/);
  });

  it('requires a namespace for the Kubernetes store and defaults its Secret name', () => {
    expect(() => loadConfig({ ORCA_SECRET_STORE_MODE: 'kubernetes' })).toThrowError(
      /ORCA_SECRET_STORE_K8S_NAMESPACE is required/,
    );

    const config = loadConfig({
      ORCA_SECRET_STORE_MODE: 'kubernetes',
      ORCA_SECRET_STORE_K8S_NAMESPACE: 'orca-system',
    });
    expect(config.secretStoreMode).toBe('kubernetes');
    expect(config.secretStoreKubernetesNamespace).toBe('orca-system');
    expect(config.secretStoreKubernetesSecretName).toBe('registry-secret-store');
  });

  it('accepts an explicit Kubernetes Secret name', () => {
    const config = loadConfig({
      ORCA_SECRET_STORE_MODE: 'kubernetes',
      ORCA_SECRET_STORE_K8S_NAMESPACE: 'orca-system',
      ORCA_SECRET_STORE_K8S_SECRET_NAME: 'managed-agent-credentials',
    });
    expect(config.secretStoreKubernetesSecretName).toBe('managed-agent-credentials');
  });
});

describe('loadConfig — database pool sizing', () => {
  it('defaults every pool ceiling to 10', () => {
    const config = loadConfig({});

    expect(config.databasePoolMax).toBe(10);
    expect(config.transcriptStorePoolMax).toBe(10);
    expect(config.fileStorePoolMax).toBe(10);
    expect(config.memorystorePoolMax).toBe(10);
  });

  it('reads per-pool overrides from the environment', () => {
    const config = loadConfig({
      DATABASE_POOL_MAX: '12',
      TRANSCRIPT_STORE_POOL_MAX: '4',
      FILESTORE_POOL_MAX: '6',
      MEMORYSTORE_POOL_MAX: '3',
    });

    expect(config.databasePoolMax).toBe(12);
    expect(config.transcriptStorePoolMax).toBe(4);
    expect(config.fileStorePoolMax).toBe(6);
    expect(config.memorystorePoolMax).toBe(3);
  });

  it('rejects non-positive or non-integer pool sizes', () => {
    expect(() => loadConfig({ DATABASE_POOL_MAX: '0' })).toThrowError(
      /DATABASE_POOL_MAX=0 is not a positive integer/,
    );
    expect(() => loadConfig({ FILESTORE_POOL_MAX: 'ten' })).toThrowError(
      /FILESTORE_POOL_MAX=ten is not a positive integer/,
    );
  });
});

describe('loadConfig — OIDC metadata claim fallback', () => {
  it('leaves every plane off when the variables are unset', () => {
    expect(loadConfig({})).toMatchObject({
      oidcMetadataClaims: false,
      adminOidcMetadataClaims: false,
      platformOidcMetadataClaims: false,
    });
  });

  it('enables each plane independently', () => {
    expect(loadConfig({ OIDC_METADATA_CLAIMS: 'true' })).toMatchObject({
      oidcMetadataClaims: true,
      adminOidcMetadataClaims: false,
      platformOidcMetadataClaims: false,
    });
    expect(loadConfig({ ADMIN_OIDC_METADATA_CLAIMS: '1' })).toMatchObject({
      oidcMetadataClaims: false,
      adminOidcMetadataClaims: true,
      platformOidcMetadataClaims: false,
    });
    expect(loadConfig({ PLATFORM_OIDC_METADATA_CLAIMS: 'yes' })).toMatchObject({
      oidcMetadataClaims: false,
      adminOidcMetadataClaims: false,
      platformOidcMetadataClaims: true,
    });
  });

  it('rejects an unrecognized value rather than reading it as off', () => {
    expect(() => loadConfig({ OIDC_METADATA_CLAIMS: 'enabled' })).toThrowError(
      /OIDC_METADATA_CLAIMS=enabled is not recognized/,
    );
  });
});

describe('loadConfig — OIDC denied-JTI list', () => {
  it('leaves revocation checking off when the variable is unset or empty', () => {
    expect(loadConfig({}).oidcDeniedJtiFile).toBeUndefined();
    expect(loadConfig({ OIDC_DENIED_JTI_FILE: '' }).oidcDeniedJtiFile).toBeUndefined();
  });

  it('reads the configured path', () => {
    expect(
      loadConfig({ OIDC_DENIED_JTI_FILE: '/etc/orca/revocation-list.json' }).oidcDeniedJtiFile,
    ).toBe('/etc/orca/revocation-list.json');
  });
});

describe('loadConfig — workspace resolution by organization audience', () => {
  it('leaves the workspace plane on its static audience check by default', () => {
    expect(loadConfig({}).oidcResolveWorkspaceByAudience).toBe(false);
  });

  it('enables the mode explicitly', () => {
    expect(
      loadConfig({ OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE: 'true' }).oidcResolveWorkspaceByAudience,
    ).toBe(true);
  });

  it('rejects an unrecognized value rather than reading it as off', () => {
    // Silently reading a typo as "off" would leave an operator believing
    // organization-scoped credentials are accepted while every one of them is
    // rejected by the static audience check instead.
    expect(() => loadConfig({ OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE: 'on' })).toThrowError(
      /OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE=on is not recognized/,
    );
  });
});

describe('loadConfig — model price refresh', () => {
  it('treats an unset refresh URL as a supported configuration, not an error', () => {
    const config = loadConfig({});

    expect(config.priceRefreshUrl).toBeUndefined();
    expect(config.priceRefreshIntervalMs).toBeGreaterThan(0);
  });

  it('reads the refresh URL and interval from the environment', () => {
    const config = loadConfig({
      PRICE_REFRESH_URL: 'https://prices.example/catalog.json',
      PRICE_REFRESH_INTERVAL_MS: '3600000',
    });

    expect(config.priceRefreshUrl).toBe('https://prices.example/catalog.json');
    expect(config.priceRefreshIntervalMs).toBe(3_600_000);
  });

  it('rejects a refresh URL that is not an absolute http(s) URL', () => {
    for (const url of ['prices.example/catalog.json', 'file:///etc/passwd', 'not a url']) {
      expect(() => loadConfig({ PRICE_REFRESH_URL: url })).toThrowError(/PRICE_REFRESH_URL/);
    }
  });

  it('rejects an interval that is not a positive integer', () => {
    expect(() => loadConfig({ PRICE_REFRESH_INTERVAL_MS: '0' })).toThrowError(
      /PRICE_REFRESH_INTERVAL_MS/,
    );
    expect(() => loadConfig({ PRICE_REFRESH_INTERVAL_MS: 'hourly' })).toThrowError(
      /PRICE_REFRESH_INTERVAL_MS/,
    );
  });

  it('rejects an interval short enough to hammer the upstream catalog', () => {
    expect(() => loadConfig({ PRICE_REFRESH_INTERVAL_MS: '1000' })).toThrowError(
      /PRICE_REFRESH_INTERVAL_MS/,
    );
  });
});
describe('Kafka transcript schema configuration', () => {
  it('passes reader-first Registry configuration independently of broker authentication', () => {
    const config = loadConfig({
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
        TRANSCRIPT_STORE_BACKEND: 'postgres',
        KAFKA_SCHEMA_REGISTRY_URL: 'http://schemas.local',
      }),
    ).toThrow(/kafka/i);
  });

  it('keeps schema integration disabled by default', () => {
    const config = loadConfig({});
    expect(config.kafkaTranscript).toBeDefined();
    expect(config.kafkaTranscript?.encoding ?? 'raw').toBe('raw');
    expect(config.kafkaTranscript?.schemaRegistry).toBeUndefined();
  });

  it('rejects insecure Registry credentials without exposing them', () => {
    let failure: unknown;
    try {
      loadConfig({
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
    expect(() => loadConfig({ KAFKA_TRANSCRIPT_ENCODING: 'avro' })).toThrow(
      /KAFKA_SCHEMA_REGISTRY_URL/,
    );
  });
});
