// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import fs from 'node:fs';
import { isIP } from 'node:net';
import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import { SEED_PRICE_PROVIDER } from '@orca/harness-catalog';
import {
  validateTopicPrefix,
  parseKafkaTranscriptConfig,
  type KafkaTranscriptCodecOptions,
  type PulsarAuthConfig,
} from '@orca/transcript-store';

export type TranscriptStoreBackend = 'kafka' | 'postgres' | 'pulsar';
export type KafkaConnectionMode =
  | 'plaintext'
  | 'sasl-plain-token-tls'
  | 'sasl-plain-tls'
  | 'custom';
export type SecretStoreMode = 'none' | 'local' | 'kubernetes';
export type InternalAuthMode = 'static_token' | 'kubernetes_service_account';

export interface KafkaClientConfig {
  brokers: string[];
  clientId: string;
  ssl?: boolean | TlsConnectionOptions;
  sasl?: { mechanism: 'plain'; username: string; password: string };
  connectionTimeout?: number;
  authenticationTimeout?: number;
}

export interface ServiceConfig {
  httpPort: number;
  batchReadsEnabled: boolean;
  apiKeyProofCacheEnabled: boolean;
  heavyReadAdmission: import('./middleware/read-admission.js').ReadAdmissionOptions;
  internalHttpPort: number;
  adminHttpPort: number;
  trustedProxyCidrs: string[];
  internalAuthMode: InternalAuthMode;
  internalServiceToken: string | undefined;
  internalServiceTokenFile: string | undefined;
  internalAuthAudience: string;
  internalAuthHarnessSubject: string | undefined;
  internalAuthAiGatewaySubject: string | undefined;
  internalAuthObservabilityExporterSubject: string | undefined;
  grpcPort: number;
  databaseUrl: string;
  oidcAllowedIssuers: string[];
  oidcAudience: string;
  adminOidcAllowedIssuers: string[];
  adminOidcAudience: string;
  platformOidcAllowedIssuers: string[];
  platformOidcAudience: string;
  /**
   * Per-plane opt-in to reading identity and scope claims from a verified
   * token's nested `metadata` object, from `OIDC_METADATA_CLAIMS`,
   * `ADMIN_OIDC_METADATA_CLAIMS` and `PLATFORM_OIDC_METADATA_CLAIMS`. All three
   * default to false: enable a plane only for issuers whose `metadata` is
   * issuer-controlled rather than supplied by whoever requests the credential.
   */
  oidcMetadataClaims: boolean;
  adminOidcMetadataClaims: boolean;
  platformOidcMetadataClaims: boolean;
  /**
   * Optional path to a JSON file of revoked token identifiers, from
   * `OIDC_DENIED_JTI_FILE`. Applies to all three OIDC planes (workspace, admin
   * and platform); unset leaves revocation checking off.
   */
  oidcDeniedJtiFile: string | undefined;
  /**
   * Workspace plane only, from `OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE`. Accept
   * organization-scoped credentials whose `aud` names an organization's
   * configured audience, resolve the workspace server-side, and bind every
   * workspace-plane token to the audience of the organization owning its
   * workspace. Defaults to false, which leaves the plane's static
   * `OIDC_AUDIENCE` check exactly as it was.
   *
   * `OIDC_AUDIENCE` must be EMPTY when this is on and non-empty when it is off
   * on a plane that has issuers; the registry refuses to start otherwise. The
   * defaults above are non-empty, so only an explicitly empty value can trip
   * that assertion.
   */
  oidcResolveWorkspaceByAudience: boolean;
  kafkaBrokers: string[];
  kafkaClientId: string;
  kafka: KafkaClientConfig;
  kafkaTranscript?: KafkaTranscriptCodecOptions;
  /**
   * Optional Kafka topic prefix (dot-terminated segments, e.g.
   * `public.default.`), parsed from `KAFKA_TOPIC_PREFIX`. Only meaningful for
   * the kafka transcript backend. Kafka-on-Pulsar (KoP) endpoints interpret
   * dotted Kafka topic names as `<tenant>.<namespace>.<local-topic>`,
   * so session topics must be prefixed with an existing tenant/namespace pair.
   * Kept OUTSIDE {@link kafka} because that object is passed verbatim to
   * `new Kafka(...)` (kafkajs). Defaults to '' (bare names, plain Kafka).
   */
  kafkaTopicPrefix: string;
  transcriptStoreBackend: TranscriptStoreBackend;
  transcriptStoreDatabaseUrl: string;
  pulsarServiceUrl: string;
  pulsarAuth?: PulsarAuthConfig;
  pulsarTenant: string;
  pulsarNamespace: string;
  pulsarTopicPrefix: string;
  sseBufferSize: number;
  sseDropAgeMs: number;
  sseHeartbeatMs: number;
  /**
   * Heartbeat staleness TTL for durable environment claims, in milliseconds. A
   * claim whose `last_ping` is older than this is reapable (see
   * `src/domain/environment-claims.ts`). Default 90s ≈ 3 missed 30s worker
   * heartbeats. Parsed from `ENVIRONMENT_CLAIM_TTL_MS`.
   */
  environmentClaimTtlMs: number;
  /** Whether colocated LLM usage is written by AI Gateway's Registry sink. */
  gatewayRegistryUsageEnabled: boolean;
  sessionJwtPrivateKeyPem: string;
  sessionJwtIssuer: string;
  sessionJwtAudience: string;
  sessionJwtTtlSecs: number;
  sessionJwtLlmPolicy: SessionJwtLlmPolicy | undefined;
  fileStoreDatabaseUrl: string;
  memorystoreDatabaseUrl: string;
  /**
   * Per-pool node-postgres connection ceilings, from `DATABASE_POOL_MAX`,
   * `TRANSCRIPT_STORE_POOL_MAX`, `FILESTORE_POOL_MAX` and `MEMORYSTORE_POOL_MAX`
   * (10 each by default). Each opens an independent pool, so a registry
   * process's worst-case connection footprint is the sum of all four. Kept
   * env-tunable so several deployments can share one Postgres without
   * exhausting its `max_connections`.
   */
  databasePoolMax: number;
  transcriptStorePoolMax: number;
  fileStorePoolMax: number;
  memorystorePoolMax: number;
  /** Run the cron Trigger planner and dispatcher in this Registry process. */
  triggerSchedulerEnabled: boolean;
  triggerReconcileIntervalMs: number;
  triggerReconcileBatchSize: number;
  /** Shared object-store root; canonical builders append workspace/resource segments. */
  s3KeyPrefix: string;
  s3Endpoint: string;
  s3ForcePathStyle: boolean;
  s3Bucket: string;
  s3Region: string;
  /** Optional static credentials. When absent, AWS SDK uses its default provider chain. */
  s3AccessKeyId: string | undefined;
  s3SecretAccessKey: string | undefined;
  secretStoreMode: SecretStoreMode;
  /**
   * Binding-token allow-list for the PUBLIC runner tunnel
   * (`/v1/tunnels/runners/:runnerId`), parsed from `RUNNER_TUNNEL_TOKENS`
   * (comma-separated). The runner tunnel is reachable off-mesh, so this is the
   * operator's opt-in to admit REMOTE runners: only these exact tokens may open a
   * remote tunnel, and each registers under a stable (not token-derived) id.
   * Defaults to empty — the tunnel then accepts loopback runners only and fails
   * closed for every remote peer (see `src/auth/tunnel-auth.ts`). Opening the
   * tunnel to remote runners is therefore always explicit, never the silent
   * default of a `0.0.0.0` listener.
   */
  runnerTunnelTokens: ReadonlySet<string>;
  /**
   * Extra permitted WS `Origin` values for the runner tunnel's CSWSH guard,
   * parsed from `RUNNER_TUNNEL_ALLOWED_ORIGINS` (comma-separated), beyond the
   * internal sentinel + loopback hosts. Defaults to empty.
   */
  runnerTunnelAllowedOrigins: ReadonlySet<string>;
  /**
   * Runner-tunnel CSWSH local-mode flag, parsed from `RUNNER_TUNNEL_LOCAL_MODE`.
   * In local mode an `Origin` is allowed only when its hostname is loopback (the
   * single-user posture with no cookie/proxy auth). Defaults to `true` for the
   * self-hosted single-user deployment.
   */
  runnerTunnelLocalMode: boolean;
  /**
   * Public ai-gateway MCP base URL, parsed from `AI_GATEWAY_MCP_URL`. Gateway
   * egress (the default `egress_mode`) rewrites the agent's MCP servers in the
   * credential-free snapshot delivered to a runner at session start to this URL
   * and carries a scoped session JWT. The snapshot resolver is always wired:
   * undefined → a gateway-egress session's snapshot fails to resolve, while a
   * `sidecar` session resolves without it.
   */
  aiGatewayMcpUrl?: string;
  /**
   * Public ai-gateway LLM-proxy base URL, parsed from `AI_GATEWAY_LLM_URL`. When
   * set alongside {@link aiGatewayMcpUrl}, the gateway egress config carries an
   * LLM-proxy base URL + a scoped JWT so the runner reaches the LLM through the
   * gateway without holding a provider key: `aud='llm-proxy'`, except that a
   * `codex_sdk` or `pi_sdk` session gets the session JWT audience with
   * `llm_routes`/`llm_models` claims. Optional.
   */
  aiGatewayLlmUrl?: string;
  /**
   * Lifetime (seconds) of the LLM-proxy JWT, parsed from
   * `AI_GATEWAY_LLM_JWT_TTL_SECS`. The LLM token is an independently-scoped,
   * short-lived credential minted with its own TTL — deliberately shorter than
   * the session/MCP JWT's `SESSION_JWT_TTL_SECS` (default 300s) so a leaked LLM
   * token expires fast. Defaults to {@link DEFAULT_LLM_PROXY_JWT_TTL_SECS} (120s)
   * when unset; a non-positive value falls back to that default in the resolver.
   * A `codex_sdk` or `pi_sdk` token lives at least 660s, to cover the bounded
   * model turn.
   */
  aiGatewayLlmJwtTtlSecs?: number;
  secretStoreKubernetesNamespace: string;
  secretStoreKubernetesSecretName: string;
  /**
   * Upstream model-price catalog. Leaving it unset is a supported
   * configuration, not a degraded one: the deployment runs on its seed and
   * operator entries and never reaches the network.
   */
  priceRefreshUrl: string | undefined;
  /** Which provider `PRICE_REFRESH_URL` serves. One feed prices one vendor. */
  priceRefreshProvider: string;
  priceRefreshIntervalMs: number;
}

export interface SessionJwtLlmPolicy {
  routes: string[];
  models: string[];
}

function parseSessionJwtLlmPolicy(env: NodeJS.ProcessEnv): SessionJwtLlmPolicy | undefined {
  const rawRoutes = env['SESSION_JWT_LLM_ROUTES'];
  const rawModels = env['SESSION_JWT_LLM_MODELS'];
  if (rawRoutes === undefined && rawModels === undefined) return undefined;
  if (rawRoutes === undefined || rawModels === undefined) {
    throw new Error(
      'SESSION_JWT_LLM_ROUTES and SESSION_JWT_LLM_MODELS must be configured together',
    );
  }

  const parseList = (name: string, raw: string): string[] => {
    const entries = raw.split(',').map((entry) => entry.trim());
    if (entries.some((entry) => entry.length === 0)) {
      throw new Error(`${name} contains an empty entry`);
    }
    return [...new Set(entries)];
  };

  return {
    routes: parseList('SESSION_JWT_LLM_ROUTES', rawRoutes),
    models: parseList('SESSION_JWT_LLM_MODELS', rawModels),
  };
}

function parseTranscriptStoreBackend(raw: string | undefined): TranscriptStoreBackend {
  const value = (raw ?? 'kafka').toLowerCase();
  if (value === 'kafka' || value === 'postgres' || value === 'pulsar') return value;
  throw new Error(
    `TRANSCRIPT_STORE_BACKEND=${raw} is not recognized; expected kafka, postgres, or pulsar`,
  );
}

const SAFE_S3_ROOT_SEGMENT = /^[A-Za-z0-9._-]+$/;

function normalizeS3KeyPrefix(raw: string | undefined): string {
  const value = raw ?? 'managed-agents/';
  if (value === '') return '';
  if (value.startsWith('/') || value.includes('\\') || value.includes('\0')) {
    throw new Error(`S3_KEY_PREFIX=${value} is invalid`);
  }
  const withoutTrailingSlash = value.endsWith('/') ? value.slice(0, -1) : value;
  const segments = withoutTrailingSlash.split('/');
  if (
    segments.some(
      (segment) =>
        segment.length === 0 ||
        segment === '.' ||
        segment === '..' ||
        !SAFE_S3_ROOT_SEGMENT.test(segment),
    )
  ) {
    throw new Error(`S3_KEY_PREFIX=${value} is invalid`);
  }
  return `${withoutTrailingSlash}/`;
}

function requiredEnv(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name];
  if (value === undefined || value === '') throw new Error(`${name} is required`);
  return value;
}

function optionalEnv(env: NodeJS.ProcessEnv, name: string): string | undefined {
  const value = env[name];
  return value === undefined || value === '' ? undefined : value;
}

interface S3StaticCredentials {
  accessKeyId: string;
  secretAccessKey: string;
}

function readS3StaticCredentialPair(
  env: NodeJS.ProcessEnv,
  accessKeyName: string,
  secretKeyName: string,
): S3StaticCredentials | undefined {
  const accessKeyId = optionalEnv(env, accessKeyName);
  const secretAccessKey = optionalEnv(env, secretKeyName);
  if ((accessKeyId === undefined) !== (secretAccessKey === undefined)) {
    throw new Error(
      `S3 static credentials must configure both access-key and secret-key variables in the ${accessKeyName}/${secretKeyName} alias pair`,
    );
  }
  if (accessKeyId === undefined || secretAccessKey === undefined) return undefined;
  return { accessKeyId, secretAccessKey };
}

function resolveS3StaticCredentials(env: NodeJS.ProcessEnv): S3StaticCredentials | undefined {
  const canonical = readS3StaticCredentialPair(env, 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY');
  const legacy = readS3StaticCredentialPair(env, 'S3_ACCESS_KEY', 'S3_SECRET_KEY');
  if (
    canonical !== undefined &&
    legacy !== undefined &&
    (canonical.accessKeyId !== legacy.accessKeyId ||
      canonical.secretAccessKey !== legacy.secretAccessKey)
  ) {
    throw new Error('S3 static credential alias pairs must match when both are configured');
  }
  return canonical ?? legacy;
}

function parsePositiveInteger(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = optionalEnv(env, name);
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name}=${raw} is not a positive integer`);
  }
  return value;
}

function parseTrustedProxyCidrs(env: NodeJS.ProcessEnv): string[] {
  const values = (optionalEnv(env, 'TRUST_PROXY_CIDRS') ?? '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);

  for (const value of values) {
    const slash = value.lastIndexOf('/');
    const address = slash === -1 ? value : value.slice(0, slash);
    const ipVersion = isIP(address);
    if (ipVersion === 0) {
      throw new Error(`TRUST_PROXY_CIDRS contains an invalid IP or CIDR: ${value}`);
    }
    if (slash === -1) continue;

    const prefixText = value.slice(slash + 1);
    const prefix = Number(prefixText);
    const maxPrefix = ipVersion === 4 ? 32 : 128;
    if (!/^\d+$/.test(prefixText) || prefix < 1 || prefix > maxPrefix) {
      throw new Error(`TRUST_PROXY_CIDRS contains an invalid or trust-all CIDR prefix: ${value}`);
    }
  }

  return [...new Set(values)];
}

const KAFKA_CONNECTION_MODES: ReadonlyArray<KafkaConnectionMode> = [
  'plaintext',
  'sasl-plain-token-tls',
  'sasl-plain-tls',
  'custom',
];

// kafkajs defaults connectionTimeout/authenticationTimeout to 1s, which the
// TLS + SASL handshake against remote endpoints routinely exceeds (observed
// against Kafka-on-Pulsar (KoP)); raise both for every TLS-bearing mode.
const TLS_KAFKA_TIMEOUT_MS = 10_000;

function parseBooleanEnv(env: NodeJS.ProcessEnv, name: string, fallback = false): boolean {
  const raw = optionalEnv(env, name);
  if (raw === undefined) return fallback;
  const value = raw.toLowerCase();
  if (value === 'true' || value === '1' || value === 'yes') return true;
  if (value === 'false' || value === '0' || value === 'no') return false;
  throw new Error(`${name}=${raw} is not recognized; expected true or false`);
}

function readKafkaTlsFile(env: NodeJS.ProcessEnv, name: string): Buffer | undefined {
  const file = optionalEnv(env, name);
  if (file === undefined) return undefined;
  try {
    return fs.readFileSync(file);
  } catch {
    throw new Error(`${name} points to an unreadable file: ${file}`);
  }
}

function parseCustomKafkaSsl(env: NodeJS.ProcessEnv): KafkaClientConfig['ssl'] | undefined {
  const ca = readKafkaTlsFile(env, 'KAFKA_SSL_CA_FILE');
  const cert = readKafkaTlsFile(env, 'KAFKA_SSL_CERT_FILE');
  const key = readKafkaTlsFile(env, 'KAFKA_SSL_KEY_FILE');
  const rejectUnauthorizedRaw = optionalEnv(env, 'KAFKA_SSL_REJECT_UNAUTHORIZED');
  const sslEnabled = parseBooleanEnv(env, 'KAFKA_SSL', false);

  if (
    ca === undefined &&
    cert === undefined &&
    key === undefined &&
    rejectUnauthorizedRaw === undefined
  ) {
    return sslEnabled ? true : undefined;
  }

  const ssl: TlsConnectionOptions = {};
  if (ca !== undefined) ssl.ca = ca;
  if (cert !== undefined) ssl.cert = cert;
  if (key !== undefined) ssl.key = key;
  if (rejectUnauthorizedRaw !== undefined) {
    ssl.rejectUnauthorized = parseBooleanEnv(env, 'KAFKA_SSL_REJECT_UNAUTHORIZED', true);
  }
  return ssl;
}

function parseKafkaConfig(env: NodeJS.ProcessEnv): KafkaClientConfig {
  const brokers = (env['KAFKA_BROKERS'] ?? 'localhost:9092')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  if (brokers.length === 0) throw new Error('KAFKA_BROKERS must include at least one broker');

  const clientId = env['KAFKA_CLIENT_ID'] ?? 'registry-service-ts';
  const rawMode = optionalEnv(env, 'KAFKA_CONNECTION_MODE') ?? 'plaintext';
  const mode = rawMode.toLowerCase();
  if (!KAFKA_CONNECTION_MODES.includes(mode as KafkaConnectionMode)) {
    throw new Error(
      `KAFKA_CONNECTION_MODE=${rawMode} is not recognized; expected one of: ${KAFKA_CONNECTION_MODES.join(', ')}`,
    );
  }

  const base: KafkaClientConfig = { brokers, clientId };
  switch (mode as KafkaConnectionMode) {
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
      const ssl = parseCustomKafkaSsl(env);
      const saslMechanism = optionalEnv(env, 'KAFKA_SASL_MECHANISM');
      const config: KafkaClientConfig = { ...base };
      if (ssl !== undefined) {
        config.ssl = ssl;
        config.connectionTimeout = TLS_KAFKA_TIMEOUT_MS;
        config.authenticationTimeout = TLS_KAFKA_TIMEOUT_MS;
      }
      if (saslMechanism !== undefined) {
        if (saslMechanism.toLowerCase() !== 'plain') {
          throw new Error(`KAFKA_SASL_MECHANISM=${saslMechanism} is not supported; expected plain`);
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

function parseKafkaTopicPrefix(env: NodeJS.ProcessEnv): string {
  const raw = optionalEnv(env, 'KAFKA_TOPIC_PREFIX');
  if (raw === undefined) return '';
  try {
    validateTopicPrefix(raw);
  } catch {
    throw new Error(
      `KAFKA_TOPIC_PREFIX=${raw} is invalid; expected dot-terminated segments such as "public.default."`,
    );
  }
  return raw;
}

function parsePulsarAuth(env: NodeJS.ProcessEnv): PulsarAuthConfig | undefined {
  const raw = optionalEnv(env, 'PULSAR_AUTH_TYPE');
  if (!raw || raw.toLowerCase() === 'none') return undefined;
  const type = raw.toLowerCase();
  if (type === 'token') {
    return { type: 'token', token: requiredEnv(env, 'PULSAR_AUTH_TOKEN') };
  }
  if (type === 'oauth2') {
    const auth: PulsarAuthConfig = {
      type: 'oauth2',
      issuerUrl: requiredEnv(env, 'PULSAR_OAUTH2_ISSUER_URL'),
    };
    const clientId = optionalEnv(env, 'PULSAR_OAUTH2_CLIENT_ID');
    const clientSecret = optionalEnv(env, 'PULSAR_OAUTH2_CLIENT_SECRET');
    const privateKey = optionalEnv(env, 'PULSAR_OAUTH2_PRIVATE_KEY');
    const audience = optionalEnv(env, 'PULSAR_OAUTH2_AUDIENCE');
    const scope = optionalEnv(env, 'PULSAR_OAUTH2_SCOPE');
    const oauth2Type = optionalEnv(env, 'PULSAR_OAUTH2_TYPE');
    if (clientId !== undefined) auth.clientId = clientId;
    if (clientSecret !== undefined) auth.clientSecret = clientSecret;
    if (privateKey !== undefined) auth.privateKey = privateKey;
    if (audience !== undefined) auth.audience = audience;
    if (scope !== undefined) auth.scope = scope;
    if (oauth2Type !== undefined) auth.oauth2Type = oauth2Type;
    return auth;
  }
  throw new Error(`PULSAR_AUTH_TYPE=${raw} is not recognized; expected token, oauth2, or none`);
}

function parseSecretStoreMode(env: NodeJS.ProcessEnv): SecretStoreMode {
  const raw = env['ORCA_SECRET_STORE_MODE'];
  const value = (raw ?? 'none').toLowerCase();
  if (value !== 'none' && value !== 'local' && value !== 'kubernetes') {
    throw new Error(
      `ORCA_SECRET_STORE_MODE=${raw} is not recognized; expected none, local, or kubernetes`,
    );
  }
  const nodeEnv = env['NODE_ENV']?.toLowerCase();
  if (
    value === 'local' &&
    (optionalEnv(env, 'KUBERNETES_SERVICE_HOST') ||
      (nodeEnv !== 'development' && nodeEnv !== 'test'))
  ) {
    throw new Error(
      'ORCA_SECRET_STORE_MODE=local requires NODE_ENV=development or test and is forbidden in deployed registry instances',
    );
  }
  return value;
}

function parseInternalAuthMode(env: NodeJS.ProcessEnv): InternalAuthMode {
  const raw = optionalEnv(env, 'INTERNAL_AUTH_MODE');
  const value = (
    raw ??
    (optionalEnv(env, 'KUBERNETES_SERVICE_HOST') ? 'kubernetes_service_account' : 'static_token')
  ).toLowerCase();
  if (value === 'static_token' || value === 'kubernetes_service_account') return value;
  throw new Error(
    `INTERNAL_AUTH_MODE=${raw} is not recognized; expected static_token or kubernetes_service_account`,
  );
}

/**
 * Parse a comma-separated env var into a de-duplicated set of trimmed,
 * non-empty values. An unset or all-blank value yields an empty set. Used for
 * the runner-tunnel binding-token allow-list and origin allow-list, where order
 * is irrelevant and membership is the only question.
 */
function parseCsvSet(env: NodeJS.ProcessEnv, name: string): ReadonlySet<string> {
  const raw = optionalEnv(env, name);
  if (raw === undefined) return new Set<string>();
  return new Set(
    raw
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0),
  );
}

/**
 * Vendors reprice on a scale of months, so six hours is already far more often
 * than the data changes. The point of the schedule is bounded staleness, not
 * freshness.
 */
const DEFAULT_PRICE_REFRESH_INTERVAL_MS = 6 * 60 * 60 * 1000;

/**
 * A floor rather than a preference. Anything under a minute is a
 * misconfiguration that would hammer a third-party endpoint from every replica.
 */
const MIN_PRICE_REFRESH_INTERVAL_MS = 60_000;

function parsePriceRefreshUrl(env: NodeJS.ProcessEnv): string | undefined {
  const raw = optionalEnv(env, 'PRICE_REFRESH_URL');
  if (raw === undefined) return undefined;
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    throw new Error(`PRICE_REFRESH_URL=${raw} is not an absolute URL`);
  }
  // Restricted to the two schemes a catalog is actually served over: `file:`
  // would turn a configuration value into a local-file read.
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    throw new Error(`PRICE_REFRESH_URL=${raw} must use http or https`);
  }
  return raw;
}

/**
 * The provider whose rates `PRICE_REFRESH_URL` publishes.
 *
 * Defaulted rather than required so an Anthropic-only deployment configures a
 * URL and nothing else. It is validated because it is an identity the refresh
 * writes under: an unreadable value would create a provider nothing resolves
 * against, and every refreshed model would silently go unpriced.
 */
function parsePriceRefreshProvider(env: NodeJS.ProcessEnv): string {
  const raw = optionalEnv(env, 'PRICE_REFRESH_PROVIDER');
  if (raw === undefined) return SEED_PRICE_PROVIDER;
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(raw)) {
    throw new Error(`PRICE_REFRESH_PROVIDER=${raw} must be an addressable identifier`);
  }
  return raw;
}

function parsePriceRefreshIntervalMs(env: NodeJS.ProcessEnv): number {
  const raw = optionalEnv(env, 'PRICE_REFRESH_INTERVAL_MS');
  if (raw === undefined) return DEFAULT_PRICE_REFRESH_INTERVAL_MS;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < MIN_PRICE_REFRESH_INTERVAL_MS) {
    throw new Error(
      `PRICE_REFRESH_INTERVAL_MS=${raw} must be an integer of at least ${MIN_PRICE_REFRESH_INTERVAL_MS}`,
    );
  }
  return value;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): ServiceConfig {
  const rawKey = env['SESSION_JWT_PRIVATE_KEY_PEM'] ?? '';
  let pem = rawKey;
  if (rawKey && !rawKey.includes('BEGIN')) {
    // Treat as a file path.
    pem = fs.readFileSync(rawKey, 'utf8');
  }
  const kafka = parseKafkaConfig(env);
  const pulsarAuth = parsePulsarAuth(env);
  const secretStoreMode = parseSecretStoreMode(env);
  const internalAuthMode = parseInternalAuthMode(env);
  const s3StaticCredentials = resolveS3StaticCredentials(env);
  return {
    httpPort: Number(env['HTTP_PORT'] ?? 8080),
    batchReadsEnabled: parseBooleanEnv(env, 'REGISTRY_BATCH_READS_ENABLED', true),
    apiKeyProofCacheEnabled: parseBooleanEnv(env, 'REGISTRY_API_KEY_PROOF_CACHE_ENABLED', true),
    heavyReadAdmission: {
      maxConcurrent: parseBooleanEnv(env, 'REGISTRY_HEAVY_READ_ADMISSION_ENABLED')
        ? parsePositiveInteger(env, 'REGISTRY_HEAVY_READ_MAX_CONCURRENT', 4)
        : 0,
      maxPerWorkspace: parsePositiveInteger(env, 'REGISTRY_HEAVY_READ_MAX_PER_WORKSPACE', 2),
      maxQueued: parsePositiveInteger(env, 'REGISTRY_HEAVY_READ_MAX_QUEUED', 32),
      maxQueuedPerWorkspace: parsePositiveInteger(
        env,
        'REGISTRY_HEAVY_READ_MAX_QUEUED_PER_WORKSPACE',
        8,
      ),
      queueTimeoutMs: parsePositiveInteger(env, 'REGISTRY_HEAVY_READ_QUEUE_TIMEOUT_MS', 5000),
    },
    internalHttpPort: Number(env['INTERNAL_HTTP_PORT'] ?? 8081),
    adminHttpPort: Number(env['ADMIN_HTTP_PORT'] ?? 8082),
    trustedProxyCidrs: parseTrustedProxyCidrs(env),
    internalAuthMode,
    internalServiceToken: optionalEnv(env, 'INTERNAL_SERVICE_TOKEN'),
    internalServiceTokenFile: optionalEnv(env, 'INTERNAL_SERVICE_TOKEN_FILE'),
    internalAuthAudience: optionalEnv(env, 'INTERNAL_AUTH_AUDIENCE') ?? 'orca-registry-internal',
    internalAuthHarnessSubject: optionalEnv(env, 'INTERNAL_AUTH_HARNESS_SUBJECT'),
    internalAuthAiGatewaySubject: optionalEnv(env, 'INTERNAL_AUTH_AI_GATEWAY_SUBJECT'),
    internalAuthObservabilityExporterSubject: optionalEnv(
      env,
      'INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT',
    ),
    grpcPort: Number(env['GRPC_PORT'] ?? 50054),
    databaseUrl: env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry',
    oidcAllowedIssuers: (env['OIDC_ALLOWED_ISSUERS'] ?? '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    oidcAudience: env['OIDC_AUDIENCE'] ?? 'orca-managed-agents',
    adminOidcAllowedIssuers: (env['ADMIN_OIDC_ALLOWED_ISSUERS'] ?? '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    adminOidcAudience: env['ADMIN_OIDC_AUDIENCE'] ?? 'orca-managed-agents-admin',
    platformOidcAllowedIssuers: (env['PLATFORM_OIDC_ALLOWED_ISSUERS'] ?? '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    platformOidcAudience: env['PLATFORM_OIDC_AUDIENCE'] ?? 'orca-managed-agents-platform',
    oidcMetadataClaims: parseBooleanEnv(env, 'OIDC_METADATA_CLAIMS'),
    adminOidcMetadataClaims: parseBooleanEnv(env, 'ADMIN_OIDC_METADATA_CLAIMS'),
    platformOidcMetadataClaims: parseBooleanEnv(env, 'PLATFORM_OIDC_METADATA_CLAIMS'),
    oidcDeniedJtiFile: optionalEnv(env, 'OIDC_DENIED_JTI_FILE'),
    oidcResolveWorkspaceByAudience: parseBooleanEnv(env, 'OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE'),
    kafkaBrokers: kafka.brokers,
    kafkaClientId: kafka.clientId,
    kafka,
    kafkaTopicPrefix: parseKafkaTopicPrefix(env),
    transcriptStoreBackend: parseTranscriptStoreBackend(env['TRANSCRIPT_STORE_BACKEND']),
    kafkaTranscript: parseKafkaTranscriptConfig(
      env,
      parseTranscriptStoreBackend(env['TRANSCRIPT_STORE_BACKEND']),
    ),
    transcriptStoreDatabaseUrl:
      env['TRANSCRIPT_STORE_DATABASE_URL'] ??
      env['DATABASE_URL'] ??
      'postgres://orca:orca@localhost:5432/registry',
    pulsarServiceUrl: env['PULSAR_SERVICE_URL'] ?? 'pulsar://localhost:6650',
    // Optional under `exactOptionalPropertyTypes`: omit the key entirely when
    // no Pulsar auth is configured rather than assigning `undefined` (which the
    // strict optional-property type rejects). Mirrors the aiGateway* fields below.
    ...(pulsarAuth !== undefined ? { pulsarAuth } : {}),
    pulsarTenant: env['PULSAR_TENANT'] ?? 'public',
    pulsarNamespace: env['PULSAR_NAMESPACE'] ?? 'default',
    pulsarTopicPrefix: env['PULSAR_TOPIC_PREFIX'] ?? 'orca',
    sseBufferSize: Number(env['SSE_BUFFER_SIZE'] ?? 256),
    sseDropAgeMs: Number(env['SSE_DROP_AGE_MS'] ?? 5000),
    sseHeartbeatMs: Number(env['SSE_HEARTBEAT_MS'] ?? 15000),
    environmentClaimTtlMs: Number(env['ENVIRONMENT_CLAIM_TTL_MS'] ?? 90_000),
    gatewayRegistryUsageEnabled: parseBooleanEnv(env, 'AI_GATEWAY_REGISTRY_USAGE_ENABLED'),
    sessionJwtPrivateKeyPem: pem,
    sessionJwtIssuer: env['SESSION_JWT_ISSUER'] ?? 'orca-registry',
    sessionJwtAudience: env['SESSION_JWT_AUDIENCE'] ?? 'ai-gateway',
    sessionJwtTtlSecs: Number(env['SESSION_JWT_TTL_SECS'] ?? 300),
    sessionJwtLlmPolicy: parseSessionJwtLlmPolicy(env),
    fileStoreDatabaseUrl:
      env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore',
    memorystoreDatabaseUrl:
      env['MEMORYSTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/memorystore',
    databasePoolMax: parsePositiveInteger(env, 'DATABASE_POOL_MAX', 10),
    transcriptStorePoolMax: parsePositiveInteger(env, 'TRANSCRIPT_STORE_POOL_MAX', 10),
    fileStorePoolMax: parsePositiveInteger(env, 'FILESTORE_POOL_MAX', 10),
    memorystorePoolMax: parsePositiveInteger(env, 'MEMORYSTORE_POOL_MAX', 10),
    triggerSchedulerEnabled: parseBooleanEnv(env, 'TRIGGER_SCHEDULER_ENABLED', true),
    triggerReconcileIntervalMs: parsePositiveInteger(env, 'TRIGGER_RECONCILE_INTERVAL_MS', 5_000),
    triggerReconcileBatchSize: parsePositiveInteger(env, 'TRIGGER_RECONCILE_BATCH_SIZE', 100),
    s3KeyPrefix: normalizeS3KeyPrefix(env['S3_KEY_PREFIX']),
    s3Endpoint: env['S3_ENDPOINT'] ?? 'http://localhost:9000',
    s3ForcePathStyle: parseBooleanEnv(env, 'S3_FORCE_PATH_STYLE', true),
    s3Bucket: env['S3_BUCKET'] ?? 'orca-files',
    s3Region: env['S3_REGION'] ?? 'us-east-1',
    s3AccessKeyId: s3StaticCredentials?.accessKeyId,
    s3SecretAccessKey: s3StaticCredentials?.secretAccessKey,
    secretStoreMode,
    secretStoreKubernetesNamespace:
      secretStoreMode === 'kubernetes' ? requiredEnv(env, 'ORCA_SECRET_STORE_K8S_NAMESPACE') : '',
    secretStoreKubernetesSecretName:
      secretStoreMode === 'kubernetes'
        ? (optionalEnv(env, 'ORCA_SECRET_STORE_K8S_SECRET_NAME') ?? 'registry-secret-store')
        : '',
    runnerTunnelTokens: parseCsvSet(env, 'RUNNER_TUNNEL_TOKENS'),
    runnerTunnelAllowedOrigins: parseCsvSet(env, 'RUNNER_TUNNEL_ALLOWED_ORIGINS'),
    runnerTunnelLocalMode: parseBooleanEnv(env, 'RUNNER_TUNNEL_LOCAL_MODE', true),
    ...(env['AI_GATEWAY_MCP_URL'] ? { aiGatewayMcpUrl: env['AI_GATEWAY_MCP_URL'] } : {}),
    ...(env['AI_GATEWAY_LLM_URL'] ? { aiGatewayLlmUrl: env['AI_GATEWAY_LLM_URL'] } : {}),
    ...(env['AI_GATEWAY_LLM_JWT_TTL_SECS']
      ? { aiGatewayLlmJwtTtlSecs: Number(env['AI_GATEWAY_LLM_JWT_TTL_SECS']) }
      : {}),
    priceRefreshUrl: parsePriceRefreshUrl(env),
    priceRefreshProvider: parsePriceRefreshProvider(env),
    priceRefreshIntervalMs: parsePriceRefreshIntervalMs(env),
  };
}
