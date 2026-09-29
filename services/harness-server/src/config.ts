// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readPiProviderCredentials, type PiProviderCredentials } from '@orca/pi-harness';
import fs from 'node:fs';
import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import {
  validateTopicPrefix,
  parseKafkaTranscriptConfig,
  type KafkaTranscriptCodecOptions,
  type PulsarAuthConfig,
} from '@orca/transcript-store';

/**
 * Selectable sandbox runtime backends. Mirrors the implementations under
 * `src/sandbox/{local,e2b,opensandbox,agentenv,in-memory}/runtime.ts`. The harness MUST pick one
 * explicitly via the `SANDBOX_RUNTIME` env var — there is no silent default
 * because each backend has different security + dependency assumptions:
 *
 *   - `local`: spawns agent processes on the host inside an `srt`-managed
 *     sandbox (file/network allow-list enforced by `sandbox-exec` on macOS,
 *     `bubblewrap` on Linux). Requires the `srt` binary on PATH.
 *   - `e2b`: provisions an isolated micro-VM per session via the E2B SDK.
 *     Requires `E2B_API_KEY`.
 *   - `opensandbox`: provisions a remote sandbox through an OpenSandbox
 *     server. Requires `OPEN_SANDBOX_DOMAIN` and `OPEN_SANDBOX_IMAGE`.
 *   - `agentenv`: provisions a Firecracker microVM through an AgentENV
 *     gateway and executes tools through the VM's envd service.
 *   - `in-memory`: tmpdir + `child_process.spawn` on the harness host with
 *     NO sandboxing. Test stub only — `main.ts` boot-fails if any other
 *     backend is selected without the right preconditions, but selecting
 *     this one is always honored (callers are expected to know what they're
 *     doing).
 */
export type SandboxRuntimeKind = 'local' | 'e2b' | 'in-memory' | 'opensandbox' | 'agentenv';

export type TranscriptStoreBackend = 'kafka' | 'postgres' | 'pulsar';
export type LlmEgress = 'direct' | 'gateway';
export type KafkaConnectionMode =
  | 'plaintext'
  | 'sasl-plain-token-tls'
  | 'sasl-plain-tls'
  | 'custom';

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
  kafkaBrokers: string[];
  clientId: string;
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
  consumerGroupId: string;
  transcriptStoreBackend: TranscriptStoreBackend;
  transcriptStoreDatabaseUrl: string;
  fileStoreDatabaseUrl: string;
  postgresEventPollIntervalMs: number;
  postgresEventLeaseMs: number;
  /**
   * node-postgres connection ceilings for the transcript + file-store pools, from
   * `TRANSCRIPT_STORE_POOL_MAX` and `FILESTORE_POOL_MAX` (10 each by default).
   * Env-tunable so several deployments can share one Postgres without exhausting
   * its `max_connections`.
   */
  transcriptStorePoolMax: number;
  fileStorePoolMax: number;
  pulsarServiceUrl: string;
  pulsarAuth?: PulsarAuthConfig;
  pulsarTenant: string;
  pulsarNamespace: string;
  pulsarTopicPrefix: string;
  pulsarReceiveTimeoutMs: number;
  pulsarAckTimeoutMs: number | undefined;
  piProviderCredentials?: PiProviderCredentials;
  openaiApiKey: string;
  openaiBaseURL: string | undefined;
  anthropicApiKey: string;
  anthropicBaseURL: string | undefined;
  anthropicModelDefault: string;
  /** Mesh-internal registry base URL used for every Harness control-plane call. */
  registryInternalBaseUrl: string;
  /** Static internal token for non-Kubernetes deployments. */
  internalServiceToken: string | undefined;
  /** Static token file or projected ServiceAccount JWT, reread for every request. */
  internalServiceTokenFile: string | undefined;
  /** Public ai-gateway base URL used to build the MCP endpoint for rewritten MCP servers. */
  gatewayMcpUrl: string;
  /** Public ai-gateway LLM base URL the in-sandbox harness routes the model through. */
  gatewayLlmUrl: string;
  /** Default model egress for separate Sessions without an explicit metadata override. */
  llmEgressDefault: LlmEgress;

  /**
   * Which sandbox backend `main.ts` should construct. Parsed from the
   * `SANDBOX_RUNTIME` env var; loadConfig throws if the var is missing or
   * holds a value outside the {@link SandboxRuntimeKind} union. No silent
   * fallback — explicit selection makes mis-deployments fail loud at boot,
   * not at first session-spawn.
   */
  sandboxRuntime: SandboxRuntimeKind;

  // ---- Workspace-isolated S3 / FUSE / output capture envs (all optional) ----
  /**
   * Bucket holding workspace files, memory, and execution outputs. When unset,
   * the harness mints no per-session S3 credentials, so memory stores cannot use
   * `memory_fuse` and separate-mode sessions skip output mounting and indexing.
   */
  s3Bucket: string | undefined;
  /**
   * Canonical deployment root. Every component appends
   * `workspaces/{workspaceId}/...`; empty string is valid, but the default
   * keeps managed-agent objects below `managed-agents/`.
   */
  s3KeyPrefix: string;
  /** S3 data-plane endpoint used by the S3 SDK + s3fs's `-o url=`. */
  s3Endpoint: string | undefined;
  /** Use path-style bucket addressing in the S3 SDK and sandbox s3fs mounts. */
  s3ForcePathStyle: boolean;
  /** Optional STS endpoint override. When unset, the AWS SDK selects its default endpoint. */
  s3StsEndpoint: string | undefined;
  /** Region passed to STS / S3; defaults via SDK when undefined. */
  s3Region: string | undefined;
  /**
   * IAM role ARN to AssumeRole for per-session creds. When unset, the minter
   * falls back to the harness's static `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY`
   * (DEV ONLY — see `SessionCredsMinter` for details). Production MUST set this.
   */
  s3StsRoleArn: string | undefined;
  /**
   * Explicit local-dev escape hatch that permits static S3 keys to be copied
   * into sandbox FUSE credentials. It is rejected in production even when set.
   */
  allowInsecureStaticS3Creds: boolean;
  /** Static access-key used by trusted host-side S3 and STS clients when configured. */
  s3AccessKeyId: string | undefined;
  s3SecretAccessKey: string | undefined;
  /**
   * Custom E2B template id (printed by `e2b template create`). When set,
   * `E2BSandboxRuntime` uses it; otherwise the SDK default is used. The custom
   * template bakes in `s3fs-fuse` + `fuse3` so FUSE mounts (memory stores +
   * output capture) work.
   */
  e2bTemplateId: string | undefined;

  // ---- OpenSandbox runtime envs ----
  /** OpenSandbox server host[:port] or full URL. */
  openSandboxDomain: string | undefined;
  /** Protocol used when OPEN_SANDBOX_DOMAIN has no scheme. */
  openSandboxProtocol: 'http' | 'https';
  /** Optional OpenSandbox API key, forwarded via OPEN-SANDBOX-API-KEY. */
  openSandboxApiKey: string | undefined;
  /** Sandbox image used by OpenSandboxRuntime.acquire(). */
  openSandboxImage: string | undefined;
  /**
   * Runtime override for `HARNESS_CATALOG.claude_code.defaultImage`, wired
   * from the chart's `images.sandboxHarness.claudeCode` values. Unset falls
   * back to the `@orca/harness-catalog` build-time default. Codex has no
   * override yet — its build is excluded from the release matrix until the
   * provider ships.
   */
  sandboxHarnessClaudeCodeImage: string | undefined;
  /** Optional comma-separated entrypoint command for the sandbox. */
  openSandboxEntrypoint: string[] | undefined;
  /** Sandbox lifetime in seconds. */
  openSandboxTimeoutSeconds: number;
  /** Route execd traffic through opensandbox-server instead of direct pod IPs. */
  openSandboxUseServerProxy: boolean;
  /** OpenSandbox lifecycle/execd request timeout in seconds. */
  openSandboxRequestTimeoutSeconds: number;
  /** Optional OpenSandbox workload CPU resource value (for example, "500m"). */
  openSandboxResourceCpu: string | undefined;
  /** Optional OpenSandbox workload memory resource value (for example, "1Gi"). */
  openSandboxResourceMemory: string | undefined;

  // ---- AgentENV runtime envs ----
  /** AgentENV gateway URL. */
  agentEnvBaseUrl: string | undefined;
  /** AgentENV API key used only by the trusted harness process. */
  agentEnvApiKey: string | undefined;
  /** Default OCI image used for AgentENV cold sandboxes. */
  agentEnvImage: string | undefined;
  /** Sandbox lifetime and resume TTL in seconds. */
  agentEnvTimeoutSeconds: number;
  /** Lifecycle, envd command, and file-transfer request timeout in seconds. */
  agentEnvRequestTimeoutSeconds: number;
  /** Optional Firecracker vCPU count. */
  agentEnvCpuCount: number | undefined;
  /** Optional Firecracker memory size in MiB. */
  agentEnvMemoryMB: number | undefined;
  /** Optional OverlayBD virtual disk size in MiB. */
  agentEnvDiskSizeMB: number | undefined;

  // ---- github_repository envs ----
  /**
   * Base directory under which `WorkDirManager` lays out per-session ephemeral
   * git work dirs. Defaults to `/var/tmp/orca-harness` in production; tests
   * pass a fresh tmpdir. Matches `WorkDirManagerOptions.baseDir`.
   */
  harnessWorkDir: string;
  /**
   * Public-internet URL of the registry's `POST /v1/git-creds` route. Wired
   * into the sandbox via `/etc/profile.d/orca-git-creds.sh` so the in-sandbox
   * `orca-git-creds` credential helper knows where to call. Required when a
   * session has at least one `github_repository` resource — the dispatcher
   * fails fast at session-spawn if this is unset, but config-load stays
   * permissive so chat-only deployments don't need it.
   */
  gitCredsPublicUrl: string | undefined;

  /**
   * Poll interval (ms) for Kafka's explicit canonical-topic discovery. Kafka
   * backends require a positive value; an unset deployment uses 30 seconds.
   * Local development can override it with a shorter value.
   */
  kafkaTopicRediscoverIntervalMs: number;
  /**
   * How long a per-session runner stays warm after a completed turn before
   * the dispatcher marks the session idle and releases its sandbox.
   */
  sessionIdleTimeoutMs: number;
}

/** Parsed/validated value of the `SANDBOX_RUNTIME` env var. */
const SANDBOX_RUNTIMES: ReadonlyArray<SandboxRuntimeKind> = [
  'local',
  'e2b',
  'in-memory',
  'opensandbox',
  'agentenv',
];
const TRANSCRIPT_STORE_BACKENDS: ReadonlyArray<TranscriptStoreBackend> = [
  'kafka',
  'postgres',
  'pulsar',
];

function parseSandboxRuntime(raw: string | undefined): SandboxRuntimeKind {
  if (raw === undefined || raw === '') {
    throw new Error(
      `SANDBOX_RUNTIME is required (no silent default). Set it to one of: ${SANDBOX_RUNTIMES.join(', ')}`,
    );
  }
  const lower = raw.toLowerCase();
  if (!SANDBOX_RUNTIMES.includes(lower as SandboxRuntimeKind)) {
    throw new Error(
      `SANDBOX_RUNTIME=${raw} is not recognized; expected one of: ${SANDBOX_RUNTIMES.join(', ')}`,
    );
  }
  return lower as SandboxRuntimeKind;
}

function parseTranscriptStoreBackend(raw: string | undefined): TranscriptStoreBackend {
  const lower = (raw ?? 'kafka').toLowerCase();
  if (!TRANSCRIPT_STORE_BACKENDS.includes(lower as TranscriptStoreBackend)) {
    throw new Error(
      `TRANSCRIPT_STORE_BACKEND=${raw} is not recognized; expected one of: ${TRANSCRIPT_STORE_BACKENDS.join(', ')}`,
    );
  }
  return lower as TranscriptStoreBackend;
}

function parseLlmEgressDefault(raw: string | undefined): LlmEgress {
  const value = raw ?? 'direct';
  if (value === 'direct' || value === 'gateway') return value;
  throw new Error(
    `LLM_EGRESS_DEFAULT=${value} is not recognized; expected one of: direct, gateway`,
  );
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

function parsePoolMax(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = optionalEnv(env, name);
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name}=${raw} is not a positive integer`);
  }
  return value;
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

  const clientId = env['KAFKA_CLIENT_ID'] ?? 'harness-server';
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

export function loadConfig(env: NodeJS.ProcessEnv = process.env): ServiceConfig {
  const s3KeyPrefix = normalizeS3KeyPrefix(env['S3_KEY_PREFIX']);
  const sandboxRuntime = parseSandboxRuntime(env['SANDBOX_RUNTIME']);
  const transcriptStoreBackend = parseTranscriptStoreBackend(env['TRANSCRIPT_STORE_BACKEND']);
  const kafka = parseKafkaConfig(env);
  const kafkaTopicPrefix = parseKafkaTopicPrefix(env);
  const kafkaTopicRediscoverIntervalMs =
    transcriptStoreBackend === 'kafka'
      ? parseKafkaTopicRediscoverInterval(env['KAFKA_TOPIC_REDISCOVER_INTERVAL_MS'])
      : 0;
  const allowInsecureStaticS3Creds = parseBoolean(env['ALLOW_INSECURE_STATIC_S3_CREDS'], false);
  const nodeEnv = env['NODE_ENV'] ?? 'production';
  if (allowInsecureStaticS3Creds && nodeEnv !== 'development' && nodeEnv !== 'test') {
    throw new Error(
      'ALLOW_INSECURE_STATIC_S3_CREDS=true is permitted only when NODE_ENV is development or test',
    );
  }
  const s3StaticCredentials = resolveS3StaticCredentials(env);

  return {
    httpPort: Number(env['HTTP_PORT'] ?? 9094),
    kafkaBrokers: kafka.brokers,
    clientId: kafka.clientId,
    kafka,
    kafkaTopicPrefix,
    consumerGroupId: env['HARNESS_CONSUMER_GROUP'] ?? 'harness-server',
    transcriptStoreBackend,
    kafkaTranscript: parseKafkaTranscriptConfig(env, transcriptStoreBackend),
    transcriptStoreDatabaseUrl:
      env['TRANSCRIPT_STORE_DATABASE_URL'] ??
      env['DATABASE_URL'] ??
      'postgres://orca:orca@localhost:5432/transcriptstore',
    fileStoreDatabaseUrl:
      env['FILESTORE_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/filestore',
    postgresEventPollIntervalMs: Number(env['POSTGRES_EVENT_POLL_INTERVAL_MS'] ?? 500),
    postgresEventLeaseMs: Number(env['POSTGRES_EVENT_LEASE_MS'] ?? 30000),
    transcriptStorePoolMax: parsePoolMax(env, 'TRANSCRIPT_STORE_POOL_MAX', 10),
    fileStorePoolMax: parsePoolMax(env, 'FILESTORE_POOL_MAX', 10),
    pulsarServiceUrl: env['PULSAR_SERVICE_URL'] ?? 'pulsar://localhost:6650',
    pulsarAuth: parsePulsarAuth(env),
    pulsarTenant: env['PULSAR_TENANT'] ?? 'public',
    pulsarNamespace: env['PULSAR_NAMESPACE'] ?? 'default',
    pulsarTopicPrefix: env['PULSAR_TOPIC_PREFIX'] ?? 'orca',
    pulsarReceiveTimeoutMs: Number(env['PULSAR_RECEIVE_TIMEOUT_MS'] ?? 500),
    pulsarAckTimeoutMs:
      env['PULSAR_ACK_TIMEOUT_MS'] === undefined || env['PULSAR_ACK_TIMEOUT_MS'] === ''
        ? undefined
        : Number(env['PULSAR_ACK_TIMEOUT_MS']),
    openaiApiKey: env['OPENAI_API_KEY'] ?? '',
    openaiBaseURL: optionalEnv(env, 'OPENAI_BASE_URL'),
    piProviderCredentials: readPiProviderCredentials(env),
    anthropicApiKey: env['ANTHROPIC_API_KEY'] ?? '',
    anthropicBaseURL: env['ANTHROPIC_BASE_URL'],
    anthropicModelDefault: env['ANTHROPIC_MODEL_DEFAULT'] ?? 'claude-sonnet-4-5-20250929',
    registryInternalBaseUrl: env['REGISTRY_INTERNAL_BASE_URL'] ?? 'http://localhost:8081',
    internalServiceToken: optionalEnv(env, 'INTERNAL_SERVICE_TOKEN'),
    internalServiceTokenFile: optionalEnv(env, 'INTERNAL_SERVICE_TOKEN_FILE'),
    gatewayMcpUrl: env['AI_GATEWAY_URL'] ?? 'http://localhost:8090',
    gatewayLlmUrl: env['LLM_GATEWAY_URL'] ?? 'http://localhost:8090/v1',
    llmEgressDefault: parseLlmEgressDefault(env['LLM_EGRESS_DEFAULT']),
    sandboxRuntime,
    s3Bucket: env['S3_BUCKET'],
    s3KeyPrefix,
    s3Endpoint: env['S3_ENDPOINT'],
    s3ForcePathStyle: parseBooleanEnv(env, 'S3_FORCE_PATH_STYLE', true),
    s3StsEndpoint: optionalEnv(env, 'S3_STS_ENDPOINT'),
    s3Region: env['S3_REGION'],
    s3StsRoleArn: env['S3_STS_ROLE_ARN'],
    allowInsecureStaticS3Creds,
    s3AccessKeyId: s3StaticCredentials?.accessKeyId,
    s3SecretAccessKey: s3StaticCredentials?.secretAccessKey,
    e2bTemplateId: env['E2B_TEMPLATE_ID'],
    openSandboxDomain: env['OPEN_SANDBOX_DOMAIN'],
    openSandboxProtocol: parseOpenSandboxProtocol(env['OPEN_SANDBOX_PROTOCOL']),
    openSandboxApiKey: env['OPEN_SANDBOX_API_KEY'],
    openSandboxImage: env['OPEN_SANDBOX_IMAGE'],
    sandboxHarnessClaudeCodeImage: optionalEnv(env, 'SANDBOX_HARNESS_CLAUDE_CODE_IMAGE'),
    openSandboxEntrypoint: parseCsv(env['OPEN_SANDBOX_ENTRYPOINT']),
    openSandboxTimeoutSeconds: parsePositiveInt(env['OPEN_SANDBOX_TIMEOUT_SECONDS'], 1800),
    openSandboxUseServerProxy: parseBoolean(env['OPEN_SANDBOX_USE_SERVER_PROXY'], true),
    openSandboxRequestTimeoutSeconds: parsePositiveInt(
      env['OPEN_SANDBOX_REQUEST_TIMEOUT_SECONDS'],
      30,
    ),
    openSandboxResourceCpu: env['OPEN_SANDBOX_RESOURCE_CPU'],
    openSandboxResourceMemory: env['OPEN_SANDBOX_RESOURCE_MEMORY'],
    agentEnvBaseUrl: optionalEnv(env, 'AGENTENV_BASE_URL'),
    agentEnvApiKey: optionalEnv(env, 'AGENTENV_API_KEY'),
    agentEnvImage: optionalEnv(env, 'AGENTENV_IMAGE'),
    agentEnvTimeoutSeconds: parsePositiveInt(env['AGENTENV_TIMEOUT_SECONDS'], 1800),
    agentEnvRequestTimeoutSeconds: parsePositiveInt(env['AGENTENV_REQUEST_TIMEOUT_SECONDS'], 180),
    agentEnvCpuCount: parseOptionalPositiveInt(env['AGENTENV_CPU_COUNT']),
    agentEnvMemoryMB: parseOptionalPositiveInt(env['AGENTENV_MEMORY_MB']),
    agentEnvDiskSizeMB: parseOptionalPositiveInt(env['AGENTENV_DISK_SIZE_MB']),
    harnessWorkDir: env['HARNESS_WORK_DIR'] ?? '/var/tmp/orca-harness',
    gitCredsPublicUrl: env['GIT_CREDS_PUBLIC_URL'],
    kafkaTopicRediscoverIntervalMs,
    sessionIdleTimeoutMs: parseSessionIdleTimeout(env['SESSION_IDLE_TIMEOUT_MS']),
  };
}

function parseOpenSandboxProtocol(raw: string | undefined): 'http' | 'https' {
  if (raw === undefined || raw === '') return 'http';
  const lower = raw.toLowerCase();
  if (lower !== 'http' && lower !== 'https') {
    throw new Error(`OPEN_SANDBOX_PROTOCOL=${raw} is not recognized; expected http or https`);
  }
  return lower;
}

function parseBoolean(raw: string | undefined, fallback: boolean): boolean {
  if (raw === undefined || raw === '') return fallback;
  const lower = raw.toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(lower)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(lower)) return false;
  throw new Error(`invalid boolean value: ${raw}`);
}

function parsePositiveInt(raw: string | undefined, fallback: number): number {
  if (raw === undefined || raw === '') return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`invalid positive integer value: ${raw}`);
  }
  return parsed;
}

function parseOptionalPositiveInt(raw: string | undefined): number | undefined {
  if (raw === undefined || raw === '') return undefined;
  return parsePositiveInt(raw, 1);
}

function parseCsv(raw: string | undefined): string[] | undefined {
  if (raw === undefined || raw.trim() === '') return undefined;
  const values = raw
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  return values.length > 0 ? values : undefined;
}

function parseKafkaTopicRediscoverInterval(raw: string | undefined): number {
  if (raw === undefined || raw === '') return 30_000;
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(
      `KAFKA_TOPIC_REDISCOVER_INTERVAL_MS=${raw} must be a positive integer for TRANSCRIPT_STORE_BACKEND=kafka`,
    );
  }
  return n;
}

function parseSessionIdleTimeout(raw: string | undefined): number {
  if (raw === undefined || raw === '') return 60_000;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) {
    console.warn(`SESSION_IDLE_TIMEOUT_MS=${raw} is invalid; defaulting to 60000`);
    return 60_000;
  }
  return Math.floor(n);
}
