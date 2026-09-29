// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { Pool } from 'pg';
import { S3Client } from '@aws-sdk/client-s3';
import { Kafka } from 'kafkajs';
import { SandboxManager } from '@anthropic-ai/sandbox-runtime';
import {
  PostgresSessionEventSource,
  PostgresTranscriptStore,
  PulsarSessionEventSource,
  PulsarTranscriptStore,
  applyPostgresTranscriptMigrations,
  type KafkaTranscriptCodec,
  type TranscriptStore,
} from '@orca/transcript-store';
import { LocalFileStore, S3BlobStore, applyMigrations } from '@orca/file-store';
import { S3SkillStore } from '@orca/skill-store';
import { buildHealthzServer } from './healthz.js';
import { loadConfig, type ServiceConfig } from './config.js';
import { buildKafkaTranscriptBackend } from './kafka-transcript-bootstrap.js';
import { Dispatcher, type DispatcherOptions } from './runner/dispatcher.js';
import { RegistryClient } from './clients/registry.js';
import { InMemorySandboxRuntime } from './sandbox/in-memory/runtime.js';
import { E2BSandboxRuntime, type E2BSandboxRuntimeOptions } from './sandbox/e2b/runtime.js';
import { LocalSandboxRuntime } from './sandbox/local/runtime.js';
import {
  OpenSandboxRuntime,
  type OpenSandboxRuntimeOptions,
} from './sandbox/opensandbox/runtime.js';
import { AgentEnvRuntime, type AgentEnvRuntimeOptions } from './sandbox/agentenv/runtime.js';
import type { SandboxRuntime } from './sandbox/sandbox-runtime.js';
import { SessionCredsMinter, type SessionCredsConfig } from './auth/sts-creds.js';
import { buildInternalServiceTokenProvider } from './auth/internal-service-token.js';
import { makeGitWorker } from './git/git-worker.js';
import { WorkDirManager } from './git/work-dir.js';
import { createControlledShutdown, monitorEventSourceFailure } from './controlled-shutdown.js';

const config = loadConfig();
let dispatcherForReadiness: Dispatcher | null = null;
const app = buildHealthzServer(
  () =>
    dispatcherForReadiness?.readiness() ?? { ready: false, reasons: ['dispatcher_not_started'] },
);
const transcript = await buildTranscriptBackend(config);
const store = transcript.store;
const kafka = transcript.kafka;
const eventSource = transcript.eventSource;
const closeTranscriptBackend = transcript.close;

async function buildTranscriptBackend(cfg: ServiceConfig): Promise<{
  store: TranscriptStore;
  kafka?: Kafka;
  codec?: KafkaTranscriptCodec;
  eventSource?: PostgresSessionEventSource | PulsarSessionEventSource;
  close?: () => Promise<void>;
}> {
  switch (cfg.transcriptStoreBackend) {
    case 'kafka': {
      const kafkaClient = new Kafka(cfg.kafka);
      return buildKafkaTranscriptBackend(kafkaClient, cfg.kafkaTopicPrefix, cfg.kafkaTranscript);
    }
    case 'postgres': {
      const pool = new Pool({
        connectionString: cfg.transcriptStoreDatabaseUrl,
        max: cfg.transcriptStorePoolMax,
      });
      await applyPostgresTranscriptMigrations(pool);
      return {
        store: new PostgresTranscriptStore({ pool, closePool: false }),
        eventSource: new PostgresSessionEventSource({
          pool,
          groupId: cfg.consumerGroupId,
          pollIntervalMs: cfg.postgresEventPollIntervalMs,
          leaseMs: cfg.postgresEventLeaseMs,
        }),
        close: async () => {
          await pool.end();
        },
      };
    }
    case 'pulsar': {
      const eventSourceOptions: ConstructorParameters<typeof PulsarSessionEventSource>[0] = {
        serviceUrl: cfg.pulsarServiceUrl,
        tenant: cfg.pulsarTenant,
        namespace: cfg.pulsarNamespace,
        topicPrefix: cfg.pulsarTopicPrefix,
        subscription: cfg.consumerGroupId,
        receiveTimeoutMs: cfg.pulsarReceiveTimeoutMs,
      };
      if (cfg.pulsarAuth !== undefined) eventSourceOptions.auth = cfg.pulsarAuth;
      if (cfg.pulsarAckTimeoutMs !== undefined) {
        eventSourceOptions.ackTimeoutMs = cfg.pulsarAckTimeoutMs;
      }
      const storeOptions: ConstructorParameters<typeof PulsarTranscriptStore>[0] = {
        serviceUrl: cfg.pulsarServiceUrl,
        tenant: cfg.pulsarTenant,
        namespace: cfg.pulsarNamespace,
        topicPrefix: cfg.pulsarTopicPrefix,
      };
      if (cfg.pulsarAuth !== undefined) storeOptions.auth = cfg.pulsarAuth;
      return {
        store: new PulsarTranscriptStore(storeOptions),
        eventSource: new PulsarSessionEventSource(eventSourceOptions),
      };
    }
  }
}

const registry = new RegistryClient(
  config.registryInternalBaseUrl,
  buildInternalServiceTokenProvider({
    ...(config.internalServiceToken ? { token: config.internalServiceToken } : {}),
    ...(config.internalServiceTokenFile ? { tokenFile: config.internalServiceTokenFile } : {}),
  }),
);

// File-store wiring. The pool is owned by harness-server and
// closed in shutdown(). `applyMigrations` is idempotent and a no-op once
// the schema is up-to-date — running it on every boot keeps dev clusters in
// sync with shipped code.
const fileStorePool = new Pool({
  connectionString: config.fileStoreDatabaseUrl,
  max: config.fileStorePoolMax,
});
await applyMigrations(fileStorePool);

// One S3Client used for BOTH the file-store's S3BlobStore (uploads/blobs read)
// AND the dispatcher's OutputIndexer (ListObjectsV2 + GetObject over the
// current runner generation's output prefix). Re-using the same client keeps the credential and
// endpoint config in one place.
const s3Config: ConstructorParameters<typeof S3Client>[0] = {
  endpoint: config.s3Endpoint ?? 'http://localhost:9000',
  region: config.s3Region ?? 'us-east-1',
  forcePathStyle: config.s3ForcePathStyle,
};
// No credentials field means AWS SDK default chain (including IRSA).
if (config.s3AccessKeyId && config.s3SecretAccessKey) {
  s3Config.credentials = {
    accessKeyId: config.s3AccessKeyId,
    secretAccessKey: config.s3SecretAccessKey,
  };
}
const s3 = new S3Client(s3Config);

const fileStore = new LocalFileStore({
  pool: fileStorePool,
  blobStore: new S3BlobStore({
    client: s3,
    bucket: config.s3Bucket ?? 'orca-files',
    keyPrefix: config.s3KeyPrefix,
  }),
});
const skillStore = new S3SkillStore({
  client: s3,
  bucket: config.s3Bucket ?? 'orca-files',
  keyPrefix: config.s3KeyPrefix,
});

// Sandbox runtime selection (no silent fallbacks). `config.sandboxRuntime` was parsed from the
// `SANDBOX_RUNTIME` env var; loadConfig already threw on missing/unknown.
// Per-runtime preconditions are re-checked here so a misconfigured deploy
// fails at boot, not at the first session-spawn.
const sandboxRuntime: SandboxRuntime = buildSandboxRuntime(config);
app.log.info(`harness-server sandbox runtime: ${config.sandboxRuntime}`);

function buildSandboxRuntime(cfg: ServiceConfig): SandboxRuntime {
  switch (cfg.sandboxRuntime) {
    case 'local':
      return buildLocalSandboxRuntime(cfg);
    case 'e2b':
      return buildE2BSandboxRuntime(cfg);
    case 'opensandbox':
      return buildOpenSandboxRuntime(cfg);
    case 'agentenv':
      return buildAgentEnvRuntime(cfg);
    case 'in-memory':
      return new InMemorySandboxRuntime();
  }
}

function buildE2BSandboxRuntime(cfg: ServiceConfig): E2BSandboxRuntime {
  const e2bApiKey = process.env['E2B_API_KEY'];
  if (!e2bApiKey || e2bApiKey.length === 0) {
    throw new Error('SANDBOX_RUNTIME=e2b requires E2B_API_KEY to be set');
  }
  const e2bOpts: E2BSandboxRuntimeOptions = { apiKey: e2bApiKey };
  if (cfg.e2bTemplateId !== undefined) e2bOpts.templateId = cfg.e2bTemplateId;
  return new E2BSandboxRuntime(e2bOpts);
}

function buildOpenSandboxRuntime(cfg: ServiceConfig): OpenSandboxRuntime {
  if (!cfg.openSandboxDomain || cfg.openSandboxDomain.length === 0) {
    throw new Error('SANDBOX_RUNTIME=opensandbox requires OPEN_SANDBOX_DOMAIN to be set');
  }
  if (!cfg.openSandboxImage || cfg.openSandboxImage.length === 0) {
    throw new Error('SANDBOX_RUNTIME=opensandbox requires OPEN_SANDBOX_IMAGE to be set');
  }
  const opts: OpenSandboxRuntimeOptions = {
    domain: cfg.openSandboxDomain,
    protocol: cfg.openSandboxProtocol,
    image: cfg.openSandboxImage,
    timeoutSeconds: cfg.openSandboxTimeoutSeconds,
    useServerProxy: cfg.openSandboxUseServerProxy,
    requestTimeoutSeconds: cfg.openSandboxRequestTimeoutSeconds,
  };
  if (cfg.openSandboxResourceCpu !== undefined || cfg.openSandboxResourceMemory !== undefined) {
    opts.resourceLimits = {
      cpu: cfg.openSandboxResourceCpu ?? '1',
      memory: cfg.openSandboxResourceMemory ?? '2Gi',
    };
  }
  if (cfg.openSandboxApiKey !== undefined && cfg.openSandboxApiKey.length > 0) {
    opts.apiKey = cfg.openSandboxApiKey;
  }
  if (cfg.openSandboxEntrypoint !== undefined) {
    opts.entrypoint = cfg.openSandboxEntrypoint;
  }
  return new OpenSandboxRuntime(opts);
}

function buildAgentEnvRuntime(cfg: ServiceConfig): AgentEnvRuntime {
  if (!cfg.agentEnvBaseUrl) {
    throw new Error('SANDBOX_RUNTIME=agentenv requires AGENTENV_BASE_URL to be set');
  }
  if (!cfg.agentEnvApiKey) {
    throw new Error('SANDBOX_RUNTIME=agentenv requires AGENTENV_API_KEY to be set');
  }
  if (!cfg.agentEnvImage) {
    throw new Error('SANDBOX_RUNTIME=agentenv requires AGENTENV_IMAGE to be set');
  }
  const opts: AgentEnvRuntimeOptions = {
    baseUrl: cfg.agentEnvBaseUrl,
    apiKey: cfg.agentEnvApiKey,
    image: cfg.agentEnvImage,
    timeoutSeconds: cfg.agentEnvTimeoutSeconds,
    requestTimeoutSeconds: cfg.agentEnvRequestTimeoutSeconds,
  };
  if (cfg.agentEnvCpuCount !== undefined) opts.cpuCount = cfg.agentEnvCpuCount;
  if (cfg.agentEnvMemoryMB !== undefined) opts.memoryMB = cfg.agentEnvMemoryMB;
  if (cfg.agentEnvDiskSizeMB !== undefined) opts.diskSizeMB = cfg.agentEnvDiskSizeMB;
  return new AgentEnvRuntime(opts);
}

function buildLocalSandboxRuntime(cfg: ServiceConfig): LocalSandboxRuntime {
  // Boot-fail when the `srt` binary isn't reachable. The
  // `@anthropic-ai/sandbox-runtime` package ships the binary in
  // `node_modules/.bin/srt`; production deploys are expected to install it
  // globally (`npm install -g @anthropic-ai/sandbox-runtime`) so it lands on
  // PATH. We probe with `--version` (cheap, 50ms) instead of just `command -v`
  // so a stale shim with a corrupted binary is also caught.
  try {
    execFileSync('srt', ['--version'], { stdio: 'pipe' });
  } catch (e) {
    throw new Error(
      `SANDBOX_RUNTIME=local requires the 'srt' binary on PATH (npm install -g @anthropic-ai/sandbox-runtime). Probe failed: ${(e as Error).message}`,
    );
  }
  const allowedNetworkHosts = collectLocalSandboxAllowedHosts(cfg);
  return new LocalSandboxRuntime({
    harnessWorkDir: cfg.harnessWorkDir,
    allowedNetworkHosts,
    manager: SandboxManager,
  });
}

/**
 * Build the network allow-list for the local sandbox: `api.anthropic.com`
 * (LLM), the host portion of `AI_GATEWAY_URL`, and the host portion of
 * `S3_ENDPOINT`. There are no wildcards: each origin is listed explicitly.
 * Hosts that fail to parse out of the URLs are dropped with a console.warn so
 * the boot path is forgiving on placeholder values during local dev.
 */
function collectLocalSandboxAllowedHosts(cfg: ServiceConfig): string[] {
  const hosts = new Set<string>(['api.anthropic.com']);
  const candidates: Array<{ name: string; url: string | undefined }> = [
    { name: 'AI_GATEWAY_URL', url: cfg.gatewayMcpUrl },
    { name: 'S3_ENDPOINT', url: cfg.s3Endpoint },
  ];
  for (const c of candidates) {
    if (!c.url) continue;
    try {
      const parsed = new URL(c.url);
      if (parsed.hostname) hosts.add(parsed.hostname);
    } catch {
      console.warn(`buildLocalSandboxRuntime: ${c.name}=${c.url} is not a valid URL; ignoring`);
    }
  }
  return [...hosts];
}

// Per-session creds minter. Constructed only when the
// operator wired up an S3 bucket and either an STS role ARN (production) or a
// pair of static IAM keys (dev fallback). When neither path is configured we
// log a warning and skip the minter. File resources never use these
// credentials: they are always materialized through tarball prefetch.
let credsMinter: SessionCredsMinter | undefined;
if (config.s3Bucket) {
  if (config.s3StsRoleArn) {
    const credsCfg: SessionCredsConfig = {
      bucket: config.s3Bucket,
      outputsRoot: config.s3KeyPrefix,
      memoryRoot: config.s3KeyPrefix,
      stsRoleArn: config.s3StsRoleArn,
    };
    if (config.s3Region !== undefined) credsCfg.region = config.s3Region;
    if (config.s3StsEndpoint !== undefined) credsCfg.stsEndpoint = config.s3StsEndpoint;
    if (config.s3AccessKeyId && config.s3SecretAccessKey) {
      credsCfg.staticAccessKey = config.s3AccessKeyId;
      credsCfg.staticSecretKey = config.s3SecretAccessKey;
    }
    credsMinter = new SessionCredsMinter(credsCfg);
    app.log.info('SessionCredsMinter: STS AssumeRole mode');
  } else if (
    config.allowInsecureStaticS3Creds &&
    config.s3AccessKeyId &&
    config.s3SecretAccessKey
  ) {
    const credsCfg: SessionCredsConfig = {
      bucket: config.s3Bucket,
      outputsRoot: config.s3KeyPrefix,
      memoryRoot: config.s3KeyPrefix,
      staticAccessKey: config.s3AccessKeyId,
      staticSecretKey: config.s3SecretAccessKey,
    };
    if (config.s3Region !== undefined) credsCfg.region = config.s3Region;
    credsMinter = new SessionCredsMinter(credsCfg);
    app.log.info('SessionCredsMinter: dev-fallback static-keys mode');
  } else if (config.s3AccessKeyId && config.s3SecretAccessKey) {
    throw new Error(
      'S3_BUCKET with static keys cannot mint sandbox credentials; configure S3_STS_ROLE_ARN or explicitly set ALLOW_INSECURE_STATIC_S3_CREDS=true in development/test',
    );
  } else {
    app.log.warn(
      'S3_BUCKET set but no S3_STS_ROLE_ARN or static keys — sandbox FUSE mounts will be disabled',
    );
  }
}

// Git wiring. The work-dir manager owns the per-session ephemeral
// host-side dir; `makeGitWorker()` wraps simple-git for the dispatcher's
// github_repository branch. Both are constructed unconditionally — the cost
// is two object allocations + a `mkdir -p baseDir` at first acquire — so the
// config knob (`HARNESS_WORK_DIR`) is the only thing operators need to flip.
// `gitCredsPublicUrl` is propagated as-is; absent means the dispatcher will
// fail fast on the first session with a github_repository resource (chat-only
// deployments don't notice).
const workDir = new WorkDirManager({ baseDir: config.harnessWorkDir });
const gitWorker = makeGitWorker();

const dispatcherOpts: DispatcherOptions = {
  groupId: config.consumerGroupId,
  store,
  ...(config.piProviderCredentials ? { piProviderCredentials: config.piProviderCredentials } : {}),
  openaiApiKey: config.openaiApiKey,
  ...(config.openaiBaseURL ? { openaiBaseURL: config.openaiBaseURL } : {}),
  anthropicApiKey: config.anthropicApiKey,
  modelDefault: config.anthropicModelDefault,
  registry,
  gatewayMcpUrl: config.gatewayMcpUrl,
  gatewayLlmUrl: config.gatewayLlmUrl,
  llmEgressDefault: config.llmEgressDefault,
  fileStore,
  skillStore,
  sandboxRuntime,
  s3KeyPrefix: config.s3KeyPrefix,
  s3ForcePathStyle: config.s3ForcePathStyle,
  s3Client: s3,
  gitWorker,
  workDir,
  sessionIdleTimeoutMs: config.sessionIdleTimeoutMs,
};
if (kafka !== undefined) {
  dispatcherOpts.kafka = kafka;
  dispatcherOpts.topicRediscoverIntervalMs = config.kafkaTopicRediscoverIntervalMs;
}
if (transcript.codec !== undefined) dispatcherOpts.codec = transcript.codec;
if (config.kafkaTopicPrefix !== '') dispatcherOpts.topicPrefix = config.kafkaTopicPrefix;
if (eventSource !== undefined) dispatcherOpts.eventSource = eventSource;
if (config.gitCredsPublicUrl !== undefined) {
  dispatcherOpts.gitCredsPublicUrl = config.gitCredsPublicUrl;
}
if (config.anthropicBaseURL !== undefined) {
  dispatcherOpts.anthropicBaseURL = config.anthropicBaseURL;
}
if (credsMinter !== undefined) dispatcherOpts.credsMinter = credsMinter;
if (config.s3Bucket !== undefined) dispatcherOpts.s3Bucket = config.s3Bucket;
if (config.s3Endpoint !== undefined) dispatcherOpts.s3Endpoint = config.s3Endpoint;
if (config.s3Region !== undefined) dispatcherOpts.s3Region = config.s3Region;
if (config.sandboxHarnessClaudeCodeImage !== undefined) {
  dispatcherOpts.defaultImageOverrides = {
    claude_code: config.sandboxHarnessClaudeCodeImage,
    codex_sdk: config.sandboxHarnessClaudeCodeImage,
    pi_sdk: config.sandboxHarnessClaudeCodeImage,
  };
}
const dispatcher = new Dispatcher(dispatcherOpts);
dispatcherForReadiness = dispatcher;

const controlledShutdown = createControlledShutdown({
  cleanup: async (): Promise<void> => {
    app.log.info('harness-server shutting down');
    try {
      await dispatcher.stop();
    } finally {
      try {
        await store.close();
      } finally {
        try {
          await closeTranscriptBackend?.();
        } finally {
          try {
            await skillStore.close();
          } finally {
            try {
              await fileStore.close();
            } finally {
              await app.close();
            }
          }
        }
      }
    }
  },
  exit: (code) => process.exit(code),
  onCleanupFailure: () => app.log.error('harness-server shutdown cleanup failed'),
});
const shutdown = (exitCode = 0): Promise<void> => controlledShutdown.shutdown(exitCode);

await dispatcher.start();
await app.listen({ host: '0.0.0.0', port: config.httpPort });
app.log.info(`harness-server healthz listening on :${config.httpPort}`);

if (eventSource) {
  monitorEventSourceFailure(eventSource, shutdown, () => {
    app.log.error('harness-server event source failed; shutting down');
  });
}
// Registering these listeners replaces Node's default silent-ish crash with a
// logged one; we still exit(1) to preserve fail-fast semantics. The goal is
// evidence in harness.log — a prior CI run (e2e-stack pulsar leg) died with
// zero log output and no way to diagnose the cause.
process.on('unhandledRejection', (reason) => {
  const stack = reason instanceof Error ? reason.stack : undefined;
  console.error('harness-server: unhandled rejection', stack ?? reason);
  process.exit(1);
});
process.on('uncaughtException', (err) => {
  console.error('harness-server: uncaught exception', err.stack ?? err);
  process.exit(1);
});
process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());
