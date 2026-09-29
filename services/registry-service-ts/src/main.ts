// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Pool } from 'pg';
import { Kafka } from 'kafkajs';
import { S3Client } from '@aws-sdk/client-s3';
import {
  PostgresSessionEventSource,
  PostgresTranscriptStore,
  PulsarSessionEventSource,
  PulsarTranscriptStore,
  applyPostgresTranscriptMigrations,
  type Event,
  type TranscriptStore,
} from '@orca/transcript-store';
import { LocalFileStore, S3BlobStore, applyMigrations } from '@orca/file-store';
import {
  LocalMemoryStore,
  PostgresMemoryMetadataStore,
  S3MemoryBlobStore,
  applyMigrations as applyMemoryMigrations,
} from '@orca/memory-store';
import { S3SkillStore } from '@orca/skill-store';
import { buildAdminApp, buildInternalApp, buildPublicApp, type BuildAppOptions } from './server.js';
import { loadConfig } from './config.js';
import { buildKafkaTranscriptBackend } from './kafka-transcript-bootstrap.js';
import { observePostgresPool } from './observability/api-performance.js';
import { buildRunnerTunnelAuth } from './auth/tunnel-auth.js';
import { RunnerExitReports } from './tunnel/runner-exit-reports.js';
import { buildDb } from './persistence/postgres/client.js';
import { EnvironmentClaimStore, startEnvironmentClaimReaper } from './domain/environment-claims.js';
import { EnvironmentTokenStore } from './domain/environment-token-store.js';
import { createPostgresModelPriceStore, loadSeedModelPrices } from './pricing/store.js';
import { createModelPriceRefresher } from './pricing/refresher.js';
import { SessionJwtMinter } from './auth/session-jwt.js';
import { buildConfiguredInternalAuthVerifier } from './auth/internal-auth.js';
import {
  DefaultSecretProvider,
  EnvSecretProvider,
  KubernetesSecretStore,
  LocalSecretStore,
  type SecretStore,
} from './secrets/index.js';
import { AgentSnapshotResolver } from './domain/agent-snapshot-resolver.js';
import { buildSnapshotRecordLoader } from './api/snapshot-loader.js';
import { WorkerRegistry } from './tunnel/worker-registry.js';
import { launcherFactory } from './environment/launcher/launcher-factory.js';
import { EnvironmentLaunchLifecycle } from './environment/launch/environment-launch-lifecycle.js';
import {
  backfillSessionEventProjections,
  indexTranscriptEvents,
} from './events/session-events-index.js';
import {
  GIT_CREDENTIAL_STAGING_RECONCILE_INTERVAL_MS,
  reconcileGitCredentialStagingIntents,
} from './domain/git-credentials.js';
import {
  reconcileSessionLifecycleOutbox,
  SESSION_LIFECYCLE_RECONCILE_INTERVAL_MS,
} from './domain/session-lifecycle-outbox.js';
import {
  reconcileSkillBundleDeletionOutbox,
  SKILL_BUNDLE_DELETION_RECONCILE_INTERVAL_MS,
} from './domain/skill-bundle-deletion-outbox.js';
import { startAgentObservabilityMaintenance } from './domain/agent-observability-maintenance.js';
import {
  dispatchPendingAgentTriggerFires,
  reconcileDueAgentTriggers,
} from './domain/trigger-reconciler.js';
import {
  triggerDispatcherTotal,
  triggerDueBacklog,
  triggerOldestDueAgeSeconds,
  triggerPlannerTotal,
} from './metrics.js';
import {
  buildSessionDispatchObservabilityReader,
  SessionDispatchObservabilityRefresher,
  SESSION_DISPATCH_OBSERVABILITY_REFRESH_INTERVAL_MS,
} from './observability/session-dispatch.js';

const config = loadConfig();
const { db, pool: metadataPool } = buildDb({
  url: config.databaseUrl,
  poolSize: config.databasePoolMax,
});
const stopPoolObservers = [observePostgresPool(metadataPool, 'metadata')];
const { store, eventSource, close: closeTranscriptBackend } = await buildTranscriptBackend();

interface SessionEventsIndexerSource {
  start(handler: (event: Event) => Promise<void>): Promise<void>;
  stop(): Promise<void>;
}

async function buildTranscriptBackend(): Promise<{
  store: TranscriptStore;
  eventSource: SessionEventsIndexerSource;
  close?: () => Promise<void>;
}> {
  switch (config.transcriptStoreBackend) {
    case 'kafka': {
      const kafka = new Kafka(config.kafka);
      return buildKafkaTranscriptBackend(kafka, config.kafkaTopicPrefix, config.kafkaTranscript);
    }
    case 'postgres': {
      const pool = new Pool({
        connectionString: config.transcriptStoreDatabaseUrl,
        max: config.transcriptStorePoolMax,
      });
      stopPoolObservers.push(observePostgresPool(pool, 'transcript'));
      await applyPostgresTranscriptMigrations(pool);
      return {
        store: new PostgresTranscriptStore({ pool }),
        eventSource: new PostgresSessionEventSource({
          pool,
          groupId: 'registry-session-events-index',
          includeAllEvents: true,
        }),
      };
    }
    case 'pulsar': {
      const pulsarOptions: ConstructorParameters<typeof PulsarTranscriptStore>[0] = {
        serviceUrl: config.pulsarServiceUrl,
        tenant: config.pulsarTenant,
        namespace: config.pulsarNamespace,
        topicPrefix: config.pulsarTopicPrefix,
      };
      if (config.pulsarAuth !== undefined) pulsarOptions.auth = config.pulsarAuth;
      return {
        store: new PulsarTranscriptStore(pulsarOptions),
        eventSource: new PulsarSessionEventSource({
          serviceUrl: config.pulsarServiceUrl,
          tenant: config.pulsarTenant,
          namespace: config.pulsarNamespace,
          topicPrefix: config.pulsarTopicPrefix,
          subscription: 'registry-session-events-index',
          includeAllEvents: true,
          ...(config.pulsarAuth !== undefined ? { auth: config.pulsarAuth } : {}),
        }),
      };
    }
  }
}

const fileStorePool = new Pool({
  connectionString: config.fileStoreDatabaseUrl,
  max: config.fileStorePoolMax,
});
stopPoolObservers.push(observePostgresPool(fileStorePool, 'file'));
await applyMigrations(fileStorePool);
const s3Config: ConstructorParameters<typeof S3Client>[0] = {
  endpoint: config.s3Endpoint,
  region: config.s3Region,
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
    bucket: config.s3Bucket,
    keyPrefix: config.s3KeyPrefix,
  }),
});
const skillStore = new S3SkillStore({
  client: s3,
  bucket: config.s3Bucket,
  keyPrefix: config.s3KeyPrefix,
});

// Memory store: separate Postgres database (so the file-store + memory-store
// migrations don't collide), shared S3 client + bucket. The blob layer is
// workspace-agnostic at construction time — the per-request workspaceId is
// threaded through every method call.
const memoryPool = new Pool({
  connectionString: config.memorystoreDatabaseUrl,
  max: config.memorystorePoolMax,
});
stopPoolObservers.push(observePostgresPool(memoryPool, 'memory'));
await applyMemoryMigrations(memoryPool);
const memoryStore = new LocalMemoryStore({
  blobStore: new S3MemoryBlobStore({
    client: s3,
    bucket: config.s3Bucket,
    keyPrefix: config.s3KeyPrefix,
  }),
  metadataStore: new PostgresMemoryMetadataStore(memoryPool),
});

const minter = new SessionJwtMinter({
  privateKeyPem: config.sessionJwtPrivateKeyPem,
  issuer: config.sessionJwtIssuer,
  audience: config.sessionJwtAudience,
  ttlSecs: config.sessionJwtTtlSecs,
});

// Credential-free agent-snapshot resolver. Wired UNCONDITIONALLY: the owner-pod
// bridge manager delivers the snapshot (model + provider + composed system + tool
// allowlists + egress config) to a self-hosted runner at session start over the
// tunnel, before recovery + the bridge, and that delivery is independent of which
// egress mode the session resolves to. The gateway URLs are an OPTIONAL
// pass-through: a session whose environment is `egress_mode=gateway` needs
// `AI_GATEWAY_MCP_URL` (the resolver throws a loud config error at resolve-time
// if it is missing), but a `egress_mode=sidecar` session resolves with NO gateway
// at all — a pure-sidecar deployment runs without `AI_GATEWAY_MCP_URL` and still
// gets full snapshot delivery (model/provider/system/egress). The scoped session
// JWT is minted in-process via the same `minter` the mint-jwt route uses (no HTTP
// round-trip).
const agentSnapshotProvider = new AgentSnapshotResolver({
  loader: buildSnapshotRecordLoader(db),
  minter,
  ...(config.sessionJwtLlmPolicy ? { llmPolicy: config.sessionJwtLlmPolicy } : {}),
  ...(config.aiGatewayMcpUrl ? { gatewayMcpUrl: config.aiGatewayMcpUrl } : {}),
  ...(config.aiGatewayLlmUrl ? { gatewayLlmUrl: config.aiGatewayLlmUrl } : {}),
  ...(config.aiGatewayLlmJwtTtlSecs !== undefined
    ? { llmJwtTtlSecs: config.aiGatewayLlmJwtTtlSecs }
    : {}),
});

// Read-only SecretProvider for env-backed secret refs, used by `/v1/git-creds`
// (PATs for the in-sandbox credential helper), the Git proxy, and the internal
// routes. Only the env backend is wired here; the delegated provider list is
// empty.
const secretProvider = new DefaultSecretProvider(
  new EnvSecretProvider((k) => process.env[k] ?? null),
  [],
);
const secretStore = await buildSecretStore();

// Public runner-tunnel auth posture. The tunnel is mounted off-mesh on
// `/v1/tunnels/runners/:runnerId` (a self-hosted runner dials it from outside the
// cluster), so the WS handler's in-handler gates ARE its only auth — there is no
// mesh layer in front of it. `buildRunnerTunnelAuth` turns config into the route
// options that make those gates fail closed by default: with no
// `RUNNER_TUNNEL_TOKENS` allow-list it wires a loopback-only auth provider so a
// LOCAL runner registers while every remote peer is refused by the owner
// fail-closed gate; provisioning `RUNNER_TUNNEL_TOKENS` is the explicit opt-in
// that admits allow-listed remote runners. Wiring an auth provider here is what
// keeps the route OUT of single-user mode, which is what makes the fail-closed
// gate reachable in production at all.
const runnerTunnelAuth = buildRunnerTunnelAuth(config);
// Worker-reported runner exit causes (`worker.runner_exited`). Wired on the READ side
// so `GET /internal/runners/:id/status` surfaces an (owner-scoped) failure cause
// for a runner that died before connecting, letting a waiting client fail fast
// instead of polling to a timeout. The WRITE side is the mesh-only worker
// tunnel (env-key + durable claims; see docs/managed-agents/services/registry-service.md)
// whose engine mount lands with the planned `SelfHostedTransport` wiring.
const runnerExitReports = new RunnerExitReports();

// Worker-tunnel registry, constructed HERE (rather than left for `buildApp` to
// build internally) so the environment-launch lifecycle's wait-online poll
// shares the SAME live-connection view the worker-tunnel route registers into.
const workerRegistry = new WorkerRegistry();

// Environment-launch lifecycle (target=cloud colocated sessions): optional —
// wired only when an environment-launcher BACKEND is configured. Local-only
// today (A2 adds cloud provider backends to `launcherFactory`); omitted, a
// `target=cloud` session still creates and dispatches PENDING (the distributor
// is un-gated unconditionally) but no box is ever provisioned, exactly like a
// self_hosted session with no attached worker. `ORCA_REGISTRY_TUNNEL_URL`
// defaults to this process's own loopback origin — correct for a Local-spawned
// worker (same machine); a real cloud backend deployment overrides it to the
// externally-routable worker-tunnel origin the launched sandbox can reach.
const environmentLauncherBackend = process.env['ORCA_ENVIRONMENT_LAUNCHER_BACKEND'];
const environmentLaunchLifecycle = environmentLauncherBackend
  ? new EnvironmentLaunchLifecycle({
      launcher: launcherFactory(environmentLauncherBackend),
      tokens: new EnvironmentTokenStore(db),
      workerOnline: workerRegistry,
      registryTunnelUrl:
        process.env['ORCA_REGISTRY_TUNNEL_URL'] ?? `http://localhost:${config.httpPort}`,
    })
  : undefined;

const backfilledProjectionRows = await backfillSessionEventProjections(db);
if (backfilledProjectionRows > 0) {
  console.log(
    `registry-service-ts backfilled ${backfilledProjectionRows} legacy event projections`,
  );
}

async function buildSecretStore(): Promise<SecretStore | undefined> {
  switch (config.secretStoreMode) {
    case 'none':
      return undefined;
    case 'local':
      return new LocalSecretStore();
    case 'kubernetes': {
      const store = KubernetesSecretStore.systemDefault({
        namespace: config.secretStoreKubernetesNamespace,
        secretName: config.secretStoreKubernetesSecretName,
      });
      await store.assertReady();
      return store;
    }
  }
}

const appOptions: BuildAppOptions = {
  db,
  batchReadsEnabled: config.batchReadsEnabled,
  apiKeyProofCacheEnabled: config.apiKeyProofCacheEnabled,
  heavyReadAdmission: config.heavyReadAdmission,
  oidc: {
    allowedIssuers: config.oidcAllowedIssuers,
    audience: config.oidcAudience,
    metadataClaims: config.oidcMetadataClaims,
    deniedJtiFile: config.oidcDeniedJtiFile,
    resolveWorkspaceByAudience: config.oidcResolveWorkspaceByAudience,
  },
  ...(config.trustedProxyCidrs.length > 0 ? { publicTrustProxy: config.trustedProxyCidrs } : {}),
  store,
  sse: {
    bufferSize: config.sseBufferSize,
    dropAgeMs: config.sseDropAgeMs,
    heartbeatMs: config.sseHeartbeatMs,
  },
  jwtMinter: minter,
  registryLocalBaseUrl: `http://127.0.0.1:${config.httpPort}`,
  registryBaseUrl: (
    process.env['ORCA_REGISTRY_TUNNEL_URL'] ?? `http://localhost:${config.httpPort}`
  )
    .replace(/^ws:/, 'http:')
    .replace(/^wss:/, 'https:'),
  ...(config.sessionJwtLlmPolicy ? { sessionJwtLlmPolicy: config.sessionJwtLlmPolicy } : {}),
  fileStore,
  skillStore,
  memoryStore,
  secretProvider,
  secretStore,
  environmentClaimTtlMs: config.environmentClaimTtlMs,
  gatewayRegistryUsageEnabled: config.gatewayRegistryUsageEnabled,
  tunnelAuthProvider: runnerTunnelAuth.authProvider,
  ...(runnerTunnelAuth.allowedTunnelTokens !== undefined
    ? { allowedTunnelTokens: runnerTunnelAuth.allowedTunnelTokens }
    : {}),
  tunnelAllowedOrigins: runnerTunnelAuth.allowedOrigins,
  tunnelLocalMode: runnerTunnelAuth.localMode,
  runnerExitReports,
  agentSnapshotProvider,
  // The SAME resolver also composes the SESSION-WIDE Skill-bundle union (over
  // `session_skill_bindings`), so the owner pod pushes a colocated session's Skill
  // bundle bytes to the runner before the snapshot (paired with the `skillStore` above).
  skillsProvider: agentSnapshotProvider,
  workerRegistry,
  ...(environmentLaunchLifecycle !== undefined ? { environmentLaunchLifecycle } : {}),
};

const internalAuthVerifier = buildConfiguredInternalAuthVerifier({
  mode: config.internalAuthMode,
  ...(config.internalServiceToken ? { token: config.internalServiceToken } : {}),
  ...(config.internalServiceTokenFile ? { tokenFile: config.internalServiceTokenFile } : {}),
  audience: config.internalAuthAudience,
  ...(config.internalAuthHarnessSubject
    ? { harnessSubject: config.internalAuthHarnessSubject }
    : {}),
  ...(config.internalAuthAiGatewaySubject
    ? { aiGatewaySubject: config.internalAuthAiGatewaySubject }
    : {}),
  ...(config.internalAuthObservabilityExporterSubject
    ? { observabilityExporterSubject: config.internalAuthObservabilityExporterSubject }
    : {}),
});

const publicApp = buildPublicApp(appOptions);
const internalApp = buildInternalApp(appOptions, internalAuthVerifier);
const adminApp = buildAdminApp({
  db,
  oidc: {
    allowedIssuers: config.adminOidcAllowedIssuers,
    audience: config.adminOidcAudience,
    metadataClaims: config.adminOidcMetadataClaims,
    deniedJtiFile: config.oidcDeniedJtiFile,
  },
  platformOidc: {
    allowedIssuers: config.platformOidcAllowedIssuers,
    audience: config.platformOidcAudience,
    metadataClaims: config.platformOidcMetadataClaims,
    deniedJtiFile: config.oidcDeniedJtiFile,
  },
  store,
  secretStore,
});
await Promise.all([
  publicApp.listen({ host: '0.0.0.0', port: config.httpPort }),
  internalApp.listen({ host: '0.0.0.0', port: config.internalHttpPort }),
  adminApp.listen({ host: '0.0.0.0', port: config.adminHttpPort }),
]);
console.log(
  `registry-service-ts public=:${config.httpPort} internal=:${config.internalHttpPort} admin=:${config.adminHttpPort}`,
);

// Durable environment-claim reaper: sweep claims whose worker heartbeat has
// gone stale so an abandoned claim is freed even if no worker reconnects (the
// automatic consumer of `ENVIRONMENT_CLAIM_TTL_MS`). The `/internal/.../reap`
// route drives the same sweep on demand for ops/tests.
const claimReaper = startEnvironmentClaimReaper(new EnvironmentClaimStore(db), {
  ttlMs: config.environmentClaimTtlMs,
  onError: (err) => console.error('environment-claim reaper sweep failed', err),
});
const agentObservabilityMaintenance = startAgentObservabilityMaintenance({ db, secretStore });
const sessionDispatchObservability = new SessionDispatchObservabilityRefresher(
  buildSessionDispatchObservabilityReader(db),
);
void sessionDispatchObservability.refresh();
const sessionDispatchObservabilityTimer = setInterval(
  () => void sessionDispatchObservability.refresh(),
  SESSION_DISPATCH_OBSERVABILITY_REFRESH_INTERVAL_MS,
);
sessionDispatchObservabilityTimer.unref();

let gitCredentialReconcileTimer: ReturnType<typeof setInterval> | undefined;
let gitCredentialReconcileRunning = false;
const reconcileGitCredentialStaging = async (): Promise<void> => {
  if (!secretStore || gitCredentialReconcileRunning) return;
  gitCredentialReconcileRunning = true;
  try {
    const result = await reconcileGitCredentialStagingIntents(db, secretStore);
    if (result.processed > 0) {
      console.log(
        `registry-service-ts reconciled git credential staging intents: ${JSON.stringify(result)}`,
      );
    }
  } catch {
    // SecretStore implementations are third-party boundaries. Keep errors out
    // of logs so a provider cannot echo token bytes through an exception.
    console.warn('registry-service-ts failed to reconcile git credential staging intents');
  } finally {
    gitCredentialReconcileRunning = false;
  }
};
if (secretStore) {
  void reconcileGitCredentialStaging();
  gitCredentialReconcileTimer = setInterval(
    () => void reconcileGitCredentialStaging(),
    GIT_CREDENTIAL_STAGING_RECONCILE_INTERVAL_MS,
  );
  gitCredentialReconcileTimer.unref();
}

let sessionLifecycleReconcileRunning = false;
const reconcileSessionLifecycle = async (): Promise<void> => {
  if (sessionLifecycleReconcileRunning) return;
  sessionLifecycleReconcileRunning = true;
  try {
    const result = await reconcileSessionLifecycleOutbox(db, store);
    if (result.processed > 0) {
      console.log(
        `registry-service-ts reconciled session lifecycle events: ${JSON.stringify(result)}`,
      );
    }
  } catch (error) {
    console.warn('registry-service-ts failed to reconcile session lifecycle events', error);
  } finally {
    sessionLifecycleReconcileRunning = false;
  }
};
void reconcileSessionLifecycle();
const sessionLifecycleReconcileTimer = setInterval(
  () => void reconcileSessionLifecycle(),
  SESSION_LIFECYCLE_RECONCILE_INTERVAL_MS,
);
sessionLifecycleReconcileTimer.unref();

let triggerPlannerTimer: ReturnType<typeof setInterval> | undefined;
let triggerDispatcherTimer: ReturnType<typeof setInterval> | undefined;
let triggerPlannerRunning = false;
let triggerDispatcherRunning = false;
const reconcileTriggerPlanner = async (): Promise<void> => {
  if (triggerPlannerRunning) return;
  triggerPlannerRunning = true;
  try {
    const result = await reconcileDueAgentTriggers(db, {
      batchSize: config.triggerReconcileBatchSize,
    });
    if (result.created > 0) triggerPlannerTotal.inc({ result: 'created' }, result.created);
    if (result.deduplicated > 0) {
      triggerPlannerTotal.inc({ result: 'deduplicated' }, result.deduplicated);
    }
    if (result.misfired > 0) triggerPlannerTotal.inc({ result: 'misfired' }, result.misfired);
    if (result.paused > 0) triggerPlannerTotal.inc({ result: 'paused' }, result.paused);
    triggerDueBacklog.set(result.dueBacklog);
    triggerOldestDueAgeSeconds.set(result.oldestDueAgeSeconds);
  } catch (error) {
    triggerPlannerTotal.inc({ result: 'error' });
    console.warn('registry-service-ts failed to reconcile due triggers', error);
  } finally {
    triggerPlannerRunning = false;
  }
};
const reconcileTriggerDispatcher = async (): Promise<void> => {
  if (triggerDispatcherRunning) return;
  triggerDispatcherRunning = true;
  try {
    const result = await dispatchPendingAgentTriggerFires(db, {
      batchSize: config.triggerReconcileBatchSize,
    });
    if (result.enqueued > 0) {
      triggerDispatcherTotal.inc({ result: 'enqueued' }, result.enqueued);
      // Publication is outside the fire transaction. Kick the ordinary outbox
      // immediately; its own timer remains the crash/retry safety net.
      void reconcileSessionLifecycle();
    }
    if (result.canceled > 0) {
      triggerDispatcherTotal.inc({ result: 'canceled' }, result.canceled);
    }
    if (result.retried > 0) triggerDispatcherTotal.inc({ result: 'retry' }, result.retried);
    if (result.failed > 0) triggerDispatcherTotal.inc({ result: 'failed' }, result.failed);
  } catch (error) {
    triggerDispatcherTotal.inc({ result: 'error' });
    console.warn('registry-service-ts failed to dispatch trigger fires', error);
  } finally {
    triggerDispatcherRunning = false;
  }
};
if (config.triggerSchedulerEnabled) {
  void reconcileTriggerPlanner();
  void reconcileTriggerDispatcher();
  triggerPlannerTimer = setInterval(
    () => void reconcileTriggerPlanner(),
    config.triggerReconcileIntervalMs,
  );
  triggerDispatcherTimer = setInterval(
    () => void reconcileTriggerDispatcher(),
    config.triggerReconcileIntervalMs,
  );
  triggerPlannerTimer.unref();
  triggerDispatcherTimer.unref();
}

let skillBundleDeletionReconcileRunning = false;
const reconcileSkillBundleDeletions = async (): Promise<void> => {
  if (skillBundleDeletionReconcileRunning) return;
  skillBundleDeletionReconcileRunning = true;
  try {
    const result = await reconcileSkillBundleDeletionOutbox(db, skillStore);
    if (result.processed > 0) {
      console.log(
        `registry-service-ts reconciled skill bundle deletions: ${JSON.stringify(result)}`,
      );
    }
  } catch (error) {
    console.warn('registry-service-ts failed to reconcile skill bundle deletions', error);
  } finally {
    skillBundleDeletionReconcileRunning = false;
  }
};
void reconcileSkillBundleDeletions();
const skillBundleDeletionReconcileTimer = setInterval(
  () => void reconcileSkillBundleDeletions(),
  SKILL_BUNDLE_DELETION_RECONCILE_INTERVAL_MS,
);
skillBundleDeletionReconcileTimer.unref();

// Model prices. The seed catalog is loaded first so a deployment with no egress
// prices correctly from boot; the refresher then layers upstream rows on top
// when a URL is configured. No URL is a supported configuration, not an error.
const modelPriceStore = createPostgresModelPriceStore(db);
await loadSeedModelPrices(modelPriceStore).catch((err) => {
  console.error('registry: seeding model prices failed', err);
});
const modelPriceRefresher = config.priceRefreshUrl
  ? createModelPriceRefresher({
      store: modelPriceStore,
      url: config.priceRefreshUrl,
      provider: config.priceRefreshProvider,
      intervalMs: config.priceRefreshIntervalMs,
    })
  : null;
modelPriceRefresher?.start();

// Drives the registry read models off the transcript consumer: the event index
// (paginated `/events` + SSE) and the `session_threads` Threads-API read model.
// `indexTranscriptEvents` (session-events-index.ts) projects both — including
// HARNESS-emitted `session.thread_*` events (a coordinator's, arriving via the
// runner tunnel -> session-event-bridge append, not `POST /events`) — so a
// single consumer wire covers both read models.
await eventSource.start(async (event) => {
  await indexTranscriptEvents(db, [event]);
});

const shutdown = async (): Promise<void> => {
  console.log('registry-service-ts shutting down');
  clearInterval(sessionDispatchObservabilityTimer);
  if (gitCredentialReconcileTimer) clearInterval(gitCredentialReconcileTimer);
  clearInterval(sessionLifecycleReconcileTimer);
  if (triggerPlannerTimer) clearInterval(triggerPlannerTimer);
  if (triggerDispatcherTimer) clearInterval(triggerDispatcherTimer);
  clearInterval(skillBundleDeletionReconcileTimer);
  await agentObservabilityMaintenance.stop();
  modelPriceRefresher?.stop();
  try {
    claimReaper.stop();
    await Promise.all([publicApp.close(), internalApp.close(), adminApp.close()]);
    await eventSource.stop();
  } finally {
    try {
      await store.close();
    } finally {
      await closeTranscriptBackend?.();
    }
    await fileStore.close();
    await skillStore.close();
    await memoryStore.close();
    await memoryPool.end().catch(() => {});
    await metadataPool.end();
    for (const stop of stopPoolObservers) stop();
  }
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
