// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { CodexRunnerAccounting } from './domain/codex-runner-accounting.js';
import { saveRunnerHarnessState } from './domain/harness-state.js';
import { buildRunnerResourcesFactory } from './domain/runner-resources-factory.js';
import { loadSessionExecutionOwner } from './domain/session-harness-binding.js';
import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from 'fastify';
import multipart from '@fastify/multipart';
import websocket from '@fastify/websocket';
import type { IncomingMessage, ServerOptions } from 'node:http';
import type { DbClient } from './persistence/postgres/client.js';
import type { OidcConfig } from './auth/oidc.js';
import { buildAuth } from './auth/auth.js';
import { registerApiPerformance } from './observability/api-performance.js';
import { registerReadAdmission, type ReadAdmissionOptions } from './middleware/read-admission.js';
import { buildAdminAuth } from './auth/admin-auth.js';
import { buildPlatformAuth } from './auth/platform-auth.js';
import { buildInternalAuth, type InternalAuthVerifier } from './auth/internal-auth.js';
import {
  buildPgIdempotencyStore,
  buildIdempotencyPreHandler,
  buildIdempotencyResponseHook,
} from './middleware/idempotency.js';
import { registerSkillsRoutes } from './api/skills.routes.js';
import { registerAgentsRoutes } from './api/agents.routes.js';
import { registerDiscoveryRoutes } from './api/discovery.routes.js';
import { loadOrganizationId, registerGuardrailsRoutes } from './api/guardrails.routes.js';
import { registerModelPricesRoutes } from './api/model-prices.routes.js';
import { createPostgresModelPriceStore } from './pricing/store.js';
import { registerEnvironmentsRoutes } from './api/environments.routes.js';
import { registerSessionsRoutes } from './api/sessions.routes.js';
import { registerVaultsRoutes } from './api/vaults.routes.js';
import { registerVaultCredentialRoutes } from './api/vault-credentials.routes.js';
import { registerInternalRoutes } from './api/internal.routes.js';
import { registerOutcomesRoutes } from './api/outcomes.routes.js';
import { registerFilesRoutes } from './api/files.routes.js';
import { registerMemoryStoresRoutes } from './api/memory-stores.routes.js';
import { registerGitCredsRoutes } from './api/git-creds.routes.js';
import { registerGitProxyRoutes } from './api/git-proxy.routes.js';
import { registerAdminRoutes } from './api/admin.routes.js';
import { registerPlatformRoutes } from './api/platform.routes.js';
import { registerTriggerRoutes } from './api/triggers.routes.js';
import { registry } from './metrics.js';
import type { TranscriptStore } from '@orca/transcript-store';
import { transcriptStoreMetricsRegistry } from '@orca/transcript-store';
import { TUNNEL_MAX_MESSAGE_BYTES } from '@orca/harness-tunnel';
import { TunnelRegistry } from './tunnel/tunnel-registry.js';
import { WorkerRegistry } from './tunnel/worker-registry.js';
import { RunnerExitReports } from './tunnel/runner-exit-reports.js';
import { registerRunnerTunnelRoutes, type TunnelAuthProvider } from './api/runner-tunnel.routes.js';
import {
  registerWorkerTunnelRoutes,
  type EnvironmentKeyLoader,
  type EnvironmentKeyRow,
} from './api/worker-tunnel.routes.js';
import { EnvironmentClaimStore } from './domain/environment-claims.js';
import { EnvironmentTokenStore } from './domain/environment-token-store.js';
import { SessionDistributor } from './tunnel/session-distributor.js';
import { SessionEventBridgeManager } from './tunnel/session-event-bridge.js';
import type { SnapshotProvider } from './tunnel/session-snapshot-delivery.js';
import type { SkillsProvider } from './tunnel/session-skills-delivery.js';
import {
  buildDistributionSessionStore,
  buildBoundSessionResolver,
  buildEnvironmentTargetLookup,
  type EnvironmentLaunchTrigger,
} from './api/sessions.routes.js';
import { buildOnWorkerDisconnect } from './environment/launch/relaunch-on-disconnect.js';
import { environments, sessions } from './persistence/postgres/schema.js';
import { isNull, and, eq } from 'drizzle-orm';
import {
  registerTerminalAttachRoutes,
  type TerminalAttachRouteOptions,
} from './api/terminal-attach.routes.js';
import type { FileStore } from '@orca/file-store';
import type { MemoryStore } from '@orca/memory-store';
import type { SkillStore } from '@orca/skill-store';
import { InMemorySkillStore } from '@orca/skill-store';
import type { SessionJwtMinter } from './auth/session-jwt.js';
import type { SecretProvider, SecretStore } from './secrets/secret-provider.js';
import type { SessionJwtLlmPolicy } from './config.js';
import { registerClaudePublicEdge } from './middleware/claude-edge.js';
import { rewriteApiV1Alias } from './middleware/api-v1-alias.js';
import { LOGICAL_CREDENTIAL_ID_MAX_LENGTH } from './domain/provider-credential.js';
import { WORKSPACE_ID_MAX_LENGTH } from './auth/workspace-id.js';
import { INTERNAL_AGENT_OBSERVABILITY_SESSION_ID_MAX_LENGTH } from './contracts/internal.contract.js';

const SERVICE_NAME = 'registry-service-ts';
const ADMIN_PROXY_AUTHORIZATION_HEADER = 'x-orca-registry-authorization';
const BEARER_AUTHORIZATION_HEADER = /^Bearer [^\s]+$/;

interface UpgradeFilteringServerOptions extends ServerOptions {
  shouldUpgradeCallback(request: IncomingMessage): boolean;
}

// @fastify/websocket installs a server-level upgrade listener. Without an
// explicit filter, that listener also claims Java HttpClient's `Upgrade: h2c`
// probe and routes an ordinary REST request without its body. Node then reports
// a Content-Length mismatch (or multipart reports a missing file). Keep every
// non-WebSocket upgrade on the normal HTTP request path instead.
const WEBSOCKET_ONLY_HTTP_OPTIONS: UpgradeFilteringServerOptions = {
  shouldUpgradeCallback: (request) => request.headers.upgrade?.trim().toLowerCase() === 'websocket',
};

/** One externally visible route registered on a listener. */
export interface RouteRecord {
  method: string;
  url: string;
}

const routeTables = new WeakMap<FastifyInstance, RouteRecord[]>();

/**
 * Start recording what an app registers. Called immediately after the instance
 * is created and before any route, since an `onRoute` hook only sees routes
 * added after it.
 *
 * `HEAD` routes are skipped: Fastify derives one from every `GET` and they are
 * not separately declared surface.
 */
function recordRouteTable(app: FastifyInstance): void {
  const table: RouteRecord[] = [];
  routeTables.set(app, table);
  app.addHook('onRoute', (route) => {
    for (const method of [route.method].flat()) {
      // find-my-way uses `::` to escape a literal colon in route declarations.
      // Contracts and clients use the externally visible single-colon form.
      if (method !== 'HEAD') table.push({ method, url: route.url.replaceAll('::', ':') });
    }
  });
}

/**
 * Every route the given app registered, in registration order.
 *
 * `src/contracts/*.contract.ts` and the `app.get`/`app.post` calls in
 * `src/api/*.routes.ts` are two hand-maintained descriptions of the same
 * surface, and only the contract is generated from — so a handler added or
 * removed on its own is invisible to the OpenAPI and conformance drift gates.
 * `test/unit/route-contract-parity.spec.ts` closes that by diffing the two, and
 * this is where it reads the router's side.
 *
 * Recorded here rather than parsed out of `printRoutes()`: that output is a
 * human-facing tree, not an API, and a parser that quietly stopped matching it
 * would leave the parity test passing over an empty comparison.
 */
export function registeredRoutes(app: FastifyInstance): readonly RouteRecord[] {
  const table = routeTables.get(app);
  if (!table) {
    throw new Error('this app was not built by buildPublicApp/buildInternalApp/buildAdminApp');
  }
  return table;
}

export interface SseConfig {
  bufferSize: number;
  dropAgeMs: number;
  heartbeatMs: number;
}

export interface BuildAppOptions {
  db: DbClient;
  /** Disable to restore the per-item Session/Thread/Trigger and Agent list readers. */
  batchReadsEnabled?: boolean;
  apiKeyProofCacheEnabled?: boolean;
  heavyReadAdmission?: ReadAdmissionOptions;
  oidc: OidcConfig;
  /** Trusted reverse-proxy IPs/CIDRs for public client-address resolution. */
  publicTrustProxy?: string[];
  store: TranscriptStore;
  sse: SseConfig;
  jwtMinter: SessionJwtMinter;
  /** Public Registry HTTP origin reachable from managed model-tool sandboxes. */
  registryBaseUrl?: string;
  /** Same public listener, addressed locally for trusted Git seed fetches. */
  registryLocalBaseUrl?: string;
  sessionJwtLlmPolicy?: SessionJwtLlmPolicy;
  fileStore: FileStore;
  /**
   * Optional Fastify log level (`fatal|error|warn|info|debug|trace|silent`).
   * Defaults to `REGISTRY_LOG_LEVEL` from the environment, else the logger is
   * off. Enables the tunnel engines' per-connection diagnostics (they log via
   * `req.log`) for operators debugging worker/runner connectivity.
   */
  logLevel?: string;
  /**
   * Immutable custom/Anthropic skill bundles. Production always provides it;
   * omitting it is a startup configuration error rather than a partial API.
   */
  skillStore: SkillStore;
  /**
   * Optional MemoryStore. When provided, the registry registers the
   * `/v1/memory_stores/*` routes. Tests that don't exercise memory paths
   * (chat-only, files-only, etc.) can omit this and the routes simply
   * aren't mounted.
   */
  memoryStore?: MemoryStore;
  /**
   * Optional read-only SecretProvider for legacy env/cloud vault and Git
   * credential refs. Runtime routes are registered when either this provider
   * or `secretStore` is configured.
   */
  secretProvider?: SecretProvider;
  /**
   * Optional write-capable store for nested vault credentials and raw
   * github_repository authorization tokens. When omitted, secret-bearing
   * creates and rotations return 503 rather than storing raw material in
   * Postgres.
   */
  secretStore?: SecretStore | undefined;
  /**
   * Heartbeat-staleness TTL (ms) for the durable environment-claim reaper
   * (`POST /internal/environments/claims/reap`). When omitted the internal
   * routes fall back to their built-in default (90s). Production threads
   * `config.environmentClaimTtlMs` here.
   */
  environmentClaimTtlMs?: number;
  /** True only when AI Gateway's Registry usage sink is deployed and enabled. */
  gatewayRegistryUsageEnabled?: boolean;
  /**
   * Shared runner-tunnel registry. Runners connect over the
   * `/v1/tunnels/runners/:runnerId` WebSocket endpoint and register here;
   * the HTTP request-routing layer (the `TunnelTransport` built against this
   * registry) drains the same sessions to tunnel requests into runners. When
   * omitted, `buildApp` constructs a fresh registry — callers that need to
   * route requests through it should pass their own instance (and read it back
   * off the returned app via {@link getTunnelRegistry}).
   */
  tunnelRegistry?: TunnelRegistry;
  /**
   * Optional set of accepted runner-tunnel binding tokens. When set, a remote
   * runner's tunnel token must be present in this set (and the runner may use a
   * stable, non-token-derived id); loopback runners bypass it. When omitted, any
   * token-bound runner id is accepted — the shared remote-server posture.
   */
  allowedTunnelTokens?: ReadonlySet<string>;
  /**
   * Optional auth provider used to resolve the runner-tunnel owner from the WS
   * handshake. When set, an unauthenticated non-loopback runner is refused
   * (fail-closed) and an authenticated runner registers under its owner. When
   * omitted, the tunnel runs in single-user / no-auth mode. The same provider
   * gates the `GET /internal/runners` and `/internal/runners/:id/status`
   * handlers (401 on an unauthenticated request, owner-scoped listing, and
   * cross-tenant enumeration-hiding).
   */
  tunnelAuthProvider?: TunnelAuthProvider;
  /**
   * Optional store of worker-reported runner exit causes
   * (`worker.runner_exited`). When set, `GET /internal/runners/:id/status`
   * surfaces the (owner-scoped) failure cause for a runner that died before or
   * after connecting, so a waiting client fails fast instead of polling to a
   * timeout. When omitted the field is simply absent from the status response.
   */
  runnerExitReports?: RunnerExitReports;
  /**
   * Optional explicit allow-list of additional permitted runner-tunnel `Origin`
   * values for the CSWSH guard. Always honored; in non-local mode a non-empty
   * allow-list flips the origin policy from passthrough to deny-by-default.
   */
  tunnelAllowedOrigins?: ReadonlySet<string>;
  /**
   * Whether the runner-tunnel endpoint runs in single-user local mode for the
   * CSWSH origin policy. Defaults to `true` when no {@link tunnelAuthProvider} is
   * configured and `false` otherwise.
   */
  tunnelLocalMode?: boolean;
  /** Optional hook fired when a runner tunnel is established. */
  onRunnerConnect?: (runnerId: string) => Promise<void>;
  /**
   * Optional override (ms) for how long {@link onRunnerConnect} is awaited before
   * the tunnel handler stops waiting and proceeds, so a hung hook can't stall WS
   * teardown. Defaults to the route's 30s bound; primarily a tuning / test seam.
   */
  onRunnerConnectTimeoutMs?: number;
  /** Optional hook fired when a runner tunnel closes. */
  onRunnerDisconnect?: (runnerId: string) => Promise<void>;
  /**
   * Shared worker-tunnel registry of live environment-worker connections on this
   * replica. Workers connect over the public `/v1/tunnels/environments/:environmentId`
   * WebSocket endpoint and register here; the distribution layer drains the same
   * connections to push launch-runner control frames. When omitted, `buildApp`
   * constructs a fresh registry — callers that need to push control frames should
   * pass their own instance (and read it back via {@link getWorkerRegistry}).
   */
  workerRegistry?: WorkerRegistry;
  /**
   * Identity of this registry replica, recorded as the worker-tunnel claim's
   * `owner_pod`. When omitted, the worker-tunnel route derives a stable per-process
   * id; production threads the pod name here.
   */
  workerTunnelOwnerPod?: string;
  /**
   * Optional explicit allow-list of additional permitted worker-tunnel `Origin`
   * values for the CSWSH guard. Always honored; in non-local mode a non-empty
   * allow-list flips the origin policy from passthrough to deny-by-default.
   */
  workerTunnelAllowedOrigins?: ReadonlySet<string>;
  /**
   * Whether the worker-tunnel endpoint runs in single-user local mode for the
   * CSWSH origin policy. Defaults to `false` (the deployed posture; the Env Key
   * is the credential).
   */
  workerTunnelLocalMode?: boolean;
  /** Optional hook fired when a worker tunnel is established. */
  onWorkerConnect?: (environmentId: string) => Promise<void>;
  /** Optional hook fired when a worker tunnel closes. */
  onWorkerDisconnect?: (environmentId: string) => Promise<void>;
  /**
   * Optional hook fired with `(runnerId, error)` when a worker reports one of its
   * spawned runners died (`worker.runner_exited`). The server wires this to mark
   * the runner's session(s) failed and push the cause to the open view.
   */
  onWorkerRunnerExited?: (runnerId: string, error: string) => Promise<void>;
  /**
   * Optional provider that composes the credential-free agent snapshot for a
   * (re)connecting runner. When wired (production threads an
   * {@link AgentSnapshotResolver} over a Drizzle loader + the JWT minter +
   * gateway URLs), the owner-pod bridge manager delivers the snapshot — model +
   * provider + composed system + tool allowlists + egress config — over the
   * runner tunnel at session start, before recovery + the bridge. When omitted
   * (chat-only / cloud-only deployments, and tests that don't exercise snapshot
   * delivery), no snapshot is delivered and the bridge runs unchanged.
   */
  agentSnapshotProvider?: SnapshotProvider;
  /**
   * Optional provider that composes the SESSION-WIDE Skill-bundle union for a
   * (re)connecting runner. When wired ALONGSIDE {@link skillStore} (production threads
   * the SAME {@link AgentSnapshotResolver} instance, which resolves the union from
   * `session_skill_bindings`), the owner-pod bridge manager PUSHES the pinned Skill
   * bundle bytes over the runner tunnel BEFORE the snapshot, so the colocated runner
   * materializes them as a native `--plugin-dir` plugin. Omitted → no skills push.
   */
  skillsProvider?: SkillsProvider;
  /**
   * Optional environment-launch lifecycle for `target=cloud` colocated
   * sessions. The session-create route kicks its background
   * provision→mint→startWorker→wait-online pipeline
   * (`src/environment/launch/environment-launch-lifecycle.ts`) when a cloud
   * session's environment has no connected worker yet. When omitted, a
   * `target=cloud` session's create still persists distribution PENDING (the
   * distributor is un-gated for cloud unconditionally), but no box is ever
   * provisioned — the session simply stays pending, exactly like a
   * self_hosted session on an environment with no worker. Production wires an
   * `EnvironmentLaunchLifecycle` built from the configured `EnvironmentLauncher`
   * backend; a deployment with no cloud launcher backend configured omits this
   * and cloud sessions are inert (create-only), same as today.
   */
  environmentLaunchLifecycle?: EnvironmentLaunchTrigger;
  /**
   * Optional fetch implementation for outbound HTTP performed by API routes
   * (vault credential OAuth refresh, including forced internal resolution,
   * plus the MCP probe in mcp_oauth_validate). Tests inject a stub;
   * production defaults to the guarded global fetch.
   */
  fetchImpl?: typeof fetch | undefined;
}

/**
 * Fastify decoration key holding the app's {@link TunnelRegistry}, so callers
 * (and tests) can reach the same registry the runner-tunnel route registers
 * sessions into.
 */
const TUNNEL_REGISTRY_DECORATOR = 'tunnelRegistry';

/** Read the {@link TunnelRegistry} an app was built with. */
export function getTunnelRegistry(app: FastifyInstance): TunnelRegistry {
  return app.getDecorator<TunnelRegistry>(TUNNEL_REGISTRY_DECORATOR);
}

/**
 * Fastify decoration key holding the app's worker-tunnel {@link WorkerRegistry}, so
 * callers (and tests) can reach the same registry the worker-tunnel route
 * registers connections into.
 */
const WORKER_REGISTRY_DECORATOR = 'workerRegistry';

/** Read the worker-tunnel {@link WorkerRegistry} an app was built with. */
export function getWorkerRegistry(app: FastifyInstance): WorkerRegistry {
  return app.getDecorator<WorkerRegistry>(WORKER_REGISTRY_DECORATOR);
}

/**
 * Fastify decoration key holding the app's owner-pod
 * {@link SessionEventBridgeManager}, so callers (and tests) can reach the same
 * manager the runner-tunnel connect/disconnect hooks drive — e.g. to assert the
 * `onClose` shutdown hook drained its live bridges.
 */
const SESSION_EVENT_BRIDGES_DECORATOR = 'sessionEventBridges';

/** Read the owner-pod {@link SessionEventBridgeManager} an app was built with. */
export function getSessionEventBridges(app: FastifyInstance): SessionEventBridgeManager {
  return app.getDecorator<SessionEventBridgeManager>(SESSION_EVENT_BRIDGES_DECORATOR);
}

/**
 * Build an {@link EnvironmentKeyLoader} over the `environments` table.
 *
 * Loads the env-key credential row (digest + expiry + archive flag) plus the
 * owning workspace for the worker-tunnel Env-Key auth gate. Returns `null` for an
 * unknown id so the auth path fails closed without leaking which ids exist.
 */
function buildEnvironmentKeyLoader(db: DbClient): EnvironmentKeyLoader {
  return {
    async loadEnvironmentKeyRow(environmentId: string): Promise<EnvironmentKeyRow | null> {
      const rows = await db
        .select()
        .from(environments)
        .where(and(isNull(environments.deletedAt), eq(environments.id, environmentId)))
        .limit(1);
      const row = rows[0];
      if (!row) {
        return null;
      }
      return {
        workspaceId: row.workspaceId,
        envKeyDigest: row.envKeyDigest,
        envKeyExpiresAt: row.envKeyExpiresAt,
        archived: row.archivedAt !== null,
      };
    },
  };
}

/**
 * Build the terminal-attach route's runner resolver over the `sessions` table.
 *
 * Resolves the runner bound to a session for the authenticated caller: the lookup
 * is OWNER-SCOPED on `req.auth.workspaceId` (a client may only attach to a terminal
 * in a session it owns), and returns the bound `runner_id` only when that runner is
 * currently ONLINE in the shared {@link TunnelRegistry}. Returns `null` for an
 * unknown / cross-tenant / unbound / offline session so the route closes the client
 * cleanly without leaking which sessions exist.
 */
function buildTerminalAttachRunnerResolver(
  db: DbClient,
  tunnelRegistry: TunnelRegistry,
): TerminalAttachRouteOptions['resolveRunnerId'] {
  return async (sessionId, req) => {
    const workspaceId = req.auth?.workspaceId;
    if (workspaceId === undefined) {
      return null;
    }
    const rows = await db
      .select({ runnerId: sessions.runnerId })
      .from(sessions)
      .where(
        and(
          isNull(sessions.deletedAt),
          eq(sessions.id, sessionId),
          eq(sessions.workspaceId, workspaceId),
        ),
      )
      .limit(1);
    const runnerId = rows[0]?.runnerId ?? null;
    if (runnerId === null || !tunnelRegistry.has(runnerId)) {
      return null;
    }
    return runnerId;
  };
}

// Test-only compatibility surface (kept distinct from `buildCombinedTestApp`
// only by name — every existing caller is a test that omits `skillStore`, so
// this defaults it the same way). Production wires `buildPublicApp` +
// `buildInternalApp` + `buildAdminApp` on their own ports; nothing in this
// codebase constructs a single-surface `buildApp` for a real deployment.
export function buildApp(
  opts: Omit<BuildAppOptions, 'skillStore'> & { skillStore?: SkillStore },
): FastifyInstance {
  return buildSurfaceApp(
    { ...opts, skillStore: opts.skillStore ?? new InMemorySkillStore() },
    'combined',
  );
}

export interface BuildAdminAppOptions {
  db: DbClient;
  oidc: OidcConfig;
  platformOidc: OidcConfig;
  store: TranscriptStore;
  /**
   * Optional write-capable SecretStore for organization-default and
   * workspace-custom observability mutations. Without it credential-bearing
   * replacements fail closed with 503; read-only admin state remains available.
   */
  secretStore?: SecretStore | undefined;
}

type AppSurface = 'combined' | 'public' | 'internal';

/**
 * Test-only compatibility surface. Production uses the two explicitly split
 * builders below so the public listener never registers `/internal/*`.
 */
export function buildCombinedTestApp(
  opts: Omit<BuildAppOptions, 'skillStore'> & { skillStore?: SkillStore },
): FastifyInstance {
  return buildSurfaceApp(
    { ...opts, skillStore: opts.skillStore ?? new InMemorySkillStore() },
    'combined',
  );
}

export function buildPublicApp(opts: BuildAppOptions): FastifyInstance {
  return buildSurfaceApp(opts, 'public');
}

export function buildInternalApp(
  opts: Omit<BuildAppOptions, 'skillStore'> & { skillStore?: SkillStore },
  internalAuthVerifier: InternalAuthVerifier,
): FastifyInstance {
  return buildSurfaceApp(
    { ...opts, skillStore: opts.skillStore ?? new InMemorySkillStore() },
    'internal',
    internalAuthVerifier,
  );
}

/** Administrative control plane. Organization and Platform routes use
 * independent authenticators, and this listener deliberately registers
 * neither workspace data routes nor mesh-internal execution routes. */
export function buildAdminApp(opts: BuildAdminAppOptions): FastifyInstance {
  // The admin listener serves `/v1/organizations` and `/v1/platform/*`, so it
  // gets the same alias: "`/api/v1` means `/v1`" has to be one fact about this
  // deployment, not a per-listener quirk a caller has to memorise. Applying it
  // here also keeps the platform-vs-organization discriminator below reading a
  // canonical path — an aliased route tree would have had to repeat that check.
  const app = Fastify({ logger: false, rewriteUrl: rewriteApiV1Alias });
  recordRouteTable(app);
  registerApiPerformance(app, 'admin');
  // Observability mutations must never leave an auth, parser, or
  // handler error cacheable. `onRequest` precedes both JSON parsing and admin
  // auth; `rewriteUrl` has already made `/api/v1` aliases canonical here.
  app.addHook('onRequest', (req, reply, done) => {
    if (
      isAgentObservabilityInternalResolver(req) ||
      req.url.split('?')[0] === '/v1/platform/agent_observability'
    ) {
      reply.header('cache-control', 'private, no-store');
    } else if (isAgentObservabilityMutation(req)) {
      reply.header('cache-control', 'no-store');
    }
    done();
  });
  app.addContentTypeParser('application/json', { parseAs: 'string' }, (_req, body, done) => {
    const text = (body as string) ?? '';
    if (text.trim().length === 0) {
      done(null, {});
      return;
    }
    try {
      done(null, JSON.parse(text));
    } catch (err) {
      (err as { statusCode?: number }).statusCode = 400;
      done(err as Error, undefined);
    }
  });
  const adminAuth = buildAdminAuth({ db: opts.db, oidc: opts.oidc });
  const platformAuth = buildPlatformAuth({ db: opts.db, oidc: opts.platformOidc });
  app.addHook('preHandler', async (req, reply) => {
    const path = req.url.split('?')[0] ?? '';
    if (path === '/healthz' || path === '/readyz') return;
    // This path belongs only to the internal listener. Let Fastify emit its
    // normal 404 on the admin listener instead of turning a wrong-listener
    // request into an admin-auth 401.
    if (isAgentObservabilityInternalResolver(req)) return;
    if (!normalizeAdminProxyAuthorization(req, reply)) return reply;
    if (path.startsWith('/v1/platform/')) {
      return platformAuth(req, reply);
    }
    return adminAuth(req, reply);
  });
  app.get('/healthz', async () => ({ status: 'ok', service: SERVICE_NAME }));
  app.get('/readyz', async () => ({ status: 'ready', service: SERVICE_NAME }));
  registerAdminRoutes(app, opts.db, opts.store, opts.secretStore);
  registerPlatformRoutes(app, opts.db);
  return app;
}

function isAgentObservabilityMutation(req: FastifyRequest): boolean {
  const path = req.url.split('?')[0] ?? '';
  return (
    (req.method === 'PUT' && path === '/v1/organizations/agent_observability') ||
    (req.method === 'PUT' &&
      /^\/v1\/organizations\/workspaces\/[^/]+\/agent_observability$/u.test(path)) ||
    (req.method === 'POST' && path === '/v1/organizations/agent_observability:disable') ||
    (req.method === 'POST' &&
      path === '/v1/organizations/agent_observability:rotate_credentials') ||
    (req.method === 'POST' &&
      /^\/v1\/organizations\/workspaces\/[^/]+\/agent_observability:rotate_credentials$/u.test(
        path,
      ))
  );
}

type AgentObservabilityInternalResolver = 'context' | 'secret';

/**
 * Resolver paths are secret-adjacent authorization boundaries even when a
 * request reaches the wrong listener. Keep this matcher shared by every
 * listener hook so auth/parser/404 failures receive no-store before handling.
 */
function agentObservabilityInternalResolver(
  req: FastifyRequest,
): AgentObservabilityInternalResolver | null {
  const path = req.url.split('?')[0] ?? '';
  const match =
    /^\/internal\/v1\/workspaces\/[^/]+\/sessions\/[^/]+\/agent-observability\/(context|secret)\/resolve$/u.exec(
      path,
    );
  return match?.[1] === 'context' || match?.[1] === 'secret' ? match[1] : null;
}

function isAgentObservabilityInternalResolver(req: FastifyRequest): boolean {
  return agentObservabilityInternalResolver(req) !== null;
}

function overlongAgentObservabilityResolver(
  req: FastifyRequest,
): AgentObservabilityInternalResolver | null {
  const path = req.url.split('?')[0] ?? '';
  const match =
    /^\/internal\/v1\/workspaces\/([^/]+)\/sessions\/([^/]+)\/agent-observability\/(context|secret)\/resolve$/u.exec(
      path,
    );
  if (
    match === null ||
    (match[1]!.length <= WORKSPACE_ID_MAX_LENGTH &&
      match[2]!.length <= INTERNAL_AGENT_OBSERVABILITY_SESSION_ID_MAX_LENGTH)
  ) {
    return null;
  }
  return match[3] as AgentObservabilityInternalResolver;
}

/**
 * Normalize the admin listener's proxy-safe bearer carrier before either
 * control-plane authenticator runs. The alternate header is never forwarded
 * to route handlers, and ambiguous or malformed requests fail closed rather
 * than choosing one credential.
 */
function normalizeAdminProxyAuthorization(req: FastifyRequest, reply: FastifyReply): boolean {
  const alternate = req.headers[ADMIN_PROXY_AUTHORIZATION_HEADER];
  if (alternate === undefined) return true;

  delete req.headers[ADMIN_PROXY_AUTHORIZATION_HEADER];
  if (
    req.headers.authorization !== undefined ||
    typeof alternate !== 'string' ||
    !BEARER_AUTHORIZATION_HEADER.test(alternate)
  ) {
    reply.code(401).send({ error: 'unauthenticated' });
    return false;
  }

  req.headers.authorization = alternate;
  return true;
}

function buildSurfaceApp(
  opts: BuildAppOptions,
  surface: AppSurface,
  internalAuthVerifier?: InternalAuthVerifier,
): FastifyInstance {
  // Logger is off by default (the registry's own request log is noisy and the
  // service emits structured lines on its own). `REGISTRY_LOG_LEVEL` opts into
  // Fastify's pino logger at the given level — which is ALSO what surfaces the
  // worker/runner tunnel engines' per-connection diagnostics (they log through
  // `req.log`), so an operator debugging a worker that connects-but-does-not-claim
  // can see the engine's connect/claim/teardown lines instead of silence.
  const logLevel = opts.logLevel ?? process.env['REGISTRY_LOG_LEVEL'];
  const app = Fastify({
    ...(surface !== 'internal' ? { http: WEBSOCKET_ONLY_HTTP_OPTIONS } : {}),
    ...(logLevel && logLevel.toLowerCase() !== 'silent'
      ? { logger: { level: logLevel } }
      : { logger: false }),
    // Fastify rejects path parameters longer than 100 characters before the
    // handler runs and exposes this limit only per listener, not per route.
    // Logical credential IDs are valid through the exported contract maximum,
    // so the internal resolver (and its combined test surface) must admit it.
    // Other internal parameters through that length consequently reach their
    // existing handler validation; the router remains bounded at the contract's
    // single source of truth.
    ...(surface !== 'public'
      ? { routerOptions: { maxParamLength: LOGICAL_CREDENTIAL_ID_MAX_LENGTH } }
      : {}),
    // `rewriteUrl` is the only hook that runs before routing, which is what the
    // `/api/v1` → `/v1` alias has to be: routing is where the alias must already
    // have been resolved. Applied to every surface built here so there is one
    // answer to "does this listener understand `/api/v1`?" rather than one per
    // surface; on the internal listener, which serves no `/v1` routes, it is a
    // no-op that turns one 404 into an identical one.
    rewriteUrl: rewriteApiV1Alias,
    ...(surface !== 'internal' && opts.publicTrustProxy !== undefined
      ? { trustProxy: opts.publicTrustProxy }
      : {}),
  });
  recordRouteTable(app);

  registerApiPerformance(app, surface);

  // Observability resolvers are tenant/session authorization views.
  // Set this before Fastify parses JSON or authenticates so parser, auth, route,
  // and database failures cannot leave any response cacheable.
  app.addHook('onRequest', (req, reply, done) => {
    if (isAgentObservabilityInternalResolver(req)) {
      reply.header('cache-control', 'private, no-store');
    }
    const overlongResolver = surface !== 'public' ? overlongAgentObservabilityResolver(req) : null;
    if (overlongResolver !== null) {
      reply.code(400).send({
        error:
          overlongResolver === 'context'
            ? 'invalid agent observability context request'
            : 'invalid agent observability secret request',
      });
      return;
    }
    done();
  });

  // Tolerate an empty `application/json` body. Per the Claude Managed Agents
  // beta contract (managed-agents-2026-04-01), archive/delete/validate
  // operations accept a bodyless request even when the client sends
  // `Content-Type: application/json`. Fastify's built-in JSON parser otherwise
  // throws FST_ERR_CTP_EMPTY_JSON_BODY (400) before any route handler runs.
  // A zero/whitespace body is parsed as `{}`; a genuinely malformed non-empty
  // body still fails with 400 via the JSON.parse catch.
  app.addContentTypeParser('application/json', { parseAs: 'string' }, (_req, body, done) => {
    const text = (body as string) ?? '';
    if (text.trim().length === 0) {
      done(null, {});
      return;
    }
    try {
      done(null, JSON.parse(text));
    } catch (err) {
      (err as { statusCode?: number }).statusCode = 400;
      done(err as Error, undefined);
    }
  });

  // Register multipart plugin for /v1/files uploads. Fastify queues plugin
  // registration. The internal surface also needs it for output capture.
  app.register(multipart, {
    attachFieldsToBody: false,
    limits: { fileSize: 1024 * 1024 * 1024 }, // 1 GiB upper bound.
  });

  if (surface !== 'internal') {
    // The WebSocket plugin is registered inside an ENCAPSULATED child scope
    // alongside the tunnel routes (see the `app.register(tunnelScope => ...)` block
    // below), NOT on this parent instance. `@fastify/websocket` v11 and
    // `@fastify/multipart` (registered above) cannot coexist on the SAME Fastify
    // instance: multipart's body-parsing alters route compilation so the WS
    // handler is invoked with the plain `(request, reply)` HTTP shape instead of
    // `(socket, request)` — the `ws` socket is then unreachable and every tunnel
    // upgrade fails (the runner/worker tunnels become non-functional). Encapsulating
    // the WS plugin + routes in a child scope isolates them from the parent's
    // multipart parser while the parent's global `preHandler` hooks (auth,
    // idempotency) still apply, so the `/v1/tunnels/*` auth bypass keeps working.
    //
    // Runner-tunnel / worker-tunnel / claim-based distribution are PUBLIC-surface
    // concerns (external runners/workers dial the WS endpoints below) — the
    // internal (mesh-only) surface serves neither `/v1/tunnels/*` nor session
    // distribution, so all of it lives in this surface-gated branch.

    // Runner-tunnel registry: shared with the HTTP request-routing transport.
    // Runners register here over the public `/v1/tunnels/runners/:runnerId` WS
    // route (registered below, inside the WS-plugin encapsulation). Exposed on the
    // app so request-routing code and tests can drive the same sessions.
    const tunnelRegistry = opts.tunnelRegistry ?? new TunnelRegistry();
    app.decorate(TUNNEL_REGISTRY_DECORATOR, tunnelRegistry);

    // Worker-tunnel registry: live environment-worker connections on this replica.
    // Workers register here over the public `/v1/tunnels/environments/:environmentId`
    // WS route (registered below). Exposed on the app so distribution code and
    // tests can drive the same connections.
    const workerRegistry = opts.workerRegistry ?? new WorkerRegistry();
    app.decorate(WORKER_REGISTRY_DECORATOR, workerRegistry);

    // Claim-based session distributor: the single distributor that ties a client
    // session, the worker tunnel, and the runner tunnel together for a
    // `self_hosted` or `cloud` environment. It shares THIS replica's runner + worker
    // registries, so the session-create path can dispatch a launch to the
    // environment's connected worker, the runner-tunnel connect hook can flip the
    // matched session ASSIGNED, and a `worker.runner_exited` report can mark it
    // FAILED. Constructed unconditionally: a session it does not launch (no worker
    // connected, or a cloud session harness-server runs) is recorded PENDING with
    // no runner, so the wiring is uniform across deployments.
    const sessionDistributor = new SessionDistributor({
      sessions: buildDistributionSessionStore(opts.db),
      environments: buildEnvironmentTargetLookup(opts.db),
      tunnelRegistry,
      workerRegistry,
    });

    // Owner-pod session event bridge manager: when a runner connects to THIS
    // replica (the owner pod that drove its launch), it starts the single-writer
    // bridge for that runner's session — tailing the transcript for user turns,
    // sending them to the runner over the tunnel, and persisting the streamed agent
    // events back to the transcript (persist-before-forward). On runner-tunnel
    // close it stops the bridge. Shares THIS replica's transcript store + runner
    // registry; resolves a connecting runner's bound session over the `sessions`
    // table. A no-op for a runner bound to no session (cloud / not distributed
    // here), so it is safe to wire for every runner tunnel.
    const sessionEventBridges = new SessionEventBridgeManager({
      accountingFactory: (context) => new CodexRunnerAccounting(opts.db, context),
      logger: app.log,
      ownsSession: async (workspaceId, sessionId) =>
        (await loadSessionExecutionOwner(opts.db, workspaceId, sessionId)) === 'registry',
      saveHarnessState: (workspaceId, sessionId, runnerId, state) =>
        saveRunnerHarnessState(opts.db, workspaceId, sessionId, runnerId, state),
      store: opts.store,
      registry: tunnelRegistry,
      resolver: buildBoundSessionResolver(opts.db),
      resourcesFactory: buildRunnerResourcesFactory({
        db: opts.db,
        jwtMinter: opts.jwtMinter,
        ...(opts.registryBaseUrl ? { registryBaseUrl: opts.registryBaseUrl } : {}),
        ...(opts.registryLocalBaseUrl ? { registryLocalBaseUrl: opts.registryLocalBaseUrl } : {}),
        fileStore: opts.fileStore,
        store: opts.store,
        ...(opts.memoryStore ? { memoryStore: opts.memoryStore } : {}),
      }),
      // When a snapshot provider is wired, the manager delivers the credential-free
      // agent snapshot over the tunnel at session start (before recovery + the
      // bridge). Absent → no snapshot delivery; the bridge runs unchanged.
      ...(opts.agentSnapshotProvider !== undefined
        ? { snapshotProvider: opts.agentSnapshotProvider }
        : {}),
      // When a skills provider is wired, the manager PUSHES the session's pinned Skill
      // bundle bytes over the tunnel BEFORE the snapshot (colocated progressive
      // disclosure). Paired with the always-present `skillStore`. Absent → no skills push.
      ...(opts.skillsProvider !== undefined
        ? { skillsProvider: opts.skillsProvider, skillStore: opts.skillStore }
        : {}),
    });
    app.decorate(SESSION_EVENT_BRIDGES_DECORATOR, sessionEventBridges);
    // Drain every live bridge on server shutdown. `app.close()` (driven by the
    // SIGINT/SIGTERM `shutdown` in main.ts) runs onClose hooks, so the owner pod's
    // single-writer bridges are stopped — each aborts its tail loops and cancels any
    // in-flight turn — instead of being left running while the process exits. The
    // runner-tunnel close hook already stops a bridge per-runner; this covers the
    // process-wide teardown where no per-runner close fires (the sockets go down with
    // the server). `stopAll` is idempotent, so a later per-runner close is harmless.
    app.addHook('onClose', () => sessionEventBridges.stopAll());

    registerClaudePublicEdge(app);
    const idempotencyStore = buildPgIdempotencyStore(opts.db);
    app.addHook(
      'preHandler',
      buildAuth({
        db: opts.db,
        oidc: opts.oidc,
        apiKeyProofCacheEnabled: opts.apiKeyProofCacheEnabled,
      }),
    );
    app.addHook('preHandler', rejectExplicitWorkspaceSelector);
    if (opts.heavyReadAdmission) registerReadAdmission(app, opts.heavyReadAdmission);
    app.addHook('preHandler', buildIdempotencyPreHandler(idempotencyStore));
    app.addHook('onSend', buildIdempotencyResponseHook(idempotencyStore));
    if (surface === 'combined') {
      // The combined surface is test-only and intentionally has no internal
      // auth verifier. Inject the same request context that the production
      // internal-auth preHandler guarantees so handlers never need test-mode
      // identity fallbacks in security audit records.
      app.addHook('preHandler', async (req) => {
        const path = req.url.split('?')[0] ?? '';
        if (path.startsWith('/internal/')) {
          req.internalAuth = { caller: 'shared', subject: 'combined-test-internal' };
        }
      });
    }

    app.get('/healthz', async () => ({ status: 'ok', service: SERVICE_NAME }));
    app.get('/readyz', async () => ({ status: 'ready', service: SERVICE_NAME }));
    // Group/version discovery. Public-facing only: `/internal/*` is spoken between
    // the registry and the harness, which are built together and have no boundary
    // to probe. Authenticated like every other route on this listener — the
    // allowlist in `auth/auth.ts` holds the probes, plus `/metrics`, which only the
    // test-only combined surface serves here. This block only runs for non-internal
    // surfaces, so no guard is needed here. It also serves the
    // `runtime.runorca.ai` group's harness catalog.
    registerDiscoveryRoutes(app);
    // The engine's policy and pricing extension groups (the third,
    // `runtime.runorca.ai`, is registered with discovery above). Served on the
    // public listener beside core, and discoverable through the same `/apis`
    // document that advertises them. Organization-tier guardrails and every price
    // write are refused here and served on the admin listener instead.
    registerGuardrailsRoutes(app, opts.db);
    registerModelPricesRoutes(app, createPostgresModelPriceStore(opts.db), (workspaceId) =>
      loadOrganizationId(opts.db, workspaceId),
    );
    // Metrics include deployment-wide operational state and may carry tenant
    // labels. Keep them off the public listener; the internal listener is the
    // production scrape target. The combined surface remains test-only.
    if (surface !== 'public') {
      app.get('/metrics', async (_req, reply) => {
        reply.header('content-type', registry.contentType);
        const a = await registry.metrics();
        const b = await transcriptStoreMetricsRegistry.metrics();
        return `${a}\n${b}`;
      });
    }

    registerSkillsRoutes(app, opts.db, opts.skillStore);
    registerAgentsRoutes(app, opts.db, opts.batchReadsEnabled);
    registerTriggerRoutes(app, opts.db, opts.batchReadsEnabled);
    // The environment-claim TTL is threaded through so the public `work_stats`
    // route's "worker connected" liveness uses the same staleness boundary the
    // durable-claim reaper sweeps with. Omitted → the route's built-in default.
    // The launch lifecycle is ALSO threaded through (I3) so archive/delete can
    // best-effort tear down a `target=cloud` environment's launcher-level box —
    // the SAME instance `registerSessionsRoutes` below uses to launch one.
    registerEnvironmentsRoutes(app, opts.db, {
      ...(opts.environmentClaimTtlMs !== undefined
        ? { environmentClaimTtlMs: opts.environmentClaimTtlMs }
        : {}),
      ...(opts.environmentLaunchLifecycle !== undefined
        ? { environmentLaunchLifecycle: opts.environmentLaunchLifecycle }
        : {}),
    });
    // The MemoryStore is forwarded so the sessions routes can validate
    // `memory_store_id` references (and resolve default mount paths) at session
    // create + attach time. When undefined the sessions handler returns a
    // user-error 400 if a caller asks for a memory_store resource — the routes
    // are still mounted so file-only deployments work unchanged.
    registerSessionsRoutes(
      app,
      opts.db,
      opts.store,
      opts.sse,
      opts.fileStore,
      opts.skillStore,
      opts.memoryStore,
      sessionDistributor,
      opts.environmentLaunchLifecycle,
      opts.secretStore,
      opts.batchReadsEnabled,
    );
    registerVaultsRoutes(app, opts.db, opts.secretStore);
    registerVaultCredentialRoutes(app, opts.db, {
      secretStore: opts.secretStore,
      fetchImpl: opts.fetchImpl,
    });
    registerOutcomesRoutes(app, opts.db);
    registerFilesRoutes(app, opts.fileStore);
    if (opts.memoryStore) registerMemoryStoresRoutes(app, opts.memoryStore);

    // The sandbox-facing Git helper is a public-surface route with its own
    // session JWT authentication. It is not a mesh-internal control route.
    if (opts.secretProvider || opts.secretStore) {
      registerGitCredsRoutes(app, {
        db: opts.db,
        jwtMinter: opts.jwtMinter,
        ...(opts.secretProvider ? { secretProvider: opts.secretProvider } : {}),
        ...(opts.secretStore ? { secretStore: opts.secretStore } : {}),
      });
      registerGitProxyRoutes(app, {
        db: opts.db,
        jwtMinter: opts.jwtMinter,
        ...(opts.secretProvider ? { secretProvider: opts.secretProvider } : {}),
        ...(opts.secretStore ? { secretStore: opts.secretStore } : {}),
      });
    }

    // Compose the distributor's connect hook with any caller-supplied one: a
    // runner connecting flips its matched self_hosted session ASSIGNED, AND any
    // additional `onRunnerConnect` the caller wired still runs. The distributor's
    // hook is owner-agnostic + a no-op for an unbound runner, so it is safe to run
    // for every runner tunnel.
    const onRunnerConnect = async (
      runnerId: string,
      resumeCursors: Readonly<Record<string, string>>,
    ): Promise<void> => {
      await sessionDistributor.onRunnerConnect(runnerId);
      // Start the owner-pod single-writer bridge for the runner's session AFTER the
      // distributor flips it ASSIGNED. The bridge resolves its session by the
      // runner binding the dispatch already persisted, so it is a no-op for an
      // unbound (cloud / not-distributed-here) runner. The runner's per-session
      // resume cursors (from its hello) are threaded so recovery serves an
      // incremental `after={cursor}` replay for the bound session.
      await sessionEventBridges.onRunnerConnect(runnerId, resumeCursors);
      if (opts.onRunnerConnect !== undefined) {
        await opts.onRunnerConnect(runnerId);
      }
    };
    // Compose the distributor's runner-exited hook with any caller-supplied one: a
    // `worker.runner_exited` report marks the matched session FAILED (the only
    // failure signal for a runner that crashed before connecting its own tunnel),
    // AND any additional `onWorkerRunnerExited` the caller wired still runs. A no-op
    // for a runner id matching no bound session.
    const onWorkerRunnerExited = async (runnerId: string, error: string): Promise<void> => {
      await sessionDistributor.markRunnerExited(runnerId, error);
      if (opts.onWorkerRunnerExited !== undefined) {
        await opts.onWorkerRunnerExited(runnerId, error);
      }
    };
    // Compose the distributor's worker-connect hook with any caller-supplied one: a
    // worker (re)connecting DRIVES launches for every self_hosted session that was
    // stranded PENDING with no runner while its worker was offline (the create-time
    // path only dispatches when a worker is already connected), AND any additional
    // `onWorkerConnect` the caller wired still runs. The distributor's hook is a
    // no-op when no session is stranded for the environment, so it is safe to run
    // for every worker tunnel. Fired by the worker-tunnel route after the tunnel loops
    // are running and the environment is registered + claimed, so the launch frames
    // it enqueues are actually sent.
    const onWorkerConnect = async (environmentId: string): Promise<void> => {
      await sessionDistributor.onWorkerConnect(environmentId);
      if (opts.onWorkerConnect !== undefined) {
        await opts.onWorkerConnect(environmentId);
      }
    };
    // Worker-disconnect -> relaunch trigger (C2 secondary fix — see
    // `relaunch-on-disconnect.ts`'s module doc for the full "why this signal,
    // why gated this way" rationale): when a `target=cloud` environment's
    // worker tunnel drops and it still has stranded (pending, unbound) work,
    // best-effort relaunch a fresh generation so a later worker-connect can
    // dispatch that work. `undefined` when no `environmentLaunchLifecycle` is
    // configured — the same "omitted -> inert" posture every other cloud-launch
    // wiring in this file uses. Built ONCE per app (not per connection) so its
    // per-environment consecutive-attempt-cap state persists across disconnects.
    const onWorkerDisconnectRelaunch =
      opts.environmentLaunchLifecycle !== undefined
        ? buildOnWorkerDisconnect({
            lifecycle: opts.environmentLaunchLifecycle,
            environments: buildEnvironmentTargetLookup(opts.db),
            sessions: buildDistributionSessionStore(opts.db),
            workerOnline: workerRegistry,
          })
        : undefined;
    // Compose the disconnect-relaunch trigger with any caller-supplied hook. Safe
    // to run for EVERY worker tunnel, self_hosted included: the trigger itself
    // gates on target=cloud internally (and is a no-op end-to-end when no
    // lifecycle is configured), mirroring `onWorkerConnect`'s own
    // safe-to-always-wire posture above.
    const onWorkerDisconnect = async (environmentId: string): Promise<void> => {
      if (onWorkerDisconnectRelaunch !== undefined) {
        await onWorkerDisconnectRelaunch(environmentId);
      }
      if (opts.onWorkerDisconnect !== undefined) {
        await opts.onWorkerDisconnect(environmentId);
      }
    };
    // Compose the distributor's runner-disconnect hook with any caller-supplied
    // one: a connected runner's tunnel closing marks its bound session FAILED (the
    // post-connect crash-recovery signal — a runner that died after connecting has
    // no `worker.runner_exited` report), AND any additional `onRunnerDisconnect` the
    // caller wired still runs. A no-op for a runner id matching no bound session.
    const onRunnerDisconnect = async (runnerId: string): Promise<void> => {
      await sessionDistributor.markRunnerDisconnected(runnerId);
      // Stop the owner-pod bridge for this runner's session — the single writer
      // halts when the runner tunnel closes. A no-op when no bridge is live for it.
      await sessionEventBridges.onRunnerDisconnect(runnerId);
      if (opts.onRunnerDisconnect !== undefined) {
        await opts.onRunnerDisconnect(runnerId);
      }
    };

    // The two public tunnel WebSocket endpoints live in an ENCAPSULATED child
    // scope with their own `@fastify/websocket` registration (see the note where
    // the parent multipart plugin is registered: multipart + WS on one instance
    // breaks the WS handler arg shape). The parent's global `preHandler` hooks
    // still run for these routes — the `/v1/tunnels/*` auth bypass in `auth.ts`
    // applies — so encapsulation isolates only the body-parsing / route-compilation
    // concern, not the auth chain.
    //
    //   - WS `/v1/tunnels/runners/:runnerId` — a self-hosted runner dials this from
    //     outside the mesh; the handler self-authenticates it via the token-binding
    //     correlation check, the owner fail-closed gate, and the CSWSH origin guard.
    //     The companion `GET /internal/runners[/...]` control reads stay mesh-only
    //     under `/internal/*` (registered on the parent above, not here).
    //   - WS `/v1/tunnels/environments/:environmentId` — a self-hosted worker dials
    //     this; the handler authenticates its Env Key against the environment row,
    //     claims the environment in the durable claim store, and guards the
    //     handshake with the same CSWSH origin policy. The shared `RunnerExitReports`
    //     store surfaces a `worker.runner_exited` report on the runner status read.
    app.register(async (tunnelScope) => {
      // The tunnel protocol is text-only JSON frames, but a single frame can carry
      // a sizable base64 body, so the per-message cap is raised to the frame
      // codec's `TUNNEL_MAX_MESSAGE_BYTES` budget rather than the `ws` default.
      await tunnelScope.register(websocket, {
        options: { maxPayload: TUNNEL_MAX_MESSAGE_BYTES },
      });
      registerRunnerTunnelRoutes(tunnelScope, {
        registry: tunnelRegistry,
        ...(opts.allowedTunnelTokens !== undefined
          ? { allowedTunnelTokens: opts.allowedTunnelTokens }
          : {}),
        ...(opts.tunnelAuthProvider !== undefined ? { authProvider: opts.tunnelAuthProvider } : {}),
        ...(opts.runnerExitReports !== undefined
          ? { runnerExitReports: opts.runnerExitReports }
          : {}),
        ...(opts.tunnelAllowedOrigins !== undefined
          ? { allowedOrigins: opts.tunnelAllowedOrigins }
          : {}),
        ...(opts.tunnelLocalMode !== undefined ? { localMode: opts.tunnelLocalMode } : {}),
        onRunnerConnect,
        ...(opts.onRunnerConnectTimeoutMs !== undefined
          ? { onRunnerConnectTimeoutMs: opts.onRunnerConnectTimeoutMs }
          : {}),
        // Always wired: the distributor's crash-recovery hook flips a connected
        // runner's bound session FAILED on tunnel close, composing any caller hook.
        onRunnerDisconnect,
      });
      registerWorkerTunnelRoutes(tunnelScope, {
        registry: workerRegistry,
        environments: buildEnvironmentKeyLoader(opts.db),
        tokens: new EnvironmentTokenStore(opts.db),
        claims: new EnvironmentClaimStore(opts.db),
        ...(opts.workerTunnelOwnerPod !== undefined ? { ownerPod: opts.workerTunnelOwnerPod } : {}),
        ...(opts.runnerExitReports !== undefined
          ? { runnerExitReports: opts.runnerExitReports }
          : {}),
        ...(opts.workerTunnelAllowedOrigins !== undefined
          ? { allowedOrigins: opts.workerTunnelAllowedOrigins }
          : {}),
        ...(opts.workerTunnelLocalMode !== undefined
          ? { localMode: opts.workerTunnelLocalMode }
          : {}),
        // Always wired: the distributor's connect hook drives launches for sessions
        // stranded pending while this environment's worker was offline, composing
        // any caller hook.
        onWorkerConnect,
        // Always wired: the disconnect-relaunch trigger is a no-op unless a
        // cloud launch lifecycle is configured, composing any caller hook.
        onWorkerDisconnect,
        onRunnerExited: onWorkerRunnerExited,
      });
    });

    // Client-facing remote terminal-attach WS endpoint. Lives in its OWN
    // encapsulated child scope with a `@fastify/websocket` registration (the WS
    // plugin cannot share the parent instance with multipart — see the tunnel-scope
    // note above). Unlike `/v1/tunnels/*`, this path is under `/v1/sessions/*`, so
    // the parent's global auth pre-handler DOES run for it — a client must be
    // authenticated, and the resolver OWNER-SCOPES the session lookup on the caller's
    // workspace. It proxies the client socket to the session's runner's pty-bridge
    // over a tunneled WS channel (reusing the shared TunnelRegistry).
    app.register(async (attachScope) => {
      await attachScope.register(websocket, {
        options: { maxPayload: TUNNEL_MAX_MESSAGE_BYTES },
      });
      const attachOptions: TerminalAttachRouteOptions = {
        registry: tunnelRegistry,
        resolveRunnerId: buildTerminalAttachRunnerResolver(opts.db, tunnelRegistry),
      };
      registerTerminalAttachRoutes(attachScope, attachOptions);
    });
  } else {
    if (!internalAuthVerifier) throw new Error('internal auth verifier is required');
    app.addHook('preHandler', buildInternalAuth(internalAuthVerifier));

    app.get('/healthz', async () => ({ status: 'ok', service: SERVICE_NAME }));
    app.get('/readyz', async () => ({ status: 'ready', service: SERVICE_NAME }));
    app.get('/metrics', async (_req, reply) => {
      reply.header('content-type', registry.contentType);
      const a = await registry.metrics();
      const b = await transcriptStoreMetricsRegistry.metrics();
      return `${a}\n${b}`;
    });
  }

  if (surface !== 'public') {
    registerInternalRoutes(
      app,
      opts.db,
      opts.jwtMinter,
      opts.fileStore,
      opts.memoryStore,
      opts.secretProvider,
      opts.secretStore,
      opts.sessionJwtLlmPolicy,
      opts.fetchImpl,
      {
        ...(opts.environmentClaimTtlMs !== undefined
          ? { environmentClaimTtlMs: opts.environmentClaimTtlMs }
          : {}),
        ...(opts.gatewayRegistryUsageEnabled !== undefined
          ? { gatewayRegistryUsageEnabled: opts.gatewayRegistryUsageEnabled }
          : {}),
      },
    );
  }

  return app;
}

/**
 * Public workspace scope is selected exclusively by the authenticated
 * principal. Reject an explicit top-level selector instead of silently
 * ignoring it: accepting `workspace_id` would make a request appear to have
 * targeted a workspace that the server never authorized.
 *
 * Nested metadata remains opaque user data, so only top-level body/query
 * fields are reserved here.
 *
 * Covers the API-group tree as well as core. Group resources derive their
 * workspace from the credential exactly as core does, so a guard that stopped at
 * `/v1/` would let `POST /apis/policy.runorca.ai/v1/guardrails` carry a
 * `workspace_id` the server never authorized — the same hole this exists to
 * close, reachable by a different prefix. `/api/v1/*` needs no entry: it is
 * rewritten to `/v1/*` before any hook runs.
 */
const WORKSPACE_SCOPED_PREFIXES = ['/v1/', '/apis/'];

async function rejectExplicitWorkspaceSelector(
  req: FastifyRequest,
  reply: FastifyReply,
): Promise<void> {
  const path = req.url.split('?')[0] ?? '';
  if (!WORKSPACE_SCOPED_PREFIXES.some((prefix) => path.startsWith(prefix))) return;

  if (containsWorkspaceSelector(req.query) || containsWorkspaceSelector(req.body)) {
    reply.code(400).send({
      error: 'workspace_id is derived from authentication and must not be supplied',
    });
    return reply;
  }
}

function containsWorkspaceSelector(value: unknown): boolean {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  return (
    Object.prototype.hasOwnProperty.call(value, 'workspace_id') ||
    Object.prototype.hasOwnProperty.call(value, 'workspaceId')
  );
}
