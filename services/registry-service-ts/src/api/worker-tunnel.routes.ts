// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Public WebSocket endpoint for environment-worker tunnels.
//
// A self-hosted environment worker dials this endpoint from OUTSIDE the mesh
// (the worker runs wherever the environment lives — a laptop, a customer VM —
// and opens an outbound WSS back to the registry). It presents its Environment
// ID in the path and its Env Key in the dedicated `X-Orca-Environment-Key`
// handshake header. The registry authenticates the key against the environment
// row's stored digest+expiry, registers the live worker connection on this
// replica, CLAIMS the environment in the durable claim store (newest-wins, this
// pod + this connection id), and holds the control channel — over which the
// distribution layer later sends launch-runner frames.
//
// A registry-LAUNCHED worker (a server-managed sandbox with no operator to
// provision an Env Key ahead of time) instead presents a per-launch
// Environment Token on the dedicated `X-Orca-Environment-Token` header. When
// that header is present it MUST resolve via the injected `tokens` seam or
// the connection is refused — it never falls through to the Env-Key check
// (see `authenticateWorkerTunnel`). Absent that header, the Env-Key path
// below is unaffected. See `docs/managed-agents/services/registry-service.md`
// ("Worker auth").
//
// Exposure: the worker cannot reach `/internal/*` in production, so — like the
// sibling runner tunnel — this is mounted under the PUBLIC `/v1/tunnels/*`
// namespace at a single absolute path. The global auth pre-handler is bypassed
// for `/v1/tunnels/*` (see `auth.ts`): this handler self-authenticates the
// worker by its Env Key or Environment Token, and the CSWSH origin guard
// (mirroring the runner tunnel) refuses a browser cross-site hijack before any
// protocol I/O. This route's own auth + origin guard ARE the auth for this
// endpoint, not defense in depth on top of a mesh layer.
//
// The connection lifecycle — hello + version-skew check, the three concurrent
// loops (sender drains the outbound queue, receive routes results to pending
// waiters and records `worker.runner_exited`, ping keeps the tunnel and the
// durable claim heartbeat alive), and the disconnect cleanup with its
// "only-touch-a-worker-we-registered" guard — is owned by the reusable
// `WorkerTunnelServer` engine in `@orca/harness-tunnel`. This module is the thin
// adapter that wires that engine to Fastify/`ws`, to Env-Key auth, and to the
// durable environment-claim store.

import type { FastifyInstance, FastifyRequest } from 'fastify';
import type { WebSocket as WsWebSocket } from 'ws';
import {
  WorkerTunnelServer,
  INTERNAL_WS_ORIGIN,
  type WorkerAuthProvider,
  type WorkerHandshake,
  type WorkerSocketMessage,
  type WorkerStore,
  type WorkerWebSocket,
  type RunnerExitSink,
} from '@orca/harness-tunnel';
import { randomUUID } from 'node:crypto';
import { verifyEnvKeyForEnvironment } from '../domain/environment-key-state.js';
import { WorkerRegistry } from '../tunnel/worker-registry.js';

/**
 * Private-use WS close code (4000-4999) for a rejected `Origin`. Distinct from
 * the unauthenticated 4004 so a forbidden-origin rejection is diagnosable on its
 * own. Matches the runner tunnel's `FORBIDDEN_ORIGIN_CLOSE_CODE`.
 */
export const WORKER_FORBIDDEN_ORIGIN_CLOSE_CODE = 4403;
/**
 * WS close code used when the Env Key is missing / wrong / expired, or the
 * environment is archived / unknown. One code for every auth refusal so each is
 * diagnosable as a worker-auth failure. Mirrors the engine's
 * `UNAUTHENTICATED_CLOSE_CODE`.
 */
export const WORKER_UNAUTHENTICATED_CLOSE_CODE = 4004;
/** WS close code used when the first frame is not a `worker.hello`. */
export const WORKER_EXPECTED_HELLO_CLOSE_CODE = 4001;
/** WS close code used on a frame-protocol major mismatch. */
export const WORKER_VERSION_MISMATCH_CLOSE_CODE = 4002;
/** WS close code used when the ping watchdog trips. */
export const WORKER_PING_TIMEOUT_CLOSE_CODE = 4003;

/**
 * The dedicated handshake header carrying the worker's raw `sk-…` Env Key. The
 * key authenticates the worker against the environment row's stored digest +
 * expiry; it is never echoed and only its digest is persisted.
 */
export const ENVIRONMENT_KEY_HEADER = 'X-Orca-Environment-Key';

/**
 * The dedicated handshake header carrying a registry-launched worker's raw
 * `et-…` per-launch Environment Token — the managed-auth alternative to the
 * Env Key for a server-managed sandbox (no operator to provision an Env Key
 * ahead of time). When presented, it MUST resolve via
 * {@link EnvironmentTokenResolver.resolveEnvironmentToken} or the connection
 * is refused; it never falls through to the Env-Key path (see
 * {@link authenticateWorkerTunnel}). Distinct from the reusable
 * `@orca/harness-tunnel` engine's OWN (separate, and on this route
 * deliberately inert) `X-Orca-Host-Token` launch-token header — this is an
 * Orca-native, registry-service-level credential keyed by `environmentId`,
 * not the engine's generic worker-id-scoped one.
 */
export const ENVIRONMENT_TOKEN_HEADER = 'X-Orca-Environment-Token';

/** The env-key columns + archive flag a verify needs, plus the resolved tenant. */
export interface EnvironmentKeyRow {
  /** The environment's owning workspace, recorded as the worker connection owner. */
  workspaceId: string;
  /** Stored SHA-256 digest of the armed key, or `null` when revoked. */
  envKeyDigest: string | null;
  /** When the armed key stops authenticating, or `null` when none is armed. */
  envKeyExpiresAt: Date | null;
  /** Whether the environment row is archived (soft-deleted) — never authenticates. */
  archived: boolean;
}

/**
 * Loads an environment's env-key credential row for the worker-tunnel auth gate.
 *
 * Returns the row for `environmentId`, or `null` when no such environment
 * exists. An absent row fails closed exactly like a wrong key, so the auth path
 * never leaks which environment ids exist.
 */
export interface EnvironmentKeyLoader {
  loadEnvironmentKeyRow(environmentId: string): Promise<EnvironmentKeyRow | null>;
}

/**
 * Resolves a presented per-launch Environment Token to its owning workspace —
 * the managed-auth gate for a registry-launched worker.
 *
 * Structurally the subset `EnvironmentTokenStore`
 * (`src/domain/environment-token-store.ts`) needs; carried as a seam so the
 * route is unit-testable without a DB, exactly like {@link EnvironmentKeyLoader}.
 */
export interface EnvironmentTokenResolver {
  /**
   * Resolve `token` against `environmentId`'s stored digest + expiry.
   *
   * @returns The resolved owning workspace, or `null` when the token is
   *   unknown/wrong/expired, the environment is archived, or `environmentId`
   *   does not exist — fails closed identically to a wrong Env Key, so no
   *   existence oracle leaks.
   */
  resolveEnvironmentToken(environmentId: string, token: string): Promise<string | null>;
}

/**
 * The durable environment-claim surface the worker tunnel drives.
 *
 * Structurally the subset of `EnvironmentClaimStore` the route needs: claim on
 * connect (newest-wins), heartbeat on each live ping tick (connection-scoped),
 * and release on disconnect (connection-scoped). Carried as a seam so the route
 * is unit-testable without a DB; production passes the real store.
 */
export interface WorkerClaimStore {
  /** Claim `environmentId` for `ownerPod` on `workerConnId`, newest-wins. */
  claim(environmentId: string, ownerPod: string, workerConnId: string): Promise<unknown>;
  /**
   * Advance the claim's heartbeat watermark, connection-scoped.
   *
   * @returns `true` when a matching claim was refreshed, `false` when this
   *   connection no longer owns it (taken over by a newer claim).
   */
  heartbeat(environmentId: string, workerConnId: string): Promise<boolean>;
  /**
   * Release the claim, connection-scoped (the worker teardown path).
   *
   * @returns `true` when this connection's claim was removed, `false` otherwise.
   */
  release(environmentId: string, workerConnId: string): Promise<boolean>;
}

/** Options for {@link registerWorkerTunnelRoutes}. */
export interface WorkerTunnelRouteOptions {
  /** In-memory registry of live worker connections on this replica. */
  registry: WorkerRegistry;
  /** Loads the environment's env-key credential row for the auth gate. */
  environments: EnvironmentKeyLoader;
  /**
   * Resolves a presented per-launch Environment Token (managed auth) to its
   * owning workspace. Consulted only when the worker presents
   * {@link ENVIRONMENT_TOKEN_HEADER}; absent that header, this is never
   * called and the Env-Key path is unaffected.
   */
  tokens: EnvironmentTokenResolver;
  /** Durable claim store the tunnel claims/heartbeats/releases against. */
  claims: WorkerClaimStore;
  /**
   * Identity of this registry replica, recorded as the claim's `owner_pod` so a
   * worker's tunnel is known to terminate on this pod. Defaults to a stable
   * per-process id derived from the hostname + a random suffix.
   */
  ownerPod?: string;
  /**
   * Exit reports recorded when a `worker.runner_exited` frame arrives. When set,
   * `GET /internal/runners/:id/status` surfaces the (owner-scoped) failure cause
   * for a runner that died before/after connecting, so a waiting client fails
   * fast instead of polling to a timeout. `undefined` drops the reports.
   */
  runnerExitReports?: RunnerExitSink;
  /**
   * Whether this endpoint runs in single-user local mode for the CSWSH origin
   * policy. In local mode an `Origin` is allowed only when its hostname is a
   * loopback host (or it is the internal sentinel / explicitly allow-listed),
   * because there is no cookie/proxy auth to stop a cross-origin browser page.
   * Defaults to `false` (the deployed posture; the Env Key is the credential).
   */
  localMode?: boolean;
  /**
   * Optional explicit allow-list of additional permitted `Origin` values. Always
   * honored; in non-local mode a non-empty allow-list flips the origin policy
   * from passthrough to deny-by-default.
   */
  allowedOrigins?: ReadonlySet<string>;
  /**
   * Resolve the peer host used for the loopback origin decision. Defaults to the
   * upgrade request's TCP remote address (`req.socket.remoteAddress`).
   */
  resolvePeerHost?: (req: FastifyRequest) => string | undefined;
  /**
   * Async hook fired with `(runnerId, error)` when a `worker.runner_exited` frame
   * arrives — the server marks the runner's session(s) failed and pushes the
   * cause to the open view (the only failure signal for a runner that crashed
   * before connecting its own tunnel). Failures are swallowed + logged.
   */
  onRunnerExited?: (runnerId: string, error: string) => Promise<void>;
  /**
   * Async hook fired once the worker tunnel is established (after the loops are
   * running, so it can perform real tunnel I/O — e.g. reconnect reconciliation).
   * Receives the environment id. Bounded; failures are swallowed + logged.
   */
  onWorkerConnect?: (environmentId: string) => Promise<void>;
  /** Upper bound (ms) on awaiting {@link onWorkerConnect}. Defaults to the engine's 30s. */
  onWorkerConnectTimeoutMs?: number;
  /**
   * Async hook fired when the worker tunnel closes for any reason (after a
   * successful register). Receives the environment id. Failures are
   * swallowed + logged.
   */
  onWorkerDisconnect?: (environmentId: string) => Promise<void>;
  /** Clock source (epoch ms), for deterministic tests. Defaults to `Date.now`. */
  now?: () => number;
  /**
   * Sleep `ms`, used by the engine's ping loop. Defaults to a real `setTimeout`.
   * A test injects a controllable timer to drive the ping cadence deterministically.
   */
  sleep?: (ms: number) => Promise<void>;
}

/** Stable per-process owner-pod id, used when the caller does not configure one. */
const DEFAULT_OWNER_POD = `registry-${randomUUID()}`;

/**
 * Adapt a raw `ws` WebSocket to the engine's {@link WorkerWebSocket} surface.
 *
 * The engine pulls inbound via `receive()` (resolving the next text frame or a
 * close), writes one text frame via `sendText`, and closes best-effort. Inbound
 * `message` / `close` / `error` events are funneled into a queue the engine
 * drains in order, so no frame is missed between handshake and the first
 * `receive()`.
 */
class WsWorkerAdapter implements WorkerWebSocket {
  private readonly inbound: WorkerSocketMessage[] = [];
  private waiter: ((msg: WorkerSocketMessage) => void) | undefined;
  private closedMessage: WorkerSocketMessage | undefined;

  constructor(private readonly socket: WsWebSocket) {
    socket.on('message', (data: unknown, isBinary: boolean) => {
      if (isBinary) {
        // The tunnel protocol is text-only JSON; drop binary frames.
        return;
      }
      this.push({ type: 'text', data: typeof data === 'string' ? data : String(data) });
    });
    socket.on('close', (code: number, reason: Buffer) => {
      this.push({ type: 'close', code, reason: reason.toString() });
    });
    socket.on('error', () => {
      // Surface a socket error to the engine as a close so the receive loop ends.
      this.push({ type: 'close' });
    });
  }

  private push(msg: WorkerSocketMessage): void {
    // Once the socket has closed, every later `receive()` keeps returning the
    // sticky close message (the engine may receive after a loop already saw it).
    if (msg.type === 'close' && this.closedMessage === undefined) {
      this.closedMessage = msg;
    }
    const waiter = this.waiter;
    if (waiter !== undefined) {
      this.waiter = undefined;
      waiter(msg);
      return;
    }
    this.inbound.push(msg);
  }

  receive(): Promise<WorkerSocketMessage> {
    const queued = this.inbound.shift();
    if (queued !== undefined) {
      return Promise.resolve(queued);
    }
    if (this.closedMessage !== undefined) {
      return Promise.resolve(this.closedMessage);
    }
    return new Promise<WorkerSocketMessage>((resolve) => {
      this.waiter = resolve;
    });
  }

  sendText(data: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.socket.send(data, (err) => (err ? reject(err) : resolve()));
    });
  }

  close(code?: number, reason?: string): void {
    try {
      this.socket.close(code, reason);
    } catch {
      // best-effort close; swallow
    }
  }
}

/**
 * Register the worker-tunnel WebSocket endpoint on `app`.
 *
 *   - WS `/v1/tunnels/environments/:environmentId` — the worker's outbound
 *     tunnel.
 *
 * The route requires the `@fastify/websocket` plugin on the same instance (see
 * {@link buildApp}). Per dial it: runs the CSWSH origin guard (4403 on a
 * forbidden origin), authenticates the Env Key against the environment row
 * (4004 before any hello on a missing/wrong/expired key or archived/unknown
 * environment), then hands the accepted socket to the {@link WorkerTunnelServer}
 * engine — which registers the connection, claims the environment, runs the
 * tunnel loops, and on disconnect releases the claim and deregisters (guarded so
 * a connect that never registered cannot evict another owner's claim).
 */
export function registerWorkerTunnelRoutes(
  app: FastifyInstance,
  opts: WorkerTunnelRouteOptions,
): void {
  app.get<{ Params: { environmentId: string } }>(
    '/v1/tunnels/environments/:environmentId',
    { websocket: true },
    (socket: WsWebSocket, req) => {
      const environmentId = tunnelPathId(
        req as FastifyRequest<{ Params: { environmentId: string } }>,
        'environmentId',
      );
      if (environmentId === undefined) {
        // The path matched but the id could not be resolved (no params + an
        // unexpected URL shape). Fail closed before any protocol I/O.
        socket.close(WORKER_UNAUTHENTICATED_CLOSE_CODE, 'missing environment id');
        return;
      }
      void runWorkerTunnel(
        socket,
        req as FastifyRequest<{ Params: { environmentId: string } }>,
        environmentId,
        opts,
      );
    },
  );
}

/**
 * Resolve a tunnel path id robustly from a WS upgrade request.
 *
 * Prefers Fastify's parsed `req.params[key]`, but falls back to parsing the id
 * out of the URL path. On the WS upgrade lifecycle — where the route handler runs
 * through the global `preHandler` chain (`@fastify/websocket` v11) — `req.params`
 * can be `undefined` even though routing matched, so the parsed params are not a
 * reliable single source. The endpoint is the LAST path segment of the canonical
 * `/v1/tunnels/<kind>/:id` shape, so the trailing segment recovers the id. Query
 * strings are stripped. Returns `undefined` only when neither source yields a
 * non-empty id (the caller fails closed).
 */
function tunnelPathId(req: FastifyRequest, key: string): string | undefined {
  const params = req.params as Record<string, string | undefined> | undefined;
  const fromParams = params?.[key];
  if (typeof fromParams === 'string' && fromParams.length > 0) {
    return fromParams;
  }
  const url = req.url ?? '';
  const path = url.split('?')[0] ?? '';
  const segments = path.split('/').filter((s) => s.length > 0);
  const last = segments[segments.length - 1];
  return last !== undefined && last.length > 0 ? decodeURIComponent(last) : undefined;
}

/**
 * Drive one worker tunnel from handshake to teardown.
 *
 * Fire-and-forget from Fastify's perspective; this function owns the whole
 * lifecycle and never rejects (every failure funnels into a best-effort close +
 * the engine's guarded cleanup).
 */
async function runWorkerTunnel(
  socket: WsWebSocket,
  req: FastifyRequest<{ Params: { environmentId: string } }>,
  environmentId: string,
  opts: WorkerTunnelRouteOptions,
): Promise<void> {
  const allowedOrigins = opts.allowedOrigins ?? EMPTY_ORIGIN_SET;
  const localMode = opts.localMode ?? false;
  const resolvePeerHost = opts.resolvePeerHost ?? defaultPeerHost;

  // 1. CSWSH origin guard. Reject a forbidden browser cross-origin handshake
  //    before any protocol I/O — never exchange a hello on a hijack attempt.
  //    Same policy surface as the runner tunnel. This gate is SYNCHRONOUS (it
  //    only inspects the handshake `Origin`), so it runs before the inbound pump
  //    is attached: a rejected hijack never needs the buffered frames.
  if (!websocketOriginAllowed(originHeader(req), localMode, allowedOrigins)) {
    socket.close(WORKER_FORBIDDEN_ORIGIN_CLOSE_CODE, 'forbidden origin');
    return;
  }

  // 2. Attach the inbound pump BEFORE the first `await`. The `@fastify/websocket`
  //    socket is already accepted/open by the time this handler runs, so the
  //    worker's `worker.hello` `message` event can fire IMMEDIATELY — and the Env
  //    Key auth in step 3 is genuinely async (argon2 verify + a DB read), which
  //    is a real gap during which that hello arrives. `ws` does not buffer a
  //    message that lands with no `message` listener attached, so constructing the
  //    buffering adapter here (it registers `socket.on('message')` in its ctor)
  //    is what makes the hello survive the auth window. The engine's first
  //    `receive()` then drains it from the adapter's queue. Mirrors the runner
  //    tunnel's "attach the pump before the first await" invariant — the worker
  //    tunnel's async Env-Key auth makes getting this ordering right load-bearing.
  const adapter = new WsWorkerAdapter(socket);

  // 3. Auth. Verify the presented credential BEFORE handing the socket to the
  //    engine, so an unauthenticated worker never exchanges a hello. This is
  //    the worker-tunnel equivalent of the runner tunnel's in-handler
  //    binding-token gate. Fails closed on every non-match.
  //
  //    Two credential paths, mutually exclusive and checked in a strict
  //    fork: a presented Environment Token (the managed-auth path a
  //    registry-launched worker uses — a server-managed sandbox has no
  //    operator to provision an Env Key) MUST resolve or the connection
  //    is refused; it NEVER falls through to the Env-Key check below (a peer
  //    that chose this header has no Env Key to fall back to, and falling
  //    back would let a stray/garbage token downgrade into the Env-Key
  //    check). Absent that header, the existing Env-Key path is unchanged.
  const presentedToken = headerValue(req, ENVIRONMENT_TOKEN_HEADER);
  const presentedKey = headerValue(req, ENVIRONMENT_KEY_HEADER);
  const auth = await authenticateWorkerTunnel(opts, environmentId, presentedToken, presentedKey);
  if (auth === null) {
    socket.close(WORKER_UNAUTHENTICATED_CLOSE_CODE, 'unauthenticated');
    return;
  }

  // 4. Hand the accepted socket to the reusable engine. A fresh per-connection
  //    claim id binds this tunnel's claim/heartbeat/release together so a
  //    superseded worker's teardown cannot release the new owner's claim.
  const workerConnId = randomUUID();
  const ownerPod = opts.ownerPod ?? DEFAULT_OWNER_POD;
  const server = new WorkerTunnelServer({
    registry: opts.registry,
    // The env-key credential is the worker's identity, already verified above;
    // the claim store backs the engine's persistent "worker store" seam. The
    // resolved workspace is the tunnel owner (the tenant scope).
    store: new ClaimBackedWorkerStore(opts.claims, environmentId, ownerPod, workerConnId),
    authProvider: new ResolvedOwnerAuthProvider(auth.workspaceId),
    ...(opts.runnerExitReports !== undefined ? { runnerExitReports: opts.runnerExitReports } : {}),
    ...(opts.onRunnerExited !== undefined ? { onRunnerExited: opts.onRunnerExited } : {}),
    ...(opts.onWorkerConnect !== undefined ? { onWorkerConnect: opts.onWorkerConnect } : {}),
    ...(opts.onWorkerConnectTimeoutMs !== undefined
      ? { onWorkerConnectTimeoutMs: opts.onWorkerConnectTimeoutMs }
      : {}),
    ...(opts.onWorkerDisconnect !== undefined
      ? { onWorkerDisconnect: opts.onWorkerDisconnect }
      : {}),
    ...(opts.now !== undefined ? { now: opts.now } : {}),
    ...(opts.sleep !== undefined ? { sleep: opts.sleep } : {}),
    logger: {
      ...(req.log?.info ? { info: req.log.info.bind(req.log) } : {}),
      ...(req.log?.warn ? { warn: req.log.warn.bind(req.log) } : {}),
      ...(req.log?.error ? { error: req.log.error.bind(req.log) } : {}),
    },
  });
  await server.handle(adapter, handshakeOf(req), environmentId);
  void resolvePeerHost; // peer host is consumed only by the origin guard above.
}

/** The resolved tenant identity after a successful auth (either credential path). */
interface EnvKeyAuthResult {
  /** The environment's owning workspace, recorded as the worker connection owner. */
  workspaceId: string;
}

/**
 * Authenticate a worker-tunnel dial, forking on whether a managed Environment
 * Token was presented.
 *
 * A presented {@link ENVIRONMENT_TOKEN_HEADER} is an explicit credential
 * choice: it is resolved via {@link WorkerTunnelRouteOptions.tokens} and MUST
 * succeed, or auth fails closed here — this branch never falls through to
 * {@link authenticateEnvKey}, so a stray/garbage/mismatched token cannot
 * "downgrade" into the Env-Key check. Absent the header, behavior is
 * byte-for-byte the pre-existing Env-Key path.
 *
 * @param presentedToken The `X-Orca-Environment-Token` header value, or
 *   `undefined` when absent.
 * @param presentedKey The `X-Orca-Environment-Key` header value, or
 *   `undefined` when absent — only consulted when `presentedToken` is absent.
 */
async function authenticateWorkerTunnel(
  opts: WorkerTunnelRouteOptions,
  environmentId: string,
  presentedToken: string | undefined,
  presentedKey: string | undefined,
): Promise<EnvKeyAuthResult | null> {
  if (presentedToken !== undefined && presentedToken.length > 0) {
    const owner = await opts.tokens.resolveEnvironmentToken(environmentId, presentedToken);
    return owner === null ? null : { workspaceId: owner };
  }
  return authenticateEnvKey(opts, environmentId, presentedKey);
}

/**
 * Authenticate a presented Env Key against the environment row.
 *
 * Loads the environment's credential row and verifies the raw key against its
 * stored digest + expiry, refusing an archived environment. An absent key, an
 * absent row (unknown id), or any non-match returns `null` (fail closed) — the
 * unknown-id case is an auth failure, not a 404, so the path never leaks which
 * environment ids exist. On success returns the resolved workspace.
 */
async function authenticateEnvKey(
  opts: WorkerTunnelRouteOptions,
  environmentId: string,
  presentedKey: string | undefined,
): Promise<EnvKeyAuthResult | null> {
  if (presentedKey === undefined || presentedKey.length === 0) {
    return null;
  }
  const row = await opts.environments.loadEnvironmentKeyRow(environmentId);
  if (row === null) {
    return null;
  }
  const valid = verifyEnvKeyForEnvironment(
    presentedKey,
    { envKeyDigest: row.envKeyDigest, envKeyExpiresAt: row.envKeyExpiresAt },
    row.archived,
  );
  if (!valid) {
    return null;
  }
  return { workspaceId: row.workspaceId };
}

/**
 * Engine `WorkerStore` seam backed by the durable environment-claim store.
 *
 * In this system the worker's credential is the Env Key (verified in the route
 * before the engine runs), and the `environment_claims` table is the durable,
 * cross-replica record of which replica owns a worker's tunnel. So the persistent
 * "worker store" the engine expects is the claim store: `upsertOnConnect` CLAIMS
 * the environment (newest-wins), `setOffline` RELEASES the claim
 * (connection-scoped), and `heartbeat` advances the claim's heartbeat watermark.
 * The engine's token-resolver hook is unused — there is no separate launch-token
 * credential here, so `resolveLaunchToken` always returns `null` (fail closed).
 * Every method is bound to the one environment id + connection id this tunnel owns.
 */
class ClaimBackedWorkerStore implements WorkerStore {
  constructor(
    private readonly claims: WorkerClaimStore,
    private readonly environmentId: string,
    private readonly ownerPod: string,
    private readonly workerConnId: string,
  ) {}

  resolveLaunchToken(): null {
    // Intentional auth-model divergence from the engine's sandbox-launch-token
    // path (`X-Orca-Host-Token` -> resolve to worker id + owner). This registry
    // deployment maps the worker's identity onto the EnvironmentID + Env Key
    // instead: `authenticateEnvKey` verifies the raw Env Key against the
    // environment row's stored digest + expiry IN THE ROUTE, BEFORE the engine
    // runs, and `ResolvedOwnerAuthProvider` hands the engine the already-resolved
    // owner (the environment's workspace). So the engine's token-resolver path is
    // deliberately inert here — there is no separate launch-token credential and
    // no `X-Orca-Host-Token` header is honored by this route — and this method
    // always returns `null` (fail closed). The engine still treats a present-but-
    // null token as a refusal, so the inert path stays safe even if a peer sends
    // the launch-token header. The resolve / mismatch / unknown-fail-closed token
    // semantics live at the engine layer, not on this route.
    return null;
  }

  async upsertOnConnect(): Promise<void> {
    // Claim the environment for this pod + connection, newest-wins: a
    // reconnecting/relocated worker takes over from a replica that lagged on
    // cleanup.
    await this.claims.claim(this.environmentId, this.ownerPod, this.workerConnId);
  }

  async setOffline(): Promise<void> {
    // Release the claim, connection-scoped: a worker whose claim was already
    // taken over by a newer connection is a no-op here, so its teardown can
    // never delete the live owner's claim.
    await this.claims.release(this.environmentId, this.workerConnId);
  }

  async heartbeat(): Promise<void> {
    // Advance the claim heartbeat watermark while the tunnel is live, so the
    // reaper does not age this claim out. Connection-scoped; a stale connection's
    // heartbeat is a no-op.
    await this.claims.heartbeat(this.environmentId, this.workerConnId);
  }
}

/**
 * Engine `WorkerAuthProvider` that returns an already-resolved owner.
 *
 * The Env Key is verified in the route before the engine runs, so the owner (the
 * environment's workspace) is known. The engine's owner resolution consults this
 * provider for the handshake identity; it simply returns the already-resolved
 * workspace and never needs to re-read the handshake — the auth has already
 * succeeded — so it ignores it.
 */
class ResolvedOwnerAuthProvider implements WorkerAuthProvider {
  constructor(private readonly owner: string) {}

  getUserId(): string {
    return this.owner;
  }
}

/** Build the engine handshake seam (case-insensitive header lookup) from a request. */
function handshakeOf(req: FastifyRequest): WorkerHandshake {
  return {
    header(name: string): string | undefined {
      return headerValue(req, name);
    },
  };
}

/** Case-insensitive single-value header lookup off the upgrade request. */
function headerValue(req: FastifyRequest, name: string): string | undefined {
  const raw = req.headers[name.toLowerCase()];
  return Array.isArray(raw) ? raw[0] : raw;
}

/** Read the `Origin` header off the upgrade request, or `undefined` when absent. */
function originHeader(req: FastifyRequest): string | undefined {
  const raw = req.headers.origin;
  return Array.isArray(raw) ? raw[0] : raw;
}

/** Default peer-host source: the upgrade request's TCP remote address. */
function defaultPeerHost(req: FastifyRequest): string | undefined {
  return req.socket?.remoteAddress ?? undefined;
}

/** Shared empty origin allow-list, reused so the common case allocates nothing. */
const EMPTY_ORIGIN_SET: ReadonlySet<string> = new Set<string>();

// ── CSWSH origin policy (mirrors the runner tunnel) ─────────

/**
 * Decide whether a WebSocket handshake's `Origin` is acceptable.
 *
 * - The internal sentinel and any origin in `extraAllowed` are always allowed.
 * - A missing `Origin` is allowed: non-browser clients never send one and
 *   browsers always do, so its absence is not a browser CSWSH vector.
 * - In `localMode` an `Origin` is allowed only when its hostname is a loopback
 *   host — the CSWSH guard for an unauthenticated single-user server.
 * - In non-local modes the connection is authenticated by the Env Key, so any
 *   `Origin` is allowed unless `extraAllowed` is non-empty (then only the
 *   allow-list passes).
 */
function websocketOriginAllowed(
  origin: string | undefined,
  localMode: boolean,
  extraAllowed: ReadonlySet<string>,
): boolean {
  if (origin === INTERNAL_WS_ORIGIN) {
    return true;
  }
  if (origin !== undefined && extraAllowed.has(origin)) {
    return true;
  }
  if (origin === undefined) {
    return true;
  }
  if (localMode) {
    return originHostnameIsLoopback(origin);
  }
  // Non-local modes rely on the Env Key. Passthrough by default; if a deployment
  // configured an allow-list, anything not matched above is denied.
  return extraAllowed.size === 0;
}

/**
 * Return whether an `Origin` header points at a loopback host.
 *
 * Parses the URL and inspects its hostname; `localhost`, IPv4/IPv6 loopback, and
 * IPv4-mapped loopback all count. A missing / unparseable host does not.
 */
function originHostnameIsLoopback(origin: string): boolean {
  let host: string | null;
  try {
    host = new URL(origin).hostname;
  } catch {
    return false;
  }
  if (host === null || host === '') {
    return false;
  }
  // `URL` wraps IPv6 hosts in brackets; strip them before address parsing.
  if (host.startsWith('[') && host.endsWith(']')) {
    host = host.slice(1, -1);
  }
  return hostnameIsLoopback(host);
}

/**
 * Return whether a bare hostname / IP literal is a loopback host.
 *
 * `localhost`, the IPv4 loopback block `127.0.0.0/8`, IPv6 `::1`, and IPv4-mapped
 * loopback (`::ffff:127.0.0.1`, including the long form) all count; anything else
 * does not.
 */
function hostnameIsLoopback(host: string): boolean {
  if (host === 'localhost') {
    return true;
  }
  if (isIpv4LoopbackLiteral(host)) {
    return true;
  }
  if (host === '::1') {
    return true;
  }
  const mapped = host.toLowerCase().match(/:ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (mapped !== null && isIpv4LoopbackLiteral(mapped[1]!)) {
    return true;
  }
  return false;
}

/** Whether `host` is a dotted-quad IPv4 literal inside `127.0.0.0/8`. */
function isIpv4LoopbackLiteral(host: string): boolean {
  const parts = host.split('.');
  if (parts.length !== 4) {
    return false;
  }
  const octets = parts.map((p) => Number(p));
  if (octets.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) {
    return false;
  }
  // Reject non-canonical octets like "01" or "" that `Number` would accept.
  if (parts.some((p) => p.length === 0 || (p.length > 1 && p.startsWith('0')))) {
    return false;
  }
  return octets[0] === 127;
}
