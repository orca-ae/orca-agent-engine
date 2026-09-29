// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Server-side WebSocket endpoint for runner tunnels.
//
// Runners behind NAT dial this endpoint via an outbound WebSocket. The registry
// pushes framed HTTP requests over the tunnel; the runner's adapter dispatches
// them and frames responses back. This module is the live wiring between a
// Fastify `{ websocket: true }` route and the in-memory {@link TunnelRegistry}:
// it authenticates the dialing runner, performs the hello handshake, registers
// the session under its resolved owner, and runs three concurrent tunnel loops —
// sender, receive, and ping.
//
// Handshake protocol (an unauthenticated / cross-origin peer is refused with a
// close frame *before* any tunnel protocol I/O — the Fastify socket is already
// upgraded by the time the handler runs, so "before accept" maps to "close
// immediately, never exchange a hello"):
//
//   1. CSWSH origin guard — reject a forbidden `Origin` (browser cross-site
//      hijack) with 4403.
//   2. Token-binding correlation — when the runner presents
//      `RUNNER_TUNNEL_TOKEN_HEADER`, the path `runnerId` must equal the
//      token-bound id derived from it (or be present in the server's allow-list).
//      A loopback peer may omit the token (legacy local-runner flow) and bypasses
//      the allow-list. Mismatch / unauthorized / empty token → 4004.
//   3. Owner resolution + fail-closed — resolve the tunnel owner from the
//      handshake credentials; an unauthenticated non-loopback peer (auth enabled,
//      no identity) is refused with 4004 rather than registered owner-less.
//   4. Receive the `hello` frame; validate `frame_protocol_version` (strict-major,
//      4002 on mismatch; 4001 when the first frame is not a hello).
//   5. Register in the TunnelRegistry under the resolved owner.
//   6. Start sender / receive / ping loops; route response + ws frames into the
//      registry. On disconnect: abort in-flight, deregister, fire the hook.
//
// Auth posture: the self-hosted runner dials this tunnel from OUTSIDE the mesh
// (laptop, outbound WSS), so the endpoint is mounted under the PUBLIC path
// `/v1/tunnels/runners/:runnerId` — an external runner cannot reach `/internal/*`
// in production. The global auth pre-handler is bypassed for `/v1/tunnels/*`
// (see `auth.ts`): this handler self-authenticates the dialing runner by its
// tunnel binding token. The token-binding correlation, the owner fail-closed
// gate, and the CSWSH origin guard are all enforced in-handler — they ARE the
// auth for this endpoint, not defense in depth on top of a mesh layer.
//
// Concurrency model (single-threaded JS event loop): the three loops are plain
// async functions started concurrently and cancelled together via one shared
// `AbortController`. No cross-thread hop is needed to wake a waiter — resolving a
// promise on the one loop suffices. The `ws` socket is push-based
// (`socket.on('message')`), so the message handler feeds an {@link AsyncQueue}
// and the receive loop pulls framed text off that queue. The first loop to settle
// (or the socket closing) ends the tunnel; the `finally` aborts the rest,
// deregisters the session, drains the loops, then fires the disconnect hook.

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import type { WebSocket as WsWebSocket } from 'ws';
import {
  FrameKind,
  AsyncQueue,
  Deferred,
  decodeFrame,
  encodeFrame,
  tokenBoundRunnerId,
  RUNNER_TUNNEL_TOKEN_HEADER,
  INTERNAL_WS_ORIGIN,
  type Frame,
  type HelloFrame,
} from '@orca/harness-tunnel';
import {
  TunnelRegistry,
  type RegistrySession,
  type RegistryWebSocketLike,
} from '../tunnel/tunnel-registry.js';
import { RunnerExitReports } from '../tunnel/runner-exit-reports.js';

/** Wire-protocol major the server speaks; refuse a runner on a major mismatch. */
const SUPPORTED_FRAME_PROTOCOL_MAJOR = 1;
/** Interval between server→runner keepalive pings. */
const PING_INTERVAL_MS = 30_000;
/** Idle multiple of the ping interval after which a silent runner is declared dead. */
const PING_MISS_THRESHOLD = 3;
/**
 * Upper bound on how long the `onRunnerConnect` hook is awaited before the
 * handler gives up waiting on it and proceeds into the main loop race. Bounded so
 * a slow / hung hook can't stall WS shutdown: the sender / receive / ping loops
 * are already running by the time the hook fires (so tunnel I/O still flows), but
 * the handler must still be able to reach {@link Promise.race} over those loops to
 * notice a disconnect. A hook that overruns is left running detached and a warning
 * is logged. A 30s connect-hook bound.
 */
const ON_RUNNER_CONNECT_TIMEOUT_MS = 30_000;
/**
 * WS close code used when the path runner id does not match the token-bound id,
 * the token is missing/empty/unauthorized, or a non-loopback peer presented no
 * authenticated identity. One code is used for every token / ownership refusal so
 * each is diagnosable as a runner-binding failure.
 */
export const RUNNER_ID_MISMATCH_CLOSE_CODE = 4004;
/** WS close code used when the first frame is not a `hello`. */
const EXPECTED_HELLO_CLOSE_CODE = 4001;
/** WS close code used on a frame-protocol major mismatch. */
const VERSION_MISMATCH_CLOSE_CODE = 4002;
/** WS close code used when the ping watchdog trips. */
const PING_TIMEOUT_CLOSE_CODE = 4003;
/**
 * Private-use WS close code (4000-4999) for a rejected `Origin`. Distinct from
 * the runner-binding 4004 so a forbidden-origin rejection is diagnosable on its
 * own.
 */
export const FORBIDDEN_ORIGIN_CLOSE_CODE = 4403;

/**
 * Reserved owner id assigned to the unauthenticated loopback runner.
 *
 * A local server starts a runner that dials this endpoint over loopback with no
 * credentials. Assigning it a stable reserved identity (rather than leaving the
 * owner unset) keeps single-user ownership checks coherent: the listing filter
 * and the binding-ownership check both treat an owner-less runner as visible to
 * and bindable by every tenant, so an unauthenticated local runner is given this
 * id instead of `undefined`.
 */
export const LOCAL_TUNNEL_OWNER = 'local';

/**
 * Auth provider seam used to resolve the caller identity from a WebSocket
 * handshake.
 *
 * Reads only the handshake headers / cookies (available on the upgrade request
 * before any protocol I/O) and returns the authenticated user id, or `null` /
 * `undefined` when the peer presented no valid identity. When omitted entirely,
 * the route runs in single-user / no-auth mode: every peer resolves to the
 * reserved local owner on loopback and to no owner off-loopback.
 */
export interface TunnelAuthProvider {
  /**
   * Resolve the user id carried by a handshake, or `null` / `undefined` when the
   * peer is unauthenticated.
   *
   * @param req The incoming WebSocket upgrade request.
   */
  getUserId(req: FastifyRequest): string | null | undefined;
}

/** Options for {@link registerRunnerTunnelRoutes}. */
export interface RunnerTunnelRouteOptions {
  /** Shared registry the HTTP transport routes requests through. */
  registry: TunnelRegistry;
  /**
   * Optional set of accepted binding tokens. When set, a remote runner's tunnel
   * token must be present in this set and the runner may use a stable (not
   * token-derived) runner id. `undefined` keeps shared remote-server behavior by
   * accepting any token-bound runner id. Loopback peers bypass the allow-list
   * either way (they are trusted local connections).
   */
  allowedTunnelTokens?: ReadonlySet<string>;
  /**
   * Optional auth provider used to resolve the tunnel owner from the handshake.
   * When set, an unauthenticated non-loopback peer is refused (fail-closed) and
   * an authenticated runner registers under its owner. When omitted, the route
   * runs in single-user / no-auth mode.
   *
   * The same provider also gates the HTTP `GET /internal/runners` and
   * `GET /internal/runners/:runnerId/status` handlers: when set, an
   * unauthenticated request is rejected with 401 (so a `null` user can never
   * skip the owner-scoping below and expose every runner), listing is scoped to
   * the caller's own runners, and a runner owned by another user is hidden on
   * `/status`. When omitted, both handlers list/expose every online runner
   * (single-user / dev mode).
   */
  authProvider?: TunnelAuthProvider;
  /**
   * Exit reports recorded by the worker tunnel (`worker.runner_exited`). When set,
   * the `GET /internal/runners/:runnerId/status` handler includes the failure
   * cause for a runner that died before (or after) connecting, so a waiting
   * client fails fast instead of polling to a timeout. The lookup is owner-scoped
   * (via {@link RunnerExitReports.getVisible}), so another user's runner reveals
   * nothing. `undefined` (e.g. minimal test wiring, or a server without host
   * support) omits the field.
   */
  runnerExitReports?: RunnerExitReports;
  /**
   * Whether this endpoint runs in single-user local mode for the CSWSH origin
   * policy. In local mode an `Origin` is allowed only when its hostname is a
   * loopback host (or it is the internal sentinel / explicitly allow-listed),
   * because there is no cookie/proxy auth to stop a cross-origin browser page.
   * Defaults to `true` when no {@link authProvider} is configured (the
   * single-user posture) and `false` otherwise.
   */
  localMode?: boolean;
  /**
   * Optional explicit allow-list of additional permitted `Origin` values. Always
   * honored; in non-local mode a non-empty allow-list also flips the origin
   * policy from passthrough to deny-by-default.
   */
  allowedOrigins?: ReadonlySet<string>;
  /**
   * Resolve the peer host used for the loopback decision. Defaults to the
   * upgrade request's TCP remote address (`req.socket.remoteAddress`). Behind a
   * trusted L4 proxy that preserves the client address this stays correct;
   * deployments that terminate elsewhere can supply the trusted source.
   */
  resolvePeerHost?: (req: FastifyRequest) => string | undefined;
  /**
   * Optional async hook fired once a runner tunnel is established, after the
   * sender loop is running (so the hook can perform real tunnel I/O). Receives
   * the `runnerId` and the per-session resume cursors the runner presented in its
   * hello (`sessionId → lastConsumedEventId`); the owner pod uses the cursor for
   * the runner's bound session to serve an incremental `after={cursor}` resume
   * replay instead of a fresh full replay. The map is `{}` for a fresh runner.
   * Failures are swallowed and logged. The await is bounded by
   * {@link onRunnerConnectTimeoutMs} so a hung hook cannot stall WS teardown.
   */
  onRunnerConnect?: (
    runnerId: string,
    resumeCursors: Readonly<Record<string, string>>,
  ) => Promise<void>;
  /**
   * Upper bound (ms) on how long {@link onRunnerConnect} is awaited before the
   * handler stops waiting and proceeds into its main loop race (so a hung hook
   * can't stall WS shutdown). On timeout the hook is left running detached and a
   * warning is logged. Defaults to {@link ON_RUNNER_CONNECT_TIMEOUT_MS} (30s).
   * Primarily a tuning / test seam; production leaves it at the default.
   */
  onRunnerConnectTimeoutMs?: number;
  /**
   * Optional async hook fired when a runner tunnel closes for any reason.
   * Receives the `runnerId`. Failures are swallowed and logged.
   */
  onRunnerDisconnect?: (runnerId: string) => Promise<void>;
}

/**
 * Adapt a raw `ws` WebSocket to the registry's {@link RegistryWebSocketLike}
 * surface.
 *
 * The registry only ever calls `sendText` (to drain the outbound queue) and
 * `close` (best-effort, when retiring a session). Inbound frames are pumped
 * separately by the receive loop via `socket.on('message')`, so this adapter's
 * `receiveText` is intentionally a never-resolving stub — the registry never
 * pulls from it (the sole pull-style consumer is the route's own receive loop,
 * which reads the message queue directly).
 */
class WsAdapter implements RegistryWebSocketLike {
  constructor(private readonly socket: WsWebSocket) {}

  sendText(data: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.socket.send(data, (err) => (err ? reject(err) : resolve()));
    });
  }

  receiveText(): Promise<string> {
    // Never resolves: the registry doesn't pull inbound; the route's receive
    // loop owns the `message` event stream.
    return new Promise<string>(() => {});
  }

  close(opts?: { code?: number; reason?: string }): void {
    try {
      this.socket.close(opts?.code, opts?.reason);
    } catch {
      // best-effort close; swallow
    }
  }
}

/**
 * Return the runner id authorized by the WebSocket tunnel headers.
 *
 * - No token header: with an allow-list configured the token is required (throw);
 *   without one, the path runner id is accepted as-is (return `undefined`).
 * - Empty token: rejected (throw).
 * - Allow-list configured: the token must be in the set — then the path runner id
 *   is authorized directly (return `undefined`); otherwise rejected (throw).
 * - No allow-list: the token authorizes its derived token-bound runner id
 *   (return the derived id).
 *
 * @throws Error when the token is required-but-missing, present-but-empty, or
 *   not in the configured allow-list.
 */
function expectedRunnerIdFromHeaders(
  req: FastifyRequest,
  allowedTunnelTokens: ReadonlySet<string> | undefined,
): string | undefined {
  const raw = req.headers[RUNNER_TUNNEL_TOKEN_HEADER.toLowerCase()];
  const token = Array.isArray(raw) ? raw[0] : raw;
  if (token === undefined) {
    if (allowedTunnelTokens !== undefined) {
      throw new Error('runner tunnel token is required');
    }
    return undefined;
  }
  const stripped = token.trim();
  if (stripped.length === 0) {
    throw new Error('runner tunnel token must not be empty');
  }
  if (allowedTunnelTokens !== undefined) {
    if (!allowedTunnelTokens.has(stripped)) {
      throw new Error('runner tunnel token is not authorized');
    }
    return undefined;
  }
  return tokenBoundRunnerId(token);
}

/**
 * Return whether the dialing peer is a loopback client.
 *
 * A local server starts an unauthenticated runner that dials over loopback;
 * remote runners reach shared servers through the auth proxy and must present the
 * tunnel binding token. `localhost`, IPv4/IPv6 loopback (`127.0.0.0/8`, `::1`),
 * and IPv4-mapped loopback (`::ffff:127.0.0.1`) all count as loopback; everything
 * else (including a missing / unparseable host) does not.
 */
function isLoopbackPeer(host: string | undefined): boolean {
  if (host === undefined) {
    return false;
  }
  return hostnameIsLoopback(host);
}

/**
 * Resolve the owner identity for a runner tunnel handshake.
 *
 * Reads the authenticated identity from the handshake and applies the
 * single-user loopback remap. Returns the owner user id; the reserved local
 * identity for an unauthenticated loopback peer; or `undefined` when auth is
 * enabled and a non-loopback peer presented no identity (the caller fails closed)
 * or when no auth provider is configured (single-user mode).
 */
function resolveTunnelOwner(
  req: FastifyRequest,
  authProvider: TunnelAuthProvider | undefined,
  isLoopback: boolean,
): string | undefined {
  let owner: string | undefined;
  if (authProvider !== undefined) {
    owner = authProvider.getUserId(req) ?? undefined;
  }
  if (owner === undefined && isLoopback) {
    // Local runner: assign the single-user identity so ownership checks work
    // uniformly in single-user mode.
    owner = LOCAL_TUNNEL_OWNER;
  }
  return owner;
}

/**
 * Resolve the HTTP caller's user id, distinguishing "no auth configured" from
 * "auth configured but the caller is unauthenticated".
 *
 * Require-user posture: when an auth provider is configured an unauthenticated
 * request must be *rejected* (HTTP 401) rather than resolved to no identity — a
 * `null` user would skip the owner-scoping in the listing / status handlers and
 * expose every runner. When no provider is configured the route is in single-user
 * mode and there is no identity to scope by.
 *
 * @returns
 *   - `{ kind: 'anonymous' }` — no auth provider configured (single-user mode);
 *     the handlers list/expose every runner.
 *   - `{ kind: 'user', userId }` — an authenticated caller.
 *   - `{ kind: 'unauthorized' }` — a provider is configured but the caller
 *     presented no identity; the handler must reply 401.
 */
function requireUserId(
  req: FastifyRequest,
  authProvider: TunnelAuthProvider | undefined,
): { kind: 'anonymous' } | { kind: 'user'; userId: string } | { kind: 'unauthorized' } {
  if (authProvider === undefined) {
    return { kind: 'anonymous' };
  }
  const userId = authProvider.getUserId(req) ?? undefined;
  if (userId === undefined) {
    return { kind: 'unauthorized' };
  }
  return { kind: 'user', userId };
}

/**
 * Decide whether a WebSocket handshake's `Origin` is acceptable.
 *
 * - The internal sentinel and any origin in `extraAllowed` are always allowed.
 * - A missing `Origin` is allowed: non-browser clients never send one and
 *   browsers always do, so its absence is not a browser CSWSH vector.
 * - In `localMode` an `Origin` is allowed only when its hostname is a loopback
 *   host — the CSWSH guard for the unauthenticated single-user server.
 * - In non-local modes the connection is authenticated by cookie / proxy header,
 *   so any `Origin` is allowed unless `extraAllowed` is non-empty (then only the
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
  // Non-local modes rely on cookie / proxy auth. Passthrough by default; if a
  // deployment configured an allow-list, anything not matched above is denied.
  return extraAllowed.size === 0;
}

/** Read the `Origin` header off the upgrade request, or `undefined` when absent. */
function originHeader(req: FastifyRequest): string | undefined {
  const raw = req.headers.origin;
  return Array.isArray(raw) ? raw[0] : raw;
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
 * loopback (`::ffff:127.0.0.1`, including the `0:0:…:ffff:127.0.0.1` long form)
 * all count; anything else does not.
 */
function hostnameIsLoopback(host: string): boolean {
  if (host === 'localhost') {
    return true;
  }
  // IPv4 (and the IPv4 tail of a mapped IPv6) loopback is the entire 127.0.0.0/8
  // block.
  if (isIpv4LoopbackLiteral(host)) {
    return true;
  }
  if (host === '::1') {
    return true;
  }
  // IPv4-mapped IPv6 loopback: `::ffff:127.0.0.1` and its long forms. Match the
  // `ffff:` marker (case-insensitive) and validate the trailing dotted-quad as a
  // 127.0.0.0/8 address.
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

/** One entry in the `GET /internal/runners` listing. */
interface RunnerListEntry {
  runner_id: string;
  online: true;
  harnesses: string[];
}

/** Body of the `GET /internal/runners/:runnerId/status` response. */
interface RunnerStatusBody {
  runner_id: string;
  online: boolean;
  /** Present only for an offline runner with a visible host exit report. */
  error?: string;
}

/**
 * Register the runner-tunnel endpoints on `app`, wiring them into
 * `opts.registry`:
 *
 *   - `GET  /internal/runners` — list the caller's currently-online runners.
 *   - `GET  /internal/runners/:runnerId/status` — whether one runner is online,
 *     plus its host-reported exit cause when offline.
 *   - WS   `/v1/tunnels/runners/:runnerId` — the runner's outbound tunnel.
 *
 * The two HTTP GET handlers are mesh-internal control-plane reads (the registry /
 * harness query runner state over them) and stay under `/internal/*`. The WS
 * tunnel is the one endpoint a self-hosted runner dials from outside the mesh, so
 * it is mounted under the PUBLIC `/v1/tunnels/*` namespace (the global auth
 * pre-handler is bypassed there; the handler self-authenticates by binding
 * token — see the module header).
 *
 * The two HTTP GET handlers enforce a multi-tenant posture: an unauthenticated
 * request (when an {@link TunnelAuthProvider} is configured) is rejected with
 * 401; listing is scoped to the caller's own runners; a runner owned by another
 * user is hidden on `/status`; and the exit-report lookup is owner-scoped so it
 * reveals nothing about another tenant's runner.
 *
 * The WS route requires the `@fastify/websocket` plugin to be registered on the
 * same instance (see {@link buildApp}).
 */
export function registerRunnerTunnelRoutes(
  app: FastifyInstance,
  opts: RunnerTunnelRouteOptions,
): void {
  const { registry, authProvider, runnerExitReports } = opts;

  // GET /internal/runners — currently-online runners owned by the caller.
  //
  // When auth is active, only runners whose tunnel was established by the same
  // user are returned. Without auth, every online runner is listed (single-user
  // / dev mode). An owner-less runner (registered before owners were enforced)
  // stays visible to every caller.
  app.get('/internal/runners', async (req, reply) => {
    const caller = requireUserId(req, authProvider);
    if (caller.kind === 'unauthorized') {
      return reply.code(401).send({ error: 'unauthenticated' });
    }
    const userId = caller.kind === 'user' ? caller.userId : undefined;
    const data: RunnerListEntry[] = [];
    for (const runnerId of registry.onlineRunnerIds()) {
      const session = registry.get(runnerId);
      if (session === undefined) {
        continue;
      }
      // Scope listing to the caller's own runners.
      if (userId !== undefined && session.owner !== undefined && session.owner !== userId) {
        continue;
      }
      data.push({
        runner_id: runnerId,
        online: true,
        // `harnesses` is optional on the wire (defaults to `[]`); the decoder
        // normalizes it to an array, but keep the `?? []` so the type is exact.
        harnesses: [...(session.hello.harnesses ?? [])],
      });
    }
    return reply.send({ data });
  });

  // GET /internal/runners/:runnerId/status — whether a runner has an open tunnel.
  //
  // When auth is active, a runner owned by a different user appears offline to
  // prevent enumeration. When the runner is offline and a host exit report is
  // visible to the caller, its failure cause is surfaced so a waiting client
  // fails fast instead of polling to a timeout.
  app.get<{ Params: { runnerId: string } }>(
    '/internal/runners/:runnerId/status',
    async (req: FastifyRequest<{ Params: { runnerId: string } }>, reply: FastifyReply) => {
      const caller = requireUserId(req, authProvider);
      if (caller.kind === 'unauthorized') {
        return reply.code(401).send({ error: 'unauthenticated' });
      }
      const userId = caller.kind === 'user' ? caller.userId : undefined;
      const runnerId = req.params.runnerId;
      const session = registry.get(runnerId);
      let online = session !== undefined;
      // Hide runners owned by other users.
      if (
        online &&
        userId !== undefined &&
        session!.owner !== undefined &&
        session!.owner !== userId
      ) {
        online = false;
      }
      const result: RunnerStatusBody = { runner_id: runnerId, online };
      if (!online && runnerExitReports !== undefined) {
        // Host-daemon report that the runner process died (exit code + log
        // tail). Owner-scoped inside getVisible, so another user's runner
        // reveals nothing — same enumeration-hiding posture as above.
        const error = runnerExitReports.getVisible(runnerId, userId);
        if (error !== undefined) {
          result.error = error;
        }
      }
      return reply.send(result);
    },
  );

  app.get<{ Params: { runnerId: string } }>(
    '/v1/tunnels/runners/:runnerId',
    { websocket: true },
    (socket: WsWebSocket, req) => {
      const runnerId = tunnelPathId(
        req as FastifyRequest<{ Params: { runnerId: string } }>,
        'runnerId',
      );
      if (runnerId === undefined) {
        // The path matched but the id could not be resolved. Fail closed (the
        // binding-token gate would reject an empty id anyway).
        socket.close(RUNNER_ID_MISMATCH_CLOSE_CODE, 'missing runner id');
        return;
      }
      void runTunnel(
        socket,
        req as FastifyRequest<{ Params: { runnerId: string } }>,
        runnerId,
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
 * reliable single source. The runner id is the LAST path segment of the canonical
 * `/v1/tunnels/runners/:runnerId` shape, so the trailing segment recovers it.
 * Query strings are stripped. Returns `undefined` only when neither source yields
 * a non-empty id (the caller fails closed).
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
 * Drive one runner tunnel from handshake to teardown.
 *
 * The handler is fire-and-forget from Fastify's perspective; this function owns
 * the whole lifecycle and never rejects (all failures are funneled into a
 * best-effort close + the disconnect hook).
 */
async function runTunnel(
  socket: WsWebSocket,
  req: FastifyRequest<{ Params: { runnerId: string } }>,
  runnerId: string,
  opts: RunnerTunnelRouteOptions,
): Promise<void> {
  const { registry, authProvider } = opts;
  const allowedTunnelTokens = opts.allowedTunnelTokens;
  const allowedOrigins = opts.allowedOrigins ?? EMPTY_ORIGIN_SET;
  const localMode = opts.localMode ?? authProvider === undefined;
  const resolvePeerHost = opts.resolvePeerHost ?? defaultPeerHost;

  // 1. CSWSH origin guard. Reject a forbidden browser cross-origin handshake
  //    before any protocol I/O — never exchange a hello on a hijack attempt.
  if (!websocketOriginAllowed(originHeader(req), localMode, allowedOrigins)) {
    socket.close(FORBIDDEN_ORIGIN_CLOSE_CODE, 'forbidden origin');
    return;
  }

  const peerHost = resolvePeerHost(req);
  const isLoopback = isLoopbackPeer(peerHost);

  // 2. Token-binding correlation check. This endpoint is PUBLIC (off-mesh), so
  //    this gate is part of the auth itself — not defense in depth on top of a
  //    mesh layer. It proves the dialing peer holds a binding token bound to the
  //    path runner id (or an allow-listed token); the owner fail-closed gate in
  //    step 3 then refuses an unauthenticated non-loopback peer.
  let expectedRunnerId: string | undefined;
  try {
    expectedRunnerId = expectedRunnerIdFromHeaders(req, allowedTunnelTokens);
  } catch (exc) {
    if (!isLoopback) {
      socket.close(RUNNER_ID_MISMATCH_CLOSE_CODE, errMessage(exc));
      return;
    }
    // Loopback fallback: the token wasn't in the server's allow-list (an
    // external runner dialing a local server). Retry WITHOUT the allow-list so
    // token-bound id derivation kicks in instead of rejecting.
    try {
      expectedRunnerId = expectedRunnerIdFromHeaders(req, undefined);
    } catch (innerExc) {
      socket.close(RUNNER_ID_MISMATCH_CLOSE_CODE, errMessage(innerExc));
      return;
    }
  }
  if (expectedRunnerId === undefined && allowedTunnelTokens === undefined && !isLoopback) {
    socket.close(RUNNER_ID_MISMATCH_CLOSE_CODE, 'runner tunnel token is required');
    return;
  }
  if (expectedRunnerId !== undefined && runnerId !== expectedRunnerId) {
    socket.close(RUNNER_ID_MISMATCH_CLOSE_CODE, 'runner_id does not match tunnel token');
    return;
  }

  // 3. Resolve the tunnel owner from the handshake and fail closed for an
  //    unauthenticated non-loopback peer before any protocol I/O. The
  //    token-binding gate above only proves the peer knows *a* token; in
  //    no-allowlist mode (the standard deployed posture) any attacker-chosen
  //    non-empty token derives a valid runner id and clears that gate without
  //    establishing a user identity. Registering with no owner would bypass both
  //    owner-scoped guards (the listing filter and the binding-ownership check
  //    each skip enforcement when the owner is unset), so an owner-less runner
  //    would become visible to — and bindable by — every other tenant.
  const tunnelOwner = resolveTunnelOwner(req, authProvider, isLoopback);
  if (tunnelOwner === undefined && authProvider !== undefined) {
    socket.close(RUNNER_ID_MISMATCH_CLOSE_CODE, 'unauthenticated');
    return;
  }

  // The `@fastify/websocket` socket is already accepted/open by the time the
  // handler runs, so inbound `message` events can fire immediately. Attach the
  // pump before the first `await` so the hello frame is never missed.
  const inbound = new AsyncQueue<string | null>();
  const closeNotice = new Deferred<void>();
  socket.on('message', (data: unknown, isBinary: boolean) => {
    if (isBinary) {
      // The tunnel protocol is text-only JSON; drop binary frames.
      return;
    }
    inbound.put(typeof data === 'string' ? data : String(data));
  });
  socket.on('close', () => {
    // Unblock a receive loop parked on the inbound queue and end the race.
    inbound.put(null);
    if (!closeNotice.done) {
      closeNotice.resolve();
    }
  });
  socket.on('error', () => {
    inbound.put(null);
    if (!closeNotice.done) {
      closeNotice.resolve();
    }
  });

  let session: RegistrySession | undefined;

  // Exactly-once disconnect dispatch. A naive teardown that fired the disconnect
  // hook from both the inner `finally` and the outer exception handler would
  // double-fire on the exception-disconnect path: the inner `finally` fires once,
  // then a re-raised disconnect error propagates to the outer handler, which
  // fires it again. That double-fire is a hazard — a consumer that decrements a
  // gauge or emits a `runner_offline` event on each call would do so twice for one
  // close. We fire it exactly once on every path instead (the contract the route's
  // own callers, e.g. the sessions module, rely on). This guard makes that
  // structural: the inner teardown and the pre-loop `catch` both route through it,
  // so even if a loop or `closeNotice` were to reject after `Promise.race` the
  // hook still fires once, not twice.
  let disconnectFired = false;
  const fireDisconnectOnce = async (): Promise<void> => {
    if (disconnectFired) {
      return;
    }
    disconnectFired = true;
    await fireDisconnect(req, opts, runnerId);
  };

  try {
    // 4. Receive the hello frame.
    const helloRaw = await inbound.get();
    if (helloRaw === null) {
      return; // socket closed before hello
    }
    let helloFrame: Frame;
    try {
      helloFrame = decodeFrame(helloRaw);
    } catch {
      socket.close(EXPECTED_HELLO_CLOSE_CODE, 'expected hello frame');
      return;
    }
    if (helloFrame.kind !== FrameKind.Hello) {
      socket.close(EXPECTED_HELLO_CLOSE_CODE, 'expected hello frame');
      return;
    }
    const hello: HelloFrame = helloFrame;

    // 5. Version-skew check (strict-major).
    if (hello.frameProtocolVersion !== SUPPORTED_FRAME_PROTOCOL_MAJOR) {
      socket.close(
        VERSION_MISMATCH_CLOSE_CODE,
        `frame_protocol_version mismatch: server supports ${SUPPORTED_FRAME_PROTOCOL_MAJOR}, ` +
          `runner sent ${hello.frameProtocolVersion}`,
      );
      return;
    }

    // 6. Register the session under the resolved owner (newest-wins replacement
    //    handled by the registry). An unauthenticated non-loopback peer was
    //    already rejected above, so owner-binding checks can enforce ownership.
    //    Omit `owner` entirely (rather than passing `undefined`) when there is
    //    no resolved owner: under `exactOptionalPropertyTypes` an explicit
    //    `undefined` is not assignable to the `{ owner?: string }` option, and a
    //    missing key resolves to the same single-user `owner = undefined`.
    session = registry.register(runnerId, new WsAdapter(socket), hello, {
      ...(tunnelOwner !== undefined ? { owner: tunnelOwner } : {}),
    });

    // 7. Start the tunnel loops. The sender loop is the only writer to the
    //    socket; request-side callers enqueue through the registry. One shared
    //    AbortController stops every loop on the first finisher.
    const ac = new AbortController();
    const loops = [
      senderLoop(socket, session, ac.signal),
      receiveLoop(inbound, session, runnerId, registry, ac.signal),
      pingLoop(socket, session, registry, ac.signal),
    ];

    // Fire the connect hook after the sender loop is live so a hook that issues
    // a tunneled request can be answered (its response future is drained by the
    // sender + receive loops already running). Bound the await with a timeout so
    // a slow / hung hook can't stall WS shutdown: the loops are already running,
    // but the handler must still reach the `Promise.race` below to observe a
    // disconnect. On timeout the hook is left running detached and a warning is
    // logged (a bounded connect-hook wait + timeout warning).
    if (opts.onRunnerConnect !== undefined) {
      const connectTimeoutMs = opts.onRunnerConnectTimeoutMs ?? ON_RUNNER_CONNECT_TIMEOUT_MS;
      // The runner presents its per-session last-consumed cursors in the hello so
      // the owner pod can serve an incremental `after={cursor}` resume replay for
      // the bound session instead of a fresh full replay (empty map ⇒ fresh).
      const resumeCursors = hello.resumeCursors ?? {};
      try {
        await runWithTimeout(opts.onRunnerConnect(runnerId, resumeCursors), connectTimeoutMs);
      } catch (exc) {
        if (exc instanceof TimeoutError) {
          req.log?.warn?.(
            { runnerId, timeoutMs: connectTimeoutMs },
            'onRunnerConnect callback timed out',
          );
        } else {
          req.log?.error?.({ err: exc, runnerId }, 'onRunnerConnect callback failed');
        }
      }
    }

    try {
      // First loop to settle (or the socket closing) ends the tunnel.
      await Promise.race([...loops, closeNotice.promise]);
    } finally {
      ac.abort();
      // Deregister FIRST: this pushes the stop sentinel onto the session's
      // outbound queue (via the registry's writer retirement), which is what
      // unblocks a sender loop parked on `outboundQueue.get()`. Only then can
      // every loop settle — draining before deregistering would deadlock on the
      // sender. The drained results are intentionally ignored; the await just
      // keeps a late loop rejection from surfacing as an unhandled rejection.
      registry.deregister(runnerId, session);
      await Promise.allSettled(loops);
      await fireDisconnectOnce();
    }
  } catch (exc) {
    // Normally reached only on a pre-loop failure (hello read / register) — once
    // the loops are running their own try/finally owns teardown and has already
    // fired the disconnect hook. The once-guard keeps a post-loop rejection
    // (should one ever surface) from double-firing here. Deregister with the
    // generation guard when we have a session, unconditionally otherwise.
    req.log?.error?.({ err: exc, runnerId }, 'runner tunnel error');
    if (session !== undefined) {
      registry.deregister(runnerId, session);
    } else {
      registry.deregister(runnerId);
    }
    try {
      socket.close();
    } catch {
      // best-effort
    }
    await fireDisconnectOnce();
  }
}

/** Drain the session outbound queue to the socket until the stop sentinel. */
async function senderLoop(
  socket: WsWebSocket,
  session: RegistrySession,
  signal: AbortSignal,
): Promise<void> {
  // A never-resolving `outboundQueue.get()` is normally unblocked by the stop
  // sentinel the registry pushes on `deregister`, but race it against the abort
  // signal too so teardown can never deadlock on a sender with nothing queued.
  const aborted = abortPromise(signal);
  while (!signal.aborted) {
    const data = await Promise.race([session.outboundQueue.get(), aborted]);
    if (data === null || data === ABORTED || signal.aborted) {
      return; // stop sentinel, or teardown began
    }
    await new Promise<void>((resolve, reject) => {
      socket.send(data, (err) => (err ? reject(err) : resolve()));
    });
  }
}

/** Sentinel resolved value used to break out of a queue wait on abort. */
const ABORTED = Symbol('aborted');

/** Resolve with {@link ABORTED} once `signal` aborts (or immediately if it has). */
function abortPromise(signal: AbortSignal): Promise<typeof ABORTED> {
  return new Promise<typeof ABORTED>((resolve) => {
    if (signal.aborted) {
      resolve(ABORTED);
      return;
    }
    signal.addEventListener('abort', () => resolve(ABORTED), { once: true });
  });
}

/** Receive runner frames off the inbound queue and route them into the registry. */
async function receiveLoop(
  inbound: AsyncQueue<string | null>,
  session: RegistrySession,
  runnerId: string,
  registry: TunnelRegistry,
  signal: AbortSignal,
): Promise<void> {
  const aborted = abortPromise(signal);
  while (!signal.aborted) {
    const raw = await Promise.race([inbound.get(), aborted]);
    if (raw === ABORTED || signal.aborted) {
      return; // teardown began
    }
    // A frame arrived (or the socket closed): refresh liveness on every wake,
    // and bail when this session generation is no longer current.
    if (!registry.markFrameSeen(session)) {
      return;
    }
    if (raw === null) {
      return; // socket closed
    }
    let frame: Frame;
    try {
      frame = decodeFrame(raw);
    } catch {
      // Malformed frame (bad JSON / unknown kind / missing field): drop + keep.
      continue;
    }
    if (frame.kind === FrameKind.Pong) {
      continue;
    }
    if (frame.kind === FrameKind.WsFrame || frame.kind === FrameKind.WsClose) {
      registry.routeWsInbound(runnerId, frame, session);
      continue;
    }
    registry.routeResponseFrame(runnerId, frame, session);
  }
}

/** Send pings on an interval; declare the runner dead after enough silence. */
async function pingLoop(
  socket: WsWebSocket,
  session: RegistrySession,
  registry: TunnelRegistry,
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted) {
    await abortableDelay(PING_INTERVAL_MS, signal);
    if (signal.aborted) {
      return;
    }
    const elapsed = registry.secondsSinceLastFrame(session);
    if (elapsed === null) {
      return; // session went stale
    }
    if (elapsed > (PING_INTERVAL_MS / 1000) * PING_MISS_THRESHOLD) {
      try {
        socket.close(PING_TIMEOUT_CLOSE_CODE, 'ping timeout');
      } catch {
        // already closed
      }
      return;
    }
    try {
      await registry.sendText(session, encodeFrame({ kind: FrameKind.Ping, ts: Date.now() }));
    } catch {
      return; // any send failure ends the ping loop cleanly
    }
  }
}

/** Fire the disconnect hook, swallowing + logging any failure. */
async function fireDisconnect(
  req: FastifyRequest,
  opts: RunnerTunnelRouteOptions,
  runnerId: string,
): Promise<void> {
  if (opts.onRunnerDisconnect === undefined) {
    return;
  }
  try {
    await opts.onRunnerDisconnect(runnerId);
  } catch (exc) {
    req.log?.error?.({ err: exc, runnerId }, 'onRunnerDisconnect callback failed');
  }
}

/**
 * Error raised by {@link runWithTimeout} when the wrapped promise does not settle
 * within the deadline. A dedicated class (rather than a bare `Error`) so the
 * connect-hook handler can distinguish a timeout — which is logged as a warning —
 * from a genuine hook failure that is logged as an error. The wrapped promise is
 * NOT cancelled (JS promises are not abortable); it is left running detached, the
 * same way a bounded-wait timeout leaves behind a task that ignores its
 * cancellation.
 */
class TimeoutError extends Error {
  constructor(message = 'operation timed out') {
    super(message);
    this.name = 'TimeoutError';
  }
}

/**
 * Await `promise`, rejecting with a {@link TimeoutError} if it does not settle
 * within `ms`. When `promise` settles first the timer is cleared so no dangling
 * handle keeps the event loop alive; when the timeout wins `promise` keeps running
 * detached (its later settlement is intentionally ignored). The timeout branch
 * also swallows a late rejection from `promise` so it cannot surface as an
 * unhandled rejection after the deadline already fired.
 */
function runWithTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      // The hook overran: surface a timeout to the caller and make sure the
      // still-pending promise's eventual rejection doesn't become unhandled.
      promise.then(undefined, () => {});
      reject(new TimeoutError());
    }, ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err) => {
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}

/** Resolve after `ms`, or immediately once `signal` aborts. */
function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      resolve();
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

/** Default peer-host source: the upgrade request's TCP remote address. */
function defaultPeerHost(req: FastifyRequest): string | undefined {
  return req.socket?.remoteAddress ?? undefined;
}

/** Shared empty origin allow-list, reused so the common case allocates nothing. */
const EMPTY_ORIGIN_SET: ReadonlySet<string> = new Set<string>();

function errMessage(exc: unknown): string {
  return exc instanceof Error ? exc.message : String(exc);
}
