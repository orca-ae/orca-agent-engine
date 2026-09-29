// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Server-side worker tunnel engine — drives one worker's outbound WebSocket from
// handshake to teardown.
//
// Hosts (machines or server-managed sandboxes running the worker) dial this
// engine over an outbound WebSocket. The server pushes control + filesystem-op
// frames (launch/stop a runner, stat / list-dir / worktree / create-dir) over
// the tunnel; the worker spawns or terminates runner subprocesses and answers the
// filesystem ops. The worker opens with a `worker.hello` frame advertising its
// version, name, live runner IDs, and per-harness readiness; the engine
// validates `frame_protocol_version` (strict-major) for version-skew
// enforcement.
//
// This module owns only the read/write framing + lifecycle over the socket: the
// handshake auth gate, the hello + version check, the three concurrent loops
// (sender drains the outbound queue, receive routes results to pending request
// waiters and records `worker.runner_exited`, ping keeps the tunnel and the
// persistent heartbeat alive), and the disconnect cleanup with its
// "only-touch-a-worker-we-registered" guard. Everything else — the live-worker
// registry, the persistent worker store, the auth provider, the runner-exit sink —
// is an injectable seam (interfaces below), exactly as `transport.ts` keeps its
// `TransportRegistry` / `WebSocketLike` collaborators network-free so the engine
// can be unit-tested in full.
//
// Wiring posture: this engine is transport-agnostic, but in this system its one
// adapter (the registry-service worker-tunnel route a follow-up PR adds) mounts
// it on the PUBLIC `/v1/tunnels/environments/:environmentId` path — an Orca
// extension route recorded in `docs/managed-agents/orca-extensions.md` ("Route
// boundaries") — NOT under `/internal/*`.
// A self-hosted environment worker dials in from OUTSIDE the mesh (a laptop, a
// customer VM) and so cannot reach `/internal/*`; like the sibling runner tunnel,
// the endpoint sits in the public `/v1/tunnels/*` namespace and the global auth
// pre-handler bypasses `/v1/tunnels/*` (see `auth.ts`). The handshake auth gate
// here (sandbox-launch-token resolve, or the auth-provider identity, fail-closed)
// is therefore the PRIMARY credential check for this public endpoint — not
// defense-in-depth on top of a mesh — exactly as the runner tunnel's in-handler
// token-binding + owner gate is. In the registry-service adapter the worker's
// credential is its Env Key, verified against the environment row before the
// engine runs; the adapter then maps the engine's persistent "worker store" seam
// onto the registry's durable environment-claims machinery (documented with the
// follow-up wiring PR that adds the adapter). The in-memory `WorkerRegistry`
// seam below is the per-replica live-connection view.

import {
  WorkerFrameKind,
  decodeWorkerFrame,
  type WorkerFrame,
  type WorkerHelloFrame,
  type WorkerLaunchRunnerResultFrame,
  type WorkerStopRunnerResultFrame,
  type WorkerRunnerExitedFrame,
  type WorkerStatResultFrame,
  type WorkerListDirResultFrame,
  type WorkerCreateWorktreeResultFrame,
  type WorkerRemoveWorktreeResultFrame,
  type WorkerCreateDirResultFrame,
} from './worker-frames.js';
import { FrameKind, decodeFrame, encodeFrame } from './frames.js';
import { HOST_TUNNEL_TOKEN_HEADER } from './identity.js';
import { AsyncQueue } from './transport.js';

/** Wire-protocol major the server speaks; refuse a worker on a major mismatch. */
export const SUPPORTED_FRAME_PROTOCOL_MAJOR = 1;
/** Interval between server→worker keepalive pings, in milliseconds. */
export const PING_INTERVAL_MS = 30_000;
/** Idle multiple of the ping interval after which a silent worker is declared dead. */
export const PING_MISS_THRESHOLD = 3;
/**
 * Upper bound (ms) on how long the {@link WorkerTunnelOptions.onWorkerConnect} hook
 * is awaited before the engine stops waiting and proceeds into its main loop
 * race, so a slow / hung hook cannot stall tunnel teardown. The loops are
 * already running by the time the hook fires (so tunnel I/O still flows); the
 * engine must still reach the loop race to notice a disconnect. On timeout the
 * hook is left running detached and a warning is logged. A 30-second bounded
 * wait on the connect callback.
 */
export const ON_WORKER_CONNECT_TIMEOUT_MS = 30_000;
/**
 * Upper bound (ms) on awaiting the {@link WorkerTunnelOptions.onRunnerExited}
 * hook. Unlike the connect hook this await happens inside the receive loop, so
 * the bound also protects frame processing (and the liveness timestamp it
 * refreshes) from a hung hook. On timeout the hook is left running detached.
 */
export const ON_RUNNER_EXITED_TIMEOUT_MS = 30_000;

/**
 * Reserved owner id assigned to an unauthenticated local worker.
 *
 * When no auth provider is configured the engine runs in single-user / local
 * mode and accepts this stable reserved identity as the worker owner (consistent
 * with `getUserId` returning `null` on the HTTP side), so the in-memory
 * ownership checks stay coherent.
 */
export const LOCAL_TUNNEL_OWNER = 'local';

/** WS close code used when the first frame is not a `worker.hello`. */
export const EXPECTED_HELLO_CLOSE_CODE = 4001;
/** WS close code used on a frame-protocol major mismatch. */
export const VERSION_MISMATCH_CLOSE_CODE = 4002;
/** WS close code used when the ping watchdog trips. */
export const PING_TIMEOUT_CLOSE_CODE = 4003;
/**
 * WS close code used when the sandbox launch token does not resolve (or is
 * scoped to another worker id), or when an auth provider is configured but the
 * peer presented no identity. One code for every auth refusal so each is
 * diagnosable as a worker-auth failure.
 */
export const UNAUTHENTICATED_CLOSE_CODE = 4004;

// ── WebSocket seam ───────────────────────────────────────
//
// The minimal accepted-WebSocket surface the engine drives. It is intentionally
// network-free: a real deployment adapts `ws` / `@fastify/websocket` to this
// interface (as the follow-up registry route adapters will for both tunnels),
// and tests drive a fake. The engine never imports a socket library directly.

/** A received WebSocket message: a text frame, or the socket closing. */
export type WorkerSocketMessage =
  | { readonly type: 'text'; readonly data: string }
  | { readonly type: 'close'; readonly code?: number; readonly reason?: string };

/**
 * Minimal accepted-WebSocket surface the worker tunnel drives.
 *
 * `receive` resolves the next inbound message (a text frame or a close); the
 * receive loop treats a `close` message as end-of-tunnel. `sendText` writes one
 * text frame. `close` is best-effort (idempotent; a double close is swallowed by
 * the adapter).
 */
export interface WorkerWebSocket {
  /** Resolve the next inbound message (text frame or socket close). */
  receive(): Promise<WorkerSocketMessage>;
  /** Write one outbound text frame. */
  sendText(data: string): Promise<void>;
  /** Best-effort close with an optional WS close code + reason. */
  close(code?: number, reason?: string): void;
}

// ── Auth seam ────────────────────────────────────────────

/** The handshake surface the auth gate reads: just the upgrade headers. */
export interface WorkerHandshake {
  /**
   * Case-insensitive lookup of a single handshake header value, or `undefined`
   * when absent. (HTTP header names are case-insensitive; the adapter is
   * responsible for honoring that.)
   */
  header(name: string): string | undefined;
}

/**
 * Resolve the authenticated user identity carried by a worker handshake.
 *
 * Reads only the handshake headers / cookies. Returns the owner user id, or
 * `null` / `undefined` when the peer presented no valid identity. When the
 * engine is constructed with no provider it runs in single-user / local mode and
 * accepts {@link LOCAL_TUNNEL_OWNER} as the owner.
 */
export interface WorkerAuthProvider {
  getUserId(handshake: WorkerHandshake): string | null | undefined;
}

/** A resolved sandbox-worker launch token: the worker id it is scoped to + its owner. */
export interface ResolvedLaunchToken {
  /** The single worker id this token authorizes. Presenting it for any other path fails closed. */
  workerId: string;
  /** Owner recorded for the worker (a disposable sandbox has no user credential of its own). */
  owner: string | undefined;
}

// ── Persistent worker store seam ───────────────────────────

/** Arguments to {@link WorkerStore.upsertOnConnect}. */
export interface WorkerUpsertOnConnect {
  workerId: string;
  name: string;
  owner: string | undefined;
  /**
   * Allow a worker to re-own a `workerId` already registered under a different
   * owner. Needed only for the single-user local server, where the owner
   * legitimately changes across an auth-mode flip; the deployed multi-user
   * server leaves this `false` to keep the worker-hijack boundary.
   */
  allowWorkerIdReown: boolean;
  /** Per-harness readiness reported in the hello, or `null` when unknown. */
  configuredHarnesses: Record<string, boolean> | null;
}

/**
 * Persistent, cross-replica store of worker registrations.
 *
 * Also the credential source for server-provisioned sandbox hosts: a connection
 * presenting the worker-tunnel token header authenticates via
 * {@link resolveLaunchToken} instead of the auth provider (sandboxes have no
 * user credentials). A real implementation is Postgres-backed; the engine only
 * needs these four operations, so it depends on the seam (the same way
 * `transport.ts` depends on `TransportRegistry`, not a concrete registry).
 */
export interface WorkerStore {
  /**
   * Resolve a sandbox-worker launch token to its worker id + owner, or `null` when
   * the token is unknown / expired / revoked. May be async (a DB round-trip).
   */
  resolveLaunchToken(
    token: string,
  ): ResolvedLaunchToken | null | Promise<ResolvedLaunchToken | null>;
  /**
   * Upsert the worker row on connect (mark online, record name / owner / readiness).
   *
   * @throws when the peer's `owner` does not match an existing row's owner and
   *   `allowWorkerIdReown` is `false` — the cross-user worker-hijack boundary. The
   *   engine treats a throw here as "never registered" so disconnect cleanup
   *   does not flip another owner's worker offline.
   */
  upsertOnConnect(args: WorkerUpsertOnConnect): void | Promise<void>;
  /** Mark the worker offline (graceful disconnect or teardown). */
  setOffline(workerId: string): void | Promise<void>;
  /** Refresh the worker's last-seen timestamp (liveness freshness gate). */
  heartbeat(workerId: string): void | Promise<void>;
}

// ── Live worker registry + connection seam ─────────────────

/**
 * One in-flight request awaiting a result frame.
 *
 * The receive loop pops the waiter for a result's `request_id` and resolves it
 * with the decoded result payload. `resolve` is one-shot (a duplicate / late
 * result is dropped). The payload type is the matching `*ResultFrame` (minus the
 * `kind` discriminator is irrelevant to callers; the whole frame is handed back).
 *
 * `reject` is the tunnel-died path: teardown rejects every still-pending waiter
 * with a {@link WorkerTunnelClosedError} so a caller awaiting a result never
 * hangs on a worker that disconnected before answering. Like `resolve`, it is
 * one-shot; a settled waiter ignores later calls.
 */
export interface WorkerPendingRequest<T> {
  resolve(result: T): void;
  reject(reason: unknown): void;
}

/**
 * Rejection reason handed to every pending waiter when its tunnel tears down
 * before the worker answered. A dedicated class so callers can distinguish
 * "worker disconnected mid-request" (retryable against a reconnected worker)
 * from a decode or protocol failure.
 */
export class WorkerTunnelClosedError extends Error {
  constructor(message = 'worker tunnel closed before the worker answered') {
    super(message);
    this.name = 'WorkerTunnelClosedError';
  }
}

/**
 * `Omit` distributed over a union: plain `Omit<A | B, K>` collapses the union
 * into one merged shape and loses the discriminant correlation; this keeps each
 * arm intact.
 */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

/** Result payload handed to a launch waiter (keeps the status-arm correlation). */
export type WorkerLaunchResult = DistributiveOmit<WorkerLaunchRunnerResultFrame, 'kind'>;
/** Result payload handed to a stop waiter. */
export type WorkerStopResult = Omit<WorkerStopRunnerResultFrame, 'kind'>;
/** Result payload handed to a stat waiter. */
export type WorkerStatResult = Omit<WorkerStatResultFrame, 'kind'>;
/** Result payload handed to a list-dir waiter. */
export type WorkerListDirResult = Omit<WorkerListDirResultFrame, 'kind'>;
/** Result payload handed to a create-worktree waiter. */
export type WorkerCreateWorktreeResult = Omit<WorkerCreateWorktreeResultFrame, 'kind'>;
/** Result payload handed to a remove-worktree waiter. */
export type WorkerRemoveWorktreeResult = Omit<WorkerRemoveWorktreeResultFrame, 'kind'>;
/** Result payload handed to a create-dir waiter. */
export type WorkerCreateDirResult = Omit<WorkerCreateDirResultFrame, 'kind'>;

/**
 * A live worker connection on this replica.
 *
 * Owns the outbound frame queue (the sender loop's single source), the
 * per-`request_id` pending-waiter maps the receive loop resolves into, the
 * resolved owner (used to scope a `worker.runner_exited` report), and the
 * last-frame timestamp the ping watchdog reads. The engine drains
 * `outboundQueue` to the socket; a server-side caller enqueues a request frame
 * and registers the matching waiter.
 */
export interface WorkerConnection {
  /** Resolved owner of this tunnel (or `undefined` in single-user no-auth mode). */
  readonly owner: string | undefined;
  /**
   * Outbound frame queue drained by the sender loop. A `null` is the stop
   * sentinel pushed on deregister to unblock a parked sender.
   */
  readonly outboundQueue: AsyncQueue<string | null>;
  /** Epoch-ms of the last frame received; refreshed on every inbound message. */
  lastFrameAt: number;
  /** Pending launch requests keyed by `request_id`. */
  readonly pendingLaunches: Map<string, WorkerPendingRequest<WorkerLaunchResult>>;
  /** Pending stop requests keyed by `request_id`. */
  readonly pendingStops: Map<string, WorkerPendingRequest<WorkerStopResult>>;
  /** Pending stat requests keyed by `request_id`. */
  readonly pendingStats: Map<string, WorkerPendingRequest<WorkerStatResult>>;
  /** Pending list-dir requests keyed by `request_id`. */
  readonly pendingListDirs: Map<string, WorkerPendingRequest<WorkerListDirResult>>;
  /** Pending create-worktree requests keyed by `request_id`. */
  readonly pendingCreateWorktrees: Map<string, WorkerPendingRequest<WorkerCreateWorktreeResult>>;
  /** Pending remove-worktree requests keyed by `request_id`. */
  readonly pendingRemoveWorktrees: Map<string, WorkerPendingRequest<WorkerRemoveWorktreeResult>>;
  /** Pending create-dir requests keyed by `request_id`. */
  readonly pendingCreateDirs: Map<string, WorkerPendingRequest<WorkerCreateDirResult>>;
}

/**
 * In-memory, per-replica registry of live worker connections.
 *
 * `register` records the live connection (newest-wins replacement is the
 * registry's call); `deregister` retires it and pushes the sender's stop
 * sentinel. The engine depends only on these two operations.
 */
export interface WorkerRegistry {
  /**
   * Register a freshly-connected worker and return its {@link WorkerConnection}.
   *
   * @param workerId The worker id from the tunnel path.
   * @param ws The accepted WebSocket (so the registry can retire a superseded
   *   connection's socket on newest-wins replacement).
   * @param hello The validated hello frame (version / name / runners / readiness).
   * @param opts.owner The resolved tunnel owner, omitted in single-user no-auth mode.
   */
  register(
    workerId: string,
    ws: WorkerWebSocket,
    hello: WorkerHelloFrame,
    opts: { owner?: string },
  ): WorkerConnection;
  /**
   * Return the CURRENT live connection for `workerId`, or `undefined`.
   *
   * The engine's teardown identity check: a stale connection handle (superseded
   * by a newest-wins {@link register} replacement) compares the registry's
   * current entry against its own connection and, when they differ, skips
   * deregister / set-offline / disconnect-hook entirely — otherwise a zombie
   * socket's late death would retire and offline the freshly-connected worker.
   */
  get(workerId: string): WorkerConnection | undefined;
  /**
   * Retire the live connection for `workerId` and unblock its sender loop.
   *
   * The engine only calls this while its own connection is still the registry's
   * current entry (checked via {@link get} in the same synchronous step), so a
   * keyed removal cannot clobber a replacement connection.
   */
  deregister(workerId: string): void;
}

/**
 * Authenticated provenance attached to every `worker.runner_exited` delivery.
 *
 * `frame.runnerId` is ENTIRELY worker-supplied: any authenticated worker can
 * put any string — including another tenant's runner id — on the wire. The
 * engine therefore hands consumers the identity it actually authenticated
 * (the tunnel's `workerId` and resolved `owner`) alongside the claim, and
 * consumers MUST verify the runner is genuinely assigned to that worker/owner
 * before recording the cause or marking sessions failed. The engine cannot do
 * that check itself — runner assignments live in the registry's durable state,
 * behind these seams.
 */
export interface WorkerRunnerExitContext {
  /** The workerId this tunnel authenticated as (from the tunnel path + auth gate). */
  readonly workerId: string;
  /** The tunnel's resolved owner, or `undefined` in single-user no-auth mode. */
  readonly owner: string | undefined;
}

/**
 * Sink for `worker.runner_exited` reports.
 *
 * Structurally the registry-service `RunnerExitReports.record` surface: the
 * receive loop records the failure cause so the runner-status endpoint can
 * answer "offline, and here is why" to a client still waiting for the runner
 * to connect. `undefined` (minimal wiring) drops the reports. The sink MUST
 * verify `runnerId` against `ctx` (see {@link WorkerRunnerExitContext}) before
 * trusting the report.
 */
export interface RunnerExitSink {
  record(runnerId: string, error: string, ctx: WorkerRunnerExitContext): void;
}

/** Structured logger seam (a subset of the usual `req.log`). All optional. */
export interface WorkerTunnelLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/** Options for {@link WorkerTunnelServer}. */
export interface WorkerTunnelOptions {
  /** In-memory registry of live worker connections on this replica. */
  registry: WorkerRegistry;
  /** Persistent worker store (upsert / offline / heartbeat) + launch-token resolver. */
  store: WorkerStore;
  /**
   * Optional auth provider for user identity. When set, an authenticated user is
   * recorded as the worker owner and an unauthenticated non-managed peer is refused
   * (fail-closed). When omitted, the engine runs in single-user / local mode and
   * accepts {@link LOCAL_TUNNEL_OWNER} as the owner.
   */
  authProvider?: WorkerAuthProvider;
  /**
   * When `true`, allow a worker to re-own a `workerId` already registered under a
   * different owner (single-user local server only). Defaults to `false` — the
   * deployed multi-user posture that keeps the worker-hijack boundary.
   */
  allowWorkerIdReown?: boolean;
  /** Shared sink for `worker.runner_exited` reports. `undefined` drops them. */
  runnerExitReports?: RunnerExitSink;
  /**
   * Async hook fired with `(runnerId, error, ctx)` when a `worker.runner_exited`
   * frame arrives. The server wires this to mark the runner's session(s) failed
   * and push the cause to the open view — the only failure signal for a runner
   * that crashed before connecting its own tunnel. Failures are swallowed +
   * logged.
   *
   * `runnerId` is worker-supplied and unverified; `ctx` carries the identity
   * the tunnel actually authenticated. The hook MUST atomically verify the
   * runner is assigned to `ctx.workerId`/`ctx.owner` before acting — without
   * that check, any authenticated worker could fail another tenant's sessions
   * by reporting their runner id.
   */
  onRunnerExited?: (runnerId: string, error: string, ctx: WorkerRunnerExitContext) => Promise<void>;
  /**
   * Async hook fired once the worker tunnel is established (after the loops are
   * running, so the hook can perform real tunnel I/O — e.g. reconnect
   * reconciliation). Receives the `workerId`. The await is bounded by
   * {@link onWorkerConnectTimeoutMs}. Failures are swallowed + logged.
   */
  onWorkerConnect?: (workerId: string) => Promise<void>;
  /**
   * Upper bound (ms) on awaiting {@link onWorkerConnect}. Defaults to
   * {@link ON_WORKER_CONNECT_TIMEOUT_MS} (30s). Primarily a tuning / test seam.
   */
  onWorkerConnectTimeoutMs?: number;
  /**
   * Async hook fired when the worker tunnel closes for any reason (after a
   * successful register). Receives the `workerId`. Failures are swallowed + logged.
   */
  onWorkerDisconnect?: (workerId: string) => Promise<void>;
  /** Optional logger. When omitted, lifecycle events are silent. */
  logger?: WorkerTunnelLogger;
  /** Clock source (epoch ms), for deterministic tests. Defaults to `Date.now`. */
  now?: () => number;
  /**
   * Sleep `ms`, used by the ping loop. Defaults to a real `setTimeout`. A test
   * injects a controllable timer. The returned promise must resolve after `ms`.
   */
  sleep?: (ms: number) => Promise<void>;
}

/**
 * Drives one worker tunnel from handshake to teardown.
 *
 * Construct one server with its collaborators (registry, store, hooks) and call
 * {@link handle} once per accepted WebSocket. The engine owns no connections —
 * the registry and store do; this object marshals frames on/off the wire and
 * runs the lifecycle. {@link handle} is fire-and-forget from the HTTP layer's
 * perspective and never rejects (every failure funnels into a best-effort close +
 * guarded cleanup).
 */
export class WorkerTunnelServer {
  constructor(private readonly opts: WorkerTunnelOptions) {}

  private now(): number {
    return this.opts.now?.() ?? Date.now();
  }

  private sleep(ms: number): Promise<void> {
    if (this.opts.sleep !== undefined) {
      return this.opts.sleep(ms);
    }
    return new Promise<void>((resolve) => setTimeout(resolve, ms));
  }

  private log(): WorkerTunnelLogger | undefined {
    return this.opts.logger;
  }

  /**
   * Authenticate, handshake, and run one worker tunnel for `workerId`.
   *
   * Protocol (the auth gate runs BEFORE accept on a real socket — the adapter is
   * responsible for not having accepted yet; closing before accept refuses the
   * handshake so an unauthenticated peer never completes the upgrade):
   *   1. Resolve the owner from the handshake (launch token, or auth provider,
   *      or the reserved local identity). Fail-closed on a bad launch token or a
   *      missing identity under an enabled provider.
   *   2. Receive the `worker.hello` frame; validate `frame_protocol_version`
   *      (strict-major).
   *   3. Upsert the worker row (mark online). A throw here = never registered.
   *   4. Register the live connection.
   *   5. Start sender / receive / ping loops; fire the connect hook (bounded).
   *   6. On disconnect: deregister, set offline, fire the disconnect hook —
   *      exactly once, ALWAYS from the post-register inner `finally` (a socket
   *      close arrives as an aborting `close` message, not a throw, so it flows
   *      through that finally). The outer `catch` handles a PRE-register failure:
   *      it runs deregister + setOffline only and never fires the disconnect
   *      hook, so neither a cross-user pre-register throw nor a post-register
   *      loop rejection double-fires (or stray-fires) it.
   *
   *      Two refinements to that teardown: every path rejects the connection's
   *      in-flight pending waiters (no caller hangs on a worker that vanished
   *      mid-request), and a connection superseded by a newest-wins `register`
   *      replacement skips the shared steps (deregister / setOffline / hook)
   *      entirely — it settles only its own waiters and closes its own socket,
   *      so a zombie's late death cannot retire or offline the live replacement.
   */
  async handle(ws: WorkerWebSocket, handshake: WorkerHandshake, workerId: string): Promise<void> {
    // 1. Authenticate from the handshake before any protocol I/O.
    let tunnelOwner: string | undefined;
    try {
      const resolved = await this.resolveOwner(handshake, workerId);
      if (resolved.kind === 'refused') {
        ws.close(UNAUTHENTICATED_CLOSE_CODE, 'unauthenticated');
        return;
      }
      tunnelOwner = resolved.owner;
    } catch (exc) {
      // A launch-token lookup that threw (e.g. a store error): fail closed
      // rather than fall through to user/anonymous auth.
      this.log()?.error?.({ err: exc, workerId }, 'worker tunnel auth error');
      ws.close(UNAUTHENTICATED_CLOSE_CODE, 'unauthenticated');
      return;
    }

    let conn: WorkerConnection | undefined;
    // Post-register teardown latch. Once this connection registers, its inner
    // `try/finally` (below) owns ALL teardown — deregister + setOffline +
    // fireDisconnect. A loop that REJECTS (e.g. `senderLoop`'s `ws.sendText`
    // rejecting on a write-after-vanish, reachable in production) makes
    // `Promise.race(loops)` reject: the inner `finally` runs the full cleanup, and
    // the rejection then propagates to the outer `catch`. We must NOT cleanup a
    // second time there — re-running deregister/setOffline would double-process
    // reconnect reconciliation, violating the exactly-once contract the teardown
    // tests assert. This latch makes that structural: the inner finally sets it,
    // and the outer catch only runs cleanup for a PRE-register failure (latch
    // still `false`). The disconnect hook is fired SOLELY by the inner finally —
    // the outer catch never fires it: the inner teardown (the normal
    // socket-close path) owns the hook; the outer catch (a pre-register or
    // unexpected failure) deliberately does not.
    let teardownRan = false;
    try {
      // 2. Receive the hello frame.
      const first = await ws.receive();
      if (first.type === 'close') {
        return; // socket closed before hello
      }
      let frame: WorkerFrame;
      try {
        frame = decodeWorkerFrame(first.data);
      } catch {
        ws.close(EXPECTED_HELLO_CLOSE_CODE, 'expected worker.hello frame');
        return;
      }
      if (frame.kind !== WorkerFrameKind.Hello) {
        ws.close(EXPECTED_HELLO_CLOSE_CODE, 'expected worker.hello frame');
        return;
      }
      const hello: WorkerHelloFrame = frame;

      // 2b. Version-skew check (strict-major).
      const remoteMajor = hello.frameProtocolVersion;
      if (remoteMajor !== SUPPORTED_FRAME_PROTOCOL_MAJOR) {
        ws.close(
          VERSION_MISMATCH_CLOSE_CODE,
          `frame_protocol_version mismatch: server supports ${SUPPORTED_FRAME_PROTOCOL_MAJOR}, ` +
            `worker sent ${remoteMajor}`,
        );
        return;
      }

      // 3. Upsert the worker row (mark online). A throw here — e.g. an owner
      //    conflict — means we never registered THIS connection, so the catch
      //    below must NOT deregister or flip the existing owner's worker offline.
      await this.opts.store.upsertOnConnect({
        workerId,
        name: hello.name,
        owner: tunnelOwner,
        allowWorkerIdReown: this.opts.allowWorkerIdReown ?? false,
        configuredHarnesses: hello.configuredHarnesses ?? null,
      });

      // 4. Register the live connection under the resolved owner.
      conn = this.opts.registry.register(workerId, ws, hello, {
        ...(tunnelOwner !== undefined ? { owner: tunnelOwner } : {}),
      });
      conn.lastFrameAt = this.now();
      this.log()?.info?.(
        { workerId, version: hello.version, name: hello.name, runners: hello.runners ?? [] },
        'worker connected',
      );

      // 5. Start the three loops. The sender is the only socket writer; a
      //    server-side caller enqueues through the connection's outbound queue.
      //    One shared AbortController stops every loop on the first finisher.
      const ac = new AbortController();
      const loops = [
        this.senderLoop(ws, conn, ac.signal),
        this.receiveLoop(ws, conn, workerId, ac),
        this.pingLoop(ws, conn, workerId, ac.signal),
      ];
      // Materialize the race NOW so every loop has a rejection observer from the
      // moment it starts. The race is awaited only after the connect hook below
      // (up to 30s); without a pre-attached handler, a loop that rejects in that
      // window (e.g. the sender's `ws.sendText` on a write-after-vanish, which
      // the teardown latch comment calls reachable in production) would be an
      // unhandled rejection — a process-wide crash under Node's default
      // `--unhandled-rejections=throw`. The no-op catch is on a BRANCH promise;
      // awaiting `raced` later still re-raises the rejection into the teardown
      // path exactly as before.
      const raced = Promise.race(loops);
      raced.catch(() => {});

      // Fire the connect hook after the loops are live (so a hook that issues a
      // tunneled request can be answered), bounded so a hung hook can't stall
      // teardown: on timeout it is left running detached and a warning is logged.
      if (this.opts.onWorkerConnect !== undefined) {
        const timeoutMs = this.opts.onWorkerConnectTimeoutMs ?? ON_WORKER_CONNECT_TIMEOUT_MS;
        try {
          await runWithTimeout(this.opts.onWorkerConnect(workerId), timeoutMs);
        } catch (exc) {
          if (exc instanceof WorkerTunnelTimeoutError) {
            this.log()?.warn?.({ workerId, timeoutMs }, 'onWorkerConnect callback timed out');
          } else {
            this.log()?.error?.({ err: exc, workerId }, 'onWorkerConnect callback failed');
          }
        }
      }

      try {
        // First loop to settle ends the tunnel. A loop that threw re-surfaces
        // here so the outer catch logs it (the first completed loop's exception
        // is re-raised).
        await raced;
      } finally {
        // Latch BEFORE any teardown await: even if a step here rejects and
        // unwinds into the outer catch, that catch must treat teardown as already
        // owned by this finally and not run it a second time.
        teardownRan = true;
        ac.abort();
        // Reconnect-race guard: `get` + `deregister` run back-to-back with no
        // await in between, so the check is atomic within this event-loop turn.
        // When a newest-wins `register` replacement has superseded this
        // connection (worker re-dialed while our socket was still dying), this
        // stale handle must not touch shared state keyed by `workerId`: no
        // deregister (it would retire the NEW connection and kill its sender),
        // no setOffline (it would offline a freshly-connected worker), no
        // disconnect hook (it would re-run reconnect reconciliation against a
        // live worker). It still owns its private cleanup: unblock its own
        // parked sender, settle its own waiters, close its own socket.
        const stillCurrent = this.opts.registry.get(workerId) === conn;
        if (stillCurrent) {
          // Deregister FIRST: this pushes the sender's stop sentinel, which is
          // what unblocks a sender parked on `outboundQueue.get()`. Only then can
          // every loop settle. Draining before deregistering would deadlock.
          this.opts.registry.deregister(workerId);
        } else {
          // Superseded: push the stop sentinel ourselves — deregister (the usual
          // pusher) must not run, but our sender still needs unblocking.
          conn.outboundQueue.put(null);
        }
        await Promise.allSettled(loops);
        // Settle every in-flight waiter AFTER the loops stop (the receive loop
        // can no longer resolve them): a caller awaiting a launch/stop/stat
        // result when the worker vanished must get a rejection, not hang forever.
        rejectAllPending(conn, new WorkerTunnelClosedError());
        if (stillCurrent) {
          // A setOffline rejection (it is a store round trip) must not skip the
          // disconnect hook below — the hook's exactly-once contract is per
          // connection, and reconnect reconciliation must still run.
          try {
            await this.opts.store.setOffline(workerId);
          } catch (offlineExc) {
            this.log()?.error?.(
              { err: offlineExc, workerId },
              'worker set-offline failed during teardown',
            );
          }
          await this.fireDisconnect(workerId);
        }
        // Best-effort close so a teardown initiated by a non-socket event (ping
        // watchdog store path, loop rejection) does not leave the peer holding a
        // half-open socket it still believes is connected.
        try {
          ws.close();
        } catch {
          // already closed
        }
      }
    } catch (exc) {
      this.log()?.error?.({ err: exc, workerId }, 'worker tunnel error');
      // This is the unexpected-failure branch. The normal socket-close path does
      // NOT reach here: a socket close arrives as a `close` message that aborts
      // the loop race rather than a thrown exception, so the normal disconnect
      // teardown — including the disconnect hook — runs in the post-register inner
      // `finally` above, not here.
      //
      // This branch runs deregister + setOffline ONLY and deliberately does NOT
      // fire the disconnect hook. Two guards bound when it runs cleanup at all:
      //   1. `conn === undefined`: a connect that failed before register — e.g.
      //      the upsert owner-conflict throw when a peer connects with another
      //      owner's host_id — must not deregister or flip that owner's worker
      //      offline (cross-user DoS).
      //   2. `teardownRan`: the post-register inner `finally` already ran the full
      //      teardown (deregister + setOffline + fireDisconnect). A loop rejection
      //      that propagated here after that finally must NOT re-run deregister /
      //      setOffline — doing so would double-process reconnect reconciliation,
      //      breaking the exactly-once contract.
      //
      // Not firing the disconnect hook here also means the post-register inner
      // `finally` is the SOLE source of the hook: the residual window where a
      // synchronous step between register and that inner `try` throws (e.g. an
      // injected `now()` raising) now lands here and runs deregister + setOffline
      // without the hook — so the hook still fires exactly once on every path that
      // reaches the inner finally and never from this branch.
      if (conn !== undefined && !teardownRan) {
        this.opts.registry.deregister(workerId);
        try {
          await this.opts.store.setOffline(workerId);
        } catch (offlineExc) {
          this.log()?.error?.({ err: offlineExc, workerId }, 'worker set-offline failed');
        }
      }
      try {
        ws.close();
      } catch {
        // best-effort
      }
    }
  }

  /**
   * Resolve the tunnel owner from the handshake.
   *
   * - A sandbox-worker launch token is an explicit credential: when present it MUST
   *   resolve and be scoped to this `workerId`, else refuse. Never fall through to
   *   user/anonymous auth (a peer that chose this header has no user identity to
   *   fall back on, and falling back would let a junk token downgrade into
   *   header/anonymous auth). The token is scoped to one worker id, so a leaked
   *   token cannot register an arbitrary worker.
   * - Otherwise, with an auth provider: the identity must resolve, else refuse
   *   (never fall back to the reserved local owner, which is admin-equivalent
   *   under a multi-user scheme).
   * - Otherwise (no provider): single-user / local mode — the reserved local
   *   owner is the accepted owner.
   */
  private async resolveOwner(
    handshake: WorkerHandshake,
    workerId: string,
  ): Promise<{ kind: 'ok'; owner: string | undefined } | { kind: 'refused' }> {
    const launchToken = handshake.header(HOST_TUNNEL_TOKEN_HEADER);
    if (launchToken !== undefined) {
      const resolved = await this.opts.store.resolveLaunchToken(launchToken);
      if (resolved === null || resolved.workerId !== workerId) {
        return { kind: 'refused' };
      }
      return { kind: 'ok', owner: resolved.owner };
    }
    if (this.opts.authProvider !== undefined) {
      const owner = this.opts.authProvider.getUserId(handshake) ?? undefined;
      if (owner === undefined) {
        return { kind: 'refused' };
      }
      return { kind: 'ok', owner };
    }
    return { kind: 'ok', owner: LOCAL_TUNNEL_OWNER };
  }

  /** Drain the connection's outbound queue to the socket until the stop sentinel. */
  private async senderLoop(
    ws: WorkerWebSocket,
    conn: WorkerConnection,
    signal: AbortSignal,
  ): Promise<void> {
    const aborted = abortPromise(signal);
    while (!signal.aborted) {
      const data = await Promise.race([conn.outboundQueue.get(), aborted]);
      if (data === null || data === ABORTED || signal.aborted) {
        return; // stop sentinel, or teardown began
      }
      await ws.sendText(data);
    }
  }

  /**
   * Receive worker frames and route results to pending waiters.
   *
   * A `close` message ends the tunnel (aborts the other loops + returns). Each
   * inbound text refreshes liveness. A frame that is not a worker frame is tried as
   * a runner keepalive frame: a pong is expected (the ping loop sends
   * runner-tunnel pings and the worker answers with a runner-tunnel pong on the
   * same socket) and dropped; anything else malformed is dropped with a warning.
   */
  private async receiveLoop(
    ws: WorkerWebSocket,
    conn: WorkerConnection,
    workerId: string,
    ac: AbortController,
  ): Promise<void> {
    const aborted = abortPromise(ac.signal);
    while (!ac.signal.aborted) {
      const message = await Promise.race([ws.receive(), aborted]);
      if (message === ABORTED || ac.signal.aborted) {
        return; // teardown began elsewhere
      }
      if (message.type === 'close') {
        // The socket closed: end the tunnel and let the loop race settle.
        ac.abort();
        return;
      }
      conn.lastFrameAt = this.now();
      const raw = message.data;

      let frame: WorkerFrame;
      try {
        frame = decodeWorkerFrame(raw);
      } catch {
        // Not a worker frame: it may be a runner keepalive pong multiplexed on the
        // same socket. A pong is expected and dropped; anything else is dropped
        // with a warning.
        try {
          const runnerFrame = decodeFrame(raw);
          if (runnerFrame.kind === FrameKind.Pong) {
            continue;
          }
          this.log()?.warn?.(
            { workerId, kind: runnerFrame.kind },
            'worker sent unexpected runner frame; dropping',
          );
        } catch (innerExc) {
          this.log()?.warn?.({ workerId, err: innerExc }, 'worker sent malformed frame; dropping');
        }
        continue;
      }

      // A non-result frame that arrives may also be the worker's own keepalive
      // (handled by liveness refresh above). Route every known result kind.
      await this.routeFrame(frame, conn, workerId);
    }
  }

  /** Resolve the matching pending waiter (or record / report) for one worker frame. */
  private async routeFrame(
    frame: WorkerFrame,
    conn: WorkerConnection,
    workerId: string,
  ): Promise<void> {
    switch (frame.kind) {
      case WorkerFrameKind.LaunchRunnerResult: {
        // Branch on the status discriminant so the result keeps the frame
        // union's structural null-correlations (launched ⇒ runnerId present).
        const result: WorkerLaunchResult =
          frame.status === 'launched'
            ? {
                requestId: frame.requestId,
                status: frame.status,
                runnerId: frame.runnerId,
                error: null,
                errorCode: null,
              }
            : {
                requestId: frame.requestId,
                status: frame.status,
                runnerId: null,
                error: frame.error ?? null,
                errorCode: frame.errorCode ?? null,
              };
        resolvePending(conn.pendingLaunches, frame.requestId, result);
        return;
      }
      case WorkerFrameKind.StopRunnerResult: {
        resolvePending(conn.pendingStops, frame.requestId, {
          requestId: frame.requestId,
          status: frame.status,
          error: frame.error ?? null,
        });
        return;
      }
      case WorkerFrameKind.RunnerExited: {
        await this.recordRunnerExited(frame, conn, workerId);
        return;
      }
      case WorkerFrameKind.StatResult: {
        resolvePending(conn.pendingStats, frame.requestId, {
          requestId: frame.requestId,
          status: frame.status,
          exists: frame.exists ?? false,
          type: frame.type ?? null,
          canonicalPath: frame.canonicalPath ?? null,
          error: frame.error ?? null,
        });
        return;
      }
      case WorkerFrameKind.ListDirResult: {
        resolvePending(conn.pendingListDirs, frame.requestId, {
          requestId: frame.requestId,
          status: frame.status,
          entries: [...(frame.entries ?? [])],
          hasMore: frame.hasMore ?? false,
          error: frame.error ?? null,
        });
        return;
      }
      case WorkerFrameKind.CreateWorktreeResult: {
        resolvePending(conn.pendingCreateWorktrees, frame.requestId, {
          requestId: frame.requestId,
          status: frame.status,
          worktreePath: frame.worktreePath ?? null,
          branch: frame.branch ?? null,
          error: frame.error ?? null,
        });
        return;
      }
      case WorkerFrameKind.RemoveWorktreeResult: {
        resolvePending(conn.pendingRemoveWorktrees, frame.requestId, {
          requestId: frame.requestId,
          status: frame.status,
          error: frame.error ?? null,
        });
        return;
      }
      case WorkerFrameKind.CreateDirResult: {
        resolvePending(conn.pendingCreateDirs, frame.requestId, {
          requestId: frame.requestId,
          status: frame.status,
          path: frame.path ?? null,
          error: frame.error ?? null,
        });
        return;
      }
      default: {
        // A request kind (worker.launch_runner, worker.stat, …) or hello arriving
        // worker→server is unexpected; the server only ever receives results +
        // the one-way runner_exited report. Drop with a debug-level note.
        this.log()?.info?.(
          { workerId, kind: frame.kind },
          'worker sent unexpected frame; dropping',
        );
      }
    }
  }

  /**
   * Record a one-way `worker.runner_exited` report: a runner this worker spawned died
   * unexpectedly. Stash the cause (owner-scoped) so the runner-status endpoint can
   * answer "offline, and here is why", and fire the `onRunnerExited` hook so the
   * runner's session(s) are marked failed — the only failure signal for a runner
   * that crashed before connecting its own tunnel.
   */
  private async recordRunnerExited(
    frame: WorkerRunnerExitedFrame,
    conn: WorkerConnection,
    workerId: string,
  ): Promise<void> {
    this.log()?.warn?.(
      { workerId, runnerId: frame.runnerId, error: frame.error },
      'worker reported runner exited',
    );
    // Hand every consumer the AUTHENTICATED identity next to the worker-supplied
    // claim: frame.runnerId is unverified wire input, and both the sink and the
    // hook must check the runner's assignment against this context before
    // trusting it (cross-tenant spoof guard — see WorkerRunnerExitContext).
    const ctx: WorkerRunnerExitContext = { workerId, owner: conn.owner };
    this.opts.runnerExitReports?.record(frame.runnerId, frame.error, ctx);
    if (this.opts.onRunnerExited !== undefined) {
      try {
        // Bounded like the connect hook: this await runs INSIDE the receive
        // loop, so an unbounded hung hook (e.g. a stuck DB write) would halt all
        // frame processing until the ping watchdog misdiagnosed the stall as a
        // dead worker. On timeout the hook keeps running detached.
        await runWithTimeout(
          this.opts.onRunnerExited(frame.runnerId, frame.error, ctx),
          ON_RUNNER_EXITED_TIMEOUT_MS,
        );
      } catch (exc) {
        if (exc instanceof WorkerTunnelTimeoutError) {
          this.log()?.warn?.(
            { workerId, runnerId: frame.runnerId, timeoutMs: ON_RUNNER_EXITED_TIMEOUT_MS },
            'onRunnerExited callback timed out',
          );
        } else {
          this.log()?.error?.(
            { err: exc, workerId, runnerId: frame.runnerId },
            'onRunnerExited callback failed',
          );
        }
      }
    }
  }

  /**
   * Send pings every {@link PING_INTERVAL_MS}; declare the worker dead after
   * {@link PING_MISS_THRESHOLD} missed intervals. Each live tick also persists a
   * heartbeat so the worker's last-seen timestamp stays fresh (the liveness
   * freshness gate): when a worker dies without a graceful disconnect, the heartbeat
   * stops, the timestamp goes stale, and the worker's sessions correctly drop out of
   * the connected set even though `setOffline` never ran.
   */
  private async pingLoop(
    ws: WorkerWebSocket,
    conn: WorkerConnection,
    workerId: string,
    signal: AbortSignal,
  ): Promise<void> {
    while (!signal.aborted) {
      await abortableSleep(this.sleep(PING_INTERVAL_MS), signal);
      if (signal.aborted) {
        return;
      }
      const elapsed = this.now() - conn.lastFrameAt;
      if (elapsed > PING_INTERVAL_MS * PING_MISS_THRESHOLD) {
        this.log()?.warn?.(
          { workerId, missThreshold: PING_MISS_THRESHOLD, elapsedMs: elapsed },
          'worker missed ping intervals; declaring dead',
        );
        try {
          ws.close(PING_TIMEOUT_CLOSE_CODE, 'ping timeout');
        } catch {
          // already closed
        }
        return;
      }
      // Still within the liveness window — refresh last-seen, then ping.
      try {
        await this.opts.store.heartbeat(workerId);
      } catch (exc) {
        // A store blip must NOT end the loop: pingLoop is in the teardown race,
        // so returning here would run the FULL teardown — deregistering and
        // offlining a worker whose socket is perfectly healthy (and, since every
        // tunnel on the replica heartbeats the same store, one DB hiccup would
        // wedge all of them at once). Log and keep looping: the ping watchdog
        // above covers genuine liveness loss, and a persistently failing
        // heartbeat degrades to a stale last-seen timestamp, which the
        // freshness gate already handles.
        this.log()?.error?.({ err: exc, workerId }, 'worker heartbeat failed');
      }
      try {
        conn.outboundQueue.put(encodeFrame({ kind: FrameKind.Ping, ts: this.now() }));
      } catch {
        return; // any enqueue failure ends the ping loop cleanly
      }
    }
  }

  /** Fire the disconnect hook, swallowing + logging any failure. */
  private async fireDisconnect(workerId: string): Promise<void> {
    if (this.opts.onWorkerDisconnect === undefined) {
      return;
    }
    try {
      await this.opts.onWorkerDisconnect(workerId);
    } catch (exc) {
      this.log()?.error?.({ err: exc, workerId }, 'onWorkerDisconnect callback failed');
    }
  }
}

// ── Helpers ──────────────────────────────────────────────

/** Resolve a one-shot pending waiter for `requestId`, if one is registered. */
function resolvePending<T>(
  pending: Map<string, WorkerPendingRequest<T>>,
  requestId: string,
  result: T,
): void {
  const waiter = pending.get(requestId);
  if (waiter === undefined) {
    return;
  }
  pending.delete(requestId);
  waiter.resolve(result);
}

/**
 * Reject and clear every in-flight waiter on `conn` — the teardown path that
 * guarantees no caller hangs forever on a request the worker will never answer.
 */
function rejectAllPending(conn: WorkerConnection, reason: WorkerTunnelClosedError): void {
  const maps: Array<Map<string, { reject(reason: unknown): void }>> = [
    conn.pendingLaunches,
    conn.pendingStops,
    conn.pendingStats,
    conn.pendingListDirs,
    conn.pendingCreateWorktrees,
    conn.pendingRemoveWorktrees,
    conn.pendingCreateDirs,
  ];
  for (const pending of maps) {
    for (const waiter of pending.values()) {
      waiter.reject(reason);
    }
    pending.clear();
  }
}

/** Sentinel resolved value used to break out of a wait on abort. */
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

/** Await `sleeper`, returning early once `signal` aborts. */
function abortableSleep(sleeper: Promise<void>, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return Promise.resolve();
  }
  return new Promise<void>((resolve) => {
    let settled = false;
    const finish = (): void => {
      if (settled) {
        return;
      }
      settled = true;
      signal.removeEventListener('abort', finish);
      resolve();
    };
    signal.addEventListener('abort', finish, { once: true });
    sleeper.then(finish, finish);
  });
}

/**
 * Error raised by {@link runWithTimeout} when the wrapped promise does not settle
 * within the deadline. A dedicated class (not a bare `Error`) so the connect-hook
 * handler can distinguish a timeout — logged as a warning — from a genuine hook
 * failure logged as an error. The wrapped promise is NOT cancelled (JS promises
 * are not abortable); it is left running detached.
 */
export class WorkerTunnelTimeoutError extends Error {
  constructor(message = 'operation timed out') {
    super(message);
    this.name = 'WorkerTunnelTimeoutError';
  }
}

/**
 * Await `promise`, rejecting with a {@link WorkerTunnelTimeoutError} if it does not
 * settle within `ms`. When `promise` settles first the timer is cleared; when the
 * timeout wins `promise` keeps running detached (its later settlement is ignored,
 * and a late rejection is swallowed so it cannot surface as unhandled).
 */
function runWithTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      promise.then(undefined, () => {});
      reject(new WorkerTunnelTimeoutError());
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
