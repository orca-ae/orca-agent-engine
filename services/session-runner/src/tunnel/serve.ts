// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Runner-side tunnel serve loop — the CLIENT half of the runner tunnel.
//
// The runner dials the registry's PUBLIC runner-tunnel endpoint
// `/v1/tunnels/runners/:runnerId` over WSS, says hello, and then SERVES the
// request/response stream the registry pushes: for each inbound `request` frame
// it dispatches through the {@link RequestDispatcher} (turn / snapshot / replay
// handlers, registered by `register-handlers.ts`) and frames the response back as
// `response.head` + N×`response.body` + `response.end`. Tunneled WebSocket
// attaches (`ws.open` / `ws.frame` / `ws.close`) ride the same socket and are
// dispatched to per-channel handlers. The tunnel is framed HTTP: the registry is
// the request DRIVER and the runner is the responder, over the frames + transport
// shared with the registry side through `@orca/harness-tunnel`.
//
// Reconnect posture (the same as the worker-tunnel worker client):
// disconnects retry forever with capped backoff + jitter, so starting the runner
// before the registry is reachable is valid. A routine ingress recycle (close
// 1001/1012, or an abrupt drop) reconnects PROMPTLY at the base delay (the
// registry wants a quick reconnect; escalating would leave the runner
// unregistered and turns undeliverable for seconds each recycle). A frame-protocol
// / binding refusal — a 403 upgrade rejection, or a 4001/4002/4004/4500 close code
// — is FATAL: retrying can never succeed, so the loop exits with a rejection the
// caller surfaces. A 401 upgrade rejection refreshes the binding token (via the
// optional token factory) and retries after the same backoff every other retry
// takes; a bounded run of consecutive 401s (see MAX_CONSECUTIVE_401_REFRESHES)
// means the binding is permanently wrong and is fatal too.
//
// Authentication: the registry's runner-tunnel route authenticates a runner SOLELY
// by the X-Orca-Runner-Tunnel-Token binding-token header (token → runner-id
// correlation) — there is no login-redirecting ingress in front of it. So this
// client carries NEITHER (a) an Authorization: Bearer header (the optional token
// factory refreshes the binding token instead, which is what a 401 retries with),
// NOR (b) any "upgrade redirected to a login page" detection: Orca has no
// login-gated ingress, and `ws` surfaces a rejected upgrade as a status-carrying
// UpgradeRejectedError (classified above) rather than a redirect-to-http URI, so
// there is nothing to detect; a bad upgrade just falls into the status-based
// 401 / 403 / recycle / transient classification.
//
// Idle lifecycle: an optional onActivity hook is touched for each real
// server→runner WORK frame (request / request.cancel / ws.*) but NOT for ping, so
// tunnel keepalives never keep an otherwise-idle runner alive — the runner answers
// pings with pongs regardless. The wire-level keepalive (ping→pong) and the
// idle-activity touch are intentionally separate concerns.
//
// Concurrency model (single-threaded JS event loop): the serve loop pulls inbound
// messages off the socket seam one at a time; each `request` frame spawns an
// independent dispatch task (tracked by reqId), and each `ws.open` spawns an
// independent channel task (tracked by chId). On socket close (or `stop()`) every
// in-flight dispatch + channel task is cancelled and drained. The socket seam owns
// the actual `ws` plumbing — this module is network-free apart from the injected
// connector, so the whole loop is unit-testable in-process against a fake registry
// runner-tunnel WS peer + a fake dispatcher.

import {
  FrameKind,
  encodeFrame,
  decodeFrame,
  decodeBody,
  encodeBody,
  RUNNER_TUNNEL_TOKEN_HEADER,
  INTERNAL_WS_ORIGIN,
  type Frame,
  type HelloFrame,
  type RequestFrame,
  type WsOpenFrame,
} from '@orca/harness-tunnel';
import {
  type DispatchRequestInput,
  type DispatchResponse,
  type RequestDispatcher,
  type RouteDispatcher,
  type WsChannel,
  type WsChannelMessage,
  type WsHandler,
} from './request-dispatch.js';
import {
  UpgradeRejectedError,
  WsRunnerTunnelConnector,
  type RunnerTunnelConnector,
  type RunnerTunnelMessage,
  type RunnerTunnelSocket,
} from './ws-client.js';

/** Wire-protocol major the runner speaks; the registry refuses on a mismatch. */
const FRAME_PROTOCOL_VERSION = 1;

/**
 * Reconnect backoff: 0.5s initial, 10s cap, ±50% jitter. The jitter spreads
 * simultaneous reconnects from many runners across each backoff window so a
 * registry restart doesn't see a synchronized WS-accept spike. The cap is small
 * enough that a transient disconnect during startup stays visible inside a parent
 * runner-startup budget while still backing off enough not to hammer a slow
 * registry. The worker-tunnel worker uses the same schedule.
 */
const INITIAL_RECONNECT_DELAY_MS = 500;
const MAX_RECONNECT_DELAY_MS = 10_000;
const RECONNECT_JITTER_FRACTION = 0.5;

/**
 * Close codes that are FATAL frame-protocol / binding faults: retrying can never
 * succeed, so the loop exits. `4001` (first frame was not a hello), `4002`
 * (frame-protocol major mismatch), `4004` (runner-id / token-binding refusal),
 * `4500` (a reserved server-fatal code).
 *
 * Deliberately NOT fatal: the registry route's `4403` (forbidden origin) and `4003`
 * (ping watchdog). A correct runner never earns either: it always sends
 * {@link INTERNAL_WS_ORIGIN}, which the route's CSWSH guard allows (no 4403), and
 * answers every server ping with a pong while the route refreshes liveness on each
 * inbound frame (no 4003). If one ever fired (misconfig / event-loop starvation)
 * the right recovery is a plain backoff-reconnect (a generic `closed` outcome)
 * rather than a fatal exit — so they are neither fatal nor recycle codes here, and
 * fall through to reconnect.
 */
const FATAL_SERVER_CLOSE_CODES: ReadonlySet<number> = new Set([4001, 4002, 4004, 4500]);

/**
 * Routine server-initiated recycles, NOT errors: `1001` "going away" and `1012`
 * "service restart" are how an ingress cycles long-lived WebSockets out from under
 * a healthy app. The server WANTS a prompt reconnect, so the backoff resets to its
 * minimum instead of escalating toward the cap. (The route's `4003`/`4403` are
 * intentionally NOT here either — see {@link FATAL_SERVER_CLOSE_CODES}: they are
 * generic closes that reconnect with escalating backoff.)
 */
const TUNNEL_RECYCLE_CLOSE_CODES: ReadonlySet<number> = new Set([1001, 1012]);

/** HTTP upgrade status that refreshes the binding token then retries. */
const REFRESHABLE_HTTP_STATUS = 401;
/**
 * How many CONSECUTIVE 401 upgrade rejections a token refresh may absorb before
 * the loop gives up and exits fatally. A refresh that keeps earning 401 is a
 * permanently mis-bound runner (a rotated-away binding, a runner id the registry
 * no longer knows): retrying can never succeed, so the runner exits instead of
 * hammering the registry forever. The budget resets on every accepted upgrade,
 * so a genuine rotation — one 401, one refresh, one reconnect — never trips it.
 * Exported so the spec pins the bound rather than re-deriving it.
 */
export const MAX_CONSECUTIVE_401_REFRESHES = 5;
/** HTTP upgrade status that is fatal (auth ok, tunnel refused — never retryable). */
const FATAL_HTTP_STATUS = 403;
/** HTTP upgrade statuses that recycle promptly (ingress bounce) rather than escalate. */
const RECYCLE_HTTP_STATUSES: ReadonlySet<number> = new Set([502]);

/** The stable prefix of the fatal-rejection error message (a cross-test contract). */
export const RUNNER_TUNNEL_REJECTION_PREFIX = 'runner tunnel rejected by server ';

/** Structured logger seam (a subset of the usual structured logger). All optional. */
export interface ServeTunnelLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/** Options for {@link serveTunnel}. */
export interface ServeTunnelOptions {
  /** Routes the runner serves for each pushed request (turn / snapshot / replay). */
  dispatcher: RequestDispatcher;
  /**
   * Registry runner-tunnel BASE URL the runner dials, e.g.
   * `"ws://127.0.0.1:8081"` or `"https://registry.example"`. The runner builds
   * the public path `/v1/tunnels/runners/:runnerId` onto it — the same shape the
   * registry route mounts.
   */
  registryRunnerUrl: string;
  /** Stable runner id presented on the tunnel path (token-bound). */
  runnerId: string;
  /** Tunnel binding token presented in the {@link RUNNER_TUNNEL_TOKEN_HEADER}. */
  bindingToken: string;
  /** Runner version string carried in the hello, e.g. `"0.1.0"`. */
  runnerVersion: string;
  /**
   * Providers this runner can serve, advertised in the hello (capability-advertise:
   * the registry fails a session whose provider the connected runner does not
   * advertise). Rides the hello's `harnesses` field on the wire — the field the
   * registry's connect hook + listing read. Defaults to `[]`.
   */
  providers?: readonly string[];
  /**
   * Environment types this runner can serve, advertised in the hello's `envs`
   * field (capability-advertise — the parallel of {@link providers}, e.g.
   * `["os_sandbox"]`). The registry's runner-tunnel route does not consume `envs`,
   * so this is advertise-only. Defaults to `[]`.
   */
  envs?: readonly string[];
  /**
   * Per-session last-consumed event ids the runner presents on (re)connect
   * (`sessionId → eventId`), so the owner pod serves an incremental `after={cursor}`
   * resume replay instead of a fresh full replay. Defaults to `{}` (a fresh
   * runner). For a re-dial these can be recomputed via {@link resumeCursorsFactory}.
   */
  resumeCursors?: Readonly<Record<string, string>>;
  /**
   * Optional factory consulted before EACH connect to compute the current
   * per-session resume cursors. When set, its return value overrides
   * {@link resumeCursors} for that attempt — so a reconnect advertises the runner's
   * latest consumed ids. Failures fall back to {@link resumeCursors}.
   */
  resumeCursorsFactory?: () => Readonly<Record<string, string>>;
  /**
   * Optional factory consulted before EACH connect to obtain a fresh binding
   * token (e.g. a rotated credential). When set, its return value overrides
   * {@link bindingToken} for that attempt. On a 401 upgrade rejection it is
   * consulted once more before giving up. A `null`/`undefined` return or a throw
   * falls back to the last token.
   */
  tokenFactory?: () => string | null | undefined;
  /** Network seam. Defaults to the production `ws`-backed connector. */
  connector?: RunnerTunnelConnector;
  /**
   * Async hook fired after a successful RE-dial (not the first connect), before
   * serving frames. The runner uses it for a catch-up scan after a reconnect.
   * Failures are swallowed + logged.
   */
  onReconnect?: () => Promise<void>;
  /**
   * Sync hook fired for each real server→runner WORK frame (`request`,
   * `request.cancel`, `ws.open`, `ws.frame`, `ws.close`) — deliberately NOT for
   * `ping`, so tunnel keepalives don't keep an otherwise-idle runner alive. A
   * higher layer (idle-shutdown lifecycle) uses it as a last-activity touch.
   * Throws are swallowed + logged so a faulty hook never drops a frame.
   */
  onActivity?: () => void;
  /** Initial reconnect delay (ms). Defaults to {@link INITIAL_RECONNECT_DELAY_MS}. */
  initialReconnectDelayMs?: number;
  /** Reconnect backoff cap (ms). Defaults to {@link MAX_RECONNECT_DELAY_MS}. */
  maxReconnectDelayMs?: number;
  /** Sleep `ms`. Defaults to a real `setTimeout`. A test injects a controllable timer. */
  sleep?: (ms: number) => Promise<void>;
  /** Optional logger. When omitted, lifecycle events are silent. */
  logger?: ServeTunnelLogger;
}

/** A running runner tunnel: a `done` promise + a `stop()` for graceful teardown. */
export interface ServeTunnelHandle {
  /**
   * Resolves when the loop exits cleanly (only via {@link stop}); REJECTS when the
   * loop hit a fatal, non-retryable refusal (a 403 upgrade, an unrefreshable or
   * repeatedly-rejected 401 upgrade, or a 4001/4002/4004/4500 close code). It never
   * resolves on its own during normal operation — the loop reconnects forever — so a
   * caller awaits it to learn a fatal stop or to keep the process alive.
   */
  readonly done: Promise<void>;
  /**
   * Stop the loop: abort the current connection + every in-flight dispatch / WS
   * channel, end the reconnect loop, and await the loop's clean exit. Idempotent.
   */
  stop(): Promise<void>;
}

/**
 * Fatal, non-retryable runner-tunnel refusal. The loop exits with this when the
 * server rejects the binding (403 upgrade, a 401 upgrade that no token refresh
 * can clear, or a 4001/4002/4004/4500 close) — one "this can never succeed"
 * surface for every fatal cause.
 */
export class RunnerTunnelRejectedError extends Error {
  constructor(detail: string) {
    super(`${RUNNER_TUNNEL_REJECTION_PREFIX}(${detail})`);
    this.name = 'RunnerTunnelRejectedError';
  }
}

/**
 * Dial the registry runner tunnel and serve it forever (until {@link stop} or a
 * fatal refusal). Returns immediately with a handle; the loop runs detached.
 */
export function serveTunnel(opts: ServeTunnelOptions): ServeTunnelHandle {
  const connector = opts.connector ?? new WsRunnerTunnelConnector();
  const sleep = opts.sleep ?? defaultSleep;
  const initialDelay = opts.initialReconnectDelayMs ?? INITIAL_RECONNECT_DELAY_MS;
  const maxDelay = opts.maxReconnectDelayMs ?? MAX_RECONNECT_DELAY_MS;
  const tunnelUrl = buildTunnelUrl(opts.registryRunnerUrl, opts.runnerId);

  const stopController = new AbortController();
  let liveSocket: RunnerTunnelSocket | undefined;
  let bindingToken = opts.bindingToken;

  const loop = (async (): Promise<void> => {
    let delay = initialDelay;
    let connectedBefore = false;
    let consecutive401s = 0;
    while (!stopController.signal.aborted) {
      bindingToken = refreshToken(bindingToken, opts.tokenFactory, opts.logger);

      if (connectedBefore && opts.onReconnect !== undefined) {
        try {
          await opts.onReconnect();
        } catch (err) {
          opts.logger?.error?.({ err }, 'runner tunnel onReconnect hook failed');
        }
      }
      if (stopController.signal.aborted) {
        return;
      }

      let recycle = false;
      let served = false;
      let retryReason = 'connection closed';
      try {
        const headers = buildConnectHeaders(bindingToken);
        let socket: RunnerTunnelSocket;
        try {
          socket = await connector.connect(tunnelUrl, headers);
        } catch (err) {
          // A rejected upgrade: classify by HTTP status.
          if (err instanceof UpgradeRejectedError) {
            const status = err.status;
            if (status === REFRESHABLE_HTTP_STATUS) {
              // 401: try to refresh the binding token once more and retry promptly.
              // A 401 with NO way to obtain a fresh token can never succeed, so it
              // is fatal.
              const refreshed = refreshTokenAfter401(opts.tokenFactory, opts.logger);
              if (refreshed === null) {
                throw new RunnerTunnelRejectedError(
                  `HTTP ${status}; the runner tunnel binding token was rejected and no refresh is configured`,
                );
              }
              consecutive401s += 1;
              if (consecutive401s > MAX_CONSECUTIVE_401_REFRESHES) {
                // Refreshing is not helping: the binding is permanently wrong.
                throw new RunnerTunnelRejectedError(
                  `HTTP ${status}; ${consecutive401s} consecutive binding-token refreshes were rejected`,
                );
              }
              bindingToken = refreshed;
              // A fresh token deserves a PROMPT retry, so this is a recycle (the
              // delay resets to the base below) — but it still goes through the
              // shared bottom-of-loop backoff. Skipping it would spin the loop
              // against a registry that always 401s, with no sleep between dials.
              recycle = true;
              retryReason = `binding token refreshed after HTTP ${status}; reconnecting promptly`;
            } else if (status === FATAL_HTTP_STATUS) {
              throw new RunnerTunnelRejectedError(
                `HTTP ${status}; check the runner tunnel binding token`,
              );
            } else if (RECYCLE_HTTP_STATUSES.has(status)) {
              // A routine ingress recycle on the upgrade: reconnect promptly.
              recycle = true;
              retryReason = `server recycled the tunnel (HTTP ${status}); reconnecting promptly`;
            } else {
              // A transient rejection (5xx, 408, 429, etc.): retry with backoff.
              retryReason = err.message;
            }
            // Fall through to the shared post-attempt backoff + escalation below.
          } else {
            // Any other transport error (DNS / connect refused / abrupt drop): retry.
            retryReason = errMessage(err);
          }
          // No socket this attempt — skip the serve, go straight to backoff.
          throw new RetryAfterBackoff();
        }

        // The upgrade was ACCEPTED. Only now is this a real connection: the
        // re-dial flag flips here (not before the attempt) so `onReconnect`
        // cannot fire ahead of the FIRST successful connect just because an
        // earlier attempt failed — starting the runner before the registry is
        // reachable is a supported case. The 401 budget resets for the same
        // reason: this token binds.
        connectedBefore = true;
        consecutive401s = 0;
        liveSocket = socket;
        const outcome = await serveTunnelOnce({
          socket,
          dispatcher: opts.dispatcher,
          hello: buildHelloFrame(opts),
          signal: stopController.signal,
          logger: opts.logger,
          onActivity: opts.onActivity,
        });
        liveSocket = undefined;
        if (outcome.kind === 'fatal') {
          throw outcome.error;
        }
        if (stopController.signal.aborted) {
          return;
        }
        // We connected AND served a full session before it closed; the next dial
        // starts fresh from the base delay (as the worker-tunnel worker resets
        // `backoff = base` after `connectAndServe()`). A connection that reaches the
        // serve loop is healthy proof the registry is up, so a clean close (1000), an
        // abrupt drop, or a 4003/4403 must reconnect promptly at the base instead of
        // carrying forward (and escalating) the delay accumulated by earlier
        // connect-retries.
        served = true;
        // The served connection closed; a routine recycle reconnects promptly,
        // any other close backs off.
        recycle = outcome.kind === 'recycle';
        retryReason =
          outcome.kind === 'recycle'
            ? 'server recycled the tunnel; reconnecting promptly'
            : 'connection closed';
      } catch (err) {
        liveSocket = undefined;
        if (err instanceof RunnerTunnelRejectedError) {
          // Fatal refusal: do not reconnect. Surface it out of `done`.
          throw err;
        }
        // A `RetryAfterBackoff` carries no info — `retryReason` was set at the
        // throw site; any other error is an unexpected serve-loop fault, retried.
        if (!(err instanceof RetryAfterBackoff)) {
          retryReason = errMessage(err);
        }
      }

      if (stopController.signal.aborted) {
        return;
      }
      await backoffSleep(sleep, delay, retryReason, opts.logger);
      // Reset to the base delay after a connection that actually SERVED (it proved
      // the registry reachable — as in the worker-tunnel worker) or after a routine
      // recycle (the server wants a prompt reconnect). Escalate only when this
      // attempt never reached the serve loop, i.e. a connect/upgrade failure that
      // keeps failing: double toward the cap so we don't hammer a down registry.
      delay = served || recycle ? initialDelay : Math.min(delay * 2, maxDelay);
    }
  })();

  // Keep the loop's rejection observable via `done` without surfacing it as an
  // unhandled rejection before the caller awaits.
  const done = loop;
  done.catch(() => {
    /* observed by the caller via `done`; swallow the unhandled-rejection noise */
  });

  return {
    done,
    async stop(): Promise<void> {
      stopController.abort();
      // Aborting the signal wakes a serve parked on receive() (via receiveOrAbort)
      // so it runs its teardown — flushing each WS channel's 1001 close over the
      // STILL-OPEN socket — and then `serveTunnelOnce` closes the socket itself and
      // the outer loop exits. Awaiting the loop is enough; we do not slam the socket
      // closed up front (that would race ahead of the teardown flush and drop the
      // channels' 1001 closes).
      try {
        await loop;
      } catch {
        // A fatal rejection that raced stop() is observable via `done`; stop()
        // resolves regardless so teardown is clean.
      } finally {
        // Safety net for the narrow window where a socket was connected but the
        // serve loop had not yet entered serveTunnelOnce (so it never closed it).
        // Idempotent + best-effort.
        liveSocket?.closeSocket(1000, 'runner shutdown');
      }
    },
  };
}

/**
 * Build the per-connect hello frame. Recompute the resume cursors (via factory
 * when set) and stamp the providers so a re-dial advertises the runner's latest
 * capability + cursor view.
 */
function buildHelloFrame(opts: ServeTunnelOptions): HelloFrame {
  let resumeCursors = opts.resumeCursors ?? {};
  if (opts.resumeCursorsFactory !== undefined) {
    try {
      resumeCursors = opts.resumeCursorsFactory();
    } catch (err) {
      opts.logger?.warn?.({ err }, 'resumeCursorsFactory failed; using static resumeCursors');
      resumeCursors = opts.resumeCursors ?? {};
    }
  }
  return {
    kind: FrameKind.Hello,
    runnerVersion: opts.runnerVersion,
    frameProtocolVersion: FRAME_PROTOCOL_VERSION,
    // Capability advertise: the providers ride the hello's harnesses field (the
    // field the registry's connect hook + runner listing read); the env types ride
    // the parallel envs field (advertise-only: the registry does not match on it).
    harnesses: [...(opts.providers ?? [])],
    envs: [...(opts.envs ?? [])],
    resumeCursors: { ...resumeCursors },
  };
}

/**
 * The outcome of one served connection.
 *
 *   - `{ kind: 'closed' }` — the socket closed (the caller reconnects with backoff);
 *   - `{ kind: 'recycle' }` — a routine ingress recycle (close 1001/1012), the
 *     caller reconnects PROMPTLY at the base delay;
 *   - `{ kind: 'fatal', error }` — a fatal close code (4001/4002/4004/4500), the
 *     caller exits with the rejection.
 */
type ServeOnceOutcome =
  | { kind: 'closed' }
  | { kind: 'recycle' }
  | { kind: 'fatal'; error: RunnerTunnelRejectedError };

/**
 * Internal sentinel thrown by the reconnect loop to skip the serve and go
 * straight to the shared backoff (e.g. when the upgrade itself failed, so there
 * is no socket to serve). Carries no payload — the caller already set the retry
 * reason at the throw site.
 */
class RetryAfterBackoff extends Error {
  constructor() {
    super('retry after backoff');
    this.name = 'RetryAfterBackoff';
  }
}

/**
 * Serve ONE accepted runner tunnel until it closes.
 *
 * Sends the hello, then pulls inbound frames off the socket seam: ping→pong,
 * request→dispatch (an independent task per reqId, response framed back),
 * request.cancel→cancel that task, ws.open/ws.frame/ws.close→tunneled WS channel.
 * On close (or the stop signal), every in-flight dispatch + channel task is
 * cancelled and drained. Returns a fatal rejection when the server closed with a
 * fatal code, else `undefined`.
 */
async function serveTunnelOnce(args: {
  socket: RunnerTunnelSocket;
  dispatcher: RequestDispatcher;
  hello: HelloFrame;
  signal: AbortSignal;
  logger: ServeTunnelLogger | undefined;
  onActivity: (() => void) | undefined;
}): Promise<ServeOnceOutcome> {
  const { socket, dispatcher, hello, signal, logger, onActivity } = args;
  const send = (frame: Frame): Promise<void> => socket.sendText(encodeFrame(frame));

  // Per-request dispatch tasks (reqId → controller) and per-channel WS state.
  const dispatchControllers = new Map<string, AbortController>();
  const dispatchTasks = new Map<string, Promise<void>>();
  const wsChannels = new Map<string, RunnerWsChannel>();

  try {
    // Send the hello INSIDE the try, and never let a failed send escape.
    //
    // The registry refuses a mis-bound / unauthenticated peer by closing the
    // ALREADY-UPGRADED socket immediately: `runner-tunnel.routes.ts` runs its
    // handshake gates (origin, token binding, owner fail-closed) synchronously in
    // the route handler and closes 4403/4004 there, BEFORE its first hello read.
    // Over a low-latency hop — a co-located registry, a loopback dev stack, CI —
    // that close frame can already be in this client's receive buffer when the
    // first send runs, and `ws` rejects it with
    // `WebSocket is not open: readyState 2 (CLOSING)`.
    //
    // Letting that rejection escape reports a GENERIC, RETRYABLE transport fault
    // and discards the close code entirely, so the runner reconnect-storms a
    // registry that can never accept it instead of exiting fatally — precisely
    // the outcome the fatal close-code set exists to prevent. The close code is
    // the real outcome here, so swallow the send fault and let the loop below
    // read and classify it. Progress is guaranteed: `ws` rejects a send only when
    // the socket is CLOSING/CLOSED or the write itself failed, and every one of
    // those is followed by a `close` event.
    try {
      await send(hello);
    } catch (err) {
      logger?.info?.(
        { err: errMessage(err) },
        'runner tunnel hello send failed; classifying by the close code instead',
      );
    }
    for (;;) {
      if (signal.aborted) {
        return { kind: 'closed' };
      }
      // Race the next inbound message against the stop signal. On `stop()` the
      // signal wins and we break to the `finally` teardown WHILE THE SOCKET IS
      // STILL OPEN — so each channel's teardown 1001 close can actually flush to
      // the registry before the socket is closed (the per-attach tasks are
      // cancelled before the socket closes, never after). A server-initiated close
      // still arrives as a `close` message below.
      const msg = await receiveOrAbort(socket, signal, logger);
      if (msg === undefined) {
        // The stop signal fired (or receive() failed) while parked on receive():
        // tear down gracefully.
        return { kind: 'closed' };
      }
      if (msg.type === 'close') {
        const code = msg.code;
        if (code !== undefined && FATAL_SERVER_CLOSE_CODES.has(code)) {
          return { kind: 'fatal', error: new RunnerTunnelRejectedError(`close code ${code}`) };
        }
        if (code !== undefined && TUNNEL_RECYCLE_CLOSE_CODES.has(code)) {
          return { kind: 'recycle' };
        }
        return { kind: 'closed' };
      }
      handleTunnelFrame(msg.data, {
        send,
        dispatcher,
        dispatchControllers,
        dispatchTasks,
        wsChannels,
        logger,
        onActivity,
      });
    }
  } finally {
    // Cancel + drain every in-flight dispatch and WS channel so a tunnel teardown
    // never leaves a task running against a dead socket. WS-channel teardown is
    // best-effort allowed to frame its 1001 close here; on a `stop()` the socket is
    // still open at this point (see receiveOrAbort), on a server-side drop it is
    // already gone and the 1001 is silently swallowed (benign — the registry
    // already saw the socket close).
    for (const controller of dispatchControllers.values()) {
      controller.abort();
    }
    for (const channel of wsChannels.values()) {
      channel.teardown();
    }
    await Promise.allSettled([...dispatchTasks.values()]);
    await Promise.allSettled([...wsChannels.values()].map((c) => c.task));
    // Drain done — now close the socket. On `stop()` this sends the clean 1000
    // close AFTER the channel teardown frames flushed; on a server-side close it is
    // an idempotent no-op. Owning the close here keeps the open-socket teardown
    // window correct regardless of who triggered the exit.
    socket.closeSocket(1000, 'runner shutdown');
  }
}

/**
 * Await the next inbound message, or resolve `undefined` when `signal` aborts
 * first (or the receive itself fails). Lets the serve loop wake from a parked
 * `receive()` on `stop()` WITHOUT the socket having been closed yet, so teardown
 * can flush channel close frames over the still-open socket. The abandoned
 * `receive()` promise resolves later (to the eventual socket close) and is
 * harmlessly ignored.
 */
function receiveOrAbort(
  socket: RunnerTunnelSocket,
  signal: AbortSignal,
  logger: ServeTunnelLogger | undefined,
): Promise<RunnerTunnelMessage | undefined> {
  if (signal.aborted) {
    return Promise.resolve(undefined);
  }
  return new Promise<RunnerTunnelMessage | undefined>((resolve) => {
    let settled = false;
    const onAbort = (): void => {
      if (settled) {
        return;
      }
      settled = true;
      resolve(undefined);
    };
    signal.addEventListener('abort', onAbort, { once: true });
    void socket
      .receive()
      .then((msg) => {
        if (settled) {
          return;
        }
        settled = true;
        signal.removeEventListener('abort', onAbort);
        resolve(msg);
      })
      .catch((err: unknown) => {
        // The seam's contract is that `receive` resolves (the production socket
        // funnels errors into a close message), but a rejecting implementation
        // must not become an unhandled rejection — that is a process-wide crash
        // under Node's default `--unhandled-rejections=throw`. Treat it as
        // end-of-tunnel: the serve loop returns `closed` and reconnects.
        if (settled) {
          return;
        }
        settled = true;
        signal.removeEventListener('abort', onAbort);
        logger?.warn?.({ err }, 'runner tunnel receive failed; ending the served connection');
        resolve(undefined);
      });
  });
}

/** The collaborators a single inbound frame is dispatched against. */
interface FrameContext {
  send(frame: Frame): Promise<void>;
  dispatcher: RequestDispatcher;
  dispatchControllers: Map<string, AbortController>;
  dispatchTasks: Map<string, Promise<void>>;
  wsChannels: Map<string, RunnerWsChannel>;
  logger: ServeTunnelLogger | undefined;
  /** Last-activity touch for real work frames (NOT ping); see {@link ServeTunnelOptions.onActivity}. */
  onActivity: (() => void) | undefined;
}

/**
 * Handle one server→runner tunnel frame. A malformed frame (bad JSON / unknown
 * kind / missing field) is logged + dropped, never crashing the tunnel.
 */
function handleTunnelFrame(raw: string, ctx: FrameContext): void {
  let frame: Frame;
  try {
    frame = decodeFrame(raw);
  } catch (err) {
    ctx.logger?.warn?.({ err }, 'runner received malformed tunnel frame; dropping');
    return;
  }
  switch (frame.kind) {
    case FrameKind.Ping:
      // A keepalive — NOT activity. Pong it without touching onActivity, so an
      // idle runner's lifecycle clock keeps running.
      //
      // The catch is load-bearing, not defensive noise: `send` writes through the
      // socket seam, and a `ws` write on a non-OPEN socket REJECTS. A ping and the
      // server's close land in the same TCP read on every routine recycle, so an
      // uncaught rejection here would be an unhandled rejection — a process-wide
      // crash under Node's default `--unhandled-rejections=throw`. Mirrors the
      // worker-tunnel's pre-attached rejection observer on its socket writes.
      void ctx.send({ kind: FrameKind.Pong, ts: frame.ts }).catch((err: unknown) => {
        ctx.logger?.warn?.({ err }, 'runner tunnel pong send failed; tunnel likely closing');
      });
      return;
    case FrameKind.Request:
      fireActivity(ctx);
      startDispatch(frame, ctx);
      return;
    case FrameKind.RequestCancel: {
      fireActivity(ctx);
      const controller = ctx.dispatchControllers.get(frame.id);
      if (controller !== undefined) {
        controller.abort();
      }
      return;
    }
    case FrameKind.WsOpen:
      fireActivity(ctx);
      startWsChannel(frame, ctx);
      return;
    case FrameKind.WsFrame: {
      fireActivity(ctx);
      const channel = ctx.wsChannels.get(frame.chId);
      if (channel === undefined) {
        ctx.logger?.warn?.({ chId: frame.chId }, 'runner dropping ws.frame for unknown channel');
        return;
      }
      channel.onInboundFrame(frame.data, frame.encoding ?? 'utf-8', ctx.logger);
      return;
    }
    case FrameKind.WsClose: {
      fireActivity(ctx);
      const channel = ctx.wsChannels.get(frame.chId);
      if (channel === undefined) {
        return;
      }
      channel.onInboundClose(frame.code ?? 1000, frame.reason ?? '');
      return;
    }
    default:
      // response.* / hello / pong are not server→runner work frames; ignore.
      return;
  }
}

/**
 * Fire the {@link ServeTunnelOptions.onActivity} last-activity touch for a real
 * work frame. A throwing hook is logged + swallowed so it never drops the frame.
 */
function fireActivity(ctx: FrameContext): void {
  if (ctx.onActivity === undefined) {
    return;
  }
  try {
    ctx.onActivity();
  } catch (err) {
    ctx.logger?.warn?.({ err }, 'runner tunnel onActivity hook threw; ignoring');
  }
}

/**
 * Spawn an independent dispatch task for one `request` frame and frame the
 * response back. The task is tracked by reqId so a later `request.cancel` aborts
 * it; it is removed on completion.
 */
function startDispatch(frame: RequestFrame, ctx: FrameContext): void {
  const controller = new AbortController();
  ctx.dispatchControllers.set(frame.id, controller);
  const task = dispatchRequest(frame, controller.signal, ctx.send, ctx.dispatcher, ctx.logger)
    .catch((err: unknown) => {
      ctx.logger?.warn?.({ err, reqId: frame.id }, 'runner tunnel dispatch task failed');
    })
    .finally(() => {
      ctx.dispatchControllers.delete(frame.id);
      ctx.dispatchTasks.delete(frame.id);
    });
  ctx.dispatchTasks.set(frame.id, task);
}

/**
 * Run one pushed `request` frame through the dispatcher and stream the response
 * back as `response.head` + N×`response.body` + `response.end`.
 *
 * Error handling, keyed on whether the head has already been framed:
 *   - the handler throws BEFORE the head goes out → synthesize a 500 head + an
 *     error body so the registry's request-side awaiter doesn't hang;
 *   - the body stream throws AFTER the head → the head already went out, so just
 *     end the response (no synthetic head rewrite);
 *   - either way, a `response.end` is always sent so the registry's body iterator
 *     completes.
 */
async function dispatchRequest(
  frame: RequestFrame,
  signal: AbortSignal,
  send: (frame: Frame) => Promise<void>,
  dispatcher: RequestDispatcher,
  logger: ServeTunnelLogger | undefined,
): Promise<void> {
  const body =
    frame.body !== null && frame.body !== undefined
      ? decodeBody(frame.body, frame.encoding ?? 'utf-8')
      : new Uint8Array(0);
  const request: DispatchRequestInput = {
    method: frame.method,
    path: frame.path,
    queryString: frame.queryString ?? '',
    headers: frame.headers ?? [],
    body,
  };

  let headSent = false;
  try {
    const response: DispatchResponse = await dispatcher.dispatch(request, signal);
    await send({
      kind: FrameKind.ResponseHead,
      id: frame.id,
      status: response.status,
      // Copy each pair into a fresh mutable tuple: the response headers are a
      // readonly view, but the frame's `headers` field is mutable HeaderPair[].
      headers: response.headers.map(([k, v]) => [k, v] as [string, string]),
    });
    headSent = true;
    const contentType = contentTypeOf(response.headers);
    for await (const chunk of response.body) {
      if (chunk.length > 0) {
        const [bodyStr, encoding] = encodeBody(chunk, contentType);
        await send({ kind: FrameKind.ResponseBody, id: frame.id, body: bodyStr, encoding });
      }
    }
    await send({ kind: FrameKind.ResponseEnd, id: frame.id });
  } catch (err) {
    if (!headSent) {
      // Crashed before head: surface a 500 so the registry's awaiter completes.
      logger?.warn?.({ err, reqId: frame.id }, 'runner dispatch failed before head');
      await send({
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status: 500,
        headers: [['content-type', 'application/json']],
      });
      await send({
        kind: FrameKind.ResponseBody,
        id: frame.id,
        body: '{"error":"runner_dispatch_failed"}',
        encoding: 'utf-8',
      });
    } else {
      // Crashed after head: it was already streaming — just end the response.
      logger?.warn?.({ err, reqId: frame.id }, 'runner dispatch stream ended early');
    }
    await send({ kind: FrameKind.ResponseEnd, id: frame.id });
  }
}

/** Pick the response body's content-type for encoding selection (utf-8 vs base64). */
function contentTypeOf(headers: ReadonlyArray<readonly [string, string]>): string {
  for (const [k, v] of headers) {
    if (k.toLowerCase() === 'content-type') {
      return v;
    }
  }
  // No content-type: treat as binary (base64) unless a handler declared text.
  return 'application/octet-stream';
}

// ── Tunneled WebSocket channels ──────────────────────────

/**
 * Spawn a per-channel dispatch task for one `ws.open`. The handler registered for
 * the path drives the {@link WsChannel}; absent a handler the channel is closed
 * with 1011. The channel is tracked by chId so `ws.frame` / `ws.close` route to
 * it and teardown cancels it.
 */
function startWsChannel(frame: WsOpenFrame, ctx: FrameContext): void {
  const dispatcher = ctx.dispatcher as Partial<RouteDispatcher>;
  const handler: WsHandler | undefined =
    typeof dispatcher.wsHandlerFor === 'function' ? dispatcher.wsHandlerFor(frame.path) : undefined;
  const channel = new RunnerWsChannel(frame.chId, frame.path, frame.queryString ?? '', ctx.send);
  ctx.wsChannels.set(frame.chId, channel);
  channel.run(handler, ctx.logger).finally(() => {
    ctx.wsChannels.delete(frame.chId);
  });
}

/** Inbound item the channel's message iterator yields, or a close/teardown signal. */
type WsInbound =
  | { kind: 'text'; data: string }
  | { kind: 'bytes'; data: Uint8Array }
  | { kind: 'close'; code: number; reason: string }
  | { kind: 'teardown' };

/**
 * The runner side of one tunneled WS attach.
 *
 * Bridges the registry's `ws.*` frames (pushed in via {@link onInboundFrame} /
 * {@link onInboundClose}) and the {@link WsChannel} surface a handler drives. The
 * handler's sends are framed back as `ws.frame` / `ws.close`; inbound frames feed
 * the handler's `messages()` iterator; a peer close (or tunnel teardown) ends it.
 */
class RunnerWsChannel {
  /** The handler's dispatch task, awaited on teardown. */
  task: Promise<void> = Promise.resolve();
  private readonly inbound: WsInbound[] = [];
  private waiter: ((item: WsInbound) => void) | undefined;
  private finished = false;
  /** Fires when the tunnel tore the channel down (vs the peer closing it). */
  private readonly teardownController = new AbortController();
  /**
   * Set true once {@link messages} ended because the TUNNEL tore down (not a peer
   * close). This is the iterating-handler signal; the authoritative
   * "was this a teardown?" check is {@link wasTornDown}, which ALSO consults
   * {@link teardownController} so a handler that never iterates {@link messages}
   * still gets the teardown 1001.
   */
  private tornDown = false;

  constructor(
    private readonly chId: string,
    readonly path: string,
    readonly queryString: string,
    private readonly send: (frame: Frame) => Promise<void>,
  ) {}

  /** Push an inbound text/binary frame from the registry onto the channel. */
  onInboundFrame(data: string, encoding: string, logger: ServeTunnelLogger | undefined): void {
    if (encoding === 'utf-8') {
      this.push({ kind: 'text', data });
      return;
    }
    if (encoding === 'base64') {
      // Strict, validating decode: Node's Buffer.from(_, 'base64') silently strips
      // non-base64 chars instead of failing, so a lenient decode would let a
      // corrupt PTY frame through as garbage bytes. strictBase64Decode rejects a
      // non-canonical payload and we drop the frame.
      const decoded = strictBase64Decode(data);
      if (decoded === undefined) {
        logger?.warn?.({ chId: this.chId }, 'runner dropping ws.frame with malformed base64');
        return;
      }
      this.push({ kind: 'bytes', data: decoded });
      return;
    }
    logger?.warn?.({ chId: this.chId, encoding }, 'runner dropping ws.frame with unknown encoding');
  }

  /** Signal a peer-initiated close to the channel's message iterator. */
  onInboundClose(code: number, reason: string): void {
    this.push({ kind: 'close', code, reason });
  }

  /** Cancel the channel on tunnel teardown so its handler stops. */
  teardown(): void {
    if (!this.teardownController.signal.aborted) {
      this.teardownController.abort();
    }
    this.push({ kind: 'teardown' });
  }

  private push(item: WsInbound): void {
    const waiter = this.waiter;
    if (waiter !== undefined) {
      this.waiter = undefined;
      waiter(item);
      return;
    }
    this.inbound.push(item);
  }

  private next(): Promise<WsInbound> {
    const queued = this.inbound.shift();
    if (queued !== undefined) {
      return Promise.resolve(queued);
    }
    return new Promise<WsInbound>((resolve) => {
      this.waiter = resolve;
    });
  }

  /**
   * Drive the attach through `handler`.
   *
   *   - no handler → close 1011 (`"no handler for ws attach"`);
   *   - handler throws → close 1011 (`"runner dispatch failed"`);
   *   - the TUNNEL tore the channel down (the serve loop's socket closed or
   *     `stop()` ran) → best-effort inform the peer with a 1001
   *     (`"runner shutdown"`) close. The 1001 is unconditional on teardown, so
   *     the teardown check here ({@link wasTornDown}) reads the
   *     {@link teardownController} directly — the 1001 fires for EVERY handler
   *     shape on teardown, including one that parked on {@link accept} or other
   *     async work and never iterated {@link messages};
   *   - a clean return after a PEER close leaves the close to the handler (or the
   *     already-arrived peer close) — no 1001 is synthesized (a peer close never
   *     aborts {@link teardownController}).
   *
   * A handler that needs to react to teardown itself (flush / abort an in-flight
   * op before returning) reads {@link WsChannel.teardownSignal}; whether or not it
   * does, the wire-level teardown 1001 above still flows.
   */
  run(handler: WsHandler | undefined, logger: ServeTunnelLogger | undefined): Promise<void> {
    if (handler === undefined) {
      this.task = (async () => {
        await this.close(1011, 'no handler for ws attach');
      })();
      return this.task;
    }
    const channel: WsChannel = {
      path: this.path,
      queryString: this.queryString,
      teardownSignal: this.teardownController.signal,
      accept: () => this.accept(),
      messages: () => this.messages(),
      sendText: (text: string) =>
        this.send({ kind: FrameKind.WsFrame, chId: this.chId, data: text, encoding: 'utf-8' }),
      sendBytes: (bytes: Uint8Array) =>
        this.send({
          kind: FrameKind.WsFrame,
          chId: this.chId,
          data: Buffer.from(bytes).toString('base64'),
          encoding: 'base64',
        }),
      close: (code?: number, reason?: string) => this.close(code ?? 1000, reason ?? ''),
    };
    this.task = (async () => {
      try {
        await handler(channel);
        // The handler returned after the tunnel tore the channel down: inform the
        // peer with a 1001 close (unconditional on teardown — see wasTornDown). A
        // clean return after a peer close is left alone.
        if (this.wasTornDown()) {
          await this.close(1001, 'runner shutdown');
        }
      } catch (err) {
        logger?.warn?.({ err, chId: this.chId }, 'runner ws attach handler failed');
        // Teardown closes with 1001 ("going away"); any other handler fault is 1011.
        if (this.wasTornDown()) {
          await this.close(1001, 'runner shutdown');
        } else {
          await this.close(1011, 'runner dispatch failed');
        }
      }
    })();
    return this.task;
  }

  /**
   * Whether this channel ended because the TUNNEL tore it down (vs the peer
   * closing it). True when EITHER the {@link messages} iterator observed the
   * pushed teardown item ({@link tornDown}) OR the {@link teardownController} fired
   * — so a handler that never iterated {@link messages} (parked on {@link accept}
   * or other async work and unblocked via {@link WsChannel.teardownSignal}) still
   * counts as torn down and gets the unconditional teardown 1001 close.
   */
  private wasTornDown(): boolean {
    return this.tornDown || this.teardownController.signal.aborted;
  }

  /** Accept the attach. A no-op on the wire (the tunnel is already open). */
  private async accept(): Promise<void> {
    // The tunneled attach is already accepted by the registry; nothing to send.
    await Promise.resolve();
  }

  /** Inbound messages until the peer closes or the tunnel tears the channel down. */
  private async *messages(): AsyncIterable<WsChannelMessage> {
    for (;;) {
      const item = await this.next();
      if (item.kind === 'text') {
        yield { kind: 'text', data: item.data };
        continue;
      }
      if (item.kind === 'bytes') {
        yield { kind: 'bytes', data: item.data };
        continue;
      }
      // 'close' (peer closed) or 'teardown' (tunnel dropped): end the iterator.
      // Record WHICH so run() can emit the 1001 close on a teardown but leave a
      // peer close alone.
      if (item.kind === 'teardown') {
        this.tornDown = true;
      }
      return;
    }
  }

  /** Close the channel toward the registry (idempotent). */
  private async close(code: number, reason: string): Promise<void> {
    if (this.finished) {
      return;
    }
    this.finished = true;
    try {
      await this.send({ kind: FrameKind.WsClose, chId: this.chId, code, reason });
    } catch {
      // best-effort close; the socket may already be gone.
    }
  }
}

// ── URL + headers + backoff ──────────────────────────────

/**
 * Build the runner tunnel WebSocket URL from the registry BASE url + runner id.
 *
 * Accepts an `http(s)://`, `ws(s)://`, or bare-host base and maps it to a
 * `ws(s)://<host>/v1/tunnels/runners/<runnerId>` URL — the PUBLIC path the
 * registry mounts the runner-tunnel route under. `https`/`wss` bases produce
 * `wss`; everything else produces `ws`. The runner id is URL-encoded into the
 * path segment.
 *
 * @throws Error when `base` is empty.
 */
export function buildTunnelUrl(base: string, runnerId: string): string {
  const trimmed = base.trim();
  if (trimmed.length === 0) {
    throw new Error('registryRunnerUrl must not be empty');
  }
  const noTrailingSlash = trimmed.replace(/\/+$/, '');
  const scheme =
    noTrailingSlash.startsWith('https') || noTrailingSlash.startsWith('wss') ? 'wss' : 'ws';
  const hostPart = noTrailingSlash.includes('://')
    ? (noTrailingSlash.split('://', 2)[1] ?? '')
    : noTrailingSlash;
  return `${scheme}://${hostPart}/v1/tunnels/runners/${encodeURIComponent(runnerId)}`;
}

/**
 * Build the WS upgrade headers: the binding token on the dedicated header + the
 * internal WS origin so the registry's CSWSH guard allows the handshake (the
 * runner is a first-party non-browser client).
 */
function buildConnectHeaders(bindingToken: string): Record<string, string> {
  return {
    Origin: INTERNAL_WS_ORIGIN,
    [RUNNER_TUNNEL_TOKEN_HEADER]: bindingToken,
  };
}

/** Refresh the binding token via the factory before a connect (fallback on failure). */
function refreshToken(
  current: string,
  factory: (() => string | null | undefined) | undefined,
  logger: ServeTunnelLogger | undefined,
): string {
  if (factory === undefined) {
    return current;
  }
  try {
    const fresh = factory();
    if (fresh !== null && fresh !== undefined && fresh.length > 0) {
      return fresh;
    }
  } catch (err) {
    logger?.warn?.({ err }, 'binding token refresh failed; using previous token');
  }
  return current;
}

/**
 * Try to refresh the binding token after a 401.
 *
 * @returns the fresh token when the factory produced one, or `null` when no
 *   factory is configured or the refresh failed — a `null` means the 401 cannot
 *   be retried (the caller treats it as a fatal refusal).
 */
function refreshTokenAfter401(
  factory: (() => string | null | undefined) | undefined,
  logger: ServeTunnelLogger | undefined,
): string | null {
  if (factory === undefined) {
    return null;
  }
  try {
    const fresh = factory();
    if (fresh !== null && fresh !== undefined && fresh.length > 0) {
      logger?.info?.({}, 'binding token refreshed after HTTP 401; retrying');
      return fresh;
    }
  } catch (err) {
    logger?.warn?.({ err }, 'binding token refresh failed after HTTP 401');
  }
  return null;
}

/** Sleep `delay` jittered, logging the retry reason. */
async function backoffSleep(
  sleep: (ms: number) => Promise<void>,
  delay: number,
  retryReason: string,
  logger: ServeTunnelLogger | undefined,
): Promise<void> {
  const jittered = delay * (1 + (Math.random() * 2 - 1) * RECONNECT_JITTER_FRACTION);
  logger?.info?.(
    { retryReason, delayMs: delay, jitteredMs: Math.round(jittered) },
    'runner tunnel disconnected; retrying',
  );
  await sleep(Math.max(0, jittered));
}

/** Default real-timer sleep. */
function defaultSleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function errMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Strictly decode a standard (non-URL-safe) base64 string, returning `undefined`
 * when the input is not valid base64.
 *
 * Node's `Buffer.from(s, 'base64')` is lenient — it silently drops any character
 * outside the base64 alphabet and tolerates bad padding — so it can never signal a
 * corrupt `ws.frame` payload. This re-encodes the decoded bytes and compares
 * against the input (both stripped of `=` padding, which the codec canonicalizes)
 * so any character the lenient decoder discarded makes the round-trip differ and
 * the frame is rejected.
 */
function strictBase64Decode(data: string): Uint8Array | undefined {
  const buf = Buffer.from(data, 'base64');
  // A non-empty input that decodes to zero bytes was all-garbage: reject.
  if (buf.length === 0) {
    return data.length === 0 ? new Uint8Array(0) : undefined;
  }
  // Round-trip check: the lenient decode silently drops invalid chars, so a
  // re-encode that doesn't match (ignoring `=` padding) means the input was not
  // clean base64.
  const reencoded = buf.toString('base64').replace(/=+$/, '');
  if (reencoded !== data.replace(/=+$/, '')) {
    return undefined;
  }
  return new Uint8Array(buf);
}
