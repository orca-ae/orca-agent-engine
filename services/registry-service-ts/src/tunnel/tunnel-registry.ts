// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Server-side registry of live runner WebSocket tunnels.
//
// The registry keeps an in-memory map of `runnerId` → live tunnel session. The
// tunnel HTTP transport looks up the session for a runner, sends a `request`
// frame, and reads response frames back via per-`reqId` reassembly queues
// managed here. Tunneled WebSocket channels (e.g. a terminal attach) ride the
// same socket and are reassembled through per-`chId` inbound queues.
//
// This module ships:
//   - `RegistrySession`: per-runner state (live WS, advertised capabilities,
//     last-frame-received time, owner, in-flight requests, ws channels, and the
//     outbound queue drained by the route's sender loop).
//   - `RequestState` / `WsChannelState`: per-(runner, reqId) and
//     per-(runner, chId) reassembly state.
//   - `RunnerConnectWaitState`: bounded, transient wait state for callers
//     blocked on a runner coming online.
//   - `TunnelRegistry`: the map of runnerId → session plus the routing surface.
//
// Lifecycle: `register` is called when a fresh tunnel opens; `deregister` is
// called on tunnel close. While registered, the server can `routeResponseFrame`
// to drop an incoming `response.*` frame into the right reassembly queue (and
// `routeWsInbound` for `ws.*` frames), while the transport calls
// `openRequest` / `closeRequest` to manage per-request lifecycle.
//
// Concurrency model: Node is single-threaded with one event loop, so registry
// mutations need no lock and waiter wakeups need no cross-loop hop — they are
// plain synchronous mutation plus microtask awaits. Waiter resolution, response
// reassembly, and channel delivery all run inline on the one loop. Every state
// field, edge case, and routing branch is preserved.

import {
  FrameKind,
  Deferred,
  AsyncQueue,
  BoundedResponseBodyQueue,
  type Frame,
  type HelloFrame,
  type ResponseBodyFrame,
  type ResponseHeadFrame,
  type RequestState as TunnelRequestState,
  type TunnelSession,
  type TransportRegistry,
  type WebSocketLike,
} from '@orca/harness-tunnel';

// ── WebSocket surface ────────────────────────────────────

/**
 * Minimal WebSocket surface the registry needs: text send + text receive, plus
 * a best-effort `close`.
 *
 * Both a real adapter around a Fastify/`ws` socket and the test fakes satisfy
 * it. Text-send + text-receive is the entire request/response surface — the
 * frame protocol is text-only JSON. `close` is optional: the registry calls it
 * best-effort when retiring a replaced or closed session, and skips it when the
 * underlying socket does not expose one.
 */
export interface RegistryWebSocketLike extends WebSocketLike {
  close?(opts?: { code?: number; reason?: string }): Promise<void> | void;
}

// ── Reassembly state ─────────────────────────────────────

/**
 * Per-(runner, reqId) reassembly state.
 *
 * The transport awaits `headFuture` for the response head, then drains
 * `bodyQueue` for body chunks (a `null` sentinel ends the stream); `endEvent`
 * signals end-of-response, and `abortedWith` carries an error to re-raise from
 * the body iterator after a tunnel disconnect or registry abort.
 *
 * Structurally compatible with the tunnel transport's `RequestState`, so the
 * same state object the registry allocates is what the transport consumes.
 */
export type RequestState = TunnelRequestState;

/**
 * Inbound channel-queue item shape:
 *   `["data", Uint8Array]`        — runner sent a binary WS frame
 *   `["text", string]`            — runner sent a text WS frame
 *   `["close", [number, string]]` — peer closed the channel (code, reason)
 *   `null`                        — local abort sentinel; the channel consumer
 *                                   should surface a connection-closed error.
 */
export type WsInboundItem =
  | ['data', Uint8Array]
  | ['text', string]
  | ['close', [number, string]]
  | null;

/**
 * Per-(runner, chId) state for a tunneled WebSocket attach.
 *
 * The consumer side of the tunnel pops items off `inboundQueue`; the receive
 * loop on its tunnel side pushes them. `session` is stored so cleanup can race
 * a session replacement without leaking onto the new generation.
 */
export interface WsChannelState {
  session: RegistrySession;
  inboundQueue: AsyncQueue<WsInboundItem>;
}

// ── Session ──────────────────────────────────────────────

/**
 * Per-runner state living in the registry while the tunnel is open.
 *
 * Extends the tunnel transport's `TunnelSession` (`runnerId` / `ws` / `hello` /
 * `inFlight`) with the registry-owned fields:
 *
 * - `outboundQueue`: queue consumed by the route's sender loop. Request-side
 *   code enqueues writes here instead of touching the socket directly, so all
 *   outbound frames serialize through one writer. A `null` is the stop sentinel.
 * - `connectedAt`: epoch ms of connect time.
 * - `lastFrameAt`: epoch ms of the most recent frame from this runner; updated
 *   on every receive — feeds the watchdog inactivity check.
 * - `owner`: authenticated user who established the tunnel, e.g.
 *   `"alice@example.com"`. `undefined` when auth is disabled (single-user
 *   mode). Used to enforce runner ownership.
 * - `wsChannels`: per-channel state for tunneled WebSocket attaches, keyed by
 *   channel id.
 */
export interface RegistrySession extends TunnelSession {
  ws: RegistryWebSocketLike;
  outboundQueue: AsyncQueue<string | null>;
  connectedAt: number;
  lastFrameAt: number;
  owner: string | undefined;
  wsChannels: Map<string, WsChannelState>;
}

/**
 * In-memory wait state for requests waiting on one runner to connect.
 *
 * The state exists only while at least one active request is waiting. It is
 * removed when the runner registers, when the final waiter times out, or when
 * the final waiter is cancelled.
 *
 * - `startedAt`: epoch ms when the first waiter for this runner id was
 *   registered.
 * - `waiters`: deferreds to resolve when the runner registers.
 */
export interface RunnerConnectWaitState {
  startedAt: number;
  waiters: Set<Deferred<RegistrySession>>;
}

/** Options for {@link TunnelRegistry}. */
export interface TunnelRegistryOptions {
  /**
   * Maximum active `waitForRunner` deferreds allowed for one runner id, e.g.
   * `1024`. Additional callers are not registered as event-driven waiters; they
   * wait for their timeout and do one final registry check instead, so the
   * waiter map cannot grow without bound under a burst. Defaults to `1024`.
   */
  maxConnectWaitersPerRunner?: number;
  /**
   * Maximum active `waitForRunner` deferreds allowed across all runner ids,
   * e.g. `8192`. Additional callers use the same bounded overflow path as the
   * per-runner cap. Defaults to `8192`.
   */
  maxConnectWaitersTotal?: number;
}

/**
 * Error raised when a request targets a runner that is not online. The message
 * is the runner id so callers can surface a connect-style failure keyed on the
 * runner that was missing.
 */
export class RunnerOfflineError extends Error {
  constructor(runnerId: string) {
    super(runnerId);
    this.name = 'RunnerOfflineError';
  }
}

/**
 * Error surfaced when a session has been replaced or closed mid-operation. This
 * is the connection-aborted signal awaiters re-raise after a tunnel teardown.
 *
 * The stable cross-component contract is the leading *substring* of each
 * message, which is what awaiters and tests match on:
 *   - `"tunnel closed before request completed"` (deregister abort);
 *   - `"runner ... tunnel was replaced"` (send on a replaced session);
 *   - `"tunnel replaced by newer connection"` (newest-wins abort).
 *
 * The newest-wins message carries a trailing `"(newest-wins)"` qualifier that is
 * informational only — it is not part of the matched contract. Nothing should
 * full-string-compare these messages; match the documented prefix instead.
 */
export class ConnectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

// ── Registry ─────────────────────────────────────────────

/**
 * In-memory map of `runnerId` → {@link RegistrySession}.
 *
 * The registry is rebuilt from scratch on server reboot — runners reconnect on
 * backoff and re-register via the WS endpoint that owns the registry's
 * lifecycle. It implements the tunnel transport's `TransportRegistry` seam, so a
 * `TunnelTransport` constructed against `(registry, runnerId)` drives this
 * object for every HTTP request it tunnels.
 */
export class TunnelRegistry implements TransportRegistry {
  private readonly sessions = new Map<string, RegistrySession>();
  private readonly connectWaits = new Map<string, RunnerConnectWaitState>();
  private readonly maxConnectWaitersPerRunner: number;
  private readonly maxConnectWaitersTotal: number;
  private connectWaiterTotal = 0;

  constructor(opts: TunnelRegistryOptions = {}) {
    const perRunner = opts.maxConnectWaitersPerRunner ?? 1024;
    const total = opts.maxConnectWaitersTotal ?? 8192;
    if (perRunner < 1) {
      throw new Error('maxConnectWaitersPerRunner must be at least 1');
    }
    if (total < 1) {
      throw new Error('maxConnectWaitersTotal must be at least 1');
    }
    this.maxConnectWaitersPerRunner = perRunner;
    this.maxConnectWaitersTotal = total;
  }

  // ── Session lifecycle ──────────────────────────────────

  /**
   * Add a new session.
   *
   * Newest wins: if a session already exists for the same `runnerId` (from a
   * previous tunnel that lagged on cleanup), the OLD one is discarded and the
   * new one replaces it. Any in-flight requests on the old session are aborted
   * with a {@link ConnectionError} so awaiters get a clean failure rather than
   * hanging. Any callers blocked in {@link waitForRunner} for this runner are
   * resolved with the new session.
   */
  register(
    runnerId: string,
    ws: RegistryWebSocketLike,
    hello: HelloFrame,
    opts: { owner?: string } = {},
  ): RegistrySession {
    const now = Date.now();
    const session: RegistrySession = {
      runnerId,
      ws,
      hello,
      inFlight: new Map<string, RequestState>(),
      outboundQueue: new AsyncQueue<string | null>(),
      connectedAt: now,
      lastFrameAt: now,
      owner: opts.owner,
      wsChannels: new Map<string, WsChannelState>(),
    };

    const old = this.sessions.get(runnerId);
    if (old !== undefined) {
      this.sessions.delete(runnerId);
      abortSessionInFlight(
        old,
        new ConnectionError('tunnel replaced by newer connection (newest-wins)'),
      );
    }
    this.sessions.set(runnerId, session);
    const waitState = this.connectWaits.get(runnerId);
    if (waitState !== undefined) {
      this.connectWaits.delete(runnerId);
      this.connectWaiterTotal -= waitState.waiters.size;
    }

    if (old !== undefined) {
      retireSessionWriter(old, 4000, 'tunnel replaced');
    }
    if (waitState !== undefined) {
      for (const waiter of [...waitState.waiters]) {
        resolveConnectWaiter(waiter, session);
      }
    }
    return session;
  }

  /**
   * Remove a session and abort all its in-flight requests.
   *
   * Called by the WS route handler when the tunnel closes for any reason (clean
   * shutdown, network error, etc.). The abort ensures awaiters of in-flight
   * requests don't hang — they get a {@link ConnectionError} and propagate it.
   *
   * @param session Optional generation guard. When provided, deregistration
   *   only removes the registry entry if the current entry is this exact session
   *   object. This prevents stale route handlers from deleting a newer tunnel.
   * @returns The removed session, or `undefined` when the runner is already
   *   offline or the guard did not match.
   */
  deregister(runnerId: string, session?: RegistrySession): RegistrySession | undefined {
    const current = this.sessions.get(runnerId);
    if (current === undefined || (session !== undefined && current !== session)) {
      return undefined;
    }
    this.sessions.delete(runnerId);
    abortSessionInFlight(current, new ConnectionError('tunnel closed before request completed'));
    retireSessionWriter(current, 4003, 'tunnel closed');
    return current;
  }

  /** Return the session for a `runnerId`, or `undefined` if not online. */
  get(runnerId: string): RegistrySession | undefined {
    return this.sessions.get(runnerId);
  }

  /**
   * Wait until a runner registers or the timeout expires.
   *
   * This is the event-driven counterpart to repeatedly calling {@link get}. The
   * waiter state is bounded and transient: each waiter is removed on
   * timeout/cancellation, and {@link register} removes the whole state for the
   * runner id before resolving the waiters.
   *
   * @param timeoutS Maximum seconds to wait, e.g. `3.0`.
   * @returns The registered session, or `undefined` if the runner did not
   *   connect before the timeout.
   */
  async waitForRunner(
    runnerId: string,
    opts: { timeoutS: number },
  ): Promise<RegistrySession | undefined> {
    const timeoutS = opts.timeoutS;
    if (timeoutS <= 0) {
      return this.get(runnerId);
    }

    const future = new Deferred<RegistrySession>();
    let overflowReason: 'per-runner' | 'global' | undefined;

    const current = this.sessions.get(runnerId);
    if (current !== undefined) {
      return current;
    }
    let state = this.connectWaits.get(runnerId);
    if (state !== undefined && state.waiters.size >= this.maxConnectWaitersPerRunner) {
      overflowReason = 'per-runner';
    } else if (this.connectWaiterTotal >= this.maxConnectWaitersTotal) {
      overflowReason = 'global';
    } else {
      if (state === undefined) {
        state = { startedAt: Date.now(), waiters: new Set<Deferred<RegistrySession>>() };
        this.connectWaits.set(runnerId, state);
      }
      state.waiters.add(future);
      this.connectWaiterTotal += 1;
    }

    if (overflowReason !== undefined) {
      // Overflow path: don't register an event-driven waiter. Sleep out the
      // timeout and do one final registry check, so the waiter map cannot grow
      // without bound under a burst.
      await sleep(timeoutS * 1000);
      return this.get(runnerId);
    }

    try {
      return await waitWithTimeout(future, timeoutS * 1000);
    } finally {
      const liveState = this.connectWaits.get(runnerId);
      if (liveState !== undefined && liveState.waiters.has(future)) {
        liveState.waiters.delete(future);
        this.connectWaiterTotal -= 1;
        if (liveState.waiters.size === 0) {
          this.connectWaits.delete(runnerId);
        }
      }
    }
  }

  /**
   * Return the number of active runner-connect waiters. Intended for tests and
   * diagnostics only.
   *
   * @param runnerId Optional runner id to inspect. When omitted, returns the
   *   total waiter count across all runner ids.
   */
  connectWaiterCount(runnerId?: string): number {
    if (runnerId !== undefined) {
      const state = this.connectWaits.get(runnerId);
      return state === undefined ? 0 : state.waiters.size;
    }
    return this.connectWaiterTotal;
  }

  /**
   * Cancel every active runner-connect waiter for `runnerId`.
   *
   * Resolves each pending {@link waitForRunner} deferred for this runner id with
   * `undefined` — the same value a waiter sees on timeout — and drops the whole
   * wait state, so each waiter's own `finally` finds it already removed and the
   * total count is decremented exactly once. This is the cleanup the session
   * distributor runs when a launch it pre-registered a waiter for can no longer
   * produce a runner (the worker refused the launch, or its tunnel was replaced
   * before the launch frame left): the expected runner will never dial, so the
   * waiter must not linger until its full timeout.
   *
   * A no-op when no waiter is registered for `runnerId`.
   *
   * @returns The number of waiters cancelled.
   */
  cancelConnectWaiters(runnerId: string): number {
    const state = this.connectWaits.get(runnerId);
    if (state === undefined) {
      return 0;
    }
    this.connectWaits.delete(runnerId);
    this.connectWaiterTotal -= state.waiters.size;
    const cancelled = state.waiters.size;
    for (const waiter of [...state.waiters]) {
      if (!waiter.done) {
        // Resolve with `undefined` (the timeout value) rather than reject, so a
        // caller awaiting the wait observes a clean "no runner" outcome and the
        // promise never surfaces as an unhandled rejection.
        waiter.resolve(undefined as unknown as RegistrySession);
      }
    }
    return cancelled;
  }

  /**
   * Return when the current wait state for a runner id was created. Intended for
   * tests and diagnostics only.
   *
   * @returns Epoch ms for the first active waiter, or `undefined` when no
   *   request is currently waiting.
   */
  connectWaitStartedAt(runnerId: string): number | undefined {
    const state = this.connectWaits.get(runnerId);
    return state === undefined ? undefined : state.startedAt;
  }

  /**
   * Insertion-ordered list of currently-online `runnerId`s. The order gives the
   * routing layer a deterministic round-robin without extra bookkeeping.
   */
  onlineRunnerIds(): string[] {
    return [...this.sessions.keys()];
  }

  /**
   * Return the owner of a registered runner, or `undefined` when the runner is
   * offline or was registered without an owner.
   */
  runnerOwner(runnerId: string): string | undefined {
    const session = this.sessions.get(runnerId);
    if (session === undefined) {
      return undefined;
    }
    return session.owner;
  }

  /**
   * Record that a frame arrived for `session`.
   *
   * @returns `true` if the session is still current, `false` if it has been
   *   replaced or deregistered.
   */
  markFrameSeen(session: RegistrySession): boolean {
    if (this.sessions.get(session.runnerId) !== session) {
      return false;
    }
    session.lastFrameAt = Date.now();
    return true;
  }

  /**
   * Return idle seconds for the current session generation.
   *
   * @returns Seconds since the last received frame, or `null` when `session` is
   *   stale.
   */
  secondsSinceLastFrame(session: RegistrySession): number | null {
    if (this.sessions.get(session.runnerId) !== session) {
      return null;
    }
    return (Date.now() - session.lastFrameAt) / 1000;
  }

  // ── Per-request lifecycle ──────────────────────────────

  /**
   * Allocate reassembly state for a new outgoing request.
   *
   * @throws {RunnerOfflineError} If the runner isn't online.
   * @throws {Error} If a request with this `reqId` is already in flight on this
   *   runner — req ids must be unique per session.
   */
  openRequest(runnerId: string, reqId: string): RequestState {
    const session = this.sessions.get(runnerId);
    if (session === undefined) {
      throw new RunnerOfflineError(runnerId);
    }
    if (session.inFlight.has(reqId)) {
      throw new Error(
        `req_id ${JSON.stringify(reqId)} already in flight on runner ${JSON.stringify(runnerId)}`,
      );
    }
    const state: RequestState = {
      reqId,
      session,
      headFuture: new Deferred<ResponseHeadFrame>(),
      bodyQueue: new BoundedResponseBodyQueue(),
      endEvent: new Deferred<void>(),
      abortedWith: undefined,
    };
    session.inFlight.set(reqId, state);
    return state;
  }

  /**
   * Drop reassembly state for a completed (or aborted) request.
   *
   * @param session Optional session object that owns the request. Allows
   *   stale-session cleanup after newest-wins replacement has removed the
   *   session from the registry.
   */
  closeRequest(runnerId: string, reqId: string, session?: TunnelSession): void {
    const target = session ?? this.sessions.get(runnerId);
    if (target === undefined) {
      return;
    }
    target.inFlight.delete(reqId);
  }

  /** Return whether a request is still in flight on `session`. */
  requestIsOpen(session: TunnelSession, reqId: string): boolean {
    return session.inFlight.has(reqId);
  }

  // ── WS channel lifecycle ───────────────────────────────

  /**
   * Allocate a per-channel state for a tunneled WS attach.
   *
   * @param session Optional generation guard. When provided, allocation only
   *   succeeds if the registry's current session for `runnerId` is this object —
   *   guards against the runner reconnecting between callsite decisions.
   * @throws {RunnerOfflineError} If the runner is offline or `session` is stale.
   * @throws {Error} If `chId` is already allocated.
   */
  openWsChannel(
    runnerId: string,
    chId: string,
    opts: { session?: RegistrySession } = {},
  ): WsChannelState {
    const current = this.sessions.get(runnerId);
    if (current === undefined || (opts.session !== undefined && current !== opts.session)) {
      throw new RunnerOfflineError(runnerId);
    }
    if (current.wsChannels.has(chId)) {
      throw new Error(
        `ws ch_id ${JSON.stringify(chId)} already open on runner ${JSON.stringify(runnerId)}`,
      );
    }
    const state: WsChannelState = {
      session: current,
      inboundQueue: new AsyncQueue<WsInboundItem>(),
    };
    current.wsChannels.set(chId, state);
    return state;
  }

  /**
   * Drop a channel from the registry.
   *
   * Idempotent: closing an unknown channel is a no-op so both sides can safely
   * call this on teardown without racing.
   *
   * @param session Optional session object that owns the channel — allows
   *   stale-session cleanup after a newest-wins replacement.
   */
  closeWsChannel(runnerId: string, chId: string, session?: RegistrySession): void {
    const target = session ?? this.sessions.get(runnerId);
    if (target === undefined) {
      return;
    }
    target.wsChannels.delete(chId);
  }

  /**
   * Push a WS data/close frame onto its channel inbound queue.
   *
   * @param session Optional session-generation guard. When set, frames from
   *   stale route handlers are ignored instead of routed into a newer session.
   * @returns `true` if the frame matched a known channel and was delivered;
   *   `false` for orphans, malformed payloads, or wrong-kind frames.
   */
  routeWsInbound(runnerId: string, frame: Frame, session?: RegistrySession): boolean {
    const current = this.sessions.get(runnerId);
    if (current === undefined || (session !== undefined && current !== session)) {
      return false;
    }
    current.lastFrameAt = Date.now();
    if (frame.kind !== FrameKind.WsFrame && frame.kind !== FrameKind.WsClose) {
      return false;
    }
    const channel = current.wsChannels.get(frame.chId);
    if (channel === undefined) {
      return false;
    }

    let item: WsInboundItem;
    if (frame.kind === FrameKind.WsClose) {
      item = ['close', [frame.code ?? 1000, frame.reason ?? '']];
    } else {
      const encoding = frame.encoding ?? 'utf-8';
      if (encoding === 'utf-8') {
        item = ['text', frame.data];
      } else if (encoding === 'base64') {
        let decoded: Uint8Array;
        try {
          decoded = decodeBase64Strict(frame.data);
        } catch {
          // Dropping frame with malformed base64.
          return false;
        }
        item = ['data', decoded];
      } else {
        // Dropping frame with unknown encoding.
        return false;
      }
    }

    channel.inboundQueue.put(item);
    return true;
  }

  /**
   * Enqueue one outbound WebSocket frame on the session's outbound queue.
   *
   * The frame is handed to the route's sender loop (which drains the queue to
   * the socket) rather than written to the socket directly, so every outbound
   * frame serializes through one writer.
   *
   * @throws {ConnectionError} If `session` is no longer the registry's current
   *   generation for its runner id.
   */
  async sendText(session: TunnelSession, data: string): Promise<void> {
    const registrySession = session as RegistrySession;
    if (this.sessions.get(session.runnerId) !== session) {
      throw new ConnectionError(`runner ${JSON.stringify(session.runnerId)} tunnel was replaced`);
    }
    registrySession.outboundQueue.put(data);
  }

  // ── Routing incoming frames ────────────────────────────

  /**
   * Route an incoming response frame to the right reassembly queue.
   *
   * @param session Optional session-generation guard. When set, frames from
   *   stale route handlers are ignored instead of routed into a newer session.
   * @returns `true` if the frame's `reqId` matches a tracked in-flight request;
   *   `false` otherwise (orphan frame — could be a late frame for a request that
   *   was already cancelled).
   */
  routeResponseFrame(runnerId: string, frame: Frame, session?: RegistrySession): boolean {
    const current = this.sessions.get(runnerId);
    if (current === undefined || (session !== undefined && current !== session)) {
      return false;
    }
    current.lastFrameAt = Date.now();
    if (
      frame.kind !== FrameKind.ResponseHead &&
      frame.kind !== FrameKind.ResponseBody &&
      frame.kind !== FrameKind.ResponseEnd
    ) {
      return false;
    }
    const reqId = frame.id;
    const state = current.inFlight.get(reqId);
    if (state === undefined) {
      return false;
    }
    if (frame.kind === FrameKind.ResponseHead) {
      setResponseHead(state, frame);
      return true;
    }
    if (frame.kind === FrameKind.ResponseBody) {
      enqueueResponseBody(state, frame);
      return true;
    }
    // ResponseEnd
    endResponseBody(state);
    return true;
  }

  // ── Observability ──────────────────────────────────────

  /** Number of currently-online runners. */
  get length(): number {
    return this.sessions.size;
  }

  /** Whether `runnerId` is currently online. */
  has(runnerId: string): boolean {
    return this.sessions.has(runnerId);
  }
}

// ── Module helpers ───────────────────────────────────────

/** Abort every in-flight request + ws channel on a session. */
function abortSessionInFlight(session: RegistrySession, error: ConnectionError): void {
  for (const state of [...session.inFlight.values()]) {
    abortRequestState(state, error);
  }
  session.inFlight.clear();
  for (const channel of [...session.wsChannels.values()]) {
    channel.inboundQueue.put(null);
  }
  session.wsChannels.clear();
}

/**
 * Stop a session's sender loop and best-effort close its socket.
 *
 * Pushes the `null` stop sentinel onto the outbound queue (so the sender loop
 * returns) and, when the socket exposes `close`, invokes it best-effort with the
 * given code/reason. Any close error is swallowed.
 */
function retireSessionWriter(session: RegistrySession, code: number, reason: string): void {
  session.outboundQueue.put(null);
  const ws = session.ws;
  if (typeof ws.close === 'function') {
    try {
      const result = ws.close({ code, reason });
      if (result instanceof Promise) {
        result.catch(() => {
          // best-effort close; swallow
        });
      }
    } catch {
      // best-effort close; swallow
    }
  }
}

/**
 * Abort one request: surface `error` to the head waiter and the body iterator.
 *
 * Sets `abortedWith` (checked by the body iterator after each dequeue), rejects
 * the head future if still pending, signals `endEvent`, and pushes the `null`
 * sentinel to unblock any pending `bodyQueue.get()`.
 */
function abortRequestState(state: RequestState, error: ConnectionError): void {
  state.abortedWith = error;
  if (!state.headFuture.done) {
    state.headFuture.reject(error);
  }
  // Wake the body iterator so it sees abortedWith.
  state.endEvent.resolve();
  // Sentinel-push to unblock any pending get().
  state.bodyQueue.end();
}

/** Resolve a request's response-head future. */
function setResponseHead(state: RequestState, frame: ResponseHeadFrame): void {
  if (!state.headFuture.done) {
    state.headFuture.resolve(frame);
  }
}

/** Resolve a runner-connect waiter. */
function resolveConnectWaiter(future: Deferred<RegistrySession>, session: RegistrySession): void {
  if (!future.done) {
    future.resolve(session);
  }
}

/** Append one body frame to the request's body queue. */
function enqueueResponseBody(state: RequestState, frame: ResponseBodyFrame): void {
  state.bodyQueue.tryPut(frame);
}

/** Signal response-body completion and push a sentinel to unblock the iterator. */
function endResponseBody(state: RequestState): void {
  state.endEvent.resolve();
  state.bodyQueue.end();
}

/** Standard base64 alphabet character → 6-bit value, for strict validation. */
const BASE64_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
const BASE64_INDEX: ReadonlyMap<string, number> = new Map(
  [...BASE64_ALPHABET].map((ch, i) => [ch, i] as const),
);

/**
 * Decode a base64 string with strict, fully-padded RFC 4648 validation.
 *
 * The malformed-base64 drop branch must reject any input that is not canonical,
 * fully-padded standard base64. `Buffer.from(.., 'base64')` is far too lenient
 * on its own — it silently ignores out-of-alphabet bytes (whitespace, `-`, `_`),
 * tolerates missing padding, and stops at the first `=` — so it cannot be used
 * directly. We validate the input ourselves, then hand the already-validated
 * string to `Buffer.from` for the actual decode.
 *
 * Strict rules enforced:
 *   - Only the standard alphabet `A-Za-z0-9+/` plus `=` padding is allowed; any
 *     other character — including whitespace — is rejected (no skipping).
 *   - Padding `=` may appear only as a contiguous 1- or 2-char run at the very
 *     end; leading, interior ("discontinuous"), or excess padding is rejected.
 *   - The total length (data + padding) must be a multiple of 4. In particular
 *     **unpadded but otherwise canonical** input such as `"QQ"` / `"AAE"` is
 *     rejected (incorrect padding), rather than being silently accepted.
 *   - A data-character count `≡ 1 (mod 4)` is impossible in base64 and is
 *     rejected ("…cannot be 1 more than a multiple of 4").
 */
function decodeBase64Strict(data: string): Uint8Array {
  if (data.length === 0) {
    return new Uint8Array(0);
  }

  // Count the trailing padding run, then ensure no `=` hides before it.
  let padCount = 0;
  while (padCount < data.length && data[data.length - 1 - padCount] === '=') {
    padCount += 1;
  }
  if (padCount > 2) {
    // 3+ padding chars (e.g. "====", "QQ===") never form a valid quantum.
    throw new Error('malformed base64: excess padding');
  }
  const dataLen = data.length - padCount;
  for (let i = 0; i < dataLen; i += 1) {
    const ch = data[i]!;
    if (!BASE64_INDEX.has(ch)) {
      // Out-of-alphabet byte (whitespace, '-', '_', interior '=', …).
      throw new Error('malformed base64: invalid character');
    }
  }
  if (dataLen % 4 === 1) {
    throw new Error('malformed base64: data length cannot be 1 mod 4');
  }
  // Full padding to a 4-char boundary is required; this is what makes unpadded
  // canonical input ("QQ", "AAE", "ABC") fail.
  if (data.length % 4 !== 0) {
    throw new Error('malformed base64: incorrect padding');
  }

  return new Uint8Array(Buffer.from(data, 'base64'));
}

/** Resolve after `ms` milliseconds. */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Await a deferred, resolving to `undefined` if `ms` elapses first.
 *
 * The timer is cleared once the deferred settles so a resolved waiter does not
 * keep the event loop alive. On timeout the caller's `finally` removes the
 * waiter from the registry.
 */
function waitWithTimeout(
  future: Deferred<RegistrySession>,
  ms: number,
): Promise<RegistrySession | undefined> {
  return new Promise<RegistrySession | undefined>((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      resolve(undefined);
    }, ms);
    future.promise.then(
      (session) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        resolve(session);
      },
      (err: unknown) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}
