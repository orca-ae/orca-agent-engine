// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Server-side tunnel HTTP transport — adapts an HTTP request/response cycle onto
// a runner's tunnel WebSocket.
//
// Wire flow per request:
//   1. Allocate a fresh `reqId` (uuid hex).
//   2. Open reassembly state in the registry.
//   3. Send a `request` frame over the runner's WebSocket.
//   4. Await the `response.head` frame for status + headers.
//   5. Stream `response.body` chunks until `response.end` or session abort.
//   6. Close the request in the registry.
//
// If the runner is offline (no session in the registry) the transport raises
// `ConnectError` — the same surface an HTTP client would emit for a TCP connect
// failure, so call-site handling for "runner went away" is identical to "TCP
// connect refused." If the tunnel closes mid-request the abort propagates as the
// registry's abort error out of the body iterator.
//
// This module owns only the read/write framing over the socket: it inlines
// the body into a single `request` frame (request bodies here are tiny JSON),
// reassembles the response by draining a per-request BYTE-BOUNDED body queue
// (`put` never blocks, so an unbounded queue would let a fast runner grow the
// registry heap without limit against a slow consumer — overflow aborts the
// one request instead), and decodes each body chunk per its declared
// `encoding`. The registry collaborator owns the socket itself and the
// cross-loop wakeups; this transport is intentionally network-free so it can
// be unit-tested in full.

import {
  FrameKind,
  decodeBody,
  encodeBody,
  encodeFrame,
  type BodyEncoding,
  type HeaderPair,
  type HelloFrame,
  type ResponseBodyFrame,
  type ResponseHeadFrame,
} from './frames.js';

// ── Connect error ────────────────────────────────────────

/**
 * Raised when a request targets a runner that is not online.
 *
 * Mirrors the connect-refused surface of a real HTTP client: a caller that
 * already handles "the connection could not be established" needs no extra
 * branch for "the runner's tunnel is gone."
 */
export class ConnectError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message);
    this.name = 'ConnectError';
    // Attach the originating error (e.g. an offline race) when present.
    if (options?.cause !== undefined) {
      this.cause = options.cause;
    }
  }
}

// ── Async primitives ─────────────────────────────────────
//
// A one-shot result slot (`Deferred`) and an async FIFO (`AsyncQueue`). Kept
// small and dependency-free; the framing paths below are their only consumers.

/** A one-shot promise whose resolution is driven externally. */
export class Deferred<T> {
  readonly promise: Promise<T>;
  private settle!: (value: T) => void;
  private fail!: (reason: unknown) => void;
  private settled = false;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.settle = resolve;
      this.fail = reject;
    });
    // Avoid unhandled-rejection noise for deferreds that are rejected before
    // anyone awaits them (e.g. an abort that beats the head await).
    this.promise.catch(() => {
      // intentionally swallowed; real awaiters still observe the rejection.
    });
  }

  /** Whether this deferred has already been resolved or rejected. */
  get done(): boolean {
    return this.settled;
  }

  /** Resolve with `value`. Ignored if already settled (matches "if not done"). */
  resolve(value: T): void {
    if (this.settled) {
      return;
    }
    this.settled = true;
    this.settle(value);
  }

  /** Reject with `reason`. Ignored if already settled. */
  reject(reason: unknown): void {
    if (this.settled) {
      return;
    }
    this.settled = true;
    this.fail(reason);
  }
}

/**
 * Raised into the body iterator when a request's buffered response bytes exceed
 * {@link TUNNEL_MAX_BUFFERED_RESPONSE_BYTES}. A dedicated class so callers can
 * distinguish "the consumer was too slow for the runner's output" from a tunnel
 * disconnect.
 */
export class ResponseBufferOverflowError extends Error {
  constructor(maxBufferedBytes: number) {
    super(`tunneled response exceeded the ${maxBufferedBytes}-byte buffered limit`);
    this.name = 'ResponseBufferOverflowError';
  }
}

/**
 * Raised from {@link TunnelTransport.handleRequest} when the caller's
 * `AbortSignal` fires before (or while) the response head arrives.
 */
export class TunnelRequestAbortedError extends Error {
  constructor(message = 'tunneled request aborted by the caller', options?: { cause?: unknown }) {
    super(message);
    this.name = 'TunnelRequestAbortedError';
    // Preserve the signal's reason (e.g. AbortSignal.timeout's TimeoutError)
    // so shared handling code can tell a deadline from a client disconnect.
    if (options?.cause !== undefined) {
      this.cause = options.cause;
    }
  }
}

/** An unbounded async FIFO queue: `put` never blocks, `get` awaits an item. */
export class AsyncQueue<T> {
  private readonly items: T[] = [];
  private readonly waiters: Array<(value: T) => void> = [];

  /** Enqueue an item, waking the oldest waiting `get()` if one exists. */
  put(item: T): void {
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter(item);
      return;
    }
    this.items.push(item);
  }

  /** Dequeue the oldest item, awaiting one if the queue is empty. */
  get(): Promise<T> {
    if (this.items.length > 0) {
      return Promise.resolve(this.items.shift() as T);
    }
    return new Promise<T>((resolve) => {
      this.waiters.push(resolve);
    });
  }
}

/**
 * Default per-request cap on BUFFERED (produced-but-unconsumed) response bytes.
 * The protocol allows large WebSocket messages, and `AsyncQueue.put` never
 * blocks — without a cap, a runner streaming faster than the HTTP consumer
 * drains would grow the shared registry heap without bound (there is no
 * "natural backpressure": the producer is never slowed).
 */
export const TUNNEL_MAX_BUFFERED_RESPONSE_BYTES = 32 * 1024 * 1024;

/**
 * Byte-bounded FIFO for a single request's `response.body` frames.
 *
 * Accounting covers bytes currently BUFFERED: `tryPut` adds a frame's payload
 * size, `get` subtracts it — a steadily-draining consumer can stream far more
 * than the cap in total. "Bytes" are UTF-16 code units of the wire string
 * (`frame.body.length`): base64 overestimates decoded payload ~4/3
 * (conservative) and multibyte UTF-8 underestimates it, but buffered heap is
 * ~2x string length either way, so the heap bound the cap exists for holds
 * within a small constant. `tryPut` returns `false` once accepting the frame
 * would exceed the cap; the queue latches overflowed and drops every later
 * frame, so one slow consumer degrades one request, not the replica.
 *
 * The cap is per REQUEST, not a replica-wide budget — N concurrent slow
 * consumers can still hold N x cap — and a single frame larger than the cap
 * overflows immediately regardless of consumer speed. The registry constructs
 * the queue, so operators retune the limit there.
 */
export class BoundedResponseBodyQueue {
  private readonly queue = new AsyncQueue<ResponseBodyFrame | null>();
  private bufferedBytes = 0;
  private overflowed = false;

  constructor(private readonly maxBufferedBytes: number = TUNNEL_MAX_BUFFERED_RESPONSE_BYTES) {}

  /** The configured cap, so overflow diagnostics report the real limit. */
  get maxBytes(): number {
    return this.maxBufferedBytes;
  }

  /** Enqueue a body frame. `false` = cap exceeded (frame dropped, queue latched). */
  tryPut(frame: ResponseBodyFrame): boolean {
    if (this.overflowed) {
      return false;
    }
    if (this.bufferedBytes + frame.body.length > this.maxBufferedBytes) {
      this.overflowed = true;
      return false;
    }
    this.bufferedBytes += frame.body.length;
    this.queue.put(frame);
    return true;
  }

  /** Push the end-of-stream sentinel (always accepted — it wakes the consumer). */
  end(): void {
    this.queue.put(null);
  }

  /** Dequeue the next frame (or the `null` sentinel), releasing its bytes. */
  async get(): Promise<ResponseBodyFrame | null> {
    const item = await this.queue.get();
    if (item !== null) {
      this.bufferedBytes -= item.body.length;
    }
    return item;
  }

  get didOverflow(): boolean {
    return this.overflowed;
  }
}

// ── Registry seam ────────────────────────────────────────
//
// The transport's collaborator. It owns the live runner WebSocket and the
// per-request reassembly state; the transport drives it through this surface.

/**
 * Minimal WebSocket surface the tunnel needs: text send + text receive.
 *
 * Contract for implementers: `sendText` MUST be safe to call directly from the
 * registry's receive loop — {@link pushResponseBody} writes its overflow
 * `request.cancel` on `session.ws` without going through
 * {@link TransportRegistry.sendText} (the free function has no registry
 * handle). A registry that serializes socket writes must do that serialization
 * inside `sendText` itself, not only in its registry wrapper.
 */
export interface WebSocketLike {
  sendText(data: string): Promise<void>;
  receiveText(): Promise<string>;
}

/** Per-runner session state held while the tunnel is open. */
export interface TunnelSession {
  runnerId: string;
  ws: WebSocketLike;
  hello: HelloFrame;
  /** Per-`reqId` reassembly state for requests in flight on this session. */
  inFlight: Map<string, RequestState>;
}

/**
 * Per-(runner, request) reassembly state.
 *
 * The transport awaits `headFuture` for the response head, then drains
 * `bodyQueue` for body frames (a `null` is the end-of-stream sentinel).
 * `endEvent` signals end-of-response, and `abortedWith` carries the error to
 * re-raise from the body iterator: a tunnel disconnect / registry abort, a
 * buffered-byte overflow ({@link ResponseBufferOverflowError}), or a caller
 * abort ({@link TunnelRequestAbortedError}).
 */
export interface RequestState {
  session: TunnelSession;
  /** The request id this state reassembles — lets shared helpers emit `request.cancel`. */
  reqId: string;
  headFuture: Deferred<ResponseHeadFrame>;
  bodyQueue: BoundedResponseBodyQueue;
  endEvent: Deferred<void>;
  abortedWith: Error | undefined;
}

/**
 * Route one inbound `response.body` frame into its request's bounded queue.
 *
 * The registry's receive loop MUST deliver body frames through this helper
 * (never `bodyQueue` directly): on overflow it marks the request aborted with
 * {@link ResponseBufferOverflowError}, wakes the consumer (which re-raises the
 * error from the body iterator), and best-effort sends `request.cancel` so the
 * runner stops producing. Returns `'overflow'` so the caller can log and drop
 * its own correlation state.
 */
export function pushResponseBody(
  state: RequestState,
  frame: ResponseBodyFrame,
): 'buffered' | 'overflow' {
  if (state.bodyQueue.tryPut(frame)) {
    return 'buffered';
  }
  if (state.abortedWith === undefined) {
    // First overflow only: repeat deliveries after the latch are fully inert
    // (no duplicate end sentinels or cancel frames). The error reports the
    // queue's REAL cap, not the default constant.
    state.abortedWith = new ResponseBufferOverflowError(state.bodyQueue.maxBytes);
    state.bodyQueue.end();
    void state.session.ws
      .sendText(
        encodeFrame({ kind: FrameKind.RequestCancel, id: state.reqId, reason: 'buffer_overflow' }),
      )
      .catch(() => {
        // best-effort: the consumer-side abort is already latched.
      });
  }
  return 'overflow';
}

/** The registry operations the transport depends on. */
export interface TransportRegistry {
  /** Return the live session for `runnerId`, or `undefined` if offline. */
  get(runnerId: string): TunnelSession | undefined;
  /**
   * Allocate reassembly state for a new outgoing request.
   *
   * @throws if the runner is offline (a race after `get`) or if `reqId` is
   *   already in flight on this runner.
   */
  openRequest(runnerId: string, reqId: string): RequestState;
  /** Drop reassembly state for a completed or aborted request. Idempotent. */
  closeRequest(runnerId: string, reqId: string, session?: TunnelSession): void;
  /** Whether `reqId` is still in flight on `session`. */
  requestIsOpen(session: TunnelSession, reqId: string): boolean;
  /** Enqueue one outbound WebSocket frame on the session's owner. */
  sendText(session: TunnelSession, data: string): Promise<void>;
}

// ── Request / response shapes ────────────────────────────

/**
 * The HTTP request handed to the transport.
 *
 * The body is read up front as bytes; streaming request bodies would need a
 * multi-frame send, but every request here is tiny JSON, so the whole body
 * rides in the single `request` frame.
 */
export interface TunnelRequest {
  method: string;
  path: string;
  /** URL-encoded query string without the leading `?`. Defaults to `""`. */
  queryString?: string;
  /** Header pairs, in order. Defaults to `[]`. */
  headers?: HeaderPair[];
  /** Request body bytes, or absent for bodyless requests (e.g. GET). */
  body?: Uint8Array;
  /**
   * Convenience content-type. When set and no `content-type` header is already
   * present in `headers`, it is (a) the content-type used to choose the body
   * encoding and (b) synthesized into the emitted `request` frame's `headers`
   * as a `content-type` pair — so the runner, which rebuilds the request from
   * `frame.headers`, sees the content-type exactly as if the caller had passed
   * it as a header pair. The content-type therefore rides in the forwarded
   * header set rather than as an out-of-band hint.
   */
  contentType?: string;
  /**
   * Caller-side cancellation (e.g. the HTTP client disconnected, or a
   * deadline via `AbortSignal.timeout`). On abort the transport sends
   * `request.cancel`, fails the head wait with
   * {@link TunnelRequestAbortedError}, and marks the body stream aborted —
   * without this, a request whose head never arrives can be settled only by
   * tearing down the whole tunnel, and the caller holds neither a stream nor
   * the private reqId with which to cancel. An abort that lands after the head
   * resolves surfaces from the stream's next dequeue; the request slot is
   * released when the caller drains or closes the stream, same as any
   * response.
   */
  signal?: AbortSignal;
}

/** The HTTP response the transport returns: status, headers, and a body stream. */
export interface TunnelResponse {
  status: number;
  headers: HeaderPair[];
  /** Async-iterable body; draining it owns request cleanup. */
  stream: TunneledByteStream;
}

const EMPTY_BODY = new Uint8Array(0);

/** Find the caller-supplied `content-type` header value, if any. */
function explicitContentTypeHeader(request: TunnelRequest): string | undefined {
  for (const [name, value] of request.headers ?? []) {
    if (name.toLowerCase() === 'content-type') {
      return value;
    }
  }
  return undefined;
}

/** Read the request's `content-type`, falling back to `application/json`. */
function requestContentType(request: TunnelRequest): string {
  return explicitContentTypeHeader(request) ?? request.contentType ?? 'application/json';
}

/**
 * Build the header list emitted on the `request` frame.
 *
 * All caller-supplied headers are forwarded verbatim (the runner rebuilds the
 * request scope from `frame.headers`). When the `contentType` convenience field
 * is set and the caller did not already pass a `content-type` header, a
 * `content-type` pair is appended so the content-type reaches the runner in the
 * frame's headers — the same place an explicit header pair would land. Without
 * this, a caller using only `contentType` would emit a frame with no
 * content-type header, so the runner would lose the request's content-type.
 */
function requestFrameHeaders(request: TunnelRequest): HeaderPair[] {
  const headers: HeaderPair[] = [...(request.headers ?? [])];
  if (request.contentType !== undefined && explicitContentTypeHeader(request) === undefined) {
    headers.push(['content-type', request.contentType]);
  }
  return headers;
}

// ── Tunneled byte stream ─────────────────────────────────

/**
 * Adapts a request's body queue into an async-iterable byte stream.
 *
 * Iterating drains the queue: each `response.body` frame is decoded per its
 * `encoding` and yielded; a `null` sentinel ends the stream; an `abortedWith`
 * error (tunnel disconnect, buffered-byte overflow, or caller abort) is checked
 * after every dequeue and thrown before any further chunk is yielded — so a
 * chunk that was already queued when the abort landed is discarded rather than
 * delivered. Iteration always closes the request slot on completion, abort, or
 * early break.
 */
export class TunneledByteStream implements AsyncIterable<Uint8Array> {
  constructor(
    private readonly registry: TransportRegistry,
    private readonly runnerId: string,
    private readonly reqId: string,
    private readonly state: RequestState,
    /** Invoked exactly once when the stream releases the request (all paths). */
    private readonly onRelease?: () => void,
  ) {}

  private released = false;

  private release(): void {
    if (this.released) {
      return;
    }
    this.released = true;
    this.onRelease?.();
    this.registry.closeRequest(this.runnerId, this.reqId, this.state.session);
  }

  async *[Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    const state = this.state;
    let ended = false;
    let tunnelAborted = false;
    try {
      for (;;) {
        const item = await state.bodyQueue.get();
        if (state.abortedWith !== undefined) {
          tunnelAborted = true;
          throw state.abortedWith;
        }
        if (item === null) {
          // Sentinel: end-event signalled, no more chunks.
          ended = true;
          break;
        }
        yield decodeBody(item.body, item.encoding ?? 'utf-8');
      }
    } finally {
      // An ordinary `for await … break` (or a throw in the consumer) lands
      // here BEFORE the end sentinel: the runner is still executing the
      // request, and once the slot closes the correlation state — and with it
      // any chance to cancel — is gone. Send `request.cancel` first, exactly
      // like `close()`. A tunnel abort skips it: the socket is already dead
      // (and an overflow abort already sent its cancel).
      if (!ended && !tunnelAborted) {
        try {
          await this.registry.sendText(
            state.session,
            encodeFrame({
              kind: FrameKind.RequestCancel,
              id: this.reqId,
              reason: 'client_disconnected',
            }),
          );
        } catch {
          // best-effort cleanup
        }
      }
      this.release();
    }
  }

  /**
   * Close the request from the caller side — typically when the consumer stops
   * reading early (e.g. an SSE client disconnects). Translates into a
   * `request.cancel` frame so the runner aborts, then drops the request slot.
   */
  async close(): Promise<void> {
    const state = this.state;
    if (this.registry.requestIsOpen(state.session, this.reqId)) {
      try {
        await this.registry.sendText(
          state.session,
          encodeFrame({
            kind: FrameKind.RequestCancel,
            id: this.reqId,
            reason: 'client_disconnected',
          }),
        );
      } catch {
        // best-effort cleanup
      }
    }
    this.release();
  }
}

// ── Transport ────────────────────────────────────────────

/**
 * Tunnels each HTTP request through one runner's tunnel WebSocket.
 *
 * Construct one transport per (registry, runnerId) pair. The transport owns no
 * connections — the registry does; this object only marshals a request onto the
 * wire and reassembles the response.
 */
export class TunnelTransport {
  constructor(
    private readonly registry: TransportRegistry,
    private readonly runnerId: string,
  ) {}

  /** Send `request` over the tunnel and return the reassembled response. */
  async handleRequest(request: TunnelRequest): Promise<TunnelResponse> {
    if (request.signal?.aborted) {
      throw new TunnelRequestAbortedError(undefined, { cause: request.signal.reason });
    }
    const session = this.registry.get(this.runnerId);
    if (session === undefined) {
      // The runner is offline. Raising ConnectError matches what an HTTP client
      // would emit for a TCP connect failure.
      throw new ConnectError(`runner ${JSON.stringify(this.runnerId)} is offline`);
    }

    const reqId = newReqId();
    // Read the request body up front. v1 sends the whole body in the request
    // frame because all request bodies are tiny JSON.
    const body = request.body ?? EMPTY_BODY;
    const contentType = requestContentType(request);
    let bodyStr: string | null;
    let encoding: BodyEncoding;
    if (body.length > 0) {
      [bodyStr, encoding] = encodeBody(body, contentType);
    } else {
      bodyStr = null;
      encoding = 'utf-8';
    }

    let state: RequestState;
    try {
      state = this.registry.openRequest(this.runnerId, reqId);
    } catch (exc) {
      // A race: the runner went offline between get() and openRequest().
      throw new ConnectError(`runner ${JSON.stringify(this.runnerId)} is offline`, { cause: exc });
    }

    // Caller-side cancellation: without this listener, a request whose head
    // never arrives can settle ONLY when the runner answers or the whole
    // tunnel deregisters — a live runner answering pings could pin arbitrarily
    // many requests (and registry slots) forever, and the caller holds neither
    // the stream nor the private reqId needed to cancel. On abort: tell the
    // runner (`request.cancel`), fail the head wait, and mark the body stream
    // aborted so a mid-stream abort also surfaces on the next dequeue.
    const onAbort = (): void => {
      const abortErr = new TunnelRequestAbortedError(undefined, {
        cause: request.signal?.reason,
      });
      // Local teardown FIRST: nothing catches a throw from an abort listener,
      // so a `sendText` implementation that throws synchronously (instead of
      // rejecting) must not be able to skip the head-wait rejection — that
      // would re-create the exact hang this listener exists to prevent.
      if (state.abortedWith === undefined) {
        state.abortedWith = abortErr;
      }
      state.headFuture.reject(abortErr);
      state.bodyQueue.end();
      void this.registry
        .sendText(
          state.session,
          encodeFrame({ kind: FrameKind.RequestCancel, id: reqId, reason: 'client_disconnected' }),
        )
        .catch(() => {
          // best-effort: the local abort is already latched.
        });
    };
    request.signal?.addEventListener('abort', onAbort, { once: true });
    const removeAbortListener = (): void => {
      request.signal?.removeEventListener('abort', onAbort);
    };

    let head: ResponseHeadFrame;
    try {
      await this.registry.sendText(
        state.session,
        encodeFrame({
          kind: FrameKind.Request,
          id: reqId,
          method: request.method,
          path: request.path,
          queryString: request.queryString ?? '',
          headers: requestFrameHeaders(request),
          body: bodyStr,
          encoding,
          // Best-effort hint for streaming responses; not load-bearing on the
          // runner side.
          stream: true,
        }),
      );
      // Block until the response head arrives (or the tunnel/caller aborts).
      head = await state.headFuture.promise;
    } catch (exc) {
      // Failed before getting head — clean up the slot so we don't leak
      // in-flight state.
      removeAbortListener();
      this.registry.closeRequest(this.runnerId, reqId, state.session);
      throw exc;
    }

    // Wrap the body queue as a byte stream. The stream owns closeRequest() and
    // the abort-listener teardown: cleanup happens when the response iterator
    // finishes, aborts, or the consumer stops reading early.
    const stream = new TunneledByteStream(
      this.registry,
      this.runnerId,
      reqId,
      state,
      removeAbortListener,
    );
    return {
      status: head.status,
      headers: [...(head.headers ?? [])],
      stream,
    };
  }

  /**
   * Nothing to close — the transport doesn't own connections; the registry
   * does. Present so an HTTP client can call it without exploding.
   */
  async close(): Promise<void> {
    // no-op
  }
}

/** A fresh request id: 32 lowercase hex chars, like a dashless uuid4 hex. */
function newReqId(): string {
  return crypto.randomUUID().replace(/-/g, '');
}
