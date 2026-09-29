// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The runner's request-dispatch seam.
//
// The registry PUSHES framed HTTP requests over the runner tunnel and the runner
// serves them like a local app — the runner is the request RESPONDER, not a
// requester. This module is the registration surface the serve loop dispatches
// into: the next unit registers a turn handler (`POST /v1/runner/turn`), a
// snapshot handler (`POST /v1/runner/snapshot`), and a replay handler
// (`POST /v1/runner/replay`) on a {@link RouteDispatcher}; the serve loop calls
// {@link RequestDispatcher.dispatch} for every inbound `request` frame and frames
// the returned response back as `response.head` + N×`response.body` +
// `response.end`.
//
// The seam is deliberately framework-free: a request is just
// (method, path, query, headers, body-bytes) and a response is just
// (status, headers, async-iterable body stream). That is the Orca-native analog
// of "the runner behaves like an app reached over the tunnel" — the registry owns
// the wire, this owns the routing + streaming contract, so handlers (turn /
// snapshot / replay) are unit-testable with no socket in sight.
//
// Tunneled WebSocket attaches (`ws.open` / `ws.frame` / `ws.close`) ride the same
// dispatcher via {@link RouteDispatcher.registerWebSocket}: a per-channel handler
// accepts the attach, pumps messages both ways, and closes — the serve loop
// translates between the channel and the `ws.*` frames.

/**
 * The raw request fields the serve loop hands to {@link RequestDispatcher.dispatch}
 * — the inbound `request` frame's method / path / query / headers / body bytes.
 * The dispatcher wraps this into a {@link DispatchRequest} (attaching the
 * case-insensitive {@link DispatchRequest.header} lookup) before invoking a
 * handler, so the serve loop never has to build the lookup itself.
 */
export interface DispatchRequestInput {
  /** HTTP method, e.g. `"POST"`. Case-insensitive on the wire. */
  method: string;
  /** Request path, e.g. `"/v1/runner/turn"`. */
  path: string;
  /** URL-encoded query string without the leading `?`. */
  queryString: string;
  /** Header pairs as they arrived on the frame (names lower-cased on the wire). */
  headers: ReadonlyArray<readonly [string, string]>;
  /** Request body bytes (empty for a bodyless request). */
  body: Uint8Array;
}

/**
 * A request the registry pushed for the runner to serve, as a handler sees it —
 * the raw {@link DispatchRequestInput} plus a case-insensitive header lookup.
 */
export interface DispatchRequest extends DispatchRequestInput {
  /**
   * Case-insensitive header lookup. The frame codec lower-cases header names on
   * the wire, so a handler asking for a canonical-case name (e.g.
   * `"X-Orca-Session-Id"`) still resolves it.
   */
  header(name: string): string | undefined;
}

/** The response a handler streams back for a dispatched request. */
export interface DispatchResponse {
  /** HTTP status code, e.g. `200`. */
  status: number;
  /** Response header pairs, in order. */
  headers: ReadonlyArray<readonly [string, string]>;
  /**
   * The response body as an async-iterable of byte chunks. The serve loop frames
   * each yielded chunk as one `response.body` and sends `response.end` once the
   * stream completes. A handler that throws BEFORE the first chunk surfaces a 500
   * (the loop synthesizes the error head); a throw AFTER the first chunk just
   * ends the response (the head already went out).
   */
  body: AsyncIterable<Uint8Array>;
}

/**
 * A request handler the runner registers for one `(method, path)`.
 *
 * Receives the dispatched request and an {@link AbortSignal} that fires when the
 * registry sends `request.cancel` for the request (or the tunnel drops). A
 * streaming handler should stop producing chunks when the signal aborts.
 */
export type RequestHandler = (
  request: DispatchRequest,
  signal: AbortSignal,
) => Promise<DispatchResponse>;

/** The dispatch surface the serve loop depends on. */
export interface RequestDispatcher {
  /**
   * Route + serve one pushed request. Unrouted paths resolve to a framed 404; a
   * registered handler runs and may throw (the serve loop maps a pre-head throw to
   * a 500 so it always has a head + body + end to frame back).
   *
   * @param request The pushed request's raw fields.
   * @param signal Optional cancellation signal wired to the registry's
   *   `request.cancel` for this request.
   */
  dispatch(request: DispatchRequestInput, signal?: AbortSignal): Promise<DispatchResponse>;
}

// ── Tunneled WebSocket channel seam ──────────────────────

/** One inbound message on a tunneled WS channel. */
export type WsChannelMessage = { kind: 'text'; data: string } | { kind: 'bytes'; data: Uint8Array };

/**
 * The runner side of one tunneled WebSocket attach, handed to a WS handler.
 *
 * `accept()` accepts the attach; `messages()` is an async-iterable of inbound
 * text/binary messages that ends when the peer closes; `sendText` / `sendBytes`
 * push frames back to the registry; `close` closes the channel. The serve loop
 * owns the `ws.*` framing — the handler only speaks this surface.
 */
export interface WsChannel {
  /** The runner-side path the attach targeted, e.g. `"/v1/runner/attach"`. */
  readonly path: string;
  /** The attach's URL-encoded query string (without the leading `?`). */
  readonly queryString: string;
  /**
   * Fires when the tunnel itself is torn down out from under the channel (the
   * serve loop's socket closed or `stop()` ran), as distinct from the peer
   * closing the channel with a `ws.close`. A handler with teardown-specific
   * cleanup (flush, abort an in-flight op) reads this; both a teardown and a peer
   * close still end {@link messages}, so a handler that only iterates `messages()`
   * needs no change. This is the per-attach cancellation surface.
   */
  readonly teardownSignal: AbortSignal;
  /** Accept the attach (the WS handshake). */
  accept(): Promise<void>;
  /** Inbound messages until the peer closes (or the channel is torn down). */
  messages(): AsyncIterable<WsChannelMessage>;
  /** Send a text frame back to the registry. */
  sendText(text: string): Promise<void>;
  /** Send a binary frame back to the registry. */
  sendBytes(bytes: Uint8Array): Promise<void>;
  /** Close the channel with a code + reason. */
  close(code?: number, reason?: string): Promise<void>;
}

/** A handler for one tunneled WS attach path. */
export type WsHandler = (channel: WsChannel) => Promise<void>;

/**
 * A `(method, path)`-keyed request router with a WS-attach registry.
 *
 * Register turn / snapshot / replay handlers (and any WS-attach handlers) up
 * front; the serve loop calls {@link dispatch} for each pushed request and
 * {@link wsHandlerFor} for each `ws.open`.
 */
export class RouteDispatcher implements RequestDispatcher {
  private readonly routes = new Map<string, RequestHandler>();
  private readonly wsRoutes = new Map<string, WsHandler>();

  /**
   * Register a handler for one `(method, path)`.
   *
   * @throws Error if a handler is already registered for the same key — a route
   *   collision is a wiring bug, not a runtime condition.
   */
  register(method: string, path: string, handler: RequestHandler): this {
    const key = routeKey(method, path);
    if (this.routes.has(key)) {
      throw new Error(`a handler is already registered for ${method.toUpperCase()} ${path}`);
    }
    this.routes.set(key, handler);
    return this;
  }

  /**
   * Register a handler for one tunneled WS-attach path.
   *
   * The `path` may be a STATIC path (e.g. `"/v1/runner/attach"`) or a template
   * carrying `:name` segments (e.g. `"attach/:terminalId"`). A concrete `ws.open`
   * path is matched against static routes first, then against templates
   * segment-by-segment ({@link wsHandlerFor}); a `:name` segment matches any single
   * non-empty segment. The concrete path stays on {@link WsChannel.path}, so a
   * handler recovers the bound segment(s) from it (the tunnel WS surface carries no
   * separate params object).
   *
   * @throws Error if a handler is already registered for the path (static or
   *   template, compared verbatim).
   */
  registerWebSocket(path: string, handler: WsHandler): this {
    if (this.wsRoutes.has(path)) {
      throw new Error(`a WebSocket handler is already registered for ${path}`);
    }
    this.wsRoutes.set(path, handler);
    return this;
  }

  /**
   * Return the WS-attach handler for a concrete `ws.open` path, or `undefined`
   * when none matches. A static route wins over a template; templates are tried in
   * registration order and matched segment-by-segment, where a `:name` segment
   * matches any single non-empty segment.
   */
  wsHandlerFor(path: string): WsHandler | undefined {
    const exact = this.wsRoutes.get(path);
    if (exact !== undefined) {
      return exact;
    }
    for (const [pattern, handler] of this.wsRoutes) {
      if (pattern.includes(':') && matchPathTemplate(pattern, path)) {
        return handler;
      }
    }
    return undefined;
  }

  async dispatch(request: DispatchRequestInput, signal?: AbortSignal): Promise<DispatchResponse> {
    const handler = this.routes.get(routeKey(request.method, request.path));
    if (handler === undefined) {
      return notFoundResponse();
    }
    const effectiveSignal = signal ?? new AbortController().signal;
    // Attach the case-insensitive header lookup the handler reads from.
    return handler(makeDispatchRequest(request), effectiveSignal);
  }
}

/**
 * Build a {@link DispatchRequest} from the raw frame fields, attaching the
 * case-insensitive header lookup. Used by the serve loop to adapt an inbound
 * `request` frame into the dispatch surface.
 */
export function makeDispatchRequest(args: {
  method: string;
  path: string;
  queryString: string;
  headers: ReadonlyArray<readonly [string, string]>;
  body: Uint8Array;
}): DispatchRequest {
  return {
    method: args.method,
    path: args.path,
    queryString: args.queryString,
    headers: args.headers,
    body: args.body,
    header(name: string): string | undefined {
      const target = name.toLowerCase();
      for (const [k, v] of args.headers) {
        if (k.toLowerCase() === target) {
          return v;
        }
      }
      return undefined;
    },
  };
}

/** The framed 404 returned for an unrouted path. */
function notFoundResponse(): DispatchResponse {
  const body = new TextEncoder().encode('{"error":"not_found"}');
  return {
    status: 404,
    headers: [['content-type', 'application/json']],
    body: (async function* () {
      yield body;
    })(),
  };
}

/** Compose the route map key from a method + path (method case-normalized). */
function routeKey(method: string, path: string): string {
  return `${method.toUpperCase()} ${path}`;
}

/**
 * Whether a concrete `path` matches a `:name`-segment `template`. Splits both on
 * `/`; the segment counts must match, a `:name` template segment matches any
 * single non-empty concrete segment, and a literal segment must be equal. Used by
 * {@link RouteDispatcher.wsHandlerFor} to route a concrete `ws.open` path to a
 * template handler.
 */
function matchPathTemplate(template: string, path: string): boolean {
  return matchWsPathParams(template, path) !== null;
}

/**
 * Match a concrete `path` against a `:name`-segment `template`, returning the
 * captured params (`{ name → segment }`) on a match, or `null` when it does not
 * match. Exported so a WS handler bound to a template recovers its bound segments
 * from the concrete {@link WsChannel.path} the same way the dispatcher matched it.
 */
export function matchWsPathParams(template: string, path: string): Record<string, string> | null {
  const t = splitSegments(template);
  const p = splitSegments(path);
  if (t.length !== p.length) {
    return null;
  }
  const params: Record<string, string> = {};
  for (let i = 0; i < t.length; i += 1) {
    const seg = t[i]!;
    const val = p[i]!;
    if (seg.startsWith(':')) {
      if (val.length === 0) {
        return null;
      }
      params[seg.slice(1)] = decodeSegment(val);
      continue;
    }
    if (seg !== val) {
      return null;
    }
  }
  return params;
}

/** Split a path into non-empty `/`-delimited segments (leading/trailing `/` ignored). */
function splitSegments(path: string): string[] {
  return path.split('/').filter((s) => s.length > 0);
}

/** Percent-decode one path segment, tolerating a malformed escape (returns it as-is). */
function decodeSegment(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}
