// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// server.ts — state wiring + HTTP router for @orca/sandbox-harness.
//
// The HTTP layer. Exposes `createState` and `createApp` as library functions so
// the package entry (`index.ts`) can import them and call `.listen()`; this module
// itself never opens a socket.
//
// Two responsibilities live here and nowhere else:
//   1. createState — owns the session store, the (ephemeral, in-memory) event
//      store, and the live runtimes Map. Its `spawnManagedSession` closure wires
//      the per-session `emit` so that terminal events flip session status BEFORE
//      they are published. That ordering is the only thing that keeps
//      `GET /v1/sessions/:id` from reporting a stale "running".
//   2. createApp — builds the `node:http` server: read + JSON-parse the body,
//      match the load-bearing ROUTES table (single ":id" wildcard), dispatch,
//      and map thrown errors (HttpError -> its status, anything else -> 500).
//
// Durability is deliberately NOT handled here. The event store is an in-memory
// Map; the host that tails the SSE stream (harness-server's in-sandbox harness)
// is what persists events.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';

import { HttpError, sendError } from './core.js';
import {
  createEventStore,
  createSessionStore,
  type BareEvent,
  type EventStore,
  type SessionStatus,
  type SessionStore,
  type StoredEvent,
} from './store.js';
import {
  createManagedSession,
  type BareEvent as ManagerEvent,
  type ManagedSession,
  type SpawnArgs,
} from './session-manager.js';
import * as routes from './routes.js';

// ── request augmentation ──────────────────────────────────────────────────────

/**
 * The request, after the router has read and JSON-parsed any POST body. Handlers
 * read `req.body`; for GET requests it is left undefined.
 */
export interface HarnessRequest extends IncomingMessage {
  body?: unknown;
}

// ── shared context ─────────────────────────────────────────────────────────────

/** Path variables extracted from a matched route (only ":id" today). */
export type RouteParams = Record<string, string>;

/**
 * The shared context the router threads to every handler. Handlers never reach
 * into the runtimes Map directly — they go through `spawnManagedSession`,
 * `getRuntime`, and `deleteRuntime`, so the status-sync `emit` wiring is the
 * single source of truth for session lifecycle.
 */
export interface HarnessContext {
  readonly sessionStore: SessionStore;
  readonly eventStore: EventStore;
  /** Spawn (but do not yet `.start()`) the runtime for a session; registers it. */
  spawnManagedSession(sessionId: string, spawnArgs: SpawnArgs): ManagedSession;
  getRuntime(sessionId: string): ManagedSession | undefined;
  deleteRuntime(sessionId: string): boolean;
  /** Absolute path to the subprocess entry the runtime spawns per session. */
  readonly subprocessEntryPath: string;
  readonly env: NodeJS.ProcessEnv;
}

/** A route handler: `(req, res, ctx, params)`. */
export type RouteHandler = (
  req: HarnessRequest,
  res: ServerResponse,
  ctx: HarnessContext,
  params: RouteParams,
) => void | Promise<void>;

export interface CreateStateOptions {
  /** Absolute path to the per-session subprocess entry (the harness runner). */
  subprocessEntryPath: string;
  /** Environment handed to each spawned subprocess (LLM endpoint/key live here). */
  env: NodeJS.ProcessEnv;
}

/**
 * Adapt a session-manager / `core` event to the event store's `BareEvent`.
 *
 * `createManagedSession` hands `emit` a precise, closed discriminated-union event
 * (e.g. `SessionIdleEvent`); the store models a bare event as an open
 * `{ type; [k]: unknown }`. The event already IS a bare event — the only gap is
 * the index signature — so widen it here, in one place, exactly as the route
 * layer does, rather than scattering the conversion across call sites.
 */
function toBareEvent(event: { type: string }): BareEvent {
  return event as BareEvent;
}

/**
 * Build the shared context every handler receives.
 *
 * The `emit` passed into each runtime keeps session status in lockstep with
 * terminal events: a `session.status_idle` frame flips the record to "idle" and
 * a `session.status_error` frame to "error" — and it does so BEFORE the event is
 * published. A reader that observes the event over SSE and then immediately
 * `GET`s the session must never see a stale "running", so status mutation has to
 * happen first.
 */
export function createState({ subprocessEntryPath, env }: CreateStateOptions): HarnessContext {
  const sessionStore = createSessionStore();
  const eventStore = createEventStore();
  const runtimes = new Map<string, ManagedSession>();

  function spawnManagedSession(sessionId: string, spawnArgs: SpawnArgs): ManagedSession {
    const runtime = createManagedSession({
      sessionId,
      spawnArgs,
      subprocessEntryPath,
      env,
      emit(event: ManagerEvent): void {
        // Status sync THEN publish — see the function doc above.
        if (event.type === 'session.status_idle') {
          sessionStore.setStatus(sessionId, 'idle');
        } else if (event.type === 'session.status_error') {
          sessionStore.setStatus(sessionId, 'error');
        }
        eventStore.publish(sessionId, toBareEvent(event));
      },
    });
    runtimes.set(sessionId, runtime);
    return runtime;
  }

  return {
    sessionStore,
    eventStore,
    spawnManagedSession,
    getRuntime: (id) => runtimes.get(id),
    deleteRuntime: (id) => runtimes.delete(id),
    subprocessEntryPath,
    env,
  };
}

// ── router ──────────────────────────────────────────────────────────────────────

interface Route {
  readonly method: string;
  readonly pattern: string;
  readonly handler: RouteHandler;
}

/**
 * ROUTE ORDER IS LOAD-BEARING.
 *
 * `GET /v1/sessions/:id/events/stream` MUST precede `GET /v1/sessions/:id/events`,
 * and both MUST precede `GET /v1/sessions/:id`. The matcher walks this table top
 * to bottom and stops at the first method+arity match, so a more specific path
 * has to be listed before the wildcard that would otherwise swallow it. Reordering
 * these silently breaks streaming and event listing.
 */
const ROUTES: readonly Route[] = [
  { method: 'GET', pattern: '/healthz', handler: healthz },
  { method: 'GET', pattern: '/v1/harnesses', handler: routes.listHarnesses },
  { method: 'POST', pattern: '/v1/sessions', handler: routes.createSession },
  { method: 'GET', pattern: '/v1/sessions/:id/events/stream', handler: routes.streamEvents },
  { method: 'GET', pattern: '/v1/sessions/:id/events', handler: routes.listEvents },
  { method: 'POST', pattern: '/v1/sessions/:id/events', handler: routes.sendEvent },
  { method: 'POST', pattern: '/v1/sessions/:id/sdk-command', handler: routes.sdkCommand },
  { method: 'GET', pattern: '/v1/sessions/:id', handler: routes.getSession },
  { method: 'DELETE', pattern: '/v1/sessions/:id', handler: routes.deleteSession },
];

const segments = (path: string): string[] => path.split('/').filter(Boolean);

/**
 * Match a request path against a pattern containing zero or more ":name"
 * wildcards (today only ":id"). Returns the captured params, or `null` if the
 * pattern does not match.
 */
function matchRoute(pattern: string, path: string): { params: RouteParams } | null {
  const patternSegments = segments(pattern);
  const pathSegments = segments(path);
  if (patternSegments.length !== pathSegments.length) return null;

  const params: RouteParams = {};
  for (let i = 0; i < patternSegments.length; i += 1) {
    const patternSegment = patternSegments[i] as string;
    const pathSegment = pathSegments[i] as string;
    if (patternSegment.startsWith(':')) {
      params[patternSegment.slice(1)] = pathSegment;
    } else if (patternSegment !== pathSegment) {
      return null;
    }
  }
  return { params };
}

/**
 * Read and JSON-parse a request body, attaching it to `req.body`.
 *
 * Resolves `true` once the body is parsed (an empty body becomes `{}`), or
 * `false` if the body is not valid JSON — in which case a 400 has already been
 * written and the caller must stop. Bad JSON is rejected BEFORE routing.
 */
function readBody(req: HarnessRequest, res: ServerResponse): Promise<boolean> {
  return new Promise((resolveBody) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8').trim();
      if (!raw) {
        req.body = {};
        resolveBody(true);
        return;
      }
      try {
        req.body = JSON.parse(raw);
        resolveBody(true);
      } catch {
        sendError(res, 400, 'invalid JSON body');
        resolveBody(false);
      }
    });
    req.on('error', () => {
      sendError(res, 400, 'invalid JSON body');
      resolveBody(false);
    });
  });
}

/** GET /healthz — liveness probe. */
function healthz(_req: HarnessRequest, res: ServerResponse): void {
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ status: 'ok' }));
}

/**
 * Build the `node:http` server (not yet listening).
 *
 * Flow per request: parse the body for POSTs (bailing on bad JSON), walk the
 * load-bearing ROUTES table for the first method+path match, dispatch, and map
 * a thrown `HttpError` to its status while turning anything else into a 500.
 * An unmatched route is a 404.
 */
export function createApp(ctx: HarnessContext): Server {
  return createServer(async (req: HarnessRequest, res: ServerResponse) => {
    const urlPath = (req.url ?? '/').split('?')[0] as string;
    const method = req.method ?? 'GET';

    if (method === 'POST') {
      const ok = await readBody(req, res);
      if (!ok) return; // 400 already sent for bad JSON.
    }

    for (const route of ROUTES) {
      if (route.method !== method) continue;
      const match = matchRoute(route.pattern, urlPath);
      if (!match) continue;
      try {
        await route.handler(req, res, ctx, match.params);
      } catch (err) {
        if (err instanceof HttpError) {
          sendError(res, err.status, err.message);
        } else {
          console.error('[sandbox-harness] unhandled error:', err);
          sendError(res, 500, 'internal error');
        }
      }
      return;
    }

    sendError(res, 404, 'not found');
  });
}

// Re-export the types a thin entrypoint or test most often needs alongside the
// two factories, so consumers can import everything from `./server.js`.
export type { BareEvent, StoredEvent, SessionStatus, SpawnArgs, ManagedSession };
