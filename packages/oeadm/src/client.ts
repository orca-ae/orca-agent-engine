// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Thin HTTP client over the registry's Anthropic-compatible API. Mirrors the
// e2e-tests client shape (`packages/e2e-tests/src/client.ts`): `baseURL` from
// `ORCA_BASE_URL` (default the local `make stack-up` port), api key from
// `ORCA_API_KEY`, and the Anthropic-canonical `x-api-key` + `anthropic-beta`
// auth headers the registry's api-key middleware accepts.
//
// The CLI is a PURE CLIENT: every method here maps to an existing registry route
// (create environment/session, append events, tail the SSE stream). `fetch` is
// injectable so the interactive loop and each subcommand are unit tested against
// an in-process fake with no real stack.

import { parseSseStream, type SseFrame } from './sse.js';

/** The subset of the global `fetch` signature the client uses (injectable for tests). */
export type OrcaFetch = (input: string | URL, init?: RequestInit) => Promise<Response>;

/** Resolved client configuration. */
export interface ClientConfig {
  baseURL: string;
  apiKey: string;
}

/** The `anthropic-beta` flag the registry accepts (and otherwise ignores). */
const MANAGED_AGENTS_BETA = 'managed-agents-2026-04-01';
/**
 * `orca-beta` value sent on the transcript stream ONLY. See
 * {@link OrcaClient.stream} for why the stream needs it and why no other route
 * gets it; the registry only checks that the header is a non-empty string.
 */
const ORCA_BETA = '1';
/** Default registry base URL: the `make stack-up` port, matching e2e-tests. */
const DEFAULT_BASE_URL = 'http://localhost:8080';

/**
 * Resolve {@link ClientConfig} from the process environment. `ORCA_BASE_URL`
 * defaults to the local stack; `ORCA_API_KEY` is required (throws a guiding
 * error when unset, like the e2e client's `buildClient`). A trailing slash on
 * the base URL is trimmed so `${baseURL}${path}` never doubles a slash.
 */
export function resolveClientConfig(env: NodeJS.ProcessEnv = process.env): ClientConfig {
  const baseURL = (env['ORCA_BASE_URL'] ?? DEFAULT_BASE_URL).replace(/\/+$/, '');
  const apiKey = env['ORCA_API_KEY'] ?? '';
  if (!apiKey) {
    throw new Error(
      'ORCA_API_KEY env var is required. Export the api key for your workspace, e.g. ' +
        '`export ORCA_API_KEY=sk-...` (and optionally `ORCA_BASE_URL` for a remote stack).',
    );
  }
  return { baseURL, apiKey };
}

/**
 * Auth headers the registry's api-key middleware accepts. `x-api-key` is the
 * Anthropic-canonical header the registry gates on; `anthropic-beta` makes the
 * request fingerprint match a real Anthropic call (the registry ignores unknown
 * beta flags). Mirrors `authHeaders` in the e2e-tests client.
 */
export function authHeaders(cfg: ClientConfig): Record<string, string> {
  return {
    'x-api-key': cfg.apiKey,
    'anthropic-beta': MANAGED_AGENTS_BETA,
  };
}

/** Options for {@link OrcaClient.createEnvironment}. */
export interface CreateEnvironmentInput {
  name: string;
  /** Environment target; the CLI defaults this to `self_hosted` for the worker flow. */
  target?: string;
}

/** Environment as returned by `POST /v1/environments` (create echoes `env_key` once). */
export interface EnvironmentResponse {
  id: string;
  name: string;
  target: string | null;
  env_key?: string;
  [k: string]: unknown;
}

/** Options for {@link OrcaClient.createSession}. */
export interface CreateSessionInput {
  agentId: string;
  environmentId?: string;
}

/** Session as returned by the sessions routes (partial — the CLI reads `id`/`status`). */
export interface SessionResponse {
  id: string;
  status: string;
  [k: string]: unknown;
}

/** Options for {@link OrcaClient.stream}. */
export interface StreamOptions {
  /** Resume the tail from this cursor (SSE `from_cursor` query param). */
  fromCursor?: string;
  /** Restrict the tail to a subagent thread subpath. */
  subpath?: string;
  /** Abort signal to close the stream early. */
  signal?: AbortSignal;
}

/**
 * A thin client over the registry HTTP API. Construct with a resolved
 * {@link ClientConfig} and (optionally) an injected `fetch`; the default is the
 * global `fetch` (Node 22 / undici).
 */
export class OrcaClient {
  private readonly baseURL: string;
  private readonly cfg: ClientConfig;
  private readonly fetchImpl: OrcaFetch;

  constructor(cfg: ClientConfig, fetchImpl: OrcaFetch = globalThis.fetch.bind(globalThis)) {
    this.cfg = cfg;
    this.baseURL = cfg.baseURL;
    this.fetchImpl = fetchImpl;
  }

  /**
   * `POST /v1/environments` — returns the created environment incl. the
   * one-time `env_key`.
   *
   * `orca-beta` is REQUIRED here. The default create response is Anthropic's
   * `BetaEnvironment` projection, which has no key concept at all; the raw
   * `env_key` is an Orca extension the registry echoes only to an opted-in
   * caller. Without the header this returns a well-formed environment with no
   * key, and `envCreateCommand` — correctly — fails, because the key is
   * unrecoverable once the response is dropped.
   *
   * Scoped to this one request, like {@link OrcaClient.stream}'s: `orca-beta`
   * also switches id prefixing, the model shape, and toolset aliasing on other
   * routes, none of which this client wants.
   */
  async createEnvironment(input: CreateEnvironmentInput): Promise<EnvironmentResponse> {
    const body: Record<string, unknown> = { name: input.name };
    if (input.target !== undefined) body['target'] = input.target;
    return this.postJson<EnvironmentResponse>('/v1/environments', body, {
      'orca-beta': ORCA_BETA,
    });
  }

  /** `POST /v1/sessions` — creates a session bound to an agent (and optional environment). */
  async createSession(input: CreateSessionInput): Promise<SessionResponse> {
    const body: Record<string, unknown> = { agent_id: input.agentId };
    if (input.environmentId !== undefined) body['environment_id'] = input.environmentId;
    return this.postJson<SessionResponse>('/v1/sessions', body);
  }

  /** `GET /v1/sessions/:id` — fetch a session (used by `attach` to validate it exists). */
  async getSession(sessionId: string): Promise<SessionResponse> {
    return this.getJson<SessionResponse>(`/v1/sessions/${encodeURIComponent(sessionId)}`);
  }

  /**
   * `POST /v1/sessions/:id/events` with a single `user.message` — the wire shape
   * the harness drives a turn from (`textOf` reads the first text content part).
   */
  async postUserMessage(sessionId: string, text: string): Promise<void> {
    await this.postEvents(sessionId, [{ type: 'user.message', content: [{ type: 'text', text }] }]);
  }

  /**
   * `POST /v1/sessions/:id/events` with a `user.tool_confirmation` — the client's
   * allow/deny verdict for an in-flight gated tool call, keyed by `tool_use_id`
   * (the registry routes it to the runner's confirmation route rather than as a
   * new turn).
   */
  async postToolConfirmation(
    sessionId: string,
    toolUseId: string,
    result: 'allow' | 'deny',
    denyMessage?: string,
  ): Promise<void> {
    const event: Record<string, unknown> = {
      type: 'user.tool_confirmation',
      tool_use_id: toolUseId,
      result,
    };
    if (result === 'deny' && denyMessage !== undefined) event['deny_message'] = denyMessage;
    await this.postEvents(sessionId, [event]);
  }

  /** `POST /v1/sessions/:id/events` — append raw transcript events. */
  async postEvents(sessionId: string, events: Array<Record<string, unknown>>): Promise<void> {
    await this.postJson(`/v1/sessions/${encodeURIComponent(sessionId)}/events`, { events });
  }

  /**
   * `GET /v1/sessions/:id/events/stream` — tail the session's public transcript as
   * SSE, yielding typed frames. Long-lived: the generator runs until the stream
   * closes or `signal` aborts. The interactive loop consumes this and treats
   * `agent.turn_completed` as the turn boundary.
   *
   * `orca-beta` is REQUIRED here, and only here.
   *
   * The registry filters the default (Claude-compatible) stream to
   * `CLAUDE_SESSION_EVENT_TYPES` (`shouldEmitClaudeStreamFrame`, registry
   * `streaming/sse.ts`). That set is the documented Anthropic session-event
   * vocabulary and contains NEITHER of the two frames this loop is built on:
   * `agent.turn_completed` (its turn boundary) and `agent.requires_action` (its
   * tool-confirmation gate) are Orca runner signals, not Anthropic events.
   * Without the header the loop would never see a turn end and never prompt
   * allow/deny — every turn would drain to a dead stream and every gated tool
   * call would park forever.
   *
   * The header is scoped to this one request deliberately. `orca-beta` also
   * switches id prefixing (`toWireId`), the model shape (`modelToApi`) and
   * toolset aliasing on the OTHER routes, none of which this client wants; and
   * `from_cursor` is unaffected either way, because the resume cursor is read
   * from the SSE `id:` line, which the registry writes for beta and non-beta
   * clients alike.
   */
  async *stream(sessionId: string, opts: StreamOptions = {}): AsyncGenerator<SseFrame, void, void> {
    const url = new URL(
      `${this.baseURL}/v1/sessions/${encodeURIComponent(sessionId)}/events/stream`,
    );
    if (opts.fromCursor !== undefined) url.searchParams.set('from_cursor', opts.fromCursor);
    if (opts.subpath !== undefined) url.searchParams.set('subpath', opts.subpath);

    const init: RequestInit = {
      method: 'GET',
      headers: {
        ...authHeaders(this.cfg),
        'orca-beta': ORCA_BETA,
        accept: 'text/event-stream',
      },
    };
    if (opts.signal !== undefined) init.signal = opts.signal;

    const res = await this.fetchImpl(url, init);
    if (res.status !== 200 || res.body === null) {
      throw await httpError('GET', url.pathname, res);
    }
    yield* parseSseStream(res.body);
  }

  private async getJson<T>(path: string): Promise<T> {
    const res = await this.fetchImpl(`${this.baseURL}${path}`, {
      method: 'GET',
      headers: authHeaders(this.cfg),
    });
    if (!res.ok) throw await httpError('GET', path, res);
    return (await res.json()) as T;
  }

  private async postJson<T>(
    path: string,
    body: unknown,
    extraHeaders: Record<string, string> = {},
  ): Promise<T> {
    const res = await this.fetchImpl(`${this.baseURL}${path}`, {
      method: 'POST',
      headers: {
        ...authHeaders(this.cfg),
        'content-type': 'application/json',
        ...extraHeaders,
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw await httpError('POST', path, res);
    const text = await res.text();
    return (text.length > 0 ? JSON.parse(text) : {}) as T;
  }
}

/**
 * Build a descriptive error from a non-2xx response, surfacing the registry's
 * `{ error }` body when present so the operator sees the real cause (e.g. "agent
 * not found") rather than a bare status code.
 */
async function httpError(method: string, path: string, res: Response): Promise<Error> {
  let detail = '';
  try {
    const text = await res.text();
    if (text.length > 0) {
      try {
        const parsed = JSON.parse(text) as { error?: unknown };
        detail = typeof parsed.error === 'string' ? parsed.error : text;
      } catch {
        detail = text;
      }
    }
  } catch {
    // body already consumed / unreadable — status alone is the signal.
  }
  const suffix = detail ? `: ${detail}` : '';
  return new Error(`${method} ${path} failed (${res.status})${suffix}`);
}
