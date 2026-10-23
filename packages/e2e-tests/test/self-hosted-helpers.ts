// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Shared helpers for the self-hosted (colocated) e2e specs.
//
// Extracted from `self-hosted-e2e.spec.ts` when a second spec needed the same
// wiring. Everything here encodes a contract detail that is easy to get wrong and
// expensive to debug when you do — each one is commented with the failure it
// prevents, because "add the header" is not memorable and "without it the turn
// never appears to finish" is.
//
// The specs that use this differ only in what they REQUIRE:
//
//   self-hosted-e2e.spec.ts           a stack. Mock provider, no credential. CI default.
//   self-hosted-capabilities.spec.ts  a stack AND a real model credential.
//
// Both hard-fail rather than skip. `real-agent-loop.spec.ts` explains why in its own
// header: "silent skips were the failure mode that left nightly-e2b ineffective for
// months." A helper here that silently degraded would reintroduce exactly that.

import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect } from 'vitest';
import { apiCall, type OrcaClientConfig } from '../src/client.js';

const here = dirname(fileURLToPath(import.meta.url));

/** Where `start-self-hosted.sh` records the worker-attached environment. */
export const SELF_HOSTED_ENV_FILE = resolve(here, '../../../services/dev/run/self-hosted.env');

/**
 * The extended-wire header, required on every read that needs a RUNNER signal.
 *
 * The default stream and event list are filtered to `CLAUDE_SESSION_EVENT_TYPES` —
 * the documented Anthropic session-event vocabulary — and the frames these specs are
 * built on are not in it: `agent.turn_completed` (the turn boundary) and
 * `agent.requires_action` (the tool gate) are Orca runner signals, not Anthropic
 * events. Without the header the transcript ends at `agent.message` and the turn
 * never appears to finish — the client hangs rather than failing. The same header
 * surfaces `distribution_state` / `runner_id` / `host_environment_id` on a session,
 * which is how these specs prove a runner actually picked the work up.
 */
export const GUARDRAILS_PATH = '/apis/policy.runorca.ai/v1/guardrails';

export const ORCA_BETA_HEADERS = { 'orca-beta': 'true' } as const;

export interface EnvironmentResponse {
  id: string;
  /** Anthropic's `BetaEnvironment` projection — the DEFAULT wire. `cloud` vs `self_hosted`. */
  config?: { type?: string } | null;
  /** The Orca-only flat spelling, and the one-time key. Both `orca-beta`-gated. */
  target?: string | null;
  egress_mode?: string | null;
  env_key?: string;
}

export interface SessionResponse {
  id: string;
  status: string;
  environment_id: string | null;
  runner_id: string | null;
  host_environment_id: string | null;
  distribution_state: string | null;
}

export interface WorkStatsResponse {
  worker_connected: boolean;
  depth: number;
  in_flight: number;
}

export interface TranscriptEvent {
  type: string;
  seq?: number;
  content?: Array<{ type: string; text?: string }>;
  [key: string]: unknown;
}

/** An SSE frame after parsing the stream's `data:` line. */
export interface SseFrame {
  type: string;
  content?: Array<{ type: string; text?: string }>;
  [key: string]: unknown;
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * Read the worker-attached environment id the start script recorded.
 *
 * Fails LOUD with the fix, never skips: the self-hosted stack must be up for these
 * specs, exactly as the other Layer-B specs hard-require their stack rather than
 * silently passing.
 */
export function readWorkerAttachedEnvironmentId(): string {
  return readSelfHostedValue('ORCA_SELF_HOSTED_ENVIRONMENT_ID');
}

/** Reuse the stack's key; re-seeding rotates it and breaks the laptop example. */
export function readSelfHostedApiKey(): string {
  return readSelfHostedValue('ORCA_SELF_HOSTED_API_KEY');
}

function readSelfHostedValue(key: string): string {
  let raw: string;
  try {
    raw = readFileSync(SELF_HOSTED_ENV_FILE, 'utf8');
  } catch (err) {
    throw new Error(
      `self-hosted e2e: ${SELF_HOSTED_ENV_FILE} not found (${(err as Error).message}). ` +
        'Start the stack first: `bash services/dev/scripts/start-self-hosted.sh`.',
    );
  }
  for (const line of raw.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq <= 0) continue;
    if (trimmed.slice(0, eq).trim() === key) {
      const value = trimmed.slice(eq + 1).trim();
      if (value) return value;
    }
  }
  throw new Error(
    `self-hosted e2e: ${SELF_HOSTED_ENV_FILE} has no ${key}. ` +
      'Re-run `bash services/dev/scripts/start-self-hosted.sh`.',
  );
}

/**
 * Create an agent.
 *
 * Returns 200, not 201 — Anthropic's shape, declared as `200: Agent` in
 * `agents.contract.ts`. Asserting 201 would be asserting a shape this API does not have.
 *
 * `metadata.harness` + `metadata.mode` are the WHOLE of the engine selection. The
 * registry validates the pair on write, so an unsupported combination fails here with
 * a 400 rather than at the first turn.
 */
export async function createAgent(
  cfg: OrcaClientConfig,
  body: Record<string, unknown>,
): Promise<string> {
  const res = await apiCall(cfg, '/v1/agents', {
    method: 'POST',
    headers: { ...ORCA_BETA_HEADERS },
    body: JSON.stringify({ tools: [], mcp_servers: [], skills: [], ...body }),
  });
  expect(res.status, `create agent failed: ${res.status} ${res.text}`).toBe(200);
  return res.json<{ id: string }>().id;
}

/** The LLM-free deterministic harness: answers `mock: <user text>`, needs no credential. */
export async function createMockAgent(
  cfg: OrcaClientConfig,
  name: string,
  guardrailIds: string[] = [],
): Promise<string> {
  return createAgent(cfg, {
    name,
    // `model.provider` is the LLM provider field; the RUNNER provider (`mock`) comes
    // from the harness annotation. `mock` is a session-runner provider, and
    // session-runner is the sole `colocated` engine, so `colocated` is its only mode.
    model: { provider: 'mock', id: 'mock-1' },
    system: 'You are a deterministic mock agent.',
    metadata: { harness: 'mock', mode: 'colocated' },
    guardrail_ids: guardrailIds,
  });
}

export async function createRequestPiiGuardrail(
  cfg: OrcaClientConfig,
  name: string,
  piiTypes: string[],
): Promise<string> {
  const res = await apiCall(cfg, GUARDRAILS_PATH, {
    method: 'POST',
    body: JSON.stringify({
      name,
      scope: 'explicit',
      phases: ['request'],
      rule: {
        kind: 'builtin',
        builtin: 'deny_pii_in_llm_request',
        params: { pii_types: piiTypes },
      },
    }),
  });
  expect(res.status, `create guardrail failed: ${res.status} ${res.text}`).toBe(201);
  return res.json<{ id: string }>().id;
}

export async function updateRequestPiiGuardrail(
  cfg: OrcaClientConfig,
  guardrailId: string,
  piiTypes: string[],
): Promise<void> {
  const res = await apiCall(cfg, `${GUARDRAILS_PATH}/${guardrailId}`, {
    method: 'POST',
    body: JSON.stringify({
      rule: {
        kind: 'builtin',
        builtin: 'deny_pii_in_llm_request',
        params: { pii_types: piiTypes },
      },
    }),
  });
  expect(res.status, `update guardrail failed: ${res.status} ${res.text}`).toBe(200);
}

export async function createSelfHostedEnvironment(
  cfg: OrcaClientConfig,
  name: string,
): Promise<EnvironmentResponse> {
  const res = await apiCall(cfg, '/v1/environments', {
    method: 'POST',
    body: JSON.stringify({ name, target: 'self_hosted', egress_mode: 'sidecar' }),
  });
  expect(res.status, `create environment failed: ${res.status} ${res.text}`).toBe(200);
  return res.json<EnvironmentResponse>();
}

export async function createSession(
  cfg: OrcaClientConfig,
  agentId: string,
  environmentId: string,
  extra: Record<string, unknown> = {},
): Promise<SessionResponse> {
  const res = await apiCall(cfg, '/v1/sessions', {
    method: 'POST',
    // The create RESPONSE is read for `distribution_state` straight away, and that
    // field is `orca-beta`-gated like `runner_id` / `host_environment_id`. Without the
    // header the caller gets a session whose dispatch is invisible — not one that was
    // not dispatched.
    headers: { ...ORCA_BETA_HEADERS },
    body: JSON.stringify({ agent_id: agentId, environment_id: environmentId, ...extra }),
  });
  expect(res.status, `create session failed: ${res.status} ${res.text}`).toBe(200);
  return res.json<SessionResponse>();
}

export async function postUserMessage(
  cfg: OrcaClientConfig,
  sessionId: string,
  text: string,
): Promise<string> {
  const res = await apiCall(cfg, `/v1/sessions/${sessionId}/events`, {
    method: 'POST',
    body: JSON.stringify({
      events: [{ type: 'user.message', content: [{ type: 'text', text }] }],
      request_id: `self-hosted-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    }),
  });
  expect(res.status, `post user.message failed: ${res.status} ${res.text}`).toBe(200);
  return res.json<{ data: Array<{ id: string }> }>().data[0]!.id;
}

/** Append an arbitrary session event (interrupt, tool confirmation, custom tool result). */
export async function postSessionEvent(
  cfg: OrcaClientConfig,
  sessionId: string,
  event: Record<string, unknown>,
): Promise<void> {
  const res = await apiCall(cfg, `/v1/sessions/${sessionId}/events`, {
    method: 'POST',
    headers: { ...ORCA_BETA_HEADERS },
    body: JSON.stringify({
      events: [event],
      request_id: `self-hosted-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    }),
  });
  expect(res.status, `post ${String(event['type'])} failed: ${res.status} ${res.text}`).toBe(200);
}

export async function getSession(
  cfg: OrcaClientConfig,
  sessionId: string,
): Promise<SessionResponse> {
  const res = await apiCall(cfg, `/v1/sessions/${sessionId}`, {
    method: 'GET',
    headers: { ...ORCA_BETA_HEADERS },
  });
  expect(res.status).toBe(200);
  return res.json<SessionResponse>();
}

export async function getWorkStats(
  cfg: OrcaClientConfig,
  environmentId: string,
): Promise<WorkStatsResponse> {
  const res = await apiCall(cfg, `/v1/environments/${environmentId}/work_stats`, { method: 'GET' });
  expect(res.status).toBe(200);
  return res.json<WorkStatsResponse>();
}

/** Read the full persisted public transcript for a session (ordered by seq). */
export async function readEvents(
  cfg: OrcaClientConfig,
  sessionId: string,
): Promise<TranscriptEvent[]> {
  const res = await apiCall(cfg, `/v1/sessions/${sessionId}/events?limit=1000`, {
    method: 'GET',
    headers: { ...ORCA_BETA_HEADERS },
  });
  expect(res.status).toBe(200);
  // `data`, not `events`. This route returns `{ data, next_page }` in BOTH projections
  // — `orca-beta` adds `has_more`, it does not rename the array. The `events` spelling
  // belongs to the APPEND request body, not to any read.
  return res.json<{ data: TranscriptEvent[] }>().data;
}

/**
 * Poll `GET /events` until `predicate` holds or the timeout elapses.
 *
 * The events index is an asynchronously-maintained projection of the transcript, so a
 * read taken immediately after the live SSE tail showed an event can legitimately miss
 * it for a poll cycle. Returns the last page read either way, so a caller can assert
 * on it and get a useful diff rather than a bare timeout.
 */
export async function pollEventsUntil(
  cfg: OrcaClientConfig,
  sessionId: string,
  predicate: (events: TranscriptEvent[]) => boolean,
  timeoutMs: number,
): Promise<TranscriptEvent[]> {
  const deadline = Date.now() + timeoutMs;
  let last: TranscriptEvent[] = [];
  for (;;) {
    last = await readEvents(cfg, sessionId);
    if (predicate(last)) return last;
    if (Date.now() >= deadline) return last;
    await sleep(500);
  }
}

/**
 * Wait until the environment reports a live worker claim.
 *
 * Worth failing on explicitly: a session on an environment with no worker sits
 * `pending` FOREVER and raises nothing, because "waiting for a worker" is a legitimate
 * state. Without this the whole suite would fail as a pile of turn timeouts with no
 * indication that the cause was one missing process.
 */
export async function waitForWorkerConnected(
  cfg: OrcaClientConfig,
  environmentId: string,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const stats = await getWorkStats(cfg, environmentId);
    if (stats.worker_connected) return;
    if (Date.now() >= deadline) {
      throw new Error(
        `self-hosted e2e: no worker connected to ${environmentId} within ${timeoutMs}ms. ` +
          "Is start-self-hosted.sh's worker running? Check services/dev/logs/environment-worker.log.",
      );
    }
    await sleep(1_000);
  }
}

/**
 * Tail the SSE stream until `until(frames)` holds or the deadline passes.
 *
 * Streams `from_cursor=0` so the tail REPLAYS the transcript from the start rather than
 * tailing only what is appended after subscribing. A mock turn completes in a few
 * hundred milliseconds — faster than the subscribe round-trip — so a live-from-head
 * tail would race past the already-persisted frames.
 *
 * The path is `/events/stream`. There is no `/v1/sessions/:id/stream` on this branch.
 */
export async function collectSseFrames(
  cfg: OrcaClientConfig,
  sessionId: string,
  opts: { until: (frames: SseFrame[]) => boolean; deadlineMs: number },
): Promise<SseFrame[]> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), opts.deadlineMs + 5_000);
  try {
    const res = await fetch(`${cfg.baseURL}/v1/sessions/${sessionId}/events/stream?from_cursor=0`, {
      method: 'GET',
      headers: { 'x-api-key': cfg.apiKey, ...ORCA_BETA_HEADERS, accept: 'text/event-stream' },
      signal: ac.signal,
    });
    if (res.status !== 200) {
      throw new Error(`SSE handshake failed: ${res.status}`);
    }
    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buffered = '';
    const frames: SseFrame[] = [];
    const deadline = Date.now() + opts.deadlineMs;

    try {
      while (Date.now() < deadline) {
        let chunk: ReadableStreamReadResult<Uint8Array>;
        try {
          chunk = await reader.read();
        } catch {
          break;
        }
        if (chunk.done) break;
        buffered += decoder.decode(chunk.value, { stream: true });

        let idx: number;
        while ((idx = buffered.indexOf('\n\n')) >= 0) {
          const block = buffered.slice(0, idx);
          buffered = buffered.slice(idx + 2);
          if (block.startsWith(':')) continue; // heartbeat comment
          const dataLine = block.split('\n').find((l) => l.startsWith('data: '));
          if (!dataLine) continue;
          try {
            frames.push(JSON.parse(dataLine.slice(6)) as SseFrame);
          } catch {
            /* a frame we cannot parse is not worth killing the tail over */
          }
        }
        if (opts.until(frames)) break;
      }
    } finally {
      // Release the body, or the socket keeps the process alive after the tail ends.
      await reader.cancel().catch(() => {});
    }
    return frames;
  } finally {
    clearTimeout(timer);
    ac.abort();
  }
}

/**
 * Every text block of an event's `content`, joined.
 *
 * Tolerant of shape on purpose. `agent.message` carries the Anthropic content-block
 * array, but `agent.tool_result` carries whatever the tool returned — a bare string
 * for most, an object for some. A helper that assumed the array form threw
 * `(frame.content ?? []).filter is not a function` the first time it met a tool
 * result, which is a crash in the test harness masquerading as a product failure.
 */
export function textOf(frame: { content?: unknown }): string {
  const content = frame.content;
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return content == null ? '' : JSON.stringify(content);
  return content
    .map((part) => {
      if (typeof part === 'string') return part;
      if (part && typeof part === 'object') {
        const p = part as Record<string, unknown>;
        if (typeof p['text'] === 'string') return p['text'];
      }
      return '';
    })
    .join('');
}

/** All text across a set of frames of one kind, lowercased — for loose reply matching. */
export function joinedText(frames: SseFrame[], type = 'agent.message'): string {
  return frames
    .filter((f) => f.type === type)
    .map((f) => textOf(f))
    .join('\n')
    .toLowerCase();
}

export const TERMINAL_FRAMES = ['agent.turn_completed', 'agent.turn_failed'] as const;

/**
 * Fail with the harness's OWN error before asserting anything else.
 *
 * Surface provider errors before a missing-tool assertion hides the actual cause.
 */
export function assertNoTurnFailure(frames: SseFrame[]): void {
  const failure = frames.find((f) => f.type === 'agent.turn_failed' || f.type === 'agent.error');
  expect(failure, `agent failure: ${JSON.stringify(failure)}`).toBeUndefined();
}

/**
 * Post a turn and drain it to its terminal frame.
 *
 * Returns frames after the submitted user event through its correlated terminal.
 * Historical terminals and failures cannot finish or poison this turn.
 */
export async function runTurn(
  cfg: OrcaClientConfig,
  sessionId: string,
  prompt: string,
  deadlineMs: number,
): Promise<SseFrame[]> {
  const userEventId = await postUserMessage(cfg, sessionId, prompt);
  const completesTurn = (frame: SseFrame) =>
    frame.type === 'agent.turn_completed' &&
    !frame['subpath'] &&
    (frame['turn_event_id'] === userEventId || frame['source_event_id'] === userEventId);
  const turnFrames = (frames: SseFrame[]) => {
    const start = frames.findIndex((frame) => frame['id'] === userEventId);
    if (start < 0) return [];
    return frames.slice(start + 1).filter((frame) => {
      if (
        frame['subpath'] ||
        (!(TERMINAL_FRAMES as readonly string[]).includes(frame.type) &&
          frame.type !== 'agent.error')
      )
        return true;
      const source = frame['turn_event_id'] ?? frame['source_event_id'];
      // Ignore identified other turns; unattributed errors must still fail this run.
      return typeof source !== 'string' || source.length === 0 || source === userEventId;
    });
  };
  const history = await collectSseFrames(cfg, sessionId, {
    until: (fs) =>
      turnFrames(fs).some((frame) =>
        frame.type === 'agent.turn_completed'
          ? completesTurn(frame)
          : (TERMINAL_FRAMES as readonly string[]).includes(frame.type),
      ),
    deadlineMs,
  });
  const frames = turnFrames(history);
  assertNoTurnFailure(frames);
  expect(
    frames.some(completesTurn),
    `no agent.turn_completed within ${deadlineMs}ms; saw ${JSON.stringify(frames.map((f) => f.type))}`,
  ).toBe(true);
  return frames;
}

/** Every tool-use frame's name, in order. Covers both the plain and MCP event kinds. */
export function toolNames(frames: SseFrame[]): string[] {
  return frames
    .filter((f) => f.type === 'agent.tool_use' || f.type === 'agent.mcp_tool_use')
    .map((f) => (typeof f['name'] === 'string' ? f['name'] : ''))
    .filter((n) => n.length > 0);
}
