// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @orca/e2e-tests — self-hosted path, end to end against the live minimal stack.
 *
 * This spec proves the SELF-HOSTED control flow the cloud path never exercises:
 *
 *   client → registry → (durable claim) → environment-worker → session-runner →
 *   single-writer event bridge → client SSE
 *
 * It runs against the minimal stack `start-self-hosted.sh` brings up: Postgres +
 * RustFS only, the registry on `TRANSCRIPT_STORE_BACKEND=postgres`, and a live
 * environment-worker already attached (worker-tunnel claim) to a `self_hosted`
 * environment created over the public API. There is NO Kafka/Pulsar, NO
 * ai-gateway, and NO model credential anywhere: the agent runs the first-class
 * `mock` provider — a deterministic, LLM-free harness — so the FULL plumbing is
 * validated in CI for free.
 *
 * What it asserts:
 *   1. RUN (worker connected): an agent annotated `harness=mock` + a session on
 *      the worker-attached environment + a posted `user.message` drives the
 *      runner, and the runner's `agent.message` + `agent.turn_completed` flow
 *      back through worker → runner → registry single-writer bridge → SSE within
 *      a timeout. The `agent.message` echoes the user text (`mock: <text>`),
 *      which the mock harness can only produce by actually running the turn.
 *   2. PENDING (no worker): a session created on a DIFFERENT `self_hosted`
 *      environment that has no connected worker stays `distribution_state=pending`
 *      with no runner assigned (and `work_stats.worker_connected=false`), then —
 *      to close the "…then runs once a worker connects" half deterministically —
 *      the SAME prompt on the worker-attached environment runs to completion.
 *   3. SINGLE-WRITER ORDERING: the persisted transcript for the run session has
 *      the posted `user.message` strictly before the `agent.message`, and the
 *      `agent.message` strictly before the terminal `agent.turn_completed` — the
 *      one-writer, in-order persist-before-forward guarantee of the owner-pod
 *      bridge, read back off `GET /v1/sessions/:id/events`.
 *
 * Prerequisite: `bash services/dev/scripts/start-self-hosted.sh` (the script
 * writes the worker-attached environment id to `services/dev/run/self-hosted.env`,
 * which this spec reads). Run via `pnpm e2e:self-hosted`.
 *
 * The two OPTIONAL gated real-Claude scenarios at the bottom — the lean `claude`
 * provider (A) and the persistent `claude-sdk-persistent` provider (B) — self-skip
 * without `ANTHROPIC_API_KEY` (and are a no-op on the mock-only stack, since the
 * worker's runner needs the key in its own env to reach Anthropic) — the CI-default
 * path is the mock provider and needs no secret.
 */
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedWorkspaceApiKey } from '../src/seed.js';

const here = dirname(fileURLToPath(import.meta.url));
/** Where start-self-hosted.sh records the worker-attached environment id + name. */
const SELF_HOSTED_ENV_FILE = resolve(here, '../../../services/dev/run/self-hosted.env');

interface EnvironmentResponse {
  id: string;
  /**
   * Anthropic's `BetaEnvironment` projection — the DEFAULT wire, with no beta header.
   * `cloud` vs `self_hosted` rides here.
   */
  config?: { type?: string } | null;
  /**
   * The Orca-only flat spelling of the same thing, and the one-time key. Both are
   * gated behind `orca-beta`, so they are absent unless the caller asks for the
   * extended wire.
   */
  target?: string | null;
  egress_mode?: string | null;
  env_key?: string;
}

interface SessionResponse {
  id: string;
  status: string;
  environment_id: string | null;
  runner_id: string | null;
  host_environment_id: string | null;
  distribution_state: string | null;
}

interface WorkStatsResponse {
  worker_connected: boolean;
  depth: number;
  in_flight: number;
}

interface TranscriptEvent {
  type: string;
  seq?: number;
  content?: Array<{ type: string; text?: string }>;
  [key: string]: unknown;
}

/** An SSE frame after parsing the stream's `data:` line. */
interface SseFrame {
  type: string;
  content?: Array<{ type: string; text?: string }>;
  [key: string]: unknown;
}

/**
 * Read the worker-attached environment id the start script recorded. Fails LOUD
 * with the fix (not a skip) when the file is absent — the self-hosted stack must
 * be up for this spec, exactly like the other Layer-B specs hard-require the
 * stack rather than silently passing.
 */
function readWorkerAttachedEnvironmentId(): string {
  let raw: string;
  try {
    raw = readFileSync(SELF_HOSTED_ENV_FILE, 'utf8');
  } catch (err) {
    throw new Error(
      `self-hosted e2e: ${SELF_HOSTED_ENV_FILE} not found ` +
        `(${(err as Error).message}). Start the stack first: ` +
        '`bash services/dev/scripts/start-self-hosted.sh`, then `pnpm e2e:self-hosted`.',
    );
  }
  for (const line of raw.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq <= 0) continue;
    if (trimmed.slice(0, eq).trim() === 'ORCA_SELF_HOSTED_ENVIRONMENT_ID') {
      const value = trimmed.slice(eq + 1).trim();
      if (value) return value;
    }
  }
  throw new Error(
    `self-hosted e2e: ${SELF_HOSTED_ENV_FILE} has no ORCA_SELF_HOSTED_ENVIRONMENT_ID. ` +
      'Re-run `bash services/dev/scripts/start-self-hosted.sh`.',
  );
}

// Creates return 200, not 201. That is deliberate and is what Anthropic's Managed
// Agents API does: `agents.contract.ts` declares `200: Agent` and the routes send
// `reply.code(200)`. Asserting 201 would be asserting a shape this API does not have.
/**
 * The extended-wire header, required on every read that needs a RUNNER signal.
 *
 * The default stream and event list are filtered to `CLAUDE_SESSION_EVENT_TYPES` --
 * the documented Anthropic session-event vocabulary -- and the two frames this spec
 * is built on are not in it: `agent.turn_completed` (the turn boundary) and
 * `agent.requires_action` (the tool gate) are Orca runner signals, not Anthropic
 * events. Without the header the transcript ends at `agent.message` and the turn
 * never appears to finish. The same header also surfaces `distribution_state` /
 * `runner_id` / `host_environment_id` on a session, which is how this spec proves a
 * runner actually picked the work up.
 */
const ORCA_BETA_HEADERS = { 'orca-beta': 'true' } as const;
const GUARDRAILS_PATH = '/apis/policy.runorca.ai/v1/guardrails';

async function createMockAgent(
  cfg: OrcaClientConfig,
  name: string,
  guardrailIds: string[] = [],
): Promise<string> {
  const res = await apiCall(cfg, '/v1/agents', {
    method: 'POST',
    headers: { ...ORCA_BETA_HEADERS },
    body: JSON.stringify({
      name,
      // model.provider is the LLM provider field; the runner provider (`mock`)
      // is derived from the harness annotation. `model: mock` keeps the agent
      // self-describing; the harness annotation is what selects the mock runner.
      model: { provider: 'mock', id: 'mock-1' },
      system: 'You are a deterministic mock agent.',
      // The harness annotation the registry's snapshot resolver maps to the
      // runner's `mock` provider (single source: @orca/harness-catalog). `mock` is
      // a session-runner provider — session-runner IS the sole `colocated` engine —
      // so its only supported mode is `colocated` (harness-server, the `separate`
      // engine, never runs it).
      metadata: { harness: 'mock', mode: 'colocated' },
      guardrail_ids: guardrailIds,
    }),
  });
  expect(res.status, `create agent failed: ${res.status} ${res.text}`).toBe(200);
  return res.json<{ id: string }>().id;
}

async function createRequestPiiGuardrail(
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

async function updateRequestPiiGuardrail(
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

async function createSelfHostedEnvironment(
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

async function createSession(
  cfg: OrcaClientConfig,
  agentId: string,
  environmentId: string,
): Promise<SessionResponse> {
  const res = await apiCall(cfg, '/v1/sessions', {
    method: 'POST',
    // The create RESPONSE is read for `distribution_state` straight away, and that
    // field is `orca-beta`-gated like its `runner_id` / `host_environment_id`
    // siblings -- so without the header the caller gets a session whose dispatch is
    // simply invisible, not a session that was not dispatched.
    headers: { ...ORCA_BETA_HEADERS },
    body: JSON.stringify({ agent_id: agentId, environment_id: environmentId }),
  });
  expect(res.status, `create session failed: ${res.status} ${res.text}`).toBe(200);
  return res.json<SessionResponse>();
}

async function postUserMessage(
  cfg: OrcaClientConfig,
  sessionId: string,
  text: string,
): Promise<void> {
  const res = await apiCall(cfg, `/v1/sessions/${sessionId}/events`, {
    method: 'POST',
    body: JSON.stringify({
      events: [{ type: 'user.message', content: [{ type: 'text', text }] }],
      request_id: `self-hosted-${Date.now()}`,
    }),
  });
  expect(res.status, `post user.message failed: ${res.status} ${res.text}`).toBe(200);
}

async function getSession(cfg: OrcaClientConfig, sessionId: string): Promise<SessionResponse> {
  const res = await apiCall(cfg, `/v1/sessions/${sessionId}`, {
    method: 'GET',
    headers: { ...ORCA_BETA_HEADERS },
  });
  expect(res.status).toBe(200);
  return res.json<SessionResponse>();
}

async function getWorkStats(
  cfg: OrcaClientConfig,
  environmentId: string,
): Promise<WorkStatsResponse> {
  const res = await apiCall(cfg, `/v1/environments/${environmentId}/work_stats`, { method: 'GET' });
  expect(res.status).toBe(200);
  return res.json<WorkStatsResponse>();
}

/** Read the full persisted public transcript for a session (ordered by seq). */
async function readEvents(cfg: OrcaClientConfig, sessionId: string): Promise<TranscriptEvent[]> {
  const res = await apiCall(cfg, `/v1/sessions/${sessionId}/events?limit=1000`, {
    method: 'GET',
    headers: { ...ORCA_BETA_HEADERS },
  });
  expect(res.status).toBe(200);
  // `data`, not `events`. This route returns `{ data, next_page }` in BOTH
  // projections -- `orca-beta` adds `has_more`, it does not rename the array. The
  // `events` spelling belongs to the APPEND request body, not to any read.
  return res.json<{ data: TranscriptEvent[] }>().data;
}

/**
 * Poll `GET /events` until `predicate` holds or the timeout elapses. The events
 * index is an asynchronously-maintained projection of the transcript (a separate
 * consumer indexes appended events), so a read taken immediately after the live
 * SSE tail shows an event can legitimately miss it for a poll cycle. Polling
 * rides that eventual consistency. Returns the last page read (so the caller can
 * assert on it even on the final iteration).
 */
async function pollEventsUntil(
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

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * Tail the SSE stream until `until(frames)` is satisfied or the deadline passes.
 * Mirrors the real-agent-loop collector: split on the SSE record separator, take
 * each record's `data:` line, JSON-parse it. The registry serves the SAME
 * persisted transcript over SSE that `GET /events` reads, so the frames are the
 * bridge-persisted agent events.
 *
 * Streams `from_cursor=0` so the tail REPLAYS the session's transcript from the
 * start rather than tailing only events appended after the subscription. The
 * mock turn completes in a few hundred milliseconds — faster than an HTTP
 * subscribe round-trip — so a live-from-head tail would race past the already-
 * persisted `agent.message` / `agent.turn_completed`. Replaying from 0 makes the
 * assertion deterministic regardless of how fast the runner answered.
 */
async function collectSseFrames(
  cfg: OrcaClientConfig,
  sessionId: string,
  opts: { until: (frames: SseFrame[]) => boolean; deadlineMs: number },
): Promise<SseFrame[]> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), opts.deadlineMs + 5_000);
  try {
    // `/events/stream`, not `/stream`. This spec was written against a branch that
    // served BOTH -- an alias that neither `main` nor this branch has -- so `/stream`
    // 404s here. `@orca/oeadm`'s client had the identical bug from the identical
    // cause; its `registry-contract-pin.spec.ts` now reads the path out of
    // `sessions.contract.ts` rather than restating it, which is the durable fix.
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

    while (Date.now() < deadline) {
      let chunk: ReadableStreamReadResult<Uint8Array>;
      try {
        chunk = await reader.read();
      } catch {
        break;
      }
      if (chunk.done) break;
      buffered += decoder.decode(chunk.value, { stream: true });

      let idx;
      while ((idx = buffered.indexOf('\n\n')) >= 0) {
        const block = buffered.slice(0, idx);
        buffered = buffered.slice(idx + 2);
        if (block.startsWith(':')) continue; // heartbeat comment
        const dataLine = block.split('\n').find((l) => l.startsWith('data: '));
        if (!dataLine) continue;
        try {
          frames.push(JSON.parse(dataLine.slice(6)) as SseFrame);
        } catch {
          /* ignore non-JSON */
        }
      }

      if (opts.until(frames)) break;
    }

    await reader.cancel().catch(() => {});
    return frames;
  } finally {
    clearTimeout(timer);
    ac.abort();
  }
}

/** The first text part of an event's content array, or `''`. */
function textOf(frame: SseFrame | TranscriptEvent): string {
  const blocks = Array.isArray(frame.content) ? frame.content : [];
  const first = blocks.find((b) => b?.type === 'text' && typeof b.text === 'string');
  return first?.text ?? '';
}

describe('self-hosted path (worker → claim → runner → mock provider → bridge → SSE)', () => {
  let cfg: OrcaClientConfig;
  let workerEnvId: string;
  // Per-test created ids, drained in reverse-dependency order in afterAll so a
  // re-run on a reused Postgres volume stays clean.
  const sessionIds: string[] = [];
  const agentIds: string[] = [];
  const guardrailIds: string[] = [];
  const environmentIds: string[] = [];

  beforeAll(async () => {
    const seeded = await seedWorkspaceApiKey();
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    workerEnvId = readWorkerAttachedEnvironmentId();

    // Sanity: the recorded environment must exist, be self_hosted, and have a
    // connected worker. If the worker isn't connected the run scenario can't
    // pass — surface that precisely here instead of as a turn timeout below.
    const env = await apiCall(cfg, `/v1/environments/${workerEnvId}`, { method: 'GET' });
    expect(env.status, `worker-attached environment ${workerEnvId} not found`).toBe(200);
    // Read `config.type`, not `target`. Both name the same thing, but `target` is an
    // Orca extension gated behind `orca-beta` and this call does not send it, so it
    // comes back undefined; `config.type` is Anthropic's own field and is always
    // present. Asserting the default projection also means this check exercises the
    // wire an ordinary Anthropic-SDK client would see.
    expect(env.json<EnvironmentResponse>().config?.type).toBe('self_hosted');

    // The worker connects asynchronously after the stack script returns in CI;
    // give the claim heartbeat a brief window to be observed as live.
    let connected = false;
    for (let i = 0; i < 30; i++) {
      const stats = await getWorkStats(cfg, workerEnvId);
      if (stats.worker_connected) {
        connected = true;
        break;
      }
      await sleep(1_000);
    }
    expect(
      connected,
      `no worker connected to environment ${workerEnvId} — is start-self-hosted.sh's worker running? ` +
        'Check services/dev/logs/environment-worker.log.',
    ).toBe(true);
  }, 120_000);

  afterAll(async () => {
    for (const id of sessionIds.reverse()) {
      await apiCall(cfg, `/v1/sessions/${id}`, { method: 'DELETE' }).catch(() => {});
    }
    for (const id of agentIds.reverse()) {
      await apiCall(cfg, `/v1/agents/${id}`, { method: 'DELETE' }).catch(() => {});
    }
    for (const id of guardrailIds.reverse()) {
      await apiCall(cfg, `${GUARDRAILS_PATH}/${id}`, { method: 'DELETE' }).catch(() => {});
    }
    for (const id of environmentIds.reverse()) {
      await apiCall(cfg, `/v1/environments/${id}`, { method: 'DELETE' }).catch(() => {});
    }
  });

  it('pending-when-no-worker: a session on a worker-less self_hosted environment stays pending', async () => {
    // A fresh self_hosted environment the running worker is NOT attached to.
    const lonelyEnv = await createSelfHostedEnvironment(cfg, `self-hosted-no-worker-${Date.now()}`);
    environmentIds.push(lonelyEnv.id);

    // No worker on it — work_stats reports it.
    const stats = await getWorkStats(cfg, lonelyEnv.id);
    expect(stats.worker_connected).toBe(false);

    const agentId = await createMockAgent(cfg, `self-hosted-mock-pending-${Date.now()}`);
    agentIds.push(agentId);

    const session = await createSession(cfg, agentId, lonelyEnv.id);
    sessionIds.push(session.id);
    // The distributor persists PENDING for a self_hosted session; with no
    // connected worker it has no runner and stays pending. (Create already
    // reflects pending; assert it.)
    expect(session.distribution_state).toBe('pending');
    expect(session.runner_id).toBeNull();

    // Posting a user.message does not magically run it (still no worker). Give
    // it a moment, then confirm it is STILL pending + unassigned.
    await postUserMessage(cfg, session.id, 'are you there?');
    await sleep(3_000);
    const after = await getSession(cfg, session.id);
    expect(after.distribution_state).toBe('pending');
    expect(after.runner_id).toBeNull();
    expect(after.host_environment_id).toBeNull();

    // The work depth counts this stranded session (no worker picked it up).
    const depthStats = await getWorkStats(cfg, lonelyEnv.id);
    expect(depthStats.worker_connected).toBe(false);
    expect(depthStats.depth).toBeGreaterThanOrEqual(1);
  }, 60_000);

  it('run-when-worker-connected: the mock agent answers a turn through worker → runner → bridge → SSE', async () => {
    const agentId = await createMockAgent(cfg, `self-hosted-mock-run-${Date.now()}`);
    agentIds.push(agentId);

    const session = await createSession(cfg, agentId, workerEnvId);
    sessionIds.push(session.id);
    // On the worker-attached environment the distributor dispatched a launch —
    // the session is pending (spawning) or already assigned, never null.
    expect(session.distribution_state, 'self_hosted session should be distributed').not.toBeNull();
    expect(['pending', 'assigned']).toContain(session.distribution_state);

    const prompt = `hello from the self-hosted e2e ${Date.now()}`;
    await postUserMessage(cfg, session.id, prompt);

    // The runner's scripted turn flows back through the single-writer bridge to
    // SSE: an agent.message answering the prompt, then a terminal turn_completed.
    const frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 60_000,
      until: (f) => f.some((frame) => frame.type === 'agent.turn_completed'),
    });

    const kinds = frames.map((f) => f.type);
    expect(
      kinds,
      `expected agent.message + agent.turn_completed over SSE; saw: ${JSON.stringify(kinds)}`,
    ).toContain('agent.message');
    expect(kinds).toContain('agent.turn_completed');

    // The mock harness answers `mock: <user text>` — proof it actually ran the
    // turn (it can't produce the echoed prompt without seeing the user.message).
    const agentMessage = frames.find((f) => f.type === 'agent.message');
    expect(agentMessage, 'no agent.message frame').toBeDefined();
    expect(textOf(agentMessage!)).toBe(`mock: ${prompt}`);

    // Once the runner connected, the session moved off pending.
    const ran = await getSession(cfg, session.id);
    expect(ran.runner_id, 'a runner should be assigned after the turn').not.toBeNull();
    expect(ran.distribution_state).toBe('assigned');
  }, 90_000);

  it('guardrails: request deny and a live edit apply before the next colocated turn', async () => {
    const guardrailId = await createRequestPiiGuardrail(
      cfg,
      `self-hosted-request-guardrail-${Date.now()}`,
      ['ssn'],
    );
    guardrailIds.push(guardrailId);
    const agentId = await createMockAgent(cfg, `self-hosted-guarded-mock-${Date.now()}`, [
      guardrailId,
    ]);
    agentIds.push(agentId);
    const session = await createSession(cfg, agentId, workerEnvId);
    sessionIds.push(session.id);

    const allowedPrompt = `safe request ${Date.now()}`;
    await postUserMessage(cfg, session.id, allowedPrompt);
    let frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 60_000,
      until: (items) => items.filter((frame) => frame.type === 'agent.turn_completed').length >= 1,
    });
    expect(frames.some((frame) => textOf(frame) === `mock: ${allowedPrompt}`)).toBe(true);

    // Change the existing policy while the runner is live. The owner-pod bridge
    // must observe this fold before sending the next message and re-deliver the
    // snapshot; otherwise the mock harness would echo the email below.
    await updateRequestPiiGuardrail(cfg, guardrailId, ['email']);
    const blockedPrompt = `contact alice@example.com for guardrail ${Date.now()}`;
    await postUserMessage(cfg, session.id, blockedPrompt);
    frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 60_000,
      until: (items) => items.filter((frame) => frame.type === 'agent.turn_completed').length >= 2,
    });

    expect(frames.some((frame) => textOf(frame) === `mock: ${blockedPrompt}`)).toBe(false);
    const guardrailError = frames.find(
      (frame) =>
        frame.type === 'agent.error' &&
        typeof frame['message'] === 'string' &&
        frame['message'].includes('email'),
    );
    expect(
      guardrailError,
      'request guardrail denial did not reach the durable stream',
    ).toBeDefined();
  }, 120_000);

  it('single-writer ordering: user.message ≺ agent.message ≺ agent.turn_completed in the persisted log', async () => {
    const agentId = await createMockAgent(cfg, `self-hosted-mock-order-${Date.now()}`);
    agentIds.push(agentId);

    const session = await createSession(cfg, agentId, workerEnvId);
    sessionIds.push(session.id);

    const prompt = `ordering probe ${Date.now()}`;
    await postUserMessage(cfg, session.id, prompt);

    // Tail the durable transcript (SSE replays it from seq 0) until the turn
    // completes. The SSE stream serves the registry's single-writer transcript
    // verbatim — each frame carries the monotonic `seq` the writer assigned and
    // the `produced_by` provenance — so it is the authoritative ordering source
    // (the `GET /events` index is a SEPARATE, asynchronously-maintained
    // projection of the same transcript and lags the live tail by a poll cycle,
    // so asserting on it here would be racing an eventually-consistent read, not
    // the ordering guarantee itself).
    const frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 60_000,
      until: (f) => f.some((frame) => frame.type === 'agent.turn_completed'),
    });

    const userFrame = frames.find((f) => f.type === 'user.message' && textOf(f) === prompt);
    const msgFrame = frames.find(
      (f) => f.type === 'agent.message' && textOf(f) === `mock: ${prompt}`,
    );
    const doneFrame = frames.find((f) => f.type === 'agent.turn_completed');
    const kinds = frames.map((f) => f.type);
    expect(userFrame, `no user.message; saw ${JSON.stringify(kinds)}`).toBeDefined();
    expect(msgFrame, `no agent.message; saw ${JSON.stringify(kinds)}`).toBeDefined();
    expect(doneFrame, `no agent.turn_completed; saw ${JSON.stringify(kinds)}`).toBeDefined();

    // Single-writer provenance: the agent events were written by the owner-pod
    // bridge (`produced_by=harness`), the user event by the client — exactly the
    // one-writer split the bridge enforces (the client appends user.*, the
    // bridge is the sole writer of the agent.* events it persists-before-forward).
    expect((userFrame as { produced_by?: string }).produced_by).toBe('client');
    expect((msgFrame as { produced_by?: string }).produced_by).toBe('harness');
    expect((doneFrame as { produced_by?: string }).produced_by).toBe('harness');

    // Strict in-order: the writer assigns a single monotonic seq, so
    // user.message ≺ agent.message ≺ agent.turn_completed by seq. The seqs are
    // string-encoded bigints; compare numerically.
    const userSeq = Number((userFrame as { seq?: string }).seq);
    const msgSeq = Number((msgFrame as { seq?: string }).seq);
    const doneSeq = Number((doneFrame as { seq?: string }).seq);
    expect(Number.isFinite(userSeq) && Number.isFinite(msgSeq) && Number.isFinite(doneSeq)).toBe(
      true,
    );
    expect(userSeq).toBeLessThan(msgSeq);
    expect(msgSeq).toBeLessThan(doneSeq);

    // The index projection (`GET /events`) eventually reflects the same turn.
    // Poll it (rather than read once) so the assertion rides the eventual
    // consistency instead of racing it — the durable transcript already proved
    // ordering above; this just confirms the read API converges to it.
    const indexed = await pollEventsUntil(
      cfg,
      session.id,
      (evts) =>
        evts.some((e) => e.type === 'agent.message' && textOf(e) === `mock: ${prompt}`) &&
        evts.some((e) => e.type === 'agent.turn_completed'),
      20_000,
    );
    const iUser = indexed.findIndex((e) => e.type === 'user.message' && textOf(e) === prompt);
    const iMsg = indexed.findIndex(
      (e) => e.type === 'agent.message' && textOf(e) === `mock: ${prompt}`,
    );
    const iDone = indexed.findIndex((e) => e.type === 'agent.turn_completed');
    expect(iUser).toBeGreaterThanOrEqual(0);
    expect(iMsg).toBeGreaterThanOrEqual(0);
    expect(iDone).toBeGreaterThanOrEqual(0);
    expect(iUser).toBeLessThan(iMsg);
    expect(iMsg).toBeLessThan(iDone);
  }, 90_000);
});

/**
 * OPTIONAL, gated: the real `claude` provider over the same self-hosted path.
 * Self-skips without `ANTHROPIC_API_KEY`. NOTE: this only passes when the
 * environment-worker's runner has an `ANTHROPIC_API_KEY` in ITS environment
 * (the runner reaches Anthropic directly in the sidecar/no-gateway dev stack),
 * so it is a manual, opt-in check — the CI-default self-hosted path is the mock
 * provider above and needs no secret. Kept here so a developer with a key can
 * smoke the real model end to end over the self-hosted tunnel with one env var.
 */
describe('self-hosted path — real claude provider (gated)', () => {
  const hasKey = !!process.env['ANTHROPIC_API_KEY'];
  const maybe = hasKey ? it : it.skip;

  maybe(
    'a claude agent answers a turn through the self-hosted runner',
    async () => {
      const seeded = await seedWorkspaceApiKey();
      const cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
      await ensureStackReachable(cfg);
      const workerEnvId = readWorkerAttachedEnvironmentId();

      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `self-hosted-claude-${Date.now()}`,
          model: { provider: 'anthropic', id: 'claude-sonnet-4-5' },
          system: 'Reply with the single word: pong.',
          // Default harness (claude_agent_sdk / separate) → the runner's claude provider.
          metadata: {},
        }),
      });
      expect(agentRes.status).toBe(200);
      const agentId = agentRes.json<{ id: string }>().id;

      const sessionRes = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({ agent_id: agentId, environment_id: workerEnvId }),
      });
      expect(sessionRes.status).toBe(200);
      const sessionId = sessionRes.json<{ id: string }>().id;

      await apiCall(cfg, `/v1/sessions/${sessionId}/events`, {
        method: 'POST',
        body: JSON.stringify({
          events: [{ type: 'user.message', content: [{ type: 'text', text: 'Say pong.' }] }],
          request_id: `self-hosted-claude-${Date.now()}`,
        }),
      });

      const frames = await collectSseFrames(cfg, sessionId, {
        deadlineMs: 110_000,
        until: (f) => f.some((frame) => frame.type === 'agent.turn_completed'),
      });
      const text = frames
        .filter((f) => f.type === 'agent.message')
        .map((f) => textOf(f))
        .join('\n')
        .toLowerCase();
      expect(text).toContain('pong');

      await apiCall(cfg, `/v1/sessions/${sessionId}`, { method: 'DELETE' }).catch(() => {});
      await apiCall(cfg, `/v1/agents/${agentId}`, { method: 'DELETE' }).catch(() => {});
    },
    180_000,
  );
});

/**
 * OPTIONAL, gated: the real `claude-sdk-persistent` provider (provider B — the
 * PERSISTENT live-session Claude harness) over the same self-hosted path. Sibling to
 * the lean `claude` scenario above; self-skips without `ANTHROPIC_API_KEY` for the SAME
 * reason — the worker's runner needs the key in ITS env to reach Anthropic, and
 * `start-self-hosted.sh` forwards it worker→runner via `ORCA_RUNNER_ENV_PASSTHROUGH`.
 *
 * The persistent provider is a DISTINCT runner provider from the lean `claude`: the
 * agent selects it with the harness annotation `metadata.harness:
 * 'claude_agent_sdk_persistent'`, which the registry's snapshot resolver maps to
 * `provider: 'claude-sdk-persistent'` — mirroring how the lean scenario lets the DEFAULT
 * annotation resolve to `provider: 'claude'`. Kept here so a developer with a key can
 * smoke BOTH real Claude providers end to end over the self-hosted tunnel with one env
 * var; the CI-default self-hosted path is the mock provider and needs no secret.
 */
describe('self-hosted path — real claude-sdk-persistent provider (gated)', () => {
  const hasKey = !!process.env['ANTHROPIC_API_KEY'];
  const maybe = hasKey ? it : it.skip;

  maybe(
    'a claude-sdk-persistent agent answers a turn through the self-hosted runner',
    async () => {
      const seeded = await seedWorkspaceApiKey();
      const cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
      await ensureStackReachable(cfg);
      const workerEnvId = readWorkerAttachedEnvironmentId();

      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `self-hosted-claude-persistent-${Date.now()}`,
          model: { provider: 'anthropic', id: 'claude-sonnet-4-5' },
          system: 'Reply with the single word: pong.',
          // The persistent Claude Agent SDK harness (provider B) → the runner's
          // `claude-sdk-persistent` provider. Selected via the harness annotation, the
          // same lever the lean scenario uses (its empty annotation defaults to the lean
          // `claude`); the resolver stamps `provider: 'claude-sdk-persistent'` here.
          metadata: { harness: 'claude_agent_sdk_persistent' },
        }),
      });
      expect(agentRes.status).toBe(200);
      const agentId = agentRes.json<{ id: string }>().id;

      const sessionRes = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({ agent_id: agentId, environment_id: workerEnvId }),
      });
      expect(sessionRes.status).toBe(200);
      const sessionId = sessionRes.json<{ id: string }>().id;

      await apiCall(cfg, `/v1/sessions/${sessionId}/events`, {
        method: 'POST',
        body: JSON.stringify({
          events: [{ type: 'user.message', content: [{ type: 'text', text: 'Say pong.' }] }],
          request_id: `self-hosted-claude-persistent-${Date.now()}`,
        }),
      });

      const frames = await collectSseFrames(cfg, sessionId, {
        deadlineMs: 110_000,
        until: (f) => f.some((frame) => frame.type === 'agent.turn_completed'),
      });
      const text = frames
        .filter((f) => f.type === 'agent.message')
        .map((f) => textOf(f))
        .join('\n')
        .toLowerCase();
      expect(text).toContain('pong');

      await apiCall(cfg, `/v1/sessions/${sessionId}`, { method: 'DELETE' }).catch(() => {});
      await apiCall(cfg, `/v1/agents/${agentId}`, { method: 'DELETE' }).catch(() => {});
    },
    180_000,
  );
});
