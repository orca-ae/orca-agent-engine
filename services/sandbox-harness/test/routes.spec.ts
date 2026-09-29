// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Route-level test for the HTTP app (server.ts#createApp + routes.ts).
//
// HERMETIC: no real subprocess, no LLM, no network beyond a loopback listener.
// The seam is `createApp(ctx: HarnessContext)` — the app depends only on the
// context INTERFACE, so we inject a fake `spawnManagedSession` that returns a
// fake `ManagedSession`. The fake stands in for the provider/session-manager:
// it records the resolved `SpawnArgs` and drives events straight into the REAL
// in-memory event store, with the same "status-sync THEN publish" wiring the
// production `createState` uses. The session/event stores are the real ones, so
// these tests exercise the genuine create -> history -> SSE replay-then-live path.
//
// Covered:
//   - POST /v1/sessions creates a session (201 + record), and the resolved
//     SpawnArgs reach spawnManagedSession;
//   - the optional `replay` history is accepted and threaded through into the
//     SpawnArgs' replayEnv (the resume seam);
//   - GET /v1/sessions/:id/events returns the recorded history;
//   - GET /v1/sessions/:id/events/stream replays buffered events first, then
//     streams live ones.

import { afterEach, beforeEach, describe, it, expect } from 'vitest';
import { type AddressInfo } from 'node:net';
import { type Server } from 'node:http';

import {
  createApp,
  createSessionStore,
  createEventStore,
  userMessageEvent,
  agentMessageEvent,
  sessionIdleEvent,
  type HarnessContext,
  type EventStore,
  type SessionStore,
} from '../src/index.js';
import {
  type ManagedSession,
  type SpawnArgs,
  AGENTS_ENV_VAR,
  ALLOWED_TOOLS_ENV_VAR,
  CUSTOM_TOOLS_ENV_VAR,
  FORWARD_SUBAGENT_TEXT_ENV_VAR,
  MODEL_EFFORT_ENV_VAR,
  MODEL_SPEED_ENV_VAR,
  REPLAY_ENV_VAR,
  RUNTIME_TOOLS_ENV_VAR,
  SYSTEM_PROMPT_ENV_VAR,
  TOOLS_ENV_VAR,
} from '../src/session-manager.js';

// ── fake session-manager seam ─────────────────────────────────────────────────

interface FakeRuntime extends ManagedSession {
  /** SpawnArgs this runtime was created with (asserted in tests). */
  readonly spawnArgs: SpawnArgs;
  /** Whether start() has been called. */
  started(): boolean;
  /** Drive one provider event into the store with status-sync (test helper). */
  emit(event: { type: string; [k: string]: unknown }): void;
  /** Args passed to each sendUserMessage call, in order (asserted in tests). */
  readonly sentMessages: unknown[];
  /** Payloads passed to sendCustomToolResult, in order (asserted in tests). */
  readonly customToolResults: unknown[];
  /**
   * Make the NEXT sendUserMessage reject with this error, simulating a delivery
   * failure (the production route reports it as a session.status_error). Reset
   * after one call so subsequent sends succeed. `undefined` = always resolve.
   */
  failNextSendWith(error: Error): void;
}

/**
 * Build a fake {@link HarnessContext} backed by the REAL stores plus a fake
 * `spawnManagedSession`. No subprocess is spawned. The returned `runtimes` map
 * lets a test reach a session's fake runtime to drive events.
 */
function makeFakeContext(): {
  ctx: HarnessContext;
  sessionStore: SessionStore;
  eventStore: EventStore;
  runtimes: Map<string, FakeRuntime>;
} {
  const sessionStore = createSessionStore();
  const eventStore = createEventStore();
  const runtimes = new Map<string, FakeRuntime>();

  function spawnManagedSession(sessionId: string, spawnArgs: SpawnArgs): ManagedSession {
    let isStarted = false;
    let alive = true;
    let nextSendError: Error | undefined;
    const sentMessages: unknown[] = [];
    const customToolResults: unknown[] = [];

    // Same lifecycle wiring as the production createState: a terminal event
    // flips session status BEFORE it is published, so an SSE reader never sees a
    // stale "running".
    function emit(event: { type: string; [k: string]: unknown }): void {
      if (event.type === 'session.status_idle') sessionStore.setStatus(sessionId, 'idle');
      else if (event.type === 'session.status_error') sessionStore.setStatus(sessionId, 'error');
      eventStore.publish(sessionId, event);
    }

    const runtime: FakeRuntime = {
      spawnArgs,
      sentMessages,
      customToolResults,
      started: () => isStarted,
      emit,
      failNextSendWith(error: Error): void {
        nextSendError = error;
      },
      start() {
        isStarted = true;
      },
      sendUserMessage(content) {
        sentMessages.push(content);
        if (nextSendError) {
          const error = nextSendError;
          nextSendError = undefined;
          return Promise.reject(error);
        }
        return Promise.resolve();
      },
      sendCustomToolResult(payload) {
        customToolResults.push(payload);
        return Promise.resolve();
      },
      kill() {
        alive = false;
      },
      isAlive() {
        return alive;
      },
    };
    runtimes.set(sessionId, runtime);
    return runtime;
  }

  const ctx: HarnessContext = {
    sessionStore,
    eventStore,
    spawnManagedSession,
    getRuntime: (id) => runtimes.get(id),
    deleteRuntime: (id) => runtimes.delete(id),
    subprocessEntryPath: '/fake/subprocess-entry.js',
    env: {},
  };

  return { ctx, sessionStore, eventStore, runtimes };
}

// ── HTTP helpers (loopback) ───────────────────────────────────────────────────

let server: Server;
let baseUrl: string;
let fake: ReturnType<typeof makeFakeContext>;

beforeEach(async () => {
  fake = makeFakeContext();
  server = createApp(fake.ctx);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${port}`;
});

afterEach(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

async function postJson(path: string, body: unknown): Promise<{ status: number; json: unknown }> {
  const res = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: res.status, json: await res.json() };
}

async function getJson(path: string): Promise<{ status: number; json: unknown }> {
  const res = await fetch(`${baseUrl}${path}`);
  return { status: res.status, json: await res.json() };
}

/**
 * Poll until `predicate` holds or a short deadline passes. Used by the delivery-
 * failure test: the route responds 200 immediately and the rejection's `.catch`
 * publishes session.status_error on a later microtask, so we wait for it instead
 * of racing.
 */
async function waitFor(predicate: () => boolean, timeoutMs = 1000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

// ── tests ─────────────────────────────────────────────────────────────────────

describe('POST /v1/sessions', () => {
  it('creates a session and returns 201 with the record', async () => {
    const { status, json } = await postJson('/v1/sessions', { agent: 'claude' });

    expect(status).toBe(201);
    const session = json as { id: string; object: string; agent: string; status: string };
    expect(session.object).toBe('session');
    expect(session.agent).toBe('claude');
    expect(session.id).toMatch(/^session_/);
    expect(session.status).toBe('idle');

    // The session manager seam was invoked and started.
    const runtime = fake.runtimes.get(session.id);
    expect(runtime?.started()).toBe(true);
    // Model flags were resolved into the spawn args (no --model when omitted).
    expect(runtime?.spawnArgs.args).toContain('--agent');
    expect(runtime?.spawnArgs.args).toContain('claude');
    expect(runtime?.spawnArgs.args).not.toContain('--model');
    // No replay supplied -> empty replay env.
    expect(runtime?.spawnArgs.replayEnv).toEqual({});
  });

  it('rejects a missing agent with 400', async () => {
    const { status, json } = await postJson('/v1/sessions', {});
    expect(status).toBe(400);
    expect((json as { error: { message: string } }).error.message).toMatch(/agent is required/);
  });

  it('fails session setup when the injected write policy is malformed', async () => {
    fake.ctx.env['ORCA_SANDBOX_WRITE_POLICY'] = '{';
    const { status, json } = await postJson('/v1/sessions', { agent: 'claude' });
    expect(status).toBe(503);
    expect((json as { error: { message: string } }).error.message).toMatch(
      /invalid ORCA_SANDBOX_WRITE_POLICY/,
    );
    expect(fake.runtimes.size).toBe(0);
  });

  it('threads model into the spawn args when provided', async () => {
    const { json } = await postJson('/v1/sessions', {
      agent: 'claude',
      model: 'claude-test-model',
      modelSpeed: 'fast',
      modelEffort: 'high',
    });
    const session = json as { id: string };
    const runtime = fake.runtimes.get(session.id);
    expect(runtime?.spawnArgs.args).toContain('--model');
    expect(runtime?.spawnArgs.args).toContain('claude-test-model');
    expect(runtime?.spawnArgs.replayEnv[MODEL_SPEED_ENV_VAR]).toBe('fast');
    expect(runtime?.spawnArgs.replayEnv[MODEL_EFFORT_ENV_VAR]).toBe('high');
  });

  it('rejects invalid model controls instead of dropping them', async () => {
    const speed = await postJson('/v1/sessions', {
      agent: 'claude',
      modelSpeed: 'turbo',
    });
    expect(speed.status).toBe(400);
    expect((speed.json as { error: { message: string } }).error.message).toMatch(/modelSpeed/);

    const effort = await postJson('/v1/sessions', {
      agent: 'claude',
      modelEffort: 'extreme',
    });
    expect(effort.status).toBe(400);
    expect((effort.json as { error: { message: string } }).error.message).toMatch(/modelEffort/);
  });

  it('accepts an optional replay history and threads it into replayEnv', async () => {
    const replay = [
      { role: 'user', text: 'what is 2 + 2?' },
      { role: 'assistant', parts: [{ type: 'text', text: '4' }] },
    ];
    const { status, json } = await postJson('/v1/sessions', { agent: 'claude', replay });
    expect(status).toBe(201);

    const session = json as { id: string };
    const runtime = fake.runtimes.get(session.id);

    // The resume preamble is carried in the replay env var (base64 JSON), NOT in
    // the model flags. Decode it back and confirm it round-trips.
    const encoded = runtime?.spawnArgs.replayEnv[REPLAY_ENV_VAR];
    expect(encoded).toBeTruthy();
    const decoded = JSON.parse(Buffer.from(encoded as string, 'base64').toString('utf8'));
    expect(decoded).toEqual(replay);
    // Replay never leaks into the launch flags.
    expect(runtime?.spawnArgs.args).not.toContain('--model');
  });

  it('ignores a malformed replay (no replayEnv) but still creates the session', async () => {
    const { status, json } = await postJson('/v1/sessions', {
      agent: 'claude',
      replay: [{ role: 'bogus' }, 'not-an-object'],
    });
    expect(status).toBe(201);
    const session = json as { id: string };
    expect(fake.runtimes.get(session.id)?.spawnArgs.replayEnv).toEqual({});
  });

  it('accepts multi-agent definitions and threads them into child env', async () => {
    const agents = {
      worker: {
        description: 'Handles worker tasks',
        prompt: 'You are the worker.',
        model: 'claude-worker-model',
        speed: 'fast',
        effort: 'low',
        tools: ['Read'],
      },
    };
    const { status, json } = await postJson('/v1/sessions', {
      agent: 'claude',
      agents,
      forwardSubagentText: true,
    });
    expect(status).toBe(201);

    const session = json as { id: string };
    const runtime = fake.runtimes.get(session.id);
    const encoded = runtime?.spawnArgs.replayEnv[AGENTS_ENV_VAR];
    expect(encoded).toBeTruthy();
    const decoded = JSON.parse(Buffer.from(encoded as string, 'base64').toString('utf8'));
    expect(decoded).toEqual({
      worker: {
        description: 'Handles worker tasks',
        prompt: 'You are the worker.',
        model: 'claude-worker-model',
        modelSpeed: 'fast',
        effort: 'low',
        tools: ['Read'],
      },
    });
    expect(runtime?.spawnArgs.replayEnv[FORWARD_SUBAGENT_TEXT_ENV_VAR]).toBe('1');
  });

  it('accepts custom tool definitions and threads them into child env', async () => {
    const customTools = [
      {
        name: 'lookup_ticket',
        description: 'Look up a support ticket.',
        input_schema: {
          type: 'object',
          properties: { ticket_id: { type: 'string' } },
          required: ['ticket_id'],
        },
      },
    ];
    const { status, json } = await postJson('/v1/sessions', {
      agent: 'claude',
      customTools,
    });
    expect(status).toBe(201);

    const session = json as { id: string };
    const runtime = fake.runtimes.get(session.id);
    const encoded = runtime?.spawnArgs.replayEnv[CUSTOM_TOOLS_ENV_VAR];
    expect(encoded).toBeTruthy();
    const decoded = JSON.parse(Buffer.from(encoded as string, 'base64').toString('utf8'));
    expect(decoded).toEqual(customTools);
  });

  it('threads system prompt and SDK tool policies into child env', async () => {
    const { status, json } = await postJson('/v1/sessions', {
      agent: 'claude',
      systemPrompt: 'Use only attached resources.',
      tools: ['Read', 'Write'],
      allowedTools: ['Read'],
      runtimeTools: ['read', 'write'],
    });
    expect(status).toBe(201);

    const session = json as { id: string };
    const env = fake.runtimes.get(session.id)?.spawnArgs.replayEnv ?? {};
    expect(Buffer.from(env[SYSTEM_PROMPT_ENV_VAR]!, 'base64').toString('utf8')).toBe(
      'Use only attached resources.',
    );
    expect(JSON.parse(Buffer.from(env[TOOLS_ENV_VAR]!, 'base64').toString('utf8'))).toEqual([
      'Read',
      'Write',
    ]);
    expect(JSON.parse(Buffer.from(env[ALLOWED_TOOLS_ENV_VAR]!, 'base64').toString('utf8'))).toEqual(
      ['Read'],
    );
    expect(JSON.parse(Buffer.from(env[RUNTIME_TOOLS_ENV_VAR]!, 'base64').toString('utf8'))).toEqual(
      ['read', 'write'],
    );
  });
});

describe('GET /v1/sessions/:id/events', () => {
  it('returns the recorded event history in order', async () => {
    const created = await postJson('/v1/sessions', { agent: 'claude' });
    const { id } = created.json as { id: string };

    // Drive a few provider events through the fake runtime into the real store.
    const runtime = fake.runtimes.get(id)!;
    runtime.emit(userMessageEvent('hello'));
    runtime.emit(agentMessageEvent('hi there'));
    runtime.emit(sessionIdleEvent({ usage: { input_tokens: 3 }, total_cost_usd: 0.01 }));

    const { status, json } = await getJson(`/v1/sessions/${id}/events`);
    expect(status).toBe(200);
    const body = json as { object: string; data: Array<{ type: string }> };
    expect(body.object).toBe('list');
    expect(body.data.map((e) => e.type)).toEqual([
      'user.message',
      'agent.message',
      'session.status_idle',
    ]);
  });

  it('flips status to idle when a terminal event is emitted', async () => {
    const created = await postJson('/v1/sessions', { agent: 'claude' });
    const { id } = created.json as { id: string };

    fake.runtimes.get(id)!.emit(sessionIdleEvent());

    const { json } = await getJson(`/v1/sessions/${id}`);
    expect((json as { status: string }).status).toBe('idle');
  });

  it('returns 404 for an unknown session', async () => {
    const { status } = await getJson('/v1/sessions/session_missing/events');
    expect(status).toBe(404);
  });
});

describe('GET /v1/sessions/:id/events/stream (SSE)', () => {
  it('replays buffered events first, then streams live ones', async () => {
    const created = await postJson('/v1/sessions', { agent: 'claude' });
    const { id } = created.json as { id: string };
    const runtime = fake.runtimes.get(id)!;

    // Buffer two events BEFORE opening the stream — these must be replayed.
    runtime.emit(userMessageEvent('buffered-1'));
    runtime.emit(agentMessageEvent('buffered-2'));

    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/v1/sessions/${id}/events/stream`, {
      headers: { accept: 'text/event-stream' },
      signal: controller.signal,
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toContain('text/event-stream');

    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    const events: Array<{ type: string; content?: Array<{ text?: string }> }> = [];

    // Read SSE `data:` records until we have N parsed events (or time out).
    async function readUntil(count: number): Promise<void> {
      let buffer = '';
      const deadline = Date.now() + 2000;
      while (events.length < count && Date.now() < deadline) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let idx: number;
        while ((idx = buffer.indexOf('\n\n')) !== -1) {
          const record = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 2);
          const line = record.split('\n').find((l) => l.startsWith('data:'));
          if (line) events.push(JSON.parse(line.slice('data:'.length).trim()));
        }
      }
    }

    // 1) Replay: the two buffered events arrive first, in order.
    await readUntil(2);
    expect(events.map((e) => e.type)).toEqual(['user.message', 'agent.message']);

    // 2) Live: emit after the stream is open; it must arrive on the same stream.
    runtime.emit(agentMessageEvent('live-3'));
    await readUntil(3);
    expect(events).toHaveLength(3);
    expect(events[2]?.type).toBe('agent.message');
    expect(events[2]?.content?.[0]?.text).toBe('live-3');

    controller.abort();
    await reader.cancel().catch(() => undefined);
  });

  it('returns 404 for an unknown session', async () => {
    const res = await fetch(`${baseUrl}/v1/sessions/session_missing/events/stream`);
    expect(res.status).toBe(404);
    await res.body?.cancel().catch(() => undefined);
  });
});

describe('POST /v1/sessions/:id/events', () => {
  it('publishes a user.message and flips status to running on a live runtime', async () => {
    const created = await postJson('/v1/sessions', { agent: 'claude' });
    const { id } = created.json as { id: string };
    const runtime = fake.runtimes.get(id)!;

    const { status, json } = await postJson(`/v1/sessions/${id}/events`, {
      events: [{ type: 'user.message', content: 'hello world' }],
    });

    // The route acknowledges the fire-and-forget turn.
    expect(status).toBe(200);
    expect(json).toEqual({ ok: true });

    // The user.message was published to the (real) event store...
    const events = await getJson(`/v1/sessions/${id}/events`);
    const body = events.json as {
      data: Array<{ type: string; content?: Array<{ text?: string }> }>;
    };
    expect(body.data.map((e) => e.type)).toEqual(['user.message']);
    expect(body.data[0]?.content?.[0]?.text).toBe('hello world');

    // ...the content was handed to the runtime for delivery...
    expect(runtime.sentMessages).toEqual(['hello world']);

    // ...and status flipped to running (set before awaiting delivery).
    const session = await getJson(`/v1/sessions/${id}`);
    expect((session.json as { status: string }).status).toBe('running');
  });

  it('forwards custom tool results to the runtime without publishing a duplicate user event', async () => {
    const created = await postJson('/v1/sessions', { agent: 'claude' });
    const { id } = created.json as { id: string };
    const runtime = fake.runtimes.get(id)!;

    const payload = {
      type: 'user.custom_tool_result',
      custom_tool_use_id: 'evt_custom_1',
      content: [{ type: 'text', text: 'Ticket T-123 is open.' }],
    };
    const { status, json } = await postJson(`/v1/sessions/${id}/events`, {
      events: [payload],
    });

    expect(status).toBe(200);
    expect(json).toEqual({ ok: true });
    expect(runtime.customToolResults).toEqual([payload]);
    expect(runtime.sentMessages).toEqual([]);
    expect(fake.eventStore.list(id)).toEqual([]);
  });

  it('ignores non-user.message events in the batch', async () => {
    const created = await postJson('/v1/sessions', { agent: 'claude' });
    const { id } = created.json as { id: string };

    const { status } = await postJson(`/v1/sessions/${id}/events`, {
      events: [{ type: 'something.else', content: 'ignored' }],
    });
    expect(status).toBe(200);

    // Nothing published, nothing delivered, status stays idle.
    const events = await getJson(`/v1/sessions/${id}/events`);
    expect((events.json as { data: unknown[] }).data).toEqual([]);
    expect(fake.runtimes.get(id)!.sentMessages).toEqual([]);
    const session = await getJson(`/v1/sessions/${id}`);
    expect((session.json as { status: string }).status).toBe('idle');
  });

  it('returns 409 (not success) when the runtime is dead', async () => {
    const created = await postJson('/v1/sessions', { agent: 'claude' });
    const { id } = created.json as { id: string };

    // Kill the runtime so isAlive() === false.
    fake.runtimes.get(id)!.kill();

    const { status, json } = await postJson(`/v1/sessions/${id}/events`, {
      events: [{ type: 'user.message', content: 'too late' }],
    });

    expect(status).toBe(409);
    // It must NOT pretend success on a dead runtime.
    expect(json).not.toEqual({ ok: true });
    expect((json as { error: { message: string } }).error.message).toMatch(
      /runtime is not available/,
    );

    // Nothing was published or delivered.
    const events = await getJson(`/v1/sessions/${id}/events`);
    expect((events.json as { data: unknown[] }).data).toEqual([]);
    expect(fake.runtimes.get(id)!.sentMessages).toEqual([]);
  });

  it('returns 404 for an unknown session', async () => {
    const { status } = await postJson('/v1/sessions/session_missing/events', {
      events: [{ type: 'user.message', content: 'hi' }],
    });
    expect(status).toBe(404);
  });

  it('publishes session.status_error and sets status error when delivery rejects', async () => {
    const created = await postJson('/v1/sessions', { agent: 'claude' });
    const { id } = created.json as { id: string };
    const runtime = fake.runtimes.get(id)!;

    // Arm the next sendUserMessage to reject (simulated delivery failure).
    runtime.failNextSendWith(new Error('pipe broken'));

    const { status, json } = await postJson(`/v1/sessions/${id}/events`, {
      events: [{ type: 'user.message', content: 'will fail to deliver' }],
    });
    // The route still acknowledges synchronously; the failure surfaces as an event.
    expect(status).toBe(200);
    expect(json).toEqual({ ok: true });

    // The rejection's .catch runs on a later microtask: wait for the error event.
    await waitFor(() => fake.eventStore.list(id).some((e) => e.type === 'session.status_error'));

    const events = fake.eventStore.list(id);
    expect(events.map((e) => e.type)).toEqual(['user.message', 'session.status_error']);
    const errorEvent = events.find((e) => e.type === 'session.status_error') as
      | { error: string }
      | undefined;
    expect(errorEvent?.error).toMatch(/failed to deliver message: pipe broken/);

    // Status was flipped to error by the failure path.
    const session = await getJson(`/v1/sessions/${id}`);
    expect((session.json as { status: string }).status).toBe('error');
  });
});
