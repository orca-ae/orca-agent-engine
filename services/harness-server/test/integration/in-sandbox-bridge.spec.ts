// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Hermetic integration test for DialInTransport + InSandboxHarness.
 *
 * Stands up a minimal in-process fake sandbox-harness wire server (4 endpoints
 * + SSE replay) using node:http, with no real network beyond 127.0.0.1 and no
 * real child processes.
 *
 * Assertions:
 * 1. Normal flow: user.message echo is dropped; agent.message + agent.tool_use
 *    + agent.tool_result are mapped with stable ids; a user.message submit injects the leading
 *    session.status_running + span.model_request_start; and the sandbox's
 *    session.status_idle frame fans out into agent.usage +
 *    span.model_request_end + a terminal session.status_idle{end_turn}.
 *    NOTE: unlike the dispatcher path, this test drives the bridge directly via
 *    harness.events(), so agent.usage IS observed here (SessionRunner is what
 *    diverts it to the usage sink).
 * 2. Reconnect/dedup: server drops the SSE connection mid-stream; each raw frame
 *    that carries an id is delivered exactly once (no duplicate ids after
 *    reconnect).
 */

import { describe, it, expect, afterEach, vi } from 'vitest';
import { createServer, type Server, type ServerResponse, type IncomingMessage } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';
import { DialInTransport } from '../../src/harness/in-sandbox/dial-in.js';
import { InSandboxHarness } from '../../src/harness/in-sandbox/index.js';
import type { AgentEvent, UserEvent } from '../../src/harness/agent-harness.js';
import type {
  HarnessChannel,
  HarnessTransport,
  RawSandboxEvent,
} from '../../src/harness/in-sandbox/transport.js';
import type { ReplayTurn } from '../../src/harness/in-sandbox/replay.js';
import { SessionRunner } from '../../src/runner/session-runner.js';
import {
  expectCanonicalRootTurn,
  expectRequiredActionContinuation,
  expectTerminalError,
  expectToolUseResultCorrelation,
} from '../support/model-summary.js';

// ── helpers ──────────────────────────────────────────────────────────────────

/** Build a stamped sandbox event as the sandbox-harness wire emits. */
function makeEvent(
  id: string,
  sessionId: string,
  type: string,
  extra: Record<string, unknown> = {},
): RawSandboxEvent {
  return {
    id,
    session_id: sessionId,
    created_at: new Date().toISOString(),
    type,
    ...extra,
  };
}

/** Read and JSON-parse a POST body from an IncomingMessage. */
function readBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'));
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

/** Write an SSE data line. */
function writeSse(res: ServerResponse, event: RawSandboxEvent): void {
  res.write(`data: ${JSON.stringify(event)}\n\n`);
}

class ManualChannel implements HarnessChannel {
  private readonly items: RawSandboxEvent[] = [];
  private readonly waiters: Array<(result: IteratorResult<RawSandboxEvent>) => void> = [];
  private closed = false;

  onSubmit: ((event: UserEvent) => void | Promise<void>) | undefined;

  async *events(): AsyncIterable<RawSandboxEvent> {
    while (true) {
      while (this.items.length > 0) {
        const item = this.items.shift();
        if (item !== undefined) yield item;
      }
      if (this.closed) return;
      const result = await new Promise<IteratorResult<RawSandboxEvent>>((resolve) => {
        if (this.items.length > 0) {
          const item = this.items.shift() as RawSandboxEvent;
          resolve({ value: item, done: false });
          return;
        }
        if (this.closed) {
          resolve({ value: undefined as unknown as RawSandboxEvent, done: true });
          return;
        }
        this.waiters.push(resolve);
      });
      if (result.done) return;
      yield result.value;
    }
  }

  async submit(event: UserEvent): Promise<void> {
    await this.onSubmit?.(event);
  }

  async stop(): Promise<void> {
    this.closed = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter({ value: undefined as unknown as RawSandboxEvent, done: true });
    }
  }

  emit(event: RawSandboxEvent): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter({ value: event, done: false });
    } else {
      this.items.push(event);
    }
  }
}

// ── fake server factory ───────────────────────────────────────────────────────

interface FakeServerOptions {
  /** Session events to emit over SSE. */
  events: RawSandboxEvent[];
  /** Emit the scripted events only after a user message is submitted. */
  emitEventsAfterPost?: boolean;
  /**
   * If set, the SSE handler drops the connection after emitting this many events
   * on the FIRST connect, then emits ALL events on subsequent connects (simulating
   * a mid-stream drop for the reconnect-dedup test).
   */
  dropAfterFirst?: number;
  /** Return 502 from /healthz this many times before reporting ready. */
  healthFailuresBeforeReady?: number;
}

interface FakeServer {
  baseUrl: string;
  server: Server;
  /** Ordered method/path pairs observed by the server. */
  requestLog: () => string[];
  /** Number of times the SSE endpoint was connected. */
  sseConnectCount: () => number;
  /** Recorded POST /v1/sessions request body (if captureSessionBody was true). */
  capturedSessionBody: unknown;
  close: () => Promise<void>;
}

function startFakeServer(opts: FakeServerOptions): Promise<FakeServer> {
  return new Promise((resolve) => {
    let sessionId: string | null = null;
    let sseConnects = 0;
    let healthChecks = 0;
    let capturedSessionBody: unknown = undefined;
    const requestLog: string[] = [];
    const sseClients = new Set<ServerResponse>();

    function emitScriptedEvents(): void {
      for (const client of sseClients) {
        for (const ev of opts.events) writeSse(client, ev);
      }
    }

    const server = createServer(async (req, res) => {
      const url = req.url ?? '/';
      const method = req.method ?? 'GET';
      requestLog.push(`${method} ${url}`);

      if (method === 'GET' && url === '/healthz') {
        healthChecks += 1;
        const ready = healthChecks > (opts.healthFailuresBeforeReady ?? 0);
        res.writeHead(ready ? 200 : 502, { 'content-type': 'application/json' });
        res.end(JSON.stringify(ready ? { status: 'ok' } : { error: 'backend not ready' }));
        return;
      }

      // POST /v1/sessions → create session
      if (method === 'POST' && url === '/v1/sessions') {
        capturedSessionBody = await readBody(req);
        sessionId = `session_test_${Date.now()}`;
        res.writeHead(201, { 'content-type': 'application/json' });
        res.end(
          JSON.stringify({
            id: sessionId,
            object: 'session',
            agent: 'test',
            status: 'idle',
            created_at: new Date().toISOString(),
          }),
        );
        return;
      }

      // POST /v1/sessions/:id/events → accept user message
      if (method === 'POST' && sessionId && url === `/v1/sessions/${sessionId}/events`) {
        await readBody(req);
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
        if (opts.emitEventsAfterPost) {
          setTimeout(emitScriptedEvents, 25);
        }
        return;
      }

      // GET /v1/sessions/:id/events/stream → SSE
      if (method === 'GET' && sessionId && url === `/v1/sessions/${sessionId}/events/stream`) {
        sseConnects += 1;
        const connectNum = sseConnects;

        res.writeHead(200, {
          'content-type': 'text/event-stream',
          'cache-control': 'no-cache',
          connection: 'keep-alive',
        });

        if (opts.emitEventsAfterPost) {
          sseClients.add(res);
        } else {
          const eventsToSend =
            opts.dropAfterFirst !== undefined && connectNum === 1
              ? opts.events.slice(0, opts.dropAfterFirst)
              : opts.events;

          for (const ev of eventsToSend) {
            writeSse(res, ev);
          }
        }

        // If this is the first connect and we're simulating a drop, end the
        // response without completing, forcing the transport to reconnect.
        if (opts.dropAfterFirst !== undefined && connectNum === 1) {
          res.end();
          return;
        }

        // Normal: keep open until the client disconnects (res.end on close).
        req.on('close', () => {
          sseClients.delete(res);
          try {
            res.end();
          } catch {
            /* already ended */
          }
        });
        return;
      }

      // DELETE /v1/sessions/:id → tear down
      if (method === 'DELETE' && sessionId && url === `/v1/sessions/${sessionId}`) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ id: sessionId, object: 'session', deleted: true }));
        return;
      }

      res.writeHead(404, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'not found', url, method }));
    });

    server.listen({ host: '127.0.0.1', port: 0 }, () => {
      const addr = server.address() as AddressInfo;
      const fakeServer: FakeServer = {
        baseUrl: `http://127.0.0.1:${addr.port}`,
        server,
        requestLog: () => [...requestLog],
        sseConnectCount: () => sseConnects,
        get capturedSessionBody() {
          return capturedSessionBody;
        },
        close: () =>
          new Promise<void>((r, j) => {
            // Drop undici keep-alive sockets immediately; otherwise server.close()
            // waits ~4s for them to time out before the close callback fires.
            server.closeAllConnections();
            server.close((e) => (e ? j(e) : r()));
          }),
      };
      resolve(fakeServer);
    });
  });
}

// ── collect helper ────────────────────────────────────────────────────────────

/**
 * Collect events from the harness until it emits the unified terminal
 * `session.status_idle` (or the hard terminal `session.status_terminated`), or
 * until `limit` events have been collected.
 * Stops the harness after collection and returns the events.
 */
async function collectUntilTerminal(harness: InSandboxHarness, limit = 20): Promise<AgentEvent[]> {
  const collected: AgentEvent[] = [];
  for await (const ev of harness.events()) {
    collected.push(ev);
    if (ev.kind === 'session.status_idle' || ev.kind === 'session.status_terminated') break;
    if (collected.length >= limit) break;
  }
  return collected;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function stubTranscriptStore(): TranscriptStore {
  return {
    async append(_workspaceId: string, _sessionId: string, events: Event[]): Promise<string[]> {
      return events.map((event) => event.id);
    },
    async *read(_workspaceId: string, _sessionId: string, _opts: ReadOptions) {},
    async *tail(_workspaceId: string, _sessionId: string, _opts: TailOptions) {},
    async archive() {},
    async close() {},
  };
}

// ── test state for cleanup ────────────────────────────────────────────────────

let _harness: InSandboxHarness | undefined;
let _fakeServer: FakeServer | undefined;

afterEach(async () => {
  if (_harness) {
    await _harness.stop('error').catch(() => undefined);
    _harness = undefined;
  }
  if (_fakeServer) {
    await _fakeServer.close().catch(() => undefined);
    _fakeServer = undefined;
  }
});

// ── tests ─────────────────────────────────────────────────────────────────────

describe('in-sandbox bridge', () => {
  it('routes read through the paginated Orca MCP contract', async () => {
    _fakeServer = await startFakeServer({ events: [] });
    _harness = new InSandboxHarness({
      providerId: 'claude',
      port: 4096,
      transport: new DialInTransport({ baseUrl: _fakeServer.baseUrl }),
    });

    await _harness.start({
      workspaceId: 'ws_read_contract',
      sessionId: 'ses_read_contract',
      agentSnapshot: {
        allowed_tool_names: ['read'],
        tool_permission_policies: { mcp__orca__read: 'always_allow' },
      },
    });

    expect(_fakeServer.capturedSessionBody).toMatchObject({
      tools: [],
      allowedTools: ['mcp__orca__read'],
      runtimeTools: ['read'],
    });
  });

  it('waits for the harness health endpoint before creating a session', async () => {
    _fakeServer = await startFakeServer({ events: [], healthFailuresBeforeReady: 2 });
    const transport = new DialInTransport({ baseUrl: _fakeServer.baseUrl });
    const channel = await transport.open({ agent: 'test' });

    try {
      expect(_fakeServer.requestLog()).toEqual([
        'GET /healthz',
        'GET /healthz',
        'GET /healthz',
        'POST /v1/sessions',
      ]);
    } finally {
      await channel.stop();
    }
  });

  it('warns when a stateful guardrail has no request-phase enforcement point', async () => {
    const channel = new ManualChannel();
    const transport: HarnessTransport = { open: async () => channel };
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });
    await _harness.start({
      workspaceId: 'ws_guardrail_warning',
      sessionId: 'ses_guardrail_warning',
      agentSnapshot: {},
      guardrails: [
        {
          id: 'grd_tool_calls',
          name: 'Tool call cap',
          tier: 'workspace',
          phases: ['tool_call'],
          rule: {
            kind: 'builtin',
            builtin: 'max_tool_calls_per_session',
            params: { limit: 5 },
          },
          stateful: true,
          stateScope: 'session',
        },
      ],
    });

    const warning = await _harness.events()[Symbol.asyncIterator]().next();
    expect(warning.value).toMatchObject({
      kind: 'session.warning',
      payload: {
        warning: {
          type: 'guardrail_not_enforced',
        },
        guardrail_id: 'grd_tool_calls',
        guardrail_name: 'Tool call cap',
      },
    });
  });

  it('rejects llm_request guardrails before opening an unenforced sandbox session', async () => {
    const open = vi.fn(async () => new ManualChannel());
    const transport: HarnessTransport = { open };
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });

    await expect(
      _harness.start({
        workspaceId: 'ws_llm_guardrail',
        sessionId: 'ses_llm_guardrail',
        agentSnapshot: {},
        guardrails: [
          {
            id: 'grd_pii',
            name: 'PII screen',
            tier: 'workspace',
            phases: ['request', 'llm_request'],
            rule: { kind: 'builtin', builtin: 'deny_pii_in_llm_request', params: {} },
            stateful: false,
          },
        ],
      }),
    ).rejects.toThrow('in_sandbox cannot enforce llm_request guardrails');
    expect(open).not.toHaveBeenCalled();
  });

  it('refreshes the authenticated daily window before request budget evaluation', async () => {
    const channel = new ManualChannel();
    const open = vi.fn(async () => channel);
    const refreshGuardrailSubjectWindow = vi.fn(async () => ({ daily_cost_usd: 2 }));
    _harness = new InSandboxHarness({
      providerId: 'test',
      port: 4096,
      transport: { open },
      refreshGuardrailSubjectWindow,
    });
    const remoteSubmit = vi.fn();
    channel.onSubmit = remoteSubmit;
    await _harness.start({
      workspaceId: 'ws_daily_budget',
      sessionId: 'ses_daily_budget',
      agentSnapshot: { model_id: 'claude-expensive' },
      guardrails: [
        {
          id: 'grd_daily_budget',
          name: 'Daily budget',
          tier: 'workspace',
          phases: ['request', 'tool_call'],
          rule: {
            kind: 'builtin',
            builtin: 'user_daily_cost_budget',
            params: { max_cost_usd: 1 },
          },
          stateful: true,
          stateScope: 'subject_window',
        },
      ],
    });

    await expect(
      _harness.submit({
        id: 'evt_daily_turn',
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'continue' }] },
      }),
    ).rejects.toThrow("Today's spend has reached its $1.00 budget");
    expect(refreshGuardrailSubjectWindow).toHaveBeenCalledWith('evt_daily_turn');
    expect(remoteSubmit).not.toHaveBeenCalled();
  });

  it('persists acceptance before emitting running events or submitting remotely', async () => {
    const sessionId = 'ses_acceptance_order';
    const channel = new ManualChannel();
    const transport: HarnessTransport = { open: async () => channel };
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });
    let remoteSubmitted = false;
    channel.onSubmit = () => {
      remoteSubmitted = true;
      channel.emit(
        makeEvent('evt_acceptance_idle', sessionId, 'session.status_idle', {
          usage: {},
          total_cost_usd: 0,
        }),
      );
    };
    await _harness.start({
      workspaceId: 'ws_acceptance_order',
      sessionId,
      agentSnapshot: {},
    });
    let releaseAcceptance: (() => void) | null = null;
    const acceptance = new Promise<void>((resolve) => {
      releaseAcceptance = resolve;
    });
    let acceptanceStarted = false;

    const submit = _harness.submit(
      { kind: 'user.message', payload: { content: [{ type: 'text', text: 'hello' }] } },
      {
        onAccepted: () => {
          acceptanceStarted = true;
          return acceptance;
        },
      },
    );

    expect(acceptanceStarted).toBe(true);
    expect(remoteSubmitted).toBe(false);
    expect(
      (_harness as unknown as { items: AgentEvent[] }).items.some(
        (event) => event.kind === 'session.status_running',
      ),
    ).toBe(false);
    releaseAcceptance?.();
    await submit;
    expect(remoteSubmitted).toBe(true);
    const events = await collectUntilTerminal(_harness);
    expect(events[0]?.kind).toBe('session.status_running');
  });

  it('rejects unsupported controls before acceptance or remote submission', async () => {
    const channel = new ManualChannel();
    const transport: HarnessTransport = { open: async () => channel };
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });
    const onAccepted = vi.fn(async () => {});
    const onSubmit = vi.fn();
    channel.onSubmit = onSubmit;
    await _harness.start({
      workspaceId: 'ws_unsupported_control',
      sessionId: 'ses_unsupported_control',
      agentSnapshot: {},
    });

    await expect(
      _harness.submit({ kind: 'user.interrupt', payload: {} }, { onAccepted }),
    ).rejects.toThrow('not supported by the colocated harness protocol');
    expect(onAccepted).not.toHaveBeenCalled();
    expect(onSubmit).not.toHaveBeenCalled();
  });

  it('validates messages before deferring them at a required-action boundary', async () => {
    const sessionId = 'ses_deferred_validation';
    const channel = new ManualChannel();
    const transport: HarnessTransport = { open: async () => channel };
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });
    const onAccepted = vi.fn(async () => {});
    const onSubmit = vi.fn();
    channel.onSubmit = onSubmit;
    await _harness.start({
      workspaceId: 'ws_deferred_validation',
      sessionId,
      agentSnapshot: {},
    });

    channel.emit(
      makeEvent('evt_pending_tool', sessionId, 'agent.custom_tool_use', {
        name: 'client_tool',
        input: {},
      }),
    );
    await collectUntilTerminal(_harness);
    expect(_harness.hasPendingRequiredAction()).toBe(true);

    await expect(
      _harness.submit(
        { kind: 'user.message', payload: { content: [{ type: 'image', source: {} }] } },
        { onAccepted },
      ),
    ).rejects.toThrow('supported non-empty text content');
    await expect(
      _harness.submit(
        { kind: 'user.message', payload: { content: [{ type: 'text', text: 'later' }] } },
        { onAccepted },
      ),
    ).resolves.toBe('deferred');
    expect(onAccepted).not.toHaveBeenCalled();
    expect(onSubmit).not.toHaveBeenCalled();
  });

  it('does not wait for turn completion until every custom tool result is submitted', async () => {
    const sessionId = 'ses_multiple_custom_results';
    const channel = new ManualChannel();
    const transport: HarnessTransport = { open: async () => channel };
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });
    let submitCount = 0;
    channel.onSubmit = () => {
      submitCount += 1;
      if (submitCount === 2) {
        channel.emit(
          makeEvent('evt_all_results_idle', sessionId, 'session.status_idle', {
            usage: {},
            total_cost_usd: 0,
          }),
        );
      }
    };
    await _harness.start({
      workspaceId: 'ws_multiple_custom_results',
      sessionId,
      agentSnapshot: {},
    });

    channel.emit(
      makeEvent('evt_custom_1', sessionId, 'agent.custom_tool_use', {
        name: 'client_tool_1',
        input: {},
      }),
    );
    channel.emit(
      makeEvent('evt_custom_2', sessionId, 'agent.custom_tool_use', {
        name: 'client_tool_2',
        input: {},
      }),
    );
    await collectUntilTerminal(_harness);
    await collectUntilTerminal(_harness);
    expect(_harness.hasPendingRequiredAction()).toBe(true);

    await expect(
      _harness.submit({
        kind: 'user.custom_tool_result',
        payload: { custom_tool_use_id: 'evt_custom_1', result: 'first' },
      }),
    ).resolves.toBeUndefined();
    expect(submitCount).toBe(1);
    expect(_harness.hasPendingRequiredAction()).toBe(true);

    await expect(
      _harness.submit({
        kind: 'user.custom_tool_result',
        payload: { custom_tool_use_id: 'evt_custom_2', result: 'second' },
      }),
    ).resolves.toBeUndefined();
    expect(submitCount).toBe(2);
    expect(_harness.hasPendingRequiredAction()).toBe(false);
  });

  it('keeps a custom tool result pending when remote submission fails', async () => {
    const sessionId = 'ses_retry_custom_result';
    const channel = new ManualChannel();
    const transport: HarnessTransport = { open: async () => channel };
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });
    let submitCount = 0;
    channel.onSubmit = () => {
      submitCount += 1;
      if (submitCount === 1) throw new Error('remote submit failed');
      channel.emit(
        makeEvent('evt_retry_result_idle', sessionId, 'session.status_idle', {
          usage: {},
          total_cost_usd: 0,
        }),
      );
    };
    await _harness.start({
      workspaceId: 'ws_retry_custom_result',
      sessionId,
      agentSnapshot: {},
    });

    channel.emit(
      makeEvent('evt_retry_custom_tool', sessionId, 'agent.custom_tool_use', {
        name: 'client_tool',
        input: {},
      }),
    );
    await collectUntilTerminal(_harness);

    const result: UserEvent = {
      kind: 'user.custom_tool_result',
      payload: { custom_tool_use_id: 'evt_retry_custom_tool', result: 'done' },
    };
    await expect(_harness.submit(result)).rejects.toThrow('remote submit failed');
    expect(_harness.hasPendingRequiredAction()).toBe(true);

    await expect(_harness.submit(result)).resolves.toBeUndefined();
    expect(submitCount).toBe(2);
    expect(_harness.hasPendingRequiredAction()).toBe(false);
  });

  it('bounds final custom tool result redelivery when the sandbox stays silent', async () => {
    const sessionId = 'ses_ambiguous_custom_result';
    const channel = new ManualChannel();
    const transport: HarnessTransport = { open: async () => channel };
    _harness = new InSandboxHarness({
      providerId: 'test',
      port: 4096,
      transport,
      customToolResultTerminalTimeoutMs: 20,
    });
    let finalResultSubmitCount = 0;
    channel.onSubmit = (event) => {
      const customToolUseId = (event.payload as { custom_tool_use_id?: string }).custom_tool_use_id;
      if (customToolUseId === 'evt_ambiguous_custom_tool_1') return;
      finalResultSubmitCount += 1;
      if (finalResultSubmitCount === 1) {
        setTimeout(() => {
          channel.emit(
            makeEvent('evt_ambiguous_result_idle', sessionId, 'session.status_idle', {
              usage: {},
              total_cost_usd: 0,
            }),
          );
        }, 0);
        throw new Error('response lost after delivery');
      }
      // Duplicate-after-apply and sandbox-restart mismatches are stderr-only on
      // the sandbox wire. The host-side timeout must bound this silence.
    };
    await _harness.start({
      workspaceId: 'ws_ambiguous_custom_result',
      sessionId,
      agentSnapshot: {},
    });

    channel.emit(
      makeEvent('evt_ambiguous_custom_tool_1', sessionId, 'agent.custom_tool_use', {
        name: 'client_tool_1',
        input: {},
      }),
    );
    await collectUntilTerminal(_harness);
    channel.emit(
      makeEvent('evt_ambiguous_custom_tool_2', sessionId, 'agent.custom_tool_use', {
        name: 'client_tool_2',
        input: {},
      }),
    );
    await collectUntilTerminal(_harness);

    await expect(
      _harness.submit({
        kind: 'user.custom_tool_result',
        payload: { custom_tool_use_id: 'evt_ambiguous_custom_tool_1', result: 'first' },
      }),
    ).resolves.toBeUndefined();

    const result: UserEvent = {
      kind: 'user.custom_tool_result',
      payload: { custom_tool_use_id: 'evt_ambiguous_custom_tool_2', result: 'second' },
    };
    await expect(_harness.submit(result)).rejects.toThrow('response lost after delivery');
    expect(_harness.hasPendingRequiredAction()).toBe(true);
    // The original turn completed after the lost response, when the failed
    // submission's terminal waiter had already been canceled.
    await collectUntilTerminal(_harness);

    await expect(_harness.submit(result)).rejects.toThrow(
      'timed out after 20ms waiting for the custom tool result turn to finish',
    );
    expect(finalResultSubmitCount).toBe(2);
    expect(_harness.hasPendingRequiredAction()).toBe(true);
  });

  it('maps events correctly: user-echo dropped, agent events mapped with ids, terminal emitted', async () => {
    const SESSION_ID = 'ses_test_1';
    const events: RawSandboxEvent[] = [
      makeEvent('evt_1', SESSION_ID, 'user.message', { content: 'hello' }),
      makeEvent('evt_2', SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'Hi there!' }],
      }),
      makeEvent('evt_3', SESSION_ID, 'agent.tool_use', {
        name: 'Bash',
        input: { command: 'ls' },
        tool_use_id: 'tu_1',
      }),
      makeEvent('evt_4', SESSION_ID, 'agent.tool_result', {
        tool_use_id: 'tu_1',
        content: 'file list',
        is_error: false,
      }),
      makeEvent('evt_5', SESSION_ID, 'session.status_idle', {
        usage: { input_tokens: 10 },
        total_cost_usd: 0.001,
      }),
    ];

    _fakeServer = await startFakeServer({ events });
    const transport = new DialInTransport({ baseUrl: _fakeServer.baseUrl });
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });

    await _harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test_1',
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-3-5-sonnet-20241022',
      },
    });

    // Submit a user message.
    await _harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'hello' }] },
    });

    const collected = await collectUntilTerminal(_harness);
    await _harness.stop('client.archived');

    // user.message echo (evt_1) should be DROPPED.
    const ids = collected.map((e) => e.id);
    expect(ids).not.toContain('evt_1');

    // agent.message (evt_2) should be present.
    const agentMsg = collected.find((e) => e.id === 'evt_2');
    expect(agentMsg).toBeDefined();
    expect(agentMsg?.kind).toBe('agent.message');

    // agent.tool_use (evt_3) should be present.
    const toolUse = collected.find((e) => e.id === 'evt_3');
    expect(toolUse).toBeDefined();
    expect(toolUse?.kind).toBe('agent.tool_use');
    const toolResult = collected.find((e) => e.id === 'evt_4');
    expect(toolResult).toBeDefined();
    expect(toolResult?.kind).toBe('agent.tool_result');

    // session.status_idle (evt_5) fans out: every sibling gets a
    // stable idempotency key derived from the raw frame id.
    const usage = collected.find((e) => e.id === 'evt_5');
    expect(usage).toBeDefined();
    expect(usage?.kind).toBe('agent.usage');

    // The submit() opened the turn with session.status_running +
    // span.model_request_start, and pump() correlates the closing
    // span.model_request_end back to that start id.
    const { start: startSpan, end: endSpan } = expectCanonicalRootTurn(collected, {
      provider: 'anthropic',
      model: 'claude-3-5-sonnet-20241022',
    });
    expectToolUseResultCorrelation(collected);
    const startId = startSpan.id;
    expect(startSpan.payload).toEqual({
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'claude-3-5-sonnet-20241022',
    });
    expect(endSpan.id).toMatch(/^evt_/);
    expect(endSpan.payload).toMatchObject({
      model_request_start_id: startId,
      model_observation_kind: 'turn_model_summary',
    });

    // Full turn shape (submit-injected lead-in, mapped content, fanned-out idle):
    expect(collected.map((e) => e.kind)).toEqual([
      'session.status_running',
      'span.model_request_start',
      'agent.message',
      'agent.tool_use',
      'agent.tool_result',
      'agent.usage',
      'span.model_request_end',
      'session.status_idle',
    ]);
    expect(collected.length).toBe(8);
  });

  it('maps a terminal sandbox error as a typed root-path terminal error', async () => {
    const sessionId = 'ses_terminal_error';
    const channel = new ManualChannel();
    _harness = new InSandboxHarness({
      providerId: 'test',
      port: 4096,
      transport: { open: async () => channel },
    });
    channel.onSubmit = () => {
      channel.emit(
        makeEvent('evt_terminal_error', sessionId, 'session.status_error', {
          error: 'sandbox model failed',
        }),
      );
    };

    await _harness.start({
      workspaceId: 'ws_terminal_error',
      sessionId,
      agentSnapshot: {},
    });
    await _harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'trigger error' }] },
    });
    const collected = await collectUntilTerminal(_harness);

    const { error, terminalIdle } = expectTerminalError(collected);
    expect((error.payload as { error: { message: string } }).error.message).toBe(
      'sandbox model failed',
    );
    expect((terminalIdle.payload as { stop_reason: { type: string } }).stop_reason.type).toBe(
      'retries_exhausted',
    );
  });

  it('submit waits until the sandbox emits a terminal turn event', async () => {
    const SESSION_ID = 'ses_test_submit_wait';
    const events: RawSandboxEvent[] = [
      makeEvent('evt_sw1', SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'working' }],
      }),
      makeEvent('evt_sw2', SESSION_ID, 'session.status_idle', { usage: {}, total_cost_usd: 0 }),
    ];

    _fakeServer = await startFakeServer({ events, emitEventsAfterPost: true });
    const transport = new DialInTransport({ baseUrl: _fakeServer.baseUrl });
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });

    await _harness.start({
      workspaceId: 'ws_submit_wait',
      sessionId: SESSION_ID,
      agentSnapshot: {},
    });

    let submitResolved = false;
    const submitPromise = _harness
      .submit({ kind: 'user.message', payload: { content: [{ type: 'text', text: 'hello' }] } })
      .then(() => {
        submitResolved = true;
      });

    await sleep(5);
    expect(submitResolved).toBe(false);

    await submitPromise;
    expect(submitResolved).toBe(true);

    const collected = await collectUntilTerminal(_harness);
    // submit injects the leading running/start; the sandbox's agent.message and
    // its session.status_idle (fanned out into usage + model_request_end + idle)
    // follow after the POST.
    expect(collected.map((e) => e.kind)).toEqual([
      'session.status_running',
      'span.model_request_start',
      'agent.message',
      'agent.usage',
      'span.model_request_end',
      'session.status_idle',
    ]);
  });

  it('keeps one turn model summary across a required-action pause and resume', async () => {
    const sessionId = 'ses_required_action_summary';
    const customToolUseId = 'evt_required_action_tool';
    const channel = new ManualChannel();
    const transport: HarnessTransport = { open: async () => channel };
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });

    channel.onSubmit = (event) => {
      if (event.kind === 'user.message') {
        channel.emit(
          makeEvent(customToolUseId, sessionId, 'agent.custom_tool_use', {
            name: 'client_tool',
            input: { ticket: 'T-123' },
          }),
        );
        return;
      }
      channel.emit(
        makeEvent('evt_required_action_done', sessionId, 'session.status_idle', {
          usage: { input_tokens: 7, output_tokens: 3 },
          total_cost_usd: 0.01,
        }),
      );
    };

    await _harness.start({
      workspaceId: 'ws_required_action_summary',
      sessionId,
      agentSnapshot: { model_provider: 'anthropic', model_id: 'claude-test-model' },
    });

    await _harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'use the client tool' }] },
    });
    const paused = await collectUntilTerminal(_harness);
    const originalStart = paused.find((event) => event.kind === 'span.model_request_start');
    expect(originalStart?.id).toMatch(/^evt_/);
    expect(paused.map((event) => event.kind)).toEqual([
      'session.status_running',
      'span.model_request_start',
      'agent.custom_tool_use',
      'session.status_idle',
    ]);
    expect(_harness.hasPendingRequiredAction()).toBe(true);

    await _harness.submit({
      kind: 'user.custom_tool_result',
      payload: { custom_tool_use_id: customToolUseId, content: 'done' },
    });
    const resumed = await collectUntilTerminal(_harness);
    expect(resumed.map((event) => event.kind)).toEqual([
      'session.status_running',
      'agent.usage',
      'span.model_request_end',
      'session.status_idle',
    ]);
    expect(resumed.some((event) => event.kind === 'span.model_request_start')).toBe(false);

    const turnEvents = [...paused, ...resumed];
    const { start, end } = expectCanonicalRootTurn(turnEvents, {
      provider: 'anthropic',
      model: 'claude-test-model',
    });
    expectRequiredActionContinuation(turnEvents);
    expect(start.id).toBe(originalStart?.id);
    expect((end.payload as { model_request_start_id: string }).model_request_start_id).toBe(
      originalStart?.id,
    );
    expect(_harness.hasPendingRequiredAction()).toBe(false);
  });

  it('propagates invalid sandbox event IDs and poisons the SessionRunner', async () => {
    const sessionId = 'ses_invalid_sandbox_event_id';
    const channel = new ManualChannel();
    channel.onSubmit = () => {
      channel.emit(
        makeEvent('toolu_not_canonical', sessionId, 'agent.custom_tool_use', {
          name: 'client_tool',
          input: {},
        }),
      );
    };
    const harness = new InSandboxHarness({
      providerId: 'test',
      port: 4096,
      transport: { open: async () => channel },
    });
    const runner = new SessionRunner({
      workspaceId: 'ws_invalid_sandbox_event_id',
      sessionId,
      harness,
      store: stubTranscriptStore(),
    });

    await runner.start({ agentSnapshot: {} });
    await expect(
      runner.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'trigger malformed tool event' }] },
      }),
    ).rejects.toThrow('agent.custom_tool_use requires a canonical sandbox event id');

    await vi.waitFor(() =>
      expect((runner as unknown as { pumpFailure: Error | null }).pumpFailure).toBeInstanceOf(
        Error,
      ),
    );
    await expect(
      runner.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'must stay poisoned' }] },
      }),
    ).rejects.toThrow('agent.custom_tool_use requires a canonical sandbox event id');
    await runner.stop('error');
  });

  it('rejects submit when the event stream closes during durable acceptance', async () => {
    const sessionId = 'ses_acceptance_stream_close';
    const channel = new ManualChannel();
    _harness = new InSandboxHarness({
      providerId: 'test',
      port: 4096,
      transport: { open: async () => channel },
    });
    await _harness.start({
      workspaceId: 'ws_acceptance_stream_close',
      sessionId,
      agentSnapshot: {},
    });

    const events: AgentEvent[] = [];
    const collector = (async (): Promise<Error | null> => {
      try {
        for await (const event of _harness!.events()) events.push(event);
        return null;
      } catch (error) {
        return error instanceof Error ? error : new Error(String(error));
      }
    })();
    let releaseAcceptance!: () => void;
    let acceptanceStarted = false;
    const acceptanceGate = new Promise<void>((resolve) => {
      releaseAcceptance = resolve;
    });
    const submit = _harness.submit(
      {
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'wait for durable acceptance' }] },
      },
      {
        onAccepted: () => {
          acceptanceStarted = true;
          return acceptanceGate;
        },
      },
    );
    await vi.waitFor(() => expect(acceptanceStarted).toBe(true));

    await channel.stop();
    await vi.waitFor(() =>
      expect((_harness as unknown as { pumpFailure: Error | null }).pumpFailure).toBeInstanceOf(
        Error,
      ),
    );
    releaseAcceptance();

    await expect(submit).rejects.toThrow('event stream closed unexpectedly');
    await expect(collector).resolves.toMatchObject({
      message: expect.stringContaining('event stream closed unexpectedly'),
    });
    expect(events.some((event) => event.kind === 'session.status_running')).toBe(false);
    await expect(
      _harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'must reject again' }] },
      }),
    ).rejects.toThrow('event stream closed unexpectedly');
  });

  it('clears turn-model-summary correlation after terminal events', async () => {
    const SESSION_ID = 'ses_test_clear_span';
    const channel = new ManualChannel();
    const transport: HarnessTransport = { open: async () => channel };
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });

    let submitCount = 0;
    channel.onSubmit = () => {
      submitCount += 1;
      channel.emit(
        makeEvent(`evt_clear_${submitCount}`, SESSION_ID, 'session.status_idle', {
          usage: {},
          total_cost_usd: 0,
        }),
      );
    };

    await _harness.start({
      workspaceId: 'ws_clear_span',
      sessionId: SESSION_ID,
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-3-5-sonnet-20241022',
      },
    });

    await _harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'hello' }] },
    });
    const firstTurn = await collectUntilTerminal(_harness);
    const firstStart = firstTurn.find((e) => e.kind === 'span.model_request_start');
    const firstEnd = firstTurn.find((e) => e.kind === 'span.model_request_end');
    const firstStartId = firstStart?.id;
    expect(firstStartId).toBeDefined();
    expect(firstStart?.payload).toEqual({
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'claude-3-5-sonnet-20241022',
    });
    expect(firstEnd?.payload).toMatchObject({
      model_request_start_id: firstStartId,
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'claude-3-5-sonnet-20241022',
    });

    channel.emit(
      makeEvent('evt_tu_1', SESSION_ID, 'agent.custom_tool_use', {
        name: 'client_tool',
        input: {},
      }),
    );
    const requiredAction = await collectUntilTerminal(_harness);
    expect(requiredAction.some((event) => event.kind === 'agent.custom_tool_use')).toBe(true);
    expect(_harness.hasPendingRequiredAction()).toBe(true);

    await _harness.submit({
      kind: 'user.custom_tool_result',
      payload: { tool_use_id: 'evt_tu_1' },
    });
    expect(_harness.hasPendingRequiredAction()).toBe(false);
    const secondSubmit = await collectUntilTerminal(_harness);
    const secondStart = secondSubmit.find((e) => e.kind === 'span.model_request_start');
    const secondEnd = secondSubmit.find((e) => e.kind === 'span.model_request_end');
    const secondStartId = secondStart?.id;

    expect(secondStartId).toBeDefined();
    expect(secondEnd?.payload).toMatchObject({
      model_request_start_id: secondStartId,
      model_observation_kind: 'turn_model_summary',
      provider: 'anthropic',
      model: 'claude-3-5-sonnet-20241022',
    });
  });

  it('reconnect-dedup: each event delivered exactly once even when SSE drops mid-stream', async () => {
    const SESSION_ID = 'ses_test_reconnect';
    // 4 events; server drops SSE after 2 on first connect, then replays all 4 on reconnect.
    const events: RawSandboxEvent[] = [
      makeEvent('evt_r1', SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'msg1' }],
      }),
      makeEvent('evt_r2', SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'msg2' }],
      }),
      makeEvent('evt_r3', SESSION_ID, 'agent.tool_use', {
        name: 'Bash',
        input: { command: 'pwd' },
        tool_use_id: 'tu_r1',
      }),
      makeEvent('evt_r4', SESSION_ID, 'session.status_idle', { usage: {}, total_cost_usd: 0 }),
    ];

    _fakeServer = await startFakeServer({ events, dropAfterFirst: 2 });
    const transport = new DialInTransport({ baseUrl: _fakeServer.baseUrl });
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });

    await _harness.start({
      workspaceId: 'ws_reconnect',
      sessionId: SESSION_ID,
      agentSnapshot: {},
    });

    const collected = await collectUntilTerminal(_harness);
    await _harness.stop('client.archived');

    // No submit here, so there is no leading running event. The four raw frames map to:
    // agent.message x2, agent.tool_use, then the session.status_idle fan-out.
    // Because no submit opened a model span, the pump synthesizes the matching
    // span.model_request_start before span.model_request_end.
    // Dedup is a property of the RAW frames, which are keyed by id — the
    // fanned-out span/idle carry derived ids, so replay dedups each sibling.
    const ids = collected.map((event) => event.id);
    const uniqueIds = new Set(ids);

    // Each id-bearing event appears exactly once (no duplicates from reconnect replay).
    expect(uniqueIds.size).toBe(ids.length);

    // SSE was connected at least twice (the reconnect happened).
    expect(_fakeServer.sseConnectCount()).toBeGreaterThanOrEqual(2);

    // All raw frames and derived fan-out siblings are present exactly once.
    expect(uniqueIds.has('evt_r1')).toBe(true);
    expect(uniqueIds.has('evt_r2')).toBe(true);
    expect(uniqueIds.has('evt_r3')).toBe(true); // agent.tool_use
    expect(uniqueIds.has('evt_r4')).toBe(true); // agent.usage sibling of session.status_idle
    expect(uniqueIds.has('evt_r4:model_end')).toBe(true);
    expect(uniqueIds.has('evt_r4:idle')).toBe(true);
    expect(ids.length).toBe(7);

    // The terminal event is last; it carries a derived id from the raw frame.
    const last = collected[collected.length - 1];
    expect(last?.kind).toBe('session.status_idle');
    expect(last?.id).toBe('evt_r4:idle');

    // Full mapped sequence: two messages, a tool_use, then the idle fan-out.
    expect(collected.map((e) => e.kind)).toEqual([
      'agent.message',
      'agent.message',
      'agent.tool_use',
      'agent.usage',
      'span.model_request_start',
      'span.model_request_end',
      'session.status_idle',
    ]);
  });

  it('replaySource: prior turns forwarded to POST /v1/sessions as replay field', async () => {
    const SESSION_ID = 'ses_test_replay';
    const events: RawSandboxEvent[] = [
      makeEvent('evt_rp1', SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'context restored' }],
      }),
      makeEvent('evt_rp2', SESSION_ID, 'session.status_idle', { usage: {}, total_cost_usd: 0 }),
    ];

    _fakeServer = await startFakeServer({ events });
    const transport = new DialInTransport({ baseUrl: _fakeServer.baseUrl });

    const priorTurns: ReplayTurn[] = [
      { role: 'user', text: 'prior user message' },
      { role: 'assistant', text: 'prior assistant reply' },
    ];

    _harness = new InSandboxHarness({
      providerId: 'test',
      port: 4096,
      transport,
      replaySource: async () => priorTurns,
    });

    await _harness.start({
      workspaceId: 'ws_replay',
      sessionId: SESSION_ID,
      agentSnapshot: { model_id: 'claude-3-5-sonnet-20241022' },
    });

    const collected = await collectUntilTerminal(_harness);
    await _harness.stop('client.archived');

    // The fake server captured the POST /v1/sessions body — assert replay is present.
    const body = _fakeServer.capturedSessionBody as Record<string, unknown>;
    expect(body['replay']).toBeDefined();
    expect(body['model']).toBe('claude-3-5-sonnet-20241022');
    expect(body['replay']).toEqual([
      { role: 'user', text: 'prior user message' },
      { role: 'assistant', text: 'prior assistant reply' },
    ]);

    // Events are still mapped correctly (no submit → no leading running;
    // the pump supplies the model-request start/end pair before terminal idle).
    expect(collected.map((e) => e.kind)).toEqual([
      'agent.message',
      'agent.usage',
      'span.model_request_start',
      'span.model_request_end',
      'session.status_idle',
    ]);
  });

  it('replaySource absent: no replay sent to POST /v1/sessions (cold fresh start)', async () => {
    const SESSION_ID = 'ses_test_no_replay';
    const events: RawSandboxEvent[] = [
      makeEvent('evt_nr1', SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'fresh start' }],
      }),
      makeEvent('evt_nr2', SESSION_ID, 'session.status_idle', { usage: {}, total_cost_usd: 0 }),
    ];

    _fakeServer = await startFakeServer({ events });
    const transport = new DialInTransport({ baseUrl: _fakeServer.baseUrl });

    // No replaySource — should behave like the original cold-start.
    _harness = new InSandboxHarness({ providerId: 'test', port: 4096, transport });

    await _harness.start({
      workspaceId: 'ws_no_replay',
      sessionId: SESSION_ID,
      agentSnapshot: {},
    });

    const collected = await collectUntilTerminal(_harness);
    await _harness.stop('client.archived');

    // No replay field in the POST body.
    const body = _fakeServer.capturedSessionBody as Record<string, unknown>;
    expect(body['replay']).toBeUndefined();
    expect(body['model']).toBeUndefined();

    // Events still delivered: agent.message + the session.status_idle fan-out.
    expect(collected.map((e) => e.kind)).toEqual([
      'agent.message',
      'agent.usage',
      'span.model_request_start',
      'span.model_request_end',
      'session.status_idle',
    ]);
  });

  it('endpoint-resolution: resolves transport from sandbox.endpoint(port) when no transport override given', async () => {
    const SESSION_ID = 'ses_test_endpoint';
    const PORT = 4096;
    const events: RawSandboxEvent[] = [
      makeEvent('evt_e1', SESSION_ID, 'agent.message', {
        content: [{ type: 'text', text: 'from endpoint' }],
      }),
      makeEvent('evt_e2', SESSION_ID, 'session.status_idle', { usage: {}, total_cost_usd: 0 }),
    ];

    // Reuse the existing fake server — its base URL will be returned by the sandbox endpoint.
    _fakeServer = await startFakeServer({ events });
    const fakeServerUrl = _fakeServer.baseUrl;

    // Fake SandboxHandle whose endpoint(port) returns the fake server URL.
    const fakeSandbox = {
      id: 'sandbox_endpoint_test',
      endpoint: async (port: number) => {
        expect(port).toBe(PORT);
        return { url: fakeServerUrl };
      },
      run: async () => ({ stdout: '' }),
      files: {
        write: async () => undefined,
        read: async () => Buffer.alloc(0),
        readUtf8Page: async () => {
          throw new Error('files.readUtf8Page not used');
        },
        list: async () => [],
        chmod: async () => undefined,
        delete: async () => undefined,
      },
      runPrivileged: async () => ({ stdout: '' }),
      pause: async () => undefined,
      resume: async () => undefined,
      destroy: async () => undefined,
    };

    // Construct with NO transport override — must resolve from sandbox.endpoint.
    _harness = new InSandboxHarness({ providerId: 'test', port: PORT });

    await _harness.start({
      workspaceId: 'ws_endpoint',
      sessionId: SESSION_ID,
      agentSnapshot: { model_id: 'claude-3-5-sonnet-20241022' },
      sandbox: fakeSandbox,
    });

    const collected = await collectUntilTerminal(_harness);
    await _harness.stop('client.archived');

    // agent.message (evt_e1) should be present.
    const agentMsg = collected.find((e) => e.id === 'evt_e1');
    expect(agentMsg).toBeDefined();
    expect(agentMsg?.kind).toBe('agent.message');

    // session.status_idle (evt_e2) fans out: the frame id rides on agent.usage,
    // and public siblings carry derived ids.
    const usage = collected.find((e) => e.id === 'evt_e2');
    expect(usage).toBeDefined();
    expect(usage?.kind).toBe('agent.usage');

    // Order and count (no submit → no leading running; pump supplies the span start).
    expect(collected.map((e) => e.kind)).toEqual([
      'agent.message',
      'agent.usage',
      'span.model_request_start',
      'span.model_request_end',
      'session.status_idle',
    ]);
  });
});
