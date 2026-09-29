// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { OrcaClient, resolveClientConfig, authHeaders } from '../../src/client.js';
import { fakeRegistry, scriptSse } from '../fakes/registry.js';
// The registry's own event-type set, so the reason `stream()` sends `orca-beta`
// is pinned to the filter it works around rather than restated from memory.
import { CLAUDE_SESSION_EVENT_TYPES } from '../../../../services/registry-service-ts/src/domain/events.js';

const BASE = 'http://localhost:8080';

describe('resolveClientConfig', () => {
  it('reads baseURL and apiKey from the ORCA_* env', () => {
    const cfg = resolveClientConfig({ ORCA_BASE_URL: 'http://reg:9000', ORCA_API_KEY: 'sk-1' });
    expect(cfg).toEqual({ baseURL: 'http://reg:9000', apiKey: 'sk-1' });
  });

  it('defaults baseURL to the local stack port when unset', () => {
    const cfg = resolveClientConfig({ ORCA_API_KEY: 'sk-1' });
    expect(cfg.baseURL).toBe('http://localhost:8080');
  });

  it('strips a trailing slash from baseURL', () => {
    const cfg = resolveClientConfig({ ORCA_BASE_URL: 'http://reg:9000/', ORCA_API_KEY: 'sk-1' });
    expect(cfg.baseURL).toBe('http://reg:9000');
  });

  it('throws a guiding error when the api key is missing', () => {
    expect(() => resolveClientConfig({})).toThrow(/ORCA_API_KEY/);
  });
});

describe('authHeaders', () => {
  it('sends the Anthropic-canonical x-api-key plus a beta flag', () => {
    const headers = authHeaders({ baseURL: BASE, apiKey: 'sk-secret' });
    expect(headers['x-api-key']).toBe('sk-secret');
    expect(headers['anthropic-beta']).toMatch(/managed-agents/);
  });
});

describe('OrcaClient.createEnvironment', () => {
  it('POSTs /v1/environments with name + target and returns the env (incl. env_key)', async () => {
    const reg = fakeRegistry({
      'POST /v1/environments': {
        status: 201,
        json: { id: 'env_1', name: 'laptop', target: 'self_hosted', env_key: 'envk_raw_123' },
      },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const env = await client.createEnvironment({ name: 'laptop', target: 'self_hosted' });

    expect(env.id).toBe('env_1');
    expect(env.env_key).toBe('envk_raw_123');
    const req = reg.requestsFor('POST /v1/environments')[0]!;
    expect(req.headers['x-api-key']).toBe('sk-1');
    expect(req.headers['content-type']).toContain('application/json');
    expect(req.body).toEqual({ name: 'laptop', target: 'self_hosted' });
  });

  // The header is load-bearing, not cosmetic. The registry's default create
  // response is Anthropic's `BetaEnvironment` projection, which has no key
  // concept; `env_key` is echoed only to an `orca-beta` caller. Drop this
  // header and `orca env create` produces an environment whose one-time key is
  // gone for good, so this asserts the opt-in is actually sent.
  it('sends orca-beta, without which the registry never echoes env_key', async () => {
    const reg = fakeRegistry({
      'POST /v1/environments': { status: 200, json: { id: 'env_1', env_key: 'k' } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await client.createEnvironment({ name: 'laptop' });

    expect(reg.requestsFor('POST /v1/environments')[0]!.headers['orca-beta']).toBeTruthy();
  });
});

describe('OrcaClient.createSession', () => {
  it('POSTs /v1/sessions with agent_id + environment_id and returns the session', async () => {
    const reg = fakeRegistry({
      'POST /v1/sessions': { status: 201, json: { id: 'ses_1', status: 'idle' } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const session = await client.createSession({ agentId: 'agt_1', environmentId: 'env_1' });

    expect(session.id).toBe('ses_1');
    const req = reg.requestsFor('POST /v1/sessions')[0]!;
    expect(req.body).toEqual({ agent_id: 'agt_1', environment_id: 'env_1' });
  });

  it('omits environment_id when not provided', async () => {
    const reg = fakeRegistry({
      'POST /v1/sessions': { status: 201, json: { id: 'ses_2', status: 'idle' } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await client.createSession({ agentId: 'agt_9' });
    expect(reg.requestsFor('POST /v1/sessions')[0]!.body).toEqual({ agent_id: 'agt_9' });
  });
});

describe('OrcaClient.getSession', () => {
  it('GETs /v1/sessions/:id', async () => {
    const reg = fakeRegistry({
      'GET /v1/sessions/ses_1': { json: { id: 'ses_1', status: 'running' } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const session = await client.getSession('ses_1');
    expect(session.status).toBe('running');
  });
});

describe('OrcaClient.postUserMessage', () => {
  it('POSTs a user.message event with a text content block', async () => {
    const reg = fakeRegistry({
      'POST /v1/sessions/ses_1/events': { json: { events: [] } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await client.postUserMessage('ses_1', 'do the thing');

    const req = reg.requestsFor('POST /v1/sessions/ses_1/events')[0]!;
    expect(req.body).toEqual({
      events: [{ type: 'user.message', content: [{ type: 'text', text: 'do the thing' }] }],
    });
  });
});

describe('OrcaClient.postToolConfirmation', () => {
  it('POSTs a user.tool_confirmation with the routing id and result', async () => {
    const reg = fakeRegistry({
      'POST /v1/sessions/ses_1/events': { json: { events: [] } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await client.postToolConfirmation('ses_1', 'tool_abc', 'allow');

    const req = reg.requestsFor('POST /v1/sessions/ses_1/events')[0]!;
    expect(req.body).toEqual({
      events: [{ type: 'user.tool_confirmation', tool_use_id: 'tool_abc', result: 'allow' }],
    });
  });

  it('encodes a deny result', async () => {
    const reg = fakeRegistry({
      'POST /v1/sessions/ses_1/events': { json: { events: [] } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await client.postToolConfirmation('ses_1', 'tool_xyz', 'deny');
    const req = reg.requestsFor('POST /v1/sessions/ses_1/events')[0]!;
    expect((req.body as { events: Array<{ result: string }> }).events[0]!.result).toBe('deny');
  });

  it('carries deny_message only on a deny', async () => {
    const reg = fakeRegistry({
      'POST /v1/sessions/ses_1/events': { json: { events: [] } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await client.postToolConfirmation('ses_1', 'tool_xyz', 'deny', 'not on a shared host');
    await client.postToolConfirmation('ses_1', 'tool_abc', 'allow', 'ignored');

    const posts = reg.requestsFor('POST /v1/sessions/ses_1/events');
    expect((posts[0]!.body as { events: Array<Record<string, unknown>> }).events[0]).toEqual({
      type: 'user.tool_confirmation',
      tool_use_id: 'tool_xyz',
      result: 'deny',
      deny_message: 'not on a shared host',
    });
    // `deny_message` alongside `result: 'allow'` is a contract violation the
    // registry rejects outright (`superRefine`), so the client must drop it.
    expect((posts[1]!.body as { events: Array<Record<string, unknown>> }).events[0]).toEqual({
      type: 'user.tool_confirmation',
      tool_use_id: 'tool_abc',
      result: 'allow',
    });
  });
});

describe('OrcaClient error handling', () => {
  it('throws with status + body when a request fails', async () => {
    const reg = fakeRegistry({
      'POST /v1/sessions': { status: 404, json: { error: 'agent not found' } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await expect(client.createSession({ agentId: 'nope' })).rejects.toThrow(/agent not found/);
  });

  // The POST guard above was pinned; the GET guard was not — deleting it left all
  // 71 tests green. Without it every READ path swallows a 401/404/500 and returns
  // whatever the error envelope happens to parse into, so `oeadm attach` against a
  // deleted session "succeeded" with an undefined session id.
  it.each([
    [401, 'invalid x-api-key'],
    [404, 'session not found'],
    [500, 'internal error'],
  ])('rejects a GET that returns %i, carrying the status and the message', async (status, msg) => {
    const reg = fakeRegistry({
      'GET /v1/sessions/ses_gone': { status, json: { error: msg } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);

    const attempt = client.getSession('ses_gone');
    await expect(attempt).rejects.toThrow(new RegExp(String(status)));
    await expect(attempt).rejects.toThrow(new RegExp(msg));
    await expect(attempt).rejects.toThrow(/GET \/v1\/sessions\/ses_gone/);
  });

  // The registry's real error envelope is Anthropic-shaped, not `{ error: "…" }`.
  // The message must still reach the operator rather than being reduced to a bare
  // status, so a non-string `error` surfaces the body verbatim.
  it('surfaces an Anthropic-style error envelope on a GET', async () => {
    const reg = fakeRegistry({
      'GET /v1/sessions/ses_gone': {
        status: 404,
        json: { type: 'error', error: { type: 'not_found_error', message: 'session not found' } },
      },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);

    const attempt = client.getSession('ses_gone');
    await expect(attempt).rejects.toThrow(/404/);
    await expect(attempt).rejects.toThrow(/session not found/);
  });

  it('rejects a POST that returns a non-2xx with an Anthropic-style envelope', async () => {
    const reg = fakeRegistry({
      'POST /v1/sessions/ses_1/events': {
        status: 409,
        json: { type: 'error', error: { type: 'conflict_error', message: 'session is terminal' } },
      },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);

    const attempt = client.postUserMessage('ses_1', 'hi');
    await expect(attempt).rejects.toThrow(/409/);
    await expect(attempt).rejects.toThrow(/session is terminal/);
  });

  // The SSE tail is a GET too, with its own `status !== 200` guard.
  it('rejects a stream that opens with a non-200 rather than yielding frames', async () => {
    const reg = fakeRegistry({
      'GET /v1/sessions/ses_1/events/stream': { status: 403, json: { error: 'forbidden' } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);

    await expect(async () => {
      for await (const _frame of client.stream('ses_1')) {
        throw new Error('stream yielded a frame despite a 403');
      }
    }).rejects.toThrow(/403/);
  });
});

describe('OrcaClient.stream', () => {
  it('opens the SSE stream with the accept header and yields parsed frames', async () => {
    const reg = fakeRegistry({
      'GET /v1/sessions/ses_1/events/stream': {
        stream: scriptSse([
          { type: 'agent.message', seq: '1', content: [{ type: 'text', text: 'hi' }] },
          { type: 'agent.turn_completed', seq: '2' },
        ]),
      },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    const types: string[] = [];
    for await (const frame of client.stream('ses_1')) {
      types.push(frame.type);
    }
    expect(types).toEqual(['agent.message', 'agent.turn_completed']);
    const req = reg.requestsFor('GET /v1/sessions/ses_1/events/stream')[0]!;
    expect(req.headers['accept']).toContain('text/event-stream');
    expect(req.headers['x-api-key']).toBe('sk-1');
  });

  it('passes from_cursor as a query param when provided', async () => {
    const reg = fakeRegistry({
      'GET /v1/sessions/ses_1/events/stream': {
        stream: scriptSse([{ type: 'agent.turn_completed' }]),
      },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    for await (const _ of client.stream('ses_1', { fromCursor: '42' })) {
      // drain
    }
    const req = reg.requestsFor('GET /v1/sessions/ses_1/events/stream')[0]!;
    expect(new URL(req.url).searchParams.get('from_cursor')).toBe('42');
  });

  /**
   * WHY the stream — and only the stream — carries `orca-beta`.
   *
   * The registry filters the default, Claude-compatible stream to
   * `CLAUDE_SESSION_EVENT_TYPES` (`shouldEmitClaudeStreamFrame`). That set is
   * asserted here from the registry's own module rather than restated, because
   * both frames the interactive loop is built on are missing from it:
   * `agent.turn_completed` is its turn boundary and `agent.requires_action` is
   * its tool-confirmation gate, and both are Orca runner signals rather than
   * documented Anthropic session events. Drop the header and the loop never
   * observes a turn ending and never prompts allow/deny — every turn drains to a
   * dead stream and every gated tool call parks forever.
   */
  it('sends orca-beta on the stream, because the default view drops its control frames', async () => {
    expect(CLAUDE_SESSION_EVENT_TYPES.has('agent.turn_completed')).toBe(false);
    expect(CLAUDE_SESSION_EVENT_TYPES.has('agent.requires_action')).toBe(false);

    const reg = fakeRegistry({
      'GET /v1/sessions/ses_1/events/stream': {
        stream: scriptSse([{ type: 'agent.turn_completed' }]),
      },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    for await (const _ of client.stream('ses_1')) {
      // drain
    }
    const req = reg.requestsFor('GET /v1/sessions/ses_1/events/stream')[0]!;
    expect(req.headers['orca-beta']).toBe('1');
  });

  /**
   * `orca-beta` ALSO switches id prefixing (`toWireId`), the model shape
   * (`modelToApi`) and toolset aliasing on the other routes. The CLI wants none
   * of that, so the header is scoped to the one request that needs it.
   */
  it('does not send orca-beta on the ordinary JSON routes', async () => {
    const reg = fakeRegistry({
      'POST /v1/sessions': { status: 200, json: { id: 'ses_1', status: 'idle' } },
      'GET /v1/sessions/ses_1': { status: 200, json: { id: 'ses_1', status: 'idle' } },
    });
    const client = new OrcaClient({ baseURL: BASE, apiKey: 'sk-1' }, reg.fetch);
    await client.createSession({ agentId: 'agt_1' });
    await client.getSession('ses_1');

    expect(reg.requestsFor('POST /v1/sessions')[0]!.headers['orca-beta']).toBeUndefined();
    expect(reg.requestsFor('GET /v1/sessions/ses_1')[0]!.headers['orca-beta']).toBeUndefined();
  });
});
