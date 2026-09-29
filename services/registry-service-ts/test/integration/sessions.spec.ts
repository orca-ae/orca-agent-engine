// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import { and, desc, eq, sql } from 'drizzle-orm';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { createTestApiKey, TEST_ORGANIZATION_ID, uniqueWorkspace } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agentVersions,
  sessionLifecycleOutbox,
  sessionEventsIndex,
  guardrails,
  sessions,
  sessionThreads,
} from '../../src/persistence/postgres/schema.js';
import { reconcileSessionLifecycleOutbox } from '../../src/domain/session-lifecycle-outbox.js';
import type { ClaudeErrorType } from '../../src/contracts/common.js';

function expectClaudeError(body: unknown, type: ClaudeErrorType, message: string | RegExp): void {
  const error = body as {
    type: string;
    error: { type: string; message: string };
    request_id: string | null;
  };
  expect(error.type).toBe('error');
  expect(error.error.type).toBe(type);
  if (typeof message === 'string') {
    expect(error.error.message).toBe(message);
  } else {
    expect(error.error.message).toMatch(message);
  }
  expect(error.request_id).toEqual(expect.any(String));
}

describe('Sessions CRUD (integration)', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let pool: Pool;
  let workspaceId: string;
  let apiKey: string;
  let otherApiKey: string;
  let otherWorkspaceId: string;
  let agentId: string;
  let agentName: string;
  let store: TranscriptStore;
  const appendedEvents: Event[] = [];
  let lifecycleAppendError: Error | null = null;
  let initialEventsAppendError: Error | null = null;
  let initialEventsAppendAfterWriteError: Error | null = null;
  let observeInitialEventsAppend: ((sessionId: string) => Promise<void>) | null = null;
  const archivedSessions: Array<{ workspaceId: string; sessionId: string }> = [];
  const deletedSessions: Array<{ workspaceId: string; sessionId: string }> = [];
  let environmentId: string;

  beforeAll(async () => {
    ({ db, pool } = await getTestDb());
    store = buildStubStore();
    store.append = async (archivedWorkspaceId, sessionId, events) => {
      if (
        observeInitialEventsAppend &&
        events.some((event) => event.idempotencyKey.includes(':initial:'))
      ) {
        await observeInitialEventsAppend(sessionId);
      }
      if (
        initialEventsAppendError &&
        events.some((event) => event.idempotencyKey.includes(':initial:'))
      ) {
        throw initialEventsAppendError;
      }
      const lifecycleEvents = events.filter(
        (event) => event.kind === 'session.archived' || event.kind === 'session.deleted',
      );
      if (lifecycleEvents.length > 0 && lifecycleAppendError) throw lifecycleAppendError;
      for (const event of lifecycleEvents) {
        const target = event.kind === 'session.deleted' ? deletedSessions : archivedSessions;
        target.push({ workspaceId: archivedWorkspaceId, sessionId });
      }
      appendedEvents.push(...events);
      if (
        initialEventsAppendAfterWriteError &&
        events.some((event) => event.idempotencyKey.includes(':initial:'))
      ) {
        throw initialEventsAppendAfterWriteError;
      }
      return events.map((event) => event.id);
    };
    store.read = async function* (readWorkspaceId, sessionId) {
      for (const event of appendedEvents) {
        if (event.workspaceId === readWorkspaceId && event.sessionId === sessionId) yield event;
      }
    };
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store,
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();
    workspaceId = uniqueWorkspace('sessions');
    apiKey = await createTestApiKey(db, workspaceId);
    otherWorkspaceId = uniqueWorkspace('sessions_other');
    otherApiKey = await createTestApiKey(db, otherWorkspaceId);
    // Seed an agent so sessions can reference it
    agentName = `seed-${Date.now()}`;
    const agentResp = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: agentName,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      },
    });
    agentId = agentResp.json().id;
    const environmentResp = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `session-env-${Date.now()}`, config: { type: 'cloud' } },
    });
    if (environmentResp.statusCode !== 200) {
      throw new Error(`environment create failed: ${environmentResp.statusCode}`);
    }
    environmentId = environmentResp.json().id as string;
  });
  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  it('POST /v1/sessions creates a session referencing an agent', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId, title: 'wire session' },
    });
    expect(res.statusCode).toBe(200);
    const session = res.json();
    expect(session.id).toMatch(/^ses_/);
    expect(session).not.toHaveProperty('workspace_id');
    expect(session).not.toHaveProperty('agent_id');
    expect(session).not.toHaveProperty('agent_version');
    expect(session.agent).toMatchObject({ id: agentId, type: 'agent', version: 1 });
    expect(session.environment_id).toBe(environmentId);
    expect(session.title).toBe('wire session');
    expect(session.status).toBe('idle');
    expect(session).not.toHaveProperty('sandbox_handle_id');
    expect(session.timing).toMatchObject({
      started_at: null,
      last_active_at: null,
      active_seconds: 0,
    });
    expect(session).not.toHaveProperty('started_at');
    expect(session.timing.duration_seconds).toBeGreaterThanOrEqual(0);
    expect(session).not.toHaveProperty('last_active_at');
    expect(session.stats).toEqual({
      active_seconds: session.timing.active_seconds,
      duration_seconds: session.timing.duration_seconds,
    });
    expect(session.deployment_id).toBeNull();
    expect(session.outcome_evaluations).toEqual([]);
    expect(session.usage).toEqual({
      cache_creation: {
        ephemeral_1h_input_tokens: 0,
        ephemeral_5m_input_tokens: 0,
      },
      cache_read_input_tokens: 0,
      input_tokens: 0,
      output_tokens: 0,
    });
    expect(session).not.toHaveProperty('outcome');
  });

  it('projects the canonical Agent wire shape in sessions and threads', async () => {
    const agentResponse = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `session-wire-agent-${Date.now()}`,
        model: { id: 'claude-opus-4-6' },
        tools: [
          { type: 'agent_toolset_20260401' },
          { type: 'mcp_toolset', mcp_server_name: 'github' },
        ],
        mcp_servers: [{ type: 'url', name: 'github', url: 'https://example.com/mcp' }],
      },
    });
    expect(agentResponse.statusCode).toBe(200);
    const projectedAgentId = agentResponse.json().id as string;
    const expectedAgentShape = {
      id: projectedAgentId,
      type: 'agent',
      tools: [
        {
          type: 'agent_toolset_20260401',
          default_config: { enabled: true, permission_policy: { type: 'always_allow' } },
          configs: [],
        },
        {
          type: 'mcp_toolset',
          mcp_server_name: 'github',
          default_config: { enabled: true, permission_policy: { type: 'always_allow' } },
          configs: [],
        },
      ],
      mcp_servers: [{ type: 'url', name: 'github', url: 'https://example.com/mcp' }],
      skills: [],
    };

    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent: projectedAgentId },
    });
    expect(create.statusCode).toBe(200);
    expect(create.json().agent).toMatchObject(expectedAgentShape);
    const sessionId = create.json().id as string;

    const get = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${sessionId}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().agent).toMatchObject(expectedAgentShape);

    const list = await app.inject({
      method: 'GET',
      url: `/v1/sessions?agent_id=${projectedAgentId}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(list.statusCode).toBe(200);
    expect(list.json().data[0].agent).toMatchObject(expectedAgentShape);

    const threads = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${sessionId}/threads`,
      headers: { 'x-api-key': apiKey },
    });
    expect(threads.statusCode).toBe(200);
    expect(threads.json().data[0].agent).toMatchObject(expectedAgentShape);
  });

  it('POST /v1/sessions requires environment_id', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent_id: agentId },
    });

    expect(res.statusCode).toBe(400);
    expectClaudeError(res.json(), 'invalid_request_error', 'environment_id is required');

    const malformed = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent_id: agentId, environment_id: 'not-an-environment-id' },
    });
    expect(malformed.statusCode).toBe(400);
    expectClaudeError(
      malformed.json(),
      'invalid_request_error',
      'environment_id must be an env_… identifier',
    );
  });

  it('POST /v1/sessions rejects an unknown or cross-workspace environment_id', async () => {
    const unknown = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent_id: agentId, environment_id: 'env_does_not_exist' },
    });
    expect(unknown.statusCode).toBe(404);
    expectClaudeError(unknown.json(), 'not_found_error', 'environment not found');

    const otherEnvironment = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': otherApiKey, 'content-type': 'application/json' },
      payload: { name: `other-session-env-${Date.now()}`, config: { type: 'cloud' } },
    });
    expect(otherEnvironment.statusCode).toBe(200);

    const crossWorkspace = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent_id: agentId, environment_id: otherEnvironment.json().id },
    });
    expect(crossWorkspace.statusCode).toBe(404);
    expectClaudeError(crossWorkspace.json(), 'not_found_error', 'environment not found');
  });

  it('POST /v1/sessions persists metadata and update patches it', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent_id: agentId,
        metadata: { owner: 'platform', keep: 'yes' },
      },
    });
    expect(create.statusCode).toBe(200);
    const id = create.json().id;
    expect(create.json().metadata).toEqual({ owner: 'platform', keep: 'yes' });

    const update = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { metadata: { owner: 'agent-team', keep: null, added: 'value' } },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().metadata).toEqual({ owner: 'agent-team', added: 'value' });
    expect(update.json().stats).toEqual({
      active_seconds: update.json().timing.active_seconds,
      duration_seconds: update.json().timing.duration_seconds,
    });

    const get = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().metadata).toEqual({ owner: 'agent-team', added: 'value' });

    const clear = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { metadata: { owner: null, added: null } },
    });
    expect(clear.statusCode).toBe(200);
    expect(clear.json().metadata).toEqual({});
    // `.slice().reverse().find(...)` rather than `Array.prototype.findLast`:
    // the registry tsconfig's `lib` targets ES2022 (no ES2023 array methods).
    const clearEvent = appendedEvents
      .slice()
      .reverse()
      .find((event) => event.sessionId === id && event.kind === 'session.updated');
    expect(clearEvent).toBeDefined();
    expect(JSON.parse(Buffer.from(clearEvent!.payload).toString('utf8'))).toMatchObject({
      type: 'session.updated',
      metadata: {},
    });
  });

  it('validates Session LLM egress on create and update', async () => {
    const invalidCreate = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent_id: agentId,
        metadata: { orca_llm_egress: 'other' },
      },
    });
    expect(invalidCreate.statusCode).toBe(400);
    expectClaudeError(invalidCreate.json(), 'invalid_request_error', /orca_llm_egress/);

    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent_id: agentId,
        metadata: { orca_llm_egress: 'gateway' },
      },
    });
    expect(create.statusCode).toBe(200);
    const id = create.json().id;
    expect(create.json().metadata.orca_llm_egress).toBe('gateway');

    const invalidUpdate = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { metadata: { orca_llm_egress: 'other' } },
    });
    expect(invalidUpdate.statusCode).toBe(400);
    expectClaudeError(invalidUpdate.json(), 'invalid_request_error', /orca_llm_egress/);

    const direct = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { metadata: { orca_llm_egress: 'direct' } },
    });
    expect(direct.statusCode).toBe(200);
    expect(direct.json().metadata.orca_llm_egress).toBe('direct');
  });

  it('POST /v1/sessions round-trips reserved metadata keys as data', async () => {
    const metadata = metadataWithReservedKeys('literal-prototype');
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: JSON.stringify({ environment_id: environmentId, agent_id: agentId, metadata }),
    });
    expect(create.statusCode).toBe(200);
    expect(create.json().metadata['__proto__']).toBe('literal-prototype');
    expect(create.json().metadata.constructor).toBe('literal-constructor');

    const updatePatch = metadataWithReservedKeys('updated-prototype');
    const update = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${create.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: JSON.stringify({ metadata: updatePatch }),
    });
    expect(update.statusCode).toBe(200);
    expect(update.json().metadata['__proto__']).toBe('updated-prototype');
    expect(update.json().metadata.constructor).toBe('literal-constructor');
  });

  it('POST /v1/sessions rejects metadata beyond managed limits', async () => {
    const tooMany = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId, metadata: metadataPairs(17) },
    });
    expect(tooMany.statusCode).toBe(400);
    expectClaudeError(tooMany.json(), 'invalid_request_error', /at most 16 pairs/);

    const longKey = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent_id: agentId,
        metadata: { ['k'.repeat(65)]: 'value' },
      },
    });
    expect(longKey.statusCode).toBe(400);
    expectClaudeError(
      longKey.json(),
      'invalid_request_error',
      /keys must be at most 64 characters/,
    );

    const longValue = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent_id: agentId,
        metadata: { key: 'v'.repeat(513) },
      },
    });
    expect(longValue.statusCode).toBe(400);
    expectClaudeError(longValue.json(), 'invalid_request_error', /at most 512 characters/);

    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId, metadata: metadataPairs(16) },
    });
    expect(create.statusCode).toBe(200);
    const update = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${create.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { metadata: { extra: 'value' } },
    });
    expect(update.statusCode).toBe(400);
    expectClaudeError(update.json(), 'invalid_request_error', /at most 16 pairs/);
  });

  it('POST /v1/sessions creates a primary session thread for the main transcript', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent_id: agentId,
        title: 'primary thread session',
      },
    });
    expect(create.statusCode).toBe(200);
    const session = create.json();

    const listThreads = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${session.id}/threads`,
      headers: { 'x-api-key': apiKey },
    });
    expect(listThreads.statusCode).toBe(200);
    const threadsBody = listThreads.json();
    expect(threadsBody.data).toHaveLength(1);
    const primaryThread = threadsBody.data[0];
    expect(primaryThread).toMatchObject({
      type: 'session_thread',
      session_id: session.id,
      parent_thread_id: null,
      agent: { id: agentId, version: 1 },
      status: 'idle',
      archived_at: null,
    });
    expect(primaryThread.id).toMatch(/^sth_/);

    const append = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${session.id}/events`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        events: [{ type: 'user.message', content: [{ type: 'text', text: 'main thread hi' }] }],
      },
    });
    expect(append.statusCode).toBe(200);

    const runningThread = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${session.id}/threads/${primaryThread.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(runningThread.statusCode).toBe(200);
    expect(runningThread.json()).toMatchObject({
      id: primaryThread.id,
      status: 'running',
    });

    const threadEvents = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${session.id}/threads/${primaryThread.id}/events`,
      headers: { 'x-api-key': apiKey },
    });
    expect(threadEvents.statusCode).toBe(200);
    expect(threadEvents.json().data).toEqual([
      expect.objectContaining({
        type: 'user.message',
        content: [{ type: 'text', text: 'main thread hi' }],
      }),
    ]);
  });

  it('round-trips Claude session agent overrides, initial events, stats, and thread parents', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        agent: {
          type: 'agent_with_overrides',
          id: agentId,
          model: { id: 'claude-opus-4-8', speed: 'fast', effort: { type: 'high' } },
          system: null,
          tools: [],
          mcp_servers: [],
          skills: [],
        },
        environment_id: environmentId,
        initial_events: [{ type: 'user.message', content: [{ type: 'text', text: 'start now' }] }],
      },
    });

    expect(create.statusCode).toBe(200);
    expect(create.json()).toMatchObject({
      type: 'session',
      agent: {
        id: agentId,
        type: 'agent',
        model: {
          id: 'claude-opus-4-8',
          speed: 'fast',
          effort: { type: 'high' },
        },
        system: null,
        tools: [],
        mcp_servers: [],
        skills: [],
      },
      status: 'running',
      stats: { active_seconds: expect.any(Number), duration_seconds: expect.any(Number) },
      timing: { active_seconds: expect.any(Number), duration_seconds: expect.any(Number) },
    });
    expect(create.json()).not.toHaveProperty('agent_id');
    expect(create.json()).not.toHaveProperty('sandbox_handle_id');

    const sessionId = create.json().id as string;
    const retrieve = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${sessionId}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(retrieve.statusCode).toBe(200);
    expect(retrieve.json().agent).toEqual(create.json().agent);
    expect(retrieve.json()).toHaveProperty('stats');
    expect(retrieve.json()).toHaveProperty('timing');

    const primaryRows = await db
      .select()
      .from(sessionThreads)
      .where(
        and(
          eq(sessionThreads.workspaceId, workspaceId),
          eq(sessionThreads.sessionId, sessionId),
          eq(sessionThreads.subpath, ''),
        ),
      )
      .limit(1);
    const primary = primaryRows[0]!;
    const childId = `sth_roundtrip_${Date.now()}`;
    const now = new Date();
    await db.insert(sessionThreads).values({
      id: childId,
      workspaceId,
      sessionId,
      subpath: `threads/${childId}`,
      agentId,
      agentVersion: 1,
      agentName,
      parentThreadId: primary.id,
      status: 'idle',
      createdAt: now,
      updatedAt: now,
    });

    const child = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${sessionId}/threads/${childId}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(child.statusCode).toBe(200);
    expect(child.json()).toMatchObject({
      id: childId,
      type: 'session_thread',
      session_id: sessionId,
      parent_thread_id: primary.id,
    });
  });

  it('validates and composes session-local guardrail overrides at prepare time', async () => {
    const guardrailId = `grd_session_override_${Date.now()}`;
    const siblingGuardrailId = `grd_sibling_workspace_${Date.now()}`;
    await db.insert(guardrails).values([
      {
        id: guardrailId,
        organizationId: TEST_ORGANIZATION_ID,
        workspaceId,
        name: 'session override',
        phases: ['tool_call'],
        scope: 'explicit',
        rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
      },
      {
        id: siblingGuardrailId,
        organizationId: TEST_ORGANIZATION_ID,
        workspaceId: otherWorkspaceId,
        name: 'sibling workspace default',
        phases: ['tool_call'],
        scope: 'workspace',
        rule: { kind: 'builtin', builtin: 'ask_on_os_tools' },
      },
    ]);

    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: {
          type: 'agent_with_overrides',
          id: agentId,
          guardrail_ids: [guardrailId],
        },
      },
    });
    expect(create.statusCode).toBe(200);

    const prepared = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${create.json().id}/executions:prepare`,
      payload: {},
    });
    expect(prepared.statusCode).toBe(200);
    expect(prepared.json().guardrails).toContainEqual(
      expect.objectContaining({ id: guardrailId, tier: 'session' }),
    );
    expect(prepared.json().guardrails).not.toContainEqual(
      expect.objectContaining({ id: siblingGuardrailId }),
    );

    const invalid = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: {
          type: 'agent_with_overrides',
          id: agentId,
          guardrail_ids: ['grd_missing_session_override'],
        },
      },
    });
    expect(invalid.statusCode).toBe(400);
    expect(invalid.json().error.message).toContain('unknown or archived guardrail_ids');
  });

  it('uses replacement semantics for session model overrides', async () => {
    const agentResponse = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `session-model-replacement-${Date.now()}`,
        model: { id: 'claude-opus-5', speed: 'fast', effort: { type: 'high' } },
        tools: [],
        mcp_servers: [],
        skills: [],
      },
    });
    expect(agentResponse.statusCode).toBe(200);
    const replacementAgentId = agentResponse.json().id as string;

    const partialReplacement = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: {
          type: 'agent_with_overrides',
          id: replacementAgentId,
          model: { id: 'claude-opus-4-8', effort: { type: 'low' } },
        },
      },
    });
    expect(partialReplacement.statusCode).toBe(200);
    expect(partialReplacement.json().agent.model).toEqual({
      id: 'claude-opus-4-8',
      speed: 'standard',
      effort: { type: 'low' },
    });

    const stringReplacement = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: {
          type: 'agent_with_overrides',
          id: replacementAgentId,
          model: 'claude-sonnet-4-6',
        },
      },
    });
    expect(stringReplacement.statusCode).toBe(200);
    expect(stringReplacement.json().agent.model).toEqual({
      id: 'claude-sonnet-4-6',
      speed: 'standard',
      effort: { type: 'high' },
    });

    const cleared = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: {
          type: 'agent_with_overrides',
          id: replacementAgentId,
          model: { id: 'claude-opus-4-8', speed: null, effort: null },
        },
      },
    });
    expect(cleared.statusCode).toBe(200);
    expect(cleared.json().agent.model).toEqual({
      id: 'claude-opus-4-8',
      speed: 'standard',
      effort: { type: 'high' },
    });

    const unsupportedEffort = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: {
          type: 'agent_with_overrides',
          id: replacementAgentId,
          model: { id: 'claude-sonnet-4-6', effort: 'xhigh' },
        },
      },
    });
    expect(unsupportedEffort.statusCode).toBe(400);
    expect(unsupportedEffort.json().error.message).toMatch(
      /supported levels are low, medium, high, max/,
    );
  });

  it('rejects a session model replacement that makes coordinator speed mixed', async () => {
    const child = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `fast-session-child-${Date.now()}`,
        model: { id: 'claude-opus-4-8', speed: 'fast' },
      },
    });
    expect(child.statusCode).toBe(200);
    const coordinator = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `fast-session-coordinator-${Date.now()}`,
        model: { id: 'claude-opus-5', speed: 'fast' },
        multiagent: { type: 'coordinator', agents: [child.json().id] },
      },
    });
    expect(coordinator.statusCode).toBe(200);

    const mixed = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: {
          type: 'agent_with_overrides',
          id: coordinator.json().id,
          model: 'claude-opus-5',
        },
      },
    });
    expect(mixed.statusCode).toBe(400);
    expect(mixed.json().error.message).toMatch(/mixed model\.speed/);
  });

  it('publishes initial events only after the session transaction commits', async () => {
    let sessionVisibleDuringAppend = false;
    observeInitialEventsAppend = async (sessionId) => {
      const persisted = await db
        .select({ id: sessions.id })
        .from(sessions)
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
      sessionVisibleDuringAppend = persisted.length === 1;
    };
    try {
      const response = await app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          agent_id: agentId,
          environment_id: environmentId,
          initial_events: [
            { type: 'user.message', content: [{ type: 'text', text: 'after commit' }] },
            {
              type: 'user.define_outcome',
              description: 'after commit outcome',
              rubric: { type: 'text', content: 'done' },
            },
          ],
        },
      });
      expect(response.statusCode).toBe(200);
      expect(sessionVisibleDuringAppend).toBe(true);
      const sessionId = response.json().id as string;
      expect(
        appendedEvents.filter((event) => event.sessionId === sessionId).map((event) => event.kind),
      ).toEqual(['user.message', 'user.define_outcome']);
    } finally {
      observeInitialEventsAppend = null;
    }
  });

  it('durably retries initial events when the transcript backend is unavailable', async () => {
    const title = `initial-events-failure-${Date.now()}-${Math.random()}`;
    const response = await withInitialEventsAppendFailure(() =>
      app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          agent_id: agentId,
          environment_id: environmentId,
          title,
          initial_events: [
            { type: 'user.message', content: [{ type: 'text', text: 'must be atomic' }] },
          ],
        },
      }),
    );

    expect(response.statusCode).toBe(200);
    const sessionId = response.json().id as string;
    const persisted = await db
      .select({ id: sessions.id, status: sessions.status })
      .from(sessions)
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.title, title)));
    expect(persisted).toEqual([{ id: sessionId, status: 'running' }]);

    const [pending] = await db
      .select()
      .from(sessionLifecycleOutbox)
      .where(
        and(
          eq(sessionLifecycleOutbox.workspaceId, workspaceId),
          eq(sessionLifecycleOutbox.sessionId, sessionId),
          eq(sessionLifecycleOutbox.kind, 'session.initial_events'),
        ),
      )
      .limit(1);
    expect(pending).toMatchObject({
      publishedAt: null,
      attemptCount: 1,
    });

    const result = await reconcileSessionLifecycleOutbox(db, store, {
      eventIds: [pending!.id],
    });
    expect(result).toEqual({ processed: 1, published: 1, failed: 0 });
    const [published] = await db
      .select()
      .from(sessionLifecycleOutbox)
      .where(eq(sessionLifecycleOutbox.id, pending!.id));
    expect(published).toMatchObject({ attemptCount: 2 });
    expect(published!.publishedAt).toBeInstanceOf(Date);
    expect(appendedEvents).toContainEqual(
      expect.objectContaining({
        id: pending!.id,
        sessionId,
        kind: 'user.message',
      }),
    );
  });

  it('reconciles optional user attribution from durable initial-event rows', async () => {
    const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const sessionId = `ses_outbox_user_${suffix}`;
    const outboxId = `evt_outbox_user_${suffix}`;
    const invalidOutboxId = `evt_outbox_invalid_user_${suffix}`;
    const payloadBase64 = Buffer.from(
      JSON.stringify({ type: 'user.message', content: [{ type: 'text', text: 'outbox user' }] }),
    ).toString('base64');
    const event = {
      subpath: '',
      seq: 0,
      producedAt: '2026-08-26T00:00:00.000Z',
      producedBy: 'client',
      kind: 'user.message',
      payloadBase64,
      idempotencyKey: 'outbox-user-idempotency',
    };

    try {
      await db.insert(sessionLifecycleOutbox).values({
        id: outboxId,
        workspaceId,
        sessionId,
        kind: 'session.initial_events',
        events: [
          { ...event, id: outboxId, userId: 'user_outbox_1' },
          { ...event, id: `${outboxId}_missing` },
          { ...event, id: `${outboxId}_empty`, userId: '' },
        ],
      });

      expect(await reconcileSessionLifecycleOutbox(db, store, { eventIds: [outboxId] })).toEqual({
        processed: 1,
        published: 1,
        failed: 0,
      });
      expect(
        appendedEvents
          .filter((candidate) => candidate.sessionId === sessionId)
          .map((candidate) => candidate.userId),
      ).toEqual(['user_outbox_1', undefined, undefined]);

      await db.insert(sessionLifecycleOutbox).values({
        id: invalidOutboxId,
        workspaceId,
        sessionId,
        kind: 'session.initial_events',
        events: [{ ...event, id: invalidOutboxId, userId: 17 }],
      });
      expect(
        await reconcileSessionLifecycleOutbox(db, store, { eventIds: [invalidOutboxId] }),
      ).toEqual({ processed: 1, published: 0, failed: 1 });
      expect(appendedEvents.some((candidate) => candidate.id === invalidOutboxId)).toBe(false);
    } finally {
      await db
        .delete(sessionLifecycleOutbox)
        .where(
          and(
            eq(sessionLifecycleOutbox.workspaceId, workspaceId),
            eq(sessionLifecycleOutbox.sessionId, sessionId),
          ),
        );
    }
  });

  it('does not append initial events twice after an ambiguous transcript failure', async () => {
    const title = `initial-events-ambiguous-${Date.now()}-${Math.random()}`;
    initialEventsAppendAfterWriteError = new Error('append committed before connection failed');
    let response;
    try {
      response = await app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          agent_id: agentId,
          environment_id: environmentId,
          title,
          initial_events: [
            { type: 'user.message', content: [{ type: 'text', text: 'publish once' }] },
          ],
        },
      });
    } finally {
      initialEventsAppendAfterWriteError = null;
    }

    expect(response!.statusCode).toBe(200);
    const sessionId = response!.json().id as string;
    const [pending] = await db
      .select()
      .from(sessionLifecycleOutbox)
      .where(
        and(
          eq(sessionLifecycleOutbox.workspaceId, workspaceId),
          eq(sessionLifecycleOutbox.sessionId, sessionId),
          eq(sessionLifecycleOutbox.kind, 'session.initial_events'),
        ),
      )
      .limit(1);
    expect(pending).toMatchObject({ publishedAt: null, attemptCount: 1 });
    expect(appendedEvents.filter((event) => event.id === pending!.id)).toHaveLength(1);

    const result = await reconcileSessionLifecycleOutbox(db, store, {
      eventIds: [pending!.id],
    });

    expect(result).toEqual({ processed: 1, published: 1, failed: 0 });
    expect(appendedEvents.filter((event) => event.id === pending!.id)).toHaveLength(1);
    const [published] = await db
      .select()
      .from(sessionLifecycleOutbox)
      .where(eq(sessionLifecycleOutbox.id, pending!.id));
    expect(published!.publishedAt).toBeInstanceOf(Date);
  });

  it('serializes concurrent initial-event outbox publishers', async () => {
    const response = await withInitialEventsAppendFailure(() =>
      app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          agent_id: agentId,
          environment_id: environmentId,
          initial_events: [
            { type: 'user.message', content: [{ type: 'text', text: 'single publisher' }] },
          ],
        },
      }),
    );
    expect(response.statusCode).toBe(200);
    const sessionId = response.json().id as string;
    const [pending] = await db
      .select()
      .from(sessionLifecycleOutbox)
      .where(
        and(
          eq(sessionLifecycleOutbox.workspaceId, workspaceId),
          eq(sessionLifecycleOutbox.sessionId, sessionId),
          eq(sessionLifecycleOutbox.kind, 'session.initial_events'),
        ),
      )
      .limit(1);

    let releaseAppend!: () => void;
    let markAppendStarted!: () => void;
    const appendStarted = new Promise<void>((resolve) => {
      markAppendStarted = resolve;
    });
    const appendRelease = new Promise<void>((resolve) => {
      releaseAppend = resolve;
    });
    observeInitialEventsAppend = async () => {
      markAppendStarted();
      await appendRelease;
    };
    try {
      const first = reconcileSessionLifecycleOutbox(db, store, { eventIds: [pending!.id] });
      await appendStarted;
      const second = reconcileSessionLifecycleOutbox(db, store, { eventIds: [pending!.id] });
      await new Promise((resolve) => setTimeout(resolve, 25));
      releaseAppend();
      const outcomes = await Promise.all([first, second]);

      expect(outcomes).toContainEqual({ processed: 1, published: 1, failed: 0 });
      expect(outcomes).toContainEqual({ processed: 0, published: 0, failed: 0 });
      expect(appendedEvents.filter((event) => event.id === pending!.id)).toHaveLength(1);
    } finally {
      observeInitialEventsAppend = null;
      releaseAppend();
    }
  });

  it('keeps the session when initial event indexing fails after the durable append', async () => {
    await db.execute(
      sql.raw(
        'ALTER TABLE "session_events_index" ' +
          'DROP CONSTRAINT IF EXISTS "session_events_index_initial_failure_test"',
      ),
    );
    await db.execute(
      sql.raw(
        'ALTER TABLE "session_events_index" ' +
          'ADD CONSTRAINT "session_events_index_initial_failure_test" ' +
          `CHECK (("payload" #>> '{content,0,text}') IS DISTINCT FROM ` +
          "'force-initial-index-failure') NOT VALID",
      ),
    );

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          agent_id: agentId,
          environment_id: environmentId,
          initial_events: [
            {
              type: 'user.message',
              content: [{ type: 'text', text: 'force-initial-index-failure' }],
            },
          ],
        },
      });

      expect(response.statusCode).toBe(200);
      expect(response.json()).toMatchObject({ status: 'running' });
      const sessionId = response.json().id as string;
      const persisted = await db
        .select({ id: sessions.id, status: sessions.status })
        .from(sessions)
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
      expect(persisted).toEqual([{ id: sessionId, status: 'running' }]);
      const indexed = await db
        .select({ eventId: sessionEventsIndex.eventId })
        .from(sessionEventsIndex)
        .where(
          and(
            eq(sessionEventsIndex.workspaceId, workspaceId),
            eq(sessionEventsIndex.sessionId, sessionId),
          ),
        );
      expect(indexed).toEqual([]);
    } finally {
      await db.execute(
        sql.raw(
          'ALTER TABLE "session_events_index" ' +
            'DROP CONSTRAINT IF EXISTS "session_events_index_initial_failure_test"',
        ),
      );
    }
  });

  it('validates agent_with_overrides tools and MCP servers as one configuration', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        agent: {
          type: 'agent_with_overrides',
          id: agentId,
          mcp_servers: [{ type: 'url', name: 'github', url: 'https://example.com/mcp' }],
        },
        environment_id: environmentId,
      },
    });

    expect(response.statusCode).toBe(400);
    expectClaudeError(
      response.json(),
      'invalid_request_error',
      /mcp_servers\.github must be referenced by an mcp_toolset/,
    );
  });

  it('accepts user.tool_result for self-hosted Claude SDK sessions', async () => {
    const environmentResponse = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `self-hosted-${Date.now()}`, config: { type: 'self_hosted' } },
    });
    expect(environmentResponse.statusCode).toBe(200);
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        agent_id: agentId,
        environment_id: environmentResponse.json().id,
      },
    });
    expect(create.statusCode).toBe(200);

    const append = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${create.json().id}/events`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { events: [{ type: 'user.tool_result', tool_use_id: 'toolu_1' }] },
    });
    expect(append.statusCode).toBe(200);
    expect(append.json().data).toEqual([
      expect.objectContaining({ type: 'user.tool_result', tool_use_id: 'toolu_1' }),
    ]);
  });

  it('rejects user.tool_result when the self-hosted session harness cannot consume it', async () => {
    const agentResponse = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `in-sandbox-tool-result-${Date.now()}`,
        model: { id: 'claude-opus-4-6' },
        metadata: { harness: 'claude_code', mode: 'colocated' },
      },
    });
    expect(agentResponse.statusCode).toBe(200);
    const environmentResponse = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `self-hosted-in-sandbox-${Date.now()}`, config: { type: 'self_hosted' } },
    });
    expect(environmentResponse.statusCode).toBe(200);
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        agent_id: agentResponse.json().id,
        environment_id: environmentResponse.json().id,
      },
    });
    expect(create.statusCode).toBe(200);

    const append = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${create.json().id}/events`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { events: [{ type: 'user.tool_result', tool_use_id: 'toolu_in_sandbox' }] },
    });
    expect(append.statusCode).toBe(400);
    expectClaudeError(
      append.json(),
      'invalid_request_error',
      "user.tool_result is not supported by this session's execution harness",
    );
  });

  it('rejects user.tool_result when the stored harness annotation no longer resolves', async () => {
    // Catalog DRIFT, not a bad request. `POST` / `PATCH /v1/agents` validate
    // `metadata.harness` and `metadata.mode` at write time, so this state cannot be
    // created through the public API — but it is reached whenever the catalog moves
    // underneath rows already in the table. Retiring a harness does it, and so does
    // renaming a mode value (which this very stack does). The capability resolver
    // reads `agent_versions.snapshot` straight from the DB, so seeding the drift there
    // is exactly what a deployed registry would find after such a change.
    const agentResponse = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `harness-drift-${Date.now()}`,
        model: { id: 'claude-opus-4-6' },
        metadata: {},
      },
    });
    expect(agentResponse.statusCode).toBe(200);
    const driftedAgentId = agentResponse.json().id as string;
    const environmentResponse = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: `self-hosted-drift-${Date.now()}`, config: { type: 'self_hosted' } },
    });
    expect(environmentResponse.statusCode).toBe(200);
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        agent_id: driftedAgentId,
        environment_id: environmentResponse.json().id,
      },
    });
    expect(create.statusCode).toBe(200);
    const sessionId = create.json().id as string;

    const appendToolResult = (toolUseId: string) =>
      app.inject({
        method: 'POST',
        url: `/v1/sessions/${sessionId}/events`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: { events: [{ type: 'user.tool_result', tool_use_id: toolUseId }] },
      });

    /** Overwrite the STORED annotation the resolver reads, leaving the rest of the snapshot. */
    const driftStoredMetadata = async (metadata: Record<string, unknown>): Promise<void> => {
      const rows = await db
        .select({ id: agentVersions.id, snapshot: agentVersions.snapshot })
        .from(agentVersions)
        .where(
          and(
            eq(agentVersions.workspaceId, workspaceId),
            eq(agentVersions.agentId, driftedAgentId),
          ),
        );
      expect(rows).toHaveLength(1);
      const snapshot = rows[0]!.snapshot as Record<string, unknown>;
      await db
        .update(agentVersions)
        .set({ snapshot: { ...snapshot, metadata } })
        .where(and(eq(agentVersions.workspaceId, workspaceId), eq(agentVersions.id, rows[0]!.id)));
    };

    // Baseline: the agent resolves to the default harness/mode, which DOES accept
    // user.tool_result. Only the stored annotation changes below, so the 400s that
    // follow can have no other cause.
    expect((await appendToolResult('toolu_drift_baseline')).statusCode).toBe(200);

    // A harness name the catalog no longer knows (a retired entry). The accepted list
    // is matched loosely on purpose — `agents.spec.ts` is the one place that pins it.
    await driftStoredMetadata({ harness: 'nope' });
    const unknownHarness = await appendToolResult('toolu_drift_harness');
    expect(unknownHarness.statusCode).toBe(400);
    expectClaudeError(
      unknownHarness.json(),
      'invalid_request_error',
      /^user\.tool_result is unavailable: the agent's harness annotation is invalid \(metadata\.harness must be one of: .+\)$/,
    );

    // A mode value the catalog no longer knows — the shape a mode RENAME leaves behind.
    await driftStoredMetadata({ harness: 'claude_agent_sdk', mode: 'in_the_sandbox' });
    const unknownMode = await appendToolResult('toolu_drift_mode');
    expect(unknownMode.statusCode).toBe(400);
    expectClaudeError(
      unknownMode.json(),
      'invalid_request_error',
      "user.tool_result is unavailable: the agent's harness annotation is invalid " +
        '(metadata.mode must be one of: colocated, separate)',
    );
  });

  it('rejects user.tool_result for cloud sessions', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent_id: agentId, environment_id: environmentId },
    });
    expect(create.statusCode).toBe(200);

    const append = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${create.json().id}/events`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { events: [{ type: 'user.tool_result', tool_use_id: 'toolu_cloud' }] },
    });
    expect(append.statusCode).toBe(400);
    expectClaudeError(
      append.json(),
      'invalid_request_error',
      'user.tool_result is only valid for self_hosted environments',
    );
  });

  it('returns accepted user.define_outcome events with canonical defaults', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: agentId,
        initial_events: [
          {
            type: 'user.define_outcome',
            description: 'initial outcome',
            rubric: { type: 'text', content: 'initial done' },
          },
        ],
      },
    });
    expect(create.statusCode).toBe(200);
    const sessionId = create.json().id as string;

    const initialList = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${sessionId}/events`,
      headers: { 'x-api-key': apiKey },
    });
    expect(initialList.statusCode).toBe(200);
    expect(initialList.json().data[0]).toMatchObject({
      type: 'user.define_outcome',
      processed_at: expect.any(String),
      outcome_id: expect.stringMatching(/^outc_/),
      max_iterations: 3,
    });

    const append = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${sessionId}/events`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        events: [
          {
            type: 'user.define_outcome',
            description: 'appended outcome',
            rubric: { type: 'text', content: 'appended done' },
          },
        ],
      },
    });
    expect(append.statusCode).toBe(200);
    expect(append.json().data[0]).toMatchObject({
      type: 'user.define_outcome',
      processed_at: expect.any(String),
      outcome_id: expect.stringMatching(/^outc_/),
      max_iterations: 3,
    });
  });

  it('POST /v1/sessions/:id/events records the session agent name on reserved child threads', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent_id: agentId,
        title: 'reserved thread agent name',
      },
    });
    expect(create.statusCode).toBe(200);
    const session = create.json();
    const subpath = `threads/name-${Date.now()}`;

    const append = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${session.id}/events`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'legacy-event-subpath',
      },
      payload: {
        events: [
          {
            type: 'user.message',
            subpath,
            content: [{ type: 'text', text: 'child thread hi' }],
          },
        ],
      },
    });
    expect(append.statusCode).toBe(200);

    const rows = await db
      .select({ agentName: sessionThreads.agentName })
      .from(sessionThreads)
      .where(
        and(
          eq(sessionThreads.workspaceId, workspaceId),
          eq(sessionThreads.sessionId, session.id),
          eq(sessionThreads.subpath, subpath),
        ),
      )
      .limit(1);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.agentName).toBe(agentName);
    expect(rows[0]?.agentName).not.toBe(agentId);
  });

  it('normalizes Claude wire-prefixed agent and session ids at the public edge', async () => {
    const wireAgentId = agentId.replace(/^agt_/, 'agent_');
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: wireAgentId },
    });
    expect(res.statusCode).toBe(200);
    const session = res.json();
    expect(session.agent.id).toBe(agentId);

    const wireSessionId = session.id.replace(/^ses_/, 'sesn_');
    const get = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${wireSessionId}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().id).toBe(session.id);
  });

  it('POST /v1/sessions validates vault_ids in same workspace and not archived', async () => {
    const vaultId = await createVault(apiKey, 'session-vault-ok');
    const valid = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId, vault_ids: [vaultId] },
    });
    expect(valid.statusCode).toBe(200);
    expect(valid.json().vault_ids).toEqual([vaultId]);

    const duplicate = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId, vault_ids: [vaultId, vaultId] },
    });
    expect(duplicate.statusCode).toBe(200);
    expect(duplicate.json().vault_ids).toEqual([vaultId, vaultId]);

    const otherVaultId = await createVault(otherApiKey, 'session-vault-other-workspace');
    const crossWorkspace = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId, vault_ids: [otherVaultId] },
    });
    expect(crossWorkspace.statusCode).toBe(400);
    expectClaudeError(crossWorkspace.json(), 'invalid_request_error', /not found in workspace/);

    const archivedVaultId = await createVault(apiKey, 'session-vault-archived');
    const archive = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${archivedVaultId}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archive.statusCode).toBe(200);
    const archived = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId, vault_ids: [archivedVaultId] },
    });
    expect(archived.statusCode).toBe(400);
    expectClaudeError(archived.json(), 'invalid_request_error', /is archived/);
  });

  it('POST /v1/sessions keeps a concurrent vault archive as an invalid request', async () => {
    const vaultId = await createVault(apiKey, 'session-vault-concurrent-archive');
    const blocker = await pool.connect();
    let transactionOpen = false;
    let settleCreate: Promise<void> | undefined;
    try {
      await blocker.query('BEGIN');
      transactionOpen = true;
      const blockerPid = await blocker.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
      await blocker.query(
        'SELECT id FROM environments WHERE workspace_id = $1 AND id = $2 FOR UPDATE',
        [workspaceId, environmentId],
      );

      const pendingCreate = app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: { environment_id: environmentId, agent_id: agentId, vault_ids: [vaultId] },
      });
      settleCreate = pendingCreate.then(
        () => undefined,
        () => undefined,
      );
      await waitForQueryBlockedBy(pool, blockerPid.rows[0]!.pid, '"environments"');

      const archive = await app.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/archive`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {},
      });
      expect(archive.statusCode).toBe(200);

      await blocker.query('COMMIT');
      transactionOpen = false;
      const response = await pendingCreate;
      expect(response.statusCode).toBe(400);
      expectClaudeError(response.json(), 'invalid_request_error', /is archived/);
    } finally {
      if (transactionOpen) await blocker.query('ROLLBACK');
      blocker.release();
      await settleCreate;
    }
  });

  it('POST /v1/sessions rejects output-only status', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId, status: 'running' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('POST /v1/sessions with unknown agent_id returns 404', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: 'agt_does_not_exist' },
    });
    expect(res.statusCode).toBe(404);
  });

  // === polymorphic agent ref + explicit version pinning ===

  it('POST /v1/sessions accepts `agent` as a bare string id (latest pin)', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent: agentId },
    });
    expect(res.statusCode).toBe(200);
    const session = res.json();
    expect(session.agent).toMatchObject({ id: agentId, type: 'agent', version: 1 });
  });

  it('POST /v1/sessions pins an explicit older version via {type:"agent",id,version}', async () => {
    // Seed a dedicated agent and bump it to v2 so v1 is a real older version.
    const seed = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `pin-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: 'v1',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      },
    });
    const seededAgent = seed.json();
    const pinAgentId = seededAgent.id;
    const bump = await app.inject({
      method: 'POST',
      url: `/v1/agents/${pinAgentId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: seededAgent.version, system: 'v2' },
    });
    expect(bump.statusCode).toBe(200);
    expect(bump.json().version).toBe(2);

    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: { type: 'agent', id: pinAgentId, version: 1 },
      },
    });
    expect(res.statusCode).toBe(200);
    const session = res.json();
    expect(session.agent).toMatchObject({ id: pinAgentId, type: 'agent', version: 1 });
  });

  it('POST /v1/sessions with a nonexistent explicit version returns 404', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: { type: 'agent', id: agentId, version: 999 },
      },
    });
    expect(res.statusCode).toBe(404);
  });

  it('POST /v1/sessions with neither agent nor agent_id returns 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, title: 'no agent' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('POST /v1/sessions with BOTH agent and agent_id returns 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent: agentId, agent_id: agentId },
    });
    expect(res.statusCode).toBe(400);
  });

  // === sessions.update (POST /v1/sessions/:id) overrides ===

  it('POST /v1/sessions/:id updates title and returns the same id (no version bump)', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId, title: 'before' },
    });
    const id = create.json().id;
    const upd = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { title: 'after' },
    });
    expect(upd.statusCode).toBe(200);
    expect(upd.json().id).toBe(id);
    expect(upd.json().title).toBe('after');
    expect(upd.json().agent).toMatchObject({ id: agentId, type: 'agent', version: 1 });
  });

  it('POST /v1/sessions/:id full-replaces agent.tools on an idle session', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id;
    expect(create.json().agent.tools).toEqual([]);

    const tools = [
      {
        type: 'custom',
        name: 'only_this',
        description: 'Only this tool',
        input_schema: { type: 'object', properties: {} },
      },
    ];
    const upd = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent: { tools } },
    });
    expect(upd.statusCode).toBe(200);
    expect(upd.json().agent.tools).toEqual(tools);

    const get = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.json().agent.tools).toEqual(tools);
    // Override must not bump the pinned agent version.
    expect(get.json().agent).toMatchObject({ id: agentId, type: 'agent', version: 1 });
  });

  it('POST /v1/sessions/:id replaces a valid tools/MCP configuration but rejects vault_ids', async () => {
    const vaultId = await createVault(apiKey, 'session-update-vault-ok');
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id;

    const tools = [{ type: 'mcp_toolset', mcp_server_name: 'github' }];
    const mcpServers = [{ name: 'github', type: 'url', url: 'https://mcp.example.com/' }];
    const upd = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent: { tools, mcp_servers: mcpServers } },
    });
    expect(upd.statusCode).toBe(200);
    expect(upd.json().agent.tools).toEqual([
      expect.objectContaining({ type: 'mcp_toolset', mcp_server_name: 'github' }),
    ]);
    expect(upd.json().agent.mcp_servers).toEqual(mcpServers);
    expect(upd.json().vault_ids).toEqual([]);

    const rejectedVaultUpdate = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { vault_ids: [vaultId] },
    });
    expect(rejectedVaultUpdate.statusCode).toBe(400);
    expectClaudeError(
      rejectedVaultUpdate.json(),
      'invalid_request_error',
      'vault_ids updates are not yet supported',
    );

    const after = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(after.json().vault_ids).toEqual([]);
    expect(after.json().agent.mcp_servers).toEqual(mcpServers);
  });

  it('POST /v1/sessions/:id rejects invalid runtime reconfiguration inputs', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id;

    // `model` is now a supported runtime reconfiguration: a cost guardrail's
    // downgrade gate refuses expensive models over budget and allows cheaper
    // ones, which needs a way to switch mid-session. It is persisted as a
    // session agent override and applied at the next runtime preparation.
    const modelSwitch = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent: { model: { provider: 'anthropic', id: 'claude-haiku-4-5' } } },
    });
    expect(modelSwitch.statusCode).toBe(200);

    const malformedModel = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent: { model: { provider: 'anthropic' } } },
    });
    expect(malformedModel.statusCode).toBe(400);

    const emptyAgent = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent: {} },
    });
    expect(emptyAgent.statusCode).toBe(200);
    expect(emptyAgent.json().id).toBe(id);

    const unreferencedMcpServer = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        agent: {
          mcp_servers: [{ name: 'github', type: 'url', url: 'https://mcp.example.com/' }],
        },
      },
    });
    expect(unreferencedMcpServer.statusCode).toBe(400);
    expectClaudeError(
      unreferencedMcpServer.json(),
      'invalid_request_error',
      /mcp_servers\.github must be referenced by an mcp_toolset/,
    );
  });

  it('POST /v1/sessions/:id rejects agent overrides while running but allows metadata edits', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id;
    // Drive the session to running via the mesh-internal state route.
    const run = await app.inject({
      method: 'PATCH',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${id}/state`,
      headers: { 'content-type': 'application/json' },
      payload: { status: 'running' },
    });
    expect(run.statusCode).toBe(200);

    const upd = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { agent: { tools: [] } },
    });
    expect(upd.statusCode).toBe(409);
    expectClaudeError(upd.json(), 'conflict_error', /must be idle/);

    const egressUpdate = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { metadata: { orca_llm_egress: 'gateway' } },
    });
    expect(egressUpdate.statusCode).toBe(409);
    expectClaudeError(egressUpdate.json(), 'conflict_error', /must be idle/);

    // Title and metadata edits remain allowed while running.
    const titleUpd = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { title: 'running-title', metadata: { phase: 'running' } },
    });
    expect(titleUpd.statusCode).toBe(200);
    expect(titleUpd.json().title).toBe('running-title');
    expect(titleUpd.json().metadata).toEqual({ phase: 'running' });
  });

  it('POST /v1/sessions/:id rejects updates after archive or termination', async () => {
    const archivedCreate = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const archivedId = archivedCreate.json().id;
    const archive = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${archivedId}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
    });
    expect(archive.statusCode).toBe(200);
    const archivedUpdate = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${archivedId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { title: 'too late' },
    });
    expect(archivedUpdate.statusCode).toBe(409);
    expectClaudeError(archivedUpdate.json(), 'conflict_error', /cannot be updated/);

    const terminatedCreate = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const terminatedId = terminatedCreate.json().id;
    const terminate = await app.inject({
      method: 'PATCH',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${terminatedId}/state`,
      headers: { 'content-type': 'application/json' },
      payload: { status: 'terminated' },
    });
    expect(terminate.statusCode).toBe(200);
    const terminatedUpdate = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${terminatedId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { title: 'too late' },
    });
    expect(terminatedUpdate.statusCode).toBe(409);
    expectClaudeError(terminatedUpdate.json(), 'conflict_error', /cannot be updated/);
  });

  it('POST /v1/sessions/:id on an unknown session returns 404', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/sessions/ses_does_not_exist',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { title: 'x' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('GET /v1/sessions/:id', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id;
    const get = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().id).toBe(id);
    expect(get.json()).not.toHaveProperty('workspace_id');
    expect(get.json()).not.toHaveProperty('outcome');
    expect(get.json()).toMatchObject({
      agent: { id: agentId, version: 1 },
      title: null,
      deployment_id: null,
      outcome_evaluations: [],
    });
    expect(get.json()).not.toHaveProperty('last_active_at');
    expect(get.json()).not.toHaveProperty('started_at');
    expect(get.json().stats).toEqual({
      active_seconds: get.json().timing.active_seconds,
      duration_seconds: get.json().timing.duration_seconds,
    });
  });

  it('GET /v1/sessions returns the Claude list envelope only', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    expect(create.statusCode).toBe(200);

    const list = await app.inject({
      method: 'GET',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey },
    });
    expect(list.statusCode).toBe(200);
    const body = list.json() as {
      data: Array<{
        id: string;
        stats: { active_seconds: number; duration_seconds: number };
        timing: { active_seconds: number; duration_seconds: number };
      }>;
      next_page: string | null;
    };
    const listed = body.data.find((session) => session.id === create.json().id);
    expect(listed).toBeDefined();
    expect(listed!.stats).toEqual({
      active_seconds: listed!.timing.active_seconds,
      duration_seconds: listed!.timing.duration_seconds,
    });
    expect(body.next_page).toBeNull();
    expect(body).not.toHaveProperty('sessions');
    expect(body).not.toHaveProperty('page_info');
  });

  it('GET /v1/sessions honors agent_version without requiring agent_id', async () => {
    const createAgent = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `version-filter-${Date.now()}`,
        model: { id: 'claude-opus-4-6' },
        system: 'version one',
      },
    });
    expect(createAgent.statusCode).toBe(200);
    const versionedAgentId = createAgent.json().id as string;
    const updateAgent = await app.inject({
      method: 'POST',
      url: `/v1/agents/${versionedAgentId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { version: 1, system: 'version two' },
    });
    expect(updateAgent.statusCode).toBe(200);
    expect(updateAgent.json().version).toBe(2);

    const versionOne = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: { type: 'agent', id: versionedAgentId, version: 1 },
      },
    });
    expect(versionOne.statusCode).toBe(200);
    const versionTwo = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        environment_id: environmentId,
        agent: { type: 'agent', id: versionedAgentId, version: 2 },
      },
    });
    expect(versionTwo.statusCode).toBe(200);

    const list = await app.inject({
      method: 'GET',
      url: '/v1/sessions?agent_version=2',
      headers: { 'x-api-key': apiKey },
    });
    expect(list.statusCode).toBe(200);
    const listed = list.json().data as Array<{ id: string; agent: { version: number } }>;
    expect(listed.map((session) => session.id)).toContain(versionTwo.json().id);
    expect(listed.map((session) => session.id)).not.toContain(versionOne.json().id);
    expect(listed.every((session) => session.agent.version === 2)).toBe(true);
  });

  it('GET /v1/sessions honors limit and page cursors', async () => {
    const first = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    expect(first.statusCode).toBe(200);
    const second = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    expect(second.statusCode).toBe(200);

    const firstPage = await app.inject({
      method: 'GET',
      url: '/v1/sessions?limit=1',
      headers: { 'x-api-key': apiKey },
    });
    expect(firstPage.statusCode).toBe(200);
    const firstPageBody = firstPage.json() as {
      data: Array<{ id: string }>;
      next_page: string | null;
    };
    expect(firstPageBody.data).toHaveLength(1);
    expect(firstPageBody.next_page).toBeTruthy();

    const secondPage = await app.inject({
      method: 'GET',
      url: `/v1/sessions?limit=1&page=${encodeURIComponent(firstPageBody.next_page!)}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(secondPage.statusCode).toBe(200);
    const secondPageBody = secondPage.json() as {
      data: Array<{ id: string }>;
      next_page: string | null;
    };
    expect(secondPageBody.data).toHaveLength(1);
    expect(secondPageBody.data[0]!.id).not.toBe(firstPageBody.data[0]!.id);
  });

  it('GET /v1/sessions binds page cursors to their original order', async () => {
    const agentResponse = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `ascending-pages-${Date.now()}`,
        model: { id: 'claude-opus-4-6' },
      },
    });
    expect(agentResponse.statusCode).toBe(200);
    const paginationAgentId = agentResponse.json().id as string;
    for (let index = 0; index < 3; index++) {
      const create = await app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          environment_id: environmentId,
          agent_id: paginationAgentId,
          title: `ascending-page-${index}`,
        },
      });
      expect(create.statusCode).toBe(200);
    }

    const first = await app.inject({
      method: 'GET',
      url: `/v1/sessions?agent_id=${paginationAgentId}&limit=1&order=asc`,
      headers: { 'x-api-key': apiKey },
    });
    expect(first.statusCode).toBe(200);
    const firstBody = first.json() as {
      data: Array<{ id: string }>;
      next_page: string;
    };

    const second = await app.inject({
      method: 'GET',
      url:
        `/v1/sessions?agent_id=${paginationAgentId}&limit=1&page=` +
        encodeURIComponent(firstBody.next_page),
      headers: { 'x-api-key': apiKey },
    });
    expect(second.statusCode).toBe(200);
    expect(second.json().data[0].id).not.toBe(firstBody.data[0]!.id);

    const mismatchedOrder = await app.inject({
      method: 'GET',
      url:
        `/v1/sessions?agent_id=${paginationAgentId}&limit=1&order=desc&page=` +
        encodeURIComponent(firstBody.next_page),
      headers: { 'x-api-key': apiKey },
    });
    expect(mismatchedOrder.statusCode).toBe(400);
    expectClaudeError(
      mismatchedOrder.json(),
      'invalid_request_error',
      /cursor order does not match/,
    );
  });

  it('GET /v1/sessions returns the preceding descending page in descending order', async () => {
    const agentResponse = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `descending-pages-${Date.now()}`,
        model: { id: 'claude-opus-4-6' },
      },
    });
    expect(agentResponse.statusCode).toBe(200);
    const paginationAgentId = agentResponse.json().id as string;
    for (let index = 0; index < 4; index++) {
      const create = await app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          environment_id: environmentId,
          agent_id: paginationAgentId,
          title: `descending-page-${index}`,
        },
      });
      expect(create.statusCode).toBe(200);
    }

    const first = await app.inject({
      method: 'GET',
      url: `/v1/sessions?agent_id=${paginationAgentId}&limit=2&order=desc`,
      headers: { 'x-api-key': apiKey },
    });
    expect(first.statusCode).toBe(200);
    const firstBody = first.json() as {
      data: Array<{ id: string }>;
      next_page: string;
      prev_page: string | null;
    };
    expect(firstBody.data).toHaveLength(2);
    expect(firstBody.next_page).toEqual(expect.any(String));

    const second = await app.inject({
      method: 'GET',
      url:
        `/v1/sessions?agent_id=${paginationAgentId}&limit=2&order=desc&page=` +
        encodeURIComponent(firstBody.next_page),
      headers: { 'x-api-key': apiKey },
    });
    expect(second.statusCode).toBe(200);
    const secondBody = second.json() as {
      data: Array<{ id: string }>;
      prev_page: string;
    };
    expect(secondBody.prev_page).toEqual(expect.any(String));

    const preceding = await app.inject({
      method: 'GET',
      url:
        `/v1/sessions?agent_id=${paginationAgentId}&limit=2&order=desc&page=` +
        encodeURIComponent(secondBody.prev_page),
      headers: { 'x-api-key': apiKey },
    });
    expect(preceding.statusCode).toBe(200);
    expect(preceding.json().data.map((session: { id: string }) => session.id)).toEqual(
      firstBody.data.map((session) => session.id),
    );
  });

  it('POST /v1/sessions/:id/archive sets archived_at', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id;
    // Omit `payload`: zero-byte body with Content-Type: application/json, as
    // sent by some Anthropic-compatible clients and proxies.
    const arch = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
    });
    expect(arch.statusCode).toBe(200);
    expect(arch.json().archived_at).toBeTruthy();
    expect(arch.json().stats).toEqual({
      active_seconds: arch.json().timing.active_seconds,
      duration_seconds: arch.json().timing.duration_seconds,
    });
    expect(archivedSessions).toContainEqual({ workspaceId, sessionId: id });
  });

  it('freezes archived session stats at archived_at', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id as string;
    const createdAt = new Date('2026-07-24T00:00:00.000Z');
    const lastActiveAt = new Date('2026-07-24T00:00:10.000Z');
    const archivedAt = new Date('2026-07-24T00:00:20.000Z');
    await db
      .update(sessions)
      .set({
        status: 'running',
        activeSeconds: 5,
        createdAt,
        lastActiveAt,
        archivedAt,
      })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, id)));

    const get = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json().stats).toEqual({ active_seconds: 15, duration_seconds: 20 });
    expect(get.json().timing).toMatchObject({ active_seconds: 15, duration_seconds: 20 });
  });

  it('durably retries an archive sentinel when the transcript backend is unavailable', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id;
    const archive = await withLifecycleAppendFailure(() =>
      app.inject({
        method: 'POST',
        url: `/v1/sessions/${id}/archive`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      }),
    );

    expect(archive.statusCode).toBe(200);
    const [pending] = await db
      .select()
      .from(sessionLifecycleOutbox)
      .where(
        and(
          eq(sessionLifecycleOutbox.workspaceId, workspaceId),
          eq(sessionLifecycleOutbox.sessionId, id),
        ),
      )
      .orderBy(desc(sessionLifecycleOutbox.createdAt))
      .limit(1);
    expect(pending).toMatchObject({ publishedAt: null, attemptCount: 1 });

    const result = await reconcileSessionLifecycleOutbox(db, store, { eventIds: [pending!.id] });
    expect(result).toEqual({ processed: 1, published: 1, failed: 0 });
    const [published] = await db
      .select()
      .from(sessionLifecycleOutbox)
      .where(eq(sessionLifecycleOutbox.id, pending!.id));
    expect(published).toMatchObject({ attemptCount: 2 });
    expect(published!.publishedAt).toBeInstanceOf(Date);
    expect(archivedSessions).toContainEqual({ workspaceId, sessionId: id });
  });

  it('POST /v1/sessions/:id/archive makes the primary thread report archived lifecycle', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    expect(create.statusCode).toBe(200);
    const session = create.json();

    const listThreads = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${session.id}/threads`,
      headers: { 'x-api-key': apiKey },
    });
    expect(listThreads.statusCode).toBe(200);
    const primaryThread = listThreads.json().data[0];

    const archive = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${session.id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
    });
    expect(archive.statusCode).toBe(200);
    const archivedAt = archive.json().archived_at;
    expect(archivedAt).toBeTruthy();

    const getPrimaryThread = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${session.id}/threads/${primaryThread.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(getPrimaryThread.statusCode).toBe(200);
    expect(getPrimaryThread.json()).toMatchObject({
      id: primaryThread.id,
      status: 'terminated',
      archived_at: archivedAt,
    });
  });

  it('GET /v1/sessions/:id/outcome returns null (Phase 2 stub)', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id;
    const res = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${id}/outcome`,
      headers: { 'x-api-key': apiKey },
    });
    expect(res.statusCode).toBe(200);
    expect(res.body).toBe('null'); // Fastify serializes JSON null as the string 'null'
  });

  it('DELETE /v1/sessions/:id returns a tombstone then 404', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id;
    const [primaryThread] = await db
      .select()
      .from(sessionThreads)
      .where(and(eq(sessionThreads.workspaceId, workspaceId), eq(sessionThreads.sessionId, id)))
      .limit(1);
    await db.insert(sessionThreads).values({
      id: `sth_delete_child_${Date.now()}`,
      workspaceId,
      sessionId: id,
      subpath: 'threads/delete-child',
      agentId: primaryThread!.agentId,
      agentVersion: primaryThread!.agentVersion,
      agentName: primaryThread!.agentName,
      parentThreadId: primaryThread!.id,
      status: 'idle',
    });
    const del = await app.inject({
      method: 'DELETE',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(del.statusCode).toBe(200);
    expect(del.json()).toEqual({ id, type: 'session_deleted' });
    const [retained] = await db.select().from(sessions).where(eq(sessions.id, id));
    expect(retained!.deletedAt).toBeInstanceOf(Date);
    const retainedThreads = await db
      .select()
      .from(sessionThreads)
      .where(eq(sessionThreads.sessionId, id));
    expect(retainedThreads).toHaveLength(2);
    expect(retainedThreads.every((row) => row.deletedAt !== null)).toBe(true);
    expect(deletedSessions).toContainEqual({ workspaceId, sessionId: id });
    expect(archivedSessions).not.toContainEqual({ workspaceId, sessionId: id });
    const get = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(404);
  });

  it('retains a delete sentinel for retry after soft deletion', async () => {
    const create = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { environment_id: environmentId, agent_id: agentId },
    });
    const id = create.json().id;
    const del = await withLifecycleAppendFailure(() =>
      app.inject({
        method: 'DELETE',
        url: `/v1/sessions/${id}`,
        headers: { 'x-api-key': apiKey },
      }),
    );

    expect(del.statusCode).toBe(200);
    const [pending] = await db
      .select()
      .from(sessionLifecycleOutbox)
      .where(
        and(
          eq(sessionLifecycleOutbox.workspaceId, workspaceId),
          eq(sessionLifecycleOutbox.sessionId, id),
        ),
      )
      .orderBy(desc(sessionLifecycleOutbox.createdAt))
      .limit(1);
    expect(pending).toMatchObject({
      kind: 'session.deleted',
      publishedAt: null,
      attemptCount: 1,
    });

    const result = await reconcileSessionLifecycleOutbox(db, store, { eventIds: [pending!.id] });
    expect(result).toEqual({ processed: 1, published: 1, failed: 0 });
    expect(deletedSessions).toContainEqual({ workspaceId, sessionId: id });
  });

  async function createVault(key: string, displayName: string): Promise<string> {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/vaults',
      headers: { 'x-api-key': key, 'content-type': 'application/json' },
      payload: { display_name: displayName },
    });
    expect(res.statusCode).toBe(200);
    return res.json().id;
  }

  async function withLifecycleAppendFailure<T>(operation: () => Promise<T>): Promise<T> {
    lifecycleAppendError = new Error('transcript unavailable');
    try {
      return await operation();
    } finally {
      lifecycleAppendError = null;
    }
  }

  async function withInitialEventsAppendFailure<T>(operation: () => Promise<T>): Promise<T> {
    initialEventsAppendError = new Error('transcript unavailable');
    try {
      return await operation();
    } finally {
      initialEventsAppendError = null;
    }
  }
});

async function waitForQueryBlockedBy(
  pool: Pool,
  blockerPid: number,
  queryFragment: string,
): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const result = await pool.query<{ blocked: boolean }>(
      `SELECT EXISTS (
         SELECT 1
         FROM pg_stat_activity
         WHERE $1 = ANY(pg_blocking_pids(pid))
           AND position($2 in query) > 0
       ) AS blocked`,
      [blockerPid, queryFragment],
    );
    if (result.rows[0]?.blocked) return;
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 10));
  }
  throw new Error(`timed out waiting for query containing ${queryFragment} to block`);
}

function metadataPairs(count: number): Record<string, string> {
  return Object.fromEntries(
    Array.from({ length: count }, (_, index) => [`key_${index}`, `value_${index}`]),
  );
}

function metadataWithReservedKeys(protoValue: string): Record<string, string> {
  const metadata = Object.create(null) as Record<string, string>;
  metadata['__proto__'] = protoValue;
  metadata['constructor'] = 'literal-constructor';
  return metadata;
}
