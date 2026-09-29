// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Layer A black-box coverage for the Orca cron Trigger extension.
 *
 * The live Registry is exercised only through public HTTP APIs. Direct SQL is
 * limited to removing soft-deleted Trigger fixtures after each test.
 */
import { randomUUID } from 'node:crypto';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type ApiResponse,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedWorkspaceApiKey } from '../src/seed.js';
import { createTestEnvironment, deleteTestEnvironment } from './environment-helpers.js';

interface AgentResponse {
  id: string;
  version: number;
}

interface TriggerResponse {
  id: string;
  type: 'trigger';
  name: string;
  agent: { type: 'agent'; id: string; version: number };
  session_mode: 'SESSION_PER_EVENT';
  source: {
    type: 'cron';
    schedule: string;
    timezone: string;
    payload: string;
  };
  session: {
    environment_id: string;
    title_template: string | null;
    metadata: Record<string, string>;
    vault_ids: string[];
  };
  replicas: 1;
  status: 'active' | 'paused' | 'archived';
  next_fire_at: string | null;
  last_fired_at: string | null;
  error: string | null;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

describe('Layer A: cron Trigger wire contract', () => {
  let cfg: OrcaClientConfig;
  let workspaceId: string;
  let environmentId: string;
  let pool: Pool;
  const triggerIds: string[] = [];
  const agentIds: string[] = [];

  beforeAll(async () => {
    const seeded = await seedWorkspaceApiKey();
    workspaceId = seeded.workspaceId;
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    pool = new Pool({
      connectionString:
        process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry',
      max: 1,
    });
    environmentId = await createTestEnvironment(cfg, 'trigger-wire-env');
  });

  afterEach(async () => {
    for (const triggerId of triggerIds.splice(0)) {
      await apiCall(cfg, `/v1/triggers/${triggerId}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
      await pool
        .query('DELETE FROM agent_trigger_fires WHERE workspace_id = $1 AND trigger_id = $2', [
          workspaceId,
          triggerId,
        ])
        .catch(() => {});
      await pool
        .query('DELETE FROM agent_triggers WHERE workspace_id = $1 AND id = $2', [
          workspaceId,
          triggerId,
        ])
        .catch(() => {});
    }
    for (const agentId of agentIds.splice(0)) {
      await apiCall(cfg, `/v1/agents/${agentId}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
  });

  afterAll(async () => {
    if (cfg && environmentId) {
      await deleteTestEnvironment(cfg, environmentId).catch(() => {});
    }
    await pool?.end().catch(() => {});
  });

  it('round-trips the complete v1 shape, lifecycle, idempotency, and pagination', async () => {
    const agent = await createAgent();
    const suffix = randomUUID();
    const firstBody = {
      name: `trigger-wire-first-${suffix}`,
      agent: { type: 'agent', id: agent.id, version: agent.version },
      session_mode: 'SESSION_PER_EVENT',
      source: {
        type: 'cron',
        schedule: '0 0 1 1 *',
        timezone: 'Asia/Shanghai',
        payload: 'Create the annual report.',
      },
      session: {
        environment_id: environmentId,
        title_template: '${trigger.name}: ${payload}',
        metadata: { suite: 'trigger-wire', phase: 'created' },
        vault_ids: [],
      },
      replicas: 1,
      paused: true,
    } as const;
    const idempotencyKey = `trigger-wire-${suffix}`;

    const firstResult = await apiCall(cfg, '/v1/triggers', {
      method: 'POST',
      headers: { 'idempotency-key': idempotencyKey },
      body: JSON.stringify(firstBody),
    });
    expect(firstResult.status, firstResult.text).toBe(200);
    const first = firstResult.json<TriggerResponse>();
    triggerIds.push(first.id);
    assertCompleteTrigger(first, firstBody.name, agent, {
      schedule: '0 0 1 1 *',
      timezone: 'Asia/Shanghai',
      payload: 'Create the annual report.',
      status: 'paused',
    });

    const replay = await apiCall(cfg, '/v1/triggers', {
      method: 'POST',
      headers: { 'idempotency-key': idempotencyKey },
      body: JSON.stringify(firstBody),
    });
    expect(replay.status, replay.text).toBe(200);
    expect(replay.json<TriggerResponse>().id).toBe(first.id);

    const get = await apiCall(cfg, `/v1/triggers/${first.id}`, { method: 'GET' });
    expect(get.status, get.text).toBe(200);
    expect(get.json<TriggerResponse>()).toEqual(first);

    const second = await createTrigger(agent, `trigger-wire-second-${suffix}`);
    const firstPage = await apiCall(cfg, `/v1/triggers?agent_id=${agent.id}&limit=1`, {
      method: 'GET',
    });
    expect(firstPage.status, firstPage.text).toBe(200);
    const firstPageBody = firstPage.json<{ data: TriggerResponse[]; next_page: string | null }>();
    expect(firstPageBody.data).toHaveLength(1);
    expect(firstPageBody.next_page).not.toBeNull();

    const secondPage = await apiCall(
      cfg,
      `/v1/triggers?agent_id=${agent.id}&limit=1&page=${firstPageBody.next_page}`,
      { method: 'GET' },
    );
    expect(secondPage.status, secondPage.text).toBe(200);
    const secondPageBody = secondPage.json<{ data: TriggerResponse[]; next_page: string | null }>();
    expect(secondPageBody.data).toHaveLength(1);
    expect(new Set([firstPageBody.data[0]!.id, secondPageBody.data[0]!.id])).toEqual(
      new Set([first.id, second.id]),
    );
    expect(secondPageBody.next_page).toBeNull();

    const update = await apiCall(cfg, `/v1/triggers/${first.id}`, {
      method: 'POST',
      body: JSON.stringify({
        name: `${firstBody.name}-updated`,
        session_mode: 'SESSION_PER_EVENT',
        source: {
          type: 'cron',
          schedule: '30 8 * * 1-5',
          payload: 'Create the updated report.',
        },
        session: {
          title_template: 'Updated ${trigger.name}: ${payload}',
          metadata: { phase: 'updated' },
        },
        replicas: 1,
      }),
    });
    expect(update.status, update.text).toBe(200);
    expect(update.json<TriggerResponse>()).toMatchObject({
      name: `${firstBody.name}-updated`,
      session_mode: 'SESSION_PER_EVENT',
      source: {
        type: 'cron',
        schedule: '30 8 * * 1-5',
        timezone: 'Asia/Shanghai',
        payload: 'Create the updated report.',
      },
      session: {
        environment_id: environmentId,
        title_template: 'Updated ${trigger.name}: ${payload}',
        metadata: { suite: 'trigger-wire', phase: 'updated' },
        vault_ids: [],
      },
      replicas: 1,
      status: 'paused',
      next_fire_at: null,
    });

    const unpause = await apiCall(cfg, `/v1/triggers/${first.id}/unpause`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
    expect(unpause.status, unpause.text).toBe(200);
    expect(unpause.json<TriggerResponse>()).toMatchObject({
      status: 'active',
      next_fire_at: expect.any(String),
    });

    const pause = await apiCall(cfg, `/v1/triggers/${first.id}/pause`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
    expect(pause.status, pause.text).toBe(200);
    expect(pause.json<TriggerResponse>()).toMatchObject({ status: 'paused', next_fire_at: null });

    const sessions = await apiCall(cfg, `/v1/triggers/${first.id}/sessions?limit=1`, {
      method: 'GET',
    });
    expect(sessions.status, sessions.text).toBe(200);
    expect(sessions.json()).toEqual({ data: [], next_page: null });

    const deleted = await apiCall(cfg, `/v1/triggers/${first.id}`, {
      method: 'DELETE',
      body: JSON.stringify({}),
    });
    expect(deleted.status, deleted.text).toBe(200);
    expect(deleted.json()).toEqual({ id: first.id, type: 'trigger_deleted' });

    const removed = await apiCall(cfg, `/v1/triggers/${first.id}`, { method: 'GET' });
    expect(removed.status, removed.text).toBe(404);
    expect(removed.json()).toMatchObject({ error: { type: 'not_found_error' } });

    const activeList = await apiCall(cfg, `/v1/triggers?agent_id=${agent.id}`, {
      method: 'GET',
    });
    expect(activeList.status, activeList.text).toBe(200);
    expect(activeList.json<{ data: TriggerResponse[] }>().data.map((item) => item.id)).toEqual([
      second.id,
    ]);

    const archivedList = await apiCall(
      cfg,
      `/v1/triggers?agent_id=${agent.id}&include_archived=true`,
      { method: 'GET' },
    );
    expect(archivedList.status, archivedList.text).toBe(200);
    expect(
      new Set(archivedList.json<{ data: TriggerResponse[] }>().data.map((item) => item.id)),
    ).toEqual(new Set([second.id]));
  });

  it.each([
    [
      'shared session mode',
      {
        session_mode: 'SHARED',
      },
    ],
    [
      'multiple replicas',
      {
        replicas: 2,
      },
    ],
    [
      'non-cron source',
      {
        source: { type: 'kafka', topic: 'events' },
      },
    ],
    [
      'invalid cron schedule',
      {
        source: { type: 'cron', schedule: 'not a cron', payload: 'payload' },
      },
    ],
    [
      'legacy top-level payload and schedule',
      {
        payload: 'legacy payload',
        schedule: '* * * * *',
      },
    ],
  ])('rejects unsupported %s', async (_label, override) => {
    const agent = await createAgent();
    const body = {
      name: `trigger-wire-invalid-${randomUUID()}`,
      agent: { type: 'agent', id: agent.id, version: agent.version },
      session_mode: 'SESSION_PER_EVENT',
      source: { type: 'cron', schedule: '0 0 1 1 *', payload: 'payload' },
      session: { environment_id: environmentId },
      replicas: 1,
      paused: true,
      ...override,
    };
    const response = await apiCall(cfg, '/v1/triggers', {
      method: 'POST',
      body: JSON.stringify(body),
    });
    assertInvalidRequest(response);
  });

  async function createAgent(): Promise<AgentResponse> {
    const response = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `trigger-wire-agent-${randomUUID()}`,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-5-20250929' },
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      }),
    });
    expect(response.status, response.text).toBe(200);
    const agent = response.json<AgentResponse>();
    agentIds.push(agent.id);
    return agent;
  }

  async function createTrigger(agent: AgentResponse, name: string): Promise<TriggerResponse> {
    const response = await apiCall(cfg, '/v1/triggers', {
      method: 'POST',
      body: JSON.stringify({
        name,
        agent: { type: 'agent', id: agent.id, version: agent.version },
        session_mode: 'SESSION_PER_EVENT',
        source: { type: 'cron', schedule: '0 0 1 1 *', payload: 'Second payload.' },
        session: { environment_id: environmentId },
        replicas: 1,
        paused: true,
      }),
    });
    expect(response.status, response.text).toBe(200);
    const trigger = response.json<TriggerResponse>();
    triggerIds.push(trigger.id);
    return trigger;
  }
});

function assertCompleteTrigger(
  trigger: TriggerResponse,
  name: string,
  agent: AgentResponse,
  expected: {
    schedule: string;
    timezone: string;
    payload: string;
    status: TriggerResponse['status'];
  },
): void {
  expect(Object.keys(trigger).sort()).toEqual(
    [
      'agent',
      'archived_at',
      'created_at',
      'error',
      'id',
      'last_fired_at',
      'name',
      'next_fire_at',
      'replicas',
      'session',
      'session_mode',
      'source',
      'status',
      'type',
      'updated_at',
    ].sort(),
  );
  expect(trigger).toMatchObject({
    type: 'trigger',
    name,
    agent: { type: 'agent', id: agent.id, version: agent.version },
    session_mode: 'SESSION_PER_EVENT',
    source: {
      type: 'cron',
      schedule: expected.schedule,
      timezone: expected.timezone,
      payload: expected.payload,
    },
    replicas: 1,
    status: expected.status,
    next_fire_at: expected.status === 'active' ? expect.any(String) : null,
    last_fired_at: null,
    error: null,
    archived_at: null,
    created_at: expect.any(String),
    updated_at: expect.any(String),
  });
}

function assertInvalidRequest(response: ApiResponse): void {
  expect(response.status, response.text).toBe(400);
  expect(response.json()).toMatchObject({
    type: 'error',
    error: { type: 'invalid_request_error' },
  });
}
