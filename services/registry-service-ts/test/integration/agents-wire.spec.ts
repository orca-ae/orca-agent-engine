// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { eq } from 'drizzle-orm';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { agents } from '../../src/persistence/postgres/schema.js';

/**
 * Wire-format compat (registry edge-translation): model encodings,
 * custom-tool schema, agent_toolset configs.
 */
describe('Agents wire-format compat (integration)', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let apiKey: string;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();
    apiKey = await createTestApiKey(db, uniqueWorkspace('agents-wire'));
  });
  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  const create = (payload: Record<string, unknown>, beta = false) =>
    app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        ...(beta ? { 'orca-beta': '1' } : {}),
      },
      payload,
    });

  it('accepts a bare model string and echoes Claude {id, speed} by default', async () => {
    const res = await create({
      name: `m-string-${Date.now()}`,
      model: 'claude-3-5-sonnet-20240620',
    });
    expect(res.statusCode).toBe(200);
    expect(res.json().model).toEqual({ id: 'claude-3-5-sonnet-20240620', speed: 'standard' });
  });

  it('accepts the Claude model object and returns resolved controls', async () => {
    const res = await create({
      name: `m-speed-${Date.now()}`,
      model: { id: 'claude-opus-5', speed: 'fast' },
    });
    expect(res.statusCode).toBe(200);
    const id = res.json().id;
    expect(res.json().model).toEqual({
      id: 'claude-opus-5',
      speed: 'fast',
      effort: { type: 'high' },
    });

    const get = await app.inject({
      method: 'GET',
      url: `/v1/agents/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.json().model).toEqual({
      id: 'claude-opus-5',
      speed: 'fast',
      effort: { type: 'high' },
    });
  });

  it('rejects explicit fast mode for unsupported models', async () => {
    const unsupported = await create({
      name: `unsupported-fast-${Date.now()}`,
      model: { id: 'claude-opus-4-6', speed: 'fast' },
    });
    expect(unsupported.statusCode).toBe(400);
    expect(unsupported.json().error.message).toMatch(/claude-opus-5/);
  });

  it('preserves omitted controls only when the model identity is unchanged', async () => {
    const created = await create({
      name: `update-fast-${Date.now()}`,
      model: { id: 'claude-opus-5', speed: 'fast', effort: { type: 'xhigh' } },
    });
    expect(created.statusCode).toBe(200);

    const sameModel = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { model: { id: 'claude-opus-5' } },
    });
    expect(sameModel.statusCode).toBe(200);
    expect(sameModel.json().model).toEqual({
      id: 'claude-opus-5',
      speed: 'fast',
      effort: { type: 'xhigh' },
    });

    const sameModelString = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { model: 'claude-opus-5' },
    });
    expect(sameModelString.statusCode).toBe(200);
    expect(sameModelString.json().model).toEqual({
      id: 'claude-opus-5',
      speed: 'fast',
      effort: { type: 'xhigh' },
    });

    const replacement = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { model: { id: 'claude-sonnet-4-6' } },
    });
    expect(replacement.statusCode).toBe(200);
    expect(replacement.json().model).toEqual({
      id: 'claude-sonnet-4-6',
      speed: 'standard',
      effort: { type: 'high' },
    });
  });

  it('rejects unsupported effort levels at create and update time', async () => {
    const unsupported = await create({
      name: `unsupported-effort-${Date.now()}`,
      model: { id: 'claude-sonnet-4-6', effort: { type: 'xhigh' } },
    });
    expect(unsupported.statusCode).toBe(400);
    expect(unsupported.json().error.message).toMatch(/supported levels are low, medium, high, max/);

    const created = await create({
      name: `update-effort-${Date.now()}`,
      model: 'claude-sonnet-4-6',
    });
    expect(created.statusCode).toBe(200);
    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { model: { id: 'claude-sonnet-4-6', effort: 'xhigh' } },
    });
    expect(update.statusCode).toBe(400);
    expect(update.json().error.message).toMatch(/supported levels are low, medium, high, max/);
  });

  it('rejects unrelated updates to legacy rows with invalid fast controls', async () => {
    const created = await create({
      name: `legacy-invalid-fast-${Date.now()}`,
      model: { id: 'claude-opus-5', speed: 'fast' },
    });
    expect(created.statusCode).toBe(200);
    await db
      .update(agents)
      .set({ modelId: 'claude-opus-4-6' })
      .where(eq(agents.id, created.json().id));

    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${created.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { description: 'touch invalid legacy row' },
    });
    expect(update.statusCode).toBe(400);
    expect(update.json().error.message).toMatch(/claude-opus-5/);
  });

  it('preserves Pi omitted effort on create, rename, clear and session override', async () => {
    const model = { provider: 'anthropic', id: 'claude-sonnet-4-6' };
    const created = await create({
      name: 'pi-effort-default',
      model,
      metadata: { harness: 'pi_sdk' },
    });
    expect(created.statusCode, created.body).toBe(200);
    const agentId = created.json().id;
    expect(created.json().model).not.toHaveProperty('effort');
    const headers = { 'x-api-key': apiKey, 'content-type': 'application/json' };
    for (const patch of [
      { name: 'renamed' },
      { model: { ...model, effort: 'high' } },
      { model: { ...model, effort: null } },
    ]) {
      const updated = await app.inject({
        method: 'POST',
        url: `/v1/agents/${agentId}`,
        headers,
        payload: patch,
      });
      expect(updated.statusCode, updated.body).toBe(200);
      if (patch.model?.effort !== 'high') expect(updated.json().model).not.toHaveProperty('effort');
    }
    const env = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers,
      payload: { name: 'pi-effort', config: { type: 'cloud' } },
    });
    expect(env.statusCode, env.body).toBe(200);
    const session = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers,
      payload: {
        environment_id: env.json().id,
        agent: { type: 'agent_with_overrides', id: agentId, version: 4, model },
      },
    });
    expect(session.statusCode, session.body).toBe(200);
    expect(session.json().agent.model).not.toHaveProperty('effort');
    const update = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${session.json().id}`,
      headers,
      payload: { agent: { model } },
    });
    expect(update.statusCode, update.body).toBe(200);
    expect(update.json().agent.model).not.toHaveProperty('effort');
  });

  it('rejects model controls for non-Anthropic providers', async () => {
    const effort = await create(
      {
        name: `unsupported-provider-effort-${Date.now()}`,
        model: { provider: 'openai', id: 'gpt-5', effort: { type: 'high' } },
      },
      true,
    );
    expect(effort.statusCode).toBe(400);
    expect(effort.json().error.message).toMatch(/supports model providers: anthropic/);

    const speed = await create(
      {
        name: `unsupported-provider-speed-${Date.now()}`,
        model: { provider: 'openai', id: 'gpt-5', speed: 'standard' },
      },
      true,
    );
    expect(speed.statusCode).toBe(400);
    expect(speed.json().error.message).toMatch(/supports model providers: anthropic/);
  });

  it('rejects mixed coordinator speeds at create and update time', async () => {
    const child = await create({
      name: `standard-child-${Date.now()}`,
      model: 'claude-opus-4-8',
    });
    expect(child.statusCode).toBe(200);

    const mixedCreate = await create({
      name: `mixed-coordinator-${Date.now()}`,
      model: { id: 'claude-opus-5', speed: 'fast' },
      multiagent: { type: 'coordinator', agents: [child.json().id] },
    });
    expect(mixedCreate.statusCode).toBe(400);
    expect(mixedCreate.json().error.message).toMatch(/mixed model\.speed/);

    const coordinator = await create({
      name: `standard-coordinator-${Date.now()}`,
      model: 'claude-opus-5',
      multiagent: { type: 'coordinator', agents: [child.json().id] },
    });
    expect(coordinator.statusCode).toBe(200);
    const mixedUpdate = await app.inject({
      method: 'POST',
      url: `/v1/agents/${coordinator.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { model: { id: 'claude-opus-5', speed: 'fast' } },
    });
    expect(mixedUpdate.statusCode).toBe(400);
    expect(mixedUpdate.json().error.message).toMatch(/mixed model\.speed/);
  });

  it('resolves nullable controls to defaults and separates them from metadata patches', async () => {
    const createResponse = await create({
      name: `nullable-${Date.now()}`,
      description: 'top-level description',
      metadata: { description: 'metadata value', removable: 'yes' },
      model: { id: 'claude-opus-5', effort: { type: 'low' } },
      system: null,
    });
    expect(createResponse.statusCode).toBe(200);
    expect(createResponse.json()).toMatchObject({
      description: 'top-level description',
      metadata: { description: 'metadata value', removable: 'yes' },
      model: { id: 'claude-opus-5', speed: 'standard', effort: { type: 'low' } },
      system: null,
    });

    const update = await app.inject({
      method: 'POST',
      url: `/v1/agents/${createResponse.json().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        description: null,
        metadata: { removable: null, owner: 'platform' },
        model: { id: 'claude-opus-5', effort: null, speed: null },
        system: null,
        tools: null,
        mcp_servers: null,
        skills: null,
        multiagent: null,
      },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json()).toMatchObject({
      description: null,
      metadata: { description: 'metadata value', owner: 'platform' },
      model: { id: 'claude-opus-5', speed: 'standard', effort: { type: 'high' } },
      system: null,
      tools: [],
      mcp_servers: [],
      skills: [],
      multiagent: null,
      version: 2,
    });
  });

  it('emits legacy {provider, id} for orca-beta clients', async () => {
    const res = await create(
      {
        name: `m-beta-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-opus-5', speed: 'fast' },
      },
      true,
    );
    expect(res.statusCode).toBe(200);
    expect(res.json().model).toEqual({ provider: 'anthropic', id: 'claude-opus-5' });
  });

  it('rejects an empty model string with 400', async () => {
    const res = await create({ name: `m-bad-${Date.now()}`, model: '' });
    expect(res.statusCode).toBe(400);
  });

  it('persists and echoes a custom tool description + input_schema', async () => {
    const inputSchema = {
      type: 'object',
      properties: { q: { type: 'string' } },
      required: ['q'],
    };
    const res = await create({
      name: `t-custom-${Date.now()}`,
      model: 'claude-sonnet-4-6',
      tools: [
        {
          type: 'custom',
          name: 'search',
          description: 'Search the web',
          input_schema: inputSchema,
        },
      ],
    });
    expect(res.statusCode).toBe(200);
    const tool = res.json().tools[0];
    expect(tool).toMatchObject({
      type: 'custom',
      name: 'search',
      description: 'Search the web',
      input_schema: inputSchema,
    });
  });

  it('round-trips agent_toolset_20260401 default_config + configs', async () => {
    const res = await create({
      name: `t-set-${Date.now()}`,
      model: 'claude-sonnet-4-6',
      tools: [
        {
          type: 'agent_toolset_20260401',
          default_config: { enabled: true, permission_policy: { type: 'always_ask' } },
          configs: [{ name: 'bash', enabled: false, permission_policy: { type: 'always_allow' } }],
        },
      ],
    });
    expect(res.statusCode).toBe(200);
    const tool = res.json().tools[0];
    // Tool TYPE name is echoed in dated form to default clients (existing behavior).
    expect(tool.type).toBe('agent_toolset_20260401');
    expect(tool.default_config).toEqual({
      enabled: true,
      permission_policy: { type: 'always_ask' },
    });
    expect(tool.configs).toEqual([
      { name: 'bash', enabled: false, permission_policy: { type: 'always_allow' } },
    ]);
  });

  it('rejects the unsupported legacy always_deny policy', async () => {
    const res = await create({
      name: `t-deny-${Date.now()}`,
      model: 'claude-sonnet-4-6',
      tools: [
        {
          type: 'agent_toolset_20260401',
          configs: [{ name: 'bash', permission_policy: { type: 'always_deny' } }],
        },
      ],
    });
    expect(res.statusCode).toBe(400);
    expect(res.json()).toMatchObject({
      type: 'error',
      error: {
        type: 'invalid_request_error',
        message: 'tools.0.configs: Invalid input',
      },
      request_id: expect.any(String),
    });
  });
});
