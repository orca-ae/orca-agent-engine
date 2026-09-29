// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import {
  createTestEnvironment,
  provisionWorkspace,
  type ProvisionedWorkspace,
} from './environment-helpers.js';
import {
  seedIsolatedOrganizationAdmin,
  type IsolatedOrganizationAdmin,
} from './organization-admin-helpers.js';

const BASE = '/apis/policy.runorca.ai/v1/guardrails';
const ADMIN = '/v1/organizations/guardrails';
const publicURL = process.env['ORCA_BASE_URL'] ?? 'http://localhost:8080';
const adminURL = process.env['ORCA_ADMIN_BASE_URL'] ?? 'http://localhost:8082';
const costKinds = ['cost_budget', 'subagent_cost_budget', 'user_daily_cost_budget'] as const;

describe('Layer A: budget authoring and authority through live HTTP', () => {
  let organization: IsolatedOrganizationAdmin | undefined;
  let workspace: ProvisionedWorkspace | undefined;
  let cfg: OrcaClientConfig;
  let admin: OrcaClientConfig;
  let reader: OrcaClientConfig;
  const created: Array<{ cfg: OrcaClientConfig; path: string }> = [];
  beforeAll(async () => {
    await ensureStackReachable(
      buildClientFromConfig({ baseURL: publicURL, apiKey: 'health-probe' }),
    );
    organization = await seedIsolatedOrganizationAdmin('budget-wire', {
      reader: ['guardrails:read'],
    });
    admin = buildClientFromConfig({ baseURL: adminURL, apiKey: organization.apiKey });
    reader = buildClientFromConfig({
      baseURL: adminURL,
      apiKey: organization.additionalApiKeys['reader']!,
    });
    workspace = await provisionWorkspace(admin, 'budget-wire');
    cfg = buildClientFromConfig({ baseURL: publicURL, apiKey: workspace.cfg.apiKey });
  });
  afterAll(async () => {
    const errors: unknown[] = [];
    const clean = async (operation: () => Promise<unknown>) => {
      try {
        await operation();
      } catch (error) {
        errors.push(error);
      }
    };
    for (const resource of created.reverse())
      await clean(async () => {
        const response = await apiCall(resource.cfg, resource.path, {
          method: 'DELETE',
          body: '{}',
        });
        expect(response.status, response.text).toBe(200);
      });
    if (workspace)
      await clean(async () => {
        const response = await apiCall(
          admin,
          `/v1/organizations/workspaces/${workspace!.id}/archive`,
          { method: 'POST', body: '{}' },
        );
        expect(response.status, response.text).toBe(200);
      });
    if (organization) await clean(() => organization!.archive());
    if (errors.length) throw new AggregateError(errors, 'Budget wire cleanup failed');
  });
  async function create(builtin: string, params: unknown, scope = 'explicit', client = cfg) {
    const path = client === cfg ? BASE : ADMIN;
    const response = await apiCall(client, path, {
      method: 'POST',
      body: JSON.stringify({
        name: `budget-${randomUUID()}`,
        ...(client === cfg ? { scope } : {}),
        rule: { kind: 'builtin', builtin, params },
      }),
    });
    if (response.status === 201)
      created.push({ cfg: client, path: `${path}/${response.json<{ id: string }>().id}` });
    return response;
  }
  it('advertises token and cost schemas with daily authority scopes', async () => {
    const response = await apiCall(cfg, '/apis/policy.runorca.ai/v1/guardrailtypes');
    expect(response.status, response.text).toBe(200);
    const entries = response.json<{ data: Array<Record<string, unknown>> }>().data;
    for (const name of ['token_budget', ...costKinds]) {
      expect(entries.find((entry) => entry.name === name)).toMatchObject({
        name,
        stateful: true,
        phases: ['request', 'tool_call'],
      });
    }
    expect(entries.find((entry) => entry.name === 'user_daily_cost_budget')).toMatchObject({
      allowedScopes: ['workspace', 'organization'],
      stateScope: 'subject_window',
    });
  });
  it.each([
    ['token_budget', {}],
    ['token_budget', { max_total_tokens: 0 }],
    ['token_budget', { max_total_tokens: 1.5 }],
    ['token_budget', { max_total_tokens: 10, ask_thresholds: [0] }],
    ['cost_budget', {}],
    ['cost_budget', { ask_thresholds_usd: [] }],
    ['cost_budget', { max_cost_usd: 0 }],
    ['cost_budget', { max_cost_usd: -1 }],
    ['cost_budget', { max_cost_usd: 1, on_unpriced: 'ignore' }],
    ['cost_budget', { ask_thresholds_usd: [0] }],
    ['cost_budget', { max_cost_usd: 1, unexpected: true }],
    ['subagent_cost_budget', {}],
    ['user_daily_cost_budget', { ask_thresholds_usd: [1] }],
  ] as const)('rejects malformed %s parameters %j', async (builtin, params) => {
    const response = await create(
      builtin,
      params,
      builtin === 'user_daily_cost_budget' ? 'workspace' : 'explicit',
    );
    expect(response.status, response.text).toBe(400);
    expect(response.json()).toMatchObject({
      type: 'error',
      error: { type: 'invalid_request_error', message: expect.any(String) },
    });
  });
  it.each(['ask', 'deny', 'allow'] as const)(
    'round-trips on_unpriced=%s for each cost budget',
    async (on_unpriced) => {
      for (const builtin of costKinds) {
        const params = {
          max_cost_usd: 5,
          ask_thresholds_usd: [1, 2],
          on_unpriced,
          expensive_models: ['expensive-model'],
        };
        const response = await create(
          builtin,
          params,
          builtin === 'user_daily_cost_budget' ? 'workspace' : 'explicit',
        );
        expect(response.status, response.text).toBe(201);
        expect(response.json()).toMatchObject({ rule: { kind: 'builtin', builtin, params } });
      }
    },
  );
  it('accepts positive integer tokens and threshold-only session/subagent budgets', async () => {
    for (const [builtin, params] of [
      ['token_budget', { max_total_tokens: 1 }],
      ['cost_budget', { ask_thresholds_usd: [0.01] }],
      ['subagent_cost_budget', { ask_thresholds_usd: [0.01] }],
    ] as const) {
      const response = await create(builtin, params);
      expect(response.status, response.text).toBe(201);
    }
  });
  it('rejects invalid updates without replacing the accepted rule', async () => {
    const response = await create('cost_budget', { max_cost_usd: 5 });
    expect(response.status).toBe(201);
    const path = `${BASE}/${response.json<{ id: string }>().id}`;
    const changed = await apiCall(cfg, path, {
      method: 'POST',
      body: JSON.stringify({
        rule: { kind: 'builtin', builtin: 'cost_budget', params: { max_cost_usd: 0 } },
      }),
    });
    expect(changed.status, changed.text).toBe(400);
    const current = await apiCall(cfg, path);
    expect(current.status).toBe(200);
    expect(current.json()).toMatchObject({ rule: { params: { max_cost_usd: 5 } } });
  });
  it('restricts daily authoring scope while preserving visible Agent and Session references', async () => {
    expect((await create('user_daily_cost_budget', { max_cost_usd: 1 })).status).toBe(400);
    const daily = await create('user_daily_cost_budget', { max_cost_usd: 1 }, 'workspace');
    expect(daily.status, daily.text).toBe(201);
    const dailyId = daily.json<{ id: string }>().id;
    const agentBody = {
      name: 'budget reference',
      model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
    };
    const environmentId = await createTestEnvironment(cfg, 'budget-wire');
    created.push({ cfg, path: `/v1/environments/${environmentId}` });
    const referenced = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      headers: { 'orca-beta': 'guardrails' },
      body: JSON.stringify({ ...agentBody, guardrail_ids: [dailyId] }),
    });
    expect(referenced.status, referenced.text).toBe(200);
    expect(referenced.json()).toMatchObject({ guardrail_ids: [dailyId] });
    const agentId = referenced.json<{ id: string }>().id;
    created.push({ cfg, path: `/v1/agents/${agentId}` });
    const session = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      headers: { 'orca-beta': 'guardrails' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent: { type: 'agent_with_overrides', id: agentId, guardrail_ids: [dailyId] },
      }),
    });
    expect(session.status, session.text).toBe(200);
    created.push({ cfg, path: `/v1/sessions/${session.json<{ id: string }>().id}` });
    const stored = await apiCall(cfg, `${BASE}/${dailyId}`);
    expect(stored.status, stored.text).toBe(200);
    expect(stored.json()).toMatchObject({ scope: 'workspace' });
    expect(
      (await create('user_daily_cost_budget', { max_cost_usd: 1 }, 'organization', admin)).status,
    ).toBe(201);
    const denied = await create(
      'user_daily_cost_budget',
      { max_cost_usd: 1 },
      'organization',
      reader,
    );
    expect(denied.status, denied.text).toBe(403);
    expect(denied.text).toContain('guardrails:write');
  });
});
