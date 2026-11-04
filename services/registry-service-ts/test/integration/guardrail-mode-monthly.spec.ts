// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { and, eq } from 'drizzle-orm';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  apiKeys,
  guardrailCounters,
  sessionEventsIndex,
} from '../../src/persistence/postgres/schema.js';
import { buildCombinedTestApp } from '../../src/server.js';
import { persistUsage, loadGuardrailBudgetState } from '../../src/domain/usage-accounting.js';
import { prepareGatewayGuardrails } from '../../src/domain/prepare-execution.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import {
  createTestAgent,
  createTestApiKey,
  createTestSession,
  uniqueWorkspace,
} from './fixtures.js';

const BASE = '/apis/policy.runorca.ai/v1';
const block = { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } };
const month = {
  kind: 'builtin',
  builtin: 'workspace_monthly_cost_budget',
  params: { max_cost_usd: 5 },
};
const usage = {
  inputTokens: 1,
  outputTokens: 0,
  cacheReadInputTokens: 0,
  cacheCreationEphemeral1hInputTokens: 0,
  cacheCreationEphemeral5mInputTokens: 0,
};

describe('Registry guardrail mode, monthly budgets and leases', () => {
  let db: DbClient;
  let app: FastifyInstance;
  let workspaceId: string;
  let key: string;
  let sessionId: string;
  let currentTime = new Date('2026-10-31T23:59:00Z');
  const gates = { observeEnabled: false, monthlyBudgetsEnabled: false, now: () => currentTime };
  const headers = () => ({ 'x-api-key': key });
  const create = (body: Record<string, unknown>) =>
    app.inject({ method: 'POST', url: `${BASE}/guardrails`, headers: headers(), payload: body });
  beforeAll(async () => {
    ({ db } = await getTestDb());
    workspaceId = uniqueWorkspace('oip028');
    key = await createTestApiKey(db, workspaceId);
    await db
      .update(apiKeys)
      .set({ scopes: ['workspace.full_access'] })
      .where(eq(apiKeys.workspaceId, workspaceId));
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'test' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      guardrails: gates,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    const url = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    const agent = await createTestAgent(url, key);
    sessionId = await createTestSession(url, key, agent);
  });
  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  it('gates authoring/catalog independently and preserves stored observe at rollback', async () => {
    expect((await create({ name: 'observe', mode: 'observe', rule: block })).statusCode).toBe(400);
    expect((await create({ name: 'month', rule: month })).statusCode).toBe(400);
    expect(
      (await app.inject({ url: `${BASE}/guardrailtypes`, headers: headers() }))
        .json()
        .data.some((x: { name: string }) => x.name.includes('monthly')),
    ).toBe(false);
    const legacy = await create({ name: 'legacy', rule: block });
    expect(legacy.statusCode).toBe(201);
    expect(
      (
        await app.inject({
          method: 'POST',
          url: `${BASE}/guardrails/${legacy.json().id}`,
          headers: headers(),
          payload: { enabled: false, mode: 'observe' },
        })
      ).statusCode,
    ).toBe(400);
    await app.inject({
      method: 'POST',
      url: `${BASE}/guardrails/${legacy.json().id}`,
      headers: headers(),
      payload: { enabled: false },
    });
    gates.observeEnabled = true;
    gates.monthlyBudgetsEnabled = true;
    const made = await create({ name: 'observe-month', mode: 'observe', rule: month });
    expect(made.statusCode).toBe(201);
    const id = made.json().id as string;
    const prepUrl = `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/executions:prepare`;
    expect((await app.inject({ method: 'POST', url: prepUrl, payload: {} })).statusCode).toBe(409);
    expect(
      (
        await app.inject({
          method: 'POST',
          url: prepUrl,
          payload: {
            guardrail_capabilities: ['guardrails.observe.v1', 'guardrails.monthly_budgets.v1'],
          },
        })
      ).statusCode,
    ).toBe(200);

    expect(made.json()).toMatchObject({ mode: 'observe', created_at: currentTime.toISOString() });
    expect(
      (
        await prepareGatewayGuardrails({ db, workspaceId, sessionId, guardrails: gates })
      ).guardrails.find((g) => g.id === id),
    ).toMatchObject({ mode: 'observe', state_scope: 'workspace_window' });
    gates.observeEnabled = false;
    await expect(
      prepareGatewayGuardrails({ db, workspaceId, sessionId, guardrails: gates }),
    ).rejects.toThrow(/OBSERVE/);
    expect(
      (
        await app.inject({
          url: `${BASE}/effectiveguardrails?guardrail_capabilities=guardrails.observe.v1,guardrails.monthly_budgets.v1`,
          headers: headers(),
        })
      ).statusCode,
    ).toBe(503);
    expect(
      (
        await app.inject({
          method: 'POST',
          url: `${BASE}/guardrails/${id}`,
          headers: headers(),
          payload: { enabled: false },
        })
      ).statusCode,
    ).toBe(200);
    gates.observeEnabled = true;
    gates.monthlyBudgetsEnabled = true;
  });

  it('renews unchanged content after two hours on both routes and checks current auth/composition', async () => {
    const made = await create({ name: 'lease-test', rule: block });
    expect(made.statusCode).toBe(201);
    const first = await app.inject({
      url: `${BASE}/effectiveguardrails?guardrail_capabilities=guardrails.observe.v1,guardrails.monthly_budgets.v1`,
      headers: headers(),
    });
    expect(first.statusCode).toBe(200);
    expect(first.headers['cache-control']).toBe('no-store');
    expect(first.headers['orca-guardrail-issued-at']).toBe(first.json().issued_at);
    const etag = first.headers.etag as string;
    currentTime = new Date(currentTime.getTime() + 3 * 3600000);
    for (const url of [
      `${BASE}/effectiveguardrails?guardrail_capabilities=guardrails.observe.v1,guardrails.monthly_budgets.v1`,
      `/internal/v1/workspaces/${workspaceId}/guardrails/effective?guardrail_capabilities=guardrails.observe.v1,guardrails.monthly_budgets.v1`,
    ]) {
      const renewed = await app.inject({ url, headers: { ...headers(), 'if-none-match': etag } });
      expect(renewed.statusCode).toBe(304);
      expect(renewed.body).toBe('');
      expect(renewed.headers.etag).toBe(etag);
      expect(renewed.headers['orca-guardrail-generation']).toBe(String(currentTime.getTime()));
      expect(renewed.headers['orca-guardrail-expires-at']).toBe(
        new Date(currentTime.getTime() + 3600000).toISOString(),
      );
    }
    const changed = await app.inject({
      method: 'POST',
      url: `${BASE}/guardrails/${made.json().id}`,
      headers: headers(),
      payload: { mode: 'observe' },
    });
    expect(changed.statusCode).toBe(200);
    const next = await app.inject({
      url: `${BASE}/effectiveguardrails?guardrail_capabilities=guardrails.observe.v1,guardrails.monthly_budgets.v1`,
      headers: { ...headers(), 'if-none-match': etag },
    });
    expect(next.statusCode).toBe(200);
    expect(next.headers.etag).not.toBe(etag);
    const forbidden = await app.inject({
      url: `${BASE}/effectiveguardrails?guardrail_capabilities=guardrails.observe.v1,guardrails.monthly_budgets.v1`,
      headers: { 'if-none-match': next.headers.etag as string },
    });
    expect(forbidden.statusCode).toBe(401);
    await db
      .update(apiKeys)
      .set({ scopes: ['workspace.sessions.describe'] })
      .where(eq(apiKeys.workspaceId, workspaceId));
    expect(
      (
        await app.inject({
          url: `${BASE}/effectiveguardrails?guardrail_capabilities=guardrails.observe.v1,guardrails.monthly_budgets.v1`,
          headers: headers(),
        })
      ).statusCode,
    ).toBe(403);
    await db
      .update(apiKeys)
      .set({ scopes: ['workspace.full_access'] })
      .where(eq(apiKeys.workspaceId, workspaceId));
    gates.observeEnabled = false;
    expect(
      (
        await app.inject({
          url: `${BASE}/effectiveguardrails?guardrail_capabilities=guardrails.observe.v1,guardrails.monthly_budgets.v1`,
          headers: { ...headers(), 'if-none-match': next.headers.etag as string },
        })
      ).statusCode,
    ).toBe(503);
    gates.observeEnabled = true;
  });

  it('updates principal day/month and workspace month atomically, with replay and rollover isolation', async () => {
    const input = {
      workspaceId,
      sessionId,
      usage,
      costNanoUsd: 1250000000n,
      now: new Date('2026-10-31T23:59:59Z'),
      turnSubject: 'usr_alice',
      usageEventId: 'evt_oip028october',
    };
    expect(await db.transaction((tx) => persistUsage(tx, input))).toBe('applied');
    await db.transaction((tx) => persistUsage(tx, { ...input, costNanoUsd: 999000000000n }));
    const first = await loadGuardrailBudgetState(db, workspaceId, 'usr_alice', input.now, true);
    expect(first).toMatchObject({
      subject_window: { daily_cost_usd: 1.25, monthly_cost_usd: 1.25 },
      workspace_window: { workspace_monthly_cost_usd: 1.25 },
    });
    await expect(
      db.transaction(async (tx) => {
        await persistUsage(tx, { ...input, usageEventId: 'evt_oip028rollback' });
        throw new Error('rollback');
      }),
    ).rejects.toThrow('rollback');
    expect(
      (await loadGuardrailBudgetState(db, workspaceId, 'usr_alice', input.now, true))
        .workspace_window,
    ).toEqual(first.workspace_window);
    const november = new Date('2026-11-01T00:00:00Z');
    await db.transaction((tx) =>
      persistUsage(tx, {
        ...input,
        usageEventId: 'evt_oip028november',
        turnSubject: 'usr_bob',
        now: november,
        costNanoUsd: null,
      }),
    );
    expect(
      await loadGuardrailBudgetState(db, workspaceId, 'usr_bob', november, true),
    ).toMatchObject({
      subject_window: { daily_cost_unpriced: 1, monthly_cost_unpriced: 1 },
      workspace_window: { workspace_monthly_cost_unpriced: 1 },
    });
    expect(
      (await loadGuardrailBudgetState(db, workspaceId, 'usr_alice', november, true)).subject_window,
    ).toEqual({});
    const otherWs = uniqueWorkspace('oip028other');
    await createTestApiKey(db, otherWs);
    expect(await loadGuardrailBudgetState(db, otherWs, 'usr_alice', input.now, true)).toEqual({
      subject_window: {},
      workspace_window: {},
    });
  });

  it('resolves a UUID turn to its authenticated principal and refuses old-period approvals', async () => {
    const made = await create({
      name: 'principal-month',
      rule: { kind: 'builtin', builtin: 'user_monthly_cost_budget', params: { max_cost_usd: 10 } },
    });
    expect(made.statusCode).toBe(201);
    const eventId = 'evt_019c1111-1234-7000-8000-123456789abc';
    await db.insert(sessionEventsIndex).values({
      workspaceId,
      sessionId,
      eventId,
      seq: 1,
      producedAt: currentTime.toISOString(),
      producedBy: 'client',
      kind: 'user.message',
      visibility: 'public',
      guardrailSubject: 'usr_alice',
    });
    const url = `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/guardrail-state`;
    const stateKey = `g:${made.json().id}:budget_ask_approved`;
    const payload = {
      turn_event_id: eventId,
      subject: 'usr_forged',
      window: '2026-11',
      updates: [{ scope: 'subject_window', key: stateKey, action: 'set', value: 5 }],
    };
    expect((await app.inject({ method: 'POST', url, payload })).statusCode).toBe(200);
    const [row] = await db
      .select()
      .from(guardrailCounters)
      .where(
        and(eq(guardrailCounters.workspaceId, workspaceId), eq(guardrailCounters.key, stateKey)),
      );
    expect(row).toMatchObject({ subject: 'usr_alice', window: '2026-11', valueNum: 5 });
    const refresh = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/guardrail-subject-window`,
      payload: { turn_event_id: eventId },
    });
    expect(refresh.statusCode).toBe(200);
    expect(refresh.json().guardrail_month_window).toBe('2026-11');
    expect(refresh.json().guardrail_subject_window_state[stateKey]).toBe(5);
    const previousTime = currentTime;
    currentTime = new Date('2026-12-01T00:00:00Z');
    expect((await app.inject({ method: 'POST', url, payload })).statusCode).toBe(409);
    currentTime = previousTime;
  });

  it('stores workspace approvals under the server-owned subject and refuses forged principal identity', async () => {
    const url = `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/guardrail-state`;
    const update = {
      scope: 'workspace_window',
      key: 'g:workspace:budget_ask_approved',
      action: 'set',
      value: 3,
    };
    expect(
      (
        await app.inject({
          method: 'POST',
          url,
          payload: { subject: 'usr_forged', window: '2026-11', updates: [update] },
        })
      ).statusCode,
    ).toBe(200);
    const [stored] = await db
      .select()
      .from(guardrailCounters)
      .where(
        and(eq(guardrailCounters.workspaceId, workspaceId), eq(guardrailCounters.key, update.key)),
      );
    expect(stored).toMatchObject({ subject: '*', window: '2026-11', valueNum: 3 });
    expect(
      (
        await app.inject({
          method: 'POST',
          url,
          payload: {
            subject: '*',
            window: '2026-11',
            updates: [{ ...update, scope: 'subject_window' }],
          },
        })
      ).statusCode,
    ).toBe(400);
    gates.monthlyBudgetsEnabled = false;
    expect(
      (await app.inject({ method: 'POST', url, payload: { updates: [update] } })).statusCode,
    ).toBe(400);
  });
});
