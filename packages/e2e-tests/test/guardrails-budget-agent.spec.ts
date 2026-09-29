// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Paid smoke: a priced text turn crosses the cap; the next request never reaches the model. */
import { randomUUID } from 'node:crypto';
import { collectTurnEvents } from './turn-events-helpers.js';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedOrganizationAdminApiKey, seedWorkspaceApiKey } from '../src/seed.js';
import { createTestEnvironment } from './environment-helpers.js';
import { listScopedOutputFiles } from './output-file-helpers.js';
import { costRule, request, type SpendEvent } from './spend-control-helpers.js';
import { COLOCATED_AGENT, REAL_AGENT, requireRealAgentKey } from './real-agent-config.js';

const model =
  process.env['ORCA_E2E_BUDGET_MODEL'] ??
  (REAL_AGENT.isNativeSdk ? REAL_AGENT.model.id : 'claude-sonnet-4-6');
const topologies =
  process.env['ORCA_E2E_SANDBOX_HARNESS'] === '1'
    ? (['separate', 'colocated'] as const)
    : (['separate'] as const);

describe(`Layer B: priced request budget (${REAL_AGENT.harness})`, () => {
  let cfg: OrcaClientConfig;
  const resources: string[] = [];
  let fixturePriceAdmin: OrcaClientConfig | undefined;
  const priceIdentity = `${encodeURIComponent(model)}?provider=${REAL_AGENT.model.provider}`;

  beforeAll(async () => {
    requireRealAgentKey();
    const seeded = await seedWorkspaceApiKey();
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    if (REAL_AGENT.isNativeSdk) {
      const quote = await apiCall(cfg, `/apis/pricing.runorca.ai/v1/modelprices/${priceIdentity}`);
      expect([200, 404], quote.text).toContain(quote.status);
      if (quote.status === 404) {
        // Clean stacks seed Anthropic prices only. These organization-scoped
        // fixture rates exercise accounting; they are not OpenAI's retail prices.
        const admin = await seedOrganizationAdminApiKey();
        const adminCfg = buildClientFromConfig({
          baseURL: process.env['ORCA_ADMIN_BASE_URL'] ?? 'http://localhost:8082',
          apiKey: admin.apiKey,
        });
        const created = await apiCall(adminCfg, '/v1/organizations/modelprices', {
          method: 'POST',
          body: JSON.stringify({
            provider: REAL_AGENT.model.provider,
            model_id: model,
            input_per_million_tokens: 1,
            output_per_million_tokens: 2,
          }),
        });
        // A concurrent creator owns its row. Never replace or clean up that price.
        if (created.status === 201) fixturePriceAdmin = adminCfg;
        expect([201, 409], created.text).toContain(created.status);
      }
    }
  });

  afterAll(async () => {
    if (!fixturePriceAdmin) return;
    const removed = await apiCall(
      fixturePriceAdmin,
      `/v1/organizations/modelprices/${priceIdentity}`,
      { method: 'DELETE' },
    );
    expect(removed.status, removed.text).toBe(200);
  });

  afterEach(async () => {
    const errors: unknown[] = [];
    for (const path of resources.splice(0).reverse()) {
      try {
        await request(cfg, path, {}, 'DELETE');
      } catch (error) {
        errors.push(error);
      }
    }
    if (errors.length) throw new AggregateError(errors, 'Budget smoke cleanup failed');
  });

  it.each(topologies)(
    'denies a second text request before model work in %s mode',
    async (topology) => {
      const price = await request<{
        input_per_million_tokens: number;
        output_per_million_tokens: number;
      }>(cfg, `/apis/pricing.runorca.ai/v1/modelprices/${priceIdentity}`);
      expect(price.input_per_million_tokens).toBeGreaterThan(0);
      expect(price.output_per_million_tokens).toBeGreaterThan(0);
      const environmentId = await createTestEnvironment(cfg, `budget-${topology}`);
      resources.push(`/v1/environments/${environmentId}`);
      const guardrail = await request(cfg, '/apis/policy.runorca.ai/v1/guardrails', {
        name: `budget-smoke-${randomUUID()}`,
        scope: 'explicit',
        phases: ['request'],
        // Stay below one output token at the effective price, including model
        // overrides. Allow unknown pricing so an unpriced denial cannot pass.
        rule: costRule({
          max_cost_usd: Math.min(1e-9, price.output_per_million_tokens / 2_000_000),
          on_unpriced: 'allow',
        }),
      });
      resources.push(`/apis/policy.runorca.ai/v1/guardrails/${guardrail.id}`);
      const agent = await request(cfg, '/v1/agents', {
        name: `budget-smoke-${topology}`,
        model: { ...REAL_AGENT.model, id: model },
        system: 'Return the requested marker as plain text. Do not use tools or write files.',
        tools: [],
        mcp_servers: [],
        skills: [],
        guardrail_ids: [guardrail.id],
        metadata: topology === 'colocated' ? COLOCATED_AGENT.metadata : REAL_AGENT.metadata,
      });
      resources.push(`/v1/agents/${agent.id}`);
      const session = await request(cfg, '/v1/sessions', {
        environment_id: environmentId,
        agent_id: agent.id,
      });
      resources.push(`/v1/sessions/${session.id}`);
      const events = async (): Promise<SpendEvent[]> => {
        const result: SpendEvent[] = [];
        let page: string | null = '';
        while (page !== null) {
          const response: { data: SpendEvent[]; next_page: string | null } = await request(
            cfg,
            `/v1/sessions/${session.id}/events?limit=1000${page ? `&page=${encodeURIComponent(page)}` : ''}`,
          );
          result.push(...response.data);
          page = response.next_page;
        }
        return result;
      };
      const send = (text: string) =>
        request(cfg, `/v1/sessions/${session.id}/events`, {
          events: [{ type: 'user.message', content: [{ type: 'text', text }] }],
        });
      const initial = new Set((await events()).map((event) => event.id));
      const marker = `budget-first-${randomUUID()}`;
      await send(`Reply with exactly this marker: ${marker}`);
      const first = await collectTurnEvents(
        cfg,
        session.id,
        initial,
        (items) =>
          items.some(
            (event) => event.type === 'session.status_idle' || event.type === 'session.error',
          ),
        `first priced ${topology} turn`,
        topology === 'colocated' ? 420000 : 180000,
      );
      expect(
        first.filter((event) => event.type === 'session.error'),
        JSON.stringify(first),
      ).toEqual([]);
      expect(
        first.some(
          (event) =>
            event.type === 'agent.message' && JSON.stringify(event.content).includes(marker),
        ),
      ).toBe(true);
      expect(first.some((event) => event.type === 'span.model_request_start')).toBe(true);
      const before = await request<{ usage: { input_tokens: number; output_tokens: number } }>(
        cfg,
        `/v1/sessions/${session.id}`,
      );
      expect(before.usage.output_tokens).toBeGreaterThan(0);
      expect(await listScopedOutputFiles(cfg, session.id)).toEqual([]);

      // The event-list index can lag the transcript SSE stream. Keep the events
      // already observed in the first turn so replay cannot look like new model work.
      const seen = new Set([...initial, ...(await events()).map((event) => event.id)]);
      await send(`Reply with the second marker: budget-blocked-${randomUUID()}`);
      const fresh = await collectTurnEvents(
        cfg,
        session.id,
        seen,
        (items) =>
          items.some(
            (event) => event.type === 'session.error' && event.error?.type === 'policy_denied',
          ) && items.some((event) => event.type === 'session.status_idle'),
        `next ${topology} request budget denial`,
        45000,
      );
      expect(
        fresh.filter((event) => event.type === 'session.error').map((event) => event.error?.type),
      ).toEqual(['policy_denied']);
      // These spans cover an SDK query, not individual provider requests. No new
      // query or assistant output proves denial at the public request boundary.
      expect(
        fresh.filter((event) => ['span.model_request_start', 'agent.message'].includes(event.type)),
      ).toEqual([]);
      const after = await request<{ usage: { input_tokens: number; output_tokens: number } }>(
        cfg,
        `/v1/sessions/${session.id}`,
      );
      expect(after.usage).toEqual(before.usage);
      expect(await listScopedOutputFiles(cfg, session.id)).toEqual([]);
    },
    { timeout: 540000, retry: Number(process.env['ORCA_E2E_REAL_CLAUDE_RETRY'] ?? '2') },
  );
});
