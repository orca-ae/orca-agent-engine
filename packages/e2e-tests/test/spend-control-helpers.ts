// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect } from 'vitest';
import { apiCall, buildClientFromConfig, type OrcaClientConfig } from '../src/client.js';
import { createTestEnvironment, provisionWorkspace } from './environment-helpers.js';
import { seedIsolatedOrganizationAdmin } from './organization-admin-helpers.js';
import { downloadFileText, listScopedOutputFiles } from './output-file-helpers.js';
import type { ScriptedReply } from './scripted-messages-helpers.js';
import type { SpendStack } from './spend-stack-helpers.js';

export type Tier = 'session' | 'agent' | 'workspace' | 'organization';
export interface Rule {
  kind: 'builtin';
  builtin: string;
  params: Record<string, unknown>;
}
export interface SpendEvent {
  id: string;
  type: string;
  stop_reason?: { type: string; event_ids?: string[] };
  error?: { type: string; message: string };
  is_error?: boolean;
  [key: string]: unknown;
}

export async function request<T = { id: string }>(
  cfg: OrcaClientConfig,
  path: string,
  body?: unknown,
  method = body === undefined ? 'GET' : 'POST',
): Promise<T> {
  const response = await apiCall(cfg, path, {
    method,
    headers: { 'orca-beta': 'guardrails' },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(10000),
  });
  expect(response.status, `${method} ${path}: ${response.text}`).toBeGreaterThanOrEqual(200);
  expect(response.status, `${method} ${path}: ${response.text}`).toBeLessThan(300);
  return response.json<T>();
}

export async function eventually<T>(
  read: () => Promise<T>,
  ready: (value: T) => boolean,
  label: string,
  timeout = 45000,
): Promise<T> {
  const deadline = Date.now() + timeout;
  let last: T | undefined;
  while (Date.now() < deadline) {
    last = await read();
    if (ready(last)) return last;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`${label} timed out; last observation: ${JSON.stringify(last)}`);
}

export const costRule = (params: Record<string, unknown>): Rule => ({
  kind: 'builtin',
  builtin: 'cost_budget',
  params,
});
export const writeReply = (
  filename: string,
  content = `effect-${filename}`,
  usage: Partial<ScriptedReply> = {},
): ScriptedReply => ({
  tool: { name: 'mcp__orca__write', input: { path: `/mnt/session/outputs/${filename}`, content } },
  ...usage,
});

/** Each scenario gets an organization: inherited organization rules cannot leak between cases. */
export class SpendScenario {
  readonly model = 'claude-sonnet-4-6';
  private readonly sessions: SpendSession[] = [];
  private constructor(
    readonly stack: SpendStack,
    readonly admin: OrcaClientConfig,
    readonly cfg: OrcaClientConfig,
    readonly workspaceId: string,
    readonly environmentId: string,
  ) {}

  static async create(stack: SpendStack): Promise<SpendScenario> {
    const organization = await seedIsolatedOrganizationAdmin(
      'spend-control',
      {},
      stack.databaseUrl,
    );
    const admin = buildClientFromConfig({
      baseURL: stack.adminBaseURL,
      apiKey: organization.apiKey,
    });
    const workspace = await provisionWorkspace(admin, 'spend-control');
    const cfg = buildClientFromConfig({
      baseURL: stack.publicBaseURL,
      apiKey: workspace.cfg.apiKey,
    });
    const environment = await createTestEnvironment(cfg, 'spend-control');
    return new SpendScenario(stack, admin, cfg, workspace.id, environment);
  }

  async price(model = this.model, input = 1, output = 2): Promise<void> {
    await request(this.admin, '/v1/organizations/modelprices', {
      model_id: model,
      input_per_million_tokens: input,
      output_per_million_tokens: output,
    });
  }

  async rule(rule: Rule, tier: Tier = 'agent', name = `${tier}-${rule.builtin}`): Promise<string> {
    return (
      await request(
        tier === 'organization' ? this.admin : this.cfg,
        tier === 'organization'
          ? '/v1/organizations/guardrails'
          : '/apis/policy.runorca.ai/v1/guardrails',
        {
          name,
          scope: tier === 'session' || tier === 'agent' ? 'explicit' : tier,
          rule,
        },
      )
    ).id;
  }

  async session(
    options: {
      ruleId?: string | undefined;
      tier?: Tier;
      model?: string;
      cfg?: OrcaClientConfig;
      agentId?: string;
    } = {},
  ): Promise<SpendSession> {
    const cfg = options.cfg ?? this.cfg;
    const agentId =
      options.agentId ??
      (
        await request(cfg, '/v1/agents', {
          name: 'spend fixture',
          model: { provider: 'anthropic', id: options.model ?? this.model },
          system: 'Exercise the requested fixture action.',
          tools: [{ type: 'agent_toolset_20260401' }],
          ...(options.ruleId && options.tier !== 'session'
            ? { guardrail_ids: [options.ruleId] }
            : {}),
        })
      ).id;
    const session = await request(cfg, '/v1/sessions', {
      environment_id: this.environmentId,
      ...(options.ruleId && options.tier === 'session'
        ? { agent: { type: 'agent_with_overrides', id: agentId, guardrail_ids: [options.ruleId] } }
        : { agent_id: agentId }),
    });
    const created = new SpendSession(this.stack, cfg, session.id, agentId);
    this.sessions.push(created);
    return created;
  }

  async close(): Promise<void> {
    for (const session of this.sessions)
      await request(session.cfg, `/v1/sessions/${session.id}`, {}, 'DELETE');
  }

  async anotherKey(): Promise<OrcaClientConfig> {
    const key = await request<{ key: string }>(
      this.admin,
      `/v1/organizations/workspaces/${this.workspaceId}/api_keys`,
      { name: 'independent subject' },
    );
    return buildClientFromConfig({ baseURL: this.stack.publicBaseURL, apiKey: key.key });
  }
}

/** Read fresh event IDs for every public action; earlier idle frames cannot satisfy later turns. */
export class SpendSession {
  private before = new Set<string>();
  constructor(
    readonly stack: SpendStack,
    readonly cfg: OrcaClientConfig,
    readonly id: string,
    readonly agentId: string,
  ) {}

  async events(): Promise<SpendEvent[]> {
    const events: SpendEvent[] = [];
    let page: string | null = '';
    while (page !== null) {
      const result: { data: SpendEvent[]; next_page: string | null } = await request(
        this.cfg,
        `/v1/sessions/${this.id}/events?limit=1000${page ? `&page=${encodeURIComponent(page)}` : ''}`,
      );
      events.push(...result.data);
      page = result.next_page;
    }
    return events;
  }

  async send(events: unknown[]): Promise<void> {
    this.before = new Set((await this.events()).map((event) => event.id));
    await request(this.cfg, `/v1/sessions/${this.id}/events`, { events });
  }

  async message(text = 'Run the next fixture action.'): Promise<void> {
    await this.send([{ type: 'user.message', content: [{ type: 'text', text }] }]);
  }

  async wait(
    type:
      | 'end_turn'
      | 'requires_action'
      | 'policy_denied'
      | 'guardrail_usage_unavailable'
      | 'any_error' = 'end_turn',
  ): Promise<SpendEvent[]> {
    return eventually(
      async () => {
        this.stack.model.assertHealthy();
        const fresh = (await this.events()).filter((event) => !this.before.has(event.id));
        if (type === 'end_turn' || type === 'requires_action') {
          expect(
            fresh.filter((event) => event.type === 'session.error'),
            JSON.stringify(fresh),
          ).toEqual([]);
        }
        return fresh;
      },
      (events) =>
        events.some((event) =>
          type === 'end_turn' || type === 'requires_action'
            ? event.type === 'session.status_idle' && event.stop_reason?.type === type
            : event.type === 'session.error' &&
              (type === 'any_error' || event.error?.type === type),
        ),
      `session ${this.id}: ${type}`,
    );
  }

  async turn(type: Parameters<SpendSession['wait']>[0] = 'end_turn'): Promise<SpendEvent[]> {
    await this.message();
    return this.wait(type);
  }

  async confirm(pending: SpendEvent[], result: 'allow' | 'deny'): Promise<SpendEvent[]> {
    const ids = pending.find(
      (event) =>
        event.type === 'session.status_idle' && event.stop_reason?.type === 'requires_action',
    )?.stop_reason?.event_ids;
    expect(ids).toHaveLength(1);
    await this.send([{ type: 'user.tool_confirmation', tool_use_id: ids![0], result }]);
    return this.wait();
  }

  async files(expected: number): Promise<void> {
    const files = await listScopedOutputFiles(this.cfg, this.id);
    expect(files, `session ${this.id} outputs`).toHaveLength(expected);
  }

  async absent(filename: string): Promise<void> {
    expect(
      await this.stack.localOutputFiles(filename),
      `unexpected physical output ${filename}`,
    ).toEqual([]);
  }

  async output(filename: string, expected: string): Promise<void> {
    expect(await this.stack.localOutputFiles(filename), `physical output ${filename}`).toHaveLength(
      1,
    );
    const files = await listScopedOutputFiles(this.cfg, this.id);
    const file = files.find((entry) => entry.filename === filename);
    expect(file, `missing ${filename}`).toBeDefined();
    expect(await downloadFileText(this.cfg, file!.id)).toBe(expected);
  }

  async tokens(input: number, output: number): Promise<void> {
    const current = await request<{ usage: { input_tokens: number; output_tokens: number } }>(
      this.cfg,
      `/v1/sessions/${this.id}`,
    );
    expect(current.usage.input_tokens).toBe(input);
    expect(current.usage.output_tokens).toBe(output);
  }

  costs(): number[] {
    return this.stack
      .usageReports(this.id)
      .map((report) => report.response.guardrail_usage_state.session_cost_usd!);
  }
}

export function expectToolResult(events: SpendEvent[], error: boolean, reason?: string): void {
  const results = events.filter((event) =>
    ['agent.tool_result', 'agent.mcp_tool_result'].includes(event.type),
  );
  expect(results, JSON.stringify(events)).toHaveLength(1);
  expect(results[0]!.is_error).toBe(error);
  if (reason) expect(JSON.stringify(results[0])).toContain(reason);
}
