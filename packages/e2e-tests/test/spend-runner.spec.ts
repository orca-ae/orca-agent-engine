// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Production EnvironmentWorker + SessionRunner: stateful budgets fail closed. */
import { readFile } from 'node:fs/promises';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { buildClientFromConfig, type OrcaClientConfig } from '../src/client.js';
import { provisionWorkspace } from './environment-helpers.js';
import { seedIsolatedOrganizationAdmin } from './organization-admin-helpers.js';
import { costRule, eventually, request, type SpendEvent } from './spend-control-helpers.js';
import { SpendStack } from './spend-stack-helpers.js';

describe('deterministic spend: self-hosted runner capability gate', () => {
  const stack = new SpendStack();
  let cfg: OrcaClientConfig;

  beforeAll(async () => {
    await stack.start({ harness: false });
    const organization = await seedIsolatedOrganizationAdmin('spend-runner', {}, stack.databaseUrl);
    const admin = buildClientFromConfig({
      baseURL: stack.adminBaseURL,
      apiKey: organization.apiKey,
    });
    const workspace = await provisionWorkspace(admin, 'spend-runner');
    cfg = buildClientFromConfig({ baseURL: stack.publicBaseURL, apiKey: workspace.cfg.apiKey });
  });

  // The stack owns all resource databases, worker/runner process groups, and
  // temporary files. close() retains logs and also runs on assertion failure.
  afterAll(async () => stack.close());

  it('rejects a stateful request rule with 422, then runs a stateless rule in the same Session', async () => {
    const environment = await request<{ id: string; env_key: string }>(cfg, '/v1/environments', {
      name: 'spend-runner',
      target: 'self_hosted',
      egress_mode: 'sidecar',
    });
    expect(environment.env_key).toBeTruthy();
    const logPath = await stack.startWorker(environment.id, environment.env_key);
    await eventually(
      () =>
        request<{ worker_connected: boolean }>(
          cfg,
          `/v1/environments/${environment.id}/work_stats`,
        ),
      (stats) => stats.worker_connected,
      'owned environment worker connection',
    );
    const guardrail = await request(cfg, '/apis/policy.runorca.ai/v1/guardrails', {
      name: 'unsupported stateful budget',
      scope: 'explicit',
      phases: ['request'],
      rule: costRule({ max_cost_usd: 1 }),
    });
    const agent = await request(cfg, '/v1/agents', {
      name: 'self-hosted budget probe',
      model: { provider: 'mock', id: 'mock-1' },
      system: 'Mock positive control.',
      metadata: { harness: 'mock', mode: 'colocated' },
      guardrail_ids: [guardrail.id],
    });
    const session = await request(cfg, '/v1/sessions', {
      environment_id: environment.id,
      agent_id: agent.id,
    });
    const events = async () =>
      (await request<{ data: SpendEvent[] }>(cfg, `/v1/sessions/${session.id}/events?limit=1000`))
        .data;
    const message = (text: string) =>
      request<{ events: SpendEvent[] }>(cfg, `/v1/sessions/${session.id}/events`, {
        events: [{ type: 'user.message', content: [{ type: 'text', text }] }],
      });
    // Reject the initial snapshot before posting a message so startup validation
    // cannot be mistaken for refusal of that message's turn.
    const log = await eventually(
      () =>
        readFile(logPath, 'utf8').catch((error: NodeJS.ErrnoException) => {
          if (error.code === 'ENOENT') return '';
          throw error;
        }),
      (text) =>
        text.includes('stateful rules require durable Registry write-through') &&
        text.includes('status: 422'),
      `runner stateful refusal; logs: ${stack.logDirectory}`,
    );
    expect(log).toContain('GuardrailUnsupportedError');
    const blocked = await message('blocked stateful request');
    expect(blocked.events).toHaveLength(1);
    // The public POST accepts the event, but the bridge must refuse this exact
    // turn before we replace the rule. There is no public session.error for it.
    await eventually(
      () => readFile(stack.registryLogPath, 'utf8'),
      (text) =>
        text
          .split('\n')
          .slice(0, -1)
          .some((line) => {
            if (!line.startsWith('{')) return false;
            const entry = JSON.parse(line) as {
              msg?: string;
              sessionId?: string;
              userEventId?: string;
            };
            return (
              entry.msg === 'session event bridge refused turn because runner preparation failed' &&
              entry.sessionId === session.id &&
              entry.userEventId === blocked.events[0]!.id
            );
          }),
      `bridge refusal for ${blocked.events[0]!.id}; logs: ${stack.logDirectory}`,
    );
    expect((await events()).filter((event) => event.type === 'agent.message')).toEqual([]);

    await request(cfg, `/apis/policy.runorca.ai/v1/guardrails/${guardrail.id}`, {
      rule: { kind: 'builtin', builtin: 'deny_pii_in_llm_request', params: { pii_types: ['ssn'] } },
    });
    await message('allowed stateless control');
    const after = await eventually(
      events,
      (items) =>
        items.some(
          (event) =>
            event.type === 'agent.message' &&
            JSON.stringify(event.content).includes('mock: allowed stateless control'),
        ),
      `runner positive control; logs: ${stack.logDirectory}`,
    );
    const replies = after.filter((event) => event.type === 'agent.message');
    expect(JSON.stringify(replies)).not.toContain('blocked stateful request');
    expect(stack.model.exchanges).toEqual([]);
    await request(cfg, `/v1/sessions/${session.id}`, {}, 'DELETE');
  });
});
