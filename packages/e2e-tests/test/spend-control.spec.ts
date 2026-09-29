// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { SpendStack } from './spend-stack-helpers.js';
import {
  costRule,
  eventually,
  expectToolResult,
  request,
  SpendScenario,
  writeReply,
  type Tier,
} from './spend-control-helpers.js';

// Deterministic full-stack coverage: real Registry, Harness, pinned Claude SDK,
// Postgres and S3; only Messages responses and selected internal HTTP faults are fixtures.
describe('spend control through the real stack', () => {
  const stack = new SpendStack();
  beforeAll(async () => {
    await stack.start();
    console.info(`Spend stack diagnostics: ${stack.logDirectory}`);
  });
  const scenarios: SpendScenario[] = [];
  const createScenario = async () => {
    const scenario = await SpendScenario.create(stack);
    scenarios.push(scenario);
    return scenario;
  };
  afterEach(async () => {
    for (const scenario of scenarios.splice(0)) await scenario.close();
    stack.model.assertHealthy();
    stack.model.reset();
  });
  afterAll(async () => {
    await stack.close();
  });

  const tiers: Tier[] = ['session', 'agent', 'workspace', 'organization'];
  it.each(tiers)('%s denies the real write tool with its transcript reason', async (tier) => {
    const scenario = await createScenario();
    const reason = `denied-by-${tier}`;
    const ruleId = await scenario.rule(
      { kind: 'builtin', builtin: 'block_tools', params: { tools: ['mcp__orca__write'], reason } },
      tier,
    );
    const session = await scenario.session({
      ruleId: tier === 'session' || tier === 'agent' ? ruleId : undefined,
      tier,
    });
    stack.model.script(scenario.model, [writeReply(`${tier}.txt`)]);
    expectToolResult(await session.turn(), true, reason);
    await session.files(0);
    await session.absent(`${tier}.txt`);
  });

  for (const decision of ['allow', 'deny'] as const) {
    it.each(tiers)(`%s approval ${decision} gates the actual file effect`, async (tier) => {
      const scenario = await createScenario();
      const ruleId = await scenario.rule(
        {
          kind: 'builtin',
          builtin: 'require_approval_for_tools',
          params: { tools: ['mcp__orca__write'] },
        },
        tier,
      );
      const session = await scenario.session({
        ruleId: tier === 'session' || tier === 'agent' ? ruleId : undefined,
        tier,
      });
      const filename = `${tier}-${decision}.txt`;
      stack.model.script(scenario.model, [writeReply(filename)]);
      const pending = await session.turn('requires_action');
      await session.files(0);
      await session.absent(filename);
      expectToolResult(await session.confirm(pending, decision), decision === 'deny');
      await session.files(decision === 'allow' ? 1 : 0);
      if (decision === 'allow') await session.output(filename, `effect-${filename}`);
      else await session.absent(filename);
    });
  }

  it.each(['allow', 'ask'] as const)(
    'organization deny overrides lower %s and forged confirmation',
    async (lower) => {
      const scenario = await createScenario();
      await scenario.rule(
        {
          kind: 'builtin',
          builtin: 'block_tools',
          params: { tools: ['mcp__orca__write'], reason: 'organization-wins' },
        },
        'organization',
      );
      const ruleId = await scenario.rule(
        lower === 'ask'
          ? {
              kind: 'builtin',
              builtin: 'require_approval_for_tools',
              params: { tools: ['mcp__orca__write'] },
            }
          : { kind: 'builtin', builtin: 'block_tools', params: { tools: ['mcp__orca__read'] } },
        'session',
      );
      const session = await scenario.session({ ruleId, tier: 'session' });
      stack.model.script(scenario.model, [writeReply('forged.txt')]);
      const events = await session.turn();
      expectToolResult(events, true, 'organization-wins');
      expect(events.some((event) => event.stop_reason?.type === 'requires_action')).toBe(false);
      const tool = events.find((event) => event.type === 'agent.tool_use');
      expect(tool).toBeDefined();
      const count = stack.model.exchanges.length;
      await session.send([
        { type: 'user.tool_confirmation', tool_use_id: tool!.id, result: 'allow' },
      ]);
      const rejected = await session.wait('any_error');
      expect(rejected.find((event) => event.type === 'session.error')?.error?.type).toBe(
        'unapplied_event',
      );
      expect(stack.model.exchanges).toHaveLength(count);
      await session.absent('forged.txt');
      await session.files(0);
      expect(
        (await session.events()).filter((event) =>
          ['agent.tool_result', 'agent.mcp_tool_result'].includes(event.type),
        ),
      ).toHaveLength(1);
    },
  );

  it.each(['tool', 'text'] as const)(
    'charges the crossing %s response and denies the next model request',
    async (kind) => {
      const scenario = await createScenario();
      await scenario.price();
      const session = await scenario.session({
        // Prior spend plus current input is $0.003; final output must arrive to cross this cap.
        ruleId: await scenario.rule(costRule({ max_cost_usd: 0.0035 })),
      });
      stack.model.script(scenario.model, [
        { text: 'first charged text' },
        kind === 'tool' ? writeReply('over-budget.txt') : { text: 'crossing text' },
      ]);
      await session.turn();
      const second = await session.turn();
      if (kind === 'tool') expectToolResult(second, true, 'budget');
      await session.files(0);
      await session.absent('over-budget.txt');
      expect(session.costs()).toEqual([0.002, 0.004]);
      await session.tokens(2000, 1000);
      const count = stack.model.exchanges.length;
      await session.turn('policy_denied');
      expect(stack.model.exchanges).toHaveLength(count);
      await session.files(0);
    },
  );

  it.each([false, true])(
    'deduplicates a committed usage report when its ACK is lost and fails closed (wait for failure: %s)',
    async (waitForFailure) => {
      const scenario = await createScenario();
      await scenario.price();
      const session = await scenario.session({
        ruleId: await scenario.rule(costRule({ max_cost_usd: 5 })),
      });
      stack.model.script(scenario.model, [writeReply('lost-ack.txt')]);
      stack.dropNextUsageResponse(session.id);
      await session.message();
      await eventually(
        async () => stack.usageReports(session.id),
        (reports) => reports.some((report) => report.dropped),
        'committed ACK loss',
      );
      await session.files(0);
      await session.absent('lost-ack.txt');
      if (waitForFailure) await session.wait('guardrail_usage_unavailable');
      // Interrupt the same turn without resetting its event cursor: the usage
      // failure may already be visible before cancellation flushes pending usage.
      await request(session.cfg, `/v1/sessions/${session.id}/events`, {
        events: [{ type: 'user.interrupt' }],
      });
      await session.wait('guardrail_usage_unavailable');
      const reports = await eventually(
        async () => stack.usageReports(session.id),
        (value) => value.length === 2,
        'usage replay',
      );
      expect(reports[0]!.body.usage_event_id).toBeTruthy();
      expect(reports[1]!.body).toEqual(reports[0]!.body);
      for (const report of reports) {
        expect(report.status).toBe(200);
        expect(report.response.guardrail_usage_state).toMatchObject({
          session_cost_usd: 0.002,
          total_tokens: 1500,
        });
        expect(report.response.guardrail_subject_window_state.daily_cost_usd).toBe(0.002);
      }
      await session.tokens(1000, 500);
      await session.files(0);
    },
  );

  it('refreshes a warm session daily budget by authenticated key while another key stays independent', async () => {
    const scenario = await createScenario();
    await scenario.price();
    await scenario.rule(
      { kind: 'builtin', builtin: 'user_daily_cost_budget', params: { max_cost_usd: 0.003 } },
      'workspace',
    );
    const s1 = await scenario.session();
    const s2 = await scenario.session({ agentId: s1.agentId });
    stack.model.script(scenario.model, [
      { text: 'warm S2', input: 0, output: 0 },
      { text: 'spend one' },
      { text: 'spend two' },
      { text: 'different key' },
    ]);
    await s2.turn();
    await s1.turn();
    await s1.turn();
    expect(
      stack
        .usageReports(s1.id)
        .map((report) => report.response.guardrail_subject_window_state.daily_cost_usd),
    ).toEqual([0.002, 0.004]);
    const count = stack.model.exchanges.length;
    await s2.turn('policy_denied');
    expect(stack.model.exchanges).toHaveLength(count);
    const s3 = await scenario.session({ agentId: s1.agentId, cfg: await scenario.anotherKey() });
    await s3.turn();
    expect(
      stack.usageReports(s3.id)[0]!.response.guardrail_subject_window_state.daily_cost_usd,
    ).toBe(0.002);
    await s1.tokens(2000, 1000);
    await s2.tokens(0, 0);
    await s3.tokens(1000, 500);
  });

  it('persists approved soft thresholds across Harness restart and re-asks denied thresholds', async () => {
    const scenario = await createScenario();
    await scenario.price();
    const session = await scenario.session({
      ruleId: await scenario.rule(
        costRule({ max_cost_usd: 0.02, ask_thresholds_usd: [0.001, 0.005] }),
      ),
    });
    const steps = [
      { input: 1000, output: 500, decision: 'deny', files: 0 },
      { input: 1000, output: 500, decision: 'allow', files: 1 },
      { input: 0, output: 0, decision: undefined, files: 2 },
      { input: 1000, output: 500, decision: 'deny', files: 2 },
      { input: 0, output: 0, decision: 'allow', files: 3 },
    ] as const;
    let expectedInput = 0;
    let expectedOutput = 0;
    for (const [index, step] of steps.entries()) {
      const filename = `threshold-${index}.txt`;
      stack.model.script(scenario.model, [
        writeReply(filename, `effect-${index}`, { input: step.input, output: step.output }),
      ]);
      const events = await session.turn(step.decision ? 'requires_action' : 'end_turn');
      if (step.decision) {
        await session.files(index === 0 ? 0 : steps[index - 1]!.files);
        await session.absent(filename);
        expectToolResult(await session.confirm(events, step.decision), step.decision === 'deny');
      } else {
        expect(events.some((event) => event.stop_reason?.type === 'requires_action')).toBe(false);
        expectToolResult(events, false);
      }
      await session.files(step.files);
      if (step.decision !== 'deny') await session.output(filename, `effect-${index}`);
      else await session.absent(filename);
      expectedInput += step.input;
      expectedOutput += step.output;
      await session.tokens(expectedInput, expectedOutput);
      if (index === 1) await stack.restartHarness();
    }
    expect(session.costs()).toEqual([0.002, 0.004, 0.004, 0.006, 0.006]);
  }, 120000);

  it.each(['ask', 'deny', 'allow'] as const)(
    'applies on_unpriced=%s to the real write tool',
    async (policy) => {
      const scenario = await createScenario();
      const session = await scenario.session({
        ruleId: await scenario.rule(costRule({ max_cost_usd: 1, on_unpriced: policy })),
      });
      const filename = `unpriced-${policy}.txt`;
      stack.model.script(scenario.model, [writeReply(filename)]);
      let events = await session.turn(policy === 'ask' ? 'requires_action' : 'end_turn');
      if (policy === 'ask') {
        await session.files(0);
        await session.absent(filename);
        events = await session.confirm(events, 'allow');
      }
      expectToolResult(events, policy === 'deny');
      await session.files(policy === 'deny' ? 0 : 1);
      if (policy !== 'deny') await session.output(filename, `effect-${filename}`);
      else await session.absent(filename);
      const state = stack.usageReports(session.id)[0]!.response.guardrail_usage_state;
      expect(state.session_usage_has_unpriced).toBe(true);
      expect(state).not.toHaveProperty('session_cost_usd');
      await session.tokens(1000, 500);
    },
  );

  it('retains the unpriced flag after priced, unknown, and later known model usage', async () => {
    const scenario = await createScenario();
    await scenario.price();
    const unknown = 'unpriced-provider-model';
    const session = await scenario.session({
      ruleId: await scenario.rule(costRule({ max_cost_usd: 1, on_unpriced: 'allow' })),
    });
    stack.model.script(scenario.model, [
      { text: 'known first' },
      { text: 'unknown actual model', actualModel: unknown },
      { text: 'known again' },
    ]);
    await session.turn();
    await session.turn();
    await session.turn();
    const reports = stack.usageReports(session.id);
    expect(reports.map((report) => report.body.model)).toEqual([
      scenario.model,
      unknown,
      scenario.model,
    ]);
    // These are known subtotals; the sticky flag prevents treating them as the full cost.
    expect(reports.map((report) => report.response.guardrail_usage_state.session_cost_usd)).toEqual(
      [0.002, 0.002, 0.004],
    );
    for (const report of reports.slice(1)) {
      expect(report.response.guardrail_usage_state.session_usage_has_unpriced).toBe(true);
    }
    await session.tokens(3000, 1500);
  });

  it('allows the first unknown text usage and denies another unapproved request after the default ask becomes pending', async () => {
    const scenario = await createScenario();
    const session = await scenario.session({
      ruleId: await scenario.rule(costRule({ max_cost_usd: 1 })),
    });
    stack.model.script(scenario.model, [
      { text: 'first unknown text' },
      { text: 'pending request allowed once', input: 0, output: 0 },
    ]);
    await session.turn();
    await session.turn();
    expect(
      stack.model.exchanges.filter((exchange) => exchange.body.model === scenario.model),
    ).toHaveLength(2);
    const count = stack.model.exchanges.length;
    await session.turn('policy_denied');
    expect(stack.model.exchanges).toHaveLength(count);
    await session.files(0);
    await session.tokens(1000, 500);
  });

  it('refreshes rule edits next turn and fails closed while durable state cannot be restored', async () => {
    const scenario = await createScenario();
    await scenario.price();
    const ruleId = await scenario.rule(costRule({ max_cost_usd: 0.01 }));
    const session = await scenario.session({ ruleId });
    stack.model.script(scenario.model, [{ text: 'durable spend' }]);
    await session.turn();
    expect(session.costs()).toEqual([0.002]);
    await request(scenario.cfg, `/apis/policy.runorca.ai/v1/guardrails/${ruleId}`, {
      rule: costRule({ max_cost_usd: 0.001 }),
    });
    const count = stack.model.exchanges.length;
    await session.turn('policy_denied');
    expect(stack.model.exchanges).toHaveLength(count);
    await stack.restartHarness();
    stack.failRuntimePreparation(session.id);
    try {
      await session.message();
      await eventually(
        async () => stack.prepareFailures.filter((path) => path.includes(session.id)),
        (failures) => failures.length >= 2,
        'runtime preparation retries',
      );
      expect(stack.model.exchanges).toHaveLength(count);
      await session.files(0);
    } finally {
      stack.failRuntimePreparation();
    }
    // The same accepted user event retries after recovery using its durable spend.
    await session.wait('policy_denied');
    expect(stack.model.exchanges).toHaveLength(count);
    await session.tokens(1000, 500);
    // A larger current cap now permits a real tool, proving the recovered runtime is usable.
    await request(scenario.cfg, `/apis/policy.runorca.ai/v1/guardrails/${ruleId}`, {
      rule: costRule({ max_cost_usd: 0.01 }),
    });
    stack.model.script(scenario.model, [writeReply('restored.txt')]);
    expectToolResult(await session.turn(), false);
    await session.files(1);
    await session.output('restored.txt', 'effect-restored.txt');
    expect(session.costs()).toEqual([0.002, 0.004]);
  }, 120000);

  it('preserves historical prices and permits an explicit cheaper model after the expensive model is blocked', async () => {
    const scenario = await createScenario();
    const low = 'claude-haiku-4-5';
    await scenario.price();
    await scenario.price(low, 0.1, 0.2);
    const session = await scenario.session({
      ruleId: await scenario.rule(
        costRule({ max_cost_usd: 0.005, expensive_models: [scenario.model] }),
      ),
    });
    stack.model.script(scenario.model, [{ text: 'original price' }, writeReply('expensive.txt')]);
    await session.turn();
    await request(
      scenario.admin,
      `/v1/organizations/modelprices/${encodeURIComponent(scenario.model)}`,
      { input_per_million_tokens: 2, output_per_million_tokens: 4 },
      'PATCH',
    );
    expectToolResult(await session.turn(), true, 'budget');
    await session.files(0);
    await session.absent('expensive.txt');
    expect(session.costs()).toEqual([0.002, 0.006]);
    const count = stack.model.exchanges.length;
    await session.turn('policy_denied');
    expect(stack.model.exchanges).toHaveLength(count);
    await request(scenario.cfg, `/v1/sessions/${session.id}`, {
      agent: { model: { provider: 'anthropic', id: low } },
    });
    stack.model.script(low, [writeReply('cheap.txt')]);
    expectToolResult(await session.turn(), false);
    await session.files(1);
    await session.output('cheap.txt', 'effect-cheap.txt');
    expect(session.costs()).toEqual([0.002, 0.006, 0.0062]);
    await session.tokens(3000, 1500);
  });
});
