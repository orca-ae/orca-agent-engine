// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  costRule,
  expectToolResult,
  request,
  SpendScenario,
  writeReply,
  type Rule,
  type SpendEvent,
  type SpendSession,
} from './spend-control-helpers.js';
import type { ScriptedReply } from './scripted-messages-helpers.js';
import { SpendStack } from './spend-stack-helpers.js';

// Real native SDK Agent dispatch, public approvals, Registry pricing/state, and
// child MCP file effects. Only the Messages HTTP responses are scripted.
describe('native subagent spend control through the real stack', () => {
  const stack = new SpendStack();
  const scenarios: SpendScenario[] = [];
  beforeAll(async () => {
    await stack.start();
    console.info(`Subagent spend diagnostics: ${stack.logDirectory}`);
  });
  afterEach(async () => {
    for (const scenario of scenarios.splice(0)) await scenario.close();
    stack.model.assertHealthy();
    stack.model.reset();
  });
  afterAll(async () => {
    await stack.close();
  });
  const createScenario = async () => {
    const scenario = await SpendScenario.create(stack);
    scenarios.push(scenario);
    await scenario.price();
    return scenario;
  };

  it('denies native Agent before any child model request', async () => {
    const scenario = await createScenario();
    const { session, workers } = await coordinator(scenario, {
      parentRule: {
        kind: 'builtin',
        builtin: 'block_tools',
        params: { tools: ['Agent'], reason: 'dispatch-denied' },
      },
    });
    stack.model.script(scenario.model, [delegate(workers[0]!.name)]);
    stack.model.script(workers[0]!.model, [writeReply('dispatch-denied.txt')]);
    expectToolResult(await session.turn(), true, 'dispatch-denied');
    expect(childRequests(stack, workers[0]!.model)).toHaveLength(0);
    expect(childUsage(stack, session, workers[0]!.id)).toHaveLength(0);
    await session.files(0);
    await session.absent('dispatch-denied.txt');
  });

  it.each(['allow', 'deny'] as const)(
    'keeps native Agent unlaunched before public approval %s',
    async (decision) => {
      const scenario = await createScenario();
      const { session, workers } = await coordinator(scenario, {
        parentRule: {
          kind: 'builtin',
          builtin: 'require_approval_for_tools',
          params: { tools: ['Agent'] },
        },
      });
      const worker = workers[0]!;
      const filename = `dispatch-${decision}.txt`;
      stack.model.script(scenario.model, [delegate(worker.name)]);
      stack.model.script(worker.model, [writeReply(filename)]);
      const pending = await session.turn('requires_action');
      expect(childRequests(stack, worker.model)).toHaveLength(0);
      expect(childUsage(stack, session, worker.id)).toHaveLength(0);
      await session.files(0);
      await session.absent(filename);
      const dispatchId = pending.find(
        (event) => event.type === 'agent.tool_use' && event.name === 'Agent',
      )?.id;
      expect(dispatchId).toBeDefined();
      const completed = await session.confirm(pending, decision);
      expectToolResult(
        completed.filter((event) => event.tool_use_id === dispatchId),
        decision === 'deny',
      );
      expect(childRequests(stack, worker.model)).toHaveLength(decision === 'allow' ? 2 : 0);
      await session.files(decision === 'allow' ? 1 : 0);
      if (decision === 'allow') {
        await session.output(filename, `effect-${filename}`);
        expect(childUsage(stack, session, worker.id)).toHaveLength(1);
      } else {
        await session.absent(filename);
        expect(childUsage(stack, session, worker.id)).toHaveLength(0);
      }
    },
  );

  it('charges the proposing native Agent response before rejecting its child launch', async () => {
    const scenario = await createScenario();
    const { session, workers } = await coordinator(scenario, {
      parentRule: costRule({ max_cost_usd: 0.0015 }),
    });
    const worker = workers[0]!;
    // Input alone is $0.001; the final 500 output tokens cross the cap at $0.002.
    stack.model.script(scenario.model, [{ ...delegate(worker.name), input: 1000, output: 500 }]);
    stack.model.script(worker.model, [writeReply('same-call-child.txt')]);
    expectToolResult(await session.turn(), true, 'budget');
    expect(session.costs()).toEqual([0.002]);
    await session.tokens(1000, 500);
    expect(childRequests(stack, worker.model)).toHaveLength(0);
    expect(childUsage(stack, session, worker.id)).toHaveLength(0);
    await session.files(0);
    await session.absent('same-call-child.txt');
  });

  it('applies the coordinator write prohibition to an actual native child', async () => {
    const scenario = await createScenario();
    const { session, workers } = await coordinator(scenario, {
      parentRule: {
        kind: 'builtin',
        builtin: 'block_tools',
        params: { tools: ['mcp__orca__write'], reason: 'coordinator-write-denied' },
      },
    });
    const worker = workers[0]!;
    stack.model.script(scenario.model, [delegate(worker.name)]);
    stack.model.script(worker.model, [writeReply('coordinator-blocked-child.txt')]);
    await session.turn();
    expect(childRequests(stack, worker.model)).toHaveLength(2);
    const results = await childResults(session, worker.id);
    expect(results).toHaveLength(1);
    expect(results[0]!.is_error).toBe(true);
    expect(JSON.stringify(results)).toContain('coordinator-write-denied');
    expect(childUsage(stack, session, worker.id)).toHaveLength(1);
    await session.files(0);
    await session.absent('coordinator-blocked-child.txt');
  });

  it('shares repeated Agent A spend while Agent B with the same rule stays independent', async () => {
    const scenario = await createScenario();
    const {
      session,
      workers: [a, b],
    } = await coordinator(scenario, {
      workers: 2,
      childRule: subagentCost({ max_cost_usd: 0.003 }),
    });
    stack.model.script(scenario.model, [delegate(a!.name), delegate(a!.name), delegate(b!.name)]);
    stack.model.script(a!.model, [writeReply('a-first.txt'), writeReply('a-repeated.txt')]);
    stack.model.script(b!.model, [writeReply('b-independent.txt')]);
    await session.turn();
    await session.files(1);
    await session.output('a-first.txt', 'effect-a-first.txt');
    await session.turn();
    await session.files(1);
    await session.absent('a-repeated.txt');
    await session.turn();
    await session.files(2);
    await session.output('b-independent.txt', 'effect-b-independent.txt');
    expect((await childResults(session, a!.id)).map((event) => event.is_error)).toEqual([
      false,
      true,
    ]);
    expect((await childResults(session, b!.id)).map((event) => event.is_error)).toEqual([false]);
    expect(childRequests(stack, a!.model)).toHaveLength(4);
    expect(childRequests(stack, b!.model)).toHaveLength(2);
    const aReports = childUsage(stack, session, a!.id);
    expect(aReports).toHaveLength(2);
    expect(aReports.map((report) => report.response.guardrail_usage_state)).toEqual([
      expect.objectContaining({ [`subagent_cost_${a!.id}`]: 0.002 }),
      expect.objectContaining({ [`subagent_cost_${a!.id}`]: 0.004 }),
    ]);
    expect(childUsage(stack, session, b!.id)).toHaveLength(1);
    expect(childUsage(stack, session, b!.id)[0]!.response.guardrail_usage_state).toMatchObject({
      [`subagent_cost_${b!.id}`]: 0.002,
    });
    await session.tokens(3000, 1500);
  }, 120_000);

  it('retains Agent A threshold approval without approving Agent B under the same rule', async () => {
    const scenario = await createScenario();
    const {
      session,
      workers: [a, b],
    } = await coordinator(scenario, {
      workers: 2,
      childRule: subagentCost({ max_cost_usd: 0.02, ask_thresholds_usd: [0.001] }),
    });
    stack.model.script(scenario.model, [delegate(a!.name), delegate(a!.name), delegate(b!.name)]);
    stack.model.script(a!.model, [
      writeReply('approved-a.txt'),
      writeReply('approved-a-again.txt'),
    ]);
    stack.model.script(b!.model, [writeReply('unapproved-b.txt')]);
    const aPending = await session.turn('requires_action');
    await session.files(0);
    await session.absent('approved-a.txt');
    expect(childRequests(stack, a!.model)).toHaveLength(1);
    expect(childRequests(stack, b!.model)).toHaveLength(0);
    await session.confirm(aPending, 'allow');
    await session.files(1);
    await session.output('approved-a.txt', 'effect-approved-a.txt');
    const repeated = await session.turn();
    expect(repeated.some((event) => event.stop_reason?.type === 'requires_action')).toBe(false);
    await session.files(2);
    await session.output('approved-a-again.txt', 'effect-approved-a-again.txt');
    const bPending = await session.turn('requires_action');
    await session.files(2);
    await session.absent('unapproved-b.txt');
    expect(childRequests(stack, b!.model)).toHaveLength(1);
    await session.confirm(bPending, 'deny');
    await session.files(2);
    await session.absent('unapproved-b.txt');
    expect((await childResults(session, a!.id)).map((event) => event.is_error)).toEqual([
      false,
      false,
    ]);
    expect((await childResults(session, b!.id)).map((event) => event.is_error)).toEqual([true]);
    expect(childUsage(stack, session, a!.id)).toHaveLength(2);
    expect(childUsage(stack, session, b!.id)).toHaveLength(1);
  }, 120_000);

  it('keeps unpriced Agent A usage from poisoning priced Agent B under the same rule', async () => {
    const scenario = await createScenario();
    const {
      session,
      workers: [a, b],
    } = await coordinator(scenario, {
      workers: 2,
      unpricedFirst: true,
      childRule: subagentCost({ max_cost_usd: 0.01, on_unpriced: 'deny' }),
    });
    stack.model.script(scenario.model, [delegate(a!.name), delegate(b!.name)]);
    stack.model.script(a!.model, [writeReply('unknown-a.txt')]);
    stack.model.script(b!.model, [writeReply('priced-b.txt')]);
    await session.turn();
    await session.files(0);
    await session.absent('unknown-a.txt');
    await session.turn();
    await session.files(1);
    await session.output('priced-b.txt', 'effect-priced-b.txt');
    expect((await childResults(session, a!.id)).map((event) => event.is_error)).toEqual([true]);
    expect((await childResults(session, b!.id)).map((event) => event.is_error)).toEqual([false]);
    expect(childUsage(stack, session, a!.id)).toHaveLength(1);
    expect(childUsage(stack, session, b!.id)).toHaveLength(1);
    // Each ACK includes the reporting child plus shared totals, not every child.
    expect(childUsage(stack, session, a!.id)[0]!.response.guardrail_usage_state).toMatchObject({
      [`subagent_usage_has_unpriced_${a!.id}`]: true,
    });
    const state = childUsage(stack, session, b!.id)[0]!.response.guardrail_usage_state;
    expect(state).toMatchObject({
      session_usage_has_unpriced: true,
      [`subagent_cost_${b!.id}`]: 0.002,
    });
    expect(state).not.toHaveProperty(`subagent_usage_has_unpriced_${b!.id}`);
    await session.tokens(2000, 1000);
  }, 120_000);
});

const subagentCost = (params: Record<string, unknown>): Rule => ({
  kind: 'builtin',
  builtin: 'subagent_cost_budget',
  params,
});

function delegate(name: string): ScriptedReply {
  return {
    input: 0,
    output: 0,
    tool: {
      name: 'Agent',
      input: {
        description: 'Run child fixture',
        subagent_type: name,
        prompt: 'Perform the next fixture action.',
        run_in_background: false,
      },
    },
  };
}

async function coordinator(
  scenario: SpendScenario,
  options: { parentRule?: Rule; childRule?: Rule; workers?: number; unpricedFirst?: boolean },
) {
  const childRuleId = options.childRule ? await scenario.rule(options.childRule) : undefined;
  const workers = [];
  for (let index = 0; index < (options.workers ?? 1); index++) {
    const name = `worker-${index}`;
    // Distinct roots prevent family price fallback from pricing the unknown worker.
    const model = ['claude-opus-4-6', 'claude-haiku-4-5'][index]!;
    if (index !== 0 || !options.unpricedFirst) await scenario.price(model);
    const worker = await request(scenario.cfg, '/v1/agents', {
      name,
      model: { provider: 'anthropic', id: model },
      system: `Fixture worker ${index}. Execute the requested file action.`,
      tools: [{ type: 'agent_toolset_20260401' }],
      ...(childRuleId ? { guardrail_ids: [childRuleId] } : {}),
    });
    workers.push({ id: worker.id, name, model });
  }
  const parentRuleId = options.parentRule ? await scenario.rule(options.parentRule) : undefined;
  const parent = await request(scenario.cfg, '/v1/agents', {
    name: 'spend coordinator',
    model: { provider: 'anthropic', id: scenario.model },
    system: 'Fixture coordinator. Delegate to the requested worker.',
    tools: [{ type: 'agent_toolset_20260401' }],
    multiagent: { type: 'coordinator', agents: workers.map((worker) => worker.id) },
    ...(parentRuleId ? { guardrail_ids: [parentRuleId] } : {}),
  });
  return { session: await scenario.session({ agentId: parent.id }), workers };
}

function childRequests(stack: SpendStack, model: string) {
  return stack.model.exchanges.filter((exchange) => exchange.body.model === model);
}

function childUsage(stack: SpendStack, session: SpendSession, agentId: string) {
  return stack.usageReports(session.id).filter((report) => report.body['subagent_id'] === agentId);
}

async function childResults(session: SpendSession, agentId: string): Promise<SpendEvent[]> {
  // Child tools are canonical events in the public session stream. The child
  // thread instead contains transcript projections, not agent.tool_result.
  const models = new Set(
    childUsage(session.stack, session, agentId).map((report) => report.body.model),
  );
  const paths = new Set(
    session.stack.model.exchanges
      .filter((exchange) => models.has(exchange.body.model))
      .flatMap((exchange) =>
        exchange.reply.tool?.name === 'mcp__orca__write' ? [exchange.reply.tool.input.path] : [],
      ),
  );
  const events = await session.events();
  const ids = new Set(
    events
      .filter(
        (event) =>
          event.type === 'agent.tool_use' &&
          event.name === 'mcp__orca__write' &&
          paths.has((event.input as { path?: string })?.path),
      )
      .map((event) => event.id),
  );
  return events.filter(
    (event) =>
      ['agent.tool_result', 'agent.mcp_tool_result'].includes(event.type) &&
      ids.has(event.tool_use_id as string),
  );
}
