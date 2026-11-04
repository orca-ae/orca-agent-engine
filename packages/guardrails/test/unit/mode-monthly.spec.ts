// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { BUILTIN_EVALUATORS } from '../../src/builtins/index.js';
import { compileGuardrailRule } from '../../src/compile.js';
import { evaluateGuardrails, type PreparedGuardrail } from '../../src/engine.js';
import { InMemoryGuardrailStateStore } from '../../src/state.js';
import type { GuardrailEvent, StateScope } from '../../src/types.js';

const EVENT: GuardrailEvent = {
  phase: 'tool_call',
  sessionId: 'ses_one',
  tool: { name: 'Bash', input: { command: 'git status' } },
};

function prepared(
  id: string,
  builtin: string,
  mode: 'observe' | 'enforce',
  params: Record<string, unknown> = {},
): PreparedGuardrail {
  const compiled = compileGuardrailRule({ kind: 'builtin', builtin, params }, 'workspace');
  if (!compiled.ok) throw new Error(JSON.stringify(compiled.errors));
  return {
    tier: 'workspace',
    stateful: compiled.stateful,
    ...(compiled.stateScope ? { stateScope: compiled.stateScope } : {}),
    guardrail: {
      id,
      name: id,
      enabled: true,
      mode,
      phases: ['request', 'tool_call'],
      scope: 'workspace',
      rule: { kind: 'builtin', builtin, params },
    },
  };
}

describe('observe-mode composition', () => {
  it('records a deny without folding it into the enforced decision', () => {
    const rule = prepared('observe', 'block_tools', 'observe', { tools: ['Bash'] });
    const decision = evaluateGuardrails([rule], EVENT, { builtins: BUILTIN_EVALUATORS });
    expect(decision.verdict).toBe('allow');
    expect(decision.reasons).toEqual([]);
    expect(decision.deniedBy).toBeUndefined();
    expect(decision.observed).toEqual([
      expect.objectContaining({ guardrailId: 'observe', mode: 'observe', verdict: 'deny' }),
    ]);
  });

  it('does not turn an observed budget ask into a stored approval', () => {
    const rule = prepared('budget', 'cost_budget', 'observe', {
      max_cost_usd: 20,
      ask_thresholds_usd: [5],
    });
    const store = new InMemoryGuardrailStateStore({ session: { session_cost_usd: 8 } });
    for (let call = 0; call < 2; call += 1) {
      const decision = evaluateGuardrails([rule], EVENT, { builtins: BUILTIN_EVALUATORS, store });
      expect(decision.verdict).toBe('allow');
      expect(decision.observed?.[0]?.verdict).toBe('ask');
      expect(decision.stateUpdates).toEqual([]);
      expect(decision.intendedStateUpdates).toEqual([]);
      store.apply(decision.stateUpdates);
    }
  });

  it('counts observed calls that proceed beyond a call cap', () => {
    const rule = prepared('cap', 'max_tool_calls_per_session', 'observe', { limit: 1 });
    const store = new InMemoryGuardrailStateStore();
    for (let call = 0; call < 3; call += 1) {
      const decision = evaluateGuardrails([rule], EVENT, { builtins: BUILTIN_EVALUATORS, store });
      expect(decision.verdict).toBe('allow');
      store.apply(decision.stateUpdates);
    }
    expect(store.read('session')['g:cap:tool_calls']).toBe(3);
  });

  it('withholds observed state when another enforced rule blocks the call', () => {
    const cap = prepared('cap', 'max_tool_calls_per_session', 'observe', { limit: 1 });
    const deny = prepared('deny', 'block_tools', 'enforce', { tools: ['Bash'] });
    const store = new InMemoryGuardrailStateStore();
    const decision = evaluateGuardrails([deny, cap], EVENT, {
      builtins: BUILTIN_EVALUATORS,
      store,
    });
    expect(decision.verdict).toBe('deny');
    expect(decision.stateUpdates).toEqual([]);
    expect(decision.observed?.[0]?.guardrailId).toBe('cap');
    expect(store.read('session')).toEqual({});
  });

  it('records observed evaluator failures without claiming they enforced', () => {
    const rule = prepared('bad', 'block_tools', 'observe', { tools: ['Bash'] });
    const decision = evaluateGuardrails([rule], EVENT, { builtins: new Map() });
    expect(decision.verdict).toBe('allow');
    expect(decision.observed?.[0]?.verdict).toBe('deny');
    expect(decision.errors).toEqual([
      expect.objectContaining({ guardrailId: 'bad', failedClosed: false }),
    ]);
  });

  it('preserves legacy enforce behavior when mode is absent', () => {
    const rule = prepared('deny', 'block_tools', 'enforce', { tools: ['Bash'] });
    delete rule.guardrail.mode;
    const decision = evaluateGuardrails([rule], EVENT, { builtins: BUILTIN_EVALUATORS });
    expect(decision.verdict).toBe('deny');
    expect(decision.observed).toBeUndefined();
  });
});

describe.each([
  ['user_monthly_cost_budget', 'subject_window', 'monthly_cost_usd', 'monthly_cost_unpriced'],
  [
    'workspace_monthly_cost_budget',
    'workspace_window',
    'workspace_monthly_cost_usd',
    'workspace_monthly_cost_unpriced',
  ],
] as const)('%s', (builtin, scope, costKey, unpricedKey) => {
  it('compiles only at workspace/organization scope and requires a cap', () => {
    const rule = { kind: 'builtin' as const, builtin, params: { max_cost_usd: 10 } };
    expect(compileGuardrailRule(rule, 'workspace')).toMatchObject({ ok: true, stateScope: scope });
    expect(compileGuardrailRule(rule, 'organization').ok).toBe(true);
    expect(compileGuardrailRule(rule, 'explicit').ok).toBe(false);
    expect(compileGuardrailRule({ ...rule, params: {} }, 'workspace').ok).toBe(false);
  });

  it('reads its own counter instead of event cost or another scope', () => {
    const rule = prepared('budget', builtin, 'enforce', { max_cost_usd: 10 });
    const otherScope: StateScope =
      scope === 'subject_window' ? 'workspace_window' : 'subject_window';
    const store = new InMemoryGuardrailStateStore({
      [scope]: { [costKey]: 3 },
      [otherScope]: { [costKey]: 100 },
    });
    const decision = evaluateGuardrails(
      [rule],
      { ...EVENT, usage: { totalCostUsd: 100 } },
      {
        builtins: BUILTIN_EVALUATORS,
        store,
      },
    );
    expect(decision.verdict).toBe('allow');
    store.replace(scope, { [costKey]: 10 });
    expect(evaluateGuardrails([rule], EVENT, { builtins: BUILTIN_EVALUATORS, store }).verdict).toBe(
      'deny',
    );
  });

  it('holds soft questions for tool_call and keeps their approval in its own scope', () => {
    const rule = prepared('budget', builtin, 'enforce', {
      max_cost_usd: 10,
      ask_thresholds_usd: [5],
    });
    const store = new InMemoryGuardrailStateStore({ [scope]: { [costKey]: 7 } });
    const request = evaluateGuardrails(
      [rule],
      { ...EVENT, phase: 'request' },
      {
        builtins: BUILTIN_EVALUATORS,
        store,
      },
    );
    expect(request.verdict).toBe('allow');
    const decision = evaluateGuardrails([rule], EVENT, { builtins: BUILTIN_EVALUATORS, store });
    expect(decision.verdict).toBe('ask');
    expect(decision.stateUpdates).toEqual([]);
    expect(decision.intendedStateUpdates).toEqual([
      { scope, key: 'g:budget:budget_ask_approved', action: 'set', value: 5 },
    ]);
    store.apply(decision.intendedStateUpdates);
    expect(evaluateGuardrails([rule], EVENT, { builtins: BUILTIN_EVALUATORS, store }).verdict).toBe(
      'allow',
    );
  });

  it('preserves unpriced state and defaults to asking', () => {
    const rule = prepared('budget', builtin, 'enforce', { max_cost_usd: 10 });
    const store = new InMemoryGuardrailStateStore({ [scope]: { [costKey]: 0, [unpricedKey]: 1 } });
    expect(evaluateGuardrails([rule], EVENT, { builtins: BUILTIN_EVALUATORS, store }).verdict).toBe(
      'ask',
    );
  });

  it('keeps the expensive-model downgrade gate', () => {
    const rule = prepared('budget', builtin, 'enforce', {
      max_cost_usd: 10,
      expensive_models: ['opus'],
    });
    const store = new InMemoryGuardrailStateStore({ [scope]: { [costKey]: 11 } });
    expect(
      evaluateGuardrails(
        [rule],
        { ...EVENT, modelId: 'sonnet' },
        {
          builtins: BUILTIN_EVALUATORS,
          store,
        },
      ).verdict,
    ).toBe('allow');
    expect(
      evaluateGuardrails(
        [rule],
        { ...EVENT, modelId: 'opus' },
        {
          builtins: BUILTIN_EVALUATORS,
          store,
        },
      ).verdict,
    ).toBe('deny');
  });
});
