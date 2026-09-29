// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { compileGuardrailRule } from '../../src/compile.js';
import type { GuardrailRule, Scope } from '../../src/types.js';

function builtin(name: string, params?: Record<string, unknown>): GuardrailRule {
  return { kind: 'builtin', builtin: name, ...(params ? { params } : {}) };
}

function ok(rule: GuardrailRule, scope: Scope = 'explicit') {
  const r = compileGuardrailRule(rule, scope);
  if (!r.ok)
    throw new Error(`expected compile to succeed: ${r.errors.map((e) => e.message).join('; ')}`);
  return r;
}

function err(rule: GuardrailRule, scope: Scope = 'explicit') {
  const r = compileGuardrailRule(rule, scope);
  if (r.ok) throw new Error('expected compile to fail');
  return r.errors;
}

describe('builtin rules', () => {
  it('accepts a known builtin with valid parameters', () => {
    const r = ok(builtin('max_tool_calls_per_session', { limit: 50 }));
    expect(r.stateful).toBe(true);
    expect(r.stateScope).toBe('session');
  });

  it('rejects the internal tool_permission_policy as if it were unknown', () => {
    // An internal builtin is machinery, not something an author may reference;
    // it must be indistinguishable from a name that does not exist.
    const errors = err(builtin('tool_permission_policy'));
    expect(errors[0]?.message).toMatch(/unknown guardrail type/i);
  });

  it('rejects a parameter named after an Object.prototype member', () => {
    // `toString` is on the prototype chain; a closed schema must still reject it.
    const errors = err(builtin('block_tools', { tools: ['Bash'], toString: 'x' }));
    expect(errors.some((e) => e.path === 'toString')).toBe(true);
  });

  it('rejects a budget that configures neither a cap nor a threshold', () => {
    expect(err(builtin('cost_budget', {})).length).toBeGreaterThan(0);
    expect(err(builtin('subagent_cost_budget', {})).length).toBeGreaterThan(0);
  });

  it('accepts a budget with a cap alone or thresholds alone', () => {
    ok(builtin('cost_budget', { max_cost_usd: 25 }));
    ok(builtin('cost_budget', { ask_thresholds_usd: [10] }));
  });

  it('rejects an unknown builtin by name', () => {
    const errors = err(builtin('no_such_guardrail'));
    expect(errors[0]?.message).toMatch(/unknown guardrail type/i);
  });

  it('accepts a builtin whose parameters are all optional with none supplied', () => {
    expect(ok(builtin('ask_on_os_tools')).stateful).toBe(false);
  });

  it('reports statefulness and scope from the type, not the rule', () => {
    expect(ok(builtin('spawn_bounds')).stateScope).toBe('turn');
    expect(ok(builtin('user_daily_cost_budget', { max_cost_usd: 5 }), 'workspace').stateScope).toBe(
      'subject_window',
    );
    expect(ok(builtin('block_tools', { tools: ['Bash'] })).stateful).toBe(false);
  });

  it('rejects a principal-wide budget at an explicit agent or session scope', () => {
    const rule = builtin('user_daily_cost_budget', { max_cost_usd: 5 });
    expect(compileGuardrailRule(rule, 'workspace').ok).toBe(true);
    expect(compileGuardrailRule(rule, 'organization').ok).toBe(true);
    const explicit = compileGuardrailRule(rule, 'explicit');
    expect(explicit.ok).toBe(false);
    if (!explicit.ok) expect(explicit.errors[0]?.message).toMatch(/scope/i);
  });

  it('rejects a missing or invalid authoring scope at runtime', () => {
    const rule = builtin('user_daily_cost_budget', { max_cost_usd: 5 });
    for (const scope of [undefined, 'session']) {
      const result = compileGuardrailRule(rule, scope as unknown as Scope);
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.errors[0]).toMatchObject({ path: 'scope' });
    }
  });
});

describe('parameter validation', () => {
  it('rejects a missing required parameter', () => {
    const errors = err(builtin('block_tools'));
    expect(errors[0]?.message).toMatch(/required/i);
    expect(errors[0]?.path).toBe('tools');
  });

  it('rejects a parameter of the wrong type', () => {
    const errors = err(builtin('max_tool_calls_per_session', { limit: 'lots' }));
    expect(errors[0]?.path).toBe('limit');
  });

  it('rejects an unknown parameter rather than ignoring it', () => {
    // Silently dropping a misspelled parameter would leave an author believing
    // a limit is enforced that never was.
    const errors = err(builtin('max_tool_calls_per_session', { limitt: 5 }));
    expect(errors[0]?.message).toMatch(/unknown parameter/i);
  });

  it('rejects a value outside a declared minimum', () => {
    expect(err(builtin('max_tool_calls_per_session', { limit: 0 })).length).toBeGreaterThan(0);
  });

  it('rejects a value outside a declared enum', () => {
    expect(err(builtin('block_working_dir_changes', { action: 'shrug' })).length).toBeGreaterThan(
      0,
    );
  });

  it('rejects a non-array where an array is declared', () => {
    expect(err(builtin('block_tools', { tools: 'Bash' })).length).toBeGreaterThan(0);
  });

  it('rejects an array whose items are the wrong type', () => {
    expect(err(builtin('block_tools', { tools: [1, 2] })).length).toBeGreaterThan(0);
  });

  it('accepts an integer where a number is declared', () => {
    expect(ok(builtin('cost_budget', { max_cost_usd: 25 })).stateful).toBe(true);
  });

  it('rejects a non-integer where an integer is declared', () => {
    expect(err(builtin('max_tool_calls_per_session', { limit: 1.5 })).length).toBeGreaterThan(0);
  });

  it('reports every invalid parameter, not just the first', () => {
    const errors = err(builtin('detect_loop', { threshold: 'x', window: 'y' }));
    expect(errors.length).toBeGreaterThanOrEqual(2);
  });
});

describe('cost budget parameters', () => {
  it('leaves an omitted expensive_models omitted rather than defaulting it', () => {
    // An absent list and an empty list both mean "every model is blocked at the
    // cap". Materialising a default here would put a value in the record that
    // the author never wrote, which then reads as an explicit choice.
    const r = ok(builtin('cost_budget', { max_cost_usd: 10 }));
    expect(Object.prototype.hasOwnProperty.call(r.params, 'expensive_models')).toBe(false);
  });

  it('requires a cap on the per-principal daily budget', () => {
    expect(err(builtin('user_daily_cost_budget')).length).toBeGreaterThan(0);
  });

  it('allows a session budget with thresholds and no cap, for spend visibility', () => {
    expect(ok(builtin('cost_budget', { ask_thresholds_usd: [1, 5] })).stateful).toBe(true);
  });
});

describe('expression rules', () => {
  it('accepts a valid expression and reports it as stateless when it reads no state', () => {
    const r = ok({
      kind: 'expression',
      expression: "event.tool.name != 'Bash'",
      onFalse: 'deny',
    });
    expect(r.stateful).toBe(false);
  });

  it('reports an expression that reads state as stateful', () => {
    const r = ok({
      kind: 'expression',
      expression: 'event.state.calls < 10',
      onFalse: 'deny',
    });
    expect(r.stateful).toBe(true);
  });

  it('rejects a syntactically invalid expression at compile time', () => {
    const errors = err({ kind: 'expression', expression: 'event.tool.name ===', onFalse: 'deny' });
    expect(errors.length).toBeGreaterThan(0);
  });

  it('carries the compiled expression through so it is not recompiled per event', () => {
    const r = ok({ kind: 'expression', expression: 'true', onFalse: 'deny' });
    expect(r.compiled).toBeDefined();
  });

  it('rejects an expression onFalse verdict decoded outside the type boundary', () => {
    for (const onFalse of ['allow', 'shrug', undefined]) {
      const errors = err({
        kind: 'expression',
        expression: 'false',
        onFalse,
      } as unknown as GuardrailRule);
      expect(errors).toContainEqual(
        expect.objectContaining({ path: 'onFalse', message: expect.stringMatching(/ask.*deny/i) }),
      );
    }
  });
});

describe('bounds the catalog declares are actually enforced', () => {
  it('rejects a spend cap of zero, which is a cap already exceeded rather than no spend', () => {
    expect(err(builtin('cost_budget', { max_cost_usd: 0 })).length).toBeGreaterThan(0);
  });

  it('rejects a negative spend cap', () => {
    expect(err(builtin('user_daily_cost_budget', { max_cost_usd: -1 })).length).toBeGreaterThan(0);
  });

  it('rejects an empty list where the catalog requires at least one entry', () => {
    const entries = err(builtin('block_tools', { tools: [] }));
    expect(entries.length).toBeGreaterThan(0);
  });
});

describe('numeric and one-of parameter guards', () => {
  it('rejects an infinite budget cap', () => {
    // `1e999` in a JSON body parses to Infinity, which no finite spend reaches.
    const cap = JSON.parse('{"max_cost_usd": 1e999}') as { max_cost_usd: number };
    expect(err(builtin('cost_budget', cap)).length).toBeGreaterThan(0);
  });

  it('rejects a budget whose only threshold list is empty', () => {
    expect(err(builtin('cost_budget', { ask_thresholds_usd: [] })).length).toBeGreaterThan(0);
  });
});

describe('parameters on the prototype do not slip through', () => {
  it('rejects a budget whose cap lives on the prototype rather than as an own key', () => {
    const viaCreate = Object.create({ max_cost_usd: 25 }) as Record<string, unknown>;
    expect(compileGuardrailRule(builtin('cost_budget', viaCreate), 'explicit').ok).toBe(false);

    const viaProto = Object.assign({}, JSON.parse('{"__proto__":{"max_cost_usd":25}}')) as Record<
      string,
      unknown
    >;
    expect(compileGuardrailRule(builtin('cost_budget', viaProto), 'explicit').ok).toBe(false);
  });
});
