// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type { Phase } from '@orca/guardrails';
import {
  Guardrail,
  GuardrailCreate,
  GuardrailRuleWire,
  GuardrailUpdate,
  resolveGuardrailAuthoring,
  ruleToStorage,
  ruleToWire,
} from '../../src/contracts/guardrails.contract.js';

const blockTools = {
  kind: 'builtin' as const,
  builtin: 'block_tools',
  params: { tools: ['Bash'] },
};

type AuthoringInput = Parameters<typeof resolveGuardrailAuthoring>[0];

/**
 * Scope defaults to `workspace`. Most cases here are about the rule and its
 * phases rather than tiering, and a case that cares names its own.
 */
function authoring(
  input: Omit<AuthoringInput, 'scope'> & { scope?: AuthoringInput['scope'] },
): AuthoringInput {
  return { scope: 'workspace', ...input };
}

function errorsFor(input: Omit<AuthoringInput, 'scope'> & { scope?: AuthoringInput['scope'] }) {
  const result = resolveGuardrailAuthoring(authoring(input));
  if (result.ok) throw new Error('expected authoring to fail');
  return result.errors;
}

function resolved(input: Omit<AuthoringInput, 'scope'> & { scope?: AuthoringInput['scope'] }) {
  const result = resolveGuardrailAuthoring(authoring(input));
  if (!result.ok)
    throw new Error(`expected authoring to succeed: ${JSON.stringify(result.errors)}`);
  return result.value;
}

describe('GuardrailRuleWire', () => {
  it('accepts a builtin rule with and without params', () => {
    expect(GuardrailRuleWire.safeParse(blockTools).success).toBe(true);
    expect(
      GuardrailRuleWire.safeParse({ kind: 'builtin', builtin: 'ask_on_os_tools' }).success,
    ).toBe(true);
  });

  it('rejects an unknown key on a builtin rule', () => {
    // Closed by design: a misspelled field silently dropped leaves an author
    // believing they configured something they did not.
    expect(
      GuardrailRuleWire.safeParse({ ...blockTools, parameters: { tools: ['Bash'] } }).success,
    ).toBe(false);
  });

  it('rejects an unknown rule kind', () => {
    expect(GuardrailRuleWire.safeParse({ kind: 'regex', pattern: '.*' }).success).toBe(false);
  });

  it('accepts an expression rule and requires on_false', () => {
    expect(
      GuardrailRuleWire.safeParse({
        kind: 'expression',
        expression: 'event.tool.name != "Bash"',
        on_false: 'deny',
        reason: 'shell is off',
      }).success,
    ).toBe(true);
    expect(GuardrailRuleWire.safeParse({ kind: 'expression', expression: 'true' }).success).toBe(
      false,
    );
  });

  it('rejects allow as an expression verdict', () => {
    // `allow` on false would be a no-op guardrail, so it is not a choice.
    expect(
      GuardrailRuleWire.safeParse({
        kind: 'expression',
        expression: 'true',
        on_false: 'allow',
      }).success,
    ).toBe(false);
  });

  it('rejects an empty expression', () => {
    expect(
      GuardrailRuleWire.safeParse({ kind: 'expression', expression: '', on_false: 'deny' }).success,
    ).toBe(false);
  });
});

describe('GuardrailCreate', () => {
  it('accepts a minimal builtin guardrail', () => {
    expect(GuardrailCreate.safeParse({ name: 'no-shell', rule: blockTools }).success).toBe(true);
  });

  it('requires a name and a rule', () => {
    expect(GuardrailCreate.safeParse({ rule: blockTools }).success).toBe(false);
    expect(GuardrailCreate.safeParse({ name: 'no-shell' }).success).toBe(false);
    expect(GuardrailCreate.safeParse({ name: '', rule: blockTools }).success).toBe(false);
  });

  it('rejects an empty or unknown phase list', () => {
    expect(GuardrailCreate.safeParse({ name: 'g', rule: blockTools, phases: [] }).success).toBe(
      false,
    );
    expect(
      GuardrailCreate.safeParse({ name: 'g', rule: blockTools, phases: ['startup'] }).success,
    ).toBe(false);
    expect(
      GuardrailCreate.safeParse({ name: 'g', rule: blockTools, phases: ['tool_call'] }).success,
    ).toBe(true);
  });

  it('parses every scope, leaving authority to the listener', () => {
    // The schema describes shape. Whether a caller may write an
    // organization-scoped guardrail is an authorization question, and
    // answering it here would turn a 403 into a 400.
    for (const scope of ['workspace', 'explicit', 'organization']) {
      expect(GuardrailCreate.safeParse({ name: 'g', rule: blockTools, scope }).success).toBe(true);
    }
    expect(
      GuardrailCreate.safeParse({ name: 'g', rule: blockTools, scope: 'session' }).success,
    ).toBe(false);
  });

  it('rejects unknown top-level fields', () => {
    expect(
      GuardrailCreate.safeParse({ name: 'g', rule: blockTools, workspace_id: 'ws_1' }).success,
    ).toBe(false);
  });

  it('bounds description and metadata', () => {
    expect(
      GuardrailCreate.safeParse({ name: 'g', rule: blockTools, description: 'd'.repeat(1025) })
        .success,
    ).toBe(false);
    expect(
      GuardrailCreate.safeParse({ name: 'g', rule: blockTools, description: null }).success,
    ).toBe(true);
  });
});

describe('GuardrailUpdate', () => {
  it('accepts a partial patch and rejects unknown fields', () => {
    expect(GuardrailUpdate.safeParse({}).success).toBe(true);
    expect(GuardrailUpdate.safeParse({ enabled: false }).success).toBe(true);
    expect(GuardrailUpdate.safeParse({ rule: blockTools }).success).toBe(true);
    expect(GuardrailUpdate.safeParse({ archived_at: null }).success).toBe(false);
  });

  it('accepts a metadata patch with null deletions', () => {
    expect(GuardrailUpdate.safeParse({ metadata: { owner: null } }).success).toBe(true);
  });
});

describe('Guardrail response', () => {
  const wire = {
    id: 'grd_01ABC',
    type: 'guardrail',
    name: 'no-shell',
    description: '',
    enabled: true,
    phases: ['tool_call'],
    scope: 'workspace',
    rule: blockTools,
    metadata: {},
    archived_at: null,
    created_at: '2026-07-31T00:00:00.000Z',
    updated_at: '2026-07-31T00:00:00.000Z',
  };

  it('round-trips a stored guardrail projection', () => {
    expect(Guardrail.safeParse(wire).success).toBe(true);
  });

  it('requires the grd_ id prefix', () => {
    expect(Guardrail.safeParse({ ...wire, id: 'env_01ABC' }).success).toBe(false);
  });
});

describe('rule wire/storage translation', () => {
  it('maps on_false to the evaluated field name and back', () => {
    const stored = ruleToStorage({
      kind: 'expression',
      expression: 'true',
      on_false: 'deny',
      reason: 'nope',
    });
    expect(stored).toEqual({
      kind: 'expression',
      expression: 'true',
      onFalse: 'deny',
      reason: 'nope',
    });
    expect(ruleToWire(stored)).toEqual({
      kind: 'expression',
      expression: 'true',
      on_false: 'deny',
      reason: 'nope',
    });
  });

  it('passes builtin rules through unchanged', () => {
    expect(ruleToStorage(blockTools)).toEqual(blockTools);
    expect(ruleToWire(blockTools)).toEqual(blockTools);
  });

  it('omits absent optional fields rather than writing nulls', () => {
    const stored = ruleToStorage({ kind: 'builtin', builtin: 'ask_on_os_tools' });
    expect(stored).toEqual({ kind: 'builtin', builtin: 'ask_on_os_tools' });
    expect(Object.keys(stored)).not.toContain('params');
  });
});

describe('resolveGuardrailAuthoring', () => {
  it('defaults a builtin to the phases the catalog says it fires on', () => {
    // The catalog is the single description of where a builtin fires, so an
    // author who omits `phases` cannot land a guardrail that fires nowhere.
    expect(resolved({ rule: blockTools }).phases).toEqual(['tool_call']);
    // `cost_budget` needs a cap or a threshold to compile at all — the catalog
    // declares `requireAtLeastOneOf`, because a budget configured with neither
    // enforces nothing.
    expect(
      resolved({
        rule: { kind: 'builtin', builtin: 'cost_budget', params: { max_cost_usd: 25 } },
      }).phases,
    ).toEqual(['request', 'tool_call']);
  });

  it('accepts an explicit subset of the catalog phases', () => {
    const value = resolved({
      rule: { kind: 'builtin', builtin: 'cost_budget', params: { max_cost_usd: 25 } },
      phases: ['tool_call'],
    });
    expect(value.phases).toEqual(['tool_call']);
  });

  it('rejects a builtin authored outside the scopes its catalog entry allows', () => {
    // `user_daily_cost_budget` caps a principal across sessions, so it is only
    // coherent at a tier that owns every session — the catalog says workspace or
    // organization. Authoring it at `explicit` scope would attach a
    // cross-session cap to one agent, and the compiler refuses.
    //
    // This is the check that never ran while the scope was optional: the rule
    // was accepted at write time and only failed later, during composition.
    const errors = errorsFor({
      rule: { kind: 'builtin', builtin: 'user_daily_cost_budget', params: { max_cost_usd: 25 } },
      scope: 'explicit',
    });
    expect(errors).toEqual([
      { message: expect.stringContaining('may only be authored at workspace or organization') },
    ]);
  });

  it('accepts that same builtin at a scope the catalog allows', () => {
    // Reaching the compiler at all is the point: this is the builtin the
    // registry withheld until its counter was maintained, and it is authorable
    // again now that the usage route advances `daily_cost_usd`.
    expect(
      resolved({
        rule: { kind: 'builtin', builtin: 'user_daily_cost_budget', params: { max_cost_usd: 25 } },
        scope: 'workspace',
      }).phases,
    ).toEqual(['request', 'tool_call']);
  });

  it('rejects a phase the builtin never fires on', () => {
    const errors = errorsFor({ rule: blockTools, phases: ['response'] });
    expect(errors).toEqual([{ path: 'phases', message: expect.stringContaining('response') }]);
  });

  it('rejects an expression authored onto a phase no enforcement point fires', () => {
    // An expression rule has no catalog entry, so the catalog cross-check
    // above cannot screen it: every one of the six modelled phases used to be
    // accepted here. `response` and `llm_response` reach no runtime, and
    // `llm_request` awaits the model-endpoint interceptor, so each of these
    // returned a 201 for a guardrail that could never evaluate.
    for (const phase of ['response', 'llm_response', 'llm_request'] as Phase[]) {
      const errors = errorsFor({
        rule: { kind: 'expression', expression: 'event.tool.name == "Bash"', onFalse: 'deny' },
        phases: [phase],
      });
      expect(errors).toEqual([
        { path: 'phases', message: expect.stringContaining(`no enforcement point fires ${phase}`) },
      ]);
    }
  });

  it('rejects a builtin asked for an unfired phase but keeps that phase in its default', () => {
    // `deny_pii_in_llm_request` declares `llm_request` in the catalog, and the
    // stored row keeps it: the catalog describes the rule, not this tree's
    // wiring, so the rule starts working when the interceptor lands with no
    // row to migrate. Asking for that phase by hand is refused, because it
    // would leave the guardrail with nothing it can evaluate at all.
    const pii = { kind: 'builtin', builtin: 'deny_pii_in_llm_request' } as const;
    expect(resolved({ rule: pii }).phases).toEqual(['request', 'llm_request']);
    expect(errorsFor({ rule: pii, phases: ['llm_request'] })).toEqual([
      {
        path: 'phases',
        message: expect.stringContaining('no enforcement point fires llm_request'),
      },
    ]);
  });

  it('rejects an unknown builtin', () => {
    expect(errorsFor({ rule: { kind: 'builtin', builtin: 'no_such_rule' } })).toEqual([
      { message: 'unknown guardrail type: no_such_rule' },
    ]);
  });

  it('rejects an internal builtin nobody authors', () => {
    // `tool_permission_policy` is the seed of the composition fold, not a rule
    // anyone writes, and it is absent from the served catalog. Accepting it
    // would let a client author a type the catalog never advertises.
    expect(errorsFor({ rule: { kind: 'builtin', builtin: 'tool_permission_policy' } })).toEqual([
      { message: 'unknown guardrail type: tool_permission_policy' },
    ]);
  });

  it('surfaces parameter errors from the shared compiler', () => {
    const errors = errorsFor({
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tool: ['Bash'] } },
    });
    expect(errors).toEqual(
      expect.arrayContaining([
        { path: 'tool', message: 'unknown parameter: tool' },
        { path: 'tools', message: 'missing required parameter: tools' },
      ]),
    );
  });

  it('rejects a cost cap of zero, which reads as a budget and evaluates as exceeded', () => {
    const errors = errorsFor({
      rule: { kind: 'builtin', builtin: 'cost_budget', params: { max_cost_usd: 0 } },
    });
    expect(errors).toEqual([
      { path: 'max_cost_usd', message: 'max_cost_usd must be greater than 0' },
    ]);
  });

  it('requires phases for an expression rule, which has no catalog default', () => {
    expect(
      errorsFor({ rule: { kind: 'expression', expression: 'true', onFalse: 'deny' } }),
    ).toEqual([{ path: 'phases', message: expect.stringContaining('phases') }]);
  });

  it('rejects an expression that does not compile', () => {
    const errors = errorsFor({
      rule: { kind: 'expression', expression: 'event.tool.name ===', onFalse: 'deny' },
      phases: ['tool_call'],
    });
    expect(errors).toHaveLength(1);
    expect(errors[0]!.path).toBe('expression');
  });

  it('rejects an expression that selects an unknown activation field', () => {
    const errors = errorsFor({
      rule: { kind: 'expression', expression: 'event.nonsense == 1', onFalse: 'deny' },
      phases: ['tool_call'],
    });
    expect(errors).toHaveLength(1);
    expect(errors[0]!.path).toBe('expression');
  });

  it('accepts a compiling expression on a phase that can ask', () => {
    const value = resolved({
      rule: { kind: 'expression', expression: 'event.state.calls < 10', onFalse: 'ask' },
      phases: ['tool_call'],
    });
    expect(value.phases).toEqual(['tool_call']);
    expect(value.rule).toEqual({
      kind: 'expression',
      expression: 'event.state.calls < 10',
      onFalse: 'ask',
    });
  });

  it('rejects ask on a phase with no round trip to resolve it', () => {
    // The engine is obliged to degrade `ask` to `deny` where no approval
    // exchange exists. Accepting it here would store a verdict the author
    // chose and the runtime cannot honor.
    const errors = errorsFor({
      rule: { kind: 'expression', expression: 'true', onFalse: 'ask' },
      phases: ['request'],
    });
    expect(errors).toEqual([{ path: 'on_false', message: expect.stringContaining('request') }]);
  });
});
