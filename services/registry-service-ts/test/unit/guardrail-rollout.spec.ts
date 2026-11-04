// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { guardrailRolloutError, isMonthlyBudget } from '../../src/domain/guardrail-rollout.js';
import { composeGuardrails, type GuardrailRow } from '../../src/domain/guardrail-composition.js';

const rule = { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } };
const monthly = {
  kind: 'builtin',
  builtin: 'workspace_monthly_cost_budget',
  params: { max_cost_usd: 10 },
};
const row: GuardrailRow = {
  id: 'grd_one',
  name: 'one',
  enabled: true,
  scope: 'workspace',
  phases: ['tool_call'],
  rule,
};
const compose = (visible: GuardrailRow[], opts = {}) =>
  composeGuardrails({ sessionIds: [], agentIds: [], subagentIds: {}, visible }, opts);

describe('guardrail rollout gates', () => {
  it('defaults both capabilities off without blocking legacy rules', () => {
    expect(guardrailRolloutError({ rule })).toBeUndefined();
    expect(guardrailRolloutError({ rule, mode: 'observe' })).toContain(
      'GUARDRAILS_OBSERVE_ENABLED',
    );
    expect(guardrailRolloutError({ rule: monthly })).toContain(
      'GUARDRAILS_MONTHLY_BUDGETS_ENABLED',
    );
    expect(isMonthlyBudget(monthly)).toBe(true);
    expect(isMonthlyBudget({ ...monthly, builtin: 'cost_budget' })).toBe(false);
  });
  it('opens each gate independently', () => {
    expect(
      guardrailRolloutError({ rule, mode: 'observe' }, { observeEnabled: true }),
    ).toBeUndefined();
    expect(
      guardrailRolloutError({ rule: monthly }, { monthlyBudgetsEnabled: true }),
    ).toBeUndefined();
    expect(
      guardrailRolloutError({ rule: monthly, mode: 'observe' }, { monthlyBudgetsEnabled: true }),
    ).toContain('OBSERVE');
  });
  it('refuses incompatible stored compositions instead of dropping or enforcing them', () => {
    expect(compose([{ ...row, mode: 'observe' }])).toMatchObject({
      guardrails: [],
      invalid: [{ id: row.id }],
    });
    expect(compose([{ ...row, rule: monthly }])).toMatchObject({
      guardrails: [],
      invalid: [{ id: row.id }],
    });
    expect(compose([{ ...row, enabled: false, mode: 'observe' }]).invalid).toEqual([]);
  });
  it('keeps legacy prepared shape while closed and publishes normalized modes while open', () => {
    expect(compose([row]).guardrails[0]?.guardrail.mode).toBeUndefined();
    expect(compose([row], { observeEnabled: true }).guardrails[0]?.guardrail.mode).toBe('enforce');
    expect(
      compose([{ ...row, mode: 'observe' }], { observeEnabled: true }).guardrails[0]?.guardrail
        .mode,
    ).toBe('observe');
    expect(
      compose([{ ...row, rule: monthly }], { monthlyBudgetsEnabled: true }).guardrails[0]
        ?.stateScope,
    ).toBe('workspace_window');
  });
});
