// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Authoring and publication gates; absent options always retain the legacy surface. */
export interface GuardrailRollout {
  observeEnabled?: boolean;
  dogwoodEnabled?: boolean;
  gatewayDogwoodEnabled?: boolean;
  monthlyBudgetsEnabled?: boolean;
}

export interface GuardrailRouteOptions extends GuardrailRollout {
  now?: () => Date;
}

export function isMonthlyBudget(rule: unknown): boolean {
  if (rule === null || typeof rule !== 'object') return false;
  const value = rule as Record<string, unknown>;
  return (
    value['kind'] === 'builtin' &&
    (value['builtin'] === 'user_monthly_cost_budget' ||
      value['builtin'] === 'workspace_monthly_cost_budget')
  );
}

export function guardrailRolloutError(
  guardrail: { mode?: string; rule: unknown },
  rollout: GuardrailRollout = {},
): string | undefined {
  if (
    guardrail.mode !== undefined &&
    guardrail.mode !== 'enforce' &&
    guardrail.mode !== 'observe'
  ) {
    return 'unknown guardrail mode';
  }
  if (guardrail.mode === 'observe' && !rollout.observeEnabled) {
    return 'observe mode requires GUARDRAILS_OBSERVE_ENABLED';
  }
  if (isMonthlyBudget(guardrail.rule) && !rollout.monthlyBudgetsEnabled) {
    return 'monthly budgets require GUARDRAILS_MONTHLY_BUDGETS_ENABLED';
  }
  if (
    guardrail.rule &&
    typeof guardrail.rule === 'object' &&
    'kind' in guardrail.rule &&
    guardrail.rule.kind === 'dogwood' &&
    !rollout.dogwoodEnabled
  )
    return 'Dogwood requires GUARDRAILS_DOGWOOD_ENABLED';
  return undefined;
}
