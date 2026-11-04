// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export const GUARDRAIL_READER_CAPABILITIES = [
  'guardrails.observe.v1',
  'guardrails.monthly_budgets.v1',
  'dogwood.0.1.0',
] as const;

/** Refuse delivery to a reader that would ignore a new enforcement semantic. */
export function assertGuardrailReaderCapabilities(
  guardrails: readonly { mode?: string | undefined; rule?: unknown }[],
  capabilities: readonly string[],
): void {
  for (const guardrail of guardrails) {
    const rule = guardrail.rule as { kind?: string; builtin?: string } | null;
    const required = [
      ...(guardrail.mode === 'observe' ? ['guardrails.observe.v1'] : []),
      ...(rule?.kind === 'builtin' &&
      ['user_monthly_cost_budget', 'workspace_monthly_cost_budget'].includes(rule.builtin ?? '')
        ? ['guardrails.monthly_budgets.v1']
        : []),
      ...(rule?.kind === 'dogwood' ? ['dogwood.0.1.0'] : []),
    ];
    for (const capability of required)
      if (!capabilities.includes(capability))
        throw new Error(`guardrail reader must declare ${capability}`);
  }
}
