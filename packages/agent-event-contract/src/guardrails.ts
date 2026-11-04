// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Orca-only evidence for observer outcomes, independent of the enforcing verdict. */
export function guardrailDecisionPayload(input: {
  phase: string;
  decision: {
    verdict: 'allow' | 'ask' | 'deny';
    observed?: readonly {
      guardrailId: string;
      mode: 'observe';
      verdict: 'allow' | 'ask' | 'deny';
      reason?: string;
    }[];
  };
  toolUseId?: string;
  tools?: readonly string[];
}): Record<string, unknown> | undefined {
  const entries = input.decision.observed?.filter((row) => row.verdict !== 'allow');
  if (!entries?.length) return undefined;
  return {
    phase: input.phase,
    decision: input.decision.verdict,
    ...(input.toolUseId ? { tool_use_id: input.toolUseId } : {}),
    guardrails: entries.map((row) => ({
      guardrail_id: row.guardrailId,
      mode: row.mode,
      verdict: row.verdict,
      ...(row.reason ? { reason: row.reason } : {}),
      ...(input.tools ? { tools: [...input.tools] } : {}),
    })),
  };
}
