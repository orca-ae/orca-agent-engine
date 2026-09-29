// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { compileGuardrailRule } from '@orca/guardrails';
import type { Guardrail, GuardrailRule, Phase, Scope, StateScope, Tier } from '@orca/guardrails';

/**
 * Resolving which guardrails apply to a session, and in what order.
 *
 * Composition is monotonic — a guardrail can tighten a verdict but never loosen
 * one — so ordering does not change the outcome, only which reason a client
 * sees first. What ordering does encode is authority, and the order is fixed:
 * session, agent, workspace, organization.
 */

/** A stored guardrail row, narrowed to the fields composition needs. */
export interface GuardrailRow {
  id: string;
  name: string;
  enabled: boolean;
  phases: readonly string[];
  scope: Scope;
  rule: unknown;
}

export interface GuardrailSources {
  /** Guardrails a session named in its agent overrides. */
  sessionIds: readonly string[];
  /** Guardrails the coordinator agent names. */
  agentIds: readonly string[];
  /**
   * Guardrails each dispatched subagent names, keyed by subagent id.
   *
   * The coordinator's own guardrails are composed too, and that is the point:
   * without them, an agent that blocks a tool would not stop a subagent it
   * dispatched, and delegation would launder work past the guardrails of the
   * agent the user actually invoked.
   */
  subagentIds: Readonly<Record<string, readonly string[]>>;
  /** Every non-archived guardrail visible to this session's workspace. */
  visible: readonly GuardrailRow[];
}

export interface ComposedGuardrail {
  guardrail: Guardrail;
  tier: Tier;
  stateful: boolean;
  stateScope?: StateScope;
  subagentId?: string;
}

export interface InvalidGuardrail {
  id: string;
  name: string;
  errors: readonly string[];
}

export interface ComposedGuardrails {
  guardrails: readonly ComposedGuardrail[];
  /**
   * Stored rules that no longer compile — the catalog changed under them.
   * Surfaced rather than dropped: a session running without a guardrail its
   * operator believes is applied is a worse outcome than a visible failure.
   */
  invalid: readonly InvalidGuardrail[];
}

export function composeGuardrails(sources: GuardrailSources): ComposedGuardrails {
  const byId = new Map(sources.visible.map((row) => [row.id, row]));
  const guardrails: ComposedGuardrail[] = [];
  const invalid: InvalidGuardrail[] = [];
  const seen = new Set<string>();

  const take = (row: GuardrailRow | undefined, tier: Tier, subagentId?: string): void => {
    if (!row || !row.enabled) return;
    // Kept at its most authoritative appearance. Because composition is
    // monotonic, a second appearance at a weaker tier adds nothing.
    const bindingKey = subagentId === undefined ? row.id : `${row.id}\0${subagentId}`;
    if (seen.has(bindingKey)) return;
    seen.add(bindingKey);

    const compiled = compileGuardrailRule(row.rule as GuardrailRule, row.scope);
    if (!compiled.ok) {
      invalid.push({ id: row.id, name: row.name, errors: compiled.errors.map((e) => e.message) });
      return;
    }

    guardrails.push({
      guardrail: {
        id: row.id,
        name: row.name,
        enabled: row.enabled,
        phases: row.phases as readonly Phase[],
        scope: row.scope,
        rule: row.rule as GuardrailRule,
      },
      tier,
      stateful: compiled.stateful,
      ...(compiled.stateScope ? { stateScope: compiled.stateScope } : {}),
      ...(subagentId !== undefined ? { subagentId } : {}),
    });
  };

  for (const id of sources.sessionIds) take(byId.get(id), 'session');
  for (const id of sources.agentIds) take(byId.get(id), 'agent');
  for (const [subagentId, ids] of Object.entries(sources.subagentIds)) {
    for (const id of ids) take(byId.get(id), 'agent', subagentId);
  }
  for (const row of sources.visible) {
    if (row.scope === 'workspace') take(row, 'workspace');
  }
  for (const row of sources.visible) {
    if (row.scope === 'organization') take(row, 'organization');
  }

  return { guardrails, invalid };
}
