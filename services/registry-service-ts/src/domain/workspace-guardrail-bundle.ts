// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { and, eq, isNull, or } from 'drizzle-orm';
import type { DbClient } from '../persistence/postgres/client.js';
import { guardrails, workspaces } from '../persistence/postgres/schema.js';
import { composeGuardrails, type GuardrailRow } from './guardrail-composition.js';
import type { GuardrailRollout } from './guardrail-rollout.js';

/** Stable JSON for content identity, independent of database object-key order. */
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    return `{${Object.entries(value)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

export function workspaceGuardrailBundle(input: {
  workspaceId: string;
  organizationId: string;
  visible: readonly GuardrailRow[];
  issuedAt: Date;
  ruleKinds?: readonly string[];
  rollout?: GuardrailRollout;
}) {
  const ruleKinds = input.ruleKinds ?? ['builtin', 'expression'];
  const eligible = input.visible.filter(
    (row) => row.enabled && (row.scope === 'workspace' || row.scope === 'organization'),
  );
  const supported = eligible.filter(
    (row) =>
      row.rule !== null &&
      typeof row.rule === 'object' &&
      ruleKinds.includes((row.rule as { kind: string }).kind),
  );
  const composed = composeGuardrails(
    { sessionIds: [], agentIds: [], subagentIds: {}, visible: supported },
    input.rollout,
  );
  if (composed.invalid.length) {
    throw new Error(
      composed.invalid.map((row) => `${row.id}: ${row.errors.join('; ')}`).join(' | '),
    );
  }
  const prepared = composed.guardrails.map((entry) => ({
    id: entry.guardrail.id,
    name: entry.guardrail.name,
    tier: entry.tier,
    phases: [...entry.guardrail.phases],
    rule: entry.guardrail.rule,
    stateful: entry.stateful,
    ...(entry.stateScope ? { state_scope: entry.stateScope } : {}),
    mode: entry.guardrail.mode ?? 'enforce',
  }));
  const scope = { org_id: input.organizationId, workspace_id: input.workspaceId };
  const sorted = [...prepared].sort((a, b) => {
    const left = `${a.tier}\0${a.id}`,
      right = `${b.tier}\0${b.id}`;
    return left < right ? -1 : left > right ? 1 : 0;
  });
  const omitted = eligible.length - supported.length;
  const bundleId = `registry-${createHash('sha256')
    .update(canonical({ scope, guardrails: sorted, omitted_rules: omitted }))
    .digest('hex')}`;
  return {
    schema: '1' as const,
    bundle_id: bundleId,
    generation: input.issuedAt.getTime(),
    scope,
    runtime_config_revision: bundleId,
    issued_at: input.issuedAt.toISOString(),
    expires_at: new Date(input.issuedAt.getTime() + 3600000).toISOString(),
    seed_verdict: 'allow' as const,
    guardrails: prepared,
    omitted_rules: omitted,
  };
}

/** Read one tenant snapshot. Neither tenant identity nor the clock comes from a client. */
export async function composeWorkspaceBundle(input: {
  db: DbClient;
  workspaceId: string;
  now: () => Date;
  ruleKinds?: readonly string[];
  rollout?: GuardrailRollout;
}) {
  return input.db.transaction(
    async (tx) => {
      const [workspace] = await tx
        .select({ organizationId: workspaces.organizationId })
        .from(workspaces)
        .where(and(eq(workspaces.id, input.workspaceId), eq(workspaces.status, 'active')))
        .limit(1);
      if (!workspace) return null;
      const visible = await tx
        .select()
        .from(guardrails)
        .where(
          and(
            isNull(guardrails.deletedAt),
            isNull(guardrails.archivedAt),
            eq(guardrails.enabled, true),
            or(
              and(eq(guardrails.workspaceId, input.workspaceId), eq(guardrails.scope, 'workspace')),
              and(
                eq(guardrails.organizationId, workspace.organizationId),
                eq(guardrails.scope, 'organization'),
              ),
            ),
          ),
        )
        .orderBy(guardrails.id);
      return workspaceGuardrailBundle({
        ...input,
        organizationId: workspace.organizationId,
        visible: visible.map((row) => ({
          ...row,
          phases: row.phases as string[],
          scope: row.scope as GuardrailRow['scope'],
        })),
        issuedAt: input.now(),
      });
    },
    { isolationLevel: 'repeatable read' },
  );
}
