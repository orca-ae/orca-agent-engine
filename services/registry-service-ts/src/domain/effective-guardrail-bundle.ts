// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import type { PreparedExecutionV2 } from '../contracts/internal.contract.js';

/** The AI Gateway registry source's GuardrailBundle wire shape. */
export function effectiveGuardrailBundle(input: {
  prepared: {
    workspace_id: string;
    session: { id: string; runtime_revision: number };
    guardrails: PreparedExecutionV2['guardrails'];
    guardrail_state: Record<string, unknown>;
  };
  organizationId: string;
  issuedAt: Date;
}): {
  schema: '1';
  bundle_id: string;
  generation: number;
  scope: Record<string, string>;
  runtime_config_revision: string;
  issued_at: string;
  expires_at: string;
  seed_verdict: 'allow';
  guardrails: PreparedExecutionV2['guardrails'];
  guardrail_state: { session: Record<string, unknown> };
} {
  const { prepared, organizationId, issuedAt } = input;
  const revision = String(prepared.session.runtime_revision);
  const scope = {
    org_id: organizationId,
    workspace_id: prepared.workspace_id,
    session_id: prepared.session.id,
  };
  const canonicalGuardrails = [...prepared.guardrails].sort((left, right) => {
    const leftKey = `${left.tier}\0${left.id}\0${left.subagent_id ?? ''}`;
    const rightKey = `${right.tier}\0${right.id}\0${right.subagent_id ?? ''}`;
    return leftKey.localeCompare(rightKey);
  });
  const digest = createHash('sha256')
    .update(JSON.stringify({ scope, revision, guardrails: canonicalGuardrails }))
    .digest('hex');
  return {
    schema: '1',
    bundle_id: `registry-${digest}`,
    // Registry source validates revision, scope and expiry; only the file source
    // persists a high-water generation for replay protection.
    generation: issuedAt.getTime(),
    scope,
    runtime_config_revision: revision,
    issued_at: issuedAt.toISOString(),
    expires_at: new Date(issuedAt.getTime() + 60 * 60 * 1000).toISOString(),
    seed_verdict: 'allow',
    guardrails: prepared.guardrails,
    guardrail_state: { session: prepared.guardrail_state },
  };
}
