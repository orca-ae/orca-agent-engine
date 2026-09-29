// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type { PreparedExecutionV2 } from '../../src/contracts/internal.contract.js';
import { effectiveGuardrailBundle } from '../../src/domain/effective-guardrail-bundle.js';

describe('effectiveGuardrailBundle', () => {
  it('binds the prepared policy and state to the Registry session scope', () => {
    const prepared = {
      workspace_id: 'wrkspc_example',
      session: { id: 'ses_example', runtime_revision: 7 },
      guardrails: [{ id: 'grd_example', tier: 'workspace', phases: ['llm_request'] }],
      guardrail_state: { 'tokens:grd_example': 12 },
    } as unknown as PreparedExecutionV2;
    const bundle = effectiveGuardrailBundle({
      prepared,
      organizationId: 'org_example',
      issuedAt: new Date('2026-09-18T00:00:00.000Z'),
    });

    expect(bundle.scope).toEqual({
      org_id: 'org_example',
      workspace_id: 'wrkspc_example',
      session_id: 'ses_example',
    });
    expect(bundle.runtime_config_revision).toBe('7');
    expect(bundle.guardrails).toBe(prepared.guardrails);
    expect(bundle.guardrail_state).toEqual({ session: prepared.guardrail_state });
    expect(bundle.seed_verdict).toBe('allow');
    expect(bundle.expires_at).toBe('2026-09-18T01:00:00.000Z');
  });

  it('keeps the bundle id stable when database guardrails arrive in a different order', () => {
    const guardrails = [
      { id: 'grd_b', tier: 'workspace', phases: ['llm_request'] },
      { id: 'grd_a', tier: 'workspace', phases: ['llm_request'] },
    ];
    const prepared = {
      workspace_id: 'wrkspc_example',
      session: { id: 'ses_example', runtime_revision: 7 },
      guardrails,
      guardrail_state: {},
    } as unknown as PreparedExecutionV2;
    const issuedAt = new Date('2026-09-18T00:00:00.000Z');
    const first = effectiveGuardrailBundle({ prepared, organizationId: 'org_example', issuedAt });
    const reversed = effectiveGuardrailBundle({
      prepared: { ...prepared, guardrails: [...prepared.guardrails].reverse() },
      organizationId: 'org_example',
      issuedAt,
    });
    expect(reversed.bundle_id).toBe(first.bundle_id);
    expect(reversed.guardrails).toEqual([...prepared.guardrails].reverse());
  });
});
