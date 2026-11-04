// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { workspaceGuardrailBundle } from '../../src/domain/workspace-guardrail-bundle.js';
import type { GuardrailRow } from '../../src/domain/guardrail-composition.js';

const row: GuardrailRow = {
  id: 'grd_one',
  name: 'one',
  enabled: true,
  phases: ['tool_call'],
  scope: 'workspace',
  rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
};
const input = {
  workspaceId: 'wrkspc_one',
  organizationId: 'org_one',
  visible: [row],
  issuedAt: new Date('2026-11-01T00:00:00Z'),
};

describe('workspace guardrail bundle', () => {
  it('contains only scoped rules, compiled state metadata and normalized mode, without state', () => {
    const b = workspaceGuardrailBundle({
      ...input,
      visible: [
        row,
        { ...row, id: 'grd_x', scope: 'explicit' },
        { ...row, id: 'grd_o', scope: 'organization' },
      ],
    });
    expect(b.guardrails.map((g) => g.id)).toEqual(['grd_one', 'grd_o']);
    expect(b.guardrails[0]).toMatchObject({ mode: 'enforce', stateful: false });
    expect(b).not.toHaveProperty('guardrail_state');
    expect(b.runtime_config_revision).toBe(b.bundle_id);
  });
  it('separates content identity from a lease over more than two hours', () => {
    const first = workspaceGuardrailBundle(input);
    const later = workspaceGuardrailBundle({
      ...input,
      issuedAt: new Date('2026-11-01T03:00:00Z'),
    });
    expect(later.bundle_id).toBe(first.bundle_id);
    expect(later.generation).toBe(first.generation + 3 * 3600000);
    expect(later.expires_at).toBe('2026-11-01T04:00:00.000Z');
  });
  it('normalizes object order and absent mode while binding tenant and mode', () => {
    const first = workspaceGuardrailBundle({ ...input, rollout: { observeEnabled: true } });
    expect(
      workspaceGuardrailBundle({
        ...input,
        visible: [{ ...row, mode: 'enforce' }],
        rollout: { observeEnabled: true },
      }).bundle_id,
    ).toBe(first.bundle_id);
    expect(
      workspaceGuardrailBundle({
        ...input,
        visible: [{ ...row, mode: 'observe' }],
        rollout: { observeEnabled: true },
      }).bundle_id,
    ).not.toBe(first.bundle_id);
    expect(workspaceGuardrailBundle({ ...input, workspaceId: 'wrkspc_other' }).bundle_id).not.toBe(
      first.bundle_id,
    );
    const reordered = { params: { tools: ['Bash'] }, builtin: 'block_tools', kind: 'builtin' };
    expect(
      workspaceGuardrailBundle({ ...input, visible: [{ ...row, rule: reordered }] }).bundle_id,
    ).toBe(first.bundle_id);
  });
  it('counts omitted rule kinds and fails incompatible stored policies before issuing a lease', () => {
    const b = workspaceGuardrailBundle({
      ...input,
      visible: [row, { ...row, id: 'grd_new', rule: { kind: 'future' } }],
    });
    expect(b.omitted_rules).toBe(1);
    expect(b.guardrails).toHaveLength(1);
    expect(() =>
      workspaceGuardrailBundle({ ...input, visible: [{ ...row, mode: 'observe' }] }),
    ).toThrow(/OBSERVE/);
  });
});
