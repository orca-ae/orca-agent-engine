// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { canonicalJson, policyStateOwner } from '../../src/domain/policy-events.js';
import { guardrailCounterWindow } from '../../src/domain/usage-accounting.js';
import type { DogwoodPrepared } from '../../src/domain/dogwood-types.js';

const now = new Date('2026-11-02T00:00:00Z');
function rule(id: string, kind = 'builtin'): DogwoodPrepared {
  return {
    id,
    name: id,
    tier: 'workspace',
    phases: ['tool_call'],
    stateful: true,
    rule:
      kind === 'dogwood'
        ? { kind, policy: 'permit(principal,action,resource);' }
        : { kind, builtin: 'max_tool_calls', params: { max_calls: 10 } },
  };
}
describe('policy metadata', () => {
  it('canonicalizes nested keys with locale-independent code-unit ordering', () => {
    const compare = vi.spyOn(String.prototype, 'localeCompare').mockImplementation(() => {
      throw new Error('locale-dependent comparison');
    });
    try {
      expect(
        canonicalJson({ ä: 1, a: 2, Z: 3, A: 4, nested: { z: 1, A: 2 }, omit: undefined }),
      ).toBe('{"A":4,"Z":3,"a":2,"nested":{"A":2,"z":1},"ä":1}');
      expect(canonicalJson({ a: { Z: 1, A: 2 } })).toBe(canonicalJson({ a: { A: 2, Z: 1 } }));
    } finally {
      compare.mockRestore();
    }
  });
  it('uses the longest prepared namespace regardless of rule order', () => {
    const short = rule('a'),
      long = rule('a:b', 'dogwood');
    expect(policyStateOwner([short, long], 'g:a:b:count')).toBe(long);
    expect(policyStateOwner([long, short], 'g:a:b:count')).toBe(long);
    expect(policyStateOwner([short, long], 'g:a:count')).toBe(short);
    expect(policyStateOwner([short], 'g:ab:count')).toBeUndefined();
  });
  it('selects declared builtin and exact shared usage periods', () => {
    expect(
      guardrailCounterWindow('subject_window', 'g:x:asked', now, {
        kind: 'builtin',
        builtin: 'user_monthly_cost_budget',
      }),
    ).toBe('2026-11');
    expect(
      guardrailCounterWindow('subject_window', 'g:x:asked', now, {
        kind: 'builtin',
        builtin: 'user_daily_cost_budget',
      }),
    ).toBe('2026-11-02');
    expect(
      guardrailCounterWindow('subject_window', 'g:x:asked', now, {
        kind: 'builtin',
        builtin: 'unknown_monthly_rule',
      }),
    ).toBe('2026-11-02');
    expect(guardrailCounterWindow('subject_window', 'monthly_cost_usd', now)).toBe('2026-11');
    expect(guardrailCounterWindow('subject_window', 'daily_cost_usd', now)).toBe('2026-11-02');
    expect(guardrailCounterWindow('subject_window', 'monthly_note', now)).toBe('2026-11-02');
    expect(guardrailCounterWindow('workspace_window', 'workspace_monthly_cost_usd', now)).toBe(
      '2026-11',
    );
  });
});
