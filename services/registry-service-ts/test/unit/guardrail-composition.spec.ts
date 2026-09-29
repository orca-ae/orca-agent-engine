// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  composeGuardrails,
  type GuardrailRow,
  type GuardrailSources,
} from '../../src/domain/guardrail-composition.js';

function row(
  id: string,
  scope: 'organization' | 'workspace' | 'explicit',
  over: Partial<GuardrailRow> = {},
): GuardrailRow {
  return {
    id,
    name: id,
    enabled: true,
    phases: ['tool_call'],
    scope,
    rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    ...over,
  };
}

function sources(over: Partial<GuardrailSources> = {}): GuardrailSources {
  return {
    sessionIds: [],
    agentIds: [],
    subagentIds: {},
    visible: [],
    ...over,
  };
}

describe('tier ordering', () => {
  it('composes session, then agent, then workspace, then organization', () => {
    const result = composeGuardrails(
      sources({
        sessionIds: ['grd_s'],
        agentIds: ['grd_a'],
        visible: [
          row('grd_s', 'explicit'),
          row('grd_a', 'explicit'),
          row('grd_w', 'workspace'),
          row('grd_o', 'organization'),
        ],
      }),
    );
    expect(result.guardrails.map((g) => g.guardrail.id)).toEqual([
      'grd_s',
      'grd_a',
      'grd_w',
      'grd_o',
    ]);
    expect(result.guardrails.map((g) => g.tier)).toEqual([
      'session',
      'agent',
      'workspace',
      'organization',
    ]);
  });

  it('includes scoped guardrails without them being referenced', () => {
    const result = composeGuardrails(
      sources({ visible: [row('grd_w', 'workspace'), row('grd_o', 'organization')] }),
    );
    expect(result.guardrails).toHaveLength(2);
  });

  it('ignores an explicit guardrail nobody references', () => {
    const result = composeGuardrails(sources({ visible: [row('grd_x', 'explicit')] }));
    expect(result.guardrails).toHaveLength(0);
  });
});

describe('delegation cannot launder work past a coordinator', () => {
  it("applies a coordinator's guardrails alongside a subagent's own", () => {
    const result = composeGuardrails(
      sources({
        agentIds: ['grd_coord'],
        subagentIds: { sub_1: ['grd_sub'] },
        visible: [row('grd_coord', 'explicit'), row('grd_sub', 'explicit')],
      }),
    );
    const ids = result.guardrails.map((g) => g.guardrail.id);
    expect(ids).toContain('grd_coord');
    expect(ids).toContain('grd_sub');
  });

  it('tags a subagent-sourced guardrail with the subagent it came from', () => {
    const result = composeGuardrails(
      sources({ subagentIds: { sub_1: ['grd_sub'] }, visible: [row('grd_sub', 'explicit')] }),
    );
    expect(result.guardrails[0]?.subagentId).toBe('sub_1');
  });

  it('keeps the same child guardrail independently for every referenced child', () => {
    const result = composeGuardrails(
      sources({
        subagentIds: { sub_1: ['grd_sub'], sub_2: ['grd_sub'] },
        visible: [row('grd_sub', 'explicit')],
      }),
    );
    expect(result.guardrails.map((guardrail) => guardrail.subagentId)).toEqual(['sub_1', 'sub_2']);
  });

  it("leaves a coordinator's own guardrail untagged", () => {
    const result = composeGuardrails(
      sources({ agentIds: ['grd_coord'], visible: [row('grd_coord', 'explicit')] }),
    );
    expect(result.guardrails[0]?.subagentId).toBeUndefined();
  });
});

describe('exclusions', () => {
  it('skips a disabled guardrail', () => {
    const result = composeGuardrails(
      sources({ visible: [row('grd_w', 'workspace', { enabled: false })] }),
    );
    expect(result.guardrails).toHaveLength(0);
  });

  it('does not duplicate a guardrail referenced at more than one tier', () => {
    const result = composeGuardrails(
      sources({
        sessionIds: ['grd_x'],
        agentIds: ['grd_x'],
        visible: [row('grd_x', 'explicit')],
      }),
    );
    expect(result.guardrails).toHaveLength(1);
    // Kept at its most authoritative appearance: composition is monotonic, so
    // the earlier (weaker) tier adds nothing the later one does not.
    expect(result.guardrails[0]?.tier).toBe('session');
  });
});

describe('compilation', () => {
  it('carries statefulness and scope through from the type', () => {
    const result = composeGuardrails(
      sources({
        visible: [
          row('grd_c', 'workspace', {
            rule: { kind: 'builtin', builtin: 'max_tool_calls_per_session', params: { limit: 5 } },
          }),
        ],
      }),
    );
    expect(result.guardrails[0]?.stateful).toBe(true);
    expect(result.guardrails[0]?.stateScope).toBe('session');
  });

  it('reports a stored rule that no longer compiles rather than dropping it', () => {
    // The catalog changed under a stored rule. Silently omitting it would leave
    // a session running without a guardrail its operator believes is applied —
    // the failure has to be visible.
    const result = composeGuardrails(
      sources({
        visible: [
          row('grd_bad', 'workspace', { rule: { kind: 'builtin', builtin: 'no_such_type' } }),
        ],
      }),
    );
    expect(result.guardrails).toHaveLength(0);
    expect(result.invalid).toHaveLength(1);
    expect(result.invalid[0]?.id).toBe('grd_bad');
  });

  it('rejects a principal-wide budget stored at explicit scope', () => {
    const result = composeGuardrails(
      sources({
        sessionIds: ['grd_bad_scope'],
        visible: [
          row('grd_bad_scope', 'explicit', {
            rule: {
              kind: 'builtin',
              builtin: 'user_daily_cost_budget',
              params: { max_cost_usd: 5 },
            },
          }),
        ],
      }),
    );
    expect(result.guardrails).toHaveLength(0);
    expect(result.invalid[0]?.errors.join(' ')).toMatch(/scope/i);
  });
});
