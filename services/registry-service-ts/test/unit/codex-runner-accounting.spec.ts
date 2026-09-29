// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  compileCodexRequestGuardrails,
  parseCodexUsage,
  codexUsageEventId,
} from '../../src/domain/codex-runner-accounting.js';
import type { SnapshotGuardrail } from '../../src/domain/agent-snapshot.js';

const budget = (builtin: string, params: Record<string, unknown> = {}): SnapshotGuardrail => ({
  id: 'grd_budget',
  name: 'budget',
  tier: 'workspace',
  phases: ['request'],
  stateful: false,
  rule: { kind: 'builtin', builtin, params },
});
describe('Codex Registry request accounting contract', () => {
  it('compiles statefulness rather than trusting the snapshot flag', () => {
    const [rule] = compileCodexRequestGuardrails([
      budget('token_budget', { max_total_tokens: 10 }),
    ]);
    expect(rule?.stateful).toBe(true);
  });
  it('refuses soft thresholds and subagent budgets', () => {
    expect(() =>
      compileCodexRequestGuardrails([
        budget('cost_budget', { max_cost_usd: 1, ask_thresholds_usd: [0.5] }),
      ]),
    ).toThrow(/soft approval/);
    expect(() =>
      compileCodexRequestGuardrails([budget('subagent_cost_budget', { max_cost_usd: 1 })]),
    ).toThrow(/subagent/);
  });
  it('turns unpriced ask into denial while retaining explicit allow', () => {
    for (const on_unpriced of ['ask', 'deny', 'allow']) {
      const [rule] = compileCodexRequestGuardrails([
        budget('cost_budget', { max_cost_usd: 1, on_unpriced }),
      ]);
      expect(rule?.guardrail.rule).toMatchObject({
        params: { on_unpriced: on_unpriced === 'allow' ? 'allow' : 'deny' },
      });
    }
  });
  it('requires complete nonnegative integer usage without trusting metadata', () => {
    const usage = {
      input_tokens: 1,
      output_tokens: 2,
      cache_read_input_tokens: 3,
      cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
    };
    expect(parseCodexUsage(usage)?.inputTokens).toBe(1);
    for (const bad of [
      {},
      { ...usage, input_tokens: 1.5 },
      { ...usage, output_tokens: -1 },
      { ...usage, cache_creation: {} },
    ])
      expect(parseCodexUsage(bad)).toBeNull();
  });
  it('uses a stable scope-bound usage id for one accepted turn', () => {
    expect(codexUsageEventId('ws_a', 'ses_a', 'evt_a')).toBe(
      codexUsageEventId('ws_a', 'ses_a', 'evt_a'),
    );
    expect(codexUsageEventId('ws_a', 'ses_a', 'evt_a')).not.toBe(
      codexUsageEventId('ws_b', 'ses_a', 'evt_a'),
    );
  });
});
