// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { applyGuardrailUsageAcknowledgment, InMemoryGuardrailStateStore } from '../../src/state.js';

describe('authoritative usage acknowledgment', () => {
  it('clears expired window approvals without resetting session counters', () => {
    const store = new InMemoryGuardrailStateStore({
      session: { tool_calls: 4 },
      subject_window: { 'g:monthly:approved': 5, monthly_cost_usd: 20 },
      workspace_window: { 'g:workspace:approved': 10 },
    });
    applyGuardrailUsageAcknowledgment(store, {
      session_cost_usd: 2,
      subject_window: { monthly_cost_usd: 1 },
      workspace_window: {},
    });
    expect(store.read('session')).toEqual({ tool_calls: 4, session_cost_usd: 2 });
    expect(store.read('subject_window')).toEqual({ monthly_cost_usd: 1 });
    expect(store.read('workspace_window')).toEqual({});
  });
  it('handles legacy flattened acknowledgments and every unpriced flag in its own scope', () => {
    const store = new InMemoryGuardrailStateStore();
    applyGuardrailUsageAcknowledgment(store, {
      daily_cost_unpriced: 1,
      monthly_cost_usd: 3,
      workspace_monthly_cost_unpriced: 1,
    });
    expect(store.read('subject_window')).toEqual({ daily_cost_unpriced: 1, monthly_cost_usd: 3 });
    expect(store.read('workspace_window')).toEqual({ workspace_monthly_cost_unpriced: 1 });
  });
});
