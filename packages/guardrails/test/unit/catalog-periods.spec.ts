// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  getGuardrailCounterPeriod,
  listGuardrailTypes,
  GUARDRAIL_CATALOG,
} from '../../src/index.js';

describe('declared counter periods', () => {
  it('declares each windowed builtin period explicitly', () => {
    expect(getGuardrailCounterPeriod('user_daily_cost_budget')).toBe('day');
    expect(getGuardrailCounterPeriod('user_monthly_cost_budget')).toBe('month');
    expect(getGuardrailCounterPeriod('workspace_monthly_cost_budget')).toBe('month');
    for (const entry of GUARDRAIL_CATALOG.filter(
      (e) => e.stateScope === 'subject_window' || e.stateScope === 'workspace_window',
    ))
      expect(getGuardrailCounterPeriod(entry.name)).toBeDefined();
  });
  it('does not infer a period from a name substring', () => {
    expect(getGuardrailCounterPeriod('unknown_monthly_rule')).toBeUndefined();
    expect(getGuardrailCounterPeriod('cost_budget')).toBeUndefined();
  });
  it('keeps period metadata out of the public catalog shape', () => {
    expect(listGuardrailTypes().every((e) => !Object.hasOwn(e, 'counterPeriod'))).toBe(true);
    expect(GUARDRAIL_CATALOG.every((e) => !Object.hasOwn(e, 'counterPeriod'))).toBe(true);
  });
});
