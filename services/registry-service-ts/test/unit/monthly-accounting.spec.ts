// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  usageMonthlyCounterStateUpdates,
  utcMonthWindow,
  parseGuardrailStateBody,
} from '../../src/domain/usage-accounting.js';
const usage = {
  inputTokens: 1,
  outputTokens: 0,
  cacheReadInputTokens: 0,
  cacheCreationEphemeral1hInputTokens: 0,
  cacheCreationEphemeral5mInputTokens: 0,
};

describe('monthly usage counters', () => {
  it('uses UTC month boundaries and the immutable accepted cost', () => {
    expect(utcMonthWindow(new Date('2026-11-01T00:15:00+01:00'))).toBe('2026-10');
    expect(usageMonthlyCounterStateUpdates(usage, 1250000000n)).toEqual([
      { scope: 'subject_window', key: 'monthly_cost_usd', action: 'increment', value: 1.25 },
      {
        scope: 'workspace_window',
        key: 'workspace_monthly_cost_usd',
        action: 'increment',
        value: 1.25,
      },
    ]);
  });
  it('marks both buckets unpriced and never equates unpriced with zero', () => {
    expect(usageMonthlyCounterStateUpdates(usage, null)).toEqual([
      { scope: 'subject_window', key: 'monthly_cost_unpriced', action: 'set', value: 1 },
      {
        scope: 'workspace_window',
        key: 'workspace_monthly_cost_unpriced',
        action: 'set',
        value: 1,
      },
    ]);
    expect(usageMonthlyCounterStateUpdates({ ...usage, inputTokens: 0 }, null)).toEqual([]);
    expect(usageMonthlyCounterStateUpdates(usage, 0n)[0]?.value).toBe(0);
  });
  it('accepts numeric workspace state without a client-selected workspace subject', () => {
    expect(
      parseGuardrailStateBody({
        window: '2026-11',
        updates: [{ scope: 'workspace_window', key: 'g:one:approval', action: 'set', value: 1 }],
      }).ok,
    ).toBe(true);
    expect(
      parseGuardrailStateBody({
        window: '2026-11',
        updates: [{ scope: 'workspace_window', key: 'x', action: 'append', value: [] }],
      }).ok,
    ).toBe(false);
    expect(
      parseGuardrailStateBody({
        subject: '*',
        window: '2026-11',
        updates: [{ scope: 'subject_window', key: 'x', action: 'set', value: 1 }],
      }).ok,
    ).toBe(false);
  });
});
