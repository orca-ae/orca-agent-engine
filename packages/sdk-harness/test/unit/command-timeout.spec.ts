// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import {
  sdkCommandTimeoutMs,
  sdkExecutionBudgetMs,
  sdkTimeoutSignal,
} from '../../src/command-timeout.js';

afterEach(() => vi.useRealTimers());

it.each([null, 0, -1, 1.5, '900000', NaN, Infinity, 2_147_483_648])(
  'rejects malformed or expired budget %s',
  (value) => expect(() => sdkExecutionBudgetMs(value)).toThrow('Invalid SDK execution budget'),
);

it('preserves legacy submit defaults and keeps control waits independent of the turn', () => {
  expect(sdkExecutionBudgetMs(undefined)).toBe(600_000);
  expect(sdkCommandTimeoutMs({ type: 'submit' })).toBe(660_000);
  expect(sdkCommandTimeoutMs({ type: 'submit', executionBudgetMs: 1_200_000 })).toBe(1_260_000);
  for (const type of ['start', 'refresh', 'interrupt', 'stop', 'tool_result', 'archive_fence'])
    expect(sdkCommandTimeoutMs({ type, executionBudgetMs: 1_200_000 })).toBe(60_000);
});

it('supports maximum configured budget plus drain headroom without overflowing Node timers', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
  const watchdog = sdkTimeoutSignal(
    sdkCommandTimeoutMs({ type: 'submit', executionBudgetMs: 2_147_483_647 }),
  );
  await vi.advanceTimersByTimeAsync(2_147_483_647);
  expect(watchdog.signal.aborted).toBe(false);
  await vi.advanceTimersByTimeAsync(59_999);
  expect(watchdog.signal.aborted).toBe(false);
  await vi.advanceTimersByTimeAsync(1);
  expect(watchdog.signal.aborted).toBe(true);
  watchdog.dispose();
});

it.each([900_000, -900_000])(
  'keeps control and chunked watchdog durations across wall clock correction %s',
  async (correction) => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
    const control = sdkTimeoutSignal(60_000);
    const turn = sdkTimeoutSignal(2_147_483_647 + 60_000);
    await vi.advanceTimersByTimeAsync(30_000);
    vi.setSystemTime(Date.now() + correction);
    await vi.advanceTimersByTimeAsync(29_999);
    expect(control.signal.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(control.signal.aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(2_147_483_647 - 60_000);
    expect(turn.signal.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(59_999);
    expect(turn.signal.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(turn.signal.aborted).toBe(true);
    control.dispose();
    turn.dispose();
    expect(vi.getTimerCount()).toBe(0);
  },
);
