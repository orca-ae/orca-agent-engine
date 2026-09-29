// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the TTL-bounded runner exit-report store.
//
// Exercises record / get / getVisible, owner-scoped visibility, TTL expiry, and
// the max-entries memory bound. A deterministic injectable clock drives the TTL.

import { describe, it, expect } from 'vitest';
import { RunnerExitReports } from '../../src/tunnel/runner-exit-reports.js';

/** A controllable clock for deterministic TTL assertions. */
function fakeClock(start = 1_000_000): { now: () => number; advance: (ms: number) => void } {
  let t = start;
  return {
    now: () => t,
    advance: (ms: number) => {
      t += ms;
    },
  };
}

describe('RunnerExitReports — record + get (unscoped)', () => {
  it('returns the recorded error for a known runner', () => {
    const reports = new RunnerExitReports();
    reports.record('runner_a', 'exited with code 1', {
      workerId: 'wc_1',
      owner: 'alice@example.com',
    });
    expect(reports.get('runner_a')).toBe('exited with code 1');
  });

  it('returns undefined for an unknown runner', () => {
    const reports = new RunnerExitReports();
    expect(reports.get('runner_missing')).toBeUndefined();
  });

  it('get() ignores owner scoping (caller is authorized by another means)', () => {
    const reports = new RunnerExitReports();
    reports.record('runner_a', 'boom', { workerId: 'wc_1', owner: 'alice@example.com' });
    // Unscoped read returns the error regardless of who asks.
    expect(reports.get('runner_a')).toBe('boom');
  });

  it('re-recording a runner overwrites the prior error', () => {
    const reports = new RunnerExitReports();
    reports.record('runner_a', 'first', { workerId: 'wc_1', owner: 'alice@example.com' });
    reports.record('runner_a', 'second', { workerId: 'wc_1', owner: 'alice@example.com' });
    expect(reports.get('runner_a')).toBe('second');
  });
});

describe('RunnerExitReports — getVisible (owner-scoped)', () => {
  it('returns the error to the owner', () => {
    const reports = new RunnerExitReports();
    reports.record('runner_a', 'boom', { workerId: 'wc_1', owner: 'alice@example.com' });
    expect(reports.getVisible('runner_a', 'alice@example.com')).toBe('boom');
  });

  it('withholds the error from a different user', () => {
    const reports = new RunnerExitReports();
    reports.record('runner_a', 'boom', { workerId: 'wc_1', owner: 'alice@example.com' });
    expect(reports.getVisible('runner_a', 'bob@example.com')).toBeUndefined();
  });

  it('returns the error when auth is disabled (no requesting user)', () => {
    const reports = new RunnerExitReports();
    reports.record('runner_a', 'boom', { workerId: 'wc_1', owner: 'alice@example.com' });
    // user_id undefined → single-user mode → visible.
    expect(reports.getVisible('runner_a', undefined)).toBe('boom');
  });

  it('returns the error for an owner-less report regardless of caller', () => {
    const reports = new RunnerExitReports();
    reports.record('runner_a', 'boom', { workerId: 'wc_1', owner: undefined });
    expect(reports.getVisible('runner_a', 'bob@example.com')).toBe('boom');
    expect(reports.getVisible('runner_a', undefined)).toBe('boom');
  });

  it('returns undefined for an unknown runner', () => {
    const reports = new RunnerExitReports();
    expect(reports.getVisible('runner_missing', 'alice@example.com')).toBeUndefined();
  });
});

describe('RunnerExitReports — TTL expiry', () => {
  it('expires an entry once its TTL elapses', () => {
    const clock = fakeClock();
    const reports = new RunnerExitReports({ ttlMs: 1000, now: clock.now });
    reports.record('runner_a', 'boom', { workerId: 'wc_1', owner: 'alice@example.com' });

    clock.advance(999);
    expect(reports.get('runner_a')).toBe('boom');

    clock.advance(2); // now 1001ms past record → expired
    expect(reports.get('runner_a')).toBeUndefined();
    expect(reports.getVisible('runner_a', 'alice@example.com')).toBeUndefined();
  });

  it('re-recording refreshes the TTL window', () => {
    const clock = fakeClock();
    const reports = new RunnerExitReports({ ttlMs: 1000, now: clock.now });
    reports.record('runner_a', 'first', { workerId: 'wc_1', owner: 'alice@example.com' });

    clock.advance(800);
    reports.record('runner_a', 'second', { workerId: 'wc_1', owner: 'alice@example.com' }); // fresh 1000ms window

    clock.advance(800); // 1600ms after first record, 800ms after the refresh
    expect(reports.get('runner_a')).toBe('second');
  });
});

describe('RunnerExitReports — max-entries bound', () => {
  it('evicts the oldest entry when capacity is exceeded', () => {
    const reports = new RunnerExitReports({ maxEntries: 2 });
    reports.record('runner_1', 'e1', { workerId: 'wc_1', owner: undefined });
    reports.record('runner_2', 'e2', { workerId: 'wc_1', owner: undefined });
    reports.record('runner_3', 'e3', { workerId: 'wc_1', owner: undefined }); // evicts runner_1 (oldest)

    expect(reports.get('runner_1')).toBeUndefined();
    expect(reports.get('runner_2')).toBe('e2');
    expect(reports.get('runner_3')).toBe('e3');
  });

  it('keeps the most recently recorded entries within the bound', () => {
    const reports = new RunnerExitReports({ maxEntries: 3 });
    for (let i = 1; i <= 5; i += 1) {
      reports.record(`runner_${i}`, `e${i}`, { workerId: 'wc_1', owner: undefined });
    }
    // Only the last 3 survive.
    expect(reports.get('runner_1')).toBeUndefined();
    expect(reports.get('runner_2')).toBeUndefined();
    expect(reports.get('runner_3')).toBe('e3');
    expect(reports.get('runner_4')).toBe('e4');
    expect(reports.get('runner_5')).toBe('e5');
  });
});
