// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the approval-parking registry — the client half of the uniform
// transcript approval (the runner-side approval-parking registry).
//
// When a tool needs human confirmation the runner PARKS a verdict (a Promise) keyed
// by the tool-use id and waits; the registry-delivered `user.tool_confirmation`
// (which rode the transcript) RESOLVES it so the agent proceeds (allow) or gets a
// clean denial (deny). These pin the park → resolve round-trip,
// allow vs deny, the bounded wait (timeout collapses to deny), idempotent /
// unknown-id resolves, the pending-count bookkeeping the ingest guard reads, and the
// cleanup contract (a settled park leaves no leaked entry). All in-process — no
// harness, no tunnel.

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { PendingApprovals } from '../../src/pending-approvals.js';

describe('PendingApprovals — park + resolve round-trip', () => {
  it('resolves a parked verdict to ALLOW when the confirmation accepts', async () => {
    const approvals = new PendingApprovals();
    const parked = approvals.park({ toolUseId: 'toolu_1' });
    // The verdict is delivered out-of-band (the confirmation route → resolve).
    const delivered = approvals.resolve('toolu_1', true);
    expect(delivered).toBe(true);
    await expect(parked).resolves.toEqual({ approved: true });
  });

  it('resolves a parked verdict to DENY when the confirmation denies', async () => {
    const approvals = new PendingApprovals();
    const parked = approvals.park({ toolUseId: 'toolu_2' });
    approvals.resolve('toolu_2', false);
    await expect(parked).resolves.toEqual({ approved: false });
  });

  it('routes each verdict to its own parked tool-use id (parallel tool calls)', async () => {
    const approvals = new PendingApprovals();
    const a = approvals.park({ toolUseId: 'toolu_a' });
    const b = approvals.park({ toolUseId: 'toolu_b' });
    // Deny a, allow b — each promise gets exactly its own verdict.
    approvals.resolve('toolu_b', true);
    approvals.resolve('toolu_a', false);
    await expect(a).resolves.toEqual({ approved: false });
    await expect(b).resolves.toEqual({ approved: true });
  });
});

describe('PendingApprovals — resolve edge cases', () => {
  it('resolve for an UNKNOWN tool-use id is a no-op (returns false)', () => {
    const approvals = new PendingApprovals();
    expect(approvals.resolve('toolu_missing', true)).toBe(false);
  });

  it('a SECOND resolve for the same id is a no-op (the verdict is already set)', async () => {
    const approvals = new PendingApprovals();
    const parked = approvals.park({ toolUseId: 'toolu_3' });
    expect(approvals.resolve('toolu_3', true)).toBe(true);
    // The verdict already landed — a late/duplicate confirmation does not flip it.
    expect(approvals.resolve('toolu_3', false)).toBe(false);
    await expect(parked).resolves.toEqual({ approved: true });
  });

  it('re-parking the same id overwrites the prior (stale) entry', async () => {
    const approvals = new PendingApprovals();
    const first = approvals.park({ toolUseId: 'toolu_4' });
    // A fresh park for the same id (a re-issued confirmation) replaces the routing
    // entry; resolving now settles only the SECOND park.
    const second = approvals.park({ toolUseId: 'toolu_4' });
    approvals.resolve('toolu_4', true);
    await expect(second).resolves.toEqual({ approved: true });
    // The first park is abandoned (its routing slot was taken) — settle it so the
    // test does not leak a pending promise, then assert it never got the verdict.
    let firstSettled = false;
    void first.then(() => {
      firstSettled = true;
    });
    await Promise.resolve();
    expect(firstSettled).toBe(false);
  });
});

describe('PendingApprovals — pending-count bookkeeping (the ingest guard reads this)', () => {
  it('reports a tool-use id as pending only between park and resolve', async () => {
    const approvals = new PendingApprovals();
    expect(approvals.hasPending('toolu_5')).toBe(false);
    const parked = approvals.park({ toolUseId: 'toolu_5' });
    expect(approvals.hasPending('toolu_5')).toBe(true);
    expect(approvals.hasPending()).toBe(true); // any pending
    approvals.resolve('toolu_5', true);
    await parked;
    expect(approvals.hasPending('toolu_5')).toBe(false);
    expect(approvals.hasPending()).toBe(false);
  });

  it('counts multiple outstanding verdicts and decrements as each resolves', async () => {
    const approvals = new PendingApprovals();
    const a = approvals.park({ toolUseId: 'toolu_x' });
    const b = approvals.park({ toolUseId: 'toolu_y' });
    expect(approvals.pendingCount()).toBe(2);
    approvals.resolve('toolu_x', true);
    await a;
    expect(approvals.pendingCount()).toBe(1);
    approvals.resolve('toolu_y', false);
    await b;
    expect(approvals.pendingCount()).toBe(0);
  });
});

describe('PendingApprovals — bounded wait (a user who walked away never pins the runner)', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('collapses to DENY on timeout when no verdict arrives in the budget', async () => {
    const approvals = new PendingApprovals();
    const parked = approvals.park({ toolUseId: 'toolu_slow', timeoutMs: 1000 });
    await vi.advanceTimersByTimeAsync(1000);
    // Treated as refused (a timeout collapses to a denial) — a clean denial.
    await expect(parked).resolves.toEqual({ approved: false, timedOut: true });
    // The timed-out entry was cleaned up — it no longer reports pending.
    expect(approvals.hasPending('toolu_slow')).toBe(false);
  });

  it('a verdict that arrives BEFORE the timeout wins (the timer is cleared)', async () => {
    const approvals = new PendingApprovals();
    const parked = approvals.park({ toolUseId: 'toolu_race', timeoutMs: 1000 });
    await vi.advanceTimersByTimeAsync(500);
    approvals.resolve('toolu_race', true);
    await expect(parked).resolves.toEqual({ approved: true });
    // Advancing past the original deadline does not flip the already-set verdict,
    // and resolving again is a no-op (the timer fired into a done entry, or not at all).
    await vi.advanceTimersByTimeAsync(1000);
    expect(approvals.resolve('toolu_race', false)).toBe(false);
  });

  it('uses the default budget when no per-park timeout is given', async () => {
    const approvals = new PendingApprovals({ defaultTimeoutMs: 2000 });
    const parked = approvals.park({ toolUseId: 'toolu_default' });
    await vi.advanceTimersByTimeAsync(1999);
    let settled = false;
    void parked.then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false); // not yet — under the default budget
    await vi.advanceTimersByTimeAsync(1);
    await expect(parked).resolves.toEqual({ approved: false, timedOut: true });
  });

  it('a non-positive timeout disables the deadline (waits indefinitely for a verdict)', async () => {
    const approvals = new PendingApprovals();
    const parked = approvals.park({ toolUseId: 'toolu_forever', timeoutMs: 0 });
    await vi.advanceTimersByTimeAsync(10_000);
    let settled = false;
    void parked.then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false); // no deadline — still parked
    approvals.resolve('toolu_forever', true);
    await expect(parked).resolves.toEqual({ approved: true });
  });
});

describe('PendingApprovals — cleanup + reset', () => {
  it('a resolved park leaves NO leaked routing entry', async () => {
    const approvals = new PendingApprovals();
    const parked = approvals.park({ toolUseId: 'toolu_clean' });
    approvals.resolve('toolu_clean', true);
    await parked;
    // No entry remains: a second resolve finds nothing, and the count is zero.
    expect(approvals.resolve('toolu_clean', true)).toBe(false);
    expect(approvals.pendingCount()).toBe(0);
  });

  it('reset() denies every outstanding verdict and clears the table', async () => {
    const approvals = new PendingApprovals();
    const a = approvals.park({ toolUseId: 'toolu_r1' });
    const b = approvals.park({ toolUseId: 'toolu_r2' });
    // A teardown (the harness is stopping) must not leave a tool call blocked forever:
    // every parked verdict resolves to a clean denial.
    approvals.reset();
    await expect(a).resolves.toEqual({ approved: false });
    await expect(b).resolves.toEqual({ approved: false });
    expect(approvals.pendingCount()).toBe(0);
  });
});

describe('PendingApprovals — pending signal stays in lockstep on EVERY exit path', () => {
  // No per-approval "resolved" wire event is published: the "is this session
  // awaiting approval" signal is the in-process count exposed via hasPending /
  // pendingCount. This test pins its lockstep guarantee — the count returns to zero
  // no matter HOW the park ends (verdict / timeout / cancel) — so a UI badge /
  // mid-turn ingest guard built on hasPending can never strand a stale "still
  // awaiting" after the awaiter is gone.
  it('a DELIVERED verdict clears the pending signal', async () => {
    const approvals = new PendingApprovals();
    const parked = approvals.park({ toolUseId: 'toolu_verdict' });
    expect(approvals.hasPending()).toBe(true);
    approvals.resolve('toolu_verdict', true);
    await parked;
    expect(approvals.hasPending()).toBe(false);
    expect(approvals.pendingCount()).toBe(0);
  });

  it('a TIMEOUT clears the pending signal', async () => {
    vi.useFakeTimers();
    try {
      const approvals = new PendingApprovals();
      const parked = approvals.park({ toolUseId: 'toolu_timeout', timeoutMs: 1000 });
      expect(approvals.hasPending()).toBe(true);
      await vi.advanceTimersByTimeAsync(1000);
      await parked;
      expect(approvals.hasPending()).toBe(false);
      expect(approvals.pendingCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('a TEARDOWN / cancel (reset) clears the pending signal', async () => {
    const approvals = new PendingApprovals();
    const parked = approvals.park({ toolUseId: 'toolu_cancel' });
    expect(approvals.hasPending()).toBe(true);
    approvals.reset();
    await parked;
    expect(approvals.hasPending()).toBe(false);
    expect(approvals.pendingCount()).toBe(0);
  });
});
