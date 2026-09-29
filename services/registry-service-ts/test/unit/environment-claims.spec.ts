// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import {
  isClaimStale,
  staleCutoff,
  shouldReplaceClaim,
  shouldReleaseClaim,
  buildClaimRow,
  startEnvironmentClaimReaper,
  type ClaimReaper,
  type EnvironmentClaim,
} from '../../src/domain/environment-claims.js';

/**
 * Pure claim-decision invariants, unit-tested without a DB. An environment is
 * owned by exactly one registry pod at a time; these cover the two decisions the
 * Drizzle layer leans on — newest-wins replacement and staleness — plus the row
 * a `claim` upsert persists. The DB-touching upsert/exclusivity/release behavior
 * is asserted in the infra-gated integration suite.
 */

function makeClaim(overrides: Partial<EnvironmentClaim> = {}): EnvironmentClaim {
  const at = new Date('2026-06-18T00:00:00.000Z');
  return {
    environmentId: 'env_abc',
    ownerPod: 'registry-0',
    workerConnId: 'conn-1',
    claimedAt: at,
    lastPing: at,
    ...overrides,
  };
}

describe('environment-claims — isClaimStale (heartbeat TTL boundary)', () => {
  const ttlMs = 30_000;

  it('is not stale while inside the TTL window', () => {
    const claim = makeClaim({ lastPing: new Date('2026-06-18T00:00:00.000Z') });
    const now = new Date('2026-06-18T00:00:20.000Z'); // 20s < 30s
    expect(isClaimStale(claim, ttlMs, now)).toBe(false);
  });

  it('is not stale at exactly the TTL boundary (strictly-greater-than is stale)', () => {
    const claim = makeClaim({ lastPing: new Date('2026-06-18T00:00:00.000Z') });
    const now = new Date('2026-06-18T00:00:30.000Z'); // now - lastPing == ttl
    expect(isClaimStale(claim, ttlMs, now)).toBe(false);
  });

  it('is stale one millisecond past the TTL boundary', () => {
    const claim = makeClaim({ lastPing: new Date('2026-06-18T00:00:00.000Z') });
    const now = new Date('2026-06-18T00:00:30.001Z'); // now - lastPing == ttl + 1ms
    expect(isClaimStale(claim, ttlMs, now)).toBe(true);
  });

  it('treats a future last_ping (clock skew) as fresh', () => {
    const claim = makeClaim({ lastPing: new Date('2026-06-18T00:01:00.000Z') });
    const now = new Date('2026-06-18T00:00:00.000Z'); // negative age
    expect(isClaimStale(claim, ttlMs, now)).toBe(false);
  });
});

describe('environment-claims — shouldReplaceClaim (newest-wins)', () => {
  const incoming = { ownerPod: 'registry-1', workerConnId: 'conn-2' };

  it('replaces when no claim currently exists', () => {
    expect(shouldReplaceClaim(null, incoming)).toBe(true);
  });

  it('replaces an existing claim held by a different pod (worker relocated)', () => {
    const existing = makeClaim({ ownerPod: 'registry-0', workerConnId: 'conn-1' });
    expect(shouldReplaceClaim(existing, incoming)).toBe(true);
  });

  it('replaces an existing claim on the same pod with a new connection (worker reconnected)', () => {
    const existing = makeClaim({ ownerPod: 'registry-1', workerConnId: 'conn-1' });
    expect(shouldReplaceClaim(existing, { ownerPod: 'registry-1', workerConnId: 'conn-2' })).toBe(
      true,
    );
  });

  it('replaces even an identical claim — newest-wins is unconditional (re-asserts ownership)', () => {
    const existing = makeClaim({ ownerPod: 'registry-1', workerConnId: 'conn-2' });
    expect(shouldReplaceClaim(existing, incoming)).toBe(true);
  });
});

describe('environment-claims — buildClaimRow (the persisted upsert row)', () => {
  it('stamps claimed_at and last_ping to the same now on a fresh claim', () => {
    const now = new Date('2026-06-18T12:00:00.000Z');
    const row = buildClaimRow('env_xyz', 'registry-2', 'conn-9', now);
    expect(row).toEqual({
      environmentId: 'env_xyz',
      ownerPod: 'registry-2',
      workerConnId: 'conn-9',
      claimedAt: now,
      lastPing: now,
    });
  });

  it('defaults now to the current time when omitted', () => {
    const before = Date.now();
    const row = buildClaimRow('env_xyz', 'registry-2', 'conn-9');
    const after = Date.now();
    expect(row.claimedAt.getTime()).toBeGreaterThanOrEqual(before);
    expect(row.claimedAt.getTime()).toBeLessThanOrEqual(after);
    // A fresh claim's two stamps are the same instant.
    expect(row.lastPing.getTime()).toBe(row.claimedAt.getTime());
  });
});

describe('environment-claims — shouldReleaseClaim (connection-scoped release)', () => {
  it('does not release an unclaimed environment (nothing to drop)', () => {
    expect(shouldReleaseClaim(null, 'conn-1')).toBe(false);
  });

  it('releases when the releasing connection still owns the claim', () => {
    const current = makeClaim({ workerConnId: 'conn-1' });
    expect(shouldReleaseClaim(current, 'conn-1')).toBe(true);
  });

  it('does NOT release when a newer connection has taken over (takeover-then-release race)', () => {
    // Worker A (conn-1) was superseded by worker B (conn-2); A's teardown must
    // not delete B's live claim.
    const livedByB = makeClaim({ ownerPod: 'registry-1', workerConnId: 'conn-2' });
    expect(shouldReleaseClaim(livedByB, 'conn-1')).toBe(false);
  });

  it('is connection-scoped, not pod-scoped: same pod, different connection cannot release', () => {
    const current = makeClaim({ ownerPod: 'registry-0', workerConnId: 'conn-2' });
    expect(shouldReleaseClaim(current, 'conn-1')).toBe(false);
  });
});

describe('environment-claims — staleCutoff (reaper boundary matches isClaimStale)', () => {
  const ttlMs = 30_000;

  it('returns now - ttl', () => {
    const now = new Date('2026-06-18T00:01:00.000Z');
    expect(staleCutoff(ttlMs, now).toISOString()).toBe('2026-06-18T00:00:30.000Z');
  });

  it('a claim is reapable (last_ping < cutoff) iff isClaimStale is true — strictly past TTL', () => {
    const now = new Date('2026-06-18T00:01:00.000Z');
    const cutoff = staleCutoff(ttlMs, now);

    // Exactly on the boundary: kept by both the predicate and the cutoff.
    const boundary = makeClaim({ lastPing: new Date('2026-06-18T00:00:30.000Z') });
    expect(isClaimStale(boundary, ttlMs, now)).toBe(false);
    expect(boundary.lastPing.getTime() < cutoff.getTime()).toBe(false);

    // 1ms past the boundary: reaped by both.
    const past = makeClaim({ lastPing: new Date('2026-06-18T00:00:29.999Z') });
    expect(isClaimStale(past, ttlMs, now)).toBe(true);
    expect(past.lastPing.getTime() < cutoff.getTime()).toBe(true);
  });
});

describe('environment-claims — startEnvironmentClaimReaper (interval sweeper)', () => {
  it('sweeps on the interval with the configured ttl and reports the count', async () => {
    vi.useFakeTimers();
    try {
      const reapStale = vi.fn<ClaimReaper['reapStale']>().mockResolvedValue(3);
      const onSweep = vi.fn();
      const handle = startEnvironmentClaimReaper(
        { reapStale },
        { ttlMs: 90_000, intervalMs: 1_000, onSweep },
      );

      expect(reapStale).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1_000);
      expect(reapStale).toHaveBeenCalledTimes(1);
      expect(reapStale).toHaveBeenCalledWith(90_000);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(reapStale).toHaveBeenCalledTimes(2);
      expect(onSweep).toHaveBeenCalledWith(3);

      handle.stop();
      await vi.advanceTimersByTimeAsync(5_000);
      // No further sweeps after stop().
      expect(reapStale).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps running after a failed sweep and reports it via onError', async () => {
    vi.useFakeTimers();
    try {
      const reapStale = vi
        .fn<ClaimReaper['reapStale']>()
        .mockRejectedValueOnce(new Error('db down'))
        .mockResolvedValue(0);
      const onError = vi.fn();
      const handle = startEnvironmentClaimReaper(
        { reapStale },
        { ttlMs: 1_000, intervalMs: 1_000, onError },
      );

      await vi.advanceTimersByTimeAsync(1_000);
      expect(onError).toHaveBeenCalledTimes(1);
      // Loop survives the error and sweeps again.
      await vi.advanceTimersByTimeAsync(1_000);
      expect(reapStale).toHaveBeenCalledTimes(2);

      handle.stop();
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not overlap sweeps when one is still in flight', async () => {
    vi.useFakeTimers();
    try {
      let resolveSweep: ((n: number) => void) | undefined;
      const reapStale = vi.fn<ClaimReaper['reapStale']>().mockImplementation(
        () =>
          new Promise<number>((resolve) => {
            resolveSweep = resolve;
          }),
      );
      const handle = startEnvironmentClaimReaper(
        { reapStale },
        { ttlMs: 1_000, intervalMs: 1_000 },
      );

      // First tick starts a sweep that never resolves yet.
      await vi.advanceTimersByTimeAsync(1_000);
      expect(reapStale).toHaveBeenCalledTimes(1);
      // Second tick fires while the first is in flight — must be skipped.
      await vi.advanceTimersByTimeAsync(1_000);
      expect(reapStale).toHaveBeenCalledTimes(1);

      // Let the in-flight sweep finish, then the next tick runs.
      resolveSweep?.(0);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(reapStale).toHaveBeenCalledTimes(2);

      handle.stop();
    } finally {
      vi.useRealTimers();
    }
  });
});
