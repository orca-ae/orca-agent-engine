// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  computeWorkStats,
  isWorkerConnected,
  type WorkStatsCounts,
  type WorkStatsClaim,
} from '../../src/domain/work-stats.js';

/**
 * Pure work-queue-stats decision logic, unit-tested without a DB. Given the
 * per-distribution-state session counts for one environment plus the durable
 * claim's heartbeat liveness, `computeWorkStats` partitions the work queue into
 * the Anthropic-compatible `{ depth, in_flight, worker_connected }` surface:
 *
 *   - `depth`           — sessions PENDING with no runner minted yet (nobody has
 *                         picked them up: the create-time-stranded set).
 *   - `in_flight`       — sessions a worker is already working on but that are
 *                         not complete: a launch in flight (PENDING with a runner
 *                         binding) OR a connected runner (ASSIGNED).
 *   - `worker_connected`— whether the environment's claim is live (a claim row
 *                         exists AND its heartbeat is within the staleness TTL).
 *
 * FAILED + cloud/never-distributed (null distribution_state) sessions are
 * neither pending nor in-flight, so the DB layer never counts them — they never
 * reach this function. The DB GROUP BY counting is asserted in the infra-gated
 * integration suite; here we pin the partition + the claim-liveness predicate.
 */

const TTL_MS = 90_000;

function counts(overrides: Partial<WorkStatsCounts> = {}): WorkStatsCounts {
  return {
    pendingUnassigned: 0,
    pendingAssigned: 0,
    assigned: 0,
    ...overrides,
  };
}

function liveClaim(overrides: Partial<WorkStatsClaim> = {}): WorkStatsClaim {
  return {
    lastPing: new Date('2026-06-18T00:00:00.000Z'),
    ...overrides,
  };
}

describe('work-stats — computeWorkStats depth/in_flight partition', () => {
  it('reports depth as the pending-unassigned count (nobody has picked them up)', () => {
    const stats = computeWorkStats(counts({ pendingUnassigned: 4 }), null, TTL_MS);
    expect(stats.depth).toBe(4);
    expect(stats.in_flight).toBe(0);
  });

  it('counts a pending session with a runner binding (launch in flight) as in_flight, not depth', () => {
    // A launch frame was sent and the runner is spawning/connecting — a worker
    // has picked the session up, so it is in-flight even while still PENDING.
    const stats = computeWorkStats(counts({ pendingAssigned: 3 }), null, TTL_MS);
    expect(stats.depth).toBe(0);
    expect(stats.in_flight).toBe(3);
  });

  it('counts an assigned (connected-runner) session as in_flight', () => {
    const stats = computeWorkStats(counts({ assigned: 2 }), null, TTL_MS);
    expect(stats.depth).toBe(0);
    expect(stats.in_flight).toBe(2);
  });

  it('sums launch-in-flight and connected-runner sessions into in_flight', () => {
    const stats = computeWorkStats(counts({ pendingAssigned: 3, assigned: 2 }), null, TTL_MS);
    expect(stats.in_flight).toBe(5);
  });

  it('partitions a mixed queue: depth and in_flight are disjoint and exact', () => {
    const stats = computeWorkStats(
      counts({ pendingUnassigned: 7, pendingAssigned: 2, assigned: 4 }),
      null,
      TTL_MS,
    );
    expect(stats.depth).toBe(7);
    expect(stats.in_flight).toBe(6);
  });

  it('reports an empty queue as all zeros', () => {
    const stats = computeWorkStats(counts(), null, TTL_MS);
    expect(stats).toEqual({ depth: 0, in_flight: 0, worker_connected: false });
  });
});

describe('work-stats — worker_connected from claim liveness', () => {
  it('is false when the environment is unclaimed (no claim row)', () => {
    const stats = computeWorkStats(counts({ pendingUnassigned: 1 }), null, TTL_MS);
    expect(stats.worker_connected).toBe(false);
  });

  it('is true when a claim exists and its heartbeat is within the TTL', () => {
    const now = new Date('2026-06-18T00:01:00.000Z');
    const claim = liveClaim({ lastPing: new Date('2026-06-18T00:00:30.000Z') }); // 30s < 90s
    const stats = computeWorkStats(counts(), claim, TTL_MS, now);
    expect(stats.worker_connected).toBe(true);
  });

  it('is true at exactly the TTL boundary (strictly-greater-than is stale)', () => {
    const now = new Date('2026-06-18T00:01:30.000Z');
    const claim = liveClaim({ lastPing: new Date('2026-06-18T00:00:00.000Z') }); // now - ping == ttl
    const stats = computeWorkStats(counts(), claim, TTL_MS, now);
    expect(stats.worker_connected).toBe(true);
  });

  it('is false one millisecond past the TTL boundary (claim heartbeat went stale)', () => {
    const now = new Date('2026-06-18T00:01:30.001Z');
    const claim = liveClaim({ lastPing: new Date('2026-06-18T00:00:00.000Z') }); // ttl + 1ms
    const stats = computeWorkStats(counts(), claim, TTL_MS, now);
    expect(stats.worker_connected).toBe(false);
  });

  it('treats a future last_ping (clock skew across replicas) as connected', () => {
    const now = new Date('2026-06-18T00:00:00.000Z');
    const claim = liveClaim({ lastPing: new Date('2026-06-18T00:01:00.000Z') }); // negative age
    const stats = computeWorkStats(counts(), claim, TTL_MS, now);
    expect(stats.worker_connected).toBe(true);
  });

  it('worker_connected is independent of queue depth (a live claim with an empty queue still connected)', () => {
    const now = new Date('2026-06-18T00:00:10.000Z');
    const claim = liveClaim({ lastPing: new Date('2026-06-18T00:00:05.000Z') });
    const stats = computeWorkStats(counts(), claim, TTL_MS, now);
    expect(stats).toEqual({ depth: 0, in_flight: 0, worker_connected: true });
  });

  it('a stale claim with a non-empty depth still reports the depth (counts and liveness are orthogonal)', () => {
    const now = new Date('2026-06-18T01:00:00.000Z');
    const claim = liveClaim({ lastPing: new Date('2026-06-18T00:00:00.000Z') }); // long stale
    const stats = computeWorkStats(counts({ pendingUnassigned: 5 }), claim, TTL_MS, now);
    expect(stats.depth).toBe(5);
    expect(stats.worker_connected).toBe(false);
  });
});

describe('work-stats — isWorkerConnected predicate (public symbol, in isolation)', () => {
  // `isWorkerConnected` is exported and reused directly by callers that only want
  // the liveness bit (no queue counts), so pin its contract at the symbol level
  // rather than only transitively through computeWorkStats: an unclaimed env is
  // never connected, and a claimed env is connected exactly while its heartbeat
  // is not stale — the boundary is byte-identical to isClaimStale (reused inside).
  it('returns false when the environment is unclaimed (claim is null)', () => {
    expect(isWorkerConnected(null, TTL_MS)).toBe(false);
  });

  it('returns false for a null claim regardless of the now argument', () => {
    expect(isWorkerConnected(null, TTL_MS, new Date('2026-06-18T00:00:00.000Z'))).toBe(false);
  });

  it('returns true when a claim heartbeat is within the TTL', () => {
    const now = new Date('2026-06-18T00:01:00.000Z');
    const claim = liveClaim({ lastPing: new Date('2026-06-18T00:00:30.000Z') }); // 30s < 90s
    expect(isWorkerConnected(claim, TTL_MS, now)).toBe(true);
  });

  it('returns true at exactly the TTL boundary (mirrors isClaimStale: == ttl is still live)', () => {
    const now = new Date('2026-06-18T00:01:30.000Z');
    const claim = liveClaim({ lastPing: new Date('2026-06-18T00:00:00.000Z') }); // now - ping == ttl
    expect(isWorkerConnected(claim, TTL_MS, now)).toBe(true);
  });

  it('returns false one millisecond past the TTL boundary (heartbeat went stale)', () => {
    const now = new Date('2026-06-18T00:01:30.001Z');
    const claim = liveClaim({ lastPing: new Date('2026-06-18T00:00:00.000Z') }); // ttl + 1ms
    expect(isWorkerConnected(claim, TTL_MS, now)).toBe(false);
  });

  it('treats a future last_ping (replica clock skew) as connected', () => {
    const now = new Date('2026-06-18T00:00:00.000Z');
    const claim = liveClaim({ lastPing: new Date('2026-06-18T00:01:00.000Z') }); // negative age
    expect(isWorkerConnected(claim, TTL_MS, now)).toBe(true);
  });

  it('defaults now to the current time when omitted (a just-pinged claim is connected)', () => {
    expect(isWorkerConnected(liveClaim({ lastPing: new Date() }), TTL_MS)).toBe(true);
  });
});

describe('work-stats — computeWorkStats defaults now to current time', () => {
  it('uses the current time when now is omitted (a just-pinged claim is connected)', () => {
    const claim = liveClaim({ lastPing: new Date() });
    const stats = computeWorkStats(counts(), claim, TTL_MS);
    expect(stats.worker_connected).toBe(true);
  });
});
