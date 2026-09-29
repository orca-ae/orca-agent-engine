// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { isClaimStale, type EnvironmentClaim } from './environment-claims.js';

/**
 * Pure work-queue-stats decision logic for a single environment.
 *
 * The work-queue stats surface answers three questions about an environment's
 * `self_hosted` distribution backlog, mirroring Anthropic's claim/lease/stats
 * shape:
 *   - `depth`            — how many sessions are PENDING with no worker yet (no
 *                          runner minted: nobody has picked them up).
 *   - `in_flight`        — how many sessions a worker is already working on but
 *                          that are not complete (a launch in flight OR a
 *                          connected runner).
 *   - `worker_connected` — whether a worker is currently connected, derived from
 *                          the durable `environment_claims` heartbeat liveness.
 *
 * The partition is driven by the same `distribution_state` + `runner_id`
 * lifecycle the {@link SessionDistributor} maintains on the `sessions` row:
 *
 *   distribution_state | runner_id | bucket
 *   -------------------|-----------|----------------------------------------
 *   'pending'          | null      | depth      (create-time-stranded; no worker)
 *   'pending'          | set       | in_flight  (launch frame sent; runner spawning)
 *   'assigned'         | set       | in_flight  (runner connected; running)
 *   'failed'           | any       | excluded   (terminal)
 *   null (cloud)       | null      | excluded   (never distributed here)
 *
 * `depth` and `in_flight` are therefore a disjoint partition of the non-terminal
 * distributed sessions for the environment. FAILED + cloud/never-distributed
 * sessions are excluded by the DB counting query, so they never reach this
 * function — keeping the bucket totals here a straight sum.
 *
 * Keeping this pure (counts + claim row in, stats out) makes the partition and
 * the claim-liveness predicate unit-testable without a DB; the route supplies the
 * counts via a thin Drizzle aggregate and the claim via {@link EnvironmentClaimStore.getOwner}.
 */

/**
 * Per-distribution-state session counts for one environment, as produced by the
 * route's Drizzle aggregate. Each field is a non-negative count of NON-archived
 * sessions for the environment in that exact lifecycle position:
 *   - `pendingUnassigned` — distribution_state='pending' AND runner_id IS NULL.
 *   - `pendingAssigned`   — distribution_state='pending' AND runner_id IS NOT NULL.
 *   - `assigned`          — distribution_state='assigned'.
 */
export interface WorkStatsCounts {
  pendingUnassigned: number;
  pendingAssigned: number;
  assigned: number;
}

/**
 * The minimal claim shape the liveness check needs — just the heartbeat
 * watermark. A full {@link EnvironmentClaim} satisfies it; the route passes the
 * row from {@link EnvironmentClaimStore.getOwner}, or `null` when the environment
 * is unclaimed.
 */
export type WorkStatsClaim = Pick<EnvironmentClaim, 'lastPing'>;

/** The Anthropic-compatible work-queue stats wire shape. */
export interface WorkStats {
  /** Sessions pending with no worker yet (the queue depth). */
  depth: number;
  /** Sessions assigned to a worker but not yet complete (launch-in-flight + running). */
  in_flight: number;
  /** Whether a worker is currently connected (the claim heartbeat is live). */
  worker_connected: boolean;
}

/**
 * Whether an environment's worker is currently connected.
 *
 * A worker is connected iff a durable claim row exists for the environment AND
 * its heartbeat is within the staleness TTL. Reuses {@link isClaimStale} so the
 * liveness boundary here is byte-identical to the reaper's predicate — a claim is
 * "connected" exactly while it is not reapable. An unclaimed environment (`null`)
 * is never connected.
 *
 * @param claim The current claim row, or `null` when the environment is unclaimed.
 * @param ttlMs Heartbeat staleness TTL (ms) — the same value the reaper sweeps with.
 * @param now Instant to measure liveness against; defaults to the current time.
 */
export function isWorkerConnected(
  claim: WorkStatsClaim | null,
  ttlMs: number,
  now: Date = new Date(),
): boolean {
  return claim !== null && !isClaimStale(claim, ttlMs, now);
}

/**
 * Compute the work-queue stats for an environment from its session counts and
 * durable claim.
 *
 * `depth` is the pending-unassigned count; `in_flight` is the sum of
 * launch-in-flight (pending-with-runner) and connected-runner (assigned) counts;
 * `worker_connected` is the claim heartbeat liveness ({@link isWorkerConnected}).
 * Counts and liveness are orthogonal — a non-empty queue can have no connected
 * worker (all sessions stranded PENDING), and a connected worker can have an
 * empty queue.
 *
 * @param counts Per-distribution-state session counts for the environment.
 * @param claim The environment's current claim row, or `null` when unclaimed.
 * @param ttlMs Heartbeat staleness TTL (ms) for the liveness check.
 * @param now Instant to measure claim liveness against; defaults to the current time.
 */
export function computeWorkStats(
  counts: WorkStatsCounts,
  claim: WorkStatsClaim | null,
  ttlMs: number,
  now: Date = new Date(),
): WorkStats {
  return {
    depth: counts.pendingUnassigned,
    in_flight: counts.pendingAssigned + counts.assigned,
    worker_connected: isWorkerConnected(claim, ttlMs, now),
  };
}
