// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, lt, sql } from 'drizzle-orm';
import type { DbClient } from '../persistence/postgres/client.js';
import { environmentClaims } from '../persistence/postgres/schema.js';

/**
 * Durable environment-claim model: an environment is claimed by exactly one
 * registry pod at a time, so a worker's tunnel terminates on the one replica
 * that owns its environment.
 *
 * The exclusive-owner invariant lives in the schema (the claim's primary key is
 * `environment_id`), so there is at most one row per environment. The pure
 * decision logic — newest-wins replacement and heartbeat staleness — is split
 * out here so it is unit-testable without a DB; the Drizzle operations
 * (`claim` upsert, `heartbeat`, `release`, `getOwner`) stay thin wrappers over a
 * single row write/read each.
 *
 * Claim semantics, enforced across registry replicas via the persisted row:
 * - `claim` is **newest-wins**: a fresh claim unconditionally replaces any
 *   existing one. A reconnecting or relocated worker takes over its environment
 *   from a pod that lagged on cleanup, rather than being rejected.
 * - `heartbeat` advances `last_ping` so a live owner is not reaped as stale. It
 *   is **connection-scoped**: a ping from a connection that has since been taken
 *   over (newest-wins) is a no-op and must not resurrect the stale owner.
 * - `release` is also **connection-scoped** for the same reason: it drops the
 *   claim only when the releasing worker still owns it. After worker B takes
 *   over (newest-wins), worker A's teardown calling `release` MUST NOT delete
 *   B's live claim — that would unclaim an environment that has a live owner,
 *   violating the one-exclusive-claim invariant from B's perspective.
 *   `shouldReleaseClaim` is the pure guard; `releaseUnconditional` exists for
 *   the operator/cascade path that intentionally ignores ownership.
 * - `isClaimStale` is the reaper's predicate: a claim whose last heartbeat is
 *   older than the TTL is abandonable. `reapStale` is the background sweeper
 *   that actually deletes those rows so a stale claim does not linger forever
 *   when no worker ever reconnects (newest-wins alone only reclaims a row when
 *   a *new* worker claims the same environment).
 * - `getOwner` reads the current owner.
 */

/** A persisted environment claim row. */
export interface EnvironmentClaim {
  environmentId: string;
  ownerPod: string;
  workerConnId: string;
  claimedAt: Date;
  lastPing: Date;
}

/** The columns a `claim` upsert writes (claimed_at / last_ping share one now). */
export interface EnvironmentClaimRow {
  environmentId: string;
  ownerPod: string;
  workerConnId: string;
  claimedAt: Date;
  lastPing: Date;
}

/** The identity an incoming claim asserts. */
export interface ClaimOwner {
  ownerPod: string;
  workerConnId: string;
}

/**
 * Whether a claim's heartbeat has gone stale.
 *
 * A claim is stale once the elapsed time since its last heartbeat strictly
 * exceeds the TTL (`now - last_ping > ttl`). At exactly the TTL boundary the
 * claim is still considered live, so a heartbeat that lands precisely on the
 * deadline is not reaped. A `last_ping` in the future (clock skew across
 * replicas) yields a negative age and is always fresh.
 *
 * @param claim The claim to test.
 * @param ttlMs Heartbeat time-to-live in milliseconds.
 * @param now Instant to measure against; defaults to the current time.
 */
export function isClaimStale(
  claim: Pick<EnvironmentClaim, 'lastPing'>,
  ttlMs: number,
  now: Date = new Date(),
): boolean {
  return now.getTime() - claim.lastPing.getTime() > ttlMs;
}

/**
 * The `last_ping` cutoff the reaper deletes strictly below.
 *
 * A claim is reapable exactly when {@link isClaimStale} holds:
 * `now - last_ping > ttl` ⟺ `last_ping < now - ttl`. This returns that
 * `now - ttl` instant so the bulk `reapStale` delete (`where last_ping < cutoff`)
 * uses the *same* boundary arithmetic as the per-row predicate — a claim sitting
 * exactly on the TTL boundary is kept by both. Deriving the SQL cutoff from one
 * place keeps the sweeper and the predicate from drifting apart.
 *
 * @param ttlMs Heartbeat time-to-live in milliseconds.
 * @param now Instant the sweep runs at; defaults to the current time.
 */
export function staleCutoff(ttlMs: number, now: Date = new Date()): Date {
  return new Date(now.getTime() - ttlMs);
}

/**
 * Whether an incoming claim should replace the current one (newest-wins).
 *
 * Replacement is unconditional: a fresh claim always wins, whether the
 * environment is unclaimed, held by a different pod (the worker relocated), held
 * by the same pod on a different connection (the worker reconnected), or even
 * re-asserting an identical claim. The single-owner invariant is preserved by
 * the upsert overwriting the one row keyed on `environment_id`.
 *
 * Both the existing and incoming claims are accepted for symmetry and for future
 * tie-break policies; newest-wins ignores them and always replaces.
 *
 * @param _current The existing claim, or `null` when the environment is unclaimed.
 * @param _incoming The identity the new claim asserts.
 */
export function shouldReplaceClaim(
  _current: EnvironmentClaim | null,
  _incoming: ClaimOwner,
): boolean {
  return true;
}

/**
 * Whether a worker's `release` should drop the current claim (connection-scoped).
 *
 * Release is asymmetric with claim and symmetric with heartbeat: a worker may
 * only release the claim it still holds. The releaser is identified by its
 * `workerConnId`; release succeeds only when that matches the current owner's
 * connection.
 *
 * The race this guards: worker A (conn-1) is superseded by worker B (conn-2) via
 * newest-wins; A's teardown then calls release. Without this guard the delete
 * keyed on `environment_id` alone would remove B's *live* claim, leaving the
 * environment unclaimed despite a live owner. With the guard A's release is a
 * no-op (A no longer owns the connection), so B keeps its claim.
 *
 * Releasing an unclaimed environment is a no-op (nothing to drop).
 *
 * @param current The existing claim, or `null` when the environment is unclaimed.
 * @param workerConnId The connection the releasing worker asserts it holds.
 */
export function shouldReleaseClaim(
  current: EnvironmentClaim | null,
  workerConnId: string,
): boolean {
  return current !== null && current.workerConnId === workerConnId;
}

/**
 * Build the row a `claim` upsert persists.
 *
 * A fresh claim stamps `claimed_at` and `last_ping` to the same instant — the
 * heartbeat clock starts at claim time.
 *
 * @param environmentId Environment being claimed.
 * @param ownerPod Registry replica taking ownership.
 * @param workerConnId Worker tunnel connection bound to the owner.
 * @param now Instant the claim is taken at; defaults to the current time.
 */
export function buildClaimRow(
  environmentId: string,
  ownerPod: string,
  workerConnId: string,
  now: Date = new Date(),
): EnvironmentClaimRow {
  return {
    environmentId,
    ownerPod,
    workerConnId,
    claimedAt: now,
    lastPing: now,
  };
}

/**
 * Durable claim store over the `environment_claims` table.
 *
 * Each method is a single row write or read; all replacement/staleness policy
 * lives in the pure functions above. Constructed with a `DbClient` so callers
 * share one connection pool.
 */
export class EnvironmentClaimStore {
  constructor(private readonly db: DbClient) {}

  /**
   * Claim an environment for a registry pod, newest-wins.
   *
   * Upserts the single row keyed on `environment_id`: if the environment is
   * already claimed (by any pod, on any connection), the existing claim is
   * overwritten, so a reconnecting or relocated worker takes over. `claimed_at`
   * and `last_ping` are reset to `now`.
   *
   * @returns The persisted claim.
   */
  async claim(
    environmentId: string,
    ownerPod: string,
    workerConnId: string,
    now: Date = new Date(),
  ): Promise<EnvironmentClaim> {
    const row = buildClaimRow(environmentId, ownerPod, workerConnId, now);
    const [claimed] = await this.db
      .insert(environmentClaims)
      .values(row)
      .onConflictDoUpdate({
        target: environmentClaims.environmentId,
        set: {
          ownerPod: row.ownerPod,
          workerConnId: row.workerConnId,
          claimedAt: row.claimedAt,
          lastPing: row.lastPing,
        },
      })
      .returning();
    // An upsert with RETURNING always yields exactly one row (insert or update);
    // the guard is for the type system, not a reachable runtime state.
    if (!claimed) throw new Error(`claim upsert returned no row for ${environmentId}`);
    return toClaim(claimed);
  }

  /**
   * Advance a claim's heartbeat watermark.
   *
   * Updates `last_ping` to `now` only when the claim is still owned by the same
   * worker connection — a heartbeat from a connection that has since been taken
   * over (newest-wins) is a no-op and must not resurrect the stale owner.
   *
   * @returns `true` when a matching claim was refreshed, `false` otherwise.
   */
  async heartbeat(
    environmentId: string,
    workerConnId: string,
    now: Date = new Date(),
  ): Promise<boolean> {
    const updated = await this.db
      .update(environmentClaims)
      .set({ lastPing: now })
      .where(
        sql`${environmentClaims.environmentId} = ${environmentId} and ${environmentClaims.workerConnId} = ${workerConnId}`,
      )
      .returning({ environmentId: environmentClaims.environmentId });
    return updated.length > 0;
  }

  /**
   * Release an environment's claim, connection-scoped (the worker teardown path).
   *
   * Deletes the row only when it is still owned by the releasing worker's
   * connection — symmetric with {@link heartbeat} and guarded by
   * {@link shouldReleaseClaim}. A worker whose claim was already taken over by a
   * newer connection (newest-wins) is a no-op here, so its teardown can never
   * delete the live owner's claim and unclaim an environment that has a live
   * owner.
   *
   * Idempotent: releasing an unclaimed environment, or one now owned by a
   * different connection, removes nothing and returns `false`.
   *
   * @returns `true` when this connection's claim was removed, `false` otherwise.
   */
  async release(environmentId: string, workerConnId: string): Promise<boolean> {
    const removed = await this.db
      .delete(environmentClaims)
      .where(
        and(
          eq(environmentClaims.environmentId, environmentId),
          eq(environmentClaims.workerConnId, workerConnId),
        ),
      )
      .returning({ environmentId: environmentClaims.environmentId });
    return removed.length > 0;
  }

  /**
   * Release an environment's claim regardless of which connection holds it.
   *
   * The operator/cascade escape hatch that intentionally ignores ownership —
   * e.g. forcibly evicting a wedged claim. The normal worker teardown path is
   * {@link release}, which is connection-scoped; this MUST NOT be used on the
   * worker path or the takeover-then-release race returns.
   *
   * Idempotent: releasing an unclaimed environment is a no-op.
   *
   * @returns `true` when a claim was removed, `false` when none existed.
   */
  async releaseUnconditional(environmentId: string): Promise<boolean> {
    const removed = await this.db
      .delete(environmentClaims)
      .where(eq(environmentClaims.environmentId, environmentId))
      .returning({ environmentId: environmentClaims.environmentId });
    return removed.length > 0;
  }

  /**
   * Reap every claim whose heartbeat has gone stale (background sweeper).
   *
   * Bulk-deletes all rows with `last_ping < now - ttl` — the same boundary as
   * {@link isClaimStale}, via {@link staleCutoff}. This is what gives the
   * staleness TTL effect on its own: without it a dead owner's row lingers until
   * some *new* worker happens to claim the same environment (newest-wins), and
   * `getOwner` would keep returning a dead owner indefinitely. A registry pod
   * runs this on an interval so abandoned claims are freed even when no worker
   * reconnects.
   *
   * @param ttlMs Heartbeat time-to-live in milliseconds.
   * @param now Instant the sweep runs at; defaults to the current time.
   * @returns The number of stale claims removed.
   */
  async reapStale(ttlMs: number, now: Date = new Date()): Promise<number> {
    const cutoff = staleCutoff(ttlMs, now);
    const removed = await this.db
      .delete(environmentClaims)
      .where(lt(environmentClaims.lastPing, cutoff))
      .returning({ environmentId: environmentClaims.environmentId });
    return removed.length;
  }

  /**
   * Read the current claim for an environment.
   *
   * @returns The claim, or `null` when the environment is unclaimed.
   */
  async getOwner(environmentId: string): Promise<EnvironmentClaim | null> {
    const rows = await this.db
      .select()
      .from(environmentClaims)
      .where(eq(environmentClaims.environmentId, environmentId))
      .limit(1);
    return rows[0] ? toClaim(rows[0]) : null;
  }
}

function toClaim(row: typeof environmentClaims.$inferSelect): EnvironmentClaim {
  return {
    environmentId: row.environmentId,
    ownerPod: row.ownerPod,
    workerConnId: row.workerConnId,
    claimedAt: row.claimedAt,
    lastPing: row.lastPing,
  };
}

/** Minimal sweeper surface a reaper drives (so the loop is unit-testable). */
export interface ClaimReaper {
  reapStale(ttlMs: number, now?: Date): Promise<number>;
}

/** Tunables for {@link startEnvironmentClaimReaper}. */
export interface EnvironmentClaimReaperOptions {
  /** Heartbeat-staleness TTL (ms); rows older than this are deleted. */
  ttlMs: number;
  /** How often the sweep runs (ms). Defaults to `ttlMs`. */
  intervalMs?: number;
  /** Invoked after each sweep with the number of rows reaped. */
  onSweep?: (reaped: number) => void;
  /** Invoked if a sweep throws — the loop keeps running regardless. */
  onError?: (err: unknown) => void;
}

/** Stops a running reaper loop. Idempotent. */
export interface ReaperHandle {
  stop(): void;
}

/**
 * Run {@link EnvironmentClaimStore.reapStale} on a fixed interval.
 *
 * This is the automatic production consumer of the staleness TTL: a registry
 * pod calls this at boot so abandoned claims are freed even when no worker ever
 * reconnects (newest-wins alone only reclaims a row when a *new* worker claims
 * the same environment). The interval is unref'd so it never keeps the process
 * alive on its own, and a failing sweep is reported via `onError` rather than
 * crashing the loop.
 *
 * @returns A handle whose `stop()` clears the interval (call on shutdown).
 */
export function startEnvironmentClaimReaper(
  reaper: ClaimReaper,
  options: EnvironmentClaimReaperOptions,
): ReaperHandle {
  const intervalMs = options.intervalMs ?? options.ttlMs;
  let running = false;
  const tick = (): void => {
    // Skip if a prior sweep is still in flight — sweeps must not overlap.
    if (running) return;
    running = true;
    void reaper
      .reapStale(options.ttlMs)
      .then((reaped) => options.onSweep?.(reaped))
      .catch((err) => options.onError?.(err))
      .finally(() => {
        running = false;
      });
  };
  const handle = setInterval(tick, intervalMs);
  // Don't let the sweep timer hold the event loop open.
  (handle as { unref?: () => void }).unref?.();
  return {
    stop(): void {
      clearInterval(handle);
    },
  };
}
