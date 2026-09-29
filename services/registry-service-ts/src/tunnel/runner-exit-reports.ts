// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// In-memory, TTL-bounded store of runner exit reports.
//
// Written by the worker tunnel when a `worker.runner_exited` frame arrives; read by
// the runner-status endpoint so a client polling a never-connecting runner
// learns *why* it died instead of timing out. In-memory and per-replica (same
// posture as the live tunnel sessions in the registry): the exit report and the
// status poll meet on the replica that held the host tunnel.
//
// Concurrency model: Node is single-threaded with one event loop, so the store
// needs no lock — every reader/writer runs inline on the one loop. The TTL +
// max-entries bound is preserved exactly: runner ids are unique per launch so
// entries never need invalidation, and the TTL is purely a memory bound for
// slow client retries.

import type { WorkerRunnerExitContext } from '@orca/harness-tunnel';

/**
 * A host daemon's report that a spawned runner died unexpectedly.
 *
 * - `error`: human-readable cause composed by the daemon (exit code, host-side
 *   log path, log tail), e.g.
 *   `"runner process exited with code 1 (log on host: ~/...)"`.
 * - `owner`: user who owns the host tunnel the report arrived on, e.g.
 *   `"alice@example.com"`. `undefined` when auth is disabled. Gates visibility:
 *   only the owner may read the report (the log tail can contain agent output).
 */
export interface RunnerExitReport {
  error: string;
  owner: string | undefined;
}

/**
 * How long a runner exit report stays answerable (ms).
 *
 * Reports only matter while a client is still waiting for the runner to come
 * online (a ~60s window today); 10 minutes covers slow retries with margin.
 */
const EXIT_REPORT_TTL_MS = 600_000;
/** How many reports are kept before the oldest is evicted (memory bound). */
const EXIT_REPORT_MAX_ENTRIES = 1024;

/** Internal record: the report plus the epoch-ms instant it expires. */
interface ExpiringReport {
  report: RunnerExitReport;
  expiresAt: number;
}

/**
 * TTL- and size-bounded store of runner exit reports.
 *
 * Each entry expires `ttlMs` after it is written and the store keeps at most
 * `maxEntries` live entries, evicting the oldest insertion when full. Expired
 * entries are pruned lazily on access (and eagerly when inserting at capacity),
 * so a stale report is never returned.
 */
export class RunnerExitReports {
  private readonly reports = new Map<string, ExpiringReport>();
  private readonly ttlMs: number;
  private readonly maxEntries: number;

  /**
   * @param opts.ttlMs Per-entry time-to-live in ms. Defaults to
   *   {@link EXIT_REPORT_TTL_MS} (10 minutes).
   * @param opts.maxEntries Maximum live entries before the oldest is evicted.
   *   Defaults to {@link EXIT_REPORT_MAX_ENTRIES} (1024).
   * @param opts.now Clock source (epoch ms), for deterministic tests. Defaults
   *   to `Date.now`.
   */
  constructor(
    private readonly opts: {
      ttlMs?: number;
      maxEntries?: number;
      now?: () => number;
    } = {},
  ) {
    this.ttlMs = opts.ttlMs ?? EXIT_REPORT_TTL_MS;
    this.maxEntries = opts.maxEntries ?? EXIT_REPORT_MAX_ENTRIES;
  }

  private now(): number {
    return this.opts.now?.() ?? Date.now();
  }

  /** Drop any entry whose TTL has elapsed as of `now`. */
  private pruneExpired(now: number): void {
    for (const [runnerId, entry] of this.reports) {
      if (entry.expiresAt <= now) {
        this.reports.delete(runnerId);
      }
    }
  }

  /** Read a live (non-expired) entry, pruning it if it has expired. */
  private liveEntry(runnerId: string, now: number): ExpiringReport | undefined {
    const entry = this.reports.get(runnerId);
    if (entry === undefined) {
      return undefined;
    }
    if (entry.expiresAt <= now) {
      this.reports.delete(runnerId);
      return undefined;
    }
    return entry;
  }

  /**
   * Store a runner exit report.
   *
   * @param runnerId The dead runner, e.g. `"runner_abc123"`.
   * @param error Human-readable cause from the host daemon.
   * @param ctx The authenticated worker-tunnel identity (`workerId` + resolved
   *   `owner`) the frame arrived on. Only `owner` is stored, and it gates read
   *   visibility so another tenant's runner reveals nothing — see
   *   {@link getVisible}.
   */
  record(runnerId: string, error: string, ctx: WorkerRunnerExitContext): void {
    const now = this.now();
    // Re-inserting an existing key must refresh its insertion order so the
    // max-entries eviction stays "oldest first" (Map preserves insertion order;
    // delete-then-set moves the key to the end).
    this.reports.delete(runnerId);
    this.pruneExpired(now);
    // Evict the oldest live entry while at capacity (the new key is not yet in
    // the map, so capacity is checked against the pruned size).
    while (this.reports.size >= this.maxEntries) {
      const oldest = this.reports.keys().next().value;
      if (oldest === undefined) {
        break;
      }
      this.reports.delete(oldest);
    }
    this.reports.set(runnerId, {
      report: { error, owner: ctx.owner },
      expiresAt: now + this.ttlMs,
    });
  }

  /**
   * Look up a report's error WITHOUT owner scoping.
   *
   * For callers that have already authorized access by another means (e.g. the
   * session snapshot, gated on session permission): the report pertains to that
   * session's own runner, so no separate owner check is needed. The runner
   * status endpoint — keyed only by `runnerId` with no session-level auth — must
   * use {@link getVisible} instead.
   *
   * @param runnerId Runner id, e.g. `"runner_abc123"`.
   * @returns The error message, or `undefined` when no live report exists.
   */
  get(runnerId: string): string | undefined {
    const entry = this.liveEntry(runnerId, this.now());
    return entry?.report.error;
  }

  /**
   * Look up a report, scoped to its owner.
   *
   * @param runnerId Runner id, e.g. `"runner_abc123"`.
   * @param userId The requesting user, or `undefined` when auth is disabled.
   * @returns The error message, or `undefined` when no live report exists or the
   *   caller doesn't own it (other users' runners reveal nothing — same
   *   enumeration-hiding posture as the status endpoint).
   */
  getVisible(runnerId: string, userId: string | undefined): string | undefined {
    const entry = this.liveEntry(runnerId, this.now());
    if (entry === undefined) {
      return undefined;
    }
    const { report } = entry;
    if (userId !== undefined && report.owner !== undefined && report.owner !== userId) {
      return undefined;
    }
    return report.error;
  }
}
