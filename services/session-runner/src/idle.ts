// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Runner inactivity watchdog — request shutdown after an idle window with no
// active agent work.
//
// A long-lived runner that has served no real tunnel WORK for the idle window AND
// has no agent turn in flight asks for a graceful shutdown, so an abandoned
// session's runner does not linger forever. The monitor reads two inputs:
//
//   - last-activity: the serve loop's `onActivity` hook touches a monotonic stamp
//     for each real server→runner work frame (NOT ping — keepalives never keep an
//     idle runner alive); the monitor measures the elapsed window against it.
//   - active-work: whether a turn is currently running. If the window expires while
//     a turn is in flight the monitor KEEPS WAITING and shuts down soon after the
//     work clears, unless new activity resets the timer — so a long turn is never
//     interrupted by the idle clock.
//
// A timeout of `0` (or negative) DISABLES the watchdog (the disable-by-zero
// contract). The clock + sleep are injected so the monitor is unit-testable with a
// controllable timer.

/** Structured logger seam. All optional. */
export interface IdleMonitorLogger {
  info?(obj: unknown, msg?: string): void;
}

/** Options for {@link runInactivityMonitor}. */
export interface InactivityMonitorOptions {
  /**
   * Idle window in seconds; `<= 0` disables the monitor (returns immediately).
   * The caller resolves the concrete value from config (default 1 hour).
   */
  idleTimeoutS: number;
  /** Returns the most recent real-activity time on the monitor's monotonic clock. */
  getLastActivity: () => number;
  /** Returns whether a turn is currently running (keeps the monitor waiting). */
  hasActiveWork: () => boolean;
  /** Called once to request a graceful runner shutdown when the window expires idle. */
  requestShutdown: () => void;
  /** Fires when `signal` aborts (the runner is stopping for another reason). */
  signal?: AbortSignal;
  /** Monotonic clock in seconds. Defaults to a `performance.now()`-derived clock. */
  now?: () => number;
  /** Sleep `seconds`. Defaults to a real timer. A test injects a controllable sleep. */
  sleep?: (seconds: number) => Promise<void>;
  /** Optional test override for the poll cadence (seconds); derived when omitted. */
  pollIntervalS?: number;
  /** Optional logger. */
  logger?: IdleMonitorLogger;
}

/** Upper bound on the derived poll cadence (seconds). */
const MAX_POLL_INTERVAL_S = 60;

/**
 * Run the inactivity monitor until it requests shutdown or `signal` aborts.
 *
 * Returns immediately (a no-op) when `idleTimeoutS <= 0`. Otherwise it loops:
 * compute the elapsed idle window; if it has expired AND no work is active, request
 * shutdown and return; if it has expired WHILE work is active, wait a poll interval
 * and re-check (so a turn in flight is never cut off); otherwise sleep until the
 * window would next expire and re-check (new activity in between resets the timer).
 */
export async function runInactivityMonitor(opts: InactivityMonitorOptions): Promise<void> {
  const { idleTimeoutS, getLastActivity, hasActiveWork, requestShutdown } = opts;
  if (idleTimeoutS <= 0) {
    return;
  }
  const now = opts.now ?? defaultNow;
  const sleep = opts.sleep ?? defaultSleep;
  const signal = opts.signal;
  // Bound the production cadence between 1s and the cap, derived from the window
  // (timeout/30, clamped to [1, MAX]); a test can pin it explicitly.
  const pollIntervalS =
    opts.pollIntervalS ?? Math.min(MAX_POLL_INTERVAL_S, Math.max(1, idleTimeoutS / 30));

  while (signal === undefined || !signal.aborted) {
    const elapsedS = now() - getLastActivity();
    if (elapsedS >= idleTimeoutS) {
      if (hasActiveWork()) {
        await sleep(pollIntervalS);
        continue;
      }
      opts.logger?.info?.(
        { elapsedS },
        'runner idle timeout reached with no active work; requesting shutdown',
      );
      requestShutdown();
      return;
    }
    // Sleep until the window would next expire (or one poll interval, whichever is
    // smaller) and re-check; activity in the meantime pushes lastActivity forward.
    await sleep(Math.min(pollIntervalS, idleTimeoutS - elapsedS));
  }
}

/** Default monotonic clock in seconds (immune to wall-clock jumps). */
function defaultNow(): number {
  return performance.now() / 1000;
}

/** Default real-timer sleep (seconds). */
function defaultSleep(seconds: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, Math.max(0, seconds) * 1000));
}
