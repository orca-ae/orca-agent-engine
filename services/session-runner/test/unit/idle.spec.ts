// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner inactivity watchdog.
//
// The monitor requests a graceful shutdown after the idle window elapses with no
// active agent work; a turn in flight keeps it waiting; new activity resets the
// timer; a zero/negative timeout disables it. The clock + sleep are injected so
// the whole loop is deterministic — no real timers.

import { describe, it, expect } from 'vitest';
import { runInactivityMonitor } from '../../src/idle.js';

/** A controllable monotonic clock + a sleep that advances it. */
function fakeClock(): {
  now: () => number;
  sleep: (s: number) => Promise<void>;
  advance: (s: number) => void;
} {
  let t = 0;
  return {
    now: () => t,
    sleep: async (s: number) => {
      t += s;
    },
    advance: (s: number) => {
      t += s;
    },
  };
}

describe('runInactivityMonitor', () => {
  it('is a no-op when the timeout is zero (disabled)', async () => {
    let shutdown = false;
    await runInactivityMonitor({
      idleTimeoutS: 0,
      getLastActivity: () => 0,
      hasActiveWork: () => false,
      requestShutdown: () => {
        shutdown = true;
      },
    });
    expect(shutdown).toBe(false);
  });

  it('is a no-op when the timeout is negative (disabled)', async () => {
    let shutdown = false;
    await runInactivityMonitor({
      idleTimeoutS: -5,
      getLastActivity: () => 0,
      hasActiveWork: () => false,
      requestShutdown: () => {
        shutdown = true;
      },
    });
    expect(shutdown).toBe(false);
  });

  it('requests shutdown after the idle window elapses with no active work', async () => {
    const clock = fakeClock();
    let shutdown = false;
    // lastActivity stays at 0; the fake sleep advances the clock past the window.
    await runInactivityMonitor({
      idleTimeoutS: 10,
      getLastActivity: () => 0,
      hasActiveWork: () => false,
      requestShutdown: () => {
        shutdown = true;
      },
      now: clock.now,
      sleep: clock.sleep,
      pollIntervalS: 1,
    });
    expect(shutdown).toBe(true);
  });

  it('keeps waiting while a turn is in flight, then shuts down once work clears', async () => {
    const clock = fakeClock();
    let shutdown = false;
    let polls = 0;
    // Active for the first few polls past the window, then clears.
    const hasActiveWork = (): boolean => {
      polls += 1;
      return polls <= 3;
    };
    await runInactivityMonitor({
      idleTimeoutS: 10,
      getLastActivity: () => 0,
      hasActiveWork,
      requestShutdown: () => {
        shutdown = true;
      },
      now: clock.now,
      sleep: clock.sleep,
      pollIntervalS: 1,
    });
    expect(shutdown).toBe(true);
    // It polled the active gate more than once (it waited through the active window).
    expect(polls).toBeGreaterThan(3);
  });

  it('resets the timer when activity advances, never shutting down while busy', async () => {
    const clock = fakeClock();
    let shutdown = false;
    let lastActivity = 0;
    let loops = 0;
    // Each time the monitor sleeps, bump activity forward so the window never
    // elapses for the first several iterations, then freeze it so it finally fires.
    const sleep = async (s: number): Promise<void> => {
      clock.advance(s);
      loops += 1;
      if (loops < 5) {
        lastActivity = clock.now(); // fresh activity: resets the idle window.
      }
    };
    await runInactivityMonitor({
      idleTimeoutS: 10,
      getLastActivity: () => lastActivity,
      hasActiveWork: () => false,
      requestShutdown: () => {
        shutdown = true;
      },
      now: clock.now,
      sleep,
      pollIntervalS: 3,
    });
    // It eventually shut down (once activity stopped resetting), proving the reset
    // path held it off while activity kept arriving.
    expect(shutdown).toBe(true);
    expect(loops).toBeGreaterThanOrEqual(5);
  });

  it('exits without shutdown when the abort signal fires first', async () => {
    const clock = fakeClock();
    let shutdown = false;
    const ac = new AbortController();
    let polls = 0;
    const sleep = async (s: number): Promise<void> => {
      clock.advance(s);
      polls += 1;
      if (polls >= 2) {
        ac.abort();
      }
    };
    await runInactivityMonitor({
      idleTimeoutS: 1_000_000, // huge window so only the abort can end the loop
      getLastActivity: () => clock.now(), // always "just active" so it never fires
      hasActiveWork: () => false,
      requestShutdown: () => {
        shutdown = true;
      },
      signal: ac.signal,
      now: clock.now,
      sleep,
      pollIntervalS: 1,
    });
    expect(shutdown).toBe(false);
  });
});
