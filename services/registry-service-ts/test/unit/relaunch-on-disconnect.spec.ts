// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the worker-disconnect -> relaunch trigger (C2 secondary fix).
// Driven entirely against fakes (a fake target lookup, a fake stranded-work
// store, a fake "is a worker online" check, a fake relaunch trigger, and an
// injectable sleep), so the grace-period + attempt-cap state machine is
// deterministic and DB/network-free — mirrors
// `environment-launch-lifecycle.spec.ts`'s own fake-only testing style.

import { describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_DISCONNECT_GRACE_MS,
  DEFAULT_MAX_CONSECUTIVE_RELAUNCH_ATTEMPTS,
  buildOnWorkerDisconnect,
  type RelaunchTrigger,
} from '../../src/environment/launch/relaunch-on-disconnect.js';
import type { EnvironmentTargetLookup } from '../../src/tunnel/session-distributor.js';

const ENV_ID = 'env_disconnect_test';

/** Fake target lookup — a single mutable map, like the route's real seam resolving a row. */
class FakeTargetLookup implements EnvironmentTargetLookup {
  private targets = new Map<string, string | null>();

  set(environmentId: string, target: string | null): void {
    this.targets.set(environmentId, target);
  }

  async loadTarget(environmentId: string): Promise<string | null> {
    return this.targets.get(environmentId) ?? null;
  }
}

/** Fake stranded-work store — mirrors `DistributionSessionStore.loadPendingForEnvironment`. */
class FakeSessionsStore {
  private pending = new Map<string, Array<{ id: string }>>();

  setPending(environmentId: string, ids: string[]): void {
    this.pending.set(
      environmentId,
      ids.map((id) => ({ id })),
    );
  }

  async loadPendingForEnvironment(environmentId: string): Promise<Array<{ id: string }>> {
    return this.pending.get(environmentId) ?? [];
  }
}

/** Fake "is a worker connected" check — mirrors `WorkerRegistry.get` / `WorkerOnlineCheck`. */
class FakeWorkerOnline {
  private online = new Set<string>();

  markOnline(environmentId: string): void {
    this.online.add(environmentId);
  }

  markOffline(environmentId: string): void {
    this.online.delete(environmentId);
  }

  get(environmentId: string): unknown {
    return this.online.has(environmentId) ? {} : undefined;
  }
}

/** Records every relaunch call; returns a scripted outcome (defaults to 'launched'). */
class FakeRelaunchTrigger implements RelaunchTrigger {
  readonly calls: Array<{ environmentId: string; opts: { name: string } }> = [];
  nextStatus: 'launched' | 'failed' = 'launched';

  async relaunch(
    environmentId: string,
    opts: { name: string },
  ): Promise<{ status: string; error?: string }> {
    this.calls.push({ environmentId, opts });
    return this.nextStatus === 'launched'
      ? { status: 'launched' }
      : { status: 'failed', error: 'boom' };
  }
}

/** Instant fake sleep — asserts it was called with `graceMs`, without waiting real time. */
function instantSleep(calls: number[]): (ms: number) => Promise<void> {
  return async (ms: number) => {
    calls.push(ms);
  };
}

function buildHarness(opts?: { graceMs?: number; maxConsecutiveAttempts?: number }) {
  const environments = new FakeTargetLookup();
  const sessions = new FakeSessionsStore();
  const workerOnline = new FakeWorkerOnline();
  const lifecycle = new FakeRelaunchTrigger();
  const sleepCalls: number[] = [];
  const onWorkerDisconnect = buildOnWorkerDisconnect({
    lifecycle,
    environments,
    sessions,
    workerOnline,
    sleep: instantSleep(sleepCalls),
    ...(opts?.graceMs !== undefined ? { graceMs: opts.graceMs } : {}),
    ...(opts?.maxConsecutiveAttempts !== undefined
      ? { maxConsecutiveAttempts: opts.maxConsecutiveAttempts }
      : {}),
  });
  return { environments, sessions, workerOnline, lifecycle, sleepCalls, onWorkerDisconnect };
}

describe('buildOnWorkerDisconnect — target gate', () => {
  it('never relaunches a self_hosted environment', async () => {
    const h = buildHarness();
    h.environments.set(ENV_ID, 'self_hosted');
    h.sessions.setPending(ENV_ID, ['ses_1']);

    await h.onWorkerDisconnect(ENV_ID);

    expect(h.lifecycle.calls).toEqual([]);
  });

  it('never relaunches an unknown/archived environment (target resolves null)', async () => {
    const h = buildHarness();
    // FakeTargetLookup.loadTarget resolves null for anything unset — mirrors
    // the real EnvironmentTargetLookup's "gone/archived -> null" contract.
    h.sessions.setPending(ENV_ID, ['ses_1']);

    await h.onWorkerDisconnect(ENV_ID);

    expect(h.lifecycle.calls).toEqual([]);
  });
});

describe('buildOnWorkerDisconnect — grace period', () => {
  it('sleeps the grace period before deciding', async () => {
    const h = buildHarness();
    h.environments.set(ENV_ID, 'cloud');
    h.sessions.setPending(ENV_ID, ['ses_1']);

    await h.onWorkerDisconnect(ENV_ID);

    expect(h.sleepCalls).toEqual([DEFAULT_DISCONNECT_GRACE_MS]);
  });

  it('honors a custom graceMs override', async () => {
    const h = buildHarness({ graceMs: 500 });
    h.environments.set(ENV_ID, 'cloud');
    h.sessions.setPending(ENV_ID, ['ses_1']);

    await h.onWorkerDisconnect(ENV_ID);

    expect(h.sleepCalls).toEqual([500]);
  });

  it('does not relaunch if the worker reconnected on its own during the grace period', async () => {
    const h = buildHarness();
    h.environments.set(ENV_ID, 'cloud');
    h.sessions.setPending(ENV_ID, ['ses_1']);
    // Simulate the worker's OWN reconnect winning the race: by the time the
    // grace sleep resolves, workerOnline reports it back.
    h.workerOnline.markOnline(ENV_ID);

    await h.onWorkerDisconnect(ENV_ID);

    expect(h.lifecycle.calls).toEqual([]);
  });
});

describe('buildOnWorkerDisconnect — stranded-work gate', () => {
  it('does not relaunch when there is no stranded (pending, unbound) work', async () => {
    const h = buildHarness();
    h.environments.set(ENV_ID, 'cloud');
    // No setPending call — loadPendingForEnvironment resolves [].

    await h.onWorkerDisconnect(ENV_ID);

    expect(h.lifecycle.calls).toEqual([]);
  });

  it('relaunches when the worker is offline and stranded work exists', async () => {
    const h = buildHarness();
    h.environments.set(ENV_ID, 'cloud');
    h.sessions.setPending(ENV_ID, ['ses_1', 'ses_2']);

    await h.onWorkerDisconnect(ENV_ID);

    expect(h.lifecycle.calls).toEqual([{ environmentId: ENV_ID, opts: { name: ENV_ID } }]);
  });
});

describe('buildOnWorkerDisconnect — consecutive attempt cap', () => {
  it('stops attempting after maxConsecutiveAttempts consecutive non-launched outcomes', async () => {
    const h = buildHarness({ maxConsecutiveAttempts: 2 });
    h.environments.set(ENV_ID, 'cloud');
    h.sessions.setPending(ENV_ID, ['ses_1']);
    h.lifecycle.nextStatus = 'failed';

    await h.onWorkerDisconnect(ENV_ID); // attempt 1
    await h.onWorkerDisconnect(ENV_ID); // attempt 2
    await h.onWorkerDisconnect(ENV_ID); // would be attempt 3 — capped

    expect(h.lifecycle.calls).toHaveLength(2);
  });

  it('a successful relaunch resets the attempt counter for a later burst', async () => {
    const h = buildHarness({ maxConsecutiveAttempts: 1 });
    h.environments.set(ENV_ID, 'cloud');
    h.sessions.setPending(ENV_ID, ['ses_1']);

    h.lifecycle.nextStatus = 'launched';
    await h.onWorkerDisconnect(ENV_ID); // attempt 1 — succeeds, resets counter
    expect(h.lifecycle.calls).toHaveLength(1);

    // A fresh disconnect later (new problem) gets a fresh budget rather than
    // being permanently capped by the earlier (successful) attempt.
    h.lifecycle.nextStatus = 'launched';
    await h.onWorkerDisconnect(ENV_ID);
    expect(h.lifecycle.calls).toHaveLength(2);
  });

  it('tracks attempt counts independently per environment id', async () => {
    const h = buildHarness({ maxConsecutiveAttempts: 1 });
    h.environments.set('env_a', 'cloud');
    h.environments.set('env_b', 'cloud');
    h.sessions.setPending('env_a', ['ses_a']);
    h.sessions.setPending('env_b', ['ses_b']);
    h.lifecycle.nextStatus = 'failed';

    await h.onWorkerDisconnect('env_a');
    await h.onWorkerDisconnect('env_a'); // env_a capped after this
    await h.onWorkerDisconnect('env_b'); // env_b's own first attempt — must still fire

    expect(h.lifecycle.calls.filter((c) => c.environmentId === 'env_a')).toHaveLength(1);
    expect(h.lifecycle.calls.filter((c) => c.environmentId === 'env_b')).toHaveLength(1);
  });
});

describe('buildOnWorkerDisconnect — defaults', () => {
  it('exposes sane, documented default constants', () => {
    expect(DEFAULT_DISCONNECT_GRACE_MS).toBeGreaterThan(0);
    expect(DEFAULT_MAX_CONSECUTIVE_RELAUNCH_ATTEMPTS).toBeGreaterThan(0);
  });

  it('never throws even if the lifecycle rejects unexpectedly (fire-and-forget hook contract)', async () => {
    const h = buildHarness();
    h.environments.set(ENV_ID, 'cloud');
    h.sessions.setPending(ENV_ID, ['ses_1']);
    h.lifecycle.relaunch = vi.fn().mockRejectedValue(new Error('unexpected'));

    await expect(h.onWorkerDisconnect(ENV_ID)).resolves.toBeUndefined();
  });
});
