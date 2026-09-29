// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the environment-launch lifecycle — the background
// provision->mint->startWorker->wait-online pipeline a `target=cloud`
// colocated session's create kicks off. Driven entirely against FAKES (a fake
// EnvironmentLauncher, a fake token issuer, a fake worker-online check, and an
// injectable clock/sleep), so the whole state machine — including the
// wait-online poll+timeout and the single-flight concurrency guard — is
// deterministic and network/DB-free.
//
// Behaviors pinned: the token is armed BEFORE start, any post-provision failure
// terminates + revokes, the wait-online timeout/poll is 120s/1s, and launches are
// single-flight (a racing caller awaits the in-flight launch instead of
// triggering a duplicate). See the module doc in `../../src/environment/launch/
// environment-launch-lifecycle.ts` for how these fit Orca's (persistent,
// multi-session) Environment model — in particular why "bind" is deliberately
// NOT part of this module (the `SessionDistributor` owns it).

import { describe, expect, it } from 'vitest';
import {
  DEFAULT_POLL_INTERVAL_MS,
  DEFAULT_WAIT_ONLINE_TIMEOUT_MS,
  EnvironmentLaunchLifecycle,
  type EnvironmentTokenIssuer,
  type WorkerOnlineCheck,
} from '../../src/environment/launch/environment-launch-lifecycle.js';
import type {
  EnvironmentLauncher,
  StartWorkerOptions,
} from '../../src/environment/launcher/types.js';

// ── Fakes ──────────────────────────────────────────────────

/** Records every call; provisions deterministic, distinct launcher-level ids. */
class FakeLauncher implements EnvironmentLauncher {
  readonly provisionCalls: string[] = [];
  readonly startWorkerCalls: Array<{ launcherEnvironmentId: string; opts: StartWorkerOptions }> =
    [];
  readonly terminateCalls: string[] = [];
  provisionShouldFail = false;
  startWorkerShouldFail = false;
  private provisionCounter = 0;

  async provision(name: string): Promise<string> {
    this.provisionCalls.push(name);
    if (this.provisionShouldFail) {
      throw new Error(`provision failed for ${name}`);
    }
    this.provisionCounter += 1;
    return `local_${this.provisionCounter}`;
  }

  async startWorker(launcherEnvironmentId: string, opts: StartWorkerOptions): Promise<void> {
    this.startWorkerCalls.push({ launcherEnvironmentId, opts });
    if (this.startWorkerShouldFail) {
      throw new Error(`startWorker failed for ${launcherEnvironmentId}`);
    }
  }

  async terminate(launcherEnvironmentId: string): Promise<void> {
    this.terminateCalls.push(launcherEnvironmentId);
  }

  async isRunning(): Promise<boolean> {
    return true;
  }
}

/** Records every mint/revoke; mints deterministic, distinct raw tokens. */
class FakeTokenIssuer implements EnvironmentTokenIssuer {
  readonly minted: string[] = [];
  readonly revoked: string[] = [];
  private mintCounter = 0;

  async mintEnvironmentToken(environmentId: string): Promise<string> {
    this.minted.push(environmentId);
    this.mintCounter += 1;
    return `et-fake-${this.mintCounter}`;
  }

  async revokeEnvironmentToken(environmentId: string): Promise<void> {
    this.revoked.push(environmentId);
  }
}

/** A settable "is a worker connected for this environment" view, like `WorkerRegistry.get`. */
class FakeWorkerOnline implements WorkerOnlineCheck {
  private readonly online = new Set<string>();
  markOnline(environmentId: string): void {
    this.online.add(environmentId);
  }
  markOffline(environmentId: string): void {
    this.online.delete(environmentId);
  }
  get(environmentId: string): unknown {
    return this.online.has(environmentId) ? { fakeConnection: true } : undefined;
  }
}

/** A controllable fake clock: `sleep` advances `now` by the requested ms. */
function fakeClock(startAt = 0): { now: () => number; sleep: (ms: number) => Promise<void> } {
  let current = startAt;
  return {
    now: () => current,
    sleep: async (ms: number) => {
      current += ms;
    },
  };
}

const REGISTRY_TUNNEL_URL = 'http://127.0.0.1:8080';
const ENV_ID = 'env_cloud_1';

// ── Tests ──────────────────────────────────────────────────

describe('EnvironmentLaunchLifecycle — defaults', () => {
  it('defaults the wait-online timeout to 120s and the poll interval to 1s', () => {
    expect(DEFAULT_WAIT_ONLINE_TIMEOUT_MS).toBe(120_000);
    expect(DEFAULT_POLL_INTERVAL_MS).toBe(1_000);
  });
});

describe('EnvironmentLaunchLifecycle.ensureLaunched — happy path', () => {
  it('provisions, mints a token, starts the worker, and resolves once the worker is observed online', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const clock = fakeClock();
    let sleepCalls = 0;
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
      waitOnlineTimeoutMs: 5_000,
      pollIntervalMs: 1_000,
      now: clock.now,
      sleep: async (ms) => {
        sleepCalls += 1;
        await clock.sleep(ms);
        // The worker "dials in" partway through the wait — proves the outcome
        // resolves via the POLL, not merely because it happened to already be
        // online before the first check.
        workerOnline.markOnline(ENV_ID);
      },
    });

    const outcome = await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });

    expect(outcome).toEqual({ status: 'launched', environmentId: ENV_ID });
    expect(launcher.provisionCalls).toEqual(['cloud-env-1']);
    expect(tokens.minted).toEqual([ENV_ID]);
    expect(launcher.startWorkerCalls).toHaveLength(1);
    const call = launcher.startWorkerCalls[0]!;
    // Started inside the LAUNCHER-level environment (provision's return value).
    expect(call.launcherEnvironmentId).toBe('local_1');
    // The raw token minted for THIS registry environment id — never the digest.
    expect(call.opts.token).toBe('et-fake-1');
    expect(call.opts.registryTunnelUrl).toBe(REGISTRY_TUNNEL_URL);
    // The registry's own environment id is what the worker announces back —
    // NOT the launcher-level id — so the worker-tunnel path/claim/WorkerRegistry
    // key all agree with what dispatch() later looks up.
    expect(call.opts.environmentId).toBe(ENV_ID);
    expect(call.opts.identity.workerName).toBe('cloud-env-1');
    // The mint happens BEFORE startWorker (order matters: the credential must
    // already resolve by the time the worker's first dial arrives).
    expect(tokens.minted.length).toBeLessThanOrEqual(launcher.startWorkerCalls.length + 1);
    // Exactly one poll tick was needed to observe the worker online.
    expect(sleepCalls).toBe(1);
  });

  it('mints the token strictly before starting the worker (credential must already resolve on first dial)', async () => {
    const order: string[] = [];
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const originalMint = tokens.mintEnvironmentToken.bind(tokens);
    tokens.mintEnvironmentToken = async (environmentId: string) => {
      order.push('mint');
      return originalMint(environmentId);
    };
    const originalStart = launcher.startWorker.bind(launcher);
    launcher.startWorker = async (launcherEnvironmentId: string, opts: StartWorkerOptions) => {
      order.push('startWorker');
      return originalStart(launcherEnvironmentId, opts);
    };
    const workerOnline = new FakeWorkerOnline();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
      sleep: async () => workerOnline.markOnline(ENV_ID),
    });

    await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });

    expect(order).toEqual(['mint', 'startWorker']);
  });
});

describe('EnvironmentLaunchLifecycle.ensureLaunched — already online', () => {
  it('is a no-op (no provision/mint/startWorker) when a worker is already connected for the environment', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    workerOnline.markOnline(ENV_ID);
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
    });

    const outcome = await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });

    expect(outcome).toEqual({ status: 'already_online', environmentId: ENV_ID });
    expect(launcher.provisionCalls).toEqual([]);
    expect(tokens.minted).toEqual([]);
  });
});

describe('EnvironmentLaunchLifecycle.ensureLaunched — wait-online timeout', () => {
  it('terminates the provisioned environment and revokes the token when the worker never comes online', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline(); // never marked online
    const clock = fakeClock();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
      waitOnlineTimeoutMs: 3_000,
      pollIntervalMs: 1_000,
      now: clock.now,
      sleep: clock.sleep,
    });

    const outcome = await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });

    expect(outcome.status).toBe('failed');
    expect(outcome.environmentId).toBe(ENV_ID);
    expect(outcome.error).toMatch(/online/i);
    // Terminates the LAUNCHER-level id that was actually provisioned.
    expect(launcher.terminateCalls).toEqual(['local_1']);
    // Revokes the registry environment's token so the leaked credential (it
    // was injected into a sandbox that never checked in) cannot later
    // authenticate an unrelated worker.
    expect(tokens.revoked).toEqual([ENV_ID]);
  });

  it('retries a fresh launch on the next call after a wait-online timeout (not permanently poisoned)', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const clock = fakeClock();
    let onlineOnSecondAttempt = false;
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
      waitOnlineTimeoutMs: 2_000,
      pollIntervalMs: 1_000,
      now: clock.now,
      sleep: async (ms) => {
        await clock.sleep(ms);
        if (onlineOnSecondAttempt) workerOnline.markOnline(ENV_ID);
      },
    });

    const first = await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });
    expect(first.status).toBe('failed');

    onlineOnSecondAttempt = true;
    const second = await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });
    expect(second.status).toBe('launched');
    // A fresh provision + mint happened for the retry (not reusing the dead
    // generation's launcher-level id or a stale token).
    expect(launcher.provisionCalls).toEqual(['cloud-env-1', 'cloud-env-1']);
    expect(tokens.minted).toEqual([ENV_ID, ENV_ID]);
  });
});

describe('EnvironmentLaunchLifecycle.ensureLaunched — provisioning itself fails', () => {
  it('fails cleanly with no terminate/revoke when provision() throws (nothing was armed yet)', async () => {
    const launcher = new FakeLauncher();
    launcher.provisionShouldFail = true;
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
    });

    const outcome = await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });

    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/provision/i);
    expect(tokens.minted).toEqual([]);
    expect(tokens.revoked).toEqual([]);
    expect(launcher.terminateCalls).toEqual([]);
  });

  it('terminates + revokes when startWorker() itself throws (after the token was already armed)', async () => {
    const launcher = new FakeLauncher();
    launcher.startWorkerShouldFail = true;
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
    });

    const outcome = await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });

    expect(outcome.status).toBe('failed');
    expect(launcher.terminateCalls).toEqual(['local_1']);
    expect(tokens.revoked).toEqual([ENV_ID]);
  });
});

describe('EnvironmentLaunchLifecycle — the launch tracker (single-flight)', () => {
  it('a racing concurrent ensureLaunched for the same environment awaits the in-flight launch instead of provisioning twice', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
      sleep: async () => workerOnline.markOnline(ENV_ID),
    });

    // Two "racing" callers — e.g. a second session created against the same
    // not-yet-provisioned environment while the first launch is in flight.
    // Fired without awaiting in between, so both start before either settles.
    const [a, b] = await Promise.all([
      lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' }),
      lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' }),
    ]);

    // Exactly ONE provision/mint/startWorker — the second caller rendezvoused
    // on the first's in-flight launch rather than triggering a duplicate
    // (which would double-spawn a worker and stomp the just-minted token).
    expect(launcher.provisionCalls).toHaveLength(1);
    expect(tokens.minted).toHaveLength(1);
    expect(launcher.startWorkerCalls).toHaveLength(1);
    // Both racing callers observe the SAME settled outcome (readiness), not a
    // "no runner yet" failure.
    expect(a).toEqual(b);
    expect(a.status).toBe('launched');
  });

  it('a racing relaunch for the same environment also rendezvouses on an in-flight ensureLaunched', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
      sleep: async () => workerOnline.markOnline(ENV_ID),
    });

    const [a, b] = await Promise.all([
      lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' }),
      lifecycle.relaunch(ENV_ID, { name: 'cloud-env-1' }),
    ]);

    expect(launcher.provisionCalls).toHaveLength(1);
    expect(launcher.terminateCalls).toEqual([]); // no PRIOR generation existed to tear down
    expect(a).toEqual(b);
  });

  it('does not single-flight across DIFFERENT environments (each gets its own launch)', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
      // Mark BOTH environments online on the first poll tick of either — if the
      // two launches were accidentally coalesced into one, only one of the two
      // environment ids would ever be provisioned in the first place.
      sleep: async () => {
        workerOnline.markOnline('env_a');
        workerOnline.markOnline('env_b');
      },
    });

    const [a, b] = await Promise.all([
      lifecycle.ensureLaunched('env_a', { name: 'cloud-env-a' }),
      lifecycle.ensureLaunched('env_b', { name: 'cloud-env-b' }),
    ]);

    expect(launcher.provisionCalls.sort()).toEqual(['cloud-env-a', 'cloud-env-b']);
    expect(a.environmentId).toBe('env_a');
    expect(b.environmentId).toBe('env_b');
  });
});

describe('EnvironmentLaunchLifecycle.relaunch — same environment identity, new generation', () => {
  it('terminates the old generation, provisions + mints a NEW generation under the SAME registry environment id', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
      sleep: async () => workerOnline.markOnline(ENV_ID),
    });

    // Generation 1.
    const first = await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });
    expect(first.status).toBe('launched');
    const gen1LauncherId = launcher.startWorkerCalls[0]!.launcherEnvironmentId;

    // The worker dies — the caller (some future "worker died" detector, out of
    // this module's scope) observes it offline and asks for a relaunch.
    workerOnline.markOffline(ENV_ID);

    // Generation 2, under the SAME environmentId.
    const second = await lifecycle.relaunch(ENV_ID, { name: 'cloud-env-1' });
    expect(second.status).toBe('launched');
    const gen2LauncherId = launcher.startWorkerCalls[1]!.launcherEnvironmentId;

    // A NEW launcher-level box (generation 2 ≠ generation 1)...
    expect(gen2LauncherId).not.toBe(gen1LauncherId);
    // ...but the SAME registry-facing environment identity throughout: the
    // launcher-level box churns while the durable identity — the environmentId
    // itself, with no separate host id — stays fixed.
    expect(launcher.startWorkerCalls[0]!.opts.environmentId).toBe(ENV_ID);
    expect(launcher.startWorkerCalls[1]!.opts.environmentId).toBe(ENV_ID);
    expect(launcher.startWorkerCalls[0]!.opts.identity.workerId).toBe(
      launcher.startWorkerCalls[1]!.opts.identity.workerId,
    );

    // The old generation was torn down (a best-effort, defensive terminate
    // before a relaunch's fresh provision).
    expect(launcher.terminateCalls).toEqual([gen1LauncherId]);

    // A fresh token was minted for the new generation — its digest overwrites
    // the prior one in place (EnvironmentTokenStore semantics), atomically
    // revoking generation 1's token; the lifecycle does not ALSO explicitly
    // revoke here (the mint IS the revoke, in place).
    expect(tokens.minted).toEqual([ENV_ID, ENV_ID]);
  });

  it('best-effort tolerates the old generation terminate failing (still proceeds to provision the new one)', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
      sleep: async () => workerOnline.markOnline(ENV_ID),
    });
    const first = await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });
    expect(first.status).toBe('launched');
    workerOnline.markOffline(ENV_ID);

    launcher.terminate = async () => {
      throw new Error('provider terminate API is down');
    };

    const second = await lifecycle.relaunch(ENV_ID, { name: 'cloud-env-1' });
    expect(second.status).toBe('launched');
  });
});

describe('EnvironmentLaunchLifecycle.terminate', () => {
  it('terminates the tracked launcher-level environment and revokes the token', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
      sleep: async () => workerOnline.markOnline(ENV_ID),
    });
    await lifecycle.ensureLaunched(ENV_ID, { name: 'cloud-env-1' });

    await lifecycle.terminate(ENV_ID);

    expect(launcher.terminateCalls).toEqual(['local_1']);
    expect(tokens.revoked).toEqual([ENV_ID]);
  });

  it('is safe to call for an environment this process never launched (nothing tracked)', async () => {
    const launcher = new FakeLauncher();
    const tokens = new FakeTokenIssuer();
    const workerOnline = new FakeWorkerOnline();
    const lifecycle = new EnvironmentLaunchLifecycle({
      launcher,
      tokens,
      workerOnline,
      registryTunnelUrl: REGISTRY_TUNNEL_URL,
    });

    await expect(lifecycle.terminate('env_never_launched_here')).resolves.toBeUndefined();
    expect(launcher.terminateCalls).toEqual([]);
    // Still revokes — a defensive no-op DB write in case a token WAS minted
    // by a different process/replica for this environment id.
    expect(tokens.revoked).toEqual(['env_never_launched_here']);
  });
});
