// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Environment-launch lifecycle — the background provision task a
// `target=cloud` colocated session's create kicks off.
//
// Pipeline (`ensureLaunched` / `relaunch`): provision the Environment via the
// injected `EnvironmentLauncher` → mint a fresh per-launch Environment Token
// (armed BEFORE the worker starts, so the credential already resolves on its
// first dial) → `startWorker` with the raw token + this registry's own tunnel
// URL + the REGISTRY environment id + identity → wait-online: poll the
// worker-online view until the worker has dialed in and registered, or time
// out. ANY failure from the moment the token is minted onward best-effort
// terminates the launcher-level environment and revokes the token, so a failed
// launch leaves neither a running box nor a live credential behind. A failure
// in `provision` itself needs no cleanup — nothing was armed yet.
//
// Single-flight: concurrent `ensureLaunched`/`relaunch` calls for the SAME
// environment id rendezvous on the one in-flight launch instead of triggering
// a duplicate (two workers racing to claim one environment id, or two mints
// stomping each other's digest) — a racing caller awaits readiness instead of
// failing. See below for why the tracker is keyed by ENVIRONMENT id, not
// session id, and why it clears on settle (success OR failure) rather than
// retaining a failure.
//
// ── What is deliberately NOT here, and why ──────────────────────────────
//
// "Bind" — the session-level `worker.launch_runner` dispatch that binds a
// session to a runner — is NOT part of this module. Once `ensureLaunched`
// gets a worker online, the EXISTING `SessionDistributor` already does the
// rest: the worker-tunnel route's connect hook calls `SessionDistributor.onWorkerConnect`
// for EVERY worker that dials in (self_hosted or cloud alike, now that the
// distributor is un-gated for `target=cloud`), which dispatches every session
// still PENDING with no runner binding for that environment — precisely the
// session whose create kicked off this launch. Re-implementing that dispatch
// here would risk a DOUBLE dispatch (two `worker.launch_runner` frames, two
// runners, for one session) racing the connect hook; reusing the existing,
// already-tested mechanism is both less code and provably race-safe (see
// `session-distributor.ts`'s module doc). This module's contract ends at "a
// worker is online for this environment" — getting a turn routed from there
// is the distributor's job, exactly as it already is for self_hosted.
//
// The launch tracker does NOT protect the turn-POST path. A message that
// arrives before the worker is online is not a failure here: `POST
// /v1/sessions/:id/events` durably appends to the transcript regardless of
// distribution state (persist-before-forward — see `session-event-bridge.ts`),
// and the owner-pod bridge tails from the beginning once a runner connects.
// So the tracker exists purely to prevent DUPLICATE launches when two callers
// race for the same not-yet-online environment — there is no turn to rescue.
//
// The tracker is keyed by ENVIRONMENT id, not session id: a `target=cloud`
// Environment is a durable, potentially multi-session resource — the SAME
// shape as `target=self_hosted` — so many sessions can share one in-flight
// (or already-online) launch.
//
// On settle the tracker entry is cleared REGARDLESS of outcome. Retaining a
// failure would let one transient provisioning error permanently wedge a
// shared, reusable environment for every future session that targets it, and
// nothing in the call graph would ever reset it. Clearing lets the NEXT
// caller (another session create, or the disconnect-triggered relaunch — see
// `relaunch-on-disconnect.ts`) simply retry.
//
// The launcher-level environment id (`provision`'s return value) is tracked
// ONLY in this process's memory, keyed by the registry environment id — so is
// the single-flight map. That is a known, explicit scope boundary: the
// registry is multi-replica-capable, and a launch racing across TWO replicas
// (both see "not online yet" and both provision) is not guarded here.
// `WorkerRegistry`'s newest-wins registration and the durable
// `environment_claims` table already make a double-worker outcome safe for
// ROUTING (one claim wins), but it would still leak a second launcher-level
// box. Cross-replica single-flighting belongs in the durable claim (or a new
// lease row), not in this in-memory map.

import type { EnvironmentLauncher, StartWorkerOptions } from '../launcher/types.js';

/** Default wait-online timeout (ms): how long a started worker has to dial in before the launch fails. */
export const DEFAULT_WAIT_ONLINE_TIMEOUT_MS = 120_000;
/** Default wait-online poll interval (ms). */
export const DEFAULT_POLL_INTERVAL_MS = 1_000;

/**
 * Mint/revoke seam this module needs from `EnvironmentTokenStore`
 * (`src/domain/environment-token-store.ts`). Carried as a narrow interface —
 * not the concrete class — so the lifecycle is unit-testable without a DB.
 */
export interface EnvironmentTokenIssuer {
  /** Mint + persist a fresh per-launch Environment Token; returns the raw token to forward once. */
  mintEnvironmentToken(environmentId: string, ttlMs?: number, now?: Date): Promise<string>;
  /** Revoke the environment's current Environment Token (best-effort cleanup on launch failure). */
  revokeEnvironmentToken(environmentId: string): Promise<void>;
}

/**
 * The "is a worker currently connected for this environment" view this
 * module polls during wait-online. Structurally the subset of
 * `WorkerRegistry` (`src/tunnel/worker-registry.ts`) this module needs — its
 * `get(environmentId)` already returns `undefined` for an offline
 * environment and a live connection object otherwise, exactly the predicate
 * wait-online needs. Carried as an interface so a unit test can fake "worker
 * online" without a real WebSocket.
 */
export interface WorkerOnlineCheck {
  get(environmentId: string): unknown;
}

/** Structured logger seam (a subset of the usual `req.log` / pino). All optional. */
export interface LifecycleLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/** Options for {@link EnvironmentLaunchLifecycle}. */
export interface EnvironmentLaunchLifecycleOptions {
  /** The provider backend (local, or a cloud sandbox backend) that actually provisions/starts/terminates. */
  launcher: EnvironmentLauncher;
  /** Mints/revokes the per-launch Environment Token. */
  tokens: EnvironmentTokenIssuer;
  /** Per-replica "is a worker connected for this environment" view wait-online polls. */
  workerOnline: WorkerOnlineCheck;
  /** Base URL of THIS registry's worker tunnel — forwarded to the launched worker as `StartWorkerOptions.registryTunnelUrl`. */
  registryTunnelUrl: string;
  /** Wait-online timeout (ms). Defaults to {@link DEFAULT_WAIT_ONLINE_TIMEOUT_MS}. */
  waitOnlineTimeoutMs?: number;
  /** Wait-online poll interval (ms). Defaults to {@link DEFAULT_POLL_INTERVAL_MS}. */
  pollIntervalMs?: number;
  /** Sleep primitive, injectable for deterministic tests. Defaults to a real `setTimeout`. */
  sleep?: (ms: number) => Promise<void>;
  /** Clock (epoch ms), injectable for deterministic tests. Defaults to `Date.now`. */
  now?: () => number;
  /** Optional structured logger. */
  logger?: LifecycleLogger;
}

/** Inputs to {@link EnvironmentLaunchLifecycle.ensureLaunched} / `.relaunch`. */
export interface EnvironmentLaunchOptions {
  /** Human-readable label: passed to `launcher.provision(name)` and as the worker's announced identity name. */
  name: string;
}

/** Terminal outcome of a launch attempt. */
export type LaunchStatus = 'already_online' | 'launched' | 'failed';

/** Result of {@link EnvironmentLaunchLifecycle.ensureLaunched} / `.relaunch`. */
export interface LaunchOutcome {
  status: LaunchStatus;
  environmentId: string;
  /** Failure detail when `status === 'failed'`. */
  error?: string;
}

/**
 * Background provision→mint→startWorker→wait-online lifecycle for one
 * `target=cloud` Environment, keyed by the REGISTRY environment id.
 *
 * See the module doc for the pipeline, the single-flight tracker and —
 * importantly — what this module deliberately does NOT do (bind a session to
 * a runner; that stays the existing `SessionDistributor`'s job).
 */
export class EnvironmentLaunchLifecycle {
  private readonly launcher: EnvironmentLauncher;
  private readonly tokens: EnvironmentTokenIssuer;
  private readonly workerOnline: WorkerOnlineCheck;
  private readonly registryTunnelUrl: string;
  private readonly waitOnlineTimeoutMs: number;
  private readonly pollIntervalMs: number;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly now: () => number;
  private readonly logger: LifecycleLogger | undefined;

  /** The launch tracker: in-flight launches keyed by REGISTRY environment id. */
  private readonly inFlight = new Map<string, Promise<LaunchOutcome>>();
  /** The current generation's launcher-level environment id, keyed by REGISTRY environment id. */
  private readonly launcherEnvironmentIds = new Map<string, string>();

  constructor(opts: EnvironmentLaunchLifecycleOptions) {
    this.launcher = opts.launcher;
    this.tokens = opts.tokens;
    this.workerOnline = opts.workerOnline;
    this.registryTunnelUrl = opts.registryTunnelUrl;
    this.waitOnlineTimeoutMs = opts.waitOnlineTimeoutMs ?? DEFAULT_WAIT_ONLINE_TIMEOUT_MS;
    this.pollIntervalMs = opts.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.sleep = opts.sleep ?? defaultSleep;
    this.now = opts.now ?? Date.now;
    this.logger = opts.logger;
  }

  /**
   * Ensure a worker is online for `environmentId`, launching one if not.
   *
   * Fast path: if a worker is already connected (per {@link WorkerOnlineCheck}),
   * resolves immediately with `'already_online'` — no provision/mint/startWorker.
   * Otherwise runs the full provision→mint→startWorker→wait-online pipeline,
   * single-flighted: a concurrent call for the SAME `environmentId` while one
   * is in flight awaits the SAME outcome rather than triggering a duplicate
   * launch.
   */
  async ensureLaunched(
    environmentId: string,
    opts: EnvironmentLaunchOptions,
  ): Promise<LaunchOutcome> {
    if (this.workerOnline.get(environmentId) !== undefined) {
      return { status: 'already_online', environmentId };
    }
    return this.singleFlight(environmentId, () => this.runLaunch(environmentId, opts, false));
  }

  /**
   * Re-provision a NEW generation for `environmentId` under the SAME
   * registry-facing identity: best-effort terminate the current generation's
   * launcher-level environment (if this process tracked one), then run the
   * same provision→mint→startWorker→wait-online pipeline. Callers use this
   * when they have already determined the environment's worker is dead
   * (unlike `ensureLaunched`, this does NOT check "already online" first —
   * the caller already knows the sandbox is gone).
   *
   * Single-flighted through the SAME tracker as {@link ensureLaunched}: a
   * racing `ensureLaunched`/`relaunch` for the same environment rendezvouses
   * on this call instead of double-provisioning.
   */
  async relaunch(environmentId: string, opts: EnvironmentLaunchOptions): Promise<LaunchOutcome> {
    return this.singleFlight(environmentId, () => this.runLaunch(environmentId, opts, true));
  }

  /**
   * Best-effort terminate the tracked launcher-level environment (if this
   * process launched/tracks one) and revoke the environment's token.
   * Idempotent — safe to call for an environment this process never
   * launched (a no-op terminate; the revoke is still issued defensively in
   * case a different process/replica minted a token for it).
   */
  async terminate(environmentId: string): Promise<void> {
    const launcherEnvironmentId = this.launcherEnvironmentIds.get(environmentId);
    this.launcherEnvironmentIds.delete(environmentId);
    if (launcherEnvironmentId !== undefined) {
      await this.safeTerminate(launcherEnvironmentId);
    }
    await this.safeRevoke(environmentId);
  }

  /**
   * The launch tracker. Runs `run()` for `environmentId` unless one is
   * already in flight, in which case the existing promise is returned
   * instead. The entry is removed once `run()` settles — success OR failure
   * — so a subsequent call always starts fresh (see the module doc for why a
   * failure is cleared rather than retained).
   *
   * Concurrency-safe under Node's single-threaded event loop without a lock:
   * the check (`inFlight.get`) and the set (`inFlight.set`) below have no
   * `await` between them, so two calls issued back-to-back (e.g. two
   * `Promise.all`-fired session creates) cannot both observe an empty map —
   * whichever executes first populates the entry before the second's
   * synchronous prefix runs.
   */
  private singleFlight(
    environmentId: string,
    run: () => Promise<LaunchOutcome>,
  ): Promise<LaunchOutcome> {
    const existing = this.inFlight.get(environmentId);
    if (existing !== undefined) {
      return existing;
    }
    const launch = run().finally(() => {
      this.inFlight.delete(environmentId);
    });
    this.inFlight.set(environmentId, launch);
    return launch;
  }

  /** The actual provision→mint→startWorker→wait-online pipeline (one generation). */
  private async runLaunch(
    environmentId: string,
    opts: EnvironmentLaunchOptions,
    relaunch: boolean,
  ): Promise<LaunchOutcome> {
    if (relaunch) {
      // Defensive: the old generation is normally already dead (that's why a
      // relaunch was asked for), but terminate it anyway, before provisioning
      // the replacement, so a transient tunnel blip can never leave two live
      // boxes claiming one environment id.
      const oldLauncherEnvironmentId = this.launcherEnvironmentIds.get(environmentId);
      if (oldLauncherEnvironmentId !== undefined) {
        await this.safeTerminate(oldLauncherEnvironmentId);
      }
    }

    let launcherEnvironmentId: string;
    try {
      launcherEnvironmentId = await this.launcher.provision(opts.name);
    } catch (err) {
      // Nothing was armed yet — no terminate/revoke needed, just report.
      this.logger?.error?.({ err, environmentId }, 'environment-launch: provision failed');
      return { status: 'failed', environmentId, error: describeError(err) };
    }
    this.launcherEnvironmentIds.set(environmentId, launcherEnvironmentId);

    try {
      // Arm the credential BEFORE starting the worker, so it already resolves
      // by the time the worker's first dial arrives. On a relaunch this
      // mint's fresh digest overwrites the prior one in place — atomically
      // revoking the dead generation's token (EnvironmentTokenStore semantics
      // — see its `mintEnvironmentToken` doc).
      const token = await this.tokens.mintEnvironmentToken(environmentId);
      const startOpts: StartWorkerOptions = {
        token,
        registryTunnelUrl: this.registryTunnelUrl,
        environmentId,
        // The registry environment id doubles as the durable per-launch
        // identity: stable across relaunches, unlike the launcher-level id.
        identity: { workerId: environmentId, workerName: opts.name },
      };
      await this.launcher.startWorker(launcherEnvironmentId, startOpts);
      await this.waitOnline(environmentId);
    } catch (err) {
      // Any failure from here on tears down what was just provisioned/armed:
      // terminate the box, then revoke the token.
      this.logger?.warn?.(
        { err, environmentId, launcherEnvironmentId },
        'environment-launch: launch failed after provisioning; tearing down',
      );
      await this.safeTerminate(launcherEnvironmentId);
      this.launcherEnvironmentIds.delete(environmentId);
      await this.safeRevoke(environmentId);
      return { status: 'failed', environmentId, error: describeError(err) };
    }

    return { status: 'launched', environmentId };
  }

  /**
   * Poll {@link WorkerOnlineCheck} until the worker for `environmentId` has
   * dialed in, or throw once {@link waitOnlineTimeoutMs} elapses. The loop
   * checks, then sleeps, bounded by a deadline computed once up front — so a
   * worker that is already online returns without a sleep.
   */
  private async waitOnline(environmentId: string): Promise<void> {
    const deadline = this.now() + this.waitOnlineTimeoutMs;
    while (this.now() < deadline) {
      if (this.workerOnline.get(environmentId) !== undefined) {
        return;
      }
      await this.sleep(this.pollIntervalMs);
    }
    throw new Error(
      `environment ${environmentId} worker did not come online within ${this.waitOnlineTimeoutMs}ms`,
    );
  }

  /** Best-effort launcher terminate — logs, never throws (a cleanup path must not itself fail the caller). */
  private async safeTerminate(launcherEnvironmentId: string): Promise<void> {
    try {
      await this.launcher.terminate(launcherEnvironmentId);
    } catch (err) {
      this.logger?.warn?.(
        { err, launcherEnvironmentId },
        'environment-launch: best-effort terminate failed',
      );
    }
  }

  /** Best-effort token revoke — logs, never throws (a cleanup path must not itself fail the caller). */
  private async safeRevoke(environmentId: string): Promise<void> {
    try {
      await this.tokens.revokeEnvironmentToken(environmentId);
    } catch (err) {
      this.logger?.warn?.({ err, environmentId }, 'environment-launch: best-effort revoke failed');
    }
  }
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function describeError(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
