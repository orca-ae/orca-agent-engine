// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Worker-disconnect -> relaunch trigger for `target=cloud` Environments (C2
// secondary fix) — the one clean, already-existing signal
// `EnvironmentLaunchLifecycle.relaunch()` can safely hang off, without
// inventing a new "is the worker actually dead" detector.
//
// `EnvironmentLaunchLifecycle.relaunch()` (terminate the dead generation,
// re-provision a fresh one under the SAME environment id — see that module's
// doc) has existed since A3b with no production caller: it deliberately does
// not self-trigger, leaving "when is a relaunch warranted" to a caller that
// already knows the worker is dead. The worker-tunnel route's
// `onWorkerDisconnect` hook (`server.ts`'s `BuildAppOptions.onWorkerDisconnect`,
// wired through `registerWorkerTunnelRoutes` in `worker-tunnel.routes.ts`)
// already fires exactly that "this environment's worker tunnel just closed"
// signal, for EVERY environment-worker tunnel — self_hosted and cloud alike,
// the endpoint itself is target-agnostic. That makes it this port's one clean
// auto-recovery trigger; `server.ts` composes {@link buildOnWorkerDisconnect}'s
// output with that hook exactly like it already composes the OTHER worker/
// runner tunnel hooks (`onWorkerConnect`, `onRunnerConnect`, `onRunnerDisconnect`).
//
// Deliberately narrow, reusing ONLY signals that already exist:
//   - `target=cloud` ONLY (via {@link EnvironmentTargetLookup}, the SAME seam
//     `SessionDistributor.onWorkerConnect` already uses to resolve a real
//     target). A self_hosted worker is operator-launched — a human or their
//     own process manager decides whether/when to restart it — so this
//     registry must never provision a box on that environment's behalf.
//   - Only when there is STRANDED work, via
//     `DistributionSessionStore.loadPendingForEnvironment` — the EXACT SAME
//     query `SessionDistributor.onWorkerConnect` already runs to decide "is
//     there work for this environment" (there: to DISPATCH it; here: to
//     RELAUNCH a fresh worker so a SUBSEQUENT connect can dispatch it). An
//     environment with no pending, unbound sessions has nothing this
//     relaunch would help, so provisioning a fresh (billed) box would be
//     pure waste.
//   - A short GRACE period before deciding, re-checking
//     {@link WorkerOnlineCheck} (the SAME "is a worker connected" predicate
//     `EnvironmentLaunchLifecycle.ensureLaunched`'s own fast path already
//     uses). A raw disconnect event does not distinguish "the box is dead"
//     from "a transient tunnel blip the worker's OWN client-side reconnect
//     loop (`environment-worker`'s `run()`, `RECONNECT_BASE_MS`=500ms) is
//     about to resolve on its own" — relaunching on the latter would
//     needlessly terminate a healthy box mid-reconnect. The grace window is
//     comfortably longer than that base reconnect delay without meaningfully
//     delaying genuine recovery.
//   - A small consecutive-ATTEMPT cap, so a provider that keeps failing to
//     relaunch (or a environment whose worker keeps flapping) does not retry
//     forever. `EnvironmentLaunchLifecycle`'s own tracker already single-flights
//     CONCURRENT relaunch/ensureLaunched calls for the same environment id
//     (see that module's doc) — this cap instead bounds REPEATED attempts
//     across separate disconnect events. The cap is scoped to THIS hook only:
//     giving up here does not brick the environment — a later session-create
//     still calls `ensureLaunched` independently (a separate call site, an
//     separate concern; see `sessions.routes.ts`), so manual/organic recovery
//     stays available even after this auto-recovery path gives up. The
//     counter resets on a `'launched'` outcome (a fresh problem later gets a
//     fresh budget) and when a check finds no stranded work (nothing left to
//     worry about).
//
// This is intentionally NOT a general health-check / heartbeat subsystem —
// it is a thin, few-line composition over signals the codebase already
// computes elsewhere, reacting only to a signal the tunnel engine already
// emits.

import type {
  EnvironmentTargetLookup,
  DistributionSessionStore,
} from '../../tunnel/session-distributor.js';
import type { WorkerOnlineCheck } from './environment-launch-lifecycle.js';

/** Grace period (ms) before deciding whether to relaunch — see the module doc. */
export const DEFAULT_DISCONNECT_GRACE_MS = 3_000;
/** Consecutive-attempt cap per environment id before giving up until a relaunch succeeds — see the module doc. */
export const DEFAULT_MAX_CONSECUTIVE_RELAUNCH_ATTEMPTS = 3;

/**
 * The relaunch seam this module needs from `EnvironmentLaunchLifecycle`
 * (`environment-launch-lifecycle.ts`) — structurally identical to
 * `EnvironmentLaunchTrigger.relaunch` (`sessions.routes.ts`); the concrete
 * lifecycle satisfies both. Carried as its own narrow interface (rather than
 * requiring the whole `EnvironmentLaunchTrigger`) so this module's fakes/tests
 * don't need to stub `ensureLaunched`/`terminate` too.
 */
export interface RelaunchTrigger {
  relaunch(
    environmentId: string,
    opts: { name: string },
  ): Promise<{ status: string; error?: string }>;
}

/** Structured logger seam (a subset of the usual `req.log` / pino). All optional. */
export interface RelaunchLogger {
  warn?(obj: unknown, msg?: string): void;
}

/** Options for {@link buildOnWorkerDisconnect}. */
export interface RelaunchOnDisconnectOptions {
  /** Triggers the actual re-provision. Satisfied by `EnvironmentLaunchLifecycle`. */
  lifecycle: RelaunchTrigger;
  /** Resolves an environment's target; gates this trigger to `target=cloud` only. */
  environments: EnvironmentTargetLookup;
  /** Stranded-work signal — only `loadPendingForEnvironment` is used. */
  sessions: Pick<DistributionSessionStore, 'loadPendingForEnvironment'>;
  /** "Is a worker connected for this environment right now" — re-checked after the grace period. */
  workerOnline: WorkerOnlineCheck;
  /** Grace period (ms) before deciding. Defaults to {@link DEFAULT_DISCONNECT_GRACE_MS}. */
  graceMs?: number;
  /** Consecutive-attempt cap per environment id. Defaults to {@link DEFAULT_MAX_CONSECUTIVE_RELAUNCH_ATTEMPTS}. */
  maxConsecutiveAttempts?: number;
  /** Sleep primitive, injectable for deterministic tests. Defaults to a real `setTimeout`. */
  sleep?: (ms: number) => Promise<void>;
  /** Optional structured logger. */
  logger?: RelaunchLogger;
}

/**
 * Build a worker-tunnel `onWorkerDisconnect(environmentId)` hook that
 * best-effort relaunches a `target=cloud` environment whose worker just
 * dropped and still has stranded (pending, unbound) work — see the module
 * doc for the full "why this signal, why gated this way" rationale.
 *
 * The returned function never throws (every step is defensive: an
 * unexpected `relaunch` rejection is caught and logged, not propagated) —
 * safe to compose directly into `server.ts`'s `onWorkerDisconnect` chain
 * alongside any caller-supplied hook, matching every other worker/runner
 * tunnel hook's "failures are swallowed + logged" contract.
 */
export function buildOnWorkerDisconnect(
  opts: RelaunchOnDisconnectOptions,
): (environmentId: string) => Promise<void> {
  const sleep = opts.sleep ?? defaultSleep;
  const graceMs = opts.graceMs ?? DEFAULT_DISCONNECT_GRACE_MS;
  const maxAttempts = opts.maxConsecutiveAttempts ?? DEFAULT_MAX_CONSECUTIVE_RELAUNCH_ATTEMPTS;
  /** Consecutive attempts since the last successful relaunch, per environment id. */
  const attempts = new Map<string, number>();

  return async function onWorkerDisconnect(environmentId: string): Promise<void> {
    const target = await opts.environments.loadTarget(environmentId);
    if (target !== 'cloud') {
      // self_hosted (operator-launched — never auto-relaunched), or the
      // environment vanished/archived between the disconnect and this
      // lookup (loadTarget resolves null) — nothing to drive either way.
      return;
    }

    // Absorb a disconnect the worker's OWN client-side reconnect resolves on
    // its own within the grace window (or one a racing relaunch elsewhere
    // already fixed) — see the module doc.
    await sleep(graceMs);
    if (opts.workerOnline.get(environmentId) !== undefined) {
      attempts.delete(environmentId);
      return;
    }

    const pending = await opts.sessions.loadPendingForEnvironment(environmentId);
    if (pending.length === 0) {
      // Nothing stranded for this environment — no reason to spin up a fresh
      // (billed) box. Clears any stale counter so a later, genuine burst
      // gets a fresh budget.
      attempts.delete(environmentId);
      return;
    }

    const attemptCount = attempts.get(environmentId) ?? 0;
    if (attemptCount >= maxAttempts) {
      opts.logger?.warn?.(
        { environmentId, attemptCount, pendingCount: pending.length },
        'relaunch-on-disconnect: consecutive attempt cap reached; not relaunching ' +
          '(a later session-create against this environment still retries independently)',
      );
      return;
    }
    attempts.set(environmentId, attemptCount + 1);

    try {
      // `name` is a human-readable label only (see `EnvironmentLaunchOptions`
      // in `environment-launch-lifecycle.ts`) — this hook has no environment
      // row to read a display name from (it only ever sees the id), so the
      // id itself stands in; this affects logging/provider metadata only, no
      // correctness impact.
      const outcome = await opts.lifecycle.relaunch(environmentId, { name: environmentId });
      if (outcome.status === 'launched') {
        attempts.delete(environmentId);
      } else {
        opts.logger?.warn?.(
          { environmentId, status: outcome.status, error: outcome.error ?? null },
          'relaunch-on-disconnect: relaunch did not succeed',
        );
      }
    } catch (err) {
      // relaunch() is documented to resolve (never reject) on failure — a
      // rejection here is an unexpected fault, not a normal relaunch
      // failure. Still must not throw out of a tunnel-close hook.
      opts.logger?.warn?.(
        { err, environmentId },
        'relaunch-on-disconnect: relaunch rejected unexpectedly',
      );
    }
  };
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
