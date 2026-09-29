// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Claim-based session distributor: connects a Registry-owned Session to its
// environment worker and runner tunnel. Self-hosted Sessions always use this
// path. For cloud Sessions, the pinned harness capability selects the owner;
// cloud separate and claude_code remain with harness-server, even if a worker
// is connected. Other cloud harnesses use the configured Environment launcher.
// The shared resolveExecutionOwner rule also gates the bridge and harness-server.
// This module distributes existing workers; it never provisions a sandbox.
//
// Distribution flow (registry is the single distributor):
//   1. A client `POST /v1/sessions` targets a `self_hosted` environment. The
//      registry persists the session and marks its distribution_state PENDING —
//      no runner yet. (The route owns the persist; `dispatch` runs after.) A
//      `cloud` session's first dispatch instead comes from the environment-launch
//      lifecycle once its provisioned worker comes online (see that module for
//      the provision/mint/start/wait-online/bind sequence) — the SAME `dispatch`
//      call, just triggered later, by the worker-tunnel connect hook (step 4)
//      rather than at session create.
//   2. If that environment has a CONNECTED worker on this replica (a live worker
//      tunnel + the durable `environment_claims` claim owned by this pod) AND —
//      the pinned harness belongs to Registry, the distributor MINTS a
//      per-runner binding token, derives the expected runner id via
//      `tokenBoundRunnerId` (the cross-component derivation both sides
//      reproduce), PRE-REGISTERS that expected runner in the `TunnelRegistry`
//      (so the dialing runner is matched to this session), persists the runner
//      binding + worker environment id on the session row (still PENDING), and
//      SENDS a `worker.launch_runner` frame down the worker's worker tunnel. A
//      cloud separate or claude_code Session instead falls through to step 4 (stays
//      PENDING, no binding) even with a connected worker — harness-server
//      drives it.
//   3. The worker spawns a runner with that binding token; the runner dials
//      `/v1/tunnels/runners/:runnerId`. The TunnelRegistry matches it to the
//      pre-registered waiter; the runner-tunnel route's `onRunnerConnect` hook
//      (wired to {@link SessionDistributor.onRunnerConnect}) flips the session
//      ASSIGNED.
//   4. No connected worker -> the session STAYS pending (surfaced by the work
//      depth: pending sessions with no runner binding). When that environment's
//      worker (re)connects, the worker-tunnel route's `onWorkerConnect` hook (wired
//      to {@link SessionDistributor.onWorkerConnect}) DRIVES the launch: it finds
//      every still-pending session bound to the environment that has no live
//      runner and dispatches each, exactly as a create-time dispatch would. A
//      worker (re)connecting thus reconciles every session stranded while it was
//      offline, driving the same launch path the create path uses — keyed off
//      the Env-Key/claim ownership rather than a worker binding.
//   5. Failure signals. On a `worker.runner_exited` report
//      ({@link SessionDistributor.markRunnerExited}, wired to the worker tunnel's
//      `onRunnerExited`) the bound session is marked FAILED — the only failure
//      signal for a runner that crashed BEFORE connecting its own tunnel. On a
//      runner-tunnel close AFTER the runner connected
//      ({@link SessionDistributor.markRunnerDisconnected}, wired to the
//      runner-tunnel route's `onRunnerDisconnect`) the bound session is likewise
//      marked FAILED — the post-connect crash-recovery signal: when a runner's
//      tunnel closes for good, the session bound to it is flipped FAILED. The two
//      are disjoint (a runner is either pre-connect or post-connect when it
//      dies), so there is no double transition.
//
// The launch flow PUSHES a `worker.launch_runner` frame to the environment's
// CLAIMED worker over its live worker tunnel. The reusable pieces are REUSED, not
// re-implemented — `WorkerRegistry.sendText` to enqueue the control frame,
// `TunnelRegistry.waitForRunner` to pre-register + match the dialing runner, and
// `tokenBoundRunnerId` for the derivation.
//
// Concurrency model: Node is single-threaded with one event loop. `dispatch`
// persists the PENDING transition and enqueues the launch frame synchronously
// on the loop, then returns an outcome whose `launchSettled` promise resolves
// when the worker's `worker.launch_runner_result` arrives (or the bounded wait
// elapses). A caller running `dispatch` in the create request can `void` the
// settle promise (fire-and-forget) or await it; the connect-waiter that flips
// ASSIGNED is driven independently by the runner-tunnel route hook.

import {
  encodeWorkerFrame,
  tokenBoundRunnerId,
  WorkerFrameKind,
  type WorkerLaunchResult,
} from '@orca/harness-tunnel';
import { resolveExecutionOwner, type HarnessMode, type HarnessType } from '@orca/harness-catalog';
import { randomBytes } from 'node:crypto';
import { WorkerConnectionReplacedError, type WorkerRegistry } from './worker-registry.js';
import { TunnelRegistry } from './tunnel-registry.js';

/** distribution_state value: session persisted, no runner assigned yet. */
export const DISTRIBUTION_PENDING = 'pending';
/** distribution_state value: the matched runner connected; the session is routed. */
export const DISTRIBUTION_ASSIGNED = 'assigned';
/** distribution_state value: the launch was refused or the runner died before connecting. */
export const DISTRIBUTION_FAILED = 'failed';

/** The set of recognized distribution states (the lifecycle this module drives). */
export type DistributionState =
  | typeof DISTRIBUTION_PENDING
  | typeof DISTRIBUTION_ASSIGNED
  | typeof DISTRIBUTION_FAILED;

/**
 * Structured error code the worker returns in a `worker.launch_runner_result` when
 * it refuses the launch because the session's harness is not configured on that
 * machine. Mirrors {@link HARNESS_NOT_CONFIGURED_ERROR_CODE} from the worker frame
 * schema; surfaced here so a categorical refusal is distinguishable from a
 * generic failure when marking the session FAILED.
 */
export const HARNESS_NOT_CONFIGURED_ERROR_CODE = 'harness_not_configured';

/**
 * Structured error code the distributor stamps on a launch-result settlement when
 * it refuses a dispatch at the REGISTRY, BEFORE sending `worker.launch_runner`,
 * because the session's provider is not among the connected worker's advertised
 * configured harnesses (its worker hello `configuredHarnesses`). Distinct from
 * {@link HARNESS_NOT_CONFIGURED_ERROR_CODE} — which is the WORKER's own refusal
 * AFTER receiving a launch — so a pre-spawn capability mismatch (no runner ever
 * spawned) is diagnosable apart from a worker-side spawn refusal.
 */
export const CAPABILITY_MISMATCH_ERROR_CODE = 'capability_mismatch';

/**
 * Default seconds the dispatch waits for the worker's `worker.launch_runner_result`
 * frame before settling. A 10s worker-launch-result budget — a healthy worker
 * answers in well under a second; this bound only matters when the worker is slow
 * or wedged. The wait resolving early (the result arrived) or late
 * (timeout) both settle the dispatch; a timeout does NOT fail the session (the
 * worker may still be spawning), it leaves it PENDING for the runner-connect hook.
 */
export const LAUNCH_RESULT_TIMEOUT_MS = 10_000;

/**
 * Default seconds the dispatch pre-registers the runner-connect waiter for. The
 * waiter is what MATCHES the dialing runner to this session: registering it (via
 * {@link TunnelRegistry.waitForRunner}) makes a runner that dials the derived id
 * resolve this session. The durable ASSIGNED flip is owned by the runner-tunnel
 * route's `onRunnerConnect` hook ({@link SessionDistributor.onRunnerConnect}), so
 * the in-process waiter timing out does NOT strand the session — it is the
 * matcher, not the sole assigner. A ~30s connect budget.
 */
export const RUNNER_CONNECT_TIMEOUT_MS = 30_000;

// ── Pure decision logic ──────────────────────────────────

/**
 * Mint a fresh per-runner binding token.
 *
 * A secret, URL-safe, per-launch random token (256 bits): the runner must
 * present it on its tunnel handshake, and the registry derives the expected
 * runner id from it. Binding the runner id to a per-run random token prevents one
 * authenticated caller from claiming another caller's runner id on a shared
 * server. URL-safe base64 (no `+`/`/`/`=`) so it rides a header / process env
 * cleanly — a 256-bit URL-safe launch token.
 */
export function mintBindingToken(): string {
  return randomBytes(32).toString('base64url');
}

/**
 * Derive the runner id authorized by a binding token.
 *
 * Delegates to the shared {@link tokenBoundRunnerId} identity helper so the id is
 * reproduced byte-for-byte by the runner and by the runner-tunnel route's
 * token-binding correlation check — the moment this derivation diverged, the
 * dialing runner would never match the pre-registered waiter. Kept as a thin
 * wrapper (not an alias) so the distributor's call sites read intent and a future
 * change is caught by this module's unit test.
 *
 * @throws Error when `token` is empty (the identity helper rejects it).
 */
export function expectedRunnerIdFor(token: string): string {
  return tokenBoundRunnerId(token);
}

/**
 * Environment `target` values the distributor can dispatch to, once a worker is
 * connected.
 *
 *   - `self_hosted` — an operator-run `environment-worker` dials in on its own;
 *     the distributor never provisions anything.
 *   - `cloud` — the environment-launch lifecycle (`src/environment/launch/`)
 *     provisions the box, mints its token, and starts the worker; once THAT
 *     worker dials in, dispatch works identically — the distributor does not
 *     care who started the worker, only that one is live for the environment.
 *     A connected worker is NECESSARY but not SUFFICIENT for `cloud`, though:
 *     {@link SessionDistributor.dispatch} additionally requires the SESSION's
 *     own agent harness mode to resolve to `colocated` (see
 *     {@link DistributionSessionRow.agentMode}) before it will actually drive
 *     it — a durable cloud Environment can be shared by a `colocated` session
 *     (registry-driven) and a `separate` one (harness-server-driven), so
 *     target+worker alone is not enough to decide who drives a GIVEN session.
 *     `self_hosted` has no such further gate: harness-server never drives ANY
 *     self_hosted session, so target+worker fully decides it there.
 *
 * Shared by {@link isDispatchable}, {@link SessionDistributor.dispatch}, and
 * {@link SessionDistributor.onWorkerConnect} so the three never drift on which
 * targets are eligible.
 */
const DISPATCHABLE_TARGETS: ReadonlySet<string> = new Set(['self_hosted', 'cloud']);

/** The minimal environment shape the dispatch decision needs. */
export interface DispatchableEnvironment {
  /** Environment id, e.g. `"env_a1b2..."` — also the worker-tunnel `workerId`. */
  id: string;
  /** Provisioning target. Dispatchable targets: {@link DISPATCHABLE_TARGETS}. */
  target: string | null;
}

/**
 * Whether a session for `environment` can be dispatched to a connected worker on
 * this replica RIGHT NOW.
 *
 * Dispatchable iff the environment's target is one of {@link DISPATCHABLE_TARGETS}
 * AND a live worker (worker) tunnel is registered for it on this replica — the
 * in-memory {@link WorkerRegistry} is the per-replica "is a worker connected here"
 * view (the durable claim is the cross-replica record that the worker terminates
 * on THIS pod, enforced by the worker-tunnel route on connect). An unrecognized
 * target, an absent environment, or a dispatchable-target environment with no
 * connected worker is NOT dispatchable.
 *
 * NECESSARY but not SUFFICIENT for an actual `cloud` dispatch: this predicate is
 * target+worker only and does not know a specific session's agent harness mode,
 * so it says nothing about {@link SessionDistributor.dispatch}'s further
 * `cloud`-mode gate (a `cloud`+`separate` session is NOT actually dispatched even
 * when this returns `true`). Exported standalone (pre-dates the mode gate; not
 * consulted by `dispatch`/`onWorkerConnect`, which reimplement the target+worker
 * check inline where they already hold the live connection) for callers that only
 * need the target+worker-connected fact.
 */
export function isDispatchable(
  environment: DispatchableEnvironment | null,
  workerRegistry: WorkerRegistry,
): boolean {
  if (environment === null || !DISPATCHABLE_TARGETS.has(environment.target ?? '')) {
    return false;
  }
  return workerRegistry.get(environment.id) !== undefined;
}

/**
 * Whether a connected worker can serve a session's provider, given the worker's
 * advertised configured-harness map (its worker hello `configuredHarnesses`). This
 * is the pre-spawn CAPABILITY-MATCH: the runner a worker spawns advertises the
 * SAME provider set (`ProviderRegistry.providerNames()`) on its own tunnel hello,
 * so a worker whose configured set does not include the session's provider would
 * only spawn a runner that cannot serve it — the dispatch must fail fast instead.
 *
 * Fail-OPEN in two cases so the check never over-refuses:
 *   - `agentHarness === null` — the session has no resolvable canonical harness, so
 *     there is nothing to validate (the launch frame carries `harness=null` and the
 *     worker fails open on its own configured check too);
 *   - `configuredHarnesses === null` — an older worker that does not advertise its
 *     configured set; the pre-spawn check is skipped and the worker's own
 *     launch-result ({@link HARNESS_NOT_CONFIGURED_ERROR_CODE}) remains the guard.
 *
 * Otherwise the provider must be present in the map AND marked ready (`true`); a
 * missing key or a `false` (known-but-not-configured) value is a mismatch.
 */
export function providerServedByWorker(
  agentHarness: string | null,
  configuredHarnesses: Record<string, boolean> | null | undefined,
): boolean {
  if (agentHarness === null) {
    return true;
  }
  if (configuredHarnesses === null || configuredHarnesses === undefined) {
    return true;
  }
  return configuredHarnesses[agentHarness] === true;
}

/**
 * Whether a CONNECTED runner advertises a session's provider, given the runner's
 * tunnel-hello `harnesses` set (`ProviderRegistry.providerNames()`). This is the
 * authoritative capability-match against the ACTUAL runner (as opposed to the
 * pre-spawn worker check in {@link providerServedByWorker}): once the runner
 * dials, its advertised set is known exactly, so a session bound to a runner that
 * cannot serve its provider is failed rather than routed turns it would reject.
 *
 * Fail-OPEN when `agentHarness === null` (no provider to check) or the runner
 * advertises an EMPTY set (an older runner that advertises nothing — its own
 * `UnknownProviderError` guards). Otherwise the provider must appear in the set.
 */
export function providerAdvertisedByRunner(
  agentHarness: string | null,
  advertisedHarnesses: readonly string[] | undefined,
): boolean {
  if (agentHarness === null) {
    return true;
  }
  if (advertisedHarnesses === undefined || advertisedHarnesses.length === 0) {
    return true;
  }
  return advertisedHarnesses.includes(agentHarness);
}

/** The columns a distribution transition writes on the session row. */
export interface DistributionSessionPatch {
  runnerId?: string | null;
  hostEnvironmentId?: string | null;
  distributionState?: DistributionState;
}

/**
 * Build the PENDING transition patch for a session that is being dispatched to a
 * connected worker: it records the minted runner binding + worker environment id
 * and sets distribution_state PENDING. The runner is not yet connected, so the
 * session stays PENDING until the connect hook flips it ASSIGNED.
 */
export function pendingDistributionPatch(args: {
  runnerId: string;
  hostEnvironmentId: string;
}): DistributionSessionPatch {
  return {
    runnerId: args.runnerId,
    hostEnvironmentId: args.hostEnvironmentId,
    distributionState: DISTRIBUTION_PENDING,
  };
}

/**
 * Build the PENDING transition patch for a session with NO connected worker: the
 * session is persisted PENDING with no runner binding (nothing was minted). The
 * work depth surfaces it; when the environment's worker (re)connects,
 * {@link SessionDistributor.onWorkerConnect} drives the launch.
 */
export function unassignedPendingPatch(): DistributionSessionPatch {
  return {
    runnerId: null,
    hostEnvironmentId: null,
    distributionState: DISTRIBUTION_PENDING,
  };
}

/** Build the ASSIGNED transition patch (flip only the distribution_state). */
export function assignedDistributionPatch(): DistributionSessionPatch {
  return { distributionState: DISTRIBUTION_ASSIGNED };
}

/** Build the FAILED transition patch (flip only the distribution_state). */
export function failedDistributionPatch(): DistributionSessionPatch {
  return { distributionState: DISTRIBUTION_FAILED };
}

// ── Session store seam ───────────────────────────────────

/** The session-row fields the distributor reads when dispatching. */
export interface DistributionSessionRow {
  /** Session id, e.g. `"ses_a1b2..."`. */
  id: string;
  /** Owning workspace (the tenant scope). */
  workspaceId: string;
  /** Environment id the session targets, or `null`. */
  environmentId: string | null;
  /** Runner id minted for this session's distribution, or `null` when unbound. */
  runnerId: string | null;
  /** Environment whose worker was dispatched the launch, or `null`. */
  hostEnvironmentId: string | null;
  /** Current distribution state, or `null` for a cloud / never-dispatched session. */
  distributionState: DistributionState | null;
  /**
   * Canonical harness the agent runs, or `null` when not resolvable. Carried in
   * the `worker.launch_runner` frame so the worker can refuse an unconfigured
   * harness before spawning; `null` skips that check (fail open).
   */
  agentHarness: string | null;
  /** Session-pinned mode for the shared execution-owner decision; invalid bindings throw on load. */
  agentMode: HarnessMode;
  /** Immutable harness identity from the same pinned version as agentMode. */
  agentHarnessType: HarnessType;
  /** Workspace path passed to the runner, or `null` for an empty workspace. */
  workspace: string | null;
}

/**
 * Persistence seam the distributor drives. Carried as an interface so the pure
 * decision logic + orchestration is unit-testable against an in-memory fake; the
 * route wires a Drizzle-backed implementation over the `sessions` table.
 */
export interface DistributionSessionStore {
  /** Load the distribution view of a session by id, or `null` when absent. */
  load(id: string): Promise<DistributionSessionRow | null>;
  /**
   * Apply a distribution transition patch to a session row.
   *
   * @returns `true` when a row was updated, `false` when the session id is
   *   unknown (e.g. deleted mid-dispatch).
   */
  applyDistributionPatch(id: string, patch: DistributionSessionPatch): Promise<boolean>;
  /**
   * Find the session currently bound to `runnerId`, or `null` when none is.
   *
   * Used by the runner-connect, runner-exited, and runner-disconnect hooks to
   * resolve a runner id back to the session it was minted for (those signals
   * carry only the runner id).
   */
  findBoundSessionId(runnerId: string): Promise<string | null>;
  /**
   * Load every still-PENDING session for `environmentId` that has NO runner
   * binding yet — the sessions a worker (re)connect must drive a launch for.
   *
   * Scoped on purpose to the create-time-stranded set: distribution_state
   * PENDING with a null runner_id. A session that already minted a runner (its
   * launch is in flight, or its runner is connecting) is excluded — re-driving it
   * would mint a second runner for the same session. ASSIGNED / FAILED sessions
   * are excluded by the PENDING filter. Cloud / never-distributed sessions (null
   * distribution_state) are excluded too.
   */
  loadPendingForEnvironment(environmentId: string): Promise<DispatchableSessionRef[]>;
}

/** The minimal session reference {@link SessionDistributor.onWorkerConnect} drives. */
export interface DispatchableSessionRef {
  /** Session id to dispatch, e.g. `"ses_a1b2..."`. */
  id: string;
}

/**
 * Resolves an environment's `target`, for {@link SessionDistributor.onWorkerConnect}
 * to thread the REAL target into a reconnect-driven dispatch rather than assume
 * one. Before cloud environments could ever be left PENDING, a non-empty
 * `loadPendingForEnvironment` result was proof-by-construction that the
 * environment was `self_hosted` (the create path only ever dispatched
 * self_hosted); now that a `cloud` launch's own dispatch (or a future relaunch)
 * can equally leave a session PENDING for a cloud environment, that invariant no
 * longer holds, so the hook must resolve the real target instead of hardcoding
 * one. Structurally the subset of the `environments` table's row the hook
 * needs — carried as a seam so the distributor is unit-testable without a DB,
 * exactly like {@link DistributionSessionStore}.
 */
export interface EnvironmentTargetLookup {
  /**
   * Resolve `environmentId`'s target.
   *
   * @returns The target, or `null` when the environment is unknown or
   *   archived — {@link SessionDistributor.onWorkerConnect} treats a `null`
   *   result as "nothing to drive" (fail closed: an environment that vanished
   *   between the worker's connect and this lookup gets no launches sent).
   */
  loadTarget(environmentId: string): Promise<string | null>;
}

// ── Distributor ──────────────────────────────────────────

/** Options for {@link SessionDistributor}. */
export interface SessionDistributorOptions {
  /** Persistence seam over the `sessions` table. */
  sessions: DistributionSessionStore;
  /**
   * Resolves an environment's `target`, consulted by {@link SessionDistributor.onWorkerConnect}
   * so a reconnect-driven dispatch threads the environment's REAL target
   * instead of assuming `self_hosted`.
   */
  environments: EnvironmentTargetLookup;
  /** Shared runner-tunnel registry: pre-register the connect-waiter + match the runner. */
  tunnelRegistry: TunnelRegistry;
  /** Shared worker-tunnel registry of live worker connections on this replica. */
  workerRegistry: WorkerRegistry;
  /** Bounded wait (ms) for the worker's launch-result frame. Defaults to {@link LAUNCH_RESULT_TIMEOUT_MS}. */
  launchResultTimeoutMs?: number;
  /** Bounded pre-registration of the runner-connect waiter (ms). Defaults to {@link RUNNER_CONNECT_TIMEOUT_MS}. */
  runnerConnectTimeoutMs?: number;
  /** Optional structured logger (a subset of `req.log`). */
  logger?: DistributorLogger;
}

/** Structured logger seam (a subset of the usual `req.log`). All optional. */
export interface DistributorLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/** Input to {@link SessionDistributor.dispatch}. */
export interface DispatchInput {
  /** The freshly-created session id the route already persisted. */
  sessionId: string;
  /** The session's resolved environment (target + id). */
  environment: DispatchableEnvironment;
}

/**
 * Outcome of a {@link SessionDistributor.dispatch} call.
 *
 * `state` is the distribution_state the session was left in by the synchronous
 * part of dispatch (always PENDING — a launch is in flight or no worker was
 * connected). `runnerId` is the minted runner id when a launch was sent, else
 * `null`. `launchSettled` resolves when the worker's `worker.launch_runner_result`
 * arrives or the bounded wait elapses: its `status` is the worker's verdict
 * (`"launched"` / `"failed"`), or `"no_worker"` when no launch was sent, or
 * `"timeout"` / `"send_failed"` for the degenerate paths. A `"failed"` verdict
 * has already flipped the session FAILED by the time this resolves.
 */
export interface DispatchOutcome {
  state: DistributionState;
  runnerId: string | null;
  launchSettled: Promise<LaunchSettlement>;
}

/** The settled verdict of a dispatched launch. */
export interface LaunchSettlement {
  /**
   * `"launched"` — the worker confirmed the spawn; `"failed"` — the worker
   * refused (session flipped FAILED); `"no_worker"` — no connected worker, no
   * launch sent (session stayed PENDING, no binding); `"timeout"` — no result
   * within the budget (session stays PENDING, the connect hook owns it);
   * `"send_failed"` — the launch frame never left the registry (the worker
   * connection was replaced, or an unexpected send error), so the binding was
   * rolled back to a clean PENDING for a later worker reconnect to re-drive;
   * `"capability_mismatch"` — the registry refused BEFORE launch because the
   * session's provider is not in the connected worker's advertised configured
   * harnesses (session flipped FAILED, no runner spawned); `"not_registry_driven"`
   * — the registry refused BEFORE launch because the environment is `cloud` and
   * the session's agent harness mode did not resolve to `colocated` (session
   * stays PENDING, no binding, no runner spawned) — harness-server owns this
   * session's turn loop instead, per the shared ownership rule;
   * distinct from `"no_worker"` (no launch sent for a DIFFERENT
   * reason: no worker was connected at all) so the two are diagnosable apart.
   */
  status:
    | 'launched'
    | 'failed'
    | 'no_worker'
    | 'timeout'
    | 'send_failed'
    | 'capability_mismatch'
    | 'not_registry_driven';
  /** The worker's structured failure category on a `"failed"` verdict, else `null`. */
  errorCode?: string | null;
  /** The worker's human-readable failure message on a `"failed"` verdict, else `null`. */
  error?: string | null;
}

/**
 * The single, claim-aware distributor for `self_hosted` sessions, and for
 * `cloud` sessions whose agent harness mode resolves to `colocated` — a
 * `cloud`+`separate` session is left for harness-server to drive instead (see
 * {@link dispatch}'s mode gate), so the two services agree on exactly one
 * driver per session even when they share a cloud Environment's worker.
 *
 * One instance per registry replica, sharing the replica's {@link TunnelRegistry}
 * and {@link WorkerRegistry}. {@link dispatch} is called from the session-create
 * path after the row is persisted; {@link onWorkerConnect} is wired to the
 * worker-tunnel route's connect hook (it drives launches for sessions stranded
 * pending while their worker was offline); {@link onRunnerConnect} is wired to
 * the runner-tunnel route's connect hook; {@link markRunnerExited} is wired to
 * the worker tunnel's `worker.runner_exited` hook (pre-connect crash); and
 * {@link markRunnerDisconnected} is wired to the runner-tunnel route's
 * `onRunnerDisconnect` hook (post-connect crash).
 */
export class SessionDistributor {
  private readonly sessions: DistributionSessionStore;
  private readonly environments: EnvironmentTargetLookup;
  private readonly tunnelRegistry: TunnelRegistry;
  private readonly workerRegistry: WorkerRegistry;
  private readonly launchResultTimeoutMs: number;
  private readonly runnerConnectTimeoutMs: number;
  private readonly logger: DistributorLogger | undefined;

  constructor(opts: SessionDistributorOptions) {
    this.sessions = opts.sessions;
    this.environments = opts.environments;
    this.tunnelRegistry = opts.tunnelRegistry;
    this.workerRegistry = opts.workerRegistry;
    this.launchResultTimeoutMs = opts.launchResultTimeoutMs ?? LAUNCH_RESULT_TIMEOUT_MS;
    this.runnerConnectTimeoutMs = opts.runnerConnectTimeoutMs ?? RUNNER_CONNECT_TIMEOUT_MS;
    this.logger = opts.logger;
  }

  /**
   * Dispatch a freshly-created session to its environment's worker.
   *
   * Resolves the live worker connection for the environment on this replica:
   *   - None connected -> persist PENDING with NO runner binding and return
   *     immediately (`launchSettled` -> `"no_worker"`). The session is surfaced by
   *     the work depth; a later worker reconnect drives the launch.
   *   - Connected, but `target=cloud` and the session's agent mode does NOT
   *     resolve to `colocated` -> persist PENDING with NO runner binding and
   *     return immediately (`launchSettled` -> `"not_registry_driven"`), EVEN
   *     THOUGH a worker is connected. A cloud Environment is a durable
   *     multi-session resource: a `separate`-mode session can share its
   *     `environment_id` with a `colocated` one whose worker is already
   *     online, and the registry must NOT drive the `separate` session's turn
   *     loop — harness-server does, via the same ownership rule.
   *     `self_hosted` is always Registry-owned
   *     (mode-agnostic: harness-server never drives ANY self_hosted session).
   *   - Connected (and, for `cloud`, `colocated`) -> mint a binding token,
   *     derive the expected runner id, PRE-REGISTER the connect-waiter (so the
   *     dialing runner is matched), persist the binding + worker environment id
   *     (PENDING), and SEND `worker.launch_runner` down the worker tunnel. A
   *     bounded background wait resolves the worker's result frame and flips
   *     the session FAILED on a refusal. A send error (the frame never left)
   *     rolls the binding back to a clean PENDING so a later worker reconnect
   *     can re-drive it.
   *
   * Never throws on a dispatch failure: a connected-but-uncooperative worker is a
   * PENDING/FAILED outcome on the session row, not a 500 on the create request.
   */
  async dispatch(input: DispatchInput): Promise<DispatchOutcome> {
    const { sessionId, environment } = input;

    const conn = this.workerRegistry.get(environment.id);
    if (
      conn === undefined ||
      environment.target === null ||
      !DISPATCHABLE_TARGETS.has(environment.target)
    ) {
      // No connected worker (or not a dispatchable-target environment): persist
      // PENDING with no binding and surface it via the work depth.
      await this.sessions.applyDistributionPatch(sessionId, unassignedPendingPatch());
      return {
        state: DISTRIBUTION_PENDING,
        runnerId: null,
        launchSettled: Promise.resolve({ status: 'no_worker' }),
      };
    }

    const row = await this.sessions.load(sessionId);
    if (row === null) {
      // The session was deleted between create and dispatch. Nothing to do.
      this.logger?.warn?.({ sessionId }, 'session vanished before dispatch');
      return {
        state: DISTRIBUTION_PENDING,
        runnerId: null,
        launchSettled: Promise.resolve({ status: 'no_worker' }),
      };
    }

    // Share the ownership rule with the internal route and bridge. A connected
    // worker never takes over a harness-server-owned cloud Session.
    if (
      resolveExecutionOwner(environment.target, {
        harness: row.agentHarnessType,
        mode: row.agentMode,
      }) !== 'registry'
    ) {
      await this.sessions.applyDistributionPatch(sessionId, unassignedPendingPatch());
      this.logger?.info?.(
        { sessionId, environmentId: environment.id, agentMode: row.agentMode },
        'session harness belongs to harness-server; registry does not drive it',
      );
      return {
        state: DISTRIBUTION_PENDING,
        runnerId: null,
        launchSettled: Promise.resolve({ status: 'not_registry_driven' }),
      };
    }

    // CAPABILITY-MATCH (pre-spawn): the worker advertised the harnesses it can
    // serve in its worker hello. The runner it would spawn advertises the SAME
    // provider set on its own tunnel hello (ProviderRegistry.providerNames()), so a
    // worker whose configured set does not include this session's provider would
    // only ever spawn a runner that cannot serve it. Refuse fast — flip the session
    // FAILED with a clear capability-mismatch verdict — rather than spawning a
    // doomed runner and waiting out the connect timeout. Fails open when the worker
    // advertises no set (older worker) or the session has no resolvable provider.
    if (!providerServedByWorker(row.agentHarness, conn.hello.configuredHarnesses)) {
      await this.sessions.applyDistributionPatch(sessionId, failedDistributionPatch());
      const error =
        `worker for environment ${environment.id} does not serve provider ` +
        `'${row.agentHarness}' (advertised harnesses: ` +
        `${JSON.stringify(conn.hello.configuredHarnesses)})`;
      this.logger?.warn?.(
        { sessionId, environmentId: environment.id, provider: row.agentHarness },
        'capability mismatch: worker does not serve the session provider; session marked failed',
      );
      return {
        state: DISTRIBUTION_PENDING,
        runnerId: null,
        launchSettled: Promise.resolve({
          status: 'capability_mismatch',
          errorCode: CAPABILITY_MISMATCH_ERROR_CODE,
          error,
        }),
      };
    }

    const bindingToken = mintBindingToken();
    const runnerId = expectedRunnerIdFor(bindingToken);

    // PRE-REGISTER the connect-waiter BEFORE sending the launch so a runner that
    // dials the derived id the instant it spawns is matched to this session
    // (registering the waiter is also how a later `register` resolves it). The
    // returned promise resolves when the runner connects or the bound elapses;
    // the durable ASSIGNED flip is owned by `onRunnerConnect`, so a timeout here
    // does not strand the session. Kept (not awaited) so dispatch returns
    // promptly; we attach a handler so a rejection can't surface as unhandled.
    const connectWait = this.tunnelRegistry.waitForRunner(runnerId, {
      timeoutS: this.runnerConnectTimeoutMs / 1000,
    });
    connectWait.catch(() => {
      // The wait never rejects in practice; guard defensively.
    });

    // Persist the runner binding + worker environment id (still PENDING).
    await this.sessions.applyDistributionPatch(
      sessionId,
      pendingDistributionPatch({ runnerId, hostEnvironmentId: environment.id }),
    );

    // Register the pending launch waiter against the worker connection, then send
    // the frame. The worker's `worker.launch_runner_result` resolves this waiter
    // (the worker-tunnel engine's receive loop drives that).
    const requestId = randomBytes(8).toString('hex');
    const launchResult = new Promise<WorkerLaunchResult>((resolve, reject) => {
      conn.pendingLaunches.set(requestId, { resolve, reject });
    });

    const launchFrame = encodeWorkerFrame({
      kind: WorkerFrameKind.LaunchRunner,
      requestId,
      bindingToken,
      workspace: row.workspace ?? '',
      harness: row.agentHarness,
    });

    try {
      this.workerRegistry.sendText(conn, launchFrame);
    } catch (exc) {
      // The send failed, so the launch frame never left the registry — whether
      // the worker tunnel was replaced (newest-wins, the documented
      // WorkerConnectionReplacedError) or the send failed for an unexpected reason.
      // Either way the binding is now a lie: no runner will ever dial the derived
      // id (the worker never received the launch), so the connect hook cannot
      // assign it, and a non-null runner_id would make the worker-reconnect drive
      // (which scopes on a NULL runner_id) skip the session forever. Roll the
      // binding back to a clean PENDING and cancel the pre-registered
      // connect-waiter so the session is recoverable by a later worker reconnect,
      // exactly the recovery the replaced-connection case already relied on.
      conn.pendingLaunches.delete(requestId);
      this.tunnelRegistry.cancelConnectWaiters(runnerId);
      await this.sessions.applyDistributionPatch(sessionId, unassignedPendingPatch());
      if (exc instanceof WorkerConnectionReplacedError) {
        this.logger?.warn?.(
          { sessionId, environmentId: environment.id },
          'worker connection replaced before launch frame sent; session rolled back to pending',
        );
      } else {
        // An unexpected send error is a real fault (not the benign newest-wins
        // race), so log it at error severity — but the session-row outcome is the
        // same clean, recoverable PENDING.
        this.logger?.error?.(
          { err: exc, sessionId, environmentId: environment.id },
          'unexpected error sending launch frame; session rolled back to pending',
        );
      }
      return {
        state: DISTRIBUTION_PENDING,
        runnerId: null,
        launchSettled: Promise.resolve({ status: 'send_failed' }),
      };
    }

    // Background: await the worker's launch-result frame (bounded), flipping the
    // session FAILED on a refusal so a categorical failure (harness not
    // configured) surfaces instead of silently waiting out the connect timeout.
    const launchSettled = this.awaitLaunchResult({
      sessionId,
      runnerId,
      requestId,
      conn,
      launchResult,
    });

    return { state: DISTRIBUTION_PENDING, runnerId, launchSettled };
  }

  /**
   * Await the worker's `worker.launch_runner_result` for a dispatched launch.
   *
   * Resolves the worker verdict within the bounded budget. On a `"failed"`
   * verdict the session is flipped FAILED and the pre-registered connect-waiter
   * is cancelled (the runner will never dial). A timeout leaves the session
   * PENDING (the worker may still be spawning; the connect hook owns it). The
   * pending launch waiter is always cleaned up.
   */
  private async awaitLaunchResult(args: {
    sessionId: string;
    runnerId: string;
    requestId: string;
    conn: { pendingLaunches: Map<string, { resolve(r: WorkerLaunchResult): void }> };
    launchResult: Promise<WorkerLaunchResult>;
  }): Promise<LaunchSettlement> {
    const { sessionId, runnerId, requestId, conn, launchResult } = args;
    const result = await withTimeout(launchResult, this.launchResultTimeoutMs);
    if (result === TIMED_OUT) {
      // No verdict in time: drop the waiter, keep the session PENDING. A
      // slow-but-fine worker still gets its runner matched by the connect hook.
      conn.pendingLaunches.delete(requestId);
      this.logger?.warn?.({ sessionId, runnerId }, 'worker did not answer launch within budget');
      return { status: 'timeout' };
    }
    if (result.status === 'failed') {
      // The worker refused the launch. Cancel the connect-waiter (the runner
      // will never dial) and mark the session FAILED so the failure surfaces.
      this.tunnelRegistry.cancelConnectWaiters(runnerId);
      await this.sessions.applyDistributionPatch(sessionId, failedDistributionPatch());
      this.logger?.warn?.(
        { sessionId, runnerId, errorCode: result.errorCode ?? null, error: result.error ?? null },
        'worker refused launch; session marked failed',
      );
      return {
        status: 'failed',
        errorCode: result.errorCode ?? null,
        error: result.error ?? null,
      };
    }
    return { status: 'launched' };
  }

  /**
   * Drive launches for sessions stranded pending while their worker was offline.
   *
   * Wired to the worker-tunnel route's `onWorkerConnect` hook, which fires AFTER the
   * tunnel's loops are running (so the launch frame this enqueues can actually be
   * sent) and AFTER the environment is claimed + registered in the
   * {@link WorkerRegistry} (so {@link dispatch} resolves the live connection). It
   * reconciles every session stranded while the environment's worker was offline:
   * a `self_hosted` session created with no connected worker was persisted PENDING
   * with no runner binding (see {@link unassignedPendingPatch}); when the worker
   * connects, every such session is dispatched exactly as a create-time dispatch
   * would be — mint a binding token, pre-register the expected runner, persist the
   * binding, and send `worker.launch_runner` down the now-live worker tunnel. The
   * trigger is the Env-Key/claim ownership coming live, not a worker binding.
   *
   * Only create-time-stranded sessions are driven (distribution_state PENDING
   * with a null runner_id); a session that already minted a runner is skipped so
   * a reconnect cannot mint a second runner for it. Each dispatch is fire-and-
   * forget on its bounded launch-result wait (the create path's semantics), and
   * never throws — a worker-side refusal/timeout is a per-session state, not a
   * failure of the connect hook. A no-op when no session is stranded for the
   * environment.
   *
   * For a `cloud` environment, "every such session is dispatched" is exactly as
   * true as it is for the create path: {@link dispatch} applies the SAME
   * `cloud`-mode gate here, so a pending `separate`-mode session sharing this
   * `environment_id` with a `colocated` one is SKIPPED (left PENDING, no
   * binding, no frame sent) rather than driven — this loop does not special-case
   * it; it relies entirely on `dispatch`'s own gate, the single place that
   * decision is made, so the create path and this reconnect path can never
   * drift on which sessions the registry actually drives.
   *
   * @param environmentId The environment whose worker just connected — also the
   *   worker-tunnel `workerId` and the `host_environment_id` a driven launch records.
   */
  async onWorkerConnect(environmentId: string): Promise<void> {
    const pending = await this.sessions.loadPendingForEnvironment(environmentId);
    if (pending.length === 0) {
      return;
    }
    // Resolve the environment's REAL target rather than assuming self_hosted:
    // a cloud environment's own launch-lifecycle dispatch (or a future relaunch)
    // can equally leave a session PENDING for it now, so a non-empty pending set
    // no longer proves self_hosted on its own (see EnvironmentTargetLookup's
    // doc). One lookup per connect event, not per session — every session
    // `loadPendingForEnvironment` returned here shares this same environmentId,
    // hence the same target.
    const target = await this.environments.loadTarget(environmentId);
    if (target === null || !DISPATCHABLE_TARGETS.has(target)) {
      // The environment vanished (deleted/archived) or is not a dispatchable
      // target between the pending-lookup and here — nothing to drive. `dispatch`
      // would refuse these anyway, but resolving `null` up front avoids minting
      // bindings for an environment id `dispatch` cannot serve.
      this.logger?.warn?.(
        { environmentId, target },
        'worker connected but its environment is not dispatchable; skipping pending reconciliation',
      );
      return;
    }
    this.logger?.info?.(
      { environmentId, pendingCount: pending.length, target },
      'worker connected; driving pending sessions',
    );
    // `dispatch` independently re-guards on the live connection AND the target,
    // so the launch is sent iff the worker is still connected. Settle each
    // launch-result wait in the background — the connect hook must return
    // promptly so the worker-tunnel loops keep running.
    for (const session of pending) {
      const outcome = await this.dispatch({
        sessionId: session.id,
        environment: { id: environmentId, target },
      });
      void outcome.launchSettled;
    }
  }

  /**
   * Flip a session ASSIGNED once its matched runner connects.
   *
   * Wired to the runner-tunnel route's `onRunnerConnect` hook. Resolves the
   * runner id back to the session it was minted for; when one is bound, sets its
   * distribution_state ASSIGNED. A no-op when the runner matches no bound session
   * (a runner for a session this replica did not distribute, or one already
   * cleaned up) — the per-session runner-id binding is the cross-runner guard.
   */
  async onRunnerConnect(runnerId: string): Promise<void> {
    const sessionId = await this.sessions.findBoundSessionId(runnerId);
    if (sessionId === null) {
      return;
    }

    // CAPABILITY-MATCH against the ACTUAL connected runner: the runner advertised
    // the providers it can serve in its tunnel hello. If the session's provider is
    // not in that set, this runner cannot serve the session — fail it instead of
    // flipping ASSIGNED (which would route turns to a runner that rejects the
    // provider). Fails open when the session has no resolvable provider or the
    // runner advertised nothing. The runner's advertised set is read from the live
    // tunnel session's hello; when it is unavailable (a race where the tunnel
    // closed between connect and here) the check is skipped (fail open).
    const advertised = this.tunnelRegistry.get(runnerId)?.hello.harnesses;
    const row = await this.sessions.load(sessionId);
    if (row !== null && !providerAdvertisedByRunner(row.agentHarness, advertised)) {
      await this.sessions.applyDistributionPatch(sessionId, failedDistributionPatch());
      this.logger?.warn?.(
        { sessionId, runnerId, provider: row.agentHarness, advertised },
        'capability mismatch: connected runner does not advertise the session provider; session marked failed',
      );
      return;
    }

    await this.sessions.applyDistributionPatch(sessionId, assignedDistributionPatch());
    this.logger?.info?.({ sessionId, runnerId }, 'runner connected; session assigned');
  }

  /**
   * Mark a session FAILED when its runner is reported exited.
   *
   * Wired to the worker tunnel's `worker.runner_exited` hook. A runner that crashed
   * before connecting its own tunnel has no runner-tunnel disconnect event, so
   * this report is the only failure signal. Resolves the runner id back to its
   * bound session and flips it FAILED; a no-op when the runner matches no bound
   * session.
   */
  async markRunnerExited(runnerId: string, error: string): Promise<void> {
    const sessionId = await this.sessions.findBoundSessionId(runnerId);
    if (sessionId === null) {
      return;
    }
    await this.sessions.applyDistributionPatch(sessionId, failedDistributionPatch());
    this.logger?.warn?.({ sessionId, runnerId, error }, 'runner exited; session marked failed');
  }

  /**
   * Mark a session FAILED when its connected runner's tunnel closes for good.
   *
   * Wired to the runner-tunnel route's `onRunnerDisconnect` hook — the
   * post-connect crash-recovery signal: when a runner's tunnel closes for good,
   * the session bound to it is flipped FAILED. A runner that died AFTER connecting
   * its tunnel produces this close event; one that died BEFORE connecting never
   * registered a tunnel, so it has no close event and is covered by
   * {@link markRunnerExited} instead — the two signals are disjoint, so a session
   * is flipped FAILED at most once.
   *
   * Reconnect guard: the runner-tunnel route fires this hook for the OLD
   * generation when a runner reconnects (newest-wins replaces the session), so a
   * bare flip would spuriously FAIL a session whose runner is in fact live on a
   * newer tunnel. Before flipping, this re-checks the {@link TunnelRegistry}: if
   * the runner id is still online (a newer tunnel registered), the close was a
   * replacement, not a death — skip the flip and let the live tunnel keep the
   * session ASSIGNED. Only a runner that is truly gone flips its session FAILED.
   * A no-op when the runner matches no bound session (a runner this replica did
   * not distribute, or one already cleaned up).
   */
  async markRunnerDisconnected(runnerId: string): Promise<void> {
    if (this.tunnelRegistry.has(runnerId)) {
      // A newer tunnel is live for this runner id (newest-wins reconnect). The
      // close event was for the superseded generation; the session is still
      // served by the live tunnel, so it must NOT be failed.
      return;
    }
    const sessionId = await this.sessions.findBoundSessionId(runnerId);
    if (sessionId === null) {
      return;
    }
    await this.sessions.applyDistributionPatch(sessionId, failedDistributionPatch());
    this.logger?.warn?.({ sessionId, runnerId }, 'runner tunnel closed; session marked failed');
  }
}

// ── Timeout helper ───────────────────────────────────────

/** Sentinel returned by {@link withTimeout} when the deadline wins. */
const TIMED_OUT = Symbol('timed-out');

/**
 * Await `promise`, resolving to {@link TIMED_OUT} if `ms` elapses first.
 *
 * The timer is cleared once the promise settles so a resolved wait does not keep
 * the event loop alive. The wrapped promise is not cancellable (the pending
 * launch waiter is cleaned up by the caller on the timeout branch); its later
 * settlement is harmless.
 */
function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T | typeof TIMED_OUT> {
  return new Promise<T | typeof TIMED_OUT>((resolve) => {
    const timer = setTimeout(() => resolve(TIMED_OUT), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      () => {
        clearTimeout(timer);
        resolve(TIMED_OUT);
      },
    );
  });
}
