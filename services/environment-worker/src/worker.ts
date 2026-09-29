// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Environment-worker main loop — the CLIENT counterpart to the registry's
// worker-tunnel engine.
//
// The worker dials the registry worker tunnel, announces itself with a
// `worker.hello`, then services the control frames the registry pushes —
// `worker.launch_runner` (spawn a session runner) and `worker.stop_runner`
// (terminate one) — and PROACTIVELY reports a runner that dies unexpectedly via
// `worker.runner_exited`. It answers the registry's keepalive pings with a pong on
// the same socket, and reconnects with backoff on a transient disconnect while
// failing loud on a permanent upgrade rejection (auth / authorization / outdated
// registry).
//
// Scope of this unit: the launch / stop / watch / connect / reconnect core PLUS
// the filesystem + git-worktree request frames (stat / list_dir / create_dir /
// create_worktree / remove_worktree) that back workspace selection — the registry
// sends these to stage repos and inspect the worker workspace, and the worker
// answers each with the matching result frame (see {@link dispatchWorkerFrame}).
// The filesystem/worktree work lives in `fileops.ts` + `git-worktree.ts`; an
// inbound frame the worker still has no handler for (a result frame, or a future
// request kind) is dropped, not errored. A spawned runner gets the registry
// runner-tunnel URL + its binding token + workspace through {@link buildRunnerEnv};
// the runner binary itself does not exist yet, so the launch command is injected.
//
// The network (`RegistryConnector`) and process (`ProcessSpawner`) collaborators
// are seams (see `ws-client.ts` / `process-spawner.ts`), exactly as the engine
// keeps its collaborators injectable, so the whole loop is unit-testable against
// a fake registry ws server + a stub runner command with no external infra.

import {
  FrameKind,
  WorkerFrameKind,
  INTERNAL_WS_ORIGIN,
  HARNESS_NOT_CONFIGURED_ERROR_CODE,
  decodeFrame,
  decodeWorkerFrame,
  encodeFrame,
  encodeWorkerFrame,
  tokenBoundRunnerId,
  type Frame,
  type WorkerCreateWorktreeFrame,
  type WorkerCreateWorktreeResultFrame,
  type WorkerFrame,
  type WorkerLaunchRunnerFrame,
  type WorkerLaunchRunnerResultFrame,
  type WorkerRemoveWorktreeFrame,
  type WorkerRemoveWorktreeResultFrame,
  type WorkerStopRunnerFrame,
  type WorkerStopRunnerResultFrame,
} from '@orca/harness-tunnel';
import { existsSync, mkdirSync, statSync } from 'node:fs';
import { isAbsolute, join } from 'node:path';
import { EnvironmentConnectError } from './errors.js';
import { handleCreateDir, handleListDir, handleStat } from './fileops.js';
import { WorktreeError } from './git-worktree.js';
import {
  createJob,
  createWorktreeOffload,
  removeJob,
  type WorktreeOffload,
} from './worktree-offload.js';
import { buildRunnerEnv } from './runner-env.js';
import { ChildProcessSpawner, type ProcessSpawner, type RunnerProcess } from './process-spawner.js';
import {
  UpgradeRejectedError,
  WsRegistryConnector,
  type RegistryConnector,
  type WorkerSocket,
  type WorkerSocketMessage,
} from './ws-client.js';

/** The dedicated handshake header carrying the worker's raw Env Key. */
export const ENVIRONMENT_KEY_HEADER = 'X-Orca-Environment-Key';

/**
 * The dedicated handshake header carrying a registry-launched worker's raw
 * per-launch Environment Token — the managed-auth alternative to the Env Key
 * (see {@link ENVIRONMENT_KEY_HEADER}) for a server-managed sandbox with no
 * operator to provision an Env Key ahead of time. Mirrors the registry's own
 * `ENVIRONMENT_TOKEN_HEADER` constant in `worker-tunnel.routes.ts` — the two
 * are independently declared (environment-worker and registry-service-ts are
 * separate deployable services) but MUST stay byte-identical as the wire
 * contract, same convention as {@link ENVIRONMENT_KEY_HEADER}.
 */
export const ENVIRONMENT_TOKEN_HEADER = 'X-Orca-Environment-Token';

/** Worker software version reported in the hello. */
const WORKER_VERSION = '0.1.0';
/** Wire-protocol major the worker speaks (the engine refuses on a major mismatch). */
const FRAME_PROTOCOL_VERSION = 1;

/** Base reconnect interval (ms). */
export const RECONNECT_BASE_MS = 500;
/** Reconnect backoff cap (ms). */
export const RECONNECT_CAP_MS = 10_000;
/** Backoff jitter fraction applied to the growing reconnect delay. */
const RECONNECT_JITTER = 0.5;
/**
 * Consecutive reconnects that may use the prompt recycle cadence before the
 * normal backoff takes over. A recycle is by definition a one-off — an ingress
 * or a deploy moving a healthy socket — so a condition that keeps producing them
 * is NOT a recycle, and without this cap it would pin the worker (and, since
 * every worker sees the same condition, the whole fleet) at the base interval
 * indefinitely.
 */
export const MAX_RECYCLE_RECONNECTS = 5;
/**
 * A connection that served at least this long counts as healthy: the recycle
 * allowance is restored, so a worker that has been up for a while still gets a
 * prompt reconnect after the next genuine ingress recycle. Comfortably longer
 * than the instant accept→close of a registry that is failing, and shorter than
 * the idle windows that produce real recycles.
 */
const HEALTHY_CONNECTION_MS = 30_000;
/**
 * WebSocket close codes the reconnect classifier reads structurally.
 * `1012`/`1001` are the peer explicitly saying "I am going away"; `1006` is the
 * abnormal closure `ws` reports when the socket dropped with no close frame.
 */
const RECYCLE_CLOSE_CODES = { serviceRestart: 1012, goingAway: 1001, abnormal: 1006 } as const;

/** Poll cadence (ms) for the per-runner exit watcher. */
const RUNNER_WATCH_INTERVAL_MS = 500;
/** Grace (ms) to await a terminated runner before escalating to SIGKILL. */
const RUNNER_TERMINATE_GRACE_MS = 5_000;
/**
 * Bound (ms) on the post-SIGKILL reap. A process that has not reaped by now is
 * wedged (uninterruptible I/O, a stuck FUSE mount), and no further waiting will
 * change that — so the stop reports the failure instead of blocking the serve
 * loop, which is what would take the whole tunnel down with it.
 */
const RUNNER_KILL_GRACE_MS = 2_000;
/**
 * Idle wakeup (ms) for the serve loop's receive: the loop is event-driven on the
 * socket, but wakes at least this often even when the socket is silent, so a
 * half-open socket that never surfaces a close/error can't park the loop forever.
 */
const SERVE_IDLE_TIMEOUT_MS = 60_000;

/**
 * HTTP statuses on the WS upgrade worth retrying. Everything else in the 4xx
 * range is a permanent client error (auth, authorization, wrong/old registry)
 * where reconnecting can never succeed. 408 (Request Timeout) and 429 (Too Many
 * Requests) are transient by HTTP semantics, so they stay on the reconnect path.
 */
const RETRYABLE_UPGRADE_STATUSES: ReadonlySet<number> = new Set([408, 429]);

/** A spawned runner and its handle. */
interface RunnerHandle {
  proc: RunnerProcess;
}

/** Minimal logger seam (a subset of the usual structured logger). */
export interface WorkerLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/** Construction options for {@link EnvironmentWorker}. */
export interface EnvironmentWorkerOptions {
  /** This environment's id, presented in the tunnel path. */
  readonly environmentId: string;
  /**
   * This environment's key, presented in the {@link ENVIRONMENT_KEY_HEADER}.
   * Required for the self-hosted path; omitted (or ignored, if set) when
   * {@link environmentToken} is present — see that field.
   */
  readonly environmentKey?: string;
  /**
   * Per-launch Environment Token — the managed-auth alternative to
   * {@link environmentKey} for a registry-launched worker (a server-managed
   * sandbox with no operator to provision an Env Key). When set, the worker
   * sends this on {@link ENVIRONMENT_TOKEN_HEADER} and skips the Env-Key
   * header entirely (never both) — see {@link buildConnectHeaders}. `undefined`
   * for a self-hosted worker, which is the existing, unchanged behavior.
   */
  readonly environmentToken?: string;
  /**
   * Server-assigned per-launch worker identity, injected alongside
   * {@link environmentToken} on the managed path. Not yet consumed by the
   * tunnel/frame protocol (the hello frame carries `name`, not a separate
   * id) — accepted and retained for forward-compatibility, mirroring
   * `local-environment-launcher.ts`'s own "forward, don't drop" posture for
   * this same identity field.
   */
  readonly workerId?: string;
  /** Registry worker-tunnel base URL the worker dials (`wss://…` or an `http(s)://` origin). */
  readonly registryTunnelBaseUrl: string;
  /** Registry runner-tunnel base URL passed to spawned runners so they dial back. */
  readonly registryRunnerUrl: string;
  /** Worker directory under which session-runner workspaces live (the launch cwd). */
  readonly workspaceDir: string;
  /** Argv used to launch a session runner; element 0 is the executable. */
  readonly runnerLaunchCommand: readonly string[];
  /** Human-readable worker name reported in the hello. */
  readonly name: string;
  /**
   * Per-harness readiness reported in the hello, e.g. `{ "claude-code": true }`,
   * or `null` ("unknown"). Defaults to `null`.
   */
  readonly configuredHarnesses?: Record<string, boolean> | null;
  /**
   * Returns whether `harness` is configured on this machine; consulted at launch
   * time to refuse a runner the machine cannot run. Defaults to always-true
   * (every harness considered configured) — the launch-time gate is only as
   * strict as the operator wires it.
   */
  readonly harnessConfigured?: (harness: string) => boolean;
  /** Network seam. Defaults to the production `ws`-backed connector. */
  readonly connector?: RegistryConnector;
  /** Process seam. Defaults to the production `node:child_process` spawner. */
  readonly spawner?: ProcessSpawner;
  /**
   * Worktree offload seam: runs a git worktree job off the EVENT loop. Defaults
   * to the production worker-thread offload ({@link createWorktreeOffload}),
   * which runs the blocking `git` shell-out on a dedicated thread. The serve
   * loop is freed separately, by dispatching the request itself off the loop
   * (see {@link serveRequestOffLoop}). A unit test injects an in-process offload
   * to exercise the dispatch without building the thread entry.
   */
  readonly worktreeOffload?: WorktreeOffload;
  /** Base environment the runner env is filtered from. Defaults to `process.env`. */
  readonly baseEnv?: Record<string, string | undefined>;
  /** Worker pid recorded as the runner's parent. Defaults to `process.pid`. */
  readonly parentPid?: number;
  /** Base reconnect interval (ms). Defaults to {@link RECONNECT_BASE_MS}. */
  readonly reconnectBaseMs?: number;
  /** Reconnect backoff cap (ms). Defaults to {@link RECONNECT_CAP_MS}. */
  readonly reconnectCapMs?: number;
  /** Sleep `ms`. Defaults to a real `setTimeout`. A test injects a controllable timer. */
  readonly sleep?: (ms: number) => Promise<void>;
  /**
   * Idle wakeup (ms) for the serve loop's receive. Defaults to
   * {@link SERVE_IDLE_TIMEOUT_MS}. A test shrinks it to observe the defensive
   * wakeup without waiting a real minute.
   */
  readonly serveIdleTimeoutMs?: number;
  /**
   * Test hook fired each time the serve loop's idle timer wins the receive race
   * (a defensive wakeup on a silent socket). Lets a test prove the wakeup
   * actually fired while the pending receive was preserved. Unused in production.
   */
  readonly onServeIdleForTest?: () => void;
  /** Optional logger. When omitted, lifecycle events are silent. */
  readonly logger?: WorkerLogger;
}

/** A launch request stripped of its frame discriminator. */
interface LaunchRequest {
  requestId: string;
  bindingToken: string;
  workspace: string;
  harness?: string | null;
}

/**
 * The request kinds served OFF the serve loop. Both shell out to `git`, whose
 * bound (`GIT_TIMEOUT_MS`, 120s) exceeds the registry's dead-worker window
 * (`PING_INTERVAL_MS * PING_MISS_THRESHOLD`, 90s), so awaiting them inline would
 * leave a keepalive ping unread until git finished.
 */
type OffLoopRequestFrame = WorkerCreateWorktreeFrame | WorkerRemoveWorktreeFrame;

/** Whether `frame` is one of the {@link OffLoopRequestFrame} kinds. */
function isOffLoopRequest(frame: WorkerFrame): frame is OffLoopRequestFrame {
  return (
    frame.kind === WorkerFrameKind.CreateWorktree || frame.kind === WorkerFrameKind.RemoveWorktree
  );
}

/** Max characters of an undecodable frame included in the drop log line. */
const UNDECODABLE_FRAME_PREVIEW_CHARS = 120;

/** Decode a worker frame, or `undefined` when `raw` is not one. */
function tryDecodeWorkerFrame(raw: string): WorkerFrame | undefined {
  try {
    return decodeWorkerFrame(raw);
  } catch {
    return undefined;
  }
}

/** Decode a runner-tunnel frame, or `undefined` when `raw` is not one. */
function tryDecodeRunnerFrame(raw: string): Frame | undefined {
  try {
    return decodeFrame(raw);
  } catch {
    return undefined;
  }
}

/**
 * The `status: "failed"` result frame answering `frame`, or `undefined` when
 * `frame` is not a request (nothing to correlate a result to). Every request
 * kind is covered: a handler that throws must still produce the frame its
 * requestId is waiting for.
 */
function failedResultFor(frame: WorkerFrame, error: string): WorkerFrame | undefined {
  switch (frame.kind) {
    case WorkerFrameKind.LaunchRunner:
      return {
        kind: WorkerFrameKind.LaunchRunnerResult,
        requestId: frame.requestId,
        status: 'failed',
        runnerId: null,
        error,
      };
    case WorkerFrameKind.StopRunner:
      return {
        kind: WorkerFrameKind.StopRunnerResult,
        requestId: frame.requestId,
        status: 'failed',
        error,
      };
    case WorkerFrameKind.Stat:
      return {
        kind: WorkerFrameKind.StatResult,
        requestId: frame.requestId,
        status: 'failed',
        exists: false,
        type: null,
        canonicalPath: null,
        error,
      };
    case WorkerFrameKind.ListDir:
      return {
        kind: WorkerFrameKind.ListDirResult,
        requestId: frame.requestId,
        status: 'failed',
        entries: [],
        hasMore: false,
        error,
      };
    case WorkerFrameKind.CreateDir:
      return {
        kind: WorkerFrameKind.CreateDirResult,
        requestId: frame.requestId,
        status: 'failed',
        path: null,
        error,
      };
    case WorkerFrameKind.CreateWorktree:
      return {
        kind: WorkerFrameKind.CreateWorktreeResult,
        requestId: frame.requestId,
        status: 'failed',
        worktreePath: null,
        branch: null,
        error,
      };
    case WorkerFrameKind.RemoveWorktree:
      return {
        kind: WorkerFrameKind.RemoveWorktreeResult,
        requestId: frame.requestId,
        status: 'failed',
        error,
      };
    default:
      return undefined;
  }
}

/**
 * The worker main loop: dial the registry, serve control frames, monitor
 * runners, reconnect. Construct one and call {@link run}.
 */
export class EnvironmentWorker {
  private readonly opts: EnvironmentWorkerOptions;
  private readonly connector: RegistryConnector;
  private readonly spawner: ProcessSpawner;
  /** Runs a git worktree job off the serve loop (worker thread in production). */
  private readonly worktreeOffload: WorktreeOffload;
  private readonly runners = new Map<string, RunnerHandle>();
  /**
   * Live tunnel socket, set by {@link serveFrames} for the watcher tasks (which
   * outlive any single connection) to report on. `undefined` between connections.
   */
  private socket: WorkerSocket | undefined;
  /**
   * runner_id → composed error for exits that could not be sent (tunnel down at
   * the time). Flushed after the next hello.
   */
  private readonly unreportedExits = new Map<string, string>();
  /** In-flight per-runner watcher promises; awaited on shutdown / in tests. */
  private readonly watchers = new Set<Promise<void>>();
  /**
   * In-flight requests being served OFF the serve loop (see
   * {@link serveRequestOffLoop}). Tracked so a test can drain them; each removes
   * itself when it settles.
   */
  private readonly offLoopRequests = new Set<Promise<void>>();

  constructor(opts: EnvironmentWorkerOptions) {
    this.opts = opts;
    this.connector = opts.connector ?? new WsRegistryConnector();
    this.spawner = opts.spawner ?? new ChildProcessSpawner();
    this.worktreeOffload = opts.worktreeOffload ?? createWorktreeOffload();
  }

  // ── Runner bookkeeping ─────────────────────────────────

  /**
   * Return the ids of runners still alive, pruning dead handles as a side
   * effect.
   */
  aliveRunnerIds(): string[] {
    for (const [id, handle] of [...this.runners.entries()]) {
      if (handle.proc.poll() !== null) {
        this.runners.delete(id);
      }
    }
    return [...this.runners.keys()];
  }

  // ── URL + headers ──────────────────────────────────────

  /** Build the WebSocket tunnel URL for the registry worker tunnel. */
  private tunnelUrl(): string {
    const base = this.opts.registryTunnelBaseUrl.replace(/\/+$/, '');
    const scheme = base.startsWith('https') || base.startsWith('wss') ? 'wss' : 'ws';
    const hostPart = base.includes('://') ? base.split('://', 2)[1]! : base;
    return `${scheme}://${hostPart}/v1/tunnels/environments/${this.opts.environmentId}`;
  }

  /**
   * Build the WS upgrade headers. The worker identifies as a first-party
   * client via the internal WS origin so the registry's CSWSH guard allows
   * the handshake (the worker is not a browser), plus exactly ONE credential
   * header:
   *
   * - A managed {@link EnvironmentWorkerOptions.environmentToken}, when set,
   *   is an explicit credential choice: it is sent on
   *   {@link ENVIRONMENT_TOKEN_HEADER} and the Env-Key header is skipped
   *   entirely — this worker has no Env Key to fall back to (a
   *   server-managed sandbox has no operator to provision one), so sending
   *   both would be meaningless and sending the Env Key instead would be
   *   wrong. Takes priority even if an Env Key also happens to be configured.
   * - Otherwise (the existing, self-hosted path) the Env Key is sent on
   *   {@link ENVIRONMENT_KEY_HEADER}, unchanged.
   */
  private buildConnectHeaders(): Record<string, string> {
    if (this.opts.environmentToken !== undefined) {
      return {
        [ENVIRONMENT_TOKEN_HEADER]: this.opts.environmentToken,
        Origin: INTERNAL_WS_ORIGIN,
      };
    }
    return {
      [ENVIRONMENT_KEY_HEADER]: this.opts.environmentKey ?? '',
      Origin: INTERNAL_WS_ORIGIN,
    };
  }

  // ── Launch ─────────────────────────────────────────────

  /**
   * Handle a `worker.launch_runner` request. Refuses a harness the machine cannot
   * run (structured `harness_not_configured`), validates the workspace exists,
   * spawns the runner with its wiring env, detects an immediate death, and starts
   * the exit watcher.
   */
  private async handleLaunch(
    frame: WorkerLaunchRunnerFrame,
  ): Promise<WorkerLaunchRunnerResultFrame> {
    // Refuse to spawn for a harness this machine can't run — otherwise the runner
    // starts, the session looks alive, and the first turn dies confusingly inside
    // the executor. `null` (older registry, or no resolvable harness) skips the
    // check so version skew fails open.
    if (frame.harness !== null && frame.harness !== undefined) {
      const check = this.opts.harnessConfigured;
      if (check !== undefined && !check(frame.harness)) {
        return {
          kind: WorkerFrameKind.LaunchRunnerResult,
          requestId: frame.requestId,
          status: 'failed',
          error:
            `harness ${JSON.stringify(frame.harness)} is not configured on worker ` +
            `${JSON.stringify(this.opts.name)} — install it on that machine and set a ` +
            'default credential',
          errorCode: HARNESS_NOT_CONFIGURED_ERROR_CODE,
          runnerId: null,
        };
      }
    }

    const runnerId = tokenBoundRunnerId(frame.bindingToken);

    // A launch is IDEMPOTENT on its runner id. The id is a pure function of the
    // binding token, and the registry documents its launch as retryable against
    // a reconnected worker ("worker disconnected mid-request"), so a retry
    // arrives with the same token and maps to the same id. Spawning again would
    // overwrite the tracked handle with the second process and orphan the first:
    // still running, no longer watched, absent from the hello, its eventual
    // death misread as intentional — while both processes share one binding
    // token, one runner-tunnel identity, and one derived workspace.
    const live = this.runners.get(runnerId);
    if (live !== undefined && live.proc.poll() === null) {
      this.opts.logger?.info?.(
        { runnerId, pid: live.proc.pid, requestId: frame.requestId },
        'launch_runner is a retry for a runner already running; not spawning again',
      );
      return {
        kind: WorkerFrameKind.LaunchRunnerResult,
        requestId: frame.requestId,
        status: 'launched',
        runnerId,
      };
    }

    // Resolve the runner's launch workspace (its cwd). The runner workspace is a
    // WORKER-side concern, so the registry frame does NOT dictate the worker's
    // filesystem layout: when the frame carries an absolute path the worker
    // honors it (a future registry feature that pins a session workspace — it
    // must already exist), but the common case is an empty frame workspace, where
    // the worker derives a per-runner directory UNDER its configured
    // `workspaceDir` and creates it. This is what makes `workspaceDir` the
    // operative "worker directory under which session-runner workspaces live" the
    // worker is configured with (a self_hosted session has no attached workspace
    // by default, so the registry legitimately sends '').
    let workspace: string;
    if (frame.workspace && isAbsolute(frame.workspace)) {
      workspace = frame.workspace;
      if (!directoryExists(workspace)) {
        return {
          kind: WorkerFrameKind.LaunchRunnerResult,
          requestId: frame.requestId,
          status: 'failed',
          error: `workspace path does not exist: ${workspace}`,
          runnerId: null,
        };
      }
    } else {
      workspace = join(this.opts.workspaceDir, runnerId);
      try {
        mkdirSync(workspace, { recursive: true });
      } catch (exc) {
        return {
          kind: WorkerFrameKind.LaunchRunnerResult,
          requestId: frame.requestId,
          status: 'failed',
          error: `failed to create runner workspace ${workspace}: ${errMessage(exc)}`,
          runnerId: null,
        };
      }
    }

    const env = buildRunnerEnv(this.opts.baseEnv ?? process.env, {
      registryRunnerUrl: this.opts.registryRunnerUrl,
      runnerId,
      bindingToken: frame.bindingToken,
      workspace,
      parentPid: this.opts.parentPid ?? process.pid,
    });

    let proc: RunnerProcess;
    try {
      proc = this.spawner.spawn({
        command: this.opts.runnerLaunchCommand,
        env,
        cwd: workspace,
      });
    } catch (exc) {
      return {
        kind: WorkerFrameKind.LaunchRunnerResult,
        requestId: frame.requestId,
        status: 'failed',
        error: `failed to spawn runner: ${errMessage(exc)}`,
        runnerId: null,
      };
    }

    // A runner can die before the spawn settles (a bad command, an instant
    // crash). Give a spawn error a tick to surface, then ship the cause with the
    // failed result instead of falsely reporting "launched". This is a
    // best-effort FAST path only: a runner that starts, logs, and exits a moment
    // later is legitimately reported "launched" and its death arrives on the
    // watcher's `worker.runner_exited` instead — the alternative is delaying
    // every launch by however long we are willing to wait for a crash.
    await macrotaskTick();
    const code = proc.poll();
    if (code !== null) {
      // One more turn so any stdout/stderr `data` callbacks already queued are
      // delivered before the tail is composed. (Awaiting `proc.wait()` here
      // would be a no-op: poll() is non-null, so wait() resolves immediately.)
      await macrotaskTick();
      return {
        kind: WorkerFrameKind.LaunchRunnerResult,
        requestId: frame.requestId,
        status: 'failed',
        error: runnerExitError(code, proc),
        runnerId: null,
      };
    }

    this.runners.set(runnerId, { proc });
    this.startWatcher(runnerId);
    this.opts.logger?.info?.({ runnerId, workspace, pid: proc.pid }, 'launched runner');
    return {
      kind: WorkerFrameKind.LaunchRunnerResult,
      requestId: frame.requestId,
      status: 'launched',
      runnerId,
    };
  }

  // ── Stop ───────────────────────────────────────────────

  /**
   * Handle a `worker.stop_runner` request: terminate the runner if tracked. Pops
   * the handle BEFORE terminating so the watcher reads the death as intentional
   * (and does not report it as a crash) — and puts it BACK if the runner turns
   * out to have survived, since a live process must not be silently forgotten.
   */
  private async handleStop(frame: WorkerStopRunnerFrame): Promise<WorkerStopRunnerResultFrame> {
    const handle = this.runners.get(frame.runnerId);
    if (handle === undefined) {
      return {
        kind: WorkerFrameKind.StopRunnerResult,
        requestId: frame.requestId,
        status: 'failed',
        error: `unknown runner: ${frame.runnerId}`,
      };
    }
    this.runners.delete(frame.runnerId);
    if (handle.proc.poll() === null) {
      const termDelivered = handle.proc.terminate();
      const outcome = await this.awaitExitOrKill(frame.runnerId, handle.proc, termDelivered);
      if (outcome !== null) {
        // The process outlived SIGKILL. Reporting "stopped" here would tell the
        // registry a runner is gone while it is still running and still holding
        // its tunnel — and the handle was already popped, so nothing would ever
        // notice it again. Put it back (its watcher is still polling, so a later
        // death is still seen) and answer with the real outcome.
        this.runners.set(frame.runnerId, handle);
        return {
          kind: WorkerFrameKind.StopRunnerResult,
          requestId: frame.requestId,
          status: 'failed',
          error: outcome,
        };
      }
    }
    this.opts.logger?.info?.({ runnerId: frame.runnerId }, 'stopped runner');
    return {
      kind: WorkerFrameKind.StopRunnerResult,
      requestId: frame.requestId,
      status: 'stopped',
    };
  }

  /**
   * Await a terminated runner up to the grace window, then SIGKILL + reap.
   * Resolves `null` once the process is gone, or a human-readable error naming
   * the pid when it is STILL RUNNING after SIGKILL.
   *
   * `termDelivered` is the caller's `proc.terminate()` result. A refused SIGTERM
   * (EPERM, or an invalid signal) means the process was never asked to stop, so
   * waiting out the grace window is time spent waiting for something that cannot
   * happen: skip straight to SIGKILL, and name the refusal in the error rather
   * than reporting an unexplained 7s timeout. Discarding that boolean is how the
   * condition came to cost the full grace and go unnamed.
   *
   * Both waits are bounded. An unbounded final wait is not a theoretical
   * concern: a process wedged in an uninterruptible state (a stuck FUSE mount on
   * this repo's gVisor runtime is the reachable case) never reaps, and this is
   * awaited inline from the serve loop — so the worker would stop answering the
   * stop, the keepalive pings, and every later frame, with a socket close unable
   * to break it.
   */
  private async awaitExitOrKill(
    runnerId: string,
    proc: RunnerProcess,
    termDelivered = true,
  ): Promise<string | null> {
    if (termDelivered && (await this.exitedWithin(proc, RUNNER_TERMINATE_GRACE_MS))) {
      return null;
    }
    if (!termDelivered) {
      this.opts.logger?.warn?.(
        { runnerId, pid: proc.pid },
        'SIGTERM was not delivered; escalating to SIGKILL without waiting out the grace window',
      );
    }
    const delivered = proc.kill();
    if (await this.exitedWithin(proc, RUNNER_KILL_GRACE_MS)) {
      return null;
    }
    const waited = termDelivered ? RUNNER_TERMINATE_GRACE_MS + RUNNER_KILL_GRACE_MS : 0;
    const error =
      `runner ${runnerId} (pid ${proc.pid ?? 'unknown'}) is still running ` +
      (termDelivered
        ? `${Math.round(waited / 1000)}s after SIGTERM and `
        : 'after SIGTERM was refused (not delivered) and ') +
      `${Math.round(RUNNER_KILL_GRACE_MS / 1000)}s after SIGKILL` +
      (delivered ? '' : ' (SIGKILL was not delivered)');
    this.opts.logger?.error?.(
      { runnerId, pid: proc.pid, termDelivered, killDelivered: delivered },
      error,
    );
    return error;
  }

  // ── Watch + report ─────────────────────────────────────

  /** Spawn (and track) the exit watcher for a runner. */
  private startWatcher(runnerId: string): void {
    const watcher = this.watchRunner(runnerId).finally(() => {
      this.watchers.delete(watcher);
    });
    this.watchers.add(watcher);
  }

  /**
   * Watch a spawned runner and report an unexpected exit. An exit while the
   * runner is still tracked is unexpected (a stop pops the entry first), so the
   * watcher composes the exit error and reports it via `worker.runner_exited`.
   */
  private async watchRunner(runnerId: string): Promise<void> {
    const handle = this.runners.get(runnerId);
    if (handle === undefined) {
      return;
    }
    while (handle.proc.poll() === null) {
      await this.sleep(RUNNER_WATCH_INTERVAL_MS);
    }
    if (this.runners.get(runnerId) !== handle) {
      // A stop (or shutdown cleanup) removed it first — intentional, not a crash.
      return;
    }
    this.runners.delete(runnerId);
    const error = runnerExitError(handle.proc.poll(), handle.proc);
    this.opts.logger?.warn?.({ runnerId, error }, 'runner died unexpectedly');
    await this.reportRunnerExit(runnerId, error);
  }

  /**
   * Send a `worker.runner_exited` report, parking it on failure (tunnel down or
   * mid-reconnect) for the next hello to flush.
   */
  private async reportRunnerExit(runnerId: string, error: string): Promise<void> {
    const frame = encodeWorkerFrame({ kind: WorkerFrameKind.RunnerExited, runnerId, error });
    const socket = this.socket;
    if (socket !== undefined) {
      try {
        await socket.sendText(frame);
        return;
      } catch (exc) {
        // Any send failure parks the report for reconnect.
        this.opts.logger?.warn?.(
          { runnerId, err: exc },
          'could not send a runner_exited report; parking it for the next hello',
        );
      }
    }
    this.opts.logger?.warn?.(
      { runnerId, error },
      'parked a runner_exited report (no tunnel); it will flush after the next hello',
    );
    this.unreportedExits.set(runnerId, error);
  }

  // ── Run loop + reconnect ───────────────────────────────

  /** Aborted by {@link stop} to break the reconnect loop (cooperative cancellation). */
  private readonly stopController = new AbortController();

  /**
   * Run the worker with reconnection. Connects, serves, and reconnects with
   * backoff on a transient disconnect; re-raises an
   * {@link EnvironmentConnectError} on a permanent upgrade rejection so the entry
   * point can fail loud, and returns cleanly once {@link stop} is called.
   */
  async run(): Promise<void> {
    const base = this.opts.reconnectBaseMs ?? RECONNECT_BASE_MS;
    const cap = this.opts.reconnectCapMs ?? RECONNECT_CAP_MS;
    const signal = this.stopController.signal;
    let backoff = base;
    // Consecutive reconnects already granted the prompt recycle cadence. Capped
    // so a PERSISTENT condition that keeps looking like a recycle still falls
    // back to the growing backoff instead of dialing forever at `base`.
    let recycleStreak = 0;
    try {
      while (!signal.aborted) {
        const connectedAt = Date.now();
        try {
          await this.connectAndServe();
          backoff = base;
          recycleStreak = 0;
        } catch (exc) {
          if (signal.aborted) {
            // A stop arrived (the close it triggered surfaced here): exit cleanly.
            break;
          }
          if (exc instanceof EnvironmentConnectError) {
            // Permanent failure (auth / authorization / outdated registry). Do
            // NOT back off and retry — propagate so the entry point fails loud.
            throw exc;
          }
          if (Date.now() - connectedAt >= HEALTHY_CONNECTION_MS) {
            // The tunnel served for a real span before dropping, so this is not
            // a persistent failure: restore the prompt-recycle allowance.
            recycleStreak = 0;
          }
          const recycle = this.recycleReconnect(exc);
          const prompt = recycle && recycleStreak < MAX_RECYCLE_RECONNECTS;
          recycleStreak = recycle ? recycleStreak + 1 : 0;
          const wait = prompt ? base : backoff;
          this.opts.logger?.warn?.(
            { err: exc, waitMs: wait, recycle, recycleStreak },
            'worker tunnel disconnected; reconnecting',
          );
          await this.sleep(wait);
          if (signal.aborted) {
            break;
          }
          backoff = prompt
            ? base
            : Math.min(backoff * 2 * (1 + Math.random() * RECONNECT_JITTER), cap);
        }
      }
    } finally {
      await this.cleanupRunners();
      this.logUnreportedExits();
    }
  }

  /**
   * Request shutdown: break the reconnect loop and terminate every live runner.
   * Aborting the loop's signal unblocks an in-flight serve (by closing the live
   * socket) and stops further reconnects; the loop's own `finally` then cleans up
   * runners. Awaitable so a signal handler / test can confirm teardown.
   */
  async stop(): Promise<void> {
    this.stopController.abort();
    // Unblock a serve parked on receive() so run()'s loop reaches its exit.
    this.socket?.closeSocket();
    await this.cleanupRunners();
    this.logUnreportedExits();
  }

  /** Terminate all live runners on shutdown. */
  private async cleanupRunners(): Promise<void> {
    // Whether each runner actually RECEIVED the SIGTERM. Carried to the wait
    // below rather than discarded: a refused signal means the grace window can
    // only expire, so escalating immediately saves the whole 5s per runner on a
    // shutdown that is already racing its own supervisor's SIGKILL. A runner
    // that had already exited is absent here and defaults to delivered — there
    // was nothing left to signal.
    const termDelivered = new Map<string, boolean>();
    for (const [runnerId, handle] of this.runners) {
      if (handle.proc.poll() === null) {
        this.opts.logger?.info?.({ runnerId }, 'terminating runner on shutdown');
        termDelivered.set(runnerId, handle.proc.terminate());
      }
    }
    const entries = [...this.runners.entries()];
    this.runners.clear();
    // Bounded exactly as the stop path is: a runner that survives SIGKILL must
    // not hang shutdown (a SIGTERM'd worker that never exits gets SIGKILLed by
    // its supervisor, losing the clean teardown entirely).
    await Promise.all(
      entries.map(async ([runnerId, handle]) => {
        const outcome = await this.awaitExitOrKill(
          runnerId,
          handle.proc,
          termDelivered.get(runnerId) ?? true,
        );
        if (outcome !== null) {
          this.opts.logger?.error?.({ runnerId, pid: handle.proc.pid }, outcome);
        }
      }),
    );
  }

  /**
   * Drain the parked exit reports to the log on shutdown. They were composed
   * because a runner CRASHED, and their only delivery path is the next hello —
   * which will never come once the worker is stopping. Discarding them silently
   * loses the sole record of why a runner died; logging is the last resort.
   */
  private logUnreportedExits(): void {
    for (const [runnerId, error] of [...this.unreportedExits.entries()]) {
      this.unreportedExits.delete(runnerId);
      this.opts.logger?.error?.(
        { runnerId, error },
        'worker stopped with an unreported runner exit; it was never delivered to the registry',
      );
    }
  }

  /**
   * Classify a disconnect reason to choose a reconnect cadence.
   *
   * Scoped to a post-hello {@link TunnelClosedError} — an ESTABLISHED tunnel
   * that dropped. The heuristic is about a healthy socket being recycled under
   * the worker, so it must never reach an {@link UpgradeRejectedError}: a
   * registry that is rejecting the upgrade is not recycling anything, and 502 is
   * exactly what a rolling deploy returns, so treating it as a recycle would pin
   * the whole fleet at the base interval for the length of the redeploy.
   *
   * Explicit recycle CLOSE CODES (`1012` service-restart / `1001` going-away)
   * get a prompt reconnect. So does an abrupt drop with no close frame — code
   * absent, or `1006` abnormal-closure — on a REMOTE registry, where that is an
   * ingress recycling a long-lived WebSocket and the tunnel must not be down
   * long enough to drop a launch. On a LOOPBACK registry there is no ingress; an
   * abrupt drop is a real condition and a tight reconnect loop would fuel a
   * re-registration flap, so back off normally there.
   *
   * Codes are compared NUMERICALLY against the close frame, never substring-
   * matched out of the composed message: the peer controls the close `reason`,
   * and `'1001'` / `'1012'` / `'502'` can all appear inside one.
   */
  private recycleReconnect(exc: unknown): boolean {
    if (!(exc instanceof TunnelClosedError)) {
      return false;
    }
    if (
      exc.code === RECYCLE_CLOSE_CODES.serviceRestart ||
      exc.code === RECYCLE_CLOSE_CODES.goingAway
    ) {
      return true;
    }
    const abrupt = exc.code === undefined || exc.code === RECYCLE_CLOSE_CODES.abnormal;
    return abrupt && !urlIsLoopback(this.opts.registryTunnelBaseUrl);
  }

  // ── Connect + serve ────────────────────────────────────

  /** Single connection attempt: connect, hello, serve. */
  private async connectAndServe(): Promise<void> {
    const url = this.tunnelUrl();
    const headers = this.buildConnectHeaders();
    this.opts.logger?.info?.({ url }, 'connecting to registry worker tunnel');

    let socket: WorkerSocket;
    try {
      socket = await this.connector.connect(url, headers);
    } catch (exc) {
      // The upgrade itself was rejected. Fail loud on a permanent status; let the
      // reconnect loop retry a transient one (or any non-upgrade transport error).
      if (exc instanceof UpgradeRejectedError) {
        const fatal = this.classifyHttpStatus(exc.status);
        if (fatal !== null) {
          throw fatal;
        }
      }
      throw exc;
    }

    try {
      await this.serveFrames(socket);
    } finally {
      // Drop the watchers' send target — exit reports raised between connections
      // park in unreportedExits instead of racing a half-closed socket.
      this.socket = undefined;
      socket.closeSocket();
    }
  }

  /**
   * Map a rejected-upgrade HTTP status to a fatal error, or `null` for a status
   * the reconnect loop should retry (retryable 4xx, or any non-4xx such as a 5xx
   * bounce). The Env-Key endpoint has no OAuth-login remedy, so the messages
   * point at the Env Key + registry URL rather than a login command.
   */
  private classifyHttpStatus(status: number): EnvironmentConnectError | null {
    if (RETRYABLE_UPGRADE_STATUSES.has(status) || !(status >= 400 && status < 500)) {
      return null;
    }
    if (status === 401) {
      return new EnvironmentConnectError(
        'Authentication failed (HTTP 401): the registry rejected the Environment Key. ' +
          'Check ENVIRONMENT_KEY matches the key armed for this environment.',
      );
    }
    if (status === 403) {
      return new EnvironmentConnectError(
        'Connection refused (HTTP 403): the credential authenticated, but the registry ' +
          'did not accept the worker tunnel. Confirm this environment is authorized and the ' +
          'registry is up to date (the /v1/tunnels/environments route), then retry.',
      );
    }
    return new EnvironmentConnectError(
      `Connection refused (HTTP ${status}): the registry rejected the worker tunnel request. ` +
        'This is a permanent error; retrying will not help. Check the registry URL and access.',
    );
  }

  /**
   * Announce readiness, then service worker frames until disconnect. Sends
   * `worker.hello`, flushes any parked exit reports, then loops dispatching
   * requests and answering pings until the connection closes.
   *
   * The loop must keep returning to `receive()`: the registry's liveness check
   * is the pong, so anything awaited here that can outlast the dead-worker
   * window takes the tunnel down. Handlers that can are dispatched off the loop
   * (see {@link serveRequestOffLoop}); the rest are bounded.
   */
  private async serveFrames(socket: WorkerSocket): Promise<void> {
    // Resolve the live runner set ONCE and reuse it for both the hello frame and
    // the banner count: aliveRunnerIds() prunes dead handles as a side effect, so
    // calling it twice would re-walk (and re-mutate) the map for no reason.
    const liveRunners = this.aliveRunnerIds();
    await socket.sendText(
      encodeWorkerFrame({
        kind: WorkerFrameKind.Hello,
        version: WORKER_VERSION,
        frameProtocolVersion: FRAME_PROTOCOL_VERSION,
        name: this.opts.name,
        runners: liveRunners,
        configuredHarnesses: this.opts.configuredHarnesses ?? null,
      }),
    );
    this.socket = socket;
    // Flush exit reports that raced a disconnect: a runner that died while the
    // tunnel was down would otherwise never be reported.
    for (const [runnerId, error] of [...this.unreportedExits.entries()]) {
      this.unreportedExits.delete(runnerId);
      await this.reportRunnerExit(runnerId, error);
    }
    this.opts.logger?.info?.(
      { environmentId: this.opts.environmentId, runners: liveRunners.length },
      'connected to registry; listening for sessions',
    );

    // A single in-flight receive() is reused across idle wakeups: each loop turn
    // races it against a fresh idle timer, so a timeout never consumes the
    // pending frame — the next turn awaits the SAME receive promise.
    const idleMs = this.opts.serveIdleTimeoutMs ?? SERVE_IDLE_TIMEOUT_MS;
    let pending: Promise<WorkerSocketMessage> | undefined;
    for (;;) {
      if (pending === undefined) {
        pending = socket.receive();
      }
      // Wake at least every `idleMs` even when the socket is silent. The tunnel's
      // liveness is server→worker pings answered inline and the ws adapter
      // surfacing error/close as a close message; this defensive wakeup is a
      // cancelable timer (its own seam, NOT the reconnect-backoff sleep), so a
      // hypothetical half-open socket that never emits a close/error can't park the
      // loop forever.
      const message = await raceIdle(pending, idleMs);
      if (message === IDLE_WAKEUP) {
        // Timer won the race; the receive() is still in flight — loop and await
        // it again (do NOT issue a second receive()).
        this.opts.onServeIdleForTest?.();
        continue;
      }
      pending = undefined;
      if (message.type === 'close') {
        // The socket closed: surface the close reason so the reconnect loop's
        // recycle heuristic can read it.
        throw new TunnelClosedError(message);
      }
      await this.handleRawMessage(socket, message.data);
    }
  }

  /**
   * Decode one inbound text frame and route it. Worker frames go to
   * {@link dispatchWorkerFrame}; a runner-tunnel ping is answered with a pong
   * inline; anything that decodes as neither is logged and dropped
   * (forward-compatible with frame types this worker version doesn't handle).
   *
   * Every `catch` here is scoped to a DECODE and nothing else. A `sendText` must
   * never sit inside one: a pong that cannot be written means the tunnel is
   * broken, and swallowing it leaves the worker looping in the belief that it is
   * answering keepalives while the registry counts missed pongs and fails every
   * in-flight launch/stop/stat against it as retryable. Like every other send in
   * this file, it propagates to the reconnect loop.
   */
  private async handleRawMessage(socket: WorkerSocket, raw: string): Promise<void> {
    const frame = tryDecodeWorkerFrame(raw);
    if (frame !== undefined) {
      await this.dispatchWorkerFrame(socket, frame);
      return;
    }
    // Not a worker frame — it may be a runner-tunnel ping (the tunnel multiplexes
    // both frame families over one socket).
    const runnerFrame = tryDecodeRunnerFrame(raw);
    if (runnerFrame === undefined) {
      this.opts.logger?.warn?.(
        { bytes: raw.length, preview: raw.slice(0, UNDECODABLE_FRAME_PREVIEW_CHARS) },
        'dropping an inbound frame that decodes as neither a worker nor a runner frame',
      );
      return;
    }
    if (runnerFrame.kind === FrameKind.Ping) {
      await socket.sendText(encodeFrame({ kind: FrameKind.Pong, ts: runnerFrame.ts }));
    }
  }

  /**
   * Handle a decoded worker frame and send its result back. Launch / stop spawn or
   * terminate a runner; stat / list_dir / create_dir inspect or create on the
   * worker filesystem; create_worktree / remove_worktree stage or tear down a git
   * worktree. Any other inbound frame — a result frame (which only flows
   * worker→registry), or a future request kind this worker version does not yet
   * handle — is ignored (forward-compatible: dropped, not errored).
   */
  private async dispatchWorkerFrame(socket: WorkerSocket, frame: WorkerFrame): Promise<void> {
    if (isOffLoopRequest(frame)) {
      // Answered OFF the serve loop — see {@link serveRequestOffLoop}.
      this.serveRequestOffLoop(socket, frame);
      return;
    }
    const result = await this.resolveWorkerFrame(frame);
    if (result !== undefined) {
      await socket.sendText(encodeWorkerFrame(result));
    }
  }

  /**
   * Run a SLOW request concurrently with the serve loop, sending its own result
   * frame (keyed by `requestId`) when it settles.
   *
   * The worktree ops shell out to `git`, bounded at `GIT_TIMEOUT_MS` (120s),
   * while the registry declares a worker dead after `PING_INTERVAL_MS *
   * PING_MISS_THRESHOLD` (90s). Running the worktree offload on a worker THREAD
   * frees the event loop, but awaiting it inline still parks the FRAME loop:
   * `receive()` is not called again until the handler returns, so a keepalive
   * ping sits unread in the socket's inbound queue for the whole git operation
   * and the worker is closed mid-op. Dispatching off the loop is what actually
   * keeps the tunnel answered — the loop returns to `receive()` immediately and
   * pongs land while git runs.
   *
   * Frames are requestId-correlated, so a result arriving after a later
   * request's is the protocol working as designed, not a reordering hazard.
   */
  private serveRequestOffLoop(socket: WorkerSocket, frame: OffLoopRequestFrame): void {
    const task = (async () => {
      // Nothing may escape a detached task: an unhandled rejection here takes
      // the whole worker process down, and would do so for a single failed
      // request. `resolveWorkerFrame` already answers what it can; this is the
      // last barrier.
      try {
        const result = await this.resolveWorkerFrame(frame);
        if (result === undefined) {
          return;
        }
        await socket.sendText(encodeWorkerFrame(result));
      } catch (exc) {
        // Either the tunnel dropped while the job ran — there is nothing to
        // park, the registry's waiter has already failed and the request is
        // retryable — or the handler failed in a way with no result frame to
        // send. Both are logged, neither is fatal.
        this.opts.logger?.warn?.(
          { err: exc, kind: frame.kind, requestId: frame.requestId },
          'a worker request served off the serve loop did not produce a delivered result',
        );
      }
    })().finally(() => {
      this.offLoopRequests.delete(task);
    });
    this.offLoopRequests.add(task);
  }

  /**
   * Produce the result frame for a request, or `undefined` for an inbound frame
   * with no result to send.
   *
   * NOTHING a handler throws escapes here. An unexpected throw used to unwind
   * through the serve loop into `run()`'s catch, where it was logged as
   * `'worker tunnel disconnected; reconnecting'` and left the request
   * unanswered — so the registry's waiter rejected with its tunnel-closed error,
   * which is classified RETRYABLE. That turns a permanent, actionable filesystem
   * failure into an infinitely retried "transient disconnect". Every request is
   * answered, and an unexpected failure is answered as `status: "failed"` with
   * the cause.
   */
  private async resolveWorkerFrame(frame: WorkerFrame): Promise<WorkerFrame | undefined> {
    try {
      if (frame.kind === WorkerFrameKind.LaunchRunner) {
        return await this.handleLaunch(frame);
      }
      if (frame.kind === WorkerFrameKind.StopRunner) {
        return await this.handleStop(frame);
      }
      if (frame.kind === WorkerFrameKind.Stat) {
        return handleStat(frame);
      }
      if (frame.kind === WorkerFrameKind.ListDir) {
        return handleListDir(frame);
      }
      if (frame.kind === WorkerFrameKind.CreateDir) {
        return handleCreateDir(frame);
      }
      if (frame.kind === WorkerFrameKind.CreateWorktree) {
        return await this.handleCreateWorktree(frame);
      }
      if (frame.kind === WorkerFrameKind.RemoveWorktree) {
        return await this.handleRemoveWorktree(frame);
      }
      return undefined;
    } catch (exc) {
      const failure = failedResultFor(frame, `worker request failed: ${errMessage(exc)}`);
      if (failure === undefined) {
        // Not a request frame: there is no requestId to answer, so the throw is
        // a genuine loop failure and stays one.
        throw exc;
      }
      this.opts.logger?.error?.(
        { err: exc, kind: frame.kind },
        'worker request handler failed unexpectedly; answering the request as failed',
      );
      return failure;
    }
  }

  // ── Filesystem + git-worktree handlers ─────────────────
  //
  // stat / list_dir / create_dir are pure filesystem reads/writes and delegate
  // straight to `fileops.ts` (synchronous, no event-loop concern). The worktree
  // ops shell out to `git`, which can be slow on a large repo, so keeping the
  // tunnel answered during git takes BOTH halves: {@link worktreeOffload} runs
  // the blocking `spawnSync` on a worker thread so the EVENT loop stays free,
  // and {@link serveRequestOffLoop} dispatches the request off the SERVE loop so
  // `receive()` keeps draining and a keepalive ping is read and ponged while the
  // job runs. Either half alone leaves the ping unanswered. The handlers then
  // translate a {@link WorktreeError} into a `status: "failed"` result.

  /**
   * Handle a `worker.create_worktree` request: create a git worktree with a new
   * branch checked out, off the source repo's MAIN work tree. A
   * {@link WorktreeError} (bad branch name, not a repo, unresolvable base, git
   * failure) becomes a `status: "failed"` result carrying the message.
   */
  private async handleCreateWorktree(
    frame: WorkerCreateWorktreeFrame,
  ): Promise<WorkerCreateWorktreeResultFrame> {
    let created: { worktreePath: string; branch: string };
    try {
      const value = await this.worktreeOffload(
        createJob({
          repoPath: frame.repoPath,
          branchName: frame.branchName,
          // `baseBranch` is optional on the frame (string | null | undefined);
          // collapse an absent value to null so `createWorktree` branches from
          // HEAD, the same as an explicit null.
          baseBranch: frame.baseBranch ?? null,
        }),
      );
      // A create job always yields a value; a null here would be a thread-protocol
      // bug, so fail loud rather than send a half-populated "ok" frame.
      if (value === null) {
        throw new Error('worktree thread returned no result for a create job');
      }
      created = value;
    } catch (exc) {
      if (exc instanceof WorktreeError) {
        return {
          kind: WorkerFrameKind.CreateWorktreeResult,
          requestId: frame.requestId,
          status: 'failed',
          worktreePath: null,
          branch: null,
          error: exc.message,
        };
      }
      throw exc;
    }
    this.opts.logger?.info?.(
      { worktreePath: created.worktreePath, branch: created.branch, repoPath: frame.repoPath },
      'created worktree',
    );
    return {
      kind: WorkerFrameKind.CreateWorktreeResult,
      requestId: frame.requestId,
      status: 'ok',
      worktreePath: created.worktreePath,
      branch: created.branch,
      error: null,
    };
  }

  /**
   * Handle a `worker.remove_worktree` request: remove the worktree directory and
   * (when `deleteBranch`) its branch, in that order. A {@link WorktreeError}
   * (missing path, git failure) becomes a `status: "failed"` result.
   */
  private async handleRemoveWorktree(
    frame: WorkerRemoveWorktreeFrame,
  ): Promise<WorkerRemoveWorktreeResultFrame> {
    try {
      await this.worktreeOffload(
        removeJob({
          worktreePath: frame.worktreePath,
          // Both are optional on the frame; collapse absent values to the
          // defaults (no branch deletion) so no explicit `undefined` crosses into
          // the job under exactOptionalPropertyTypes.
          branch: frame.branch ?? null,
          deleteBranch: frame.deleteBranch ?? false,
        }),
      );
    } catch (exc) {
      if (exc instanceof WorktreeError) {
        return {
          kind: WorkerFrameKind.RemoveWorktreeResult,
          requestId: frame.requestId,
          status: 'failed',
          error: exc.message,
        };
      }
      throw exc;
    }
    this.opts.logger?.info?.(
      {
        worktreePath: frame.worktreePath,
        deleteBranch: frame.deleteBranch ?? false,
        branch: frame.branch ?? null,
      },
      'removed worktree',
    );
    return {
      kind: WorkerFrameKind.RemoveWorktreeResult,
      requestId: frame.requestId,
      status: 'ok',
      error: null,
    };
  }

  private sleep(ms: number): Promise<void> {
    return this.delay(ms).promise;
  }

  /**
   * A CANCELABLE delay: the injected `sleep` seam when a test supplies one, else
   * a real timer whose handle is returned so a race that finishes early can
   * clear it. Left uncleared, a lost race keeps a live timer (and, in Node, the
   * event loop) alive for its full duration — the stop path would hold the
   * process open for the whole terminate grace after the runner had already gone.
   */
  private delay(ms: number): { promise: Promise<void>; cancel: () => void } {
    const seam = this.opts.sleep;
    if (seam !== undefined) {
      return { promise: seam(ms), cancel: () => {} };
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    const promise = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, ms);
    });
    return {
      promise,
      cancel: () => {
        if (timer !== undefined) {
          clearTimeout(timer);
        }
      },
    };
  }

  /** Whether `proc` exits within `ms`; the timer is cleared either way. */
  private async exitedWithin(proc: RunnerProcess, ms: number): Promise<boolean> {
    const exited = Symbol('exited');
    const timer = this.delay(ms);
    try {
      const winner = await Promise.race([proc.wait().then(() => exited), timer.promise]);
      return winner === exited;
    } finally {
      timer.cancel();
    }
  }

  // ── Test seams ─────────────────────────────────────────
  //
  // These expose the internal launch / serve / watcher paths to the unit suite
  // without a live socket. They are thin pass-throughs to the same private
  // methods the production path uses (no behavior of their own).

  /** Run {@link handleLaunch} for a constructed launch request (test seam). */
  async handleLaunchForTest(req: LaunchRequest): Promise<WorkerLaunchRunnerResultFrame> {
    return this.handleLaunch({
      kind: WorkerFrameKind.LaunchRunner,
      requestId: req.requestId,
      bindingToken: req.bindingToken,
      workspace: req.workspace,
      harness: req.harness ?? null,
    });
  }

  /** Seed an exit report as if it could not be sent while the tunnel was down. */
  parkUnreportedExitForTest(runnerId: string, error: string): void {
    this.unreportedExits.set(runnerId, error);
  }

  /** Run {@link serveFrames} against a fake socket (test seam). */
  serveFramesForTest(socket: WorkerSocket): Promise<void> {
    return this.serveFrames(socket);
  }

  /** Snapshot the parked-exit queue (test seam). */
  unreportedExitsForTest(): Record<string, string> {
    return Object.fromEntries(this.unreportedExits);
  }

  /** Await every in-flight exit watcher (test seam). */
  async drainWatchersForTest(): Promise<void> {
    await Promise.all([...this.watchers]);
  }

  /** Await every request being served off the serve loop (test seam). */
  async drainOffLoopRequestsForTest(): Promise<void> {
    await Promise.all([...this.offLoopRequests]);
  }
}

/**
 * A tunnel close surfaced as an error so the reconnect loop can classify it.
 *
 * The close CODE and reason are kept as structured fields, not just baked into
 * the message: the reconnect classifier reads {@link code}, so a server-supplied
 * `reason` string that happens to contain `1012` or `502` cannot masquerade as a
 * recycle close. Raised only AFTER a successful upgrade + hello, which is what
 * makes it the one error the recycle cadence applies to.
 */
class TunnelClosedError extends Error {
  /** WebSocket close code, or `undefined` for an abrupt drop with no close frame. */
  readonly code: number | undefined;
  /** Close reason as sent by the peer (or a transport error message); `''` when absent. */
  readonly reason: string;

  constructor(message: WorkerSocketMessage & { type: 'close' }) {
    const parts: string[] = ['worker tunnel closed'];
    if (message.code !== undefined) {
      parts.push(`code ${message.code}`);
    }
    if (message.reason !== undefined && message.reason !== '') {
      parts.push(message.reason);
    }
    super(parts.join(': '));
    this.name = 'TunnelClosedError';
    this.code = message.code;
    this.reason = message.reason ?? '';
  }
}

/**
 * Compose the human-readable error for a runner that died: exit code plus the
 * tail of the runner's captured output (the part that usually holds the cause).
 * The cause is carried in the report so a waiting client fails fast instead of
 * polling to a timeout; the output tail is captured in memory (not a log file).
 *
 * A process that never STARTED has neither — no output was produced and the exit
 * code is synthesized — so the OS spawn error takes precedence: it is the only
 * thing that distinguishes a missing binary from a non-executable one, a bad
 * cwd, or an fd/memory exhaustion. Without it a caller receives the same
 * causeless "exited with code 127" for all of them.
 */
function runnerExitError(exitCode: number | null, proc: RunnerProcess): string {
  const spawnError = proc.spawnError?.();
  let message =
    spawnError !== undefined ? 'runner process failed to start' : 'runner process exited';
  if (exitCode !== null) {
    message += ` with code ${exitCode}`;
  }
  if (spawnError !== undefined) {
    message += `: ${errMessage(spawnError)}`;
    const errno = (spawnError as NodeJS.ErrnoException).code;
    if (errno !== undefined && !message.includes(errno)) {
      message += ` (${errno})`;
    }
  }
  const running = proc.postSpawnError?.();
  if (running !== undefined) {
    message += `\n--- runner process error ---\n${errMessage(running)}`;
  }
  const tail = proc.outputTail?.() ?? '';
  if (tail.trim() !== '') {
    message += `\n--- runner output tail ---\n${tail}`;
  }
  return message;
}

/** Whether `url`'s host is loopback (`127.0.0.1` / `localhost` / `::1`). */
function urlIsLoopback(url: string): boolean {
  let candidate = url;
  if (!candidate.includes('://')) {
    candidate = `ws://${candidate}`;
  }
  try {
    const host = new URL(candidate).hostname;
    return host === '127.0.0.1' || host === 'localhost' || host === '::1';
  } catch {
    // Fail toward "remote", the safer default for the recycle heuristic.
    return false;
  }
}

/** Whether `path` exists and is a directory (after symlink resolution). */
function directoryExists(path: string): boolean {
  try {
    return existsSync(path) && statSync(path).isDirectory();
  } catch {
    return false;
  }
}

/** Sentinel resolved by {@link raceIdle} when the idle timer wins the race. */
const IDLE_WAKEUP = Symbol('idle-wakeup');

/**
 * Race a (persistent) receive against an idle timer. Resolves the received
 * message when it arrives first, or {@link IDLE_WAKEUP} when the timer wins — in
 * which case `pending` is still in flight and the caller re-awaits it. The
 * received value is never dropped on a timeout, so no frame is lost.
 *
 * The timer is a real `setTimeout` cleared the moment receive wins, so the
 * production path leaks no pending handle. This is deliberately NOT the
 * reconnect-backoff `sleep` seam: the idle wakeup is a cancelable per-iteration
 * timeout, not a recorded backoff sleep.
 */
function raceIdle(
  pending: Promise<WorkerSocketMessage>,
  ms: number,
): Promise<WorkerSocketMessage | typeof IDLE_WAKEUP> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const idle = new Promise<typeof IDLE_WAKEUP>((resolve) => {
    timer = setTimeout(() => resolve(IDLE_WAKEUP), ms);
  });
  return Promise.race([pending, idle]).finally(() => {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  });
}

/** Yield one timer tick (draining the microtask queue), so an instant child exit registers. */
function macrotaskTick(): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, 0));
}

/** Best-effort message extraction from an unknown thrown value. */
function errMessage(exc: unknown): string {
  if (exc instanceof Error) {
    return exc.message;
  }
  return String(exc);
}
