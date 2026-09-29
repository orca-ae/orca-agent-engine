// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Shared EnvironmentLauncher core for every cloud sandbox-provider backend
// (E2B, OpenSandbox, ...).
//
// `@orca/cloud-sandbox`'s `SandboxRuntime` already abstracts "create a box,
// get a handle that can exec a command and be destroyed" identically across
// E2B and OpenSandbox (that abstraction is the whole point of the package —
// see its module doc). Both backends need EXACTLY that primitive plus the
// SAME dial-back env (worker-env.ts) and the SAME "run environment-worker
// detached, track it, destroy the box to tear down" lifecycle — there is no
// provider-specific LOGIC left to differ once `provision`/`startWorker` are
// expressed against `SandboxRuntime`/`SandboxHandle` rather than a raw SDK.
// So one class implements the lifecycle; `e2b-environment-launcher.ts` and
// `opensandbox-environment-launcher.ts` are thin wrappers that construct the
// right `SandboxRuntime` from provider-specific config (API keys, template
// ids, server URLs) and delegate every `EnvironmentLauncher` method here.
// One exec-model launcher shape serves every cloud provider, while each
// provider keeps its own config surface + exported class name, which is
// what `launcherFactory` and callers actually need to tell them apart.
//
// Provisioning model — "exec the worker" (see types.ts's module doc for the
// entrypoint-as-worker/exec-the-worker distinction): `provision` acquires a
// bare box via the injected `SandboxRuntime` (no packages/image overrides —
// those are per-SESSION `EnvironmentSpec` concerns; an Environment-level box
// takes its image from the runtime's OWN constructor config). `startWorker`
// then execs `environment-worker` INSIDE that box, detached (`nohup ... &`)
// so the blocking bash tool-call returns immediately while the worker keeps
// running — the box must already have `environment-worker` (and, for when
// it in turn spawns session-runner per session, `session-runner`) installed,
// e.g. via a custom Orca Environment template/image, exactly like the E2B
// `orca-default` template already bakes in FUSE tooling for session sandboxes
// today. Building/publishing that image is an infra follow-up, not this
// module's concern — this module only supplies the launcher-side wiring.

import type { SandboxHandle, SandboxRuntime } from '@orca/sandbox-runtime';
import type { EnvironmentLauncher, StartWorkerOptions } from './types.js';
import { dialBackEnv } from './worker-env.js';

/** Default workspace dir inside the box, under which environment-worker creates session-runner workspaces. Override per deployment if the Environment image uses a different layout. */
export const DEFAULT_CLOUD_WORKSPACE_DIR = '/home/user/orca-environment';

/** Default path (inside the box) the exec'd environment-worker's stdout/stderr are redirected to. */
const DEFAULT_WORKER_LOG_PATH = '/tmp/orca-environment-worker.log';

/** Construction options for {@link CloudEnvironmentLauncher}. */
export interface CloudEnvironmentLauncherOptions {
  /** The provider-specific `SandboxRuntime` (E2B, OpenSandbox, or a fake for tests) that provisions the box. */
  readonly runtime: SandboxRuntime;
  /**
   * Argv used to exec `environment-worker` INSIDE the box; element 0 is the
   * executable (e.g. `['node', '/opt/orca/environment-worker/dist/main.js']`
   * baked into the Environment image).
   */
  readonly workerLaunchCommand: readonly string[];
  /**
   * Argv handed to the exec'd worker as its own `RUNNER_LAUNCH_COMMAND` (the
   * session-runner launch argv it in turn spawns, inside the SAME box, per
   * session).
   */
  readonly runnerLaunchCommand: readonly string[];
  /** Workspace dir inside the box. Defaults to {@link DEFAULT_CLOUD_WORKSPACE_DIR}. */
  readonly workspaceDir?: string;
  /** Provider label used in error messages (e.g. `'E2B'`, `'OpenSandbox'`). Defaults to `'Cloud'`. */
  readonly label?: string;
}

/**
 * Shared cloud backend: an Environment is a provider sandbox (E2B micro-VM,
 * OpenSandbox container, ...), and its worker is `environment-worker` exec'd
 * detached inside that same sandbox. See the module doc for why one class
 * backs every cloud provider.
 */
export class CloudEnvironmentLauncher implements EnvironmentLauncher {
  private readonly runtime: SandboxRuntime;
  private readonly workerLaunchCommand: readonly string[];
  private readonly runnerLaunchCommand: readonly string[];
  private readonly workspaceDir: string;
  private readonly label: string;
  private readonly handles = new Map<string, SandboxHandle>();
  /**
   * Environment ids with a successful, not-yet-terminated `startWorker`.
   * Unlike `LocalEnvironmentLauncher.isRunning` (which polls the REAL child
   * process's exit code because that process lives in this SAME machine's
   * process table), a cloud sandbox's worker liveness can only be checked
   * with a round trip INTO the box — and nothing in today's call graph reads
   * `EnvironmentLauncher.isRunning` at all (`EnvironmentLaunchLifecycle`'s
   * wait-online instead polls whether the worker has actually dialed the
   * registry tunnel, which is the real liveness signal for "is this
   * environment usable"). So this tracks the launcher's OWN bookkeeping
   * (started-and-not-yet-terminated) rather than adding an unexercised
   * remote probe; a future caller that needs true remote liveness should add
   * an explicit health-check method rather than overload this one.
   */
  private readonly started = new Set<string>();

  constructor(opts: CloudEnvironmentLauncherOptions) {
    this.runtime = opts.runtime;
    this.workerLaunchCommand = opts.workerLaunchCommand;
    this.runnerLaunchCommand = opts.runnerLaunchCommand;
    this.workspaceDir = opts.workspaceDir ?? DEFAULT_CLOUD_WORKSPACE_DIR;
    this.label = opts.label ?? 'Cloud';
  }

  /**
   * Acquire a bare box via the injected `SandboxRuntime` and return its
   * sandbox id. `name` is accepted for interface parity with other backends
   * but not forwarded — `EnvironmentSpec` has no label field, and an
   * Environment-level box's image comes from the runtime's OWN constructor
   * config, not a per-acquire override (mirrors `LocalEnvironmentLauncher`,
   * which likewise ignores `name`).
   */
  async provision(_name: string): Promise<string> {
    const handle = await this.runtime.acquire({});
    this.handles.set(handle.id, handle);
    return handle.id;
  }

  async startWorker(environmentId: string, opts: StartWorkerOptions): Promise<void> {
    const handle = this.requireHandle(environmentId);
    const env = dialBackEnv(opts, this.workspaceDir, this.runnerLaunchCommand);
    const command = buildWorkerExecCommand(this.workerLaunchCommand, env);
    const result = await handle.run({ tool: 'bash', args: { command } });
    if (result.exit_code !== undefined && result.exit_code !== 0) {
      throw new Error(
        `${this.label}EnvironmentLauncher: failed to start environment-worker in sandbox ` +
          `${JSON.stringify(environmentId)} (exit_code=${result.exit_code}): ${result.stderr ?? ''}`,
      );
    }
    this.started.add(environmentId);
  }

  /** Idempotent: terminating an unknown or already-terminated id is a no-op. */
  async terminate(environmentId: string): Promise<void> {
    const handle = this.handles.get(environmentId);
    if (handle === undefined) {
      return;
    }
    // Pop the handle BEFORE destroying, matching LocalEnvironmentLauncher's
    // terminate — a subsequent isRunning() reads "not running" immediately
    // rather than racing the destroy.
    this.handles.delete(environmentId);
    this.started.delete(environmentId);
    await handle.destroy();
  }

  async isRunning(environmentId: string): Promise<boolean> {
    return this.started.has(environmentId);
  }

  /**
   * This launcher only ever destroys a box (`terminate`) — it never pauses
   * one, since the `EnvironmentLauncher` contract has no `pause` method for
   * anything to call. The underlying SDKs (E2B, OpenSandbox) DO expose a
   * pause/resume capability on the acquired `SandboxHandle`, but nothing in
   * this launcher's lifecycle ever produces a paused-and-resumable id to
   * resume FROM. Always rejects, like `LocalEnvironmentLauncher.resume`; a
   * future feature that adds explicit pause support should be the trigger to
   * wire a real resume here.
   */
  async resume(environmentId: string): Promise<void> {
    throw new Error(
      `${this.label}EnvironmentLauncher does not support resume (environment ${JSON.stringify(
        environmentId,
      )}): this launcher only ever destroys an environment's sandbox, never pauses it, so there is no in-place-stopped state to resume from — provision + startWorker a fresh one instead.`,
    );
  }

  private requireHandle(environmentId: string): SandboxHandle {
    const handle = this.handles.get(environmentId);
    if (handle === undefined) {
      throw new Error(
        `${this.label}EnvironmentLauncher: unknown environment ${JSON.stringify(environmentId)} — call provision() first`,
      );
    }
    return handle;
  }
}

/** Single-quote a shell word, escaping any embedded single quote (POSIX `'\''` trick). */
function shellQuote(value: string): string {
  return `'${value.replace(/'/g, String.raw`'\''`)}'`;
}

/**
 * Build the single shell command `startWorker` execs inside the box: launch
 * `workerLaunchCommand` with `env` set via `env KEY='value' ...`, detached
 * via `nohup ... &` so the blocking bash tool-call (`SandboxHandle.run`'s
 * `bash` tool has no separate "background" flag — see
 * `@orca/cloud-sandbox`'s runtimes) returns immediately while
 * environment-worker keeps running, redirecting its output to a log file an
 * operator can inspect (`< /dev/null` so the detached process never blocks
 * waiting on stdin). Exported so it is unit-testable as a pure function,
 * independent of any SandboxHandle fake.
 */
export function buildWorkerExecCommand(
  workerLaunchCommand: readonly string[],
  env: Record<string, string>,
  logPath: string = DEFAULT_WORKER_LOG_PATH,
): string {
  const envAssignments = Object.entries(env)
    .map(([key, value]) => `${key}=${shellQuote(value)}`)
    .join(' ');
  const command = workerLaunchCommand.map(shellQuote).join(' ');
  return `env ${envAssignments} nohup ${command} < /dev/null > ${shellQuote(logPath)} 2>&1 &`;
}
