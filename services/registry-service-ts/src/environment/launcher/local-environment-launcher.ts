// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Local environment-launcher backend.
//
// `provision` creates a local work-dir as the Environment; `startWorker` spawns
// `environment-worker` as a local child process wired to dial the registry —
// the programmatic equivalent of what `services/dev/scripts/start-self-hosted.sh`
// does by hand for a single self-hosted worker (create the environment, then
// `RUNNER_LAUNCH_COMMAND=... ENVIRONMENT_ID=... start_service environment-worker
// node .../dist/main.js`). The process spawner is injected (see
// `process-spawner.ts`) so unit tests assert the exact spawn command + env
// without ever starting a real process.
//
// The env vars set on the spawned child mirror
// `services/environment-worker/src/config.ts`'s expected variable NAMES,
// duplicated here by convention rather than imported: `registry-service-ts` and
// `environment-worker` are separate deployable services (see the repo's
// services/ vs packages/ boundary in CLAUDE.md), so this module does not take a
// source dependency on environment-worker — exactly like
// start-self-hosted.sh already duplicates these same var names as plain shell
// assignments across the same service boundary. A future change to
// environment-worker's expected vars must update both.
//
// `StartWorkerOptions.identity` carries two fields (`workerId`, `workerName`),
// forwarded as `ENVIRONMENT_WORKER_ID` / `ENVIRONMENT_WORKER_NAME`. The worker
// announces `workerName` in its hello; it requires `workerId` on the managed
// (Environment Token) path and retains it, but the hello frame carries only
// the name. The launcher forwards the id anyway rather than silently dropping
// identity the caller passed in.

import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { newId } from '../../domain/versioning.js';
import {
  ChildProcessSpawner,
  type ProcessSpawner,
  type SpawnedProcess,
} from './process-spawner.js';
import type { EnvironmentLauncher, StartWorkerOptions } from './types.js';
import {
  dialBackEnv,
  ENVIRONMENT_ID_ENV_VAR,
  ENVIRONMENT_WORKER_ID_ENV_VAR,
  ENVIRONMENT_WORKER_NAME_ENV_VAR,
  ORCA_ENVIRONMENT_TOKEN_ENV_VAR,
  REGISTRY_TUNNEL_BASE_URL_ENV_VAR,
  RUNNER_LAUNCH_COMMAND_ENV_VAR,
  WORKSPACE_DIR_ENV_VAR,
} from './worker-env.js';

// Re-exported so imports of these symbols from THIS module path keep working:
// they live in worker-env.ts so the cloud backends (E2B, OpenSandbox) share
// the same names + assembly logic instead of redefining them.
export {
  ENVIRONMENT_ID_ENV_VAR,
  ENVIRONMENT_WORKER_ID_ENV_VAR,
  ENVIRONMENT_WORKER_NAME_ENV_VAR,
  ORCA_ENVIRONMENT_TOKEN_ENV_VAR,
  REGISTRY_TUNNEL_BASE_URL_ENV_VAR,
  RUNNER_LAUNCH_COMMAND_ENV_VAR,
  WORKSPACE_DIR_ENV_VAR,
};

/** Construction options for {@link LocalEnvironmentLauncher}. */
export interface LocalEnvironmentLauncherOptions {
  /** Base directory under which each provisioned environment gets its own work-dir. */
  readonly baseDir: string;
  /**
   * Argv used to launch `environment-worker`; element 0 is the executable
   * (e.g. `['node', '/abs/path/services/environment-worker/dist/main.js']`).
   */
  readonly workerLaunchCommand: readonly string[];
  /**
   * Argv handed to the spawned worker as its own `RUNNER_LAUNCH_COMMAND` (the
   * session-runner launch argv it in turn spawns per session).
   */
  readonly runnerLaunchCommand: readonly string[];
  /** Process seam. Defaults to the production `ChildProcessSpawner`. */
  readonly spawner?: ProcessSpawner;
  /** Base env the spawned worker inherits before the wiring vars are layered on. Defaults to `process.env`. */
  readonly baseEnv?: NodeJS.ProcessEnv;
}

/**
 * Local backend: an Environment is a directory on this machine, and its
 * worker is a plain child process. Not resumable (there is no stop/resume
 * lifecycle for a local directory + process — terminating ends it for good).
 */
export class LocalEnvironmentLauncher implements EnvironmentLauncher {
  private readonly opts: LocalEnvironmentLauncherOptions;
  private readonly spawner: ProcessSpawner;
  private readonly workDirs = new Map<string, string>();
  private readonly processes = new Map<string, SpawnedProcess>();

  constructor(opts: LocalEnvironmentLauncherOptions) {
    this.opts = opts;
    this.spawner = opts.spawner ?? new ChildProcessSpawner();
  }

  /**
   * Create the environment's local work-dir and return its id. `name` is
   * accepted for interface parity with other backends (which may use it as a
   * provider-facing label) but is not embedded in the id — mirrors
   * `newId`'s use elsewhere in the registry, where a row's id and its
   * human-readable `name` are always orthogonal.
   */
  async provision(_name: string): Promise<string> {
    const id = newId('local');
    const dir = join(this.opts.baseDir, id);
    mkdirSync(dir, { recursive: true });
    this.workDirs.set(id, dir);
    return id;
  }

  async startWorker(environmentId: string, opts: StartWorkerOptions): Promise<void> {
    const workDir = this.workDirs.get(environmentId);
    if (workDir === undefined) {
      throw new Error(
        `LocalEnvironmentLauncher: unknown environment ${JSON.stringify(environmentId)} — call provision() first`,
      );
    }

    // Local spawns a REAL child process, so unlike the cloud backends it
    // must inherit the registry host's own env first (PATH, HOME, ...) —
    // the shared dial-back vars are layered on top so they always win.
    const baseEnv = this.opts.baseEnv ?? process.env;
    const env: Record<string, string> = {};
    for (const [key, value] of Object.entries(baseEnv)) {
      if (value !== undefined) {
        env[key] = value;
      }
    }
    Object.assign(env, dialBackEnv(opts, workDir, this.opts.runnerLaunchCommand));

    const proc = this.spawner.spawn({
      command: this.opts.workerLaunchCommand,
      env,
      cwd: workDir,
    });
    this.processes.set(environmentId, proc);
  }

  /** Idempotent: terminating an unknown or already-terminated id is a no-op. */
  async terminate(environmentId: string): Promise<void> {
    const proc = this.processes.get(environmentId);
    if (proc === undefined) {
      return;
    }
    // Pop the handle BEFORE terminating, matching environment-worker's own
    // stop handling — a subsequent isRunning() reads "not running" immediately
    // rather than racing the child's actual exit.
    this.processes.delete(environmentId);
    if (proc.poll() === null) {
      proc.terminate();
      await proc.wait();
    }
  }

  async isRunning(environmentId: string): Promise<boolean> {
    const proc = this.processes.get(environmentId);
    return proc !== undefined && proc.poll() === null;
  }

  /**
   * Local environments have no stop/resume lifecycle — a directory + process
   * is either running or gone. Always rejects: there is no stopped sandbox to
   * resume.
   */
  async resume(environmentId: string): Promise<void> {
    throw new Error(
      `LocalEnvironmentLauncher does not support resume (environment ${JSON.stringify(
        environmentId,
      )}): local environments are not resumable — provision + startWorker a fresh one instead.`,
    );
  }
}
