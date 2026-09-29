// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Environment launcher framework — provider-agnostic lifecycle + start-worker
// contract for running `environment-worker` inside one Environment backend
// (a local child process, an E2B/OpenSandbox sandbox, ...).
//
// Everything provider-specific lives behind an `EnvironmentLauncher`
// implementation (see `local-environment-launcher.ts` and
// `cloud-environment-launcher.ts`); everything provider-agnostic (which backend
// to use, how the caller sequences provision → startWorker) lives above this
// seam. The seam is only the lifecycle + start-worker contract — it carries no
// exec-transport primitives (run / put / stream-exec), because no caller needs
// them: a backend gets `environment-worker` running itself, either by
// spawning it directly (Local) or by exec'ing it detached inside the box
// through the box's own `SandboxHandle` (exec-the-worker, the cloud
// backends). An image that boots straight into `environment-worker` as its
// entrypoint (entrypoint-as-worker) needs no exec at all.

/**
 * Identity the spawned `environment-worker` announces when it dials the
 * registry back.
 *
 * `workerId` is the per-launch identity minted by the caller; `workerName` is
 * the human-readable label reported in the worker's hello and is the value
 * `environment-worker` actually reads today via `ENVIRONMENT_WORKER_NAME`
 * (see `local-environment-launcher.ts`'s module doc for how each field is
 * wired into the spawned process).
 */
export interface WorkerIdentity {
  readonly workerId: string;
  readonly workerName: string;
}

/** Inputs to {@link EnvironmentLauncher.startWorker}. */
export interface StartWorkerOptions {
  /** The per-launch Environment token (minted by `EnvironmentTokenStore`; opaque to the launcher). */
  readonly token: string;
  /** Base URL of the registry worker tunnel the spawned `environment-worker` dials. */
  readonly registryTunnelUrl: string;
  /** Registry Environment id the spawned worker presents when it dials back. */
  readonly environmentId: string;
  /** Identity the spawned worker announces. */
  readonly identity: WorkerIdentity;
}

/**
 * Provider-agnostic lifecycle + start-worker contract for one Environment
 * backend. There is no preflight step before `provision`: no backend needs
 * one.
 */
export interface EnvironmentLauncher {
  /**
   * Create the Environment (VM / box / local dir) and return the launcher's
   * own id for it. `name` is a human-readable label, not necessarily embedded
   * in the returned id (provider-assigned sandbox ids routinely differ from
   * the label passed in).
   */
  provision(name: string): Promise<string>;
  /** Start `environment-worker` inside the environment identified by `environmentId`. */
  startWorker(environmentId: string, opts: StartWorkerOptions): Promise<void>;
  /** Tear down the environment, releasing its resources. Safe to call more than once. */
  terminate(environmentId: string): Promise<void>;
  /** Whether the environment's worker process is currently running. */
  isRunning(environmentId: string): Promise<boolean>;
  /**
   * Resume a stopped, resumable environment in place. Optional: a backend with
   * no stop/resume lifecycle (e.g. Local) omits real support and rejects
   * instead.
   */
  resume?(environmentId: string): Promise<void>;
}
