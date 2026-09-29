// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Shared `environment-worker` dial-back env-var contract every
// `EnvironmentLauncher` backend wires identically (Local, E2B, OpenSandbox,
// ...). Mirrors services/environment-worker/src/config.ts's *_ENV_VAR
// constants, duplicated here by convention rather than imported —
// registry-service-ts and environment-worker are separate deployable
// services (see the repo's services/ vs packages/ boundary in CLAUDE.md). A
// future change to environment-worker's expected vars must update both.
//
// Shared here, rather than defined in `local-environment-launcher.ts`, so the
// cloud backends use the SAME names + assembly logic instead of redefining
// them; `local-environment-launcher.ts` re-exports these same symbols so
// imports from that module path keep working.

import type { StartWorkerOptions } from './types.js';

export const ENVIRONMENT_ID_ENV_VAR = 'ENVIRONMENT_ID';
export const REGISTRY_TUNNEL_BASE_URL_ENV_VAR = 'REGISTRY_TUNNEL_BASE_URL';
export const WORKSPACE_DIR_ENV_VAR = 'WORKSPACE_DIR';
export const RUNNER_LAUNCH_COMMAND_ENV_VAR = 'RUNNER_LAUNCH_COMMAND';
export const ENVIRONMENT_WORKER_NAME_ENV_VAR = 'ENVIRONMENT_WORKER_NAME';
/**
 * Carries `identity.workerId` through so it is never silently dropped;
 * environment-worker/config.ts requires it on the managed (Environment Token)
 * path.
 */
export const ENVIRONMENT_WORKER_ID_ENV_VAR = 'ENVIRONMENT_WORKER_ID';
/**
 * Mirrors environment-worker/config.ts's `ORCA_ENVIRONMENT_TOKEN_ENV_VAR` —
 * the managed-auth alternative to the self-hosted `ENVIRONMENT_KEY` a
 * registry-launched worker presents instead. Every environment ANY
 * `EnvironmentLauncher` backend starts is registry-launched (an operator
 * never hand-provisions a managed environment's Env Key), so
 * `StartWorkerOptions.token` — the per-launch Environment Token
 * `EnvironmentTokenStore` mints — always belongs here, never in an
 * `ENVIRONMENT_KEY` slot no backend sets.
 */
export const ORCA_ENVIRONMENT_TOKEN_ENV_VAR = 'ORCA_ENVIRONMENT_TOKEN';

/**
 * Build the 7 dial-back env vars every backend wires into the
 * spawned/exec'd `environment-worker` identically. Callers that need to
 * layer these onto an inherited base env (Local spawns a REAL child process
 * that also needs `PATH`/`HOME`/... from the registry host) merge this
 * AFTER their base env so these vars always win; a cloud backend (whose
 * exec'd box has no reason to see the registry process's own env at all)
 * uses the result directly.
 */
export function dialBackEnv(
  opts: StartWorkerOptions,
  workspaceDir: string,
  runnerLaunchCommand: readonly string[],
): Record<string, string> {
  return {
    [REGISTRY_TUNNEL_BASE_URL_ENV_VAR]: opts.registryTunnelUrl,
    [ENVIRONMENT_ID_ENV_VAR]: opts.environmentId,
    [ORCA_ENVIRONMENT_TOKEN_ENV_VAR]: opts.token,
    [WORKSPACE_DIR_ENV_VAR]: workspaceDir,
    [RUNNER_LAUNCH_COMMAND_ENV_VAR]: runnerLaunchCommand.join(' '),
    [ENVIRONMENT_WORKER_NAME_ENV_VAR]: opts.identity.workerName,
    [ENVIRONMENT_WORKER_ID_ENV_VAR]: opts.identity.workerId,
  };
}
