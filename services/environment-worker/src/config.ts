// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Environment-worker configuration, sourced entirely from the process
// environment. The worker is a long-running CLIENT (no HTTP server): a
// self-hosted operator runs it, and it dials the registry host tunnel and
// spawns session runners.

import { hostname } from 'node:os';

/**
 * Resolved configuration for an environment-worker process.
 *
 * Field meanings:
 * - `environmentId`   identity of this environment, presented to the registry.
 * - `environmentKey`  shared secret used to authenticate the host tunnel dial
 *                     (the self-hosted path). Required unless `environmentToken`
 *                     is set (the managed path — see below).
 * - `environmentToken` per-launch Environment Token a registry-launched worker
 *                     (a server-managed sandbox with no operator to provision
 *                     an Env Key) presents instead of `environmentKey`. `undefined`
 *                     for a self-hosted worker — the existing, unchanged path.
 * - `workerId`        server-assigned per-launch worker identity, injected
 *                     alongside `environmentToken`. Not read/required outside the
 *                     managed path; has no wire consumer yet (forwarded for a
 *                     future use, same posture as `local-environment-launcher.ts`'s
 *                     `ENVIRONMENT_WORKER_ID_ENV_VAR` forwarding).
 * - `registryTunnelBaseUrl` base URL of the registry host tunnel to dial
 *                     (e.g. `wss://registry.example.com` or an `http(s)://`
 *                     origin the tunnel client upgrades to a WebSocket).
 * - `registryRunnerUrl` base URL of the registry runner tunnel, handed to each
 *                     spawned runner so it dials back to the registry. Defaults
 *                     to `registryTunnelBaseUrl` when unset (single-registry
 *                     deployments share one base).
 * - `workspaceDir`    host directory under which session-runner workspaces are
 *                     created.
 * - `runnerLaunchCommand` argv used to launch a session runner; element [0] is
 *                     the executable and the remainder are its arguments.
 * - `name`            human-readable worker name announced to the registry in the
 *                     host hello. Defaults to the OS hostname when unset — UNLESS
 *                     `environmentToken` is set, in which case it is the
 *                     server-injected identity and REQUIRED (no hostname
 *                     fallback: a managed sandbox's hostname is meaningless).
 */
export interface EnvironmentWorkerConfig {
  readonly environmentId: string;
  readonly environmentKey: string | undefined;
  readonly environmentToken: string | undefined;
  readonly workerId: string | undefined;
  readonly registryTunnelBaseUrl: string;
  readonly registryRunnerUrl: string;
  readonly workspaceDir: string;
  readonly runnerLaunchCommand: readonly string[];
  readonly name: string;
}

export const ENVIRONMENT_ID_ENV_VAR = 'ENVIRONMENT_ID';
export const ENVIRONMENT_KEY_ENV_VAR = 'ENVIRONMENT_KEY';
export const REGISTRY_TUNNEL_BASE_URL_ENV_VAR = 'REGISTRY_TUNNEL_BASE_URL';
export const REGISTRY_RUNNER_URL_ENV_VAR = 'REGISTRY_RUNNER_URL';
export const WORKSPACE_DIR_ENV_VAR = 'WORKSPACE_DIR';
export const RUNNER_LAUNCH_COMMAND_ENV_VAR = 'RUNNER_LAUNCH_COMMAND';
export const ENVIRONMENT_WORKER_NAME_ENV_VAR = 'ENVIRONMENT_WORKER_NAME';
/**
 * Server-assigned per-launch worker id, injected alongside
 * {@link ORCA_ENVIRONMENT_TOKEN_ENV_VAR} on the managed path. This is the
 * SAME variable name `local-environment-launcher.ts` already forwards for
 * `WorkerIdentity.workerId` (services/registry-service-ts and
 * environment-worker are separate deployable services, so the name is
 * duplicated here by convention rather than imported — see that module's doc).
 */
export const ENVIRONMENT_WORKER_ID_ENV_VAR = 'ENVIRONMENT_WORKER_ID';
/**
 * Per-launch Environment Token: the managed-auth alternative to
 * {@link ENVIRONMENT_KEY_ENV_VAR} for a registry-launched worker. When set,
 * the worker authenticates the tunnel dial with this token instead of an Env
 * Key, and its identity is read from {@link ENVIRONMENT_WORKER_ID_ENV_VAR} /
 * {@link ENVIRONMENT_WORKER_NAME_ENV_VAR} (both required in that case) rather
 * than falling back to the OS hostname.
 */
export const ORCA_ENVIRONMENT_TOKEN_ENV_VAR = 'ORCA_ENVIRONMENT_TOKEN';

function requireEnv(name: string, env: NodeJS.ProcessEnv): string {
  const value = env[name];
  if (value === undefined || value.trim() === '') {
    throw new Error(`missing required environment variable: ${name}`);
  }
  return value;
}

/**
 * Split a launch command string into an argv array on whitespace.
 *
 * This is intentionally simple (no shell quoting): operators that need
 * arguments containing spaces should wrap the worker in a launcher script.
 */
function parseLaunchCommand(raw: string): string[] {
  const argv = raw
    .trim()
    .split(/\s+/)
    .filter((part) => part.length > 0);
  if (argv.length === 0) {
    throw new Error(`${RUNNER_LAUNCH_COMMAND_ENV_VAR} must contain a command`);
  }
  return argv;
}

/** Return a trimmed env value, or `undefined` when unset or blank. */
function optionalEnv(name: string, env: NodeJS.ProcessEnv): string | undefined {
  const value = env[name];
  return value === undefined || value.trim() === '' ? undefined : value;
}

/**
 * Read and validate the worker configuration from `env` (defaults to
 * `process.env`). Throws if any required variable is missing or empty.
 *
 * `registryRunnerUrl` defaults to `registryTunnelBaseUrl` (one registry base for
 * both tunnels) and `name` to the OS hostname, so a minimal self-hosted
 * deployment sets only the five required vars.
 *
 * Managed-auth fork: when {@link ORCA_ENVIRONMENT_TOKEN_ENV_VAR} is set (a
 * registry-launched worker), {@link ENVIRONMENT_KEY_ENV_VAR} is no longer
 * required, and identity is instead REQUIRED from the server-injected
 * {@link ENVIRONMENT_WORKER_ID_ENV_VAR} / {@link ENVIRONMENT_WORKER_NAME_ENV_VAR}
 * — no hostname fallback, since a managed sandbox's identity is the
 * registry's to assign, not the sandbox's own hostname. Absent the token,
 * every var and default resolves exactly as before (the self-hosted path is
 * unaffected).
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): EnvironmentWorkerConfig {
  const registryTunnelBaseUrl = requireEnv(REGISTRY_TUNNEL_BASE_URL_ENV_VAR, env);
  const environmentToken = optionalEnv(ORCA_ENVIRONMENT_TOKEN_ENV_VAR, env);
  const managed = environmentToken !== undefined;
  return {
    environmentId: requireEnv(ENVIRONMENT_ID_ENV_VAR, env),
    environmentKey: managed
      ? optionalEnv(ENVIRONMENT_KEY_ENV_VAR, env)
      : requireEnv(ENVIRONMENT_KEY_ENV_VAR, env),
    environmentToken,
    workerId: managed
      ? requireEnv(ENVIRONMENT_WORKER_ID_ENV_VAR, env)
      : optionalEnv(ENVIRONMENT_WORKER_ID_ENV_VAR, env),
    registryTunnelBaseUrl,
    registryRunnerUrl: optionalEnv(REGISTRY_RUNNER_URL_ENV_VAR, env) ?? registryTunnelBaseUrl,
    workspaceDir: requireEnv(WORKSPACE_DIR_ENV_VAR, env),
    runnerLaunchCommand: parseLaunchCommand(requireEnv(RUNNER_LAUNCH_COMMAND_ENV_VAR, env)),
    name: managed
      ? requireEnv(ENVIRONMENT_WORKER_NAME_ENV_VAR, env)
      : (optionalEnv(ENVIRONMENT_WORKER_NAME_ENV_VAR, env) ?? hostname()),
  };
}
