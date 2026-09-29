// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Environment-launcher backend factory.
//
// Selects an `EnvironmentLauncher` implementation by backend name: 'local'
// (A1), 'e2b' / 'opensandbox' (A2). Provider creds/config come from the
// registry's own process env — read here, not baked into the shared
// `EnvironmentLauncher` contract — mirroring how
// `registry-service-ts/src/config.ts` reads every other provider setting.
//
// Deployment-GLOBAL provider vars (`E2B_API_KEY`, `E2B_ENVIRONMENT_BASE_URL`,
// `OPEN_SANDBOX_DOMAIN`, `OPEN_SANDBOX_PROTOCOL`, `OPEN_SANDBOX_API_KEY`)
// legitimately reuse the SAME names harness-server's `src/config.ts` already
// reads for its own (session-sandbox) E2B/OpenSandbox runtimes — an operator
// sets one `E2B_API_KEY` / one OpenSandbox server for the whole deployment,
// not a registry-specific copy.
//
// The BOOT IMAGE vars do NOT: harness-server's `E2B_TEMPLATE_ID` /
// `OPEN_SANDBOX_IMAGE` select its own `orca-default` FUSE/tool-exec sandbox
// template, a completely different image than the (future) Orca Environment
// image this launcher needs (environment-worker + session-runner pre-baked).
// Reusing those two names here would be a config footgun: a single
// `E2B_TEMPLATE_ID` set "for the deployment" would silently misconfigure
// whichever use case didn't get it, surfacing only as a runtime exec failure
// inside the box. So this factory reads its OWN
// `E2B_ENVIRONMENT_TEMPLATE_ID` / `OPEN_SANDBOX_ENVIRONMENT_IMAGE` instead —
// distinct names, following the SAME `*_ENVIRONMENT_*` prefix already used
// below for the (also registry-launcher-specific, no harness-server analog)
// `*_ENVIRONMENT_WORKER_COMMAND` / `*_ENVIRONMENT_RUNNER_COMMAND` /
// `LOCAL_ENVIRONMENT_*` vars this factory established for A1.

import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  E2BEnvironmentLauncher,
  type E2BEnvironmentLauncherOptions,
} from './e2b-environment-launcher.js';
import {
  LocalEnvironmentLauncher,
  type LocalEnvironmentLauncherOptions,
} from './local-environment-launcher.js';
import {
  OpenSandboxEnvironmentLauncher,
  type OpenSandboxEnvironmentLauncherOptions,
} from './opensandbox-environment-launcher.js';
import type { EnvironmentLauncher } from './types.js';

/** Known launcher backend names. */
const KNOWN_BACKENDS: readonly string[] = ['local', 'e2b', 'opensandbox'];

/** Base directory under which each provisioned Local environment's work-dir is created. Optional. */
export const LOCAL_ENVIRONMENT_BASE_DIR_ENV_VAR = 'LOCAL_ENVIRONMENT_BASE_DIR';
/** Argv (space-separated) used to launch `environment-worker`. Required for the 'local' backend. */
export const LOCAL_ENVIRONMENT_WORKER_COMMAND_ENV_VAR = 'LOCAL_ENVIRONMENT_WORKER_COMMAND';
/** Argv (space-separated) handed to the spawned worker as its `RUNNER_LAUNCH_COMMAND`. Required. */
export const LOCAL_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR = 'LOCAL_ENVIRONMENT_RUNNER_COMMAND';

/** E2B API key. Same var name harness-server's config.ts reads for its own E2B session-sandbox runtime. Required for the 'e2b' backend. */
export const E2B_API_KEY_ENV_VAR = 'E2B_API_KEY';
/**
 * Custom E2B template id the Environment box boots from. DISTINCT from
 * harness-server's own `E2B_TEMPLATE_ID` (its session-sandbox tool-exec
 * template) — this factory's box needs the Orca Environment image instead;
 * see the module doc. Optional (SDK default template if unset).
 */
export const E2B_ENVIRONMENT_TEMPLATE_ID_ENV_VAR = 'E2B_ENVIRONMENT_TEMPLATE_ID';
/** Optional E2B server URL override (e.g. for self-hosted E2B). */
export const E2B_ENVIRONMENT_BASE_URL_ENV_VAR = 'E2B_ENVIRONMENT_BASE_URL';
/** Argv (space-separated) used to exec `environment-worker` inside the E2B box. Required for the 'e2b' backend. */
export const E2B_ENVIRONMENT_WORKER_COMMAND_ENV_VAR = 'E2B_ENVIRONMENT_WORKER_COMMAND';
/** Argv (space-separated) handed to the exec'd worker as its `RUNNER_LAUNCH_COMMAND`. Required for the 'e2b' backend. */
export const E2B_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR = 'E2B_ENVIRONMENT_RUNNER_COMMAND';
/** Workspace dir inside the E2B box. Optional — defaults to CloudEnvironmentLauncher's own default. */
export const E2B_ENVIRONMENT_WORKSPACE_DIR_ENV_VAR = 'E2B_ENVIRONMENT_WORKSPACE_DIR';

/** OpenSandbox server host[:port] or full URL. Same var name harness-server's config.ts reads. Required for the 'opensandbox' backend. */
export const OPEN_SANDBOX_DOMAIN_ENV_VAR = 'OPEN_SANDBOX_DOMAIN';
/** Protocol used when the domain has no scheme. Same var name harness-server reads. Optional, defaults to 'http'. */
export const OPEN_SANDBOX_PROTOCOL_ENV_VAR = 'OPEN_SANDBOX_PROTOCOL';
/** Optional OpenSandbox API key. Same var name harness-server reads. */
export const OPEN_SANDBOX_API_KEY_ENV_VAR = 'OPEN_SANDBOX_API_KEY';
/**
 * Sandbox image the Environment box boots from. DISTINCT from
 * harness-server's own `OPEN_SANDBOX_IMAGE` (its session-sandbox tool-exec
 * image) — this factory's box needs the Orca Environment image instead; see
 * the module doc. Required for the 'opensandbox' backend.
 */
export const OPEN_SANDBOX_ENVIRONMENT_IMAGE_ENV_VAR = 'OPEN_SANDBOX_ENVIRONMENT_IMAGE';
/** Argv (space-separated) used to exec `environment-worker` inside the OpenSandbox box. Required for the 'opensandbox' backend. */
export const OPEN_SANDBOX_ENVIRONMENT_WORKER_COMMAND_ENV_VAR =
  'OPEN_SANDBOX_ENVIRONMENT_WORKER_COMMAND';
/** Argv (space-separated) handed to the exec'd worker as its `RUNNER_LAUNCH_COMMAND`. Required for the 'opensandbox' backend. */
export const OPEN_SANDBOX_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR =
  'OPEN_SANDBOX_ENVIRONMENT_RUNNER_COMMAND';
/** Workspace dir inside the OpenSandbox box. Optional — defaults to CloudEnvironmentLauncher's own default. */
export const OPEN_SANDBOX_ENVIRONMENT_WORKSPACE_DIR_ENV_VAR =
  'OPEN_SANDBOX_ENVIRONMENT_WORKSPACE_DIR';

function optionalEnv(env: NodeJS.ProcessEnv, name: string): string | undefined {
  const value = env[name];
  return value === undefined || value.trim() === '' ? undefined : value;
}

function requireEnv(env: NodeJS.ProcessEnv, name: string, backend: string): string {
  const value = optionalEnv(env, name);
  if (value === undefined) {
    throw new Error(`${name} is required for the '${backend}' environment launcher backend`);
  }
  return value;
}

/** Split a launch command string into an argv array on whitespace (no shell quoting; see config.ts's `parseLaunchCommand`). */
function parseCommand(raw: string, varName: string): string[] {
  const argv = raw
    .trim()
    .split(/\s+/)
    .filter((part) => part.length > 0);
  if (argv.length === 0) {
    throw new Error(`${varName} must contain a command`);
  }
  return argv;
}

function localOptionsFromEnv(env: NodeJS.ProcessEnv): LocalEnvironmentLauncherOptions {
  return {
    baseDir:
      optionalEnv(env, LOCAL_ENVIRONMENT_BASE_DIR_ENV_VAR) ??
      join(tmpdir(), 'orca-local-environments'),
    workerLaunchCommand: parseCommand(
      requireEnv(env, LOCAL_ENVIRONMENT_WORKER_COMMAND_ENV_VAR, 'local'),
      LOCAL_ENVIRONMENT_WORKER_COMMAND_ENV_VAR,
    ),
    runnerLaunchCommand: parseCommand(
      requireEnv(env, LOCAL_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR, 'local'),
      LOCAL_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR,
    ),
  };
}

/**
 * Exported (unlike its `local`/`opensandbox` siblings) so the
 * `E2B_ENVIRONMENT_TEMPLATE_ID_ENV_VAR` -> `templateId` mapping is directly
 * unit-testable: `templateId` is OPTIONAL, so there is no "missing required
 * var throws" proof available the way there is for e.g. `OPEN_SANDBOX_ENVIRONMENT_IMAGE_ENV_VAR` —
 * the constructed `E2BEnvironmentLauncher` does not expose the value it was
 * built with (it is buried inside the E2B SDK runtime it wraps), so asserting
 * against this pure function is the only way to prove which env var name the
 * factory actually reads.
 */
export function e2bOptionsFromEnv(env: NodeJS.ProcessEnv): E2BEnvironmentLauncherOptions {
  const templateId = optionalEnv(env, E2B_ENVIRONMENT_TEMPLATE_ID_ENV_VAR);
  const baseURL = optionalEnv(env, E2B_ENVIRONMENT_BASE_URL_ENV_VAR);
  const workspaceDir = optionalEnv(env, E2B_ENVIRONMENT_WORKSPACE_DIR_ENV_VAR);
  return {
    apiKey: requireEnv(env, E2B_API_KEY_ENV_VAR, 'e2b'),
    ...(templateId !== undefined ? { templateId } : {}),
    ...(baseURL !== undefined ? { baseURL } : {}),
    workerLaunchCommand: parseCommand(
      requireEnv(env, E2B_ENVIRONMENT_WORKER_COMMAND_ENV_VAR, 'e2b'),
      E2B_ENVIRONMENT_WORKER_COMMAND_ENV_VAR,
    ),
    runnerLaunchCommand: parseCommand(
      requireEnv(env, E2B_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR, 'e2b'),
      E2B_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR,
    ),
    ...(workspaceDir !== undefined ? { workspaceDir } : {}),
  };
}

function parseOpenSandboxProtocol(raw: string | undefined): 'http' | 'https' {
  const value = (raw ?? 'http').toLowerCase();
  if (value === 'http' || value === 'https') return value;
  throw new Error(
    `${OPEN_SANDBOX_PROTOCOL_ENV_VAR}=${raw} is not recognized; expected http or https`,
  );
}

function openSandboxOptionsFromEnv(env: NodeJS.ProcessEnv): OpenSandboxEnvironmentLauncherOptions {
  const apiKey = optionalEnv(env, OPEN_SANDBOX_API_KEY_ENV_VAR);
  const workspaceDir = optionalEnv(env, OPEN_SANDBOX_ENVIRONMENT_WORKSPACE_DIR_ENV_VAR);
  return {
    domain: requireEnv(env, OPEN_SANDBOX_DOMAIN_ENV_VAR, 'opensandbox'),
    protocol: parseOpenSandboxProtocol(optionalEnv(env, OPEN_SANDBOX_PROTOCOL_ENV_VAR)),
    ...(apiKey !== undefined ? { apiKey } : {}),
    image: requireEnv(env, OPEN_SANDBOX_ENVIRONMENT_IMAGE_ENV_VAR, 'opensandbox'),
    workerLaunchCommand: parseCommand(
      requireEnv(env, OPEN_SANDBOX_ENVIRONMENT_WORKER_COMMAND_ENV_VAR, 'opensandbox'),
      OPEN_SANDBOX_ENVIRONMENT_WORKER_COMMAND_ENV_VAR,
    ),
    runnerLaunchCommand: parseCommand(
      requireEnv(env, OPEN_SANDBOX_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR, 'opensandbox'),
      OPEN_SANDBOX_ENVIRONMENT_RUNNER_COMMAND_ENV_VAR,
    ),
    ...(workspaceDir !== undefined ? { workspaceDir } : {}),
  };
}

/**
 * Resolve the `EnvironmentLauncher` backend named `backend`, reading its
 * config from `env` (defaults to `process.env`).
 *
 * @throws {Error} When `backend` is not a known backend name, or a backend's
 *   required config is missing.
 */
export function launcherFactory(
  backend: string,
  env: NodeJS.ProcessEnv = process.env,
): EnvironmentLauncher {
  switch (backend) {
    case 'local':
      return new LocalEnvironmentLauncher(localOptionsFromEnv(env));
    case 'e2b':
      return new E2BEnvironmentLauncher(e2bOptionsFromEnv(env));
    case 'opensandbox':
      return new OpenSandboxEnvironmentLauncher(openSandboxOptionsFromEnv(env));
    default:
      throw new Error(
        `unknown environment launcher backend ${JSON.stringify(backend)} ` +
          `(known: ${KNOWN_BACKENDS.join(', ')})`,
      );
  }
}
