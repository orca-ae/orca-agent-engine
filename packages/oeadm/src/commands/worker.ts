// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// `oeadm worker` — the operator's one-command environment worker. It runs the
// `@orca/environment-worker` (a long-running CLIENT that dials the registry host
// tunnel and spawns session runners) with configuration taken from CLI flags,
// falling back to the process environment.
//
// The worker reads its config purely from env vars (see the worker's `config.ts`
// contract), so this command's job is to map `--flag` → the worker's env-var
// name and then launch the worker's entry. Launching is done by spawning the
// worker's built entry as a child (its `main()` calls `process.exit` paths and
// installs signal handlers) — the robust "spawn/exec its main" option. Both the
// env mapping ({@link resolveWorkerEnv}) and the spawn ({@link runWorker}) are
// exposed and injectable so the mapping is unit tested and the launch is
// asserted without starting a real worker.

import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { parseArgs } from '../args.js';

/** The environment-worker env-var contract (mirrors its `config.ts`). */
export interface WorkerEnv {
  ENVIRONMENT_ID: string;
  ENVIRONMENT_KEY: string;
  REGISTRY_TUNNEL_BASE_URL: string;
  WORKSPACE_DIR: string;
  RUNNER_LAUNCH_COMMAND: string;
  /** Optional: defaults to REGISTRY_TUNNEL_BASE_URL inside the worker. */
  REGISTRY_RUNNER_URL?: string;
  /** Optional: defaults to the OS hostname inside the worker. */
  ENVIRONMENT_WORKER_NAME?: string;
}

/** Flag → worker-env mapping. Each entry: CLI flag name and target env var. */
const REQUIRED_MAPPINGS: ReadonlyArray<{ flag: string; env: keyof WorkerEnv }> = [
  { flag: 'environment', env: 'ENVIRONMENT_ID' },
  { flag: 'env-key', env: 'ENVIRONMENT_KEY' },
  { flag: 'registry', env: 'REGISTRY_TUNNEL_BASE_URL' },
  { flag: 'workspace-dir', env: 'WORKSPACE_DIR' },
  { flag: 'runner-command', env: 'RUNNER_LAUNCH_COMMAND' },
];

const OPTIONAL_MAPPINGS: ReadonlyArray<{ flag: string; env: keyof WorkerEnv }> = [
  { flag: 'runner-registry', env: 'REGISTRY_RUNNER_URL' },
  { flag: 'name', env: 'ENVIRONMENT_WORKER_NAME' },
];

/**
 * Resolve the worker's env from CLI `args`, falling back to `baseEnv` (the
 * operator's environment) for any flag not passed. Every required setting must
 * resolve from EITHER the flag OR the corresponding env var, else this throws a
 * message naming both the flag and the env var.
 */
export function resolveWorkerEnv(args: readonly string[], baseEnv: NodeJS.ProcessEnv): WorkerEnv {
  const { options } = parseArgs(args);
  const flagValue = (flag: string): string | undefined => {
    const value = options[flag];
    return typeof value === 'string' && value.length > 0 ? value : undefined;
  };

  const resolved: Partial<WorkerEnv> = {};
  for (const { flag, env } of REQUIRED_MAPPINGS) {
    const value = flagValue(flag) ?? envValue(baseEnv, env);
    if (value === undefined) {
      throw new Error(`missing worker setting: pass --${flag} or set ${env}`);
    }
    resolved[env] = value;
  }
  for (const { flag, env } of OPTIONAL_MAPPINGS) {
    const value = flagValue(flag) ?? envValue(baseEnv, env);
    if (value !== undefined) {
      resolved[env] = value;
    }
  }
  return resolved as WorkerEnv;
}

/** Read a trimmed, non-empty env var, or undefined. */
function envValue(env: NodeJS.ProcessEnv, name: string): string | undefined {
  const value = env[name];
  return value !== undefined && value.trim().length > 0 ? value : undefined;
}

/** Spawn seam so tests assert the launch without starting a real worker. */
export type SpawnWorker = (entry: string, env: NodeJS.ProcessEnv) => ChildProcess;

/** Options for {@link workerCommand}. */
export interface WorkerCommandOptions {
  args: readonly string[];
  /** The operator's base environment (defaults to process.env). */
  baseEnv?: NodeJS.ProcessEnv;
  /** Resolve the worker entry path (defaults to the `@orca/environment-worker` main). */
  resolveEntry?: () => Promise<string>;
  /** Spawn implementation (defaults to launching `node <entry>`). */
  spawnWorker?: SpawnWorker;
}

/**
 * Launch the environment worker with the resolved env. Resolves when the worker
 * child exits, with its exit code (or 1 when it exits via signal). Rejects if
 * the child cannot be spawned.
 */
export async function workerCommand(opts: WorkerCommandOptions): Promise<number> {
  const baseEnv = opts.baseEnv ?? process.env;
  const env = resolveWorkerEnv(opts.args, baseEnv);
  const resolveEntry = opts.resolveEntry ?? defaultResolveEntry;
  const spawnWorker = opts.spawnWorker ?? defaultSpawnWorker;

  const entry = await resolveEntry();
  const child = spawnWorker(entry, { ...baseEnv, ...env });

  return new Promise<number>((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      resolve(code ?? (signal ? 1 : 0));
    });
  });
}

/** Seams for {@link resolveWorkerEntry}, injectable so the guard is unit tested. */
export interface ResolveEntryDeps {
  /** Map the worker specifier to a file URL (defaults to `import.meta.resolve`). */
  resolve: (specifier: string) => string;
  /** Whether the resolved entry exists on disk (defaults to `fs.existsSync`). */
  exists: (path: string) => boolean;
}

/**
 * Resolve the worker's built entry via its package `main` export.
 *
 * `import.meta.resolve` maps the specifier to the package `main` (its built
 * `dist/main.js`) by path alone — it does not check the file exists. Since this
 * monorepo builds the worker separately (`pnpm -r build`), guard the missing
 * build here so the operator gets an actionable message instead of a cryptic
 * `spawn ENOENT` surfacing from `node:child_process` later.
 */
export function resolveWorkerEntry(deps: ResolveEntryDeps): string {
  // `fileURLToPath`, not `.pathname`: the latter keeps percent-encoding, so a
  // checkout under `~/My Projects/` resolved to `.../My%20Projects/...`, which
  // `existsSync` cannot find. A correctly built worker was then reported missing
  // with "build it first", on any path containing a space or a non-ASCII
  // character. `worktree-offload.ts` already uses the right one.
  const entry = fileURLToPath(deps.resolve('@orca/environment-worker'));
  if (!deps.exists(entry)) {
    throw new Error(
      `environment-worker entry not found at ${entry}; build it first with \`pnpm -r build\``,
    );
  }
  return entry;
}

/** Default entry resolution: real `import.meta.resolve` + real `fs.existsSync`. */
async function defaultResolveEntry(): Promise<string> {
  return resolveWorkerEntry({ resolve: (s) => import.meta.resolve(s), exists: existsSync });
}

/** Default spawn: run the worker entry under the current node, inheriting stdio. */
function defaultSpawnWorker(entry: string, env: NodeJS.ProcessEnv): ChildProcess {
  return spawn(process.execPath, [entry], { env, stdio: 'inherit' });
}
