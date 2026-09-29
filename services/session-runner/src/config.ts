// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readPiProviderCredentials, type PiProviderCredentials } from '@orca/pi-harness';
// Runner-wiring config the session-runner reads from its environment.
//
// The session-runner is a long-running CLIENT process the environment-worker
// spawns once per session. The worker builds the runner's environment in
// `services/environment-worker/src/runner-env.ts` (`buildRunnerEnv`): it
// inherits only an allowlist plus the runner-wiring vars below, which the
// runner consumes here to dial the registry runner tunnel and bind its own
// identity. This module is the runner-side mirror of that worker-side contract.
//
// Env var NAMES for the worker→runner wiring contract come from the shared
// boundary (`@orca/harness-tunnel`) so the two sides cannot drift — including
// the registry URL: the runner does not import the worker and the worker does
// not import the runner, so a name either side spells for itself is a
// hand-copied contract. All four are imported below.
//
// The remaining names here are the runner's OWN operator surface — the idle
// watchdog, the opportunistic session/workspace ids, and the model knobs — and
// are declared in this file because nothing else owns them. They are documented
// in `services/session-runner/AGENTS.md` and checked by
// `scripts/check-env-docs.mjs`, which scans this file.

import {
  RUNNER_PARENT_PID_ENV_VAR,
  RUNNER_REGISTRY_URL_ENV_VAR,
  RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR,
  RUNNER_WORKSPACE_ENV_VAR,
} from '@orca/harness-tunnel';

/**
 * Orca session id this runner serves. Runner-local for now: the worker→runner
 * env contract (`buildRunnerEnv`) does not seed a session id today — the id is
 * carried over the tunnel in the `worker.launch_runner` frame. This var is read
 * opportunistically so the config surface is complete and a future worker
 * revision (or a manual `dev` launch) can pin the session id up front.
 */
export const RUNNER_SESSION_ID_ENV_VAR = 'ORCA_RUNNER_SESSION_ID';

/**
 * Owning workspace id (the tenant scope) this runner serves. Like
 * {@link RUNNER_SESSION_ID_ENV_VAR}, the worker→runner env contract does not seed
 * it today — the runner emits agent events up the tunnel and the OWNER POD's
 * bridge stamps the workspace scope on persist, so the runner does not need the
 * real id for emission. It is read opportunistically (defaulting to empty) so the
 * config surface is complete and the session loop can forward it into a provider's
 * boot context when a future revision seeds it.
 */
export const RUNNER_WORKSPACE_ID_ENV_VAR = 'ORCA_RUNNER_WORKSPACE_ID';

/**
 * Runner inactivity timeout in SECONDS — the runner requests a graceful shutdown
 * after this window with no real work and no turn in flight (see `idle.ts`). `0`
 * (or negative) disables the watchdog; defaults to
 * {@link DEFAULT_RUNNER_IDLE_TIMEOUT_S} (1 hour). A non-numeric / malformed value
 * fails loud at startup rather than silently changing the runner's lifecycle.
 */
export const RUNNER_IDLE_TIMEOUT_S_ENV_VAR = 'ORCA_RUNNER_IDLE_TIMEOUT_S';

/** Default runner idle timeout (seconds) — 1 hour. */
export const DEFAULT_RUNNER_IDLE_TIMEOUT_S = 60 * 60;

/**
 * Default model id the claude provider configures the SDK with when a snapshot did
 * not pin one. Mirrors the harness-server's `ANTHROPIC_MODEL_DEFAULT`.
 */
export const RUNNER_MODEL_DEFAULT_ENV_VAR = 'ANTHROPIC_MODEL_DEFAULT';

/** Default model id when {@link RUNNER_MODEL_DEFAULT_ENV_VAR} is unset. */
export const DEFAULT_RUNNER_MODEL = 'claude-sonnet-4-5';

/**
 * Optional fallback LLM credential for the claude provider, used ONLY when the
 * snapshot egress carries no scoped LLM JWT (e.g. a dev runner pointed straight at
 * the Anthropic API). Gateway egress normally supplies the per-session JWT, which
 * takes precedence; this never overrides it.
 */
export const RUNNER_ANTHROPIC_API_KEY_ENV_VAR = 'ANTHROPIC_API_KEY';

/**
 * Optional comma-separated allow-list of model ids a per-turn override may select on
 * the `claude-sdk-persistent` provider — the runner-side, defense-in-depth model knob.
 * When set, a well-formed per-turn model override is admitted only if it is in this set
 * (the snapshot's pinned model and {@link RUNNER_MODEL_DEFAULT_ENV_VAR} are always
 * implicitly permitted); an override outside it falls back to the snapshot/default model
 * with an `agent.status` diagnostic. When UNSET (the default), a well-formed per-turn
 * override is admitted on FORMAT alone — the gateway remains the model-policy enforcement
 * point and the harness is the structural guard. Empty / blank → unset (no allow-list).
 */
export const RUNNER_ALLOWED_MODELS_ENV_VAR = 'ANTHROPIC_ALLOWED_MODELS';

/**
 * The provider defaults the claude providers need at boot.
 *
 * No transcript-backend (Kafka/Postgres) wiring: a self-hosted runner is
 * OUTBOUND-WSS-ONLY behind a NAT and has no direct transcript backend. Its
 * conversation-history substrate is the in-memory, tunnel-fed store
 * ({@link InMemoryTranscriptStore}) `main.ts` constructs — the registry pushes the
 * recovery replay down the tunnel and the runner streams new events up — so the
 * runner builds + boots with no `KAFKA_*` env. Only the model default + optional LLM
 * knobs remain.
 */
export interface RunnerProviderConfig {
  /** Default model id when a snapshot did not pin one. */
  readonly modelDefault: string;
  /** Optional fallback LLM credential (snapshot egress JWT takes precedence). */
  readonly fallbackApiKey?: string;
  readonly openaiApiKey?: string;
  readonly piProviderCredentials?: PiProviderCredentials;
  /**
   * Optional allow-list of model ids a per-turn override may select on the persistent
   * provider (the defense-in-depth model knob). Undefined / empty when unset — a
   * well-formed per-turn override is then admitted on format alone (gateway is the
   * policy point). The snapshot/default models are always implicitly permitted.
   */
  readonly allowedModels?: readonly string[];
}

/**
 * Resolved runner-wiring config. Mirrors the inputs `buildRunnerEnv` layers on
 * (minus `runnerId`, which the runner derives from the binding token via
 * `tokenBoundRunnerId`).
 */
export interface RunnerConfig {
  /** Per-run tunnel binding token; also the runner-side request auth token. */
  readonly bindingToken: string;
  /** Registry runner-tunnel base URL the runner dials back. */
  readonly registryRunnerUrl: string;
  /** Absolute runner working directory on the host. */
  readonly workspace: string;
  /** Orca session id, when pinned up front (see {@link RUNNER_SESSION_ID_ENV_VAR}). */
  readonly sessionId?: string;
  /**
   * Owning workspace id, when pinned up front (see
   * {@link RUNNER_WORKSPACE_ID_ENV_VAR}). Defaults to `''` — the runner does not
   * need the real id for agent-event emission (the owner pod stamps the scope).
   */
  readonly workspaceId: string;
  /**
   * Worker process pid, from {@link RUNNER_PARENT_PID_ENV_VAR}. Parsed and validated
   * only: nothing in the runner watches it.
   */
  readonly parentPid?: number;
  /**
   * Runner inactivity timeout (seconds); `0` disables the watchdog. Resolved from
   * {@link RUNNER_IDLE_TIMEOUT_S_ENV_VAR}, defaulting to
   * {@link DEFAULT_RUNNER_IDLE_TIMEOUT_S}.
   */
  readonly idleTimeoutS: number;
  /**
   * Provider defaults the claude providers need at boot (the default model, an
   * optional fallback LLM credential, and an optional per-turn model allow-list for
   * the persistent provider). Resolved with sensible defaults so the claude providers
   * are always registrable. No transcript-backend wiring: the runner's history
   * substrate is the in-memory, tunnel-fed store (see {@link RunnerProviderConfig}).
   */
  readonly provider: RunnerProviderConfig;
}

function requireEnv(name: string, env: NodeJS.ProcessEnv): string {
  const value = env[name];
  if (value === undefined || value.trim().length === 0) {
    throw new Error(`${name} is required`);
  }
  return value;
}

/**
 * Resolve the provider defaults the claude providers need. Every field has a default
 * so the providers are always registrable; no transcript-backend wiring (the runner's
 * history substrate is the in-memory, tunnel-fed store — see {@link RunnerProviderConfig}).
 */
function parseProviderConfig(env: NodeJS.ProcessEnv): RunnerProviderConfig {
  const modelDefault = optionalTrimmed(env[RUNNER_MODEL_DEFAULT_ENV_VAR]) ?? DEFAULT_RUNNER_MODEL;
  // Spread the optional fields in only when present: under `exactOptionalPropertyTypes`
  // an explicit `undefined` is not assignable to the `?:`-typed fields.
  const fallbackApiKey = optionalTrimmed(env[RUNNER_ANTHROPIC_API_KEY_ENV_VAR]);
  const allowedModels = parseAllowedModels(env);
  return {
    modelDefault,
    piProviderCredentials: readPiProviderCredentials(env),
    ...(optionalTrimmed(env.OPENAI_API_KEY)
      ? { openaiApiKey: optionalTrimmed(env.OPENAI_API_KEY)! }
      : {}),
    ...(fallbackApiKey !== undefined ? { fallbackApiKey } : {}),
    ...(allowedModels !== undefined ? { allowedModels } : {}),
  };
}

/**
 * Parse the optional per-turn model allow-list from {@link RUNNER_ALLOWED_MODELS_ENV_VAR}
 * — a comma-separated list of model ids. Returns `undefined` when the var is unset or
 * resolves to no ids (every entry blank), so the harness keeps its format-only default
 * (gateway as the policy point). Otherwise returns the trimmed, de-duplicated, non-blank
 * ids in declared order — the defense-in-depth allow-list the persistent provider enforces.
 */
function parseAllowedModels(env: NodeJS.ProcessEnv): readonly string[] | undefined {
  const raw = env[RUNNER_ALLOWED_MODELS_ENV_VAR];
  if (raw === undefined || raw.trim().length === 0) {
    return undefined;
  }
  const seen = new Set<string>();
  const models: string[] = [];
  for (const entry of raw.split(',')) {
    const id = entry.trim();
    if (id.length > 0 && !seen.has(id)) {
      seen.add(id);
      models.push(id);
    }
  }
  return models.length > 0 ? models : undefined;
}

/** A trimmed env value, or `undefined` when absent / blank. */
function optionalTrimmed(raw: string | undefined): string | undefined {
  if (raw === undefined) {
    return undefined;
  }
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/**
 * Read and validate the runner-wiring config from `env` (defaults to
 * `process.env`). Throws if a required wiring var is missing or empty.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): RunnerConfig {
  const bindingToken = requireEnv(RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR, env);
  const registryRunnerUrl = requireEnv(RUNNER_REGISTRY_URL_ENV_VAR, env);
  const workspace = requireEnv(RUNNER_WORKSPACE_ENV_VAR, env);

  const workspaceIdRaw = env[RUNNER_WORKSPACE_ID_ENV_VAR];
  const config: RunnerConfig = {
    bindingToken,
    registryRunnerUrl,
    workspace,
    // Opportunistic: empty when the worker did not seed it (the owner pod stamps
    // the workspace scope on persist, so the runner does not need it for emission).
    workspaceId:
      workspaceIdRaw !== undefined && workspaceIdRaw.trim().length > 0 ? workspaceIdRaw.trim() : '',
    idleTimeoutS: parseIdleTimeoutS(env),
    provider: parseProviderConfig(env),
  };

  const sessionId = env[RUNNER_SESSION_ID_ENV_VAR];
  if (sessionId !== undefined && sessionId.trim().length > 0) {
    return assignParentPid({ ...config, sessionId }, env);
  }
  return assignParentPid(config, env);
}

/**
 * Resolve the runner idle timeout (seconds) from the environment. Missing/blank
 * defaults to {@link DEFAULT_RUNNER_IDLE_TIMEOUT_S}; `0` disables the watchdog; a
 * negative, boolean-ish, or non-numeric value fails loud so the operator does not
 * get silently different lifecycle behavior than requested.
 */
function parseIdleTimeoutS(env: NodeJS.ProcessEnv): number {
  const raw = env[RUNNER_IDLE_TIMEOUT_S_ENV_VAR];
  if (raw === undefined || raw.trim().length === 0) {
    return DEFAULT_RUNNER_IDLE_TIMEOUT_S;
  }
  const value = Number(raw.trim());
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${RUNNER_IDLE_TIMEOUT_S_ENV_VAR} must be a non-negative number of seconds`);
  }
  return value;
}

function assignParentPid(config: RunnerConfig, env: NodeJS.ProcessEnv): RunnerConfig {
  const raw = env[RUNNER_PARENT_PID_ENV_VAR];
  if (raw === undefined || raw.trim().length === 0) {
    return config;
  }
  const parentPid = Number.parseInt(raw, 10);
  if (!Number.isInteger(parentPid) || parentPid <= 0) {
    throw new Error(`${RUNNER_PARENT_PID_ENV_VAR} must be a positive integer`);
  }
  return { ...config, parentPid };
}
