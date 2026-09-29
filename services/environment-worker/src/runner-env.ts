// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Build the environment for a spawned session runner.
//
// The worker runs as the operator, so its environment can hold the operator's
// personal secrets; a runner has no business inheriting those. So the runner
// inherits only an ALLOWLIST (process essentials + locale family + TLS trust
// stores + the runner's own operator knobs) — not the whole environment — plus
// the runner-wiring vars the runner reads to dial the registry runner tunnel and
// bind its own identity. Operators that legitimately need an extra var forwarded
// (a credential, custom gateway wiring, provider env refs) name it in the
// passthrough var.
//
// The allowlist separates SECRETS from CONFIGURATION, not worker vars from runner
// vars. A self-hosted operator runs one command, `oeadm worker`, so the worker's
// environment is the only surface they have: a runner knob that does not cross
// this boundary cannot be set at all. Leaving the runner's own knobs out silently
// pinned them to their defaults — and for `ANTHROPIC_ALLOWED_MODELS` that default
// is permit-all, so a deny-by-default control failed OPEN.
//
// The runner's own tunnel binding token is a control-plane secret: it is seeded
// ONLY through the wiring var below, never inherited. `stripRunnerAuthSecrets`
// (the shared boundary helper) removes any inherited copy before this layer
// re-seeds THIS launch's token, so a stale/leaked token in the base environment
// can never ride through.

import {
  RUNNER_PARENT_PID_ENV_VAR,
  RUNNER_REGISTRY_URL_ENV_VAR,
  RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR,
  RUNNER_WORKSPACE_ENV_VAR,
  stripRunnerAuthSecrets,
} from '@orca/harness-tunnel';

// `RUNNER_REGISTRY_URL_ENV_VAR` is IMPORTED, never declared here — the same rule
// its three siblings above already follow. It lives in `@orca/harness-tunnel`'s
// `identity.ts` because this file is the PRODUCER (it seeds the runner's
// environment) and the session-runner is the CONSUMER, and the two packages never
// import each other: a name either side spells for itself is a hand-copied
// contract, and a local re-declaration is exactly the drift the hoist prevents.
// Re-exported so the worker's own modules keep one import path for it.
export { RUNNER_REGISTRY_URL_ENV_VAR };

/**
 * Comma-separated EXTRA env var names the operator wants forwarded worker→runner
 * beyond the allowlist (custom gateway tokens/URLs, provider env refs). Operator-
 * controlled: name exactly what the runner needs; everything unnamed stays
 * behind the allowlist.
 */
export const RUNNER_ENV_PASSTHROUGH_ENV_VAR = 'ORCA_RUNNER_ENV_PASSTHROUGH';

/**
 * The session-runner's own operator knobs, forwarded worker→runner.
 *
 * A self-hosted operator runs ONE command — `oeadm worker` — so the worker's
 * environment is the only one they control. A runner knob that does not cross
 * this boundary is therefore not merely inconvenient to set: it cannot be set at
 * all, and the runner silently uses its default.
 *
 * That made `ANTHROPIC_ALLOWED_MODELS` a security control that failed OPEN. It is
 * the allow-list a per-turn model override may select from on
 * `claude-sdk-persistent`; unset, the runner admits any well-formed model id on
 * FORMAT alone. An operator who set it on the worker got no allow-list and no
 * diagnostic — the deny-by-default they configured was silently a permit-all.
 *
 * These carry configuration, not credentials, so the allowlist's rationale (keep
 * the OPERATOR'S PERSONAL SECRETS out of a runner) does not reach them. A
 * credential — `ANTHROPIC_API_KEY`, a custom gateway token — stays out and is
 * named explicitly in {@link RUNNER_ENV_PASSTHROUGH_ENV_VAR} when an operator
 * genuinely wants it forwarded.
 *
 * Their meaning is the session-runner's, documented in that service's
 * `AGENTS.md`; the names are spelled here rather than imported because the worker
 * does not depend on `@orca/session-runner` (it spawns it as an opaque argv).
 */
const RUNNER_OPERATOR_KNOBS: readonly string[] = [
  'ANTHROPIC_ALLOWED_MODELS',
  'ANTHROPIC_MODEL_DEFAULT',
  'ORCA_RUNNER_IDLE_TIMEOUT_S',
];

/**
 * Worker-environment variables a spawned runner is allowed to inherit.
 *
 * Deliberately an allowlist (not the whole environment): limited to process
 * essentials (PATH/HOME/shell/locale/temp), TLS trust stores so the runner's
 * outbound HTTPS still works, and the runner's own operator knobs
 * ({@link RUNNER_OPERATOR_KNOBS}). Anything a runner legitimately needs beyond
 * this flows through its session spec, or the operator names it in
 * {@link RUNNER_ENV_PASSTHROUGH_ENV_VAR}.
 */
const RUNNER_ENV_ALLOWLIST: ReadonlySet<string> = new Set([
  'PATH',
  'HOME',
  'USER',
  'LOGNAME',
  'SHELL',
  'TMPDIR',
  'TZ',
  'TERM',
  'TERMINFO',
  'TERMINFO_DIRS',
  'LANG',
  'SSL_CERT_FILE',
  'SSL_CERT_DIR',
  'REQUESTS_CA_BUNDLE',
  'CURL_CA_BUNDLE',
  'NODE_EXTRA_CA_CERTS',
  ...RUNNER_OPERATOR_KNOBS,
]);

/** Locale family (`LC_ALL`, `LC_CTYPE`, …) — allowed by prefix. */
const RUNNER_ENV_ALLOWLIST_PREFIXES: readonly string[] = ['LC_'];

/** The runner-wiring inputs layered onto the filtered environment. */
export interface RunnerWiring {
  /** Registry runner-tunnel base URL the runner dials back. */
  readonly registryRunnerUrl: string;
  /** Token-bound runner id. */
  readonly runnerId: string;
  /** Per-run tunnel binding token (also the runner-side request auth token). */
  readonly bindingToken: string;
  /** Absolute runner working directory on the host. */
  readonly workspace: string;
  /** Worker process pid. The session-runner parses and validates it but does not watch it. */
  readonly parentPid: number;
}

/**
 * Build the runner subprocess environment from the worker's `baseEnv`.
 *
 * Inherits only the allowlisted subset of `baseEnv` (plus operator-named
 * passthrough extras), strips any inherited runner-auth secret, then layers on
 * the runner-wiring vars. The source mapping is not mutated.
 */
export function buildRunnerEnv(
  baseEnv: Record<string, string | undefined>,
  wiring: RunnerWiring,
): Record<string, string> {
  const passthrough = new Set(
    (baseEnv[RUNNER_ENV_PASSTHROUGH_ENV_VAR] ?? '')
      .split(',')
      .map((name) => name.trim())
      .filter((name) => name.length > 0),
  );

  const filtered: Record<string, string> = {};
  for (const [key, value] of Object.entries(baseEnv)) {
    if (value === undefined) {
      continue;
    }
    const allowed =
      RUNNER_ENV_ALLOWLIST.has(key) ||
      RUNNER_ENV_ALLOWLIST_PREFIXES.some((prefix) => key.startsWith(prefix)) ||
      passthrough.has(key);
    if (allowed) {
      filtered[key] = value;
    }
  }

  // Drop any inherited runner-auth secret before re-seeding this launch's token.
  const env = stripRunnerAuthSecrets(filtered);
  env[RUNNER_REGISTRY_URL_ENV_VAR] = wiring.registryRunnerUrl;
  env[RUNNER_TUNNEL_BINDING_TOKEN_ENV_VAR] = wiring.bindingToken;
  env[RUNNER_WORKSPACE_ENV_VAR] = wiring.workspace;
  env[RUNNER_PARENT_PID_ENV_VAR] = String(wiring.parentPid);
  return env;
}
