// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Single source of truth for the per-agent harness annotation
 * (`metadata.harness` + `metadata.mode`).
 *
 * The annotation selects which harness drives an agent and whether it runs
 * inside the sandbox (`colocated`) or separate from it (`separate`). It lives
 * in the agent's free-form `metadata` JSONB —
 * Anthropic Managed Agents-compatible, no dedicated contract field. The registry
 * validates it on agent create/update; harness-server reads the
 * same catalog to route and to resolve the in-sandbox image.
 */

export type HarnessMode = 'colocated' | 'separate';

/**
 * `in_sandbox` is the DEPRECATED spelling of `colocated`.
 *
 * `metadata.mode` is user-facing and stored verbatim: it lives in the agent's
 * `metadata` JSONB and is snapshotted into `agent_versions`, so rows written
 * before the rename still carry `in_sandbox` and there is no migration that
 * rewrites them. Accepting it as an alias keeps those agents runnable and keeps
 * a value that was valid yesterday from becoming a 400 today (a `GET
 * /v1/agents/:id` returns the stored metadata; PUTting it back unchanged must
 * not fail on a value the caller never wrote).
 *
 * It normalizes to `colocated` everywhere; nothing downstream sees the alias.
 * New annotations should use `colocated` — the validation error deliberately
 * advertises only the current spellings.
 */
export const DEPRECATED_MODE_ALIASES: Readonly<Record<string, HarnessMode>> = {
  in_sandbox: 'colocated',
};

export type HarnessType =
  | 'claude_agent_sdk'
  | 'claude_agent_sdk_persistent'
  | 'claude_code'
  | 'codex_sdk'
  | 'pi_sdk'
  | 'codex'
  | 'cursor'
  | 'pi'
  | 'custom'
  | 'mock';

/**
 * The runner provider id a harness maps to. The provider is what a
 * multi-provider runner reads to spin up the right agent harness:
 *
 *   - `claude` — the lean Anthropic Claude Agent SDK harness (`claude_agent_sdk`):
 *     the stateless one-shot `query()`-per-turn provider (provider A);
 *   - `claude-code` — the NATIVE Claude Code harness (`claude_code`): the real
 *     `claude` binary, booted headless as a long-lived child of the session sandbox
 *     and driven over stream-json stdio, with the native-CLI tool-bridge wired as its
 *     `orca` MCP server. A distinct runner provider from `claude` — the CLI drives the
 *     turn, not an in-process SDK loop — resolved to `ClaudeCodeCliHarness`. The
 *     cloud in-sandbox path also accepts this id: `@orca/sandbox-harness` declares
 *     `claude-code` among its `claude` provider's aliases, so harness-server's cloud
 *     `claude_code` sessions resolve it to that SDK provider;
 *   - `claude-sdk-persistent` — the Anthropic Claude PERSISTENT provider (provider B):
 *     a live SDK session kept alive across turns (`claude_agent_sdk_persistent`). It is
 *     a distinct runner provider from the lean `claude` (the runner registers both), so
 *     it needs its own provider id — the session-runner resolves a `provider:
 *     'claude-sdk-persistent'` snapshot to `ClaudePersistentSdkHarness`. Both Claude
 *     providers are selectable per agent via the harness annotation (see
 *     `docs/managed-agents/session-runner-scope.md`);
 *   - `codex` — the OpenAI Codex harness;
 *   - `cursor` — the `cursor` native-CLI harness (boots the headless `cursor-agent`
 *     binary and drives it over its stream-json stdio, wiring the native-CLI
 *     tool-bridge as its `orca` MCP server). Registered + advertised by the runner
 *     like the other native-CLI providers; the snapshot resolver carries `cursor`
 *     so a `cursor`-annotated agent resolves to that provider;
 *   - `pi` — the `pi` native-CLI harness (boots `pi --mode rpc` and drives it over
 *     pi's newline-delimited JSON command/event protocol, wiring the native-CLI
 *     tool-bridge as a pi extension). Registered + advertised by the runner like the
 *     other native-CLI providers; the snapshot resolver carries `pi` so a
 *     `pi`-annotated agent resolves to that provider;
 *   - `custom` — the GENERIC native-CLI harness: it boots an OPERATOR-DECLARED CLI
 *     (per the snapshot's declarative JSON `custom_spec` block — command / argv-with-
 *     placeholders / env / cwd / a stdout→AgentEvent mapping / an optional approvals
 *     opt-in) and drives it over its stdin/stdout per the spec, wiring the native-CLI
 *     tool-bridge as its `orca` MCP server. It lets an operator register ANY CLI agent
 *     WITHOUT a bespoke provider. The spec is a structured object (JSON, not YAML —
 *     operator-facing serialization is out of the runner's scope); it rides the
 *     snapshot as `custom_spec`. Registered + advertised by the runner like the other
 *     native-CLI providers; the snapshot resolver carries `custom` so a
 *     `custom`-annotated agent resolves to that provider;
 *   - `mock`  — the deterministic, LLM-free harness (a first-class runner feature,
 *     not a stub): it answers each turn with a scripted response so the full
 *     worker → runner → registry → SSE plumbing can be validated with NO model
 *     credential. The runner registers + advertises it, and the registry's
 *     snapshot resolver carries it for a `mock`-annotated agent.
 *
 * Carried in the catalog so the registry's snapshot resolver and the
 * harness-server in-sandbox builder both read ONE mapping (see
 * {@link harnessToProvider}) rather than maintaining parallel copies.
 */
export type HarnessProvider =
  | 'claude'
  | 'claude-code'
  | 'claude-sdk-persistent'
  | 'codex-sdk'
  | 'pi-sdk'
  | 'codex'
  | 'cursor'
  | 'pi'
  | 'custom'
  | 'mock';

export interface HarnessCatalogEntry {
  /** Modes this harness can run in. The first entry is the default when
   *  `metadata.mode` is omitted. */
  supportedModes: [HarnessMode, ...HarnessMode[]]; // non-empty: every harness has ≥1 mode
  /** Default sandbox image for `colocated` harnesses; `null` for harnesses that
   *  run separate from the sandbox (no dedicated image). */
  defaultImage: string | null;
  /** Container entrypoint to set when booting a `colocated` harness image;
   *  `null` to leave the image's own `CMD` in charge. Set only for `claude_code`,
   *  `pi_sdk`, `codex_sdk`, and `codex`, which point it at the
   *  `orca-sandbox-harness` server. */
  entrypoint: string[] | null;
  /** Port the in-sandbox harness HTTP server listens on; `null` when separate. */
  port: number | null;
  /** The runner provider id this harness maps to (the harness→provider mapping). */
  provider: HarnessProvider;
}

export const DEFAULT_HARNESS: HarnessType = 'claude_agent_sdk';
export const DEFAULT_MODE: HarnessMode = 'separate';

export const HARNESS_CATALOG: Record<HarnessType, HarnessCatalogEntry> = {
  claude_agent_sdk: {
    supportedModes: ['separate'],
    defaultImage: null,
    entrypoint: null,
    port: null,
    provider: 'claude',
  },
  // The persistent Claude Agent SDK harness (provider B). Runs `separate` like its
  // lean sibling `claude_agent_sdk` (it is NOT an in-sandbox image harness), but maps
  // to the DISTINCT `claude-sdk-persistent` runner provider — a live SDK session kept
  // alive across turns rather than a stateless one-shot `query()` per turn. The runner
  // registers both providers and resolves the snapshot's `provider` to the matching
  // harness, so an agent selects the persistent path via `metadata.harness:
  // 'claude_agent_sdk_persistent'`. No sandbox image/port (it never boots a harness-
  // image sandbox). See `docs/managed-agents/session-runner-scope.md` (providers A/B)
  // and `services/session-runner/src/harness/claude/persistent-provider.ts`.
  claude_agent_sdk_persistent: {
    supportedModes: ['separate'],
    defaultImage: null,
    entrypoint: null,
    port: null,
    provider: 'claude-sdk-persistent',
  },
  // The native Claude Code harness: the real `claude` binary, booted headless as a
  // long-lived child of the session sandbox and driven over stream-json stdio, with
  // the native-CLI tool-bridge wired as its `orca` MCP server. It maps to the runner's
  // `claude-code` provider (`services/session-runner/src/harness/claude-code/`), NOT to
  // the in-process Agent SDK `claude` provider its two siblings above use — the whole
  // point of the harness is that the CLI, not an SDK loop, drives the turn.
  //
  // That distinction is easy to lose: this entry is the ONLY thing that reaches the
  // provider. `resolveAgentProvider` is the single writer of `snapshot.provider` and
  // goes straight through `harnessToProvider`, so a `provider: 'claude'` here silently
  // routes every `claude_code` agent to the SDK, and the transcripts of the two look
  // nearly identical. Assert on the snapshot's `provider`, never on the event stream.
  claude_code: {
    supportedModes: ['colocated'],
    defaultImage: 'ghcr.io/orca-ae/sandbox-harness-claude-code:latest',
    entrypoint: ['/usr/local/bin/orca-sandbox-harness'],
    port: 4096,
    provider: 'claude-code',
  },
  pi_sdk: {
    supportedModes: ['separate', 'colocated'],
    defaultImage: 'ghcr.io/orca-ae/sandbox-harness-claude-code:latest',
    entrypoint: ['/usr/local/bin/orca-sandbox-harness'],
    port: 4096,
    provider: 'pi-sdk',
  },
  codex_sdk: {
    // Managed SDKs default to running separate from the tool sandbox.
    supportedModes: ['separate', 'colocated'],
    defaultImage: 'ghcr.io/orca-ae/sandbox-harness-claude-code:latest',
    entrypoint: ['/usr/local/bin/orca-sandbox-harness'],
    port: 4096,
    provider: 'codex-sdk',
  },
  codex: {
    supportedModes: ['colocated'],
    defaultImage: 'ghcr.io/orca-ae/sandbox-harness-codex:latest',
    entrypoint: ['/usr/local/bin/orca-sandbox-harness'],
    port: 4096,
    provider: 'codex',
  },
  // The `cursor` native-CLI harness. Modeled like `codex`/`pi` (its sibling native-CLI
  // providers): `colocated` with a harness image + the shared 4096 port, mapping to
  // the runner's `cursor` provider. The runner boots the headless `cursor-agent` binary
  // inside the session sandbox and drives it over its stream-json stdio, wiring the
  // native-CLI tool-bridge as its `orca` MCP server and routing cursor's tool-permission
  // requests to the uniform transcript gate. `harnessToProvider('cursor')` returns
  // `'cursor'` so the registry's snapshot resolver stamps `provider: 'cursor'` — the key
  // the runner's ProviderRegistry resolves — making the harness selectable via
  // `metadata.harness: 'cursor'`.
  cursor: {
    supportedModes: ['colocated'],
    defaultImage: 'ghcr.io/orca-ae/sandbox-harness-cursor:latest',
    entrypoint: null,
    port: 4096,
    provider: 'cursor',
  },
  // The `pi` native-CLI harness. Modeled like `codex` (its sibling native-CLI
  // provider): `colocated` with a harness image + the shared 4096 port, mapping to
  // the runner's `pi` provider. The runner boots `pi --mode rpc` inside the session
  // sandbox and drives it over pi's newline-delimited JSON command/event protocol,
  // wiring the native-CLI tool-bridge as a pi extension. `harnessToProvider('pi')`
  // returns `'pi'` so the registry's snapshot resolver stamps `provider: 'pi'` — the
  // key the runner's ProviderRegistry resolves — making the harness selectable via
  // `metadata.harness: 'pi'`.
  pi: {
    supportedModes: ['colocated'],
    defaultImage: 'ghcr.io/orca-ae/sandbox-harness-pi:latest',
    entrypoint: null,
    port: 4096,
    provider: 'pi',
  },
  // The generic `custom` native-CLI harness. Modeled like `codex`/`cursor`/`pi` (its
  // sibling native-CLI providers): `colocated` with a harness image + the shared 4096
  // port, mapping to the runner's `custom` provider. The runner boots an operator-
  // declared CLI as a long-lived child INSIDE the session sandbox — per the snapshot's
  // declarative JSON `custom_spec` block (command / argv-with-placeholders / env / cwd /
  // a stdout→AgentEvent mapping / an optional approvals opt-in) — and drives it over its
  // stdin/stdout per the spec, wiring the native-CLI tool-bridge as its `orca` MCP server
  // and routing a spec-declared approval request to the uniform transcript gate. This is
  // the ANY-CLI escape hatch: an operator registers a new CLI agent by dropping the spec
  // on the agent (no runner change). `harnessToProvider('custom')` returns `'custom'` so
  // the registry's snapshot resolver stamps `provider: 'custom'` — the key the runner's
  // ProviderRegistry resolves — making the harness selectable via `metadata.harness:
  // 'custom'`. (The spec is a structured object — JSON, not YAML; operator-facing
  // serialization is out of the runner's scope, so the runner receives it parsed.)
  custom: {
    supportedModes: ['colocated'],
    defaultImage: 'ghcr.io/orca-ae/sandbox-harness-custom:latest',
    entrypoint: null,
    port: 4096,
    provider: 'custom',
  },
  // The deterministic, LLM-free harness. Runs `colocated`: it is a first-class
  // `session-runner` provider (registered + advertised alongside `claude` /
  // `claude-code` / `codex` / ...). Unlike the cloud claude_code HTTP bridge,
  // this harness always belongs to the Registry runner; harness-server never runs it. The runner builds it in-process and
  // answers each turn with a scripted response, so a `mock`-annotated agent drives
  // the full self-hosted plumbing (worker → runner → registry bridge → SSE) with no
  // model credential. No sandbox image / port: unlike the native-CLI harnesses it
  // never spawns a child process, so there is nothing to boot (like `claude`, whose
  // image/port are also null).
  mock: {
    supportedModes: ['colocated'],
    defaultImage: null,
    entrypoint: null,
    port: null,
    provider: 'mock',
  },
};

/**
 * Map a harness type to its runner provider id — the SINGLE source of truth for
 * the harness→provider mapping, driven by {@link HARNESS_CATALOG}.
 *
 * Two callers read this one mapping (rather than each keeping a parallel switch):
 *
 *   - the registry's snapshot resolver, which carries the provider in the
 *     credential-free agent snapshot (the self-hosted launch frame omits the
 *     provider by design, so the snapshot must carry it); and
 *   - the harness-server in-sandbox builder, which selects the in-sandbox
 *     provider runtime.
 *
 * A value outside {@link HarnessType} cannot reach here through the type system;
 * the runtime guard throws on a forced cast rather than silently falling back,
 * so a newly-added harness without a catalog provider fails loud.
 */
export function harnessToProvider(harness: HarnessType): HarnessProvider {
  const entry = HARNESS_CATALOG[harness] as HarnessCatalogEntry | undefined;
  if (entry === undefined) {
    throw new Error(`unsupported harness: ${String(harness)}`);
  }
  return entry.provider;
}

export interface ResolvedHarness {
  harness: HarnessType;
  mode: HarnessMode;
}

function isHarnessType(value: unknown): value is HarnessType {
  return typeof value === 'string' && Object.prototype.hasOwnProperty.call(HARNESS_CATALOG, value);
}

function isHarnessMode(value: unknown): value is HarnessMode {
  return value === 'colocated' || value === 'separate';
}

/**
 * Normalize a raw `metadata.mode` to a {@link HarnessMode}, resolving the
 * deprecated spellings in {@link DEPRECATED_MODE_ALIASES}; `null` when the value
 * is not a mode at all.
 */
export function normalizeHarnessMode(value: unknown): HarnessMode | null {
  if (isHarnessMode(value)) return value;
  if (typeof value === 'string' && Object.hasOwn(DEPRECATED_MODE_ALIASES, value)) {
    return DEPRECATED_MODE_ALIASES[value] ?? null;
  }
  return null;
}

/**
 * Resolve + validate the harness annotation carried in an agent's `metadata`.
 *
 * - Both keys absent → platform default (`claude_agent_sdk` / `separate`).
 * - `harness` present, `mode` absent → the harness's default (first supported) mode.
 * - Unknown `harness`, invalid `mode`, or an unsupported (harness, mode) combo → `{ error }`.
 *
 * Does NOT mutate `metadata`; the caller persists it as-is.
 */
export function resolveHarnessAnnotation(
  metadata: Record<string, unknown> | null | undefined,
): ResolvedHarness | { error: string } {
  const rawHarness = metadata?.['harness'];
  const rawMode = metadata?.['mode'];

  if (rawHarness === undefined && rawMode === undefined) {
    return { harness: DEFAULT_HARNESS, mode: DEFAULT_MODE };
  }

  let harness: HarnessType;
  if (rawHarness === undefined) {
    harness = DEFAULT_HARNESS;
  } else if (isHarnessType(rawHarness)) {
    harness = rawHarness;
  } else {
    const known = Object.keys(HARNESS_CATALOG).join(', ');
    return { error: `metadata.harness must be one of: ${known}` };
  }

  const entry = HARNESS_CATALOG[harness];

  let mode: HarnessMode;
  if (rawMode === undefined) {
    mode = entry.supportedModes[0];
  } else {
    // Resolves the deprecated `in_sandbox` spelling to `colocated`; see
    // DEPRECATED_MODE_ALIASES for why persisted rows must keep working.
    const normalized = normalizeHarnessMode(rawMode);
    if (normalized === null) {
      return { error: 'metadata.mode must be one of: colocated, separate' };
    }
    mode = normalized;
  }

  if (!entry.supportedModes.includes(mode)) {
    const allowed = entry.supportedModes.join(', ');
    return { error: `harness '${harness}' does not support mode '${mode}' (allowed: ${allowed})` };
  }

  return { harness, mode };
}
