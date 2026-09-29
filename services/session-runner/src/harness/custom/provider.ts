// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Register the generic `custom` native-CLI provider on a {@link ProviderRegistry}.
//
// The runner constructs a harness by dispatching on `snapshot.provider`; this wires the `custom` key
// to a real {@link CustomCliHarness} — the harness that boots an OPERATOR-DECLARED CLI as a long-
// lived child (per the declarative `custom_spec` block on the snapshot) and drives it over its
// stdin/stdout per the spec's mapping (as opposed to the in-process claude providers, which run the
// model inside the runner via the Agent SDK). It reads the SAME credential-free gateway egress block
// from the snapshot (the LLM base URL + scoped session JWT — reusing {@link readGatewayLlmEgress}),
// exposed to the CLI under neutral `ORCA_LLM_*` env vars, so a `provider: "custom"` snapshot resolves
// to a live harness the loop can `start` + drive — not an unknown-provider 422. The per-session
// sandbox rides the provider context; the native-CLI launcher is injectable end-to-end (deps →
// harness) so the whole path is unit-testable with a fake CLI, no real binary in CI.
//
// The declarative CLI spec rides the snapshot as `custom_spec` (opaque to the loop, validated by the
// harness's spec parser); the provider forwards it verbatim to the harness.

import { ProviderRegistry, type ProviderFactory } from '../provider.js';
import { readGatewayLlmEgress } from '../claude/provider.js';
import { CustomCliHarness } from './index.js';
import { launchNativeCli, type NativeCliLogger } from '../../sandbox/native-cli-launcher.js';

/** The provider name the custom native-CLI harness registers under (matches the snapshot's). */
export const CUSTOM_PROVIDER_NAME = 'custom';

/** Collaborators the custom provider needs at boot (resolved once, in `main.ts`). */
export interface CustomProviderDeps {
  /** Default model id when a snapshot did not pin one. */
  modelDefault: string;
  /**
   * Fallback LLM credential used when the snapshot egress carries no LLM JWT (e.g. a dev runner
   * pointed straight at an upstream API). Optional — gateway egress normally supplies the scoped
   * JWT per session, which takes precedence.
   */
  fallbackApiKey?: string;
  /**
   * Override the spec's `command` with this binary path/name (defaults to the spec's `command`).
   * A self-hosted operator normally pins the command in the spec; a test injects a fake CLI runner.
   */
  cliCommand?: string;
  /** Prefix args before the spec argv (e.g. a fake CLI script path under `node`). */
  cliPrefixArgs?: string[];
  /**
   * The bridge entrypoint executable the CLI spawns as its `orca` MCP server (defaults to the
   * current Node executable). A test injects `tsx` so the CLI child runs the TS entry.
   */
  bridgeCommand?: string;
  /** Prefix args before the bridge entry's own flags (the bridge entry module path). */
  bridgePrefixArgs?: string[];
  /** The native-CLI launcher; defaults to the real {@link launchNativeCli}. Injected for tests. */
  launch?: typeof launchNativeCli;
  /**
   * Structured logger for faults that have NO other channel: the spec's approval response frame carries a bare allow/deny value with no message field,
   * so when the approval GATE itself faults (a permission-store outage denying every tool
   * call in the session) this is the only place the reason can reach.
   */
  logger?: NativeCliLogger;
}

/**
 * Register the `custom` provider on `registry` and return it (for chaining).
 *
 * @throws Error if a `custom` provider is already registered (a wiring bug).
 */
export function registerCustomProvider(
  registry: ProviderRegistry,
  deps: CustomProviderDeps,
): ProviderRegistry {
  const factory: ProviderFactory = (snapshot, ctx) => {
    const egress = readGatewayLlmEgress(snapshot);
    const harnessOpts: ConstructorParameters<typeof CustomCliHarness>[0] = {
      // The declarative CLI spec rides the snapshot; the harness parses + validates it at start.
      spec: readCustomSpec(snapshot),
      modelDefault: deps.modelDefault,
      workspaceId: ctx.workspaceId,
      sessionId: ctx.sessionId,
    };
    const apiKey = egress.apiKey ?? deps.fallbackApiKey;
    if (apiKey !== undefined && apiKey.length > 0) {
      harnessOpts.apiKey = apiKey;
    }
    if (egress.baseURL !== undefined) {
      harnessOpts.baseURL = egress.baseURL;
    }
    // Capture the per-session sandbox from the context (spread in only when present — under
    // `exactOptionalPropertyTypes` an explicit `undefined` is not assignable to `sandbox?`).
    if (ctx.sandbox !== undefined) {
      harnessOpts.sandbox = ctx.sandbox;
    }
    if (deps.cliCommand !== undefined) {
      harnessOpts.cliCommand = deps.cliCommand;
    }
    if (deps.cliPrefixArgs !== undefined) {
      harnessOpts.cliPrefixArgs = deps.cliPrefixArgs;
    }
    if (deps.bridgeCommand !== undefined) {
      harnessOpts.bridgeCommand = deps.bridgeCommand;
    }
    if (deps.bridgePrefixArgs !== undefined) {
      harnessOpts.bridgePrefixArgs = deps.bridgePrefixArgs;
    }
    if (deps.launch !== undefined) {
      harnessOpts.launch = deps.launch;
    }
    if (deps.logger !== undefined) {
      harnessOpts.logger = deps.logger;
    }
    return new CustomCliHarness(harnessOpts);
  };
  registry.register(CUSTOM_PROVIDER_NAME, factory);
  return registry;
}

/**
 * Read the declarative CLI spec off the snapshot's `custom_spec` field. Left opaque here (the
 * harness's spec parser validates it fail-fast at start); a snapshot with no spec yields `undefined`,
 * which the harness surfaces as a clean capability error at start rather than a silent no-op.
 */
function readCustomSpec(snapshot: { custom_spec?: unknown }): unknown {
  return snapshot.custom_spec;
}
