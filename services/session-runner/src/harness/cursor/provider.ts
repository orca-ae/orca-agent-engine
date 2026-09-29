// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Register the `cursor` native-CLI provider on a {@link ProviderRegistry}.
//
// The runner constructs a harness by dispatching on `snapshot.provider`; this wires the `cursor`
// key to a real {@link CursorCliHarness} — the harness that boots the headless `cursor-agent`
// binary as a long-lived child and drives it over its stream-json stdio (as opposed to the
// in-process claude providers, which run the model inside the runner via the Agent SDK). It
// reads the SAME credential-free gateway egress block from the snapshot (the base URL + scoped
// session credential — reusing {@link readGatewayLlmEgress}) as the sibling native-CLI
// providers, so a `provider: "cursor"` snapshot resolves to a live harness the loop can `start`
// + drive — not an unknown-provider 422. The per-session sandbox rides the provider context; the
// native-CLI launcher is injectable end-to-end (deps → harness) so the whole path is
// unit-testable with a fake CLI, no real `cursor-agent` binary in CI.

import { ProviderRegistry, type ProviderFactory } from '../provider.js';
import { readGatewayLlmEgress } from '../claude/provider.js';
import { CursorCliHarness } from './index.js';
import { launchNativeCli } from '../../sandbox/native-cli-launcher.js';

/** The provider name the cursor native-CLI harness registers under (matches the snapshot's). */
export const CURSOR_PROVIDER_NAME = 'cursor';

/** Collaborators the cursor provider needs at boot (resolved once, in `main.ts`). */
export interface CursorProviderDeps {
  /** Default model id when a snapshot did not pin one. */
  modelDefault: string;
  /**
   * Fallback credential used when the snapshot egress carries no scoped credential (e.g. a dev
   * runner pointed straight at Cursor's backend). Optional — gateway egress normally supplies
   * the scoped credential per session, which takes precedence.
   */
  fallbackApiKey?: string;
  /**
   * The CLI binary path/name (defaults to `cursor-agent` on the sandbox PATH). A self-hosted
   * operator may pin an absolute path here; a test injects a fake CLI runner.
   */
  cliCommand?: string;
  /** Prefix args before the generated flags (e.g. a fake CLI script path under `node`). */
  cliPrefixArgs?: string[];
  /**
   * The bridge entrypoint executable cursor spawns as its `orca` MCP server (defaults to the
   * current Node executable). A test injects `tsx` so the CLI child runs the TS entry.
   */
  bridgeCommand?: string;
  /** Prefix args before the bridge entry's own flags (the bridge entry module path). */
  bridgePrefixArgs?: string[];
  /** The native-CLI launcher; defaults to the real {@link launchNativeCli}. Injected for tests. */
  launch?: typeof launchNativeCli;
}

/**
 * Register the `cursor` provider on `registry` and return it (for chaining).
 *
 * @throws Error if a `cursor` provider is already registered (a wiring bug).
 */
export function registerCursorProvider(
  registry: ProviderRegistry,
  deps: CursorProviderDeps,
): ProviderRegistry {
  const factory: ProviderFactory = (snapshot, ctx) => {
    const egress = readGatewayLlmEgress(snapshot);
    const harnessOpts: ConstructorParameters<typeof CursorCliHarness>[0] = {
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
    return new CursorCliHarness(harnessOpts);
  };
  registry.register(CURSOR_PROVIDER_NAME, factory);
  return registry;
}
