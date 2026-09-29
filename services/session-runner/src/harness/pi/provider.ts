// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Register the `pi` native-CLI provider on a {@link ProviderRegistry}.
//
// The runner constructs a harness by dispatching on `snapshot.provider`; this wires the `pi` key
// to a real {@link PiCliHarness} — the harness that boots `pi --mode rpc` as a long-lived child and
// drives it over pi's newline-delimited JSON command/event protocol (as opposed to the in-process
// claude providers, which run the model inside the runner via the Agent SDK). It reads the SAME
// credential-free gateway egress block from the snapshot (the LLM base URL + scoped session JWT —
// reusing {@link readGatewayLlmEgress}), so a `provider: "pi"` snapshot resolves to a live harness
// the loop can `start` + drive — not an unknown-provider 422. The per-session sandbox rides the
// provider context; the native-CLI launcher is injectable end-to-end (deps → harness) so the whole
// path is unit-testable with a fake CLI, no real `pi` binary in CI.

import { ProviderRegistry, type ProviderFactory } from '../provider.js';
import { readGatewayLlmEgress } from '../claude/provider.js';
import { PiCliHarness } from './index.js';
import { launchNativeCli, type NativeCliLogger } from '../../sandbox/native-cli-launcher.js';

/** The provider name the pi native-CLI harness registers under (matches the snapshot's). */
export const PI_PROVIDER_NAME = 'pi';

/** Collaborators the pi provider needs at boot (resolved once, in `main.ts`). */
export interface PiProviderDeps {
  /** Default model id when a snapshot did not pin one. */
  modelDefault: string;
  /**
   * Fallback LLM credential used when the snapshot egress carries no LLM JWT (e.g. a dev runner
   * pointed straight at the Anthropic API). Optional — gateway egress normally supplies the scoped
   * JWT per session, which takes precedence.
   */
  fallbackApiKey?: string;
  /**
   * The CLI binary path/name (defaults to `pi` on the sandbox PATH). A self-hosted operator may
   * pin an absolute path here; a test injects a fake CLI runner.
   */
  cliCommand?: string;
  /** Prefix args before the generated flags (e.g. a fake CLI script path under `node`). */
  cliPrefixArgs?: string[];
  /**
   * The bridge entrypoint executable pi's orca extension spawns as its MCP server (defaults to the
   * current Node executable). A test injects `tsx` so the CLI child runs the TS entry.
   */
  bridgeCommand?: string;
  /** Prefix args before the bridge entry's own flags (the bridge entry module path). */
  bridgePrefixArgs?: string[];
  /**
   * The pi orca-extension module path wired via `--extension` (defaults to the shipped
   * `orca-extension.mjs`). A test injects the source path.
   */
  extensionPath?: string;
  /** The native-CLI launcher; defaults to the real {@link launchNativeCli}. Injected for tests. */
  launch?: typeof launchNativeCli;
  /**
   * Structured logger for faults that have NO other channel: pi's extension-UI reply is a bare Allow/Block option with no message field,
   * so when the approval GATE itself faults (a permission-store outage denying every tool
   * call in the session) this is the only place the reason can reach.
   */
  logger?: NativeCliLogger;
}

/**
 * Register the `pi` provider on `registry` and return it (for chaining).
 *
 * @throws Error if a `pi` provider is already registered (a wiring bug).
 */
export function registerPiProvider(
  registry: ProviderRegistry,
  deps: PiProviderDeps,
): ProviderRegistry {
  const factory: ProviderFactory = (snapshot, ctx) => {
    const egress = readGatewayLlmEgress(snapshot);
    const harnessOpts: ConstructorParameters<typeof PiCliHarness>[0] = {
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
    if (deps.extensionPath !== undefined) {
      harnessOpts.extensionPath = deps.extensionPath;
    }
    if (deps.launch !== undefined) {
      harnessOpts.launch = deps.launch;
    }
    if (deps.logger !== undefined) {
      harnessOpts.logger = deps.logger;
    }
    return new PiCliHarness(harnessOpts);
  };
  registry.register(PI_PROVIDER_NAME, factory);
  return registry;
}
