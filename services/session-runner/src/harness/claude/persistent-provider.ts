// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Register the `claude-sdk-persistent` provider (B) on a {@link ProviderRegistry}.
//
// The runner constructs a harness by dispatching on `snapshot.provider`; this wires
// the `claude-sdk-persistent` key to a real {@link ClaudePersistentSdkHarness} — the
// persistent live-session harness (the streaming-input `query()` kept alive across
// turns), as opposed to the lean `claude` provider's stateless one-shot-per-turn
// harness. It reads the SAME credential-free gateway egress block from the snapshot
// (the LLM base URL + scoped session JWT — reusing {@link readGatewayLlmEgress}) and
// builds the SAME transcript-store-backed SDK session adapter, so a
// `provider: "claude-sdk-persistent"` snapshot resolves to a live harness the loop can
// `start` + drive — not an unknown-provider 422. The streaming-input `query()` call is
// injectable end-to-end (factory → harness) so the whole persistent path is
// unit-testable without the network.

import type { TranscriptStore } from '@orca/transcript-store-types';
import { ProviderRegistry, type ProviderFactory } from '../provider.js';
import { ClaudeAgentSdkAdapter } from './session-adapter.js';
import { ClaudePersistentSdkHarness, type PersistentClaudeQuery } from './persistent.js';
import { readGatewayLlmEgress } from './provider.js';

/** The provider name the persistent claude harness registers under (matches the snapshot's). */
export const CLAUDE_PERSISTENT_PROVIDER_NAME = 'claude-sdk-persistent';

/** Collaborators the persistent claude provider needs at boot (resolved once, in `main.ts`). */
export interface ClaudePersistentProviderDeps {
  /** Transcript store backing the SDK `SessionStore` (history persist + the rebuild reload). */
  store: TranscriptStore;
  /** Default model id when neither a turn nor the snapshot pinned one. */
  modelDefault: string;
  /**
   * Optional allow-list of model ids a turn may select. When set, a per-turn model
   * override on a user message is admitted only if it is in this set (the snapshot's
   * pinned model and {@link modelDefault} are always implicitly permitted); an override
   * outside it falls back to the snapshot/default model with an `agent.status`
   * diagnostic. When unset, a per-turn override is admitted on FORMAT alone (the gateway
   * remains the model-policy enforcement point). Forwarded verbatim to the harness.
   */
  allowedModels?: readonly string[];
  /**
   * Fallback LLM credential used when the snapshot egress carries no LLM JWT (e.g. a
   * dev runner pointed straight at the Anthropic API). Optional — gateway egress
   * normally supplies the scoped JWT per session, which takes precedence.
   */
  fallbackApiKey?: string;
  /** The streaming-input `query()` call; defaults to the real SDK. Injected for tests. */
  query?: PersistentClaudeQuery;
}

/**
 * Register the `claude-sdk-persistent` provider on `registry` and return it (for
 * chaining).
 *
 * @throws Error if a `claude-sdk-persistent` provider is already registered (a wiring bug).
 */
export function registerClaudePersistentProvider(
  registry: ProviderRegistry,
  deps: ClaudePersistentProviderDeps,
): ProviderRegistry {
  const factory: ProviderFactory = (snapshot, ctx) => {
    const adapter = new ClaudeAgentSdkAdapter(deps.store, ctx.workspaceId);
    const egress = readGatewayLlmEgress(snapshot);
    const apiKey = egress.apiKey ?? deps.fallbackApiKey ?? '';
    const harnessOpts: ConstructorParameters<typeof ClaudePersistentSdkHarness>[0] = {
      apiKey,
      modelDefault: deps.modelDefault,
      adapter,
      workspaceId: ctx.workspaceId,
      sessionId: ctx.sessionId,
    };
    if (egress.baseURL !== undefined) {
      harnessOpts.baseURL = egress.baseURL;
    }
    if (deps.allowedModels !== undefined) {
      harnessOpts.allowedModels = deps.allowedModels;
    }
    // Capture the per-session sandbox from the context so the harness has it even when a
    // caller hand-builds a `SessionStartInput` without `.sandbox`. Production sets it on
    // the start input too (via `buildSessionStartInput`), so the two agree; this makes
    // the provider context a SUFFICIENT source rather than requiring the start-input
    // builder to be the sole carrier. Spread in only when present — under
    // `exactOptionalPropertyTypes` an explicit `undefined` is not assignable to `sandbox?`.
    if (ctx.sandbox !== undefined) {
      harnessOpts.sandbox = ctx.sandbox;
    }
    if (deps.query !== undefined) {
      harnessOpts.query = deps.query;
    }
    return new ClaudePersistentSdkHarness(harnessOpts);
  };
  registry.register(CLAUDE_PERSISTENT_PROVIDER_NAME, factory);
  return registry;
}
