// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Register the default `claude` provider on a {@link ProviderRegistry}.
//
// The runner constructs a harness by dispatching on `snapshot.provider`; this wires
// the `claude` key to a real {@link ClaudeAgentSdkHarness}. The factory reads the
// credential-free gateway egress block from the snapshot (the LLM base URL + scoped
// session JWT) and builds a transcript-store-backed SDK session adapter, so a
// `provider: "claude"` snapshot resolves to a live harness the loop can `start` +
// drive — not an unknown-provider 422.
//
// The transcript store is the boot-level collaborator (the SDK's `SessionStore`
// needs it to persist + reload conversation history); the per-session LLM egress
// comes from the snapshot, so the same registered provider serves any session
// regardless of its gateway URL. The SDK `query()` call is injectable end-to-end
// (factory → harness) so the whole path is unit-testable without the network.

import type { TranscriptStore } from '@orca/transcript-store-types';
import { ProviderRegistry, type ProviderFactory } from '../provider.js';
import { ClaudeAgentSdkAdapter } from './session-adapter.js';
import { ClaudeAgentSdkHarness, type ClaudeQuery } from './index.js';
import type { RunnerSnapshot } from '../../snapshot.js';

/** The provider name the claude harness registers under (matches the snapshot's). */
export const CLAUDE_PROVIDER_NAME = 'claude';

/** Collaborators the claude provider needs at boot (resolved once, in `main.ts`). */
export interface ClaudeProviderDeps {
  /** Transcript store backing the SDK `SessionStore` (history persist + reload). */
  store: TranscriptStore;
  /** Default model id when a snapshot did not pin one. */
  modelDefault: string;
  /**
   * Fallback LLM credential used when the snapshot egress carries no LLM JWT (e.g.
   * a dev runner pointed straight at the Anthropic API). Optional — gateway egress
   * normally supplies the scoped JWT per session, which takes precedence.
   */
  fallbackApiKey?: string;
  /** The SDK `query()` call; defaults to the real SDK. Injected for tests. */
  query?: ClaudeQuery;
}

/**
 * The gateway LLM egress the claude harness reads from a snapshot: the LLM-proxy
 * base URL + the scoped JWT the gateway swaps for the real upstream credential.
 * Both are optional — a snapshot with no LLM gateway leaves them absent.
 */
export interface ClaudeLlmEgress {
  baseURL?: string;
  apiKey?: string;
}

/**
 * Register the `claude` provider on `registry` and return it (for chaining).
 *
 * @throws Error if a `claude` provider is already registered (a wiring bug).
 */
export function registerClaudeProvider(
  registry: ProviderRegistry,
  deps: ClaudeProviderDeps,
): ProviderRegistry {
  const factory: ProviderFactory = (snapshot, ctx) => {
    const adapter = new ClaudeAgentSdkAdapter(deps.store, ctx.workspaceId);
    const egress = readGatewayLlmEgress(snapshot);
    const apiKey = egress.apiKey ?? deps.fallbackApiKey ?? '';
    const harnessOpts: ConstructorParameters<typeof ClaudeAgentSdkHarness>[0] = {
      apiKey,
      modelDefault: deps.modelDefault,
      adapter,
      workspaceId: ctx.workspaceId,
      sessionId: ctx.sessionId,
    };
    if (egress.baseURL !== undefined) {
      harnessOpts.baseURL = egress.baseURL;
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
    return new ClaudeAgentSdkHarness(harnessOpts);
  };
  registry.register(CLAUDE_PROVIDER_NAME, factory);
  return registry;
}

/**
 * Read the gateway LLM egress (base URL + scoped JWT) out of a snapshot's opaque
 * `egress` block. Returns an empty object for any non-gateway / malformed / absent
 * egress (the harness then falls back to its ambient credential resolution) — the
 * reader is defensive because the runner treats `egress` as opaque and the registry
 * owns its structural guarantees. Only the `gateway` mode carries LLM egress
 * (`{ mode: 'gateway', gateway: { llm_base_url?, llm_jwt? } }`); `sidecar` egress
 * proxies upstream secrets out of band and supplies no JWT here.
 */
export function readGatewayLlmEgress(snapshot: RunnerSnapshot): ClaudeLlmEgress {
  const egress = snapshot.egress;
  if (egress === null || typeof egress !== 'object') {
    return {};
  }
  const obj = egress as { mode?: unknown; gateway?: unknown };
  if (obj.mode !== 'gateway' || obj.gateway === null || typeof obj.gateway !== 'object') {
    return {};
  }
  const gateway = obj.gateway as { llm_base_url?: unknown; llm_jwt?: unknown };
  const out: ClaudeLlmEgress = {};
  if (typeof gateway.llm_base_url === 'string' && gateway.llm_base_url.length > 0) {
    out.baseURL = gateway.llm_base_url;
  }
  if (typeof gateway.llm_jwt === 'string' && gateway.llm_jwt.length > 0) {
    out.apiKey = gateway.llm_jwt;
  }
  return out;
}
