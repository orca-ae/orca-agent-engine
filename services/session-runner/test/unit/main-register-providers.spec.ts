// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for `defaultRegisterProviders` — the real provider wiring `main.ts` uses
// when no test seam overrides it. The composition tests in `main.spec.ts` inject a fake
// `registerProviders`, so they never exercise this function; this spec pins the ACTUAL
// wiring of `RunnerConfig` + the shared in-memory transcript store into the provider
// registrations.
//
// What it pins:
//   - both the lean `claude` provider (A) and the `claude-sdk-persistent` provider (B)
//     are registered over the ONE shared in-memory transcript store passed in, each
//     with the configured default model + (when set) the fallback LLM credential;
//   - the first-class `mock` provider is registered too (the LLM-free e2e provider);
//   - the per-turn model ALLOW-LIST (`config.provider.allowedModels`, from
//     `ANTHROPIC_ALLOWED_MODELS`) is forwarded to the PERSISTENT provider when the
//     operator configured one — the defense-in-depth model knob is actually active in
//     production, not just unit-tested in isolation;
//   - when the allow-list is UNSET, `allowedModels` is NOT passed (the harness keeps its
//     format-only default; the gateway remains the model-policy enforcement point);
//   - the lean provider never receives an allow-list (it takes no per-turn model override).
//
// The three `registerClaude*Provider` / `registerMockProvider` modules are mocked so the
// assertions read the exact `deps`/registry each received.

import { describe, expect, it, vi, beforeEach } from 'vitest';

// Hoisted so the spies exist before the (hoisted) `vi.mock` factories reference them.
const {
  registerClaudeProvider,
  registerClaudePersistentProvider,
  registerClaudeCodeProvider,
  registerCodexProvider,
  registerCursorProvider,
  registerPiProvider,
  registerCustomProvider,
  registerMockProvider,
} = vi.hoisted(() => ({
  registerClaudeProvider: vi.fn(),
  registerClaudePersistentProvider: vi.fn(),
  registerClaudeCodeProvider: vi.fn(),
  registerCodexProvider: vi.fn(),
  registerCursorProvider: vi.fn(),
  registerPiProvider: vi.fn(),
  registerCustomProvider: vi.fn(),
  registerMockProvider: vi.fn(),
}));

vi.mock('../../src/harness/claude/provider.js', () => ({
  registerClaudeProvider,
}));
vi.mock('../../src/harness/claude/persistent-provider.js', () => ({
  registerClaudePersistentProvider,
}));
vi.mock('../../src/harness/claude-code/provider.js', () => ({
  registerClaudeCodeProvider,
}));
vi.mock('../../src/harness/codex/provider.js', () => ({
  registerCodexProvider,
}));
vi.mock('../../src/harness/cursor/provider.js', () => ({
  registerCursorProvider,
}));
vi.mock('../../src/harness/pi/provider.js', () => ({
  registerPiProvider,
}));
vi.mock('../../src/harness/custom/provider.js', () => ({
  registerCustomProvider,
}));
vi.mock('../../src/harness/mock/provider.js', () => ({
  registerMockProvider,
}));

import { defaultRegisterProviders } from '../../src/main.js';
import { ProviderRegistry } from '../../src/harness/provider.js';
import { InMemoryTranscriptStore } from '../../src/transcript/in-memory-transcript-store.js';
import type { RunnerConfig } from '../../src/config.js';

function baseConfig(providerOverrides: Partial<RunnerConfig['provider']> = {}): RunnerConfig {
  return {
    bindingToken: 'binding-token',
    registryRunnerUrl: 'ws://registry:8081/runner',
    workspace: '/var/run/orca/ws',
    workspaceId: 'ws_1',
    idleTimeoutS: 0,
    provider: {
      modelDefault: 'claude-sonnet-4-5',
      ...providerOverrides,
    },
  };
}

/** The deps the persistent provider was registered with (last call). */
function lastPersistentDeps(): Record<string, unknown> {
  const call = registerClaudePersistentProvider.mock.calls.at(-1);
  return (call?.[1] ?? {}) as Record<string, unknown>;
}

/** The deps the lean provider was registered with (last call). */
function lastLeanDeps(): Record<string, unknown> {
  const call = registerClaudeProvider.mock.calls.at(-1);
  return (call?.[1] ?? {}) as Record<string, unknown>;
}

/** The deps the claude-code native-CLI provider was registered with (last call). */
function lastClaudeCodeDeps(): Record<string, unknown> {
  const call = registerClaudeCodeProvider.mock.calls.at(-1);
  return (call?.[1] ?? {}) as Record<string, unknown>;
}

/** The deps the codex native-CLI provider was registered with (last call). */
function lastCodexDeps(): Record<string, unknown> {
  const call = registerCodexProvider.mock.calls.at(-1);
  return (call?.[1] ?? {}) as Record<string, unknown>;
}

/** The deps the cursor native-CLI provider was registered with (last call). */
function lastCursorDeps(): Record<string, unknown> {
  const call = registerCursorProvider.mock.calls.at(-1);
  return (call?.[1] ?? {}) as Record<string, unknown>;
}

/** The deps the pi native-CLI provider was registered with (last call). */
function lastPiDeps(): Record<string, unknown> {
  const call = registerPiProvider.mock.calls.at(-1);
  return (call?.[1] ?? {}) as Record<string, unknown>;
}

/** The deps the custom native-CLI provider was registered with (last call). */
function lastCustomDeps(): Record<string, unknown> {
  const call = registerCustomProvider.mock.calls.at(-1);
  return (call?.[1] ?? {}) as Record<string, unknown>;
}

describe('defaultRegisterProviders — claude + mock provider wiring', () => {
  beforeEach(() => {
    registerClaudeProvider.mockClear();
    registerClaudePersistentProvider.mockClear();
    registerClaudeCodeProvider.mockClear();
    registerCodexProvider.mockClear();
    registerCursorProvider.mockClear();
    registerPiProvider.mockClear();
    registerCustomProvider.mockClear();
    registerMockProvider.mockClear();
  });

  it('registers BOTH claude providers over the ONE shared in-memory transcript store passed in', () => {
    const registry = new ProviderRegistry();
    const store = new InMemoryTranscriptStore();
    defaultRegisterProviders(registry, baseConfig(), store);

    expect(registerClaudeProvider).toHaveBeenCalledTimes(1);
    expect(registerClaudePersistentProvider).toHaveBeenCalledTimes(1);
    // Same registry instance threaded to both…
    expect(registerClaudeProvider.mock.calls[0]?.[0]).toBe(registry);
    expect(registerClaudePersistentProvider.mock.calls[0]?.[0]).toBe(registry);
    // …and the SAME (passed-in) in-memory transcript store object shared by both.
    expect(lastLeanDeps()['store']).toBe(store);
    expect(lastPersistentDeps()['store']).toBe(store);
    // Both carry the configured default model.
    expect(lastLeanDeps()['modelDefault']).toBe('claude-sonnet-4-5');
    expect(lastPersistentDeps()['modelDefault']).toBe('claude-sonnet-4-5');
  });

  it('registers the first-class mock provider (the LLM-free e2e provider) on the same registry', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(registry, baseConfig(), new InMemoryTranscriptStore());
    expect(registerMockProvider).toHaveBeenCalledTimes(1);
    expect(registerMockProvider.mock.calls[0]?.[0]).toBe(registry);
  });

  it('registers the claude-code native-CLI provider on the same registry with the default model', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(registry, baseConfig(), new InMemoryTranscriptStore());
    expect(registerClaudeCodeProvider).toHaveBeenCalledTimes(1);
    // Same registry instance threaded to it…
    expect(registerClaudeCodeProvider.mock.calls[0]?.[0]).toBe(registry);
    // …carrying the configured default model. The native-CLI harness holds its history in
    // the CLI process, so (unlike the in-process claude providers) it is NOT given a store.
    expect(lastClaudeCodeDeps()['modelDefault']).toBe('claude-sonnet-4-5');
    expect(lastClaudeCodeDeps()).not.toHaveProperty('store');
  });

  it('forwards the fallback LLM credential to the claude-code provider when set', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(
      registry,
      baseConfig({ fallbackApiKey: 'sk-fallback' }),
      new InMemoryTranscriptStore(),
    );
    expect(lastClaudeCodeDeps()['fallbackApiKey']).toBe('sk-fallback');
  });

  it('registers the codex native-CLI provider on the same registry with the default model', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(registry, baseConfig(), new InMemoryTranscriptStore());
    expect(registerCodexProvider).toHaveBeenCalledTimes(1);
    // Same registry instance threaded to it…
    expect(registerCodexProvider.mock.calls[0]?.[0]).toBe(registry);
    // …carrying the configured default model. The native-CLI harness holds its history in
    // the CLI process, so (like claude-code, unlike the in-process providers) it gets no store.
    expect(lastCodexDeps()['modelDefault']).toBe('claude-sonnet-4-5');
    expect(lastCodexDeps()).not.toHaveProperty('store');
  });

  it('forwards the fallback LLM credential to the codex provider when set', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(
      registry,
      baseConfig({ fallbackApiKey: 'sk-fallback' }),
      new InMemoryTranscriptStore(),
    );
    expect(lastCodexDeps()['fallbackApiKey']).toBe('sk-fallback');
  });

  it('registers the cursor native-CLI provider on the same registry with the default model', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(registry, baseConfig(), new InMemoryTranscriptStore());
    expect(registerCursorProvider).toHaveBeenCalledTimes(1);
    // Same registry instance threaded to it…
    expect(registerCursorProvider.mock.calls[0]?.[0]).toBe(registry);
    // …carrying the configured default model. The native-CLI harness holds its history in
    // the CLI process, so (like claude-code/codex, unlike the in-process providers) it gets no store.
    expect(lastCursorDeps()['modelDefault']).toBe('claude-sonnet-4-5');
    expect(lastCursorDeps()).not.toHaveProperty('store');
  });

  it('forwards the fallback LLM credential to the cursor provider when set', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(
      registry,
      baseConfig({ fallbackApiKey: 'sk-fallback' }),
      new InMemoryTranscriptStore(),
    );
    expect(lastCursorDeps()['fallbackApiKey']).toBe('sk-fallback');
  });

  it('registers the pi native-CLI provider on the same registry with the default model', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(registry, baseConfig(), new InMemoryTranscriptStore());
    expect(registerPiProvider).toHaveBeenCalledTimes(1);
    // Same registry instance threaded to it…
    expect(registerPiProvider.mock.calls[0]?.[0]).toBe(registry);
    // …carrying the configured default model. The native-CLI harness holds its history in the CLI
    // process, so (like claude-code/codex, unlike the in-process providers) it gets no store.
    expect(lastPiDeps()['modelDefault']).toBe('claude-sonnet-4-5');
    expect(lastPiDeps()).not.toHaveProperty('store');
  });

  it('forwards the fallback LLM credential to the pi provider when set', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(
      registry,
      baseConfig({ fallbackApiKey: 'sk-fallback' }),
      new InMemoryTranscriptStore(),
    );
    expect(lastPiDeps()['fallbackApiKey']).toBe('sk-fallback');
  });

  it('registers the custom native-CLI provider on the same registry with the default model', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(registry, baseConfig(), new InMemoryTranscriptStore());
    expect(registerCustomProvider).toHaveBeenCalledTimes(1);
    // Same registry instance threaded to it…
    expect(registerCustomProvider.mock.calls[0]?.[0]).toBe(registry);
    // …carrying the configured default model. The native-CLI harness holds its history in the CLI
    // process, so (like claude-code/codex/cursor/pi, unlike the in-process providers) it gets no store.
    expect(lastCustomDeps()['modelDefault']).toBe('claude-sonnet-4-5');
    expect(lastCustomDeps()).not.toHaveProperty('store');
  });

  it('forwards the fallback LLM credential to the custom provider when set', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(
      registry,
      baseConfig({ fallbackApiKey: 'sk-fallback' }),
      new InMemoryTranscriptStore(),
    );
    expect(lastCustomDeps()['fallbackApiKey']).toBe('sk-fallback');
  });

  it('FORWARDS the per-turn model allow-list to the persistent provider when configured', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(
      registry,
      baseConfig({ allowedModels: ['claude-opus-4', 'claude-sonnet-4'] }),
      new InMemoryTranscriptStore(),
    );

    // The defense-in-depth allow-list reached the persistent provider verbatim…
    expect(lastPersistentDeps()['allowedModels']).toEqual(['claude-opus-4', 'claude-sonnet-4']);
    // …and the lean provider (no per-turn model override) never receives it.
    expect(lastLeanDeps()).not.toHaveProperty('allowedModels');
  });

  it('does NOT pass allowedModels when the allow-list is unset (format-only default; gateway is the policy point)', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(registry, baseConfig(), new InMemoryTranscriptStore()); // no allowedModels in config

    expect(lastPersistentDeps()).not.toHaveProperty('allowedModels');
    expect(lastLeanDeps()).not.toHaveProperty('allowedModels');
  });

  it('forwards the fallback LLM credential to both providers when set, alongside the allow-list', () => {
    const registry = new ProviderRegistry();
    defaultRegisterProviders(
      registry,
      baseConfig({ fallbackApiKey: 'sk-fallback', allowedModels: ['claude-opus-4'] }),
      new InMemoryTranscriptStore(),
    );

    expect(lastLeanDeps()['fallbackApiKey']).toBe('sk-fallback');
    expect(lastPersistentDeps()['fallbackApiKey']).toBe('sk-fallback');
    expect(lastPersistentDeps()['allowedModels']).toEqual(['claude-opus-4']);
  });
});
