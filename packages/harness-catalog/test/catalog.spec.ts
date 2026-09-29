// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  resolveHarnessAnnotation,
  normalizeHarnessMode,
  harnessToProvider,
  HARNESS_CATALOG,
  DEFAULT_HARNESS,
  DEFAULT_MODE,
  type HarnessType,
  CLAUDE_FAST_MODE_MODEL_IDS,
  CLAUDE_MODEL_EFFORT_LEVELS,
  getClaudeModelEffortCapability,
  isClaudeFastModeModel,
  isClaudeModelEffortSupported,
} from '../src/index.js';

describe('resolveHarnessAnnotation', () => {
  it('defaults to claude_agent_sdk/separate when both keys are absent', () => {
    expect(resolveHarnessAnnotation({})).toEqual({ harness: 'claude_agent_sdk', mode: 'separate' });
    expect(resolveHarnessAnnotation(undefined)).toEqual({
      harness: DEFAULT_HARNESS,
      mode: DEFAULT_MODE,
    });
  });

  it('accepts an explicit colocated harness', () => {
    expect(resolveHarnessAnnotation({ harness: 'claude_code', mode: 'colocated' })).toEqual({
      harness: 'claude_code',
      mode: 'colocated',
    });
    expect(resolveHarnessAnnotation({ harness: 'codex', mode: 'colocated' })).toEqual({
      harness: 'codex',
      mode: 'colocated',
    });
    expect(resolveHarnessAnnotation({ harness: 'cursor', mode: 'colocated' })).toEqual({
      harness: 'cursor',
      mode: 'colocated',
    });
    expect(resolveHarnessAnnotation({ harness: 'pi', mode: 'colocated' })).toEqual({
      harness: 'pi',
      mode: 'colocated',
    });
    expect(resolveHarnessAnnotation({ harness: 'custom', mode: 'colocated' })).toEqual({
      harness: 'custom',
      mode: 'colocated',
    });
  });

  it("defaults the custom harness's mode to colocated and rejects separate", () => {
    // The generic native-CLI escape hatch: like its codex/cursor/pi siblings it is
    // colocated-only (it boots an operator-declared CLI inside the session sandbox).
    expect(resolveHarnessAnnotation({ harness: 'custom' })).toEqual({
      harness: 'custom',
      mode: 'colocated',
    });
    expect(resolveHarnessAnnotation({ harness: 'custom', mode: 'separate' })).toEqual({
      error: "harness 'custom' does not support mode 'separate' (allowed: colocated)",
    });
  });

  it("defaults mode to the harness's first supported mode when mode is absent", () => {
    expect(resolveHarnessAnnotation({ harness: 'claude_code' })).toEqual({
      harness: 'claude_code',
      mode: 'colocated',
    });
    expect(resolveHarnessAnnotation({ harness: 'claude_agent_sdk' })).toEqual({
      harness: 'claude_agent_sdk',
      mode: 'separate',
    });
  });

  it('rejects an unknown harness', () => {
    const r = resolveHarnessAnnotation({ harness: 'gpt5_cli', mode: 'colocated' });
    expect(r).toEqual({
      error:
        'metadata.harness must be one of: claude_agent_sdk, claude_agent_sdk_persistent, claude_code, pi_sdk, codex_sdk, codex, cursor, pi, custom, mock',
    });
  });

  it('accepts the persistent Claude Agent SDK harness (separate-mode) and defaults its mode', () => {
    // Provider B — the persistent live-session Claude harness. Like its lean sibling
    // `claude_agent_sdk` it is separate-mode only; the snapshot resolver maps it to the
    // distinct `claude-sdk-persistent` runner provider (asserted in harnessToProvider below).
    expect(resolveHarnessAnnotation({ harness: 'claude_agent_sdk_persistent' })).toEqual({
      harness: 'claude_agent_sdk_persistent',
      mode: 'separate',
    });
    expect(
      resolveHarnessAnnotation({ harness: 'claude_agent_sdk_persistent', mode: 'separate' }),
    ).toEqual({ harness: 'claude_agent_sdk_persistent', mode: 'separate' });
    expect(
      resolveHarnessAnnotation({ harness: 'claude_agent_sdk_persistent', mode: 'colocated' }),
    ).toEqual({
      error:
        "harness 'claude_agent_sdk_persistent' does not support mode 'colocated' (allowed: separate)",
    });
  });

  it('accepts the mock harness (colocated, LLM-free) and defaults its mode', () => {
    // `mock` is a session-runner provider — session-runner IS the sole `colocated`
    // engine — so, like the other runner-driven harnesses, it is colocated-only.
    expect(resolveHarnessAnnotation({ harness: 'mock' })).toEqual({
      harness: 'mock',
      mode: 'colocated',
    });
    expect(resolveHarnessAnnotation({ harness: 'mock', mode: 'colocated' })).toEqual({
      harness: 'mock',
      mode: 'colocated',
    });
    expect(resolveHarnessAnnotation({ harness: 'mock', mode: 'separate' })).toEqual({
      error: "harness 'mock' does not support mode 'separate' (allowed: colocated)",
    });
  });

  it('rejects an invalid mode value', () => {
    const r = resolveHarnessAnnotation({ harness: 'claude_code', mode: 'hybrid' });
    expect(r).toEqual({ error: 'metadata.mode must be one of: colocated, separate' });
  });

  it('accepts the deprecated `in_sandbox` alias and normalizes it to colocated', () => {
    // `metadata.mode` is stored verbatim and snapshotted into agent_versions,
    // and no migration rewrites the rows written before the rename. A value that
    // was valid yesterday must not become a 400 today.
    expect(resolveHarnessAnnotation({ harness: 'claude_code', mode: 'in_sandbox' })).toEqual({
      harness: 'claude_code',
      mode: 'colocated',
    });
    expect(resolveHarnessAnnotation({ harness: 'codex', mode: 'in_sandbox' })).toEqual({
      harness: 'codex',
      mode: 'colocated',
    });
  });

  it('validates the alias against supportedModes like the value it resolves to', () => {
    expect(resolveHarnessAnnotation({ harness: 'claude_agent_sdk', mode: 'in_sandbox' })).toEqual({
      error: "harness 'claude_agent_sdk' does not support mode 'colocated' (allowed: separate)",
    });
  });

  it('normalizeHarnessMode resolves the alias and rejects everything else', () => {
    expect(normalizeHarnessMode('colocated')).toBe('colocated');
    expect(normalizeHarnessMode('separate')).toBe('separate');
    expect(normalizeHarnessMode('in_sandbox')).toBe('colocated');
    expect(normalizeHarnessMode('hybrid')).toBeNull();
    expect(normalizeHarnessMode(undefined)).toBeNull();
    // Guards against a prototype-chain hit standing in for a real alias.
    expect(normalizeHarnessMode('toString')).toBeNull();
    expect(normalizeHarnessMode('constructor')).toBeNull();
  });

  it('rejects an unsupported (harness, mode) combination', () => {
    expect(resolveHarnessAnnotation({ harness: 'claude_agent_sdk', mode: 'colocated' })).toEqual({
      error: "harness 'claude_agent_sdk' does not support mode 'colocated' (allowed: separate)",
    });
    expect(resolveHarnessAnnotation({ harness: 'claude_code', mode: 'separate' })).toEqual({
      error: "harness 'claude_code' does not support mode 'separate' (allowed: colocated)",
    });
  });

  it('mode-only annotation: falls back to DEFAULT_HARNESS and validates mode against it', () => {
    expect(resolveHarnessAnnotation({ mode: 'separate' })).toEqual({
      harness: 'claude_agent_sdk',
      mode: 'separate',
    });
    expect(resolveHarnessAnnotation({ mode: 'colocated' })).toEqual({
      error: "harness 'claude_agent_sdk' does not support mode 'colocated' (allowed: separate)",
    });
  });

  it('ignores unrelated metadata keys', () => {
    expect(resolveHarnessAnnotation({ team: 'growth', harness: 'codex' })).toEqual({
      harness: 'codex',
      mode: 'colocated',
    });
  });

  it('catalog: claude_agent_sdk runs separate with no image/port; CLI harnesses run colocated with an image+port', () => {
    expect(HARNESS_CATALOG.claude_agent_sdk).toEqual({
      supportedModes: ['separate'],
      defaultImage: null,
      entrypoint: null,
      port: null,
      provider: 'claude',
    });
    expect(HARNESS_CATALOG.claude_code.supportedModes).toEqual(['colocated']);
    expect(typeof HARNESS_CATALOG.claude_code.defaultImage).toBe('string');
    expect(HARNESS_CATALOG.claude_code.port).toBe(4096);
    expect(HARNESS_CATALOG.codex.supportedModes).toEqual(['colocated']);
    expect(HARNESS_CATALOG.cursor).toEqual({
      supportedModes: ['colocated'],
      defaultImage: 'ghcr.io/orca-ae/sandbox-harness-cursor:latest',
      entrypoint: null,
      port: 4096,
      provider: 'cursor',
    });
    expect(HARNESS_CATALOG.pi).toEqual({
      supportedModes: ['colocated'],
      defaultImage: 'ghcr.io/orca-ae/sandbox-harness-pi:latest',
      entrypoint: null,
      port: 4096,
      provider: 'pi',
    });
    expect(HARNESS_CATALOG.custom).toEqual({
      supportedModes: ['colocated'],
      defaultImage: 'ghcr.io/orca-ae/sandbox-harness-custom:latest',
      entrypoint: null,
      port: 4096,
      provider: 'custom',
    });
  });
});

describe('harnessToProvider', () => {
  it('maps the lean Claude Agent SDK harness to the claude provider', () => {
    // The single source of the harness→provider mapping, shared by the registry
    // snapshot resolver and the harness-server in-sandbox builder.
    expect(harnessToProvider('claude_agent_sdk')).toBe('claude');
  });

  it('maps claude_code to the NATIVE CLI provider, not the SDK one', () => {
    // `claude_code` boots the real `claude` binary via the runner's `claude-code`
    // provider; `claude_agent_sdk` runs an in-process SDK loop via `claude`. This
    // entry is the only thing that separates them — `resolveAgentProvider` is the
    // single writer of `snapshot.provider` and reads it straight from the catalog —
    // and a turn driven by the SDK looks nearly identical on the wire to one driven
    // by the CLI, so nothing downstream would notice the difference.
    expect(harnessToProvider('claude_code')).toBe('claude-code');
    expect(harnessToProvider('claude_code')).not.toBe(harnessToProvider('claude_agent_sdk'));
  });

  it('maps the persistent Claude Agent SDK harness to the claude-sdk-persistent provider', () => {
    // Provider B is a DISTINCT runner provider from the lean `claude` (provider A):
    // the snapshot resolver stamps `provider: 'claude-sdk-persistent'` off this mapping,
    // and the runner's ProviderRegistry resolves that key to ClaudePersistentSdkHarness.
    expect(harnessToProvider('claude_agent_sdk_persistent')).toBe('claude-sdk-persistent');
  });

  it('maps codex to the codex provider', () => {
    expect(harnessToProvider('codex')).toBe('codex');
  });

  it('maps the cursor native-CLI harness to the cursor provider', () => {
    // The snapshot resolver stamps `provider: 'cursor'` off this mapping, and the runner's
    // ProviderRegistry resolves that key to the cursor native-CLI harness.
    expect(harnessToProvider('cursor')).toBe('cursor');
  });

  it('maps the pi native-CLI harness to the pi provider', () => {
    // The snapshot resolver stamps `provider: 'pi'` off this mapping, and the runner's
    // ProviderRegistry resolves that key to the pi native-CLI harness.
    expect(harnessToProvider('pi')).toBe('pi');
  });

  it('maps the generic custom native-CLI harness to the custom provider', () => {
    // The snapshot resolver stamps `provider: 'custom'` off this mapping, and the runner's
    // ProviderRegistry resolves that key to the generic custom native-CLI harness (which
    // boots the operator-declared CLI from the snapshot's `custom_spec`).
    expect(harnessToProvider('custom')).toBe('custom');
  });

  it('maps the mock harness to the mock provider', () => {
    // The deterministic, LLM-free provider the self-hosted e2e drives through the
    // real snapshot path — the resolver carries `mock` so the runner builds the
    // mock harness instead of a model-backed one.
    expect(harnessToProvider('mock')).toBe('mock');
  });

  it('is consistent with each catalog entry’s provider field', () => {
    // Drives off the catalog so the mapping cannot drift from the catalog data.
    for (const harness of Object.keys(HARNESS_CATALOG) as HarnessType[]) {
      expect(harnessToProvider(harness)).toBe(HARNESS_CATALOG[harness].provider);
    }
  });

  it('throws on a harness outside the catalog (forced cast) instead of falling back', () => {
    expect(() => harnessToProvider('gpt5_cli' as HarnessType)).toThrow(/unsupported harness/);
  });
});

describe('Claude model controls', () => {
  it('catalogs Claude fast-mode models once for every validation boundary', () => {
    expect(CLAUDE_FAST_MODE_MODEL_IDS).toEqual([
      'claude-opus-5-5',
      'claude-opus-5',
      'claude-opus-4-8',
    ]);
    expect(isClaudeFastModeModel('claude-opus-5-5')).toBe(true);
    expect(isClaudeFastModeModel('claude-opus-5')).toBe(true);
    expect(isClaudeFastModeModel('claude-opus-4-8')).toBe(true);
    expect(isClaudeFastModeModel('claude-sonnet-4-6')).toBe(false);
  });

  it('catalogs model-specific Claude effort levels and defaults', () => {
    expect(CLAUDE_MODEL_EFFORT_LEVELS).toEqual(['low', 'medium', 'high', 'xhigh', 'max']);
    expect(getClaudeModelEffortCapability('claude-opus-5-5')).toEqual({
      supportedEfforts: ['low', 'medium', 'high', 'xhigh', 'max'],
      defaultEffort: 'medium',
    });
    expect(getClaudeModelEffortCapability('claude-opus-5')).toEqual({
      supportedEfforts: ['low', 'medium', 'high', 'xhigh', 'max'],
      defaultEffort: 'high',
    });
    expect(getClaudeModelEffortCapability('claude-sonnet-4-6')).toEqual({
      supportedEfforts: ['low', 'medium', 'high', 'max'],
      defaultEffort: 'high',
    });
    expect(getClaudeModelEffortCapability('claude-opus-4-5-20251101')).toEqual({
      supportedEfforts: ['low', 'medium', 'high'],
      defaultEffort: 'high',
    });
    expect(getClaudeModelEffortCapability('claude-sonnet-4-5')).toBeNull();
    expect(isClaudeModelEffortSupported('claude-sonnet-4-6', 'max')).toBe(true);
    expect(isClaudeModelEffortSupported('claude-sonnet-4-6', 'xhigh')).toBe(false);
  });

  it('does not resolve a prototype member as a model capability', () => {
    // `__proto__`/`constructor`/`toString` would otherwise index an
    // Object.prototype member — truthy, then throwing on `.supportedEfforts`.
    for (const id of ['__proto__', 'constructor', 'toString']) {
      expect(getClaudeModelEffortCapability(id)).toBeNull();
      expect(isClaudeModelEffortSupported(id, 'high')).toBe(false);
    }
  });
});
