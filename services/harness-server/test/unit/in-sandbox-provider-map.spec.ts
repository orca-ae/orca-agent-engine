// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { HARNESS_CATALOG, type HarnessType } from '@orca/harness-catalog';
import {
  assertHarnessServerCanRunColocated,
  harnessToProvider,
} from '../../src/harness/in-sandbox/provider-map.js';

describe('harnessToProvider', () => {
  it('maps claude_code to "claude-code", which this path resolves by alias', () => {
    // `claude_code` names the NATIVE Claude Code CLI, so the catalog maps it to the
    // runner's `claude-code` provider rather than to the in-process SDK `claude`.
    // The retired in-sandbox path is unaffected: `@orca/sandbox-harness` declares
    // `claude-code` among its `claude` provider's aliases (`providers/claude.ts`),
    // so a deployment still on that path resolves the new id to the same provider.
    expect(harnessToProvider('claude_code')).toBe('claude-code');
  });

  it('maps claude_agent_sdk to "claude"', () => {
    expect(harnessToProvider('claude_agent_sdk')).toBe('claude');
  });

  it('maps codex to "codex"', () => {
    expect(harnessToProvider('codex')).toBe('codex');
  });

  it('resolves EVERY harness the catalog accepts, and agrees with the catalog', () => {
    // The parallel switch this file used to hold threw for cursor/pi/custom/mock
    // and claude_agent_sdk_persistent — harnesses `resolveHarnessAnnotation`
    // accepts, so `POST /v1/agents` returned 200 and the session then died on a
    // throw outside every emit path.
    for (const harness of Object.keys(HARNESS_CATALOG) as HarnessType[]) {
      expect(harnessToProvider(harness)).toBe(HARNESS_CATALOG[harness].provider);
    }
  });

  it('rejects unknown harness values instead of falling back silently', () => {
    expect(() => harnessToProvider('unknown_harness' as HarnessType)).toThrow(
      /unsupported harness/,
    );
  });
});

describe('assertHarnessServerCanRunColocated', () => {
  it('accepts a colocated harness the catalog gives an image and a port', () => {
    expect(() => assertHarnessServerCanRunColocated('claude_code')).not.toThrow();
    expect(() => assertHarnessServerCanRunColocated('codex')).not.toThrow();
    expect(() => assertHarnessServerCanRunColocated('cursor')).not.toThrow();
    expect(() => assertHarnessServerCanRunColocated('pi')).not.toThrow();
    expect(() => assertHarnessServerCanRunColocated('custom')).not.toThrow();
  });

  it('rejects a colocated harness with no in-sandbox image, naming the harness', () => {
    // `mock` is a session-runner in-process provider: there is no image for
    // harness-server to boot and no harness port to reach it on.
    expect(() => assertHarnessServerCanRunColocated('mock')).toThrow(
      /harness 'mock' declares no in-sandbox image\/port/,
    );
  });

  it('rejects an unknown harness', () => {
    expect(() => assertHarnessServerCanRunColocated('unknown_harness' as HarnessType)).toThrow(
      /unsupported harness/,
    );
  });
});
