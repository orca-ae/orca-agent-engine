// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  toCanonicalToolName,
  toAnthropicWireToolName,
} from '../../src/contracts/toolset-aliasing.js';

describe('toCanonicalToolName', () => {
  it('rewrites agent_toolset_20260401 → agent_toolset', () => {
    expect(toCanonicalToolName('agent_toolset_20260401')).toBe('agent_toolset');
  });
  it('passes through other names unchanged', () => {
    expect(toCanonicalToolName('mcp_toolset')).toBe('mcp_toolset');
    expect(toCanonicalToolName('custom')).toBe('custom');
  });
});

describe('toAnthropicWireToolName', () => {
  it('emits agent_toolset_20260401 if orcaBeta=false (Anthropic SDK clients)', () => {
    expect(toAnthropicWireToolName('agent_toolset', false)).toBe('agent_toolset_20260401');
  });
  it('emits agent_toolset if orcaBeta=true (first-party clients)', () => {
    expect(toAnthropicWireToolName('agent_toolset', true)).toBe('agent_toolset');
  });
});
