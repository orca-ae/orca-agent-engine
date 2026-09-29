// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  authoritativeUsageCallerForSnapshot,
  registryOwnsCodexUsage,
} from '../../src/domain/usage-authority.js';

function snapshot(metadata: Record<string, unknown>) {
  return {
    id: 'agt_usage_authority',
    name: 'usage authority test agent',
    version: 1,
    model: { provider: 'anthropic', id: 'claude-test' },
    system: '',
    tools: [],
    mcp_servers: [],
    skills: [],
    metadata,
    multiagent: null,
  };
}

describe('authoritative session usage caller', () => {
  it('selects AI Gateway for colocated sessions only after its Registry sink is enabled', () => {
    expect(
      authoritativeUsageCallerForSnapshot(
        snapshot({ harness: 'claude_code', mode: 'colocated' }),
        true,
      ),
    ).toBe('ai-gateway');
    expect(
      authoritativeUsageCallerForSnapshot(snapshot({ harness: 'claude_code', mode: 'colocated' })),
    ).toBe('harness');
  });

  it('selects Harness for separate and default sessions', () => {
    expect(
      authoritativeUsageCallerForSnapshot(
        snapshot({ harness: 'claude_agent_sdk', mode: 'separate' }),
        true,
      ),
    ).toBe('harness');
    expect(authoritativeUsageCallerForSnapshot(snapshot({}))).toBe('harness');
  });

  it('keeps SDK usage authoritative for both Codex modes with Gateway sink enabled', () => {
    for (const mode of ['separate', 'colocated'])
      expect(
        authoritativeUsageCallerForSnapshot(snapshot({ harness: 'codex_sdk', mode }), true),
      ).toBe('harness');
  });

  it('reserves internal Codex usage for the Registry only on self-hosted targets', () => {
    const state = snapshot({ harness: 'codex_sdk', mode: 'colocated' });
    expect(registryOwnsCodexUsage(state, 'cloud')).toBe(false);
    expect(registryOwnsCodexUsage(state, 'self_hosted')).toBe(true);
    expect(registryOwnsCodexUsage(snapshot({ harness: 'codex_sdk' }), 'self_hosted')).toBe(false);
  });

  it('fails closed when the pinned harness annotation is invalid', () => {
    expect(
      authoritativeUsageCallerForSnapshot(snapshot({ harness: 'claude_code', mode: 'separate' })),
    ).toBeNull();
    expect(authoritativeUsageCallerForSnapshot({ metadata: {} })).toBeNull();
  });
});
