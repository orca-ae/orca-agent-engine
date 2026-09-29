// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { applySessionAgentOverrides } from '../../src/runner/dispatcher.js';
import type { AgentRecord, SessionRecord } from '../../src/clients/registry.js';

const baseAgent: AgentRecord = {
  id: 'agt_test',
  workspace_id: 'ws_test',
  name: 'agent',
  version: 2,
  system: '',
  skills: [],
  tools: [{ type: 'agent_toolset' }],
  mcp_servers: [{ name: 'github', url: 'https://mcp.example.com' }],
};

const baseSession: SessionRecord = {
  id: 'ses_test',
  workspace_id: 'ws_test',
  agent_id: 'agt_test',
  agent_version: 2,
};

describe('applySessionAgentOverrides (session-local overrides)', () => {
  it('falls back to the agent when the session carries no override', () => {
    const merged = applySessionAgentOverrides(baseAgent, baseSession);
    expect(merged).toBe(baseAgent);
    expect(merged.tools).toBe(baseAgent.tools);
    expect(merged.mcp_servers).toBe(baseAgent.mcp_servers);
  });

  it('full-replaces tools from the session override (does not merge)', () => {
    const merged = applySessionAgentOverrides(baseAgent, {
      ...baseSession,
      tools: [{ type: 'custom', name: 'only_this' }],
    });
    expect(merged.tools).toEqual([{ type: 'custom', name: 'only_this' }]);
    // mcp_servers untouched -> still the agent's
    expect(merged.mcp_servers).toBe(baseAgent.mcp_servers);
  });

  it('full-replaces mcp_servers from the session override', () => {
    const merged = applySessionAgentOverrides(baseAgent, {
      ...baseSession,
      mcp_servers: [{ name: 'linear', url: 'https://linear.example/mcp' }],
    });
    expect(merged.mcp_servers).toEqual([{ name: 'linear', url: 'https://linear.example/mcp' }]);
    expect(merged.tools).toBe(baseAgent.tools);
  });

  it('treats an empty-array override as a real (clearing) replacement', () => {
    const merged = applySessionAgentOverrides(baseAgent, { ...baseSession, tools: [] });
    expect(merged.tools).toEqual([]);
  });

  it('does not mutate the original agent record', () => {
    applySessionAgentOverrides(baseAgent, { ...baseSession, tools: [] });
    expect(baseAgent.tools).toEqual([{ type: 'agent_toolset' }]);
  });
});
