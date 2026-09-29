// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  buildToolPermissionPolicies,
  enabledMcpToolsetServerNames,
  enabledOrcaToolNames,
  hasEnabledReadTool,
} from '../../src/runner/dispatcher.js';

describe('dispatcher remote MCP toolsets', () => {
  it('enables only declared remote MCP toolsets', () => {
    const agentTools = [
      { type: 'agent_toolset' },
      { type: 'mcp_toolset', mcp_server_name: 'github' },
    ];

    expect([...enabledMcpToolsetServerNames(agentTools)]).toEqual(['github']);
  });

  it('honors remote toolset default enabled and per-tool overrides', () => {
    expect([
      ...enabledMcpToolsetServerNames([
        {
          type: 'mcp_toolset',
          mcp_server_name: 'disabled',
          default_config: { enabled: false },
        },
        {
          type: 'mcp_toolset',
          mcp_server_name: 'selected',
          default_config: { enabled: false },
          configs: [{ name: 'create_issue', enabled: true }],
        },
      ]),
    ]).toEqual(['selected']);
  });

  it('denies disabled remote tools while allowing an enabled override', () => {
    expect(
      buildToolPermissionPolicies(
        {
          id: 'agt_test',
          name: 'agent',
          version: 1,
          system: '',
          skills: [],
          tools: [
            {
              type: 'mcp_toolset',
              mcp_server_name: 'github',
              default_config: { enabled: false },
              configs: [{ name: 'create_issue', enabled: true }],
            },
          ],
          mcp_servers: [{ name: 'github', url: 'https://mcp.example.com' }],
        },
        new Set(['github']),
      ),
    ).toEqual({
      'mcp__github__*': 'always_deny',
      mcp__github__create_issue: 'always_allow',
    });
  });

  it('prefers mcp_toolset default permission policy over mcp server default', () => {
    expect(
      buildToolPermissionPolicies({
        id: 'agt_test',
        name: 'agent',
        version: 1,
        system: '',
        skills: [],
        tools: [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'github',
            default_config: { permission_policy: { type: 'always_ask' } },
          },
        ],
        mcp_servers: [{ name: 'github', url: 'https://mcp.example.com' }],
      }),
    ).toEqual({ 'mcp__github__*': 'always_ask' });
  });

  it('reads mcp_toolset permission policies from Anthropic object shape', () => {
    expect(
      buildToolPermissionPolicies({
        id: 'agt_test',
        name: 'agent',
        version: 1,
        system: '',
        skills: [],
        tools: [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'github',
            default_config: { permission_policy: { type: 'always_allow' } },
          },
        ],
        mcp_servers: [{ name: 'github', url: 'https://mcp.example.com' }],
      }),
    ).toEqual({ 'mcp__github__*': 'always_allow' });
  });

  it('applies per-tool mcp_toolset configs before the server wildcard policy', () => {
    expect(
      buildToolPermissionPolicies({
        id: 'agt_test',
        name: 'agent',
        version: 1,
        system: '',
        skills: [],
        tools: [
          {
            type: 'mcp_toolset',
            mcp_server_name: 'github',
            default_config: { permission_policy: { type: 'always_ask' } },
            configs: [
              {
                name: 'mcp__github__create_issue',
                permission_policy: { type: 'always_allow' },
              },
            ],
          },
        ],
        mcp_servers: [{ name: 'github', url: 'https://mcp.example.com' }],
      }),
    ).toEqual({
      'mcp__github__*': 'always_ask',
      mcp__github__create_issue: 'always_allow',
    });
  });

  it('defaults enabled remote MCP toolsets to always_ask', () => {
    expect(
      buildToolPermissionPolicies({
        id: 'agt_test',
        name: 'agent',
        version: 1,
        system: '',
        skills: [],
        tools: [{ type: 'mcp_toolset', mcp_server_name: 'github' }],
        mcp_servers: [{ name: 'github', url: 'https://mcp.example.com' }],
      }),
    ).toEqual({ 'mcp__github__*': 'always_ask' });
  });

  it('omits policies for MCP toolsets absent from the runtime tool list', () => {
    expect(
      buildToolPermissionPolicies(
        {
          id: 'agt_test',
          name: 'agent',
          version: 1,
          system: '',
          skills: [],
          tools: [{ type: 'mcp_toolset', mcp_server_name: 'github' }],
          mcp_servers: [{ name: 'github', url: 'https://mcp.example.com' }],
        },
        new Set(),
      ),
    ).toEqual({});
  });
});

describe('dispatcher built-in agent toolset permission policies', () => {
  it('maps agent_toolset default object policy to the orca wildcard', () => {
    expect(
      buildToolPermissionPolicies({
        id: 'agt_test',
        name: 'agent',
        version: 1,
        system: '',
        skills: [],
        tools: [
          {
            type: 'agent_toolset',
            default_config: { permission_policy: { type: 'always_ask' } },
          },
        ],
        mcp_servers: [],
      }),
    ).toEqual({ 'mcp__orca__*': 'always_ask' });
  });

  it('maps agent_toolset_20260401 default bare string policy to the orca wildcard', () => {
    expect(
      buildToolPermissionPolicies({
        id: 'agt_test',
        name: 'agent',
        version: 1,
        system: '',
        skills: [],
        tools: [
          {
            type: 'agent_toolset_20260401',
            default_config: { permission_policy: 'always_ask' },
          },
        ],
        mcp_servers: [],
      }),
    ).toEqual({ 'mcp__orca__*': 'always_ask' });
  });

  it('maps built-in per-tool config policies to exact orca MCP tool names', () => {
    expect(
      buildToolPermissionPolicies({
        id: 'agt_test',
        name: 'agent',
        version: 1,
        system: '',
        skills: [],
        tools: [
          {
            type: 'agent_toolset',
            configs: [{ name: 'bash', permission_policy: { type: 'always_deny' } }],
          },
        ],
        mcp_servers: [],
      }),
    ).toEqual({ mcp__orca__bash: 'always_deny' });
  });

  it('does not emit orca policy keys when built-in policies are missing', () => {
    const policies = buildToolPermissionPolicies({
      id: 'agt_test',
      name: 'agent',
      version: 1,
      system: '',
      skills: [],
      tools: [{ type: 'agent_toolset', configs: [{ name: 'read' }] }],
      mcp_servers: [],
    });

    expect(Object.keys(policies).filter((key) => key.startsWith('mcp__orca__'))).toEqual([]);
  });

  it('does not require mcp_servers and does not affect enabled MCP server traversal', () => {
    const agentTools = [
      {
        type: 'agent_toolset',
        default_config: { permission_policy: { type: 'always_ask' } },
      },
    ];

    expect([...enabledMcpToolsetServerNames(agentTools)]).toEqual([]);
    expect(
      buildToolPermissionPolicies({
        id: 'agt_test',
        name: 'agent',
        version: 1,
        system: '',
        skills: [],
        tools: agentTools,
        mcp_servers: [],
      }),
    ).toEqual({ 'mcp__orca__*': 'always_ask' });
  });

  it('treats orca as a reserved MCP server name for remote toolsets', () => {
    const agentTools = [
      {
        type: 'agent_toolset',
        default_config: { permission_policy: { type: 'always_ask' } },
        configs: [{ name: 'bash', permission_policy: { type: 'always_deny' } }],
      },
      {
        type: 'mcp_toolset',
        mcp_server_name: 'orca',
        default_config: { permission_policy: { type: 'always_allow' } },
        configs: [{ name: 'bash', permission_policy: { type: 'always_allow' } }],
      },
    ];

    expect([...enabledMcpToolsetServerNames(agentTools)]).toEqual([]);
    expect(
      buildToolPermissionPolicies({
        id: 'agt_test',
        name: 'agent',
        version: 1,
        system: '',
        skills: [],
        tools: agentTools,
        mcp_servers: [{ name: 'orca', url: 'https://mcp.example.com' }],
      }),
    ).toEqual({
      'mcp__orca__*': 'always_ask',
      mcp__orca__bash: 'always_deny',
    });
  });
});

describe('dispatcher built-in runtime tool selection', () => {
  it('applies default enabled before per-tool enabled overrides', () => {
    const tools = [
      {
        type: 'agent_toolset',
        default_config: { enabled: false },
        configs: {
          read: { enabled: true },
          write: { enabled: null },
        },
      },
    ];

    expect(enabledOrcaToolNames(tools)).toEqual(['read']);
    expect(hasEnabledReadTool(tools)).toBe(true);
  });

  it('removes an explicitly disabled tool from both runtime exposure and the Skill gate', () => {
    const tools = [
      {
        type: 'agent_toolset_20260401',
        default_config: { enabled: true },
        configs: [{ name: 'read', enabled: false }],
      },
    ];

    expect(enabledOrcaToolNames(tools)).not.toContain('read');
    expect(hasEnabledReadTool(tools)).toBe(false);
    expect(
      buildToolPermissionPolicies({
        id: 'agt_test',
        name: 'agent',
        version: 1,
        system: '',
        skills: [],
        tools,
        mcp_servers: [],
      }),
    ).toMatchObject({ mcp__orca__read: 'always_deny' });
  });
});
