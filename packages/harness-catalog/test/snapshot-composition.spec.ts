// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  composeSystemPrompt,
  intersectToolAllowlists,
  canonicalAgentToolNames,
  expandAllowedToolNames,
  enabledMcpToolsetServerNames,
  managedToolPermissionPolicyName,
  toolsetConfigEntries,
  toolsetPermissionPolicies,
  toCanonicalToolName,
  toAnthropicWireToolName,
  AGENT_TOOLSET_LOGICAL_TOOLS,
  ORCA_MCP_SERVER_NAME,
} from '../src/snapshot-composition.js';

describe('toCanonicalToolName / toAnthropicWireToolName', () => {
  it('canonicalizes the dated agent_toolset alias', () => {
    expect(toCanonicalToolName('agent_toolset_20260401')).toBe('agent_toolset');
    expect(toCanonicalToolName('agent_toolset')).toBe('agent_toolset');
    expect(toCanonicalToolName('lookup_invoice')).toBe('lookup_invoice');
  });

  it('maps canonical → dated wire name only when orcaBeta is off', () => {
    expect(toAnthropicWireToolName('agent_toolset', false)).toBe('agent_toolset_20260401');
    expect(toAnthropicWireToolName('agent_toolset', true)).toBe('agent_toolset');
    expect(toAnthropicWireToolName('bash', false)).toBe('bash');
  });
});

describe('composeSystemPrompt', () => {
  it('joins the agent prompt with each non-blank skill prompt in order', () => {
    expect(
      composeSystemPrompt('Agent.', [
        { systemPrompt: 'A.' },
        { systemPrompt: '' },
        { systemPrompt: 'B.' },
      ]),
    ).toBe('Agent.\n\nA.\n\nB.');
  });

  it('drops a blank agent prompt', () => {
    expect(composeSystemPrompt('', [{ systemPrompt: 'Only skill.' }])).toBe('Only skill.');
  });
});

describe('intersectToolAllowlists', () => {
  it('a null/absent allowlist does not narrow', () => {
    expect(
      intersectToolAllowlists(['agent_toolset', 'mcp_toolset'], [{ toolAllowlist: null }]),
    ).toEqual(['agent_toolset', 'mcp_toolset']);
    expect(intersectToolAllowlists(['agent_toolset'], [{}])).toEqual(['agent_toolset']);
  });

  it('intersects cumulatively, preserving agent order', () => {
    expect(
      intersectToolAllowlists(
        ['agent_toolset', 'lookup_invoice', 'mcp_toolset'],
        [
          { toolAllowlist: ['agent_toolset', 'lookup_invoice'] },
          { toolAllowlist: ['lookup_invoice'] },
        ],
      ),
    ).toEqual(['lookup_invoice']);
  });
});

describe('canonicalAgentToolNames', () => {
  it('keeps agent_toolset/mcp_toolset atomic, surfaces a custom tool by name', () => {
    expect(
      canonicalAgentToolNames([
        { type: 'agent_toolset_20260401' },
        { type: 'custom', name: 'lookup_invoice' },
        { type: 'mcp_toolset', mcp_server_name: 'github' },
      ]),
    ).toEqual(['agent_toolset', 'lookup_invoice', 'mcp_toolset']);
  });

  it('drops a custom tool with no usable name', () => {
    expect(canonicalAgentToolNames([{ type: 'custom' }, { type: 'custom', name: '' }])).toEqual([]);
  });
});

describe('expandAllowedToolNames', () => {
  it('expands agent_toolset (and its dated alias) to the logical file/shell tools', () => {
    expect(expandAllowedToolNames(['agent_toolset']).sort()).toEqual(
      [...AGENT_TOOLSET_LOGICAL_TOOLS].sort(),
    );
    expect(expandAllowedToolNames(['agent_toolset_20260401']).sort()).toEqual(
      [...AGENT_TOOLSET_LOGICAL_TOOLS].sort(),
    );
  });

  it('drops mcp_toolset (surfaced separately, not a runnable tool name)', () => {
    expect(expandAllowedToolNames(['mcp_toolset'])).toEqual([]);
  });

  it('KEEPS a custom tool name verbatim alongside the agent_toolset expansion', () => {
    // Intentional registry-native behavior: a generic runner has no side channel
    // for custom tools, so a declared custom tool must survive into the snapshot.
    expect(expandAllowedToolNames(['agent_toolset', 'lookup_invoice']).sort()).toEqual(
      [...AGENT_TOOLSET_LOGICAL_TOOLS, 'lookup_invoice'].sort(),
    );
  });

  it('is empty for an empty input', () => {
    expect(expandAllowedToolNames([])).toEqual([]);
  });
});

describe('managedToolPermissionPolicyName', () => {
  // The contract (`registry-service-ts/src/contracts/agents.contract.ts`) accepts
  // `permission_policy` BOTH as the bare policy string and as an object with a
  // `type` discriminant, and `resolvedToolsetConfig` is the only caller — so a
  // reading that handles one shape and not the other silently drops the policy
  // and falls back to `always_allow`.
  it('reads the bare-string form', () => {
    expect(managedToolPermissionPolicyName('always_deny')).toBe('always_deny');
    expect(managedToolPermissionPolicyName('always_ask')).toBe('always_ask');
    expect(managedToolPermissionPolicyName('always_allow')).toBe('always_allow');
  });

  it('reads the object form via its `type` discriminant', () => {
    expect(managedToolPermissionPolicyName({ type: 'always_ask' })).toBe('always_ask');
    expect(managedToolPermissionPolicyName({ type: 'always_deny' })).toBe('always_deny');
  });

  it('is null for anything else — absent, malformed, or an unknown policy', () => {
    expect(managedToolPermissionPolicyName(undefined)).toBeNull();
    expect(managedToolPermissionPolicyName(null)).toBeNull();
    expect(managedToolPermissionPolicyName(42)).toBeNull();
    expect(managedToolPermissionPolicyName(['always_allow'])).toBeNull();
    expect(managedToolPermissionPolicyName({ type: 'sometimes_ask' })).toBeNull();
    expect(managedToolPermissionPolicyName({})).toBeNull();
  });
});

describe('toolsetConfigEntries', () => {
  // `ToolsetConfigs` is a union: an ARRAY of `{ name, ... }` entries or a
  // `{ [name]: { ... } }` MAP. Both halves are live on the wire.
  it('normalizes the array form, dropping malformed entries', () => {
    expect(
      toolsetConfigEntries([
        { name: 'create_issue', enabled: true },
        null,
        'nope',
        ['also-nope'],
        { enabled: true },
      ]),
    ).toEqual([{ name: 'create_issue', value: { name: 'create_issue', enabled: true } }]);
  });

  it('normalizes the map form, dropping non-object values', () => {
    expect(
      toolsetConfigEntries({
        create_issue: { enabled: true },
        list_issues: 'nope',
        close_issue: null,
      }),
    ).toEqual([{ name: 'create_issue', value: { enabled: true } }]);
  });

  it('is empty for a value that is neither an array nor an object', () => {
    expect(toolsetConfigEntries(undefined)).toEqual([]);
    expect(toolsetConfigEntries(null)).toEqual([]);
    expect(toolsetConfigEntries(42)).toEqual([]);
  });
});

describe('enabledMcpToolsetServerNames', () => {
  const tools = [{ type: 'agent_toolset' }, { type: 'mcp_toolset', mcp_server_name: 'github' }];

  it('enables servers only when mcp_toolset survived the intersection', () => {
    expect([...enabledMcpToolsetServerNames(tools, [])]).toEqual([]);
    expect([...enabledMcpToolsetServerNames(tools, ['agent_toolset'])]).toEqual([]);
    expect([...enabledMcpToolsetServerNames(tools, ['mcp_toolset'])]).toEqual(['github']);
  });

  it('recognizes the dated alias as mcp_toolset survival', () => {
    expect([
      ...enabledMcpToolsetServerNames(tools, ['agent_toolset_20260401', 'mcp_toolset']),
    ]).toEqual(['github']);
  });

  it('applies every filter when the intersection argument is omitted', () => {
    // The harness-server snapshot build calls this with ONE argument (it applies
    // no skill intersection here). Omitting the gate must not disable the
    // enabled / reserved-name filters below.
    expect([...enabledMcpToolsetServerNames(tools)]).toEqual(['github']);
  });

  it('is empty when the tool list itself is absent', () => {
    // The parameter is `AgentToolEntry[] | undefined`; an agent with no tools at
    // all is a supported call, not a type hole.
    expect([...enabledMcpToolsetServerNames(undefined)]).toEqual([]);
    expect([...enabledMcpToolsetServerNames(undefined, ['mcp_toolset'])]).toEqual([]);
  });

  it('EXCLUDES a toolset disabled by default_config', () => {
    // The whole gate on `allowed_mcp_server_names`: a server that reaches the
    // runner here is reachable, and nothing downstream re-applies this filter.
    const disabled = [
      { type: 'mcp_toolset', mcp_server_name: 'github', default_config: { enabled: false } },
    ];
    expect([...enabledMcpToolsetServerNames(disabled, ['mcp_toolset'])]).toEqual([]);
    expect([...enabledMcpToolsetServerNames(disabled)]).toEqual([]);
  });

  it('re-enables a default-disabled toolset when a per-tool config turns one tool on', () => {
    const overridden = [
      {
        type: 'mcp_toolset',
        mcp_server_name: 'github',
        default_config: { enabled: false },
        configs: [{ name: 'create_issue', enabled: true }],
      },
    ];
    expect([...enabledMcpToolsetServerNames(overridden, ['mcp_toolset'])]).toEqual(['github']);
  });

  it('EXCLUDES the reserved internal `orca` server name', () => {
    // `orca` names the runtime's own in-process tool server, never an outbound
    // MCP destination — an agent naming it is configuring permission policies
    // on the built-in surface.
    const reserved = [{ type: 'mcp_toolset', mcp_server_name: ORCA_MCP_SERVER_NAME }];
    expect([...enabledMcpToolsetServerNames(reserved, ['mcp_toolset'])]).toEqual([]);
    expect([...enabledMcpToolsetServerNames(reserved)]).toEqual([]);
  });

  it('EXCLUDES a toolset whose declared server name is empty or not a string', () => {
    // Filter #1 of the three load-bearing controls. `mcp_server_name` is read off
    // persisted JSONB metadata, so a missing or empty name is a real input — and
    // admitting it would put `''` into `allowed_mcp_server_names`, which nothing
    // downstream re-checks.
    const nameless = [
      { type: 'mcp_toolset' },
      { type: 'mcp_toolset', mcp_server_name: '' },
      { type: 'mcp_toolset', mcp_server_name: 42 as unknown as string },
    ];
    expect([...enabledMcpToolsetServerNames(nameless, ['mcp_toolset'])]).toEqual([]);
    expect([...enabledMcpToolsetServerNames(nameless)]).toEqual([]);
  });
});

describe('toolsetPermissionPolicies', () => {
  const compose = (tools: Parameters<typeof toolsetPermissionPolicies>[0]) =>
    toolsetPermissionPolicies(tools, 'orca', (tool) => tool.type === 'agent_toolset');

  it('leaves implicit defaults to the caller and ignores other toolsets', () => {
    expect(
      compose([
        { type: 'mcp_toolset', default_config: { enabled: false } },
        { type: 'agent_toolset', configs: { read: { enabled: true } } },
      ]),
    ).toEqual({});
  });

  it('keeps the first explicit wildcard and applies exact policies in either config shape', () => {
    expect(
      compose([
        {
          type: 'agent_toolset',
          default_config: { permission_policy: 'always_ask' },
          configs: {
            bash: { enabled: false },
            read: { permission_policy: { type: 'always_allow' } },
            '': { enabled: false },
            mcp__other__write: { enabled: false },
            mcp__orca__edit: { permission_policy: 'always_deny' },
          },
        },
        {
          type: 'agent_toolset',
          default_config: { permission_policy: 'always_deny' },
          configs: [{ name: 'read', permission_policy: 'always_ask' }],
        },
      ]),
    ).toEqual({
      'mcp__orca__*': 'always_ask',
      mcp__orca__bash: 'always_deny',
      mcp__orca__read: 'always_ask',
      mcp__orca__edit: 'always_deny',
    });
  });

  it('denies disabled defaults while explicitly enabled tools inherit the default policy', () => {
    expect(
      compose([
        {
          type: 'agent_toolset',
          default_config: { enabled: false },
          configs: {
            read: { enabled: true },
            write: {},
            edit: { enabled: true, permission_policy: 'always_ask' },
          },
        },
        {
          type: 'agent_toolset',
          default_config: { enabled: false, permission_policy: 'always_ask' },
          configs: { bash: { enabled: true } },
        },
      ]),
    ).toEqual({
      'mcp__orca__*': 'always_deny',
      mcp__orca__read: 'always_allow',
      mcp__orca__write: 'always_deny',
      mcp__orca__edit: 'always_ask',
      mcp__orca__bash: 'always_ask',
    });
  });
});
