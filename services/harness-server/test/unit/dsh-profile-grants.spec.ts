// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { buildToolPermissionPolicies } from '../../src/runner/dispatcher.js';
import type { DshProfileDescriptor } from '@orca/harness-catalog';

it('preserves managed policies and composes plugin policies with the same toolset defaults', () => {
  const tools = [
    {
      type: 'agent_toolset',
      default_config: { enabled: true, permission_policy: 'always_allow' },
      configs: {
        allowed: { enabled: true, permission_policy: { type: 'always_allow' } },
        ask: { enabled: true, permission_policy: 'always_ask' },
        denied: { enabled: true, permission_policy: 'always_deny' },
        missing_policy: { enabled: true },
        missing_enablement: { permission_policy: 'always_allow' },
        disabled: { enabled: false, permission_policy: 'always_allow' },
        '*': { enabled: true, permission_policy: 'always_allow' },
        mcp__external__remote: { enabled: true, permission_policy: 'always_allow' },
      },
    },
  ];
  const custom = buildToolPermissionPolicies({
    tools,
    mcp_servers: [],
    dsh_profile: {} as DshProfileDescriptor,
  });
  expect(custom.mcp__orca__allowed).toBe('always_allow');
  expect(custom.mcp__orca__ask).toBe('always_ask');
  expect(custom.mcp__orca__denied).toBe('always_deny');
  expect(custom.mcp__orca__disabled).toBe('always_deny');
  expect(custom['mcp__orca__*']).toBe('always_allow');
  const ordinary = buildToolPermissionPolicies({ tools, mcp_servers: [] });
  expect(custom).toEqual(ordinary);
});

it('retains qualified managed confirmation when enabled is omitted', () => {
  const policies = buildToolPermissionPolicies({
    tools: [
      {
        type: 'agent_toolset',
        configs: {
          mcp__orca__bash: { permission_policy: { type: 'always_ask' } },
          mcp__orca__write: { enabled: false },
        },
      },
    ],
    mcp_servers: [],
    dsh_profile: {} as DshProfileDescriptor,
  });
  expect(policies.mcp__orca__bash).toBe('always_ask');
  expect(policies.mcp__orca__write).toBe('always_deny');
});

it('denies the plugin toolset when no agent_toolset is declared', () => {
  expect(
    buildToolPermissionPolicies({
      tools: [],
      mcp_servers: [],
      dsh_profile: {} as DshProfileDescriptor,
    })['mcp__orca__*'],
  ).toBe('always_deny');
});
