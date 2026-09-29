// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { stripMcpServerPermissionPolicyFields } from '../../src/api/mcp-servers.js';

describe('agent route normalization', () => {
  it('strips legacy MCP server permission policies', () => {
    expect(
      stripMcpServerPermissionPolicyFields([
        { name: 'github', url: 'https://example.test/mcp' },
        {
          name: 'linear',
          url: 'https://linear.example/mcp',
          permission_policy: 'unexpected_policy',
        },
        {
          name: 'trusted',
          url: 'https://trusted.example/mcp',
          permission_policy: 'always_allow',
        },
      ]),
    ).toEqual([
      { name: 'github', url: 'https://example.test/mcp' },
      { name: 'linear', url: 'https://linear.example/mcp' },
      { name: 'trusted', url: 'https://trusted.example/mcp' },
    ]);
  });
});
