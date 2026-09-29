// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { rewriteMcpServers, withMcpAuthorization } from '../../src/mcp/rewrite.js';

describe('rewriteMcpServers', () => {
  it('rotates authorization immutably without dropping routing or SDK settings', () => {
    const servers = {
      remote: {
        type: 'http' as const,
        url: 'https://gateway.example/v1/mcp',
        alwaysLoad: true,
        headers: { authorization: 'Bearer old', 'X-Orca-Backend': 'remote' },
        tools: [{ name: 'read', permission_policy: 'always_ask' }],
      },
    };
    const updated = withMcpAuthorization(servers, 'new');
    expect(updated.remote).toEqual({
      ...servers.remote,
      headers: { Authorization: 'Bearer new', 'X-Orca-Backend': 'remote' },
    });
    expect(updated.remote).not.toBe(servers.remote);
    expect(updated.remote!.tools).toBe(servers.remote.tools);
    expect(servers.remote.headers.authorization).toBe('Bearer old');
  });

  it('emits X-Orca-Credential-Id only for servers whose URL is in the credential map', () => {
    const out = rewriteMcpServers(
      [
        { name: 'github', url: 'https://api.githubcopilot.com/mcp/' },
        { name: 'pulsar', url: 'https://pulsar.internal/mcp' },
      ],
      'ses_abc',
      'https://ai-gateway.internal',
      'tok-xyz',
      new Map([['https://api.githubcopilot.com/mcp/', 'vcrd_gh']]),
    );
    expect(out['github']!.url).toBe('https://ai-gateway.internal/v1/mcp');
    expect(out['github']!.alwaysLoad).toBe(true);
    expect(out['github']!.headers['X-Orca-Backend']).toBe('github');
    expect(out['github']!.headers['X-Orca-Credential-Id']).toBe('vcrd_gh');
    expect(out['github']!.headers['Authorization']).toBe('Bearer tok-xyz');

    expect(out['pulsar']!.headers['X-Orca-Credential-Id']).toBeUndefined();
    expect(out['pulsar']!.headers['X-Orca-Backend']).toBe('pulsar');
  });

  it('omits the credential header when the map is empty', () => {
    const out = rewriteMcpServers(
      [{ name: 'github', url: 'https://api.githubcopilot.com/mcp/' }],
      'ses_abc',
      'https://ai-gateway.internal',
      'tok-xyz',
      new Map(),
    );
    expect(out['github']!.headers['X-Orca-Credential-Id']).toBeUndefined();
  });

  it('matches startup credential metadata using Anthropic URL normalization', () => {
    const out = rewriteMcpServers(
      [{ name: 'github', url: 'https://api.githubcopilot.com/mcp' }],
      'ses_abc',
      'https://ai-gateway.internal',
      'tok-xyz',
      new Map([['HTTPS://API.GITHUBCOPILOT.COM:443/mcp/', 'vcrd_gh']]),
    );
    expect(out['github']!.headers['X-Orca-Credential-Id']).toBe('vcrd_gh');
  });

  it('accepts the legacy MCP endpoint URL without appending the path twice', () => {
    const out = rewriteMcpServers(
      [{ name: 'github', url: 'https://api.githubcopilot.com/mcp/' }],
      'ses_abc',
      'https://ai-gateway.internal/v1/mcp',
      'tok-xyz',
      new Map(),
    );

    expect(out['github']!.url).toBe('https://ai-gateway.internal/v1/mcp');
  });
});
