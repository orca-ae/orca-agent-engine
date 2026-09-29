// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the registry-side MCP-server rewrite.
//
// The registry composes a credential-free agent snapshot at session start and
// delivers it to the runner over the tunnel. Part of that snapshot (gateway
// egress mode) is the agent's `mcp_servers` rewritten so every entry points at
// the ai-gateway and carries the routing + scoping headers the gateway needs.
//
// The contract asserted here:
//   - every server URL is rewritten to the single gateway URL;
//   - the logical server name is carried in `X-Orca-Backend` so the gateway can
//     resolve the upstream;
//   - the session id is carried for audit + per-session vault scoping;
//   - the session JWT is carried as `Authorization: Bearer <jwt>`;
//   - a vault id is attached ONLY when the session vault target_url matched the
//     server's original URL (first-match-wins), and absent otherwise.

import { describe, it, expect } from 'vitest';
import { rewriteMcpServers } from '../../src/domain/mcp-rewrite.js';

const SESSION_ID = 'ses_rewrite_1';
const GATEWAY_URL = 'https://ai-gateway.internal/mcp';
const JWT = 'eyJ.session.jwt';

describe('rewriteMcpServers', () => {
  it('rewrites every server URL to the gateway and stamps routing headers', () => {
    const out = rewriteMcpServers(
      [
        { name: 'github', url: 'https://github.example/mcp' },
        { name: 'jira', url: 'https://jira.example/mcp' },
      ],
      SESSION_ID,
      GATEWAY_URL,
      JWT,
      new Map(),
    );

    expect(Object.keys(out).sort()).toEqual(['github', 'jira']);
    for (const name of ['github', 'jira']) {
      const server = out[name]!;
      expect(server.type).toBe('http');
      expect(server.url).toBe(GATEWAY_URL);
      expect(server.alwaysLoad).toBe(true);
      expect(server.headers['X-Orca-Backend']).toBe(name);
      expect(server.headers['X-Orca-Session-Id']).toBe(SESSION_ID);
      expect(server.headers['Authorization']).toBe(`Bearer ${JWT}`);
      // No vault matched → no vault header.
      expect(server.headers['X-Orca-Vault-Id']).toBeUndefined();
    }
  });

  it('attaches X-Orca-Vault-Id only for a server whose URL matched a session vault', () => {
    const vaultByUrl = new Map<string, string>([['https://github.example/mcp', 'vlt_gh']]);
    const out = rewriteMcpServers(
      [
        { name: 'github', url: 'https://github.example/mcp' },
        { name: 'jira', url: 'https://jira.example/mcp' },
      ],
      SESSION_ID,
      GATEWAY_URL,
      JWT,
      vaultByUrl,
    );

    expect(out['github']!.headers['X-Orca-Vault-Id']).toBe('vlt_gh');
    expect(out['jira']!.headers['X-Orca-Vault-Id']).toBeUndefined();
  });

  it('embeds no raw secret — only the JWT and vault id reference', () => {
    const vaultByUrl = new Map<string, string>([['https://github.example/mcp', 'vlt_gh']]);
    const out = rewriteMcpServers(
      [{ name: 'github', url: 'https://github.example/mcp' }],
      SESSION_ID,
      GATEWAY_URL,
      JWT,
      vaultByUrl,
    );
    const serialized = JSON.stringify(out);
    // The only credential-shaped value is the opaque session JWT; the vault id is
    // a reference, never a secret. Asserting the JWT is the sole bearer value.
    expect(serialized).toContain(`Bearer ${JWT}`);
    expect(serialized).not.toContain('secret');
    expect(serialized).not.toContain('password');
  });

  it('returns an empty map for an empty server list', () => {
    expect(rewriteMcpServers([], SESSION_ID, GATEWAY_URL, JWT, new Map())).toEqual({});
  });
});
