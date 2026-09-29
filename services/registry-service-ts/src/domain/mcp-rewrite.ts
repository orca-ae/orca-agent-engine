// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Rewrites an agent's `mcp_servers` config so every entry's URL points at the
 * ai-gateway and carries the headers the gateway needs to route + audit:
 *
 *   X-Orca-Backend     : original logical name (e.g. "github") — the gateway
 *                        looks this up in its destination registry to find the
 *                        upstream URL.
 *   X-Orca-Session-Id  : session id — used for audit + per-session vault scoping.
 *   X-Orca-Vault-Id    : (optional) vault id when a session vault matched this
 *                        server's URL. Callers build the `vaultByUrl` map by
 *                        walking session vault_ids → vault.target_url, first match
 *                        wins (per Anthropic Managed Agents vault-binding
 *                        semantics). Servers without a match get no header — the
 *                        gateway forwards unauthenticated and the upstream
 *                        typically returns 401.
 *   Authorization      : Bearer <session JWT> — the gateway validates this against
 *                        the registry's signing key and checks the claims include
 *                        the requested backend + vault.
 *
 * Output shape matches the Claude Agent SDK's `mcpServers` option: a
 * `Record<string, RewrittenServer>` keyed by logical server name.
 *
 * The rewrite is PURE and credential-free: the only credential-shaped value it
 * embeds is the caller-minted, audience-scoped session JWT — never an upstream
 * secret. This is the registry-native home of the rewrite the runner used to do;
 * the registry now owns snapshot composition end-to-end.
 */

export interface AgentMcpServer {
  name: string;
  url: string;
}

export interface RewrittenServer {
  type: 'http';
  url: string;
  headers: Record<string, string>;
  alwaysLoad: true;
}

export function rewriteMcpServers(
  servers: AgentMcpServer[],
  sessionId: string,
  gatewayUrl: string,
  jwt: string,
  vaultByUrl: Map<string, string>,
): Record<string, RewrittenServer> {
  const out: Record<string, RewrittenServer> = {};
  for (const s of servers) {
    const headers: Record<string, string> = {
      'X-Orca-Backend': s.name,
      'X-Orca-Session-Id': sessionId,
      Authorization: `Bearer ${jwt}`,
    };
    const matched = vaultByUrl.get(s.url);
    if (matched) headers['X-Orca-Vault-Id'] = matched;
    out[s.name] = { type: 'http', url: gatewayUrl, headers, alwaysLoad: true };
  }
  return out;
}
