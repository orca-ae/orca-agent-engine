// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Rewrites the agent's `mcp_servers` config so every entry's URL points at
 * `ai-gateway` and carries the headers the gateway needs to route + audit:
 *
 *   X-Orca-Backend     : original logical name (e.g. "github") — gateway looks
 *                        this up in its destination registry to find the upstream URL.
 *   X-Orca-Session-Id       : session id — used for audit + per-session scoping.
 *   X-Orca-Credential-Id    : (optional) credential id when a session credential
 *                             matched this server's URL. Callers build the
 *                             `credentialByUrl` map from registry runtime
 *                             credentials, first match wins. This is startup
 *                             metadata for policy/exact static overrides;
 *                             wildcard dispatch always uses Registry's live
 *                             authoritative binding.
 *   Authorization           : Bearer <session JWT> — gateway validates this
 *                             against the registry's signing key and checks the
 *                             requested backend allowlist.
 *
 * Output shape matches the Anthropic Claude Agent SDK's `mcpServers` option:
 * a `Record<string, McpHttpServerConfig>` keyed by logical server name.
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

/** Anthropic-compatible URL identity for startup credential metadata. */
export function canonicalMcpServerUrl(value: string): string | null {
  try {
    const parsed = new URL(value);
    if (
      (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') ||
      parsed.hostname === '' ||
      parsed.username !== '' ||
      parsed.password !== '' ||
      parsed.hash !== ''
    ) {
      return null;
    }
    const path = parsed.pathname.replace(/\/+$/u, '');
    return `${parsed.protocol}//${parsed.host}${path}${parsed.search}`;
  } catch {
    return null;
  }
}

export function rewriteMcpServers(
  servers: AgentMcpServer[],
  sessionId: string,
  gatewayUrl: string,
  jwt: string,
  credentialByUrl: Map<string, string>,
): Record<string, RewrittenServer> {
  const out: Record<string, RewrittenServer> = {};
  const mcpUrl = gatewayMcpEndpointUrl(gatewayUrl);
  const credentialByCanonicalUrl = new Map<string, string>();
  for (const [url, credentialId] of credentialByUrl) {
    const canonical = canonicalMcpServerUrl(url);
    if (canonical !== null && !credentialByCanonicalUrl.has(canonical)) {
      credentialByCanonicalUrl.set(canonical, credentialId);
    }
  }
  for (const s of servers) {
    const headers: Record<string, string> = {
      'X-Orca-Backend': s.name,
      'X-Orca-Session-Id': sessionId,
      Authorization: `Bearer ${jwt}`,
    };
    const canonical = canonicalMcpServerUrl(s.url);
    const matched = canonical === null ? undefined : credentialByCanonicalUrl.get(canonical);
    if (matched) headers['X-Orca-Credential-Id'] = matched;
    out[s.name] = { type: 'http', url: mcpUrl, headers, alwaysLoad: true };
  }
  return out;
}

/** Replace only gateway authentication, preserving routing and SDK tool policy fields. */
export function withMcpAuthorization<T extends { headers: Record<string, string> }>(
  servers: Record<string, T>,
  token: string,
): Record<string, T> {
  return Object.fromEntries(
    Object.entries(servers).map(([name, server]) => [
      name,
      {
        ...server,
        headers: {
          ...Object.fromEntries(
            Object.entries(server.headers).filter(([key]) => key.toLowerCase() !== 'authorization'),
          ),
          Authorization: `Bearer ${token}`,
        },
      },
    ]),
  );
}

export function gatewayMcpEndpointUrl(gatewayUrl: string): string {
  let trimmed = gatewayUrl;
  while (trimmed.endsWith('/')) trimmed = trimmed.slice(0, -1);
  if (trimmed.endsWith('/v1/mcp')) return trimmed;
  return trimmed + '/v1/mcp';
}
