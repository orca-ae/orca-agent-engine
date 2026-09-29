// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyRequest, FastifyReply } from 'fastify';
import type { DbClient } from '../persistence/postgres/client.js';
import type { OidcConfig, OrganizationAudienceLookups } from './oidc.js';
import { and, eq, inArray } from 'drizzle-orm';
import { organizations, workspaces } from '../persistence/postgres/schema.js';
import {
  buildApiKeyAuth,
  LegacyApiKeyFallbackRateLimitError,
  type ApiKeyAuthResult,
} from './api-key.js';
import { buildOidcAuth } from './oidc.js';
import type { AuthenticatedPrincipal } from './principal.js';
import { measureAuthStage } from '../observability/api-performance.js';

export interface AuthOptions {
  apiKeyProofCacheEnabled?: boolean | undefined;
  db: DbClient;
  oidc: OidcConfig;
}

/**
 * The entire unauthenticated surface of the public listener.
 *
 * Named rather than inlined so that "what can be reached without a key" is one
 * fact a test can read, instead of a disjunction buried in a hook. Adding an
 * entry here is a security decision; `test/unit/auth-allowlist.spec.ts` pins the
 * exact contents so one cannot arrive as a side effect of adding a route.
 *
 * Only the probes qualify. Their callers — a kubelet, a compose healthcheck, an
 * ingress — structurally cannot present a credential, and "this process is up"
 * says nothing about the API surface or about any principal. Group/version
 * discovery (`/api`, `/apis`) is *not* here: it names the API versions and
 * extension groups this deployment serves, which is an answer about the
 * deployment, and a caller that cannot present a key has no claim on it. See
 * `docs/managed-agents/api-groups-and-extensions.md#discovery`.
 *
 * `/metrics` is reachable through this hook only on the test-only combined
 * surface; production serves it on the separate internal listener, where this
 * public hook is not installed.
 */
export const UNAUTHENTICATED_PATHS: ReadonlySet<string> = new Set([
  '/healthz',
  '/readyz',
  '/metrics',
]);

/**
 * How many rows the "exactly one" lookups read.
 *
 * Both callers reject anything but a single row, so a second row is already the
 * whole answer and reading further would only make an unbounded organization or
 * workspace count into an unbounded query on the authentication path.
 */
const AMBIGUITY_PROBE_LIMIT = 2;

/**
 * Binds {@link OrganizationAudienceLookups} to the database.
 *
 * Each query answers only about ACTIVE rows, which is what lets the
 * authenticator treat archived and missing alike, and each returns just the
 * organization id and audience — never a whole row — so nothing else about an
 * organization becomes reachable from an unauthenticated request.
 */
function buildOrganizationAudienceLookups(db: DbClient): OrganizationAudienceLookups {
  return {
    async organizationsForAudiences(audiences) {
      if (audiences.length === 0) return [];
      return db
        .select({ id: organizations.id, audience: organizations.audience })
        .from(organizations)
        .where(
          and(eq(organizations.status, 'active'), inArray(organizations.audience, [...audiences])),
        )
        .limit(AMBIGUITY_PROBE_LIMIT);
    },
    async organizationForWorkspace(workspaceId) {
      const rows = await db
        .select({ id: organizations.id, audience: organizations.audience })
        .from(workspaces)
        .innerJoin(organizations, eq(workspaces.organizationId, organizations.id))
        .where(
          and(
            eq(workspaces.id, workspaceId),
            eq(workspaces.status, 'active'),
            eq(organizations.status, 'active'),
          ),
        )
        .limit(1);
      return rows[0] ?? null;
    },
    async activeWorkspaceIds(organizationId) {
      const rows = await db
        .select({ id: workspaces.id })
        .from(workspaces)
        .where(and(eq(workspaces.organizationId, organizationId), eq(workspaces.status, 'active')))
        .limit(AMBIGUITY_PROBE_LIMIT);
      return rows.map((row) => row.id);
    },
  };
}

export function buildAuth(opts: AuthOptions) {
  const apiKey = buildApiKeyAuth(opts.db, { proofCacheEnabled: opts.apiKeyProofCacheEnabled });
  const oidc = buildOidcAuth(opts.oidc, buildOrganizationAudienceLookups(opts.db));

  return async function authPreHandler(req: FastifyRequest, reply: FastifyReply): Promise<void> {
    // /internal/* endpoints: mesh-only traffic, skip app-layer auth.
    if (req.url.startsWith('/internal/')) {
      req.auth = { workspaceId: '', principal: 'internal-mesh', scopes: [], authMethod: 'api-key' };
      return;
    }
    // The only paths reachable without a credential. See UNAUTHENTICATED_PATHS
    // for why the list is this short.
    const path = req.url.split('?')[0]!;
    if (UNAUTHENTICATED_PATHS.has(path)) {
      req.auth = {
        workspaceId: '',
        principal: 'unauthenticated',
        scopes: [],
        authMethod: 'api-key',
      };
      return;
    }
    // /v1/git-creds: public route with custom Bearer-JWT auth.
    // The in-sandbox `orca-git-creds` helper has only the session-scoped
    // JWT — no api-key, no OIDC token. The route's own handler verifies the
    // JWT (audience='git-creds') and 401s on failure. We bypass the global
    // pre-handler here so the api-key check doesn't 401 first and mask the
    // JWT-specific error.
    if (path === '/v1/git-creds') {
      req.auth = {
        workspaceId: '',
        principal: 'git-creds-helper',
        scopes: [],
        authMethod: 'api-key',
      };
      return;
    }
    // These two read-only smart HTTP operations verify their resource-scoped
    // git-proxy JWT themselves. A receive-pack/write path gets no exemption.
    if (
      (req.method === 'GET' && /^\/v1\/git-proxy\/[^/]+\/info\/refs$/.test(path)) ||
      (req.method === 'POST' && /^\/v1\/git-proxy\/[^/]+\/git-upload-pack$/.test(path))
    ) {
      req.auth = { workspaceId: '', principal: 'git-proxy', scopes: [], authMethod: 'api-key' };
      return;
    }
    // /v1/tunnels/*: the public tunnel WebSocket endpoints, which self-authenticate
    // in their own handlers. Both the runner tunnel and the environment-worker
    // (worker) tunnel live here, because both peers dial in from OUTSIDE the mesh (a
    // laptop / customer VM, outbound WSS) and so cannot reach `/internal/*`:
    //   - `/v1/tunnels/runners/:runnerId` — the runner presents its tunnel binding
    //     token in the `X-Orca-Runner-Tunnel-Token` handshake header; the WS
    //     handler verifies that credential itself (token-binding correlation, the
    //     owner fail-closed gate, and the CSWSH origin guard — see
    //     `runner-tunnel.routes.ts`).
    //   - `/v1/tunnels/environments/:environmentId` — the worker presents its raw
    //     Env Key in the `X-Orca-Environment-Key` handshake header; the WS handler
    //     verifies it against the environment row's stored digest + expiry (inline
    //     via `verifyEnvKeyForEnvironment`, before the tunnel engine runs) and
    //     applies the same CSWSH origin guard — see `worker-tunnel.routes.ts` and
    //     `docs/managed-agents/services/registry-service.md` ("Environment
    //     worker-tunnel auth + durable claims").
    // Neither endpoint carries an api-key / OIDC token; the Env-Key / binding-token
    // check IS the auth for these public paths. Bypass the global pre-handler here
    // so the api-key / OIDC check doesn't 401 the upgrade before the handler's own
    // credential check runs (mirrors the git-creds bypass above).
    if (path.startsWith('/v1/tunnels/')) {
      req.auth = {
        workspaceId: '',
        principal: 'tunnel-endpoint',
        scopes: [],
        authMethod: 'api-key',
      };
      return;
    }

    // Declared without an initialiser on purpose, the way `internal-auth.ts`
    // declares its principal: there is no safe default here, so the catch below
    // is forced to state a verdict rather than silently leave a permissive one.
    let apiKeyResult: ApiKeyAuthResult;
    let legacyFallbackRateLimit: LegacyApiKeyFallbackRateLimitError | null = null;
    try {
      apiKeyResult = await apiKey(req);
    } catch (error) {
      if (!(error instanceof LegacyApiKeyFallbackRateLimitError)) throw error;
      legacyFallbackRateLimit = error;
      // The throw means we could not *finish* validating a key the caller
      // presented — an infrastructure condition, not "no key was presented".
      // Treating it as absent let OIDC rescue the request: two throwaway
      // requests exhaust the per-source limiter (keyed on the attacker's own
      // `req.ip`), and from the third onward a bad `x-api-key` plus a valid
      // Bearer authenticated as the Bearer's workspace — `401, 401, 200, 200,
      // 200`. `internal-auth.ts` models the rule this follows: "the check could
      // not run" must deny, kept separate from "the check failed". Recording
      // `rejected` denies here, and the reply below still makes it a 429 with
      // its retry-after rather than a bare 401.
      apiKeyResult = { outcome: 'rejected' };
    }

    // OIDC runs only when no api key was presented. `rejected` means the caller
    // explicitly supplied one and it did not authenticate; falling through
    // there is what let a valid Bearer token — which a gateway, proxy or
    // wrapper may have injected without the caller's knowledge — rescue a bad
    // `x-api-key` and authenticate as a workspace the caller never named.
    let principal: AuthenticatedPrincipal | null = null;
    if (apiKeyResult.outcome === 'authenticated') {
      principal = apiKeyResult.principal;
    } else if (apiKeyResult.outcome === 'absent') {
      principal = await oidc(req);
    }
    if (!principal) {
      if (legacyFallbackRateLimit) {
        reply.header('retry-after', String(legacyFallbackRateLimit.retryAfterSeconds));
        reply.code(429).send({ error: legacyFallbackRateLimit.message });
        return reply;
      }
      reply.code(401).send({ error: 'unauthenticated' });
      return reply;
    }
    const workspace = await measureAuthStage('workspace_lookup', () =>
      opts.db.query.workspaces.findFirst({
        where: eq(workspaces.id, principal.workspaceId),
      }),
    );
    if (!workspace || workspace.status !== 'active') {
      reply.code(401).send({ error: 'unauthenticated' });
      return reply;
    }
    req.auth = principal;
  };
}
