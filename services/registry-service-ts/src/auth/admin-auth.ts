// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { eq } from 'drizzle-orm';
import type { FastifyReply, FastifyRequest } from 'fastify';
import type { DbClient } from '../persistence/postgres/client.js';
import { organizations } from '../persistence/postgres/schema.js';
import { buildAdminApiKeyAuth } from './admin-api-key.js';
import type { AdminPrincipal } from './principal.js';
import { buildAdminOidcAuth, type OidcConfig } from './oidc.js';

export function buildAdminAuth(opts: { db: DbClient; oidc: OidcConfig }) {
  const apiKey = buildAdminApiKeyAuth(opts.db);
  const oidc = buildAdminOidcAuth(opts.oidc);

  return async function adminAuth(req: FastifyRequest, reply: FastifyReply): Promise<void> {
    const path = req.url.split('?')[0] ?? '';
    if (path === '/healthz' || path === '/readyz') return;

    // Admin OIDC runs only when no `x-api-key` was presented. `rejected` means
    // the caller supplied one and it did not authenticate; falling through
    // there is what let a valid admin Bearer rescue a bad admin key and
    // authenticate the request as an organization the caller never named.
    const apiKeyResult = await apiKey(req);
    let principal: AdminPrincipal | null = null;
    if (apiKeyResult.outcome === 'authenticated') {
      principal = apiKeyResult.principal;
    } else if (apiKeyResult.outcome === 'absent') {
      principal = await oidc(req);
    }
    if (!principal) {
      reply.code(401).send({ error: 'unauthenticated' });
      return reply;
    }
    if (principal.authMethod === 'oidc') {
      const organization = await opts.db.query.organizations.findFirst({
        where: eq(organizations.id, principal.organizationId),
      });
      if (!organization || organization.status !== 'active') {
        reply.code(401).send({ error: 'unauthenticated' });
        return reply;
      }
    }
    req.adminAuth = principal;
  };
}

export function requireAdminScope(
  req: FastifyRequest,
  reply: FastifyReply,
  required: string,
): boolean {
  const scopes = req.adminAuth?.scopes ?? [];
  if (scopes.includes('org:admin') || scopes.includes(required)) return true;
  reply.code(403).send({ error: `missing required scope: ${required}` });
  return false;
}
