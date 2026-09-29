// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyReply, FastifyRequest } from 'fastify';
import type { DbClient } from '../persistence/postgres/client.js';
import { buildPlatformApiKeyAuth } from './platform-api-key.js';
import type { PlatformPrincipal } from './principal.js';
import { buildPlatformOidcAuth, type OidcConfig } from './oidc.js';

export function buildPlatformAuth(opts: { db: DbClient; oidc: OidcConfig }) {
  const apiKey = buildPlatformApiKeyAuth(opts.db);
  const oidc = buildPlatformOidcAuth(opts.oidc);

  return async function platformAuth(req: FastifyRequest, reply: FastifyReply): Promise<void> {
    // Platform OIDC runs only when no `x-api-key` was presented. `rejected`
    // means the caller supplied one and it did not authenticate; falling
    // through there let a valid platform Bearer rescue a bad platform key.
    const apiKeyResult = await apiKey(req);
    let principal: PlatformPrincipal | null = null;
    if (apiKeyResult.outcome === 'authenticated') {
      principal = apiKeyResult.principal;
    } else if (apiKeyResult.outcome === 'absent') {
      principal = await oidc(req);
    }
    if (!principal || !principal.scopes.includes('platform:admin')) {
      reply.code(401).send({ error: 'unauthenticated' });
      return reply;
    }
    req.platformAuth = principal;
  };
}
