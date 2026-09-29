// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { initContract } from '@ts-rest/core';
import { z } from 'zod';
import { ClaudeErrorResponse } from './common.js';
import { openApiSecurity } from './openapi-security.js';

const c = initContract();

/**
 * In-sandbox git credential helper contract.
 *
 * Called by `orca-git-creds` (the helper baked into the sandbox images) when
 * `git` invokes `credential.helper`. The helper sends the protocol/host/path
 * git is asking about; the registry resolves it against the session-scoped
 * JWT's `repo_urls` allowlist + the bound vault, and returns the PAT bytes.
 *
 * Auth: session-scoped JWT (`aud='git-creds'`). The route handler verifies
 * the JWT itself — `auth.ts` skips api-key/OIDC for this path because the
 * helper inside the sandbox only has the JWT.
 */
const GitCredsResponse = z.object({
  username: z.string(),
  password: z.string(),
});

export const gitCredsContract = c.router({
  resolveCreds: {
    method: 'POST',
    path: '/v1/git-creds',
    body: z.object({
      protocol: z.literal('https'),
      host: z.string().min(1).max(255),
      path: z.string().optional(),
    }),
    responses: {
      200: GitCredsResponse,
      400: ClaudeErrorResponse,
      401: ClaudeErrorResponse,
      403: ClaudeErrorResponse,
      404: ClaudeErrorResponse,
    },
    headers: z.object({ authorization: z.string() }).passthrough(),
    // The one route the public auth hook exempts: the helper inside the sandbox
    // holds neither an api key nor an OIDC token.
    metadata: openApiSecurity([{ gitCredsJwt: [] }]),
  },
});
