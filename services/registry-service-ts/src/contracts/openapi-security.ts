// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * What the public listener actually requires, transcribed from `src/auth/`.
 *
 * Omitting this was a claim, not the absence of one: OpenAPI reads a document
 * with no `security` as "these operations need no authentication", and a client
 * generated from it has nowhere to put an API key. The earlier objection — that
 * inventing per-operation security would be a claim about the server rather than
 * a record of it — is answered by not inventing any. Every line below cites the
 * code that enforces it, and the exception set is pinned by a test rather than
 * maintained by hand.
 *
 * The published `/healthz` and `/readyz` probes carry an empty operation-level
 * requirement to clear this default. `/metrics`, which `src/auth/auth.ts` also
 * leaves unauthenticated, is not part of the published contract. The admin
 * listener is a separate surface.
 */

/** An OpenAPI security requirement: satisfy any one entry in the list. */
export type SecurityRequirement = Record<string, string[]>;

export const SECURITY_SCHEMES = {
  apiKey: {
    type: 'apiKey',
    in: 'header',
    name: 'x-api-key',
    description:
      'Workspace API key. Read from the `x-api-key` header by `src/auth/api-key.ts`. A key ' +
      'that is present and does not authenticate is final — no other scheme is consulted.',
  },
  oidcBearer: {
    type: 'http',
    scheme: 'bearer',
    bearerFormat: 'JWT',
    description:
      'OIDC access token, verified by `src/auth/oidc.ts`. Consulted only when no `x-api-key` ' +
      'was presented at all.',
  },
  gitCredsJwt: {
    type: 'http',
    scheme: 'bearer',
    bearerFormat: 'JWT',
    description:
      'Session-scoped JWT with audience `git-creds`, held only by the in-sandbox ' +
      '`orca-git-creds` helper and verified by the route handler itself.',
  },
  gitProxyJwt: {
    type: 'http',
    scheme: 'bearer',
    bearerFormat: 'JWT',
    description:
      'Short-lived JWT with audience `git-proxy`, bound to one active session resource, repository and credential revision. Authorizes Git reads only; never resolves raw credentials.',
  },
} as const;

/**
 * The default for every published operation.
 *
 * A list of requirements is an OR: either scheme authenticates the caller, which
 * is what `src/auth/auth.ts` does — the API key is tried first and OIDC runs
 * only when none was presented.
 */
export const DEFAULT_SECURITY: SecurityRequirement[] = [{ apiKey: [] }, { oidcBearer: [] }];

/** Marker key. Namespaced so unrelated route metadata can coexist. */
export interface WithOpenApiSecurity {
  openApiSecurity: SecurityRequirement[];
}

/**
 * Override {@link DEFAULT_SECURITY} for one route.
 *
 * Declared on the route rather than in a table keyed by path, for the same
 * reason the media overrides are: delete the route and the exception goes with
 * it. `openapi-document.spec.ts` asserts the exact set of operations carrying
 * an override, so a second exception has to be decided about rather than
 * absorbed.
 */
export function openApiSecurity(requirements: SecurityRequirement[]): WithOpenApiSecurity {
  return { openApiSecurity: requirements };
}

/** Read the override back off a route, if it declared one. */
export function readOpenApiSecurity(metadata: unknown): SecurityRequirement[] | undefined {
  if (!metadata || typeof metadata !== 'object') return undefined;
  const security = (metadata as Partial<WithOpenApiSecurity>).openApiSecurity;
  return Array.isArray(security) ? security : undefined;
}
