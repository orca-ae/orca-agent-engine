// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The `/api/v1` → `/v1` alias, applied before routing.
 *
 * Core is canonical at `/v1`, not `/api/v1`. Kubernetes' `/api/v1` is a legacy
 * constraint its core group cannot escape; ours is pinned by the Claude contract
 * this service mirrors, and canonical `/v1` is what lets a Claude curl example
 * work here with only the host swapped. The alias exists so tooling built around
 * the Kubernetes shape can pair `/api/v1` with `/apis/<group>/<version>`.
 *
 * It is a URL rewrite rather than a second route tree because `rewriteUrl` is
 * the only Fastify hook that runs *before* routing, so rewriting here leaves
 * exactly one canonical path for every downstream `req.url` consumer — and
 * there are four, none of them touched by this file:
 *
 *  - `rejectExplicitWorkspaceSelector` (`server.ts`), which only inspects paths
 *    under `/v1/`. A second mount would let `POST /api/v1/agents` carry a
 *    `workspace_id` the server never authorized.
 *  - `isClaudeEnvelopePath` (`middleware/claude-edge.ts`), which decides
 *    whether a failure answers in Claude's envelope.
 *  - the idempotency scope key (`middleware/idempotency.ts`). A second mount
 *    would split one logical write across two scopes, so the same
 *    `idempotency-key` replayed against the alias would create a second
 *    resource.
 *  - the unauthenticated allowlist (`auth/auth.ts`).
 *
 * Registering every route twice would also double the surface a reader has to
 * keep consistent, and the conformance differ would see each operation twice.
 *
 * See `docs/managed-agents/api-groups-and-extensions.md`.
 */

const ALIAS_PREFIX = '/api/v1';

/**
 * Rewrite an alias URL to its canonical form, leaving everything else alone.
 *
 * Only `/api/v1` and `/api/v1/...` are rewritten. In particular:
 *
 * - `/api` is a *discovery route*, not a prefix to strip. Rewriting it to `''`
 *   or `/` would make the core-version probe answer something else entirely.
 * - `/apis`, `/apis/<group>/<version>/...` are the extension-group tree and are
 *   already canonical.
 * - `/api/v2/...` and `/api/v1beta/...` name versions this service does not
 *   serve; they must 404 as themselves rather than be silently downgraded to
 *   `/v1`.
 *
 * The query string is carried across untouched — it is not part of the prefix
 * being replaced.
 */
export function rewriteApiV1Path(url: string): string {
  const queryStart = url.indexOf('?');
  const path = queryStart === -1 ? url : url.slice(0, queryStart);
  const query = queryStart === -1 ? '' : url.slice(queryStart);

  if (path !== ALIAS_PREFIX && !path.startsWith(`${ALIAS_PREFIX}/`)) return url;

  return `${path.slice('/api'.length)}${query}`;
}

/**
 * Fastify `rewriteUrl` adapter.
 *
 * Fastify hands this the raw Node request and requires a string back; a request
 * with no URL is not something this alias can improve on, so it becomes `/` and
 * routing answers it as it would have anyway.
 */
export function rewriteApiV1Alias(req: { url?: string | undefined }): string {
  return rewriteApiV1Path(req.url ?? '/');
}
