// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance } from 'fastify';
import { eq, and, isNull, ne } from 'drizzle-orm';
import type { DbClient } from '../persistence/postgres/client.js';
import type { SecretProvider, SecretStore } from '../secrets/secret-provider.js';
import type { SessionJwtMinter } from '../auth/session-jwt.js';
import {
  gitCredentials,
  sessionResources,
  sessions,
  workspaces,
} from '../persistence/postgres/schema.js';
import { registryGitCredsRequestTotal } from '../metrics.js';
import { buildRuntimeSecretResolver } from '../secrets/runtime-resolver.js';
import { urlMatches } from '../domain/git-credentials.js';

export { urlMatches } from '../domain/git-credentials.js';

/**
 * `POST /v1/git-creds`. The in-sandbox `orca-git-creds` helper
 * (baked into the sandbox images) calls this every time `git` needs creds for
 * a remote. The session-scoped JWT (`aud='git-creds'`) carries the workspace
 * id, session id, and the `repo_urls[]` allowlist that the dispatcher pinned
 * when it minted the token at session-spawn. We resolve the matching
 * `github_repository` session resource → git credential → PAT and return it as
 * `{ username: 'x-access-token', password: <PAT> }`.
 *
 * Trust model:
 * - Auth is JWT-only (no api-key). `auth.ts` skips the api-key/OIDC pre-handler
 *   for `/v1/git-creds` so the helper can call us with just the bearer token.
 * - The audience MUST be `'git-creds'` so a leaked `'ai-gateway'` token
 *   cannot be replayed against this route.
 * - The requested repo URL MUST appear in the JWT's `repo_urls` claim AND in
 *   the session's persisted `github_repository` resources AND match the
 *   bound git credential's `repo_url`. Three checks defend against three
 *   attacks (token tampering, ResourceMutation post-spawn, credential re-binding).
 */

interface GitCredsBody {
  protocol?: string;
  host?: string;
  path?: string;
}

export interface GitCredsRouteDeps {
  db: DbClient;
  jwtMinter: SessionJwtMinter;
  secretProvider?: SecretProvider;
  secretStore?: SecretStore;
}

export function registerGitCredsRoutes(app: FastifyInstance, deps: GitCredsRouteDeps): void {
  const secrets = buildRuntimeSecretResolver(deps.secretProvider, deps.secretStore);
  app.post('/v1/git-creds', async (req, reply) => {
    // 1. Parse Bearer token. The helper sends `Authorization: Bearer <jwt>`;
    //    every other shape (missing header, non-bearer scheme, empty token)
    //    is a 401 — we don't want partial parsing to leak detail to a probe.
    const authHeader = req.headers['authorization'];
    if (typeof authHeader !== 'string' || !authHeader.startsWith('Bearer ')) {
      registryGitCredsRequestTotal.inc({ result: 'jwt_invalid' });
      return reply.code(401).send({ error: 'missing or malformed Authorization header' });
    }
    const token = authHeader.substring('Bearer '.length).trim();
    if (!token) {
      registryGitCredsRequestTotal.inc({ result: 'jwt_invalid' });
      return reply.code(401).send({ error: 'missing or malformed Authorization header' });
    }

    // 2. Verify JWT with audience='git-creds'. Any failure (signature, expiry,
    //    issuer, audience mismatch) collapses to 401 — the helper has no way
    //    to differentiate, and we don't expose verifier internals.
    let verified;
    try {
      verified = await deps.jwtMinter.verify(token, { expectedAudience: 'git-creds' });
    } catch (e) {
      registryGitCredsRequestTotal.inc({ result: 'jwt_invalid' });
      return reply.code(401).send({ error: `JWT verification failed: ${(e as Error).message}` });
    }

    const { workspaceId, sessionId, repoUrls } = verified;
    if (!workspaceId || !sessionId) {
      registryGitCredsRequestTotal.inc({ result: 'jwt_invalid' });
      return reply
        .code(401)
        .send({ error: 'JWT missing required claims (workspace_id, session_id)' });
    }

    // 3. Validate body. The git credential-helper protocol is line-based
    //    (`protocol=https\nhost=github.com\n...`); the in-sandbox helper
    //    parses that and POSTs the JSON. Missing protocol/host or any
    //    non-HTTPS protocol = 400; PATs must never be released for plaintext
    //    Git remotes.
    const body = req.body as GitCredsBody;
    if (!body || typeof body.protocol !== 'string' || typeof body.host !== 'string') {
      registryGitCredsRequestTotal.inc({ result: 'jwt_invalid' });
      return reply.code(400).send({ error: 'protocol and host are required' });
    }
    if (body.protocol !== 'https') {
      registryGitCredsRequestTotal.inc({ result: 'repo_unmatched' });
      return reply.code(400).send({ error: 'protocol must be https' });
    }
    const protocol = body.protocol;
    const host = body.host;
    const pathPart = typeof body.path === 'string' ? body.path : '';
    // Build the canonical URL from helper input. The path may be absent
    // (some git callers don't send one); when present, normalize the
    // leading slash so `path` and `/path` resolve to the same URL.
    const requestedPath = pathPart ? (pathPart.startsWith('/') ? pathPart : '/' + pathPart) : '';
    const requestedUrl = `${protocol}://${host}${requestedPath}`;

    const activeSessionRows = await deps.db
      .select({ id: sessions.id })
      .from(sessions)
      .innerJoin(workspaces, eq(workspaces.id, sessions.workspaceId))
      .where(
        and(
          isNull(sessions.deletedAt),
          eq(sessions.id, sessionId),
          eq(sessions.workspaceId, workspaceId),
          isNull(sessions.archivedAt),
          ne(sessions.status, 'terminated'),
          eq(workspaces.status, 'active'),
        ),
      )
      .limit(1);
    if (!activeSessionRows[0]) {
      registryGitCredsRequestTotal.inc({ result: 'credential_mismatch' });
      return reply.code(404).send({ error: 'session is not active' });
    }

    // 4. Verify the requested URL is in the JWT's repo_urls allowlist. If
    //    the dispatcher did not pin this repo for this session, refuse —
    //    even if a github_repository row exists in the DB (defense in depth
    //    against a session-resource mutation racing the JWT).
    const allowed = (repoUrls ?? []).some((u) => urlMatches(u, requestedUrl));
    if (!allowed) {
      registryGitCredsRequestTotal.inc({ result: 'repo_unmatched' });
      return reply
        .code(404)
        .send({ error: `no github_repository resource matches ${requestedUrl}` });
    }

    // 5. Look up the matching session_resource. Drizzle's relational query
    //    API (`db.query.X.findFirst`) requires a `relations` setup that this
    //    schema does not declare, so use the plain `select().from()` form
    //    and filter the JSONB `repo_ref.url` in app code. The session
    //    typically has 1–2 github_repository rows, so the loop is cheap.
    const repoRows = await deps.db
      .select()
      .from(sessionResources)
      .where(
        and(
          isNull(sessionResources.deletedAt),
          eq(sessionResources.workspaceId, workspaceId),
          eq(sessionResources.sessionId, sessionId),
          eq(sessionResources.type, 'github_repository'),
          isNull(sessionResources.detachedAt),
        ),
      );
    const matchedRows = repoRows.filter((row) => {
      const ref = row.repoRef as { url?: string } | null;
      return typeof ref?.url === 'string' && urlMatches(ref.url, requestedUrl);
    });
    if (matchedRows.length === 0) {
      registryGitCredsRequestTotal.inc({ result: 'repo_unmatched' });
      return reply.code(404).send({ error: 'no matching github_repository session resource' });
    }
    if (matchedRows.length > 1) {
      registryGitCredsRequestTotal.inc({ result: 'credential_mismatch' });
      return reply
        .code(409)
        .send({ error: 'multiple github_repository session resources match the requested URL' });
    }
    const matchedRow = matchedRows[0]!;
    const refTyped = matchedRow.repoRef as { git_credential_id?: string; url: string };
    if (typeof refTyped.git_credential_id !== 'string' || !refTyped.git_credential_id) {
      registryGitCredsRequestTotal.inc({ result: 'credential_mismatch' });
      return reply.code(404).send({ error: 'session resource missing git_credential_id' });
    }

    const credentialRows = await deps.db
      .select()
      .from(gitCredentials)
      .where(
        and(
          isNull(gitCredentials.deletedAt),
          eq(gitCredentials.id, refTyped.git_credential_id),
          eq(gitCredentials.workspaceId, workspaceId),
        ),
      )
      .limit(1);
    const credential = credentialRows[0];
    if (!credential) {
      registryGitCredsRequestTotal.inc({ result: 'credential_mismatch' });
      return reply.code(404).send({ error: 'git credential not found in workspace' });
    }
    if (credential.archivedAt !== null) {
      registryGitCredsRequestTotal.inc({ result: 'credential_mismatch' });
      return reply.code(404).send({ error: 'git credential is archived' });
    }
    if (credential.sessionResourceId !== null && credential.sessionResourceId !== matchedRow.id) {
      registryGitCredsRequestTotal.inc({ result: 'credential_mismatch' });
      return reply.code(404).send({ error: 'git credential belongs to another session resource' });
    }
    if (!urlMatches(credential.repoUrl, requestedUrl)) {
      registryGitCredsRequestTotal.inc({ result: 'credential_mismatch' });
      return reply
        .code(403)
        .send({ error: 'git credential repo_url does not match the requested repo URL' });
    }

    const pat = await secrets.resolve(credential.secretRef);
    if (!pat) {
      registryGitCredsRequestTotal.inc({ result: 'pat_unresolvable' });
      return reply.code(404).send({ error: 'PAT not resolvable from git credential secret' });
    }

    // 8. Return creds. GitHub's PAT-as-password convention is
    //    `username='x-access-token', password=<PAT>`. The same shape works
    //    for fine-grained PATs and classic PATs.
    registryGitCredsRequestTotal.inc({ result: 'ok' });
    return reply.code(200).send({ username: 'x-access-token', password: pat });
  });
}
