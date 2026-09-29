// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { and, eq, isNull, ne } from 'drizzle-orm';
import type { GitProxyScope, SessionJwtMinter } from '../auth/session-jwt.js';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  environments,
  gitCredentials,
  sessionResources,
  sessions,
  workspaces,
} from '../persistence/postgres/schema.js';
import { validateGitRepositoryUrl } from './git-credentials.js';

export const GIT_PROXY_TTL_SECONDS = 900;

export interface GitProxyBinding {
  organizationId: string;
  scope: GitProxyScope;
  secretRef: string;
}

/** Reload authority before minting and on every read; never trust a snapshot as authorization. */
export async function loadGitProxyBinding(
  db: DbClient,
  input: { workspaceId: string; sessionId: string; resourceId: string },
): Promise<GitProxyBinding | null> {
  const [binding] = await db
    .select({
      repoRef: sessionResources.repoRef,
      organizationId: workspaces.organizationId,
      environmentId: sessions.environmentId,
      networking: environments.networking,
    })
    .from(sessionResources)
    .innerJoin(
      sessions,
      and(
        eq(sessions.id, sessionResources.sessionId),
        eq(sessions.workspaceId, sessionResources.workspaceId),
      ),
    )
    .innerJoin(workspaces, eq(workspaces.id, sessions.workspaceId))
    .leftJoin(
      environments,
      and(
        eq(environments.id, sessions.environmentId),
        eq(environments.workspaceId, sessions.workspaceId),
      ),
    )
    .where(
      and(
        eq(sessionResources.id, input.resourceId),
        eq(sessionResources.sessionId, input.sessionId),
        eq(sessionResources.workspaceId, input.workspaceId),
        eq(sessionResources.type, 'github_repository'),
        isNull(sessionResources.deletedAt),
        isNull(sessionResources.detachedAt),
        isNull(sessions.deletedAt),
        isNull(sessions.archivedAt),
        ne(sessions.status, 'terminated'),
        eq(workspaces.status, 'active'),
        isNull(workspaces.archivedAt),
      ),
    )
    .limit(1);
  const repo = binding?.repoRef as { url?: unknown; git_credential_id?: unknown } | undefined;
  if (
    !binding ||
    typeof repo?.url !== 'string' ||
    typeof repo.git_credential_id !== 'string' ||
    validateGitRepositoryUrl(repo.url)
  )
    return null;
  // A scoped read token must not turn the implicitly reachable Registry into
  // an egress bypass. Even valid Git fields can encode data for a hostile host.
  const networking = binding.networking as { type?: unknown; allowed_hosts?: unknown } | null;
  if (binding.environmentId && !networking) return null;
  const repositoryHost = new URL(repo.url).hostname.toLowerCase();
  if (networking?.type !== undefined && networking.type !== 'unrestricted') {
    if (
      networking.type !== 'limited' ||
      !Array.isArray(networking.allowed_hosts) ||
      !networking.allowed_hosts.some(
        (host) => typeof host === 'string' && host.toLowerCase() === repositoryHost,
      )
    )
      return null;
  }
  const [credential] = await db
    .select()
    .from(gitCredentials)
    .where(
      and(
        eq(gitCredentials.id, repo.git_credential_id),
        eq(gitCredentials.workspaceId, input.workspaceId),
        isNull(gitCredentials.deletedAt),
        isNull(gitCredentials.archivedAt),
      ),
    )
    .limit(1);
  if (
    !credential ||
    validateGitRepositoryUrl(credential.repoUrl) ||
    canonicalRepositoryUrl(credential.repoUrl) !== canonicalRepositoryUrl(repo.url) ||
    (credential.sessionResourceId !== null && credential.sessionResourceId !== input.resourceId)
  )
    return null;
  return {
    organizationId: binding.organizationId,
    secretRef: credential.secretRef,
    scope: {
      resourceId: input.resourceId,
      repoUrl: new URL(repo.url).href.replace(/\/+$/, ''),
      credentialId: credential.id,
      // A same-ID update revokes old capabilities too, without exposing the secret reference.
      credentialRevision: createHash('sha256')
        .update(JSON.stringify([credential.secretRef, credential.updatedAt.toISOString()]))
        .digest('hex'),
    },
  };
}

function canonicalRepositoryUrl(value: string): string {
  // Only the final repository component has Git's optional .git suffix. Do
  // not normalize owner names or intermediate path segments into another repo.
  return new URL(value).href.replace(/\/+$/, '').replace(/\.git$/, '');
}

export async function mintGitProxyCapability(
  db: DbClient,
  minter: SessionJwtMinter,
  input: { registryBaseUrl: string; workspaceId: string; sessionId: string; resourceId: string },
): Promise<{ remoteUrl: string; authorizationHeader: string; expiresAt: number }> {
  const base = new URL(input.registryBaseUrl);
  if (
    !['https:', 'http:'].includes(base.protocol) ||
    base.username ||
    base.password ||
    base.search ||
    base.hash
  )
    throw new Error('invalid Git proxy Registry URL');
  const binding = await loadGitProxyBinding(db, input);
  if (!binding) throw new Error('attached Git resource is unavailable');
  const minted = await minter.mint(
    {
      org_id: binding.organizationId,
      workspace_id: input.workspaceId,
      session_id: input.sessionId,
      mcp_server_names: [],
      vault_ids: [],
      credential_ids: [],
    },
    { audience: 'git-proxy', ttlSecs: GIT_PROXY_TTL_SECONDS, gitProxy: binding.scope },
  );
  return {
    remoteUrl: `${base.origin}/v1/git-proxy/${encodeURIComponent(input.resourceId)}`,
    authorizationHeader: `Authorization: Bearer ${minted.token}`,
    expiresAt: minted.expiresAt,
  };
}
