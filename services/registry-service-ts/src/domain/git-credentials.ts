// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, asc, eq, inArray, isNull, lte, or } from 'drizzle-orm';
import type { DbClient } from '../persistence/postgres/client.js';
import { gitCredentials, gitCredentialStagingIntents } from '../persistence/postgres/schema.js';
import { newId } from './versioning.js';
import type { SecretStore } from '../secrets/secret-provider.js';

export const GIT_CREDENTIAL_REFERENCE_PREFIX = 'git_cred://';
export const GIT_CREDENTIAL_STAGING_GRACE_MS = 15 * 60 * 1_000;
export const GIT_CREDENTIAL_STAGING_CLEANING_RETRY_MS = 5 * 60 * 1_000;
export const GIT_CREDENTIAL_STAGING_RECONCILE_INTERVAL_MS = 60 * 1_000;

export interface ManagedGitCredentialDraft {
  id: string;
  workspaceId: string;
  sessionResourceId: string;
  repoUrl: string;
  secretRef: string;
  authorizationToken: string;
}

export interface PreparedGitCredentialBinding {
  gitCredentialId: string;
  managedCredential?: ManagedGitCredentialDraft;
}

/**
 * Resolve public `authorization_token` input to an internal git credential.
 *
 * Raw tokens are the Claude-compatible default. They become resource-owned
 * credential drafts whose bytes are staged in SecretStore before the DB
 * transaction. `git_cred://<id>` remains an Orca extension for pre-provisioned
 * credentials and never appears in public responses.
 */
export async function prepareGitCredentialBinding(
  db: DbClient,
  input: {
    workspaceId: string;
    sessionResourceId: string;
    repoUrl: string;
    authorizationToken: string | undefined;
  },
): Promise<PreparedGitCredentialBinding | { error: string }> {
  const repoUrlError = validateGitRepositoryUrl(input.repoUrl);
  if (repoUrlError) return { error: repoUrlError };

  const token = input.authorizationToken;
  if (typeof token !== 'string' || token.trim().length === 0) {
    return { error: 'github_repository resource missing authorization_token' };
  }

  if (token.startsWith(GIT_CREDENTIAL_REFERENCE_PREFIX)) {
    const gitCredentialId = token.slice(GIT_CREDENTIAL_REFERENCE_PREFIX.length);
    if (!/^gitcred_[A-Za-z0-9_-]+$/.test(gitCredentialId)) {
      return { error: 'authorization_token must contain a valid git_cred://<id> reference' };
    }

    const rows = await db
      .select()
      .from(gitCredentials)
      .where(
        and(
          isNull(gitCredentials.deletedAt),
          eq(gitCredentials.id, gitCredentialId),
          eq(gitCredentials.workspaceId, input.workspaceId),
        ),
      )
      .limit(1);
    const credential = rows[0];
    if (!credential) {
      return { error: `git credential ${gitCredentialId} not found in workspace` };
    }
    if (credential.archivedAt !== null) {
      return { error: `git credential ${gitCredentialId} is archived` };
    }
    if (
      credential.sessionResourceId !== null &&
      credential.sessionResourceId !== input.sessionResourceId
    ) {
      return { error: `git credential ${gitCredentialId} belongs to another session resource` };
    }
    if (!urlMatches(credential.repoUrl, input.repoUrl)) {
      return {
        error: `git credential repo_url ${credential.repoUrl} does not match resource.url ${input.repoUrl}`,
      };
    }
    return { gitCredentialId };
  }

  const gitCredentialId = newId('gitcred');
  return {
    gitCredentialId,
    managedCredential: {
      id: gitCredentialId,
      workspaceId: input.workspaceId,
      sessionResourceId: input.sessionResourceId,
      repoUrl: input.repoUrl,
      secretRef: managedGitCredentialSecretRef(input.workspaceId, gitCredentialId),
      authorizationToken: token,
    },
  };
}

export function managedGitCredentialSecretRef(workspaceId: string, credentialId: string): string {
  // `local:` is an opaque-reference namespace, not a backend selector. The
  // configured SecretStore owns the bytes behind this stable reference.
  return `local:git_credentials/${workspaceId}/${credentialId}/authorization_token`;
}

export function validateGitRepositoryUrl(repoUrl: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(repoUrl);
  } catch {
    return 'github_repository url must be a valid repository URL';
  }
  if (parsed.protocol !== 'https:') {
    return 'github_repository url must use https';
  }
  if (parsed.username || parsed.password || parsed.search || parsed.hash) {
    return 'github_repository url must not contain credentials, query parameters, or fragments';
  }
  if (normalizedRepositoryPath(parsed.pathname).length < 2) {
    return 'github_repository url must include owner and repository path segments';
  }
  return null;
}

/**
 * Compare validated repository URLs by origin + canonical repository path.
 * A trailing slash and the conventional `.git` suffix do not identify a
 * different repository.
 */
export function repositoryUrlsEqual(leftUrl: string, rightUrl: string): boolean {
  let left: URL;
  let right: URL;
  try {
    left = new URL(leftUrl);
    right = new URL(rightUrl);
  } catch {
    return false;
  }
  if (left.origin !== right.origin) return false;

  const leftPath = normalizedRepositoryPath(left.pathname);
  const rightPath = normalizedRepositoryPath(right.pathname);
  if (leftPath.length < 2 || rightPath.length < 2) return false;
  return leftPath.join('/') === rightPath.join('/');
}

export async function stageManagedGitCredentials(
  secretStore: SecretStore,
  drafts: ManagedGitCredentialDraft[],
): Promise<string[]> {
  const attemptedRefs: string[] = [];
  try {
    for (const draft of drafts) {
      attemptedRefs.push(draft.secretRef);
      await secretStore.put(draft.secretRef, draft.authorizationToken);
    }
    return attemptedRefs;
  } catch (error) {
    await purgeGitCredentialSecretRefs(secretStore, attemptedRefs).catch(() => undefined);
    throw error;
  }
}

/**
 * Persist recoverable intent before writing any raw token bytes to SecretStore.
 * Successful session-resource transactions consume these rows atomically with
 * their git_credentials inserts. A process crash leaves enough durable data
 * for the staging reconciler to purge the unreferenced secret later.
 */
export async function persistGitCredentialStagingIntents(
  db: DbClient,
  drafts: ManagedGitCredentialDraft[],
  options: { now?: Date; graceMs?: number } = {},
): Promise<void> {
  if (drafts.length === 0) return;
  const now = options.now ?? new Date();
  const cleanupAfter = new Date(
    now.getTime() + (options.graceMs ?? GIT_CREDENTIAL_STAGING_GRACE_MS),
  );
  await db.insert(gitCredentialStagingIntents).values(
    drafts.map((draft) => ({
      credentialId: draft.id,
      workspaceId: draft.workspaceId,
      sessionResourceId: draft.sessionResourceId,
      secretRef: draft.secretRef,
      status: 'pending',
      cleanupAfter,
      createdAt: now,
      updatedAt: now,
    })),
  );
}

/**
 * Claim request-owned pending intents before compensating their SecretStore
 * writes. Claiming first prevents an ambiguous/late DB transaction commit from
 * creating credential metadata after the secret has been purged.
 *
 * If cleanup fails, the durable `cleaning` row remains for the reconciler.
 */
export async function discardGitCredentialStagingIntents(
  db: DbClient,
  secretStore: SecretStore,
  drafts: ManagedGitCredentialDraft[],
  now = new Date(),
): Promise<void> {
  if (drafts.length === 0) return;
  const idsByWorkspace = new Map<string, Set<string>>();
  for (const draft of drafts) {
    const ids = idsByWorkspace.get(draft.workspaceId) ?? new Set<string>();
    ids.add(draft.id);
    idsByWorkspace.set(draft.workspaceId, ids);
  }
  const claimed: Array<{ workspaceId: string; credentialId: string; secretRef: string }> = [];
  for (const [workspaceId, ids] of idsByWorkspace) {
    claimed.push(
      ...(await db
        .update(gitCredentialStagingIntents)
        .set({ status: 'cleaning', updatedAt: now })
        .where(
          and(
            eq(gitCredentialStagingIntents.workspaceId, workspaceId),
            inArray(gitCredentialStagingIntents.credentialId, [...ids]),
            eq(gitCredentialStagingIntents.status, 'pending'),
          ),
        )
        .returning({
          workspaceId: gitCredentialStagingIntents.workspaceId,
          credentialId: gitCredentialStagingIntents.credentialId,
          secretRef: gitCredentialStagingIntents.secretRef,
        })),
    );
  }
  if (claimed.length === 0) return;

  await purgeGitCredentialSecretRefs(
    secretStore,
    claimed.map((row) => row.secretRef),
  );
  await Promise.all(
    claimed.map((row) =>
      db
        .delete(gitCredentialStagingIntents)
        .where(
          and(
            eq(gitCredentialStagingIntents.workspaceId, row.workspaceId),
            eq(gitCredentialStagingIntents.credentialId, row.credentialId),
            eq(gitCredentialStagingIntents.status, 'cleaning'),
          ),
        ),
    ),
  );
}

export interface GitCredentialStagingReconcileResult {
  processed: number;
  purged: number;
  preserved: number;
  failed: number;
}

/**
 * Reconcile expired staging intents in bounded batches.
 *
 * Claim transitions commit before external SecretStore deletion. Final
 * session-resource transactions consume only `pending` rows, so once an intent
 * becomes `cleaning` no late commit can create metadata pointing at a purged
 * secret. Stale `cleaning` rows are retried after a lease-like delay.
 */
export async function reconcileGitCredentialStagingIntents(
  db: DbClient,
  secretStore: SecretStore,
  options: { now?: Date; batchSize?: number; cleaningRetryMs?: number } = {},
): Promise<GitCredentialStagingReconcileResult> {
  const now = options.now ?? new Date();
  const batchSize = options.batchSize ?? 100;
  const cleaningRetryBefore = new Date(
    now.getTime() - (options.cleaningRetryMs ?? GIT_CREDENTIAL_STAGING_CLEANING_RETRY_MS),
  );
  const candidates = await db
    .select({
      workspaceId: gitCredentialStagingIntents.workspaceId,
      credentialId: gitCredentialStagingIntents.credentialId,
    })
    .from(gitCredentialStagingIntents)
    .where(
      or(
        and(
          eq(gitCredentialStagingIntents.status, 'pending'),
          lte(gitCredentialStagingIntents.cleanupAfter, now),
        ),
        and(
          eq(gitCredentialStagingIntents.status, 'cleaning'),
          lte(gitCredentialStagingIntents.updatedAt, cleaningRetryBefore),
        ),
      ),
    )
    .orderBy(asc(gitCredentialStagingIntents.cleanupAfter))
    .limit(batchSize);

  const result: GitCredentialStagingReconcileResult = {
    processed: 0,
    purged: 0,
    preserved: 0,
    failed: 0,
  };

  for (const candidate of candidates) {
    const claimed = await db.transaction(async (tx) => {
      const rows = await tx
        .select()
        .from(gitCredentialStagingIntents)
        .where(
          and(
            eq(gitCredentialStagingIntents.workspaceId, candidate.workspaceId),
            eq(gitCredentialStagingIntents.credentialId, candidate.credentialId),
          ),
        )
        .for('update', { skipLocked: true })
        .limit(1);
      const row = rows[0];
      if (!row) return null;

      const eligible =
        (row.status === 'pending' && row.cleanupAfter.getTime() <= now.getTime()) ||
        (row.status === 'cleaning' && row.updatedAt.getTime() <= cleaningRetryBefore.getTime());
      if (!eligible) return null;

      const credentialRows = await tx
        .select({ id: gitCredentials.id })
        .from(gitCredentials)
        .where(
          and(
            isNull(gitCredentials.deletedAt),
            eq(gitCredentials.workspaceId, row.workspaceId),
            eq(gitCredentials.id, row.credentialId),
            isNull(gitCredentials.archivedAt),
          ),
        )
        .limit(1);
      if (credentialRows[0]) {
        await tx
          .delete(gitCredentialStagingIntents)
          .where(
            and(
              eq(gitCredentialStagingIntents.workspaceId, row.workspaceId),
              eq(gitCredentialStagingIntents.credentialId, row.credentialId),
            ),
          );
        return { kind: 'preserved' as const };
      }

      await tx
        .update(gitCredentialStagingIntents)
        .set({ status: 'cleaning', updatedAt: now })
        .where(
          and(
            eq(gitCredentialStagingIntents.workspaceId, row.workspaceId),
            eq(gitCredentialStagingIntents.credentialId, row.credentialId),
          ),
        );
      return {
        kind: 'purge' as const,
        workspaceId: row.workspaceId,
        credentialId: row.credentialId,
        secretRef: row.secretRef,
      };
    });
    if (!claimed) continue;

    result.processed += 1;
    if (claimed.kind === 'preserved') {
      result.preserved += 1;
      continue;
    }

    try {
      await secretStore.delete(claimed.secretRef);
      await db
        .delete(gitCredentialStagingIntents)
        .where(
          and(
            eq(gitCredentialStagingIntents.workspaceId, claimed.workspaceId),
            eq(gitCredentialStagingIntents.credentialId, claimed.credentialId),
            eq(gitCredentialStagingIntents.status, 'cleaning'),
          ),
        );
      result.purged += 1;
    } catch {
      // Keep the claimed row. A later pass retries idempotent SecretStore
      // deletion after the cleaning lease expires.
      result.failed += 1;
    }
  }

  return result;
}

export async function purgeGitCredentialSecretRefs(
  secretStore: SecretStore | undefined,
  refs: Array<string | null | undefined>,
): Promise<void> {
  if (!secretStore) return;
  const uniqueRefs = [...new Set(refs.filter((ref): ref is string => Boolean(ref)))];
  const results = await Promise.allSettled(uniqueRefs.map((ref) => secretStore.delete(ref)));
  const failures: unknown[] = [];
  for (const [index, result] of results.entries()) {
    if (result.status === 'rejected')
      failures.push({ ref: uniqueRefs[index], cause: result.reason });
  }
  if (failures.length > 0) {
    throw new AggregateError(failures, 'failed to purge one or more git credential secrets');
  }
}

/**
 * Match the requested URL against a repository-bound credential URL.
 * Handles Git's `.git/info/refs` and `.git/git-receive-pack` request paths
 * without allowing a credential bound to one repository to reach a sibling.
 */
export function urlMatches(allowlistUrl: string, requestedUrl: string): boolean {
  let allowlist: URL;
  let requested: URL;
  try {
    allowlist = new URL(allowlistUrl);
    requested = new URL(requestedUrl);
  } catch {
    return false;
  }
  if (allowlist.protocol !== requested.protocol) return false;
  if (allowlist.host !== requested.host) return false;

  const allowlistSegments = normalizedRepositoryPath(allowlist.pathname);
  if (allowlistSegments.length < 2) return false;
  const allowlistPath = allowlistSegments.join('/');
  const requestedPath = normalizedRepositoryPath(requested.pathname).join('/');
  return requestedPath === allowlistPath || requestedPath.startsWith(`${allowlistPath}/`);
}

function normalizedRepositoryPath(path: string): string[] {
  return path
    .replace(/^\/+/, '')
    .replace(/\/+$/, '')
    .split('/')
    .map((segment) => segment.replace(/\.git$/, ''))
    .filter((segment) => segment.length > 0);
}
