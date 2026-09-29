// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, asc, eq, inArray, sql } from 'drizzle-orm';
import type { SkillStore } from '@orca/skill-store';
import type { DbClient } from '../persistence/postgres/client.js';
import { skillBundleDeletionOutbox, skillVersions } from '../persistence/postgres/schema.js';

export const SKILL_BUNDLE_DELETION_RECONCILE_INTERVAL_MS = 5_000;

export interface SkillBundleDeletionReconcileResult {
  processed: number;
  deleted: number;
  failed: number;
}

/**
 * Serialize metadata ownership decisions and object deletion for one immutable
 * bundle identity. The lock is transaction-scoped, so a reconciler that races
 * an unacknowledged COMMIT waits until Postgres has resolved that transaction
 * before it checks for the exact SkillVersion row.
 */
export async function acquireSkillBundleLifecycleLock(
  db: Pick<DbClient, 'execute'>,
  workspaceId: string,
  skillVersionId: string,
): Promise<void> {
  const lockIdentity = `${workspaceId.length}:${workspaceId}${skillVersionId}`;
  await db.execute(sql`select pg_advisory_xact_lock(hashtextextended(${lockIdentity}, 0))`);
}

/**
 * Delete pending immutable Skill bundles in bounded batches.
 *
 * A candidate stays in Postgres until SkillStore confirms deletion. The row is
 * locked across the object-store call so multiple Registry replicas serialize
 * work for the same bundle. SkillStore deletion is idempotent, closing the
 * crash window between deleting the object and deleting the outbox row.
 */
export async function reconcileSkillBundleDeletionOutbox(
  db: DbClient,
  skillStore: SkillStore,
  options: { skillVersionIds?: string[]; batchSize?: number; now?: Date } = {},
): Promise<SkillBundleDeletionReconcileResult> {
  const result: SkillBundleDeletionReconcileResult = {
    processed: 0,
    deleted: 0,
    failed: 0,
  };
  if (options.skillVersionIds?.length === 0) return result;

  const rows = await db
    .select({
      workspaceId: skillBundleDeletionOutbox.workspaceId,
      skillVersionId: skillBundleDeletionOutbox.skillVersionId,
      packageSha256: skillBundleDeletionOutbox.packageSha256,
    })
    .from(skillBundleDeletionOutbox)
    .where(
      options.skillVersionIds
        ? inArray(skillBundleDeletionOutbox.skillVersionId, options.skillVersionIds)
        : undefined,
    )
    .orderBy(asc(skillBundleDeletionOutbox.createdAt))
    .limit(options.batchSize ?? 100);

  for (const candidate of rows) {
    const attemptedAt = options.now ?? new Date();
    const outcome = await db.transaction(async (tx) => {
      const lockedRows = await tx
        .select({
          workspaceId: skillBundleDeletionOutbox.workspaceId,
          skillVersionId: skillBundleDeletionOutbox.skillVersionId,
          packageSha256: skillBundleDeletionOutbox.packageSha256,
        })
        .from(skillBundleDeletionOutbox)
        .where(
          and(
            eq(skillBundleDeletionOutbox.workspaceId, candidate.workspaceId),
            eq(skillBundleDeletionOutbox.skillVersionId, candidate.skillVersionId),
            eq(skillBundleDeletionOutbox.packageSha256, candidate.packageSha256),
          ),
        )
        .for('update')
        .limit(1);
      const row = lockedRows[0];
      if (!row) return 'skipped' as const;

      await acquireSkillBundleLifecycleLock(tx, row.workspaceId, row.skillVersionId);

      // A metadata COMMIT can succeed even when the client loses its
      // acknowledgement and enters the create failure path. Treat an exact
      // SkillVersion reference as authoritative: clear the stale cleanup
      // request, but never delete a bundle that committed metadata still owns.
      const referencedRows = await tx
        .select({ id: skillVersions.id })
        .from(skillVersions)
        .where(
          and(
            eq(skillVersions.workspaceId, row.workspaceId),
            eq(skillVersions.id, row.skillVersionId),
            eq(skillVersions.packageSha256, row.packageSha256),
          ),
        )
        .limit(1);
      if (referencedRows[0]) {
        await tx
          .delete(skillBundleDeletionOutbox)
          .where(
            and(
              eq(skillBundleDeletionOutbox.workspaceId, row.workspaceId),
              eq(skillBundleDeletionOutbox.skillVersionId, row.skillVersionId),
              eq(skillBundleDeletionOutbox.packageSha256, row.packageSha256),
            ),
          );
        return 'skipped' as const;
      }

      try {
        await skillStore.delete(row.workspaceId, row.skillVersionId, row.packageSha256);
        await tx
          .delete(skillBundleDeletionOutbox)
          .where(
            and(
              eq(skillBundleDeletionOutbox.workspaceId, row.workspaceId),
              eq(skillBundleDeletionOutbox.skillVersionId, row.skillVersionId),
              eq(skillBundleDeletionOutbox.packageSha256, row.packageSha256),
            ),
          );
        return 'deleted' as const;
      } catch {
        await tx
          .update(skillBundleDeletionOutbox)
          .set({
            attemptCount: sql`${skillBundleDeletionOutbox.attemptCount} + 1`,
            lastAttemptAt: attemptedAt,
          })
          .where(
            and(
              eq(skillBundleDeletionOutbox.workspaceId, row.workspaceId),
              eq(skillBundleDeletionOutbox.skillVersionId, row.skillVersionId),
              eq(skillBundleDeletionOutbox.packageSha256, row.packageSha256),
            ),
          );
        return 'failed' as const;
      }
    });

    if (outcome === 'skipped') continue;
    result.processed += 1;
    if (outcome === 'deleted') {
      result.deleted += 1;
    } else {
      result.failed += 1;
    }
  }

  return result;
}
