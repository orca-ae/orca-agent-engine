// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0
import { and, eq, sql } from 'drizzle-orm';
import type { DbTransaction } from '../persistence/postgres/client.js';
import { guardrailPolicyHeads as heads } from '../persistence/postgres/schema.js';
/** Workspace serialization also covers shared budget subjects whose Vault identities differ. */
export async function lockPolicyStateRevision(tx: DbTransaction, workspaceId: string) {
  await tx
    .insert(heads)
    .values({ workspaceId, scope: 'state', subject: '*' })
    .onConflictDoNothing();
  const [row] = await tx
    .select()
    .from(heads)
    .where(
      and(eq(heads.workspaceId, workspaceId), eq(heads.scope, 'state'), eq(heads.subject, '*')),
    )
    .for('update');
  return row!.head;
}
export async function bumpPolicyStateRevision(tx: DbTransaction, workspaceId: string) {
  await tx
    .insert(heads)
    .values({ workspaceId, scope: 'state', subject: '*', head: 1n })
    .onConflictDoUpdate({
      target: [heads.workspaceId, heads.scope, heads.subject],
      set: { head: sql`${heads.head}+1` },
    });
}
