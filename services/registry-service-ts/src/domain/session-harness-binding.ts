// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull } from 'drizzle-orm';
import {
  bindStoredHarness,
  resolveHarnessAnnotation,
  resolveExecutionOwner,
  type ExecutionOwner,
  type ResolvedHarness,
} from '@orca/harness-catalog';
import type { DbClient } from '../persistence/postgres/client.js';
import { agents, agentVersions, environments, sessions } from '../persistence/postgres/schema.js';

export class SessionHarnessBindingError extends Error {}

/** Read-only ownership remains available independently of execution preparation. */
export async function loadSessionExecutionOwner(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
): Promise<ExecutionOwner | null> {
  const [row] = await db
    .select({
      agentId: sessions.agentId,
      agentVersion: sessions.agentVersion,
      environmentId: sessions.environmentId,
      resolvedEnvironmentId: environments.id,
      target: environments.target,
    })
    .from(sessions)
    .leftJoin(
      environments,
      and(
        eq(environments.id, sessions.environmentId),
        eq(environments.workspaceId, sessions.workspaceId),
      ),
    )
    .where(
      and(
        eq(sessions.workspaceId, workspaceId),
        eq(sessions.id, sessionId),
        isNull(sessions.deletedAt),
      ),
    )
    .limit(1);
  if (!row) return null;
  if (row.environmentId && !row.resolvedEnvironmentId)
    throw new Error('session environment is unavailable');
  const selection = await loadSessionHarnessBinding(db, workspaceId, row.agentId, row.agentVersion);
  return resolveExecutionOwner(row.target ?? 'cloud', selection);
}

/** Routing survives archive/delete, but never substitutes the latest Agent version. */
export async function loadSessionHarnessBinding(
  db: DbClient,
  workspaceId: string,
  agentId: string,
  version: number,
): Promise<ResolvedHarness> {
  const [row] = await db
    .select({ harnessType: agents.harnessType, snapshot: agentVersions.snapshot })
    .from(agents)
    .innerJoin(
      agentVersions,
      and(
        eq(agentVersions.workspaceId, agents.workspaceId),
        eq(agentVersions.agentId, agents.id),
        eq(agentVersions.version, version),
      ),
    )
    .where(and(eq(agents.workspaceId, workspaceId), eq(agents.id, agentId)))
    .limit(1);
  if (!row || !row.snapshot || typeof row.snapshot !== 'object' || Array.isArray(row.snapshot)) {
    throw new SessionHarnessBindingError('session pinned harness binding is unavailable');
  }
  const snapshot = row.snapshot as Record<string, unknown>;
  if (
    snapshot.metadata != null &&
    (typeof snapshot.metadata !== 'object' || Array.isArray(snapshot.metadata))
  ) {
    throw new SessionHarnessBindingError('session pinned harness metadata is invalid');
  }
  try {
    const metadata = bindStoredHarness(
      snapshot.metadata as Record<string, unknown> | undefined,
      row.harnessType,
      snapshot.harness_type,
    );
    const selection = resolveHarnessAnnotation(metadata);
    if ('error' in selection) throw new SessionHarnessBindingError(selection.error);
    return selection;
  } catch (error) {
    throw new SessionHarnessBindingError('session pinned harness binding is invalid', {
      cause: error,
    });
  }
}
