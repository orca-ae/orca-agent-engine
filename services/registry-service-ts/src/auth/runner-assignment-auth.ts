// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { eq } from 'drizzle-orm';
import type { RunnerTunnelRouteOptions } from '../api/runner-tunnel.routes.js';
import type { DbClient } from '../persistence/postgres/client.js';
import { environments, sessions, workspaces } from '../persistence/postgres/schema.js';

/** The persisted runner id is the verifier for the distributor's random binding token. */
export function buildRunnerAssignmentAuth(
  db: DbClient,
): NonNullable<RunnerTunnelRouteOptions['authorizeAssignedRunner']> {
  return (runnerId, accept) =>
    db.transaction(async (tx) => {
      // Count before filtering: an ambiguous id must never select an arbitrary tenant.
      const rows = await tx
        .select({
          workspaceId: sessions.workspaceId,
          environmentId: sessions.environmentId,
          hostEnvironmentId: sessions.hostEnvironmentId,
          status: sessions.status,
          deletedAt: sessions.deletedAt,
          archivedAt: sessions.archivedAt,
        })
        .from(sessions)
        .where(eq(sessions.runnerId, runnerId))
        .limit(2)
        .for('share');
      if (rows.length === 0) return 'unassigned';
      const session = rows[0]!;
      if (
        rows.length !== 1 ||
        session.deletedAt !== null ||
        session.archivedAt !== null ||
        session.status === 'terminated' ||
        session.environmentId === null ||
        session.hostEnvironmentId !== session.environmentId
      )
        return 'rejected';

      // Archive locks Workspace before Session; the Session lock already orders admission.
      // Read Workspace without a row lock, as native completion does, to avoid a reverse wait.
      const [workspace] = await tx
        .select({ status: workspaces.status })
        .from(workspaces)
        .where(eq(workspaces.id, session.workspaceId));
      if (workspace?.status !== 'active') return 'rejected';
      const [environment] = await tx
        .select({
          workspaceId: environments.workspaceId,
          deletedAt: environments.deletedAt,
          archivedAt: environments.archivedAt,
        })
        .from(environments)
        .where(eq(environments.id, session.environmentId))
        .for('share');
      if (
        !environment ||
        environment.workspaceId !== session.workspaceId ||
        environment.deletedAt !== null ||
        environment.archivedAt !== null
      )
        return 'rejected';

      // Synchronous registration under the locks orders admission against revocation.
      // Distribution/turn lifecycle is not credential lifetime; the current binding is.
      accept(session.workspaceId);
      return 'accepted';
    });
}
