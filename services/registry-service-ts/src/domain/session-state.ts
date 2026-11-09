// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull, ne, sql } from 'drizzle-orm';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import { sessions } from '../persistence/postgres/schema.js';

export async function updateSessionState(
  db: DbClient | DbTransaction,
  workspaceId: string,
  id: string,
  body: { status: string; sandbox_handle_id?: string | null },
  now: Date,
): Promise<boolean> {
  const hasSandboxHandle = Object.prototype.hasOwnProperty.call(body, 'sandbox_handle_id');
  const update = {
    status: body.status,
    ...(hasSandboxHandle ? { sandboxHandleId: body.sandbox_handle_id } : {}),
    ...(body.status === 'running'
      ? {
          startedAt: sql`coalesce(${sessions.startedAt}, ${now})`,
          lastActiveAt: sql`case when ${sessions.status} <> 'running' then ${now} else ${sessions.lastActiveAt} end`,
        }
      : {
          activeSeconds: sql`${sessions.activeSeconds} + case when ${sessions.status} = 'running' and ${sessions.lastActiveAt} is not null then greatest(0, floor(extract(epoch from (cast(${now} as timestamptz) - ${sessions.lastActiveAt})))::int) else 0 end`,
          lastActiveAt: now,
        }),
    updatedAt: now,
  };
  const rows = await db
    .update(sessions)
    .set(update)
    .where(
      and(
        isNull(sessions.deletedAt),
        eq(sessions.workspaceId, workspaceId),
        eq(sessions.id, id),
        isNull(sessions.archivedAt),
        ne(sessions.status, 'terminated'),
      ),
    )
    .returning({ id: sessions.id });
  return rows.length > 0;
}
