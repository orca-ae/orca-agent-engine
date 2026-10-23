// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { and, count, eq, ne } from 'drizzle-orm';
import { AgentRuntimeSignalKind } from '@orca/agent-event-contract';
import { buildDb, type DbClient } from './persistence/postgres/client.js';
import { sessionEventsIndex } from './persistence/postgres/schema.js';
import {
  backfillUsageEventVisibility,
  type UsageVisibilityCursor,
} from './events/session-events-index.js';

/** Run after old writers are drained; rerunning resumes from committed visibility. */
export async function runUsageVisibilityBackfill(
  db: DbClient,
  options: { verifyOnly?: boolean; batchSize?: number } = {},
): Promise<number> {
  let total = 0;
  if (!options.verifyOnly) {
    let after: UsageVisibilityCursor | undefined;
    while (true) {
      const batch = await backfillUsageEventVisibility(db, options.batchSize ?? 500, after);
      total += batch.changed;
      if (batch.nextCursor === null) break;
      after = batch.nextCursor;
    }
  }
  // SKIP LOCKED can return an empty batch while another worker owns rows. Do
  // not report completion until a separate, non-lock-skipping query sees zero.
  const [row] = await db
    .select({ remaining: count() })
    .from(sessionEventsIndex)
    .where(
      and(
        eq(sessionEventsIndex.kind, AgentRuntimeSignalKind.usage),
        ne(sessionEventsIndex.visibility, 'internal'),
      ),
    );
  if (!row || row.remaining !== 0)
    throw new Error(
      'Usage visibility verification failed; drain old writers and rerun the backfill',
    );
  return total;
}

const entrypoint = process.argv[1];
if (entrypoint !== undefined && resolve(entrypoint) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  if (args.some((arg) => arg !== '--verify'))
    throw new Error('Usage: backfill-usage-visibility [--verify]');
  const url = process.env['DATABASE_URL'];
  if (!url) throw new Error('DATABASE_URL is required');
  const { db, pool } = buildDb({ url, poolSize: 1 });
  try {
    const changed = await runUsageVisibilityBackfill(db, { verifyOnly: args.includes('--verify') });
    console.log(`Usage visibility verified; changed ${changed} index rows`);
  } finally {
    await pool.end();
  }
}
