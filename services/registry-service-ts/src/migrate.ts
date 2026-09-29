// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';

const __dirname = dirname(fileURLToPath(import.meta.url));

export const ENSURE_SESSION_EVENTS_INDEX_UNPROCESSED_CLIENT_USER_SQL =
  'CREATE INDEX CONCURRENTLY IF NOT EXISTS "session_events_index_unprocessed_client_user_idx" ON "session_events_index" USING btree ("workspace_id","produced_at") WHERE "session_events_index"."visibility" = \'public\' AND "session_events_index"."produced_by" = \'client\' AND "session_events_index"."kind" LIKE \'user.%\' AND "session_events_index"."processed_at" IS NULL;';
const FIND_INVALID_SESSION_EVENTS_INDEX_UNPROCESSED_CLIENT_USER_SQL =
  "SELECT 1 FROM pg_index WHERE indexrelid = to_regclass('session_events_index_unprocessed_client_user_idx') AND NOT indisvalid;";
const DROP_INVALID_SESSION_EVENTS_INDEX_UNPROCESSED_CLIENT_USER_SQL =
  'DROP INDEX CONCURRENTLY IF EXISTS "session_events_index_unprocessed_client_user_idx";';

export interface MigrationQueryClient {
  query(sql: string): Promise<{ rows: unknown[] }>;
}

export async function ensureSessionEventsIndexUnprocessedClientUser(
  client: MigrationQueryClient,
): Promise<void> {
  // PostgreSQL leaves an invalid relation behind if a concurrent build is
  // interrupted. IF NOT EXISTS would otherwise silently preserve it.
  const invalidIndex = await client.query(
    FIND_INVALID_SESSION_EVENTS_INDEX_UNPROCESSED_CLIENT_USER_SQL,
  );
  if (invalidIndex.rows.length > 0) {
    await client.query(DROP_INVALID_SESSION_EVENTS_INDEX_UNPROCESSED_CLIENT_USER_SQL);
  }
  await client.query(ENSURE_SESSION_EVENTS_INDEX_UNPROCESSED_CLIENT_USER_SQL);
}

function findMigrationsFolder(): string {
  const candidates = [
    resolve(__dirname, 'persistence/postgres/migrations'),
    resolve(__dirname, '../src/persistence/postgres/migrations'),
  ];
  const folder = candidates.find((candidate) => existsSync(candidate));
  if (folder === undefined) {
    throw new Error(`registry migrations folder not found; tried: ${candidates.join(', ')}`);
  }
  return folder;
}

export async function runMigrations(databaseUrl: string): Promise<void> {
  const pool = new Pool({ connectionString: databaseUrl, connectionTimeoutMillis: 10_000 });
  try {
    const client = await pool.connect();
    try {
      await client.query('SELECT pg_advisory_lock(756682501, 1)');
      await migrate(drizzle(client), { migrationsFolder: findMigrationsFolder() });
      // 0060 installs this CHECK as NOT VALID so its history scan does not hold
      // the migration transaction's ACCESS EXCLUSIVE lock. Run after commit;
      // PostgreSQL skips an already validated CHECK, so interrupted runs resume.
      await client.query(
        'ALTER TABLE "session_observability_bindings" VALIDATE CONSTRAINT "session_observability_bindings_capture_check";',
      );
      await ensureSessionEventsIndexUnprocessedClientUser(client);
      console.log('registry-service-ts migrations applied');
    } finally {
      await client.query('SELECT pg_advisory_unlock(756682501, 1)').catch(() => {});
      client.release();
    }
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1];
if (entrypoint !== undefined && resolve(entrypoint) === fileURLToPath(import.meta.url)) {
  const databaseUrl = process.env['DATABASE_URL'];
  if (databaseUrl === undefined || databaseUrl === '') {
    throw new Error('DATABASE_URL is required');
  }
  await runMigrations(databaseUrl);
}
