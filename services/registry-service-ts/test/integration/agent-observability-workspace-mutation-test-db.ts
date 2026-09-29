// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import * as schema from '../../src/persistence/postgres/schema.js';

const REGISTRY_DATABASE_URL =
  process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
const MIGRATIONS_FOLDER = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../src/persistence/postgres/migrations',
);

export interface FreshWorkspaceMutationTestDb {
  db: DbClient;
  /** Direct pool access for deterministic lock and backend-PID integration tests. */
  pool: Pool;
  close(): Promise<void>;
}

/** Isolate mutation tests from a developer's long-lived integration database. */
export async function createFreshWorkspaceMutationTestDb(
  prefix: string,
): Promise<FreshWorkspaceMutationTestDb> {
  const adminUrl = new URL(REGISTRY_DATABASE_URL);
  adminUrl.pathname = '/postgres';
  const databaseName = `registry_${prefix}_${process.pid}_${randomBytes(4).toString('hex')}`;
  const databaseUrl = new URL(REGISTRY_DATABASE_URL);
  databaseUrl.pathname = `/${databaseName}`;
  const adminPool = new Pool({ connectionString: adminUrl.toString(), max: 1 });
  let pool: Pool | undefined;
  try {
    await adminPool.query(`CREATE DATABASE "${databaseName}"`);
    pool = new Pool({ connectionString: databaseUrl.toString(), max: 5 });
    const db = drizzle(pool, { schema });
    await migrate(db, { migrationsFolder: MIGRATIONS_FOLDER });
    return {
      db,
      pool,
      async close() {
        await pool?.end().catch(() => {});
        // Staging writers intentionally detach a bounded heartbeat drain after
        // a request has settled. Let it release its checked-out client before
        // force-dropping this isolated database.
        await new Promise((resolve) => setTimeout(resolve, 250));
        await adminPool
          .query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1', [
            databaseName,
          ])
          .catch(() => {});
        await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => {});
        await adminPool.end().catch(() => {});
      },
    };
  } catch (error) {
    await pool?.end().catch(() => {});
    await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => {});
    await adminPool.end().catch(() => {});
    throw error;
  }
}
