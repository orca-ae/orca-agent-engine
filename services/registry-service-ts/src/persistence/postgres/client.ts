// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import * as schema from './schema.js';

export type DbClient = ReturnType<typeof drizzle<typeof schema>>;
type DbTransactionCallback = Parameters<DbClient['transaction']>[0];

/** A callback-scoped client that holds one real database transaction. */
export type DbTransaction = DbTransactionCallback extends (
  tx: infer Transaction,
  ...args: never[]
) => Promise<unknown>
  ? Transaction
  : never;

type Assert<T extends true> = T;
// Keep transaction-only helpers from accepting a pool-backed root client.
type _DbClientIsNotDbTransaction = Assert<DbClient extends DbTransaction ? false : true>;

export interface DbConfig {
  url: string;
  poolSize?: number;
}

export function buildDb(config: DbConfig): { db: DbClient; pool: Pool } {
  const pool = new Pool({ connectionString: config.url, max: config.poolSize ?? 10 });
  const db = drizzle(pool, { schema });
  return { db, pool };
}
