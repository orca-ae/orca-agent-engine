// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { resolve } from 'node:path';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Client, Pool } from 'pg';
import { expect, it } from 'vitest';
import { runMigrations } from '../../src/migrate.js';

it('validates Session capture after the DDL transaction and resumes after interruption', async () => {
  const url = new URL(
    process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry',
  );
  url.pathname = '/postgres';
  const admin = new Pool({ connectionString: url.toString(), max: 1 });
  const database = `capture_validation_${randomBytes(6).toString('hex')}`;
  let pool: Pool | undefined;
  let writer: Client | undefined;
  const clientClosures: Promise<void>[] = [];
  try {
    await admin.query(`CREATE DATABASE "${database}"`);
    url.pathname = `/${database}`;
    pool = new Pool({ connectionString: url.toString(), max: 1 });
    pool.on('connect', (client) => {
      clientClosures.push(new Promise<void>((resolve) => client.once('end', resolve)));
    });
    // Simulate a process stopping just after the journaled DDL commits.
    await migrate(drizzle(pool), {
      migrationsFolder: resolve('src/persistence/postgres/migrations'),
    });
    const validationState = async () =>
      (
        await pool!.query<{ convalidated: boolean }>(
          "SELECT convalidated FROM pg_constraint WHERE conrelid = 'session_observability_bindings'::regclass AND conname = 'session_observability_bindings_capture_check'",
        )
      ).rows[0]?.convalidated;
    expect(await validationState()).toBe(false);
    const journalBefore = (
      await pool.query('SELECT * FROM drizzle.__drizzle_migrations ORDER BY id')
    ).rows;
    writer = new Client({ connectionString: url.toString() });
    await writer.connect();
    await writer.query('BEGIN');
    // The lock taken by ordinary writes must not block post-commit validation.
    await writer.query('LOCK TABLE session_observability_bindings IN ROW EXCLUSIVE MODE');
    url.searchParams.set('options', '-c lock_timeout=1000 -c statement_timeout=10000');
    await runMigrations(url.toString());
    expect(await validationState()).toBe(true);
    await runMigrations(url.toString());
    expect(
      (await pool.query('SELECT * FROM drizzle.__drizzle_migrations ORDER BY id')).rows,
    ).toEqual(journalBefore);
    expect(
      (
        await pool.query(
          "SELECT max_capture_mode FROM agent_observability_platform_policy WHERE id = 'default'",
        )
      ).rows,
    ).toEqual([{ max_capture_mode: 'metadata_only' }]);
  } finally {
    await writer?.query('ROLLBACK').catch(() => {});
    await writer?.end().catch(() => {});
    await pool?.end();
    // pg-pool can finish before idle clients emit end. Await socket shutdown
    // rather than forcibly terminating those still-disconnecting clients.
    await Promise.all(clientClosures);
    try {
      await admin.query(`DROP DATABASE IF EXISTS "${database}"`);
    } finally {
      await admin.end();
    }
  }
}, 60_000);
