// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import { expect, it } from 'vitest';

const DATABASE_URL = process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
const MIGRATIONS_FOLDER = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../src/persistence/postgres/migrations',
);

it('upgrades the runner-index schema without skipping guardrail migrations or losing existing rules', async () => {
  const databaseName = `registry_policy_upgrade_${process.pid}_${randomBytes(4).toString('hex')}`;
  const adminUrl = new URL(DATABASE_URL);
  adminUrl.pathname = '/postgres';
  const databaseUrl = new URL(DATABASE_URL);
  databaseUrl.pathname = `/${databaseName}`;
  const admin = new Pool({ connectionString: adminUrl.toString(), max: 1 });
  const directory = await mkdtemp(join(tmpdir(), 'orca-policy-migration-'));
  let pool: Pool | undefined;
  const clientClosures: Promise<void>[] = [];
  try {
    const journal = JSON.parse(
      await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
    ) as { entries: Array<{ idx: number; tag: string; when: number }> };
    const baseline = { ...journal, entries: journal.entries.filter((entry) => entry.idx <= 65) };
    expect(baseline.entries.at(-1)?.tag).toBe('0065_sessions_runner_id_index');
    const baselineFolder = join(directory, 'baseline');
    await mkdir(join(baselineFolder, 'meta'), { recursive: true });
    await Promise.all(
      baseline.entries.map((entry) =>
        copyFile(
          join(MIGRATIONS_FOLDER, `${entry.tag}.sql`),
          join(baselineFolder, `${entry.tag}.sql`),
        ),
      ),
    );
    await writeFile(join(baselineFolder, 'meta/_journal.json'), JSON.stringify(baseline));

    await admin.query(`CREATE DATABASE "${databaseName}"`);
    pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
    pool.on('connect', (client) => {
      clientClosures.push(new Promise<void>((resolve) => client.once('end', resolve)));
    });
    const db = drizzle(pool);
    await migrate(db, { migrationsFolder: baselineFolder });
    await pool.query(`
      INSERT INTO organizations (id, name) VALUES ('org_upgrade', 'Policy upgrade');
      INSERT INTO workspaces (id, organization_id, name, created_by)
      VALUES ('ws_upgrade', 'org_upgrade', 'Policy upgrade', 'migration-test');
      INSERT INTO guardrails (id, organization_id, workspace_id, name, scope, rule, metadata)
      VALUES ('grd_upgrade', 'org_upgrade', 'ws_upgrade', 'Existing rule', 'workspace',
        '{"kind":"block_tool","tool_name":"bash"}', '{"preserved":true}');
    `);

    await migrate(db, { migrationsFolder: MIGRATIONS_FOLDER });
    expect((await pool.query('SELECT mode, metadata FROM guardrails')).rows).toEqual([
      { mode: 'enforce', metadata: { preserved: true } },
    ]);
    expect(
      (await pool.query("SELECT to_regclass('public.sessions_runner_id_idx') AS name")).rows,
    ).toEqual([{ name: 'sessions_runner_id_idx' }]);
    expect(
      (
        await pool.query(
          "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'guardrail_policy_%' ORDER BY tablename",
        )
      ).rows.map((row: { tablename: string }) => row.tablename),
    ).toEqual([
      'guardrail_policy_batches',
      'guardrail_policy_bindings',
      'guardrail_policy_calls',
      'guardrail_policy_events',
      'guardrail_policy_heads',
    ]);
    await expect(pool.query("UPDATE guardrails SET mode = 'invalid'")).rejects.toMatchObject({
      code: '23514',
    });
    await pool.query("UPDATE guardrails SET mode = 'observe'");
    await pool.query(`
      INSERT INTO guardrail_policy_heads (workspace_id, scope, subject)
      VALUES ('ws_upgrade', 'state', 'ws_upgrade');
    `);
    const beforeReplay = await pool.query('SELECT * FROM drizzle.__drizzle_migrations ORDER BY id');
    expect(beforeReplay.rows.at(-1)?.created_at).toBe(String(journal.entries.at(-1)!.when));
    await migrate(db, { migrationsFolder: MIGRATIONS_FOLDER });
    expect(
      (await pool.query('SELECT * FROM drizzle.__drizzle_migrations ORDER BY id')).rows,
    ).toEqual(beforeReplay.rows);
    expect((await pool.query('SELECT mode FROM guardrails')).rows).toEqual([{ mode: 'observe' }]);
  } finally {
    await pool?.end();
    await Promise.all(clientClosures);
    try {
      await admin.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
    } finally {
      await admin.end();
      await rm(directory, { recursive: true, force: true });
    }
  }
});
