// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import { expect, it } from 'vitest';

it('upgrades existing rows without deleting history and releases names only after deletion', async () => {
  const url = new URL(
    process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry',
  );
  url.pathname = '/postgres';
  const admin = new Pool({ connectionString: url.toString(), max: 1 });
  const database = `soft_delete_upgrade_${randomBytes(6).toString('hex')}`;
  const folder = await mkdtemp(join(tmpdir(), 'orca-soft-delete-'));
  const migrationsFolder = resolve('src/persistence/postgres/migrations');
  let pool: Pool | undefined;
  try {
    await admin.query(`CREATE DATABASE "${database}"`);
    url.pathname = `/${database}`;
    pool = new Pool({ connectionString: url.toString(), max: 1 });
    await mkdir(join(folder, 'meta'));
    const journal = JSON.parse(
      await readFile(join(migrationsFolder, 'meta/_journal.json'), 'utf8'),
    ) as { entries: { idx: number }[] };
    journal.entries = journal.entries.filter((entry) => entry.idx < 58);
    await writeFile(join(folder, 'meta/_journal.json'), JSON.stringify(journal));
    for (const file of await readdir(migrationsFolder)) {
      if (/^\d{4}_.*\.sql$/.test(file) && Number(file.slice(0, 4)) < 58)
        await copyFile(join(migrationsFolder, file), join(folder, file));
    }
    await migrate(drizzle(pool), { migrationsFolder: folder });
    await pool.query(`
      INSERT INTO organizations (id, name) VALUES ('org_soft', 'Soft deletion');
      INSERT INTO workspaces (id, organization_id, name, created_by) VALUES ('ws_soft', 'org_soft', 'Soft deletion', 'test');
      INSERT INTO environments (id, workspace_id, name, archived_at) VALUES ('env_soft', 'ws_soft', 'Retained', '2026-01-01');
      INSERT INTO agents (id, workspace_id, name, model_provider, model_id) VALUES ('agt_soft', 'ws_soft', 'Agent', 'anthropic', 'test');
      INSERT INTO agent_versions (id, workspace_id, agent_id, version, snapshot) VALUES ('agtver_soft', 'ws_soft', 'agt_soft', 1, '{}');
      INSERT INTO agent_triggers (id, workspace_id, guardrail_subject, name, agent_id, agent_version, environment_id, payload, cron_expression, status, archived_at)
        VALUES ('trg_soft', 'ws_soft', 'test', 'Deleted trigger', 'agt_soft', 1, 'env_soft', 'test', '* * * * *', 'archived', '2026-01-01');
    `);
    await migrate(drizzle(pool), { migrationsFolder });
    expect((await pool.query('SELECT id, deleted_at FROM environments')).rows).toEqual([
      { id: 'env_soft', deleted_at: null },
    ]);
    expect(
      (await pool.query('SELECT deleted_at = archived_at AS backfilled FROM agent_triggers')).rows,
    ).toEqual([{ backfilled: true }]);
    await expect(
      pool.query(
        "INSERT INTO environments (id, workspace_id, name) VALUES ('env_collision', 'ws_soft', 'Retained')",
      ),
    ).rejects.toMatchObject({ code: '23505' });
    await pool.query("UPDATE environments SET deleted_at = now() WHERE id = 'env_soft'");
    await pool.query(
      "INSERT INTO environments (id, workspace_id, name) VALUES ('env_replacement', 'ws_soft', 'Retained')",
    );
    expect((await pool.query('SELECT id FROM environments')).rows).toHaveLength(2);
    expect((await pool.query('SELECT id FROM agent_versions')).rows).toEqual([
      { id: 'agtver_soft' },
    ]);
    await migrate(drizzle(pool), { migrationsFolder });
    expect((await pool.query('SELECT id FROM environments')).rows).toHaveLength(2);
  } finally {
    await pool?.end();
    await admin.query(`DROP DATABASE IF EXISTS "${database}"`);
    await admin.end();
    await rm(folder, { recursive: true, force: true });
  }
});
