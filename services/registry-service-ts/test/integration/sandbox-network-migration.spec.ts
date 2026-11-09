// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Pool } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { expect, it } from 'vitest';

it('retains revoked draft grant history and runner provenance while requiring live network parents', async () => {
  const url = new URL(process.env.DATABASE_URL ?? 'postgres://orca:orca@localhost:5432/registry');
  const adminUrl = new URL(url);
  adminUrl.pathname = '/postgres';
  const databaseName = 'registry_network_upgrade_' + randomBytes(8).toString('hex');
  const admin = new Pool({ connectionString: adminUrl.toString(), max: 1 });
  const root = await mkdtemp(join(tmpdir(), 'orca-network-migration-'));
  let pool: Pool | undefined;
  let created = false;
  try {
    await admin.query('CREATE DATABASE "' + databaseName + '"');
    created = true;
    url.pathname = '/' + databaseName;
    pool = new Pool({ connectionString: url.toString(), max: 1 });
    const migrations = resolve('src/persistence/postgres/migrations');
    await mkdir(join(root, 'meta'));
    const journal = JSON.parse(await readFile(join(migrations, 'meta/_journal.json'), 'utf8')) as {
      entries: { idx: number }[];
    };
    const draftEntries = journal.entries.filter((entry) => entry.idx < 72);
    journal.entries = draftEntries.filter((entry) => entry.idx < 69);
    await writeFile(join(root, 'meta/_journal.json'), JSON.stringify(journal));
    for (const name of await readdir(migrations))
      if (/^[0-9]{4}_.+[.]sql$/.test(name) && Number(name.slice(0, 4)) < 72)
        await copyFile(join(migrations, name), join(root, name));
    await migrate(drizzle(pool), { migrationsFolder: root });
    // Upgrade an existing installation before exercising draft-grant retirement.
    // Drizzle compares journal timestamps, not numeric migration filenames.
    journal.entries = draftEntries;
    await writeFile(join(root, 'meta/_journal.json'), JSON.stringify(journal));
    await migrate(drizzle(pool), { migrationsFolder: root });
    await pool.query("INSERT INTO organizations (id,name) VALUES ('org_upgrade','upgrade')");
    await pool.query(
      "INSERT INTO workspaces (id,organization_id,name,created_by) VALUES ('ws_upgrade','org_upgrade','upgrade','fixture')",
    );
    await pool.query(
      "INSERT INTO environments (id,workspace_id,name,target) VALUES ('env_upgrade','ws_upgrade','upgrade','cloud')",
    );
    await pool.query(
      "INSERT INTO agents (id,workspace_id,name,model_provider,model_id) VALUES ('agt_upgrade','ws_upgrade','upgrade','openai','fixture')",
    );
    const provenance = { launch_digest: 'preserve-launch', worker_conn_id: 'preserve-connection' };
    await pool.query(
      "INSERT INTO agent_versions (id,workspace_id,agent_id,version,snapshot) VALUES ('agtv_upgrade','ws_upgrade','agt_upgrade',1,$1)",
      [{}],
    );
    await pool.query(
      "INSERT INTO sessions (id,workspace_id,agent_id,agent_version,environment_id,runner_launch_authority) VALUES ('ses_upgrade','ws_upgrade','agt_upgrade',1,'env_upgrade',$1)",
      [provenance],
    );
    await pool.query(
      "INSERT INTO sandbox_egress_bindings (id,session_id,execution_generation,capability_digest,credentials) VALUES ('egb_upgrade','ses_upgrade','generation','digest',$1)",
      [{ mappings: [] }],
    );
    await migrate(drizzle(pool), { migrationsFolder: migrations });
    const result = await pool.query(
      'SELECT revoked_at,network_binding_id,credentials FROM sandbox_egress_bindings WHERE id=$1',
      ['egb_upgrade'],
    );
    expect(result.rows[0].revoked_at).toBeInstanceOf(Date);
    expect(result.rows[0].network_binding_id).toBeNull();
    expect(result.rows[0].credentials).toEqual({ mappings: [] });
    expect(
      (
        await pool.query('SELECT runner_launch_authority FROM sessions WHERE id=$1', [
          'ses_upgrade',
        ])
      ).rows[0].runner_launch_authority,
    ).toEqual(provenance);
    await expect(
      pool.query('UPDATE sandbox_egress_bindings SET revoked_at=NULL WHERE id=$1', ['egb_upgrade']),
    ).rejects.toMatchObject({ code: '23514' });
  } finally {
    await pool?.end();
    if (created) await admin.query('DROP DATABASE "' + databaseName + '"');
    await admin.end();
    await rm(root, { recursive: true, force: true });
  }
});
