// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import { describe, expect, it } from 'vitest';
import { loadSessionHarnessBinding } from '../../src/domain/session-harness-binding.js';
import { buildSnapshotRecordLoader } from '../../src/api/snapshot-loader.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';

const REGISTRY_DATABASE_URL =
  process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
const ADMIN_DATABASE_URL = (() => {
  const url = new URL(REGISTRY_DATABASE_URL);
  url.pathname = '/postgres';
  return url.toString();
})();
const MIGRATIONS_FOLDER = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../src/persistence/postgres/migrations',
);

async function createPre0062MigrationsFolder(root: string, before = 62): Promise<string> {
  const folder = join(root, 'migrations');
  await mkdir(join(folder, 'meta'), { recursive: true });

  const migrationFiles = (await readdir(MIGRATIONS_FOLDER)).filter((name) => {
    const match = /^(\d{4})_.+\.sql$/.exec(name);
    return match !== null && Number(match[1]) < before;
  });
  await Promise.all(
    migrationFiles.map((name) => copyFile(join(MIGRATIONS_FOLDER, name), join(folder, name))),
  );

  const journal = JSON.parse(
    await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
  ) as { entries: Array<{ idx: number }> };
  journal.entries = journal.entries.filter((entry) => entry.idx < before);
  await writeFile(join(folder, 'meta/_journal.json'), `${JSON.stringify(journal, null, 2)}\n`);
  return folder;
}

describe('private harness state migration', () => {
  it('preserves legacy version harnesses while preventing future Agent harness changes', async () => {
    const databaseName = `registry_harness_upgrade_${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-harness-migrations-'));
    let pool: Pool | undefined;
    const clientClosures: Promise<void>[] = [];
    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      pool.on('connect', (client) => {
        clientClosures.push(new Promise<void>((resolve) => client.once('end', resolve)));
      });
      await migrate(drizzle(pool), {
        migrationsFolder: await createPre0062MigrationsFolder(tempDir, 61),
      });
      await pool.query(`
        INSERT INTO organizations (id, name) VALUES ('org_upgrade', 'Upgrade');
        INSERT INTO workspaces (id, organization_id, name, created_by)
        VALUES ('ws_upgrade', 'org_upgrade', 'Upgrade', 'migration-test');
        INSERT INTO agents (id, workspace_id, name, model_provider, model_id, metadata)
        VALUES ('agt_upgrade', 'ws_upgrade', 'Upgrade', 'anthropic', 'claude-sonnet-4', '{"harness":"claude_code"}');
      `);
      for (const [version, metadata] of [
        [1, {}],
        [2, { harness: 'claude_code' }],
      ] as const) {
        await pool.query(
          'INSERT INTO agent_versions (id, workspace_id, agent_id, version, snapshot) VALUES ($1,$2,$3,$4,$5)',
          [
            `agtver_upgrade_${version}`,
            'ws_upgrade',
            'agt_upgrade',
            version,
            {
              id: 'agt_upgrade',
              version,
              name: 'Upgrade',
              model: { provider: 'anthropic', id: 'claude-sonnet-4' },
              system: '',
              tools: [],
              mcp_servers: [],
              skills: [],
              metadata,
            },
          ],
        );
      }
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      const db = drizzle(pool) as unknown as DbClient;
      expect(await loadSessionHarnessBinding(db, 'ws_upgrade', 'agt_upgrade', 1)).toMatchObject({
        harness: 'claude_agent_sdk',
        mode: 'separate',
      });
      expect(await loadSessionHarnessBinding(db, 'ws_upgrade', 'agt_upgrade', 2)).toMatchObject({
        harness: 'claude_code',
        mode: 'colocated',
      });
      const loader = buildSnapshotRecordLoader(db);
      const old = await loader.loadAgentVersion!('ws_upgrade', 'agt_upgrade', 1);
      expect(old?.metadata).toMatchObject({ harness: 'claude_agent_sdk' });
      await expect(
        pool.query("UPDATE agents SET metadata = '{}' WHERE id = 'agt_upgrade'"),
      ).rejects.toThrow('immutable');
    } finally {
      try {
        await pool?.end();
        // pg-pool can resolve end() before its idle clients close their sockets.
        // Wait for those clients instead of killing them with DROP ... FORCE.
        await Promise.all(clientClosures);
        await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
      } finally {
        await adminPool.end();
        await rm(tempDir, { recursive: true, force: true });
      }
    }
  });

  it('preserves existing native history, excludes null state, and enforces Session ownership', async () => {
    const databaseName = `registry_checkpoint_upgrade_${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-checkpoint-migrations-'));
    let pool: Pool | undefined;
    const clientClosures: Promise<void>[] = [];
    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      pool.on('connect', (client) => {
        clientClosures.push(new Promise<void>((resolve) => client.once('end', resolve)));
      });
      await migrate(drizzle(pool), {
        migrationsFolder: await createPre0062MigrationsFolder(tempDir),
      });
      await pool.query(`
        INSERT INTO organizations (id, name) VALUES ('org_upgrade', 'Upgrade');
        INSERT INTO workspaces (id, organization_id, name, created_by)
        VALUES ('ws_upgrade', 'org_upgrade', 'Upgrade', 'migration-test');
        INSERT INTO agents (id, workspace_id, name, model_provider, model_id)
        VALUES ('agt_upgrade', 'ws_upgrade', 'Upgrade', 'openai', 'gpt-5.4');
        INSERT INTO agent_versions (id, workspace_id, agent_id, version, snapshot)
        VALUES ('agtver_upgrade', 'ws_upgrade', 'agt_upgrade', 1, '{}');
        UPDATE agents SET latest_version_id = 'agtver_upgrade' WHERE id = 'agt_upgrade';
        INSERT INTO sessions (id, workspace_id, agent_id, agent_version)
        VALUES ('ses_saved', 'ws_upgrade', 'agt_upgrade', 1), ('ses_empty', 'ws_upgrade', 'agt_upgrade', 1);
      `);
      const state = {
        version: 1,
        threadId: 'tid',
        files: {
          'sessions/2026/09/20/rollout-tid.jsonl':
            Buffer.from('private history').toString('base64'),
        },
      };
      await pool.query('UPDATE sessions SET harness_state = $1 WHERE id = $2', [
        state,
        'ses_saved',
      ]);
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      expect((await pool.query('SELECT * FROM session_harness_states')).rows).toEqual([
        { workspace_id: 'ws_upgrade', session_id: 'ses_saved', state },
      ]);
      expect(
        (await pool.query('SELECT * FROM sessions WHERE id = $1', ['ses_saved'])).rows[0],
      ).not.toHaveProperty('harness_state');
      await expect(
        pool.query('INSERT INTO session_harness_states VALUES ($1, $2, $3)', [
          'ws_foreign',
          'ses_saved',
          state,
        ]),
      ).rejects.toMatchObject({ code: '23503' });
      await pool.query('DELETE FROM sessions WHERE id = $1', ['ses_saved']);
      expect((await pool.query('SELECT * FROM session_harness_states')).rows).toEqual([]);
    } finally {
      try {
        await pool?.end();
        // pg-pool can resolve end() before its idle clients close their sockets.
        // Wait for those clients instead of killing them with DROP ... FORCE.
        await Promise.all(clientClosures);
        await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
      } finally {
        await adminPool.end();
        await rm(tempDir, { recursive: true, force: true });
      }
    }
  });
});
