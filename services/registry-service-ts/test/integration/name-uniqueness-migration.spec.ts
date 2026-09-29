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

async function createPre0057MigrationsFolder(root: string): Promise<string> {
  const folder = join(root, 'migrations');
  await mkdir(join(folder, 'meta'), { recursive: true });
  const migrationFiles = (await readdir(MIGRATIONS_FOLDER)).filter((name) => {
    const match = /^(\d{4})_.+\.sql$/.exec(name);
    return match !== null && Number(match[1]) < 57;
  });
  await Promise.all(
    migrationFiles.map((name) => copyFile(join(MIGRATIONS_FOLDER, name), join(folder, name))),
  );
  const journal = JSON.parse(
    await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
  ) as { entries: Array<{ idx: number }> };
  journal.entries = journal.entries.filter((entry) => entry.idx < 57);
  await writeFile(join(folder, 'meta/_journal.json'), `${JSON.stringify(journal, null, 2)}\n`);
  return folder;
}

async function snapshot(pool: Pool) {
  const [organizations, workspaces, migrations, indexes] = await Promise.all([
    pool.query('SELECT * FROM organizations ORDER BY id'),
    pool.query('SELECT * FROM workspaces ORDER BY id'),
    pool.query('SELECT * FROM drizzle.__drizzle_migrations ORDER BY id'),
    pool.query<{ indexname: string }>(`
      SELECT indexname FROM pg_indexes
      WHERE schemaname = 'public'
        AND indexname IN ('organizations_name_idx', 'workspaces_organization_name_idx')
      ORDER BY indexname
    `),
  ]);
  return {
    organizations: organizations.rows,
    workspaces: workspaces.rows,
    migrations: migrations.rows,
    indexes: indexes.rows,
  };
}

describe('organization and workspace name uniqueness migration', () => {
  it('fails atomically on existing duplicates and retries after explicit renaming', async () => {
    const databaseName = `registry_name_uniqueness_${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-name-migration-'));
    let pool: Pool | undefined;
    const clientClosures: Promise<void>[] = [];

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      pool.on('connect', (client) => {
        clientClosures.push(new Promise<void>((resolve) => client.once('end', resolve)));
      });
      const pre0057Folder = await createPre0057MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0057Folder });
      await pool.query(`
        INSERT INTO organizations (id, name, status) VALUES
          ('org_first', 'Migration', 'active'),
          ('org_archived', 'Migration', 'archived');
        INSERT INTO workspaces (id, organization_id, name, created_by, status) VALUES
          ('ws_first', 'org_first', 'Shared', 'test', 'active'),
          ('ws_archived', 'org_first', 'Shared', 'test', 'archived'),
          ('ws_other_org', 'org_archived', 'Shared', 'test', 'active');
      `);

      const beforeOrganizationFailure = await snapshot(pool);
      expect(beforeOrganizationFailure.indexes).toEqual([]);
      await expect(
        migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER }),
      ).rejects.toMatchObject({
        code: '23505',
        constraint: 'organizations_name_idx',
      });
      expect(await snapshot(pool)).toEqual(beforeOrganizationFailure);

      // Archived records still reserve names; a case-only rename is an explicit resolution.
      await pool.query("UPDATE organizations SET name = 'migration' WHERE id = 'org_archived'");
      const beforeWorkspaceFailure = await snapshot(pool);
      await expect(
        migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER }),
      ).rejects.toMatchObject({
        code: '23505',
        constraint: 'workspaces_organization_name_idx',
      });
      // The organization index created before the workspace failure must roll back too.
      expect(await snapshot(pool)).toEqual(beforeWorkspaceFailure);

      await pool.query("UPDATE workspaces SET name = 'shared' WHERE id = 'ws_archived'");
      const beforeSuccess = await snapshot(pool);
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      const afterSuccess = await snapshot(pool);
      expect(afterSuccess.organizations).toEqual(beforeSuccess.organizations);
      expect(afterSuccess.workspaces).toEqual(beforeSuccess.workspaces);
      expect(afterSuccess.indexes).toEqual([
        { indexname: 'organizations_name_idx' },
        { indexname: 'workspaces_organization_name_idx' },
      ]);
      const journal = JSON.parse(
        await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
      ) as { entries: unknown[] };
      expect(afterSuccess.migrations).toHaveLength(journal.entries.length);
      expect(afterSuccess.migrations.slice(0, beforeSuccess.migrations.length)).toEqual(
        beforeSuccess.migrations,
      );

      await expect(
        pool.query("INSERT INTO organizations (id, name) VALUES ('org_duplicate', 'migration')"),
      ).rejects.toMatchObject({ code: '23505', constraint: 'organizations_name_idx' });
      await expect(
        pool.query(`
          INSERT INTO workspaces (id, organization_id, name, created_by)
          VALUES ('ws_duplicate', 'org_first', 'shared', 'test')
        `),
      ).rejects.toMatchObject({ code: '23505', constraint: 'workspaces_organization_name_idx' });

      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      expect(await snapshot(pool)).toEqual(afterSuccess);
    } finally {
      await pool?.end().catch(() => {});
      await Promise.all(clientClosures);
      // pool.end() closes the owned clients; don't race their socket shutdown
      // with pg_terminate_backend(), which can emit a late unhandled 57P01.
      await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => {});
      await adminPool.end().catch(() => {});
      await rm(tempDir, { recursive: true, force: true });
    }
  }, 30_000);
});
