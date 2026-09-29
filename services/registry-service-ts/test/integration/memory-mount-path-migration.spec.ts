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

async function createPre0041MigrationsFolder(root: string): Promise<string> {
  const folder = join(root, 'migrations');
  await mkdir(join(folder, 'meta'), { recursive: true });

  const migrationFiles = (await readdir(MIGRATIONS_FOLDER)).filter((name) => {
    const match = /^(\d{4})_.+\.sql$/.exec(name);
    return match !== null && Number(match[1]) < 41;
  });
  await Promise.all(
    migrationFiles.map((name) => copyFile(join(MIGRATIONS_FOLDER, name), join(folder, name))),
  );

  const journal = JSON.parse(
    await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
  ) as { entries: Array<{ idx: number }> };
  journal.entries = journal.entries.filter((entry) => entry.idx < 41);
  await writeFile(join(folder, 'meta/_journal.json'), `${JSON.stringify(journal, null, 2)}\n`);
  return folder;
}

describe('memory mount-path migration', () => {
  it('moves active legacy paths, resolves collisions, and advances runtime revision once', async () => {
    const databaseName = `registry_mount_upgrade_${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-migrations-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      const pre0041Folder = await createPre0041MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0041Folder });

      await pool.query(`
        INSERT INTO organizations (id, name) VALUES ('org_mount_upgrade', 'Mount upgrade');
        INSERT INTO workspaces (id, organization_id, name, created_by)
        VALUES ('ws_mount_upgrade', 'org_mount_upgrade', 'Mount upgrade', 'migration-test');
        INSERT INTO agents (id, workspace_id, name, model_provider, model_id)
        VALUES ('agt_mount_upgrade', 'ws_mount_upgrade', 'Mount upgrade', 'anthropic', 'test');
        INSERT INTO agent_versions (id, workspace_id, agent_id, version, snapshot)
        VALUES ('agtver_mount_upgrade', 'ws_mount_upgrade', 'agt_mount_upgrade', 1, '{}');
        UPDATE agents SET latest_version_id = 'agtver_mount_upgrade'
        WHERE id = 'agt_mount_upgrade';
        INSERT INTO sessions (
          id, workspace_id, agent_id, agent_version, runtime_revision, status
        ) VALUES (
          'ses_mount_upgrade', 'ws_mount_upgrade', 'agt_mount_upgrade', 1, 7, 'idle'
        );

        INSERT INTO session_resources (
          id, workspace_id, session_id, type, memory_store_id, mount_path, access
        ) VALUES
          (
            'sesrsc_LEGACY_A', 'ws_mount_upgrade', 'ses_mount_upgrade', 'memory_store',
            'mems_legacy_a', '/mnt/custom/', 'read_write'
          ),
          (
            'sesrsc_LEGACY_B', 'ws_mount_upgrade', 'ses_mount_upgrade', 'memory_store',
            'mems_legacy_b', '/mnt/notes/', 'read_only'
          ),
          (
            'sesrsc_COLLISION', 'ws_mount_upgrade', 'ses_mount_upgrade', 'file',
            NULL, '/mnt/memory/legacy-sesrsc_LEGACY_B/', 'read_only'
          ),
          (
            'sesrsc_SAFE', 'ws_mount_upgrade', 'ses_mount_upgrade', 'memory_store',
            'mems_safe', '/mnt/memory/already-safe/', 'read_write'
          );

        INSERT INTO session_resources (
          id, workspace_id, session_id, type, memory_store_id, mount_path, access, detached_at
        ) VALUES (
          'sesrsc_DETACHED', 'ws_mount_upgrade', 'ses_mount_upgrade', 'memory_store',
          'mems_detached', '/mnt/detached/', 'read_write', now()
        );
      `);

      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });

      const resources = await pool.query<{
        id: string;
        mount_path: string;
        detached_at: Date | null;
      }>(`
        SELECT id, mount_path, detached_at
        FROM session_resources
        WHERE session_id = 'ses_mount_upgrade'
        ORDER BY id
      `);
      const paths = new Map(resources.rows.map((row) => [row.id, row.mount_path]));

      expect(paths.get('sesrsc_LEGACY_A')).toBe('/mnt/memory/legacy-sesrsc_LEGACY_A/');
      expect(paths.get('sesrsc_LEGACY_B')).toBe('/mnt/memory/legacy-sesrsc_LEGACY_B-1/');
      expect(paths.get('sesrsc_COLLISION')).toBe('/mnt/memory/legacy-sesrsc_LEGACY_B/');
      expect(paths.get('sesrsc_SAFE')).toBe('/mnt/memory/already-safe/');
      expect(paths.get('sesrsc_DETACHED')).toBe('/mnt/detached/');

      const session = await pool.query<{ runtime_revision: number }>(`
        SELECT runtime_revision FROM sessions WHERE id = 'ses_mount_upgrade'
      `);
      expect(session.rows[0]?.runtime_revision).toBe(8);

      const remainingLegacy = await pool.query<{ count: string }>(`
        SELECT count(*)
        FROM session_resources
        WHERE session_id = 'ses_mount_upgrade'
          AND type = 'memory_store'
          AND detached_at IS NULL
          AND mount_path NOT LIKE '/mnt/memory/%'
      `);
      expect(remainingLegacy.rows[0]?.count).toBe('0');
    } finally {
      await pool?.end().catch(() => {});
      await adminPool
        .query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1', [
          databaseName,
        ])
        .catch(() => {});
      await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => {});
      await adminPool.end().catch(() => {});
      await rm(tempDir, { recursive: true, force: true });
    }
  }, 30_000);
});
