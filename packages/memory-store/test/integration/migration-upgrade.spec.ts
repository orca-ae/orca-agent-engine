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
import { describe, expect, it } from 'vitest';
import { applyMigrations, PostgresMemoryMetadataStore } from '../../src/metadata/postgres.js';

const ADMIN_DATABASE_URL =
  process.env['MEMORYSTORE_ADMIN_DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/postgres';
const MIGRATIONS_FOLDER = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../src/metadata/migrations',
);

async function createPre0002MigrationsFolder(root: string): Promise<string> {
  const folder = join(root, 'migrations');
  await mkdir(join(folder, 'meta'), { recursive: true });
  await Promise.all(
    ['0000_memory_stores.sql', '0001_memory_store_metadata.sql'].map((name) =>
      copyFile(join(MIGRATIONS_FOLDER, name), join(folder, name)),
    ),
  );
  const journal = JSON.parse(
    await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
  ) as { entries: Array<{ idx: number }> };
  journal.entries = journal.entries.filter((entry) => entry.idx < 2);
  await writeFile(join(folder, 'meta/_journal.json'), `${JSON.stringify(journal, null, 2)}\n`);
  return folder;
}

describe('memory-store populated migration upgrades', () => {
  it('preserves orphaned legacy versions through the pre-0002 upgrade', async () => {
    const databaseName = `memory_upgrade_${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(ADMIN_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-memory-migrations-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      const pre0002Folder = await createPre0002MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0002Folder });

      await pool.query(`
        INSERT INTO memory_stores (id, workspace_id, name)
        VALUES ('mems_legacy', 'ws_legacy', 'legacy');

        INSERT INTO memories (
          id, store_id, path, current_sha256, size_bytes, updated_at,
          updated_by_session_id, updated_by_event_id
        ) VALUES (
          'mem_deleted', 'mems_legacy', 'notes/shared.md', '${'b'.repeat(64)}', 22,
          '2026-01-02T00:00:00Z', 'ses_deleted', 'evt_deleted'
        );

        INSERT INTO memory_versions (
          id, store_id, memory_id, path, sha256, size_bytes,
          written_by_session_id, written_by_event_id, written_at
        ) VALUES
          (
            'memver_deleted_1', 'mems_legacy', 'mem_deleted', 'notes/shared.md',
            '${'a'.repeat(64)}', 11, 'ses_deleted', 'evt_deleted_1', '2026-01-01T00:00:00Z'
          ),
          (
            'memver_deleted_2', 'mems_legacy', 'mem_deleted', 'notes/shared.md',
            '${'b'.repeat(64)}', 22, 'ses_deleted', 'evt_deleted_2', '2026-01-02T00:00:00Z'
          );

        DELETE FROM memories WHERE id = 'mem_deleted';

        INSERT INTO memories (
          id, store_id, path, current_sha256, size_bytes, updated_at
        ) VALUES (
          'mem_recreated', 'mems_legacy', 'notes/shared.md', '${'c'.repeat(64)}', 33,
          '2026-01-03T00:00:00Z'
        );

        INSERT INTO memory_versions (
          id, store_id, memory_id, path, sha256, size_bytes, written_at
        ) VALUES (
          'memver_recreated', 'mems_legacy', 'mem_recreated', 'notes/shared.md',
          '${'c'.repeat(64)}', 33, '2026-01-03T00:00:00Z'
        );
      `);

      await applyMigrations(pool);

      const metadata = new PostgresMemoryMetadataStore(pool);
      await expect(
        metadata.getMemory('ws_legacy', 'mems_legacy', 'mem_deleted'),
      ).resolves.toBeNull();
      await expect(
        metadata.listVersions('ws_legacy', 'mems_legacy', 'mem_deleted'),
      ).resolves.toMatchObject([
        { id: 'memver_deleted_2', sha256: 'b'.repeat(64) },
        { id: 'memver_deleted_1', sha256: 'a'.repeat(64) },
      ]);
      await expect(
        metadata.getMemoryByPath('ws_legacy', 'mems_legacy', 'notes/shared.md'),
      ).resolves.toMatchObject({ id: 'mem_recreated', currentSha256: 'c'.repeat(64) });

      const tombstone = await pool.query<{
        current_sha256: string;
        size_bytes: string;
        deleted_at: Date | null;
      }>(`SELECT current_sha256, size_bytes, deleted_at FROM memories WHERE id = 'mem_deleted'`);
      expect(tombstone.rows[0]).toMatchObject({
        current_sha256: 'b'.repeat(64),
        size_bytes: '22',
        deleted_at: expect.any(Date),
      });

      const versions = await pool.query<{ count: string }>('SELECT count(*) FROM memory_versions');
      expect(versions.rows[0]?.count).toBe('3');
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
