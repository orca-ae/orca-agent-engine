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

async function createPre0044MigrationsFolder(root: string): Promise<string> {
  const folder = join(root, 'migrations');
  await mkdir(join(folder, 'meta'), { recursive: true });

  const migrationFiles = (await readdir(MIGRATIONS_FOLDER)).filter((name) => {
    const match = /^(\d{4})_.+\.sql$/.exec(name);
    return match !== null && Number(match[1]) < 44;
  });
  await Promise.all(
    migrationFiles.map((name) => copyFile(join(MIGRATIONS_FOLDER, name), join(folder, name))),
  );

  const journal = JSON.parse(
    await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
  ) as { entries: Array<{ idx: number }> };
  journal.entries = journal.entries.filter((entry) => entry.idx < 44);
  await writeFile(join(folder, 'meta/_journal.json'), `${JSON.stringify(journal, null, 2)}\n`);
  return folder;
}

describe('provider credential migration', () => {
  it('preserves legacy MCP rows and adds constrained provider records without backfill', async () => {
    const databaseName = `registry_provider_credential_${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-provider-migration-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      const pre0044Folder = await createPre0044MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0044Folder });

      await pool.query(`
        INSERT INTO organizations (id, name)
        VALUES ('org_provider_migration', 'Provider migration');
        INSERT INTO workspaces (id, organization_id, name, created_by)
        VALUES ('ws_provider_migration', 'org_provider_migration', 'Provider migration', 'test');
        INSERT INTO vaults (id, workspace_id, display_name)
        VALUES ('vlt_provider_migration', 'ws_provider_migration', 'Provider migration');
        INSERT INTO vault_credentials (
          id, workspace_id, vault_id, auth_type, mcp_server_url, access_secret_ref
        ) VALUES (
          'vcrd_legacy_mcp', 'ws_provider_migration', 'vlt_provider_migration',
          'static_bearer', 'https://mcp.example.test/sse', 'secret:legacy'
        );
      `);

      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });

      const legacy = await pool.query<{
        provider: string | null;
        scheme: string | null;
        logical_id: string | null;
        resolution_version: string | null;
        mcp_server_url: string | null;
      }>(`
        SELECT provider, scheme, logical_id, resolution_version, mcp_server_url
        FROM vault_credentials
        WHERE id = 'vcrd_legacy_mcp'
      `);
      expect(legacy.rows).toEqual([
        {
          provider: null,
          scheme: null,
          logical_id: null,
          resolution_version: null,
          mcp_server_url: 'https://mcp.example.test/sse',
        },
      ]);

      await pool.query(`
        INSERT INTO vault_credentials (
          id, workspace_id, vault_id, auth_type, provider, scheme, logical_id,
          resolution_version, access_secret_ref
        ) VALUES (
          'vcrd_provider', 'ws_provider_migration', 'vlt_provider_migration', 'provider',
          'vertex', 'gcp-service-account', 'llm:vertex', 'revision-1', 'secret:provider'
        )
      `);
      const provider = await pool.query<{
        mcp_server_url: string | null;
        logical_id: string;
      }>(`
        SELECT mcp_server_url, logical_id
        FROM vault_credentials
        WHERE id = 'vcrd_provider'
      `);
      expect(provider.rows).toEqual([{ mcp_server_url: null, logical_id: 'llm:vertex' }]);

      await expect(
        pool.query(`
          INSERT INTO vault_credentials (
            id, workspace_id, vault_id, auth_type, provider, scheme, logical_id,
            resolution_version, access_secret_ref
          ) VALUES (
            'vcrd_unsafe', 'ws_provider_migration', 'vlt_provider_migration', 'provider',
            'openai', 'bearer', 'llm/openai', 'revision-2', 'secret:unsafe'
          )
        `),
      ).rejects.toMatchObject({ code: '23514' });

      await expect(
        pool.query(`
          INSERT INTO vault_credentials (
            id, workspace_id, vault_id, auth_type, provider, scheme, logical_id,
            resolution_version, access_secret_ref
          ) VALUES (
            'vcrd_alias_collision', 'ws_provider_migration', 'vlt_provider_migration', 'provider',
            'openai', 'bearer', 'vcrd_legacy_mcp', 'revision-collision', 'secret:collision'
          )
        `),
      ).rejects.toMatchObject({ code: '23514' });

      await expect(
        pool.query(`
          INSERT INTO vault_credentials (
            id, workspace_id, vault_id, auth_type, provider, scheme, logical_id,
            resolution_version, access_secret_ref
          ) VALUES (
            'vcrd_incompatible', 'ws_provider_migration', 'vlt_provider_migration', 'provider',
            'anthropic', 'bearer', 'llm:anthropic', 'revision-3', 'secret:incompatible'
          )
        `),
      ).rejects.toMatchObject({ code: '23514' });
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
