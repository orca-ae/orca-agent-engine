// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import { describe, expect, it } from 'vitest';

const execFileAsync = promisify(execFile);
const REGISTRY_DATABASE_URL =
  process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
const ADMIN_DATABASE_URL = (() => {
  const url = new URL(REGISTRY_DATABASE_URL);
  url.pathname = '/postgres';
  return url.toString();
})();
const integrationDirectory = dirname(fileURLToPath(import.meta.url));
const MIGRATIONS_FOLDER = resolve(
  integrationDirectory,
  '../../src/persistence/postgres/migrations',
);
const BOOTSTRAP = resolve(integrationDirectory, '../../src/bootstrap-admin.ts');
const TSX = resolve(integrationDirectory, '../../node_modules/.bin/tsx');

describe('bootstrap observability settings', () => {
  it('creates organization and workspace setting rows in its bootstrap transaction', async () => {
    const databaseName = `registry_bootstrap_observability_${process.pid}_${randomBytes(4).toString('hex')}`;
    const organizationId = `org_bootstrap_observability_${randomBytes(3).toString('hex')}`;
    const workspaceId = `ws_bootstrap_observability_${randomBytes(3).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });

      const env = { ...process.env, DATABASE_URL: databaseUrl.toString() };
      delete env['ORCA_BOOTSTRAP_ADMIN_API_KEY'];
      delete env['ORCA_BOOTSTRAP_PLATFORM_API_KEY'];
      await execFileAsync(TSX, [BOOTSTRAP], {
        env: {
          ...env,
          ORCA_BOOTSTRAP_ORGANIZATION_ID: organizationId,
          ORCA_BOOTSTRAP_WORKSPACE_ID: workspaceId,
          ORCA_BOOTSTRAP_ORGANIZATION_NAME: 'Bootstrap observability organization',
          ORCA_BOOTSTRAP_WORKSPACE_NAME: 'Bootstrap observability workspace',
        },
      });

      const settings = await pool.query<{
        organization_id: string;
        workspace_id: string | null;
        mode: string | null;
        capture_ceiling: string;
      }>(
        `
        SELECT organization_id, NULL::text AS workspace_id, NULL::text AS mode, capture_ceiling
        FROM agent_observability_organization_settings
        WHERE organization_id = $1
        UNION ALL
        SELECT organization_id, workspace_id, mode, capture_ceiling
        FROM agent_observability_workspace_settings
        WHERE workspace_id = $2
        ORDER BY workspace_id NULLS FIRST
      `,
        [organizationId, workspaceId],
      );
      expect(settings.rows).toEqual([
        {
          organization_id: organizationId,
          workspace_id: null,
          mode: null,
          capture_ceiling: 'metadata_only',
        },
        {
          organization_id: organizationId,
          workspace_id: workspaceId,
          mode: 'inherit',
          capture_ceiling: 'metadata_only',
        },
      ]);
    } finally {
      await pool?.end().catch(() => {});
      await adminPool
        .query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1', [
          databaseName,
        ])
        .catch(() => {});
      await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => {});
      await adminPool.end().catch(() => {});
    }
  }, 30_000);
});
