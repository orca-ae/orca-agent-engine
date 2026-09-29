// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

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

describe('agent observability schema constraints', () => {
  let pool: Pool | undefined;
  let adminPool: Pool | undefined;
  const databaseName = `registry_agent_observability_schema_${process.pid}_${randomBytes(4).toString('hex')}`;
  const suffix = `${process.pid}_${Date.now()}`;
  const organizationA = `org_observability_schema_a_${suffix}`;
  const organizationB = `org_observability_schema_b_${suffix}`;
  const workspaceA = `ws_observability_schema_a_${suffix}`;
  const workspaceB = `ws_observability_schema_b_${suffix}`;
  const organizationBindingA = `aob_observability_schema_org_${suffix}`;

  beforeAll(async () => {
    adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    await adminPool.query(`CREATE DATABASE "${databaseName}"`);
    pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
    await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
    await pool.query(
      `INSERT INTO organizations (id, name) VALUES ($1, 'Observability schema A'), ($2, 'Observability schema B')`,
      [organizationA, organizationB],
    );
    await pool.query(
      `
        INSERT INTO workspaces (id, organization_id, name, created_by) VALUES
          ($1, $2, 'Observability workspace A', 'integration-test'),
          ($3, $4, 'Observability workspace B', 'integration-test')
      `,
      [workspaceA, organizationA, workspaceB, organizationB],
    );
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `
        INSERT INTO agent_observability_bindings (
          id, organization_id, workspace_id, scope_type, adapter_type, endpoint_kind, endpoint_class, endpoint,
          external_project_id, current_version, status, revocation_epoch, created_by, updated_by
        ) VALUES ($1, $2, NULL, 'organization', 'otlp_http', 'traces_endpoint', 'public', 'https://otlp.example/v1/traces',
                  'pk-lf-observability-schema-a', 1, 'active', 0, 'integration-test', 'integration-test')
      `,
        [organizationBindingA, organizationA],
      );
      await client.query(
        `
          INSERT INTO agent_observability_binding_versions (
            binding_id, version, adapter_type, semantic_profile, protocol, created_by
          ) VALUES ($1, 1, 'otlp_http', 'langfuse', 'http/protobuf', 'integration-test')
        `,
        [organizationBindingA],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  });

  afterAll(async () => {
    await pool?.end().catch(() => {});
    await adminPool
      ?.query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1', [
        databaseName,
      ])
      .catch(() => {});
    await adminPool?.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => {});
    await adminPool?.end().catch(() => {});
  });

  function requirePool(): Pool {
    if (!pool) throw new Error('agent observability schema test database is not initialized');
    return pool;
  }

  it('rejects cross-organization workspace ownership and cross-scope setting selection', async () => {
    const pool = requirePool();
    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_bindings (
            id, organization_id, workspace_id, scope_type, adapter_type, endpoint_kind, endpoint_class, endpoint,
            current_version, status, revocation_epoch, created_by, updated_by
          ) VALUES ($1, $2, $3, 'workspace', 'otlp_http', 'traces_endpoint', 'public', 'https://otlp.example/v1/traces',
                    1, 'active', 0, 'integration-test', 'integration-test')
        `,
        [`aob_observability_schema_cross_${suffix}`, organizationA, workspaceB],
      ),
    ).rejects.toMatchObject({ code: '23503' });

    await expect(
      pool.query(
        `
          UPDATE agent_observability_organization_settings
          SET active_default_binding_id = $1, active_default_binding_scope = 'organization'
          WHERE organization_id = $2
        `,
        [organizationBindingA, organizationB],
      ),
    ).rejects.toMatchObject({ code: '23503' });

    await expect(
      pool.query(
        `
          UPDATE agent_observability_workspace_settings
          SET mode = 'custom', binding_id = $3
          WHERE workspace_id = $1 AND organization_id = $2
        `,
        [workspaceA, organizationA, organizationBindingA],
      ),
    ).rejects.toMatchObject({ code: '23503' });

    await expect(
      pool.query(
        `
          UPDATE agent_observability_organization_settings
          SET active_default_binding_id = $1, active_default_binding_scope = NULL
          WHERE organization_id = $2
        `,
        [organizationBindingA, organizationB],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await expect(
      pool.query(
        `
          INSERT INTO session_observability_bindings (
            workspace_id, session_id, organization_id, binding_id, binding_version,
            binding_scope, binding_workspace_id, selection_source, status, agent_id, agent_version
          ) VALUES ($1, $2, $3, $4, 1, 'organization', NULL, 'organization_default', 'active',
                    'agt_observability_schema_wrong_org', 1)
        `,
        [
          workspaceB,
          `ses_observability_schema_wrong_org_binding_${suffix}`,
          organizationB,
          organizationBindingA,
        ],
      ),
    ).rejects.toMatchObject({ code: '23503' });

    await expect(
      pool.query(
        `
          INSERT INTO session_observability_bindings (
            workspace_id, session_id, organization_id, binding_id, binding_version,
            binding_scope, selection_source, status, agent_id, agent_version
          ) VALUES ($1, $2, $3, $4, 1, NULL, 'organization_default', 'active',
                    'agt_observability_schema_cross', 1)
        `,
        [
          workspaceB,
          `ses_observability_schema_cross_binding_${suffix}`,
          organizationB,
          organizationBindingA,
        ],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await expect(
      pool.query(
        `
          INSERT INTO session_observability_bindings (
            workspace_id, session_id, organization_id, binding_id, binding_version,
            binding_scope, binding_workspace_id, selection_source, status, agent_id, agent_version
          ) VALUES ($1, $2, $3, $4, 1, NULL, NULL, 'workspace_custom', 'active',
                    'agt_observability_schema_cross_workspace', 1)
        `,
        [
          workspaceB,
          `ses_observability_schema_cross_workspace_binding_${suffix}`,
          organizationB,
          organizationBindingA,
        ],
      ),
    ).rejects.toMatchObject({ code: '23514' });
  });

  it('rejects invalid status, mode, version, and capture combinations', async () => {
    const pool = requirePool();
    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_bindings (
            id, organization_id, scope_type, adapter_type, endpoint_kind, endpoint_class, endpoint,
            external_project_id, current_version, status, revocation_epoch, created_by, updated_by
          ) VALUES ($1, $2, 'organization', 'otlp_http', 'traces_endpoint', 'public', 'https://otlp.example/v1/traces',
                    '   ', 1, 'active', 0, 'integration-test', 'integration-test')
        `,
        [`aob_observability_schema_empty_project_${suffix}`, organizationA],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_bindings (
            id, organization_id, scope_type, adapter_type, endpoint_kind, endpoint_class, endpoint,
            current_version, status, revocation_epoch, created_by, updated_by
          ) VALUES ($1, $2, 'organization', 'otlp_http', 'traces_endpoint', 'public',
                    'https://otlp.example/v1/traces', 1, 'archived', 0,
                    'integration-test', 'integration-test')
        `,
        [`aob_observability_schema_archive_without_timestamp_${suffix}`, organizationA],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_bindings (
            id, organization_id, scope_type, adapter_type, endpoint_kind, endpoint_class, endpoint,
            current_version, status, revocation_epoch, created_by, updated_by
          ) VALUES ($1, $2, 'organization', 'otlp_http', 'traces_endpoint', 'public',
                    'https://otlp.example/v1/traces', 99, 'active', 0,
                    'integration-test', 'integration-test')
        `,
        [`aob_observability_schema_dangling_head_${suffix}`, organizationA],
      ),
    ).rejects.toMatchObject({ code: '23503' });

    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_bindings (
            id, organization_id, scope_type, adapter_type, endpoint_kind, endpoint_class, endpoint,
            current_version, status, revocation_epoch, created_by, updated_by
          ) VALUES ($1, $2, 'organization', 'otlp_http', 'traces_endpoint', 'public', 'https://otlp.example/v1/traces',
                    1, 'unknown', 0, 'integration-test', 'integration-test')
        `,
        [`aob_observability_schema_status_${suffix}`, organizationA],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await expect(
      pool.query(
        `
          INSERT INTO session_observability_bindings (
            workspace_id, session_id, organization_id, selection_source, status,
            agent_id, agent_version
          ) VALUES ($1, $2, $3, 'disabled', 'deleted', 'agt_observability_schema_deleted', 1)
        `,
        [workspaceA, `ses_observability_schema_deleted_without_timestamp_${suffix}`, organizationA],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await expect(
      pool.query(
        `
          UPDATE agent_observability_workspace_settings
          SET mode = 'custom', binding_id = NULL
          WHERE workspace_id = $1 AND organization_id = $2
        `,
        [workspaceA, organizationA],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_binding_versions (
            binding_id, version, adapter_type, semantic_profile, protocol, created_by
          ) VALUES ($1, 0, 'otlp_http', 'otel_genai', 'http/protobuf', 'integration-test')
        `,
        [organizationBindingA],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_binding_versions (
            binding_id, version, adapter_type, semantic_profile, protocol, capture_mode, created_by
          ) VALUES ($1, 2, 'otlp_http', 'otel_genai', 'http/protobuf', 'raw', 'integration-test')
        `,
        [organizationBindingA],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await expect(
      pool.query(
        `
          INSERT INTO session_observability_bindings (
            workspace_id, session_id, organization_id, effective_capture_mode, agent_id, agent_version
          ) VALUES ($1, $2, $3, 'redacted_io', 'agt_observability_schema', 1)
        `,
        [workspaceA, `ses_observability_schema_invalid_${suffix}`, organizationA],
      ),
    ).rejects.toMatchObject({ code: '23514' });
  });

  it('enforces durable Workspace archive marker ownership and epoch bounds', async () => {
    const pool = requirePool();
    const archiveAt = new Date('2035-08-18T03:00:00.000Z');
    await pool.query(
      `
        INSERT INTO agent_observability_workspace_archive_revocations (
          workspace_id, organization_id, archived_at, revocation_epoch
        ) VALUES ($1, $2, $3, 1)
      `,
      [workspaceA, organizationA, archiveAt],
    );
    const stored = await pool.query<{
      organization_id: string;
      workspace_id: string;
      archived_at: Date;
      revocation_epoch: string;
    }>(
      `
        SELECT organization_id, workspace_id, archived_at, revocation_epoch
        FROM agent_observability_workspace_archive_revocations
        WHERE workspace_id = $1
      `,
      [workspaceA],
    );
    expect(stored.rows).toEqual([
      {
        organization_id: organizationA,
        workspace_id: workspaceA,
        archived_at: archiveAt,
        revocation_epoch: '1',
      },
    ]);

    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_workspace_archive_revocations (
            workspace_id, organization_id, archived_at, revocation_epoch
          ) VALUES ($1, $2, $3, 1)
        `,
        [workspaceA, organizationA, archiveAt],
      ),
    ).rejects.toMatchObject({ code: '23505' });

    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_workspace_archive_revocations (
            workspace_id, organization_id, archived_at, revocation_epoch
          ) VALUES ($1, $2, $3, 1)
        `,
        [workspaceB, organizationA, archiveAt],
      ),
    ).rejects.toMatchObject({ code: '23503' });

    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_workspace_archive_revocations (
            workspace_id, organization_id, archived_at, revocation_epoch
          ) VALUES ($1, $2, $3, 0)
        `,
        [workspaceB, organizationB, archiveAt],
      ),
    ).rejects.toMatchObject({ code: '23514' });

    await pool.query(
      `
        DELETE FROM agent_observability_workspace_settings
        WHERE organization_id = $1 AND workspace_id = $2
      `,
      [organizationB, workspaceB],
    );
    await expect(
      pool.query(
        `
          INSERT INTO agent_observability_workspace_archive_revocations (
            workspace_id, organization_id, archived_at, revocation_epoch
          ) VALUES ($1, $2, $3, 1)
        `,
        [workspaceB, organizationB, archiveAt],
      ),
    ).rejects.toMatchObject({ code: '23503' });
  });

  it('keeps a disabled pin as a tombstone without a Session FK', async () => {
    const pool = requirePool();
    const sessionId = `ses_observability_schema_tombstone_${suffix}`;
    await pool.query(
      `
        INSERT INTO session_observability_bindings (
          workspace_id, session_id, organization_id, agent_id, agent_version
        ) VALUES ($1, $2, $3, 'agt_observability_schema', 1)
      `,
      [workspaceA, sessionId, organizationA],
    );
    const result = await pool.query<{
      binding_id: string | null;
      selection_source: string;
      status: string;
    }>(
      `
        SELECT binding_id, selection_source, status
        FROM session_observability_bindings
        WHERE workspace_id = $1 AND session_id = $2
      `,
      [workspaceA, sessionId],
    );
    expect(result.rows).toEqual([
      { binding_id: null, selection_source: 'disabled', status: 'disabled' },
    ]);

    await expect(
      pool.query(
        `
          INSERT INTO session_observability_bindings (
            workspace_id, session_id, organization_id, agent_id, agent_version
          ) VALUES ($1, $2, $3, 'agt_observability_schema', 1)
        `,
        [workspaceA, sessionId, organizationA],
      ),
    ).rejects.toMatchObject({ code: '23505' });
  });
});
