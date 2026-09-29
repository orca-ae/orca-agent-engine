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

async function createPre0048MigrationsFolder(root: string): Promise<string> {
  const folder = join(root, 'migrations');
  await mkdir(join(folder, 'meta'), { recursive: true });

  const migrationFiles = (await readdir(MIGRATIONS_FOLDER)).filter((name) => {
    const match = /^(\d{4})_.+\.sql$/.exec(name);
    return match !== null && Number(match[1]) < 48;
  });
  await Promise.all(
    migrationFiles.map((name) => copyFile(join(MIGRATIONS_FOLDER, name), join(folder, name))),
  );

  const journal = JSON.parse(
    await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
  ) as { entries: Array<{ idx: number }> };
  journal.entries = journal.entries.filter((entry) => entry.idx < 48);
  await writeFile(join(folder, 'meta/_journal.json'), `${JSON.stringify(journal, null, 2)}\n`);
  return folder;
}

async function createPre0049MigrationsFolder(root: string): Promise<string> {
  const folder = join(root, 'migrations');
  await mkdir(join(folder, 'meta'), { recursive: true });

  const migrationFiles = (await readdir(MIGRATIONS_FOLDER)).filter((name) => {
    const match = /^(\d{4})_.+\.sql$/.exec(name);
    return match !== null && Number(match[1]) < 49;
  });
  await Promise.all(
    migrationFiles.map((name) => copyFile(join(MIGRATIONS_FOLDER, name), join(folder, name))),
  );

  const journal = JSON.parse(
    await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
  ) as { entries: Array<{ idx: number }> };
  journal.entries = journal.entries.filter((entry) => entry.idx < 49);
  await writeFile(join(folder, 'meta/_journal.json'), `${JSON.stringify(journal, null, 2)}\n`);
  return folder;
}

async function createPre0054MigrationsFolder(root: string): Promise<string> {
  const folder = join(root, 'migrations');
  await mkdir(join(folder, 'meta'), { recursive: true });

  const migrationFiles = (await readdir(MIGRATIONS_FOLDER)).filter((name) => {
    const match = /^(\d{4})_.+\.sql$/.exec(name);
    return match !== null && Number(match[1]) < 54;
  });
  await Promise.all(
    migrationFiles.map((name) => copyFile(join(MIGRATIONS_FOLDER, name), join(folder, name))),
  );

  const journal = JSON.parse(
    await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
  ) as { entries: Array<{ idx: number }> };
  journal.entries = journal.entries.filter((entry) => entry.idx < 54);
  await writeFile(join(folder, 'meta/_journal.json'), `${JSON.stringify(journal, null, 2)}\n`);
  return folder;
}

async function createPre0056MigrationsFolder(root: string): Promise<string> {
  const folder = join(root, 'migrations');
  await mkdir(join(folder, 'meta'), { recursive: true });

  const migrationFiles = (await readdir(MIGRATIONS_FOLDER)).filter((name) => {
    const match = /^(\d{4})_.+\.sql$/.exec(name);
    return match !== null && Number(match[1]) < 56;
  });
  await Promise.all(
    migrationFiles.map((name) => copyFile(join(MIGRATIONS_FOLDER, name), join(folder, name))),
  );

  const journal = JSON.parse(
    await readFile(join(MIGRATIONS_FOLDER, 'meta/_journal.json'), 'utf8'),
  ) as { entries: Array<{ idx: number }> };
  journal.entries = journal.entries.filter((entry) => entry.idx < 56);
  await writeFile(join(folder, 'meta/_journal.json'), `${JSON.stringify(journal, null, 2)}\n`);
  return folder;
}

async function rerunObservabilityBackfill(pool: Pool): Promise<void> {
  const migration = (await readdir(MIGRATIONS_FOLDER)).find((name) => /^0048_.+\.sql$/.test(name));
  if (!migration) throw new Error('0048 observability migration not found');
  const sql = await readFile(join(MIGRATIONS_FOLDER, migration), 'utf8');
  const start = sql.indexOf('-- observability-backfill-start');
  const end = sql.indexOf('-- observability-backfill-end');
  if (start < 0 || end < 0) throw new Error('0048 observability backfill markers not found');
  const backfill = sql
    .slice(start + '-- observability-backfill-start'.length, end)
    .replaceAll('--> statement-breakpoint', '');
  await pool.query(backfill);
}

const SESSION_PIN_REPAIR_INITIAL_BACKFILL_START =
  '-- session-observability-pin-repair-initial-backfill-start';
const SESSION_PIN_REPAIR_INITIAL_BACKFILL_END =
  '-- session-observability-pin-repair-initial-backfill-end';
const SESSION_PIN_REPAIR_TRIGGER_INSTALL_START =
  '-- session-observability-pin-repair-trigger-install-start';
const SESSION_PIN_REPAIR_TRIGGER_INSTALL_END =
  '-- session-observability-pin-repair-trigger-install-end';
const SESSION_PIN_REPAIR_CATCH_UP_BACKFILL_START =
  '-- session-observability-pin-repair-catch-up-backfill-start';
const SESSION_PIN_REPAIR_CATCH_UP_BACKFILL_END =
  '-- session-observability-pin-repair-catch-up-backfill-end';

interface SessionPinRepairMigrationSections {
  sql: string;
  setup: string;
  initialBackfill: string;
  triggerInstall: string;
  catchUpBackfill: string;
}

const SESSION_LIFECYCLE_ENFORCEMENT_INITIAL_BACKFILL_START =
  '-- session-observability-lifecycle-enforcement-initial-backfill-start';
const SESSION_LIFECYCLE_ENFORCEMENT_INITIAL_BACKFILL_END =
  '-- session-observability-lifecycle-enforcement-initial-backfill-end';
const SESSION_LIFECYCLE_ENFORCEMENT_TRIGGER_INSTALL_START =
  '-- session-observability-lifecycle-enforcement-trigger-install-start';
const SESSION_LIFECYCLE_ENFORCEMENT_TRIGGER_INSTALL_END =
  '-- session-observability-lifecycle-enforcement-trigger-install-end';
const SESSION_LIFECYCLE_ENFORCEMENT_CATCH_UP_BACKFILL_START =
  '-- session-observability-lifecycle-enforcement-catch-up-backfill-start';
const SESSION_LIFECYCLE_ENFORCEMENT_CATCH_UP_BACKFILL_END =
  '-- session-observability-lifecycle-enforcement-catch-up-backfill-end';

const WORKSPACE_ARCHIVE_REVOCATION_INITIAL_BACKFILL_START =
  '-- workspace-archive-observability-revocation-initial-backfill-start';
const WORKSPACE_ARCHIVE_REVOCATION_INITIAL_BACKFILL_END =
  '-- workspace-archive-observability-revocation-initial-backfill-end';
const WORKSPACE_ARCHIVE_REVOCATION_TRIGGER_INSTALL_START =
  '-- workspace-archive-observability-revocation-trigger-install-start';
const WORKSPACE_ARCHIVE_REVOCATION_TRIGGER_INSTALL_END =
  '-- workspace-archive-observability-revocation-trigger-install-end';
const WORKSPACE_ARCHIVE_REVOCATION_CATCH_UP_BACKFILL_START =
  '-- workspace-archive-observability-revocation-catch-up-backfill-start';
const WORKSPACE_ARCHIVE_REVOCATION_CATCH_UP_BACKFILL_END =
  '-- workspace-archive-observability-revocation-catch-up-backfill-end';

interface SessionLifecycleEnforcementMigrationSections {
  sql: string;
  setup: string;
  initialBackfill: string;
  triggerInstall: string;
  catchUpBackfill: string;
}

interface WorkspaceArchiveRevocationMigrationSections {
  sql: string;
  schemaSetup: string;
  setup: string;
  initialBackfill: string;
  triggerInstall: string;
  catchUpBackfill: string;
}

async function readSessionPinRepairMigrationSections(): Promise<SessionPinRepairMigrationSections> {
  const migration = (await readdir(MIGRATIONS_FOLDER)).find((name) => /^0049_.+\.sql$/.test(name));
  if (!migration) throw new Error('0049 Session observability pin repair migration not found');
  const sql = await readFile(join(MIGRATIONS_FOLDER, migration), 'utf8');
  const initialStart = sql.indexOf(SESSION_PIN_REPAIR_INITIAL_BACKFILL_START);
  const initialEnd = sql.indexOf(SESSION_PIN_REPAIR_INITIAL_BACKFILL_END);
  const triggerStart = sql.indexOf(SESSION_PIN_REPAIR_TRIGGER_INSTALL_START);
  const triggerEnd = sql.indexOf(SESSION_PIN_REPAIR_TRIGGER_INSTALL_END);
  const catchUpStart = sql.indexOf(SESSION_PIN_REPAIR_CATCH_UP_BACKFILL_START);
  const catchUpEnd = sql.indexOf(SESSION_PIN_REPAIR_CATCH_UP_BACKFILL_END);
  if (
    initialStart < 0 ||
    initialEnd < initialStart ||
    triggerStart < initialEnd ||
    triggerEnd < triggerStart ||
    catchUpStart < triggerEnd ||
    catchUpEnd < catchUpStart
  ) {
    throw new Error(
      '0049 Session observability pin repair phase markers are missing or out of order',
    );
  }

  const statementSql = (value: string) => value.replaceAll('--> statement-breakpoint', '');
  return {
    sql,
    setup: statementSql(sql.slice(0, initialStart)),
    initialBackfill: statementSql(
      sql.slice(initialStart + SESSION_PIN_REPAIR_INITIAL_BACKFILL_START.length, initialEnd),
    ),
    triggerInstall: statementSql(
      sql.slice(triggerStart + SESSION_PIN_REPAIR_TRIGGER_INSTALL_START.length, triggerEnd),
    ),
    catchUpBackfill: statementSql(
      sql.slice(catchUpStart + SESSION_PIN_REPAIR_CATCH_UP_BACKFILL_START.length, catchUpEnd),
    ),
  };
}

async function readSessionLifecycleEnforcementMigrationSections(): Promise<SessionLifecycleEnforcementMigrationSections> {
  const migration = (await readdir(MIGRATIONS_FOLDER)).find((name) => /^0054_.+\.sql$/.test(name));
  if (!migration)
    throw new Error('0054 Session observability lifecycle enforcement migration not found');
  const sql = await readFile(join(MIGRATIONS_FOLDER, migration), 'utf8');
  const initialStart = sql.indexOf(SESSION_LIFECYCLE_ENFORCEMENT_INITIAL_BACKFILL_START);
  const initialEnd = sql.indexOf(SESSION_LIFECYCLE_ENFORCEMENT_INITIAL_BACKFILL_END);
  const triggerStart = sql.indexOf(SESSION_LIFECYCLE_ENFORCEMENT_TRIGGER_INSTALL_START);
  const triggerEnd = sql.indexOf(SESSION_LIFECYCLE_ENFORCEMENT_TRIGGER_INSTALL_END);
  const catchUpStart = sql.indexOf(SESSION_LIFECYCLE_ENFORCEMENT_CATCH_UP_BACKFILL_START);
  const catchUpEnd = sql.indexOf(SESSION_LIFECYCLE_ENFORCEMENT_CATCH_UP_BACKFILL_END);
  if (
    initialStart < 0 ||
    initialEnd < initialStart ||
    triggerStart < initialEnd ||
    triggerEnd < triggerStart ||
    catchUpStart < triggerEnd ||
    catchUpEnd < catchUpStart
  ) {
    throw new Error(
      '0054 Session observability lifecycle enforcement phase markers are missing or out of order',
    );
  }

  const statementSql = (value: string) => value.replaceAll('--> statement-breakpoint', '');
  return {
    sql,
    setup: statementSql(sql.slice(0, initialStart)),
    initialBackfill: statementSql(
      sql.slice(
        initialStart + SESSION_LIFECYCLE_ENFORCEMENT_INITIAL_BACKFILL_START.length,
        initialEnd,
      ),
    ),
    triggerInstall: statementSql(
      sql.slice(
        triggerStart + SESSION_LIFECYCLE_ENFORCEMENT_TRIGGER_INSTALL_START.length,
        triggerEnd,
      ),
    ),
    catchUpBackfill: statementSql(
      sql.slice(
        catchUpStart + SESSION_LIFECYCLE_ENFORCEMENT_CATCH_UP_BACKFILL_START.length,
        catchUpEnd,
      ),
    ),
  };
}

async function readWorkspaceArchiveRevocationMigrationSections(): Promise<WorkspaceArchiveRevocationMigrationSections> {
  const migration = (await readdir(MIGRATIONS_FOLDER)).find((name) => /^0056_.+\.sql$/.test(name));
  if (!migration)
    throw new Error('0056 Workspace archive observability revocation migration not found');
  const sql = await readFile(join(MIGRATIONS_FOLDER, migration), 'utf8');
  const fallbackStart = sql.indexOf('-- Temporary mixed-version fallback');
  const initialStart = sql.indexOf(WORKSPACE_ARCHIVE_REVOCATION_INITIAL_BACKFILL_START);
  const initialEnd = sql.indexOf(WORKSPACE_ARCHIVE_REVOCATION_INITIAL_BACKFILL_END);
  const triggerStart = sql.indexOf(WORKSPACE_ARCHIVE_REVOCATION_TRIGGER_INSTALL_START);
  const triggerEnd = sql.indexOf(WORKSPACE_ARCHIVE_REVOCATION_TRIGGER_INSTALL_END);
  const catchUpStart = sql.indexOf(WORKSPACE_ARCHIVE_REVOCATION_CATCH_UP_BACKFILL_START);
  const catchUpEnd = sql.indexOf(WORKSPACE_ARCHIVE_REVOCATION_CATCH_UP_BACKFILL_END);
  if (
    fallbackStart < 0 ||
    initialStart < fallbackStart ||
    initialEnd < initialStart ||
    triggerStart < initialEnd ||
    triggerEnd < triggerStart ||
    catchUpStart < triggerEnd ||
    catchUpEnd < catchUpStart
  ) {
    throw new Error(
      '0056 Workspace archive observability revocation phase markers are missing or out of order',
    );
  }

  const statementSql = (value: string) => value.replaceAll('--> statement-breakpoint', '');
  return {
    sql,
    schemaSetup: statementSql(sql.slice(0, fallbackStart)),
    setup: statementSql(sql.slice(fallbackStart, initialStart)),
    initialBackfill: statementSql(
      sql.slice(
        initialStart + WORKSPACE_ARCHIVE_REVOCATION_INITIAL_BACKFILL_START.length,
        initialEnd,
      ),
    ),
    triggerInstall: statementSql(
      sql.slice(
        triggerStart + WORKSPACE_ARCHIVE_REVOCATION_TRIGGER_INSTALL_START.length,
        triggerEnd,
      ),
    ),
    catchUpBackfill: statementSql(
      sql.slice(
        catchUpStart + WORKSPACE_ARCHIVE_REVOCATION_CATCH_UP_BACKFILL_START.length,
        catchUpEnd,
      ),
    ),
  };
}

async function rerunSessionPinRepairBackfills(pool: Pool): Promise<void> {
  const { initialBackfill, catchUpBackfill } = await readSessionPinRepairMigrationSections();
  await pool.query(initialBackfill);
  await pool.query(catchUpBackfill);
}

async function rerunSessionLifecycleEnforcementBackfills(pool: Pool): Promise<void> {
  const { initialBackfill, catchUpBackfill } =
    await readSessionLifecycleEnforcementMigrationSections();
  await pool.query(initialBackfill);
  await pool.query(catchUpBackfill);
}

async function rerunWorkspaceArchiveRevocationBackfills(pool: Pool): Promise<void> {
  const { initialBackfill, catchUpBackfill } =
    await readWorkspaceArchiveRevocationMigrationSections();
  await pool.query(initialBackfill);
  await pool.query(catchUpBackfill);
}

async function closeTemporaryDatabase(
  pool: Pool | undefined,
  adminPool: Pool,
  databaseName: string,
): Promise<void> {
  await pool?.end().catch(() => {});
  await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => {});
  await adminPool.end().catch(() => {});
}

interface SessionLifecycleFixture {
  organizationId: string;
  workspaceId: string;
  agentId: string;
}

interface StoredSessionObservabilityPin {
  status: string;
  archived_at: Date | null;
  deleted_at: Date | null;
  updated_at: Date;
  session_revocation_epoch: string;
}

async function seedSessionLifecycleFixture(
  pool: Pool,
  suffix: string,
): Promise<SessionLifecycleFixture> {
  const fixture = {
    organizationId: `org_session_lifecycle_${suffix}`,
    workspaceId: `ws_session_lifecycle_${suffix}`,
    agentId: `agt_session_lifecycle_${suffix}`,
  };
  await pool.query(`INSERT INTO organizations (id, name) VALUES ($1, 'Session lifecycle')`, [
    fixture.organizationId,
  ]);
  await pool.query(
    `
      INSERT INTO workspaces (id, organization_id, name, created_by)
      VALUES ($1, $2, 'Session lifecycle workspace', 'test')
    `,
    [fixture.workspaceId, fixture.organizationId],
  );
  await pool.query(
    `
      INSERT INTO agents (id, workspace_id, name, model_provider, model_id)
      VALUES ($1, $2, 'Session lifecycle agent', 'anthropic', 'test')
    `,
    [fixture.agentId, fixture.workspaceId],
  );
  await pool.query(
    `
      INSERT INTO agent_versions (id, workspace_id, agent_id, version, snapshot)
      VALUES ($1, $2, $3, 1, '{}'::jsonb)
    `,
    [`agtver_session_lifecycle_${suffix}`, fixture.workspaceId, fixture.agentId],
  );
  return fixture;
}

async function insertLegacySession(
  pool: Pick<Pool, 'query'>,
  fixture: SessionLifecycleFixture,
  sessionId: string,
  now = new Date('2035-08-18T03:00:00.000Z'),
): Promise<void> {
  await pool.query(
    `
      INSERT INTO sessions (id, workspace_id, agent_id, agent_version, created_at, updated_at)
      VALUES ($1, $2, $3, 1, $4, $4)
    `,
    [sessionId, fixture.workspaceId, fixture.agentId, now],
  );
}

async function loadSessionObservabilityPin(
  pool: Pool,
  workspaceId: string,
  sessionId: string,
): Promise<StoredSessionObservabilityPin | undefined> {
  const result = await pool.query<StoredSessionObservabilityPin>(
    `
      SELECT status, archived_at, deleted_at, updated_at, session_revocation_epoch
      FROM session_observability_bindings
      WHERE workspace_id = $1 AND session_id = $2
    `,
    [workspaceId, sessionId],
  );
  return result.rows[0];
}

interface WorkspaceArchiveFixture {
  organizationId: string;
  workspaceId: string;
}

interface StoredWorkspaceArchiveSetting {
  organization_id: string;
  workspace_id: string;
  mode: string;
  binding_id: string | null;
  selection_epoch: string;
  revocation_epoch: string;
  capture_ceiling: string;
  capture_restriction_epoch: string;
  updated_at: Date;
}

interface StoredWorkspaceArchiveRevocation {
  organization_id: string;
  workspace_id: string;
  archived_at: Date;
  revocation_epoch: string;
  created_at: Date;
}

async function seedWorkspaceArchiveFixture(
  pool: Pick<Pool, 'query'>,
  suffix: string,
): Promise<WorkspaceArchiveFixture> {
  const fixture = {
    organizationId: `org_workspace_archive_${suffix}`,
    workspaceId: `ws_workspace_archive_${suffix}`,
  };
  await pool.query(`INSERT INTO organizations (id, name) VALUES ($1, $2)`, [
    fixture.organizationId,
    `Workspace archive ${suffix}`,
  ]);
  await pool.query(
    `
      INSERT INTO workspaces (id, organization_id, name, created_by)
      VALUES ($1, $2, 'Workspace archive', 'test')
    `,
    [fixture.workspaceId, fixture.organizationId],
  );
  return fixture;
}

async function loadWorkspaceArchiveSetting(
  pool: Pick<Pool, 'query'>,
  fixture: WorkspaceArchiveFixture,
): Promise<StoredWorkspaceArchiveSetting | undefined> {
  const result = await pool.query<StoredWorkspaceArchiveSetting>(
    `
      SELECT organization_id, workspace_id, mode, binding_id, selection_epoch,
             revocation_epoch, capture_ceiling, capture_restriction_epoch, updated_at
      FROM agent_observability_workspace_settings
      WHERE organization_id = $1 AND workspace_id = $2
    `,
    [fixture.organizationId, fixture.workspaceId],
  );
  return result.rows[0];
}

async function loadWorkspaceArchiveRevocation(
  pool: Pick<Pool, 'query'>,
  fixture: WorkspaceArchiveFixture,
): Promise<StoredWorkspaceArchiveRevocation | undefined> {
  const result = await pool.query<StoredWorkspaceArchiveRevocation>(
    `
      SELECT organization_id, workspace_id, archived_at, revocation_epoch, created_at
      FROM agent_observability_workspace_archive_revocations
      WHERE organization_id = $1 AND workspace_id = $2
    `,
    [fixture.organizationId, fixture.workspaceId],
  );
  return result.rows[0];
}

describe('agent observability migration', () => {
  it('backfills disabled pins and setting rows from legacy parents without storing secrets', async () => {
    const databaseName = `registry_agent_observability_${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-agent-observability-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      const pre0048Folder = await createPre0048MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0048Folder });

      await pool.query(`
        INSERT INTO organizations (id, name) VALUES
          ('org_observability_legacy_a', 'Legacy A'),
          ('org_observability_legacy_b', 'Legacy B');
        INSERT INTO workspaces (id, organization_id, name, created_by) VALUES
          ('ws_observability_legacy_a', 'org_observability_legacy_a', 'Legacy workspace A', 'test'),
          ('ws_observability_legacy_b', 'org_observability_legacy_b', 'Legacy workspace B', 'test');
        INSERT INTO agents (id, workspace_id, name, model_provider, model_id) VALUES
          ('agt_observability_legacy_a', 'ws_observability_legacy_a', 'Legacy agent A', 'anthropic', 'test'),
          ('agt_observability_legacy_b', 'ws_observability_legacy_b', 'Legacy agent B', 'anthropic', 'test');
        INSERT INTO agent_versions (id, workspace_id, agent_id, version, snapshot) VALUES
          ('agtver_observability_legacy_a', 'ws_observability_legacy_a', 'agt_observability_legacy_a', 1, '{}'),
          ('agtver_observability_legacy_b', 'ws_observability_legacy_b', 'agt_observability_legacy_b', 1, '{}');
        UPDATE agents SET latest_version_id = CASE id
          WHEN 'agt_observability_legacy_a' THEN 'agtver_observability_legacy_a'
          WHEN 'agt_observability_legacy_b' THEN 'agtver_observability_legacy_b'
        END;
        INSERT INTO sessions (id, workspace_id, agent_id, agent_version) VALUES
          ('ses_observability_legacy_a', 'ws_observability_legacy_a', 'agt_observability_legacy_a', 1),
          ('ses_observability_legacy_b', 'ws_observability_legacy_b', 'agt_observability_legacy_b', 1);
      `);

      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      // The migration runner does not execute an applied migration again. Run
      // the exact conflict-safe data section twice to prove restart/retry does
      // not duplicate policy, setting, or pin rows.
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      await rerunObservabilityBackfill(pool);
      await rerunObservabilityBackfill(pool);

      const policy = await pool.query<{
        id: string;
        allowed_adapters: string[];
        allowed_endpoint_classes: string[];
        max_capture_mode: string;
        capture_restriction_epoch: string;
      }>(`
        SELECT id, allowed_adapters, allowed_endpoint_classes, max_capture_mode, capture_restriction_epoch
        FROM agent_observability_platform_policy
      `);
      expect(policy.rows).toEqual([
        {
          id: 'default',
          allowed_adapters: ['otlp_http'],
          allowed_endpoint_classes: ['public'],
          max_capture_mode: 'metadata_only',
          capture_restriction_epoch: '0',
        },
      ]);

      const organizationSettings = await pool.query<{
        organization_id: string;
        active_default_binding_id: string | null;
        capture_ceiling: string;
      }>(`
        SELECT organization_id, active_default_binding_id, capture_ceiling
        FROM agent_observability_organization_settings
        ORDER BY organization_id
      `);
      expect(organizationSettings.rows).toEqual([
        {
          organization_id: 'org_observability_legacy_a',
          active_default_binding_id: null,
          capture_ceiling: 'metadata_only',
        },
        {
          organization_id: 'org_observability_legacy_b',
          active_default_binding_id: null,
          capture_ceiling: 'metadata_only',
        },
      ]);

      const workspaceSettings = await pool.query<{
        workspace_id: string;
        organization_id: string;
        mode: string;
        binding_id: string | null;
      }>(`
        SELECT workspace_id, organization_id, mode, binding_id
        FROM agent_observability_workspace_settings
        ORDER BY workspace_id
      `);
      expect(workspaceSettings.rows).toEqual([
        {
          workspace_id: 'ws_observability_legacy_a',
          organization_id: 'org_observability_legacy_a',
          mode: 'inherit',
          binding_id: null,
        },
        {
          workspace_id: 'ws_observability_legacy_b',
          organization_id: 'org_observability_legacy_b',
          mode: 'inherit',
          binding_id: null,
        },
      ]);

      const pins = await pool.query<{
        workspace_id: string;
        session_id: string;
        organization_id: string;
        binding_id: string | null;
        binding_version: number | null;
        selection_source: string;
        status: string;
        effective_capture_mode: string;
      }>(`
        SELECT workspace_id, session_id, organization_id, binding_id, binding_version,
               selection_source, status, effective_capture_mode
        FROM session_observability_bindings
        ORDER BY session_id
      `);
      expect(pins.rows).toEqual([
        {
          workspace_id: 'ws_observability_legacy_a',
          session_id: 'ses_observability_legacy_a',
          organization_id: 'org_observability_legacy_a',
          binding_id: null,
          binding_version: null,
          selection_source: 'disabled',
          status: 'disabled',
          effective_capture_mode: 'metadata_only',
        },
        {
          workspace_id: 'ws_observability_legacy_b',
          session_id: 'ses_observability_legacy_b',
          organization_id: 'org_observability_legacy_b',
          binding_id: null,
          binding_version: null,
          selection_source: 'disabled',
          status: 'disabled',
          effective_capture_mode: 'metadata_only',
        },
      ]);

      // Old Registry replicas remain live during a rolling update and know
      // nothing about the new setting tables. The migration trigger closes
      // that mixed-version window until a later release removes it.
      await pool.query(`
        INSERT INTO organizations (id, name)
        VALUES ('org_observability_mixed_writer', 'Mixed-version writer');
        INSERT INTO workspaces (id, organization_id, name, created_by)
        VALUES (
          'ws_observability_mixed_writer',
          'org_observability_mixed_writer',
          'Mixed-version workspace',
          'old-registry-replica'
        );
      `);
      const mixedVersionSettings = await pool.query<{
        organization_id: string;
        workspace_id: string | null;
        mode: string | null;
      }>(`
        SELECT organization_id, NULL::text AS workspace_id, NULL::text AS mode
        FROM agent_observability_organization_settings
        WHERE organization_id = 'org_observability_mixed_writer'
        UNION ALL
        SELECT organization_id, workspace_id, mode
        FROM agent_observability_workspace_settings
        WHERE workspace_id = 'ws_observability_mixed_writer'
        ORDER BY workspace_id NULLS FIRST
      `);
      expect(mixedVersionSettings.rows).toEqual([
        {
          organization_id: 'org_observability_mixed_writer',
          workspace_id: null,
          mode: null,
        },
        {
          organization_id: 'org_observability_mixed_writer',
          workspace_id: 'ws_observability_mixed_writer',
          mode: 'inherit',
        },
      ]);

      const secretValueColumns = await pool.query<{ table_name: string; column_name: string }>(`
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN (
            'agent_observability_platform_policy',
            'agent_observability_bindings',
            'agent_observability_binding_versions',
            'agent_observability_binding_credentials',
            'agent_observability_organization_settings',
            'agent_observability_workspace_settings',
            'session_observability_bindings'
          )
          AND column_name ~* '(password|token|authorization|header)'
        ORDER BY table_name, column_name
      `);
      expect(secretValueColumns.rows).toEqual([]);
      const credentialColumns = await pool.query<{ column_name: string }>(`
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'agent_observability_binding_credentials'
          AND column_name ~* 'secret'
        ORDER BY column_name
      `);
      expect(credentialColumns.rows).toEqual([{ column_name: 'secret_ref' }]);
      const credentialRows = await pool.query<{ count: string }>(`
        SELECT count(*) FROM agent_observability_binding_credentials
      `);
      expect(credentialRows.rows).toEqual([{ count: '0' }]);
    } finally {
      await closeTemporaryDatabase(pool, adminPool, databaseName);
      await rm(tempDir, { recursive: true, force: true });
    }
  }, 30_000);

  it('repairs post-0048 Sessions and preserves mixed-version authoritative pins', async () => {
    const databaseName = `registry_session_pin_repair_${process.pid}_${randomBytes(4).toString('hex')}`;
    const suffix = `${process.pid}_${randomBytes(4).toString('hex')}`;
    const organizationA = `org_session_pin_repair_a_${suffix}`;
    const organizationB = `org_session_pin_repair_b_${suffix}`;
    const workspaceA = `ws_session_pin_repair_a_${suffix}`;
    const workspaceB = `ws_session_pin_repair_b_${suffix}`;
    const agentA = `agt_session_pin_repair_a_${suffix}`;
    const agentB = `agt_session_pin_repair_b_${suffix}`;
    const missingSessionA = `ses_session_pin_repair_missing_a_${suffix}`;
    const missingSessionB = `ses_session_pin_repair_missing_b_${suffix}`;
    const preservedSession = `ses_session_pin_repair_preserved_${suffix}`;
    const preseededCurrentSession = `ses_session_pin_repair_current_${suffix}`;
    const gapLegacySession = `ses_session_pin_repair_gap_legacy_${suffix}`;
    const legacyWriterSession = `ses_session_pin_repair_legacy_${suffix}`;
    const workspaceBinding = `aob_session_pin_repair_${suffix}`;
    const sessionCreatedAt = new Date('2035-08-18T03:00:00.000Z');
    const sessionUpdatedAt = new Date('2035-08-18T04:00:00.000Z');
    const authoritativePinCreatedAt = new Date('2035-08-18T05:00:00.000Z');
    const authoritativePinUpdatedAt = new Date('2035-08-18T06:00:00.000Z');
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-session-pin-repair-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      const pre0049Folder = await createPre0049MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0049Folder });

      await pool.query(
        `INSERT INTO organizations (id, name) VALUES ($1, 'Session pin repair A'), ($2, 'Session pin repair B')`,
        [organizationA, organizationB],
      );
      await pool.query(
        `
          INSERT INTO workspaces (id, organization_id, name, created_by) VALUES
            ($1, $2, 'Session pin repair workspace A', 'test'),
            ($3, $4, 'Session pin repair workspace B', 'test')
        `,
        [workspaceA, organizationA, workspaceB, organizationB],
      );
      await pool.query(
        `
          INSERT INTO agents (id, workspace_id, name, model_provider, model_id) VALUES
            ($1, $2, 'Session pin repair agent A', 'anthropic', 'test'),
            ($3, $4, 'Session pin repair agent B', 'anthropic', 'test')
        `,
        [agentA, workspaceA, agentB, workspaceB],
      );
      await pool.query(
        `
          INSERT INTO agent_versions (id, workspace_id, agent_id, version, snapshot) VALUES
            ($1, $2, $3, 1, '{}'::jsonb),
            ($4, $5, $6, 1, '{}'::jsonb)
        `,
        [
          `agtver_session_pin_repair_a_${suffix}`,
          workspaceA,
          agentA,
          `agtver_session_pin_repair_b_${suffix}`,
          workspaceB,
          agentB,
        ],
      );
      await pool.query(
        `
          INSERT INTO sessions (id, workspace_id, agent_id, agent_version, created_at, updated_at) VALUES
            ($1, $2, $3, 1, $4, $5),
            ($6, $7, $8, 1, $4, $5),
            ($9, $7, $8, 1, $4, $5)
        `,
        [
          missingSessionA,
          workspaceA,
          agentA,
          sessionCreatedAt,
          sessionUpdatedAt,
          missingSessionB,
          workspaceB,
          agentB,
          preservedSession,
        ],
      );

      const bindingClient = await pool.connect();
      try {
        await bindingClient.query('BEGIN');
        await bindingClient.query(
          `
            INSERT INTO agent_observability_bindings (
              id, organization_id, workspace_id, scope_type, adapter_type, endpoint_kind, endpoint_class,
              endpoint, current_version, status, revocation_epoch, created_by, updated_by
            ) VALUES ($1, $2, $3, 'workspace', 'otlp_http', 'traces_endpoint', 'public',
                      'https://collector.example/v1/traces', 1, 'active', 0, 'test', 'test')
          `,
          [workspaceBinding, organizationB, workspaceB],
        );
        await bindingClient.query(
          `
            INSERT INTO agent_observability_binding_versions (
              binding_id, version, adapter_type, semantic_profile, protocol, compression, timeout_ms,
              capture_mode, sample_rate, config_schema_version, created_by
            ) VALUES ($1, 1, 'otlp_http', 'otel_genai', 'http/protobuf', 'none', 5000,
                      'metadata_only', 1, 1, 'test')
          `,
          [workspaceBinding],
        );
        await bindingClient.query(
          `
            INSERT INTO session_observability_bindings (
              workspace_id, session_id, organization_id, binding_id, binding_version, binding_scope,
              binding_workspace_id, selection_source, status, agent_id, agent_version, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, 1, 'workspace', $1, 'workspace_custom', 'active', $5, 1, $6, $7)
          `,
          [
            workspaceB,
            preservedSession,
            organizationB,
            workspaceBinding,
            agentB,
            authoritativePinCreatedAt,
            authoritativePinUpdatedAt,
          ],
        );
        await bindingClient.query('COMMIT');
      } catch (error) {
        await bindingClient.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        bindingClient.release();
      }

      const preRepairMissingPins = await pool.query<{ count: string }>(
        `
          SELECT count(*)
          FROM session_observability_bindings
          WHERE workspace_id IN ($1, $2)
            AND session_id IN ($3, $4)
        `,
        [workspaceA, workspaceB, missingSessionA, missingSessionB],
      );
      expect(preRepairMissingPins.rows).toEqual([{ count: '0' }]);

      const repairMigration = await readSessionPinRepairMigrationSections();
      const initialBackfillEnd = repairMigration.sql.indexOf(
        SESSION_PIN_REPAIR_INITIAL_BACKFILL_END,
      );
      const triggerInstallStart = repairMigration.sql.indexOf(
        SESSION_PIN_REPAIR_TRIGGER_INSTALL_START,
      );
      const triggerInstallEnd = repairMigration.sql.indexOf(SESSION_PIN_REPAIR_TRIGGER_INSTALL_END);
      const catchUpBackfillStart = repairMigration.sql.indexOf(
        SESSION_PIN_REPAIR_CATCH_UP_BACKFILL_START,
      );
      const createTrigger = repairMigration.sql.indexOf(
        'CREATE TRIGGER "agent_observability_sessions_provision_legacy_pin"',
      );
      expect(repairMigration.sql.slice(0, initialBackfillEnd)).not.toContain('CREATE TRIGGER');
      expect(initialBackfillEnd).toBeLessThan(triggerInstallStart);
      expect(triggerInstallStart).toBeLessThan(triggerInstallEnd);
      expect(triggerInstallEnd).toBeLessThan(catchUpBackfillStart);
      expect(createTrigger).toBeGreaterThan(triggerInstallStart);
      expect(createTrigger).toBeLessThan(triggerInstallEnd);
      expect(repairMigration.initialBackfill).toContain(
        'ON CONFLICT ("workspace_id", "session_id") DO NOTHING',
      );
      expect(repairMigration.catchUpBackfill).toContain(
        'ON CONFLICT ("workspace_id", "session_id") DO NOTHING',
      );
      expect(repairMigration.triggerInstall).toContain("SET LOCAL lock_timeout = '5s';");
      expect(repairMigration.triggerInstall).toContain('SET LOCAL lock_timeout = DEFAULT;');

      // Execute 0049's phases with a separate writer committing in the narrow
      // interval after pass one but before CREATE TRIGGER. The catch-up pass
      // must repair the legacy row and preserve a current writer's preseeded
      // authoritative pin once the trigger lock closes that interval.
      const repairClient = await pool.connect();
      try {
        const defaultLockTimeout = await repairClient.query<{ lock_timeout: string }>(
          'SHOW lock_timeout',
        );
        await repairClient.query('BEGIN');
        await repairClient.query(repairMigration.setup);
        await repairClient.query(repairMigration.initialBackfill);

        const gapWriter = await pool.connect();
        try {
          await gapWriter.query('BEGIN');
          await gapWriter.query(
            `
              INSERT INTO session_observability_bindings (
                workspace_id, session_id, organization_id, binding_id, binding_version, binding_scope,
                binding_workspace_id, selection_source, status, agent_id, agent_version, created_at, updated_at
              ) VALUES ($1, $2, $3, $4, 1, 'workspace', $1, 'workspace_custom', 'active', $5, 1, $6, $7)
            `,
            [
              workspaceB,
              preseededCurrentSession,
              organizationB,
              workspaceBinding,
              agentB,
              authoritativePinCreatedAt,
              authoritativePinUpdatedAt,
            ],
          );
          await gapWriter.query(
            `
              INSERT INTO sessions (id, workspace_id, agent_id, agent_version, created_at, updated_at)
              VALUES ($1, $2, $3, 1, $4, $5)
            `,
            [preseededCurrentSession, workspaceB, agentB, sessionCreatedAt, sessionUpdatedAt],
          );
          await gapWriter.query(
            `
              INSERT INTO sessions (id, workspace_id, agent_id, agent_version, created_at, updated_at)
              VALUES ($1, $2, $3, 1, $4, $5)
            `,
            [gapLegacySession, workspaceA, agentA, sessionCreatedAt, sessionUpdatedAt],
          );
          await gapWriter.query('COMMIT');
        } catch (error) {
          await gapWriter.query('ROLLBACK').catch(() => {});
          throw error;
        } finally {
          gapWriter.release();
        }

        await repairClient.query(repairMigration.triggerInstall);
        const lockTimeout = await repairClient.query<{ lock_timeout: string }>('SHOW lock_timeout');
        expect(lockTimeout.rows).toEqual(defaultLockTimeout.rows);
        await repairClient.query(repairMigration.catchUpBackfill);
        await repairClient.query('COMMIT');
      } catch (error) {
        await repairClient.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        repairClient.release();
      }

      // The manual phase run leaves no Drizzle ledger entry. Apply the exact
      // migration once to prove repeat-safe trigger/function DDL, then again
      // to retain the normal migrator idempotence coverage.
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });

      // A direct old-writer insert after trigger installation gets its own
      // disabled pin from the fallback rather than the catch-up query.
      await pool.query(
        `
          INSERT INTO sessions (id, workspace_id, agent_id, agent_version, created_at, updated_at)
          VALUES ($1, $2, $3, 1, $4, $5)
        `,
        [legacyWriterSession, workspaceA, agentA, sessionCreatedAt, sessionUpdatedAt],
      );

      await rerunSessionPinRepairBackfills(pool);
      await rerunSessionPinRepairBackfills(pool);

      const pins = await pool.query<{
        workspace_id: string;
        session_id: string;
        organization_id: string;
        binding_id: string | null;
        binding_version: number | null;
        binding_scope: string | null;
        binding_workspace_id: string | null;
        selection_source: string;
        status: string;
        effective_capture_mode: string;
        agent_id: string;
        agent_version: number;
        created_at: Date;
        updated_at: Date;
      }>(`
        SELECT workspace_id, session_id, organization_id, binding_id, binding_version, binding_scope,
               binding_workspace_id, selection_source, status, effective_capture_mode, agent_id,
               agent_version, created_at, updated_at
        FROM session_observability_bindings
        ORDER BY session_id
      `);
      const pinFor = (sessionId: string) => {
        const pin = pins.rows.find((row) => row.session_id === sessionId);
        expect(pin).toBeDefined();
        return pin!;
      };

      expect(pins.rows).toHaveLength(6);
      expect(pinFor(missingSessionA)).toEqual({
        workspace_id: workspaceA,
        session_id: missingSessionA,
        organization_id: organizationA,
        binding_id: null,
        binding_version: null,
        binding_scope: null,
        binding_workspace_id: null,
        selection_source: 'disabled',
        status: 'disabled',
        effective_capture_mode: 'metadata_only',
        agent_id: agentA,
        agent_version: 1,
        created_at: sessionCreatedAt,
        updated_at: sessionUpdatedAt,
      });
      expect(pinFor(missingSessionB)).toMatchObject({
        workspace_id: workspaceB,
        organization_id: organizationB,
        selection_source: 'disabled',
        status: 'disabled',
        agent_id: agentB,
        agent_version: 1,
        created_at: sessionCreatedAt,
        updated_at: sessionUpdatedAt,
      });
      expect(pinFor(preservedSession)).toMatchObject({
        workspace_id: workspaceB,
        organization_id: organizationB,
        binding_id: workspaceBinding,
        binding_version: 1,
        binding_scope: 'workspace',
        binding_workspace_id: workspaceB,
        selection_source: 'workspace_custom',
        status: 'active',
        created_at: authoritativePinCreatedAt,
        updated_at: authoritativePinUpdatedAt,
      });
      expect(pinFor(preseededCurrentSession)).toMatchObject({
        workspace_id: workspaceB,
        organization_id: organizationB,
        binding_id: workspaceBinding,
        binding_version: 1,
        binding_scope: 'workspace',
        binding_workspace_id: workspaceB,
        selection_source: 'workspace_custom',
        status: 'active',
        created_at: authoritativePinCreatedAt,
        updated_at: authoritativePinUpdatedAt,
      });
      expect(pinFor(gapLegacySession)).toEqual({
        workspace_id: workspaceA,
        session_id: gapLegacySession,
        organization_id: organizationA,
        binding_id: null,
        binding_version: null,
        binding_scope: null,
        binding_workspace_id: null,
        selection_source: 'disabled',
        status: 'disabled',
        effective_capture_mode: 'metadata_only',
        agent_id: agentA,
        agent_version: 1,
        created_at: sessionCreatedAt,
        updated_at: sessionUpdatedAt,
      });
      expect(pinFor(legacyWriterSession)).toEqual({
        workspace_id: workspaceA,
        session_id: legacyWriterSession,
        organization_id: organizationA,
        binding_id: null,
        binding_version: null,
        binding_scope: null,
        binding_workspace_id: null,
        selection_source: 'disabled',
        status: 'disabled',
        effective_capture_mode: 'metadata_only',
        agent_id: agentA,
        agent_version: 1,
        created_at: sessionCreatedAt,
        updated_at: sessionUpdatedAt,
      });

      const pinSecretOrEndpointColumns = await pool.query<{ column_name: string }>(`
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'session_observability_bindings'
          AND column_name ~* '(secret|endpoint|token|authorization|password|credential)'
        ORDER BY column_name
      `);
      expect(pinSecretOrEndpointColumns.rows).toEqual([]);
    } finally {
      await closeTemporaryDatabase(pool, adminPool, databaseName);
      await rm(tempDir, { recursive: true, force: true });
    }
  }, 30_000);

  it('rolls back pin repair when the sessions trigger lock times out, then retries cleanly', async () => {
    const databaseName = `registry_session_pin_lock_timeout_${process.pid}_${randomBytes(4).toString('hex')}`;
    const suffix = `${process.pid}_${randomBytes(4).toString('hex')}`;
    const organizationId = `org_session_pin_lock_timeout_${suffix}`;
    const workspaceId = `ws_session_pin_lock_timeout_${suffix}`;
    const agentId = `agt_session_pin_lock_timeout_${suffix}`;
    const missingSessionId = `ses_session_pin_lock_missing_${suffix}`;
    const blockerSessionId = `ses_session_pin_lock_blocker_${suffix}`;
    const legacySessionId = `ses_session_pin_lock_legacy_${suffix}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-session-pin-lock-timeout-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 3 });
      const pre0049Folder = await createPre0049MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0049Folder });

      await pool.query(
        `INSERT INTO organizations (id, name) VALUES ($1, 'Session pin lock timeout')`,
        [organizationId],
      );
      await pool.query(
        `
          INSERT INTO workspaces (id, organization_id, name, created_by)
          VALUES ($1, $2, 'Session pin lock timeout workspace', 'test')
        `,
        [workspaceId, organizationId],
      );
      await pool.query(
        `
          INSERT INTO agents (id, workspace_id, name, model_provider, model_id)
          VALUES ($1, $2, 'Session pin lock timeout agent', 'anthropic', 'test')
        `,
        [agentId, workspaceId],
      );
      await pool.query(
        `
          INSERT INTO agent_versions (id, workspace_id, agent_id, version, snapshot)
          VALUES ($1, $2, $3, 1, '{}'::jsonb)
        `,
        [`agtver_session_pin_lock_timeout_${suffix}`, workspaceId, agentId],
      );
      await pool.query(
        `
          INSERT INTO sessions (id, workspace_id, agent_id, agent_version)
          VALUES ($1, $2, $3, 1)
        `,
        [missingSessionId, workspaceId, agentId],
      );

      const repairMigration = await readSessionPinRepairMigrationSections();
      const blocker = await pool.connect();
      let blockerTransactionOpen = false;
      try {
        await blocker.query('BEGIN');
        blockerTransactionOpen = true;
        // INSERT holds a RowExclusiveLock on sessions until this transaction
        // ends, conflicting with CREATE TRIGGER's ShareRowExclusiveLock.
        await blocker.query(
          `
            INSERT INTO sessions (id, workspace_id, agent_id, agent_version)
            VALUES ($1, $2, $3, 1)
          `,
          [blockerSessionId, workspaceId, agentId],
        );

        const repair = await pool.connect();
        let repairTransactionOpen = false;
        try {
          await repair.query('BEGIN');
          repairTransactionOpen = true;
          await repair.query(repairMigration.setup);
          await repair.query(repairMigration.initialBackfill);

          const startedAt = Date.now();
          let lockError: unknown;
          try {
            await repair.query(repairMigration.triggerInstall);
          } catch (error) {
            lockError = error;
          }
          const elapsedMs = Date.now() - startedAt;
          await repair.query('ROLLBACK');
          repairTransactionOpen = false;

          expect(lockError).toMatchObject({ code: '55P03' });
          expect(elapsedMs).toBeGreaterThanOrEqual(4_000);
          expect(elapsedMs).toBeLessThan(15_000);

          const missingPin = await repair.query<{ count: string }>(
            `
              SELECT count(*)
              FROM session_observability_bindings
              WHERE workspace_id = $1 AND session_id = $2
            `,
            [workspaceId, missingSessionId],
          );
          const fallbackTrigger = await repair.query<{ exists: boolean }>(`
            SELECT EXISTS (
              SELECT 1
              FROM pg_trigger
              WHERE tgrelid = 'public.sessions'::regclass
                AND tgname = 'agent_observability_sessions_provision_legacy_pin'
                AND NOT tgisinternal
            ) AS exists
          `);
          const fallbackFunction = await repair.query<{ exists: boolean }>(`
            SELECT to_regprocedure(
              'public.agent_observability_provision_legacy_session_pin()'
            ) IS NOT NULL AS exists
          `);
          expect(missingPin.rows).toEqual([{ count: '0' }]);
          expect(fallbackTrigger.rows).toEqual([{ exists: false }]);
          expect(fallbackFunction.rows).toEqual([{ exists: false }]);
        } finally {
          if (repairTransactionOpen) await repair.query('ROLLBACK').catch(() => {});
          repair.release();
        }

        await blocker.query('COMMIT');
        blockerTransactionOpen = false;
      } finally {
        if (blockerTransactionOpen) await blocker.query('ROLLBACK').catch(() => {});
        blocker.release();
      }

      // The failed transaction wrote no Drizzle ledger entry, so normal retry
      // executes all 0049 phases after the blocker is gone.
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });

      const [repairedPin, fallbackTrigger] = await Promise.all([
        pool.query<{
          organization_id: string;
          selection_source: string;
          status: string;
          effective_capture_mode: string;
        }>(
          `
            SELECT organization_id, selection_source, status, effective_capture_mode
            FROM session_observability_bindings
            WHERE workspace_id = $1 AND session_id = $2
          `,
          [workspaceId, missingSessionId],
        ),
        pool.query<{ exists: boolean }>(`
          SELECT EXISTS (
            SELECT 1
            FROM pg_trigger
            WHERE tgrelid = 'public.sessions'::regclass
              AND tgname = 'agent_observability_sessions_provision_legacy_pin'
              AND NOT tgisinternal
          ) AS exists
        `),
      ]);
      expect(repairedPin.rows).toEqual([
        {
          organization_id: organizationId,
          selection_source: 'disabled',
          status: 'disabled',
          effective_capture_mode: 'metadata_only',
        },
      ]);
      expect(fallbackTrigger.rows).toEqual([{ exists: true }]);

      await pool.query(
        `
          INSERT INTO sessions (id, workspace_id, agent_id, agent_version)
          VALUES ($1, $2, $3, 1)
        `,
        [legacySessionId, workspaceId, agentId],
      );
      const legacyPin = await pool.query<{
        organization_id: string;
        selection_source: string;
        status: string;
        effective_capture_mode: string;
      }>(
        `
          SELECT organization_id, selection_source, status, effective_capture_mode
          FROM session_observability_bindings
          WHERE workspace_id = $1 AND session_id = $2
        `,
        [workspaceId, legacySessionId],
      );
      expect(legacyPin.rows).toEqual([
        {
          organization_id: organizationId,
          selection_source: 'disabled',
          status: 'disabled',
          effective_capture_mode: 'metadata_only',
        },
      ]);
    } finally {
      await closeTemporaryDatabase(pool, adminPool, databaseName);
      await rm(tempDir, { recursive: true, force: true });
    }
  }, 30_000);

  it('enforces legacy and current Session lifecycle writers on a fresh database', async () => {
    const databaseName = `registry_session_lifecycle_fresh_${process.pid}_${randomBytes(4).toString('hex')}`;
    const suffix = `${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      const fixture = await seedSessionLifecycleFixture(pool, suffix);

      const legacyArchiveSession = `ses_session_lifecycle_legacy_archive_${suffix}`;
      const firstArchiveAt = new Date('2035-08-18T04:00:00.000Z');
      const repeatArchiveAt = new Date('2035-08-18T05:00:00.000Z');
      await insertLegacySession(pool, fixture, legacyArchiveSession);
      await pool.query(
        `
          UPDATE sessions
          SET archived_at = $1, updated_at = $1
          WHERE workspace_id = $2 AND id = $3
        `,
        [firstArchiveAt, fixture.workspaceId, legacyArchiveSession],
      );
      expect(
        await loadSessionObservabilityPin(pool, fixture.workspaceId, legacyArchiveSession),
      ).toEqual({
        status: 'archived',
        archived_at: firstArchiveAt,
        deleted_at: null,
        updated_at: firstArchiveAt,
        session_revocation_epoch: '1',
      });
      await pool.query(
        `
          UPDATE sessions
          SET archived_at = $1, updated_at = $1
          WHERE workspace_id = $2 AND id = $3
        `,
        [repeatArchiveAt, fixture.workspaceId, legacyArchiveSession],
      );
      expect(
        await loadSessionObservabilityPin(pool, fixture.workspaceId, legacyArchiveSession),
      ).toEqual({
        status: 'archived',
        archived_at: firstArchiveAt,
        deleted_at: null,
        updated_at: firstArchiveAt,
        session_revocation_epoch: '1',
      });

      const legacyDeleteSession = `ses_session_lifecycle_legacy_delete_${suffix}`;
      const legacyDeletePeerSession = `ses_session_lifecycle_legacy_delete_peer_${suffix}`;
      await insertLegacySession(pool, fixture, legacyDeleteSession);
      await insertLegacySession(pool, fixture, legacyDeletePeerSession);
      await pool.query(`DELETE FROM sessions WHERE workspace_id = $1 AND id IN ($2, $3)`, [
        fixture.workspaceId,
        legacyDeleteSession,
        legacyDeletePeerSession,
      ]);
      const legacyDeletedPin = await loadSessionObservabilityPin(
        pool,
        fixture.workspaceId,
        legacyDeleteSession,
      );
      const legacyDeletedPeerPin = await loadSessionObservabilityPin(
        pool,
        fixture.workspaceId,
        legacyDeletePeerSession,
      );
      for (const pin of [legacyDeletedPin, legacyDeletedPeerPin]) {
        expect(pin).toMatchObject({
          status: 'deleted',
          archived_at: null,
          deleted_at: expect.any(Date),
          session_revocation_epoch: '1',
        });
        expect(pin?.updated_at).toEqual(pin?.deleted_at);
      }
      expect(legacyDeletedPeerPin?.deleted_at).toEqual(legacyDeletedPin?.deleted_at);

      const currentArchiveSession = `ses_session_lifecycle_current_archive_${suffix}`;
      const currentArchiveAt = new Date('2035-08-18T06:00:00.000Z');
      await insertLegacySession(pool, fixture, currentArchiveSession);
      const currentArchiveWriter = await pool.connect();
      try {
        await currentArchiveWriter.query('BEGIN');
        await currentArchiveWriter.query(
          `
            UPDATE session_observability_bindings
            SET status = 'archived',
                archived_at = $1,
                deleted_at = NULL,
                session_revocation_epoch = session_revocation_epoch + 1,
                updated_at = $1
            WHERE workspace_id = $2 AND session_id = $3
          `,
          [currentArchiveAt, fixture.workspaceId, currentArchiveSession],
        );
        await currentArchiveWriter.query(
          `
            UPDATE sessions
            SET archived_at = $1, updated_at = $1
            WHERE workspace_id = $2 AND id = $3
          `,
          [currentArchiveAt, fixture.workspaceId, currentArchiveSession],
        );
        await currentArchiveWriter.query('COMMIT');
      } catch (error) {
        await currentArchiveWriter.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        currentArchiveWriter.release();
      }
      expect(
        await loadSessionObservabilityPin(pool, fixture.workspaceId, currentArchiveSession),
      ).toEqual({
        status: 'archived',
        archived_at: currentArchiveAt,
        deleted_at: null,
        updated_at: currentArchiveAt,
        session_revocation_epoch: '1',
      });

      const currentDeleteSession = `ses_session_lifecycle_current_delete_${suffix}`;
      const currentDeleteAt = new Date('2035-08-18T07:00:00.000Z');
      await insertLegacySession(pool, fixture, currentDeleteSession);
      const currentDeleteWriter = await pool.connect();
      try {
        await currentDeleteWriter.query('BEGIN');
        await currentDeleteWriter.query(
          `
            UPDATE session_observability_bindings
            SET status = 'deleted',
                archived_at = NULL,
                deleted_at = $1,
                session_revocation_epoch = session_revocation_epoch + 1,
                updated_at = $1
            WHERE workspace_id = $2 AND session_id = $3
          `,
          [currentDeleteAt, fixture.workspaceId, currentDeleteSession],
        );
        await currentDeleteWriter.query(
          `DELETE FROM sessions WHERE workspace_id = $1 AND id = $2`,
          [fixture.workspaceId, currentDeleteSession],
        );
        await currentDeleteWriter.query('COMMIT');
      } catch (error) {
        await currentDeleteWriter.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        currentDeleteWriter.release();
      }
      expect(
        await loadSessionObservabilityPin(pool, fixture.workspaceId, currentDeleteSession),
      ).toEqual({
        status: 'deleted',
        archived_at: null,
        deleted_at: currentDeleteAt,
        updated_at: currentDeleteAt,
        session_revocation_epoch: '1',
      });

      const missingArchiveSession = `ses_session_lifecycle_missing_archive_${suffix}`;
      await insertLegacySession(pool, fixture, missingArchiveSession);
      await pool.query(
        `DELETE FROM session_observability_bindings WHERE workspace_id = $1 AND session_id = $2`,
        [fixture.workspaceId, missingArchiveSession],
      );
      await expect(
        pool.query(
          `
            UPDATE sessions
            SET archived_at = $1, updated_at = $1
            WHERE workspace_id = $2 AND id = $3
          `,
          [new Date('2035-08-18T08:00:00.000Z'), fixture.workspaceId, missingArchiveSession],
        ),
      ).rejects.toMatchObject({ code: '23514' });
      const missingArchiveRows = await pool.query<{ archived_at: Date | null }>(
        `SELECT archived_at FROM sessions WHERE workspace_id = $1 AND id = $2`,
        [fixture.workspaceId, missingArchiveSession],
      );
      expect(missingArchiveRows.rows).toEqual([{ archived_at: null }]);

      const missingDeleteSession = `ses_session_lifecycle_missing_delete_${suffix}`;
      await insertLegacySession(pool, fixture, missingDeleteSession);
      await pool.query(
        `DELETE FROM session_observability_bindings WHERE workspace_id = $1 AND session_id = $2`,
        [fixture.workspaceId, missingDeleteSession],
      );
      await expect(
        pool.query(`DELETE FROM sessions WHERE workspace_id = $1 AND id = $2`, [
          fixture.workspaceId,
          missingDeleteSession,
        ]),
      ).rejects.toMatchObject({ code: '23514' });
      const missingDeleteRows = await pool.query<{ count: string }>(
        `SELECT count(*) FROM sessions WHERE workspace_id = $1 AND id = $2`,
        [fixture.workspaceId, missingDeleteSession],
      );
      expect(missingDeleteRows.rows).toEqual([{ count: '1' }]);
    } finally {
      await closeTemporaryDatabase(pool, adminPool, databaseName);
    }
  }, 30_000);

  it('upgrades and catches up legacy archive/delete lifecycle pins without replacing tombstones', async () => {
    const databaseName = `registry_session_lifecycle_upgrade_${process.pid}_${randomBytes(4).toString('hex')}`;
    const suffix = `${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-session-lifecycle-upgrade-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 3 });
      const pre0054Folder = await createPre0054MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0054Folder });
      const fixture = await seedSessionLifecycleFixture(pool, suffix);

      const initialArchiveSession = `ses_session_lifecycle_initial_archive_${suffix}`;
      const initialArchiveAt = new Date('2035-08-18T04:00:00.000Z');
      await insertLegacySession(pool, fixture, initialArchiveSession);
      await pool.query(
        `
          UPDATE sessions
          SET archived_at = $1, updated_at = $1
          WHERE workspace_id = $2 AND id = $3
        `,
        [initialArchiveAt, fixture.workspaceId, initialArchiveSession],
      );

      const initialDeleteSession = `ses_session_lifecycle_initial_delete_${suffix}`;
      await insertLegacySession(pool, fixture, initialDeleteSession);
      await pool.query(`DELETE FROM sessions WHERE workspace_id = $1 AND id = $2`, [
        fixture.workspaceId,
        initialDeleteSession,
      ]);

      const preservedArchiveDeleteSession = `ses_session_lifecycle_preserved_delete_${suffix}`;
      const preservedArchiveAt = new Date('2035-08-18T05:00:00.000Z');
      await insertLegacySession(pool, fixture, preservedArchiveDeleteSession);
      await pool.query(
        `
          UPDATE session_observability_bindings
          SET status = 'archived',
              archived_at = $1,
              deleted_at = NULL,
              session_revocation_epoch = session_revocation_epoch + 1,
              updated_at = $1
          WHERE workspace_id = $2 AND session_id = $3
        `,
        [preservedArchiveAt, fixture.workspaceId, preservedArchiveDeleteSession],
      );
      await pool.query(`DELETE FROM sessions WHERE workspace_id = $1 AND id = $2`, [
        fixture.workspaceId,
        preservedArchiveDeleteSession,
      ]);

      const lifecycleMigration = await readSessionLifecycleEnforcementMigrationSections();
      const initialBackfillEnd = lifecycleMigration.sql.indexOf(
        SESSION_LIFECYCLE_ENFORCEMENT_INITIAL_BACKFILL_END,
      );
      const triggerInstallStart = lifecycleMigration.sql.indexOf(
        SESSION_LIFECYCLE_ENFORCEMENT_TRIGGER_INSTALL_START,
      );
      const triggerInstallEnd = lifecycleMigration.sql.indexOf(
        SESSION_LIFECYCLE_ENFORCEMENT_TRIGGER_INSTALL_END,
      );
      const catchUpBackfillStart = lifecycleMigration.sql.indexOf(
        SESSION_LIFECYCLE_ENFORCEMENT_CATCH_UP_BACKFILL_START,
      );
      const createTrigger = lifecycleMigration.sql.indexOf(
        'CREATE TRIGGER "agent_observability_sessions_enforce_lifecycle"',
      );
      expect(lifecycleMigration.sql.slice(0, initialBackfillEnd)).not.toContain('CREATE TRIGGER');
      expect(initialBackfillEnd).toBeLessThan(triggerInstallStart);
      expect(triggerInstallStart).toBeLessThan(triggerInstallEnd);
      expect(triggerInstallEnd).toBeLessThan(catchUpBackfillStart);
      expect(createTrigger).toBeGreaterThan(triggerInstallStart);
      expect(createTrigger).toBeLessThan(triggerInstallEnd);
      expect(lifecycleMigration.triggerInstall).toContain("SET LOCAL lock_timeout = '5s';");
      expect(lifecycleMigration.triggerInstall).toContain('SET LOCAL lock_timeout = DEFAULT;');

      const gapArchiveSession = `ses_session_lifecycle_gap_archive_${suffix}`;
      const gapDeleteSession = `ses_session_lifecycle_gap_delete_${suffix}`;
      const gapArchiveAt = new Date('2035-08-18T06:00:00.000Z');
      const repairClient = await pool.connect();
      try {
        const defaultLockTimeout = await repairClient.query<{ lock_timeout: string }>(
          'SHOW lock_timeout',
        );
        await repairClient.query('BEGIN');
        await repairClient.query(lifecycleMigration.setup);
        await repairClient.query(lifecycleMigration.initialBackfill);

        // Commit two old-writer lifecycle changes in the narrow window between
        // first pass and trigger installation. Catch-up must repair both.
        const gapWriter = await pool.connect();
        try {
          await gapWriter.query('BEGIN');
          await insertLegacySession(gapWriter, fixture, gapArchiveSession);
          await gapWriter.query(
            `
              UPDATE sessions
              SET archived_at = $1, updated_at = $1
              WHERE workspace_id = $2 AND id = $3
            `,
            [gapArchiveAt, fixture.workspaceId, gapArchiveSession],
          );
          await insertLegacySession(gapWriter, fixture, gapDeleteSession);
          await gapWriter.query(`DELETE FROM sessions WHERE workspace_id = $1 AND id = $2`, [
            fixture.workspaceId,
            gapDeleteSession,
          ]);
          await gapWriter.query('COMMIT');
        } catch (error) {
          await gapWriter.query('ROLLBACK').catch(() => {});
          throw error;
        } finally {
          gapWriter.release();
        }

        await repairClient.query(lifecycleMigration.triggerInstall);
        const lockTimeout = await repairClient.query<{ lock_timeout: string }>('SHOW lock_timeout');
        expect(lockTimeout.rows).toEqual(defaultLockTimeout.rows);
        await repairClient.query(lifecycleMigration.catchUpBackfill);
        await repairClient.query('COMMIT');
      } catch (error) {
        await repairClient.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        repairClient.release();
      }

      // Manual phase execution does not write Drizzle's ledger. Applying the
      // full migration twice proves normal upgrade/retry is idempotent too.
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      await rerunSessionLifecycleEnforcementBackfills(pool);
      await rerunSessionLifecycleEnforcementBackfills(pool);

      expect(
        await loadSessionObservabilityPin(pool, fixture.workspaceId, initialArchiveSession),
      ).toEqual({
        status: 'archived',
        archived_at: initialArchiveAt,
        deleted_at: null,
        updated_at: initialArchiveAt,
        session_revocation_epoch: '1',
      });
      const initialDeletedPin = await loadSessionObservabilityPin(
        pool,
        fixture.workspaceId,
        initialDeleteSession,
      );
      expect(initialDeletedPin).toMatchObject({
        status: 'deleted',
        archived_at: null,
        deleted_at: expect.any(Date),
        session_revocation_epoch: '1',
      });
      expect(initialDeletedPin?.updated_at).toEqual(initialDeletedPin?.deleted_at);
      const preservedDeletedPin = await loadSessionObservabilityPin(
        pool,
        fixture.workspaceId,
        preservedArchiveDeleteSession,
      );
      expect(preservedDeletedPin).toMatchObject({
        status: 'deleted',
        archived_at: preservedArchiveAt,
        deleted_at: expect.any(Date),
        session_revocation_epoch: '2',
      });

      expect(
        await loadSessionObservabilityPin(pool, fixture.workspaceId, gapArchiveSession),
      ).toMatchObject({
        status: 'archived',
        archived_at: gapArchiveAt,
        deleted_at: null,
        session_revocation_epoch: '1',
      });
      expect(
        await loadSessionObservabilityPin(pool, fixture.workspaceId, gapDeleteSession),
      ).toMatchObject({
        status: 'deleted',
        archived_at: null,
        deleted_at: expect.any(Date),
        session_revocation_epoch: '1',
      });
      const trigger = await pool.query<{ exists: boolean }>(`
        SELECT EXISTS (
          SELECT 1
          FROM pg_trigger
          WHERE tgrelid = 'public.sessions'::regclass
            AND tgname = 'agent_observability_sessions_enforce_lifecycle'
            AND NOT tgisinternal
        ) AS exists
      `);
      expect(trigger.rows).toEqual([{ exists: true }]);
    } finally {
      await closeTemporaryDatabase(pool, adminPool, databaseName);
      await rm(tempDir, { recursive: true, force: true });
    }
  }, 30_000);

  it('rolls back lifecycle enforcement when the sessions trigger lock times out, then retries cleanly', async () => {
    const databaseName = `registry_session_lifecycle_lock_timeout_${process.pid}_${randomBytes(4).toString('hex')}`;
    const suffix = `${process.pid}_${randomBytes(4).toString('hex')}`;
    const missingSession = `ses_session_lifecycle_lock_missing_${suffix}`;
    const blockerSession = `ses_session_lifecycle_lock_blocker_${suffix}`;
    const archiveAt = new Date('2035-08-18T04:00:00.000Z');
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-session-lifecycle-lock-timeout-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 3 });
      const pre0054Folder = await createPre0054MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0054Folder });
      const fixture = await seedSessionLifecycleFixture(pool, suffix);
      await insertLegacySession(pool, fixture, missingSession);
      await pool.query(
        `
          UPDATE sessions
          SET archived_at = $1, updated_at = $1
          WHERE workspace_id = $2 AND id = $3
        `,
        [archiveAt, fixture.workspaceId, missingSession],
      );

      const lifecycleMigration = await readSessionLifecycleEnforcementMigrationSections();
      const blocker = await pool.connect();
      let blockerTransactionOpen = false;
      try {
        await blocker.query('BEGIN');
        blockerTransactionOpen = true;
        // INSERT holds a RowExclusiveLock on sessions until transaction end,
        // conflicting with CREATE TRIGGER's ShareRowExclusiveLock.
        await insertLegacySession(blocker, fixture, blockerSession);

        const repair = await pool.connect();
        let repairTransactionOpen = false;
        try {
          await repair.query('BEGIN');
          repairTransactionOpen = true;
          await repair.query(lifecycleMigration.setup);
          await repair.query(lifecycleMigration.initialBackfill);

          const startedAt = Date.now();
          let lockError: unknown;
          try {
            await repair.query(lifecycleMigration.triggerInstall);
          } catch (error) {
            lockError = error;
          }
          const elapsedMs = Date.now() - startedAt;
          await repair.query('ROLLBACK');
          repairTransactionOpen = false;

          expect(lockError).toMatchObject({ code: '55P03' });
          expect(elapsedMs).toBeGreaterThanOrEqual(4_000);
          expect(elapsedMs).toBeLessThan(15_000);
        } finally {
          if (repairTransactionOpen) await repair.query('ROLLBACK').catch(() => {});
          repair.release();
        }

        expect(
          await loadSessionObservabilityPin(pool, fixture.workspaceId, missingSession),
        ).toEqual({
          status: 'disabled',
          archived_at: null,
          deleted_at: null,
          updated_at: new Date('2035-08-18T03:00:00.000Z'),
          session_revocation_epoch: '0',
        });
        const [trigger, lifecycleFunction] = await Promise.all([
          pool.query<{ exists: boolean }>(`
            SELECT EXISTS (
              SELECT 1
              FROM pg_trigger
              WHERE tgrelid = 'public.sessions'::regclass
                AND tgname = 'agent_observability_sessions_enforce_lifecycle'
                AND NOT tgisinternal
            ) AS exists
          `),
          pool.query<{ exists: boolean }>(`
            SELECT to_regprocedure(
              'public.agent_observability_enforce_session_pin_lifecycle()'
            ) IS NOT NULL AS exists
          `),
        ]);
        expect(trigger.rows).toEqual([{ exists: false }]);
        expect(lifecycleFunction.rows).toEqual([{ exists: false }]);

        await blocker.query('COMMIT');
        blockerTransactionOpen = false;
      } finally {
        if (blockerTransactionOpen) await blocker.query('ROLLBACK').catch(() => {});
        blocker.release();
      }

      // The failed transaction has no Drizzle ledger entry. Normal migration
      // retry runs every 0054 phase, repairs the legacy archive, and installs
      // the lifecycle guard.
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      expect(await loadSessionObservabilityPin(pool, fixture.workspaceId, missingSession)).toEqual({
        status: 'archived',
        archived_at: archiveAt,
        deleted_at: null,
        updated_at: archiveAt,
        session_revocation_epoch: '1',
      });
      const trigger = await pool.query<{ exists: boolean }>(`
        SELECT EXISTS (
          SELECT 1
          FROM pg_trigger
          WHERE tgrelid = 'public.sessions'::regclass
            AND tgname = 'agent_observability_sessions_enforce_lifecycle'
            AND NOT tgisinternal
        ) AS exists
      `);
      expect(trigger.rows).toEqual([{ exists: true }]);
    } finally {
      await closeTemporaryDatabase(pool, adminPool, databaseName);
      await rm(tempDir, { recursive: true, force: true });
    }
  }, 30_000);

  it('backfills durable workspace archive markers without timestamp inference', async () => {
    const databaseName = `registry_workspace_archive_marker_backfill_${process.pid}_${randomBytes(4).toString('hex')}`;
    const suffix = `${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-workspace-archive-marker-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 2 });
      const pre0056Folder = await createPre0056MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0056Folder });

      const explicitDisabled = await seedWorkspaceArchiveFixture(pool, `disabled_${suffix}`);
      const disabledSettingAt = new Date('2035-08-18T01:00:00.000Z');
      const disabledArchiveAt = new Date('2035-08-18T02:00:00.000Z');
      await pool.query(
        `
          UPDATE agent_observability_workspace_settings
          SET mode = 'disabled',
              binding_id = NULL,
              selection_epoch = 17,
              revocation_epoch = 41,
              capture_ceiling = 'metadata_only',
              capture_restriction_epoch = 23,
              updated_at = $1
          WHERE organization_id = $2 AND workspace_id = $3
        `,
        [disabledSettingAt, explicitDisabled.organizationId, explicitDisabled.workspaceId],
      );
      await pool.query(
        `
          UPDATE workspaces
          SET status = 'archived', archived_at = $1, updated_at = $1
          WHERE organization_id = $2 AND id = $3
        `,
        [disabledArchiveAt, explicitDisabled.organizationId, explicitDisabled.workspaceId],
      );

      // Regression for the timestamp-inference design: this legacy archive's
      // setting timestamp intentionally equals its archive timestamp, but its
      // epoch is still zero and needs one durable marker/increment.
      const timestampCollision = await seedWorkspaceArchiveFixture(pool, `collision_${suffix}`);
      const collisionArchiveAt = new Date('2035-08-18T03:00:00.000Z');
      await pool.query(
        `
          UPDATE agent_observability_workspace_settings
          SET updated_at = $1
          WHERE organization_id = $2 AND workspace_id = $3
        `,
        [collisionArchiveAt, timestampCollision.organizationId, timestampCollision.workspaceId],
      );
      await pool.query(
        `
          UPDATE workspaces
          SET status = 'archived', archived_at = $1, updated_at = $1
          WHERE organization_id = $2 AND id = $3
        `,
        [collisionArchiveAt, timestampCollision.organizationId, timestampCollision.workspaceId],
      );

      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      await rerunWorkspaceArchiveRevocationBackfills(pool);
      await rerunWorkspaceArchiveRevocationBackfills(pool);

      expect(await loadWorkspaceArchiveSetting(pool, explicitDisabled)).toMatchObject({
        mode: 'disabled',
        selection_epoch: '17',
        revocation_epoch: '42',
        capture_restriction_epoch: '23',
        updated_at: disabledArchiveAt,
      });
      expect(await loadWorkspaceArchiveRevocation(pool, explicitDisabled)).toMatchObject({
        organization_id: explicitDisabled.organizationId,
        workspace_id: explicitDisabled.workspaceId,
        archived_at: disabledArchiveAt,
        revocation_epoch: '42',
        created_at: expect.any(Date),
      });
      expect(await loadWorkspaceArchiveSetting(pool, timestampCollision)).toMatchObject({
        revocation_epoch: '1',
        updated_at: collisionArchiveAt,
      });
      expect(await loadWorkspaceArchiveRevocation(pool, timestampCollision)).toMatchObject({
        organization_id: timestampCollision.organizationId,
        workspace_id: timestampCollision.workspaceId,
        archived_at: collisionArchiveAt,
        revocation_epoch: '1',
        created_at: expect.any(Date),
      });
    } finally {
      await closeTemporaryDatabase(pool, adminPool, databaseName);
      await rm(tempDir, { recursive: true, force: true });
    }
  }, 30_000);

  it('backfills and fences exactly one workspace archive revocation across mixed-version writers', async () => {
    const databaseName = `registry_workspace_archive_revocation_${process.pid}_${randomBytes(4).toString('hex')}`;
    const suffix = `${process.pid}_${randomBytes(4).toString('hex')}`;
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-workspace-archive-revocation-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 3 });
      const pre0056Folder = await createPre0056MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0056Folder });

      const preexisting = await seedWorkspaceArchiveFixture(pool, `preexisting_${suffix}`);
      const preexistingSettingAt = new Date('2035-08-18T01:00:00.000Z');
      const preexistingArchiveAt = new Date('2035-08-18T02:00:00.000Z');
      await pool.query(
        `
          UPDATE agent_observability_workspace_settings
          SET mode = 'disabled',
              binding_id = NULL,
              selection_epoch = 17,
              revocation_epoch = 41,
              capture_ceiling = 'metadata_only',
              capture_restriction_epoch = 23,
              updated_at = $1
          WHERE organization_id = $2 AND workspace_id = $3
        `,
        [preexistingSettingAt, preexisting.organizationId, preexisting.workspaceId],
      );
      // This archive predates 0056, so its setting has no archive fence yet.
      await pool.query(
        `
          UPDATE workspaces
          SET status = 'archived', archived_at = $1, updated_at = $1
          WHERE organization_id = $2 AND id = $3
        `,
        [preexistingArchiveAt, preexisting.organizationId, preexisting.workspaceId],
      );

      const gap = await seedWorkspaceArchiveFixture(pool, `gap_${suffix}`);
      const gapArchiveAt = new Date('2035-08-18T03:00:00.000Z');
      const migration = await readWorkspaceArchiveRevocationMigrationSections();
      const initialBackfillEnd = migration.sql.indexOf(
        WORKSPACE_ARCHIVE_REVOCATION_INITIAL_BACKFILL_END,
      );
      const triggerInstallStart = migration.sql.indexOf(
        WORKSPACE_ARCHIVE_REVOCATION_TRIGGER_INSTALL_START,
      );
      const triggerInstallEnd = migration.sql.indexOf(
        WORKSPACE_ARCHIVE_REVOCATION_TRIGGER_INSTALL_END,
      );
      const catchUpBackfillStart = migration.sql.indexOf(
        WORKSPACE_ARCHIVE_REVOCATION_CATCH_UP_BACKFILL_START,
      );
      const createTrigger = migration.sql.indexOf(
        'CREATE TRIGGER "agent_observability_workspaces_enforce_archive_revocation"',
      );
      expect(migration.sql.slice(0, initialBackfillEnd)).not.toContain('CREATE TRIGGER');
      expect(initialBackfillEnd).toBeLessThan(triggerInstallStart);
      expect(triggerInstallStart).toBeLessThan(triggerInstallEnd);
      expect(triggerInstallEnd).toBeLessThan(catchUpBackfillStart);
      expect(createTrigger).toBeGreaterThan(triggerInstallStart);
      expect(createTrigger).toBeLessThan(triggerInstallEnd);
      expect(migration.triggerInstall).toContain("SET LOCAL lock_timeout = '5s';");
      expect(migration.triggerInstall).toContain('SET LOCAL lock_timeout = DEFAULT;');
      expect(migration.triggerInstall).toContain(
        'agent_observability_workspace_archive_revocations_workspace_fk',
      );
      expect(migration.triggerInstall).toContain('NOT VALID');
      expect(migration.schemaSetup).not.toContain(
        'agent_observability_workspace_archive_revocations_workspace_fk',
      );
      expect(
        migration.triggerInstall.indexOf(
          'agent_observability_workspace_archive_revocations_workspace_fk',
        ),
      ).toBeGreaterThan(migration.triggerInstall.indexOf("SET LOCAL lock_timeout = '5s';"));

      // Execute 0056 in phases. An old writer commits after initial backfill
      // but before CREATE TRIGGER; catch-up must account for it while the
      // trigger lock closes all later direct-write gaps.
      const migrationClient = await pool.connect();
      let migrationTransactionOpen = false;
      try {
        const defaultLockTimeout = await migrationClient.query<{ lock_timeout: string }>(
          'SHOW lock_timeout',
        );
        await migrationClient.query('BEGIN');
        migrationTransactionOpen = true;
        // Table creation itself takes no parent-table lock. Keep it inside the
        // real migration transaction so the old writer below exercises the
        // same initial-backfill/trigger-install window as production.
        await migrationClient.query(migration.schemaSetup);
        await migrationClient.query(migration.setup);
        await migrationClient.query(migration.initialBackfill);
        expect(await loadWorkspaceArchiveSetting(migrationClient, preexisting)).toEqual({
          organization_id: preexisting.organizationId,
          workspace_id: preexisting.workspaceId,
          mode: 'disabled',
          binding_id: null,
          selection_epoch: '17',
          revocation_epoch: '42',
          capture_ceiling: 'metadata_only',
          capture_restriction_epoch: '23',
          updated_at: preexistingArchiveAt,
        });
        expect(await loadWorkspaceArchiveRevocation(migrationClient, preexisting)).toMatchObject({
          organization_id: preexisting.organizationId,
          workspace_id: preexisting.workspaceId,
          archived_at: preexistingArchiveAt,
          revocation_epoch: '42',
          created_at: expect.any(Date),
        });

        const gapWriter = await pool.connect();
        try {
          await gapWriter.query('BEGIN');
          await gapWriter.query(
            `
              UPDATE workspaces
              SET status = 'archived', archived_at = $1, updated_at = $1
              WHERE organization_id = $2 AND id = $3
            `,
            [gapArchiveAt, gap.organizationId, gap.workspaceId],
          );
          await gapWriter.query('COMMIT');
        } catch (error) {
          await gapWriter.query('ROLLBACK').catch(() => {});
          throw error;
        } finally {
          gapWriter.release();
        }

        await migrationClient.query(migration.triggerInstall);
        expect(await migrationClient.query<{ lock_timeout: string }>('SHOW lock_timeout')).toEqual(
          defaultLockTimeout,
        );
        await migrationClient.query(migration.catchUpBackfill);
        expect(await loadWorkspaceArchiveSetting(migrationClient, gap)).toMatchObject({
          revocation_epoch: '1',
          updated_at: gapArchiveAt,
        });
        expect(await loadWorkspaceArchiveRevocation(migrationClient, gap)).toMatchObject({
          organization_id: gap.organizationId,
          workspace_id: gap.workspaceId,
          archived_at: gapArchiveAt,
          revocation_epoch: '1',
          created_at: expect.any(Date),
        });
        await migrationClient.query('COMMIT');
        migrationTransactionOpen = false;
      } catch (error) {
        if (migrationTransactionOpen) await migrationClient.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        migrationClient.release();
      }

      // Manual phases create the table but deliberately do not create a
      // Drizzle ledger entry. Re-run both marker backfills directly to prove
      // they do not advance an already-recorded archive a second time.
      await rerunWorkspaceArchiveRevocationBackfills(pool);
      await rerunWorkspaceArchiveRevocationBackfills(pool);
      expect(await loadWorkspaceArchiveSetting(pool, preexisting)).toMatchObject({
        mode: 'disabled',
        selection_epoch: '17',
        revocation_epoch: '42',
        capture_restriction_epoch: '23',
        updated_at: preexistingArchiveAt,
      });
      expect(await loadWorkspaceArchiveSetting(pool, gap)).toMatchObject({
        revocation_epoch: '1',
        updated_at: gapArchiveAt,
      });
      expect(await loadWorkspaceArchiveRevocation(pool, preexisting)).toMatchObject({
        archived_at: preexistingArchiveAt,
        revocation_epoch: '42',
      });
      expect(await loadWorkspaceArchiveRevocation(pool, gap)).toMatchObject({
        archived_at: gapArchiveAt,
        revocation_epoch: '1',
      });
      const trigger = await pool.query<{ exists: boolean }>(`
        SELECT EXISTS (
          SELECT 1
          FROM pg_trigger
          WHERE tgrelid = 'public.workspaces'::regclass
            AND tgname = 'agent_observability_workspaces_enforce_archive_revocation'
            AND NOT tgisinternal
        ) AS exists
      `);
      expect(trigger.rows).toEqual([{ exists: true }]);

      // A direct old writer is fenced by the trigger after installation.
      const oldWriter = await seedWorkspaceArchiveFixture(pool, `old_writer_${suffix}`);
      const oldWriterArchiveAt = new Date('2035-08-18T04:00:00.000Z');
      await pool.query(
        `
          UPDATE workspaces
          SET status = 'archived', archived_at = $1, updated_at = $1
          WHERE organization_id = $2 AND id = $3
        `,
        [oldWriterArchiveAt, oldWriter.organizationId, oldWriter.workspaceId],
      );
      expect(await loadWorkspaceArchiveSetting(pool, oldWriter)).toMatchObject({
        revocation_epoch: '1',
        updated_at: oldWriterArchiveAt,
      });
      expect(await loadWorkspaceArchiveRevocation(pool, oldWriter)).toMatchObject({
        organization_id: oldWriter.organizationId,
        workspace_id: oldWriter.workspaceId,
        archived_at: oldWriterArchiveAt,
        revocation_epoch: '1',
      });

      // Current writers insert a durable marker and CAS their setting before
      // Workspace status. The trigger validates that marker rather than
      // inferring state from timestamps, so it cannot double increment.
      const currentWriter = await seedWorkspaceArchiveFixture(pool, `current_writer_${suffix}`);
      const currentWriterArchiveAt = new Date('2035-08-18T05:00:00.000Z');
      const currentWriterClient = await pool.connect();
      try {
        await currentWriterClient.query('BEGIN');
        const marker = await currentWriterClient.query<{
          workspace_id: string;
          revocation_epoch: string;
        }>(
          `
            INSERT INTO agent_observability_workspace_archive_revocations (
              workspace_id, organization_id, archived_at, revocation_epoch
            ) VALUES ($1, $2, $3, 1)
            RETURNING workspace_id, revocation_epoch
          `,
          [currentWriter.workspaceId, currentWriter.organizationId, currentWriterArchiveAt],
        );
        expect(marker.rows).toEqual([
          { workspace_id: currentWriter.workspaceId, revocation_epoch: '1' },
        ]);
        const advanced = await currentWriterClient.query<{ workspace_id: string }>(
          `
            UPDATE agent_observability_workspace_settings
            SET revocation_epoch = $1, updated_at = $2
            WHERE organization_id = $3
              AND workspace_id = $4
              AND revocation_epoch = 0
            RETURNING workspace_id
          `,
          [
            marker.rows[0]!.revocation_epoch,
            currentWriterArchiveAt,
            currentWriter.organizationId,
            currentWriter.workspaceId,
          ],
        );
        expect(advanced.rows).toEqual([{ workspace_id: currentWriter.workspaceId }]);
        await currentWriterClient.query(
          `
            UPDATE workspaces
            SET status = 'archived', archived_at = $1, updated_at = $1
            WHERE organization_id = $2 AND id = $3
          `,
          [currentWriterArchiveAt, currentWriter.organizationId, currentWriter.workspaceId],
        );
        await currentWriterClient.query('COMMIT');
      } catch (error) {
        await currentWriterClient.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        currentWriterClient.release();
      }
      const currentSetting = await loadWorkspaceArchiveSetting(pool, currentWriter);
      expect(currentSetting).toMatchObject({
        revocation_epoch: '1',
        updated_at: currentWriterArchiveAt,
      });
      const currentMarker = await loadWorkspaceArchiveRevocation(pool, currentWriter);
      expect(currentMarker).toMatchObject({
        organization_id: currentWriter.organizationId,
        workspace_id: currentWriter.workspaceId,
        archived_at: currentWriterArchiveAt,
        revocation_epoch: '1',
      });
      await pool.query(
        `
          UPDATE workspaces
          SET status = 'archived', archived_at = $1, updated_at = $1
          WHERE organization_id = $2 AND id = $3
        `,
        [currentWriterArchiveAt, currentWriter.organizationId, currentWriter.workspaceId],
      );
      expect(await loadWorkspaceArchiveSetting(pool, currentWriter)).toEqual(currentSetting);
      expect(await loadWorkspaceArchiveRevocation(pool, currentWriter)).toEqual(currentMarker);

      const mismatchedMarker = await seedWorkspaceArchiveFixture(pool, `mismatch_${suffix}`);
      const markerArchiveAt = new Date('2035-08-18T06:00:00.000Z');
      await pool.query(
        `
          UPDATE agent_observability_workspace_settings
          SET revocation_epoch = 1
          WHERE organization_id = $1 AND workspace_id = $2
        `,
        [mismatchedMarker.organizationId, mismatchedMarker.workspaceId],
      );
      await pool.query(
        `
          INSERT INTO agent_observability_workspace_archive_revocations (
            workspace_id, organization_id, archived_at, revocation_epoch
          ) VALUES ($1, $2, $3, 1)
        `,
        [mismatchedMarker.workspaceId, mismatchedMarker.organizationId, markerArchiveAt],
      );
      await expect(
        pool.query(
          `
            UPDATE workspaces
            SET status = 'archived', archived_at = $1, updated_at = $1
            WHERE organization_id = $2 AND id = $3
          `,
          [
            new Date('2035-08-18T06:00:01.000Z'),
            mismatchedMarker.organizationId,
            mismatchedMarker.workspaceId,
          ],
        ),
      ).rejects.toMatchObject({ code: '23514' });

      const missingSetting = await seedWorkspaceArchiveFixture(pool, `missing_setting_${suffix}`);
      await pool.query(
        `
          DELETE FROM agent_observability_workspace_settings
          WHERE organization_id = $1 AND workspace_id = $2
        `,
        [missingSetting.organizationId, missingSetting.workspaceId],
      );
      await expect(
        pool.query(
          `
            UPDATE workspaces
            SET status = 'archived', archived_at = $1, updated_at = $1
            WHERE organization_id = $2 AND id = $3
          `,
          [
            new Date('2035-08-18T06:00:00.000Z'),
            missingSetting.organizationId,
            missingSetting.workspaceId,
          ],
        ),
      ).rejects.toMatchObject({ code: '23514' });
      expect(
        (
          await pool.query<{ status: string; archived_at: Date | null }>(
            `SELECT status, archived_at FROM workspaces WHERE organization_id = $1 AND id = $2`,
            [missingSetting.organizationId, missingSetting.workspaceId],
          )
        ).rows,
      ).toEqual([{ status: 'active', archived_at: null }]);

      const overflow = await seedWorkspaceArchiveFixture(pool, `overflow_${suffix}`);
      await pool.query(
        `
          UPDATE agent_observability_workspace_settings
          SET revocation_epoch = 9007199254740991
          WHERE organization_id = $1 AND workspace_id = $2
        `,
        [overflow.organizationId, overflow.workspaceId],
      );
      await expect(
        pool.query(
          `
            UPDATE workspaces
            SET status = 'archived', archived_at = $1, updated_at = $1
            WHERE organization_id = $2 AND id = $3
          `,
          [new Date('2035-08-18T07:00:00.000Z'), overflow.organizationId, overflow.workspaceId],
        ),
      ).rejects.toMatchObject({ code: '23514' });

      const corrupt = await seedWorkspaceArchiveFixture(pool, `corrupt_${suffix}`);
      await pool.query(
        `
          UPDATE agent_observability_workspace_settings
          SET selection_epoch = 9007199254740992
          WHERE organization_id = $1 AND workspace_id = $2
        `,
        [corrupt.organizationId, corrupt.workspaceId],
      );
      await expect(
        pool.query(
          `
            UPDATE workspaces
            SET status = 'archived', archived_at = $1, updated_at = $1
            WHERE organization_id = $2 AND id = $3
          `,
          [new Date('2035-08-18T08:00:00.000Z'), corrupt.organizationId, corrupt.workspaceId],
        ),
      ).rejects.toMatchObject({ code: '23514' });
    } finally {
      await closeTemporaryDatabase(pool, adminPool, databaseName);
      await rm(tempDir, { recursive: true, force: true });
    }
  }, 30_000);

  it('bounds Workspace trigger and ownership-FK lock acquisition before retrying cleanly', async () => {
    const databaseName = `registry_workspace_archive_lock_timeout_${process.pid}_${randomBytes(4).toString('hex')}`;
    const suffix = `${process.pid}_${randomBytes(4).toString('hex')}`;
    const archiveAt = new Date('2035-08-18T09:00:00.000Z');
    const adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    const tempDir = await mkdtemp(join(tmpdir(), 'orca-registry-workspace-archive-lock-timeout-'));
    let pool: Pool | undefined;

    try {
      await adminPool.query(`CREATE DATABASE "${databaseName}"`);
      pool = new Pool({ connectionString: databaseUrl.toString(), max: 3 });
      const pre0056Folder = await createPre0056MigrationsFolder(tempDir);
      await migrate(drizzle(pool), { migrationsFolder: pre0056Folder });
      const archived = await seedWorkspaceArchiveFixture(pool, `archived_${suffix}`);
      const blockerFixture = await seedWorkspaceArchiveFixture(pool, `blocker_${suffix}`);
      await pool.query(
        `
          UPDATE workspaces
          SET status = 'archived', archived_at = $1, updated_at = $1
          WHERE organization_id = $2 AND id = $3
        `,
        [archiveAt, archived.organizationId, archived.workspaceId],
      );

      const migration = await readWorkspaceArchiveRevocationMigrationSections();
      const expectInstallLockTimeout = async (): Promise<void> => {
        const repair = await pool.connect();
        let repairTransactionOpen = false;
        try {
          await repair.query('BEGIN');
          repairTransactionOpen = true;
          await repair.query(migration.schemaSetup);
          await repair.query(migration.setup);
          await repair.query(migration.initialBackfill);

          const startedAt = Date.now();
          let lockError: unknown;
          try {
            await repair.query(migration.triggerInstall);
          } catch (error) {
            lockError = error;
          }
          const elapsedMs = Date.now() - startedAt;
          await repair.query('ROLLBACK');
          repairTransactionOpen = false;

          expect(lockError).toMatchObject({ code: '55P03' });
          expect(elapsedMs).toBeGreaterThanOrEqual(4_000);
          expect(elapsedMs).toBeLessThan(15_000);
        } finally {
          if (repairTransactionOpen) await repair.query('ROLLBACK').catch(() => {});
          repair.release();
        }
      };

      const workspaceBlocker = await pool.connect();
      let workspaceBlockerTransactionOpen = false;
      try {
        await workspaceBlocker.query('BEGIN');
        workspaceBlockerTransactionOpen = true;
        // This RowExclusiveLock conflicts with CREATE TRIGGER's
        // ShareRowExclusiveLock but does not block the archived fixture row
        // that the initial pass locks.
        await workspaceBlocker.query(
          `
            UPDATE workspaces
            SET updated_at = updated_at
            WHERE organization_id = $1 AND id = $2
          `,
          [blockerFixture.organizationId, blockerFixture.workspaceId],
        );
        await expectInstallLockTimeout();
        await workspaceBlocker.query('COMMIT');
        workspaceBlockerTransactionOpen = false;
      } finally {
        if (workspaceBlockerTransactionOpen)
          await workspaceBlocker.query('ROLLBACK').catch(() => {});
        workspaceBlocker.release();
      }

      const settingBlocker = await pool.connect();
      let settingBlockerTransactionOpen = false;
      try {
        await settingBlocker.query('BEGIN');
        settingBlockerTransactionOpen = true;
        // CREATE TRIGGER now succeeds, then the ownership FK's parent-setting
        // lock must obey the same five-second bound.
        await settingBlocker.query(
          `
            UPDATE agent_observability_workspace_settings
            SET updated_at = updated_at
            WHERE organization_id = $1 AND workspace_id = $2
          `,
          [blockerFixture.organizationId, blockerFixture.workspaceId],
        );
        await expectInstallLockTimeout();
        await settingBlocker.query('COMMIT');
        settingBlockerTransactionOpen = false;
      } finally {
        if (settingBlockerTransactionOpen) await settingBlocker.query('ROLLBACK').catch(() => {});
        settingBlocker.release();
      }

      expect(await loadWorkspaceArchiveSetting(pool, archived)).toMatchObject({
        revocation_epoch: '0',
      });
      const [markerTable, trigger, workspaceFunction] = await Promise.all([
        pool.query<{ exists: boolean }>(`
          SELECT to_regclass(
            'public.agent_observability_workspace_archive_revocations'
          ) IS NOT NULL AS exists
        `),
        pool.query<{ exists: boolean }>(`
          SELECT EXISTS (
            SELECT 1
            FROM pg_trigger
            WHERE tgrelid = 'public.workspaces'::regclass
              AND tgname = 'agent_observability_workspaces_enforce_archive_revocation'
              AND NOT tgisinternal
          ) AS exists
        `),
        pool.query<{ exists: boolean }>(`
          SELECT to_regprocedure(
            'public.agent_observability_enforce_workspace_archive_revocation()'
          ) IS NOT NULL AS exists
        `),
      ]);
      expect(markerTable.rows).toEqual([{ exists: false }]);
      expect(trigger.rows).toEqual([{ exists: false }]);
      expect(workspaceFunction.rows).toEqual([{ exists: false }]);

      // The failed transaction has no migration ledger entry. A normal retry
      // creates the marker and advances this historical archive exactly once.
      await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
      expect(await loadWorkspaceArchiveSetting(pool, archived)).toMatchObject({
        revocation_epoch: '1',
        updated_at: archiveAt,
      });
      expect(await loadWorkspaceArchiveRevocation(pool, archived)).toMatchObject({
        organization_id: archived.organizationId,
        workspace_id: archived.workspaceId,
        archived_at: archiveAt,
        revocation_epoch: '1',
      });
    } finally {
      await closeTemporaryDatabase(pool, adminPool, databaseName);
      await rm(tempDir, { recursive: true, force: true });
    }
  }, 40_000);
});
