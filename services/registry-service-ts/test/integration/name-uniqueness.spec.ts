// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import { and, eq, inArray, like } from 'drizzle-orm';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  partialAdminApiKeyHint,
} from '../../src/auth/admin-api-key.js';
import {
  fingerprintPlatformApiKey,
  generatePlatformApiKey,
  hashPlatformApiKey,
  partialPlatformApiKeyHint,
} from '../../src/auth/platform-api-key.js';
import { buildAdminApp } from '../../src/server.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  adminApiKeys,
  adminAuditEvents,
  agentObservabilityOrganizationSettings,
  agentObservabilityWorkspaceSettings,
  organizations,
  platformApiKeys,
  platformAuditEvents,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import { buildStubStore, closeTestDb, getTestDb } from './setup.js';

describe('organization and workspace name uniqueness', () => {
  let db: DbClient;
  let pool: Pool;
  let app: FastifyInstance | undefined;
  let platformKey: string;
  let adminKey: string;
  const suffix = randomUUID();
  const prefix = `Name uniqueness ${suffix}`;
  const organizationId = `org_name_unique_${suffix}`;
  const otherOrganizationId = `org_name_unique_other_${suffix}`;
  const organizationIds = [organizationId, otherOrganizationId];
  const platformKeyId = `platformkey_name_unique_${suffix}`;
  const adminKeyId = `adminkey_name_unique_${suffix}`;
  const organizationConflict = { error: 'organization name already exists' };
  const workspaceConflict = { error: 'workspace name already exists in this organization' };

  beforeAll(async () => {
    ({ db, pool } = await getTestDb());
    platformKey = generatePlatformApiKey();
    adminKey = generateAdminApiKey();
    await db.insert(organizations).values([
      { id: organizationId, name: `${prefix} seed`, status: 'active' },
      { id: otherOrganizationId, name: `${prefix} other seed`, status: 'active' },
    ]);
    await db.insert(platformApiKeys).values({
      id: platformKeyId,
      name: 'Name uniqueness platform key',
      hashedKey: await hashPlatformApiKey(platformKey),
      keyFingerprint: fingerprintPlatformApiKey(platformKey),
      partialKeyHint: partialPlatformApiKeyHint(platformKey),
      scopes: ['platform:admin'],
      status: 'active',
      createdBy: 'integration-test',
    });
    await db.insert(adminApiKeys).values({
      id: adminKeyId,
      organizationId,
      name: 'Name uniqueness organization key',
      hashedKey: await hashAdminApiKey(adminKey),
      keyFingerprint: fingerprintAdminApiKey(adminKey),
      partialKeyHint: partialAdminApiKeyHint(adminKey),
      scopes: ['org:admin'],
      status: 'active',
      createdBy: 'integration-test',
    });
    app = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
    });
    await app.ready();
  });

  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  function createOrganization(name: string, idempotencyKey?: string) {
    return app!.inject({
      method: 'POST',
      url: '/v1/platform/organizations',
      headers: {
        'x-api-key': platformKey,
        ...(idempotencyKey ? { 'idempotency-key': idempotencyKey } : {}),
      },
      payload: { name },
    });
  }

  function createPlatformWorkspace(
    name: string,
    targetOrganizationId = organizationId,
    idempotencyKey?: string,
  ) {
    return app!.inject({
      method: 'POST',
      url: `/v1/platform/organizations/${targetOrganizationId}/workspaces`,
      headers: {
        'x-api-key': platformKey,
        ...(idempotencyKey ? { 'idempotency-key': idempotencyKey } : {}),
      },
      payload: { name },
    });
  }

  function createAdminWorkspace(name: string) {
    return app!.inject({
      method: 'POST',
      url: '/v1/organizations/workspaces',
      headers: { 'x-api-key': adminKey },
      payload: { name },
    });
  }

  function renameWorkspace(workspaceId: string, name: string) {
    return app!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${workspaceId}`,
      headers: { 'x-api-key': adminKey },
      payload: { name },
    });
  }

  // Limit snapshots to this suite's fixtures so unrelated integration data
  // cannot hide a partial write or manufacture a spurious difference.
  async function mutationState() {
    const [
      organizationRows,
      workspaceRows,
      organizationSettings,
      workspaceSettings,
      platformAudit,
      adminAudit,
    ] = await Promise.all([
      db
        .select()
        .from(organizations)
        .where(like(organizations.name, `${prefix}%`))
        .orderBy(organizations.id),
      db
        .select()
        .from(workspaces)
        .where(inArray(workspaces.organizationId, organizationIds))
        .orderBy(workspaces.id),
      db
        .select({ organizationId: agentObservabilityOrganizationSettings.organizationId })
        .from(agentObservabilityOrganizationSettings)
        .innerJoin(
          organizations,
          eq(organizations.id, agentObservabilityOrganizationSettings.organizationId),
        )
        .where(like(organizations.name, `${prefix}%`))
        .orderBy(agentObservabilityOrganizationSettings.organizationId),
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(inArray(agentObservabilityWorkspaceSettings.organizationId, organizationIds))
        .orderBy(agentObservabilityWorkspaceSettings.workspaceId),
      db
        .select()
        .from(platformAuditEvents)
        .where(eq(platformAuditEvents.actor, `platform-api-key:${platformKeyId}`))
        .orderBy(platformAuditEvents.id),
      db
        .select()
        .from(adminAuditEvents)
        .where(eq(adminAuditEvents.actor, `admin-api-key:${adminKeyId}`))
        .orderBy(adminAuditEvents.id),
    ]);
    return {
      organizationRows,
      workspaceRows,
      organizationSettings,
      workspaceSettings,
      platformAudit,
      adminAudit,
    };
  }

  it('rejects duplicate organization names after trimming without settings or success audit writes', async () => {
    const name = `${prefix} duplicate organization`;
    const created = await createOrganization(`  ${name}  `);
    expect(created.statusCode, created.body).toBe(201);
    expect(created.json().name).toBe(name);
    const before = await mutationState();

    const duplicate = await createOrganization(name);
    expect(duplicate.statusCode, duplicate.body).toBe(409);
    expect(duplicate.json()).toEqual(organizationConflict);
    expect(await mutationState()).toEqual(before);
  });

  it('reserves archived organization names', async () => {
    const name = `${prefix} archived organization`;
    const created = await createOrganization(name);
    expect(created.statusCode, created.body).toBe(201);
    await db
      .update(organizations)
      .set({ status: 'archived' })
      .where(eq(organizations.id, created.json().id));
    const before = await mutationState();

    const duplicate = await createOrganization(name);
    expect(duplicate.statusCode, duplicate.body).toBe(409);
    expect(duplicate.json()).toEqual(organizationConflict);
    expect(await mutationState()).toEqual(before);
  });

  it('preserves case-sensitive organization and workspace names', async () => {
    const results = await Promise.all([
      createOrganization(`${prefix} Case`),
      createOrganization(`${prefix} case`),
      createPlatformWorkspace(`${prefix} Case`),
      createAdminWorkspace(`${prefix} case`),
    ]);
    expect(results.map((result) => result.statusCode)).toEqual([201, 201, 201, 200]);
    expect(new Set(results.map((result) => result.json().id)).size).toBe(4);
  });

  it.each(['platform', 'admin'] as const)(
    'rejects duplicate workspace creation by %s after trimming without secondary writes',
    async (writer) => {
      const name = `${prefix} duplicate workspace ${writer}`;
      const created = await createPlatformWorkspace(`  ${name}  `);
      expect(created.statusCode, created.body).toBe(201);
      expect(created.json().name).toBe(name);
      const before = await mutationState();

      const duplicate = await (writer === 'platform'
        ? createPlatformWorkspace(name)
        : createAdminWorkspace(`  ${name}  `));
      expect(duplicate.statusCode, duplicate.body).toBe(409);
      expect(duplicate.json()).toEqual(workspaceConflict);
      expect(await mutationState()).toEqual(before);
    },
  );

  it('allows the same workspace name in different organizations', async () => {
    const name = `${prefix} shared workspace`;
    const [first, other] = await Promise.all([
      createAdminWorkspace(name),
      createPlatformWorkspace(name, otherOrganizationId),
    ]);
    expect(first.statusCode, first.body).toBe(200);
    expect(other.statusCode, other.body).toBe(201);
    expect(first.json().id).not.toBe(other.json().id);
    const rows = await db.select().from(workspaces).where(eq(workspaces.name, name));
    expect(rows.map((row) => row.organizationId).sort()).toEqual([...organizationIds].sort());
  });

  it('reserves archived workspace names for both create routes and rename', async () => {
    const name = `${prefix} archived workspace`;
    const archived = await createAdminWorkspace(name);
    const active = await createAdminWorkspace(`${name} active`);
    expect(archived.statusCode, archived.body).toBe(200);
    expect(active.statusCode, active.body).toBe(200);
    const archive = await app!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${archived.json().id}/archive`,
      headers: { 'x-api-key': adminKey },
    });
    expect(archive.statusCode, archive.body).toBe(200);
    const before = await mutationState();

    const conflicts = await Promise.all([
      createPlatformWorkspace(name),
      createAdminWorkspace(name),
      renameWorkspace(active.json().id, name),
    ]);
    for (const conflict of conflicts) {
      expect(conflict.statusCode, conflict.body).toBe(409);
      expect(conflict.json()).toEqual(workspaceConflict);
    }
    expect(await mutationState()).toEqual(before);
  });

  it('allows self rename and rejects a conflicting rename without changing the workspace or audit', async () => {
    const name = `${prefix} rename source`;
    const source = await createAdminWorkspace(name);
    const target = await createPlatformWorkspace(`${prefix} rename target`);
    expect(source.statusCode, source.body).toBe(200);
    expect(target.statusCode, target.body).toBe(201);
    const self = await renameWorkspace(source.json().id, `  ${name}  `);
    expect(self.statusCode, self.body).toBe(200);
    expect(self.json()).toMatchObject({ id: source.json().id, name });
    const before = await mutationState();

    const conflict = await renameWorkspace(source.json().id, `  ${target.json().name}  `);
    expect(conflict.statusCode, conflict.body).toBe(409);
    expect(conflict.json()).toEqual(workspaceConflict);
    expect(await mutationState()).toEqual(before);
  });

  it('allows renaming to a name used only in another organization', async () => {
    const name = `${prefix} cross organization rename`;
    const source = await createAdminWorkspace(`${name} source`);
    const other = await createPlatformWorkspace(name, otherOrganizationId);
    expect(source.statusCode, source.body).toBe(200);
    expect(other.statusCode, other.body).toBe(201);

    const renamed = await renameWorkspace(source.json().id, name);
    expect(renamed.statusCode, renamed.body).toBe(200);
    expect(renamed.json()).toMatchObject({ id: source.json().id, name });
  });

  it('allows only one concurrent organization create and records only its settings and audit', async () => {
    const name = `${prefix} concurrent organization`;
    const before = await mutationState();
    const results = await Promise.all([createOrganization(name), createOrganization(name)]);
    expect(results.map((result) => result.statusCode).sort()).toEqual([201, 409]);
    expect(results.find((result) => result.statusCode === 409)!.json()).toEqual(
      organizationConflict,
    );
    const after = await mutationState();
    expect(after.organizationRows).toHaveLength(before.organizationRows.length + 1);
    expect(after.organizationSettings).toHaveLength(before.organizationSettings.length + 1);
    expect(after.platformAudit).toHaveLength(before.platformAudit.length + 1);
    expect(after.workspaceRows).toEqual(before.workspaceRows);
    expect(after.workspaceSettings).toEqual(before.workspaceSettings);
    expect(after.adminAudit).toEqual(before.adminAudit);
  });

  it('allows only one concurrent platform/admin workspace create with one settings and audit row', async () => {
    const name = `${prefix} concurrent workspace`;
    const before = await mutationState();
    const results = await Promise.all([createPlatformWorkspace(name), createAdminWorkspace(name)]);
    const success = results.filter(
      (result) => result.statusCode === 200 || result.statusCode === 201,
    );
    expect(success).toHaveLength(1);
    const conflict = results.find((result) => result.statusCode === 409);
    expect(conflict?.json()).toEqual(workspaceConflict);
    const after = await mutationState();
    expect(after.workspaceRows).toHaveLength(before.workspaceRows.length + 1);
    expect(after.workspaceSettings).toHaveLength(before.workspaceSettings.length + 1);
    expect(after.platformAudit.length + after.adminAudit.length).toBe(
      before.platformAudit.length + before.adminAudit.length + 1,
    );
    expect(after.organizationRows).toEqual(before.organizationRows);
    expect(after.organizationSettings).toEqual(before.organizationSettings);
    const rows = after.workspaceRows.filter((row) => row.name === name);
    expect(rows).toEqual([expect.objectContaining({ id: success[0]!.json().id, organizationId })]);
  });

  it('allows an admin insert to finish while a platform create holds the organization row lock', async () => {
    const name = `${prefix} ordered create race`;
    const functionName = `name_uniqueness_gate_${suffix.replaceAll('-', '')}`;
    const lockId = Number.parseInt(suffix.slice(0, 7), 16);
    const blocker = await pool.connect();
    const pendingRequests: Promise<unknown>[] = [];
    let timer: ReturnType<typeof setTimeout> | undefined;

    // Pause the actual platform route after its organization lock, before its
    // workspace insert acquires the unique index entry. An admin insert must
    // still be able to take the FK's KEY SHARE lock. FOR UPDATE here deadlocks
    // when the platform insert later waits on the admin's unique index entry.
    // The trigger is restricted to this test's name and platform credential.
    try {
      await pool.query(`
        CREATE FUNCTION ${functionName}() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock(${lockId}::bigint);
          RETURN NEW;
        END;
        $$
      `);
      await pool.query(`
        CREATE TRIGGER ${functionName} BEFORE INSERT ON workspaces
        FOR EACH ROW
        WHEN (NEW.name = '${name}' AND NEW.created_by = 'platform-api-key:${platformKeyId}')
        EXECUTE FUNCTION ${functionName}()
      `);
      await blocker.query('SELECT pg_advisory_lock($1::bigint)', [lockId]);
      const before = await mutationState();
      const platformRequest = Promise.resolve(createPlatformWorkspace(name));
      pendingRequests.push(platformRequest);

      let platformIsPaused = false;
      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        const waiting = await pool.query(
          `SELECT 1 FROM pg_locks
           WHERE locktype = 'advisory' AND classid = 0 AND objid = $1 AND NOT granted`,
          [lockId],
        );
        if (waiting.rowCount) {
          platformIsPaused = true;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      expect(platformIsPaused, 'platform create must reach the insert gate').toBe(true);

      const adminRequest = Promise.resolve(createAdminWorkspace(name));
      pendingRequests.push(adminRequest);
      const adminResult = await Promise.race([
        adminRequest,
        new Promise<undefined>((resolve) => {
          timer = setTimeout(() => resolve(undefined), 3000);
        }),
      ]);
      expect(adminResult, 'organization row lock must permit the workspace FK check').toBeDefined();
      expect(adminResult!.statusCode, adminResult!.body).toBe(200);

      await blocker.query('SELECT pg_advisory_unlock($1::bigint)', [lockId]);
      const platformResult = await platformRequest;
      expect(platformResult.statusCode, platformResult.body).toBe(409);
      expect(platformResult.json()).toEqual(workspaceConflict);
      const after = await mutationState();
      expect(after.workspaceRows).toHaveLength(before.workspaceRows.length + 1);
      expect(after.workspaceSettings).toHaveLength(before.workspaceSettings.length + 1);
      expect(after.adminAudit).toHaveLength(before.adminAudit.length + 1);
      expect(after.platformAudit).toEqual(before.platformAudit);
    } finally {
      clearTimeout(timer);
      await blocker.query('SELECT pg_advisory_unlock($1::bigint)', [lockId]);
      blocker.release();
      await Promise.allSettled(pendingRequests);
      await pool.query(`DROP TRIGGER IF EXISTS ${functionName} ON workspaces`);
      await pool.query(`DROP FUNCTION IF EXISTS ${functionName}()`);
    }
  });

  it('allows only one concurrent rename to the same name and leaves the losing workspace intact', async () => {
    const name = `${prefix} concurrent rename`;
    const first = await createAdminWorkspace(`${name} first`);
    const second = await createAdminWorkspace(`${name} second`);
    expect(first.statusCode, first.body).toBe(200);
    expect(second.statusCode, second.body).toBe(200);
    const before = await mutationState();
    const sources = [first.json(), second.json()];
    const results = await Promise.all(sources.map((source) => renameWorkspace(source.id, name)));
    expect(results.map((result) => result.statusCode).sort()).toEqual([200, 409]);
    const losingIndex = results.findIndex((result) => result.statusCode === 409);
    expect(results[losingIndex]!.json()).toEqual(workspaceConflict);
    const after = await mutationState();
    expect(after.workspaceRows).toHaveLength(before.workspaceRows.length);
    expect(after.workspaceSettings).toEqual(before.workspaceSettings);
    expect(after.adminAudit).toHaveLength(before.adminAudit.length + 1);
    expect(after.platformAudit).toEqual(before.platformAudit);
    expect(after.workspaceRows.filter((row) => row.name === name)).toHaveLength(1);
    expect(after.workspaceRows.find((row) => row.id === sources[losingIndex]!.id)).toEqual(
      before.workspaceRows.find((row) => row.id === sources[losingIndex]!.id),
    );
  });

  it('replays successful organization and workspace creates as 201 without duplicate writes', async () => {
    const organizationName = `${prefix} replay organization`;
    const workspaceName = `${prefix} replay workspace`;
    const organizationKey = `${suffix}-organization`;
    const workspaceKey = `${suffix}-workspace`;
    const createdOrganization = await createOrganization(organizationName, organizationKey);
    const createdWorkspace = await createPlatformWorkspace(
      workspaceName,
      organizationId,
      workspaceKey,
    );
    expect(createdOrganization.statusCode, createdOrganization.body).toBe(201);
    expect(createdWorkspace.statusCode, createdWorkspace.body).toBe(201);
    const before = await mutationState();

    const replayedOrganization = await createOrganization(organizationName, organizationKey);
    const replayedWorkspace = await createPlatformWorkspace(
      workspaceName,
      organizationId,
      workspaceKey,
    );
    expect(replayedOrganization.statusCode, replayedOrganization.body).toBe(201);
    expect(replayedOrganization.body).toBe(createdOrganization.body);
    expect(replayedWorkspace.statusCode, replayedWorkspace.body).toBe(201);
    expect(replayedWorkspace.body).toBe(createdWorkspace.body);
    expect(await mutationState()).toEqual(before);
  });

  it('enforces uniqueness in Postgres for writers that bypass the HTTP routes', async () => {
    const organizationName = `${prefix} database organization`;
    const workspaceName = `${prefix} database workspace`;
    expect((await createOrganization(organizationName)).statusCode).toBe(201);
    expect((await createAdminWorkspace(workspaceName)).statusCode).toBe(200);
    const before = await mutationState();

    await expect(
      pool.query('INSERT INTO organizations (id, name) VALUES ($1, $2)', [
        `org_name_unique_rejected_${suffix}`,
        organizationName,
      ]),
    ).rejects.toMatchObject({ code: '23505', constraint: 'organizations_name_idx' });
    await expect(
      pool.query(
        'INSERT INTO workspaces (id, organization_id, name, created_by) VALUES ($1, $2, $3, $4)',
        [
          `wrkspc_name_unique_rejected_${suffix}`,
          organizationId,
          workspaceName,
          'integration-test',
        ],
      ),
    ).rejects.toMatchObject({ code: '23505', constraint: 'workspaces_organization_name_idx' });
    expect(await mutationState()).toEqual(before);
    const rows = await db
      .select()
      .from(workspaces)
      .where(
        and(eq(workspaces.organizationId, organizationId), eq(workspaces.name, workspaceName)),
      );
    expect(rows).toHaveLength(1);
  });
});
