// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { and, eq, sql } from 'drizzle-orm';
import { InMemorySkillStore } from '@orca/skill-store';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  partialAdminApiKeyHint,
} from '../../src/auth/admin-api-key.js';
import { fingerprintApiKey, hashApiKey } from '../../src/auth/api-key.js';
import { buildAdminApp, buildPublicApp } from '../../src/server.js';
import { reconcileSessionLifecycleOutbox } from '../../src/domain/session-lifecycle-outbox.js';
import {
  adminApiKeys,
  adminAuditEvents,
  agentObservabilityBindingCredentials,
  agentObservabilityBindingVersions,
  agentObservabilityBindings,
  agentObservabilityWorkspaceArchiveRevocations,
  agentObservabilityWorkspaceSettings,
  apiKeys,
  organizations,
  sessionLifecycleOutbox,
  sessionObservabilityBindings,
  sessions,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';

describe('organization admin workspace control plane', () => {
  let db: DbClient;
  let adminApp: FastifyInstance | undefined;
  let publicApp: FastifyInstance | undefined;
  let adminKey: string;
  let workspaceId: string;
  let workspaceKey: string;
  let sessionId: string;
  let activeSessionId: string;
  let failTranscriptAppend = false;
  const appendedEvents: Array<{ id: string; kind: string; sessionId: string }> = [];
  const organizationId = `org_admin_it_${Date.now()}`;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    adminKey = generateAdminApiKey();
    await db.insert(organizations).values({
      id: organizationId,
      name: `Admin integration test ${organizationId}`,
      status: 'active',
    });
    await db.insert(adminApiKeys).values({
      id: `adminkey_it_${Date.now()}`,
      organizationId,
      name: 'Integration admin',
      hashedKey: await hashAdminApiKey(adminKey),
      keyFingerprint: fingerprintAdminApiKey(adminKey),
      partialKeyHint: partialAdminApiKeyHint(adminKey),
      scopes: ['org:admin'],
      status: 'active',
      createdBy: 'integration-test',
    });
    const store = buildStubStore();
    store.append = async (_workspaceId, appendedSessionId, events) => {
      if (failTranscriptAppend) throw new Error('injected transcript failure');
      appendedEvents.push(
        ...events.map((event) => ({
          id: event.id,
          kind: event.kind,
          sessionId: appendedSessionId,
        })),
      );
      return events.map((event) => event.id);
    };
    adminApp = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store,
    });
    publicApp = buildPublicApp({
      db,
      oidc: { allowedIssuers: [], audience: 'public-test' },
      store,
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      skillStore: new InMemorySkillStore(),
    });
    await Promise.all([adminApp.ready(), publicApp.ready()]);
  });

  afterAll(async () => {
    await Promise.all([adminApp?.close(), publicApp?.close()]);
    await closeTestDb();
  });

  it('creates a workspace and returns only organization-scoped resources', async () => {
    const create = await adminApp!.inject({
      method: 'POST',
      url: '/v1/organizations/workspaces',
      headers: { 'x-api-key': adminKey },
      payload: { name: 'Workspace A' },
    });
    expect(create.statusCode).toBe(200);
    workspaceId = create.json().id as string;
    const settings = await db
      .select()
      .from(agentObservabilityWorkspaceSettings)
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId));
    expect(settings).toEqual([
      expect.objectContaining({
        organizationId,
        mode: 'inherit',
        bindingId: null,
        captureCeiling: 'metadata_only',
      }),
    ]);

    const list = await adminApp!.inject({
      method: 'GET',
      url: '/v1/organizations/workspaces',
      headers: { 'x-api-key': adminKey },
    });
    expect(list.statusCode).toBe(200);
    expect(list.json().data).toContainEqual(expect.objectContaining({ id: workspaceId }));
  });

  it('paginates workspaces by creation time with an id tiebreaker', async () => {
    const suffix = Date.now();
    const earliestId = `wrkspc_order_z_${suffix}`;
    const tiedFirstId = `wrkspc_order_a_${suffix}`;
    const tiedSecondId = `wrkspc_order_b_${suffix}`;
    const earliestCreatedAt = new Date('2000-01-01T00:00:00.000Z');
    const tiedCreatedAt = new Date('2000-01-02T00:00:00.000Z');
    await db.insert(workspaces).values([
      {
        id: earliestId,
        organizationId,
        name: 'Created first despite sorting last by ID',
        status: 'active',
        createdBy: 'integration-test',
        createdAt: earliestCreatedAt,
        updatedAt: earliestCreatedAt,
      },
      {
        id: tiedFirstId,
        organizationId,
        name: 'Tied timestamp A',
        status: 'active',
        createdBy: 'integration-test',
        createdAt: tiedCreatedAt,
        updatedAt: tiedCreatedAt,
      },
      {
        id: tiedSecondId,
        organizationId,
        name: 'Tied timestamp B',
        status: 'active',
        createdBy: 'integration-test',
        createdAt: tiedCreatedAt,
        updatedAt: tiedCreatedAt,
      },
    ]);
    const first = await adminApp!.inject({
      method: 'GET',
      url: '/v1/organizations/workspaces?limit=1',
      headers: { 'x-api-key': adminKey },
    });
    expect(first.statusCode).toBe(200);
    expect(first.json()).toMatchObject({
      data: [{ id: earliestId }],
      first_id: earliestId,
      last_id: earliestId,
      has_more: true,
    });

    const afterEarliest = await adminApp!.inject({
      method: 'GET',
      url: `/v1/organizations/workspaces?limit=1&after_id=${earliestId}`,
      headers: { 'x-api-key': adminKey },
    });
    expect(afterEarliest.statusCode).toBe(200);
    expect(afterEarliest.json().data).toEqual([expect.objectContaining({ id: tiedFirstId })]);

    const afterTiedFirst = await adminApp!.inject({
      method: 'GET',
      url: `/v1/organizations/workspaces?limit=1&after_id=${tiedFirstId}`,
      headers: { 'x-api-key': adminKey },
    });
    expect(afterTiedFirst.statusCode).toBe(200);
    expect(afterTiedFirst.json().data).toEqual([expect.objectContaining({ id: tiedSecondId })]);

    const beforeTiedSecond = await adminApp!.inject({
      method: 'GET',
      url: `/v1/organizations/workspaces?limit=1&before_id=${tiedSecondId}`,
      headers: { 'x-api-key': adminKey },
    });
    expect(beforeTiedSecond.statusCode).toBe(200);
    expect(beforeTiedSecond.json().data).toEqual([expect.objectContaining({ id: tiedFirstId })]);
  });

  it('issues plaintext once and can disable a workspace key', async () => {
    const create = await adminApp!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${workspaceId}/api_keys`,
      headers: { 'x-api-key': adminKey },
      payload: { name: 'Runtime key' },
    });
    expect(create.statusCode).toBe(201);
    expect(create.headers['cache-control']).toBe('no-store');
    workspaceKey = create.json().key as string;
    const keyId = create.json().id as string;

    const publicBeforeDisable = await publicApp!.inject({
      method: 'GET',
      url: '/v1/skills',
      headers: { 'x-api-key': workspaceKey },
    });
    expect(publicBeforeDisable.statusCode).toBe(200);

    const agent = await publicApp!.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': workspaceKey },
      payload: {
        name: `admin-archive-agent-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      },
    });
    expect(agent.statusCode).toBe(200);
    const environment = await publicApp!.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': workspaceKey },
      payload: {
        name: `admin-archive-environment-${Date.now()}`,
        config: { type: 'cloud' },
      },
    });
    expect(environment.statusCode).toBe(200);
    const session = await publicApp!.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': workspaceKey },
      payload: { agent_id: agent.json().id, environment_id: environment.json().id },
    });
    expect(session.statusCode).toBe(200);
    sessionId = session.json().id as string;

    // Keep the first Session disabled, then create an actual custom active
    // pin. Workspace archive must tombstone both lifecycle shapes through the
    // database Session trigger rather than an application-side fan-out helper.
    const activeBindingId = `aob_workspace_archive_${workspaceId}`;
    const pinNow = new Date();
    await db.transaction(async (tx) => {
      await tx.insert(agentObservabilityBindings).values({
        id: activeBindingId,
        organizationId,
        workspaceId,
        scopeType: 'workspace',
        adapterType: 'otlp_http',
        endpointKind: 'traces_endpoint',
        endpointClass: 'public',
        endpoint: 'https://collector.example/v1/traces',
        externalProjectId: null,
        currentVersion: 1,
        status: 'active',
        revocationEpoch: 1,
        createdBy: 'integration-test',
        updatedBy: 'integration-test',
        archivedAt: null,
        createdAt: pinNow,
        updatedAt: pinNow,
      });
      await tx.insert(agentObservabilityBindingVersions).values({
        bindingId: activeBindingId,
        version: 1,
        adapterType: 'otlp_http',
        semanticProfile: 'otel_genai',
        protocol: 'http/protobuf',
        compression: 'none',
        timeoutMs: 5_000,
        environment: null,
        release: null,
        captureMode: 'metadata_only',
        sampleRate: '1',
        configSchemaVersion: 1,
        createdBy: 'integration-test',
        createdAt: pinNow,
      });
      await tx.insert(agentObservabilityBindingCredentials).values({
        bindingId: activeBindingId,
        secretRef: `workspace-archive-secret-ref-${workspaceId}`,
        credentialVersion: 1,
        keyHint: null,
        rotatedAt: pinNow,
        updatedBy: 'integration-test',
        createdAt: pinNow,
        updatedAt: pinNow,
      });
      await tx
        .update(agentObservabilityWorkspaceSettings)
        .set({
          mode: 'custom',
          bindingId: activeBindingId,
          selectionEpoch: 1,
          revocationEpoch: 1,
          captureCeiling: 'metadata_only',
          captureRestrictionEpoch: 1,
          updatedAt: pinNow,
        })
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId));
    });
    const activeSession = await publicApp!.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': workspaceKey },
      payload: { agent_id: agent.json().id, environment_id: environment.json().id },
    });
    expect(activeSession.statusCode).toBe(200);
    activeSessionId = activeSession.json().id as string;
    const [disabledPin, activePin] = await Promise.all([
      db
        .select({ status: sessionObservabilityBindings.status })
        .from(sessionObservabilityBindings)
        .where(
          and(
            eq(sessionObservabilityBindings.workspaceId, workspaceId),
            eq(sessionObservabilityBindings.sessionId, sessionId),
          ),
        )
        .limit(1),
      db
        .select({ status: sessionObservabilityBindings.status })
        .from(sessionObservabilityBindings)
        .where(
          and(
            eq(sessionObservabilityBindings.workspaceId, workspaceId),
            eq(sessionObservabilityBindings.sessionId, activeSessionId),
          ),
        )
        .limit(1),
    ]);
    expect(disabledPin).toEqual([{ status: 'disabled' }]);
    expect(activePin).toEqual([{ status: 'active' }]);

    const update = await adminApp!.inject({
      method: 'POST',
      url: `/v1/organizations/api_keys/${keyId}`,
      headers: { 'x-api-key': adminKey },
      payload: { status: 'inactive' },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json()).not.toHaveProperty('key');

    const publicAfterDisable = await publicApp!.inject({
      method: 'GET',
      url: '/v1/skills',
      headers: { 'x-api-key': workspaceKey },
    });
    expect(publicAfterDisable.statusCode).toBe(401);

    const expiredPlaintext = `orca_expired_${Date.now()}`;
    const expiredKeyId = `apikey_expired_${Date.now()}`;
    await db.insert(apiKeys).values({
      id: expiredKeyId,
      workspaceId,
      hashedKey: await hashApiKey(expiredPlaintext),
      keyFingerprint: fingerprintApiKey(expiredPlaintext),
      name: 'Expired key',
      partialKeyHint: `...${expiredPlaintext.slice(-4)}`,
      principal: `api-key:${expiredKeyId}`,
      scopes: ['workspace.full_access'],
      status: 'active',
      expiresAt: new Date(Date.now() - 60_000),
      createdBy: 'integration-test',
    });
    const [activeKeys, expiredKeys] = await Promise.all([
      adminApp!.inject({
        method: 'GET',
        url: `/v1/organizations/api_keys?workspace_id=${workspaceId}&status=active`,
        headers: { 'x-api-key': adminKey },
      }),
      adminApp!.inject({
        method: 'GET',
        url: `/v1/organizations/api_keys?workspace_id=${workspaceId}&status=expired`,
        headers: { 'x-api-key': adminKey },
      }),
    ]);
    expect(activeKeys.json().data).not.toContainEqual(
      expect.objectContaining({ id: expiredKeyId }),
    );
    expect(expiredKeys.json().data).toContainEqual(
      expect.objectContaining({ id: expiredKeyId, status: 'expired' }),
    );
  });

  it('archives a workspace terminally and records non-secret audit events', async () => {
    const [beforeSetting] = await db
      .select()
      .from(agentObservabilityWorkspaceSettings)
      .where(
        and(
          eq(agentObservabilityWorkspaceSettings.organizationId, organizationId),
          eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId),
        ),
      )
      .limit(1);
    const [beforeBinding, beforeCredential] = await Promise.all([
      db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, beforeSetting!.bindingId!))
        .limit(1),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, beforeSetting!.bindingId!))
        .limit(1),
    ]);
    failTranscriptAppend = true;
    const archive = await adminApp!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${workspaceId}/archive`,
      headers: { 'x-api-key': adminKey },
    });
    expect(archive.statusCode).toBe(200);
    expect(archive.json().archived_at).toBeTypeOf('string');
    const [[afterSetting], [afterMarker], afterBinding, afterCredential] = await Promise.all([
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId))
        .limit(1),
      db
        .select()
        .from(agentObservabilityWorkspaceArchiveRevocations)
        .where(
          and(
            eq(agentObservabilityWorkspaceArchiveRevocations.organizationId, organizationId),
            eq(agentObservabilityWorkspaceArchiveRevocations.workspaceId, workspaceId),
          ),
        )
        .limit(1),
      db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, beforeSetting!.bindingId!))
        .limit(1),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, beforeSetting!.bindingId!))
        .limit(1),
    ]);
    expect(afterSetting).toMatchObject({
      organizationId: beforeSetting!.organizationId,
      workspaceId: beforeSetting!.workspaceId,
      mode: beforeSetting!.mode,
      bindingId: beforeSetting!.bindingId,
      selectionEpoch: beforeSetting!.selectionEpoch,
      captureCeiling: beforeSetting!.captureCeiling,
      captureRestrictionEpoch: beforeSetting!.captureRestrictionEpoch,
      createdAt: beforeSetting!.createdAt,
      revocationEpoch: beforeSetting!.revocationEpoch + 1,
    });
    expect(afterSetting!.updatedAt.toISOString()).toBe(archive.json().archived_at);
    expect(afterMarker).toMatchObject({
      organizationId,
      workspaceId,
      archivedAt: new Date(archive.json().archived_at),
      revocationEpoch: afterSetting!.revocationEpoch,
      createdAt: expect.any(Date),
    });
    expect(afterBinding).toEqual(beforeBinding);
    expect(afterCredential).toEqual(beforeCredential);
    const repeatedArchive = await adminApp!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${workspaceId}/archive`,
      headers: { 'x-api-key': adminKey },
    });
    expect(repeatedArchive.statusCode).toBe(200);
    const [[repeatedSetting], [repeatedMarker]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId))
        .limit(1),
      db
        .select()
        .from(agentObservabilityWorkspaceArchiveRevocations)
        .where(eq(agentObservabilityWorkspaceArchiveRevocations.workspaceId, workspaceId))
        .limit(1),
    ]);
    expect(repeatedSetting).toEqual(afterSetting);
    expect(repeatedMarker).toEqual(afterMarker);
    const [archivedDisabledPin, archivedActivePin] = await Promise.all([
      db
        .select({
          status: sessionObservabilityBindings.status,
          archivedAt: sessionObservabilityBindings.archivedAt,
          deletedAt: sessionObservabilityBindings.deletedAt,
          sessionRevocationEpoch: sessionObservabilityBindings.sessionRevocationEpoch,
        })
        .from(sessionObservabilityBindings)
        .where(
          and(
            eq(sessionObservabilityBindings.workspaceId, workspaceId),
            eq(sessionObservabilityBindings.sessionId, sessionId),
          ),
        )
        .limit(1),
      db
        .select({
          status: sessionObservabilityBindings.status,
          archivedAt: sessionObservabilityBindings.archivedAt,
          deletedAt: sessionObservabilityBindings.deletedAt,
          sessionRevocationEpoch: sessionObservabilityBindings.sessionRevocationEpoch,
        })
        .from(sessionObservabilityBindings)
        .where(
          and(
            eq(sessionObservabilityBindings.workspaceId, workspaceId),
            eq(sessionObservabilityBindings.sessionId, activeSessionId),
          ),
        )
        .limit(1),
    ]);
    for (const pin of [archivedDisabledPin[0], archivedActivePin[0]]) {
      expect(pin).toMatchObject({
        status: 'archived',
        archivedAt: expect.any(Date),
        deletedAt: null,
        sessionRevocationEpoch: 1,
      });
    }

    const pendingRows = await db
      .select()
      .from(sessionLifecycleOutbox)
      .where(
        and(
          eq(sessionLifecycleOutbox.workspaceId, workspaceId),
          eq(sessionLifecycleOutbox.sessionId, sessionId),
        ),
      );
    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0]).toMatchObject({ publishedAt: null, attemptCount: 1 });

    failTranscriptAppend = false;
    await reconcileSessionLifecycleOutbox(db, buildRetryStore(), {
      eventIds: [pendingRows[0]!.id],
    });
    const publishedRows = await db
      .select()
      .from(sessionLifecycleOutbox)
      .where(eq(sessionLifecycleOutbox.id, pendingRows[0]!.id));
    expect(publishedRows[0]?.publishedAt).toBeInstanceOf(Date);
    expect(appendedEvents).toContainEqual(
      expect.objectContaining({ kind: 'session.archived', sessionId }),
    );

    const createKey = await adminApp!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${workspaceId}/api_keys`,
      headers: { 'x-api-key': adminKey },
      payload: { name: 'Must fail' },
    });
    expect(createKey.statusCode).toBe(409);

    const [defaultWorkspaceList, archivedWorkspaceList] = await Promise.all([
      adminApp!.inject({
        method: 'GET',
        url: '/v1/organizations/workspaces',
        headers: { 'x-api-key': adminKey },
      }),
      adminApp!.inject({
        method: 'GET',
        url: '/v1/organizations/workspaces?include_archived=true',
        headers: { 'x-api-key': adminKey },
      }),
    ]);
    expect(defaultWorkspaceList.json().data).not.toContainEqual(
      expect.objectContaining({ id: workspaceId }),
    );
    expect(archivedWorkspaceList.json().data).toContainEqual(
      expect.objectContaining({ id: workspaceId }),
    );

    const audit = await db.select().from(adminAuditEvents);
    const organizationAudit = audit.filter((event) => event.organizationId === organizationId);
    expect(organizationAudit.map((event) => event.action)).toEqual(
      expect.arrayContaining(['workspace.created', 'api_key.created', 'workspace.archived']),
    );
    expect(JSON.stringify(organizationAudit)).not.toContain(workspaceKey);
  });

  it('rolls back workspace archive when one Session observability pin is missing', async () => {
    const fixture = await createArchiveRollbackFixture(`Missing pin rollback ${Date.now()}`);
    const [beforeSetting] = await db
      .select()
      .from(agentObservabilityWorkspaceSettings)
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, fixture.workspaceId))
      .limit(1);
    await db
      .delete(sessionObservabilityBindings)
      .where(
        and(
          eq(sessionObservabilityBindings.workspaceId, fixture.workspaceId),
          eq(sessionObservabilityBindings.sessionId, fixture.sessionId),
        ),
      );

    const archive = await adminApp!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${fixture.workspaceId}/archive`,
      headers: { 'x-api-key': adminKey },
    });
    expect(archive.statusCode).toBe(500);
    await expectArchiveRollback(fixture, beforeSetting!, false);
  });

  it('fails closed when a workspace observability setting is missing', async () => {
    const fixture = await createArchiveRollbackFixture(`Missing setting rollback ${Date.now()}`);
    await db
      .delete(agentObservabilityWorkspaceSettings)
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, fixture.workspaceId));

    const archive = await adminApp!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${fixture.workspaceId}/archive`,
      headers: { 'x-api-key': adminKey },
    });
    expect(archive.statusCode).toBe(500);
    await expectArchiveRollback(fixture, null);
  });

  it('fails closed when an active workspace already has an archive marker', async () => {
    const fixture = await createArchiveRollbackFixture(`Active marker rollback ${Date.now()}`);
    const [beforeSetting] = await db
      .select()
      .from(agentObservabilityWorkspaceSettings)
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, fixture.workspaceId))
      .limit(1);
    const markerAt = new Date('2035-08-18T09:00:00.000Z');
    await db.insert(agentObservabilityWorkspaceArchiveRevocations).values({
      organizationId,
      workspaceId: fixture.workspaceId,
      archivedAt: markerAt,
      revocationEpoch: beforeSetting!.revocationEpoch + 1,
    });

    const archive = await adminApp!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${fixture.workspaceId}/archive`,
      headers: { 'x-api-key': adminKey },
    });
    expect(archive.statusCode).toBe(500);

    const [[storedWorkspace], [storedSetting], [storedMarker]] = await Promise.all([
      db
        .select({ status: workspaces.status, archivedAt: workspaces.archivedAt })
        .from(workspaces)
        .where(eq(workspaces.id, fixture.workspaceId))
        .limit(1),
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, fixture.workspaceId))
        .limit(1),
      db
        .select()
        .from(agentObservabilityWorkspaceArchiveRevocations)
        .where(eq(agentObservabilityWorkspaceArchiveRevocations.workspaceId, fixture.workspaceId))
        .limit(1),
    ]);
    expect(storedWorkspace).toEqual({ status: 'active', archivedAt: null });
    expect(storedSetting).toEqual(beforeSetting);
    expect(storedMarker).toMatchObject({
      organizationId,
      workspaceId: fixture.workspaceId,
      archivedAt: markerAt,
      revocationEpoch: beforeSetting!.revocationEpoch + 1,
    });
  });

  it('fails closed when a workspace observability revocation epoch overflows', async () => {
    const fixture = await createArchiveRollbackFixture(`Overflow rollback ${Date.now()}`);
    await db
      .update(agentObservabilityWorkspaceSettings)
      .set({ revocationEpoch: Number.MAX_SAFE_INTEGER })
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, fixture.workspaceId));
    const [beforeSetting] = await db
      .select()
      .from(agentObservabilityWorkspaceSettings)
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, fixture.workspaceId))
      .limit(1);

    const archive = await adminApp!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${fixture.workspaceId}/archive`,
      headers: { 'x-api-key': adminKey },
    });
    expect(archive.statusCode).toBe(500);
    await expectArchiveRollback(fixture, beforeSetting!);
  });

  it('rolls back workspace archive when the observability epoch CAS loses its row', async () => {
    const fixture = await createArchiveRollbackFixture(`CAS rollback ${Date.now()}`);
    const [beforeSetting] = await db
      .select()
      .from(agentObservabilityWorkspaceSettings)
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, fixture.workspaceId))
      .limit(1);
    const suffix = `${process.pid}_${Date.now()}`;
    const functionName = `test_workspace_archive_epoch_cas_${suffix}`;
    const triggerName = `${functionName}_trigger`;
    const workspaceLiteral = fixture.workspaceId.replaceAll("'", "''");
    try {
      await db.execute(
        sql.raw(`
          CREATE FUNCTION "${functionName}"() RETURNS trigger
          LANGUAGE plpgsql
          AS $$
          BEGIN
            IF NEW."workspace_id" = '${workspaceLiteral}' THEN
              RETURN NULL;
            END IF;
            RETURN NEW;
          END;
          $$
        `),
      );
      await db.execute(
        sql.raw(`
          CREATE TRIGGER "${triggerName}"
          BEFORE UPDATE OF "revocation_epoch" ON "agent_observability_workspace_settings"
          FOR EACH ROW EXECUTE FUNCTION "${functionName}"()
        `),
      );
      const archive = await adminApp!.inject({
        method: 'POST',
        url: `/v1/organizations/workspaces/${fixture.workspaceId}/archive`,
        headers: { 'x-api-key': adminKey },
      });
      expect(archive.statusCode).toBe(500);
    } finally {
      await db.execute(
        sql.raw(
          `DROP TRIGGER IF EXISTS "${triggerName}" ON "agent_observability_workspace_settings"`,
        ),
      );
      await db.execute(sql.raw(`DROP FUNCTION IF EXISTS "${functionName}"()`));
    }
    await expectArchiveRollback(fixture, beforeSetting!);
  });

  async function createArchiveRollbackFixture(name: string) {
    const workspace = await adminApp!.inject({
      method: 'POST',
      url: '/v1/organizations/workspaces',
      headers: { 'x-api-key': adminKey },
      payload: { name },
    });
    expect(workspace.statusCode).toBe(200);
    const fixtureWorkspaceId = workspace.json().id as string;

    const key = await adminApp!.inject({
      method: 'POST',
      url: `/v1/organizations/workspaces/${fixtureWorkspaceId}/api_keys`,
      headers: { 'x-api-key': adminKey },
      payload: { name: 'Rollback runtime key' },
    });
    expect(key.statusCode).toBe(201);
    const fixtureKey = key.json().key as string;
    const keyId = key.json().id as string;
    const agent = await publicApp!.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': fixtureKey },
      payload: {
        name: `workspace-rollback-agent-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      },
    });
    expect(agent.statusCode).toBe(200);
    const environment = await publicApp!.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: { 'x-api-key': fixtureKey },
      payload: {
        name: `workspace-rollback-environment-${Date.now()}`,
        config: { type: 'cloud' },
      },
    });
    expect(environment.statusCode).toBe(200);
    const session = await publicApp!.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: { 'x-api-key': fixtureKey },
      payload: { agent_id: agent.json().id, environment_id: environment.json().id },
    });
    expect(session.statusCode).toBe(200);
    return { workspaceId: fixtureWorkspaceId, keyId, sessionId: session.json().id as string };
  }

  async function expectArchiveRollback(
    fixture: { workspaceId: string; keyId: string; sessionId: string },
    expectedSetting: typeof agentObservabilityWorkspaceSettings.$inferSelect | null,
    pinPresent = true,
  ) {
    const [
      [storedWorkspace],
      [storedSession],
      [storedKey],
      settings,
      markers,
      outbox,
      audit,
      pins,
    ] = await Promise.all([
      db
        .select({ status: workspaces.status, archivedAt: workspaces.archivedAt })
        .from(workspaces)
        .where(eq(workspaces.id, fixture.workspaceId))
        .limit(1),
      db
        .select({ status: sessions.status, archivedAt: sessions.archivedAt })
        .from(sessions)
        .where(
          and(eq(sessions.workspaceId, fixture.workspaceId), eq(sessions.id, fixture.sessionId)),
        )
        .limit(1),
      db
        .select({ status: apiKeys.status, revokedAt: apiKeys.revokedAt })
        .from(apiKeys)
        .where(eq(apiKeys.id, fixture.keyId))
        .limit(1),
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, fixture.workspaceId)),
      db
        .select()
        .from(agentObservabilityWorkspaceArchiveRevocations)
        .where(eq(agentObservabilityWorkspaceArchiveRevocations.workspaceId, fixture.workspaceId)),
      db
        .select({ id: sessionLifecycleOutbox.id })
        .from(sessionLifecycleOutbox)
        .where(
          and(
            eq(sessionLifecycleOutbox.workspaceId, fixture.workspaceId),
            eq(sessionLifecycleOutbox.sessionId, fixture.sessionId),
          ),
        ),
      db
        .select({ id: adminAuditEvents.id })
        .from(adminAuditEvents)
        .where(
          and(
            eq(adminAuditEvents.workspaceId, fixture.workspaceId),
            eq(adminAuditEvents.action, 'workspace.archived'),
          ),
        ),
      db
        .select({
          status: sessionObservabilityBindings.status,
          archivedAt: sessionObservabilityBindings.archivedAt,
          sessionRevocationEpoch: sessionObservabilityBindings.sessionRevocationEpoch,
        })
        .from(sessionObservabilityBindings)
        .where(
          and(
            eq(sessionObservabilityBindings.workspaceId, fixture.workspaceId),
            eq(sessionObservabilityBindings.sessionId, fixture.sessionId),
          ),
        ),
    ]);
    expect(storedWorkspace).toEqual({ status: 'active', archivedAt: null });
    expect(storedSession).toEqual({ status: 'idle', archivedAt: null });
    expect(storedKey).toEqual({ status: 'active', revokedAt: null });
    expect(settings).toEqual(expectedSetting === null ? [] : [expectedSetting]);
    expect(markers).toEqual([]);
    expect(outbox).toEqual([]);
    expect(audit).toEqual([]);
    expect(pins).toEqual(
      pinPresent ? [{ status: 'disabled', archivedAt: null, sessionRevocationEpoch: 0 }] : [],
    );
  }

  function buildRetryStore() {
    const store = buildStubStore();
    store.append = async (_workspaceId, appendedSessionId, events) => {
      appendedEvents.push(
        ...events.map((event) => ({
          id: event.id,
          kind: event.kind,
          sessionId: appendedSessionId,
        })),
      );
      return events.map((event) => event.id);
    };
    return store;
  }
});
