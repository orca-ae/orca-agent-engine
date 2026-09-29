// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { eq } from 'drizzle-orm';
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
  agentObservabilityOrganizationSettings,
  agentObservabilityWorkspaceSettings,
  organizations,
  platformApiKeys,
  platformAuditEvents,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import { buildStubStore, closeTestDb, getTestDb } from './setup.js';

/**
 * Restated rather than imported from the route: this is a wire bound callers
 * build against, so a test that read it from the implementation could not
 * notice the implementation changing it.
 */
const AUDIENCE_MAX_LENGTH = 200;

describe('platform administration control plane', () => {
  let db: DbClient;
  let app: FastifyInstance | undefined;
  let platformKey: string;
  let seedOrganizationAdminKey: string;
  let createdOrganizationId: string;
  const suffix = `${Date.now()}`;
  const seedOrganizationId = `org_platform_seed_${suffix}`;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    platformKey = generatePlatformApiKey();
    seedOrganizationAdminKey = generateAdminApiKey();

    await db.insert(organizations).values({
      id: seedOrganizationId,
      name: `Seed organization ${seedOrganizationId}`,
      status: 'active',
    });
    await db.insert(platformApiKeys).values({
      id: `platformkey_it_${suffix}`,
      name: 'Platform integration key',
      hashedKey: await hashPlatformApiKey(platformKey),
      keyFingerprint: fingerprintPlatformApiKey(platformKey),
      partialKeyHint: partialPlatformApiKeyHint(platformKey),
      scopes: ['platform:admin'],
      status: 'active',
      createdBy: 'integration-test',
    });
    await db.insert(adminApiKeys).values({
      id: `adminkey_platform_seed_${suffix}`,
      organizationId: seedOrganizationId,
      name: 'Seed organization admin',
      hashedKey: await hashAdminApiKey(seedOrganizationAdminKey),
      keyFingerprint: fingerprintAdminApiKey(seedOrganizationAdminKey),
      partialKeyHint: partialAdminApiKeyHint(seedOrganizationAdminKey),
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

  it('keeps platform and organization credentials mutually exclusive', async () => {
    const [organizationOnPlatform, platformOnOrganization] = await Promise.all([
      app!.inject({
        method: 'POST',
        url: '/v1/platform/organizations',
        headers: { 'x-api-key': seedOrganizationAdminKey },
        payload: { name: 'Must not be created' },
      }),
      app!.inject({
        method: 'GET',
        url: '/v1/organizations/me',
        headers: { 'x-api-key': platformKey },
      }),
    ]);

    expect(organizationOnPlatform.statusCode).toBe(401);
    expect(platformOnOrganization.statusCode).toBe(401);
  });

  it('creates an organization idempotently without an organization-scoped principal', async () => {
    const request = {
      method: 'POST' as const,
      url: '/v1/platform/organizations',
      headers: {
        'x-api-key': platformKey,
        'idempotency-key': `  create-org-${suffix}  `,
      },
      payload: { name: `Provisioned organization ${suffix}` },
    };
    const [first, replay] = await Promise.all([
      app!.inject(request),
      app!.inject({
        ...request,
        headers: {
          ...request.headers,
          'idempotency-key': `create-org-${suffix}`,
        },
      }),
    ]);

    expect(first.statusCode, first.body).toBe(201);
    expect(replay.statusCode, replay.body).toBe(201);
    expect(replay.body).toBe(first.body);
    // An organization created without one takes part in no audience-based
    // workspace resolution, and says so on the wire rather than omitting it.
    expect(first.json()).toMatchObject({ audience: null });
    createdOrganizationId = first.json().id as string;
    expect(createdOrganizationId).toMatch(/^org_/);

    const rows = await db
      .select()
      .from(organizations)
      .where(eq(organizations.name, request.payload.name));
    expect(rows.filter((row) => row.id === createdOrganizationId)).toHaveLength(1);
    const settings = await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, createdOrganizationId));
    expect(settings).toEqual([
      expect.objectContaining({
        activeDefaultBindingId: null,
        activeDefaultBindingScope: null,
        captureCeiling: 'metadata_only',
        selectionEpoch: 0,
      }),
    ]);

    const conflict = await app!.inject({
      ...request,
      payload: { name: 'Different organization' },
    });
    expect(conflict.statusCode).toBe(409);
    expect(conflict.json()).toEqual({
      error: 'idempotency-key reused with different body',
    });
  });

  it('creates a workspace under an explicit active organization', async () => {
    const organizationAdminKey = generateAdminApiKey();
    await db.insert(adminApiKeys).values({
      id: `adminkey_platform_created_${suffix}`,
      organizationId: createdOrganizationId,
      name: 'Provisioned organization admin',
      hashedKey: await hashAdminApiKey(organizationAdminKey),
      keyFingerprint: fingerprintAdminApiKey(organizationAdminKey),
      partialKeyHint: partialAdminApiKeyHint(organizationAdminKey),
      scopes: ['org:admin'],
      status: 'active',
      createdBy: 'integration-test',
    });

    const bodySelectedOrganization = await app!.inject({
      method: 'POST',
      url: `/v1/platform/organizations/${createdOrganizationId}/workspaces`,
      headers: { 'x-api-key': platformKey },
      payload: {
        name: 'Body-selected workspace',
        organization_id: seedOrganizationId,
      },
    });
    expect(bodySelectedOrganization.statusCode).toBe(400);
    expect(bodySelectedOrganization.json()).toEqual({
      error: 'unsupported request fields: organization_id',
    });

    const create = await app!.inject({
      method: 'POST',
      url: `/v1/platform/organizations/${createdOrganizationId}/workspaces`,
      headers: {
        'x-api-key': platformKey,
        'idempotency-key': `create-workspace-${suffix}`,
      },
      payload: { name: 'Platform-created workspace' },
    });
    expect(create.statusCode).toBe(201);
    const workspaceId = create.json().id as string;

    const [targetOrganizationList, seedOrganizationList] = await Promise.all([
      app!.inject({
        method: 'GET',
        url: '/v1/organizations/workspaces',
        headers: { 'x-api-key': organizationAdminKey },
      }),
      app!.inject({
        method: 'GET',
        url: '/v1/organizations/workspaces',
        headers: { 'x-api-key': seedOrganizationAdminKey },
      }),
    ]);
    expect(targetOrganizationList.json().data).toContainEqual(
      expect.objectContaining({ id: workspaceId }),
    );
    expect(seedOrganizationList.json().data).not.toContainEqual(
      expect.objectContaining({ id: workspaceId }),
    );

    const row = await db.query.workspaces.findFirst({
      where: eq(workspaces.id, workspaceId),
    });
    expect(row).toMatchObject({
      organizationId: createdOrganizationId,
      createdBy: `platform-api-key:platformkey_it_${suffix}`,
    });
    const settings = await db
      .select()
      .from(agentObservabilityWorkspaceSettings)
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId));
    expect(settings).toEqual([
      expect.objectContaining({
        organizationId: createdOrganizationId,
        mode: 'inherit',
        bindingId: null,
        captureCeiling: 'metadata_only',
        selectionEpoch: 0,
      }),
    ]);
  });

  it('rejects workspace creation for missing or archived organizations', async () => {
    const archivedOrganizationId = `org_platform_archived_${suffix}`;
    await db.insert(organizations).values({
      id: archivedOrganizationId,
      name: `Archived organization ${archivedOrganizationId}`,
      status: 'archived',
    });

    const [missing, archived] = await Promise.all([
      app!.inject({
        method: 'POST',
        url: '/v1/platform/organizations/org_missing/workspaces',
        headers: { 'x-api-key': platformKey },
        payload: { name: 'Missing' },
      }),
      app!.inject({
        method: 'POST',
        url: `/v1/platform/organizations/${archivedOrganizationId}/workspaces`,
        headers: { 'x-api-key': platformKey },
        payload: { name: 'Archived' },
      }),
    ]);
    expect(missing.statusCode).toBe(404);
    expect(archived.statusCode).toBe(409);
  });

  it('rejects an audience that is present but unusable', async () => {
    // An organization's audience is only ever set here, so a value that cannot
    // serve as one is a 400 rather than a quiet "no audience" the caller would
    // have no way to correct later.
    const before = await db.select({ id: organizations.id }).from(organizations);
    const unusable: readonly unknown[] = [
      '',
      '   ',
      null,
      42,
      true,
      ['https://a.example/orca'],
      { value: 'https://a.example/orca' },
    ];

    const replies = await Promise.all(
      unusable.map((audience, index) =>
        app!.inject({
          method: 'POST',
          url: '/v1/platform/organizations',
          headers: { 'x-api-key': platformKey },
          payload: { name: `Unusable audience ${index}`, audience },
        }),
      ),
    );

    replies.forEach((reply, index) => {
      const context = `${JSON.stringify(unusable[index])}: ${reply.body}`;
      expect(reply.statusCode, context).toBe(400);
      expect(reply.json(), context).toEqual({
        error: `audience must be a non-empty string of at most ${AUDIENCE_MAX_LENGTH} characters`,
      });
    });

    const after = await db.select({ id: organizations.id }).from(organizations);
    expect(after).toHaveLength(before.length);
  });

  it('bounds the audience length inclusively, measured after trimming', async () => {
    const atLimit = `https://a.example/limit-${suffix}-`.padEnd(AUDIENCE_MAX_LENGTH, 'x');
    expect(atLimit).toHaveLength(AUDIENCE_MAX_LENGTH);

    const [accepted, refused] = await Promise.all([
      app!.inject({
        method: 'POST',
        url: '/v1/platform/organizations',
        headers: { 'x-api-key': platformKey },
        // Surrounding whitespace pushes the raw string past the bound; the
        // trimmed value is what has to be measured.
        payload: { name: `Audience at the bound ${suffix}`, audience: `  ${atLimit}  ` },
      }),
      app!.inject({
        method: 'POST',
        url: '/v1/platform/organizations',
        headers: { 'x-api-key': platformKey },
        payload: { name: 'Audience past the bound', audience: `${atLimit}x` },
      }),
    ]);

    expect(accepted.statusCode, accepted.body).toBe(201);
    expect(accepted.json().audience).toBe(atLimit);
    expect(refused.statusCode, refused.body).toBe(400);
    expect(refused.json()).toEqual({
      error: `audience must be a non-empty string of at most ${AUDIENCE_MAX_LENGTH} characters`,
    });
  });

  it('stores a trimmed audience and echoes it on the organization wire', async () => {
    const audience = `https://a.example/orca-${suffix}`;
    const create = await app!.inject({
      method: 'POST',
      url: '/v1/platform/organizations',
      headers: { 'x-api-key': platformKey },
      payload: { name: `Audience-carrying organization ${suffix}`, audience: `  ${audience}  ` },
    });

    expect(create.statusCode, create.body).toBe(201);
    expect(create.json()).toMatchObject({ type: 'organization', audience });
    const organizationId = create.json().id as string;

    const row = await db.query.organizations.findFirst({
      where: eq(organizations.id, organizationId),
    });
    expect(row?.audience).toBe(audience);

    // The audit trail has to record what was stored, not what was sent: an
    // operator reading it back is checking the value the authenticator matches
    // `aud` against.
    const events = await db
      .select()
      .from(platformAuditEvents)
      .where(eq(platformAuditEvents.targetId, organizationId));
    expect(events.map((event) => event.metadata)).toContainEqual(
      expect.objectContaining({ audience }),
    );
  });

  it('refuses a second organization claiming an assigned audience', async () => {
    // The unique index on `audience` would answer this by aborting the
    // transaction, taking the audit and idempotency writes with it and
    // surfacing as a 500. This pins the advisory-lock pre-check that turns it
    // into a clean 409 instead.
    const audience = `https://a.example/duplicate-${suffix}`;
    const holder = await app!.inject({
      method: 'POST',
      url: '/v1/platform/organizations',
      headers: { 'x-api-key': platformKey },
      payload: { name: `Audience holder ${suffix}`, audience },
    });
    expect(holder.statusCode, holder.body).toBe(201);

    // Neither call carries an idempotency key, so the 409 can only have come
    // from the audience check rather than from a replay of a stored response.
    const claimant = await app!.inject({
      method: 'POST',
      url: '/v1/platform/organizations',
      headers: { 'x-api-key': platformKey },
      payload: { name: 'Audience claimant', audience },
    });
    expect(claimant.statusCode, claimant.body).toBe(409);
    expect(claimant.json()).toEqual({
      error: 'audience is already assigned to another organization',
    });

    const rows = await db
      .select({ id: organizations.id })
      .from(organizations)
      .where(eq(organizations.audience, audience));
    expect(rows).toHaveLength(1);
    expect(rows[0]?.id).toBe(holder.json().id);
  });

  it('still rejects unrecognised fields on the widened organization body', async () => {
    const reply = await app!.inject({
      method: 'POST',
      url: '/v1/platform/organizations',
      headers: { 'x-api-key': platformKey },
      payload: {
        name: 'Unsupported fields',
        organization_id: seedOrganizationId,
        audiences: 'https://a.example/typo',
      },
    });

    expect(reply.statusCode, reply.body).toBe(400);
    expect(reply.json()).toEqual({
      error: 'unsupported request fields: audiences, organization_id',
    });
  });

  it('records deployment-wide non-secret audit events', async () => {
    const events = await db
      .select()
      .from(platformAuditEvents)
      .where(eq(platformAuditEvents.organizationId, createdOrganizationId));
    expect(events.map((event) => event.action)).toEqual(
      expect.arrayContaining(['organization.created', 'workspace.created']),
    );
    expect(JSON.stringify(events)).not.toContain(platformKey);
  });

  it('initializes an explicit raw ceiling in the organization creation transaction and never resets it on replay', async () => {
    const request = {
      method: 'POST' as const,
      url: '/v1/platform/organizations',
      headers: { 'x-api-key': platformKey, 'idempotency-key': `raw-create-${suffix}` },
      payload: { name: `Raw org ${suffix}`, capture_ceiling: 'raw_io' },
    };
    const created = await app!.inject(request);
    expect(created.statusCode, created.body).toBe(201);
    const id = created.json().id as string;
    const [setting] = await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, id));
    expect(setting).toMatchObject({
      captureCeiling: 'raw_io',
      captureRestrictionEpoch: 0,
      activeDefaultBindingId: null,
      selectionEpoch: 0,
    });
    const audit = await db
      .select()
      .from(platformAuditEvents)
      .where(eq(platformAuditEvents.targetId, id));
    expect(audit).toHaveLength(1);
    expect(audit[0]?.metadata).toMatchObject({ capture_ceiling: 'raw_io' });
    const adminKey = generateAdminApiKey();
    await db.insert(adminApiKeys).values({
      id: `adminkey_raw_${suffix}`,
      organizationId: id,
      name: 'Raw org admin',
      hashedKey: await hashAdminApiKey(adminKey),
      keyFingerprint: fingerprintAdminApiKey(adminKey),
      partialKeyHint: partialAdminApiKeyHint(adminKey),
      scopes: ['org:admin'],
      status: 'active',
      createdBy: 'test',
    });
    const state = await app!.inject({
      method: 'GET',
      url: '/v1/organizations/agent_observability',
      headers: { 'x-api-key': adminKey },
    });
    const restricted = await app!.inject({
      method: 'PUT',
      url: '/v1/organizations/agent_observability/capture_ceiling',
      headers: {
        'x-api-key': adminKey,
        'if-match': state.headers.etag!,
        'idempotency-key': `restrict-${suffix}`,
      },
      payload: { capture_ceiling: 'metadata_only' },
    });
    expect(restricted.statusCode, restricted.body).toBe(200);
    const replay = await app!.inject(request);
    expect(replay.body).toBe(created.body);
    const [after] = await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, id));
    expect(after).toMatchObject({ captureCeiling: 'metadata_only', captureRestrictionEpoch: 1 });
    expect(
      (
        await app!.inject({
          ...request,
          payload: { ...request.payload, capture_ceiling: 'metadata_only' },
        })
      ).statusCode,
    ).toBe(409);
  });

  it('validates capture initialization without granting organization principals platform authority', async () => {
    for (const capture_ceiling of [null, 'unknown', 42, {}]) {
      const response = await app!.inject({
        method: 'POST',
        url: '/v1/platform/organizations',
        headers: { 'x-api-key': platformKey },
        payload: { name: `Invalid capture ${suffix}`, capture_ceiling },
      });
      expect(response.statusCode).toBe(400);
    }
    const rejected = await app!.inject({
      method: 'POST',
      url: '/v1/platform/organizations',
      headers: { 'x-api-key': seedOrganizationAdminKey },
      payload: { name: 'Unauthorized raw org', capture_ceiling: 'raw_io' },
    });
    expect(rejected.statusCode).toBe(401);
    expect(
      await db
        .select()
        .from(organizations)
        .where(eq(organizations.name, `Invalid capture ${suffix}`)),
    ).toHaveLength(0);
  });
});
