// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { and, eq } from 'drizzle-orm';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  partialAdminApiKeyHint,
} from '../../src/auth/admin-api-key.js';
import { buildAdminApp } from '../../src/server.js';
import {
  adminApiKeys,
  adminAuditEvents,
  agentObservabilityBindingCredentials,
  agentObservabilityBindingVersions,
  agentObservabilityBindings,
  agentObservabilityCredentialStagingIntents,
  agentObservabilityIdempotencyKeys,
  agentObservabilityMutationReservations,
  agentObservabilityOrganizationSettings,
  agentObservabilitySecretCleanupOutbox,
  organizations,
} from '../../src/persistence/postgres/schema.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import type { SecretStore, SecretStorePutOptions } from '../../src/secrets/secret-provider.js';
import { decodeAgentObservabilitySecretBundle } from '../../src/domain/agent-observability-secrets.js';
import { reconcileAgentObservabilitySecretCleanup } from '../../src/domain/agent-observability-mutations.js';
import { buildStubStore, closeTestDb, getTestDb } from './setup.js';

describe('organization agent observability mutations', () => {
  let db: DbClient;
  let app: FastifyInstance;
  let organizationId: string;
  let adminKey: string;
  let secretStore: TrackingSecretStore;

  beforeAll(async () => {
    ({ db } = await getTestDb());
  });

  beforeEach(async () => {
    organizationId = `org_observability_put_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    adminKey = generateAdminApiKey();
    await db.insert(organizations).values({
      id: organizationId,
      name: `Observability PUT integration ${organizationId}`,
      status: 'active',
    });
    await insertAdminKey(db, organizationId, adminKey, [
      'observability:read',
      'observability:write',
      'observability:rotate',
    ]);
    secretStore = new TrackingSecretStore();
    app = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
      secretStore,
    });
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  afterAll(async () => {
    await closeTestDb();
  });

  it('creates once, replays authoritative state, and never stores a credential bundle outside SecretStore', async () => {
    const payload = requestBody({ password: 'secret-integration-create' });
    const create = await put(payload, 'create-replay-key');
    expect(create.statusCode).toBe(201);
    expect(create.headers['cache-control']).toBe('no-store');
    expect(JSON.stringify(create.json())).not.toContain('secret-integration-create');
    const bindingId = create.json().configured.default_binding.id as string;

    const replay = await put(payload, 'create-replay-key');
    expect(replay.statusCode).toBe(201);
    expect(replay.json()).toEqual(create.json());
    expect(secretStore.putCalls).toBe(1);

    const [credential] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));
    expect(credential).toEqual(expect.objectContaining({ credentialVersion: 1, keyHint: null }));
    expect(await secretStore.resolve(credential!.secretRef)).toContain('secret-integration-create');

    const [audit] = await db
      .select()
      .from(adminAuditEvents)
      .where(eq(adminAuditEvents.organizationId, organizationId));
    const [idempotency] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(eq(agentObservabilityIdempotencyKeys.organizationId, organizationId));
    const ordinaryRows = await Promise.all([
      db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, bindingId)),
      db
        .select()
        .from(agentObservabilityBindingVersions)
        .where(eq(agentObservabilityBindingVersions.bindingId, bindingId)),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
    ]);
    for (const value of [audit, idempotency, ordinaryRows]) {
      expect(JSON.stringify(value)).not.toContain('secret-integration-create');
    }
  });

  it('disables only the authenticated organization default with a strict empty body', async () => {
    const created = await put(
      requestBody({ password: 'secret-disable-http' }),
      'disable-http-create',
    );
    expect(created.statusCode).toBe(201);
    const bindingId = created.json().configured.default_binding.id as string;
    const state = await getState();
    const [[beforeSetting], [beforeBinding], [beforeHead]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityOrganizationSettings)
        .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId)),
      db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, bindingId)),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
    ]);

    const disabled = await disable({}, 'disable-http', state.headers.etag);
    expect(disabled.statusCode).toBe(200);
    expect(disabled.headers['cache-control']).toBe('no-store');
    expect(disabled.json()).toMatchObject({
      configured: { default_binding: null },
      effective: {
        source: 'none',
        status: 'disabled',
        disabled_reason: 'no_default_binding',
        binding: null,
      },
    });
    const [[afterSetting], [afterBinding], [afterHead], cleanup] = await Promise.all([
      db
        .select()
        .from(agentObservabilityOrganizationSettings)
        .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId)),
      db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, bindingId)),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
      db
        .select()
        .from(agentObservabilitySecretCleanupOutbox)
        .where(eq(agentObservabilitySecretCleanupOutbox.bindingId, bindingId)),
    ]);
    expect(afterSetting).toMatchObject({
      activeDefaultBindingId: null,
      activeDefaultBindingScope: null,
      selectionEpoch: beforeSetting!.selectionEpoch + 1,
      defaultRevocationEpoch: beforeSetting!.defaultRevocationEpoch + 1,
      organizationRevocationEpoch: beforeSetting!.organizationRevocationEpoch,
      captureRestrictionEpoch: beforeSetting!.captureRestrictionEpoch,
    });
    expect(afterBinding).toMatchObject({
      status: 'draining',
      currentVersion: beforeBinding!.currentVersion,
      revocationEpoch: beforeBinding!.revocationEpoch,
    });
    expect(afterHead).toEqual(beforeHead);
    expect(cleanup).toEqual([]);
    expect(secretStore.deleteCalls).toBe(0);

    const unknown = await disable({ target: { binding_id: bindingId } }, 'disable-http-invalid');
    expect(unknown.statusCode).toBe(400);
    expect(unknown.json()).toEqual({ error: 'invalid agent observability request' });
  });

  it('checks stale If-Match before reservation, staging, or SecretStore put', async () => {
    const rejected = await put(requestBody({ password: 'secret-stale' }), 'stale-key', '"stale"');
    expect(rejected.statusCode).toBe(412);
    expect(rejected.json()).toEqual({ error: 'agent observability state is stale' });
    expect(secretStore.putCalls).toBe(0);
    expect(
      await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.organizationId, organizationId)),
    ).toEqual([]);
    expect(
      await db
        .select()
        .from(agentObservabilityCredentialStagingIntents)
        .where(eq(agentObservabilityCredentialStagingIntents.candidateBindingId, 'never-created')),
    ).toEqual([]);
  });

  it('stops replaying an expired idempotency record before checking the current precondition', async () => {
    const payload = requestBody({ password: 'secret-expired-replay' });
    expect((await put(payload, 'expired-replay-key')).statusCode).toBe(201);
    await db
      .update(agentObservabilityIdempotencyKeys)
      .set({ createdAt: new Date(0), expiresAt: new Date(1) })
      .where(eq(agentObservabilityIdempotencyKeys.organizationId, organizationId));

    const retry = await put(payload, 'expired-replay-key');
    expect(retry.statusCode).toBe(428);
    expect(retry.json()).toEqual({ error: 'if-match is required' });
    expect(secretStore.putCalls).toBe(1);

    const rebind = await put(
      requestBody({ credentials: 'omit', compression: 'gzip' }),
      'expired-replay-key',
      (await getState()).headers.etag,
    );
    expect(rebind.statusCode).toBe(200);
    expect(rebind.json().configured.default_binding.config).toMatchObject({
      version: 2,
      compression: 'gzip',
    });
    expect(secretStore.putCalls).toBe(1);
  });

  it('creates an immutable same-target config version without changing credential head', async () => {
    const initial = await put(
      requestBody({
        password: 'secret-policy',
        captureCeiling: 'redacted_io',
        captureMode: 'redacted_io',
      }),
      'policy-create',
    );
    expect(initial.statusCode).toBe(201);
    const bindingId = initial.json().configured.default_binding.id as string;
    const [beforeCredential] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));
    const missingPrecondition = await put(
      requestBody({ credentials: 'omit', compression: 'gzip', sampleRate: 0.5 }),
      'policy-missing-precondition',
    );
    expect(missingPrecondition.statusCode).toBe(428);
    const current = await getState();
    const replacement = await put(
      requestBody({ credentials: 'omit', compression: 'gzip', sampleRate: 0.5 }),
      'policy-replace',
      current.headers.etag,
    );
    expect(replacement.statusCode).toBe(200);
    expect(replacement.json().configured.default_binding).toMatchObject({
      id: bindingId,
      config: { version: 2, compression: 'gzip', sample_rate: 0.5 },
      credential: { configured: true, version: 1 },
    });
    const [afterCredential] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));
    expect(afterCredential).toEqual(beforeCredential);
    expect(secretStore.putCalls).toBe(1);
    const versions = await db
      .select({ version: agentObservabilityBindingVersions.version })
      .from(agentObservabilityBindingVersions)
      .where(eq(agentObservabilityBindingVersions.bindingId, bindingId));
    expect(versions.map((row) => row.version).sort()).toEqual([1, 2]);
    const [setting] = await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId));
    expect(setting).toMatchObject({ captureCeiling: 'metadata_only', captureRestrictionEpoch: 1 });
  });

  it('treats Langfuse semantics as reversible same-target policy without replacing credentials', async () => {
    const initial = await put(
      requestBody({
        semanticProfile: 'langfuse',
        externalProjectId: 'project-langfuse',
        credentials: 'bearer',
      }),
      'langfuse-create',
    );
    expect(initial.statusCode).toBe(201);
    const bindingId = initial.json().configured.default_binding.id as string;
    const [credentialBefore] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

    const otel = await put(
      requestBody({
        semanticProfile: 'otel_genai',
        externalProjectId: 'project-langfuse',
        credentials: 'omit',
      }),
      'langfuse-to-otel',
      (await getState()).headers.etag,
    );
    expect(otel.statusCode).toBe(200);
    expect(otel.json().configured.default_binding).toMatchObject({
      id: bindingId,
      config: { version: 2, semantic_profile: 'otel_genai' },
      credential: { configured: true, version: 1 },
    });

    const langfuse = await put(
      requestBody({
        semanticProfile: 'langfuse',
        externalProjectId: 'project-langfuse',
        credentials: 'omit',
      }),
      'otel-to-langfuse',
      (await getState()).headers.etag,
    );
    expect(langfuse.statusCode).toBe(200);
    expect(langfuse.json().configured.default_binding).toMatchObject({
      id: bindingId,
      config: { version: 3, semantic_profile: 'langfuse' },
      credential: { configured: true, version: 1 },
    });
    const [credentialAfter] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));
    expect(credentialAfter).toEqual(credentialBefore);
    expect(secretStore.putCalls).toBe(1);
  });

  it('replaces target atomically and drains prior active binding without revoking it', async () => {
    const initial = await put(requestBody({ password: 'secret-old' }), 'replace-create');
    expect(initial.statusCode).toBe(201);
    const oldBindingId = initial.json().configured.default_binding.id as string;
    const current = await getState();
    const replaced = await put(
      requestBody({ endpoint: 'https://collector.example/v1/traces-next', password: 'secret-new' }),
      'replace-target',
      current.headers.etag,
    );
    expect(replaced.statusCode).toBe(200);
    const newBindingId = replaced.json().configured.default_binding.id as string;
    expect(newBindingId).not.toBe(oldBindingId);
    const [oldBinding] = await db
      .select()
      .from(agentObservabilityBindings)
      .where(eq(agentObservabilityBindings.id, oldBindingId));
    const [setting] = await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId));
    expect(oldBinding).toMatchObject({ status: 'draining', revocationEpoch: 0 });
    expect(setting).toMatchObject({
      activeDefaultBindingId: newBindingId,
      selectionEpoch: 2,
      defaultRevocationEpoch: 0,
    });
  });

  it('fails credential-bearing writes without a SecretStore before reservation', async () => {
    await app.close();
    app = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
    });
    await app.ready();
    const unavailable = await put(
      requestBody({ password: 'secret-missing-store' }),
      'missing-store',
    );
    expect(unavailable.statusCode).toBe(503);
    expect(unavailable.json()).toEqual({ error: 'agent observability unavailable' });
    expect(
      await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.organizationId, organizationId)),
    ).toEqual([]);
  });

  it('fences failed SecretStore writes and leaves cleanup to durable reconciliation', async () => {
    await app.close();
    const failingStore = new FailingSecretStore();
    app = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
      secretStore: failingStore,
    });
    await app.ready();
    const failed = await put(requestBody({ password: 'secret-put-failure' }), 'put-failure');
    expect(failed.statusCode).toBe(503);
    expect(failingStore.deleteCalls).toBe(0);
    const [reservation] = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.organizationId, organizationId));
    const [staging] = await db
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservation!.id));
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
  });

  it('returns pending conflict for same-key retries and scopes write access', async () => {
    const readOnlyKey = generateAdminApiKey();
    await insertAdminKey(db, organizationId, readOnlyKey, ['observability:read']);
    const denied = await app.inject({
      method: 'PUT',
      url: '/v1/organizations/agent_observability',
      headers: { 'x-api-key': readOnlyKey, 'idempotency-key': 'scope-denied' },
      payload: requestBody({ password: 'secret-scope' }),
    });
    expect(denied.statusCode).toBe(403);

    const gate = deferred<void>();
    secretStore.beforePut = async () => gate.promise;
    const payload = requestBody({ password: 'secret-concurrent' });
    const first = put(payload, 'concurrent-key');
    await secretStore.awaitPutStarted();
    const second = await put(payload, 'concurrent-key');
    expect(second.statusCode).toBe(409);
    gate.resolve();
    const created = await first;
    expect(created.statusCode).toBe(201);
    const replayed = await put(payload, 'concurrent-key');
    expect(replayed.statusCode).toBe(201);
    expect(replayed.json()).toEqual(created.json());
  });

  it('rotates one current default credential head and cleans its old reference only in maintenance', async () => {
    const initial = await put(requestBody({ password: 'secret-rotate-old' }), 'rotate-create');
    expect(initial.statusCode).toBe(201);
    const bindingId = initial.json().configured.default_binding.id as string;
    const [beforeHead] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));
    const [beforeSetting] = await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId));
    const [beforeBinding] = await db
      .select()
      .from(agentObservabilityBindings)
      .where(eq(agentObservabilityBindings.id, bindingId));
    const state = await getState();

    const rotated = await rotate(
      { credentials: { type: 'bearer', token: 'secret-rotate-new' } },
      'rotate-current-default',
      state.headers.etag,
    );
    expect(rotated.statusCode).toBe(200);
    expect(rotated.headers['cache-control']).toBe('no-store');
    expect(rotated.json().configured.default_binding).toMatchObject({
      id: bindingId,
      config: { version: 1 },
      credential: { configured: true, version: 2 },
    });
    expect(JSON.stringify(rotated.json())).not.toContain('secret-rotate-new');

    const [afterHead] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));
    const [afterSetting] = await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId));
    const [afterBinding] = await db
      .select()
      .from(agentObservabilityBindings)
      .where(eq(agentObservabilityBindings.id, bindingId));
    expect(afterHead).toMatchObject({ credentialVersion: 2, keyHint: null });
    expect(afterHead!.secretRef).not.toBe(beforeHead!.secretRef);
    expect(afterSetting).toMatchObject({
      activeDefaultBindingId: beforeSetting!.activeDefaultBindingId,
      selectionEpoch: beforeSetting!.selectionEpoch,
      defaultRevocationEpoch: beforeSetting!.defaultRevocationEpoch,
      organizationRevocationEpoch: beforeSetting!.organizationRevocationEpoch,
      captureCeiling: beforeSetting!.captureCeiling,
      captureRestrictionEpoch: beforeSetting!.captureRestrictionEpoch,
    });
    expect(afterBinding).toMatchObject({
      currentVersion: beforeBinding!.currentVersion,
      status: beforeBinding!.status,
      revocationEpoch: beforeBinding!.revocationEpoch,
      updatedAt: beforeBinding!.updatedAt,
    });
    const newBundle = await secretStore.resolve(afterHead!.secretRef);
    if (newBundle === null) throw new Error('expected staged rotation bundle');
    expect(
      decodeAgentObservabilitySecretBundle(newBundle, {
        bindingId,
        credentialVersion: 2,
      }),
    ).toMatchObject({ auth: { type: 'bearer', token: 'secret-rotate-new' } });

    const cleanup = await db
      .select()
      .from(agentObservabilitySecretCleanupOutbox)
      .where(eq(agentObservabilitySecretCleanupOutbox.bindingId, bindingId));
    expect(cleanup).toHaveLength(1);
    expect(cleanup[0]).toMatchObject({
      organizationId,
      bindingScope: 'organization',
      secretRef: beforeHead!.secretRef,
    });
    expect(secretStore.deleteCalls).toBe(0);
    expect(await secretStore.resolve(beforeHead!.secretRef)).not.toBeNull();

    const [audit] = await db
      .select()
      .from(adminAuditEvents)
      .where(
        and(
          eq(adminAuditEvents.organizationId, organizationId),
          eq(adminAuditEvents.action, 'organization.agent_observability.credentials_rotated'),
        ),
      );
    const [idempotency] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, 'rotate-current-default'),
        ),
      );
    expect(JSON.stringify([audit, idempotency, rotated.json()])).not.toContain('secret-rotate-new');
    expect(JSON.stringify([audit, idempotency, rotated.json()])).not.toContain(
      afterHead!.secretRef,
    );
    expect(JSON.stringify([audit, idempotency, rotated.json()])).not.toContain(
      beforeHead!.secretRef,
    );

    const maintenance = await reconcileAgentObservabilitySecretCleanup(db, secretStore);
    expect(maintenance).toMatchObject({ failed: 0, stale: 0 });
    expect(maintenance.claimed).toBeGreaterThanOrEqual(1);
    expect(maintenance.deleted).toBeGreaterThanOrEqual(1);
    expect(secretStore.deleteCalls).toBeGreaterThanOrEqual(1);
    expect(await secretStore.resolve(beforeHead!.secretRef)).toBeNull();
    expect(
      await db
        .select()
        .from(agentObservabilitySecretCleanupOutbox)
        .where(eq(agentObservabilitySecretCleanupOutbox.bindingId, bindingId)),
    ).toEqual([]);
  });

  it('replays an exact credential rotation after target replacement without another SecretStore put', async () => {
    expect(
      (await put(requestBody({ password: 'secret-replay-create' }), 'replay-create')).statusCode,
    ).toBe(201);
    const firstState = await getState();
    const rotation = {
      credentials: {
        type: 'basic' as const,
        username: 'project',
        password: 'secret-replay-rotation',
      },
    };
    const rotated = await rotate(rotation, 'rotate-replay-key', firstState.headers.etag);
    expect(rotated.statusCode).toBe(200);
    const originalResponse = rotated.json();
    const putCallsAfterRotation = secretStore.putCalls;

    const differentBody = await rotate(
      { credentials: { type: 'bearer', token: 'secret-replay-different' } },
      'rotate-replay-key',
      '"stale-but-reused-key"',
    );
    expect(differentBody.statusCode).toBe(409);

    const replacement = await put(
      requestBody({
        endpoint: 'https://collector.example/v1/traces-replay-replacement',
        password: 'secret-replay-replacement',
      }),
      'replay-target-replacement',
      (await getState()).headers.etag,
    );
    expect(replacement.statusCode).toBe(200);

    const replay = await rotate(rotation, 'rotate-replay-key');
    expect(replay.statusCode).toBe(200);
    expect(replay.json()).toEqual(originalResponse);
    expect(secretStore.putCalls).toBe(putCallsAfterRotation + 1);
  });

  it('rejects missing or stale rotation preconditions before reservation, staging, or SecretStore put', async () => {
    expect(
      (await put(requestBody({ password: 'secret-precondition-create' }), 'precondition-create'))
        .statusCode,
    ).toBe(201);
    const reservationsBefore = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.organizationId, organizationId));
    const putsBefore = secretStore.putCalls;
    const body = {
      credentials: { type: 'basic' as const, username: 'project', password: 'secret-precondition' },
    };

    const missing = await rotate(body, 'rotation-missing-precondition');
    const stale = await rotate(body, 'rotation-stale-precondition', '"stale"');
    expect(missing.statusCode).toBe(428);
    expect(stale.statusCode).toBe(412);
    expect(secretStore.putCalls).toBe(putsBefore);
    const reservationsAfter = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.organizationId, organizationId));
    expect(reservationsAfter.map((row) => row.id)).toEqual(reservationsBefore.map((row) => row.id));
  });

  it('requires observability:rotate while org:admin remains a wildcard', async () => {
    expect(
      (await put(requestBody({ password: 'secret-rotate-scope' }), 'rotate-scope-create'))
        .statusCode,
    ).toBe(201);
    const current = await getState();
    const writeOnlyKey = generateAdminApiKey();
    const rotateOnlyKey = generateAdminApiKey();
    const orgAdminKey = generateAdminApiKey();
    await Promise.all([
      insertAdminKey(db, organizationId, writeOnlyKey, ['observability:write']),
      insertAdminKey(db, organizationId, rotateOnlyKey, ['observability:rotate']),
      insertAdminKey(db, organizationId, orgAdminKey, ['org:admin']),
    ]);
    const body = { credentials: { type: 'bearer' as const, token: 'secret-rotate-scope-next' } };
    const denied = await app.inject({
      method: 'POST',
      url: '/v1/organizations/agent_observability:rotate_credentials',
      headers: {
        'x-api-key': writeOnlyKey,
        'idempotency-key': 'rotate-scope-denied',
        'if-match': current.headers.etag,
      },
      payload: body,
    });
    expect(denied.statusCode).toBe(403);
    expect(denied.json()).toEqual({ error: 'missing required scope: observability:rotate' });

    const disableDenied = await app.inject({
      method: 'POST',
      url: '/v1/organizations/agent_observability:disable',
      headers: {
        'x-api-key': rotateOnlyKey,
        'idempotency-key': 'disable-scope-denied',
      },
      payload: {},
    });
    expect(disableDenied.statusCode).toBe(403);
    expect(disableDenied.json()).toEqual({ error: 'missing required scope: observability:write' });

    const delegated = await app.inject({
      method: 'POST',
      url: '/v1/organizations/agent_observability:rotate_credentials',
      headers: {
        'x-api-key': rotateOnlyKey,
        'idempotency-key': 'rotate-scope-delegated',
        'if-match': current.headers.etag,
      },
      payload: body,
    });
    expect(delegated.statusCode).toBe(200);

    const wildcard = await app.inject({
      method: 'POST',
      url: '/v1/organizations/agent_observability:rotate_credentials',
      headers: {
        'x-api-key': orgAdminKey,
        'idempotency-key': 'rotate-scope-wildcard',
        'if-match': (await getState()).headers.etag,
      },
      payload: {
        credentials: { type: 'custom_headers', headers: { 'x-api-key': 'secret-org-admin' } },
      },
    });
    expect(wildcard.statusCode).toBe(200);

    const writeDelegated = await app.inject({
      method: 'POST',
      url: '/v1/organizations/agent_observability:disable',
      headers: {
        'x-api-key': writeOnlyKey,
        'idempotency-key': 'disable-scope-delegated',
      },
      payload: {},
    });
    expect(writeDelegated.statusCode).toBe(200);
  });

  it('sets no-store for PUT, disable, and rotation authentication, scope, and request errors through the api alias', async () => {
    const readOnlyKey = generateAdminApiKey();
    await insertAdminKey(db, organizationId, readOnlyKey, ['observability:read']);
    const putPath = '/api/v1/organizations/agent_observability';
    const disablePath = '/api/v1/organizations/agent_observability:disable';
    const rotationPath = '/api/v1/organizations/agent_observability:rotate_credentials';
    const rotationBody = { credentials: { type: 'bearer' as const, token: 'secret-cache-rotate' } };

    for (const request of [
      {
        method: 'PUT' as const,
        url: putPath,
        payload: requestBody({ password: 'secret-cache-put' }),
      },
      { method: 'POST' as const, url: disablePath, payload: {} },
      { method: 'POST' as const, url: rotationPath, payload: rotationBody },
    ]) {
      expectNoStore(
        await app.inject({
          ...request,
          headers: { 'idempotency-key': `cache-unauthenticated-${request.method}` },
        }),
        401,
      );
      expectNoStore(
        await app.inject({
          ...request,
          headers: {
            'x-api-key': readOnlyKey,
            'idempotency-key': `cache-forbidden-${request.method}`,
          },
        }),
        403,
      );
    }

    expectNoStore(
      await app.inject({
        method: 'PUT',
        url: putPath,
        headers: { 'x-api-key': adminKey, 'idempotency-key': 'cache-put-invalid' },
        payload: {},
      }),
      400,
    );
    expectNoStore(
      await app.inject({
        method: 'POST',
        url: disablePath,
        headers: { 'x-api-key': adminKey, 'idempotency-key': 'cache-disable-invalid' },
        payload: { binding_id: 'aob_not_allowed' },
      }),
      400,
    );
    expectNoStore(
      await app.inject({
        method: 'POST',
        url: rotationPath,
        headers: { 'x-api-key': adminKey, 'idempotency-key': 'cache-rotation-invalid' },
        payload: {},
      }),
      400,
    );
  });

  it('sets no-store for PUT, disable, and rotation conflicts, preconditions, and unavailable SecretStore', async () => {
    const putPath = '/v1/organizations/agent_observability';
    const disablePath = '/v1/organizations/agent_observability:disable';
    const rotationPath = '/v1/organizations/agent_observability:rotate_credentials';
    expectNoStore(await put(requestBody({ password: 'secret-cache-base' }), 'cache-base'), 201);
    const initialEtag = (await getState()).headers.etag;

    expectNoStore(
      await put(requestBody({ password: 'secret-cache-put-conflict' }), 'cache-base', '"stale"'),
      409,
    );
    expectNoStore(
      await put(
        requestBody({
          endpoint: 'https://collector.example/v1/traces-cache-put-stale',
          password: 'secret-cache-put-stale',
        }),
        'cache-put-stale',
        '"stale"',
      ),
      412,
    );
    expectNoStore(
      await put(
        requestBody({
          endpoint: 'https://collector.example/v1/traces-cache-put-missing',
          password: 'secret-cache-put-missing',
        }),
        'cache-put-missing',
      ),
      428,
    );
    expectNoStore(await disable({}, 'cache-disable-stale', '"stale"'), 412);

    const rotation = {
      credentials: { type: 'bearer' as const, token: 'secret-cache-rotation-completed' },
    };
    expectNoStore(await rotate(rotation, 'cache-rotation-completed', initialEtag), 200);
    expectNoStore(
      await rotate(
        { credentials: { type: 'bearer', token: 'secret-cache-rotation-conflict' } },
        'cache-rotation-completed',
        '"stale"',
      ),
      409,
    );
    expectNoStore(
      await rotate(
        { credentials: { type: 'bearer', token: 'secret-cache-rotation-stale' } },
        'cache-rotation-stale',
        '"stale"',
      ),
      412,
    );
    expectNoStore(
      await rotate(
        { credentials: { type: 'bearer', token: 'secret-cache-rotation-missing' } },
        'cache-rotation-missing',
      ),
      428,
    );

    const unavailableApp = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
    });
    try {
      await unavailableApp.ready();
      const currentEtag = (await getState()).headers.etag;
      expectNoStore(
        await unavailableApp.inject({
          method: 'PUT',
          url: putPath,
          headers: {
            'x-api-key': adminKey,
            'idempotency-key': 'cache-put-unavailable',
            'if-match': currentEtag,
          },
          payload: requestBody({
            endpoint: 'https://collector.example/v1/traces-cache-put-unavailable',
            password: 'secret-cache-put-unavailable',
          }),
        }),
        503,
      );
      expectNoStore(
        await unavailableApp.inject({
          method: 'POST',
          url: rotationPath,
          headers: {
            'x-api-key': adminKey,
            'idempotency-key': 'cache-rotation-unavailable',
            'if-match': currentEtag,
          },
          payload: {
            credentials: { type: 'bearer', token: 'secret-cache-rotation-unavailable' },
          },
        }),
        503,
      );
      expectNoStore(
        await unavailableApp.inject({
          method: 'POST',
          url: disablePath,
          headers: {
            'x-api-key': adminKey,
            'idempotency-key': 'cache-disable-unavailable-is-not-secret-dependent',
            'if-match': currentEtag,
          },
          payload: {},
        }),
        200,
      );
    } finally {
      await unavailableApp.close();
    }
  });

  it('sets no-store for authenticated PUT, disable, and rotation not-found responses', async () => {
    for (const request of [
      {
        method: 'PUT' as const,
        url: '/v1/organizations/agent_observability',
        payload: requestBody({ password: 'secret-cache-put-not-found' }),
      },
      {
        method: 'POST' as const,
        url: '/v1/organizations/agent_observability:disable',
        payload: {},
      },
      {
        method: 'POST' as const,
        url: '/v1/organizations/agent_observability:rotate_credentials',
        payload: {
          credentials: { type: 'bearer' as const, token: 'secret-cache-rotation-not-found' },
        },
      },
    ]) {
      const notFoundOrganizationId = `${organizationId}_${request.url
        .replace(/[^a-z]+/giu, '_')
        .slice(-32)}_not_found`;
      const notFoundKey = generateAdminApiKey();
      await db.insert(organizations).values({
        id: notFoundOrganizationId,
        name: `Observability no-store not-found integration ${notFoundOrganizationId}`,
        status: 'active',
      });
      await insertAdminKey(db, notFoundOrganizationId, notFoundKey, ['org:admin']);
      const notFoundApp = buildAdminApp({
        db,
        oidc: { allowedIssuers: [], audience: 'admin-test' },
        platformOidc: { allowedIssuers: [], audience: 'platform-test' },
        store: buildStubStore(),
        secretStore: new TrackingSecretStore(),
      });
      notFoundApp.addHook('preHandler', async (req) => {
        if (req.method !== request.method || req.url.split('?')[0] !== request.url) return;
        await db
          .update(organizations)
          .set({ status: 'archived' })
          .where(eq(organizations.id, notFoundOrganizationId));
      });
      try {
        await notFoundApp.ready();
        expectNoStore(
          await notFoundApp.inject({
            ...request,
            headers: {
              'x-api-key': notFoundKey,
              'idempotency-key': `cache-not-found-${request.method}`,
              'if-match': '"current"',
            },
          }),
          404,
        );
      } finally {
        await notFoundApp.close();
      }
    }
  });

  it('rejects rotation when no current default exists', async () => {
    const state = await getState();
    const rejected = await rotate(
      { credentials: { type: 'bearer', token: 'secret-no-default' } },
      'rotate-no-default',
      state.headers.etag,
    );
    expect(rejected.statusCode).toBe(409);
    expect(secretStore.putCalls).toBe(0);
  });

  it('does not route rotation near-misses to the credential handler', async () => {
    expect(
      (
        await put(
          requestBody({ password: 'secret-near-miss-default' }),
          'rotation-near-miss-create',
        )
      ).statusCode,
    ).toBe(201);
    const state = await getState();
    const putCallsBefore = secretStore.putCalls;
    for (const url of [
      '/v1/organizations/agent_observability:rotate_credentials-extra',
      '/v1/organizations/agent_observabilityrotate_credentials',
      '/v1/organizations/agent_observability:rotate_credential',
    ]) {
      const response = await app.inject({
        method: 'POST',
        url,
        headers: {
          'x-api-key': adminKey,
          'idempotency-key': `rotation-near-miss-${url.slice(-24)}`,
          'if-match': state.headers.etag,
        },
        payload: { credentials: { type: 'bearer', token: 'secret-near-miss' } },
      });
      expect(response.statusCode).toBe(404);
    }
    expect(secretStore.putCalls).toBe(putCallsBefore);
  });

  async function put(
    payload: ReturnType<typeof requestBody>,
    idempotencyKey: string,
    ifMatch?: string,
  ) {
    return app.inject({
      method: 'PUT',
      url: '/v1/organizations/agent_observability',
      headers: {
        'x-api-key': adminKey,
        'idempotency-key': idempotencyKey,
        ...(ifMatch === undefined ? {} : { 'if-match': ifMatch }),
      },
      payload,
    });
  }

  async function rotate(
    payload: {
      credentials:
        | { type: 'basic'; username: string; password: string }
        | { type: 'bearer'; token: string }
        | { type: 'custom_headers'; headers: Record<string, string> };
    },
    idempotencyKey: string,
    ifMatch?: string,
  ) {
    return app.inject({
      method: 'POST',
      url: '/v1/organizations/agent_observability:rotate_credentials',
      headers: {
        'x-api-key': adminKey,
        'idempotency-key': idempotencyKey,
        ...(ifMatch === undefined ? {} : { 'if-match': ifMatch }),
      },
      payload,
    });
  }

  async function disable(
    payload: Record<string, unknown>,
    idempotencyKey: string,
    ifMatch?: string,
  ) {
    return app.inject({
      method: 'POST',
      url: '/v1/organizations/agent_observability:disable',
      headers: {
        'x-api-key': adminKey,
        'idempotency-key': idempotencyKey,
        ...(ifMatch === undefined ? {} : { 'if-match': ifMatch }),
      },
      payload,
    });
  }

  async function getState() {
    const response = await app.inject({
      method: 'GET',
      url: '/v1/organizations/agent_observability',
      headers: { 'x-api-key': adminKey },
    });
    expect(response.statusCode).toBe(200);
    expect(response.headers.etag).toEqual(expect.any(String));
    return response;
  }
});

function requestBody(
  options: {
    endpoint?: string;
    password?: string;
    credentials?: 'omit' | 'bearer' | 'custom_headers';
    compression?: 'none' | 'gzip';
    sampleRate?: number;
    captureCeiling?: 'metadata_only' | 'redacted_io';
    captureMode?: 'metadata_only' | 'redacted_io';
    semanticProfile?: 'otel_genai' | 'langfuse';
    externalProjectId?: string;
  } = {},
) {
  return {
    target: {
      adapter_type: 'otlp_http' as const,
      endpoint_kind: 'traces_endpoint' as const,
      endpoint_class: 'public' as const,
      endpoint_url: options.endpoint ?? 'https://collector.example/v1/traces',
      ...(options.externalProjectId === undefined
        ? {}
        : { external_project_id: options.externalProjectId }),
    },
    config: {
      semantic_profile: options.semanticProfile ?? ('otel_genai' as const),
      protocol: 'http/protobuf' as const,
      compression: options.compression ?? 'none',
      timeout_ms: 5000,
      capture_mode: options.captureMode ?? ('metadata_only' as const),
      sample_rate: options.sampleRate ?? 1,
    },
    capture_ceiling: options.captureCeiling ?? ('metadata_only' as const),
    ...(options.credentials === 'omit'
      ? {}
      : options.credentials === 'bearer'
        ? { credentials: { type: 'bearer' as const, token: options.password ?? 'secret' } }
        : options.credentials === 'custom_headers'
          ? {
              credentials: {
                type: 'custom_headers' as const,
                headers: { 'x-api-key': options.password ?? 'secret' },
              },
            }
          : {
              credentials: {
                type: 'basic' as const,
                username: 'project',
                password: options.password ?? 'secret',
              },
            }),
  };
}

async function insertAdminKey(
  db: DbClient,
  organizationId: string,
  plaintext: string,
  scopes: string[],
): Promise<void> {
  await db.insert(adminApiKeys).values({
    id: `adminkey_observability_${Date.now()}_${Math.random().toString(16).slice(2)}`,
    organizationId,
    name: 'Observability mutation integration key',
    hashedKey: await hashAdminApiKey(plaintext),
    keyFingerprint: fingerprintAdminApiKey(plaintext),
    partialKeyHint: partialAdminApiKeyHint(plaintext),
    scopes,
    status: 'active',
    createdBy: 'integration-test',
  });
}

function expectNoStore(
  response: { statusCode: number; headers: { ['cache-control']?: unknown } },
  statusCode: number,
): void {
  expect(response.statusCode).toBe(statusCode);
  expect(response.headers['cache-control']).toBe('no-store');
}

class TrackingSecretStore implements SecretStore {
  readonly values = new Map<string, string>();
  private readonly putStarted = deferred<void>();
  private putStartedSignaled = false;
  putCalls = 0;
  deleteCalls = 0;
  beforePut: (() => Promise<void>) | undefined;

  async put(reference: string, value: string, options?: SecretStorePutOptions): Promise<void> {
    this.putCalls += 1;
    if (!this.putStartedSignaled) {
      this.putStartedSignaled = true;
      this.putStarted.resolve();
    }
    options?.signal?.throwIfAborted();
    await this.beforePut?.();
    options?.signal?.throwIfAborted();
    this.values.set(reference, value);
  }

  async resolve(reference: string): Promise<string | null> {
    return this.values.get(reference) ?? null;
  }

  async delete(reference: string): Promise<void> {
    this.deleteCalls += 1;
    this.values.delete(reference);
  }

  awaitPutStarted(): Promise<void> {
    return this.putStarted.promise;
  }
}

class FailingSecretStore extends TrackingSecretStore {
  override async put(
    _reference: string,
    _value: string,
    options?: SecretStorePutOptions,
  ): Promise<void> {
    this.putCalls += 1;
    options?.signal?.throwIfAborted();
    throw new Error('secret store rejected write');
  }
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}
