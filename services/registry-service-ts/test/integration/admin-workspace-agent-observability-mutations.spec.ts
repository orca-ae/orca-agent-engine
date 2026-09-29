// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { and, eq, sql } from 'drizzle-orm';
import {
  fingerprintAdminApiKey,
  generateAdminApiKey,
  hashAdminApiKey,
  partialAdminApiKeyHint,
} from '../../src/auth/admin-api-key.js';
import { buildAdminApp } from '../../src/server.js';
import {
  adminApiKeys,
  agentObservabilityBindingCredentials,
  agentObservabilityBindings,
  agentObservabilityCredentialStagingIntents,
  agentObservabilityMutationReservations,
  agentObservabilitySecretCleanupOutbox,
  agentObservabilityWorkspaceSettings,
  organizations,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import type { SecretStore, SecretStorePutOptions } from '../../src/secrets/secret-provider.js';
import { agentObservabilityStateEtag } from '../../src/domain/agent-observability-etag.js';
import { loadWorkspaceAgentObservabilityState } from '../../src/domain/agent-observability-state.js';
import { executeWorkspaceAgentObservabilityPut } from '../../src/domain/agent-observability-workspace-service.js';
import { parseWorkspaceAgentObservabilityPutRequest } from '../../src/domain/agent-observability-workspace-mutation.js';
import type { WorkspaceAgentObservabilityMutationFailureEvent } from '../../src/domain/agent-observability-workspace-service-common.js';
import {
  createFreshWorkspaceMutationTestDb,
  type FreshWorkspaceMutationTestDb,
} from './agent-observability-workspace-mutation-test-db.js';
import { buildStubStore } from './setup.js';

describe('workspace agent observability PUT admin route', () => {
  let db: DbClient;
  let testDb: FreshWorkspaceMutationTestDb;
  let app: FastifyInstance;
  let organizationId: string;
  let workspaceId: string;
  let adminKey: string;
  let secretStore: TrackingSecretStore;

  beforeAll(async () => {
    testDb = await createFreshWorkspaceMutationTestDb('workspace_mutation_route');
    db = testDb.db;
  });

  beforeEach(async () => {
    organizationId = `org_workspace_route_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    workspaceId = `ws_workspace_route_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    adminKey = generateAdminApiKey();
    await db.insert(organizations).values({
      id: organizationId,
      name: `Workspace observability route integration ${organizationId}`,
      status: 'active',
    });
    await db.insert(workspaces).values({
      id: workspaceId,
      organizationId,
      name: 'Workspace observability route integration',
      status: 'active',
      createdBy: 'integration-test',
    });
    await insertAdminKey(db, organizationId, adminKey, [
      'observability:read',
      'observability:write',
      'observability:rotate',
      'workspaces:write',
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
    await testDb.close();
  });

  it('serves canonical and /api/v1 PUTs with authoritative replay and no-store', async () => {
    const state = await getState();
    const payload = requestBody({ password: 'workspace-route-secret' });
    const created = await put(
      `/api/v1/organizations/workspaces/${workspaceId}/agent_observability`,
      {
        payload,
        idempotencyKey: '  workspace-route-create  ',
        ifMatch: state.headers.etag as string,
      },
    );
    expectNoStore(created, 201);
    expect(created.json()).toMatchObject({
      scope: 'workspace',
      organization_id: organizationId,
      workspace_id: workspaceId,
      configured: {
        mode: 'custom',
        binding: { scope: 'workspace', credential: { configured: true, version: 1 } },
      },
      effective: { source: 'workspace_custom', status: 'enabled' },
    });
    expect(created.body).not.toContain('workspace-route-secret');

    const replay = await put(`/v1/organizations/workspaces/${workspaceId}/agent_observability`, {
      payload,
      idempotencyKey: 'workspace-route-create',
    });
    expectNoStore(replay, 201);
    expect(replay.json()).toEqual(created.json());
    expect(secretStore.putCalls).toBe(1);
  });

  it('serves workspace PUT through a listening admin socket', async () => {
    const baseUrl = await app.listen({ port: 0, host: '127.0.0.1' });
    const state = await fetch(
      `${baseUrl}/v1/organizations/workspaces/${workspaceId}/agent_observability`,
      { headers: { 'x-api-key': adminKey } },
    );
    expect(state.status).toBe(200);
    const response = await fetch(
      `${baseUrl}/v1/organizations/workspaces/${workspaceId}/agent_observability`,
      {
        method: 'PUT',
        headers: {
          'content-type': 'application/json',
          'x-api-key': adminKey,
          'idempotency-key': 'workspace-route-listening-socket',
          'if-match': state.headers.get('etag')!,
        },
        body: JSON.stringify(requestBody({ password: 'workspace-route-socket-secret' })),
      },
    );
    expect(response.status).toBe(201);
    expect(response.headers.get('cache-control')).toBe('no-store');
    expect(await response.json()).toMatchObject({
      configured: { mode: 'custom' },
      effective: { source: 'workspace_custom', status: 'enabled' },
    });

    const rotatedState = await fetch(
      `${baseUrl}/v1/organizations/workspaces/${workspaceId}/agent_observability`,
      { headers: { 'x-api-key': adminKey } },
    );
    const rotated = await fetch(
      `${baseUrl}/v1/organizations/workspaces/${workspaceId}/agent_observability:rotate_credentials`,
      {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-api-key': adminKey,
          'idempotency-key': 'workspace-route-rotate-listening-socket',
          'if-match': rotatedState.headers.get('etag')!,
        },
        body: JSON.stringify(rotationBody('workspace-route-rotate-socket-secret')),
      },
    );
    expect(rotated.status).toBe(200);
    expect(rotated.headers.get('cache-control')).toBe('no-store');
    expect(await rotated.json()).toMatchObject({
      configured: { mode: 'custom', binding: { credential: { version: 2 } } },
      effective: { source: 'workspace_custom' },
    });
  });

  it('rotates only a custom workspace head and replays historical state after replacement', async () => {
    const created = await put(`/v1/organizations/workspaces/${workspaceId}/agent_observability`, {
      payload: requestBody({ password: 'workspace-route-rotate-create-secret' }),
      idempotencyKey: 'workspace-route-rotate-create',
      ifMatch: (await getState()).headers.etag as string,
    });
    expectNoStore(created, 201);
    const bindingId = created.json().configured.binding.id as string;
    const [oldHead] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));
    if (!oldHead) throw new Error('missing custom workspace credential head');

    const rotated = await rotate(
      `/api/v1/organizations/workspaces/${workspaceId}/agent_observability:rotate_credentials`,
      {
        payload: rotationBody('workspace-route-rotate-next-secret'),
        idempotencyKey: '  workspace-route-rotate  ',
        ifMatch: (await getState()).headers.etag as string,
      },
    );
    expectNoStore(rotated, 200);
    expect(rotated.json()).toMatchObject({
      scope: 'workspace',
      organization_id: organizationId,
      workspace_id: workspaceId,
      configured: {
        mode: 'custom',
        binding: { id: bindingId, config: { version: 1 }, credential: { version: 2 } },
      },
      effective: { source: 'workspace_custom' },
    });
    expect(rotated.body).not.toContain('workspace-route-rotate-next-secret');
    expect(await secretStore.resolve(oldHead.secretRef)).not.toBeNull();
    expect(
      await db
        .select()
        .from(agentObservabilitySecretCleanupOutbox)
        .where(eq(agentObservabilitySecretCleanupOutbox.secretRef, oldHead.secretRef)),
    ).toMatchObject([
      {
        organizationId,
        bindingId,
        bindingScope: 'workspace',
        status: 'pending',
      },
    ]);

    const replacement = await put(
      `/v1/organizations/workspaces/${workspaceId}/agent_observability`,
      {
        payload: requestBody({
          endpoint: 'https://collector.example/v1/traces-workspace-route-replaced',
          password: 'workspace-route-rotate-replacement-secret',
        }),
        idempotencyKey: 'workspace-route-rotate-replacement',
        ifMatch: (await getState()).headers.etag as string,
      },
    );
    expectNoStore(replacement, 200);
    expect(replacement.json().configured.binding.id).not.toBe(bindingId);

    const disabled = await put(`/v1/organizations/workspaces/${workspaceId}/agent_observability`, {
      payload: modeBody('disabled'),
      idempotencyKey: 'workspace-route-rotate-disabled',
      ifMatch: (await getState()).headers.etag as string,
    });
    expectNoStore(disabled, 200);
    expect(disabled.json()).toMatchObject({ configured: { mode: 'disabled' } });

    const replay = await rotate(
      `/v1/organizations/workspaces/${workspaceId}/agent_observability:rotate_credentials`,
      {
        payload: rotationBody('workspace-route-rotate-next-secret'),
        idempotencyKey: 'workspace-route-rotate',
      },
    );
    expectNoStore(replay, 200);
    expect(replay.json()).toEqual(rotated.json());
    expect(secretStore.putCalls).toBe(3);

    const differentBody = await rotate(
      `/v1/organizations/workspaces/${workspaceId}/agent_observability:rotate_credentials`,
      {
        payload: rotationBody('workspace-route-rotate-different-body-secret'),
        idempotencyKey: 'workspace-route-rotate',
        ifMatch: '"stale"',
      },
    );
    expectNoStore(differentBody, 409);
    expect(differentBody.body).not.toContain('workspace-route-rotate-different-body-secret');
  });

  it('returns route-level 409 for a live exact workspace rotation retry before staging another secret', async () => {
    const created = await put(`/v1/organizations/workspaces/${workspaceId}/agent_observability`, {
      payload: requestBody({ password: 'workspace-route-rotate-pending-create-secret' }),
      idempotencyKey: 'workspace-route-rotate-pending-create',
      ifMatch: (await getState()).headers.etag as string,
    });
    expectNoStore(created, 201);
    const etag = (await getState()).headers.etag as string;
    const stagingStarted = deferred<void>();
    const releaseStaging = deferred<void>();
    secretStore.beforePut = async () => {
      stagingStarted.resolve();
      await releaseStaging.promise;
    };
    const payload = rotationBody('workspace-route-rotate-pending-next-secret');
    const first = rotate(
      `/v1/organizations/workspaces/${workspaceId}/agent_observability:rotate_credentials`,
      {
        payload,
        idempotencyKey: 'workspace-route-rotate-pending',
        ifMatch: etag,
      },
    );
    try {
      await stagingStarted.promise;
      const retry = await rotate(
        `/v1/organizations/workspaces/${workspaceId}/agent_observability:rotate_credentials`,
        {
          payload,
          idempotencyKey: 'workspace-route-rotate-pending',
          ifMatch: '"stale"',
        },
      );
      expectNoStore(retry, 409);
      expect(secretStore.putCalls).toBe(2);
      releaseStaging.resolve();
      expectNoStore(await first, 200);
    } finally {
      releaseStaging.resolve();
    }
  });

  it('uses strict body/header/auth/path errors and marks every outcome no-store', async () => {
    const path = `/v1/organizations/workspaces/${workspaceId}/agent_observability`;
    const state = await getState();
    const headers = {
      'idempotency-key': 'workspace-route-error',
      'if-match': state.headers.etag as string,
    };
    const noAuth = await app.inject({
      method: 'PUT',
      url: path,
      headers,
      payload: modeBody('inherit'),
    });
    expectNoStore(noAuth, 401);

    const readOnlyKey = generateAdminApiKey();
    await insertAdminKey(db, organizationId, readOnlyKey, ['observability:read']);
    const noScope = await app.inject({
      method: 'PUT',
      url: path,
      headers: { ...headers, 'x-api-key': readOnlyKey },
      payload: modeBody('inherit'),
    });
    expectNoStore(noScope, 403);

    const malformed = await put(path, {
      payload: { ...modeBody('inherit'), target: requestBody().target },
      idempotencyKey: 'workspace-route-malformed',
      ifMatch: state.headers.etag as string,
    });
    expectNoStore(malformed, 400);
    expect(malformed.json()).toEqual({ error: 'invalid agent observability request' });

    const malformedJson = await app.inject({
      method: 'PUT',
      url: path,
      headers: {
        ...headers,
        'x-api-key': adminKey,
        'content-type': 'application/json',
      },
      payload: '{',
    });
    expectNoStore(malformedJson, 400);

    const missingKey = await app.inject({
      method: 'PUT',
      url: path,
      headers: { 'x-api-key': adminKey, 'if-match': state.headers.etag as string },
      payload: modeBody('inherit'),
    });
    expectNoStore(missingKey, 400);

    const missingIfMatch = await put(path, {
      payload: modeBody('inherit'),
      idempotencyKey: 'workspace-route-missing-if-match',
    });
    expectNoStore(missingIfMatch, 428);

    const stale = await put(path, {
      payload: modeBody('inherit'),
      idempotencyKey: 'workspace-route-stale',
      ifMatch: '"stale"',
    });
    expectNoStore(stale, 412);

    const foreign = await put(
      '/v1/organizations/workspaces/ws_workspace_route_missing/agent_observability',
      {
        payload: modeBody('inherit'),
        idempotencyKey: 'workspace-route-foreign',
        ifMatch: state.headers.etag as string,
      },
    );
    expectNoStore(foreign, 404);
    expect(foreign.json()).toEqual({ error: 'workspace not found' });
  });

  it('uses strict workspace rotation body/header/auth/path errors and marks every outcome no-store', async () => {
    const path = `/v1/organizations/workspaces/${workspaceId}/agent_observability:rotate_credentials`;
    const state = await getState();
    const headers = {
      'idempotency-key': 'workspace-rotate-route-error',
      'if-match': state.headers.etag as string,
    };
    const noAuth = await app.inject({
      method: 'POST',
      url: path,
      headers,
      payload: rotationBody(),
    });
    expectNoStore(noAuth, 401);

    const writeOnlyKey = generateAdminApiKey();
    await insertAdminKey(db, organizationId, writeOnlyKey, ['observability:write']);
    const noScope = await app.inject({
      method: 'POST',
      url: path,
      headers: { ...headers, 'x-api-key': writeOnlyKey },
      payload: rotationBody(),
    });
    expectNoStore(noScope, 403);

    const malformed = await rotate(path, {
      payload: { ...rotationBody(), target: requestBody().target },
      idempotencyKey: 'workspace-rotate-route-malformed',
      ifMatch: state.headers.etag as string,
    });
    expectNoStore(malformed, 400);

    const malformedJson = await app.inject({
      method: 'POST',
      url: path,
      headers: {
        ...headers,
        'x-api-key': adminKey,
        'content-type': 'application/json',
      },
      payload: '{',
    });
    expectNoStore(malformedJson, 400);

    const missingKey = await app.inject({
      method: 'POST',
      url: path,
      headers: { 'x-api-key': adminKey, 'if-match': state.headers.etag as string },
      payload: rotationBody(),
    });
    expectNoStore(missingKey, 400);

    const missingIfMatch = await rotate(path, {
      payload: rotationBody(),
      idempotencyKey: 'workspace-rotate-route-missing-if-match',
    });
    expectNoStore(missingIfMatch, 428);

    const stale = await rotate(path, {
      payload: rotationBody(),
      idempotencyKey: 'workspace-rotate-route-stale',
      ifMatch: '"stale"',
    });
    expectNoStore(stale, 412);

    const ineligible = await rotate(path, {
      payload: rotationBody(),
      idempotencyKey: 'workspace-rotate-route-ineligible',
      ifMatch: state.headers.etag as string,
    });
    expectNoStore(ineligible, 409);

    const foreign = await rotate(
      '/v1/organizations/workspaces/ws_workspace_rotate_route_missing/agent_observability:rotate_credentials',
      {
        payload: rotationBody(),
        idempotencyKey: 'workspace-rotate-route-foreign',
        ifMatch: state.headers.etag as string,
      },
    );
    expectNoStore(foreign, 404);
    expect(foreign.json()).toEqual({ error: 'workspace not found' });
  });

  it('fails credential-bearing custom replacement closed when no SecretStore exists', async () => {
    const noSecretApp = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
    });
    await noSecretApp.ready();
    try {
      const state = await getState(noSecretApp);
      const response = await noSecretApp.inject({
        method: 'PUT',
        url: `/v1/organizations/workspaces/${workspaceId}/agent_observability`,
        headers: {
          'x-api-key': adminKey,
          'idempotency-key': 'workspace-route-no-secret-store',
          'if-match': state.headers.etag as string,
        },
        payload: requestBody({ password: 'workspace-route-no-store-secret' }),
      });
      expectNoStore(response, 503);
      expect(response.body).not.toContain('workspace-route-no-store-secret');
    } finally {
      await noSecretApp.close();
    }
  });

  it('fails a credential rotation closed with no SecretStore before it reserves or stages', async () => {
    const created = await put(`/v1/organizations/workspaces/${workspaceId}/agent_observability`, {
      payload: requestBody({ password: 'workspace-route-rotate-no-store-current-secret' }),
      idempotencyKey: 'workspace-route-rotate-no-store-create',
      ifMatch: (await getState()).headers.etag as string,
    });
    expectNoStore(created, 201);
    const noSecretApp = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: buildStubStore(),
    });
    await noSecretApp.ready();
    try {
      const state = await getState(noSecretApp);
      const beforeReservations = await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.workspaceId, workspaceId));
      const beforeStaging = await db.select().from(agentObservabilityCredentialStagingIntents);
      const response = await noSecretApp.inject({
        method: 'POST',
        url: `/v1/organizations/workspaces/${workspaceId}/agent_observability:rotate_credentials`,
        headers: {
          'x-api-key': adminKey,
          'idempotency-key': 'workspace-route-rotate-no-secret-store',
          'if-match': state.headers.etag as string,
        },
        payload: rotationBody('workspace-route-rotate-no-store-next-secret'),
      });
      expectNoStore(response, 503);
      expect(response.body).not.toContain('workspace-route-rotate-no-store-next-secret');
      const afterReservations = await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.workspaceId, workspaceId));
      expect(afterReservations).toEqual(beforeReservations);
      expect(await db.select().from(agentObservabilityCredentialStagingIntents)).toEqual(
        beforeStaging,
      );
    } finally {
      await noSecretApp.close();
    }
  });

  it('does not replay a completed request through an archived workspace path', async () => {
    const state = await getState();
    const payload = requestBody({ password: 'workspace-route-archive-secret' });
    const created = await put(`/v1/organizations/workspaces/${workspaceId}/agent_observability`, {
      payload,
      idempotencyKey: 'workspace-route-archive-replay',
      ifMatch: state.headers.etag as string,
    });
    expectNoStore(created, 201);
    await db
      .update(workspaces)
      .set({ status: 'archived', archivedAt: new Date() })
      .where(eq(workspaces.id, workspaceId));

    const replay = await put(`/v1/organizations/workspaces/${workspaceId}/agent_observability`, {
      payload,
      idempotencyKey: 'workspace-route-archive-replay',
    });
    expectNoStore(replay, 404);
    expect(replay.json()).toEqual({ error: 'workspace not found' });
  });

  it('does not replay a completed credential rotation through an archived workspace path', async () => {
    const created = await put(`/v1/organizations/workspaces/${workspaceId}/agent_observability`, {
      payload: requestBody({ password: 'workspace-route-rotate-archive-current-secret' }),
      idempotencyKey: 'workspace-route-rotate-archive-create',
      ifMatch: (await getState()).headers.etag as string,
    });
    expectNoStore(created, 201);
    const payload = rotationBody('workspace-route-rotate-archive-next-secret');
    const completed = await rotate(
      `/v1/organizations/workspaces/${workspaceId}/agent_observability:rotate_credentials`,
      {
        payload,
        idempotencyKey: 'workspace-route-rotate-archive',
        ifMatch: (await getState()).headers.etag as string,
      },
    );
    expectNoStore(completed, 200);
    await db
      .update(workspaces)
      .set({ status: 'archived', archivedAt: new Date() })
      .where(eq(workspaces.id, workspaceId));

    const replay = await rotate(
      `/v1/organizations/workspaces/${workspaceId}/agent_observability:rotate_credentials`,
      { payload, idempotencyKey: 'workspace-route-rotate-archive' },
    );
    expectNoStore(replay, 404);
    expect(replay.json()).toEqual({ error: 'workspace not found' });
    expect(replay.body).not.toContain('workspace-route-rotate-archive-next-secret');
  });

  it('returns no-store 404 and settles staged credentials when real archive wins PUT finalization', async () => {
    let archiveResponse: Awaited<ReturnType<typeof app.inject>> | undefined;
    secretStore.beforePut = async () => {
      archiveResponse = await app.inject({
        method: 'POST',
        url: `/v1/organizations/workspaces/${workspaceId}/archive`,
        headers: { 'x-api-key': adminKey },
      });
    };

    const putResponse = await put(
      `/v1/organizations/workspaces/${workspaceId}/agent_observability`,
      {
        payload: requestBody({ password: 'workspace-route-archive-race-secret' }),
        idempotencyKey: 'workspace-route-archive-race',
        ifMatch: (await getState()).headers.etag as string,
      },
    );

    expect(archiveResponse).toBeDefined();
    expect(archiveResponse!.statusCode).toBe(200);
    expectNoStore(putResponse, 404);
    expect(putResponse.json()).toEqual({ error: 'workspace not found' });
    expect(putResponse.body).not.toContain('workspace-route-archive-race-secret');
    const [[workspace], [setting], [reservation]] = await Promise.all([
      db.select().from(workspaces).where(eq(workspaces.id, workspaceId)),
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId)),
      db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(
          and(
            eq(agentObservabilityMutationReservations.organizationId, organizationId),
            eq(agentObservabilityMutationReservations.workspaceId, workspaceId),
          ),
        ),
    ]);
    expect(workspace).toMatchObject({ status: 'archived' });
    expect(setting).toMatchObject({ mode: 'inherit', revocationEpoch: 1 });
    expect(reservation).toMatchObject({ status: 'fenced' });
    const [staging] = await db
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservation!.id));
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
    expect(
      await db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, staging!.candidateBindingId)),
    ).toEqual([]);
  });

  it('retries a real archive-first 40001 finalizer then returns 404 with exact cleanup handoff', async () => {
    const finalizationReady = deferred<void>();
    const releaseFinalization = deferred<void>();
    const barrierLocked = deferred<void>();
    const releaseBarrier = deferred<void>();
    const firstFinalizerPid = deferred<number>();
    const attempts: number[] = [];
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const state = await loadWorkspaceAgentObservabilityState({ db, organizationId, workspaceId });
    const ordinary = executeWorkspaceAgentObservabilityPut({
      db,
      secretStore,
      organizationId,
      workspaceId,
      principal: 'workspace-route-race-test',
      authMethod: 'admin_api_key',
      requestId: 'request_workspace-route-archive-40001',
      request: parseWorkspaceAgentObservabilityPutRequest(
        requestBody({ password: 'workspace-route-archive-40001-secret' }),
      ),
      idempotencyKey: 'workspace-route-archive-40001',
      ifMatch: agentObservabilityStateEtag(state.etagInput),
      reporter: { report: (event) => reports.push(event) },
      hooks: {
        beforeFinalizationAuthorityLock: async () => {
          finalizationReady.resolve();
          await releaseFinalization.promise;
        },
        beforeFinalizationTransaction: async ({ attempt, tx }) => {
          attempts.push(attempt);
          if (attempt === 1) firstFinalizerPid.resolve(await finalizationBackendPid(tx));
        },
      },
    });

    try {
      await finalizationReady.promise;
      const barrier = db.transaction(async (tx) => {
        await tx
          .select()
          .from(workspaces)
          .where(and(eq(workspaces.id, workspaceId), eq(workspaces.organizationId, organizationId)))
          .for('update');
        barrierLocked.resolve();
        await releaseBarrier.promise;
      });
      await barrierLocked.promise;
      const archive = app.inject({
        method: 'POST',
        url: `/v1/organizations/workspaces/${workspaceId}/archive`,
        headers: { 'x-api-key': adminKey },
      });
      await waitForAnyBlockedDatabaseLock();
      releaseFinalization.resolve();
      await waitForFinalizationAuthorityLock(await firstFinalizerPid.promise);
      releaseBarrier.resolve();
      await barrier;

      expect((await archive).statusCode).toBe(200);
      await expect(ordinary).resolves.toEqual({ kind: 'not_found' });
      expect(attempts).toEqual([1, 2]);
      expect(reports).toEqual([]);
      const [reservation] = await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(
          and(
            eq(agentObservabilityMutationReservations.organizationId, organizationId),
            eq(agentObservabilityMutationReservations.workspaceId, workspaceId),
            eq(agentObservabilityMutationReservations.ownerPrincipal, 'workspace-route-race-test'),
          ),
        );
      if (!reservation) throw new Error('missing archive-race reservation');
      const [[staging], candidates] = await Promise.all([
        db
          .select()
          .from(agentObservabilityCredentialStagingIntents)
          .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservation.id)),
        db
          .select()
          .from(agentObservabilityBindings)
          .where(eq(agentObservabilityBindings.workspaceId, workspaceId)),
      ]);
      expect(reservation).toMatchObject({ status: 'fenced' });
      expect(staging).toMatchObject({ status: 'cleanup_pending' });
      expect(candidates).toEqual([]);
    } finally {
      releaseFinalization.resolve();
      releaseBarrier.resolve();
    }
  });

  async function put(
    url: string,
    input: {
      payload: Record<string, unknown>;
      idempotencyKey: string;
      ifMatch?: string;
    },
  ) {
    return app.inject({
      method: 'PUT',
      url,
      headers: {
        'x-api-key': adminKey,
        'idempotency-key': input.idempotencyKey,
        ...(input.ifMatch === undefined ? {} : { 'if-match': input.ifMatch }),
      },
      payload: input.payload,
    });
  }

  async function rotate(
    url: string,
    input: {
      payload: Record<string, unknown>;
      idempotencyKey: string;
      ifMatch?: string;
    },
  ) {
    return app.inject({
      method: 'POST',
      url,
      headers: {
        'x-api-key': adminKey,
        'idempotency-key': input.idempotencyKey,
        ...(input.ifMatch === undefined ? {} : { 'if-match': input.ifMatch }),
      },
      payload: input.payload,
    });
  }

  async function getState(targetApp: FastifyInstance = app) {
    const response = await targetApp.inject({
      method: 'GET',
      url: `/v1/organizations/workspaces/${workspaceId}/agent_observability`,
      headers: { 'x-api-key': adminKey },
    });
    expect(response.statusCode).toBe(200);
    expect(response.headers.etag).toEqual(expect.any(String));
    return response;
  }

  async function finalizationBackendPid(tx: Pick<DbClient, 'execute'>): Promise<number> {
    const result = await tx.execute<{ pid: number }>(sql`select pg_backend_pid() as pid`);
    const pid = result.rows[0]?.pid;
    if (typeof pid !== 'number' || !Number.isSafeInteger(pid) || pid <= 0) {
      throw new Error('missing archive-race finalization backend pid');
    }
    return pid;
  }

  async function waitForAnyBlockedDatabaseLock(): Promise<void> {
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      const result = await db.execute<{ waiting: boolean }>(sql`
        select exists(
          select 1
          from pg_locks locks
          inner join pg_stat_activity activity on activity.pid = locks.pid
          where not locks.granted
            and activity.datname = current_database()
        ) as waiting
      `);
      if (result.rows[0]?.waiting === true) return;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error('archive did not block behind workspace authority barrier');
  }

  async function waitForFinalizationAuthorityLock(pid: number): Promise<void> {
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      const result = await db.execute<{ waiting: boolean }>(sql`
        select exists(
          select 1
          from pg_locks
          where pid = ${pid}
            and not granted
        ) as waiting
      `);
      if (result.rows[0]?.waiting === true) return;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error('archive-race finalizer did not block on workspace authority');
  }
});

function requestBody(
  options: { endpoint?: string; password?: string } = {},
): Record<string, unknown> {
  return {
    mode: 'custom',
    target: {
      adapter_type: 'otlp_http',
      endpoint_kind: 'traces_endpoint',
      endpoint_class: 'public',
      endpoint_url: options.endpoint ?? 'https://collector.example/v1/traces',
    },
    config: {
      semantic_profile: 'otel_genai',
      protocol: 'http/protobuf',
      compression: 'none',
      timeout_ms: 5000,
      capture_mode: 'metadata_only',
      sample_rate: 1,
    },
    capture_ceiling: 'metadata_only',
    credentials: {
      type: 'basic',
      username: 'project',
      password: options.password ?? 'secret',
    },
  };
}

function rotationBody(token = 'workspace-rotation-secret'): Record<string, unknown> {
  return { credentials: { type: 'bearer', token } };
}

function modeBody(mode: 'inherit' | 'disabled'): Record<string, unknown> {
  return { mode, capture_ceiling: 'metadata_only' };
}

async function insertAdminKey(
  db: DbClient,
  organizationId: string,
  plaintext: string,
  scopes: string[],
): Promise<void> {
  await db.insert(adminApiKeys).values({
    id: `adminkey_workspace_route_${Date.now()}_${Math.random().toString(16).slice(2)}`,
    organizationId,
    name: 'Workspace observability route integration key',
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

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

class TrackingSecretStore implements SecretStore {
  readonly values = new Map<string, string>();
  beforePut: (() => Promise<void>) | undefined;
  putCalls = 0;

  async put(reference: string, value: string, options?: SecretStorePutOptions): Promise<void> {
    options?.signal?.throwIfAborted();
    this.putCalls += 1;
    this.values.set(reference, value);
    await this.beforePut?.();
    options?.signal?.throwIfAborted();
  }

  async resolve(reference: string): Promise<string | null> {
    return this.values.get(reference) ?? null;
  }

  async delete(reference: string): Promise<void> {
    this.values.delete(reference);
  }
}
