// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { and, eq, sql } from 'drizzle-orm';
import { WorkspaceAgentObservabilityStateSchema } from '../../src/contracts/agent-observability.contract.js';
import {
  adminAuditEvents,
  agentObservabilityBindingCredentials,
  agentObservabilityBindingVersions,
  agentObservabilityBindings,
  agentObservabilityCredentialStagingIntents,
  agentObservabilityIdempotencyKeys,
  agentObservabilityMutationReservations,
  agentObservabilityPlatformPolicy,
  agentObservabilitySecretCleanupOutbox,
  agentObservabilityWorkspaceSettings,
  organizations,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import {
  parseWorkspaceAgentObservabilityPutRequest,
  parseWorkspaceAgentObservabilityCredentialRotationRequest,
  classifyWorkspaceAgentObservabilityMutation,
  lockWorkspaceAgentObservabilityMutationAuthority,
  type NormalizedWorkspaceAgentObservabilityPutRequest,
} from '../../src/domain/agent-observability-workspace-mutation.js';
import { parseOrganizationAgentObservabilityPutRequest } from '../../src/domain/agent-observability-organization-mutation.js';
import { executeOrganizationAgentObservabilityPut } from '../../src/domain/agent-observability-organization-service.js';
import {
  executeWorkspaceAgentObservabilityPut,
  type ExecuteWorkspaceAgentObservabilityPutResult,
} from '../../src/domain/agent-observability-workspace-service.js';
import {
  executeWorkspaceAgentObservabilityCredentialRotation,
  type ExecuteWorkspaceAgentObservabilityCredentialRotationResult,
} from '../../src/domain/agent-observability-workspace-rotation-service.js';
import type {
  WorkspaceAgentObservabilityMutationExecutionHooks,
  WorkspaceAgentObservabilityMutationFailureEvent,
  WorkspaceAgentObservabilityMutationReporter,
} from '../../src/domain/agent-observability-workspace-service-common.js';
import {
  AGENT_OBSERVABILITY_CREDENTIAL_VERSION_MAX,
  acquireAgentObservabilityMutationReservation,
  agentObservabilityMutationBodyHash,
  reconcileAgentObservabilitySecretCleanup,
} from '../../src/domain/agent-observability-mutations.js';
import { agentObservabilityStateEtag } from '../../src/domain/agent-observability-etag.js';
import {
  loadOrganizationAgentObservabilityState,
  loadWorkspaceAgentObservabilityState,
} from '../../src/domain/agent-observability-state.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import type { SecretStore, SecretStorePutOptions } from '../../src/secrets/secret-provider.js';
import {
  createFreshWorkspaceMutationTestDb,
  type FreshWorkspaceMutationTestDb,
} from './agent-observability-workspace-mutation-test-db.js';

const WORKSPACE_TRANSITION_MATRIX = (['inherit', 'disabled', 'custom'] as const).flatMap((from) =>
  (['inherit', 'disabled', 'custom'] as const).flatMap((to) =>
    (['restrict', 'expand'] as const).map((ceilingChange) => ({ from, to, ceilingChange })),
  ),
);

describe('workspace agent observability mutation service', () => {
  let db: DbClient;
  let testDb: FreshWorkspaceMutationTestDb;
  let organizationId: string;
  let workspaceId: string;
  let secretStore: TrackingSecretStore;

  beforeAll(async () => {
    testDb = await createFreshWorkspaceMutationTestDb('workspace_mutation_service');
    db = testDb.db;
  });

  beforeEach(async () => {
    organizationId = nextOrganizationId();
    workspaceId = `ws_workspace_mutation_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    await db.insert(organizations).values({
      id: organizationId,
      name: `Workspace observability mutation integration ${organizationId}`,
      status: 'active',
    });
    await db.insert(workspaces).values({
      id: workspaceId,
      organizationId,
      name: 'Workspace observability mutation integration',
      status: 'active',
      createdBy: 'integration-test',
    });
    secretStore = new TrackingSecretStore();
  });

  afterAll(async () => {
    await testDb.close();
  });

  it('requires a current ETag, stages a custom binding, and caches an authoritative workspace state', async () => {
    const request = parse(requestBody({ password: 'workspace-create-secret' }));

    expect(await execute(request, 'workspace-missing-if-match', null)).toEqual({
      kind: 'precondition_required',
    });
    expect(secretStore.putCalls).toBe(0);
    expect(
      await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.workspaceId, workspaceId)),
    ).toEqual([]);

    const created = expectSuccess(await execute(request, 'workspace-create', await currentEtag()));
    expect(created.status).toBe(201);
    expect(created.body).toMatchObject({
      scope: 'workspace',
      organization_id: organizationId,
      workspace_id: workspaceId,
      configured: {
        mode: 'custom',
        binding: {
          scope: 'workspace',
          credential: { configured: true, version: 1 },
        },
      },
      effective: { source: 'workspace_custom', status: 'enabled' },
    });
    expect(JSON.stringify(created.body)).not.toContain('workspace-create-secret');
    const bindingId = created.body.configured.binding!.id;
    const [[setting], [credential]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId)),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
    ]);
    expect(setting).toMatchObject({
      organizationId,
      mode: 'custom',
      bindingId,
      selectionEpoch: 1,
      revocationEpoch: 0,
    });
    expect(credential).toMatchObject({ bindingId, credentialVersion: 1, keyHint: null });
    expect(await secretStore.resolve(credential!.secretRef)).toContain('workspace-create-secret');
    expect(secretStore.putCalls).toBe(1);
  });

  it('keeps a same-target credential head, then drains it on inherit and revokes on explicit disable', async () => {
    const initial = expectSuccess(
      await execute(
        parse(
          requestBody({
            password: 'workspace-policy-secret',
            captureCeiling: 'redacted_io',
            captureMode: 'redacted_io',
          }),
        ),
        'workspace-policy-create',
        await currentEtag(),
      ),
    );
    const bindingId = initial.body.configured.binding!.id;
    const [beforeCredential] = await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

    const sameTarget = expectSuccess(
      await execute(
        parse(
          requestBody({
            credentials: undefined,
            compression: 'gzip',
          }),
        ),
        'workspace-policy-same-target',
        await currentEtag(),
      ),
    );
    expect(sameTarget.status).toBe(200);
    expect(sameTarget.body.configured).toMatchObject({
      mode: 'custom',
      capture_ceiling: 'metadata_only',
      binding: {
        id: bindingId,
        config: { version: 2, compression: 'gzip' },
        credential: { configured: true, version: 1 },
      },
    });
    const [[afterCredential], versions, [afterSameTargetSetting]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
      db
        .select({ version: agentObservabilityBindingVersions.version })
        .from(agentObservabilityBindingVersions)
        .where(eq(agentObservabilityBindingVersions.bindingId, bindingId)),
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId)),
    ]);
    expect(afterCredential).toEqual(beforeCredential);
    expect(versions.map((row) => row.version).sort()).toEqual([1, 2]);
    expect(afterSameTargetSetting).toMatchObject({
      selectionEpoch: 1,
      revocationEpoch: 0,
      captureRestrictionEpoch: 1,
    });
    expect(secretStore.putCalls).toBe(1);

    const inherited = expectSuccess(
      await execute(modeRequest('inherit'), 'workspace-policy-inherit', await currentEtag()),
    );
    expect(inherited.body).toMatchObject({
      configured: { mode: 'inherit', binding: null },
      effective: { source: 'none', status: 'disabled', disabled_reason: 'no_organization_default' },
    });
    const disabled = expectSuccess(
      await execute(modeRequest('disabled'), 'workspace-policy-disable', await currentEtag()),
    );
    expect(disabled.body).toMatchObject({
      configured: { mode: 'disabled', binding: null },
      effective: { source: 'none', status: 'disabled', disabled_reason: 'workspace_disabled' },
    });
    const [[drained], [finalSetting]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, bindingId)),
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId)),
    ]);
    expect(drained).toMatchObject({ status: 'draining', currentVersion: 2 });
    expect(finalSetting).toMatchObject({
      mode: 'disabled',
      bindingId: null,
      selectionEpoch: 3,
      revocationEpoch: 1,
      captureRestrictionEpoch: 1,
    });
    expect(secretStore.putCalls).toBe(1);
  });

  it.each(WORKSPACE_TRANSITION_MATRIX)(
    'applies $from -> $to with $ceilingChange ceiling epoch and binding semantics',
    async ({ from, to, ceilingChange }) => {
      const matrixWorkspaceId = `ws_workspace_matrix_${from}_${to}_${ceilingChange}_${Date.now()}_${Math.random().toString(16).slice(2)}`;
      const beforeCeiling = ceilingChange === 'restrict' ? 'redacted_io' : 'metadata_only';
      const requestedCeiling =
        ceilingChange === 'restrict' ? ('metadata_only' as const) : ('redacted_io' as const);
      await db.insert(workspaces).values({
        id: matrixWorkspaceId,
        organizationId,
        name: 'Workspace observability transition matrix',
        status: 'active',
        createdBy: 'integration-test',
      });
      await seedWorkspaceMode(matrixWorkspaceId, from, beforeCeiling);
      const [before] = await db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, matrixWorkspaceId));
      if (!before) throw new Error('missing transition-matrix workspace setting');
      const oldBindingId = before.bindingId;
      const request =
        to === 'custom'
          ? parse(
              requestBody({
                endpoint: `https://collector-matrix-${from}-${to}-${ceilingChange}.example/v1/traces`,
                password: `workspace-matrix-${from}-${to}-${ceilingChange}-secret`,
                captureCeiling: requestedCeiling,
                captureMode: requestedCeiling,
              }),
            )
          : modeRequest(to, requestedCeiling);
      const authority = await db.transaction((tx) =>
        lockWorkspaceAgentObservabilityMutationAuthority(tx, organizationId, matrixWorkspaceId),
      );
      const classification = classifyWorkspaceAgentObservabilityMutation(authority, request);
      expect(classification.type).toBe(
        to === 'custom' ? (from === 'custom' ? 'replacement' : 'initial') : 'mode_only',
      );
      const putsBefore = secretStore.putCalls;
      const result = expectSuccess(
        await executeForWorkspace(
          matrixWorkspaceId,
          request,
          `workspace-matrix-${from}-${to}-${ceilingChange}`,
          await workspaceEtag(matrixWorkspaceId),
        ),
      );
      const [after] = await db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, matrixWorkspaceId));
      if (!after) throw new Error('missing updated transition-matrix workspace setting');

      const selectionDelta = to === 'custom' || from !== to ? 1 : 0;
      const revocationDelta = to === 'disabled' && from !== 'disabled' ? 1 : 0;
      const restrictionDelta = ceilingChange === 'restrict' ? 1 : 0;
      expect(result.status).toBe(to === 'custom' && from !== 'custom' ? 201 : 200);
      expect(after).toMatchObject({
        mode: to,
        bindingId: to === 'custom' ? result.body.configured.binding!.id : null,
        selectionEpoch: before.selectionEpoch + selectionDelta,
        revocationEpoch: before.revocationEpoch + revocationDelta,
        captureCeiling: requestedCeiling,
        captureRestrictionEpoch: before.captureRestrictionEpoch + restrictionDelta,
      });
      expect(secretStore.putCalls - putsBefore).toBe(to === 'custom' ? 1 : 0);

      if (to === 'custom') {
        expect(result.body.configured.binding).toMatchObject({
          scope: 'workspace',
          organization_id: organizationId,
          workspace_id: matrixWorkspaceId,
          status: 'active',
        });
        if (from === 'custom') {
          expect(result.body.configured.binding!.id).not.toBe(oldBindingId);
        }
      }
      if (from === 'custom') {
        const [oldBinding] = await db
          .select()
          .from(agentObservabilityBindings)
          .where(eq(agentObservabilityBindings.id, oldBindingId!));
        expect(oldBinding).toMatchObject({ status: 'draining' });
      }
    },
  );

  it.each([
    {
      name: 'active binding missing credential head',
      status: 'active' as const,
      missingHead: true,
    },
    { name: 'draining binding', status: 'draining' as const, missingHead: false },
    { name: 'disabled binding', status: 'disabled' as const, missingHead: false },
    { name: 'archived binding', status: 'archived' as const, missingHead: false },
  ])(
    'clears $name through inherit and disabled without rewriting nonactive bindings',
    async ({ status, missingHead }) => {
      for (const requestMode of ['inherit', 'disabled'] as const) {
        const degraded = await createDegradedCustomWorkspace(status, missingHead);
        const result = expectSuccess(
          await executeForWorkspace(
            degraded.workspaceId,
            modeRequest(requestMode),
            `workspace-degraded-clear-${status}-${requestMode}-${degraded.workspaceId}`,
            await workspaceEtag(degraded.workspaceId),
          ),
        );
        expect(result).toMatchObject({
          status: 200,
          body: { configured: { mode: requestMode, binding: null } },
        });
        const [[setting], [binding]] = await Promise.all([
          db
            .select()
            .from(agentObservabilityWorkspaceSettings)
            .where(eq(agentObservabilityWorkspaceSettings.workspaceId, degraded.workspaceId)),
          db
            .select()
            .from(agentObservabilityBindings)
            .where(eq(agentObservabilityBindings.id, degraded.bindingId)),
        ]);
        expect(setting).toMatchObject({ mode: requestMode, bindingId: null });
        expect(binding).toMatchObject({ status: status === 'active' ? 'draining' : status });
      }
    },
  );

  it.each([
    {
      name: 'active binding missing credential head',
      status: 'active' as const,
      missingHead: true,
    },
    { name: 'draining binding', status: 'draining' as const, missingHead: false },
    { name: 'disabled binding', status: 'disabled' as const, missingHead: false },
    { name: 'archived binding', status: 'archived' as const, missingHead: false },
  ])(
    'requires new credentials and binding to reconfigure $name',
    async ({ status, missingHead }) => {
      const degraded = await createDegradedCustomWorkspace(status, missingHead);
      const missingCredentials = parse(
        requestBody({
          endpoint: `https://collector-degraded-missing-${status}.example/v1/traces`,
          credentials: undefined,
        }),
      );
      expect(
        await executeForWorkspace(
          degraded.workspaceId,
          missingCredentials,
          `workspace-degraded-missing-credentials-${status}-${degraded.workspaceId}`,
          await workspaceEtag(degraded.workspaceId),
        ),
      ).toEqual({ kind: 'bad_request' });

      const putsBefore = secretStore.putCalls;
      const replacement = expectSuccess(
        await executeForWorkspace(
          degraded.workspaceId,
          parse(
            requestBody({
              endpoint: `https://collector-degraded-replacement-${status}.example/v1/traces`,
              password: `workspace-degraded-${status}-replacement-secret`,
            }),
          ),
          `workspace-degraded-replacement-${status}-${degraded.workspaceId}`,
          await workspaceEtag(degraded.workspaceId),
        ),
      );
      expect(replacement).toMatchObject({ status: 200, body: { configured: { mode: 'custom' } } });
      expect(replacement.body.configured.binding!.id).not.toBe(degraded.bindingId);
      expect(secretStore.putCalls - putsBefore).toBe(1);
      const [oldBinding] = await db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, degraded.bindingId));
      expect(oldBinding).toMatchObject({ status: status === 'active' ? 'draining' : status });
    },
  );

  it('fails closed for same-target custom policy update when its active credential head is missing', async () => {
    const degraded = await createDegradedCustomWorkspace('active', true);
    const beforePutCalls = secretStore.putCalls;
    const [beforeBinding] = await db
      .select()
      .from(agentObservabilityBindings)
      .where(eq(agentObservabilityBindings.id, degraded.bindingId));
    expect(
      await executeForWorkspace(
        degraded.workspaceId,
        parse(
          requestBody({
            endpoint: degraded.endpoint,
            credentials: undefined,
            compression: 'gzip',
          }),
        ),
        `workspace-degraded-same-target-${degraded.workspaceId}`,
        await workspaceEtag(degraded.workspaceId),
        secretStore,
        undefined,
        { report: () => {} },
      ),
    ).toEqual({ kind: 'unavailable' });
    const [afterBinding] = await db
      .select()
      .from(agentObservabilityBindings)
      .where(eq(agentObservabilityBindings.id, degraded.bindingId));
    expect(afterBinding).toEqual(beforeBinding);
    expect(secretStore.putCalls).toBe(beforePutCalls);
  });

  it('creates a new target, drains prior active binding, and replays its historical response after later mode change', async () => {
    const firstRequest = parse(requestBody({ password: 'workspace-first-secret' }));
    const first = expectSuccess(
      await execute(firstRequest, 'workspace-historical-first', await currentEtag()),
    );
    const firstBindingId = first.body.configured.binding!.id;
    const replacement = expectSuccess(
      await execute(
        parse(
          requestBody({
            endpoint: 'https://collector-two.example/v1/traces',
            password: 'workspace-second-secret',
          }),
        ),
        'workspace-historical-second',
        await currentEtag(),
      ),
    );
    const replacementBindingId = replacement.body.configured.binding!.id;
    expect(replacementBindingId).not.toBe(firstBindingId);
    const [drained] = await db
      .select()
      .from(agentObservabilityBindings)
      .where(eq(agentObservabilityBindings.id, firstBindingId));
    expect(drained).toMatchObject({ status: 'draining' });

    expectSuccess(
      await execute(modeRequest('inherit'), 'workspace-historical-inherit', await currentEtag()),
    );
    const replay = expectSuccess(await execute(firstRequest, 'workspace-historical-first', null));
    expect(replay).toEqual(first);
    expect(secretStore.putCalls).toBe(2);
  });

  it('fails closed on mode-specific corrupt workspace replay records without secret leakage', async () => {
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const inherit = modeRequest('inherit');
    expectSuccess(await execute(inherit, 'workspace-corrupt-inherit-201', await currentEtag()));
    await overwriteCachedResponse('workspace-corrupt-inherit-201', (body) => body, 201);

    const initialCustom = parse(requestBody({ password: 'workspace-corrupt-source-secret' }));
    const custom = expectSuccess(
      await execute(initialCustom, 'workspace-corrupt-custom-foreign', await currentEtag()),
    );
    const inherited = expectSuccess(
      await execute(inherit, 'workspace-corrupt-inherit-source', await currentEtag()),
    );
    await overwriteCachedResponse('workspace-corrupt-inherit-source', (body) => ({
      ...body,
      effective: custom.body.effective,
    }));
    await overwriteCachedResponse('workspace-corrupt-custom-foreign', (body) => {
      const configured = body.configured as { binding: Record<string, unknown> | null };
      const effective = body.effective as { binding: Record<string, unknown> | null };
      if (configured.binding === null || effective.binding === null) {
        throw new Error('expected custom cache binding');
      }
      const foreign = {
        organization_id: 'org_workspace_corrupt_foreign',
        workspace_id: 'ws_workspace_corrupt_foreign',
      };
      return {
        ...body,
        configured: { ...configured, binding: { ...configured.binding, ...foreign } },
        effective: { ...effective, binding: { ...effective.binding, ...foreign } },
      };
    });

    const missingCredential = parse(
      requestBody({
        endpoint: 'https://collector-corrupt-credential.example/v1/traces',
        password: 'workspace-corrupt-missing-credential-secret',
      }),
    );
    expectSuccess(
      await execute(
        missingCredential,
        'workspace-corrupt-custom-missing-credential',
        await currentEtag(),
      ),
    );
    await overwriteCachedResponse('workspace-corrupt-custom-missing-credential', (body) => {
      const configured = body.configured as { binding: Record<string, unknown> | null };
      if (configured.binding === null) throw new Error('expected custom cache binding');
      return {
        ...body,
        configured: {
          ...configured,
          binding: {
            ...configured.binding,
            credential: { configured: false, version: null, key_hint: null, rotated_at: null },
          },
        },
      };
    });

    for (const [request, key] of [
      [inherit, 'workspace-corrupt-inherit-201'],
      [inherit, 'workspace-corrupt-inherit-source'],
      [initialCustom, 'workspace-corrupt-custom-foreign'],
      [missingCredential, 'workspace-corrupt-custom-missing-credential'],
    ] as const) {
      expect(
        await execute(request, key, null, secretStore, undefined, {
          report: (event) => reports.push(event),
        }),
      ).toEqual({
        kind: 'unavailable',
      });
    }
    expect(reports).toHaveLength(4);
    expect(reports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ phase: 'acquisition', code: 'corrupt_cache' }),
      ]),
    );
    expect(JSON.stringify(reports)).not.toContain('workspace-corrupt-source-secret');
    expect(JSON.stringify(reports)).not.toContain('workspace-corrupt-missing-credential-secret');
    expect(inherited.status).toBe(200);
  });

  it('rejects custom replay whose effective capture mode exceeds its workspace ceiling', async () => {
    const request = parse(
      requestBody({
        password: 'workspace-corrupt-capture-ceiling-secret',
        captureCeiling: 'metadata_only',
        captureMode: 'redacted_io',
      }),
    );
    const created = expectSuccess(
      await execute(request, 'workspace-corrupt-capture-ceiling', await currentEtag()),
    );
    expect(created.body.effective.capture_mode).toBe('metadata_only');
    await overwriteCachedResponse('workspace-corrupt-capture-ceiling', (body) => ({
      ...body,
      effective: { ...(body.effective as Record<string, unknown>), capture_mode: 'redacted_io' },
    }));

    await expectCorruptWorkspaceReplay(
      request,
      'workspace-corrupt-capture-ceiling',
      'workspace-corrupt-capture-ceiling-secret',
    );
  });

  it('rejects custom replay whose effective capture mode exceeds its binding config', async () => {
    const request = parse(
      requestBody({
        password: 'workspace-corrupt-capture-binding-secret',
        captureCeiling: 'redacted_io',
        captureMode: 'metadata_only',
      }),
    );
    const created = expectSuccess(
      await execute(request, 'workspace-corrupt-capture-binding', await currentEtag()),
    );
    expect(created.body.effective.capture_mode).toBe('metadata_only');
    await overwriteCachedResponse('workspace-corrupt-capture-binding', (body) => ({
      ...body,
      effective: { ...(body.effective as Record<string, unknown>), capture_mode: 'redacted_io' },
    }));

    await expectCorruptWorkspaceReplay(
      request,
      'workspace-corrupt-capture-binding',
      'workspace-corrupt-capture-binding-secret',
    );
  });

  it('replays valid enabled inherit responses and rejects schema-valid invalid binding configurations', async () => {
    const secret = 'workspace-corrupt-enabled-inherit-configuration-secret';
    await createOrganizationDefault('workspace-corrupt-enabled-inherit-configuration', secret);
    const request = modeRequest('inherit');
    const first = expectSuccess(
      await execute(
        request,
        'workspace-corrupt-enabled-inherit-configuration',
        await currentEtag(),
      ),
    );
    expect(first).toMatchObject({
      status: 200,
      body: {
        effective: {
          source: 'organization_default',
          status: 'enabled',
          binding: {
            target: { adapter_type: 'otlp_http', external_project_id: null },
            config: { semantic_profile: 'otel_genai', protocol: 'http/protobuf' },
          },
        },
      },
    });
    expect(await execute(request, 'workspace-corrupt-enabled-inherit-configuration', null)).toEqual(
      first,
    );

    await overwriteCachedResponse('workspace-corrupt-enabled-inherit-configuration', (body) => {
      const effective = body.effective as { binding: Record<string, unknown> | null };
      if (effective.binding === null) throw new Error('expected inherited effective binding');
      const corrupt = {
        ...body,
        effective: {
          ...effective,
          binding: {
            ...effective.binding,
            config: {
              ...(effective.binding.config as Record<string, unknown>),
              semantic_profile: 'langfuse',
            },
          },
        },
      };
      expect(WorkspaceAgentObservabilityStateSchema.safeParse(corrupt).success).toBe(true);
      return corrupt;
    });

    await expectCorruptWorkspaceReplay(
      request,
      'workspace-corrupt-enabled-inherit-configuration',
      secret,
    );
  });

  it('rejects inherit replay whose enabled organization binding lacks credentials', async () => {
    const defaultResult = await executeOrganizationAgentObservabilityPut({
      db,
      secretStore,
      organizationId,
      principal: 'workspace-service-integration-test',
      authMethod: 'admin_api_key',
      requestId: 'request_workspace-corrupt-inherit-credential-default',
      request: parseOrganizationAgentObservabilityPutRequest(
        organizationRequestBody(
          'https://collector-inherit-credential.example/v1/traces',
          'workspace-corrupt-inherit-credential-default-secret',
        ),
      ),
      idempotencyKey: 'workspace-corrupt-inherit-credential-default',
      ifMatch: null,
    });
    if (defaultResult.kind !== 'success') throw new Error('expected organization default');
    const request = modeRequest('inherit');
    const inherited = expectSuccess(
      await execute(request, 'workspace-corrupt-inherit-credential', await currentEtag()),
    );
    expect(inherited.body.effective).toMatchObject({
      source: 'organization_default',
      status: 'enabled',
      binding: { credential: { configured: true } },
    });
    await overwriteCachedResponse('workspace-corrupt-inherit-credential', (body) => {
      const effective = body.effective as { binding: Record<string, unknown> | null };
      if (effective.binding === null) throw new Error('expected inherited effective binding');
      return {
        ...body,
        effective: {
          ...effective,
          binding: {
            ...effective.binding,
            credential: { configured: false, version: null, key_hint: null, rotated_at: null },
          },
        },
      };
    });

    await expectCorruptWorkspaceReplay(
      request,
      'workspace-corrupt-inherit-credential',
      'workspace-corrupt-inherit-credential-default-secret',
    );
  });

  it('rejects inherited disabled replay that claims configured credentials are missing', async () => {
    const secret = 'workspace-corrupt-disabled-credential-secret';
    await createOrganizationDefault('workspace-corrupt-disabled-credential', secret);
    const request = modeRequest('inherit');
    const first = expectSuccess(
      await execute(request, 'workspace-corrupt-disabled-credential', await currentEtag()),
    );
    expect(first.body.effective).toMatchObject({
      source: 'organization_default',
      status: 'enabled',
      binding: { credential: { configured: true, version: 1 } },
    });
    await overwriteCachedResponse('workspace-corrupt-disabled-credential', (body) => ({
      ...body,
      effective: {
        ...(body.effective as Record<string, unknown>),
        status: 'disabled',
        disabled_reason: 'credential_not_configured',
        capture_mode: 'metadata_only',
      },
    }));

    await expectCorruptWorkspaceReplay(request, 'workspace-corrupt-disabled-credential', secret);
  });

  it('rejects inherited disabled replay that claims a valid configuration is invalid', async () => {
    const secret = 'workspace-corrupt-disabled-configuration-secret';
    await createOrganizationDefault('workspace-corrupt-disabled-configuration', secret);
    const request = modeRequest('inherit');
    const first = expectSuccess(
      await execute(request, 'workspace-corrupt-disabled-configuration', await currentEtag()),
    );
    expect(first.body.effective).toMatchObject({
      source: 'organization_default',
      status: 'enabled',
      binding: {
        target: { adapter_type: 'otlp_http', endpoint_kind: 'traces_endpoint' },
        config: { semantic_profile: 'otel_genai', protocol: 'http/protobuf' },
      },
    });
    await overwriteCachedResponse('workspace-corrupt-disabled-configuration', (body) => ({
      ...body,
      effective: {
        ...(body.effective as Record<string, unknown>),
        status: 'disabled',
        disabled_reason: 'binding_configuration_invalid',
        capture_mode: 'metadata_only',
      },
    }));

    await expectCorruptWorkspaceReplay(request, 'workspace-corrupt-disabled-configuration', secret);
  });

  it('replays a coherent inherited credential-not-configured response', async () => {
    const bindingId = await createOrganizationDefault(
      'workspace-replay-disabled-credential',
      'workspace-replay-disabled-credential-secret',
    );
    await db
      .delete(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

    const request = modeRequest('inherit');
    const first = expectSuccess(
      await execute(request, 'workspace-replay-disabled-credential', await currentEtag()),
    );
    expect(first.body.effective).toMatchObject({
      source: 'organization_default',
      status: 'disabled',
      disabled_reason: 'credential_not_configured',
      binding: {
        id: bindingId,
        status: 'active',
        credential: { configured: false, version: null },
      },
    });
    expect(await execute(request, 'workspace-replay-disabled-credential', null)).toEqual(first);
  });

  it('rejects inherited credential-not-configured replay with invalid binding configuration', async () => {
    const secret = 'workspace-corrupt-disabled-credential-priority-secret';
    const bindingId = await createOrganizationDefault(
      'workspace-corrupt-disabled-credential-priority',
      secret,
    );
    await db
      .delete(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId));

    const request = modeRequest('inherit');
    const first = expectSuccess(
      await execute(request, 'workspace-corrupt-disabled-credential-priority', await currentEtag()),
    );
    expect(first.body).toMatchObject({
      configured: { binding: null },
      effective: {
        disabled_reason: 'credential_not_configured',
        binding: {
          id: bindingId,
          credential: { configured: false, version: null },
          config: { semantic_profile: 'otel_genai', protocol: 'http/protobuf' },
        },
      },
    });
    await overwriteCachedResponse('workspace-corrupt-disabled-credential-priority', (body) => {
      const effective = body.effective as { binding: Record<string, unknown> | null };
      if (effective.binding === null) throw new Error('expected inherited effective binding');
      return {
        ...body,
        effective: {
          ...effective,
          binding: {
            ...effective.binding,
            config: {
              ...(effective.binding.config as Record<string, unknown>),
              semantic_profile: 'langfuse',
            },
          },
        },
      };
    });

    await expectCorruptWorkspaceReplay(
      request,
      'workspace-corrupt-disabled-credential-priority',
      secret,
    );
  });

  it('replays a coherent inherited binding-configuration-invalid response', async () => {
    const bindingId = await createOrganizationDefault(
      'workspace-replay-disabled-configuration',
      'workspace-replay-disabled-configuration-secret',
    );
    await db
      .update(agentObservabilityBindingVersions)
      .set({ semanticProfile: 'langfuse' })
      .where(eq(agentObservabilityBindingVersions.bindingId, bindingId));

    const request = modeRequest('inherit');
    const first = expectSuccess(
      await execute(request, 'workspace-replay-disabled-configuration', await currentEtag()),
    );
    expect(first.body.effective).toMatchObject({
      source: 'organization_default',
      status: 'disabled',
      disabled_reason: 'binding_configuration_invalid',
      binding: {
        id: bindingId,
        status: 'active',
        target: { adapter_type: 'otlp_http', external_project_id: null },
        config: { semantic_profile: 'langfuse', protocol: 'http/protobuf' },
      },
    });
    expect(await execute(request, 'workspace-replay-disabled-configuration', null)).toEqual(first);
  });

  it('rejects a custom initial replay cached with status 200', async () => {
    const request = parse(requestBody({ password: 'workspace-corrupt-initial-status-secret' }));
    expectSuccess(await execute(request, 'workspace-corrupt-initial-status', await currentEtag()));
    await overwriteCachedResponse('workspace-corrupt-initial-status', (body) => body, 200);

    await expectCorruptWorkspaceReplay(
      request,
      'workspace-corrupt-initial-status',
      'workspace-corrupt-initial-status-secret',
    );
  });

  it('rejects a custom replacement replay cached with status 201', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-corrupt-replacement-current-secret' })),
        'workspace-corrupt-replacement-current',
        await currentEtag(),
      ),
    );
    const replacement = parse(
      requestBody({
        endpoint: 'https://collector-corrupt-replacement.example/v1/traces',
        password: 'workspace-corrupt-replacement-secret',
      }),
    );
    expectSuccess(
      await execute(replacement, 'workspace-corrupt-replacement-status', await currentEtag()),
    );
    await overwriteCachedResponse('workspace-corrupt-replacement-status', (body) => body, 201);

    await expectCorruptWorkspaceReplay(
      replacement,
      'workspace-corrupt-replacement-status',
      'workspace-corrupt-replacement-secret',
    );
  });

  it('rejects a custom replay with impossible expected version state', async () => {
    const request = parse(requestBody({ password: 'workspace-corrupt-version-state-secret' }));
    expectSuccess(await execute(request, 'workspace-corrupt-version-state', await currentEtag()));
    const [cached] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, 'workspace-corrupt-version-state'),
        ),
      );
    if (!cached) throw new Error('missing completed workspace idempotency response');
    await db
      .update(agentObservabilityMutationReservations)
      .set({ expectedConfigVersion: null, expectedCredentialVersion: 1 })
      .where(eq(agentObservabilityMutationReservations.id, cached.reservationId));

    await expectCorruptWorkspaceReplay(
      request,
      'workspace-corrupt-version-state',
      'workspace-corrupt-version-state-secret',
    );
  });

  it('replays a legitimate custom initial status 201', async () => {
    const request = parse(requestBody({ password: 'workspace-replay-initial-status-secret' }));
    const initial = expectSuccess(
      await execute(request, 'workspace-replay-initial-status', await currentEtag()),
    );
    expect(initial.status).toBe(201);
    expect(await execute(request, 'workspace-replay-initial-status', null)).toEqual(initial);
  });

  it('replays a legitimate custom replacement status 200', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-replay-replacement-current-secret' })),
        'workspace-replay-replacement-current',
        await currentEtag(),
      ),
    );
    const replacement = parse(
      requestBody({
        endpoint: 'https://collector-replay-replacement.example/v1/traces',
        password: 'workspace-replay-replacement-secret',
      }),
    );
    const replaced = expectSuccess(
      await execute(replacement, 'workspace-replay-replacement-status', await currentEtag()),
    );
    expect(replaced.status).toBe(200);
    expect(await execute(replacement, 'workspace-replay-replacement-status', null)).toEqual(
      replaced,
    );
  });

  it('rejects an initial custom replay with non-initial config generation', async () => {
    const secret = 'workspace-corrupt-initial-generation-secret';
    const request = parse(requestBody({ password: secret }));
    const initial = expectSuccess(
      await execute(request, 'workspace-corrupt-initial-generation', await currentEtag()),
    );
    expect(initial).toMatchObject({
      status: 201,
      body: { configured: { binding: { config: { version: 1 }, credential: { version: 1 } } } },
    });
    await overwriteCachedCustomBindingGenerations('workspace-corrupt-initial-generation', {
      configVersion: 2,
    });

    await expectCorruptWorkspaceReplay(request, 'workspace-corrupt-initial-generation', secret);
  });

  it('rejects a replacement custom replay with non-initial config generation', async () => {
    const previous = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-corrupt-replacement-generation-current-secret' })),
        'workspace-corrupt-replacement-generation-current',
        await currentEtag(),
      ),
    );
    expect(previous.status).toBe(201);
    const secret = 'workspace-corrupt-replacement-generation-secret';
    const request = parse(
      requestBody({
        endpoint: 'https://collector-corrupt-replacement-generation.example/v1/traces',
        password: secret,
      }),
    );
    const replacement = expectSuccess(
      await execute(request, 'workspace-corrupt-replacement-generation', await currentEtag()),
    );
    expect(replacement).toMatchObject({
      status: 200,
      body: { configured: { binding: { config: { version: 1 }, credential: { version: 1 } } } },
    });
    await overwriteCachedCustomBindingGenerations('workspace-corrupt-replacement-generation', {
      configVersion: 2,
    });

    await expectCorruptWorkspaceReplay(request, 'workspace-corrupt-replacement-generation', secret);
  });

  it('rejects a same-target custom replay with a non-successor config generation', async () => {
    const secret = 'workspace-corrupt-same-target-config-generation-secret';
    const { request, response } = await createSameTargetCustomReplay(
      'workspace-corrupt-same-target-config-generation',
      secret,
    );
    expect(response).toMatchObject({
      status: 200,
      body: { configured: { binding: { config: { version: 2 }, credential: { version: 1 } } } },
    });
    await overwriteCachedCustomBindingGenerations(
      'workspace-corrupt-same-target-config-generation',
      {
        configVersion: 3,
      },
    );

    await expectCorruptWorkspaceReplay(
      request,
      'workspace-corrupt-same-target-config-generation',
      secret,
    );
  });

  it('rejects a same-target custom replay with a changed credential generation', async () => {
    const secret = 'workspace-corrupt-same-target-credential-generation-secret';
    const { request, response } = await createSameTargetCustomReplay(
      'workspace-corrupt-same-target-credential-generation',
      secret,
    );
    expect(response).toMatchObject({
      status: 200,
      body: { configured: { binding: { config: { version: 2 }, credential: { version: 1 } } } },
    });
    await overwriteCachedCustomBindingGenerations(
      'workspace-corrupt-same-target-credential-generation',
      { credentialVersion: 2 },
    );

    await expectCorruptWorkspaceReplay(
      request,
      'workspace-corrupt-same-target-credential-generation',
      secret,
    );
  });

  it('replays a legitimate same-target custom generation', async () => {
    const { request, response } = await createSameTargetCustomReplay(
      'workspace-replay-same-target-generation',
      'workspace-replay-same-target-generation-secret',
    );
    expect(response).toMatchObject({
      status: 200,
      body: { configured: { binding: { config: { version: 2 }, credential: { version: 1 } } } },
    });
    expect(await execute(request, 'workspace-replay-same-target-generation', null)).toEqual(
      response,
    );
  });

  it('lets explicit disabled preempt only its pending workspace mutation after a staged write begins', async () => {
    const releasePut = deferred<void>();
    secretStore.beforePut = async () => releasePut.promise;
    const originalEtag = await currentEtag();
    const ordinary = execute(
      parse(requestBody({ password: 'workspace-preempt-secret' })),
      'workspace-preempt-ordinary',
      originalEtag,
    );
    await secretStore.awaitPutStarted();

    const disabled = expectSuccess(
      await execute(modeRequest('disabled'), 'workspace-preempt-disable', originalEtag),
    );
    expect(disabled.body).toMatchObject({ configured: { mode: 'disabled', binding: null } });
    releasePut.resolve();
    await expect(ordinary).resolves.toEqual({ kind: 'conflict' });

    const [[setting], reservations] = await Promise.all([
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
    expect(setting).toMatchObject({ mode: 'disabled', revocationEpoch: 1 });
    expect(reservations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ status: 'fenced' }),
        expect.objectContaining({ status: 'committed' }),
      ]),
    );
    const fenced = reservations.find((reservation) => reservation.status === 'fenced');
    if (!fenced) throw new Error('missing fenced ordinary reservation');
    const staging = await db
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(eq(agentObservabilityCredentialStagingIntents.reservationId, fenced.id));
    expect(staging).toEqual(
      expect.arrayContaining([expect.objectContaining({ status: 'cleanup_pending' })]),
    );
  });

  it('returns conflict rather than unavailable when disabled preempts a completed staged write before finalization', async () => {
    const finalizationStarted = deferred<void>();
    const releaseFinalization = deferred<void>();
    const originalEtag = await currentEtag();
    const ordinary = execute(
      parse(requestBody({ password: 'workspace-post-write-preemption-secret' })),
      'workspace-post-write-ordinary',
      originalEtag,
      secretStore,
      {
        beforeFinalizationAuthorityLock: async () => {
          finalizationStarted.resolve();
          await releaseFinalization.promise;
        },
      },
    );
    await finalizationStarted.promise;
    expect(secretStore.putCalls).toBe(1);

    expectSuccess(
      await execute(modeRequest('disabled'), 'workspace-post-write-disable', originalEtag),
    );
    releaseFinalization.resolve();
    await expect(ordinary).resolves.toEqual({ kind: 'conflict' });

    const fencedReservation = (
      await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(
          and(
            eq(agentObservabilityMutationReservations.organizationId, organizationId),
            eq(agentObservabilityMutationReservations.workspaceId, workspaceId),
            eq(agentObservabilityMutationReservations.status, 'fenced'),
          ),
        )
    )[0];
    if (!fencedReservation) throw new Error('missing fenced post-write reservation');
    const staging = await db
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(eq(agentObservabilityCredentialStagingIntents.reservationId, fencedReservation.id));
    expect(staging).toEqual([expect.objectContaining({ status: 'cleanup_pending' })]);
  });

  it('returns not_found and settles exact staged state when archive wins before finalization', async () => {
    const finalizationStarted = deferred<void>();
    const releaseFinalization = deferred<void>();
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const ordinary = execute(
      parse(requestBody({ password: 'workspace-archive-wins-secret' })),
      'workspace-archive-wins',
      await currentEtag(),
      secretStore,
      {
        beforeFinalizationAuthorityLock: async () => {
          finalizationStarted.resolve();
          await releaseFinalization.promise;
        },
      },
      { report: (event) => reports.push(event) },
    );

    try {
      await finalizationStarted.promise;
      const [reservation] = await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(
          and(
            eq(agentObservabilityMutationReservations.organizationId, organizationId),
            eq(agentObservabilityMutationReservations.workspaceId, workspaceId),
            eq(
              agentObservabilityMutationReservations.ownerPrincipal,
              'workspace-service-integration-test',
            ),
          ),
        );
      if (!reservation) throw new Error('missing staged workspace reservation');
      const [staging] = await db
        .select()
        .from(agentObservabilityCredentialStagingIntents)
        .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservation.id));
      if (!staging) throw new Error('missing staged workspace credential');

      await db
        .update(workspaces)
        .set({ status: 'archived', archivedAt: new Date(), updatedAt: new Date() })
        .where(eq(workspaces.id, workspaceId));
      releaseFinalization.resolve();

      await expect(ordinary).resolves.toEqual({ kind: 'not_found' });
      expect(reports).toEqual([]);
      const [[settled], [cleanup], candidate] = await Promise.all([
        db
          .select()
          .from(agentObservabilityMutationReservations)
          .where(eq(agentObservabilityMutationReservations.id, reservation.id)),
        db
          .select()
          .from(agentObservabilityCredentialStagingIntents)
          .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservation.id)),
        db
          .select()
          .from(agentObservabilityBindings)
          .where(eq(agentObservabilityBindings.id, staging.candidateBindingId)),
      ]);
      expect(settled).toMatchObject({ status: 'fenced' });
      expect(cleanup).toMatchObject({ status: 'cleanup_pending' });
      expect(candidate).toEqual([]);
    } finally {
      releaseFinalization.resolve();
    }
  });

  it('retries a real SQLSTATE 40001 workspace finalizer without repeating its SecretStore write', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-serialization-current-secret' })),
        'workspace-serialization-current',
        await currentEtag(),
      ),
    );
    const finalizationReady = deferred<void>();
    const releaseFinalization = deferred<void>();
    const disableLocked = deferred<void>();
    const releaseDisable = deferred<void>();
    const firstFinalizerPid = deferred<number>();
    const finalizationAttempts: number[] = [];
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    let outerFinalizationCalls = 0;
    const putsBefore = secretStore.putCalls;
    const loser = execute(
      parse(
        requestBody({
          endpoint: 'https://collector-workspace-serialization-next.example/v1/traces',
          password: 'workspace-serialization-next-secret',
        }),
      ),
      'workspace-serialization-loser',
      await currentEtag(),
      secretStore,
      {
        beforeFinalizationAuthorityLock: async () => {
          outerFinalizationCalls += 1;
          finalizationReady.resolve();
          await releaseFinalization.promise;
        },
        beforeFinalizationTransaction: async ({ attempt, tx }) => {
          finalizationAttempts.push(attempt);
          if (attempt === 1) firstFinalizerPid.resolve(await finalizationBackendPid(tx));
        },
      },
      { report: (event) => reports.push(event) },
    );

    try {
      await finalizationReady.promise;
      const disabling = execute(
        modeRequest('disabled'),
        'workspace-serialization-disable',
        await currentEtag(),
        secretStore,
        {
          afterAcquisitionAuthorityLocked: async () => {
            disableLocked.resolve();
            await releaseDisable.promise;
          },
        },
      );
      await disableLocked.promise;
      releaseFinalization.resolve();
      await waitForFinalizationAuthorityLock(await firstFinalizerPid.promise);
      releaseDisable.resolve();

      expectSuccess(await disabling);
      await expect(loser).resolves.toEqual({ kind: 'conflict' });
      expect(finalizationAttempts).toEqual([1, 2]);
      expect(outerFinalizationCalls).toBe(1);
      expect(reports).toEqual([]);
      expect(secretStore.putCalls - putsBefore).toBe(1);
      const [idempotency] = await db
        .select()
        .from(agentObservabilityIdempotencyKeys)
        .where(eq(agentObservabilityIdempotencyKeys.key, 'workspace-serialization-loser'));
      if (!idempotency) throw new Error('missing workspace serialization idempotency record');
      const [[reservation], [staging]] = await Promise.all([
        db
          .select()
          .from(agentObservabilityMutationReservations)
          .where(eq(agentObservabilityMutationReservations.id, idempotency.reservationId)),
        db
          .select()
          .from(agentObservabilityCredentialStagingIntents)
          .where(
            eq(agentObservabilityCredentialStagingIntents.reservationId, idempotency.reservationId),
          ),
      ]);
      expect(reservation).toMatchObject({ status: 'fenced' });
      expect(staging).toMatchObject({ status: 'cleanup_pending' });
    } finally {
      releaseFinalization.resolve();
      releaseDisable.resolve();
    }
  });

  it('resets explicit-disable failure phase for every serialization retry attempt', async () => {
    let repeatableReadAttempts = 0;
    let acquisitionAttempts = 0;
    const finalizationAttempts: number[] = [];
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const originalTransaction = db.transaction.bind(db);
    const retryDb = new Proxy(db, {
      get(target, property, receiver) {
        if (property !== 'transaction') return Reflect.get(target, property, receiver);
        return (async (...args: Parameters<DbClient['transaction']>) => {
          const [callback, options] = args;
          if (options?.isolationLevel !== 'repeatable read') {
            return originalTransaction(callback, options);
          }
          repeatableReadAttempts += 1;
          return originalTransaction(async (tx) => {
            const result = await callback(tx);
            if (repeatableReadAttempts === 1) throw postgresError('40001');
            return result;
          }, options);
        }) as DbClient['transaction'];
      },
    }) as DbClient;

    const result = await executeWorkspaceAgentObservabilityPut({
      db: retryDb,
      organizationId,
      workspaceId,
      principal: 'workspace-service-integration-test',
      authMethod: 'admin_api_key',
      requestId: 'request_workspace-disable-phase-retry',
      request: modeRequest('disabled'),
      idempotencyKey: 'workspace-disable-phase-retry',
      ifMatch: await currentEtag(),
      reporter: { report: (event) => reports.push(event) },
      hooks: {
        beforeFinalizationTransaction: async ({ attempt }) => {
          finalizationAttempts.push(attempt);
        },
        beforeAcquisitionAuthorityLock: async () => {
          acquisitionAttempts += 1;
          if (acquisitionAttempts === 2) throw new Error('second-attempt-acquisition-failure');
        },
      },
    });

    expect(result).toEqual({ kind: 'unavailable' });
    expect(repeatableReadAttempts).toBe(2);
    expect(finalizationAttempts).toEqual([1, 2]);
    expect(acquisitionAttempts).toBe(2);
    expect(reports).toEqual([
      expect.objectContaining({ phase: 'acquisition', code: 'unexpected_database_or_programmer' }),
    ]);
    expect(
      await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.workspaceId, workspaceId)),
    ).toEqual([]);
  });

  it('checks stale explicit disabled before preemption and lets the live ordinary PUT finish', async () => {
    const releaseStaging = deferred<void>();
    secretStore.beforePut = async () => releaseStaging.promise;
    const etag = await currentEtag();
    const ordinary = execute(
      parse(requestBody({ password: 'workspace-stale-disable-secret' })),
      'workspace-stale-disable-ordinary',
      etag,
    );
    await secretStore.awaitPutStarted();

    try {
      expect(
        await execute(modeRequest('disabled'), 'workspace-stale-disable', '"workspace-stale"'),
      ).toEqual({ kind: 'stale' });
      const [reservation] = await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(
          and(
            eq(agentObservabilityMutationReservations.organizationId, organizationId),
            eq(agentObservabilityMutationReservations.workspaceId, workspaceId),
            eq(
              agentObservabilityMutationReservations.ownerPrincipal,
              'workspace-service-integration-test',
            ),
          ),
        );
      if (!reservation) throw new Error('missing pending ordinary reservation');
      const [staging] = await db
        .select()
        .from(agentObservabilityCredentialStagingIntents)
        .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservation.id));
      expect(reservation).toMatchObject({ status: 'pending' });
      expect(staging).toMatchObject({ status: 'writing' });

      releaseStaging.resolve();
      expectSuccess(await ordinary);
      expect(await reservationById(reservation.id)).toMatchObject({ status: 'committed' });
    } finally {
      releaseStaging.resolve();
    }
  });

  it('serializes organization default replacement before inherited workspace PUT without cross-target fencing', async () => {
    const executeOrganization = async (
      endpoint: string,
      key: string,
      ifMatch: string | null,
      hooks?: WorkspaceAgentObservabilityMutationExecutionHooks,
    ) =>
      executeOrganizationAgentObservabilityPut({
        db,
        secretStore,
        organizationId,
        principal: 'workspace-service-integration-test',
        authMethod: 'admin_api_key',
        requestId: `request_${key}`,
        request: parseOrganizationAgentObservabilityPutRequest(
          organizationRequestBody(endpoint, `${key}-secret`),
        ),
        idempotencyKey: key,
        ifMatch,
        ...(hooks === undefined ? {} : { hooks }),
      });
    const organizationEtag = async () => {
      const state = await loadOrganizationAgentObservabilityState({ db, organizationId });
      return agentObservabilityStateEtag(state.etagInput);
    };

    const defaultA = await executeOrganization(
      'https://collector-org-default-a.example/v1/traces',
      'workspace-inherited-org-default-a',
      null,
    );
    if (defaultA.kind !== 'success') throw new Error('expected initial organization default');
    const workspaceEtagBeforeDefaultReplacement = await currentEtag();
    const organizationLocked = deferred<void>();
    const releaseOrganization = deferred<void>();
    const workspaceStarted = deferred<void>();
    const replacing = executeOrganization(
      'https://collector-org-default-b.example/v1/traces',
      'workspace-inherited-org-default-b',
      await organizationEtag(),
      {
        afterFinalizationAuthorityLocked: async () => {
          organizationLocked.resolve();
          await releaseOrganization.promise;
        },
      },
    );
    await organizationLocked.promise;
    const inheritedWorkspacePut = execute(
      modeRequest('disabled'),
      'workspace-inherited-stale-after-org-replacement',
      workspaceEtagBeforeDefaultReplacement,
      secretStore,
      {
        beforeAcquisitionAuthorityLock: async () => {
          workspaceStarted.resolve();
        },
      },
    );
    try {
      await workspaceStarted.promise;
      releaseOrganization.resolve();
      expect(await replacing).toMatchObject({ kind: 'success', status: 200 });
      await expect(inheritedWorkspacePut).resolves.toEqual({ kind: 'stale' });
      const [[setting], reservations] = await Promise.all([
        db
          .select()
          .from(agentObservabilityWorkspaceSettings)
          .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId)),
        db
          .select()
          .from(agentObservabilityMutationReservations)
          .where(eq(agentObservabilityMutationReservations.workspaceId, workspaceId)),
      ]);
      expect(setting).toMatchObject({ mode: 'inherit', bindingId: null });
      expect(reservations).toEqual([]);
      const state = await loadWorkspaceAgentObservabilityState({ db, organizationId, workspaceId });
      expect(state.response.effective).toMatchObject({
        source: 'organization_default',
        status: 'enabled',
      });
      expect(state.response.effective.binding!.target.endpoint_url).toBe(
        'https://collector-org-default-b.example/v1/traces',
      );
    } finally {
      releaseOrganization.resolve();
    }
  });

  it('replays a coherent inherited historical response when its organization binding was disabled', async () => {
    const defaultResult = await executeOrganizationAgentObservabilityPut({
      db,
      secretStore,
      organizationId,
      principal: 'workspace-service-integration-test',
      authMethod: 'admin_api_key',
      requestId: 'request_workspace-inherit-disabled-org-default',
      request: parseOrganizationAgentObservabilityPutRequest(
        organizationRequestBody(
          'https://collector-org-default-disabled.example/v1/traces',
          'workspace-inherit-disabled-org-default-secret',
        ),
      ),
      idempotencyKey: 'workspace-inherit-disabled-org-default',
      ifMatch: null,
    });
    if (defaultResult.kind !== 'success') throw new Error('expected organization default');
    const organizationBindingId = defaultResult.body.configured.default_binding!.id;
    await db
      .update(agentObservabilityBindings)
      .set({ status: 'draining', updatedAt: new Date() })
      .where(eq(agentObservabilityBindings.id, organizationBindingId));

    const request = modeRequest('inherit');
    const first = expectSuccess(
      await execute(request, 'workspace-inherit-disabled-org-replay', await currentEtag()),
    );
    expect(first).toMatchObject({
      status: 200,
      body: {
        configured: { mode: 'inherit', binding: null },
        effective: {
          source: 'organization_default',
          status: 'disabled',
          disabled_reason: 'binding_draining',
          binding: {
            id: organizationBindingId,
            scope: 'organization',
            organization_id: organizationId,
            workspace_id: null,
            status: 'draining',
          },
        },
      },
    });
    expect(await execute(request, 'workspace-inherit-disabled-org-replay', null)).toEqual(first);
  });

  it('audits an already-disabled no-op even when selection and revocation epochs are maxed', async () => {
    await db
      .update(agentObservabilityWorkspaceSettings)
      .set({
        mode: 'disabled',
        bindingId: null,
        selectionEpoch: Number.MAX_SAFE_INTEGER,
        revocationEpoch: Number.MAX_SAFE_INTEGER,
      })
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId));

    const result = expectSuccess(
      await execute(modeRequest('disabled'), 'workspace-disable-at-max', await currentEtag()),
    );
    expect(result).toMatchObject({ status: 200, body: { configured: { mode: 'disabled' } } });
    const [[setting], [idempotency]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId)),
      db
        .select()
        .from(agentObservabilityIdempotencyKeys)
        .where(eq(agentObservabilityIdempotencyKeys.key, 'workspace-disable-at-max')),
    ]);
    expect(setting).toMatchObject({
      selectionEpoch: Number.MAX_SAFE_INTEGER,
      revocationEpoch: Number.MAX_SAFE_INTEGER,
      mode: 'disabled',
    });
    expect(idempotency).toMatchObject({ status: 'completed', responseStatus: 200 });
  });

  it('fences only its current workspace binding and leaves sibling workspace reservations live', async () => {
    const created = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-exact-preemption-secret' })),
        'workspace-exact-preemption-create',
        await currentEtag(),
      ),
    );
    const bindingId = created.body.configured.binding!.id;
    const siblingWorkspaceId = `ws_workspace_mutation_sibling_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    await db.insert(workspaces).values({
      id: siblingWorkspaceId,
      organizationId,
      name: 'Sibling workspace preemption boundary',
      status: 'active',
      createdBy: 'integration-test',
    });
    const [currentBinding, siblingSetting] = await Promise.all([
      acquirePendingReservation(
        {
          type: 'binding',
          organizationId,
          workspaceId,
          bindingId,
          bindingScope: 'workspace',
        },
        'workspace-exact-current-binding',
      ),
      acquirePendingReservation(
        {
          type: 'workspace_setting',
          organizationId,
          workspaceId: siblingWorkspaceId,
        },
        'workspace-exact-sibling-setting',
      ),
    ]);

    expectSuccess(
      await execute(modeRequest('disabled'), 'workspace-exact-disable', await currentEtag()),
    );
    const [fenced, untouched] = await Promise.all([
      reservationById(currentBinding.reservationId),
      reservationById(siblingSetting.reservationId),
    ]);
    expect(fenced).toMatchObject({ status: 'fenced' });
    expect(untouched).toMatchObject({ status: 'pending' });
  });

  it('fails closed before reserving when custom credentials cannot be staged or workspace authority is corrupt', async () => {
    const missingStore = await execute(
      parse(requestBody({ password: 'workspace-no-store-secret' })),
      'workspace-no-store',
      await currentEtag(),
      null,
    );
    expect(missingStore).toEqual({ kind: 'unavailable' });
    expect(
      await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.workspaceId, workspaceId)),
    ).toEqual([]);

    await db
      .delete(agentObservabilityWorkspaceSettings)
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId));
    expect(await execute(modeRequest('inherit'), 'workspace-corrupt-state', null)).toEqual({
      kind: 'unavailable',
    });
  });

  it('rotates one custom workspace credential head without mutating its target, config, or setting', async () => {
    const created = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-create-secret' })),
        'workspace-rotate-create',
        await currentEtag(),
      ),
    );
    const bindingId = created.body.configured.binding!.id;
    const [[beforeSetting], [beforeHead]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId)),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
    ]);
    if (!beforeSetting || !beforeHead) throw new Error('missing workspace rotation authority');

    const rotation = expectRotationSuccess(
      await rotate(
        parseWorkspaceAgentObservabilityCredentialRotationRequest({
          credentials: { type: 'bearer', token: 'workspace-rotate-next-secret' },
        }),
        'workspace-rotate',
        await currentEtag(),
      ),
    );
    expect(rotation.body).toMatchObject({
      configured: {
        mode: 'custom',
        binding: {
          id: bindingId,
          target: created.body.configured.binding!.target,
          config: { version: 1 },
          credential: { configured: true, version: 2 },
        },
      },
      effective: { source: 'workspace_custom' },
    });
    expect(JSON.stringify(rotation.body)).not.toContain('workspace-rotate-next-secret');
    const [[afterSetting], [afterHead], cleanup, [audit], [idempotency]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId)),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
      db
        .select()
        .from(agentObservabilitySecretCleanupOutbox)
        .where(eq(agentObservabilitySecretCleanupOutbox.secretRef, beforeHead.secretRef)),
      db
        .select()
        .from(adminAuditEvents)
        .where(eq(adminAuditEvents.requestId, 'request_workspace-rotate')),
      db
        .select()
        .from(agentObservabilityIdempotencyKeys)
        .where(eq(agentObservabilityIdempotencyKeys.key, 'workspace-rotate')),
    ]);
    expect(afterSetting).toMatchObject({
      mode: beforeSetting.mode,
      bindingId: beforeSetting.bindingId,
      selectionEpoch: beforeSetting.selectionEpoch,
      revocationEpoch: beforeSetting.revocationEpoch,
      captureRestrictionEpoch: beforeSetting.captureRestrictionEpoch,
    });
    expect(afterHead).toMatchObject({ credentialVersion: 2 });
    expect(afterHead!.secretRef).not.toBe(beforeHead.secretRef);
    expect(await secretStore.resolve(beforeHead.secretRef)).not.toBeNull();
    expect(cleanup).toMatchObject([
      {
        organizationId,
        bindingId,
        bindingScope: 'workspace',
        status: 'pending',
      },
    ]);
    expect(audit).toMatchObject({
      action: 'workspace.agent_observability.credentials_rotated',
      targetType: 'binding',
      targetId: bindingId,
      workspaceId,
    });
    const durable = JSON.stringify({ audit, idempotency });
    expect(durable).not.toContain('workspace-rotate-next-secret');
    expect(durable).not.toContain(beforeHead.secretRef);
    expect(durable).not.toContain(afterHead!.secretRef);
    await reconcileAgentObservabilitySecretCleanup(db, secretStore);
    expect(await secretStore.resolve(beforeHead.secretRef)).toBeNull();
  });

  it('replays a completed workspace rotation after custom target replacement and explicit disable', async () => {
    const initial = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-replay-create-secret' })),
        'workspace-rotate-replay-create',
        await currentEtag(),
      ),
    );
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'basic', username: 'project', password: 'workspace-rotate-replay-next' },
    });
    const rotated = expectRotationSuccess(
      await rotate(request, 'workspace-rotate-replay', await currentEtag()),
    );
    const replacement = expectSuccess(
      await execute(
        parse(
          requestBody({
            endpoint: 'https://collector.example/v1/traces-workspace-rotate-replay-replaced',
            password: 'workspace-rotate-replay-replacement-secret',
          }),
        ),
        'workspace-rotate-replay-replacement',
        await currentEtag(),
      ),
    );
    expect(replacement.body.configured.binding!.id).not.toBe(initial.body.configured.binding!.id);
    expectSuccess(
      await execute(
        modeRequest('disabled'),
        'workspace-rotate-replay-disabled',
        await currentEtag(),
      ),
    );

    const replay = await rotate(request, 'workspace-rotate-replay', null);
    expect(replay).toEqual(rotated);
    expect(secretStore.putCalls).toBe(3);
  });

  it('retries a stale acquisition into same-key route replay without a second SecretStore put', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-acquire-retry-current-secret' })),
        'workspace-rotate-acquire-retry-create',
        await currentEtag(),
      ),
    );
    const etag = await currentEtag();
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-acquire-retry-next-secret' },
    });
    const key = 'workspace-rotate-acquire-retry';
    const firstFinalizationReady = deferred<void>();
    const releaseFirstFinalization = deferred<void>();
    const firstAuthorityLocked = deferred<void>();
    const releaseFirstCommit = deferred<void>();
    const secondSnapshotReady = deferred<void>();
    const releaseSecondAuthority = deferred<void>();
    const firstFinalizerPid = deferred<number>();
    const secondAcquirerPid = deferred<number>();
    const acquisitionAttempts: number[] = [];
    let secondFinalizationHookCalls = 0;
    const firstReports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const secondReports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const putsBefore = secretStore.putCalls;

    const first = rotate(
      request,
      key,
      etag,
      secretStore,
      {
        beforeFinalizationAuthorityLock: async () => {
          firstFinalizationReady.resolve();
          await releaseFirstFinalization.promise;
        },
        beforeFinalizationTransaction: async ({ attempt, tx }) => {
          if (attempt === 1) firstFinalizerPid.resolve(await finalizationBackendPid(tx));
        },
        afterFinalizationAuthorityLocked: async () => {
          firstAuthorityLocked.resolve();
          await releaseFirstCommit.promise;
        },
      },
      { report: (event) => firstReports.push(event) },
    );

    try {
      await firstFinalizationReady.promise;
      const second = rotate(
        request,
        key,
        etag,
        secretStore,
        {
          beforeAcquisitionTransaction: async ({ attempt, tx }) => {
            acquisitionAttempts.push(attempt);
            if (attempt !== 1) return;
            await tx.execute(sql`select 1`);
            secondAcquirerPid.resolve(await finalizationBackendPid(tx));
            secondSnapshotReady.resolve();
            await releaseSecondAuthority.promise;
          },
          beforeFinalizationTransaction: async () => {
            secondFinalizationHookCalls += 1;
          },
        },
        { report: (event) => secondReports.push(event) },
      );
      await secondSnapshotReady.promise;
      releaseFirstFinalization.resolve();
      await firstAuthorityLocked.promise;
      releaseSecondAuthority.resolve();
      await waitForFinalizationAuthorityLock(await secondAcquirerPid.promise);
      releaseFirstCommit.resolve();

      const [firstResult, secondResult] = await Promise.all([first, second]);
      expectRotationSuccess(firstResult);
      expect(secondResult).toEqual(firstResult);
      expect(acquisitionAttempts).toEqual([1, 2]);
      expect(secondFinalizationHookCalls).toBe(0);
      expect(secretStore.putCalls - putsBefore).toBe(1);
      expect(firstReports).toEqual([]);
      expect(secondReports).toEqual([]);
      await expect(firstFinalizerPid.promise).resolves.toEqual(expect.any(Number));
    } finally {
      releaseFirstFinalization.resolve();
      releaseSecondAuthority.resolve();
      releaseFirstCommit.resolve();
    }
  });

  it('retries a stale acquisition into a fresh stale-ETag result without staging', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-acquire-stale-current-secret' })),
        'workspace-rotate-acquire-stale-create',
        await currentEtag(),
      ),
    );
    const etag = await currentEtag();
    const firstRequest = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-acquire-stale-first-secret' },
    });
    const secondRequest = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-acquire-stale-second-secret' },
    });
    const firstFinalizationReady = deferred<void>();
    const releaseFirstFinalization = deferred<void>();
    const firstAuthorityLocked = deferred<void>();
    const releaseFirstCommit = deferred<void>();
    const secondSnapshotReady = deferred<void>();
    const releaseSecondAuthority = deferred<void>();
    const secondAcquirerPid = deferred<number>();
    const acquisitionAttempts: number[] = [];
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const putsBefore = secretStore.putCalls;
    const first = rotate(firstRequest, 'workspace-rotate-acquire-stale-first', etag, secretStore, {
      beforeFinalizationAuthorityLock: async () => {
        firstFinalizationReady.resolve();
        await releaseFirstFinalization.promise;
      },
      afterFinalizationAuthorityLocked: async () => {
        firstAuthorityLocked.resolve();
        await releaseFirstCommit.promise;
      },
    });

    try {
      await firstFinalizationReady.promise;
      const second = rotate(
        secondRequest,
        'workspace-rotate-acquire-stale-second',
        etag,
        secretStore,
        {
          beforeAcquisitionTransaction: async ({ attempt, tx }) => {
            acquisitionAttempts.push(attempt);
            if (attempt !== 1) return;
            await tx.execute(sql`select 1`);
            secondAcquirerPid.resolve(await finalizationBackendPid(tx));
            secondSnapshotReady.resolve();
            await releaseSecondAuthority.promise;
          },
        },
        { report: (event) => reports.push(event) },
      );
      await secondSnapshotReady.promise;
      releaseFirstFinalization.resolve();
      await firstAuthorityLocked.promise;
      releaseSecondAuthority.resolve();
      await waitForFinalizationAuthorityLock(await secondAcquirerPid.promise);
      releaseFirstCommit.resolve();

      expectRotationSuccess(await first);
      await expect(second).resolves.toEqual({ kind: 'stale' });
      expect(acquisitionAttempts).toEqual([1, 2]);
      expect(secretStore.putCalls - putsBefore).toBe(1);
      expect(reports).toEqual([]);
    } finally {
      releaseFirstFinalization.resolve();
      releaseSecondAuthority.resolve();
      releaseFirstCommit.resolve();
    }
  });

  it('reports once when acquisition retry exhausts or receives a non-serialization error', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-acquire-error-current-secret' })),
        'workspace-rotate-acquire-error-create',
        await currentEtag(),
      ),
    );
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-acquire-error-next-secret' },
    });
    const originalTransaction = db.transaction.bind(db);
    for (const [name, code, expectedAttempts] of [
      ['serialization-exhausted', '40001', 3],
      ['non-serialization', '23505', 1],
    ] as const) {
      let attempts = 0;
      const retryDb = new Proxy(db, {
        get(target, property, receiver) {
          if (property !== 'transaction') return Reflect.get(target, property, receiver);
          return (async (...args: Parameters<DbClient['transaction']>) => {
            const [, options] = args;
            if (options?.isolationLevel === 'repeatable read') {
              attempts += 1;
              throw postgresError(code);
            }
            return originalTransaction(...args);
          }) as DbClient['transaction'];
        },
      }) as DbClient;
      const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
      const putsBefore = secretStore.putCalls;
      const result = await executeWorkspaceAgentObservabilityCredentialRotation({
        db: retryDb,
        secretStore,
        organizationId,
        workspaceId,
        principal: 'workspace-service-integration-test',
        authMethod: 'admin_api_key',
        requestId: `request_workspace-rotate-acquire-${name}`,
        request,
        idempotencyKey: `workspace-rotate-acquire-${name}`,
        ifMatch: await currentEtag(),
        reporter: { report: (event) => reports.push(event) },
      });
      expect(result).toEqual({ kind: 'unavailable' });
      expect(attempts).toBe(expectedAttempts);
      expect(secretStore.putCalls).toBe(putsBefore);
      expect(reports).toEqual([
        expect.objectContaining({
          phase: 'acquisition',
          code: 'unexpected_database_or_programmer',
        }),
      ]);
    }
  });

  it('returns ordinary conflicts before staging for ineligible workspace rotation heads', async () => {
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-ineligible-next' },
    });
    expect(await rotate(request, 'workspace-rotate-inherit', await currentEtag())).toEqual({
      kind: 'conflict',
    });

    const created = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-ineligible-create-secret' })),
        'workspace-rotate-ineligible-create',
        await currentEtag(),
      ),
    );
    const bindingId = created.body.configured.binding!.id;
    await db
      .update(agentObservabilityBindings)
      .set({ status: 'draining', updatedAt: new Date() })
      .where(eq(agentObservabilityBindings.id, bindingId));
    expect(await rotate(request, 'workspace-rotate-draining', await currentEtag())).toEqual({
      kind: 'conflict',
    });
    expect(secretStore.putCalls).toBe(1);
  });

  it('returns 409 without staging for every ordinary workspace rotation ineligibility', async () => {
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-eligibility-next' },
    });
    const expectConflictWithoutPut = async (targetWorkspaceId: string, key: string) => {
      const putsBefore = secretStore.putCalls;
      await expect(
        rotateForWorkspace(targetWorkspaceId, request, key, await workspaceEtag(targetWorkspaceId)),
      ).resolves.toEqual({ kind: 'conflict' });
      expect(secretStore.putCalls).toBe(putsBefore);
    };

    await expectConflictWithoutPut(workspaceId, 'workspace-rotate-eligibility-inherit');
    expectSuccess(
      await execute(
        modeRequest('disabled'),
        'workspace-rotate-eligibility-disabled',
        await currentEtag(),
      ),
    );
    await expectConflictWithoutPut(workspaceId, 'workspace-rotate-eligibility-explicit-disabled');

    for (const status of ['draining', 'disabled', 'archived'] as const) {
      const degraded = await createDegradedCustomWorkspace(status, false);
      await expectConflictWithoutPut(
        degraded.workspaceId,
        `workspace-rotate-eligibility-${status}`,
      );
    }
    const missingHead = await createDegradedCustomWorkspace('active', true);
    await expectConflictWithoutPut(
      missingHead.workspaceId,
      'workspace-rotate-eligibility-missing-head',
    );
    const nonOtlp = await createNonOtlpCustomWorkspace();
    await expectConflictWithoutPut(nonOtlp.workspaceId, 'workspace-rotate-eligibility-non-otlp');
  });

  it('treats every coherent active custom no-head resolver priority as ordinary non-rotatable', async () => {
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-no-head-priority-next' },
    });
    const expectNoHeadConflict = async (targetWorkspaceId: string, key: string) => {
      const putsBefore = secretStore.putCalls;
      const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
      await expect(
        rotateForWorkspace(
          targetWorkspaceId,
          request,
          key,
          await workspaceEtag(targetWorkspaceId),
          secretStore,
          undefined,
          { report: (event) => reports.push(event) },
        ),
      ).resolves.toEqual({ kind: 'conflict' });
      expect(secretStore.putCalls).toBe(putsBefore);
      expect(reports).toEqual([]);
    };

    const [originalPolicy] = await db
      .select()
      .from(agentObservabilityPlatformPolicy)
      .where(eq(agentObservabilityPlatformPolicy.id, 'default'));
    if (!originalPolicy) throw new Error('missing platform policy');
    const adapterDisabled = await createDegradedCustomWorkspace('active', true);
    const endpointDisabled = await createDegradedCustomWorkspace('active', true);
    const invalidConfig = await createDegradedCustomWorkspace('active', true);
    await db
      .update(agentObservabilityBindingVersions)
      .set({ semanticProfile: 'langfuse' })
      .where(
        and(
          eq(agentObservabilityBindingVersions.bindingId, invalidConfig.bindingId),
          eq(agentObservabilityBindingVersions.version, 1),
        ),
      );
    try {
      await db
        .update(agentObservabilityPlatformPolicy)
        .set({
          allowedAdapters: ['langfuse_sdk'],
          allowedEndpointClasses: originalPolicy.allowedEndpointClasses,
          updatedAt: new Date(),
        })
        .where(eq(agentObservabilityPlatformPolicy.id, 'default'));
      await expectNoHeadConflict(
        adapterDisabled.workspaceId,
        'workspace-rotate-no-head-platform-adapter',
      );

      await db
        .update(agentObservabilityPlatformPolicy)
        .set({
          allowedAdapters: originalPolicy.allowedAdapters,
          allowedEndpointClasses: ['private'],
          updatedAt: new Date(),
        })
        .where(eq(agentObservabilityPlatformPolicy.id, 'default'));
      await expectNoHeadConflict(
        endpointDisabled.workspaceId,
        'workspace-rotate-no-head-platform-endpoint',
      );

      await db
        .update(agentObservabilityPlatformPolicy)
        .set({
          allowedAdapters: originalPolicy.allowedAdapters,
          allowedEndpointClasses: originalPolicy.allowedEndpointClasses,
          updatedAt: new Date(),
        })
        .where(eq(agentObservabilityPlatformPolicy.id, 'default'));
      await expectNoHeadConflict(
        invalidConfig.workspaceId,
        'workspace-rotate-no-head-configuration-invalid',
      );
    } finally {
      await db
        .update(agentObservabilityPlatformPolicy)
        .set({
          allowedAdapters: originalPolicy.allowedAdapters,
          allowedEndpointClasses: originalPolicy.allowedEndpointClasses,
          maxCaptureMode: originalPolicy.maxCaptureMode,
          captureRestrictionEpoch: originalPolicy.captureRestrictionEpoch,
          updatedAt: originalPolicy.updatedAt,
        })
        .where(eq(agentObservabilityPlatformPolicy.id, 'default'));
    }
  });

  it('fails closed before staging on credential-generation and SecretStore-reference corruption', async () => {
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-corrupt-head-next-secret' },
    });
    const corruptCases = [
      {
        name: 'generation-overflow',
        mutate: async (bindingId: string) =>
          db
            .update(agentObservabilityBindingCredentials)
            .set({ credentialVersion: AGENT_OBSERVABILITY_CREDENTIAL_VERSION_MAX })
            .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
      },
      {
        name: 'non-agent-secret-reference',
        mutate: async (bindingId: string) =>
          db
            .update(agentObservabilityBindingCredentials)
            .set({ secretRef: 'external://other-system/workspace-rotation' })
            .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
      },
      {
        name: 'malformed-secret-reference',
        mutate: async (bindingId: string) =>
          db
            .update(agentObservabilityBindingCredentials)
            .set({ secretRef: 'not-a-secret-reference' })
            .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
      },
    ] as const;

    for (const corruptCase of corruptCases) {
      const degraded = await createDegradedCustomWorkspace('active', false);
      await corruptCase.mutate(degraded.bindingId);
      const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
      const putsBefore = secretStore.putCalls;
      await expect(
        rotateForWorkspace(
          degraded.workspaceId,
          request,
          `workspace-rotate-corrupt-head-${corruptCase.name}`,
          await workspaceEtag(degraded.workspaceId),
          secretStore,
          undefined,
          { report: (event) => reports.push(event) },
        ),
      ).resolves.toEqual({ kind: 'unavailable' });
      expect(secretStore.putCalls).toBe(putsBefore);
      expect(reports).toEqual([
        expect.objectContaining({ phase: 'acquisition', code: 'invariant_violation' }),
      ]);
      expect(JSON.stringify(reports)).not.toContain('workspace-rotate-corrupt-head-next-secret');
      expect(JSON.stringify(reports)).not.toContain('external://other-system/workspace-rotation');
    }
  });

  it('fails closed when a syntactically valid replay cache substitutes a binding target', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-corrupt-cache-create-secret' })),
        'workspace-rotate-corrupt-cache-create',
        await currentEtag(),
      ),
    );
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-corrupt-cache-next-secret' },
    });
    const key = 'workspace-rotate-corrupt-cache';
    expectRotationSuccess(await rotate(request, key, await currentEtag()));
    await overwriteCachedResponse(key, (body) => {
      const configured = body.configured as { binding: Record<string, unknown> };
      const effective = body.effective as { binding: Record<string, unknown> };
      const rewrite = (binding: Record<string, unknown>) => ({
        ...binding,
        target: {
          ...(binding.target as Record<string, unknown>),
          endpoint_url: 'https://collector.example/v1/traces-corrupt-cache',
        },
      });
      return {
        ...body,
        configured: { ...configured, binding: rewrite(configured.binding) },
        effective: { ...effective, binding: rewrite(effective.binding) },
      };
    });
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    expect(
      await rotate(request, key, null, secretStore, undefined, {
        report: (event) => reports.push(event),
      }),
    ).toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      expect.objectContaining({ phase: 'acquisition', code: 'corrupt_cache' }),
    ]);
    expect(JSON.stringify(reports)).not.toContain('workspace-rotate-corrupt-cache-next-secret');
  });

  it('rejects a rotation replay that labels a semantically valid config invalid', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-valid-config-secret' })),
        'workspace-rotate-valid-config-create',
        await currentEtag(),
      ),
    );
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-valid-config-next-secret' },
    });
    const key = 'workspace-rotate-valid-config-corrupt';
    expectRotationSuccess(await rotate(request, key, await currentEtag()));
    await overwriteCachedResponse(key, (body) => ({
      ...body,
      effective: {
        ...(body.effective as Record<string, unknown>),
        status: 'disabled',
        disabled_reason: 'binding_configuration_invalid',
        capture_mode: 'metadata_only',
      },
    }));

    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    await expect(
      rotate(request, key, null, secretStore, undefined, {
        report: (event) => reports.push(event),
      }),
    ).resolves.toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      expect.objectContaining({ phase: 'acquisition', code: 'corrupt_cache' }),
    ]);
    expect(JSON.stringify(reports)).not.toContain('workspace-rotate-valid-config-next-secret');
  });

  it('replays a legitimate configuration-invalid rotation and rejects an enabled corruption', async () => {
    const created = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-invalid-config-secret' })),
        'workspace-rotate-invalid-config-create',
        await currentEtag(),
      ),
    );
    const bindingId = created.body.configured.binding!.id;
    // This shape is representable by durable schema but no longer semantic
    // policy: langfuse OTLP requires an external project ID.
    await db
      .update(agentObservabilityBindingVersions)
      .set({ semanticProfile: 'langfuse' })
      .where(
        and(
          eq(agentObservabilityBindingVersions.bindingId, bindingId),
          eq(agentObservabilityBindingVersions.version, 1),
        ),
      );
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-invalid-config-next-secret' },
    });
    const key = 'workspace-rotate-invalid-config';
    const rotated = expectRotationSuccess(await rotate(request, key, await currentEtag()));
    expect(rotated.body.effective).toMatchObject({
      status: 'disabled',
      disabled_reason: 'binding_configuration_invalid',
      capture_mode: 'metadata_only',
    });
    await expect(rotate(request, key, null)).resolves.toEqual(rotated);

    await overwriteCachedResponse(key, (body) => ({
      ...body,
      effective: {
        ...(body.effective as Record<string, unknown>),
        status: 'enabled',
        disabled_reason: null,
      },
    }));
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    await expect(
      rotate(request, key, null, secretStore, undefined, {
        report: (event) => reports.push(event),
      }),
    ).resolves.toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      expect.objectContaining({ phase: 'acquisition', code: 'corrupt_cache' }),
    ]);
    expect(JSON.stringify(reports)).not.toContain('workspace-rotate-invalid-config-next-secret');
  });

  it('replays platform-policy-disabled rotations without inventing a historical policy snapshot', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-platform-replay-current-secret' })),
        'workspace-rotate-platform-replay-create',
        await currentEtag(),
      ),
    );
    const [originalPolicy] = await db
      .select()
      .from(agentObservabilityPlatformPolicy)
      .where(eq(agentObservabilityPlatformPolicy.id, 'default'));
    if (!originalPolicy) throw new Error('missing platform policy');
    try {
      for (const [name, update, reason] of [
        [
          'adapter',
          {
            allowedAdapters: ['langfuse_sdk'],
            allowedEndpointClasses: originalPolicy.allowedEndpointClasses,
          },
          'platform_adapter_disallowed',
        ],
        [
          'endpoint',
          { allowedAdapters: originalPolicy.allowedAdapters, allowedEndpointClasses: ['private'] },
          'platform_endpoint_class_disallowed',
        ],
      ] as const) {
        await db
          .update(agentObservabilityPlatformPolicy)
          .set({
            allowedAdapters: [...update.allowedAdapters],
            allowedEndpointClasses: [...update.allowedEndpointClasses],
            updatedAt: new Date(),
          })
          .where(eq(agentObservabilityPlatformPolicy.id, 'default'));
        const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
          credentials: { type: 'bearer', token: `workspace-rotate-platform-${name}-secret` },
        });
        const key = `workspace-rotate-platform-${name}`;
        const rotated = expectRotationSuccess(await rotate(request, key, await currentEtag()));
        expect(rotated.body.effective).toMatchObject({
          status: 'disabled',
          disabled_reason: reason,
          capture_mode: 'metadata_only',
        });
        await expect(rotate(request, key, null)).resolves.toEqual(rotated);
      }
    } finally {
      await db
        .update(agentObservabilityPlatformPolicy)
        .set({
          allowedAdapters: originalPolicy.allowedAdapters,
          allowedEndpointClasses: originalPolicy.allowedEndpointClasses,
          maxCaptureMode: originalPolicy.maxCaptureMode,
          captureRestrictionEpoch: originalPolicy.captureRestrictionEpoch,
          updatedAt: originalPolicy.updatedAt,
        })
        .where(eq(agentObservabilityPlatformPolicy.id, 'default'));
    }
  });

  it('fails closed for every rotation replay authority and effective-state corruption', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-replay-matrix-current-secret' })),
        'workspace-rotate-replay-matrix-create',
        await currentEtag(),
      ),
    );
    const rewriteBindings = (
      body: Record<string, unknown>,
      mutate: (binding: Record<string, unknown>) => Record<string, unknown>,
    ) => {
      const configured = body.configured as { binding: Record<string, unknown> };
      const effective = body.effective as { binding: Record<string, unknown> };
      return {
        ...body,
        configured: { ...configured, binding: mutate(configured.binding) },
        effective: { ...effective, binding: mutate(effective.binding) },
      };
    };
    const cases: Array<{
      name: string;
      mutate: (body: Record<string, unknown>) => Record<string, unknown>;
    }> = [
      {
        name: 'organization',
        mutate: (body) => ({ ...body, organization_id: 'org_other_workspace_rotation' }),
      },
      {
        name: 'workspace',
        mutate: (body) => ({ ...body, workspace_id: 'ws_other_workspace_rotation' }),
      },
      {
        name: 'binding-id',
        mutate: (body) => rewriteBindings(body, (binding) => ({ ...binding, id: 'aob_other' })),
      },
      {
        name: 'binding-status',
        mutate: (body) => rewriteBindings(body, (binding) => ({ ...binding, status: 'draining' })),
      },
      {
        name: 'effective-source',
        mutate: (body) => ({
          ...body,
          effective: {
            ...(body.effective as Record<string, unknown>),
            source: 'organization_default',
          },
        }),
      },
      {
        name: 'config-generation',
        mutate: (body) =>
          rewriteBindings(body, (binding) => ({
            ...binding,
            config: { ...(binding.config as Record<string, unknown>), version: 9_999 },
          })),
      },
      {
        name: 'credential-generation',
        mutate: (body) =>
          rewriteBindings(body, (binding) => ({
            ...binding,
            credential: { ...(binding.credential as Record<string, unknown>), version: 9_999 },
          })),
      },
      {
        name: 'capture',
        mutate: (body) => ({
          ...body,
          effective: {
            ...(body.effective as Record<string, unknown>),
            capture_mode: 'redacted_io',
          },
        }),
      },
      {
        name: 'credential-configured',
        mutate: (body) =>
          rewriteBindings(body, (binding) => ({
            ...binding,
            credential: {
              ...(binding.credential as Record<string, unknown>),
              configured: false,
              version: null,
              key_hint: null,
              rotated_at: null,
            },
          })),
      },
      {
        name: 'target-semantic-config',
        mutate: (body) =>
          rewriteBindings(body, (binding) => ({
            ...binding,
            config: {
              ...(binding.config as Record<string, unknown>),
              semantic_profile: 'langfuse',
            },
          })),
      },
    ];

    for (const replayCase of cases) {
      const secret = `workspace-rotate-replay-matrix-${replayCase.name}-secret`;
      const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: secret },
      });
      const key = `workspace-rotate-replay-matrix-${replayCase.name}`;
      expectRotationSuccess(await rotate(request, key, await currentEtag()));
      await overwriteCachedResponse(key, replayCase.mutate);
      const putsBeforeReplay = secretStore.putCalls;
      const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
      await expect(
        rotate(request, key, null, secretStore, undefined, {
          report: (event) => reports.push(event),
        }),
      ).resolves.toEqual({ kind: 'unavailable' });
      expect(secretStore.putCalls).toBe(putsBeforeReplay);
      expect(reports).toEqual([
        expect.objectContaining({ phase: 'acquisition', code: 'corrupt_cache' }),
      ]);
      expect(JSON.stringify(reports)).not.toContain(secret);
    }
  });

  it('lets explicit disabled preempt a rotation before the staging writer claim without telemetry', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-preempt-create-secret' })),
        'workspace-rotate-preempt-create',
        await currentEtag(),
      ),
    );
    const etag = await currentEtag();
    const stagingPaused = deferred<void>();
    const releaseStaging = deferred<void>();
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const rotation = rotate(
      parseWorkspaceAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'workspace-rotate-preempt-next' },
      }),
      'workspace-rotate-preempt',
      etag,
      secretStore,
      {
        beforeStagingWrite: async () => {
          stagingPaused.resolve();
          await releaseStaging.promise;
        },
      },
      { report: (event) => reports.push(event) },
    );
    await stagingPaused.promise;
    expectSuccess(
      await execute(modeRequest('disabled'), 'workspace-rotate-preempt-disabled', etag),
    );
    releaseStaging.resolve();
    await expect(rotation).resolves.toEqual({ kind: 'conflict' });
    expect(secretStore.putCalls).toBe(1);
    expect(reports).toEqual([]);
  });

  it('lets explicit disabled preempt a rotation after its staged write without telemetry', async () => {
    const created = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-post-write-current-secret' })),
        'workspace-rotate-post-write-create',
        await currentEtag(),
      ),
    );
    const bindingId = created.body.configured.binding!.id;
    const etag = await currentEtag();
    const finalizationReady = deferred<void>();
    const releaseFinalization = deferred<void>();
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const rotation = rotate(
      parseWorkspaceAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'workspace-rotate-post-write-next-secret' },
      }),
      'workspace-rotate-post-write',
      etag,
      secretStore,
      {
        beforeFinalizationAuthorityLock: async () => {
          finalizationReady.resolve();
          await releaseFinalization.promise;
        },
      },
      { report: (event) => reports.push(event) },
    );

    try {
      await finalizationReady.promise;
      expect(secretStore.putCalls).toBe(2);
      expectSuccess(
        await execute(modeRequest('disabled'), 'workspace-rotate-post-write-disabled', etag),
      );
      releaseFinalization.resolve();
      await expect(rotation).resolves.toEqual({ kind: 'conflict' });
      expect(reports).toEqual([]);

      const [idempotency] = await db
        .select()
        .from(agentObservabilityIdempotencyKeys)
        .where(eq(agentObservabilityIdempotencyKeys.key, 'workspace-rotate-post-write'));
      if (!idempotency) throw new Error('missing post-write rotation idempotency');
      const [[reservation], [staging]] = await Promise.all([
        db
          .select()
          .from(agentObservabilityMutationReservations)
          .where(eq(agentObservabilityMutationReservations.id, idempotency.reservationId)),
        db
          .select()
          .from(agentObservabilityCredentialStagingIntents)
          .where(
            eq(agentObservabilityCredentialStagingIntents.reservationId, idempotency.reservationId),
          ),
      ]);
      expect(reservation).toMatchObject({ status: 'fenced', targetBindingId: bindingId });
      expect(staging).toMatchObject({ status: 'cleanup_pending' });
      expect(await secretStore.resolve(staging!.secretRef)).not.toBeNull();
      expect(secretStore.deleteCalls).toBe(0);
    } finally {
      releaseFinalization.resolve();
    }
  });

  it('tombstones failed or timed-out rotation staging without an inline delete or secret telemetry', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-staging-current-secret' })),
        'workspace-rotate-staging-create',
        await currentEtag(),
      ),
    );
    const request = parseWorkspaceAgentObservabilityCredentialRotationRequest({
      credentials: { type: 'bearer', token: 'workspace-rotate-staging-next-secret' },
    });

    const failingStore = new FailingSecretStore();
    const failingReports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    await expect(
      rotate(
        request,
        'workspace-rotate-staging-failed',
        await currentEtag(),
        failingStore,
        undefined,
        { report: (event) => failingReports.push(event) },
      ),
    ).resolves.toEqual({ kind: 'unavailable' });
    expect(failingStore.putCalls).toBe(1);
    expect(failingStore.deleteCalls).toBe(0);
    await expectRotationStagingCleanup('workspace-rotate-staging-failed');
    expect(failingReports).toEqual([
      expect.objectContaining({ phase: 'staging_write', code: 'staging_failure' }),
    ]);
    expect(JSON.stringify(failingReports)).not.toContain('workspace-rotate-staging-next-secret');
    expect(JSON.stringify(failingReports)).not.toContain('provider rejected rotation staging');

    const hangingStore = new HangingSecretStore();
    const timeoutReports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    await expect(
      executeWorkspaceAgentObservabilityCredentialRotation({
        db,
        secretStore: hangingStore,
        organizationId,
        workspaceId,
        principal: 'workspace-service-integration-test',
        authMethod: 'admin_api_key',
        requestId: 'request_workspace-rotate-staging-timeout',
        request,
        idempotencyKey: 'workspace-rotate-staging-timeout',
        ifMatch: await currentEtag(),
        stagingWriteTimeoutMs: 10,
        reporter: { report: (event) => timeoutReports.push(event) },
      }),
    ).resolves.toEqual({ kind: 'unavailable' });
    expect(hangingStore.putCalls).toBe(1);
    expect(hangingStore.deleteCalls).toBe(0);
    expect(hangingStore.signal?.aborted).toBe(true);
    await expectRotationStagingCleanup('workspace-rotate-staging-timeout');
    expect(timeoutReports).toEqual([
      expect.objectContaining({ phase: 'staging_write', code: 'staging_timeout' }),
    ]);
    expect(JSON.stringify(timeoutReports)).not.toContain('workspace-rotate-staging-next-secret');
  });

  it('lets a target-replacement PUT win over a staged rotation without head overwrite', async () => {
    const initial = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-target-put-current-secret' })),
        'workspace-rotate-target-put-create',
        await currentEtag(),
      ),
    );
    const originalBindingId = initial.body.configured.binding!.id;
    const etag = await currentEtag();
    const finalizationReady = deferred<void>();
    const releaseFinalization = deferred<void>();
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const rotation = rotate(
      parseWorkspaceAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'workspace-rotate-target-put-next-secret' },
      }),
      'workspace-rotate-target-put-rotation',
      etag,
      secretStore,
      {
        beforeFinalizationAuthorityLock: async () => {
          finalizationReady.resolve();
          await releaseFinalization.promise;
        },
      },
      { report: (event) => reports.push(event) },
    );

    try {
      await finalizationReady.promise;
      const replacement = expectSuccess(
        await execute(
          parse(
            requestBody({
              endpoint: 'https://collector.example/v1/traces-workspace-rotate-target-put',
              password: 'workspace-rotate-target-put-replacement-secret',
            }),
          ),
          'workspace-rotate-target-put-replacement',
          etag,
        ),
      );
      const replacementBindingId = replacement.body.configured.binding!.id;
      expect(replacementBindingId).not.toBe(originalBindingId);
      releaseFinalization.resolve();
      await expect(rotation).resolves.toEqual({ kind: 'conflict' });
      expect(reports).toEqual([]);

      const [[originalHead], [replacementHead]] = await Promise.all([
        db
          .select()
          .from(agentObservabilityBindingCredentials)
          .where(eq(agentObservabilityBindingCredentials.bindingId, originalBindingId)),
        db
          .select()
          .from(agentObservabilityBindingCredentials)
          .where(eq(agentObservabilityBindingCredentials.bindingId, replacementBindingId)),
      ]);
      expect(originalHead).toMatchObject({ credentialVersion: 1 });
      expect(replacementHead).toMatchObject({ credentialVersion: 1 });
      await expectRotationStagingCleanup('workspace-rotate-target-put-rotation');
    } finally {
      releaseFinalization.resolve();
    }
  });

  it('lets a rotation win over a staged target-replacement PUT and fences that stale PUT', async () => {
    const initial = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-wins-put-current-secret' })),
        'workspace-rotate-wins-put-create',
        await currentEtag(),
      ),
    );
    const originalBindingId = initial.body.configured.binding!.id;
    const etag = await currentEtag();
    const putFinalizationReady = deferred<void>();
    const releasePutFinalization = deferred<void>();
    const putReports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const replacement = execute(
      parse(
        requestBody({
          endpoint: 'https://collector.example/v1/traces-workspace-rotate-wins-put',
          password: 'workspace-rotate-wins-put-replacement-secret',
        }),
      ),
      'workspace-rotate-wins-put-replacement',
      etag,
      secretStore,
      {
        beforeFinalizationAuthorityLock: async () => {
          putFinalizationReady.resolve();
          await releasePutFinalization.promise;
        },
      },
      { report: (event) => putReports.push(event) },
    );

    try {
      await putFinalizationReady.promise;
      const rotation = expectRotationSuccess(
        await rotate(
          parseWorkspaceAgentObservabilityCredentialRotationRequest({
            credentials: { type: 'bearer', token: 'workspace-rotate-wins-put-next-secret' },
          }),
          'workspace-rotate-wins-put-rotation',
          etag,
        ),
      );
      expect(rotation.body.configured.binding).toMatchObject({
        id: originalBindingId,
        credential: { version: 2 },
        config: { version: 1 },
      });
      releasePutFinalization.resolve();
      await expect(replacement).resolves.toEqual({ kind: 'conflict' });
      expect(putReports).toEqual([]);
      const [setting] = await db
        .select()
        .from(agentObservabilityWorkspaceSettings)
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId));
      expect(setting).toMatchObject({ bindingId: originalBindingId, mode: 'custom' });
      await expectRotationStagingCleanup('workspace-rotate-wins-put-replacement');
    } finally {
      releasePutFinalization.resolve();
    }
  });

  it('serializes same-binding config PUT and rotation so each winner changes only its own generation', async () => {
    const initial = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-same-target-current-secret' })),
        'workspace-rotate-same-target-create',
        await currentEtag(),
      ),
    );
    const bindingId = initial.body.configured.binding!.id;
    const configRequest = parse(requestBody({ credentials: undefined, compression: 'gzip' }));

    const configWinnerEtag = await currentEtag();
    const rotationFinalizationReady = deferred<void>();
    const releaseRotationFinalization = deferred<void>();
    const rotationLoser = rotate(
      parseWorkspaceAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'workspace-rotate-same-target-loser-secret' },
      }),
      'workspace-rotate-same-target-rotation-loser',
      configWinnerEtag,
      secretStore,
      {
        beforeFinalizationAuthorityLock: async () => {
          rotationFinalizationReady.resolve();
          await releaseRotationFinalization.promise;
        },
      },
    );
    try {
      await rotationFinalizationReady.promise;
      expectSuccess(
        await execute(
          configRequest,
          'workspace-rotate-same-target-config-winner',
          configWinnerEtag,
        ),
      );
      releaseRotationFinalization.resolve();
      await expect(rotationLoser).resolves.toEqual({ kind: 'conflict' });
    } finally {
      releaseRotationFinalization.resolve();
    }
    const [[configAfter], [headAfterConfig]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityBindings)
        .where(eq(agentObservabilityBindings.id, bindingId)),
      db
        .select()
        .from(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId)),
    ]);
    expect(configAfter).toMatchObject({ currentVersion: 2 });
    expect(headAfterConfig).toMatchObject({ credentialVersion: 1 });
    await expectRotationStagingCleanup('workspace-rotate-same-target-rotation-loser');

    const rotationWinnerEtag = await currentEtag();
    const configFinalizationReady = deferred<void>();
    const releaseConfigFinalization = deferred<void>();
    const configLoser = execute(
      configRequest,
      'workspace-rotate-same-target-config-loser',
      rotationWinnerEtag,
      secretStore,
      {
        beforeFinalizationAuthorityLock: async () => {
          configFinalizationReady.resolve();
          await releaseConfigFinalization.promise;
        },
      },
    );
    try {
      await configFinalizationReady.promise;
      expectRotationSuccess(
        await rotate(
          parseWorkspaceAgentObservabilityCredentialRotationRequest({
            credentials: { type: 'bearer', token: 'workspace-rotate-same-target-winner-secret' },
          }),
          'workspace-rotate-same-target-rotation-winner',
          rotationWinnerEtag,
        ),
      );
      releaseConfigFinalization.resolve();
      await expect(configLoser).resolves.toEqual({ kind: 'conflict' });
    } finally {
      releaseConfigFinalization.resolve();
    }
    const [[configAfterRotation], [headAfterRotation], versions] = await Promise.all([
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
        .from(agentObservabilityBindingVersions)
        .where(eq(agentObservabilityBindingVersions.bindingId, bindingId)),
    ]);
    expect(configAfterRotation).toMatchObject({ currentVersion: 2 });
    expect(headAfterRotation).toMatchObject({ credentialVersion: 2 });
    expect(versions).toHaveLength(2);
  });

  it('retries a real authority-lock serialization failure after disabled preempts a staged rotation', async () => {
    expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-disable-serialization-current-secret' })),
        'workspace-rotate-disable-serialization-create',
        await currentEtag(),
      ),
    );
    const rotationEtag = await currentEtag();
    const finalizationReady = deferred<void>();
    const releaseFinalization = deferred<void>();
    const disableLocked = deferred<void>();
    const releaseDisable = deferred<void>();
    const firstFinalizerPid = deferred<number>();
    const finalizationAttempts: number[] = [];
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const putsBefore = secretStore.putCalls;
    const rotation = rotate(
      parseWorkspaceAgentObservabilityCredentialRotationRequest({
        credentials: {
          type: 'bearer',
          token: 'workspace-rotate-disable-serialization-next-secret',
        },
      }),
      'workspace-rotate-disable-serialization',
      rotationEtag,
      secretStore,
      {
        beforeFinalizationAuthorityLock: async () => {
          finalizationReady.resolve();
          await releaseFinalization.promise;
        },
        beforeFinalizationTransaction: async ({ attempt, tx }) => {
          finalizationAttempts.push(attempt);
          if (attempt === 1) firstFinalizerPid.resolve(await finalizationBackendPid(tx));
        },
      },
      { report: (event) => reports.push(event) },
    );

    try {
      await finalizationReady.promise;
      const disabling = execute(
        modeRequest('disabled'),
        'workspace-rotate-disable-serialization-disabled',
        rotationEtag,
        secretStore,
        {
          afterAcquisitionAuthorityLocked: async () => {
            disableLocked.resolve();
            await releaseDisable.promise;
          },
        },
      );
      await disableLocked.promise;
      releaseFinalization.resolve();
      await waitForFinalizationAuthorityLock(await firstFinalizerPid.promise);
      releaseDisable.resolve();

      expectSuccess(await disabling);
      await expect(rotation).resolves.toEqual({ kind: 'conflict' });
      expect(finalizationAttempts).toEqual([1, 2]);
      expect(secretStore.putCalls - putsBefore).toBe(1);
      expect(reports).toEqual([]);
      await expectRotationStagingCleanup('workspace-rotate-disable-serialization');
    } finally {
      releaseFinalization.resolve();
      releaseDisable.resolve();
    }
  });

  it('returns not_found and hands staged workspace rotation cleanup to the reconciler when archive wins', async () => {
    const created = expectSuccess(
      await execute(
        parse(requestBody({ password: 'workspace-rotate-archive-create-secret' })),
        'workspace-rotate-archive-create',
        await currentEtag(),
      ),
    );
    const bindingId = created.body.configured.binding!.id;
    secretStore.beforePut = async () => {
      if (secretStore.putCalls !== 2) return;
      await db
        .update(workspaces)
        .set({ status: 'archived', archivedAt: new Date(), updatedAt: new Date() })
        .where(eq(workspaces.id, workspaceId));
    };
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    const result = await rotate(
      parseWorkspaceAgentObservabilityCredentialRotationRequest({
        credentials: { type: 'bearer', token: 'workspace-rotate-archive-next-secret' },
      }),
      'workspace-rotate-archive',
      await currentEtag(),
      secretStore,
      undefined,
      { report: (event) => reports.push(event) },
    );
    expect(result).toEqual({ kind: 'not_found' });
    expect(reports).toEqual([]);
    const [reservation] = await db
      .select()
      .from(agentObservabilityMutationReservations)
      .where(
        and(
          eq(agentObservabilityMutationReservations.workspaceId, workspaceId),
          eq(agentObservabilityMutationReservations.targetBindingId, bindingId),
        ),
      );
    if (!reservation) throw new Error('missing staged workspace rotation reservation');
    const [staging] = await db
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(eq(agentObservabilityCredentialStagingIntents.reservationId, reservation.id));
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
  });

  async function seedWorkspaceMode(
    targetWorkspaceId: string,
    mode: 'inherit' | 'disabled' | 'custom',
    captureCeiling: 'metadata_only' | 'redacted_io',
  ): Promise<void> {
    const request =
      mode === 'custom'
        ? parse(
            requestBody({
              endpoint: `https://collector-matrix-current-${targetWorkspaceId}.example/v1/traces`,
              password: `workspace-matrix-current-${targetWorkspaceId}-secret`,
              captureCeiling,
              captureMode: captureCeiling,
            }),
          )
        : modeRequest(mode, captureCeiling);
    expectSuccess(
      await executeForWorkspace(
        targetWorkspaceId,
        request,
        `workspace-matrix-seed-${targetWorkspaceId}`,
        await workspaceEtag(targetWorkspaceId),
      ),
    );
  }

  async function createDegradedCustomWorkspace(
    status: 'active' | 'draining' | 'disabled' | 'archived',
    missingHead: boolean,
  ): Promise<{ workspaceId: string; bindingId: string; endpoint: string }> {
    const targetWorkspaceId = `ws_workspace_degraded_${status}_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const endpoint = `https://collector-matrix-current-${targetWorkspaceId}.example/v1/traces`;
    await db.insert(workspaces).values({
      id: targetWorkspaceId,
      organizationId,
      name: `Workspace observability degraded binding ${targetWorkspaceId}`,
      status: 'active',
      createdBy: 'integration-test',
    });
    await seedWorkspaceMode(targetWorkspaceId, 'custom', 'metadata_only');
    const [setting] = await db
      .select()
      .from(agentObservabilityWorkspaceSettings)
      .where(eq(agentObservabilityWorkspaceSettings.workspaceId, targetWorkspaceId));
    if (!setting?.bindingId) throw new Error('missing degraded workspace binding');
    if (missingHead) {
      await db
        .delete(agentObservabilityBindingCredentials)
        .where(eq(agentObservabilityBindingCredentials.bindingId, setting.bindingId));
    }
    if (status !== 'active') {
      await db
        .update(agentObservabilityBindings)
        .set({
          status,
          archivedAt: status === 'archived' ? new Date() : null,
          updatedAt: new Date(),
        })
        .where(eq(agentObservabilityBindings.id, setting.bindingId));
    }
    return { workspaceId: targetWorkspaceId, bindingId: setting.bindingId, endpoint };
  }

  async function createNonOtlpCustomWorkspace(): Promise<{
    workspaceId: string;
    bindingId: string;
  }> {
    const targetWorkspaceId = `ws_workspace_non_otlp_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const bindingId = `aob_workspace_non_otlp_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const now = new Date();
    await db.insert(workspaces).values({
      id: targetWorkspaceId,
      organizationId,
      name: 'Workspace observability non-OTLP binding',
      status: 'active',
      createdBy: 'integration-test',
    });
    await db.transaction(async (tx) => {
      await tx.insert(agentObservabilityBindings).values({
        id: bindingId,
        organizationId,
        workspaceId: targetWorkspaceId,
        scopeType: 'workspace',
        adapterType: 'langfuse_sdk',
        endpointKind: 'base_endpoint',
        endpointClass: 'public',
        endpoint: 'https://langfuse.example/api/public',
        externalProjectId: 'workspace-project',
        currentVersion: 1,
        status: 'active',
        revocationEpoch: 0,
        createdBy: 'integration-test',
        updatedBy: 'integration-test',
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      });
      await tx.insert(agentObservabilityBindingVersions).values({
        bindingId,
        version: 1,
        adapterType: 'langfuse_sdk',
        semanticProfile: 'langfuse',
        protocol: 'sdk',
        compression: 'none',
        timeoutMs: 5_000,
        environment: null,
        release: null,
        captureMode: 'metadata_only',
        sampleRate: '1',
        configSchemaVersion: 1,
        createdBy: 'integration-test',
        createdAt: now,
      });
      await tx.insert(agentObservabilityBindingCredentials).values({
        bindingId,
        secretRef: 'non-otlp-test-ref',
        credentialVersion: 1,
        keyHint: null,
        rotatedAt: now,
        updatedBy: 'integration-test',
        createdAt: now,
        updatedAt: now,
      });
      await tx
        .update(agentObservabilityWorkspaceSettings)
        .set({ mode: 'custom', bindingId, updatedAt: now })
        .where(eq(agentObservabilityWorkspaceSettings.workspaceId, targetWorkspaceId));
    });
    return { workspaceId: targetWorkspaceId, bindingId };
  }

  async function createOrganizationDefault(
    idempotencyKey: string,
    password: string,
  ): Promise<string> {
    const organizationIdempotencyKey = `organization-default-${idempotencyKey}`;
    const result = await executeOrganizationAgentObservabilityPut({
      db,
      secretStore,
      organizationId,
      principal: 'workspace-service-integration-test',
      authMethod: 'admin_api_key',
      requestId: `request_${organizationIdempotencyKey}`,
      request: parseOrganizationAgentObservabilityPutRequest(
        organizationRequestBody(`https://collector-${idempotencyKey}.example/v1/traces`, password),
      ),
      idempotencyKey: organizationIdempotencyKey,
      ifMatch: null,
    });
    if (result.kind !== 'success' || result.body.configured.default_binding === null) {
      throw new Error('expected organization default');
    }
    return result.body.configured.default_binding.id;
  }

  async function createSameTargetCustomReplay(idempotencyPrefix: string, password: string) {
    const initial = expectSuccess(
      await execute(
        parse(requestBody({ password })),
        `${idempotencyPrefix}-initial`,
        await currentEtag(),
      ),
    );
    if (initial.status !== 201) throw new Error('expected initial custom binding');
    const request = parse(requestBody({ credentials: undefined, compression: 'gzip' }));
    const response = expectSuccess(await execute(request, idempotencyPrefix, await currentEtag()));
    return { request, response };
  }

  function execute(
    request: NormalizedWorkspaceAgentObservabilityPutRequest,
    idempotencyKey: string,
    ifMatch: string | null,
    store: SecretStore | null = secretStore,
    hooks?: WorkspaceAgentObservabilityMutationExecutionHooks,
    reporter?: WorkspaceAgentObservabilityMutationReporter,
  ): Promise<ExecuteWorkspaceAgentObservabilityPutResult> {
    return executeForWorkspace(
      workspaceId,
      request,
      idempotencyKey,
      ifMatch,
      store,
      hooks,
      reporter,
    );
  }

  function executeForWorkspace(
    targetWorkspaceId: string,
    request: NormalizedWorkspaceAgentObservabilityPutRequest,
    idempotencyKey: string,
    ifMatch: string | null,
    store: SecretStore | null = secretStore,
    hooks?: WorkspaceAgentObservabilityMutationExecutionHooks,
    reporter?: WorkspaceAgentObservabilityMutationReporter,
  ): Promise<ExecuteWorkspaceAgentObservabilityPutResult> {
    return executeWorkspaceAgentObservabilityPut({
      db,
      ...(store === null ? {} : { secretStore: store }),
      organizationId,
      workspaceId: targetWorkspaceId,
      principal: 'workspace-service-integration-test',
      authMethod: 'admin_api_key',
      requestId: `request_${idempotencyKey}`,
      request,
      idempotencyKey,
      ifMatch,
      ...(hooks === undefined ? {} : { hooks }),
      ...(reporter === undefined ? {} : { reporter }),
    });
  }

  function rotate(
    request: ReturnType<typeof parseWorkspaceAgentObservabilityCredentialRotationRequest>,
    idempotencyKey: string,
    ifMatch: string | null,
    store: SecretStore | null = secretStore,
    hooks?: WorkspaceAgentObservabilityMutationExecutionHooks,
    reporter?: WorkspaceAgentObservabilityMutationReporter,
  ): Promise<ExecuteWorkspaceAgentObservabilityCredentialRotationResult> {
    return rotateForWorkspace(
      workspaceId,
      request,
      idempotencyKey,
      ifMatch,
      store,
      hooks,
      reporter,
    );
  }

  function rotateForWorkspace(
    targetWorkspaceId: string,
    request: ReturnType<typeof parseWorkspaceAgentObservabilityCredentialRotationRequest>,
    idempotencyKey: string,
    ifMatch: string | null,
    store: SecretStore | null = secretStore,
    hooks?: WorkspaceAgentObservabilityMutationExecutionHooks,
    reporter?: WorkspaceAgentObservabilityMutationReporter,
  ): Promise<ExecuteWorkspaceAgentObservabilityCredentialRotationResult> {
    return executeWorkspaceAgentObservabilityCredentialRotation({
      db,
      ...(store === null ? {} : { secretStore: store }),
      organizationId,
      workspaceId: targetWorkspaceId,
      principal: 'workspace-service-integration-test',
      authMethod: 'admin_api_key',
      requestId: `request_${idempotencyKey}`,
      request,
      idempotencyKey,
      ifMatch,
      ...(hooks === undefined ? {} : { hooks }),
      ...(reporter === undefined ? {} : { reporter }),
    });
  }

  async function currentEtag(): Promise<string> {
    return workspaceEtag(workspaceId);
  }

  async function workspaceEtag(targetWorkspaceId: string): Promise<string> {
    const state = await loadWorkspaceAgentObservabilityState({
      db,
      organizationId,
      workspaceId: targetWorkspaceId,
    });
    return agentObservabilityStateEtag(state.etagInput);
  }

  async function acquirePendingReservation(
    target:
      | {
          type: 'binding';
          organizationId: string;
          workspaceId: string;
          bindingId: string;
          bindingScope: 'workspace';
        }
      | { type: 'workspace_setting'; organizationId: string; workspaceId: string },
    key: string,
  ) {
    const result = await db.transaction((tx) =>
      acquireAgentObservabilityMutationReservation(tx, {
        target,
        idempotency: {
          organizationId,
          principal: 'workspace-preemption-boundary-test',
          scope: 'workspace_agent_observability.preemption_test',
          key,
        },
        bodyHash: agentObservabilityMutationBodyHash({ operation: key }),
        expectedVersions: {
          stateVersion: `preemption-boundary-${key}`,
          configVersion: null,
          credentialVersion: null,
        },
      }),
    );
    if (result.kind !== 'acquired')
      throw new Error(`expected pending reservation, got ${result.kind}`);
    return result.reservation;
  }

  async function reservationById(reservationId: string) {
    return (
      await db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.id, reservationId))
    )[0];
  }

  async function expectRotationStagingCleanup(key: string): Promise<void> {
    const [idempotency] = await db
      .select({ reservationId: agentObservabilityIdempotencyKeys.reservationId })
      .from(agentObservabilityIdempotencyKeys)
      .where(eq(agentObservabilityIdempotencyKeys.key, key));
    if (!idempotency) throw new Error(`missing rotation idempotency row for ${key}`);
    const [[reservation], [staging]] = await Promise.all([
      db
        .select()
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.id, idempotency.reservationId)),
      db
        .select()
        .from(agentObservabilityCredentialStagingIntents)
        .where(
          eq(agentObservabilityCredentialStagingIntents.reservationId, idempotency.reservationId),
        ),
    ]);
    expect(reservation).toMatchObject({ status: 'fenced' });
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
  }

  async function finalizationBackendPid(tx: Pick<DbClient, 'execute'>): Promise<number> {
    const result = await tx.execute<{ pid: number }>(sql`select pg_backend_pid() as pid`);
    const pid = result.rows[0]?.pid;
    if (typeof pid !== 'number' || !Number.isSafeInteger(pid) || pid <= 0) {
      throw new Error('missing workspace finalization backend pid');
    }
    return pid;
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
    throw new Error('workspace finalization did not block on authority');
  }

  async function overwriteCachedResponse(
    key: string,
    mutate: (body: Record<string, unknown>) => Record<string, unknown>,
    responseStatus?: number,
  ): Promise<void> {
    const [cached] = await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(
        and(
          eq(agentObservabilityIdempotencyKeys.organizationId, organizationId),
          eq(agentObservabilityIdempotencyKeys.key, key),
        ),
      );
    if (!cached || cached.responseBody === null || typeof cached.responseBody !== 'object') {
      throw new Error('missing completed workspace idempotency response');
    }
    const body = structuredClone(cached.responseBody) as Record<string, unknown>;
    await db
      .update(agentObservabilityIdempotencyKeys)
      .set({
        responseBody: mutate(body),
        ...(responseStatus === undefined ? {} : { responseStatus }),
        updatedAt: new Date(),
      })
      .where(eq(agentObservabilityIdempotencyKeys.reservationId, cached.reservationId));
  }

  async function overwriteCachedCustomBindingGenerations(
    key: string,
    generations: { configVersion?: number; credentialVersion?: number },
  ): Promise<void> {
    await overwriteCachedResponse(key, (body) => {
      const configured = body.configured as { binding: Record<string, unknown> | null };
      const effective = body.effective as { binding: Record<string, unknown> | null };
      if (configured.binding === null || effective.binding === null) {
        throw new Error('expected custom cache bindings');
      }
      const rewrite = (binding: Record<string, unknown>) => ({
        ...binding,
        ...(generations.configVersion === undefined
          ? {}
          : {
              config: {
                ...(binding.config as Record<string, unknown>),
                version: generations.configVersion,
              },
            }),
        ...(generations.credentialVersion === undefined
          ? {}
          : {
              credential: {
                ...(binding.credential as Record<string, unknown>),
                version: generations.credentialVersion,
              },
            }),
      });
      return {
        ...body,
        configured: { ...configured, binding: rewrite(configured.binding) },
        effective: { ...effective, binding: rewrite(effective.binding) },
      };
    });
  }

  async function expectCorruptWorkspaceReplay(
    request: NormalizedWorkspaceAgentObservabilityPutRequest,
    key: string,
    secret: string,
  ): Promise<void> {
    const reports: WorkspaceAgentObservabilityMutationFailureEvent[] = [];
    expect(
      await execute(request, key, null, secretStore, undefined, {
        report: (event) => reports.push(event),
      }),
    ).toEqual({ kind: 'unavailable' });
    expect(reports).toEqual([
      expect.objectContaining({ phase: 'acquisition', code: 'corrupt_cache' }),
    ]);
    expect(JSON.stringify(reports)).not.toContain(secret);
  }
});

function parse(value: unknown): NormalizedWorkspaceAgentObservabilityPutRequest {
  return parseWorkspaceAgentObservabilityPutRequest(value);
}

function modeRequest(
  mode: 'inherit' | 'disabled',
  captureCeiling: 'metadata_only' | 'redacted_io' = 'metadata_only',
) {
  return parse({ mode, capture_ceiling: captureCeiling });
}

function requestBody(
  options: {
    endpoint?: string;
    password?: string;
    credentials?: undefined;
    compression?: 'none' | 'gzip';
    captureCeiling?: 'metadata_only' | 'redacted_io';
    captureMode?: 'metadata_only' | 'redacted_io';
  } = {},
) {
  return {
    mode: 'custom' as const,
    target: {
      adapter_type: 'otlp_http' as const,
      endpoint_kind: 'traces_endpoint' as const,
      endpoint_class: 'public' as const,
      endpoint_url: options.endpoint ?? 'https://collector.example/v1/traces',
    },
    config: {
      semantic_profile: 'otel_genai' as const,
      protocol: 'http/protobuf' as const,
      compression: options.compression ?? ('none' as const),
      timeout_ms: 5000,
      capture_mode: options.captureMode ?? ('metadata_only' as const),
      sample_rate: 1,
    },
    capture_ceiling: options.captureCeiling ?? ('metadata_only' as const),
    ...(Object.hasOwn(options, 'credentials')
      ? {}
      : {
          credentials: {
            type: 'basic' as const,
            username: 'project',
            password: options.password ?? 'secret',
          },
        }),
  };
}

function organizationRequestBody(endpoint: string, password: string) {
  const body = requestBody({ endpoint, password });
  const { mode: _mode, ...organizationBody } = body;
  return organizationBody;
}

function expectSuccess(
  result: ExecuteWorkspaceAgentObservabilityPutResult,
): Extract<ExecuteWorkspaceAgentObservabilityPutResult, { kind: 'success' }> {
  if (result.kind !== 'success') throw new Error(`expected success, got ${result.kind}`);
  return result;
}

function expectRotationSuccess(
  result: ExecuteWorkspaceAgentObservabilityCredentialRotationResult,
): Extract<ExecuteWorkspaceAgentObservabilityCredentialRotationResult, { kind: 'success' }> {
  if (result.kind !== 'success') throw new Error(`expected rotation success, got ${result.kind}`);
  return result;
}

function nextOrganizationId(): string {
  return `org_workspace_mutation_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

class TrackingSecretStore implements SecretStore {
  readonly values = new Map<string, string>();
  readonly putStarted = deferred<void>();
  beforePut: (() => Promise<void>) | undefined;
  putCalls = 0;
  deleteCalls = 0;
  private putStartedSignaled = false;

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
    _options?: SecretStorePutOptions,
  ): Promise<void> {
    this.putCalls += 1;
    throw new Error('provider rejected rotation staging');
  }
}

class HangingSecretStore implements SecretStore {
  putCalls = 0;
  deleteCalls = 0;
  signal: AbortSignal | undefined;

  async put(_reference: string, _value: string, options?: SecretStorePutOptions): Promise<void> {
    this.putCalls += 1;
    this.signal = options?.signal;
    await new Promise<void>((_resolve, reject) => {
      const abort = () => reject(this.signal?.reason ?? new Error('rotation staging aborted'));
      if (this.signal?.aborted) abort();
      else this.signal?.addEventListener('abort', abort, { once: true });
    });
  }

  async resolve(_reference: string): Promise<string | null> {
    return null;
  }

  async delete(_reference: string): Promise<void> {
    this.deleteCalls += 1;
  }
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

function postgresError(code: string): Error & { code: string } {
  return Object.assign(new Error(`postgres ${code}`), { code });
}
