// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import { and, count, eq } from 'drizzle-orm';
import { buildCombinedTestApp } from '../../src/server.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agentObservabilityBindings,
  agentObservabilityOrganizationSettings,
  agentObservabilityPlatformPolicy,
  agentObservabilityWorkspaceSettings,
  agentTriggerFires,
  agentTriggers,
  agents,
  agentVersions,
  organizations,
  sessionLifecycleOutbox,
  sessionObservabilityBindings,
  sessionThreads,
  sessions,
} from '../../src/persistence/postgres/schema.js';
import { SessionObservabilitySelectionAvailabilityError } from '../../src/domain/agent-observability-session-selection.js';
import { executeOrganizationAgentObservabilityDisable } from '../../src/domain/agent-observability-organization-disable-service.js';
import { parseOrganizationAgentObservabilityDisableRequest } from '../../src/domain/agent-observability-organization-mutation.js';
import { executeWorkspaceAgentObservabilityPut } from '../../src/domain/agent-observability-workspace-service.js';
import { executeWorkspaceAgentObservabilityCredentialRotation } from '../../src/domain/agent-observability-workspace-rotation-service.js';
import type { WorkspaceAgentObservabilityMutationExecutionHooks } from '../../src/domain/agent-observability-workspace-service-common.js';
import {
  parseWorkspaceAgentObservabilityPutRequest,
  parseWorkspaceAgentObservabilityCredentialRotationRequest,
  type NormalizedWorkspaceAgentObservabilityPutRequest,
} from '../../src/domain/agent-observability-workspace-mutation.js';
import { agentObservabilityStateEtag } from '../../src/domain/agent-observability-etag.js';
import {
  loadOrganizationAgentObservabilityState,
  loadWorkspaceAgentObservabilityState,
} from '../../src/domain/agent-observability-state.js';
import { createSessionInTransaction } from '../../src/domain/session-creation.js';
import { dispatchPendingAgentTriggerFires } from '../../src/domain/trigger-reconciler.js';
import type { SecretStore, SecretStorePutOptions } from '../../src/secrets/secret-provider.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, TEST_ORGANIZATION_ID, uniqueWorkspace } from './fixtures.js';

const malformedAgentVersionSnapshotCases = [
  ['model', (snapshot: Record<string, unknown>) => ({ ...snapshot, model: {} })],
  ['tools', (snapshot: Record<string, unknown>) => ({ ...snapshot, tools: [null] })],
  ['mcp_servers', (snapshot: Record<string, unknown>) => ({ ...snapshot, mcp_servers: [null] })],
] as const;

describe('Session observability pinning (integration)', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let pool: Pool;

  beforeAll(async () => {
    ({ db, pool } = await getTestDb());
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();
  });

  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  it('pins disabled, inherited, and custom selections without fallback-trigger overwrites', async () => {
    const workspaceId = uniqueWorkspace('session_observability_pinning');
    const apiKey = await createTestApiKey(db, workspaceId);
    const [originalOrganizationSetting] = await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, TEST_ORGANIZATION_ID))
      .limit(1);
    expect(originalOrganizationSetting).toBeDefined();

    try {
      const agent = await createAgent(apiKey, { harness: 'codex', mode: 'in_sandbox' });
      const environmentId = await createEnvironment(apiKey);
      // Simulate corrupt mutable mode metadata without changing the immutable
      // harness identity. Pins must still use the locked version snapshot.
      await db
        .update(agents)
        .set({ metadata: { harness: 'codex', mode: 'separate' } })
        .where(and(eq(agents.workspaceId, workspaceId), eq(agents.id, agent.id)));

      await db
        .update(agentObservabilityOrganizationSettings)
        .set({
          activeDefaultBindingId: null,
          activeDefaultBindingScope: null,
          selectionEpoch: 21,
          defaultRevocationEpoch: 22,
          organizationRevocationEpoch: 23,
          captureCeiling: 'redacted_io',
          captureRestrictionEpoch: 24,
        })
        .where(eq(agentObservabilityOrganizationSettings.organizationId, TEST_ORGANIZATION_ID));
      await updateWorkspaceSetting(workspaceId, {
        mode: 'disabled',
        bindingId: null,
        selectionEpoch: 31,
        revocationEpoch: 32,
        captureCeiling: 'redacted_io',
        captureRestrictionEpoch: 33,
      });

      const disabledSessionId = await createSession(apiKey, agent.id, environmentId, {
        harness: 'claude_agent_sdk',
        mode: 'separate',
      });
      const disabledPin = await loadPin(workspaceId, disabledSessionId);
      expect(disabledPin).toMatchObject({
        organizationId: TEST_ORGANIZATION_ID,
        workspaceId,
        sessionId: disabledSessionId,
        selectionSource: 'disabled',
        status: 'disabled',
        bindingId: null,
        bindingVersion: null,
        bindingScope: null,
        bindingWorkspaceId: null,
        organizationSelectionEpoch: 21,
        workspaceSelectionEpoch: 31,
        organizationDefaultRevocationEpoch: 22,
        organizationRevocationEpoch: 23,
        workspaceRevocationEpoch: 32,
        bindingRevocationEpoch: 0,
        platformCaptureRestrictionEpoch: expect.any(Number),
        organizationCaptureRestrictionEpoch: 24,
        workspaceCaptureRestrictionEpoch: 33,
        effectiveCaptureMode: 'metadata_only',
        sessionRevocationEpoch: 0,
        agentId: agent.id,
        agentVersion: agent.version,
        harness: 'codex',
        harnessMode: 'colocated',
        archivedAt: null,
        deletedAt: null,
        createdAt: expect.any(Date),
        updatedAt: expect.any(Date),
      });

      const organizationBindingId = `aob_session_pin_organization_${workspaceId}`;
      await insertActiveBinding({
        id: organizationBindingId,
        workspaceId: null,
        revocationEpoch: 41,
      });
      await db
        .update(agentObservabilityOrganizationSettings)
        .set({
          activeDefaultBindingId: organizationBindingId,
          activeDefaultBindingScope: 'organization',
          selectionEpoch: 51,
          defaultRevocationEpoch: 52,
          organizationRevocationEpoch: 53,
          captureCeiling: 'redacted_io',
          captureRestrictionEpoch: 54,
        })
        .where(eq(agentObservabilityOrganizationSettings.organizationId, TEST_ORGANIZATION_ID));
      await updateWorkspaceSetting(workspaceId, {
        mode: 'inherit',
        bindingId: null,
        selectionEpoch: 61,
        revocationEpoch: 62,
        captureCeiling: 'redacted_io',
        captureRestrictionEpoch: 63,
      });

      const [platformPolicy] = await db
        .select({
          maxCaptureMode: agentObservabilityPlatformPolicy.maxCaptureMode,
          captureRestrictionEpoch: agentObservabilityPlatformPolicy.captureRestrictionEpoch,
        })
        .from(agentObservabilityPlatformPolicy)
        .where(eq(agentObservabilityPlatformPolicy.id, 'default'))
        .limit(1);
      expect(platformPolicy).toBeDefined();

      const inheritedSessionId = await createSession(apiKey, agent.id, environmentId);
      const inheritedPin = await loadPin(workspaceId, inheritedSessionId);
      expect(inheritedPin).toMatchObject({
        selectionSource: 'organization_default',
        status: 'active',
        bindingId: organizationBindingId,
        bindingVersion: 1,
        bindingScope: 'organization',
        bindingWorkspaceId: null,
        organizationSelectionEpoch: 51,
        workspaceSelectionEpoch: 61,
        organizationDefaultRevocationEpoch: 52,
        organizationRevocationEpoch: 53,
        workspaceRevocationEpoch: 62,
        bindingRevocationEpoch: 41,
        platformCaptureRestrictionEpoch: platformPolicy!.captureRestrictionEpoch,
        organizationCaptureRestrictionEpoch: 54,
        workspaceCaptureRestrictionEpoch: 63,
        effectiveCaptureMode:
          platformPolicy!.maxCaptureMode === 'metadata_only' ? 'metadata_only' : 'redacted_io',
        sessionRevocationEpoch: 0,
        agentId: agent.id,
        agentVersion: agent.version,
        harness: 'codex',
        harnessMode: 'colocated',
      });

      const workspaceBindingId = `aob_session_pin_workspace_${workspaceId}`;
      await insertActiveBinding({
        id: workspaceBindingId,
        workspaceId,
        revocationEpoch: 71,
      });
      await updateWorkspaceSetting(workspaceId, {
        mode: 'custom',
        bindingId: workspaceBindingId,
        selectionEpoch: 81,
        revocationEpoch: 82,
        captureCeiling: 'redacted_io',
        captureRestrictionEpoch: 83,
      });

      const customSessionId = await createSession(apiKey, agent.id, environmentId);
      const customPin = await loadPin(workspaceId, customSessionId);
      expect(customPin).toMatchObject({
        selectionSource: 'workspace_custom',
        status: 'active',
        bindingId: workspaceBindingId,
        bindingVersion: 1,
        bindingScope: 'workspace',
        bindingWorkspaceId: workspaceId,
        organizationSelectionEpoch: 51,
        workspaceSelectionEpoch: 81,
        organizationDefaultRevocationEpoch: 52,
        organizationRevocationEpoch: 53,
        workspaceRevocationEpoch: 82,
        bindingRevocationEpoch: 71,
        platformCaptureRestrictionEpoch: platformPolicy!.captureRestrictionEpoch,
        organizationCaptureRestrictionEpoch: 54,
        workspaceCaptureRestrictionEpoch: 83,
        effectiveCaptureMode:
          platformPolicy!.maxCaptureMode === 'metadata_only' ? 'metadata_only' : 'redacted_io',
        sessionRevocationEpoch: 0,
        agentId: agent.id,
        agentVersion: agent.version,
        harness: 'codex',
        harnessMode: 'colocated',
      });

      // Current writers insert this authoritative row before `sessions`; the
      // temporary AFTER INSERT fallback must lose its conflict race.
      const directSessionId = `ses_session_pin_direct_${workspaceId}`;
      const directNow = new Date();
      const directCreation = await db.transaction((tx) =>
        createSessionInTransaction(tx, {
          id: directSessionId,
          workspaceId,
          agentId: agent.id,
          agentVersion: agent.version,
          environmentId,
          title: null,
          metadata: {},
          vaultIds: [],
          tools: null,
          mcpServers: null,
          agentOverrides: null,
          initialEvents: [],
          now: directNow,
        }),
      );
      expect(directCreation).toEqual({ ok: true, initialEventsOutboxId: null });
      const directPin = await loadPin(workspaceId, directSessionId);
      expect(directPin).toMatchObject({
        selectionSource: 'workspace_custom',
        status: 'active',
        bindingId: workspaceBindingId,
        bindingVersion: 1,
        bindingScope: 'workspace',
        bindingWorkspaceId: workspaceId,
        effectiveCaptureMode: customPin!.effectiveCaptureMode,
        agentId: agent.id,
        agentVersion: agent.version,
        harness: 'codex',
        harnessMode: 'colocated',
        createdAt: directNow,
        updatedAt: directNow,
      });

      const trigger = await createTrigger(apiKey, agent, environmentId, 'pin-session-trigger');
      const triggerNow = new Date();
      const triggerSessionId = `ses_session_pin_trigger_${workspaceId}`;
      await db.insert(agentTriggerFires).values({
        id: `trgfire_session_pin_${workspaceId}`,
        workspaceId,
        triggerId: trigger.id,
        generation: 1,
        scheduledFor: triggerNow,
        status: 'pending',
        plannedSessionId: triggerSessionId,
        sessionId: null,
        eventId: `evt_session_pin_${workspaceId}`,
        attemptCount: 0,
        lastError: null,
        nextAttemptAt: triggerNow,
        enqueuedAt: null,
        createdAt: triggerNow,
        updatedAt: triggerNow,
      });
      await dispatchPendingAgentTriggerFires(db, { now: triggerNow, batchSize: 1_000 });
      const triggerPin = await loadPin(workspaceId, triggerSessionId);
      expect(triggerPin).toMatchObject({
        selectionSource: 'workspace_custom',
        status: 'active',
        bindingId: workspaceBindingId,
        bindingVersion: 1,
        effectiveCaptureMode: customPin!.effectiveCaptureMode,
        agentId: agent.id,
        agentVersion: agent.version,
        harness: 'codex',
        harnessMode: 'colocated',
      });
      // Trigger dispatch shares the current Session writer, so its active pin
      // must survive the same database fallback conflict.
      await archiveTriggerForTest(workspaceId, trigger.id);

      const corruptSessionId = `ses_session_pin_corrupt_${workspaceId}`;
      await expect(
        db.transaction(async (tx) => {
          await tx
            .update(agentObservabilityBindings)
            .set({ currentVersion: 99 })
            .where(eq(agentObservabilityBindings.id, workspaceBindingId));
          return createSessionInTransaction(tx, {
            id: corruptSessionId,
            workspaceId,
            agentId: agent.id,
            agentVersion: agent.version,
            environmentId,
            title: null,
            metadata: {},
            vaultIds: [],
            tools: null,
            mcpServers: null,
            agentOverrides: null,
            initialEvents: [],
            now: triggerNow,
          });
        }),
      ).rejects.toBeInstanceOf(SessionObservabilitySelectionAvailabilityError);
      expect(await loadPin(workspaceId, corruptSessionId)).toBeUndefined();
      expect(
        await db
          .select({ id: sessions.id })
          .from(sessions)
          .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, corruptSessionId)))
          .limit(1),
      ).toEqual([]);
    } finally {
      await db
        .update(agentObservabilityOrganizationSettings)
        .set({
          activeDefaultBindingId: originalOrganizationSetting!.activeDefaultBindingId,
          activeDefaultBindingScope: originalOrganizationSetting!.activeDefaultBindingScope,
          selectionEpoch: originalOrganizationSetting!.selectionEpoch,
          defaultRevocationEpoch: originalOrganizationSetting!.defaultRevocationEpoch,
          organizationRevocationEpoch: originalOrganizationSetting!.organizationRevocationEpoch,
          captureCeiling: originalOrganizationSetting!.captureCeiling,
          captureRestrictionEpoch: originalOrganizationSetting!.captureRestrictionEpoch,
        })
        .where(eq(agentObservabilityOrganizationSettings.organizationId, TEST_ORGANIZATION_ID));
    }
  });

  it('linearizes Session pins around workspace custom replacement and explicit disabled without rewriting old pins', async () => {
    const workspaceId = uniqueWorkspace('session_observability_workspace_put_boundary');
    const apiKey = await createTestApiKey(db, workspaceId);
    const [agent, environmentId] = await Promise.all([
      createAgent(apiKey),
      createEnvironment(apiKey),
    ]);
    const secretStore = new MemorySecretStore();
    const executeWorkspacePut = async (
      request: NormalizedWorkspaceAgentObservabilityPutRequest,
      key: string,
      ifMatch: string | null,
      hooks?: WorkspaceAgentObservabilityMutationExecutionHooks,
    ) =>
      executeWorkspaceAgentObservabilityPut({
        db,
        secretStore,
        organizationId: TEST_ORGANIZATION_ID,
        workspaceId,
        principal: 'session-observability-workspace-put-boundary',
        authMethod: 'admin_api_key',
        requestId: `request_${key}`,
        request,
        idempotencyKey: key,
        ifMatch,
        ...(hooks === undefined ? {} : { hooks }),
      });
    const executeWorkspaceRotation = async (key: string, ifMatch: string) =>
      executeWorkspaceAgentObservabilityCredentialRotation({
        db,
        secretStore,
        organizationId: TEST_ORGANIZATION_ID,
        workspaceId,
        principal: 'session-observability-workspace-put-boundary',
        authMethod: 'admin_api_key',
        requestId: `request_${key}`,
        request: parseWorkspaceAgentObservabilityCredentialRotationRequest({
          credentials: { type: 'bearer', token: 'session-boundary-rotation-secret' },
        }),
        idempotencyKey: key,
        ifMatch,
      });
    const currentWorkspaceEtag = async () => {
      const state = await loadWorkspaceAgentObservabilityState({
        db,
        organizationId: TEST_ORGANIZATION_ID,
        workspaceId,
      });
      return agentObservabilityStateEtag(state.etagInput);
    };

    const initial = await executeWorkspacePut(
      parseWorkspaceAgentObservabilityPutRequest(
        workspaceCustomBody('https://collector-session-a.example/v1/traces'),
      ),
      `session-boundary-initial-${workspaceId}`,
      await currentWorkspaceEtag(),
    );
    if (initial.kind !== 'success') throw new Error('expected initial workspace custom binding');
    const bindingA = initial.body.configured.binding!.id;
    const oldSessionId = await createSession(apiKey, agent.id, environmentId);
    const oldPin = await loadPin(workspaceId, oldSessionId);
    if (!oldPin) throw new Error('missing old Session pin');
    const rotated = await executeWorkspaceRotation(
      `session-boundary-rotation-${workspaceId}`,
      await currentWorkspaceEtag(),
    );
    expect(rotated).toMatchObject({ kind: 'success', status: 200 });
    await expect(loadPin(workspaceId, oldSessionId)).resolves.toEqual(oldPin);

    const finalizationReady = deferred<void>();
    const releaseFinalization = deferred<void>();
    const disabledLocked = deferred<void>();
    const releaseDisabled = deferred<void>();
    const replacement = executeWorkspacePut(
      parseWorkspaceAgentObservabilityPutRequest(
        workspaceCustomBody('https://collector-session-b.example/v1/traces'),
      ),
      `session-boundary-replacement-${workspaceId}`,
      await currentWorkspaceEtag(),
      {
        beforeFinalizationAuthorityLock: async () => {
          finalizationReady.resolve();
          await releaseFinalization.promise;
        },
      },
    );
    try {
      await finalizationReady.promise;
      const duringSessionId = await createSession(apiKey, agent.id, environmentId);
      const duringPin = await loadPin(workspaceId, duringSessionId);
      expect(duringPin).toMatchObject({
        selectionSource: 'workspace_custom',
        status: 'active',
        bindingId: bindingA,
      });

      releaseFinalization.resolve();
      const replaced = await replacement;
      if (replaced.kind !== 'success') throw new Error('expected workspace replacement');
      const bindingB = replaced.body.configured.binding!.id;
      expect(bindingB).not.toBe(bindingA);
      const afterSessionId = await createSession(apiKey, agent.id, environmentId);
      const afterPin = await loadPin(workspaceId, afterSessionId);
      expect(afterPin).toMatchObject({
        selectionSource: 'workspace_custom',
        status: 'active',
        bindingId: bindingB,
        workspaceSelectionEpoch: oldPin.workspaceSelectionEpoch + 1,
      });

      const [oldBeforeDisable, duringBeforeDisable, afterBeforeDisable] = await Promise.all([
        loadPin(workspaceId, oldSessionId),
        loadPin(workspaceId, duringSessionId),
        loadPin(workspaceId, afterSessionId),
      ]);
      const disabling = executeWorkspacePut(
        parseWorkspaceAgentObservabilityPutRequest({
          mode: 'disabled',
          capture_ceiling: 'metadata_only',
        }),
        `session-boundary-disabled-${workspaceId}`,
        await currentWorkspaceEtag(),
        {
          afterAcquisitionAuthorityLocked: async () => {
            disabledLocked.resolve();
            await releaseDisabled.promise;
          },
        },
      );
      await disabledLocked.promise;
      const disabledSession = createSession(apiKey, agent.id, environmentId);
      releaseDisabled.resolve();
      const disabled = await disabling;
      if (disabled.kind !== 'success') throw new Error('expected workspace explicit disabled');
      const disabledSessionId = await disabledSession;
      const disabledPin = await loadPin(workspaceId, disabledSessionId);
      expect(disabledPin).toMatchObject({
        selectionSource: 'disabled',
        status: 'disabled',
        bindingId: null,
        bindingVersion: null,
        bindingScope: null,
        bindingWorkspaceId: null,
        effectiveCaptureMode: 'metadata_only',
        workspaceSelectionEpoch: afterPin!.workspaceSelectionEpoch + 1,
        workspaceRevocationEpoch: afterPin!.workspaceRevocationEpoch + 1,
      });
      await expect(loadPin(workspaceId, oldSessionId)).resolves.toEqual(oldBeforeDisable);
      await expect(loadPin(workspaceId, duringSessionId)).resolves.toEqual(duringBeforeDisable);
      await expect(loadPin(workspaceId, afterSessionId)).resolves.toEqual(afterBeforeDisable);
    } finally {
      releaseFinalization.resolve();
      releaseDisabled.resolve();
    }
  });

  it('pins inherited sessions before disable and disabled sessions after without changing workspace custom state', async () => {
    const inheritedWorkspaceId = uniqueWorkspace('session_observability_disable_inherited');
    const customWorkspaceId = uniqueWorkspace('session_observability_disable_custom');
    const [inheritedApiKey, customApiKey] = await Promise.all([
      createTestApiKey(db, inheritedWorkspaceId),
      createTestApiKey(db, customWorkspaceId),
    ]);
    const [[originalOrganizationSetting], inheritedAgent, customAgent] = await Promise.all([
      db
        .select()
        .from(agentObservabilityOrganizationSettings)
        .where(eq(agentObservabilityOrganizationSettings.organizationId, TEST_ORGANIZATION_ID)),
      createAgent(inheritedApiKey),
      createAgent(customApiKey),
    ]);
    const [inheritedEnvironmentId, customEnvironmentId] = await Promise.all([
      createEnvironment(inheritedApiKey),
      createEnvironment(customApiKey),
    ]);
    const organizationBindingId = `aob_session_disable_organization_${inheritedWorkspaceId}`;
    const customBindingId = `aob_session_disable_custom_${customWorkspaceId}`;

    try {
      await insertActiveBinding({
        id: organizationBindingId,
        workspaceId: null,
        revocationEpoch: 7,
      });
      await insertActiveBinding({
        id: customBindingId,
        workspaceId: customWorkspaceId,
        revocationEpoch: 8,
      });
      await db
        .update(agentObservabilityOrganizationSettings)
        .set({
          activeDefaultBindingId: organizationBindingId,
          activeDefaultBindingScope: 'organization',
          selectionEpoch: originalOrganizationSetting!.selectionEpoch + 1,
          defaultRevocationEpoch: originalOrganizationSetting!.defaultRevocationEpoch,
          organizationRevocationEpoch: originalOrganizationSetting!.organizationRevocationEpoch,
          captureCeiling: originalOrganizationSetting!.captureCeiling,
          captureRestrictionEpoch: originalOrganizationSetting!.captureRestrictionEpoch,
        })
        .where(eq(agentObservabilityOrganizationSettings.organizationId, TEST_ORGANIZATION_ID));
      await updateWorkspaceSetting(inheritedWorkspaceId, {
        mode: 'inherit',
        bindingId: null,
        selectionEpoch: 71,
        revocationEpoch: 72,
        captureCeiling: 'metadata_only',
        captureRestrictionEpoch: 73,
      });
      await updateWorkspaceSetting(customWorkspaceId, {
        mode: 'custom',
        bindingId: customBindingId,
        selectionEpoch: 81,
        revocationEpoch: 82,
        captureCeiling: 'metadata_only',
        captureRestrictionEpoch: 83,
      });

      const inheritedBeforeId = await createSession(
        inheritedApiKey,
        inheritedAgent.id,
        inheritedEnvironmentId,
      );
      const customBeforeId = await createSession(customApiKey, customAgent.id, customEnvironmentId);
      const [inheritedBefore, customBefore, customSettingBefore, state] = await Promise.all([
        loadPin(inheritedWorkspaceId, inheritedBeforeId),
        loadPin(customWorkspaceId, customBeforeId),
        db
          .select()
          .from(agentObservabilityWorkspaceSettings)
          .where(eq(agentObservabilityWorkspaceSettings.workspaceId, customWorkspaceId)),
        loadOrganizationAgentObservabilityState({ db, organizationId: TEST_ORGANIZATION_ID }),
      ]);
      expect(inheritedBefore).toMatchObject({
        selectionSource: 'organization_default',
        bindingId: organizationBindingId,
      });
      expect(customBefore).toMatchObject({
        selectionSource: 'workspace_custom',
        bindingId: customBindingId,
      });
      const [lifecycleOutboxBefore] = await db
        .select({ count: count() })
        .from(sessionLifecycleOutbox);

      const disabled = await executeOrganizationAgentObservabilityDisable({
        db,
        organizationId: TEST_ORGANIZATION_ID,
        principal: 'session-pinning-disable-test',
        authMethod: 'admin_api_key',
        requestId: `request_disable_${inheritedWorkspaceId}`,
        request: parseOrganizationAgentObservabilityDisableRequest({}),
        idempotencyKey: `disable-${inheritedWorkspaceId}`,
        ifMatch: agentObservabilityStateEtag(state.etagInput),
      });
      expect(disabled).toMatchObject({ kind: 'success', status: 200 });
      const [lifecycleOutboxAfter] = await db
        .select({ count: count() })
        .from(sessionLifecycleOutbox);
      expect(lifecycleOutboxAfter).toEqual(lifecycleOutboxBefore);
      expect(await loadPin(inheritedWorkspaceId, inheritedBeforeId)).toEqual(inheritedBefore);

      const [customSettingAfter, inheritedAfterId, customAfterId] = await Promise.all([
        db
          .select()
          .from(agentObservabilityWorkspaceSettings)
          .where(eq(agentObservabilityWorkspaceSettings.workspaceId, customWorkspaceId)),
        createSession(inheritedApiKey, inheritedAgent.id, inheritedEnvironmentId),
        createSession(customApiKey, customAgent.id, customEnvironmentId),
      ]);
      const [inheritedAfter, customAfter] = await Promise.all([
        loadPin(inheritedWorkspaceId, inheritedAfterId),
        loadPin(customWorkspaceId, customAfterId),
      ]);
      expect(inheritedAfter).toMatchObject({
        selectionSource: 'disabled',
        status: 'disabled',
        bindingId: null,
        organizationDefaultRevocationEpoch: inheritedBefore!.organizationDefaultRevocationEpoch + 1,
      });
      expect(customAfter).toMatchObject({
        selectionSource: 'workspace_custom',
        status: 'active',
        bindingId: customBindingId,
      });
      expect(customSettingAfter).toEqual(customSettingBefore);
    } finally {
      await db
        .update(agentObservabilityOrganizationSettings)
        .set({
          activeDefaultBindingId: originalOrganizationSetting!.activeDefaultBindingId,
          activeDefaultBindingScope: originalOrganizationSetting!.activeDefaultBindingScope,
          selectionEpoch: originalOrganizationSetting!.selectionEpoch,
          defaultRevocationEpoch: originalOrganizationSetting!.defaultRevocationEpoch,
          organizationRevocationEpoch: originalOrganizationSetting!.organizationRevocationEpoch,
          captureCeiling: originalOrganizationSetting!.captureCeiling,
          captureRestrictionEpoch: originalOrganizationSetting!.captureRestrictionEpoch,
        })
        .where(eq(agentObservabilityOrganizationSettings.organizationId, TEST_ORGANIZATION_ID));
    }
  });

  it('archives active and disabled pins, preserving the first archive tombstone on repeat', async () => {
    const activeWorkspaceId = uniqueWorkspace('session_observability_lifecycle_active');
    const activeApiKey = await createTestApiKey(db, activeWorkspaceId);
    const activeAgent = await createAgent(activeApiKey);
    const activeEnvironmentId = await createEnvironment(activeApiKey);
    const activeBindingId = `aob_session_lifecycle_active_${activeWorkspaceId}`;
    await insertActiveBinding({
      id: activeBindingId,
      workspaceId: activeWorkspaceId,
      revocationEpoch: 1,
    });
    await updateWorkspaceSetting(activeWorkspaceId, {
      mode: 'custom',
      bindingId: activeBindingId,
      selectionEpoch: 1,
      revocationEpoch: 1,
      captureCeiling: 'metadata_only',
      captureRestrictionEpoch: 1,
    });
    const activeSessionId = await createSession(activeApiKey, activeAgent.id, activeEnvironmentId);
    expect(await loadPin(activeWorkspaceId, activeSessionId)).toMatchObject({ status: 'active' });

    const activeArchive = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${activeSessionId}/archive`,
      headers: requestHeaders(activeApiKey),
    });
    expect(activeArchive.statusCode).toBe(200);
    const activeArchivedPin = await loadPin(activeWorkspaceId, activeSessionId);
    expect(activeArchivedPin).toMatchObject({
      status: 'archived',
      archivedAt: expect.any(Date),
      deletedAt: null,
      sessionRevocationEpoch: 1,
      updatedAt: expect.any(Date),
    });

    const repeatArchive = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${activeSessionId}/archive`,
      headers: requestHeaders(activeApiKey),
    });
    expect(repeatArchive.statusCode).toBe(200);
    expect(await loadPin(activeWorkspaceId, activeSessionId)).toMatchObject({
      status: 'archived',
      archivedAt: activeArchivedPin!.archivedAt,
      deletedAt: null,
      sessionRevocationEpoch: 1,
      updatedAt: activeArchivedPin!.updatedAt,
    });

    const disabledWorkspaceId = uniqueWorkspace('session_observability_lifecycle_disabled');
    const disabledApiKey = await createTestApiKey(db, disabledWorkspaceId);
    const disabledAgent = await createAgent(disabledApiKey);
    const disabledEnvironmentId = await createEnvironment(disabledApiKey);
    await updateWorkspaceSetting(disabledWorkspaceId, {
      mode: 'disabled',
      bindingId: null,
      selectionEpoch: 1,
      revocationEpoch: 1,
      captureCeiling: 'metadata_only',
      captureRestrictionEpoch: 1,
    });
    const disabledSessionId = await createSession(
      disabledApiKey,
      disabledAgent.id,
      disabledEnvironmentId,
    );
    expect(await loadPin(disabledWorkspaceId, disabledSessionId)).toMatchObject({
      status: 'disabled',
    });

    const disabledArchive = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${disabledSessionId}/archive`,
      headers: requestHeaders(disabledApiKey),
    });
    expect(disabledArchive.statusCode).toBe(200);
    expect(await loadPin(disabledWorkspaceId, disabledSessionId)).toMatchObject({
      status: 'archived',
      archivedAt: expect.any(Date),
      deletedAt: null,
      sessionRevocationEpoch: 1,
    });
  });

  it('retains a direct-delete pin alongside its soft-deleted Session', async () => {
    const workspaceId = uniqueWorkspace('session_observability_lifecycle_delete');
    const apiKey = await createTestApiKey(db, workspaceId);
    const agent = await createAgent(apiKey);
    const environmentId = await createEnvironment(apiKey);
    await updateWorkspaceSetting(workspaceId, {
      mode: 'disabled',
      bindingId: null,
      selectionEpoch: 1,
      revocationEpoch: 1,
      captureCeiling: 'metadata_only',
      captureRestrictionEpoch: 1,
    });
    const sessionId = await createSession(apiKey, agent.id, environmentId);

    const deleted = await app.inject({
      method: 'DELETE',
      url: `/v1/sessions/${sessionId}`,
      headers: requestHeaders(apiKey),
    });
    expect(deleted.statusCode).toBe(200);
    expect(
      await db
        .select({ id: sessions.id, deletedAt: sessions.deletedAt })
        .from(sessions)
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)))
        .limit(1),
    ).toEqual([{ id: sessionId, deletedAt: expect.any(Date) }]);
    expect(await loadPin(workspaceId, sessionId)).toMatchObject({
      status: 'deleted',
      archivedAt: null,
      deletedAt: expect.any(Date),
      sessionRevocationEpoch: 1,
    });
  });

  it('preserves archive tombstone while deleting an archived Session pin', async () => {
    const workspaceId = uniqueWorkspace('session_observability_lifecycle_archive_delete');
    const apiKey = await createTestApiKey(db, workspaceId);
    const agent = await createAgent(apiKey);
    const environmentId = await createEnvironment(apiKey);
    await updateWorkspaceSetting(workspaceId, {
      mode: 'disabled',
      bindingId: null,
      selectionEpoch: 1,
      revocationEpoch: 1,
      captureCeiling: 'metadata_only',
      captureRestrictionEpoch: 1,
    });
    const sessionId = await createSession(apiKey, agent.id, environmentId);

    const archived = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${sessionId}/archive`,
      headers: requestHeaders(apiKey),
    });
    expect(archived.statusCode).toBe(200);
    const archivedPin = await loadPin(workspaceId, sessionId);
    expect(archivedPin).toMatchObject({
      status: 'archived',
      archivedAt: expect.any(Date),
      sessionRevocationEpoch: 1,
    });

    const deleted = await app.inject({
      method: 'DELETE',
      url: `/v1/sessions/${sessionId}`,
      headers: requestHeaders(apiKey),
    });
    expect(deleted.statusCode).toBe(200);
    expect(await loadPin(workspaceId, sessionId)).toMatchObject({
      status: 'deleted',
      archivedAt: archivedPin!.archivedAt,
      deletedAt: expect.any(Date),
      sessionRevocationEpoch: 2,
    });
  });

  it('rolls back archive and delete when a Session pin is missing', async () => {
    const workspaceId = uniqueWorkspace('session_observability_lifecycle_missing');
    const apiKey = await createTestApiKey(db, workspaceId);
    const agent = await createAgent(apiKey);
    const environmentId = await createEnvironment(apiKey);
    await updateWorkspaceSetting(workspaceId, {
      mode: 'disabled',
      bindingId: null,
      selectionEpoch: 1,
      revocationEpoch: 1,
      captureCeiling: 'metadata_only',
      captureRestrictionEpoch: 1,
    });

    for (const [method, suffix] of [
      ['POST', '/archive'],
      ['DELETE', ''],
    ] as const) {
      const sessionId = await createSession(apiKey, agent.id, environmentId);
      await db
        .delete(sessionObservabilityBindings)
        .where(
          and(
            eq(sessionObservabilityBindings.workspaceId, workspaceId),
            eq(sessionObservabilityBindings.sessionId, sessionId),
          ),
        );

      const response = await app.inject({
        method,
        url: `/v1/sessions/${sessionId}${suffix}`,
        headers: requestHeaders(apiKey),
      });
      expect(response.statusCode).toBe(500);
      expect(
        await db
          .select({ archivedAt: sessions.archivedAt })
          .from(sessions)
          .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)))
          .limit(1),
      ).toEqual([{ archivedAt: null }]);
      const [outboxCount] = await db
        .select({ value: count() })
        .from(sessionLifecycleOutbox)
        .where(
          and(
            eq(sessionLifecycleOutbox.workspaceId, workspaceId),
            eq(sessionLifecycleOutbox.sessionId, sessionId),
          ),
        );
      expect(outboxCount!.value).toBe(0);
    }
  });

  it.each(malformedAgentVersionSnapshotCases)(
    'returns a sanitized 503 before graph writes when a persisted AgentVersion %s is malformed',
    async (_field, corruptSnapshot) => {
      const workspaceId = uniqueWorkspace('session_observability_corrupt_agent');
      const apiKey = await createTestApiKey(db, workspaceId);
      const agent = await createAgent(apiKey);
      const environmentId = await createEnvironment(apiKey);
      const [version] = await db
        .select({ snapshot: agentVersions.snapshot })
        .from(agentVersions)
        .where(
          and(
            eq(agentVersions.workspaceId, workspaceId),
            eq(agentVersions.agentId, agent.id),
            eq(agentVersions.version, agent.version),
          ),
        )
        .limit(1);
      expect(version).toBeDefined();

      await db
        .update(agentVersions)
        .set({
          snapshot: corruptSnapshot(version!.snapshot as Record<string, unknown>),
        })
        .where(
          and(
            eq(agentVersions.workspaceId, workspaceId),
            eq(agentVersions.agentId, agent.id),
            eq(agentVersions.version, agent.version),
          ),
        );
      try {
        const response = await app.inject({
          method: 'POST',
          url: '/v1/sessions',
          headers: requestHeaders(apiKey),
          payload: { agent_id: agent.id, environment_id: environmentId },
        });
        expect(response.statusCode).toBe(503);
        expect(response.json()).toMatchObject({
          type: 'error',
          error: { type: 'overloaded_error', message: 'agent observability unavailable' },
          request_id: expect.any(String),
        });
        expect(await sessionGraphCounts(workspaceId)).toEqual({
          sessions: 0,
          pins: 0,
          threads: 0,
          outbox: 0,
        });
      } finally {
        await db
          .update(agentVersions)
          .set({ snapshot: version!.snapshot })
          .where(
            and(
              eq(agentVersions.workspaceId, workspaceId),
              eq(agentVersions.agentId, agent.id),
              eq(agentVersions.version, agent.version),
            ),
          );
      }
    },
  );

  it.each(malformedAgentVersionSnapshotCases)(
    'retries a Trigger without graph writes when a persisted AgentVersion %s is malformed',
    async (field, corruptSnapshot) => {
      const workspaceId = uniqueWorkspace(`session_observability_trigger_corrupt_${field}`);
      const apiKey = await createTestApiKey(db, workspaceId);
      const agent = await createAgent(apiKey);
      const environmentId = await createEnvironment(apiKey);
      const trigger = await createTrigger(apiKey, agent, environmentId, `corrupt-${field}-trigger`);
      const [version] = await db
        .select({ snapshot: agentVersions.snapshot })
        .from(agentVersions)
        .where(
          and(
            eq(agentVersions.workspaceId, workspaceId),
            eq(agentVersions.agentId, agent.id),
            eq(agentVersions.version, agent.version),
          ),
        )
        .limit(1);
      expect(version).toBeDefined();

      const now = new Date();
      const plannedSessionId = `ses_session_pin_trigger_corrupt_${field}_${workspaceId}`;
      const fireId = `trgfire_session_pin_trigger_corrupt_${field}_${workspaceId}`;
      await db.insert(agentTriggerFires).values({
        id: fireId,
        workspaceId,
        triggerId: trigger.id,
        generation: 1,
        scheduledFor: new Date(0),
        status: 'pending',
        plannedSessionId,
        sessionId: null,
        eventId: `evt_session_pin_trigger_corrupt_${field}_${workspaceId}`,
        attemptCount: 0,
        lastError: null,
        nextAttemptAt: now,
        enqueuedAt: null,
        createdAt: now,
        updatedAt: now,
      });
      await db
        .update(agentVersions)
        .set({ snapshot: corruptSnapshot(version!.snapshot as Record<string, unknown>) })
        .where(
          and(
            eq(agentVersions.workspaceId, workspaceId),
            eq(agentVersions.agentId, agent.id),
            eq(agentVersions.version, agent.version),
          ),
        );
      try {
        const result = await dispatchPendingAgentTriggerFires(db, { now, batchSize: 1 });
        expect(result).toMatchObject({ processed: 1, enqueued: 0, retried: 1, failed: 0 });

        const [fire] = await db
          .select()
          .from(agentTriggerFires)
          .where(
            and(eq(agentTriggerFires.workspaceId, workspaceId), eq(agentTriggerFires.id, fireId)),
          )
          .limit(1);
        const [storedTrigger] = await db
          .select({ status: agentTriggers.status })
          .from(agentTriggers)
          .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)))
          .limit(1);
        expect(fire).toMatchObject({
          status: 'pending',
          attemptCount: 1,
          sessionId: null,
          lastError: 'session agent snapshot unavailable',
        });
        expect(storedTrigger).toEqual({ status: 'active' });
        expect(await sessionGraphCounts(workspaceId)).toEqual({
          sessions: 0,
          pins: 0,
          threads: 0,
          outbox: 0,
        });
      } finally {
        await db
          .update(agentVersions)
          .set({ snapshot: version!.snapshot })
          .where(
            and(
              eq(agentVersions.workspaceId, workspaceId),
              eq(agentVersions.agentId, agent.id),
              eq(agentVersions.version, agent.version),
            ),
          );
        await archiveTriggerForTest(workspaceId, trigger.id);
      }
    },
  );

  it('returns a sanitized 503 with no partial graph, then retries a Trigger selection fault without pausing it', async () => {
    const workspaceId = uniqueWorkspace('session_observability_missing');
    const apiKey = await createTestApiKey(db, workspaceId);
    const agent = await createAgent(apiKey);
    const environmentId = await createEnvironment(apiKey);
    await db
      .delete(agentObservabilityWorkspaceSettings)
      .where(
        and(
          eq(agentObservabilityWorkspaceSettings.organizationId, TEST_ORGANIZATION_ID),
          eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId),
        ),
      );

    const before = await sessionGraphCounts(workspaceId);
    const response = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: requestHeaders(apiKey),
      payload: { agent_id: agent.id, environment_id: environmentId },
    });
    expect(response.statusCode).toBe(503);
    expect(response.json()).toMatchObject({
      type: 'error',
      error: { type: 'overloaded_error', message: 'agent observability unavailable' },
      request_id: expect.any(String),
    });
    expect(response.body).not.toContain(workspaceId);
    expect(response.body).not.toContain('missing_workspace_setting');
    expect(await sessionGraphCounts(workspaceId)).toEqual(before);

    const trigger = await createTrigger(apiKey, agent, environmentId, 'retry-selection-trigger');
    const now = new Date();
    const plannedSessionId = `ses_session_pin_retry_${workspaceId}`;
    const fireId = `trgfire_session_pin_retry_${workspaceId}`;
    await db.insert(agentTriggerFires).values({
      id: fireId,
      workspaceId,
      triggerId: trigger.id,
      generation: 1,
      scheduledFor: new Date(0),
      status: 'pending',
      plannedSessionId,
      sessionId: null,
      eventId: `evt_session_pin_retry_${workspaceId}`,
      attemptCount: 0,
      lastError: null,
      nextAttemptAt: now,
      enqueuedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await dispatchPendingAgentTriggerFires(db, { now, batchSize: 1_000 });

    const [fire] = await db
      .select()
      .from(agentTriggerFires)
      .where(and(eq(agentTriggerFires.workspaceId, workspaceId), eq(agentTriggerFires.id, fireId)))
      .limit(1);
    const [storedTrigger] = await db
      .select({ status: agentTriggers.status })
      .from(agentTriggers)
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, trigger.id)))
      .limit(1);
    expect(fire).toMatchObject({
      status: 'pending',
      attemptCount: 1,
      sessionId: null,
      lastError: 'session observability selection unavailable',
    });
    expect(storedTrigger).toEqual({ status: 'active' });
    expect(await loadPin(workspaceId, plannedSessionId)).toBeUndefined();
    expect(
      await db
        .select({ id: sessions.id })
        .from(sessions)
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, plannedSessionId)))
        .limit(1),
    ).toEqual([]);
    await archiveTriggerForTest(workspaceId, trigger.id);
  });

  it('maps an archived authoritative organization to the existing non-leaking Session-create 404', async () => {
    const workspaceId = uniqueWorkspace('session_observability_archived_parent');
    const apiKey = await createTestApiKey(db, workspaceId);
    const agent = await createAgent(apiKey);
    const environmentId = await createEnvironment(apiKey);

    await db
      .update(organizations)
      .set({ status: 'archived' })
      .where(eq(organizations.id, TEST_ORGANIZATION_ID));
    try {
      const response = await app.inject({
        method: 'POST',
        url: '/v1/sessions',
        headers: requestHeaders(apiKey),
        payload: { agent_id: agent.id, environment_id: environmentId },
      });
      expect(response.statusCode).toBe(404);
      expect(response.json()).toMatchObject({
        type: 'error',
        error: { type: 'not_found_error', message: 'workspace not found' },
        request_id: expect.any(String),
      });
      expect(await sessionGraphCounts(workspaceId)).toEqual({
        sessions: 0,
        pins: 0,
        threads: 0,
        outbox: 0,
      });
    } finally {
      await db
        .update(organizations)
        .set({ status: 'active' })
        .where(eq(organizations.id, TEST_ORGANIZATION_ID));
    }
  });

  async function createAgent(
    apiKey: string,
    metadata: Record<string, unknown> = {},
  ): Promise<{ id: string; version: number }> {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: requestHeaders(apiKey),
      payload: {
        name: `session-observability-pin-agent-${Date.now()}-${Math.random()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata,
      },
    });
    expect(response.statusCode).toBe(200);
    return response.json<{ id: string; version: number }>();
  }

  async function createEnvironment(apiKey: string): Promise<string> {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers: requestHeaders(apiKey),
      payload: {
        name: `session-observability-pin-env-${Date.now()}-${Math.random()}`,
        config: { type: 'cloud' },
      },
    });
    expect(response.statusCode).toBe(200);
    return response.json<{ id: string }>().id;
  }

  async function createSession(
    apiKey: string,
    agentId: string,
    environmentId: string,
    metadata?: Record<string, string>,
  ): Promise<string> {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: requestHeaders(apiKey),
      payload: {
        agent_id: agentId,
        environment_id: environmentId,
        ...(metadata ? { metadata } : {}),
      },
    });
    expect(response.statusCode).toBe(200);
    return response.json<{ id: string }>().id;
  }

  async function createTrigger(
    apiKey: string,
    agent: { id: string; version: number },
    environmentId: string,
    name: string,
  ): Promise<{ id: string }> {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/triggers',
      headers: requestHeaders(apiKey),
      payload: {
        name,
        agent: { type: 'agent', id: agent.id, version: agent.version },
        session_mode: 'SESSION_PER_EVENT',
        source: {
          type: 'cron',
          schedule: '0 * * * *',
          timezone: 'UTC',
          payload: 'pin this Trigger Session',
        },
        session: { environment_id: environmentId },
        replicas: 1,
      },
    });
    expect(response.statusCode).toBe(200);
    return response.json<{ id: string }>();
  }

  async function archiveTriggerForTest(workspaceId: string, triggerId: string): Promise<void> {
    const now = new Date();
    await db
      .delete(agentTriggerFires)
      .where(
        and(
          eq(agentTriggerFires.workspaceId, workspaceId),
          eq(agentTriggerFires.triggerId, triggerId),
        ),
      );
    await db
      .update(agentTriggers)
      .set({ status: 'archived', archivedAt: now, nextFireAt: null, updatedAt: now })
      .where(and(eq(agentTriggers.workspaceId, workspaceId), eq(agentTriggers.id, triggerId)));
  }

  async function updateWorkspaceSetting(
    workspaceId: string,
    input: {
      mode: 'inherit' | 'disabled' | 'custom';
      bindingId: string | null;
      selectionEpoch: number;
      revocationEpoch: number;
      captureCeiling: 'metadata_only' | 'redacted_io';
      captureRestrictionEpoch: number;
    },
  ): Promise<void> {
    await db
      .update(agentObservabilityWorkspaceSettings)
      .set({
        mode: input.mode,
        bindingId: input.bindingId,
        selectionEpoch: input.selectionEpoch,
        revocationEpoch: input.revocationEpoch,
        captureCeiling: input.captureCeiling,
        captureRestrictionEpoch: input.captureRestrictionEpoch,
      })
      .where(
        and(
          eq(agentObservabilityWorkspaceSettings.organizationId, TEST_ORGANIZATION_ID),
          eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId),
        ),
      );
  }

  async function insertActiveBinding(input: {
    id: string;
    workspaceId: string | null;
    revocationEpoch: number;
  }): Promise<void> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `
          INSERT INTO agent_observability_bindings (
            id, organization_id, workspace_id, scope_type, adapter_type, endpoint_kind, endpoint_class,
            endpoint, external_project_id, current_version, status, revocation_epoch, created_by, updated_by
          ) VALUES ($1, $2, $3, $4, 'otlp_http', 'traces_endpoint', 'public',
                    'https://collector.example/v1/traces', 'pk-session-pinning', 1, 'active', $5,
                    'integration-test', 'integration-test')
        `,
        [
          input.id,
          TEST_ORGANIZATION_ID,
          input.workspaceId,
          input.workspaceId === null ? 'organization' : 'workspace',
          input.revocationEpoch,
        ],
      );
      await client.query(
        `
          INSERT INTO agent_observability_binding_versions (
            binding_id, version, adapter_type, semantic_profile, protocol, compression, timeout_ms,
            environment, release, capture_mode, sample_rate, config_schema_version, created_by
          ) VALUES ($1, 1, 'otlp_http', 'langfuse', 'http/protobuf', 'none', 5000,
                    'integration', 'v1', 'redacted_io', 0.5000, 1, 'integration-test')
        `,
        [input.id],
      );
      await client.query(
        `
          INSERT INTO agent_observability_binding_credentials (
            binding_id, secret_ref, credential_version, key_hint, rotated_at, updated_by
          ) VALUES ($1, $2, 7, '...pin', now(), 'integration-test')
        `,
        [input.id, `session-pinning-secret-ref-${input.id}`],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async function loadPin(workspaceId: string, sessionId: string) {
    return (
      await db
        .select()
        .from(sessionObservabilityBindings)
        .where(
          and(
            eq(sessionObservabilityBindings.workspaceId, workspaceId),
            eq(sessionObservabilityBindings.sessionId, sessionId),
          ),
        )
        .limit(1)
    )[0];
  }

  async function sessionGraphCounts(workspaceId: string): Promise<{
    sessions: number;
    pins: number;
    threads: number;
    outbox: number;
  }> {
    const [[sessionCount], [pinCount], [threadCount], [outboxCount]] = await Promise.all([
      db.select({ value: count() }).from(sessions).where(eq(sessions.workspaceId, workspaceId)),
      db
        .select({ value: count() })
        .from(sessionObservabilityBindings)
        .where(eq(sessionObservabilityBindings.workspaceId, workspaceId)),
      db
        .select({ value: count() })
        .from(sessionThreads)
        .where(eq(sessionThreads.workspaceId, workspaceId)),
      db
        .select({ value: count() })
        .from(sessionLifecycleOutbox)
        .where(eq(sessionLifecycleOutbox.workspaceId, workspaceId)),
    ]);
    return {
      sessions: sessionCount!.value,
      pins: pinCount!.value,
      threads: threadCount!.value,
      outbox: outboxCount!.value,
    };
  }
});

function requestHeaders(apiKey: string): Record<string, string> {
  return { 'x-api-key': apiKey, 'content-type': 'application/json' };
}

function workspaceCustomBody(endpoint: string): Record<string, unknown> {
  return {
    mode: 'custom',
    target: {
      adapter_type: 'otlp_http',
      endpoint_kind: 'traces_endpoint',
      endpoint_class: 'public',
      endpoint_url: endpoint,
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
      password: 'session-boundary-secret',
    },
  };
}

class MemorySecretStore implements SecretStore {
  readonly values = new Map<string, string>();

  async put(reference: string, value: string, options?: SecretStorePutOptions): Promise<void> {
    options?.signal?.throwIfAborted();
    this.values.set(reference, value);
  }

  async resolve(reference: string): Promise<string | null> {
    return this.values.get(reference) ?? null;
  }

  async delete(reference: string): Promise<void> {
    this.values.delete(reference);
  }
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}
