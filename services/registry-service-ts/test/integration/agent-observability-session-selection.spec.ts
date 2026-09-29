// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { InMemorySkillStore } from '@orca/skill-store';
import { eq } from 'drizzle-orm';
import { TransactionRollbackError } from 'drizzle-orm/errors';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import type { FastifyInstance } from 'fastify';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { KubernetesServiceAccountAuthVerifier } from '../../src/auth/internal-auth.js';
import {
  SessionObservabilitySelectionAvailabilityError,
  SessionObservabilitySelectionResourceUnavailableError,
  selectSessionObservabilityBindingInTransaction,
  type SessionObservabilitySelection,
} from '../../src/domain/agent-observability-session-selection.js';
import {
  AgentObservabilitySessionContextUnavailableError,
  loadAgentObservabilitySessionContext,
  loadAgentObservabilitySessionContextInTransaction,
} from '../../src/domain/agent-observability-context-resolver.js';
import { createSessionInTransaction } from '../../src/domain/session-creation.js';
import {
  buildAdminApp,
  buildCombinedTestApp,
  buildInternalApp,
  buildPublicApp,
} from '../../src/server.js';
import type { DbClient, DbTransaction } from '../../src/persistence/postgres/client.js';
import * as schema from '../../src/persistence/postgres/schema.js';
import {
  agentObservabilityBindings,
  organizations,
  sessionObservabilityBindings,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  STUB_SSE_CONFIG,
} from './setup.js';

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

describe('Session observability selection', () => {
  let db: DbClient;
  let pool: Pool;
  let adminPool: Pool | undefined;
  let app: FastifyInstance;
  const suffix = `${process.pid}_${randomBytes(5).toString('hex')}`;
  const databaseName = `registry_agent_observability_selection_${process.pid}_${randomBytes(4).toString('hex')}`;
  const organizationId = `org_observability_selection_${suffix}`;
  const otherOrganizationId = `org_observability_selection_other_${suffix}`;
  const inactiveOrganizationId = `org_observability_selection_inactive_${suffix}`;
  const inheritedWorkspaceId = `ws_observability_selection_inherit_${suffix}`;
  const customWorkspaceId = `ws_observability_selection_custom_${suffix}`;
  const disabledWorkspaceId = `ws_observability_selection_disabled_${suffix}`;
  const versionWorkspaceId = `ws_observability_selection_version_${suffix}`;
  const lockWorkspaceId = `ws_observability_selection_lock_${suffix}`;
  const missingWorkspaceSettingId = `ws_observability_selection_missing_setting_${suffix}`;
  const inactiveWorkspaceId = `ws_observability_selection_inactive_${suffix}`;
  const otherWorkspaceId = `ws_observability_selection_other_${suffix}`;
  const crossOrganizationWorkspaceId = `ws_observability_selection_cross_${suffix}`;
  const inactiveOrganizationWorkspaceId = `ws_observability_selection_inactive_org_${suffix}`;
  const defaultBindingId = `aob_observability_selection_default_${suffix}`;
  const customBindingId = `aob_observability_selection_custom_${suffix}`;
  const versionBindingId = `aob_observability_selection_version_${suffix}`;
  const lockBindingId = `aob_observability_selection_lock_${suffix}`;
  const otherWorkspaceBindingId = `aob_observability_selection_other_${suffix}`;
  const missingVersionBindingId = `aob_observability_selection_missing_version_${suffix}`;
  const missingBindingId = `aob_observability_selection_missing_binding_${suffix}`;
  const secretRefSentinel = `session_selection_secret_ref_sentinel_${suffix}`;
  const contextSessionId = `ses_observability_context_${suffix}`;
  const contextCustomSessionId = `ses_observability_context_custom_${suffix}`;
  const contextDisabledSessionId = `ses_observability_context_disabled_${suffix}`;
  const contextArchivedSessionId = `ses_observability_context_archived_${suffix}`;
  const contextDeletedSessionId = `ses_observability_context_deleted_${suffix}`;
  const contextMissingVersionSessionId = `ses_observability_context_missing_version_${suffix}`;
  const contextMissingBindingSessionId = `ses_observability_context_missing_binding_${suffix}`;

  beforeAll(async () => {
    adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 2 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    await adminPool.query(`CREATE DATABASE "${databaseName}"`);
    pool = new Pool({ connectionString: databaseUrl.toString(), max: 8 });
    db = drizzle(pool, { schema });
    await migrate(db, { migrationsFolder: MIGRATIONS_FOLDER });

    await createOrganization(db, organizationId, 'Session selection organization');
    await createOrganization(db, otherOrganizationId, 'Other organization');
    await createOrganization(db, inactiveOrganizationId, 'Inactive organization');
    for (const workspaceId of [
      inheritedWorkspaceId,
      customWorkspaceId,
      disabledWorkspaceId,
      versionWorkspaceId,
      lockWorkspaceId,
      missingWorkspaceSettingId,
      inactiveWorkspaceId,
      otherWorkspaceId,
    ]) {
      await createWorkspace(db, workspaceId, organizationId, workspaceId);
    }
    await createWorkspace(
      db,
      crossOrganizationWorkspaceId,
      otherOrganizationId,
      'Cross organization workspace',
    );
    await createWorkspace(
      db,
      inactiveOrganizationWorkspaceId,
      inactiveOrganizationId,
      'Inactive organization workspace',
    );

    await insertBinding(pool, {
      id: defaultBindingId,
      organizationId,
      workspaceId: null,
      secretRef: secretRefSentinel,
      revocationEpoch: 109,
    });
    await insertBinding(pool, {
      id: customBindingId,
      organizationId,
      workspaceId: customWorkspaceId,
      revocationEpoch: 209,
    });
    await insertBinding(pool, {
      id: versionBindingId,
      organizationId,
      workspaceId: versionWorkspaceId,
    });
    await insertBinding(pool, {
      id: lockBindingId,
      organizationId,
      workspaceId: lockWorkspaceId,
    });
    await insertBinding(pool, {
      id: otherWorkspaceBindingId,
      organizationId,
      workspaceId: otherWorkspaceId,
    });
    await insertBinding(pool, {
      id: missingVersionBindingId,
      organizationId,
      workspaceId: null,
    });
    await insertBinding(pool, {
      id: missingBindingId,
      organizationId,
      workspaceId: null,
    });
    await pool.query(
      `
        INSERT INTO agent_observability_binding_versions (
          binding_id, version, adapter_type, semantic_profile, protocol, compression, timeout_ms,
          environment, release, capture_mode, sample_rate, config_schema_version, created_by
        ) VALUES ($1, 2, 'otlp_http', 'langfuse', 'http/protobuf', 'none', 5000,
                  'integration', 'v2-current-head', 'redacted_io', 0.5, 1, 'integration-test')
      `,
      [missingVersionBindingId],
    );
    await pool.query(`UPDATE agent_observability_bindings SET current_version = 2 WHERE id = $1`, [
      missingVersionBindingId,
    ]);

    await pool.query(
      `
        UPDATE agent_observability_organization_settings
        SET active_default_binding_id = $1,
            active_default_binding_scope = 'organization',
            selection_epoch = 102,
            default_revocation_epoch = 103,
            organization_revocation_epoch = 104,
            capture_ceiling = 'redacted_io',
            capture_restriction_epoch = 105
        WHERE organization_id = $2
      `,
      [defaultBindingId, organizationId],
    );
    await setWorkspaceMode(pool, customWorkspaceId, organizationId, 'custom', customBindingId, {
      selectionEpoch: 106,
      revocationEpoch: 107,
      captureCeiling: 'redacted_io',
      captureRestrictionEpoch: 108,
    });
    await setWorkspaceMode(pool, disabledWorkspaceId, organizationId, 'disabled', null);
    await setWorkspaceMode(pool, inheritedWorkspaceId, organizationId, 'inherit', null, {
      captureCeiling: 'redacted_io',
    });
    await setWorkspaceMode(pool, versionWorkspaceId, organizationId, 'custom', versionBindingId);
    await setWorkspaceMode(pool, lockWorkspaceId, organizationId, 'custom', lockBindingId, {
      captureCeiling: 'redacted_io',
    });
    await setWorkspaceMode(
      pool,
      otherWorkspaceId,
      organizationId,
      'custom',
      otherWorkspaceBindingId,
    );
    await pool.query(
      `
        UPDATE agent_observability_platform_policy
        SET max_capture_mode = 'redacted_io', capture_restriction_epoch = 101
        WHERE id = 'default'
      `,
    );
    await db.insert(sessionObservabilityBindings).values({
      workspaceId: inheritedWorkspaceId,
      sessionId: contextSessionId,
      organizationId,
      bindingId: defaultBindingId,
      bindingVersion: 1,
      bindingScope: 'organization',
      bindingWorkspaceId: null,
      selectionSource: 'organization_default',
      status: 'active',
      organizationSelectionEpoch: 102,
      workspaceSelectionEpoch: 0,
      organizationDefaultRevocationEpoch: 103,
      organizationRevocationEpoch: 104,
      workspaceRevocationEpoch: 0,
      bindingRevocationEpoch: 109,
      platformCaptureRestrictionEpoch: 101,
      organizationCaptureRestrictionEpoch: 105,
      workspaceCaptureRestrictionEpoch: 0,
      effectiveCaptureMode: 'redacted_io',
      sessionRevocationEpoch: 0,
      agentId: `agt_observability_context_${suffix}`,
      agentVersion: 1,
      harness: 'codex',
      harnessMode: 'colocated',
    });
    await db.insert(sessionObservabilityBindings).values([
      {
        workspaceId: customWorkspaceId,
        sessionId: contextCustomSessionId,
        organizationId,
        bindingId: customBindingId,
        bindingVersion: 1,
        bindingScope: 'workspace',
        bindingWorkspaceId: customWorkspaceId,
        selectionSource: 'workspace_custom',
        status: 'active',
        organizationSelectionEpoch: 102,
        workspaceSelectionEpoch: 106,
        organizationDefaultRevocationEpoch: 103,
        organizationRevocationEpoch: 104,
        workspaceRevocationEpoch: 107,
        bindingRevocationEpoch: 209,
        platformCaptureRestrictionEpoch: 101,
        organizationCaptureRestrictionEpoch: 105,
        workspaceCaptureRestrictionEpoch: 108,
        effectiveCaptureMode: 'redacted_io',
        sessionRevocationEpoch: 0,
        agentId: `agt_observability_context_custom_${suffix}`,
        agentVersion: 1,
        harness: null,
        harnessMode: null,
      },
      {
        workspaceId: disabledWorkspaceId,
        sessionId: contextDisabledSessionId,
        organizationId,
        bindingId: null,
        bindingVersion: null,
        bindingScope: null,
        bindingWorkspaceId: null,
        selectionSource: 'disabled',
        status: 'disabled',
        organizationSelectionEpoch: 102,
        workspaceSelectionEpoch: 0,
        organizationDefaultRevocationEpoch: 103,
        organizationRevocationEpoch: 104,
        workspaceRevocationEpoch: 0,
        bindingRevocationEpoch: 0,
        platformCaptureRestrictionEpoch: 101,
        organizationCaptureRestrictionEpoch: 105,
        workspaceCaptureRestrictionEpoch: 0,
        effectiveCaptureMode: 'metadata_only',
        sessionRevocationEpoch: 0,
        agentId: `agt_observability_context_disabled_${suffix}`,
        agentVersion: 1,
        harness: null,
        harnessMode: null,
      },
      {
        workspaceId: inheritedWorkspaceId,
        sessionId: contextMissingVersionSessionId,
        organizationId,
        bindingId: missingVersionBindingId,
        bindingVersion: 1,
        bindingScope: 'organization',
        bindingWorkspaceId: null,
        selectionSource: 'organization_default',
        status: 'active',
        organizationSelectionEpoch: 102,
        workspaceSelectionEpoch: 0,
        organizationDefaultRevocationEpoch: 103,
        organizationRevocationEpoch: 104,
        workspaceRevocationEpoch: 0,
        bindingRevocationEpoch: 0,
        platformCaptureRestrictionEpoch: 101,
        organizationCaptureRestrictionEpoch: 105,
        workspaceCaptureRestrictionEpoch: 0,
        effectiveCaptureMode: 'redacted_io',
        sessionRevocationEpoch: 0,
        agentId: `agt_observability_context_missing_version_${suffix}`,
        agentVersion: 1,
        harness: null,
        harnessMode: null,
      },
      {
        workspaceId: inheritedWorkspaceId,
        sessionId: contextMissingBindingSessionId,
        organizationId,
        bindingId: missingBindingId,
        bindingVersion: 1,
        bindingScope: 'organization',
        bindingWorkspaceId: null,
        selectionSource: 'organization_default',
        status: 'active',
        organizationSelectionEpoch: 102,
        workspaceSelectionEpoch: 0,
        organizationDefaultRevocationEpoch: 103,
        organizationRevocationEpoch: 104,
        workspaceRevocationEpoch: 0,
        bindingRevocationEpoch: 0,
        platformCaptureRestrictionEpoch: 101,
        organizationCaptureRestrictionEpoch: 105,
        workspaceCaptureRestrictionEpoch: 0,
        effectiveCaptureMode: 'redacted_io',
        sessionRevocationEpoch: 0,
        agentId: `agt_observability_context_missing_binding_${suffix}`,
        agentVersion: 1,
        harness: null,
        harnessMode: null,
      },
      {
        workspaceId: inheritedWorkspaceId,
        sessionId: contextArchivedSessionId,
        organizationId,
        bindingId: defaultBindingId,
        bindingVersion: 1,
        bindingScope: 'organization',
        bindingWorkspaceId: null,
        selectionSource: 'organization_default',
        status: 'archived',
        organizationSelectionEpoch: 102,
        workspaceSelectionEpoch: 0,
        organizationDefaultRevocationEpoch: 103,
        organizationRevocationEpoch: 104,
        workspaceRevocationEpoch: 0,
        bindingRevocationEpoch: 109,
        platformCaptureRestrictionEpoch: 101,
        organizationCaptureRestrictionEpoch: 105,
        workspaceCaptureRestrictionEpoch: 0,
        effectiveCaptureMode: 'redacted_io',
        sessionRevocationEpoch: 0,
        agentId: `agt_observability_context_archived_${suffix}`,
        agentVersion: 1,
        harness: null,
        harnessMode: null,
        archivedAt: new Date(),
      },
      {
        workspaceId: inheritedWorkspaceId,
        sessionId: contextDeletedSessionId,
        organizationId,
        bindingId: defaultBindingId,
        bindingVersion: 1,
        bindingScope: 'organization',
        bindingWorkspaceId: null,
        selectionSource: 'organization_default',
        status: 'deleted',
        organizationSelectionEpoch: 102,
        workspaceSelectionEpoch: 0,
        organizationDefaultRevocationEpoch: 103,
        organizationRevocationEpoch: 104,
        workspaceRevocationEpoch: 0,
        bindingRevocationEpoch: 109,
        platformCaptureRestrictionEpoch: 101,
        organizationCaptureRestrictionEpoch: 105,
        workspaceCaptureRestrictionEpoch: 0,
        effectiveCaptureMode: 'redacted_io',
        sessionRevocationEpoch: 1,
        agentId: `agt_observability_context_deleted_${suffix}`,
        agentVersion: 1,
        harness: null,
        harnessMode: null,
        deletedAt: new Date(),
      },
    ]);

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
    await app?.close().catch(() => {});
    await pool?.end().catch(() => {});
    await adminPool?.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => {});
    await adminPool?.end().catch(() => {});
  });

  it('uses locked exact ownership and snapshots every current selection, revocation, and capture epoch', async () => {
    const custom = await select(customWorkspaceId);
    expect(custom).toEqual(
      expect.objectContaining({
        organizationId,
        workspaceId: customWorkspaceId,
        status: 'active',
        selectionSource: 'workspace_custom',
        bindingId: customBindingId,
        bindingVersion: 1,
        bindingScope: 'workspace',
        bindingWorkspaceId: customWorkspaceId,
        organizationSelectionEpoch: 102,
        workspaceSelectionEpoch: 106,
        organizationDefaultRevocationEpoch: 103,
        organizationRevocationEpoch: 104,
        workspaceRevocationEpoch: 107,
        bindingRevocationEpoch: 209,
        platformCaptureRestrictionEpoch: 101,
        organizationCaptureRestrictionEpoch: 105,
        workspaceCaptureRestrictionEpoch: 108,
        effectiveCaptureMode: 'redacted_io',
        disabledReason: null,
      }),
    );

    const inherited = await select(inheritedWorkspaceId);
    expect(inherited).toEqual(
      expect.objectContaining({
        organizationId,
        workspaceId: inheritedWorkspaceId,
        status: 'active',
        selectionSource: 'organization_default',
        bindingId: defaultBindingId,
        bindingScope: 'organization',
        bindingWorkspaceId: null,
        bindingRevocationEpoch: 109,
      }),
    );

    const siblingCustom = await select(otherWorkspaceId);
    expect(siblingCustom).toEqual(
      expect.objectContaining({
        status: 'active',
        selectionSource: 'workspace_custom',
        bindingId: otherWorkspaceBindingId,
        bindingScope: 'workspace',
        bindingWorkspaceId: otherWorkspaceId,
      }),
    );

    const crossOrganization = await select(crossOrganizationWorkspaceId);
    expect(crossOrganization).toEqual(
      expect.objectContaining({
        organizationId: otherOrganizationId,
        workspaceId: crossOrganizationWorkspaceId,
        status: 'disabled',
        selectionSource: 'disabled',
        disabledReason: 'no_organization_default',
        bindingId: null,
      }),
    );
  });

  it('keeps capture restrictions sticky while resolving the exact pinned version', async () => {
    const originalPlatform = { maxCaptureMode: 'redacted_io', captureRestrictionEpoch: 101 };
    const freshSessionId = `ses_observability_context_fresh_${suffix}`;
    const freshAgentId = `agt_observability_context_fresh_${suffix}`;
    const freshEnvironmentId = `env_observability_context_fresh_${suffix}`;
    try {
      const initial = await loadAgentObservabilitySessionContext({
        db,
        workspaceId: inheritedWorkspaceId,
        sessionId: contextSessionId,
      });
      expect(initial).toMatchObject({
        schema_version: 1,
        status: 'enabled',
        reason: null,
        organization_id: organizationId,
        workspace_id: inheritedWorkspaceId,
        session_id: contextSessionId,
        selection_source: 'organization_default',
        binding: {
          id: defaultBindingId,
          version: 1,
          scope: 'organization',
          target: {
            endpoint_url: 'https://collector.example/v1/traces',
            external_project_id: 'pk-session-selection',
          },
          config: { release: 'v1', capture_mode: 'redacted_io' },
          current_credential_version: 7,
        },
        capture: { pinned_mode: 'redacted_io', effective_mode: 'redacted_io' },
      });
      expect(JSON.stringify(initial)).not.toContain(secretRefSentinel);
      expect(JSON.stringify(initial)).not.toContain('secret_ref');

      await pool.query(
        `UPDATE agent_observability_bindings SET status = 'draining' WHERE id = $1`,
        [defaultBindingId],
      );
      const draining = await loadAgentObservabilitySessionContext({
        db,
        workspaceId: inheritedWorkspaceId,
        sessionId: contextSessionId,
      });
      expect(draining).toMatchObject({
        status: 'enabled',
        binding: { id: defaultBindingId, version: 1, lifecycle_status: 'draining' },
      });
      await pool.query(`UPDATE agent_observability_bindings SET status = 'active' WHERE id = $1`, [
        defaultBindingId,
      ]);

      // New binding heads change future Session selection, never this pin's
      // immutable policy version or target/config delivery metadata.
      await pool.query(
        `
          INSERT INTO agent_observability_binding_versions (
            binding_id, version, adapter_type, semantic_profile, protocol, compression, timeout_ms,
            environment, release, capture_mode, sample_rate, config_schema_version, created_by
          ) VALUES ($1, 2, 'otlp_http', 'langfuse', 'http/protobuf', 'none', 5000,
                    'integration', 'v2-current-head', 'redacted_io', 1, 1, 'integration-test')
        `,
        [defaultBindingId],
      );
      await pool.query(
        `UPDATE agent_observability_bindings SET current_version = 2 WHERE id = $1`,
        [defaultBindingId],
      );
      const oldPinAfterHeadChange = await loadAgentObservabilitySessionContext({
        db,
        workspaceId: inheritedWorkspaceId,
        sessionId: contextSessionId,
      });
      expect(oldPinAfterHeadChange.binding).toMatchObject({
        version: 1,
        config: { release: 'v1', capture_mode: 'redacted_io' },
      });

      await pool.query(
        `
          UPDATE agent_observability_binding_credentials
          SET credential_version = 8, rotated_at = now()
          WHERE binding_id = $1
        `,
        [defaultBindingId],
      );
      const afterRotation = await loadAgentObservabilitySessionContext({
        db,
        workspaceId: inheritedWorkspaceId,
        sessionId: contextSessionId,
      });
      expect(afterRotation.binding).toMatchObject({ version: 1, current_credential_version: 8 });

      await pool.query(
        `
          UPDATE agent_observability_platform_policy
          SET max_capture_mode = 'metadata_only', capture_restriction_epoch = 102
          WHERE id = 'default'
        `,
      );
      const clamped = await loadAgentObservabilitySessionContext({
        db,
        workspaceId: inheritedWorkspaceId,
        sessionId: contextSessionId,
      });
      expect(clamped).toMatchObject({
        status: 'enabled',
        reason: null,
        capture: {
          pinned_mode: 'redacted_io',
          effective_mode: 'metadata_only',
          current_ceilings: { platform: 'metadata_only' },
        },
      });

      await pool.query(
        `
          UPDATE agent_observability_platform_policy
          SET max_capture_mode = 'redacted_io', capture_restriction_epoch = 103
          WHERE id = 'default'
        `,
      );
      const expanded = await loadAgentObservabilitySessionContext({
        db,
        workspaceId: inheritedWorkspaceId,
        sessionId: contextSessionId,
      });
      expect(expanded).toMatchObject({
        status: 'enabled',
        reason: null,
        capture: {
          pinned_mode: 'redacted_io',
          effective_mode: 'metadata_only',
          current_ceilings: { platform: 'redacted_io' },
        },
        epochs: {
          pinned: { platform_capture_restriction_epoch: 101 },
          current: { platform_capture_restriction_epoch: 103 },
        },
      });

      const freshNow = new Date();
      await db.insert(schema.agents).values({
        id: freshAgentId,
        workspaceId: inheritedWorkspaceId,
        name: 'fresh context capture fixture',
        modelProvider: 'anthropic',
        modelId: 'claude-3-5-sonnet-20240620',
      });
      await db.insert(schema.agentVersions).values({
        id: `agtv_observability_context_fresh_${suffix}`,
        workspaceId: inheritedWorkspaceId,
        agentId: freshAgentId,
        version: 1,
        snapshot: {
          id: freshAgentId,
          name: 'fresh context capture fixture',
          version: 1,
          model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
          system: null,
          tools: [],
          mcp_servers: [],
          skills: [],
          metadata: {},
          multiagent: null,
        },
      });
      await db.insert(schema.environments).values({
        id: freshEnvironmentId,
        workspaceId: inheritedWorkspaceId,
        name: 'fresh context capture environment',
        target: 'cloud',
      });
      await expect(
        db.transaction((tx) =>
          createSessionInTransaction(tx, {
            id: freshSessionId,
            workspaceId: inheritedWorkspaceId,
            agentId: freshAgentId,
            agentVersion: 1,
            environmentId: freshEnvironmentId,
            title: null,
            metadata: {},
            vaultIds: [],
            tools: null,
            mcpServers: null,
            agentOverrides: null,
            initialEvents: [],
            now: freshNow,
          }),
        ),
      ).resolves.toEqual({ ok: true, initialEventsOutboxId: null });
      await expect(
        loadAgentObservabilitySessionContext({
          db,
          workspaceId: inheritedWorkspaceId,
          sessionId: freshSessionId,
        }),
      ).resolves.toMatchObject({
        status: 'enabled',
        binding: { version: 2, config: { capture_mode: 'redacted_io' } },
        capture: { pinned_mode: 'redacted_io', effective_mode: 'redacted_io' },
        epochs: {
          pinned: { platform_capture_restriction_epoch: 103 },
          current: { platform_capture_restriction_epoch: 103 },
        },
      });

      await pool.query(
        `UPDATE agent_observability_platform_policy SET capture_restriction_epoch = 100 WHERE id = 'default'`,
      );
      await expect(
        loadAgentObservabilitySessionContext({
          db,
          workspaceId: inheritedWorkspaceId,
          sessionId: contextSessionId,
        }),
      ).rejects.toMatchObject({
        name: 'AgentObservabilitySessionContextUnavailableError',
        reason: 'regressed_epoch',
      } satisfies Partial<AgentObservabilitySessionContextUnavailableError>);
      const regressed = await app.inject({
        method: 'POST',
        url:
          `/internal/v1/workspaces/${inheritedWorkspaceId}/sessions/${contextSessionId}` +
          '/agent-observability/context/resolve',
        payload: {},
      });
      expect(regressed.statusCode).toBe(503);
      expect(regressed.json()).toEqual({ error: 'agent observability unavailable' });
      await pool.query(
        `UPDATE agent_observability_platform_policy SET capture_restriction_epoch = 103 WHERE id = 'default'`,
      );
    } finally {
      await pool.query(`DELETE FROM session_threads WHERE session_id = $1`, [freshSessionId]);
      await pool.query(`DELETE FROM sessions WHERE id = $1`, [freshSessionId]);
      await pool.query(
        `DELETE FROM session_observability_bindings WHERE workspace_id = $1 AND session_id = $2`,
        [inheritedWorkspaceId, freshSessionId],
      );
      await pool.query(`DELETE FROM agent_versions WHERE agent_id = $1`, [freshAgentId]);
      await pool.query(`DELETE FROM agents WHERE id = $1`, [freshAgentId]);
      await pool.query(`DELETE FROM environments WHERE id = $1`, [freshEnvironmentId]);
      await pool.query(
        `UPDATE agent_observability_bindings SET current_version = 1 WHERE id = $1`,
        [defaultBindingId],
      );
      await pool.query(
        `UPDATE agent_observability_bindings SET status = 'active', archived_at = NULL WHERE id = $1`,
        [defaultBindingId],
      );
      await pool.query(
        `
          UPDATE agent_observability_binding_credentials
          SET credential_version = 7
          WHERE binding_id = $1
        `,
        [defaultBindingId],
      );
      await pool.query(
        `DELETE FROM agent_observability_binding_versions WHERE binding_id = $1 AND version = 2`,
        [defaultBindingId],
      );
      await pool.query(
        `
          UPDATE agent_observability_platform_policy
          SET max_capture_mode = $1, capture_restriction_epoch = $2
          WHERE id = 'default'
        `,
        [originalPlatform.maxCaptureMode, originalPlatform.captureRestrictionEpoch],
      );
    }
  });

  it('does not let current selection pointers or modes revoke immutable active pins', async () => {
    try {
      await pool.query(
        `
          UPDATE agent_observability_organization_settings
          SET active_default_binding_id = NULL,
              active_default_binding_scope = NULL,
              selection_epoch = 302
          WHERE organization_id = $1
        `,
        [organizationId],
      );
      await setWorkspaceMode(pool, customWorkspaceId, organizationId, 'disabled', null, {
        selectionEpoch: 306,
        revocationEpoch: 107,
        captureCeiling: 'redacted_io',
        captureRestrictionEpoch: 108,
      });

      const [inherited, custom] = await Promise.all([
        loadAgentObservabilitySessionContext({
          db,
          workspaceId: inheritedWorkspaceId,
          sessionId: contextSessionId,
        }),
        loadAgentObservabilitySessionContext({
          db,
          workspaceId: customWorkspaceId,
          sessionId: contextCustomSessionId,
        }),
      ]);
      expect(inherited).toMatchObject({
        status: 'enabled',
        selection_source: 'organization_default',
        binding: { id: defaultBindingId, version: 1 },
        epochs: { current: { organization_selection_epoch: 302 } },
      });
      expect(custom).toMatchObject({
        status: 'enabled',
        selection_source: 'workspace_custom',
        binding: { id: customBindingId, version: 1 },
        epochs: { current: { workspace_selection_epoch: 306 } },
      });
    } finally {
      await pool.query(
        `
          UPDATE agent_observability_organization_settings
          SET active_default_binding_id = $1,
              active_default_binding_scope = 'organization',
              selection_epoch = 102
          WHERE organization_id = $2
        `,
        [defaultBindingId, organizationId],
      );
      await setWorkspaceMode(pool, customWorkspaceId, organizationId, 'custom', customBindingId, {
        selectionEpoch: 106,
        revocationEpoch: 107,
        captureCeiling: 'redacted_io',
        captureRestrictionEpoch: 108,
      });
    }
  });

  it('applies current revocation and binding lifecycle rules at fixed precedence', async () => {
    const inheritedContext = () =>
      loadAgentObservabilitySessionContext({
        db,
        workspaceId: inheritedWorkspaceId,
        sessionId: contextSessionId,
      });

    try {
      await pool.query(
        `
          UPDATE agent_observability_organization_settings
          SET default_revocation_epoch = 104
          WHERE organization_id = $1
        `,
        [organizationId],
      );
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'organization_default_revoked',
      });
      await expect(
        loadAgentObservabilitySessionContext({
          db,
          workspaceId: customWorkspaceId,
          sessionId: contextCustomSessionId,
        }),
      ).resolves.toMatchObject({ status: 'enabled', reason: null });
    } finally {
      await pool.query(
        `
          UPDATE agent_observability_organization_settings
          SET default_revocation_epoch = 103
          WHERE organization_id = $1
        `,
        [organizationId],
      );
    }

    try {
      await pool.query(
        `
          UPDATE agent_observability_organization_settings
          SET organization_revocation_epoch = 105
          WHERE organization_id = $1
        `,
        [organizationId],
      );
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'organization_revoked',
      });
    } finally {
      await pool.query(
        `
          UPDATE agent_observability_organization_settings
          SET organization_revocation_epoch = 104
          WHERE organization_id = $1
        `,
        [organizationId],
      );
    }

    try {
      await setWorkspaceMode(pool, inheritedWorkspaceId, organizationId, 'inherit', null, {
        revocationEpoch: 1,
        captureCeiling: 'redacted_io',
      });
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'workspace_revoked',
      });
    } finally {
      await setWorkspaceMode(pool, inheritedWorkspaceId, organizationId, 'inherit', null, {
        captureCeiling: 'redacted_io',
      });
    }

    try {
      await pool.query(
        `UPDATE agent_observability_bindings SET revocation_epoch = 110 WHERE id = $1`,
        [defaultBindingId],
      );
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'binding_revoked',
      });
    } finally {
      await pool.query(
        `UPDATE agent_observability_bindings SET revocation_epoch = 109 WHERE id = $1`,
        [defaultBindingId],
      );
    }

    try {
      await pool.query(
        `UPDATE agent_observability_bindings SET status = 'disabled' WHERE id = $1`,
        [defaultBindingId],
      );
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'binding_disabled',
      });

      await pool.query(
        `
          UPDATE agent_observability_bindings
          SET status = 'archived', archived_at = now()
          WHERE id = $1
        `,
        [defaultBindingId],
      );
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'binding_archived',
      });
    } finally {
      await pool.query(
        `
          UPDATE agent_observability_bindings
          SET status = 'active', archived_at = NULL
          WHERE id = $1
        `,
        [defaultBindingId],
      );
    }

    try {
      await pool.query(
        `
          UPDATE session_observability_bindings
          SET session_revocation_epoch = 1
          WHERE workspace_id = $1 AND session_id = $2
        `,
        [inheritedWorkspaceId, contextSessionId],
      );
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'session_revoked',
      });

      await pool.query(
        `UPDATE agent_observability_bindings SET status = 'disabled' WHERE id = $1`,
        [defaultBindingId],
      );
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'session_revoked',
      });
    } finally {
      await pool.query(
        `
          UPDATE session_observability_bindings
          SET session_revocation_epoch = 0
          WHERE workspace_id = $1 AND session_id = $2
        `,
        [inheritedWorkspaceId, contextSessionId],
      );
      await pool.query(`UPDATE agent_observability_bindings SET status = 'active' WHERE id = $1`, [
        defaultBindingId,
      ]);
    }
  });

  it('suppresses policy-disallowed, invalid, and credential-less pinned contexts without reading secrets', async () => {
    const inheritedContext = () =>
      loadAgentObservabilitySessionContext({
        db,
        workspaceId: inheritedWorkspaceId,
        sessionId: contextSessionId,
      });

    try {
      await pool.query(
        `UPDATE agent_observability_platform_policy SET allowed_adapters = ARRAY['langfuse_sdk']::text[]`,
      );
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'platform_adapter_disallowed',
      });
    } finally {
      await pool.query(
        `UPDATE agent_observability_platform_policy SET allowed_adapters = ARRAY['otlp_http']::text[]`,
      );
    }

    try {
      await pool.query(
        `UPDATE agent_observability_platform_policy SET allowed_endpoint_classes = ARRAY['private']::text[]`,
      );
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'platform_endpoint_class_disallowed',
      });
    } finally {
      await pool.query(
        `UPDATE agent_observability_platform_policy SET allowed_endpoint_classes = ARRAY['public']::text[]`,
      );
    }

    try {
      await pool.query(
        `
          UPDATE agent_observability_bindings
          SET external_project_id = NULL
          WHERE id = $1
        `,
        [defaultBindingId],
      );
      await expect(inheritedContext()).resolves.toMatchObject({
        status: 'suppressed',
        reason: 'binding_configuration_invalid',
      });
    } finally {
      await pool.query(
        `
          UPDATE agent_observability_bindings
          SET external_project_id = 'pk-session-selection'
          WHERE id = $1
        `,
        [defaultBindingId],
      );
    }

    try {
      await pool.query(
        `DELETE FROM agent_observability_binding_credentials WHERE binding_id = $1`,
        [defaultBindingId],
      );
      const context = await inheritedContext();
      expect(context).toMatchObject({
        status: 'suppressed',
        reason: 'credential_not_configured',
        binding: { id: defaultBindingId, current_credential_version: null },
      });
      expect(JSON.stringify(context)).not.toContain(secretRefSentinel);
      expect(JSON.stringify(context)).not.toContain('secret_ref');
    } finally {
      await pool.query(
        `
          INSERT INTO agent_observability_binding_credentials (
            binding_id, secret_ref, credential_version, key_hint, rotated_at, updated_by
          ) VALUES ($1, $2, 7, '...sentinel', now(), 'integration-test')
        `,
        [defaultBindingId, secretRefSentinel],
      );
    }
  });

  it('retains pinned custom, disabled, archive, and hard-delete tombstone contexts', async () => {
    const custom = await loadAgentObservabilitySessionContext({
      db,
      workspaceId: customWorkspaceId,
      sessionId: contextCustomSessionId,
    });
    expect(custom).toMatchObject({
      status: 'enabled',
      selection_source: 'workspace_custom',
      binding: { id: customBindingId, scope: 'workspace', workspace_id: customWorkspaceId },
    });

    const disabled = await loadAgentObservabilitySessionContext({
      db,
      workspaceId: disabledWorkspaceId,
      sessionId: contextDisabledSessionId,
    });
    expect(disabled).toMatchObject({
      status: 'disabled',
      reason: 'session_pin_disabled',
      binding: null,
      capture: { pinned_mode: 'metadata_only', effective_mode: 'metadata_only' },
    });

    // A later ceiling expansion cannot widen a Session's pinned capture mode.
    await setWorkspaceMode(pool, disabledWorkspaceId, organizationId, 'disabled', null, {
      captureCeiling: 'redacted_io',
    });
    const disabledAfterExpansion = await loadAgentObservabilitySessionContext({
      db,
      workspaceId: disabledWorkspaceId,
      sessionId: contextDisabledSessionId,
    });
    expect(disabledAfterExpansion).toMatchObject({
      status: 'disabled',
      capture: {
        pinned_mode: 'metadata_only',
        effective_mode: 'metadata_only',
        current_ceilings: { workspace: 'redacted_io' },
      },
    });
    await setWorkspaceMode(pool, disabledWorkspaceId, organizationId, 'disabled', null);

    const archived = await loadAgentObservabilitySessionContext({
      db,
      workspaceId: inheritedWorkspaceId,
      sessionId: contextArchivedSessionId,
    });
    expect(archived).toMatchObject({
      status: 'suppressed',
      reason: 'session_archived',
      binding: { id: defaultBindingId, version: 1 },
    });

    const deleted = await loadAgentObservabilitySessionContext({
      db,
      workspaceId: inheritedWorkspaceId,
      sessionId: contextDeletedSessionId,
    });
    expect(deleted).toMatchObject({
      status: 'suppressed',
      reason: 'session_deleted',
      binding: { id: defaultBindingId, version: 1 },
      epochs: { current: { session_revocation_epoch: 1 } },
    });
  });

  it('serves strict non-secret context responses on the combined internal listener', async () => {
    const path =
      `/internal/v1/workspaces/${inheritedWorkspaceId}/sessions/${contextSessionId}` +
      '/agent-observability/context/resolve';
    const response = await app.inject({ method: 'POST', url: path, payload: {} });
    expect(response.statusCode).toBe(200);
    expect(response.headers['cache-control']).toBe('private, no-store');
    expect(response.json()).toMatchObject({
      status: 'enabled',
      binding: { id: defaultBindingId, version: 1, current_credential_version: 7 },
    });
    expect(response.body).not.toContain(secretRefSentinel);
    expect(response.body).not.toContain('secret_ref');

    for (const [workspaceId, sessionId, status, reason] of [
      [disabledWorkspaceId, contextDisabledSessionId, 'disabled', 'session_pin_disabled'],
      [inheritedWorkspaceId, contextArchivedSessionId, 'suppressed', 'session_archived'],
      [inheritedWorkspaceId, contextDeletedSessionId, 'suppressed', 'session_deleted'],
    ] as const) {
      const tombstone = await app.inject({
        method: 'POST',
        url:
          `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}` +
          '/agent-observability/context/resolve',
        payload: {},
      });
      expect(tombstone.statusCode).toBe(200);
      expect(tombstone.headers['cache-control']).toBe('private, no-store');
      expect(tombstone.json()).toMatchObject({ status, reason });
    }

    const invalidBody = await app.inject({
      method: 'POST',
      url: path,
      payload: { binding_id: defaultBindingId },
    });
    expect(invalidBody.statusCode).toBe(400);
    expect(invalidBody.headers['cache-control']).toBe('private, no-store');
    expect(invalidBody.json()).toEqual({ error: 'invalid agent observability context request' });

    const missing = await app.inject({
      method: 'POST',
      url: path.replace(contextSessionId, `ses_context_missing_${suffix}`),
      payload: {},
    });
    expect(missing.statusCode).toBe(404);
    expect(missing.headers['cache-control']).toBe('private, no-store');
    expect(missing.json()).toEqual({ error: 'not found' });

    const crossWorkspace = await app.inject({
      method: 'POST',
      url: path.replace(inheritedWorkspaceId, otherWorkspaceId),
      payload: {},
    });
    expect(crossWorkspace.statusCode).toBe(404);
    expect(crossWorkspace.headers['cache-control']).toBe('private, no-store');
    expect(crossWorkspace.json()).toEqual({ error: 'not found' });
  });

  it('serves exporter-only context resolution over split listener sockets', async () => {
    const audience = 'orca-registry-observability-socket';
    const exporterSubject = 'system:serviceaccount:orca:observability-exporter';
    const harnessSubject = 'system:serviceaccount:orca:harness';
    const aiGatewaySubject = 'system:serviceaccount:orca:ai-gateway';
    const review = vi.fn(async (token: string, requestedAudience: string) => {
      const username =
        token === 'exporter-token'
          ? exporterSubject
          : token === 'harness-token'
            ? harnessSubject
            : token === 'ai-gateway-token'
              ? aiGatewaySubject
              : undefined;
      return {
        authenticated: requestedAudience === audience && username !== undefined,
        ...(username ? { username } : {}),
        audiences: requestedAudience === audience ? [audience] : [],
      };
    });
    const verifier = new KubernetesServiceAccountAuthVerifier(
      {
        audience,
        harnessSubject,
        aiGatewaySubject,
        observabilityExporterSubject: exporterSubject,
      },
      review,
    );
    const sharedOptions = {
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    };
    const internalApp = buildInternalApp(sharedOptions, verifier);
    const publicApp = buildPublicApp({ ...sharedOptions, skillStore: new InMemorySkillStore() });
    const adminApp = buildAdminApp({
      db,
      oidc: { allowedIssuers: [], audience: 'admin-test' },
      platformOidc: { allowedIssuers: [], audience: 'platform-test' },
      store: sharedOptions.store,
    });
    const path =
      `/internal/v1/workspaces/${inheritedWorkspaceId}/sessions/${contextSessionId}` +
      '/agent-observability/context/resolve';
    try {
      await Promise.all([
        internalApp.listen({ host: '127.0.0.1', port: 0 }),
        publicApp.listen({ host: '127.0.0.1', port: 0 }),
        adminApp.listen({ host: '127.0.0.1', port: 0 }),
      ]);
      const baseUrl = (listener: { server: { address(): string | AddressInfo | null } }) => {
        const address = listener.server.address();
        if (!address || typeof address === 'string')
          throw new Error('ephemeral listener has no TCP address');
        return `http://127.0.0.1:${address.port}`;
      };
      const request = (base: string, token?: string, body: unknown = {}) =>
        fetch(`${base}${path}`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            ...(token ? { authorization: `Bearer ${token}` } : {}),
          },
          body: JSON.stringify(body),
        });

      const exporter = await request(baseUrl(internalApp), 'exporter-token');
      expect(exporter.status).toBe(200);
      expect(exporter.headers.get('cache-control')).toBe('private, no-store');
      await expect(exporter.json()).resolves.toMatchObject({ status: 'enabled', reason: null });
      expect(review).toHaveBeenCalledWith('exporter-token', audience);

      for (const token of ['harness-token', 'ai-gateway-token']) {
        const rejected = await request(baseUrl(internalApp), token);
        expect(rejected.status, token).toBe(403);
        expect(rejected.headers.get('cache-control'), token).toBe('private, no-store');
      }

      const malformed = await request(baseUrl(internalApp), 'exporter-token', {
        selector: 'forbidden',
      });
      expect(malformed.status).toBe(400);
      expect(malformed.headers.get('cache-control')).toBe('private, no-store');
      await expect(malformed.json()).resolves.toEqual({
        error: 'invalid agent observability context request',
      });

      for (const listener of [publicApp, adminApp]) {
        const response = await request(baseUrl(listener));
        expect(response.status).toBe(404);
        expect(response.headers.get('cache-control')).toBe('private, no-store');
      }
    } finally {
      await Promise.all([internalApp.close(), publicApp.close(), adminApp.close()]);
    }
  });

  it('fails closed with opaque 503s when required current authority rows are absent', async () => {
    const path =
      `/internal/v1/workspaces/${inheritedWorkspaceId}/sessions/${contextSessionId}` +
      '/agent-observability/context/resolve';
    const expectUnavailable = async () => {
      const response = await app.inject({ method: 'POST', url: path, payload: {} });
      expect(response.statusCode).toBe(503);
      expect(response.headers['cache-control']).toBe('private, no-store');
      expect(response.json()).toEqual({ error: 'agent observability unavailable' });
      expect(response.body).not.toContain(secretRefSentinel);
      expect(response.body).not.toContain('secret_ref');
    };

    try {
      await pool.query(`DELETE FROM agent_observability_platform_policy WHERE id = 'default'`);
      await expectUnavailable();
    } finally {
      await pool.query(
        `
          INSERT INTO agent_observability_platform_policy (
            id, allowed_adapters, allowed_endpoint_classes, max_capture_mode, capture_restriction_epoch
          ) VALUES ('default', ARRAY['otlp_http']::text[], ARRAY['public']::text[], 'redacted_io', 101)
        `,
      );
    }

    try {
      await pool.query(
        `DELETE FROM agent_observability_organization_settings WHERE organization_id = $1`,
        [organizationId],
      );
      await expectUnavailable();
    } finally {
      await pool.query(
        `
          INSERT INTO agent_observability_organization_settings (
            organization_id, active_default_binding_id, active_default_binding_scope,
            selection_epoch, default_revocation_epoch, organization_revocation_epoch,
            capture_ceiling, capture_restriction_epoch
          ) VALUES ($1, $2, 'organization', 102, 103, 104, 'redacted_io', 105)
        `,
        [organizationId, defaultBindingId],
      );
    }

    try {
      await pool.query(
        `
          DELETE FROM agent_observability_workspace_settings
          WHERE organization_id = $1 AND workspace_id = $2
        `,
        [organizationId, inheritedWorkspaceId],
      );
      await expectUnavailable();
    } finally {
      await pool.query(
        `
          INSERT INTO agent_observability_workspace_settings (
            organization_id, workspace_id, mode, binding_id, selection_epoch, revocation_epoch,
            capture_ceiling, capture_restriction_epoch
          ) VALUES ($1, $2, 'inherit', NULL, 0, 0, 'redacted_io', 0)
        `,
        [organizationId, inheritedWorkspaceId],
      );
    }
  });

  it('treats a current binding version behind an immutable pin as opaque corruption', async () => {
    const sessionId = `ses_observability_context_regressed_${suffix}`;
    const path =
      `/internal/v1/workspaces/${inheritedWorkspaceId}/sessions/${sessionId}` +
      '/agent-observability/context/resolve';
    try {
      await pool.query(
        `
          INSERT INTO agent_observability_binding_versions (
            binding_id, version, adapter_type, semantic_profile, protocol, compression, timeout_ms,
            environment, release, capture_mode, sample_rate, config_schema_version, created_by
          ) VALUES ($1, 2, 'otlp_http', 'langfuse', 'http/protobuf', 'none', 5000,
                    'integration', 'v2-regressed-head', 'metadata_only', 1, 1, 'integration-test')
        `,
        [defaultBindingId],
      );
      await pool.query(
        `UPDATE agent_observability_bindings SET current_version = 2 WHERE id = $1`,
        [defaultBindingId],
      );
      await db.insert(sessionObservabilityBindings).values({
        workspaceId: inheritedWorkspaceId,
        sessionId,
        organizationId,
        bindingId: defaultBindingId,
        bindingVersion: 2,
        bindingScope: 'organization',
        bindingWorkspaceId: null,
        selectionSource: 'organization_default',
        status: 'active',
        organizationSelectionEpoch: 102,
        workspaceSelectionEpoch: 0,
        organizationDefaultRevocationEpoch: 103,
        organizationRevocationEpoch: 104,
        workspaceRevocationEpoch: 0,
        bindingRevocationEpoch: 109,
        platformCaptureRestrictionEpoch: 101,
        organizationCaptureRestrictionEpoch: 105,
        workspaceCaptureRestrictionEpoch: 0,
        effectiveCaptureMode: 'metadata_only',
        sessionRevocationEpoch: 0,
        agentId: `agt_observability_context_regressed_${suffix}`,
        agentVersion: 1,
        harness: null,
        harnessMode: null,
      });
      await pool.query(
        `UPDATE agent_observability_bindings SET current_version = 1 WHERE id = $1`,
        [defaultBindingId],
      );

      await expect(
        loadAgentObservabilitySessionContext({ db, workspaceId: inheritedWorkspaceId, sessionId }),
      ).rejects.toMatchObject({
        name: 'AgentObservabilitySessionContextUnavailableError',
        reason: 'regressed_binding_version',
      } satisfies Partial<AgentObservabilitySessionContextUnavailableError>);
      const response = await app.inject({ method: 'POST', url: path, payload: {} });
      expect(response.statusCode).toBe(503);
      expect(response.headers['cache-control']).toBe('private, no-store');
      expect(response.json()).toEqual({ error: 'agent observability unavailable' });
      expect(response.body).not.toContain(secretRefSentinel);
    } finally {
      await pool.query(
        `DELETE FROM session_observability_bindings WHERE workspace_id = $1 AND session_id = $2`,
        [inheritedWorkspaceId, sessionId],
      );
      await pool.query(
        `UPDATE agent_observability_bindings SET current_version = 1 WHERE id = $1`,
        [defaultBindingId],
      );
      await pool.query(
        `DELETE FROM agent_observability_binding_versions WHERE binding_id = $1 AND version = 2`,
        [defaultBindingId],
      );
    }
  });

  it('returns ready-to-insert disabled selections for explicit disable and does not retain a target', async () => {
    const selection = await select(disabledWorkspaceId);

    expect(selection).toEqual(
      expect.objectContaining({
        status: 'disabled',
        selectionSource: 'disabled',
        disabledReason: 'workspace_disabled',
        bindingId: null,
        bindingVersion: null,
        bindingScope: null,
        bindingWorkspaceId: null,
        bindingRevocationEpoch: 0,
        effectiveCaptureMode: 'metadata_only',
      }),
    );
  });

  it('fails closed when a required setting or current binding version is absent', async () => {
    await expect(
      select(`ws_observability_selection_missing_parent_${suffix}`),
    ).rejects.toMatchObject({
      name: 'SessionObservabilitySelectionAvailabilityError',
      reason: 'missing_workspace_parent',
    } satisfies Partial<SessionObservabilitySelectionAvailabilityError>);

    await pool.query(
      `
        DELETE FROM agent_observability_workspace_settings
        WHERE organization_id = $1 AND workspace_id = $2
      `,
      [organizationId, missingWorkspaceSettingId],
    );
    await expect(select(missingWorkspaceSettingId)).rejects.toMatchObject({
      name: 'SessionObservabilitySelectionAvailabilityError',
      reason: 'missing_workspace_setting',
    } satisfies Partial<SessionObservabilitySelectionAvailabilityError>);

    await expect(
      db.transaction(async (tx) => {
        await tx
          .update(agentObservabilityBindings)
          .set({ currentVersion: 99 })
          .where(eq(agentObservabilityBindings.id, versionBindingId));
        await expect(
          selectSessionObservabilityBindingInTransaction(tx, versionWorkspaceId),
        ).rejects.toMatchObject({
          name: 'SessionObservabilitySelectionAvailabilityError',
          reason: 'missing_selected_binding_version',
        } satisfies Partial<SessionObservabilitySelectionAvailabilityError>);
        tx.rollback();
      }),
    ).rejects.toBeInstanceOf(TransactionRollbackError);
  });

  it('treats inactive organization and workspace parents as resource-unavailable', async () => {
    await pool.query(
      `UPDATE workspaces SET status = 'archived', archived_at = now(), updated_at = now() WHERE id = $1`,
      [inactiveWorkspaceId],
    );
    await pool.query(`UPDATE organizations SET status = 'archived' WHERE id = $1`, [
      inactiveOrganizationId,
    ]);

    await expect(select(inactiveWorkspaceId)).rejects.toMatchObject({
      name: 'SessionObservabilitySelectionResourceUnavailableError',
      resource: 'workspace',
    } satisfies Partial<SessionObservabilitySelectionResourceUnavailableError>);
    await expect(select(inactiveOrganizationWorkspaceId)).rejects.toMatchObject({
      name: 'SessionObservabilitySelectionResourceUnavailableError',
      resource: 'organization',
    } satisfies Partial<SessionObservabilitySelectionResourceUnavailableError>);
  });

  it('queries only credential-head existence and never returns secret_ref', async () => {
    const client = await pool.connect();
    const originalQuery = client.query;
    const observedSql: string[] = [];
    client.query = ((...args: unknown[]) => {
      const query = args[0];
      const text =
        typeof query === 'string'
          ? query
          : query && typeof query === 'object' && 'text' in query && typeof query.text === 'string'
            ? query.text
            : null;
      if (text) observedSql.push(text);
      return Reflect.apply(originalQuery, client, args);
    }) as unknown as typeof client.query;
    try {
      const clientDb = drizzle(client, { schema });
      const selection = await clientDb.transaction(async (tx) =>
        selectSessionObservabilityBindingInTransaction(tx, inheritedWorkspaceId),
      );
      const credentialQueries = observedSql.filter((query) =>
        query.includes('agent_observability_binding_credentials'),
      );

      expect(credentialQueries).toHaveLength(1);
      expect(credentialQueries[0]).not.toContain('secret_ref');
      expect(lockedSelectionTableOrder(observedSql)).toEqual([
        'agent_observability_platform_policy',
        'organizations',
        'agent_observability_organization_settings',
        'workspaces',
        'agent_observability_workspace_settings',
        'agent_observability_bindings',
        'agent_observability_binding_versions',
        'agent_observability_binding_credentials',
      ]);
      expect(JSON.stringify(selection)).not.toContain(secretRefSentinel);
      expect(JSON.stringify(selection)).not.toContain('collector.example');
      expect(selection).not.toHaveProperty('secretRef');
      expect(selection).not.toHaveProperty('credentialVersion');
      expect(selection).not.toHaveProperty('endpoint');
    } finally {
      client.query = originalQuery;
      client.release();
    }
  });

  it('blocks behind every ordered authority row then reads committed state', async () => {
    try {
      await expectSelectionToBlockBehindWriter({
        sql: `
          UPDATE agent_observability_platform_policy
          SET max_capture_mode = 'metadata_only', capture_restriction_epoch = 801
          WHERE id = 'default'
        `,
        verify: (selection) => {
          expect(selection).toMatchObject({
            status: 'active',
            platformCaptureRestrictionEpoch: 801,
            effectiveCaptureMode: 'metadata_only',
          });
        },
      });
      await pool.query(
        `
          UPDATE agent_observability_platform_policy
          SET max_capture_mode = 'redacted_io', capture_restriction_epoch = 802
          WHERE id = 'default'
        `,
      );

      await expectSelectionToBlockBehindWriter({
        sql: `
          UPDATE organizations
          SET updated_at = now()
          WHERE id = $1
        `,
        values: [organizationId],
        verify: (selection) => {
          expect(selection).toMatchObject({
            status: 'active',
            selectionSource: 'workspace_custom',
            bindingId: lockBindingId,
          });
        },
      });

      await expectSelectionToBlockBehindWriter({
        sql: `
          UPDATE agent_observability_organization_settings
          SET selection_epoch = 803
          WHERE organization_id = $1
        `,
        values: [organizationId],
        verify: (selection) => {
          expect(selection).toMatchObject({
            status: 'active',
            organizationSelectionEpoch: 803,
          });
        },
      });

      await expectSelectionToBlockBehindWriter({
        sql: `
          UPDATE workspaces
          SET updated_at = now()
          WHERE id = $1
        `,
        values: [lockWorkspaceId],
        verify: (selection) => {
          expect(selection).toMatchObject({
            status: 'active',
            bindingId: lockBindingId,
          });
        },
      });

      await expectSelectionToBlockBehindWriter({
        sql: `
          UPDATE agent_observability_workspace_settings
          SET selection_epoch = 804
          WHERE organization_id = $1 AND workspace_id = $2
        `,
        values: [organizationId, lockWorkspaceId],
        verify: (selection) => {
          expect(selection).toMatchObject({
            status: 'active',
            workspaceSelectionEpoch: 804,
          });
        },
      });

      await expectSelectionToBlockBehindWriter({
        sql: `
          UPDATE agent_observability_bindings
          SET revocation_epoch = 805
          WHERE id = $1
        `,
        values: [lockBindingId],
        verify: (selection) => {
          expect(selection).toMatchObject({
            status: 'active',
            bindingId: lockBindingId,
            bindingRevocationEpoch: 805,
          });
        },
      });

      await expectSelectionToBlockBehindWriter({
        sql: `
          UPDATE agent_observability_binding_versions
          SET release = 'lock-version'
          WHERE binding_id = $1 AND version = 1
        `,
        values: [lockBindingId],
        verify: (selection) => {
          expect(selection).toMatchObject({
            status: 'active',
            selectionSource: 'workspace_custom',
            bindingId: lockBindingId,
          });
        },
      });

      await expectSelectionToBlockBehindWriter({
        sql: `
          UPDATE agent_observability_binding_credentials
          SET key_hint = '...lock'
          WHERE binding_id = $1
        `,
        values: [lockBindingId],
        verify: (selection) => {
          expect(selection).toMatchObject({
            status: 'active',
            selectionSource: 'workspace_custom',
            bindingId: lockBindingId,
          });
        },
      });
    } finally {
      await pool.query(
        `
          UPDATE agent_observability_platform_policy
          SET max_capture_mode = 'redacted_io', capture_restriction_epoch = 101
          WHERE id = 'default'
        `,
      );
      await pool.query(
        `
          UPDATE agent_observability_organization_settings
          SET selection_epoch = 102
          WHERE organization_id = $1
        `,
        [organizationId],
      );
      await setWorkspaceMode(pool, lockWorkspaceId, organizationId, 'custom', lockBindingId, {
        captureCeiling: 'redacted_io',
      });
      await pool.query(
        `
          UPDATE agent_observability_bindings
          SET revocation_epoch = 0
          WHERE id = $1
        `,
        [lockBindingId],
      );
      await pool.query(
        `
          UPDATE agent_observability_binding_versions
          SET release = 'v1'
          WHERE binding_id = $1 AND version = 1
        `,
        [lockBindingId],
      );
      await pool.query(
        `
          UPDATE agent_observability_binding_credentials
          SET key_hint = '...sentinel'
          WHERE binding_id = $1
        `,
        [lockBindingId],
      );
    }
  });

  it('holds a coherent repeatable-read context snapshot while lifecycle mutation waits on its locks', async () => {
    const reader = await pool.connect();
    const writer = await pool.connect();
    const originalQuery = reader.query;
    const observedSql: string[] = [];
    reader.query = ((...args: unknown[]) => {
      const query = args[0];
      const text =
        typeof query === 'string'
          ? query
          : query && typeof query === 'object' && 'text' in query && typeof query.text === 'string'
            ? query.text
            : null;
      if (text) observedSql.push(text);
      return Reflect.apply(originalQuery, reader, args);
    }) as unknown as typeof reader.query;
    const snapshotReady = deferred<void>();
    const releaseSnapshot = deferred<void>();
    let contextPromise:
      | Promise<Awaited<ReturnType<typeof loadAgentObservabilitySessionContext>>>
      | undefined;
    let writerUpdate: Promise<unknown> | undefined;
    try {
      const readerDb = drizzle(reader, { schema });
      contextPromise = readerDb.transaction(
        async (tx) => {
          const context = await loadAgentObservabilitySessionContextInTransaction({
            tx: tx as DbTransaction,
            workspaceId: inheritedWorkspaceId,
            sessionId: contextSessionId,
          });
          snapshotReady.resolve();
          await releaseSnapshot.promise;
          return context;
        },
        { isolationLevel: 'repeatable read' },
      );
      await snapshotReady.promise;
      expect(lockedSelectionTableOrder(observedSql)).toEqual([
        'agent_observability_platform_policy',
        'organizations',
        'agent_observability_organization_settings',
        'workspaces',
        'agent_observability_workspace_settings',
        'agent_observability_bindings',
        'agent_observability_binding_versions',
        'agent_observability_binding_credentials',
        'session_observability_bindings',
      ]);
      expect(
        observedSql.filter((query) => query.includes('agent_observability_binding_credentials')),
      ).toHaveLength(1);
      expect(
        observedSql.find((query) => query.includes('agent_observability_binding_credentials')),
      ).not.toContain('secret_ref');

      const writerPid = (await writer.query<{ pid: number }>('SELECT pg_backend_pid() AS pid'))
        .rows[0]!.pid;
      writerUpdate = writer.query(
        `UPDATE agent_observability_bindings SET status = 'disabled' WHERE id = $1`,
        [defaultBindingId],
      );
      await waitForBackendLock(adminPool!, writerPid);

      releaseSnapshot.resolve();
      const before = await settlesWithin(contextPromise, 3000);
      await settlesWithin(writerUpdate, 3000);
      expect(before).toMatchObject({
        status: 'enabled',
        reason: null,
        binding: { id: defaultBindingId, version: 1, lifecycle_status: 'active' },
      });
      expect(JSON.stringify(before)).not.toContain(secretRefSentinel);

      const after = await loadAgentObservabilitySessionContext({
        db,
        workspaceId: inheritedWorkspaceId,
        sessionId: contextSessionId,
      });
      expect(after).toMatchObject({
        status: 'suppressed',
        reason: 'binding_disabled',
        binding: { id: defaultBindingId, version: 1, lifecycle_status: 'disabled' },
      });
    } finally {
      releaseSnapshot.resolve();
      await contextPromise?.catch(() => {});
      await writerUpdate?.catch(() => {});
      await pool.query(
        `UPDATE agent_observability_bindings SET status = 'active', archived_at = NULL WHERE id = $1`,
        [defaultBindingId],
      );
      reader.query = originalQuery;
      reader.release();
      writer.release();
    }
  });

  it('returns opaque 503s for missing pinned versions and bindings in isolated corruption fixtures', async () => {
    const resolve = async (sessionId: string) => {
      const response = await app.inject({
        method: 'POST',
        url:
          `/internal/v1/workspaces/${inheritedWorkspaceId}/sessions/${sessionId}` +
          '/agent-observability/context/resolve',
        payload: {},
      });
      expect(response.statusCode).toBe(503);
      expect(response.headers['cache-control']).toBe('private, no-store');
      expect(response.json()).toEqual({ error: 'agent observability unavailable' });
      expect(response.body).not.toContain(secretRefSentinel);
      expect(response.body).not.toContain('secret_ref');
    };

    // The normal schema prohibits both corruptions. This is the final test in
    // a fresh disposable database, so remove only those FKs to verify the
    // resolver's defensive 503 behavior against physically absent authority.
    await pool.query(
      `
        ALTER TABLE session_observability_bindings
        DROP CONSTRAINT session_observability_bindings_binding_scope_fk,
        DROP CONSTRAINT session_observability_bindings_binding_workspace_fk,
        DROP CONSTRAINT session_observability_bindings_binding_version_fk
      `,
    );
    await pool.query(
      `
        ALTER TABLE agent_observability_bindings
        DROP CONSTRAINT agent_observability_bindings_current_version_fk
      `,
    );

    await pool.query(
      `
        DELETE FROM agent_observability_binding_versions
        WHERE binding_id = $1 AND version = 1
      `,
      [missingVersionBindingId],
    );
    await resolve(contextMissingVersionSessionId);

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `DELETE FROM agent_observability_binding_credentials WHERE binding_id = $1`,
        [missingBindingId],
      );
      await client.query(`DELETE FROM agent_observability_binding_versions WHERE binding_id = $1`, [
        missingBindingId,
      ]);
      await client.query(`DELETE FROM agent_observability_bindings WHERE id = $1`, [
        missingBindingId,
      ]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
    await resolve(contextMissingBindingSessionId);
  });

  async function select(workspaceId: string): Promise<SessionObservabilitySelection> {
    return db.transaction(async (tx) =>
      selectSessionObservabilityBindingInTransaction(tx, workspaceId),
    );
  }

  async function expectSelectionToBlockBehindWriter(input: {
    sql: string;
    values?: unknown[];
    verify: (selection: SessionObservabilitySelection) => void;
  }): Promise<void> {
    const writer = await pool.connect();
    const selector = await pool.connect();
    let writerOpen = false;
    let selectionPromise: Promise<SessionObservabilitySelection> | undefined;
    try {
      await writer.query('BEGIN');
      writerOpen = true;
      await writer.query(input.sql, input.values);
      const selectorPid = (await selector.query<{ pid: number }>('SELECT pg_backend_pid() AS pid'))
        .rows[0]!.pid;
      const selectorDb = drizzle(selector, { schema });
      selectionPromise = selectorDb.transaction(async (tx) =>
        selectSessionObservabilityBindingInTransaction(tx, lockWorkspaceId),
      );

      await waitForBackendLock(adminPool!, selectorPid);
      await writer.query('COMMIT');
      writerOpen = false;
      input.verify(await settlesWithin(selectionPromise, 3000));
    } finally {
      if (writerOpen) await writer.query('ROLLBACK').catch(() => {});
      await selectionPromise?.catch(() => {});
      selector.release();
      writer.release();
    }
  }
});

async function createOrganization(db: DbClient, id: string, name: string): Promise<void> {
  await db.insert(organizations).values({ id, name, status: 'active' });
}

async function createWorkspace(
  db: DbClient,
  id: string,
  organizationId: string,
  name: string,
): Promise<void> {
  await db.insert(workspaces).values({
    id,
    organizationId,
    name,
    status: 'active',
    createdBy: 'integration-test',
  });
}

async function insertBinding(
  pool: Pool,
  input: {
    id: string;
    organizationId: string;
    workspaceId: string | null;
    secretRef?: string;
    revocationEpoch?: number;
  },
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `
        INSERT INTO agent_observability_bindings (
          id, organization_id, workspace_id, scope_type, adapter_type, endpoint_kind, endpoint_class,
          endpoint, external_project_id, current_version, status, revocation_epoch, created_by, updated_by
        ) VALUES ($1, $2, $3, $4, 'otlp_http', 'traces_endpoint', 'public',
                  'https://collector.example/v1/traces', 'pk-session-selection', 1, 'active', $5,
                  'integration-test', 'integration-test')
      `,
      [
        input.id,
        input.organizationId,
        input.workspaceId,
        input.workspaceId === null ? 'organization' : 'workspace',
        input.revocationEpoch ?? 0,
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
        ) VALUES ($1, $2, 7, '...sentinel', now(), 'integration-test')
      `,
      [input.id, input.secretRef ?? `secret-ref-${input.id}`],
    );
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function setWorkspaceMode(
  pool: Pool,
  workspaceId: string,
  organizationId: string,
  mode: 'inherit' | 'disabled' | 'custom',
  bindingId: string | null,
  epochs: Partial<{
    selectionEpoch: number;
    revocationEpoch: number;
    captureCeiling: 'metadata_only' | 'redacted_io';
    captureRestrictionEpoch: number;
  }> = {},
): Promise<void> {
  await pool.query(
    `
      UPDATE agent_observability_workspace_settings
      SET mode = $1,
          binding_id = $2,
          selection_epoch = $3,
          revocation_epoch = $4,
          capture_ceiling = $5,
          capture_restriction_epoch = $6
      WHERE organization_id = $7 AND workspace_id = $8
    `,
    [
      mode,
      bindingId,
      epochs.selectionEpoch ?? 0,
      epochs.revocationEpoch ?? 0,
      epochs.captureCeiling ?? 'metadata_only',
      epochs.captureRestrictionEpoch ?? 0,
      organizationId,
      workspaceId,
    ],
  );
}

async function waitForBackendLock(observer: Pool, pid: number): Promise<void> {
  const deadline = Date.now() + 2_000;
  const maxAttempts = 100;
  for (let attempt = 0; attempt < maxAttempts && Date.now() < deadline; attempt += 1) {
    const result = await observer.query<{ waiting: boolean | null }>(
      `SELECT wait_event_type = 'Lock' AS waiting FROM pg_stat_activity WHERE pid = $1`,
      [pid],
    );
    if (result.rows[0]?.waiting === true) return;

    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0 || attempt === maxAttempts - 1) break;
    await new Promise<void>((resolve) => setTimeout(resolve, Math.min(10, remainingMs)));
  }
  throw new Error(`selector backend ${pid} did not block on a row lock`);
}

function lockedSelectionTableOrder(queries: readonly string[]): string[] {
  return queries
    .filter((query) => /\bfor\s+share\b/iu.test(query))
    .map((query) => {
      const match = /\bfrom\s+(?:"([^"]+)"|([a-z_][a-z0-9_]*))/iu.exec(query);
      const table = match?.[1] ?? match?.[2];
      if (!table) throw new Error(`could not identify locked table from query: ${query}`);
      return table;
    });
}

async function settlesWithin<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error(`operation did not settle within ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function deferred<T>(): { promise: Promise<T>; resolve(value: T | PromiseLike<T>): void } {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}
