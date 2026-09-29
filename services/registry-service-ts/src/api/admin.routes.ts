// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, asc, desc, eq, gt, isNotNull, isNull, lt, lte, ne, or, sql } from 'drizzle-orm';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { SEED_PRICE_PROVIDER } from '@orca/harness-catalog';
import type { TranscriptStore } from '@orca/transcript-store';
import {
  fingerprintApiKey,
  generateApiKey,
  hashApiKey,
  partialApiKeyHint,
} from '../auth/api-key.js';
import { requireAdminScope } from '../auth/admin-auth.js';
import { executeOrganizationAgentObservabilityCaptureCeiling } from '../domain/agent-observability-organization-ceiling.js';
import {
  AgentObservabilityWorkspacePathParamsSchema,
  adminAgentObservabilityContract,
  OrganizationAgentObservabilityCaptureCeilingRequestSchema,
  WorkspaceAgentObservabilityPutHeadersSchema,
  OrganizationAgentObservabilityStateSchema,
  WorkspaceAgentObservabilityStateSchema,
} from '../contracts/agent-observability.contract.js';
import {
  newSessionArchiveOutboxRow,
  reconcileSessionLifecycleOutbox,
} from '../domain/session-lifecycle-outbox.js';
import { newWorkspaceObservabilitySettings } from '../domain/agent-observability-settings.js';
import {
  isAgentObservabilityCaptureMode,
  isWorkspaceObservabilityMode,
} from '../domain/agent-observability-policy.js';
import { agentObservabilityStateEtag } from '../domain/agent-observability-etag.js';
import {
  AgentObservabilityStateAvailabilityError,
  AgentObservabilityStateNotFoundError,
  loadOrganizationAgentObservabilityState,
  loadWorkspaceAgentObservabilityState,
} from '../domain/agent-observability-state.js';
import {
  OrganizationAgentObservabilityMutationRequestError,
  parseOrganizationAgentObservabilityDisableRequest,
  parseOrganizationAgentObservabilityCredentialRotationRequest,
  parseOrganizationAgentObservabilityIdempotencyKey,
  parseOrganizationAgentObservabilityIfMatch,
  parseOrganizationAgentObservabilityPutRequest,
} from '../domain/agent-observability-organization-mutation.js';
import {
  WorkspaceAgentObservabilityMutationRequestError,
  parseWorkspaceAgentObservabilityCredentialRotationRequest,
  parseWorkspaceAgentObservabilityIdempotencyKey,
  parseWorkspaceAgentObservabilityIfMatch,
  parseWorkspaceAgentObservabilityPutRequest,
} from '../domain/agent-observability-workspace-mutation.js';
import {
  executeOrganizationAgentObservabilityPut,
  type ExecuteOrganizationAgentObservabilityPutResult,
} from '../domain/agent-observability-organization-service.js';
import {
  executeWorkspaceAgentObservabilityPut,
  type ExecuteWorkspaceAgentObservabilityPutResult,
} from '../domain/agent-observability-workspace-service.js';
import {
  executeWorkspaceAgentObservabilityCredentialRotation,
  type ExecuteWorkspaceAgentObservabilityCredentialRotationResult,
} from '../domain/agent-observability-workspace-rotation-service.js';
import {
  executeOrganizationAgentObservabilityCredentialRotation,
  type ExecuteOrganizationAgentObservabilityCredentialRotationResult,
} from '../domain/agent-observability-organization-rotation-service.js';
import {
  executeOrganizationAgentObservabilityDisable,
  type ExecuteOrganizationAgentObservabilityDisableResult,
} from '../domain/agent-observability-organization-disable-service.js';
import { newId } from '../domain/versioning.js';
import type { DbClient } from '../persistence/postgres/client.js';
import { nameConflictMessage } from '../persistence/postgres/name-conflict.js';
import {
  adminAuditEvents,
  agentObservabilityWorkspaceArchiveRevocations,
  agentObservabilityWorkspaceSettings,
  agents,
  apiKeys,
  guardrails,
  organizations,
  sessions,
  sessionLifecycleOutbox,
  workspaces,
} from '../persistence/postgres/schema.js';
import type { SecretStore } from '../secrets/secret-provider.js';
import {
  GuardrailCreate,
  GuardrailUpdate,
  resolveGuardrailAuthoring,
  ruleToStorage,
} from '../contracts/guardrails.contract.js';
import {
  authoringMessage,
  guardrailToWire,
  storedPhases,
  storedRule,
  zodMessage,
} from './guardrails.routes.js';
import {
  applyMetadataPatch,
  parseMetadata,
  parseMetadataPatch,
  toJsonMetadata,
  validateMetadataLimits,
} from './metadata.js';
import {
  ModelPriceCreate,
  ModelPriceUpdate,
  ModelProviderId,
  modelPriceEntryToWire,
  modelPriceWriteFromCreate,
  modelPriceWriteFromUpdate,
  parseModelPriceSource,
} from '../contracts/model-prices.contract.js';
import {
  createPostgresModelPriceStore,
  type ModelPriceStore,
  type StoredModelPrice,
} from '../pricing/store.js';

const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 1000;
const WORKSPACE_KEY_SCOPES = ['workspace.full_access'];

class WorkspaceNotActiveError extends Error {}
class WorkspaceObservabilityArchiveInvariantError extends Error {}

interface PaginationQuery {
  limit?: string | number;
  after_id?: string;
  before_id?: string;
}

interface WorkspaceListQuery extends PaginationQuery {
  include_archived?: string | boolean;
}

interface ApiKeyListQuery extends PaginationQuery {
  workspace_id?: string;
  status?: string;
}

export function registerAdminRoutes(
  app: FastifyInstance,
  db: DbClient,
  store: TranscriptStore,
  secretStore?: SecretStore,
): void {
  app.get('/v1/organizations/me', async (req, reply) => {
    const principal = req.adminAuth!;
    const rows = await db
      .select()
      .from(organizations)
      .where(eq(organizations.id, principal.organizationId))
      .limit(1);
    const organization = rows[0];
    if (!organization) return reply.code(404).send({ error: 'organization not found' });
    return organizationWire(organization);
  });

  app.get('/v1/organizations/agent_observability', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'observability:read')) return;
    reply.header('cache-control', 'private, no-store');
    try {
      const state = await loadOrganizationAgentObservabilityState({
        db,
        organizationId: req.adminAuth!.organizationId,
      });
      const response = OrganizationAgentObservabilityStateSchema.safeParse(state.response);
      if (!response.success) {
        return reply.code(503).send({ error: 'agent observability state unavailable' });
      }
      reply.header('etag', agentObservabilityStateEtag(state.etagInput));
      return response.data;
    } catch (error) {
      if (error instanceof AgentObservabilityStateAvailabilityError) {
        return reply.code(503).send({ error: 'agent observability state unavailable' });
      }
      if (error instanceof AgentObservabilityStateNotFoundError) {
        return reply.code(404).send({ error: 'organization not found' });
      }
      throw error;
    }
  });

  app.put('/v1/organizations/agent_observability', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'observability:write')) return;
    return putOrganizationAgentObservabilityDefault(req, reply, db, secretStore);
  });

  app.put(
    adminAgentObservabilityContract.putOrganizationCaptureCeiling.path,
    async (req, reply) => {
      if (!requireAdminScope(req, reply, 'org:admin')) return;
      reply.header('cache-control', 'private, no-store');
      const body = OrganizationAgentObservabilityCaptureCeilingRequestSchema.safeParse(req.body);
      if (!body.success)
        return reply.code(400).send({ error: 'invalid agent observability request' });
      if (req.headers['if-match'] === undefined)
        return reply.code(428).send({ error: 'if-match is required' });
      const headers = WorkspaceAgentObservabilityPutHeadersSchema.safeParse(req.headers);
      if (!headers.success)
        return reply.code(400).send({ error: 'invalid agent observability request' });
      const principal = req.adminAuth!;
      const result = await executeOrganizationAgentObservabilityCaptureCeiling({
        db,
        organizationId: principal.organizationId,
        principal: principal.principal,
        authMethod: principal.authMethod,
        requestId: req.id,
        captureCeiling: body.data.capture_ceiling,
        ifMatch: headers.data['if-match'],
        idempotencyKey: headers.data['idempotency-key'],
      });
      return sendOrganizationAgentObservabilityMutationResult(reply, result);
    },
  );

  // find-my-way reserves `:` for parameter syntax; `::` registers one literal colon.
  app.post('/v1/organizations/agent_observability::disable', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'observability:write')) return;
    return disableOrganizationAgentObservabilityDefault(req, reply, db);
  });

  app.post('/v1/organizations/agent_observability::rotate_credentials', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'observability:rotate')) return;
    return rotateOrganizationAgentObservabilityCredentials(req, reply, db, secretStore);
  });

  app.get('/v1/organizations/workspaces/:workspaceId/agent_observability', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'observability:read')) return;
    reply.header('cache-control', 'private, no-store');
    const params = AgentObservabilityWorkspacePathParamsSchema.safeParse(req.params);
    if (!params.success) return reply.code(404).send({ error: 'workspace not found' });
    try {
      const state = await loadWorkspaceAgentObservabilityState({
        db,
        organizationId: req.adminAuth!.organizationId,
        workspaceId: params.data.workspaceId,
      });
      const response = WorkspaceAgentObservabilityStateSchema.safeParse(state.response);
      if (!response.success) {
        return reply.code(503).send({ error: 'agent observability state unavailable' });
      }
      reply.header('etag', agentObservabilityStateEtag(state.etagInput));
      return response.data;
    } catch (error) {
      if (error instanceof AgentObservabilityStateAvailabilityError) {
        return reply.code(503).send({ error: 'agent observability state unavailable' });
      }
      if (error instanceof AgentObservabilityStateNotFoundError) {
        return reply.code(404).send({ error: 'workspace not found' });
      }
      throw error;
    }
  });

  app.put('/v1/organizations/workspaces/:workspaceId/agent_observability', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'observability:write')) return;
    return putWorkspaceAgentObservability(req, reply, db, secretStore);
  });

  // find-my-way reserves `:` for parameter syntax; `::` registers one literal colon.
  app.post(
    '/v1/organizations/workspaces/:workspaceId/agent_observability::rotate_credentials',
    async (req, reply) => {
      if (!requireAdminScope(req, reply, 'observability:rotate')) return;
      return rotateWorkspaceAgentObservabilityCredentials(req, reply, db, secretStore);
    },
  );

  app.post('/v1/organizations/workspaces', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'workspaces:write')) return;
    const body = objectBody(req.body);
    const name = requiredName(body['name'], reply);
    if (!name) return;

    const principal = req.adminAuth!;
    const now = new Date();
    const workspace = {
      id: newId('wrkspc'),
      organizationId: principal.organizationId,
      name,
      status: 'active',
      createdBy: principal.principal,
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    };
    try {
      await db.transaction(async (tx) => {
        await tx.insert(workspaces).values(workspace);
        // Intentionally redundant with migration 0048's mixed-version trigger;
        // retained as the durable writer path after that trigger is removed.
        await tx
          .insert(agentObservabilityWorkspaceSettings)
          .values(newWorkspaceObservabilitySettings(principal.organizationId, workspace.id, now))
          .onConflictDoNothing();
        await writeAudit(tx, req, 'workspace.created', 'workspace', workspace.id, workspace.id, {
          name,
        });
      });
    } catch (error) {
      const message = nameConflictMessage(error);
      if (message) return reply.code(409).send({ error: message });
      throw error;
    }
    return reply.send(workspaceWire(workspace));
  });

  app.get('/v1/organizations/workspaces', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'workspaces:read')) return;
    const query = (req.query ?? {}) as WorkspaceListQuery;
    const limit = paginationLimit(query.limit, reply);
    if (limit === null) return;
    if (!validateCursorDirection(query, reply)) return;
    const includeArchived = optionalBoolean(query.include_archived, 'include_archived', reply);
    if (includeArchived === null) return;
    const principal = req.adminAuth!;
    const conditions = [eq(workspaces.organizationId, principal.organizationId)];
    if (!includeArchived) conditions.push(eq(workspaces.status, 'active'));
    const cursorId = query.after_id ?? query.before_id;
    if (cursorId) {
      const cursorRows = await db
        .select({ id: workspaces.id, createdAt: workspaces.createdAt })
        .from(workspaces)
        .where(
          and(eq(workspaces.id, cursorId), eq(workspaces.organizationId, principal.organizationId)),
        )
        .limit(1);
      const cursor = cursorRows[0];
      if (!cursor) return reply.code(400).send({ error: 'workspace cursor not found' });
      const createdAtComparison = query.after_id
        ? gt(workspaces.createdAt, cursor.createdAt)
        : lt(workspaces.createdAt, cursor.createdAt);
      const idComparison = query.after_id
        ? gt(workspaces.id, cursor.id)
        : lt(workspaces.id, cursor.id);
      conditions.push(
        or(createdAtComparison, and(eq(workspaces.createdAt, cursor.createdAt), idComparison))!,
      );
    }
    const backwards = query.before_id !== undefined;
    const rows = await db
      .select()
      .from(workspaces)
      .where(and(...conditions))
      .orderBy(
        backwards ? desc(workspaces.createdAt) : asc(workspaces.createdAt),
        backwards ? desc(workspaces.id) : asc(workspaces.id),
      )
      .limit(limit + 1);
    return page(rows, limit, workspaceWire, backwards);
  });

  app.get('/v1/organizations/workspaces/:workspaceId', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'workspaces:read')) return;
    const workspace = await findWorkspace(db, req);
    if (!workspace) return reply.code(404).send({ error: 'workspace not found' });
    return workspaceWire(workspace);
  });

  app.post('/v1/organizations/workspaces/:workspaceId', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'workspaces:write')) return;
    const current = await findWorkspace(db, req);
    if (!current) return reply.code(404).send({ error: 'workspace not found' });
    if (current.status === 'archived') {
      return reply.code(409).send({ error: 'archived workspaces cannot be updated' });
    }
    const body = objectBody(req.body);
    const name = requiredName(body['name'], reply);
    if (!name) return;
    try {
      const updated = await db.transaction(async (tx) => {
        const lockedRows = await tx
          .select()
          .from(workspaces)
          .where(
            and(
              eq(workspaces.id, current.id),
              eq(workspaces.organizationId, req.adminAuth!.organizationId),
            ),
          )
          .for('update')
          .limit(1);
        const locked = lockedRows[0];
        if (!locked || locked.status === 'archived') return null;
        const now = new Date();
        await tx
          .update(workspaces)
          .set({ name, updatedAt: now })
          .where(
            and(
              eq(workspaces.id, current.id),
              eq(workspaces.organizationId, req.adminAuth!.organizationId),
            ),
          );
        await writeAudit(tx, req, 'workspace.updated', 'workspace', current.id, current.id, {
          name,
        });
        return { ...locked, name, updatedAt: now };
      });
      if (!updated) return reply.code(409).send({ error: 'archived workspaces cannot be updated' });
      return workspaceWire(updated);
    } catch (error) {
      const message = nameConflictMessage(error);
      if (message) return reply.code(409).send({ error: message });
      throw error;
    }
  });

  app.post('/v1/organizations/workspaces/:workspaceId/archive', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'workspaces:write')) return;
    const current = await findWorkspace(db, req);
    if (!current) return reply.code(404).send({ error: 'workspace not found' });
    if (current.status === 'archived') return workspaceWire(current);

    const now = new Date();
    const archived = await db.transaction(async (tx) => {
      const lockedRows = await tx
        .select()
        .from(workspaces)
        .where(
          and(
            eq(workspaces.id, current.id),
            eq(workspaces.organizationId, req.adminAuth!.organizationId),
          ),
        )
        .for('update')
        .limit(1);
      const locked = lockedRows[0];
      if (!locked || locked.status === 'archived') {
        return {
          workspace: locked ?? current,
          lifecycleEventIds: [] as string[],
          changed: false,
        };
      }
      // Keep archive compatible with Session selection and workspace mutation:
      // Workspace authority, then its setting authority, archive marker, then
      // Session rows.
      const lockedWorkspaceSetting = (
        await tx
          .select()
          .from(agentObservabilityWorkspaceSettings)
          .where(
            and(
              eq(agentObservabilityWorkspaceSettings.organizationId, locked.organizationId),
              eq(agentObservabilityWorkspaceSettings.workspaceId, locked.id),
            ),
          )
          .for('update')
          .limit(1)
      )[0];
      if (!lockedWorkspaceSetting) throw new WorkspaceObservabilityArchiveInvariantError();
      const nextWorkspaceRevocationEpoch = nextWorkspaceArchiveRevocationEpoch(
        lockedWorkspaceSetting,
        locked.organizationId,
        locked.id,
      );
      const existingArchiveMarker = (
        await tx
          .select()
          .from(agentObservabilityWorkspaceArchiveRevocations)
          .where(
            and(
              eq(
                agentObservabilityWorkspaceArchiveRevocations.organizationId,
                locked.organizationId,
              ),
              eq(agentObservabilityWorkspaceArchiveRevocations.workspaceId, locked.id),
            ),
          )
          .for('update')
          .limit(1)
      )[0];
      // A committed marker for an active Workspace is impossible through the
      // archive path. Do not let a corrupt row turn a later archive into a
      // false no-op.
      if (existingArchiveMarker) throw new WorkspaceObservabilityArchiveInvariantError();
      const [archiveMarker] = await tx
        .insert(agentObservabilityWorkspaceArchiveRevocations)
        .values({
          organizationId: locked.organizationId,
          workspaceId: locked.id,
          archivedAt: now,
          revocationEpoch: nextWorkspaceRevocationEpoch,
        })
        .returning();
      if (!archiveMarker) throw new WorkspaceObservabilityArchiveInvariantError();
      // Migration 0056's mixed-version trigger validates this durable marker
      // after Workspace status changes. Insert it and CAS to its epoch first
      // so current writers cannot be counted twice while old replicas live.
      const advancedSetting = await tx
        .update(agentObservabilityWorkspaceSettings)
        .set({ revocationEpoch: archiveMarker.revocationEpoch, updatedAt: now })
        .where(
          and(
            eq(agentObservabilityWorkspaceSettings.organizationId, locked.organizationId),
            eq(agentObservabilityWorkspaceSettings.workspaceId, locked.id),
            eq(
              agentObservabilityWorkspaceSettings.revocationEpoch,
              lockedWorkspaceSetting.revocationEpoch,
            ),
          ),
        )
        .returning({ workspaceId: agentObservabilityWorkspaceSettings.workspaceId });
      if (advancedSetting.length !== 1) throw new WorkspaceObservabilityArchiveInvariantError();
      const activeSessions = await tx
        .select({ id: sessions.id })
        .from(sessions)
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.workspaceId, current.id),
            isNull(sessions.archivedAt),
          ),
        )
        .for('update');
      const lifecycleEvents = activeSessions.map((session) =>
        newSessionArchiveOutboxRow(current.id, session.id, now),
      );
      await tx
        .update(workspaces)
        .set({ status: 'archived', archivedAt: now, updatedAt: now })
        .where(
          and(
            eq(workspaces.id, current.id),
            eq(workspaces.organizationId, req.adminAuth!.organizationId),
          ),
        );
      await tx
        .update(apiKeys)
        .set({ status: 'archived', revokedAt: now, updatedAt: now })
        .where(and(eq(apiKeys.workspaceId, current.id), ne(apiKeys.status, 'archived')));
      await tx
        .update(sessions)
        .set({ status: 'archived', archivedAt: now, updatedAt: now })
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.workspaceId, current.id),
            isNull(sessions.archivedAt),
          ),
        );
      if (lifecycleEvents.length > 0) {
        await tx.insert(sessionLifecycleOutbox).values(lifecycleEvents);
      }
      await writeAudit(tx, req, 'workspace.archived', 'workspace', current.id, current.id, {
        revoked_api_keys: true,
        archived_session_count: activeSessions.length,
      });
      return {
        workspace: { ...locked, status: 'archived', archivedAt: now, updatedAt: now },
        lifecycleEventIds: lifecycleEvents.map((event) => event.id),
        changed: true,
      };
    });

    // The database state is the authorization boundary. Durable outbox rows
    // additionally prompt live Harness replicas to tear down warm runners.
    if (archived.changed && archived.lifecycleEventIds.length > 0) {
      await reconcileSessionLifecycleOutbox(db, store, {
        eventIds: archived.lifecycleEventIds,
        batchSize: archived.lifecycleEventIds.length,
      })
        .then((result) => {
          if (result.failed > 0) req.log.warn('workspace archive sentinels queued for retry');
        })
        .catch((error: unknown) => {
          req.log.warn({ err: error }, 'failed to publish queued workspace archive sentinels');
        });
    }
    return workspaceWire(archived.workspace);
  });

  app.post('/v1/organizations/workspaces/:workspaceId/api_keys', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'api_keys:write')) return;
    const workspace = await findWorkspace(db, req);
    if (!workspace) return reply.code(404).send({ error: 'workspace not found' });
    if (workspace.status !== 'active') {
      return reply.code(409).send({ error: 'cannot create a key for an archived workspace' });
    }
    const body = objectBody(req.body);
    const name = requiredName(body['name'], reply);
    if (!name) return;
    const expiresAt = optionalFutureDate(body['expires_at'], reply);
    if (expiresAt === false) return;

    const plaintext = generateApiKey();
    const now = new Date();
    const key = {
      id: newId('apikey'),
      workspaceId: workspace.id,
      hashedKey: await hashApiKey(plaintext),
      keyFingerprint: fingerprintApiKey(plaintext),
      name,
      partialKeyHint: partialApiKeyHint(plaintext),
      principal: '',
      scopes: WORKSPACE_KEY_SCOPES,
      status: 'active',
      expiresAt,
      lastUsedAt: null,
      revokedAt: null,
      createdBy: req.adminAuth!.principal,
      createdAt: now,
      updatedAt: now,
    };
    key.principal = `api-key:${key.id}`;
    try {
      await db.transaction(async (tx) => {
        const locked = await tx
          .select({ status: workspaces.status })
          .from(workspaces)
          .where(
            and(
              eq(workspaces.id, workspace.id),
              eq(workspaces.organizationId, req.adminAuth!.organizationId),
            ),
          )
          .for('update')
          .limit(1);
        if (locked[0]?.status !== 'active') throw new WorkspaceNotActiveError();
        await tx.insert(apiKeys).values(key);
        await writeAudit(tx, req, 'api_key.created', 'api_key', key.id, workspace.id, {
          name,
          expires_at: expiresAt?.toISOString() ?? null,
        });
      });
    } catch (error) {
      if (error instanceof WorkspaceNotActiveError) {
        return reply.code(409).send({ error: 'cannot create a key for an archived workspace' });
      }
      throw error;
    }
    reply.header('cache-control', 'no-store');
    return reply.code(201).send({ ...apiKeyWire(key), key: plaintext });
  });

  app.get('/v1/organizations/api_keys', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'api_keys:read')) return;
    const query = (req.query ?? {}) as ApiKeyListQuery;
    const limit = paginationLimit(query.limit, reply);
    if (limit === null) return;
    if (!validateCursorDirection(query, reply)) return;
    if (query.status && !['active', 'inactive', 'archived', 'expired'].includes(query.status)) {
      return reply.code(400).send({ error: 'invalid status' });
    }
    const principal = req.adminAuth!;
    const now = new Date();
    const conditions = [eq(workspaces.organizationId, principal.organizationId)];
    if (query.workspace_id) conditions.push(eq(apiKeys.workspaceId, query.workspace_id));
    if (query.after_id) conditions.push(gt(apiKeys.id, query.after_id));
    if (query.before_id) conditions.push(lt(apiKeys.id, query.before_id));
    if (query.status && query.status !== 'expired')
      conditions.push(eq(apiKeys.status, query.status));
    if (query.status === 'active' || query.status === 'inactive') {
      conditions.push(or(isNull(apiKeys.expiresAt), gt(apiKeys.expiresAt, now))!);
    }
    if (query.status === 'expired') {
      conditions.push(isNotNull(apiKeys.expiresAt));
      conditions.push(lte(apiKeys.expiresAt, now));
      conditions.push(ne(apiKeys.status, 'archived'));
    }
    const backwards = query.before_id !== undefined;
    const rows = await db
      .select({ key: apiKeys })
      .from(apiKeys)
      .innerJoin(workspaces, eq(apiKeys.workspaceId, workspaces.id))
      .where(and(...conditions))
      .orderBy(backwards ? desc(apiKeys.id) : asc(apiKeys.id))
      .limit(limit + 1);
    return page(
      rows.map(({ key }) => key),
      limit,
      apiKeyWire,
      backwards,
    );
  });

  app.get('/v1/organizations/api_keys/:apiKeyId', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'api_keys:read')) return;
    const key = await findApiKey(db, req);
    if (!key) return reply.code(404).send({ error: 'api key not found' });
    return apiKeyWire(key);
  });

  app.post('/v1/organizations/api_keys/:apiKeyId', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'api_keys:write')) return;
    const current = await findApiKey(db, req);
    if (!current) return reply.code(404).send({ error: 'api key not found' });
    const body = objectBody(req.body);
    const update: { name?: string; status?: string; revokedAt?: Date; updatedAt: Date } = {
      updatedAt: new Date(),
    };
    if (body['name'] !== undefined) {
      const name = requiredName(body['name'], reply);
      if (!name) return;
      update.name = name;
    }
    if (body['status'] !== undefined) {
      if (!['active', 'inactive', 'archived'].includes(String(body['status']))) {
        return reply.code(400).send({ error: 'status must be active, inactive, or archived' });
      }
      update.status = String(body['status']);
      if (update.status === 'archived') update.revokedAt = update.updatedAt;
    }
    if (update.name === undefined && update.status === undefined) {
      return reply.code(400).send({ error: 'name or status is required' });
    }
    const updated = await db.transaction(async (tx) => {
      const workspaceRows = await tx
        .select({ status: workspaces.status })
        .from(workspaces)
        .where(
          and(
            eq(workspaces.id, current.workspaceId),
            eq(workspaces.organizationId, req.adminAuth!.organizationId),
          ),
        )
        .for('update')
        .limit(1);
      if (workspaceRows[0]?.status !== 'active') return null;

      const keyRows = await tx
        .select()
        .from(apiKeys)
        .where(and(eq(apiKeys.id, current.id), eq(apiKeys.workspaceId, current.workspaceId)))
        .for('update')
        .limit(1);
      const locked = keyRows[0];
      if (!locked || locked.status === 'archived' || keyStatus(locked) === 'expired') return null;

      await tx
        .update(apiKeys)
        .set(update)
        .where(and(eq(apiKeys.id, locked.id), eq(apiKeys.workspaceId, locked.workspaceId)));
      await writeAudit(tx, req, 'api_key.updated', 'api_key', locked.id, locked.workspaceId, {
        name: update.name,
        status: update.status,
      });
      return { ...locked, ...update };
    });
    if (!updated) {
      return reply.code(409).send({ error: 'archived or expired api keys cannot be updated' });
    }
    return apiKeyWire(updated);
  });

  registerAdminGuardrailRoutes(app, db);
  registerAdminModelPriceRoutes(app, db);
}

/**
 * Organization-tier model prices.
 *
 * Writes live only here. A workspace that could set its own prices could set
 * them to zero and walk through every budget applied to it, so price authority
 * sits with the same tier that owns organization guardrails, for the same
 * reason. Every write stores `source: 'operator'` — a `source` in the request
 * body is dropped, not honoured.
 *
 * Reads and writes address different things on purpose. The list spans every
 * source because precedence is what an operator is diagnosing when they look;
 * the item routes address the operator entry alone because it is the only row
 * this listener owns. Deleting it falls back to whatever `upstream` or `seed`
 * provides, so an override is reversible without re-entering the original
 * numbers.
 *
 * See `docs/managed-agents/pricing.md`.
 */
export function registerAdminModelPriceRoutes(app: FastifyInstance, db: DbClient): void {
  const prices: ModelPriceStore = createPostgresModelPriceStore(db);

  app.post('/v1/organizations/modelprices', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'model_prices:write')) return;
    const organizationId = req.adminAuth!.organizationId;
    const parsed = ModelPriceCreate.safeParse(req.body);
    if (!parsed.success) return reply.code(400).send({ error: zodMessage(parsed.error) });

    const write = modelPriceWriteFromCreate(parsed.data);
    // PATCH is how an existing entry changes. Letting POST overwrite one would
    // make "create" silently destructive on a retry with a different body.
    if (await prices.get(write.provider, write.modelId, 'operator', organizationId)) {
      return reply
        .code(409)
        .send({ error: `an operator price for ${write.modelId} already exists` });
    }

    const now = new Date();
    // The rate change and its audit event commit together. A price is a
    // privileged mutation, and one that landed with no immutable record of who
    // made it is worse than one that failed outright.
    const created = await db.transaction(async (tx) => {
      const txPrices = createPostgresModelPriceStore(tx);
      await txPrices.upsert('operator', [write], now, organizationId);
      const row = await txPrices.get(write.provider, write.modelId, 'operator', organizationId);
      if (!row) throw new Error(`operator price for ${write.modelId} vanished after write`);
      await writeAudit(tx, req, 'model_price.created', 'model_price', write.modelId, null, {
        provider: write.provider,
        input_per_million_tokens: write.inputPerMillionTokens,
        output_per_million_tokens: write.outputPerMillionTokens,
      });
      return row;
    });
    return reply.code(201).send(modelPriceEntryToWire(created));
  });

  app.get('/v1/organizations/modelprices', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'model_prices:read')) return;
    const organizationId = req.adminAuth!.organizationId;
    const query = (req.query ?? {}) as PaginationQuery & { source?: string };
    const limit = paginationLimit(query.limit, reply);
    if (limit === null) return;
    if (!validateCursorDirection(query, reply)) return;

    let source: ReturnType<typeof parseModelPriceSource> = null;
    if (query.source !== undefined) {
      source = parseModelPriceSource(query.source);
      if (source === null) {
        return reply.code(400).send({ error: 'source must be operator, upstream, or seed' });
      }
    }

    // Seed and upstream rows are deployment-global. Operator overrides are
    // visible only to the organization that owns them.
    const rows = (await prices.list(organizationId))
      .filter((row) => source === null || row.source === source)
      .sort((a, b) => (entryCursor(a) < entryCursor(b) ? -1 : 1));

    const cursor = query.after_id ?? query.before_id;
    if (cursor !== undefined && !rows.some((row) => entryCursor(row) === cursor)) {
      return reply.code(400).send({ error: 'model price cursor not found' });
    }
    const backwards = query.before_id !== undefined;
    const candidates =
      cursor === undefined
        ? rows
        : rows.filter((row) => (backwards ? entryCursor(row) < cursor : entryCursor(row) > cursor));
    const selected = backwards ? candidates.slice(-limit) : candidates.slice(0, limit);

    // The envelope carries composite cursors rather than ids: the table is
    // keyed on (model, source) and has no surrogate id to hand back.
    return {
      data: selected.map(modelPriceEntryToWire),
      has_more: candidates.length > limit,
      first_id: selected[0] ? entryCursor(selected[0]) : null,
      last_id: selected.at(-1) ? entryCursor(selected.at(-1)!) : null,
    };
  });

  app.get('/v1/organizations/modelprices/:modelId', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'model_prices:read')) return;
    const existing = await findOperatorPrice(prices, req);
    if (!existing) return reply.code(404).send({ error: 'model price not found' });
    return modelPriceEntryToWire(existing);
  });

  app.patch('/v1/organizations/modelprices/:modelId', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'model_prices:write')) return;
    const organizationId = req.adminAuth!.organizationId;
    const parsed = ModelPriceUpdate.safeParse(req.body);
    if (!parsed.success) return reply.code(400).send({ error: zodMessage(parsed.error) });

    // Absent means there is no operator override to patch. The upstream or seed
    // row that may exist for this model is not this route's to edit — POST an
    // override instead.
    const existing = await findOperatorPrice(prices, req);
    if (!existing) return reply.code(404).send({ error: 'model price not found' });

    const write = modelPriceWriteFromUpdate(parsed.data, existing);
    const updated = await db.transaction(async (tx) => {
      const txPrices = createPostgresModelPriceStore(tx);
      await txPrices.upsert('operator', [write], new Date(), organizationId);
      const row = await txPrices.get(
        existing.provider,
        existing.modelId,
        'operator',
        organizationId,
      );
      if (!row) throw new Error(`operator price for ${existing.modelId} vanished after write`);
      await writeAudit(tx, req, 'model_price.updated', 'model_price', existing.modelId, null, {
        provider: existing.provider,
        input_per_million_tokens: write.inputPerMillionTokens,
        output_per_million_tokens: write.outputPerMillionTokens,
      });
      return row;
    });
    return modelPriceEntryToWire(updated);
  });

  app.delete('/v1/organizations/modelprices/:modelId', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'model_prices:write')) return;
    const organizationId = req.adminAuth!.organizationId;
    const { modelId } = req.params as { modelId: string };
    const provider = adminPriceProvider(req);
    if (provider === null) {
      return reply.code(400).send({ error: 'provider must be an addressable identifier' });
    }
    // Only the operator row is removed. Whatever upstream or seed carries for
    // this model takes over immediately, so the model does not become unpriced
    // as a side effect of withdrawing an override.
    const removed = await db.transaction(async (tx) => {
      const txPrices = createPostgresModelPriceStore(tx);
      if (!(await txPrices.delete(provider, modelId, 'operator', organizationId))) return false;
      await writeAudit(tx, req, 'model_price.deleted', 'model_price', modelId, null, {
        provider,
        source: 'operator',
      });
      return true;
    });
    if (!removed) return reply.code(404).send({ error: 'model price not found' });
    return reply.send({ provider, model_id: modelId, type: 'model_price_entry_deleted' });
  });
}

/**
 * The table has no surrogate id, so the composite key is both the sort key and
 * the cursor. A space sorts below every character a provider or model id may
 * contain, which keeps the string order identical to the
 * (provider, model, source) tuple order.
 */
function entryCursor(row: StoredModelPrice): string {
  return `${row.provider} ${row.modelId} ${row.source}`;
}

/**
 * The provider half of an item route's identity, from the query string.
 *
 * Absent means the default provider, so a deployment pricing one vendor
 * addresses its entries by model id alone. `null` is an unreadable value, which
 * is a 400 rather than a silent fallback: quietly reading it as the default
 * would delete or patch a different entry than the caller named.
 */
function adminPriceProvider(req: FastifyRequest): string | null {
  const raw = (req.query as { provider?: string } | undefined)?.provider;
  if (raw === undefined) return SEED_PRICE_PROVIDER;
  const parsed = ModelProviderId.safeParse(raw);
  return parsed.success ? parsed.data : null;
}

async function findOperatorPrice(
  prices: ModelPriceStore,
  req: FastifyRequest,
): Promise<StoredModelPrice | null> {
  const { modelId } = req.params as { modelId: string };
  const provider = adminPriceProvider(req);
  if (provider === null) return null;
  return prices.get(provider, modelId, 'operator', req.adminAuth!.organizationId);
}

/**
 * Organization-tier guardrails.
 *
 * This listener is the only place an organization-scoped guardrail can be
 * written. That asymmetry is the tier model: a workspace administrator sees
 * these rows on the workspace API and cannot edit or remove them, so a policy
 * the organization set stays set. Rules are validated with the same compiler
 * the workspace plane uses, so a guardrail cannot be authored here that would
 * fail to evaluate there.
 */
export function registerAdminGuardrailRoutes(app: FastifyInstance, db: DbClient): void {
  app.post('/v1/organizations/guardrails', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'guardrails:write')) return;
    const parsed = GuardrailCreate.safeParse(req.body);
    if (!parsed.success) return reply.code(400).send({ error: zodMessage(parsed.error) });
    const body = parsed.data;
    if (body.scope !== undefined && body.scope !== 'organization') {
      // A workspace-owned guardrail needs a workspace to own it, and this
      // listener has none. Minting one here would produce a row that applies
      // nowhere.
      return reply
        .code(400)
        .send({ error: "scope must be 'organization' on the organization listener" });
    }

    const rule = ruleToStorage(body.rule);
    // The guard above admits nothing else on this listener.
    const authored = resolveGuardrailAuthoring({
      rule,
      scope: 'organization',
      phases: body.phases,
    });
    if (!authored.ok) return reply.code(400).send({ error: authoringMessage(authored.errors) });
    const metadata = parseMetadata(body.metadata);
    if (!metadata.ok) return reply.code(400).send({ error: metadata.error });

    const now = new Date();
    const row = {
      id: newId('grd'),
      organizationId: req.adminAuth!.organizationId,
      // Null by design, and enforced by a table check: an organization
      // guardrail has no owning workspace because it applies across all of them.
      workspaceId: null,
      name: body.name,
      description: body.description ?? null,
      enabled: body.enabled ?? true,
      phases: authored.value.phases,
      scope: 'organization',
      rule: authored.value.rule,
      metadata: toJsonMetadata(metadata.value!),
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    };
    await db.transaction(async (tx) => {
      await tx.insert(guardrails).values(row);
      await writeAudit(tx, req, 'guardrail.created', 'guardrail', row.id, null, {
        name: row.name,
        scope: 'organization',
      });
    });
    return reply.code(201).send(guardrailToWire(row));
  });

  app.get('/v1/organizations/guardrails', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'guardrails:read')) return;
    const query = (req.query ?? {}) as PaginationQuery & { include_archived?: string | boolean };
    const limit = paginationLimit(query.limit, reply);
    if (limit === null) return;
    if (!validateCursorDirection(query, reply)) return;
    const includeArchived = optionalBoolean(query.include_archived, 'include_archived', reply);
    if (includeArchived === null) return;

    const conditions = [
      eq(guardrails.organizationId, req.adminAuth!.organizationId),
      eq(guardrails.scope, 'organization'),
    ];
    if (!includeArchived) conditions.push(isNull(guardrails.archivedAt));
    if (query.after_id) conditions.push(gt(guardrails.id, query.after_id));
    if (query.before_id) conditions.push(lt(guardrails.id, query.before_id));
    const backwards = query.before_id !== undefined;
    const rows = await db
      .select()
      .from(guardrails)
      .where(and(isNull(guardrails.deletedAt), ...conditions))
      .orderBy(backwards ? desc(guardrails.id) : asc(guardrails.id))
      .limit(limit + 1);
    return page(rows, limit, guardrailToWire, backwards);
  });

  app.get('/v1/organizations/guardrails/:guardrailId', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'guardrails:read')) return;
    const row = await findOrganizationGuardrail(db, req);
    if (!row) return reply.code(404).send({ error: 'guardrail not found' });
    return guardrailToWire(row);
  });

  app.patch('/v1/organizations/guardrails/:guardrailId', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'guardrails:write')) return;
    const parsed = GuardrailUpdate.safeParse(req.body);
    if (!parsed.success) return reply.code(400).send({ error: zodMessage(parsed.error) });
    const body = parsed.data;
    if (body.scope !== undefined && body.scope !== 'organization') {
      // Retiering an existing guardrail would hand it to a workspace that
      // cannot be named here; delete and re-create instead.
      return reply
        .code(400)
        .send({ error: 'an organization guardrail cannot be moved to another scope' });
    }

    const existing = await findOrganizationGuardrail(db, req);
    if (!existing) return reply.code(404).send({ error: 'guardrail not found' });

    const rule = body.rule ? ruleToStorage(body.rule) : storedRule(existing);
    const phases = body.phases ?? storedPhases(existing);
    // Re-tiering is refused above, so the scope cannot have moved.
    const authored = resolveGuardrailAuthoring({ rule, scope: 'organization', phases });
    if (!authored.ok) return reply.code(400).send({ error: authoringMessage(authored.errors) });

    let metadata = existing.metadata;
    if (body.metadata !== undefined) {
      const patch = parseMetadataPatch(body.metadata);
      if (!patch.ok) return reply.code(400).send({ error: patch.error });
      const patched = applyMetadataPatch(existing.metadata, patch.value!);
      const limitError = validateMetadataLimits(patched);
      if (limitError) return reply.code(400).send({ error: limitError });
      metadata = toJsonMetadata(patched);
    }

    const now = new Date();
    const next = {
      ...existing,
      name: body.name ?? existing.name,
      description: body.description !== undefined ? body.description : existing.description,
      enabled: body.enabled ?? existing.enabled,
      phases: authored.value.phases,
      rule: authored.value.rule,
      metadata,
      updatedAt: now,
    };
    await db.transaction(async (tx) => {
      await tx
        .update(guardrails)
        .set({
          name: next.name,
          description: next.description,
          enabled: next.enabled,
          phases: next.phases,
          rule: next.rule,
          metadata: next.metadata,
          updatedAt: now,
        })
        .where(
          and(
            isNull(guardrails.deletedAt),
            eq(guardrails.id, existing.id),
            eq(guardrails.organizationId, req.adminAuth!.organizationId),
          ),
        );
      await writeAudit(tx, req, 'guardrail.updated', 'guardrail', existing.id, null, {
        name: next.name,
        enabled: next.enabled,
      });
    });
    return guardrailToWire(next);
  });

  app.delete('/v1/organizations/guardrails/:guardrailId', async (req, reply) => {
    if (!requireAdminScope(req, reply, 'guardrails:write')) return;
    const organizationId = req.adminAuth!.organizationId;
    const existing = await findOrganizationGuardrail(db, req);
    if (!existing) return reply.code(404).send({ error: 'guardrail not found' });
    const outcome = await db.transaction(async (tx) => {
      // The same rule the workspace tier enforces, widened to the tier's reach.
      // An agent that names a guardrail explicitly is relying on it, and
      // `composeGuardrails` skips an id it cannot resolve *silently* — no
      // `invalid` entry, no error, unlike a rule that fails to compile. Deleting
      // a referenced org guardrail would therefore leave sessions across every
      // workspace running with one fewer guardrail and nothing anywhere saying
      // so. An org guardrail is reachable from any workspace in the
      // organization, and `agents` carries no organization of its own, so the
      // reference check joins through `workspaces`.
      const referencing = await tx
        .select({ id: agents.id })
        .from(agents)
        .innerJoin(workspaces, eq(workspaces.id, agents.workspaceId))
        .where(
          and(
            isNull(agents.deletedAt),
            eq(workspaces.organizationId, organizationId),
            sql`${agents.guardrailIds} @> ${JSON.stringify([existing.id])}::jsonb`,
          ),
        )
        .limit(1);
      if (referencing[0]) return 'in_use' as const;

      await tx
        .update(guardrails)
        .set({ deletedAt: new Date() })
        .where(
          and(
            isNull(guardrails.deletedAt),
            eq(guardrails.id, existing.id),
            eq(guardrails.organizationId, organizationId),
          ),
        );
      await writeAudit(tx, req, 'guardrail.deleted', 'guardrail', existing.id, null, {
        name: existing.name,
      });
      return 'deleted' as const;
    });
    if (outcome === 'in_use') {
      return reply
        .code(409)
        .send({ error: 'guardrail is referenced by one or more agents in this organization' });
    }
    return reply.send({ id: existing.id, type: 'guardrail_deleted' });
  });
}

async function findOrganizationGuardrail(db: DbClient, req: FastifyRequest) {
  const { guardrailId } = req.params as { guardrailId: string };
  const rows = await db
    .select()
    .from(guardrails)
    .where(
      and(
        isNull(guardrails.deletedAt),
        and(
          eq(guardrails.id, guardrailId),
          eq(guardrails.organizationId, req.adminAuth!.organizationId),
          // Scoped here as well as by id: a workspace-owned guardrail is not the
          // organization listener's to read or write, even within its own
          // organization.
          eq(guardrails.scope, 'organization'),
        ),
      ),
    )
    .limit(1);
  return rows[0];
}

function objectBody(body: unknown): Record<string, unknown> {
  return body !== null && typeof body === 'object' && !Array.isArray(body)
    ? (body as Record<string, unknown>)
    : {};
}

function requiredName(value: unknown, reply: FastifyReply): string | null {
  if (typeof value !== 'string' || value.trim().length === 0 || value.trim().length > 200) {
    reply.code(400).send({ error: 'name must be a non-empty string of at most 200 characters' });
    return null;
  }
  return value.trim();
}

function optionalFutureDate(value: unknown, reply: FastifyReply): Date | null | false {
  if (value === undefined || value === null) return null;
  if (typeof value !== 'string') {
    reply.code(400).send({ error: 'expires_at must be an ISO-8601 timestamp' });
    return false;
  }
  const date = new Date(value);
  if (!Number.isFinite(date.getTime()) || date <= new Date()) {
    reply.code(400).send({ error: 'expires_at must be a future ISO-8601 timestamp' });
    return false;
  }
  return date;
}

function paginationLimit(value: unknown, reply: FastifyReply): number | null {
  const limit = value === undefined ? DEFAULT_LIMIT : Number(value);
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT) {
    reply.code(400).send({ error: `limit must be an integer between 1 and ${MAX_LIMIT}` });
    return null;
  }
  return limit;
}

function validateCursorDirection(query: PaginationQuery, reply: FastifyReply): boolean {
  if (query.after_id !== undefined && query.before_id !== undefined) {
    reply.code(400).send({ error: 'after_id and before_id cannot be used together' });
    return false;
  }
  return true;
}

function optionalBoolean(value: unknown, field: string, reply: FastifyReply): boolean | null {
  if (value === undefined || value === false || value === 'false') return false;
  if (value === true || value === 'true') return true;
  reply.code(400).send({ error: `${field} must be true or false` });
  return null;
}

function page<T, U>(rows: T[], limit: number, wire: (row: T) => U, backwards = false) {
  const hasMore = rows.length > limit;
  const selected = rows.slice(0, limit);
  if (backwards) selected.reverse();
  const data = selected.map(wire);
  const ids = data as Array<{ id?: string }>;
  return {
    data,
    has_more: hasMore,
    first_id: ids[0]?.id ?? null,
    last_id: ids.at(-1)?.id ?? null,
  };
}

async function findWorkspace(db: DbClient, req: FastifyRequest) {
  const { workspaceId } = req.params as { workspaceId: string };
  const rows = await db
    .select()
    .from(workspaces)
    .where(
      and(
        eq(workspaces.id, workspaceId),
        eq(workspaces.organizationId, req.adminAuth!.organizationId),
      ),
    )
    .limit(1);
  return rows[0];
}

async function findApiKey(db: DbClient, req: FastifyRequest) {
  const { apiKeyId } = req.params as { apiKeyId: string };
  const rows = await db
    .select({ key: apiKeys })
    .from(apiKeys)
    .innerJoin(workspaces, eq(apiKeys.workspaceId, workspaces.id))
    .where(
      and(eq(apiKeys.id, apiKeyId), eq(workspaces.organizationId, req.adminAuth!.organizationId)),
    )
    .limit(1);
  return rows[0]?.key;
}

async function putOrganizationAgentObservabilityDefault(
  req: FastifyRequest,
  reply: FastifyReply,
  db: DbClient,
  secretStore: SecretStore | undefined,
) {
  let request: ReturnType<typeof parseOrganizationAgentObservabilityPutRequest>;
  let idempotencyKey: string;
  let ifMatch: string | null;
  try {
    request = parseOrganizationAgentObservabilityPutRequest(req.body);
    idempotencyKey = parseOrganizationAgentObservabilityIdempotencyKey(
      req.headers['idempotency-key'],
    );
    ifMatch = parseOrganizationAgentObservabilityIfMatch(req.headers['if-match']);
  } catch (error) {
    if (error instanceof OrganizationAgentObservabilityMutationRequestError) {
      return reply.code(400).send({ error: 'invalid agent observability request' });
    }
    throw error;
  }

  const principal = req.adminAuth!;
  const result = await executeOrganizationAgentObservabilityPut({
    db,
    secretStore,
    organizationId: principal.organizationId,
    principal: principal.principal,
    authMethod: principal.authMethod,
    requestId: req.id,
    request,
    idempotencyKey,
    ifMatch,
  });
  return sendOrganizationAgentObservabilityMutationResult(reply, result);
}

async function putWorkspaceAgentObservability(
  req: FastifyRequest,
  reply: FastifyReply,
  db: DbClient,
  secretStore: SecretStore | undefined,
) {
  const params = AgentObservabilityWorkspacePathParamsSchema.safeParse(req.params);
  if (!params.success) return reply.code(404).send({ error: 'workspace not found' });

  let request: ReturnType<typeof parseWorkspaceAgentObservabilityPutRequest>;
  let idempotencyKey: string;
  let ifMatch: string | null;
  try {
    request = parseWorkspaceAgentObservabilityPutRequest(req.body);
    idempotencyKey = parseWorkspaceAgentObservabilityIdempotencyKey(req.headers['idempotency-key']);
    ifMatch = parseWorkspaceAgentObservabilityIfMatch(req.headers['if-match']);
  } catch (error) {
    if (error instanceof WorkspaceAgentObservabilityMutationRequestError) {
      return reply.code(400).send({ error: 'invalid agent observability request' });
    }
    throw error;
  }

  const principal = req.adminAuth!;
  const result = await executeWorkspaceAgentObservabilityPut({
    db,
    secretStore,
    organizationId: principal.organizationId,
    workspaceId: params.data.workspaceId,
    principal: principal.principal,
    authMethod: principal.authMethod,
    requestId: req.id,
    request,
    idempotencyKey,
    ifMatch,
  });
  return sendWorkspaceAgentObservabilityMutationResult(reply, result);
}

async function rotateWorkspaceAgentObservabilityCredentials(
  req: FastifyRequest,
  reply: FastifyReply,
  db: DbClient,
  secretStore: SecretStore | undefined,
) {
  const params = AgentObservabilityWorkspacePathParamsSchema.safeParse(req.params);
  if (!params.success) return reply.code(404).send({ error: 'workspace not found' });

  let request: ReturnType<typeof parseWorkspaceAgentObservabilityCredentialRotationRequest>;
  let idempotencyKey: string;
  let ifMatch: string | null;
  try {
    request = parseWorkspaceAgentObservabilityCredentialRotationRequest(req.body);
    idempotencyKey = parseWorkspaceAgentObservabilityIdempotencyKey(req.headers['idempotency-key']);
    ifMatch = parseWorkspaceAgentObservabilityIfMatch(req.headers['if-match']);
  } catch (error) {
    if (error instanceof WorkspaceAgentObservabilityMutationRequestError) {
      return reply.code(400).send({ error: 'invalid agent observability request' });
    }
    throw error;
  }

  const principal = req.adminAuth!;
  const result = await executeWorkspaceAgentObservabilityCredentialRotation({
    db,
    secretStore,
    organizationId: principal.organizationId,
    workspaceId: params.data.workspaceId,
    principal: principal.principal,
    authMethod: principal.authMethod,
    requestId: req.id,
    request,
    idempotencyKey,
    ifMatch,
  });
  return sendWorkspaceAgentObservabilityMutationResult(reply, result);
}

async function rotateOrganizationAgentObservabilityCredentials(
  req: FastifyRequest,
  reply: FastifyReply,
  db: DbClient,
  secretStore: SecretStore | undefined,
) {
  let request: ReturnType<typeof parseOrganizationAgentObservabilityCredentialRotationRequest>;
  let idempotencyKey: string;
  let ifMatch: string | null;
  try {
    request = parseOrganizationAgentObservabilityCredentialRotationRequest(req.body);
    idempotencyKey = parseOrganizationAgentObservabilityIdempotencyKey(
      req.headers['idempotency-key'],
    );
    ifMatch = parseOrganizationAgentObservabilityIfMatch(req.headers['if-match']);
  } catch (error) {
    if (error instanceof OrganizationAgentObservabilityMutationRequestError) {
      return reply.code(400).send({ error: 'invalid agent observability request' });
    }
    throw error;
  }

  const principal = req.adminAuth!;
  const result = await executeOrganizationAgentObservabilityCredentialRotation({
    db,
    secretStore,
    organizationId: principal.organizationId,
    principal: principal.principal,
    authMethod: principal.authMethod,
    requestId: req.id,
    request,
    idempotencyKey,
    ifMatch,
  });
  return sendOrganizationAgentObservabilityMutationResult(reply, result);
}

async function disableOrganizationAgentObservabilityDefault(
  req: FastifyRequest,
  reply: FastifyReply,
  db: DbClient,
) {
  let request: ReturnType<typeof parseOrganizationAgentObservabilityDisableRequest>;
  let idempotencyKey: string;
  let ifMatch: string | null;
  try {
    request = parseOrganizationAgentObservabilityDisableRequest(req.body);
    idempotencyKey = parseOrganizationAgentObservabilityIdempotencyKey(
      req.headers['idempotency-key'],
    );
    ifMatch = parseOrganizationAgentObservabilityIfMatch(req.headers['if-match']);
  } catch (error) {
    if (error instanceof OrganizationAgentObservabilityMutationRequestError) {
      return reply.code(400).send({ error: 'invalid agent observability request' });
    }
    throw error;
  }

  const principal = req.adminAuth!;
  const result = await executeOrganizationAgentObservabilityDisable({
    db,
    organizationId: principal.organizationId,
    principal: principal.principal,
    authMethod: principal.authMethod,
    requestId: req.id,
    request,
    idempotencyKey,
    ifMatch,
  });
  return sendOrganizationAgentObservabilityMutationResult(reply, result);
}

type OrganizationAgentObservabilityMutationRouteResult =
  | ExecuteOrganizationAgentObservabilityPutResult
  | ExecuteOrganizationAgentObservabilityCredentialRotationResult
  | ExecuteOrganizationAgentObservabilityDisableResult;

type WorkspaceAgentObservabilityMutationRouteResult =
  | ExecuteWorkspaceAgentObservabilityPutResult
  | ExecuteWorkspaceAgentObservabilityCredentialRotationResult;

function sendOrganizationAgentObservabilityMutationResult(
  reply: FastifyReply,
  result: OrganizationAgentObservabilityMutationRouteResult,
) {
  if (result.kind === 'success') {
    reply.header('cache-control', 'no-store');
    return reply.code(result.status).send(result.body);
  }
  if (result.kind === 'bad_request') {
    return reply.code(400).send({ error: 'invalid agent observability request' });
  }
  if (result.kind === 'not_found') {
    return reply.code(404).send({ error: 'organization not found' });
  }
  if (result.kind === 'conflict') {
    return reply.code(409).send({ error: 'agent observability mutation conflict' });
  }
  if (result.kind === 'stale') {
    return reply.code(412).send({ error: 'agent observability state is stale' });
  }
  if (result.kind === 'precondition_required') {
    return reply.code(428).send({ error: 'if-match is required' });
  }
  return reply.code(503).send({ error: 'agent observability unavailable' });
}

function sendWorkspaceAgentObservabilityMutationResult(
  reply: FastifyReply,
  result: WorkspaceAgentObservabilityMutationRouteResult,
) {
  if (result.kind === 'success') {
    reply.header('cache-control', 'no-store');
    return reply.code(result.status).send(result.body);
  }
  if (result.kind === 'bad_request') {
    return reply.code(400).send({ error: 'invalid agent observability request' });
  }
  if (result.kind === 'not_found') {
    return reply.code(404).send({ error: 'workspace not found' });
  }
  if (result.kind === 'conflict') {
    return reply.code(409).send({ error: 'agent observability mutation conflict' });
  }
  if (result.kind === 'stale') {
    return reply.code(412).send({ error: 'agent observability state is stale' });
  }
  if (result.kind === 'precondition_required') {
    return reply.code(428).send({ error: 'if-match is required' });
  }
  return reply.code(503).send({ error: 'agent observability unavailable' });
}

function workspaceWire(row: typeof workspaces.$inferSelect) {
  return {
    type: 'workspace',
    id: row.id,
    name: row.name,
    created_at: row.createdAt.toISOString(),
    archived_at: row.archivedAt?.toISOString() ?? null,
  };
}

function nextWorkspaceArchiveRevocationEpoch(
  setting: typeof agentObservabilityWorkspaceSettings.$inferSelect,
  organizationId: string,
  workspaceId: string,
): number {
  const validModeBinding =
    (setting.mode === 'custom' && isNonEmptyString(setting.bindingId)) ||
    ((setting.mode === 'inherit' || setting.mode === 'disabled') && setting.bindingId === null);
  if (
    setting.organizationId !== organizationId ||
    setting.workspaceId !== workspaceId ||
    !isWorkspaceObservabilityMode(setting.mode) ||
    !validModeBinding ||
    !isAgentObservabilityCaptureMode(setting.captureCeiling) ||
    !isNonnegativeSafeInteger(setting.selectionEpoch) ||
    !isNonnegativeSafeInteger(setting.revocationEpoch) ||
    setting.revocationEpoch >= Number.MAX_SAFE_INTEGER ||
    !isNonnegativeSafeInteger(setting.captureRestrictionEpoch)
  ) {
    throw new WorkspaceObservabilityArchiveInvariantError();
  }
  return setting.revocationEpoch + 1;
}

function isNonnegativeSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function organizationWire(row: typeof organizations.$inferSelect) {
  return {
    type: 'organization',
    id: row.id,
    name: row.name,
    status: row.status,
    created_at: row.createdAt.toISOString(),
  };
}

function keyStatus(row: typeof apiKeys.$inferSelect): string {
  if (row.status !== 'archived' && row.expiresAt && row.expiresAt <= new Date()) return 'expired';
  return row.status;
}

function apiKeyWire(row: typeof apiKeys.$inferSelect) {
  return {
    type: 'api_key',
    id: row.id,
    name: row.name,
    workspace_id: row.workspaceId,
    partial_key_hint: row.partialKeyHint,
    status: keyStatus(row),
    created_at: row.createdAt.toISOString(),
    expires_at: row.expiresAt?.toISOString() ?? null,
    created_by: { type: 'principal', id: row.createdBy },
  };
}

type AuditDb = Pick<DbClient, 'insert'>;

async function writeAudit(
  db: AuditDb,
  req: FastifyRequest,
  action: string,
  targetType: string,
  targetId: string,
  workspaceId: string | null,
  metadata: Record<string, unknown>,
): Promise<void> {
  const principal = req.adminAuth!;
  await db.insert(adminAuditEvents).values({
    id: newId('audit'),
    organizationId: principal.organizationId,
    workspaceId,
    actor: principal.principal,
    authMethod: principal.authMethod,
    action,
    targetType,
    targetId,
    requestId: req.id,
    result: 'success',
    metadata,
  });
}
