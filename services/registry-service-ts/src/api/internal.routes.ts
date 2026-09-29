// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  parseUsageDelta,
  priceUsageDelta,
  persistUsage,
  loadGuardrailUsageState,
  loadGuardrailTurnSubject,
  loadGuardrailSubjectWindowState,
  utcDateWindow,
  applySessionStateUpdate,
  applyCounterStateUpdate,
  parseGuardrailStateBody,
} from '../domain/usage-accounting.js';
export {
  usageCostNanoUsd,
  parseGuardrailStateBody,
  type UsageDelta,
  type PersistedStateScope,
  type GuardrailStateWrite,
  type GuardrailStateBatch,
  type GuardrailStateBodyResult,
} from '../domain/usage-accounting.js';

import { Readable } from 'node:stream';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { eq, and, isNull, sql, inArray, ne } from 'drizzle-orm';
import type { FileStore, FilePurpose, FileRecord } from '@orca/file-store';
import { SEED_PRICE_PROVIDER, piLlmRoute } from '@orca/harness-catalog';
import {
  normalizeMemoryRelativePath,
  MemoryConflictError,
  type MemoryRecord,
  type MemoryStore as MemoryStoreLib,
  type MemoryVersionRecord,
} from '@orca/memory-store';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  loadSessionExecutionOwner,
  loadSessionHarnessBinding,
} from '../domain/session-harness-binding.js';
import {
  HarnessStateWriteError,
  HarnessTurnInvalidBindingError,
  normalizeHarnessState,
  saveHarnessState,
  transitionHarnessTurn,
  loadHarnessTurnState,
} from '../domain/harness-state.js';
import {
  agents,
  adminAuditEvents,
  agentVersions,
  environments,
  gitCredentials,
  sessionResources,
  sessions,
  vaultCredentials,
  workspaces,
} from '../persistence/postgres/schema.js';
import { buildRuntimeSecretResolver } from '../secrets/index.js';
import type { SecretProvider, SecretStore } from '../secrets/secret-provider.js';
import type { SessionJwtMinter } from '../auth/session-jwt.js';
import { loadInternalSession } from './sessions.routes.js';
import { verifyEnvKeyForEnvironment } from '../domain/environment-key-state.js';
import { EnvironmentClaimStore, type EnvironmentClaim } from '../domain/environment-claims.js';
import {
  parseClaimBody,
  parseConnScopedBody,
  parseReapBody,
} from '../domain/internal-claim-requests.js';
import {
  InvalidRuntimeBindingError,
  loadEffectiveSessionAgentModels,
  PreparedExecutionNotFoundError,
  prepareGatewayGuardrails,
  RuntimeDependencyUnavailableError,
  prepareExecution,
} from '../domain/prepare-execution.js';
import { isWorkspaceId } from '../auth/workspace-id.js';
import { repositoryUrlsEqual } from '../domain/git-credentials.js';
import {
  AgentObservabilitySecretResolutionSchema,
  AgentObservabilitySessionContextSchema,
  InternalAgentObservabilityContextPathParamsSchema,
  InternalAgentObservabilitySecretPathParamsSchema,
  PreparedAgentSnapshotSchema,
  type PreparedAgentSnapshot,
  internalContract,
} from '../contracts/internal.contract.js';
import {
  AgentObservabilitySessionContextNotFoundError,
  AgentObservabilitySessionContextUnavailableError,
  loadAgentObservabilitySessionContext,
} from '../domain/agent-observability-context-resolver.js';
import {
  AgentObservabilitySecretResolutionDeniedError,
  AgentObservabilitySecretResolutionNotFoundError,
  AgentObservabilitySecretResolutionUnavailableError,
  resolveAgentObservabilitySessionSecret,
} from '../domain/agent-observability-secret-resolver.js';
import { resolveMcpDestination } from '../domain/mcp-destination.js';
import { intersectLlmModels } from '../domain/llm-policy.js';
import {
  classifyRequestedVaultCredentialId,
  hasCanonicalRequestedCredentialPath,
  logicalResolutionAuditMetadata,
  providerCredentialResolution,
  selectAuthorizedLogicalCredential,
  type LogicalCredentialSelection,
  type LogicalResolutionAuditInput,
} from '../domain/vault-credential-resolution.js';
import { newId } from '../domain/versioning.js';
import type { SessionJwtLlmPolicy } from '../config.js';
import { createGuardedFetch } from './egress-guard.js';
import { effectiveGuardrailBundle } from '../domain/effective-guardrail-bundle.js';
import { refreshCredentialWithLease, type CredentialRow } from './vault-credentials.routes.js';
import {
  authoritativeUsageCallerForSnapshot,
  registryOwnsCodexUsage,
  type AuthoritativeUsageCaller,
} from '../domain/usage-authority.js';

const SESSION_ID_RE = /^ses_[A-Za-z0-9_-]+$/;
const ENV_ID_RE = /^env_[A-Za-z0-9]+$/;
const THREAD_ID_RE = /^sth_[A-Za-z0-9_-]+$/;
const AGENT_ID_RE = /^agt_[A-Za-z0-9_-]+$/;

async function loadAuthoritativeUsageCaller(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  gatewayRegistryUsageEnabled: boolean,
): Promise<
  | { kind: 'found'; caller: AuthoritativeUsageCaller; registryOwned: boolean }
  | { kind: 'not_found' }
  | { kind: 'unavailable' }
> {
  const rows = await db
    .select({
      snapshot: agentVersions.snapshot,
      environmentId: sessions.environmentId,
      target: environments.target,
    })
    .from(sessions)
    .leftJoin(
      environments,
      and(
        eq(environments.workspaceId, sessions.workspaceId),
        eq(environments.id, sessions.environmentId),
      ),
    )
    .innerJoin(
      agentVersions,
      and(
        eq(agentVersions.workspaceId, sessions.workspaceId),
        eq(agentVersions.agentId, sessions.agentId),
        eq(agentVersions.version, sessions.agentVersion),
      ),
    )
    .where(
      and(
        isNull(sessions.deletedAt),
        eq(sessions.workspaceId, workspaceId),
        eq(sessions.id, sessionId),
      ),
    )
    .limit(1);
  const row = rows[0];
  if (!row) return { kind: 'not_found' };
  if (row.environmentId && !row.target) return { kind: 'unavailable' };
  const caller = authoritativeUsageCallerForSnapshot(row.snapshot, gatewayRegistryUsageEnabled);
  return caller
    ? {
        kind: 'found',
        caller,
        registryOwned: registryOwnsCodexUsage(row.snapshot, row.target ?? 'cloud'),
      }
    : { kind: 'unavailable' };
}

/** Default heartbeat-staleness TTL (ms) when the caller doesn't override it. */
const DEFAULT_ENVIRONMENT_CLAIM_TTL_MS = 90_000;

function claimToApi(c: EnvironmentClaim): {
  environment_id: string;
  owner_pod: string;
  worker_conn_id: string;
  claimed_at: string;
  last_ping: string;
} {
  return {
    environment_id: c.environmentId,
    owner_pod: c.ownerPod,
    worker_conn_id: c.workerConnId,
    claimed_at: c.claimedAt.toISOString(),
    last_ping: c.lastPing.toISOString(),
  };
}

const SESSION_STATUSES: ReadonlySet<string> = new Set([
  'idle',
  'running',
  'rescheduling',
  'terminated',
]);

/**
 * Anthropic's Files API contract caps an individual upload at 500 MB. The
 * mesh-internal `/internal/files` route enforces the same cap so the
 * harness's output indexer can't bypass the public route's limit by
 * registering bytes here.
 */
const MAX_FILE_BYTES = 500 * 1024 * 1024;

/**
 * Anthropic's Memory Tool contract caps each memory at ~100 KB. Mirrors the
 * public PATCH route + the library's own defensive cap so the harness watcher
 * can't sneak oversize payloads through the mesh-internal channel.
 */
const MAX_MEMORY_BYTES = 100 * 1024;

/**
 * Stop the secret-release authorization transaction when its peer has gone
 * away. `close` also fires after a normal response, so only treat it as a
 * disconnect while the response remains unwritable.
 */
function requestAbortSignal(
  req: FastifyRequest,
  reply: FastifyReply,
): {
  signal: AbortSignal;
  dispose(): void;
} {
  const controller = new AbortController();
  const abort = () => {
    if (!controller.signal.aborted) controller.abort(new Error('internal request disconnected'));
  };
  const onRequestAborted = () => abort();
  const onResponseClose = () => {
    if (!reply.raw.writableEnded) abort();
  };

  if (req.raw.aborted) abort();
  else req.raw.once('aborted', onRequestAborted);
  reply.raw.once('close', onResponseClose);

  return {
    signal: controller.signal,
    dispose() {
      req.raw.removeListener('aborted', onRequestAborted);
      reply.raw.removeListener('close', onResponseClose);
    },
  };
}

function toApi(r: FileRecord): {
  id: string;
  filename: string;
  mime_type: string;
  size_bytes: number;
  sha256: string;
  metadata: Record<string, string>;
  purpose: FilePurpose;
  scope_id: string | null;
  downloadable: boolean;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
} {
  return {
    id: r.id,
    filename: r.filename,
    mime_type: r.mimeType,
    size_bytes: r.sizeBytes,
    sha256: r.sha256,
    metadata: r.metadata,
    purpose: r.purpose,
    scope_id: r.scopeId,
    downloadable: r.downloadable,
    archived_at: r.archivedAt?.toISOString() ?? null,
    created_at: r.createdAt.toISOString(),
    updated_at: r.updatedAt.toISOString(),
  };
}

function memoryToApi(r: MemoryRecord): {
  id: string;
  store_id: string;
  path: string;
  current_sha256: string;
  size_bytes: number;
  updated_at: string;
  updated_by_session_id: string | null;
  updated_by_event_id: string | null;
} {
  return {
    id: r.id,
    store_id: r.storeId,
    path: r.path,
    current_sha256: r.currentSha256,
    size_bytes: r.sizeBytes,
    updated_at: r.updatedAt.toISOString(),
    updated_by_session_id: r.updatedBySessionId,
    updated_by_event_id: r.updatedByEventId,
  };
}

function versionToApi(r: MemoryVersionRecord): {
  id: string;
  store_id: string;
  memory_id: string;
  path: string;
  sha256: string;
  size_bytes: number;
  written_by_session_id: string | null;
  written_by_event_id: string | null;
  written_at: string;
  redacted_at: string | null;
} {
  return {
    id: r.id,
    store_id: r.storeId,
    memory_id: r.memoryId,
    path: r.path,
    sha256: r.sha256,
    size_bytes: r.sizeBytes,
    written_by_session_id: r.writtenBySessionId,
    written_by_event_id: r.writtenByEventId,
    written_at: r.writtenAt.toISOString(),
    redacted_at: r.redactedAt ? r.redactedAt.toISOString() : null,
  };
}

export interface InternalRoutesOptions {
  /**
   * Heartbeat-staleness TTL (ms) for the durable environment-claim reaper
   * (`POST /internal/environments/claims/reap`). Defaults to
   * {@link DEFAULT_ENVIRONMENT_CLAIM_TTL_MS} when omitted.
   */
  environmentClaimTtlMs?: number;
  /** True only when AI Gateway's Registry usage sink is deployed and enabled. */
  gatewayRegistryUsageEnabled?: boolean;
}

export function registerInternalRoutes(
  app: FastifyInstance,
  db: DbClient,
  minter: SessionJwtMinter,
  fileStore: FileStore,
  memoryStore?: MemoryStoreLib,
  secretProvider?: SecretProvider,
  secretStore?: SecretStore,
  sessionJwtLlmPolicy?: SessionJwtLlmPolicy,
  fetchImpl?: typeof fetch,
  options: InternalRoutesOptions = {},
): void {
  const secrets = buildRuntimeSecretResolver(secretProvider, secretStore);
  const oauthFetch = fetchImpl ?? createGuardedFetch(globalThis.fetch);

  // Durable environment-claim store: the multi-replica equivalent of an
  // in-memory tunnel/host registry. Constructed once here so every claim route
  // shares the registry's connection pool.
  const claims = new EnvironmentClaimStore(db);
  const claimTtlMs = options.environmentClaimTtlMs ?? DEFAULT_ENVIRONMENT_CLAIM_TTL_MS;
  const gatewayRegistryUsageEnabled = options.gatewayRegistryUsageEnabled ?? false;

  app.get('/internal/v1/guardrails/effective', async (req, reply) => {
    // Only the authenticated Gateway workload may project its verified session
    // claims onto this query. Cross-check every authorization dimension against
    // Registry metadata and the session's pinned roster before serving policy.
    if (req.internalAuth?.caller !== 'ai-gateway' && req.internalAuth?.caller !== 'shared') {
      return reply.code(403).send({ error: 'forbidden' });
    }
    const query = internalContract.effectiveGuardrails.query.safeParse(req.query);
    if (!query.success) return reply.code(400).send({ error: 'invalid guardrail scope' });
    const {
      principal_id: principalId,
      runtime_config_revision: requestedRevision,
      'scope.org_id': organizationId,
      'scope.workspace_id': workspaceId,
      'scope.session_id': sessionId,
      'scope.agent_id': agentId,
    } = query.data;
    if (!isWorkspaceId(workspaceId) || principalId !== sessionId) {
      return reply.code(403).send({ error: 'guardrail scope mismatch' });
    }
    const session = await loadActiveRuntimeSession(db, workspaceId, sessionId);
    if (!session || session.organizationId !== organizationId) {
      return reply.code(404).send({ error: 'session not found' });
    }
    if (requestedRevision !== String(session.runtimeRevision)) {
      return reply.code(409).send({ error: 'guardrail revision or agent mismatch' });
    }
    try {
      const prepared = await prepareGatewayGuardrails({
        db,
        workspaceId,
        sessionId,
      });
      if (
        requestedRevision !== String(prepared.session.runtime_revision) ||
        (agentId !== undefined && !prepared.agent_ids.includes(agentId))
      ) {
        return reply.code(409).send({ error: 'guardrail revision or agent mismatch' });
      }
      const bundle = effectiveGuardrailBundle({ prepared, organizationId, issuedAt: new Date() });
      return reply.code(200).send(bundle);
    } catch (error) {
      if (error instanceof PreparedExecutionNotFoundError) {
        return reply.code(404).send({ error: 'session not found' });
      }
      if (error instanceof InvalidRuntimeBindingError) {
        return reply.code(409).send({ error: 'invalid runtime binding' });
      }
      if (error instanceof RuntimeDependencyUnavailableError) {
        return reply.code(503).send({ error: 'guardrail bundle unavailable' });
      }
      throw error;
    }
  });

  app.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/context/resolve',
    async (req, reply) => {
      const params = InternalAgentObservabilityContextPathParamsSchema.safeParse(req.params);
      if (!params.success) {
        return reply.code(400).send({ error: 'invalid agent observability context request' });
      }
      if (!internalContract.resolveAgentObservabilityContext.body.safeParse(req.body).success) {
        return reply.code(400).send({ error: 'invalid agent observability context request' });
      }
      const { workspaceId, sessionId } = params.data;

      try {
        const context = await loadAgentObservabilitySessionContext({ db, workspaceId, sessionId });
        const parsed = AgentObservabilitySessionContextSchema.safeParse(context);
        if (!parsed.success) {
          return reply.code(503).send({ error: 'agent observability unavailable' });
        }
        return reply.code(200).send(parsed.data);
      } catch (error) {
        if (error instanceof AgentObservabilitySessionContextNotFoundError) {
          return reply.code(404).send({ error: 'not found' });
        }
        if (error instanceof AgentObservabilitySessionContextUnavailableError) {
          return reply.code(503).send({ error: 'agent observability unavailable' });
        }
        return reply.code(503).send({ error: 'agent observability unavailable' });
      }
    },
  );

  app.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/agent-observability/secret/resolve',
    async (req, reply) => {
      const params = InternalAgentObservabilitySecretPathParamsSchema.safeParse(req.params);
      if (!params.success) {
        return reply.code(400).send({ error: 'invalid agent observability secret request' });
      }
      if (!internalContract.resolveAgentObservabilitySecret.body.safeParse(req.body).success) {
        return reply.code(400).send({ error: 'invalid agent observability secret request' });
      }
      const actor = req.internalAuth?.subject;
      if (!actor) return reply.code(503).send({ error: 'agent observability unavailable' });
      const cancellation = requestAbortSignal(req, reply);

      try {
        const secret = await resolveAgentObservabilitySessionSecret({
          db,
          secretStore,
          workspaceId: params.data.workspaceId,
          sessionId: params.data.sessionId,
          actor,
          requestId: req.id,
          signal: cancellation.signal,
        });
        // Parse the exact secret-bearing response before Fastify serializes it.
        // Do not stringify or log this object: it contains credential material.
        const parsed = AgentObservabilitySecretResolutionSchema.safeParse(secret);
        if (!parsed.success) {
          return reply.code(503).send({ error: 'agent observability unavailable' });
        }
        return reply.code(200).send(parsed.data);
      } catch (error) {
        if (error instanceof AgentObservabilitySecretResolutionNotFoundError) {
          return reply.code(404).send({ error: 'not found' });
        }
        if (error instanceof AgentObservabilitySecretResolutionDeniedError) {
          return reply.code(409).send({ error: 'agent observability secret resolution denied' });
        }
        if (error instanceof AgentObservabilitySecretResolutionUnavailableError) {
          return reply.code(503).send({ error: 'agent observability unavailable' });
        }
        return reply.code(503).send({ error: 'agent observability unavailable' });
      } finally {
        cancellation.dispose();
      }
    },
  );

  app.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/mcp-destination/resolve',
    async (req, reply) => {
      const { workspaceId, sessionId } = req.params as {
        workspaceId: string;
        sessionId: string;
      };
      if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(sessionId)) {
        return reply.code(400).send({ error: 'invalid workspace or session id' });
      }
      const body = req.body;
      if (
        body === null ||
        typeof body !== 'object' ||
        Array.isArray(body) ||
        typeof (body as Record<string, unknown>)['backend'] !== 'string' ||
        ((body as Record<string, unknown>)['backend'] as string).length === 0 ||
        Object.keys(body).some((key) => key !== 'backend')
      ) {
        return reply.code(400).send({ error: 'invalid MCP destination resolve payload' });
      }
      try {
        return reply.send(
          await resolveMcpDestination({
            db,
            workspaceId,
            sessionId,
            backend: (body as { backend: string }).backend,
          }),
        );
      } catch (error) {
        if (error instanceof PreparedExecutionNotFoundError) {
          return reply.code(404).send({ error: 'not found' });
        }
        if (error instanceof InvalidRuntimeBindingError) {
          return reply.code(409).send({
            error: 'invalid_runtime_binding',
            resource_type: error.resourceType,
            resource_id: error.resourceId,
          });
        }
        throw error;
      }
    },
  );

  // Ownership must remain readable when preparing execution would fail (for
  // example, an archived Agent). A cold interrupt must not prepare a new turn.
  app.get(internalContract.executionOwner.path, async (req, reply) => {
    const { workspaceId, sessionId } = req.params as { workspaceId: string; sessionId: string };
    if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(sessionId)) {
      return reply.code(400).send({ error: 'invalid workspace or session id' });
    }
    try {
      const owner = await loadSessionExecutionOwner(db, workspaceId, sessionId);
      if (!owner) return reply.code(404).send({ error: 'session not found' });
      return reply.send({ owner });
    } catch {
      return reply.code(409).send({ error: 'session execution routing is unavailable' });
    }
  });

  app.post(
    internalContract.harnessTurn.path,
    { bodyLimit: 25 * 1024 * 1024 },
    async (req, reply) => {
      const params = internalContract.harnessTurn.pathParams.safeParse(req.params);
      const body = internalContract.harnessTurn.body.safeParse(req.body);
      if (!params.success || !isWorkspaceId(params.data.workspaceId) || !body.success)
        return reply.code(400).send({ error: 'invalid harness turn payload' });
      if (body.data.action.type === 'commit') {
        try {
          normalizeHarnessState(body.data.action.state);
        } catch {
          return reply.code(400).send({ error: 'invalid harness checkpoint' });
        }
      }
      try {
        return reply.send(await transitionHarnessTurn({ db, ...params.data, request: body.data }));
      } catch (error) {
        if (error instanceof HarnessTurnInvalidBindingError) {
          return reply.code(409).send({
            error: 'invalid_runtime_binding',
            resource_type: error.resourceType,
            resource_id: params.data.sessionId,
          });
        }
        if (error instanceof HarnessStateWriteError) {
          if (error.status === 404) return reply.code(404).send({ error: error.message });
          return reply.code(409).send({ error: error.message });
        }
        throw error;
      }
    },
  );

  app.post(
    internalContract.saveHarnessState.path,
    {
      // Native rollout files allow 24 MiB of base64, plus bounded envelope overhead.
      bodyLimit: 25 * 1024 * 1024,
    },
    async (req, reply) => {
      const params = internalContract.saveHarnessState.pathParams.safeParse(req.params);
      const body = internalContract.saveHarnessState.body.safeParse(req.body);
      if (!params.success || !isWorkspaceId(params.data.workspaceId) || !body.success) {
        return reply.code(400).send({ error: 'invalid harness checkpoint payload' });
      }
      let state: ReturnType<typeof normalizeHarnessState>;
      try {
        state = normalizeHarnessState(body.data.state);
      } catch {
        return reply.code(400).send({ error: 'invalid harness checkpoint' });
      }
      try {
        const revision = await saveHarnessState({
          db,
          ...params.data,
          runtimeRevision: body.data.runtime_revision,
          expectedCheckpointRevision: body.data.expected_checkpoint_revision,
          state,
        });
        return reply.send({ checkpoint_revision: revision });
      } catch (error) {
        if (error instanceof HarnessStateWriteError) {
          if (error.status === 404) return reply.code(404).send({ error: error.message });
          return reply.code(409).send({ error: error.message });
        }
        throw error;
      }
    },
  );

  app.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/executions:prepare',
    async (req, reply) => {
      const { workspaceId, sessionId } = req.params as {
        workspaceId: string;
        sessionId: string;
      };
      if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(sessionId)) {
        return reply.code(400).send({ error: 'invalid workspace or session id' });
      }
      try {
        return reply.send(
          await prepareExecution({
            db,
            fileStore,
            ...(memoryStore ? { memoryStore } : {}),
            workspaceId,
            sessionId,
            gatewayRegistryUsageEnabled,
          }),
        );
      } catch (error) {
        if (error instanceof PreparedExecutionNotFoundError) {
          return reply.code(404).send({ error: 'session not found' });
        }
        if (error instanceof InvalidRuntimeBindingError) {
          return reply.code(409).send({
            error: 'invalid_runtime_binding',
            resource_type: error.resourceType,
            resource_id: error.resourceId,
          });
        }
        if (error instanceof RuntimeDependencyUnavailableError) {
          return reply.code(503).send({ error: error.message });
        }
        throw error;
      }
    },
  );

  app.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/git-credentials/:id/resolve',
    async (req, reply) => {
      const { workspaceId, sessionId, id } = req.params as {
        workspaceId: string;
        sessionId: string;
        id: string;
      };
      if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(sessionId)) {
        return reply.code(400).send({ error: 'invalid workspace or session id' });
      }
      const session = await loadActiveRuntimeSession(db, workspaceId, sessionId);
      if (!session) return reply.code(404).send({ error: 'not found' });
      const resources = await db
        .select({ id: sessionResources.id, repoRef: sessionResources.repoRef })
        .from(sessionResources)
        .where(
          and(
            isNull(sessionResources.deletedAt),
            eq(sessionResources.workspaceId, workspaceId),
            eq(sessionResources.sessionId, sessionId),
            eq(sessionResources.type, 'github_repository'),
            isNull(sessionResources.detachedAt),
          ),
        );
      const attachedResources = resources.filter(
        ({ repoRef }) =>
          repoRef !== null &&
          typeof repoRef === 'object' &&
          !Array.isArray(repoRef) &&
          (repoRef as Record<string, unknown>).git_credential_id === id,
      );
      if (attachedResources.length === 0) return reply.code(404).send({ error: 'not found' });

      const rows = await db
        .select()
        .from(gitCredentials)
        .where(
          and(
            isNull(gitCredentials.deletedAt),
            eq(gitCredentials.workspaceId, workspaceId),
            eq(gitCredentials.id, id),
            isNull(gitCredentials.archivedAt),
          ),
        )
        .limit(1);
      const row = rows[0];
      if (!row) return reply.code(404).send({ error: 'not found' });

      const bindingMatches = attachedResources.some((resource) => {
        const ref = resource.repoRef as Record<string, unknown>;
        return (
          typeof ref.url === 'string' &&
          repositoryUrlsEqual(row.repoUrl, ref.url) &&
          (row.sessionResourceId === null || row.sessionResourceId === resource.id)
        );
      });
      // A credential id alone is not authority. Resource-owned credentials
      // must point back to the exact attached row, and both shared and owned
      // credentials must retain the repository URL binding validated at
      // session setup.
      if (!bindingMatches) return reply.code(404).send({ error: 'not found' });

      const secret = await secrets.resolve(row.secretRef);
      if (secret === null) return reply.code(404).send({ error: 'secret not resolvable' });

      return reply.send({
        git_credential_id: row.id,
        provider: row.provider,
        repo_url: row.repoUrl,
        secret_value: secret,
        ttl_seconds: 300,
      });
    },
  );

  /**
   * Internal environment lookup. Returns environment details for a given
   * `env_*` id without a workspace tenancy filter. Under Kubernetes workload
   * auth only the Harness identity may call it, but nothing in this repository
   * does: harness-server's dispatcher takes the session's Environment from the
   * prepared execution (`executions:prepare`) instead.
   *
   * Auth: the internal listener's `buildInternalAuth` (`src/auth/internal-auth.ts`)
   * requires a Kubernetes ServiceAccount token verified by TokenReview, checked
   * against per-route capabilities, or the shared internal service token.
   */
  app.get('/internal/environments/:id', async (req, reply) => {
    const { id } = req.params as { id: string };
    if (!ENV_ID_RE.test(id)) {
      return reply.code(400).send({ error: 'invalid environment id' });
    }
    const rows = await db
      .select()
      .from(environments)
      .where(and(isNull(environments.deletedAt), eq(environments.id, id)))
      .limit(1);
    const row = rows[0];
    if (!row) return reply.code(404).send({ error: 'environment not found' });
    return reply.send({
      id: row.id,
      workspace_id: row.workspaceId,
      name: row.name,
      packages: row.packages,
      networking: row.networking,
      image: row.image ?? null,
      target: row.target ?? null,
    });
  });

  /**
   * Internal env-key verification. Checks a raw `sk-…` env key — the one a
   * worker is handed at create/rotate — against the stored digest + expiry
   * (and rejects archived environments), with the same
   * `verifyEnvKeyForEnvironment` check the worker tunnel route runs in-process
   * when a worker dials. The flow is: look the environment up by id, then verify
   * the presented credential — failing closed on every non-match. Nothing in
   * this repository calls this route; the worker tunnel does not use it.
   *
   * Only the boolean outcome (+ the resolved workspace on success) is returned;
   * the stored digest is never echoed. Auth: the internal listener's workload
   * authentication — same boundary as the lookup above.
   */
  app.post('/internal/environments/:id/verify-key', async (req, reply) => {
    const { id } = req.params as { id: string };
    if (!ENV_ID_RE.test(id)) {
      return reply.code(400).send({ error: 'invalid environment id' });
    }
    const body = req.body as { env_key?: unknown };
    if (typeof body?.env_key !== 'string' || body.env_key.length === 0) {
      return reply.code(400).send({ error: 'env_key is required' });
    }
    const rows = await db
      .select()
      .from(environments)
      .where(and(isNull(environments.deletedAt), eq(environments.id, id)))
      .limit(1);
    const row = rows[0];
    // Unknown id is an auth failure, not a 404: the verify path must not leak
    // which environment ids exist, and an absent row simply fails closed.
    const valid = row
      ? verifyEnvKeyForEnvironment(
          body.env_key,
          { envKeyDigest: row.envKeyDigest, envKeyExpiresAt: row.envKeyExpiresAt },
          row.archivedAt !== null,
        )
      : false;
    if (!valid) return reply.send({ valid: false });
    return reply.send({ valid: true, workspace_id: row!.workspaceId });
  });

  /**
   * Durable environment-claim routes — the multi-replica equivalent of an
   * in-memory tunnel/host registry. An environment is claimed by exactly one
   * registry pod at a time so a worker's tunnel terminates on the replica that
   * owns its environment. Policy lives in `src/domain/environment-claims.ts`;
   * these handlers are thin.
   *
   * Body validation runs against the contract's own zod schemas
   * (`internalContract.*.body`) via the pure parsers in
   * `src/domain/internal-claim-requests.ts`, so the published contract is the
   * single source of truth for both the declared shape and the runtime check —
   * the handlers never re-implement field validation by hand. (The routes are
   * still hand-mounted rather than ts-rest-served, matching every other
   * `/internal/*` route.)
   *
   * Auth: the internal listener's workload authentication — same boundary as
   * the lookups above.
   *
   * NOTE: the static `/internal/environments/claims/reap` route is registered
   * before the parameterized `/internal/environments/:id/claim*` routes so
   * Fastify's router matches the literal `claims` segment first (a stray
   * `env_*` id can never shadow the reaper).
   */
  app.post('/internal/environments/claims/reap', async (req, reply) => {
    // Strict-empty body, validated against the contract's `reapEnvironmentClaims`
    // zod (the reaper takes no parameters — the TTL is server-configured).
    const parsed = parseReapBody(req.body);
    if (!parsed.ok) {
      return reply.code(400).send({ error: parsed.error });
    }
    const reaped = await claims.reapStale(claimTtlMs);
    return reply.send({ reaped });
  });

  // Newest-wins claim: a fresh claim unconditionally replaces any existing one,
  // so a reconnecting/relocated worker takes over from a pod that lagged on
  // cleanup. PUT because the call is idempotent for a given owner identity.
  app.put('/internal/environments/:id/claim', async (req, reply) => {
    const { id } = req.params as { id: string };
    if (!ENV_ID_RE.test(id)) {
      return reply.code(400).send({ error: 'invalid environment id' });
    }
    const parsed = parseClaimBody(req.body);
    if (!parsed.ok) {
      return reply.code(400).send({ error: parsed.error });
    }
    const claim = await claims.claim(id, parsed.value.owner_pod, parsed.value.worker_conn_id);
    return reply.send(claimToApi(claim));
  });

  // Connection-scoped heartbeat: a ping from a connection that has since been
  // taken over (newest-wins) is a no-op (`refreshed: false`) and must not
  // resurrect the stale owner.
  app.post('/internal/environments/:id/claim/heartbeat', async (req, reply) => {
    const { id } = req.params as { id: string };
    if (!ENV_ID_RE.test(id)) {
      return reply.code(400).send({ error: 'invalid environment id' });
    }
    const parsed = parseConnScopedBody(req.body);
    if (!parsed.ok) {
      return reply.code(400).send({ error: parsed.error });
    }
    const refreshed = await claims.heartbeat(id, parsed.value.worker_conn_id);
    return reply.send({ refreshed });
  });

  // Connection-scoped release (worker teardown): drops the row only when the
  // releasing worker still owns it. A worker whose claim was already taken over
  // (newest-wins) is a no-op (`released: false`), so its teardown can never
  // delete the live owner's claim — preserving the one-exclusive-claim
  // invariant from the new owner's perspective.
  app.post('/internal/environments/:id/claim/release', async (req, reply) => {
    const { id } = req.params as { id: string };
    if (!ENV_ID_RE.test(id)) {
      return reply.code(400).send({ error: 'invalid environment id' });
    }
    const parsed = parseConnScopedBody(req.body);
    if (!parsed.ok) {
      return reply.code(400).send({ error: parsed.error });
    }
    const released = await claims.release(id, parsed.value.worker_conn_id);
    return reply.send({ released });
  });

  app.get('/internal/environments/:id/claim', async (req, reply) => {
    const { id } = req.params as { id: string };
    if (!ENV_ID_RE.test(id)) {
      return reply.code(400).send({ error: 'invalid environment id' });
    }
    const claim = await claims.getOwner(id);
    return reply.send({ claim: claim ? claimToApi(claim) : null });
  });

  app.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/vault-credentials/:id/resolve',
    async (req, reply) => {
      const { workspaceId, sessionId, id } = req.params as {
        workspaceId: string;
        sessionId: string;
        id: string;
      };
      if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(sessionId)) {
        return reply.code(400).send({ error: 'invalid workspace or session id' });
      }

      const body = req.body;
      if (
        body === null ||
        typeof body !== 'object' ||
        Array.isArray(body) ||
        typeof (body as Record<string, unknown>)['credential_id'] !== 'string' ||
        typeof (body as Record<string, unknown>)['vault_id'] !== 'string' ||
        ((body as Record<string, unknown>)['vault_id'] as string).length === 0 ||
        ('force_refresh' in body &&
          typeof (body as Record<string, unknown>)['force_refresh'] !== 'boolean') ||
        Object.keys(body).some(
          (key) => !['credential_id', 'vault_id', 'force_refresh'].includes(key),
        )
      ) {
        return reply.code(400).send({ error: 'invalid credential resolve payload' });
      }
      const {
        credential_id: credentialId,
        vault_id: legacyVaultId,
        force_refresh: forceRefresh = false,
      } = body as {
        credential_id: string;
        vault_id: string;
        force_refresh?: boolean;
      };
      const requestedKind = classifyRequestedVaultCredentialId(id);
      if (
        requestedKind === null ||
        !hasCanonicalRequestedCredentialPath(req.raw.url, id) ||
        classifyRequestedVaultCredentialId(credentialId) === null ||
        classifyRequestedVaultCredentialId(legacyVaultId) === null
      ) {
        return reply.code(400).send({ error: 'invalid credential resolve id' });
      }
      // The gateway still sends `vault_id` as a legacy alias for the
      // credential id. Mismatches are authorization misses, not validation
      // oracles that can reveal whether any supplied identifier exists.
      if (credentialId !== id || legacyVaultId !== id) {
        return reply.code(404).send({ error: 'not found' });
      }

      const session = await loadActiveRuntimeSession(db, workspaceId, sessionId);
      if (!session) {
        return reply.code(404).send({ error: 'not found' });
      }
      if (session.vaultIds.length === 0) {
        if (requestedKind === 'logical') {
          await recordDeniedLogicalResolution(
            db,
            req,
            session,
            workspaceId,
            sessionId,
            id,
            'not_found',
          );
        }
        return reply.code(404).send({ error: 'not found' });
      }

      const resolved = await loadActiveRuntimeCredentialForRequest(
        db,
        workspaceId,
        id,
        requestedKind,
        session.vaultIds,
      );
      if (resolved.status !== 'resolved') {
        if (requestedKind === 'logical') {
          await recordDeniedLogicalResolution(
            db,
            req,
            session,
            workspaceId,
            sessionId,
            id,
            resolved.status,
          );
        }
        return reply.code(404).send({ error: 'not found' });
      }
      let row = resolved.row;

      let secret: string | null;
      if (forceRefresh && row.authType === 'mcp_oauth') {
        if (!secretStore) {
          return reply.code(503).send({ error: 'secret store unavailable' });
        }
        const refresh = await refreshCredentialWithLease(
          db,
          secretStore,
          secrets,
          oauthFetch,
          req.log,
          row,
          workspaceId,
          row.id,
          session.vaultIds,
        );
        if (refresh.status === 'not_found') return reply.code(404).send({ error: 'not found' });
        if (refresh.status === 'no_refresh_token') {
          return reply.code(409).send({ error: 'oauth refresh unavailable' });
        }
        if (refresh.status === 'secret_unavailable' || refresh.status === 'connect_error') {
          return reply.code(503).send({ error: 'oauth refresh unavailable' });
        }
        if (refresh.status === 'failed') {
          return reply.code(409).send({ error: 'oauth refresh rejected' });
        }
        if (refresh.status === 'conflict' && refresh.accessToken === null) {
          return reply.code(409).send({ error: 'credential was rotated concurrently' });
        }

        // Refresh can spend up to the outbound timeout away from Postgres.
        // Re-check live Session/Vault/credential scope before releasing the
        // result so an archive, termination, or binding removal that raced the
        // token exchange cannot receive newly rotated secret material.
        const currentSession = await loadActiveRuntimeSession(db, workspaceId, sessionId);
        if (!currentSession) return reply.code(404).send({ error: 'not found' });
        const current = await loadActiveRuntimeCredential(
          db,
          workspaceId,
          row.id,
          currentSession.vaultIds,
        );
        if (!current) return reply.code(404).send({ error: 'not found' });
        const currentSecret = await secrets.resolve(current.accessSecretRef);
        if (currentSecret === null) return reply.code(404).send({ error: 'not found' });
        row = current;
        secret = currentSecret;
      } else {
        secret = await secrets.resolve(row.accessSecretRef);
        // A concurrent rotation may swap the pointer and purge the old secret
        // after this request loaded the row. Re-read once before treating that
        // race as an unresolvable credential.
        if (secret === null) {
          const current = await loadActiveRuntimeCredentialForRequest(
            db,
            workspaceId,
            id,
            requestedKind,
            session.vaultIds,
          );
          if (
            current.status === 'resolved' &&
            current.row.accessSecretRef !== row.accessSecretRef
          ) {
            row = current.row;
            secret = await secrets.resolve(row.accessSecretRef);
          }
        }
        if (secret === null) {
          if (requestedKind === 'logical') {
            await recordDeniedLogicalResolution(
              db,
              req,
              session,
              workspaceId,
              sessionId,
              id,
              'secret_unavailable',
            );
          }
          return reply.code(404).send({ error: 'not found' });
        }
      }

      if (row.authType === 'provider') {
        const response = providerCredentialResolution(row, secret);
        if (!response) {
          if (requestedKind === 'logical') {
            await recordDeniedLogicalResolution(
              db,
              req,
              session,
              workspaceId,
              sessionId,
              id,
              'invalid_provider_credential',
            );
          }
          return reply.code(409).send({ error: 'provider credential resolution unavailable' });
        }
        if (requestedKind === 'logical') {
          try {
            await writeLogicalResolutionAudit(db, req, session, workspaceId, sessionId, id, {
              outcome: 'success',
              selectedCredentialId: response.credential_id,
              credentialVersion: response.version,
            });
          } catch (err) {
            req.log.error(
              { err, workspaceId, sessionId, requestedCredentialId: id },
              'failed to persist logical credential resolution audit',
            );
            return reply.code(503).send({ error: 'credential resolution audit unavailable' });
          }
        }
        return reply.send(response);
      }

      if (row.authType === 'environment_variable') {
        return reply.send({
          credential_id: row.id,
          vault_id: row.vaultId,
          version: '1',
          auth_type: 'environment_variable',
          secret_name: row.secretName,
          secret_value: secret,
          networking: row.networking,
          ttl_seconds: 300,
        });
      }

      return reply.send({
        credential_id: row.id,
        vault_id: row.vaultId,
        version: '1',
        scheme: 'bearer',
        secret_value: secret,
        ttl_seconds: 300,
      });
    },
  );

  app.patch('/internal/v1/workspaces/:workspaceId/sessions/:id/state', async (req, reply) => {
    const { workspaceId, id } = req.params as { workspaceId: string; id: string };
    if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(id)) {
      return reply.code(400).send({ error: 'invalid workspace or session id' });
    }
    const body = req.body as {
      status?: string;
      sandbox_handle_id?: string | null;
    };
    if (!body.status || !SESSION_STATUSES.has(body.status)) {
      return reply.code(400).send({ error: 'invalid session status' });
    }
    if (!(await loadActiveRuntimeSession(db, workspaceId, id))) {
      return reply.code(404).send({ error: 'session not found' });
    }

    const now = new Date();
    const hasSandboxHandle = Object.prototype.hasOwnProperty.call(body, 'sandbox_handle_id');
    const update = {
      status: body.status,
      ...(hasSandboxHandle ? { sandboxHandleId: body.sandbox_handle_id } : {}),
      ...(body.status === 'running'
        ? {
            startedAt: sql`coalesce(${sessions.startedAt}, ${now})`,
            lastActiveAt: sql`case when ${sessions.status} <> 'running' then ${now} else ${sessions.lastActiveAt} end`,
          }
        : {
            activeSeconds: sql`${sessions.activeSeconds} + case when ${sessions.status} = 'running' and ${sessions.lastActiveAt} is not null then greatest(0, floor(extract(epoch from (cast(${now} as timestamptz) - ${sessions.lastActiveAt})))::int) else 0 end`,
            lastActiveAt: now,
          }),
      updatedAt: now,
    };
    const rows = await db
      .update(sessions)
      .set(update)
      .where(
        and(
          isNull(sessions.deletedAt),
          eq(sessions.workspaceId, workspaceId),
          eq(sessions.id, id),
          isNull(sessions.archivedAt),
          ne(sessions.status, 'terminated'),
        ),
      )
      .returning({ id: sessions.id });
    if (rows.length === 0) return reply.code(404).send({ error: 'session not found' });

    const session = await loadInternalSession(db, workspaceId, id);
    return reply.send(session);
  });

  app.post('/internal/v1/workspaces/:workspaceId/sessions/:id/usage', async (req, reply) => {
    const { workspaceId, id } = req.params as { workspaceId: string; id: string };
    if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(id)) {
      return reply.code(400).send({ error: 'invalid workspace or session id' });
    }
    // Route authorization admits both producers because Harness is the safe
    // default and Gateway can be enabled for colocated sessions. Workload-aware
    // auth must still select exactly one configured writer for this Session.
    const authority = await loadAuthoritativeUsageCaller(
      db,
      workspaceId,
      id,
      gatewayRegistryUsageEnabled,
    );
    // Even the legacy shared token must not duplicate Registry-owned SDK accounting.
    if (authority.kind === 'found' && authority.registryOwned)
      return reply.code(403).send({ error: 'Registry owns Codex usage accounting' });
    if (req.internalAuth?.caller !== 'shared') {
      if (authority.kind === 'not_found') {
        return reply.code(404).send({ error: 'session not found' });
      }
      if (authority.kind === 'unavailable') {
        return reply.code(503).send({ error: 'session usage authority unavailable' });
      }
      if (req.internalAuth?.caller !== authority.caller) {
        return reply.code(403).send({ error: 'forbidden' });
      }
    }
    const body = req.body as {
      usage?: Record<string, unknown>;
      model?: unknown;
      provider?: unknown;
      thread_id?: unknown;
      subagent_id?: unknown;
      turn_event_id?: unknown;
      usage_event_id?: unknown;
    };
    const usage = parseUsageDelta(body.usage);
    if (!usage) {
      return reply.code(400).send({ error: 'invalid usage payload' });
    }
    const model = body.model;
    if (model !== undefined && (typeof model !== 'string' || model.length === 0)) {
      return reply.code(400).send({ error: 'invalid model' });
    }
    const reportedProvider = body.provider;
    if (
      reportedProvider !== undefined &&
      (typeof reportedProvider !== 'string' || reportedProvider.length === 0)
    ) {
      return reply.code(400).send({ error: 'invalid provider' });
    }
    // A report that names only the model id means the deployment's default
    // provider — which is what every report meant before the field existed.
    const provider = reportedProvider ?? SEED_PRICE_PROVIDER;
    const threadId = body.thread_id;
    if (threadId !== undefined && (typeof threadId !== 'string' || !THREAD_ID_RE.test(threadId))) {
      return reply.code(400).send({ error: 'invalid thread id' });
    }
    const subagentId = body.subagent_id;
    if (
      subagentId !== undefined &&
      (typeof subagentId !== 'string' || !AGENT_ID_RE.test(subagentId))
    ) {
      return reply.code(400).send({ error: 'invalid subagent id' });
    }
    const turnEventId = body.turn_event_id;
    if (
      turnEventId !== undefined &&
      (typeof turnEventId !== 'string' || !turnEventId.startsWith('evt_'))
    ) {
      return reply.code(400).send({ error: 'invalid turn event id' });
    }
    const usageEventId = body.usage_event_id;
    if (
      usageEventId !== undefined &&
      (typeof usageEventId !== 'string' || !usageEventId.startsWith('evt_'))
    ) {
      return reply.code(400).send({ error: 'invalid usage event id' });
    }

    const turnSubject =
      typeof turnEventId === 'string'
        ? await loadGuardrailTurnSubject(db, workspaceId, id, turnEventId)
        : null;
    if (typeof turnEventId === 'string' && turnSubject === null) {
      return reply.code(404).send({ error: 'turn event subject not found' });
    }

    // Price the delta at the rate that applies now, with the model that
    // produced it: a session that changes model mid-run is priced per portion,
    // and what is recorded never changes afterwards. A model with no price data
    // yields `null` — the tokens are still recorded, the cost is not, and
    // nothing anywhere substitutes zero for "unknown".
    const costNanoUsd =
      typeof model === 'string'
        ? await priceUsageDelta(db, workspaceId, provider, model, usage)
        : null;

    const now = new Date();
    const outcome = await db.transaction(async (tx) => {
      return persistUsage(tx, {
        workspaceId,
        sessionId: id,
        usage,
        costNanoUsd,
        now,
        turnSubject,
        ...(typeof threadId === 'string' ? { threadId } : {}),
        ...(typeof subagentId === 'string' ? { subagentId } : {}),
        ...(typeof usageEventId === 'string' ? { usageEventId } : {}),
      });
    });

    if (outcome === 'thread_not_found') return reply.code(404).send({ error: 'thread not found' });
    if (outcome === 'session_not_found') {
      return reply.code(404).send({ error: 'session not found' });
    }

    const session = await loadInternalSession(db, workspaceId, id);
    const guardrailUsageState = await loadGuardrailUsageState(
      db,
      workspaceId,
      id,
      typeof subagentId === 'string' ? subagentId : undefined,
    );
    const subjectWindowState =
      turnSubject === null
        ? {}
        : await loadGuardrailSubjectWindowState(db, workspaceId, turnSubject, utcDateWindow(now));
    return reply.send({
      ...session,
      guardrail_usage_state: guardrailUsageState,
      guardrail_subject_window_state: subjectWindowState,
    });
  });

  app.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:id/guardrail-subject-window',
    async (req, reply) => {
      const { workspaceId, id } = req.params as { workspaceId: string; id: string };
      if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(id)) {
        return reply.code(400).send({ error: 'invalid workspace or session id' });
      }
      const turnEventId = (req.body as { turn_event_id?: unknown } | null)?.turn_event_id;
      if (typeof turnEventId !== 'string' || !turnEventId.startsWith('evt_')) {
        return reply.code(400).send({ error: 'invalid turn event id' });
      }
      const subject = await loadGuardrailTurnSubject(db, workspaceId, id, turnEventId);
      if (subject === null) return reply.code(404).send({ error: 'turn event subject not found' });
      return reply.send({
        guardrail_subject_window_state: await loadGuardrailSubjectWindowState(
          db,
          workspaceId,
          subject,
          utcDateWindow(new Date()),
        ),
      });
    },
  );

  /**
   * Mesh-internal: flush guardrail state. Every update lands as an atomic SQL
   * delta — an increment is a conflicting upsert that adds — which is the whole
   * reason state is one row per key rather than a blob on the session. A blob
   * would need read-modify-write, and a runner respawn produces exactly that
   * race: the outgoing runner flushes after the incoming one has started.
   *
   * The batch is validated in full before anything is written, and applied in
   * one transaction. A partially applied flush would leave the caller unable to
   * say which counters moved.
   */
  app.post(
    '/internal/v1/workspaces/:workspaceId/sessions/:id/guardrail-state',
    async (req, reply) => {
      const { workspaceId, id } = req.params as { workspaceId: string; id: string };
      if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(id)) {
        return reply.code(400).send({ error: 'invalid workspace or session id' });
      }
      const parsed = parseGuardrailStateBody(req.body);
      if (!parsed.ok) return reply.code(400).send({ error: parsed.error });
      const { subject, window, updates } = parsed.batch;

      const now = new Date();
      const found = await db.transaction(async (tx) => {
        // Session-scoped rows carry a foreign key to the session, so resolving
        // it first turns a stale flush into a 404 rather than a constraint
        // error. A terminated or archived session still accepts one: a runner's
        // last flush routinely lands after its terminal state update.
        const sessionRows = await tx
          .select({ id: sessions.id })
          .from(sessions)
          .where(
            and(
              isNull(sessions.deletedAt),
              eq(sessions.workspaceId, workspaceId),
              eq(sessions.id, id),
            ),
          )
          .limit(1);
        if (sessionRows.length === 0) return false;
        if (
          updates.some(
            (update) =>
              update.scope === 'session' &&
              update.action === 'delete' &&
              update.key.startsWith('codex_sdk_usage_pending:'),
          )
        ) {
          await tx
            .select({ id: sessions.id })
            .from(sessions)
            .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, id)))
            .for('update');
          const state = await loadHarnessTurnState(tx as unknown as DbClient, workspaceId, id);
          if (state.ownershipRevision) return 'owned' as const;
        }

        for (const update of updates) {
          if (update.scope === 'session') {
            await applySessionStateUpdate(tx, workspaceId, id, update, now);
          } else {
            // Non-null by construction: a batch carrying a `subject_window`
            // update without both key parts was refused above.
            await applyCounterStateUpdate(tx, workspaceId, subject!, window!, update, now);
          }
        }
        return true;
      });
      if (!found) return reply.code(404).send({ error: 'session not found' });
      if (found === 'owned')
        return reply.code(409).send({ error: 'Codex pending usage is owned by its turn receipt' });

      return reply.send({ applied: updates.length });
    },
  );

  app.post('/internal/v1/workspaces/:workspaceId/sessions/:id/mint-jwt', async (req, reply) => {
    const { workspaceId, id } = req.params as { workspaceId: string; id: string };
    if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(id)) {
      return reply.code(400).send({ error: 'invalid workspace or session id' });
    }
    const body = (req.body ?? {}) as {
      mcp_server_names?: string[];
      vault_ids?: string[];
      // Optional. When set the minter overrides the configured
      // default audience (e.g. `'git-creds'`) and embeds `repo_urls` as a
      // custom claim. Omitting both yields the default ai-gateway-bound JWT.
      audience?: string;
      repo_urls?: string[];
    };
    const session = await loadActiveRuntimeSession(db, workspaceId, id);
    if (!session) return reply.code(404).send({ error: 'session not found' });
    const mintOpts: { audience?: string; repoUrls?: string[]; ttlSecs?: number } = {};
    if (typeof body.audience === 'string' && body.audience.length > 0) {
      mintOpts.audience = body.audience;
    }
    if (Array.isArray(body.repo_urls) && body.repo_urls.length > 0) {
      mintOpts.repoUrls = body.repo_urls.filter(
        (u): u is string => typeof u === 'string' && u.length > 0,
      );
    }
    // Never trust caller-supplied credential ids. The ai-gateway may use this
    // startup snapshot to authorize supplied header metadata and exact static
    // destinations, so derive it only from persisted Session Vault bindings.
    const sessionVaultIds = session.vaultIds;
    const mcpBinding = await intersectEffectiveMcpServerNames(
      db,
      workspaceId,
      session,
      body.mcp_server_names,
    );
    if (mcpBinding === null) {
      return reply.code(404).send({ error: 'session not found' });
    }
    const mcpServerNames = mcpBinding.names;
    const credentialIds = await listActiveCredentialIds(db, workspaceId, sessionVaultIds);
    // These claims describe the persisted Session and its pinned primary Agent.
    // Caller-provided mint-jwt body fields must not influence policy identity.
    const snapshotGuardrailIds = Array.isArray(mcpBinding.snapshot.guardrail_ids)
      ? mcpBinding.snapshot.guardrail_ids.filter(
          (value): value is string => typeof value === 'string',
        )
      : [];
    const guardrailIds = [
      ...new Set([...guardrailIdsFromOverrides(session.agentOverrides), ...snapshotGuardrailIds]),
    ];
    const includeGuardrailClaims = minter.audienceFor(mintOpts) === 'ai-gateway';
    const includeLlmClaims =
      sessionJwtLlmPolicy !== undefined && minter.audienceFor(mintOpts) === 'ai-gateway';
    let llmRoutes = sessionJwtLlmPolicy?.routes;
    if (includeLlmClaims) {
      const selection = await loadSessionHarnessBinding(
        db,
        workspaceId,
        session.agentId,
        session.agentVersion,
      );
      // The SDK keeps one bearer token throughout its bounded ten-minute turn,
      // including time parked on client tools. Derive the lifetime from the
      // persisted harness, never a caller-supplied TTL or provider claim.
      if (selection.harness === 'codex_sdk' || selection.harness === 'pi_sdk')
        mintOpts.ttlSecs = 660;
      if (selection.harness === 'pi_sdk') {
        const override = (session.agentOverrides as { model?: unknown } | null)?.model;
        const model = PreparedAgentSnapshotSchema.shape.model.safeParse(
          override ?? mcpBinding.snapshot.model,
        );
        if (!model.success) return reply.code(409).send({ error: 'invalid_runtime_binding' });
        const route = piLlmRoute(model.data.provider, model.data.id);
        if (!sessionJwtLlmPolicy.routes.includes(route))
          return reply
            .code(403)
            .send({ error: `pi_sdk requires ${route} in SESSION_JWT_LLM_ROUTES` });
        llmRoutes = [route];
      }
    }
    let llmModels: string[] | undefined;
    if (includeLlmClaims) {
      try {
        llmModels = intersectLlmModels(
          sessionJwtLlmPolicy.models,
          await loadEffectiveSessionAgentModels({
            db,
            workspaceId,
            session,
            primarySnapshot: mcpBinding.snapshot,
          }),
        );
      } catch (error) {
        if (error instanceof InvalidRuntimeBindingError) {
          return reply.code(409).send({
            error: 'invalid_runtime_binding',
            resource_type: error.resourceType,
            resource_id: error.resourceId,
          });
        }
        throw error;
      }
    }
    const minted = await minter.mint(
      {
        org_id: session.organizationId,
        workspace_id: workspaceId,
        session_id: id,
        mcp_server_names: mcpServerNames,
        vault_ids: sessionVaultIds,
        credential_ids: credentialIds,
        ...(includeGuardrailClaims
          ? {
              agent_id: session.agentId,
              guardrail_ids: guardrailIds,
              runtime_config_revision: String(session.runtimeRevision),
            }
          : {}),
        ...(includeLlmClaims
          ? {
              llm_routes: llmRoutes,
              ...(llmModels !== undefined ? { llm_models: llmModels } : {}),
            }
          : {}),
      },
      mintOpts,
    );
    reply.send({ token: minted.token, expires_at: minted.expiresAt });
  });

  /**
   * Mesh-internal: register a blob written into S3 by an agent's FUSE-mounted
   * output prefix as a `File` row. Multipart body mirrors the public
   * `POST /v1/files` shape so we can reuse the parser, but ownership and
   * output metadata are derived from the scoped path: callers cannot choose
   * workspace, scope, purpose, or downloadability.
   *
   * Auth: the internal listener's workload authentication
   * (`src/auth/internal-auth.ts`); under Kubernetes workload auth only the
   * Harness identity may call this route.
   */
  app.post('/internal/v1/workspaces/:workspaceId/sessions/:sessionId/files', async (req, reply) => {
    const { workspaceId, sessionId } = req.params as {
      workspaceId: string;
      sessionId: string;
    };
    if (!isWorkspaceId(workspaceId) || !SESSION_ID_RE.test(sessionId)) {
      return reply.code(400).send({ error: 'invalid workspace or session id' });
    }
    const session = await loadActiveRuntimeSession(db, workspaceId, sessionId);
    if (!session) return reply.code(404).send({ error: 'session not found' });

    const partsIterable = (
      req as unknown as {
        parts: (opts?: { limits?: { fileSize?: number } }) => AsyncIterableIterator<{
          type: 'file' | 'field';
          filename?: string;
          mimetype?: string;
          fieldname: string;
          file?: NodeJS.ReadableStream;
          toBuffer?: () => Promise<Buffer>;
          value?: unknown;
        }>;
      }
    ).parts({ limits: { fileSize: MAX_FILE_BYTES } });

    const metadata: Record<string, string> = {};
    let filename: string | null = null;
    let mimeType = 'application/octet-stream';
    let fileBuffer: Buffer | null = null;

    try {
      for await (const part of partsIterable) {
        if (part.type === 'file' && part.fieldname === 'file') {
          if (fileBuffer !== null) {
            // Defensive: ignore duplicate `file` parts (one file per upload).
            continue;
          }
          if (!part.toBuffer) {
            return reply.code(400).send({ error: 'missing file stream' });
          }
          filename = part.filename ?? 'untitled';
          mimeType = part.mimetype ?? mimeType;
          fileBuffer = await part.toBuffer();
          continue;
        }
        if (part.type === 'field' && typeof part.value === 'string') {
          if (part.fieldname.startsWith('meta_')) {
            metadata[part.fieldname.slice(5)] = part.value;
            continue;
          }
          return reply.code(400).send({ error: `unexpected field ${part.fieldname}` });
        }
      }
    } catch (err) {
      const e = err as { code?: string; statusCode?: number; name?: string };
      if (e.code === 'FST_REQ_FILE_TOO_LARGE' || e.statusCode === 413) {
        return reply.code(413).send({ error: `file exceeds ${MAX_FILE_BYTES} byte limit` });
      }
      throw err;
    }

    if (fileBuffer === null || filename === null) {
      return reply.code(400).send({ error: 'no file part provided' });
    }

    const created = await fileStore.create({
      workspaceId,
      filename,
      mimeType,
      metadata,
      content: Readable.from(fileBuffer),
      purpose: 'agent_output',
      scopeId: sessionId,
      downloadable: true,
    });
    return reply.code(201).send(toApi(created));
  });

  const memoryBase =
    '/internal/v1/workspaces/:workspaceId/sessions/:sessionId/memory-stores/:storeId';

  app.get(`${memoryBase}/memories`, async (req, reply) => {
    if (!memoryStore) return reply.code(503).send({ error: 'memory_store not configured' });
    const params = req.params as { workspaceId: string; sessionId: string; storeId: string };
    if (!validMemoryRouteParams(params)) {
      return reply.code(400).send({ error: 'invalid workspace, session, or memory store id' });
    }
    const [store, access] = await Promise.all([
      memoryStore.getStore(params.workspaceId, params.storeId),
      loadAttachedMemoryAccess(db, params.workspaceId, params.sessionId, params.storeId),
    ]);
    if (!store || !access) return reply.code(404).send({ error: 'memory_store not found' });
    const rows = await memoryStore.listMemories(params.workspaceId, params.storeId);
    return reply.send({ data: rows.map(memoryToApi), next_page: null });
  });

  app.get(`${memoryBase}/memories/:memoryId/content`, async (req, reply) => {
    if (!memoryStore) return reply.code(503).send({ error: 'memory_store not configured' });
    const params = req.params as {
      workspaceId: string;
      sessionId: string;
      storeId: string;
      memoryId: string;
    };
    if (!validMemoryRouteParams(params) || !/^mem_[A-Za-z0-9_-]+$/.test(params.memoryId)) {
      return reply.code(400).send({ error: 'invalid memory route' });
    }
    const [store, access] = await Promise.all([
      memoryStore.getStore(params.workspaceId, params.storeId),
      loadAttachedMemoryAccess(db, params.workspaceId, params.sessionId, params.storeId),
    ]);
    if (!store || !access) return reply.code(404).send({ error: 'memory_store not found' });
    const opened = await memoryStore.openMemory(
      params.workspaceId,
      params.storeId,
      params.memoryId,
    );
    if (!opened) return reply.code(404).send({ error: 'memory not found' });
    reply.header('content-type', 'application/octet-stream');
    reply.header('content-length', String(opened.sizeBytes));
    return reply.send(opened.stream);
  });

  app.get(`${memoryBase}/memory-versions`, async (req, reply) => {
    if (!memoryStore) return reply.code(503).send({ error: 'memory_store not configured' });
    const params = req.params as { workspaceId: string; sessionId: string; storeId: string };
    const { memory_id: memoryId } = req.query as { memory_id?: string };
    if (
      !validMemoryRouteParams(params) ||
      typeof memoryId !== 'string' ||
      !/^mem_[A-Za-z0-9_-]+$/.test(memoryId)
    ) {
      return reply.code(400).send({ error: 'invalid memory route' });
    }
    const [store, access, memory] = await Promise.all([
      memoryStore.getStore(params.workspaceId, params.storeId),
      loadAttachedMemoryAccess(db, params.workspaceId, params.sessionId, params.storeId),
      memoryStore.getMemory(params.workspaceId, params.storeId, memoryId),
    ]);
    if (!store || !access) return reply.code(404).send({ error: 'memory_store not found' });
    if (!memory) return reply.code(404).send({ error: 'memory not found' });
    const rows = await memoryStore.listVersions(params.workspaceId, params.storeId, memoryId);
    return reply.send({ data: rows.map(versionToApi), next_page: null });
  });

  /**
   * Register a version observed on an attached memory store. Workspace, store,
   * and writer session are derived from the scoped path; the caller cannot
   * redirect a write by supplying tenant identifiers in the body.
   */
  app.post(`${memoryBase}/memory-versions`, async (req, reply) => {
    if (!memoryStore) return reply.code(503).send({ error: 'memory_store not configured' });
    const params = req.params as { workspaceId: string; sessionId: string; storeId: string };
    if (!validMemoryRouteParams(params)) {
      return reply.code(400).send({ error: 'invalid workspace, session, or memory store id' });
    }
    const [store, access] = await Promise.all([
      memoryStore.getStore(params.workspaceId, params.storeId),
      loadAttachedMemoryAccess(db, params.workspaceId, params.sessionId, params.storeId),
    ]);
    if (!store || !access) return reply.code(404).send({ error: 'memory_store not found' });
    if (access !== 'read_write') {
      return reply.code(403).send({ error: 'memory_store is attached read-only' });
    }

    const body = req.body as {
      path?: string;
      content_base64?: string;
      content_sha256?: string;
      previous_sha256?: string | null;
      version_id?: string;
      written_by_event_id?: string;
    };
    const path = typeof body?.path === 'string' ? normalizeMemoryPathOrNull(body.path) : null;
    if (
      !body ||
      path === null ||
      typeof body.content_base64 !== 'string' ||
      typeof body.content_sha256 !== 'string' ||
      !/^[0-9a-f]{64}$/.test(body.content_sha256) ||
      (body.previous_sha256 !== undefined &&
        body.previous_sha256 !== null &&
        !/^[0-9a-f]{64}$/.test(body.previous_sha256))
    ) {
      return reply.code(400).send({ error: 'invalid memory version payload' });
    }

    const buf = Buffer.from(body.content_base64, 'base64');
    if (buf.length > MAX_MEMORY_BYTES) {
      return reply
        .code(413)
        .send({ error: `memory content exceeds ${MAX_MEMORY_BYTES} byte limit` });
    }

    const write = async (includePrecondition: boolean, conflictVersion: boolean) =>
      memoryStore.writeMemory({
        workspaceId: params.workspaceId,
        storeId: params.storeId,
        path,
        content: Readable.from(buf),
        sizeBytes: buf.length,
        sha256: body.content_sha256!,
        ...(includePrecondition && body.previous_sha256 !== null
          ? { previousSha256: body.previous_sha256 }
          : {}),
        ...(body.version_id
          ? { versionId: conflictVersion ? `${body.version_id}_conflict` : body.version_id }
          : {}),
        writtenBySessionId: params.sessionId,
        ...(body.written_by_event_id ? { writtenByEventId: body.written_by_event_id } : {}),
      });

    try {
      const result = await write(body.previous_sha256 !== undefined, false);
      return reply.code(201).send({
        memory: memoryToApi(result.memory),
        version: versionToApi(result.version),
        conflict: false,
      });
    } catch (error) {
      if (!(error instanceof MemoryConflictError)) {
        const message = (error as Error).message;
        if (message.includes('sha256') || message.includes('size')) {
          return reply.code(400).send({ error: message });
        }
        throw error;
      }
    }

    const result = await write(false, true);
    return reply.code(201).send({
      memory: memoryToApi(result.memory),
      version: versionToApi(result.version),
      conflict: true,
    });
  });
}

function validMemoryRouteParams(params: {
  workspaceId: string;
  sessionId: string;
  storeId: string;
}): boolean {
  return (
    isWorkspaceId(params.workspaceId) &&
    SESSION_ID_RE.test(params.sessionId) &&
    /^mems_[A-Za-z0-9_-]+$/.test(params.storeId)
  );
}

async function loadAttachedMemoryAccess(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  storeId: string,
): Promise<'read_only' | 'read_write' | null> {
  const rows = await db
    .select({ access: sessionResources.access })
    .from(sessionResources)
    .innerJoin(
      sessions,
      and(
        isNull(sessions.deletedAt),
        eq(sessions.workspaceId, sessionResources.workspaceId),
        eq(sessions.id, sessionResources.sessionId),
      ),
    )
    .innerJoin(workspaces, eq(workspaces.id, sessions.workspaceId))
    .where(
      and(
        isNull(sessionResources.deletedAt),
        eq(sessionResources.workspaceId, workspaceId),
        eq(sessionResources.sessionId, sessionId),
        eq(sessionResources.type, 'memory_store'),
        eq(sessionResources.memoryStoreId, storeId),
        isNull(sessionResources.detachedAt),
        isNull(sessions.archivedAt),
        ne(sessions.status, 'terminated'),
        eq(workspaces.status, 'active'),
      ),
    )
    .limit(1);
  const access = rows[0]?.access;
  return access === 'read_only' || access === 'read_write' ? access : null;
}

interface ActiveRuntimeSession {
  id: string;
  agentId: string;
  agentVersion: number;
  runtimeRevision: number;
  mcpServers: unknown;
  vaultIds: string[];
  agentOverrides: unknown;
  organizationId: string;
}

function guardrailIdsFromOverrides(raw: unknown): string[] {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return [];
  const ids = (raw as Record<string, unknown>)['guardrailIds'];
  return Array.isArray(ids)
    ? ids.filter((value): value is string => typeof value === 'string')
    : [];
}

async function loadActiveRuntimeSession(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
): Promise<ActiveRuntimeSession | null> {
  const rows = await db
    .select({
      id: sessions.id,
      agentId: sessions.agentId,
      agentVersion: sessions.agentVersion,
      runtimeRevision: sessions.runtimeRevision,
      mcpServers: sessions.mcpServers,
      vaultIds: sessions.vaultIds,
      agentOverrides: sessions.agentOverrides,
      organizationId: workspaces.organizationId,
    })
    .from(sessions)
    .innerJoin(workspaces, eq(workspaces.id, sessions.workspaceId))
    .where(
      and(
        isNull(sessions.deletedAt),
        eq(sessions.workspaceId, workspaceId),
        eq(sessions.id, sessionId),
        isNull(sessions.archivedAt),
        ne(sessions.status, 'terminated'),
        eq(workspaces.status, 'active'),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

type RuntimeCredentialReadDb = Pick<DbClient, 'select'>;

async function loadActiveRuntimeCredential(
  db: RuntimeCredentialReadDb,
  workspaceId: string,
  credentialId: string,
  vaultIds: string[],
): Promise<CredentialRow | null> {
  if (vaultIds.length === 0) return null;
  const rows = await db
    .select()
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.id, credentialId),
        inArray(vaultCredentials.vaultId, vaultIds),
        isNull(vaultCredentials.archivedAt),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function loadActiveRuntimeCredentialForRequest(
  db: RuntimeCredentialReadDb,
  workspaceId: string,
  requestedCredentialId: string,
  kind: 'concrete' | 'logical',
  vaultIds: string[],
): Promise<LogicalCredentialSelection<CredentialRow>> {
  if (kind === 'concrete') {
    const row = await loadActiveRuntimeCredential(db, workspaceId, requestedCredentialId, vaultIds);
    return row ? { status: 'resolved', row } : { status: 'not_found' };
  }
  if (vaultIds.length === 0) return { status: 'not_found' };
  const rows = await db
    .select()
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        eq(vaultCredentials.authType, 'provider'),
        eq(vaultCredentials.logicalId, requestedCredentialId),
        inArray(vaultCredentials.vaultId, vaultIds),
        isNull(vaultCredentials.archivedAt),
      ),
    )
    .limit(2);
  return selectAuthorizedLogicalCredential(rows, {
    workspaceId,
    vaultIds,
    requestedCredentialId,
  });
}

async function writeLogicalResolutionAudit(
  db: Pick<DbClient, 'insert'>,
  req: FastifyRequest,
  session: ActiveRuntimeSession,
  workspaceId: string,
  sessionId: string,
  requestedCredentialId: string,
  input: LogicalResolutionAuditInput,
): Promise<void> {
  const actor = req.internalAuth?.subject;
  if (!actor) {
    throw new Error('internal authentication context unavailable for credential resolution audit');
  }
  await db.insert(adminAuditEvents).values({
    id: newId('audit'),
    organizationId: session.organizationId,
    workspaceId,
    actor,
    authMethod: 'internal-service',
    action: 'vault_credential.logical_resolution',
    targetType: 'vault_credential_alias',
    targetId: requestedCredentialId,
    requestId: req.id,
    result: input.outcome,
    metadata: logicalResolutionAuditMetadata(sessionId, requestedCredentialId, input),
  });
}

async function recordDeniedLogicalResolution(
  db: Pick<DbClient, 'insert'>,
  req: FastifyRequest,
  session: ActiveRuntimeSession,
  workspaceId: string,
  sessionId: string,
  requestedCredentialId: string,
  reason: Extract<LogicalResolutionAuditInput, { outcome: 'denied' }>['reason'],
): Promise<void> {
  try {
    await writeLogicalResolutionAudit(
      db,
      req,
      session,
      workspaceId,
      sessionId,
      requestedCredentialId,
      { outcome: 'denied', reason },
    );
  } catch (err) {
    req.log.warn(
      { err, workspaceId, sessionId, requestedCredentialId, reason },
      'failed to persist denied logical credential resolution audit',
    );
  }
}

async function intersectEffectiveMcpServerNames(
  db: DbClient,
  workspaceId: string,
  session: ActiveRuntimeSession,
  requested: unknown,
): Promise<{ names: string[]; snapshot: PreparedAgentSnapshot } | null> {
  const rows = await db
    .select({ snapshot: agentVersions.snapshot })
    .from(agentVersions)
    .innerJoin(
      agents,
      and(
        isNull(agents.deletedAt),
        eq(agents.workspaceId, agentVersions.workspaceId),
        eq(agents.id, agentVersions.agentId),
      ),
    )
    .where(
      and(
        eq(agentVersions.workspaceId, workspaceId),
        eq(agentVersions.agentId, session.agentId),
        eq(agentVersions.version, session.agentVersion),
        isNull(agents.archivedAt),
      ),
    )
    .limit(1);
  const parsed = PreparedAgentSnapshotSchema.safeParse(rows[0]?.snapshot);
  if (!parsed.success) return null;

  const effective = mcpServerNames(
    session.mcpServers === null ? parsed.data.mcp_servers : session.mcpServers,
  );
  if (effective === null) return null;
  const allowed = new Set(effective);
  const requestedNames = Array.isArray(requested)
    ? requested.filter((name): name is string => typeof name === 'string')
    : [];
  return {
    names: [...new Set(requestedNames)].filter((name) => allowed.has(name)),
    snapshot: parsed.data,
  };
}

function mcpServerNames(raw: unknown): string[] | null {
  if (!Array.isArray(raw)) return null;
  const names: string[] = [];
  for (const entry of raw) {
    if (
      entry === null ||
      typeof entry !== 'object' ||
      Array.isArray(entry) ||
      typeof (entry as Record<string, unknown>)['name'] !== 'string' ||
      ((entry as Record<string, unknown>)['name'] as string).length === 0
    ) {
      return null;
    }
    names.push((entry as Record<string, unknown>)['name'] as string);
  }
  return [...new Set(names)];
}

function normalizeMemoryPathOrNull(path: string): string | null {
  try {
    return normalizeMemoryRelativePath(path);
  } catch {
    return null;
  }
}

async function listActiveCredentialIds(
  db: DbClient,
  workspaceId: string,
  vaultIds: string[],
): Promise<string[]> {
  if (vaultIds.length === 0) return [];
  const rows = await db
    .select({ id: vaultCredentials.id })
    .from(vaultCredentials)
    .where(
      and(
        isNull(vaultCredentials.deletedAt),
        eq(vaultCredentials.workspaceId, workspaceId),
        inArray(vaultCredentials.vaultId, vaultIds),
        isNull(vaultCredentials.archivedAt),
      ),
    );
  return rows.map((row) => row.id);
}
