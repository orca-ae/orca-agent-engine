// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance } from 'fastify';
import { isDeepStrictEqual } from 'node:util';
import { eq, and, or, isNull, inArray, desc, lt, gte, lte, sql, type SQL } from 'drizzle-orm';
import type { z } from 'zod';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  guardrails,
  skills,
  skillVersions,
  workspaces,
} from '../persistence/postgres/schema.js';
import { newId } from '../domain/versioning.js';
import { toCanonicalToolName, toAnthropicWireToolName } from '../contracts/toolset-aliasing.js';
import {
  AgentCreate,
  AgentUpdate,
  CustomSkillVersionSelector,
  MAX_AGENT_SKILL_REFS,
  MAX_AGENT_SKILL_REFS_ERROR,
} from '../contracts/agents.contract.js';
import { toInternalId } from '../contracts/id-prefix.js';
import {
  modelToApi,
  normalizeModelForStorage,
  validateModelControlsForStorage,
  validateModelRosterForStorage,
  type StoredModel,
} from '../contracts/model-wire.js';
import {
  defaultHarnessModelProvider,
  validateHarnessFeatures,
  validateHarnessModel,
  validateHarnessUpdate,
  resolveHarnessAnnotation,
} from '@orca/harness-catalog';
import {
  decodeCreatedAtCursor,
  encodeCreatedAtCursor,
  parsePositiveIntQueryParam,
} from './query-params.js';
import { stripMcpServerPermissionPolicyFields } from './mcp-servers.js';
import {
  applyMetadataPatch,
  normalizeStoredMetadata,
  toJsonMetadata,
  validateMetadataLimits,
  type MetadataPatch,
} from './metadata.js';
import {
  findSkillReferenceConflict,
  type ResolvedSkillReference,
} from '../domain/skill-reference-conflicts.js';
import {
  validateAgentConfiguration,
  type AgentTool,
  type CanonicalMultiagent,
  type CanonicalMultiagentAgentRef,
} from '../domain/agent-version-configuration.js';

export {
  validateAgentConfiguration,
  type AgentTool,
  type CanonicalMultiagent,
  type CanonicalMultiagentAgentRef,
} from '../domain/agent-version-configuration.js';

const AGENT_LIST_PAGE_DEFAULT = 100;
const AGENT_LIST_PAGE_MAX = 100;
const AGENT_VERSIONS_PAGE_DEFAULT = 100;
const AGENT_VERSIONS_PAGE_MAX = 100;

type AgentCreateBody = z.infer<typeof AgentCreate>;
type AgentUpdateBody = z.infer<typeof AgentUpdate>;

function isOrcaBetaRequest(headers: Record<string, string | string[] | undefined>): boolean {
  const value = headers['orca-beta'];
  return Array.isArray(value) ? value.length > 0 : typeof value === 'string' && value.length > 0;
}

export function registerAgentsRoutes(
  app: FastifyInstance,
  db: DbClient,
  batchReadsEnabled = true,
): void {
  app.post('/v1/agents', async (req, reply) => {
    const auth = req.auth!;
    const orcaBeta = isOrcaBetaRequest(req.headers);
    const createValidation = validateAgentCreateBody(req.body);
    if ('error' in createValidation) return reply.code(400).send({ error: createValidation.error });
    const body = createValidation.value;
    const harnessCheck = resolveHarnessAnnotation(body.metadata);
    if ('error' in harnessCheck) return reply.code(400).send({ error: harnessCheck.error });
    const parsedModel = normalizeModelForStorage(
      body.model,
      defaultHarnessModelProvider(harnessCheck.harness),
    );
    if ('error' in parsedModel) return reply.code(400).send({ error: parsedModel.error });
    const normalizedModel = validateModelControlsForStorage(parsedModel, harnessCheck.harness);
    if ('error' in normalizedModel) return reply.code(400).send({ error: normalizedModel.error });
    const modelError = validateHarnessModel(body.metadata, normalizedModel);
    if (modelError) return reply.code(400).send({ error: modelError });
    const featureError = validateHarnessFeatures(body.metadata, body);
    if (featureError) return reply.code(400).send({ error: featureError });
    const agentId = newId('agt');
    const versionId = newId('agtv');
    const now = new Date();

    const normalizedTools = (body.tools ?? []).map((t: AgentTool) => ({
      ...t,
      type: toCanonicalToolName(t.type),
    }));
    const normalizedMcpServers = stripMcpServerPermissionPolicyFields(body.mcp_servers);
    const configurationError = validateAgentConfiguration(normalizedTools, normalizedMcpServers);
    if (configurationError) return reply.code(400).send({ error: configurationError });
    const validatedSkills = await validateSkillRefs(db, auth.workspaceId, body.skills ?? []);
    if ('error' in validatedSkills) return reply.code(400).send({ error: validatedSkills.error });
    const validatedGuardrails = await validateGuardrailRefs(
      db,
      auth.workspaceId,
      body.guardrail_ids,
    );
    if ('error' in validatedGuardrails)
      return reply.code(400).send({ error: validatedGuardrails.error });
    const resolvedMultiagent = await resolveMultiagentForStorage(
      db,
      auth.workspaceId,
      agentId,
      1,
      body.multiagent,
    );
    if (resolvedMultiagent && 'error' in resolvedMultiagent)
      return reply.code(400).send({ error: resolvedMultiagent.error });
    const modelRosterError = await validateMultiagentModelRoster(
      db,
      auth.workspaceId,
      agentId,
      1,
      normalizedModel,
      resolvedMultiagent,
      harnessCheck.harness,
    );
    if (modelRosterError) return reply.code(400).send({ error: modelRosterError });

    const metadata = body.metadata ?? {};

    const snapshot = {
      harness_type: harnessCheck.harness,
      id: agentId,
      name: body.name,
      description: body.description ?? null,
      version: 1,
      model: normalizedModel,
      system: body.system ?? null,
      tools: normalizedTools,
      mcp_servers: normalizedMcpServers,
      skills: validatedSkills,
      guardrail_ids: validatedGuardrails,
      metadata,
      multiagent: resolvedMultiagent,
    };

    await db.transaction(async (tx) => {
      await tx.insert(agents).values({
        id: agentId,
        workspaceId: auth.workspaceId,
        name: body.name,
        description: body.description ?? null,
        version: 1,
        latestVersionId: null,
        harnessType: harnessCheck.harness,
        modelProvider: normalizedModel.provider,
        modelId: normalizedModel.id,
        modelSpeed: normalizedModel.speed ?? null,
        modelEffort: normalizedModel.effort ?? null,
        system: body.system ?? null,
        tools: normalizedTools,
        mcpServers: normalizedMcpServers,
        skills: validatedSkills,
        guardrailIds: validatedGuardrails,
        metadata,
        multiagent: resolvedMultiagent,
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      });
      await tx.insert(agentVersions).values({
        id: versionId,
        workspaceId: auth.workspaceId,
        agentId,
        version: 1,
        snapshot,
        createdAt: now,
      });
      await tx
        .update(agents)
        .set({ latestVersionId: versionId })
        .where(
          and(
            isNull(agents.deletedAt),
            eq(agents.workspaceId, auth.workspaceId),
            eq(agents.id, agentId),
          ),
        );
    });

    const out = await loadAgent(db, auth.workspaceId, agentId, orcaBeta);
    // 200, not 201: Anthropic answers 200 from every create on this surface, and
    // the status of a successful call is something a client can observe.
    return reply.code(200).send(out);
  });

  app.get('/v1/agents', async (req, reply) => {
    const auth = req.auth!;
    const q = req.query as {
      limit?: string;
      page?: string;
      include_archived?: string;
      'created_at[gte]'?: string;
      'created_at[lte]'?: string;
    };
    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: AGENT_LIST_PAGE_DEFAULT,
      max: AGENT_LIST_PAGE_MAX,
    });
    if (!parsedLimit.ok) return reply.code(400).send({ error: parsedLimit.error });
    const includeArchived = parseOptionalBoolean(q.include_archived);
    if ('error' in includeArchived) return reply.code(400).send({ error: includeArchived.error });
    const createdGte = parseOptionalTimestamp(q['created_at[gte]'], 'created_at[gte]');
    if ('error' in createdGte) return reply.code(400).send({ error: createdGte.error });
    const createdLte = parseOptionalTimestamp(q['created_at[lte]'], 'created_at[lte]');
    if ('error' in createdLte) return reply.code(400).send({ error: createdLte.error });
    const cursor = decodeCreatedAtCursor(q.page);
    if (q.page && !cursor) return reply.code(400).send({ error: 'invalid page' });
    let cursorPredicate: SQL | undefined;
    if (cursor) {
      const cursorFilter = and(
        eq(agents.workspaceId, auth.workspaceId),
        includeArchived.value ? undefined : isNull(agents.archivedAt),
        eq(agents.id, cursor.id),
      )!;
      const cursorRows = await db
        .select({ id: agents.id })
        .from(agents)
        .where(and(isNull(agents.deletedAt), cursorFilter))
        .limit(1);
      if (cursorRows.length === 0) return reply.code(400).send({ error: 'invalid page' });
      cursorPredicate = sql`(${agents.createdAt}, ${agents.id}) < (select ${agents.createdAt}, ${agents.id} from ${agents} where ${cursorFilter} limit 1)`;
    }
    const limit = parsedLimit.value!;
    const filters = and(
      eq(agents.workspaceId, auth.workspaceId),
      includeArchived.value ? undefined : isNull(agents.archivedAt),
      createdGte.value ? gte(agents.createdAt, createdGte.value) : undefined,
      createdLte.value ? lte(agents.createdAt, createdLte.value) : undefined,
      cursorPredicate,
    );
    const rows = await db
      .select()
      .from(agents)
      .where(and(isNull(agents.deletedAt), filters))
      .orderBy(desc(agents.createdAt), desc(agents.id))
      .limit(limit + 1);
    const pageRows = rows.slice(0, limit);
    const orcaBeta = isOrcaBetaRequest(req.headers);
    const items = batchReadsEnabled
      ? pageRows.map((row) =>
          agentToApi(row, agentDataFromRow(row), orcaBeta, row.createdAt, row.updatedAt),
        )
      : await Promise.all(pageRows.map((row) => loadAgent(db, auth.workspaceId, row.id, orcaBeta)));
    reply.send({
      data: items.filter(Boolean),
      next_page:
        rows.length > limit && pageRows.length > 0
          ? encodeCreatedAtCursor(pageRows[pageRows.length - 1]! as { id: string })
          : null,
    });
  });

  app.get('/v1/agents/:id', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const query = req.query as { version?: string };
    const version = query.version === undefined ? undefined : Number(query.version);
    if (version !== undefined && (!Number.isInteger(version) || version < 1)) {
      return reply.code(400).send({ error: 'version must be a positive integer' });
    }
    const orcaBeta = isOrcaBetaRequest(req.headers);
    const agent = await loadAgent(db, auth.workspaceId, id, orcaBeta, version);
    if (!agent) return reply.code(404).send({ error: 'not found' });
    reply.send(agent);
  });

  app.get('/v1/agents/:id/versions', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const query = req.query as { limit?: string; page?: string };
    const parsedLimit = parsePositiveIntQueryParam(query.limit, 'limit', {
      defaultValue: AGENT_VERSIONS_PAGE_DEFAULT,
      max: AGENT_VERSIONS_PAGE_MAX,
    });
    if (!parsedLimit.ok) return reply.code(400).send({ error: parsedLimit.error });
    const cursor = query.page ? Number(query.page) : undefined;
    if (cursor !== undefined && (!Number.isInteger(cursor) || cursor <= 0)) {
      return reply.code(400).send({ error: 'page must be a positive integer' });
    }
    const existing = await loadAgentRow(db, auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'not found' });

    const rows = await db
      .select()
      .from(agentVersions)
      .where(
        cursor !== undefined
          ? and(
              eq(agentVersions.workspaceId, auth.workspaceId),
              eq(agentVersions.agentId, id),
              lt(agentVersions.version, cursor),
            )
          : and(eq(agentVersions.workspaceId, auth.workspaceId), eq(agentVersions.agentId, id)),
      )
      .orderBy(desc(agentVersions.version))
      .limit(parsedLimit.value! + 1);
    const orcaBeta = isOrcaBetaRequest(req.headers);
    const limit = parsedLimit.value!;
    const hasMore = rows.length > limit;
    const pageRows = rows.slice(0, limit);
    const nextPage = hasMore ? String(pageRows[pageRows.length - 1]?.version ?? '') : null;
    reply.send({
      data: pageRows.map((row) => {
        const data = agentDataFromSnapshot(row.snapshot as Record<string, unknown>);
        // Version snapshots are immutable. `updated_at` intentionally mirrors
        // the version row's `created_at`, while `archived_at` reflects the live
        // agent tombstone so old versions disappear from active inventory with
        // the parent agent.
        return agentToApi(existing, data, orcaBeta, row.createdAt, row.createdAt);
      }),
      next_page: nextPage,
    });
  });

  app.post('/v1/agents/:id', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const orcaBeta = isOrcaBetaRequest(req.headers);
    const updateValidation = validateAgentUpdateBody(req.body);
    if ('error' in updateValidation) return reply.code(400).send({ error: updateValidation.error });
    const body = updateValidation.value;

    // A request that omits `version` is unconditional on the Claude wire. If
    // another writer wins between our initial read and row lock, recompute the
    // patch against the new current value instead of surfacing a synthetic CAS
    // conflict. Explicit-version requests retain normal compare-and-swap
    // behavior.
    while (true) {
      const existing = await loadAgentRow(db, auth.workspaceId, id);
      if (!existing) return reply.code(404).send({ error: 'not found' });

      if (body.version !== undefined && existing.version !== body.version) {
        return reply.code(409).send({ error: 'version mismatch' });
      }

      const updateWhere = and(
        eq(agents.id, id),
        eq(agents.workspaceId, auth.workspaceId),
        eq(agents.version, existing.version),
      );
      const newVersion = existing.version + 1;

      const normalizedTools =
        body.tools !== undefined
          ? (body.tools ?? []).map((t: AgentTool) => ({
              ...t,
              type: toCanonicalToolName(t.type),
            }))
          : (existing.tools as AgentTool[]);

      const existingModel = storedModelFromRow(existing);
      const updatedModel = mergeModelUpdate(existingModel, body.model, existing.harnessType);
      if ('error' in updatedModel) return reply.code(400).send({ error: updatedModel.error });
      const updatedName = body.name ?? existing.name;
      const updatedDescription =
        body.description !== undefined ? body.description : (existing.description ?? null);
      const updatedSystem = body.system !== undefined ? body.system : (existing.system ?? null);
      const updatedMcpServers =
        body.mcp_servers !== undefined
          ? stripMcpServerPermissionPolicyFields(body.mcp_servers ?? [])
          : (existing.mcpServers as unknown[]);
      const configurationError = validateAgentConfiguration(normalizedTools, updatedMcpServers);
      if (configurationError) return reply.code(400).send({ error: configurationError });
      const skillsProvided = body.skills !== undefined;
      const skillInput = skillsProvided ? (body.skills ?? []) : existing.skills;
      const validatedSkills = await validateSkillRefs(db, auth.workspaceId, skillInput, {
        allowUnresolvedCustomRefs: !skillsProvided,
      });
      if ('error' in validatedSkills) return reply.code(400).send({ error: validatedSkills.error });
      const updatedSkills = validatedSkills;
      // Same shape as skills: an omitted field preserves what is stored rather
      // than clearing it. Without this branch an unrelated edit — a rename, a
      // model change — would rewrite the version snapshot with no guardrails,
      // and enforcement would stop for a rule nobody touched.
      const validatedUpdateGuardrails = await validateGuardrailRefs(
        db,
        auth.workspaceId,
        body.guardrail_ids !== undefined ? (body.guardrail_ids ?? []) : existing.guardrailIds,
      );
      if ('error' in validatedUpdateGuardrails)
        return reply.code(400).send({ error: validatedUpdateGuardrails.error });
      const existingMetadata = normalizeStoredMetadata(existing.metadata);
      const updatedMetadata =
        body.metadata === undefined
          ? existingMetadata
          : body.metadata === null
            ? {}
            : applyMetadataPatch(existingMetadata, body.metadata as MetadataPatch);
      const metadataError = validateMetadataLimits(updatedMetadata);
      if (metadataError) return reply.code(400).send({ error: metadataError });
      const harnessCheck = resolveHarnessAnnotation(updatedMetadata);
      if ('error' in harnessCheck) return reply.code(400).send({ error: harnessCheck.error });
      const harnessUpdateError =
        validateHarnessUpdate(existingMetadata, updatedMetadata) ??
        (harnessCheck.harness !== existing.harnessType ? 'metadata.harness is immutable' : null);
      if (harnessUpdateError) return reply.code(400).send({ error: harnessUpdateError });
      const modelError = validateHarnessModel(updatedMetadata, updatedModel);
      if (modelError) return reply.code(400).send({ error: modelError });
      const updatedMultiagent =
        body.multiagent !== undefined
          ? await resolveMultiagentForStorage(db, auth.workspaceId, id, newVersion, body.multiagent)
          : ((existing.multiagent as CanonicalMultiagent | null) ?? null);
      if (updatedMultiagent && 'error' in updatedMultiagent)
        return reply.code(400).send({ error: updatedMultiagent.error });
      const featureError = validateHarnessFeatures(updatedMetadata, {
        skills: updatedSkills,
        multiagent: updatedMultiagent,
      });
      if (featureError) return reply.code(400).send({ error: featureError });
      const modelRosterError = await validateMultiagentModelRoster(
        db,
        auth.workspaceId,
        id,
        newVersion,
        updatedModel,
        updatedMultiagent,
        harnessCheck.harness,
      );
      if (modelRosterError) return reply.code(400).send({ error: modelRosterError });

      const existingData = agentDataFromRow(existing);
      const updatedData: AgentData = {
        name: updatedName,
        description: updatedDescription,
        version: newVersion,
        model: updatedModel,
        system: updatedSystem,
        tools: normalizedTools,
        mcp_servers: updatedMcpServers,
        skills: updatedSkills,
        guardrail_ids: validatedUpdateGuardrails,
        metadata: updatedMetadata,
        multiagent: updatedMultiagent,
      };

      if (
        isDeepStrictEqual(
          agentConfigFromData(existingData, orcaBeta),
          agentConfigFromData(updatedData, orcaBeta),
        )
      ) {
        return reply.send(
          agentToApi(existing, existingData, orcaBeta, existing.createdAt, existing.updatedAt),
        );
      }

      const versionId = newId('agtv');
      const now = new Date();
      const snapshot = {
        harness_type: existing.harnessType,
        id,
        ...updatedData,
      };

      let versionConflict = false;
      await db.transaction(async (tx) => {
        const currentRows = await tx
          .select({ version: agents.version })
          .from(agents)
          .where(
            and(
              isNull(agents.deletedAt),
              eq(agents.id, id),
              eq(agents.workspaceId, auth.workspaceId),
            ),
          )
          .for('update')
          .limit(1);
        if (currentRows[0]?.version !== existing.version) {
          versionConflict = true;
          return;
        }

        await tx.insert(agentVersions).values({
          id: versionId,
          workspaceId: auth.workspaceId,
          agentId: id,
          version: newVersion,
          snapshot,
          createdAt: now,
        });
        const updatedRows = await tx
          .update(agents)
          .set({
            version: newVersion,
            latestVersionId: versionId,
            name: updatedName,
            description: updatedDescription,
            modelProvider: updatedModel.provider,
            modelId: updatedModel.id,
            modelSpeed: updatedModel.speed ?? null,
            modelEffort: updatedModel.effort ?? null,
            system: updatedSystem,
            tools: normalizedTools,
            mcpServers: updatedMcpServers,
            skills: updatedSkills,
            guardrailIds: validatedUpdateGuardrails,
            metadata: toJsonMetadata(updatedMetadata),
            multiagent: updatedMultiagent,
            updatedAt: now,
          })
          .where(and(isNull(agents.deletedAt), updateWhere))
          .returning({ id: agents.id });
        if (updatedRows.length === 0) {
          versionConflict = true;
          return;
        }
      });
      if (versionConflict) {
        if (body.version === undefined) continue;
        return reply.code(409).send({ error: 'version mismatch' });
      }

      const out = await loadAgent(db, auth.workspaceId, id, orcaBeta);
      if (!out) return reply.code(404).send({ error: 'not found' });
      return reply.send(out);
    }
  });

  app.post('/v1/agents/:id/archive', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    await db
      .update(agents)
      .set({ archivedAt: new Date() })
      .where(
        and(isNull(agents.deletedAt), eq(agents.id, id), eq(agents.workspaceId, auth.workspaceId)),
      );
    const orcaBeta = isOrcaBetaRequest(req.headers);
    const agent = await loadAgent(db, auth.workspaceId, id, orcaBeta);
    if (!agent) return reply.code(404).send({ error: 'not found' });
    return reply.send(agent);
  });

  app.delete('/v1/agents/:id', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);

    const deleted = await db.transaction(async (tx) => {
      // Use the same parent-first lock order as version creation so no new
      // version can commit after the deletion has marked its children.
      const [existing] = await tx
        .select({ id: agents.id })
        .from(agents)
        .where(
          and(
            eq(agents.workspaceId, auth.workspaceId),
            eq(agents.id, id),
            isNull(agents.deletedAt),
          ),
        )
        .for('update');
      if (!existing) return false;
      const now = new Date();
      await tx
        .update(agentVersions)
        .set({ deletedAt: now })
        .where(
          and(
            eq(agentVersions.workspaceId, auth.workspaceId),
            eq(agentVersions.agentId, id),
            isNull(agentVersions.deletedAt),
          ),
        );
      await tx
        .update(agents)
        .set({ deletedAt: now })
        .where(
          and(
            isNull(agents.deletedAt),
            eq(agents.id, id),
            eq(agents.workspaceId, auth.workspaceId),
          ),
        );
      return true;
    });
    if (!deleted) return reply.code(404).send({ error: 'not found' });
    return reply.send({ id, type: 'agent_deleted' });
  });
}

async function loadAgent(
  db: DbClient,
  workspaceId: string,
  agentId: string,
  orcaBeta: boolean,
  version?: number,
) {
  const agentRow = await loadAgentRow(db, workspaceId, agentId);
  if (!agentRow) return null;

  let data: AgentData;

  if (version != null) {
    const versionRows = await db
      .select()
      .from(agentVersions)
      .where(
        and(
          eq(agentVersions.workspaceId, workspaceId),
          eq(agentVersions.agentId, agentId),
          eq(agentVersions.version, version),
        ),
      )
      .limit(1);
    const versionRow = versionRows[0];
    if (!versionRow) return null;
    data = agentDataFromSnapshot(versionRow.snapshot as Record<string, unknown>);
  } else {
    data = agentDataFromRow(agentRow);
  }

  return agentToApi(agentRow, data, orcaBeta, agentRow.createdAt, agentRow.updatedAt);
}

interface AgentData {
  name: string;
  description: string | null;
  version: number;
  model: StoredModel;
  system: string | null;
  tools: AgentTool[];
  mcp_servers: unknown[];
  skills: unknown[];
  /**
   * Optional because snapshots written before guardrails existed have no such
   * field. An absent value is an agent with no explicit guardrails, which is
   * exactly what those agents had.
   */
  guardrail_ids?: string[];
  metadata: Record<string, unknown>;
  multiagent: CanonicalMultiagent | null;
}

type AgentConfigData = Omit<AgentData, 'version' | 'model' | 'tools' | 'mcp_servers'> & {
  model: StoredModel & { speed: NonNullable<StoredModel['speed']> };
  tools: Array<Record<string, unknown>>;
  mcp_servers: Array<Record<string, unknown>>;
};

function agentDataFromRow(agentRow: typeof agents.$inferSelect): AgentData {
  return {
    name: agentRow.name,
    description: agentRow.description ?? null,
    version: agentRow.version,
    model: storedModelFromRow(agentRow),
    system: agentRow.system ?? null,
    tools: (agentRow.tools as AgentTool[]) ?? [],
    mcp_servers: (agentRow.mcpServers as unknown[]) ?? [],
    skills: (agentRow.skills as unknown[]) ?? [],
    guardrail_ids: (agentRow.guardrailIds as string[] | null) ?? [],
    metadata: normalizeStoredMetadata(agentRow.metadata),
    multiagent: (agentRow.multiagent as CanonicalMultiagent | null) ?? null,
  };
}

function agentConfigFromData(data: AgentData, orcaBeta: boolean): AgentConfigData {
  return {
    name: data.name,
    description: data.description,
    model: {
      ...data.model,
      speed: data.model.speed ?? 'standard',
    },
    system: data.system,
    tools: orcaBeta ? data.tools : data.tools.map((tool) => toolToApi(tool, false)),
    mcp_servers: orcaBeta
      ? data.mcp_servers.map((server) => (isRecord(server) ? server : {}))
      : data.mcp_servers.map((server) => mcpServerToApi(server, false)),
    skills: data.skills,
    // Included so an update that changes only this field still produces a new
    // version. Omitting it would make the config compare equal and the write
    // would be silently dropped as a no-op.
    guardrail_ids: data.guardrail_ids ?? [],
    metadata: data.metadata,
    multiagent: data.multiagent,
  };
}

function agentDataFromSnapshot(snap: Record<string, unknown>): AgentData {
  const metadata = normalizeStoredMetadata(snap.metadata);
  const legacyDescription = metadata.description;
  if (typeof legacyDescription === 'string' && snap.description === undefined) {
    delete metadata.description;
  }
  return {
    name: snap.name as string,
    description:
      typeof snap.description === 'string'
        ? snap.description
        : typeof legacyDescription === 'string'
          ? legacyDescription
          : null,
    version: snap.version as number,
    model: snap.model as StoredModel,
    system: typeof snap.system === 'string' ? snap.system : null,
    tools: (snap.tools as AgentTool[]) ?? [],
    mcp_servers: (snap.mcp_servers as unknown[]) ?? [],
    skills: (snap.skills as unknown[]) ?? [],
    guardrail_ids: (snap.guardrail_ids as string[] | undefined) ?? [],
    metadata,
    multiagent: (snap.multiagent as CanonicalMultiagent | null) ?? null,
  };
}

function agentToApi(
  agentRow: typeof agents.$inferSelect,
  data: AgentData,
  orcaBeta: boolean,
  createdAt: Date,
  updatedAt: Date,
) {
  const denormalizedTools = data.tools.map((tool) => toolToApi(tool, orcaBeta));

  return {
    id: agentRow.id,
    type: 'agent',
    name: data.name,
    description: data.description,
    version: data.version,
    model: modelToApi(data.model, orcaBeta, agentRow.harnessType),
    system: data.system,
    tools: denormalizedTools,
    mcp_servers: data.mcp_servers.map((server) => mcpServerToApi(server, orcaBeta)),
    skills: data.skills.map((skill) => skillToApi(skill)),
    ...(orcaBeta ? { guardrail_ids: data.guardrail_ids ?? [] } : {}),
    metadata: data.metadata,
    multiagent: multiagentToApi(data.multiagent),
    archived_at: agentRow.archivedAt?.toISOString() ?? null,
    created_at: createdAt.toISOString(),
    updated_at: updatedAt.toISOString(),
  };
}

function storedModelFromRow(agentRow: typeof agents.$inferSelect): StoredModel {
  return {
    provider: agentRow.modelProvider,
    id: agentRow.modelId,
    ...(agentRow.modelSpeed
      ? { speed: agentRow.modelSpeed as NonNullable<StoredModel['speed']> }
      : {}),
    ...(agentRow.modelEffort
      ? { effort: agentRow.modelEffort as NonNullable<StoredModel['effort']> }
      : {}),
  };
}

function mergeModelUpdate(
  existing: StoredModel,
  input: unknown,
  harness: string,
): StoredModel | { error: string } {
  if (input === undefined) return validateModelControlsForStorage(existing, harness);
  const normalized = normalizeModelForStorage(input, existing.provider);
  if ('error' in normalized) return normalized;
  const params = typeof input === 'string' ? {} : (input as Record<string, unknown>);
  const sameModel = normalized.provider === existing.provider && normalized.id === existing.id;
  return validateModelControlsForStorage(
    {
      provider: normalized.provider,
      id: normalized.id,
      ...('speed' in params
        ? normalized.speed
          ? { speed: normalized.speed }
          : {}
        : sameModel && existing.speed
          ? { speed: existing.speed }
          : {}),
      ...('effort' in params
        ? normalized.effort
          ? { effort: normalized.effort }
          : {}
        : sameModel && existing.effort
          ? { effort: existing.effort }
          : {}),
    },
    harness,
  );
}

export function toolToApi(tool: AgentTool, orcaBeta: boolean): Record<string, unknown> {
  const type = toAnthropicWireToolName(tool.type, orcaBeta);
  if (orcaBeta) return { ...tool, type };
  if (type === 'custom') {
    return {
      type,
      name: tool.name,
      description: tool.description,
      input_schema: tool.input_schema,
    };
  }

  const defaultConfig = resolvedToolConfig(tool.default_config, {
    enabled: true,
    permission_policy: { type: 'always_allow' },
  });
  const rawConfigs = Array.isArray(tool.configs)
    ? tool.configs
    : isRecord(tool.configs)
      ? Object.entries(tool.configs).map(([name, config]) => ({
          ...(isRecord(config) ? config : {}),
          name,
        }))
      : [];
  const configs = rawConfigs.map((config) => {
    const entry = isRecord(config) ? config : {};
    return {
      name: entry.name,
      ...resolvedToolConfig(entry, defaultConfig),
    };
  });
  return {
    type,
    ...(type === 'mcp_toolset' ? { mcp_server_name: tool.mcp_server_name } : {}),
    default_config: defaultConfig,
    configs,
  };
}

function resolvedToolConfig(
  value: unknown,
  fallback: { enabled: boolean; permission_policy: { type: 'always_allow' | 'always_ask' } },
): { enabled: boolean; permission_policy: { type: 'always_allow' | 'always_ask' } } {
  const config = isRecord(value) ? value : {};
  const policy = isRecord(config.permission_policy) ? config.permission_policy.type : undefined;
  return {
    enabled: typeof config.enabled === 'boolean' ? config.enabled : fallback.enabled,
    permission_policy: {
      type:
        policy === 'always_ask' || policy === 'always_allow'
          ? policy
          : fallback.permission_policy.type,
    },
  };
}

export function mcpServerToApi(value: unknown, orcaBeta: boolean): Record<string, unknown> {
  const server = isRecord(value) ? value : {};
  if (orcaBeta) return { ...server };
  return { name: server.name, type: 'url', url: server.url };
}

export function skillToApi(value: unknown): Record<string, unknown> {
  const skill = isRecord(value) ? value : {};
  return {
    type: skill.type,
    skill_id: skill.skill_id,
    version: skill.version,
  };
}

function multiagentToApi(value: CanonicalMultiagent | null): CanonicalMultiagent | null {
  if (!value) return null;
  return {
    type: 'coordinator',
    agents: value.agents.map((agent) => ({ ...agent })),
  };
}

function parseOptionalBoolean(value: string | undefined): { value: boolean } | { error: string } {
  if (value === undefined) return { value: false };
  if (value === 'true') return { value: true };
  if (value === 'false') return { value: false };
  return { error: 'include_archived must be true or false' };
}

function parseOptionalTimestamp(
  value: string | undefined,
  field: string,
): { value: Date | undefined } | { error: string } {
  if (value === undefined) return { value: undefined };
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return { error: `${field} must be an RFC 3339 timestamp` };
  return { value: parsed };
}

function validateAgentCreateBody(input: unknown): { value: AgentCreateBody } | { error: string } {
  const parsed = AgentCreate.safeParse(input);
  if (!parsed.success) return { error: firstZodIssue(parsed.error, 'invalid agent create body') };
  return { value: parsed.data };
}

function validateAgentUpdateBody(input: unknown): { value: AgentUpdateBody } | { error: string } {
  const parsed = AgentUpdate.safeParse(input);
  if (!parsed.success) return { error: firstZodIssue(parsed.error, 'invalid agent update body') };
  return { value: parsed.data };
}

function firstZodIssue(
  error: { issues: Array<{ message: string; path: Array<string | number> }> },
  fallback: string,
): string {
  const issue = error.issues[0];
  if (!issue) return fallback;
  if (
    issue.message ===
    'skills entries must be {type:"anthropic", skill_id, version?} or {type:"custom", skill_id, version?}'
  ) {
    return issue.message;
  }
  const path = issue.path.join('.');
  if (!path || issue.message.toLowerCase().startsWith(`${path.toLowerCase()} `)) {
    return issue.message;
  }
  return `${path}: ${issue.message}`;
}

async function loadAgentRow(db: DbClient, workspaceId: string, agentId: string) {
  const rows = await db
    .select()
    .from(agents)
    .where(
      and(isNull(agents.deletedAt), eq(agents.id, agentId), eq(agents.workspaceId, workspaceId)),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function loadAgentVersionSnapshot(
  db: DbClient,
  workspaceId: string,
  agentId: string,
  version: number,
): Promise<Record<string, unknown> | null> {
  const rows = await db
    .select({ snapshot: agentVersions.snapshot })
    .from(agentVersions)
    .where(
      and(
        eq(agentVersions.workspaceId, workspaceId),
        eq(agentVersions.agentId, agentId),
        eq(agentVersions.version, version),
      ),
    )
    .limit(1);
  return (rows[0]?.snapshot as Record<string, unknown> | undefined) ?? null;
}

async function resolveMultiagentForStorage(
  db: DbClient,
  workspaceId: string,
  ownerAgentId: string,
  ownerVersion: number,
  input: unknown,
): Promise<CanonicalMultiagent | null | { error: string }> {
  if (input == null) return null;
  if (!isRecord(input) || input.type !== 'coordinator' || !Array.isArray(input.agents)) {
    return { error: 'multiagent must be a coordinator object with an agents array' };
  }
  if (input.agents.length < 1 || input.agents.length > 20) {
    return { error: 'multiagent.agents must contain 1-20 entries' };
  }

  let selfCount = 0;
  const refs: CanonicalMultiagentAgentRef[] = [];
  for (const entry of input.agents) {
    if (isRecord(entry) && entry.type === 'self') {
      selfCount += 1;
      if (selfCount > 1) return { error: 'multiagent.agents may contain at most one self entry' };
      refs.push({ type: 'agent', id: ownerAgentId, version: ownerVersion });
      continue;
    }

    const requested =
      typeof entry === 'string'
        ? { id: toInternalId(entry), version: undefined }
        : isRecord(entry) && entry.type === 'agent' && typeof entry.id === 'string'
          ? {
              id: toInternalId(entry.id),
              version: typeof entry.version === 'number' ? entry.version : undefined,
            }
          : null;
    if (!requested) {
      return {
        error:
          'multiagent.agents entries must be agent ids, {type:"agent", id, version?}, or {type:"self"}',
      };
    }
    if (
      requested.version !== undefined &&
      (!Number.isInteger(requested.version) || requested.version < 1)
    ) {
      return { error: 'multiagent agent reference version must be a positive integer' };
    }

    const agentRow = await loadAgentRow(db, workspaceId, requested.id);
    if (!agentRow || agentRow.archivedAt) {
      return { error: `multiagent referenced agent ${requested.id} not found` };
    }
    const version = requested.version ?? agentRow.version;
    const snapshot = await loadAgentVersionSnapshot(db, workspaceId, requested.id, version);
    if (!snapshot) {
      return { error: `multiagent referenced agent ${requested.id} version ${version} not found` };
    }
    const memberHarness = resolveHarnessAnnotation(
      snapshot.metadata as Record<string, unknown> | undefined,
    );
    if (
      !('error' in memberHarness) &&
      (memberHarness.harness === 'codex_sdk' || memberHarness.harness === 'pi_sdk')
    ) {
      return { error: 'codex_sdk agents cannot be multiagent roster members' };
    }
    if (snapshot.multiagent != null) {
      return { error: 'multiagent referenced agents must not themselves have multiagent set' };
    }
    refs.push({ type: 'agent', id: requested.id, version });
  }

  const uniqueAgentIds = new Set(refs.map((ref) => ref.id));
  if (uniqueAgentIds.size !== refs.length) {
    return { error: 'multiagent.agents must reference distinct agents' };
  }

  return { type: 'coordinator', agents: refs };
}

export async function validateMultiagentModelRoster(
  db: DbClient,
  workspaceId: string,
  ownerAgentId: string,
  ownerVersion: number,
  ownerModel: StoredModel,
  multiagent: CanonicalMultiagent | null,
  harness?: string,
): Promise<string | null> {
  const entries = [{ label: 'primary agent', model: ownerModel, ...(harness ? { harness } : {}) }];
  for (const ref of multiagent?.agents ?? []) {
    if (ref.id === ownerAgentId && ref.version === ownerVersion) {
      entries.push({ label: `subagent ${ref.id}@${ref.version}`, model: ownerModel });
      continue;
    }
    const snapshot = await loadAgentVersionSnapshot(db, workspaceId, ref.id, ref.version);
    if (!snapshot) return `multiagent referenced agent ${ref.id} version ${ref.version} not found`;
    const model = normalizeModelForStorage(snapshot.model);
    if ('error' in model) {
      return `multiagent referenced agent ${ref.id} version ${ref.version} has invalid model`;
    }
    entries.push({
      label: `subagent ${ref.id}@${ref.version}`,
      model,
      ...(typeof snapshot.harness_type === 'string' ? { harness: snapshot.harness_type } : {}),
    });
  }
  return validateModelRosterForStorage(entries);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

type AnthropicSkillRef = { type: 'anthropic'; skill_id: string; version: string };
type CustomSkillRef = { type: 'custom'; skill_id: string; version: string };
type SkillRef = AnthropicSkillRef | CustomSkillRef;

const SKILL_REF_SHAPE_ERROR =
  'skills entries must be {type:"anthropic", skill_id, version?} or {type:"custom", skill_id, version?}';

/**
 * Validates typed agent skill references (managed-agents-2026-04-01). Anthropic
 * catalog refs are accepted by skill_id (passthrough — the catalog is resolved
 * at session creation); custom refs must resolve to a live workspace version.
 * Resolved custom refs are checked for duplicate versions and conflicting
 * effective-name packages within this AgentVersion.
 * The requested version selector remains unchanged in the agent snapshot so a
 * later session can bind `latest` atomically with the rest of its skill graph.
 */
/**
 * Resolve explicit guardrail references for an agent.
 *
 * An id that does not resolve is a 400 rather than a silent drop: a reference
 * to a guardrail that no longer exists leaves an agent that looks governed and
 * is not, which is worse than a rejected write. Visible means owned by this
 * workspace or organization-scoped for the workspace's organization — the two
 * sets a session actually composes from.
 */
export async function validateGuardrailRefs(
  db: DbClient,
  workspaceId: string,
  input: unknown,
): Promise<string[] | { error: string }> {
  if (input === undefined || input === null) return [];
  if (!Array.isArray(input)) return { error: 'guardrail_ids must be an array of guardrail ids' };
  if (input.length === 0) return [];

  const ids: string[] = [];
  for (const value of input) {
    if (typeof value !== 'string' || !value.startsWith('grd_')) {
      return { error: 'guardrail_ids entries must be grd_ identifiers' };
    }
    if (!ids.includes(value)) ids.push(value);
  }

  const workspaceRow = await db
    .select({ organizationId: workspaces.organizationId })
    .from(workspaces)
    .where(eq(workspaces.id, workspaceId))
    .limit(1);
  const organizationId = workspaceRow[0]?.organizationId ?? null;

  const rows = await db
    .select({ id: guardrails.id })
    .from(guardrails)
    .where(
      and(
        isNull(guardrails.deletedAt),
        isNull(guardrails.archivedAt),
        inArray(guardrails.id, ids),
        organizationId === null
          ? eq(guardrails.workspaceId, workspaceId)
          : or(
              eq(guardrails.workspaceId, workspaceId),
              and(
                eq(guardrails.organizationId, organizationId),
                eq(guardrails.scope, 'organization'),
              ),
            ),
      ),
    );

  const found = new Set(rows.map((r) => r.id));
  const missing = ids.filter((id) => !found.has(id));
  if (missing.length > 0) {
    return { error: `unknown or archived guardrail_ids: ${missing.join(', ')}` };
  }
  return ids;
}

export async function validateSkillRefs(
  db: DbClient,
  workspaceId: string,
  input: unknown,
  options: { allowUnresolvedCustomRefs?: boolean } = {},
): Promise<SkillRef[] | { error: string }> {
  if (!Array.isArray(input)) {
    return { error: SKILL_REF_SHAPE_ERROR };
  }
  if (input.length > MAX_AGENT_SKILL_REFS) {
    return { error: MAX_AGENT_SKILL_REFS_ERROR };
  }

  const refs: SkillRef[] = [];
  const anthropicRefKeys = new Set<string>();
  const customRefs: CustomSkillRef[] = [];
  for (const value of input) {
    if (!isRecord(value) || typeof value.type !== 'string') {
      return { error: SKILL_REF_SHAPE_ERROR };
    }
    if (value.type === 'anthropic') {
      if (typeof value.skill_id !== 'string' || value.skill_id.length === 0) {
        return { error: 'anthropic skill reference requires a non-empty skill_id' };
      }
      if (
        value.version !== undefined &&
        value.version !== null &&
        (typeof value.version !== 'string' || value.version.length === 0)
      ) {
        return { error: 'anthropic skill reference version must be a non-empty string' };
      }
      const ref: AnthropicSkillRef = {
        type: 'anthropic',
        skill_id: value.skill_id,
        version: typeof value.version === 'string' ? value.version : 'latest',
      };
      const refKey = `${ref.skill_id}\0${ref.version}`;
      if (anthropicRefKeys.has(refKey)) {
        return {
          error: `agent skills repeat anthropic skill ${ref.skill_id} version ${ref.version}`,
        };
      }
      anthropicRefKeys.add(refKey);
      refs.push(ref);
    } else if (value.type === 'custom') {
      if (typeof value.skill_id !== 'string' || !value.skill_id.startsWith('skill_')) {
        return { error: 'custom skill reference requires a skill_ skill_id' };
      }
      if (
        value.version !== undefined &&
        value.version !== null &&
        !CustomSkillVersionSelector.safeParse(value.version).success
      ) {
        return {
          error:
            'custom skill reference version must be "latest" or a decimal timestamp version identifier',
        };
      }
      const requestedVersion = typeof value.version === 'string' ? value.version : 'latest';
      const ref: CustomSkillRef = {
        type: 'custom',
        skill_id: value.skill_id,
        version: requestedVersion,
      };
      customRefs.push(ref);
      refs.push(ref);
    } else {
      return { error: `unknown skill reference type ${value.type}` };
    }
  }

  const resolvedCustomVersions: Array<ResolvedSkillReference & { skillId: string }> = [];
  if (customRefs.length > 0) {
    const versionRows = await db
      .select({
        id: skillVersions.id,
        skillId: skillVersions.skillId,
        latestVersionId: skills.latestVersionId,
        versionIdentifier: skillVersions.versionIdentifier,
        name: skillVersions.name,
        packageSha256: skillVersions.packageSha256,
      })
      .from(skillVersions)
      .innerJoin(
        skills,
        and(
          isNull(skills.deletedAt),
          eq(skillVersions.workspaceId, skills.workspaceId),
          eq(skillVersions.skillId, skills.id),
        ),
      )
      .where(
        and(
          eq(skillVersions.workspaceId, workspaceId),
          eq(skills.workspaceId, workspaceId),
          eq(skills.type, 'custom'),
          isNull(skills.archivedAt),
          isNull(skillVersions.archivedAt),
          isNull(skillVersions.deletedAt),
          or(...customRefs.map(customSkillVersionPredicate)),
        ),
      );

    const rowsByRequest = new Map<string, (typeof versionRows)[number]>();
    for (const row of versionRows) {
      rowsByRequest.set(customSkillRequestKey(row.skillId, row.versionIdentifier), row);
      if (row.id === row.latestVersionId) {
        rowsByRequest.set(customSkillRequestKey(row.skillId, 'latest'), row);
      }
    }
    for (const ref of customRefs) {
      const resolved = rowsByRequest.get(customSkillRequestKey(ref.skill_id, ref.version));
      if (!resolved) {
        if (options.allowUnresolvedCustomRefs) continue;
        return {
          error: `skill ${ref.skill_id} version ${ref.version} not found in workspace`,
        };
      }
      resolvedCustomVersions.push(resolved);
    }
  }

  const conflict = findSkillReferenceConflict(resolvedCustomVersions);
  if (conflict?.type === 'duplicate_version') {
    const skillId = resolvedCustomVersions.find(
      (version) => version.id === conflict.skillVersionId,
    )!.skillId;
    return {
      error: `custom skill ${skillId} resolves to the same version more than once`,
    };
  }
  if (conflict?.type === 'different_packages') {
    const skillIds = [
      ...new Set(
        resolvedCustomVersions
          .filter((version) => version.name === conflict.name)
          .map((version) => version.skillId),
      ),
    ];
    const skillIdLabel = skillIds.length === 1 ? 'skill ID' : 'skill IDs';
    return {
      error: `skill name ${conflict.name} resolves to different packages for ${skillIdLabel} ${skillIds.join(', ')}`,
    };
  }

  return refs;
}

function customSkillVersionPredicate(ref: CustomSkillRef): SQL {
  return and(
    eq(skills.id, ref.skill_id),
    ref.version === 'latest'
      ? eq(skillVersions.id, skills.latestVersionId)
      : eq(skillVersions.versionIdentifier, ref.version),
  )!;
}

function customSkillRequestKey(skillId: string, version: string): string {
  return `${skillId}\0${version}`;
}
