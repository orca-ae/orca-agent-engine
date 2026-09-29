// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { and, desc, eq, isNotNull, isNull, sql, type SQL } from 'drizzle-orm';
import { requireWorkspaceScopes } from '../auth/workspace-scope.js';
import { authenticatedGuardrailSubject } from '../auth/guardrail-subject.js';
import { buildClaudeErrorResponse } from '../contracts/common.js';
import { toInternalId } from '../contracts/id-prefix.js';
import {
  triggerCreateBodySchema,
  triggerUpdateBodySchema,
} from '../contracts/triggers.contract.js';
import {
  agentTriggerFires,
  agentTriggers,
  agents,
  agentVersions,
  environments,
  sessions,
} from '../persistence/postgres/schema.js';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  applyMetadataPatch,
  normalizeStoredMetadata,
  toJsonMetadata,
  validateMetadataLimits,
} from './metadata.js';
import { parsePositiveIntQueryParam } from './query-params.js';
import { loadSession, loadSessionRows } from './sessions.routes.js';
import { requestReadSignal } from '../middleware/read-admission.js';
import {
  DEFAULT_TRIGGER_TIMEZONE,
  nextTriggerOccurrence,
  renderCronTriggerTitle,
  TriggerScheduleError,
  TriggerSessionTemplateError,
} from '../domain/trigger-cron.js';
import { validateSessionVaultIds } from '../domain/session-creation.js';
import { sessionLlmEgressMetadataError } from '../domain/session-llm-egress.js';
import { newId } from '../domain/versioning.js';

const TRIGGER_LIST_PAGE_DEFAULT = 100;
const TRIGGER_LIST_PAGE_MAX = 100;

export function registerTriggerRoutes(
  app: FastifyInstance,
  db: DbClient,
  batchReadsEnabled = true,
): void {
  app.post('/v1/triggers', async (req, reply) => {
    if (!requireWorkspaceScopes(req, reply, 'workspace.agentTriggers.create')) return;
    const parsed = triggerCreateBodySchema.safeParse(req.body);
    if (!parsed.success)
      return invalidRequest(req, reply, parsed.error.issues[0]?.message ?? 'invalid body');
    const llmEgressError = sessionLlmEgressMetadataError(parsed.data.session.metadata ?? {});
    if (llmEgressError) return invalidRequest(req, reply, llmEgressError);
    const auth = req.auth!;
    const now = new Date();
    const agentRef = normalizeAgentRef(parsed.data.agent);
    const timezone = parsed.data.source.timezone ?? DEFAULT_TRIGGER_TIMEZONE;
    let nextFireAt: Date | null = null;
    try {
      if (!parsed.data.paused) {
        nextFireAt = nextTriggerOccurrence(parsed.data.source.schedule, timezone, now);
      } else {
        // Paused schedules are still validated at creation time.
        nextTriggerOccurrence(parsed.data.source.schedule, timezone, now);
      }
      renderCronTriggerTitle(
        parsed.data.session.title_template ?? null,
        parsed.data.name,
        parsed.data.source.payload,
      );
    } catch (error) {
      return scheduleError(req, reply, error);
    }

    const id = newId('trg');
    const outcome = await db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DbClient;
      const pinnedAgent = await resolvePinnedAgent(
        tx,
        auth.workspaceId,
        agentRef.id,
        agentRef.version,
        true,
      );
      if (!pinnedAgent) return { error: 'agent' as const };
      if (
        !(await activeEnvironmentExists(
          tx,
          auth.workspaceId,
          parsed.data.session.environment_id,
          true,
        ))
      ) {
        return { error: 'environment' as const };
      }
      const vaultError = await validateSessionVaultIds(
        tx,
        auth.workspaceId,
        parsed.data.session.vault_ids ?? [],
        true,
      );
      if (vaultError) return { error: 'vault' as const, message: vaultError };

      await tx.insert(agentTriggers).values({
        id,
        workspaceId: auth.workspaceId,
        guardrailSubject: authenticatedGuardrailSubject(auth),
        name: parsed.data.name,
        agentId: pinnedAgent.id,
        agentVersion: pinnedAgent.version,
        environmentId: parsed.data.session.environment_id,
        titleTemplate: parsed.data.session.title_template ?? null,
        metadata: toJsonMetadata(parsed.data.session.metadata ?? {}),
        vaultIds: parsed.data.session.vault_ids ?? [],
        payload: parsed.data.source.payload,
        cronExpression: parsed.data.source.schedule.trim().replace(/\s+/g, ' '),
        timezone,
        status: parsed.data.paused ? 'paused' : 'active',
        generation: 1,
        nextFireAt,
        lastFiredAt: null,
        lastError: null,
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      });
      return { ok: true as const };
    });
    if ('error' in outcome) {
      if (outcome.error === 'agent') return notFound(req, reply, 'agent not found');
      if (outcome.error === 'environment') return notFound(req, reply, 'environment not found');
      if (outcome.error === 'vault') return invalidRequest(req, reply, outcome.message);
    }
    return reply.send(await loadTrigger(db, auth.workspaceId, id));
  });

  app.get('/v1/triggers', async (req, reply) => {
    if (!requireWorkspaceScopes(req, reply, 'workspace.agentTriggers.describe')) return;
    const auth = req.auth!;
    const query = req.query as {
      limit?: string;
      page?: string;
      agent_id?: string;
      include_archived?: string;
    };
    const parsedLimit = parsePositiveIntQueryParam(query.limit, 'limit', {
      defaultValue: TRIGGER_LIST_PAGE_DEFAULT,
      max: TRIGGER_LIST_PAGE_MAX,
    });
    if (!parsedLimit.ok) return invalidRequest(req, reply, parsedLimit.error!);
    const includeArchived = parseBooleanQuery(query.include_archived, false);
    if ('error' in includeArchived) return invalidRequest(req, reply, includeArchived.error);
    const agentId = query.agent_id ? toInternalId(query.agent_id) : undefined;

    let cursorPredicate: SQL | undefined;
    if (query.page) {
      const cursorFilter = and(
        eq(agentTriggers.workspaceId, auth.workspaceId),
        eq(agentTriggers.id, query.page),
        includeArchived.value ? undefined : isNull(agentTriggers.archivedAt),
        agentId ? eq(agentTriggers.agentId, agentId) : undefined,
      )!;
      const [cursor] = await db
        .select({ id: agentTriggers.id, createdAt: agentTriggers.createdAt })
        .from(agentTriggers)
        .where(and(isNull(agentTriggers.deletedAt), cursorFilter))
        .limit(1);
      if (!cursor) return invalidRequest(req, reply, 'invalid page');
      cursorPredicate = sql`(${agentTriggers.createdAt}, ${agentTriggers.id}) < (${cursor.createdAt}, ${cursor.id})`;
    }

    const limit = parsedLimit.value!;
    const rows = await db
      .select()
      .from(agentTriggers)
      .where(
        and(
          isNull(agentTriggers.deletedAt),
          eq(agentTriggers.workspaceId, auth.workspaceId),
          includeArchived.value ? undefined : isNull(agentTriggers.archivedAt),
          agentId ? eq(agentTriggers.agentId, agentId) : undefined,
          cursorPredicate,
        ),
      )
      .orderBy(desc(agentTriggers.createdAt), desc(agentTriggers.id))
      .limit(limit + 1);
    const pageRows = rows.slice(0, limit);
    return reply.send({
      data: pageRows.map(triggerToApi),
      next_page:
        rows.length > limit && pageRows.length > 0 ? pageRows[pageRows.length - 1]!.id : null,
    });
  });

  app.get('/v1/triggers/:id', async (req, reply) => {
    if (!requireWorkspaceScopes(req, reply, 'workspace.agentTriggers.describe')) return;
    const auth = req.auth!;
    const trigger = await loadTrigger(db, auth.workspaceId, triggerId(req));
    if (!trigger) return notFound(req, reply, 'trigger not found');
    return reply.send(trigger);
  });

  app.post('/v1/triggers/:id', async (req, reply) => {
    if (!requireWorkspaceScopes(req, reply, 'workspace.agentTriggers.alter')) return;
    const parsed = triggerUpdateBodySchema.safeParse(req.body);
    if (!parsed.success)
      return invalidRequest(req, reply, parsed.error.issues[0]?.message ?? 'invalid body');
    const auth = req.auth!;
    const id = triggerId(req);
    const now = new Date();
    const outcome = await db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DbClient;
      const [existing] = await tx
        .select()
        .from(agentTriggers)
        .where(
          and(
            isNull(agentTriggers.deletedAt),
            eq(agentTriggers.workspaceId, auth.workspaceId),
            eq(agentTriggers.id, id),
            isNull(agentTriggers.archivedAt),
          ),
        )
        .for('update')
        .limit(1);
      if (!existing) return { error: 'not_found' as const };

      const environmentId = parsed.data.session?.environment_id ?? existing.environmentId;
      if (!(await activeEnvironmentExists(tx, auth.workspaceId, environmentId, true))) {
        return { error: 'environment' as const };
      }
      const vaultIds = parsed.data.session?.vault_ids ?? existing.vaultIds;
      const vaultError = await validateSessionVaultIds(tx, auth.workspaceId, vaultIds, true);
      if (vaultError) return { error: 'vault' as const, message: vaultError };

      const metadata =
        parsed.data.session?.metadata === undefined
          ? normalizeStoredMetadata(existing.metadata)
          : applyMetadataPatch(existing.metadata, parsed.data.session.metadata);
      const metadataError = validateMetadataLimits(metadata);
      if (metadataError) return { error: 'metadata' as const, message: metadataError };
      const llmEgressError = sessionLlmEgressMetadataError(metadata);
      if (llmEgressError) return { error: 'metadata' as const, message: llmEgressError };
      const expression =
        parsed.data.source?.schedule?.trim().replace(/\s+/g, ' ') ?? existing.cronExpression;
      const timezone = parsed.data.source?.timezone ?? existing.timezone;
      const name = parsed.data.name ?? existing.name;
      const titleTemplate =
        parsed.data.session?.title_template !== undefined
          ? parsed.data.session.title_template
          : (existing.titleTemplate ?? null);
      const payload = parsed.data.source?.payload ?? existing.payload;
      let nextFireAt: Date | null = null;
      try {
        const next = nextTriggerOccurrence(expression, timezone, now);
        renderCronTriggerTitle(titleTemplate, name, payload);
        nextFireAt = existing.status === 'active' ? next : null;
      } catch (error) {
        return {
          error:
            error instanceof TriggerSessionTemplateError
              ? ('template' as const)
              : ('schedule' as const),
          message: error instanceof Error ? error.message : 'invalid Trigger configuration',
        };
      }

      await tx
        .update(agentTriggers)
        .set({
          name,
          environmentId,
          titleTemplate,
          metadata: toJsonMetadata(metadata),
          vaultIds,
          payload,
          cronExpression: expression,
          timezone,
          generation: existing.generation + 1,
          nextFireAt,
          lastError: null,
          updatedAt: now,
        })
        .where(
          and(
            isNull(agentTriggers.deletedAt),
            eq(agentTriggers.workspaceId, auth.workspaceId),
            eq(agentTriggers.id, id),
            eq(agentTriggers.generation, existing.generation),
          ),
        );
      return { ok: true as const };
    });

    if ('error' in outcome) {
      if (outcome.error === 'not_found') return notFound(req, reply, 'trigger not found');
      if (outcome.error === 'environment') return notFound(req, reply, 'environment not found');
      if (
        outcome.error === 'vault' ||
        outcome.error === 'metadata' ||
        outcome.error === 'schedule' ||
        outcome.error === 'template'
      ) {
        return invalidRequest(req, reply, outcome.message);
      }
    }
    return reply.send(await loadTrigger(db, auth.workspaceId, id));
  });

  app.post('/v1/triggers/:id/pause', async (req, reply) => {
    if (!requireWorkspaceScopes(req, reply, 'workspace.agentTriggers.alter')) return;
    const auth = req.auth!;
    const id = triggerId(req);
    const found = await mutateTriggerLifecycle(db, auth.workspaceId, id, 'pause');
    if (!found) return notFound(req, reply, 'trigger not found');
    return reply.send(await loadTrigger(db, auth.workspaceId, id));
  });

  app.post('/v1/triggers/:id/unpause', async (req, reply) => {
    if (!requireWorkspaceScopes(req, reply, 'workspace.agentTriggers.alter')) return;
    const auth = req.auth!;
    const id = triggerId(req);
    const outcome = await mutateTriggerLifecycle(db, auth.workspaceId, id, 'unpause');
    if (!outcome) return notFound(req, reply, 'trigger not found');
    if (typeof outcome === 'string') return invalidRequest(req, reply, outcome);
    return reply.send(await loadTrigger(db, auth.workspaceId, id));
  });

  app.delete('/v1/triggers/:id', async (req, reply) => {
    if (!requireWorkspaceScopes(req, reply, 'workspace.agentTriggers.delete')) return;
    const auth = req.auth!;
    const id = triggerId(req);
    const found = await mutateTriggerLifecycle(db, auth.workspaceId, id, 'archive');
    if (!found) return notFound(req, reply, 'trigger not found');
    return reply.send({ id, type: 'trigger_deleted' });
  });

  app.get('/v1/triggers/:id/sessions', async (req, reply) => {
    if (
      !requireWorkspaceScopes(req, reply, [
        'workspace.agentTriggers.describe',
        'workspace.sessions.describe',
      ])
    ) {
      return;
    }
    const auth = req.auth!;
    const id = triggerId(req);
    const [trigger] = await db
      .select({ id: agentTriggers.id })
      .from(agentTriggers)
      .where(
        and(
          isNull(agentTriggers.deletedAt),
          eq(agentTriggers.workspaceId, auth.workspaceId),
          eq(agentTriggers.id, id),
        ),
      )
      .limit(1);
    if (!trigger) return notFound(req, reply, 'trigger not found');

    const query = req.query as { limit?: string; page?: string; include_archived?: string };
    const parsedLimit = parsePositiveIntQueryParam(query.limit, 'limit', {
      defaultValue: TRIGGER_LIST_PAGE_DEFAULT,
      max: TRIGGER_LIST_PAGE_MAX,
    });
    if (!parsedLimit.ok) return invalidRequest(req, reply, parsedLimit.error!);
    const includeArchived = parseBooleanQuery(query.include_archived, false);
    if ('error' in includeArchived) return invalidRequest(req, reply, includeArchived.error);

    let cursorPredicate: SQL | undefined;
    if (query.page) {
      const [cursor] = await db
        .select({ id: agentTriggerFires.id, scheduledFor: agentTriggerFires.scheduledFor })
        .from(agentTriggerFires)
        .innerJoin(
          sessions,
          and(
            isNull(sessions.deletedAt),
            eq(agentTriggerFires.workspaceId, sessions.workspaceId),
            eq(agentTriggerFires.sessionId, sessions.id),
          ),
        )
        .where(
          and(
            eq(agentTriggerFires.workspaceId, auth.workspaceId),
            eq(agentTriggerFires.triggerId, id),
            eq(agentTriggerFires.id, query.page),
            eq(agentTriggerFires.status, 'enqueued'),
            includeArchived.value ? undefined : isNull(sessions.archivedAt),
          ),
        )
        .limit(1);
      if (!cursor) return invalidRequest(req, reply, 'invalid page');
      cursorPredicate = sql`(${agentTriggerFires.scheduledFor}, ${agentTriggerFires.id}) < (${cursor.scheduledFor}, ${cursor.id})`;
    }

    const limit = parsedLimit.value!;
    const rows = await db
      .select({
        fireId: agentTriggerFires.id,
        session: sessions,
      })
      .from(agentTriggerFires)
      .innerJoin(
        sessions,
        and(
          isNull(sessions.deletedAt),
          eq(agentTriggerFires.workspaceId, sessions.workspaceId),
          eq(agentTriggerFires.sessionId, sessions.id),
        ),
      )
      .where(
        and(
          eq(agentTriggerFires.workspaceId, auth.workspaceId),
          eq(agentTriggerFires.triggerId, id),
          eq(agentTriggerFires.status, 'enqueued'),
          isNotNull(agentTriggerFires.sessionId),
          includeArchived.value ? undefined : isNull(sessions.archivedAt),
          cursorPredicate,
        ),
      )
      .orderBy(desc(agentTriggerFires.scheduledFor), desc(agentTriggerFires.id))
      .limit(limit + 1);
    const pageRows = rows.slice(0, limit);
    const loaded = batchReadsEnabled
      ? await loadSessionRows(
          db,
          auth.workspaceId,
          pageRows.map((row) => row.session),
          false,
          requestReadSignal(req, reply),
        )
      : await Promise.all(pageRows.map((row) => loadSession(db, auth.workspaceId, row.session.id)));
    return reply.send({
      data: loaded.filter((session): session is NonNullable<typeof session> => session !== null),
      next_page:
        rows.length > limit && pageRows.length > 0 ? pageRows[pageRows.length - 1]!.fireId : null,
    });
  });
}

async function mutateTriggerLifecycle(
  db: DbClient,
  workspaceId: string,
  id: string,
  action: 'pause' | 'unpause' | 'archive',
): Promise<boolean | string> {
  return db.transaction(async (rawTx) => {
    const tx = rawTx as unknown as DbClient;
    const [trigger] = await tx
      .select()
      .from(agentTriggers)
      .where(
        and(
          isNull(agentTriggers.deletedAt),
          eq(agentTriggers.workspaceId, workspaceId),
          eq(agentTriggers.id, id),
          isNull(agentTriggers.archivedAt),
        ),
      )
      .for('update')
      .limit(1);
    if (!trigger) return false;
    const now = new Date();

    if (action === 'pause') {
      if (trigger.status === 'paused') return true;
      await tx
        .update(agentTriggers)
        .set({
          status: 'paused',
          generation: trigger.generation + 1,
          nextFireAt: null,
          updatedAt: now,
        })
        .where(
          and(
            isNull(agentTriggers.deletedAt),
            eq(agentTriggers.workspaceId, workspaceId),
            eq(agentTriggers.id, id),
          ),
        );
      return true;
    }
    if (action === 'archive') {
      await tx
        .update(agentTriggers)
        .set({
          status: 'archived',
          generation: trigger.generation + 1,
          nextFireAt: null,
          archivedAt: now,
          deletedAt: now,
          updatedAt: now,
        })
        .where(
          and(
            isNull(agentTriggers.deletedAt),
            eq(agentTriggers.workspaceId, workspaceId),
            eq(agentTriggers.id, id),
          ),
        );
      return true;
    }
    if (trigger.status === 'active') return true;

    if (!(await activeEnvironmentExists(tx, workspaceId, trigger.environmentId, true))) {
      return 'environment not found';
    }
    const vaultError = await validateSessionVaultIds(tx, workspaceId, trigger.vaultIds, true);
    if (vaultError) return vaultError;
    const agent = await resolvePinnedAgent(
      tx,
      workspaceId,
      trigger.agentId,
      trigger.agentVersion,
      true,
    );
    if (!agent) return `agent ${trigger.agentId} version ${trigger.agentVersion} not found`;
    let nextFireAt: Date;
    try {
      nextFireAt = nextTriggerOccurrence(trigger.cronExpression, trigger.timezone, now);
    } catch (error) {
      return error instanceof Error ? error.message : 'invalid schedule';
    }
    await tx
      .update(agentTriggers)
      .set({
        status: 'active',
        generation: trigger.generation + 1,
        nextFireAt,
        lastError: null,
        updatedAt: now,
      })
      .where(
        and(
          isNull(agentTriggers.deletedAt),
          eq(agentTriggers.workspaceId, workspaceId),
          eq(agentTriggers.id, id),
        ),
      );
    return true;
  });
}

async function resolvePinnedAgent(
  db: DbClient,
  workspaceId: string,
  agentId: string,
  requestedVersion?: number,
  lock = false,
): Promise<{ id: string; version: number } | null> {
  const base = db
    .select({ id: agents.id, currentVersion: agents.version })
    .from(agents)
    .where(
      and(
        isNull(agents.deletedAt),
        eq(agents.workspaceId, workspaceId),
        eq(agents.id, agentId),
        isNull(agents.archivedAt),
      ),
    );
  const rows = lock ? await base.for('share').limit(1) : await base.limit(1);
  const agent = rows[0];
  if (!agent) return null;
  const version = requestedVersion ?? agent.currentVersion;
  const versionBase = db
    .select({ version: agentVersions.version })
    .from(agentVersions)
    .where(
      and(
        eq(agentVersions.workspaceId, workspaceId),
        eq(agentVersions.agentId, agentId),
        eq(agentVersions.version, version),
      ),
    );
  const versions = lock ? await versionBase.for('share').limit(1) : await versionBase.limit(1);
  return versions[0] ? { id: agentId, version } : null;
}

async function activeEnvironmentExists(
  db: DbClient,
  workspaceId: string,
  environmentId: string,
  lock = false,
): Promise<boolean> {
  const base = db
    .select({ id: environments.id })
    .from(environments)
    .where(
      and(
        isNull(environments.deletedAt),
        eq(environments.workspaceId, workspaceId),
        eq(environments.id, environmentId),
        isNull(environments.archivedAt),
      ),
    );
  const rows = lock ? await base.for('share').limit(1) : await base.limit(1);
  return rows.length > 0;
}

function normalizeAgentRef(
  agent: string | { id: string; type?: 'agent'; version?: number | undefined },
): {
  id: string;
  version?: number;
} {
  if (typeof agent === 'string') return { id: toInternalId(agent) };
  return { id: toInternalId(agent.id), ...(agent.version ? { version: agent.version } : {}) };
}

async function loadTrigger(db: DbClient, workspaceId: string, id: string) {
  const [row] = await db
    .select()
    .from(agentTriggers)
    .where(
      and(
        isNull(agentTriggers.deletedAt),
        eq(agentTriggers.workspaceId, workspaceId),
        eq(agentTriggers.id, id),
      ),
    )
    .limit(1);
  return row ? triggerToApi(row) : null;
}

function triggerToApi(row: typeof agentTriggers.$inferSelect) {
  return {
    id: row.id,
    type: 'trigger' as const,
    name: row.name,
    agent: { type: 'agent' as const, id: row.agentId, version: row.agentVersion },
    session_mode: 'SESSION_PER_EVENT' as const,
    source: {
      type: 'cron' as const,
      schedule: row.cronExpression,
      timezone: row.timezone,
      payload: row.payload,
    },
    session: {
      environment_id: row.environmentId,
      title_template: row.titleTemplate ?? null,
      metadata: normalizeStoredMetadata(row.metadata),
      vault_ids: row.vaultIds,
    },
    replicas: 1 as const,
    status: row.status,
    next_fire_at: row.nextFireAt?.toISOString() ?? null,
    last_fired_at: row.lastFiredAt?.toISOString() ?? null,
    error: row.lastError ?? null,
    archived_at: row.archivedAt?.toISOString() ?? null,
    created_at: row.createdAt.toISOString(),
    updated_at: row.updatedAt.toISOString(),
  };
}

function triggerId(req: FastifyRequest): string {
  return toInternalId((req.params as { id: string }).id);
}

function parseBooleanQuery(
  raw: string | undefined,
  defaultValue: boolean,
): { value: boolean } | { error: string } {
  if (raw === undefined) return { value: defaultValue };
  if (raw === 'true') return { value: true };
  if (raw === 'false') return { value: false };
  return { error: 'include_archived must be true or false' };
}

function scheduleError(req: FastifyRequest, reply: FastifyReply, error: unknown) {
  return invalidRequest(
    req,
    reply,
    error instanceof TriggerScheduleError || error instanceof TriggerSessionTemplateError
      ? error.message
      : 'invalid Trigger configuration',
  );
}

function invalidRequest(req: FastifyRequest, reply: FastifyReply, message: string) {
  return reply.code(400).send(buildClaudeErrorResponse(req.id, 'invalid_request_error', message));
}

function notFound(req: FastifyRequest, reply: FastifyReply, message: string) {
  return reply.code(404).send(buildClaudeErrorResponse(req.id, 'not_found_error', message));
}
