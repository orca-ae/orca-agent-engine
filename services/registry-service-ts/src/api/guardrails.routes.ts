// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { and, desc, eq, isNull, or, sql, type SQL } from 'drizzle-orm';
import { z } from 'zod';
import { listGuardrailTypes, type Phase, type Scope } from '@orca/guardrails';
import type { DbClient } from '../persistence/postgres/client.js';
import { agents, guardrails, workspaces } from '../persistence/postgres/schema.js';
import { newId } from '../domain/versioning.js';
import { buildClaudeErrorResponse, type ClaudeErrorType } from '../contracts/common.js';
import {
  GUARDRAILS_API_PREFIX,
  GuardrailCreate,
  GuardrailScope,
  GuardrailUpdate,
  resolveGuardrailAuthoring,
  ruleToStorage,
  ruleToWire,
  type GuardrailAuthoringError,
} from '../contracts/guardrails.contract.js';
import {
  applyMetadataPatch,
  normalizeStoredMetadata,
  parseMetadata,
  parseMetadataPatch,
  toJsonMetadata,
  validateMetadataLimits,
} from './metadata.js';
import {
  decodeCreatedAtCursor,
  encodeCreatedAtCursor,
  parsePositiveIntQueryParam,
} from './query-params.js';

const GUARDRAIL_LIST_PAGE_DEFAULT = 100;
const GUARDRAIL_LIST_PAGE_MAX = 100;

/**
 * The one sentence a workspace principal gets when it touches the tier above
 * it. Deliberately identical across create, update, archive and delete: the
 * boundary is one rule, not four coincidences.
 */
export const ORGANIZATION_TIER_MESSAGE =
  'organization-scoped guardrails are managed by the organization';

export type GuardrailRow = typeof guardrails.$inferSelect;

/**
 * Workspace-facing guardrail CRUD.
 *
 * The tier model is the whole point of this file. A workspace principal can see
 * every guardrail that applies to it — including the organization's, which it
 * did not write — and can write only the tiers it owns. Visibility without
 * authority is what makes an organization guardrail auditable rather than
 * invisible, and it is why every read here spans two ownership predicates while
 * every write spans one.
 */
export function registerGuardrailsRoutes(app: FastifyInstance, db: DbClient): void {
  app.post(`${GUARDRAILS_API_PREFIX}/guardrails`, async (req, reply) => {
    const auth = req.auth!;
    const parsed = GuardrailCreate.safeParse(req.body);
    if (!parsed.success) return badRequest(reply, req, zodMessage(parsed.error));
    const body = parsed.data;

    if (body.scope === 'organization') return organizationTierForbidden(reply, req);

    const rule = ruleToStorage(body.rule);
    const scope = body.scope ?? 'workspace';
    const authored = resolveGuardrailAuthoring({ rule, scope, phases: body.phases });
    if (!authored.ok) return badRequest(reply, req, authoringMessage(authored.errors));

    const metadata = parseMetadata(body.metadata);
    if (!metadata.ok) return badRequest(reply, req, metadata.error!);

    const organizationId = await loadOrganizationId(db, auth.workspaceId);
    const now = new Date();
    const row = {
      id: newId('grd'),
      organizationId,
      workspaceId: auth.workspaceId,
      name: body.name,
      description: body.description ?? null,
      enabled: body.enabled ?? true,
      phases: authored.value.phases,
      scope,
      rule: authored.value.rule,
      metadata: toJsonMetadata(metadata.value!),
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    };
    await db.insert(guardrails).values(row);
    return reply.code(201).send(guardrailToWire(row));
  });

  app.get(`${GUARDRAILS_API_PREFIX}/guardrails`, async (req, reply) => {
    const auth = req.auth!;
    const q = req.query as { limit?: string; page?: string; include_archived?: string };
    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: GUARDRAIL_LIST_PAGE_DEFAULT,
      max: GUARDRAIL_LIST_PAGE_MAX,
    });
    if (!parsedLimit.ok) return badRequest(reply, req, parsedLimit.error!);
    const includeArchived = parseIncludeArchived(q.include_archived);
    if ('error' in includeArchived) return badRequest(reply, req, includeArchived.error);

    const organizationId = await loadOrganizationId(db, auth.workspaceId);
    const visible = visibleTo(auth.workspaceId, organizationId);

    const cursor = decodeCreatedAtCursor(q.page);
    if (q.page && !cursor) return badRequest(reply, req, 'invalid page');
    let cursorPredicate: SQL | undefined;
    if (cursor) {
      const cursorFilter = and(
        visible,
        includeArchived.value ? undefined : isNull(guardrails.archivedAt),
        eq(guardrails.id, cursor.id),
      )!;
      const cursorRows = await db
        .select({ id: guardrails.id })
        .from(guardrails)
        .where(and(isNull(guardrails.deletedAt), cursorFilter))
        .limit(1);
      if (cursorRows.length === 0) return badRequest(reply, req, 'invalid page');
      cursorPredicate = sql`(${guardrails.createdAt}, ${guardrails.id}) < (select ${guardrails.createdAt}, ${guardrails.id} from ${guardrails} where ${cursorFilter} limit 1)`;
    }

    const limit = parsedLimit.value!;
    const rows = await db
      .select()
      .from(guardrails)
      .where(
        and(
          isNull(guardrails.deletedAt),
          visible,
          includeArchived.value ? undefined : isNull(guardrails.archivedAt),
          cursorPredicate,
        ),
      )
      .orderBy(desc(guardrails.createdAt), desc(guardrails.id))
      .limit(limit + 1);
    const pageRows = rows.slice(0, limit);
    return reply.send({
      data: pageRows.map(guardrailToWire),
      next_page:
        rows.length > limit && pageRows.length > 0
          ? encodeCreatedAtCursor(pageRows[pageRows.length - 1]!)
          : null,
    });
  });

  // Registered before `/guardrails/:id` would ever be consulted for it, and on a
  // distinct path segment, so the catalog can never be read as an id lookup.
  app.get(`${GUARDRAILS_API_PREFIX}/guardrailtypes`, async (_req, reply) => {
    // Served verbatim: the schema a client validates against is the one the
    // server enforces, because they are the same object.
    return reply.send({ data: listGuardrailTypes() });
  });

  app.get(`${GUARDRAILS_API_PREFIX}/guardrails/:id`, async (req, reply) => {
    const row = await loadVisible(db, req);
    if (!row) return notFound(reply, req);
    return reply.send(guardrailToWire(row));
  });

  app.post(`${GUARDRAILS_API_PREFIX}/guardrails/:id`, async (req, reply) => {
    const auth = req.auth!;
    const parsed = GuardrailUpdate.safeParse(req.body);
    if (!parsed.success) return badRequest(reply, req, zodMessage(parsed.error));
    const body = parsed.data;
    if (body.scope === 'organization') return organizationTierForbidden(reply, req);

    const existing = await loadVisible(db, req);
    if (!existing) return notFound(reply, req);
    if (existing.scope === 'organization') return organizationTierForbidden(reply, req);

    // The effective rule and phases are resolved together and never inferred
    // apart: a rule swap that leaves the stored phases unreachable is rejected
    // rather than silently re-pointed, because moving where a guardrail fires
    // is a policy change the author has to make on purpose.
    const rule = body.rule ? ruleToStorage(body.rule) : storedRule(existing);
    const phases = body.phases ?? storedPhases(existing);
    const scope = body.scope ?? storedScope(existing);
    const authored = resolveGuardrailAuthoring({ rule, scope, phases });
    if (!authored.ok) return badRequest(reply, req, authoringMessage(authored.errors));

    let metadata = existing.metadata;
    if (body.metadata !== undefined) {
      const patch = parseMetadataPatch(body.metadata);
      if (!patch.ok) return badRequest(reply, req, patch.error!);
      const patched = applyMetadataPatch(existing.metadata, patch.value!);
      const limitError = validateMetadataLimits(patched);
      if (limitError) return badRequest(reply, req, limitError);
      metadata = toJsonMetadata(patched);
    }

    const now = new Date();
    const next = {
      ...existing,
      name: body.name ?? existing.name,
      description: body.description !== undefined ? body.description : existing.description,
      enabled: body.enabled ?? existing.enabled,
      phases: authored.value.phases,
      scope,
      rule: authored.value.rule,
      metadata,
      updatedAt: now,
    };
    await db
      .update(guardrails)
      .set({
        name: next.name,
        description: next.description,
        enabled: next.enabled,
        phases: next.phases,
        scope: next.scope,
        rule: next.rule,
        metadata: next.metadata,
        updatedAt: now,
      })
      .where(
        and(
          isNull(guardrails.deletedAt),
          eq(guardrails.id, existing.id),
          eq(guardrails.workspaceId, auth.workspaceId),
        ),
      );
    return reply.send(guardrailToWire(next));
  });

  app.post(`${GUARDRAILS_API_PREFIX}/guardrails/:id/archive`, async (req, reply) => {
    const auth = req.auth!;
    const existing = await loadVisible(db, req);
    if (!existing) return notFound(reply, req);
    if (existing.scope === 'organization') return organizationTierForbidden(reply, req);

    const archivedAt = existing.archivedAt ?? new Date();
    await db
      .update(guardrails)
      .set({ archivedAt, updatedAt: archivedAt })
      .where(
        and(
          isNull(guardrails.deletedAt),
          eq(guardrails.id, existing.id),
          eq(guardrails.workspaceId, auth.workspaceId),
        ),
      );
    return reply.send(guardrailToWire({ ...existing, archivedAt, updatedAt: archivedAt }));
  });

  app.delete(`${GUARDRAILS_API_PREFIX}/guardrails/:id`, async (req, reply) => {
    const auth = req.auth!;
    const id = (req.params as { id: string }).id;
    const organizationId = await loadOrganizationId(db, auth.workspaceId);
    const result = await db.transaction(async (tx) => {
      const rows = await tx
        .select()
        .from(guardrails)
        .where(
          and(
            isNull(guardrails.deletedAt),
            visibleTo(auth.workspaceId, organizationId),
            eq(guardrails.id, id),
          ),
        )
        .for('update')
        .limit(1);
      const existing = rows[0];
      if (!existing) return 'not_found' as const;
      if (existing.scope === 'organization') return 'forbidden' as const;

      // An agent that names a guardrail explicitly is relying on it. Deleting
      // the row out from under that reference would leave the agent running
      // with one fewer guardrail and nothing anywhere saying so.
      const referencing = await tx
        .select({ id: agents.id })
        .from(agents)
        .where(
          and(
            isNull(agents.deletedAt),
            eq(agents.workspaceId, auth.workspaceId),
            sql`${agents.guardrailIds} @> ${JSON.stringify([id])}::jsonb`,
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
            eq(guardrails.id, id),
            eq(guardrails.workspaceId, auth.workspaceId),
          ),
        );
      return 'deleted' as const;
    });

    if (result === 'not_found') return notFound(reply, req);
    if (result === 'forbidden') return organizationTierForbidden(reply, req);
    if (result === 'in_use') {
      return fail(
        reply,
        req,
        409,
        'conflict_error',
        'guardrail is referenced by one or more agents',
      );
    }
    return reply.send({ id, type: 'guardrail_deleted' });
  });
}

/**
 * Everything a workspace principal may see: its own guardrails plus the
 * organization tier that applies to it whether it asked for it or not.
 */
function visibleTo(workspaceId: string, organizationId: string): SQL {
  return or(
    eq(guardrails.workspaceId, workspaceId),
    and(eq(guardrails.organizationId, organizationId), eq(guardrails.scope, 'organization')),
  )!;
}

async function loadVisible(db: DbClient, req: FastifyRequest): Promise<GuardrailRow | null> {
  const auth = req.auth!;
  const id = (req.params as { id: string }).id;
  const organizationId = await loadOrganizationId(db, auth.workspaceId);
  const rows = await db
    .select()
    .from(guardrails)
    .where(
      and(
        isNull(guardrails.deletedAt),
        visibleTo(auth.workspaceId, organizationId),
        eq(guardrails.id, id),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

/**
 * The caller's organization comes from the workspace row rather than the
 * request, so an organization tier cannot be selected by a client.
 *
 * Authentication has already resolved the principal to an active workspace, so
 * an absent row here is a broken invariant rather than a client error. It
 * raises instead of returning a sentinel: every caller would otherwise have to
 * invent an answer for a case that cannot happen, and the four answers would
 * not agree.
 */
export async function loadOrganizationId(db: DbClient, workspaceId: string): Promise<string> {
  const rows = await db
    .select({ organizationId: workspaces.organizationId })
    .from(workspaces)
    .where(eq(workspaces.id, workspaceId))
    .limit(1);
  const organizationId = rows[0]?.organizationId;
  if (!organizationId) {
    throw new Error(`authenticated workspace ${workspaceId} has no organization row`);
  }
  return organizationId;
}

export function storedPhases(row: { phases: unknown }): Phase[] {
  return row.phases as Phase[];
}

export function storedRule(row: { rule: unknown }) {
  return ruleToStorage(ruleToWire(row.rule));
}

/**
 * The stored scope, narrowed.
 *
 * A table check restricts the column to the three scopes, so an unparseable
 * value is a corrupt row rather than a client mistake — it throws rather than
 * becoming a 400 that would blame the caller. The narrowing matters because the
 * scope now reaches the rule compiler, which refuses a builtin authored outside
 * its `allowedScopes`.
 */
export function storedScope(row: { scope: string }): Scope {
  const parsed = GuardrailScope.safeParse(row.scope);
  if (!parsed.success) throw new Error(`guardrail row carries an unknown scope: ${row.scope}`);
  return parsed.data;
}

export function guardrailToWire(row: {
  id: string;
  name: string;
  description: string | null;
  enabled: boolean;
  phases: unknown;
  scope: string;
  rule: unknown;
  metadata: unknown;
  archivedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}) {
  return {
    id: row.id,
    type: 'guardrail' as const,
    name: row.name,
    description: row.description ?? '',
    enabled: row.enabled,
    phases: storedPhases(row),
    scope: row.scope,
    rule: ruleToWire(row.rule),
    metadata: normalizeStoredMetadata(row.metadata),
    archived_at: row.archivedAt?.toISOString() ?? null,
    created_at: row.createdAt.toISOString(),
    updated_at: row.updatedAt.toISOString(),
  };
}

/**
 * Flatten authoring errors into one message. Every problem is reported at once:
 * an author fixing a rule one 400 at a time is an author who gives up.
 */
export function authoringMessage(errors: readonly GuardrailAuthoringError[]): string {
  return errors
    .map((error) => (error.path ? `${error.path}: ${error.message}` : error.message))
    .join('; ');
}

export function zodMessage(error: z.ZodError): string {
  return error.issues
    .map((issue) => {
      const path = issue.path.join('.');
      return path ? `${path}: ${issue.message}` : issue.message;
    })
    .join('; ');
}

function parseIncludeArchived(value: string | undefined): { value: boolean } | { error: string } {
  if (value === undefined || value === 'false') return { value: false };
  if (value === 'true') return { value: true };
  return { error: 'include_archived must be true or false' };
}

/**
 * This API group sits outside `/v1`, where the Claude edge adapter rewrites
 * error payloads, so every failure here builds its own envelope. Clients get
 * one error shape across both groups.
 */
function fail(
  reply: FastifyReply,
  req: FastifyRequest,
  status: number,
  type: ClaudeErrorType,
  message: string,
) {
  return reply.code(status).send(buildClaudeErrorResponse(req.id, type, message));
}

function badRequest(reply: FastifyReply, req: FastifyRequest, message: string) {
  return fail(reply, req, 400, 'invalid_request_error', message);
}

function notFound(reply: FastifyReply, req: FastifyRequest) {
  return fail(reply, req, 404, 'not_found_error', 'guardrail not found');
}

function organizationTierForbidden(reply: FastifyReply, req: FastifyRequest) {
  return fail(reply, req, 403, 'permission_error', ORGANIZATION_TIER_MESSAGE);
}
