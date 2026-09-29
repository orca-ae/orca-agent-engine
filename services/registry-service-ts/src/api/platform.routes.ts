// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { eq, sql } from 'drizzle-orm';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { AgentObservabilityCaptureModeSchema } from '../contracts/agent-observability.contract.js';
import {
  newOrganizationObservabilitySettings,
  newWorkspaceObservabilitySettings,
} from '../domain/agent-observability-settings.js';
import { newId } from '../domain/versioning.js';
import type { DbClient } from '../persistence/postgres/client.js';
import { runPlatformMutation as runMutation, writePlatformAudit } from './platform-mutations.js';
import { registerPlatformAgentObservabilityRoutes } from './platform-agent-observability.routes.js';
import {
  agentObservabilityOrganizationSettings,
  agentObservabilityWorkspaceSettings,
  organizations,
  workspaces,
} from '../persistence/postgres/schema.js';

const CREATE_ORGANIZATION_SCOPE = 'POST /v1/platform/organizations';
const CREATE_ORGANIZATION_FIELDS = ['name', 'audience', 'capture_ceiling'] as const;
const CREATE_WORKSPACE_FIELDS = ['name'] as const;

/**
 * Bound on a configured organization audience. Audiences are issuer-side
 * identifiers, commonly a URL, so this is headroom rather than a policy; what
 * it exists to stop is an unbounded string reaching an indexed column.
 */
const AUDIENCE_MAX_LENGTH = 200;

export function registerPlatformRoutes(app: FastifyInstance, db: DbClient): void {
  registerPlatformAgentObservabilityRoutes(app, db);
  app.post('/v1/platform/organizations', async (req, reply) => {
    const body = objectBody(req.body);
    if (!validateOnlyFields(body, CREATE_ORGANIZATION_FIELDS, reply)) return;
    const name = requiredName(body['name'], reply);
    if (!name) return;
    const audience = optionalAudience(body['audience'], reply);
    if (audience === false) return;
    const captureCeiling = AgentObservabilityCaptureModeSchema.safeParse(
      body['capture_ceiling'] === undefined ? 'metadata_only' : body['capture_ceiling'],
    );
    if (!captureCeiling.success) return reply.code(400).send({ error: 'invalid capture_ceiling' });
    const idempotencyKey = parseIdempotencyKey(req, reply);
    if (idempotencyKey === false) return;

    const result = await runMutation(
      db,
      req,
      CREATE_ORGANIZATION_SCOPE,
      idempotencyKey,
      body,
      async (tx) => {
        if (audience !== null) {
          // The partial unique index on `audience` is the authority, but it can
          // only answer by aborting the transaction — which would take the audit
          // and idempotency writes with it and surface as a 500. Serializing
          // creates that name the same audience lets the check below be both
          // race-free and a clean 409. Taken after the idempotency lock in
          // `runMutation`, so the two are always acquired in one order.
          await tx.execute(
            sql`select pg_advisory_xact_lock(hashtextextended(${`organization-audience:${audience}`}, 0))`,
          );
          const taken = await tx
            .select({ id: organizations.id })
            .from(organizations)
            .where(eq(organizations.audience, audience))
            .limit(1);
          if (taken[0]) {
            return {
              status: 409,
              body: { error: 'audience is already assigned to another organization' },
            };
          }
        }

        const now = new Date();
        const organization = {
          id: newId('org'),
          name,
          audience,
          status: 'active',
          createdAt: now,
          updatedAt: now,
        };
        await tx.insert(organizations).values(organization);
        // This organization was just inserted in this transaction. The conflict
        // can only be its mixed-version trigger's freshly seeded setting, never
        // an existing organization's policy. No Session can observe it before commit.
        await tx
          .insert(agentObservabilityOrganizationSettings)
          .values({
            ...newOrganizationObservabilitySettings(organization.id, now),
            captureCeiling: captureCeiling.data,
          })
          .onConflictDoUpdate({
            target: agentObservabilityOrganizationSettings.organizationId,
            set: { captureCeiling: captureCeiling.data },
          });
        await writePlatformAudit(
          tx,
          req,
          'organization.created',
          'organization',
          organization.id,
          organization.id,
          null,
          {
            name,
            audience,
            ...(body['capture_ceiling'] === undefined
              ? {}
              : { capture_ceiling: captureCeiling.data }),
          },
        );
        return { status: 201, body: organizationWire(organization) };
      },
    );
    return sendSerialized(reply, result);
  });

  app.post('/v1/platform/organizations/:organizationId/workspaces', async (req, reply) => {
    const body = objectBody(req.body);
    if (!validateOnlyFields(body, CREATE_WORKSPACE_FIELDS, reply)) return;
    const name = requiredName(body['name'], reply);
    if (!name) return;
    const idempotencyKey = parseIdempotencyKey(req, reply);
    if (idempotencyKey === false) return;

    const { organizationId } = req.params as { organizationId: string };
    const scope = `POST /v1/platform/organizations/${organizationId}/workspaces`;
    const result = await runMutation(db, req, scope, idempotencyKey, body, async (tx) => {
      const organizationRows = await tx
        .select()
        .from(organizations)
        .where(eq(organizations.id, organizationId))
        // Block status changes while allowing other workspace inserts' FK
        // checks. FOR UPDATE can deadlock against a competing name insertion.
        .for('no key update')
        .limit(1);
      const organization = organizationRows[0];
      if (!organization) {
        return { status: 404, body: { error: 'organization not found' } };
      }
      if (organization.status !== 'active') {
        return { status: 409, body: { error: 'organization is not active' } };
      }

      const now = new Date();
      const workspace = {
        id: newId('wrkspc'),
        organizationId,
        name,
        status: 'active',
        createdBy: req.platformAuth!.principal,
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      };
      await tx.insert(workspaces).values(workspace);
      // Intentionally redundant with migration 0048's mixed-version trigger;
      // retained as the durable writer path after that trigger is removed.
      await tx
        .insert(agentObservabilityWorkspaceSettings)
        .values(newWorkspaceObservabilitySettings(organizationId, workspace.id, now))
        .onConflictDoNothing();
      await writePlatformAudit(
        tx,
        req,
        'workspace.created',
        'workspace',
        workspace.id,
        organizationId,
        workspace.id,
        { name },
      );
      return { status: 201, body: workspaceWire(workspace) };
    });
    return sendSerialized(reply, result);
  });
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

/**
 * Reads the optional organization `audience`.
 *
 * Absent is the ordinary case and answers null — an organization that takes
 * part in no audience-based workspace resolution. Anything present must be a
 * usable audience: an explicit `null`, an empty string or a non-string is a 400
 * rather than a quiet "no audience", because this route only ever sets the
 * value and offers no way to correct it afterwards. `false` reports that a
 * reply has already been sent, as {@link parseIdempotencyKey} does.
 */
function optionalAudience(value: unknown, reply: FastifyReply): string | null | false {
  if (value === undefined) return null;
  if (
    typeof value !== 'string' ||
    value.trim().length === 0 ||
    value.trim().length > AUDIENCE_MAX_LENGTH
  ) {
    reply.code(400).send({
      error: `audience must be a non-empty string of at most ${AUDIENCE_MAX_LENGTH} characters`,
    });
    return false;
  }
  return value.trim();
}

/**
 * Rejects any field the route does not deliberately accept.
 *
 * The allowlist is per route and passed in rather than inferred, so widening
 * one body is a visible edit at its call site: an unrecognised field on a
 * provisioning route is far more likely to be a caller believing it set
 * something (an id, an organization, a policy) than a harmless typo.
 */
function validateOnlyFields(
  body: Record<string, unknown>,
  allowed: readonly string[],
  reply: FastifyReply,
): boolean {
  const unsupported = Object.keys(body).filter((field) => !allowed.includes(field));
  if (unsupported.length === 0) return true;
  reply.code(400).send({ error: `unsupported request fields: ${unsupported.sort().join(', ')}` });
  return false;
}

function parseIdempotencyKey(req: FastifyRequest, reply: FastifyReply): string | undefined | false {
  const value = req.headers['idempotency-key'];
  if (value === undefined) return undefined;
  const normalized = typeof value === 'string' ? value.trim() : '';
  if (normalized.length === 0 || normalized.length > 255) {
    reply
      .code(400)
      .send({ error: 'idempotency-key must be a non-empty string of at most 255 characters' });
    return false;
  }
  return normalized;
}

function sendSerialized(
  reply: FastifyReply,
  result: { status: number; serializedBody: string },
): FastifyReply {
  return reply.code(result.status).type('application/json').send(result.serializedBody);
}

function organizationWire(row: typeof organizations.$inferSelect): Record<string, unknown> {
  return {
    type: 'organization',
    id: row.id,
    name: row.name,
    audience: row.audience,
    status: row.status,
    created_at: row.createdAt.toISOString(),
  };
}

function workspaceWire(row: typeof workspaces.$inferSelect): Record<string, unknown> {
  return {
    type: 'workspace',
    id: row.id,
    name: row.name,
    created_at: row.createdAt.toISOString(),
    archived_at: row.archivedAt?.toISOString() ?? null,
  };
}
