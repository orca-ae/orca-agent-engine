// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance } from 'fastify';
import { eq, and, isNull, desc, ne, sql, type SQL } from 'drizzle-orm';
import { validateHarnessDeployment } from '@orca/harness-catalog';
import { loadSessionHarnessBinding } from '../domain/session-harness-binding.js';
import type { DbClient } from '../persistence/postgres/client.js';
import { environments, sessions } from '../persistence/postgres/schema.js';
import { newId } from '../domain/versioning.js';
import { armEnvKeyColumns, revokeEnvKeyColumns } from '../domain/environment-key-state.js';
import { EnvironmentClaimStore } from '../domain/environment-claims.js';
import { computeWorkStats, type WorkStatsCounts } from '../domain/work-stats.js';
import { DISTRIBUTION_PENDING, DISTRIBUTION_ASSIGNED } from '../tunnel/session-distributor.js';
import type { EnvironmentLaunchTrigger } from './sessions.routes.js';
import { buildClaudeErrorResponse } from '../contracts/common.js';
import { toInternalId } from '../contracts/id-prefix.js';
import {
  configToStorage,
  hasOwn,
  hasPackages,
  legacyAptPackages,
  mergeNetworkingUpdate,
  networkingToApiConfig,
  normalizeLegacyPackages,
  normalizePackages,
  normalizeStoredPackages,
  packagesToApi,
  stripNullFields,
  type Packages,
} from '../contracts/environment-wire.js';
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

/**
 * Default heartbeat-staleness TTL (ms) for the work_stats claim-liveness check.
 * Matches the internal claim routes' default so "worker connected" here uses the
 * same boundary the reaper sweeps abandoned claims with.
 */
const DEFAULT_ENVIRONMENT_CLAIM_TTL_MS = 90_000;

/** Options for {@link registerEnvironmentsRoutes}. */
export interface EnvironmentsRoutesOptions {
  /**
   * Heartbeat-staleness TTL (ms) the `work_stats` route uses to decide whether
   * the environment's worker is currently connected. Defaults to
   * {@link DEFAULT_ENVIRONMENT_CLAIM_TTL_MS}; production threads
   * `config.environmentClaimTtlMs` so it matches the claim reaper.
   */
  environmentClaimTtlMs?: number;
  /**
   * Optional environment-launch lifecycle (I3). When set, the archive and
   * delete handlers best-effort call `lifecycle.terminate(id)` for a
   * `target=cloud` environment, tearing down its launcher-level box
   * alongside the credential revoke both handlers already perform.
   * `self_hosted` never calls it — its worker is operator-launched, so there
   * is no registry-managed box to tear down. When omitted, archive/delete
   * behave exactly as before (credential-revoke only) — the same
   * "omitted -> inert" posture every other cloud-launch wiring in this app
   * uses.
   */
  environmentLaunchLifecycle?: EnvironmentLaunchTrigger;
}

/**
 * Best-effort tear down the launcher-level box for a `target=cloud`
 * environment being archived/deleted. A no-op when no lifecycle is
 * configured, or when `target` is not `'cloud'` — `self_hosted` needs no box
 * teardown here: revoking the Env Key (which both callers already do)
 * suffices, since that worker was launched by an operator, not this
 * registry. NEVER throws — a terminate failure must not fail the
 * archive/delete request whose own DB write already committed by the time
 * this runs.
 *
 * Single-replica caveat: `EnvironmentLaunchLifecycle.terminate` only tears
 * down the launcher-level box when THIS replica is the one that launched it
 * (the launcher-level environment id is tracked in that module's in-process
 * memory only — see its module doc). Archiving/deleting a `target=cloud`
 * environment from a DIFFERENT replica than the one that launched it still
 * revokes the Environment Token (so the box can never authenticate a new
 * worker connection), but the box itself keeps running/billing until a
 * future durable, cross-replica teardown mechanism lands — a known,
 * documented gap, not something this fix closes.
 */
async function terminateCloudBox(
  lifecycle: EnvironmentLaunchTrigger | undefined,
  target: string | null,
  environmentId: string,
  log?: { warn?(obj: unknown, msg?: string): void },
): Promise<void> {
  if (lifecycle === undefined || target !== 'cloud') {
    return;
  }
  try {
    await lifecycle.terminate(environmentId);
  } catch (err) {
    log?.warn?.(
      { err, environmentId },
      'environment teardown: best-effort lifecycle.terminate failed',
    );
  }
}

const ENVIRONMENT_LIST_PAGE_DEFAULT = 100;
const ENVIRONMENT_LIST_PAGE_MAX = 100;

function isOrcaBetaRequest(headers: Record<string, string | string[] | undefined>): boolean {
  const value = headers['orca-beta'];
  return Array.isArray(value) ? value.length > 0 : typeof value === 'string' && value.length > 0;
}

interface EnvironmentCreateBody {
  name: string;
  description?: string | null;
  metadata?: unknown;
  config?: { packages?: unknown; type?: unknown; networking?: unknown } | null;
  packages?: unknown;
  networking?: Record<string, unknown>;
  image?: string | null;
  target?: string | null;
  egress_mode?: string | null;
  llm?: Record<string, unknown> | null;
  scope?: 'organization' | 'account' | null;
}

export function registerEnvironmentsRoutes(
  app: FastifyInstance,
  db: DbClient,
  options: EnvironmentsRoutesOptions = {},
): void {
  // Durable claim store: the work_stats route reads the environment's claim to
  // decide whether a worker is currently connected (heartbeat liveness). Shares
  // the registry's connection pool, like the internal claim routes.
  const claims = new EnvironmentClaimStore(db);
  const claimTtlMs = options.environmentClaimTtlMs ?? DEFAULT_ENVIRONMENT_CLAIM_TTL_MS;
  // I3: best-effort cloud-box teardown on archive/delete. `undefined` when no
  // cloud launcher backend is configured for this deployment — see
  // `terminateCloudBox`'s doc.
  const lifecycle = options.environmentLaunchLifecycle;

  app.post('/v1/environments', async (req, reply) => {
    const auth = req.auth!;
    const body = req.body as EnvironmentCreateBody;
    const requestError = validateEnvironmentBody(body, false);
    if (requestError) return reply.code(400).send({ error: requestError });
    const descriptionError = validateDescription(body.description);
    if (descriptionError) return reply.code(400).send({ error: descriptionError });
    const metadata = parseMetadata(body.metadata);
    if (!metadata.ok) return reply.code(400).send({ error: metadata.error });
    let target: string | null = body.target ?? 'cloud';
    let networking: Record<string, unknown> = (body.networking ?? {
      type: 'unrestricted',
    }) as Record<string, unknown>;
    if (body.config !== undefined) {
      const resolved = configToStorage(body.config);
      if (resolved && 'error' in resolved) return reply.code(400).send({ error: resolved.error });
      if (resolved) {
        target = resolved.target;
        networking = stripNullFields(resolved.networking);
      } else if (!isValidTarget(body.target)) {
        return reply.code(400).send({ error: "target must be 'cloud' or 'self_hosted'" });
      }
    } else if (!isValidTarget(body.target)) {
      return reply.code(400).send({ error: "target must be 'cloud' or 'self_hosted'" });
    }
    if (!isValidEgressMode(body.egress_mode)) {
      return reply.code(400).send({ error: "egress_mode must be 'gateway' or 'sidecar'" });
    }
    const id = newId('env');
    const now = new Date();
    const image = normalizeImage(body.image);
    // Arm an env key on create: persist only the digest + expiry, return the
    // raw key once below. The raw key is never stored and is unrecoverable.
    const key = armEnvKeyColumns(now);
    const packages = parsePackagesFromBody(body);
    if (!packages.ok) return reply.code(400).send({ error: packages.error });
    if (target === 'self_hosted' && hasPackages(packages.value)) {
      return reply
        .code(400)
        .send({ error: 'config.packages is only supported for cloud environments' });
    }
    if (body.scope != null && target !== 'self_hosted') {
      return reply
        .code(400)
        .send({ error: 'scope is only supported for self-hosted environments' });
    }

    try {
      await db.insert(environments).values({
        id,
        workspaceId: auth.workspaceId,
        name: body.name,
        description: body.description ?? null,
        metadata: toJsonMetadata(metadata.value),
        packages: packages.value,
        networking,
        image,
        target,
        egressMode: body.egress_mode ?? null,
        llm: body.llm ?? null,
        envKeyDigest: key.envKeyDigest,
        envKeyExpiresAt: key.envKeyExpiresAt,
        scope: body.scope ?? null,
        archivedAt: null,
        createdAt: now,
        updatedAt: now,
      });
    } catch (error) {
      if (!isEnvironmentNameConflict(error)) throw error;
      return reply
        .code(409)
        .send(
          buildClaudeErrorResponse(req.id, 'conflict_error', 'environment name is already in use'),
        );
    }

    const orcaBeta = isOrcaBetaRequest(req.headers);
    const out = await loadEnvironment(db, auth.workspaceId, id, orcaBeta);
    // `orca-beta` only: the 200 carries the raw key exactly once (response-only,
    // never persisted raw). Anthropic's create response has no key concept, so a
    // default caller gets their exact projection and nothing more. The key is
    // still armed on the row — an `orca-beta` `rotate-key` issues a fresh one,
    // which is the only recovery path either way, since the raw value is
    // unrecoverable the moment this response is dropped.
    return reply.code(200).send(orcaBeta ? { ...out, env_key: key.raw } : out);
  });

  app.get('/v1/environments', async (req, reply) => {
    const auth = req.auth!;
    const q = req.query as { limit?: string; page?: string; include_archived?: string };
    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: ENVIRONMENT_LIST_PAGE_DEFAULT,
      max: ENVIRONMENT_LIST_PAGE_MAX,
    });
    if (!parsedLimit.ok) return reply.code(400).send({ error: parsedLimit.error });
    const includeArchived = parseIncludeArchived(q.include_archived);
    if ('error' in includeArchived) return reply.code(400).send({ error: includeArchived.error });
    const cursor = decodeCreatedAtCursor(q.page);
    if (q.page && !cursor) return reply.code(400).send({ error: 'invalid page' });
    let cursorPredicate: SQL | undefined;
    if (cursor) {
      const cursorFilter = and(
        eq(environments.workspaceId, auth.workspaceId),
        includeArchived.value ? undefined : isNull(environments.archivedAt),
        eq(environments.id, cursor.id),
      )!;
      const cursorRows = await db
        .select({ id: environments.id })
        .from(environments)
        .where(and(isNull(environments.deletedAt), cursorFilter))
        .limit(1);
      if (cursorRows.length === 0) return reply.code(400).send({ error: 'invalid page' });
      cursorPredicate = sql`(${environments.createdAt}, ${environments.id}) < (select ${environments.createdAt}, ${environments.id} from ${environments} where ${cursorFilter} limit 1)`;
    }
    const limit = parsedLimit.value!;
    const filters = and(
      eq(environments.workspaceId, auth.workspaceId),
      includeArchived.value ? undefined : isNull(environments.archivedAt),
      cursorPredicate,
    );
    const rows = await db
      .select()
      .from(environments)
      .where(and(isNull(environments.deletedAt), filters))
      .orderBy(desc(environments.createdAt), desc(environments.id))
      .limit(limit + 1);
    const pageRows = rows.slice(0, limit);
    const orcaBeta = isOrcaBetaRequest(req.headers);
    const items = pageRows.map((row) => toApi(row, orcaBeta));
    reply.send({
      data: items,
      next_page:
        rows.length > limit && pageRows.length > 0
          ? encodeCreatedAtCursor(pageRows[pageRows.length - 1]!)
          : null,
    });
  });

  app.get('/v1/environments/:id', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const env = await loadEnvironment(db, auth.workspaceId, id, isOrcaBetaRequest(req.headers));
    if (!env) return reply.code(404).send({ error: 'not found' });
    reply.send(env);
  });

  // Work-queue stats: how many sessions are pending (no worker has picked them
  // up), how many are assigned/in-flight, and whether a worker is currently
  // connected (durable-claim heartbeat liveness). Workspace-scoped: a missing
  // or cross-tenant environment 404s before any counting (the row lookup is the
  // tenancy boundary), so the counts never leak across workspaces. The pure
  // partition + liveness math lives in `domain/work-stats.ts`; this handler is
  // the thin DB read.
  app.get('/v1/environments/:id/work_stats', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    // Tenancy gate: resolve the environment within the caller's workspace first.
    // A malformed / unknown / cross-workspace id is simply "not found" — matching
    // the sibling GET and the contract's 200/404-only response set.
    const env = await loadEnvironment(db, auth.workspaceId, id);
    if (!env) return reply.code(404).send({ error: 'not found' });

    // Counts are scoped to the resolved environment AND its workspace, so a
    // session another workspace happens to point at this env id is never
    // counted. The claim is read from the durable store for liveness.
    const counts = await countEnvironmentWork(db, auth.workspaceId, id);
    const claim = await claims.getOwner(id);
    reply.send(computeWorkStats(counts, claim, claimTtlMs));
  });

  app.post('/v1/environments/:id', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    const body = req.body as Partial<EnvironmentCreateBody>;
    const requestError = validateEnvironmentBody(body, true);
    if (requestError) return reply.code(400).send({ error: requestError });
    if (!isValidTarget(body.target)) {
      return reply.code(400).send({ error: "target must be 'cloud' or 'self_hosted'" });
    }
    if (!isValidEgressMode(body.egress_mode)) {
      return reply.code(400).send({ error: "egress_mode must be 'gateway' or 'sidecar'" });
    }
    const descriptionError = validateDescription(body.description);
    if (descriptionError) return reply.code(400).send({ error: descriptionError });
    const metadataPatch =
      body.metadata !== undefined
        ? parseMetadataPatch(body.metadata)
        : { ok: true as const, value: {} };
    if (!metadataPatch.ok) return reply.code(400).send({ error: metadataPatch.error });

    const existing = await loadEnvironmentRow(db, auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'not found' });

    let metadata = existing.metadata;
    if (body.metadata !== undefined) {
      const environmentPatch = Object.fromEntries(
        Object.entries(metadataPatch.value).map(([key, value]) => [
          key,
          value === '' ? null : value,
        ]),
      );
      const patchedMetadata = applyMetadataPatch(existing.metadata, environmentPatch);
      const metadataError = validateMetadataLimits(patchedMetadata);
      if (metadataError) return reply.code(400).send({ error: metadataError });
      metadata = toJsonMetadata(patchedMetadata);
    }

    let nextTarget = body.target !== undefined ? body.target : existing.target;
    let nextNetworking =
      body.networking !== undefined ? body.networking : (existing.networking as unknown);
    if (body.config !== undefined) {
      const hasConfigNetworking = hasOwn(body.config, 'networking');
      const resolved = configToStorage(body.config);
      if (resolved && 'error' in resolved) return reply.code(400).send({ error: resolved.error });
      if (resolved) {
        nextTarget = resolved.target;
        // Legacy rows can have `target = null` while still presenting as cloud
        // (only `self_hosted` is a real non-cloud target), so gate on
        // `existing.target !== 'self_hosted'` rather than `=== 'cloud'`.
        if (body.config === null) {
          nextNetworking = stripNullFields(resolved.networking);
        } else if (resolved.target === 'cloud' && existing.target !== 'self_hosted') {
          // Omitted `config.networking` preserves the existing value wholesale;
          // a provided `limited` update merges sub-fields onto the existing
          // `limited` value (see mergeNetworkingUpdate doc comment).
          nextNetworking = hasConfigNetworking
            ? mergeNetworkingUpdate(existing.networking, resolved.networking)
            : existing.networking;
        } else {
          nextNetworking = stripNullFields(resolved.networking);
        }
      } else if (!isValidTarget(body.target)) {
        return reply.code(400).send({ error: "target must be 'cloud' or 'self_hosted'" });
      }
    } else if (!isValidTarget(body.target)) {
      return reply.code(400).send({ error: "target must be 'cloud' or 'self_hosted'" });
    }

    const existingTarget = existing.target === 'self_hosted' ? 'self_hosted' : 'cloud';
    const configChangesVariant =
      body.config !== undefined && nextTarget !== null && nextTarget !== existingTarget;
    const now = new Date();
    const packages = parsePackagesFromBody(
      body,
      configChangesVariant && nextTarget === 'self_hosted' ? {} : existing.packages,
    );
    if (!packages.ok) return reply.code(400).send({ error: packages.error });
    if (nextTarget === 'self_hosted' && hasPackages(packages.value)) {
      return reply
        .code(400)
        .send({ error: 'config.packages is only supported for cloud environments' });
    }
    const nextScope =
      body.scope !== undefined
        ? body.scope
        : configChangesVariant && nextTarget === 'cloud'
          ? null
          : existing.scope;
    if (nextScope != null && nextTarget !== 'self_hosted') {
      return reply
        .code(400)
        .send({ error: 'scope is only supported for self-hosted environments' });
    }
    try {
      const conflict = await db.transaction(async (tx) => {
        // Session creation takes a share lock on this same row before admission.
        // Validate bound Sessions and mutate the target under the exclusive lock.
        const [locked] = await tx
          .select({
            id: environments.id,
            target: environments.target,
            updatedAt: environments.updatedAt,
          })
          .from(environments)
          .where(
            and(
              eq(environments.id, id),
              eq(environments.workspaceId, auth.workspaceId),
              isNull(environments.deletedAt),
            ),
          )
          .for('update');
        if (!locked) return { status: 404, error: 'not found' };
        if (
          locked.target !== existing.target ||
          locked.updatedAt.getTime() !== existing.updatedAt.getTime()
        )
          return { status: 409, error: 'environment changed before the update completed' };
        if ((nextTarget ?? 'cloud') !== existingTarget) {
          const boundSessions = await tx
            .select({ agentId: sessions.agentId, agentVersion: sessions.agentVersion })
            .from(sessions)
            .where(
              and(
                eq(sessions.workspaceId, auth.workspaceId),
                eq(sessions.environmentId, id),
                isNull(sessions.deletedAt),
                isNull(sessions.archivedAt),
                ne(sessions.status, 'terminated'),
              ),
            );
          for (const session of boundSessions) {
            const selection = await loadSessionHarnessBinding(
              tx as unknown as DbClient,
              auth.workspaceId,
              session.agentId,
              session.agentVersion,
            );
            const deploymentError = validateHarnessDeployment(nextTarget ?? 'cloud', selection);
            if (deploymentError) {
              return {
                status: 400,
                error: `environment target conflicts with an existing Session: ${deploymentError}`,
              };
            }
          }
        }
        await tx
          .update(environments)
          .set({
            name: body.name ?? existing.name,
            description: body.description !== undefined ? body.description : existing.description,
            metadata,
            packages: packages.value,
            networking: nextNetworking,
            image: body.image !== undefined ? normalizeImage(body.image) : existing.image,
            target: nextTarget,
            egressMode: body.egress_mode !== undefined ? body.egress_mode : existing.egressMode,
            llm: body.llm !== undefined ? body.llm : existing.llm,
            scope: nextScope,
            updatedAt: now,
          })
          .where(
            and(
              isNull(environments.deletedAt),
              eq(environments.id, id),
              eq(environments.workspaceId, auth.workspaceId),
            ),
          );
        return null;
      });
      if (conflict?.status === 404) return reply.code(404).send({ error: conflict.error });
      if (conflict?.status === 409) return reply.code(409).send({ error: conflict.error });
      if (conflict) return reply.code(400).send({ error: conflict.error });
    } catch (error) {
      if (!isEnvironmentNameConflict(error)) throw error;
      return reply
        .code(409)
        .send(
          buildClaudeErrorResponse(req.id, 'conflict_error', 'environment name is already in use'),
        );
    }

    const out = await loadEnvironment(db, auth.workspaceId, id, isOrcaBetaRequest(req.headers));
    return reply.send(out);
  });

  app.post('/v1/environments/:id/rotate-key', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };

    const existing = await loadEnvironmentRow(db, auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'not found' });

    const now = new Date();
    // Rotate: the new digest replaces the prior one, atomically revoking the
    // previously issued key (its digest no longer matches anything stored).
    const key = armEnvKeyColumns(now);
    await db
      .update(environments)
      .set({
        envKeyDigest: key.envKeyDigest,
        envKeyExpiresAt: key.envKeyExpiresAt,
        updatedAt: now,
      })
      .where(
        and(
          isNull(environments.deletedAt),
          eq(environments.id, id),
          eq(environments.workspaceId, auth.workspaceId),
        ),
      );

    // The raw key is returned once here and never again.
    return reply.send({ env_key: key.raw, env_key_expires_at: key.envKeyExpiresAt.toISOString() });
  });

  app.post('/v1/environments/:id/revoke-key', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };

    const existing = await loadEnvironmentRow(db, auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'not found' });

    const now = new Date();
    // Explicit revoke: clear the digest + expiry so the previously issued key
    // stops authenticating, while the environment row survives (contrast DELETE,
    // which drops the row entirely). Idempotent — revoking an already-revoked
    // env is a safe no-op that still reports the now-unarmed state.
    const cleared = revokeEnvKeyColumns();
    await db
      .update(environments)
      .set({
        envKeyDigest: cleared.envKeyDigest,
        envKeyExpiresAt: cleared.envKeyExpiresAt,
        updatedAt: now,
      })
      .where(
        and(
          isNull(environments.deletedAt),
          eq(environments.id, id),
          eq(environments.workspaceId, auth.workspaceId),
        ),
      );

    const out = await loadEnvironment(db, auth.workspaceId, id, isOrcaBetaRequest(req.headers));
    return reply.send(out);
  });

  app.post('/v1/environments/:id/archive', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);
    // Archive is soft teardown: revoke the env key alongside setting
    // `archived_at` so an archived environment never retains a live, resolvable
    // credential; both archive and DELETE revoke its stored key. A `verifyEnvKey` consumer then can't
    // authenticate against an archived environment.
    const now = new Date();
    const cleared = revokeEnvKeyColumns();
    await db
      .update(environments)
      .set({
        archivedAt: now,
        envKeyDigest: cleared.envKeyDigest,
        envKeyExpiresAt: cleared.envKeyExpiresAt,
        updatedAt: now,
      })
      .where(
        and(
          isNull(environments.deletedAt),
          eq(environments.id, id),
          eq(environments.workspaceId, auth.workspaceId),
        ),
      );
    // Read the row, not the wire projection: `target` is `orca-beta`-only on
    // the wire, and teardown must not depend on the caller's headers.
    const row = await selectEnvironmentRow(db, auth.workspaceId, id);
    if (!row) return reply.code(404).send({ error: 'not found' });
    const env = toApi(row, isOrcaBetaRequest(req.headers));
    // I3: best-effort tear down the launcher-level box for a `target=cloud`
    // environment — self_hosted needs no box teardown (see
    // `terminateCloudBox`'s doc). Runs AFTER the DB write above already
    // committed, and never fails this request even if terminate throws.
    await terminateCloudBox(lifecycle, row.target, id, req.log);
    return reply.send(env);
  });

  app.delete('/v1/environments/:id', async (req, reply) => {
    const auth = req.auth!;
    const id = toInternalId((req.params as { id: string }).id);

    const result = await db.transaction(async (tx) => {
      // Session creation takes a SHARE lock on this row before inserting its
      // reference. Taking UPDATE here makes the in-use check + delete atomic
      // with concurrent session creation.
      const environmentRows = await tx
        .select({ id: environments.id, target: environments.target })
        .from(environments)
        .where(
          and(
            isNull(environments.deletedAt),
            eq(environments.id, id),
            eq(environments.workspaceId, auth.workspaceId),
          ),
        )
        .for('update')
        .limit(1);
      const environmentRow = environmentRows[0];
      if (!environmentRow) return { status: 'not_found' as const };

      const referencingSessions = await tx
        .select({ id: sessions.id })
        .from(sessions)
        .where(
          and(
            isNull(sessions.deletedAt),
            eq(sessions.workspaceId, auth.workspaceId),
            eq(sessions.environmentId, id),
          ),
        )
        .limit(1);
      if (referencingSessions[0]) return { status: 'in_use' as const };

      await tx
        .update(environments)
        .set({ deletedAt: new Date(), ...revokeEnvKeyColumns(), updatedAt: new Date() })
        .where(
          and(
            isNull(environments.deletedAt),
            eq(environments.id, id),
            eq(environments.workspaceId, auth.workspaceId),
          ),
        );
      return { status: 'deleted' as const, target: environmentRow.target };
    });

    if (result.status === 'not_found') return reply.code(404).send({ error: 'not found' });
    if (result.status === 'in_use') {
      return reply
        .code(409)
        .send(
          buildClaudeErrorResponse(
            req.id,
            'conflict_error',
            'environment is referenced by one or more sessions',
          ),
        );
    }
    // I3: best-effort tear down the launcher-level box for a `target=cloud`
    // environment, using the retained row's target. Never fails this request even if terminate throws.
    await terminateCloudBox(lifecycle, result.target, id, req.log);
    return reply.send({ id, type: 'environment_deleted' });
  });
}

function isEnvironmentNameConflict(error: unknown): boolean {
  let current: unknown = error;
  for (let depth = 0; depth < 3; depth += 1) {
    if (!current || typeof current !== 'object') return false;
    const record = current as { code?: unknown; constraint?: unknown; cause?: unknown };
    if (record.code === '23505') {
      return record.constraint === 'environments_ws_name_idx';
    }
    current = record.cause;
  }
  return false;
}

/**
 * These handlers read `req.body` manually (the ts-rest contract isn't enforced
 * at runtime here), so `target` must be validated explicitly. `undefined`/`null`
 * are allowed (no target); any other value must match the enum.
 */
function isValidTarget(target: unknown): boolean {
  return target === undefined || target === null || target === 'cloud' || target === 'self_hosted';
}

/**
 * Validate `egress_mode` like `target`: `undefined`/`null` mean "unset" (fall
 * back to the deployment default); any other value must match the enum the
 * `environments_egress_mode_check` constraint enforces.
 */
function isValidEgressMode(egressMode: unknown): boolean {
  return (
    egressMode === undefined ||
    egressMode === null ||
    egressMode === 'gateway' ||
    egressMode === 'sidecar'
  );
}

/**
 * Coerce an empty/whitespace-only image to `null` rather than persisting (and
 * later echoing back) a blank string as if it were a real value.
 *
 * This used to also guard a downstream `env.image ?? catalog.defaultImage`
 * resolution the harness performed — a blank-but-set string would have
 * silently shadowed that fallback. That resolution no longer exists anywhere
 * in code (removed with the in-sandbox DialIn retirement, B1), so there is
 * nothing left for a blank image to shadow; this is now purely a
 * data-hygiene normalization, not a fallback guard. (`harness-catalog`'s own
 * `defaultImage`/`port` fields are unrelated and still live — they describe
 * per-harness images elsewhere in the catalog; this function neither reads
 * nor affects them.)
 */
function normalizeImage(image: unknown): string | null {
  return typeof image === 'string' && image.trim() ? image : null;
}

function validateEnvironmentBody(value: unknown, update: boolean): string | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return 'environment body must be an object';
  }
  const body = value as Record<string, unknown>;
  if (!update || Object.prototype.hasOwnProperty.call(body, 'name')) {
    if (update && body.name === null) {
      // Claude accepts null on update; it preserves the existing required name.
    } else if (typeof body.name !== 'string' || body.name.length < 1 || body.name.length > 200) {
      return 'name must contain 1-200 characters';
    }
  }
  if (
    body.scope !== undefined &&
    body.scope !== null &&
    body.scope !== 'organization' &&
    body.scope !== 'account'
  ) {
    return "scope must be 'organization', 'account', or null";
  }
  return null;
}

function parseIncludeArchived(value: string | undefined): { value: boolean } | { error: string } {
  if (value === undefined || value === 'false') return { value: false };
  if (value === 'true') return { value: true };
  return { error: 'include_archived must be true or false' };
}

function parsePackagesFromBody(
  body: Partial<EnvironmentCreateBody>,
  fallback: unknown = {},
): { ok: true; value: Packages } | { ok: false; error: string } {
  try {
    const config = body.config;
    if (config === null) return { ok: true, value: {} };
    if (config !== undefined && (typeof config !== 'object' || Array.isArray(config))) {
      return { ok: false, error: 'config must be an object' };
    }

    if (config && hasOwn(config, 'packages')) {
      return { ok: true, value: normalizePackages(config.packages) };
    }
    if (hasOwn(body, 'packages')) {
      return { ok: true, value: normalizeLegacyPackages(body.packages) };
    }
    return { ok: true, value: normalizeStoredPackages(fallback) };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : 'invalid packages' };
  }
}

function validateDescription(description: unknown): string | null {
  if (description === undefined || description === null) {
    return null;
  }
  if (typeof description === 'string') {
    if (description.length > 1024) return 'description must be at most 1024 characters';
    return null;
  }
  return 'description must be a string or null';
}

function toApi(row: typeof environments.$inferSelect, orcaBeta: boolean) {
  const packages = normalizeStoredPackages(row.packages);
  const networkingConfig = networkingToApiConfig(row.target, row.networking);
  const config =
    row.target === 'self_hosted'
      ? { type: 'self_hosted' as const }
      : {
          type: 'cloud' as const,
          networking: networkingConfig?.networking ?? { type: 'unrestricted' as const },
          packages: packagesToApi(packages),
        };
  // `base` is Anthropic's `BetaEnvironment` projection and nothing else. The
  // default (non-`orca-beta`) wire stays byte-compatible with what
  // platform.claude.com publishes, so a client written against their spec never
  // sees a field it cannot account for. `scope` belongs here because it is
  // Anthropic's own optional field, not an Orca addition.
  const base = {
    id: row.id,
    type: 'environment' as const,
    name: row.name,
    description: row.description ?? '',
    metadata: normalizeStoredMetadata(row.metadata),
    config,
    ...(row.scope === 'organization' || row.scope === 'account' ? { scope: row.scope } : {}),
    archived_at: row.archivedAt?.toISOString() ?? null,
    created_at: row.createdAt.toISOString(),
    updated_at: row.updatedAt.toISOString(),
  };
  if (!orcaBeta) return base;
  // `orca-beta` callers opt in to the Orca-only projection: the legacy flat
  // fields plus the colocated-execution ones (egress routing, LLM binding, and
  // the Env Key's presence/expiry). Same opt-in shape `toPublicHttpEvent`
  // (which strips `seq` for non-beta callers) and `modelToApi` use.
  return {
    ...base,
    packages: legacyAptPackages(packages),
    networking: row.networking,
    image: row.image ?? null,
    target: row.target ?? null,
    egress_mode: row.egressMode ?? null,
    llm: row.llm ?? null,
    // Presence + expiry only — the raw key is never echoed on reads. The raw
    // key surfaces exactly twice: create and rotate-key, both once.
    env_key_set: row.envKeyDigest !== null,
    env_key_expires_at: row.envKeyExpiresAt?.toISOString() ?? null,
  };
}

/**
 * Count an environment's NON-archived sessions by their distribution lifecycle
 * position, for the work_stats partition. One conditional aggregate,
 * scoped to the environment AND its workspace (so a foreign-workspace session
 * pointing at this env id is excluded) AND `archived_at IS NULL` AND `deleted_at IS NULL` (an
 * archived or deleted session is not live work):
 *   - `pendingUnassigned` — distribution_state='pending' AND runner_id IS NULL
 *     (the create-time-stranded set: no worker has picked it up → queue depth).
 *   - `pendingAssigned`   — distribution_state='pending' AND runner_id IS NOT NULL
 *     (a launch frame was sent; the runner is spawning/connecting → in-flight).
 *   - `assigned`          — distribution_state='assigned'
 *     (the runner connected and is running → in-flight).
 *
 * FAILED (terminal) and cloud / never-distributed (null distribution_state)
 * sessions match none of these filters, so they are excluded here and never
 * reach {@link computeWorkStats}. The partition decision (which bucket maps to
 * depth vs in_flight) lives in the pure function, not in SQL.
 */
async function countEnvironmentWork(
  db: DbClient,
  workspaceId: string,
  environmentId: string,
): Promise<WorkStatsCounts> {
  const scope = and(
    isNull(sessions.deletedAt),
    eq(sessions.workspaceId, workspaceId),
    eq(sessions.environmentId, environmentId),
    isNull(sessions.archivedAt),
  );
  const [counts] = await db
    .select({
      pendingUnassigned:
        sql<number>`count(*) filter (where ${sessions.distributionState} = ${DISTRIBUTION_PENDING} and ${sessions.runnerId} is null)`.mapWith(
          Number,
        ),
      pendingAssigned:
        sql<number>`count(*) filter (where ${sessions.distributionState} = ${DISTRIBUTION_PENDING} and ${sessions.runnerId} is not null)`.mapWith(
          Number,
        ),
      assigned:
        sql<number>`count(*) filter (where ${sessions.distributionState} = ${DISTRIBUTION_ASSIGNED})`.mapWith(
          Number,
        ),
    })
    .from(sessions)
    .where(scope);
  return counts ?? { pendingUnassigned: 0, pendingAssigned: 0, assigned: 0 };
}

/**
 * Load an environment row in any archive state, workspace-scoped. Callers that
 * need a stored column the wire projection does not carry (`target`, say — it
 * is `orca-beta`-only on the wire but the archive/delete teardown must read it
 * regardless of the caller's headers) read the row rather than the projection.
 */
async function selectEnvironmentRow(db: DbClient, workspaceId: string, id: string) {
  const rows = await db
    .select()
    .from(environments)
    .where(
      and(
        isNull(environments.deletedAt),
        eq(environments.id, id),
        eq(environments.workspaceId, workspaceId),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function loadEnvironment(db: DbClient, workspaceId: string, id: string, orcaBeta = false) {
  const row = await selectEnvironmentRow(db, workspaceId, id);
  return row ? toApi(row, orcaBeta) : null;
}

/**
 * Load a live (non-archived) environment row for a mutation. Archive is soft
 * teardown — its env key is revoked on archive — so rotate-key / revoke-key /
 * update must treat an archived environment as gone (404) rather than re-arming
 * or mutating a credential on a torn-down environment.
 */
async function loadEnvironmentRow(db: DbClient, workspaceId: string, id: string) {
  const rows = await db
    .select()
    .from(environments)
    .where(
      and(
        isNull(environments.deletedAt),
        eq(environments.id, id),
        eq(environments.workspaceId, workspaceId),
        isNull(environments.archivedAt),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}
