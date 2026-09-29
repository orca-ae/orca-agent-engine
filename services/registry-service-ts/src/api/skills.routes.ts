// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { eq, and, isNull, desc, ne, sql, type SQL } from 'drizzle-orm';
import {
  SkillBundleValidationError,
  type SkillBundleInputFile,
  type SkillStore,
} from '@orca/skill-store';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  skillBundleDeletionOutbox,
  skills,
  skillVersions,
} from '../persistence/postgres/schema.js';
import { newId } from '../domain/versioning.js';
import {
  acquireSkillBundleLifecycleLock,
  reconcileSkillBundleDeletionOutbox,
} from '../domain/skill-bundle-deletion-outbox.js';
import { buildClaudeErrorResponse } from '../contracts/common.js';
import { parsePositiveIntQueryParam } from './query-params.js';
import {
  buildZip,
  parseSkillUpload,
  SkillUploadError,
  type SkillArchiveFile,
} from './skill-version-archive.js';

interface SkillVersionListQuery {
  limit?: string;
  page?: string;
}

type SkillRow = typeof skills.$inferSelect;
type SkillVersionRow = typeof skillVersions.$inferSelect;

const SKILL_VERSION_PAGE_DEFAULT = 20;
const SKILL_VERSION_PAGE_MAX = 1000;
const SKILL_LIST_PAGE_DEFAULT = 20;
const SKILL_LIST_PAGE_MAX = 100;
const MAX_SKILL_UPLOAD_BYTES = 30 * 1024 * 1024;
const MAX_SKILL_CURSOR_MICROS = 253_402_300_799_999_999n; // 9999-12-31T23:59:59.999999Z

interface SkillListCursor {
  createdAtMicros: string;
  id: string;
  source: 'anthropic' | 'custom' | null;
  workspaceId: string;
}

interface SkillListRow {
  id: string;
  createdAt: Date;
  displayTitle: string | null;
  type: string;
  updatedAt: Date;
  latestVersionIdentifier: string | null;
  cursorCreatedAtMicros: string;
}

type SkillErrorStatus = 400 | 404 | 409 | 413 | 500;

function sendSkillError(
  reply: FastifyReply,
  requestId: string,
  status: SkillErrorStatus,
  message: string,
) {
  const type = {
    400: 'invalid_request_error',
    404: 'not_found_error',
    409: 'conflict_error',
    413: 'request_too_large',
    500: 'api_error',
  } as const;
  return reply.code(status).send(buildClaudeErrorResponse(requestId, type[status], message));
}

function encodeSkillListCursor(
  row: Pick<SkillListRow, 'cursorCreatedAtMicros' | 'id'>,
  source: SkillListCursor['source'],
  workspaceId: string,
): string {
  return Buffer.from(
    JSON.stringify({
      v: 1,
      created_at_micros: row.cursorCreatedAtMicros,
      id: row.id,
      source,
      workspace_id: workspaceId,
    }),
    'utf8',
  ).toString('base64url');
}

function decodeSkillListCursor(raw: string | undefined): SkillListCursor | null {
  if (!raw || raw.length > 1024) return null;
  try {
    const value = JSON.parse(Buffer.from(raw, 'base64url').toString('utf8')) as Record<
      string,
      unknown
    >;
    if (
      value.v !== 1 ||
      typeof value.created_at_micros !== 'string' ||
      !/^[1-9]\d{0,17}$/.test(value.created_at_micros) ||
      typeof value.id !== 'string' ||
      !/^skill_[A-Za-z0-9_-]+$/.test(value.id) ||
      typeof value.workspace_id !== 'string' ||
      (value.source !== null && value.source !== 'anthropic' && value.source !== 'custom')
    ) {
      return null;
    }
    if (BigInt(value.created_at_micros) > MAX_SKILL_CURSOR_MICROS) return null;
    return {
      createdAtMicros: value.created_at_micros,
      id: value.id,
      source: value.source,
      workspaceId: value.workspace_id,
    };
  } catch {
    return null;
  }
}

export function registerSkillsRoutes(
  app: FastifyInstance,
  db: DbClient,
  skillStore: SkillStore,
): void {
  app.post('/v1/skills', async (req, reply) => {
    const auth = req.auth!;
    if (!isMultipart(req)) {
      return sendSkillError(reply, req.id, 400, 'multipart/form-data upload is required');
    }
    const skillId = newId('skill');
    const versionId = newId('skillver');
    const now = new Date();
    const versionIdentifier = generateVersionIdentifier(now, 1);
    let uploaded;
    try {
      uploaded = await parseSkillUpload(req, MAX_SKILL_UPLOAD_BYTES);
    } catch (err) {
      if (err instanceof SkillUploadError) {
        return sendSkillError(reply, req.id, err.statusCode, err.message);
      }
      throw err;
    }
    let bundle: Awaited<ReturnType<SkillStore['put']>>;
    try {
      bundle = await skillStore.put(
        auth.workspaceId,
        versionId,
        toSkillBundleFiles(uploaded.directory, uploaded.files),
      );
    } catch (error) {
      if (error instanceof SkillUploadError || error instanceof SkillBundleValidationError) {
        return sendSkillError(reply, req.id, 400, error.message);
      }
      throw error;
    }

    try {
      await db.transaction(async (tx) => {
        await acquireSkillBundleLifecycleLock(tx, auth.workspaceId, versionId);
        await tx.insert(skills).values({
          id: skillId,
          workspaceId: auth.workspaceId,
          type: 'custom',
          name: uploaded.name,
          slug: uploaded.directory,
          version: 1,
          latestVersionId: null,
          description: uploaded.description,
          displayTitle: uploaded.displayTitle,
          archivedAt: null,
          createdAt: now,
          updatedAt: now,
        });
        await tx.insert(skillVersions).values({
          id: versionId,
          workspaceId: auth.workspaceId,
          skillId,
          version: 1,
          versionIdentifier,
          name: uploaded.name,
          description: uploaded.description,
          directory: uploaded.directory,
          entrypoint: 'SKILL.md',
          packageSha256: bundle.sha256,
          packageSizeBytes: bundle.sizeBytes,
          packageManifest: bundle.files,
          createdAt: now,
        });
        await tx
          .update(skills)
          .set({ latestVersionId: versionId })
          .where(
            and(
              isNull(skills.deletedAt),
              eq(skills.workspaceId, auth.workspaceId),
              eq(skills.id, skillId),
            ),
          );
      });
    } catch (error) {
      await cleanupUncommittedSkillBundle(req, db, skillStore, {
        workspaceId: auth.workspaceId,
        skillVersionId: versionId,
        packageSha256: bundle.sha256,
      });
      if (!isSkillDisplayTitleConflict(error)) throw error;
      return reply
        .code(409)
        .send(
          buildClaudeErrorResponse(
            req.id,
            'conflict_error',
            'skill display_title is already in use',
          ),
        );
    }

    const out = await loadSkill(db, auth.workspaceId, skillId);
    return reply.send(out);
  });

  app.get('/v1/skills', async (req, reply) => {
    const auth = req.auth!;
    const q = req.query as { limit?: string; page?: string; source?: string };
    const source = q.source;
    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: SKILL_LIST_PAGE_DEFAULT,
      max: SKILL_LIST_PAGE_MAX,
    });
    if (!parsedLimit.ok) {
      return sendSkillError(
        reply,
        req.id,
        400,
        parsedLimit.error ?? 'limit must be a positive integer',
      );
    }
    if (source !== undefined && source !== 'anthropic' && source !== 'custom') {
      return sendSkillError(reply, req.id, 400, 'source must be anthropic or custom');
    }
    const cursor = decodeSkillListCursor(q.page);
    if (q.page !== undefined && !cursor) {
      return sendSkillError(reply, req.id, 400, 'invalid page');
    }
    const createdAtMicros = sql`floor(extract(epoch from ${skills.createdAt}) * 1000000)`;
    let cursorPredicate: SQL | undefined;
    if (cursor) {
      if (cursor.workspaceId !== auth.workspaceId || cursor.source !== (source ?? null)) {
        return sendSkillError(reply, req.id, 400, 'page does not match query scope');
      }
      const cursorTimestamp = sql`(
        timestamptz 'epoch'
        + (${cursor.createdAtMicros}::bigint / 1000000) * interval '1 second'
        + (${cursor.createdAtMicros}::bigint % 1000000) * interval '1 microsecond'
      )`;
      cursorPredicate = sql`(${skills.createdAt}, ${skills.id}) < (${cursorTimestamp}, ${cursor.id})`;
    }
    const limit = parsedLimit.value!;
    const filters = and(
      eq(skills.workspaceId, auth.workspaceId),
      isNull(skills.archivedAt),
      source ? eq(skills.type, source) : undefined,
      cursorPredicate,
    );
    const rows = await db
      .select({
        id: skills.id,
        createdAt: skills.createdAt,
        displayTitle: skills.displayTitle,
        type: skills.type,
        updatedAt: skills.updatedAt,
        latestVersionIdentifier: skillVersions.versionIdentifier,
        cursorCreatedAtMicros: sql<string>`${createdAtMicros}::text`,
      })
      .from(skills)
      .leftJoin(
        skillVersions,
        and(
          eq(skillVersions.workspaceId, skills.workspaceId),
          eq(skillVersions.skillId, skills.id),
          eq(skillVersions.id, skills.latestVersionId),
          isNull(skillVersions.archivedAt),
          isNull(skillVersions.deletedAt),
        ),
      )
      .where(and(isNull(skills.deletedAt), filters))
      .orderBy(desc(skills.createdAt), desc(skills.id))
      .limit(limit + 1);
    const pageRows = rows.slice(0, limit);
    const hasMore = rows.length > limit;
    return reply.send({
      data: pageRows.map(toSkillListApi),
      has_more: hasMore,
      next_page:
        hasMore && pageRows.length > 0
          ? encodeSkillListCursor(pageRows[pageRows.length - 1]!, source ?? null, auth.workspaceId)
          : null,
    });
  });

  app.get('/v1/skills/:id/versions', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const q = req.query as SkillVersionListQuery;
    const existing = await loadSkillRow(db, auth.workspaceId, id);
    if (!existing) return sendSkillError(reply, req.id, 404, 'not found');

    const parsedLimit = parsePositiveIntQueryParam(q.limit, 'limit', {
      defaultValue: SKILL_VERSION_PAGE_DEFAULT,
      max: SKILL_VERSION_PAGE_MAX,
    });
    if (!parsedLimit.ok) {
      return sendSkillError(
        reply,
        req.id,
        400,
        parsedLimit.error ?? 'limit must be a positive integer',
      );
    }

    let pageCursor: string | null = null;
    if (q.page !== undefined) {
      if (q.page.length > 32 || !/^[1-9]\d*$/.test(q.page)) {
        return sendSkillError(reply, req.id, 400, 'page must be a positive integer');
      }
      pageCursor = q.page;
    }

    const limit = parsedLimit.value!;
    const baseWhere = and(
      eq(skillVersions.workspaceId, auth.workspaceId),
      eq(skillVersions.skillId, id),
      isNull(skillVersions.archivedAt),
      isNull(skillVersions.deletedAt),
    );
    const where =
      pageCursor === null
        ? baseWhere
        : and(baseWhere, sql`${skillVersions.versionIdentifier}::numeric < ${pageCursor}::numeric`);
    const rows = await db
      .select()
      .from(skillVersions)
      .where(where)
      .orderBy(desc(sql`${skillVersions.versionIdentifier}::numeric`))
      .limit(limit + 1);
    const dataRows = rows.slice(0, limit);
    return reply.send({
      data: dataRows.map(toSkillVersionApi),
      has_more: rows.length > limit,
      next_page: rows.length > limit ? dataRows[dataRows.length - 1]!.versionIdentifier : null,
    });
  });

  app.post('/v1/skills/:id/versions', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const existing = await loadSkillRow(db, auth.workspaceId, id);
    if (!existing || existing.archivedAt !== null) {
      return sendSkillError(reply, req.id, 404, 'not found');
    }
    if (!isMultipart(req)) {
      return sendSkillError(reply, req.id, 400, 'multipart/form-data upload is required');
    }
    let uploaded;
    try {
      uploaded = await parseSkillUpload(req, MAX_SKILL_UPLOAD_BYTES, {
        allowDisplayTitle: false,
      });
    } catch (err) {
      if (err instanceof SkillUploadError) {
        return sendSkillError(reply, req.id, err.statusCode, err.message);
      }
      throw err;
    }

    const versionId = newId('skillver');
    let bundle: Awaited<ReturnType<SkillStore['put']>>;
    try {
      bundle = await skillStore.put(
        auth.workspaceId,
        versionId,
        toSkillBundleFiles(uploaded.directory, uploaded.files),
      );
    } catch (error) {
      if (error instanceof SkillUploadError || error instanceof SkillBundleValidationError) {
        return sendSkillError(reply, req.id, 400, error.message);
      }
      throw error;
    }
    let created: { versionIdentifier: string } | null;
    try {
      created = await db.transaction(async (tx) => {
        await acquireSkillBundleLifecycleLock(tx, auth.workspaceId, versionId);
        const locked = await tx
          .select({ id: skills.id })
          .from(skills)
          .where(
            and(
              isNull(skills.deletedAt),
              eq(skills.id, id),
              eq(skills.workspaceId, auth.workspaceId),
              isNull(skills.archivedAt),
            ),
          )
          .for('update')
          .limit(1);
        if (!locked[0]) return null;

        const now = new Date();
        const { version: newVersion, versionIdentifier } = await nextSkillVersionMetadata(
          tx,
          auth.workspaceId,
          id,
          now,
        );
        await tx.insert(skillVersions).values({
          id: versionId,
          workspaceId: auth.workspaceId,
          skillId: id,
          version: newVersion,
          versionIdentifier,
          name: uploaded.name,
          description: uploaded.description,
          directory: uploaded.directory,
          entrypoint: 'SKILL.md',
          packageSha256: bundle.sha256,
          packageSizeBytes: bundle.sizeBytes,
          packageManifest: bundle.files,
          createdAt: now,
        });
        await tx
          .update(skills)
          .set({
            version: newVersion,
            latestVersionId: versionId,
            name: uploaded.name,
            slug: uploaded.directory,
            description: uploaded.description,
            updatedAt: now,
          })
          .where(
            and(
              isNull(skills.deletedAt),
              eq(skills.id, id),
              eq(skills.workspaceId, auth.workspaceId),
            ),
          );
        return { versionIdentifier };
      });
    } catch (error) {
      await cleanupUncommittedSkillBundle(req, db, skillStore, {
        workspaceId: auth.workspaceId,
        skillVersionId: versionId,
        packageSha256: bundle.sha256,
      });
      throw error;
    }
    if (!created) {
      await cleanupUncommittedSkillBundle(req, db, skillStore, {
        workspaceId: auth.workspaceId,
        skillVersionId: versionId,
        packageSha256: bundle.sha256,
      });
      return sendSkillError(reply, req.id, 404, 'not found');
    }

    const version = await findSkillVersionRow(db, auth.workspaceId, id, created.versionIdentifier);
    return reply.send(toSkillVersionApi(version!));
  });

  app.get('/v1/skills/:id/versions/:version/content', async (req, reply) => {
    const auth = req.auth!;
    const { id, version } = req.params as { id: string; version: string };
    const existing = await loadSkillRow(db, auth.workspaceId, id);
    if (!existing) return sendSkillError(reply, req.id, 404, 'not found');
    const versionRow = await findSkillVersionRow(db, auth.workspaceId, id, version);
    if (!versionRow) return sendSkillError(reply, req.id, 404, 'not found');

    try {
      const bundle = await skillStore.open(
        auth.workspaceId,
        versionRow.id,
        versionRow.packageSha256,
      );
      const files: SkillArchiveFile[] = bundle.files.map((file) => ({
        path: `${versionRow.directory}/${file.path}`,
        content_base64: file.content.toString('base64'),
        mime_type: file.mimeType,
        mode: file.mode,
      }));
      const zip = buildZip(files);
      const filename = `${versionRow.directory}-${versionRow.versionIdentifier}.zip`.replace(
        /[^A-Za-z0-9._-]/g,
        '_',
      );
      reply.header('content-type', 'application/zip');
      reply.header('content-length', String(zip.length));
      reply.header('content-disposition', `attachment; filename="${filename}"`);
      return reply.send(zip);
    } catch (err) {
      req.log.error({ err, skillVersionId: versionRow.id }, 'failed to open skill bundle');
      return sendSkillError(reply, req.id, 500, 'skill content is unavailable');
    }
  });

  app.get('/v1/skills/:id/versions/:version', async (req, reply) => {
    const auth = req.auth!;
    const { id, version } = req.params as { id: string; version: string };
    const existing = await loadSkillRow(db, auth.workspaceId, id);
    if (!existing) return sendSkillError(reply, req.id, 404, 'not found');
    const versionRow = await findSkillVersionRow(db, auth.workspaceId, id, version);
    if (!versionRow) return sendSkillError(reply, req.id, 404, 'not found');
    return reply.send(toSkillVersionApi(versionRow));
  });

  app.delete('/v1/skills/:id/versions/:version', async (req, reply) => {
    const auth = req.auth!;
    const { id, version } = req.params as { id: string; version: string };
    const result = await db.transaction(async (tx) => {
      const locked = await tx
        .select({ id: skills.id })
        .from(skills)
        .where(
          and(
            isNull(skills.deletedAt),
            eq(skills.id, id),
            eq(skills.workspaceId, auth.workspaceId),
          ),
        )
        .for('update')
        .limit(1);
      const skill = locked[0];
      if (!skill) return { kind: 'not_found' as const };
      const targetRows = await tx
        .select()
        .from(skillVersions)
        .where(
          and(
            eq(skillVersions.workspaceId, auth.workspaceId),
            eq(skillVersions.skillId, id),
            eq(skillVersions.versionIdentifier, version),
            isNull(skillVersions.archivedAt),
            isNull(skillVersions.deletedAt),
          ),
        )
        .limit(1);
      const target = targetRows[0];
      if (!target) return { kind: 'not_found' as const };
      const remaining = await tx
        .select()
        .from(skillVersions)
        .where(
          and(
            eq(skillVersions.workspaceId, auth.workspaceId),
            eq(skillVersions.skillId, id),
            ne(skillVersions.id, target.id),
            isNull(skillVersions.archivedAt),
            isNull(skillVersions.deletedAt),
          ),
        )
        .orderBy(desc(skillVersions.version))
        .limit(1);
      const now = new Date();
      const newLatest = remaining[0];
      await tx
        .update(skills)
        .set({
          latestVersionId: newLatest?.id ?? null,
          updatedAt: now,
          ...(newLatest
            ? {
                version: newLatest.version,
                name: newLatest.name,
                slug: newLatest.directory,
                description: newLatest.description,
              }
            : {}),
        })
        .where(
          and(
            isNull(skills.deletedAt),
            eq(skills.id, id),
            eq(skills.workspaceId, auth.workspaceId),
          ),
        );

      await tx
        .update(skillVersions)
        .set({ deletedAt: now })
        .where(
          and(eq(skillVersions.workspaceId, auth.workspaceId), eq(skillVersions.id, target.id)),
        );
      return { kind: 'deleted' as const, target };
    });

    if (result.kind === 'not_found') return sendSkillError(reply, req.id, 404, 'not found');
    return reply.send({
      id: result.target.versionIdentifier,
      type: 'skill_version_deleted',
    });
  });

  app.get('/v1/skills/:id', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const skill = await loadSkill(db, auth.workspaceId, id);
    if (!skill) return sendSkillError(reply, req.id, 404, 'not found');
    return reply.send(skill);
  });

  app.delete('/v1/skills/:id', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };

    const result = await db.transaction(async (tx) => {
      const locked = await tx
        .select({ id: skills.id })
        .from(skills)
        .where(
          and(
            isNull(skills.deletedAt),
            eq(skills.id, id),
            eq(skills.workspaceId, auth.workspaceId),
          ),
        )
        .for('update')
        .limit(1);
      if (!locked[0]) return { kind: 'not_found' as const };

      const activeVersions = await tx
        .select({ id: skillVersions.id })
        .from(skillVersions)
        .where(
          and(
            eq(skillVersions.workspaceId, auth.workspaceId),
            eq(skillVersions.skillId, id),
            isNull(skillVersions.archivedAt),
            isNull(skillVersions.deletedAt),
          ),
        )
        .limit(1);
      if (activeVersions.length > 0) return { kind: 'active_versions' as const };

      await tx
        .update(skills)
        .set({ deletedAt: new Date() })
        .where(
          and(
            isNull(skills.deletedAt),
            eq(skills.id, id),
            eq(skills.workspaceId, auth.workspaceId),
          ),
        );
      return {
        kind: 'deleted' as const,
      };
    });
    if (result.kind === 'not_found') return sendSkillError(reply, req.id, 404, 'not found');
    if (result.kind === 'active_versions') {
      return reply
        .code(400)
        .send(
          buildClaudeErrorResponse(
            req.id,
            'invalid_request_error',
            'all skill versions must be deleted before deleting the skill',
          ),
        );
    }
    return reply.send({ id, type: 'skill_deleted' });
  });
}

async function loadSkill(db: DbClient, workspaceId: string, skillId: string) {
  const skillRow = await loadSkillRow(db, workspaceId, skillId);
  if (!skillRow) return null;
  const versionRow = await loadLatestVersionRow(db, skillRow);
  return {
    id: skillRow.id,
    created_at: skillRow.createdAt.toISOString(),
    display_title: skillRow.displayTitle,
    latest_version: versionRow?.versionIdentifier ?? null,
    source: skillRow.type,
    type: 'skill',
    updated_at: skillRow.updatedAt.toISOString(),
  };
}

function toSkillListApi(row: SkillListRow) {
  return {
    id: row.id,
    created_at: row.createdAt.toISOString(),
    display_title: row.displayTitle,
    latest_version: row.latestVersionIdentifier,
    source: row.type,
    type: 'skill',
    updated_at: row.updatedAt.toISOString(),
  };
}

async function loadSkillRow(db: DbClient, workspaceId: string, skillId: string) {
  const rows = await db
    .select()
    .from(skills)
    .where(
      and(isNull(skills.deletedAt), eq(skills.id, skillId), eq(skills.workspaceId, workspaceId)),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function loadLatestVersionRow(
  db: DbClient,
  skillRow: SkillRow,
): Promise<SkillVersionRow | null> {
  if (!skillRow.latestVersionId) return null;
  const rows = await db
    .select()
    .from(skillVersions)
    .where(
      and(
        eq(skillVersions.workspaceId, skillRow.workspaceId),
        eq(skillVersions.skillId, skillRow.id),
        eq(skillVersions.id, skillRow.latestVersionId),
        isNull(skillVersions.archivedAt),
        isNull(skillVersions.deletedAt),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function findSkillVersionRow(
  db: DbClient,
  workspaceId: string,
  skillId: string,
  version: string,
): Promise<SkillVersionRow | null> {
  const rows = await db
    .select()
    .from(skillVersions)
    .where(
      and(
        eq(skillVersions.workspaceId, workspaceId),
        eq(skillVersions.skillId, skillId),
        eq(skillVersions.versionIdentifier, version),
        isNull(skillVersions.archivedAt),
        isNull(skillVersions.deletedAt),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function nextSkillVersionMetadata(
  db: Pick<DbClient, 'select'>,
  workspaceId: string,
  skillId: string,
  now: Date,
): Promise<{ version: number; versionIdentifier: string }> {
  const rows = await db
    .select({
      version: skillVersions.version,
      versionIdentifier: skillVersions.versionIdentifier,
    })
    .from(skillVersions)
    .where(and(eq(skillVersions.workspaceId, workspaceId), eq(skillVersions.skillId, skillId)))
    .orderBy(desc(skillVersions.version))
    .limit(1);
  const previous = rows[0];
  const version = (previous?.version ?? 0) + 1;
  const wallClockCandidate = BigInt(generateVersionIdentifier(now, Math.min(version, 999)));
  const previousIdentifier = previous === undefined ? 0n : BigInt(previous.versionIdentifier);
  return {
    version,
    versionIdentifier:
      wallClockCandidate > previousIdentifier
        ? wallClockCandidate.toString()
        : (previousIdentifier + 1n).toString(),
  };
}

function toSkillVersionApi(row: SkillVersionRow) {
  return {
    id: row.id,
    created_at: row.createdAt.toISOString(),
    description: row.description,
    directory: row.directory,
    name: row.name,
    skill_id: row.skillId,
    type: 'skill_version',
    version: row.versionIdentifier,
  };
}

function generateVersionIdentifier(now: Date, ordinal: number): string {
  return String(now.getTime() * 1000 + ordinal);
}

function isSkillDisplayTitleConflict(error: unknown): boolean {
  let current: unknown = error;
  for (let depth = 0; depth < 3; depth += 1) {
    if (!current || typeof current !== 'object') return false;
    const record = current as { code?: unknown; constraint?: unknown; cause?: unknown };
    if (record.code === '23505') {
      return record.constraint === 'skills_ws_display_title_idx';
    }
    current = record.cause;
  }
  return false;
}

function isMultipart(req: FastifyRequest): boolean {
  const contentType = req.headers['content-type'];
  if (Array.isArray(contentType)) {
    return contentType.some((value) => value.includes('multipart/form-data'));
  }
  return contentType?.includes('multipart/form-data') ?? false;
}

function toSkillBundleFiles(directory: string, files: SkillArchiveFile[]): SkillBundleInputFile[] {
  const root = `${directory}/`;
  return files.map((file) => {
    if (!file.path.startsWith(root) || file.path.length === root.length) {
      throw new SkillUploadError('all files must be located under the skill root directory');
    }
    return {
      path: file.path.slice(root.length),
      content: Buffer.from(file.content_base64, 'base64'),
      ...(file.mode !== undefined ? { mode: file.mode } : {}),
      ...(file.mime_type ? { mimeType: file.mime_type } : {}),
    };
  });
}

async function reconcileEnqueuedSkillBundles(
  req: FastifyRequest,
  db: DbClient,
  skillStore: SkillStore,
  skillVersionIds: string[],
): Promise<void> {
  try {
    const result = await reconcileSkillBundleDeletionOutbox(db, skillStore, {
      skillVersionIds,
    });
    if (result.failed > 0) {
      req.log.warn(
        { skillVersionIds, failed: result.failed },
        'skill bundle deletion remains pending for retry',
      );
    }
  } catch (error) {
    req.log.warn(
      { err: error, skillVersionIds },
      'failed to reconcile durable skill bundle deletions',
    );
  }
}

async function cleanupUncommittedSkillBundle(
  req: FastifyRequest,
  db: DbClient,
  skillStore: SkillStore,
  target: {
    workspaceId: string;
    skillVersionId: string;
    packageSha256: string;
  },
): Promise<void> {
  try {
    await db
      .insert(skillBundleDeletionOutbox)
      .values({
        workspaceId: target.workspaceId,
        skillVersionId: target.skillVersionId,
        packageSha256: target.packageSha256,
      })
      .onConflictDoNothing();
    await reconcileEnqueuedSkillBundles(req, db, skillStore, [target.skillVersionId]);
  } catch (enqueueError) {
    // A transaction error may mean COMMIT succeeded but its acknowledgement
    // was lost. If Postgres cannot durably decide whether metadata owns this
    // bundle, fail safe and leave the object for operator inventory instead of
    // risking deletion of a live SkillVersion.
    req.log.error(
      {
        err: enqueueError,
        workspaceId: target.workspaceId,
        skillVersionId: target.skillVersionId,
        packageSha256: target.packageSha256,
      },
      'failed to durably schedule cleanup for a possibly uncommitted skill bundle',
    );
  }
}
