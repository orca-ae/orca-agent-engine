// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance } from 'fastify';
import { eq, and, isNull, desc, lt, or } from 'drizzle-orm';
import type { z } from 'zod';
import type { DbClient } from '../persistence/postgres/client.js';
import { vaults } from '../persistence/postgres/schema.js';
import { newId } from '../domain/versioning.js';
import type { SecretStore } from '../secrets/secret-provider.js';
import { VaultCreate, VaultUpdate } from '../contracts/vaults.contract.js';
import {
  archiveActiveCredentialsForVault,
  deleteCredentialsForVault,
  purgeCredentialSecrets,
} from './vault-credentials.routes.js';

type VaultCreateBody = z.infer<typeof VaultCreate>;
type VaultUpdateBody = z.infer<typeof VaultUpdate>;

interface VaultListPage {
  limit: number;
  page?: VaultCursor;
  includeArchived: boolean;
}

interface VaultCursor {
  createdAt: Date;
  id: string;
}

interface ValidationResult<T> {
  ok: boolean;
  value?: T;
  error?: string;
}

export function registerVaultsRoutes(
  app: FastifyInstance,
  db: DbClient,
  secretStore?: SecretStore,
): void {
  app.post('/v1/vaults', async (req, reply) => {
    const auth = req.auth!;
    const parsed = validateVaultCreateBody(req.body);
    if (!parsed.ok) return reply.code(400).send({ error: parsed.error });
    const body = parsed.value!;
    const id = newId('vlt');
    const now = new Date();

    await db.insert(vaults).values({
      id,
      workspaceId: auth.workspaceId,
      displayName: body.display_name,
      metadata: body.metadata ?? {},
      archivedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    const out = await loadVault(db, auth.workspaceId, id);
    return reply.code(200).send(out);
  });

  app.get('/v1/vaults', async (req, reply) => {
    const auth = req.auth!;
    const page = parseVaultListQuery(req.query);
    if (!page.ok) return reply.code(400).send({ error: page.error });
    const cursor = page.value!.page;
    const filters = and(
      eq(vaults.workspaceId, auth.workspaceId),
      page.value!.includeArchived ? undefined : isNull(vaults.archivedAt),
      cursor
        ? or(
            lt(vaults.createdAt, cursor.createdAt),
            and(eq(vaults.createdAt, cursor.createdAt), lt(vaults.id, cursor.id)),
          )
        : undefined,
    );
    const rows = await db
      .select()
      .from(vaults)
      .where(and(isNull(vaults.deletedAt), filters))
      .orderBy(desc(vaults.createdAt), desc(vaults.id))
      .limit(page.value!.limit + 1);
    const items = rows.slice(0, page.value!.limit);
    reply.send({
      data: items.map(toApi),
      next_page:
        rows.length > page.value!.limit ? encodeVaultCursor(items[items.length - 1]!) : null,
    });
  });

  app.get('/v1/vaults/:id', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const vault = await loadVault(db, auth.workspaceId, id);
    if (!vault) return reply.code(404).send({ error: 'not found' });
    reply.send(vault);
  });

  app.post('/v1/vaults/:id', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const parsed = validateVaultUpdateBody(req.body);
    if (!parsed.ok) return reply.code(400).send({ error: parsed.error });
    const body = parsed.value!;

    const existing = await loadVaultRow(db, auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'not found' });

    const nextMetadata =
      body.metadata === null
        ? {}
        : body.metadata !== undefined
          ? applyMetadataPatch(existing.metadata, body.metadata)
          : existing.metadata;
    if (!validStoredMetadata(nextMetadata)) {
      return reply.code(400).send({ error: 'metadata must contain at most 16 string pairs' });
    }
    const now = new Date();
    await db
      .update(vaults)
      .set({
        displayName: body.display_name ?? existing.displayName,
        metadata: nextMetadata,
        updatedAt: now,
      })
      .where(
        and(isNull(vaults.deletedAt), eq(vaults.id, id), eq(vaults.workspaceId, auth.workspaceId)),
      );

    const out = await loadVault(db, auth.workspaceId, id);
    return reply.send(out);
  });

  app.post('/v1/vaults/:id/archive', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };
    const existing = await loadVaultRow(db, auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'not found' });
    const now = new Date();
    const credentialRows = await db.transaction(async (tx) => {
      const rows = await archiveActiveCredentialsForVault(tx, auth.workspaceId, id, now);
      await tx
        .update(vaults)
        .set({ archivedAt: now, updatedAt: now })
        .where(
          and(
            isNull(vaults.deletedAt),
            eq(vaults.id, id),
            eq(vaults.workspaceId, auth.workspaceId),
          ),
        );
      return rows;
    });
    await purgeCredentialSecrets(secretStore, credentialRows).catch((err: unknown) => {
      req.log.warn({ err, vaultId: id }, 'failed to purge credential secrets after vault archive');
    });
    const vault = await loadVault(db, auth.workspaceId, id);
    return reply.send(vault);
  });

  // Retain vault and credential metadata; revoke credential material after commit.
  app.delete('/v1/vaults/:id', async (req, reply) => {
    const auth = req.auth!;
    const { id } = req.params as { id: string };

    const existing = await loadVaultRow(db, auth.workspaceId, id);
    if (!existing) return reply.code(404).send({ error: 'not found' });

    const credentialRows = await db.transaction(
      async (tx) => {
        const lockedVault = await loadVaultRowForUpdate(tx, auth.workspaceId, id);
        if (!lockedVault) return null;
        const rows = await deleteCredentialsForVault(tx, auth.workspaceId, id);
        await tx
          .update(vaults)
          .set({ deletedAt: new Date() })
          .where(
            and(
              isNull(vaults.deletedAt),
              eq(vaults.id, id),
              eq(vaults.workspaceId, auth.workspaceId),
            ),
          );
        return rows;
      },
      { isolationLevel: 'serializable' },
    );
    if (!credentialRows) return reply.code(404).send({ error: 'not found' });
    await purgeCredentialSecrets(secretStore, credentialRows).catch((err: unknown) => {
      req.log.warn({ err, vaultId: id }, 'failed to purge credential secrets after vault delete');
    });
    return reply.send({ id, type: 'vault_deleted' });
  });
}

function parseVaultListQuery(input: unknown): ValidationResult<VaultListPage> {
  if (!isObject(input)) return { ok: true, value: { limit: 100, includeArchived: false } };
  const rawLimit = input['limit'];
  let limit = 100;
  if (rawLimit !== undefined) {
    const parsed =
      typeof rawLimit === 'number'
        ? rawLimit
        : typeof rawLimit === 'string'
          ? Number(rawLimit)
          : NaN;
    if (!Number.isInteger(parsed) || parsed < 1) return invalid('limit must be a positive integer');
    limit = Math.min(parsed, 100);
  }

  const rawIncludeArchived = input['include_archived'];
  let includeArchived = false;
  if (rawIncludeArchived !== undefined) {
    if (typeof rawIncludeArchived === 'boolean') includeArchived = rawIncludeArchived;
    else if (rawIncludeArchived === 'true') includeArchived = true;
    else if (rawIncludeArchived === 'false') includeArchived = false;
    else return invalid('include_archived must be a boolean');
  }

  const rawPage = input['page'];
  if (rawPage === undefined || rawPage === '') {
    return { ok: true, value: { limit, includeArchived } };
  }
  if (typeof rawPage !== 'string') return invalid('page must be a string');
  const page = decodeVaultCursor(rawPage);
  if (!page) return invalid('invalid page');
  return { ok: true, value: { limit, includeArchived, page } };
}

function encodeVaultCursor(row: typeof vaults.$inferSelect): string {
  return `${row.createdAt.toISOString()}|${row.id}`;
}

function decodeVaultCursor(page: string): VaultCursor | null {
  const sep = page.lastIndexOf('|');
  if (sep <= 0 || sep === page.length - 1) return null;
  const createdAt = new Date(page.slice(0, sep));
  if (Number.isNaN(createdAt.getTime())) return null;
  return { createdAt, id: page.slice(sep + 1) };
}

function validateVaultCreateBody(input: unknown): ValidationResult<VaultCreateBody> {
  const parsed = VaultCreate.safeParse(input);
  if (!parsed.success) return invalid(firstZodIssue(parsed.error, 'invalid vault create body'));
  return {
    ok: true,
    value: {
      display_name: parsed.data.display_name,
      ...(parsed.data.metadata !== undefined ? { metadata: parsed.data.metadata } : {}),
    },
  };
}

function validateVaultUpdateBody(input: unknown): ValidationResult<VaultUpdateBody> {
  const parsed = VaultUpdate.safeParse(input);
  if (!parsed.success) return invalid(firstZodIssue(parsed.error, 'invalid vault update body'));
  return {
    ok: true,
    value: {
      ...(parsed.data.display_name !== undefined ? { display_name: parsed.data.display_name } : {}),
      ...(parsed.data.metadata !== undefined ? { metadata: parsed.data.metadata } : {}),
    },
  };
}

function applyMetadataPatch(
  existing: unknown,
  patch: Record<string, string | null>,
): Record<string, string> {
  const next = isObject(existing) ? { ...existing } : {};
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) delete next[key];
    else next[key] = value;
  }
  return next as Record<string, string>;
}

function validStoredMetadata(value: unknown): value is Record<string, string> {
  if (!isObject(value)) return false;
  const entries = Object.entries(value);
  return (
    entries.length <= 16 &&
    entries.every(
      ([key, item]) =>
        key.length >= 1 && key.length <= 64 && typeof item === 'string' && item.length <= 512,
    )
  );
}

function isObject(input: unknown): input is Record<string, unknown> {
  return typeof input === 'object' && input !== null && !Array.isArray(input);
}

function firstZodIssue(error: { issues: Array<{ message: string }> }, fallback: string): string {
  return error.issues[0]?.message ?? fallback;
}

function invalid<T>(error: string): ValidationResult<T> {
  return { ok: false, error };
}

function toApi(row: typeof vaults.$inferSelect) {
  return {
    id: row.id,
    type: 'vault' as const,
    display_name: row.displayName,
    metadata: validStoredMetadata(row.metadata) ? row.metadata : {},
    archived_at: row.archivedAt?.toISOString() ?? null,
    created_at: row.createdAt.toISOString(),
    updated_at: row.updatedAt.toISOString(),
  };
}

async function loadVault(db: DbClient, workspaceId: string, id: string) {
  const rows = await db
    .select()
    .from(vaults)
    .where(and(isNull(vaults.deletedAt), eq(vaults.id, id), eq(vaults.workspaceId, workspaceId)))
    .limit(1);
  return rows[0] ? toApi(rows[0]) : null;
}

async function loadVaultRow(db: DbClient, workspaceId: string, id: string) {
  const rows = await db
    .select()
    .from(vaults)
    .where(and(isNull(vaults.deletedAt), eq(vaults.id, id), eq(vaults.workspaceId, workspaceId)))
    .limit(1);
  return rows[0] ?? null;
}

async function loadVaultRowForUpdate(
  db: Pick<DbClient, 'select'>,
  workspaceId: string,
  id: string,
) {
  const rows = await db
    .select()
    .from(vaults)
    .where(and(isNull(vaults.deletedAt), eq(vaults.id, id), eq(vaults.workspaceId, workspaceId)))
    .for('update')
    .limit(1);
  return rows[0] ?? null;
}
