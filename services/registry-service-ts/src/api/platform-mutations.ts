// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { and, eq, sql } from 'drizzle-orm';
import type { FastifyRequest } from 'fastify';
import { newId } from '../domain/versioning.js';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import { nameConflictMessage } from '../persistence/postgres/name-conflict.js';
import { platformAuditEvents, platformIdempotencyKeys } from '../persistence/postgres/schema.js';

const IDEMPOTENCY_TTL_MS = 24 * 60 * 60 * 1000;

interface MutationResult {
  status: number;
  body: Record<string, unknown>;
}

export async function runPlatformMutation(
  db: DbClient,
  req: FastifyRequest,
  scope: string,
  idempotencyKey: string | undefined,
  requestBody: Record<string, unknown>,
  mutate: (tx: DbTransaction) => Promise<MutationResult>,
): Promise<{ status: number; serializedBody: string }> {
  const principal = req.platformAuth!.principal;
  const bodyHash = createHash('sha256').update(JSON.stringify(requestBody)).digest('hex');

  return db
    .transaction(async (tx) => {
      if (idempotencyKey !== undefined) {
        const lockIdentity = createHash('sha256')
          .update(principal)
          .update('\0')
          .update(scope)
          .update('\0')
          .update(idempotencyKey)
          .digest('hex');
        await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${lockIdentity}, 0))`);
        const cachedRows = await tx
          .select()
          .from(platformIdempotencyKeys)
          .where(
            and(
              eq(platformIdempotencyKeys.principal, principal),
              eq(platformIdempotencyKeys.scope, scope),
              eq(platformIdempotencyKeys.key, idempotencyKey),
            ),
          )
          .limit(1);
        const cached = cachedRows[0];
        if (cached && cached.expiresAt > new Date()) {
          if (cached.bodyHash !== bodyHash) {
            return {
              status: 409,
              serializedBody: JSON.stringify({
                error: 'idempotency-key reused with different body',
              }),
            };
          }
          return { status: cached.responseStatus, serializedBody: cached.responseBody };
        }
      }

      const result = await mutate(tx);
      const serializedBody = JSON.stringify(result.body);
      if (idempotencyKey !== undefined && result.status >= 200 && result.status < 300) {
        await tx
          .insert(platformIdempotencyKeys)
          .values({
            principal,
            scope,
            key: idempotencyKey,
            responseStatus: result.status,
            responseBody: serializedBody,
            bodyHash,
            expiresAt: new Date(Date.now() + IDEMPOTENCY_TTL_MS),
          })
          .onConflictDoUpdate({
            target: [
              platformIdempotencyKeys.principal,
              platformIdempotencyKeys.scope,
              platformIdempotencyKeys.key,
            ],
            set: {
              responseStatus: result.status,
              responseBody: serializedBody,
              bodyHash,
              expiresAt: new Date(Date.now() + IDEMPOTENCY_TTL_MS),
            },
          });
      }
      return { status: result.status, serializedBody };
    })
    .catch((error: unknown) => {
      // A unique-index violation aborts the transaction. Translate it only after
      // rollback, so no audit, settings, or idempotency row survives a conflict.
      const message = nameConflictMessage(error);
      if (message) return { status: 409, serializedBody: JSON.stringify({ error: message }) };
      throw error;
    });
}

export async function writePlatformAudit(
  db: Pick<DbClient, 'insert'>,
  req: FastifyRequest,
  action: string,
  targetType: string,
  targetId: string,
  organizationId: string | null,
  workspaceId: string | null,
  metadata: Record<string, unknown>,
): Promise<void> {
  const principal = req.platformAuth!;
  await db.insert(platformAuditEvents).values({
    id: newId('audit'),
    organizationId,
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
