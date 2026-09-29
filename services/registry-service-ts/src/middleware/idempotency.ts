// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { eq, and } from 'drizzle-orm';
import type { FastifyRequest, FastifyReply } from 'fastify';
import type { DbClient } from '../persistence/postgres/client.js';
import { idempotencyKeys } from '../persistence/postgres/schema.js';

const TTL_MS = 24 * 60 * 60 * 1000;
// The parsed body can be normalized by a route; preserve the original request
// hash across preHandler/onSend without retaining a second copy of its bytes.
const requestHashes = new WeakMap<FastifyRequest, string>();

export interface IdempotencyStore {
  get(
    workspaceId: string,
    scope: string,
    key: string,
  ): Promise<{
    status: number;
    body: string;
    bodyHash: string;
    expiresAt: Date;
  } | null>;
  put(
    workspaceId: string,
    scope: string,
    key: string,
    value: {
      status: number;
      body: string;
      bodyHash: string;
      expiresAt: Date;
    },
  ): Promise<void>;
}

export function computeBodyHash(body: Buffer): string {
  return createHash('sha256').update(body).digest('hex');
}

/**
 * Pure function: given a store and request inputs, returns:
 *   - null  → no cached response, proceed with the handler
 *   - { status, body }  → return this cached response (replay)
 *   - { status: 409, body: { error } }  → key reused with different body
 */
export async function idempotencyHandler(
  store: IdempotencyStore,
  workspaceId: string,
  scope: string,
  key: string,
  requestBody: Buffer,
  bodyHash = computeBodyHash(requestBody),
): Promise<{ status: number; body: string | object } | null> {
  const cached = await store.get(workspaceId, scope, key);
  if (!cached) return null;
  if (cached.expiresAt.getTime() < Date.now()) return null;
  if (cached.bodyHash !== bodyHash) {
    return { status: 409, body: { error: 'idempotency-key reused with different body' } };
  }
  return { status: cached.status, body: cached.body };
}

/** Postgres-backed implementation of IdempotencyStore. */
export function buildPgIdempotencyStore(db: DbClient): IdempotencyStore {
  return {
    async get(workspaceId, scope, key) {
      const rows = await db
        .select()
        .from(idempotencyKeys)
        .where(
          and(
            eq(idempotencyKeys.workspaceId, workspaceId),
            eq(idempotencyKeys.scope, scope),
            eq(idempotencyKeys.key, key),
          ),
        )
        .limit(1);
      const row = rows[0];
      if (!row) return null;
      return {
        status: row.responseStatus,
        body: row.responseBody,
        bodyHash: row.bodyHash,
        expiresAt: row.expiresAt,
      };
    },
    async put(workspaceId, scope, key, value) {
      await db
        .insert(idempotencyKeys)
        .values({
          workspaceId,
          scope,
          key,
          responseStatus: value.status,
          responseBody: value.body,
          bodyHash: value.bodyHash,
          expiresAt: value.expiresAt,
        })
        .onConflictDoUpdate({
          target: [idempotencyKeys.workspaceId, idempotencyKeys.scope, idempotencyKeys.key],
          set: {
            responseStatus: value.status,
            responseBody: value.body,
            bodyHash: value.bodyHash,
            expiresAt: value.expiresAt,
          },
        });
    },
  };
}

/** Fastify preHandler that wraps idempotencyHandler. */
export function buildIdempotencyPreHandler(store: IdempotencyStore) {
  return async function idempotencyPreHandler(
    req: FastifyRequest,
    reply: FastifyReply,
  ): Promise<FastifyReply | void> {
    if (!['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) return;
    const idem = req.headers['idempotency-key'];
    if (typeof idem !== 'string' || idem.length === 0) return;
    const workspaceId = req.auth?.workspaceId;
    if (!workspaceId) return; // unauthenticated requests don't get cached
    const scope = `${req.method} ${req.routeOptions.url ?? req.url}`;
    const body = req.body ? Buffer.from(JSON.stringify(req.body)) : Buffer.alloc(0);
    const bodyHash = computeBodyHash(body);
    requestHashes.set(req, bodyHash);
    const result = await idempotencyHandler(store, workspaceId, scope, idem, body, bodyHash);
    if (result !== null) {
      return reply.code(result.status).send(result.body);
    }
  };
}

/** Fastify onSend hook that captures successful responses. */
export function buildIdempotencyResponseHook(store: IdempotencyStore) {
  return async function captureResponse(
    req: FastifyRequest,
    reply: FastifyReply,
    payload: unknown,
  ): Promise<unknown> {
    if (!['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) return payload;
    const idem = req.headers['idempotency-key'];
    if (typeof idem !== 'string' || idem.length === 0) return payload;
    const workspaceId = req.auth?.workspaceId;
    if (!workspaceId) return payload;
    if (reply.statusCode < 200 || reply.statusCode >= 300) return payload; // only cache successes
    const responseBody = serializeIdempotencyPayload(payload);
    if (responseBody === null) return payload;
    const scope = `${req.method} ${req.routeOptions.url ?? req.url}`;
    const bodyHash =
      requestHashes.get(req) ??
      computeBodyHash(req.body ? Buffer.from(JSON.stringify(req.body)) : Buffer.alloc(0));
    try {
      await store.put(workspaceId, scope, idem, {
        status: reply.statusCode,
        body: responseBody,
        bodyHash,
        expiresAt: new Date(Date.now() + TTL_MS),
      });
    } catch (err) {
      req.log.warn({ err }, 'idempotency response cache write failed');
    }
    return payload;
  };
}

function serializeIdempotencyPayload(payload: unknown): string | null {
  if (typeof payload === 'string') return payload;
  if (Buffer.isBuffer(payload)) return payload.toString('utf8');
  if (payload === undefined) return null;
  return JSON.stringify(payload);
}
