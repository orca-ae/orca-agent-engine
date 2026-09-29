// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import * as argon2 from 'argon2';
import { createHash } from 'node:crypto';
import { customAlphabet } from 'nanoid';
import { and, eq, gt, isNull, or } from 'drizzle-orm';
import type { FastifyRequest } from 'fastify';
import type { DbClient } from '../persistence/postgres/client.js';
import { adminApiKeys, organizations } from '../persistence/postgres/schema.js';
import type { AdminPrincipal } from './principal.js';

const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789';
const generate = customAlphabet(ALPHABET, 40);
export const ADMIN_API_KEY_SCOPES = [
  'org:admin',
  'observability:read',
  'observability:write',
  'observability:rotate',
  'workspaces:read',
  'workspaces:write',
  'api_keys:read',
  'api_keys:write',
] as const;
export type AdminApiKeyScope = (typeof ADMIN_API_KEY_SCOPES)[number];

export function generateAdminApiKey(): string {
  return `orca_admin_${generate()}`;
}

export function isAdminApiKey(plaintext: string): boolean {
  return /^orca_admin_[A-Za-z0-9_-]{32,}$/.test(plaintext);
}

export function parseAdminApiKeyScopes(raw: string | undefined): AdminApiKeyScope[] {
  if (raw === undefined) return ['org:admin'];
  const scopes = [
    ...new Set(
      raw
        .split(',')
        .map((scope) => scope.trim())
        .filter(Boolean),
    ),
  ];
  if (scopes.length === 0) {
    throw new Error('ORCA_ADMIN_KEY_SCOPES must contain at least one admin scope');
  }
  const invalid = scopes.filter(
    (scope) => !ADMIN_API_KEY_SCOPES.includes(scope as AdminApiKeyScope),
  );
  if (invalid.length > 0) {
    throw new Error(`ORCA_ADMIN_KEY_SCOPES contains unsupported scopes: ${invalid.join(', ')}`);
  }
  return scopes as AdminApiKeyScope[];
}

export async function hashAdminApiKey(plaintext: string): Promise<string> {
  return argon2.hash(plaintext, { type: argon2.argon2id });
}

export function fingerprintAdminApiKey(plaintext: string): string {
  return createHash('sha256').update('orca-admin-api-key\0').update(plaintext).digest('hex');
}

export function partialAdminApiKeyHint(plaintext: string): string {
  return `...${plaintext.slice(-4)}`;
}

async function verifyAdminApiKey(plaintext: string, hashed: string): Promise<boolean> {
  try {
    return await argon2.verify(hashed, plaintext);
  } catch {
    return false;
  }
}

/**
 * The outcome of admin api-key authentication, as three cases rather than
 * `principal | null` — the same shape `ApiKeyAuthResult` uses on the public
 * listener, and for the same reason.
 *
 * Collapsing "no key presented" and "a key was presented and did not
 * authenticate" into `null` made `buildAdminAuth` fail open: a caller who sent
 * a bad `x-api-key` alongside a valid admin OIDC Bearer authenticated as *the
 * Bearer's organization*, which they never named. That is strictly more
 * privilege than the workspace-scoped version of this bug.
 */
export type AdminApiKeyAuthResult =
  | { outcome: 'authenticated'; principal: AdminPrincipal }
  /** No `x-api-key` was presented. Admin OIDC is the legitimate next step. */
  | { outcome: 'absent' }
  /** An `x-api-key` was presented and did not authenticate. Nothing else may. */
  | { outcome: 'rejected' };

export function buildAdminApiKeyAuth(db: DbClient) {
  return async function adminApiKeyAuth(req: FastifyRequest): Promise<AdminApiKeyAuthResult> {
    const header = req.headers['x-api-key'];
    // A present `x-api-key` is authoritative whatever it contains — including a
    // workspace (`orca_…`) key sent to the admin listener, which is a caller
    // error and not an invitation to try a different credential. Only a wholly
    // absent header leaves admin OIDC to try.
    if (header === undefined) return { outcome: 'absent' };
    if (typeof header !== 'string' || !isAdminApiKey(header)) return { outcome: 'rejected' };

    const rows = await db
      .select({ key: adminApiKeys, organizationStatus: organizations.status })
      .from(adminApiKeys)
      .innerJoin(organizations, eq(adminApiKeys.organizationId, organizations.id))
      .where(
        and(
          eq(adminApiKeys.keyFingerprint, fingerprintAdminApiKey(header)),
          eq(adminApiKeys.status, 'active'),
          isNull(adminApiKeys.revokedAt),
          or(isNull(adminApiKeys.expiresAt), gt(adminApiKeys.expiresAt, new Date())),
          eq(organizations.status, 'active'),
        ),
      )
      .limit(1);
    const row = rows[0];
    if (!row || !(await verifyAdminApiKey(header, row.key.hashedKey))) {
      return { outcome: 'rejected' };
    }

    await db
      .update(adminApiKeys)
      .set({ lastUsedAt: new Date() })
      .where(eq(adminApiKeys.id, row.key.id));

    return {
      outcome: 'authenticated',
      principal: {
        organizationId: row.key.organizationId,
        principal: `admin-api-key:${row.key.id}`,
        scopes: row.key.scopes,
        authMethod: 'admin-api-key',
      },
    };
  };
}
