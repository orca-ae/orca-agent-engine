// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import * as argon2 from 'argon2';
import { createHash } from 'node:crypto';
import { customAlphabet } from 'nanoid';
import { and, eq, gt, isNull, or } from 'drizzle-orm';
import type { FastifyRequest } from 'fastify';
import type { DbClient } from '../persistence/postgres/client.js';
import { platformApiKeys } from '../persistence/postgres/schema.js';
import type { PlatformPrincipal } from './principal.js';

const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789';
const generate = customAlphabet(ALPHABET, 40);

export function generatePlatformApiKey(): string {
  return `orca_platform_${generate()}`;
}

export function isPlatformApiKey(plaintext: string): boolean {
  return /^orca_platform_[A-Za-z0-9_-]{32,}$/.test(plaintext);
}

export async function hashPlatformApiKey(plaintext: string): Promise<string> {
  return argon2.hash(plaintext, { type: argon2.argon2id });
}

export function fingerprintPlatformApiKey(plaintext: string): string {
  return createHash('sha256').update('orca-platform-api-key\0').update(plaintext).digest('hex');
}

export function partialPlatformApiKeyHint(plaintext: string): string {
  return `...${plaintext.slice(-4)}`;
}

async function verifyPlatformApiKey(plaintext: string, hashed: string): Promise<boolean> {
  try {
    return await argon2.verify(hashed, plaintext);
  } catch {
    return false;
  }
}

/**
 * The outcome of platform api-key authentication, as three cases rather than
 * `principal | null` — the same shape the public and admin listeners use, and
 * for the same reason: `buildPlatformAuth` must be able to tell "no key was
 * presented, platform OIDC is the legitimate next step" from "a key was
 * presented and it did not authenticate", where nothing else may.
 */
export type PlatformApiKeyAuthResult =
  | { outcome: 'authenticated'; principal: PlatformPrincipal }
  /** No `x-api-key` was presented. Platform OIDC is the legitimate next step. */
  | { outcome: 'absent' }
  /** An `x-api-key` was presented and did not authenticate. Nothing else may. */
  | { outcome: 'rejected' };

export function buildPlatformApiKeyAuth(db: DbClient) {
  return async function platformApiKeyAuth(req: FastifyRequest): Promise<PlatformApiKeyAuthResult> {
    const header = req.headers['x-api-key'];
    // A present `x-api-key` is authoritative whatever it contains — including
    // an organization (`orca_admin_`) or workspace (`orca_`) key sent to the
    // platform listener.
    if (header === undefined) return { outcome: 'absent' };
    if (typeof header !== 'string' || !isPlatformApiKey(header)) return { outcome: 'rejected' };

    const rows = await db
      .select()
      .from(platformApiKeys)
      .where(
        and(
          eq(platformApiKeys.keyFingerprint, fingerprintPlatformApiKey(header)),
          eq(platformApiKeys.status, 'active'),
          isNull(platformApiKeys.revokedAt),
          or(isNull(platformApiKeys.expiresAt), gt(platformApiKeys.expiresAt, new Date())),
        ),
      )
      .limit(1);
    const key = rows[0];
    if (
      !key ||
      !key.scopes.includes('platform:admin') ||
      !(await verifyPlatformApiKey(header, key.hashedKey))
    ) {
      return { outcome: 'rejected' };
    }

    await db
      .update(platformApiKeys)
      .set({ lastUsedAt: new Date() })
      .where(eq(platformApiKeys.id, key.id));

    return {
      outcome: 'authenticated',
      principal: {
        principal: `platform-api-key:${key.id}`,
        scopes: key.scopes,
        authMethod: 'platform-api-key',
      },
    };
  };
}
