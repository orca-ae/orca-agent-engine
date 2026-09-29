// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import * as argon2 from 'argon2';
import { createHash } from 'node:crypto';
import { customAlphabet } from 'nanoid';
import { and, asc, eq, gt, isNull, like, or } from 'drizzle-orm';
import type { FastifyRequest } from 'fastify';
import { legacyApiKeyFallbackTotal } from '../metrics.js';
import { measureAuthStage } from '../observability/api-performance.js';
import type { DbClient } from '../persistence/postgres/client.js';
import { apiKeys, workspaces } from '../persistence/postgres/schema.js';
import type { AuthenticatedPrincipal } from './principal.js';
import { isWorkspaceId } from './workspace-id.js';
import { createApiKeyProofCache } from './api-key-proof-cache.js';

const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789';
const generate = customAlphabet(ALPHABET, 40);
const LEGACY_API_KEY_FINGERPRINT_PREFIX = 'legacy:';
// Direct misses are unauthenticated. Bound both per-request Argon2 work and
// aggregate work admitted by each Registry replica.
export const LEGACY_API_KEY_FALLBACK_MAX_CANDIDATES = 8;
const LEGACY_API_KEY_FALLBACK_WINDOW_MS = 60_000;
const LEGACY_API_KEY_FALLBACK_MAX_ATTEMPTS_PER_WINDOW = 8;
const LEGACY_API_KEY_FALLBACK_MAX_ATTEMPTS_PER_SOURCE = 2;
const LEGACY_API_KEY_FALLBACK_MAX_CONCURRENT = 1;
const LEGACY_API_KEY_FALLBACK_CONCURRENCY_RETRY_AFTER_SECONDS = 1;

export type LegacyApiKeyFallbackAcquireResult =
  | { acquired: true; release: () => void }
  | { acquired: false; retryAfterSeconds: number };

export interface LegacyApiKeyFallbackLimiter {
  acquire(source: string): LegacyApiKeyFallbackAcquireResult;
}

interface LegacyApiKeyFallbackLimiterOptions {
  now?: () => number;
  windowMs?: number;
  maxAttemptsPerWindow?: number;
  maxAttemptsPerSource?: number;
  maxConcurrent?: number;
}

export function createLegacyApiKeyFallbackLimiter(
  options: LegacyApiKeyFallbackLimiterOptions = {},
): LegacyApiKeyFallbackLimiter {
  const now = options.now ?? Date.now;
  const windowMs = options.windowMs ?? LEGACY_API_KEY_FALLBACK_WINDOW_MS;
  const maxAttemptsPerWindow =
    options.maxAttemptsPerWindow ?? LEGACY_API_KEY_FALLBACK_MAX_ATTEMPTS_PER_WINDOW;
  const maxAttemptsPerSource =
    options.maxAttemptsPerSource ?? LEGACY_API_KEY_FALLBACK_MAX_ATTEMPTS_PER_SOURCE;
  const maxConcurrent = options.maxConcurrent ?? LEGACY_API_KEY_FALLBACK_MAX_CONCURRENT;

  let windowStartedAt = now();
  let attempts = 0;
  let inFlight = 0;
  const attemptsBySource = new Map<string, number>();

  return {
    acquire(source: string): LegacyApiKeyFallbackAcquireResult {
      const current = now();
      if (current < windowStartedAt || current - windowStartedAt >= windowMs) {
        windowStartedAt = current;
        attempts = 0;
        attemptsBySource.clear();
      }
      const sourceAttempts = attemptsBySource.get(source) ?? 0;
      const windowLimitExceeded =
        attempts >= maxAttemptsPerWindow || sourceAttempts >= maxAttemptsPerSource;
      if (inFlight >= maxConcurrent || windowLimitExceeded) {
        const retryAfterSeconds = windowLimitExceeded
          ? Math.max(1, Math.ceil((windowStartedAt + windowMs - current) / 1_000))
          : LEGACY_API_KEY_FALLBACK_CONCURRENCY_RETRY_AFTER_SECONDS;
        return { acquired: false, retryAfterSeconds };
      }

      attempts += 1;
      attemptsBySource.set(source, sourceAttempts + 1);
      inFlight += 1;
      let released = false;
      return {
        acquired: true,
        release: () => {
          if (released) return;
          released = true;
          inFlight -= 1;
        },
      };
    },
  };
}

export class LegacyApiKeyFallbackRateLimitError extends Error {
  constructor(readonly retryAfterSeconds: number) {
    super('legacy api-key verification is temporarily rate limited');
    this.name = 'LegacyApiKeyFallbackRateLimitError';
  }
}

interface ApiKeyAuthOptions {
  proofCacheEnabled?: boolean | undefined;
  legacyFallbackLimiter?: LegacyApiKeyFallbackLimiter;
  legacyFallbackMaxCandidates?: number;
}

export function generateApiKey(): string {
  return `orca_${generate()}`;
}

export async function hashApiKey(plaintext: string): Promise<string> {
  return argon2.hash(plaintext, { type: argon2.argon2id });
}

/**
 * Deterministic, non-secret lookup key. Generated API keys have enough
 * entropy for a domain-separated SHA-256 fingerprint to select one row;
 * Argon2 remains the credential verifier.
 */
export function fingerprintApiKey(plaintext: string): string {
  return createHash('sha256').update('orca-api-key\0').update(plaintext).digest('hex');
}

export function isLegacyApiKeyFingerprint(fingerprint: string): boolean {
  return fingerprint.startsWith(LEGACY_API_KEY_FINGERPRINT_PREFIX);
}

export function partialApiKeyHint(plaintext: string): string {
  return `...${plaintext.slice(-4)}`;
}

export async function verifyApiKey(plaintext: string, hashed: string): Promise<boolean> {
  try {
    return await argon2.verify(hashed, plaintext);
  } catch {
    return false;
  }
}

/**
 * The outcome of api-key authentication, as three distinct cases rather than
 * `principal | null`.
 *
 * The distinction that matters is `absent` versus `rejected`. Collapsing both
 * into `null` is what made authentication fail open: `buildAuth` could not tell
 * "no api key was presented, try OIDC" from "an api key was presented and it
 * did not authenticate", so it ran OIDC either way — and a caller who supplied
 * a bad `x-api-key` alongside a valid Bearer authenticated as *the Bearer's*
 * workspace, one they never named.
 */
export type ApiKeyAuthResult =
  | { outcome: 'authenticated'; principal: AuthenticatedPrincipal }
  /** No `x-api-key` was presented. OIDC is the legitimate next step. */
  | { outcome: 'absent' }
  /** An `x-api-key` was presented and did not authenticate. Nothing else may. */
  | { outcome: 'rejected' };

export function buildApiKeyAuth(db: DbClient, options: ApiKeyAuthOptions = {}) {
  const verifyProof = createApiKeyProofCache({ enabled: options.proofCacheEnabled ?? true });
  const legacyFallbackLimiter =
    options.legacyFallbackLimiter ?? createLegacyApiKeyFallbackLimiter();
  const legacyFallbackMaxCandidates =
    options.legacyFallbackMaxCandidates ?? LEGACY_API_KEY_FALLBACK_MAX_CANDIDATES;

  return async function apiKeyAuth(req: FastifyRequest): Promise<ApiKeyAuthResult> {
    const header = req.headers['x-api-key'];
    // A present `x-api-key` is authoritative whatever it contains: sending the
    // header is the assertion "authenticate me as this key", so its verdict
    // decides the request and no other credential is consulted. Only a wholly
    // absent header is `absent`.
    //
    // The non-string case is `rejected`, not `absent`. Node joins duplicate
    // `x-api-key` lines into one comma-separated string, so a real listener
    // never lands here — but deciding on presence rather than on shape means a
    // header we cannot interpret denies instead of delegating to OIDC.
    if (header === undefined) return { outcome: 'absent' };
    if (
      typeof header !== 'string' ||
      !header.startsWith('orca_') ||
      header.startsWith('orca_admin_') ||
      header.startsWith('orca_platform_')
    ) {
      // Wrong shape, or a credential from another family (`orca_admin_`,
      // `orca_platform_`): rejected without a database round-trip.
      return { outcome: 'rejected' };
    }

    const keyFingerprint = fingerprintApiKey(header);
    const rows = await measureAuthStage('api_key_lookup', () =>
      db
        .select({ key: apiKeys })
        .from(apiKeys)
        .innerJoin(workspaces, eq(apiKeys.workspaceId, workspaces.id))
        .where(
          and(
            eq(apiKeys.keyFingerprint, keyFingerprint),
            eq(apiKeys.status, 'active'),
            isNull(apiKeys.revokedAt),
            or(isNull(apiKeys.expiresAt), gt(apiKeys.expiresAt, new Date())),
            eq(workspaces.status, 'active'),
          ),
        )
        .limit(1),
    );
    let row = rows[0]?.key;
    let verified = row
      ? await measureAuthStage('api_key_verify', () =>
          verifyProof(keyFingerprint, row!.hashedKey, () => verifyApiKey(header, row!.hashedKey)),
        )
      : false;
    let upgradeLegacyFingerprint = false;

    if (!row) {
      const lease = legacyFallbackLimiter.acquire(req.ip || 'unknown');
      if (!lease.acquired) {
        legacyApiKeyFallbackTotal.inc({ result: 'rate_limited' });
        throw new LegacyApiKeyFallbackRateLimitError(lease.retryAfterSeconds);
      }
      try {
        const legacyRows = await db
          .select({ key: apiKeys })
          .from(apiKeys)
          .innerJoin(workspaces, eq(apiKeys.workspaceId, workspaces.id))
          .where(
            and(
              eq(apiKeys.status, 'active'),
              isNull(apiKeys.revokedAt),
              or(isNull(apiKeys.expiresAt), gt(apiKeys.expiresAt, new Date())),
              like(apiKeys.keyFingerprint, `${LEGACY_API_KEY_FINGERPRINT_PREFIX}%`),
              eq(workspaces.status, 'active'),
            ),
          )
          .orderBy(asc(apiKeys.id))
          .limit(legacyFallbackMaxCandidates + 1);
        if (legacyRows.length > legacyFallbackMaxCandidates) {
          legacyApiKeyFallbackTotal.inc({ result: 'candidate_cap_exceeded' });
          return { outcome: 'rejected' };
        }
        for (const { key: candidate } of legacyRows) {
          if (await verifyApiKey(header, candidate.hashedKey)) {
            row = candidate;
            verified = true;
            upgradeLegacyFingerprint = true;
            break;
          }
        }
      } finally {
        lease.release();
      }
    }
    if (!row || !isWorkspaceId(row.workspaceId) || !verified) {
      return { outcome: 'rejected' };
    }
    await measureAuthStage('api_key_last_used', () =>
      db
        .update(apiKeys)
        .set({
          ...(upgradeLegacyFingerprint ? { keyFingerprint } : {}),
          lastUsedAt: new Date(),
        })
        .where(eq(apiKeys.id, row.id)),
    );
    if (upgradeLegacyFingerprint) {
      legacyApiKeyFallbackTotal.inc({ result: 'upgraded' });
    }
    return {
      outcome: 'authenticated',
      principal: {
        workspaceId: row.workspaceId,
        principal: row.principal,
        scopes: row.scopes,
        authMethod: 'api-key',
        apiKeyId: row.id,
      },
    };
  };
}
