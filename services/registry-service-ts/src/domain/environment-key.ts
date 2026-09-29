// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';

/**
 * Env key auth — the secret an environment hands a worker so the worker can
 * later open its host session back to the registry. Only the SHA-256 digest is
 * ever persisted (`environments.env_key_digest`), so a database leak does not
 * leak a usable credential; the raw key is returned exactly once at
 * create/rotate time and never stored. Expiry is enforced alongside the digest
 * match, so a key leaked from a torn-down worker stops authenticating on its
 * own.
 *
 * This module is pure (no DB, no clock side effects beyond an explicit
 * `now`) so the contract — key format, digest determinism, expiry, revoke —
 * is unit-testable without infrastructure.
 */

/** Opaque-key prefix; the raw key is `sk-<url-safe-random>`. */
export const ENV_KEY_PREFIX = 'sk-';

/**
 * Bytes of randomness behind the prefix. 32 bytes → 43 url-safe base64 chars,
 * a 256-bit secret that is infeasible to guess.
 */
const ENV_KEY_RANDOM_BYTES = 32;

/**
 * Env key lifetime. A policy bound (not a platform cap): long enough for a
 * worker to keep re-opening its tunnel across reconnects, short enough that a
 * leaked key from a torn-down worker expires on its own. Rotation re-arms a
 * fresh key + expiry.
 */
export const ENV_KEY_TTL_MS = 7 * 24 * 60 * 60 * 1000;

/** A revoked env key: digest and expiry both cleared. */
export interface RevokedEnvKey {
  envKeyDigest: null;
  envKeyExpiresAt: null;
}

/** A freshly armed env key: the raw key (returned once) plus what to persist. */
export interface ArmedEnvKey {
  /** The raw `sk-…` key — return to the caller once, never persist. */
  raw: string;
  /** SHA-256 digest of {@link raw}; persisted as `environments.env_key_digest`. */
  envKeyDigest: string;
  /** When {@link raw} stops authenticating; persisted as `env_key_expires_at`. */
  envKeyExpiresAt: Date;
}

/**
 * Mint a fresh, opaque env key.
 *
 * The body is URL-safe base64 of {@link ENV_KEY_RANDOM_BYTES} cryptographically
 * random bytes (padding stripped), prefixed with {@link ENV_KEY_PREFIX}. Only
 * the holder ever sees this value; persist {@link hashEnvKey} of it.
 */
export function generateEnvKey(): string {
  return `${ENV_KEY_PREFIX}${randomBytes(ENV_KEY_RANDOM_BYTES).toString('base64url')}`;
}

/**
 * Digest a raw env key for storage / comparison.
 *
 * SHA-256 hex digest of the raw key's UTF-8 bytes — the value persisted as
 * `environments.env_key_digest`. The digest is uniform and one-way, so the
 * stored value never reveals the key.
 *
 * @param raw The raw env key, e.g. the value of {@link generateEnvKey}.
 * @returns Lowercase 64-char hex SHA-256 digest.
 */
export function hashEnvKey(raw: string): string {
  return createHash('sha256').update(raw).digest('hex');
}

/**
 * Verify a presented raw env key against a stored digest + expiry.
 *
 * Authenticates only when the key's digest matches the stored digest **and**
 * the key is unexpired. Fails closed when either the digest is absent (revoked
 * / never armed) or the expiry is absent. The expiry test is `expiresAt >= now`
 * (a key is still live at exactly its expiry instant; one tick past is dead).
 * The digest comparison is constant-time so a presented key yields no timing
 * oracle.
 *
 * @param raw The raw env key presented by the worker.
 * @param digest The stored digest, or `null` when none is armed.
 * @param expiresAt The stored expiry, or `null` when none is armed.
 * @param now Instant to measure expiry against; defaults to the current time.
 * @returns `true` when the key matches and is unexpired; `false` otherwise.
 */
export function verifyEnvKey(
  raw: string,
  digest: string | null,
  expiresAt: Date | null,
  now: Date = new Date(),
): boolean {
  if (digest === null || expiresAt === null) return false;
  if (expiresAt.getTime() < now.getTime()) return false;
  return digestsEqual(hashEnvKey(raw), digest);
}

/**
 * Mint and arm a fresh env key.
 *
 * Generates a new raw key, computes its digest, and stamps an expiry of
 * `now + ENV_KEY_TTL_MS`. Used on environment create (first arm) and on rotate
 * (the new digest replaces the prior one, atomically revoking the old key).
 * The caller persists `envKeyDigest` + `envKeyExpiresAt` and returns `raw`
 * exactly once — `raw` is never stored.
 *
 * @param now Instant the key is armed at; defaults to the current time.
 * @returns The raw key plus the digest + expiry to persist.
 */
export function armEnvKey(now: Date = new Date()): ArmedEnvKey {
  const raw = generateEnvKey();
  return {
    raw,
    envKeyDigest: hashEnvKey(raw),
    envKeyExpiresAt: new Date(now.getTime() + ENV_KEY_TTL_MS),
  };
}

/**
 * Clear an env key's credential.
 *
 * Returns the column values that revoke the key while keeping the environment
 * row — the digest no longer matches anything, so the previously issued key
 * stops authenticating. Used on rotation (replaced by a fresh digest) and on
 * explicit revoke.
 */
export function revokeEnvKey(): RevokedEnvKey {
  return { envKeyDigest: null, envKeyExpiresAt: null };
}

/**
 * Constant-time digest comparison.
 *
 * `timingSafeEqual` throws on length-mismatched buffers, so a stored digest
 * that is not the expected length (corruption, a stray value) is rejected
 * rather than crashing the caller.
 */
function digestsEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a, 'utf8');
  const bb = Buffer.from(b, 'utf8');
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}
