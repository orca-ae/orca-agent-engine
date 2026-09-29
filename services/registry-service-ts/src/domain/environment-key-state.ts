// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { armEnvKey, revokeEnvKey, verifyEnvKey } from './environment-key.js';

/**
 * Pure column-state transitions for the `environments` env-key credential.
 *
 * `environment-key.ts` holds the three credential primitives (arm = create,
 * verify = verify(digest+expiry), revoke = clear). This module sits one level
 * up: it computes the exact `{ envKeyDigest, envKeyExpiresAt }` column patch
 * each *route* writes, so the route-level credential lifecycle — arm on create,
 * rotate, explicit revoke, clear-on-archive — is unit-testable without a DB.
 * The route handlers stay thin: they call one of these, persist the returned
 * patch, and (for arm/rotate) return `raw` exactly once. The persistence layer
 * wraps the same primitives behind row writes (Drizzle `update(...).set(patch)`).
 */

/** The env-key columns a write persists. `null`/`null` is the revoked state. */
export interface EnvKeyColumns {
  envKeyDigest: string | null;
  envKeyExpiresAt: Date | null;
}

/** An armed key: the columns to persist plus the raw key to return once. */
export interface ArmedEnvKeyColumns extends EnvKeyColumns {
  envKeyDigest: string;
  envKeyExpiresAt: Date;
  /** The raw `sk-…` key — returned to the caller once, never persisted. */
  raw: string;
}

/** Minimal stored env-key shape a verify needs (the persisted columns). */
export interface StoredEnvKey {
  envKeyDigest: string | null;
  envKeyExpiresAt: Date | null;
}

/**
 * Arm a fresh env key for an environment create or a key rotation.
 *
 * On create this is the first arm; on rotate the returned digest replaces the
 * prior one in place, which atomically revokes the previously issued key (its
 * digest no longer matches anything stored). The caller persists `envKeyDigest`
 * + `envKeyExpiresAt` and returns `raw` exactly once.
 *
 * @param now Instant the key is armed at; defaults to the current time.
 */
export function armEnvKeyColumns(now: Date = new Date()): ArmedEnvKeyColumns {
  const armed = armEnvKey(now);
  return {
    raw: armed.raw,
    envKeyDigest: armed.envKeyDigest,
    envKeyExpiresAt: armed.envKeyExpiresAt,
  };
}

/**
 * Clear an environment's env-key credential, keeping the row.
 *
 * The digest/expiry are both nulled, so the previously issued key stops
 * authenticating while the environment row itself survives. Used by the
 * explicit revoke route and by archive (teardown must not leave a live key —
 * a hard delete drops the row, which is the credential).
 */
export function revokeEnvKeyColumns(): EnvKeyColumns {
  return revokeEnvKey();
}

/**
 * Authenticate a presented raw env key against an environment's stored
 * credential, honouring archive.
 *
 * The route-level wrapper over {@link verifyEnvKey}: an archived environment's
 * credential never authenticates regardless of its (stale) digest — archive is
 * soft teardown and revokes the credential. A live row delegates to the
 * digest+expiry check. Fails closed on every non-match.
 *
 * @param raw The raw env key presented by the worker.
 * @param stored The environment's persisted env-key columns.
 * @param archived Whether the environment row is archived (soft-deleted).
 * @param now Instant to measure expiry against; defaults to the current time.
 */
export function verifyEnvKeyForEnvironment(
  raw: string,
  stored: StoredEnvKey,
  archived: boolean,
  now: Date = new Date(),
): boolean {
  if (archived) return false;
  return verifyEnvKey(raw, stored.envKeyDigest, stored.envKeyExpiresAt, now);
}
