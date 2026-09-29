// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  armEnvironmentToken,
  revokeEnvironmentToken,
  verifyEnvironmentToken,
} from './environment-token.js';

/**
 * Pure column-state transitions for the `environments` per-launch Environment
 * Token credential.
 *
 * `environment-token.ts` holds the credential primitives (arm = mint, verify
 * = verify(digest+expiry)). This module sits one level up: it computes the
 * exact `{ environmentTokenDigest, environmentTokenExpiresAt }` column patch
 * a mint writes, and the environment-scoped (archived-aware) verify the
 * worker-tunnel auth fork needs — mirrors `environment-key-state.ts` exactly.
 */

/** The environment-token columns a write persists. */
export interface EnvironmentTokenColumns {
  environmentTokenDigest: string | null;
  environmentTokenExpiresAt: Date | null;
}

/** An armed token: the columns to persist plus the raw token to forward once. */
export interface ArmedEnvironmentTokenColumns extends EnvironmentTokenColumns {
  environmentTokenDigest: string;
  environmentTokenExpiresAt: Date;
  /** The raw `et-…` token — forwarded to the launched worker once, never persisted. */
  raw: string;
}

/** Minimal stored environment-token shape a resolve needs (the persisted columns). */
export interface StoredEnvironmentToken {
  environmentTokenDigest: string | null;
  environmentTokenExpiresAt: Date | null;
}

/**
 * Arm a fresh environment token for an environment launch (or a relaunch).
 *
 * On a relaunch the returned digest replaces the prior one in place, which
 * atomically revokes the previously issued token (its digest no longer
 * matches anything stored). The caller persists `environmentTokenDigest` +
 * `environmentTokenExpiresAt` and forwards `raw` to the launched worker
 * exactly once.
 *
 * @param ttlMs Token lifetime in milliseconds; defaults to the module's
 *   standard per-launch TTL (see `environment-token.ts`).
 * @param now Instant the token is armed at; defaults to the current time.
 */
export function armEnvironmentTokenColumns(
  ttlMs?: number,
  now: Date = new Date(),
): ArmedEnvironmentTokenColumns {
  const armed = armEnvironmentToken(ttlMs, now);
  return {
    raw: armed.raw,
    environmentTokenDigest: armed.environmentTokenDigest,
    environmentTokenExpiresAt: armed.environmentTokenExpiresAt,
  };
}

/**
 * Clear an environment's per-launch Environment Token credential.
 *
 * Returns the column values an explicit revoke or a launch-failure cleanup
 * persists: the digest no longer matches anything, so the previously issued
 * token stops authenticating. Used by the environment-launch lifecycle when
 * `provision` / `startWorker` / wait-online fails after a token was already
 * minted, so a token injected into a torn-down (or never-onlined) sandbox
 * cannot later authenticate an unrelated worker. Mirrors
 * `environment-key-state.ts`'s `revokeEnvKeyColumns`.
 */
export function revokeEnvironmentTokenColumns(): EnvironmentTokenColumns {
  return revokeEnvironmentToken();
}

/**
 * Authenticate a presented raw environment token against an environment's
 * stored credential, honouring archive.
 *
 * The route-level wrapper over {@link verifyEnvironmentToken}: an archived
 * environment's credential never authenticates regardless of its (stale)
 * digest — archive is soft teardown and revokes the credential. A live row
 * delegates to the digest+expiry check. Fails closed on every non-match, and
 * — because the caller always looks up `stored` by a specific
 * `environmentId` — is inherently id-scoped: a token minted for one
 * environment's digest never matches another environment's stored row.
 *
 * @param raw The raw environment token presented by the connecting worker.
 * @param stored The environment's persisted environment-token columns.
 * @param archived Whether the environment row is archived (soft-deleted).
 * @param now Instant to measure expiry against; defaults to the current time.
 */
export function verifyEnvironmentTokenForEnvironment(
  raw: string,
  stored: StoredEnvironmentToken,
  archived: boolean,
  now: Date = new Date(),
): boolean {
  if (archived) return false;
  return verifyEnvironmentToken(
    raw,
    stored.environmentTokenDigest,
    stored.environmentTokenExpiresAt,
    now,
  );
}
