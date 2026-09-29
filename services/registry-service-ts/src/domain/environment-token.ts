// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { ENV_KEY_TTL_MS } from './environment-key.js';

/**
 * Environment token auth — the credential a registry-launched worker uses to
 * dial (and REDIAL, across reconnects) the worker tunnel WITHOUT an Env Key.
 * A server-managed sandbox has no operator to provision an Env Key ahead of
 * time, so the registry mints a random secret at launch (and again on a
 * relaunch — see `EnvironmentLaunchLifecycle.relaunch`), injects it into the
 * sandbox, and the worker presents the SAME token on every dial — the first,
 * and every reconnect after — for as long as that generation's sandbox lives
 * (`environment-worker` has no refresh mechanism; it just resends what it was
 * given, see `worker.ts`'s `buildConnectHeaders`).
 *
 * Only the SHA-256 digest is ever persisted
 * (`environments.environment_token_digest`), so a database leak does not leak
 * a usable credential; the raw token is minted once per generation (at launch
 * or relaunch) and never stored. Expiry is a long-tail backstop, not the
 * primary revocation path — a torn-down sandbox's token is REVOKED
 * immediately (on launch failure, and on terminate; see
 * {@link ENVIRONMENT_TOKEN_TTL_MS}'s doc) rather than left to expire.
 *
 * The primitives (digest + resolve): SHA-256 digest, constant-time compare
 * via `timingSafeEqual`, expiry-checked, id-scoped resolve. See
 * `docs/managed-agents/services/registry-service.md` ("Worker auth").
 *
 * This module is pure (no DB, no clock side effects beyond an explicit `now`)
 * so the contract — token format, digest determinism, expiry — is
 * unit-testable without infrastructure. Mirrors `environment-key.ts`'s shape;
 * `environment-token-state.ts` sits one level up with the
 * environment-scoped (archived-aware) verify wrapper.
 */

/** Opaque-token prefix; the raw token is `et-<url-safe-random>`. */
export const ENVIRONMENT_TOKEN_PREFIX = 'et-';

/**
 * Bytes of randomness behind the prefix. 32 bytes → 43 url-safe base64 chars,
 * a 256-bit secret that is infeasible to guess.
 */
const ENVIRONMENT_TOKEN_RANDOM_BYTES = 32;

/**
 * Default environment-token lifetime: the Environment's full lifetime, not a
 * short bootstrap window.
 *
 * The token authenticates a registry-launched worker's EVERY dial — the first
 * at launch, and every reconnect after — because `environment-worker` resends
 * the identical token on each reconnect attempt with no refresh mechanism (see
 * `worker.ts`'s `buildConnectHeaders`). A short, "bootstrap ONE launch" TTL
 * (this was previously 15 minutes) therefore does not just bound the initial
 * dial: it permanently breaks every reconnect after the TTL elapses — the
 * worker keeps presenting a now-expired token, `verifyEnvironmentToken` fails
 * closed, and nothing re-arms it (there is no refresh-on-reconnect; a fresh
 * mint only happens on an explicit launch/relaunch). An Environment is a
 * durable, potentially long-lived, multi-session resource, not an ephemeral
 * per-session host, so its credential must survive for as long as the
 * Environment does.
 *
 * Reuses {@link ENV_KEY_TTL_MS} — the Env Key's own long, operator-facing
 * TTL — rather than defining an independent value: both credentials now play
 * the same role (the standing credential for a durable Environment), so
 * there is no reason for them to expire on different schedules, and reusing
 * one constant keeps that invariant from drifting. Expiry stays a backstop
 * only: the token is explicitly REVOKED the moment it should stop working —
 * on launch failure (`EnvironmentLaunchLifecycle`'s cleanup path) and on
 * terminate (`environments.routes.ts`'s archive/delete handlers,
 * best-effort) — so a torn-down Environment's token stops authenticating
 * immediately rather than lingering until this TTL lapses. A caller may still
 * override this per mint via the `ttlMs` parameter on
 * {@link armEnvironmentToken}.
 */
export const ENVIRONMENT_TOKEN_TTL_MS = ENV_KEY_TTL_MS;

/** A freshly armed environment token: the raw token (returned once) plus what to persist. */
export interface ArmedEnvironmentToken {
  /** The raw `et-…` token — return to the caller once, never persist. */
  raw: string;
  /** SHA-256 digest of {@link raw}; persisted as `environments.environment_token_digest`. */
  environmentTokenDigest: string;
  /** When {@link raw} stops authenticating; persisted as `environment_token_expires_at`. */
  environmentTokenExpiresAt: Date;
}

/**
 * Mint a fresh, opaque per-launch environment token.
 *
 * The body is URL-safe base64 of {@link ENVIRONMENT_TOKEN_RANDOM_BYTES}
 * cryptographically random bytes (padding stripped), prefixed with
 * {@link ENVIRONMENT_TOKEN_PREFIX}. Only the holder (the launched sandbox)
 * ever sees this value; persist {@link hashEnvironmentToken} of it.
 */
export function generateEnvironmentToken(): string {
  return `${ENVIRONMENT_TOKEN_PREFIX}${randomBytes(ENVIRONMENT_TOKEN_RANDOM_BYTES).toString('base64url')}`;
}

/**
 * Digest a raw environment token for storage / comparison.
 *
 * SHA-256 hex digest of the raw token's UTF-8 bytes — the value persisted as
 * `environments.environment_token_digest`. The digest is uniform and one-way,
 * so the stored value never reveals the token.
 *
 * @param raw The raw environment token, e.g. the value of {@link generateEnvironmentToken}.
 * @returns Lowercase 64-char hex SHA-256 digest.
 */
export function hashEnvironmentToken(raw: string): string {
  return createHash('sha256').update(raw).digest('hex');
}

/**
 * Verify a presented raw environment token against a stored digest + expiry.
 *
 * Authenticates only when the token's digest matches the stored digest
 * **and** the token is unexpired. Fails closed when either the digest is
 * absent (revoked / never armed) or the expiry is absent. The expiry test is
 * `expiresAt >= now` (a token is still live at exactly its expiry instant;
 * one tick past is dead). The digest comparison is constant-time so a
 * presented token yields no timing oracle.
 *
 * @param raw The raw environment token presented by the connecting worker.
 * @param digest The stored digest, or `null` when none is armed.
 * @param expiresAt The stored expiry, or `null` when none is armed.
 * @param now Instant to measure expiry against; defaults to the current time.
 * @returns `true` when the token matches and is unexpired; `false` otherwise.
 */
export function verifyEnvironmentToken(
  raw: string,
  digest: string | null,
  expiresAt: Date | null,
  now: Date = new Date(),
): boolean {
  if (digest === null || expiresAt === null) return false;
  if (expiresAt.getTime() < now.getTime()) return false;
  return digestsEqual(hashEnvironmentToken(raw), digest);
}

/**
 * Mint and arm a fresh environment token.
 *
 * Generates a new raw token, computes its digest, and stamps an expiry of
 * `now + ttlMs`. Used by the environment-launch lifecycle right before
 * starting a worker (and on a relaunch, where a fresh mint overwriting the
 * prior digest atomically revokes the previous generation's token). The
 * caller persists `environmentTokenDigest` + `environmentTokenExpiresAt` and
 * forwards `raw` to the launched worker exactly once — `raw` is never stored.
 *
 * @param ttlMs Token lifetime in milliseconds; defaults to {@link ENVIRONMENT_TOKEN_TTL_MS}.
 * @param now Instant the token is armed at; defaults to the current time.
 * @returns The raw token plus the digest + expiry to persist.
 */
export function armEnvironmentToken(
  ttlMs: number = ENVIRONMENT_TOKEN_TTL_MS,
  now: Date = new Date(),
): ArmedEnvironmentToken {
  const raw = generateEnvironmentToken();
  return {
    raw,
    environmentTokenDigest: hashEnvironmentToken(raw),
    environmentTokenExpiresAt: new Date(now.getTime() + ttlMs),
  };
}

/** A revoked environment token: digest and expiry both cleared. */
export interface RevokedEnvironmentToken {
  environmentTokenDigest: null;
  environmentTokenExpiresAt: null;
}

/**
 * Clear an environment token's credential.
 *
 * Returns the column values that revoke the token while keeping the
 * environment row — the digest no longer matches anything, so the
 * previously issued token stops authenticating. Used by the launch
 * lifecycle's cleanup path (provision/start/wait-online failure) so a
 * torn-down or never-onlined sandbox's token cannot authenticate a later,
 * unrelated worker. Mirrors {@link revokeEnvKey} in `environment-key.ts`.
 */
export function revokeEnvironmentToken(): RevokedEnvironmentToken {
  return { environmentTokenDigest: null, environmentTokenExpiresAt: null };
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
