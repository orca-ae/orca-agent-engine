// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, isNull, eq } from 'drizzle-orm';
import type { DbClient } from '../persistence/postgres/client.js';
import { environments } from '../persistence/postgres/schema.js';
import {
  armEnvironmentTokenColumns,
  revokeEnvironmentTokenColumns,
  verifyEnvironmentTokenForEnvironment,
} from './environment-token-state.js';
import { ENVIRONMENT_TOKEN_TTL_MS } from './environment-token.js';

/**
 * DB-backed mint/resolve for the per-launch Environment Token, over the
 * `environments` table's `environment_token_digest` /
 * `environment_token_expires_at` columns (the managed-auth sibling of the
 * Env Key's own digest+expiry pair on the same row).
 *
 * All replacement/verify policy lives in the pure functions in
 * `environment-token.ts` / `environment-token-state.ts` (unit-tested without
 * a DB); this class is a thin Drizzle wrapper — one row read or write each —
 * exactly mirroring `EnvironmentClaimStore`'s own split.
 */
export class EnvironmentTokenStore {
  constructor(private readonly db: DbClient) {}

  /**
   * Mint a fresh environment token for `environmentId` and persist its
   * digest + expiry, overwriting any prior credential in place.
   *
   * Called by the environment-launch lifecycle right before starting a
   * worker. A relaunch calling this again atomically revokes the previous
   * generation's token — its digest no longer matches anything stored.
   *
   * @param environmentId Environment the token is scoped to.
   * @param ttlMs Token lifetime in milliseconds; defaults to {@link ENVIRONMENT_TOKEN_TTL_MS}.
   * @param now Instant the token is armed at; defaults to the current time.
   * @returns The raw token — forward it to the launched worker exactly once;
   *   it is never persisted or returned again.
   */
  async mintEnvironmentToken(
    environmentId: string,
    ttlMs: number = ENVIRONMENT_TOKEN_TTL_MS,
    now: Date = new Date(),
  ): Promise<string> {
    const armed = armEnvironmentTokenColumns(ttlMs, now);
    await this.db
      .update(environments)
      .set({
        environmentTokenDigest: armed.environmentTokenDigest,
        environmentTokenExpiresAt: armed.environmentTokenExpiresAt,
      })
      .where(and(isNull(environments.deletedAt), eq(environments.id, environmentId)));
    return armed.raw;
  }

  /**
   * Resolve a presented environment token for `environmentId` to its owning
   * workspace.
   *
   * Loads the row by its primary key, then verifies the raw token against
   * its stored digest + expiry (fails closed on an archived environment).
   * Presenting a token for the wrong `environmentId` fails closed: the named
   * row's digest won't match (id-scoped).
   *
   * @param environmentId The environment the peer claims to be, from the
   *   tunnel path.
   * @param token The raw token presented by the connecting worker.
   * @param now Instant to measure expiry against; defaults to the current time.
   * @returns The resolved owning workspace, or `null` when the environment is
   *   unknown, the token does not match, or the token is expired/archived.
   */
  async resolveEnvironmentToken(
    environmentId: string,
    token: string,
    now: Date = new Date(),
  ): Promise<string | null> {
    const rows = await this.db
      .select()
      .from(environments)
      .where(and(isNull(environments.deletedAt), eq(environments.id, environmentId)))
      .limit(1);
    const row = rows[0];
    if (!row) {
      return null;
    }
    const valid = verifyEnvironmentTokenForEnvironment(
      token,
      {
        environmentTokenDigest: row.environmentTokenDigest,
        environmentTokenExpiresAt: row.environmentTokenExpiresAt,
      },
      row.archivedAt !== null,
      now,
    );
    return valid ? row.workspaceId : null;
  }

  /**
   * Revoke `environmentId`'s per-launch Environment Token, keeping the row.
   *
   * Clears the digest + expiry so a previously issued token stops
   * authenticating. Called by the environment-launch lifecycle's failure
   * cleanup (a launch that fails after the token was armed — e.g. the worker
   * never came online within the wait-online budget) alongside tearing down
   * the launcher-level environment, so a torn-down/never-started sandbox does
   * not leave a live, resolvable credential behind. A no-op write against an
   * unknown `environmentId` (nothing matches the `WHERE`) — the caller does
   * not need to guard on existence first.
   */
  async revokeEnvironmentToken(environmentId: string): Promise<void> {
    const revoked = revokeEnvironmentTokenColumns();
    await this.db
      .update(environments)
      .set({
        environmentTokenDigest: revoked.environmentTokenDigest,
        environmentTokenExpiresAt: revoked.environmentTokenExpiresAt,
      })
      .where(and(isNull(environments.deletedAt), eq(environments.id, environmentId)));
  }
}
