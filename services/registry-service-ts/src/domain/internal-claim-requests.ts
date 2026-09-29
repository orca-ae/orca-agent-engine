// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { z } from 'zod';
import { internalContract } from '../contracts/internal.contract.js';

/**
 * Runtime body validation for the durable environment-claim routes, driven by
 * the *contract's own* zod schemas (`internalContract.<route>.body`).
 *
 * The five claim routes (PUT claim, POST heartbeat, POST release, GET claim,
 * POST reap) are hand-mounted on Fastify rather than served by ts-rest, matching
 * the established pattern for every other `/internal/*` route. Without this
 * module each handler would re-implement field validation by hand, so the
 * contract's zod would only ever be exercised by the unit-test fixture — leaving
 * the wire validator and the published contract free to drift apart.
 *
 * These helpers close that split: each one parses against the contract schema it
 * documents, so the contract is the single source of truth for *both* the
 * declared shape and the runtime check. They are pure (no DB, no Fastify), so the
 * unit-test gate covers the exact validation the handlers run.
 *
 * On failure each returns a stable, field-naming message (`"<field> is required"`
 * for the non-empty string fields; `"unexpected body"` for the strict-empty reap
 * body) so the HTTP 400 payloads are byte-identical to the prior hand-rolled
 * checks — the integration suite pins these messages (e.g. it matches
 * `/worker_conn_id/` on a missing-field 400).
 */

/** A parsed body, or a 400-worthy error message. */
export type ParseResult<T> = { ok: true; value: T } | { ok: false; error: string };

type ClaimBody = z.infer<typeof internalContract.claimEnvironment.body>;
type ConnScopedBody = z.infer<typeof internalContract.heartbeatEnvironmentClaim.body>;
type ReapBody = z.infer<typeof internalContract.reapEnvironmentClaims.body>;

/**
 * Map a zod failure on a `worker_conn_id` / `owner_pod` body to the same
 * `"<field> is required"` message the routes returned before the contract became
 * the validator.
 *
 * The contract types both fields as non-empty strings (`z.string().min(1)`), so
 * a missing, non-string, or empty value all fail on the same field path. We
 * surface the *first* offending field by name — that is the field the caller
 * must fix, and the message the integration suite matches on.
 */
function requiredFieldError(error: z.ZodError, fields: readonly string[]): string {
  for (const field of fields) {
    if (error.issues.some((issue) => issue.path[0] === field)) {
      return `${field} is required`;
    }
  }
  // No recognized field path tripped — fall back to the first field in priority
  // order so the response always names a concrete field rather than going blank.
  return `${fields[0]} is required`;
}

/**
 * Parse a `PUT /internal/environments/:id/claim` body against
 * {@link internalContract.claimEnvironment}.
 *
 * Requires non-empty `owner_pod` and `worker_conn_id`. `owner_pod` is checked
 * first so a body missing both names `owner_pod` (the claim route's prior
 * behavior).
 */
export function parseClaimBody(body: unknown): ParseResult<ClaimBody> {
  const parsed = internalContract.claimEnvironment.body.safeParse(body);
  if (parsed.success) return { ok: true, value: parsed.data };
  return { ok: false, error: requiredFieldError(parsed.error, ['owner_pod', 'worker_conn_id']) };
}

/**
 * Parse a connection-scoped claim body (`worker_conn_id` only) against the
 * heartbeat / release contract schemas — they share one shape. Requires a
 * non-empty `worker_conn_id`.
 */
export function parseConnScopedBody(body: unknown): ParseResult<ConnScopedBody> {
  const parsed = internalContract.heartbeatEnvironmentClaim.body.safeParse(body);
  if (parsed.success) return { ok: true, value: parsed.data };
  return { ok: false, error: requiredFieldError(parsed.error, ['worker_conn_id']) };
}

/**
 * Parse a `POST /internal/environments/claims/reap` body against
 * {@link internalContract.reapEnvironmentClaims}.
 *
 * The contract body is `z.object({}).strict()`, so any property rejects — the
 * reaper takes no parameters (the TTL is server-configured). A non-empty body is
 * a caller bug surfaced as `"unexpected body"`.
 */
export function parseReapBody(body: unknown): ParseResult<ReapBody> {
  const parsed = internalContract.reapEnvironmentClaims.body.safeParse(body ?? {});
  if (parsed.success) return { ok: true, value: parsed.data };
  return { ok: false, error: 'unexpected body' };
}
