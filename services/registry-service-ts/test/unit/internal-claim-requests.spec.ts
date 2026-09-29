// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  parseClaimBody,
  parseConnScopedBody,
  parseReapBody,
} from '../../src/domain/internal-claim-requests.js';
import { internalContract } from '../../src/contracts/internal.contract.js';

/**
 * Unit coverage for the contract-driven body parsers the hand-mounted
 * environment-claim routes run at request time.
 *
 * The point of these tests is the gap they close: the five claim routes are
 * mounted on Fastify by hand (not served by ts-rest), so before this module the
 * contract's zod was exercised *only* by the schema-fixture spec
 * (internal-contract.spec.ts) and never by the wire path. These parsers make the
 * contract the runtime validator, and the assertions below pin BOTH that the
 * parsers accept/reject the right bodies AND that they delegate to the contract
 * schema (so a future contract edit changes the runtime behavior in lock-step).
 *
 * The error strings are asserted verbatim because the HTTP 400 payloads must stay
 * byte-identical to the prior hand-rolled checks — the integration suite matches
 * the field name in the message (e.g. /worker_conn_id/).
 */

describe('parseClaimBody (PUT /internal/environments/:id/claim)', () => {
  it('accepts a well-formed claim body and returns the typed value', () => {
    const result = parseClaimBody({ owner_pod: 'registry-0', worker_conn_id: 'conn-1' });
    expect(result).toEqual({
      ok: true,
      value: { owner_pod: 'registry-0', worker_conn_id: 'conn-1' },
    });
  });

  it('rejects a missing owner_pod, naming owner_pod first', () => {
    // Both fields missing → owner_pod is named first (the route's prior behavior).
    expect(parseClaimBody({})).toEqual({ ok: false, error: 'owner_pod is required' });
    expect(parseClaimBody({ worker_conn_id: 'conn-1' })).toEqual({
      ok: false,
      error: 'owner_pod is required',
    });
  });

  it('rejects an empty owner_pod (min-length, not just presence)', () => {
    expect(parseClaimBody({ owner_pod: '', worker_conn_id: 'conn-1' })).toEqual({
      ok: false,
      error: 'owner_pod is required',
    });
  });

  it('rejects a missing/empty worker_conn_id once owner_pod is present', () => {
    expect(parseClaimBody({ owner_pod: 'registry-0' })).toEqual({
      ok: false,
      error: 'worker_conn_id is required',
    });
    expect(parseClaimBody({ owner_pod: 'registry-0', worker_conn_id: '' })).toEqual({
      ok: false,
      error: 'worker_conn_id is required',
    });
  });

  it('rejects non-object and non-string field values', () => {
    expect(parseClaimBody(null).ok).toBe(false);
    expect(parseClaimBody(undefined).ok).toBe(false);
    expect(parseClaimBody('nope').ok).toBe(false);
    expect(parseClaimBody({ owner_pod: 7, worker_conn_id: 'conn-1' })).toEqual({
      ok: false,
      error: 'owner_pod is required',
    });
  });

  it('is driven by the contract schema (claimEnvironment.body)', () => {
    // The parser must accept exactly what the contract accepts and reject exactly
    // what it rejects — so the contract is the single source of truth, not a
    // parallel hand-rolled check that could drift.
    const good = { owner_pod: 'registry-0', worker_conn_id: 'conn-1' };
    const bad = { owner_pod: '' };
    expect(parseClaimBody(good).ok).toBe(
      internalContract.claimEnvironment.body.safeParse(good).success,
    );
    expect(parseClaimBody(bad).ok).toBe(
      internalContract.claimEnvironment.body.safeParse(bad).success,
    );
  });
});

describe('parseConnScopedBody (heartbeat + release)', () => {
  it('accepts a well-formed worker_conn_id body', () => {
    expect(parseConnScopedBody({ worker_conn_id: 'conn-1' })).toEqual({
      ok: true,
      value: { worker_conn_id: 'conn-1' },
    });
  });

  it('rejects a missing or empty worker_conn_id', () => {
    expect(parseConnScopedBody({})).toEqual({ ok: false, error: 'worker_conn_id is required' });
    expect(parseConnScopedBody({ worker_conn_id: '' })).toEqual({
      ok: false,
      error: 'worker_conn_id is required',
    });
    expect(parseConnScopedBody(null)).toEqual({ ok: false, error: 'worker_conn_id is required' });
  });

  it('matches the heartbeat and release contract schemas (shared shape)', () => {
    // heartbeat and release declare the identical body; the shared parser must
    // agree with both.
    const good = { worker_conn_id: 'conn-1' };
    const bad = {};
    expect(parseConnScopedBody(good).ok).toBe(
      internalContract.heartbeatEnvironmentClaim.body.safeParse(good).success,
    );
    expect(parseConnScopedBody(good).ok).toBe(
      internalContract.releaseEnvironmentClaim.body.safeParse(good).success,
    );
    expect(parseConnScopedBody(bad).ok).toBe(
      internalContract.releaseEnvironmentClaim.body.safeParse(bad).success,
    );
  });
});

describe('parseReapBody (POST /internal/environments/claims/reap)', () => {
  it('accepts an empty body (the reaper takes no parameters)', () => {
    expect(parseReapBody({})).toEqual({ ok: true, value: {} });
  });

  it('treats a missing body as the empty body', () => {
    // Fastify hands `undefined` when no JSON body is sent; the strict-empty
    // contract is satisfied by coalescing to {}.
    expect(parseReapBody(undefined)).toEqual({ ok: true, value: {} });
    expect(parseReapBody(null)).toEqual({ ok: true, value: {} });
  });

  it('rejects any property with "unexpected body" (strict)', () => {
    expect(parseReapBody({ ttl_ms: 1 })).toEqual({ ok: false, error: 'unexpected body' });
    expect(parseReapBody({ anything: true })).toEqual({ ok: false, error: 'unexpected body' });
  });

  it('is driven by the strict reapEnvironmentClaims.body contract', () => {
    expect(parseReapBody({}).ok).toBe(
      internalContract.reapEnvironmentClaims.body.safeParse({}).success,
    );
    expect(parseReapBody({ ttl_ms: 1 }).ok).toBe(
      internalContract.reapEnvironmentClaims.body.safeParse({ ttl_ms: 1 }).success,
    );
  });
});
