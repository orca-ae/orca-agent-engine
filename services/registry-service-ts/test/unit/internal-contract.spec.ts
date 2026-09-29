// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { internalContract } from '../../src/contracts/internal.contract.js';

/**
 * Schema-level contract coverage for the mesh-internal worker-auth + durable
 * environment-claim routes. These routes are hand-mounted on the Fastify app
 * (internal.routes.ts) rather than served from the ts-rest contract, so this
 * spec pins their declared method/path/body/response shapes in the contract —
 * catching request/response drift on the worker-tunnel path that the public
 * contract test (generated.spec.ts) does not cover.
 *
 * For the claim routes the contract body schemas are not merely declarative: the
 * route handlers validate incoming bodies against these same schemas at runtime
 * (via the pure parsers in `src/domain/internal-claim-requests.ts`, covered by
 * internal-claim-requests.spec.ts), so the shapes pinned here are exactly what
 * the wire path enforces.
 */

describe('internalContract — worker-auth + environment-claim route shapes', () => {
  it('declares verify-key as POST /internal/environments/:id/verify-key', () => {
    const route = internalContract.verifyEnvKey;
    expect(route.method).toBe('POST');
    expect(route.path).toBe('/internal/environments/:id/verify-key');
    // Body requires a non-empty env_key.
    expect(route.body.safeParse({ env_key: 'sk-abc' }).success).toBe(true);
    expect(route.body.safeParse({ env_key: '' }).success).toBe(false);
    expect(route.body.safeParse({}).success).toBe(false);
    // Success response is the discriminated valid:true|false union (no leak on false).
    const ok = route.responses[200].safeParse({ valid: true, workspace_id: 'ws_1' });
    expect(ok.success).toBe(true);
    const denied = route.responses[200].safeParse({ valid: false });
    expect(denied.success).toBe(true);
    // valid:true without a workspace_id is not a legal success body.
    expect(route.responses[200].safeParse({ valid: true }).success).toBe(false);
  });

  it('declares newest-wins claim as PUT /internal/environments/:id/claim', () => {
    const route = internalContract.claimEnvironment;
    expect(route.method).toBe('PUT');
    expect(route.path).toBe('/internal/environments/:id/claim');
    expect(
      route.body.safeParse({ owner_pod: 'registry-0', worker_conn_id: 'conn-1' }).success,
    ).toBe(true);
    // owner_pod + worker_conn_id are both required, non-empty.
    expect(route.body.safeParse({ owner_pod: 'registry-0' }).success).toBe(false);
    expect(route.body.safeParse({ owner_pod: '', worker_conn_id: 'conn-1' }).success).toBe(false);
    // The 200 mirrors the persisted claim shape with ISO timestamps.
    const claim = {
      environment_id: 'env_abc',
      owner_pod: 'registry-0',
      worker_conn_id: 'conn-1',
      claimed_at: '2026-06-18T00:00:00.000Z',
      last_ping: '2026-06-18T00:00:00.000Z',
    };
    expect(route.responses[200].safeParse(claim).success).toBe(true);
  });

  it('declares connection-scoped heartbeat as POST /internal/environments/:id/claim/heartbeat', () => {
    const route = internalContract.heartbeatEnvironmentClaim;
    expect(route.method).toBe('POST');
    expect(route.path).toBe('/internal/environments/:id/claim/heartbeat');
    expect(route.body.safeParse({ worker_conn_id: 'conn-1' }).success).toBe(true);
    expect(route.body.safeParse({ worker_conn_id: '' }).success).toBe(false);
    expect(route.responses[200].safeParse({ refreshed: true }).success).toBe(true);
    expect(route.responses[200].safeParse({ refreshed: 'yes' }).success).toBe(false);
  });

  it('declares connection-scoped release as POST /internal/environments/:id/claim/release', () => {
    const route = internalContract.releaseEnvironmentClaim;
    expect(route.method).toBe('POST');
    expect(route.path).toBe('/internal/environments/:id/claim/release');
    expect(route.body.safeParse({ worker_conn_id: 'conn-1' }).success).toBe(true);
    expect(route.responses[200].safeParse({ released: false }).success).toBe(true);
  });

  it('declares getEnvironmentClaim as GET /internal/environments/:id/claim with a nullable claim', () => {
    const route = internalContract.getEnvironmentClaim;
    expect(route.method).toBe('GET');
    expect(route.path).toBe('/internal/environments/:id/claim');
    // Unclaimed → { claim: null }.
    expect(route.responses[200].safeParse({ claim: null }).success).toBe(true);
    expect(
      route.responses[200].safeParse({
        claim: {
          environment_id: 'env_abc',
          owner_pod: 'registry-0',
          worker_conn_id: 'conn-1',
          claimed_at: '2026-06-18T00:00:00.000Z',
          last_ping: '2026-06-18T00:00:00.000Z',
        },
      }).success,
    ).toBe(true);
  });

  it('declares the reaper as POST /internal/environments/claims/reap returning a count', () => {
    const route = internalContract.reapEnvironmentClaims;
    expect(route.method).toBe('POST');
    expect(route.path).toBe('/internal/environments/claims/reap');
    // Strict empty body.
    expect(route.body.safeParse({}).success).toBe(true);
    expect(route.body.safeParse({ ttl_ms: 1 }).success).toBe(false);
    expect(route.responses[200].safeParse({ reaped: 0 }).success).toBe(true);
    expect(route.responses[200].safeParse({ reaped: -1 }).success).toBe(false);
  });

  it('keeps the reaper path from colliding with the parameterized claim path', () => {
    // The static reaper path must not be reachable as an `:id` claim — they are
    // distinct literals, which is what lets Fastify match `claims` before `:id`.
    expect(internalContract.reapEnvironmentClaims.path).not.toBe(
      internalContract.getEnvironmentClaim.path,
    );
  });
});
