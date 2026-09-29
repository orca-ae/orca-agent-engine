// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildApp } from '../../src/server.js';

/**
 * End-to-end coverage of the durable environment-claim routes — the multi-replica
 * equivalent of an in-memory tunnel/host registry. A worker's tunnel terminates
 * on the registry replica that owns its environment; these routes are how the
 * owning replica is recorded, heart-beaten, taken over (newest-wins), released,
 * and reaped. The store + decision logic are unit/integration-tested directly in
 * `environment-claims.spec.ts`; this asserts the HTTP request path that makes the
 * behavior reachable in production (the gap the verifier flagged: store built but
 * never wired to a route). All `/internal/*` routes are mesh-only (auth-bypassed;
 * Istio mTLS in prod), so none of these calls carry an api key.
 */

interface ClaimBody {
  environment_id: string;
  owner_pod: string;
  worker_conn_id: string;
  claimed_at: string;
  last_ping: string;
}

describe('/internal/environments/:id/claim (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;

  beforeAll(async () => {
    const { db } = await getTestDb();
    app = buildApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      // Short TTL so the reaper route has something to delete inside the test.
      environmentClaimTtlMs: 1_000,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('internal_claims');
    apiKey = await createTestApiKey(db, workspaceId);
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestDb();
  });

  async function createEnv(name: string): Promise<string> {
    const res = await fetch(`${baseURL}/v1/environments`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    if (res.status !== 200) {
      throw new Error(`env create failed: ${res.status} ${await res.text()}`);
    }
    return ((await res.json()) as { id: string }).id;
  }

  function claim(id: string, ownerPod: string, workerConnId: string): Promise<Response> {
    return fetch(`${baseURL}/internal/environments/${id}/claim`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ owner_pod: ownerPod, worker_conn_id: workerConnId }),
    });
  }

  function heartbeat(id: string, workerConnId: string): Promise<Response> {
    return fetch(`${baseURL}/internal/environments/${id}/claim/heartbeat`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ worker_conn_id: workerConnId }),
    });
  }

  function release(id: string, workerConnId: string): Promise<Response> {
    return fetch(`${baseURL}/internal/environments/${id}/claim/release`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ worker_conn_id: workerConnId }),
    });
  }

  function getOwner(id: string): Promise<Response> {
    return fetch(`${baseURL}/internal/environments/${id}/claim`);
  }

  it('claims an environment and reads the owner back — no api-key required', async () => {
    const id = await createEnv(`claim-ok-${Date.now()}`);
    const res = await claim(id, 'registry-0', 'conn-1');
    expect(res.status).toBe(200);
    const body = (await res.json()) as ClaimBody;
    expect(body.environment_id).toBe(id);
    expect(body.owner_pod).toBe('registry-0');
    expect(body.worker_conn_id).toBe('conn-1');
    expect(body.claimed_at).toBe(body.last_ping);

    const owner = (await (await getOwner(id)).json()) as { claim: ClaimBody | null };
    expect(owner.claim?.owner_pod).toBe('registry-0');
    expect(owner.claim?.worker_conn_id).toBe('conn-1');
  });

  it('getOwner returns { claim: null } for an unclaimed environment', async () => {
    const id = await createEnv(`claim-none-${Date.now()}`);
    const owner = (await (await getOwner(id)).json()) as { claim: ClaimBody | null };
    expect(owner.claim).toBeNull();
  });

  it('newest-wins: a second claim takes over (one owner, the newest)', async () => {
    const id = await createEnv(`claim-takeover-${Date.now()}`);
    await claim(id, 'registry-0', 'conn-1');
    const replaced = (await (await claim(id, 'registry-1', 'conn-2')).json()) as ClaimBody;
    expect(replaced.owner_pod).toBe('registry-1');
    expect(replaced.worker_conn_id).toBe('conn-2');

    const owner = (await (await getOwner(id)).json()) as { claim: ClaimBody | null };
    expect(owner.claim?.worker_conn_id).toBe('conn-2');
  });

  it('heartbeat refreshes the owning connection but is a no-op for a taken-over one', async () => {
    const id = await createEnv(`claim-hb-${Date.now()}`);
    await claim(id, 'registry-0', 'conn-1');
    expect(
      ((await (await heartbeat(id, 'conn-1')).json()) as { refreshed: boolean }).refreshed,
    ).toBe(true);

    // Newest-wins takeover, then the old connection's heartbeat must be rejected.
    await claim(id, 'registry-1', 'conn-2');
    expect(
      ((await (await heartbeat(id, 'conn-1')).json()) as { refreshed: boolean }).refreshed,
    ).toBe(false);
    expect(
      ((await (await heartbeat(id, 'conn-2')).json()) as { refreshed: boolean }).refreshed,
    ).toBe(true);
  });

  it('release is connection-scoped (takeover-then-release cannot evict the live owner)', async () => {
    const id = await createEnv(`claim-rel-${Date.now()}`);
    await claim(id, 'registry-0', 'conn-1');
    await claim(id, 'registry-1', 'conn-2'); // worker B takes over

    // Worker A's teardown release is a no-op — it must NOT delete B's live claim.
    expect(((await (await release(id, 'conn-1')).json()) as { released: boolean }).released).toBe(
      false,
    );
    const stillOwned = (await (await getOwner(id)).json()) as { claim: ClaimBody | null };
    expect(stillOwned.claim?.worker_conn_id).toBe('conn-2');

    // B can release its own claim.
    expect(((await (await release(id, 'conn-2')).json()) as { released: boolean }).released).toBe(
      true,
    );
    const gone = (await (await getOwner(id)).json()) as { claim: ClaimBody | null };
    expect(gone.claim).toBeNull();
  });

  it('reap deletes claims whose heartbeat is past the TTL', async () => {
    const id = await createEnv(`claim-reap-${Date.now()}`);
    await claim(id, 'registry-0', 'conn-1');
    // App TTL is 1s; wait past it so the claim is reapable.
    await new Promise((r) => setTimeout(r, 1_200));

    const res = await fetch(`${baseURL}/internal/environments/claims/reap`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(200);
    expect(((await res.json()) as { reaped: number }).reaped).toBeGreaterThanOrEqual(1);

    const gone = (await (await getOwner(id)).json()) as { claim: ClaimBody | null };
    expect(gone.claim).toBeNull();
  });

  it('the static reap path is not shadowed by the parameterized :id/claim path', async () => {
    // POST /internal/environments/claims/reap must hit the reaper, not be parsed
    // as a claim for env id "claims". A claim PUT to "claims" would 400 on the id
    // regex; the reaper POST returns a count.
    const res = await fetch(`${baseURL}/internal/environments/claims/reap`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(200);
    expect((await res.json()) as { reaped: number }).toHaveProperty('reaped');
  });

  it('400s on a missing worker_conn_id', async () => {
    const id = await createEnv(`claim-badbody-${Date.now()}`);
    const res = await fetch(`${baseURL}/internal/environments/${id}/claim`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ owner_pod: 'registry-0' }),
    });
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toMatch(/worker_conn_id/);
  });

  it('400s on an invalid environment id format', async () => {
    const res = await claim('not-an-env-id', 'registry-0', 'conn-1');
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toMatch(/invalid/i);
  });
});
