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
import { uniqueWorkspace, createTestApiKey, createTestAgent } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

interface PreparedEnvironment {
  id: string;
  name: string;
  packages: Record<string, string[]>;
  networking: Record<string, unknown>;
  image: string | null;
  target: string | null;
}

interface PreparedExecutionResp {
  workspace_id: string;
  session: { id: string; environment_id: string | null };
  environment: PreparedEnvironment | null;
}

describe('prepared execution environment (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let environmentId: string;
  let sessionId: string;

  beforeAll(async () => {
    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('internal_environments');
    apiKey = await createTestApiKey(db, workspaceId);

    // Create a test environment via the public API so the internal route has data to find.
    const res = await fetch(`${baseURL}/v1/environments`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        name: `env-test-${Date.now()}`,
        config: { packages: { apt: ['curl', 'jq'], npm: ['typescript'] } },
        networking: { egress: 'all' },
      }),
    });
    if (res.status !== 200) {
      throw new Error(`env create failed: ${res.status} ${await res.text()}`);
    }
    const environment = (await res.json()) as { id: string };
    expect(environment).not.toHaveProperty('workspace_id');
    environmentId = environment.id;

    const agentId = await createTestAgent(baseURL, apiKey);
    sessionId = await createSession(agentId, environmentId);
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestDb();
  });

  it('returns the session environment in the scoped, secret-free execution snapshot', async () => {
    const res = await prepareExecution(workspaceId, sessionId);
    expect(res.status).toBe(200);
    const body = (await res.json()) as PreparedExecutionResp;
    expect(body.workspace_id).toBe(workspaceId);
    expect(body.session.environment_id).toBe(environmentId);
    expect(body.environment?.id).toBe(environmentId);
    expect(body.environment?.packages).toEqual({ apt: ['curl', 'jq'], npm: ['typescript'] });
    expect(body.environment?.networking).toEqual({ egress: 'all' });
    // image remains nullable; target uses the public API's cloud default.
    expect(body.environment?.image).toBeNull();
    expect(body.environment?.target).toBe('cloud');
  });

  it('returns image + target when set on the environment', async () => {
    // Create an environment with image + target via the public API.
    const createRes = await fetch(`${baseURL}/v1/environments`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        name: `env-img-${Date.now()}`,
        image: 'ghcr.io/x:1',
        target: 'cloud',
      }),
    });
    if (createRes.status !== 200) {
      throw new Error(`env create failed: ${createRes.status} ${await createRes.text()}`);
    }
    const created = (await createRes.json()) as { id: string };
    expect(created).not.toHaveProperty('workspace_id');
    const agentId = await createTestAgent(baseURL, apiKey);
    const imageSessionId = await createSession(agentId, created.id);
    const res = await prepareExecution(workspaceId, imageSessionId);
    expect(res.status).toBe(200);
    const body = (await res.json()) as PreparedExecutionResp;
    expect(body.environment?.image).toBe('ghcr.io/x:1');
    expect(body.environment?.target).toBe('cloud');
  });

  it('404s when the session is addressed through a different workspace', async () => {
    const res = await prepareExecution(uniqueWorkspace('wrong_environment_scope'), sessionId);
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/not found/);
  });

  it('400s on an invalid session id format', async () => {
    const res = await prepareExecution(workspaceId, 'not-a-session-id');
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/invalid/i);
  });

  async function createSession(agentId: string, requestedEnvironmentId: string): Promise<string> {
    const res = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ agent_id: agentId, environment_id: requestedEnvironmentId }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { id: string };
    expect(body).not.toHaveProperty('workspace_id');
    return body.id;
  }

  function prepareExecution(requestedWorkspaceId: string, requestedSessionId: string) {
    return fetch(
      `${baseURL}/internal/v1/workspaces/${requestedWorkspaceId}/sessions/${requestedSessionId}/executions:prepare`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
      },
    );
  }
});

/**
 * `POST /internal/environments/:id/verify-key` is the mesh-internal env-key
 * auth path — the worker presents the raw `sk-…` key it got at create/rotate
 * and the registry authenticates it against the stored digest + expiry. This is
 * the production caller for `verifyEnvKey`.
 */
describe('/internal/environments/:id/verify-key (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;

  beforeAll(async () => {
    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('internal_verify_key');
    apiKey = await createTestApiKey(db, workspaceId);
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestDb();
  });

  async function createEnv(name: string): Promise<{ id: string; envKey: string }> {
    // `orca-beta`: the raw `env_key` is an Orca extension the create response
    // echoes only on the beta branch — the default response is Anthropic's
    // `BetaEnvironment` projection, which has no key concept at all.
    const res = await fetch(`${baseURL}/v1/environments`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json', 'orca-beta': '1' },
      body: JSON.stringify({ name }),
    });
    if (res.status !== 200) {
      throw new Error(`env create failed: ${res.status} ${await res.text()}`);
    }
    const body = (await res.json()) as { id: string; env_key: string };
    return { id: body.id, envKey: body.env_key };
  }

  async function verify(id: string, envKey: string): Promise<Response> {
    return fetch(`${baseURL}/internal/environments/${id}/verify-key`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ env_key: envKey }),
    });
  }

  it('authenticates the right key and resolves the workspace — no api-key required', async () => {
    const { id, envKey } = await createEnv(`verify-ok-${Date.now()}`);
    // No x-api-key header — /internal/* is auth-bypassed (mesh mTLS in prod).
    const res = await verify(id, envKey);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { valid: boolean; workspace_id?: string };
    expect(body.valid).toBe(true);
    expect(body.workspace_id).toBe(workspaceId);
  });

  it('rejects the wrong key (valid:false, no workspace leaked)', async () => {
    const { id } = await createEnv(`verify-wrong-${Date.now()}`);
    const res = await verify(id, 'sk-not-the-real-key');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { valid: boolean; workspace_id?: string };
    expect(body.valid).toBe(false);
    expect(body.workspace_id).toBeUndefined();
  });

  it('fails closed for an unknown environment id (no existence oracle)', async () => {
    const res = await verify('env_doesnotexist00000', 'sk-anything');
    expect(res.status).toBe(200);
    expect(((await res.json()) as { valid: boolean }).valid).toBe(false);
  });

  it('rejects after the key is revoked', async () => {
    const { id, envKey } = await createEnv(`verify-revoked-${Date.now()}`);
    // Sanity: valid before revoke, so the post-revoke assertion isn't vacuous.
    expect(((await (await verify(id, envKey)).json()) as { valid: boolean }).valid).toBe(true);

    const revoke = await fetch(`${baseURL}/v1/environments/${id}/revoke-key`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(revoke.status).toBe(200);

    expect(((await (await verify(id, envKey)).json()) as { valid: boolean }).valid).toBe(false);
  });

  it('rejects after the environment is archived (teardown revokes the credential)', async () => {
    const { id, envKey } = await createEnv(`verify-archived-${Date.now()}`);
    expect(((await (await verify(id, envKey)).json()) as { valid: boolean }).valid).toBe(true);

    const archive = await fetch(`${baseURL}/v1/environments/${id}/archive`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(archive.status).toBe(200);

    expect(((await (await verify(id, envKey)).json()) as { valid: boolean }).valid).toBe(false);
  });

  it('rotated key authenticates and the old key stops', async () => {
    const { id, envKey: firstKey } = await createEnv(`verify-rotate-${Date.now()}`);
    const rotate = await fetch(`${baseURL}/v1/environments/${id}/rotate-key`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(rotate.status).toBe(200);
    const newKey = ((await rotate.json()) as { env_key: string }).env_key;

    expect(((await (await verify(id, newKey)).json()) as { valid: boolean }).valid).toBe(true);
    // The pre-rotation key no longer authenticates — the new digest revoked it.
    expect(((await (await verify(id, firstKey)).json()) as { valid: boolean }).valid).toBe(false);
  });

  it('400s on a missing env_key body', async () => {
    const { id } = await createEnv(`verify-nobody-${Date.now()}`);
    const res = await fetch(`${baseURL}/internal/environments/${id}/verify-key`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toMatch(/env_key/);
  });

  it('400s on an invalid environment id format', async () => {
    const res = await verify('not-an-env-id', 'sk-anything');
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toMatch(/invalid/i);
  });
});
