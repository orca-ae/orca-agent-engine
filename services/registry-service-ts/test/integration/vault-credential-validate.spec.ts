// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { and, eq } from 'drizzle-orm';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import { LocalSecretStore } from '../../src/secrets/index.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  sessions,
  vaultCredentials,
} from '../../src/persistence/postgres/schema.js';
import { newId } from '../../src/domain/versioning.js';
import { refreshCredentialWithLease } from '../../src/api/vault-credentials.routes.js';

interface HttpResponseCapture {
  status_code: number;
  content_type: string;
  body: string;
  body_truncated: boolean;
}

interface ValidationResponse {
  type: 'vault_credential_validation';
  credential_id: string;
  vault_id: string;
  validated_at: string;
  has_refresh_token: boolean;
  status: 'valid' | 'invalid' | 'unknown';
  mcp_probe: { method: 'initialize'; http_response: HttpResponseCapture | null };
  refresh: {
    status: 'succeeded' | 'connect_error' | 'failed' | 'no_refresh_token';
    http_response: HttpResponseCapture | null;
  };
}

interface ClaudeErrorOut {
  type: 'error';
  error: { type: string; message: string };
  request_id: string | null;
}

type FetchHandler = (url: string, init: RequestInit) => Promise<Response> | Response;

const MCP_URL_BASE = 'https://mcp.validate.example';
const TOKEN_ENDPOINT = 'https://auth.validate.example/oauth/token';

describe('Vault credential mcp_oauth_validate (integration)', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let workspaceId: string;
  let apiKey: string;
  let secretStore: LocalSecretStore;

  let fetchHandler: FetchHandler;
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) =>
    fetchHandler(String(input), init ?? {}),
  );

  beforeAll(async () => {
    ({ db } = await getTestDb());
    secretStore = new LocalSecretStore();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      secretStore,
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    await app.ready();
    workspaceId = uniqueWorkspace('vault_credential_validate');
    apiKey = await createTestApiKey(db, workspaceId);
  });

  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  beforeEach(() => {
    fetchImpl.mockClear();
    fetchHandler = () => {
      throw new Error('no fetch handler configured for this test');
    };
  });

  it('reports valid for a no-refresh credential when the probe succeeds', async () => {
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential('no-refresh');
    let probeInit: RequestInit | undefined;
    fetchHandler = (url, init) => {
      expect(url).toBe(mcpServerUrl);
      probeInit = init;
      return jsonResponse(200, { jsonrpc: '2.0', id: 1, result: {} });
    };

    const res = await validate(vaultId, credentialId);
    expect(res.statusCode).toBe(200);
    const body = res.json<ValidationResponse>();
    expect(body.type).toBe('vault_credential_validation');
    expect(body.credential_id).toBe(credentialId);
    expect(body.vault_id).toBe(vaultId);
    expect(body.status).toBe('valid');
    expect(body.has_refresh_token).toBe(false);
    expect(body.refresh).toEqual({ status: 'no_refresh_token', http_response: null });
    expect(body.mcp_probe.method).toBe('initialize');
    expect(body.mcp_probe.http_response).toMatchObject({
      status_code: 200,
      content_type: 'application/json',
      body_truncated: false,
    });
    expect(Date.parse(body.validated_at)).not.toBeNaN();

    const headers = probeInit!.headers as Record<string, string>;
    expect(headers['authorization']).toBe('Bearer access-token-no-refresh');
    expect(headers['content-type']).toBe('application/json');
    expect(headers['accept']).toBe('application/json, text/event-stream');
    const rpc = JSON.parse(String(probeInit!.body)) as Record<string, unknown>;
    expect(rpc).toMatchObject({ jsonrpc: '2.0', id: 1, method: 'initialize' });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('reports invalid when the probe is rejected with 401 and no refresh token exists', async () => {
    const { vaultId, credentialId } = await createOauthCredential('probe-401');
    fetchHandler = () => jsonResponse(401, { error: 'invalid_token' });

    const body = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(body.status).toBe('invalid');
    expect(body.refresh).toEqual({ status: 'no_refresh_token', http_response: null });
    expect(body.mcp_probe.http_response).toMatchObject({
      status_code: 401,
      body: '{"error":"invalid_token"}',
      body_truncated: false,
    });
  });

  it('refreshes, persists rotated tokens, probes with the new token, and redacts the token response', async () => {
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential('rotate', {
      refresh: true,
    });
    const before = await loadRow(credentialId);
    const oldAccessRef = before.accessSecretRef;
    const oldRefreshRef = before.refreshSecretRef!;

    let tokenInit: RequestInit | undefined;
    let probeAuthHeader: string | undefined;
    fetchHandler = (url, init) => {
      if (url === TOKEN_ENDPOINT) {
        tokenInit = init;
        return jsonResponse(200, {
          access_token: 'rotated-access-token',
          refresh_token: 'rotated-refresh-token',
          token_type: 'Bearer',
        });
      }
      expect(url).toBe(mcpServerUrl);
      probeAuthHeader = (init.headers as Record<string, string>)['authorization'];
      return jsonResponse(200, { jsonrpc: '2.0', id: 1, result: {} });
    };

    const body = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(body.status).toBe('valid');
    expect(body.has_refresh_token).toBe(true);
    expect(body.refresh.status).toBe('succeeded');
    expect(body.refresh.http_response).toEqual({
      status_code: 200,
      content_type: 'application/json',
      body: '[redacted]',
      body_truncated: false,
    });
    expect(JSON.stringify(body)).not.toContain('rotated-access-token');
    expect(JSON.stringify(body)).not.toContain('rotated-refresh-token');

    // Token endpoint call shape: refresh_token grant with client_secret_basic auth.
    const tokenHeaders = tokenInit!.headers as Record<string, string>;
    expect(tokenHeaders['content-type']).toBe('application/x-www-form-urlencoded');
    expect(tokenHeaders['authorization']).toBe(
      `Basic ${Buffer.from('client_123:client-secret').toString('base64')}`,
    );
    const form = new URLSearchParams(String(tokenInit!.body));
    expect(form.get('grant_type')).toBe('refresh_token');
    expect(form.get('refresh_token')).toBe('refresh-token-rotate');
    expect(form.get('client_id')).toBe('client_123');
    expect(form.get('client_secret')).toBeNull();

    // Probe used the freshly minted access token.
    expect(probeAuthHeader).toBe('Bearer rotated-access-token');

    // Rotated tokens are persisted: new refs resolvable, old refs purged.
    const after = await loadRow(credentialId);
    expect(after.accessSecretRef).not.toBe(oldAccessRef);
    expect(after.refreshSecretRef).not.toBe(oldRefreshRef);
    expect(after.accessSecretRef).toContain('/rotations/');
    await expect(secretStore.resolve(after.accessSecretRef)).resolves.toBe('rotated-access-token');
    await expect(secretStore.resolve(after.refreshSecretRef!)).resolves.toBe(
      'rotated-refresh-token',
    );
    await expect(secretStore.resolve(oldAccessRef)).resolves.toBeNull();
    await expect(secretStore.resolve(oldRefreshRef)).resolves.toBeNull();
    expect(after.updatedAt.getTime()).toBeGreaterThanOrEqual(before.updatedAt.getTime());
  });

  it('accepts Slack authed_user token rotation responses and updates expires_at', async () => {
    const oldExpiresAt = new Date(Date.now() + 60_000).toISOString();
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential(
      'slack-authed-user',
      { refresh: true, expiresAt: oldExpiresAt },
    );
    const refreshStartedAt = Date.now();

    fetchHandler = (url) => {
      if (url === TOKEN_ENDPOINT) {
        return jsonResponse(200, {
          ok: true,
          authed_user: {
            access_token: 'slack-user-access-token',
            refresh_token: 'slack-user-refresh-token',
            expires_in: 43_200,
          },
        });
      }
      expect(url).toBe(mcpServerUrl);
      return jsonResponse(200, { jsonrpc: '2.0', id: 1, result: {} });
    };

    const body = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(body.status).toBe('valid');
    expect(body.refresh.status).toBe('succeeded');

    const after = await loadRow(credentialId);
    await expect(secretStore.resolve(after.accessSecretRef)).resolves.toBe(
      'slack-user-access-token',
    );
    await expect(secretStore.resolve(after.refreshSecretRef!)).resolves.toBe(
      'slack-user-refresh-token',
    );
    const expiresAt = (after.authConfig as { expires_at?: unknown }).expires_at;
    expect(typeof expiresAt).toBe('string');
    expect(expiresAt).not.toBe(oldExpiresAt);
    const expiresAtMs = Date.parse(expiresAt as string);
    expect(expiresAtMs).toBeGreaterThanOrEqual(refreshStartedAt + 43_200_000);
    expect(expiresAtMs).toBeLessThanOrEqual(Date.now() + 43_200_000);
  });

  it('refreshes and persists Slack user tokens from the internal force_refresh resolver', async () => {
    const { vaultId, credentialId } = await createOauthCredential('internal-force-refresh', {
      refresh: true,
    });
    const sessionId = await createSessionFixture(vaultId);
    const refreshStartedAt = Date.now();
    fetchHandler = (url) => {
      expect(url).toBe(TOKEN_ENDPOINT);
      return jsonResponse(200, {
        ok: true,
        authed_user: {
          access_token: 'internal-slack-access-token',
          refresh_token: 'internal-slack-refresh-token',
          expires_in: 43_200,
        },
      });
    };

    const refreshed = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${credentialId}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: {
        credential_id: credentialId,
        vault_id: credentialId,
        force_refresh: true,
      },
    });
    expect(refreshed.statusCode).toBe(200);
    expect(refreshed.json()).toMatchObject({
      credential_id: credentialId,
      vault_id: vaultId,
      scheme: 'bearer',
      secret_value: 'internal-slack-access-token',
    });
    expect(fetchImpl).toHaveBeenCalledTimes(1);

    const after = await loadRow(credentialId);
    await expect(secretStore.resolve(after.accessSecretRef)).resolves.toBe(
      'internal-slack-access-token',
    );
    await expect(secretStore.resolve(after.refreshSecretRef!)).resolves.toBe(
      'internal-slack-refresh-token',
    );
    const expiresAt = (after.authConfig as { expires_at?: unknown }).expires_at;
    expect(typeof expiresAt).toBe('string');
    expect(Date.parse(expiresAt as string)).toBeGreaterThanOrEqual(refreshStartedAt + 43_200_000);

    const cached = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${credentialId}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: {
        credential_id: credentialId,
        vault_id: credentialId,
        force_refresh: false,
      },
    });
    expect(cached.statusCode).toBe(200);
    expect(cached.json()).toMatchObject({ secret_value: 'internal-slack-access-token' });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('serializes validation against internal force_refresh and returns one committed winner', async () => {
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential(
      'validate-vs-force-refresh',
      { refresh: true },
    );
    const sessionId = await createSessionFixture(vaultId);
    const firstExchangeStarted = deferred<void>();
    const releaseFirstExchange = deferred<void>();
    let tokenEndpointCalls = 0;
    let probeCalls = 0;
    fetchHandler = async (url, init) => {
      if (url === TOKEN_ENDPOINT) {
        tokenEndpointCalls += 1;
        if (tokenEndpointCalls === 1) {
          firstExchangeStarted.resolve(undefined);
          await releaseFirstExchange.promise;
          return jsonResponse(200, {
            access_token: 'validate-force-access-token',
            refresh_token: 'validate-force-refresh-token',
          });
        }
        return jsonResponse(200, { ok: false, error: 'invalid_refresh_token' });
      }
      expect(url).toBe(mcpServerUrl);
      probeCalls += 1;
      expect((init.headers as Record<string, string>)['authorization']).toBe(
        'Bearer validate-force-access-token',
      );
      return jsonResponse(200, { jsonrpc: '2.0', id: 1, result: {} });
    };

    const validation = validate(vaultId, credentialId);
    await firstExchangeStarted.promise;
    const forced = app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${credentialId}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: {
        credential_id: credentialId,
        vault_id: credentialId,
        force_refresh: true,
      },
    });
    await sleep(100);
    releaseFirstExchange.resolve(undefined);

    const [validationResponse, forcedResponse] = await Promise.all([validation, forced]);
    expect(validationResponse.statusCode).toBe(200);
    expect(validationResponse.json<ValidationResponse>()).toMatchObject({
      status: 'valid',
      refresh: { status: 'succeeded' },
    });
    expect(forcedResponse.statusCode).toBe(200);
    expect(forcedResponse.json()).toMatchObject({
      credential_id: credentialId,
      secret_value: 'validate-force-access-token',
    });
    expect(tokenEndpointCalls).toBe(1);
    expect(probeCalls).toBe(1);
  });

  it('does not return a stale token when internal force_refresh is rejected', async () => {
    const { vaultId, credentialId } = await createOauthCredential('internal-refresh-rejected', {
      refresh: true,
    });
    const sessionId = await createSessionFixture(vaultId);
    const before = await loadRow(credentialId);
    fetchHandler = (url) => {
      expect(url).toBe(TOKEN_ENDPOINT);
      return jsonResponse(200, { ok: false, error: 'invalid_refresh_token' });
    };

    const rejected = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${credentialId}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: {
        credential_id: credentialId,
        vault_id: credentialId,
        force_refresh: true,
      },
    });
    expect(rejected.statusCode).toBe(409);
    expect(rejected.json()).toEqual({ error: 'oauth refresh rejected' });
    expect(JSON.stringify(rejected.json())).not.toContain(`access-token-internal-refresh-rejected`);

    const after = await loadRow(credentialId);
    expect(after.accessSecretRef).toBe(before.accessSecretRef);
    expect(after.refreshSecretRef).toBe(before.refreshSecretRef);
  });

  it('serializes concurrent internal refreshes and returns the committed winner', async () => {
    const { vaultId, credentialId } = await createOauthCredential('internal-refresh-lock', {
      refresh: true,
    });
    const sessionId = await createSessionFixture(vaultId);
    const firstExchangeStarted = deferred<void>();
    const releaseFirstExchange = deferred<void>();
    let tokenEndpointCalls = 0;
    fetchHandler = async (url) => {
      expect(url).toBe(TOKEN_ENDPOINT);
      tokenEndpointCalls += 1;
      if (tokenEndpointCalls === 1) {
        firstExchangeStarted.resolve(undefined);
        await releaseFirstExchange.promise;
        return jsonResponse(200, {
          access_token: 'serialized-access-token',
          refresh_token: 'serialized-refresh-token',
          expires_in: 43_200,
        });
      }
      // Without the cross-replica lease, the waiter would reuse Slack's
      // one-time refresh token and receive this definitive rejection.
      return jsonResponse(200, { ok: false, error: 'invalid_refresh_token' });
    };

    const request = () =>
      app.inject({
        method: 'POST',
        url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${credentialId}/resolve`,
        headers: { 'content-type': 'application/json' },
        payload: {
          credential_id: credentialId,
          vault_id: credentialId,
          force_refresh: true,
        },
      });

    const first = request();
    await firstExchangeStarted.promise;
    const waiter = request();
    await new Promise((resolve) => setTimeout(resolve, 100));
    releaseFirstExchange.resolve(undefined);

    const [firstResponse, waiterResponse] = await Promise.all([first, waiter]);
    expect(firstResponse.statusCode).toBe(200);
    expect(waiterResponse.statusCode).toBe(200);
    expect(firstResponse.json()).toMatchObject({ secret_value: 'serialized-access-token' });
    expect(waiterResponse.json()).toMatchObject({ secret_value: 'serialized-access-token' });
    expect(tokenEndpointCalls).toBe(1);

    const after = await loadRow(credentialId);
    expect(after.oauthRefreshLeaseOwner).toBeNull();
    expect(after.oauthRefreshLeaseExpiresAt).toBeNull();
    await expect(secretStore.resolve(after.accessSecretRef)).resolves.toBe(
      'serialized-access-token',
    );
    await expect(secretStore.resolve(after.refreshSecretRef!)).resolves.toBe(
      'serialized-refresh-token',
    );
  });

  it('renews the refresh lease while staged persistence crosses the lease TTL', async () => {
    const { vaultId, credentialId } = await createOauthCredential('refresh-lease-heartbeat', {
      refresh: true,
    });
    const initialRow = await loadRow(credentialId);
    const persistenceStarted = deferred<void>();
    const releasePersistence = deferred<void>();
    const originalPut = secretStore.put.bind(secretStore);
    let blocked = false;
    const putSpy = vi.spyOn(secretStore, 'put').mockImplementation(async (reference, value) => {
      if (
        !blocked &&
        reference.includes(`/${credentialId}/rotations/`) &&
        reference.endsWith('/access_token')
      ) {
        blocked = true;
        persistenceStarted.resolve(undefined);
        await releasePersistence.promise;
      }
      return originalPut(reference, value);
    });
    let tokenEndpointCalls = 0;
    fetchHandler = (url) => {
      expect(url).toBe(TOKEN_ENDPOINT);
      tokenEndpointCalls += 1;
      if (tokenEndpointCalls === 1) {
        return jsonResponse(200, {
          access_token: 'heartbeat-access-token',
          refresh_token: 'heartbeat-refresh-token',
        });
      }
      return jsonResponse(200, { ok: false, error: 'invalid_refresh_token' });
    };
    const timing = {
      // Keep the same ordering proof without making correctness depend on the
      // CI runner scheduling a 30 ms timer before a 120 ms lease expires.
      leaseTtlMs: 2_000,
      heartbeatIntervalMs: 100,
      waitTimeoutMs: 5_000,
      waitInitialMs: 10,
      waitMaxMs: 30,
    };
    const refresh = (row: typeof initialRow) =>
      refreshCredentialWithLease(
        db,
        secretStore,
        secretStore,
        fetchImpl as unknown as typeof fetch,
        app.log,
        row,
        workspaceId,
        credentialId,
        [vaultId],
        timing,
      );

    try {
      const first = refresh(initialRow);
      await persistenceStarted.promise;
      const claimed = await loadRow(credentialId);
      expect(claimed.oauthRefreshLeaseOwner).toEqual(expect.any(String));
      expect(claimed.oauthRefreshLeaseExpiresAt).not.toBeNull();

      await sleep(2_250);
      const renewed = await loadRow(credentialId);
      expect(renewed.oauthRefreshLeaseOwner).toBe(claimed.oauthRefreshLeaseOwner);
      expect(renewed.oauthRefreshLeaseExpiresAt!.getTime()).toBeGreaterThan(
        claimed.oauthRefreshLeaseExpiresAt!.getTime(),
      );

      const waiter = refresh(renewed);
      await sleep(100);
      releasePersistence.resolve(undefined);

      const [firstResult, waiterResult] = await Promise.all([first, waiter]);
      expect(firstResult).toMatchObject({
        status: 'succeeded',
        accessToken: 'heartbeat-access-token',
      });
      expect(waiterResult).toMatchObject({
        status: 'succeeded',
        accessToken: 'heartbeat-access-token',
      });
      expect(tokenEndpointCalls).toBe(1);

      const after = await loadRow(credentialId);
      expect(after.oauthRefreshLeaseOwner).toBeNull();
      expect(after.oauthRefreshLeaseExpiresAt).toBeNull();
    } finally {
      releasePersistence.resolve(undefined);
      putSpy.mockRestore();
    }
  });

  it('rejects access-only rotation while a live refresh lease persists rotated tokens', async () => {
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential(
      'access-only-vs-refresh',
      { refresh: true },
    );
    const before = await loadRow(credentialId);
    const persistenceStarted = deferred<void>();
    const releasePersistence = deferred<void>();
    const originalPut = secretStore.put.bind(secretStore);
    let blocked = false;
    const putSpy = vi.spyOn(secretStore, 'put').mockImplementation(async (reference, value) => {
      if (
        !blocked &&
        reference.includes(`/${credentialId}/rotations/`) &&
        reference.endsWith('/access_token')
      ) {
        blocked = true;
        persistenceStarted.resolve(undefined);
        await releasePersistence.promise;
      }
      return originalPut(reference, value);
    });
    fetchHandler = (url, init) => {
      if (url === TOKEN_ENDPOINT) {
        return jsonResponse(200, {
          access_token: 'lease-winner-access-token',
          refresh_token: 'lease-winner-refresh-token',
        });
      }
      expect(url).toBe(mcpServerUrl);
      expect((init.headers as Record<string, string>)['authorization']).toBe(
        'Bearer lease-winner-access-token',
      );
      return jsonResponse(200, { jsonrpc: '2.0', id: 1, result: {} });
    };

    try {
      const validation = validate(vaultId, credentialId);
      await persistenceStarted.promise;

      const leased = await loadRow(credentialId);
      expect(leased.oauthRefreshLeaseOwner).toEqual(expect.any(String));
      expect(leased.oauthRefreshLeaseExpiresAt).not.toBeNull();

      const update = await app.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials/${credentialId}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          auth: { type: 'mcp_oauth', access_token: 'manual-access-only-token' },
        },
      });
      expect(update.statusCode).toBe(409);
      expect(update.json<ClaudeErrorOut>()).toMatchObject({
        type: 'error',
        error: { type: 'conflict_error', message: 'credential refresh in progress' },
        request_id: expect.any(String),
      });

      const stillLeased = await loadRow(credentialId);
      expect(stillLeased.accessSecretRef).toBe(before.accessSecretRef);
      expect(stillLeased.refreshSecretRef).toBe(before.refreshSecretRef);
      expect(stillLeased.oauthRefreshLeaseOwner).toBe(leased.oauthRefreshLeaseOwner);

      releasePersistence.resolve(undefined);
      const validationResponse = await validation;
      expect(validationResponse.statusCode).toBe(200);
      expect(validationResponse.json<ValidationResponse>()).toMatchObject({
        status: 'valid',
        refresh: { status: 'succeeded' },
      });

      const after = await loadRow(credentialId);
      expect(after.oauthRefreshLeaseOwner).toBeNull();
      expect(after.oauthRefreshLeaseExpiresAt).toBeNull();
      await expect(secretStore.resolve(after.accessSecretRef)).resolves.toBe(
        'lease-winner-access-token',
      );
      await expect(secretStore.resolve(after.refreshSecretRef!)).resolves.toBe(
        'lease-winner-refresh-token',
      );
      await expect(secretStore.resolve(before.refreshSecretRef!)).resolves.toBeNull();
    } finally {
      releasePersistence.resolve(undefined);
      putSpy.mockRestore();
    }
  });

  it('takes over an expired internal refresh lease', async () => {
    const { vaultId, credentialId } = await createOauthCredential('expired-refresh-lease', {
      refresh: true,
    });
    const sessionId = await createSessionFixture(vaultId);
    await db
      .update(vaultCredentials)
      .set({
        oauthRefreshLeaseOwner: 'dead-replica',
        oauthRefreshLeaseExpiresAt: new Date(Date.now() - 1_000),
      })
      .where(eq(vaultCredentials.id, credentialId));
    fetchHandler = (url) => {
      expect(url).toBe(TOKEN_ENDPOINT);
      return jsonResponse(200, {
        access_token: 'lease-takeover-access-token',
        refresh_token: 'lease-takeover-refresh-token',
      });
    };

    const refreshed = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${credentialId}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: {
        credential_id: credentialId,
        vault_id: credentialId,
        force_refresh: true,
      },
    });
    expect(refreshed.statusCode).toBe(200);
    expect(refreshed.json()).toMatchObject({ secret_value: 'lease-takeover-access-token' });

    const after = await loadRow(credentialId);
    expect(after.oauthRefreshLeaseOwner).toBeNull();
    expect(after.oauthRefreshLeaseExpiresAt).toBeNull();
  });

  it('reports invalid on a 4xx refresh failure and leaves stored secrets untouched', async () => {
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential('bad-grant', {
      refresh: true,
    });
    const before = await loadRow(credentialId);

    fetchHandler = (url) => {
      if (url === TOKEN_ENDPOINT) return jsonResponse(400, { error: 'invalid_grant' });
      expect(url).toBe(mcpServerUrl);
      // Probe succeeding must not override the conclusive refresh rejection.
      return jsonResponse(200, { jsonrpc: '2.0', id: 1, result: {} });
    };

    const body = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(body.status).toBe('invalid');
    expect(body.has_refresh_token).toBe(true);
    expect(body.refresh.status).toBe('failed');
    expect(body.refresh.http_response).toMatchObject({
      status_code: 400,
      body: '{"error":"invalid_grant"}',
      body_truncated: false,
    });

    const after = await loadRow(credentialId);
    expect(after.accessSecretRef).toBe(before.accessSecretRef);
    expect(after.refreshSecretRef).toBe(before.refreshSecretRef);
    await expect(secretStore.resolve(after.accessSecretRef)).resolves.toBe(
      'access-token-bad-grant',
    );
    await expect(secretStore.resolve(after.refreshSecretRef!)).resolves.toBe(
      'refresh-token-bad-grant',
    );
  });

  it('treats Slack ok:false in an HTTP 200 response as a failed refresh', async () => {
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential('slack-ok-false', {
      refresh: true,
    });
    const before = await loadRow(credentialId);
    fetchHandler = (url) => {
      if (url === TOKEN_ENDPOINT) {
        return jsonResponse(200, { ok: false, error: 'invalid_refresh_token' });
      }
      expect(url).toBe(mcpServerUrl);
      return jsonResponse(200, { jsonrpc: '2.0', id: 1, result: {} });
    };

    const body = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(body.status).toBe('invalid');
    expect(body.refresh.status).toBe('failed');
    expect(body.refresh.http_response).toMatchObject({
      status_code: 200,
      body: '[redacted]',
    });

    const after = await loadRow(credentialId);
    expect(after.accessSecretRef).toBe(before.accessSecretRef);
    expect(after.refreshSecretRef).toBe(before.refreshSecretRef);
  });

  it('keeps transient Slack ok:false refresh errors retryable', async () => {
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential(
      'slack-transient-error',
      { refresh: true },
    );
    const sessionId = await createSessionFixture(vaultId);
    const before = await loadRow(credentialId);
    fetchHandler = (url) => {
      if (url === TOKEN_ENDPOINT) {
        return jsonResponse(200, { ok: false, error: 'internal_error' });
      }
      expect(url).toBe(mcpServerUrl);
      return jsonResponse(200, { jsonrpc: '2.0', id: 1, result: {} });
    };

    const validation = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(validation.status).toBe('unknown');
    expect(validation.refresh.status).toBe('connect_error');

    const resolved = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${credentialId}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: {
        credential_id: credentialId,
        vault_id: credentialId,
        force_refresh: true,
      },
    });
    expect(resolved.statusCode).toBe(503);
    expect(resolved.json()).toEqual({ error: 'oauth refresh unavailable' });

    const after = await loadRow(credentialId);
    expect(after.accessSecretRef).toBe(before.accessSecretRef);
    expect(after.refreshSecretRef).toBe(before.refreshSecretRef);
  });

  it('scrubs stored secrets from failed refresh and MCP probe response captures', async () => {
    const label = 'scrub-captures';
    const accessToken = `access token+/${label}`;
    const refreshToken = `refresh token+/${label}`;
    const clientSecret = 'client secret+/?';
    const basicCredential = Buffer.from(`client_123:${clientSecret}`).toString('base64');
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential(label, {
      refresh: true,
    });
    const row = await loadRow(credentialId);
    await secretStore.put(row.accessSecretRef, accessToken);
    await secretStore.put(row.refreshSecretRef!, refreshToken);
    await secretStore.put(row.clientSecretRef!, clientSecret);
    const formEncodedRefresh = new URLSearchParams([['secret', refreshToken]])
      .toString()
      .slice('secret='.length);

    fetchHandler = (url) => {
      if (url === TOKEN_ENDPOINT) {
        return new Response(
          JSON.stringify({
            error: 'invalid_grant',
            debug: `${accessToken} ${formEncodedRefresh} ${clientSecret} ${basicCredential}`,
          }),
          { status: 400, headers: { 'content-type': 'application/json' } },
        );
      }
      expect(url).toBe(mcpServerUrl);
      return jsonResponse(401, {
        error: 'invalid_token',
        debug: `Bearer ${accessToken}`,
      });
    };

    const body = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(body.status).toBe('invalid');
    expect(body.refresh.http_response?.body).toContain('[redacted]');
    expect(body.mcp_probe.http_response?.body).toContain('[redacted]');
    const serialized = JSON.stringify(body);
    for (const secret of [
      accessToken,
      refreshToken,
      formEncodedRefresh,
      clientSecret,
      basicCredential,
    ]) {
      expect(serialized).not.toContain(secret);
    }
  });

  it('reports unknown with a null probe capture when the probe fetch fails', async () => {
    const { vaultId, credentialId } = await createOauthCredential('probe-down');
    fetchHandler = () => {
      throw new TypeError('fetch failed: network unreachable');
    };

    const body = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(body.status).toBe('unknown');
    expect(body.mcp_probe.http_response).toBeNull();
    expect(body.refresh).toEqual({ status: 'no_refresh_token', http_response: null });
  });

  it('reports unknown when the token endpoint returns 5xx and the probe is inconclusive', async () => {
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential('token-5xx', {
      refresh: true,
    });
    fetchHandler = (url) => {
      if (url === TOKEN_ENDPOINT) return jsonResponse(503, { error: 'temporarily_unavailable' });
      expect(url).toBe(mcpServerUrl);
      return jsonResponse(500, { error: 'server_error' });
    };

    const body = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(body.status).toBe('unknown');
    expect(body.refresh.status).toBe('connect_error');
    expect(body.refresh.http_response).toMatchObject({ status_code: 503 });
    expect(body.mcp_probe.http_response).toMatchObject({ status_code: 500 });
  });

  it('short-circuits top-level status to unknown on a transient refresh error even when the old access token still probes 2xx', async () => {
    const { vaultId, credentialId, mcpServerUrl } = await createOauthCredential(
      'token-5xx-probe-ok',
      { refresh: true },
    );
    fetchHandler = (url) => {
      if (url === TOKEN_ENDPOINT) return jsonResponse(503, { error: 'temporarily_unavailable' });
      // The stale access token still works — this must NOT be allowed to mask
      // the transient refresh breakage as top-level 'valid'.
      expect(url).toBe(mcpServerUrl);
      return jsonResponse(200, { jsonrpc: '2.0', id: 1, result: {} });
    };

    const body = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(body.refresh.status).toBe('connect_error');
    expect(body.mcp_probe.http_response).toMatchObject({ status_code: 200 });
    expect(body.status).toBe('unknown');
  });

  it('returns 404 for unknown credentials and 400 for non-mcp_oauth credentials', async () => {
    const vaultId = await createVault('errors');
    const missing = await validate(vaultId, 'vcrd_does_not_exist');
    expect(missing.statusCode).toBe(404);
    expect(missing.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: { type: 'not_found_error', message: 'not found' },
      request_id: expect.any(String),
    });

    const missingVault = await validate('vlt_does_not_exist', 'vcrd_does_not_exist');
    expect(missingVault.statusCode).toBe(404);

    const staticCreate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        auth: {
          type: 'static_bearer',
          token: 'bearer-token',
          mcp_server_url: `${MCP_URL_BASE}/static/sse`,
        },
      },
    });
    expect(staticCreate.statusCode).toBe(200);
    const staticId = staticCreate.json<{ id: string }>().id;
    const wrongType = await validate(vaultId, staticId);
    expect(wrongType.statusCode).toBe(400);
    expect(wrongType.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: { type: 'invalid_request_error', message: 'credential is not mcp_oauth' },
      request_id: expect.any(String),
    });
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it('accepts a completely bodyless POST', async () => {
    const { vaultId, credentialId } = await createOauthCredential('bodyless');
    fetchHandler = () => jsonResponse(200, { jsonrpc: '2.0', id: 1, result: {} });

    // No payload and no content-type at all.
    const bare = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credentialId}/mcp_oauth_validate`,
      headers: { 'x-api-key': apiKey },
    });
    expect(bare.statusCode).toBe(200);
    expect(bare.json<ValidationResponse>().status).toBe('valid');

    // Empty body with an explicit application/json content type.
    const emptyJson = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credentialId}/mcp_oauth_validate`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: '',
    });
    expect(emptyJson.statusCode).toBe(200);
    expect(emptyJson.json<ValidationResponse>().status).toBe('valid');
  });

  it('truncates captured probe bodies at 2048 chars', async () => {
    const { vaultId, credentialId } = await createOauthCredential('big-body');
    const bigBody = 'x'.repeat(3000);
    fetchHandler = () =>
      new Response(bigBody, { status: 200, headers: { 'content-type': 'text/html' } });

    const body = (await validate(vaultId, credentialId)).json<ValidationResponse>();
    expect(body.status).toBe('valid');
    expect(body.mcp_probe.http_response).toMatchObject({
      status_code: 200,
      content_type: 'text/html',
      body_truncated: true,
    });
    expect(body.mcp_probe.http_response!.body).toHaveLength(2048);
    expect(body.mcp_probe.http_response!.body).toBe(bigBody.slice(0, 2048));
  });

  it('returns 404 for an archived credential without probing or refreshing', async () => {
    const { vaultId, credentialId } = await createOauthCredential('archived', { refresh: true });

    const archive = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credentialId}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
    });
    expect(archive.statusCode).toBe(200);

    const res = await validate(vaultId, credentialId);
    expect(res.statusCode).toBe(404);
    expect(res.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: { type: 'not_found_error', message: 'not found' },
      request_id: expect.any(String),
    });
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it('returns 409 when another writer wins the token-rotation race', async () => {
    const { vaultId, credentialId } = await createOauthCredential('concurrent-rotate', {
      refresh: true,
    });

    // Deterministically simulate a concurrent rotation: mutate the row's
    // access_secret_ref out from under this request's CAS while the token
    // exchange is in flight, awaited so the write is visible before
    // persistRotatedTokens issues its conditional UPDATE.
    fetchHandler = async (url) => {
      if (url !== TOKEN_ENDPOINT) throw new Error(`unexpected fetch to ${url}`);
      await db
        .update(vaultCredentials)
        .set({ accessSecretRef: 'vsec:concurrent-writer', updatedAt: new Date() })
        .where(eq(vaultCredentials.id, credentialId));
      return jsonResponse(200, {
        access_token: 'rotated-access-token-loser',
        refresh_token: 'rotated-refresh-token-loser',
        token_type: 'Bearer',
      });
    };

    const res = await validate(vaultId, credentialId);
    expect(res.statusCode).toBe(409);
    expect(res.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: { message: 'credential was rotated concurrently' },
      request_id: expect.any(String),
    });
    // CAS loses before the probe step is reached: only the token exchange fires.
    expect(fetchImpl).toHaveBeenCalledTimes(1);

    // The lost rotation's staged tokens must not have been persisted; the
    // concurrent writer's ref (simulating the winner) is left untouched.
    const after = await loadRow(credentialId);
    expect(after.accessSecretRef).toBe('vsec:concurrent-writer');
  });

  function jsonResponse(status: number, payload: unknown): Response {
    return new Response(JSON.stringify(payload), {
      status,
      headers: { 'content-type': 'application/json' },
    });
  }

  function deferred<T>(): {
    promise: Promise<T>;
    resolve: (value: T | PromiseLike<T>) => void;
    reject: (reason?: unknown) => void;
  } {
    let resolve!: (value: T | PromiseLike<T>) => void;
    let reject!: (reason?: unknown) => void;
    const promise = new Promise<T>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    return { promise, resolve, reject };
  }

  function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function validate(vaultId: string, credentialId: string) {
    return app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credentialId}/mcp_oauth_validate`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
  }

  async function loadRow(credentialId: string) {
    const rows = await db
      .select()
      .from(vaultCredentials)
      .where(eq(vaultCredentials.id, credentialId));
    expect(rows).toHaveLength(1);
    return rows[0]!;
  }

  async function createVault(label: string): Promise<string> {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/vaults',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        display_name: `vcv-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      },
    });
    expect(res.statusCode).toBe(200);
    return res.json<{ id: string }>().id;
  }

  async function createOauthCredential(
    label: string,
    options: { refresh?: boolean; expiresAt?: string } = {},
  ): Promise<{ vaultId: string; credentialId: string; mcpServerUrl: string }> {
    const vaultId = await createVault(label);
    const mcpServerUrl = `${MCP_URL_BASE}/${label}/sse`;
    const res = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        auth: {
          type: 'mcp_oauth',
          access_token: `access-token-${label}`,
          mcp_server_url: mcpServerUrl,
          ...(options.expiresAt !== undefined ? { expires_at: options.expiresAt } : {}),
          ...(options.refresh
            ? {
                refresh: {
                  refresh_token: `refresh-token-${label}`,
                  token_endpoint: TOKEN_ENDPOINT,
                  client_id: 'client_123',
                  token_endpoint_auth: {
                    type: 'client_secret_basic',
                    client_secret: 'client-secret',
                  },
                },
              }
            : {}),
        },
      },
    });
    expect(res.statusCode).toBe(200);
    return { vaultId, credentialId: res.json<{ id: string }>().id, mcpServerUrl };
  }

  async function createSessionFixture(vaultId: string): Promise<string> {
    const agentId = newId('agt');
    const agentVersionId = newId('agtv');
    const sessionId = newId('ses');
    const now = new Date();
    await db.insert(agents).values({
      id: agentId,
      workspaceId,
      name: `oauth-refresh-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      version: 1,
      latestVersionId: null,
      modelProvider: 'anthropic',
      modelId: 'claude-test',
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(agentVersions).values({
      id: agentVersionId,
      workspaceId,
      agentId,
      version: 1,
      snapshot: {
        id: agentId,
        name: 'oauth refresh fixture',
        version: 1,
        model: { provider: 'anthropic', id: 'claude-test' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
        multiagent: null,
      },
      createdAt: now,
    });
    await db
      .update(agents)
      .set({ latestVersionId: agentVersionId })
      .where(and(eq(agents.workspaceId, workspaceId), eq(agents.id, agentId)));
    await db.insert(sessions).values({
      id: sessionId,
      workspaceId,
      agentId,
      agentVersion: 1,
      status: 'idle',
      vaultIds: [vaultId],
      createdAt: now,
      updatedAt: now,
    });
    return sessionId;
  }
});
