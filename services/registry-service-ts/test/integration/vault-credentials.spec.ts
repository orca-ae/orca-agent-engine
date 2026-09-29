// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { and, eq, isNull } from 'drizzle-orm';
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
import type { SecretStore, SecretStorePutOptions } from '../../src/secrets/index.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  adminAuditEvents,
  agents,
  agentVersions,
  sessions,
  vaultCredentials,
} from '../../src/persistence/postgres/schema.js';
import { newId } from '../../src/domain/versioning.js';
import {
  LOGICAL_CREDENTIAL_ID_MAX_LENGTH,
  LOGICAL_CREDENTIAL_ID_PREFIX,
} from '../../src/domain/provider-credential.js';

interface CredentialResponse {
  id: string;
  type: 'vault_credential';
  vault_id: string;
  display_name: string | null;
  metadata: Record<string, unknown>;
  auth: {
    type: 'static_bearer' | 'mcp_oauth';
    mcp_server_url: string;
    expires_at?: string | null;
    refresh?: {
      token_endpoint: string;
      client_id: string;
      token_endpoint_auth: { type: string };
      resource?: string | null;
      scope?: string | null;
    };
  };
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

interface EnvVarCredentialResponse {
  id: string;
  type: 'vault_credential';
  vault_id: string;
  display_name: string | null;
  metadata: Record<string, unknown>;
  auth: {
    type: 'environment_variable';
    secret_name: string;
    networking: { type: 'limited'; allowed_hosts: string[] } | { type: 'unrestricted' };
    injection_location: { header: boolean; body: boolean };
  };
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

interface ProviderCredentialResponse {
  id: string;
  type: 'vault_credential';
  vault_id: string;
  display_name: string | null;
  metadata: Record<string, unknown>;
  auth: {
    type: 'provider';
    provider: 'anthropic' | 'openai' | 'openai_compatible' | 'azure_openai' | 'vertex' | 'bedrock';
    scheme: 'api_key' | 'bearer' | 'gcp-service-account' | 'aws-sig-v4';
    logical_id: string;
    version: string;
  };
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

interface ClaudeErrorOut {
  type: 'error';
  error: { type: string; message: string };
  request_id: string | null;
}

const MAXIMUM_LOGICAL_ID = `${LOGICAL_CREDENTIAL_ID_PREFIX}${'a'.repeat(
  LOGICAL_CREDENTIAL_ID_MAX_LENGTH - LOGICAL_CREDENTIAL_ID_PREFIX.length,
)}`;

const INVALID_LOGICAL_IDS = [
  ['below-minimum', LOGICAL_CREDENTIAL_ID_PREFIX],
  ['overlength', `${MAXIMUM_LOGICAL_ID}a`],
  ['non-ASCII', 'llm:\u6a21\u578b'],
  ['whitespace', 'llm:with space'],
  ['slash', 'llm:with/value'],
  ['question mark', 'llm:with?value'],
  ['fragment', 'llm:with#value'],
  ['percent', 'llm:with%value'],
] as const;

describe('Vault credentials lifecycle (integration)', () => {
  let app: FastifyInstance;
  let db: DbClient;
  let workspaceId: string;
  let otherWorkspaceId: string;
  let apiKey: string;
  let otherApiKey: string;
  let secretStore: LocalSecretStore;

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
    });
    await app.ready();
    workspaceId = uniqueWorkspace('vault_credentials');
    otherWorkspaceId = uniqueWorkspace('vault_credentials_other');
    apiKey = await createTestApiKey(db, workspaceId);
    otherApiKey = await createTestApiKey(db, otherWorkspaceId);
  });

  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  it('creates static bearer credentials with vcrd_ ids, sanitized responses, and secret refs only in Postgres', async () => {
    const vaultId = await createVault('static');
    const create = await createStaticCredential(
      vaultId,
      'https://mcp.example.com/sse',
      'secret-token',
      {
        display_name: 'Acme MCP',
        metadata: { user_id: 'u_123' },
      },
    );

    expect(create.statusCode).toBe(200);
    const credential = create.json<CredentialResponse>();
    expect(credential.id).toMatch(/^vcrd_/);
    expect(credential.vault_id).toBe(vaultId);
    expect(credential.display_name).toBe('Acme MCP');
    expect(credential.metadata).toMatchObject({ user_id: 'u_123' });
    expect(credential.auth).toEqual({
      type: 'static_bearer',
      mcp_server_url: 'https://mcp.example.com/sse',
    });
    expect(JSON.stringify(credential)).not.toContain('secret-token');
    expect(JSON.stringify(credential)).not.toContain('accessSecretRef');

    const rows = await db
      .select()
      .from(vaultCredentials)
      .where(eq(vaultCredentials.id, credential.id));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.accessSecretRef).toContain(
      `vault_credentials/${rows[0]!.workspaceId}/${credential.id}/access_token`,
    );
    expect(JSON.stringify(rows[0])).not.toContain('secret-token');
    await expect(secretStore.resolve(rows[0]!.accessSecretRef)).resolves.toBe('secret-token');
  });

  it('creates mcp_oauth credentials and omits access, refresh, and client secrets', async () => {
    const vaultId = await createVault('oauth');
    const res = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        display_name: 'Linear OAuth',
        auth: {
          type: 'mcp_oauth',
          access_token: 'access-token',
          mcp_server_url: 'https://mcp.linear.app/sse',
          refresh: {
            refresh_token: 'refresh-token',
            token_endpoint: 'https://linear.app/oauth/token',
            client_id: 'client_123',
            token_endpoint_auth: {
              type: 'client_secret_basic',
              client_secret: 'client-secret',
            },
          },
        },
      },
    });

    expect(res.statusCode).toBe(200);
    const credential = res.json<CredentialResponse>();
    expect(credential.auth).toEqual({
      type: 'mcp_oauth',
      mcp_server_url: 'https://mcp.linear.app/sse',
      expires_at: null,
      refresh: {
        token_endpoint: 'https://linear.app/oauth/token',
        client_id: 'client_123',
        token_endpoint_auth: { type: 'client_secret_basic' },
        resource: null,
        scope: null,
      },
    });
    const body = JSON.stringify(credential);
    expect(body).not.toContain('access-token');
    expect(body).not.toContain('refresh-token');
    expect(body).not.toContain('client-secret');

    const rows = await db
      .select()
      .from(vaultCredentials)
      .where(eq(vaultCredentials.id, credential.id));
    expect(rows[0]!.refreshSecretRef).toBeTruthy();
    expect(rows[0]!.clientSecretRef).toBeTruthy();
    await expect(secretStore.resolve(rows[0]!.accessSecretRef)).resolves.toBe('access-token');
    await expect(secretStore.resolve(rows[0]!.refreshSecretRef!)).resolves.toBe('refresh-token');
    await expect(secretStore.resolve(rows[0]!.clientSecretRef!)).resolves.toBe('client-secret');

    const noRefresh = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        display_name: 'No Refresh OAuth',
        auth: {
          type: 'mcp_oauth',
          access_token: 'access-only-token',
          mcp_server_url: 'https://mcp-no-refresh.linear.app/sse',
        },
      },
    });
    expect(noRefresh.statusCode).toBe(200);
    expect(noRefresh.json<CredentialResponse>().auth).toEqual({
      type: 'mcp_oauth',
      mcp_server_url: 'https://mcp-no-refresh.linear.app/sse',
      expires_at: null,
    });

    const nullRefresh = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        display_name: 'Null Refresh OAuth',
        auth: {
          type: 'mcp_oauth',
          access_token: 'access-null-refresh-token',
          mcp_server_url: 'https://mcp-null-refresh.linear.app/sse',
          refresh: null,
        },
      },
    });
    expect(nullRefresh.statusCode).toBe(200);
    expect(nullRefresh.json<CredentialResponse>().auth).toEqual({
      type: 'mcp_oauth',
      mcp_server_url: 'https://mcp-null-refresh.linear.app/sse',
      expires_at: null,
    });
  });

  it('rejects adding an orphan refresh token to OAuth credentials without refresh configuration', async () => {
    const vaultId = await createVault('oauth-orphan-refresh');
    const create = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        auth: {
          type: 'mcp_oauth',
          access_token: 'access-only',
          mcp_server_url: 'https://oauth-orphan-refresh.example/sse',
        },
      },
    });
    expect(create.statusCode).toBe(200);
    const credential = create.json<CredentialResponse>();

    const update = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        auth: {
          type: 'mcp_oauth',
          refresh: { refresh_token: 'orphan-refresh-token' },
        },
      },
    });

    expect(update.statusCode).toBe(400);
    expect(update.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: {
        type: 'invalid_request_error',
        message: 'auth.refresh cannot add a refresh token without existing refresh configuration',
      },
      request_id: expect.any(String),
    });
    expect(JSON.stringify(update.json())).not.toContain('orphan-refresh-token');
    const row = (
      await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, credential.id))
    )[0]!;
    expect(row.refreshSecretRef).toBeNull();
  });

  it.each(['client_secret_basic', 'client_secret_post'] as const)(
    'requires a client secret when changing OAuth endpoint auth from none to %s',
    async (endpointAuthType) => {
      const vaultId = await createVault(`oauth-enable-${endpointAuthType}`);
      const create = await app.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          auth: {
            type: 'mcp_oauth',
            access_token: `access-${endpointAuthType}`,
            mcp_server_url: `https://${endpointAuthType.replaceAll('_', '-')}.example/sse`,
            refresh: {
              refresh_token: `refresh-${endpointAuthType}`,
              token_endpoint: 'https://oauth.example/token',
              client_id: 'client-id',
              token_endpoint_auth: { type: 'none' },
            },
          },
        },
      });
      expect(create.statusCode).toBe(200);
      const credential = create.json<CredentialResponse>();

      const missingSecret = await app.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          auth: {
            type: 'mcp_oauth',
            refresh: { token_endpoint_auth: { type: endpointAuthType } },
          },
        },
      });
      expect(missingSecret.statusCode).toBe(400);
      expect(missingSecret.json<ClaudeErrorOut>()).toMatchObject({
        type: 'error',
        error: {
          type: 'invalid_request_error',
          message:
            'auth.refresh.token_endpoint_auth.client_secret is required when enabling client authentication',
        },
        request_id: expect.any(String),
      });

      const clientSecret = `secret-${endpointAuthType}`;
      const update = await app.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          auth: {
            type: 'mcp_oauth',
            refresh: {
              token_endpoint_auth: { type: endpointAuthType, client_secret: clientSecret },
            },
          },
        },
      });
      expect(update.statusCode).toBe(200);
      expect(update.json<CredentialResponse>().auth.refresh?.token_endpoint_auth).toEqual({
        type: endpointAuthType,
      });
      expect(JSON.stringify(update.json())).not.toContain(clientSecret);
      const row = (
        await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, credential.id))
      )[0]!;
      expect(row.clientSecretRef).not.toBeNull();
      await expect(secretStore.resolve(row.clientSecretRef!)).resolves.toBe(clientSecret);

      const clearSecret = await app.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          auth: {
            type: 'mcp_oauth',
            refresh: {
              token_endpoint_auth: { type: endpointAuthType, client_secret: null },
            },
          },
        },
      });
      expect(clearSecret.statusCode).toBe(400);
      expect(clearSecret.json<ClaudeErrorOut>()).toMatchObject({
        type: 'error',
        error: {
          type: 'invalid_request_error',
          message:
            'auth.refresh.token_endpoint_auth.client_secret cannot be null for client authentication',
        },
        request_id: expect.any(String),
      });
      const unchanged = (
        await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, credential.id))
      )[0]!;
      expect(unchanged.clientSecretRef).toBe(row.clientSecretRef);
      await expect(secretStore.resolve(unchanged.clientSecretRef!)).resolves.toBe(clientSecret);
    },
  );

  it('lists newest active credentials first and fetches one workspace-owned credential', async () => {
    const vaultId = await createVault('list');
    const first = (
      await createStaticCredential(vaultId, 'https://one.example/sse', 'one')
    ).json<CredentialResponse>();
    await new Promise((resolve) => setTimeout(resolve, 5));
    const second = (
      await createStaticCredential(vaultId, 'https://two.example/sse', 'two')
    ).json<CredentialResponse>();
    await new Promise((resolve) => setTimeout(resolve, 5));
    const third = (
      await createStaticCredential(vaultId, 'https://three.example/sse', 'three')
    ).json<CredentialResponse>();

    const list = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey },
    });
    expect(list.statusCode).toBe(200);
    const credentials = list.json<{ data: CredentialResponse[] }>().data;
    expect(credentials.map((c) => c.id).slice(0, 3)).toEqual([third.id, second.id, first.id]);

    const firstPage = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials?limit=1`,
      headers: { 'x-api-key': apiKey },
    });
    expect(firstPage.statusCode).toBe(200);
    const firstPageBody = firstPage.json<{
      data: CredentialResponse[];
      next_page: string | null;
    }>();
    expect(firstPageBody.data.map((c) => c.id)).toEqual([third.id]);
    expect(firstPageBody.next_page).toBeTruthy();

    const secondPage = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials?limit=1&page=${encodeURIComponent(
        firstPageBody.next_page!,
      )}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(secondPage.statusCode).toBe(200);
    expect(secondPage.json<{ data: CredentialResponse[] }>().data.map((c) => c.id)).toEqual([
      second.id,
    ]);

    const emptyPage = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials?page=`,
      headers: { 'x-api-key': apiKey },
    });
    expect(emptyPage.statusCode).toBe(200);

    const get = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials/${first.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json<CredentialResponse>().id).toBe(first.id);

    const foreignGet = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials/${first.id}`,
      headers: { 'x-api-key': otherApiKey },
    });
    expect(foreignGet.statusCode).toBe(404);
  });

  it('updates mutable fields and rotates secrets while rejecting mcp_server_url changes', async () => {
    const vaultId = await createVault('update');
    const credential = (
      await createStaticCredential(vaultId, 'https://update.example/sse', 'old-token')
    ).json<CredentialResponse>();

    const bad = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { auth: { mcp_server_url: 'https://changed.example/sse' } },
    });
    expect(bad.statusCode).toBe(400);

    const update = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        display_name: 'rotated',
        metadata: { rotated: 'true', keep: 'yes' },
        auth: { type: 'static_bearer', token: 'new-token' },
      },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json<CredentialResponse>()).toMatchObject({
      id: credential.id,
      display_name: 'rotated',
      metadata: { rotated: 'true' },
      auth: { type: 'static_bearer', mcp_server_url: 'https://update.example/sse' },
    });

    const patch = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { metadata: { keep: null, added: 'value' } },
    });
    expect(patch.statusCode).toBe(200);
    expect(patch.json<CredentialResponse>().metadata).toEqual({
      rotated: 'true',
      added: 'value',
    });
    const rows = await db
      .select()
      .from(vaultCredentials)
      .where(eq(vaultCredentials.id, credential.id));
    await expect(secretStore.resolve(rows[0]!.accessSecretRef)).resolves.toBe('new-token');
  });

  it('archives and deletes credentials, purging stored secrets', async () => {
    const vaultId = await createVault('lifecycle');
    const archived = (
      await createStaticCredential(vaultId, 'https://archive.example/sse', 'archive-token')
    ).json<CredentialResponse>();
    const archivedRef = (
      await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, archived.id))
    )[0]!.accessSecretRef;

    const archive = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${archived.id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archive.statusCode).toBe(200);
    expect(archive.json<CredentialResponse>().archived_at).not.toBeNull();
    await expect(secretStore.resolve(archivedRef)).resolves.toBeNull();

    const list = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey },
    });
    expect(list.json<{ data: CredentialResponse[] }>().data.some((c) => c.id === archived.id)).toBe(
      false,
    );

    const updateArchived = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${archived.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { auth: { type: 'static_bearer', token: 'recreated-token' } },
    });
    expect(updateArchived.statusCode).toBe(404);
    await expect(secretStore.resolve(archivedRef)).resolves.toBeNull();

    const deleted = (
      await createStaticCredential(vaultId, 'https://delete.example/sse', 'delete-token')
    ).json<CredentialResponse>();
    const deletedRef = (
      await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, deleted.id))
    )[0]!.accessSecretRef;
    const del = await app.inject({
      method: 'DELETE',
      url: `/v1/vaults/${vaultId}/credentials/${deleted.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(del.statusCode).toBe(200);
    expect(del.json()).toEqual({ id: deleted.id, type: 'vault_credential_deleted' });
    await expect(secretStore.resolve(deletedRef)).resolves.toBeNull();
    const rows = await db
      .select()
      .from(vaultCredentials)
      .where(eq(vaultCredentials.id, deleted.id));
    expect(rows).toHaveLength(1);
    expect(rows[0]!.deletedAt).toBeInstanceOf(Date);
  });

  it('lists archived credentials only when include_archived=true and rejects invalid pagination filters', async () => {
    const vaultId = await createVault('credential-filters');
    const credential = (
      await createStaticCredential(vaultId, 'https://credential-filter.example/sse', 'token')
    ).json<CredentialResponse>();

    const archive = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credential.id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archive.statusCode).toBe(200);

    const defaultList = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey },
    });
    expect(defaultList.statusCode).toBe(200);
    expect(
      defaultList.json<{ data: CredentialResponse[] }>().data.some((c) => c.id === credential.id),
    ).toBe(false);

    const includeArchived = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials?include_archived=true`,
      headers: { 'x-api-key': apiKey },
    });
    expect(includeArchived.statusCode).toBe(200);
    expect(
      includeArchived
        .json<{ data: CredentialResponse[] }>()
        .data.some((c) => c.id === credential.id),
    ).toBe(true);

    for (const query of ['limit=0', 'include_archived=yes', 'page=not-a-cursor']) {
      const invalid = await app.inject({
        method: 'GET',
        url: `/v1/vaults/${vaultId}/credentials?${query}`,
        headers: { 'x-api-key': apiKey },
      });
      expect(invalid.statusCode).toBe(400);
      const error = invalid.json<ClaudeErrorOut>();
      expect(error.type).toBe('error');
      expect(error.error.type).toBe('invalid_request_error');
      expect(error.error.message).toEqual(expect.any(String));
      expect(error.request_id).toEqual(expect.any(String));
    }
  });

  it('rejects duplicate active mcp_server_url, the twenty-first active credential, and archived vault creation', async () => {
    const vaultId = await createVault('edges');
    await createStaticCredential(vaultId, 'https://dup.example/sse', 'dup');
    const duplicate = await createStaticCredential(vaultId, 'https://dup.example/sse', 'dup2');
    expect(duplicate.statusCode).toBe(409);
    const normalizedDuplicate = await createStaticCredential(
      vaultId,
      'HTTPS://DUP.EXAMPLE:443/sse/',
      'dup3',
    );
    expect(normalizedDuplicate.statusCode).toBe(409);
    expect(normalizedDuplicate.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: {
        type: 'conflict_error',
        message: 'credential already exists for mcp_server_url',
      },
      request_id: expect.any(String),
    });

    const firstRow = (
      await db
        .select()
        .from(vaultCredentials)
        .where(
          and(
            eq(vaultCredentials.vaultId, vaultId),
            eq(vaultCredentials.mcpServerUrl, 'https://dup.example/sse'),
          ),
        )
    )[0]!;
    const now = new Date();
    await db.insert(vaultCredentials).values(
      Array.from({ length: 19 }, (_, i) => {
        const id = newId('vcrd');
        return {
          id,
          workspaceId: firstRow.workspaceId,
          vaultId,
          displayName: `limit-${i}`,
          authType: 'static_bearer',
          mcpServerUrl: `https://limit-${i}.example/sse`,
          accessSecretRef: `local:vault_credentials/${firstRow.workspaceId}/${id}/access_token`,
          metadata: {},
          archivedAt: null,
          createdAt: now,
          updatedAt: now,
        };
      }),
    );
    const twentyFirst = await createStaticCredential(
      vaultId,
      'https://limit-20.example/sse',
      'overflow',
    );
    expect(twentyFirst.statusCode).toBe(409);
    expect(twentyFirst.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: { message: 'credential limit exceeded' },
      request_id: expect.any(String),
    });

    const archivedVaultId = await createVault('archived-parent');
    const archiveVault = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${archivedVaultId}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archiveVault.statusCode).toBe(200);
    const createOnArchived = await createStaticCredential(
      archivedVaultId,
      'https://archived.example/sse',
      'x',
    );
    expect(createOnArchived.statusCode).toBe(404);
  });

  it('serializes concurrent normalized mcp_server_url creation', async () => {
    const vaultId = await createVault('normalized-race');
    const responses = await Promise.all([
      createStaticCredential(vaultId, 'https://race.example/mcp', 'race-1'),
      createStaticCredential(vaultId, 'HTTPS://RACE.EXAMPLE:443/mcp/', 'race-2'),
    ]);

    expect(responses.map((response) => response.statusCode).sort()).toEqual([200, 409]);
    const rows = await db
      .select({ id: vaultCredentials.id })
      .from(vaultCredentials)
      .where(
        and(
          eq(vaultCredentials.workspaceId, workspaceId),
          eq(vaultCredentials.vaultId, vaultId),
          isNull(vaultCredentials.archivedAt),
        ),
      );
    expect(rows).toHaveLength(1);
  });

  it('returns 400 for invalid auth.type or missing required secret fields', async () => {
    const vaultId = await createVault('invalid-body');
    const invalidType = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        auth: { type: 'not_supported', token: 'x', mcp_server_url: 'https://invalid.example/sse' },
      },
    });
    expect(invalidType.statusCode).toBe(400);

    const missingToken = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        auth: { type: 'static_bearer', mcp_server_url: 'https://missing-token.example/sse' },
      },
    });
    expect(missingToken.statusCode).toBe(400);

    const invalidUrl = await createStaticCredential(vaultId, 'not-a-url', 'x');
    expect(invalidUrl.statusCode).toBe(400);
    expect(invalidUrl.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: { message: 'mcp_server_url must be a valid http(s) URL' },
      request_id: expect.any(String),
    });
  });

  it('rejects credential display_name outside 1-255 characters when provided', async () => {
    const vaultId = await createVault('invalid-display-name');
    const emptyCreate = await createStaticCredential(
      vaultId,
      'https://empty-name.example/sse',
      'x',
      {
        display_name: '',
      },
    );
    expect(emptyCreate.statusCode).toBe(400);
    expect(emptyCreate.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: { message: 'display_name must be 1-255 characters' },
      request_id: expect.any(String),
    });

    const longCreate = await createStaticCredential(vaultId, 'https://long-name.example/sse', 'x', {
      display_name: 'x'.repeat(256),
    });
    expect(longCreate.statusCode).toBe(400);

    const credential = (
      await createStaticCredential(vaultId, 'https://valid-name.example/sse', 'x')
    ).json<CredentialResponse>();
    const emptyUpdate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { display_name: '' },
    });
    expect(emptyUpdate.statusCode).toBe(400);
    expect(emptyUpdate.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: { message: 'display_name must be 1-255 characters' },
      request_id: expect.any(String),
    });
  });

  it('purges staged secrets when credential creation fails during multi-secret writes', async () => {
    const vaultId = await createVault('secret-write-failure');
    const failingStore = new FailingRefreshSecretStore();
    const failingApp = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      secretStore: failingStore,
    });
    await failingApp.ready();

    try {
      const res = await failingApp.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          auth: {
            type: 'mcp_oauth',
            access_token: 'access-token',
            mcp_server_url: 'https://secret-write-failure.example/sse',
            refresh: {
              refresh_token: 'refresh-token',
              token_endpoint: 'https://example.com/oauth/token',
              client_id: 'client_123',
              token_endpoint_auth: { type: 'none' },
            },
          },
        },
      });

      expect(res.statusCode).toBe(500);
      const accessRef = failingStore.putRefs.find((ref) => ref.endsWith('/access_token'))!;
      await expect(failingStore.resolve(accessRef)).resolves.toBeNull();

      const rows = await db
        .select()
        .from(vaultCredentials)
        .where(eq(vaultCredentials.mcpServerUrl, 'https://secret-write-failure.example/sse'));
      expect(rows).toHaveLength(0);
    } finally {
      await failingApp.close();
    }
  });

  it('keeps old rotated secrets and purges staged refs when credential update fails', async () => {
    const vaultId = await createVault('rotation-write-failure');
    const failingStore = new FailingRefreshSecretStore();
    failingStore.failRefreshWrites = false;
    const failingApp = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      secretStore: failingStore,
    });
    await failingApp.ready();

    try {
      const create = await failingApp.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          auth: {
            type: 'mcp_oauth',
            access_token: 'old-access-token',
            mcp_server_url: 'https://rotation-write-failure.example/sse',
            refresh: {
              refresh_token: 'old-refresh-token',
              token_endpoint: 'https://example.com/oauth/token',
              client_id: 'client_123',
              token_endpoint_auth: { type: 'none' },
            },
          },
        },
      });
      expect(create.statusCode).toBe(200);
      const credential = create.json<CredentialResponse>();
      const oldRow = (
        await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, credential.id))
      )[0]!;
      failingStore.putRefs.length = 0;
      failingStore.failRefreshWrites = true;

      const update = await failingApp.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {
          auth: {
            type: 'mcp_oauth',
            access_token: 'new-access-token',
            refresh: {
              refresh_token: 'new-refresh-token',
              token_endpoint: 'https://example.com/oauth/token',
              client_id: 'client_123',
            },
          },
        },
      });

      expect(update.statusCode).toBe(500);
      const stagedAccessRef = failingStore.putRefs.find(
        (ref) => ref.includes('/rotations/') && ref.endsWith('/access_token'),
      )!;
      await expect(failingStore.resolve(stagedAccessRef)).resolves.toBeNull();

      const currentRow = (
        await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, credential.id))
      )[0]!;
      expect(currentRow.accessSecretRef).toBe(oldRow.accessSecretRef);
      expect(currentRow.refreshSecretRef).toBe(oldRow.refreshSecretRef);
      await expect(failingStore.resolve(oldRow.accessSecretRef)).resolves.toBe('old-access-token');
      await expect(failingStore.resolve(oldRow.refreshSecretRef!)).resolves.toBe(
        'old-refresh-token',
      );
    } finally {
      await failingApp.close();
    }
  });

  it('prepares session-usable credential metadata and resolves static bearer by credential id', async () => {
    const vaultId = await createVault('internal-runtime');
    const credential = (
      await createStaticCredential(vaultId, 'https://runtime.example/sse', 'runtime-token')
    ).json<CredentialResponse>();
    expect(credential).not.toHaveProperty('workspace_id');
    const credentialWorkspaceId = (
      await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, credential.id))
    )[0]!.workspaceId;
    expect(credentialWorkspaceId).toBe(workspaceId);
    const sessionId = await createSessionFixture(workspaceId, [vaultId]);

    const list = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/executions:prepare`,
      headers: { 'content-type': 'application/json' },
      payload: {},
    });
    expect(list.statusCode).toBe(200);
    expect(list.json()).toMatchObject({
      workspace_id: workspaceId,
      vault_credentials: [
        {
          credential_id: credential.id,
          vault_id: vaultId,
          mcp_server_url: 'https://runtime.example/sse',
          auth_type: 'static_bearer',
        },
      ],
    });
    expect(JSON.stringify(list.json())).not.toContain('runtime-token');

    const wrongWorkspace = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${uniqueWorkspace('vault_prepare_wrong')}/sessions/${sessionId}/executions:prepare`,
      headers: { 'content-type': 'application/json' },
      payload: {},
    });
    expect(wrongWorkspace.statusCode).toBe(404);

    const resolve = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${credential.id}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: {
        credential_id: credential.id,
        vault_id: credential.id,
        force_refresh: true,
      },
    });
    expect(resolve.statusCode).toBe(200);
    expect(resolve.json()).toMatchObject({
      credential_id: credential.id,
      vault_id: vaultId,
      version: '1',
      scheme: 'bearer',
      secret_value: 'runtime-token',
      ttl_seconds: 300,
    });

    const mismatchedBodyId = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${credential.id}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: newId('vcrd'), vault_id: credential.id },
    });
    expect(mismatchedBodyId.statusCode).toBe(404);

    const mismatchedLegacyVaultId = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${credential.id}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: credential.id, vault_id: newId('vcrd') },
    });
    expect(mismatchedLegacyVaultId.statusCode).toBe(404);

    const sessionWithoutVault = await createSessionFixture(workspaceId, []);
    const crossSession = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionWithoutVault}/vault-credentials/${credential.id}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: credential.id, vault_id: credential.id },
    });
    expect(crossSession.statusCode).toBe(404);

    const foreignSession = await createSessionFixture(otherWorkspaceId, [vaultId]);
    const crossWorkspace = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${otherWorkspaceId}/sessions/${foreignSession}/vault-credentials/${credential.id}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: credential.id, vault_id: credential.id },
    });
    expect(crossWorkspace.statusCode).toBe(404);

    const terminatedSession = await createSessionFixture(workspaceId, [vaultId], 'terminated');
    const terminated = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${terminatedSession}/vault-credentials/${credential.id}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: credential.id, vault_id: credential.id },
    });
    expect(terminated.statusCode).toBe(404);
  });

  it('archives credentials when archiving a vault and soft-deletes them when deleting a vault', async () => {
    const archiveVaultId = await createVault('archive-cascade');
    const archiveCredential = (
      await createStaticCredential(archiveVaultId, 'https://cascade-archive.example/sse', 'cascade')
    ).json<CredentialResponse>();
    const archiveRef = (
      await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, archiveCredential.id))
    )[0]!.accessSecretRef;

    const archiveVault = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${archiveVaultId}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archiveVault.statusCode).toBe(200);
    const archivedRows = await db
      .select()
      .from(vaultCredentials)
      .where(eq(vaultCredentials.id, archiveCredential.id));
    expect(archivedRows[0]!.archivedAt).not.toBeNull();
    await expect(secretStore.resolve(archiveRef)).resolves.toBeNull();

    const getArchivedCredential = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${archiveVaultId}/credentials/${archiveCredential.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(getArchivedCredential.statusCode).toBe(200);
    expect(getArchivedCredential.json<CredentialResponse>().archived_at).not.toBeNull();

    const listArchivedCredentials = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${archiveVaultId}/credentials?include_archived=true`,
      headers: { 'x-api-key': apiKey },
    });
    expect(listArchivedCredentials.statusCode).toBe(200);
    expect(
      listArchivedCredentials
        .json<{ data: CredentialResponse[] }>()
        .data.some((credential) => credential.id === archiveCredential.id),
    ).toBe(true);

    const deleteArchivedCredential = await app.inject({
      method: 'DELETE',
      url: `/v1/vaults/${archiveVaultId}/credentials/${archiveCredential.id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(deleteArchivedCredential.statusCode).toBe(200);
    expect(deleteArchivedCredential.json()).toEqual({
      id: archiveCredential.id,
      type: 'vault_credential_deleted',
    });
    const deletedArchivedRows = await db
      .select()
      .from(vaultCredentials)
      .where(eq(vaultCredentials.id, archiveCredential.id));
    expect(deletedArchivedRows).toHaveLength(1);
    expect(deletedArchivedRows[0]!.deletedAt).toBeInstanceOf(Date);

    const deleteVaultId = await createVault('delete-cascade');
    const deleteCredential = (
      await createStaticCredential(deleteVaultId, 'https://cascade-delete.example/sse', 'cascade')
    ).json<CredentialResponse>();
    const deleteRef = (
      await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, deleteCredential.id))
    )[0]!.accessSecretRef;
    const deleteVault = await app.inject({
      method: 'DELETE',
      url: `/v1/vaults/${deleteVaultId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(deleteVault.statusCode).toBe(200);
    expect(deleteVault.json()).toEqual({ id: deleteVaultId, type: 'vault_deleted' });
    const deletedRows = await db
      .select()
      .from(vaultCredentials)
      .where(
        and(
          eq(vaultCredentials.vaultId, deleteVaultId),
          eq(vaultCredentials.id, deleteCredential.id),
        ),
      );
    expect(deletedRows).toHaveLength(1);
    expect(deletedRows[0]!.deletedAt).toBeInstanceOf(Date);
    await expect(secretStore.resolve(deleteRef)).resolves.toBeNull();
  });

  it('creates and rotates provider credentials without exposing secret material', async () => {
    const vaultId = await createVault('provider');
    const create = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'managed-agents-provider-credentials',
      },
      payload: {
        display_name: 'Anthropic production',
        auth: {
          type: 'provider',
          provider: 'anthropic',
          scheme: 'api_key',
          logical_id: 'llm:anthropic',
          secret_value: 'sk-ant-secret-v1',
        },
      },
    });

    expect(create.statusCode).toBe(200);
    const created = create.json<ProviderCredentialResponse>();
    expect(created.auth).toEqual({
      type: 'provider',
      provider: 'anthropic',
      scheme: 'api_key',
      logical_id: 'llm:anthropic',
      version: expect.any(String),
    });
    expect(JSON.stringify(created)).not.toContain('sk-ant-secret-v1');
    expect(JSON.stringify(created)).not.toContain('mcp_server_url');

    const initialRow = (
      await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, created.id))
    )[0]!;
    expect(initialRow).toMatchObject({
      authType: 'provider',
      provider: 'anthropic',
      scheme: 'api_key',
      logicalId: 'llm:anthropic',
      resolutionVersion: created.auth.version,
      mcpServerUrl: null,
      secretName: null,
    });
    expect(JSON.stringify(initialRow)).not.toContain('sk-ant-secret-v1');
    await expect(secretStore.resolve(initialRow.accessSecretRef)).resolves.toBe('sk-ant-secret-v1');

    const rotate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${created.id}`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'managed-agents-provider-credentials',
      },
      payload: { auth: { type: 'provider', secret_value: 'sk-ant-secret-v2' } },
    });
    expect(rotate.statusCode).toBe(200);
    const rotated = rotate.json<ProviderCredentialResponse>();
    expect(rotated.auth.version).not.toBe(created.auth.version);
    expect(JSON.stringify(rotated)).not.toContain('sk-ant-secret-v2');

    const rotatedRow = (
      await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, created.id))
    )[0]!;
    expect(rotatedRow.accessSecretRef).not.toBe(initialRow.accessSecretRef);
    await expect(secretStore.resolve(initialRow.accessSecretRef)).resolves.toBeNull();
    await expect(secretStore.resolve(rotatedRow.accessSecretRef)).resolves.toBe('sk-ant-secret-v2');

    const rebind = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${created.id}`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'managed-agents-provider-credentials',
      },
      payload: { auth: { type: 'provider', logical_id: 'llm:anthropic-next' } },
    });
    expect(rebind.statusCode).toBe(200);
    const rebound = rebind.json<ProviderCredentialResponse>();
    expect(rebound.auth.logical_id).toBe('llm:anthropic-next');
    expect(rebound.auth.version).not.toBe(rotated.auth.version);
    const reboundRow = (
      await db.select().from(vaultCredentials).where(eq(vaultCredentials.id, created.id))
    )[0]!;
    expect(reboundRow.accessSecretRef).toBe(rotatedRow.accessSecretRef);

    const exactResolve = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${await createSessionFixture(workspaceId, [vaultId])}/vault-credentials/${created.id}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: created.id, vault_id: created.id },
    });
    expect(exactResolve.statusCode).toBe(200);
    expect(exactResolve.json()).toEqual({
      credential_id: created.id,
      vault_id: vaultId,
      version: rebound.auth.version,
      scheme: 'api_key',
      secret_value: 'sk-ant-secret-v2',
      ttl_seconds: 300,
    });
  });

  it('rejects unnamespaced, unsafe, incompatible, and duplicate active provider bindings', async () => {
    const vaultId = await createVault('provider-validation');
    const createProvider = (auth: Record<string, unknown>) =>
      app.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials`,
        headers: {
          'x-api-key': apiKey,
          'content-type': 'application/json',
          'orca-beta': 'managed-agents-provider-credentials',
        },
        payload: { auth },
      });

    const incompatible = await createProvider({
      type: 'provider',
      provider: 'vertex',
      scheme: 'api_key',
      logical_id: 'llm:vertex',
      secret_value: '{}',
    });
    expect(incompatible.statusCode).toBe(400);
    expect(incompatible.json<ClaudeErrorOut>().error.message).toContain('not compatible');

    const unsafe = await createProvider({
      type: 'provider',
      provider: 'openai',
      scheme: 'bearer',
      logical_id: 'llm/openai',
      secret_value: 'sk-openai',
    });
    expect(unsafe.statusCode).toBe(400);

    const concreteIdCollision = await createProvider({
      type: 'provider',
      provider: 'openai',
      scheme: 'bearer',
      logical_id: 'vcrd_concrete_id',
      secret_value: 'sk-openai',
    });
    expect(concreteIdCollision.statusCode).toBe(400);
    expect(concreteIdCollision.json<ClaudeErrorOut>().error.message).toContain('^llm:');

    const first = await createProvider({
      type: 'provider',
      provider: 'openai',
      scheme: 'bearer',
      logical_id: 'llm:openai',
      secret_value: 'sk-openai-a',
    });
    expect(first.statusCode).toBe(200);
    const duplicate = await createProvider({
      type: 'provider',
      provider: 'openai',
      scheme: 'bearer',
      logical_id: 'llm:openai',
      secret_value: 'sk-openai-b',
    });
    expect(duplicate.statusCode).toBe(409);
    expect(duplicate.json<ClaudeErrorOut>().error.message).toBe(
      'credential already exists for logical_id',
    );
  });

  it.each([
    ['minimum', `${LOGICAL_CREDENTIAL_ID_PREFIX}a`],
    ['maximum', MAXIMUM_LOGICAL_ID],
  ] as const)('creates and resolves a %s-length logical provider id', async (boundary, alias) => {
    const vaultId = await createVault(`provider-logical-${boundary}`);
    const secretValue = `logical-${boundary}-secret`;
    const credential = await createProviderCredential(
      vaultId,
      'anthropic',
      'api_key',
      alias,
      secretValue,
    );
    const sessionId = await createSessionFixture(workspaceId, [vaultId]);

    expect(credential.auth.logical_id).toBe(alias);
    const resolved = await resolveCredential(sessionId, alias);
    expect(resolved.statusCode).toBe(200);
    expect(resolved.json()).toEqual({
      credential_id: credential.id,
      vault_id: vaultId,
      version: credential.auth.version,
      scheme: 'api_key',
      secret_value: secretValue,
      ttl_seconds: 300,
    });
  });

  it('rejects invalid logical ids at public provider create', async () => {
    const vaultId = await createVault('provider-invalid-logical');
    const secretValue = 'invalid-logical-secret';
    for (const [kind, alias] of INVALID_LOGICAL_IDS) {
      const response = await app.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials`,
        headers: {
          'x-api-key': apiKey,
          'content-type': 'application/json',
          'orca-beta': 'managed-agents-provider-credentials',
        },
        payload: {
          auth: {
            type: 'provider',
            provider: 'anthropic',
            scheme: 'api_key',
            logical_id: alias,
            secret_value: secretValue,
          },
        },
      });

      expect(response.statusCode, kind).toBe(400);
      expect(response.body, kind).not.toContain(secretValue);
    }
  });

  it('rejects an invalid logical-id rebind without changing its alias or version', async () => {
    const vaultId = await createVault('provider-invalid-rebind');
    const alias = `llm:stable-${Date.now()}`;
    const credential = await createProviderCredential(
      vaultId,
      'anthropic',
      'api_key',
      alias,
      'stable-rebind-secret',
    );

    const rebind = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'managed-agents-provider-credentials',
      },
      payload: { auth: { type: 'provider', logical_id: 'llm:invalid/rebind' } },
    });
    expect(rebind.statusCode).toBe(400);

    const [row] = await db
      .select()
      .from(vaultCredentials)
      .where(eq(vaultCredentials.id, credential.id));
    expect(row).toMatchObject({
      logicalId: alias,
      resolutionVersion: credential.auth.version,
    });
  });

  it('rejects invalid internal logical-id resolves without credential disclosure', async () => {
    const vaultId = await createVault('provider-invalid-resolve');
    const alias = `llm:valid-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const secretValue = 'invalid-resolve-secret';
    const credential = await createProviderCredential(
      vaultId,
      'anthropic',
      'api_key',
      alias,
      secretValue,
    );
    const sessionId = await createSessionFixture(workspaceId, [vaultId]);

    for (const [kind, invalidId] of INVALID_LOGICAL_IDS) {
      const pathId = `llm:${encodeURIComponent(invalidId.slice(4))}`;

      const response = await app.inject({
        method: 'POST',
        url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${pathId}/resolve`,
        headers: { 'content-type': 'application/json' },
        payload: { credential_id: invalidId, vault_id: invalidId },
      });

      if (kind === 'overlength') {
        // The router is deliberately capped at the largest valid logical ID,
        // so an overlength segment is rejected before the handler runs.
        expect(response.statusCode, kind).toBe(404);
      } else {
        expect(response.statusCode, kind).toBe(400);
        expect(response.json(), kind).toEqual({ error: 'invalid credential resolve id' });
      }
      expect(response.body, kind).not.toContain(credential.id);
      expect(response.body, kind).not.toContain(secretValue);
    }
  });

  it('keeps provider credential CRUD behind orca-beta without hiding standard credentials', async () => {
    const vaultId = await createVault('provider-beta');
    const standard = (
      await createStaticCredential(vaultId, 'https://provider-beta.example/sse', 'standard-token')
    ).json<CredentialResponse>();
    const providerAuth = {
      type: 'provider',
      provider: 'anthropic',
      scheme: 'api_key',
      logical_id: 'llm:anthropic-beta',
      secret_value: 'sk-ant-beta',
    };

    const rejectedCreate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'anthropic-beta': 'managed-agents-2026-04-01',
      },
      payload: { auth: providerAuth },
    });
    expect(rejectedCreate.statusCode).toBe(400);
    expect(rejectedCreate.json<ClaudeErrorOut>().error.message).toContain('requires orca-beta');

    const optedCreate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'managed-agents-provider-credentials',
      },
      payload: { auth: providerAuth },
    });
    expect(optedCreate.statusCode).toBe(200);
    const provider = optedCreate.json<ProviderCredentialResponse>();

    const defaultList = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials?limit=1`,
      headers: { 'x-api-key': apiKey },
    });
    expect(defaultList.statusCode).toBe(200);
    expect(defaultList.json<{ data: CredentialResponse[]; next_page: string | null }>()).toEqual({
      data: [expect.objectContaining({ id: standard.id })],
      next_page: null,
    });

    const optedList = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: {
        'x-api-key': apiKey,
        'orca-beta': 'managed-agents-provider-credentials',
      },
    });
    expect(optedList.statusCode).toBe(200);
    expect(
      optedList
        .json<{ data: Array<CredentialResponse | ProviderCredentialResponse> }>()
        .data.map((credential) => credential.id),
    ).toEqual(expect.arrayContaining([standard.id, provider.id]));

    const hiddenRequests = await Promise.all([
      app.inject({
        method: 'GET',
        url: `/v1/vaults/${vaultId}/credentials/${provider.id}`,
        headers: { 'x-api-key': apiKey },
      }),
      app.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials/${provider.id}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: { display_name: 'hidden update' },
      }),
      app.inject({
        method: 'POST',
        url: `/v1/vaults/${vaultId}/credentials/${provider.id}/archive`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {},
      }),
      app.inject({
        method: 'DELETE',
        url: `/v1/vaults/${vaultId}/credentials/${provider.id}`,
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload: {},
      }),
    ]);
    expect(hiddenRequests.map((response) => response.statusCode)).toEqual([404, 404, 404, 404]);

    const betaHeaders = {
      'x-api-key': apiKey,
      'content-type': 'application/json',
      'orca-beta': 'managed-agents-provider-credentials',
    };
    const optedGet = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials/${provider.id}`,
      headers: betaHeaders,
    });
    expect(optedGet.statusCode).toBe(200);
    expect(optedGet.json<ProviderCredentialResponse>().auth.type).toBe('provider');

    const optedUpdate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${provider.id}`,
      headers: betaHeaders,
      payload: { display_name: 'visible update' },
    });
    expect(optedUpdate.statusCode).toBe(200);
    expect(optedUpdate.json<ProviderCredentialResponse>().display_name).toBe('visible update');

    const optedArchive = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${provider.id}/archive`,
      headers: betaHeaders,
      payload: {},
    });
    expect(optedArchive.statusCode).toBe(200);
    expect(optedArchive.json<ProviderCredentialResponse>().archived_at).not.toBeNull();

    const optedDelete = await app.inject({
      method: 'DELETE',
      url: `/v1/vaults/${vaultId}/credentials/${provider.id}`,
      headers: betaHeaders,
      payload: {},
    });
    expect(optedDelete.statusCode).toBe(200);
  });

  it('resolves a logical provider credential to its concrete binding and audits no secrets', async () => {
    const vaultId = await createVault('provider-resolve');
    const alias = `llm:resolve-${Date.now()}`;
    const credential = await createProviderCredential(
      vaultId,
      'anthropic',
      'api_key',
      alias,
      'sk-ant-logical-secret',
    );
    const sessionId = await createSessionFixture(workspaceId, [vaultId]);

    const logical = await resolveCredential(sessionId, alias);
    expect(logical.statusCode).toBe(200);
    expect(logical.json()).toEqual({
      credential_id: credential.id,
      vault_id: vaultId,
      version: credential.auth.version,
      scheme: 'api_key',
      secret_value: 'sk-ant-logical-secret',
      ttl_seconds: 300,
    });

    const exact = await resolveCredential(sessionId, credential.id);
    expect(exact.statusCode).toBe(200);
    expect(exact.json()).toEqual(logical.json());

    const audit = (
      await db
        .select()
        .from(adminAuditEvents)
        .where(
          and(
            eq(adminAuditEvents.workspaceId, workspaceId),
            eq(adminAuditEvents.action, 'vault_credential.logical_resolution'),
            eq(adminAuditEvents.targetId, alias),
          ),
        )
    ).filter(
      (event) =>
        (event.metadata as Record<string, unknown>)['session_id'] === sessionId &&
        event.result === 'success',
    );
    expect(audit).toHaveLength(1);
    expect(audit[0]).toMatchObject({
      workspaceId,
      actor: 'combined-test-internal',
      authMethod: 'internal-service',
      action: 'vault_credential.logical_resolution',
      targetType: 'vault_credential_alias',
      targetId: alias,
      result: 'success',
      metadata: {
        session_id: sessionId,
        requested_credential_id: alias,
        selected_credential_id: credential.id,
        credential_version: credential.auth.version,
      },
    });
    expect(JSON.stringify(audit[0])).not.toContain('sk-ant-logical-secret');
    expect(JSON.stringify(audit[0])).not.toContain('access_secret_ref');
  });

  it('scopes logical lookup to session vaults and fails closed on ambiguous bindings', async () => {
    const vaultA = await createVault('provider-scope-a');
    const vaultB = await createVault('provider-scope-b');
    const alias = `llm:scoped-${Date.now()}`;
    const credentialA = await createProviderCredential(
      vaultA,
      'openai',
      'bearer',
      alias,
      'openai-secret-a',
    );
    const credentialB = await createProviderCredential(
      vaultB,
      'openai',
      'bearer',
      alias,
      'openai-secret-b',
    );
    const sessionA = await createSessionFixture(workspaceId, [vaultA]);
    const sessionB = await createSessionFixture(workspaceId, [vaultB]);
    const ambiguousSession = await createSessionFixture(workspaceId, [vaultA, vaultB]);
    const unboundSession = await createSessionFixture(workspaceId, []);

    const resolvedA = await resolveCredential(sessionA, alias);
    expect(resolvedA.statusCode).toBe(200);
    expect(resolvedA.json()).toEqual({
      credential_id: credentialA.id,
      vault_id: vaultA,
      version: credentialA.auth.version,
      scheme: 'bearer',
      secret_value: 'openai-secret-a',
      ttl_seconds: 300,
    });
    const resolvedB = await resolveCredential(sessionB, alias);
    expect(resolvedB.statusCode).toBe(200);
    expect(resolvedB.json()).toEqual({
      credential_id: credentialB.id,
      vault_id: vaultB,
      version: credentialB.auth.version,
      scheme: 'bearer',
      secret_value: 'openai-secret-b',
      ttl_seconds: 300,
    });

    const ambiguous = await resolveCredential(ambiguousSession, alias);
    const unbound = await resolveCredential(unboundSession, alias);
    const missing = await resolveCredential(sessionA, `llm:missing-${Date.now()}`);
    const terminatedSession = await createSessionFixture(workspaceId, [vaultA], 'terminated');
    const terminated = await resolveCredential(terminatedSession, alias);
    const foreignSession = await createSessionFixture(otherWorkspaceId, [vaultA]);
    const crossWorkspace = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${otherWorkspaceId}/sessions/${foreignSession}/vault-credentials/${alias}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: alias, vault_id: alias },
    });
    for (const denied of [ambiguous, unbound, missing, terminated, crossWorkspace]) {
      expect(denied.statusCode).toBe(404);
      expect(denied.json()).toEqual({ error: 'not found' });
      expect(denied.body).not.toContain(credentialA.id);
      expect(denied.body).not.toContain(credentialB.id);
      expect(denied.body).not.toContain('openai-secret-a');
      expect(denied.body).not.toContain('openai-secret-b');
    }

    const mismatchedBody = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionA}/vault-credentials/${alias}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: 'llm:different', vault_id: alias },
    });
    expect(mismatchedBody.statusCode).toBe(404);
    const mismatchedLegacyBody = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionA}/vault-credentials/${alias}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: alias, vault_id: 'llm:different' },
    });
    expect(mismatchedLegacyBody.statusCode).toBe(404);
    expect(mismatchedBody.json()).toEqual({ error: 'not found' });
    expect(mismatchedLegacyBody.json()).toEqual(mismatchedBody.json());

    const malformed = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionA}/vault-credentials/llm:bad%25id/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: 'llm:bad%id', vault_id: 'llm:bad%id' },
    });
    expect(malformed.statusCode).toBe(400);

    const encodedCanonicalAlias = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionA}/vault-credentials/${alias.replace(':', '%3A')}/resolve`,
      payload: { credential_id: alias, vault_id: alias },
    });
    expect(encodedCanonicalAlias.statusCode).toBe(400);

    const deniedAudit = (
      await db
        .select()
        .from(adminAuditEvents)
        .where(
          and(
            eq(adminAuditEvents.workspaceId, workspaceId),
            eq(adminAuditEvents.action, 'vault_credential.logical_resolution'),
            eq(adminAuditEvents.targetId, alias),
          ),
        )
    ).find(
      (event) =>
        (event.metadata as Record<string, unknown>)['session_id'] === ambiguousSession &&
        event.result === 'denied',
    );
    expect(deniedAudit?.metadata).toEqual({
      session_id: ambiguousSession,
      requested_credential_id: alias,
      reason: 'ambiguous',
    });
    expect(JSON.stringify(deniedAudit)).not.toContain(credentialA.id);
    expect(JSON.stringify(deniedAudit)).not.toContain(credentialB.id);

    const archived = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultA}/credentials/${credentialA.id}/archive`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'managed-agents-provider-credentials',
      },
      payload: {},
    });
    expect(archived.statusCode).toBe(200);
    const archivedResolve = await resolveCredential(sessionA, alias);
    expect(archivedResolve.statusCode).toBe(404);
    expect(archivedResolve.json()).toEqual({ error: 'not found' });
    expect(archivedResolve.body).not.toContain(credentialA.id);
    expect(archivedResolve.body).not.toContain('openai-secret-a');
  });

  it.each([
    {
      provider: 'azure_openai',
      scheme: 'api_key',
      variant: 'api-key',
      secretValue: 'azure-api-key',
    },
    {
      provider: 'azure_openai',
      scheme: 'bearer',
      variant: 'Entra',
      secretValue: 'azure-entra-token',
    },
    {
      provider: 'vertex',
      scheme: 'gcp-service-account',
      variant: 'service-account',
      secretValue: '{"client_email":"vertex@example.com"}',
    },
    {
      provider: 'bedrock',
      scheme: 'aws-sig-v4',
      variant: 'long-lived',
      secretValue: 'access-key:secret-key',
    },
    {
      provider: 'bedrock',
      scheme: 'aws-sig-v4',
      variant: 'session',
      secretValue: 'access-key:secret-key:session-token',
    },
  ] as const)(
    'returns canonical $provider provider material with scheme $scheme ($variant)',
    async ({ provider, scheme, secretValue }) => {
      const vaultId = await createVault(`provider-${provider}-${scheme}`);
      const alias = `llm:${provider}-${scheme}-${Date.now()}`;
      const credential = await createProviderCredential(
        vaultId,
        provider,
        scheme,
        alias,
        secretValue,
      );
      const sessionId = await createSessionFixture(workspaceId, [vaultId]);

      const response = await resolveCredential(sessionId, alias);
      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        credential_id: credential.id,
        vault_id: vaultId,
        version: credential.auth.version,
        scheme,
        secret_value: secretValue,
        ttl_seconds: 300,
      });
    },
  );

  it('returns a new opaque version and secret after logical credential rotation', async () => {
    const vaultId = await createVault('provider-rotation-resolution');
    const alias = `llm:rotate-${Date.now()}`;
    const credential = await createProviderCredential(
      vaultId,
      'anthropic',
      'api_key',
      alias,
      'rotation-secret-v1',
    );
    const sessionId = await createSessionFixture(workspaceId, [vaultId]);

    const before = await resolveCredential(sessionId, alias);
    expect(before.statusCode).toBe(200);
    expect(before.json()).toEqual({
      credential_id: credential.id,
      vault_id: vaultId,
      version: credential.auth.version,
      scheme: 'api_key',
      secret_value: 'rotation-secret-v1',
      ttl_seconds: 300,
    });

    const rotate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'managed-agents-provider-credentials',
      },
      payload: { auth: { type: 'provider', secret_value: 'rotation-secret-v2' } },
    });
    expect(rotate.statusCode).toBe(200);
    const rotated = rotate.json<ProviderCredentialResponse>();
    expect(rotated.auth.version).not.toBe(credential.auth.version);
    expect(rotate.body).not.toContain('rotation-secret-v1');
    expect(rotate.body).not.toContain('rotation-secret-v2');

    const after = await resolveCredential(sessionId, alias);
    expect(after.statusCode).toBe(200);
    expect(after.json()).toEqual({
      credential_id: credential.id,
      vault_id: vaultId,
      version: rotated.auth.version,
      scheme: 'api_key',
      secret_value: 'rotation-secret-v2',
      ttl_seconds: 300,
    });

    const auditRows = await db
      .select()
      .from(adminAuditEvents)
      .where(
        and(
          eq(adminAuditEvents.workspaceId, workspaceId),
          eq(adminAuditEvents.action, 'vault_credential.logical_resolution'),
          eq(adminAuditEvents.targetId, alias),
        ),
      );
    expect(auditRows.map((row) => row.metadata)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ credential_version: credential.auth.version }),
        expect.objectContaining({ credential_version: rotated.auth.version }),
      ]),
    );
    expect(JSON.stringify(auditRows)).not.toContain('rotation-secret-v1');
    expect(JSON.stringify(auditRows)).not.toContain('rotation-secret-v2');
    expect(JSON.stringify(auditRows)).not.toContain('access_secret_ref');
  });

  it('returns a new concrete id and version when a logical alias is rebound', async () => {
    const vaultId = await createVault('provider-rebind-resolution');
    const alias = `llm:rebind-${Date.now()}`;
    const first = await createProviderCredential(
      vaultId,
      'anthropic',
      'api_key',
      alias,
      'first-secret',
    );
    const second = await createProviderCredential(
      vaultId,
      'anthropic',
      'api_key',
      `${alias}-next`,
      'second-secret',
    );
    const sessionId = await createSessionFixture(workspaceId, [vaultId]);

    const before = await resolveCredential(sessionId, alias);
    expect(before.statusCode).toBe(200);
    expect(before.json()).toMatchObject({
      credential_id: first.id,
      version: first.auth.version,
      secret_value: 'first-secret',
    });

    const betaHeaders = {
      'x-api-key': apiKey,
      'content-type': 'application/json',
      'orca-beta': 'managed-agents-provider-credentials',
    };
    const releaseAlias = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${first.id}`,
      headers: betaHeaders,
      payload: { auth: { type: 'provider', logical_id: `${alias}-retired` } },
    });
    expect(releaseAlias.statusCode).toBe(200);
    const bindAlias = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${second.id}`,
      headers: betaHeaders,
      payload: { auth: { type: 'provider', logical_id: alias } },
    });
    expect(bindAlias.statusCode).toBe(200);
    const rebound = bindAlias.json<ProviderCredentialResponse>();
    expect(rebound.auth.version).not.toBe(second.auth.version);

    const after = await resolveCredential(sessionId, alias);
    expect(after.statusCode).toBe(200);
    expect(after.json()).toEqual({
      credential_id: second.id,
      vault_id: vaultId,
      version: rebound.auth.version,
      scheme: 'api_key',
      secret_value: 'second-secret',
      ttl_seconds: 300,
    });
  });

  it('creates environment_variable credentials with limited/unrestricted networking and hides secret_value', async () => {
    const vaultId = await createVault('env-var');
    const limited = await createEnvVarCredential(vaultId, 'OPENAI_API_KEY', 'sk-env-secret', {
      type: 'limited',
      allowed_hosts: ['api.openai.com'],
    });
    expect(limited.statusCode).toBe(200);
    const limitedCred = limited.json<EnvVarCredentialResponse>();
    expect(limitedCred.id).toMatch(/^vcrd_/);
    expect(limitedCred.auth).toEqual({
      type: 'environment_variable',
      secret_name: 'OPENAI_API_KEY',
      networking: { type: 'limited', allowed_hosts: ['api.openai.com'] },
      injection_location: { header: true, body: false },
    });
    expect(JSON.stringify(limitedCred)).not.toContain('sk-env-secret');
    expect(JSON.stringify(limitedCred)).not.toContain('mcp_server_url');

    const rows = await db
      .select()
      .from(vaultCredentials)
      .where(eq(vaultCredentials.id, limitedCred.id));
    expect(rows[0]!.mcpServerUrl).toBeNull();
    expect(rows[0]!.secretName).toBe('OPENAI_API_KEY');
    await expect(secretStore.resolve(rows[0]!.accessSecretRef)).resolves.toBe('sk-env-secret');

    const unrestricted = await createEnvVarCredential(vaultId, 'ANTHROPIC_API_KEY', 'sk-other', {
      type: 'unrestricted',
    });
    expect(unrestricted.statusCode).toBe(200);
    expect(unrestricted.json<EnvVarCredentialResponse>().auth).toEqual({
      type: 'environment_variable',
      secret_name: 'ANTHROPIC_API_KEY',
      networking: { type: 'unrestricted' },
      injection_location: { header: true, body: false },
    });
  });

  it('rejects environment_variable credentials with no enabled injection location', async () => {
    const vaultId = await createVault('env-var-injection');
    const rejectedCreate = await createEnvVarCredential(
      vaultId,
      'NO_INJECTION',
      'secret',
      { type: 'unrestricted' },
      { injection_location: { header: false, body: false } },
    );
    expect(rejectedCreate.statusCode).toBe(400);
    expect(rejectedCreate.json<ClaudeErrorOut>().error).toEqual({
      type: 'invalid_request_error',
      message: 'auth.injection_location must enable header or body',
    });

    const created = await createEnvVarCredential(
      vaultId,
      'HEADER_ONLY',
      'secret',
      { type: 'unrestricted' },
      { injection_location: { header: true, body: false } },
    );
    expect(created.statusCode).toBe(200);

    const rejectedUpdate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${created.json<EnvVarCredentialResponse>().id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        auth: {
          type: 'environment_variable',
          injection_location: { header: false },
        },
      },
    });
    expect(rejectedUpdate.statusCode).toBe(400);
    expect(rejectedUpdate.json<ClaudeErrorOut>().error.message).toBe(
      'auth.injection_location must enable header or body',
    );

    const get = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials/${created.json<EnvVarCredentialResponse>().id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json<EnvVarCredentialResponse>().auth.injection_location).toEqual({
      header: true,
      body: false,
    });
  });

  it('rejects duplicate active secret_name but allows reuse after archiving', async () => {
    const vaultId = await createVault('env-var-dup');
    const first = await createEnvVarCredential(vaultId, 'SHARED_NAME', 'v1', {
      type: 'unrestricted',
    });
    expect(first.statusCode).toBe(200);
    const dup = await createEnvVarCredential(vaultId, 'SHARED_NAME', 'v2', {
      type: 'unrestricted',
    });
    expect(dup.statusCode).toBe(409);
    expect(dup.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: { message: 'credential already exists for secret_name' },
      request_id: expect.any(String),
    });

    const archive = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials/${first.json<EnvVarCredentialResponse>().id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archive.statusCode).toBe(200);
    const reuse = await createEnvVarCredential(vaultId, 'SHARED_NAME', 'v3', {
      type: 'unrestricted',
    });
    expect(reuse.statusCode).toBe(200);
  });

  it('accepts empty allowed_hosts and rejects env-var credentials missing secret_value', async () => {
    const vaultId = await createVault('env-var-invalid');
    const emptyHosts = await createEnvVarCredential(vaultId, 'NAME_A', 'v', {
      type: 'limited',
      allowed_hosts: [],
    });
    expect(emptyHosts.statusCode).toBe(200);
    expect(emptyHosts.json<EnvVarCredentialResponse>().auth).toMatchObject({
      networking: { type: 'limited', allowed_hosts: [] },
      injection_location: { header: true, body: false },
    });

    const missingValue = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        auth: {
          type: 'environment_variable',
          secret_name: 'NAME_B',
          networking: { type: 'unrestricted' },
        },
      },
    });
    expect(missingValue.statusCode).toBe(400);
  });

  it('counts env-var credentials toward the twenty-per-vault cap', async () => {
    const vaultId = await createVault('env-var-limit');
    const created = await createEnvVarCredential(vaultId, 'CAP_PROBE', 'v', {
      type: 'unrestricted',
    });
    expect(created.statusCode).toBe(200);
    const firstRow = (
      await db
        .select()
        .from(vaultCredentials)
        .where(eq(vaultCredentials.id, created.json<EnvVarCredentialResponse>().id))
    )[0]!;
    const now = new Date();
    await db.insert(vaultCredentials).values(
      Array.from({ length: 19 }, (_, i) => {
        const id = newId('vcrd');
        return {
          id,
          workspaceId: firstRow.workspaceId,
          vaultId,
          displayName: `cap-${i}`,
          authType: 'static_bearer',
          mcpServerUrl: `https://cap-${i}.example/sse`,
          accessSecretRef: `local:vault_credentials/${firstRow.workspaceId}/${id}/access_token`,
          metadata: {},
          archivedAt: null,
          createdAt: now,
          updatedAt: now,
        };
      }),
    );
    const overflow = await createEnvVarCredential(vaultId, 'OVERFLOW', 'v', {
      type: 'unrestricted',
    });
    expect(overflow.statusCode).toBe(409);
    expect(overflow.json<ClaudeErrorOut>()).toMatchObject({
      type: 'error',
      error: { message: 'credential limit exceeded' },
      request_id: expect.any(String),
    });
  });

  it('resolves env-var credentials with secret_value over the internal route only', async () => {
    const vaultId = await createVault('env-var-resolve');
    const created = await createEnvVarCredential(vaultId, 'RESOLVE_KEY', 'resolve-secret', {
      type: 'limited',
      allowed_hosts: ['api.example.com'],
    });
    expect(created.statusCode).toBe(200);
    const credential = created.json<EnvVarCredentialResponse>();

    const publicGet = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${vaultId}/credentials/${credential.id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(publicGet.statusCode).toBe(200);
    expect(JSON.stringify(publicGet.json())).not.toContain('resolve-secret');

    const resolve = await app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${await createSessionFixture(workspaceId, [vaultId])}/vault-credentials/${credential.id}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: credential.id, vault_id: credential.id },
    });
    expect(resolve.statusCode).toBe(200);
    expect(resolve.json()).toMatchObject({
      credential_id: credential.id,
      vault_id: vaultId,
      auth_type: 'environment_variable',
      secret_name: 'RESOLVE_KEY',
      secret_value: 'resolve-secret',
      networking: { type: 'limited', allowed_hosts: ['api.example.com'] },
      ttl_seconds: 300,
    });
  });

  async function createEnvVarCredential(
    vaultId: string,
    secretName: string,
    secretValue: string,
    networking: { type: 'limited'; allowed_hosts: string[] } | { type: 'unrestricted' },
    extraAuth: Record<string, unknown> = {},
  ) {
    return app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        auth: {
          type: 'environment_variable',
          secret_name: secretName,
          secret_value: secretValue,
          networking,
          ...extraAuth,
        },
      },
    });
  }

  async function createProviderCredential(
    vaultId: string,
    provider: ProviderCredentialResponse['auth']['provider'],
    scheme: ProviderCredentialResponse['auth']['scheme'],
    logicalId: string,
    secretValue: string,
  ): Promise<ProviderCredentialResponse> {
    const response = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'managed-agents-provider-credentials',
      },
      payload: {
        auth: {
          type: 'provider',
          provider,
          scheme,
          logical_id: logicalId,
          secret_value: secretValue,
        },
      },
    });
    expect(response.statusCode).toBe(200);
    return response.json<ProviderCredentialResponse>();
  }

  function resolveCredential(sessionId: string, requestedCredentialId: string) {
    return app.inject({
      method: 'POST',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/vault-credentials/${requestedCredentialId}/resolve`,
      headers: { 'content-type': 'application/json' },
      payload: {
        credential_id: requestedCredentialId,
        vault_id: requestedCredentialId,
      },
    });
  }

  async function createVault(label: string): Promise<string> {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/vaults',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        display_name: `vc-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      },
    });
    expect(res.statusCode).toBe(200);
    return res.json<{ id: string }>().id;
  }

  async function createSessionFixture(
    ownerWorkspaceId: string,
    vaultIds: string[],
    status: 'idle' | 'terminated' = 'idle',
  ): Promise<string> {
    const agentId = newId('agt');
    const agentVersionId = newId('agtv');
    const sessionId = newId('ses');
    const now = new Date();
    await db.insert(agents).values({
      id: agentId,
      workspaceId: ownerWorkspaceId,
      name: `agent-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      version: 1,
      latestVersionId: null,
      modelProvider: 'anthropic',
      modelId: 'claude-sonnet-4-6',
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(agentVersions).values({
      id: agentVersionId,
      workspaceId: ownerWorkspaceId,
      agentId,
      version: 1,
      snapshot: {
        id: agentId,
        name: 'vault credential fixture',
        version: 1,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
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
      .where(and(eq(agents.workspaceId, ownerWorkspaceId), eq(agents.id, agentId)));
    await db.insert(sessions).values({
      id: sessionId,
      workspaceId: ownerWorkspaceId,
      agentId,
      agentVersion: 1,
      status,
      vaultIds,
      createdAt: now,
      updatedAt: now,
    });
    return sessionId;
  }

  async function createStaticCredential(
    vaultId: string,
    mcpServerUrl: string,
    token: string,
    extra: Record<string, unknown> = {},
  ) {
    return app.inject({
      method: 'POST',
      url: `/v1/vaults/${vaultId}/credentials`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        ...extra,
        auth: { type: 'static_bearer', token, mcp_server_url: mcpServerUrl },
      },
    });
  }
});

class FailingRefreshSecretStore extends LocalSecretStore implements SecretStore {
  readonly putRefs: string[] = [];
  failRefreshWrites = true;

  override async put(
    reference: string,
    value: string,
    options?: SecretStorePutOptions,
  ): Promise<void> {
    this.putRefs.push(reference);
    if (this.failRefreshWrites && reference.endsWith('/refresh_token')) {
      throw new Error('injected refresh write failure');
    }
    await super.put(reference, value, options);
  }
}
