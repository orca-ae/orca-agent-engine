// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
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

interface VaultResponse {
  id: string;
  type: 'vault';
  display_name: string;
  metadata: Record<string, unknown>;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

interface ClaudeErrorOut {
  type: 'error';
  error: { type: string; message: string };
  request_id: string | null;
}

describe('Vaults CRUD (integration)', () => {
  let app: FastifyInstance;
  let apiKey: string;

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
    await app.ready();
    apiKey = await createTestApiKey(db, uniqueWorkspace('vaults'));
  });
  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  it('POST /v1/vaults creates Claude-shaped vault without secret material', async () => {
    const res = await createVault('v-create', { owner: 'platform' });
    expect(res.statusCode).toBe(200);
    const vault = res.json<VaultResponse>();
    expect(vault).toMatchObject({
      type: 'vault',
      display_name: 'v-create',
      metadata: { owner: 'platform' },
    });
    expect(vault.id).toMatch(/^vlt_/);
    expect(JSON.stringify(vault)).not.toContain('secret_ref');
    expect(JSON.stringify(vault)).not.toContain('target_url');
  });

  it('rejects legacy flat fields on create and update', async () => {
    const badCreate = await app.inject({
      method: 'POST',
      url: '/v1/vaults',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        display_name: 'legacy',
        target_kind: 'bearer',
        target_url: 'https://api.example.com',
        secret_ref: 'env:X',
      },
    });
    expect(badCreate.statusCode).toBe(400);

    const id = (await createVault('strict-update')).json<VaultResponse>().id;
    const badUpdate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { name: 'legacy-name' },
    });
    expect(badUpdate.statusCode).toBe(400);
  });

  it('rejects invalid display_name at route boundary with consistent error', async () => {
    const expectedError = 'display_name must be a non-empty string up to 255 characters';
    for (const payload of [
      { metadata: {} },
      { display_name: '' },
      { display_name: '   ' },
      { display_name: 'x'.repeat(256) },
    ]) {
      const badCreate = await app.inject({
        method: 'POST',
        url: '/v1/vaults',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        payload,
      });
      expect(badCreate.statusCode).toBe(400);
      const error = badCreate.json<ClaudeErrorOut>();
      expect(error.type).toBe('error');
      expect(error.error).toEqual({ type: 'invalid_request_error', message: expectedError });
      expect(error.request_id).toEqual(expect.any(String));
    }

    const id = (await createVault('strict-blank-update')).json<VaultResponse>().id;
    const nullUpdate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { display_name: null },
    });
    expect(nullUpdate.statusCode).toBe(200);
    expect(nullUpdate.json<VaultResponse>().display_name).toBe('strict-blank-update');

    const whitespaceUpdate = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { display_name: '   ' },
    });
    expect(whitespaceUpdate.statusCode).toBe(200);
    expect(whitespaceUpdate.json<VaultResponse>().display_name).toBe('   ');
  });

  it('GET/update use Claude shape', async () => {
    const id = (await createVault('v-get')).json<VaultResponse>().id;
    const get = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${id}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(get.statusCode).toBe(200);
    expect(get.json<VaultResponse>().display_name).toBe('v-get');

    const update = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { display_name: 'v-updated', metadata: { keep: 'yes', changed: 'true' } },
    });
    expect(update.statusCode).toBe(200);
    expect(update.json<VaultResponse>()).toMatchObject({
      display_name: 'v-updated',
      metadata: { keep: 'yes', changed: 'true' },
    });

    const patch = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${id}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { metadata: { keep: null, added: 'value' } },
    });
    expect(patch.statusCode).toBe(200);
    expect(patch.json<VaultResponse>().metadata).toEqual({ changed: 'true', added: 'value' });
  });

  it('list/archive use Claude shape', async () => {
    const id = (await createVault('v-list-archive')).json<VaultResponse>().id;

    const list = await app.inject({
      method: 'GET',
      url: '/v1/vaults',
      headers: { 'x-api-key': apiKey },
    });
    expect(list.statusCode).toBe(200);
    expect(list.json<{ data: VaultResponse[] }>().data.some((v) => v.id === id)).toBe(true);

    const arch = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${id}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(arch.statusCode).toBe(200);
    expect(arch.json<VaultResponse>().archived_at).toBeTruthy();
  });

  it('GET /v1/vaults honors include_archived and rejects invalid pagination filters', async () => {
    await createVault('v-active-filter');
    const archivedId = (await createVault('v-archived-filter')).json<VaultResponse>().id;

    const archive = await app.inject({
      method: 'POST',
      url: `/v1/vaults/${archivedId}/archive`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(archive.statusCode).toBe(200);

    const defaultList = await app.inject({
      method: 'GET',
      url: '/v1/vaults?limit=100',
      headers: { 'x-api-key': apiKey },
    });
    expect(defaultList.statusCode).toBe(200);
    const defaultIds = defaultList
      .json<{ data: VaultResponse[]; next_page: string | null }>()
      .data.map((v) => v.id);
    expect(defaultIds).not.toContain(archivedId);

    const withArchived = await app.inject({
      method: 'GET',
      url: '/v1/vaults?include_archived=true',
      headers: { 'x-api-key': apiKey },
    });
    expect(withArchived.statusCode).toBe(200);
    const archivedIds = withArchived
      .json<{ data: VaultResponse[]; next_page: string | null }>()
      .data.map((v) => v.id);
    expect(archivedIds).toContain(archivedId);

    const emptyPage = await app.inject({
      method: 'GET',
      url: '/v1/vaults?page=',
      headers: { 'x-api-key': apiKey },
    });
    expect(emptyPage.statusCode).toBe(200);

    for (const query of ['limit=0', 'include_archived=yes', 'page=not-a-cursor']) {
      const invalid = await app.inject({
        method: 'GET',
        url: `/v1/vaults?${query}`,
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

  it('DELETE returns Claude-shaped tombstone and removes the vault', async () => {
    const deleteId = (await createVault('v-delete')).json<VaultResponse>().id;
    const del = await app.inject({
      method: 'DELETE',
      url: `/v1/vaults/${deleteId}`,
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {},
    });
    expect(del.statusCode).toBe(200);
    expect(del.json()).toEqual({ id: deleteId, type: 'vault_deleted' });

    const getDeleted = await app.inject({
      method: 'GET',
      url: `/v1/vaults/${deleteId}`,
      headers: { 'x-api-key': apiKey },
    });
    expect(getDeleted.statusCode).toBe(404);
  });

  it('unknown vault credential does not resolve', async () => {
    const resolve = await app.inject({
      method: 'POST',
      url: '/internal/v1/workspaces/ws_unknown/sessions/ses_unknown/vault-credentials/vcrd_unknown_id/resolve',
      headers: { 'content-type': 'application/json' },
      payload: { credential_id: 'vcrd_unknown_id', vault_id: 'vcrd_unknown_id' },
    });
    expect(resolve.statusCode).toBe(404);
  });

  function createVault(displayName: string, metadata?: Record<string, unknown>) {
    return app.inject({
      method: 'POST',
      url: '/v1/vaults',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: { display_name: displayName, ...(metadata ? { metadata } : {}) },
    });
  }
});
