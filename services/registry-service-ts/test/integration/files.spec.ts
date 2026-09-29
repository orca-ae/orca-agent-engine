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
  buildTestFileStore,
  closeTestFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

describe('/v1/files (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;

  beforeAll(async () => {
    const { db } = await getTestDb();
    const fileStore = await buildTestFileStore();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    apiKey = await createTestApiKey(db, uniqueWorkspace('files'));
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestFileStore();
    await closeTestDb();
  });

  it('POST /v1/files multipart -> Claude File', async () => {
    const payload = Buffer.from('hello phase 5');
    const form = new FormData();
    form.append('file', new Blob([payload], { type: 'text/plain' }), 'hello.txt');
    const res = await fetch(`${baseURL}/v1/files`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey },
      body: form,
    });
    expect(res.status).toBe(200);
    const file = (await res.json()) as {
      id: string;
      type: string;
      size_bytes: number;
      downloadable: boolean;
      scope: null;
    };
    expect(file.id).toMatch(/^file_/);
    expect(file.type).toBe('file');
    expect(file.size_bytes).toBe(payload.length);
    expect(file.downloadable).toBe(false);
    expect(file.scope).toBeNull();
  }, 30000);

  it.each(['workspace_id', 'workspaceId'])(
    'ignores multipart workspace selector field %s',
    async (fieldName) => {
      const form = new FormData();
      form.append(fieldName, uniqueWorkspace('forged_file_upload'));
      form.append('file', new Blob([Buffer.from(`forged-${Date.now()}`)]), 'forged.txt');

      const res = await fetch(`${baseURL}/v1/files`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey },
        body: form,
      });

      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toMatchObject({
        type: 'file',
        downloadable: false,
        scope: null,
      });
    },
    30000,
  );

  it('ignores non-contract multipart metadata fields', async () => {
    const form = new FormData();
    form.append('meta_workspace_id', 'opaque-user-value');
    form.append('file', new Blob([Buffer.from(`metadata-${Date.now()}`)]), 'metadata.txt');

    const res = await fetch(`${baseURL}/v1/files`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey },
      body: form,
    });

    expect(res.status).toBe(200);
    const file = (await res.json()) as Record<string, unknown>;
    expect(file).not.toHaveProperty('metadata');
  }, 30000);

  it('GET /v1/files/:id returns the record', async () => {
    const payload = Buffer.from('round trip');
    const form = new FormData();
    form.append('file', new Blob([payload], { type: 'text/plain' }), 't.txt');
    const post = await fetch(`${baseURL}/v1/files`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey },
      body: form,
    });
    const created = (await post.json()) as { id: string };
    const get = await fetch(`${baseURL}/v1/files/${created.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(get.status).toBe(200);
    const fetched = (await get.json()) as { id: string };
    expect(fetched.id).toBe(created.id);
  }, 30000);

  it('GET /v1/files/:id/content rejects public uploads even with legacy selectors', async () => {
    const payload = Buffer.from('content stream');
    const form = new FormData();
    form.append('purpose', 'agent_output');
    form.append('scope_id', 'ses_filescontent_legacy');
    form.append('file', new Blob([payload], { type: 'text/plain' }), 'c.txt');
    const post = await fetch(`${baseURL}/v1/files`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey },
      body: form,
    });
    const created = (await post.json()) as { id: string };
    const get = await fetch(`${baseURL}/v1/files/${created.id}/content`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(get.status).toBe(403);
    expect(await get.json()).toMatchObject({
      type: 'error',
      error: { type: 'permission_error', message: expect.stringMatching(/not downloadable/i) },
      request_id: expect.any(String),
    });
  }, 30000);

  it('SHA-256 dedup: same content twice -> same id', async () => {
    const payload = Buffer.from(`dedup-${Date.now()}`);
    const post1 = await fetch(`${baseURL}/v1/files`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey },
      body: (() => {
        const f = new FormData();
        f.append('file', new Blob([payload]), 'a.txt');
        return f;
      })(),
    });
    const a = (await post1.json()) as { id: string };
    const post2 = await fetch(`${baseURL}/v1/files`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey },
      body: (() => {
        const f = new FormData();
        f.append('file', new Blob([payload]), 'b.txt');
        return f;
      })(),
    });
    const b = (await post2.json()) as { id: string };
    expect(b.id).toBe(a.id);
  }, 30000);
});
