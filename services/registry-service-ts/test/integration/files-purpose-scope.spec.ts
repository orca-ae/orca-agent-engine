// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { Readable } from 'node:stream';
import {
  buildStubStore,
  buildTestFileStore,
  buildTestJwtMinter,
  closeTestDb,
  closeTestFileStore,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

interface ClaudeFile {
  id: string;
  created_at: string;
  filename: string;
  mime_type: string;
  size_bytes: number;
  type: 'file';
  downloadable?: boolean;
  scope?: { type: 'session'; id: string } | null;
}

interface FilePage {
  data: ClaudeFile[];
  first_id: string | null;
  last_id: string | null;
  has_more: boolean;
}

interface ClaudeError {
  type: 'error';
  error: { type: string; message: string };
  request_id: string | null;
}

async function uploadFile(
  baseURL: string,
  apiKey: string,
  payload: Buffer,
  opts: {
    filename?: string;
    purpose?: string;
    scopeId?: string;
    fileFirst?: boolean;
    orcaBeta?: boolean;
  } = {},
): Promise<Response> {
  const form = new FormData();
  const appendFile = () =>
    form.append(
      'file',
      new Blob([Uint8Array.from(payload)], { type: 'text/plain' }),
      opts.filename ?? 'payload.txt',
    );
  const appendLegacyFields = () => {
    if (opts.purpose !== undefined) form.append('purpose', opts.purpose);
    if (opts.scopeId !== undefined) form.append('scope_id', opts.scopeId);
  };
  if (opts.fileFirst) {
    appendFile();
    appendLegacyFields();
  } else {
    appendLegacyFields();
    appendFile();
  }
  return fetch(`${baseURL}/v1/files`, {
    method: 'POST',
    headers: {
      'x-api-key': apiKey,
      ...(opts.orcaBeta ? { 'orca-beta': 'legacy' } : {}),
    },
    body: form,
  });
}

describe('/v1/files public upload security and pagination (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let fileStore: Awaited<ReturnType<typeof buildTestFileStore>>;

  beforeAll(async () => {
    const { db } = await getTestDb();
    fileStore = await buildTestFileStore();
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
    workspaceId = uniqueWorkspace('files-public');
    apiKey = await createTestApiKey(db, workspaceId);
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestFileStore();
    await closeTestDb();
  });

  it('returns the Claude File projection for an upload', async () => {
    const payload = Buffer.from(`default-${Date.now()}`);
    const res = await uploadFile(baseURL, apiKey, payload);
    expect(res.status).toBe(200);
    const file = (await res.json()) as ClaudeFile & Record<string, unknown>;
    expect(file).toMatchObject({
      id: expect.stringMatching(/^file_/),
      filename: 'payload.txt',
      size_bytes: payload.length,
      type: 'file',
      downloadable: false,
      scope: null,
    });
    expect(file).not.toHaveProperty('purpose');
    expect(file).not.toHaveProperty('scope_id');
    expect(file).not.toHaveProperty('sha256');
    expect(file).not.toHaveProperty('metadata');
  }, 30000);

  it.each([false, true])(
    'never honors public purpose/scope_id selectors (fileFirst=%s)',
    async (fileFirst) => {
      const sessionId = `ses_${Date.now().toString(36)}_${fileFirst ? 'after' : 'before'}`;
      const post = await uploadFile(baseURL, apiKey, Buffer.from(`untrusted-${sessionId}`), {
        purpose: 'agent_output',
        scopeId: sessionId,
        fileFirst,
      });
      expect(post.status).toBe(200);
      const created = (await post.json()) as ClaudeFile;
      expect(created.downloadable).toBe(false);
      expect(created.scope).toBeNull();

      const scoped = await fetch(`${baseURL}/v1/files?scope_id=${sessionId}`, {
        headers: { 'x-api-key': apiKey },
      });
      expect(scoped.status).toBe(200);
      expect(((await scoped.json()) as FilePage).data).toEqual([]);

      const content = await fetch(`${baseURL}/v1/files/${created.id}/content`, {
        headers: { 'x-api-key': apiKey },
      });
      expect(content.status).toBe(403);
      const error = (await content.json()) as ClaudeError;
      expect(error).toMatchObject({
        type: 'error',
        error: { type: 'permission_error', message: expect.stringMatching(/not downloadable/i) },
      });
      expect(error.request_id).toEqual(expect.any(String));
    },
    30000,
  );

  it('ignores legacy selectors even when a legacy response is requested', async () => {
    const sessionId = `ses_${Date.now().toString(36)}_legacy`;
    const res = await uploadFile(baseURL, apiKey, Buffer.from(`legacy-${sessionId}`), {
      purpose: 'agent_output',
      scopeId: sessionId,
      orcaBeta: true,
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({
      purpose: 'agent',
      scope_id: null,
      downloadable: false,
    });
  }, 30000);

  it('does not deduplicate a public upload against an internal agent_output with the same SHA', async () => {
    const payload = Buffer.from(`purpose-separated-dedup-${Date.now()}`);
    const sessionId = `ses_internal_${Date.now().toString(36)}`;
    const internal = await fileStore.create({
      workspaceId,
      filename: 'internal-output.txt',
      mimeType: 'text/plain',
      content: Readable.from(payload),
      metadata: {},
      purpose: 'agent_output',
      scopeId: sessionId,
    });
    expect(internal.downloadable).toBe(true);

    const response = await uploadFile(baseURL, apiKey, payload);
    expect(response.status).toBe(200);
    const uploaded = (await response.json()) as ClaudeFile;
    expect(uploaded.id).not.toBe(internal.id);
    expect(uploaded.downloadable).toBe(false);
    expect(uploaded.scope).toBeNull();

    const storedUpload = await fileStore.get(workspaceId, uploaded.id);
    expect(storedUpload).toMatchObject({
      purpose: 'agent',
      scopeId: null,
      downloadable: false,
    });
    const storedOutput = await fileStore.get(workspaceId, internal.id);
    expect(storedOutput).toMatchObject({
      purpose: 'agent_output',
      scopeId: sessionId,
      downloadable: true,
    });
  }, 30000);

  it('paginates with after_id and before_id cursors', async () => {
    await uploadFile(baseURL, apiKey, Buffer.from(`page-a-${Date.now()}`));
    await uploadFile(baseURL, apiKey, Buffer.from(`page-b-${Date.now()}`));

    const first = await fetch(`${baseURL}/v1/files?limit=1`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(first.status).toBe(200);
    const firstBody = (await first.json()) as FilePage;
    expect(firstBody.data).toHaveLength(1);
    expect(firstBody.first_id).toBe(firstBody.data[0]!.id);
    expect(firstBody.last_id).toBe(firstBody.data[0]!.id);
    expect(firstBody.has_more).toBe(true);

    const second = await fetch(
      `${baseURL}/v1/files?limit=1&after_id=${encodeURIComponent(firstBody.last_id!)}`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(second.status).toBe(200);
    const secondBody = (await second.json()) as FilePage;
    expect(secondBody.data).toHaveLength(1);
    expect(secondBody.data[0]!.id).not.toBe(firstBody.data[0]!.id);

    const previous = await fetch(
      `${baseURL}/v1/files?limit=1&before_id=${encodeURIComponent(secondBody.first_id!)}`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(previous.status).toBe(200);
    expect(((await previous.json()) as FilePage).data[0]!.id).toBe(firstBody.data[0]!.id);
  }, 30000);

  it('returns Claude error envelopes for invalid pagination', async () => {
    for (const query of ['limit=0', 'limit=abc', 'after_id=file_after&before_id=file_before']) {
      const res = await fetch(`${baseURL}/v1/files?${query}`, {
        headers: { 'x-api-key': apiKey },
      });
      expect(res.status).toBe(400);
      expect(await res.json()).toMatchObject({
        type: 'error',
        error: { type: 'invalid_request_error', message: expect.any(String) },
        request_id: expect.any(String),
      });
    }
  });
});
