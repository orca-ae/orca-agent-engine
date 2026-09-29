// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { createHash } from 'node:crypto';
import { and, eq } from 'drizzle-orm';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildTestFileStore,
  closeTestFileStore,
} from './setup.js';
import {
  uniqueWorkspace,
  createTestApiKey,
  createTestAgent,
  createTestSession,
} from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import { sessions } from '../../src/persistence/postgres/schema.js';

interface FileResp {
  id: string;
  filename: string;
  mime_type: string;
  size_bytes: number;
  sha256: string;
  metadata: Record<string, string>;
  purpose: 'agent' | 'agent_output';
  scope_id: string | null;
  downloadable: boolean;
}

interface ListResp {
  data: FileResp[];
  first_id: string | null;
  last_id: string | null;
  has_more: boolean;
}

describe('/internal/v1/workspaces/:workspaceId/sessions/:sessionId/files (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let sessionId: string;

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
    workspaceId = uniqueWorkspace('internal');
    apiKey = await createTestApiKey(db, workspaceId);
    const agentId = await createTestAgent(baseURL, apiKey);
    sessionId = await createTestSession(baseURL, apiKey, agentId);
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestFileStore();
    await closeTestDb();
  });

  it('derives workspace, purpose, scope, and downloadability from the scoped path', async () => {
    const payload = Buffer.from('hello internal');
    const expectedSha = createHash('sha256').update(payload).digest('hex');

    const form = new FormData();
    form.append('meta_source', 'harness');
    form.append('file', new Blob([payload], { type: 'text/plain' }), 'output.txt');

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/files`,
      {
        method: 'POST',
        body: form,
      },
    );
    expect(res.status).toBe(201);
    const file = (await res.json()) as FileResp;
    expect(file.id).toMatch(/^file_/);
    expect(file.filename).toBe('output.txt');
    expect(file.purpose).toBe('agent_output');
    expect(file.scope_id).toBe(sessionId);
    expect(file.downloadable).toBe(true);
    expect(file.size_bytes).toBe(payload.length);
    expect(file.sha256).toBe(expectedSha);
    expect(file.metadata).toEqual({ source: 'harness' });
    expect(file).not.toHaveProperty('workspace_id');

    // The public list endpoint with the workspace's api-key MUST surface the
    // file when filtered by scope_id (locks in the harness → registry → SDK
    // round-trip).
    const list = await fetch(`${baseURL}/v1/files?scope_id=${sessionId}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(list.status).toBe(200);
    const body = (await list.json()) as ListResp;
    expect(body.data).toHaveLength(1);
    expect(body.data[0]?.id).toBe(file.id);
    expect(body).not.toHaveProperty('files');
    expect(body).not.toHaveProperty('page_info');

    // The bytes must round-trip through `getContent` (downloadable=true means
    // the gate lets it through).
    const content = await fetch(`${baseURL}/v1/files/${file.id}/content`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(content.status).toBe(200);
    const back = Buffer.from(await content.arrayBuffer());
    expect(back).toEqual(payload);
  }, 30000);

  it('rejects caller-supplied workspace routing fields', async () => {
    const form = new FormData();
    form.append('workspace_id', uniqueWorkspace('redirect'));
    form.append('file', new Blob([Buffer.from('x')], { type: 'text/plain' }), 'x.txt');

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/files`,
      { method: 'POST', body: form },
    );
    expect(res.status).toBe(400);
    const err = (await res.json()) as { error: string };
    expect(err.error).toBe('unexpected field workspace_id');
  }, 30000);

  it('rejects caller-supplied purpose instead of trusting a multipart override', async () => {
    const form = new FormData();
    form.append('purpose', 'spam');
    form.append('file', new Blob([Buffer.from('x')], { type: 'text/plain' }), 'x.txt');

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/files`,
      { method: 'POST', body: form },
    );
    expect(res.status).toBe(400);
    const err = (await res.json()) as { error: string };
    expect(err.error).toBe('unexpected field purpose');
  }, 30000);

  it('fails closed when the session is addressed through another workspace', async () => {
    const form = new FormData();
    form.append(
      'file',
      new Blob([Buffer.from('cross-workspace output')], { type: 'text/plain' }),
      'cross.txt',
    );

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${uniqueWorkspace('wrong_file_scope')}/sessions/${sessionId}/files`,
      { method: 'POST', body: form },
    );
    expect(res.status).toBe(404);
  }, 30000);

  it('does not register output files for a terminated session', async () => {
    const { db } = await getTestDb();
    await db
      .update(sessions)
      .set({ status: 'terminated' })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    try {
      const form = new FormData();
      form.append('file', new Blob([Buffer.from('late output')]), 'late.txt');
      const res = await fetch(
        `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/files`,
        { method: 'POST', body: form },
      );
      expect(res.status).toBe(404);
    } finally {
      await db
        .update(sessions)
        .set({ status: 'idle' })
        .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId)));
    }
  }, 30000);
});
