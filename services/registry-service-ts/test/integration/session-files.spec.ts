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
import {
  createTestAgent,
  createTestApiKey,
  createTestSession,
  uniqueWorkspace,
} from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

interface LegacyFile {
  id: string;
  purpose: 'agent' | 'agent_output';
  scope_id: string | null;
  downloadable: boolean;
}

interface FilePage {
  data: LegacyFile[];
  first_id: string | null;
  last_id: string | null;
  has_more: boolean;
}

describe.sequential('/v1/sessions/:id/files (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let otherApiKey: string;
  let workspaceId: string;
  let sessionId: string;
  let otherSessionId: string;
  let ownedFileId: string;
  let deletableFileId: string;
  let gatedFileId: string;
  let otherSessionFileId: string;
  let wrongPurposeFileId: string;
  const ownedPayload = Buffer.from(`session-file-${Date.now()}`);

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

    workspaceId = uniqueWorkspace('session-files');
    apiKey = await createTestApiKey(db, workspaceId);
    const agentId = await createTestAgent(baseURL, apiKey);
    sessionId = await createTestSession(baseURL, apiKey, agentId);
    otherSessionId = await createTestSession(baseURL, apiKey, agentId);

    const otherWorkspaceId = uniqueWorkspace('session-files-other');
    otherApiKey = await createTestApiKey(db, otherWorkspaceId);
    const otherAgentId = await createTestAgent(baseURL, otherApiKey);
    const otherWorkspaceSessionId = await createTestSession(baseURL, otherApiKey, otherAgentId);

    ownedFileId = (
      await fileStore.create({
        workspaceId,
        filename: 'owned.txt',
        mimeType: 'text/plain',
        content: Readable.from(ownedPayload),
        purpose: 'agent_output',
        scopeId: sessionId,
      })
    ).id;
    deletableFileId = (
      await fileStore.create({
        workspaceId,
        filename: 'delete.txt',
        mimeType: 'text/plain',
        content: Readable.from(Buffer.from('delete me')),
        purpose: 'agent_output',
        scopeId: sessionId,
      })
    ).id;
    gatedFileId = (
      await fileStore.create({
        workspaceId,
        filename: 'gated.txt',
        mimeType: 'text/plain',
        content: Readable.from(Buffer.from('gated')),
        purpose: 'agent_output',
        scopeId: sessionId,
        downloadable: false,
      })
    ).id;
    otherSessionFileId = (
      await fileStore.create({
        workspaceId,
        filename: 'other-session.txt',
        mimeType: 'text/plain',
        content: Readable.from(Buffer.from('other session')),
        purpose: 'agent_output',
        scopeId: otherSessionId,
      })
    ).id;
    wrongPurposeFileId = (
      await fileStore.create({
        workspaceId,
        filename: 'input-with-scope.txt',
        mimeType: 'text/plain',
        content: Readable.from(Buffer.from('not an output')),
        purpose: 'agent',
        scopeId: sessionId,
      })
    ).id;
    await fileStore.create({
      workspaceId: otherWorkspaceId,
      filename: 'other-workspace.txt',
      mimeType: 'text/plain',
      content: Readable.from(Buffer.from('other workspace')),
      purpose: 'agent_output',
      scopeId: otherWorkspaceSessionId,
    });
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestFileStore();
    await closeTestDb();
  });

  it('lists only output files owned by the addressed session and paginates them', async () => {
    const headers = { 'x-api-key': apiKey, 'orca-beta': 'legacy' };
    const first = await fetch(`${baseURL}/v1/sessions/${sessionId}/files?limit=2`, { headers });
    expect(first.status).toBe(200);
    const firstPage = (await first.json()) as FilePage;
    expect(firstPage.data).toHaveLength(2);
    expect(firstPage.has_more).toBe(true);

    const second = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/files?limit=2&after_id=${firstPage.last_id}`,
      { headers },
    );
    expect(second.status).toBe(200);
    const secondPage = (await second.json()) as FilePage;
    const files = [...firstPage.data, ...secondPage.data];

    expect(files.map((file) => file.id).sort()).toEqual(
      [ownedFileId, deletableFileId, gatedFileId].sort(),
    );
    expect(files.every((file) => file.purpose === 'agent_output')).toBe(true);
    expect(files.every((file) => file.scope_id === sessionId)).toBe(true);
    expect(files.map((file) => file.id)).not.toContain(otherSessionFileId);
    expect(files.map((file) => file.id)).not.toContain(wrongPurposeFileId);
    expect(secondPage.has_more).toBe(false);
  });

  it('gets and downloads only files owned by the addressed session', async () => {
    const headers = { 'x-api-key': apiKey };
    const get = await fetch(`${baseURL}/v1/sessions/${sessionId}/files/${ownedFileId}`, {
      headers,
    });
    expect(get.status).toBe(200);
    expect(await get.json()).toMatchObject({
      id: ownedFileId,
      type: 'file',
      scope: { type: 'session', id: sessionId },
    });

    const content = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/files/${ownedFileId}/content`,
      { headers },
    );
    expect(content.status).toBe(200);
    expect(Buffer.from(await content.arrayBuffer())).toEqual(ownedPayload);

    const gated = await fetch(`${baseURL}/v1/sessions/${sessionId}/files/${gatedFileId}/content`, {
      headers,
    });
    expect(gated.status).toBe(403);

    for (const fileId of [otherSessionFileId, wrongPurposeFileId]) {
      const response = await fetch(`${baseURL}/v1/sessions/${sessionId}/files/${fileId}`, {
        headers,
      });
      expect(response.status).toBe(404);
    }

    const crossWorkspace = await fetch(`${baseURL}/v1/sessions/${sessionId}/files/${ownedFileId}`, {
      headers: { 'x-api-key': otherApiKey },
    });
    expect(crossWorkspace.status).toBe(404);
  });

  it('deletes only files owned by the addressed session', async () => {
    const wrongSession = await fetch(
      `${baseURL}/v1/sessions/${otherSessionId}/files/${deletableFileId}`,
      { method: 'DELETE', headers: { 'x-api-key': apiKey } },
    );
    expect(wrongSession.status).toBe(404);

    const deleted = await fetch(`${baseURL}/v1/sessions/${sessionId}/files/${deletableFileId}`, {
      method: 'DELETE',
      headers: { 'x-api-key': apiKey },
    });
    expect(deleted.status).toBe(200);
    expect(await deleted.json()).toEqual({ id: deletableFileId, type: 'file_deleted' });

    const missing = await fetch(`${baseURL}/v1/sessions/${sessionId}/files/${deletableFileId}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(missing.status).toBe(404);
  });

  it('rejects invalid pagination and unknown sessions', async () => {
    const invalid = await fetch(
      `${baseURL}/v1/sessions/${sessionId}/files?after_id=file_a&before_id=file_b`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(invalid.status).toBe(400);

    const unknown = await fetch(`${baseURL}/v1/sessions/ses_missing/files`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(unknown.status).toBe(404);
  });
});
