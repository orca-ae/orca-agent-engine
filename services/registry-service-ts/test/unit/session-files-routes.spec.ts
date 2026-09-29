// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Fastify, { type FastifyInstance } from 'fastify';
import { Readable } from 'node:stream';
import {
  InMemoryBlobStore,
  InMemoryFileMetadataStore,
  LocalFileStore,
  type FileRecord,
} from '@orca/file-store';
import { InMemorySkillStore } from '@orca/skill-store';
import type { TranscriptStore } from '@orca/transcript-store';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { registerSessionsRoutes } from '../../src/api/sessions.routes.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';

function sessionExistsDb(): DbClient {
  return {
    select: () => ({
      from: () => ({
        where: () => ({
          limit: async () => [{ id: 'ses_exists' }],
        }),
      }),
    }),
  } as unknown as DbClient;
}

async function createFile(
  store: LocalFileStore,
  input: {
    workspaceId?: string;
    filename: string;
    content: string;
    purpose: 'agent' | 'agent_output';
    scopeId: string | null;
    downloadable?: boolean;
  },
): Promise<FileRecord> {
  return store.create({
    workspaceId: input.workspaceId ?? 'ws_session_files',
    filename: input.filename,
    mimeType: 'text/plain',
    content: Readable.from(input.content),
    purpose: input.purpose,
    scopeId: input.scopeId,
    ...(input.downloadable === undefined ? {} : { downloadable: input.downloadable }),
  });
}

describe('Session file routes', () => {
  let app: FastifyInstance;
  let store: LocalFileStore;

  beforeEach(async () => {
    app = Fastify();
    app.addHook('onRequest', async (request) => {
      request.auth = {
        workspaceId: 'ws_session_files',
        principal: 'test',
        scopes: [],
        authMethod: 'api-key',
      };
    });
    store = new LocalFileStore({
      blobStore: new InMemoryBlobStore(),
      metadataStore: new InMemoryFileMetadataStore(),
    });
    registerSessionsRoutes(
      app,
      sessionExistsDb(),
      {} as TranscriptStore,
      { bufferSize: 1, dropAgeMs: 1, heartbeatMs: 1 },
      store,
      new InMemorySkillStore(),
    );
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
    await store.close();
  });

  it('lists, gets, downloads, and deletes only active outputs from the addressed session', async () => {
    const owned = await createFile(store, {
      filename: 'owned.txt',
      content: 'owned bytes',
      purpose: 'agent_output',
      scopeId: 'ses_a',
    });
    const otherSession = await createFile(store, {
      filename: 'other.txt',
      content: 'other bytes',
      purpose: 'agent_output',
      scopeId: 'ses_b',
    });
    const wrongPurpose = await createFile(store, {
      filename: 'input.txt',
      content: 'input bytes',
      purpose: 'agent',
      scopeId: 'ses_a',
    });
    const gated = await createFile(store, {
      filename: 'gated.txt',
      content: 'gated bytes',
      purpose: 'agent_output',
      scopeId: 'ses_a',
      downloadable: false,
    });

    const list = await app.inject({
      method: 'GET',
      url: '/v1/sessions/ses_a/files',
      headers: { 'orca-beta': 'legacy' },
    });
    expect(list.statusCode).toBe(200);
    expect(list.json().data).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: owned.id, purpose: 'agent_output', scope_id: 'ses_a' }),
        expect.objectContaining({ id: gated.id, purpose: 'agent_output', scope_id: 'ses_a' }),
      ]),
    );
    expect(list.json().data.map((file: { id: string }) => file.id)).not.toContain(otherSession.id);
    expect(list.json().data.map((file: { id: string }) => file.id)).not.toContain(wrongPurpose.id);

    const get = await app.inject({ method: 'GET', url: `/v1/sessions/ses_a/files/${owned.id}` });
    expect(get.statusCode).toBe(200);
    expect(get.json()).toMatchObject({
      id: owned.id,
      type: 'file',
      scope: { type: 'session', id: 'ses_a' },
    });

    const content = await app.inject({
      method: 'GET',
      url: `/v1/sessions/ses_a/files/${owned.id}/content`,
    });
    expect(content.statusCode).toBe(200);
    expect(content.body).toBe('owned bytes');

    const gatedContent = await app.inject({
      method: 'GET',
      url: `/v1/sessions/ses_a/files/${gated.id}/content`,
    });
    expect(gatedContent.statusCode).toBe(403);

    for (const inaccessible of [otherSession, wrongPurpose]) {
      const response = await app.inject({
        method: 'GET',
        url: `/v1/sessions/ses_a/files/${inaccessible.id}`,
      });
      expect(response.statusCode).toBe(404);
    }

    const wrongSessionDelete = await app.inject({
      method: 'DELETE',
      url: `/v1/sessions/ses_b/files/${owned.id}`,
    });
    expect(wrongSessionDelete.statusCode).toBe(404);

    const deleted = await app.inject({
      method: 'DELETE',
      url: `/v1/sessions/ses_a/files/${owned.id}`,
    });
    expect(deleted.statusCode).toBe(200);
    expect(deleted.json()).toEqual({ id: owned.id, type: 'file_deleted' });

    const missing = await app.inject({
      method: 'GET',
      url: `/v1/sessions/ses_a/files/${owned.id}`,
    });
    expect(missing.statusCode).toBe(404);
  });
});
