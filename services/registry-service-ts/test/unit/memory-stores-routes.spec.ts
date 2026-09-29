// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import Fastify, { type FastifyInstance } from 'fastify';
import {
  InMemoryMemoryBlobStore,
  InMemoryMemoryMetadataStore,
  LocalMemoryStore,
} from '@orca/memory-store';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { registerMemoryStoresRoutes } from '../../src/api/memory-stores.routes.js';

describe('memory store routes', () => {
  let app: FastifyInstance;
  let memoryStore: LocalMemoryStore;

  beforeEach(async () => {
    app = Fastify();
    app.addHook('onRequest', async (request) => {
      const userId = request.headers['x-test-user-id'];
      if (typeof userId === 'string') {
        request.auth = {
          workspaceId: 'ws_memory_route_test',
          principal: userId,
          scopes: [],
          authMethod: 'oidc',
          userId,
        };
      } else {
        const apiKeyId = request.headers['x-test-api-key-id'];
        request.auth = {
          workspaceId: 'ws_memory_route_test',
          principal: 'test',
          scopes: [],
          authMethod: 'api-key',
          apiKeyId: typeof apiKeyId === 'string' ? apiKeyId : 'key_test',
        };
      }
    });
    memoryStore = new LocalMemoryStore({
      blobStore: new InMemoryMemoryBlobStore(),
      metadataStore: new InMemoryMemoryMetadataStore(),
    });
    registerMemoryStoresRoutes(app, memoryStore);
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
    await memoryStore.close();
  });

  it('loads only page memory histories, preserves prefix/Unicode ordering and caps full-view blob reads', async () => {
    const store = await memoryStore.createStore({
      workspaceId: 'ws_memory_route_test',
      name: 'page',
    });
    for (const path of ['/a.txt', '/dir/a.txt', '/dir/b.txt', '/é.txt', '/😀.txt', '/\uE000.txt']) {
      expect(
        (
          await app.inject({
            method: 'POST',
            url: `/v1/memory_stores/${store.id}/memories`,
            payload: { path, content: path },
          })
        ).statusCode,
      ).toBe(200);
    }
    const histories = vi.spyOn(memoryStore, 'listAllVersions');
    let active = 0;
    let peak = 0;
    const open = memoryStore.openMemory.bind(memoryStore);
    vi.spyOn(memoryStore, 'openMemory').mockImplementation(async (...args) => {
      active++;
      peak = Math.max(peak, active);
      try {
        return await open(...args);
      } finally {
        active--;
      }
    });
    const first = await app.inject(`/v1/memory_stores/${store.id}/memories?limit=1`);
    expect(first.statusCode).toBe(200);
    expect(histories.mock.calls.at(-1)?.[2]).toEqual({ memoryIds: [first.json().data[0].id] });
    const full = await app.inject(`/v1/memory_stores/${store.id}/memories?view=full`);
    expect(full.statusCode).toBe(200);
    expect(full.json().data.map((item: { path: string }) => item.path)).toEqual([
      '/a.txt',
      '/dir/a.txt',
      '/dir/b.txt',
      '/é.txt',
      '/😀.txt',
      '/\uE000.txt',
    ]);
    expect(peak).toBeLessThanOrEqual(4);
    const prefixes = await app.inject(
      `/v1/memory_stores/${store.id}/memories?depth=1&path_prefix=/dir/&limit=1`,
    );
    expect(prefixes.json().data[0].path).toBe('/dir/a.txt');
    const root = await app.inject(`/v1/memory_stores/${store.id}/memories?depth=1`);
    expect(
      root.json().data.filter((item: { type: string }) => item.type === 'memory_prefix'),
    ).toEqual([{ type: 'memory_prefix', path: '/dir/' }]);
    const versionList = await app.inject(
      `/v1/memory_stores/${store.id}/memory_versions?memory_id=${first.json().data[0].id}`,
    );
    expect(versionList.statusCode).toBe(200);
    expect(histories.mock.calls.at(-1)?.[2]).toEqual({ memoryIds: [first.json().data[0].id] });
  });

  it('returns Anthropic 200 for creates under `orca-beta`', async () => {
    const storeResponse = await app.inject({
      method: 'POST',
      url: '/v1/memory_stores',
      headers: { 'orca-beta': '1' },
      payload: { name: 'beta-notes' },
    });
    expect(storeResponse.statusCode).toBe(200);

    const memoryResponse = await app.inject({
      method: 'POST',
      url: `/v1/memory_stores/${storeResponse.json().id}/memories`,
      headers: { 'orca-beta': '1' },
      payload: { path: '/note.txt', content: 'hello' },
    });
    expect(memoryResponse.statusCode).toBe(200);
  });

  it('creates a memory from path and UTF-8 content and derives its metadata', async () => {
    const store = await memoryStore.createStore({
      workspaceId: 'ws_memory_route_test',
      name: 'notes',
    });
    const content = '蓝色 🐋';
    const bytes = Buffer.from(content, 'utf8');

    const response = await app.inject({
      method: 'POST',
      url: `/v1/memory_stores/${store.id}/memories`,
      payload: { path: '/preferences/color.txt', content },
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      memory_store_id: store.id,
      path: '/preferences/color.txt',
      content_sha256: createHash('sha256').update(bytes).digest('hex'),
      content_size_bytes: bytes.length,
      type: 'memory',
      content: null,
    });
    expect(response.json().memory_version_id).toMatch(/^memver_/);

    const retrieved = await app.inject({
      method: 'GET',
      url: `/v1/memory_stores/${store.id}/memories/${response.json().id}`,
    });
    expect(retrieved.statusCode).toBe(200);
    expect(retrieved.json()).toMatchObject({ content, path: '/preferences/color.txt' });
  });

  it('rejects the removed base64-only request shape', async () => {
    const store = await memoryStore.createStore({
      workspaceId: 'ws_memory_route_test',
      name: 'legacy',
    });

    const response = await app.inject({
      method: 'POST',
      url: `/v1/memory_stores/${store.id}/memories`,
      payload: {
        path: '/legacy.txt',
        content_base64: Buffer.from('legacy').toString('base64'),
        content_sha256: createHash('sha256').update('legacy').digest('hex'),
      },
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'content is required' });
  });

  it('renames with POST, preserves the memory id, and does not version no-op updates', async () => {
    const store = await memoryStore.createStore({
      workspaceId: 'ws_memory_route_test',
      name: 'rename',
    });
    const createdResponse = await app.inject({
      method: 'POST',
      url: `/v1/memory_stores/${store.id}/memories`,
      payload: { path: '/before.txt', content: 'same bytes' },
    });
    const created = createdResponse.json();

    const renamedResponse = await app.inject({
      method: 'POST',
      url: `/v1/memory_stores/${store.id}/memories/${created.id}?view=full`,
      payload: { path: '/after.txt' },
    });
    expect(renamedResponse.statusCode).toBe(200);
    expect(renamedResponse.json()).toMatchObject({
      id: created.id,
      path: '/after.txt',
      content: 'same bytes',
    });
    expect(
      await memoryStore.listVersions('ws_memory_route_test', store.id, created.id),
    ).toHaveLength(2);

    const noOpResponse = await app.inject({
      method: 'POST',
      url: `/v1/memory_stores/${store.id}/memories/${created.id}`,
      payload: {
        path: '/after.txt',
        content: 'same bytes',
        precondition: { type: 'content_sha256', content_sha256: '0'.repeat(64) },
      },
    });
    expect(noOpResponse.statusCode).toBe(200);
    expect(
      await memoryStore.listVersions('ws_memory_route_test', store.id, created.id),
    ).toHaveLength(2);
  });

  it('returns a delete tombstone and retains the deleted version history', async () => {
    const store = await memoryStore.createStore({
      workspaceId: 'ws_memory_route_test',
      name: 'history',
    });
    const created = (
      await app.inject({
        method: 'POST',
        url: `/v1/memory_stores/${store.id}/memories`,
        payload: { path: '/gone.txt', content: 'gone' },
      })
    ).json();

    const deleted = await app.inject({
      method: 'DELETE',
      url: `/v1/memory_stores/${store.id}/memories/${created.id}`,
    });
    expect(deleted.statusCode).toBe(200);
    expect(deleted.json()).toEqual({ id: created.id, type: 'memory_deleted' });

    const versions = await app.inject({
      method: 'GET',
      url: `/v1/memory_stores/${store.id}/memory_versions?memory_id=${created.id}`,
    });
    expect(versions.statusCode).toBe(200);
    expect(versions.json().data.map((version: { operation: string }) => version.operation)).toEqual(
      ['deleted', 'created'],
    );
    expect(versions.json().data[0]).toMatchObject({
      memory_id: created.id,
      content: null,
      content_sha256: null,
      type: 'memory_version',
    });
  });

  it('filters versions by session actor and masks a redacted version', async () => {
    const store = await memoryStore.createStore({
      workspaceId: 'ws_memory_route_test',
      name: 'actors',
    });
    const bytes = Buffer.from('session-authored');
    const write = await memoryStore.writeMemory({
      workspaceId: 'ws_memory_route_test',
      storeId: store.id,
      path: 'session.txt',
      content: Readable.from(bytes),
      sizeBytes: bytes.length,
      sha256: createHash('sha256').update(bytes).digest('hex'),
      writtenBySessionId: 'ses_writer',
    });

    const listed = await app.inject({
      method: 'GET',
      url: `/v1/memory_stores/${store.id}/memory_versions?session_id=ses_writer&view=full`,
    });
    expect(listed.statusCode).toBe(200);
    expect(listed.json().data).toEqual([
      expect.objectContaining({
        id: write.version.id,
        operation: 'created',
        content: 'session-authored',
        created_by: { type: 'session_actor', session_id: 'ses_writer' },
      }),
    ]);

    const redacted = await app.inject({
      method: 'POST',
      url: `/v1/memory_stores/${store.id}/memory_versions/${write.version.id}/redact`,
    });
    expect(redacted.statusCode).toBe(200);
    expect(redacted.json()).toMatchObject({
      id: write.version.id,
      content: null,
      content_sha256: null,
      content_size_bytes: null,
      path: null,
      redacted_by: { type: 'api_actor', api_key_id: 'key_test' },
    });
    expect(redacted.json().redacted_at).toEqual(expect.any(String));
  });

  it('attributes public create, update, delete, and redact actors and filters API keys', async () => {
    const store = await memoryStore.createStore({
      workspaceId: 'ws_memory_route_test',
      name: 'public-actors',
    });
    const created = (
      await app.inject({
        method: 'POST',
        url: `/v1/memory_stores/${store.id}/memories`,
        headers: { 'x-test-api-key-id': 'key_create' },
        payload: { path: '/actor.txt', content: 'created' },
      })
    ).json();

    const updated = await app.inject({
      method: 'POST',
      url: `/v1/memory_stores/${store.id}/memories/${created.id}`,
      headers: { 'x-test-user-id': 'user_update' },
      payload: { content: 'updated' },
    });
    expect(updated.statusCode).toBe(200);

    const deleted = await app.inject({
      method: 'DELETE',
      url: `/v1/memory_stores/${store.id}/memories/${created.id}`,
      headers: { 'x-test-api-key-id': 'key_delete' },
    });
    expect(deleted.statusCode).toBe(200);

    const versions = await app.inject({
      method: 'GET',
      url: `/v1/memory_stores/${store.id}/memory_versions?memory_id=${created.id}`,
    });
    expect(
      versions
        .json()
        .data.map(
          (version: {
            operation: string;
            created_by?: { type: string; api_key_id?: string; user_id?: string };
          }) => ({
            operation: version.operation,
            created_by: version.created_by,
          }),
        ),
    ).toEqual([
      {
        operation: 'deleted',
        created_by: { type: 'api_actor', api_key_id: 'key_delete' },
      },
      {
        operation: 'modified',
        created_by: { type: 'user_actor', user_id: 'user_update' },
      },
      {
        operation: 'created',
        created_by: { type: 'api_actor', api_key_id: 'key_create' },
      },
    ]);

    const byCreateKey = await app.inject({
      method: 'GET',
      url: `/v1/memory_stores/${store.id}/memory_versions?api_key_id=key_create`,
    });
    expect(byCreateKey.json().data).toEqual([
      expect.objectContaining({
        operation: 'created',
        created_by: { type: 'api_actor', api_key_id: 'key_create' },
      }),
    ]);
    const byUnknownKey = await app.inject({
      method: 'GET',
      url: `/v1/memory_stores/${store.id}/memory_versions?api_key_id=key_unknown`,
    });
    expect(byUnknownKey.json().data).toEqual([]);

    const createdVersion = versions
      .json()
      .data.find((version: { operation: string }) => version.operation === 'created');
    const redacted = await app.inject({
      method: 'POST',
      url: `/v1/memory_stores/${store.id}/memory_versions/${createdVersion.id}/redact`,
      headers: { 'x-test-user-id': 'user_redactor' },
    });
    expect(redacted.statusCode).toBe(200);
    expect(redacted.json()).toMatchObject({
      created_by: { type: 'api_actor', api_key_id: 'key_create' },
      redacted_by: { type: 'user_actor', user_id: 'user_redactor' },
      content: null,
      content_sha256: null,
      content_size_bytes: null,
      path: null,
    });
  });

  it('filters archived stores before applying the public page limit', async () => {
    for (let index = 0; index < 25; index += 1) {
      await memoryStore.createStore({
        workspaceId: 'ws_memory_route_test',
        name: `store-${index}`,
      });
    }
    const raw = await memoryStore.listStores('ws_memory_route_test', { limit: 100 });
    for (const store of raw.items.slice(0, 5)) {
      await memoryStore.archiveStore('ws_memory_route_test', store.id);
    }

    const response = await app.inject({ method: 'GET', url: '/v1/memory_stores?limit=20' });
    expect(response.statusCode).toBe(200);
    expect(response.json().data).toHaveLength(20);
    expect(response.json().next_page).toBeNull();
    expect(
      response.json().data.every((store: { type: string }) => store.type === 'memory_store'),
    ).toBe(true);
  });

  it('treats an empty page cursor as the first page', async () => {
    await memoryStore.createStore({
      workspaceId: 'ws_memory_route_test',
      name: 'empty-page',
    });

    const response = await app.inject({ method: 'GET', url: '/v1/memory_stores?page=' });

    expect(response.statusCode).toBe(200);
    expect(response.json().data).toHaveLength(1);
  });

  it('validates store names by Unicode code points and only rejects control characters', async () => {
    const formatCharacter = await app.inject({
      method: 'POST',
      url: '/v1/memory_stores',
      payload: { name: 'joined\u200Dname' },
    });
    expect(formatCharacter.statusCode).toBe(200);

    const maxCodePoints = await app.inject({
      method: 'POST',
      url: '/v1/memory_stores',
      payload: { name: '🐋'.repeat(255) },
    });
    expect(maxCodePoints.statusCode).toBe(200);

    const tooManyCodePoints = await app.inject({
      method: 'POST',
      url: '/v1/memory_stores',
      payload: { name: '🐋'.repeat(256) },
    });
    expect(tooManyCodePoints.statusCode).toBe(400);

    const controlCharacter = await app.inject({
      method: 'POST',
      url: '/v1/memory_stores',
      payload: { name: 'bad\u0000name' },
    });
    expect(controlCharacter.statusCode).toBe(400);
  });
});
