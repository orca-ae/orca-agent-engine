// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { createHash, randomBytes } from 'node:crypto';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildTestFileStore,
  closeTestFileStore,
  buildTestMemoryStore,
  closeTestMemoryStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

interface ApiMemoryStore {
  id: string;
  name: string;
  type: 'memory_store';
  description: string;
  metadata: Record<string, string>;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

interface ClaudeError {
  type: 'error';
  error: { type: string; message: string; [key: string]: unknown };
  request_id: string | null;
}

function uniqueName(prefix = 'memstore'): string {
  return `${prefix}-${randomBytes(4).toString('hex')}`;
}

describe('/v1/memory_stores (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let otherApiKey: string;

  beforeAll(async () => {
    const { db } = await getTestDb();
    const fileStore = await buildTestFileStore();
    const memoryStore = await buildTestMemoryStore();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore,
      memoryStore,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('memstores');
    apiKey = await createTestApiKey(db, workspaceId);
    otherApiKey = await createTestApiKey(db, uniqueWorkspace('memstores-other'));
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestMemoryStore();
    await closeTestFileStore();
    await closeTestDb();
  });

  it('POST /v1/memory_stores returns a canonical memory_store', async () => {
    const name = uniqueName();
    const res = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name, description: 'shared scratchpad' }),
    });
    expect(res.status).toBe(200);
    const created = (await res.json()) as ApiMemoryStore;
    expect(created.id).toMatch(/^mems_/);
    expect(created.type).toBe('memory_store');
    expect(created).not.toHaveProperty('workspace_id');
    expect(created.name).toBe(name);
    expect(created.description).toBe('shared scratchpad');
    expect(created.metadata).toEqual({});
    expect(created.archived_at).toBeNull();
  });

  it('POST /v1/memory_stores persists metadata and update patches it', async () => {
    const name = uniqueName('meta');
    const create = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name, metadata: { owner: 'platform', keep: 'yes' } }),
    });
    expect(create.status).toBe(200);
    const created = (await create.json()) as ApiMemoryStore;
    expect(created.metadata).toEqual({ owner: 'platform', keep: 'yes' });

    const update = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ metadata: { owner: 'agent-team', keep: null, added: 'value' } }),
    });
    expect(update.status).toBe(200);
    expect(((await update.json()) as ApiMemoryStore).metadata).toEqual({
      owner: 'agent-team',
      added: 'value',
    });
  });

  it('POST /v1/memory_stores round-trips reserved metadata keys as data', async () => {
    const name = uniqueName('reserved');
    const create = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name, metadata: metadataWithReservedKeys('literal-prototype') }),
    });
    expect(create.status).toBe(200);
    const created = (await create.json()) as ApiMemoryStore;
    expect(created.metadata['__proto__']).toBe('literal-prototype');
    expect(created.metadata.constructor).toBe('literal-constructor');

    const update = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ metadata: metadataWithReservedKeys('updated-prototype') }),
    });
    expect(update.status).toBe(200);
    const updated = (await update.json()) as ApiMemoryStore;
    expect(updated.metadata['__proto__']).toBe('updated-prototype');
    expect(updated.metadata.constructor).toBe('literal-constructor');
  });

  it('POST /v1/memory_stores rejects metadata beyond managed limits', async () => {
    const create = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: uniqueName('limit'), metadata: metadataPairs(17) }),
    });
    expect(create.status).toBe(400);
    expect(((await create.json()) as ClaudeError).error.message).toMatch(/at most 16 pairs/);

    const valid = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: uniqueName('limit-valid'), metadata: metadataPairs(16) }),
    });
    expect(valid.status).toBe(200);
    const created = (await valid.json()) as ApiMemoryStore;

    const update = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ metadata: { extra: 'value' } }),
    });
    expect(update.status).toBe(400);
    expect(((await update.json()) as ClaudeError).error.message).toMatch(/at most 16 pairs/);
  });

  it('allows duplicate memory store names', async () => {
    const name = uniqueName('dup');
    const first = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    expect(first.status).toBe(200);
    const firstStore = (await first.json()) as ApiMemoryStore;
    const second = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    expect(second.status).toBe(200);
    const secondStore = (await second.json()) as ApiMemoryStore;
    expect(secondStore.id).not.toBe(firstStore.id);
    expect(secondStore.name).toBe(firstStore.name);
  });

  it('POST with empty name -> 400', async () => {
    const res = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: '' }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as ClaudeError;
    expect(body.error.message).toMatch(/name/);
  });

  it('GET /v1/memory_stores returns the created store', async () => {
    const name = uniqueName('list');
    const post = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    const created = (await post.json()) as ApiMemoryStore;

    const res = await fetch(`${baseURL}/v1/memory_stores`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(res.status).toBe(200);
    const page = (await res.json()) as {
      data: ApiMemoryStore[];
      next_page: string | null;
    };
    expect(page.data.map((s) => s.id)).toContain(created.id);
    expect(page.next_page).toBeNull();
    expect(page).not.toHaveProperty('memory_stores');
    expect(page).not.toHaveProperty('page_info');
  });

  it('GET /v1/memory_stores rejects non-positive and non-numeric limits', async () => {
    for (const limit of ['0', 'abc']) {
      const res = await fetch(`${baseURL}/v1/memory_stores?limit=${limit}`, {
        headers: { 'x-api-key': apiKey },
      });
      expect(res.status).toBe(400);
      expect(((await res.json()) as ClaudeError).error.message).toBe(
        'limit must be a positive integer',
      );
    }
  });

  it('GET /v1/memory_stores paginates with page and next_page', async () => {
    for (const prefix of ['page-a', 'page-b']) {
      const post = await fetch(`${baseURL}/v1/memory_stores`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({ name: uniqueName(prefix) }),
      });
      expect(post.status).toBe(200);
    }

    const first = await fetch(`${baseURL}/v1/memory_stores?limit=1`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(first.status).toBe(200);
    const firstBody = (await first.json()) as {
      data: ApiMemoryStore[];
      next_page: string | null;
    };
    expect(firstBody.data).toHaveLength(1);
    expect(firstBody.next_page).toBeTruthy();

    const second = await fetch(
      `${baseURL}/v1/memory_stores?limit=1&page=${encodeURIComponent(firstBody.next_page!)}`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(second.status).toBe(200);
    const secondBody = (await second.json()) as {
      data: ApiMemoryStore[];
      next_page: string | null;
    };
    expect(secondBody.data).toHaveLength(1);
    expect(secondBody.data[0]!.id).not.toBe(firstBody.data[0]!.id);

    const emptyPage = await fetch(`${baseURL}/v1/memory_stores?limit=1&page=`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(emptyPage.status).toBe(200);
    expect(((await emptyPage.json()) as { data: ApiMemoryStore[] }).data).toHaveLength(1);
  });

  it('POST /v1/memory_stores/:id updates name and description', async () => {
    const originalName = uniqueName('upd');
    const post = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: originalName, description: 'before' }),
    });
    const created = (await post.json()) as ApiMemoryStore;
    const nextName = uniqueName('upd-next');

    const update = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: nextName, description: null }),
    });
    expect(update.status).toBe(200);
    const updated = (await update.json()) as ApiMemoryStore;
    expect(updated.id).toBe(created.id);
    expect(updated.name).toBe(nextName);
    expect(updated.description).toBe('');

    const get = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(get.status).toBe(200);
    expect(((await get.json()) as ApiMemoryStore).name).toBe(nextName);
  });

  it('POST /v1/memory_stores/:id validates fields and allows empty/duplicate-name updates', async () => {
    const firstName = uniqueName('upd-dup-a');
    const secondName = uniqueName('upd-dup-b');
    const first = (await (
      await fetch(`${baseURL}/v1/memory_stores`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({ name: firstName }),
      })
    ).json()) as ApiMemoryStore;
    const second = (await (
      await fetch(`${baseURL}/v1/memory_stores`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({ name: secondName }),
      })
    ).json()) as ApiMemoryStore;

    const badName = await fetch(`${baseURL}/v1/memory_stores/${first.id}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: '' }),
    });
    expect(badName.status).toBe(400);

    const missingFields = await fetch(`${baseURL}/v1/memory_stores/${first.id}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(missingFields.status).toBe(200);
    expect(((await missingFields.json()) as ApiMemoryStore).id).toBe(first.id);

    const badDescription = await fetch(`${baseURL}/v1/memory_stores/${first.id}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ description: 42 }),
    });
    expect(badDescription.status).toBe(400);

    const duplicate = await fetch(`${baseURL}/v1/memory_stores/${first.id}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: second.name }),
    });
    expect(duplicate.status).toBe(200);
    expect(((await duplicate.json()) as ApiMemoryStore).name).toBe(second.name);
  });

  it('GET /v1/memory_stores/:id -> 200; cross-workspace -> 404', async () => {
    const name = uniqueName('iso');
    const post = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    const created = (await post.json()) as ApiMemoryStore;

    // Same workspace: 200
    const same = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(same.status).toBe(200);
    const fetched = (await same.json()) as ApiMemoryStore;
    expect(fetched.id).toBe(created.id);

    // Other workspace: 404 (the row exists but is invisible).
    const other = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      headers: { 'x-api-key': otherApiKey },
    });
    expect(other.status).toBe(404);
  });

  it('POST /v1/memory_stores/:id/archive -> 200 with archived_at set', async () => {
    const name = uniqueName('arch');
    const post = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    const created = (await post.json()) as ApiMemoryStore;
    expect(created.archived_at).toBeNull();

    const res = await fetch(`${baseURL}/v1/memory_stores/${created.id}/archive`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(200);
    const archived = (await res.json()) as ApiMemoryStore;
    expect(archived.id).toBe(created.id);
    expect(archived.archived_at).not.toBeNull();
    // The store should still be queryable after archive (archive is a soft
    // marker, distinct from delete).
    const stillThere = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(stillThere.status).toBe(200);
  });

  it('DELETE /v1/memory_stores/:id -> tombstone; subsequent GET -> 404', async () => {
    const name = uniqueName('del');
    const post = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    const created = (await post.json()) as ApiMemoryStore;

    const del = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      method: 'DELETE',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(del.status).toBe(200);
    expect(await del.json()).toEqual({ id: created.id, type: 'memory_store_deleted' });

    const after = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(after.status).toBe(404);
  });

  it('DELETE on missing id -> 404', async () => {
    const del = await fetch(`${baseURL}/v1/memory_stores/mems_does_not_exist`, {
      method: 'DELETE',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(del.status).toBe(404);
  });

  it('archive on missing id -> 404', async () => {
    const res = await fetch(`${baseURL}/v1/memory_stores/mems_does_not_exist/archive`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(404);
  });

  it('Idempotency-Key replay on POST returns the cached response', async () => {
    const name = uniqueName('idem');
    const idemKey = `idem-${randomBytes(8).toString('hex')}`;
    const headers = {
      'x-api-key': apiKey,
      'content-type': 'application/json',
      'idempotency-key': idemKey,
    };
    const body = JSON.stringify({ name });

    const first = await fetch(`${baseURL}/v1/memory_stores`, { method: 'POST', headers, body });
    expect(first.status).toBe(200);
    const created = (await first.json()) as ApiMemoryStore;

    const second = await fetch(`${baseURL}/v1/memory_stores`, { method: 'POST', headers, body });
    expect(second.status).toBe(200);
    const replayed = (await second.json()) as ApiMemoryStore;
    expect(replayed.id).toBe(created.id);
    expect(replayed.name).toBe(created.name);
  });

  it('Idempotency-Key replay on POST /v1/memory_stores/:id returns the cached update', async () => {
    const created = (await (
      await fetch(`${baseURL}/v1/memory_stores`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({ name: uniqueName('idem-upd') }),
      })
    ).json()) as ApiMemoryStore;
    const idemKey = `idem-upd-${randomBytes(8).toString('hex')}`;
    const headers = {
      'x-api-key': apiKey,
      'content-type': 'application/json',
      'idempotency-key': idemKey,
    };
    const body = JSON.stringify({ description: 'cached update' });

    const first = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      method: 'POST',
      headers,
      body,
    });
    expect(first.status).toBe(200);
    const updated = (await first.json()) as ApiMemoryStore;
    expect(updated.description).toBe('cached update');

    const second = await fetch(`${baseURL}/v1/memory_stores/${created.id}`, {
      method: 'POST',
      headers,
      body,
    });
    expect(second.status).toBe(200);
    expect(await second.json()).toEqual(updated);
  });

  describe('memories CRUD with server-derived content metadata', () => {
    interface ApiMemory {
      id: string;
      memory_store_id: string;
      memory_version_id: string;
      path: string;
      content_sha256: string;
      content_size_bytes: number;
      content: string | null;
      type: 'memory';
      created_at: string;
      updated_at: string;
    }

    interface ApiMemoryVersion {
      id: string;
      memory_id: string;
      memory_store_id: string;
      operation: 'created' | 'modified' | 'deleted';
      path: string | null;
      content_sha256: string | null;
      content_size_bytes: number | null;
      content: string | null;
      type: 'memory_version';
      created_at: string;
      redacted_at: string | null;
    }

    function sha256Hex(buf: Buffer): string {
      return createHash('sha256').update(buf).digest('hex');
    }

    async function createStore(prefix: string): Promise<ApiMemoryStore> {
      const res = await fetch(`${baseURL}/v1/memory_stores`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({ name: uniqueName(prefix) }),
      });
      expect(res.status).toBe(200);
      return (await res.json()) as ApiMemoryStore;
    }

    async function postMemory(storeId: string, path: string, content: Buffer): Promise<Response> {
      return fetch(`${baseURL}/v1/memory_stores/${storeId}/memories`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          path: path.startsWith('/') ? path : `/${path}`,
          content: content.toString('utf8'),
        }),
      });
    }

    it('POST /memories creates a memory with the supplied path and content', async () => {
      const store = await createStore('prefs');
      const content = Buffer.from('blue');
      const sha = sha256Hex(content);
      const res = await postMemory(store.id, 'color.txt', content);
      expect(res.status).toBe(200);
      const memory = (await res.json()) as ApiMemory;
      expect(memory.id).toMatch(/^mem_/);
      expect(memory.memory_store_id).toBe(store.id);
      expect(memory.path).toBe('/color.txt');
      expect(memory.content_sha256).toBe(sha);
      expect(memory.content_size_bytes).toBe(4);
      expect(memory.type).toBe('memory');
      expect(memory.content).toBeNull();
    });

    it('POST /memories rejects 413 when content exceeds 100 KB', async () => {
      const store = await createStore('big');
      // 100 KB + 1 byte: just over the cap.
      const content = Buffer.alloc(100 * 1024 + 1, 0x61);
      const res = await postMemory(store.id, 'big.txt', content);
      expect(res.status).toBe(413);
      const body = (await res.json()) as ClaudeError;
      // 100 KB = 102400 bytes; the error should reference the byte cap.
      expect(body.error.message).toMatch(/102400/);
    });

    it('POST /memories derives size and sha256 from UTF-8 content', async () => {
      const store = await createStore('utf8');
      const content = '蓝色 🐋';
      const bytes = Buffer.from(content, 'utf8');
      const res = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({ path: '/utf8.txt', content }),
      });
      expect(res.status).toBe(200);
      const memory = (await res.json()) as ApiMemory;
      expect(memory.content_sha256).toBe(sha256Hex(bytes));
      expect(memory.content_size_bytes).toBe(bytes.length);
    });

    it('POST /memories rejects the removed base64-only request shape', async () => {
      const store = await createStore('legacy-base64');
      const res = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          path: '/legacy.txt',
          content_base64: Buffer.from('legacy').toString('base64'),
          content_sha256: sha256Hex(Buffer.from('legacy')),
        }),
      });
      expect(res.status).toBe(400);
      expect(((await res.json()) as ClaudeError).error.message).toBe('content is required');
    });

    it('POST /memories rejects 400 when path is empty', async () => {
      const store = await createStore('emptypath');
      const content = Buffer.from('x');
      const res = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          path: '',
          content: content.toString('utf8'),
        }),
      });
      expect(res.status).toBe(400);
      const body = (await res.json()) as ClaudeError;
      expect(body.error.message).toMatch(/path/);
    });

    it('POST /memories rejects traversal, empty segments, and oversized paths', async () => {
      const store = await createStore('invalid-path');
      for (const path of ['../secret', 'a//b', 'x'.repeat(1025)]) {
        const res = await postMemory(store.id, path, Buffer.from('x'));
        expect(res.status).toBe(400);
        expect(((await res.json()) as ClaudeError).error.message).toMatch(/path/);
      }

      const list = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
        headers: { 'x-api-key': apiKey },
      });
      expect(list.status).toBe(200);
      expect(((await list.json()) as { data: ApiMemory[] }).data).toEqual([]);
    });

    it('POST /memories rejects 409 when path already exists', async () => {
      const store = await createStore('dup');
      const content = Buffer.from('one');
      const first = await postMemory(store.id, 'note.txt', content);
      expect(first.status).toBe(200);
      const second = await postMemory(store.id, 'note.txt', Buffer.from('two'));
      expect(second.status).toBe(409);
      const body = (await second.json()) as ClaudeError;
      expect(body.error.type).toBe('memory_path_conflict_error');
      expect(body.error.message).toMatch(/already exists/);
    });

    it('POST /memories on non-existent store -> 404', async () => {
      const content = Buffer.from('x');
      const res = await fetch(`${baseURL}/v1/memory_stores/mems_does_not_exist/memories`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          path: '/a.txt',
          content: content.toString('utf8'),
        }),
      });
      expect(res.status).toBe(404);
    });

    it('GET /memories lists the created memories', async () => {
      const store = await createStore('list');
      const c1 = await postMemory(store.id, 'a.txt', Buffer.from('alpha'));
      expect(c1.status).toBe(200);
      const c2 = await postMemory(store.id, 'b.txt', Buffer.from('beta'));
      expect(c2.status).toBe(200);

      const res = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
        headers: { 'x-api-key': apiKey },
      });
      expect(res.status).toBe(200);
      const page = (await res.json()) as { data: ApiMemory[]; next_page: string | null };
      const paths = page.data.map((m) => m.path).sort();
      expect(paths).toEqual(['/a.txt', '/b.txt']);
      expect(page.next_page).toBeNull();
      expect(page).not.toHaveProperty('memories');
    });

    it('GET /memories paginates with page and next_page', async () => {
      const store = await createStore('mem-page');
      for (const path of ['A.txt', 'a.txt', 'B.txt']) {
        const created = await postMemory(store.id, path, Buffer.from(path));
        expect(created.status).toBe(200);
      }

      const paths: string[] = [];
      let page: string | null = null;
      do {
        const res = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memories?limit=1${
            page ? `&page=${encodeURIComponent(page)}` : ''
          }`,
          { headers: { 'x-api-key': apiKey } },
        );
        expect(res.status).toBe(200);
        const body = (await res.json()) as { data: ApiMemory[]; next_page: string | null };
        expect(body.data).toHaveLength(1);
        paths.push(body.data[0]!.path);
        page = body.next_page;
      } while (page);

      expect(paths).toEqual(['/A.txt', '/B.txt', '/a.txt']);
    });

    it('GET /memories on non-existent store -> 404', async () => {
      const res = await fetch(`${baseURL}/v1/memory_stores/mems_does_not_exist/memories`, {
        headers: { 'x-api-key': apiKey },
      });
      expect(res.status).toBe(404);
    });

    it('GET /memories/:memory_id returns the memory', async () => {
      const store = await createStore('getone');
      const created = (await (
        await postMemory(store.id, 'pref.txt', Buffer.from('dark'))
      ).json()) as ApiMemory;

      const res = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}`, {
        headers: { 'x-api-key': apiKey },
      });
      expect(res.status).toBe(200);
      const fetched = (await res.json()) as ApiMemory;
      expect(fetched.id).toBe(created.id);
      expect(fetched.path).toBe('/pref.txt');
      expect(fetched.content_sha256).toBe(created.content_sha256);
    });

    it('GET /memories/:memory_id on missing id -> 404', async () => {
      const store = await createStore('missing');
      const res = await fetch(
        `${baseURL}/v1/memory_stores/${store.id}/memories/mem_does_not_exist`,
        { headers: { 'x-api-key': apiKey } },
      );
      expect(res.status).toBe(404);
    });

    it('GET /memories/:memory_id?view=full includes the content', async () => {
      const store = await createStore('content');
      const content = Buffer.from('some content bytes');
      const created = (await (await postMemory(store.id, 'data.bin', content)).json()) as ApiMemory;

      const res = await fetch(
        `${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}?view=full`,
        { headers: { 'x-api-key': apiKey } },
      );
      expect(res.status).toBe(200);
      expect(((await res.json()) as ApiMemory).content).toBe(content.toString('utf8'));
    });

    it('GET /memories/:memory_id?view=full on missing id -> 404', async () => {
      const store = await createStore('missing-content');
      const res = await fetch(
        `${baseURL}/v1/memory_stores/${store.id}/memories/mem_does_not_exist?view=full`,
        { headers: { 'x-api-key': apiKey } },
      );
      expect(res.status).toBe(404);
    });

    it('POST with precondition matches -> 200 with new sha', async () => {
      const store = await createStore('cas-ok');
      const c0 = Buffer.from('v0');
      const sha0 = sha256Hex(c0);
      const created = (await (await postMemory(store.id, 'cas.txt', c0)).json()) as ApiMemory;
      expect(created.content_sha256).toBe(sha0);

      const c1 = Buffer.from('v1');
      const sha1 = sha256Hex(c1);
      const res = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          content: c1.toString('utf8'),
          precondition: { type: 'content_sha256', content_sha256: sha0 },
        }),
      });
      expect(res.status).toBe(200);
      const updated = (await res.json()) as ApiMemory;
      expect(updated.content_sha256).toBe(sha1);
      expect(updated.content_size_bytes).toBe(c1.length);
    });

    it('POST with precondition mismatch -> 409 with both shas', async () => {
      const store = await createStore('cas-conflict');
      const cA = Buffer.from('A');
      const shaA = sha256Hex(cA);
      const created = (await (await postMemory(store.id, 'cas.txt', cA)).json()) as ApiMemory;
      expect(created.content_sha256).toBe(shaA);

      // Move forward: shaA -> shaB.
      const cB = Buffer.from('B');
      const ok = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          content: cB.toString('utf8'),
          precondition: { type: 'content_sha256', content_sha256: shaA },
        }),
      });
      expect(ok.status).toBe(200);

      // Stale precondition: caller still thinks the value is shaA, but the
      // current state is shaB. POST must surface the conflict in the 409.
      const cC = Buffer.from('C');
      const conflict = await fetch(
        `${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}`,
        {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            content: cC.toString('utf8'),
            precondition: { type: 'content_sha256', content_sha256: shaA },
          }),
        },
      );
      expect(conflict.status).toBe(409);
      const body = (await conflict.json()) as ClaudeError;
      expect(body.error.type).toBe('memory_precondition_failed_error');
    });

    it('POST without precondition -> 200 (last-writer-wins)', async () => {
      const store = await createStore('lww');
      const c0 = Buffer.from('first');
      const created = (await (await postMemory(store.id, 'lww.txt', c0)).json()) as ApiMemory;

      // No precondition: even if someone else wrote between create and now,
      // this POST wins.
      const c1 = Buffer.from('second');
      const sha1 = sha256Hex(c1);
      const res = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          content: c1.toString('utf8'),
        }),
      });
      expect(res.status).toBe(200);
      const updated = (await res.json()) as ApiMemory;
      expect(updated.content_sha256).toBe(sha1);
    });

    it('POST on missing memory_id -> 404', async () => {
      const store = await createStore('update-missing');
      const c = Buffer.from('x');
      const res = await fetch(
        `${baseURL}/v1/memory_stores/${store.id}/memories/mem_does_not_exist`,
        {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            content: c.toString('utf8'),
          }),
        },
      );
      expect(res.status).toBe(404);
    });

    it('POST oversize content -> 413', async () => {
      const store = await createStore('update-big');
      const created = (await (
        await postMemory(store.id, 'p.txt', Buffer.from('small'))
      ).json()) as ApiMemory;
      const big = Buffer.alloc(100 * 1024 + 1, 0x62);
      const res = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          content: big.toString('utf8'),
        }),
      });
      expect(res.status).toBe(413);
    });

    it('DELETE /memories/:memory_id removes live memory but preserves versions', async () => {
      const store = await createStore('del-mem');
      const created = (await (
        await postMemory(store.id, 'd.txt', Buffer.from('x'))
      ).json()) as ApiMemory;

      const ok = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}`, {
        method: 'DELETE',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({}),
      });
      expect(ok.status).toBe(200);
      expect(await ok.json()).toEqual({ id: created.id, type: 'memory_deleted' });

      const getDeleted = await fetch(
        `${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}`,
        { headers: { 'x-api-key': apiKey } },
      );
      expect(getDeleted.status).toBe(404);

      const versions = await fetch(
        `${baseURL}/v1/memory_stores/${store.id}/memory_versions?memory_id=${created.id}`,
        { headers: { 'x-api-key': apiKey } },
      );
      expect(versions.status).toBe(200);
      expect(
        ((await versions.json()) as { data: ApiMemoryVersion[]; next_page: string | null }).data,
      ).toHaveLength(2);

      const missing = await fetch(
        `${baseURL}/v1/memory_stores/${store.id}/memories/mem_does_not_exist`,
        {
          method: 'DELETE',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({}),
        },
      );
      expect(missing.status).toBe(404);
    });

    it('cross-workspace memory access -> 404', async () => {
      const store = await createStore('iso');
      const created = (await (
        await postMemory(store.id, 'private.txt', Buffer.from('secret'))
      ).json()) as ApiMemory;

      // Same workspace: 200.
      const same = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}`, {
        headers: { 'x-api-key': apiKey },
      });
      expect(same.status).toBe(200);

      // Other workspace: 404 (the row exists but the store is invisible).
      const other = await fetch(
        `${baseURL}/v1/memory_stores/${store.id}/memories/${created.id}?view=full`,
        { headers: { 'x-api-key': otherApiKey } },
      );
      expect(other.status).toBe(404);
    });

    describe('memory_versions list + redact', () => {
      it('GET /memory_versions?memory_id= returns versions ordered DESC', async () => {
        const store = await createStore('versions-list');
        // Create memory at v1.
        const v1 = sha256Hex(Buffer.from('v1'));
        const createRes = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            path: '/note.txt',
            content: 'v1',
          }),
        });
        expect(createRes.status).toBe(200);
        const memory = (await createRes.json()) as { id: string };
        // Update to v2.
        await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories/${memory.id}`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            content: 'v2',
          }),
        });
        // Update to v3.
        const v3 = sha256Hex(Buffer.from('v3'));
        await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories/${memory.id}`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            content: 'v3',
          }),
        });
        // List versions.
        const listRes = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions?memory_id=${memory.id}`,
          { headers: { 'x-api-key': apiKey } },
        );
        expect(listRes.status).toBe(200);
        const body = (await listRes.json()) as {
          data: Array<{
            content_sha256: string | null;
            created_at: string;
            redacted_at: string | null;
          }>;
        };
        expect(body.data).toHaveLength(3);
        // First in the list is the latest (v3).
        expect(body.data[0]!.content_sha256).toBe(v3);
        expect(body.data[2]!.content_sha256).toBe(v1);
        // All are non-redacted.
        expect(body.data.every((v) => v.redacted_at === null)).toBe(true);
      });

      it('GET /memory_versions without memory_id returns merged DESC across all memories in the store', async () => {
        const store = await createStore('merged-versions');
        // Create two memories so we can verify cross-memory merge ordering.
        await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            path: '/a.txt',
            content: 'a',
          }),
        });
        await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            path: '/b.txt',
            content: 'b',
          }),
        });
        const listRes = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memory_versions`, {
          headers: { 'x-api-key': apiKey },
        });
        expect(listRes.status).toBe(200);
        const body = (await listRes.json()) as { data: Array<{ created_at: string }> };
        expect(body.data.length).toBeGreaterThanOrEqual(2);
        // Verify DESC ordering across the merged list.
        for (let i = 0; i < body.data.length - 1; i++) {
          expect(body.data[i]!.created_at >= body.data[i + 1]!.created_at).toBe(true);
        }
      });

      it('GET /memory_versions rejects non-positive and non-numeric limits', async () => {
        const store = await createStore('versions-bad-limit');
        for (const limit of ['0', 'abc']) {
          const res = await fetch(
            `${baseURL}/v1/memory_stores/${store.id}/memory_versions?limit=${limit}`,
            { headers: { 'x-api-key': apiKey } },
          );
          expect(res.status).toBe(400);
          expect(((await res.json()) as ClaudeError).error.message).toBe(
            'limit must be a positive integer',
          );
        }
      });

      it('GET /memory_versions paginates with page and next_page', async () => {
        const store = await createStore('versions-page');
        const initial = await postMemory(store.id, 'page.txt', Buffer.from('one'));
        expect(initial.status).toBe(200);
        const memory = (await initial.json()) as ApiMemory;

        const updated = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memories/${memory.id}`,
          {
            method: 'POST',
            headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
            body: JSON.stringify({
              content: 'two',
            }),
          },
        );
        expect(updated.status).toBe(200);

        const first = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions?memory_id=${memory.id}&limit=1`,
          { headers: { 'x-api-key': apiKey } },
        );
        expect(first.status).toBe(200);
        const firstBody = (await first.json()) as {
          data: ApiMemoryVersion[];
          next_page: string | null;
        };
        expect(firstBody.data).toHaveLength(1);
        expect(firstBody.next_page).toBeTruthy();

        const second = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions?memory_id=${
            memory.id
          }&limit=1&page=${encodeURIComponent(firstBody.next_page!)}`,
          { headers: { 'x-api-key': apiKey } },
        );
        expect(second.status).toBe(200);
        const secondBody = (await second.json()) as { data: ApiMemoryVersion[] };
        expect(secondBody.data).toHaveLength(1);
        expect(secondBody.data[0]!.id).not.toBe(firstBody.data[0]!.id);
      });

      it('POST /memory_versions/:version_id/redact sets redacted_at and clears the version blob', async () => {
        const store = await createStore('redact-target');
        const createRes = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            path: '/hello.txt',
            content: 'hello',
          }),
        });
        const memory = (await createRes.json()) as { id: string };
        const listRes = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions?memory_id=${memory.id}`,
          { headers: { 'x-api-key': apiKey } },
        );
        const versions = ((await listRes.json()) as { data: Array<{ id: string }> }).data;
        expect(versions).toHaveLength(1);
        const versionId = versions[0]!.id;

        const redactRes = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions/${versionId}/redact`,
          {
            method: 'POST',
            headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
            body: '{}',
          },
        );
        expect(redactRes.status).toBe(200);
        const redacted = (await redactRes.json()) as { id: string; redacted_at: string | null };
        expect(redacted.id).toBe(versionId);
        expect(redacted.redacted_at).not.toBeNull();
      });

      it('GET /memory_versions/:version_id returns direct version metadata', async () => {
        const store = await createStore('version-get');
        const sha = sha256Hex(Buffer.from('direct'));
        const createRes = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            path: '/direct.txt',
            content: 'direct',
          }),
        });
        const memory = (await createRes.json()) as { id: string };
        const listRes = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions?memory_id=${memory.id}`,
          { headers: { 'x-api-key': apiKey } },
        );
        const version = ((await listRes.json()) as { data: Array<{ id: string }> }).data[0]!;

        const get = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions/${version.id}`,
          { headers: { 'x-api-key': apiKey } },
        );
        expect(get.status).toBe(200);
        expect(await get.json()).toMatchObject({
          id: version.id,
          content_sha256: sha,
          redacted_at: null,
        });

        const otherStore = await createStore('version-cross-store');
        const crossStore = await fetch(
          `${baseURL}/v1/memory_stores/${otherStore.id}/memory_versions/${version.id}`,
          { headers: { 'x-api-key': apiKey } },
        );
        expect(crossStore.status).toBe(404);

        const crossWorkspace = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions/${version.id}`,
          { headers: { 'x-api-key': otherApiKey } },
        );
        expect(crossWorkspace.status).toBe(404);
      });

      it('GET /memory_versions/:version_id returns redacted metadata', async () => {
        const store = await createStore('version-get-redacted');
        const createRes = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            path: '/redact-direct.txt',
            content: 'redact-direct',
          }),
        });
        const memory = (await createRes.json()) as { id: string };
        const listRes = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions?memory_id=${memory.id}`,
          { headers: { 'x-api-key': apiKey } },
        );
        const versionId = ((await listRes.json()) as { data: Array<{ id: string }> }).data[0]!.id;
        const redact = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions/${versionId}/redact`,
          {
            method: 'POST',
            headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
            body: '{}',
          },
        );
        expect(redact.status).toBe(200);

        const get = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions/${versionId}`,
          { headers: { 'x-api-key': apiKey } },
        );
        expect(get.status).toBe(200);
        expect(((await get.json()) as { redacted_at: string | null }).redacted_at).not.toBeNull();
      });

      it('redact is idempotent: second call returns the same redacted_at timestamp', async () => {
        const store = await createStore('redact-idempotent');
        const createRes = await fetch(`${baseURL}/v1/memory_stores/${store.id}/memories`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            path: '/r.txt',
            content: 'once',
          }),
        });
        const memory = (await createRes.json()) as { id: string };
        const listRes = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions?memory_id=${memory.id}`,
          { headers: { 'x-api-key': apiKey } },
        );
        const versionId = ((await listRes.json()) as { data: Array<{ id: string }> }).data[0]!.id;
        const r1 = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions/${versionId}/redact`,
          {
            method: 'POST',
            headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
            body: '{}',
          },
        );
        const r1Body = (await r1.json()) as { redacted_at: string };
        const r2 = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions/${versionId}/redact`,
          {
            method: 'POST',
            headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
            body: '{}',
          },
        );
        expect(r2.status).toBe(200);
        const r2Body = (await r2.json()) as { redacted_at: string };
        expect(r2Body.redacted_at).toBe(r1Body.redacted_at);
      });

      it('redact on missing version_id -> 404', async () => {
        const store = await createStore('redact-missing');
        const res = await fetch(
          `${baseURL}/v1/memory_stores/${store.id}/memory_versions/memver_nonexistent000/redact`,
          {
            method: 'POST',
            headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
            body: '{}',
          },
        );
        expect(res.status).toBe(404);
      });

      it('list versions on missing store -> 404', async () => {
        const res = await fetch(`${baseURL}/v1/memory_stores/mems_nonexistent000/memory_versions`, {
          headers: { 'x-api-key': apiKey },
        });
        expect(res.status).toBe(404);
      });

      it('list versions with memory_id from a different store -> empty page', async () => {
        const store1 = await createStore('versions-store-1');
        const store2 = await createStore('versions-store-2');
        const createRes = await fetch(`${baseURL}/v1/memory_stores/${store2.id}/memories`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({
            path: '/x.txt',
            content: 'x',
          }),
        });
        const memory = (await createRes.json()) as { id: string };
        // The filter is scoped to store1, so a memory ID from store2 yields an
        // empty page without disclosing whether that foreign ID exists.
        const res = await fetch(
          `${baseURL}/v1/memory_stores/${store1.id}/memory_versions?memory_id=${memory.id}`,
          { headers: { 'x-api-key': apiKey } },
        );
        expect(res.status).toBe(200);
        expect(((await res.json()) as { data: ApiMemoryVersion[] }).data).toEqual([]);
      });
    });
  });
});

function metadataPairs(count: number): Record<string, string> {
  return Object.fromEntries(
    Array.from({ length: count }, (_, index) => [`key_${index}`, `value_${index}`]),
  );
}

function metadataWithReservedKeys(protoValue: string): Record<string, string> {
  const metadata = Object.create(null) as Record<string, string>;
  metadata['__proto__'] = protoValue;
  metadata['constructor'] = 'literal-constructor';
  return metadata;
}
