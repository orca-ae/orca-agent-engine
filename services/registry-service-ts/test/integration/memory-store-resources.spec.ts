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
  buildTestMemoryStore,
  closeTestMemoryStore,
} from './setup.js';
import {
  uniqueWorkspace,
  createTestApiKey,
  createTestAgent,
  createTestEnvironment,
} from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

interface SessionResourceOut {
  id?: string;
  type: string;
  file_id?: string | null;
  memory_store_id: string | null;
  mount_path: string;
  access: string;
  mount_strategy?: string | null;
  instructions: string | null;
}

interface SessionOut {
  id: string;
  resources: SessionResourceOut[];
}

interface ApiMemoryStore {
  id: string;
  name: string;
}

interface ClaudeErrorOut {
  type: 'error';
  error: { type: string; message: string };
  request_id: string | null;
}

describe('session resources: memory_store (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let agentId: string;
  let environmentId: string;
  let storeId: string;
  let storeName: string;

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
    workspaceId = uniqueWorkspace('memresources');
    apiKey = await createTestApiKey(db, workspaceId);
    agentId = await createTestAgent(baseURL, apiKey);
    environmentId = await createTestEnvironment(baseURL, apiKey);

    storeName = `prefs-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const createStoreRes = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: storeName }),
    });
    expect(createStoreRes.status).toBe(200);
    storeId = ((await createStoreRes.json()) as ApiMemoryStore).id;
  }, 60_000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestMemoryStore();
    await closeTestFileStore();
    await closeTestDb();
  });

  it('attach memory_store at session create with default mount_path', async () => {
    const res = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [{ type: 'memory_store', memory_store_id: storeId, access: 'read_write' }],
      }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as SessionOut;
    const memResource = body.resources.find((r) => r.type === 'memory_store');
    expect(memResource).toBeDefined();
    expect(memResource!.memory_store_id).toBe(storeId);
    expect(memResource!.mount_path).toBe(`/mnt/memory/${storeName}/`);
    expect(memResource!.access).toBe('read_write');
    // File-only fields are omitted from Claude's discriminated memory-store
    // projection rather than emitted as null.
    expect(memResource).not.toHaveProperty('file_id');
    expect(memResource).not.toHaveProperty('mount_strategy');
  }, 30_000);

  it('falls back to the stable store id when the store name is not a safe path segment', async () => {
    const createStore = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: '../unsafe-name' }),
    });
    expect(createStore.status).toBe(200);
    const unsafeNameStoreId = ((await createStore.json()) as ApiMemoryStore).id;

    const res = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [{ type: 'memory_store', memory_store_id: unsafeNameStoreId }],
      }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as SessionOut;
    expect(body.resources[0]!.mount_path).toBe(`/mnt/memory/${unsafeNameStoreId}/`);
  }, 30_000);

  it('rejects caller-supplied memory_store mount_path on session create', async () => {
    const res = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          {
            type: 'memory_store',
            memory_store_id: storeId,
            mount_path: '/mnt/custom/',
            access: 'read_only',
          },
        ],
      }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as ClaudeErrorOut;
    expect(body.type).toBe('error');
    expect(body.error.type).toBe('invalid_request_error');
    expect(body.error.message).toMatch(/mount_path/i);
    expect(body.request_id).toEqual(expect.any(String));
  }, 30_000);

  it('rejects colliding default memory mount paths on create and runtime attach', async () => {
    const duplicateName = `duplicate-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const duplicateStoreIds: string[] = [];
    for (let index = 0; index < 2; index++) {
      const createStore = await fetch(`${baseURL}/v1/memory_stores`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({ name: duplicateName }),
      });
      expect(createStore.status).toBe(200);
      duplicateStoreIds.push(((await createStore.json()) as ApiMemoryStore).id);
    }

    const createWithBoth = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: duplicateStoreIds.map((memoryStoreId) => ({
          type: 'memory_store',
          memory_store_id: memoryStoreId,
        })),
      }),
    });
    expect(createWithBoth.status).toBe(400);
    expect((await createWithBoth.json()) as ClaudeErrorOut).toMatchObject({
      type: 'error',
      error: {
        type: 'invalid_request_error',
        message: 'session resources must use unique mount_path values',
      },
      request_id: expect.any(String),
    });

    const createSession = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ environment_id: environmentId, agent_id: agentId }),
    });
    expect(createSession.status).toBe(200);
    const session = (await createSession.json()) as SessionOut;
    for (let index = 0; index < duplicateStoreIds.length; index++) {
      const attach = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          type: 'memory_store',
          memory_store_id: duplicateStoreIds[index],
        }),
      });
      expect(attach.status).toBe(index === 0 ? 200 : 400);
      if (index === 1) {
        expect((await attach.json()) as ClaudeErrorOut).toMatchObject({
          type: 'error',
          error: {
            type: 'invalid_request_error',
            message: 'session resources must use unique mount_path values',
          },
          request_id: expect.any(String),
        });
      }
    }

    const concurrentSessionResponse = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ environment_id: environmentId, agent_id: agentId }),
    });
    expect(concurrentSessionResponse.status).toBe(200);
    const concurrentSession = (await concurrentSessionResponse.json()) as SessionOut;
    const concurrentAttaches = await Promise.all(
      duplicateStoreIds.map((memoryStoreId) =>
        fetch(`${baseURL}/v1/sessions/${concurrentSession.id}/resources`, {
          method: 'POST',
          headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
          body: JSON.stringify({ type: 'memory_store', memory_store_id: memoryStoreId }),
        }),
      ),
    );
    expect(concurrentAttaches.map((response) => response.status).sort()).toEqual([200, 400]);
    const rejectedConcurrentAttach = concurrentAttaches.find(
      (response) => response.status === 400,
    )!;
    expect((await rejectedConcurrentAttach.json()) as ClaudeErrorOut).toMatchObject({
      type: 'error',
      error: {
        type: 'invalid_request_error',
        message: 'session resources must use unique mount_path values',
      },
      request_id: expect.any(String),
    });
  }, 30_000);

  it('attach memory_store at runtime via POST /resources', async () => {
    const sessionRes = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ environment_id: environmentId, agent_id: agentId }),
    });
    expect(sessionRes.status).toBe(200);
    const session = (await sessionRes.json()) as SessionOut;

    const attachRes = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        type: 'memory_store',
        memory_store_id: storeId,
        access: 'read_write',
      }),
    });
    expect(attachRes.status).toBe(200);
    const resource = (await attachRes.json()) as SessionResourceOut;
    expect(resource.type).toBe('memory_store');
    expect(resource.memory_store_id).toBe(storeId);
    expect(resource.mount_path).toBe(`/mnt/memory/${storeName}/`);
    expect(resource.access).toBe('read_write');

    // GET the session afterwards: the resource is in the list.
    const get = await fetch(`${baseURL}/v1/sessions/${session.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(get.status).toBe(200);
    const fetched = (await get.json()) as SessionOut;
    const m = fetched.resources.find((r) => r.id === resource.id);
    expect(m).toBeDefined();
    expect(m!.memory_store_id).toBe(storeId);
  }, 30_000);

  it('rejects caller-supplied memory mount paths on attach and update', async () => {
    const sessionRes = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': 'session-resource-extensions',
      },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [{ type: 'memory_store', memory_store_id: storeId }],
      }),
    });
    expect(sessionRes.status).toBe(200);
    const session = (await sessionRes.json()) as SessionOut;
    const resource = session.resources.find((item) => item.type === 'memory_store')!;
    expect(resource.id).toEqual(expect.any(String));

    const attach = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        type: 'memory_store',
        memory_store_id: storeId,
        mount_path: '/mnt/memory/alternate/',
      }),
    });
    expect(attach.status).toBe(400);
    const attachError = (await attach.json()) as ClaudeErrorOut;
    expect(attachError.type).toBe('error');
    expect(attachError.error.type).toBe('invalid_request_error');
    expect(attachError.error.message).toMatch(/mount_path/);
    expect(attachError.request_id).toEqual(expect.any(String));

    const update = await fetch(`${baseURL}/v1/sessions/${session.id}/resources/${resource.id!}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ mount_path: '/mnt/memory/renamed/' }),
    });
    expect(update.status).toBe(400);
    const updateError = (await update.json()) as ClaudeErrorOut;
    expect(updateError.type).toBe('error');
    expect(updateError.error.type).toBe('invalid_request_error');
    expect(updateError.error.message).toMatch(/mount_path/);
    expect(updateError.request_id).toEqual(expect.any(String));
  }, 30_000);

  it('rejects memory_store_id from a different workspace with 400', async () => {
    const otherWorkspace = uniqueWorkspace('otherws');
    const { db } = await getTestDb();
    const otherKey = await createTestApiKey(db, otherWorkspace);
    const otherStoreRes = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': otherKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        name: `other-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      }),
    });
    expect(otherStoreRes.status).toBe(200);
    const otherStoreId = ((await otherStoreRes.json()) as ApiMemoryStore).id;

    // Attempt to reference the other workspace's memory_store from the
    // primary workspace's session — must surface as 400 (workspace boundary).
    const res = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [{ type: 'memory_store', memory_store_id: otherStoreId, access: 'read_write' }],
      }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as ClaudeErrorOut;
    expect(body.type).toBe('error');
    expect(body.error.type).toBe('invalid_request_error');
    expect(body.error.message).toMatch(/memory_store/i);
    expect(body.error.message).toMatch(/not found in workspace/i);
    expect(body.request_id).toEqual(expect.any(String));
  }, 30_000);

  it('rejects creating a session with > 8 memory_store resources (400)', async () => {
    // Create 9 stores then try to attach all 9 in one session.
    const stores: string[] = [];
    for (let i = 0; i < 9; i++) {
      const s = await fetch(`${baseURL}/v1/memory_stores`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          name: `cap-${Date.now()}-${i}-${Math.random().toString(36).slice(2, 6)}`,
        }),
      });
      expect(s.status).toBe(200);
      stores.push(((await s.json()) as ApiMemoryStore).id);
    }
    const res = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: stores.map((id) => ({
          type: 'memory_store',
          memory_store_id: id,
          access: 'read_write',
        })),
      }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as ClaudeErrorOut;
    expect(body.type).toBe('error');
    expect(body.error.type).toBe('invalid_request_error');
    expect(body.error.message).toMatch(/8/);
    expect(body.request_id).toEqual(expect.any(String));
  }, 60_000);

  it('rejects attaching a 9th memory_store at runtime (400)', async () => {
    // Create 8 stores and attach all of them at create.
    const stores: string[] = [];
    for (let i = 0; i < 8; i++) {
      const s = await fetch(`${baseURL}/v1/memory_stores`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          name: `attach-cap-${Date.now()}-${i}-${Math.random().toString(36).slice(2, 6)}`,
        }),
      });
      expect(s.status).toBe(200);
      stores.push(((await s.json()) as ApiMemoryStore).id);
    }
    const sessionRes = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: stores.map((id) => ({
          type: 'memory_store',
          memory_store_id: id,
          access: 'read_write',
        })),
      }),
    });
    expect(sessionRes.status).toBe(200);
    const session = (await sessionRes.json()) as SessionOut;
    expect(session.resources.filter((r) => r.type === 'memory_store')).toHaveLength(8);

    // The 9th attach must fail with 400.
    const ninth = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        name: `ninth-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      }),
    });
    const ninthId = ((await ninth.json()) as ApiMemoryStore).id;

    const attach = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        type: 'memory_store',
        memory_store_id: ninthId,
        access: 'read_write',
      }),
    });
    expect(attach.status).toBe(400);
    const body = (await attach.json()) as ClaudeErrorOut;
    expect(body.type).toBe('error');
    expect(body.error.type).toBe('invalid_request_error');
    expect(body.error.message).toMatch(/8/);
    expect(body.request_id).toEqual(expect.any(String));
  }, 60_000);

  it('persists `instructions` field through round-trip', async () => {
    const instructions = 'remember user color preferences here';
    const res = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          {
            type: 'memory_store',
            memory_store_id: storeId,
            access: 'read_write',
            instructions,
          },
        ],
      }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as SessionOut;
    const m = body.resources.find((r) => r.type === 'memory_store');
    expect(m).toBeDefined();
    expect(m!.instructions).toBe(instructions);

    // GET round-trip also returns it.
    const get = await fetch(`${baseURL}/v1/sessions/${body.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(get.status).toBe(200);
    const getBody = (await get.json()) as SessionOut;
    const m2 = getBody.resources.find((r) => r.type === 'memory_store');
    expect(m2).toBeDefined();
    expect(m2!.instructions).toBe(instructions);
  }, 30_000);

  it('rejects memory_store resource without memory_store_id (400)', async () => {
    const res = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [{ type: 'memory_store', access: 'read_write' }],
      }),
    });
    // Either ts-rest's discriminated-union validation rejects it (4xx), or the
    // route's runtime check fires. Either way the response should be a 4xx.
    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(res.status).toBeLessThan(500);
  }, 30_000);
});
