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
  id: string;
  type: string;
  file_id: string | null;
  memory_store_id: string | null;
  mount_path: string;
  access: string;
  mount_strategy: string | null;
  instructions: string | null;
  created_at: string;
  updated_at: string;
}

interface SessionOut {
  id: string;
  resources: SessionResourceOut[];
}

interface ClaudeErrorOut {
  type: 'error';
  error: { type: string; message: string };
  request_id: string | null;
}

describe('session_resources mount_strategy (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let environmentId: string;

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
    apiKey = await createTestApiKey(db, uniqueWorkspace('sesrsc_ms'));
    environmentId = await createTestEnvironment(baseURL, apiKey);
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestMemoryStore();
    await closeTestFileStore();
    await closeTestDb();
  });

  async function uploadFile(name: string, body: string): Promise<string> {
    const f = new FormData();
    f.append('file', new Blob([body], { type: 'text/plain' }), name);
    const res = await fetch(`${baseURL}/v1/files`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey },
      body: f,
    });
    expect(res.status).toBe(200);
    const file = (await res.json()) as { id: string };
    return file.id;
  }

  it('persists per-resource mount_strategy on create; null when omitted', async () => {
    const fileA = await uploadFile('a.txt', `aaa-${Date.now()}-${Math.random()}`);
    const fileB = await uploadFile('b.txt', `bbb-${Date.now()}-${Math.random()}`);
    const agentId = await createTestAgent(baseURL, apiKey);

    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          {
            type: 'file',
            file_id: fileA,
            mount_path: '/mnt/a.txt',
            access: 'read_only',
            mount_strategy: 'tarball_prefetch',
          },
          // No override: the row stays null and the runtime uses its tarball default.
          { type: 'file', file_id: fileB, mount_path: '/mnt/b.txt', access: 'read_only' },
        ],
      }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as SessionOut;

    const get = await fetch(`${baseURL}/v1/sessions/${session.id}`, {
      headers: { 'x-api-key': apiKey, 'orca-beta': '1' },
    });
    expect(get.status).toBe(200);
    const fetched = (await get.json()) as SessionOut;
    expect(fetched.resources).toHaveLength(2);

    const byFile = new Map(fetched.resources.map((r) => [r.file_id, r]));
    expect(byFile.get(fileA)!.mount_strategy).toBe('tarball_prefetch');
    expect(byFile.get(fileB)!.mount_strategy).toBeNull();
  }, 30000);

  it('persists mount_strategy on POST /v1/sessions/:id/resources (attach)', async () => {
    const file1 = await uploadFile('c.txt', `ccc-${Date.now()}-${Math.random()}`);
    const file2 = await uploadFile('d.txt', `ddd-${Date.now()}-${Math.random()}`);
    const agentId = await createTestAgent(baseURL, apiKey);

    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          { type: 'file', file_id: file1, mount_path: '/mnt/c.txt', access: 'read_only' },
        ],
      }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as SessionOut;

    const attach = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': '1',
      },
      body: JSON.stringify({
        type: 'file',
        file_id: file2,
        mount_path: '/mnt/d.txt',
        access: 'read_only',
        mount_strategy: 'tarball_prefetch',
      }),
    });
    expect(attach.status).toBe(200);
    const attached = (await attach.json()) as SessionResourceOut;
    expect(attached.mount_strategy).toBe('tarball_prefetch');
    expect(attached.created_at).toEqual(expect.any(String));
    expect(attached.updated_at).toEqual(expect.any(String));

    const get = await fetch(`${baseURL}/v1/sessions/${session.id}`, {
      headers: { 'x-api-key': apiKey, 'orca-beta': '1' },
    });
    const fetched = (await get.json()) as SessionOut;
    expect(fetched.resources).toHaveLength(2);
    const byFile = new Map(fetched.resources.map((r) => [r.file_id, r]));
    // file1 had no override → null
    expect(byFile.get(file1)!.mount_strategy).toBeNull();
    // file2 attached with tarball_prefetch
    expect(byFile.get(file2)!.mount_strategy).toBe('tarball_prefetch');
  }, 30000);

  it('memory_store resources never carry mount_strategy', async () => {
    // memory_store has its own strategy mechanism; mount_strategy is
    // file-only. Per the route contract: rows for non-file resources
    // always serialize mount_strategy as null in the response.
    const agentId = await createTestAgent(baseURL, apiKey);

    // Create a real memory_store first — `sessions.routes.ts` validates
    // memory_store_id against the workspace, so a synthetic ID gets rejected
    // with 400.
    const storeName = `prefs-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const createStore = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: storeName }),
    });
    expect(createStore.status).toBe(200);
    const storeId = ((await createStore.json()) as { id: string }).id;

    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          // Registry derives mount_path; access defaults to read_write.
          {
            type: 'memory_store',
            memory_store_id: storeId,
          },
        ],
      }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as SessionOut;

    const get = await fetch(`${baseURL}/v1/sessions/${session.id}`, {
      headers: { 'x-api-key': apiKey, 'orca-beta': '1' },
    });
    const fetched = (await get.json()) as SessionOut;
    expect(fetched.resources).toHaveLength(1);
    const memRsc = fetched.resources[0]!;
    expect(memRsc.type).toBe('memory_store');
    expect(memRsc.memory_store_id).toBe(storeId);
    // Always null for non-file resources — consistent serialization shape
    // means the harness factory has one fewer null-vs-undefined distinction
    // to handle.
    expect(memRsc.mount_strategy).toBeNull();
  }, 30000);

  it.each(['s3_fuse', 'nonsense'])(
    'rejects unsupported mount_strategy %s (400)',
    async (strategy) => {
      const fileId = await uploadFile('bad.txt', `bad-${Date.now()}-${Math.random()}`);
      const agentId = await createTestAgent(baseURL, apiKey);
      const res = await fetch(`${baseURL}/v1/sessions`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          environment_id: environmentId,
          agent_id: agentId,
          resources: [
            {
              type: 'file',
              file_id: fileId,
              mount_path: '/mnt/bad.txt',
              access: 'read_only',
              mount_strategy: strategy,
            },
          ],
        }),
      });
      expect(res.status).toBe(400);
      const err = (await res.json()) as ClaudeErrorOut;
      expect(err.type).toBe('error');
      expect(err.error.type).toBe('invalid_request_error');
      expect(err.error.message).toMatch(/mount_strategy/);
      expect(err.request_id).toEqual(expect.any(String));
    },
    30000,
  );
});
