// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { gitCredentials } from '../../src/persistence/postgres/schema.js';
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
  createTestEnvironment,
} from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

describe('/v1/sessions/:id/resources (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let db: DbClient;
  let workspaceId: string;
  let environmentId: string;

  beforeAll(async () => {
    ({ db } = await getTestDb());
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
    workspaceId = uniqueWorkspace('sesrsc');
    apiKey = await createTestApiKey(db, workspaceId);
    environmentId = await createTestEnvironment(baseURL, apiKey);
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
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

  it('rejects malformed add bodies with the canonical public error envelope', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ environment_id: environmentId, agent_id: agentId }),
    });
    const session = (await create.json()) as { id: string };

    const invalid = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'file', file_id: 'file_missing', unknown_field: true }),
    });

    expect(invalid.status).toBe(400);
    expect(await invalid.json()).toMatchObject({
      type: 'error',
      error: { type: 'invalid_request_error' },
      request_id: expect.any(String),
    });
  });

  it('rejects an invalid file access value before persistence', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const response = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [{ type: 'file', file_id: 'file_invalid_access', access: 'invalid' }],
      }),
    });

    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({
      type: 'error',
      error: { type: 'invalid_request_error' },
      request_id: expect.any(String),
    });
  });

  it('create session with resources[] then GET returns them', async () => {
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
          { type: 'file', file_id: fileA, mount_path: '/mnt/a.txt', access: 'read_only' },
          { type: 'file', file_id: fileB, mount_path: '/mnt/b.txt', access: 'read_only' },
        ],
      }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as { id: string };
    expect(session).not.toHaveProperty('workspace_id');

    const get = await fetch(`${baseURL}/v1/sessions/${session.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    const fetched = (await get.json()) as {
      id: string;
      resources: Array<{ id: string; type: string; file_id: string | null; mount_path: string }>;
    };
    expect(fetched).not.toHaveProperty('workspace_id');
    expect(fetched.resources).toHaveLength(2);
    expect(fetched.resources.map((r) => r.file_id).sort()).toEqual([fileA, fileB].sort());
  }, 30000);

  it('rejects reserved Skill-root overlaps on create, attach, and update', async () => {
    const fileA = await uploadFile('reserved-a.txt', `a-${randomUUID()}`);
    const fileB = await uploadFile('reserved-b.txt', `b-${randomUUID()}`);
    const agentId = await createTestAgent(baseURL, apiKey);

    const rejectedCreate = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          {
            type: 'file',
            file_id: fileA,
            mount_path: '/workspace/skills/untrusted.txt',
          },
        ],
      }),
    });
    expect(rejectedCreate.status).toBe(400);
    expect(await rejectedCreate.text()).toContain('reserved Skill root /workspace/skills');

    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [{ type: 'file', file_id: fileA, mount_path: '/mnt/reserved-a.txt' }],
      }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as {
      id: string;
      resources: Array<{ id: string }>;
    };

    const rejectedAttach = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        type: 'file',
        file_id: fileB,
        mount_path: '/workspace',
      }),
    });
    expect(rejectedAttach.status).toBe(400);
    expect(await rejectedAttach.text()).toContain('reserved Skill root /workspace/skills');

    const rejectedUpdate = await fetch(
      `${baseURL}/v1/sessions/${session.id}/resources/${session.resources[0]!.id}`,
      {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({ mount_path: '/' }),
      },
    );
    expect(rejectedUpdate.status).toBe(400);
    expect(await rejectedUpdate.text()).toContain('reserved Skill root /workspace/skills');
  }, 30000);

  it('defaults github_repository access consistently on create and attach', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const repoA = `https://github.com/orca-test/repo-a-${randomUUID()}`;
    const repoB = `https://github.com/orca-test/repo-b-${randomUUID()}`;
    const credA = `gitcred_${randomUUID()}`;
    const credB = `gitcred_${randomUUID()}`;
    await db.insert(gitCredentials).values([
      {
        id: credA,
        workspaceId,
        provider: 'github',
        repoUrl: repoA,
        secretRef: 'env://TEST_GITHUB_TOKEN_A',
        metadata: {},
      },
      {
        id: credB,
        workspaceId,
        provider: 'github',
        repoUrl: repoB,
        secretRef: 'env://TEST_GITHUB_TOKEN_B',
        metadata: {},
      },
    ]);

    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          {
            type: 'github_repository',
            url: repoA,
            authorization_token: `git_cred://${credA}`,
            mount_path: '/workspace/repo-a/',
          },
        ],
      }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as { id: string };

    const attach = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        type: 'github_repository',
        url: repoB,
        authorization_token: `git_cred://${credB}`,
        mount_path: '/workspace/repo-b/',
      }),
    });
    expect(attach.status).toBe(200);
    expect(((await attach.json()) as { type: string }).type).toBe('github_repository');

    const prepared = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${session.id}/executions:prepare`,
      { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' },
    );
    expect(prepared.status).toBe(200);
    const execution = (await prepared.json()) as {
      resources: Array<{ type: string; access: string }>;
    };
    expect(
      execution.resources
        .filter((resource) => resource.type === 'github_repository')
        .map((resource) => resource.access),
    ).toEqual(['read_write', 'read_write']);
  }, 30000);

  it('attach + detach a file resource at runtime', async () => {
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
    const session = (await create.json()) as { id: string };

    const attach = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        type: 'file',
        file_id: file2,
        mount_path: '/mnt/d.txt',
        access: 'read_only',
      }),
    });
    expect(attach.status).toBe(200);
    const attached = (await attach.json()) as { id: string };

    const get1 = await fetch(`${baseURL}/v1/sessions/${session.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    const fetched1 = (await get1.json()) as {
      resources: Array<{ id: string; file_id: string | null }>;
    };
    expect(fetched1.resources).toHaveLength(2);

    const detach = await fetch(`${baseURL}/v1/sessions/${session.id}/resources/${attached.id}`, {
      method: 'DELETE',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': '1',
      },
      body: JSON.stringify({}),
    });
    expect(detach.status).toBe(200);
    expect(await detach.json()).toEqual({
      id: attached.id,
      type: 'session_resource_deleted',
    });

    const get2 = await fetch(`${baseURL}/v1/sessions/${session.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    const fetched2 = (await get2.json()) as {
      resources: Array<{ id: string; file_id: string | null }>;
    };
    expect(fetched2.resources).toHaveLength(1);
    expect(fetched2.resources[0]!.file_id).toBe(file1);
  }, 30000);

  it('lists, gets, updates, and hides detached resources through Claude-compatible routes', async () => {
    const file1 = await uploadFile('list-a.txt', `list-a-${Date.now()}-${Math.random()}`);
    const file2 = await uploadFile('list-b.txt', `list-b-${Date.now()}-${Math.random()}`);
    const agentId = await createTestAgent(baseURL, apiKey);
    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          { type: 'file', file_id: file1, mount_path: '/mnt/list-a.txt', access: 'read_only' },
        ],
      }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as { id: string };

    const attach = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        type: 'file',
        file_id: file2,
        mount_path: '/mnt/list-b.txt',
        access: 'read_only',
        mount_strategy: 'tarball_prefetch',
      }),
    });
    expect(attach.status).toBe(200);
    const attached = (await attach.json()) as { id: string; file_id: string | null };

    const list = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(list.status).toBe(200);
    const listed = (await list.json()) as {
      data: Array<{ id: string; file_id: string | null }>;
      next_page: string | null;
    };
    expect(listed.data.map((r) => r.file_id).sort()).toEqual([file1, file2].sort());
    expect(listed.next_page).toBeNull();

    const get = await fetch(`${baseURL}/v1/sessions/${session.id}/resources/${attached.id}`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(get.status).toBe(200);
    expect(((await get.json()) as { file_id: string | null }).file_id).toBe(file2);

    const update = await fetch(`${baseURL}/v1/sessions/${session.id}/resources/${attached.id}`, {
      method: 'POST',
      headers: {
        'x-api-key': apiKey,
        'content-type': 'application/json',
        'orca-beta': '1',
      },
      body: JSON.stringify({
        mount_path: '/mnt/updated.txt',
        access: 'read_write',
        instructions: 'Use updated file path',
        mount_strategy: 'tarball_prefetch',
      }),
    });
    expect(update.status).toBe(200);
    const updated = (await update.json()) as {
      mount_path: string;
      access: string;
      instructions: string | null;
      mount_strategy: string | null;
    };
    expect(updated).toMatchObject({
      mount_path: '/mnt/updated.txt',
      access: 'read_write',
      instructions: 'Use updated file path',
      mount_strategy: 'tarball_prefetch',
    });

    const badUpdate = await fetch(`${baseURL}/v1/sessions/${session.id}/resources/${attached.id}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ mount_strategy: 'nonsense' }),
    });
    expect(badUpdate.status).toBe(400);
    expect(((await badUpdate.json()) as { error: { message: string } }).error.message).toMatch(
      /mount_strategy/,
    );

    const missingBody = await fetch(
      `${baseURL}/v1/sessions/${session.id}/resources/${attached.id}`,
      {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      },
    );
    expect(missingBody.status).toBe(400);

    const detach = await fetch(`${baseURL}/v1/sessions/${session.id}/resources/${attached.id}`, {
      method: 'DELETE',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(detach.status).toBe(200);
    expect(await detach.json()).toEqual({
      id: attached.id,
      type: 'session_resource_deleted',
    });

    const detachedGet = await fetch(
      `${baseURL}/v1/sessions/${session.id}/resources/${attached.id}`,
      {
        headers: { 'x-api-key': apiKey },
      },
    );
    expect(detachedGet.status).toBe(404);

    const relist = await fetch(`${baseURL}/v1/sessions/${session.id}/resources`, {
      headers: { 'x-api-key': apiKey },
    });
    const relisted = (await relist.json()) as {
      data: Array<{ id: string; file_id: string | null }>;
      next_page: string | null;
    };
    expect(relisted.data).toHaveLength(1);
    expect(relisted.data[0]!.file_id).toBe(file1);
    expect(relisted.next_page).toBeNull();
  }, 60000);

  it('paginates resources with page and next_page', async () => {
    const file1 = await uploadFile('page-a.txt', `page-a-${Date.now()}-${Math.random()}`);
    const file2 = await uploadFile('page-b.txt', `page-b-${Date.now()}-${Math.random()}`);
    const agentId = await createTestAgent(baseURL, apiKey);
    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [
          { type: 'file', file_id: file1, mount_path: '/mnt/page-a.txt', access: 'read_only' },
          { type: 'file', file_id: file2, mount_path: '/mnt/page-b.txt', access: 'read_only' },
        ],
      }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as { id: string };

    const firstPage = await fetch(`${baseURL}/v1/sessions/${session.id}/resources?limit=1`, {
      headers: { 'x-api-key': apiKey },
    });
    expect(firstPage.status).toBe(200);
    const firstPageBody = (await firstPage.json()) as {
      data: Array<{ id: string; file_id: string | null }>;
      next_page: string | null;
    };
    expect(firstPageBody.data).toHaveLength(1);
    expect(firstPageBody.next_page).toBeTruthy();

    const secondPage = await fetch(
      `${baseURL}/v1/sessions/${session.id}/resources?limit=1&page=${encodeURIComponent(
        firstPageBody.next_page!,
      )}`,
      { headers: { 'x-api-key': apiKey } },
    );
    expect(secondPage.status).toBe(200);
    const secondPageBody = (await secondPage.json()) as {
      data: Array<{ id: string; file_id: string | null }>;
      next_page: string | null;
    };
    expect(secondPageBody.data).toHaveLength(1);
    expect(secondPageBody.data[0]!.id).not.toBe(firstPageBody.data[0]!.id);
    expect([firstPageBody.data[0]!.file_id, secondPageBody.data[0]!.file_id].sort()).toEqual(
      [file1, file2].sort(),
    );
    expect(secondPageBody.next_page).toBeNull();
  }, 30000);

  it('serves /events/stream as an SSE alias for /stream and accepts Last-Event-ID', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const create = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ environment_id: environmentId, agent_id: agentId }),
    });
    expect(create.status).toBe(200);
    const session = (await create.json()) as { id: string };

    const res = await fetch(`${baseURL}/v1/sessions/${session.id}/events/stream`, {
      headers: {
        'x-api-key': apiKey,
        accept: 'text/event-stream',
        'last-event-id': '42',
      },
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toMatch(/text\/event-stream/);
    expect(res.headers.get('cache-control')).toMatch(/no-cache/);
    await res.body?.cancel().catch(() => {});
  }, 30000);

  it('rejects resources[] referencing a file_id outside the workspace', async () => {
    // Build a second workspace, upload a file there, then try to reference it
    // from this workspace's session.
    const { db } = await getTestDb();
    const otherWs = uniqueWorkspace('other');
    const otherKey = await createTestApiKey(db, otherWs);
    const f = new FormData();
    f.append(
      'file',
      new Blob([`outside-${Date.now()}-${Math.random()}`], { type: 'text/plain' }),
      'x.txt',
    );
    const otherFileResp = await fetch(`${baseURL}/v1/files`, {
      method: 'POST',
      headers: { 'x-api-key': otherKey },
      body: f,
    });
    expect(otherFileResp.status).toBe(200);
    const otherFile = (await otherFileResp.json()) as { id: string };

    const agentId = await createTestAgent(baseURL, apiKey);
    const res = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agentId,
        resources: [{ type: 'file', file_id: otherFile.id, mount_path: '/mnt/x.txt' }],
      }),
    });
    expect(res.status).toBe(400);
  }, 30000);

  it('rejects more than 100 resources at create', async () => {
    const fileId = await uploadFile('many.txt', `many-${Date.now()}-${Math.random()}`);
    const agentId = await createTestAgent(baseURL, apiKey);
    const big = Array.from({ length: 101 }, (_, i) => ({
      type: 'file' as const,
      file_id: fileId,
      mount_path: `/mnt/m${i}.txt`,
      access: 'read_only' as const,
    }));
    const res = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ environment_id: environmentId, agent_id: agentId, resources: big }),
    });
    // ts-rest's Zod validation is hand-wired; the route's own check fires.
    // Either way, expect a 4xx.
    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(res.status).toBeLessThan(500);
  }, 30000);
});
