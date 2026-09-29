// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import { createHash, randomBytes } from 'node:crypto';
import {
  buildStubStore,
  buildTestFileStore,
  buildTestJwtMinter,
  buildTestMemoryStore,
  closeTestDb,
  closeTestFileStore,
  closeTestMemoryStore,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';
import {
  createTestAgent,
  createTestApiKey,
  createTestEnvironment,
  uniqueWorkspace,
} from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';

interface ApiMemoryStore {
  id: string;
  name: string;
  description: string | null;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

interface ApiMemory {
  id: string;
  store_id: string;
  path: string;
  current_sha256: string;
  size_bytes: number;
  updated_at: string;
  updated_by_session_id: string | null;
  updated_by_event_id: string | null;
}

interface ApiMemoryVersion {
  id: string;
  store_id: string;
  memory_id: string;
  path: string;
  sha256: string;
  size_bytes: number;
  written_by_session_id: string | null;
  written_by_event_id: string | null;
  written_at: string;
  redacted_at: string | null;
}

interface RecordResp {
  memory: ApiMemory;
  version: ApiMemoryVersion;
  conflict: boolean;
}

interface VersionsResp {
  data: ApiMemoryVersion[];
  next_page: string | null;
}

function sha256Hex(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

function uniqueName(prefix: string): string {
  return `${prefix}-${randomBytes(4).toString('hex')}`;
}

describe('workspace-scoped internal memory API (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let workspaceId: string;
  let otherWorkspaceId: string;
  let apiKey: string;
  let storeId: string;
  let otherStoreId: string;
  let sessionId: string;

  async function recordVersion(body: {
    path: string;
    content: Buffer;
    previous_sha256: string | null;
    sha_override?: string;
    workspace_id?: string;
    store_id?: string;
    session_id?: string;
  }): Promise<Response> {
    const sha = body.sha_override ?? sha256Hex(body.content);
    return fetch(
      `${baseURL}/internal/v1/workspaces/${body.workspace_id ?? workspaceId}/sessions/${
        body.session_id ?? sessionId
      }/memory-stores/${body.store_id ?? storeId}/memory-versions`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          path: body.path,
          content_base64: body.content.toString('base64'),
          content_sha256: sha,
          previous_sha256: body.previous_sha256,
        }),
      },
    );
  }

  async function listVersions(memoryId: string): Promise<ApiMemoryVersion[]> {
    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/memory-stores/${storeId}/memory-versions?memory_id=${memoryId}`,
    );
    expect(res.status).toBe(200);
    return ((await res.json()) as VersionsResp).data;
  }

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
    workspaceId = uniqueWorkspace('intmemver');
    otherWorkspaceId = uniqueWorkspace('intmemver-other');
    apiKey = await createTestApiKey(db, workspaceId);
    const otherApiKey = await createTestApiKey(db, otherWorkspaceId);

    // Create a store in the primary workspace via the public route — locks
    // in that the internal route accepts ids the public surface produces.
    const createRes = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: uniqueName('memver') }),
    });
    expect(createRes.status).toBe(200);
    const store = (await createRes.json()) as ApiMemoryStore;
    expect(store).not.toHaveProperty('workspace_id');
    storeId = store.id;

    const agentId = await createTestAgent(baseURL, apiKey);
    const environmentId = await createTestEnvironment(baseURL, apiKey);
    const createSession = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        agent_id: agentId,
        environment_id: environmentId,
        resources: [
          {
            type: 'memory_store',
            memory_store_id: storeId,
            access: 'read_write',
          },
        ],
      }),
    });
    expect(createSession.status).toBe(200);
    const session = (await createSession.json()) as { id: string };
    expect(session).not.toHaveProperty('workspace_id');
    sessionId = session.id;

    // A store in a *different* workspace, used by the cross-workspace 404 case.
    const otherCreate = await fetch(`${baseURL}/v1/memory_stores`, {
      method: 'POST',
      headers: { 'x-api-key': otherApiKey, 'content-type': 'application/json' },
      body: JSON.stringify({ name: uniqueName('memver-other') }),
    });
    expect(otherCreate.status).toBe(200);
    const otherStore = (await otherCreate.json()) as ApiMemoryStore;
    expect(otherStore).not.toHaveProperty('workspace_id');
    otherStoreId = otherStore.id;
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestMemoryStore();
    await closeTestFileStore();
    await closeTestDb();
  });

  it('cold start: no precondition -> 201, conflict=false, memory + version persisted', async () => {
    const path = `cold/${randomBytes(3).toString('hex')}.txt`;
    const content = Buffer.from('cold-start');
    const expectedSha = sha256Hex(content);
    const res = await recordVersion({
      path,
      content,
      previous_sha256: null,
    });
    expect(res.status).toBe(201);
    const body = (await res.json()) as RecordResp;
    expect(body.conflict).toBe(false);
    expect(body.memory.id).toMatch(/^mem_/);
    expect(body.memory.store_id).toBe(storeId);
    expect(body.memory.path).toBe(path);
    expect(body.memory.current_sha256).toBe(expectedSha);
    expect(body.memory.size_bytes).toBe(content.length);
    expect(body.memory.updated_by_session_id).toBe(sessionId);
    expect(body.version.id).toMatch(/^memver_/);
    expect(body.version.memory_id).toBe(body.memory.id);
    expect(body.version.sha256).toBe(expectedSha);
    expect(body.version.written_by_session_id).toBe(sessionId);

    const memories = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/memory-stores/${storeId}/memories`,
    );
    expect(memories.status).toBe(200);
    const memoryList = (await memories.json()) as { data: ApiMemory[]; next_page: null };
    expect(memoryList.data.some((memory) => memory.id === body.memory.id)).toBe(true);

    const contentRes = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/memory-stores/${storeId}/memories/${body.memory.id}/content`,
    );
    expect(contentRes.status).toBe(200);
    expect(Buffer.from(await contentRes.arrayBuffer())).toEqual(content);

    // The version log holds exactly one row for this memory now.
    const versions = await listVersions(body.memory.id);
    expect(versions).toHaveLength(1);
    expect(versions[0]!.sha256).toBe(expectedSha);
  });

  it('matching precondition -> 201, conflict=false, version chain has 2 entries DESC', async () => {
    const path = `match/${randomBytes(3).toString('hex')}.txt`;

    // Cold start: sha A.
    const cA = Buffer.from('A');
    const shaA = sha256Hex(cA);
    const cold = await recordVersion({
      path,
      content: cA,
      previous_sha256: null,
    });
    expect(cold.status).toBe(201);
    const memoryId = ((await cold.json()) as RecordResp).memory.id;

    // Update with matching precondition: sha A -> sha B.
    const cB = Buffer.from('B');
    const shaB = sha256Hex(cB);
    const update = await recordVersion({
      path,
      content: cB,
      previous_sha256: shaA,
    });
    expect(update.status).toBe(201);
    const updateBody = (await update.json()) as RecordResp;
    expect(updateBody.conflict).toBe(false);
    expect(updateBody.memory.id).toBe(memoryId);
    expect(updateBody.memory.current_sha256).toBe(shaB);
    expect(updateBody.version.sha256).toBe(shaB);

    const versions = await listVersions(memoryId);
    expect(versions).toHaveLength(2);
    expect(versions[0]!.sha256).toBe(shaB); // newest first
    expect(versions[1]!.sha256).toBe(shaA);
  });

  it('stale precondition -> 201, conflict=true, version recorded last-writer-wins', async () => {
    const path = `stale/${randomBytes(3).toString('hex')}.txt`;

    // Build the chain: A -> B (without precondition for the cold start, with for the second).
    const cA = Buffer.from('A');
    const shaA = sha256Hex(cA);
    const cold = await recordVersion({
      path,
      content: cA,
      previous_sha256: null,
    });
    expect(cold.status).toBe(201);
    const memoryId = ((await cold.json()) as RecordResp).memory.id;

    const cB = Buffer.from('B');
    const shaB = sha256Hex(cB);
    const second = await recordVersion({
      path,
      content: cB,
      previous_sha256: shaA,
    });
    expect(second.status).toBe(201);
    expect(((await second.json()) as RecordResp).conflict).toBe(false);

    // Now post sha C with a STALE precondition (caller still thinks the live
    // sha is A, but it's already B). Last-writer-wins: the write must succeed
    // and the response carries `conflict: true`.
    const cC = Buffer.from('C');
    const shaC = sha256Hex(cC);
    const stale = await recordVersion({
      path,
      content: cC,
      previous_sha256: shaA, // stale!
    });
    expect(stale.status).toBe(201);
    const staleBody = (await stale.json()) as RecordResp;
    expect(staleBody.conflict).toBe(true);
    expect(staleBody.memory.id).toBe(memoryId);
    expect(staleBody.memory.current_sha256).toBe(shaC);
    expect(staleBody.version.sha256).toBe(shaC);

    // The version log should now hold 3 rows DESC.
    const versions = await listVersions(memoryId);
    expect(versions).toHaveLength(3);
    expect(versions[0]!.sha256).toBe(shaC);
    expect(versions[1]!.sha256).toBe(shaB);
    expect(versions[2]!.sha256).toBe(shaA);
  });

  it('rejects 413 when content exceeds 100 KB', async () => {
    const big = Buffer.alloc(100 * 1024 + 1, 0x61);
    const res = await recordVersion({
      path: `big/${randomBytes(3).toString('hex')}.bin`,
      content: big,
      previous_sha256: null,
    });
    expect(res.status).toBe(413);
    const body = (await res.json()) as { error: string };
    // 100 KB = 102400 bytes; the cap message must reference the limit.
    expect(body.error).toMatch(/102400/);
  });

  it('rejects 400 when content_sha256 disagrees with content', async () => {
    const content = Buffer.from('hello');
    const wrongSha = sha256Hex(Buffer.from('goodbye'));
    const res = await recordVersion({
      path: `mismatch/${randomBytes(3).toString('hex')}.txt`,
      content,
      previous_sha256: null,
      sha_override: wrongSha,
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/sha256/);
  });

  it('rejects invalid or oversized paths before creating memory metadata', async () => {
    for (const path of ['../secret', 'a//b', 'x'.repeat(1025)]) {
      const res = await recordVersion({
        path,
        content: Buffer.from('x'),
        previous_sha256: null,
      });
      expect(res.status).toBe(400);
    }

    const memories = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/memory-stores/${storeId}/memories`,
    );
    expect(memories.status).toBe(200);
    const rows = ((await memories.json()) as { data: ApiMemory[] }).data;
    expect(rows.some((memory) => memory.path.includes('secret'))).toBe(false);
  });

  it('cross-workspace store_id -> 404', async () => {
    // The store id was created under `otherWorkspaceId`; calling with
    // `workspaceId` must 404, not silently fall through to the other tenant.
    const content = Buffer.from('x');
    const res = await recordVersion({
      store_id: otherStoreId,
      path: `cross/${randomBytes(3).toString('hex')}.txt`,
      content,
      previous_sha256: null,
    });
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/memory_store/);
  });
});
