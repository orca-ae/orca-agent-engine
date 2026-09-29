// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { and, eq } from 'drizzle-orm';
import { buildCombinedTestApp } from '../../src/server.js';
import { environments, sessions } from '../../src/persistence/postgres/schema.js';
import { toInternalId } from '../../src/contracts/id-prefix.js';
import { createTestApiKey, uniqueWorkspace } from './fixtures.js';
import {
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  closeTestDb,
  getTestDb,
  STUB_SSE_CONFIG,
} from './setup.js';

const describeSdk = describe.each(['codex_sdk', 'pi_sdk'] as const);
describeSdk('%s deployment admission (integration)', (harness) => {
  const workspaceId = uniqueWorkspace('codex_deployment');
  let db: Awaited<ReturnType<typeof getTestDb>>['db'];
  let app: ReturnType<typeof buildCombinedTestApp>;
  let headers: Record<string, string>;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    headers = {
      'x-api-key': await createTestApiKey(db, workspaceId),
      'content-type': 'application/json',
      'orca-beta': 'true',
    };
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'test' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();
  });

  afterAll(async () => {
    await app?.close();
    await closeTestDb();
  });

  async function agent(mode?: 'colocated' | 'separate') {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers,
      payload: {
        name: `Codex ${mode ?? 'default'} ${crypto.randomUUID()}`,
        model: { provider: 'openai', id: 'gpt-5.4' },
        metadata: { harness, ...(mode ? { mode } : {}) },
      },
    });
    expect(response.statusCode, response.body).toBe(200);
    return response.json().id as string;
  }

  async function environment(target: 'cloud' | 'self_hosted') {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/environments',
      headers,
      payload: { name: `Codex ${target} ${crypto.randomUUID()}`, config: { type: target } },
    });
    expect(response.statusCode, response.body).toBe(200);
    return response.json().id as string;
  }

  const createSession = (agentId: string, environmentId: string, version = 1) =>
    app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers,
      payload: { agent: { type: 'agent', id: agentId, version }, environment_id: environmentId },
    });

  async function owner(sessionId: string) {
    const response = await app.inject({
      method: 'GET',
      url: `/internal/v1/workspaces/${workspaceId}/sessions/${toInternalId(sessionId)}/execution-owner`,
    });
    expect(response.statusCode, response.body).toBe(200);
    return response.json().owner;
  }

  it.each([undefined, 'separate'] as const)(
    'rejects self-hosted Codex mode %s without creating a Session',
    async (mode) => {
      const agentId = await agent(mode);
      const environmentId = await environment('self_hosted');
      const response = await createSession(agentId, environmentId);
      expect(response.statusCode, response.body).toBe(400);
      expect(response.json().error.message).toMatch(/(?:codex_sdk|pi_sdk)\/separate.*self_hosted/);
      const rows = await db
        .select({ id: sessions.id })
        .from(sessions)
        .where(
          and(eq(sessions.workspaceId, workspaceId), eq(sessions.agentId, toInternalId(agentId))),
        );
      expect(rows).toEqual([]);
    },
  );

  it('keeps explicit self-hosted colocated and cloud default separate executable', async () => {
    const colocated = await createSession(
      await agent('colocated'),
      await environment('self_hosted'),
    );
    expect(colocated.statusCode, colocated.body).toBe(200);
    expect(await owner(colocated.json().id)).toBe('registry');
    const updated = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${colocated.json().id}`,
      headers,
      payload: { agent: { model: { provider: 'openai', id: 'gpt-5.5' } } },
    });
    expect(updated.statusCode, updated.body).toBe(200);
    expect(await owner(colocated.json().id)).toBe('registry');
    const separate = await createSession(await agent(), await environment('cloud'));
    expect(separate.statusCode, separate.body).toBe(200);
    expect(await owner(separate.json().id)).toBe('harness-server');
  });

  it('validates the requested Agent version rather than its latest mode', async () => {
    const agentId = await agent();
    const environmentId = await environment('self_hosted');
    const edit = await app.inject({
      method: 'POST',
      url: `/v1/agents/${agentId}`,
      headers,
      payload: { metadata: { mode: 'colocated' } },
    });
    expect(edit.statusCode, edit.body).toBe(200);
    expect((await createSession(agentId, environmentId, 1)).statusCode).toBe(400);
    const updatedVersion = await createSession(agentId, environmentId, edit.json().version);
    expect(updatedVersion.statusCode, updatedVersion.body).toBe(200);
    expect(await owner(updatedVersion.json().id)).toBe('registry');
  });

  it('rejects Environment target changes incompatible with a pinned Session mode', async () => {
    const agentId = await agent();
    const environmentId = await environment('cloud');
    const created = await createSession(agentId, environmentId);
    expect(created.statusCode, created.body).toBe(200);
    // A latest-version edit must not invalidate the existing Session's binding.
    const latest = await app.inject({
      method: 'POST',
      url: `/v1/agents/${agentId}`,
      headers,
      payload: { metadata: { mode: 'colocated' } },
    });
    expect(latest.statusCode, latest.body).toBe(200);
    for (const payload of [{ target: 'self_hosted' }, { config: { type: 'self_hosted' } }]) {
      const response = await app.inject({
        method: 'POST',
        url: `/v1/environments/${environmentId}`,
        headers,
        payload,
      });
      expect(response.statusCode, response.body).toBe(400);
      expect(response.json().error.message).toMatch(
        /existing Session.*(?:codex_sdk|pi_sdk)\/separate/,
      );
    }
    const [stored] = await db
      .select()
      .from(environments)
      .where(eq(environments.id, toInternalId(environmentId)));
    expect(stored?.target).toBe('cloud');
    expect(await owner(created.json().id)).toBe('harness-server');
  });

  it('allows an Environment target change when its pinned Codex Session is colocated', async () => {
    const environmentId = await environment('cloud');
    const created = await createSession(await agent('colocated'), environmentId);
    expect(created.statusCode, created.body).toBe(200);
    const response = await app.inject({
      method: 'POST',
      url: `/v1/environments/${environmentId}`,
      headers,
      payload: { config: { type: 'self_hosted' } },
    });
    expect(response.statusCode, response.body).toBe(200);
    expect(await owner(created.json().id)).toBe('registry');
  });

  it('does not permit Session updates to replace the pinned Agent, mode, or Environment', async () => {
    const environmentId = await environment('cloud');
    const created = await createSession(await agent(), environmentId);
    expect(created.statusCode, created.body).toBe(200);
    for (const payload of [
      { environment_id: await environment('self_hosted') },
      { agent: { id: await agent('colocated') } },
      { agent: { metadata: { mode: 'colocated' } } },
    ]) {
      const response = await app.inject({
        method: 'POST',
        url: `/v1/sessions/${created.json().id}`,
        headers,
        payload,
      });
      expect(response.statusCode, response.body).toBe(400);
    }
    const metadataUpdate = await app.inject({
      method: 'POST',
      url: `/v1/sessions/${created.json().id}`,
      headers,
      payload: { metadata: { harness, mode: 'colocated' } },
    });
    expect(metadataUpdate.statusCode, metadataUpdate.body).toBe(200);
    expect(await owner(created.json().id)).toBe('harness-server');
  });
});
