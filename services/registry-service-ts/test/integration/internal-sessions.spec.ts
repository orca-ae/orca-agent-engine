// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
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
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { sessions } from '../../src/persistence/postgres/schema.js';

interface SessionResp {
  id: string;
  status: string;
  timing: {
    started_at: string | null;
    last_active_at: string | null;
    active_seconds: number;
    duration_seconds: number;
  };
  usage: {
    cache_creation: {
      ephemeral_1h_input_tokens: number;
      ephemeral_5m_input_tokens: number;
    };
    cache_read_input_tokens: number;
    input_tokens: number;
    output_tokens: number;
  };
  resources: Array<{ id: string; type: string; mount_path: string }>;
  created_at: string;
  updated_at: string;
}

interface InternalSessionResp extends SessionResp {
  workspace_id: string;
  runtime_revision: number;
  sandbox_handle_id: string | null;
}

interface SessionThreadResp {
  id: string;
  status: string;
  stats: {
    active_seconds: number;
    duration_seconds: number;
    startup_seconds: number;
  };
}

describe('/internal/v1/workspaces/:workspaceId/sessions/:id lifecycle (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let apiKey: string;
  let workspaceId: string;
  let sessionId: string;
  let db: DbClient;

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
    workspaceId = uniqueWorkspace('internal_sessions');
    apiKey = await createTestApiKey(db, workspaceId);
    const agentId = await createTestAgent(baseURL, apiKey);
    sessionId = await createTestSession(baseURL, apiKey, agentId);
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await closeTestFileStore();
    await closeTestDb();
  });

  it('404s on a missing session in the scoped workspace', async () => {
    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/ses_nonexistent_xyz/state`,
      {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ status: 'idle' }),
      },
    );
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/not found/);
  });

  it('400s on an invalid session id format', async () => {
    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/not-a-session-id/state`,
      {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ status: 'idle' }),
      },
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/invalid/i);
  });

  it('updates lifecycle state for the harness without an api-key', async () => {
    const running = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/state`,
      {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ status: 'running', sandbox_handle_id: 'sbx_local_test' }),
      },
    );
    expect(running.status).toBe(200);
    const runningBody = (await running.json()) as InternalSessionResp;
    expect(runningBody.workspace_id).toBe(workspaceId);
    expect(runningBody.status).toBe('running');
    expect(runningBody.sandbox_handle_id).toBe('sbx_local_test');
    expect(runningBody).not.toHaveProperty('started_at');
    expect(runningBody.timing.started_at).toBeTruthy();
    expect(runningBody.timing.last_active_at).toBeTruthy();
    expect(runningBody.timing.active_seconds).toBe(0);

    const stillRunningBody = await waitForSession(baseURL, apiKey, sessionId, (body) => {
      expect(body.status).toBe('running');
      return body.timing.active_seconds >= 1;
    });
    expect(stillRunningBody.status).toBe('running');
    expect(stillRunningBody.timing.active_seconds).toBeGreaterThanOrEqual(1);
    const runningThread = await getPrimaryThread(baseURL, apiKey, sessionId);
    expect(runningThread.status).toBe('running');
    expect(runningThread.stats.active_seconds).toBeGreaterThanOrEqual(1);

    const idle = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/state`,
      {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ status: 'idle', sandbox_handle_id: null }),
      },
    );
    expect(idle.status).toBe(200);
    const idleBody = (await idle.json()) as InternalSessionResp;
    expect(idleBody.status).toBe('idle');
    expect(idleBody.sandbox_handle_id).toBe(null);
    expect(idleBody.timing.started_at).toBe(runningBody.timing.started_at);
    expect(idleBody.timing.active_seconds).toBeGreaterThanOrEqual(1);

    const afterIdleBody = await observeStableActiveSeconds(
      baseURL,
      apiKey,
      sessionId,
      idleBody.timing.active_seconds,
    );
    expect(afterIdleBody.status).toBe('idle');
    expect(afterIdleBody.timing.active_seconds).toBe(idleBody.timing.active_seconds);
    const idleThread = await getPrimaryThread(baseURL, apiKey, sessionId);
    expect(idleThread.status).toBe('idle');
    expect(idleThread.stats.active_seconds).toBe(idleBody.timing.active_seconds);
  });

  it('does not double-count active seconds when concurrent idle updates race', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const raceSessionId = await createTestSession(baseURL, apiKey, agentId);
    const activeSince = new Date(Date.now() - 10_000);
    await db
      .update(sessions)
      .set({
        status: 'running',
        sandboxHandleId: 'sbx_local_race',
        startedAt: activeSince,
        lastActiveAt: activeSince,
        activeSeconds: 5,
      })
      .where(eq(sessions.id, raceSessionId));

    const markIdle = () =>
      fetch(`${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${raceSessionId}/state`, {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ status: 'idle', sandbox_handle_id: null }),
      });
    const responses = await Promise.all([markIdle(), markIdle()]);
    for (const res of responses) expect(res.status).toBe(200);
    const internalBodies = (await Promise.all(
      responses.map((response) => response.json()),
    )) as InternalSessionResp[];
    expect(internalBodies.every((response) => response.sandbox_handle_id === null)).toBe(true);

    const body = await getSession(baseURL, apiKey, raceSessionId);
    expect(body.status).toBe('idle');
    expect(body.timing.active_seconds).toBeGreaterThanOrEqual(14);
    expect(body.timing.active_seconds).toBeLessThan(20);
  });

  it('accumulates session token usage for the harness without an api-key', async () => {
    const first = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/usage`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          usage: {
            cache_creation: {
              ephemeral_1h_input_tokens: 2,
              ephemeral_5m_input_tokens: 3,
            },
            cache_read_input_tokens: 5,
            input_tokens: 7,
            output_tokens: 11,
          },
        }),
      },
    );
    expect(first.status).toBe(200);
    const firstBody = (await first.json()) as InternalSessionResp;
    expect(firstBody.usage).toEqual({
      cache_creation: {
        ephemeral_1h_input_tokens: 2,
        ephemeral_5m_input_tokens: 3,
      },
      cache_read_input_tokens: 5,
      input_tokens: 7,
      output_tokens: 11,
    });

    const second = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/usage`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          usage: {
            cache_creation: {
              ephemeral_5m_input_tokens: 13,
            },
            input_tokens: 17,
            output_tokens: 19,
          },
        }),
      },
    );
    expect(second.status).toBe(200);
    const secondBody = (await second.json()) as InternalSessionResp;
    expect(secondBody.usage).toEqual({
      cache_creation: {
        ephemeral_1h_input_tokens: 2,
        ephemeral_5m_input_tokens: 16,
      },
      cache_read_input_tokens: 5,
      input_tokens: 24,
      output_tokens: 30,
    });
  });

  it('accepts a final usage flush after the session becomes terminated', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const terminalSessionId = await createTestSession(baseURL, apiKey, agentId);
    await db
      .update(sessions)
      .set({ status: 'terminated' })
      .where(and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, terminalSessionId)));

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${terminalSessionId}/usage`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ usage: { input_tokens: 23, output_tokens: 29 } }),
      },
    );

    expect(res.status).toBe(200);
    const body = (await res.json()) as InternalSessionResp;
    expect(body.status).toBe('terminated');
    expect(body.usage.input_tokens).toBe(23);
    expect(body.usage.output_tokens).toBe(29);
  });

  it('rejects malformed usage counters without partially updating totals', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const usageSessionId = await createTestSession(baseURL, apiKey, agentId);
    const before = await getSession(baseURL, apiKey, usageSessionId);

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${usageSessionId}/usage`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          usage: {
            cache_creation: {
              ephemeral_1h_input_tokens: 1,
              ephemeral_5m_input_tokens: -1,
            },
            input_tokens: 2,
            output_tokens: 3,
          },
        }),
      },
    );
    expect(res.status).toBe(400);

    const after = await getSession(baseURL, apiKey, usageSessionId);
    expect(after.usage).toEqual(before.usage);
  });

  it('atomically accumulates concurrent session token usage reports', async () => {
    const agentId = await createTestAgent(baseURL, apiKey);
    const concurrentSessionId = await createTestSession(baseURL, apiKey, agentId);

    const report = (inputTokens: number, outputTokens: number) =>
      fetch(
        `${baseURL}/internal/v1/workspaces/${workspaceId}/sessions/${concurrentSessionId}/usage`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            usage: {
              cache_creation: {
                ephemeral_1h_input_tokens: inputTokens,
                ephemeral_5m_input_tokens: outputTokens,
              },
              cache_read_input_tokens: inputTokens + outputTokens,
              input_tokens: inputTokens,
              output_tokens: outputTokens,
            },
          }),
        },
      );

    const responses = await Promise.all([report(3, 5), report(7, 11), report(13, 17)]);
    for (const res of responses) expect(res.status).toBe(200);

    const body = await getSession(baseURL, apiKey, concurrentSessionId);
    expect(body.usage).toEqual({
      cache_creation: {
        ephemeral_1h_input_tokens: 23,
        ephemeral_5m_input_tokens: 33,
      },
      cache_read_input_tokens: 56,
      input_tokens: 23,
      output_tokens: 33,
    });
  });

  it('fails closed for the wrong workspace and keeps public responses workspace-implicit', async () => {
    const otherWorkspace = uniqueWorkspace('internal_sessions_other');
    const { db } = await getTestDb();
    const otherKey = await createTestApiKey(db, otherWorkspace);

    const publicRes = await fetch(`${baseURL}/v1/sessions/${sessionId}`, {
      headers: { 'x-api-key': otherKey },
    });
    expect(publicRes.status).toBe(404);

    const internalRes = await fetch(
      `${baseURL}/internal/v1/workspaces/${otherWorkspace}/sessions/${sessionId}/state`,
      {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ status: 'idle' }),
      },
    );
    expect(internalRes.status).toBe(404);

    const own = await getSession(baseURL, apiKey, sessionId);
    expect(own.id).toBe(sessionId);
    expect(own).not.toHaveProperty('workspace_id');
  });
});

async function getSession(
  baseURL: string,
  apiKey: string,
  sessionId: string,
): Promise<SessionResp> {
  const res = await fetch(`${baseURL}/v1/sessions/${sessionId}`, {
    headers: { 'x-api-key': apiKey },
  });
  expect(res.status).toBe(200);
  const body = (await res.json()) as SessionResp;
  expect(body).not.toHaveProperty('workspace_id');
  return body;
}

async function getPrimaryThread(
  baseURL: string,
  apiKey: string,
  sessionId: string,
): Promise<SessionThreadResp> {
  const list = await fetch(`${baseURL}/v1/sessions/${sessionId}/threads`, {
    headers: { 'x-api-key': apiKey },
  });
  expect(list.status).toBe(200);
  const listBody = (await list.json()) as { data: Array<{ id: string }> };
  expect(listBody.data.length).toBeGreaterThan(0);
  const threadId = listBody.data[0]!.id;

  const get = await fetch(`${baseURL}/v1/sessions/${sessionId}/threads/${threadId}`, {
    headers: { 'x-api-key': apiKey },
  });
  expect(get.status).toBe(200);
  return (await get.json()) as SessionThreadResp;
}

async function waitForSession(
  baseURL: string,
  apiKey: string,
  sessionId: string,
  predicate: (body: SessionResp) => boolean,
  timeoutMs = 2500,
): Promise<SessionResp> {
  const deadline = Date.now() + timeoutMs;
  let last: SessionResp | null = null;
  while (Date.now() < deadline) {
    last = await getSession(baseURL, apiKey, sessionId);
    if (predicate(last)) return last;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(
    `session ${sessionId} did not satisfy condition within ${timeoutMs}ms; last=${JSON.stringify(
      last,
    )}`,
  );
}

async function observeStableActiveSeconds(
  baseURL: string,
  apiKey: string,
  sessionId: string,
  expectedActiveSeconds: number,
  observationMs = 1200,
): Promise<SessionResp> {
  const deadline = Date.now() + observationMs;
  let last: SessionResp | null = null;
  do {
    last = await getSession(baseURL, apiKey, sessionId);
    expect(last.timing.active_seconds).toBe(expectedActiveSeconds);
    await new Promise((resolve) => setTimeout(resolve, 100));
  } while (Date.now() < deadline);
  return last;
}
