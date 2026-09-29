// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Layer A.2: black-box session-thread wire checks.
 *
 * This spec only uses public registry HTTP APIs. It does not seed
 * `session_threads` directly, so it exercises the same wire path a Managed
 * Agents client uses: create a session with its primary thread, append a
 * subpath event, discover the server-created child session_thread_id, route
 * follow-up control events through that id, then read them back through the
 * thread events API.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedWorkspaceApiKey } from '../src/seed.js';
import { createTestEnvironment, deleteTestEnvironment } from './environment-helpers.js';

interface AgentResponse {
  id: string;
  version: number;
  multiagent?: {
    type: 'coordinator';
    agents: Array<{ type: 'agent'; id: string; version: number }>;
  } | null;
}

interface SessionResponse {
  id: string;
  agent: { id: string; version: number };
}

interface SessionThreadResponse {
  id: string;
  type: 'session_thread';
  session_id: string;
  parent_thread_id: string | null;
  agent: { id: string; version: number; name: string };
  status: 'idle' | 'running' | 'rescheduling' | 'terminated';
  archived_at: string | null;
}

interface EventResponse {
  id: string;
  type: string;
  subpath?: string;
  session_thread_id?: string;
  content?: unknown;
  [key: string]: unknown;
}

describe('Layer A.2: session threads black-box API (live registry)', () => {
  let cfg: OrcaClientConfig;
  let environmentId: string;
  const created: { sessions: string[]; agents: string[] } = { sessions: [], agents: [] };

  beforeAll(async () => {
    const seeded = await seedWorkspaceApiKey();
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    environmentId = await createTestEnvironment(cfg, 'session-threads-env');
  });

  afterAll(async () => {
    if (cfg && environmentId) {
      await deleteTestEnvironment(cfg, environmentId).catch(() => {});
    }
  });

  afterEach(async () => {
    for (const id of created.sessions.splice(0)) {
      await apiCall(cfg, `/v1/sessions/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
    for (const id of created.agents.splice(0)) {
      await apiCall(cfg, `/v1/agents/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
  });

  it('routes session_thread_id control events to the discovered thread and archives it', async () => {
    const worker = await createAgent(`thread-worker-${Date.now()}`);
    const workerV2 = await apiCall(cfg, `/v1/agents/${worker.id}`, {
      method: 'POST',
      body: JSON.stringify({ version: worker.version, system: 'worker v2' }),
    });
    expect(workerV2.status).toBe(200);
    expect(workerV2.json<AgentResponse>().version).toBe(2);

    const coordinator = await createAgent(`thread-coordinator-${Date.now()}`, {
      multiagent: { type: 'coordinator', agents: [worker.id] },
    });
    expect(coordinator.multiagent).toEqual({
      type: 'coordinator',
      agents: [{ type: 'agent', id: worker.id, version: 2 }],
    });

    const session = await createSession(coordinator.id);
    expect(session.agent.id).toBe(coordinator.id);
    expect(session.agent.version).toBe(coordinator.version);

    const subpath = `threads/e2e-route-${Date.now()}`;
    const seedEvent = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      headers: { 'orca-beta': 'session-thread-extensions' },
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            subpath,
            content: [{ type: 'text', text: 'hello worker thread' }],
          },
        ],
      }),
    });
    expect(seedEvent.status).toBe(200);

    const thread = await waitForThreadWithEvent(session.id, 'user.message');
    const retrievedThread = await apiCall(cfg, `/v1/sessions/${session.id}/threads/${thread.id}`, {
      method: 'GET',
    });
    expect(retrievedThread.status).toBe(200);
    expect(retrievedThread.json<SessionThreadResponse>()).toMatchObject({ id: thread.id });
    expect(thread.session_id).toBe(session.id);
    expect(thread.type).toBe('session_thread');
    expect(thread.agent.id).toBe(coordinator.id);
    expect(thread.agent.version).toBe(coordinator.version);
    expect(thread.status).toBe('idle');

    const controlEvents = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      headers: { 'orca-beta': 'session-thread-extensions' },
      body: JSON.stringify({
        events: [
          { type: 'user.interrupt', session_thread_id: thread.id },
          {
            type: 'user.tool_confirmation',
            session_thread_id: thread.id,
            tool_use_id: 'evt_e2e_pending_tool',
            approved: true,
          },
          {
            type: 'user.custom_tool_result',
            session_thread_id: thread.id,
            tool_use_id: 'evt_e2e_pending_custom_tool',
            content: [{ type: 'text', text: 'custom result' }],
          },
        ],
      }),
    });
    expect(controlEvents.status).toBe(200);
    expect(controlEvents.json<{ events: EventResponse[] }>().events).toEqual([
      expect.objectContaining({ type: 'user.interrupt', session_thread_id: thread.id }),
      expect.objectContaining({ type: 'user.tool_confirmation', session_thread_id: thread.id }),
      expect.objectContaining({ type: 'user.custom_tool_result', session_thread_id: thread.id }),
    ]);

    const threadEvents = await apiCall(
      cfg,
      `/v1/sessions/${session.id}/threads/${thread.id}/events`,
      { method: 'GET' },
    );
    expect(threadEvents.status).toBe(200);
    const eventTypes = threadEvents
      .json<{ data: EventResponse[]; has_more: boolean; next_page: string | null }>()
      .data.map((event) => event.type);
    expect(eventTypes).toEqual([
      'user.message',
      'user.interrupt',
      'user.tool_confirmation',
      'user.custom_tool_result',
    ]);

    const archive = await apiCall(cfg, `/v1/sessions/${session.id}/threads/${thread.id}/archive`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
    expect(archive.status).toBe(200);
    expect(archive.json<SessionThreadResponse>()).toMatchObject({
      id: thread.id,
      status: 'terminated',
      archived_at: expect.any(String),
    });
  }, 60_000);

  it('rejects the 26th active thread created through public event appends', async () => {
    const agent = await createAgent(`thread-limit-${Date.now()}`);
    const session = await createSession(agent.id);

    const events = Array.from({ length: 25 }, (_, i) => ({
      type: 'user.message',
      subpath: `threads/e2e-limit-${Date.now()}-${i}`,
      content: [{ type: 'text', text: `thread ${i}` }],
    }));
    const seed = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      headers: { 'orca-beta': 'session-thread-extensions' },
      body: JSON.stringify({ events }),
    });
    expect(seed.status).toBe(200);
    await waitForThreadCount(session.id, 26);

    const overflow = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      headers: { 'orca-beta': 'session-thread-extensions' },
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            subpath: `threads/e2e-limit-overflow-${Date.now()}`,
            content: [{ type: 'text', text: 'too many' }],
          },
        ],
      }),
    });
    expect(overflow.status).toBe(409);
    expect(overflow.json<{ error: { message: string } }>().error.message).toMatch(
      /maximum concurrent session threads/i,
    );
  }, 30_000);

  async function createAgent(
    name: string,
    extra: Record<string, unknown> = {},
  ): Promise<AgentResponse> {
    const res = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
        ...extra,
      }),
    });
    expect(res.status).toBe(200);
    const agent = res.json<AgentResponse>();
    created.agents.push(agent.id);
    return agent;
  }

  async function createSession(agentId: string): Promise<SessionResponse> {
    const res = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({ environment_id: environmentId, agent_id: agentId }),
    });
    expect(res.status).toBe(200);
    const session = res.json<SessionResponse>();
    created.sessions.push(session.id);
    return session;
  }

  async function waitForThreadCount(
    sessionId: string,
    count: number,
  ): Promise<SessionThreadResponse[]> {
    const deadline = Date.now() + 10_000;
    let lastThreads: SessionThreadResponse[] = [];
    while (Date.now() < deadline) {
      const res = await apiCall(cfg, `/v1/sessions/${sessionId}/threads`, { method: 'GET' });
      expect(res.status).toBe(200);
      lastThreads = res.json<{ data: SessionThreadResponse[] }>().data;
      if (lastThreads.length === count) return lastThreads;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error(
      `expected ${count} session threads for ${sessionId}, got ${lastThreads.length}`,
    );
  }

  async function waitForThreadWithEvent(
    sessionId: string,
    eventType: string,
  ): Promise<SessionThreadResponse> {
    const deadline = Date.now() + 10_000;
    let lastThreads: SessionThreadResponse[] = [];
    while (Date.now() < deadline) {
      const res = await apiCall(cfg, `/v1/sessions/${sessionId}/threads`, { method: 'GET' });
      expect(res.status).toBe(200);
      lastThreads = res.json<{ data: SessionThreadResponse[] }>().data;
      for (const thread of lastThreads) {
        const eventsRes = await apiCall(
          cfg,
          `/v1/sessions/${sessionId}/threads/${thread.id}/events`,
          { method: 'GET' },
        );
        expect(eventsRes.status).toBe(200);
        const events = eventsRes.json<{ data: EventResponse[] }>().data;
        if (events.some((event) => event.type === eventType)) return thread;
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error(
      `expected a session thread for ${sessionId} containing ${eventType}; got ${lastThreads.length} thread(s)`,
    );
  }
});
