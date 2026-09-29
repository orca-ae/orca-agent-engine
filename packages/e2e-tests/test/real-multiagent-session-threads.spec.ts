// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Layer B.2: real Claude multi-agent session threads.
 *
 * This is a black-box test: it creates Managed Agents and Sessions through the
 * public registry API, submits a user turn, and verifies the resulting session
 * thread APIs. It requires a real `ANTHROPIC_API_KEY`, like
 * `real-agent-loop.spec.ts`.
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
  name: string;
  version: number;
  multiagent?: {
    type: 'coordinator';
    agents: Array<{ type: 'agent'; id: string; version: number }>;
  } | null;
}

interface SessionResponse {
  id: string;
  agent_id: string;
  status: string;
  usage?: { input_tokens?: number; output_tokens?: number };
}

interface SessionThreadResponse {
  id: string;
  type: 'session_thread';
  session_id: string;
  agent: { id: string; version: number; name: string };
  status: string;
}

interface EventResponse {
  id: string;
  type: string;
  content?: unknown;
  [key: string]: unknown;
}

describe('Layer B.2: real Claude multi-agent session threads', () => {
  let cfg: OrcaClientConfig;
  let environmentId: string;
  const created: { sessions: string[]; agents: string[] } = { sessions: [], agents: [] };
  const realClaudeRetry = Number(process.env['ORCA_E2E_REAL_CLAUDE_RETRY'] ?? '2');

  beforeAll(async () => {
    if (!process.env['ANTHROPIC_API_KEY']) {
      throw new Error(
        'Layer B.2 requires ANTHROPIC_API_KEY. Add it to services/dev/.env and restart with make stack-up.',
      );
    }
    const seeded = await seedWorkspaceApiKey();
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    environmentId = await createTestEnvironment(cfg, 'real-multiagent-env');
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

  it(
    'spawns a real Claude subagent and exposes its transcript through session thread APIs',
    async () => {
      const marker = `MULTIAGENT_THREAD_MARKER_${Date.now()}`;
      const delegationMarker = `MULTIAGENT_DELEGATION_PROMPT_${Date.now()}`;
      const workerName = `e2e-worker-${Date.now()}`;
      const worker = await createAgent({
        name: workerName,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-5-20250929' },
        system:
          `You are the worker subagent for an end-to-end test. ` +
          `When asked anything, respond with exactly "${marker}" and no other text.`,
      });

      const coordinator = await createAgent({
        name: `e2e-coordinator-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-5-20250929' },
        system:
          `You coordinate work by delegating to worker subagents. ` +
          `For this test you MUST use the Agent tool exactly once with ` +
          `subagent_type="${workerName}" and prompt="${delegationMarker}". ` +
          `After the subagent responds, reply with the exact subagent marker and no other text.`,
        multiagent: { type: 'coordinator', agents: [worker.id] },
      });
      expect(coordinator.multiagent).toEqual({
        type: 'coordinator',
        agents: [{ type: 'agent', id: worker.id, version: worker.version }],
      });

      const session = await createSession(coordinator.id);

      const submit = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
        method: 'POST',
        body: JSON.stringify({
          events: [
            {
              type: 'user.message',
              content: [
                {
                  type: 'text',
                  text:
                    `Use the Agent tool with subagent_type "${workerName}" now. ` +
                    `Use the exact prompt "${delegationMarker}", then return only its marker.`,
                },
              ],
            },
          ],
          request_id: `real-multiagent-${Date.now()}`,
        }),
      });
      expect(submit.status).toBe(200);

      const primaryEvents = await waitForPrimaryProjection(
        session.id,
        delegationMarker,
        marker,
        180_000,
      );
      const primarySentMessages = primaryEvents.filter(
        (event) => event.type === 'agent.thread_message_sent',
      );
      const primaryReceivedMessages = primaryEvents.filter(
        (event) => event.type === 'agent.thread_message_received',
      );
      const matchingSentMessages = primarySentMessages.filter((event) =>
        textFromMessages([event]).includes(delegationMarker),
      );
      expect(matchingSentMessages).toHaveLength(1);
      expect(textFromMessages(primaryReceivedMessages)).toContain(marker);

      const threads = await waitForThreads(session.id, 2, 60_000);
      const thread = threads.find((candidate) => candidate.agent.id === worker.id);
      expect(thread).toBeDefined();
      if (!thread) throw new Error(`expected worker thread for ${worker.id}`);
      expect(thread.session_id).toBe(session.id);
      expect(thread.type).toBe('session_thread');
      expect(thread.agent.id).toBe(worker.id);
      expect(thread.agent.version).toBe(worker.version);
      expect(thread.agent.name).toBe(worker.name);

      const threadEvents = await waitForThreadEventsContaining(
        session.id,
        thread.id,
        marker,
        30_000,
      );
      expect(threadEvents.some((event) => event.type === 'agent.message')).toBe(true);
      expect(
        primaryReceivedMessages.some((event) => event.from_session_thread_id === thread.id),
      ).toBe(true);
      expect(matchingSentMessages[0]?.to_session_thread_id).toBe(thread.id);

      const usage = await waitForSessionUsage(session.id, 30_000);
      expect(usage.usage?.input_tokens ?? 0).toBeGreaterThan(0);
      expect(usage.usage?.output_tokens ?? 0).toBeGreaterThan(0);
    },
    { timeout: 240_000, retry: realClaudeRetry },
  );

  async function createAgent(input: {
    name: string;
    model: { provider: string; id: string };
    system: string;
    multiagent?: { type: 'coordinator'; agents: string[] };
  }): Promise<AgentResponse> {
    const res = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: input.name,
        model: input.model,
        system: input.system,
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
        ...(input.multiagent ? { multiagent: input.multiagent } : {}),
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

  async function waitForThreads(
    sessionId: string,
    minCount: number,
    timeoutMs: number,
  ): Promise<SessionThreadResponse[]> {
    const deadline = Date.now() + timeoutMs;
    let last: SessionThreadResponse[] = [];
    while (Date.now() < deadline) {
      const res = await apiCall(cfg, `/v1/sessions/${sessionId}/threads`, { method: 'GET' });
      expect(res.status).toBe(200);
      last = res.json<{ data: SessionThreadResponse[] }>().data;
      if (last.length >= minCount) return last;
      await delay(500);
    }
    throw new Error(`expected at least ${minCount} session thread(s), got ${last.length}`);
  }

  async function waitForThreadEventsContaining(
    sessionId: string,
    threadId: string,
    text: string,
    timeoutMs: number,
  ): Promise<EventResponse[]> {
    const deadline = Date.now() + timeoutMs;
    let last: EventResponse[] = [];
    while (Date.now() < deadline) {
      const res = await apiCall(cfg, `/v1/sessions/${sessionId}/threads/${threadId}/events`, {
        method: 'GET',
      });
      expect(res.status).toBe(200);
      last = res.json<{ data: EventResponse[]; has_more: boolean; next_page: string | null }>()
        .data;
      if (textFromMessages(last).includes(text)) return last;
      await delay(500);
    }
    throw new Error(
      `thread ${threadId} events did not contain ${text}; events=${JSON.stringify(last)}`,
    );
  }

  async function waitForPrimaryProjection(
    sessionId: string,
    sentText: string,
    receivedText: string,
    timeoutMs: number,
  ): Promise<EventResponse[]> {
    const deadline = Date.now() + timeoutMs;
    let last: EventResponse[] = [];
    while (Date.now() < deadline) {
      const res = await apiCall(cfg, `/v1/sessions/${sessionId}/events?limit=1000`, {
        method: 'GET',
      });
      expect(res.status).toBe(200);
      last = res.json<{ data: EventResponse[] }>().data;
      const sentMessages = last.filter((event) => event.type === 'agent.thread_message_sent');
      const receivedMessages = last.filter(
        (event) => event.type === 'agent.thread_message_received',
      );
      if (
        textFromMessages(sentMessages).includes(sentText) &&
        textFromMessages(receivedMessages).includes(receivedText)
      ) {
        return last;
      }
      await delay(500);
    }
    throw new Error(
      `primary events did not contain agent.thread_message_sent with ${sentText} ` +
        `and agent.thread_message_received with ${receivedText}`,
    );
  }

  async function waitForSessionUsage(
    sessionId: string,
    timeoutMs: number,
  ): Promise<SessionResponse> {
    const deadline = Date.now() + timeoutMs;
    let last: SessionResponse | null = null;
    while (Date.now() < deadline) {
      const res = await apiCall(cfg, `/v1/sessions/${sessionId}`, { method: 'GET' });
      expect(res.status).toBe(200);
      last = res.json<SessionResponse>();
      if ((last.usage?.input_tokens ?? 0) > 0 && (last.usage?.output_tokens ?? 0) > 0) {
        return last;
      }
      await delay(500);
    }
    throw new Error(
      `session ${sessionId} did not report token usage; last=${JSON.stringify(last)}`,
    );
  }

  function textFromMessages(items: EventResponse[]): string {
    return items
      .map((item) => blocksToText(item.content))
      .filter(Boolean)
      .join('\n');
  }

  function blocksToText(value: unknown): string {
    if (typeof value === 'string') return value;
    if (!Array.isArray(value)) return '';
    return value
      .filter((block): block is { type: string; text: string } =>
        Boolean(
          block &&
          typeof block === 'object' &&
          (block as { type?: unknown }).type === 'text' &&
          typeof (block as { text?: unknown }).text === 'string',
        ),
      )
      .map((block) => block.text)
      .join('\n');
  }

  function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
});
