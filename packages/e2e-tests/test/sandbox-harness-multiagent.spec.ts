// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Layer B.1: sandbox harness multi-agent mode.
 *
 * This black-box spec covers the OpenSandbox/in-sandbox bridge for
 * multi-agent execution. It creates a coordinator with `metadata.harness`
 * forcing the sandbox-harness path, then verifies the live session used the
 * Claude Agent tool and returned the worker subagent marker.
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
import { REAL_AGENT } from './real-agent-config.js';

interface AgentResponse {
  id: string;
  name: string;
  version: number;
}

interface SessionResponse {
  id: string;
}

interface SseFrame {
  type: string;
  processed_at: string;
  content?: unknown;
  name?: string;
  input?: Record<string, unknown> | null;
  [k: string]: unknown;
}

const SANDBOX_HARNESS_MODEL = process.env['ORCA_E2E_SANDBOX_HARNESS_MODEL'] ?? 'claude-sonnet-4-6';

describe.skipIf(REAL_AGENT.isNativeSdk)('Layer B.1: sandbox harness multi-agent mode', () => {
  let cfg: OrcaClientConfig;
  let environmentId: string;
  const created: { sessions: string[]; agents: string[] } = { sessions: [], agents: [] };

  beforeAll(async () => {
    if (process.env['ORCA_E2E_SANDBOX_HARNESS'] !== '1') {
      throw new Error(
        'sandbox harness multi-agent e2e is opt-in. Set ORCA_E2E_SANDBOX_HARNESS=1 and run against SANDBOX_RUNTIME=opensandbox.',
      );
    }
    if (!process.env['ANTHROPIC_API_KEY']) {
      throw new Error('sandbox harness multi-agent e2e requires ANTHROPIC_API_KEY.');
    }
    const seeded = await seedWorkspaceApiKey();
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    environmentId = await createTestEnvironment(cfg, 'sandbox-multiagent-env');
  });

  afterAll(async () => {
    if (process.env['ORCA_E2E_SANDBOX_HARNESS_KEEP_RESOURCES'] !== '1' && cfg && environmentId) {
      await deleteTestEnvironment(cfg, environmentId).catch(() => {});
    }
  });

  afterEach(async () => {
    if (process.env['ORCA_E2E_SANDBOX_HARNESS_KEEP_RESOURCES'] === '1') {
      return;
    }
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

  it('runs a coordinator that delegates to a worker subagent inside OpenSandbox', async () => {
    const marker = `SANDBOX_MULTIAGENT_MARKER_${Date.now()}`;
    const workerName = `sandbox-worker-${Date.now()}`;
    const worker = await createAgent({
      name: workerName,
      system:
        `You are the OpenSandbox worker subagent. ` +
        `When asked anything, respond with exactly "${marker}" and no other text.`,
    });

    const coordinator = await createAgent({
      name: `sandbox-coordinator-${Date.now()}`,
      system:
        `You coordinate work by delegating to worker subagents. ` +
        `Use the Agent tool for the worker and return only the worker marker.`,
      metadata: { harness: 'claude_code' },
      multiagent: { type: 'coordinator', agents: [worker.id] },
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
                  `Use the Agent tool exactly once with subagent_type "${workerName}" ` +
                  `and prompt "Return your marker.". Then reply with only the marker.`,
              },
            ],
          },
        ],
        request_id: `sandbox-multiagent-${Date.now()}`,
      }),
    });
    expect(submit.status, submit.text).toBe(200);

    const frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 480_000,
      until: (items) =>
        items.some((frame) =>
          ['session.status_idle', 'session.error', 'result'].includes(frame.type),
        ),
    });
    const failure = frames.find((frame) => frame.type === 'session.error');
    expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();
    expect(frames.some((frame) => frame.type === 'session.status_idle')).toBe(true);
    expect(textFromFrames(frames)).toContain(marker);
    expect(frames.some((frame) => frame.type === 'agent.tool_use' && frame.name === 'Agent')).toBe(
      true,
    );

    const listed = await apiCall(cfg, `/v1/sessions/${session.id}/events?limit=200`, {
      method: 'GET',
    });
    expect(listed.status, listed.text).toBe(200);
    const body = listed.json<{ data: SseFrame[] }>();
    expect(textFromFrames(body.data)).toContain(marker);
    expect(
      body.data.some((event) => event.type === 'agent.tool_use' && event.name === 'Agent'),
    ).toBe(true);
  }, 540_000);

  async function createAgent(input: {
    name: string;
    system: string;
    metadata?: Record<string, unknown>;
    multiagent?: { type: 'coordinator'; agents: string[] };
  }): Promise<AgentResponse> {
    const res = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: input.name,
        model: { provider: 'anthropic', id: SANDBOX_HARNESS_MODEL },
        system: input.system,
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: input.metadata ?? {},
        ...(input.multiagent ? { multiagent: input.multiagent } : {}),
      }),
    });
    expect(res.status, res.text).toBe(200);
    const agent = res.json<AgentResponse>();
    created.agents.push(agent.id);
    return agent;
  }

  async function createSession(agentId: string): Promise<SessionResponse> {
    const res = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({ environment_id: environmentId, agent_id: agentId }),
    });
    expect(res.status, res.text).toBe(200);
    const session = res.json<SessionResponse>();
    created.sessions.push(session.id);
    return session;
  }
});

async function collectSseFrames(
  cfg: OrcaClientConfig,
  sessionId: string,
  opts: { deadlineMs: number; until: (frames: SseFrame[]) => boolean },
): Promise<SseFrame[]> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), opts.deadlineMs);
  const frames: SseFrame[] = [];
  try {
    const res = await fetch(`${cfg.baseURL}/v1/sessions/${sessionId}/events/stream?from_cursor=0`, {
      headers: { 'x-api-key': cfg.apiKey, accept: 'text/event-stream' },
      signal: ac.signal,
    });
    expect(res.status).toBe(200);
    if (!res.body) throw new Error('SSE response had no body');

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffered = '';
    while (!opts.until(frames)) {
      const chunk = await reader.read();
      if (chunk.done) break;
      buffered += decoder.decode(chunk.value, { stream: true });
      buffered = drainSseBuffer(buffered, frames);
    }
    await reader.cancel().catch(() => {});
    return frames;
  } finally {
    clearTimeout(timer);
    ac.abort();
  }
}

function drainSseBuffer(input: string, frames: SseFrame[]): string {
  let rest = input;
  while (true) {
    const idx = rest.indexOf('\n\n');
    if (idx === -1) return rest;
    const block = rest.slice(0, idx);
    rest = rest.slice(idx + 2);
    if (block.startsWith(':')) continue;
    const dataLine = block.split('\n').find((line) => line.startsWith('data: '));
    if (!dataLine) continue;
    try {
      frames.push(JSON.parse(dataLine.slice(6)) as SseFrame);
    } catch {
      /* ignore non-JSON frames */
    }
  }
}

function textFromFrames(frames: SseFrame[]): string {
  return frames
    .filter((frame) => frame.type === 'agent.message' || frame.type === 'agent.thread_message_sent')
    .map((frame) => {
      expect(typeof frame.processed_at).toBe('string');
      expect(Number.isNaN(Date.parse(frame.processed_at))).toBe(false);
      expect(Object.hasOwn(frame, 'message')).toBe(false);
      return assistantText(frame);
    })
    .filter(Boolean)
    .join('\n');
}

function assistantText(f: SseFrame): string {
  const blocks = (Array.isArray(f.content) ? f.content : []) as Array<{
    type: string;
    text?: string;
  }>;
  return blocks
    .filter((block) => block?.type === 'text' && typeof block.text === 'string')
    .map((block) => block.text!)
    .join('\n');
}
