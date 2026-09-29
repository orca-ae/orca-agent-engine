// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @orca/e2e-tests — Layer B: guardrail enforcement against a live agent loop.
 *
 * `docs/managed-agents/guardrails.md` sets this bar itself: "guardrails at each
 * tier against a live session, observing both a denial and an approval in the
 * transcript." Everything below it is covered — the evaluation library by unit
 * tests, the control plane by integration and by `guardrails-wire.spec.ts` —
 * but nothing joined the two ends until here. The path this exercises is
 *
 *   POST /apis/policy.runorca.ai/v1/guardrails
 *     -> composeGuardrails (tier resolution)
 *     -> prepared runtime handed to the harness
 *     -> evaluateGuardrails at the tool-call gate
 *     -> a verdict the transcript can actually show
 *
 * and every link in it is real: real registry, real harness, real selected model, real
 * sandbox. A unit test can assert the engine returns `deny`; only this can
 * assert the agent was actually stopped.
 *
 * **Scope discipline.** Both cases use `scope: 'explicit'` guardrails attached
 * by reference — one from the Agent, one from the Session. A
 * `workspace`-scoped rule would apply to every session in the shared e2e
 * workspace, including other specs in the same run, so a leaked one would look
 * like an unrelated suite breaking.
 *
 * **No skips.** Like the sibling Layer B spec, a missing selected provider key
 * hard-fails rather than silently passing — a silently skipped guardrail test
 * is worse than no guardrail test.
 */
import { REAL_AGENT, requireRealAgentKey } from './real-agent-config.js';
import { randomUUID } from 'node:crypto';
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedWorkspaceApiKey } from '../src/seed.js';
import { createTestEnvironment, deleteTestEnvironment } from './environment-helpers.js';
import { collectSseFrames, type SseFrame } from './session-observation.js';

const GUARDRAILS = '/apis/policy.runorca.ai/v1/guardrails';
const BASH_TOOL = 'mcp__orca__bash';
const REAL_CLAUDE_RETRY = Number(process.env['ORCA_E2E_REAL_CLAUDE_RETRY'] ?? '2');

interface AgentResponse {
  id: string;
}
interface SessionResponse {
  id: string;
}

const TOOL_RESULT_TYPES = ['agent.tool_result', 'agent.mcp_tool_result'];

describe(`Layer B: guardrail enforcement (${REAL_AGENT.harness} + live sandbox)`, () => {
  let cfg: OrcaClientConfig;
  let environmentId: string;
  const runId = randomUUID().replaceAll('-', '').slice(0, 12);
  const created: { sessions: string[]; agents: string[]; guardrails: string[] } = {
    sessions: [],
    agents: [],
    guardrails: [],
  };

  beforeAll(async () => {
    requireRealAgentKey();
    const seeded = await seedWorkspaceApiKey();
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    environmentId = await createTestEnvironment(cfg, 'guardrails-agent-env');
  });

  afterAll(async () => {
    if (cfg && environmentId) await deleteTestEnvironment(cfg, environmentId).catch(() => {});
  });

  afterEach(async () => {
    // Reverse dependency order: a guardrail an agent still names cannot be
    // deleted, and that refusal is deliberate — see `guardrails-wire.spec.ts`.
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
    for (const id of created.guardrails.splice(0)) {
      await apiCall(cfg, `${GUARDRAILS}/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
  });

  async function createGuardrail(body: Record<string, unknown>): Promise<string> {
    const res = await apiCall(cfg, GUARDRAILS, {
      method: 'POST',
      body: JSON.stringify({ scope: 'explicit', ...body }),
    });
    expect(res.status, res.text).toBe(201);
    const id = res.json<{ id: string }>().id;
    created.guardrails.push(id);
    return id;
  }

  async function createAgent(name: string, guardrailIds: string[]): Promise<string> {
    const res = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      headers: { 'orca-beta': 'guardrails' },
      body: JSON.stringify({
        name,
        model: REAL_AGENT.model,
        system:
          'You are a shell assistant. When asked to run a command, call the ' +
          `${BASH_TOOL} tool exactly once with the command supplied. Do not answer from memory. ` +
          'If the tool returns an error, report the error text verbatim and stop.',
        tools: [{ type: 'agent_toolset_20260401' }],
        mcp_servers: [],
        skills: [],
        guardrail_ids: guardrailIds,
        metadata: REAL_AGENT.metadata,
      }),
    });
    expect(res.status, res.text).toBe(200);
    const id = res.json<AgentResponse>().id;
    created.agents.push(id);
    return id;
  }

  async function startSession(agent: unknown): Promise<string> {
    const res = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      headers: { 'orca-beta': 'guardrails' },
      body: JSON.stringify(
        typeof agent === 'string'
          ? { environment_id: environmentId, agent_id: agent }
          : { environment_id: environmentId, agent },
      ),
    });
    expect(res.status, res.text).toBe(200);
    const id = res.json<SessionResponse>().id;
    created.sessions.push(id);
    return id;
  }

  async function askToRunBash(sessionId: string, marker: string): Promise<void> {
    const res = await apiCall(cfg, `/v1/sessions/${sessionId}/events`, {
      method: 'POST',
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            content: [
              {
                type: 'text',
                text: `Call ${BASH_TOOL} once with command exactly ${JSON.stringify(`printf %s ${marker}`)}.`,
              },
            ],
          },
        ],
        request_id: `guardrail-agent-${marker}`,
      }),
    });
    expect(res.status, res.text).toBe(200);
  }

  function toolResults(frames: SseFrame[]): SseFrame[] {
    return frames.filter((frame) => TOOL_RESULT_TYPES.includes(frame.type));
  }

  it(
    'denies a tool call through an Agent-tier guardrail and says so in the transcript',
    async () => {
      const reason = `blocked by e2e ${runId}`;
      const guardrailId = await createGuardrail({
        name: `deny-bash-${runId}`,
        rule: {
          kind: 'builtin',
          builtin: 'block_tools',
          params: { tools: [BASH_TOOL], reason },
        },
      });
      const agentId = await createAgent(`deny-bash-agent-${runId}`, [guardrailId]);
      const sessionId = await startSession(agentId);
      await askToRunBash(sessionId, `deny-${runId}`);

      const frames = await collectSseFrames(cfg, sessionId, {
        deadlineMs: 110_000,
        until: (items) =>
          items.some((frame) => frame.type === 'session.error') ||
          toolResults(items).length >= 1 ||
          items.some((frame) => frame.type === 'session.status_idle'),
      });
      const failure = frames.find((frame) => frame.type === 'session.error');
      expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();

      // A `deny` at tool_call becomes a tool error the agent reads — not a
      // dropped turn, and not a silent success.
      const results = toolResults(frames);
      expect(results.length, JSON.stringify(results)).toBeGreaterThanOrEqual(1);
      expect(results[0]?.is_error, JSON.stringify(results[0])).toBe(true);
      expect(JSON.stringify(results[0])).toContain(reason);
    },
    { timeout: 180_000, retry: REAL_CLAUDE_RETRY },
  );

  it(
    'asks through a Session-tier guardrail and runs the tool once approved',
    async () => {
      // `ask` reuses the tool-confirmation exchange that already exists, which
      // is exactly why `ask` is confined to `tool_call`: guardrails add no new
      // client protocol. This asserts that reuse end to end.
      const guardrailId = await createGuardrail({
        name: `ask-bash-${runId}`,
        rule: {
          kind: 'builtin',
          builtin: 'require_approval_for_tools',
          params: { tools: [BASH_TOOL] },
        },
      });
      // Attached at the Session tier, so the Agent itself carries no guardrail
      // — the rule reaches this run only through `agent_with_overrides`.
      const agentId = await createAgent(`ask-bash-agent-${runId}`, []);
      const sessionId = await startSession({
        type: 'agent_with_overrides',
        id: agentId,
        guardrail_ids: [guardrailId],
      });
      const marker = `ask-${runId}`;
      await askToRunBash(sessionId, marker);

      const pending = await collectSseFrames(cfg, sessionId, {
        deadlineMs: 110_000,
        until: (items) =>
          items.some((frame) => frame.type === 'session.error') ||
          items.some((frame) => frame.type === 'session.status_idle'),
      });
      const failure = pending.find((frame) => frame.type === 'session.error');
      expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();

      // `requires_action` is a stop reason on `session.status_idle`, not an
      // event type of its own — the turn parks rather than ending, and the
      // pending tool-use event ids ride on the stop reason.
      const idle = pending.find((frame) => frame.type === 'session.status_idle');
      expect(idle, JSON.stringify(pending.map((f) => f.type))).toBeDefined();
      const stopReason = (idle as { stop_reason?: { type?: string; event_ids?: string[] } })
        .stop_reason;
      expect(stopReason?.type, JSON.stringify(idle)).toBe('requires_action');

      const toolUse = pending.find((frame) =>
        ['agent.tool_use', 'agent.mcp_tool_use'].includes(frame.type),
      );
      expect(toolUse, JSON.stringify(pending.map((f) => f.type))).toBeDefined();
      // The confirmation is keyed by the tool-use *event* id, which is what the
      // stop reason names too.
      const toolUseId = toolUse!.id as string;
      expect(stopReason?.event_ids, JSON.stringify(stopReason)).toContain(toolUseId);

      const approve = await apiCall(cfg, `/v1/sessions/${sessionId}/events`, {
        method: 'POST',
        body: JSON.stringify({
          // `result`, not `approved`: the event schema is `.strict()`, so an
          // invented field is a 400 rather than an ignored key.
          events: [{ type: 'user.tool_confirmation', tool_use_id: toolUseId, result: 'allow' }],
        }),
      });
      expect(approve.status, approve.text).toBe(200);

      const after = await collectSseFrames(cfg, sessionId, {
        deadlineMs: 110_000,
        until: (items) =>
          items.some((frame) => frame.type === 'session.error') || toolResults(items).length >= 1,
      });
      const afterFailure = after.find((frame) => frame.type === 'session.error');
      expect(afterFailure, afterFailure ? JSON.stringify(afterFailure) : undefined).toBeUndefined();

      // Approved becomes allow: the tool actually ran, and its output is the
      // proof — an approval that produced no tool result would mean the
      // confirmation never reached the parked call.
      const results = toolResults(after);
      expect(results.length, JSON.stringify(after.map((f) => f.type))).toBeGreaterThanOrEqual(1);
      expect(results[0]?.is_error, JSON.stringify(results[0])).toBe(false);
      expect(JSON.stringify(results[0])).toContain(marker);
    },
    { timeout: 180_000, retry: REAL_CLAUDE_RETRY },
  );
});
