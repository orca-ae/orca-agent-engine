// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Layer B.1: sandbox harness mode.
 *
 * This spec drives the real stack through the Claude Code or Codex SDK
 * in-sandbox topology. It is intentionally separate from real-agent-loop.spec:
 * normal `pnpm e2e:agent` can keep using the fast local sandbox runtime, while
 * CI opts into this file with a real cloud sandbox. Both SDKs use the same
 * harness-server HTTP bridge, image, resource mounts and sandbox lifecycle.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import { createHash, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedWorkspaceApiKey } from '../src/seed.js';
import {
  downloadFileText,
  listScopedOutputFiles,
  waitForScopedOutputFile,
} from './output-file-helpers.js';
import { createTestEnvironment, deleteTestEnvironment } from './environment-helpers.js';
import { COLOCATED_AGENT, REAL_AGENT, requireRealAgentKey } from './real-agent-config.js';
import { expectTicketCallbackRoundTrip } from './custom-tool-result-assertions.js';

interface AgentResponse {
  id: string;
  metadata?: Record<string, unknown>;
}

interface SkillResponse {
  id: string;
  latest_version: string;
}

interface SkillVersionResponse {
  version: string;
}

interface SessionResponse {
  id: string;
  agent_id: string;
  status: string;
  sandbox_handle_id?: string | null;
  usage?: {
    input_tokens?: number;
    output_tokens?: number;
  };
  resources: Array<{
    id: string;
    type: 'file' | 'memory_store' | 'github_repository';
    file_id: string | null;
    memory_store_id: string | null;
    mount_path: string;
  }>;
}

interface FileResponse {
  id: string;
}

interface MemoryStoreResponse {
  id: string;
  name: string;
}

interface MemoryVersion {
  id: string;
  memory_id: string;
  path: string;
  content_sha256: string | null;
}

interface SseFrame {
  type: string;
  processed_at: string;
  seq?: string;
  produced_by?: string;
  content?: unknown;
  [k: string]: unknown;
}

function isTerminalIdle(frame: SseFrame): boolean {
  const reason = frame.stop_reason;
  return (
    frame.type === 'session.status_idle' &&
    typeof reason === 'object' &&
    reason !== null &&
    (reason as { type?: unknown }).type === 'end_turn'
  );
}

interface GatewayUsageEvent {
  schema?: string;
  event_id?: string;
  status?: string;
  traffic_kind?: string;
  scope?: {
    workspace_id?: string;
    session_id?: string;
  };
  destination?: string;
  provider?: string;
  route?: string;
  model?: string;
  input_tokens?: number;
  output_tokens?: number;
}

interface GatewayUsageBatch {
  events: GatewayUsageEvent[];
  inputTokens: number;
  outputTokens: number;
}

const EXPECTED_TEXT = `in-sandbox ${COLOCATED_AGENT.harness} e2e ok`;
const GUARDRAILS = '/apis/policy.runorca.ai/v1/guardrails';
const SANDBOX_HARNESS_MODEL = COLOCATED_AGENT.model.id;
const registryInternalBaseURL =
  process.env['REGISTRY_INTERNAL_BASE_URL'] ?? 'http://localhost:8081';
const here = dirname(fileURLToPath(import.meta.url));
const defaultInternalTokenFile = resolve(here, '../../../services/dev/run/internal-service-token');
const ALWAYS_ALLOW_SANDBOX_TOOLS = [
  {
    type: 'agent_toolset_20260401',
    default_config: { permission_policy: { type: 'always_allow' } },
  },
];

function assertSkillCleanupResponse(
  response: { status: number; text: string },
  operation: string,
): void {
  if (response.status !== 200 && response.status !== 404) {
    throw new Error(`${operation} failed: ${response.status} ${response.text}`);
  }
}

async function deleteSkillAndVersions(cfg: OrcaClientConfig, skillId: string): Promise<void> {
  const versions = await apiCall(cfg, `/v1/skills/${skillId}/versions?limit=1000`, {
    method: 'GET',
  });
  assertSkillCleanupResponse(versions, `list versions for ${skillId}`);
  if (versions.status === 200) {
    for (const version of versions.json<{ data: SkillVersionResponse[] }>().data) {
      const deletedVersion = await apiCall(
        cfg,
        `/v1/skills/${skillId}/versions/${version.version}`,
        {
          method: 'DELETE',
          body: JSON.stringify({}),
        },
      );
      assertSkillCleanupResponse(deletedVersion, `delete version ${skillId}@${version.version}`);
    }
  }
  const deletedSkill = await apiCall(cfg, `/v1/skills/${skillId}`, {
    method: 'DELETE',
    body: JSON.stringify({}),
  });
  assertSkillCleanupResponse(deletedSkill, `delete skill ${skillId}`);
}

describe('Layer B.1: sandbox harness mode (live registry + harness + OpenSandbox)', () => {
  let cfg: OrcaClientConfig;
  let githubToken: string;
  let environmentId: string;
  let workspaceId: string;
  let internalServiceToken: string;
  const created: {
    sessions: string[];
    agents: string[];
    files: string[];
    memStores: string[];
    skills: string[];
    guardrails: string[];
  } = {
    sessions: [],
    agents: [],
    files: [],
    memStores: [],
    skills: [],
    guardrails: [],
  };

  beforeAll(async () => {
    if (process.env['ORCA_E2E_SANDBOX_HARNESS'] !== '1') {
      throw new Error(
        'sandbox harness e2e is opt-in. Set ORCA_E2E_SANDBOX_HARNESS=1 and run against a stack booted with an endpoint-capable runtime such as SANDBOX_RUNTIME=opensandbox.',
      );
    }
    requireRealAgentKey();
    githubToken = process.env['GITHUB_TOKEN'] ?? '';
    if (!githubToken) {
      throw new Error(
        'sandbox harness resource e2e requires GITHUB_TOKEN so it can attach and clone the repository through the real Git credential path.',
      );
    }
    const tokenFile = process.env['INTERNAL_SERVICE_TOKEN_FILE'] ?? defaultInternalTokenFile;
    const [seeded, token] = await Promise.all([seedWorkspaceApiKey(), readFile(tokenFile, 'utf8')]);
    workspaceId = seeded.workspaceId;
    internalServiceToken = token.trim();
    expect(internalServiceToken.length).toBeGreaterThanOrEqual(32);
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    environmentId = await createTestEnvironment(cfg, 'sandbox-harness-env');
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
    for (const id of created.files.splice(0)) {
      await apiCall(cfg, `/v1/files/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
    for (const id of created.memStores.splice(0)) {
      await apiCall(cfg, `/v1/memory_stores/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
    for (const id of created.skills.splice(0)) {
      await deleteSkillAndVersions(cfg, id).catch((error) => {
        console.warn(`sandbox-harness-agent: cleanup of Skill ${id} failed`, error);
      });
    }
    for (const id of created.guardrails.splice(0)) {
      await apiCall(cfg, `${GUARDRAILS}/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
  });

  it(`runs a ${COLOCATED_AGENT.harness} agent through its colocated bridge`, async () => {
    const guardrailRes = await apiCall(cfg, GUARDRAILS, {
      method: 'POST',
      body: JSON.stringify({
        scope: 'explicit',
        name: `sandbox-harness-token-budget-${Date.now()}`,
        phases: ['request'],
        rule: {
          kind: 'builtin',
          builtin: 'token_budget',
          params: { max_total_tokens: 1 },
        },
      }),
    });
    expect(guardrailRes.status, guardrailRes.text).toBe(201);
    const guardrailId = guardrailRes.json<{ id: string }>().id;
    created.guardrails.push(guardrailId);

    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      headers: { 'orca-beta': 'guardrails' },
      body: JSON.stringify({
        name: `sandbox-harness-${COLOCATED_AGENT.harness}-${Date.now()}`,
        model: COLOCATED_AGENT.model,
        system:
          'You are running in the sandbox harness e2e. Reply exactly with the requested phrase and do not add extra text.',
        tools: [],
        mcp_servers: [],
        skills: [],
        guardrail_ids: [guardrailId],
        metadata: COLOCATED_AGENT.metadata,
      }),
    });
    expect(agentRes.status, agentRes.text).toBe(200);
    const agent = agentRes.json<AgentResponse>();
    created.agents.push(agent.id);
    expect(agent.metadata?.['harness']).toBe(COLOCATED_AGENT.harness);

    const sessionRes = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      headers: { 'orca-beta': 'guardrails' },
      body: JSON.stringify({ environment_id: environmentId, agent_id: agent.id }),
    });
    expect(sessionRes.status, sessionRes.text).toBe(200);
    const session = sessionRes.json<SessionResponse>();
    created.sessions.push(session.id);

    const prepared = await prepareSessionExecution(workspaceId, session.id, internalServiceToken);
    expect(prepared.session).toMatchObject({
      usage_writer: REAL_AGENT.isNativeSdk ? 'harness' : 'ai-gateway',
    });

    const submitRes = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            content: [{ type: 'text', text: `Reply exactly: ${EXPECTED_TEXT}` }],
          },
        ],
        request_id: `sandbox-harness-${Date.now()}`,
      }),
    });
    expect(submitRes.status, submitRes.text).toBe(200);

    const frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 420_000,
      until: (items) =>
        items.some((frame) =>
          ['session.status_idle', 'session.error', 'result'].includes(frame.type),
        ),
    });
    const failure = frames.find((frame) => frame.type === 'session.error');
    expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();
    const text = frames.filter(isAssistantTextFrame).map(assistantText).join('\n').trim();
    expect(text).toContain(EXPECTED_TEXT);
    expect(frames.some((frame) => frame.type === 'session.status_idle')).toBe(true);

    const body = await waitForListedEvents(cfg, session.id, (events) => {
      const types = new Set(events.map((event) => event.type));
      return (
        types.has('user.message') &&
        types.has('session.status_idle') &&
        events.some(
          (event) => event.type === 'agent.message' && assistantText(event).includes(EXPECTED_TEXT),
        )
      );
    });
    expect(body.data.some((event) => event.type === 'user.message')).toBe(true);
    expect(body.data.some((event) => event.type === 'agent.message')).toBe(true);
    expect(body.data.some((event) => event.type === 'session.status_idle')).toBe(true);
    const agentMessage = body.data.find(
      (event) => event.type === 'agent.message' && assistantText(event).includes(EXPECTED_TEXT),
    );
    expect(agentMessage).toBeDefined();
    expect(typeof agentMessage?.processed_at).toBe('string');
    expect(Number.isNaN(Date.parse(agentMessage!.processed_at))).toBe(false);
    expect(Object.hasOwn(agentMessage!, 'message')).toBe(false);

    const assigned = await apiCall(cfg, `/v1/sessions/${session.id}`, {
      method: 'GET',
      headers: { 'orca-beta': '1' },
    });
    expect(assigned.status, assigned.text).toBe(200);
    const execution = assigned.json<Record<string, unknown>>();
    expect(execution['sandbox_handle_id']).toEqual(expect.any(String));
    expect(execution['host_environment_id'] ?? null).toBeNull();

    const usage = await waitForSessionUsage(cfg, session.id, 60_000);
    const expectedInputTokens = usage.usage?.input_tokens ?? 0;
    const expectedOutputTokens = usage.usage?.output_tokens ?? 0;
    expect(expectedInputTokens).toBeGreaterThan(0);
    expect(expectedOutputTokens).toBeGreaterThan(0);

    const kafkaUsageTopic = process.env['ORCA_E2E_GATEWAY_KAFKA_USAGE_TOPIC'];
    if (kafkaUsageTopic) {
      const kafkaUsage = await waitForGatewayKafkaUsage(
        session.id,
        kafkaUsageTopic,
        expectedInputTokens,
        expectedOutputTokens,
        60_000,
      );
      expect(kafkaUsage.events.length).toBeGreaterThan(0);
      for (const event of kafkaUsage.events) {
        expect(event).toMatchObject({
          schema: '2',
          status: 'ok',
          traffic_kind: 'model',
          scope: { workspace_id: 'ws_e2e_tests', session_id: session.id },
          destination: 'anthropic-proxy',
          provider: 'anthropic',
          route: 'llm-messages',
          model: SANDBOX_HARNESS_MODEL,
        });
        expect(event.event_id).toBeTruthy();
      }
      expect(kafkaUsage.inputTokens).toBe(expectedInputTokens);
      expect(kafkaUsage.outputTokens).toBe(expectedOutputTokens);
    }

    const deniedRes = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            content: [
              { type: 'text', text: 'This request must be denied before another LLM call.' },
            ],
          },
        ],
        request_id: `sandbox-harness-budget-denied-${Date.now()}`,
      }),
    });
    expect(deniedRes.status, deniedRes.text).toBe(200);

    const deniedEvents = await waitForListedEvents(cfg, session.id, (events) =>
      events.some(isTokenBudgetDeniedFrame),
    );
    expect(deniedEvents.data.some(isTokenBudgetDeniedFrame)).toBe(true);
  }, 480_000);

  it('materializes and progressively discloses a pinned Skill in-sandbox', async () => {
    const suffix = Date.now();
    const marker = `IN_SANDBOX_SKILL_MARKER_${suffix}`;
    const directory = `sandbox-harness-skill-${suffix}`;
    const skillMarkdownPath = `/workspace/skills/${directory}/SKILL.md`;
    const markerPath = `/workspace/skills/${directory}/references/marker.txt`;
    const form = new FormData();
    form.set(
      'files[]',
      new Blob(
        [
          [
            '---',
            `name: ${directory}`,
            'description: in-sandbox progressive disclosure fixture',
            '---',
            '',
            'When asked for the configured marker, read references/marker.txt and reply with',
            'exactly its contents. Do not guess the value.',
          ].join('\n'),
        ],
        { type: 'text/markdown' },
      ),
      `${directory}/SKILL.md`,
    );
    form.append(
      'files[]',
      new Blob([marker], { type: 'text/plain' }),
      `${directory}/references/marker.txt`,
    );
    const skillRes = await apiCall(cfg, '/v1/skills', { method: 'POST', body: form });
    expect(skillRes.status, skillRes.text).toBe(200);
    const skill = skillRes.json<SkillResponse>();
    created.skills.push(skill.id);

    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `sandbox-harness-skill-agent-${suffix}`,
        model: COLOCATED_AGENT.model,
        system:
          'Use the configured Skill and filesystem tools. Read every referenced file before answering.',
        tools: ALWAYS_ALLOW_SANDBOX_TOOLS,
        mcp_servers: [],
        skills: [{ type: 'custom', skill_id: skill.id, version: skill.latest_version }],
        metadata: COLOCATED_AGENT.metadata,
      }),
    });
    expect(agentRes.status, agentRes.text).toBe(200);
    const agent = agentRes.json<AgentResponse>();
    created.agents.push(agent.id);

    const sessionRes = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({ environment_id: environmentId, agent_id: agent.id }),
    });
    expect(sessionRes.status, sessionRes.text).toBe(200);
    const session = sessionRes.json<SessionResponse>();
    created.sessions.push(session.id);

    const submitRes = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            content: [
              {
                type: 'text',
                text: 'Return only the configured marker from the attached Skill.',
              },
            ],
          },
        ],
        request_id: `sandbox-harness-skill-${suffix}`,
      }),
    });
    expect(submitRes.status, submitRes.text).toBe(200);

    const frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 420_000,
      until: (items) =>
        items.some((frame) => ['session.status_idle', 'session.error'].includes(frame.type)),
    });
    const failure = frames.find((frame) => frame.type === 'session.error');
    expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();
    expect(frames.some((frame) => frame.type === 'session.status_idle')).toBe(true);
    expect(frames.filter(isAssistantTextFrame).map(assistantText).join('\n')).toContain(marker);

    const indexedReadUses = frames
      .map((frame, index) => ({ frame, index }))
      .filter(
        ({ frame }) =>
          frame.type === 'agent.tool_use' &&
          frame.name === COLOCATED_AGENT.tools.read &&
          typeof frame.id === 'string' &&
          typeof frame.input === 'object',
      );
    const skillMarkdownRead = indexedReadUses.find(({ frame }) =>
      JSON.stringify(frame.input).includes(skillMarkdownPath),
    );
    const markerRead = indexedReadUses.find(({ frame }) =>
      JSON.stringify(frame.input).includes(markerPath),
    );
    expect(
      skillMarkdownRead,
      `missing ${COLOCATED_AGENT.tools.read}(${skillMarkdownPath}) in ${JSON.stringify(frames)}`,
    ).toBeDefined();
    expect(
      markerRead,
      `missing ${COLOCATED_AGENT.tools.read}(${markerPath}) in ${JSON.stringify(frames)}`,
    ).toBeDefined();
    expect(skillMarkdownRead!.index).toBeLessThan(markerRead!.index);

    expect(markerRead!.frame.type).toBe('agent.tool_use');
    expect(typeof markerRead!.frame.id).toBe('string');
    const markerReadId = markerRead!.frame.id as string;
    const markerResult = frames.find(
      (frame) => frame.type === 'agent.tool_result' && frame.tool_use_id === markerReadId,
    );
    expect(
      markerResult,
      `missing result for ${COLOCATED_AGENT.tools.read}(${markerPath})`,
    ).toBeDefined();
    expect(markerResult?.is_error, JSON.stringify(markerResult)).toBe(false);
    expect(JSON.stringify(markerResult?.content)).toContain(marker);
  }, 480_000);

  it('reads and writes attached resources in the real in-sandbox filesystem', async () => {
    const suffix = Date.now();
    const filePath = '/mnt/inputs/session-context.txt';
    const repoPath = '/workspace/repository';
    const fileMarker = `IN_SANDBOX_FILE_RESOURCE_${suffix}`;
    const ownershipMarker = `IN_SANDBOX_REPO_WRITE_${suffix}`;

    const upload = new FormData();
    upload.append('file', new Blob([fileMarker], { type: 'text/plain' }), 'session-context.txt');
    const fileRes = await apiCall(cfg, '/v1/files', { method: 'POST', body: upload });
    expect(fileRes.status, fileRes.text).toBe(200);
    const file = fileRes.json<FileResponse>();
    created.files.push(file.id);

    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `sandbox-harness-resources-${suffix}`,
        model: COLOCATED_AGENT.model,
        system:
          'Use filesystem tools exactly as requested. Never guess file contents or command output. Wait for every tool result before replying.',
        tools: ALWAYS_ALLOW_SANDBOX_TOOLS,
        mcp_servers: [],
        skills: [],
        metadata: COLOCATED_AGENT.metadata,
      }),
    });
    expect(agentRes.status, agentRes.text).toBe(200);
    const agent = agentRes.json<AgentResponse>();
    created.agents.push(agent.id);

    const sessionRes = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agent.id,
        resources: [
          {
            type: 'file',
            file_id: file.id,
            mount_path: filePath,
            access: 'read_only',
          },
          {
            type: 'github_repository',
            url: 'https://github.com/orca-ae/orca-sdk-go.git',
            authorization_token: githubToken,
            mount_path: repoPath,
            access: 'read_write',
          },
        ],
      }),
    });
    expect(sessionRes.status, sessionRes.text).toBe(200);
    const session = sessionRes.json<SessionResponse>();
    created.sessions.push(session.id);
    expect(sessionRes.text).not.toContain(githubToken);
    expect(session.resources.map((resource) => resource.type).sort()).toEqual([
      'file',
      'github_repository',
    ]);

    const submitRes = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            content: [
              {
                type: 'text',
                text:
                  `Use ${COLOCATED_AGENT.tools.read} to read ${filePath}. Then use ${COLOCATED_AGENT.tools.bash} to run exactly ` +
                  `\`git -C ${repoPath} ls-remote origin HEAD\`. ` +
                  `Then use ${COLOCATED_AGENT.tools.bash} to run exactly \`printf '\\n${ownershipMarker}\\n' >> ${repoPath}/README.md && tail -n 1 ${repoPath}/README.md\`. ` +
                  'Reply only after all three tool results return.',
              },
            ],
          },
        ],
        request_id: `sandbox-harness-resources-${suffix}`,
      }),
    });
    expect(submitRes.status, submitRes.text).toBe(200);

    const frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 420_000,
      until: (items) =>
        items.some((frame) => ['session.status_idle', 'session.error'].includes(frame.type)),
    });
    const failure = frames.find((frame) => frame.type === 'session.error');
    expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();

    const readUse = frames.find(
      (frame) =>
        frame.type === 'agent.tool_use' &&
        frame.name === COLOCATED_AGENT.tools.read &&
        JSON.stringify(frame.input).includes(filePath),
    );
    expect(
      readUse,
      `missing ${COLOCATED_AGENT.tools.read}(${filePath}) in ${JSON.stringify(frames)}`,
    ).toBeDefined();
    const readResult = frames.find(
      (frame) =>
        frame.type === 'agent.tool_result' &&
        frame.tool_use_id === (readUse?.id ?? readUse?.tool_use_id),
    );
    expect(JSON.stringify(readResult?.content)).toContain(fileMarker);

    const gitUse = frames.find(
      (frame) =>
        frame.type === 'agent.tool_use' &&
        frame.name === COLOCATED_AGENT.tools.bash &&
        JSON.stringify(frame.input).includes(`git -C ${repoPath} ls-remote origin HEAD`),
    );
    expect(gitUse, `missing git ls-remote tool use in ${JSON.stringify(frames)}`).toBeDefined();
    const gitResult = frames.find(
      (frame) =>
        frame.type === 'agent.tool_result' &&
        frame.tool_use_id === (gitUse?.id ?? gitUse?.tool_use_id),
    );
    expect(gitResult?.is_error, gitResult ? JSON.stringify(gitResult) : undefined).toBe(false);
    expect(JSON.stringify(gitResult?.content)).toMatch(/[0-9a-f]{40}(?:\\t|\s+)HEAD/);

    const writeUse = frames.find(
      (frame) =>
        frame.type === 'agent.tool_use' &&
        frame.name === COLOCATED_AGENT.tools.bash &&
        JSON.stringify(frame.input).includes(`>> ${repoPath}/README.md`),
    );
    expect(
      writeUse,
      `missing repository write tool use in ${JSON.stringify(frames)}`,
    ).toBeDefined();
    const writeResult = frames.find(
      (frame) =>
        frame.type === 'agent.tool_result' &&
        frame.tool_use_id === (writeUse?.id ?? writeUse?.tool_use_id),
    );
    expect(writeResult?.is_error, writeResult ? JSON.stringify(writeResult) : undefined).toBe(
      false,
    );
    expect(JSON.stringify(writeResult?.content)).toContain(ownershipMarker);
    expect(JSON.stringify(frames)).not.toContain(githubToken);

    const listed = await apiCall(cfg, `/v1/sessions/${session.id}/events?limit=200`, {
      method: 'GET',
      headers: { 'orca-beta': '1' },
    });
    expect(listed.status, listed.text).toBe(200);
    expect(
      listed
        .json<{ data: SseFrame[] }>()
        .data.some(
          (event) =>
            event.type === 'session.resource_mounted' &&
            event.file_id === file.id &&
            event.mount_path === filePath,
        ),
    ).toBe(true);
  }, 480_000);

  it('persists an in-sandbox write through an attached memory store', async () => {
    const suffix = Date.now();
    const storeName = `sandbox-harness-memory-${suffix}`;
    const expectedContent = `IN_SANDBOX_MEMORY_RESOURCE_${suffix}`;
    const expectedSha = createHash('sha256').update(expectedContent).digest('hex');

    const memoryStoreRes = await apiCall(cfg, '/v1/memory_stores', {
      method: 'POST',
      body: JSON.stringify({ name: storeName }),
    });
    expect(memoryStoreRes.status, memoryStoreRes.text).toBe(200);
    const memoryStore = memoryStoreRes.json<MemoryStoreResponse>();
    created.memStores.push(memoryStore.id);
    const memoryPath = `/mnt/memory/${memoryStore.name}/proof.txt`;

    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `sandbox-harness-memory-${suffix}`,
        model: COLOCATED_AGENT.model,
        system: `Use ${COLOCATED_AGENT.tools.write} to create files with the exact path and content requested.`,
        tools: ALWAYS_ALLOW_SANDBOX_TOOLS,
        mcp_servers: [],
        skills: [],
        metadata: COLOCATED_AGENT.metadata,
      }),
    });
    expect(agentRes.status, agentRes.text).toBe(200);
    const agent = agentRes.json<AgentResponse>();
    created.agents.push(agent.id);

    const sessionRes = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({
        environment_id: environmentId,
        agent_id: agent.id,
        resources: [
          {
            type: 'memory_store',
            memory_store_id: memoryStore.id,
            access: 'read_write',
          },
        ],
      }),
    });
    expect(sessionRes.status, sessionRes.text).toBe(200);
    const session = sessionRes.json<SessionResponse>();
    created.sessions.push(session.id);
    expect(session.resources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: 'memory_store',
          memory_store_id: memoryStore.id,
          mount_path: `/mnt/memory/${memoryStore.name}/`,
        }),
      ]),
    );

    const submitRes = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            content: [
              {
                type: 'text',
                text:
                  `Use ${COLOCATED_AGENT.tools.write} now to create ${memoryPath} with content exactly ` +
                  `${JSON.stringify(expectedContent)} and no trailing newline. Wait for the tool result before replying.`,
              },
            ],
          },
        ],
        request_id: `sandbox-harness-memory-${suffix}`,
      }),
    });
    expect(submitRes.status, submitRes.text).toBe(200);

    const frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 420_000,
      until: (items) =>
        items.some((frame) => ['session.status_idle', 'session.error'].includes(frame.type)),
    });
    const failure = frames.find((frame) => frame.type === 'session.error');
    expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();
    const writeUse = frames.find(
      (frame) =>
        frame.type === 'agent.tool_use' &&
        frame.name === COLOCATED_AGENT.tools.write &&
        JSON.stringify(frame.input).includes(memoryPath),
    );
    expect(
      writeUse,
      `missing ${COLOCATED_AGENT.tools.write}(${memoryPath}) in ${JSON.stringify(frames)}`,
    ).toBeDefined();
    const writeResult = frames.find(
      (frame) =>
        frame.type === 'agent.tool_result' &&
        frame.tool_use_id === (writeUse?.id ?? writeUse?.tool_use_id),
    );
    expect(writeResult?.is_error, writeResult ? JSON.stringify(writeResult) : undefined).toBe(
      false,
    );

    let persistedVersion: MemoryVersion | undefined;
    for (let attempt = 0; attempt < 15 && !persistedVersion; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 2_000));
      const versionsRes = await apiCall(
        cfg,
        `/v1/memory_stores/${memoryStore.id}/memory_versions`,
        { method: 'GET' },
      );
      if (versionsRes.status !== 200) continue;
      persistedVersion = versionsRes
        .json<{ data: MemoryVersion[] }>()
        .data.find(
          (version) => version.path === '/proof.txt' && version.content_sha256 === expectedSha,
        );
    }
    expect(
      persistedVersion,
      `expected proof.txt memory version with sha256 ${expectedSha}`,
    ).toBeDefined();

    const contentRes = await apiCall(
      cfg,
      `/v1/memory_stores/${memoryStore.id}/memories/${persistedVersion!.memory_id}?view=full`,
      { method: 'GET' },
    );
    expect(contentRes.status, contentRes.text).toBe(200);
    expect(contentRes.json<{ content: string | null }>().content).toBe(expectedContent);
  }, 480_000);

  it('resumes a custom tool callback without an extra tool confirmation', async () => {
    const suffix = Date.now();
    // Only the callback carries this nonce; it cannot be inferred from the ticket ID.
    const marker = `colocated-custom-${randomUUID()}`;
    const ticketId = `T-${suffix}`;
    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `sandbox-custom-${suffix}`,
        model: COLOCATED_AGENT.model,
        system:
          'Call lookup_ticket exactly once for the requested ticket. Wait for its result, then include the returned marker verbatim and report the ticket status. Do not use other tools.',
        tools: [
          { type: 'agent_toolset', default_config: { permission_policy: { type: 'always_ask' } } },
          {
            type: 'custom',
            name: 'lookup_ticket',
            description: 'Return the status of a support ticket.',
            input_schema: {
              type: 'object',
              properties: { ticket_id: { type: 'string' } },
              required: ['ticket_id'],
            },
          },
        ],
        mcp_servers: [],
        skills: [],
        metadata: COLOCATED_AGENT.metadata,
      }),
    });
    expect(agentRes.status, agentRes.text).toBe(200);
    const agent = agentRes.json<AgentResponse>();
    created.agents.push(agent.id);
    const sessionRes = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({ environment_id: environmentId, agent_id: agent.id }),
    });
    expect(sessionRes.status, sessionRes.text).toBe(200);
    const session = sessionRes.json<SessionResponse>();
    created.sessions.push(session.id);
    const sent = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            content: [{ type: 'text', text: `Look up ticket ${ticketId} using lookup_ticket.` }],
          },
        ],
      }),
    });
    expect(sent.status, sent.text).toBe(200);
    const requested = await collectSseFrames(cfg, session.id, {
      deadlineMs: 420_000,
      until: (frames) =>
        frames.some(
          (frame) => frame.type === 'agent.custom_tool_use' || frame.type === 'session.error',
        ),
    });
    expect(requested.find((frame) => frame.type === 'session.error')).toBeUndefined();
    const call = requested.find((frame) => frame.type === 'agent.custom_tool_use');
    expect(call).toMatchObject({
      name: 'lookup_ticket',
      input: { ticket_id: ticketId },
      id: expect.any(String),
    });
    const responseText = `${marker} status=ready`;
    const result = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      body: JSON.stringify({
        events: [
          {
            type: 'user.custom_tool_result',
            custom_tool_use_id: call!.id,
            content: [{ type: 'text', text: responseText }],
          },
        ],
        request_id: `colocated-custom-result-${suffix}`,
      }),
    });
    expect(result.status, result.text).toBe(200);
    const final = await collectSseFrames(cfg, session.id, {
      deadlineMs: 420_000,
      until: (frames) =>
        frames.some((frame) => isTerminalIdle(frame) || frame.type === 'session.error'),
    });
    expect(final.find((frame) => frame.type === 'session.error')).toBeUndefined();
    const listed = await waitForListedEvents(cfg, session.id, (events) =>
      events.some(isTerminalIdle),
    );
    expectTicketCallbackRoundTrip(
      final.filter(isAssistantTextFrame).map(assistantText).join('\n'),
      listed.data,
      { callId: call!.id as string, marker, text: responseText },
    );
    expect(listed.data.filter((frame) => frame.type === 'agent.custom_tool_use')).toHaveLength(1);
    expect(listed.data.some((frame) => frame.type === 'agent.tool_use')).toBe(false);
    expect(
      listed.data.some(
        (frame) => frame.type === 'agent.requires_action' && frame.action === 'tool_confirmation',
      ),
    ).toBe(false);
    expect(listed.data.some((frame) => frame.type === 'agent.tool_result')).toBe(false);
  }, 480_000);

  it('captures an in-sandbox output immediately after its tool result', async () => {
    const suffix = Date.now();
    const filename = `in-sandbox-output-${suffix}.txt`;
    const outputPath = `/mnt/session/outputs/${filename}`;
    const deniedFilename = `in-sandbox-denied-${suffix}.txt`;
    const deniedPath = `/mnt/${deniedFilename}`;
    const expectedContent = `in-sandbox output ${suffix}`;
    const bashFilename = `in-sandbox-bash-output-${suffix}.txt`;
    const deniedBashFilename = `in-sandbox-bash-denied-${suffix}.txt`;
    const expectedBashContent = `in-sandbox bash output ${suffix}`;
    const bashCommand =
      `if printf %s 'must not exist' > '/mnt/${deniedBashFilename}'; then exit 42; fi; ` +
      `printf %s '${expectedBashContent}' > '${bashFilename}'`;

    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `sandbox-harness-output-${suffix}`,
        model: COLOCATED_AGENT.model,
        system:
          'When asked to test file writes, make every requested tool call in the exact order supplied. ' +
          `A failed first call is expected: observe its tool error, continue with the second ${COLOCATED_AGENT.tools.write}, then run the exact ${COLOCATED_AGENT.tools.bash} command. Do not stop early.`,
        tools: ALWAYS_ALLOW_SANDBOX_TOOLS,
        mcp_servers: [],
        skills: [],
        metadata: COLOCATED_AGENT.metadata,
      }),
    });
    expect(agentRes.status, agentRes.text).toBe(200);
    const agent = agentRes.json<AgentResponse>();
    created.agents.push(agent.id);

    const sessionRes = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({ environment_id: environmentId, agent_id: agent.id }),
    });
    expect(sessionRes.status, sessionRes.text).toBe(200);
    const session = sessionRes.json<SessionResponse>();
    created.sessions.push(session.id);

    const submitRes = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
      method: 'POST',
      body: JSON.stringify({
        events: [
          {
            type: 'user.message',
            content: [
              {
                type: 'text',
                text:
                  `First use ${COLOCATED_AGENT.tools.write} to create ${deniedPath} with content "must not exist". ` +
                  `After that call returns the expected denial, use ${COLOCATED_AGENT.tools.write} to create ${outputPath} with content exactly ${JSON.stringify(expectedContent)}. ` +
                  `After the second result, use ${COLOCATED_AGENT.tools.bash} once with command exactly ${JSON.stringify(bashCommand)}. ` +
                  'Wait for each tool result before making the next call or replying.',
              },
            ],
          },
        ],
        request_id: `sandbox-harness-output-${suffix}`,
      }),
    });
    expect(submitRes.status, submitRes.text).toBe(200);

    const frames = await collectSseFrames(cfg, session.id, {
      deadlineMs: 420_000,
      until: (items) =>
        items.some((frame) => frame.type === 'session.error') ||
        items.filter((frame) => frame.type === 'agent.tool_result').length >= 3,
    });
    const failure = frames.find((frame) => frame.type === 'session.error');
    expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();
    const toolResults = frames.filter((frame) => frame.type === 'agent.tool_result');
    expect(toolResults).toHaveLength(3);
    expect(toolResults[0]?.is_error, JSON.stringify(toolResults[0])).toBe(true);
    expect(toolResults[1]?.is_error, JSON.stringify(toolResults[1])).toBe(false);
    expect(toolResults[2]?.is_error, JSON.stringify(toolResults[2])).toBe(false);

    const output = await waitForScopedOutputFile(cfg, session.id, filename, 30_000);
    created.files.push(output.id);
    expect(output).toMatchObject({
      filename,
      scope: { type: 'session', id: session.id },
      downloadable: true,
      size_bytes: Buffer.byteLength(expectedContent),
    });
    expect(await downloadFileText(cfg, output.id)).toBe(expectedContent);
    const bashOutput = await waitForScopedOutputFile(cfg, session.id, bashFilename, 30_000);
    created.files.push(bashOutput.id);
    expect(await downloadFileText(cfg, bashOutput.id)).toBe(expectedBashContent);
    const filenames = (await listScopedOutputFiles(cfg, session.id)).map((file) => file.filename);
    expect(filenames).not.toContain(deniedFilename);
    expect(filenames).not.toContain(deniedBashFilename);
  }, 480_000);
});

async function waitForListedEvents(
  cfg: OrcaClientConfig,
  sessionId: string,
  ready: (events: SseFrame[]) => boolean,
): Promise<{ data: SseFrame[] }> {
  const deadline = Date.now() + 10_000;
  let latest: SseFrame[] = [];
  while (Date.now() < deadline) {
    const listed = await apiCall(cfg, `/v1/sessions/${sessionId}/events?limit=100`, {
      method: 'GET',
    });
    expect(listed.status, listed.text).toBe(200);
    const body = listed.json<{ data: SseFrame[] }>();
    latest = body.data;
    if (ready(latest)) return body;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `timed out waiting for listed session events; saw: ${latest.map((event) => event.type).join(', ')}`,
  );
}

function isTokenBudgetDeniedFrame(frame: SseFrame): boolean {
  if (frame.type !== 'session.error') return false;
  const error = frame['error'];
  if (error === null || typeof error !== 'object' || Array.isArray(error)) return false;
  const fields = error as Record<string, unknown>;
  const retryStatus = fields['retry_status'];
  return (
    fields['type'] === 'unknown_error' &&
    typeof fields['message'] === 'string' &&
    /^This session has reached its budget of 1 tokens \(used \d+\)\.$/.test(fields['message']) &&
    retryStatus !== null &&
    typeof retryStatus === 'object' &&
    !Array.isArray(retryStatus) &&
    (retryStatus as Record<string, unknown>)['type'] === 'exhausted'
  );
}

async function prepareSessionExecution(
  workspaceId: string,
  sessionId: string,
  internalServiceToken: string,
): Promise<{ session: Record<string, unknown> }> {
  const response = await fetch(
    `${registryInternalBaseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/executions:prepare`,
    {
      method: 'POST',
      headers: {
        authorization: `Bearer ${internalServiceToken}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({}),
    },
  );
  const text = await response.text();
  expect(response.status, text).toBe(200);
  const body = JSON.parse(text) as Record<string, unknown>;
  const session = body['session'];
  expect(session).not.toBeNull();
  expect(typeof session).toBe('object');
  expect(Array.isArray(session)).toBe(false);
  return { session: session as Record<string, unknown> };
}

async function waitForSessionUsage(
  cfg: OrcaClientConfig,
  sessionId: string,
  timeoutMs: number,
): Promise<SessionResponse> {
  const deadline = Date.now() + timeoutMs;
  let last: SessionResponse | null = null;
  while (Date.now() < deadline) {
    const res = await apiCall(cfg, `/v1/sessions/${sessionId}`, { method: 'GET' });
    expect(res.status, res.text).toBe(200);
    last = res.json<SessionResponse>();
    if ((last.usage?.input_tokens ?? 0) > 0 && (last.usage?.output_tokens ?? 0) > 0) {
      return last;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(
    `session ${sessionId} did not report token usage within ${timeoutMs}ms; last=${JSON.stringify(last)}`,
  );
}

async function waitForGatewayKafkaUsage(
  sessionId: string,
  topic: string,
  expectedInputTokens: number,
  expectedOutputTokens: number,
  timeoutMs: number,
): Promise<GatewayUsageBatch> {
  const brokers = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);
  const kafka = new Kafka({
    brokers,
    clientId: `orca-e2e-gateway-usage-${process.pid}`,
    logLevel: logLevel.NOTHING,
  });
  await waitForKafkaTopic(kafka, topic, timeoutMs);

  const consumer = kafka.consumer({
    groupId: `orca-e2e-gateway-usage-${process.pid}-${Date.now()}`,
  });
  await consumer.connect();
  try {
    await consumer.subscribe({ topic, fromBeginning: true });
    const found = new Promise<GatewayUsageBatch>((resolve, reject) => {
      const events: GatewayUsageEvent[] = [];
      let inputTokens = 0;
      let outputTokens = 0;
      void consumer
        .run({
          autoCommit: false,
          eachMessage: async ({ message }) => {
            const event = parseGatewayUsageMessage(message.value?.toString('utf8'));
            if (event?.scope?.session_id !== sessionId) return;
            events.push(event);
            inputTokens += event.input_tokens ?? 0;
            outputTokens += event.output_tokens ?? 0;
            if (inputTokens > expectedInputTokens || outputTokens > expectedOutputTokens) {
              reject(
                new Error(
                  `Gateway Kafka usage exceeded Registry usage for session ${sessionId}: ` +
                    `${inputTokens}/${outputTokens} > ${expectedInputTokens}/${expectedOutputTokens}`,
                ),
              );
            } else if (
              inputTokens === expectedInputTokens &&
              outputTokens === expectedOutputTokens
            ) {
              resolve({ events: [...events], inputTokens, outputTokens });
            }
          },
        })
        .catch(reject);
    });
    return await withTimeout(
      found,
      timeoutMs,
      `timed out waiting for Gateway Kafka usage session_id=${sessionId} on topic ${topic}`,
    );
  } finally {
    await consumer.stop().catch(() => {});
    await consumer.disconnect().catch(() => {});
  }
}

async function waitForKafkaTopic(kafka: Kafka, topic: string, timeoutMs: number): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if ((await admin.listTopics()).includes(topic)) return;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
    throw new Error(`Kafka topic ${topic} was not created by the Gateway usage sink`);
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

function parseGatewayUsageMessage(value: string | undefined): GatewayUsageEvent | null {
  if (!value) return null;
  try {
    return JSON.parse(value) as GatewayUsageEvent;
  } catch {
    return null;
  }
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), timeoutMs);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

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
      drainSseBuffer(
        (next) => {
          buffered = next;
        },
        buffered,
        frames,
      );
    }
    await reader.cancel().catch(() => {});
    return frames;
  } finally {
    clearTimeout(timer);
    ac.abort();
  }
}

function drainSseBuffer(setRest: (rest: string) => void, input: string, frames: SseFrame[]): void {
  let rest = input;
  while (true) {
    const idx = rest.indexOf('\n\n');
    if (idx === -1) {
      setRest(rest);
      return;
    }
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

function isAssistantTextFrame(f: SseFrame): boolean {
  if (f.type !== 'agent.message') return false;
  expect(typeof f.processed_at).toBe('string');
  expect(Number.isNaN(Date.parse(f.processed_at))).toBe(false);
  expect(Object.hasOwn(f, 'message')).toBe(false);
  return assistantText(f).length > 0;
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
