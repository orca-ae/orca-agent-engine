// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @orca/e2e-tests — Layer B: real-agent-loop scenarios.
 *
 * Layer B drives the FULL agent loop end-to-end against the live local stack:
 *
 *   client (this spec)
 *      │  POST /v1/files / /v1/agents / /v1/sessions / /v1/sessions/:id/events
 *      ▼
 *   registry-service-ts ──── transcript store ────► harness-server
 *                                              │
 *                                              ├── Claude Agent SDK or Codex SDK
 *                                              │     ├── native provider API (REAL model)
 *                                              │     ├── mcp__orca__* tools (in-process MCP server)
 *                                              │     └── remote MCP tools rewritten through ai-gateway
 *                                              │
 *                                              └── selected SandboxRuntime
 *                                                    └── tool dispatch lands inside the
 *                                                        per-session sandbox work-dir
 *
 * The harness's `mcp-tools.ts` builds an in-process SDK MCP server per session
 * that exposes `mcp__orca__{bash,read,write,edit,list,delete,glob,grep}` bound
 * to the per-session `SandboxHandle`. The harness sets `options.tools = []` so
 * the model only sees the orca tools — every file/exec call lands inside the
 * sandbox work-dir.
 *
 * **Provider selection.** ORCA_E2E_AGENT_HARNESS selects claude_agent_sdk
 * (default) or codex_sdk. The selected provider key is required in both the
 * test process and harness-server; missing credentials fail instead of skipping.
 * Both use the default separate mode and exercise managed Skill disclosure
 * through the catalog, SKILL.md, and referenced bundle files.
 *
 * **Cleanup.** Each `it` pushes created IDs into describe-scope arrays as
 * soon as the create call returns; `afterEach` drains them in
 * reverse-dependency order with `.catch(() => {})`. A thrown assertion
 * mid-test still leaves the dev DB clean for the next run.
 */
import { REAL_AGENT, requireRealAgentKey, canonicalSandboxToolName } from './real-agent-config.js';
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { createServer, type IncomingMessage, type Server } from 'node:http';
import { createConnection } from 'node:net';
import { Kafka, logLevel } from 'kafkajs';
import {
  apiCall,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedOrganizationAdminApiKey, seedWorkspaceApiKey } from '../src/seed.js';
import { mentionsSandboxWorkDir } from '../src/sandbox-workdir.js';
import {
  downloadFileText,
  listScopedOutputFiles,
  waitForScopedOutputFile,
} from './output-file-helpers.js';
import { createTestEnvironment, deleteTestEnvironment } from './environment-helpers.js';
import { collectSseFrames, type SseFrame } from './session-observation.js';

interface AgentResponse {
  id: string;
  name: string;
  version: number;
  metadata?: Record<string, unknown>;
}

interface SkillResponse {
  id: string;
  latest_version: string;
}

interface SkillVersionResponse {
  version: string;
}

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

function reportSkillCleanupFailure(skillId: string, error: unknown): void {
  console.warn(`real-agent-loop: cleanup of Skill ${skillId} failed`, error);
}

interface FileResponse {
  id: string;
  filename: string;
  sha256: string;
  size_bytes: number;
  purpose: 'agent' | 'agent_output';
  scope_id: string | null;
  downloadable: boolean;
}

interface MemoryStoreResponse {
  id: string;
  name: string;
  workspace_id: string;
}

interface MemoryVersion {
  id: string;
  memory_id: string;
  content_sha256: string | null;
  content_size_bytes: number | null;
  path: string;
  created_at: string;
}

interface SessionResource {
  id: string;
  type: 'file' | 'memory_store' | 'github_repository';
  file_id: string | null;
  memory_store_id: string | null;
  mount_path: string;
  access: string;
}

interface SessionResponse {
  id: string;
  agent_id: string;
  status: string;
  sandbox_handle_id: string | null;
  timing?: {
    active_seconds: number;
    duration_seconds: number;
  };
  usage?: {
    cache_creation?: {
      ephemeral_1h_input_tokens?: number;
      ephemeral_5m_input_tokens?: number;
    };
    cache_read_input_tokens?: number;
    input_tokens?: number;
    output_tokens?: number;
  };
  resources: SessionResource[];
}

interface VaultResponse {
  id: string;
}

interface VaultCredentialResponse {
  id: string;
}

interface CapturedUpstreamRequest {
  accept: string | undefined;
  authorization: string | undefined;
  mcpSessionId: string | undefined;
  method: string | undefined;
  url: string | undefined;
  rawBody: string;
  jsonBody: unknown;
}

interface AuditRecord {
  request_id?: string;
  action?: string;
  resource?: unknown;
  decision?: unknown;
  principal_id?: string;
  scope?: Record<string, unknown>;
  attributes?: Record<string, unknown>;
}

/** SSE frame after parsing the `data:` line. */

const LOREM_IPSUM =
  'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ' +
  'The orca pod hunts in coordinated waves, leveraging echolocation to map prey at depth. ' +
  'Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ' +
  'Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. ' +
  'Researchers have documented hunting cooperation, vocal dialects unique to each matriline, and tool use among bottlenose populations. ' +
  'Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. ' +
  "The marine biology team observed three distinct calls used to coordinate the pod's herding maneuver. " +
  'Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.';

const GATEWAY_UPSTREAM_PORT = 18_191;
const GATEWAY_BACKEND_NAME = 'gateway-e2e';
const GATEWAY_UPSTREAM_URL = `http://host.docker.internal:${GATEWAY_UPSTREAM_PORT}/mcp`;
const REAL_CLAUDE_RETRY = Number(process.env['ORCA_E2E_REAL_CLAUDE_RETRY'] ?? '2');

describe(`Layer B: real-agent-loop (${REAL_AGENT.harness} + live sandbox)`, () => {
  let cfg: OrcaClientConfig;
  let workspaceId: string;
  let environmentId: string;
  let gatewayUpstream: Server | undefined;
  const created: {
    sessions: string[];
    agents: string[];
    files: string[];
    memStores: string[];
    skills: string[];
    vaults: string[];
  } = { sessions: [], agents: [], files: [], memStores: [], skills: [], vaults: [] };

  beforeAll(async () => {
    requireRealAgentKey();
    const seeded = await seedWorkspaceApiKey();
    workspaceId = seeded.workspaceId;
    const { apiKey } = seeded;
    cfg = buildClientFromConfig({ apiKey });
    await ensureStackReachable(cfg);
    environmentId = await createTestEnvironment(cfg, 'real-agent-loop-env');
  });

  afterAll(async () => {
    if (cfg && environmentId) {
      await deleteTestEnvironment(cfg, environmentId).catch(() => {});
    }
  });

  afterEach(async () => {
    if (gatewayUpstream) {
      const closing = once(gatewayUpstream, 'close');
      gatewayUpstream.close();
      // ai-gateway keeps its upstream HTTP connection alive. Force it closed
      // after stopping new accepts so a failed attempt cannot exhaust the hook
      // timeout or leave the fixed test port occupied for Vitest's retry.
      gatewayUpstream.closeAllConnections();
      await closing.catch(() => {});
      gatewayUpstream = undefined;
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
      await deleteSkillAndVersions(cfg, id).catch((error) => reportSkillCleanupFailure(id, error));
    }
    for (const id of created.vaults.splice(0)) {
      await apiCall(cfg, `/v1/vaults/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
  });

  // ---------------------------------------------------------------- scenario 1
  /**
   * **File summarization through the sandbox.** Upload `lorem.txt`, attach it
   * to a session at `/mnt/lorem.txt`, ask Claude to summarize. The harness's
   * orca MCP server is the ONLY tool surface (built-ins stripped via
   * `options.tools = []`), so Claude must call `mcp__orca__bash` to `cat`
   * the file inside the sandbox
   * work-dir where `materializeResources` wrote the file's bytes.
   *
   * Asserts:
   *   (a) the SSE stream shows at least one `mcp__orca__*` tool_use — proves
   *       the sandbox-bound tools were dispatched, not the SDK built-ins
   *       (which would be `Bash`/`Read`/etc. without the `mcp__orca__` prefix).
   *   (b) the assistant's reply mentions "orca" — a unique keyword embedded
   *       in the lorem text. This proves the model actually read the file
   *       (it can't hallucinate "orca pod hunts in coordinated waves").
   */
  it(
    'summarizes a file mounted at /mnt/lorem.txt via mcp__orca__bash',
    async () => {
      const upload = new FormData();
      upload.append('file', new Blob([LOREM_IPSUM], { type: 'text/plain' }), 'lorem.txt');
      const fileRes = await fetch(`${cfg.baseURL}/v1/files`, {
        method: 'POST',
        headers: { 'x-api-key': cfg.apiKey },
        body: upload,
      });
      expect(fileRes.status).toBe(200);
      const file = (await fileRes.json()) as FileResponse;
      created.files.push(file.id);

      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `agent-loop-summarize-${Date.now()}`,
          model: REAL_AGENT.model,
          system:
            'You are a shell file-reading assistant. The available shell tool is named mcp__orca__bash. ' +
            'When asked to read, inspect, or summarize a mounted file, you MUST call mcp__orca__bash first. ' +
            'Do not answer from memory, path names, or assumptions.',
          tools: [
            {
              type: 'agent_toolset_20260401',
              default_config: { permission_policy: { type: 'always_ask' } },
              configs: [{ name: 'bash', permission_policy: { type: 'always_allow' } }],
            },
          ],
          mcp_servers: [],
          skills: [],
          metadata: REAL_AGENT.metadata,
        }),
      });
      expect(agentRes.status).toBe(200);
      const agent = agentRes.json<AgentResponse>();
      expect(agent.metadata?.harness).toBe(REAL_AGENT.harness);
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
              mount_path: '/mnt/lorem.txt',
              access: 'read_only',
            },
          ],
        }),
      });
      expect(sessionRes.status).toBe(200);
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
                    'Call the mcp__orca__bash tool now with this exact command: `cat /mnt/lorem.txt || cat mnt/lorem.txt`. ' +
                    'After the tool result returns, summarize the file in 1-2 sentences and include the exact phrase "orca pod hunts in coordinated waves". ' +
                    'Do not answer until the tool result is available.',
                },
              ],
            },
          ],
          request_id: `agent-loop-summarize-${Date.now()}`,
        }),
      });
      expect(submitRes.status).toBe(200);

      const frames = await collectSseFrames(cfg, session.id, {
        deadlineMs: 110_000,
        until: (f) =>
          f.some((frame) => frame.type === 'session.error') ||
          (toolUseNamesFromFrames(f).some((n) => n.startsWith('mcp__orca__')) &&
            assistantTextFromFrames(f).toLowerCase().includes('orca')),
      });
      const failure = frames.find((frame) => frame.type === 'session.error');
      expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();

      // (a) A sandbox-bound `mcp__orca__bash` tool was dispatched to read
      // the file. The harness obtains one immutable, workspace/session-scoped
      // prepared execution, so the per-session `SandboxHandle` is wired into
      // the SDK and the orca MCP server becomes the model's tool surface.
      const toolUses = toolUseNamesFromFrames(frames);
      const usedOrca = toolUses.some((n) => n === 'mcp__orca__bash');
      expect(
        usedOrca,
        `Expected at least one mcp__orca__bash tool_use; saw: ${JSON.stringify(toolUses)}; event types: ${frames.map((frame) => frame.type).join(', ')}`,
      ).toBe(true);

      // (b) Model actually read the file content. The lorem text contains
      // "orca pod hunts in coordinated waves" — a phrase the model can't
      // hallucinate. Its presence in the assistant reply proves the file
      // was read from the sandbox-mounted path.
      const allText = assistantTextFromFrames(frames).toLowerCase();
      expect(
        allText,
        `Assistant reply did not reference file content. Reply: ${allText.slice(0, 500)}`,
      ).toContain('orca');

      const usageSession = await waitForSessionUsage(cfg, session.id, 30_000);
      expect(['running', 'idle']).toContain(usageSession.status);
      expect(usageSession.timing?.duration_seconds).toBeGreaterThanOrEqual(0);
      expect(usageSession.timing?.active_seconds).toBeGreaterThan(0);
      expect(usageSession.usage?.input_tokens).toBeGreaterThan(0);
      expect(usageSession.usage?.output_tokens).toBeGreaterThan(0);
      expect(usageSession.usage?.cache_read_input_tokens).toBeGreaterThanOrEqual(0);
      expect(
        (usageSession.usage?.cache_creation?.ephemeral_1h_input_tokens ?? 0) +
          (usageSession.usage?.cache_creation?.ephemeral_5m_input_tokens ?? 0),
      ).toBeGreaterThanOrEqual(0);

      const eventsRes = await apiCall(cfg, `/v1/sessions/${session.id}/events?limit=1000`, {
        method: 'GET',
      });
      expect(eventsRes.status).toBe(200);
      const eventsBody = eventsRes.json<{ data: Array<{ type: string }> }>();
      expect(eventsBody.data.some((event) => event.type === 'agent.usage')).toBe(false);
    },
    { timeout: 180_000, retry: REAL_CLAUDE_RETRY },
  );

  // ------------------------------------------------------- output capture
  /**
   * A separated runner exposes sandbox file tools through the harness's
   * `mcp__orca__*` server. The orca server is session-local, so the harness
   * surfaces its completion as `agent.tool_result`; remote MCP servers use
   * `agent.mcp_tool_result`. Either result must trigger output indexing while
   * the runner is still alive rather than relying on the idle-timeout scan.
   */
  it(
    'captures a separated-runner output immediately after its tool result',
    async () => {
      const suffix = Date.now();
      const filename = `separated-output-${suffix}.txt`;
      const outputPath = `/mnt/session/outputs/${filename}`;
      const deniedFilename = `separated-denied-${suffix}.txt`;
      const deniedPath = `/mnt/${deniedFilename}`;
      const expectedContent = `separated output ${suffix}`;
      const bashFilename = `separated-bash-output-${suffix}.txt`;
      const deniedBashFilename = `separated-bash-denied-${suffix}.txt`;
      const expectedBashContent = `separated bash output ${suffix}`;
      const bashCommand =
        `if printf %s 'must not exist' > '/mnt/${deniedBashFilename}'; then exit 42; fi; ` +
        `printf %s '${expectedBashContent}' > '${bashFilename}'`;

      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `agent-loop-output-${suffix}`,
          model: REAL_AGENT.model,
          system:
            'When asked to test file writes, make every requested tool call in the exact order supplied. ' +
            'A failed first call is expected: observe its tool error, continue with the second write, then run the exact mcp__orca__bash command. Do not stop early.',
          tools: [{ type: 'agent_toolset_20260401' }],
          mcp_servers: [],
          skills: [],
          metadata: REAL_AGENT.metadata,
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
                    `First call mcp__orca__write with path ${deniedPath} and content "must not exist". ` +
                    `After that call returns the expected denial, call mcp__orca__write with path ${outputPath} and content exactly ${JSON.stringify(expectedContent)}. ` +
                    `After the second result, call mcp__orca__bash once with command exactly ${JSON.stringify(bashCommand)}. ` +
                    'Wait for each tool result before making the next call or replying.',
                },
              ],
            },
          ],
          request_id: `agent-loop-output-${suffix}`,
        }),
      });
      expect(submitRes.status, submitRes.text).toBe(200);

      const frames = await collectSseFrames(cfg, session.id, {
        deadlineMs: 110_000,
        until: (items) =>
          items.some((frame) => frame.type === 'session.error') ||
          items.filter((frame) =>
            ['agent.tool_result', 'agent.mcp_tool_result'].includes(frame.type),
          ).length >= 3,
      });
      const failure = frames.find((frame) => frame.type === 'session.error');
      expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();
      const toolResults = frames.filter((frame) =>
        ['agent.tool_result', 'agent.mcp_tool_result'].includes(frame.type),
      );
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

      // Output capture must be observable immediately after tool results, but
      // cleanup must not delete the Session while the model's final response
      // and fail-closed guardrail usage flush are still in flight.
      await collectSseFrames(cfg, session.id, {
        deadlineMs: 60_000,
        until: (items) => hasIdleAfterToolResults(items, 3),
      });
    },
    { timeout: 180_000, retry: REAL_CLAUDE_RETRY },
  );

  // ---------------------------------------------------------------- scenario 2
  /**
   * **Memory write-through.** Create a memory_store, attach at
   * `/mnt/memory/<name>/`, ask Claude to write `hello.txt` with content
   * `world`. The harness's `MemoryFuseStrategy` (or `LocalMemoryStrategy` for
   * the in-process variant) mounts the memory store onto the sandbox FS;
   * a write inside the sandbox lands in the live S3 backend, and the
   * `MemoryVersionWatcher` polls + emits a `memver_*` row.
   *
   * Asserts that after the turn completes,
   * `GET /v1/memory_stores/:id/memory_versions` lists at least one version
   * whose sha256 matches `world`. This verifies persisted memory-store writes
   * without coupling the test to an Orca-specific tool name.
   */
  it(
    'persists a sandbox write through the memory_store version log',
    async () => {
      const storeName = `agent-loop-mem-${Date.now()}`;
      const memRes = await apiCall(cfg, '/v1/memory_stores', {
        method: 'POST',
        body: JSON.stringify({ name: storeName }),
      });
      expect(memRes.status).toBe(200);
      const store = memRes.json<MemoryStoreResponse>();
      created.memStores.push(store.id);

      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `agent-loop-mem-write-${Date.now()}`,
          model: REAL_AGENT.model,
          system:
            'You write files when asked. USE the write tool to create files at the requested path with the requested content.',
          tools: [{ type: 'agent_toolset_20260401' }],
          mcp_servers: [],
          skills: [],
          metadata: REAL_AGENT.metadata,
        }),
      });
      expect(agentRes.status).toBe(200);
      const agent = agentRes.json<AgentResponse>();
      created.agents.push(agent.id);

      const mountPath = `/mnt/memory/${storeName}/`;
      const sessionRes = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({
          environment_id: environmentId,
          agent_id: agent.id,
          resources: [{ type: 'memory_store', memory_store_id: store.id }],
        }),
      });
      expect(sessionRes.status).toBe(200);
      const session = sessionRes.json<SessionResponse>();
      created.sessions.push(session.id);
      const memResource = session.resources.find((r) => r.type === 'memory_store');
      expect(memResource?.mount_path).toBe(mountPath);

      const submitRes = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
        method: 'POST',
        body: JSON.stringify({
          events: [
            {
              type: 'user.message',
              content: [
                {
                  type: 'text',
                  text: `Write a file at ${mountPath}hello.txt with the exact content "world" (no newline). After writing, reply with "done".`,
                },
              ],
            },
          ],
          request_id: `agent-loop-mem-${Date.now()}`,
        }),
      });
      expect(submitRes.status).toBe(200);

      await collectSseFrames(cfg, session.id, {
        deadlineMs: 110_000,
        until: (f) => assistantTextFromFrames(f).toLowerCase().includes('done'),
      });

      // The write surfaced in the version log. With sandbox wiring active,
      // the write lands bytes in the memory_store's mount directory;
      // `MemoryVersionWatcher` picks the change up on its next poll (~2s) and
      // emits a `memver_*` row. We tolerate up to 30s for the watcher cycle.
      const expectedSha = createHash('sha256').update('world').digest('hex');
      let versionFound = false;
      for (let i = 0; i < 15 && !versionFound; i++) {
        await new Promise((r) => setTimeout(r, 2000));
        const versionsRes = await apiCall(cfg, `/v1/memory_stores/${store.id}/memory_versions`, {
          method: 'GET',
        });
        if (versionsRes.status === 200) {
          const body = versionsRes.json<{ data: MemoryVersion[]; next_page: string | null }>();
          versionFound = body.data.some((version) => version.content_sha256 === expectedSha);
        }
      }
      expect(
        versionFound,
        `Expected a memory_version with sha256(${expectedSha}) within 30s of write completion`,
      ).toBe(true);
    },
    { timeout: 180_000, retry: REAL_CLAUDE_RETRY },
  );

  // ---------------------------------------------------------------- scenario 3
  /**
   * **Progressive Skill disclosure.** Put a unique marker in a referenced
   * bundle file, attach the Skill to an agent, and ask the running Session to
   * report it. Neither the base system prompt, catalog description, user
   * prompt, nor SKILL.md contains the marker. Returning it therefore proves
   * the Session pinned and materialized the bundle and the model followed the
   * catalog → SKILL.md → referenced-file disclosure path.
   */
  it(
    'materializes a pinned skill and discloses its files on demand',
    async () => {
      const marker = `SKILL_MARKER_ALPHA_${Date.now()}`;
      const directory = `agent-loop-skill-${Date.now()}`;
      const form = new FormData();
      form.set(
        'files[]',
        new Blob(
          [
            [
              '---',
              `name: ${directory}`,
              'description: Use when asked to return the configured skill marker',
              '---',
              '',
              'When the user asks for the configured skill marker, use the read tool to open',
              '`references/marker.txt` relative to this Skill directory and answer with exactly',
              'the file contents. Do not guess the value.',
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
      const skillRes = await apiCall(cfg, '/v1/skills', {
        method: 'POST',
        body: form,
      });
      expect(skillRes.status).toBe(200);
      const skill = skillRes.json<SkillResponse>();
      created.skills.push(skill.id);
      expect(skill.latest_version).toBeTypeOf('string');
      expect(skill.latest_version).not.toBe('');

      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `agent-loop-skill-agent-${Date.now()}`,
          model: REAL_AGENT.model,
          system: 'You answer exactly what the configured runtime instructions require.',
          tools: [
            {
              type: 'agent_toolset_20260401',
              default_config: { enabled: false },
              configs: [
                { name: 'read', enabled: true, permission_policy: { type: 'always_allow' } },
              ],
            },
          ],
          mcp_servers: [],
          skills: [{ type: 'custom', skill_id: skill.id, version: skill.latest_version }],
          metadata: REAL_AGENT.metadata,
        }),
      });
      expect(agentRes.status, agentRes.text).toBe(200);
      const agent = agentRes.json<AgentResponse>();
      created.agents.push(agent.id);

      const sessionRes = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({ environment_id: environmentId, agent_id: agent.id }),
      });
      expect(sessionRes.status).toBe(200);
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
                  text: 'Use the relevant available Skill and return only the configured skill marker.',
                },
              ],
            },
          ],
          request_id: `agent-loop-skill-${Date.now()}`,
        }),
      });
      expect(submitRes.status).toBe(200);

      const frames = await collectSseFrames(cfg, session.id, {
        deadlineMs: 110_000,
        until: (f) => assistantTextFromFrames(f).includes(marker),
      });
      const allText = assistantTextFromFrames(frames);
      expect(allText, `Assistant did not return skill marker. Reply: ${allText}`).toContain(marker);
      const skillMarkdownPath = `/workspace/skills/${directory}/SKILL.md`;
      const markerPath = `/workspace/skills/${directory}/references/marker.txt`;
      const indexedReadUses = frames
        .map((frame, index) => ({ frame, index }))
        .filter(
          ({ frame }) =>
            frame.type === 'agent.tool_use' &&
            canonicalSandboxToolName(String(frame.name)) === 'mcp__orca__read' &&
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
        `Expected mcp__orca__read(${skillMarkdownPath}); saw: ${JSON.stringify(indexedReadUses)}`,
      ).toBeDefined();
      expect(
        markerRead,
        `Expected mcp__orca__read(${markerPath}); saw: ${JSON.stringify(indexedReadUses)}`,
      ).toBeDefined();
      expect(skillMarkdownRead!.index).toBeLessThan(markerRead!.index);

      expect(markerRead!.frame.type).toBe('agent.tool_use');
      expect(typeof markerRead!.frame.id).toBe('string');
      const markerReadId = markerRead!.frame.id as string;
      const markerResult = frames.find(
        (frame) => frame.type === 'agent.tool_result' && frame.tool_use_id === markerReadId,
      );
      expect(markerResult, `Missing result for marker read ${String(markerReadId)}`).toBeDefined();
      expect(JSON.stringify(markerResult?.content)).toContain(marker);
    },
    { timeout: 180_000, retry: REAL_CLAUDE_RETRY },
  );

  // ---------------------------------------------------------------- scenario 4
  /**
   * **Bash execution under srt.** Send `pwd && ls /tmp` and assert the
   * tool_result includes the per-session work-dir path (proves cwd is the
   * sandbox work-dir, not the harness's host cwd) and that stdout returned
   * cleanly (exit_code 0).
   *
   * Asserts:
   *   (a) `mcp__orca__bash` tool_use appeared.
   *   (b) The assistant's reply contains a path under the per-session
   *       work-dir base (default `/var/tmp/orca-harness/sessions/`),
   *       proving `pwd` returned the sandbox cwd, NOT the harness's
   *       process cwd.
   */
  it(
    'runs bash inside the per-session sandbox work-dir',
    async () => {
      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `agent-loop-bash-${Date.now()}`,
          model: REAL_AGENT.model,
          system:
            'You are a shell assistant. The available shell tool is named mcp__orca__bash. ' +
            'When asked to run a command, you MUST call mcp__orca__bash; do not answer from memory. ' +
            'Then report the EXACT stdout you observed verbatim.',
          tools: [{ type: 'agent_toolset_20260401' }],
          mcp_servers: [],
          skills: [],
          metadata: REAL_AGENT.metadata,
        }),
      });
      expect(agentRes.status).toBe(200);
      const agent = agentRes.json<AgentResponse>();
      created.agents.push(agent.id);

      const sessionRes = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({ environment_id: environmentId, agent_id: agent.id }),
      });
      expect(sessionRes.status).toBe(200);
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
                    'Call the mcp__orca__bash tool now with this exact command: `pwd && echo MARKER && ls`. ' +
                    'Do not answer until the tool_result is available. Report the EXACT stdout verbatim (preserve newlines).',
                },
              ],
            },
          ],
          request_id: `agent-loop-bash-${Date.now()}`,
        }),
      });
      expect(submitRes.status).toBe(200);

      const frames = await collectSseFrames(cfg, session.id, {
        deadlineMs: 110_000,
        until: (f) => {
          const text = assistantTextFromFrames(f);
          return (
            toolUseNamesFromFrames(f).some((n) => n === 'mcp__orca__bash') &&
            text.includes('MARKER') &&
            mentionsSandboxWorkDir(text)
          );
        },
      });

      // (a) `mcp__orca__bash` was dispatched — sandbox-bound bash through
      // the harness's per-session SandboxHandle, NOT the SDK's host-side
      // built-in `Bash`. The orca MCP server is registered when the
      // dispatcher's session lookup succeeds (Gap 1), so seeing
      // `mcp__orca__bash` proves both the session→sandbox wiring and the
      // SDK→MCP registration.
      const toolUses = toolUseNamesFromFrames(frames);
      const ranOrcaBash = toolUses.some((n) => n === 'mcp__orca__bash');
      expect(
        ranOrcaBash,
        `Expected at least one mcp__orca__bash tool_use; saw: ${JSON.stringify(toolUses)}`,
      ).toBe(true);

      // (b) Reply mentions a per-session work-dir path. Local runtime paths
      // include `/var/tmp/orca-harness/.../sbx_local_*`; in-memory runtime
      // paths include the host tmpdir-backed `orca-sandbox-*` root. The
      // MARKER token confirms bash ran (Claude can't hallucinate the literal
      // "MARKER" output unless it observed the tool_result).
      const allText = assistantTextFromFrames(frames);
      expect(allText, `Assistant did not report MARKER. Reply: ${allText.slice(0, 600)}`).toContain(
        'MARKER',
      );
      // The reply must contain a per-session sandbox work-dir path. With Gap
      // 1 wired and the orca MCP `bash` tool dispatched above, `pwd` runs
      // inside the selected SandboxHandle's `rootDir()`.
      expect(
        mentionsSandboxWorkDir(allText),
        `Assistant reply lacked sandbox work-dir path. Reply: ${allText.slice(0, 600)}`,
      ).toBe(true);
    },
    { timeout: 180_000, retry: REAL_CLAUDE_RETRY },
  );

  // ---------------------------------------------------------------- scenario 5
  /**
   * **Custom tool callback.** Creates an agent with one client-executed
   * `type: custom` tool. Real Claude must request that tool on the stream,
   * the client test posts `user.custom_tool_result`, and the model must resume
   * with the returned marker. This is intentionally opt-in because it depends
   * on a real model choosing the custom-tool path.
   */
  it.skipIf(process.env['ORCA_E2E_REAL_CUSTOM_TOOL'] !== '1')(
    'uses a custom tool and continues after user.custom_tool_result',
    async () => {
      const marker = `CUSTOM_TOOL_E2E_OK_${Date.now()}`;
      const ticketId = `T-${Date.now()}`;
      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `agent-loop-custom-tool-${Date.now()}`,
          model: REAL_AGENT.model,
          system:
            'You are testing managed-agent custom tools. ' +
            'When the user asks for a ticket status, you MUST call the lookup_ticket custom tool exactly once. ' +
            'Do not answer before the custom tool result is available. ' +
            `After the tool result arrives, answer with the exact marker ${marker} and include the ticket status from the result.`,
          tools: [
            {
              type: 'agent_toolset',
              default_config: {
                enabled: true,
                permission_policy: { type: 'always_ask' },
              },
            },
            {
              type: 'custom',
              name: 'lookup_ticket',
              description:
                'Look up a support ticket by id. Use this whenever the user asks for ticket status.',
              input_schema: {
                type: 'object',
                properties: { ticket_id: { type: 'string' } },
                required: ['ticket_id'],
              },
            },
          ],
          mcp_servers: [],
          skills: [],
          metadata: { ...REAL_AGENT.metadata, suite: 'real-agent-loop-custom-tool' },
        }),
      });
      expect(agentRes.status).toBe(200);
      const agent = agentRes.json<AgentResponse>();
      created.agents.push(agent.id);

      const sessionRes = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({ environment_id: environmentId, agent_id: agent.id }),
      });
      expect(sessionRes.status).toBe(200);
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
                    `Use the lookup_ticket custom tool to look up ticket_id "${ticketId}". ` +
                    `Do not answer until the tool result is returned.`,
                },
              ],
            },
          ],
          request_id: `agent-loop-custom-tool-${Date.now()}`,
        }),
      });
      expect(submitRes.status).toBe(200);

      const toolFrames = await collectSseFrames(cfg, session.id, {
        deadlineMs: 110_000,
        until: (frames) =>
          customToolUsesFromFrames(frames).some((tool) => tool.name === 'lookup_ticket') ||
          frames.some((frame) =>
            ['agent.turn_failed', 'session.setup_failed', 'session.status_error'].includes(
              frame.type,
            ),
          ),
      });
      const failure = toolFrames.find((frame) =>
        ['agent.turn_failed', 'session.setup_failed', 'session.status_error'].includes(frame.type),
      );
      expect(failure, failure ? JSON.stringify(failure) : undefined).toBeUndefined();
      const customToolUse = customToolUsesFromFrames(toolFrames).find(
        (tool) => tool.name === 'lookup_ticket',
      );
      expect(
        customToolUse,
        `Expected lookup_ticket custom tool use; frames=${JSON.stringify(toolFrames)}`,
      ).toBeDefined();
      expect(
        toolFrames.find(
          (frame) =>
            frame.type === 'agent.tool_use' &&
            typeof frame.name === 'string' &&
            canonicalSandboxToolName(frame.name) === 'mcp__orca__lookup_ticket',
        ),
        `Custom tool was also projected as agent.tool_use; frames=${JSON.stringify(toolFrames)}`,
      ).toBeUndefined();

      const resultText = `${marker} ticket ${ticketId} status=ready`;
      const resultRes = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
        method: 'POST',
        body: JSON.stringify({
          events: [
            {
              type: 'user.custom_tool_result',
              custom_tool_use_id: customToolUse!.id,
              content: [{ type: 'text', text: resultText }],
            },
          ],
          request_id: `agent-loop-custom-tool-result-${Date.now()}`,
        }),
      });
      expect(resultRes.status).toBe(200);

      const finalFrames = await collectSseFrames(cfg, session.id, {
        deadlineMs: 110_000,
        until: (frames) =>
          assistantTextFromFrames(frames).includes(marker) ||
          frames.some((frame) =>
            ['agent.turn_failed', 'session.setup_failed', 'session.status_error'].includes(
              frame.type,
            ),
          ),
      });
      const finalFailure = finalFrames.find((frame) =>
        ['agent.turn_failed', 'session.setup_failed', 'session.status_error'].includes(frame.type),
      );
      expect(finalFailure, finalFailure ? JSON.stringify(finalFailure) : undefined).toBeUndefined();
      const finalText = assistantTextFromFrames(finalFrames);
      expect(
        finalText,
        `Assistant did not include custom tool marker. Reply: ${finalText}`,
      ).toContain(marker);

      await waitForSessionEventTypes(
        cfg,
        session.id,
        ['agent.custom_tool_use', 'user.custom_tool_result'],
        30000,
      );
    },
    { timeout: 220_000, retry: REAL_CLAUDE_RETRY },
  );

  // ---------------------------------------------------------------- scenario 6
  /**
   * **Remote MCP via ai-gateway.** Start a fake Streamable HTTP MCP upstream
   * on the Docker host, create a vault credential for that upstream URL, then ask
   * real Claude to call the remote MCP tool. The harness must fetch the agent
   * + session, mint the ai-gateway JWT, rewrite the SDK's remote MCP config to
   * `/v1/mcp`, and preserve `X-Orca-Backend` / `X-Orca-Credential-Id` headers.
   *
   * Asserts:
   *   (a) The SSE stream shows a remote `mcp__gateway-e2e__gateway_e2e_echo`
   *       tool_use, proving the Claude SDK remote MCP client saw the gateway-
   *       rewritten server, not just the in-process `mcp__orca__*` tools.
   *   (b) The fake upstream received `tools/list` and `tools/call` through the
   *       real ai-gateway with the credential-injected bearer token.
   *   (c) The ai-gateway Kafka audit topic contains an `mcp.forward` record for
   *       this session/backend, and the credential secret is absent from the audit.
   */
  it(
    'calls a remote MCP tool through harness rewrite and ai-gateway audit path',
    async () => {
      await ensureGatewayReachable();

      const marker = `AGENT_GATEWAY_MARKER_${Date.now()}`;
      const secret = `agent-gateway-secret-${Date.now()}`;
      let expectedUpstreamAuthorization = `Bearer ${secret}`;
      const captured: CapturedUpstreamRequest[] = [];
      gatewayUpstream = await startFakeGatewayMcpUpstream(
        captured,
        () => expectedUpstreamAuthorization,
      );

      const vaultRes = await apiCall(cfg, '/v1/vaults', {
        method: 'POST',
        body: JSON.stringify({
          display_name: `agent-loop-gateway-${Date.now()}`,
          metadata: { suite: 'real-agent-loop' },
        }),
      });
      expect(vaultRes.status, vaultRes.text).toBe(200);
      const vault = vaultRes.json<VaultResponse>();
      created.vaults.push(vault.id);

      const credentialRes = await apiCall(cfg, `/v1/vaults/${vault.id}/credentials`, {
        method: 'POST',
        body: JSON.stringify({
          display_name: 'Agent loop gateway static bearer',
          auth: { type: 'static_bearer', token: secret, mcp_server_url: GATEWAY_UPSTREAM_URL },
        }),
      });
      expect(credentialRes.status, credentialRes.text).toBe(200);
      const credential = credentialRes.json<VaultCredentialResponse>();

      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `agent-loop-gateway-agent-${Date.now()}`,
          model: REAL_AGENT.model,
          system:
            'You have access to a remote MCP server named gateway-e2e. ' +
            'When the user asks for the gateway echo, you MUST call the gateway_e2e_echo MCP tool exactly once and then report the tool result.',
          tools: [
            {
              type: 'mcp_toolset',
              mcp_server_name: GATEWAY_BACKEND_NAME,
              default_config: { permission_policy: { type: 'always_allow' } },
            },
          ],
          mcp_servers: [
            {
              name: GATEWAY_BACKEND_NAME,
              url: GATEWAY_UPSTREAM_URL,
            },
          ],
          skills: [],
          metadata: { ...REAL_AGENT.metadata, suite: 'real-agent-loop-ai-gateway' },
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
          vault_ids: [vault.id],
        }),
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
                    `Use the gateway_e2e_echo MCP tool from the ${GATEWAY_BACKEND_NAME} server with message "${marker}". ` +
                    `After the tool returns, reply with only the exact text returned by the tool.`,
                },
              ],
            },
          ],
          request_id: `agent-loop-gateway-${Date.now()}`,
        }),
      });
      expect(submitRes.status, submitRes.text).toBe(200);

      const frames = await collectSseFrames(cfg, session.id, {
        deadlineMs: 140_000,
        until: (f) => hasIdleAfterAssistantText(f, `AI_GATEWAY_AGENT_LOOP_OK ${marker}`),
      });

      const toolUses = toolUseNamesFromFrames(frames);
      const usedRemoteGatewayTool = toolUses.some(
        (n) => n.includes(GATEWAY_BACKEND_NAME) && n.includes('gateway_e2e_echo'),
      );
      expect(
        usedRemoteGatewayTool,
        `Expected remote gateway_e2e_echo tool_use; saw: ${JSON.stringify(toolUses)}`,
      ).toBe(true);

      const allText = assistantTextFromFrames(frames);
      expect(
        allText,
        `Assistant did not report remote MCP tool output. Reply: ${allText.slice(0, 600)}`,
      ).toContain(`AI_GATEWAY_AGENT_LOOP_OK ${marker}`);

      expect(
        captured.some((r) => rpcMethod(r.jsonBody) === 'tools/list'),
        `Expected upstream tools/list call; captured: ${JSON.stringify(captured.map((r) => r.jsonBody))}`,
      ).toBe(true);
      expect(
        captured.some(
          (r) => rpcMethod(r.jsonBody) === 'tools/list' && r.accept?.includes('text/event-stream'),
        ),
        `Expected upstream tools/list to carry Streamable HTTP Accept header; captured: ${JSON.stringify(captured)}`,
      ).toBe(true);
      const toolCall = captured.find((r) => rpcMethod(r.jsonBody) === 'tools/call');
      expect(
        toolCall,
        `Expected upstream tools/call; captured: ${JSON.stringify(captured.map((r) => r.jsonBody))}`,
      ).toBeDefined();
      expect(toolCall).toMatchObject({
        accept: expect.stringContaining('text/event-stream'),
        method: 'POST',
        url: '/mcp',
        authorization: expectedUpstreamAuthorization,
      });
      expect(toolCall?.mcpSessionId).toBeTruthy();
      expect(JSON.stringify(toolCall?.jsonBody)).toContain(marker);

      const audit = await waitForGatewayAudit(workspaceId, session.id, GATEWAY_BACKEND_NAME);
      expect(audit).toMatchObject({
        action: 'mcp.forward',
        principal_id: session.id,
        scope: { workspace_id: workspaceId, session_id: session.id },
        attributes: { destination: GATEWAY_BACKEND_NAME },
      });
      expect(JSON.stringify(audit.decision).toLowerCase()).toContain('allow');
      expect(JSON.stringify(audit)).not.toContain(secret);

      const rotatedMarker = `AGENT_GATEWAY_ROTATED_${Date.now()}`;
      const rotatedSecret = `${secret}-rotated`;
      const archived = await apiCall(
        cfg,
        `/v1/vaults/${vault.id}/credentials/${credential.id}/archive`,
        { method: 'POST' },
      );
      expect(archived.status, archived.text).toBe(200);
      const replacementRes = await apiCall(cfg, `/v1/vaults/${vault.id}/credentials`, {
        method: 'POST',
        body: JSON.stringify({
          display_name: 'Agent loop gateway replacement bearer',
          auth: {
            type: 'static_bearer',
            token: rotatedSecret,
            mcp_server_url: GATEWAY_UPSTREAM_URL,
          },
        }),
      });
      expect(replacementRes.status, replacementRes.text).toBe(200);
      expectedUpstreamAuthorization = `Bearer ${rotatedSecret}`;

      const callsBeforeRotation = captured.length;
      const rotatedSubmit = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
        method: 'POST',
        body: JSON.stringify({
          events: [
            {
              type: 'user.message',
              content: [
                {
                  type: 'text',
                  text:
                    `Call gateway_e2e_echo again with message "${rotatedMarker}". ` +
                    'Reply with only the exact tool result.',
                },
              ],
            },
          ],
          request_id: `agent-loop-gateway-rotated-${Date.now()}`,
        }),
      });
      expect(rotatedSubmit.status, rotatedSubmit.text).toBe(200);
      const rotatedFrames = await collectSseFrames(cfg, session.id, {
        deadlineMs: 140_000,
        until: (f) => hasIdleAfterAssistantText(f, `AI_GATEWAY_AGENT_LOOP_OK ${rotatedMarker}`),
      });
      expect(assistantTextFromFrames(rotatedFrames)).toContain(
        `AI_GATEWAY_AGENT_LOOP_OK ${rotatedMarker}`,
      );
      const rotatedToolCall = captured
        .slice(callsBeforeRotation)
        .find(
          (request) =>
            rpcMethod(request.jsonBody) === 'tools/call' &&
            JSON.stringify(request.jsonBody).includes(rotatedMarker),
        );
      expect(rotatedToolCall).toMatchObject({
        authorization: `Bearer ${rotatedSecret}`,
        method: 'POST',
        url: '/mcp',
      });
    },
    { timeout: 360_000, retry: REAL_CLAUDE_RETRY },
  );

  // ---------------------------------------------------------------- scenario 7
  /**
   * **Concurrent workspace isolation.** Provision two workspaces through the
   * organization admin listener, mount one marker file into each session, and
   * run both turns concurrently through the same Harness replica. Each model
   * must read only its own marker. Archiving one workspace must invalidate its
   * key and tear down its warm runner while the sibling stays on the same
   * sandbox. The active-session gauge is process-global, so assertions are
   * relative to the runners left warm by earlier scenarios.
   */
  it(
    'runs two workspaces concurrently and tears down only the archived workspace runner',
    async () => {
      const seededAdmin = await seedOrganizationAdminApiKey();
      const adminCfg = buildClientFromConfig({
        baseURL: process.env['ORCA_ADMIN_BASE_URL'] ?? 'http://localhost:8082',
        apiKey: seededAdmin.apiKey,
      });
      await ensureStackReachable(adminCfg);

      // Earlier scenarios delete their Registry sessions. The delete route
      // emits lifecycle sentinels so their warm Harness runners release the
      // single-node kind capacity before two new workspaces run concurrently.
      await waitForHarnessActiveSessionsBelow(1, 30_000);

      const runId = Date.now();
      const markerA = `WORKSPACE_A_${runId}`;
      const markerB = `WORKSPACE_B_${runId}`;
      const workspaces: RuntimeWorkspace[] = [];
      try {
        const workspaceA = await provisionRuntimeWorkspace(adminCfg, `Runtime A ${runId}`, markerA);
        workspaces.push(workspaceA);
        const workspaceB = await provisionRuntimeWorkspace(adminCfg, `Runtime B ${runId}`, markerB);
        workspaces.push(workspaceB);

        await Promise.all([
          submitMarkerRead(workspaceA, markerA),
          submitMarkerRead(workspaceB, markerB),
        ]);
        const [framesA, framesB] = await Promise.all([
          collectSseFrames(workspaceA.cfg, workspaceA.sessionId, {
            deadlineMs: 140_000,
            until: (frames) => assistantTextFromFrames(frames).includes(markerA),
          }),
          collectSseFrames(workspaceB.cfg, workspaceB.sessionId, {
            deadlineMs: 140_000,
            until: (frames) => assistantTextFromFrames(frames).includes(markerB),
          }),
        ]);

        const textA = assistantTextFromFrames(framesA);
        const textB = assistantTextFromFrames(framesB);
        expect(textA).toContain(markerA);
        expect(textA).not.toContain(markerB);
        expect(textB).toContain(markerB);
        expect(textB).not.toContain(markerA);
        expect(toolUseNamesFromFrames(framesA)).toContain('mcp__orca__bash');
        expect(toolUseNamesFromFrames(framesB)).toContain('mcp__orca__bash');

        const [sessionABeforeArchive, sessionBBeforeArchive] = await Promise.all([
          apiCall(workspaceA.cfg, `/v1/sessions/${workspaceA.sessionId}`, {
            headers: { 'orca-beta': '1' },
          }),
          apiCall(workspaceB.cfg, `/v1/sessions/${workspaceB.sessionId}`, {
            headers: { 'orca-beta': '1' },
          }),
        ]);
        expect(sessionABeforeArchive.status, sessionABeforeArchive.text).toBe(200);
        expect(sessionBBeforeArchive.status, sessionBBeforeArchive.text).toBe(200);
        const sandboxHandleA = sessionABeforeArchive.json<SessionResponse>().sandbox_handle_id;
        const sandboxHandleB = sessionBBeforeArchive.json<SessionResponse>().sandbox_handle_id;
        expect(sandboxHandleA).toBeTypeOf('string');
        expect(sandboxHandleB).toBeTypeOf('string');
        expect(sandboxHandleA).not.toBe(sandboxHandleB);

        const activeBeforeArchive = await readHarnessActiveSessions();
        expect(activeBeforeArchive).toBeGreaterThanOrEqual(2);

        const archiveA = await apiCall(
          adminCfg,
          `/v1/organizations/workspaces/${workspaceA.id}/archive`,
          { method: 'POST', body: JSON.stringify({}) },
        );
        expect(archiveA.status, archiveA.text).toBe(200);
        expect(archiveA.json<{ archived_at: string | null }>().archived_at).toBeTypeOf('string');
        await waitForHarnessActiveSessionsBelow(activeBeforeArchive, 30_000);

        const [archivedWorkspace, activeWorkspace] = await Promise.all([
          apiCall(workspaceA.cfg, '/v1/agents'),
          apiCall(workspaceB.cfg, `/v1/sessions/${workspaceB.sessionId}`, {
            headers: { 'orca-beta': '1' },
          }),
        ]);
        expect(archivedWorkspace.status).toBe(401);
        expect(activeWorkspace.status, activeWorkspace.text).toBe(200);
        expect(activeWorkspace.json<SessionResponse>().sandbox_handle_id).toBe(sandboxHandleB);
      } finally {
        await Promise.allSettled(
          workspaces.map((workspace) =>
            apiCall(adminCfg, `/v1/organizations/workspaces/${workspace.id}/archive`, {
              method: 'POST',
              body: JSON.stringify({}),
            }),
          ),
        );
      }
    },
    { timeout: 300_000, retry: REAL_CLAUDE_RETRY },
  );
});

// ---------------------------------------------------------------- helpers ----

interface RuntimeWorkspace {
  id: string;
  cfg: OrcaClientConfig;
  sessionId: string;
}

async function provisionRuntimeWorkspace(
  adminCfg: OrcaClientConfig,
  name: string,
  marker: string,
): Promise<RuntimeWorkspace> {
  const workspaceRes = await apiCall(adminCfg, '/v1/organizations/workspaces', {
    method: 'POST',
    body: JSON.stringify({ name }),
  });
  if (workspaceRes.status !== 200) {
    throw new Error(`runtime workspace create failed: ${workspaceRes.status} ${workspaceRes.text}`);
  }
  const workspaceId = workspaceRes.json<{ id: string }>().id;
  try {
    const keyRes = await apiCall(adminCfg, `/v1/organizations/workspaces/${workspaceId}/api_keys`, {
      method: 'POST',
      body: JSON.stringify({ name: `${name} runtime` }),
    });
    if (keyRes.status !== 201) {
      throw new Error(`runtime workspace key create failed: ${keyRes.status} ${keyRes.text}`);
    }
    const apiKey = keyRes.json<{ key?: string }>().key;
    if (!apiKey) throw new Error('runtime workspace key response omitted plaintext key');
    const workspaceCfg = buildClientFromConfig({ apiKey });

    const upload = new FormData();
    upload.set('file', new Blob([`${marker}\n`], { type: 'text/plain' }), 'workspace-marker.txt');
    upload.set('purpose', 'agent');
    const fileRes = await apiCall(workspaceCfg, '/v1/files', { method: 'POST', body: upload });
    if (fileRes.status !== 200) {
      throw new Error(`runtime marker upload failed: ${fileRes.status} ${fileRes.text}`);
    }
    const fileId = fileRes.json<{ id: string }>().id;

    const agentRes = await apiCall(workspaceCfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `workspace-reader-${marker}`,
        model: REAL_AGENT.model,
        system:
          'You are a strict file reader. You MUST call mcp__orca__bash to read the requested file. ' +
          'After the tool result arrives, reply with only the exact file contents.',
        tools: [{ type: 'agent_toolset_20260401' }],
        mcp_servers: [],
        skills: [],
        metadata: { ...REAL_AGENT.metadata, suite: 'multi-workspace-runtime-isolation' },
      }),
    });
    if (agentRes.status !== 200) {
      throw new Error(`runtime agent create failed: ${agentRes.status} ${agentRes.text}`);
    }
    const agentId = agentRes.json<{ id: string }>().id;
    const environmentId = await createTestEnvironment(workspaceCfg, `runtime-${marker}`);

    const sessionRes = await apiCall(workspaceCfg, '/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({
        agent_id: agentId,
        environment_id: environmentId,
        resources: [
          {
            type: 'file',
            file_id: fileId,
            mount_path: '/mnt/workspace-marker.txt',
            access: 'read_only',
          },
        ],
      }),
    });
    if (sessionRes.status !== 200) {
      throw new Error(`runtime session create failed: ${sessionRes.status} ${sessionRes.text}`);
    }
    return {
      id: workspaceId,
      cfg: workspaceCfg,
      sessionId: sessionRes.json<{ id: string }>().id,
    };
  } catch (error) {
    await apiCall(adminCfg, `/v1/organizations/workspaces/${workspaceId}/archive`, {
      method: 'POST',
      body: JSON.stringify({}),
    }).catch(() => undefined);
    throw error;
  }
}

async function submitMarkerRead(
  workspace: RuntimeWorkspace,
  marker: string,
): Promise<{ json: <T = unknown>() => T }> {
  const response = await apiCall(workspace.cfg, `/v1/sessions/${workspace.sessionId}/events`, {
    method: 'POST',
    body: JSON.stringify({
      events: [
        {
          type: 'user.message',
          content: [
            {
              type: 'text',
              text:
                'Call mcp__orca__bash with `cat /mnt/workspace-marker.txt || cat mnt/workspace-marker.txt`. ' +
                `Reply with only the file contents, which must include ${marker}.`,
            },
          ],
        },
      ],
      request_id: `workspace-runtime-${marker}`,
    }),
  });
  if (response.status !== 200) {
    throw new Error(`runtime event submit failed: ${response.status} ${response.text}`);
  }
  return response;
}

async function readHarnessActiveSessions(): Promise<number> {
  const baseURL = process.env['ORCA_HARNESS_BASE_URL'] ?? 'http://localhost:9094';
  const response = await fetch(`${baseURL}/metrics`);
  if (!response.ok) {
    throw new Error(`harness metrics returned ${response.status}`);
  }
  const metrics = await response.text();
  const match = /^harness_server_active_sessions\s+([0-9.]+)$/m.exec(metrics);
  if (!match) throw new Error('harness active-session gauge is missing');
  return Number(match[1]);
}

async function waitForHarnessActiveSessionsBelow(
  upperBound: number,
  deadlineMs: number,
): Promise<void> {
  const deadline = Date.now() + deadlineMs;
  let observed = -1;
  while (Date.now() < deadline) {
    observed = await readHarnessActiveSessions();
    if (observed < upperBound) return;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `expected fewer than ${upperBound} active Harness sessions, observed ${observed}`,
  );
}

function isAssistantTextFrame(f: SseFrame): boolean {
  if (f.type !== 'agent.message') return false;
  expect(typeof f.processed_at).toBe('string');
  expect(Number.isNaN(Date.parse(f.processed_at))).toBe(false);
  expect(Object.hasOwn(f, 'message')).toBe(false);
  const blocks = (Array.isArray(f.content) ? f.content : []) as Array<{
    type: string;
    text?: string;
  }>;
  return blocks.some((b) => b?.type === 'text' && typeof b.text === 'string' && b.text.length > 0);
}

function assistantTextFromFrames(frames: SseFrame[]): string {
  return frames.filter(isAssistantTextFrame).map(assistantText).join('\n');
}

function hasIdleAfterAssistantText(frames: SseFrame[], expectedText: string): boolean {
  const assistantIndex = frames.findIndex(
    (frame) => frame.type === 'agent.message' && assistantText(frame).includes(expectedText),
  );
  return (
    assistantIndex >= 0 &&
    frames.slice(assistantIndex + 1).some((frame) => frame.type === 'session.status_idle')
  );
}

function hasIdleAfterToolResults(frames: SseFrame[], requiredCount: number): boolean {
  const toolResultIndexes = frames.flatMap((frame, index) =>
    frame.type === 'agent.tool_result' || frame.type === 'agent.mcp_tool_result' ? [index] : [],
  );
  if (toolResultIndexes.length < requiredCount) return false;
  const lastRequiredResult = toolResultIndexes[requiredCount - 1]!;
  return frames.slice(lastRequiredResult + 1).some((frame) => frame.type === 'session.status_idle');
}

function assistantText(f: SseFrame): string {
  const blocks = (Array.isArray(f.content) ? f.content : []) as Array<{
    type: string;
    text?: string;
  }>;
  return blocks
    .filter((b) => b?.type === 'text' && typeof b.text === 'string')
    .map((b) => b.text!)
    .join('\n');
}

/** Extract tool_use `name` fields from canonical standalone tool events. */
function extractToolUseNames(f: SseFrame): string[] {
  const names: string[] = [];
  if (f.type === 'agent.tool_use' || f.type === 'agent.mcp_tool_use') {
    const name = (f as { name?: string }).name;
    const serverName = (f as { mcp_server_name?: string }).mcp_server_name;
    if (typeof name === 'string') {
      names.push(
        f.type === 'agent.mcp_tool_use' && typeof serverName === 'string'
          ? `mcp__${serverName}__${name}`
          : canonicalSandboxToolName(name),
      );
    }
  }
  return names;
}

function toolUseNamesFromFrames(frames: SseFrame[]): string[] {
  return frames.flatMap(extractToolUseNames);
}

function customToolUsesFromFrames(frames: SseFrame[]): Array<{
  id: string;
  name: string;
  input?: unknown;
}> {
  return frames
    .filter((frame) => frame.type === 'agent.custom_tool_use')
    .flatMap((frame) => {
      const id = typeof frame.id === 'string' ? frame.id : undefined;
      const name = typeof frame.name === 'string' ? frame.name : undefined;
      if (!id || !name) return [];
      return [{ id, name, input: frame.input }];
    });
}

async function waitForSessionEventTypes(
  cfg: OrcaClientConfig,
  sessionId: string,
  expectedTypes: string[],
  timeoutMs: number,
): Promise<Array<{ type: string }>> {
  const deadline = Date.now() + timeoutMs;
  let last: Array<{ type: string }> = [];
  while (Date.now() < deadline) {
    const eventsRes = await apiCall(cfg, `/v1/sessions/${sessionId}/events?limit=1000`, {
      method: 'GET',
    });
    expect(eventsRes.status).toBe(200);
    const eventsBody = eventsRes.json<{ data: Array<{ type: string }> }>();
    last = eventsBody.data;
    if (expectedTypes.every((type) => last.some((event) => event.type === type))) {
      return last;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(
    `session ${sessionId} did not index expected event types ${expectedTypes.join(', ')} within ${timeoutMs}ms; saw ${last.map((event) => event.type).join(', ')}`,
  );
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
    expect(res.status).toBe(200);
    last = res.json<SessionResponse>();
    if ((last.usage?.input_tokens ?? 0) > 0 && (last.usage?.output_tokens ?? 0) > 0) {
      return last;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(
    `session ${sessionId} did not report positive token usage within ${timeoutMs}ms; last=${JSON.stringify(
      last,
    )}`,
  );
}

async function startFakeGatewayMcpUpstream(
  captured: CapturedUpstreamRequest[],
  expectedAuthorization: () => string,
): Promise<Server> {
  await assertLocalPortUnused(GATEWAY_UPSTREAM_PORT, 'real-agent-loop gateway upstream');
  const server = createServer(async (req, res) => {
    if (req.method !== 'POST' || req.url !== '/mcp') {
      res.writeHead(404, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'not found' }));
      return;
    }

    const rawBody = await readRequestBody(req);
    const jsonBody = JSON.parse(rawBody) as { id?: unknown; method?: string; params?: unknown };
    captured.push({
      accept: req.headers.accept,
      authorization: req.headers.authorization,
      mcpSessionId:
        typeof req.headers['mcp-session-id'] === 'string'
          ? req.headers['mcp-session-id']
          : undefined,
      method: req.method,
      url: req.url,
      rawBody,
      jsonBody,
    });

    if (req.headers.authorization !== expectedAuthorization()) {
      res.writeHead(401, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'missing injected credential' }));
      return;
    }

    if (typeof jsonBody.id === 'undefined') {
      // JSON-RPC notification (e.g. notifications/initialized). Streamable
      // HTTP servers acknowledge notifications without a JSON-RPC response.
      res.writeHead(202, { 'content-type': 'application/json' });
      res.end('');
      return;
    }

    const response = responseForGatewayMcpRequest(jsonBody);
    const mcpSessionId =
      jsonBody.method === 'initialize'
        ? `mcp-session-${Date.now()}`
        : typeof req.headers['mcp-session-id'] === 'string'
          ? req.headers['mcp-session-id']
          : undefined;
    res.writeHead(200, {
      'content-type': 'application/json',
      ...(mcpSessionId ? { 'mcp-session-id': mcpSessionId } : {}),
    });
    res.end(JSON.stringify({ jsonrpc: '2.0', id: jsonBody.id, result: response }));
  });

  const error = once(server, 'error').then(([e]) => {
    throw e;
  });
  server.listen(GATEWAY_UPSTREAM_PORT, '0.0.0.0');
  await Promise.race([once(server, 'listening'), error]);
  return server;
}

async function assertLocalPortUnused(port: number, label: string): Promise<void> {
  await Promise.all(['127.0.0.1', '::1'].map((host) => assertHostPortUnused(host, port, label)));
}

function assertHostPortUnused(host: string, port: number, label: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ host, port });
    const finish = (err?: Error) => {
      socket.removeAllListeners();
      socket.destroy();
      if (err) reject(err);
      else resolve();
    };
    socket.setTimeout(300, () => finish());
    socket.once('connect', () =>
      finish(
        new Error(
          `${label} port ${host}:${port} is already in use; stop the listener or update services/dev/ai-gateway-config.yaml and this spec together`,
        ),
      ),
    );
    socket.once('error', (err: NodeJS.ErrnoException) => {
      if (err.code === 'ECONNREFUSED' || err.code === 'EADDRNOTAVAIL') finish();
      else finish(err);
    });
  });
}

function responseForGatewayMcpRequest(req: {
  method?: string;
  params?: unknown;
}): Record<string, unknown> {
  switch (req.method) {
    case 'initialize':
      return {
        protocolVersion: '2025-06-18',
        capabilities: { tools: {} },
        serverInfo: { name: GATEWAY_BACKEND_NAME, version: '1.0.0' },
      };
    case 'tools/list':
      return {
        tools: [
          {
            name: 'gateway_e2e_echo',
            description:
              'Echo a marker through the real ai-gateway e2e upstream. Use this when the user asks for gateway echo.',
            inputSchema: {
              type: 'object',
              properties: { message: { type: 'string' } },
              required: ['message'],
            },
          },
        ],
      };
    case 'tools/call': {
      const params = req.params as { arguments?: { message?: unknown }; name?: string } | undefined;
      const message =
        typeof params?.arguments?.message === 'string' ? params.arguments.message : '';
      return {
        content: [{ type: 'text', text: `AI_GATEWAY_AGENT_LOOP_OK ${message}` }],
        isError: false,
      };
    }
    case 'ping':
      return {};
    default:
      return {};
  }
}

function readRequestBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function rpcMethod(body: unknown): string | undefined {
  return typeof body === 'object' && body !== null && 'method' in body
    ? String((body as { method?: unknown }).method)
    : undefined;
}

async function ensureGatewayReachable(): Promise<void> {
  const gatewayUrl = new URL(gatewayMcpUrl());
  const adminPort = process.env['AI_GATEWAY_ADMIN_PORT'] ?? '9099';
  const healthz = `${gatewayUrl.protocol}//${gatewayUrl.hostname}:${adminPort}/healthz`;
  const res = await fetchWithTimeout(healthz, {}, 5_000);
  if (!res.ok) {
    throw new Error(
      `ai-gateway not healthy at ${healthz}: ${res.status} ${await res.text()}. ` +
        'Start the local stack with `make stack-up`.',
    );
  }
}

function gatewayMcpUrl(): string {
  return gatewayMcpEndpointUrl(process.env['AI_GATEWAY_URL'] ?? 'http://localhost:8090');
}

function gatewayMcpEndpointUrl(gatewayUrl: string): string {
  let trimmed = gatewayUrl;
  while (trimmed.endsWith('/')) trimmed = trimmed.slice(0, -1);
  if (trimmed.endsWith('/v1/mcp')) return trimmed;
  return trimmed + '/v1/mcp';
}

async function waitForGatewayAudit(
  workspaceId: string,
  sessionId: string,
  backend: string,
): Promise<AuditRecord> {
  const topic = `orca.${workspaceId}.audit.ai-gateway`;
  const brokers = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
  const kafka = new Kafka({
    brokers,
    clientId: 'orca-e2e-agent-ai-gateway',
    logLevel: logLevel.NOTHING,
  });
  await waitForKafkaTopic(kafka, topic);

  const consumer = kafka.consumer({
    groupId: `orca-e2e-agent-ai-gateway-${process.pid}-${Date.now()}`,
  });
  await consumer.connect();
  try {
    await consumer.subscribe({ topic, fromBeginning: true });
    const found = new Promise<AuditRecord>((resolve) => {
      void consumer.run({
        eachMessage: async ({ message }) => {
          const record = parseAuditMessage(message.value?.toString('utf8'));
          if (
            record?.action === 'mcp.forward' &&
            record.principal_id === sessionId &&
            record.scope?.['session_id'] === sessionId &&
            record.attributes?.['destination'] === backend
          ) {
            resolve(record);
          }
        },
      });
    });
    return await withTimeout(
      found,
      25_000,
      `timed out waiting for ai-gateway audit session_id=${sessionId} backend=${backend} on topic ${topic}`,
    );
  } finally {
    await consumer.stop().catch(() => {});
    await consumer.disconnect().catch(() => {});
  }
}

async function waitForKafkaTopic(kafka: Kafka, topic: string): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
      const topics = await admin.listTopics();
      if (topics.includes(topic)) return;
      await new Promise((r) => setTimeout(r, 500));
    }
    throw new Error(`Kafka topic ${topic} was not created by ai-gateway audit sink`);
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

function parseAuditMessage(value: string | undefined): AuditRecord | null {
  if (!value) return null;
  try {
    return JSON.parse(value) as AuditRecord;
  } catch {
    return null;
  }
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  timeoutMs: number,
): Promise<Response> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: ac.signal });
  } finally {
    clearTimeout(timer);
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
      (err) => {
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}
