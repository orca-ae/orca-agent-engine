// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @orca/e2e-tests — Layer A: wire-protocol conformance.
 *
 * These specs drive every public REST surface on the registry-service-ts via
 * raw HTTP and assert the response shape matches the contracts in
 * `services/registry-service-ts/src/contracts/`. They run against a real
 * stack started by `make stack-up`; nothing is mocked.
 *
 * **SDK vs raw fetch.** The suite is pinned to official SDK 0.113.0. SDK calls
 * prove generated-client compatibility; raw fetch remains useful for exact
 * response-key, error-envelope, and Orca-extension assertions.
 *
 * **No skips.** Every test runs unconditionally; if the stack isn't
 * reachable the `ensureStackReachable` pre-flight throws with a guidance
 * message so the failure mode is friendly.
 *
 * **Error envelope.** Every non-2xx public response uses the
 * Anthropic-canonical `{ type: "error", error: { type, message }, request_id }`
 * response body. Header-level equivalence is outside this milestone.
 *
 * Keep covered scenarios aligned with
 * `docs/managed-agents/conformance-matrix.md`.
 */
import { createHash, randomUUID } from 'node:crypto';
import { strFromU8, strToU8, unzipSync, zipSync } from 'fflate';
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import {
  apiCall,
  authHeaders,
  buildClientFromConfig,
  ensureStackReachable,
  type OrcaClientConfig,
} from '../src/client.js';
import { seedWorkspaceApiKey } from '../src/seed.js';
import { createTestEnvironment, deleteTestEnvironment } from './environment-helpers.js';

interface AgentResponse {
  id: string;
  type: 'agent';
  name: string;
  description: string | null;
  multiagent: null;
  version: number;
  // Default (Claude) clients receive the {id, speed?, effort?} shape per
  // managed-agents-2026-04-01; orca-beta clients keep {provider, id}.
  model: {
    id: string;
    speed?: 'standard' | 'fast';
    effort?: { type: 'low' | 'medium' | 'high' | 'xhigh' | 'max' };
  };
  system: string | null;
  tools: Array<{ type: string }>;
  mcp_servers: unknown[];
  skills: AgentSkillRef[];
  metadata: Record<string, unknown>;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

type AgentSkillRef =
  | { type: 'anthropic'; skill_id: string; version: string }
  | { type: 'custom'; skill_id: string; version: string };

interface SkillResponse {
  id: string;
  created_at: string;
  display_title: string | null;
  latest_version: string;
  source: 'anthropic' | 'custom';
  type: 'skill';
  updated_at: string;
}

interface SkillVersionResponse {
  id: string;
  created_at: string;
  description: string;
  directory: string;
  name: string;
  skill_id: string;
  type: 'skill_version';
  version: string;
}

interface FileResponse {
  id: string;
  created_at: string;
  filename: string;
  mime_type: string;
  size_bytes: number;
  type: 'file';
  downloadable: boolean;
  scope: { type: 'session'; id: string } | null;
}

interface MemoryStoreResponse {
  id: string;
  type: 'memory_store';
  name: string;
  description: string;
  metadata: Record<string, string>;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

interface FileSessionResource {
  id: string;
  type: 'file';
  file_id: string;
  mount_path: string;
  created_at: string;
  updated_at: string;
}

interface MemoryStoreSessionResource {
  type: 'memory_store';
  memory_store_id: string;
  access?: 'read_only' | 'read_write' | null;
  description?: string;
  instructions?: string | null;
  mount_path?: string | null;
  name?: string | null;
}

interface GithubRepositorySessionResource {
  id: string;
  type: 'github_repository';
  url: string;
  mount_path: string;
  checkout?: { type: 'branch'; name: string } | { type: 'commit'; sha: string } | null;
  created_at: string;
  updated_at: string;
}

type SessionResource =
  | FileSessionResource
  | MemoryStoreSessionResource
  | GithubRepositorySessionResource;

type SessionAgentResponse = Omit<
  AgentResponse,
  'metadata' | 'archived_at' | 'created_at' | 'updated_at'
>;

interface SessionResponse {
  id: string;
  type: 'session';
  agent: SessionAgentResponse;
  title: string | null;
  metadata: Record<string, string>;
  environment_id: string;
  vault_ids: string[];
  status: 'idle' | 'running' | 'rescheduling' | 'terminated';
  stats: {
    active_seconds: number;
    duration_seconds: number;
  };
  timing: {
    started_at: string | null;
    last_active_at: string | null;
    active_seconds: number;
    duration_seconds: number;
  };
  deployment_id: string | null;
  outcome_evaluations: unknown[];
  usage: {
    cache_creation?: {
      ephemeral_1h_input_tokens?: number;
      ephemeral_5m_input_tokens?: number;
    };
    cache_read_input_tokens?: number;
    input_tokens?: number;
    output_tokens?: number;
  };
  archived_at: string | null;
  resources: SessionResource[];
  created_at: string;
  updated_at: string;
}

interface VaultResponse {
  id: string;
  type: 'vault';
  display_name: string;
  metadata: Record<string, unknown>;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

interface VaultCredentialResponse {
  id: string;
  type: 'vault_credential';
  vault_id: string;
  display_name: string | null;
  auth:
    | { type: 'static_bearer'; mcp_server_url: string }
    | {
        type: 'mcp_oauth';
        mcp_server_url: string;
        expires_at?: string | null;
        refresh?: {
          token_endpoint: string;
          client_id: string;
          token_endpoint_auth: { type: string };
          resource?: string | null;
          scope?: string | null;
        };
      };
  metadata: Record<string, unknown>;
  archived_at: string | null;
  created_at: string;
  updated_at: string;
}

interface ErrorEnvelope {
  type: 'error';
  error: { type: string; message: string };
  request_id: string | null;
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
    const body = versions.json<{ data: SkillVersionResponse[] }>();
    for (const version of body.data) {
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

describe('Layer A: wire-protocol conformance (live registry)', () => {
  let cfg: OrcaClientConfig;
  let agentId: string | null = null;
  let environmentId: string;

  /**
   * Per-test scoped cleanup buckets. Each `it()` body pushes the IDs it
   * creates into these arrays AS SOON AS the create call returns, BEFORE
   * any subsequent `expect(...)` calls. The `afterEach` then drains the
   * arrays in reverse-dependency order (sessions before
   * agents/files/memory_stores; vaults last for dependency cleanup). This way
   * a thrown assertion in the middle of a test still leaves the dev DB clean for the next run instead of
   * orphaning rows that accumulate on every retry.
   *
   * Each cleanup call uses `.catch(() => {})` so a failed delete doesn't mask
   * the actual test failure — the assertion error stays the surface signal.
   */
  const created: {
    sessions: string[];
    agents: string[];
    files: string[];
    memStores: string[];
    vaults: string[];
    skills: string[];
  } = {
    sessions: [],
    agents: [],
    files: [],
    memStores: [],
    vaults: [],
    skills: [],
  };

  beforeAll(async () => {
    const seeded = await seedWorkspaceApiKey();
    const { apiKey } = seeded;
    cfg = buildClientFromConfig({ apiKey });
    await ensureStackReachable(cfg);
    environmentId = await createTestEnvironment(cfg, 'wire-session-env');
    // Seed a single agent the session-scenario tests reuse. Each test that
    // mutates state creates its own agent so the assertions are independent.
    const res = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `wire-baseline-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      }),
    });
    if (res.status !== 200) {
      throw new Error(`baseline agent create failed: ${res.status} ${res.text}`);
    }
    agentId = res.json<AgentResponse>().id;
  });

  afterEach(async () => {
    // Reverse-dependency order: sessions before agents/files/memory_stores;
    // vaults last for dependency cleanup.
    // `.splice(0)` drains-and-empties the array atomically so a failed
    // cleanup of one entity doesn't re-attempt next test.
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
        console.warn(`wire-conformance: cleanup of Skill ${id} failed`, error);
      });
    }
    for (const id of created.vaults.splice(0)) {
      await apiCall(cfg, `/v1/vaults/${id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
  });

  afterAll(async () => {
    // Clean up the baseline agent. Best-effort — if delete fails the next
    // run still works since seedWorkspaceApiKey is idempotent.
    if (agentId) {
      await apiCall(cfg, `/v1/agents/${agentId}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      }).catch(() => {});
    }
    if (environmentId) {
      await deleteTestEnvironment(cfg, environmentId).catch(() => {});
    }
  });

  // ---------------------------------------------------------------- scenario 1
  describe('scenario 1: agents lifecycle (POST → GET → POST update → DELETE)', () => {
    it('round-trips a complete agent lifecycle with the shape contracts', async () => {
      // POST /v1/agents
      const createRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `wire-lifecycle-${Date.now()}`,
          model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
          system: 'wire-conformance baseline',
          tools: [{ type: 'agent_toolset_20260401' }],
          mcp_servers: [],
          skills: [],
          metadata: { suite: 'wire-conformance', scenario: '1' },
        }),
      });
      expect(createRes.status).toBe(200);
      expect(createRes.headers.get('content-type')).toMatch(/application\/json/);
      const createdAgent = createRes.json<AgentResponse>();
      // Track for afterEach cleanup BEFORE any other expect() that could throw.
      // The DELETE at the end of the happy path removes it; if any
      // assertion below throws, afterEach catches the orphan.
      created.agents.push(createdAgent.id);
      expect(createdAgent.id).toMatch(/^agt_[A-Za-z0-9_-]+$/);
      expect(createdAgent.type).toBe('agent');
      expect(createdAgent.name).toMatch(/^wire-lifecycle-/);
      expect(createdAgent.description).toBeNull();
      expect(createdAgent.multiagent).toBeNull();
      expect(createdAgent.version).toBe(1);
      // Default clients (no orca-beta header) receive the Claude {id, speed}
      // shape; speed defaults to 'standard' when unset on input.
      expect(createdAgent.model).toEqual({
        id: 'claude-3-5-sonnet-20240620',
        speed: 'standard',
      });
      // tools[].type round-trips as the dated form (agent_toolset_20260401)
      // when no `orca-beta` header is set — see toolset-aliasing in the
      // contract layer.
      expect(createdAgent.tools).toHaveLength(1);
      expect(createdAgent.tools[0]?.type).toBe('agent_toolset_20260401');
      expect(createdAgent.archived_at).toBeNull();
      expect(createdAgent.created_at).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
      expect(createdAgent.updated_at).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);

      // GET /v1/agents/:id
      const getRes = await apiCall(cfg, `/v1/agents/${createdAgent.id}`, { method: 'GET' });
      expect(getRes.status).toBe(200);
      const got = getRes.json<AgentResponse>();
      expect(got.id).toBe(createdAgent.id);
      expect(got.version).toBe(1);
      expect(got.type).toBe('agent');
      expect(got.metadata).toMatchObject({ suite: 'wire-conformance', scenario: '1' });

      const noopRes = await apiCall(cfg, `/v1/agents/${createdAgent.id}`, {
        method: 'POST',
        body: JSON.stringify({
          version: got.version,
          name: got.name,
          model: got.model,
          system: got.system,
          tools: got.tools,
          mcp_servers: got.mcp_servers,
          skills: got.skills,
          metadata: got.metadata,
        }),
      });
      expect(noopRes.status).toBe(200);
      const noop = noopRes.json<AgentResponse>();
      expect(noop.id).toBe(createdAgent.id);
      expect(noop.version).toBe(1);
      expect(noop.system).toBe('wire-conformance baseline');

      const listRes = await apiCall(cfg, '/v1/agents', { method: 'GET' });
      expect(listRes.status).toBe(200);
      const listBody = listRes.json<{
        data: AgentResponse[];
        next_page: string | null;
      }>();
      expect(listBody.data.map((a) => a.id)).toContain(createdAgent.id);
      expect(listBody.next_page).toBeNull();
      expect(listBody).not.toHaveProperty('agents');
      expect(listBody).not.toHaveProperty('page_info');

      // Update — registry uses POST /v1/agents/:id (not PATCH).
      // See `services/registry-service-ts/src/contracts/agents.contract.ts:76`.
      const updateRes = await apiCall(cfg, `/v1/agents/${createdAgent.id}`, {
        method: 'POST',
        body: JSON.stringify({ version: createdAgent.version, system: 'updated-system' }),
      });
      expect(updateRes.status).toBe(200);
      const updated = updateRes.json<AgentResponse>();
      expect(updated.id).toBe(createdAgent.id);
      expect(updated.version).toBe(2);
      expect(updated.system).toBe('updated-system');

      // GET /v1/agents/:id/versions — Claude Managed Agents beta shape.
      const versionsRes = await apiCall(cfg, `/v1/agents/${createdAgent.id}/versions`, {
        method: 'GET',
      });
      expect(versionsRes.status).toBe(200);
      const versionsBody = versionsRes.json<{ data: AgentResponse[]; next_page: string | null }>();
      expect(versionsBody.data.map((v) => v.version)).toEqual([2, 1]);
      expect(versionsBody.data[0]?.system).toBe('updated-system');
      expect(versionsBody.data[1]?.system).toBe('wire-conformance baseline');
      expect(versionsBody.next_page).toBeNull();
      expect(versionsBody).not.toHaveProperty('versions');
      expect(versionsBody).not.toHaveProperty('page_info');

      // DELETE /v1/agents/:id — registry contract requires an empty JSON body.
      // afterEach also runs DELETE; the second call is a 404 swallowed by
      // `.catch(() => {})`. Net effect: cleanup happens exactly once even on
      // failure paths.
      const delRes = await apiCall(cfg, `/v1/agents/${createdAgent.id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      });
      expect(delRes.status).toBe(200);
      expect(delRes.json<{ id: string; type: string }>()).toEqual({
        id: createdAgent.id,
        type: 'agent_deleted',
      });

      // GET after delete → 404.
      const get404 = await apiCall(cfg, `/v1/agents/${createdAgent.id}`, { method: 'GET' });
      expect(get404.status).toBe(404);
      const err = get404.json<ErrorEnvelope>();
      expect(err.type).toBe('error');
      expect(err.error.type).toBe('not_found_error');
      expect(err.error.message.length).toBeGreaterThan(0);
    });

    it('updates an agent when the request version matches the current version', async () => {
      const createRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `wire-update-version-ok-${Date.now()}`,
          model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
          system: 'version precondition baseline',
          tools: [],
          mcp_servers: [],
          skills: [],
          metadata: { suite: 'wire-conformance', scenario: '1-version-ok' },
        }),
      });
      expect(createRes.status).toBe(200);
      const createdAgent = createRes.json<AgentResponse>();
      created.agents.push(createdAgent.id);
      expect(createdAgent.version).toBe(1);

      const updateRes = await apiCall(cfg, `/v1/agents/${createdAgent.id}`, {
        method: 'POST',
        body: JSON.stringify({ version: createdAgent.version, system: 'version-matched update' }),
      });
      expect(updateRes.status).toBe(200);
      const updated = updateRes.json<AgentResponse>();
      expect(updated.id).toBe(createdAgent.id);
      expect(updated.version).toBe(2);
      expect(updated.system).toBe('version-matched update');
    });

    it('rejects an agent update when the request version does not match the current version', async () => {
      const createRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `wire-update-version-conflict-${Date.now()}`,
          model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
          system: 'version precondition baseline',
          tools: [],
          mcp_servers: [],
          skills: [],
          metadata: { suite: 'wire-conformance', scenario: '1-version-conflict' },
        }),
      });
      expect(createRes.status).toBe(200);
      const createdAgent = createRes.json<AgentResponse>();
      created.agents.push(createdAgent.id);
      expect(createdAgent.version).toBe(1);

      const conflictRes = await apiCall(cfg, `/v1/agents/${createdAgent.id}`, {
        method: 'POST',
        body: JSON.stringify({ version: createdAgent.version + 1, system: 'conflicting update' }),
      });
      expect(conflictRes.status).toBe(409);
      expect(conflictRes.json<ErrorEnvelope>().error.message).toMatch(/version/i);

      const getRes = await apiCall(cfg, `/v1/agents/${createdAgent.id}`, { method: 'GET' });
      expect(getRes.status).toBe(200);
      const got = getRes.json<AgentResponse>();
      expect(got.version).toBe(1);
      expect(got.system).toBe('version precondition baseline');
    });

    it('updates an agent when the optional version precondition is omitted', async () => {
      const createRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `wire-update-version-required-${Date.now()}`,
          model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
          system: 'version required baseline',
          tools: [],
          mcp_servers: [],
          skills: [],
          metadata: { suite: 'wire-conformance', scenario: '1-version-optional' },
        }),
      });
      expect(createRes.status).toBe(200);
      const createdAgent = createRes.json<AgentResponse>();
      created.agents.push(createdAgent.id);

      const updateRes = await apiCall(cfg, `/v1/agents/${createdAgent.id}`, {
        method: 'POST',
        body: JSON.stringify({ system: 'versionless update' }),
      });
      expect(updateRes.status).toBe(200);
      const updated = updateRes.json<AgentResponse>();
      expect(updated.id).toBe(createdAgent.id);
      expect(updated.version).toBe(2);
      expect(updated.system).toBe('versionless update');
    });
  });

  // ---------------------------------------------------------------- scenario 2
  describe('scenario 2: files lifecycle (POST upload → GET → DELETE)', () => {
    it('round-trips a file upload through multipart, GET, and DELETE', async () => {
      const content = Buffer.from('wire-conformance file contents — hello, orca\n', 'utf-8');

      // POST /v1/files — multipart form-data.
      const form = new FormData();
      form.set(
        'file',
        new Blob([content], { type: 'text/plain' }),
        `wire-conformance-${Date.now()}.txt`,
      );
      const createRes = await apiCall(cfg, '/v1/files', {
        method: 'POST',
        body: form,
      });
      expect(createRes.status).toBe(200);
      const file = createRes.json<FileResponse>();
      created.files.push(file.id);
      expect(file.id).toMatch(/^file_[A-Za-z0-9_-]+$/);
      expect(file.type).toBe('file');
      expect(file.filename).toMatch(/^wire-conformance-/);
      expect(file.mime_type).toBe('text/plain');
      expect(file.size_bytes).toBe(content.byteLength);
      expect(file.downloadable).toBe(false);
      expect(file.scope).toBeNull();
      expect(file.created_at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
      expect(Object.keys(file).sort()).toEqual([
        'created_at',
        'downloadable',
        'filename',
        'id',
        'mime_type',
        'scope',
        'size_bytes',
        'type',
      ]);

      // GET /v1/files/:id
      const getRes = await apiCall(cfg, `/v1/files/${file.id}`, { method: 'GET' });
      expect(getRes.status).toBe(200);
      const got = getRes.json<FileResponse>();
      expect(got).toEqual(file);

      // DELETE /v1/files/:id
      const delRes = await apiCall(cfg, `/v1/files/${file.id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      });
      expect(delRes.status).toBe(200);
      expect(delRes.json<{ id: string; type: string }>()).toEqual({
        id: file.id,
        type: 'file_deleted',
      });
      const get404 = await apiCall(cfg, `/v1/files/${file.id}`, { method: 'GET' });
      expect(get404.status).toBe(404);
      expect(get404.json<ErrorEnvelope>().error.message).toBeTypeOf('string');
    });
  });

  // ---------------------------------------------------------------- scenario 3
  describe('scenario 3: memory_stores lifecycle (POST → GET → DELETE)', () => {
    it('round-trips a memory_store create, fetch, and delete', async () => {
      const name = `wire-mem-${Date.now()}`;
      // POST /v1/memory_stores
      const createRes = await apiCall(cfg, '/v1/memory_stores', {
        method: 'POST',
        body: JSON.stringify({ name, description: 'wire-conformance memory store' }),
      });
      expect(createRes.status).toBe(200);
      const store = createRes.json<MemoryStoreResponse>();
      created.memStores.push(store.id);
      expect(store.id).toMatch(/^mems_[A-Za-z0-9_-]+$/);
      expect(store.type).toBe('memory_store');
      expect(store.name).toBe(name);
      expect(store.description).toBe('wire-conformance memory store');
      expect(store.metadata).toEqual({});
      expect(store).not.toHaveProperty('workspace_id');
      expect(store.archived_at).toBeNull();

      // GET /v1/memory_stores/:id
      const getRes = await apiCall(cfg, `/v1/memory_stores/${store.id}`, { method: 'GET' });
      expect(getRes.status).toBe(200);
      const got = getRes.json<MemoryStoreResponse>();
      expect(got.id).toBe(store.id);
      expect(got.name).toBe(name);

      const updateRes = await apiCall(cfg, `/v1/memory_stores/${store.id}`, {
        method: 'POST',
        body: JSON.stringify({ description: null }),
      });
      expect(updateRes.status).toBe(200);
      expect(updateRes.json<MemoryStoreResponse>().description).toBe('');

      const listRes = await apiCall(cfg, '/v1/memory_stores', { method: 'GET' });
      expect(listRes.status).toBe(200);
      const listBody = listRes.json<{
        data: MemoryStoreResponse[];
        next_page: string | null;
      }>();
      expect(listBody.data.map((s) => s.id)).toContain(store.id);
      expect(listBody.next_page).toBeNull();
      expect(listBody).not.toHaveProperty('memory_stores');
      expect(listBody).not.toHaveProperty('page_info');

      const memoryContent = Buffer.from('wire memory body', 'utf-8');
      const memorySha = sha256Hex(memoryContent);
      const memoryRes = await apiCall(cfg, `/v1/memory_stores/${store.id}/memories`, {
        method: 'POST',
        body: JSON.stringify({
          path: '/wire.txt',
          content: memoryContent.toString('utf8'),
        }),
      });
      expect(memoryRes.status).toBe(200);
      const memory = memoryRes.json<{
        id: string;
        content_sha256: string;
        memory_store_id: string;
        path: string;
        type: 'memory';
      }>();
      expect(memory.type).toBe('memory');
      expect(memory.memory_store_id).toBe(store.id);
      expect(memory.path).toBe('/wire.txt');
      expect(memory.content_sha256).toBe(memorySha);
      const versionsRes = await apiCall(
        cfg,
        `/v1/memory_stores/${store.id}/memory_versions?memory_id=${memory.id}`,
        { method: 'GET' },
      );
      expect(versionsRes.status).toBe(200);
      const version = versionsRes.json<{
        data: Array<{ id: string; content_sha256: string | null; path: string | null }>;
        next_page: string | null;
      }>().data[0]!;
      expect(version.path).toBe('/wire.txt');
      expect(version.content_sha256).toBe(memorySha);
      const versionGet = await apiCall(
        cfg,
        `/v1/memory_stores/${store.id}/memory_versions/${version.id}`,
        { method: 'GET' },
      );
      expect(versionGet.status).toBe(200);
      expect(versionGet.json<{ id: string; content_sha256: string | null }>().content_sha256).toBe(
        memorySha,
      );

      // DELETE /v1/memory_stores/:id
      const delRes = await apiCall(cfg, `/v1/memory_stores/${store.id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      });
      expect(delRes.status).toBe(200);
      expect(delRes.json<{ id: string; type: string }>()).toEqual({
        id: store.id,
        type: 'memory_store_deleted',
      });
      const get404 = await apiCall(cfg, `/v1/memory_stores/${store.id}`, { method: 'GET' });
      expect(get404.status).toBe(404);
      expect(get404.json<ErrorEnvelope>().error.message).toBeTypeOf('string');
    });
  });

  // --------------------------------------------------------------- scenario 3.5
  describe('scenario 3.5: vault credential wire shape', () => {
    it('creates, lists, fetches, and updates sanitized nested credentials', async () => {
      const vaultRes = await apiCall(cfg, '/v1/vaults', {
        method: 'POST',
        body: JSON.stringify({
          display_name: `wire-credential-vault-${Date.now()}`,
          metadata: { suite: 'wire-conformance' },
        }),
      });
      expect(vaultRes.status).toBe(200);
      const vault = vaultRes.json<VaultResponse>();
      expect(vault).toMatchObject({
        type: 'vault',
        display_name: expect.stringMatching(/^wire-credential-vault-/),
        metadata: { suite: 'wire-conformance' },
      });
      expect(JSON.stringify(vault)).not.toMatch(/target_|secret/);
      const vaultId = vault.id;
      created.vaults.push(vaultId);

      const createStatic = await apiCall(cfg, `/v1/vaults/${vaultId}/credentials`, {
        method: 'POST',
        body: JSON.stringify({
          display_name: 'Wire static bearer',
          metadata: { suite: 'wire-conformance' },
          auth: {
            type: 'static_bearer',
            token: 'wire-static-secret',
            mcp_server_url: 'https://wire-static.example/sse',
          },
        }),
      });
      expect(createStatic.status).toBe(200);
      const staticCredential = createStatic.json<VaultCredentialResponse>();
      expect(staticCredential.id).toMatch(/^vcrd_[A-Za-z0-9_-]+$/);
      expect(staticCredential.type).toBe('vault_credential');
      expect(staticCredential.vault_id).toBe(vaultId);
      expect(staticCredential.auth).toEqual({
        type: 'static_bearer',
        mcp_server_url: 'https://wire-static.example/sse',
      });
      expect(JSON.stringify(staticCredential)).not.toContain('wire-static-secret');

      const createOauth = await apiCall(cfg, `/v1/vaults/${vaultId}/credentials`, {
        method: 'POST',
        body: JSON.stringify({
          display_name: 'Wire OAuth',
          auth: {
            type: 'mcp_oauth',
            access_token: 'wire-access-secret',
            mcp_server_url: 'https://wire-oauth.example/sse',
            refresh: {
              refresh_token: 'wire-refresh-secret',
              token_endpoint: 'https://wire-oauth.example/token',
              client_id: 'wire-client',
              token_endpoint_auth: {
                type: 'client_secret_basic',
                client_secret: 'wire-client-secret',
              },
            },
          },
        }),
      });
      expect(createOauth.status).toBe(200);
      const oauthCredential = createOauth.json<VaultCredentialResponse>();
      expect(oauthCredential.auth).toEqual({
        type: 'mcp_oauth',
        mcp_server_url: 'https://wire-oauth.example/sse',
        expires_at: null,
        refresh: {
          token_endpoint: 'https://wire-oauth.example/token',
          client_id: 'wire-client',
          token_endpoint_auth: { type: 'client_secret_basic' },
          resource: null,
          scope: null,
        },
      });
      const oauthBody = JSON.stringify(oauthCredential);
      expect(oauthBody).not.toContain('wire-access-secret');
      expect(oauthBody).not.toContain('wire-refresh-secret');
      expect(oauthBody).not.toContain('wire-client-secret');

      const listRes = await apiCall(cfg, `/v1/vaults/${vaultId}/credentials`, { method: 'GET' });
      expect(listRes.status).toBe(200);
      const listBody = listRes.json<{
        data: VaultCredentialResponse[];
        next_page: string | null;
      }>();
      expect(listBody.data.map((c) => c.id)).toContain(staticCredential.id);
      expect(listBody.data.map((c) => c.id)).toContain(oauthCredential.id);
      expect(JSON.stringify(listBody)).not.toContain('wire-static-secret');

      const getRes = await apiCall(
        cfg,
        `/v1/vaults/${vaultId}/credentials/${staticCredential.id}`,
        {
          method: 'GET',
        },
      );
      expect(getRes.status).toBe(200);
      expect(getRes.json<VaultCredentialResponse>().id).toBe(staticCredential.id);

      const updateRes = await apiCall(
        cfg,
        `/v1/vaults/${vaultId}/credentials/${staticCredential.id}`,
        {
          method: 'POST',
          body: JSON.stringify({
            display_name: 'Wire static bearer rotated',
            auth: { type: 'static_bearer', token: 'wire-static-rotated-secret' },
          }),
        },
      );
      expect(updateRes.status).toBe(200);
      const updated = updateRes.json<VaultCredentialResponse>();
      expect(updated.display_name).toBe('Wire static bearer rotated');
      expect(JSON.stringify(updated)).not.toContain('wire-static-rotated-secret');
    });
  });

  // ---------------------------------------------------------------- scenario 4
  describe('scenario 4: sessions with resources + SSE handshake', () => {
    it('rejects Create Session without required environment_id', async () => {
      const sessionRes = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({ agent: agentId }),
      });

      expect(sessionRes.status).toBe(400);
      expect(sessionRes.json<ErrorEnvelope>()).toEqual({
        type: 'error',
        error: { type: 'invalid_request_error', message: 'environment_id is required' },
        request_id: expect.any(String),
      });
    });

    it('pins a session to an explicit older agent version', async () => {
      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `wire-session-pin-${Date.now()}`,
          model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
          system: 'session pin v1',
          tools: [],
          mcp_servers: [],
          skills: [],
          metadata: {},
        }),
      });
      expect(agentRes.status).toBe(200);
      const agent = agentRes.json<AgentResponse>();
      created.agents.push(agent.id);
      expect(agent.version).toBe(1);

      const updateRes = await apiCall(cfg, `/v1/agents/${agent.id}`, {
        method: 'POST',
        body: JSON.stringify({ version: agent.version, system: 'session pin v2' }),
      });
      expect(updateRes.status).toBe(200);
      expect(updateRes.json<AgentResponse>().version).toBe(2);

      const sessionRes = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({
          environment_id: environmentId,
          agent: { type: 'agent', id: agent.id, version: 1 },
        }),
      });
      expect(sessionRes.status).toBe(200);
      const session = sessionRes.json<SessionResponse>();
      created.sessions.push(session.id);
      expect(session.type).toBe('session');
      expect(session.agent).toMatchObject({
        id: agent.id,
        type: 'agent',
        name: agent.name,
        version: 1,
        system: 'session pin v1',
      });
      expect(session).not.toHaveProperty('agent_id');
      expect(session).not.toHaveProperty('agent_version');
      expect(session.environment_id).toBe(environmentId);

      const getRes = await apiCall(cfg, `/v1/sessions/${session.id}`, { method: 'GET' });
      expect(getRes.status).toBe(200);
      const got = getRes.json<SessionResponse>();
      expect(got.agent).toEqual(session.agent);
      expect(got.agent.version).toBe(1);
      expect(got.agent.system).toBe('session pin v1');
      expect(got.environment_id).toBe(environmentId);
    });

    it('rejects caller-supplied memory_store mount_path', async () => {
      const memRes = await apiCall(cfg, '/v1/memory_stores', {
        method: 'POST',
        body: JSON.stringify({ name: `wire-derived-memory-path-${Date.now()}` }),
      });
      expect(memRes.status).toBe(200);
      const memId = memRes.json<MemoryStoreResponse>().id;
      created.memStores.push(memId);

      const sessionRes = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({
          environment_id: environmentId,
          agent_id: agentId,
          resources: [
            {
              type: 'memory_store',
              memory_store_id: memId,
              mount_path: '/mnt/custom/',
            },
          ],
        }),
      });

      expect(sessionRes.status).toBe(400);
      expect(sessionRes.json<ErrorEnvelope>()).toMatchObject({
        type: 'error',
        error: {
          type: 'invalid_request_error',
          message: expect.stringMatching(/mount_path/i),
        },
        request_id: expect.any(String),
      });
    });

    it('creates a session bound to file + memory_store + github_repository, attaches another resource, and tails SSE for the heartbeat', async () => {
      // ---- prerequisite resources ---------------------------------------
      // Each entity ID is pushed into the describe-scope cleanup buckets
      // BEFORE any subsequent expect() that could throw, so a mid-test
      // assertion failure still triggers afterEach to delete the orphans.
      // 1. A file — minimal text upload.
      const fileContent = Buffer.from('session-resource-file', 'utf-8');
      const fileForm = new FormData();
      fileForm.set('file', new Blob([fileContent]), 'session-resource.txt');
      const fileRes = await apiCall(cfg, '/v1/files', { method: 'POST', body: fileForm });
      expect(fileRes.status).toBe(200);
      const fileId = fileRes.json<FileResponse>().id;
      created.files.push(fileId);

      // 2. A memory_store.
      const memName = `wire-ses-mem-${Date.now()}`;
      const memRes = await apiCall(cfg, '/v1/memory_stores', {
        method: 'POST',
        body: JSON.stringify({ name: memName }),
      });
      expect(memRes.status).toBe(200);
      const memId = memRes.json<MemoryStoreResponse>().id;
      created.memStores.push(memId);

      // 3. A raw GitHub token. Registry writes it to SecretStore and persists
      // only a resource-owned internal credential reference.
      const repoUrl = `https://github.com/orca-test/wire-${Date.now()}`;
      const githubToken = `ghp_wire_${randomUUID()}`;

      // 4. A dedicated agent (the baseline one might be deleted by other
      // tests in interactive runs — fresh-create for isolation).
      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `wire-ses-agent-${Date.now()}`,
          model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
          tools: [],
          mcp_servers: [],
          skills: [],
          metadata: {},
        }),
      });
      expect(agentRes.status).toBe(200);
      const sesAgentId = agentRes.json<AgentResponse>().id;
      created.agents.push(sesAgentId);

      // ---- POST /v1/sessions with all three resource families ----------
      const sessionRes = await apiCall(cfg, '/v1/sessions', {
        method: 'POST',
        body: JSON.stringify({
          environment_id: environmentId,
          // The legacy alias remains accepted without requiring `orca-beta`;
          // the response below must still use the canonical nested agent.
          agent_id: sesAgentId,
          resources: [
            { type: 'file', file_id: fileId, mount_path: '/mnt/file/wire.txt' },
            { type: 'memory_store', memory_store_id: memId },
            {
              type: 'github_repository',
              url: repoUrl,
              authorization_token: githubToken,
              mount_path: '/workspace/wire/',
            },
          ],
        }),
      });
      expect(sessionRes.status).toBe(200);
      const session = sessionRes.json<SessionResponse>();
      created.sessions.push(session.id);
      expect(session.id).toMatch(/^ses_[A-Za-z0-9_-]+$/);
      expect(session.type).toBe('session');
      expect(session.agent).toMatchObject({
        id: sesAgentId,
        type: 'agent',
        version: expect.any(Number),
        model: {
          id: 'claude-3-5-sonnet-20240620',
          speed: 'standard',
        },
        tools: [],
        mcp_servers: [],
        skills: [],
        multiagent: null,
      });
      expect(session.agent.version).toBeGreaterThan(0);
      expect(session).not.toHaveProperty('agent_id');
      expect(session).not.toHaveProperty('agent_version');
      expect(session.environment_id).toBe(environmentId);
      expect(session.title).toBeNull();
      expect(session.metadata).toEqual({});
      expect(session.status).toBe('idle');
      expect(session.timing).toMatchObject({
        started_at: null,
        last_active_at: null,
        active_seconds: 0,
      });
      expect(session).not.toHaveProperty('started_at');
      expect(session).not.toHaveProperty('last_active_at');
      expect(session.stats).toEqual({
        active_seconds: session.timing.active_seconds,
        duration_seconds: session.timing.duration_seconds,
      });
      expect(session.deployment_id).toBeNull();
      expect(session.outcome_evaluations).toEqual([]);
      expect(session.usage).toEqual({
        cache_creation: {
          ephemeral_1h_input_tokens: 0,
          ephemeral_5m_input_tokens: 0,
        },
        cache_read_input_tokens: 0,
        input_tokens: 0,
        output_tokens: 0,
      });
      expect(session.resources).toHaveLength(3);

      const fileResource = session.resources.find((r) => r.type === 'file');
      expect(fileResource).toBeDefined();
      expect(fileResource!.file_id).toBe(fileId);
      expect(fileResource!.mount_path).toBe('/mnt/file/wire.txt');
      expect(Object.keys(fileResource!).sort()).toEqual([
        'created_at',
        'file_id',
        'id',
        'mount_path',
        'type',
        'updated_at',
      ]);

      const memResource = session.resources.find((r) => r.type === 'memory_store');
      expect(memResource).toBeDefined();
      expect(memResource!.memory_store_id).toBe(memId);
      expect(memResource!.mount_path).toBe(`/mnt/memory/${memName}/`);
      expect(memResource!.access).toBe('read_write');
      expect(memResource).not.toHaveProperty('id');

      const repoResource = session.resources.find((r) => r.type === 'github_repository');
      expect(repoResource).toBeDefined();
      expect(repoResource!.url).toBe(repoUrl);
      expect(repoResource!.mount_path).toBe('/workspace/wire/');
      expect(repoResource).not.toHaveProperty('checkout');
      expect(repoResource).not.toHaveProperty('authorization_token');
      expect(repoResource).not.toHaveProperty('repo_ref');
      expect(JSON.stringify(session)).not.toContain(githubToken);

      // Claude-compatible resource update rotates the write-only token.
      const rotatedToken = `ghs_wire_${randomUUID()}`;
      const rotateRes = await apiCall(
        cfg,
        `/v1/sessions/${session.id}/resources/${repoResource!.id}`,
        {
          method: 'POST',
          body: JSON.stringify({ authorization_token: rotatedToken }),
        },
      );
      expect(rotateRes.status).toBe(200);
      const rotated = rotateRes.json<GithubRepositorySessionResource>();
      expect(rotated.url).toBe(repoUrl);
      expect(rotated).not.toHaveProperty('authorization_token');
      expect(rotated).not.toHaveProperty('repo_ref');
      expect(rotateRes.text).not.toContain(rotatedToken);

      // ---- GET /v1/sessions/:id round-trip ------------------------------
      const getRes = await apiCall(cfg, `/v1/sessions/${session.id}`, { method: 'GET' });
      expect(getRes.status).toBe(200);
      const got = getRes.json<SessionResponse>();
      expect(got.id).toBe(session.id);
      expect(got.agent).toEqual(session.agent);
      expect(got.resources).toHaveLength(3);
      expect(got.timing.active_seconds).toBeGreaterThanOrEqual(0);
      expect(got.timing.duration_seconds).toBeGreaterThanOrEqual(session.timing.duration_seconds);
      expect(got).not.toHaveProperty('last_active_at');
      expect(got.stats).toEqual({
        active_seconds: got.timing.active_seconds,
        duration_seconds: got.timing.duration_seconds,
      });
      expect(got.usage.input_tokens).toBe(0);
      expect(got.usage.output_tokens).toBe(0);

      const sessionList = await apiCall(cfg, '/v1/sessions', { method: 'GET' });
      expect(sessionList.status).toBe(200);
      const sessionListBody = sessionList.json<{
        data: SessionResponse[];
        next_page: string | null;
        prev_page: string | null;
      }>();
      const listedSession = sessionListBody.data.find((item) => item.id === session.id);
      expect(listedSession).toBeDefined();
      expect(listedSession!.stats).toEqual({
        active_seconds: listedSession!.timing.active_seconds,
        duration_seconds: listedSession!.timing.duration_seconds,
      });
      expect(sessionListBody.next_page).toBeNull();
      expect(sessionListBody.prev_page).toBeNull();
      expect(sessionListBody).not.toHaveProperty('sessions');
      expect(sessionListBody).not.toHaveProperty('page_info');

      // ---- POST /v1/sessions/:id/resources — attach another file ---------
      const file2Form = new FormData();
      file2Form.set('file', new Blob([Buffer.from('attached', 'utf-8')]), 'attach.txt');
      const file2Res = await apiCall(cfg, '/v1/files', { method: 'POST', body: file2Form });
      expect(file2Res.status).toBe(200);
      const file2Id = file2Res.json<FileResponse>().id;
      created.files.push(file2Id);

      const attachRes = await apiCall(cfg, `/v1/sessions/${session.id}/resources`, {
        method: 'POST',
        body: JSON.stringify({
          type: 'file',
          file_id: file2Id,
          mount_path: '/mnt/file/attached.txt',
        }),
      });
      expect(attachRes.status).toBe(200);
      const attached = attachRes.json<FileSessionResource>();
      expect(attached.id).toMatch(/^sesrsc_[A-Za-z0-9_-]+$/);
      expect(attached.type).toBe('file');
      expect(attached.file_id).toBe(file2Id);
      expect(attached.mount_path).toBe('/mnt/file/attached.txt');
      expect(Object.keys(attached).sort()).toEqual([
        'created_at',
        'file_id',
        'id',
        'mount_path',
        'type',
        'updated_at',
      ]);

      // ---- GET/POST /v1/sessions/:id/resources/:resource_id -------------
      const listResourcesRes = await apiCall(cfg, `/v1/sessions/${session.id}/resources`, {
        method: 'GET',
      });
      expect(listResourcesRes.status).toBe(200);
      const listResourcesBody = listResourcesRes.json<{
        data: SessionResource[];
        next_page: string | null;
      }>();
      expect(
        listResourcesBody.data
          .filter((resource) => 'id' in resource)
          .map((resource) => resource.id),
      ).toContain(attached.id);
      expect(listResourcesBody.next_page).toBeNull();

      const getResourceRes = await apiCall(
        cfg,
        `/v1/sessions/${session.id}/resources/${attached.id}`,
        { method: 'GET' },
      );
      expect(getResourceRes.status).toBe(200);
      expect(getResourceRes.json<FileSessionResource>().file_id).toBe(file2Id);

      // Legacy resource update fields remain accepted without `orca-beta`.
      // The response remains the canonical six-field file resource.
      const updateResourceRes = await apiCall(
        cfg,
        `/v1/sessions/${session.id}/resources/${attached.id}`,
        {
          method: 'POST',
          body: JSON.stringify({
            mount_path: '/mnt/file/attached-updated.txt',
            access: 'read_write',
            instructions: 'wire updated resource',
            mount_strategy: 'tarball_prefetch',
          }),
        },
      );
      expect(updateResourceRes.status).toBe(200);
      const updatedResource = updateResourceRes.json<FileSessionResource>();
      expect(updatedResource.mount_path).toBe('/mnt/file/attached-updated.txt');
      expect(Object.keys(updatedResource).sort()).toEqual([
        'created_at',
        'file_id',
        'id',
        'mount_path',
        'type',
        'updated_at',
      ]);

      // ---- prime the transcript so the SSE tail has something to deliver
      // The Kafka topic for a session is created lazily on the first append;
      // opening a tail before any append races with topic creation and the
      // consumer can fail with "This server does not host this topic-partition"
      // before the heartbeat interval even fires once. Append a minimal event
      // first — this materializes the topic and gives the SSE loop a real
      // event to flush so we test BOTH the handshake AND a real frame.
      const eventAppend = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
        method: 'POST',
        body: JSON.stringify({
          events: [
            {
              type: 'user.message',
              content: [{ type: 'text', text: 'wire-conformance.scenario4' }],
            },
          ],
          // This extension is accepted without changing the canonical response.
          request_id: `wire-${Date.now()}`,
        }),
      });
      expect(eventAppend.status).toBe(200);
      const appended = eventAppend.json<{
        data: Array<{ id: string; type: string; processed_at: string | null }>;
      }>();
      expect(appended.data).toHaveLength(1);
      expect(appended.data[0]?.type).toBe('user.message');
      expect(appended.data[0]?.id).toMatch(/^evt_/);
      expect(appended).not.toHaveProperty('events');
      expect(appended.data[0]).not.toHaveProperty('seq');

      const eventsRes = await apiCall(cfg, `/v1/sessions/${session.id}/events`, {
        method: 'GET',
      });
      expect(eventsRes.status).toBe(200);
      const eventsBody = eventsRes.json<{
        data: Array<{ id: string; type: string; processed_at: string | null }>;
        next_page: string | null;
      }>();
      expect(eventsBody.data.map((event) => event.id)).toContain(appended.data[0]!.id);
      expect(eventsBody.next_page).toBeNull();
      expect(eventsBody).not.toHaveProperty('events');
      expect(eventsBody).not.toHaveProperty('has_more');

      // ---- SSE stream — read the headers + first frame ------------------
      // Layer A asserts the *handshake* (HTTP 200, text/event-stream
      // content-type, no-cache control) AND at least one frame. The frame can
      // be the priming event we just appended OR a `:heartbeat` comment —
      // either proves the stream is live.
      const sseAc = new AbortController();
      const sseTimer = setTimeout(() => sseAc.abort(), 30_000);
      const sseRes = await fetch(`${cfg.baseURL}/v1/sessions/${session.id}/events/stream`, {
        method: 'GET',
        headers: { 'x-api-key': cfg.apiKey, accept: 'text/event-stream' },
        signal: sseAc.signal,
      });
      try {
        expect(sseRes.status).toBe(200);
        expect(sseRes.headers.get('content-type')).toMatch(/text\/event-stream/);
        expect(sseRes.headers.get('cache-control')).toMatch(/no-cache/);
        expect(sseRes.body).toBeDefined();

        const reader = sseRes.body!.getReader();
        const decoder = new TextDecoder();
        let buffered = '';
        const deadline = Date.now() + 25_000;
        let sawFrame = false;
        while (Date.now() < deadline && !sawFrame) {
          const { value, done } = await reader.read();
          if (done) break;
          buffered += decoder.decode(value, { stream: true });
          // Heartbeat lines start with `:` (SSE comment); event frames have
          // `id:` / `event:` / `data:` lines. Either is acceptable.
          if (buffered.includes(':heartbeat') || /\bevent:|\bid:|\bdata:/.test(buffered)) {
            sawFrame = true;
          }
        }
        expect(
          sawFrame,
          `No SSE frame received within deadline. Buffered: ${JSON.stringify(buffered.slice(0, 200))}`,
        ).toBe(true);
        // Cancel the reader (server-side stream cleanup happens on socket close).
        await reader.cancel().catch(() => {});
      } finally {
        clearTimeout(sseTimer);
        sseAc.abort();
      }

      const deleteSession = await apiCall(cfg, `/v1/sessions/${session.id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      });
      expect(deleteSession.status).toBe(200);
      expect(deleteSession.json<{ id: string; type: string }>()).toEqual({
        id: session.id,
        type: 'session_deleted',
      });

      // No inline cleanup chain — afterEach drains `created.*` in
      // reverse-dependency order (sessions, agents, files, memStores,
      // vaults) using `.catch(() => {})` so a failed delete here doesn't
      // mask the real test failure.
    }, 120_000);
  });

  // ---------------------------------------------------------------- scenario 5
  describe('scenario 5: skills lifecycle + agent embedding', () => {
    it('creates distinct skills from zip archives with the same directory', async () => {
      const directory = `wire-zipped-skill-${randomUUID()}`;
      const skillMarkdown = [
        '---',
        `name: ${directory}`,
        'description: uploaded as a zip archive',
        '---',
        '',
        'Zipped skill instructions',
      ].join('\n');
      const archive = zipSync({
        [`${directory}/SKILL.md`]: strToU8(skillMarkdown),
        [`${directory}/assets/example.txt`]: strToU8('zipped asset'),
      });
      const createSkill = async () => {
        const form = new FormData();
        form.set('files[]', new Blob([archive], { type: 'application/zip' }), `${directory}.zip`);
        return apiCall(cfg, '/v1/skills', {
          method: 'POST',
          body: form,
        });
      };

      const firstCreate = await createSkill();
      expect(firstCreate.status).toBe(200);
      const firstSkill = firstCreate.json<SkillResponse>();
      created.skills.push(firstSkill.id);
      expect(firstSkill).toMatchObject({
        display_title: null,
        source: 'custom',
        type: 'skill',
        latest_version: expect.stringMatching(/^\d+$/),
      });

      const secondCreate = await createSkill();
      expect(secondCreate.status).toBe(200);
      const secondSkill = secondCreate.json<SkillResponse>();
      created.skills.push(secondSkill.id);
      expect(secondSkill.id).not.toBe(firstSkill.id);
      expect(secondSkill).toMatchObject({
        source: 'custom',
        type: 'skill',
        latest_version: expect.stringMatching(/^\d+$/),
      });

      const contentRes = await fetch(
        `${cfg.baseURL}/v1/skills/${firstSkill.id}/versions/${firstSkill.latest_version}/content`,
        { headers: authHeaders(cfg) },
      );
      expect(contentRes.status).toBe(200);
      expect(contentRes.headers.get('content-type')).toMatch(/application\/zip/);
      const content = unzipSync(new Uint8Array(await contentRes.arrayBuffer()));
      expect(strFromU8(content[`${directory}/SKILL.md`]!)).toBe(skillMarkdown);
      expect(strFromU8(content[`${directory}/assets/example.txt`]!)).toBe('zipped asset');
    });

    it('creates a skill from multiple uploaded files', async () => {
      const directory = `wire-multi-file-skill-${randomUUID()}`;
      const skillMarkdown = [
        '---',
        `name: ${directory}`,
        'description: uploaded as multiple files',
        '---',
        '',
        'Multi-file skill instructions',
      ].join('\n');
      const form = new FormData();
      form.append(
        'files[]',
        new Blob([skillMarkdown], { type: 'text/markdown' }),
        `${directory}/SKILL.md`,
      );
      form.append(
        'files[]',
        new Blob(['{"enabled":true}'], { type: 'application/json' }),
        `${directory}/assets/config.json`,
      );

      const createRes = await apiCall(cfg, '/v1/skills', {
        method: 'POST',
        body: form,
      });
      expect(createRes.status).toBe(200);
      const skill = createRes.json<SkillResponse>();
      created.skills.push(skill.id);
      expect(skill).toMatchObject({
        source: 'custom',
        type: 'skill',
        latest_version: expect.stringMatching(/^\d+$/),
      });

      const contentRes = await fetch(
        `${cfg.baseURL}/v1/skills/${skill.id}/versions/${skill.latest_version}/content`,
        { headers: authHeaders(cfg) },
      );
      expect(contentRes.status).toBe(200);
      const content = unzipSync(new Uint8Array(await contentRes.arrayBuffer()));
      expect(strFromU8(content[`${directory}/SKILL.md`]!)).toBe(skillMarkdown);
      expect(strFromU8(content[`${directory}/assets/config.json`]!)).toBe('{"enabled":true}');
    });

    it('rejects a duplicate custom skill display_title with a Claude conflict error', async () => {
      const displayTitle = `Wire duplicate title ${randomUUID()}`;
      const createSkill = async (directory: string) => {
        const form = new FormData();
        form.set(
          'files[]',
          new Blob(
            [
              [
                '---',
                `name: ${directory}`,
                'description: display title uniqueness',
                '---',
                '',
                'Titled skill instructions',
              ].join('\n'),
            ],
            { type: 'text/markdown' },
          ),
          `${directory}/SKILL.md`,
        );
        form.set('display_title', displayTitle);
        return apiCall(cfg, '/v1/skills', {
          method: 'POST',
          body: form,
        });
      };

      const firstCreate = await createSkill(`wire-title-first-${randomUUID()}`);
      expect(firstCreate.status).toBe(200);
      const firstSkill = firstCreate.json<SkillResponse>();
      created.skills.push(firstSkill.id);

      const secondCreate = await createSkill(`wire-title-second-${randomUUID()}`);
      expect(secondCreate.status).toBe(409);
      expect(secondCreate.json<ErrorEnvelope>()).toMatchObject({
        type: 'error',
        error: {
          type: 'conflict_error',
          message: 'skill display_title is already in use',
        },
        request_id: expect.any(String),
      });
    });

    it('round-trips skills CRUD and embeds a concrete skill version in an agent', async () => {
      const unique = Date.now();
      const directory = `wire-skill-${unique}`;
      const displayTitle = `Wire conformance skill ${randomUUID()}`;
      const skillForm = new FormData();
      skillForm.set(
        'files[]',
        new Blob(
          [
            [
              '---',
              `name: ${directory}`,
              'description: wire skill lifecycle',
              '---',
              '',
              'Skill v1 instructions',
            ].join('\n'),
          ],
          { type: 'text/markdown' },
        ),
        `${directory}/SKILL.md`,
      );
      skillForm.set('display_title', displayTitle);
      const createRes = await apiCall(cfg, '/v1/skills', {
        method: 'POST',
        body: skillForm,
      });
      expect(createRes.status).toBe(200);
      const skill = createRes.json<SkillResponse>();
      created.skills.push(skill.id);
      expect(skill.id).toMatch(/^skill_[A-Za-z0-9_-]+$/);
      expect(skill.display_title).toBe(displayTitle);
      expect(skill.latest_version).toMatch(/^\d+$/);
      expect(skill.source).toBe('custom');
      expect(skill.type).toBe('skill');
      expect(Object.keys(skill).sort()).toEqual([
        'created_at',
        'display_title',
        'id',
        'latest_version',
        'source',
        'type',
        'updated_at',
      ]);

      const getRes = await apiCall(cfg, `/v1/skills/${skill.id}`, { method: 'GET' });
      expect(getRes.status).toBe(200);
      expect(getRes.json<SkillResponse>()).toEqual(skill);

      const listRes = await apiCall(cfg, '/v1/skills', { method: 'GET' });
      expect(listRes.status).toBe(200);
      const skillsListBody = listRes.json<{
        data: SkillResponse[];
        has_more: boolean;
        next_page: string | null;
      }>();
      const listed = skillsListBody.data;
      expect(listed.some((item) => item.id === skill.id)).toBe(true);
      expect(skillsListBody.has_more).toBe(false);
      expect(skillsListBody.next_page).toBeNull();
      expect(skillsListBody).not.toHaveProperty('skills');
      expect(skillsListBody).not.toHaveProperty('page_info');

      const versionForm = new FormData();
      versionForm.set(
        'files[]',
        new Blob(
          [
            [
              '---',
              `name: ${directory}`,
              'description: wire skill lifecycle v2',
              '---',
              '',
              'Skill v2 instructions',
            ].join('\n'),
          ],
          { type: 'text/markdown' },
        ),
        `${directory}/SKILL.md`,
      );
      const versionRes = await apiCall(cfg, `/v1/skills/${skill.id}/versions`, {
        method: 'POST',
        body: versionForm,
      });
      expect(versionRes.status).toBe(200);
      const version = versionRes.json<SkillVersionResponse>();
      expect(version.id).toMatch(/^skillver_[A-Za-z0-9_-]+$/);
      expect(version.type).toBe('skill_version');
      expect(version.skill_id).toBe(skill.id);
      expect(version.directory).toBe(directory);
      expect(version.description).toBe('wire skill lifecycle v2');
      expect(version.version).toMatch(/^\d+$/);
      expect(version.version).not.toBe(skill.latest_version);

      const updatedRes = await apiCall(cfg, `/v1/skills/${skill.id}`, { method: 'GET' });
      expect(updatedRes.status).toBe(200);
      const updated = updatedRes.json<SkillResponse>();
      expect(updated.latest_version).toBe(version.version);
      expect(updated.display_title).toBe(skill.display_title);

      const versionsRes = await apiCall(cfg, `/v1/skills/${skill.id}/versions`, {
        method: 'GET',
      });
      expect(versionsRes.status).toBe(200);
      const versions = versionsRes.json<{
        data: SkillVersionResponse[];
        has_more: boolean;
        next_page: string | null;
      }>();
      expect(versions.data.map((item) => item.version)).toContain(version.version);
      expect(versions.has_more).toBe(false);
      expect(versions.next_page).toBeNull();

      const agentRes = await apiCall(cfg, '/v1/agents', {
        method: 'POST',
        body: JSON.stringify({
          name: `wire-skill-agent-${unique}`,
          model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
          system: 'base system',
          tools: [{ type: 'agent_toolset_20260401' }],
          mcp_servers: [],
          skills: [{ type: 'custom', skill_id: skill.id, version: version.version }],
          metadata: { suite: 'wire-conformance' },
        }),
      });
      expect(agentRes.status).toBe(200);
      const agent = agentRes.json<AgentResponse>();
      created.agents.push(agent.id);
      expect(agent.skills).toEqual([
        { type: 'custom', skill_id: skill.id, version: version.version },
      ]);

      const getAgentRes = await apiCall(cfg, `/v1/agents/${agent.id}`, { method: 'GET' });
      expect(getAgentRes.status).toBe(200);
      expect(getAgentRes.json<AgentResponse>().skills).toEqual([
        { type: 'custom', skill_id: skill.id, version: version.version },
      ]);

      const deleteWithVersionsRes = await apiCall(cfg, `/v1/skills/${skill.id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      });
      expect(deleteWithVersionsRes.status).toBe(400);
      expect(deleteWithVersionsRes.json<ErrorEnvelope>()).toMatchObject({
        type: 'error',
        error: {
          type: 'invalid_request_error',
          message: expect.stringMatching(/versions must be deleted/i),
        },
        request_id: expect.any(String),
      });

      const deleteLatestVersion = await apiCall(
        cfg,
        `/v1/skills/${skill.id}/versions/${version.version}`,
        { method: 'DELETE', body: JSON.stringify({}) },
      );
      expect(deleteLatestVersion.status).toBe(200);
      expect(deleteLatestVersion.json()).toEqual({
        id: version.version,
        type: 'skill_version_deleted',
      });
      const afterLatestDelete = await apiCall(cfg, `/v1/skills/${skill.id}`, { method: 'GET' });
      expect(afterLatestDelete.status).toBe(200);
      expect(afterLatestDelete.json<SkillResponse>().latest_version).toBe(skill.latest_version);

      const deleteFinalVersion = await apiCall(
        cfg,
        `/v1/skills/${skill.id}/versions/${skill.latest_version}`,
        { method: 'DELETE', body: JSON.stringify({}) },
      );
      expect(deleteFinalVersion.status).toBe(200);
      const withoutVersions = await apiCall(cfg, `/v1/skills/${skill.id}`, { method: 'GET' });
      expect(withoutVersions.status).toBe(200);
      expect(
        withoutVersions.json<
          Omit<SkillResponse, 'latest_version'> & { latest_version: string | null }
        >(),
      ).toMatchObject({ id: skill.id, latest_version: null });

      const deleteRes = await apiCall(cfg, `/v1/skills/${skill.id}`, {
        method: 'DELETE',
        body: JSON.stringify({}),
      });
      expect(deleteRes.status).toBe(200);
      expect(deleteRes.json<{ id: string; type: string }>()).toEqual({
        id: skill.id,
        type: 'skill_deleted',
      });
      const getDeleted = await apiCall(cfg, `/v1/skills/${skill.id}`, { method: 'GET' });
      expect(getDeleted.status).toBe(404);
    });
  });

  // ---------------------------------------------------------------- scenario 6
  describe('scenario 6: error envelopes (401 + 400)', () => {
    it('returns 401 with the error envelope when x-api-key is missing', async () => {
      const res = await fetch(`${cfg.baseURL}/v1/agents`, {
        method: 'GET',
        headers: { accept: 'application/json' },
      });
      expect(res.status).toBe(401);
      const body = (await res.json()) as ErrorEnvelope;
      expect(body).toMatchObject({
        type: 'error',
        error: { type: 'authentication_error', message: expect.stringMatching(/unauth/i) },
        request_id: expect.any(String),
      });
    });

    it('returns 400 with an error envelope when agent model is missing', async () => {
      const res = await fetch(`${cfg.baseURL}/v1/agents`, {
        method: 'POST',
        headers: {
          'x-api-key': cfg.apiKey,
          'content-type': 'application/json',
        },
        body: JSON.stringify({ name: `missing-model-${Date.now()}` }),
      });
      expect(res.status).toBe(400);
      const body = (await res.json()) as ErrorEnvelope;
      expect(body).toMatchObject({
        type: 'error',
        error: { type: 'invalid_request_error', message: expect.stringMatching(/model/i) },
        request_id: expect.any(String),
      });
    });

    it('returns 401 with an error envelope when x-api-key has the wrong prefix', async () => {
      const res = await fetch(`${cfg.baseURL}/v1/agents`, {
        method: 'GET',
        headers: {
          accept: 'application/json',
          // The api-key middleware short-circuits anything without `orca_` prefix
          // — see `services/registry-service-ts/src/auth/api-key.ts:31`.
          'x-api-key': 'sk-not-an-orca-key',
        },
      });
      expect(res.status).toBe(401);
      const body = (await res.json()) as ErrorEnvelope;
      expect(body).toMatchObject({
        type: 'error',
        error: { type: 'authentication_error', message: expect.any(String) },
        request_id: expect.any(String),
      });
    });
  });
});

/**
 * SHA-256 hex digest used to verify canonical memory content hashes.
 */
function sha256Hex(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}
