// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import fs from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { jwtVerify, importSPKI } from 'jose';
import { and, eq } from 'drizzle-orm';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import {
  TEST_ORGANIZATION_ID,
  uniqueWorkspace,
  createTestApiKey,
  createTestAgent,
  createTestEnvironment,
  createTestSession,
} from './fixtures.js';
import { buildCombinedTestApp } from '../../src/server.js';
import {
  agentVersions,
  agents,
  agentObservabilityWorkspaceArchiveRevocations,
  guardrails,
  sessions,
  workspaces,
} from '../../src/persistence/postgres/schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PUBLIC_PEM = fs.readFileSync(
  resolve(__dirname, '../fixtures/session-jwt-public.pem'),
  'utf8',
);

async function createAgentWithModel(
  baseURL: string,
  apiKey: string,
  model: { provider: string; id: string },
  multiagent?: { type: 'coordinator'; agents: string[] },
): Promise<string> {
  const res = await fetch(`${baseURL}/v1/agents`, {
    method: 'POST',
    headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
    body: JSON.stringify({
      name: `jwt-model-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      model,
      tools: [],
      mcp_servers: [],
      skills: [],
      // These cases exercise JWT model scoping, including legacy cross-provider
      // rosters. Give OpenAI models their native CLI harness explicitly.
      metadata: model.provider === 'openai' ? { harness: 'codex' } : {},
      ...(multiagent ? { multiagent } : {}),
    }),
  });
  if (res.status !== 200) throw new Error(`agent create failed: ${res.status} ${await res.text()}`);
  return ((await res.json()) as { id: string }).id;
}

describe('session JWT mint (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;

  beforeAll(async () => {
    const { db } = await getTestDb();
    app = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      sessionJwtLlmPolicy: {
        routes: ['managed-agent-llm-openai'],
        models: ['claude-*', 'gpt-4o-mini'],
      },
      fileStore: buildStubFileStore(),
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
  }, 30000);

  afterAll(async () => {
    await app.close();
    await closeTestDb();
  });

  it('mints a session JWT verifiable with the public key', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('mintjwt');
    const apiKey = await createTestApiKey(db, ws);
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    await db
      .update(sessions)
      .set({
        mcpServers: [{ name: 'github', url: 'https://api.githubcopilot.com/mcp/' }],
      })
      .where(andSession(ws, sessionId));

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          org_id: 'org_spoofed',
          mcp_server_names: ['github', 'caller-injected'],
          llm_routes: ['caller-route'],
          llm_models: ['caller-model'],
        }),
      },
    );
    expect(res.status).toBe(200);
    const got = (await res.json()) as { token: string; expires_at: number };
    expect(got.token.split('.')).toHaveLength(3);

    const pub = await importSPKI(PUBLIC_PEM, 'RS256');
    const { payload } = await jwtVerify(got.token, pub, {
      issuer: 'orca-registry',
      audience: 'ai-gateway',
    });
    expect(payload['session_id']).toBe(sessionId);
    expect(payload['org_id']).toBe(TEST_ORGANIZATION_ID);
    expect(payload['org_id']).not.toBe('org_spoofed');
    expect(payload['workspace_id']).toBe(ws);
    expect(payload['mcp_server_names']).toEqual(['github']);
    expect(payload['vault_ids']).toEqual([]);
    expect(payload['credential_ids']).toEqual([]);
    expect(payload['llm_routes']).toEqual(['managed-agent-llm-openai']);
    expect(payload['llm_models']).toEqual(['claude-3-5-sonnet-20240620']);
    expect(payload['agent_id']).toBe(agentId);
    expect(payload['guardrail_ids']).toEqual([]);
    expect(payload['runtime_config_revision']).toBe('1');
    expect(payload.sub).toBe(sessionId);
  }, 30000);

  it('keeps Codex Gateway tokens valid throughout a turn without trusting caller TTLs', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('codex_jwt_window');
    const apiKey = await createTestApiKey(db, ws);
    const response = await fetch(`${baseURL}/v1/agents`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        name: 'Codex JWT window',
        model: { provider: 'openai', id: 'gpt-5.4' },
        metadata: { harness: 'codex_sdk' },
        tools: [],
      }),
    });
    expect(response.status).toBe(200);
    const agent = (await response.json()) as { id: string };
    const codexSession = await createTestSession(baseURL, apiKey, agent.id);
    const claudeSession = await createTestSession(
      baseURL,
      apiKey,
      await createTestAgent(baseURL, apiKey),
    );
    const pub = await importSPKI(PUBLIC_PEM, 'RS256');
    for (const [sessionId, audience, lifetime] of [
      [codexSession, 'ai-gateway', 660],
      [codexSession, 'git-creds', 300],
      [claudeSession, 'ai-gateway', 300],
    ] as const) {
      const minted = await fetch(
        `${baseURL}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            audience,
            ttl_secs: 86400,
            ttl_seconds: 86400,
            harness: 'codex_sdk',
          }),
        },
      );
      expect(minted.status).toBe(200);
      const { token } = (await minted.json()) as { token: string };
      const { payload } = await jwtVerify(token, pub, { audience });
      expect(payload.exp! - payload.iat!).toBe(lifetime);
      if (lifetime === 660) {
        await expect(
          jwtVerify(token, pub, {
            audience,
            currentDate: new Date((payload.iat! + 601) * 1000),
          }),
        ).resolves.toBeDefined();
      }
    }
  });

  it('pins Pi native route and model grants to the effective persisted provider', async () => {
    const { db } = await getTestDb();
    const piApp = buildCombinedTestApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      sessionJwtLlmPolicy: {
        routes: ['llm-responses', 'llm-messages', 'llm-pi-anthropic-anthropic-messages'],
        models: ['claude-*', 'gpt-*', 'deepseek-*'],
      },
      fileStore: buildStubFileStore(),
    });
    await piApp.listen({ host: '127.0.0.1', port: 0 });
    try {
      const url = `http://127.0.0.1:${(piApp.server.address() as AddressInfo).port}`;
      const ws = uniqueWorkspace('pi_native_scope');
      const apiKey = await createTestApiKey(db, ws);
      const created = await fetch(`${url}/v1/agents`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        body: JSON.stringify({
          name: 'Pi scoped route',
          model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
          metadata: { harness: 'pi_sdk' },
          tools: [],
        }),
      });
      expect(created.status).toBe(200);
      const sessionId = await createTestSession(
        url,
        apiKey,
        ((await created.json()) as { id: string }).id,
      );
      const mint = () =>
        fetch(`${url}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            llm_routes: ['llm-responses'],
            model: { provider: 'openai', id: 'gpt-5.4' },
          }),
        });
      const response = await mint();
      expect(response.status).toBe(200);
      const pub = await importSPKI(PUBLIC_PEM, 'RS256');
      const { payload } = await jwtVerify(
        ((await response.json()) as { token: string }).token,
        pub,
        { audience: 'ai-gateway' },
      );
      expect(payload['llm_routes']).toEqual(['llm-pi-anthropic-anthropic-messages']);
      expect(payload['llm_models']).toEqual(['claude-sonnet-4-6']);
      expect(payload.exp! - payload.iat!).toBe(660);
      await db
        .update(sessions)
        .set({ agentOverrides: { model: { provider: 'deepseek', id: 'deepseek-flash' } } })
        .where(and(eq(sessions.workspaceId, ws), eq(sessions.id, sessionId)));
      const denied = await mint();
      expect(denied.status).toBe(403);
      expect(await denied.text()).toContain('llm-pi-deepseek-openai-completions');
    } finally {
      await piApp.close();
    }
  });

  it('returns a Session-scoped Registry guardrail bundle and rejects mismatched scope', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('effective_guardrail');
    const apiKey = await createTestApiKey(db, ws);
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const guardrailId = `grd_effective_${Date.now()}`;
    await db.insert(guardrails).values({
      id: guardrailId,
      organizationId: TEST_ORGANIZATION_ID,
      workspaceId: ws,
      name: 'workspace-only policy',
      phases: ['tool_call'],
      scope: 'workspace',
      rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
    });
    const query = new URLSearchParams({
      principal_id: sessionId,
      request_id: 'req_test',
      traffic_kind: 'llm',
      runtime_config_revision: '1',
      'scope.org_id': TEST_ORGANIZATION_ID,
      'scope.workspace_id': ws,
      'scope.session_id': sessionId,
      'scope.agent_id': agentId,
    });
    const endpoint = `${baseURL}/internal/v1/guardrails/effective`;
    const result = await fetch(`${endpoint}?${query}`);
    expect(result.status).toBe(200);
    const bundle = (await result.json()) as Record<string, unknown>;
    expect(bundle['schema']).toBe('1');
    expect(bundle['scope']).toEqual({
      org_id: TEST_ORGANIZATION_ID,
      workspace_id: ws,
      session_id: sessionId,
    });
    expect(bundle['runtime_config_revision']).toBe('1');
    expect(bundle['guardrails']).toEqual([
      expect.objectContaining({ id: guardrailId, tier: 'workspace' }),
    ]);

    const otherWorkspace = uniqueWorkspace('effective_guardrail_other');
    const otherApiKey = await createTestApiKey(db, otherWorkspace);
    const otherAgent = await createTestAgent(baseURL, otherApiKey);
    const otherSession = await createTestSession(baseURL, otherApiKey, otherAgent);
    query.set('principal_id', otherSession);
    query.set('scope.workspace_id', otherWorkspace);
    query.set('scope.session_id', otherSession);
    query.set('scope.agent_id', otherAgent);
    const otherResponse = await fetch(`${endpoint}?${query}`);
    expect(otherResponse.status).toBe(200);
    expect(((await otherResponse.json()) as { guardrails: unknown[] }).guardrails).toEqual([]);

    query.set('principal_id', sessionId);
    query.set('scope.workspace_id', ws);
    query.set('scope.session_id', sessionId);
    query.set('scope.agent_id', agentId);

    await db.update(guardrails).set({ enabled: false }).where(eq(guardrails.id, guardrailId));
    const refreshed = await fetch(`${endpoint}?${query}`);
    expect(refreshed.status).toBe(200);
    expect(((await refreshed.json()) as { guardrails: unknown[] }).guardrails).toEqual([]);

    query.set('scope.org_id', 'org_other');
    expect((await fetch(`${endpoint}?${query}`)).status).toBe(404);
    query.set('scope.org_id', TEST_ORGANIZATION_ID);
    query.set('runtime_config_revision', '2');
    expect((await fetch(`${endpoint}?${query}`)).status).toBe(409);
    query.set('runtime_config_revision', '1');
    await db.update(sessions).set({ status: 'terminated' }).where(andSession(ws, sessionId));
    expect((await fetch(`${endpoint}?${query}`)).status).toBe(404);
  }, 30000);

  it('uses the server policy when caller LLM claims are omitted', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('mintjwt_no_llm');
    const apiKey = await createTestApiKey(db, ws);
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          mcp_server_names: [],
          vault_ids: [],
        }),
      },
    );
    expect(res.status).toBe(200);
    const got = (await res.json()) as { token: string; expires_at: number };

    const pub = await importSPKI(PUBLIC_PEM, 'RS256');
    const { payload } = await jwtVerify(got.token, pub, {
      issuer: 'orca-registry',
      audience: 'ai-gateway',
    });
    expect(payload['llm_routes']).toEqual(['managed-agent-llm-openai']);
    expect(payload['llm_models']).toEqual(['claude-3-5-sonnet-20240620']);
  }, 30000);

  it('keeps an empty llm_models claim when the configured policy has no intersection', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('mintjwt_empty_llm_intersection');
    const apiKey = await createTestApiKey(db, ws);
    const agentId = await createAgentWithModel(baseURL, apiKey, {
      provider: 'openai',
      id: 'gpt-4o',
    });
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`,
      { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' },
    );
    expect(res.status).toBe(200);
    const got = (await res.json()) as { token: string };
    const pub = await importSPKI(PUBLIC_PEM, 'RS256');
    const { payload } = await jwtVerify(got.token, pub, {
      issuer: 'orca-registry',
      audience: 'ai-gateway',
    });
    expect(Object.prototype.hasOwnProperty.call(payload, 'llm_models')).toBe(true);
    expect(payload['llm_models']).toEqual([]);
  }, 30000);

  it('fails closed for a wildcard model id persisted by an older version', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('mintjwt_legacy_wildcard_model');
    const apiKey = await createTestApiKey(db, ws);
    const agentId = await createAgentWithModel(baseURL, apiKey, {
      provider: 'openai',
      id: 'gpt-4o',
    });
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const [version] = await db
      .select()
      .from(agentVersions)
      .where(
        and(
          eq(agentVersions.workspaceId, ws),
          eq(agentVersions.agentId, agentId),
          eq(agentVersions.version, 1),
        ),
      )
      .limit(1);
    expect(version).toBeDefined();
    const snapshot = version!.snapshot as Record<string, unknown>;
    await db
      .update(agentVersions)
      .set({
        snapshot: {
          ...snapshot,
          model: { ...(snapshot.model as Record<string, unknown>), id: 'gpt-*' },
        },
      })
      .where(eq(agentVersions.id, version!.id));

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`,
      { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' },
    );
    expect(res.status).toBe(200);
    const got = (await res.json()) as { token: string };
    const pub = await importSPKI(PUBLIC_PEM, 'RS256');
    const { payload } = await jwtVerify(got.token, pub, {
      issuer: 'orca-registry',
      audience: 'ai-gateway',
    });
    expect(payload['llm_models']).toEqual([]);
  }, 30000);

  it('derives concrete coordinator and pinned subagent models', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('mintjwt_multiagent');
    const apiKey = await createTestApiKey(db, ws);
    const workerId = await createAgentWithModel(baseURL, apiKey, {
      provider: 'openai',
      id: 'gpt-4o-mini',
    });
    const coordinatorId = await createAgentWithModel(
      baseURL,
      apiKey,
      { provider: 'anthropic', id: 'claude-sonnet-4-5' },
      { type: 'coordinator', agents: [workerId] },
    );
    const workerUpdate = await fetch(`${baseURL}/v1/agents/${workerId}`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        version: 1,
        model: { provider: 'openai', id: 'gpt-4o' },
      }),
    });
    expect(workerUpdate.status).toBe(200);
    const sessionId = await createTestSession(baseURL, apiKey, coordinatorId);

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`,
      { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' },
    );
    expect(res.status).toBe(200);
    const got = (await res.json()) as { token: string };
    const pub = await importSPKI(PUBLIC_PEM, 'RS256');
    const { payload } = await jwtVerify(got.token, pub, {
      issuer: 'orca-registry',
      audience: 'ai-gateway',
    });
    expect(payload['llm_models']).toEqual(['claude-sonnet-4-5', 'gpt-4o-mini']);
    const guardrailQuery = new URLSearchParams({
      principal_id: sessionId,
      request_id: 'req_delegated',
      traffic_kind: 'llm',
      runtime_config_revision: String(payload['runtime_config_revision']),
      'scope.org_id': TEST_ORGANIZATION_ID,
      'scope.workspace_id': ws,
      'scope.session_id': sessionId,
      'scope.agent_id': workerId,
    });
    const guardrailUrl = `${baseURL}/internal/v1/guardrails/effective`;
    expect((await fetch(`${guardrailUrl}?${guardrailQuery}`)).status).toBe(200);
    guardrailQuery.set('scope.agent_id', `agt_not_in_roster_${Date.now()}`);
    expect((await fetch(`${guardrailUrl}?${guardrailQuery}`)).status).toBe(409);
  }, 30000);

  it('uses the session primary model override', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('mintjwt_override');
    const apiKey = await createTestApiKey(db, ws);
    const agentId = await createTestAgent(baseURL, apiKey);
    const environmentId = await createTestEnvironment(baseURL, apiKey);
    const sessionRes = await fetch(`${baseURL}/v1/sessions`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      body: JSON.stringify({
        agent: {
          type: 'agent_with_overrides',
          id: agentId,
          model: { provider: 'anthropic', id: 'claude-opus-4-6' },
        },
        environment_id: environmentId,
      }),
    });
    expect(sessionRes.status).toBe(200);
    const sessionId = ((await sessionRes.json()) as { id: string }).id;

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`,
      { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' },
    );
    expect(res.status).toBe(200);
    const got = (await res.json()) as { token: string };
    const pub = await importSPKI(PUBLIC_PEM, 'RS256');
    const { payload } = await jwtVerify(got.token, pub, {
      issuer: 'orca-registry',
      audience: 'ai-gateway',
    });
    expect(payload['llm_models']).toEqual(['claude-opus-4-6']);
  }, 30000);

  it('returns a structured conflict when a pinned subagent binding is invalid', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('mintjwt_invalid_subagent');
    const apiKey = await createTestApiKey(db, ws);
    const workerId = await createAgentWithModel(baseURL, apiKey, {
      provider: 'openai',
      id: 'gpt-4o-mini',
    });
    const coordinatorId = await createAgentWithModel(
      baseURL,
      apiKey,
      { provider: 'anthropic', id: 'claude-sonnet-4-5' },
      { type: 'coordinator', agents: [workerId] },
    );
    const sessionId = await createTestSession(baseURL, apiKey, coordinatorId);

    await db
      .update(agents)
      .set({ archivedAt: new Date() })
      .where(and(eq(agents.workspaceId, ws), eq(agents.id, workerId)));

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`,
      { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' },
    );
    expect(res.status).toBe(409);
    expect(await res.json()).toEqual({
      error: 'invalid_runtime_binding',
      resource_type: 'agent_version',
      resource_id: `${workerId}@1`,
    });
  }, 30000);

  it('omits LLM claims from git-creds tokens minted by the shared endpoint', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('mintjwt_git_creds');
    const apiKey = await createTestApiKey(db, ws);
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const repoUrls = ['https://github.com/orca-ae/orca-agent-engine'];

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ audience: 'git-creds', repo_urls: repoUrls }),
      },
    );
    expect(res.status).toBe(200);
    const got = (await res.json()) as { token: string };

    const pub = await importSPKI(PUBLIC_PEM, 'RS256');
    const { payload } = await jwtVerify(got.token, pub, {
      issuer: 'orca-registry',
      audience: 'git-creds',
    });
    expect(payload['repo_urls']).toEqual(repoUrls);
    expect(payload['llm_routes']).toBeUndefined();
    expect(payload['llm_models']).toBeUndefined();
  }, 30000);

  it('rejects JWT minting for terminated sessions and archived workspaces', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('mintjwt_inactive');
    const apiKey = await createTestApiKey(db, ws);
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);
    const url = `${baseURL}/internal/v1/workspaces/${ws}/sessions/${sessionId}/mint-jwt`;
    const request = () =>
      fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ mcp_server_names: [] }),
      });

    await db.update(sessions).set({ status: 'terminated' }).where(andSession(ws, sessionId));
    await expect(request()).resolves.toMatchObject({ status: 404 });

    await db.update(sessions).set({ status: 'idle' }).where(andSession(ws, sessionId));
    await db
      .update(workspaces)
      .set({ status: 'archived', archivedAt: new Date(), updatedAt: new Date() })
      .where(eq(workspaces.id, ws));
    try {
      await expect(request()).resolves.toMatchObject({ status: 404 });
    } finally {
      await db.transaction(async (tx) => {
        await tx
          .update(workspaces)
          .set({ status: 'active', archivedAt: null, updatedAt: new Date() })
          .where(eq(workspaces.id, ws));
        await tx
          .delete(agentObservabilityWorkspaceArchiveRevocations)
          .where(eq(agentObservabilityWorkspaceArchiveRevocations.workspaceId, ws));
      });
    }
  }, 30000);

  it('returns 404 when session does not exist for workspace', async () => {
    const ws = uniqueWorkspace('mintjwtnf');
    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${ws}/sessions/ses_nonexistent/mint-jwt`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ mcp_server_names: [] }),
      },
    );
    expect(res.status).toBe(404);
  }, 30000);

  it('returns 404 when an existing session is addressed through another workspace', async () => {
    const { db } = await getTestDb();
    const ws = uniqueWorkspace('mintjwt_scope');
    const apiKey = await createTestApiKey(db, ws);
    const agentId = await createTestAgent(baseURL, apiKey);
    const sessionId = await createTestSession(baseURL, apiKey, agentId);

    const res = await fetch(
      `${baseURL}/internal/v1/workspaces/${uniqueWorkspace('mintjwt_wrong')}/sessions/${sessionId}/mint-jwt`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ mcp_server_names: [] }),
      },
    );
    expect(res.status).toBe(404);
  }, 30000);
});

function andSession(workspaceId: string, sessionId: string) {
  return and(eq(sessions.workspaceId, workspaceId), eq(sessions.id, sessionId));
}
