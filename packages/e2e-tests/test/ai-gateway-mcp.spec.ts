// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @orca/e2e-tests — ai-gateway MCP forwarding.
 *
 * This spec drives the real ai-gateway container from the local stack. It does
 * not use Claude or the harness. Instead, it starts a fake Streamable HTTP MCP
 * upstream on the Docker host, creates registry vault credential/session state, mints the
 * same session-scoped JWT the harness would use, and calls ai-gateway's
 * `/v1/mcp` endpoint directly.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { once } from 'node:events';
import { readFile } from 'node:fs/promises';
import { createServer, type IncomingMessage, type Server } from 'node:http';
import { createConnection } from 'node:net';
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
import { createTestEnvironment, deleteTestEnvironment } from './environment-helpers.js';

const UPSTREAM_PORT = 18_191;
const REDIRECT_TARGET_PORT = 18_192;
const UPSTREAM_BACKEND_NAME = 'gateway-e2e';
const upstreamHost = process.env['AI_GATEWAY_E2E_UPSTREAM_HOST'] ?? 'host.docker.internal';
const UPSTREAM_URL = `http://${upstreamHost}:${UPSTREAM_PORT}/mcp`;
const MISMATCH_URL = `http://${upstreamHost}:${UPSTREAM_PORT}/other-mcp`;
const REDIRECT_URL = `http://${upstreamHost}:${UPSTREAM_PORT}/redirect`;
const REDIRECT_TARGET_URL = `http://${upstreamHost}:${REDIRECT_TARGET_PORT}/must-not-reach`;
const registryInternalBaseURL =
  process.env['REGISTRY_INTERNAL_BASE_URL'] ?? 'http://localhost:8081';
const here = dirname(fileURLToPath(import.meta.url));
const defaultInternalTokenFile = resolve(here, '../../../services/dev/run/internal-service-token');

let internalServiceToken: string;

interface AgentResponse {
  id: string;
}

interface SessionResponse {
  id: string;
}

interface VaultResponse {
  id: string;
}

interface VaultCredentialResponse {
  id: string;
}

interface MintJwtResponse {
  token: string;
  expires_at: number;
}

interface ResolvedDestinationResponse {
  url: string;
  credential_id: string | null;
  revision: number;
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

describe('Layer A.5: ai-gateway MCP forwarding (live ai-gateway + Kafka)', () => {
  let cfg: OrcaClientConfig;
  let workspaceId: string;
  let environmentId: string;
  let upstream: Server;
  let redirectTarget: Server;
  let expectedUpstreamAuthorization = '';
  const captured: CapturedUpstreamRequest[] = [];
  const redirectTargetCaptured: CapturedUpstreamRequest[] = [];
  const created: { sessions: string[]; agents: string[]; vaults: string[] } = {
    sessions: [],
    agents: [],
    vaults: [],
  };

  beforeAll(async () => {
    const tokenFile = process.env['INTERNAL_SERVICE_TOKEN_FILE'] ?? defaultInternalTokenFile;
    internalServiceToken = (await readFile(tokenFile, 'utf8')).trim();
    expect(internalServiceToken.length).toBeGreaterThanOrEqual(32);

    const seeded = await seedWorkspaceApiKey();
    workspaceId = seeded.workspaceId;
    cfg = buildClientFromConfig({ apiKey: seeded.apiKey });
    await ensureStackReachable(cfg);
    environmentId = await createTestEnvironment(cfg, 'gateway-e2e-env');
    await ensureGatewayReachable();
    redirectTarget = await startRedirectTarget(redirectTargetCaptured);
    upstream = await startFakeMcpUpstream(captured, () => expectedUpstreamAuthorization);
  });

  afterAll(async () => {
    if (cfg) {
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
      for (const id of created.vaults.splice(0)) {
        await apiCall(cfg, `/v1/vaults/${id}`, {
          method: 'DELETE',
          body: JSON.stringify({}),
        }).catch(() => {});
      }
      if (environmentId) {
        await deleteTestEnvironment(cfg, environmentId).catch(() => {});
      }
    }
    await Promise.all([closeServer(upstream), closeServer(redirectTarget)]);
  });

  it('forwards /v1/mcp through ai-gateway with credential injection, JWT ACLs, and audit', async () => {
    const secret = `gateway-e2e-secret-${Date.now()}`;
    expectedUpstreamAuthorization = `Bearer ${secret}`;

    const vaultRes = await apiCall(cfg, '/v1/vaults', {
      method: 'POST',
      body: JSON.stringify({
        display_name: `gateway-e2e-${Date.now()}`,
        metadata: { suite: 'ai-gateway-mcp' },
      }),
    });
    expect(vaultRes.status, vaultRes.text).toBe(200);
    const vault = vaultRes.json<VaultResponse>();
    created.vaults.push(vault.id);

    const credentialRes = await apiCall(cfg, `/v1/vaults/${vault.id}/credentials`, {
      method: 'POST',
      body: JSON.stringify({
        display_name: 'Gateway e2e static bearer',
        auth: { type: 'static_bearer', token: secret, mcp_server_url: UPSTREAM_URL },
      }),
    });
    expect(credentialRes.status, credentialRes.text).toBe(200);
    const credential = credentialRes.json<VaultCredentialResponse>();

    const otherCredentialRes = await apiCall(cfg, `/v1/vaults/${vault.id}/credentials`, {
      method: 'POST',
      body: JSON.stringify({
        display_name: 'Gateway e2e tuple-mismatch bearer',
        auth: { type: 'static_bearer', token: `${secret}-other`, mcp_server_url: MISMATCH_URL },
      }),
    });
    expect(otherCredentialRes.status, otherCredentialRes.text).toBe(200);
    const otherCredential = otherCredentialRes.json<VaultCredentialResponse>();

    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `gateway-e2e-agent-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: '',
        tools: [
          {
            type: 'mcp_toolset',
            mcp_server_name: UPSTREAM_BACKEND_NAME,
            default_config: { permission_policy: { type: 'always_allow' } },
          },
        ],
        mcp_servers: [{ name: UPSTREAM_BACKEND_NAME, url: UPSTREAM_URL }],
        skills: [],
        metadata: { suite: 'ai-gateway-mcp' },
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

    const token = await mintSessionJwt(workspaceId, session.id, {
      mcpServerNames: [UPSTREAM_BACKEND_NAME],
      vaultIds: [vault.id],
    });
    const resolvedDestination = await resolveMcpDestination(
      workspaceId,
      session.id,
      UPSTREAM_BACKEND_NAME,
    );
    expect(resolvedDestination).toMatchObject({
      url: UPSTREAM_URL,
      credential_id: credential.id,
    });
    expect(resolvedDestination.revision).toBeGreaterThan(0);
    const initRequestId = `req_gateway_e2e_init_${Date.now()}`;
    const initialized = await gatewayMcpCall({
      token,
      backend: UPSTREAM_BACKEND_NAME,
      sessionId: session.id,
      requestId: initRequestId,
      body: {
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2025-06-18',
          capabilities: {},
          clientInfo: { name: 'orca-e2e-tests', version: '0.0.0' },
        },
      },
    });
    expect(initialized.status, initialized.text).toBe(200);
    const upstreamMcpSessionId = initialized.headers.get('mcp-session-id');
    expect(upstreamMcpSessionId).toBeTruthy();

    const happyRequestId = `req_gateway_e2e_${Date.now()}`;
    const happy = await gatewayMcpCall({
      token,
      backend: UPSTREAM_BACKEND_NAME,
      sessionId: session.id,
      requestId: happyRequestId,
      mcpSessionId: upstreamMcpSessionId!,
    });
    expect(happy.status, happy.text).toBe(200);
    expect(JSON.parse(happy.text)).toMatchObject({
      jsonrpc: '2.0',
      id: 1,
      result: { tools: [{ name: 'gateway_e2e_echo' }] },
    });
    expect(happy.headers.get('mcp-session-id')).toBe(upstreamMcpSessionId);

    const beforeReplayCalls = captured.length;
    const replay = await gatewayMcpCall({
      token,
      backend: UPSTREAM_BACKEND_NAME,
      sessionId: session.id,
      requestId: happyRequestId,
      mcpSessionId: upstreamMcpSessionId!,
    });
    expect(replay.status, replay.text).toBe(200);
    expect(replay.text).toBe(happy.text);
    expect(replay.headers.get('mcp-session-id')).toBe(upstreamMcpSessionId);
    expect(captured).toHaveLength(beforeReplayCalls);

    expect(captured).toHaveLength(2);
    expect(captured[0]).toMatchObject({
      accept: expect.stringContaining('text/event-stream'),
      method: 'POST',
      url: '/mcp',
      authorization: expectedUpstreamAuthorization,
      jsonBody: { jsonrpc: '2.0', id: 1, method: 'initialize' },
    });
    expect(captured[1]).toMatchObject({
      accept: expect.stringContaining('text/event-stream'),
      method: 'POST',
      url: '/mcp',
      authorization: expectedUpstreamAuthorization,
      mcpSessionId: upstreamMcpSessionId,
      jsonBody: { jsonrpc: '2.0', id: 1, method: 'tools/list' },
    });

    const audit = await waitForGatewayAudit(workspaceId, happyRequestId);
    expect(audit).toMatchObject({
      request_id: happyRequestId,
      action: 'mcp.forward',
      principal_id: session.id,
      scope: { workspace_id: workspaceId, session_id: session.id },
      attributes: {
        destination: UPSTREAM_BACKEND_NAME,
        resolver_revision: resolvedDestination.revision,
        credential_id: credential.id,
      },
    });
    expect(JSON.stringify(audit.resource)).toContain(UPSTREAM_BACKEND_NAME);
    expect(JSON.stringify(audit.decision).toLowerCase()).toContain('allow');
    expect(JSON.stringify(audit)).not.toContain(secret);
    expect(JSON.stringify(audit)).not.toContain(UPSTREAM_URL);

    const advisoryHint = await gatewayMcpCall({
      token,
      backend: UPSTREAM_BACKEND_NAME,
      // Dynamic Registry binding remains authoritative; this startup hint
      // cannot rebind the request to another credential in the same Vault.
      credentialId: otherCredential.id,
      sessionId: session.id,
      requestId: `req_gateway_e2e_advisory_credential_${Date.now()}`,
      mcpSessionId: upstreamMcpSessionId!,
    });
    expect(advisoryHint.status, advisoryHint.text).toBe(200);
    expect(captured.at(-1)?.authorization).toBe(expectedUpstreamAuthorization);

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
        display_name: 'Gateway e2e replacement bearer',
        auth: { type: 'static_bearer', token: rotatedSecret, mcp_server_url: UPSTREAM_URL },
      }),
    });
    expect(replacementRes.status, replacementRes.text).toBe(200);
    const replacement = replacementRes.json<VaultCredentialResponse>();
    expect(decodeJwtPayload(token)['credential_ids']).not.toContain(replacement.id);
    expectedUpstreamAuthorization = `Bearer ${rotatedSecret}`;

    const rotated = await gatewayMcpCall({
      token,
      backend: UPSTREAM_BACKEND_NAME,
      // Reuse both startup JWT and archived credential hint. Registry's live
      // tuple must select the replacement without restarting the session.
      credentialId: credential.id,
      sessionId: session.id,
      requestId: `req_gateway_e2e_rotated_credential_${Date.now()}`,
      mcpSessionId: upstreamMcpSessionId!,
    });
    expect(rotated.status, rotated.text).toBe(200);
    expect(captured.at(-1)?.authorization).toBe(`Bearer ${rotatedSecret}`);

    const beforeDeniedCalls = captured.length;
    const missingAuth = await gatewayMcpCall({
      backend: UPSTREAM_BACKEND_NAME,
      credentialId: credential.id,
      sessionId: session.id,
      requestId: `req_gateway_e2e_missing_auth_${Date.now()}`,
    });
    // Current gateway releases let the anonymous principal reach the ACL
    // stage, which rejects as 403. A stricter authn stage may surface this
    // as 401. Either way, the security property is fail-closed before the
    // upstream sees the request.
    expect([401, 403], missingAuth.text).toContain(missingAuth.status);

    const wrongAudience = await mintSessionJwt(workspaceId, session.id, {
      mcpServerNames: [UPSTREAM_BACKEND_NAME],
      vaultIds: [vault.id],
      audience: 'git-creds',
    });
    const wrongAudienceResp = await gatewayMcpCall({
      token: wrongAudience,
      backend: UPSTREAM_BACKEND_NAME,
      credentialId: credential.id,
      sessionId: session.id,
      requestId: `req_gateway_e2e_wrong_aud_${Date.now()}`,
    });
    expect(wrongAudienceResp.status, wrongAudienceResp.text).toBe(401);

    const forbiddenBackend = await gatewayMcpCall({
      token,
      backend: 'github',
      credentialId: credential.id,
      sessionId: session.id,
      requestId: `req_gateway_e2e_forbidden_backend_${Date.now()}`,
    });
    expect(forbiddenBackend.status, forbiddenBackend.text).toBe(403);

    const wildcardSession = await createAgentSession(
      cfg,
      environmentId,
      created,
      '*',
      UPSTREAM_URL,
    );
    const wildcardToken = await mintSessionJwt(workspaceId, wildcardSession.id, {
      mcpServerNames: ['*'],
      vaultIds: [],
    });
    expect(decodeJwtPayload(wildcardToken)['mcp_server_names']).toEqual(['*']);
    const literalWildcard = await gatewayMcpCall({
      token: wildcardToken,
      backend: '*',
      sessionId: wildcardSession.id,
      requestId: `req_gateway_e2e_literal_wildcard_${Date.now()}`,
    });
    expect(literalWildcard.status, literalWildcard.text).toBe(404);
    expect(captured).toHaveLength(beforeDeniedCalls);

    for (const [backend, url] of [
      ['blocked-loopback', 'http://127.0.0.1:18191/mcp'],
      ['blocked-metadata', 'http://169.254.169.254/latest/meta-data/'],
      ['blocked-control-plane', 'http://registry:8081/healthz'],
    ] as const) {
      const blocked = await createAgentSession(cfg, environmentId, created, backend, url);
      const blockedToken = await mintSessionJwt(workspaceId, blocked.id, {
        mcpServerNames: [backend],
        vaultIds: [],
      });
      const beforeBlockedCalls = captured.length;
      const response = await gatewayMcpCall({
        token: blockedToken,
        backend,
        sessionId: blocked.id,
        requestId: `req_gateway_e2e_${backend}_${Date.now()}`,
      });
      expect(response.status, `${backend}: ${response.text}`).toBe(403);
      expect(captured).toHaveLength(beforeBlockedCalls);
    }

    const redirectSession = await createAgentSession(
      cfg,
      environmentId,
      created,
      'allowed-redirect',
      REDIRECT_URL,
    );
    const redirectToken = await mintSessionJwt(workspaceId, redirectSession.id, {
      mcpServerNames: ['allowed-redirect'],
      vaultIds: [],
    });
    const beforeRedirectCalls = captured.length;
    const redirect = await gatewayMcpCall({
      token: redirectToken,
      backend: 'allowed-redirect',
      sessionId: redirectSession.id,
      requestId: `req_gateway_e2e_redirect_${Date.now()}`,
    });
    expect(redirect.status, redirect.text).toBe(502);
    expect(captured).toHaveLength(beforeRedirectCalls + 1);
    expect(captured.at(-1)?.url).toBe('/redirect');
    await new Promise((resolve) => setTimeout(resolve, 250));
    expect(redirectTargetCaptured).toHaveLength(0);
  }, 120_000);

  it('denies an LLM model that is not pinned by the session JWT', async () => {
    const agentRes = await apiCall(cfg, '/v1/agents', {
      method: 'POST',
      body: JSON.stringify({
        name: `gateway-llm-scope-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-sonnet-4-6' },
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: { suite: 'ai-gateway-llm-scope' },
      }),
    });
    expect(agentRes.status, agentRes.text).toBe(200);
    const agent = agentRes.json<AgentResponse>();
    created.agents.push(agent.id);

    const sessionRes = await apiCall(cfg, '/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({ environment_id: environmentId, agent_id: agent.id, vault_ids: [] }),
    });
    expect(sessionRes.status, sessionRes.text).toBe(200);
    const session = sessionRes.json<SessionResponse>();
    created.sessions.push(session.id);

    const token = await mintSessionJwt(workspaceId, session.id, {
      mcpServerNames: [],
      vaultIds: [],
    });
    expect(decodeJwtPayload(token)).toMatchObject({
      llm_routes: ['llm-messages'],
      llm_models: ['claude-sonnet-4-6'],
    });

    const denied = await fetchWithTimeout(
      gatewayMessagesUrl(),
      {
        method: 'POST',
        headers: {
          authorization: `Bearer ${token}`,
          'content-type': 'application/json',
          'x-orca-session-id': session.id,
        },
        body: JSON.stringify({
          model: 'claude-opus-5',
          max_tokens: 1,
          messages: [{ role: 'user', content: 'this request must be denied' }],
        }),
      },
      15_000,
    );
    expect(denied.status).toBe(403);
  });
});

async function startFakeMcpUpstream(
  captured: CapturedUpstreamRequest[],
  expectedAuthorization: () => string,
): Promise<Server> {
  await assertLocalPortUnused(UPSTREAM_PORT, 'ai-gateway MCP e2e upstream');
  const server = createServer(async (req, res) => {
    if (req.method === 'POST' && req.url === '/redirect') {
      const rawBody = await readRequestBody(req);
      captured.push({
        accept: req.headers.accept,
        authorization: req.headers.authorization,
        mcpSessionId: undefined,
        method: req.method,
        url: req.url,
        rawBody,
        jsonBody: JSON.parse(rawBody),
      });
      res.writeHead(302, { location: REDIRECT_TARGET_URL });
      res.end();
      return;
    }
    if (req.method !== 'POST' || req.url !== '/mcp') {
      res.writeHead(404, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'not found' }));
      return;
    }
    const rawBody = await readRequestBody(req);
    const jsonBody = JSON.parse(rawBody) as { id?: unknown; method?: string };
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
    res.end(
      JSON.stringify({
        jsonrpc: '2.0',
        id: jsonBody.id ?? null,
        result:
          jsonBody.method === 'initialize'
            ? {
                protocolVersion: '2025-06-18',
                capabilities: { tools: {} },
                serverInfo: { name: UPSTREAM_BACKEND_NAME, version: '1.0.0' },
              }
            : { tools: [{ name: 'gateway_e2e_echo', inputSchema: { type: 'object' } }] },
      }),
    );
  });

  const error = once(server, 'error').then(([e]) => {
    throw e;
  });
  server.listen(UPSTREAM_PORT, '0.0.0.0');
  await Promise.race([once(server, 'listening'), error]);
  return server;
}

async function startRedirectTarget(captured: CapturedUpstreamRequest[]): Promise<Server> {
  await assertLocalPortUnused(REDIRECT_TARGET_PORT, 'ai-gateway redirect target');
  const server = createServer(async (req, res) => {
    const rawBody = await readRequestBody(req);
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
      jsonBody: rawBody.length > 0 ? JSON.parse(rawBody) : null,
    });
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'redirect was followed' }));
  });
  const error = once(server, 'error').then(([e]) => {
    throw e;
  });
  server.listen(REDIRECT_TARGET_PORT, '0.0.0.0');
  await Promise.race([once(server, 'listening'), error]);
  return server;
}

async function closeServer(server: Server | undefined): Promise<void> {
  if (!server) return;
  await new Promise<void>((resolve) => server.close(() => resolve())).catch(() => {});
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

function readRequestBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
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

async function mintSessionJwt(
  workspaceId: string,
  sessionId: string,
  opts: { mcpServerNames: string[]; vaultIds: string[]; audience?: string },
): Promise<string> {
  const res = await fetch(
    `${registryInternalBaseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/mint-jwt`,
    {
      method: 'POST',
      headers: {
        authorization: `Bearer ${internalServiceToken}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        mcp_server_names: opts.mcpServerNames,
        vault_ids: opts.vaultIds,
        audience: opts.audience,
      }),
    },
  );
  const text = await res.text();
  expect(res.status, text).toBe(200);
  return (JSON.parse(text) as MintJwtResponse).token;
}

async function resolveMcpDestination(
  workspaceId: string,
  sessionId: string,
  backend: string,
): Promise<ResolvedDestinationResponse> {
  const response = await fetch(
    `${registryInternalBaseURL}/internal/v1/workspaces/${workspaceId}/sessions/${sessionId}/mcp-destination/resolve`,
    {
      method: 'POST',
      headers: {
        authorization: `Bearer ${internalServiceToken}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ backend }),
    },
  );
  const text = await response.text();
  expect(response.status, text).toBe(200);
  return JSON.parse(text) as ResolvedDestinationResponse;
}

function decodeJwtPayload(token: string): Record<string, unknown> {
  const payload = token.split('.')[1];
  if (!payload) throw new Error('minted JWT has no payload segment');
  return JSON.parse(Buffer.from(payload, 'base64url').toString('utf8')) as Record<string, unknown>;
}

async function gatewayMcpCall(opts: {
  token?: string;
  backend: string;
  credentialId?: string;
  sessionId: string;
  requestId: string;
  mcpSessionId?: string;
  body?: Record<string, unknown>;
}): Promise<{ status: number; text: string; headers: Headers }> {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    'x-orca-backend': opts.backend,
    'x-orca-session-id': opts.sessionId,
    'x-request-id': opts.requestId,
    'idempotency-key': opts.requestId,
  };
  if (opts.credentialId) headers['x-orca-credential-id'] = opts.credentialId;
  if (opts.token) headers.authorization = `Bearer ${opts.token}`;
  if (opts.mcpSessionId) headers['mcp-session-id'] = opts.mcpSessionId;
  const res = await fetchWithTimeout(
    gatewayMcpUrl(),
    {
      method: 'POST',
      redirect: 'manual',
      headers,
      body: JSON.stringify(opts.body ?? { jsonrpc: '2.0', id: 1, method: 'tools/list' }),
    },
    15_000,
  );
  return { status: res.status, text: await res.text(), headers: res.headers };
}

async function createAgentSession(
  cfg: OrcaClientConfig,
  environmentId: string,
  created: { sessions: string[]; agents: string[] },
  backend: string,
  url: string,
): Promise<SessionResponse> {
  const agentRes = await apiCall(cfg, '/v1/agents', {
    method: 'POST',
    body: JSON.stringify({
      name: `${backend}-${Date.now()}`,
      model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
      system: '',
      tools: [
        {
          type: 'mcp_toolset',
          mcp_server_name: backend,
          default_config: { permission_policy: { type: 'always_allow' } },
        },
      ],
      mcp_servers: [{ name: backend, url }],
      skills: [],
      metadata: { suite: 'ai-gateway-mcp-egress' },
    }),
  });
  expect(agentRes.status, agentRes.text).toBe(200);
  const agent = agentRes.json<AgentResponse>();
  created.agents.push(agent.id);

  const sessionRes = await apiCall(cfg, '/v1/sessions', {
    method: 'POST',
    body: JSON.stringify({ environment_id: environmentId, agent_id: agent.id, vault_ids: [] }),
  });
  expect(sessionRes.status, sessionRes.text).toBe(200);
  const session = sessionRes.json<SessionResponse>();
  created.sessions.push(session.id);
  return session;
}

function gatewayMcpUrl(): string {
  return gatewayMcpEndpointUrl(process.env['AI_GATEWAY_URL'] ?? 'http://localhost:8090');
}

function gatewayMessagesUrl(): string {
  const configured =
    process.env['LLM_GATEWAY_URL'] ?? process.env['AI_GATEWAY_URL'] ?? 'http://localhost:8090';
  const url = new URL(configured);
  url.pathname = '/v1/messages';
  url.search = '';
  return url.toString();
}

function gatewayMcpEndpointUrl(gatewayUrl: string): string {
  let trimmed = gatewayUrl;
  while (trimmed.endsWith('/')) trimmed = trimmed.slice(0, -1);
  if (trimmed.endsWith('/v1/mcp')) return trimmed;
  return trimmed + '/v1/mcp';
}

async function waitForGatewayAudit(workspaceId: string, requestId: string): Promise<AuditRecord> {
  const topic = `orca.${workspaceId}.audit.ai-gateway`;
  const brokers = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
  const kafka = new Kafka({ brokers, clientId: 'orca-e2e-ai-gateway', logLevel: logLevel.NOTHING });
  await waitForKafkaTopic(kafka, topic);

  const consumer = kafka.consumer({
    groupId: `orca-e2e-ai-gateway-${process.pid}-${Date.now()}`,
  });
  await consumer.connect();
  try {
    await consumer.subscribe({ topic, fromBeginning: true });
    const found = new Promise<AuditRecord>((resolve) => {
      void consumer.run({
        eachMessage: async ({ message }) => {
          const record = parseAuditMessage(message.value?.toString('utf8'));
          if (record?.request_id === requestId && record.action === 'mcp.forward') {
            resolve(record);
          }
        },
      });
    });
    return await withTimeout(
      found,
      20_000,
      `timed out waiting for ai-gateway audit request_id=${requestId} on topic ${topic}`,
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
