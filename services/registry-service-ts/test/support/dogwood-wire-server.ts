// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0
// Owned loopback qualification fixture. No model provider, Kafka or production credentials.
import { createServer } from 'node:http';
import { buildCombinedTestApp } from '../../src/server.js';
import {
  getTestDb,
  closeTestDb,
  buildStubFileStore,
  buildStubStore,
  buildTestJwtMinter,
  STUB_SSE_CONFIG,
} from '../integration/setup.js';
import {
  createTestApiKey,
  createTestSession,
  uniqueWorkspace,
  TEST_ORGANIZATION_ID,
} from '../integration/fixtures.js';
import { bindPolicyTurn } from '../../src/domain/policy-events.js';
const port = Number(process.env['OIP_REGISTRY_PORT'] ?? 55433),
  backendPort = Number(process.env['OIP_MCP_PORT'] ?? 55434);
const backendUrl = `http://127.0.0.1:${backendPort}/mcp`;
const tools = [
  {
    name: 'charge',
    description: 'Owned qualification tool',
    inputSchema: {
      type: 'object',
      properties: { amount: { type: 'number' } },
      required: ['amount'],
    },
    outputSchema: {
      type: 'object',
      properties: { receipt: { type: 'string' } },
      required: ['receipt'],
    },
  },
];
let calls = 0;
const backend = createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/count') {
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ calls }));
    return;
  }
  if (req.method === 'GET') {
    res.writeHead(405);
    res.end();
    return;
  }
  let raw = '';
  for await (const chunk of req) raw += String(chunk);
  const body = JSON.parse(raw || '{}') as {
    id?: unknown;
    method?: string;
    params?: { protocolVersion?: string };
  };
  if (!body.id) {
    res.writeHead(202);
    res.end();
    return;
  }
  const result =
    body.method === 'initialize'
      ? {
          protocolVersion: body.params?.protocolVersion ?? '2025-03-26',
          capabilities: { tools: {} },
          serverInfo: { name: 'owned-dogwood-fixture', version: '1' },
        }
      : body.method === 'tools/list'
        ? { tools }
        : body.method === 'tools/call'
          ? (calls++,
            {
              content: [{ type: 'text', text: 'recorded' }],
              structuredContent: { receipt: 'owned' },
            })
          : {};
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ jsonrpc: '2.0', id: body.id, result }));
});
await new Promise<void>((resolve) => backend.listen(backendPort, '127.0.0.1', resolve));
const { db } = await getTestDb();
const workspaceId = uniqueWorkspace('dogwood_wire');
const key = await createTestApiKey(db, workspaceId);
const app = buildCombinedTestApp({
  db,
  oidc: { allowedIssuers: [], audience: 'test' },
  store: buildStubStore(),
  fileStore: buildStubFileStore(),
  sse: STUB_SSE_CONFIG,
  jwtMinter: buildTestJwtMinter(),
  guardrails: { dogwoodEnabled: true, gatewayDogwoodEnabled: true, observeEnabled: true },
});
await app.listen({ host: '127.0.0.1', port });
const base = `http://127.0.0.1:${port}`;
const response = await fetch(`${base}/v1/agents`, {
  method: 'POST',
  headers: { 'x-api-key': key, 'content-type': 'application/json' },
  body: JSON.stringify({
    name: 'wire-agent',
    model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
    tools: [{ type: 'mcp_toolset', mcp_server_name: 'wire' }],
    mcp_servers: [{ name: 'wire', url: backendUrl }],
    skills: [],
    metadata: {},
  }),
});
if (!response.ok) throw new Error(`fixture agent failed: ${await response.text()}`);
const agentId = ((await response.json()) as { id: string }).id,
  sessionId = await createTestSession(base, key, agentId);
const rule = {
  kind: 'dogwood',
  appliesTo: 'mcp_tools',
  history: 'session',
  policy:
    '@id("baseline") permit(principal,action,resource); @id("one-charge") forbid(principal,action == Orca::Action::"mcp__payments__charge",resource) when temporal {count_within(1h,Orca::Action::"mcp__payments__charge"::request{}) > 1};',
  mcpManifests: [{ server: 'payments', url: backendUrl, tools }],
};
const binding = await db.transaction((tx) =>
  bindPolicyTurn(tx, {
    workspaceId,
    sessionId,
    owner: 'owned-wire-fixture',
    enforcer: 'ai-gateway',
    guardrails: [
      {
        id: 'grd_wire',
        name: 'one-charge',
        tier: 'workspace',
        phases: ['tool_call', 'tool_result'],
        stateful: true,
        mode: 'enforce',
        rule,
      },
    ],
    resources: [{ id: agentId, name: 'wire-agent' }],
    mcpServers: [{ name: 'wire', url: backendUrl }],
    now: () => new Date(),
  }),
);
console.log(
  JSON.stringify({
    base,
    workspaceId,
    sessionId,
    agentId,
    organizationId: TEST_ORGANIZATION_ID,
    runtimeConfigRevision: '1',
    backendUrl,
    ...binding,
  }),
);
const close = async () => {
  await app.close();
  await new Promise<void>((resolve) => backend.close(() => resolve()));
  await closeTestDb();
  process.exit(0);
};
process.on('SIGTERM', () => {
  void close();
});
process.on('SIGINT', () => {
  void close();
});
