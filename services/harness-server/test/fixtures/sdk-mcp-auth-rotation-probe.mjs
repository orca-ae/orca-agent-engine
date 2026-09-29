// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer } from 'node:http';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createSdkMcpServer, query, tool } from '@anthropic-ai/claude-agent-sdk';

// Only synthetic credentials and loopback endpoints. Never inherit provider auth,
// proxy settings, user MCP configuration, or a real user's Claude config.
const sdkVersion = JSON.parse(
  await readFile(
    new URL('package.json', import.meta.resolve('@anthropic-ai/claude-agent-sdk')),
    'utf8',
  ),
).version;
if (sdkVersion !== '0.3.283')
  throw new Error('Re-characterize this contract before changing SDK version');
const configDir = await mkdtemp(join(tmpdir(), 'orca-sdk-mcp-rotation-'));
const wire = [];
const calls = [];
const llm = [];
const sessions = new Map();
const updates = [];
const statuses = [];
const results = [];
let initializeCount = 0;
let orcaCalls = 0;
let phase = 0;
let releaseTurn;
let stream;
const abortController = new AbortController();
const watchdog = setTimeout(() => abortController.abort(), 25_000);
const server = createServer(async (request, response) => {
  try {
    let raw = '';
    for await (const chunk of request) raw += chunk;
    if (request.url?.startsWith('/v1/messages')) {
      const body = JSON.parse(raw);
      const step = llm.length % 3;
      llm.push({
        phase,
        tools: (body.tools ?? []).map((entry) => entry.name),
        hasConversationMarker: JSON.stringify(body.messages).includes('CONVERSATION_MARKER'),
        hasStateMarker: JSON.stringify(body.messages).includes('STATE_MARKER'),
      });
      if (step === 0) {
        sendMessage(response, {
          type: 'tool_use',
          id: `toolu_state_${phase}`,
          name: 'mcp__remote__state',
          input: { operation: phase === 0 ? 'write' : 'read' },
        });
      } else if (step === 1) {
        sendMessage(response, {
          type: 'tool_use',
          id: `toolu_orca_${phase}`,
          name: 'mcp__orca__ping',
          input: {},
        });
      } else {
        sendMessage(response, { type: 'text', text: 'turn complete' });
      }
      return;
    }
    if (request.url !== '/mcp') {
      response.writeHead(404).end();
      return;
    }
    const body = raw ? JSON.parse(raw) : {};
    const sessionId = request.headers['mcp-session-id'] ?? null;
    const authorization = request.headers.authorization;
    const token =
      authorization === 'Bearer probe-old'
        ? 'old'
        : authorization === 'Bearer probe-new'
          ? 'new'
          : 'unknown';
    wire.push({ phase, method: request.method, rpc: body.method ?? null, token, sessionId });
    if (token === 'unknown') {
      response.writeHead(401).end();
      return;
    }
    // Streamable HTTP permits JSON POST responses and a 405 for optional GET SSE.
    if (request.method === 'GET') {
      response.writeHead(405).end();
      return;
    }
    if (body.method === 'initialize') {
      const id = `probe-session-${++initializeCount}`;
      sessions.set(id, { value: null });
      rpc(
        response,
        body.id,
        {
          protocolVersion: body.params.protocolVersion,
          capabilities: { tools: {} },
          serverInfo: { name: 'stateful-rotation-probe', version: '1' },
        },
        id,
      );
      return;
    }
    if (!sessions.has(sessionId)) {
      response.writeHead(404).end();
      return;
    }
    if (request.method === 'DELETE') {
      sessions.delete(sessionId);
      response.writeHead(200).end();
      return;
    }
    if (body.id === undefined) {
      response.writeHead(202).end();
      return;
    }
    if (body.method === 'tools/list') {
      rpc(
        response,
        body.id,
        {
          tools: [
            {
              name: 'state',
              description: 'Read or write session-scoped state',
              inputSchema: {
                type: 'object',
                properties: { operation: { type: 'string', enum: ['write', 'read'] } },
                required: ['operation'],
              },
            },
          ],
        },
        sessionId,
      );
      return;
    }
    if (body.method === 'tools/call' && body.params.name === 'state') {
      const state = sessions.get(sessionId);
      if (body.params.arguments.operation === 'write') state.value = 'STATE_MARKER';
      calls.push({ phase, token, sessionId, value: state.value });
      rpc(
        response,
        body.id,
        { content: [{ type: 'text', text: JSON.stringify(state) }] },
        sessionId,
      );
      return;
    }
    rpc(response, body.id, {}, sessionId);
  } catch {
    response.writeHead(500).end();
  }
});

try {
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  const orca = createSdkMcpServer({
    name: 'orca',
    version: '0.0.0',
    tools: [
      tool('ping', 'Built-in liveness probe', {}, async () => {
        orcaCalls++;
        return { content: [{ type: 'text', text: 'orca alive' }] };
      }),
    ],
  });
  const remote = (token) => ({
    type: 'http',
    url: `${baseUrl}/mcp`,
    headers: { Authorization: `Bearer probe-${token}` },
  });
  async function* prompts() {
    for (let turn = 0; turn < 3; turn++) {
      const next = new Promise((resolve) => {
        releaseTurn = resolve;
      });
      yield {
        type: 'user',
        session_id: '',
        parent_tool_use_id: null,
        message: {
          role: 'user',
          content:
            turn === 0
              ? 'CONVERSATION_MARKER: write state and ping orca.'
              : 'Read state and ping orca.',
        },
      };
      await next;
    }
  }
  stream = query({
    prompt: prompts(),
    options: {
      model: 'claude-sonnet-4-5-20250929',
      tools: [],
      mcpServers: { orca, remote: remote('old') },
      strictMcpConfig: true,
      settingSources: [],
      cwd: configDir,
      persistSession: false,
      maxTurns: 4,
      abortController,
      canUseTool: async (_name, input) => ({ behavior: 'allow', updatedInput: input }),
      env: {
        PATH: process.env.PATH,
        HOME: configDir,
        TMPDIR: tmpdir(),
        NODE_ENV: 'production',
        ANTHROPIC_BASE_URL: baseUrl,
        ANTHROPIC_API_KEY: 'probe-key',
        CLAUDE_CONFIG_DIR: configDir,
        CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1',
        CLAUDE_CODE_USE_BEDROCK: '0',
        CLAUDE_CODE_USE_VERTEX: '0',
        CLAUDE_CODE_USE_FOUNDRY: '0',
      },
    },
  });
  for await (const message of stream) {
    if (message.type !== 'result') continue;
    results.push({ subtype: message.subtype, isError: message.is_error });
    statuses.push((await stream.mcpServerStatus()).map(({ name, status }) => ({ name, status })));
    if (phase < 2) {
      // Full replacement payload deliberately retains the original SDK orca instance.
      // First reapply identical config as a no-op control, then change only Authorization.
      updates.push(
        await stream.setMcpServers({ orca, remote: remote(phase === 0 ? 'old' : 'new') }),
      );
    }
    phase++;
    releaseTurn();
  }
  process.stdout.write(
    JSON.stringify({
      sdkVersion,
      initializeCount,
      wire,
      calls,
      orcaCalls,
      llm,
      updates,
      statuses,
      results,
    }),
  );
} finally {
  clearTimeout(watchdog);
  stream?.close();
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
  await rm(configDir, { recursive: true, force: true });
}

function rpc(response, id, result, sessionId) {
  response.writeHead(200, { 'content-type': 'application/json', 'mcp-session-id': sessionId });
  response.end(JSON.stringify({ jsonrpc: '2.0', id, result }));
}

function sendMessage(response, block) {
  const isTool = block.type === 'tool_use';
  const events = [
    [
      'message_start',
      {
        type: 'message_start',
        message: {
          id: `msg_${llm.length}`,
          type: 'message',
          role: 'assistant',
          model: 'claude-sonnet-4-5-20250929',
          content: [],
          stop_reason: null,
          stop_sequence: null,
          usage: { input_tokens: 1, output_tokens: 0 },
        },
      },
    ],
    [
      'content_block_start',
      {
        type: 'content_block_start',
        index: 0,
        content_block: isTool ? { ...block, input: {} } : { type: 'text', text: '' },
      },
    ],
    [
      'content_block_delta',
      {
        type: 'content_block_delta',
        index: 0,
        delta: isTool
          ? { type: 'input_json_delta', partial_json: JSON.stringify(block.input) }
          : { type: 'text_delta', text: block.text },
      },
    ],
    ['content_block_stop', { type: 'content_block_stop', index: 0 }],
    [
      'message_delta',
      {
        type: 'message_delta',
        delta: { stop_reason: isTool ? 'tool_use' : 'end_turn', stop_sequence: null },
        usage: { output_tokens: 5 },
      },
    ],
    ['message_stop', { type: 'message_stop' }],
  ];
  response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
  for (const [event, data] of events)
    response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  response.end();
}
