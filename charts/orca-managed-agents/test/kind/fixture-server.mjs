// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer } from 'node:http';
import { pathToFileURL } from 'node:url';

const expectedMcpAuthorization = 'Bearer kind-helm-e2e-secret';
const mcpSessionId = 'kind-helm-mcp-session';
const maxCapturedValues = 100;
let responseSequence = 0;

// Keep this deliberately summary-only. The fixture receives both API keys and
// Registry-resolved bearer credentials, neither of which may escape through a
// diagnostic endpoint or process logs.
const captured = {
  anthropic: {
    requests: 0,
    streaming: 0,
    toolResultRounds: 0,
    toolUseRounds: 0,
    markers: [],
  },
  mcp: {
    authorized: 0,
    unauthorized: 0,
    validAccepts: 0,
    invalidAccepts: 0,
    validSessionIds: 0,
    missingSessionIds: 0,
    invalidSessionIds: 0,
    methods: [],
    markers: [],
  },
};

export const server = createServer((request, response) => {
  void handleRequest(request, response).catch((error) => {
    const diagnostic = sanitizedError(error);
    console.error(`fixture request error (${diagnostic.name}): ${diagnostic.message}`);
    if (!response.headersSent) {
      json(response, 500, { error: 'fixture internal error' });
      return;
    }
    response.end();
  });
});

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  server.listen(8080, '0.0.0.0', () => {
    console.log('kind Helm fixture listening on :8080');
  });
}

async function handleRequest(request, response) {
  const url = new URL(request.url ?? '/', 'http://fixture');

  if (request.method === 'GET' && url.pathname === '/healthz') {
    json(response, 200, { status: 'ok' });
    return;
  }

  if (request.method === 'GET' && url.pathname === '/captured') {
    json(response, 200, captured);
    return;
  }

  if (request.method === 'POST' && url.pathname === '/v1/messages') {
    const body = await readJson(request);
    if (body === null) {
      json(response, 400, { error: 'invalid JSON request body' });
      return;
    }
    anthropicResponse(response, body);
    return;
  }

  if (request.method === 'POST' && url.pathname === '/mcp') {
    const body = await readJson(request);
    if (body === null) {
      json(response, 400, mcpError(null, -32700, 'Parse error'));
      return;
    }
    mcpResponse(response, request, body);
    return;
  }

  json(response, 404, { error: 'not found' });
}

async function readJson(request) {
  let raw = '';
  for await (const chunk of request) raw += chunk;
  if (raw === '') return {};
  try {
    const body = JSON.parse(raw);
    return isRecord(body) ? body : {};
  } catch {
    return null;
  }
}

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function json(response, status, body, headers = {}) {
  response.writeHead(status, {
    'cache-control': 'no-store',
    'content-type': 'application/json; charset=utf-8',
    ...headers,
  });
  response.end(JSON.stringify(body));
}

function appendCaptured(values, value) {
  if (value !== null && !values.includes(value) && values.length < maxCapturedValues) {
    values.push(value);
  }
}

function sanitizedError(error) {
  const name =
    error instanceof Error && /^[A-Za-z][A-Za-z0-9_.-]{0,63}$/.test(error.name)
      ? error.name
      : 'Error';
  const message =
    error instanceof Error ? redactErrorMessage(error.message) : 'non-Error rejection';
  return { name, message: message || 'request handling failed' };
}

function redactErrorMessage(message) {
  return message
    .replace(/https?:\/\/[^\s]+/gi, '[REDACTED_URL]')
    .replace(/\bBearer\s+[^\s,;]+/gi, 'Bearer [REDACTED]')
    .replace(/\beyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g, '[REDACTED_JWT]')
    .replace(
      /\b(authorization|access[_-]?token|api[_-]?key|token|secret|password)\b\s*[:=]\s*(?:"[^"]*"|'[^']*'|[^\s,;]+)/gi,
      '$1=[REDACTED]',
    )
    .replace(/[\r\n]+/g, ' ')
    .slice(0, 500);
}

function markerFrom(value, fallback = 'KIND_HELM_DEFAULT') {
  const serialized = typeof value === 'string' ? value : (JSON.stringify(value) ?? '');
  const matches = serialized.match(/KIND_HELM_[A-Za-z0-9_-]+/g);
  return matches?.at(-1) ?? fallback;
}

function latestMessage(body) {
  if (!Array.isArray(body.messages)) return {};
  const message = body.messages.at(-1);
  return isRecord(message) ? message : {};
}

function latestMessageHasToolResult(message) {
  return (
    Array.isArray(message.content) &&
    message.content.some((block) => isRecord(block) && block.type === 'tool_result')
  );
}

function toolResultTextValues(block) {
  if (typeof block.content === 'string') return [block.content];
  if (!Array.isArray(block.content)) return [];
  return block.content.flatMap((content) =>
    isRecord(content) && content.type === 'text' && typeof content.text === 'string'
      ? [content.text]
      : [],
  );
}

function latestMessageHasSuccessfulToolResult(message, marker) {
  if (!Array.isArray(message.content)) return false;
  const expected = `MCP_OK ${marker}`;
  return message.content.some(
    (block) =>
      isRecord(block) &&
      block.type === 'tool_result' &&
      block.is_error !== true &&
      toolResultTextValues(block).some((text) => text === expected),
  );
}

function echoToolName(body) {
  if (!Array.isArray(body.tools)) return null;
  const tool = body.tools.find(
    (candidate) =>
      isRecord(candidate) &&
      typeof candidate.name === 'string' &&
      candidate.name.toLowerCase().includes('echo'),
  );
  return tool?.name ?? null;
}

function anthropicModel(body) {
  return typeof body.model === 'string' && body.model !== ''
    ? body.model
    : 'claude-sonnet-4-5-20250929';
}

function anthropicResponse(response, body) {
  const message = latestMessage(body);
  const marker = markerFrom(message);
  const hasToolResult = latestMessageHasToolResult(message);
  const hasSuccessfulToolResult = latestMessageHasSuccessfulToolResult(message, marker);
  const toolName = echoToolName(body);

  captured.anthropic.requests += 1;
  if (body.stream === true) captured.anthropic.streaming += 1;
  if (hasToolResult) captured.anthropic.toolResultRounds += 1;
  if (!hasToolResult && toolName !== null) captured.anthropic.toolUseRounds += 1;
  appendCaptured(captured.anthropic.markers, marker);

  const model = anthropicModel(body);
  let events;
  if (hasToolResult) {
    events = textEvents(
      marker,
      model,
      hasSuccessfulToolResult ? `AGENT_OK ${marker}` : `MCP_TOOL_FAILED ${marker}`,
    );
  } else if (toolName === null) {
    events = textEvents(marker, model, `MISSING_ECHO_TOOL ${marker}`);
  } else {
    events = toolUseEvents(marker, model, toolName);
  }

  response.writeHead(200, {
    'cache-control': 'no-cache',
    'content-type': 'text/event-stream; charset=utf-8',
  });
  for (const [event, data] of events) {
    response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  }
  response.end();
}

function nextResponseId(prefix) {
  responseSequence += 1;
  return `${prefix}_${responseSequence}`;
}

function messageStart(model) {
  return [
    'message_start',
    {
      type: 'message_start',
      message: {
        id: nextResponseId('msg_kind'),
        type: 'message',
        role: 'assistant',
        model,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: 1, output_tokens: 0 },
      },
    },
  ];
}

function toolUseEvents(marker, model, toolName) {
  const input = { message: marker };
  return [
    messageStart(model),
    [
      'content_block_start',
      {
        type: 'content_block_start',
        index: 0,
        content_block: {
          type: 'tool_use',
          id: nextResponseId('toolu_kind'),
          name: toolName,
          input: {},
        },
      },
    ],
    [
      'content_block_delta',
      {
        type: 'content_block_delta',
        index: 0,
        delta: { type: 'input_json_delta', partial_json: JSON.stringify(input) },
      },
    ],
    ['content_block_stop', { type: 'content_block_stop', index: 0 }],
    [
      'message_delta',
      {
        type: 'message_delta',
        delta: { stop_reason: 'tool_use', stop_sequence: null },
        usage: { output_tokens: 10 },
      },
    ],
    ['message_stop', { type: 'message_stop' }],
  ];
}

function textEvents(marker, model, text = `AGENT_OK ${marker}`) {
  return [
    messageStart(model),
    [
      'content_block_start',
      {
        type: 'content_block_start',
        index: 0,
        content_block: { type: 'text', text: '' },
      },
    ],
    [
      'content_block_delta',
      {
        type: 'content_block_delta',
        index: 0,
        delta: { type: 'text_delta', text },
      },
    ],
    ['content_block_stop', { type: 'content_block_stop', index: 0 }],
    [
      'message_delta',
      {
        type: 'message_delta',
        delta: { stop_reason: 'end_turn', stop_sequence: null },
        usage: { output_tokens: 5 },
      },
    ],
    ['message_stop', { type: 'message_stop' }],
  ];
}

function requestMethod(body) {
  if (typeof body.method !== 'string') return 'invalid';
  switch (body.method) {
    case 'server/discover':
    case 'initialize':
    case 'notifications/initialized':
    case 'tools/list':
    case 'tools/call':
      return body.method;
    default:
      // Keep diagnostic output useful without recording arbitrary request
      // strings that could themselves contain a credential.
      return body.method.startsWith('notifications/') ? 'notification' : 'invalid';
  }
}

function acceptsStreamableHttp(request) {
  const header = request.headers.accept;
  const values = Array.isArray(header) ? header : typeof header === 'string' ? [header] : [];
  const mediaTypes = values.flatMap((value) =>
    value.split(',').map((entry) => entry.split(';', 1)[0].trim().toLowerCase()),
  );
  return mediaTypes.includes('application/json') && mediaTypes.includes('text/event-stream');
}

function sessionIdStatus(request) {
  const sessionId = request.headers['mcp-session-id'];
  if (sessionId === undefined || sessionId === '') return 'missing';
  return sessionId === mcpSessionId ? 'valid' : 'invalid';
}

function echoToolCallMarker(body) {
  if (!isRecord(body.params) || body.params.name !== 'echo') return null;
  if (!isRecord(body.params.arguments)) return null;
  const message = body.params.arguments.message;
  if (typeof message !== 'string' || !/^KIND_HELM_[A-Za-z0-9_-]+$/.test(message)) return null;
  return message;
}

function mcpError(id, code, message) {
  return { jsonrpc: '2.0', id, error: { code, message } };
}

function mcpResponse(response, request, body) {
  const method = requestMethod(body);
  const marker = method === 'tools/call' ? echoToolCallMarker(body) : null;
  const authorized = request.headers.authorization === expectedMcpAuthorization;
  const validAccept = acceptsStreamableHttp(request);

  if (authorized) captured.mcp.authorized += 1;
  else captured.mcp.unauthorized += 1;
  appendCaptured(captured.mcp.methods, method);
  appendCaptured(captured.mcp.markers, marker);
  if (validAccept) captured.mcp.validAccepts += 1;
  else captured.mcp.invalidAccepts += 1;

  if (!validAccept) {
    json(
      response,
      406,
      mcpError(
        Object.hasOwn(body, 'id') ? body.id : null,
        -32000,
        'Accept header must include application/json and text/event-stream',
      ),
    );
    return;
  }

  if (!authorized) {
    json(
      response,
      401,
      mcpError(Object.hasOwn(body, 'id') ? body.id : null, -32001, 'Unauthorized'),
    );
    return;
  }

  // Newer clients probe discovery before initialize, so no session exists yet.
  // This fixture implements the initialize handshake, not discovery: reject
  // the optional probe explicitly without counting it as lost session state.
  if (method === 'server/discover' && Object.hasOwn(body, 'id')) {
    json(response, 200, mcpError(body.id, -32601, 'Method not found'));
    return;
  }

  if (method !== 'initialize') {
    const sessionStatus = sessionIdStatus(request);
    if (sessionStatus === 'missing') {
      captured.mcp.missingSessionIds += 1;
      json(
        response,
        400,
        mcpError(Object.hasOwn(body, 'id') ? body.id : null, -32000, 'Missing MCP session ID'),
      );
      return;
    }
    if (sessionStatus === 'invalid') {
      captured.mcp.invalidSessionIds += 1;
      json(
        response,
        404,
        mcpError(Object.hasOwn(body, 'id') ? body.id : null, -32000, 'Unknown MCP session ID'),
      );
      return;
    }
    captured.mcp.validSessionIds += 1;
  }

  if (method === 'tools/call' && Object.hasOwn(body, 'id') && marker === null) {
    json(
      response,
      200,
      mcpError(Object.hasOwn(body, 'id') ? body.id : null, -32602, 'Invalid tool call'),
      {
        'mcp-session-id': mcpSessionId,
      },
    );
    return;
  }

  // Streamable HTTP notifications never receive a JSON-RPC response. Return
  // the session header so clients/proxies that retain it can continue using
  // the same server session.
  if (!Object.hasOwn(body, 'id')) {
    response.writeHead(202, {
      'cache-control': 'no-store',
      'mcp-session-id': mcpSessionId,
    });
    response.end();
    return;
  }

  let result;
  switch (method) {
    case 'initialize':
      result = {
        protocolVersion: '2025-06-18',
        capabilities: { tools: {} },
        serverInfo: { name: 'kind_e2e', version: '1.0.0' },
      };
      break;
    case 'tools/list':
      result = {
        tools: [
          {
            name: 'echo',
            description: 'Echoes a marker through Kind Helm E2E.',
            inputSchema: {
              type: 'object',
              properties: { message: { type: 'string' } },
              required: ['message'],
            },
          },
        ],
      };
      break;
    case 'tools/call':
      result = {
        content: [{ type: 'text', text: `MCP_OK ${marker}` }],
        isError: false,
      };
      break;
    default:
      json(response, 404, mcpError(body.id, -32601, 'Method not found'), {
        'mcp-session-id': mcpSessionId,
      });
      return;
  }

  json(response, 200, { jsonrpc: '2.0', id: body.id, result }, { 'mcp-session-id': mcpSessionId });
}
