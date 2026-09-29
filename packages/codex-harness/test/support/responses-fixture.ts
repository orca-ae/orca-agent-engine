// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer } from 'node:http';
import { startNativeGateway } from './native-gateway.js';

export async function createResponsesFixture(
  options: {
    callTool?: boolean;
    sessionId?: string;
    apiKey?: string;
    toolName?: string;
    toolArguments?: Record<string, unknown>;
    terminalUsage?: unknown;
  } = {},
): Promise<{
  url: string;
  apiKey: string;
  requests: Array<Record<string, unknown>>;
  close(): Promise<void>;
}> {
  const {
    callTool = false,
    sessionId = 'ses_sdk',
    apiKey = 'test-gateway-token',
    toolName = 'write',
    toolArguments = { path: 'proof.txt', content: 'proof' },
  } = options;
  const cleanup: Array<() => Promise<unknown>> = [];
  const close = async () => {
    for (const dispose of cleanup.splice(0).reverse()) await dispose();
  };
  const requests: Array<Record<string, unknown>> = [];
  const server = createServer(async (req, res) => {
    if (req.method !== 'POST') {
      res.writeHead(404).end();
      return;
    }
    if (
      req.url !== '/v1/responses' ||
      (!process.env.CODEX_TEST_GATEWAY_BINARY && req.headers['x-orca-session-id'] !== sessionId) ||
      req.headers.authorization !==
        `Bearer ${process.env.CODEX_TEST_GATEWAY_BINARY ? 'test-upstream-key' : apiKey}`
    ) {
      res.writeHead(403).end();
      return;
    }
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    requests.push(JSON.parse(Buffer.concat(chunks).toString()));
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    if (callTool && requests.length === 1) {
      const tools = requests[0]!.tools as Array<{ name?: string; tools?: Array<{ name: string }> }>;
      const namespace = tools.find((item) => item.name === 'mcp__orca');
      const tool =
        namespace?.tools?.find((item) => item.name === toolName) ??
        tools.find((item) => item.name === toolName);
      if (!tool) {
        res.end();
        return;
      }
      const item = {
        type: 'function_call',
        id: 'fc_test',
        call_id: 'call_test',
        name: tool.name,
        ...(namespace ? { namespace: namespace.name } : {}),
        arguments: JSON.stringify(toolArguments),
        status: 'completed',
      };
      const response = {
        id: 'resp_tool',
        object: 'response',
        status: 'completed',
        output: [item],
        usage: {
          input_tokens: 10,
          output_tokens: 5,
          total_tokens: 15,
          input_tokens_details: { cached_tokens: 0 },
        },
      };
      for (const event of [
        { type: 'response.created', response: { ...response, status: 'in_progress', output: [] } },
        {
          type: 'response.output_item.added',
          output_index: 0,
          item: { ...item, arguments: '', status: 'in_progress' },
        },
        { type: 'response.output_item.done', output_index: 0, item },
        { type: 'response.completed', response },
      ]) {
        res.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
      }
      res.end();
      return;
    }
    const message = {
      id: 'msg_test',
      type: 'message',
      role: 'assistant',
      status: 'completed',
      content: [{ type: 'output_text', text: 'SDK reply', annotations: [] }],
    };
    const response = {
      id: 'resp_test',
      object: 'response',
      status: 'completed',
      output: [message],
      usage: {
        input_tokens: 20,
        output_tokens: 5,
        total_tokens: 25,
        input_tokens_details: { cached_tokens: 7 },
      },
    };
    if (Object.hasOwn(options, 'terminalUsage'))
      response.usage = options.terminalUsage as typeof response.usage;
    for (const event of [
      { type: 'response.created', response: { ...response, status: 'in_progress', output: [] } },
      {
        type: 'response.output_item.added',
        output_index: 0,
        item: { ...message, status: 'in_progress', content: [] },
      },
      {
        type: 'response.content_part.added',
        item_id: 'msg_test',
        output_index: 0,
        content_index: 0,
        part: { type: 'output_text', text: '', annotations: [] },
      },
      {
        type: 'response.output_text.delta',
        item_id: 'msg_test',
        output_index: 0,
        content_index: 0,
        delta: 'SDK reply',
      },
      {
        type: 'response.output_text.done',
        item_id: 'msg_test',
        output_index: 0,
        content_index: 0,
        text: 'SDK reply',
      },
      { type: 'response.output_item.done', output_index: 0, item: message },
      { type: 'response.completed', response },
    ])
      res.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
    res.end();
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  cleanup.push(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
  const port = (server.address() as { port: number }).port;
  const url = `http://127.0.0.1:${port}/v1`;
  if (process.env.CODEX_TEST_GATEWAY_BINARY) {
    try {
      const proxy = await startNativeGateway(process.env.CODEX_TEST_GATEWAY_BINARY, url);
      cleanup.push(proxy.close);
      return { requests, url: proxy.url, apiKey: proxy.apiKey, close };
    } catch (error) {
      await close();
      throw error;
    }
  }
  return { requests, url, apiKey, close };
}
