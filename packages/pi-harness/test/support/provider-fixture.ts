// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer } from 'node:http';
import { createResponsesFixture } from '../../../codex-harness/test/support/responses-fixture.js';
export const MODELS = {
  openai: 'gpt-5.4',
  anthropic: 'claude-sonnet-4-6',
  deepseek: 'deepseek-flash',
  google: 'gemini-2.5-flash',
  zai: 'glm-5.3-flash',
} as const;
export type TestProvider = keyof typeof MODELS;
export async function createProviderFixture(
  provider: TestProvider,
  options: Parameters<typeof createResponsesFixture>[0] & { invalidUsage?: boolean } = {},
) {
  if (provider === 'openai') return createResponsesFixture(options);
  const requests: Array<Record<string, unknown>> = [];
  const apiKey = options.apiKey ?? 'test-gateway-token';
  const server = createServer(async (req, res) => {
    const path =
      provider === 'anthropic'
        ? '/v1/messages'
        : provider === 'google'
          ? `/v1beta/models/${MODELS.google}:streamGenerateContent`
          : '/v1/chat/completions';
    if (
      req.method !== 'POST' ||
      req.url?.split('?')[0] !== path ||
      (provider === 'anthropic'
        ? req.headers['x-api-key'] !== apiKey
        : provider === 'google'
          ? req.headers['x-goog-api-key'] !== apiKey
          : req.headers.authorization !== `Bearer ${apiKey}`) ||
      req.headers['x-orca-session-id'] !== (options.sessionId ?? 'ses_sdk')
    ) {
      res.writeHead(403).end('wrong route or scoped credentials');
      return;
    }
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk);
    const request = JSON.parse(Buffer.concat(chunks).toString());
    requests.push(request);
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    const tool = options.callTool && requests.length === 1;
    const name = options.toolName ?? 'write';
    const args = JSON.stringify(options.toolArguments ?? { path: 'proof.txt', content: 'proof' });
    const send = (event: Record<string, unknown>) =>
      res.write(`${event.type ? `event: ${event.type}\n` : ''}data: ${JSON.stringify(event)}\n\n`);
    if (provider === 'google') {
      send({
        candidates: [
          {
            index: 0,
            content: {
              role: 'model',
              parts: tool
                ? [{ functionCall: { name, args: JSON.parse(args) } }]
                : [{ text: 'SDK reply' }],
            },
            finishReason: 'STOP',
          },
        ],
        usageMetadata: {
          promptTokenCount: tool ? 10 : 20,
          candidatesTokenCount: options.invalidUsage ? -1 : 5,
          cachedContentTokenCount: tool ? 0 : 7,
          totalTokenCount: tool ? 15 : 25,
        },
      });
    } else if (provider === 'anthropic') {
      send({
        type: 'message_start',
        message: {
          id: 'msg_native',
          type: 'message',
          role: 'assistant',
          model: MODELS[provider],
          content: [],
          stop_reason: null,
          stop_sequence: null,
          usage: {
            input_tokens: tool ? 10 : 13,
            output_tokens: 0,
            cache_read_input_tokens: tool ? 0 : 7,
            cache_creation_input_tokens: tool ? 0 : 3,
            cache_creation: {
              ephemeral_5m_input_tokens: tool ? 0 : 2,
              ephemeral_1h_input_tokens: tool ? 0 : 1,
            },
          },
        },
      });
      send({
        type: 'content_block_start',
        index: 0,
        content_block: tool
          ? { type: 'tool_use', id: 'call_test', name, input: {} }
          : { type: 'text', text: '' },
      });
      send({
        type: 'content_block_delta',
        index: 0,
        delta: tool
          ? { type: 'input_json_delta', partial_json: args }
          : { type: 'text_delta', text: 'SDK reply' },
      });
      send({ type: 'content_block_stop', index: 0 });
      send({
        type: 'message_delta',
        delta: { stop_reason: tool ? 'tool_use' : 'end_turn', stop_sequence: null },
        usage: { output_tokens: options.invalidUsage ? -1 : 5 },
      });
      send({ type: 'message_stop' });
    } else {
      const base = {
        id: 'chat_fixture',
        object: 'chat.completion.chunk',
        created: 1,
        model: MODELS[provider],
      };
      send({
        ...base,
        choices: [
          {
            index: 0,
            delta: tool
              ? {
                  role: 'assistant',
                  tool_calls: [
                    {
                      index: 0,
                      id: 'call_test',
                      type: 'function',
                      function: { name, arguments: args },
                    },
                  ],
                }
              : { role: 'assistant', content: 'SDK reply' },
            finish_reason: null,
          },
        ],
      });
      send({
        ...base,
        choices: [{ index: 0, delta: {}, finish_reason: tool ? 'tool_calls' : 'stop' }],
      });
      send({
        ...base,
        choices: [],
        usage: {
          prompt_tokens: tool ? 10 : 20,
          completion_tokens: options.invalidUsage ? -1 : 5,
          total_tokens: tool ? 15 : 25,
          prompt_cache_hit_tokens: tool ? 0 : 7,
        },
      });
      res.write('data: [DONE]\n\n');
    }
    res.end();
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    url: `http://127.0.0.1:${(server.address() as { port: number }).port}/${provider === 'google' ? 'v1beta' : 'v1'}`,
    apiKey,
    requests,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
