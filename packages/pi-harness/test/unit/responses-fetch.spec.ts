// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { validatedProviderFetch } from '../../src/responses-fetch.js';

afterEach(() => vi.unstubAllGlobals());
async function consume(payload: string, api = 'openai-responses') {
  const chunks = new TextEncoder().encode(payload);
  vi.stubGlobal(
    'fetch',
    async () =>
      new Response(
        new ReadableStream({
          start(controller) {
            // Split every UTF-8 byte and CRLF boundary as a real network may do.
            for (const byte of chunks) controller.enqueue(new Uint8Array([byte]));
            controller.close();
          },
        }),
        { headers: { 'content-type': 'text/event-stream' } },
      ),
  );
  const response = await validatedProviderFetch(api)('https://fixture.invalid/v1/responses');
  return response.text();
}
it('preserves chunked UTF-8, multiline SSE data and valid cache usage', async () => {
  const payload =
    ': keepalive\r\nevent: response.completed\r\ndata: {"type":"response.completed",\r\ndata: "text":"中文", "response":{"usage":{"input_tokens":8,"output_tokens":2,"input_tokens_details":{"cached_tokens":3,"cache_write_tokens":2}}}}\r\n\r\ndata: [DONE]\r\n\r\n';
  await expect(consume(payload)).resolves.toBe(payload);
});
it('refuses a truncated stream without terminal usage', async () => {
  await expect(consume('data: {"type":"response.created"}\n\n')).rejects.toThrow(
    'terminal usage unavailable',
  );
});
it.each([null, -1, 8])(
  'rejects invalid cached counters before Pi normalizes them: %s',
  async (cached_tokens) => {
    const payload = JSON.stringify({
      type: 'response.completed',
      response: {
        usage: { input_tokens: 5, output_tokens: 2, input_tokens_details: { cached_tokens } },
      },
    });
    await expect(consume(`data: ${payload}\n\n`)).rejects.toThrow('usage');
  },
);

const sse = (event: unknown) => `data: ${JSON.stringify(event)}\n\n`;
it.each([null, -1, 8])('rejects malformed DeepSeek cached input: %s', async (cached) => {
  await expect(
    consume(
      sse({
        choices: [{ finish_reason: 'stop' }],
        usage: { prompt_tokens: 5, completion_tokens: 2, prompt_cache_hit_tokens: cached },
      }),
      'openai-completions',
    ),
  ).rejects.toThrow('usage');
});
it.each([
  { choices: [{ finish_reason: 'stop' }] },
  { choices: [], usage: { prompt_tokens: 5, completion_tokens: 2 } },
])('requires both Chat termination and usage', async (event) => {
  await expect(consume(sse(event), 'openai-completions')).rejects.toThrow(
    'terminal usage unavailable',
  );
});
it('requires Anthropic initial usage before accepting terminal deltas', async () => {
  await expect(
    consume(
      sse({
        type: 'message_delta',
        delta: { stop_reason: 'end_turn' },
        usage: { input_tokens: 1, output_tokens: 1 },
      }) + sse({ type: 'message_stop' }),
      'anthropic-messages',
    ),
  ).rejects.toThrow('initial usage unavailable');
});
it.each([null, -1, 4])('rejects invalid Anthropic one-hour cache creation: %s', async (count) => {
  await expect(
    consume(
      sse({
        type: 'message_start',
        message: {
          usage: {
            input_tokens: 10,
            output_tokens: 0,
            cache_creation_input_tokens: 3,
            cache_creation: { ephemeral_1h_input_tokens: count },
          },
        },
      }),
      'anthropic-messages',
    ),
  ).rejects.toThrow('usage');
});
