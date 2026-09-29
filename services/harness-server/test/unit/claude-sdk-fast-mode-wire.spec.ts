// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer, type ServerResponse } from 'node:http';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';
import { describe, expect, it } from 'vitest';
import { ClaudeAgentSdkHarness } from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';

describe('separate Claude SDK fast-mode wire compatibility', () => {
  it('keeps fast mode active across a cold SessionStore-backed resume', async () => {
    const requests: Array<{
      url: string | undefined;
      headers: typeof import('node:http').IncomingHttpHeaders;
      body: Record<string, unknown>;
    }> = [];
    let responseNumber = 0;
    const server = createServer(async (request, response) => {
      let rawBody = '';
      for await (const chunk of request) rawBody += chunk;
      requests.push({
        url: request.url,
        headers: request.headers,
        body: rawBody === '' ? {} : (JSON.parse(rawBody) as Record<string, unknown>),
      });
      if (request.url === '/api/hello') {
        // Custom gateways need not implement Claude Code's ancillary hello
        // probe; SDK fast mode must still activate when it returns 404.
        response.writeHead(404, { 'content-type': 'application/json' });
        response.end('{"error":"not found"}');
        return;
      }
      responseNumber += 1;
      writeFastResponse(response, responseNumber);
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));

    const persisted: Event[] = [];
    const store = inMemoryStore(persisted);
    const address = server.address();
    if (address === null || typeof address === 'string')
      throw new Error('missing test server port');
    const buildHarness = (): ClaudeAgentSdkHarness =>
      new ClaudeAgentSdkHarness({
        apiKey: 'test-key',
        baseURL: `http://127.0.0.1:${address.port}`,
        modelDefault: 'claude-opus-5',
        adapter: new ClaudeAgentSdkAdapter(store, 'ws_fast_wire'),
        workspaceId: 'ws_fast_wire',
        sessionId: 'ses_fast_wire',
      });
    let harness = buildHarness();

    try {
      await harness.start({
        workspaceId: 'ws_fast_wire',
        sessionId: 'ses_fast_wire',
        agentSnapshot: {
          model_provider: 'anthropic',
          model_id: 'claude-opus-5',
          model_speed: 'fast',
          model_effort: 'high',
        },
      });
      await harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'first fast turn' }] },
      });
      expect(persisted.length).toBeGreaterThan(0);

      // Simulate runner reap/restart: local SDK files disappear, while Orca's
      // SessionStore transcript remains authoritative for the resumed turn.
      await harness.stop('completed');
      harness = buildHarness();
      await harness.start({
        workspaceId: 'ws_fast_wire',
        sessionId: 'ses_fast_wire',
        agentSnapshot: {
          model_provider: 'anthropic',
          model_id: 'claude-opus-5',
          model_speed: 'fast',
          model_effort: 'high',
        },
      });

      await harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'second fast turn' }] },
      });

      const messageRequests = requests.filter(
        (request) => request.url === '/v1/messages?beta=true',
      );
      expect(messageRequests).toHaveLength(2);
      for (const request of messageRequests) {
        expect(request.headers['anthropic-beta']).toContain('fast-mode-2026-02-01');
        expect(request.body).toMatchObject({
          model: 'claude-opus-5',
          output_config: { effort: 'high' },
          speed: 'fast',
          stream: true,
        });
      }
      const resumedMessages = JSON.stringify(messageRequests[1]!.body['messages']);
      expect(resumedMessages).toContain('first fast turn');
      expect(resumedMessages).toContain('ok 1');
      expect(resumedMessages).toContain('second fast turn');
    } finally {
      await harness.stop('completed');
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  }, 20_000);

  it('preserves an actual HTTP provider error instead of replacing it with speed validation', async () => {
    const server = createServer(async (request, response) => {
      for await (const _chunk of request) {
        // Drain request body so the SDK can reuse/close the connection cleanly.
      }
      if (request.url === '/api/hello') {
        response.writeHead(404, { 'content-type': 'application/json' });
        response.end('{"error":"not found"}');
        return;
      }
      response.writeHead(400, { 'content-type': 'application/json' });
      response.end(
        JSON.stringify({
          type: 'error',
          error: { type: 'invalid_request_error', message: 'upstream fast rejected' },
        }),
      );
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));

    const address = server.address();
    if (address === null || typeof address === 'string')
      throw new Error('missing test server port');
    const harness = new ClaudeAgentSdkHarness({
      apiKey: 'test-key',
      baseURL: `http://127.0.0.1:${address.port}`,
      modelDefault: 'claude-opus-5',
      adapter: new ClaudeAgentSdkAdapter(inMemoryStore([]), 'ws_fast_error'),
      workspaceId: 'ws_fast_error',
      sessionId: 'ses_fast_error',
    });

    try {
      await harness.start({
        workspaceId: 'ws_fast_error',
        sessionId: 'ses_fast_error',
        agentSnapshot: {
          model_provider: 'anthropic',
          model_id: 'claude-opus-5',
          model_speed: 'fast',
        },
      });
      await expect(
        harness.submit({
          kind: 'user.message',
          payload: { content: [{ type: 'text', text: 'trigger provider error' }] },
        }),
      ).rejects.toThrow(/API Error: 400.*upstream fast rejected/);
    } finally {
      await harness.stop('completed');
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  }, 20_000);

  it('keeps the default outcome judge on the same fast model controls', async () => {
    const requests: Array<{
      url: string | undefined;
      headers: typeof import('node:http').IncomingHttpHeaders;
      body: Record<string, unknown>;
    }> = [];
    const server = createServer(async (request, response) => {
      let rawBody = '';
      for await (const chunk of request) rawBody += chunk;
      const body = rawBody === '' ? {} : (JSON.parse(rawBody) as Record<string, unknown>);
      requests.push({ url: request.url, headers: request.headers, body });
      if (request.url === '/api/hello') {
        response.writeHead(404, { 'content-type': 'application/json' });
        response.end('{"error":"not found"}');
        return;
      }
      if (body['stream'] === true) {
        writeFastResponse(response, 1);
        return;
      }
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(
        JSON.stringify({
          type: 'message',
          content: [{ type: 'text', text: '{"achieved":true,"reasoning":"done"}' }],
          usage: { input_tokens: 1, output_tokens: 1, speed: 'fast' },
        }),
      );
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));

    const address = server.address();
    if (address === null || typeof address === 'string')
      throw new Error('missing test server port');
    const harness = new ClaudeAgentSdkHarness({
      apiKey: 'test-key',
      baseURL: `http://127.0.0.1:${address.port}`,
      modelDefault: 'claude-opus-5',
      adapter: new ClaudeAgentSdkAdapter(inMemoryStore([]), 'ws_fast_outcome'),
      workspaceId: 'ws_fast_outcome',
      sessionId: 'ses_fast_outcome',
    });

    try {
      await harness.start({
        workspaceId: 'ws_fast_outcome',
        sessionId: 'ses_fast_outcome',
        agentSnapshot: {
          model_provider: 'anthropic',
          model_id: 'claude-opus-5',
          model_speed: 'fast',
          model_effort: 'high',
        },
      });
      await harness.submit({
        kind: 'user.define_outcome',
        payload: { description: 'finish task', rubric: 'task is complete' },
      });

      const modelRequests = requests.filter((request) => request.url?.startsWith('/v1/messages'));
      expect(modelRequests).toHaveLength(2);
      for (const request of modelRequests) {
        expect(request.headers['anthropic-beta']).toContain('fast-mode-2026-02-01');
        expect(request.body).toMatchObject({
          model: 'claude-opus-5',
          speed: 'fast',
          output_config: { effort: 'high' },
        });
      }
      expect(modelRequests.some((request) => request.body['stream'] === true)).toBe(true);
      expect(modelRequests.some((request) => request.body['stream'] !== true)).toBe(true);
    } finally {
      await harness.stop('completed');
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  }, 20_000);
});

function writeFastResponse(response: ServerResponse, number: number): void {
  response.writeHead(200, { 'content-type': 'text/event-stream' });
  const events: Array<[string, unknown]> = [
    [
      'message_start',
      {
        type: 'message_start',
        message: {
          id: `msg_fast_${number}`,
          type: 'message',
          role: 'assistant',
          model: 'claude-opus-5',
          content: [],
          stop_reason: null,
          stop_sequence: null,
          usage: {
            input_tokens: 1,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            output_tokens: 0,
            service_tier: 'standard',
            speed: 'fast',
          },
        },
      },
    ],
    [
      'content_block_start',
      { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } },
    ],
    [
      'content_block_delta',
      {
        type: 'content_block_delta',
        index: 0,
        delta: { type: 'text_delta', text: `ok ${number}` },
      },
    ],
    ['content_block_stop', { type: 'content_block_stop', index: 0 }],
    [
      'message_delta',
      {
        type: 'message_delta',
        delta: { stop_reason: 'end_turn', stop_sequence: null },
        usage: { output_tokens: 2 },
      },
    ],
    ['message_stop', { type: 'message_stop' }],
  ];
  for (const [event, data] of events) {
    response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  }
  response.end();
}

function inMemoryStore(events: Event[]): TranscriptStore {
  return {
    append: async (_workspaceId, _sessionId, appended) => {
      events.push(...appended);
      return appended.map((event) => event.id);
    },
    read: (_workspaceId: string, _sessionId: string, options: ReadOptions) => ({
      async *[Symbol.asyncIterator](): AsyncGenerator<Event> {
        for (const event of events) {
          if (options.subpath !== '*' && (event.subpath ?? '') !== (options.subpath ?? ''))
            continue;
          yield event;
        }
      },
    }),
    tail: (_workspaceId: string, _sessionId: string, _options: TailOptions) => ({
      async *[Symbol.asyncIterator](): AsyncGenerator<Event> {
        yield* [];
      },
    }),
    archive: async () => {},
    close: async () => {},
  };
}
