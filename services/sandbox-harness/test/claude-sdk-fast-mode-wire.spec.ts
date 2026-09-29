// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, rm } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { query } from '@anthropic-ai/claude-agent-sdk';
import { afterEach, describe, expect, it } from 'vitest';
import { toFrames } from '../src/providers/claude-transformation.js';
import {
  translateFrame,
  type BareEvent,
  type FrameTranslationState,
} from '../src/session-manager.js';

interface CapturedRequest {
  url: string | undefined;
  headers: Record<string, string | string[] | undefined>;
  body: Record<string, unknown>;
}

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

describe('Claude SDK fast-mode wire compatibility', () => {
  it('activates fast mode through a custom base URL and sends required API controls', async () => {
    const captured: CapturedRequest[] = [];
    const server = createServer(async (request, response) => {
      let rawBody = '';
      for await (const chunk of request) rawBody += chunk;
      captured.push({
        url: request.url,
        headers: request.headers,
        body: rawBody === '' ? {} : (JSON.parse(rawBody) as Record<string, unknown>),
      });

      if (request.url === '/api/hello') {
        // A custom gateway may not expose Claude Code's ancillary hello probe.
        // A 404 must not require undocumented org-check bypass flags.
        response.writeHead(404, { 'content-type': 'application/json' });
        response.end('{"error":"not found"}');
        return;
      }

      response.writeHead(200, { 'content-type': 'text/event-stream' });
      writeSse(response, 'message_start', {
        type: 'message_start',
        message: {
          id: 'msg_fast_test',
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
      });
      writeSse(response, 'content_block_start', {
        type: 'content_block_start',
        index: 0,
        content_block: { type: 'text', text: '' },
      });
      writeSse(response, 'content_block_delta', {
        type: 'content_block_delta',
        index: 0,
        delta: { type: 'text_delta', text: 'ok' },
      });
      writeSse(response, 'content_block_stop', { type: 'content_block_stop', index: 0 });
      writeSse(response, 'message_delta', {
        type: 'message_delta',
        delta: { stop_reason: 'end_turn', stop_sequence: null },
        usage: { output_tokens: 2 },
      });
      writeSse(response, 'message_stop', { type: 'message_stop' });
      response.end();
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));

    try {
      const address = server.address();
      if (address === null || typeof address === 'string')
        throw new Error('missing test server port');
      const home = await mkdtemp(join(tmpdir(), 'orca-claude-sdk-fast-'));
      temporaryDirectories.push(home);
      let initState: string | undefined;
      let assistantSpeed: string | undefined;
      let resultSpeed: string | undefined;
      const translated: BareEvent[] = [];
      const translationState: FrameTranslationState = {
        subagentIdByType: new Map(),
        subagentIdByParentToolUseId: new Map(),
        reportedAssistantUsage: false,
      };

      for await (const message of query({
        prompt: 'say ok',
        options: {
          model: 'claude-opus-5',
          settings: { fastMode: true },
          settingSources: [],
          includePartialMessages: true,
          promptSuggestions: false,
          persistSession: false,
          maxTurns: 1,
          cwd: home,
          env: sdkTestEnv(home, `http://127.0.0.1:${address.port}`),
        },
      })) {
        if (message.type === 'system' && message.subtype === 'init') {
          initState = message.fast_mode_state;
        }
        if (message.type === 'assistant') assistantSpeed = message.message.usage.speed;
        if (message.type === 'result') resultSpeed = message.usage.speed;
        for (const frame of toFrames(message, { sessionId: 'managed-usage-test' })) {
          translated.push(...translateFrame(frame, translationState));
        }
      }

      expect(initState).toBe('on');
      expect(assistantSpeed).toBe('fast');
      expect(resultSpeed).toBe('fast');
      // The real SDK emits the assistant block with output_tokens=0 before
      // message_delta supplies the final count. The colocated wire must report
      // that completed message exactly once, before its idle event.
      expect(translated.filter((event) => event.type === 'agent.usage')).toEqual([
        expect.objectContaining({
          model: 'claude-opus-5',
          usage: expect.objectContaining({ input_tokens: 1, output_tokens: 2 }),
        }),
      ]);
      expect(translated.at(-1)).toMatchObject({
        type: 'session.status_idle',
        usage_already_reported: true,
      });
      const messagesRequest = captured.find((request) => request.url === '/v1/messages?beta=true');
      expect(messagesRequest).toBeDefined();
      expect(messagesRequest?.headers['anthropic-beta']).toContain('fast-mode-2026-02-01');
      expect(messagesRequest?.headers['anthropic-version']).toBe('2023-06-01');
      expect(messagesRequest?.body).toMatchObject({
        model: 'claude-opus-5',
        speed: 'fast',
        stream: true,
      });
    } finally {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  }, 20_000);

  it('applies fast speed and per-agent effort to a delegated subagent request', async () => {
    const captured: CapturedRequest[] = [];
    let requestNumber = 0;
    const server = createServer(async (request, response) => {
      let rawBody = '';
      for await (const chunk of request) rawBody += chunk;
      if (request.url === '/api/hello') {
        response.writeHead(404, { 'content-type': 'application/json' });
        response.end('{"error":"not found"}');
        return;
      }

      const body = rawBody === '' ? {} : (JSON.parse(rawBody) as Record<string, unknown>);
      captured.push({ url: request.url, headers: request.headers, body });
      requestNumber += 1;
      if (requestNumber === 1) {
        writeAgentToolResponse(response);
      } else if (body['model'] === 'claude-opus-4-8') {
        writeTextResponse(response, 'msg_fast_child', 'claude-opus-4-8', 'worker done');
      } else {
        writeTextResponse(response, 'msg_fast_parent_final', 'claude-opus-5', 'parent done');
      }
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));

    try {
      const address = server.address();
      if (address === null || typeof address === 'string')
        throw new Error('missing test server port');
      const home = await mkdtemp(join(tmpdir(), 'orca-claude-sdk-subagent-'));
      temporaryDirectories.push(home);
      let initState: string | undefined;
      let childAssistantSpeed: string | undefined;
      let resultSpeed: string | undefined;

      for await (const message of query({
        prompt: 'delegate now',
        options: {
          model: 'claude-opus-5',
          effort: 'high',
          settings: { fastMode: true },
          agents: {
            worker: {
              description: 'Handles delegated work',
              prompt: 'You are the worker.',
              model: 'claude-opus-4-8',
              effort: 'low',
            },
          },
          tools: ['Agent'],
          forwardSubagentText: true,
          settingSources: [],
          includePartialMessages: true,
          promptSuggestions: false,
          persistSession: false,
          maxTurns: 3,
          title: 'managed fast subagent wire test',
          cwd: home,
          env: sdkTestEnv(home, `http://127.0.0.1:${address.port}`),
        },
      })) {
        if (message.type === 'system' && message.subtype === 'init') {
          initState = message.fast_mode_state;
        }
        if (
          message.type === 'assistant' &&
          message.parent_tool_use_id === 'toolu_agent_fast_test'
        ) {
          childAssistantSpeed = message.message.usage.speed;
        }
        if (message.type === 'result') resultSpeed = message.usage.speed;
      }

      expect(initState).toBe('on');
      expect(childAssistantSpeed).toBe('fast');
      expect(resultSpeed).toBe('fast');
      const messageRequests = captured.filter(
        (request) => request.url === '/v1/messages?beta=true',
      );
      expect(messageRequests).toHaveLength(3);
      for (const request of messageRequests) {
        expect(request.headers['anthropic-beta']).toContain('fast-mode-2026-02-01');
        expect(request.body['speed']).toBe('fast');
      }
      expect(messageRequests.map((request) => request.body['model'])).toEqual([
        'claude-opus-5',
        'claude-opus-4-8',
        'claude-opus-5',
      ]);
      expect(messageRequests[0]!.body['output_config']).toEqual({ effort: 'high' });
      expect(messageRequests[1]!.body['output_config']).toEqual({ effort: 'low' });
      expect(messageRequests[2]!.body['output_config']).toEqual({ effort: 'high' });
    } finally {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  }, 20_000);

  it('keeps standard mode off the fast-mode wire contract', async () => {
    const captured: CapturedRequest[] = [];
    const server = createServer(async (request, response) => {
      let rawBody = '';
      for await (const chunk of request) rawBody += chunk;
      captured.push({
        url: request.url,
        headers: request.headers,
        body: rawBody === '' ? {} : (JSON.parse(rawBody) as Record<string, unknown>),
      });

      if (request.url === '/api/hello') {
        response.writeHead(404, { 'content-type': 'application/json' });
        response.end('{"error":"not found"}');
        return;
      }

      response.writeHead(200, { 'content-type': 'text/event-stream' });
      writeSse(response, 'message_start', {
        type: 'message_start',
        message: {
          id: 'msg_standard_test',
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
            speed: 'standard',
          },
        },
      });
      writeSse(response, 'content_block_start', {
        type: 'content_block_start',
        index: 0,
        content_block: { type: 'text', text: '' },
      });
      writeSse(response, 'content_block_delta', {
        type: 'content_block_delta',
        index: 0,
        delta: { type: 'text_delta', text: 'ok' },
      });
      writeSse(response, 'content_block_stop', { type: 'content_block_stop', index: 0 });
      writeSse(response, 'message_delta', {
        type: 'message_delta',
        delta: { stop_reason: 'end_turn', stop_sequence: null },
        usage: { output_tokens: 2 },
      });
      writeSse(response, 'message_stop', { type: 'message_stop' });
      response.end();
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));

    try {
      const address = server.address();
      if (address === null || typeof address === 'string')
        throw new Error('missing test server port');
      const home = await mkdtemp(join(tmpdir(), 'orca-claude-sdk-standard-'));
      temporaryDirectories.push(home);
      let initState: string | undefined;
      let assistantSpeed: string | undefined;
      let resultSpeed: string | undefined;

      for await (const message of query({
        prompt: 'say ok',
        options: {
          model: 'claude-opus-5',
          settings: { fastMode: false },
          settingSources: [],
          includePartialMessages: true,
          promptSuggestions: false,
          persistSession: false,
          maxTurns: 1,
          cwd: home,
          env: sdkTestEnv(home, `http://127.0.0.1:${address.port}`),
        },
      })) {
        if (message.type === 'system' && message.subtype === 'init') {
          initState = message.fast_mode_state;
        }
        if (message.type === 'assistant') assistantSpeed = message.message.usage.speed;
        if (message.type === 'result') resultSpeed = message.usage.speed;
      }

      expect(initState).not.toBe('on');
      expect(assistantSpeed).toBe('standard');
      expect(resultSpeed).toBe('standard');
      const messagesRequest = captured.find((request) => request.url === '/v1/messages?beta=true');
      expect(messagesRequest).toBeDefined();
      expect(String(messagesRequest?.headers['anthropic-beta'] ?? '')).not.toContain(
        'fast-mode-2026-02-01',
      );
      expect(messagesRequest?.body).not.toHaveProperty('speed');
    } finally {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  }, 20_000);
});

function sdkTestEnv(home: string, baseURL: string): NodeJS.ProcessEnv {
  return {
    ...process.env,
    HOME: home,
    CLAUDE_CONFIG_DIR: join(home, '.claude'),
    ANTHROPIC_BASE_URL: baseURL,
    ANTHROPIC_API_KEY: 'test-key',
    ANTHROPIC_AUTH_TOKEN: undefined,
    ANTHROPIC_CUSTOM_HEADERS: undefined,
    CLAUDE_CODE_SKIP_FAST_MODE_ORG_CHECK: undefined,
    CLAUDE_CODE_USE_BEDROCK: undefined,
    CLAUDE_CODE_USE_FOUNDRY: undefined,
    CLAUDE_CODE_USE_VERTEX: undefined,
    _CLAUDE_CODE_ASSUME_FIRST_PARTY_BASE_URL: undefined,
    ALL_PROXY: undefined,
    HTTPS_PROXY: undefined,
    HTTP_PROXY: undefined,
    all_proxy: undefined,
    https_proxy: undefined,
    http_proxy: undefined,
    NO_PROXY: '127.0.0.1,localhost',
    no_proxy: '127.0.0.1,localhost',
  };
}

function writeAgentToolResponse(response: import('node:http').ServerResponse): void {
  response.writeHead(200, { 'content-type': 'text/event-stream' });
  writeSse(response, 'message_start', fastMessageStart('msg_fast_parent_tool', 'claude-opus-5'));
  writeSse(response, 'content_block_start', {
    type: 'content_block_start',
    index: 0,
    content_block: {
      type: 'tool_use',
      id: 'toolu_agent_fast_test',
      name: 'Agent',
      input: {},
    },
  });
  writeSse(response, 'content_block_delta', {
    type: 'content_block_delta',
    index: 0,
    delta: {
      type: 'input_json_delta',
      partial_json: JSON.stringify({
        description: 'delegate work',
        prompt: 'return worker marker',
        subagent_type: 'worker',
        run_in_background: false,
      }),
    },
  });
  writeSse(response, 'content_block_stop', { type: 'content_block_stop', index: 0 });
  writeSse(response, 'message_delta', {
    type: 'message_delta',
    delta: { stop_reason: 'tool_use', stop_sequence: null },
    usage: { output_tokens: 5 },
  });
  writeSse(response, 'message_stop', { type: 'message_stop' });
  response.end();
}

function writeTextResponse(
  response: import('node:http').ServerResponse,
  messageId: string,
  model: string,
  text: string,
): void {
  response.writeHead(200, { 'content-type': 'text/event-stream' });
  writeSse(response, 'message_start', fastMessageStart(messageId, model));
  writeSse(response, 'content_block_start', {
    type: 'content_block_start',
    index: 0,
    content_block: { type: 'text', text: '' },
  });
  writeSse(response, 'content_block_delta', {
    type: 'content_block_delta',
    index: 0,
    delta: { type: 'text_delta', text },
  });
  writeSse(response, 'content_block_stop', { type: 'content_block_stop', index: 0 });
  writeSse(response, 'message_delta', {
    type: 'message_delta',
    delta: { stop_reason: 'end_turn', stop_sequence: null },
    usage: { output_tokens: 2 },
  });
  writeSse(response, 'message_stop', { type: 'message_stop' });
  response.end();
}

function fastMessageStart(id: string, model: string): Record<string, unknown> {
  return {
    type: 'message_start',
    message: {
      id,
      type: 'message',
      role: 'assistant',
      model,
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
  };
}

function writeSse(
  response: import('node:http').ServerResponse,
  event: string,
  data: unknown,
): void {
  response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}
