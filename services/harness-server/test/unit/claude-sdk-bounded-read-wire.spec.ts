// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import { createServer, type ServerResponse } from 'node:http';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ClaudeAgentSdkHarness } from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import {
  buildSandboxWritePolicy,
  createPolicyEnforcedSandbox,
} from '../../src/sandbox/write-policy.js';

const fixturePath = '/mnt/session/outputs/read-wire.txt';
const toolUseId = 'toolu_bounded_read';

afterEach(() => vi.unstubAllEnvs());

describe('Claude SDK bounded agent Read wire', () => {
  it.each([
    { name: 'default page', limit: undefined, expectedLimit: 4096 },
    { name: 'legacy large limit', limit: 100_000, expectedLimit: 8192 },
  ])(
    'keeps $name inline instead of producing another SDK spill',
    async ({ limit, expectedLimit }) => {
      vi.stubEnv('CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC', '1');
      for (const name of [
        'CLAUDE_CODE_OAUTH_TOKEN',
        'CLAUDE_CODE_SESSION_ACCESS_TOKEN',
        'ANTHROPIC_AUTH_TOKEN',
        'ANTHROPIC_CUSTOM_HEADERS',
      ])
        vi.stubEnv(name, undefined);

      const requests: Array<Record<string, unknown>> = [];
      const failures: unknown[] = [];
      const server = createServer(async (request, response) => {
        try {
          let body = '';
          for await (const chunk of request) body += chunk;
          const url = new URL(request.url ?? '/', 'http://127.0.0.1');
          if (url.pathname === '/v1/messages/count_tokens') {
            response.writeHead(200, { 'content-type': 'application/json' });
            // Exercise the real over-limit branch if Read regresses to its old
            // 100 KB output. A constant small count would hide this regression.
            response.end(JSON.stringify({ input_tokens: body.length > 30_000 ? 50_000 : 100 }));
            return;
          }
          if (url.pathname !== '/v1/messages') {
            response.writeHead(404, { 'content-type': 'application/json' });
            response.end('{"error":"not found"}');
            return;
          }
          requests.push(JSON.parse(body) as Record<string, unknown>);
          if (requests.length > 2) throw new Error('Unexpected additional model request');
          writeResponse(response, requests.length, limit);
        } catch (error) {
          failures.push(error);
          if (!response.headersSent) response.writeHead(500);
          response.end();
        }
      });
      await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
      const address = server.address();
      if (!address || typeof address === 'string') throw new Error('Missing fixture address');

      const raw = await new InMemorySandboxRuntime().acquire({});
      const fixture = 'read-wire-'.repeat(15_619).slice(0, 156_186);
      await raw.files.write(fixturePath, Buffer.from(fixture));
      const sandbox = await createPolicyEnforcedSandbox(raw, buildSandboxWritePolicy([]));
      const events: Event[] = [];
      const store: TranscriptStore = {
        append: async (_workspaceId, _sessionId, appended) => {
          events.push(...appended);
          return appended.map((event) => event.id);
        },
        read: (_workspaceId, _sessionId, options) => ({
          async *[Symbol.asyncIterator]() {
            for (const event of events) {
              if (options.subpath !== '*' && (event.subpath ?? '') !== (options.subpath ?? ''))
                continue;
              yield event;
            }
          },
        }),
        tail: () => ({ async *[Symbol.asyncIterator]() {} }),
        archive: async () => {},
        close: async () => {},
      };
      const sessionId = `ses_read_wire_${randomUUID()}`;
      const harness = new ClaudeAgentSdkHarness({
        apiKey: 'local-read-fixture-key',
        baseURL: `http://127.0.0.1:${address.port}`,
        modelDefault: 'claude-sonnet-4-6',
        workspaceId: 'ws_read_wire',
        sessionId,
        adapter: new ClaudeAgentSdkAdapter(store, 'ws_read_wire'),
      });
      const timeout = setTimeout(() => {
        failures.push(new Error('Read wire fixture exceeded its deadline'));
        void harness.stop('idle.timeout').catch((error) => failures.push(error));
      }, 15_000);
      try {
        await harness.start({
          workspaceId: 'ws_read_wire',
          sessionId,
          sandbox,
          agentSnapshot: {
            allowed_tool_names: ['read'],
            tool_permission_policies: { mcp__orca__read: 'always_allow' },
          },
        });
        await harness.submit({
          kind: 'user.message',
          payload: { content: [{ type: 'text', text: 'Read one page of the fixture.' }] },
        });
        expect(failures).toEqual([]);
        expect(requests).toHaveLength(2);
        const messages = requests[1]!['messages'] as Array<{ content?: unknown }>;
        const block = messages
          .flatMap((message) => (Array.isArray(message.content) ? message.content : []))
          .find((item) => item.type === 'tool_result' && item.tool_use_id === toolUseId);
        expect(block).toBeDefined();
        expect(block.is_error).not.toBe(true);
        const text =
          typeof block.content === 'string'
            ? block.content
            : block.content
                .map((item: { type: string; text?: string }) => item.text ?? '')
                .join('');
        const metadata = /\[orca_read (\{[^\n]+\})\]$/.exec(text);
        expect(
          metadata,
          'Read must remain an inline page with continuation metadata',
        ).not.toBeNull();
        expect(JSON.parse(metadata![1]!)).toMatchObject({
          offset: 0,
          limit: expectedLimit,
          bytes_read: expectedLimit,
          total_bytes: 156_186,
          next_offset: expectedLimit,
          truncation: true,
          offset_unit: 'utf8_bytes',
        });
        expect(text.slice(0, expectedLimit)).toBe(fixture.slice(0, expectedLimit));
        expect(Buffer.byteLength(JSON.stringify(block.content), 'utf8')).toBeLessThanOrEqual(
          16_384,
        );
        expect(text).not.toContain('claude-resume-');
        expect(text).not.toContain('Output has been saved to');
        expect(text).not.toContain('<persisted-output>');
      } finally {
        clearTimeout(timeout);
        await harness.stop('idle.timeout');
        await raw.destroy();
        server.closeAllConnections();
        await new Promise<void>((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        );
      }
    },
    20_000,
  );
});

function writeResponse(response: ServerResponse, number: number, limit: number | undefined): void {
  response.writeHead(200, { 'content-type': 'text/event-stream' });
  const send = (type: string, fields: Record<string, unknown>) => {
    response.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...fields })}\n\n`);
  };
  send('message_start', {
    message: {
      id: `msg_read_${number}`,
      type: 'message',
      role: 'assistant',
      model: 'claude-sonnet-4-6',
      content: [],
      stop_reason: null,
      stop_sequence: null,
      usage: {
        input_tokens: 10,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
      },
    },
  });
  if (number === 1) {
    send('content_block_start', {
      index: 0,
      content_block: { type: 'tool_use', id: toolUseId, name: 'mcp__orca__read', input: {} },
    });
    send('content_block_delta', {
      index: 0,
      delta: {
        type: 'input_json_delta',
        partial_json: JSON.stringify({
          path: fixturePath,
          ...(limit === undefined ? {} : { limit }),
        }),
      },
    });
  } else {
    send('content_block_start', { index: 0, content_block: { type: 'text', text: '' } });
    send('content_block_delta', {
      index: 0,
      delta: { type: 'text_delta', text: 'Read complete.' },
    });
  }
  send('content_block_stop', { index: 0 });
  send('message_delta', {
    delta: { stop_reason: number === 1 ? 'tool_use' : 'end_turn', stop_sequence: null },
    usage: { output_tokens: 10 },
  });
  send('message_stop', {});
  response.end();
}
