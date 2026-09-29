// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type { Event, ReadOptions, TailOptions, TranscriptStore } from '@orca/transcript-store';

const { queryMock } = vi.hoisted(() => ({ queryMock: vi.fn() }));

vi.mock('@anthropic-ai/claude-agent-sdk', () => ({
  query: queryMock,
  createSdkMcpServer: vi.fn((cfg: unknown) => ({ type: 'sdk', instance: cfg })),
  tool: vi.fn((name: string, description: string, inputSchema: unknown, handler: unknown) => ({
    name,
    description,
    inputSchema,
    handler,
  })),
}));

import { ClaudeAgentSdkHarness } from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import { SessionRunner } from '../../src/runner/session-runner.js';

describe('Claude custom tools through SessionRunner (integration)', () => {
  it('persists agent.custom_tool_use from the SDK handler and resolves user.custom_tool_result', async () => {
    queryMock.mockReset();
    const query = deferredAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const store = new RecordingStore();
    const harness = new ClaudeAgentSdkHarness({
      apiKey: 'unused',
      modelDefault: 'fake-model',
      adapter: new ClaudeAgentSdkAdapter(store, 'ws_custom_tool_it'),
      workspaceId: 'ws_custom_tool_it',
      sessionId: 'ses_custom_tool_it',
    });
    const runner = new SessionRunner({
      workspaceId: 'ws_custom_tool_it',
      sessionId: 'ses_custom_tool_it',
      harness,
      store,
    });

    await runner.start({
      agentSnapshot: {
        custom_tools: [
          {
            name: 'lookup_ticket',
            description: 'Look up a support ticket.',
            input_schema: {
              type: 'object',
              properties: { ticket_id: { type: 'string' } },
              required: ['ticket_id'],
            },
          },
        ],
        tool_permission_policies: { 'mcp__orca__*': 'always_ask' },
      },
    });
    const submit = runner.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'look up ticket T-123' }] },
    });
    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));

    const options = queryOptions();
    const canUseTool = options['canUseTool'] as (
      toolName: string,
      input: Record<string, unknown>,
      opts: { signal: AbortSignal; toolUseID: string },
    ) => Promise<unknown>;
    await expect(
      canUseTool(
        'mcp__orca__lookup_ticket',
        { ticket_id: 'T-123' },
        { signal: new AbortController().signal, toolUseID: 'toolu_custom_1' },
      ),
    ).resolves.toMatchObject({ behavior: 'allow', toolUseID: 'toolu_custom_1' });

    const lookup = orcaSdkServerTools(options).find((tool) => tool.name === 'lookup_ticket');
    expect(lookup).toBeDefined();
    let handlerSettled = false;
    const handlerResult = lookup!.handler({ ticket_id: 'T-123' }, {}).then((result) => {
      handlerSettled = true;
      return result;
    });

    await vi.waitFor(() =>
      expect(store.appended.some((event) => event.kind === 'agent.custom_tool_use')).toBe(true),
    );
    expect(store.appended.some((event) => event.kind === 'agent.tool_use')).toBe(false);
    const customToolUse = store.appended.find((event) => event.kind === 'agent.custom_tool_use')!;
    const customPayload = JSON.parse(Buffer.from(customToolUse.payload).toString('utf8')) as {
      id: string;
      name: string;
      input: Record<string, unknown>;
    };
    expect(customToolUse.id).toBe(customPayload.id);
    expect(customPayload).toMatchObject({
      name: 'lookup_ticket',
      input: { ticket_id: 'T-123' },
    });
    expect(handlerSettled).toBe(false);

    const resultSubmit = runner.submit({
      kind: 'user.custom_tool_result',
      payload: {
        custom_tool_use_id: customPayload.id,
        content: [{ type: 'text', text: 'Ticket T-123 is open.' }],
      },
    });
    await expect(handlerResult).resolves.toEqual({
      content: [{ type: 'text', text: 'Ticket T-123 is open.' }],
      isError: false,
    });
    query.resolve();
    await resultSubmit;
    await submit;
    await runner.stop('replica.shutting_down');
  });
});

class RecordingStore implements TranscriptStore {
  readonly appended: Event[] = [];

  async append(workspaceId: string, sessionId: string, events: Event[]): Promise<string[]> {
    const normalized = events.map((event, index) => ({
      ...event,
      workspaceId,
      sessionId,
      seq: this.appended.length + index + 1,
    }));
    this.appended.push(...normalized);
    return events.map((event) => event.id);
  }

  async *read(_workspaceId: string, _sessionId: string, _opts: ReadOptions): AsyncIterable<Event> {
    yield* [];
  }

  async *tail(_workspaceId: string, _sessionId: string, _opts: TailOptions): AsyncIterable<Event> {
    yield* [];
  }

  async archive(_workspaceId: string, _sessionId: string): Promise<void> {}

  async close(): Promise<void> {}
}

function deferredAsyncIterable<T>(): { iterable: AsyncIterable<T>; resolve: () => void } {
  let resolveNext: (() => void) | null = null;
  const done = new Promise<void>((resolve) => {
    resolveNext = resolve;
  });
  return {
    iterable: {
      [Symbol.asyncIterator](): AsyncIterator<T> {
        return {
          async next(): Promise<IteratorResult<T>> {
            await done;
            return { done: true, value: undefined as T };
          },
        };
      },
    },
    resolve: () => resolveNext?.(),
  };
}

function queryOptions(): Record<string, unknown> {
  const call = queryMock.mock.calls.at(-1);
  expect(call).toBeDefined();
  return (call![0] as { options: Record<string, unknown> }).options;
}

function orcaSdkServerTools(options: Record<string, unknown>): Array<{
  name: string;
  handler: (args: Record<string, unknown>, extra: unknown) => Promise<unknown>;
}> {
  const mcpServers = options.mcpServers as
    | Record<string, { instance?: { tools?: unknown } }>
    | undefined;
  const tools = mcpServers?.orca?.instance?.tools;
  expect(Array.isArray(tools)).toBe(true);
  return tools as Array<{
    name: string;
    handler: (args: Record<string, unknown>, extra: unknown) => Promise<unknown>;
  }>;
}
