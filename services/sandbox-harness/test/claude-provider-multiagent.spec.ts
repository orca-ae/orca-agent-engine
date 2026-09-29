// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AgentDefinition } from '@anthropic-ai/claude-agent-sdk';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SANDBOX_WRITE_POLICY_ENV } from '../src/write-policy.js';
import type { RuntimeAgentDefinitions } from '../src/providers/types.js';

const { queryMock, createSdkMcpServerMock, toolMock } = vi.hoisted(() => ({
  queryMock: vi.fn(),
  createSdkMcpServerMock: vi.fn((cfg: unknown) => ({ type: 'sdk', instance: cfg })),
  toolMock: vi.fn((name: string, description: string, inputSchema: unknown, handler: unknown) => ({
    name,
    description,
    inputSchema,
    handler,
  })),
}));

vi.mock('@anthropic-ai/claude-agent-sdk', () => ({
  query: queryMock,
  createSdkMcpServer: createSdkMcpServerMock,
  tool: toolMock,
}));

// The provider boundary tests run on the host, where the in-container
// /mnt/session/outputs mount does not exist. Alias/mount validation has dedicated
// write-policy coverage; preserve every other policy function for the read tests.
vi.mock('../src/write-policy.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/write-policy.js')>();
  return { ...actual, assertNoWritableAliasToSkills: vi.fn() };
});

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

function sessionSnapshot(sessionId = 'sess_1') {
  return {
    sessionId,
    turns: 0,
    startedAt: Date.now(),
    history: [],
    mcpServers: [],
  };
}

async function drainTurn(
  runtime: ReturnType<
    (typeof import('../src/providers/claude.js'))['claudeProvider']['createRuntime']
  >,
  prompt: string,
  sessionId?: string,
  history: Array<{ role: 'user' | 'assistant'; text: string }> = [],
): Promise<unknown[]> {
  const frames: unknown[] = [];
  for await (const frame of runtime.runTurn({
    prompt,
    content: prompt,
    session: { ...sessionSnapshot(sessionId), history },
  })) {
    frames.push(frame);
  }
  return frames;
}

function customSdkTools(options: Record<string, unknown>): Array<{
  name: string;
  inputSchema: Record<string, { parse: (value: unknown) => unknown }>;
  handler: (args: Record<string, unknown>, extra: unknown) => Promise<unknown>;
}> {
  const mcpServers = options.mcpServers as
    | Record<string, { instance?: { tools?: unknown } }>
    | undefined;
  const tools = mcpServers?.orca?.instance?.tools;
  expect(Array.isArray(tools)).toBe(true);
  return tools as Array<{
    name: string;
    inputSchema: Record<string, { parse: (value: unknown) => unknown }>;
    handler: (args: Record<string, unknown>, extra: unknown) => Promise<unknown>;
  }>;
}

function readPolicyEnv(readonlyPath: string): Record<string, string> {
  return {
    [SANDBOX_WRITE_POLICY_ENV]: JSON.stringify({
      writablePaths: [{ path: '/mnt/session/outputs', kind: 'session_output' }],
      readonlyPaths: [readonlyPath],
    }),
  };
}

function readMetadata(text: string): {
  truncation: boolean;
  next_offset: number | null;
} {
  const prefix = '[orca_read ';
  const start = text.lastIndexOf(prefix);
  expect(start).toBeGreaterThanOrEqual(0);
  expect(text.endsWith(']')).toBe(true);
  return JSON.parse(text.slice(start + prefix.length, -1)) as {
    truncation: boolean;
    next_offset: number | null;
  };
}

describe('claude provider options', () => {
  beforeEach(() => {
    queryMock.mockReset();
    createSdkMcpServerMock.mockClear();
    toolMock.mockClear();
    queryMock.mockImplementation(async function* () {
      yield {
        type: 'result',
        subtype: 'success',
        is_error: false,
        duration_ms: 1,
        duration_api_ms: 1,
        num_turns: 1,
        total_cost_usd: 0,
        usage: {},
        result: 'ok',
      };
    });
  });

  it('resumes the SDK-native session on later turns without replaying history', async () => {
    queryMock
      .mockImplementationOnce(async function* () {
        yield { type: 'assistant', session_id: 'sdk-native-1', message: { content: [] } };
      })
      .mockImplementationOnce(async function* () {
        yield { type: 'result', session_id: 'sdk-native-1', subtype: 'success' };
      });
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({ env: {}, diagnostics: () => {} });

    await drainTurn(runtime, 'first turn');
    await drainTurn(runtime, 'second turn', undefined, [
      { role: 'user', text: 'first turn' },
      { role: 'assistant', text: 'first answer' },
    ]);

    expect(queryMock.mock.calls[0]?.[0]).toMatchObject({ prompt: 'first turn' });
    expect(queryMock.mock.calls[0]?.[0].options).toHaveProperty('persistSession', true);
    expect(queryMock.mock.calls[0]?.[0].options).toHaveProperty('settings', {
      fastMode: false,
    });
    expect(queryMock.mock.calls[0]?.[0].options).not.toHaveProperty('resume');
    expect(queryMock.mock.calls[0]?.[0].options).not.toHaveProperty('sessionId');
    expect(queryMock).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        prompt: 'second turn',
        options: expect.objectContaining({ resume: 'sdk-native-1' }),
      }),
    );
    expect(queryOptions()).not.toHaveProperty('sessionId');
  });

  it('keeps SDK-native session ids isolated between runtimes', async () => {
    queryMock
      .mockImplementationOnce(async function* () {
        yield { type: 'result', session_id: 'sdk-runtime-a', subtype: 'success' };
      })
      .mockImplementationOnce(async function* () {
        yield { type: 'result', session_id: 'sdk-runtime-b', subtype: 'success' };
      })
      .mockImplementationOnce(async function* () {
        yield { type: 'result', session_id: 'sdk-runtime-a', subtype: 'success' };
      })
      .mockImplementationOnce(async function* () {
        yield { type: 'result', session_id: 'sdk-runtime-b', subtype: 'success' };
      });
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtimeA = claudeProvider.createRuntime({ env: {}, diagnostics: () => {} });
    const runtimeB = claudeProvider.createRuntime({ env: {}, diagnostics: () => {} });

    await drainTurn(runtimeA, 'a1', 'sess_a');
    await drainTurn(runtimeB, 'b1', 'sess_b');
    await drainTurn(runtimeA, 'a2', 'sess_a');
    await drainTurn(runtimeB, 'b2', 'sess_b');

    expect(queryMock.mock.calls[2]?.[0]).toMatchObject({ options: { resume: 'sdk-runtime-a' } });
    expect(queryMock.mock.calls[3]?.[0]).toMatchObject({ options: { resume: 'sdk-runtime-b' } });
  });

  it('passes agents, Agent tool exposure, and forwardSubagentText to the SDK', async () => {
    queryMock.mockImplementationOnce(async function* () {
      yield { type: 'system', subtype: 'init', fast_mode_state: 'on' };
      yield {
        type: 'result',
        subtype: 'success',
        is_error: false,
        duration_ms: 1,
        duration_api_ms: 1,
        num_turns: 1,
        total_cost_usd: 0,
        usage: { speed: 'fast' },
        result: 'ok',
        fast_mode_state: 'on',
      };
    });
    const { claudeProvider } = await import('../src/providers/claude.js');
    const agents: RuntimeAgentDefinitions = {
      worker: {
        description: 'Handles delegated work',
        prompt: 'You are the worker.',
        model: 'claude-opus-4-8',
        modelSpeed: 'fast',
        managedAgentId: 'agt_worker',
        effort: 'low',
      },
    };
    const runtime = claudeProvider.createRuntime({
      model: 'claude-opus-5',
      modelSpeed: 'fast',
      modelEffort: 'high',
      permissionMode: 'default',
      agents,
      forwardSubagentText: true,
      env: {},
      diagnostics: () => {},
    });

    for await (const _frame of runtime.runTurn({
      prompt: 'delegate this',
      content: 'delegate this',
      session: {
        sessionId: 'sess_1',
        turns: 0,
        startedAt: Date.now(),
        history: [],
        mcpServers: [],
      },
    })) {
      // Drain the generator so query() is invoked.
    }

    expect(queryMock).toHaveBeenCalledTimes(1);
    const sdkAgents = {
      worker: {
        description: 'Handles delegated work',
        prompt: 'You are the worker.',
        model: 'claude-opus-4-8',
        effort: 'low',
      },
    };
    expect(queryMock.mock.calls[0]?.[0]).toMatchObject({
      prompt: 'delegate this',
      options: {
        model: 'claude-opus-5',
        effort: 'high',
        settings: { fastMode: true },
        agent: '__orca_primary',
        agents: {
          worker: sdkAgents.worker,
          __orca_primary: {
            description: 'Internal primary managed-agent boundary. Never delegate to this agent.',
            prompt: '',
            model: 'claude-opus-5',
            effort: 'high',
            tools: ['Agent'],
          },
        },
        forwardSubagentText: true,
        tools: ['Agent'],
      },
    });
    expect(
      (queryMock.mock.calls[0]?.[0].options.agents as Record<string, unknown>).worker,
    ).not.toHaveProperty('managedAgentId');
  });

  it('registers child-only read without exposing it to the primary agent', async () => {
    const workDir = await mkdtemp(join(tmpdir(), 'orca-child-read-'));
    try {
      const path = join(workDir, 'SKILL.md');
      await writeFile(path, 'CHILD_SKILL_READ_MARKER', 'utf8');
      const { claudeProvider } = await import('../src/providers/claude.js');
      const agents: Record<string, AgentDefinition> = {
        worker: {
          description: 'Uses the child Skill.',
          prompt: 'Read the child Skill entrypoint.',
          tools: ['mcp__orca__read'],
        },
      };
      const runtime = claudeProvider.createRuntime({
        model: 'claude-main-model',
        permissionMode: 'default',
        tools: [],
        allowedTools: [],
        runtimeTools: ['read'],
        agents,
        env: readPolicyEnv(workDir),
        diagnostics: () => {},
      });

      await drainTurn(runtime, 'delegate to the worker');

      expect(queryOptions()).toMatchObject({
        agent: '__orca_primary',
        tools: ['Agent'],
        allowedTools: [],
        agents: {
          worker: agents.worker,
          __orca_primary: {
            tools: ['Agent'],
          },
        },
      });
      const guard = (
        queryOptions().hooks as {
          PreToolUse: Array<{
            hooks: Array<(input: unknown, id: string, options: unknown) => Promise<unknown>>;
          }>;
        }
      ).PreToolUse[0]!.hooks[0]!;
      await expect(
        guard(
          {
            hook_event_name: 'PreToolUse',
            tool_name: 'Agent',
            tool_input: { subagent_type: '__orca_primary' },
          },
          'toolu_self',
          { signal: new AbortController().signal },
        ),
      ).resolves.toMatchObject({
        hookSpecificOutput: { permissionDecision: 'deny' },
      });
      const read = customSdkTools(queryOptions()).find((tool) => tool.name === 'read');
      expect(read).toBeDefined();
      const result = (await read!.handler({ path }, {})) as {
        content: Array<{ text: string }>;
        isError: boolean;
      };
      expect(result).toMatchObject({ isError: false });
      expect(result.content[0]!.text).toBe('CHILD_SKILL_READ_MARKER');
    } finally {
      await rm(workDir, { recursive: true, force: true });
    }
  });

  it('rejects mixed primary and subagent speeds at runtime creation', async () => {
    const { claudeProvider } = await import('../src/providers/claude.js');
    expect(() =>
      claudeProvider.createRuntime({
        model: 'claude-opus-5',
        modelSpeed: 'fast',
        agents: {
          worker: {
            description: 'Handles delegated work',
            prompt: 'You are the worker.',
            modelSpeed: 'standard',
          },
        },
        env: {},
        diagnostics: () => {},
      }),
    ).toThrow(/mixed model\.speed values are unsupported/);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('rejects fast mode for unsupported models at runtime creation', async () => {
    const { claudeProvider } = await import('../src/providers/claude.js');
    expect(() =>
      claudeProvider.createRuntime({
        model: 'claude-sonnet-4-6',
        modelSpeed: 'fast',
        env: {},
        diagnostics: () => {},
      }),
    ).toThrow(/unsupported model claude-sonnet-4-6/);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('rejects effort unsupported by the selected model at runtime creation', async () => {
    const { claudeProvider } = await import('../src/providers/claude.js');
    expect(() =>
      claudeProvider.createRuntime({
        model: 'claude-sonnet-4-6',
        modelEffort: 'xhigh',
        env: {},
        diagnostics: () => {},
      }),
    ).toThrow(/supported levels are low, medium, high, max/);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('rejects set_model when it would disable active fast mode', async () => {
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-opus-5',
      modelSpeed: 'fast',
      env: {},
      diagnostics: () => {},
    });

    expect(() => runtime.setModel?.('claude-sonnet-4-6')).toThrow(
      /unsupported model claude-sonnet-4-6/,
    );
    expect(runtime.model).toBe('claude-opus-5');

    runtime.setModel?.('claude-opus-4-8');
    queryMock.mockImplementationOnce(async function* () {
      yield { type: 'system', subtype: 'init', fast_mode_state: 'on' };
      yield {
        type: 'result',
        subtype: 'success',
        is_error: false,
        usage: { speed: 'fast' },
        result: 'ok',
        fast_mode_state: 'on',
      };
    });

    await drainTurn(runtime, 'continue fast');
    expect(queryOptions()).toMatchObject({
      model: 'claude-opus-4-8',
      settings: { fastMode: true },
    });
  });

  it('rejects set_model when the active effort is unsupported by the replacement', async () => {
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-opus-5',
      modelEffort: 'xhigh',
      env: {},
      diagnostics: () => {},
    });

    expect(() => runtime.setModel?.('claude-sonnet-4-6')).toThrow(
      /supported levels are low, medium, high, max/,
    );
    expect(runtime.model).toBe('claude-opus-5');
  });

  it('fails the turn when requested fast mode is not active', async () => {
    queryMock.mockImplementationOnce(async function* () {
      yield {
        type: 'system',
        subtype: 'init',
        fast_mode_state: 'off',
        fast_mode_disabled_reason: 'model_not_allowed',
      };
    });
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-opus-5',
      modelSpeed: 'fast',
      env: {},
      diagnostics: () => {},
    });

    await expect(drainTurn(runtime, 'use fast mode')).rejects.toThrow(
      /fast_mode_state="off" \(model_not_allowed\)/,
    );
  });

  it('fails before forwarding output when a gateway downgrades fast mode', async () => {
    queryMock.mockImplementationOnce(async function* () {
      yield { type: 'system', subtype: 'init', fast_mode_state: 'on' };
      yield {
        type: 'stream_event',
        event: {
          type: 'message_start',
          message: { usage: { speed: 'standard' } },
        },
      };
      yield {
        type: 'stream_event',
        event: {
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'text_delta', text: 'standard-speed output' },
        },
      };
    });
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-opus-5',
      modelSpeed: 'fast',
      env: {},
      diagnostics: () => {},
    });

    await expect(drainTurn(runtime, 'verify provider speed')).rejects.toThrow(
      /usage\.speed="standard"/,
    );
  });

  it('preserves the provider error when a failed assistant omits usage.speed', async () => {
    queryMock.mockImplementationOnce(async function* () {
      yield { type: 'system', subtype: 'init', fast_mode_state: 'off' };
      yield {
        type: 'assistant',
        error: 'invalid_request',
        message: { content: [], usage: { speed: null } },
      };
      throw new Error('API Error: 400 fast mode is not enabled upstream');
    });
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-opus-5',
      modelSpeed: 'fast',
      env: {},
      diagnostics: () => {},
    });

    await expect(drainTurn(runtime, 'surface provider failure')).rejects.toThrow(
      /API Error: 400 fast mode is not enabled upstream/,
    );
  });

  it('preserves a failed result instead of applying fast-mode validation to it', async () => {
    queryMock.mockImplementationOnce(async function* () {
      yield { type: 'system', subtype: 'init', fast_mode_state: 'off' };
      yield {
        type: 'result',
        subtype: 'success',
        is_error: true,
        result: 'API Error: 400 upstream fast rejected',
        num_turns: 1,
        usage: { speed: null },
        fast_mode_state: 'off',
      };
    });
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-opus-5',
      modelSpeed: 'fast',
      env: {},
      diagnostics: () => {},
    });

    await expect(drainTurn(runtime, 'surface result failure')).resolves.toContainEqual(
      expect.objectContaining({
        type: 'result',
        is_error: true,
        result: 'API Error: 400 upstream fast rejected',
      }),
    );
  });

  it('passes platform system instructions to the SDK', async () => {
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-main-model',
      permissionMode: 'default',
      systemPrompt: 'Write downloadable artifacts to /mnt/session/outputs.',
      env: {},
      diagnostics: () => {},
    });

    for await (const _frame of runtime.runTurn({
      prompt: 'write a poem',
      content: 'write a poem',
      session: {
        sessionId: 'sess_1',
        turns: 0,
        startedAt: Date.now(),
        history: [],
        mcpServers: [],
      },
    })) {
      // Drain the generator so query() is invoked.
    }

    expect(queryOptions().systemPrompt).toBe(
      'Write downloadable artifacts to /mnt/session/outputs.',
    );
  });

  it('restricts built-in tools and auto-allows only the declared tool policies', async () => {
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-main-model',
      permissionMode: 'default',
      tools: ['Read', 'Write'],
      allowedTools: ['Read'],
      env: {},
      diagnostics: () => {},
    });

    for await (const _frame of runtime.runTurn({
      prompt: 'read the file',
      content: 'read the file',
      session: {
        sessionId: 'sess_1',
        turns: 0,
        startedAt: Date.now(),
        history: [],
        mcpServers: [],
      },
    })) {
      // Drain the generator so query() is invoked.
    }

    expect(queryOptions()).toMatchObject({
      tools: ['Read', 'Write'],
      allowedTools: ['Read'],
    });
  });

  it('uses the bounded Orca read tool to reach content after 100k bytes', async () => {
    const workDir = await mkdtemp(join(tmpdir(), 'orca-read-pagination-'));
    try {
      const marker = 'IN_SANDBOX_SKILL_TAIL_MARKER';
      const path = join(workDir, 'SKILL.md');
      await writeFile(path, `${'x'.repeat(100_250)}${marker}`, 'utf8');
      const { claudeProvider } = await import('../src/providers/claude.js');
      const runtime = claudeProvider.createRuntime({
        model: 'claude-main-model',
        permissionMode: 'default',
        tools: [],
        allowedTools: ['mcp__orca__read'],
        runtimeTools: ['read'],
        env: readPolicyEnv(workDir),
        diagnostics: () => {},
      });

      await drainTurn(runtime, 'read the skill');

      expect(queryOptions()).toMatchObject({
        tools: [],
        allowedTools: ['mcp__orca__read'],
        strictMcpConfig: true,
      });
      const read = customSdkTools(queryOptions()).find((tool) => tool.name === 'read');
      expect(read).toBeDefined();
      expect(() => read!.inputSchema.offset!.parse(0)).not.toThrow();
      expect(() => read!.inputSchema.limit!.parse(100_001)).toThrow();

      const first = (await read!.handler({ path }, {})) as {
        content: Array<{ text: string }>;
      };
      const firstMetadata = readMetadata(first.content[0]!.text);
      expect(first).not.toHaveProperty('structuredContent');
      expect(first.content[0]!.text).not.toContain(marker);
      expect(firstMetadata).toMatchObject({
        truncation: true,
        next_offset: 100_000,
      });

      const second = (await read!.handler({ path, offset: firstMetadata.next_offset }, {})) as {
        content: Array<{ text: string }>;
      };
      expect(second.content[0]!.text).toContain(marker);
      expect(readMetadata(second.content[0]!.text)).toMatchObject({
        truncation: false,
        next_offset: null,
      });
    } finally {
      await rm(workDir, { recursive: true, force: true });
    }
  });

  it('registers custom tools with the SDK and waits for user custom_tool_result', async () => {
    const query = deferredAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-main-model',
      permissionMode: 'default',
      customTools: [
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
      env: {},
      diagnostics: () => {},
    });

    const iterator = runtime
      .runTurn({
        prompt: 'look up T-123',
        content: 'look up T-123',
        session: {
          sessionId: 'sess_1',
          turns: 0,
          startedAt: Date.now(),
          history: [],
          mcpServers: [],
        },
      })
      [Symbol.asyncIterator]();
    const nextFrame = iterator.next();
    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));

    const lookup = customSdkTools(queryOptions()).find((t) => t.name === 'lookup_ticket');
    expect(lookup).toBeDefined();
    let handlerSettled = false;
    const handlerResult = lookup!.handler({ ticket_id: 'T-123' }, {}).then((result) => {
      handlerSettled = true;
      return result;
    });

    const customToolUse = await nextFrame;
    expect(customToolUse.value).toMatchObject({
      type: 'custom_tool_use',
      name: 'lookup_ticket',
      input: { ticket_id: 'T-123' },
    });
    expect(handlerSettled).toBe(false);

    runtime.handleCustomToolResult?.({
      custom_tool_use_id: (customToolUse.value as { id: string }).id,
      content: [{ type: 'text', text: 'Ticket T-123 is open.' }],
    });

    await expect(handlerResult).resolves.toEqual({
      content: [{ type: 'text', text: 'Ticket T-123 is open.' }],
      isError: false,
    });

    query.resolve();
    await iterator.next();
  });

  it('suppresses SDK echoes of custom callbacks while preserving ordinary tools and usage', async () => {
    queryMock.mockReturnValueOnce(
      (async function* () {
        yield {
          type: 'assistant',
          message: {
            model: 'claude-main-model',
            usage: { input_tokens: 10, output_tokens: 2 },
            content: [
              { type: 'tool_use', id: 'custom-call', name: 'mcp__orca__lookup_ticket', input: {} },
              { type: 'tool_use', id: 'read-call', name: 'Read', input: { file_path: '/tmp/a' } },
              { type: 'text', text: 'Working' },
            ],
          },
        };
        yield {
          type: 'user',
          message: {
            role: 'user',
            content: [
              { type: 'tool_result', tool_use_id: 'custom-call', content: 'custom result' },
              { type: 'tool_result', tool_use_id: 'read-call', content: 'read result' },
            ],
          },
        };
      })(),
    );
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-main-model',
      permissionMode: 'default',
      env: {},
      diagnostics: () => {},
      customTools: [
        {
          name: 'lookup_ticket',
          description: 'Look up',
          input_schema: { type: 'object', properties: {} },
        },
      ],
    });
    const frames = [];
    for await (const frame of runtime.runTurn({
      prompt: 'lookup',
      content: 'lookup',
      session: sessionSnapshot(),
    }))
      frames.push(frame);
    expect(frames).toMatchObject([
      {
        type: 'assistant',
        message: {
          usage: { input_tokens: 10, output_tokens: 2 },
          content: [
            { type: 'tool_use', id: 'read-call' },
            { type: 'text', text: 'Working' },
          ],
        },
      },
      { type: 'user', message: { content: [{ type: 'tool_result', tool_use_id: 'read-call' }] } },
    ]);
  });

  it('falls back to unknown for custom tool enum schemas with non-primitive members', async () => {
    const { claudeProvider } = await import('../src/providers/claude.js');
    const runtime = claudeProvider.createRuntime({
      model: 'claude-main-model',
      permissionMode: 'default',
      customTools: [
        {
          name: 'lookup_ticket',
          input_schema: {
            type: 'object',
            properties: {
              selector: {
                enum: [{ field: 'status' }],
              },
            },
          },
        },
      ],
      env: {},
      diagnostics: () => {},
    });

    for await (const _frame of runtime.runTurn({
      prompt: 'look up T-123',
      content: 'look up T-123',
      session: {
        sessionId: 'sess_1',
        turns: 0,
        startedAt: Date.now(),
        history: [],
        mcpServers: [],
      },
    })) {
      // Drain the generator so query() is invoked.
    }

    const lookup = customSdkTools(queryOptions()).find((t) => t.name === 'lookup_ticket');
    expect(lookup).toBeDefined();
    expect(() => lookup!.inputSchema.selector!.parse({ field: 'status' })).not.toThrow();
  });
});
