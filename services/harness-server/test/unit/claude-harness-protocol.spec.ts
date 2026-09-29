// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { access } from 'node:fs/promises';
import { beforeEach, describe, it, expect, vi } from 'vitest';
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
import { entryToEvent } from '../../src/harness/claude/event-mapper.js';
import { GuardrailPolicyDeniedError, type AgentEvent } from '../../src/harness/agent-harness.js';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';
import {
  buildSandboxWritePolicy,
  createPolicyEnforcedSandbox,
} from '../../src/sandbox/write-policy.js';
import {
  expectCanonicalRootTurn,
  expectRequiredActionContinuation,
} from '../support/model-summary.js';

function buildStubStore(): TranscriptStore {
  return {
    append: async () => [],
    read: (_workspaceId: string, _sessionId: string, _opts: ReadOptions) => {
      void _workspaceId;
      void _sessionId;
      void _opts;
      return emptyAsyncIterable();
    },
    tail: (_workspaceId: string, _sessionId: string, _opts: TailOptions) => {
      void _workspaceId;
      void _sessionId;
      void _opts;
      return emptyAsyncIterable();
    },
    archive: async () => {},
    close: async () => {},
  } satisfies TranscriptStore;
}

function emptyAsyncIterable<T>(): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      return {
        async next(): Promise<IteratorResult<T>> {
          return { done: true, value: undefined as T };
        },
      };
    },
  };
}

function pendingAsyncIterable<T>(): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      return {
        async next(): Promise<IteratorResult<T>> {
          return new Promise(() => {});
        },
      };
    },
  };
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

function controlledAsyncIterable<T>(): {
  iterable: AsyncIterable<T>;
  push: (value: T) => void;
  resolve: () => void;
} {
  const queued: T[] = [];
  const waiters: Array<(result: IteratorResult<T>) => void> = [];
  let done = false;
  return {
    iterable: {
      [Symbol.asyncIterator](): AsyncIterator<T> {
        return {
          next(): Promise<IteratorResult<T>> {
            const value = queued.shift();
            if (value !== undefined) return Promise.resolve({ done: false, value });
            if (done) return Promise.resolve({ done: true, value: undefined as T });
            return new Promise((resolve) => waiters.push(resolve));
          },
        };
      },
    },
    push: (value) => {
      if (done) throw new Error('cannot push after the iterable is resolved');
      const waiter = waiters.shift();
      if (waiter) waiter({ done: false, value });
      else queued.push(value);
    },
    resolve: () => {
      done = true;
      for (const waiter of waiters.splice(0)) {
        waiter({ done: true, value: undefined as T });
      }
    },
  };
}

/** An async iterable whose iterator hangs until `crash(err)` rejects it. */
function rejectableAsyncIterable<T>(): { iterable: AsyncIterable<T>; crash: (err: Error) => void } {
  let rejectNext: ((err: Error) => void) | null = null;
  const gate = new Promise<never>((_, reject) => {
    rejectNext = reject;
  });
  return {
    iterable: {
      [Symbol.asyncIterator](): AsyncIterator<T> {
        return {
          async next(): Promise<IteratorResult<T>> {
            await gate; // rejects when crash() is called
            return { done: true, value: undefined as T };
          },
        };
      },
    },
    crash: (err: Error) => rejectNext?.(err),
  };
}

function buildHarness(
  overrides: Partial<ConstructorParameters<typeof ClaudeAgentSdkHarness>[0]> = {},
): ClaudeAgentSdkHarness {
  return new ClaudeAgentSdkHarness({
    apiKey: 'unused',
    modelDefault: 'fake',
    adapter: new ClaudeAgentSdkAdapter(buildStubStore(), 'ws_test'),
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
    ...overrides,
  });
}

function arrayAsyncIterable<T>(items: T[]): AsyncIterable<T> {
  return {
    async *[Symbol.asyncIterator](): AsyncGenerator<T> {
      for (const item of items) yield item;
    },
  };
}

/** Store whose `read()` replays a fixed set of events (e.g. a prior turn's transcript). */
function buildStoreReturning(events: Event[]): TranscriptStore {
  return {
    append: async () => [],
    read: (_workspaceId: string, _sessionId: string, _opts: ReadOptions) => {
      void _workspaceId;
      void _sessionId;
      void _opts;
      return arrayAsyncIterable(events);
    },
    tail: (_workspaceId: string, _sessionId: string, _opts: TailOptions) => {
      void _workspaceId;
      void _sessionId;
      void _opts;
      return emptyAsyncIterable();
    },
    archive: async () => {},
    close: async () => {},
  } satisfies TranscriptStore;
}

function buildStoreThrowingRead(): TranscriptStore {
  return {
    append: async () => [],
    read: (_workspaceId: string, _sessionId: string, _opts: ReadOptions) => {
      void _workspaceId;
      void _sessionId;
      void _opts;
      throw new Error('store read failed');
    },
    tail: (_workspaceId: string, _sessionId: string, _opts: TailOptions) => {
      void _workspaceId;
      void _sessionId;
      void _opts;
      return emptyAsyncIterable();
    },
    archive: async () => {},
    close: async () => {},
  } satisfies TranscriptStore;
}

function harnessWithStore(store: TranscriptStore): ClaudeAgentSdkHarness {
  return new ClaudeAgentSdkHarness({
    apiKey: 'unused',
    modelDefault: 'fake',
    adapter: new ClaudeAgentSdkAdapter(store, 'ws_test'),
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
  });
}

// Turn-framing / observability events that wrap every turn. These tests
// assert the tool/confirmation content flow, so nextEvent() skips the framing to
// keep the assertions focused; a dedicated spec covers the framing explicitly.
const FRAMING_EVENT_KINDS = new Set<string>([
  'session.status_running',
  'span.model_request_start',
  'span.model_request_end',
  'span.outcome_evaluation_start',
  'span.outcome_evaluation_ongoing',
  'span.outcome_evaluation_end',
  'agent.usage',
]);

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

async function nextEvent(harness: ClaudeAgentSdkHarness): Promise<AgentEvent> {
  const iter = harness.events()[Symbol.asyncIterator]();
  for (;;) {
    const next = await iter.next();
    expect(next.done).toBe(false);
    if (!FRAMING_EVENT_KINDS.has(next.value.kind)) return next.value;
  }
}

describe('ClaudeAgentSdkHarness managed-agent protocol', () => {
  beforeEach(() => {
    queryMock.mockReset();
  });

  it('does not resume the SDK session on the first turn (no prior transcript)', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'turn one' }] },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as Record<string, unknown>;
    expect(options.sessionId).toEqual(expect.any(String));
    expect(options.resume).toBeUndefined();
  });

  it('awaits user.message acceptance before starting the model turn', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    let releaseAcceptance: (() => void) | null = null;
    const acceptance = new Promise<void>((resolve) => {
      releaseAcceptance = resolve;
    });
    const onAccepted = vi.fn(() => acceptance);

    const submit = harness.submit(
      {
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'turn one' }] },
      },
      { onAccepted },
    );

    await vi.waitFor(() => expect(onAccepted).toHaveBeenCalledTimes(1));
    expect(queryMock).not.toHaveBeenCalled();
    releaseAcceptance?.();
    await submit;
    expect(queryMock).toHaveBeenCalledTimes(1);
  });

  it('does not start a user.message turn when acceptance persistence fails', async () => {
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });

    await expect(
      harness.submit(
        {
          kind: 'user.message',
          payload: { content: [{ type: 'text', text: 'turn one' }] },
        },
        { onAccepted: async () => await Promise.reject(new Error('marker unavailable')) },
      ),
    ).rejects.toThrow('marker unavailable');
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('rejects unsupported empty message content before acceptance', async () => {
    const harness = buildHarness();
    const onAccepted = vi.fn(async () => {});
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });

    await expect(
      harness.submit(
        {
          kind: 'user.message',
          payload: { content: [{ type: 'text', text: '' }] },
        },
        { onAccepted },
      ),
    ).rejects.toThrow('supported non-empty text content');
    expect(onAccepted).not.toHaveBeenCalled();
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('joins user text blocks with newlines when building the model prompt', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });

    await harness.submit({
      kind: 'user.message',
      payload: {
        content: [
          { type: 'text', text: 'first block' },
          { type: 'image', source: { type: 'base64', data: 'ignored' } },
          { type: 'text', text: '' },
          { type: 'text', text: 'second block' },
        ],
      },
    });

    expect(queryMock.mock.calls[0]?.[0]?.prompt).toBe('first block\nsecond block');
  });

  it('applies a companion system.message as privileged context for the turn', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable()).mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: { system: 'Agent instructions.' },
    });

    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'turn one' }] },
      systemMessage: {
        content: [
          { type: 'text', text: 'Treat this turn as a production incident.' },
          { type: 'image', source: { type: 'base64', data: 'ignored' } },
        ],
      },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as Record<string, unknown>;
    expect(options.systemPrompt).toBe(
      'Agent instructions.\n\nTreat this turn as a production incident.',
    );

    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'turn two' }] },
    });
    const nextOptions = queryMock.mock.calls[1]?.[0]?.options as Record<string, unknown>;
    expect(nextOptions.systemPrompt).toBe(
      'Agent instructions.\n\nTreat this turn as a production incident.',
    );
  });

  it('awaits user.define_outcome acceptance before registering and running it', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    let releaseAcceptance: (() => void) | null = null;
    const acceptance = new Promise<void>((resolve) => {
      releaseAcceptance = resolve;
    });
    const onAccepted = vi.fn(() => acceptance);

    const submit = harness.submit(
      {
        kind: 'user.define_outcome',
        payload: { description: 'Tests pass', rubric: 'All tests are green.' },
      },
      { onAccepted },
    );

    await vi.waitFor(() => expect(onAccepted).toHaveBeenCalledTimes(1));
    expect(queryMock).not.toHaveBeenCalled();
    releaseAcceptance?.();
    await submit;
    expect(queryMock).toHaveBeenCalledTimes(1);
  });

  it('resumes the SDK session when the store already holds a prior transcript', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    // Simulate turn 1 having persisted an SDK transcript entry to the store.
    const priorEntry = entryToEvent({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      subpath: '',
      producedBy: 'harness',
      entry: { type: 'user', uuid: '11111111-1111-1111-1111-111111111111' },
    });
    const harness = harnessWithStore(buildStoreReturning([priorEntry]));
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'turn two' }] },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as Record<string, unknown>;
    // On a resumed turn we pass `resume` and MUST NOT also pass `sessionId`:
    // the CLI rejects "--session-id" together with "--resume". `resume` carries
    // the derived SDK session id, so continuity is preserved.
    expect(options.resume).toEqual(expect.any(String));
    expect(options.sessionId).toBeUndefined();
  });

  it('fails the turn when resume-state probing cannot read transcript-store', async () => {
    const harness = harnessWithStore(buildStoreThrowingRead());
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });

    await expect(
      harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'turn two' }] },
      }),
    ).rejects.toThrow(/resume probe failed/);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('does not start an SDK query when stop races a pending resume probe', async () => {
    let releaseProbe!: (hasTranscript: boolean) => void;
    const probe = new Promise<boolean>((resolve) => {
      releaseProbe = resolve;
    });
    const adapter = new ClaudeAgentSdkAdapter(buildStubStore(), 'ws_test');
    const probeSpy = vi.spyOn(adapter, 'hasClaudeTranscript').mockReturnValue(probe);
    const harness = new ClaudeAgentSdkHarness({
      apiKey: 'unused',
      modelDefault: 'fake',
      adapter,
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
    });
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    const configDir = (harness as unknown as { claudeConfigDir?: string }).claudeConfigDir;
    expect(configDir).toEqual(expect.any(String));
    await expect(access(configDir!)).resolves.toBeUndefined();

    const submit = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'race shutdown' }] },
    });
    await vi.waitFor(() => expect(probeSpy).toHaveBeenCalledTimes(1));
    let stopSettled = false;
    const stop = harness.stop('replica.shutting_down').finally(() => {
      stopSettled = true;
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(stopSettled).toBe(false);
    await expect(access(configDir!)).resolves.toBeUndefined();

    releaseProbe(false);

    await expect(submit).resolves.toBe('submitted');
    await expect(stop).resolves.toBeUndefined();
    expect(queryMock).not.toHaveBeenCalled();
    await expect(access(configDir!)).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('does not bypass SDK permissions for managed-agent turns', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });

    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'hello' }] },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as Record<string, unknown>;
    expect(options.permissionMode).toBe('default');
    expect(options.settingSources).toEqual([]);
    expect(options.settings).toEqual({ fastMode: false });
    expect(options).not.toHaveProperty('allowDangerouslySkipPermissions');
    expect(options.canUseTool).toEqual(expect.any(Function));
  });

  it('registers child-only read while keeping it out of the primary tool set', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    const rawSandbox = await new InMemorySandboxRuntime().acquire({});
    await rawSandbox.files.write(
      '/workspace/skills/worker/SKILL.md',
      Buffer.from('CHILD_SKILL_READ_MARKER', 'utf8'),
    );
    const sandbox = await createPolicyEnforcedSandbox(
      rawSandbox,
      buildSandboxWritePolicy([], { includeSkillsRoot: true }),
    );
    try {
      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {
          system: 'PRIMARY_CATALOG_MARKER',
          tool_permission_policies: {
            mcp__orca__read: 'always_deny',
          },
          multiagent: {
            type: 'coordinator',
            agents: [
              {
                type: 'agent',
                id: 'agt_worker',
                version: 3,
                name: 'Worker Agent',
                model_id: 'claude-sonnet-4-5-20250929',
                system: 'Read /workspace/skills/worker/SKILL.md before working.',
                allowed_tool_names: ['read'],
                tool_permission_policies: {
                  mcp__orca__read: 'always_allow',
                },
              },
              {
                type: 'agent',
                id: 'agt_observer',
                version: 1,
                name: 'No Tools',
                allowed_tool_names: [],
              },
              {
                type: 'agent',
                id: 'agt_denied_reader',
                version: 1,
                name: 'Denied Reader',
                allowed_tool_names: ['read'],
                tool_permission_policies: {
                  mcp__orca__read: 'always_allow',
                },
              },
            ],
          },
        },
        guardrails: [
          {
            id: 'grd_child_read',
            name: 'Block one child reader',
            tier: 'agent',
            phases: ['tool_call'],
            rule: {
              kind: 'builtin',
              builtin: 'block_tools',
              params: { tools: ['mcp__orca__read'] },
            },
            stateful: false,
            subagentId: 'agt_denied_reader',
          },
        ],
        sandbox,
      });

      await harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'delegate' }] },
        systemMessage: {
          content: [{ type: 'text', text: 'TURN_SYSTEM_MARKER' }],
        },
      });

      const options = queryMock.mock.calls[0]?.[0]?.options as Record<string, unknown>;
      expect(options.tools).toEqual(['Agent']);
      expect(options.allowedTools).toBeUndefined();
      expect(options.forwardSubagentText).toBe(true);
      expect(options.agent).toBe('__orca_primary');
      expect(options.systemPrompt).toBeUndefined();
      expect(options.agents).toEqual({
        'worker-agent': {
          description: 'Managed agent Worker Agent (agt_worker v3)',
          prompt: 'Read /workspace/skills/worker/SKILL.md before working.',
          model: 'claude-sonnet-4-5-20250929',
          tools: ['mcp__orca__read'],
        },
        'no-tools': {
          description: 'Managed agent No Tools (agt_observer v1)',
          prompt: 'You are No Tools.',
          tools: [],
        },
        'denied-reader': {
          description: 'Managed agent Denied Reader (agt_denied_reader v1)',
          prompt: 'You are Denied Reader.',
          tools: ['mcp__orca__read'],
        },
        __orca_primary: {
          description: 'Internal primary managed-agent boundary. Never delegate to this agent.',
          prompt: 'PRIMARY_CATALOG_MARKER\n\nTURN_SYSTEM_MARKER',
          tools: ['Agent'],
        },
      });
      const guard = (
        options.hooks as {
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
        hookSpecificOutput: {
          permissionDecision: 'deny',
        },
      });
      await expect(
        guard(
          {
            hook_event_name: 'PreToolUse',
            tool_name: 'Agent',
            tool_input: { subagent_type: 'worker-agent' },
          },
          'toolu_worker',
          { signal: new AbortController().signal },
        ),
      ).resolves.toMatchObject({
        hookSpecificOutput: {
          hookEventName: 'PreToolUse',
          permissionDecision: 'ask',
        },
      });
      const hooks = options.hooks as {
        SubagentStart: Array<{
          hooks: Array<(input: unknown, id: string, options: unknown) => Promise<unknown>>;
        }>;
        SubagentStop: Array<{
          hooks: Array<(input: unknown, id: string, options: unknown) => Promise<unknown>>;
        }>;
      };
      await hooks.SubagentStart[0]!.hooks[0]!(
        {
          hook_event_name: 'SubagentStart',
          agent_id: 'runtime_reader',
          agent_type: 'worker-agent',
        },
        'hook_reader',
        { signal: new AbortController().signal },
      );
      await hooks.SubagentStart[0]!.hooks[0]!(
        {
          hook_event_name: 'SubagentStart',
          agent_id: 'runtime_denied',
          agent_type: 'denied-reader',
        },
        'hook_denied',
        { signal: new AbortController().signal },
      );
      const canUseTool = options.canUseTool as (
        toolName: string,
        input: Record<string, unknown>,
        options: { signal: AbortSignal; toolUseID: string; agentID?: string },
      ) => Promise<unknown>;
      const permissionContext = {
        signal: new AbortController().signal,
        toolUseID: 'toolu_read',
      };
      await expect(
        canUseTool(
          'mcp__orca__read',
          { path: '/workspace/skills/worker/SKILL.md' },
          {
            ...permissionContext,
            agentID: 'runtime_reader',
          },
        ),
      ).resolves.toMatchObject({ behavior: 'allow' });
      await expect(
        canUseTool(
          'mcp__orca__read',
          { path: '/workspace/skills/worker/SKILL.md' },
          {
            ...permissionContext,
            agentID: 'runtime_denied',
          },
        ),
      ).resolves.toMatchObject({ behavior: 'deny' });
      await expect(
        canUseTool(
          'mcp__orca__read',
          { path: '/workspace/skills/worker/SKILL.md' },
          {
            ...permissionContext,
            agentID: 'runtime_unknown',
          },
        ),
      ).resolves.toMatchObject({ behavior: 'deny' });
      await expect(
        canUseTool(
          'mcp__orca__read',
          { path: '/workspace/skills/worker/SKILL.md' },
          permissionContext,
        ),
      ).resolves.toMatchObject({ behavior: 'deny' });
      await hooks.SubagentStop[0]!.hooks[0]!(
        {
          hook_event_name: 'SubagentStop',
          agent_id: 'runtime_reader',
          agent_type: 'worker-agent',
          stop_hook_active: false,
          agent_transcript_path: '/tmp/reader.jsonl',
        },
        'hook_reader_stop',
        { signal: new AbortController().signal },
      );
      await expect(
        canUseTool(
          'mcp__orca__read',
          { path: '/workspace/skills/worker/SKILL.md' },
          {
            ...permissionContext,
            agentID: 'runtime_reader',
          },
        ),
      ).resolves.toMatchObject({ behavior: 'deny' });
      const orcaServer = (options.mcpServers as Record<string, { instance: { tools: unknown[] } }>)
        .orca;
      const registeredTools = orcaServer.instance.tools as Array<{
        name: string;
        handler: (input: unknown, context: unknown) => Promise<unknown>;
      }>;
      expect(registeredTools.map((tool) => tool.name)).toEqual(['read']);
      await expect(
        registeredTools[0]!.handler({ path: '/workspace/skills/worker/SKILL.md' }, {}),
      ).resolves.toMatchObject({
        content: [
          {
            type: 'text',
            text: expect.stringMatching(/^CHILD_SKILL_READ_MARKER\n\[orca_read /),
          },
        ],
        isError: false,
      });
    } finally {
      await sandbox.destroy();
    }
  });

  it('passes managed-agent roster entries and model controls to the SDK', async () => {
    queryMock.mockReturnValueOnce(
      arrayAsyncIterable([{ type: 'system', subtype: 'init', fast_mode_state: 'on' }]),
    );
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-opus-5',
        model_speed: 'fast',
        model_effort: 'high',
        multiagent: {
          type: 'coordinator',
          agents: [
            {
              type: 'agent',
              id: 'agt_worker',
              version: 3,
              name: 'Worker Agent',
              model_provider: 'anthropic',
              model_id: 'claude-opus-4-8',
              model_speed: 'fast',
              model_effort: 'low',
              system: 'Return the worker marker.',
              allowed_tool_names: ['read'],
            },
          ],
        },
      },
    });

    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'delegate' }] },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as Record<string, unknown>;
    expect(options.tools).toEqual(['Agent']);
    expect(options.allowedTools).toBeUndefined();
    expect(options.forwardSubagentText).toBe(true);
    expect(options.effort).toBe('high');
    expect(options.settings).toEqual({ fastMode: true });
    expect(options.agent).toBe('__orca_primary');
    expect(options.systemPrompt).toBeUndefined();
    expect(options.agents).toEqual({
      'worker-agent': {
        description: 'Managed agent Worker Agent (agt_worker v3)',
        prompt: 'Return the worker marker.',
        model: 'claude-opus-4-8',
        effort: 'low',
        tools: ['mcp__orca__read'],
      },
      __orca_primary: {
        description: 'Internal primary managed-agent boundary. Never delegate to this agent.',
        prompt: '',
        model: 'claude-opus-5',
        effort: 'high',
        tools: ['Agent'],
      },
    });
  });

  it('allows a fast subagent to inherit the primary fast-mode model', async () => {
    queryMock.mockReturnValueOnce(
      arrayAsyncIterable([{ type: 'system', subtype: 'init', fast_mode_state: 'on' }]),
    );
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-opus-5',
        model_speed: 'fast',
        multiagent: {
          type: 'coordinator',
          agents: [
            {
              id: 'agt_worker',
              version: 1,
              name: 'Worker',
              model_speed: 'fast',
            },
          ],
        },
      },
    });

    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'delegate' }] },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as Record<string, unknown>;
    expect(options.settings).toEqual({ fastMode: true });
    expect(options.agent).toBe('__orca_primary');
    expect(options.agents).toEqual({
      worker: {
        description: 'Managed agent Worker (agt_worker v1)',
        prompt: 'You are Worker.',
      },
      __orca_primary: {
        description: 'Internal primary managed-agent boundary. Never delegate to this agent.',
        prompt: '',
        model: 'claude-opus-5',
        tools: ['Agent'],
      },
    });
  });

  it('rejects mixed primary and subagent speeds instead of changing either silently', async () => {
    const harness = buildHarness();
    await expect(
      harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {
          model_provider: 'anthropic',
          model_id: 'claude-opus-5',
          model_speed: 'fast',
          multiagent: {
            type: 'coordinator',
            agents: [
              {
                id: 'agt_worker',
                version: 1,
                name: 'Worker',
                model_speed: 'standard',
              },
            ],
          },
        },
      }),
    ).rejects.toThrow(/mixed model\.speed values are unsupported/);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('rejects model controls on a non-Anthropic provider', async () => {
    const harness = buildHarness();

    await expect(
      harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {
          model_provider: 'openai',
          model_id: 'gpt-5',
          model_speed: 'fast',
        },
      }),
    ).rejects.toThrow(/supported only by the Anthropic Claude harness/);
  });

  it('rejects fast mode for an unsupported model before querying the SDK', async () => {
    const harness = buildHarness();
    await expect(
      harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {
          model_provider: 'anthropic',
          model_id: 'claude-sonnet-4-6',
          model_speed: 'fast',
        },
      }),
    ).rejects.toThrow(/unsupported model claude-sonnet-4-6/);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('rejects effort unsupported by the selected model before querying the SDK', async () => {
    const harness = buildHarness();
    await expect(
      harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {
          model_provider: 'anthropic',
          model_id: 'claude-sonnet-4-6',
          model_effort: 'xhigh',
        },
      }),
    ).rejects.toThrow(/supported levels are low, medium, high, max/);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('fails a fast turn when the SDK reports standard-speed fallback', async () => {
    queryMock.mockReturnValueOnce(
      arrayAsyncIterable([{ type: 'system', subtype: 'init', fast_mode_state: 'off' }]),
    );
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-opus-5',
        model_speed: 'fast',
      },
    });

    await expect(
      harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'use fast mode' }] },
      }),
    ).rejects.toThrow(/fast mode requested.*fast_mode_state="off"/);
  });

  it('fails before forwarding output when the API reports a fast-mode downgrade', async () => {
    queryMock.mockReturnValueOnce(
      arrayAsyncIterable([
        { type: 'system', subtype: 'init', fast_mode_state: 'on' },
        {
          type: 'stream_event',
          event: {
            type: 'message_start',
            message: { usage: { speed: 'standard' } },
          },
        },
      ]),
    );
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-opus-5',
        model_speed: 'fast',
      },
    });

    await expect(
      harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'verify provider speed' }] },
      }),
    ).rejects.toThrow(/usage\.speed="standard"/);
  });

  it('fails when a fast API response omits provider speed evidence', async () => {
    queryMock.mockReturnValueOnce(
      arrayAsyncIterable([
        { type: 'system', subtype: 'init', fast_mode_state: 'on' },
        {
          type: 'stream_event',
          event: { type: 'message_start', message: { usage: {} } },
        },
      ]),
    );
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-opus-5',
        model_speed: 'fast',
      },
    });

    await expect(
      harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'verify provider speed' }] },
      }),
    ).rejects.toThrow(/omitted usage\.speed/);
  });

  it('preserves the provider error when a failed assistant omits usage.speed', async () => {
    queryMock.mockReturnValueOnce(
      (async function* () {
        yield { type: 'system', subtype: 'init', fast_mode_state: 'off' };
        yield {
          type: 'assistant',
          error: 'invalid_request',
          message: { content: [], usage: { speed: null } },
        };
        throw new Error('API Error: 400 fast mode is not enabled upstream');
      })(),
    );
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-opus-5',
        model_speed: 'fast',
      },
    });

    await expect(
      harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'surface provider failure' }] },
      }),
    ).rejects.toThrow(/API Error: 400 fast mode is not enabled upstream/);
  });

  it('preserves a failed result instead of applying fast-mode validation to it', async () => {
    queryMock.mockReturnValueOnce(
      arrayAsyncIterable([
        { type: 'system', subtype: 'init', fast_mode_state: 'on' },
        {
          type: 'result',
          subtype: 'success',
          is_error: true,
          result: 'API Error: 400 upstream fast rejected',
          num_turns: 1,
          usage: { speed: null },
          fast_mode_state: 'off',
        },
      ]),
    );
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        model_provider: 'anthropic',
        model_id: 'claude-opus-5',
        model_speed: 'fast',
      },
    });

    await expect(
      harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'surface result failure' }] },
      }),
    ).resolves.toBe('submitted');

    const iterator = harness.events()[Symbol.asyncIterator]();
    let errorEvent: AgentEvent | undefined;
    while (!errorEvent) {
      const next = await iterator.next();
      expect(next.done).toBe(false);
      if (next.value.kind === 'session.error') errorEvent = next.value;
    }
    expect(errorEvent).toMatchObject({
      payload: {
        error: {
          type: 'processing_error',
          message: 'API Error: 400 upstream fast rejected',
        },
      },
    });
  });

  it('auto-allows tools whose managed-agent policy is always_allow', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        allowed_tool_names: ['read'],
        tool_permission_policies: { mcp__orca__read: 'always_allow' },
      },
    });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'hello' }] },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    await expect(
      options.canUseTool(
        'mcp__orca__read',
        { path: '/workspace/a.txt' },
        {
          signal: new AbortController().signal,
          toolUseID: 'toolu_read',
        },
      ),
    ).resolves.toMatchObject({
      behavior: 'allow',
      updatedInput: { path: '/workspace/a.txt' },
    });
  });

  it('uses orca wildcard policies for built-in MCP tool permission checks', async () => {
    const query = deferredAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { 'mcp__orca__*': 'always_ask' },
      },
    });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'hello' }] },
    });
    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));

    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    const decision = options.canUseTool(
      'mcp__orca__bash',
      { cmd: 'pwd' },
      {
        signal: new AbortController().signal,
        toolUseID: 'toolu_bash',
      },
    );

    const toolUse = await nextEvent(harness);
    expect(toolUse).toMatchObject({
      kind: 'agent.tool_use',
      payload: expect.objectContaining({
        id: expect.stringMatching(/^evt_/),
        name: 'mcp__orca__bash',
        input: { cmd: 'pwd' },
      }),
    });
    const toolUseId = (toolUse.payload as { id: string }).id;
    await expect(nextEvent(harness)).resolves.toMatchObject({
      kind: 'session.status_idle',
      payload: { stop_reason: { type: 'requires_action', event_ids: [toolUseId] } },
    });

    const submit = harness.submit({
      kind: 'user.tool_confirmation',
      payload: { tool_use_id: toolUseId, result: 'deny' },
    });
    await expect(decision).resolves.toMatchObject({
      behavior: 'deny',
      toolUseID: 'toolu_bash',
    });
    query.resolve();
    await submit;
  });

  it('uses exact orca built-in MCP tool policies before wildcard defaults', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: {
          'mcp__orca__*': 'always_allow',
          mcp__orca__bash: 'always_deny',
        },
      },
    });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'hello' }] },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    await expect(
      options.canUseTool(
        'mcp__orca__bash',
        { cmd: 'pwd' },
        {
          signal: new AbortController().signal,
          toolUseID: 'toolu_bash',
        },
      ),
    ).resolves.toMatchObject({
      behavior: 'deny',
      message: expect.stringContaining('denied by managed-agent permission policy'),
      toolUseID: 'toolu_bash',
    });
  });

  it('denies remote MCP tools that were not enabled by the runtime snapshot', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'hello' }] },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    await expect(
      options.canUseTool(
        'mcp__github__create_issue',
        { title: 'Bug' },
        {
          signal: new AbortController().signal,
          toolUseID: 'toolu_issue',
        },
      ),
    ).resolves.toMatchObject({
      behavior: 'deny',
      message: expect.stringContaining('denied by managed-agent permission policy'),
    });
  });

  it('denies tool permission requests whose SDK signal is already aborted', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
      },
    });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'hello' }] },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    const controller = new AbortController();
    controller.abort();

    await expect(
      options.canUseTool(
        'mcp__github__create_issue',
        { title: 'Bug' },
        {
          signal: controller.signal,
          toolUseID: 'toolu_issue',
        },
      ),
    ).resolves.toMatchObject({
      behavior: 'deny',
      message: 'Tool permission request was aborted.',
      toolUseID: 'toolu_issue',
    });
    expect(harness.hasPendingRequiredAction()).toBe(false);
  });

  it('surfaces a terminal session.error + idle when the query crashes while parked on requires_action', async () => {
    const query = rejectableAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
      },
    });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create issue' }] },
    });
    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));

    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    void options.canUseTool(
      'mcp__github__create_issue',
      { title: 'Bug' },
      { signal: new AbortController().signal, toolUseID: 'toolu_issue' },
    );

    await expect(nextEvent(harness)).resolves.toMatchObject({ kind: 'agent.mcp_tool_use' });
    await expect(nextEvent(harness)).resolves.toMatchObject({
      kind: 'session.status_idle',
      payload: { stop_reason: { type: 'requires_action' } },
    });
    expect(harness.hasPendingRequiredAction()).toBe(true);

    // The Claude CLI subprocess dies while awaiting the human decision.
    query.crash(new Error('Claude Code process exited with code 1'));

    // The harness must surface a terminal error + idle and clear the gate so the
    // runner is not wedged (nobody is awaiting the parked query).
    await expect(nextEvent(harness)).resolves.toMatchObject({
      kind: 'session.error',
      payload: { retry_status: { will_retry: false } },
    });
    await expect(nextEvent(harness)).resolves.toMatchObject({
      kind: 'session.status_idle',
      payload: { stop_reason: { type: 'retries_exhausted' } },
    });
    expect(harness.hasPendingRequiredAction()).toBe(false);
  });

  it('does not resume a stale required-action gate after acceptance races with query failure', async () => {
    const query = rejectableAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const harness = buildHarness();
    const events: AgentEvent[] = [];
    const collector = (async () => {
      for await (const event of harness.events()) events.push(event);
    })();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
      },
    });

    const turn = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create issue' }] },
    });
    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    void options.canUseTool(
      'mcp__github__create_issue',
      { title: 'Bug' },
      { signal: new AbortController().signal, toolUseID: 'toolu_issue_race' },
    );

    await vi.waitFor(() =>
      expect(
        events.some(
          (event) =>
            event.kind === 'session.status_idle' &&
            (event.payload as { stop_reason?: { type?: string } }).stop_reason?.type ===
              'requires_action',
        ),
      ).toBe(true),
    );
    await turn;
    const toolUse = events.find((event) => event.kind === 'agent.mcp_tool_use')!;
    const toolUseId = (toolUse.payload as { id: string }).id;

    let releaseAcceptance!: () => void;
    let acceptanceStarted = false;
    const acceptanceGate = new Promise<void>((resolve) => {
      releaseAcceptance = resolve;
    });
    const resume = harness.submit(
      {
        kind: 'user.tool_confirmation',
        payload: { tool_use_id: toolUseId, result: 'allow' },
      },
      {
        onAccepted: () => {
          acceptanceStarted = true;
          return acceptanceGate;
        },
      },
    );
    await vi.waitFor(() => expect(acceptanceStarted).toBe(true));

    query.crash(new Error('Claude Code process exited during acceptance'));
    await vi.waitFor(() =>
      expect(
        events.some(
          (event) =>
            event.kind === 'session.status_idle' &&
            (event.payload as { stop_reason?: { type?: string } }).stop_reason?.type ===
              'retries_exhausted',
        ),
      ).toBe(true),
    );
    const terminalIndex = events.findIndex(
      (event) =>
        event.kind === 'session.status_idle' &&
        (event.payload as { stop_reason?: { type?: string } }).stop_reason?.type ===
          'retries_exhausted',
    );

    releaseAcceptance();
    await expect(resume).resolves.toBe('submitted');
    await new Promise((resolve) => setImmediate(resolve));
    expect(
      events.slice(terminalIndex + 1).some((event) => event.kind === 'session.status_running'),
    ).toBe(false);
    expect(harness.hasPendingRequiredAction()).toBe(false);
    await harness.stop('client.archived');
    await collector;
  });

  it('emits a blocking tool-use event and resolves it from user.tool_confirmation', async () => {
    const query = deferredAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
      },
    });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create issue' }] },
    });

    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    const decision = options.canUseTool(
      'mcp__github__create_issue',
      { title: 'Bug' },
      {
        signal: new AbortController().signal,
        toolUseID: 'toolu_issue',
      },
    );

    const toolUseEvent = await nextEvent(harness);
    expect(toolUseEvent).toMatchObject({
      kind: 'agent.mcp_tool_use',
      payload: expect.objectContaining({
        id: expect.stringMatching(/^evt_/),
        name: 'create_issue',
        mcp_server_name: 'github',
        input: { title: 'Bug' },
      }),
    });
    expect(toolUseEvent.payload).not.toHaveProperty('tool_use_id');
    const idle = await nextEvent(harness);
    expect(idle).toMatchObject({
      kind: 'session.status_idle',
      payload: {
        stop_reason: { type: 'requires_action', event_ids: [expect.stringMatching(/^evt_/)] },
      },
    });
    const toolUseId = (idle.payload as { stop_reason: { event_ids: string[] } }).stop_reason
      .event_ids[0]!;

    let releaseAcceptance: (() => void) | null = null;
    const acceptance = new Promise<void>((resolve) => {
      releaseAcceptance = resolve;
    });
    const onAccepted = vi.fn(() => acceptance);
    let decisionSettled = false;
    void decision.then(() => {
      decisionSettled = true;
    });
    let submitSettled = false;
    const submit = harness
      .submit(
        {
          kind: 'user.tool_confirmation',
          payload: { tool_use_id: toolUseId, result: 'allow' },
        },
        { onAccepted },
      )
      .then(() => {
        submitSettled = true;
      });

    await vi.waitFor(() => expect(onAccepted).toHaveBeenCalledTimes(1));
    expect(decisionSettled).toBe(false);
    releaseAcceptance?.();
    await expect(decision).resolves.toMatchObject({
      behavior: 'allow',
      updatedInput: { title: 'Bug' },
    });
    await Promise.resolve();
    expect(submitSettled).toBe(false);

    query.resolve();
    await submit;
    expect(submitSettled).toBe(true);
  });

  it('persists intended guardrail updates only after an approval', async () => {
    const query = deferredAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const onGuardrailState = vi.fn(async () => {});
    const harness = buildHarness({ onGuardrailState });
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
      },
      guardrails: [
        {
          id: 'grd_calls',
          name: 'Count calls',
          tier: 'workspace',
          phases: ['tool_call'],
          rule: { kind: 'builtin', builtin: 'max_tool_calls_per_session', params: { limit: 10 } },
          stateful: true,
          stateScope: 'session',
        },
      ],
    });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create issue' }] },
    });
    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryOptions() as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    const decision = options.canUseTool(
      'mcp__github__create_issue',
      { title: 'Bug' },
      { signal: new AbortController().signal, toolUseID: 'toolu_issue' },
    );

    const toolUse = await nextEvent(harness);
    const eventId = (toolUse.payload as { id: string }).id;
    await nextEvent(harness);
    expect(onGuardrailState).not.toHaveBeenCalled();

    const confirmation = harness.submit({
      kind: 'user.tool_confirmation',
      payload: { tool_use_id: eventId, result: 'allow' },
    });
    await expect(decision).resolves.toMatchObject({ behavior: 'allow' });
    expect(onGuardrailState).toHaveBeenCalledWith([
      {
        scope: 'session',
        key: 'g:grd_calls:tool_calls',
        action: 'increment',
        value: 1,
      },
    ]);
    const deniedDecision = options.canUseTool(
      'mcp__github__create_issue',
      { title: 'Second bug' },
      { signal: new AbortController().signal, toolUseID: 'toolu_issue_2' },
    );
    const deniedToolUse = await nextEvent(harness);
    const deniedEventId = (deniedToolUse.payload as { id: string }).id;
    await nextEvent(harness);
    const deniedConfirmation = harness.submit({
      kind: 'user.tool_confirmation',
      payload: { tool_use_id: deniedEventId, result: 'deny' },
    });
    await expect(deniedDecision).resolves.toMatchObject({ behavior: 'deny' });
    expect(onGuardrailState).toHaveBeenCalledTimes(1);
    query.resolve();
    await Promise.all([confirmation, deniedConfirmation]);
  });

  it('resets turn-scoped guardrail counters before each new user turn', async () => {
    queryMock.mockReturnValue(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: { tool_permission_policies: { Agent: 'always_allow' } },
      guardrails: [
        {
          id: 'grd_spawns',
          name: 'One dispatch per turn',
          tier: 'workspace',
          phases: ['tool_call'],
          rule: {
            kind: 'builtin',
            builtin: 'spawn_bounds',
            params: { max_dispatches_per_turn: 1 },
          },
          stateful: true,
          stateScope: 'turn',
        },
      ],
    });

    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'one' }] },
    });
    const firstOptions = queryOptions() as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    await expect(
      firstOptions.canUseTool(
        'Agent',
        { description: 'first' },
        { signal: new AbortController().signal, toolUseID: 'toolu_agent_1' },
      ),
    ).resolves.toMatchObject({ behavior: 'allow' });

    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'two' }] },
    });
    const secondOptions = queryOptions() as typeof firstOptions;
    await expect(
      secondOptions.canUseTool(
        'Agent',
        { description: 'second' },
        { signal: new AbortController().signal, toolUseID: 'toolu_agent_2' },
      ),
    ).resolves.toMatchObject({ behavior: 'allow' });
  });

  it('evaluates prepared CEL expressions at the tool-call gate', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__orca__read: 'always_allow' },
      },
      guardrails: [
        {
          id: 'grd_expression',
          name: 'No secret reads',
          tier: 'workspace',
          phases: ['tool_call'],
          rule: {
            kind: 'expression',
            expression: `event.tool.input.path != '/secret.txt'`,
            onFalse: 'deny',
            reason: 'Secret reads are blocked.',
          },
          stateful: false,
        },
      ],
    });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'read a file' }] },
    });
    const options = queryOptions() as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };

    await expect(
      options.canUseTool(
        'mcp__orca__read',
        { path: '/secret.txt' },
        { signal: new AbortController().signal, toolUseID: 'toolu_secret' },
      ),
    ).resolves.toMatchObject({ behavior: 'deny', message: 'Secret reads are blocked.' });
  });

  it('fails closed on a request-phase guardrail before starting the model query', async () => {
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {},
      guardrails: [
        {
          id: 'grd_request',
          name: 'Request gate',
          tier: 'workspace',
          phases: ['request'],
          rule: {
            kind: 'expression',
            expression: `event.session.id == 'ses_allowed'`,
            onFalse: 'deny',
            reason: 'This session cannot start a model turn.',
          },
          stateful: false,
        },
      ],
    });

    const denied = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'hello' }] },
    });
    await expect(denied).rejects.toBeInstanceOf(GuardrailPolicyDeniedError);
    await expect(denied).rejects.toMatchObject({
      reasons: ['This session cannot start a model turn.'],
    });
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('warns that a partially unenforceable guardrail keeps only the phases separate fires', async () => {
    // `separate` wires request/tool_call/tool_result and has no model-endpoint
    // interceptor, so `deny_pii_in_llm_request`'s `llm_request` leg cannot be
    // evaluated. It used to be dropped in silence: an operator configured a
    // PII screen and got only the preview over the user message, which the
    // design calls out as not being a screen at all.
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {},
      guardrails: [
        {
          id: 'grd_pii',
          name: 'PII screen',
          tier: 'workspace',
          phases: ['request', 'llm_request'],
          rule: { kind: 'builtin', builtin: 'deny_pii_in_llm_request' },
          stateful: false,
        },
      ],
    });

    const warning = await nextEvent(harness);
    expect(warning.kind).toBe('session.warning');
    expect(warning.payload).toMatchObject({
      warning: { type: 'guardrail_not_enforced' },
      guardrail_id: 'grd_pii',
      guardrail_name: 'PII screen',
    });
    const { message } = (warning.payload as { warning: { message: string } }).warning;
    expect(message).toContain('llm_request');
    expect(message).toContain('request');
  });

  it('refuses to start when a guardrail has no phase separate can evaluate at all', async () => {
    // Nothing to degrade to here, so the only honest answer is to fail closed
    // rather than run a session under a rule that can never fire.
    const harness = buildHarness();
    await expect(
      harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {},
        guardrails: [
          {
            id: 'grd_only_llm',
            name: 'Outbound screen',
            tier: 'workspace',
            phases: ['llm_request'],
            rule: {
              kind: 'expression',
              expression: `event.session.id == 'ses_allowed'`,
              onFalse: 'deny',
            },
            stateful: false,
          },
        ],
      }),
    ).rejects.toThrow(/Outbound screen \(grd_only_llm\).*llm_request/s);
  });

  it('applies tool-result state before releasing output or evaluating the next tool call', async () => {
    const query = controlledAsyncIterable<unknown>();
    queryMock.mockReturnValueOnce(query.iterable);
    const onGuardrailState = vi.fn(async () => {});
    const harness = buildHarness({ onGuardrailState });
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { 'mcp__google_drive__*': 'always_allow' },
      },
      remoteMcpToolsets: [{ serverName: 'google_drive', permissionPolicy: 'always_allow' }],
      guardrails: [
        {
          id: 'grd_drive',
          name: 'Drive containment',
          tier: 'workspace',
          phases: ['tool_call', 'tool_result'],
          rule: {
            kind: 'builtin',
            builtin: 'gdrive_policy',
            params: { allow_create: true },
          },
          stateful: true,
          stateScope: 'session',
        },
      ],
    });
    const turn = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create and update a file' }] },
    });
    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryOptions() as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    await expect(
      options.canUseTool(
        'mcp__google_drive__create_file',
        { name: 'notes' },
        { signal: new AbortController().signal, toolUseID: 'toolu_create' },
      ),
    ).resolves.toMatchObject({ behavior: 'allow' });

    query.push({
      type: 'assistant',
      message: {
        role: 'assistant',
        content: [
          {
            type: 'tool_use',
            id: 'toolu_create',
            name: 'mcp__google_drive__create_file',
            input: { name: 'notes' },
          },
        ],
        usage: {},
      },
      parent_tool_use_id: null,
      uuid: 'assistant_create',
      session_id: 'sdk_session',
    });
    await expect(nextEvent(harness)).resolves.toMatchObject({ kind: 'agent.mcp_tool_use' });

    query.push({
      type: 'user',
      message: {
        role: 'user',
        content: [
          {
            type: 'tool_result',
            tool_use_id: 'toolu_create',
            content: { id: 'doc-9' },
            is_error: false,
          },
        ],
      },
      uuid: 'result_create',
      session_id: 'sdk_session',
    });
    let toolResult: AgentEvent | undefined;
    for (let i = 0; i < 3 && !toolResult; i += 1) {
      const event = await nextEvent(harness);
      if (event.kind === 'agent.mcp_tool_result') toolResult = event;
    }
    expect(toolResult).toMatchObject({
      kind: 'agent.mcp_tool_result',
      payload: { content: { id: 'doc-9' }, is_error: false },
    });
    expect(onGuardrailState).toHaveBeenCalledWith([
      {
        scope: 'session',
        key: 'g:grd_drive:gdrive_created',
        action: 'append',
        value: 'doc-9',
      },
    ]);
    await expect(
      options.canUseTool(
        'mcp__google_drive__update_file',
        { file_id: 'doc-9' },
        { signal: new AbortController().signal, toolUseID: 'toolu_update' },
      ),
    ).resolves.toMatchObject({ behavior: 'allow' });

    query.resolve();
    await turn;
    await harness.stop('client.archived');
  });

  it.each(['assistant-first', 'permission-first'] as const)(
    'deduplicates tool-use events when the %s path wins the SDK ordering race',
    async (order) => {
      const query = controlledAsyncIterable<unknown>();
      queryMock.mockReturnValueOnce(query.iterable);
      const harness = buildHarness();
      const events: AgentEvent[] = [];
      const collector = (async () => {
        for await (const event of harness.events()) events.push(event);
      })();
      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {
          tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
        },
      });
      const turn = harness.submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'create issue' }] },
      });
      await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));

      const options = queryMock.mock.calls[0]?.[0]?.options as {
        canUseTool: (
          toolName: string,
          input: Record<string, unknown>,
          opts: { signal: AbortSignal; toolUseID: string },
        ) => Promise<unknown>;
      };
      const assistantFrame = {
        type: 'assistant',
        message: {
          role: 'assistant',
          content: [
            {
              type: 'tool_use',
              id: 'toolu_issue',
              name: 'mcp__github__create_issue',
              input: { title: 'Bug' },
            },
          ],
          stop_reason: 'tool_use',
          usage: {},
        },
        parent_tool_use_id: null,
        uuid: 'assistant_tool_use',
        session_id: 'sdk_session',
      };
      let decision: Promise<unknown>;
      if (order === 'assistant-first') {
        query.push(assistantFrame);
        await vi.waitFor(() =>
          expect(events.filter((event) => event.kind === 'agent.mcp_tool_use')).toHaveLength(1),
        );
        decision = options.canUseTool(
          'mcp__github__create_issue',
          { title: 'Bug' },
          {
            signal: new AbortController().signal,
            toolUseID: 'toolu_issue',
          },
        );
      } else {
        decision = options.canUseTool(
          'mcp__github__create_issue',
          { title: 'Bug' },
          {
            signal: new AbortController().signal,
            toolUseID: 'toolu_issue',
          },
        );
        await vi.waitFor(() =>
          expect(events.filter((event) => event.kind === 'agent.mcp_tool_use')).toHaveLength(1),
        );
        query.push(assistantFrame);
      }

      await vi.waitFor(() =>
        expect(
          events.some(
            (event) =>
              event.kind === 'session.status_idle' &&
              (event.payload as { stop_reason?: { type?: string } }).stop_reason?.type ===
                'requires_action',
          ),
        ).toBe(true),
      );
      expect(events.filter((event) => event.kind === 'agent.mcp_tool_use')).toHaveLength(1);
      const toolUseId = (
        events.find((event) => event.kind === 'agent.mcp_tool_use')!.payload as { id: string }
      ).id;
      const confirmation = harness.submit({
        kind: 'user.tool_confirmation',
        payload: { tool_use_id: toolUseId, result: 'allow' },
      });
      await expect(decision).resolves.toMatchObject({ behavior: 'allow' });
      query.resolve();
      await confirmation;
      await turn;
      await harness.stop('client.archived');
      await collector;

      expect(events.filter((event) => event.kind === 'agent.mcp_tool_use')).toHaveLength(1);
    },
  );

  it('resolves a pending confirmation from the deprecated approved alias (allow)', async () => {
    const query = deferredAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
      },
    });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create issue' }] },
    });

    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    const decision = options.canUseTool(
      'mcp__github__create_issue',
      { title: 'Bug' },
      { signal: new AbortController().signal, toolUseID: 'toolu_issue' },
    );

    await nextEvent(harness); // agent.mcp_tool_use
    const idle = await nextEvent(harness);
    const toolUseId = (idle.payload as { stop_reason: { event_ids: string[] } }).stop_reason
      .event_ids[0]!;

    const submit = harness.submit({
      kind: 'user.tool_confirmation',
      payload: { tool_use_id: toolUseId, approved: true },
    });

    await expect(decision).resolves.toMatchObject({
      behavior: 'allow',
      updatedInput: { title: 'Bug' },
    });
    query.resolve();
    await submit;
  });

  it('resolves a pending confirmation from result:deny with a deny_message', async () => {
    const query = deferredAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
      },
    });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create issue' }] },
    });

    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    const decision = options.canUseTool(
      'mcp__github__create_issue',
      { title: 'Bug' },
      { signal: new AbortController().signal, toolUseID: 'toolu_issue' },
    );

    await nextEvent(harness); // agent.mcp_tool_use
    const idle = await nextEvent(harness);
    const toolUseId = (idle.payload as { stop_reason: { event_ids: string[] } }).stop_reason
      .event_ids[0]!;

    const submit = harness.submit({
      kind: 'user.tool_confirmation',
      payload: { tool_use_id: toolUseId, result: 'deny', deny_message: 'nope' },
    });

    await expect(decision).resolves.toMatchObject({
      behavior: 'deny',
      message: 'nope',
    });
    query.resolve();
    await submit;
  });

  it('rejects unknown user.tool_confirmation ids instead of acknowledging them', async () => {
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });

    await expect(
      harness.submit({
        kind: 'user.tool_confirmation',
        payload: { tool_use_id: 'evt_missing_confirmation', result: 'allow' },
      }),
    ).rejects.toMatchObject({
      name: 'UnappliedUserEventError',
      kind: 'user.tool_confirmation',
    });
  });

  it('returns after requires_action while waiting for user.tool_confirmation', async () => {
    queryMock.mockReturnValueOnce(pendingAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
      },
    });
    const submit = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create issue' }] },
    });

    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    void options.canUseTool(
      'mcp__github__create_issue',
      { title: 'Bug' },
      {
        signal: new AbortController().signal,
        toolUseID: 'toolu_issue',
      },
    );

    await expect(nextEvent(harness)).resolves.toMatchObject({ kind: 'agent.mcp_tool_use' });
    await expect(nextEvent(harness)).resolves.toMatchObject({ kind: 'session.status_idle' });
    await submit;
  });

  it('defers user messages when an active turn reaches requires_action', async () => {
    queryMock.mockReturnValueOnce(pendingAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
      },
    });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create issue' }] },
    });

    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    let secondSubmitSettled = false;
    const secondSubmit = harness
      .submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'second message' }] },
      })
      .then((result) => {
        secondSubmitSettled = true;
        return result;
      });
    await Promise.resolve();
    expect(secondSubmitSettled).toBe(false);

    void options.canUseTool(
      'mcp__github__create_issue',
      { title: 'Bug' },
      {
        signal: new AbortController().signal,
        toolUseID: 'toolu_issue',
      },
    );

    await expect(nextEvent(harness)).resolves.toMatchObject({ kind: 'agent.mcp_tool_use' });
    await expect(nextEvent(harness)).resolves.toMatchObject({ kind: 'session.status_idle' });
    await expect(secondSubmit).resolves.toBe('deferred');
  });

  it('submits user messages posted while a non-blocked turn is active', async () => {
    const firstQuery = deferredAsyncIterable();
    queryMock.mockReturnValueOnce(firstQuery.iterable).mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    const firstSubmit = harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'first message' }] },
    });

    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));

    let secondSubmitSettled = false;
    const secondSubmit = harness
      .submit({
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'second message' }] },
      })
      .then(() => {
        secondSubmitSettled = true;
      });
    await Promise.resolve();
    expect(secondSubmitSettled).toBe(false);
    expect(queryMock).toHaveBeenCalledTimes(1);

    firstQuery.resolve();
    await firstSubmit;
    await secondSubmit;

    expect(queryMock).toHaveBeenCalledTimes(2);
    expect(queryMock.mock.calls[1]?.[0]?.prompt).toBe('second message');
  });

  it('aborts the current SDK query on user.interrupt', async () => {
    queryMock.mockReturnValueOnce(pendingAsyncIterable());
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'long running' }] },
    });
    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryMock.mock.calls[0]?.[0]?.options as { abortController: AbortController };

    expect(options.abortController.signal.aborted).toBe(false);
    let releaseAcceptance: (() => void) | null = null;
    const acceptance = new Promise<void>((resolve) => {
      releaseAcceptance = resolve;
    });
    const onAccepted = vi.fn(() => acceptance);
    const interrupt = harness.submit(
      {
        kind: 'user.interrupt',
        payload: { session_thread_id: 'sth_worker' },
      },
      { onAccepted },
    );
    await vi.waitFor(() => expect(onAccepted).toHaveBeenCalledTimes(1));
    expect(options.abortController.signal.aborted).toBe(false);
    releaseAcceptance?.();
    await expect(interrupt).resolves.toBe('submitted');
    expect(options.abortController.signal.aborted).toBe(true);
  });

  it('splits remote MCP tool names at the configured server boundary', async () => {
    queryMock.mockReturnValueOnce(pendingAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { 'mcp__github__*': 'always_ask' },
      },
      remoteMcpToolsets: [{ serverName: 'github', permissionPolicy: 'always_ask' }],
    });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'search issues' }] },
    });

    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    void options.canUseTool(
      'mcp__github__search__issues',
      { q: 'bug' },
      {
        signal: new AbortController().signal,
        toolUseID: 'toolu_search',
      },
    );

    await expect(nextEvent(harness)).resolves.toMatchObject({
      kind: 'agent.mcp_tool_use',
      payload: expect.objectContaining({
        mcp_server_name: 'github',
        name: 'search__issues',
      }),
    });
  });

  it('reports all pending required action ids in the latest idle status', async () => {
    queryMock.mockReturnValueOnce(pendingAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: {
          mcp__github__create_issue: 'always_ask',
          mcp__github__add_label: 'always_ask',
        },
      },
    });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create issue and add label' }] },
    });

    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const options = queryMock.mock.calls[0]?.[0]?.options as {
      canUseTool: (
        toolName: string,
        input: Record<string, unknown>,
        opts: { signal: AbortSignal; toolUseID: string },
      ) => Promise<unknown>;
    };
    void options.canUseTool(
      'mcp__github__create_issue',
      { title: 'Bug' },
      {
        signal: new AbortController().signal,
        toolUseID: 'toolu_issue',
      },
    );
    await expect(nextEvent(harness)).resolves.toMatchObject({ kind: 'agent.mcp_tool_use' });
    const firstIdle = await nextEvent(harness);
    const firstEventId = (firstIdle.payload as { stop_reason: { event_ids: string[] } }).stop_reason
      .event_ids[0]!;

    void options.canUseTool(
      'mcp__github__add_label',
      { label: 'bug' },
      {
        signal: new AbortController().signal,
        toolUseID: 'toolu_label',
      },
    );
    await expect(nextEvent(harness)).resolves.toMatchObject({ kind: 'agent.mcp_tool_use' });
    const secondIdle = await nextEvent(harness);
    const secondEventIds = (secondIdle.payload as { stop_reason: { event_ids: string[] } })
      .stop_reason.event_ids;

    expect(secondEventIds).toHaveLength(2);
    expect(secondEventIds).toContain(firstEventId);
  });

  it('waits for the current turn after user.custom_tool_result when a query is active', async () => {
    const query = deferredAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    void harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'run custom tool' }] },
    });
    await vi.waitFor(() => expect(queryMock).toHaveBeenCalledTimes(1));
    const pending = harness.requestCustomToolUse('lookup_ticket', { id: 123 });
    await expect(nextEvent(harness)).resolves.toMatchObject({ kind: 'agent.custom_tool_use' });
    await expect(nextEvent(harness)).resolves.toMatchObject({ kind: 'session.status_idle' });

    let submitSettled = false;
    const submit = harness
      .submit({
        kind: 'user.custom_tool_result',
        payload: {
          custom_tool_use_id: pending.id,
          content: [{ type: 'text', text: 'done' }],
        },
      })
      .then(() => {
        submitSettled = true;
      });

    await expect(pending.result).resolves.toMatchObject({ custom_tool_use_id: pending.id });
    await Promise.resolve();
    expect(submitSettled).toBe(false);

    query.resolve();
    await submit;
    expect(submitSettled).toBe(true);
  });

  it('resolves pending custom tool uses from user.custom_tool_result without an active query', async () => {
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    const pending = harness.requestCustomToolUse('lookup_ticket', { id: 123 });
    const customToolUseId = pending.id;

    await expect(nextEvent(harness)).resolves.toMatchObject({
      kind: 'agent.custom_tool_use',
      payload: {
        id: customToolUseId,
        name: 'lookup_ticket',
        input: { id: 123 },
      },
    });

    let releaseAcceptance: (() => void) | null = null;
    const acceptance = new Promise<void>((resolve) => {
      releaseAcceptance = resolve;
    });
    const onAccepted = vi.fn(() => acceptance);
    let resultSettled = false;
    void pending.result.then(() => {
      resultSettled = true;
    });
    const submit = harness.submit(
      {
        kind: 'user.custom_tool_result',
        payload: {
          custom_tool_use_id: customToolUseId,
          content: [{ type: 'text', text: 'done' }],
        },
      },
      { onAccepted },
    );

    await vi.waitFor(() => expect(onAccepted).toHaveBeenCalledTimes(1));
    expect(resultSettled).toBe(false);
    releaseAcceptance?.();
    await submit;
    await expect(pending.result).resolves.toEqual({
      custom_tool_use_id: customToolUseId,
      content: [{ type: 'text', text: 'done' }],
    });
  });

  it('resolves self-hosted agent tool uses from user.tool_result', async () => {
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      clientToolExecution: true,
      agentSnapshot: { allowed_tool_names: ['bash'] },
    });
    const pending = harness.requestAgentToolUse('bash', { command: 'pwd' });

    await expect(nextEvent(harness)).resolves.toMatchObject({
      kind: 'agent.tool_use',
      id: pending.id,
      payload: {
        id: pending.id,
        name: 'bash',
        input: { command: 'pwd' },
      },
    });
    await expect(nextEvent(harness)).resolves.toMatchObject({
      kind: 'session.status_idle',
      payload: { stop_reason: { type: 'requires_action', event_ids: [pending.id] } },
    });

    const onAccepted = vi.fn(async () => {});
    await expect(
      harness.submit(
        {
          kind: 'user.tool_result',
          payload: {
            tool_use_id: pending.id,
            content: [{ type: 'text', text: '/workspace' }],
          },
          systemMessage: {
            content: [{ type: 'text', text: 'Treat command output as sensitive.' }],
          },
        },
        { onAccepted },
      ),
    ).resolves.toBe('submitted');
    expect(onAccepted).toHaveBeenCalledTimes(1);
    await expect(pending.result).resolves.toEqual({
      tool_use_id: pending.id,
      content: [
        { type: 'text', text: '/workspace' },
        {
          type: 'text',
          text: '<system-reminder>\nTreat command output as sensitive.\n</system-reminder>',
        },
      ],
    });
    expect(harness.hasPendingRequiredAction()).toBe(false);

    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'continue' }] },
    });
    expect((queryMock.mock.calls[0]?.[0]?.options as Record<string, unknown>).systemPrompt).toBe(
      'Treat command output as sensitive.',
    );
  });

  it('resolves pending custom tool uses when the harness stops', async () => {
    const harness = buildHarness();
    await harness.start({ workspaceId: 'ws_test', sessionId: 'ses_test', agentSnapshot: {} });
    const pending = harness.requestCustomToolUse('lookup_ticket', { id: 123 });

    await harness.stop('replica.shutting_down');

    await expect(pending.result).resolves.toEqual({
      custom_tool_use_id: pending.id,
      result: {
        error: 'Session stopped before custom tool result was received.',
      },
    });
  });

  it('registers custom tools with the SDK even when agent_toolset is disabled', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
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
      },
    });

    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'look up ticket T-123' }] },
    });

    const tools = orcaSdkServerTools(queryOptions());
    expect(tools.map((t) => t.name)).toContain('lookup_ticket');
  });

  it('exempts only declared custom tools from the orca wildcard policy', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        custom_tools: [{ name: 'lookup_ticket' }],
        tool_permission_policies: { 'mcp__orca__*': 'always_deny' },
      },
    });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'look up ticket T-123' }] },
    });

    const canUseTool = queryOptions()['canUseTool'] as (
      toolName: string,
      input: Record<string, unknown>,
      opts: { signal: AbortSignal; toolUseID: string },
    ) => Promise<unknown>;
    await expect(
      canUseTool(
        'mcp__orca__lookup_ticket',
        { ticket_id: 'T-123' },
        { signal: new AbortController().signal, toolUseID: 'toolu_declared_custom' },
      ),
    ).resolves.toMatchObject({ behavior: 'allow', toolUseID: 'toolu_declared_custom' });
    await expect(
      canUseTool(
        'mcp__orca__lookup_ticket_typo',
        { ticket_id: 'T-123' },
        { signal: new AbortController().signal, toolUseID: 'toolu_unknown_orca' },
      ),
    ).resolves.toMatchObject({
      behavior: 'deny',
      toolUseID: 'toolu_unknown_orca',
      message: expect.stringContaining('denied by managed-agent permission policy'),
    });
    await harness.stop('client.archived');
  });

  it('turns a custom SDK tool call into agent.custom_tool_use and waits for user.custom_tool_result', async () => {
    const query = deferredAsyncIterable();
    queryMock.mockReturnValueOnce(query.iterable);
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
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

    const submit = harness.submit({
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

    const lookup = orcaSdkServerTools(options).find((t) => t.name === 'lookup_ticket');
    expect(lookup).toBeDefined();

    let handlerSettled = false;
    const resultPromise = lookup!.handler({ ticket_id: 'T-123' }, {}).then((result) => {
      handlerSettled = true;
      return result;
    });

    const toolUse = await nextEvent(harness);
    expect(toolUse).toMatchObject({
      kind: 'agent.custom_tool_use',
      payload: { name: 'lookup_ticket', input: { ticket_id: 'T-123' } },
    });
    const customToolUseId = (toolUse.payload as { id: string }).id;
    await expect(nextEvent(harness)).resolves.toMatchObject({
      kind: 'session.status_idle',
      payload: { stop_reason: { type: 'requires_action', event_ids: [customToolUseId] } },
    });
    await submit;
    await Promise.resolve();
    expect(handlerSettled).toBe(false);

    const resultSubmit = harness.submit({
      kind: 'user.custom_tool_result',
      payload: {
        custom_tool_use_id: customToolUseId,
        content: [{ type: 'text', text: 'Ticket T-123 is open.' }],
      },
    });

    await expect(resultPromise).resolves.toEqual({
      content: [{ type: 'text', text: 'Ticket T-123 is open.' }],
      isError: false,
    });
    query.resolve();
    await resultSubmit;
  });

  it.each(['assistant-first', 'permission-first'] as const)(
    'keeps custom SDK tool frames out of generic tool events when the %s path wins',
    async (order) => {
      const query = controlledAsyncIterable<unknown>();
      queryMock.mockReturnValueOnce(query.iterable);
      const harness = buildHarness();
      const events: AgentEvent[] = [];
      const collector = (async () => {
        for await (const event of harness.events()) events.push(event);
      })();
      await harness.start({
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        agentSnapshot: {
          custom_tools: [{ name: 'lookup_ticket' }],
          tool_permission_policies: { 'mcp__orca__*': 'always_ask' },
        },
      });

      const turn = harness.submit({
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
      const assistantToolUse = {
        type: 'assistant',
        message: {
          role: 'assistant',
          content: [
            { type: 'text', text: 'custom tool requested' },
            {
              type: 'tool_use',
              id: 'toolu_custom_1',
              name: 'mcp__orca__lookup_ticket',
              input: { ticket_id: 'T-123' },
            },
          ],
          stop_reason: 'tool_use',
          usage: {},
        },
        parent_tool_use_id: null,
        uuid: 'assistant_custom_tool_use',
        session_id: 'sdk_session',
      };

      let permission: Promise<unknown>;
      if (order === 'assistant-first') {
        query.push(assistantToolUse);
        await vi.waitFor(() =>
          expect(
            events.some(
              (event) =>
                event.kind === 'agent.message' &&
                JSON.stringify(event.payload).includes('custom tool requested'),
            ),
          ).toBe(true),
        );
        permission = canUseTool(
          'mcp__orca__lookup_ticket',
          { ticket_id: 'T-123' },
          { signal: new AbortController().signal, toolUseID: 'toolu_custom_1' },
        );
      } else {
        permission = canUseTool(
          'mcp__orca__lookup_ticket',
          { ticket_id: 'T-123' },
          { signal: new AbortController().signal, toolUseID: 'toolu_custom_1' },
        );
        query.push(assistantToolUse);
      }
      await expect(permission).resolves.toMatchObject({
        behavior: 'allow',
        toolUseID: 'toolu_custom_1',
      });

      const lookup = orcaSdkServerTools(options).find((tool) => tool.name === 'lookup_ticket');
      expect(lookup).toBeDefined();
      const handlerResult = lookup!.handler({ ticket_id: 'T-123' }, {});
      await vi.waitFor(() =>
        expect(events.filter((event) => event.kind === 'agent.custom_tool_use')).toHaveLength(1),
      );
      const customToolUse = events.find((event) => event.kind === 'agent.custom_tool_use')!;
      const customToolUseId = (customToolUse.payload as { id: string }).id;
      await vi.waitFor(() =>
        expect(
          events.some(
            (event) =>
              event.kind === 'session.status_idle' &&
              (
                event.payload as { stop_reason?: { event_ids?: string[] } }
              ).stop_reason?.event_ids?.includes(customToolUseId),
          ),
        ).toBe(true),
      );
      await turn;

      const resultSubmit = harness.submit({
        kind: 'user.custom_tool_result',
        payload: {
          custom_tool_use_id: customToolUseId,
          content: [{ type: 'text', text: 'Ticket T-123 is open.' }],
        },
      });
      await expect(handlerResult).resolves.toEqual({
        content: [{ type: 'text', text: 'Ticket T-123 is open.' }],
        isError: false,
      });
      query.push({
        type: 'user',
        message: {
          role: 'user',
          content: [
            {
              type: 'tool_result',
              tool_use_id: 'toolu_custom_1',
              content: 'Ticket T-123 is open.',
              is_error: false,
            },
          ],
        },
        parent_tool_use_id: null,
        uuid: 'custom_tool_result',
        session_id: 'sdk_session',
      });
      query.push({
        type: 'result',
        subtype: 'success',
        stop_reason: 'end_turn',
        usage: {},
        modelUsage: {},
        total_cost_usd: 0,
        is_error: false,
        result: 'done',
        session_id: 'sdk_session',
      });
      query.resolve();
      await resultSubmit;
      await harness.stop('client.archived');
      await collector;

      expect(
        events.filter(
          (event) =>
            event.kind === 'agent.tool_use' &&
            (event.payload as { name?: string }).name === 'mcp__orca__lookup_ticket',
        ),
      ).toHaveLength(0);
      expect(
        events.filter(
          (event) =>
            event.kind === 'agent.tool_result' &&
            (event.payload as { tool_use_id?: string }).tool_use_id === 'toolu_custom_1',
        ),
      ).toHaveLength(0);
      expectCanonicalRootTurn(events, { model: 'fake' });
      expectRequiredActionContinuation(events);
      expect(events.filter((event) => event.kind === 'session.status_running')).toHaveLength(2);
    },
  );

  it('keeps always_ask tools out of SDK allowedTools so canUseTool gates them', async () => {
    queryMock.mockReturnValueOnce(emptyAsyncIterable());
    const harness = buildHarness();
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      agentSnapshot: {
        tool_permission_policies: { mcp__github__create_issue: 'always_ask' },
      },
      remoteMcpToolsets: [{ serverName: 'github', permissionPolicy: 'always_ask' }],
    });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'create issue' }] },
    });

    const options = queryMock.mock.calls[0]?.[0]?.options as Record<string, unknown>;
    expect(options.allowedTools).toBeUndefined();
    expect(options.settingSources).toEqual([]);
    expect(options.canUseTool).toEqual(expect.any(Function));
  });
});
