// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer, type ServerResponse } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import { describe, expect, it, vi } from 'vitest';
import {
  ClaudeAgentSdkHarness,
  type ClaudeHarnessOptions,
} from '../../src/harness/claude/index.js';
import { ClaudeAgentSdkAdapter } from '../../src/harness/claude/session-adapter.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';
import { InMemorySandboxRuntime } from '../../src/sandbox/in-memory/runtime.js';

// Uses the real pinned SDK subprocess and MCP transport against a local Messages
// API fixture. In particular canUseTool runs independently of the SDK iterator.
describe('Claude SDK final model-call usage and tool permissions', () => {
  it('records final cumulative output and cache usage once across multiple content blocks', async () => {
    const onUsage = vi.fn<NonNullable<ClaudeHarnessOptions['onUsage']>>(async () => undefined);
    const fixture = await startFixture(onUsage, false);
    try {
      await fixture.submit();
      expect(onUsage).toHaveBeenCalledTimes(1);
      expect(onUsage).toHaveBeenCalledWith(
        expectedUsage,
        'orca-actual-model',
        undefined,
        'evt_usage_wire',
        expect.stringMatching(/^evt_/),
      );
    } finally {
      await fixture.close();
    }
  }, 20_000);

  it.each([
    ['allows below the cap', 0.001, true],
    ['denies at the cap', 0.003, false],
    ['denies above the cap', 0.004, false],
  ] as const)(
    '%s only after the proposing message usage ACK',
    async (_name, cost, allowed) => {
      const entered = deferred<void>();
      const ack = deferred<void>();
      const onUsage = vi.fn<NonNullable<ClaudeHarnessOptions['onUsage']>>(async () => {
        entered.resolve();
        await ack.promise;
        return { session_cost_usd: cost, total_tokens: 1500 };
      });
      const fixture = await startFixture(onUsage, true);
      const submitting = fixture.submit();
      try {
        await entered.promise;
        // A delayed ACK must keep the actual MCP file write parked, even though
        // the SDK has already invoked its independent permission callback.
        await delay(150);
        expect(fixture.write).not.toHaveBeenCalled();
        expect(onUsage.mock.calls[0]?.[0]).toEqual(expectedUsage);
        ack.resolve();
        await submitting;
        expect(fixture.write).toHaveBeenCalledTimes(allowed ? 1 : 0);
        if (allowed)
          expect((await fixture.sandbox.files.read('/marker.txt')).toString()).toBe('written');
        expect(onUsage).toHaveBeenCalledTimes(2);
      } finally {
        ack.resolve();
        await submitting.catch(() => undefined);
        await fixture.close();
      }
    },
    20_000,
  );

  it.each([0.001, 0.003])(
    'counts final native subagent usage and gates its tool at child cost %s',
    async (cost) => {
      const entered = deferred<void>();
      const ack = deferred<void>();
      const onUsage = vi.fn<NonNullable<ClaudeHarnessOptions['onUsage']>>(async (_usage, model) => {
        if (model === 'orca-child-actual') {
          entered.resolve();
          await ack.promise;
        }
        return { subagent_cost_agent_worker: cost };
      });
      const fixture = await startFixture(onUsage, true, true);
      const submitting = fixture.submit();
      try {
        await Promise.race([
          entered.promise,
          submitting.then(() => {
            throw new Error(`No child usage: ${JSON.stringify(fixture.observed)}`);
          }),
        ]);
        await delay(150);
        expect(fixture.write).not.toHaveBeenCalled();
        const childCall = onUsage.mock.calls.find((call) => call[1] === 'orca-child-actual')!;
        expect(childCall[0]).toEqual(expectedUsage);
        expect(childCall[2]).toBe('agent_worker');
        ack.resolve();
        await submitting;
        const childCalls = onUsage.mock.calls.filter((call) => call[1] === 'orca-child-actual');
        expect(childCalls).toHaveLength(2);
        expect(
          childCalls.every((call) => call[2] === 'agent_worker' && call[0].output_tokens === 500),
        ).toBe(true);
        expect(onUsage).toHaveBeenCalledTimes(4);
        expect(fixture.write).toHaveBeenCalledTimes(cost < 0.003 ? 1 : 0);
      } finally {
        ack.resolve();
        await submitting.catch(() => undefined);
        await fixture.close();
      }
    },
    20_000,
  );

  it('blocks native Agent dispatch before the SDK sends a child model request', async () => {
    const fixture = await startFixture(async () => ({}), true, true, undefined, 'block');
    try {
      await fixture.submit();
      expect(fixture.callsByRole.child).toBe(0);
      expect(fixture.write).not.toHaveBeenCalled();
      expect(JSON.stringify(fixture.observed)).toContain('native dispatch denied');
    } finally {
      await fixture.close();
    }
  }, 20_000);

  it.each(['allow', 'deny'] as const)(
    'parks native Agent before a child request until public approval %s',
    async (result) => {
      const fixture = await startFixture(async () => ({}), true, true, undefined, 'approval');
      try {
        await fixture.submit();
        expect(fixture.harness.hasPendingRequiredAction()).toBe(true);
        expect(fixture.callsByRole.child).toBe(0);
        expect(fixture.write).not.toHaveBeenCalled();
        const tool = fixture.observed.find(
          (event) => event.kind === 'agent.tool_use' && event.payload['name'] === 'Agent',
        );
        expect(tool).toBeDefined();
        await fixture.harness.submit({
          kind: 'user.tool_confirmation',
          payload: { tool_use_id: tool!.id, result },
        });
        expect(fixture.callsByRole.child).toBe(result === 'allow' ? 2 : 0);
        expect(fixture.write).toHaveBeenCalledTimes(result === 'allow' ? 1 : 0);
      } finally {
        await fixture.close();
      }
    },
    20_000,
  );

  it.each([0.001, 0.003])(
    'gates native Agent on the proposing call usage ACK at session cost %s',
    async (cost) => {
      const entered = deferred<void>();
      const ack = deferred<void>();
      const fixture = await startFixture(
        async (_usage, model) => {
          if (model === 'orca-actual-model') {
            entered.resolve();
            await ack.promise;
          }
          return { session_cost_usd: cost };
        },
        true,
        true,
        undefined,
        'cost',
      );
      const submitting = fixture.submit();
      try {
        await entered.promise;
        await delay(150);
        expect(fixture.callsByRole.child).toBe(0);
        expect(fixture.write).not.toHaveBeenCalled();
        ack.resolve();
        await submitting;
        expect(fixture.callsByRole.child).toBe(cost < 0.003 ? 2 : 0);
        expect(fixture.write).toHaveBeenCalledTimes(cost < 0.003 ? 1 : 0);
      } finally {
        ack.resolve();
        await submitting.catch(() => undefined);
        await fixture.close();
      }
    },
    20_000,
  );

  it.each([false, true])(
    'fails closed on a rejected ACK and retries the same usage event id (subagent=%s)',
    async (child) => {
      const entered = deferred<void>();
      const ack = deferred<void>();
      const onUsage = vi.fn<NonNullable<ClaudeHarnessOptions['onUsage']>>(async (_usage, model) => {
        if (child && model !== 'orca-child-actual') return {};
        entered.resolve();
        await ack.promise;
        throw new Error('fixture ACK failure');
      });
      const fixture = await startFixture(onUsage, true, child);
      const submitting = fixture.submit();
      const rejected = expect(submitting).rejects.toThrow('Registry did not acknowledge usage');
      try {
        await entered.promise;
        ack.resolve();
        await rejected;
        expect(fixture.write).not.toHaveBeenCalled();
        expect(fixture.harness.hasPendingGuardrailUsage()).toBe(true);
        const original = onUsage.mock.calls.at(-1)!;
        onUsage.mockResolvedValue({ session_cost_usd: 0.003 });
        await fixture.harness.submit({ kind: 'user.interrupt', payload: {} });
        expect(onUsage.mock.calls.at(-1)).toEqual(original);
        expect(fixture.harness.hasPendingGuardrailUsage()).toBe(false);
        expect(fixture.write).not.toHaveBeenCalled();
      } finally {
        ack.resolve();
        await submitting.catch(() => undefined);
        await fixture.close();
      }
    },
    20_000,
  );

  it.each(['missing_input', 'invalid_output'] as const)(
    'rejects provider %s usage before an actual SDK tool side effect',
    async (fault) => {
      const onUsage = vi.fn<NonNullable<ClaudeHarnessOptions['onUsage']>>(async () => ({}));
      const fixture = await startFixture(onUsage, true, false, fault);
      try {
        await expect(fixture.submit()).rejects.toThrow('missing or invalid final usage');
        expect(fixture.write).not.toHaveBeenCalled();
        expect(onUsage).not.toHaveBeenCalled();
      } finally {
        await fixture.close();
      }
    },
    20_000,
  );

  it('stops with an ACK still pending without releasing a tool or waiting for that ACK', async () => {
    const entered = deferred<void>();
    const ack = deferred<void>();
    const fixture = await startFixture(async () => {
      entered.resolve();
      await ack.promise;
      return { session_cost_usd: 0.001 };
    }, true);
    const submitting = fixture.submit().catch(() => undefined);
    try {
      await entered.promise;
      await expect(
        Promise.race([
          fixture.harness.stop('idle.timeout').then(() => 'stopped'),
          delay(5000).then(() => 'timeout'),
        ]),
      ).resolves.toBe('stopped');
      expect(fixture.write).not.toHaveBeenCalled();
      ack.resolve();
      await submitting;
      expect(fixture.write).not.toHaveBeenCalled();
    } finally {
      ack.resolve();
      await submitting;
      await fixture.close();
    }
  }, 20_000);
});

const expectedUsage = {
  input_tokens: 1000,
  output_tokens: 500,
  cache_read_input_tokens: 70,
  cache_creation: { ephemeral_1h_input_tokens: 20, ephemeral_5m_input_tokens: 30 },
};

async function startFixture(
  onUsage: NonNullable<ClaudeHarnessOptions['onUsage']>,
  toolCall: boolean,
  child = false,
  usageFault?: 'missing_input' | 'invalid_output',
  nativeGate?: 'block' | 'approval' | 'cost',
) {
  let responseNumber = 0;
  const callsByRole = { parent: 0, child: 0 };
  const server = createServer(async (request, response) => {
    let rawBody = '';
    for await (const chunk of request) rawBody += chunk;
    if (!request.url?.startsWith('/v1/messages')) {
      response.writeHead(404);
      response.end('{}');
      return;
    }
    responseNumber += 1;
    const isChild = child && JSON.stringify(JSON.parse(rawBody).system).includes('Fixture worker');
    const roleCall = ++callsByRole[isChild ? 'child' : 'parent'];
    writeResponse(
      response,
      responseNumber,
      child && !isChild && roleCall === 1
        ? 'delegate'
        : toolCall && (isChild || !child) && roleCall === 1
          ? 'write'
          : false,
      isChild,
      responseNumber === 1 ? usageFault : undefined,
    );
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('missing fixture port');
  const events: Event[] = [];
  const store: TranscriptStore = {
    append: async (_w, _s, appended) => {
      events.push(...appended);
      return appended.map((event) => event.id);
    },
    read: (_w, _s, options) => ({
      async *[Symbol.asyncIterator]() {
        for (const event of events) {
          if (options.subpath === '*' || (event.subpath ?? '') === (options.subpath ?? ''))
            yield event;
        }
      },
    }),
    tail: () => ({
      async *[Symbol.asyncIterator]() {
        yield* [];
      },
    }),
    archive: async () => {},
    close: async () => {},
  };
  const sandbox = await new InMemorySandboxRuntime().acquire({});
  const write = vi.spyOn(sandbox.files, 'write');
  const harness = new ClaudeAgentSdkHarness({
    apiKey: 'fixture-key',
    baseURL: `http://127.0.0.1:${address.port}`,
    modelDefault: 'orca-requested-model',
    adapter: new ClaudeAgentSdkAdapter(store, 'ws_usage_wire'),
    workspaceId: 'ws_usage_wire',
    sessionId: 'ses_usage_wire',
    onUsage,
  });
  const observed: AgentEvent[] = [];
  const pump = (async () => {
    for await (const event of harness.events()) observed.push(event);
  })();
  await harness.start({
    workspaceId: 'ws_usage_wire',
    sessionId: 'ses_usage_wire',
    agentSnapshot: {
      model_id: child ? 'sonnet' : 'orca-requested-model',
      system: 'Fixture primary. Delegate only to worker when requested.',
      allowed_tool_names: ['write'],
      tool_permission_policies: { mcp__orca__write: 'always_allow' },
      ...(child
        ? {
            multiagent: {
              type: 'coordinator' as const,
              agents: [
                {
                  id: 'agent_worker',
                  name: 'Worker',
                  version: 1,
                  model_id: 'haiku',
                  system: 'Fixture worker. Write marker once.',
                  allowed_tool_names: ['write'],
                  tool_permission_policies: { mcp__orca__write: 'always_allow' as const },
                },
              ],
            },
          }
        : {}),
    },
    ...(toolCall ? { sandbox } : {}),
    guardrails: [
      {
        id: 'gr_cost',
        name: 'cost cap',
        tier: 'session',
        phases: ['tool_call'],
        stateful: true,
        stateScope: 'session',
        rule: {
          kind: 'builtin',
          builtin:
            nativeGate === 'block'
              ? 'block_tools'
              : nativeGate === 'approval'
                ? 'require_approval_for_tools'
                : child && nativeGate !== 'cost'
                  ? 'subagent_cost_budget'
                  : 'cost_budget',
          params:
            nativeGate === 'block' || nativeGate === 'approval'
              ? { tools: ['Agent'], reason: 'native dispatch denied' }
              : { max_cost_usd: 0.003 },
        },
      },
    ],
  });
  return {
    harness,
    observed,
    sandbox,
    write,
    callsByRole,
    submit: () =>
      harness.submit({
        id: 'evt_usage_wire',
        kind: 'user.message',
        payload: { content: [{ type: 'text', text: 'Fixture: write marker if requested.' }] },
      }),
    close: async () => {
      await harness.stop('idle.timeout');
      await pump;
      await sandbox.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

function writeResponse(
  response: ServerResponse,
  number: number,
  withTool: false | 'write' | 'delegate',
  child = false,
  usageFault?: 'missing_input' | 'invalid_output',
): void {
  response.writeHead(200, { 'content-type': 'text/event-stream' });
  const send = (type: string, fields: Record<string, unknown> = {}) => {
    response.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...fields })}\n\n`);
  };
  const initialUsage: Record<string, unknown> = {
    ...expectedUsage,
    output_tokens: 0,
    cache_creation_input_tokens: 50,
  };
  if (usageFault === 'missing_input') delete initialUsage.input_tokens;
  send('message_start', {
    message: {
      id: `msg_usage_${number}`,
      type: 'message',
      role: 'assistant',
      model: child ? 'orca-child-actual' : 'orca-actual-model',
      content: [],
      stop_reason: null,
      stop_sequence: null,
      usage: initialUsage,
    },
  });
  send('content_block_start', { index: 0, content_block: { type: 'text', text: '' } });
  send('content_block_delta', { index: 0, delta: { type: 'text_delta', text: 'first block' } });
  send('content_block_stop', { index: 0 });
  send('content_block_start', {
    index: 1,
    content_block: withTool
      ? {
          type: 'tool_use',
          id: withTool === 'delegate' ? 'toolu_delegate' : 'toolu_usage',
          name: withTool === 'delegate' ? 'Agent' : 'mcp__orca__write',
          input: {},
        }
      : { type: 'text', text: '' },
  });
  send('content_block_delta', {
    index: 1,
    delta: withTool
      ? {
          type: 'input_json_delta',
          partial_json: JSON.stringify(
            withTool === 'delegate'
              ? {
                  subagent_type: 'worker',
                  description: 'Check usage',
                  run_in_background: false,
                  prompt: 'Write the marker once.',
                }
              : { path: '/marker.txt', content: 'written' },
          ),
        }
      : { type: 'text_delta', text: 'second block' },
  });
  send('content_block_stop', { index: 1 });
  send('message_delta', {
    delta: { stop_reason: withTool ? 'tool_use' : 'end_turn', stop_sequence: null },
    usage: { output_tokens: usageFault === 'invalid_output' ? '500' : 500 },
  });
  send('message_stop');
  response.end();
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
