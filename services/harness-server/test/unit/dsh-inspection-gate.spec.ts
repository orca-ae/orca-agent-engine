// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import type { WorkerCommand, WorkerEvent } from '@orca/sdk-harness';
import { CodexSdkHarness } from '../../src/harness/codex-sdk/index.js';
import type { AgentEvent, SessionStartInput } from '../../src/harness/agent-harness.js';
import { turnStoreFixture } from '../support/codex-turn-store.js';

const child = 'subagents/11111111-1111-4111-8111-111111111111';
const grandchild = 'subagents/22222222-2222-4222-8222-222222222222';
const sibling = 'subagents/33333333-3333-4333-8333-333333333333';
const unknown = 'subagents/44444444-4444-4444-8444-444444444444';
const stopped: CodexSdkHarness[] = [];
afterEach(async () => {
  await Promise.all(stopped.splice(0).map((harness) => harness.stop('error')));
});

async function fixture(changes: Partial<SessionStartInput> = {}) {
  let emit!: (event: WorkerEvent) => void;
  let run = async () => {};
  const replies: WorkerCommand[] = [];
  const events: AgentEvent[] = [];
  const waiters = new Map<
    string,
    (reply: Extract<WorkerCommand, { type: 'orchestration_result' }>) => void
  >();
  const harness = new CodexSdkHarness({
    harness: 'dsh_sdk',
    apiKey: 'jwt',
    dshGatewayUrl: 'http://gateway.test',
    turns: turnStoreFixture().store,
    llmGatewayJwtProvider: {
      getValidToken: async () => ({ token: 'jwt' }) as never,
      close: () => {},
    },
    createWorker: (send) => {
      emit = send;
      return {
        close: async () => {},
        refreshOptions: () => {},
        handle: async (command) => {
          replies.push(command);
          if (command.type === 'start')
            emit({
              type: 'thread_tree',
              threads: [
                { subpath: child, parentSubpath: '' },
                { subpath: grandchild, parentSubpath: child },
                { subpath: sibling, parentSubpath: '' },
              ],
            });
          if (command.type === 'submit') await run();
          if (command.type === 'orchestration_result') waiters.get(command.id)?.(command);
        },
      };
    },
  });
  stopped.push(harness);
  void (async () => {
    for await (const event of harness.events()) {
      events.push(event);
      event.persistence?.resolve();
    }
  })();
  await harness.start({
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
    archivedThreadSubpaths: [],
    agentSnapshot: { model_provider: 'deepseek', model_id: 'deepseek-v4-flash' },
    ...changes,
  });
  let serial = 0;
  return {
    harness,
    events,
    replies,
    request: (
      name: Extract<WorkerEvent, { type: 'orchestration_request' }>['name'],
      args: Record<string, unknown> = {},
      subpath = '',
      parentSubpath?: string,
    ) =>
      new Promise<Extract<WorkerCommand, { type: 'orchestration_result' }>>((resolve) => {
        const id = 'query_' + ++serial;
        waiters.set(id, resolve);
        emit({
          type: 'orchestration_request',
          id,
          name,
          arguments: args,
          origin: {
            eventId: 'evt_' + id,
            subpath,
            ...(parentSubpath === undefined ? {} : { parentSubpath }),
          },
        });
      }),
    submit: async (callback: () => Promise<void>) => {
      run = async () => {
        await callback();
        emit({
          type: 'event',
          event: {
            type: 'turn.completed',
            usage: {
              input_tokens: 1,
              cached_input_tokens: 0,
              output_tokens: 1,
            },
          },
        });
        emit({
          type: 'checkpoint',
          checkpoint: {
            version: 1,
            threadId: 'root',
            format: 'dsh_sdk',
            files: {},
          },
        });
      };
      return harness.submit({ id: 'evt_turn', kind: 'user.message', payload: { content: 'work' } });
    },
  };
}

it('allows descendant reads across archived targets without consuming spawn bounds or emitting tools', async () => {
  const f = await fixture({
    archivedThreadSubpaths: [grandchild],
    guardrails: [
      {
        id: 'bounds',
        name: 'bounds',
        tier: 'agent',
        phases: ['tool_call'],
        rule: { kind: 'builtin', builtin: 'spawn_bounds', params: { max_dispatches_per_turn: 1 } },
        stateful: true,
        stateScope: 'turn',
      },
    ],
  });
  await f.submit(async () => {
    expect((await f.request('list_child_sessions')).allowed).toBe(true);
    expect(
      (await f.request('read_child_session', { agent_id: grandchild.slice(10) })).allowed,
    ).toBe(true);
    expect(
      (await f.request('read_child_session', { agent_id: grandchild.slice(10) }, child, ''))
        .allowed,
    ).toBe(true);
    expect((await f.request('spawn', { description: 'task', prompt: 'work' })).allowed).toBe(true);
    expect((await f.request('spawn', { description: 'task', prompt: 'work' })).allowed).toBe(false);
  });
  expect(
    f.events.filter((event) => ['agent.tool_use', 'agent.tool_result'].includes(event.kind)),
  ).toEqual([]);
});

it('gives the same denial for self, siblings, ancestors and unknown targets before interpreting cursors', async () => {
  const f = await fixture();
  const reasons: Array<string | undefined> = [];
  await f.submit(async () => {
    for (const target of [child, sibling, unknown]) {
      const reply = await f.request(
        'read_child_session',
        {
          agent_id: target.slice(10),
          continuation: JSON.stringify({ generation: 'forged', position: -1 }),
        },
        child,
        '',
      );
      expect(reply.allowed).toBe(false);
      reasons.push(reply.reason);
    }
    const ancestor = await f.request(
      'read_child_session',
      { agent_id: child.slice(10) },
      grandchild,
      child,
    );
    reasons.push(ancestor.reason);
    expect(new Set(reasons).size).toBe(1);
    expect((await f.request('list_child_sessions', {}, unknown, '')).allowed).toBe(false);
    expect((await f.request('list_child_sessions', {}, child, sibling)).allowed).toBe(false);
    expect(
      (await f.request('read_child_session', { agent_id: unknown.slice(10), continuation: '[]' }))
        .reason,
    ).toContain('continuation');
  });
});

it('rejects archived callers and their descendants while allowing active ancestors', async () => {
  const f = await fixture({ archivedThreadSubpaths: [child] });
  await f.submit(async () => {
    expect((await f.request('read_child_session', { agent_id: child.slice(10) })).allowed).toBe(
      true,
    );
    expect((await f.request('list_child_sessions', {}, child, '')).allowed).toBe(false);
    expect((await f.request('list_child_sessions', {}, grandchild, child)).allowed).toBe(false);
  });
});

it('honors explicit native tool policies and allows native names in the configured tool list', async () => {
  const f = await fixture({
    agentSnapshot: {
      model_provider: 'deepseek',
      model_id: 'deepseek-v4-flash',
      allowed_tool_names: ['list_child_sessions', 'read_child_session'],
      tool_permission_policies: { read_child_session: 'always_deny' },
    },
  });
  await f.submit(async () => {
    expect((await f.request('list_child_sessions')).allowed).toBe(true);
    expect((await f.request('read_child_session', { agent_id: child.slice(10) })).allowed).toBe(
      false,
    );
  });
});

it('uses the existing confirmation transport for allowed inspection', async () => {
  const f = await fixture({
    agentSnapshot: {
      model_provider: 'deepseek',
      model_id: 'deepseek-v4-flash',
      tool_permission_policies: { list_child_sessions: 'always_ask' },
    },
  });
  const submitted = f.submit(async () => {
    expect((await f.request('list_child_sessions')).allowed).toBe(true);
  });
  await vi.waitFor(() => expect(f.harness.hasPendingRequiredAction()).toBe(true));
  await f.harness.submit({
    id: 'evt_approval',
    kind: 'user.tool_confirmation',
    payload: { tool_use_id: 'evt_query_1', result: 'allow' },
  });
  await submitted;
});

it('reserves native inspection names against sandbox bindings', async () => {
  await expect(
    fixture({
      agentSnapshot: {
        model_provider: 'deepseek',
        model_id: 'deepseek-v4-flash',
        allowed_tool_names: ['list_child_sessions'],
      },
      tools: [
        {
          name: 'list_child_sessions',
          description: '',
          input_schema: { type: 'object' },
          execute: async () => ({ content: 'spoof' }),
        },
      ],
    }),
  ).rejects.toThrow('reserved DSH native tool');
});

it.each(['archive', 'retire'])(
  'rechecks caller validity after approval during %s',
  async (mode) => {
    const f = await fixture({
      agentSnapshot: {
        model_provider: 'deepseek',
        model_id: 'deepseek-v4-flash',
        tool_permission_policies: { list_child_sessions: 'always_ask' },
      },
    });
    let reply: Awaited<ReturnType<typeof f.request>> | undefined;
    let release!: () => void;
    const hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    const submitted = f
      .submit(async () => {
        reply = await f.request('list_child_sessions', {}, child, '');
        await hold;
      })
      .catch((error: unknown) => error);
    await vi.waitFor(() => expect(f.harness.hasPendingRequiredAction()).toBe(true));
    const approval =
      mode === 'archive'
        ? f.harness.submit({
            id: 'evt_archive',
            kind: 'user.interrupt',
            subpath: child,
            payload: { stop_reason: 'archived' },
          })
        : (async () => {
            // Host retirement has no plugin flag: the host abort and failed state must fence admission.
            const state = f.harness as unknown as { failed: boolean; abort: AbortController };
            state.failed = true;
            state.abort.abort();
            await f.harness.submit({
              id: 'evt_approval',
              kind: 'user.tool_confirmation',
              subpath: child,
              payload: { tool_use_id: 'evt_query_1', result: 'allow' },
            });
          })();
    try {
      await vi.waitFor(() => expect(reply?.allowed).toBe(false));
    } finally {
      release();
    }
    if (mode === 'retire') await expect(approval).rejects.toThrow('tree retired');
    else await approval;
    await submitted;
  },
);
