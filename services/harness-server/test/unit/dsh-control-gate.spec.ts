// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import type { WorkerCommand, WorkerEvent } from '@orca/sdk-harness';
import { CodexSdkHarness } from '../../src/harness/codex-sdk/index.js';
import type { AgentEvent, SessionStartInput } from '../../src/harness/agent-harness.js';
import { turnStoreFixture } from '../support/codex-turn-store.js';
import { loadConfig } from '../../src/config.js';

const child = 'subagents/11111111-1111-4111-8111-111111111111';
const stopped: CodexSdkHarness[] = [];
afterEach(async () => {
  await Promise.all(stopped.splice(0).map((h) => h.stop('error')));
});

function fixture(
  changes: Partial<SessionStartInput> = {},
  afterCommit?: () => Promise<void>,
  turnTimeoutMs?: number,
) {
  let emit!: (event: WorkerEvent) => void;
  let run = async () => {};
  const replies: WorkerCommand[] = [];
  const waiters = new Map<string, (allowed: boolean) => void>();
  const turns = turnStoreFixture(afterCommit ? { afterCommit } : {});
  const close = vi.fn(async () => {});
  const harness = new CodexSdkHarness({
    harness: 'dsh_sdk',
    apiKey: 'jwt',
    ...(turnTimeoutMs === undefined ? {} : { turnTimeoutMs }),
    dshGatewayUrl: 'http://gateway.test',
    turns: turns.store,
    llmGatewayJwtProvider: {
      getValidToken: async () => ({ token: 'jwt' }) as never,
      close: () => {},
    },
    createWorker: (send) => {
      emit = send;
      return {
        close,
        refreshOptions: () => {},
        handle: async (command) => {
          replies.push(command);
          if (command.type === 'start')
            emit({ type: 'thread_tree', threads: [{ subpath: child, parentSubpath: '' }] });
          if (command.type === 'submit') await run();
          if (command.type === 'orchestration_result') waiters.get(command.id)?.(command.allowed);
        },
      };
    },
  });
  stopped.push(harness);
  const events: AgentEvent[] = [];
  void (async () => {
    for await (const event of harness.events()) {
      events.push(event);
      event.persistence?.resolve();
    }
  })();
  const input: SessionStartInput = {
    workspaceId: 'ws_test',
    sessionId: 'ses_test',
    archivedThreadSubpaths: [],
    agentSnapshot: { model_provider: 'deepseek', model_id: 'deepseek-v4-flash' },
    ...changes,
  };
  const request = (
    id: string,
    name: 'spawn' | 'send_message' = 'spawn',
    args = name === 'spawn'
      ? { description: 'task', prompt: 'work' }
      : { agent_id: child.slice(10), message: 'work' },
  ) =>
    new Promise<boolean>((resolve) => {
      waiters.set(id, resolve);
      emit({
        type: 'orchestration_request',
        id,
        name,
        arguments: args,
        origin: { eventId: `evt_${id}`, subpath: '' },
      });
    });
  const complete = () => {
    emit({
      type: 'event',
      event: {
        type: 'turn.completed',
        usage: { input_tokens: 1, cached_input_tokens: 0, output_tokens: 1 },
      },
    });
    emit({
      type: 'checkpoint',
      checkpoint: { version: 1, threadId: 'root', format: 'dsh_sdk', files: {} },
    });
  };
  return {
    harness,
    input,
    events,
    replies,
    request,
    complete,
    close,
    turns,
    emit: (event: WorkerEvent) => emit(event),
    setRun: (fn: () => Promise<void>) => {
      run = fn;
    },
  };
}
const message = { id: 'evt_turn', kind: 'user.message', payload: { content: 'work' } };

it('applies the configured whole-tree deadline to DSH turns', async () => {
  const config = loadConfig({ SANDBOX_RUNTIME: 'in-memory', HARNESS_TURN_TIMEOUT_MS: '1234' });
  const f = fixture({}, undefined, config.harnessTurnTimeoutMs);
  f.setRun(async () => {
    f.complete();
  });
  await f.harness.start(f.input);
  const timer = vi.spyOn(globalThis, 'setTimeout');
  try {
    await f.harness.submit(message);
    expect(timer).toHaveBeenCalledWith(expect.any(Function), 1234);
  } finally {
    timer.mockRestore();
  }
});

it('warm Registry archive refresh is monotonic and reaches the native admission fence without starting a turn', async () => {
  const f = fixture();
  await f.harness.start(f.input);
  await f.harness.applyArchivedThreadSubpaths([child]);
  await f.harness.applyArchivedThreadSubpaths([]);
  expect(f.replies.filter((command) => command.type === 'archive_fence')).toEqual([
    { type: 'archive_fence', threadId: child.slice('subagents/'.length) },
  ]);
  await expect(f.harness.submit({ ...message, subpath: child })).rejects.toThrow('Archived');
  expect(f.replies.some((command) => command.type === 'submit')).toBe(false);
});

it('requires durable archive snapshot and rejects Agent-only purpose policy', async () => {
  const f = fixture();
  const { archivedThreadSubpaths: _, ...input } = f.input;
  await expect(f.harness.start(input)).rejects.toThrow('authoritative archived');
  const g = fixture({
    guardrails: [
      {
        id: 'purpose',
        name: 'purpose',
        tier: 'agent',
        phases: ['tool_call'],
        rule: { kind: 'builtin', builtin: 'headless_subagent_purpose_guard' },
        stateful: false,
      },
    ],
  });
  await expect(g.harness.start(g.input)).rejects.toThrow('adapter');
});

it('reserves cumulative spawn dispatch count atomically and does not duplicate canonical tool events', async () => {
  const f = fixture({
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
  const allowed: boolean[] = [];
  f.setRun(async () => {
    allowed.push(...(await Promise.all([f.request('one'), f.request('two')])));
    f.complete();
  });
  await f.harness.start(f.input);
  await f.harness.submit(message);
  expect(allowed).toEqual([true, false]);
  expect(
    f.events.filter((e) => e.kind === 'agent.tool_use' || e.kind === 'agent.tool_result'),
  ).toEqual([]);
});

it('cold archived child rejects explicit follow-up and model send before execution', async () => {
  const f = fixture({ archivedThreadSubpaths: [child] });
  let allowed: boolean | undefined;
  f.setRun(async () => {
    allowed = await f.request('send', 'send_message');
    f.complete();
  });
  await f.harness.start(f.input);
  await expect(f.harness.submit({ ...message, subpath: child })).rejects.toThrow('Archived');
  await f.harness.submit(message);
  expect(allowed).toBe(false);
  expect(f.replies.find((c) => c.type === 'start')).toMatchObject({
    archivedThreadSubpaths: [child],
  });
});

it('missing terminal usage durably fails source and retires tree without replay or zero usage', async () => {
  const f = fixture();
  f.setRun(async () => {
    f.emit({ type: 'failure', message: 'provider failed' });
  });
  await f.harness.start(f.input);
  await expect(f.harness.submit(message)).rejects.toThrow();
  expect(f.close).toHaveBeenCalledOnce();
  expect(f.turns.snapshot.receipt).toMatchObject({ phase: 'settled', error: 'provider failed' });
  expect(f.events.some((e) => e.kind === 'agent.usage')).toBe(false);
  await expect(f.harness.submit({ ...message, id: 'evt_next' })).rejects.toThrow('unavailable');
  expect(f.replies.filter((c) => c.type === 'submit')).toHaveLength(1);
});

it('lost commit ACK inspects authoritative receipt and never rolls back committed state or replays tools', async () => {
  const f = fixture({}, async () => {
    throw new Error('ACK lost');
  });
  f.setRun(async () => {
    f.complete();
  });
  const inspect = vi.spyOn(f.turns.store, 'inspect');
  await f.harness.start(f.input);
  await expect(f.harness.submit(message)).rejects.toThrow();
  expect(inspect).toHaveBeenCalledOnce();
  expect(f.turns.snapshot.receipt).toMatchObject({ phase: 'settled', error: null });
  expect(f.turns.snapshot.state).toMatchObject({ threadId: 'root' });
  expect(f.replies.filter((c) => c.type === 'submit')).toHaveLength(1);
});

it('late approval cannot revive cancelled orchestration and archive fence precedes native cleanup', async () => {
  const f = fixture({
    agentSnapshot: {
      model_provider: 'deepseek',
      model_id: 'deepseek-v4-flash',
      tool_permission_policies: { spawn: 'always_ask' },
    },
  });
  let release!: () => void;
  const hold = new Promise<void>((resolve) => {
    release = resolve;
  });
  let allowed: boolean | undefined;
  f.setRun(async () => {
    const reply = new Promise<boolean>((resolve) => {
      // Request from the child so targeted archive must invalidate this exact approval.
      f.emit({
        type: 'orchestration_request',
        id: 'child_spawn',
        name: 'spawn',
        arguments: { description: 'task', prompt: 'work' },
        origin: { eventId: 'evt_child_spawn', subpath: child },
      });
      const check = setInterval(() => {
        const response = f.replies.find(
          (c) => c.type === 'orchestration_result' && c.id === 'child_spawn',
        );
        if (response?.type === 'orchestration_result') {
          clearInterval(check);
          resolve(response.allowed);
        }
      }, 1);
    });
    allowed = await reply;
    await hold;
    f.complete();
  });
  await f.harness.start(f.input);
  await f.harness.submit(message);
  expect(f.harness.hasPendingRequiredAction()).toBe(true);
  await f.harness.submit({
    id: 'evt_archive',
    kind: 'user.interrupt',
    subpath: child,
    payload: { stop_reason: 'archived' },
  });
  await expect(
    f.harness.submit({
      id: 'evt_late',
      kind: 'user.tool_confirmation',
      subpath: child,
      payload: { tool_use_id: 'evt_child_spawn', result: 'allow' },
    }),
  ).rejects.toThrow('No pending');
  const fence = f.replies.findIndex((c) => c.type === 'archive_fence');
  const interrupt = f.replies.findIndex((c) => c.type === 'interrupt');
  expect(fence).toBeGreaterThan(-1);
  expect(fence).toBeLessThan(interrupt);
  await vi.waitFor(() => expect(allowed).toBe(false));
  release();
});

it.each([false, true])(
  'target cancellation support=%s fences delayed executable effects without stopping siblings',
  async (supported) => {
    let childSignal: AbortSignal | undefined;
    let siblingSignal: AbortSignal | undefined;
    let release!: () => void;
    const hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    const execute = vi.fn(async (input, options) => {
      const signal = (options?.signal ?? new AbortController().signal) as AbortSignal;
      if (input.command === 'child') {
        childSignal = signal;
        if (!supported) await hold;
        else
          await new Promise<never>((_, reject) =>
            signal.addEventListener(
              'abort',
              () => reject(new DOMException('aborted', 'AbortError')),
              { once: true },
            ),
          );
      } else {
        siblingSignal = signal;
        await hold;
      }
      return { content: 'done' };
    });
    const f = fixture({
      supportsPerCallCancellation: supported,
      tools: [{ name: 'bash', description: '', input_schema: { type: 'object' }, execute }],
      agentSnapshot: {
        model_provider: 'deepseek',
        model_id: 'deepseek-v4-flash',
        allowed_tool_names: ['bash'],
      },
    });
    f.setRun(async () => {
      for (const [id, subpath] of [
        ['child', child],
        ['sibling', ''],
      ])
        f.emit({
          type: 'tool_call',
          id: id!,
          name: 'bash',
          arguments: { command: id },
          origin: { eventId: `evt_${id}`, subpath: subpath! },
        });
      await hold;
      f.complete();
    });
    await f.harness.start(f.input);
    const turn = f.harness.submit(message);
    if (!supported) {
      await vi.waitFor(() =>
        expect(f.replies.filter((c) => c.type === 'tool_result')).toHaveLength(2),
      );
      expect(execute).not.toHaveBeenCalled();
      expect(f.replies.filter((c) => c.type === 'tool_result')).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ result: expect.objectContaining({ isError: true }) }),
        ]),
      );
      release();
      await turn.catch(() => {});
      return;
    }
    await vi.waitFor(() => expect(childSignal && siblingSignal).toBeDefined());
    const control = f.harness.submit({
      id: 'evt_cancel',
      kind: 'user.interrupt',
      subpath: child,
      payload: {},
    });
    if (supported) {
      await control;
      expect(childSignal!.aborted).toBe(true);
      expect(f.replies.some((c) => c.type === 'interrupt')).toBe(true);
    } else {
      await expect(control).rejects.toThrow('cannot confirm');
      expect(childSignal!.aborted).toBe(false);
      expect(f.replies.some((c) => c.type === 'interrupt')).toBe(false);
    }
    expect(siblingSignal!.aborted).toBe(false);
    release();
    if (!supported) await f.harness.stop('error');
    await turn.catch(() => {});
  },
);
