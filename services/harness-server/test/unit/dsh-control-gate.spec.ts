// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import type { WorkerCommand, WorkerEvent } from '@orca/sdk-harness';
import { CodexSdkHarness, type CodexSdkHarnessOptions } from '../../src/harness/codex-sdk/index.js';
import type { AgentEvent, SessionStartInput } from '../../src/harness/agent-harness.js';
import { turnStoreFixture } from '../support/codex-turn-store.js';
import { loadConfig } from '../../src/config.js';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runCancellableOpenSandboxCommand } from '@orca/sandbox-runtime';
import type { CodexCheckpoint } from '@orca/codex-harness';
import { captureCheckpoint } from '../../../../packages/dsh-harness/src/checkpoint.js';
import { Dispatcher } from '../../src/runner/dispatcher.js';
import type { RegistryClient } from '../../src/clients/registry.js';
import type { TranscriptStore } from '@orca/transcript-store';
import { createServer } from 'node:http';
import { DshSdkWorker } from '@orca/dsh-harness';

const child = 'subagents/11111111-1111-4111-8111-111111111111';
const stopped: CodexSdkHarness[] = [];
afterEach(async () => {
  await Promise.all(stopped.splice(0).map((h) => h.stop('error')));
});

function fixture(
  changes: Partial<SessionStartInput> = {},
  afterCommit?: () => Promise<void>,
  turnTimeoutMs?: number,
  rootInterrupts?: CodexSdkHarnessOptions['rootInterrupts'],
  existingTurns?: ReturnType<typeof turnStoreFixture>,
  onInterrupt?: () => Promise<void>,
  onToolResult?: () => Promise<void>,
  options: Partial<CodexSdkHarnessOptions> = {},
) {
  let emit!: (event: WorkerEvent) => void;
  let run = async () => {};
  const replies: WorkerCommand[] = [];
  const waiters = new Map<string, (allowed: boolean) => void>();
  const turns = existingTurns ?? turnStoreFixture(afterCommit ? { afterCommit } : {});
  const close = vi.fn(async () => {});
  const harness = new CodexSdkHarness({
    harness: 'dsh_sdk',
    apiKey: 'jwt',
    ...(turnTimeoutMs === undefined ? {} : { turnTimeoutMs }),
    dshGatewayUrl: 'http://gateway.test',
    turns: turns.store,
    ...(rootInterrupts ? { rootInterrupts } : {}),
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
          if (command.type === 'interrupt') await onInterrupt?.();
          if (command.type === 'tool_result') await onToolResult?.();
          if (command.type === 'orchestration_result') waiters.get(command.id)?.(command.allowed);
        },
      };
    },
    ...options,
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
  const complete = (
    checkpoint: CodexCheckpoint = { version: 1, threadId: 'root', format: 'dsh_sdk', files: {} },
  ) => {
    emit({
      type: 'event',
      event: {
        type: 'turn.completed',
        usage: { input_tokens: 1, cached_input_tokens: 0, output_tokens: 1 },
      },
    });
    emit({
      type: 'checkpoint',
      checkpoint,
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

it.each([false, true])(
  'joins confirmed Bash exit when interrupted SDK submit rejects=%s',
  async (aborts) => {
    const directory = await mkdtemp(join(tmpdir(), 'dsh-root-interrupt-'));
    let abortSubmit!: (error: Error) => void;
    let send!: () => void;
    let followerClosed = false;
    const accepted = vi.fn(async () => {});
    const control = { id: 'evt_busy_interrupt', kind: 'user.interrupt', payload: {} };
    const rootInterrupts: CodexSdkHarnessOptions['rootInterrupts'] = async function* (
      _turn,
      signal,
      ready,
    ) {
      if (_turn.id === 'evt_seed') {
        ready();
        await new Promise<void>((resolve) =>
          signal.addEventListener('abort', () => resolve(), { once: true }),
        );
        return;
      }
      try {
        const pending = new Promise<void>((resolve) => {
          send = resolve;
        });
        ready();
        await pending;
        yield { event: control, hooks: { onAccepted: accepted } };
        yield { event: control, hooks: { onAccepted: accepted } };
        if (!signal.aborted)
          await new Promise<void>((resolve) =>
            signal.addEventListener('abort', () => resolve(), { once: true }),
          );
      } finally {
        followerClosed = true;
      }
    };
    let exited = false;
    let killed = false;
    let child: ReturnType<typeof spawn> | undefined;
    const execute = vi.fn(async (_input, options) => {
      if (aborts)
        options!.signal!.addEventListener(
          'abort',
          () => abortSubmit(new DOMException('SDK submit aborted', 'AbortError')),
          { once: true },
        );
      child = spawn('sh', ['-c', 'printf START > start; sleep 2; printf FINISH > finish'], {
        cwd: directory,
        detached: true,
        stdio: 'ignore',
      });
      const exit = new Promise<void>((resolve) =>
        child!.once('exit', () => {
          exited = true;
          resolve();
        }),
      );
      return runCancellableOpenSandboxCommand(
        async (path, init) => {
          if (init.method === 'DELETE') {
            process.kill(-child!.pid!, 'SIGKILL');
            killed = true;
            await exit;
          }
          return path.includes('/status/')
            ? Response.json({
                id: 'active-root',
                running: !exited,
                exit_code: -1,
                finished_at: new Date().toISOString(),
              })
            : new Response();
        },
        '{}',
        async (_response, emit) => {
          emit({ type: 'init', text: 'active-root' });
          await exit;
          emit({ type: 'execution_complete' });
          return { content: 'done' };
        },
        options!.signal!,
      );
    });
    const f = fixture(
      {
        supportsPerCallCancellation: true,
        tools: [{ name: 'bash', description: '', input_schema: { type: 'object' }, execute }],
        agentSnapshot: {
          model_provider: 'deepseek',
          model_id: 'deepseek-v4-flash',
          allowed_tool_names: ['bash'],
        },
      },
      undefined,
      undefined,
      rootInterrupts,
    );
    const busyRun = async () => {
      const submitted = aborts
        ? new Promise<never>((_resolve, reject) => {
            abortSubmit = reject;
          })
        : undefined;
      f.emit({
        type: 'event',
        event: {
          type: 'item.completed',
          item: { type: 'agent_message', id: 'partial', text: 'Starting Bash' },
        },
        origin: { eventId: 'evt_partial', subpath: '' },
      });
      f.emit({
        type: 'tool_call',
        id: 'busy_root',
        name: 'bash',
        arguments: { command: 'active-root' },
        origin: { eventId: 'evt_busy_root', subpath: '' },
      });
      if (submitted) await submitted;
      await vi.waitFor(() => expect(f.replies.some((c) => c.type === 'tool_result')).toBe(true));
      f.complete();
    };
    try {
      await f.harness.start(f.input);
      const nativeId = '12345678-1234-1234-1234-123456789abc';
      const committed = captureCheckpoint(nativeId, '', {
        ['_no-cwd/' + nativeId + '/session.v4.jsonl']: Buffer.from(
          JSON.stringify({
            type: 'session',
            version: 4,
            id: nativeId,
            createdAt: 0,
            isSeeded: false,
            delegationDepth: 0,
          }) + '\n',
        ),
      });
      f.setRun(async () => f.complete(committed));
      await f.harness.submit({ ...message, id: 'evt_seed' });
      expect(f.turns.snapshot.state).toEqual(committed);
      f.setRun(busyRun);
      const turn = f.harness.submit(message);
      const outcome = turn.catch((error: unknown) => error);
      await vi.waitFor(async () =>
        expect(await readFile(join(directory, 'start'), 'utf8')).toBe('START'),
      );
      expect(exited).toBe(false);
      expect(f.events.some((event) => event.kind === 'agent.message')).toBe(true);
      expect(f.turns.snapshot.receipt?.phase).toBe('pending');
      expect(f.harness.hasPendingRequiredAction()).toBe(false);
      send();
      await outcome;
      await f.harness.submit(control, { onAccepted: accepted });
      expect(killed && exited && followerClosed).toBe(true);
      expect(accepted).toHaveBeenCalledTimes(1);
      expect(f.replies.filter((c) => c.type === 'interrupt')).toHaveLength(1);
      expect(f.turns.snapshot.receipt).toMatchObject({
        phase: 'settled',
        sourceIds: ['evt_turn', control.id],
        error: 'DSH SDK turn interrupted',
      });
      expect(f.turns.snapshot.state).toEqual(committed);
      expect(
        f.events.filter((event) => event.kind === 'session.status_idle').at(-1)?.payload,
      ).toMatchObject({ stop_reason: { type: 'retries_exhausted' } });
      await new Promise((resolve) => setTimeout(resolve, 2100));
      await expect(readFile(join(directory, 'finish'))).rejects.toMatchObject({ code: 'ENOENT' });
      expect(execute).toHaveBeenCalledTimes(1);
      const recovered = fixture({}, undefined, undefined, undefined, f.turns);
      recovered.setRun(async () => recovered.complete(committed));
      await recovered.harness.start(recovered.input);
      expect(recovered.replies.find((command) => command.type === 'start')).toMatchObject({
        checkpoint: committed,
      });
      expect(recovered.replies.filter((command) => command.type === 'submit')).toEqual([]);
      await recovered.harness.submit({
        id: 'evt_explicit_recovery',
        kind: 'user.message',
        payload: { content: 'continue from the committed context' },
      });
      expect(recovered.replies.filter((command) => command.type === 'submit')).toEqual([
        expect.objectContaining({
          text: 'continue from the committed context',
          sourceEventId: 'evt_explicit_recovery',
        }),
      ]);
      expect(execute).toHaveBeenCalledTimes(1);
    } finally {
      if (child?.pid && !exited) process.kill(-child.pid, 'SIGKILL');
      await rm(directory, { recursive: true, force: true });
    }
  },
);

it('waits for control readiness and fails closed on a follower read failure before execution', async () => {
  let ready!: () => void;
  let fail!: (error: Error) => void;
  const f = fixture({}, undefined, undefined, async function* (_turn, _signal, positioned) {
    ready = positioned;
    await new Promise<never>((_resolve, reject) => {
      fail = reject;
    });
    yield { event: message };
  });
  const run = vi.fn(async () => {});
  f.setRun(run);
  await f.harness.start(f.input);
  const outcome = f.harness.submit(message).catch((error: unknown) => error);
  await vi.waitFor(() => expect(ready).toBeTypeOf('function'));
  expect(run).not.toHaveBeenCalled();
  fail(new Error('transcript read failed'));
  await outcome;
  expect(run).not.toHaveBeenCalled();
  expect(f.close).toHaveBeenCalled();
  expect(f.turns.snapshot.receipt?.phase).toBe('settled');
});

it.each(['timeout', 'complete', 'stop'])(
  'joins stalled ownership admission on %s without delivering the control',
  async (mode) => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
    const lookup = vi.fn(() => new Promise<never>(() => {}));
    let joined = false;
    const store: TranscriptStore = {
      append: async (_ws, _session, events) => events.map((event) => event.id),
      read: async function* () {},
      tail: async function* (_ws, _session, opts) {
        opts.onReady?.();
        try {
          yield {
            id: 'evt_stalled_control',
            workspaceId: 'ws_test',
            sessionId: 'ses_test',
            subpath: '',
            seq: 2,
            producedAt: new Date().toISOString(),
            producedBy: 'client',
            kind: 'user.interrupt',
            payload: Buffer.from('{}'),
            idempotencyKey: '',
          };
        } finally {
          joined = true;
        }
      },
      close: async () => {},
      archive: async () => {},
    };
    const dispatcher = new Dispatcher({
      store,
      registry: { getExecutionOwner: lookup } as unknown as RegistryClient,
      groupId: 'dsh-owner-stall',
      anthropicApiKey: 'unused',
      modelDefault: 'fake',
    });
    const state = dispatcher as unknown as {
      runners: Map<string, unknown>;
      followRootInterrupts(
        ws: string,
        session: string,
        ...args: Parameters<NonNullable<CodexSdkHarnessOptions['rootInterrupts']>>
      ): ReturnType<NonNullable<CodexSdkHarnessOptions['rootInterrupts']>>;
    };
    state.runners.set('ws_test/ses_test', {});
    let release!: () => void;
    const executing = new Promise<void>((resolve) => {
      release = resolve;
    });
    const f = fixture({}, undefined, undefined, (...args) =>
      state.followRootInterrupts('ws_test', 'ses_test', ...args),
    );
    f.close.mockImplementation(async () => {
      release();
    });
    f.setRun(async () => {
      await executing;
      f.complete();
    });
    try {
      await f.harness.start(f.input);
      const submitted = f.harness.submit({ ...message, seq: 1 }).catch((error: unknown) => error);
      await vi.waitFor(() => expect(lookup).toHaveBeenCalledTimes(1));
      if (mode === 'timeout') await vi.advanceTimersByTimeAsync(5_000);
      else if (mode === 'complete') release();
      else await f.harness.stop('error');
      await submitted;
      expect(joined).toBe(true);
      expect(f.turns.snapshot.receipt?.sourceIds).toEqual([message.id]);
      if (mode === 'timeout') {
        expect(f.close).toHaveBeenCalled();
        expect(f.turns.snapshot.receipt).toMatchObject({
          phase: 'settled',
          error: expect.any(String),
        });
      } else if (mode === 'complete') {
        expect(f.turns.snapshot.receipt).toMatchObject({ phase: 'settled', error: null });
        expect(f.replies.filter((command) => command.type === 'interrupt')).toEqual([]);
      }
    } finally {
      release();
      state.runners.clear();
      vi.useRealTimers();
    }
  },
);

it('joins a follower stopped before readiness without starting executable work', async () => {
  let following = false;
  let joined = false;
  const f = fixture({}, undefined, undefined, async function* (_turn, signal) {
    following = true;
    try {
      await new Promise<void>((resolve) =>
        signal.addEventListener('abort', () => resolve(), { once: true }),
      );
      yield { event: message };
    } finally {
      joined = true;
    }
  });
  const run = vi.fn(async () => {});
  f.setRun(run);
  await f.harness.start(f.input);
  const submitted = f.harness.submit(message);
  await vi.waitFor(() => expect(following).toBe(true));
  await f.harness.stop('error');
  await submitted;
  expect(joined).toBe(true);
  expect(run).not.toHaveBeenCalled();
});

it('drops a late root interrupt after normal completion and does not cancel the next turn', async () => {
  const accepted = vi.fn(async () => {});
  const f = fixture({}, undefined, undefined, async function* (_turn, signal, ready) {
    ready();
    await new Promise<void>((resolve) =>
      signal.addEventListener('abort', () => resolve(), { once: true }),
    );
    yield {
      event: { id: 'evt_late', kind: 'user.interrupt', payload: {} },
      hooks: { onAccepted: accepted },
    };
  });
  f.setRun(async () => f.complete());
  await f.harness.start(f.input);
  await f.harness.submit(message);
  await f.harness.submit({ ...message, id: 'evt_next' });
  expect(accepted).not.toHaveBeenCalled();
  expect(f.replies.filter((c) => c.type === 'interrupt')).toEqual([]);
  expect(f.turns.snapshot.receipt).toMatchObject({
    turnId: 'evt_next',
    phase: 'settled',
    error: null,
  });
});

it.each([false, true])(
  'bounds active cleanup when interrupt ACK hangs=%s, including a hung submit',
  async (hangs) => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
    let send!: () => void;
    const interrupt = vi
      .fn()
      .mockImplementationOnce(() => (hangs ? new Promise<void>(() => {}) : Promise.resolve()));
    const f = fixture(
      {},
      undefined,
      undefined,
      async function* (_turn, _signal, ready) {
        ready();
        await new Promise<void>((resolve) => {
          send = resolve;
        });
        yield { event: { id: 'evt_timeout', kind: 'user.interrupt', payload: {} } };
      },
      undefined,
      interrupt,
    );
    let rejectExecution!: (error: Error) => void;
    f.setRun(
      () =>
        new Promise<void>((_resolve, reject) => {
          rejectExecution = reject;
        }),
    );
    try {
      await f.harness.start(f.input);
      const outcome = f.harness.submit(message).catch((error: unknown) => error);
      await vi.waitFor(() => expect(f.replies.some((c) => c.type === 'submit')).toBe(true));
      send();
      await vi.advanceTimersByTimeAsync(15_000);
      await outcome;
      expect(f.close).toHaveBeenCalled();
      expect(f.turns.snapshot.receipt).toMatchObject({
        phase: 'settled',
        error: expect.stringContaining('root interruption unconfirmed'),
      });
      expect(f.turns.snapshot.state).toBeNull();
      // The losing execution remains rejection-handled by run()'s Promise.race.
      rejectExecution(new Error('late worker rejection after cleanup timeout'));
      await vi.advanceTimersByTimeAsync(0);
    } finally {
      vi.useRealTimers();
    }
  },
);

it.each(['interrupt', 'tool', 'submit'] as const)(
  'does not confirm root cleanup after a real %s failure',
  async (failure) => {
    let send!: () => void;
    let rejectSubmit!: (error: Error) => void;
    let interruptFailed = false;
    const f = fixture(
      {
        supportsPerCallCancellation: true,
        agentSnapshot: {
          model_provider: 'deepseek',
          model_id: 'deepseek-v4-flash',
          allowed_tool_names: ['bash'],
        },
        tools: [
          {
            name: 'bash',
            description: '',
            input_schema: { type: 'object' },
            execute: async (_input, options) =>
              new Promise<never>((_resolve, reject) => {
                options!.signal!.addEventListener(
                  'abort',
                  () => reject(new Error('kill failed; termination unknown')),
                  { once: true },
                );
              }),
          },
        ],
      },
      undefined,
      undefined,
      async function* (_turn, _signal, ready) {
        ready();
        await new Promise<void>((resolve) => {
          send = resolve;
        });
        yield { event: { id: 'evt_failed_cleanup', kind: 'user.interrupt', payload: {} } };
      },
      undefined,
      async () => {
        if (failure === 'interrupt' && !interruptFailed) {
          interruptFailed = true;
          throw new Error('interrupt RPC failed');
        }
        if (failure === 'submit') rejectSubmit(new Error('SDK transport failed'));
      },
    );
    f.setRun(async () => {
      if (failure !== 'tool') {
        await new Promise<never>((_resolve, reject) => {
          rejectSubmit = reject;
        });
      } else {
        f.emit({
          type: 'tool_call',
          id: 'failed_kill',
          name: 'bash',
          arguments: {},
          origin: { eventId: 'evt_failed_kill', subpath: '' },
        });
        await vi.waitFor(() => expect(f.replies.some((c) => c.type === 'tool_result')).toBe(true));
        f.complete();
      }
    });
    f.close.mockImplementation(async () => {
      rejectSubmit?.(new Error('worker closed'));
    });
    await f.harness.start(f.input);
    const outcome = f.harness.submit(message).catch((error: unknown) => error);
    await vi.waitFor(() =>
      expect(
        failure === 'tool' ? f.events.some((e) => e.kind === 'agent.tool_use') : rejectSubmit,
      ).toBeTruthy(),
    );
    send();
    await outcome;
    expect(f.close).toHaveBeenCalled();
    expect(f.turns.snapshot.receipt).toMatchObject({
      phase: 'settled',
      error: expect.stringContaining('root interruption unconfirmed'),
    });
    expect(f.turns.snapshot.state).toBeNull();
  },
);

it.each(['deadline', 'interrupt'])(
  'shares cancellation budgets and preserves the first %s cause',
  async (first) => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
    let send!: () => void;
    const accepted = vi.fn(async () => {});
    const control = { id: 'evt_overlapping_interrupt', kind: 'user.interrupt', payload: {} };
    const interrupt = vi.fn().mockImplementationOnce(() => new Promise<void>(() => {}));
    const f = fixture(
      {
        agentSnapshot: {
          model_provider: 'deepseek',
          model_id: 'deepseek-v4-flash',
          tool_permission_policies: { spawn: 'always_ask' },
        },
      },
      undefined,
      1000,
      async function* (_turn, _signal, ready) {
        const incoming = new Promise<void>((resolve) => {
          send = resolve;
        });
        ready();
        await incoming;
        yield { event: control, hooks: { onAccepted: accepted } };
      },
      undefined,
      interrupt,
    );
    f.close.mockImplementationOnce(() => new Promise<void>(() => {}));
    f.setRun(async () => {
      void f.request('pending_spawn');
      await new Promise<void>(() => {});
    });
    try {
      await f.harness.start(f.input);
      await f.harness.submit(message);
      expect(f.harness.hasPendingRequiredAction()).toBe(true);
      if (first === 'deadline') await vi.advanceTimersByTimeAsync(15_000);
      send();
      await vi.advanceTimersByTimeAsync(0);
      expect(accepted).toHaveBeenCalledOnce();
      expect(f.turns.snapshot.receipt?.sourceIds).toEqual([message.id, control.id]);
      expect(interrupt).toHaveBeenCalledOnce();
      // The deadline can fire while an explicit interrupt is still awaiting cleanup.
      if (first === 'interrupt') await vi.advanceTimersByTimeAsync(1000);
      await vi.advanceTimersByTimeAsync(first === 'deadline' ? 999 : 13_999);
      expect(f.close).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(f.close).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(14_999);
      expect(f.turns.snapshot.receipt?.phase).toBe('pending');
      await vi.advanceTimersByTimeAsync(1);
      expect(f.turns.snapshot.receipt).toMatchObject({
        phase: 'settled',
        sourceIds: [message.id, control.id],
        error:
          (first === 'deadline' ? 'DSH SDK turn timed out' : 'DSH SDK turn interrupted') +
          '; root interruption unconfirmed: DSH root interrupt cleanup timed out' +
          '; retirement unconfirmed: DSH tree retirement cleanup timed out',
      });
      expect(f.events.find((event) => event.kind === 'session.error')?.payload).toMatchObject({
        error: { message: f.turns.snapshot.receipt!.error },
      });
      expect(f.events.filter((event) => event.kind === 'session.status_idle').at(-1)).toMatchObject(
        {
          completionPolicy: { sourceIds: [message.id, control.id] },
          payload: { stop_reason: { type: 'retries_exhausted' } },
        },
      );
    } finally {
      vi.useRealTimers();
    }
  },
);

it.each(['interrupt', 'interrupt-reject', 'submit', 'tool-result', 'retirement'])(
  'settles an approval deadline with stalled %s and fences late work',
  async (stalled) => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
    const directory = await mkdtemp(join(tmpdir(), 'dsh-deadline-'));
    const marker = join(directory, 'forbidden');
    const execute = vi.fn(async () => {
      const { writeFile } = await import('node:fs/promises');
      await writeFile(marker, 'forbidden');
      return { content: 'done' };
    });
    let rejectSubmit!: (error: Error) => void;
    let rejectToolResult!: (error: Error) => void;
    const pendingSubmit = new Promise<never>((_, reject) => {
      rejectSubmit = reject;
    });
    const pendingToolResult = new Promise<never>((_, reject) => {
      rejectToolResult = reject;
    });
    // Unused stalled promises also have owners before the deliberate late rejection.
    void pendingSubmit.catch(() => {});
    void pendingToolResult.catch(() => {});
    const interrupt = vi
      .fn()
      .mockImplementationOnce(() =>
        stalled === 'interrupt'
          ? new Promise<void>(() => {})
          : stalled === 'interrupt-reject'
            ? Promise.reject(new Error('interrupt rejected'))
            : Promise.resolve(),
      );
    const f = fixture(
      {
        supportsPerCallCancellation: true,
        tools: [{ name: 'bash', description: '', input_schema: { type: 'object' }, execute }],
        agentSnapshot: {
          model_provider: 'deepseek',
          model_id: 'deepseek-v4-flash',
          allowed_tool_names: ['bash'],
          tool_permission_policies: { mcp__orca__bash: 'always_ask' },
        },
      },
      undefined,
      1000,
      undefined,
      undefined,
      interrupt,
      () => (stalled === 'tool-result' ? pendingToolResult : Promise.resolve()),
    );
    if (stalled === 'retirement') f.close.mockImplementationOnce(() => new Promise<void>(() => {}));
    f.setRun(async () => {
      f.emit({
        type: 'tool_call',
        id: 'deadline_bash',
        name: 'bash',
        arguments: {},
        origin: { eventId: 'evt_deadline_bash', subpath: '' },
      });
      if (stalled !== 'tool-result') await pendingSubmit;
    });
    try {
      await f.harness.start(f.input);
      await f.harness.submit(message);
      expect(f.harness.hasPendingRequiredAction()).toBe(true);
      await vi.advanceTimersByTimeAsync(1000);
      expect(f.harness.hasPendingRequiredAction()).toBe(false);
      // Abort fences worker output even while cleanup is pending and retirement has not run.
      f.emit({
        type: 'canonical_event',
        event: { id: 'evt_late_canonical', subpath: '', kind: 'agent.message', payload: {} },
      });
      f.emit({
        type: 'event',
        event: {
          type: 'item.completed',
          item: { type: 'agent_message', id: 'late_message', text: 'late output' },
        },
        origin: { eventId: 'evt_late_message', subpath: '' },
      });
      f.emit({
        type: 'checkpoint',
        checkpoint: { version: 1, threadId: 'late', format: 'dsh_sdk', files: {} },
      });
      f.emit({
        type: 'tool_call',
        id: 'late_before_retirement',
        name: 'bash',
        arguments: {},
        origin: { eventId: 'evt_late_before_retirement', subpath: '' },
      });
      await vi.advanceTimersByTimeAsync(0);
      expect(f.events.some((event) => event.id.startsWith('evt_late_'))).toBe(false);
      await expect(
        f.harness.submit({
          id: 'evt_late_approval',
          kind: 'user.tool_confirmation',
          payload: { tool_use_id: 'evt_deadline_bash', result: 'allow' },
        }),
      ).rejects.toThrow('No pending');
      await vi.advanceTimersByTimeAsync(15_000);
      if (stalled === 'retirement' || stalled === 'tool-result') {
        expect(f.turns.snapshot.receipt?.phase).toBe('pending');
        await vi.advanceTimersByTimeAsync(15_000);
      }
      expect(f.turns.snapshot.receipt).toMatchObject({
        phase: 'settled',
        sourceIds: [message.id],
        error: expect.stringContaining('turn timed out'),
      });
      expect(f.harness.hasSettledFailure()).toBe(true);
      expect(f.events.find((e) => e.kind === 'session.error')?.payload).toMatchObject({
        error: { message: expect.stringContaining('turn timed out') },
        retry_status: { will_retry: false },
      });
      expect(f.events.filter((e) => e.kind === 'session.status_idle').at(-1)).toMatchObject({
        payload: { stop_reason: { type: 'retries_exhausted' } },
        completionPolicy: { sourceIds: [message.id] },
      });
      const count = f.events.length;
      f.complete();
      f.emit({
        type: 'tool_call',
        id: 'late',
        name: 'bash',
        arguments: {},
        origin: { eventId: 'evt_late_tool', subpath: '' },
      });
      rejectSubmit(new Error('late submit rejection'));
      rejectToolResult(new Error('late tool delivery rejection'));
      await vi.advanceTimersByTimeAsync(0);
      expect(f.events).toHaveLength(count);
      expect(f.turns.snapshot.state).toBeNull();
      expect(execute).not.toHaveBeenCalled();
      expect(f.events.some((e) => e.kind === 'agent.usage')).toBe(false);
      await expect(readFile(marker)).rejects.toMatchObject({ code: 'ENOENT' });
    } finally {
      vi.useRealTimers();
      await rm(directory, { recursive: true, force: true });
    }
  },
);

it('does not execute an approval accepted concurrently with deadline cancellation', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
  const execute = vi.fn(async () => ({ content: 'forbidden' }));
  const f = fixture(
    {
      supportsPerCallCancellation: true,
      tools: [{ name: 'bash', description: '', input_schema: { type: 'object' }, execute }],
      agentSnapshot: {
        model_provider: 'deepseek',
        model_id: 'deepseek-v4-flash',
        allowed_tool_names: ['bash'],
        tool_permission_policies: { mcp__orca__bash: 'always_ask' },
      },
    },
    undefined,
    1000,
  );
  f.setRun(async () => {
    f.emit({
      type: 'tool_call',
      id: 'racing',
      name: 'bash',
      arguments: {},
      origin: { eventId: 'evt_racing', subpath: '' },
    });
    await new Promise<void>(() => {});
  });
  try {
    await f.harness.start(f.input);
    await f.harness.submit(message);
    await f.harness.submit(
      {
        id: 'evt_racing_approval',
        kind: 'user.tool_confirmation',
        payload: { tool_use_id: 'evt_racing', result: 'allow' },
      },
      {
        onAccepted: async () => {
          await vi.advanceTimersByTimeAsync(1000);
        },
      },
    );
    await vi.advanceTimersByTimeAsync(15_000);
    expect(f.turns.snapshot.receipt).toMatchObject({
      phase: 'settled',
      error: expect.stringContaining('timed out'),
    });
    expect(execute).not.toHaveBeenCalled();
    expect(f.harness.hasPendingRequiredAction()).toBe(false);
  } finally {
    vi.useRealTimers();
  }
});

it.each([false, true])(
  'settles deadline with acknowledged terminal usage=%s without inventing usage',
  async (known) => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
    const onUsage = vi.fn(async () => ({ total_tokens: 2 }));
    const onGuardrailState = vi.fn(async () => {});
    const f = fixture(
      {
        guardrails: [
          {
            id: 'budget',
            name: 'budget',
            tier: 'session',
            phases: ['request'],
            stateful: true,
            rule: { kind: 'builtin', builtin: 'token_budget', params: { max_total_tokens: 100 } },
          },
        ],
      },
      undefined,
      1000,
      undefined,
      undefined,
      undefined,
      undefined,
      { onUsage, onGuardrailState },
    );
    f.setRun(async () => {
      if (known) f.complete();
      await new Promise<void>(() => {});
    });
    try {
      await f.harness.start(f.input);
      const outcome = f.harness.submit(message).catch((error: unknown) => error);
      await vi.waitFor(() =>
        expect(f.replies.some((command) => command.type === 'submit')).toBe(true),
      );
      await vi.advanceTimersByTimeAsync(16_000);
      await outcome;
      expect(f.turns.snapshot.receipt).toMatchObject({
        phase: 'settled',
        error: expect.stringContaining('timed out'),
      });
      expect(f.turns.snapshot.state).toBeNull();
      expect(onUsage).toHaveBeenCalledTimes(known ? 1 : 0);
      expect(f.events.filter((event) => event.kind === 'agent.usage')).toHaveLength(known ? 1 : 0);
      if (!known) {
        expect(f.harness.hasPendingGuardrailUsage()).toBe(true);
        await expect(f.harness.submit({ ...message, id: 'evt_guarded_recovery' })).rejects.toThrow(
          'pending usage accounting',
        );
      }
    } finally {
      vi.useRealTimers();
    }
  },
);

it('settles a pending Bash deadline through the real native DSH worker', async () => {
  let requests = 0;
  const newline = String.fromCharCode(10);
  const server = createServer((request, response) => {
    request.resume();
    request.on('end', () => {
      requests += 1;
      response.writeHead(200, { 'content-type': 'text/event-stream' });
      const frames = [
        {
          type: 'message_start',
          message: {
            id: 'fixture',
            type: 'message',
            role: 'assistant',
            model: 'deepseek-v4-flash',
            content: [],
            usage: { input_tokens: 4, output_tokens: 0 },
          },
        },
        {
          type: 'content_block_start',
          index: 0,
          content_block: { type: 'tool_use', id: 'native-bash', name: 'bash', input: {} },
        },
        {
          type: 'content_block_delta',
          index: 0,
          delta: {
            type: 'input_json_delta',
            partial_json: JSON.stringify({ command: 'forbidden' }),
          },
        },
        { type: 'content_block_stop', index: 0 },
        {
          type: 'message_delta',
          delta: { stop_reason: 'tool_use', stop_sequence: null },
          usage: { output_tokens: 2 },
        },
        { type: 'message_stop' },
      ];
      response.end(
        frames
          .map(
            (frame) =>
              'event: ' +
              frame.type +
              newline +
              'data: ' +
              JSON.stringify(frame) +
              newline +
              newline,
          )
          .join(''),
      );
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const execute = vi.fn(async () => ({ content: 'forbidden' }));
  const f = fixture(
    {
      supportsPerCallCancellation: true,
      tools: [{ name: 'bash', description: '', input_schema: { type: 'object' }, execute }],
      agentSnapshot: {
        model_provider: 'deepseek',
        model_id: 'deepseek-v4-flash',
        allowed_tool_names: ['bash'],
        tool_permission_policies: { mcp__orca__bash: 'always_ask' },
      },
    },
    undefined,
    2000,
    undefined,
    undefined,
    undefined,
    undefined,
    {
      createWorker: (emit) => new DshSdkWorker(emit),
      dshGatewayUrl: 'http://127.0.0.1:' + (server.address() as { port: number }).port,
    },
  );
  try {
    await f.harness.start(f.input);
    await f.harness.submit(message);
    expect(f.harness.hasPendingRequiredAction()).toBe(true);
    const tool = f.events.find((event) => event.kind === 'agent.tool_use')!;
    await vi.waitFor(() => expect(f.turns.snapshot.receipt?.phase).toBe('settled'), {
      timeout: 35_000,
    });
    expect(f.turns.snapshot.receipt?.error).toContain('timed out');
    expect(f.events.filter((event) => event.kind === 'session.status_idle').at(-1)).toMatchObject({
      payload: { stop_reason: { type: 'retries_exhausted' } },
      completionPolicy: { sourceIds: [message.id] },
    });
    await expect(
      f.harness.submit({
        id: 'evt_native_late',
        kind: 'user.tool_confirmation',
        payload: { tool_use_id: tool.id, result: 'allow' },
      }),
    ).rejects.toThrow('No pending');
    expect(execute).not.toHaveBeenCalled();
    expect(requests).toBe(1);
    expect(f.turns.snapshot.state).toBeNull();
  } finally {
    await f.harness.stop('error');
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}, 40_000);

it.each([600_000, 1_200_000])(
  'subtracts credential refresh time from the DSH submit budget %s',
  async (budget) => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
    let refreshing = false;
    const f = fixture({}, undefined, budget, undefined, undefined, undefined, undefined, {
      llmGatewayJwtProvider: {
        getValidToken: async () => {
          if (refreshing) await vi.advanceTimersByTimeAsync(20_000);
          return { token: 'jwt' } as never;
        },
        close: () => {},
      },
    });
    f.setRun(async () => f.complete());
    try {
      await f.harness.start(f.input);
      refreshing = true;
      await f.harness.submit(message);
      expect(f.replies.find((command) => command.type === 'submit')).toMatchObject({
        executionBudgetMs: budget - 20_000,
      });
    } finally {
      await f.harness.stop('error');
      vi.useRealTimers();
    }
  },
);

it('never admits native submit when refresh consumes the remaining host budget', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
  let refreshing = false;
  const f = fixture({}, undefined, 1_200_000, undefined, undefined, undefined, undefined, {
    llmGatewayJwtProvider: {
      getValidToken: async () => {
        if (refreshing) await vi.advanceTimersByTimeAsync(1_200_000);
        return { token: 'jwt' } as never;
      },
      close: () => {},
    },
  });
  try {
    await f.harness.start(f.input);
    refreshing = true;
    await expect(f.harness.submit(message)).rejects.toThrow('timed out');
    expect(f.replies.filter((command) => command.type === 'submit')).toEqual([]);
    expect(f.turns.snapshot.receipt).toMatchObject({
      phase: 'settled',
      error: expect.stringContaining('timed out'),
    });
    expect(f.turns.snapshot.state).toBeNull();
  } finally {
    await f.harness.stop('error');
    vi.useRealTimers();
  }
});

it.each([900_000, -900_000])(
  'ignores wall clock correction %s during refresh at the maximum configured budget',
  async (correction) => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
    let refreshing = false;
    const f = fixture({}, undefined, 2_147_483_647, undefined, undefined, undefined, undefined, {
      llmGatewayJwtProvider: {
        getValidToken: async () => {
          if (refreshing) {
            await vi.advanceTimersByTimeAsync(1_000);
            vi.setSystemTime(Date.now() + correction);
          }
          return { token: 'jwt' } as never;
        },
        close: () => {},
      },
    });
    f.setRun(async () => f.complete());
    try {
      await f.harness.start(f.input);
      refreshing = true;
      await f.harness.submit(message);
      expect(f.replies.find((command) => command.type === 'submit')).toMatchObject({
        executionBudgetMs: 2_147_482_647,
      });
    } finally {
      await f.harness.stop('error');
      vi.useRealTimers();
    }
  },
);

it('rounds an unexpired sub-millisecond execution budget up to a valid integer', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'performance', 'setTimeout', 'clearTimeout'] });
  const clock = vi.spyOn(performance, 'now');
  let refreshing = false;
  const f = fixture({}, undefined, 1, undefined, undefined, undefined, undefined, {
    llmGatewayJwtProvider: {
      getValidToken: async () => {
        // The fake timer's integer tick does not model performance.now()'s fractional precision.
        if (refreshing) clock.mockReturnValue(0.75);
        return { token: 'jwt' } as never;
      },
      close: () => {},
    },
  });
  f.setRun(async () => f.complete());
  try {
    await f.harness.start(f.input);
    refreshing = true;
    await f.harness.submit(message);
    expect(f.replies.find((command) => command.type === 'submit')).toMatchObject({
      executionBudgetMs: 1,
    });
  } finally {
    await f.harness.stop('error');
    clock.mockRestore();
    vi.useRealTimers();
  }
});

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
