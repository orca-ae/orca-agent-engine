// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { turnStoreFixture } from '../support/codex-turn-store.js';
import {
  customResultCases,
  pngBase64,
} from '../../../../packages/codex-harness/test/support/custom-tool-results.js';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import type { CodexCheckpoint, WorkerCommand, WorkerEvent } from '@orca/codex-harness';
import { CodexSdkHarness, type CodexSdkHarnessOptions } from '../../src/harness/codex-sdk/index.js';
import {
  GuardrailUsageUnavailableError,
  type AgentEvent,
  type SessionStartInput,
} from '../../src/harness/agent-harness.js';

const checkpoint: CodexCheckpoint = {
  version: 1,
  threadId: 'thread-test',
  instructionsSha256: createHash('sha256').update('Developer policy').digest('hex'),
  files: {
    'sessions/2026/09/20/rollout-thread-test.jsonl':
      Buffer.from('{"history":"private"}').toString('base64'),
  },
};
const input: SessionStartInput = {
  workspaceId: 'ws_test',
  sessionId: 'ses_test',
  agentSnapshot: {
    model_provider: 'openai',
    model_id: 'gpt-5.4-mini',
    system: 'Developer policy',
    allowed_tool_names: ['bash'],
    tool_permission_policies: { mcp__orca__bash: 'always_allow' },
  },
};
const message = {
  id: 'evt_user_turn',
  kind: 'user.message',
  payload: { content: [{ type: 'text', text: 'run the tool' }] },
};
const stopped: CodexSdkHarness[] = [];
afterEach(async () => {
  await Promise.all(stopped.splice(0).map((h) => h.stop('client.archived')));
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
type TestOptions = Partial<CodexSdkHarnessOptions> & {
  onCheckpoint?: (state: CodexCheckpoint) => Promise<void>;
};
function setup(opts: TestOptions = {}) {
  let emit!: (event: WorkerEvent) => void;
  const toolResult = deferred<void>();
  let run: () => Promise<void> = async () => {
    emit({
      type: 'tool_call',
      id: 'native_call',
      name: 'bash',
      arguments: { command: 'echo sandbox' },
    });
    await toolResult.promise;
    emit({
      type: 'event',
      event: {
        type: 'item.completed',
        item: { id: 'response', type: 'agent_message', text: 'finished' },
      },
    });
    emit({
      type: 'event',
      event: {
        type: 'turn.completed',
        usage: {
          input_tokens: 15,
          cached_input_tokens: 4,
          output_tokens: 2,
          cache_write_input_tokens: 0,
          reasoning_output_tokens: 0,
        },
      },
    });
    emit({ type: 'checkpoint', checkpoint });
    emit({ type: 'done' });
  };
  const handle = vi.fn(async (command: WorkerCommand) => {
    if (command.type === 'submit') await run();
    if (command.type === 'tool_result' || command.type === 'interrupt') toolResult.resolve();
  });
  const refreshOptions = vi.fn();
  const onCheckpoint = vi.fn(async () => {});
  const harness = new CodexSdkHarness({
    apiKey: 'test-key',
    ...opts,
    turns:
      opts.turns ??
      turnStoreFixture({
        ...(opts.checkpoint ? { checkpoint: opts.checkpoint } : {}),
        onCommit: async (state, receipt) => {
          await (opts.onCheckpoint ?? onCheckpoint)(state);
          if (receipt.guarded && !receipt.error)
            await opts.onGuardrailState?.([
              {
                scope: 'session',
                key: `codex_sdk_usage_pending:${receipt.turnId}`,
                action: 'delete',
              },
            ]);
        },
      }).store,
    createWorker: (send) => {
      emit = send;
      return { handle, refreshOptions, close: vi.fn() };
    },
  });
  stopped.push(harness);
  const events: AgentEvent[] = [];
  const pump = (async () => {
    for await (const event of harness.events()) {
      events.push(event);
      event.persistence?.resolve();
    }
  })();
  const execute = vi.fn(async () => ({ content: 'remote result' }));
  const start = (changes: Partial<SessionStartInput> = {}) =>
    harness.start({
      ...input,
      tools: [
        { name: 'bash', description: 'Sandbox bash', input_schema: { type: 'object' }, execute },
      ],
      ...changes,
    });
  return {
    harness,
    events,
    pump,
    handle,
    refreshOptions,
    onCheckpoint,
    execute,
    start,
    setRun: (fn: (send: typeof emit) => Promise<void>) => {
      run = () => fn(emit);
    },
  };
}
function idle(events: AgentEvent[], type: string) {
  return events.find(
    (e) =>
      e.kind === 'session.status_idle' &&
      (e.payload as { stop_reason: { type: string } }).stop_reason.type === type,
  );
}

describe('Codex SDK separate harness', () => {
  it('rejects a changed catalog before starting the worker or replacing native history', async () => {
    const h = setup({ checkpoint });
    await expect(
      h.start({
        agentSnapshot: {
          ...input.agentSnapshot,
          system: 'Developer policy\n<available_skills>changed</available_skills>',
        },
      }),
    ).rejects.toThrow('checkpoint developer instructions differ');
    expect(h.handle).not.toHaveBeenCalled();
    expect(h.onCheckpoint).not.toHaveBeenCalled();
    expect(h.execute).not.toHaveBeenCalled();
  });

  it.each(['always_allow', 'always_ask', 'always_deny'] as const)(
    'forwards the managed Skill catalog and applies %s to Skill reads',
    async (policy) => {
      const h = setup();
      const path = '/workspace/skills/report-skill/SKILL.md';
      const system = [
        'Base instructions',
        '<available_skills>',
        JSON.stringify({ name: 'report-skill', description: 'Prepare reports', path }),
        '</available_skills>',
      ].join('\n');
      h.setRun(async (emit) => {
        emit({ type: 'tool_call', id: 'skill_read', name: 'read', arguments: { path } });
        await vi.waitFor(() =>
          expect(h.handle.mock.calls.some(([command]) => command.type === 'tool_result')).toBe(
            true,
          ),
        );
        emit({ type: 'done' });
      });
      await h.start({
        agentSnapshot: {
          ...input.agentSnapshot,
          system,
          allowed_tool_names: ['read'],
          tool_permission_policies: { mcp__orca__read: policy },
        },
        tools: [
          {
            name: 'read',
            description: 'Read files',
            input_schema: { type: 'object' },
            execute: h.execute,
          },
        ],
      });
      expect(h.handle.mock.calls[0]![0]).toMatchObject({
        type: 'start',
        system,
        tools: [{ name: 'read' }],
      });
      await h.harness.submit(message);
      if (policy === 'always_ask') {
        expect(h.execute).not.toHaveBeenCalled();
        const tool = h.events.find((event) => event.kind === 'agent.tool_use')!;
        expect(idle(h.events, 'requires_action')).toBeDefined();
        await h.harness.submit({
          id: 'evt_control_6820',
          kind: 'user.tool_confirmation',
          payload: { tool_use_id: tool.id, result: 'allow' },
        });
      }
      if (policy === 'always_deny') expect(h.execute).not.toHaveBeenCalled();
      else expect(h.execute).toHaveBeenCalledWith({ path });
      expect(h.handle).toHaveBeenCalledWith({
        type: 'tool_result',
        id: 'skill_read',
        result: expect.objectContaining({ isError: policy === 'always_deny' }),
      });
    },
  );

  it('uses the managed always_allow default when a sandbox tool has no explicit policy', async () => {
    const h = setup();
    const snapshot = { ...input.agentSnapshot };
    delete snapshot.tool_permission_policies;
    await h.start({ agentSnapshot: snapshot });
    await h.harness.submit(message);
    await vi.waitFor(() => expect(idle(h.events, 'end_turn')).toBeDefined());
    expect(h.execute).toHaveBeenCalledTimes(1);
    expect(idle(h.events, 'requires_action')).toBeUndefined();
    expect(h.onCheckpoint).toHaveBeenCalledWith(checkpoint);
  });

  it('joins startup during shutdown and closes late-created SDK resources', async () => {
    const key = deferred<{ token: string; expiresAt: number }>();
    const getValidToken = vi.fn(() => key.promise);
    const handle = vi.fn(async () => {});
    const close = vi.fn(async () => {});
    const harness = new CodexSdkHarness({
      apiKey: '',
      turns: turnStoreFixture().store,
      llmGatewayJwtProvider: {
        getValidToken,
        close: () => key.resolve({ token: 'token', expiresAt: Date.now() / 1000 + 300 }),
      },
      createWorker: () => ({ handle, close, refreshOptions: vi.fn() }),
    });
    const starting = harness.start({
      ...input,
      agentSnapshot: { ...input.agentSnapshot, allowed_tool_names: [] },
    });
    const startupResult = expect(starting).rejects.toThrow('stopped during startup');
    await vi.waitFor(() => expect(getValidToken).toHaveBeenCalled());
    const root = harness['root']!;
    await harness.stop('replica.shutting_down');
    await startupResult;
    expect(handle).toHaveBeenCalledTimes(1);
    expect(handle).toHaveBeenCalledWith({ type: 'interrupt' });
    expect(close).toHaveBeenCalledTimes(1);
    await expect(stat(root)).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('blocks subsequent turns after native checkpoint export fails', async () => {
    const h = setup();
    h.setRun(async (emit) => {
      emit({ type: 'failure', message: 'Native history could not be checkpointed', fatal: true });
    });
    await h.start();
    await expect(h.harness.submit(message)).rejects.toThrow(
      'Native history could not be checkpointed',
    );
    expect(idle(h.events, 'retries_exhausted')).toBeDefined();
    await expect(h.harness.submit(message)).rejects.toThrow('unavailable');
    expect(h.execute).not.toHaveBeenCalled();
  });

  it.each(['request', 'tool_call', 'tool_result'])(
    'enforces stateless %s guardrails at the corresponding boundary',
    async (phase) => {
      const h = setup();
      await h.start({
        guardrails: [
          {
            id: 'gr_deny',
            name: 'test deny',
            tier: 'workspace',
            phases: [phase],
            stateful: false,
            rule: {
              kind: 'expression',
              expression: 'false',
              onFalse: 'deny',
              reason: 'blocked by test policy',
            },
          },
        ],
      });
      if (phase === 'request') {
        await expect(h.harness.submit(message)).rejects.toThrow('blocked by test policy');
        expect(h.handle.mock.calls.some(([command]) => command.type === 'submit')).toBe(false);
      } else {
        await h.harness.submit(message);
        const response = h.handle.mock.calls
          .map(([command]) => command)
          .find((command) => command.type === 'tool_result');
        expect(response).toMatchObject({ result: { isError: true } });
        expect(JSON.stringify(response)).not.toContain('remote result');
      }
      expect(h.execute).toHaveBeenCalledTimes(phase === 'tool_result' ? 1 : 0);
    },
  );
  it('runs in a private host directory, routes tools through the supplied Sandbox, and saves history before idle', async () => {
    const persisted = deferred<void>();
    const entered = deferred<void>();
    const onCheckpoint = vi.fn(async () => {
      entered.resolve();
      await persisted.promise;
    });
    const h = setup({ onCheckpoint });
    await h.start();
    const startCommand = h.handle.mock.calls[0]![0];
    expect(startCommand.type).toBe('start');
    if (startCommand.type !== 'start') throw new Error('missing start');
    expect(startCommand.root).not.toBe(process.cwd());
    expect((await stat(startCommand.root)).mode & 0o777).toBe(0o700);
    const onAccepted = vi.fn(async () => {
      expect(h.execute).not.toHaveBeenCalled();
    });
    const turn = h.harness.submit(message, { onAccepted });
    await entered.promise;
    expect(h.execute).toHaveBeenCalledWith({ command: 'echo sandbox' });
    expect(onAccepted).toHaveBeenCalledTimes(1);
    expect(idle(h.events, 'end_turn')).toBeUndefined();
    persisted.resolve();
    await turn;
    expect(idle(h.events, 'end_turn')).toBeDefined();
    expect(h.events.find((e) => e.kind === 'agent.usage')?.payload).toMatchObject({
      model: 'gpt-5.4-mini',
      turn_event_id: message.id,
      usage: { input_tokens: 11, cache_read_input_tokens: 4, output_tokens: 2 },
    });
    expect(JSON.stringify(h.events)).not.toContain('history');
    expect(h.events.every((e) => e.id.startsWith('evt_') && e.subpath === '')).toBe(true);
    await h.harness.stop('client.archived');
    await expect(stat(startCommand.root)).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it.each(['allow', 'deny'])(
    'uses durable event IDs for tool confirmation %s and accepts before executing',
    async (result) => {
      const h = setup();
      await h.start({
        agentSnapshot: {
          ...input.agentSnapshot,
          tool_permission_policies: { mcp__orca__bash: 'always_ask' },
        },
      });
      await h.harness.submit(message);
      const tool = h.events.find((e) => e.kind === 'agent.tool_use')!;
      expect(tool.payload).toMatchObject({ id: tool.id });
      expect(idle(h.events, 'requires_action')?.payload).toEqual({
        stop_reason: { type: 'requires_action', event_ids: [tool.id] },
      });
      expect(h.execute).not.toHaveBeenCalled();
      const accepted = vi.fn(async () => {
        expect(h.execute).not.toHaveBeenCalled();
      });
      await expect(
        h.harness.submit(
          {
            id: 'evt_control_13364',
            kind: 'user.tool_confirmation',
            payload: { tool_use_id: 'evt_wrong', result },
          },
          { onAccepted: accepted },
        ),
      ).rejects.toThrow('No pending');
      expect(accepted).not.toHaveBeenCalled();
      expect(await h.harness.submit(message, { onAccepted: accepted })).toBe('deferred');
      expect(accepted).not.toHaveBeenCalled();
      await h.harness.submit(
        {
          id: 'evt_control_13755',
          kind: 'user.tool_confirmation',
          payload: { tool_use_id: tool.id, result },
        },
        { onAccepted: accepted },
      );
      expect(accepted).toHaveBeenCalledTimes(1);
      expect(h.execute).toHaveBeenCalledTimes(result === 'allow' ? 1 : 0);
      expect(h.events.find((e) => e.kind === 'agent.tool_result')?.payload).toMatchObject({
        tool_use_id: tool.id,
        is_error: result === 'deny',
      });
      expect(idle(h.events, 'end_turn')).toBeDefined();
    },
  );

  it('never executes always_deny tools or requests confirmation for them', async () => {
    const h = setup();
    await h.start({
      agentSnapshot: {
        ...input.agentSnapshot,
        tool_permission_policies: { mcp__orca__bash: 'always_deny' },
      },
    });
    await h.harness.submit(message);
    expect(h.execute).not.toHaveBeenCalled();
    expect(idle(h.events, 'requires_action')).toBeUndefined();
    const use = h.events.find((e) => e.kind === 'agent.tool_use')!;
    expect(h.events.find((e) => e.kind === 'agent.tool_result')?.payload).toMatchObject({
      tool_use_id: use.id,
      is_error: true,
    });
  });

  it('releases a waiting approval on interrupt without executing the tool', async () => {
    const h = setup();
    await h.start({
      agentSnapshot: {
        ...input.agentSnapshot,
        tool_permission_policies: { mcp__orca__bash: 'always_ask' },
      },
    });
    await h.harness.submit(message);
    expect(h.harness.hasPendingRequiredAction()).toBe(true);
    await h.harness.submit({ id: 'evt_control_15280', kind: 'user.interrupt', payload: {} });
    expect(h.harness.hasPendingRequiredAction()).toBe(false);
    expect(h.execute).not.toHaveBeenCalled();
    expect(idle(h.events, 'retries_exhausted')).toBeDefined();
  });

  it('does not persist late interrupted checkpoints after retiring a worker', async () => {
    const h = setup();
    await h.start({
      agentSnapshot: {
        ...input.agentSnapshot,
        tool_permission_policies: { mcp__orca__bash: 'always_ask' },
      },
    });
    await h.harness.submit(message);
    expect(h.onCheckpoint).not.toHaveBeenCalled();
    await h.harness.stop('replica.shutting_down');
    expect(h.onCheckpoint).not.toHaveBeenCalled();
    expect(h.execute).not.toHaveBeenCalled();
    expect(idle(h.events, 'end_turn')).toBeUndefined();
  });

  it.each(customResultCases)(
    'returns a $name custom tool result through the client gate',
    async ({ content, name, proof }) => {
      const h = setup();
      h.setRun(async (emit) => {
        emit({ type: 'tool_call', id: 'native_custom', name: 'lookup', arguments: { key: 'x' } });
        await vi.waitFor(() =>
          expect(h.handle.mock.calls.some(([command]) => command.type === 'tool_result')).toBe(
            true,
          ),
        );
        emit({ type: 'checkpoint', checkpoint });
      });
      await h.start({
        agentSnapshot: {
          ...input.agentSnapshot,
          custom_tools: [{ name: 'lookup', input_schema: { type: 'object' } }],
        },
      });
      await h.harness.submit(message);
      const tool = h.events.find((e) => e.kind === 'agent.custom_tool_use')!;
      await h.harness.submit({
        id: 'evt_control_16955',
        kind: 'user.custom_tool_result',
        payload: { custom_tool_use_id: tool.id, content },
      });
      expect(h.handle).toHaveBeenCalledWith({
        type: 'tool_result',
        id: 'native_custom',
        result: { content: expect.any(Array), isError: false },
      });
      const returned = h.handle.mock.calls
        .map(([command]) => command)
        .find((command) => command.type === 'tool_result')!;
      if (returned.type !== 'tool_result') throw new Error('missing result');
      if (name === 'inline image')
        expect(returned.result.content).toEqual([
          { type: 'image', data: pngBase64, mimeType: 'image/png' },
        ]);
      else expect(JSON.stringify(returned.result.content)).toContain(proof);
      expect(
        h.events.filter((event) => ['agent.tool_use', 'agent.tool_result'].includes(event.kind)),
      ).toEqual([]);
    },
  );

  it('terminates visibly when a public callback format cannot be represented for Codex', async () => {
    const h = setup();
    h.setRun(async (emit) => {
      emit({ type: 'tool_call', id: 'native_custom', name: 'lookup', arguments: {} });
      await vi.waitFor(() =>
        expect(h.handle.mock.calls.some(([command]) => command.type === 'interrupt')).toBe(true),
      );
      emit({ type: 'checkpoint', checkpoint });
    });
    await h.start({
      agentSnapshot: {
        ...input.agentSnapshot,
        custom_tools: [{ name: 'lookup', input_schema: { type: 'object' } }],
      },
    });
    await h.harness.submit(message);
    const tool = h.events.find((event) => event.kind === 'agent.custom_tool_use')!;
    await h.harness.submit({
      id: 'evt_control_18605',
      kind: 'user.custom_tool_result',
      payload: {
        custom_tool_use_id: tool.id,
        content: [{ type: 'document', source: { type: 'file', file_id: 'file_123' } }],
      },
    });
    expect(h.events.find((event) => event.kind === 'session.error')?.payload).toMatchObject({
      error: { message: expect.stringContaining("document source 'file' is not supported") },
    });
    expect(idle(h.events, 'retries_exhausted')).toBeDefined();
    expect(idle(h.events, 'end_turn')).toBeUndefined();
    expect(h.harness.hasPendingRequiredAction()).toBe(false);
  });

  it('fails closed if native history cannot be persisted and never publishes the checkpoint', async () => {
    const h = setup({
      onCheckpoint: async () => {
        throw new Error('revision conflict');
      },
    });
    await h.start();
    await expect(h.harness.submit(message)).rejects.toThrow('persistence was not acknowledged');
    expect(idle(h.events, 'end_turn')).toBeUndefined();
    expect(idle(h.events, 'retries_exhausted')).toBeUndefined();
    expect(h.events.find((e) => e.kind === 'session.error')).toBeUndefined();
    await expect(h.harness.submit(message)).rejects.toThrow('unavailable');
  });

  it('restores checkpoint, rotates gateway credentials without replacing native conversation history', async () => {
    const jwt = {
      getValidToken: vi
        .fn()
        .mockResolvedValueOnce({ token: 'jwt-1' })
        .mockResolvedValue({ token: 'jwt-2' }),
      close: vi.fn(),
    };
    const h = setup({ apiKey: '', checkpoint, llmGatewayJwtProvider: jwt });
    await h.start();
    expect(h.handle.mock.calls[0]![0]).toMatchObject({ checkpoint, apiKey: 'jwt-1' });
    await expect(
      h.harness.submit({
        ...message,
        systemMessage: { content: [{ type: 'text', text: 'Additional policy' }] },
      }),
    ).rejects.toThrow('system.message');
    await h.harness.submit(message);
    expect(h.refreshOptions).toHaveBeenCalledWith({ apiKey: 'jwt-2' });
    expect(h.handle).toHaveBeenCalledWith({ type: 'submit', text: 'run the tool' });
    await h.harness.stop('client.archived');
    expect(jwt.close).toHaveBeenCalledTimes(1);
  });

  it('refuses unsupported guardrails and malformed messages before SDK execution', async () => {
    const h = setup();
    await expect(
      h.start({
        guardrails: [
          {
            id: 'gr_budget',
            name: 'budget',
            tier: 'session',
            phases: ['request', 'tool_call'],
            stateful: true,
            rule: { kind: 'builtin', builtin: 'token_budget', params: { max_total_tokens: 10 } },
          },
        ],
      }),
    ).rejects.toThrow('stateful');
    const plain = setup();
    await plain.start();
    const onAccepted = vi.fn();
    await expect(
      plain.harness.submit(
        { ...message, payload: { content: [{ type: 'image', source: {} }] } },
        { onAccepted },
      ),
    ).rejects.toThrow('text');
    expect(onAccepted).not.toHaveBeenCalled();
  });
});

const tokenGuard = {
  id: 'gr_budget',
  name: 'budget',
  tier: 'session' as const,
  phases: ['request'],
  stateful: true,
  rule: { kind: 'builtin', builtin: 'token_budget', params: { max_total_tokens: 10 } },
};
const pendingKey = `codex_sdk_usage_pending:${message.id}`;
function budgetSetup(opts: TestOptions = {}) {
  const durable: Record<string, unknown> = {};
  const onGuardrailState = vi.fn(
    async (updates: readonly import('@orca/guardrails').StateUpdate[]) => {
      for (const update of updates) {
        if (update.action === 'delete') delete durable[update.key];
        else durable[update.key] = update.value;
      }
    },
  );
  const onUsage = vi.fn(async () => ({ total_tokens: 17, session_cost_usd: 0.1 }));
  const h = setup({ onGuardrailState, onUsage, ...opts });
  return { ...h, durable, onGuardrailState, onUsage };
}
function submitCount(h: ReturnType<typeof setup>) {
  return h.handle.mock.calls.filter(([command]) => command.type === 'submit').length;
}

describe('Codex SDK durable request budgets', () => {
  it('uses authoritative ownership-claim totals instead of stale prepared counters', async () => {
    const turns = turnStoreFixture();
    turns.snapshot.guardrailState = { total_tokens: 17 };
    const h = budgetSetup({ turns: turns.store });
    await h.start({ guardrails: [tokenGuard], guardrailState: { total_tokens: 0 } });
    await expect(h.harness.submit(message)).rejects.toThrow(/budget/);
    expect(submitCount(h)).toBe(0);
  });

  it('retains guarded accounting when a worker reports malformed raw usage followed by a checkpoint', async () => {
    const h = budgetSetup();
    h.setRun(async (emit) => {
      emit({
        type: 'event',
        event: {
          type: 'turn.completed',
          usage: {
            input_tokens: 1,
            cached_input_tokens: 2,
            output_tokens: 1,
            cache_write_input_tokens: 0,
            reasoning_output_tokens: 0,
          },
        },
      });
      emit({ type: 'checkpoint', checkpoint });
    });
    await h.start({ guardrails: [tokenGuard] });
    await expect(h.harness.submit(message)).rejects.toThrow('execution or durable usage failed');
    expect(h.onUsage).not.toHaveBeenCalled();
    expect(h.events.some((event) => event.kind === 'agent.usage')).toBe(false);
    expect(h.durable).toHaveProperty(pendingKey);
    expect(
      h.onGuardrailState.mock.calls
        .flatMap(([updates]) => updates)
        .some((update) => update.action === 'delete'),
    ).toBe(false);
    await expect(h.harness.submit({ ...message, id: 'evt_next' })).rejects.toThrow();
  });

  it.each([
    { rule: tokenGuard.rule, guardrailState: { total_tokens: 10 } },
    {
      rule: { kind: 'builtin', builtin: 'cost_budget', params: { max_cost_usd: 1 } },
      guardrailState: { session_cost_usd: 1 },
    },
  ])(
    'rejects restored totals before model work even with forged stateless metadata',
    async ({ rule, guardrailState }) => {
      const h = budgetSetup();
      await h.start({ guardrails: [{ ...tokenGuard, stateful: false, rule }], guardrailState });
      await expect(h.harness.submit(message)).rejects.toThrow(/budget/);
      expect(submitCount(h)).toBe(0);
      expect(h.onGuardrailState).not.toHaveBeenCalled();
    },
  );

  it('requires durable callbacks for derived stateful rules', async () => {
    const h = setup();
    await expect(h.start({ guardrails: [{ ...tokenGuard, stateful: false }] })).rejects.toThrow(
      'durable',
    );
    expect(h.handle).not.toHaveBeenCalled();
  });

  it('serializes concurrent acceptance and applies usage ACK totals before the next request', async () => {
    const accepted = deferred<void>();
    const firstAccepted = vi.fn(() => accepted.promise);
    const secondAccepted = vi.fn(async () => {});
    const h = budgetSetup();
    await h.start({ guardrails: [tokenGuard] });
    const first = h.harness.submit(message, { onAccepted: firstAccepted });
    await vi.waitFor(() => expect(firstAccepted).toHaveBeenCalledTimes(1));
    const second = h.harness.submit({ ...message, id: 'evt_next' }, { onAccepted: secondAccepted });
    const secondResult = expect(second).rejects.toThrow(/budget/);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(secondAccepted).not.toHaveBeenCalled();
    accepted.resolve();
    await first;
    await secondResult;
    expect(submitCount(h)).toBe(1);
    expect(h.onUsage).toHaveBeenCalledWith(
      expect.objectContaining({ input_tokens: 11, output_tokens: 2, cache_read_input_tokens: 4 }),
      'gpt-5.4-mini',
      undefined,
      message.id,
      expect.stringMatching(/^evt_/),
    );
    const marker = h.onGuardrailState.mock.calls[0]![0].find((u) => u.key === pendingKey)!;
    const usageEvent = h.events.find((event) => event.kind === 'agent.usage')!;
    expect(marker.value).toEqual({
      version: 1,
      turnEventId: message.id,
      usageEventId: usageEvent.id,
    });
    expect(usageEvent.payload).toMatchObject({ guardrail_usage_recorded: true });
    expect(h.durable).not.toHaveProperty(pendingKey);
    expect(h.harness.hasPendingGuardrailUsage()).toBe(false);
  });

  it.each(['marker', 'usage', 'checkpoint', 'clear'] as const)(
    'waits for the %s ACK and retains the marker on failure',
    async (stage) => {
      let reject!: (reason: Error) => void;
      const blocked = new Promise<void>((_, fail) => {
        reject = fail;
      });
      const reached = deferred<void>();
      const durable: Record<string, unknown> = {};
      const h = budgetSetup({
        onGuardrailState: async (updates) => {
          const clear = updates.some((u) => u.action === 'delete');
          if ((stage === 'marker' && !clear) || (stage === 'clear' && clear)) {
            // Model an uncertain ACK: the write may have reached durable storage.
            if (!clear) for (const u of updates) durable[u.key] = u.value;
            reached.resolve();
            await blocked;
          }
          for (const u of updates) {
            if (u.action === 'delete') delete durable[u.key];
            else durable[u.key] = u.value;
          }
        },
        onUsage: async () => {
          if (stage === 'usage') {
            reached.resolve();
            await blocked;
          }
          return { total_tokens: 17 };
        },
        onCheckpoint: async () => {
          if (stage === 'checkpoint') {
            reached.resolve();
            await blocked;
          }
        },
      });
      await h.start({ guardrails: [tokenGuard] });
      const submit = h.harness.submit(message).catch((error) => error);
      await reached.promise;
      expect(h.harness.hasPendingGuardrailUsage()).toBe(true);
      expect(submitCount(h)).toBe(stage === 'marker' ? 0 : 1);
      expect(durable).toHaveProperty(pendingKey);
      reject(new Error(`${stage} ACK unavailable`));
      await submit;
      await expect(h.harness.submit({ ...message, id: 'evt_next' })).rejects.toThrow(
        /accounting|unavailable/,
      );
      expect(durable).toHaveProperty(pendingKey);
      // A fresh adapter also refuses the persisted marker before acceptance.
      const rebuilt = budgetSetup();
      await rebuilt.start({ guardrails: [tokenGuard], guardrailState: durable });
      const onAccepted = vi.fn();
      await expect(
        rebuilt.harness.submit({ ...message, id: 'evt_rebuilt' }, { onAccepted }),
      ).rejects.toBeInstanceOf(GuardrailUsageUnavailableError);
      expect(onAccepted).not.toHaveBeenCalled();
      expect(submitCount(rebuilt)).toBe(0);
    },
  );

  it('recovers a committed marker clear with a lost response only through authoritative reload', async () => {
    const durable: Record<string, unknown> = {};
    const persistenceOrder: string[] = [];
    let durableCheckpoint: CodexCheckpoint | undefined;
    let loseClearResponse = true;
    const onGuardrailState: NonNullable<CodexSdkHarnessOptions['onGuardrailState']> = async (
      updates,
    ) => {
      for (const update of updates) {
        if (update.action === 'delete') delete durable[update.key];
        else durable[update.key] = update.value;
      }
      if (updates.some((update) => update.action === 'delete')) {
        persistenceOrder.push('clear');
        if (loseClearResponse) {
          loseClearResponse = false;
          throw new Error('Marker deletion committed, but its response was lost');
        }
      } else persistenceOrder.push('marker');
    };
    const onUsage = vi.fn(async () => {
      durable.total_tokens = Number(durable.total_tokens ?? 0) + 17;
      persistenceOrder.push('usage');
      return { total_tokens: durable.total_tokens };
    });
    const onCheckpoint = vi.fn(async (state: CodexCheckpoint) => {
      durableCheckpoint = state;
      persistenceOrder.push('checkpoint');
    });
    const guardrails = [
      {
        ...tokenGuard,
        rule: { ...tokenGuard.rule, params: { max_total_tokens: 100 } },
      },
    ];
    const callbacks = { onGuardrailState, onUsage, onCheckpoint };
    const warm = budgetSetup(callbacks);
    await warm.start({ guardrails });
    await expect(warm.harness.submit(message)).rejects.toThrow('persistence was not acknowledged');
    expect(persistenceOrder).toEqual(['marker', 'usage', 'checkpoint', 'clear']);
    expect(durable).toEqual({ total_tokens: 17 });
    expect(durableCheckpoint).toEqual(checkpoint);
    expect(warm.harness.hasPendingGuardrailUsage()).toBe(true);

    const next = { ...message, id: 'evt_after_lost_clear_response' };
    await expect(warm.harness.submit(next)).rejects.toBeInstanceOf(GuardrailUsageUnavailableError);
    expect(submitCount(warm)).toBe(1);
    expect(onUsage).toHaveBeenCalledTimes(1);
    await warm.harness.stop('session.updated');

    // Reload the acknowledged usage and checkpoint with Registry's actual
    // post-commit marker state; the warm adapter's uncertain state is not reused.
    const rebuilt = budgetSetup({ ...callbacks, checkpoint: durableCheckpoint! });
    await rebuilt.start({ guardrails, guardrailState: { ...durable } });
    expect(rebuilt.handle.mock.calls[0]![0]).toMatchObject({ checkpoint: durableCheckpoint });
    expect(rebuilt.harness.hasPendingGuardrailUsage()).toBe(false);
    await rebuilt.harness.submit(next);
    expect(submitCount(rebuilt)).toBe(1);
    expect(onUsage).toHaveBeenCalledTimes(2);
    expect(onCheckpoint).toHaveBeenCalledTimes(2);
    expect(durable).toEqual({ total_tokens: 34 });
    expect(rebuilt.harness.hasPendingGuardrailUsage()).toBe(false);
    expect(rebuilt.events.filter((event) => event.kind === 'session.error')).toEqual([]);
  });

  it('keeps controls available after guarded interruption without terminal usage', async () => {
    const h = budgetSetup();
    h.setRun(async (emit) => {
      await vi.waitFor(() =>
        expect(h.handle.mock.calls.some(([c]) => c.type === 'interrupt')).toBe(true),
      );
      emit({ type: 'checkpoint', checkpoint });
    });
    await h.start({ guardrails: [tokenGuard] });
    const turn = h.harness.submit(message);
    await vi.waitFor(() => expect(submitCount(h)).toBe(1));
    await h.harness.submit({ id: 'evt_control_32260', kind: 'user.interrupt', payload: {} });
    await turn;
    expect(h.onUsage).not.toHaveBeenCalled();
    expect(h.durable).toHaveProperty(pendingKey);
    await expect(
      h.harness.submit({ id: 'evt_control_32448', kind: 'user.interrupt', payload: {} }),
    ).resolves.toBe('submitted');
    await expect(h.harness.submit({ ...message, id: 'evt_next' })).rejects.toThrow();
  });

  it('preserves unguarded interrupt and resume without a pending usage marker', async () => {
    const h = setup();
    h.setRun(async (emit) => {
      await vi.waitFor(() =>
        expect(h.handle.mock.calls.some(([c]) => c.type === 'interrupt')).toBe(true),
      );
      emit({ type: 'checkpoint', checkpoint });
    });
    await h.start();
    const turn = h.harness.submit(message);
    await vi.waitFor(() => expect(submitCount(h)).toBe(1));
    await h.harness.submit({ id: 'evt_control_33104', kind: 'user.interrupt', payload: {} });
    await turn;
    h.setRun(async (emit) => {
      emit({
        type: 'event',
        event: {
          type: 'turn.completed',
          usage: {
            input_tokens: 1,
            cached_input_tokens: 0,
            output_tokens: 1,
            cache_write_input_tokens: 0,
            reasoning_output_tokens: 0,
          },
        },
      });
      emit({ type: 'checkpoint', checkpoint });
    });
    await h.harness.submit({ ...message, id: 'evt_resumed' });
    expect(submitCount(h)).toBe(2);
    expect(h.harness.hasPendingGuardrailUsage()).toBe(false);
    expect(h.events.filter((event) => event.kind === 'session.error')).toHaveLength(1);
  });

  it('retains the durable marker when a guarded turn times out without terminal usage', async () => {
    const h = budgetSetup({ turnTimeoutMs: 5 });
    h.setRun(async (emit) => {
      await vi.waitFor(() =>
        expect(h.handle.mock.calls.some(([c]) => c.type === 'interrupt')).toBe(true),
      );
      emit({ type: 'checkpoint', checkpoint });
    });
    await h.start({ guardrails: [tokenGuard] });
    await h.harness.submit(message);
    expect(h.durable).toHaveProperty(pendingKey);
    expect(h.onUsage).not.toHaveBeenCalled();
    await expect(h.harness.submit({ ...message, id: 'evt_after_timeout' })).rejects.toBeInstanceOf(
      GuardrailUsageUnavailableError,
    );
  });

  it('an interrupt during preflight prevents SDK execution without waiting for the accounting lane', async () => {
    const blocked = deferred<void>();
    const writer = vi.fn(() => blocked.promise);
    const h = budgetSetup({ onGuardrailState: writer });
    await h.start({ guardrails: [tokenGuard] });
    const turn = h.harness.submit(message);
    await vi.waitFor(() => expect(writer).toHaveBeenCalledTimes(1));
    await h.harness.submit({ id: 'evt_control_34961', kind: 'user.interrupt', payload: {} });
    blocked.resolve();
    await turn;
    expect(submitCount(h)).toBe(0);
    expect(h.harness.hasPendingGuardrailUsage()).toBe(true);
  });

  it('refreshes daily unpriced facts using the accepted event identity and never user payload', async () => {
    const refresh = vi.fn(async () => ({ daily_cost_usd: 0, daily_cost_unpriced: true }));
    const h = budgetSetup({ refreshGuardrailSubjectWindow: refresh });
    await h.start({
      guardrails: [
        {
          ...tokenGuard,
          tier: 'workspace',
          stateScope: 'session',
          rule: {
            kind: 'builtin',
            builtin: 'user_daily_cost_budget',
            params: { max_cost_usd: 1 },
          },
        },
      ],
    });
    await expect(
      h.harness.submit({
        ...message,
        payload: { ...message.payload, turn_event_id: 'evt_forged', daily_cost_unpriced: false },
      }),
    ).rejects.toThrow('no price data');
    expect(refresh).toHaveBeenCalledWith(message.id);
    expect(submitCount(h)).toBe(0);
  });

  it('keeps acknowledged daily unpriced usage in the subject window', async () => {
    const h = budgetSetup({
      refreshGuardrailSubjectWindow: async () => ({}),
      onUsage: async () => ({ daily_cost_usd: 0, daily_cost_unpriced: true }),
    });
    await h.start({
      guardrails: [
        {
          ...tokenGuard,
          tier: 'workspace',
          rule: {
            kind: 'builtin',
            builtin: 'user_daily_cost_budget',
            params: { max_cost_usd: 1, on_unpriced: 'deny' },
          },
        },
      ],
    });
    // Read both keys from their declared scope, without going through a new refresh.
    h.harness.applyGuardrailUsageState({ daily_cost_usd: 0, daily_cost_unpriced: true });
    const store = (
      h.harness as unknown as {
        guardrailStore: import('@orca/guardrails').InMemoryGuardrailStateStore;
      }
    ).guardrailStore;
    expect(store.read('subject_window')).toEqual({ daily_cost_usd: 0, daily_cost_unpriced: true });
    expect(store.read('session')).not.toHaveProperty('daily_cost_unpriced');
  });

  it.each(['missing usage', 'missing checkpoint', 'turn.failed'])(
    'retains the marker for %s',
    async (failure) => {
      const h = budgetSetup();
      h.setRun(async (emit) => {
        if (failure === 'missing usage') emit({ type: 'checkpoint', checkpoint });
        else if (failure === 'missing checkpoint')
          emit({
            type: 'event',
            event: {
              type: 'turn.completed',
              usage: {
                input_tokens: 1,
                cached_input_tokens: 0,
                output_tokens: 1,
                cache_write_input_tokens: 0,
                reasoning_output_tokens: 0,
              },
            },
          });
        else
          emit({
            type: 'event',
            event: { type: 'turn.failed', error: { message: 'provider failed' } },
          });
      });
      await h.start({ guardrails: [tokenGuard] });
      await h.harness.submit(message);
      expect(h.durable).toHaveProperty(pendingKey);
      await expect(h.harness.submit({ ...message, id: 'evt_next' })).rejects.toThrow();
    },
  );

  it('rejects unsupported soft thresholds instead of claiming approval support', async () => {
    const h = budgetSetup();
    await expect(
      h.start({
        guardrails: [
          {
            ...tokenGuard,
            rule: {
              kind: 'builtin',
              builtin: 'token_budget',
              params: { max_total_tokens: 10, ask_thresholds: [1] },
            },
          },
        ],
      }),
    ).rejects.toThrow('soft approval thresholds');
  });
});
