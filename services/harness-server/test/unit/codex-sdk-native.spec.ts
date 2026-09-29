// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createNativeProxyFixture } from '../../../../packages/pi-harness/test/support/native-proxy-fixture.js';
import { PiSdkWorker } from '@orca/pi-harness';
import { createApp, createState } from '../../../sandbox-harness/src/server.js';
import { RemoteCodexSdkWorker } from '../../src/harness/codex-sdk/remote-worker.js';
import { DialInTransport } from '../../src/harness/in-sandbox/dial-in.js';
import { fileURLToPath } from 'node:url';
import type { AddressInfo } from 'node:net';
import { turnStoreFixture } from '../support/codex-turn-store.js';
import { customResultCases } from '../../../../packages/codex-harness/test/support/custom-tool-results.js';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { stat } from 'node:fs/promises';
import { join } from 'node:path';
import { CodexSdkWorker, type CodexCheckpoint, type StartCommand } from '@orca/codex-harness';
import {
  createProviderFixture,
  MODELS,
  type TestProvider,
} from '../../../../packages/pi-harness/test/support/provider-fixture.js';
import { CodexSdkHarness, type CodexSdkHarnessOptions } from '../../src/harness/codex-sdk/index.js';
import type { AgentEvent, SessionStartInput } from '../../src/harness/agent-harness.js';
import { buildAgentToolset } from '../../src/sandbox/agent-toolset.js';
import type { SandboxHandle } from '../../src/sandbox/sandbox-runtime.js';

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
function idle(events: AgentEvent[], type: string) {
  return events.find(
    (event) =>
      event.kind === 'session.status_idle' &&
      (event.payload as { stop_reason: { type: string } }).stop_reason.type === type,
  );
}
async function startHarness(
  topology: 'separate' | 'colocated',
  sdk: 'codex_sdk' | 'pi_sdk',
  options: {
    callTool?: boolean;
    provider?: TestProvider;
    customCallback?: boolean;
    guardrails?: SessionStartInput['guardrails'];
    guardrailState?: SessionStartInput['guardrailState'];
    onGuardrailState?: CodexSdkHarnessOptions['onGuardrailState'];
    onUsage?: CodexSdkHarnessOptions['onUsage'];
    checkpoint?: CodexCheckpoint;
    onCheckpoint?: (state: CodexCheckpoint) => Promise<void>;
  } = {},
) {
  const api = await (sdk === 'pi_sdk' ? createNativeProxyFixture : createProviderFixture)(
    options.provider ?? 'openai',
    {
      callTool: options.callTool ?? true,
      ...(options.customCallback ? { toolName: 'lookup_ticket', toolArguments: { id: 'T1' } } : {}),
    },
  );
  cleanup.push(api.close);
  // This models a cloud sandbox's Files API. There is deliberately no local
  // workspace root or spawn() method for the SDK to use as an execution path.
  const remoteFiles = new Map<string, Buffer>();
  const write = vi.fn(async (path: string, content: Buffer) => {
    remoteFiles.set(path, content);
  });
  const sandbox = { id: 'remote-sandbox', files: { write } } as unknown as SandboxHandle;
  let transport: DialInTransport | undefined;
  if (topology === 'colocated') {
    const state = createState({
      subprocessEntryPath: fileURLToPath(
        new URL('../../../sandbox-harness/dist/subprocess-entry.js', import.meta.url),
      ),
      env: { ...process.env, ORCA_SANDBOX_WRITE_POLICY: undefined },
    });
    const server = createApp(state);
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    transport = new DialInTransport({
      baseUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    });
    cleanup.push(async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    });
  }
  const checkpoints: CodexCheckpoint[] = [];
  const starts: StartCommand[] = [];
  const harness = new CodexSdkHarness({
    harness: sdk,
    apiKey: api.apiKey,
    turnTimeoutMs: 20000,
    onGuardrailState: options.onGuardrailState,
    onUsage: options.onUsage,
    ...(sdk === 'pi_sdk' ? { piGatewayUrl: api.url } : { baseUrl: api.url }),
    ...(options.checkpoint ? { checkpoint: options.checkpoint } : {}),
    turns: turnStoreFixture({
      ...(options.checkpoint ? { checkpoint: options.checkpoint } : {}),
      onCommit: async (state, receipt) => {
        await options.onCheckpoint?.(state);
        checkpoints.push(state);
        if (receipt.guarded && !receipt.error)
          await options.onGuardrailState?.([
            {
              scope: 'session',
              key: `codex_sdk_usage_pending:${receipt.turnId}`,
              action: 'delete',
            },
          ]);
      },
    }).store,
    createWorker: (emit, input) => {
      const worker = transport
        ? new RemoteCodexSdkWorker(
            emit,
            input,
            transport,
            sdk === 'pi_sdk' ? 'pi-sdk' : 'codex-sdk',
          )
        : sdk === 'pi_sdk'
          ? new PiSdkWorker(emit)
          : new CodexSdkWorker(emit);
      return {
        handle: async (command) => {
          if (command.type === 'start') starts.push(command);
          await worker.handle(command);
        },
        refreshOptions: (input) => worker.refreshOptions(input),
        close: () => worker.close(),
      };
    },
  });
  const events: AgentEvent[] = [];
  const pump = (async () => {
    for await (const event of harness.events()) {
      events.push(event);
      event.persistence?.resolve();
    }
  })();
  cleanup.push(async () => {
    await harness.stop('client.archived');
    await pump;
  });
  const input: SessionStartInput = {
    workspaceId: 'ws_test',
    sessionId: 'ses_sdk',
    sandbox,
    agentSnapshot: {
      model_provider: options.provider ?? 'openai',
      model_id: MODELS[options.provider ?? 'openai'],
      system: 'Honor the server-pinned developer policy.',
      allowed_tool_names: ['bash', 'read', 'write', 'edit', 'glob', 'grep', 'list', 'delete'],
      tool_permission_policies: { mcp__orca__write: 'always_ask' },
      ...(options.customCallback
        ? {
            custom_tools: [
              {
                name: 'lookup_ticket',
                description: 'Lookup a ticket',
                input_schema: {
                  type: 'object',
                  properties: { id: { type: 'string' } },
                  required: ['id'],
                },
              },
            ],
          }
        : {}),
    },
    tools: buildAgentToolset(sandbox),
    ...(options.guardrails ? { guardrails: options.guardrails } : {}),
    ...(options.guardrailState ? { guardrailState: options.guardrailState } : {}),
  };
  await harness.start(input);
  return { api, harness, events, checkpoints, starts, write, remoteFiles };
}
const userMessage = {
  id: 'evt_first_native_turn',
  kind: 'user.message',
  payload: { content: [{ type: 'text', text: 'Write proof.txt in the remote sandbox' }] },
};

describe.each([
  ['separate', 'codex_sdk', 'openai'],
  ['colocated', 'codex_sdk', 'openai'],
  ['separate', 'pi_sdk', 'openai'],
  ['separate', 'pi_sdk', 'anthropic'],
  ['separate', 'pi_sdk', 'deepseek'],
  ['separate', 'pi_sdk', 'google'],
  ['separate', 'pi_sdk', 'zai'],
  ['colocated', 'pi_sdk', 'openai'],
  ['colocated', 'pi_sdk', 'anthropic'],
  ['colocated', 'pi_sdk', 'deepseek'],
  ['colocated', 'pi_sdk', 'google'],
  ['colocated', 'pi_sdk', 'zai'],
] as const)('%s: %s %s native SDK', (topology, sdk, provider) => {
  const start = (options: Parameters<typeof startHarness>[2] = {}) =>
    startHarness(topology, sdk, { ...options, provider });
  it.each(customResultCases)(
    'continues a native client callback with $name',
    async ({ content, proof }) => {
      const { harness, events, api } = await start({ customCallback: true });
      await harness.submit({ ...userMessage, payload: { content: 'Look up ticket T1' } });
      const use = events.find((event) => event.kind === 'agent.custom_tool_use')!;
      expect(use).toBeDefined();
      await harness.submit({
        id: 'evt_control_5492',
        kind: 'user.custom_tool_result',
        payload: { custom_tool_use_id: use.id, content },
      });
      expect(events.filter((event) => event.kind === 'session.error')).toEqual([]);
      expect(
        events.filter((event) => ['agent.tool_use', 'agent.tool_result'].includes(event.kind)),
      ).toEqual([]);
      expect(idle(events, 'end_turn')).toBeDefined();
      expect(api.requests).toHaveLength(2);
      const continuation = JSON.stringify(api.requests.at(-1));
      if (provider === 'anthropic' && proof.startsWith('data:image/')) {
        expect(continuation).toContain(proof.split(',')[1]);
        expect(continuation).toContain('"media_type":"image/png"');
      } else if (provider === 'google' && proof.startsWith('data:image/')) {
        expect(continuation).toContain(proof.split(',')[1]);
        expect(continuation).toContain('image/png');
      } else expect(continuation).toContain(proof);
      expect(continuation).not.toContain('Invalid tools/call result');
      expect(continuation).not.toContain('-32602');
    },
    30000,
  );

  it('terminates a native callback with a clear error for unsupported document sources', async () => {
    const { harness, events, api } = await start({ customCallback: true });
    await harness.submit({ ...userMessage, payload: { content: 'Look up ticket T1' } });
    const use = events.find((event) => event.kind === 'agent.custom_tool_use')!;
    await harness.submit({
      id: 'evt_control_6551',
      kind: 'user.custom_tool_result',
      payload: {
        custom_tool_use_id: use.id,
        content: [{ type: 'document', source: { type: 'file', file_id: 'file_123' } }],
      },
    });
    expect(events.find((event) => event.kind === 'session.error')?.payload).toMatchObject({
      error: { message: expect.stringContaining("document source 'file' is not supported") },
    });
    expect(idle(events, 'retries_exhausted')).toBeDefined();
    expect(idle(events, 'end_turn')).toBeUndefined();
    expect(api.requests).toHaveLength(1);
  }, 30000);

  it('mediates a remote sandbox write, saves native history before idle, and resumes in a new host worker', async () => {
    let releaseCheckpoint!: () => void;
    const durable = new Promise<void>((resolve) => {
      releaseCheckpoint = resolve;
    });
    let sawCheckpoint = false;
    const first = await start({
      onCheckpoint: async () => {
        sawCheckpoint = true;
        await durable;
      },
    });
    cleanup.push(async () => releaseCheckpoint());
    await first.harness.submit(userMessage);
    const use = first.events.find((event) => event.kind === 'agent.tool_use')!;
    expect(use).toBeDefined();
    expect(idle(first.events, 'requires_action')?.payload).toEqual({
      stop_reason: { type: 'requires_action', event_ids: [use.id] },
    });
    expect(first.write).not.toHaveBeenCalled();
    const approval = first.harness.submit({
      id: 'evt_control_7983',
      kind: 'user.tool_confirmation',
      payload: { tool_use_id: use.id, result: 'allow' },
    });
    await vi.waitFor(() => expect(sawCheckpoint).toBe(true), { timeout: 10000 });
    expect(first.write).toHaveBeenCalledOnce();
    expect(first.remoteFiles.get('proof.txt')?.toString()).toBe('proof');
    const hostRoot = first.starts[0]!.root;
    await expect(stat(join(hostRoot, 'proof.txt'))).rejects.toMatchObject({ code: 'ENOENT' });
    expect(idle(first.events, 'end_turn')).toBeUndefined();
    releaseCheckpoint();
    await approval;
    expect(first.checkpoints).toHaveLength(1);
    expect(idle(first.events, 'end_turn')).toBeDefined();
    expect(first.events.filter((event) => event.kind === 'session.error')).toEqual([]);
    expect(first.events.find((event) => event.kind === 'agent.usage')?.payload).toMatchObject({
      model: MODELS[provider],
      turn_event_id: userMessage.id,
      usage: { input_tokens: 23, cache_read_input_tokens: 7, output_tokens: 10 },
    });
    const allTools = first.api.requests.flatMap(
      (request) => request.tools as Array<{ name?: string }>,
    );
    expect(
      allTools.some((tool) =>
        ['exec_command', 'shell', 'apply_patch', 'view_image', 'spawn_agent'].includes(
          tool.name ?? '',
        ),
      ),
    ).toBe(false);
    expect(JSON.stringify(first.api.requests[0])).toContain(
      'Honor the server-pinned developer policy.',
    );
    expect(JSON.stringify(first.events)).not.toContain('rollout-');
    await first.harness.stop('client.archived');
    await expect(stat(hostRoot)).rejects.toMatchObject({ code: 'ENOENT' });

    const second = await start({ callTool: false, checkpoint: first.checkpoints[0]! });
    expect(second.starts[0]!.root).not.toBe(hostRoot);
    await second.harness.submit({
      id: 'evt_second_native_turn',
      kind: 'user.message',
      payload: { content: 'Recall the prior remote write' },
    });
    expect(second.events.filter((event) => event.kind === 'session.error')).toEqual([]);
    expect(second.checkpoints[0]!.threadId).toBe(first.checkpoints[0]!.threadId);
    expect(JSON.stringify(second.api.requests[0])).toContain(
      'Write proof.txt in the remote sandbox',
    );
    expect(JSON.stringify(second.api.requests[0])).toContain('wrote proof.txt');
  }, 30000);

  it('returns a denied native tool call without reaching the remote sandbox', async () => {
    const { harness, events, write, api, checkpoints } = await start();
    await harness.submit(userMessage);
    const use = events.find((event) => event.kind === 'agent.tool_use')!;
    await harness.submit({
      id: 'evt_control_10611',
      kind: 'user.tool_confirmation',
      payload: {
        tool_use_id: use.id,
        result: 'deny',
        deny_message: 'Denied by managed approval',
      },
    });
    expect(write).not.toHaveBeenCalled();
    expect(events.find((event) => event.kind === 'agent.tool_result')?.payload).toMatchObject({
      tool_use_id: use.id,
      is_error: true,
    });
    expect(JSON.stringify(api.requests.at(-1))).toContain('Denied by managed approval');
    expect(checkpoints).toHaveLength(1);
    expect(idle(events, 'end_turn')).toBeDefined();
    expect(events.filter((event) => event.kind === 'session.error')).toEqual([]);
  }, 30000);

  it('interrupts a native worker waiting on approval without executing the tool', async () => {
    const { harness, events, write } = await start();
    await harness.submit(userMessage);
    expect(harness.hasPendingRequiredAction()).toBe(true);
    await harness.submit({ id: 'evt_control_11506', kind: 'user.interrupt', payload: {} });
    expect(harness.hasPendingRequiredAction()).toBe(false);
    expect(write).not.toHaveBeenCalled();
    expect(idle(events, 'retries_exhausted')).toBeDefined();
    expect(events.filter((event) => event.kind === 'session.error')).toHaveLength(1);
    const boundary = events.length;
    await harness.submit({
      ...userMessage,
      id: 'evt_after_interrupt',
      payload: { content: 'Continue after cancellation' },
    });
    expect(events.slice(boundary).filter((event) => event.kind === 'session.error')).toEqual([]);
    expect(idle(events.slice(boundary), 'end_turn')).toBeDefined();
  }, 30000);
});

describe.each([
  ['separate', 'codex_sdk', 'openai'],
  ['colocated', 'codex_sdk', 'openai'],
  ['separate', 'pi_sdk', 'openai'],
  ['separate', 'pi_sdk', 'anthropic'],
  ['separate', 'pi_sdk', 'deepseek'],
  ['separate', 'pi_sdk', 'google'],
  ['separate', 'pi_sdk', 'zai'],
  ['colocated', 'pi_sdk', 'openai'],
  ['colocated', 'pi_sdk', 'anthropic'],
  ['colocated', 'pi_sdk', 'deepseek'],
  ['colocated', 'pi_sdk', 'google'],
  ['colocated', 'pi_sdk', 'zai'],
] as const)('%s %s %s native request budgets', (topology, sdk, provider) => {
  const start = (options: Parameters<typeof startHarness>[2] = {}) =>
    startHarness(topology, sdk, { ...options, provider });
  const guardrails: SessionStartInput['guardrails'] = [
    {
      id: 'gr_budget',
      name: 'native budget',
      tier: 'session',
      phases: ['request'],
      stateful: true,
      rule: { kind: 'builtin', builtin: 'token_budget', params: { max_total_tokens: 1 } },
    },
  ];
  it('records native usage once and blocks the second turn before a provider request', async () => {
    const onUsage = vi.fn(async () => ({ total_tokens: 40 }));
    const onGuardrailState = vi.fn(async () => {});
    const h = await start({ callTool: false, guardrails, onUsage, onGuardrailState });
    await h.harness.submit(userMessage);
    expect(h.events.filter((e) => e.kind === 'session.error')).toEqual([]);
    expect(onUsage).toHaveBeenCalledOnce();
    expect(h.checkpoints).toHaveLength(1);
    expect(onGuardrailState).toHaveBeenCalledTimes(2);
    const count = h.api.requests.length;
    expect(count).toBeGreaterThan(0);
    await expect(h.harness.submit({ ...userMessage, id: 'evt_next_native_turn' })).rejects.toThrow(
      'budget',
    );
    expect(h.api.requests).toHaveLength(count);
  }, 30000);

  it('does not call the native provider with restored budget exhaustion', async () => {
    const h = await start({
      callTool: false,
      guardrails,
      guardrailState: { total_tokens: 1 },
      onUsage: async () => ({}),
      onGuardrailState: async () => {},
    });
    await expect(h.harness.submit(userMessage)).rejects.toThrow('budget');
    expect(h.api.requests).toHaveLength(0);
  }, 30000);
});
