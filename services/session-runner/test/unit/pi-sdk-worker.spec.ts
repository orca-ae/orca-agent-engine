// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createNativeProxyFixture } from '../../../../packages/pi-harness/test/support/native-proxy-fixture.js';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { fileURLToPath } from 'node:url';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { CodexSdkHarness } from '../../src/harness/codex-sdk/index.js';
import type { AgentEvent, ToolConfirmer } from '../../src/harness/agent-harness.js';
import type { SdkCheckpoint } from '@orca/sdk-harness';
import {
  MODELS,
  type TestProvider,
} from '../../../../packages/pi-harness/test/support/provider-fixture.js';

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
async function start(
  options: {
    callTool?: boolean;
    checkpoint?: SdkCheckpoint;
    confirm?: ToolConfirmer;
    provider?: TestProvider;
  } = {},
) {
  const api = await createNativeProxyFixture(options.provider ?? 'openai', {
    callTool: options.callTool ?? true,
  });
  cleanup.push(api.close);
  const sandbox = await new InMemorySandboxRuntime().acquire({});
  cleanup.push(() => sandbox.destroy());
  const harness = new CodexSdkHarness({
    provider: 'pi-sdk',
    apiKey: api.apiKey,
    piGatewayUrl: api.url,
    workerArgs: [
      fileURLToPath(new URL('../../dist/harness/pi-sdk/worker-entry.js', import.meta.url)),
    ],
    timeoutMs: 20000,
    ...(options.checkpoint ? { checkpoint: options.checkpoint } : {}),
  });
  const events: AgentEvent[] = [];
  const drained = (async () => {
    for await (const event of harness.events()) events.push(event);
  })();
  cleanup.push(async () => {
    await harness.stop('client.archived');
    await drained;
  });
  await harness.start({
    workspaceId: 'ws',
    sessionId: 'ses_sdk',
    sandbox,
    agentSnapshot: {
      model_provider: options.provider ?? 'openai',
      model_id: MODELS[options.provider ?? 'openai'],
      allowed_tool_names: ['write'],
      system: 'Use managed tools.',
    },
    toolPermissions: { policyFor: () => 'always_ask' },
    confirmTool:
      options.confirm ?? (async (_name, input) => ({ behavior: 'allow', updatedInput: input })),
  });
  return { api, sandbox, harness, events };
}
const message = { kind: 'user.message', payload: { content: 'Write proof.txt' } };
describe.each(['openai', 'anthropic', 'deepseek', 'google', 'zai'] as const)(
  'self-hosted Pi SDK %s subprocess',
  (provider) => {
    const startProvider = (options: Parameters<typeof start>[0] = {}) =>
      start({ ...options, provider });
    it('gates tool writes, emits usage and a private checkpoint, then resumes in a new process', async () => {
      const first = await startProvider();
      await first.harness.submit(message);
      expect((await first.sandbox.files.read('proof.txt')).toString()).toBe('proof');
      expect(first.events.filter((event) => event.kind === 'agent.error')).toEqual([]);
      expect(first.events.find((event) => event.kind === 'agent.usage')?.payload).toMatchObject({
        usage: {
          input_tokens: 23,
          output_tokens: 10,
          cache_read_input_tokens: 7,
          cache_creation: {
            ephemeral_5m_input_tokens: provider === 'anthropic' ? 2 : 0,
            ephemeral_1h_input_tokens: provider === 'anthropic' ? 1 : 0,
          },
        },
      });
      const checkpoint = first.events.find((event) => event.kind === 'orca.harness_checkpoint')
        ?.payload as { provider: string; state: SdkCheckpoint };
      expect(checkpoint.provider).toBe('pi-sdk');
      expect(checkpoint.state.format).toBe('pi_sdk');
      await first.harness.stop('client.archived');
      const second = await startProvider({ callTool: false, checkpoint: checkpoint.state });
      await second.harness.submit({
        ...message,
        payload: { content: 'Recall the previous write' },
      });
      expect(second.events.filter((event) => event.kind === 'agent.error')).toEqual([]);
      expect(JSON.stringify(second.api.requests[0])).toContain('Write proof.txt');
    }, 30000);
    it('continues in the same subprocess after an interrupted approval', async () => {
      const h = await startProvider({ confirm: async () => new Promise(() => {}) });
      const turn = h.harness.submit(message);
      await vi.waitFor(() =>
        expect(h.events.some((e) => e.kind === 'agent.requires_action')).toBe(true),
      );
      h.harness.interrupt();
      await turn;
      const boundary = h.events.length;
      await h.harness.submit({ ...message, payload: { content: 'Continue after cancellation' } });
      expect(h.events.slice(boundary).filter((e) => e.kind === 'agent.error')).toEqual([]);
      expect(h.events.slice(boundary).some((e) => e.kind === 'orca.harness_checkpoint')).toBe(true);
    }, 30000);

    it('denies a native tool without writing to the sandbox', async () => {
      const h = await startProvider({
        confirm: async () => ({ behavior: 'deny', message: 'denied' }),
      });
      await h.harness.submit(message);
      await expect(h.sandbox.files.read('proof.txt')).rejects.toThrow();
      expect(JSON.stringify(h.api.requests[1])).toContain('denied');
      expect(h.events.filter((event) => event.kind === 'agent.error')).toEqual([]);
    }, 30000);
  },
);
