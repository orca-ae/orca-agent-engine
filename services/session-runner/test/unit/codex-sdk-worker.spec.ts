// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { customResultCases } from '../../../../packages/codex-harness/test/support/custom-tool-results.js';
import { CustomToolResultSchema } from '../../src/custom-tools.js';
import { PendingCustomToolResults } from '../../src/pending-custom-tool-results.js';
import { createResponsesFixture } from '../../../../packages/codex-harness/test/support/responses-fixture.js';
import { afterEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { InMemorySandboxRuntime } from '@orca/sandbox-runtime';
import { CodexSdkHarness } from '../../src/harness/codex-sdk/index.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';
import { CodexSdkWorker, captureCheckpoint, restoreCheckpoint } from '@orca/codex-harness';
import type { WorkerEvent } from '@orca/codex-harness';

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});

async function gateway(callTool = false, apiKey = 'test-gateway-token') {
  const fixture = await createResponsesFixture({ callTool, apiKey });
  cleanup.push(fixture.close);
  return fixture;
}

describe('Codex SDK worker using the installed SDK and bundled executable', () => {
  it('refreshes gateway credentials while preserving pinned native instructions and history', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-codex-refresh-'));
    cleanup.push(() => rm(root, { recursive: true, force: true }));
    const original = await gateway();
    const refreshed = await gateway(false, 'new-gateway-token');
    const events: WorkerEvent[] = [];
    const worker = new CodexSdkWorker((event) => events.push(event));
    cleanup.push(() => worker.close());
    await worker.handle({
      type: 'start',
      root,
      sessionId: 'ses_sdk',
      model: 'gpt-5.4',
      system: 'Original developer instructions',
      apiKey: original.apiKey,
      baseUrl: original.url,
      tools: [],
    });
    await worker.handle({ type: 'submit', text: 'Remember the first native turn' });
    expect(JSON.stringify(original.requests[0])).toContain('Original developer instructions');
    expect(() =>
      worker.refreshOptions({ apiKey: refreshed.apiKey, system: 'Changed instructions' }),
    ).toThrow('cannot change developer instructions');
    worker.refreshOptions({
      apiKey: refreshed.apiKey,
      baseUrl: refreshed.url,
      system: 'Original developer instructions',
    });
    await worker.handle({ type: 'submit', text: 'Continue with refreshed credentials' });
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    expect(original.requests).toHaveLength(1);
    expect(refreshed.requests).toHaveLength(1);
    expect(JSON.stringify(refreshed.requests[0])).toContain('Remember the first native turn');
    expect(JSON.stringify(refreshed.requests[0])).toContain('Original developer instructions');
    const checkpoints = events.filter((event) => event.type === 'checkpoint');
    expect(checkpoints).toHaveLength(2);
    expect(checkpoints[0]!.checkpoint.threadId).toBe(checkpoints[1]!.checkpoint.threadId);
  }, 30000);

  it.each([false, true])(
    'runs and resumes native history with unchanged catalog (hasSkill=%s)',
    async (hasSkill) => {
      const root = await mkdtemp(join(tmpdir(), 'orca-codex-sdk-'));
      cleanup.push(() => rm(root, { recursive: true, force: true }));
      const api = await gateway();
      const events: WorkerEvent[] = [];
      const worker = new CodexSdkWorker((event) => events.push(event));
      cleanup.push(() => worker.close());
      const start = {
        type: 'start' as const,
        root,
        sessionId: 'ses_sdk',
        model: 'gpt-5.4',
        system:
          'You are concise.' +
          (hasSkill
            ? '\n<available_skills>report: /workspace/skills/report/SKILL.md</available_skills>'
            : ''),
        apiKey: api.apiKey,
        baseUrl: api.url,
        tools: [],
      };
      await worker.handle(start);
      await worker.handle({ type: 'submit', text: 'First turn' });
      expect(events.filter((event) => event.type === 'failure')).toEqual([]);
      expect(events).toContainEqual(
        expect.objectContaining({
          type: 'event',
          event: expect.objectContaining({ type: 'turn.completed' }),
        }),
      );
      expect(api.requests.length).toBeGreaterThan(0);
      const tools = api.requests.flatMap(
        (request) => request.tools as Array<{ name?: string; type?: string }>,
      );
      expect(
        tools.some((tool) =>
          ['exec_command', 'shell', 'apply_patch', 'view_image', 'spawn_agent'].includes(
            tool.name ?? '',
          ),
        ),
      ).toBe(false);
      const state = events.find((event) => event.type === 'checkpoint');
      expect(state?.type).toBe('checkpoint');
      if (state?.type !== 'checkpoint') throw new Error('no native checkpoint');
      expect(state.checkpoint.instructionsSha256).toMatch(/^[a-f0-9]{64}$/);
      await worker.close();
      const { instructionsSha256: _fingerprint, ...legacy } = state.checkpoint;
      const changedSystem = hasSkill
        ? 'You are concise.'
        : start.system +
          '\n<available_skills>report: /workspace/skills/report/SKILL.md</available_skills>';
      for (const checkpoint of [state.checkpoint, legacy]) {
        const incompatible = new CodexSdkWorker((event) => events.push(event));
        cleanup.push(() => incompatible.close());
        await expect(
          incompatible.handle({ ...start, system: changedSystem, checkpoint }),
        ).rejects.toThrow('checkpoint developer instructions differ');
        await incompatible.close();
      }
      expect(api.requests).toHaveLength(1);
      const replacement = new CodexSdkWorker((event) => events.push(event));
      cleanup.push(() => replacement.close());
      const replacementRoot = await mkdtemp(join(tmpdir(), 'orca-codex-replacement-'));
      cleanup.push(() => rm(replacementRoot, { recursive: true, force: true }));
      for (const data of Object.values(state.checkpoint.files)) {
        expect(Buffer.from(data, 'base64').toString()).not.toContain(api.apiKey);
      }
      await replacement.handle({ ...start, root: replacementRoot, checkpoint: state.checkpoint });
      await replacement.handle({ type: 'submit', text: 'Second turn' });
      expect(events.filter((event) => event.type === 'failure')).toEqual([]);
      expect(JSON.stringify(api.requests.at(-1)?.input)).toContain('First turn');
      // A legacy plain-chat or Skill checkpoint with verifiable initial instructions
      // remains resumable. Its next checkpoint upgrades to the explicit fingerprint.
      const legacyWorker = new CodexSdkWorker((event) => events.push(event));
      cleanup.push(() => legacyWorker.close());
      await legacyWorker.handle({ ...start, root: replacementRoot, checkpoint: legacy });
      await legacyWorker.handle({ type: 'submit', text: 'Legacy resume' });
      expect(events.filter((event) => event.type === 'failure')).toEqual([]);
      expect(JSON.stringify(api.requests.at(-1)?.input)).toContain('First turn');
    },
    30000,
  );

  it('routes a real native SDK tool call through the Orca MCP relay', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-codex-tool-'));
    cleanup.push(() => rm(root, { recursive: true, force: true }));
    const api = await gateway(true);
    const events: WorkerEvent[] = [];
    const worker = new CodexSdkWorker((event) => {
      events.push(event);
      if (event.type === 'tool_call')
        void worker.handle({
          type: 'tool_result',
          id: event.id,
          result: { content: [{ type: 'text', text: 'approved write complete' }] },
        });
    });
    cleanup.push(() => worker.close());
    await worker.handle({
      type: 'start',
      root,
      sessionId: 'ses_sdk',
      model: 'gpt-5.4',
      system: 'Use the tool.',
      apiKey: api.apiKey,
      baseUrl: api.url,
      tools: [
        {
          name: 'write',
          description: 'Write a file',
          inputSchema: {
            type: 'object',
            properties: { path: { type: 'string' }, content: { type: 'string' } },
            required: ['path', 'content'],
          },
        },
      ],
    });
    await worker.handle({ type: 'submit', text: 'Write proof.txt' });
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'tool_call',
        name: 'write',
        arguments: { path: 'proof.txt', content: 'proof' },
      }),
    );
    expect(JSON.stringify(api.requests.at(-1)?.input)).toContain('approved write complete');
  }, 30000);

  it('launches the built SDK worker and writes only through the parent approval gate', async () => {
    const api = await gateway(true);
    const sandbox = await new InMemorySandboxRuntime().acquire({});
    cleanup.push(() => sandbox.destroy());
    const harness = new CodexSdkHarness({
      apiKey: api.apiKey,
      baseUrl: api.url,
      workerArgs: [
        fileURLToPath(new URL('../../dist/harness/codex-sdk/worker-entry.js', import.meta.url)),
      ],
      timeoutMs: 20000,
    });
    const events: AgentEvent[] = [];
    const drained = (async () => {
      for await (const event of harness.events()) events.push(event);
    })();
    cleanup.push(async () => {
      await harness.stop('client.archived');
      await drained;
    });
    let approvals = 0;
    await harness.start({
      workspaceId: 'ws_test',
      sessionId: 'ses_sdk',
      sandbox,
      agentSnapshot: {
        model_provider: 'openai',
        model_id: 'gpt-5.4',
        allowed_tool_names: ['write'],
      },
      confirmTool: async (_name, input) => {
        await expect(sandbox.files.read('proof.txt')).rejects.toThrow();
        approvals++;
        return { behavior: 'allow', updatedInput: input };
      },
    });
    await harness.submit({
      kind: 'user.message',
      payload: { content: [{ type: 'text', text: 'Write proof.txt' }] },
    });
    await harness.stop('client.archived');
    await drained;
    expect(approvals).toBe(1);
    expect((await sandbox.files.read('proof.txt')).toString()).toBe('proof');
    expect(events.filter((event) => event.kind === 'agent.error')).toEqual([]);
    expect(events.some((event) => event.kind === 'orca.harness_checkpoint')).toBe(true);
    expect(events.filter((event) => event.kind === 'agent.turn_completed')).toHaveLength(1);
  }, 30000);

  it('rejects path traversal and missing native history', async () => {
    const root = await mkdtemp(join(tmpdir(), 'orca-checkpoint-'));
    cleanup.push(() => rm(root, { recursive: true, force: true }));
    await expect(
      restoreCheckpoint(
        root,
        {
          version: 1,
          threadId: 'thread-id',
          files: { '../auth.json': 'e30=' },
        },
        '',
      ),
    ).rejects.toThrow('path');
    await expect(captureCheckpoint(root, 'thread-id', '')).rejects.toThrow();
  });
});

it.each([
  ...customResultCases.map((item) => ({ ...item, unsupported: false })),
  {
    name: 'unsupported URL image',
    content: [{ type: 'image', source: { type: 'url', url: 'https://example.test/image.png' } }],
    proof: "image source 'url' is not supported",
    unsupported: true,
  },
])(
  'handles a $name callback with the installed SDK without agent_toolset approval',
  async ({ content, proof, unsupported }) => {
    const api = await createResponsesFixture({
      callTool: true,
      toolName: 'lookup_ticket',
      toolArguments: { id: 'T1' },
    });
    cleanup.push(api.close);
    const sandbox = await new InMemorySandboxRuntime().acquire({});
    cleanup.push(() => sandbox.destroy());
    const pending = new PendingCustomToolResults();
    const harness = new CodexSdkHarness({
      apiKey: api.apiKey,
      baseUrl: api.url,
      workerArgs: [
        fileURLToPath(new URL('../../dist/harness/codex-sdk/worker-entry.js', import.meta.url)),
      ],
      timeoutMs: 20000,
    });
    const events: AgentEvent[] = [];
    const drain = (async () => {
      for await (const event of harness.events()) {
        events.push(event);
        if (event.kind === 'agent.custom_tool_use')
          pending.resolve(
            CustomToolResultSchema.parse({
              type: 'user.custom_tool_result',
              custom_tool_use_id: event.id!,
              content,
            }),
          );
      }
    })();
    cleanup.push(async () => {
      await harness.stop('client.archived');
      await drain;
    });
    let approvals = 0;
    await harness.start({
      workspaceId: 'ws',
      sessionId: 'ses_sdk',
      sandbox,
      agentSnapshot: {
        model_provider: 'openai',
        model_id: 'gpt-5.4',
        allowed_tool_names: ['lookup_ticket'],
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
      },
      toolPermissions: { policyFor: () => 'always_ask' },
      confirmTool: async () => {
        approvals++;
        return { behavior: 'deny', message: 'not a built-in tool' };
      },
      awaitCustomToolResult: (id, signal) => pending.park(id, signal),
    });
    await harness.submit({ kind: 'user.message', payload: { content: 'Find ticket T1' } });
    await harness.stop('client.archived');
    await drain;
    expect(approvals).toBe(0);
    if (unsupported) {
      expect(events.find((event) => event.kind === 'agent.error')?.payload).toMatchObject({
        message: expect.stringContaining(proof),
      });
      expect(events.filter((event) => event.kind === 'agent.turn_completed')).toHaveLength(1);
      expect(api.requests).toHaveLength(1);
      return;
    }
    expect(events.filter((e) => e.kind === 'agent.error')).toEqual([]);
    expect(events.filter((e) => e.kind === 'agent.custom_tool_use')).toHaveLength(1);
    expect(
      events.filter((e) =>
        ['agent.tool_use', 'agent.tool_result', 'agent.requires_action'].includes(e.kind),
      ),
    ).toEqual([]);
    expect(api.requests).toHaveLength(2);
    const continuation = JSON.stringify(api.requests.at(-1)?.input);
    expect(continuation).toContain(proof);
    expect(continuation).not.toContain('Invalid tools/call result');
    expect(continuation).not.toContain('-32602');
    expect(events.filter((e) => e.kind === 'agent.turn_completed')).toHaveLength(1);
  },
  30000,
);
