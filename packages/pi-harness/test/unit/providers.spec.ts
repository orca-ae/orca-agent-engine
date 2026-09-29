// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PiSdkWorker, decodePiCheckpoint, capturePiCheckpoint } from '../../src/index.js';
import type { WorkerEvent, StartCommand } from '@orca/sdk-harness';
import { createProviderFixture, MODELS } from '../support/provider-fixture.js';
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
it.each(['anthropic', 'deepseek', 'google', 'zai'] as const)(
  'runs native %s tools, scoped auth, usage and cold history recovery',
  async (provider) => {
    const api = await createProviderFixture(provider, { callTool: true });
    cleanup.push(api.close);
    const root = await mkdtemp(join(tmpdir(), 'pi-provider-'));
    cleanup.push(() => rm(root, { recursive: true, force: true }));
    const events: WorkerEvent[] = [];
    const worker = new PiSdkWorker((event) => {
      events.push(event);
      if (event.type === 'tool_call')
        void worker.handle({
          type: 'tool_result',
          id: event.id,
          result: { content: [{ type: 'text', text: 'approved' }] },
        });
    });
    cleanup.push(() => worker.close());
    const start: StartCommand = {
      type: 'start',
      sessionId: 'ses_sdk',
      root,
      modelProvider: provider,
      model: MODELS[provider],
      system: 'Managed tools only',
      apiKey: api.apiKey,
      baseUrl: api.url,
      tools: [
        {
          name: 'write',
          description: 'write a file',
          inputSchema: {
            type: 'object',
            properties: { path: { type: 'string' }, content: { type: 'string' } },
            required: ['path', 'content'],
          },
        },
      ],
    };
    await worker.handle(start);
    await worker.handle({ type: 'submit', text: 'Remember my write' });
    expect(events.filter((e) => e.type === 'failure')).toEqual([]);
    expect(events).toContainEqual(expect.objectContaining({ type: 'tool_call', name: 'write' }));
    expect(api.requests).toHaveLength(2);
    expect(events).toContainEqual({
      type: 'event',
      event: {
        type: 'turn.completed',
        usage: {
          input_tokens: 30,
          output_tokens: 10,
          cached_input_tokens: 7,
          cache_write_input_tokens: provider === 'anthropic' ? 3 : 0,
          cache_write_input_tokens_1h: provider === 'anthropic' ? 1 : 0,
        },
      },
    });
    const checkpoint = events.find((e) => e.type === 'checkpoint');
    if (checkpoint?.type !== 'checkpoint') throw new Error('checkpoint missing');
    expect(decodePiCheckpoint(checkpoint.checkpoint).modelProvider).toBe(provider);
    const wrongProvider = new PiSdkWorker(() => {});
    cleanup.push(() => wrongProvider.close());
    await expect(
      wrongProvider.handle({
        ...start,
        checkpoint: capturePiCheckpoint(checkpoint.checkpoint.threadId, {
          ...decodePiCheckpoint(checkpoint.checkpoint),
          modelProvider: 'openai',
          model: MODELS.openai,
        }),
      }),
    ).rejects.toThrow('model differs');
    const resumed = new PiSdkWorker((e) => events.push(e));
    cleanup.push(() => resumed.close());
    await resumed.handle({ ...start, checkpoint: checkpoint.checkpoint });
    await resumed.handle({ type: 'submit', text: 'continue' });
    expect(JSON.stringify(api.requests[2])).toContain('Remember my write');
    expect(events.filter((e) => e.type === 'failure')).toEqual([]);
    const rotated = await createProviderFixture(provider, { apiKey: 'rotated-fixture-token' });
    cleanup.push(rotated.close);
    await resumed.refreshOptions({ apiKey: rotated.apiKey, baseUrl: rotated.url });
    await resumed.handle({ type: 'submit', text: 'After credential rotation' });
    expect(events.filter((e) => e.type === 'failure')).toEqual([]);
    expect(rotated.requests).toHaveLength(1);
    expect(JSON.stringify(rotated.requests[0])).toContain('Remember my write');
  },
);
it.each(['anthropic', 'deepseek', 'google', 'zai'] as const)(
  'rejects malformed raw %s usage before committing history',
  async (provider) => {
    const api = await createProviderFixture(provider, { invalidUsage: true });
    cleanup.push(api.close);
    const root = await mkdtemp(join(tmpdir(), 'pi-invalid-'));
    cleanup.push(() => rm(root, { recursive: true, force: true }));
    const events: WorkerEvent[] = [];
    const worker = new PiSdkWorker((e) => events.push(e));
    cleanup.push(() => worker.close());
    await worker.handle({
      type: 'start',
      root,
      sessionId: 'ses_sdk',
      model: MODELS[provider],
      modelProvider: provider,
      system: 'test',
      apiKey: api.apiKey,
      baseUrl: api.url,
      tools: [],
    });
    await worker.handle({ type: 'submit', text: 'hello' });
    expect(events).toContainEqual(expect.objectContaining({ type: 'failure', fatal: true }));
    expect(events.some((e) => e.type === 'checkpoint')).toBe(false);
  },
);
