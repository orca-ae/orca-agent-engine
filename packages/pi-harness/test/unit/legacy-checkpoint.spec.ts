// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { WorkerEvent } from '@orca/sdk-harness';
import { PiSdkWorker, decodePiCheckpoint } from '../../src/index.js';
import { legacyPiCheckpoint } from '../support/legacy-checkpoint.js';
import { createProviderFixture } from '../support/provider-fixture.js';

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});

it.each([
  ['openai', '0.87.1'],
  ['anthropic', '0.87.1'],
  ['deepseek', '0.87.1'],
  ['google', '0.87.1'],
  ['zai', '0.87.1'],
  ['anthropic', '1.0.4'],
] as const)(
  'continues a native %s history from Pi %s under the installed 1.5.0 SDK',
  async (provider, sdkVersion) => {
    const checkpoint = legacyPiCheckpoint(provider, sdkVersion);
    expect(checkpoint.sdkVersion).toBe(sdkVersion);
    const original = decodePiCheckpoint(checkpoint);
    const api = await createProviderFixture(provider, { callTool: true });
    cleanup.push(api.close);
    const root = await mkdtemp(join(tmpdir(), 'pi-upgrade-'));
    cleanup.push(() => rm(root, { recursive: true, force: true }));
    const events: WorkerEvent[] = [];
    const worker = new PiSdkWorker((event) => {
      events.push(event);
      if (event.type === 'tool_call')
        void worker.handle({
          type: 'tool_result',
          id: event.id,
          result: { content: [{ type: 'text', text: 'New write approved' }] },
        });
    });
    cleanup.push(() => worker.close());
    await worker.handle({
      type: 'start',
      root,
      sessionId: 'ses_sdk',
      checkpoint,
      modelProvider: provider,
      model: original.model,
      system: original.system,
      apiKey: api.apiKey,
      baseUrl: api.url,
      tools: [
        {
          name: 'write',
          inputSchema: {
            type: 'object',
            properties: { path: { type: 'string' }, content: { type: 'string' } },
          },
        },
      ],
    });
    await worker.handle({ type: 'submit', text: 'Continue the legacy conversation' });
    expect(events.filter((event) => event.type === 'failure')).toEqual([]);
    expect(api.requests).toHaveLength(2);
    expect(JSON.stringify(api.requests[0])).toContain('Remember my legacy write');
    expect(JSON.stringify(api.requests[0])).toContain('Legacy write approved');
    expect(JSON.stringify(api.requests[1])).toContain('New write approved');
    const saved = events.find((event) => event.type === 'checkpoint');
    if (saved?.type !== 'checkpoint') throw new Error('checkpoint missing');
    expect(saved.checkpoint).toMatchObject({
      sdkVersion: '1.5.0',
      threadId: checkpoint.threadId,
      instructionsSha256: checkpoint.instructionsSha256,
    });
    const resumed = decodePiCheckpoint(saved.checkpoint);
    for (const entry of original.entries) expect(resumed.entries).toContainEqual(entry);
    expect(events).toContainEqual(expect.objectContaining({ type: 'tool_call', name: 'write' }));
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
  },
);
