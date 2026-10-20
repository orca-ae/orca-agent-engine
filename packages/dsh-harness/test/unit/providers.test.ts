// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect, it } from 'vitest';
import type { WorkerEvent, StartCommand } from '@orca/sdk-harness';
import { DshSdkWorker } from '../../dist/index.js';
import { createNativeProxyFixture } from '../../../pi-harness/test/support/native-proxy-fixture.js';
import { MODELS } from '../../../pi-harness/test/support/provider-fixture.js';

it.each(['openai', 'deepseek', 'google'] as const)(
  'runs the official %s adapter with managed tools, usage and cold restore',
  async (provider) => {
    const gateway = await createNativeProxyFixture(provider, {
      sessionId: 'ses_dsh',
      callTool: true,
      toolName: 'read',
      toolArguments: { path: '/proof.txt' },
    });
    const root = await mkdtemp(join(tmpdir(), 'dsh-provider-'));
    const events: WorkerEvent[] = [];
    let worker: DshSdkWorker;
    const emit = (event: WorkerEvent) => {
      events.push(event);
      if (event.type === 'tool_call')
        void worker.handle({
          type: 'tool_result',
          id: event.id,
          result: { content: [{ type: 'text', text: 'managed proof' }] },
        });
    };
    worker = new DshSdkWorker(emit);
    const input: StartCommand = {
      type: 'start',
      root,
      sessionId: 'ses_dsh',
      modelProvider: provider,
      model: MODELS[provider],
      system: 'Use the managed read tool.',
      apiKey: gateway.apiKey,
      dshGatewayUrl: new URL(gateway.url).origin,
      archivedThreadSubpaths: [],
      tools: [
        {
          name: 'read',
          inputSchema: {
            type: 'object',
            properties: { path: { type: 'string' } },
            required: ['path'],
          },
        },
      ],
    };
    try {
      await worker.handle(input);
      await worker.handle({
        type: 'submit',
        sourceEventId: 'evt_first',
        text: 'Read the proof file.',
      });
      expect(events.filter((event) => event.type === 'failure')).toEqual([]);
      expect(events.some((event) => event.type === 'tool_call' && event.name === 'read')).toBe(
        true,
      );
      const checkpoint = events.findLast((event) => event.type === 'checkpoint');
      expect(checkpoint?.type).toBe('checkpoint');
      const usage = events.filter(
        (event) => event.type === 'event' && event.event.type === 'turn.completed',
      );
      expect(usage).toHaveLength(1);
      expect(usage[0]).toMatchObject({
        event: { usage: { input_tokens: 30, output_tokens: 10, cached_input_tokens: 7 } },
      });
      await worker.close();
      worker = new DshSdkWorker(emit);
      await worker.handle({
        ...input,
        checkpoint: (checkpoint as Extract<WorkerEvent, { type: 'checkpoint' }>).checkpoint,
      });
      await worker.refreshOptions({ apiKey: gateway.apiKey });
      await worker.handle({
        type: 'submit',
        sourceEventId: 'evt_second',
        text: 'Remember our previous tool result.',
      });
      expect(events.filter((event) => event.type === 'failure')).toEqual([]);
      expect(gateway.requests.length).toBeGreaterThanOrEqual(3);
      expect(gateway.paths.every((path) => path.startsWith('/v1/proxy/' + provider + '/'))).toBe(
        true,
      );
    } finally {
      await worker.close();
      await gateway.close();
      await rm(root, { recursive: true, force: true });
    }
  },
  15000,
);
