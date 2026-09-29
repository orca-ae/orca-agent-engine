// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { createServer } from 'node:http';
import { join } from 'node:path';
import {
  PiSdkWorker,
  decodePiCheckpoint,
  transitionPiCheckpointInstructions,
} from '../../src/index.js';
import { ModelRuntime } from '@earendil-works/pi-coding-agent';
import type { WorkerEvent, StartCommand } from '@orca/sdk-harness';
import { createResponsesFixture } from '../../../codex-harness/test/support/responses-fixture.js';

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
it('runs the installed Pi SDK with only managed tools and restores native history', async () => {
  const api = await createResponsesFixture({ callTool: true });
  cleanup.push(api.close);
  const root = await mkdtemp(join(tmpdir(), 'pi-test-'));
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
    root,
    sessionId: 'ses_sdk',
    model: 'gpt-5.4',
    system: 'Managed instructions',
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
  };
  await worker.handle(start);
  await worker.handle({ type: 'submit', text: 'write proof' });
  expect(events.filter((event) => event.type === 'failure')).toEqual([]);
  expect(events.some((event) => event.type === 'tool_call')).toBe(true);
  expect(api.requests).toHaveLength(2);
  const cp = events.find((event) => event.type === 'checkpoint');
  expect(cp?.type).toBe('checkpoint');
  if (cp?.type !== 'checkpoint') throw new Error('checkpoint missing');
  expect(decodePiCheckpoint(cp.checkpoint).system).toBe(start.system);
  await worker.close();
  const resumedEvents: WorkerEvent[] = [];
  const resumed = new PiSdkWorker((event) => resumedEvents.push(event));
  cleanup.push(() => resumed.close());
  await resumed.handle({ ...start, checkpoint: cp.checkpoint });
  await resumed.handle({ type: 'submit', text: 'continue' });
  expect(resumedEvents.filter((event) => event.type === 'failure')).toEqual([]);
  expect(JSON.stringify(api.requests[2]!.input)).toContain('write proof');
  expect(resumedEvents).toContainEqual(
    expect.objectContaining({
      type: 'checkpoint',
      checkpoint: expect.objectContaining({ threadId: cp.checkpoint.threadId }),
    }),
  );
}, 20000);

async function plainWorker(
  options: Parameters<typeof createResponsesFixture>[0] = {},
  overrides: Partial<StartCommand> = {},
) {
  const api = await createResponsesFixture(options);
  cleanup.push(api.close);
  const root = await mkdtemp(join(tmpdir(), 'pi-test-'));
  cleanup.push(() => rm(root, { recursive: true, force: true }));
  const events: WorkerEvent[] = [];
  const worker = new PiSdkWorker((event) => events.push(event));
  cleanup.push(() => worker.close());
  const start: StartCommand = {
    type: 'start',
    root,
    sessionId: 'ses_sdk',
    model: 'gpt-5.4',
    system: 'Managed instructions',
    apiKey: api.apiKey,
    baseUrl: api.url,
    tools: [],
  };
  Object.assign(start, overrides);
  await worker.handle(start);
  return { api, events, worker, start };
}

it('rotates credentials and endpoint without losing native conversation history', async () => {
  const h = await plainWorker();
  await h.worker.handle({ type: 'submit', text: 'Remember the private proof' });
  const next = await createResponsesFixture({ apiKey: 'rotated-token' });
  cleanup.push(next.close);
  await h.worker.refreshOptions({ apiKey: next.apiKey, baseUrl: next.url });
  await h.worker.handle({ type: 'submit', text: 'Continue after rotation' });
  expect(h.api.requests).toHaveLength(1);
  expect(next.requests).toHaveLength(1);
  expect(JSON.stringify(next.requests[0]!.input)).toContain('Remember the private proof');
  expect(h.events.filter((event) => event.type === 'failure')).toEqual([]);
  await expect(h.worker.refreshOptions({ apiKey: next.apiKey, system: 'changed' })).rejects.toThrow(
    'pinned',
  );
}, 20000);

it.each([
  null,
  {},
  { input_tokens: -1, output_tokens: 1 },
  { input_tokens: 2, output_tokens: 1, input_tokens_details: { cached_tokens: 3 } },
])(
  'fails closed before Pi normalizes malformed terminal usage: %j',
  async (terminalUsage) => {
    const h = await plainWorker({ terminalUsage });
    await h.worker.handle({ type: 'submit', text: 'hello' });
    expect(h.events).toContainEqual(expect.objectContaining({ type: 'failure', fatal: true }));
    expect(h.events.some((event) => event.type === 'checkpoint')).toBe(false);
    expect(
      h.events.some((event) => event.type === 'event' && event.event.type === 'turn.completed'),
    ).toBe(false);
    expect(h.api.requests).toHaveLength(1);
    await expect(h.worker.handle({ type: 'submit', text: 'retry' })).rejects.toThrow('not ready');
  },
  20000,
);

it('restores an approved instruction transition and rejects a different model or malformed history', async () => {
  const h = await plainWorker();
  await h.worker.handle({ type: 'submit', text: 'Keep this conversation' });
  const event = h.events.find((event) => event.type === 'checkpoint');
  if (event?.type !== 'checkpoint') throw new Error('checkpoint missing');
  const original = event.checkpoint;
  expect(original.sdkVersion).toBe('0.87.1');
  expect(() => decodePiCheckpoint({ ...original, sdkVersion: '0.87.0' })).not.toThrow();
  const system = 'Managed instructions with an updated Skill catalog';
  expect(() => transitionPiCheckpointInstructions(original, system, () => false)).toThrow(
    'pinned Skill',
  );
  const updated = transitionPiCheckpointInstructions(
    original,
    system,
    (previous) => previous === h.start.system,
  );
  expect(decodePiCheckpoint(original).system).toBe(h.start.system);
  expect(decodePiCheckpoint(updated).entries).toEqual(decodePiCheckpoint(original).entries);
  const events: WorkerEvent[] = [];
  const next = new PiSdkWorker((event) => events.push(event));
  cleanup.push(() => next.close());
  await next.handle({ ...h.start, system, checkpoint: updated });
  await next.handle({ type: 'submit', text: 'Continue with the new catalog' });
  expect(events.some((event) => event.type === 'failure')).toBe(false);
  expect(JSON.stringify(h.api.requests[1])).toContain(system);
  expect(JSON.stringify(h.api.requests[1])).toContain('Keep this conversation');
  for (const patch of [{ model: 'gpt-5.4-mini' }, { system: 'unapproved' }]) {
    const invalid = new PiSdkWorker(() => {});
    cleanup.push(() => invalid.close());
    await expect(invalid.handle({ ...h.start, checkpoint: original, ...patch })).rejects.toThrow(
      /pinned Agent/,
    );
  }
  expect(() => decodePiCheckpoint({ ...original, sdkVersion: 'unknown' })).toThrow();
  expect(() =>
    decodePiCheckpoint({
      ...original,
      files: { 'session.json': Buffer.from('{}').toString('base64') },
    }),
  ).toThrow();
}, 20000);

it('rejects a retired 0.87.0 checkpoint model with a migration error while restoring supported models', async () => {
  const h = await plainWorker();
  await h.worker.handle({ type: 'submit', text: 'Keep this conversation' });
  const event = h.events.find((entry) => entry.type === 'checkpoint');
  if (event?.type !== 'checkpoint') throw new Error('checkpoint missing');
  const supportedLegacy = { ...event.checkpoint, sdkVersion: '0.87.0' };
  const resumed = new PiSdkWorker(() => {});
  cleanup.push(() => resumed.close());
  await resumed.handle({ ...h.start, checkpoint: supportedLegacy });

  const history = decodePiCheckpoint(event.checkpoint);
  const retiredLegacy = {
    ...supportedLegacy,
    files: {
      'session.json': Buffer.from(
        JSON.stringify({ ...history, modelProvider: 'opencode', model: 'mimo-v2.5-free' }),
      ).toString('base64'),
    },
  };
  const retiredError =
    /Pi 0\.87\.0 checkpoint model 'opencode\/mimo-v2\.5-free' is unavailable in Pi 0\.87\.1; start a new session/;
  expect(() => decodePiCheckpoint(retiredLegacy)).toThrow(retiredError);
  const rejected = new PiSdkWorker(() => {});
  cleanup.push(() => rejected.close());
  await expect(
    rejected.handle({
      ...h.start,
      modelProvider: 'opencode',
      model: 'mimo-v2.5-free',
      checkpoint: retiredLegacy,
    }),
  ).rejects.toThrow(retiredError);
}, 20000);

it.each([undefined, 'low', 'high', 'xhigh', 'max', 'ultra'])(
  'preserves pinned reasoning effort %s on the Responses wire',
  async (effort) => {
    const h = await plainWorker({}, { model: 'gpt-6-astra', ...(effort ? { effort } : {}) });
    await h.worker.handle({ type: 'submit', text: 'hello' });
    expect(h.events.filter((event) => event.type === 'failure')).toEqual([]);
    expect(h.api.requests[0]?.reasoning).toMatchObject({ effort: effort ?? 'medium' });
  },
  20000,
);

it('continues after a user interrupt without inventing terminal usage or committing aborted history', async () => {
  const h = await plainWorker(
    { callTool: true },
    { tools: [{ name: 'write', inputSchema: { type: 'object' } }] },
  );
  const turn = h.worker.handle({ type: 'submit', text: 'wait at the managed tool' });
  await vi.waitFor(() => expect(h.events.some((e) => e.type === 'tool_call')).toBe(true));
  await h.worker.handle({ type: 'interrupt' });
  await turn;
  expect(h.events.filter((e) => e.type === 'failure' && e.fatal)).toEqual([]);
  const boundary = h.events.length;
  await h.worker.handle({ type: 'submit', text: 'continue after cancellation' });
  expect(h.events.slice(boundary).some((e) => e.type === 'failure')).toBe(false);
  expect(h.events.slice(boundary).some((e) => e.type === 'checkpoint')).toBe(true);
});

it('releases a failed startup and accepts a corrected start', async () => {
  const h = await plainWorker();
  const worker = new PiSdkWorker(() => {});
  cleanup.push(() => worker.close());
  await expect(worker.handle({ ...h.start, apiKey: '' })).rejects.toThrow(
    'explicit LLM credentials',
  );
  const lookup = vi
    .spyOn(ModelRuntime.prototype, 'getModel')
    .mockReturnValueOnce(undefined)
    .mockReturnValueOnce(undefined);
  try {
    await expect(worker.handle(h.start)).rejects.toThrow('managed model is unavailable');
  } finally {
    lookup.mockRestore();
  }

  await worker.handle(h.start);
  await worker.handle({ type: 'submit', text: 'started after a corrected config' });
  expect(h.api.requests).toHaveLength(1);
});

it('aborts an unfinished provider stream without poisoning the next turn or reporting zero usage', async () => {
  let requested = false;
  const server = createServer((_req, res) => {
    requested = true;
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(': waiting for provider\n\n');
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  cleanup.push(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('missing fixture port');
  const h = await plainWorker({}, { baseUrl: `http://127.0.0.1:${address.port}/v1` });
  const turn = h.worker.handle({ type: 'submit', text: 'cancel an unfinished request' });
  await vi.waitFor(() => expect(requested).toBe(true));
  await h.worker.handle({ type: 'interrupt' });
  await turn;
  expect(h.events.filter((e) => e.type === 'failure' && e.fatal)).toEqual([]);
  expect(
    h.events.some(
      (e) => e.type === 'checkpoint' || (e.type === 'event' && e.event.type === 'turn.completed'),
    ),
  ).toBe(false);
  const boundary = h.events.length;
  await h.worker.refreshOptions({ apiKey: h.api.apiKey, baseUrl: h.api.url });
  await h.worker.handle({ type: 'submit', text: 'recover without losing a follow-up' });
  expect(h.events.slice(boundary).filter((e) => e.type === 'failure')).toEqual([]);
  expect(h.events.slice(boundary).some((e) => e.type === 'checkpoint')).toBe(true);
});
