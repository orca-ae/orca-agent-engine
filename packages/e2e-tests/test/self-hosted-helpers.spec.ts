// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import { buildClientFromConfig } from '../src/client.js';
import { runTurn, type SseFrame } from './self-hosted-helpers.js';

const cfg = buildClientFromConfig({ baseURL: 'http://127.0.0.1:8080', apiKey: 'fixture' });
const user = { type: 'user.message', id: 'evt_current' };

afterEach(() => vi.unstubAllGlobals());

function mockTranscript(frames: SseFrame[]) {
  const pending = [...frames];
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      const frame = pending.shift();
      if (frame) controller.enqueue(encoder.encode(`data: ${JSON.stringify(frame)}\n\n`));
      else controller.close();
    },
  });
  const fetch = vi
    .fn<typeof globalThis.fetch>()
    .mockResolvedValueOnce(new Response(JSON.stringify({ data: [{ id: user.id }] })))
    .mockResolvedValueOnce(
      new Response(stream, { headers: { 'content-type': 'text/event-stream' } }),
    );
  vi.stubGlobal('fetch', fetch);
  return fetch;
}

describe('self-hosted turn correlation', () => {
  it.each(['turn_event_id', 'source_event_id'])(
    'ignores prior terminals and failures before and after the user event (%s)',
    async (field) => {
      const message = { type: 'agent.message', content: [{ type: 'text', text: 'current' }] };
      const completed = { type: 'agent.turn_completed', [field]: user.id };
      const fetch = mockTranscript([
        { type: 'user.message', id: 'evt_previous' },
        { type: 'agent.turn_failed', [field]: 'evt_previous' },
        { type: 'agent.turn_completed', [field]: 'evt_previous' },
        user,
        { type: 'agent.turn_completed', [field]: 'evt_previous' },
        { type: 'agent.turn_failed', [field]: 'evt_previous' },
        { type: 'agent.error', [field]: 'evt_previous' },
        message,
        completed,
        { type: 'agent.turn_failed', [field]: 'evt_later' },
      ]);
      expect(await runTurn(cfg, 'ses_fixture', 'Hello', 1_000)).toEqual([message, completed]);
      expect(fetch.mock.calls[0]![1]!.method).toBe('POST');
      expect(fetch.mock.calls[1]![0]).toBe(
        cfg.baseURL + '/v1/sessions/ses_fixture/events/stream?from_cursor=0',
      );
    },
  );

  it('waits for the parent completion when a child finishes first', async () => {
    const child = { type: 'agent.turn_completed', subpath: 'child', source_event_id: user.id };
    const parent = { type: 'agent.turn_completed', source_event_id: user.id };
    mockTranscript([user, child, parent]);
    expect(await runTurn(cfg, 'ses_fixture', 'Hello', 1_000)).toEqual([child, parent]);
  });

  it.each(['agent.turn_failed', 'agent.error'])(
    'surfaces current, child or unattributed %s rather than accepting a later completion',
    async (type) => {
      for (const identity of [
        { source_event_id: user.id },
        { turn_event_id: user.id },
        { source_event_id: 'evt_child', subpath: 'child' },
        {},
      ]) {
        mockTranscript([
          user,
          { type, ...identity },
          { type: 'agent.turn_completed', turn_event_id: user.id },
        ]);
        await expect(runTurn(cfg, 'ses_fixture', 'Hello', 1_000)).rejects.toThrow('agent failure');
      }
    },
  );

  it.each([
    {
      name: 'missing user replay',
      frames: [{ type: 'agent.turn_completed', turn_event_id: user.id }],
    },
    { name: 'uncorrelated completion', frames: [user, { type: 'agent.turn_completed' }] },
    {
      name: 'another turn completion',
      frames: [user, { type: 'agent.turn_completed', source_event_id: 'evt_other' }],
    },
  ])('rejects $name', async ({ frames }) => {
    mockTranscript(frames);
    await expect(runTurn(cfg, 'ses_fixture', 'Hello', 1_000)).rejects.toThrow(
      'no agent.turn_completed',
    );
  });
});
