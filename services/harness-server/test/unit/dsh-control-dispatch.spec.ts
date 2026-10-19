// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it, vi } from 'vitest';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import type { EachMessagePayload } from 'kafkajs';
import { Dispatcher } from '../../src/runner/dispatcher.js';

it.each(['kafka', 'transcript'] as const)(
  'delivers Registry archive/interrupt with durable IDs on %s, without cold model startup',
  async (path) => {
    const dispatcher = new Dispatcher({
      store: {} as TranscriptStore,
      groupId: 'control',
      anthropicApiKey: 'unused',
      modelDefault: 'unused',
    });
    const state = dispatcher as unknown as {
      runners: Map<string, unknown>;
      handleUserEvent: (...args: unknown[]) => Promise<void>;
      onTranscriptEvent(event: Event): Promise<void>;
      onMessage(delivery: EachMessagePayload, event: Event): Promise<void>;
    };
    const dispatch = vi.spyOn(state, 'handleUserEvent').mockResolvedValue();
    const delivery = {
      topic: 'orca.ws_test.sessions.ses_test.events',
      partition: 0,
      message: { offset: '10' },
    } as EachMessagePayload;
    const subpath = 'subagents/11111111-1111-4111-8111-111111111111';
    for (const stop_reason of ['interrupted', 'archived']) {
      const payload = { session_thread_id: 'sth_child', stop_reason };
      const event: Event = {
        id: `evt_${stop_reason}`,
        idempotencyKey: `control:${stop_reason}`,
        workspaceId: 'ws_test',
        sessionId: 'ses_test',
        subpath,
        seq: 10,
        producedBy: 'registry',
        kind: 'session.thread_status_terminated',
        producedAt: new Date().toISOString(),
        payload: Buffer.from(JSON.stringify(payload)),
      };
      const deliver = () =>
        path === 'kafka' ? state.onMessage(delivery, event) : state.onTranscriptEvent(event);
      await deliver();
      expect(dispatch).not.toHaveBeenCalled();
      state.runners.set('ws_test/ses_test', { supportsNativeThreadInterrupt: true });
      await deliver();
      expect(dispatch).toHaveBeenLastCalledWith(
        'ws_test',
        'ses_test',
        'user.interrupt',
        payload,
        { userEventId: event.id, subpath },
        0,
      );
      dispatch.mockClear();
      state.runners.clear();
    }
    // includeAllEvents also delivers our own output; it must be acknowledged without re-dispatch.
    state.runners.set('ws_test/ses_test', { supportsNativeThreadInterrupt: true });
    const output: Event = {
      id: 'evt_output',
      idempotencyKey: 'output',
      workspaceId: 'ws_test',
      sessionId: 'ses_test',
      subpath,
      seq: 11,
      producedBy: 'harness',
      kind: 'session.thread_status_terminated',
      producedAt: new Date().toISOString(),
      payload: Buffer.from(JSON.stringify({ stop_reason: 'archived' })),
    };
    if (path === 'kafka') await state.onMessage(delivery, output);
    else await state.onTranscriptEvent(output);
    expect(dispatch).not.toHaveBeenCalled();
  },
);
