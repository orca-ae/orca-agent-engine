// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type { WorkerEvent, StartCommand } from '@orca/codex-harness';
import { RemoteCodexSdkWorker } from '../../src/harness/codex-sdk/remote-worker.js';
import type { HarnessChannel, RawSandboxEvent } from '../../src/harness/in-sandbox/transport.js';
import type { SessionStartInput } from '../../src/harness/agent-harness.js';

function fixture() {
  const queue: RawSandboxEvent[] = [];
  let wake: (() => void) | undefined;
  let stopped = false;
  const sdkCommand = vi.fn(async () => 0);
  const channel: HarnessChannel = {
    sdkCommand,
    submit: async () => {},
    async *events() {
      while (!stopped) {
        if (queue.length) yield queue.shift()!;
        else
          await new Promise<void>((resolve) => {
            wake = resolve;
          });
      }
    },
    stop: async () => {
      stopped = true;
      wake?.();
    },
  };
  const events: WorkerEvent[] = [];
  const worker = new RemoteCodexSdkWorker((event) => events.push(event), {} as SessionStartInput, {
    open: async () => channel,
  });
  return {
    worker,
    sdkCommand,
    events,
    emit(sequence: number, event: WorkerEvent) {
      queue.push({
        id: `evt_${sequence}`,
        session_id: 'ses',
        created_at: '',
        type: 'harness.sdk_event',
        sequence,
        event,
      });
      wake?.();
    },
  };
}
const start: StartCommand = {
  type: 'start',
  root: '/host/private',
  sessionId: 'ses',
  model: 'gpt-5.4',
  system: '',
  apiKey: 'scoped-jwt',
  tools: [],
};

describe('remote SDK acknowledgement boundary', () => {
  it('waits for delayed SSE usage and checkpoint after HTTP completion and ignores old replays', async () => {
    const f = fixture();
    await f.worker.handle(start);
    f.sdkCommand.mockResolvedValue(3);
    const complete = vi.fn();
    const turn = f.worker.handle({ type: 'submit', text: 'hello' }).then(complete);
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(complete).not.toHaveBeenCalled();
    f.emit(1, {
      type: 'event',
      event: {
        type: 'turn.completed',
        usage: { input_tokens: 5, cached_input_tokens: 1, output_tokens: 2 },
      },
    });
    f.emit(2, { type: 'checkpoint', checkpoint: { version: 1, threadId: 'thread', files: {} } });
    f.emit(1, { type: 'failure', message: 'old replay' });
    f.emit(3, { type: 'done' });
    await turn;
    expect(f.events.map((event) => event.type)).toEqual(['event', 'checkpoint', 'done']);
    await f.worker.close();
  });

  it('fails a turn instead of committing a truncated native stream', async () => {
    const f = fixture();
    await f.worker.handle(start);
    f.sdkCommand.mockResolvedValue(2);
    const turn = f.worker.handle({ type: 'submit', text: 'hello' });
    const rejected = expect(turn).rejects.toThrow('SDK event delivery aborted');
    f.emit(2, { type: 'done' });
    await rejected;
    expect(f.events).toEqual([
      { type: 'failure', message: 'sandbox SDK event stream failed', fatal: true },
    ]);
    await f.worker.close();
  });

  it('rejects an unacknowledged command and a stopped delivery barrier', async () => {
    const f = fixture();
    await f.worker.handle(start);
    f.sdkCommand.mockRejectedValueOnce(new Error('child rejected command'));
    await expect(f.worker.refreshOptions({ apiKey: 'new-jwt' })).rejects.toThrow('child rejected');
    f.sdkCommand.mockResolvedValue(1);
    const turn = f.worker.handle({ type: 'submit', text: 'hello' });
    const rejected = expect(turn).rejects.toThrow('SDK event delivery aborted');
    await f.worker.close();
    await rejected;
  });
});
