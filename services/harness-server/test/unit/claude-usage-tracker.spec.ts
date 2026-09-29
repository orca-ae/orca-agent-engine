// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  ClaudeUsageTracker,
  awaitUsageAcknowledgment,
} from '../../src/harness/claude/usage-tracker.js';

const usage = {
  input_tokens: 10,
  output_tokens: 0,
  cache_read_input_tokens: 3,
  cache_creation: { ephemeral_1h_input_tokens: 4, ephemeral_5m_input_tokens: 5 },
};
const signal = () => new AbortController().signal;
const partial = (event: unknown, parent: string | null = null) => ({
  type: 'stream_event',
  session_id: 'session',
  parent_tool_use_id: parent,
  event,
});
const start = (id: string, parent: string | null = null) =>
  partial({ type: 'message_start', message: { id, model: 'actual', usage } }, parent);
const tool = (id: string, parent: string | null = null) =>
  partial({ type: 'content_block_start', content_block: { type: 'tool_use', id } }, parent);
const stop = (parent: string | null = null) => partial({ type: 'message_stop' }, parent);

describe('Claude usage tracker lifecycle', () => {
  it('preserves empty compatibility callbacks only on normal completion, never query failure or abort', async () => {
    const finished = new ClaudeUsageTracker('fallback', async () => {});
    finished.close(true);
    await expect(finished.waitForTool('standalone', signal())).resolves.toBe(true);
    const failed = new ClaudeUsageTracker('fallback', async () => {});
    failed.close();
    await expect(failed.waitForTool('failed', signal())).resolves.toBe(false);
    // A later successful cleanup must not reopen an aborted tracker.
    failed.close(true);
    await expect(failed.waitForTool('aborted', signal())).resolves.toBe(false);
  });
  it('waits for a tool whose partial has not been consumed and merges cumulative deltas', async () => {
    const ack = deferred();
    const report = vi.fn(() => ack.promise);
    const tracker = new ClaudeUsageTracker('fallback', report);
    await tracker.observe(start('message'));
    const decided = vi.fn();
    const permission = tracker.waitForTool('tool', signal()).then(decided);
    await tracker.observe(tool('tool'));
    await tracker.observe(partial({ type: 'message_delta', usage: { output_tokens: 7 } }));
    await tracker.observe(
      partial({
        type: 'message_delta',
        usage: { output_tokens: 9, cache_creation: { ephemeral_1h_input_tokens: 8 } },
      }),
    );
    const finalizing = tracker.observe(stop());
    await Promise.resolve();
    expect(decided).not.toHaveBeenCalled();
    expect(report).toHaveBeenCalledWith(
      {
        ...usage,
        output_tokens: 9,
        cache_creation: { ephemeral_1h_input_tokens: 8, ephemeral_5m_input_tokens: 5 },
      },
      'actual',
      undefined,
      undefined,
    );
    ack.resolve();
    await finalizing;
    await permission;
    expect(decided).toHaveBeenCalledWith(true);
    // Duplicate partial + buffered assistant frames cannot bill the call twice.
    await tracker.observe(start('message'));
    await tracker.observe(stop());
    await tracker.observe({
      type: 'assistant',
      session_id: 'session',
      parent_tool_use_id: null,
      message: { id: 'message', usage },
    });
    expect(report).toHaveBeenCalledTimes(1);
  });

  it('keeps concurrent parent/subagent messages and their tool ACKs separate', async () => {
    const report = vi.fn(async () => {});
    const tracker = new ClaudeUsageTracker('fallback', report);
    await tracker.observe(start('parent'));
    await tracker.observe(start('child', 'agent_dispatch'));
    const parentDecision = vi.fn();
    const parentPermission = tracker.waitForTool('parent_tool', signal()).then(parentDecision);
    const childPermission = tracker.waitForTool('child_tool', signal());
    await tracker.observe(tool('parent_tool'));
    await tracker.observe(tool('child_tool', 'agent_dispatch'));
    await tracker.observe(
      partial({ type: 'message_delta', usage: { output_tokens: 11 } }, 'agent_dispatch'),
    );
    await tracker.observe(stop('agent_dispatch'));
    await expect(childPermission).resolves.toBe(true);
    expect(parentDecision).not.toHaveBeenCalled();
    expect(report).toHaveBeenLastCalledWith(
      { ...usage, output_tokens: 11 },
      'actual',
      'agent_dispatch',
      undefined,
    );
    await tracker.observe(partial({ type: 'message_delta', usage: { output_tokens: 2 } }));
    await tracker.observe(stop());
    await parentPermission;
    expect(parentDecision).toHaveBeenCalledWith(true);
    expect(report).toHaveBeenLastCalledWith(
      { ...usage, output_tokens: 2 },
      'actual',
      undefined,
      undefined,
    );
  });

  it('denies both known and unmatched pending tool ids when a usage ACK fails', async () => {
    const tracker = new ClaudeUsageTracker('fallback', async () => {
      throw new Error('ACK failed');
    });
    await tracker.observe(start('message'));
    await tracker.observe(tool('known'));
    const known = tracker.waitForTool('known', signal());
    const unknown = tracker.waitForTool('unknown', signal());
    await tracker.observe(partial({ type: 'message_delta', usage: { output_tokens: 0 } }));
    await expect(tracker.observe(stop())).rejects.toThrow('ACK failed');
    await expect(known).resolves.toBe(false);
    await expect(unknown).resolves.toBe(false);
    await expect(tracker.waitForTool('later', signal())).resolves.toBe(false);
  });

  it('releases unmatched ids on query completion and individually cancelled permission requests', async () => {
    const tracker = new ClaudeUsageTracker('fallback', async () => {});
    await tracker.observe(start('message'));
    const controller = new AbortController();
    const aborted = tracker.waitForTool('aborted', controller.signal);
    const unmatched = tracker.waitForTool('unmatched', signal());
    controller.abort();
    await expect(aborted).resolves.toBe(false);
    await tracker.observe(partial({ type: 'message_delta', usage: { output_tokens: 0 } }));
    await tracker.observe(stop());
    await expect(unmatched).resolves.toBe(false);
    await expect(tracker.waitForTool('too_late', signal())).resolves.toBe(false);
    tracker.close();
  });

  it('preserves assistant-only usage and waits on its pending ACK', async () => {
    const ack = deferred();
    const report = vi.fn(() => ack.promise);
    const tracker = new ClaudeUsageTracker('fallback', report);
    const message = {
      type: 'assistant',
      message: {
        id: 'legacy',
        content: [{ type: 'tool_use', id: 'legacy_tool' }],
        usage: { input_tokens: 2, output_tokens: 3, cache_creation_input_tokens: 6 },
      },
    };
    const observing = tracker.observe(message);
    const decided = vi.fn();
    const permission = tracker.waitForTool('legacy_tool', signal()).then(decided);
    await Promise.resolve();
    expect(decided).not.toHaveBeenCalled();
    ack.resolve();
    await observing;
    await permission;
    await tracker.observe(message);
    expect(report).toHaveBeenCalledTimes(1);
    expect(report).toHaveBeenCalledWith(
      {
        input_tokens: 2,
        output_tokens: 3,
        cache_read_input_tokens: 0,
        cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 6 },
      },
      'fallback',
      undefined,
      undefined,
    );
  });

  it('uses eager final subagent entries before forwarded frames, serializes ACKs, and deduplicates mirror retries', async () => {
    const firstAck = deferred();
    const report = vi
      .fn()
      .mockImplementationOnce(() => firstAck.promise)
      .mockResolvedValue(undefined);
    const tracker = new ClaudeUsageTracker('fallback', report, true);
    const controller = new AbortController();
    const firstPermission = tracker.waitForTool('first_tool', controller.signal, 'runtime_child');
    const stored = {
      sessionId: 'session',
      projectKey: 'project',
      subpath: 'subagents/agent-runtime_child',
    };
    const entry = {
      type: 'assistant',
      agentId: 'runtime_child',
      attributionAgent: 'worker',
      message: {
        id: 'stored_child',
        model: 'child_model',
        content: [{ type: 'tool_use', id: 'first_tool' }],
        stop_reason: 'tool_use',
        usage: { ...usage, output_tokens: 12 },
      },
    };
    const first = tracker.observeStoredEntries(stored, [entry]);
    const replay = tracker.observeStoredEntries(stored, [entry]);
    await tracker.observe(start('parent'));
    await tracker.observe(partial({ type: 'message_delta', usage: { output_tokens: 0 } }));
    const second = tracker.observe(stop());
    await Promise.resolve();
    expect(report).toHaveBeenCalledTimes(1);
    expect(report).toHaveBeenLastCalledWith(
      { ...usage, output_tokens: 12 },
      'child_model',
      undefined,
      'worker',
    );
    firstAck.resolve();
    await Promise.all([first, replay, second]);
    await expect(firstPermission).resolves.toBe(true);
    expect(report).toHaveBeenCalledTimes(2);
    await tracker.observe({
      type: 'assistant',
      session_id: 'session',
      parent_tool_use_id: 'agent_dispatch',
      message: { ...entry.message, usage },
    });
    await tracker.observeStoredEntries(stored, [entry]);
    expect(report).toHaveBeenCalledTimes(2);
  });

  it('waits for final child usage while retaining tool ids from earlier content blocks', async () => {
    const report = vi.fn(async () => {});
    const tracker = new ClaudeUsageTracker('fallback', report, true);
    const stored = {
      sessionId: 'session',
      projectKey: 'project',
      subpath: 'subagents/agent-runtime_child',
    };
    const firstMessage = {
      id: 'child',
      model: 'child_model',
      content: [{ type: 'tool_use', id: 'first' }],
      stop_reason: null,
      usage,
    };
    await tracker.observeStoredEntries(stored, [{ type: 'assistant', message: firstMessage }]);
    const permission = tracker.waitForTool('first', signal(), 'runtime_child');
    await tracker.observe({
      type: 'assistant',
      session_id: 'session',
      parent_tool_use_id: 'dispatch',
      message: firstMessage,
    });
    expect(report).not.toHaveBeenCalled();
    await tracker.observeStoredEntries(stored, [
      {
        type: 'assistant',
        attributionAgent: 'worker',
        message: {
          ...firstMessage,
          content: [{ type: 'text', text: 'done' }],
          stop_reason: 'tool_use',
          usage: { ...usage, output_tokens: 9 },
        },
      },
    ]);
    await expect(permission).resolves.toBe(true);
    expect(report).toHaveBeenCalledTimes(1);
  });

  it.each([
    ['missing usage', undefined],
    ['missing input', { output_tokens: 0 }],
    ['null input', { input_tokens: null, output_tokens: 0 }],
    ['string input', { input_tokens: '10', output_tokens: 0 }],
    ['negative input', { input_tokens: -1, output_tokens: 0 }],
    ['fractional input', { input_tokens: 0.5, output_tokens: 0 }],
    ['nonfinite input', { input_tokens: Infinity, output_tokens: 0 }],
    ['invalid cache read', { input_tokens: 0, output_tokens: 0, cache_read_input_tokens: '3' }],
    [
      'invalid cache creation',
      {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation: { ephemeral_1h_input_tokens: -1, ephemeral_5m_input_tokens: 0 },
      },
    ],
  ])(
    'rejects streamed %s instead of acknowledging fabricated zero usage',
    async (_name, initialUsage) => {
      const report = vi.fn(async () => {});
      const tracker = new ClaudeUsageTracker('fallback', report);
      await tracker.observe(
        partial({
          type: 'message_start',
          message: { id: 'bad_usage', model: 'actual', usage: initialUsage },
        }),
      );
      await tracker.observe(tool('bad_tool'));
      const permission = tracker.waitForTool('bad_tool', signal());
      await tracker.observe(partial({ type: 'message_delta', usage: { output_tokens: 0 } }));
      await expect(tracker.observe(stop())).rejects.toThrow('missing or invalid final usage');
      await expect(permission).resolves.toBe(false);
      expect(report).not.toHaveBeenCalled();
    },
  );

  it.each([
    undefined,
    {},
    { output_tokens: undefined },
    { output_tokens: null },
    { output_tokens: '500' },
    { output_tokens: NaN },
    { output_tokens: -1 },
  ])('rejects a missing or malformed final output delta: %j', async (finalUsage) => {
    const report = vi.fn(async () => {});
    const tracker = new ClaudeUsageTracker('fallback', report);
    await tracker.observe(start('bad_delta'));
    await tracker.observe(tool('bad_tool'));
    const permission = tracker.waitForTool('bad_tool', signal());
    if (finalUsage !== undefined)
      await tracker.observe(partial({ type: 'message_delta', usage: finalUsage }));
    await expect(tracker.observe(stop())).rejects.toThrow('missing or invalid final usage');
    await expect(permission).resolves.toBe(false);
    expect(report).not.toHaveBeenCalled();
  });

  it.each([
    undefined,
    {},
    { input_tokens: 0 },
    { input_tokens: 0, output_tokens: -1 },
    { input_tokens: 0, output_tokens: '500' },
  ])('rejects malformed final subagent mirror usage: %j', async (finalUsage) => {
    const report = vi.fn(async () => {});
    const tracker = new ClaudeUsageTracker('fallback', report, true);
    const permission = tracker.waitForTool('bad_tool', signal(), 'runtime_child');
    await expect(
      tracker.observeStoredEntries(
        { sessionId: 'session', projectKey: 'project', subpath: 'subagents/agent-runtime_child' },
        [
          {
            type: 'assistant',
            attributionAgent: 'worker',
            message: {
              id: 'bad_mirror',
              model: 'actual',
              content: [{ type: 'tool_use', id: 'bad_tool' }],
              stop_reason: 'tool_use',
              usage: finalUsage,
            },
          },
        ],
      ),
    ).rejects.toThrow('missing or invalid final usage');
    await expect(permission).resolves.toBe(false);
    expect(report).not.toHaveBeenCalled();
  });

  it('accepts explicit zero final usage, preserves nullable delta input/cache fields, and closes incomplete streams on cancellation', async () => {
    const report = vi.fn(async () => {});
    const tracker = new ClaudeUsageTracker('fallback', report);
    await tracker.observe(
      partial({
        type: 'message_start',
        message: {
          id: 'zero',
          usage: {
            input_tokens: 0,
            output_tokens: 0,
            cache_creation: null,
            cache_creation_input_tokens: null,
            cache_read_input_tokens: null,
          },
        },
      }),
    );
    await tracker.observe(tool('zero_tool'));
    const zeroPermission = tracker.waitForTool('zero_tool', signal());
    await tracker.observe(
      partial({
        type: 'message_delta',
        usage: {
          output_tokens: 0,
          input_tokens: null,
          cache_read_input_tokens: null,
          cache_creation_input_tokens: null,
        },
      }),
    );
    await tracker.observe(stop());
    await expect(zeroPermission).resolves.toBe(true);
    expect(report).not.toHaveBeenCalled();
    await tracker.observe(start('partial'));
    await tracker.observe(tool('cancelled_tool'));
    const cancelled = tracker.waitForTool('cancelled_tool', signal());
    tracker.close();
    await expect(cancelled).resolves.toBe(false);
    expect(report).not.toHaveBeenCalled();
  });

  it('accepts explicit zero mirror usage and preserves positive counts across nullable cumulative deltas', async () => {
    const report = vi.fn(async () => {});
    const tracker = new ClaudeUsageTracker('fallback', report, true);
    const permission = tracker.waitForTool('zero_mirror', signal(), 'runtime_child');
    await tracker.observeStoredEntries(
      { sessionId: 'session', projectKey: 'project', subpath: 'subagents/agent-runtime_child' },
      [
        {
          type: 'assistant',
          message: {
            id: 'zero_child',
            content: [{ type: 'tool_use', id: 'zero_mirror' }],
            stop_reason: 'tool_use',
            usage: { input_tokens: 0, output_tokens: 0 },
          },
        },
      ],
    );
    await expect(permission).resolves.toBe(true);
    expect(report).not.toHaveBeenCalled();
    await tracker.observe(start('nullable_delta'));
    await tracker.observe(
      partial({
        type: 'message_delta',
        usage: {
          output_tokens: 2,
          input_tokens: null,
          cache_read_input_tokens: null,
          cache_creation_input_tokens: null,
          cache_creation: null,
        },
      }),
    );
    await tracker.observe(stop());
    expect(report).toHaveBeenCalledWith(
      { ...usage, output_tokens: 2 },
      'actual',
      undefined,
      undefined,
    );
  });

  it.each(['ack', 'failure', 'abort'] as const)(
    'keeps zero-usage tools behind a preceding nonzero ACK (%s)',
    async (outcome) => {
      const ack = deferred();
      const report = vi.fn(() => ack.promise);
      const tracker = new ClaudeUsageTracker('fallback', report);
      await tracker.observe(start('nonzero'));
      await tracker.observe(partial({ type: 'message_delta', usage: { output_tokens: 5 } }));
      const preceding = tracker.observe(stop()).then(
        () => undefined,
        (error: unknown) => error,
      );
      await tracker.observe(
        partial(
          {
            type: 'message_start',
            message: { id: 'zero_after_nonzero', usage: { input_tokens: 0, output_tokens: 0 } },
          },
          'zero_dispatch',
        ),
      );
      await tracker.observe(tool('zero_tool', 'zero_dispatch'));
      const decided = vi.fn();
      const permission = tracker.waitForTool('zero_tool', signal()).then(decided);
      await tracker.observe(
        partial({ type: 'message_delta', usage: { output_tokens: 0 } }, 'zero_dispatch'),
      );
      const finalizingZero = tracker.observe(stop('zero_dispatch'));
      try {
        await Promise.resolve();
        expect(decided).not.toHaveBeenCalled();
        expect(report).toHaveBeenCalledTimes(1);
        if (outcome === 'abort') {
          tracker.close();
          await permission;
          expect(decided).toHaveBeenCalledWith(false);
        }
        if (outcome === 'failure') ack.reject(new Error('preceding ACK failed'));
        else ack.resolve();
        const precedingError = await preceding;
        if (outcome === 'failure')
          expect(precedingError).toEqual(new Error('preceding ACK failed'));
        await finalizingZero;
        await permission;
        expect(decided).toHaveBeenCalledWith(outcome === 'ack');
        expect(report).toHaveBeenCalledTimes(1);
      } finally {
        tracker.close();
        ack.resolve();
        await preceding;
        await finalizingZero;
        await permission;
      }
    },
  );

  it('handles a late rejected Registry request after cancellation', async () => {
    const controller = new AbortController();
    let reject!: (error: Error) => void;
    const remote = new Promise<void>((_resolve, fail) => {
      reject = fail;
    });
    const waiting = awaitUsageAcknowledgment(remote, controller.signal);
    controller.abort();
    await expect(waiting).rejects.toThrow('interrupted');
    reject(new Error('late remote failure'));
    await Promise.resolve();
  });
});

function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { resolve, reject, promise };
}
