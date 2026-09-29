// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit test for the NDJSON frame translator (session-manager.ts#translateFrame).
//
// `translateFrame` is a PURE function and is load-bearing for the future SSE
// bridge: the bridge tails the subprocess wire and turns each raw frame into the
// same bare managed-agents events asserted here. Every mapping is locked exactly:
//   - system / control_response -> []  (housekeeping)
//   - stream_event -> completed per-message usage
//   - assistant text block      -> [agent.message]
//   - assistant tool_use block  -> [agent.tool_use]
//   - user tool_result block    -> [agent.tool_result]
//   - result is_error=false     -> [session.status_idle]
//   - result is_error=true      -> [session.status_error]
//   - unknown / garbage         -> []  (never throws)

import { describe, it, expect } from 'vitest';

import { translateFrame, type FrameTranslationState } from '../src/session-manager.js';

describe('translateFrame', () => {
  describe('housekeeping frames -> []', () => {
    it('drops a system frame', () => {
      expect(translateFrame({ type: 'system', subtype: 'init', session_id: 'sess_1' })).toEqual([]);
    });

    it('drops internal turn_complete marker', () => {
      expect(
        translateFrame({ type: 'system', subtype: 'turn_complete', session_id: 'sess_1' }),
      ).toEqual([]);
    });

    it('drops a control_response frame', () => {
      expect(
        translateFrame({
          type: 'control_response',
          response: { request_id: 'req_1', subtype: 'success' },
        }),
      ).toEqual([]);
    });

    it('drops a stream_event frame', () => {
      expect(
        translateFrame({ type: 'stream_event', session_id: 'sess_1', event: { type: 'x' } }),
      ).toEqual([]);
    });
  });

  describe('assistant frame', () => {
    it('maps a text block to a single agent.message', () => {
      const events = translateFrame({
        type: 'assistant',
        message: { content: [{ type: 'text', text: 'hello' }] },
      });
      expect(events).toEqual([
        { type: 'agent.message', content: [{ type: 'text', text: 'hello' }] },
      ]);
    });

    it('maps a tool_use block to a single agent.tool_use', () => {
      const events = translateFrame({
        type: 'assistant',
        message: {
          content: [{ type: 'tool_use', id: 'toolu_1', name: 'Bash', input: { command: 'ls' } }],
        },
      });
      expect(events).toEqual([
        { type: 'agent.tool_use', name: 'Bash', input: { command: 'ls' }, tool_use_id: 'toolu_1' },
      ]);
    });

    it('emits one event per block, in order, for mixed content', () => {
      const events = translateFrame({
        type: 'assistant',
        message: {
          content: [
            { type: 'text', text: 'running it' },
            { type: 'tool_use', id: 'toolu_2', name: 'Read', input: { path: '/tmp/x' } },
          ],
        },
      });
      expect(events.map((e) => e.type)).toEqual(['agent.message', 'agent.tool_use']);
    });

    it('maps a thinking block to a single agent.thinking event', () => {
      const events = translateFrame({
        type: 'assistant',
        message: { content: [{ type: 'thinking', thinking: 'hmm...' }] },
      });
      expect(events.map((e) => e.type)).toEqual(['agent.thinking']);
      expect((events[0] as { content: unknown[] }).content).toEqual([
        { type: 'thinking', thinking: 'hmm...' },
      ]);
    });

    it('skips unknown blocks and a non-array content', () => {
      expect(
        translateFrame({
          type: 'assistant',
          message: { content: [{ type: 'some_unknown_block', foo: '...' }] },
        }),
      ).toEqual([]);
      expect(translateFrame({ type: 'assistant', message: { content: 'not-an-array' } })).toEqual(
        [],
      );
    });

    it('defaults a tool_use with missing name/input/id to empty values', () => {
      const events = translateFrame({
        type: 'assistant',
        message: { content: [{ type: 'tool_use' }] },
      });
      expect(events).toEqual([{ type: 'agent.tool_use', name: '', input: {}, tool_use_id: '' }]);
    });

    it('attributes assistant usage to the managed subagent dispatched by the parent Agent call', () => {
      const state: FrameTranslationState = {
        subagentIdByType: new Map([['researcher', 'agt_researcher']]),
        subagentIdByParentToolUseId: new Map(),
        reportedAssistantUsage: false,
      };
      translateFrame(
        {
          type: 'assistant',
          message: {
            content: [
              {
                type: 'tool_use',
                id: 'toolu_agent',
                name: 'Agent',
                input: { subagent_type: 'researcher' },
              },
            ],
          },
        },
        state,
      );

      const events = translateFrame(
        {
          type: 'assistant',
          parent_tool_use_id: 'toolu_agent',
          message: {
            model: 'claude-subagent-model',
            content: [{ type: 'text', text: 'research complete' }],
            usage: { input_tokens: 12, output_tokens: 3 },
          },
        },
        state,
      );

      expect(events).toContainEqual({
        type: 'agent.usage',
        model: 'claude-subagent-model',
        subagent_id: 'agt_researcher',
        usage: { input_tokens: 12, output_tokens: 3 },
      });
      expect(
        translateFrame({ type: 'result', usage: { input_tokens: 12, output_tokens: 3 } }, state),
      ).toEqual([
        {
          type: 'session.status_idle',
          usage: { input_tokens: 12, output_tokens: 3 },
          total_cost_usd: 0,
          usage_already_reported: true,
        },
      ]);
    });
  });

  describe('streamed usage', () => {
    const newState = (): FrameTranslationState => ({
      subagentIdByType: new Map(),
      subagentIdByParentToolUseId: new Map([['tool_child', 'agt_child']]),
      reportedAssistantUsage: false,
    });
    const stream = (state: FrameTranslationState, event: unknown, parent?: string) =>
      translateFrame({ type: 'stream_event', event, parent_tool_use_id: parent }, state);
    const start = (
      state: FrameTranslationState,
      id: string,
      model: string,
      usage: unknown,
      parent?: string,
    ) => stream(state, { type: 'message_start', message: { id, model, usage } }, parent);

    it('reports final cumulative usage once across multiple assistant blocks and preserves cache TTLs', () => {
      const state = newState();
      const initial = {
        input_tokens: 7,
        output_tokens: 0,
        cache_read_input_tokens: 11,
        cache_creation_input_tokens: 20,
        cache_creation: { ephemeral_5m_input_tokens: 8, ephemeral_1h_input_tokens: 12 },
        speed: 'fast',
      };
      expect(start(state, 'msg_1', 'claude-first', initial)).toEqual([]);
      for (const text of ['first block', 'second block']) {
        expect(
          translateFrame(
            {
              type: 'assistant',
              message: {
                id: 'msg_1',
                model: 'claude-first',
                content: [{ type: 'text', text }],
                usage: initial,
              },
            },
            state,
          ),
        ).toEqual([{ type: 'agent.message', content: [{ type: 'text', text }] }]);
      }
      stream(state, { type: 'message_delta', usage: { output_tokens: 3 } });
      stream(state, {
        type: 'message_delta',
        usage: {
          input_tokens: null,
          output_tokens: 9,
          cache_read_input_tokens: null,
          cache_creation: { ephemeral_5m_input_tokens: null },
        },
      });
      expect(stream(state, { type: 'message_stop' })).toEqual([
        {
          type: 'agent.usage',
          model: 'claude-first',
          usage: { ...initial, output_tokens: 9 },
        },
      ]);
      expect(stream(state, { type: 'message_stop' })).toEqual([]);
      expect(
        translateFrame({ type: 'assistant', message: { id: 'msg_1', usage: initial } }, state),
      ).toEqual([]);
      expect(start(state, 'msg_1', 'claude-first', initial)).toEqual([]);
      expect(stream(state, { type: 'message_stop' })).toEqual([]);
    });

    it('keeps interleaved parent and child model usage separate and resets between turns', () => {
      const state = newState();
      start(state, 'msg_parent', 'claude-parent', { input_tokens: 2, output_tokens: 0 });
      start(
        state,
        'msg_child',
        'claude-child',
        { input_tokens: 5, output_tokens: 0 },
        'tool_child',
      );
      stream(state, { type: 'message_delta', usage: { output_tokens: 4 } }, 'tool_child');
      expect(stream(state, { type: 'message_stop' }, 'tool_child')).toEqual([
        {
          type: 'agent.usage',
          model: 'claude-child',
          subagent_id: 'agt_child',
          usage: { input_tokens: 5, output_tokens: 4 },
        },
      ]);
      stream(state, { type: 'message_delta', usage: { output_tokens: 6 } });
      expect(stream(state, { type: 'message_stop' })).toEqual([
        {
          type: 'agent.usage',
          model: 'claude-parent',
          usage: { input_tokens: 2, output_tokens: 6 },
        },
      ]);
      start(state, 'msg_next', 'claude-next', { input_tokens: 3, output_tokens: 0 });
      stream(state, { type: 'message_delta', usage: { output_tokens: 2 } });
      expect(stream(state, { type: 'message_stop' })).toEqual([
        {
          type: 'agent.usage',
          model: 'claude-next',
          usage: { input_tokens: 3, output_tokens: 2 },
        },
      ]);
      expect(
        translateFrame({ type: 'result', usage: { input_tokens: 10, output_tokens: 12 } }, state),
      ).toEqual([
        expect.objectContaining({ type: 'session.status_idle', usage_already_reported: true }),
      ]);
      expect(state.usageByMessage?.size).toBe(0);
      expect(state.activeUsageByParent?.size).toBe(0);
      start(state, 'msg_parent', 'claude-parent', { input_tokens: 1, output_tokens: 0 });
      expect(stream(state, { type: 'message_stop' })).toHaveLength(1);
    });

    it('retains observed usage on an interrupted stream and distinguishes zero from missing usage', () => {
      const state = newState();
      start(state, 'msg_partial', 'claude-model', { input_tokens: 8, output_tokens: 0 });
      stream(state, { type: 'message_delta', usage: { output_tokens: 2 } });
      expect(
        translateFrame({ type: 'result', is_error: true, result: 'interrupted' }, state),
      ).toEqual([
        {
          type: 'agent.usage',
          model: 'claude-model',
          usage: { input_tokens: 8, output_tokens: 2 },
        },
        { type: 'session.status_error', error: 'interrupted' },
      ]);
      start(state, 'msg_zero', 'claude-model', { input_tokens: 0, output_tokens: 0 });
      expect(stream(state, { type: 'message_stop' })).toHaveLength(1);
      translateFrame({ type: 'result' }, state);
      start(state, 'msg_unknown', 'claude-model', {});
      expect(stream(state, { type: 'message_stop' })).toEqual([]);
      expect(translateFrame({ type: 'result', usage: { output_tokens: 5 } }, state)).toEqual([
        { type: 'session.status_idle', usage: { output_tokens: 5 }, total_cost_usd: 0 },
      ]);
    });

    it('deduplicates assistant-only message blocks while retaining their fallback usage', () => {
      const state = newState();
      const frame = {
        type: 'assistant',
        message: {
          id: 'msg_fallback',
          model: 'claude-child',
          usage: { input_tokens: 1, output_tokens: 2 },
        },
        parent_tool_use_id: 'tool_child',
      };
      expect(translateFrame(frame, state)).toEqual([
        {
          type: 'agent.usage',
          model: 'claude-child',
          subagent_id: 'agt_child',
          usage: { input_tokens: 1, output_tokens: 2 },
        },
      ]);
      expect(translateFrame(frame, state)).toEqual([]);
    });
  });

  describe('custom_tool_use frame', () => {
    it('maps to a single agent.custom_tool_use event', () => {
      const events = translateFrame({
        type: 'custom_tool_use',
        id: 'evt_custom_1',
        name: 'lookup_ticket',
        input: { ticket_id: 'T-123' },
      });

      expect(events).toEqual([
        {
          type: 'agent.custom_tool_use',
          id: 'evt_custom_1',
          name: 'lookup_ticket',
          input: { ticket_id: 'T-123' },
        },
      ]);
    });
  });

  describe('user frame', () => {
    it('maps a tool_result block to a single agent.tool_result', () => {
      const events = translateFrame({
        type: 'user',
        message: {
          content: [
            { type: 'tool_result', tool_use_id: 'toolu_1', content: 'ok', is_error: false },
          ],
        },
      });
      expect(events).toEqual([
        { type: 'agent.tool_result', tool_use_id: 'toolu_1', content: 'ok', is_error: false },
      ]);
    });

    it('carries through is_error=true on a tool_result', () => {
      const events = translateFrame({
        type: 'user',
        message: {
          content: [
            { type: 'tool_result', tool_use_id: 'toolu_1', content: 'boom', is_error: true },
          ],
        },
      });
      expect(events).toEqual([
        { type: 'agent.tool_result', tool_use_id: 'toolu_1', content: 'boom', is_error: true },
      ]);
    });

    it('ignores non-tool_result blocks', () => {
      expect(
        translateFrame({ type: 'user', message: { content: [{ type: 'text', text: 'hi' }] } }),
      ).toEqual([]);
    });
  });

  describe('result frame', () => {
    it('maps is_error=false to a single session.status_idle', () => {
      const events = translateFrame({
        type: 'result',
        is_error: false,
        usage: { input_tokens: 7 },
        total_cost_usd: 0.02,
      });
      expect(events).toEqual([
        { type: 'session.status_idle', usage: { input_tokens: 7 }, total_cost_usd: 0.02 },
      ]);
    });

    it('preserves cache-creation TTL counters on session.status_idle', () => {
      const usage = {
        cache_creation_input_tokens: 30,
        cache_creation: {
          ephemeral_1h_input_tokens: 13,
          ephemeral_5m_input_tokens: 17,
        },
      };
      const events = translateFrame({ type: 'result', is_error: false, usage });
      expect(events).toEqual([{ type: 'session.status_idle', usage, total_cost_usd: 0 }]);
    });

    it('maps is_error=true to a single session.status_error carrying the result text', () => {
      const events = translateFrame({ type: 'result', is_error: true, result: 'it failed' });
      expect(events).toEqual([{ type: 'session.status_error', error: 'it failed' }]);
    });

    it('falls back to a generic message when an error result has no text', () => {
      const events = translateFrame({ type: 'result', is_error: true });
      expect(events).toEqual([{ type: 'session.status_error', error: 'error during execution' }]);
    });
  });

  describe('unknown / garbage input -> [] (never throws)', () => {
    it('drops an unrecognized type', () => {
      expect(translateFrame({ type: 'totally_new_frame' })).toEqual([]);
    });

    it('returns [] for null / non-object input', () => {
      expect(translateFrame(null)).toEqual([]);
      expect(translateFrame(undefined)).toEqual([]);
      expect(translateFrame('a string')).toEqual([]);
      expect(translateFrame(42)).toEqual([]);
    });

    it('returns [] for a frame with no type', () => {
      expect(translateFrame({ message: { content: [] } })).toEqual([]);
    });
  });
});
