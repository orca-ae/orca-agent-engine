// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the claude-code stream-json normalizer.
//
// The headless `claude` binary emits one JSON object per stdout line ("stream-json").
// Most lines are the SAME SDK message shapes the in-process claude provider already
// normalizes (assistant text/thinking/tool_use, user tool_result, system init,
// result usage, partial-thinking stream events) — so the normalizer maps them into
// the identical Anthropic-native {@link AgentEvent}s, reusing the ONE shared mapper.
//
// On TOP of that the normalizer recognizes the stream-json control frames the headless
// protocol adds that an in-process SDK consumer never sees on its message stream:
//
//   - a `control_request` of subtype `can_use_tool` — the CLI asking whether a tool
//     call may proceed. The normalizer surfaces it as a PERMISSION REQUEST carrying
//     the request_id (the reply's routing key), the tool name + input, and the
//     tool_use_id, so the harness can route it to the uniform transcript approval and
//     answer over stdin.
//   - a `control_cancel_request` — the CLI WITHDRAWING an earlier request. The
//     normalizer surfaces it as a PERMISSION CANCEL carrying the retracted request_id,
//     so the harness releases the matching parked gate as a deny.
//   - the `result` frame ends the TURN (headless: the CLI stays alive for the next
//     turn), surfaced as a turn-end signal so the harness settles `submit`.
//
// Housekeeping frames (`control_response`, `keep_alive`, an unknown/blank line) are
// ignored.

import { describe, it, expect } from 'vitest';
import {
  ClaudeCodeStreamNormalizer,
  type NormalizedLine,
} from '../../src/harness/claude-code/stream-json.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';

/** Map a line and assert it produced a stream of agent events; return them. */
function eventsOf(result: NormalizedLine): AgentEvent[] {
  expect(result.kind).toBe('events');
  return result.kind === 'events' ? result.events : [];
}

describe('ClaudeCodeStreamNormalizer — stream-json line → Orca events + control frames', () => {
  it('maps an assistant text block to an agent.message', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const events = eventsOf(
      n.map({ type: 'assistant', message: { content: [{ type: 'text', text: 'hello' }] } }),
    );
    expect(events).toEqual([
      { kind: 'agent.message', payload: { content: [{ type: 'text', text: 'hello' }] } },
    ]);
  });

  it('maps an assistant tool_use block to agent.tool_use keyed by tool_use_id', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const events = eventsOf(
      n.map({
        type: 'assistant',
        message: {
          content: [
            { type: 'tool_use', id: 'toolu_1', name: 'mcp__orca__bash', input: { command: 'ls' } },
          ],
        },
      }),
    );
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      kind: 'agent.tool_use',
      id: 'toolu_1',
      payload: { name: 'mcp__orca__bash', input: { command: 'ls' }, tool_use_id: 'toolu_1' },
    });
  });

  it('maps a user tool_result block to agent.tool_result paired by tool_use_id', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const events = eventsOf(
      n.map({
        type: 'user',
        message: {
          content: [
            { type: 'tool_result', tool_use_id: 'toolu_1', content: 'done', is_error: false },
          ],
        },
      }),
    );
    expect(events).toEqual([
      {
        kind: 'agent.tool_result',
        id: 'toolu_1',
        payload: { tool_use_id: 'toolu_1', content: 'done', is_error: false },
      },
    ]);
  });

  it('maps a partial thinking stream_event to a partial thinking agent.message', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const events = eventsOf(
      n.map({
        type: 'stream_event',
        event: { type: 'content_block_delta', delta: { type: 'thinking_delta', thinking: 'hmm' } },
      }),
    );
    expect(events).toEqual([
      {
        kind: 'agent.message',
        payload: { content: [{ type: 'thinking', thinking: 'hmm' }], partial: true },
      },
    ]);
  });

  it('maps the system init frame to a system event', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const frame = {
      type: 'system',
      subtype: 'init',
      mcp_servers: [{ name: 'orca', status: 'connected' }],
    };
    const events = eventsOf(n.map(frame));
    expect(events).toEqual([{ kind: 'system', payload: frame }]);
  });

  it('maps the result frame usage to agent.usage AND flags the turn end', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const result = n.map({
      type: 'result',
      subtype: 'success',
      usage: { input_tokens: 10, output_tokens: 5 },
    });
    // A result frame is BOTH: it carries usage AND it ends the turn.
    expect(result.kind).toBe('turn_end');
    if (result.kind === 'turn_end') {
      expect(result.events).toEqual([
        {
          kind: 'agent.usage',
          payload: {
            usage: {
              cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
              cache_read_input_tokens: 0,
              input_tokens: 10,
              output_tokens: 5,
            },
          },
        },
      ]);
    }
  });

  it('flags turn end even when the result frame carried no usage', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const result = n.map({ type: 'result', subtype: 'success' });
    expect(result.kind).toBe('turn_end');
    if (result.kind === 'turn_end') {
      expect(result.events).toEqual([]);
    }
  });

  it('recognizes a can_use_tool control_request as a permission request', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const result = n.map({
      type: 'control_request',
      request_id: 'req_7',
      request: {
        subtype: 'can_use_tool',
        tool_name: 'mcp__orca__bash',
        input: { command: 'rm -rf /tmp/x' },
        tool_use_id: 'toolu_9',
      },
    });
    expect(result.kind).toBe('permission_request');
    if (result.kind === 'permission_request') {
      expect(result.request).toEqual({
        requestId: 'req_7',
        toolName: 'mcp__orca__bash',
        input: { command: 'rm -rf /tmp/x' },
        toolUseId: 'toolu_9',
      });
    }
  });

  it('defaults a missing permission input to an empty object and a missing tool_use_id to empty', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const result = n.map({
      type: 'control_request',
      request_id: 'req_8',
      request: { subtype: 'can_use_tool', tool_name: 'mcp__orca__read' },
    });
    expect(result.kind).toBe('permission_request');
    if (result.kind === 'permission_request') {
      expect(result.request.input).toEqual({});
      expect(result.request.toolUseId).toBe('');
      expect(result.request.toolName).toBe('mcp__orca__read');
    }
  });

  it('ignores a non-permission control_request (e.g. an unknown subtype)', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const result = n.map({
      type: 'control_request',
      request_id: 'req_9',
      request: { subtype: 'mcp_message', message: {} },
    });
    expect(result.kind).toBe('ignore');
  });

  it('recognizes a control_cancel_request as a permission cancel keyed by the retracted request_id', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const result = n.map({ type: 'control_cancel_request', request_id: 'req_7' });
    expect(result.kind).toBe('permission_cancel');
    if (result.kind === 'permission_cancel') {
      expect(result.cancel).toEqual({ requestId: 'req_7' });
    }
  });

  it('ignores a control_cancel_request with no string request_id (nothing to release)', () => {
    const n = new ClaudeCodeStreamNormalizer();
    expect(n.map({ type: 'control_cancel_request' }).kind).toBe('ignore');
    expect(n.map({ type: 'control_cancel_request', request_id: '' }).kind).toBe('ignore');
    expect(n.map({ type: 'control_cancel_request', request_id: 7 }).kind).toBe('ignore');
  });

  it('ignores control_response and keep_alive housekeeping', () => {
    const n = new ClaudeCodeStreamNormalizer();
    expect(
      n.map({ type: 'control_response', response: { subtype: 'success', request_id: 'req_1' } })
        .kind,
    ).toBe('ignore');
    expect(n.map({ type: 'keep_alive' }).kind).toBe('ignore');
  });

  it('ignores an unknown message type and a non-object line', () => {
    const n = new ClaudeCodeStreamNormalizer();
    expect(n.map({ type: 'mystery' }).kind).toBe('ignore');
    expect(n.map(null).kind).toBe('ignore');
    expect(n.map('not json').kind).toBe('ignore');
  });

  it('carries call_id pairing across messages within one turn (tool_use then tool_result)', () => {
    const n = new ClaudeCodeStreamNormalizer();
    const use = eventsOf(
      n.map({
        type: 'assistant',
        message: { content: [{ type: 'tool_use', id: 'toolu_z', name: 'x', input: {} }] },
      }),
    );
    const res = eventsOf(
      n.map({
        type: 'user',
        message: { content: [{ type: 'tool_result', tool_use_id: 'toolu_z', content: 'ok' }] },
      }),
    );
    expect(use[0]?.id).toBe('toolu_z');
    expect(res[0]?.id).toBe('toolu_z');
  });
});
