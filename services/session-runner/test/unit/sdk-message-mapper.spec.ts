// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the SDK-message → Anthropic-native AgentEvent mapper.
//
// The mapper is the pure heart of the claude provider's streaming: it turns each
// Claude Agent SDK message into zero-or-more Orca-native `AgentEvent`s, in the
// canonical vocabulary the rest of Orca already speaks (`agent.message` with a
// content-block array, `agent.tool_use` / `agent.tool_result` keyed by
// `tool_use_id`). It also surfaces extended-thinking as Anthropic-native `thinking`
// content blocks — both the streamed `thinking_delta` (live) and the settled
// `thinking` block (non-streaming) — and pairs every tool call with its result by
// the SDK's `tool_use_id`. Driven here against scripted SDK message shapes (the
// exact `@anthropic-ai/claude-agent-sdk` wire shapes), no network, no harness.

import { describe, it, expect } from 'vitest';
import { SdkMessageMapper } from '../../src/harness/claude/sdk-message-mapper.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';

/** Map a sequence of SDK messages through one mapper, collecting all emitted events. */
function mapAll(messages: unknown[]): AgentEvent[] {
  const mapper = new SdkMessageMapper();
  const out: AgentEvent[] = [];
  for (const m of messages) {
    out.push(...mapper.map(m as never));
  }
  return out;
}

describe('SdkMessageMapper — assistant content blocks', () => {
  it('maps an assistant text block to agent.message with a text content block', () => {
    const events = mapAll([
      {
        type: 'assistant',
        message: { role: 'assistant', content: [{ type: 'text', text: 'hello' }] },
      },
    ]);
    expect(events).toEqual([
      { kind: 'agent.message', payload: { content: [{ type: 'text', text: 'hello' }] } },
    ]);
  });

  it('maps an assistant thinking block to agent.message with a thinking content block (non-streaming)', () => {
    const events = mapAll([
      {
        type: 'assistant',
        message: {
          role: 'assistant',
          content: [{ type: 'thinking', thinking: 'let me reason', signature: 'sig-1' }],
        },
      },
    ]);
    // Anthropic-native: thinking rides INSIDE agent.message as a `thinking` block —
    // no separate reasoning-chunk event kind.
    expect(events).toEqual([
      {
        kind: 'agent.message',
        payload: { content: [{ type: 'thinking', thinking: 'let me reason', signature: 'sig-1' }] },
      },
    ]);
  });

  it('maps an assistant tool_use block to agent.tool_use keyed by tool_use_id (id preserved)', () => {
    const events = mapAll([
      {
        type: 'assistant',
        message: {
          role: 'assistant',
          content: [{ type: 'tool_use', id: 'toolu_1', name: 'Bash', input: { command: 'ls' } }],
        },
      },
    ]);
    expect(events).toEqual([
      {
        kind: 'agent.tool_use',
        id: 'toolu_1',
        payload: { name: 'Bash', input: { command: 'ls' }, tool_use_id: 'toolu_1' },
      },
    ]);
  });

  it('emits one event per content block, in order (thinking → text → tool_use)', () => {
    const events = mapAll([
      {
        type: 'assistant',
        message: {
          role: 'assistant',
          content: [
            { type: 'thinking', thinking: 'plan', signature: 's' },
            { type: 'text', text: 'on it' },
            { type: 'tool_use', id: 'toolu_2', name: 'Read', input: { path: '/x' } },
          ],
        },
      },
    ]);
    expect(events.map((e) => e.kind)).toEqual(['agent.message', 'agent.message', 'agent.tool_use']);
    expect(events[0]?.payload).toEqual({
      content: [{ type: 'thinking', thinking: 'plan', signature: 's' }],
    });
    expect(events[1]?.payload).toEqual({ content: [{ type: 'text', text: 'on it' }] });
    expect(events[2]?.id).toBe('toolu_2');
  });

  it('defaults a tool_use with missing input to an empty object and a missing name to empty string', () => {
    const events = mapAll([
      {
        type: 'assistant',
        message: { role: 'assistant', content: [{ type: 'tool_use', id: 'toolu_3' }] },
      },
    ]);
    expect(events).toEqual([
      {
        kind: 'agent.tool_use',
        id: 'toolu_3',
        payload: { name: '', input: {}, tool_use_id: 'toolu_3' },
      },
    ]);
  });

  it('skips a tool_use block with no string id (malformed) rather than bucketing it under ""', () => {
    const events = mapAll([
      {
        type: 'assistant',
        message: { role: 'assistant', content: [{ type: 'tool_use', name: 'Bash', input: {} }] },
      },
    ]);
    expect(events).toEqual([]);
  });

  it('ignores an assistant message whose content is a plain string (no block array)', () => {
    // The lean provider previously forwarded the whole SDK message; the mapper only
    // emits for a structured content-block array. A string-content assistant message
    // carries no blocks to map → no events (the result frame still completes the turn).
    const events = mapAll([
      { type: 'assistant', message: { role: 'assistant', content: 'hi there' } },
    ]);
    expect(events).toEqual([]);
  });
});

describe('SdkMessageMapper — streamed thinking deltas (partial messages)', () => {
  it('maps a content_block_delta thinking_delta to a partial agent.message thinking block', () => {
    const events = mapAll([
      {
        type: 'stream_event',
        event: {
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'thinking_delta', thinking: 'step ' },
        },
      },
      {
        type: 'stream_event',
        event: {
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'thinking_delta', thinking: 'by step' },
        },
      },
    ]);
    expect(events).toEqual([
      {
        kind: 'agent.message',
        payload: { content: [{ type: 'thinking', thinking: 'step ' }], partial: true },
      },
      {
        kind: 'agent.message',
        payload: { content: [{ type: 'thinking', thinking: 'by step' }], partial: true },
      },
    ]);
  });

  it('does not emit for a content_block_delta text_delta (text rides the settled assistant message)', () => {
    // Text is emitted from the settled assistant message (so the persisted agent.message
    // is whole); only thinking is streamed live as a partial, mirroring the reasoning path.
    const events = mapAll([
      {
        type: 'stream_event',
        event: { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'hi' } },
      },
    ]);
    expect(events).toEqual([]);
  });

  it('ignores a thinking_delta with an empty / non-string thinking field', () => {
    const events = mapAll([
      {
        type: 'stream_event',
        event: {
          type: 'content_block_delta',
          index: 0,
          delta: { type: 'thinking_delta', thinking: '' },
        },
      },
      {
        type: 'stream_event',
        event: { type: 'content_block_delta', index: 0, delta: { type: 'thinking_delta' } },
      },
    ]);
    expect(events).toEqual([]);
  });

  it('ignores non-delta stream events (content_block_start, message_start, message_delta)', () => {
    const events = mapAll([
      {
        type: 'stream_event',
        event: { type: 'content_block_start', index: 0, content_block: { type: 'thinking' } },
      },
      {
        type: 'stream_event',
        event: { type: 'message_start', message: { usage: { input_tokens: 1 } } },
      },
      {
        type: 'stream_event',
        event: { type: 'message_delta', delta: { stop_reason: 'end_turn' } },
      },
    ]);
    expect(events).toEqual([]);
  });
});

describe('SdkMessageMapper — tool results (user messages) paired by call_id', () => {
  it('maps a user tool_result block to agent.tool_result keyed by tool_use_id', () => {
    const events = mapAll([
      {
        type: 'user',
        message: {
          role: 'user',
          content: [
            { type: 'tool_result', tool_use_id: 'toolu_1', content: 'ok', is_error: false },
          ],
        },
      },
    ]);
    expect(events).toEqual([
      {
        kind: 'agent.tool_result',
        id: 'toolu_1',
        payload: { tool_use_id: 'toolu_1', content: 'ok', is_error: false },
      },
    ]);
  });

  it('marks a tool_result with is_error true as an error result', () => {
    const events = mapAll([
      {
        type: 'user',
        message: {
          role: 'user',
          content: [
            { type: 'tool_result', tool_use_id: 'toolu_2', content: 'boom', is_error: true },
          ],
        },
      },
    ]);
    expect(events[0]?.payload).toEqual({ tool_use_id: 'toolu_2', content: 'boom', is_error: true });
  });

  it('pairs a tool_use with its tool_result by call_id across messages (request → result)', () => {
    const events = mapAll([
      {
        type: 'assistant',
        message: {
          role: 'assistant',
          content: [
            { type: 'tool_use', id: 'toolu_pair', name: 'Bash', input: { command: 'pwd' } },
          ],
        },
      },
      {
        type: 'user',
        message: {
          role: 'user',
          content: [
            { type: 'tool_result', tool_use_id: 'toolu_pair', content: '/home', is_error: false },
          ],
        },
      },
    ]);
    // The use event and the result event share the same id — downstream pairs by it.
    const use = events.find((e) => e.kind === 'agent.tool_use');
    const result = events.find((e) => e.kind === 'agent.tool_result');
    expect(use?.id).toBe('toolu_pair');
    expect(result?.id).toBe('toolu_pair');
    expect((result?.payload as { tool_use_id: string }).tool_use_id).toBe('toolu_pair');
  });

  it('ignores a tool_result with no string tool_use_id (cannot be paired)', () => {
    const events = mapAll([
      {
        type: 'user',
        message: { role: 'user', content: [{ type: 'tool_result', content: 'orphan' }] },
      },
    ]);
    expect(events).toEqual([]);
  });

  it('ignores a user message that carries no tool_result blocks (a plain user echo)', () => {
    const events = mapAll([
      { type: 'user', message: { role: 'user', content: [{ type: 'text', text: 'echo' }] } },
    ]);
    expect(events).toEqual([]);
  });
});

describe('SdkMessageMapper — system + result frames', () => {
  it('maps the system init frame to a system event', () => {
    const events = mapAll([{ type: 'system', subtype: 'init', mcp_servers: [] }]);
    expect(events).toHaveLength(1);
    expect(events[0]?.kind).toBe('system');
  });

  it('maps a result frame usage to an agent.usage event', () => {
    const events = mapAll([
      {
        type: 'result',
        subtype: 'success',
        usage: { input_tokens: 10, output_tokens: 4, cache_read_input_tokens: 1 },
      },
    ]);
    expect(events).toEqual([
      {
        kind: 'agent.usage',
        payload: {
          usage: {
            cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
            cache_read_input_tokens: 1,
            input_tokens: 10,
            output_tokens: 4,
          },
        },
      },
    ]);
  });

  it('forwards the SDK usage cache_creation TTL breakdown faithfully (1h + 5m)', () => {
    // The SDK result usage carries a STRUCTURED cache_creation object (the same
    // {ephemeral_1h_input_tokens, ephemeral_5m_input_tokens} the Messages API returns).
    // Each TTL bucket is forwarded as-is — a 1h cache-creation token is NOT collapsed
    // into the 5m bucket.
    const events = mapAll([
      {
        type: 'result',
        subtype: 'success',
        usage: {
          input_tokens: 10,
          output_tokens: 4,
          cache_read_input_tokens: 1,
          cache_creation: { ephemeral_1h_input_tokens: 7, ephemeral_5m_input_tokens: 3 },
          // A flat total may ALSO be present; the structured object wins when present.
          cache_creation_input_tokens: 10,
        },
      },
    ]);
    expect(events).toEqual([
      {
        kind: 'agent.usage',
        payload: {
          usage: {
            cache_creation: { ephemeral_1h_input_tokens: 7, ephemeral_5m_input_tokens: 3 },
            cache_read_input_tokens: 1,
            input_tokens: 10,
            output_tokens: 4,
          },
        },
      },
    ]);
  });

  it('falls back to the flat cache_creation_input_tokens in the 5m bucket when no breakdown is present', () => {
    // An older / partial usage that omits the structured cache_creation object: the
    // flat total maps to the 5m bucket (1h = 0), preserving the prior behavior.
    const events = mapAll([
      {
        type: 'result',
        subtype: 'success',
        usage: { input_tokens: 10, output_tokens: 4, cache_creation_input_tokens: 5 },
      },
    ]);
    expect(events).toEqual([
      {
        kind: 'agent.usage',
        payload: {
          usage: {
            cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 5 },
            cache_read_input_tokens: 0,
            input_tokens: 10,
            output_tokens: 4,
          },
        },
      },
    ]);
  });

  it('emits nothing for a result frame with no usage', () => {
    const events = mapAll([{ type: 'result', subtype: 'success' }]);
    expect(events).toEqual([]);
  });

  it('maps a system mirror_error frame to agent.error carrying the dropped batch key', () => {
    // `SDKMirrorErrorMessage`: SessionStore.append() failed for a transcript-mirror
    // batch after bounded retry and the batch was DROPPED. The SDK emits it precisely so
    // consumers are not silent on data loss — dropping it here would restore the silence.
    const events = mapAll([
      {
        type: 'system',
        subtype: 'mirror_error',
        error: 'append timed out',
        key: { projectKey: 'ws_1', sessionId: 'ses_1', subpath: 'subagents/a' },
        uuid: 'u-1',
        session_id: 'ses_1',
      },
    ]);
    expect(events).toEqual([
      {
        kind: 'agent.error',
        payload: {
          message: 'transcript mirror append failed (batch dropped): append timed out',
          key: { projectKey: 'ws_1', sessionId: 'ses_1', subpath: 'subagents/a' },
        },
      },
    ]);
  });

  it('maps a failed result (error_max_turns) to agent.error before the usage event', () => {
    // `SDKResultError`: every consumer treats a result frame as the turn's END, so a
    // max-turns / budget / execution failure must carry its reason out with it instead
    // of reaching the client as an ordinary clean turn end.
    const events = mapAll([
      {
        type: 'result',
        subtype: 'error_max_turns',
        is_error: true,
        errors: ['max turns reached', 'aborted'],
        usage: { input_tokens: 3, output_tokens: 1 },
      },
    ]);
    expect(events).toEqual([
      {
        kind: 'agent.error',
        // TERMINAL: a result frame IS the turn's end, so this error is the turn's own last
        // word — the runner loop reads the flag and does not bury it under a generic one.
        terminal: true,
        payload: { message: 'max turns reached; aborted', subtype: 'error_max_turns' },
      },
      {
        kind: 'agent.usage',
        payload: {
          usage: {
            cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
            cache_read_input_tokens: 0,
            input_tokens: 3,
            output_tokens: 1,
          },
        },
      },
    ]);
  });

  it('falls back to the subtype when a failed result carries no errors, and honors is_error alone', () => {
    expect(mapAll([{ type: 'result', subtype: 'error_during_execution', errors: [] }])).toEqual([
      {
        kind: 'agent.error',
        terminal: true,
        payload: { message: 'error_during_execution', subtype: 'error_during_execution' },
      },
    ]);
    // A `success` subtype that still flags is_error is a failure too — but `success` is not
    // a REASON, so the fallback must not use it as the message. It used to: a failed turn
    // was described to the operator as `"success"`, and because this error is TERMINAL that
    // string also disarmed the runner loop's own terminal fault, so the turn's real cause
    // (a stream cut short) went unreported behind a message asserting the opposite.
    expect(mapAll([{ type: 'result', subtype: 'success', is_error: true }])).toEqual([
      {
        kind: 'agent.error',
        terminal: true,
        payload: { message: 'result reported is_error', subtype: 'success' },
      },
    ]);
  });

  it('emits no error for a clean success result (or a subtype-less malformed frame)', () => {
    expect(mapAll([{ type: 'result', subtype: 'success', is_error: false }])).toEqual([]);
    // No subtype at all is MALFORMED, not failed — inventing an error for garbage input
    // would make every unknown result frame look like a turn failure.
    expect(mapAll([{ type: 'result' }])).toEqual([]);
  });

  it('drops unhandled SDK message variants (status, retry, auth) without throwing', () => {
    const events = mapAll([
      { type: 'system', subtype: 'api_retry', attempt: 1 },
      { type: 'stream_event', event: { type: 'message_stop' } },
      { type: 'totally_unknown' },
      null,
      42,
    ]);
    expect(events).toEqual([]);
  });
});
