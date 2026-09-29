// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the `cursor` stream-json normalizer — one stdout line → Orca-native
// {@link AgentEvent}s + control frames.
//
// The headless `cursor-agent` binary emits one JSON object per stdout line ("stream-json").
// The normalizer maps cursor's NATIVE event vocabulary to the SAME Anthropic-shaped
// {@link AgentEvent}s every other Orca harness emits (NO cursor dialect), and lifts
// the control frames the headless protocol adds (a tool-permission request the harness routes
// to the approval gate; a withdrawal that releases a parked gate; the terminal turn marker).
//
// The cursor event shapes this suite pins are grounded in the observable cursor-agent stream:
//   · `system`/`init`                 → a `system` event (MCP connection status);
//   · `assistant` (text block)        → `agent.message` `{ content:[{type:'text',text}] }`;
//   · `assistant` (thinking block) /
//     a bare `thinking` event         → `agent.message` `{ content:[{type:'thinking',...}] }`;
//   · `tool_call` started + completed → a paired `agent.tool_use` + `agent.tool_result`
//     (keyed by the call id), incl. the `mcp` custom-tool envelope unwrapped to the real
//     `mcp__orca__<tool>` name; a completed call whose status is an error → an error result;
//   · `result`                        → a `turn_end` carrying any `agent.usage`;
//   · a tool-permission request        → a `permission_request` (routed to the gate);
//   · a permission withdrawal          → a `permission_cancel`;
//   · unknown / housekeeping           → `ignore`.

import { describe, it, expect } from 'vitest';
import {
  CursorStreamNormalizer,
  type NormalizedLine,
} from '../../src/harness/cursor/stream-json.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';

/** Map one raw object through a fresh normalizer (most frames are stateless). */
function mapOne(line: unknown): NormalizedLine {
  return new CursorStreamNormalizer().map(line);
}

/** The `events` array of a `NormalizedLine`, or `[]` for any other kind. */
function eventsOf(line: NormalizedLine): AgentEvent[] {
  return line.kind === 'events' || line.kind === 'turn_end' ? line.events : [];
}

describe('cursor stream-json normalizer', () => {
  it('ignores non-objects and unknown frames', () => {
    expect(mapOne(null).kind).toBe('ignore');
    expect(mapOne('nope').kind).toBe('ignore');
    expect(mapOne({ type: 'user' }).kind).toBe('ignore');
    expect(mapOne({ type: 'unrecognized-thing' }).kind).toBe('ignore');
  });

  it('maps a system/init frame to a `system` event', () => {
    const out = mapOne({ type: 'system', subtype: 'init', session_id: 'cur-1' });
    expect(out.kind).toBe('events');
    const ev = eventsOf(out)[0];
    expect(ev?.kind).toBe('system');
  });

  it('maps an assistant text block to a text agent.message', () => {
    const out = mapOne({
      type: 'assistant',
      message: { content: [{ type: 'text', text: 'hello there' }] },
    });
    expect(out.kind).toBe('events');
    expect(eventsOf(out)).toEqual([
      { kind: 'agent.message', payload: { content: [{ type: 'text', text: 'hello there' }] } },
    ]);
  });

  it('maps an assistant thinking block to a thinking agent.message', () => {
    const out = mapOne({
      type: 'assistant',
      message: { content: [{ type: 'thinking', thinking: 'let me reason', signature: 'sig' }] },
    });
    const ev = eventsOf(out)[0];
    expect(ev?.kind).toBe('agent.message');
    expect(ev?.payload).toEqual({
      content: [{ type: 'thinking', thinking: 'let me reason', signature: 'sig' }],
    });
  });

  it('maps a bare `thinking` event (text form) to a thinking agent.message', () => {
    const out = mapOne({ type: 'thinking', text: 'pondering' });
    const ev = eventsOf(out)[0];
    expect(ev?.kind).toBe('agent.message');
    expect(ev?.payload).toEqual({ content: [{ type: 'thinking', thinking: 'pondering' }] });
  });

  it('maps a completed tool_call to a paired tool_use + tool_result (keyed by call id)', () => {
    const out = mapOne({
      type: 'tool_call',
      subtype: 'completed',
      callId: 'call_42',
      name: 'mcp__orca__bash',
      args: { command: 'echo hi' },
      status: 'completed',
      result: { content: [{ type: 'text', text: 'hi' }] },
    });
    const events = eventsOf(out);
    const use = events.find((e) => e.kind === 'agent.tool_use');
    const result = events.find((e) => e.kind === 'agent.tool_result');
    expect(use?.id).toBe('call_42');
    expect(use?.payload).toEqual({
      name: 'mcp__orca__bash',
      input: { command: 'echo hi' },
      tool_use_id: 'call_42',
    });
    expect(result?.id).toBe('call_42');
    expect(result?.payload).toMatchObject({ tool_use_id: 'call_42', is_error: false });
  });

  it('unwraps the `mcp` custom-tool envelope to the real mcp__<server>__<tool> name', () => {
    // Cursor surfaces a host custom tool under an envelope: name == "mcp",
    // args == { providerIdentifier, toolName, args }.
    const out = mapOne({
      type: 'tool_call',
      subtype: 'completed',
      callId: 'call_7',
      name: 'mcp',
      args: {
        providerIdentifier: 'orca',
        toolName: 'bash',
        args: { command: 'ls' },
      },
      status: 'completed',
      result: { content: [{ type: 'text', text: 'file.txt' }] },
    });
    const use = eventsOf(out).find((e) => e.kind === 'agent.tool_use');
    expect(use?.payload).toMatchObject({ name: 'mcp__orca__bash', input: { command: 'ls' } });
  });

  it('marks a failed tool_call result as an error', () => {
    const out = mapOne({
      type: 'tool_call',
      subtype: 'completed',
      callId: 'call_9',
      name: 'mcp__orca__bash',
      args: { command: 'false' },
      status: 'error',
      result: { content: [{ type: 'text', text: 'boom' }] },
    });
    const result = eventsOf(out).find((e) => e.kind === 'agent.tool_result');
    expect((result?.payload as { is_error?: boolean }).is_error).toBe(true);
  });

  it('emits ONLY the tool_use for a started tool_call (result comes on completion)', () => {
    const out = mapOne({
      type: 'tool_call',
      subtype: 'started',
      callId: 'call_1',
      name: 'mcp__orca__bash',
      args: { command: 'sleep 1' },
    });
    const events = eventsOf(out);
    expect(events.some((e) => e.kind === 'agent.tool_use')).toBe(true);
    expect(events.some((e) => e.kind === 'agent.tool_result')).toBe(false);
  });

  it('maps a result frame to a turn_end carrying agent.usage', () => {
    const out = mapOne({
      type: 'result',
      subtype: 'success',
      usage: { input_tokens: 11, output_tokens: 5 },
    });
    expect(out.kind).toBe('turn_end');
    const usage = eventsOf(out).find((e) => e.kind === 'agent.usage');
    expect(usage?.payload).toEqual({
      usage: {
        cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
        cache_read_input_tokens: 0,
        input_tokens: 11,
        output_tokens: 5,
      },
    });
  });

  it('maps a result frame with no usage to a turn_end with no events', () => {
    const out = mapOne({ type: 'result', subtype: 'success' });
    expect(out.kind).toBe('turn_end');
    expect(eventsOf(out)).toEqual([]);
  });

  it('surfaces a tool-permission request as a permission_request', () => {
    const out = mapOne({
      type: 'control_request',
      request_id: 'req_1',
      request: {
        subtype: 'can_use_tool',
        tool_name: 'mcp__orca__bash',
        input: { command: 'rm -rf /' },
        tool_use_id: 'call_5',
      },
    });
    expect(out.kind).toBe('permission_request');
    if (out.kind === 'permission_request') {
      expect(out.request).toEqual({
        requestId: 'req_1',
        toolName: 'mcp__orca__bash',
        input: { command: 'rm -rf /' },
        toolUseId: 'call_5',
      });
    }
  });

  it('surfaces a permission withdrawal as a permission_cancel', () => {
    const out = mapOne({ type: 'control_cancel_request', request_id: 'req_1' });
    expect(out.kind).toBe('permission_cancel');
    if (out.kind === 'permission_cancel') {
      expect(out.cancel).toEqual({ requestId: 'req_1' });
    }
  });

  it('ignores a control_request that is not a can_use_tool subtype', () => {
    const out = mapOne({
      type: 'control_request',
      request_id: 'req_2',
      request: { subtype: 'interrupt' },
    });
    expect(out.kind).toBe('ignore');
  });

  it('ignores housekeeping control frames', () => {
    expect(mapOne({ type: 'control_response' }).kind).toBe('ignore');
    expect(mapOne({ type: 'keep_alive' }).kind).toBe('ignore');
  });
});
