// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the `pi` RPC protocol normalizer — one stdout line → a matched command
// response, Orca-native events, an extension-UI approval request, a turn end, or ignore.
//
// Pi's headless `--mode rpc` speaks newline-delimited JSON over stdio: the harness (the
// CLIENT) writes command objects (`{type:"prompt",...}`) to stdin and reads pi's responses
// (`{type:"response",command,success,...}`) + streamed session events (`AgentSessionEvent`
// objects: `agent_start`, `message_end`, `tool_execution_start`, `tool_execution_end`,
// `agent_end`, …) + extension-UI requests off stdout. This normalizer separates three concerns:
//   1. FRAMING — a `{type:"response"}` line is the reply to a command (routed by its `id`);
//      an event line carries a session event.
//   2. EVENT NORMALIZATION — pi's session events map to Orca-native Anthropic-shaped
//      {@link AgentEvent}s (the SAME shapes every other Orca harness emits; NO pi dialect):
//        · `message_end` (assistant) → an `agent.message` (text blocks) + a paired
//          `agent.tool_use` for each `toolCall` content block + an `agent.usage` when the
//          message carried a `usage` breakdown (internal token accounting);
//        · `tool_execution_end`     → an `agent.tool_result` paired by the tool-call id;
//        · `agent_end`              → a `turn_end` (the terminal turn boundary);
//        · `tool_execution_start` / streaming deltas / unknown types → `ignore` (the pre-exec gate
//          is the extension-UI request below, NOT this after-the-fact start event).
//   3. APPROVAL FRAMING — an `extension_ui_request` whose `title` carries the tagged approval
//      marker → a `ui_request` the harness routes to the gate (a genuine PRE-EXEC per-tool gate,
//      driven by the orca extension's `tool_call` hook via `ctx.ui.select`); a non-approval
//      extension-UI request → `ignore`.
//
// The normalizer performs no I/O.

import { describe, it, expect } from 'vitest';
import { PiProtocol, ORCA_TOOL_APPROVAL_TITLE_PREFIX } from '../../src/harness/pi/protocol.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';

/** Collect the `agent.*` events out of an `events`/`turn_end` normalized line. */
function eventsOf(line: { kind: string; events?: AgentEvent[] }): AgentEvent[] {
  return line.events ?? [];
}

describe('pi RPC protocol normalizer', () => {
  it('routes a command response by id (framing)', () => {
    const p = new PiProtocol();
    const line = p.map({ id: 'c1', type: 'response', command: 'prompt', success: true });
    expect(line.kind).toBe('response');
    if (line.kind === 'response') {
      expect(line.id).toBe('c1');
      expect(line.success).toBe(true);
    }
  });

  it('maps an assistant message_end to a text agent.message', () => {
    const p = new PiProtocol();
    const line = p.map({
      type: 'message_end',
      message: { role: 'assistant', content: [{ type: 'text', text: 'hello world' }] },
    });
    expect(line.kind).toBe('events');
    const events = eventsOf(line as { kind: string; events?: AgentEvent[] });
    const message = events.find((e) => e.kind === 'agent.message');
    expect(message?.payload).toEqual({ content: [{ type: 'text', text: 'hello world' }] });
  });

  it('emits a paired agent.tool_use for a toolCall content block in an assistant message', () => {
    const p = new PiProtocol();
    const line = p.map({
      type: 'message_end',
      message: {
        role: 'assistant',
        content: [
          { type: 'text', text: 'let me run it' },
          { type: 'toolCall', id: 'tc_1', name: 'bash', arguments: { command: 'ls' } },
        ],
      },
    });
    const events = eventsOf(line as { kind: string; events?: AgentEvent[] });
    const use = events.find((e) => e.kind === 'agent.tool_use');
    expect(use?.payload).toMatchObject({
      name: 'bash',
      input: { command: 'ls' },
      tool_use_id: 'tc_1',
    });
  });

  it('maps an assistant message_end usage to an agent.usage event (internal accounting shape)', () => {
    const p = new PiProtocol();
    const line = p.map({
      type: 'message_end',
      message: {
        role: 'assistant',
        content: [{ type: 'text', text: 'done' }],
        // Pi's camelCase usage breakdown (per assistant message): input/output/cacheRead/cacheWrite.
        usage: { input: 100, output: 50, cacheRead: 40, cacheWrite: 5, cost: { total: 0.001 } },
      },
    });
    const events = eventsOf(line as { kind: string; events?: AgentEvent[] });
    const usage = events.find((e) => e.kind === 'agent.usage');
    // Projected onto the SAME internal shape the claude + codex mappers emit; pi's flat
    // cache-creation total (cacheWrite) lands in the 5m bucket (1h = 0).
    expect(usage?.payload).toEqual({
      usage: {
        cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 5 },
        cache_read_input_tokens: 40,
        input_tokens: 100,
        output_tokens: 50,
      },
    });
  });

  it('emits agent.usage even when the assistant message_end carries only usage (no text/toolCall)', () => {
    const p = new PiProtocol();
    const line = p.map({
      type: 'message_end',
      message: { role: 'assistant', content: [], usage: { input: 7, output: 3 } },
    });
    expect(line.kind).toBe('events');
    const events = eventsOf(line as { kind: string; events?: AgentEvent[] });
    // No text/toolCall → only the usage event; missing cache counters coerce to 0.
    expect(events.map((e) => e.kind)).toEqual(['agent.usage']);
    expect(events[0]?.payload).toEqual({
      usage: {
        cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
        cache_read_input_tokens: 0,
        input_tokens: 7,
        output_tokens: 3,
      },
    });
  });

  it('does NOT emit agent.usage for a message_end without a usage breakdown', () => {
    const p = new PiProtocol();
    const line = p.map({
      type: 'message_end',
      message: { role: 'assistant', content: [{ type: 'text', text: 'hi' }] },
    });
    const events = eventsOf(line as { kind: string; events?: AgentEvent[] });
    expect(events.some((e) => e.kind === 'agent.usage')).toBe(false);
    expect(events.map((e) => e.kind)).toEqual(['agent.message']);
  });

  it('ignores a non-assistant message_end (e.g. a user echo)', () => {
    const p = new PiProtocol();
    const line = p.map({
      type: 'message_end',
      message: { role: 'user', content: [{ type: 'text', text: 'hi' }] },
    });
    expect(line.kind).toBe('ignore');
  });

  it('ignores tool_execution_start (informational — the gate is the extension_ui_request)', () => {
    const p = new PiProtocol();
    const line = p.map({
      type: 'tool_execution_start',
      toolCallId: 'tc_9',
      toolName: 'bash',
      args: { command: 'rm -rf /' },
    });
    // The pre-exec gate is the extension `tool_call` hook (surfaced as an extension_ui_request),
    // NOT this after-the-fact start event — mapping it to an approval would gate AFTER exec begins.
    expect(line.kind).toBe('ignore');
  });

  it('surfaces a tagged extension_ui_request as a ui_request carrying the tool id + name', () => {
    const p = new PiProtocol();
    const line = p.map({
      type: 'extension_ui_request',
      id: 'ui_7',
      method: 'select',
      title: `${ORCA_TOOL_APPROVAL_TITLE_PREFIX}\ttc_9\tbash`,
      options: ['Allow', 'Block'],
    });
    expect(line.kind).toBe('ui_request');
    if (line.kind === 'ui_request') {
      expect(line.request.requestId).toBe('ui_7');
      expect(line.request.toolCallId).toBe('tc_9');
      expect(line.request.toolName).toBe('bash');
    }
  });

  it('ignores a non-approval extension_ui_request (title lacks the tagged marker)', () => {
    const p = new PiProtocol();
    const line = p.map({
      type: 'extension_ui_request',
      id: 'ui_8',
      method: 'select',
      title: 'Pick a session',
      options: ['a', 'b'],
    });
    expect(line.kind).toBe('ignore');
  });

  it('maps tool_execution_end to a paired agent.tool_result (error flag honored)', () => {
    const p = new PiProtocol();
    const ok = p.map({
      type: 'tool_execution_end',
      toolCallId: 'tc_1',
      toolName: 'bash',
      result: { content: [{ type: 'text', text: 'done' }], details: {} },
      isError: false,
    });
    let events = eventsOf(ok as { kind: string; events?: AgentEvent[] });
    let result = events.find((e) => e.kind === 'agent.tool_result');
    expect(result?.payload).toMatchObject({ tool_use_id: 'tc_1', is_error: false });

    const bad = p.map({
      type: 'tool_execution_end',
      toolCallId: 'tc_2',
      toolName: 'bash',
      result: { content: [{ type: 'text', text: 'boom' }], details: {} },
      isError: true,
    });
    events = eventsOf(bad as { kind: string; events?: AgentEvent[] });
    result = events.find((e) => e.kind === 'agent.tool_result');
    expect((result?.payload as { is_error?: boolean }).is_error).toBe(true);
  });

  it('treats agent_end as the terminal turn boundary', () => {
    const p = new PiProtocol();
    const line = p.map({ type: 'agent_end', messages: [] });
    expect(line.kind).toBe('turn_end');
  });

  it('ignores housekeeping / unknown event types (agent_start, deltas)', () => {
    const p = new PiProtocol();
    expect(p.map({ type: 'agent_start' }).kind).toBe('ignore');
    expect(p.map({ type: 'turn_start' }).kind).toBe('ignore');
    expect(p.map({ type: 'message_update', message: {}, assistantMessageEvent: {} }).kind).toBe(
      'ignore',
    );
    expect(
      p.map({ type: 'extension_ui_request', id: 'x', method: 'notify', message: 'hi' }).kind,
    ).toBe('ignore');
  });

  it('ignores malformed lines (non-object / no type)', () => {
    const p = new PiProtocol();
    expect(p.map(null).kind).toBe('ignore');
    expect(p.map(42).kind).toBe('ignore');
    expect(p.map({}).kind).toBe('ignore');
  });
});
