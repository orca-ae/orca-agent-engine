// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the generic custom-provider STDOUT NORMALIZER.
//
// The `custom` provider maps a CLI's stdout to Orca-native Anthropic-shaped {@link AgentEvent}s
// per the operator's declarative stdout mapping. Two modes:
//   - `text`  — every non-blank stdout line becomes an `agent.message` text event (the simplest
//     CLI: it just prints assistant text), and a per-turn sentinel / EOF ends the turn;
//   - `jsonLine` — each stdout line is a JSON object matched against the spec's field rules,
//     mapping to `agent.message` / `agent.tool_use` / `agent.tool_result` / a turn-end / an
//     approval request per the matched rule.
//
// This suite pins that both modes emit the SAME Anthropic-native shapes every other provider
// emits (no dialect). The normalizer is a small stateful object (per turn); it performs no I/O.

import { describe, it, expect } from 'vitest';
import { parseCustomAgentSpec } from '../../src/harness/custom/spec.js';
import { CustomStreamNormalizer } from '../../src/harness/custom/normalizer.js';

describe('custom stdout normalizer — text mode', () => {
  it('maps every non-blank stdout line to an Anthropic-native agent.message', () => {
    const spec = parseCustomAgentSpec({ command: 'x' });
    const norm = new CustomStreamNormalizer(spec);
    const out = norm.map('hello world');
    expect(out).toEqual({
      kind: 'events',
      events: [
        { kind: 'agent.message', payload: { content: [{ type: 'text', text: 'hello world' }] } },
      ],
    });
  });

  it('ignores a blank line', () => {
    const spec = parseCustomAgentSpec({ command: 'x' });
    const norm = new CustomStreamNormalizer(spec);
    expect(norm.map('   ')).toEqual({ kind: 'ignore' });
  });

  it('treats a configured end sentinel line as the turn boundary', () => {
    const spec = parseCustomAgentSpec({
      command: 'x',
      stdout: { mode: 'text', end_sentinel: '<<END>>' },
    });
    const norm = new CustomStreamNormalizer(spec);
    expect(norm.map('some text').kind).toBe('events');
    expect(norm.map('<<END>>')).toEqual({ kind: 'turn_end', events: [] });
  });
});

describe('custom stdout normalizer — jsonLine mode', () => {
  const jsonSpec = parseCustomAgentSpec({
    command: 'x',
    stdout: {
      mode: 'jsonLine',
      text: { type_equals: 'assistant', text_field: 'text' },
      tool_use: {
        type_equals: 'tool_call',
        name_field: 'tool',
        input_field: 'args',
        id_field: 'id',
      },
      tool_result: {
        type_equals: 'tool_result',
        id_field: 'id',
        content_field: 'output',
        error_field: 'is_error',
      },
      usage: { type_equals: 'usage', input_tokens_field: 'in', output_tokens_field: 'out' },
      turn_completed: { type_equals: 'done' },
      approval_request: {
        type_equals: 'approval',
        id_field: 'request_id',
        name_field: 'tool',
        input_field: 'args',
      },
    },
    // The `approval_request` rule and an `approvals` block are one loop; the parser requires both.
    // The normalizer under test reads only the stdout rules, but the spec must parse.
    approvals: {
      response: { type: 'approval_response', request_id: '{requestId}', decision: '{decision}' },
    },
  });

  it('maps a matched text frame to an agent.message', () => {
    const norm = new CustomStreamNormalizer(jsonSpec);
    const out = norm.map({ type: 'assistant', text: 'hi there' });
    expect(out).toEqual({
      kind: 'events',
      events: [
        { kind: 'agent.message', payload: { content: [{ type: 'text', text: 'hi there' }] } },
      ],
    });
  });

  it('maps a matched tool_call frame to an agent.tool_use (name/input/tool_use_id)', () => {
    const norm = new CustomStreamNormalizer(jsonSpec);
    const out = norm.map({
      type: 'tool_call',
      tool: 'bash',
      args: { command: 'ls' },
      id: 'call_1',
    });
    expect(out).toEqual({
      kind: 'events',
      events: [
        {
          kind: 'agent.tool_use',
          id: 'call_1',
          payload: { name: 'bash', input: { command: 'ls' }, tool_use_id: 'call_1' },
        },
      ],
    });
  });

  it('maps a matched tool_result frame to an agent.tool_result (tool_use_id/content/is_error)', () => {
    const norm = new CustomStreamNormalizer(jsonSpec);
    const out = norm.map({ type: 'tool_result', id: 'call_1', output: 'done', is_error: false });
    expect(out.kind).toBe('events');
    if (out.kind !== 'events') {
      throw new Error('expected events');
    }
    expect(out.events[0]).toMatchObject({
      kind: 'agent.tool_result',
      payload: { tool_use_id: 'call_1', is_error: false },
    });
    // The content is normalized to the Anthropic content-block array shape.
    expect((out.events[0]?.payload as { content: unknown }).content).toEqual([
      { type: 'text', text: 'done' },
    ]);
  });

  it('maps a matched usage frame to an internal agent.usage event', () => {
    const norm = new CustomStreamNormalizer(jsonSpec);
    const out = norm.map({ type: 'usage', in: 12, out: 5 });
    expect(out.kind).toBe('events');
    if (out.kind !== 'events') {
      throw new Error('expected events');
    }
    expect(out.events[0]?.kind).toBe('agent.usage');
    expect(
      (out.events[0]?.payload as { usage: { input_tokens: number; output_tokens: number } }).usage,
    ).toMatchObject({
      input_tokens: 12,
      output_tokens: 5,
    });
  });

  it('maps the turn-completed frame to a turn_end', () => {
    const norm = new CustomStreamNormalizer(jsonSpec);
    expect(norm.map({ type: 'done' })).toEqual({ kind: 'turn_end', events: [] });
  });

  it('surfaces a matched approval frame as an approval_request the harness routes to the gate', () => {
    const norm = new CustomStreamNormalizer(jsonSpec);
    const out = norm.map({
      type: 'approval',
      request_id: 'req_9',
      tool: 'bash',
      args: { command: 'rm -rf /' },
    });
    expect(out.kind).toBe('approval_request');
    if (out.kind !== 'approval_request') {
      throw new Error('expected approval_request');
    }
    expect(out.request).toEqual({
      requestId: 'req_9',
      toolName: 'bash',
      input: { command: 'rm -rf /' },
    });
  });

  it('ignores an unmatched / non-object frame', () => {
    const norm = new CustomStreamNormalizer(jsonSpec);
    expect(norm.map({ type: 'heartbeat' })).toEqual({ kind: 'ignore' });
    expect(norm.map('not json object here')).toEqual({ kind: 'ignore' });
  });
});
