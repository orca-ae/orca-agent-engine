// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the `codex` app-server protocol normalizer.
//
// Codex's `app-server` speaks JSON-RPC over stdio: the harness (the CLIENT) writes
// request/response objects to stdin and reads the app-server's responses + notifications +
// server→client approval requests off stdout, one JSON object per line. Two concerns live
// in the normalizer, proven here:
//
//   1. JSON-RPC FRAMING — a line with `id` + (`result`|`error`) and NO `method` is the
//      RESPONSE to an earlier request (routed by id); a line with `method` is a
//      notification (an event) or, when it ALSO has an `id`, a server→client request (an
//      approval the client must answer).
//   2. EVENT NORMALIZATION — codex `item/*` + `turn/*` notifications map to Orca-native
//      Anthropic-shaped AgentEvents (NO codex dialect):
//        · `item/completed` agentMessage → `agent.message` `{content:[{type:'text',...}]}`
//        · `item/completed` commandExecution/tool → `agent.tool_use` + `agent.tool_result`
//        · `turn/completed`  → an `agent.usage` event (when usage present) + a `turn_end`
//        · `turn/failed` / `error` → a `turn_end` (the harness settles the turn)
//        · a server→client approval request → an `approval_request` (routed to the gate)
//
// The normalizer is PURE (no I/O).

import { describe, it, expect } from 'vitest';
import { CodexProtocol } from '../../src/harness/codex/protocol.js';
import type { AgentEvent } from '../../src/harness/agent-harness.js';

function eventsOf(result: { kind: string; events?: AgentEvent[] }): AgentEvent[] {
  return result.events ?? [];
}

describe('codex protocol — JSON-RPC framing', () => {
  it('routes a response (id + result, no method) to a matched pending request', () => {
    const proto = new CodexProtocol();
    // A response with id=1 + result matches the pending id=1 registered on send.
    proto.expectResponse(1);
    const line = proto.map({ id: 1, result: { thread: { id: 'thr_1' } } });
    expect(line.kind).toBe('response');
    if (line.kind === 'response') {
      expect(line.id).toBe(1);
      expect(line.result).toEqual({ thread: { id: 'thr_1' } });
      expect(line.error).toBeUndefined();
    }
  });

  it('surfaces a JSON-RPC error response with its error payload', () => {
    const proto = new CodexProtocol();
    proto.expectResponse(2);
    const line = proto.map({ id: 2, error: { code: -32000, message: 'boom' } });
    expect(line.kind).toBe('response');
    if (line.kind === 'response') {
      expect(line.error).toMatchObject({ message: 'boom' });
    }
  });

  it('treats a frame with a method + id as a server→client request, not a response', () => {
    const proto = new CodexProtocol();
    // No pending request for id 7 — and the presence of `method` marks it a request anyway.
    const line = proto.map({
      id: 7,
      method: 'execCommandApproval',
      params: { command: ['rm', '-rf', '/tmp/x'], cwd: '/work', callId: 'call_1' },
    });
    expect(line.kind).toBe('approval_request');
  });
});

describe('codex protocol — event normalization (Anthropic-native)', () => {
  it('maps an agentMessage item/completed to an agent.message text block', () => {
    const proto = new CodexProtocol();
    const line = proto.map({
      method: 'item/completed',
      params: {
        turnId: 't1',
        item: { id: 'i1', type: 'agentMessage', text: 'hello world', phase: 'final_answer' },
      },
    });
    expect(line.kind).toBe('events');
    const msg = eventsOf(line as { kind: string; events?: AgentEvent[] }).find(
      (e) => e.kind === 'agent.message',
    );
    expect(msg?.payload).toEqual({ content: [{ type: 'text', text: 'hello world' }] });
  });

  it('maps a commandExecution item/completed to a paired agent.tool_use + agent.tool_result', () => {
    const proto = new CodexProtocol();
    const line = proto.map({
      method: 'item/completed',
      params: {
        turnId: 't1',
        item: {
          id: 'cmd_1',
          type: 'commandExecution',
          command: 'echo hi',
          aggregatedOutput: 'hi\n',
          exitCode: 0,
          status: 'completed',
        },
      },
    });
    expect(line.kind).toBe('events');
    const events = eventsOf(line as { kind: string; events?: AgentEvent[] });
    const use = events.find((e) => e.kind === 'agent.tool_use');
    const result = events.find((e) => e.kind === 'agent.tool_result');
    // The tool_use carries the codex built-in name + the command input, keyed by item id.
    expect(use?.id).toBe('cmd_1');
    expect(use?.payload).toMatchObject({ tool_use_id: 'cmd_1', input: { command: 'echo hi' } });
    // The tool_result pairs back by the same id and is not an error (exit 0).
    expect(result?.id).toBe('cmd_1');
    expect(result?.payload).toMatchObject({ tool_use_id: 'cmd_1', is_error: false });
  });

  it('marks a non-zero-exit commandExecution result as an error', () => {
    const proto = new CodexProtocol();
    const line = proto.map({
      method: 'item/completed',
      params: {
        turnId: 't1',
        item: {
          id: 'cmd_2',
          type: 'commandExecution',
          command: 'false',
          aggregatedOutput: '',
          exitCode: 1,
          status: 'failed',
        },
      },
    });
    const result = eventsOf(line as { kind: string; events?: AgentEvent[] }).find(
      (e) => e.kind === 'agent.tool_result',
    );
    expect((result?.payload as { is_error?: boolean }).is_error).toBe(true);
  });

  it('maps an mcp tool call item/completed to a paired tool_use + tool_result', () => {
    const proto = new CodexProtocol();
    const line = proto.map({
      method: 'item/completed',
      params: {
        turnId: 't1',
        item: {
          id: 'mcp_1',
          type: 'mcpToolCall',
          server: 'orca',
          tool: 'bash',
          arguments: { command: 'ls' },
          status: 'completed',
          result: { content: [{ type: 'text', text: 'file.txt' }] },
        },
      },
    });
    const events = eventsOf(line as { kind: string; events?: AgentEvent[] });
    const use = events.find((e) => e.kind === 'agent.tool_use');
    const result = events.find((e) => e.kind === 'agent.tool_result');
    // MCP calls surface under the `mcp__orca__bash` identity (matches the rest of Orca).
    expect(use?.payload).toMatchObject({ name: 'mcp__orca__bash', input: { command: 'ls' } });
    expect(result).toBeDefined();
    expect((result?.payload as { is_error?: boolean }).is_error).toBe(false);
  });

  it('turn/completed ends the turn and carries usage as agent.usage', () => {
    const proto = new CodexProtocol();
    // A token-usage notification lands first; turn/completed then flushes it as the turn usage.
    proto.map({
      method: 'thread/tokenUsage/updated',
      params: { usage: { last: { input_tokens: 10, output_tokens: 4 } } },
    });
    const line = proto.map({ method: 'turn/completed', params: { turn: { id: 't1' } } });
    expect(line.kind).toBe('turn_end');
    const usage = eventsOf(line as { kind: string; events?: AgentEvent[] }).find(
      (e) => e.kind === 'agent.usage',
    );
    expect(usage?.payload).toMatchObject({
      usage: { input_tokens: 10, output_tokens: 4 },
    });
  });

  it('turn/failed ends the turn (so the harness settles submit)', () => {
    const proto = new CodexProtocol();
    const line = proto.map({
      method: 'turn/failed',
      params: { turn: { id: 't1' }, message: 'model overloaded' },
    });
    expect(line.kind).toBe('turn_end');
  });

  it('surfaces a server→client approval request with the command + call id', () => {
    const proto = new CodexProtocol();
    const line = proto.map({
      id: 12,
      method: 'item/commandExecution/requestApproval',
      params: { command: ['rm', '-rf', 'x'], cwd: '/w', callId: 'call_9', reason: 'destructive' },
    });
    expect(line.kind).toBe('approval_request');
    if (line.kind === 'approval_request') {
      expect(line.request.requestId).toBe(12);
      // A displayable command + the codex approval method are captured for the gate.
      expect(line.request.command).toBe('rm -rf x');
      expect(line.request.method).toBe('item/commandExecution/requestApproval');
    }
  });

  it('ignores housekeeping notifications (deltas, unknown methods)', () => {
    const proto = new CodexProtocol();
    // Streaming deltas are not persisted as discrete events (the completed item carries the
    // whole text) — they normalize to `ignore` so the reader drops them.
    expect(
      proto.map({
        method: 'item/agentMessage/delta',
        params: { turnId: 't1', itemId: 'i1', delta: 'he' },
      }).kind,
    ).toBe('ignore');
    expect(proto.map({ method: 'some/unknown/notification', params: {} }).kind).toBe('ignore');
    // A blank / non-object line is ignored too.
    expect(proto.map(null).kind).toBe('ignore');
    expect(proto.map(42).kind).toBe('ignore');
  });
});

describe('codex protocol — approval decision replies', () => {
  it('builds the codex decision result for allow/deny per approval method', () => {
    const proto = new CodexProtocol();
    // legacy execCommandApproval / applyPatchApproval → approved|denied
    expect(proto.approvalResult('execCommandApproval', 'allow')).toEqual({ decision: 'approved' });
    expect(proto.approvalResult('execCommandApproval', 'deny')).toEqual({ decision: 'abort' });
    expect(proto.approvalResult('applyPatchApproval', 'allow')).toEqual({ decision: 'approved' });
    // current item/* approvals → accept|decline
    expect(proto.approvalResult('item/commandExecution/requestApproval', 'allow')).toEqual({
      decision: 'accept',
    });
    expect(proto.approvalResult('item/commandExecution/requestApproval', 'deny')).toEqual({
      decision: 'decline',
    });
    expect(proto.approvalResult('item/fileChange/requestApproval', 'allow')).toEqual({
      decision: 'accept',
    });
  });
});
