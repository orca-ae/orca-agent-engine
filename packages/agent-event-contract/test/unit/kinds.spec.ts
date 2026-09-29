// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  AgentEventKind,
  AgentRuntimeSignalKind,
  AgentThreadEventKind,
  CANONICAL_AGENT_EVENT_KINDS,
  InternalTranscriptEventKind,
  SessionEventKind,
  SessionThreadEventKind,
  SpanEventKind,
  type AgentEvent,
  type CanonicalAgentEventKind,
} from '../../src/index.js';

describe('canonical event kinds', () => {
  it('groups every persisted producer kind by event family', () => {
    expect(AgentEventKind).toEqual({
      message: 'agent.message',
      thinking: 'agent.thinking',
      toolUse: 'agent.tool_use',
      toolResult: 'agent.tool_result',
      mcpToolUse: 'agent.mcp_tool_use',
      mcpToolResult: 'agent.mcp_tool_result',
      customToolUse: 'agent.custom_tool_use',
      threadContextCompacted: 'agent.thread_context_compacted',
    });
    expect(AgentThreadEventKind).toEqual({
      messageSent: 'agent.thread_message_sent',
      messageReceived: 'agent.thread_message_received',
    });
    expect(SessionEventKind).toEqual({
      statusRunning: 'session.status_running',
      statusIdle: 'session.status_idle',
      statusRescheduled: 'session.status_rescheduled',
      statusTerminated: 'session.status_terminated',
      error: 'session.error',
    });
    expect(SessionThreadEventKind).toEqual({
      created: 'session.thread_created',
      statusRunning: 'session.thread_status_running',
      statusIdle: 'session.thread_status_idle',
      statusRescheduled: 'session.thread_status_rescheduled',
      statusTerminated: 'session.thread_status_terminated',
    });
    expect(SpanEventKind).toEqual({
      modelRequestStart: 'span.model_request_start',
      modelRequestEnd: 'span.model_request_end',
      outcomeEvaluationStart: 'span.outcome_evaluation_start',
      outcomeEvaluationOngoing: 'span.outcome_evaluation_ongoing',
      outcomeEvaluationEnd: 'span.outcome_evaluation_end',
    });
    expect(InternalTranscriptEventKind).toEqual({
      userEventProcessed: 'session.user_event_processed',
      userEventCompleted: 'session.user_event_completed',
    });
  });

  it('lists each persisted producer kind once and excludes internal-only signals', () => {
    const expected: readonly CanonicalAgentEventKind[] = [
      ...Object.values(AgentEventKind),
      ...Object.values(AgentThreadEventKind),
      ...Object.values(SessionEventKind),
      ...Object.values(SessionThreadEventKind),
      ...Object.values(SpanEventKind),
    ];

    expect(CANONICAL_AGENT_EVENT_KINDS).toEqual(expected);
    expect(new Set(CANONICAL_AGENT_EVENT_KINDS)).toHaveLength(CANONICAL_AGENT_EVENT_KINDS.length);
    expect(CANONICAL_AGENT_EVENT_KINDS).not.toContain(AgentRuntimeSignalKind.usage);
    expect(CANONICAL_AGENT_EVENT_KINDS).not.toContain(
      InternalTranscriptEventKind.userEventProcessed,
    );
    expect(CANONICAL_AGENT_EVENT_KINDS).not.toContain(
      InternalTranscriptEventKind.userEventCompleted,
    );
  });

  it('requires a stable id and subpath for generic canonical events', () => {
    const event: AgentEvent<typeof AgentEventKind.message, { text: string }> = {
      id: 'evt_message',
      subpath: 'subagents/thread_1',
      kind: AgentEventKind.message,
      payload: { text: 'done' },
    };

    expect(event).toEqual({
      id: 'evt_message',
      subpath: 'subagents/thread_1',
      kind: 'agent.message',
      payload: { text: 'done' },
    });
  });
});
