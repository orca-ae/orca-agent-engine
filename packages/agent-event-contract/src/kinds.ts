// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Canonical agent-content events persisted for a turn. */
export const AgentEventKind = {
  message: 'agent.message',
  thinking: 'agent.thinking',
  toolUse: 'agent.tool_use',
  toolResult: 'agent.tool_result',
  mcpToolUse: 'agent.mcp_tool_use',
  mcpToolResult: 'agent.mcp_tool_result',
  customToolUse: 'agent.custom_tool_use',
  threadContextCompacted: 'agent.thread_context_compacted',
} as const;

/** Canonical messages exchanged between session threads. */
export const AgentThreadEventKind = {
  messageSent: 'agent.thread_message_sent',
  messageReceived: 'agent.thread_message_received',
} as const;

/** Canonical session lifecycle events. */
export const SessionEventKind = {
  statusRunning: 'session.status_running',
  statusIdle: 'session.status_idle',
  statusRescheduled: 'session.status_rescheduled',
  statusTerminated: 'session.status_terminated',
  error: 'session.error',
} as const;

/** Canonical lifecycle events for a session thread. */
export const SessionThreadEventKind = {
  created: 'session.thread_created',
  statusRunning: 'session.thread_status_running',
  statusIdle: 'session.thread_status_idle',
  statusRescheduled: 'session.thread_status_rescheduled',
  statusTerminated: 'session.thread_status_terminated',
} as const;

/** Canonical observability span events. */
export const SpanEventKind = {
  modelRequestStart: 'span.model_request_start',
  modelRequestEnd: 'span.model_request_end',
  outcomeEvaluationStart: 'span.outcome_evaluation_start',
  outcomeEvaluationOngoing: 'span.outcome_evaluation_ongoing',
  outcomeEvaluationEnd: 'span.outcome_evaluation_end',
} as const;

/** Persisted internal transcript markers, never producer-facing canonical events. */
export const InternalTranscriptEventKind = {
  userEventProcessed: 'session.user_event_processed',
  userEventCompleted: 'session.user_event_completed',
} as const;

/** Runtime-only signals, never persisted canonical events. */
export const AgentRuntimeSignalKind = {
  usage: 'agent.usage',
} as const;

export type CanonicalAgentEventKind =
  | (typeof AgentEventKind)[keyof typeof AgentEventKind]
  | (typeof AgentThreadEventKind)[keyof typeof AgentThreadEventKind]
  | (typeof SessionEventKind)[keyof typeof SessionEventKind]
  | (typeof SessionThreadEventKind)[keyof typeof SessionThreadEventKind]
  | (typeof SpanEventKind)[keyof typeof SpanEventKind];

/**
 * Every canonical persisted producer kind. Runtime-only usage and internal
 * acceptance markers deliberately do not appear here.
 */
export const CANONICAL_AGENT_EVENT_KINDS: readonly CanonicalAgentEventKind[] = Object.freeze([
  AgentEventKind.message,
  AgentEventKind.thinking,
  AgentEventKind.toolUse,
  AgentEventKind.toolResult,
  AgentEventKind.mcpToolUse,
  AgentEventKind.mcpToolResult,
  AgentEventKind.customToolUse,
  AgentEventKind.threadContextCompacted,
  AgentThreadEventKind.messageSent,
  AgentThreadEventKind.messageReceived,
  SessionEventKind.statusRunning,
  SessionEventKind.statusIdle,
  SessionEventKind.statusRescheduled,
  SessionEventKind.statusTerminated,
  SessionEventKind.error,
  SessionThreadEventKind.created,
  SessionThreadEventKind.statusRunning,
  SessionThreadEventKind.statusIdle,
  SessionThreadEventKind.statusRescheduled,
  SessionThreadEventKind.statusTerminated,
  SpanEventKind.modelRequestStart,
  SpanEventKind.modelRequestEnd,
  SpanEventKind.outcomeEvaluationStart,
  SpanEventKind.outcomeEvaluationOngoing,
  SpanEventKind.outcomeEvaluationEnd,
]);
