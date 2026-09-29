// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Session-thread event kinds + thread/subpath addressing for the Anthropic
 * thread-model multiagent.
 *
 * ONE session runs MULTIPLE threads. The PRIMARY thread IS the session-level
 * event stream (`parent_thread_id = null`, transcript `subpath = ''`). Each roster
 * subagent the coordinator delegates to runs in its OWN session thread — a
 * context-isolated event stream keyed by a transcript SUBPATH
 * (`subagents/<session_thread_id>`), mirroring the Kafka `subpath` header the data
 * model already uses to differentiate subagents on the per-session topic.
 *
 * Two families of thread events surface on the PRIMARY thread stream (so a client
 * following the session sees the whole multiagent choreography without opening
 * every child thread):
 *
 *   - `session.thread_*` — thread lifecycle: created / running / idle / terminated;
 *   - `agent.thread_message_*` — cross-thread delegation traffic: a message received
 *     by / sent to another session thread.
 *
 * These are ordinary PUBLIC transcript event kinds (they do not start with the
 * internal `harness.` prefix), so the existing session-events index + SSE relay
 * carry them on the primary stream with no special-casing. This module is the
 * single source of truth for the kind strings + the subpath addressing, so the
 * registry routes, the harness/runner, and tests agree on one vocabulary.
 */

import type { Event } from '@orca/transcript-store';

/** The transcript subpath of the PRIMARY (session-level) thread. */
export const PRIMARY_THREAD_SUBPATH = '';

/** Subpath prefix for a child (subagent) session thread's event stream. */
export const SUBAGENT_SUBPATH_PREFIX = 'subagents/';

// === Primary-thread event kinds (public) ===

/** A new session thread was created (a subagent thread was spun up). */
export const SESSION_THREAD_CREATED = 'session.thread_created';
/** A session thread began running a turn. */
export const SESSION_THREAD_STATUS_RUNNING = 'session.thread_status_running';
/** A session thread went idle (finished a turn); carries a `stop_reason`. */
export const SESSION_THREAD_STATUS_IDLE = 'session.thread_status_idle';
/** A session thread was terminated (interrupted, archived, or run to completion). */
export const SESSION_THREAD_STATUS_TERMINATED = 'session.thread_status_terminated';
/** A session thread received a delegation message from another thread. */
export const AGENT_THREAD_MESSAGE_RECEIVED = 'agent.thread_message_received';
/** A session thread sent a delegation message to another thread. */
export const AGENT_THREAD_MESSAGE_SENT = 'agent.thread_message_sent';

/** All primary-thread event kinds the multiagent choreography surfaces. */
export const PRIMARY_THREAD_EVENT_KINDS = [
  SESSION_THREAD_CREATED,
  SESSION_THREAD_STATUS_RUNNING,
  SESSION_THREAD_STATUS_IDLE,
  SESSION_THREAD_STATUS_TERMINATED,
  AGENT_THREAD_MESSAGE_RECEIVED,
  AGENT_THREAD_MESSAGE_SENT,
] as const;

export type PrimaryThreadEventKind = (typeof PRIMARY_THREAD_EVENT_KINDS)[number];

const PRIMARY_THREAD_EVENT_KIND_SET = new Set<string>(PRIMARY_THREAD_EVENT_KINDS);

/** Whether an event kind is one of the primary-thread multiagent events. */
export function isPrimaryThreadEventKind(kind: string): kind is PrimaryThreadEventKind {
  return PRIMARY_THREAD_EVENT_KIND_SET.has(kind);
}

/**
 * The transcript SUBPATH a thread's events live under.
 *
 * The PRIMARY thread (`parentThreadId === null`) is the session-level stream at
 * the empty subpath; a child thread's events live under
 * `subagents/<sessionThreadId>` — the same subpath convention the data model uses
 * to differentiate subagents on the per-session topic.
 */
export function threadSubpath(sessionThreadId: string, parentThreadId: string | null): string {
  return parentThreadId === null
    ? PRIMARY_THREAD_SUBPATH
    : `${SUBAGENT_SUBPATH_PREFIX}${sessionThreadId}`;
}

/**
 * The transcript SUBPATH a cross-thread client reply
 * (`user.tool_confirmation` / `user.custom_tool_result`) must be routed to.
 *
 * A subagent's `always_ask` / custom-tool request is cross-posted onto the PRIMARY
 * thread (so the client, which follows the session stream, sees it) tagged with the
 * originating `session_thread_id`. When the client posts its reply back with that
 * `session_thread_id`, the reply must be delivered to the ORIGINATING thread's
 * stream — NOT the primary — so the parked tool call in that thread is unblocked.
 * This resolves the subpath: the session's own primary thread id maps back to the
 * empty subpath; any other thread id maps to its `subagents/<id>` subpath.
 *
 * `primaryThreadId` is the session's primary thread row id (the one with
 * `parent_thread_id = null`); a reply carrying it (or no id at all) targets the
 * primary stream.
 */
export function replyThreadSubpath(
  sessionThreadId: string | undefined,
  primaryThreadId: string,
): string {
  if (sessionThreadId === undefined || sessionThreadId === primaryThreadId) {
    return PRIMARY_THREAD_SUBPATH;
  }
  return `${SUBAGENT_SUBPATH_PREFIX}${sessionThreadId}`;
}

/**
 * Extract the `session_thread_id` a client reply targets, from a parsed event
 * body. Returns `undefined` when the field is absent or not a string — such a
 * reply targets the primary thread (single-agent behavior is unchanged).
 */
export function readReplyThreadId(body: Record<string, unknown> | undefined): string | undefined {
  const value = body?.['session_thread_id'];
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

/**
 * The client reply kinds that carry a `session_thread_id` for cross-thread
 * routing. A `user.tool_confirmation` (allow/deny a gated tool) or a
 * `user.custom_tool_result` (a custom tool's result) posted against a coordinator
 * session may target a specific subagent thread.
 */
export const USER_TOOL_CONFIRMATION_KIND = 'user.tool_confirmation';
export const USER_CUSTOM_TOOL_RESULT_KIND = 'user.custom_tool_result';

const CROSS_THREAD_REPLY_KINDS = new Set<string>([
  USER_TOOL_CONFIRMATION_KIND,
  USER_CUSTOM_TOOL_RESULT_KIND,
]);

/** Whether an event kind is a client reply that may carry a `session_thread_id`. */
export function isCrossThreadReplyKind(kind: string): boolean {
  return CROSS_THREAD_REPLY_KINDS.has(kind);
}

/**
 * The `agent_name` a `session.thread_created` event announces for a thread, read
 * from a transcript event's parsed payload. Used by the read-model projector /
 * list route to name a thread it learns about from the stream. Returns
 * `undefined` when absent.
 */
export function readThreadAgentName(event: Pick<Event, 'payload'>): string | undefined {
  try {
    const parsed = JSON.parse(Buffer.from(event.payload).toString('utf8')) as unknown;
    if (parsed && typeof parsed === 'object') {
      const name = (parsed as Record<string, unknown>)['agent_name'];
      if (typeof name === 'string') return name;
    }
  } catch {
    // Non-JSON payload — no agent name to read.
  }
  return undefined;
}
