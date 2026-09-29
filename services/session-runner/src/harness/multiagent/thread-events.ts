// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The runner-side vocabulary for the Anthropic thread-model multiagent choreography
// — the {@link AgentEvent} builders a coordinator emits so the registry projects the
// session's threads.
//
// ONE session runs MULTIPLE threads. The PRIMARY thread IS the session-level event
// stream (empty transcript subpath); each roster subagent the coordinator delegates
// to runs in its OWN thread whose events live under `subagents/<session_thread_id>`.
// Two families of events surface on the PRIMARY thread so a client following the
// session sees the whole choreography without opening every child thread:
//
//   - `session.thread_*` — thread lifecycle: created / running / idle / terminated;
//   - `agent.thread_message_*` — cross-thread delegation traffic (received / sent).
//
// These are ordinary PUBLIC transcript kinds (no `harness.` prefix), so the registry
// bridge persists them and its read-model projector
// (`registry-service-ts/src/events/session-events-index.ts`) upserts the
// `session_threads` row from their payloads. This module is the RUNNER's single
// source of truth for those kind strings + payload shapes; the kind strings are
// re-declared here (not imported) because the runner is a separate process that does
// not link `@orca/agent-event-contract` — they mirror that package's
// `SessionThreadEventKind` / `AgentThreadEventKind` EXACTLY (the thread-events spec
// pins that equality), the same pattern the runner uses for the `user.*` / `agent.*`
// kinds it already speaks.
//
// Each builder returns a fully-formed {@link AgentEvent} — kind + payload + subpath —
// that the coordinator hands straight to the loop's event stream. The loop encodes
// `{ ...payload, type: kind, id?, subpath? }` (see `encodeAgentEventLine`), which is
// exactly the `AgentEventLine` shape the bridge reads. Lifecycle + message events
// carry the PRIMARY subpath (so the projector, which reads the session stream, sees
// them); a subagent's own turn events carry `subagents/<id>` (see
// {@link subagentThreadSubpath}).

import type { AgentEvent } from '../agent-harness.js';

// === Primary-thread event kinds (public) — mirror @orca/agent-event-contract kinds.ts ===

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

/** The transcript subpath of the PRIMARY (session-level) thread. */
export const PRIMARY_THREAD_SUBPATH = '';

/** Subpath prefix for a child (subagent) session thread's event stream. */
export const SUBAGENT_SUBPATH_PREFIX = 'subagents/';

/**
 * The transcript SUBPATH a child (subagent) thread's OWN turn events live under —
 * `subagents/<session_thread_id>`, the same convention the registry data model uses
 * to differentiate subagents on the per-session topic. A subagent harness's agent
 * events (its `agent.message`, tool calls, `agent.turn_completed`) ride this so they
 * land on the child thread's stream, not the primary.
 */
export function subagentThreadSubpath(sessionThreadId: string): string {
  return `${SUBAGENT_SUBPATH_PREFIX}${sessionThreadId}`;
}

/** Args for {@link threadCreatedEvent}. */
export interface ThreadCreatedArgs {
  /** The new child thread's id (`sth_…`). */
  sessionThreadId: string;
  /** The roster agent running in the thread (announced as `agent_name`). */
  agentName: string;
  /** The parent (primary) thread id, or `null` when none is known. */
  parentThreadId: string | null;
}

/**
 * A `session.thread_created` event on the PRIMARY thread — the durable signal the
 * registry projects into a new `session_threads` row (id + agent_name + parent,
 * status 'running'). Emitted when the coordinator spins up a subagent thread.
 */
export function threadCreatedEvent(args: ThreadCreatedArgs): AgentEvent {
  return {
    kind: SESSION_THREAD_CREATED,
    payload: {
      session_thread_id: args.sessionThreadId,
      agent_name: args.agentName,
      parent_thread_id: args.parentThreadId,
    },
    subpath: PRIMARY_THREAD_SUBPATH,
  };
}

/**
 * A `session.thread_status_running` event on the PRIMARY thread — flips the child
 * thread's read-model row to `running` (emitted as the subagent's turn starts).
 */
export function threadStatusRunningEvent(args: { sessionThreadId: string }): AgentEvent {
  return {
    kind: SESSION_THREAD_STATUS_RUNNING,
    payload: { session_thread_id: args.sessionThreadId },
    subpath: PRIMARY_THREAD_SUBPATH,
  };
}

/**
 * A `session.thread_status_idle` event on the PRIMARY thread — flips the child
 * thread's row to `idle` and records the `stop_reason` (emitted when the subagent's
 * delegated turn completes).
 */
export function threadStatusIdleEvent(args: {
  sessionThreadId: string;
  stopReason: string;
}): AgentEvent {
  return {
    kind: SESSION_THREAD_STATUS_IDLE,
    payload: { session_thread_id: args.sessionThreadId, stop_reason: args.stopReason },
    subpath: PRIMARY_THREAD_SUBPATH,
  };
}

/**
 * A `session.thread_status_terminated` event on the PRIMARY thread — flips the child
 * thread's row to `terminated` (emitted on interrupt / coordinator teardown).
 */
export function threadStatusTerminatedEvent(args: { sessionThreadId: string }): AgentEvent {
  return {
    kind: SESSION_THREAD_STATUS_TERMINATED,
    payload: { session_thread_id: args.sessionThreadId },
    subpath: PRIMARY_THREAD_SUBPATH,
  };
}

/**
 * An `agent.thread_message_received` event on the PRIMARY thread — the delegation
 * PROMPT the coordinator handed a subagent, surfaced as "received BY the child thread
 * FROM the coordinator's thread". Emitted when a delegation begins.
 */
export function threadMessageReceivedEvent(args: {
  /** The RECEIVING (child) thread's id. */
  sessionThreadId: string;
  /** The thread the message came FROM (the coordinator's / primary thread). */
  fromSessionThreadId: string;
  /** The delegation prompt content. */
  content: string;
}): AgentEvent {
  return {
    kind: AGENT_THREAD_MESSAGE_RECEIVED,
    payload: {
      session_thread_id: args.sessionThreadId,
      from_session_thread_id: args.fromSessionThreadId,
      content: args.content,
    },
    subpath: PRIMARY_THREAD_SUBPATH,
  };
}

/**
 * An `agent.thread_message_sent` event on the PRIMARY thread — the RESULT a subagent
 * returned to the coordinator, surfaced as "sent BY the child thread TO the
 * coordinator's thread". Emitted when a delegated turn's result is delivered back.
 */
export function threadMessageSentEvent(args: {
  /** The SENDING (child) thread's id. */
  sessionThreadId: string;
  /** The thread the message is sent TO (the coordinator's / primary thread). */
  toSessionThreadId: string;
  /** The result content. */
  content: string;
}): AgentEvent {
  return {
    kind: AGENT_THREAD_MESSAGE_SENT,
    payload: {
      session_thread_id: args.sessionThreadId,
      to_session_thread_id: args.toSessionThreadId,
      content: args.content,
    },
    subpath: PRIMARY_THREAD_SUBPATH,
  };
}
