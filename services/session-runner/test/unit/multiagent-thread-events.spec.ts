// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner-side thread-event builders — the vocabulary the
// coordinator emits so the registry projects the multiagent choreography.
//
// These builders are the runner's mirror of the canonical `session.thread_*` /
// `agent.thread_message_*` kinds declared in `@orca/agent-event-contract` — the
// package the registry imports in `src/domain/thread-projection.ts`. That projector
// reads specific payload fields — `session_thread_id`, `agent_name`,
// `parent_thread_id`, `stop_reason` — off these events, so this spec pins the exact
// kind strings + payload shapes it expects. It also pins the SUBPATH each event rides:
// the lifecycle + message events surface on the PRIMARY thread (empty subpath) so a
// client following the session sees the whole choreography; a subagent's own turn
// events ride its `subagents/<id>` subpath.

import { describe, it, expect } from 'vitest';
import {
  SESSION_THREAD_CREATED,
  SESSION_THREAD_STATUS_RUNNING,
  SESSION_THREAD_STATUS_IDLE,
  SESSION_THREAD_STATUS_TERMINATED,
  AGENT_THREAD_MESSAGE_RECEIVED,
  AGENT_THREAD_MESSAGE_SENT,
  PRIMARY_THREAD_SUBPATH,
  SUBAGENT_SUBPATH_PREFIX,
  subagentThreadSubpath,
  threadCreatedEvent,
  threadStatusRunningEvent,
  threadStatusIdleEvent,
  threadStatusTerminatedEvent,
  threadMessageReceivedEvent,
  threadMessageSentEvent,
} from '../../src/harness/multiagent/thread-events.js';
import { contractConst, contractConstMember } from './support/registry-source-pin.js';

// The runner's thread-event kind strings + subpath constants MIRROR the canonical
// vocabulary in `@orca/agent-event-contract` (the runner is a separate process that
// does not link that package, so they are re-declared, not imported — the same
// pattern used for the `user.*` / `agent.*` kinds). Rather than assert the runner
// constants against a hand-copied COPY of the contract values (which would silently
// pass if only the CONTRACT side drifted), this pin reads the contract source files
// and extracts their canonical literals, so a one-sided change on EITHER side trips
// the test.
//
// The extraction and the path resolution live in `support/registry-source-pin.ts`,
// shared with the runner-tunnel wire pin in `protocol-registry-pin.spec.ts` — one
// implementation of "what does the other side actually say", rather than a second
// copy of the mechanism that exists to stop copies drifting.
//
// Unlike the tunnel pin, this one has NO absent-source skip. It used to point at
// `registry-service-ts/src/domain/session-threads.ts`, which no longer exists — the
// vocabulary moved into `@orca/agent-event-contract` — so the pin was permanently
// skipped and asserted nothing. The contract is a landed workspace package, not a
// file still waiting on the registry re-architecture, so a missing or renamed
// literal there is a hard failure rather than a skip.
const CONTRACT_KINDS_SRC = 'kinds.ts';
const CONTRACT_SUBPATHS_SRC = 'subpaths.ts';

/** The contract's literal for `SessionThreadEventKind.<member>`. */
const sessionThreadKind = (member: string): string =>
  contractConstMember(CONTRACT_KINDS_SRC, 'SessionThreadEventKind', member);

/** The contract's literal for `AgentThreadEventKind.<member>`. */
const agentThreadKind = (member: string): string =>
  contractConstMember(CONTRACT_KINDS_SRC, 'AgentThreadEventKind', member);

/** The contract's literal for a top-level subpath constant. */
const subpathConst = (name: string): string => contractConst(CONTRACT_SUBPATHS_SRC, name);

describe('thread-event kind constants mirror the canonical vocabulary (pinned to the contract SOURCE)', () => {
  it('matches the contract kind strings exactly (extracted from the contract source)', () => {
    // The canonical values are read from `@orca/agent-event-contract` itself — a
    // one-sided drift (renaming a kind on either the runner or the contract the
    // registry projects from) fails this equality.
    expect(SESSION_THREAD_CREATED).toBe(sessionThreadKind('created'));
    expect(SESSION_THREAD_STATUS_RUNNING).toBe(sessionThreadKind('statusRunning'));
    expect(SESSION_THREAD_STATUS_IDLE).toBe(sessionThreadKind('statusIdle'));
    expect(SESSION_THREAD_STATUS_TERMINATED).toBe(sessionThreadKind('statusTerminated'));
    expect(AGENT_THREAD_MESSAGE_RECEIVED).toBe(agentThreadKind('messageReceived'));
    expect(AGENT_THREAD_MESSAGE_SENT).toBe(agentThreadKind('messageSent'));
  });

  it('matches the contract subpath addressing exactly (primary + subagent prefix)', () => {
    // The subpath constants are equally load-bearing: the registry's projector treats
    // the PRIMARY subpath as the session stream, and a subagent's events ride the
    // `subagents/` prefix. Pin both to the contract source.
    expect(PRIMARY_THREAD_SUBPATH).toBe(subpathConst('PRIMARY_AGENT_SUBPATH'));
    expect(SUBAGENT_SUBPATH_PREFIX).toBe(subpathConst('SUBAGENT_SUBPATH_PREFIX'));
    // …and the derived child subpath uses that exact prefix.
    expect(subagentThreadSubpath('sth_child')).toBe(
      `${subpathConst('SUBAGENT_SUBPATH_PREFIX')}sth_child`,
    );
  });
});

describe('subagentThreadSubpath', () => {
  it('addresses a child thread under subagents/<id> (the registry subpath convention)', () => {
    expect(subagentThreadSubpath('sth_child')).toBe('subagents/sth_child');
  });
  it('the primary thread is the empty subpath', () => {
    expect(PRIMARY_THREAD_SUBPATH).toBe('');
  });
});

describe('thread lifecycle events (surface on the PRIMARY thread for the read-model projector)', () => {
  it('threadCreatedEvent carries session_thread_id + agent_name + parent_thread_id on the primary subpath', () => {
    const ev = threadCreatedEvent({
      sessionThreadId: 'sth_child',
      agentName: 'researcher',
      parentThreadId: 'sth_primary',
    });
    expect(ev.kind).toBe(SESSION_THREAD_CREATED);
    // The projector upserts the row from EXACTLY these fields (id, agent_name, parent).
    expect(ev.payload).toEqual({
      session_thread_id: 'sth_child',
      agent_name: 'researcher',
      parent_thread_id: 'sth_primary',
    });
    // Lifecycle events ride the PRIMARY stream so the projector (which reads the
    // session stream) and a session-following client both see them.
    expect(ev.subpath).toBe(PRIMARY_THREAD_SUBPATH);
  });

  it('threadCreatedEvent emits a null parent when no primary thread id is known', () => {
    const ev = threadCreatedEvent({
      sessionThreadId: 'sth_child',
      agentName: 'researcher',
      parentThreadId: null,
    });
    expect((ev.payload as Record<string, unknown>).parent_thread_id).toBeNull();
  });

  it('threadStatusRunningEvent marks a thread running (status upsert)', () => {
    const ev = threadStatusRunningEvent({ sessionThreadId: 'sth_child' });
    expect(ev.kind).toBe(SESSION_THREAD_STATUS_RUNNING);
    expect(ev.payload).toEqual({ session_thread_id: 'sth_child' });
    expect(ev.subpath).toBe(PRIMARY_THREAD_SUBPATH);
  });

  it('threadStatusIdleEvent carries the stop_reason the projector records', () => {
    const ev = threadStatusIdleEvent({ sessionThreadId: 'sth_child', stopReason: 'end_turn' });
    expect(ev.kind).toBe(SESSION_THREAD_STATUS_IDLE);
    expect(ev.payload).toEqual({ session_thread_id: 'sth_child', stop_reason: 'end_turn' });
    expect(ev.subpath).toBe(PRIMARY_THREAD_SUBPATH);
  });

  it('threadStatusTerminatedEvent marks a thread terminated', () => {
    const ev = threadStatusTerminatedEvent({ sessionThreadId: 'sth_child' });
    expect(ev.kind).toBe(SESSION_THREAD_STATUS_TERMINATED);
    expect(ev.payload).toEqual({ session_thread_id: 'sth_child' });
    expect(ev.subpath).toBe(PRIMARY_THREAD_SUBPATH);
  });
});

describe('cross-thread delegation message events (surface on the PRIMARY thread)', () => {
  it('threadMessageReceivedEvent names the RECEIVING thread + the FROM thread + content', () => {
    const ev = threadMessageReceivedEvent({
      sessionThreadId: 'sth_child',
      fromSessionThreadId: 'sth_primary',
      content: 'go research topic X',
    });
    expect(ev.kind).toBe(AGENT_THREAD_MESSAGE_RECEIVED);
    // The delegation prompt the coordinator handed the subagent: received BY the child
    // thread, FROM the primary (coordinator) thread.
    expect(ev.payload).toEqual({
      session_thread_id: 'sth_child',
      from_session_thread_id: 'sth_primary',
      content: 'go research topic X',
    });
    expect(ev.subpath).toBe(PRIMARY_THREAD_SUBPATH);
  });

  it('threadMessageSentEvent names the SENDING thread + the TO thread + content', () => {
    const ev = threadMessageSentEvent({
      sessionThreadId: 'sth_child',
      toSessionThreadId: 'sth_primary',
      content: 'here is the research result',
    });
    expect(ev.kind).toBe(AGENT_THREAD_MESSAGE_SENT);
    // The subagent's result returning to the coordinator: sent BY the child thread, TO
    // the primary thread.
    expect(ev.payload).toEqual({
      session_thread_id: 'sth_child',
      to_session_thread_id: 'sth_primary',
      content: 'here is the research result',
    });
    expect(ev.subpath).toBe(PRIMARY_THREAD_SUBPATH);
  });
});
