// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { CanonicalAgentEventKind } from './kinds.js';

/** Stable identifier for one persisted agent event. */
export type AgentEventId = `evt_${string}`;

/** Parent-agent events use `''`; child-agent events use `subagents/<thread-id>`. */
export type AgentEventSubpath = '' | `subagents/${string}`;

/**
 * Canonical event a harness persists for an agent turn.
 *
 * IDs and subpaths are required so consumers can correlate an event without
 * deriving identity from a payload or transcript position.
 */
export interface AgentEvent<
  K extends CanonicalAgentEventKind = CanonicalAgentEventKind,
  P = unknown,
> {
  id: AgentEventId;
  subpath: AgentEventSubpath;
  kind: K;
  payload: P;
}
