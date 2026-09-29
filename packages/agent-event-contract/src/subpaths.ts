// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { AgentEventSubpath } from './events.js';

export const PRIMARY_AGENT_SUBPATH = '' as const;
export const SUBAGENT_SUBPATH_PREFIX = 'subagents/' as const;

/** Construct a canonical subagent producer path from its session thread ID. */
export function subagentEventSubpath(threadId: string): AgentEventSubpath {
  if (threadId === '' || threadId === '*') {
    throw new Error('subagent thread id must not be empty or wildcard');
  }
  return `${SUBAGENT_SUBPATH_PREFIX}${threadId}`;
}

/**
 * Accept only producer paths from this contract. `*` is a read selector, not
 * an event subpath; legacy `threads/...` paths remain outside this contract.
 */
export function isAgentEventSubpath(value: unknown): value is AgentEventSubpath {
  return (
    value === PRIMARY_AGENT_SUBPATH ||
    (typeof value === 'string' &&
      value.startsWith(SUBAGENT_SUBPATH_PREFIX) &&
      value.length > SUBAGENT_SUBPATH_PREFIX.length &&
      value.slice(SUBAGENT_SUBPATH_PREFIX.length) !== '*')
  );
}
