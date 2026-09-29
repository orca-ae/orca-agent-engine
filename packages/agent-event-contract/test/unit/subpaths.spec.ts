// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  PRIMARY_AGENT_SUBPATH,
  SUBAGENT_SUBPATH_PREFIX,
  isAgentEventSubpath,
  subagentEventSubpath,
  type AgentEventSubpath,
} from '../../src/index.js';

describe('canonical event subpaths', () => {
  it('constructs a subagent producer path and exposes the primary path', () => {
    const primary: AgentEventSubpath = PRIMARY_AGENT_SUBPATH;

    expect(primary).toBe('');
    expect(SUBAGENT_SUBPATH_PREFIX).toBe('subagents/');
    expect(subagentEventSubpath('thread_1')).toBe('subagents/thread_1');
  });

  it('rejects empty and wildcard thread IDs', () => {
    expect(() => subagentEventSubpath('')).toThrow(
      'subagent thread id must not be empty or wildcard',
    );
    expect(() => subagentEventSubpath('*')).toThrow(
      'subagent thread id must not be empty or wildcard',
    );
  });

  it('accepts only canonical primary and subagent producer paths', () => {
    for (const value of ['', 'subagents/thread_1', 'subagents/thread_1/nested']) {
      expect(isAgentEventSubpath(value)).toBe(true);
    }

    for (const value of [
      undefined,
      null,
      1,
      '*',
      'threads/thread_1',
      'subagents/',
      'subagents/*',
    ]) {
      expect(isAgentEventSubpath(value)).toBe(false);
    }
  });
});
