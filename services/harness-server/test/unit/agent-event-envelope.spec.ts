// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { isAgentEventId, PRIMARY_AGENT_SUBPATH } from '@orca/agent-event-contract';
import { withCanonicalAgentEventEnvelope } from '../../src/harness/agent-harness.js';

describe('withCanonicalAgentEventEnvelope', () => {
  it('generates a canonical ID and explicit primary path for omitted fields', () => {
    const event = withCanonicalAgentEventEnvelope({ kind: 'agent.message', payload: {} });

    expect(isAgentEventId(event.id)).toBe(true);
    expect(event.subpath).toBe(PRIMARY_AGENT_SUBPATH);
  });

  it('preserves valid producer-owned identity and child path', () => {
    expect(
      withCanonicalAgentEventEnvelope({
        kind: 'agent.message',
        id: 'evt_child_message',
        subpath: 'subagents/reviewer/0',
        payload: { content: [] },
      }),
    ).toEqual({
      kind: 'agent.message',
      id: 'evt_child_message',
      subpath: 'subagents/reviewer/0',
      payload: { content: [] },
    });
  });

  it('rejects malformed supplied envelope fields instead of replacing them', () => {
    expect(() =>
      withCanonicalAgentEventEnvelope({ kind: 'agent.message', id: 'message_1', payload: {} }),
    ).toThrow('invalid AgentEvent.id');
    expect(() =>
      withCanonicalAgentEventEnvelope({
        kind: 'agent.message',
        id: null as never,
        payload: {},
      }),
    ).toThrow('invalid AgentEvent.id');
    expect(() =>
      withCanonicalAgentEventEnvelope({
        kind: 'agent.message',
        subpath: '*' as never,
        payload: {},
      }),
    ).toThrow('invalid AgentEvent.subpath');
    expect(() =>
      withCanonicalAgentEventEnvelope({
        kind: 'agent.message',
        subpath: null as never,
        payload: {},
      }),
    ).toThrow('invalid AgentEvent.subpath');
  });
});
