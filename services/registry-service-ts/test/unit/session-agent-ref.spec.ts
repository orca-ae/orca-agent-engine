// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { resolveSessionAgentRef } from '../../src/api/sessions.routes.js';

describe('resolveSessionAgentRef (polymorphic agent ref)', () => {
  it('accepts a bare string `agent` as the latest-version pin', () => {
    expect(resolveSessionAgentRef({ agent: 'agt_abc' })).toEqual({ agentId: 'agt_abc' });
  });

  it('accepts {type:"agent", id} without a version (latest)', () => {
    expect(resolveSessionAgentRef({ agent: { type: 'agent', id: 'agt_abc' } })).toEqual({
      agentId: 'agt_abc',
    });
  });

  it('pins an explicit version from {type:"agent", id, version}', () => {
    expect(resolveSessionAgentRef({ agent: { type: 'agent', id: 'agt_abc', version: 3 } })).toEqual(
      { agentId: 'agt_abc', requestedVersion: 3 },
    );
  });

  it('supports the legacy agent_id alias', () => {
    expect(resolveSessionAgentRef({ agent_id: 'agt_legacy' })).toEqual({ agentId: 'agt_legacy' });
  });

  it('normalizes Claude wire-prefixed agent ids', () => {
    expect(resolveSessionAgentRef({ agent_id: 'agent_legacy' })).toEqual({ agentId: 'agt_legacy' });
    expect(resolveSessionAgentRef({ agent: 'agent_abc' })).toEqual({ agentId: 'agt_abc' });
    expect(
      resolveSessionAgentRef({ agent: { type: 'agent', id: 'agent_abc', version: 2 } }),
    ).toEqual({ agentId: 'agt_abc', requestedVersion: 2 });
  });

  it('rejects when neither agent nor agent_id is provided', () => {
    const out = resolveSessionAgentRef({});
    expect('error' in out).toBe(true);
  });

  it('rejects when both agent and agent_id are provided', () => {
    const out = resolveSessionAgentRef({ agent: 'agt_abc', agent_id: 'agt_abc' });
    expect('error' in out).toBe(true);
  });

  it('rejects a non-positive / non-integer version', () => {
    expect(
      'error' in resolveSessionAgentRef({ agent: { type: 'agent', id: 'agt_abc', version: 0 } }),
    ).toBe(true);
    expect(
      'error' in resolveSessionAgentRef({ agent: { type: 'agent', id: 'agt_abc', version: 1.5 } }),
    ).toBe(true);
  });
});
