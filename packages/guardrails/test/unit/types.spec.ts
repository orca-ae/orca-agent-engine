// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { PHASES, SCOPES, TIERS, failsClosed, supportsAsk, type Phase } from '../../src/types.js';

describe('phase semantics', () => {
  it('models all six evaluation points', () => {
    expect(PHASES).toEqual([
      'request',
      'tool_call',
      'tool_result',
      'response',
      'llm_request',
      'llm_response',
    ]);
  });

  it('fails closed exactly on the phases that gate an action before it happens', () => {
    const closed = PHASES.filter(failsClosed);
    expect(closed).toEqual(['request', 'tool_call', 'llm_request']);
  });

  it('fails open on observational phases, where suppressing on an internal error is worse', () => {
    for (const phase of ['tool_result', 'response', 'llm_response'] as Phase[]) {
      expect(failsClosed(phase)).toBe(false);
    }
  });

  it('supports ask only where a client round trip already exists', () => {
    const askable = PHASES.filter(supportsAsk);
    expect(askable).toEqual(['tool_call']);
  });

  it('does not support ask at tool_result, where the tool has already run', () => {
    expect(supportsAsk('tool_result')).toBe(false);
  });
});

describe('authority', () => {
  it('composes tiers least to most authoritative', () => {
    expect(TIERS).toEqual(['session', 'agent', 'workspace', 'organization']);
  });

  it('offers exactly the three attachment scopes', () => {
    expect(SCOPES).toEqual(['organization', 'workspace', 'explicit']);
  });
});
