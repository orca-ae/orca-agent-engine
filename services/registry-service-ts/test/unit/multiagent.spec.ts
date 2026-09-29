// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import {
  MAX_ROSTER_SIZE,
  parseMultiagent,
  rosterAgentIds,
  snapshotMultiagent,
  readMultiagentSnapshot,
  isCoordinatorSnapshot,
  type ResolvedRosterAgent,
} from '../../src/domain/multiagent.js';

describe('multiagent parse', () => {
  it('accepts a coordinator with agent + self roster members', () => {
    const result = parseMultiagent({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_researcher' }, { type: 'self' }],
    });
    expect(result).toEqual({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_researcher' }, { type: 'self' }],
    });
  });

  it('accepts an explicitly version-pinned roster member', () => {
    const result = parseMultiagent({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_writer', version: 3 }],
    });
    expect(result).toEqual({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_writer', version: 3 }],
    });
  });

  it('rejects a non-object multiagent', () => {
    expect(parseMultiagent('nope')).toEqual({ error: 'multiagent must be an object' });
    expect(parseMultiagent([])).toEqual({ error: 'multiagent must be an object' });
    expect(parseMultiagent(null)).toEqual({ error: 'multiagent must be an object' });
  });

  it('rejects a non-coordinator type', () => {
    expect(parseMultiagent({ type: 'agent', agents: [] })).toEqual({
      error: "multiagent.type must be 'coordinator'",
    });
  });

  it('rejects a non-array or empty roster', () => {
    expect(parseMultiagent({ type: 'coordinator', agents: 'x' })).toEqual({
      error: 'multiagent.agents must be an array',
    });
    expect(parseMultiagent({ type: 'coordinator', agents: [] })).toEqual({
      error: 'multiagent.agents must not be empty',
    });
  });

  it('rejects a roster larger than the max size', () => {
    const agents = Array.from({ length: MAX_ROSTER_SIZE + 1 }, (_, i) => ({
      type: 'agent',
      id: `agt_${i}`,
    }));
    expect(parseMultiagent({ type: 'coordinator', agents })).toEqual({
      error: `multiagent.agents exceeds the max roster size of ${MAX_ROSTER_SIZE}`,
    });
  });

  it('accepts a roster exactly at the max size', () => {
    const agents = Array.from({ length: MAX_ROSTER_SIZE }, (_, i) => ({
      type: 'agent',
      id: `agt_${i}`,
    }));
    const result = parseMultiagent({ type: 'coordinator', agents });
    expect('error' in result).toBe(false);
  });

  it('rejects a malformed roster member', () => {
    expect(parseMultiagent({ type: 'coordinator', agents: ['x'] })).toEqual({
      error: 'multiagent.agents[] entries must be objects',
    });
    expect(parseMultiagent({ type: 'coordinator', agents: [{ type: 'nope' }] })).toEqual({
      error: "multiagent.agents[].type must be 'agent' or 'self'",
    });
    expect(
      parseMultiagent({ type: 'coordinator', agents: [{ type: 'agent', id: 'nope' }] }),
    ).toEqual({ error: 'multiagent.agents[].id must be an agt_… identifier' });
    expect(
      parseMultiagent({
        type: 'coordinator',
        agents: [{ type: 'agent', id: 'agt_x', version: 0 }],
      }),
    ).toEqual({ error: 'multiagent.agents[].version must be a positive integer' });
    expect(
      parseMultiagent({
        type: 'coordinator',
        agents: [{ type: 'agent', id: 'agt_x', version: 1.5 }],
      }),
    ).toEqual({ error: 'multiagent.agents[].version must be a positive integer' });
  });
});

describe('multiagent rosterAgentIds', () => {
  it('collects distinct agent ids, skipping self', () => {
    const parsed = parseMultiagent({
      type: 'coordinator',
      agents: [
        { type: 'agent', id: 'agt_a' },
        { type: 'self' },
        { type: 'agent', id: 'agt_b' },
        { type: 'agent', id: 'agt_a' },
      ],
    });
    if ('error' in parsed) throw new Error(parsed.error);
    expect(rosterAgentIds(parsed).sort()).toEqual(['agt_a', 'agt_b']);
  });
});

describe('multiagent snapshot', () => {
  const resolved = new Map<string, ResolvedRosterAgent>([
    ['agt_a', { id: 'agt_a', currentVersion: 4, isCoordinator: false }],
    ['agt_b', { id: 'agt_b', currentVersion: 2, isCoordinator: false }],
    ['agt_coord', { id: 'agt_coord', currentVersion: 1, isCoordinator: true }],
  ]);

  it('pins the current version when the member omits one', () => {
    const parsed = parseMultiagent({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_a' }, { type: 'self' }],
    });
    if ('error' in parsed) throw new Error(parsed.error);
    expect(snapshotMultiagent(parsed, resolved)).toEqual({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_a', version: 4 }, { type: 'self' }],
    });
  });

  it('honors an explicit version pin over the current version', () => {
    const parsed = parseMultiagent({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_a', version: 2 }],
    });
    if ('error' in parsed) throw new Error(parsed.error);
    expect(snapshotMultiagent(parsed, resolved)).toEqual({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_a', version: 2 }],
    });
  });

  it('rejects an unresolved roster agent', () => {
    const parsed = parseMultiagent({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_missing' }],
    });
    if ('error' in parsed) throw new Error(parsed.error);
    expect(snapshotMultiagent(parsed, resolved)).toEqual({
      error: 'multiagent roster agent agt_missing not found in workspace',
    });
  });

  it('rejects a roster agent that is itself a coordinator (one level only)', () => {
    const parsed = parseMultiagent({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_coord' }],
    });
    if ('error' in parsed) throw new Error(parsed.error);
    expect(snapshotMultiagent(parsed, resolved)).toEqual({
      error:
        'multiagent roster agent agt_coord is itself a coordinator (delegation is one level only)',
    });
  });
});

describe('multiagent snapshot read-back', () => {
  it('reads a persisted coordinator snapshot', () => {
    expect(
      readMultiagentSnapshot({
        type: 'coordinator',
        agents: [{ type: 'agent', id: 'agt_a', version: 4 }, { type: 'self' }],
      }),
    ).toEqual({
      type: 'coordinator',
      agents: [{ type: 'agent', id: 'agt_a', version: 4 }, { type: 'self' }],
    });
    expect(isCoordinatorSnapshot({ type: 'coordinator', agents: [{ type: 'self' }] })).toBe(true);
  });

  it('returns null for a single-agent (no multiagent) snapshot', () => {
    expect(readMultiagentSnapshot(undefined)).toBeNull();
    expect(readMultiagentSnapshot(null)).toBeNull();
    expect(readMultiagentSnapshot({})).toBeNull();
    expect(readMultiagentSnapshot({ type: 'agent' })).toBeNull();
    expect(isCoordinatorSnapshot(null)).toBe(false);
    expect(isCoordinatorSnapshot({})).toBe(false);
  });
});
