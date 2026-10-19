// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  catalogName,
  turnEndPayload,
  isSurfaceReplacement,
  CompactionFacts,
} from '../../src/event-facts.js';

describe('bounded native event facts', () => {
  it('maps labels and structured reasons without diagnostic text', () => {
    expect(catalogName('reviewer')).toEqual({ agent_name: 'reviewer' });
    expect(catalogName(undefined)).toEqual({});
    expect(() => catalogName('x'.repeat(257))).toThrow();
    expect(() => catalogName({ name: 'reviewer' })).toThrow();
    expect(turnEndPayload({ kind: 'completed' })).toEqual({
      stop_reason: 'completed',
      native_reason: 'completed',
    });
    expect(turnEndPayload({ kind: 'aborted' })).toEqual({
      stop_reason: 'interrupted',
      native_reason: 'aborted',
    });
    expect(turnEndPayload({ kind: 'interrupted' }).stop_reason).toBe('interrupted');
    expect(turnEndPayload({ kind: 'error' }).stop_reason).toBe('error');
    expect(() => turnEndPayload({ kind: 'private error text' })).toThrow();
  });

  it('identifies replacement results even when their ID is a live call ID', () => {
    const live = new Map([['shared-call', 10]]);
    const replacement = {
      surfaceOp: { op: 'replace', startSeq: 1, endSeq: 3 },
      sourceEventSeqs: [1, 3],
    };
    if (!isSurfaceReplacement(replacement)) live.delete('shared-call');
    expect(live.get('shared-call')).toBe(10);
    expect(isSurfaceReplacement({ surfaceOp: 'append' })).toBe(false);
  });

  it('publishes only paired successful compactions, without raw summary or output', () => {
    const facts = new CompactionFacts();
    const accept = (type: string, seq: number, data: Record<string, unknown> = {}) =>
      facts.accept('child', { type, seq, data: { compactionId: 'compact-1', turn: 1, ...data } });
    expect(accept('compaction/end', 3)).toBeUndefined();
    accept('compaction/start', 4);
    expect(accept('compaction/end', 5)).toBeUndefined();
    accept('compaction/start', 6);
    accept('compaction/summary', 7, { summary: 'private', rawOutput: 'private' });
    expect(accept('compaction/end', 8, { error: 'private error' })).toBeUndefined();
    expect(accept('compaction/end', 9)).toBeUndefined();
    accept('compaction/start', 10);
    accept('compaction/summary', 11);
    expect(accept('compaction/end', 13)).toEqual({
      native_compaction_id: 'compact-1',
      native_source: { session_id: 'child', seq: 13 },
      source_event_seqs: [10, 11, 13],
    });
    accept('compaction/start', 14);
    accept('compaction/summary', 15);
    accept('turn/end', 16);
    expect(accept('compaction/end', 17)).toBeUndefined();
    accept('compaction/start', 18);
    accept('compaction/summary', 19, { compactionId: 'another' });
    expect(accept('compaction/end', 20)).toBeUndefined();
    accept('compaction/start', 21);
    accept('compaction/summary', 22);
    facts.clear();
    expect(accept('compaction/end', 23)).toBeUndefined();
  });
});
