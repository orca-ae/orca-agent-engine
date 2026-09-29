// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  boundedAgentEventId,
  eventIdentityKey,
  isBoundedAgentEventId,
  outcomeIdentityKey,
  isOutcomeIdentityKey,
} from '../../src/event-identity.js';

describe('bounded observability event identity', () => {
  it('preserves ordinary canonical IDs and digests oversized valid IDs', () => {
    expect(boundedAgentEventId('evt_normal:id/with.dots')).toBe('evt_normal:id/with.dots');
    const oversized = `evt_${'x'.repeat(4_000)}`;
    const bounded = boundedAgentEventId(oversized);
    expect(bounded).toMatch(/^evt_digest_[0-9a-f]{64}$/u);
    expect(bounded).toBe(boundedAgentEventId(oversized));
    expect(isBoundedAgentEventId(bounded)).toBe(true);
    expect(Buffer.byteLength(bounded!, 'utf8')).toBeLessThanOrEqual(512);
    expect(boundedAgentEventId('evt_\ud800')).toMatch(/^evt_digest_[0-9a-f]{64}$/u);
    expect(eventIdentityKey('evt_\ud800')).not.toBe(eventIdentityKey('evt_\ud801'));
    const craftedAlias = `evt_digest_${eventIdentityKey(oversized)}`;
    expect(boundedAgentEventId(craftedAlias)).not.toBe(bounded);
  });

  it('uses a fixed-width key without indexing the raw identifier', () => {
    expect(eventIdentityKey(`evt_${'x'.repeat(4_000)}`)).toMatch(/^[0-9a-f]{64}$/u);
  });

  it('hashes every bounded outcome identity without preserving content', () => {
    for (const id of [
      'outcome_defined_outcome',
      'outcome_quality_1',
      'outc_01AbC-123',
      'Bearer secret',
      'outcome_中文',
      'outcome_text with spaces',
      'outcome_x\n',
      'outcome_x/secret',
      '\0\ud800',
      `outcome_${'x'.repeat(504)}`,
    ]) {
      const key = outcomeIdentityKey(id);
      expect(key).toMatch(/^outcome_digest_[0-9a-f]{64}$/u);
      expect(key).toBe(outcomeIdentityKey(id));
      expect(isOutcomeIdentityKey(key)).toBe(true);
      expect(key).not.toContain(id);
      expect(key?.slice('outcome_digest_'.length)).not.toBe(eventIdentityKey(id));
      expect(outcomeIdentityKey(key)).not.toBe(key);
    }
    for (const id of [null, 1, '', `outcome_${'x'.repeat(505)}`]) {
      expect(outcomeIdentityKey(id)).toBeUndefined();
    }
    expect(outcomeIdentityKey(String.fromCharCode(0xd800))).not.toBe(
      outcomeIdentityKey(String.fromCharCode(0xd801)),
    );
  });

  it('validates only exact lowercase digest keys', () => {
    const key = outcomeIdentityKey('outcome_test')!;
    for (const invalid of [
      null,
      1,
      '',
      'outcome_test',
      key.toUpperCase(),
      key + '\n',
      key + 'a',
      key.slice(1),
    ]) {
      expect(isOutcomeIdentityKey(invalid)).toBe(false);
    }
  });
});
