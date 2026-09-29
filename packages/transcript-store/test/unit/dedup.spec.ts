// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { LruDedup } from '../../src/kafka/dedup.js';

describe('LruDedup', () => {
  it('reports unseen on first observation, seen on second', () => {
    const lru = new LruDedup<string>(3);
    expect(lru.has('a')).toBe(false);
    lru.add('a', 'result-a');
    expect(lru.has('a')).toBe(true);
    expect(lru.get('a')).toBe('result-a');
  });

  it('evicts the oldest entry when capacity is exceeded', () => {
    const lru = new LruDedup<string>(2);
    lru.add('a', 'A');
    lru.add('b', 'B');
    lru.add('c', 'C');
    expect(lru.has('a')).toBe(false);
    expect(lru.has('b')).toBe(true);
    expect(lru.has('c')).toBe(true);
  });

  it('promotes on access (touched entries are kept)', () => {
    const lru = new LruDedup<string>(2);
    lru.add('a', 'A');
    lru.add('b', 'B');
    lru.get('a'); // touches 'a'
    lru.add('c', 'C'); // should evict 'b', not 'a'
    expect(lru.has('a')).toBe(true);
    expect(lru.has('b')).toBe(false);
    expect(lru.has('c')).toBe(true);
  });
});
