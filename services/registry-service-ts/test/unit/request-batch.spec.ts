// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { createRequestBatch } from '../../src/domain/request-batch.js';

describe('request-owned batch loader', () => {
  it('coalesces one wave, preserves key ordering, and memoizes duplicate and missing keys', async () => {
    const fetch = vi.fn(
      async (keys: readonly string[]) =>
        new Map(keys.filter((k) => k !== 'missing').map((k) => [k, k.toUpperCase()])),
    );
    const load = createRequestBatch(
      (key: string) => key,
      fetch,
      () => null,
    );
    const first = load('b');
    expect(load('b')).toBe(first);
    expect(await Promise.all([first, load('a'), load('missing')])).toEqual(['B', 'A', null]);
    expect(await load('missing')).toBeNull();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith(['b', 'a', 'missing']);
  });

  it('limits query size and runs chunks sequentially', async () => {
    let active = 0;
    let peak = 0;
    const fetch = vi.fn(async (keys: readonly string[]) => {
      active++;
      peak = Math.max(peak, active);
      await Promise.resolve();
      active--;
      return new Map(keys.map((key) => [key, key]));
    });
    const load = createRequestBatch(
      (key: string) => key,
      fetch,
      () => '',
      2,
    );
    expect(await Promise.all(['a', 'b', 'c', 'd', 'e'].map(load))).toEqual([
      'a',
      'b',
      'c',
      'd',
      'e',
    ]);
    expect(fetch.mock.calls.map(([keys]) => keys.length)).toEqual([2, 2, 1]);
    expect(peak).toBe(1);
  });

  it('rejects the remaining wave after a failure without starting more queries', async () => {
    const error = new Error('database unavailable');
    const fetch = vi.fn(async () => {
      throw error;
    });
    const load = createRequestBatch(
      (key: string) => key,
      fetch,
      () => '',
      1,
    );
    const first = load('a');
    const result = await Promise.allSettled([first, load('b'), load('c')]);
    expect(result).toEqual(
      Array.from({ length: 3 }, () => ({ status: 'rejected', reason: error })),
    );
    await expect(load('a')).rejects.toBe(error);
    expect(load('a')).toBe(first);
    await expect(load('new-key')).rejects.toBe(error);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('retains even an undefined rejection for keys requested after failure', async () => {
    const fetch = vi.fn(() => Promise.reject(undefined));
    const load = createRequestBatch(String, fetch, () => null);
    await expect(load('first')).rejects.toBeUndefined();
    await expect(load('later')).rejects.toBeUndefined();
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('does not share results between requests and accepts a later independent wave', async () => {
    const fetch = vi.fn(async (keys: readonly string[]) => new Map(keys.map((key) => [key, key])));
    const request = () =>
      createRequestBatch(
        (key: string) => key,
        fetch,
        () => '',
      );
    const first = request();
    await first('a');
    await first('b');
    await request()('a');
    expect(fetch.mock.calls.map(([keys]) => keys)).toEqual([['a'], ['b'], ['a']]);
  });

  it('rejects invalid batch sizes', () => {
    for (const size of [0, -1, 1.5, Infinity]) {
      expect(() =>
        createRequestBatch(
          String,
          async () => new Map(),
          () => null,
          size,
        ),
      ).toThrow('positive integer');
    }
  });

  it('serializes a second wave arriving while the first is in flight', async () => {
    let release!: () => void;
    const fetch = vi.fn(async (keys: readonly string[]) => {
      if (keys[0] === 'a')
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      return new Map(keys.map((key) => [key, key]));
    });
    const load = createRequestBatch(String, fetch, () => '');
    const first = load('a');
    await Promise.resolve();
    const later = load('b');
    await Promise.resolve();
    expect(fetch).toHaveBeenCalledTimes(1);
    release();
    expect(await Promise.all([first, later])).toEqual(['a', 'b']);
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});
