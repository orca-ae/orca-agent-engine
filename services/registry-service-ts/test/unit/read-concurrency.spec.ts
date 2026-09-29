// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { mapReadItems } from '../../src/domain/read-concurrency.js';
import { createRequestBatch } from '../../src/domain/request-batch.js';

describe('bounded cancelable reads', () => {
  it('caps concurrency and preserves order', async () => {
    let active = 0;
    let peak = 0;
    const output = await mapReadItems([3, 2, 1, 0], 2, async (item) => {
      active++;
      peak = Math.max(peak, active);
      await Promise.resolve();
      active--;
      return item * 2;
    });
    expect(output).toEqual([6, 4, 2, 0]);
    expect(peak).toBe(2);
  });

  it('does not launch following work after a failure or abort', async () => {
    const seen: number[] = [];
    await expect(
      mapReadItems([1, 2, 3], 1, async (item) => {
        seen.push(item);
        throw new Error('failed');
      }),
    ).rejects.toThrow('failed');
    expect(seen).toEqual([1]);
    const abort = new AbortController();
    await expect(
      mapReadItems(
        [1, 2, 3],
        1,
        async (item) => {
          abort.abort();
          return item;
        },
        abort.signal,
      ),
    ).rejects.toMatchObject({ name: 'AbortError' });
  });

  it.each(['failure', 'abort'])(
    'drains started siblings before reporting the first %s',
    async (mode) => {
      const firstError = new Error('first failure');
      const abort = new AbortController();
      const started: number[] = [];
      let fail!: () => void;
      let release!: () => void;
      const failing = new Promise<void>((resolve) => {
        fail = resolve;
      });
      const siblings = new Promise<void>((resolve) => {
        release = resolve;
      });
      let settled = false;
      const result = mapReadItems(
        [0, 1, 2, 3],
        3,
        async (item) => {
          started.push(item);
          if (item === 0) {
            await failing;
            if (mode === 'abort') abort.abort(firstError);
            else throw firstError;
          } else {
            await siblings;
            throw new Error('later sibling failure');
          }
          return item;
        },
        abort.signal,
      ).then(
        () => {
          settled = true;
          return undefined;
        },
        (error: unknown) => {
          settled = true;
          return error;
        },
      );
      try {
        fail();
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(started).toEqual([0, 1, 2]);
        expect(settled).toBe(false);
      } finally {
        release();
      }
      expect(await result).toBe(firstError);
      expect(started).toEqual([0, 1, 2]);
    },
  );

  it('stops batch chunks after the current SQL settles without pretending it was canceled', async () => {
    const abort = new AbortController();
    const seen: string[][] = [];
    const load = createRequestBatch(
      String,
      async (keys: readonly string[]) => {
        seen.push([...keys]);
        abort.abort();
        return new Map(keys.map((key) => [key, key]));
      },
      () => '',
      1,
      abort.signal,
    );
    const results = await Promise.allSettled([load('a'), load('b')]);
    expect(results.map((result) => result.status)).toEqual(['rejected', 'rejected']);
    expect(seen).toEqual([['a']]);
    await expect(load('c')).rejects.toMatchObject({ name: 'AbortError' });
  });
});
