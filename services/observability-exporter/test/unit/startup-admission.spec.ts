// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { StartupAdmission } from '../../src/startup-admission.js';

afterEach(() => vi.useRealTimers());

it('reports current bounded work and lifetime durations without retaining cancelled queue entries', async () => {
  vi.useFakeTimers({ toFake: ['performance'] });
  const admission = new StartupAdmission(1);
  const signal = new AbortController().signal;
  let release!: () => void;
  const running = admission.run(
    signal,
    () =>
      new Promise<void>((resolve) => {
        release = resolve;
      }),
  );
  await Promise.resolve();
  const cancelled = new AbortController();
  const waiting = admission.run(cancelled.signal, async () => {}).catch(() => undefined);
  const order: number[] = [];
  const next = admission.run(signal, async () => {
    order.push(1);
  });
  const last = admission.run(signal, async () => {
    order.push(2);
  });
  vi.advanceTimersByTime(25);
  expect(admission.snapshot()).toEqual({ active: 1, queued: 3, maxWaitMs: 25, maxRunMs: 25 });
  cancelled.abort();
  await waiting;
  expect(admission.snapshot().queued).toBe(2);
  release();
  await Promise.all([running, next, last]);
  expect(order).toEqual([1, 2]);
  expect(admission.snapshot()).toEqual({ active: 0, queued: 0, maxWaitMs: 25, maxRunMs: 25 });
  vi.advanceTimersByTime(100);
  expect(admission.snapshot()).toEqual({ active: 0, queued: 0, maxWaitMs: 25, maxRunMs: 25 });
});

it('retains only scalar history across repeated admission and drains a failed pool', async () => {
  vi.useFakeTimers({ toFake: ['performance'] });
  const admission = new StartupAdmission(2);
  const controller = new AbortController();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const work = admission.runAll(
    Array.from({ length: 1000 }, (_, i) => i),
    controller.signal,
    async (i) => {
      expect(admission.snapshot().active).toBeLessThanOrEqual(2);
      expect(admission.snapshot().queued).toBeLessThanOrEqual(998);
      if (i < 2) await gate;
    },
    () => controller.abort(),
  );
  await Promise.resolve();
  vi.advanceTimersByTime(70);
  expect(admission.snapshot()).toEqual({ active: 2, queued: 998, maxWaitMs: 70, maxRunMs: 70 });
  release();
  await work;
  expect(admission.snapshot()).toEqual({ active: 0, queued: 0, maxWaitMs: 70, maxRunMs: 70 });
  await expect(
    admission.runAll(
      [1, 2, 3],
      controller.signal,
      async () => {
        throw new Error('failed');
      },
      () => controller.abort(),
    ),
  ).rejects.toThrow('failed');
  expect(admission.snapshot()).toMatchObject({ active: 0, queued: 0 });
});
