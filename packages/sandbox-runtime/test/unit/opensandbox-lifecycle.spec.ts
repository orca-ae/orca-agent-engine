// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import { waitUntilOpenSandboxReady } from '../../src/opensandbox-lifecycle.js';

afterEach(() => vi.useRealTimers());

describe('OpenSandbox readiness', () => {
  it('bounds package Pod reads to one poll every two seconds and reports failure without logs', async () => {
    vi.useFakeTimers();
    const getLifecycle = vi
      .fn()
      .mockResolvedValueOnce({ status: { state: 'Allocated' } })
      .mockResolvedValue({
        status: {
          state: 'Failed',
          reason: 'ENVIRONMENT_PACKAGES_INSTALL_FAILED',
          message: 'installer-private-output',
        },
      });
    const ping = vi.fn().mockResolvedValue({ ok: false, status: 503 });
    const outcome = waitUntilOpenSandboxReady({ id: 'package-sandbox', getLifecycle, ping }).catch(
      (error: unknown) => error,
    );
    await vi.advanceTimersByTimeAsync(1_999);
    expect(getLifecycle).toHaveBeenCalledTimes(1);
    expect(ping).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    const failure = await outcome;
    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toContain('ENVIRONMENT_PACKAGES_INSTALL_FAILED');
    expect((failure as Error).message).not.toContain('installer-private-output');
    expect(getLifecycle).toHaveBeenCalledTimes(2);
    expect(ping).toHaveBeenCalledTimes(1);
  });

  it('retains the 500 ms readiness interval without packages', async () => {
    vi.useFakeTimers();
    const ping = vi
      .fn()
      .mockResolvedValueOnce({ ok: false, status: 503 })
      .mockResolvedValue({ ok: true, status: 200 });
    const ready = waitUntilOpenSandboxReady({ id: 'plain-sandbox', ping });
    await vi.advanceTimersByTimeAsync(499);
    expect(ping).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    await ready;
    expect(ping).toHaveBeenCalledTimes(2);
  });

  it('retries a lifecycle read error before accepting execd readiness', async () => {
    vi.useFakeTimers();
    const getLifecycle = vi
      .fn()
      .mockRejectedValueOnce(new Error('temporary lifecycle outage'))
      .mockResolvedValue({ status: { state: 'Allocated' } });
    const ping = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    const ready = waitUntilOpenSandboxReady({ id: 'package-sandbox', getLifecycle, ping });
    await vi.advanceTimersByTimeAsync(1_999);
    expect(getLifecycle).toHaveBeenCalledTimes(1);
    expect(ping).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await ready;
    expect(getLifecycle).toHaveBeenCalledTimes(2);
    expect(ping).toHaveBeenCalledTimes(1);
  });

  it('honors a deadline shorter than the package poll interval', async () => {
    vi.useFakeTimers();
    const getLifecycle = vi.fn().mockResolvedValue({ status: { state: 'Allocated' } });
    const ping = vi.fn().mockResolvedValue({ ok: false, status: 503 });
    const outcome = waitUntilOpenSandboxReady({
      id: 'package-sandbox',
      timeoutMs: 750,
      getLifecycle,
      ping,
    }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(750);
    expect(((await outcome) as Error).message).toContain(
      'did not become ready: execd ping returned 503',
    );
    expect(getLifecycle).toHaveBeenCalledTimes(1);
    expect(ping).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('returns immediately when execd is ready', async () => {
    vi.useFakeTimers();
    await waitUntilOpenSandboxReady({
      id: 'plain-sandbox',
      ping: async () => ({ ok: true, status: 200 }),
    });
    expect(vi.getTimerCount()).toBe(0);
  });
});
