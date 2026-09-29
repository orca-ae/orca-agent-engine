// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SessionJwtProvider, type SessionJwt } from '../../src/mcp/session-jwt-provider.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(0);
});
afterEach(() => vi.useRealTimers());

describe('SessionJwtProvider', () => {
  it('refreshes before a bounded execution window and rejects too-short minted credentials', async () => {
    const mint = vi.fn(async () => ({
      token: `jwt-${Date.now()}`,
      expiresAt: Math.floor(Date.now() / 1000) + 660,
    }));
    const provider = new SessionJwtProvider(mint, 630_000);
    expect((await provider.getValidToken()).token).toBe('jwt-0');
    vi.setSystemTime(300_000);
    expect((await provider.getValidToken()).token).toBe('jwt-300000');
    expect(mint).toHaveBeenCalledTimes(2);
    vi.setSystemTime(600_000);
    mint.mockResolvedValue({ token: 'short', expiresAt: 900 });
    await expect(provider.getValidToken()).rejects.toThrow('execution window');
    provider.close();
  });

  it('has no idle timers and uses the default 30s headroom and 10s refresh deadline', async () => {
    const mint = vi
      .fn<(signal: AbortSignal) => Promise<SessionJwt>>()
      .mockResolvedValueOnce({ token: 'initial', expiresAt: 300 })
      .mockImplementation(() => new Promise(() => {}));
    const provider = new SessionJwtProvider(mint);
    await provider.getValidToken();
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(269_999);
    await provider.getValidToken();
    expect(mint).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    const rejected = expect(provider.getValidToken()).rejects.toThrow('timed out');
    expect(mint).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(9999);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    await rejected;
    expect(vi.getTimerCount()).toBe(0);
    provider.close();
  });

  it('removes caller abort listeners after success and failure', async () => {
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, 'removeEventListener');
    const mint = vi
      .fn()
      .mockRejectedValueOnce(new Error('secret'))
      .mockResolvedValueOnce({ token: 'fresh', expiresAt: 100 });
    const provider = new SessionJwtProvider(mint);
    await expect(provider.getValidToken(controller.signal)).rejects.toThrow('refresh failed');
    expect(remove).toHaveBeenCalledTimes(1);
    await provider.getValidToken(controller.signal);
    expect(remove).toHaveBeenCalledTimes(2);
    provider.close();
  });

  it('copies and freezes minted caches, refreshing on demand only', async () => {
    vi.setSystemTime(1_000_000);
    const initial = { token: 'initial', expiresAt: 1100 };
    const fresh = { token: 'fresh', expiresAt: 1200 };
    const mint = vi.fn().mockResolvedValueOnce(initial).mockResolvedValueOnce(fresh);
    const provider = new SessionJwtProvider(mint);
    const cached = await provider.getValidToken();
    initial.token = 'polluted';
    expect(cached.token).toBe('initial');
    expect(Object.isFrozen(cached)).toBe(true);
    vi.setSystemTime(1_080_000);
    expect(mint).toHaveBeenCalledTimes(1);
    expect((await provider.getValidToken()).token).toBe('fresh');
    fresh.token = 'polluted';
    expect((await provider.getValidToken()).token).toBe('fresh');
    expect(mint).toHaveBeenCalledTimes(2);
    provider.close();
  });

  it('shares refresh but isolates caller cancellation', async () => {
    const pending = deferred<SessionJwt>();
    const mint = vi.fn((_signal: AbortSignal) => pending.promise);
    const provider = new SessionJwtProvider(mint);
    const caller = new AbortController();
    const first = provider.getValidToken(caller.signal);
    const second = provider.getValidToken();
    const rejected = expect(first).rejects.toMatchObject({
      name: 'AbortError',
      message: 'Session JWT request cancelled',
    });
    caller.abort(new Error('secret'));
    await rejected;
    expect(mint.mock.calls[0]![0].aborted).toBe(false);
    pending.resolve({ token: 'fresh', expiresAt: 100 });
    await expect(second).resolves.toEqual({ token: 'fresh', expiresAt: 100 });
    expect(mint).toHaveBeenCalledTimes(1);
    provider.close();
  });

  it('does not start work for an already cancelled caller', async () => {
    const mint = vi.fn();
    const provider = new SessionJwtProvider(mint);
    await expect(provider.getValidToken(AbortSignal.abort('secret'))).rejects.toMatchObject({
      name: 'AbortError',
      message: 'Session JWT request cancelled',
    });
    expect(mint).not.toHaveBeenCalled();
  });

  it('close aborts shared work and rejects late resolution and subsequent reads', async () => {
    const pending = deferred<SessionJwt>();
    let signal!: AbortSignal;
    const provider = new SessionJwtProvider((s) => {
      signal = s;
      return pending.promise;
    });
    const result = provider.getValidToken();
    const rejected = expect(result).rejects.toThrow('closed');
    provider.close();
    provider.close();
    await rejected;
    expect(signal.aborted).toBe(true);
    pending.resolve({ token: 'late-secret', expiresAt: 100 });
    await expect(provider.getValidToken()).rejects.toThrow('closed');
  });

  it('bounds uncooperative mint, consumes late rejection, and allows another flight', async () => {
    const pending = deferred<SessionJwt>();
    const mint = vi.fn((_signal: AbortSignal) => pending.promise);
    const provider = new SessionJwtProvider(mint);
    const result = provider.getValidToken();
    const rejected = expect(result).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    expect(mint.mock.calls[0]![0].aborted).toBe(true);
    pending.reject(new Error('late-secret'));
    mint.mockResolvedValue({ token: 'next', expiresAt: 100 });
    await expect(provider.getValidToken()).resolves.toEqual({ token: 'next', expiresAt: 100 });
    expect(vi.getTimerCount()).toBe(0);
    provider.close();
  });

  it('does not let timed-out late success overwrite a newer cache', async () => {
    const pending = deferred<SessionJwt>();
    const mint = vi
      .fn()
      .mockReturnValueOnce(pending.promise)
      .mockResolvedValue({ token: 'next', expiresAt: 100 });
    const provider = new SessionJwtProvider(mint);
    const rejected = expect(provider.getValidToken()).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    await provider.getValidToken();
    pending.resolve({ token: 'late', expiresAt: 200 });
    await Promise.resolve();
    expect((await provider.getValidToken()).token).toBe('next');
    provider.close();
  });

  it('uses proportional refresh headroom for short TTL without refresh loops', async () => {
    const mint = vi.fn(async () => ({ token: 'short', expiresAt: Date.now() / 1000 + 5 }));
    const provider = new SessionJwtProvider(mint);
    await provider.getValidToken();
    vi.setSystemTime(3000);
    await provider.getValidToken();
    expect(mint).toHaveBeenCalledTimes(1);
    vi.setSystemTime(4000);
    await provider.getValidToken();
    await provider.getValidToken();
    expect(mint).toHaveBeenCalledTimes(2);
    provider.close();
  });

  it.each([
    null,
    {},
    { token: '', expiresAt: 200 },
    { token: 'bad token', expiresAt: 200 },
    { token: 'secret', expiresAt: '200' },
    { token: 'secret', expiresAt: NaN },
    { token: 'secret', expiresAt: Infinity },
    { token: 'secret', expiresAt: 100 },
    { token: 'secret', expiresAt: 99 },
    { token: 'secret', expiresAt: 200.5 },
  ])('rejects invalid fresh values without leaking data: %j', async (value) => {
    vi.setSystemTime(100_000);
    const provider = new SessionJwtProvider(async () => value as SessionJwt);
    await expect(provider.getValidToken()).rejects.toThrow(/^Session JWT refresh failed$/);
    provider.close();
  });

  it('sanitizes mint failures and never falls back to expired cache', async () => {
    const mint = vi
      .fn()
      .mockResolvedValueOnce({ token: 'expired', expiresAt: 1 })
      .mockImplementation(() => {
        throw new Error('secret token response');
      });
    const provider = new SessionJwtProvider(mint);
    await provider.getValidToken();
    vi.setSystemTime(1000);
    await expect(provider.getValidToken()).rejects.toThrow(/^Session JWT refresh failed$/);
    provider.close();
  });
});
