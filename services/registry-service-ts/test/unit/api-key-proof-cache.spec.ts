// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { createApiKeyProofCache } from '../../src/auth/api-key-proof-cache.js';

describe('API key cryptographic proof cache', () => {
  it('coalesces concurrent verification and caches only successful proofs', async () => {
    let release!: (valid: boolean) => void;
    const verify = vi.fn(
      () =>
        new Promise<boolean>((resolve) => {
          release = resolve;
        }),
    );
    const cache = createApiKeyProofCache();
    const calls = Array.from({ length: 20 }, () => cache('fingerprint', 'current-hash', verify));
    await Promise.resolve();
    expect(verify).toHaveBeenCalledTimes(1);
    release(true);
    expect(await Promise.all(calls)).toEqual(Array(20).fill(true));
    expect(await cache('fingerprint', 'current-hash', verify)).toBe(true);
    expect(verify).toHaveBeenCalledTimes(1);
  });

  it('does not share proofs across presented credentials or changed stored hashes', async () => {
    const cache = createApiKeyProofCache();
    const success = vi.fn(async () => true);
    const rejected = vi.fn(async () => false);
    await cache('original', 'hash-a', success);
    expect(await cache('different', 'hash-a', rejected)).toBe(false);
    expect(await cache('original', 'hash-b', rejected)).toBe(false);
    expect(await cache('original', 'hash-b', rejected)).toBe(false);
    expect(rejected).toHaveBeenCalledTimes(3);
  });

  it('expires without sliding TTL and bounds retained entries with LRU eviction', async () => {
    let now = 0;
    const cache = createApiKeyProofCache({ ttlMs: 10, maxEntries: 2, now: () => now });
    const verify = vi.fn(async () => true);
    await cache('a', 'hash', verify);
    await cache('b', 'hash', verify);
    now = 5;
    await cache('a', 'hash', verify);
    await cache('c', 'hash', verify); // evicts b, not the recently used a
    await cache('a', 'hash', verify);
    expect(verify).toHaveBeenCalledTimes(3);
    now = 10;
    await cache('a', 'hash', verify); // a's original expiry was not extended
    expect(verify).toHaveBeenCalledTimes(4);
    await cache('b', 'hash', verify);
    expect(verify).toHaveBeenCalledTimes(5);
  });

  it('removes failed in-flight work and preserves a disabled verifier', async () => {
    const cache = createApiKeyProofCache();
    const failure = new Error('verifier failed');
    await expect(
      cache('a', 'hash', async () => {
        throw failure;
      }),
    ).rejects.toBe(failure);
    expect(await cache('a', 'hash', async () => true)).toBe(true);
    const disabled = createApiKeyProofCache({ enabled: false });
    const verify = vi.fn(async () => true);
    await Promise.all([disabled('a', 'hash', verify), disabled('a', 'hash', verify)]);
    expect(verify).toHaveBeenCalledTimes(2);
  });

  it('fails closed after a backward clock jump and validates capacity', async () => {
    let now = 100;
    const cache = createApiKeyProofCache({ now: () => now, ttlMs: 10 });
    await cache('a', 'hash', async () => true);
    now = 90;
    expect(await cache('a', 'hash', async () => false)).toBe(false);
    for (const maxEntries of [0, -1, Infinity, 1.5]) {
      expect(() => createApiKeyProofCache({ maxEntries })).toThrow('bounds');
    }
  });
});
