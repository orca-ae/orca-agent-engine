// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Counter } from 'prom-client';
import { registry } from '../metrics.js';

const proofTotal = new Counter({
  name: 'registry_service_api_key_proof_cache_total',
  help: 'Cryptographic proof cache lookups; authorization is always re-read from the database.',
  labelNames: ['result'] as const,
  registers: [registry],
});

/** Positive cryptographic proofs only. Never stores plaintext, principals or authorization decisions. */
export function createApiKeyProofCache(
  options: {
    enabled?: boolean;
    ttlMs?: number;
    maxEntries?: number;
    now?: () => number;
  } = {},
) {
  const enabled = options.enabled ?? true;
  const ttl = options.ttlMs ?? 60_000;
  const max = options.maxEntries ?? 1024;
  const now = options.now ?? Date.now;
  const proofs = new Map<string, number>();
  const pending = new Map<string, Promise<boolean>>();
  if (!Number.isInteger(max) || max < 1 || !Number.isFinite(ttl) || ttl <= 0) {
    throw new Error('invalid API key proof cache bounds');
  }
  return async (
    fingerprint: string,
    storedHash: string,
    verify: () => Promise<boolean>,
  ): Promise<boolean> => {
    if (!enabled) return verify();
    // The supplied credential fingerprint AND current stored hash participate.
    // A collision in a database lookup or a rotated hash cannot reuse a proof.
    const key = JSON.stringify([fingerprint, storedHash]);
    const expiresAt = proofs.get(key);
    if (expiresAt !== undefined) {
      proofs.delete(key);
      const currentTime = now();
      if (currentTime < expiresAt && expiresAt - currentTime <= ttl) {
        proofs.set(key, expiresAt); // bounded LRU without sliding expiration
        proofTotal.inc({ result: 'hit' });
        return true;
      }
    }
    const inFlight = pending.get(key);
    if (inFlight) {
      proofTotal.inc({ result: 'coalesced' });
      return inFlight;
    }
    proofTotal.inc({ result: 'miss' });
    // Bound the bookkeeping even during a burst of distinct valid credentials.
    // At capacity the existing verifier remains authoritative, without caching.
    if (pending.size >= max) return verify();
    const promise = Promise.resolve()
      .then(verify)
      .then((valid) => {
        if (valid) {
          while (proofs.size >= max) proofs.delete(proofs.keys().next().value!);
          proofs.set(key, now() + ttl);
        }
        return valid;
      })
      .finally(() => {
        pending.delete(key);
      });
    pending.set(key, promise);
    return promise;
  };
}
