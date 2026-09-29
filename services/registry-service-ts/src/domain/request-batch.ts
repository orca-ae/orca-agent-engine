// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * A request-owned, bounded-query loader. Calls made in one microtask turn are
 * coalesced; repeated keys share a promise only for the lifetime of this loader.
 * Missing values and failures are cached too, so a response observes one result
 * per key rather than retrying part of a failed hydration graph.
 * A failed loader rejects new keys too, without starting another fetch.
 */
export function createRequestBatch<Key, Value>(
  keyOf: (key: Key) => string,
  fetchBatch: (keys: readonly Key[]) => Promise<ReadonlyMap<string, Value>>,
  missing: () => Value,
  batchSize = 100,
  signal?: AbortSignal,
): (key: Key) => Promise<Value> {
  if (!Number.isInteger(batchSize) || batchSize < 1) {
    throw new Error('request batch size must be a positive integer');
  }
  const cache = new Map<string, Promise<Value>>();
  let pending: Array<{
    key: Key;
    id: string;
    resolve: (value: Value) => void;
    reject: (error: unknown) => void;
  }> = [];
  let scheduled = false;
  let failed = false;
  let firstError: unknown;

  async function flush(): Promise<void> {
    // Also drain calls arriving while a previous chunk awaits the database.
    // There is at most one active query per loader, including later waves.
    while (pending.length > 0) {
      const batch = pending.splice(0, batchSize);
      try {
        signal?.throwIfAborted();
        const values = await fetchBatch(batch.map((entry) => entry.key));
        signal?.throwIfAborted();
        for (const entry of batch) {
          entry.resolve(values.has(entry.id) ? values.get(entry.id)! : missing());
        }
      } catch (error) {
        // Fail this loader permanently, including keys requested in later waves.
        failed = true;
        firstError = error;
        for (const entry of [...batch, ...pending]) entry.reject(error);
        pending = [];
        break;
      }
    }
    scheduled = false;
  }

  return (key) => {
    const id = keyOf(key);
    const cached = cache.get(id);
    if (failed) return cached ?? Promise.reject(firstError);
    if (signal?.aborted) return Promise.reject(signal.reason);
    if (cached) return cached;
    const promise = new Promise<Value>((resolve, reject) => {
      pending.push({ key, id, resolve, reject });
    });
    cache.set(id, promise);
    if (!scheduled) {
      scheduled = true;
      queueMicrotask(() => {
        void flush();
      });
    }
    return promise;
  };
}
