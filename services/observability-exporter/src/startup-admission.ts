// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** FIFO admission shared by startup and lazy restore; permits never outlive their work. */
export class StartupAdmission {
  private active = 0;
  private readonly queue = new Map<() => void, number>();
  private readonly running = new Map<symbol, number>();
  // One counter per live pool, not one queued closure per not-yet-started item.
  private readonly pools = new Set<{ remaining: number; queuedAt: number }>();
  private maxWaitMs = 0;
  private maxRunMs = 0;

  constructor(private readonly concurrency: number) {
    if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 32) {
      throw new Error('invalid Kafka exporter concurrency');
    }
  }

  /** Lifetime high-water durations, including work currently queued/running; no retained history. */
  snapshot(): { active: number; queued: number; maxWaitMs: number; maxRunMs: number } {
    const now = performance.now();
    let queued = this.queue.size;
    let maxWaitMs = this.maxWaitMs;
    for (const start of this.queue.values()) maxWaitMs = Math.max(maxWaitMs, now - start);
    for (const pool of this.pools) {
      queued += pool.remaining;
      if (pool.remaining > 0) maxWaitMs = Math.max(maxWaitMs, now - pool.queuedAt);
    }
    return {
      active: this.active,
      queued,
      maxWaitMs,
      maxRunMs: Math.max(this.maxRunMs, ...[...this.running.values()].map((start) => now - start)),
    };
  }

  async run<T>(
    signal: AbortSignal,
    operation: () => Promise<T>,
    queuedAt = performance.now(),
  ): Promise<T> {
    signal.throwIfAborted();
    const token = Symbol();
    await new Promise<void>((resolve, reject) => {
      const cancel = (): void => {
        this.maxWaitMs = Math.max(this.maxWaitMs, performance.now() - queuedAt);
        this.queue.delete(admit);
        reject(signal.reason);
      };
      const admit = (): void => {
        this.queue.delete(admit);
        signal.removeEventListener('abort', cancel);
        this.active += 1;
        const now = performance.now();
        this.maxWaitMs = Math.max(this.maxWaitMs, now - queuedAt);
        this.running.set(token, now);
        resolve();
      };
      if (this.active < this.concurrency) admit();
      else {
        this.queue.set(admit, queuedAt);
        signal.addEventListener('abort', cancel, { once: true });
      }
    });
    try {
      signal.throwIfAborted();
      return await operation();
    } finally {
      this.maxRunMs = Math.max(this.maxRunMs, performance.now() - this.running.get(token)!);
      this.running.delete(token);
      this.active -= 1;
      this.queue.keys().next().value?.();
    }
  }

  /** Fail fast to stop admission, but drain every started operation before returning. */
  async runAll<T>(
    items: readonly T[],
    signal: AbortSignal,
    operation: (item: T) => Promise<void>,
    onFailure: () => void,
  ): Promise<void> {
    let failure: { error: unknown } | undefined;
    let next = 0;
    const pool = { remaining: items.length, queuedAt: performance.now() };
    this.pools.add(pool);
    try {
      const results = await Promise.allSettled(
        Array.from({ length: Math.min(items.length, this.concurrency) }, async () => {
          while (next < items.length && failure === undefined) {
            signal.throwIfAborted();
            const item = items[next++]!;
            pool.remaining -= 1;
            await this.run(
              signal,
              async () => {
                try {
                  await operation(item);
                } catch (error) {
                  failure ??= { error };
                  this.pools.delete(pool);
                  onFailure();
                  throw error;
                }
              },
              pool.queuedAt,
            );
          }
        }),
      );
      if (failure !== undefined) throw failure.error;
      const rejected = results.find((result) => result.status === 'rejected');
      if (rejected?.status === 'rejected') throw rejected.reason;
    } finally {
      this.pools.delete(pool);
    }
  }
}
