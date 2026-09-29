// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

const HEARTBEAT_INTERVAL_MS = 3_000;

export class KafkaTranscriptHeartbeatError extends Error {
  constructor() {
    super('Kafka transcript heartbeat failed');
    this.name = 'KafkaTranscriptHeartbeatError';
  }
}

/** Keep membership alive while a codec waiter performs bounded registry I/O.
 * Only the waiter is aborted, never the codec's shared schema request. */
export async function withHeartbeat<T>(
  heartbeat: () => Promise<void>,
  parentSignal: AbortSignal,
  assertOwned: () => void,
  operation: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  const waiter = new AbortController();
  const signal = AbortSignal.any([parentSignal, waiter.signal]);
  let onAbort!: () => void;
  const aborted = new Promise<never>((_resolve, reject) => {
    onAbort = () => reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    if (signal.aborted) onAbort();
  });
  const beat = async (): Promise<void> => {
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      signal.throwIfAborted();
      assertOwned();
      await Promise.race([
        Promise.resolve().then(heartbeat),
        new Promise<never>((_resolve, reject) => {
          timeout = setTimeout(
            () => reject(new KafkaTranscriptHeartbeatError()),
            HEARTBEAT_INTERVAL_MS,
          );
        }),
        aborted,
      ]);
      signal.throwIfAborted();
      assertOwned();
    } catch {
      throw new KafkaTranscriptHeartbeatError();
    } finally {
      clearTimeout(timeout);
    }
  };
  let interval: ReturnType<typeof setInterval> | undefined;
  let activeBeat: Promise<void> | undefined;
  try {
    await beat();
    interval = setInterval(() => {
      if (activeBeat) return;
      activeBeat = beat()
        .catch(() => {
          waiter.abort(new KafkaTranscriptHeartbeatError());
        })
        .finally(() => {
          activeBeat = undefined;
        });
    }, HEARTBEAT_INTERVAL_MS);
    const result = await Promise.race([operation(signal), aborted]);
    clearInterval(interval);
    await activeBeat;
    signal.throwIfAborted();
    await beat();
    return result;
  } finally {
    clearInterval(interval);
    waiter.abort();
    await activeBeat;
    signal.removeEventListener('abort', onAbort);
  }
}
