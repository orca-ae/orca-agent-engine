// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Request-local bounded reads, preserving input order and stopping new work after a failure/disconnect. */
export async function mapReadItems<Input, Output>(
  items: readonly Input[],
  concurrency: number,
  read: (item: Input) => Promise<Output>,
  signal?: AbortSignal,
): Promise<Output[]> {
  if (!Number.isSafeInteger(concurrency) || concurrency < 1)
    throw new Error('invalid read concurrency');
  const results: Output[] = new Array(items.length);
  let next = 0;
  let failed = false;
  let firstError: unknown;
  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, async () => {
      while (!failed && next < items.length) {
        const index = next++;
        try {
          signal?.throwIfAborted();
          results[index] = await read(items[index]!);
          signal?.throwIfAborted();
        } catch (error) {
          if (!failed) firstError = error;
          failed = true;
          // Keep this worker fulfilled so Promise.all drains its siblings.
          // The caller must not release admission while started I/O is running.
        }
      }
    }),
  );
  if (failed) throw firstError;
  signal?.throwIfAborted();
  return results;
}
