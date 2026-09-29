// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  createControlledShutdown,
  monitorEventSourceFailure,
} from '../../src/controlled-shutdown.js';

describe('controlled shutdown', () => {
  it('runs one cleanup and lets a terminal source failure escalate exit code to 1', async () => {
    let releaseCleanup!: () => void;
    const cleanupGate = new Promise<void>((resolve) => {
      releaseCleanup = resolve;
    });
    const cleanup = vi.fn(async () => await cleanupGate);
    const exit = vi.fn();
    const sourceFailure = deferred<void>();
    const onFailure = vi.fn();
    const controller = createControlledShutdown({ cleanup, exit });

    monitorEventSourceFailure(
      { whenFailed: () => sourceFailure.promise },
      controller.shutdown,
      onFailure,
    );
    const signalShutdown = controller.shutdown(0);
    sourceFailure.resolve();
    await vi.waitFor(() => expect(onFailure).toHaveBeenCalledOnce());

    releaseCleanup();
    await signalShutdown;

    expect(cleanup).toHaveBeenCalledOnce();
    expect(exit).toHaveBeenCalledWith(1);
  });
});

function deferred<T>(): { promise: Promise<T>; resolve(value: T): void } {
  let resolvePromise: (value: T) => void = () => {};
  const promise = new Promise<T>((resolve) => {
    resolvePromise = resolve;
  });
  return { promise, resolve: resolvePromise };
}
