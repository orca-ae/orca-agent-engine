// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { LocalSecretStore } from '../../src/secrets/local-store.js';

describe('LocalSecretStore', () => {
  it('writes only at caller-owned reference and returns no replacement', async () => {
    const store = new LocalSecretStore();

    await expect(store.put('local:caller-owned', 'secret')).resolves.toBeUndefined();
    await expect(store.resolve('local:caller-owned')).resolves.toBe('secret');
  });

  it('does not write when its signal was already aborted', async () => {
    const store = new LocalSecretStore();
    const controller = new AbortController();
    const reason = new Error('staging deadline elapsed');
    controller.abort(reason);

    await expect(
      store.put('local:caller-owned', 'secret', { signal: controller.signal }),
    ).rejects.toBe(reason);
    await expect(store.resolve('local:caller-owned')).resolves.toBeNull();
  });

  it('does not resolve when its signal was already aborted', async () => {
    const store = new LocalSecretStore();
    const controller = new AbortController();
    const reason = new Error('resolution deadline elapsed');
    await store.put('local:caller-owned', 'secret');
    controller.abort(reason);

    await expect(store.resolve('local:caller-owned', { signal: controller.signal })).rejects.toBe(
      reason,
    );
  });

  it('does not delete when its signal was already aborted', async () => {
    const store = new LocalSecretStore();
    const controller = new AbortController();
    const reason = new Error('cleanup deadline elapsed');
    await store.put('local:caller-owned', 'secret');
    controller.abort(reason);

    await expect(store.delete('local:caller-owned', { signal: controller.signal })).rejects.toBe(
      reason,
    );
    await expect(store.resolve('local:caller-owned')).resolves.toBe('secret');
  });
});
