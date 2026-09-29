// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type {
  SecretResolveOptions,
  SecretStore,
  SecretStoreDeleteOptions,
  SecretStorePutOptions,
} from './secret-provider.js';

export class LocalSecretStore implements SecretStore {
  private readonly values = new Map<string, string>();

  async put(reference: string, value: string, options?: SecretStorePutOptions): Promise<void> {
    throwIfAborted(options?.signal);
    this.values.set(reference, value);
  }

  async resolve(reference: string, options?: SecretResolveOptions): Promise<string | null> {
    options?.signal?.throwIfAborted();
    const value = this.values.get(reference) ?? null;
    options?.signal?.throwIfAborted();
    return value;
  }

  async delete(reference: string, options?: SecretStoreDeleteOptions): Promise<void> {
    throwIfAborted(options?.signal, 'delete');
    this.values.delete(reference);
  }
}

function throwIfAborted(
  signal: AbortSignal | undefined,
  operation: 'put' | 'delete' = 'put',
): void {
  if (!signal?.aborted) return;
  throw signal.reason ?? new Error(`SecretStore ${operation} aborted`);
}
