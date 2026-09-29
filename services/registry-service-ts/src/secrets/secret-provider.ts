// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export interface SecretResolveOptions {
  /** Best-effort cancellation for an in-flight secret resolution. */
  signal?: AbortSignal;
}

export interface SecretProvider {
  resolve(reference: string, options?: SecretResolveOptions): Promise<string | null>;
}

export interface SecretStorePutOptions {
  /** Best-effort cancellation for an in-flight SecretStore operation. */
  signal?: AbortSignal;
}

/** Delete accepts the same cancellation contract as put. */
export type SecretStoreDeleteOptions = SecretStorePutOptions;

export interface SecretStore extends SecretProvider {
  /**
   * Store value at caller-owned opaque reference. Implementations never mint
   * or return a replacement reference, so callers retain durable authority.
   */
  put(reference: string, value: string, options?: SecretStorePutOptions): Promise<void>;
  delete(reference: string, options?: SecretStoreDeleteOptions): Promise<void>;
}
