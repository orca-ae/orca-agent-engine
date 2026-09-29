// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { CoreV1Api, KubeConfig, PatchUtils } from '@kubernetes/client-node';
import type {
  SecretResolveOptions,
  SecretStore,
  SecretStoreDeleteOptions,
  SecretStorePutOptions,
} from './secret-provider.js';

type KubernetesSecretStoreApi = Pick<
  CoreV1Api,
  'addInterceptor' | 'readNamespacedSecret' | 'patchNamespacedSecret'
>;

export type KubernetesSecretStoreApiFactory = () => KubernetesSecretStoreApi;

export interface KubernetesSecretStoreOptions {
  namespace: string;
  secretName: string;
  /** Raw client-node request deadline, independent of a caller's wait signal. */
  patchTimeoutMs?: number;
}

/** Below the persistence kernel's 45-second SecretStore wait by default. */
export const DEFAULT_KUBERNETES_SECRET_STORE_PATCH_TIMEOUT_MS = 30_000;

const PATCH_OPTIONS = {
  headers: { 'Content-Type': PatchUtils.PATCH_FORMAT_JSON_MERGE_PATCH },
};

export function kubernetesSecretStoreDataKey(reference: string): string {
  return `sha256-${createHash('sha256').update(reference).digest('hex')}`;
}

export class KubernetesSecretStore implements SecretStore {
  private readonly api: KubernetesSecretStoreApi;
  private readonly patchTimeoutMs: number;

  /**
   * Keep one bounded raw delete request per opaque reference until it settles
   * so timeout-driven reconciliation retries do not accumulate patches against
   * an unavailable API server.
   */
  private readonly inFlightDeletes = new Map<string, Promise<void>>();

  constructor(
    private readonly options: KubernetesSecretStoreOptions,
    apiFactory: KubernetesSecretStoreApiFactory,
  ) {
    this.patchTimeoutMs = validatePatchTimeoutMs(options.patchTimeoutMs);
    // client-node only carries arbitrary request options through interceptors.
    // Keep this API instance private to one store so its transport deadline
    // cannot alter unrelated Kubernetes traffic.
    this.api = apiFactory();
    this.api.addInterceptor((requestOptions) => {
      requestOptions.timeout = this.patchTimeoutMs;
    });
  }

  static systemDefault(options: KubernetesSecretStoreOptions): KubernetesSecretStore {
    const kubeConfig = new KubeConfig();
    kubeConfig.loadFromDefault();
    const api = kubeConfig.makeApiClient(CoreV1Api);
    return new KubernetesSecretStore(options, () => api);
  }

  async assertReady(): Promise<void> {
    try {
      await this.api.readNamespacedSecret(this.options.secretName, this.options.namespace);
    } catch (error) {
      if (hasStatusCode(error, 404)) {
        throw this.backingSecretMissingError(error);
      }
      throw error;
    }
  }

  async put(reference: string, value: string, options?: SecretStorePutOptions): Promise<void> {
    throwIfAborted(options?.signal, 'put');
    // client-node 0.22 exposes no AbortSignal hook for this request. Racing
    // stops local waiting only; a late put patch can still write the key after
    // a caller records failure, so staging retains a durable cleanup tombstone.
    await raceWithAbort(
      this.patchData(reference, Buffer.from(value, 'utf8').toString('base64')),
      options?.signal,
      'put',
    );
  }

  async resolve(reference: string, options?: SecretResolveOptions): Promise<string | null> {
    options?.signal?.throwIfAborted();
    let result;
    try {
      // client-node 0.22 exposes no AbortSignal hook for reads, so passing a
      // signal through its generated request options would be ineffective.
      result = await this.api.readNamespacedSecret(this.options.secretName, this.options.namespace);
    } catch (error) {
      options?.signal?.throwIfAborted();
      if (hasStatusCode(error, 404)) throw this.backingSecretMissingError(error);
      throw error;
    }
    options?.signal?.throwIfAborted();
    const value = result.body.data?.[kubernetesSecretStoreDataKey(reference)];
    return value === undefined ? null : Buffer.from(value, 'base64').toString('utf8');
  }

  async delete(reference: string, options?: SecretStoreDeleteOptions): Promise<void> {
    throwIfAborted(options?.signal, 'delete');
    // Racing cannot cancel client-node's request. A late delete patch can
    // remove the key after this call reports cancellation, so reconcilers
    // retain durable failed work and retry rather than treating abort as delete.
    // Repeated retries share that raw request until it settles.
    await raceWithAbort(this.deleteDataOnce(reference), options?.signal, 'delete');
  }

  private deleteDataOnce(reference: string): Promise<void> {
    const existing = this.inFlightDeletes.get(reference);
    if (existing !== undefined) return existing;

    const operation = this.patchData(reference, null);
    this.inFlightDeletes.set(reference, operation);
    const clear = () => {
      if (this.inFlightDeletes.get(reference) === operation) {
        this.inFlightDeletes.delete(reference);
      }
    };
    // Explicit handlers retain a late rejection after all waiting callers
    // abandoned their races.
    void operation.then(clear, clear);
    return operation;
  }

  private async patchData(reference: string, value: string | null): Promise<void> {
    try {
      await this.api.patchNamespacedSecret(
        this.options.secretName,
        this.options.namespace,
        { data: { [kubernetesSecretStoreDataKey(reference)]: value } },
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        PATCH_OPTIONS,
      );
    } catch (error) {
      if (hasStatusCode(error, 404)) throw this.backingSecretMissingError(error);
      throw error;
    }
  }

  private backingSecretMissingError(cause: unknown): Error {
    return new Error(
      `Kubernetes SecretStore secret ${this.options.namespace}/${this.options.secretName} does not exist`,
      { cause },
    );
  }
}

function hasStatusCode(error: unknown, expected: number): boolean {
  if (typeof error !== 'object' || error === null) return false;
  const candidate = error as {
    statusCode?: unknown;
    response?: { statusCode?: unknown };
    body?: { code?: unknown };
  };
  return (
    candidate.statusCode === expected ||
    candidate.response?.statusCode === expected ||
    candidate.body?.code === expected
  );
}

function validatePatchTimeoutMs(value: number | undefined): number {
  const timeoutMs = value ?? DEFAULT_KUBERNETES_SECRET_STORE_PATCH_TIMEOUT_MS;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
    throw new Error('Kubernetes SecretStore patchTimeoutMs must be a positive safe integer');
  }
  return timeoutMs;
}

function throwIfAborted(signal: AbortSignal | undefined, operation: 'put' | 'delete'): void {
  if (!signal?.aborted) return;
  throw abortReason(signal, operation);
}

function abortReason(signal: AbortSignal, operation: 'put' | 'delete'): unknown {
  return signal.reason ?? new Error(`SecretStore ${operation} aborted`);
}

function raceWithAbort<T>(
  operation: Promise<T>,
  signal: AbortSignal | undefined,
  operationName: 'put' | 'delete',
): Promise<T> {
  if (signal === undefined) return operation;
  throwIfAborted(signal, operationName);
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      cleanup();
      reject(abortReason(signal, operationName));
    };
    const cleanup = () => signal.removeEventListener('abort', onAbort);
    signal.addEventListener('abort', onAbort, { once: true });
    operation.then(
      (value) => {
        cleanup();
        resolve(value);
      },
      (error: unknown) => {
        cleanup();
        reject(error);
      },
    );
  });
}
