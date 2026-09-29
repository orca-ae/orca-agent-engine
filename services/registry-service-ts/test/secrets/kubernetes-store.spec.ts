// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { beforeEach, describe, expect, it, vi } from 'vitest';
import { PatchUtils } from '@kubernetes/client-node';
import {
  DEFAULT_KUBERNETES_SECRET_STORE_PATCH_TIMEOUT_MS,
  KubernetesSecretStore,
  kubernetesSecretStoreDataKey,
  type KubernetesSecretStoreApiFactory,
} from '../../src/secrets/kubernetes-store.js';

describe('KubernetesSecretStore', () => {
  const readNamespacedSecret = vi.fn();
  const patchNamespacedSecret = vi.fn();
  const addInterceptor = vi.fn();
  const apiFactory: KubernetesSecretStoreApiFactory = () => ({
    readNamespacedSecret,
    patchNamespacedSecret,
    addInterceptor,
  });
  let store: KubernetesSecretStore;

  beforeEach(() => {
    vi.clearAllMocks();
    store = new KubernetesSecretStore(
      { namespace: 'orca-system', secretName: 'registry-secret-store' },
      apiFactory,
    );
  });

  it('installs one bounded raw transport deadline on its dedicated API instance', () => {
    expect(addInterceptor).toHaveBeenCalledTimes(1);
    const requestOptions: { timeout?: number } = {};
    const interceptor = addInterceptor.mock.calls[0]?.[0] as (
      options: typeof requestOptions,
    ) => void;

    interceptor(requestOptions);

    expect(requestOptions.timeout).toBe(DEFAULT_KUBERNETES_SECRET_STORE_PATCH_TIMEOUT_MS);
    expect(DEFAULT_KUBERNETES_SECRET_STORE_PATCH_TIMEOUT_MS).toBeLessThan(45_000);
  });

  it.each([0, -1, 1.5, Number.POSITIVE_INFINITY])(
    'rejects non-positive or invalid patchTimeoutMs %s',
    (patchTimeoutMs) => {
      expect(
        () =>
          new KubernetesSecretStore(
            { namespace: 'orca-system', secretName: 'registry-secret-store', patchTimeoutMs },
            apiFactory,
          ),
      ).toThrow('Kubernetes SecretStore patchTimeoutMs must be a positive safe integer');
    },
  );

  it('stores each opaque reference under a deterministic hashed data key', async () => {
    patchNamespacedSecret.mockResolvedValueOnce({ body: {} });

    await expect(
      store.put('local:vault_credentials/ws/vcrd/access_token', 'secret'),
    ).resolves.toBeUndefined();

    const key = kubernetesSecretStoreDataKey('local:vault_credentials/ws/vcrd/access_token');
    expect(key).toMatch(/^sha256-[a-f0-9]{64}$/);
    expect(patchNamespacedSecret).toHaveBeenCalledWith(
      'registry-secret-store',
      'orca-system',
      { data: { [key]: Buffer.from('secret').toString('base64') } },
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      { headers: { 'Content-Type': PatchUtils.PATCH_FORMAT_JSON_MERGE_PATCH } },
    );
  });

  it('fails clearly if the backing Secret disappears while storing a value', async () => {
    patchNamespacedSecret.mockRejectedValueOnce({ body: { code: 404 } });

    await expect(store.put('ref', 'secret')).rejects.toThrow(
      'Kubernetes SecretStore secret orca-system/registry-secret-store does not exist',
    );
  });

  it('does not start a put when its signal was already aborted', async () => {
    const controller = new AbortController();
    const reason = new Error('staging deadline elapsed');
    controller.abort(reason);

    await expect(store.put('ref', 'secret', { signal: controller.signal })).rejects.toBe(reason);
    expect(patchNamespacedSecret).not.toHaveBeenCalled();
  });

  it('stops awaiting a client-node put when its signal aborts', async () => {
    let resolvePatch!: (value: { body: object }) => void;
    const patch = new Promise<{ body: object }>((resolve) => {
      resolvePatch = resolve;
    });
    patchNamespacedSecret.mockReturnValueOnce(patch);
    const controller = new AbortController();
    const reason = new Error('staging deadline elapsed');

    const put = store.put('ref', 'secret', { signal: controller.signal });
    controller.abort(reason);

    await expect(put).rejects.toBe(reason);
    expect(patchNamespacedSecret).toHaveBeenCalledTimes(1);
    resolvePatch({ body: {} });
    await Promise.resolve();
  });

  it('resolves and decodes a stored value', async () => {
    const key = kubernetesSecretStoreDataKey('ref');
    readNamespacedSecret.mockResolvedValueOnce({
      body: { data: { [key]: Buffer.from('decoded-value').toString('base64') } },
    });

    await expect(store.resolve('ref')).resolves.toBe('decoded-value');
  });

  it('does not start a resolve when its signal was already aborted', async () => {
    const controller = new AbortController();
    const reason = new Error('resolution deadline elapsed');
    controller.abort(reason);

    await expect(store.resolve('ref', { signal: controller.signal })).rejects.toBe(reason);
    expect(readNamespacedSecret).not.toHaveBeenCalled();
  });

  it('does not return a resolve result after its signal aborts', async () => {
    let resolveRead!: (value: { body: { data: Record<string, string> } }) => void;
    const read = new Promise<{ body: { data: Record<string, string> } }>((resolve) => {
      resolveRead = resolve;
    });
    const key = kubernetesSecretStoreDataKey('ref');
    readNamespacedSecret.mockReturnValueOnce(read);
    const controller = new AbortController();
    const reason = new Error('resolution deadline elapsed');

    const resolution = store.resolve('ref', { signal: controller.signal });
    controller.abort(reason);
    resolveRead({ body: { data: { [key]: Buffer.from('decoded-value').toString('base64') } } });

    await expect(resolution).rejects.toBe(reason);
  });

  it('returns null for a missing key', async () => {
    readNamespacedSecret.mockResolvedValueOnce({ body: { data: {} } });
    await expect(store.resolve('missing-key')).resolves.toBeNull();
  });

  it('fails clearly if the backing Secret disappears at runtime', async () => {
    readNamespacedSecret.mockRejectedValueOnce({ statusCode: 404 });
    await expect(store.resolve('missing-secret')).rejects.toThrow(
      'Kubernetes SecretStore secret orca-system/registry-secret-store does not exist',
    );
  });

  it('deletes only the hashed data key through merge patch', async () => {
    patchNamespacedSecret.mockResolvedValueOnce({ body: {} });
    const key = kubernetesSecretStoreDataKey('ref');

    await store.delete('ref');

    expect(patchNamespacedSecret).toHaveBeenCalledWith(
      'registry-secret-store',
      'orca-system',
      { data: { [key]: null } },
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      { headers: { 'Content-Type': PatchUtils.PATCH_FORMAT_JSON_MERGE_PATCH } },
    );
  });

  it('fails clearly if the backing Secret disappears while deleting a value', async () => {
    patchNamespacedSecret.mockRejectedValueOnce({ statusCode: 404 });

    await expect(store.delete('ref')).rejects.toThrow(
      'Kubernetes SecretStore secret orca-system/registry-secret-store does not exist',
    );
  });

  it('does not start a delete when its signal was already aborted', async () => {
    const controller = new AbortController();
    const reason = new Error('cleanup deadline elapsed');
    controller.abort(reason);

    await expect(store.delete('ref', { signal: controller.signal })).rejects.toBe(reason);
    expect(patchNamespacedSecret).not.toHaveBeenCalled();
  });

  it('stops awaiting a client-node delete when its signal aborts', async () => {
    let resolvePatch!: (value: { body: object }) => void;
    const patch = new Promise<{ body: object }>((resolve) => {
      resolvePatch = resolve;
    });
    patchNamespacedSecret.mockReturnValueOnce(patch);
    const controller = new AbortController();
    const reason = new Error('cleanup deadline elapsed');

    const deletion = store.delete('ref', { signal: controller.signal });
    controller.abort(reason);

    await expect(deletion).rejects.toBe(reason);
    expect(patchNamespacedSecret).toHaveBeenCalledTimes(1);
    resolvePatch({ body: {} });
    await Promise.resolve();
  });

  it('shares a late delete patch across aborted retries until it settles', async () => {
    let resolvePatch!: (value: { body: object }) => void;
    const patch = new Promise<{ body: object }>((resolve) => {
      resolvePatch = resolve;
    });
    patchNamespacedSecret.mockReturnValueOnce(patch);
    const firstController = new AbortController();
    const secondController = new AbortController();

    const first = store.delete('shared-ref', { signal: firstController.signal });
    firstController.abort(new Error('first cleanup deadline elapsed'));
    await expect(first).rejects.toThrow('first cleanup deadline elapsed');

    const second = store.delete('shared-ref', { signal: secondController.signal });
    secondController.abort(new Error('second cleanup deadline elapsed'));
    await expect(second).rejects.toThrow('second cleanup deadline elapsed');
    expect(patchNamespacedSecret).toHaveBeenCalledTimes(1);

    resolvePatch({ body: {} });
    await Promise.resolve();
    await Promise.resolve();

    patchNamespacedSecret.mockResolvedValueOnce({ body: {} });
    await expect(store.delete('shared-ref')).resolves.toBeUndefined();
    expect(patchNamespacedSecret).toHaveBeenCalledTimes(2);
  });

  it('retries after a never-completing raw delete patch hits its transport deadline', async () => {
    vi.useFakeTimers();
    try {
      const patchTimeoutMs = 25;
      const interceptors: Array<(requestOptions: { timeout?: number }) => void> = [];
      const boundedPatch = vi.fn(async () => {
        const requestOptions: { timeout?: number } = {};
        for (const interceptor of interceptors) interceptor(requestOptions);
        const timeout = requestOptions.timeout;
        if (timeout === undefined) throw new Error('raw request received no transport timeout');
        return new Promise<never>((_resolve, reject) => {
          setTimeout(() => reject(new Error('simulated raw request timeout')), timeout);
        });
      });
      const boundedApiFactory: KubernetesSecretStoreApiFactory = () => ({
        readNamespacedSecret,
        patchNamespacedSecret: boundedPatch,
        addInterceptor: (interceptor) => {
          interceptors.push(interceptor as (requestOptions: { timeout?: number }) => void);
        },
      });
      const boundedStore = new KubernetesSecretStore(
        { namespace: 'orca-system', secretName: 'registry-secret-store', patchTimeoutMs },
        boundedApiFactory,
      );

      const first = boundedStore.delete('timed-out-ref');
      const firstRejected = expect(first).rejects.toThrow('simulated raw request timeout');
      await vi.advanceTimersByTimeAsync(patchTimeoutMs);
      await firstRejected;
      expect(boundedPatch).toHaveBeenCalledTimes(1);

      const second = boundedStore.delete('timed-out-ref');
      const secondRejected = expect(second).rejects.toThrow('simulated raw request timeout');
      await vi.advanceTimersByTimeAsync(patchTimeoutMs);
      await secondRejected;
      expect(boundedPatch).toHaveBeenCalledTimes(2);
      expect(interceptors).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('fails startup clearly when the backing Secret does not exist', async () => {
    readNamespacedSecret.mockRejectedValueOnce({ response: { statusCode: 404 } });

    await expect(store.assertReady()).rejects.toThrow(
      'Kubernetes SecretStore secret orca-system/registry-secret-store does not exist',
    );
  });
});
