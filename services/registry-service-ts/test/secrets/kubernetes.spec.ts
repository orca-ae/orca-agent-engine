// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, vi } from 'vitest';
import type { K8sCoreApiFactory } from '../../src/secrets/kubernetes.js';
import { KubernetesSecretProvider, k8sIsSupported } from '../../src/secrets/kubernetes.js';

describe('KubernetesSecretProvider', () => {
  const coreApi = { readNamespacedSecret: vi.fn() };
  const factory: K8sCoreApiFactory = () => coreApi as unknown as ReturnType<K8sCoreApiFactory>;
  const p = new KubernetesSecretProvider(factory);

  it('isSupported recognizes k8s://', () => {
    expect(k8sIsSupported('k8s://default/my-secret/api-key')).toBe(true);
    expect(k8sIsSupported('s3://bad')).toBe(false);
  });

  it('reads a key from a namespaced secret (base64-decoded)', async () => {
    coreApi.readNamespacedSecret.mockResolvedValueOnce({
      body: { data: { 'api-key': Buffer.from('decoded-value', 'utf8').toString('base64') } },
    });
    expect(await p.resolve('k8s://default/my-secret/api-key')).toBe('decoded-value');
    expect(coreApi.readNamespacedSecret).toHaveBeenCalledWith('my-secret', 'default');
  });

  it('returns null if key is missing', async () => {
    coreApi.readNamespacedSecret.mockResolvedValueOnce({ body: { data: {} } });
    expect(await p.resolve('k8s://default/my-secret/missing')).toBeNull();
  });
});
