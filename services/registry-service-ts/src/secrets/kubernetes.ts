// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { CoreV1Api, KubeConfig } from '@kubernetes/client-node';
import type { SecretProvider } from './secret-provider.js';

const SCHEME = /^k8s:\/\/([^/]+)\/([^/]+)\/(.+)$/;

export function k8sIsSupported(reference: string): boolean {
  return SCHEME.test(reference);
}

export type K8sCoreApiFactory = () => CoreV1Api;

export class KubernetesSecretProvider implements SecretProvider {
  constructor(private readonly apiFactory: K8sCoreApiFactory) {}

  static systemDefault(): KubernetesSecretProvider {
    const kc = new KubeConfig();
    kc.loadFromDefault();
    return new KubernetesSecretProvider(() => kc.makeApiClient(CoreV1Api));
  }

  async resolve(reference: string): Promise<string | null> {
    const m = SCHEME.exec(reference);
    if (!m) return null;
    const [, namespace, name, key] = m;
    const api = this.apiFactory();
    const result = await api.readNamespacedSecret(name!, namespace!);
    const data = result.body.data?.[key!];
    if (!data) return null;
    return Buffer.from(data, 'base64').toString('utf8');
  }
}
