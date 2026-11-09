// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { CoreV1Api, CustomObjectsApi, KubeConfig, type V1Pod } from '@kubernetes/client-node';
import type { EgressOwnerAuthority } from '../contracts/sandbox-egress.contract.js';

export type HarnessEgressAuthority = Extract<EgressOwnerAuthority, { kind: 'harness' }>;
export type EgressLivenessVerifier = (authority: HarnessEgressAuthority) => Promise<boolean>;
export type EgressSandboxLocator = (
  sandboxId: string,
) => Promise<HarnessEgressAuthority['sandbox_pod']>;

export function liveEgressPod(pod: V1Pod, uid: string): boolean {
  return (
    pod.metadata?.uid === uid &&
    !pod.metadata.deletionTimestamp &&
    pod.status?.phase === 'Running' &&
    pod.status.conditions?.some(
      (condition) => condition.type === 'Ready' && condition.status === 'True',
    ) === true &&
    (pod.status.containerStatuses?.length ?? 0) > 0 &&
    pod.status.containerStatuses!.every(
      (container) => container.ready && !container.state?.terminated,
    )
  );
}

/** No positive cache: readiness is a liveness bound, not instant process-death detection. */
export function kubernetesEgressLivenessVerifier(
  api?: Pick<CoreV1Api, 'readNamespacedPod'>,
): EgressLivenessVerifier {
  if (!api) {
    const config = new KubeConfig();
    config.loadFromDefault();
    const client = config.makeApiClient(CoreV1Api);
    client.addInterceptor((request) => {
      request.timeout = 5000;
    });
    api = client;
  }
  const client = api;
  return async (authority) => {
    const pods = await Promise.all(
      [authority.owner_pod, authority.sandbox_pod].map(async (ref) => {
        const response = await client.readNamespacedPod(ref.name, ref.namespace);
        return liveEgressPod(response.body, ref.uid);
      }),
    );
    return pods.every(Boolean);
  };
}

/** Resolve only the managed gVisor BatchSandbox association in the configured namespace. */
export function kubernetesEgressSandboxLocator(
  namespace: string,
  clients?: {
    core: Pick<CoreV1Api, 'listNamespacedPod'>;
    custom: Pick<CustomObjectsApi, 'getNamespacedCustomObject'>;
  },
): EgressSandboxLocator {
  if (!clients) {
    const config = new KubeConfig();
    config.loadFromDefault();
    const core = config.makeApiClient(CoreV1Api);
    const custom = config.makeApiClient(CustomObjectsApi);
    core.addInterceptor((request) => {
      request.timeout = 5000;
    });
    custom.addInterceptor((request) => {
      request.timeout = 5000;
    });
    clients = { core, custom };
  }
  const { core, custom } = clients;
  return async (sandboxId) => {
    const { body } = await custom.getNamespacedCustomObject(
      'sandbox.opensandbox.io',
      'v1alpha1',
      namespace,
      'batchsandboxes',
      sandboxId,
    );
    const batch = body as {
      metadata?: { uid?: string; deletionTimestamp?: string };
      spec?: { expireTime?: string };
    };
    if (
      !batch.metadata?.uid ||
      batch.metadata.deletionTimestamp ||
      (batch.spec?.expireTime &&
        (!Number.isFinite(Date.parse(batch.spec.expireTime)) ||
          Date.parse(batch.spec.expireTime) <= Date.now()))
    )
      throw new Error('sandbox unavailable');
    const { body: pods } = await core.listNamespacedPod(
      namespace,
      undefined,
      undefined,
      undefined,
      undefined,
      'orca.ai/sandbox-egress=enabled',
      1000,
    );
    if (pods.metadata?._continue) throw new Error('sandbox lookup exceeded limit');
    const matches = pods.items.filter((pod) =>
      pod.metadata?.ownerReferences?.some(
        (owner) =>
          owner.kind === 'BatchSandbox' &&
          owner.name === sandboxId &&
          owner.uid === batch.metadata!.uid,
      ),
    );
    const pod = matches[0];
    if (
      matches.length !== 1 ||
      !pod?.metadata?.name ||
      !pod.metadata.uid ||
      pod.metadata.labels?.['orca.ai/sandbox-egress'] !== 'enabled' ||
      pod.spec?.runtimeClassName !== 'gvisor' ||
      !liveEgressPod(pod, pod.metadata.uid)
    )
      throw new Error('sandbox unavailable');
    return { namespace, name: pod.metadata.name, uid: pod.metadata.uid };
  };
}
