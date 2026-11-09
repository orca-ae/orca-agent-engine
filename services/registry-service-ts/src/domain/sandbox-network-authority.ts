// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  AuthenticationV1Api,
  CoreV1Api,
  CustomObjectsApi,
  KubeConfig,
  type V1Pod,
} from '@kubernetes/client-node';
import { z } from 'zod';
import {
  kubernetesTokenReviewerFromApi,
  type KubernetesTokenReviewer,
} from '../auth/internal-auth.js';
import { kubernetesNetworkEnforcement } from './sandbox-network-enforcement.js';
import { liveEgressPod } from './sandbox-egress-authority.js';

export const networkPod = z
  .object({ namespace: z.string().min(1), name: z.string().min(1), uid: z.string().min(1) })
  .strict();
export type NetworkPod = z.infer<typeof networkPod>;
export type NetworkWorkload = NetworkPod;
export interface BoundNetwork {
  pod: NetworkPod;
  workload: NetworkWorkload;
}
export interface NetworkRegistration {
  binding_id: string;
  generation: string;
}
export interface SandboxNetworkAuthority {
  bootstrap(token: string, registration: NetworkRegistration): Promise<BoundNetwork>;
  current(bound: BoundNetwork, registration: NetworkRegistration): Promise<boolean>;
  ownerLive(owner: NetworkPod): Promise<boolean>;
}
export class SandboxNetworkAuthenticationError extends Error {}
const batchSchema = z.object({
  metadata: z.object({
    uid: z.string(),
    name: z.string(),
    namespace: z.string(),
    deletionTimestamp: z.unknown().optional(),
    annotations: z.record(z.string()),
  }),
  spec: z.object({
    expireTime: z.string().optional(),
    replicas: z.literal(1),
    shardPatches: z.array(z.unknown()).optional(),
    template: z.object({ metadata: z.object({ annotations: z.record(z.string()) }) }),
  }),
});
export function kubernetesSandboxNetworkAuthority(
  namespace: string,
  policyDigest: string,
  clients?: {
    core: Pick<CoreV1Api, 'readNamespacedPod' | 'listNamespacedPod'>;
    custom: Pick<CustomObjectsApi, 'getNamespacedCustomObject'>;
    review: KubernetesTokenReviewer;
    enforce: (pod: V1Pod) => Promise<boolean>;
  },
): SandboxNetworkAuthority {
  if (!clients) {
    const config = new KubeConfig();
    config.loadFromDefault();
    const core = config.makeApiClient(CoreV1Api),
      custom = config.makeApiClient(CustomObjectsApi),
      auth = config.makeApiClient(AuthenticationV1Api);
    for (const api of [core, custom, auth])
      api.addInterceptor((request) => {
        request.timeout = 5000;
      });
    clients = {
      core,
      custom,
      review: kubernetesTokenReviewerFromApi(auth),
      enforce: kubernetesNetworkEnforcement(policyDigest),
    };
  }
  const { core, custom, review, enforce } = clients;
  const inspect = async (
    ref: NetworkPod,
    registration: NetworkRegistration,
  ): Promise<BoundNetwork> => {
    if (ref.namespace !== namespace) throw new Error('invalid network namespace');
    const pod = (await core.readNamespacedPod(ref.name, namespace)).body;
    if (
      pod.metadata?.uid !== ref.uid ||
      pod.metadata.deletionTimestamp ||
      !['Pending', 'Running'].includes(pod.status?.phase ?? '') ||
      pod.spec?.runtimeClassName !== 'gvisor' ||
      pod.spec.automountServiceAccountToken !== false
    )
      throw new Error('network Pod unavailable');
    const owners = pod.metadata.ownerReferences?.filter(
      (owner) =>
        owner.kind === 'BatchSandbox' && owner.apiVersion === 'sandbox.opensandbox.io/v1alpha1',
    );
    if (owners?.length !== 1) throw new Error('invalid network owner');
    const owner = owners[0]!;
    const batch = batchSchema.parse(
      (
        await custom.getNamespacedCustomObject(
          'sandbox.opensandbox.io',
          'v1alpha1',
          namespace,
          'batchsandboxes',
          owner.name,
        )
      ).body,
    );
    if (
      batch.metadata.uid !== owner.uid ||
      batch.metadata.name !== owner.name ||
      batch.metadata.namespace !== namespace ||
      batch.metadata.deletionTimestamp ||
      batch.spec.shardPatches?.length ||
      (batch.spec.expireTime &&
        (!Number.isFinite(Date.parse(batch.spec.expireTime)) ||
          Date.parse(batch.spec.expireTime) <= Date.now()))
    )
      throw new Error('network workload unavailable');
    const pods = (
      await core.listNamespacedPod(
        namespace,
        undefined,
        undefined,
        undefined,
        undefined,
        'orca.ai/sandbox-egress=enabled',
        1000,
      )
    ).body;
    if (pods.metadata?._continue) throw new Error('network Pod lookup exceeded limit');
    const associated = pods.items.filter((candidate) =>
      candidate.metadata?.ownerReferences?.some(
        (reference) =>
          reference.kind === 'BatchSandbox' &&
          reference.name === owner.name &&
          reference.uid === owner.uid,
      ),
    );
    if (
      associated.length !== 1 ||
      associated[0]?.metadata?.uid !== ref.uid ||
      associated[0].metadata.name !== ref.name
    )
      throw new Error('ambiguous network Pod association');
    for (const annotations of [
      batch.metadata.annotations,
      batch.spec.template.metadata.annotations,
      pod.metadata.annotations,
    ]) {
      if (
        annotations?.['orca.ai/network-binding-id'] !== registration.binding_id ||
        annotations?.['orca.ai/network-generation'] !== registration.generation
      )
        throw new Error('network generation mismatch');
    }
    if (!(await enforce(pod))) throw new Error('network enforcement unavailable');
    return { pod: ref, workload: { namespace, name: owner.name, uid: owner.uid } };
  };
  return {
    async bootstrap(token, registration) {
      const audience = 'orca-sandbox-network';
      const result = await review(token, audience);
      if (
        !result.authenticated ||
        result.audiences?.length !== 1 ||
        result.audiences[0] !== audience
      )
        throw new SandboxNetworkAuthenticationError('invalid bootstrap token');
      const names = result.extra?.['authentication.kubernetes.io/pod-name'],
        uids = result.extra?.['authentication.kubernetes.io/pod-uid'];
      if (names?.length !== 1 || uids?.length !== 1)
        throw new SandboxNetworkAuthenticationError('unbound bootstrap token');
      const ref = { namespace, name: names[0]!, uid: uids[0]! };
      const pod = (await core.readNamespacedPod(ref.name, namespace)).body;
      if (
        !pod.spec?.serviceAccountName ||
        result.username !== `system:serviceaccount:${namespace}:${pod.spec.serviceAccountName}` ||
        pod.metadata?.uid !== ref.uid
      )
        throw new SandboxNetworkAuthenticationError('bootstrap token identity mismatch');
      return inspect(ref, registration);
    },
    async current(bound, registration) {
      const actual = await inspect(bound.pod, registration);
      return (
        actual.workload.uid === bound.workload.uid &&
        actual.workload.name === bound.workload.name &&
        actual.workload.namespace === bound.workload.namespace
      );
    },
    async ownerLive(ref) {
      return liveEgressPod((await core.readNamespacedPod(ref.name, ref.namespace)).body, ref.uid);
    },
  };
}
