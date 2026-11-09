// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import Fastify from 'fastify';
import {
  canonicalPolicy,
  egressRuleBounded,
  egressSelectorMatches,
  kubernetesNetworkEnforcement,
} from '../../src/domain/sandbox-network-enforcement.js';
import { kubernetesSandboxNetworkAuthority } from '../../src/domain/sandbox-network-authority.js';
import { buildInternalAuth, internalRouteCallers } from '../../src/auth/internal-auth.js';

const registration = { binding_id: 'snb_fixture', generation: 'generation' };
const annotations = {
  'orca.ai/network-binding-id': registration.binding_id,
  'orca.ai/network-generation': registration.generation,
};
const labels = { 'orca.ai/sandbox-egress': 'enabled' };
const pod = () => ({
  metadata: {
    namespace: 'sandbox',
    name: 'pod',
    uid: 'pod-uid',
    annotations: { ...annotations },
    labels,
    ownerReferences: [
      {
        apiVersion: 'sandbox.opensandbox.io/v1alpha1',
        kind: 'BatchSandbox',
        name: 'batch',
        uid: 'batch-uid',
      },
    ],
  },
  spec: {
    runtimeClassName: 'gvisor',
    automountServiceAccountToken: false,
    serviceAccountName: 'bootstrap',
    containers: [],
  },
  status: { phase: 'Pending', conditions: [{ type: 'Ready', status: 'False' }] },
});
function identityFixture() {
  const actualPod = pod();
  const batch = {
    metadata: {
      namespace: 'sandbox',
      name: 'batch',
      uid: 'batch-uid',
      annotations: { ...annotations },
    },
    spec: { replicas: 1, template: { metadata: { annotations: { ...annotations } } } },
  };
  const review = vi.fn(async () => ({
    authenticated: true,
    username: 'system:serviceaccount:sandbox:bootstrap',
    audiences: ['orca-sandbox-network'],
    extra: {
      'authentication.kubernetes.io/pod-name': ['pod'],
      'authentication.kubernetes.io/pod-uid': ['pod-uid'],
    },
  }));
  const enforce = vi.fn(async () => true);
  const core = {
    readNamespacedPod: vi.fn(async () => ({ body: actualPod })),
    listNamespacedPod: vi.fn(async () => ({ body: { items: [actualPod], metadata: {} } })),
  };
  const custom = { getNamespacedCustomObject: vi.fn(async () => ({ body: batch })) };
  const authority = kubernetesSandboxNetworkAuthority('sandbox', 'a'.repeat(64), {
    core,
    custom,
    review,
    enforce,
  } as unknown as Parameters<typeof kubernetesSandboxNetworkAuthority>[2]);
  return { actualPod, batch, review, enforce, core, custom, authority };
}
function enforcementFixture() {
  const allowed = {
    toEndpoints: [
      { matchLabels: { 'k8s:app': 'gateway', 'k8s:io.kubernetes.pod.namespace': 'orca' } },
    ],
    toPorts: [{ ports: [{ port: '8443', protocol: 'TCP' }] }],
  };
  const spec = { endpointSelector: { matchLabels: labels }, egress: [allowed] };
  const cep = {
    metadata: { ownerReferences: [{ kind: 'Pod', uid: 'pod-uid' }] },
    status: { state: 'ready', policy: { egress: { enforcing: true, state: 'enforcing' } } },
  };
  const custom = {
    getNamespacedCustomObject: vi.fn(
      async (_group: string, _version: string, _namespace: string, plural: string) => ({
        body: plural === 'ciliumendpoints' ? cep : { spec },
      }),
    ),
    listNamespacedCustomObject: vi.fn(async () => ({ body: { items: [{ spec }], metadata: {} } })),
    listClusterCustomObject: vi.fn(async () => ({
      body: { items: [] as unknown[], metadata: {} },
    })),
  };
  const networking = {
    listNamespacedNetworkPolicy: vi.fn(async () => ({
      body: { items: [] as unknown[], metadata: {} },
    })),
  };
  const digest = createHash('sha256').update(canonicalPolicy(spec)).digest('hex');
  const enforce = kubernetesNetworkEnforcement(digest, {
    custom,
    networking,
  } as unknown as Parameters<typeof kubernetesNetworkEnforcement>[1]);
  return { allowed, spec, cep, custom, networking, enforce };
}
describe('Sandbox network bootstrap authority', () => {
  it('rejects ambiguous Pods under the same authoritative workload', async () => {
    const f = identityFixture();
    f.core.listNamespacedPod.mockResolvedValue({
      body: { items: [f.actualPod, f.actualPod], metadata: {} },
    });
    await expect(f.authority.bootstrap('projected-token', registration)).rejects.toThrow(
      'ambiguous',
    );
  });
  it('authenticates an init Pod before Ready using authoritative owner and token binding', async () => {
    const f = identityFixture();
    await expect(f.authority.bootstrap('projected-token', registration)).resolves.toEqual({
      pod: { namespace: 'sandbox', name: 'pod', uid: 'pod-uid' },
      workload: { namespace: 'sandbox', name: 'batch', uid: 'batch-uid' },
    });
    expect(f.review).toHaveBeenCalledWith('projected-token', 'orca-sandbox-network');
    expect(f.enforce).toHaveBeenCalledTimes(1);
    await expect(
      f.authority.ownerLive({ namespace: 'sandbox', name: 'pod', uid: 'pod-uid' }),
    ).resolves.toBe(false);
  });
  it.each([
    'token-pod',
    'service-account',
    'audience',
    'owner',
    'workload-annotation',
    'template-annotation',
    'pod-annotation',
    'enforcement',
  ])('fails closed on %s mismatch', async (kind) => {
    const f = identityFixture();
    if (kind === 'token-pod') f.actualPod.metadata.uid = 'replacement';
    if (kind === 'service-account') f.actualPod.spec.serviceAccountName = 'other';
    if (kind === 'audience')
      f.review.mockResolvedValue({ ...(await f.review()), audiences: ['other'] });
    if (kind === 'owner') f.batch.metadata.uid = 'replacement';
    if (kind === 'workload-annotation')
      f.batch.metadata.annotations['orca.ai/network-generation'] = 'replacement';
    if (kind === 'template-annotation')
      f.batch.spec.template.metadata.annotations['orca.ai/network-generation'] = 'replacement';
    if (kind === 'pod-annotation')
      f.actualPod.metadata.annotations['orca.ai/network-generation'] = 'replacement';
    if (kind === 'enforcement') f.enforce.mockResolvedValue(false);
    await expect(f.authority.bootstrap('projected-token', registration)).rejects.toThrow();
  });
  it('has an exact bootstrap auth exception with no general sandbox principal', async () => {
    const app = Fastify();
    const verifier = { verify: vi.fn(async () => null) };
    app.addHook('preHandler', buildInternalAuth(verifier, true));
    app.post('/internal/v1/sandbox-networks/:id/bootstrap', async () => ({
      routeAuthRequired: true,
    }));
    app.post('/internal/v1/sandbox-egress/authorize', async () => ({}));
    const route = '/internal/v1/sandbox-networks/snb/bootstrap';
    expect((await app.inject({ method: 'POST', url: route })).statusCode).toBe(200);
    expect(verifier.verify).not.toHaveBeenCalled();
    expect(
      (
        await app.inject({
          method: 'POST',
          url: '/internal/v1/sandbox-egress/authorize',
          headers: { authorization: 'Bearer projected-token' },
        })
      ).statusCode,
    ).toBe(401);
    expect(
      (
        await app.inject({
          method: 'GET',
          url: route,
          headers: { authorization: 'Bearer projected-token' },
        })
      ).statusCode,
    ).toBe(401);
    expect(internalRouteCallers(route)).toBeNull();
    expect(internalRouteCallers('/internal/v1/sandbox-egress/resolve')).toEqual(['ai-gateway']);
    expect(
      internalRouteCallers('/internal/v1/workspaces/w/sessions/s/sandbox-egress/resolve'),
    ).toBeNull();
    await app.close();
  });
});
describe('authoritative Cilium policy union', () => {
  it('requires exact CEP UID/state/enforcing and does not cache positive results', async () => {
    const f = enforcementFixture();
    await expect(f.enforce(pod())).resolves.toBe(true);
    f.cep.metadata.ownerReferences[0]!.uid = 'replaced';
    await expect(f.enforce(pod())).resolves.toBe(false);
    f.cep.metadata.ownerReferences[0]!.uid = 'pod-uid';
    f.cep.status.policy.egress.enforcing = false;
    await expect(f.enforce(pod())).resolves.toBe(false);
    f.cep.status.policy.egress.enforcing = true;
    f.cep.status.policy.egress.state = 'waiting';
    await expect(f.enforce(pod())).resolves.toBe(false);
  });
  it('checks trusted spec digest on every request', async () => {
    const f = enforcementFixture();
    f.spec.egress[0]!.toPorts[0]!.ports[0]!.port = '443';
    await expect(f.enforce(pod())).resolves.toBe(false);
  });
  it('rejects a clusterwide allow that widens the expected union', async () => {
    const f = enforcementFixture();
    f.custom.listClusterCustomObject.mockResolvedValue({
      body: {
        items: [{ spec: { endpointSelector: {}, egress: [{ toEntities: ['world'] }] } }],
        metadata: {},
      },
    });
    await expect(f.enforce(pod())).resolves.toBe(false);
  });
  it('rejects a matching Kubernetes NetworkPolicy widening the union', async () => {
    const f = enforcementFixture();
    f.networking.listNamespacedNetworkPolicy.mockResolvedValue({
      body: {
        items: [
          {
            spec: {
              podSelector: {},
              egress: [
                {
                  to: [
                    {
                      namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': 'orca' } },
                      podSelector: { matchLabels: { app: 'gateway' } },
                    },
                  ],
                  ports: [{ port: 443 }],
                },
              ],
            },
          },
        ],
        metadata: {},
      },
    });
    await expect(f.enforce(pod())).resolves.toBe(false);
  });
  it('accepts only a subset endpoint/port rule, never CIDR/entity/service/L7 grants', () => {
    const f = enforcementFixture();
    expect(egressRuleBounded(f.allowed, [f.allowed])).toBe(true);
    for (const field of ['toCIDR', 'toEntities', 'toServices', 'rules'])
      expect(egressRuleBounded({ ...f.allowed, [field]: [] }, [f.allowed])).toBe(false);
    expect(egressRuleBounded({ toEndpoints: [{}], toPorts: f.allowed.toPorts }, [f.allowed])).toBe(
      false,
    );
  });
  it('supports selector expressions and refuses inferred virtual identities', () => {
    expect(
      egressSelectorMatches(
        { matchExpressions: [{ key: 'toString', operator: 'DoesNotExist' }] },
        {},
      ),
    ).toBe(true);
    expect(
      egressSelectorMatches({ matchExpressions: [{ key: 'toString', operator: 'Exists' }] }, {}),
    ).toBe(false);
    expect(
      egressSelectorMatches(
        { matchExpressions: [{ key: 'k8s:app', operator: 'In', values: ['gateway'] }] },
        { app: 'gateway' },
      ),
    ).toBe(true);
    expect(
      egressSelectorMatches(
        { matchExpressions: [{ key: 'app', operator: 'NotIn', values: ['gateway'] }] },
        {},
      ),
    ).toBe(true);
    expect(() => egressSelectorMatches({ matchLabels: { 'reserved:world': '' } }, {})).toThrow();
    expect(() =>
      egressSelectorMatches({ matchLabels: { 'io.cilium.k8s.policy.serviceaccount': '' } }, {}),
    ).toThrow();
  });
});
