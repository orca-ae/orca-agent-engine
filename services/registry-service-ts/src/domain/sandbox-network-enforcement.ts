// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { CustomObjectsApi, NetworkingV1Api, KubeConfig, type V1Pod } from '@kubernetes/client-node';

type ObjectValue = Record<string, unknown>;
function object(value: unknown): ObjectValue {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('invalid policy');
  return value as ObjectValue;
}
function array(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error('invalid policy list');
  return value;
}
function keys(value: ObjectValue, allowed: string[], exact = false) {
  return (
    Object.keys(value).every((key) => allowed.includes(key)) &&
    (!exact || allowed.every((key) => key in value))
  );
}
/** Python sort_keys/separators canonicalization used by the trusted chart digest. */
export function canonicalPolicy(value: unknown): string {
  const encode = (value: unknown) =>
    JSON.stringify(value)
      .split('')
      .map((character) => {
        const code = character.charCodeAt(0);
        return code > 127
          ? String.fromCharCode(92) + 'u' + code.toString(16).padStart(4, '0')
          : character;
      })
      .join('');
  if (Array.isArray(value)) return '[' + value.map(canonicalPolicy).join(',') + ']';
  if (value && typeof value === 'object')
    return (
      '{' +
      Object.entries(object(value))
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([key, item]) => encode(key) + ':' + canonicalPolicy(item))
        .join(',') +
      '}'
    );
  return encode(value);
}
export function egressSelectorMatches(value: unknown, labels: Record<string, string>): boolean {
  const selector = object(value);
  if (!keys(selector, ['matchLabels', 'matchExpressions'])) throw new Error('unsupported selector');
  const keyName = (raw: string) => {
    const parts = raw.split(':');
    if (parts.length > 2 || (parts.length === 2 && !['k8s', 'any'].includes(parts[0]!)))
      throw new Error('unsupported label source');
    const key = parts.at(-1)!;
    if (key.startsWith('io.cilium.')) throw new Error('unknown Cilium identity');
    return key;
  };
  for (const [key, expected] of Object.entries(object(selector.matchLabels ?? {})))
    if (labels[keyName(key)] !== expected) return false;
  for (const raw of array(selector.matchExpressions ?? [])) {
    const expression = object(raw);
    if (typeof expression.key !== 'string') throw new Error('invalid expression');
    const key = keyName(expression.key);
    const values = array(expression.values ?? []);
    let matches: boolean;
    switch (expression.operator) {
      case 'In':
        matches = Object.hasOwn(labels, key) && values.includes(labels[key]);
        break;
      case 'NotIn':
        matches = !Object.hasOwn(labels, key) || !values.includes(labels[key]);
        break;
      case 'Exists':
        matches = Object.hasOwn(labels, key);
        break;
      case 'DoesNotExist':
        matches = !Object.hasOwn(labels, key);
        break;
      default:
        throw new Error('unsupported expression');
    }
    if (!matches) return false;
  }
  return true;
}
export function egressRuleBounded(value: unknown, expected: unknown[]): boolean {
  const rule = object(value);
  if (!keys(rule, ['toEndpoints', 'toPorts'], true)) return false;
  const peers = array(rule.toEndpoints),
    ports = array(rule.toPorts);
  if (!peers.length || !ports.length) return false;
  for (const rawPeer of peers) {
    const peer = object(rawPeer);
    if (!keys(peer, ['matchLabels'], true) || !Object.keys(object(peer.matchLabels)).length)
      return false;
    for (const rawPorts of ports) {
      const portGroup = object(rawPorts);
      if (!keys(portGroup, ['ports'], true) || !array(portGroup.ports).length) return false;
      for (const port of array(portGroup.ports)) {
        if (
          !expected.some((rawAllowed) => {
            const allowed = object(rawAllowed);
            return (
              array(allowed.toEndpoints).some((allowedPeer) =>
                Object.entries(object(object(allowedPeer).matchLabels)).every(
                  ([key, value]) => object(peer.matchLabels)[key] === value,
                ),
              ) &&
              array(allowed.toPorts).some((allowedPorts) =>
                array(object(allowedPorts).ports).some(
                  (allowedPort) => canonicalPolicy(port) === canonicalPolicy(allowedPort),
                ),
              )
            );
          })
        )
          return false;
      }
    }
  }
  return true;
}
function networkPolicyEgress(value: unknown, namespace: string): ObjectValue {
  const rule = object(value);
  if (!keys(rule, ['to', 'ports'], true)) throw new Error('unbounded NetworkPolicy');
  const peers = array(rule.to).map((raw) => {
    const peer = object(raw);
    if (!keys(peer, ['podSelector', 'namespaceSelector']) || !Object.keys(peer).length)
      throw new Error('unsupported peer');
    const pod = object(peer.podSelector ?? {});
    const ns = object(
      peer.namespaceSelector ?? { matchLabels: { 'kubernetes.io/metadata.name': namespace } },
    );
    if (
      !keys(pod, ['matchLabels']) ||
      !keys(ns, ['matchLabels'], true) ||
      !keys(object(ns.matchLabels), ['kubernetes.io/metadata.name'], true)
    )
      throw new Error('unsupported namespace selector');
    const labels: ObjectValue = {};
    for (const [key, value] of Object.entries(object(pod.matchLabels ?? {})))
      labels['k8s:' + key] = value;
    labels['k8s:io.kubernetes.pod.namespace'] = object(ns.matchLabels)[
      'kubernetes.io/metadata.name'
    ];
    return { matchLabels: labels };
  });
  const ports = array(rule.ports).map((raw) => {
    const port = object(raw);
    if (!keys(port, ['port', 'protocol']) || !Number.isInteger(port.port))
      throw new Error('unsupported port');
    return { port: String(port.port), protocol: port.protocol ?? 'TCP' };
  });
  return { toEndpoints: peers, toPorts: [{ ports }] };
}
function items(value: unknown): unknown[] {
  const response = object(value);
  if (object(response.metadata ?? {}).continue) throw new Error('policy list exceeded limit');
  return array(response.items);
}
export function kubernetesNetworkEnforcement(
  expectedDigest: string,
  clients?: {
    custom: Pick<
      CustomObjectsApi,
      'getNamespacedCustomObject' | 'listNamespacedCustomObject' | 'listClusterCustomObject'
    >;
    networking: Pick<NetworkingV1Api, 'listNamespacedNetworkPolicy'>;
  },
) {
  if (!/^[a-f0-9]{64}$/.test(expectedDigest)) throw new Error('invalid expected policy digest');
  if (!clients) {
    const config = new KubeConfig();
    config.loadFromDefault();
    const custom = config.makeApiClient(CustomObjectsApi),
      networking = config.makeApiClient(NetworkingV1Api);
    for (const api of [custom, networking])
      api.addInterceptor((request) => {
        request.timeout = 5000;
      });
    clients = { custom, networking };
  }
  const { custom, networking } = clients;
  return async (pod: V1Pod): Promise<boolean> => {
    const namespace = pod.metadata?.namespace,
      name = pod.metadata?.name,
      uid = pod.metadata?.uid;
    if (
      !namespace ||
      !name ||
      !uid ||
      pod.metadata?.deletionTimestamp ||
      pod.metadata?.labels?.['orca.ai/sandbox-egress'] !== 'enabled'
    )
      return false;
    const expected = object(
      (
        await custom.getNamespacedCustomObject(
          'cilium.io',
          'v2',
          namespace,
          'ciliumnetworkpolicies',
          'orca-sandbox-egress',
        )
      ).body,
    );
    const spec = object(expected.spec),
      expectedRules = array(spec.egress);
    if (
      createHash('sha256').update(canonicalPolicy(spec)).digest('hex') !== expectedDigest ||
      canonicalPolicy(spec.endpointSelector) !==
        canonicalPolicy({ matchLabels: { 'orca.ai/sandbox-egress': 'enabled' } }) ||
      !keys(spec, ['endpointSelector', 'egress'], true) ||
      !expectedRules.length ||
      expectedRules.some((rule) => !egressRuleBounded(rule, expectedRules))
    )
      return false;
    const cnp = items(
      (
        await custom.listNamespacedCustomObject(
          'cilium.io',
          'v2',
          namespace,
          'ciliumnetworkpolicies',
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          1000,
        )
      ).body,
    );
    const ccnp = items(
      (
        await custom.listClusterCustomObject(
          'cilium.io',
          'v2',
          'ciliumclusterwidenetworkpolicies',
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          1000,
        )
      ).body,
    );
    const labels = { ...pod.metadata?.labels, 'io.kubernetes.pod.namespace': namespace };
    for (const rawPolicy of [...cnp, ...ccnp]) {
      const policy = object(rawPolicy);
      for (const rawRule of [...array(policy.specs ?? []), ...(policy.spec ? [policy.spec] : [])]) {
        const rule = object(rawRule);
        if ('nodeSelector' in rule && !('endpointSelector' in rule)) continue;
        if (!('endpointSelector' in rule)) throw new Error('unknown effective policy');
        if (
          egressSelectorMatches(rule.endpointSelector, labels) &&
          array(rule.egress ?? []).some((allow) => !egressRuleBounded(allow, expectedRules))
        )
          return false;
      }
    }
    const policies = await networking.listNamespacedNetworkPolicy(
      namespace,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      1000,
    );
    if (policies.body.metadata?._continue) throw new Error('policy list exceeded limit');
    for (const policy of policies.body.items) {
      if (!policy.spec) throw new Error('invalid NetworkPolicy');
      if (
        egressSelectorMatches(policy.spec.podSelector, pod.metadata?.labels ?? {}) &&
        (policy.spec.egress ?? []).some(
          (rule) => !egressRuleBounded(networkPolicyEgress(rule, namespace), expectedRules),
        )
      )
        return false;
    }
    const cep = object(
      (
        await custom.getNamespacedCustomObject(
          'cilium.io',
          'v2',
          namespace,
          'ciliumendpoints',
          name,
        )
      ).body,
    );
    if (
      !array(object(cep.metadata).ownerReferences ?? []).some(
        (raw) => object(raw).uid === uid && object(raw).kind === 'Pod',
      )
    )
      return false;
    const state = object(cep.status),
      egress = object(object(state.policy ?? {}).egress ?? {});
    return state.state === 'ready' && egress.enforcing === true && egress.state === 'enforcing';
  };
}
