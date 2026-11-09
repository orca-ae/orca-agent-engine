// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import yaml from 'js-yaml';

const apis = ['CiliumNetworkPolicy', 'CiliumEndpoint', 'CiliumClusterwideNetworkPolicy'];
const enabled = {
  enabled: true,
  proxyUrl: 'https://proxy.engine.svc:8443',
  caBundleConfigMap: 'public-egress-ca',
  providerServiceAccount: { namespace: 'opensandbox-system', name: 'opensandbox-server' },
  gateway: { namespace: 'engine', podSelector: { app: 'forward-proxy' }, port: 8443 },
  internalBypass: [
    {
      host: 'registry-internal.engine.svc',
      namespace: 'engine',
      podSelector: { app: 'registry' },
      ports: [{ port: 8081, protocol: 'TCP' }],
    },
  ],
};

function render(sandboxEgress, versions = [], upgrade = false, overrides = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'orca-egress-chart-'));
  try {
    const path = join(directory, 'values.yaml');
    writeFileSync(
      path,
      yaml.dump({
        sandboxEgress,
        secrets: {
          values: { sessionJwtPrivateKeyPem: 'fixture', sessionJwtPublicKeyPem: 'fixture' },
        },
        external: {
          databases: {
            registryUrl: 'postgres://fixture',
            filestoreUrl: 'postgres://fixture',
            memorystoreUrl: 'postgres://fixture',
          },
        },
        objectStorage: { stsRoleArn: 'fixture' },
        ...overrides,
      }),
    );
    return spawnSync(
      'helm',
      [
        'template',
        'egress',
        'charts/orca-managed-agents',
        '--namespace',
        'engine',
        '-f',
        path,
        ...versions.flatMap((version) => ['--api-versions', 'cilium.io/v2/' + version]),
        ...(upgrade ? ['--is-upgrade'] : []),
      ],
      { encoding: 'utf8' },
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function sort(value) {
  if (Array.isArray(value)) return value.map(sort);
  if (value && typeof value === 'object')
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, sort(value[key])]),
    );
  return value;
}

test('disabled install and upgrade need no Cilium, policy, RBAC or egress config', () => {
  const result = render(undefined);
  assert.equal(result.status, 0, result.stderr);
  assert.doesNotMatch(result.stdout, /cilium.io|sandbox-egress|SANDBOX_EGRESS/);
  const upgrade = render({ enabled: false }, [], true);
  assert.equal(upgrade.status, 0, upgrade.stderr);
  assert.doesNotMatch(upgrade.stdout, /cilium.io|sandbox-egress|SANDBOX_EGRESS/);
  const malformedButDisabled = render({ enabled: false, proxyUrl: 'http://invalid', gateway: {} });
  assert.equal(malformedButDisabled.status, 0, malformedButDisabled.stderr);
  assert.equal(malformedButDisabled.stdout, result.stdout);
});

test('enabled render pins policy, explicit endpoint ports, config types and namespace RBAC', () => {
  const result = render(enabled, apis);
  assert.equal(result.status, 0, result.stderr);
  const docs = yaml.loadAll(result.stdout).filter(Boolean);
  const policy = docs.find((doc) => doc.kind === 'CiliumNetworkPolicy');
  assert.equal(policy.metadata.namespace, 'opensandbox');
  assert.deepEqual(policy.spec.endpointSelector, {
    matchLabels: { 'orca.ai/sandbox-egress': 'enabled' },
  });
  assert.equal(policy.spec.egress.length, 3);
  assert.deepEqual(policy.spec.egress[0].toPorts, [{ ports: [{ port: '8443', protocol: 'TCP' }] }]);
  assert.deepEqual(policy.spec.egress[1].toPorts, [
    {
      ports: [
        { port: '53', protocol: 'TCP' },
        { port: '53', protocol: 'UDP' },
      ],
    },
  ]);
  assert.doesNotMatch(JSON.stringify(policy.spec), /toEntities|toCIDR|toFQDNs|0.0.0.0|::|ANY/);
  const provider = docs.find(
    (doc) => doc.kind === 'ConfigMap' && doc.metadata.name === 'orca-sandbox-egress-provider',
  );
  assert.equal(
    docs.find((doc) => doc.kind === 'ConfigMap' && doc.metadata.name.endsWith('-registry-config'))
      .data.SANDBOX_EGRESS_POLICY_SHA256,
    createHash('sha256')
      .update(JSON.stringify(sort(policy.spec)))
      .digest('hex'),
  );
  assert.equal(
    provider.data.ORCA_SANDBOX_EGRESS_REGISTRY_URL,
    'http://egress-orca-managed-agents-registry-internal.engine.svc:8081',
  );
  assert.equal(provider.data.ORCA_SANDBOX_EGRESS_POLICY_SHA256, undefined);
  const harness = docs.find(
    (doc) => doc.kind === 'ConfigMap' && doc.metadata.name.endsWith('-harness-config'),
  );
  assert.deepEqual(JSON.parse(harness.data.SANDBOX_EGRESS_INTERNAL_BYPASS), [
    'registry-internal.engine.svc',
  ]);
  assert.equal(harness.data.SANDBOX_EGRESS_CA_BUNDLE_PATH, undefined);
  const registry = docs.find(
    (doc) => doc.kind === 'ConfigMap' && doc.metadata.name.endsWith('-registry-config'),
  );
  assert.equal(registry.data.SANDBOX_EGRESS_CA_BUNDLE_PATH, undefined);
  assert.equal(registry.data.SANDBOX_EGRESS_GITHUB_REST_READ_ENABLED, 'false');
  assert.equal(registry.data.SANDBOX_EGRESS_SANDBOX_NAMESPACE, 'opensandbox');
  const registryDeployment = docs.find(
    (doc) =>
      doc.kind === 'Deployment' &&
      doc.metadata.labels['app.kubernetes.io/component'] === 'registry',
  );
  const registryContainer = registryDeployment.spec.template.spec.containers.find(
    (container) => container.name === 'registry',
  );
  for (const key of ['SANDBOX_EGRESS_PROXY_URL', 'SANDBOX_EGRESS_INTERNAL_BYPASS']) {
    assert.equal(registry.data[key], harness.data[key]);
    assert.deepEqual(
      registryContainer.env.filter((entry) => entry.name === key),
      [
        {
          name: key,
          valueFrom: { configMapKeyRef: { name: registry.metadata.name, key } },
        },
      ],
    );
  }
  assert.ok(
    !(registryContainer.volumeMounts ?? []).some((mount) =>
      mount.mountPath.startsWith('/etc/orca/sandbox-egress'),
    ),
  );
  const deployment = docs.find(
    (doc) =>
      doc.kind === 'Deployment' && doc.metadata.labels['app.kubernetes.io/component'] === 'harness',
  );
  const owner = deployment.spec.template.spec.containers[0].env.filter((entry) =>
    entry.name.startsWith('SANDBOX_EGRESS_OWNER_'),
  );
  assert.deepEqual(
    owner.map((entry) => entry.valueFrom.fieldRef.fieldPath),
    ['metadata.namespace', 'metadata.name', 'metadata.uid'],
  );
  const roles = docs.filter(
    (doc) => doc.kind === 'ClusterRole' && doc.metadata.name.endsWith('-sandbox-egress-audit'),
  );
  assert.deepEqual(roles[0].rules, [
    { apiGroups: ['cilium.io'], resources: ['ciliumclusterwidenetworkpolicies'], verbs: ['list'] },
  ]);
  for (const role of docs.filter(
    (doc) => doc.kind === 'Role' && doc.metadata.name.includes('sandbox-egress'),
  )) {
    assert.ok(['engine', 'opensandbox'].includes(role.metadata.namespace));
    assert.ok(
      role.rules.every((rule) => rule.verbs.every((verb) => ['get', 'list'].includes(verb))),
    );
  }
});

for (const missing of apis) {
  test('enabled rejects missing ' + missing, () => {
    const result = render(
      enabled,
      apis.filter((api) => api !== missing),
    );
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /requires installed/);
  });
}

test('chart-owned settings cannot be overridden, unrelated scoped settings remain configurable', () => {
  for (const key of ['SANDBOX_EGRESS_ENABLED', 'SANDBOX_EGRESS_OWNER_POD_UID']) {
    const result = render(enabled, apis, false, {
      harness: { extraEnv: [{ name: key, value: 'override' }] },
    });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /reserves chart-owned/);
  }
  const result = render(enabled, apis, false, {
    registry: { extraEnv: [{ name: 'LOG_LEVEL', value: 'debug' }] },
  });
  assert.equal(result.status, 0, result.stderr);
});

for (const [name, patch] of [
  ['CA', { caBundleConfigMap: '' }],
  ['provider identity', { providerServiceAccount: { name: '' } }],
  ['plaintext', { proxyUrl: 'http://proxy.engine.svc:8443' }],
  ['credentials', { proxyUrl: 'https://orca:capability@proxy.engine.svc:8443' }],
  ['proxy port', { proxyUrl: 'https://proxy.engine.svc:443' }],
  ['gateway selector', { gateway: { namespace: 'engine', podSelector: {}, port: 8443 } }],
  ['wide bypass', { internalBypass: [{ ...enabled.internalBypass[0], host: '.svc' }] }],
  ['CIDR bypass', { internalBypass: [{ ...enabled.internalBypass[0], host: '10.0.0.0/8' }] }],
  ['wildcard port', { internalBypass: [{ ...enabled.internalBypass[0], ports: [] }] }],
]) {
  test('enabled rejects missing or unsafe ' + name, () => {
    const result = render({ ...enabled, ...patch }, apis);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /sandboxEgress/);
  });
}
