// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { cpSync, mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';
import yaml from 'js-yaml';

const chartDir = 'charts/orca-managed-agents';

test('Registry MCP snapshots use the external Gateway with an optional override', () => {
  for (const gatewayUrl of [
    'https://external-gateway.example',
    'https://external-gateway.example///',
    'https://external-gateway.example/v1/mcp',
    'https://external-gateway.example/v1/mcp/',
  ]) {
    for (const override of [undefined, 'https://runner-gateway.example/custom-mcp']) {
      const rendered = helmTemplate(
        yaml.dump({
          aiGateway: { enabled: false },
          harness: { aiGatewayUrl: gatewayUrl },
          registry: {
            aiGatewayLlmUrl: 'https://external-gateway.example/v1',
            ...(override ? { aiGatewayMcpUrl: override } : {}),
          },
        }),
      );
      const config = yaml
        .loadAll(rendered)
        .find(
          (doc) =>
            doc?.kind === 'ConfigMap' &&
            doc.metadata.name === 'orca-managed-agents-registry-config',
        );
      assert.equal(
        config.data.AI_GATEWAY_MCP_URL,
        override ?? 'https://external-gateway.example/v1/mcp',
      );
      assert.equal(config.data.AI_GATEWAY_LLM_URL, 'https://external-gateway.example/v1');
    }
  }
});

test('harness can bind a dedicated OpenAI Secret for Codex separate direct egress', () => {
  const manifest = helmTemplate(
    yaml.dump({
      harness: { secretKeyRefs: { OPENAI_API_KEY: { name: 'codex-provider', key: 'api-key' } } },
    }),
  );
  const docs = yaml.loadAll(manifest);
  const deployment = docs.find(
    (doc) =>
      doc?.kind === 'Deployment' &&
      doc.metadata.labels['app.kubernetes.io/component'] === 'harness',
  );
  const env = deployment.spec.template.spec.containers[0].env.find(
    (entry) => entry.name === 'OPENAI_API_KEY',
  );
  assert.deepEqual(env.valueFrom.secretKeyRef, {
    name: 'codex-provider',
    key: 'api-key',
    optional: true,
  });
});

test('Schema Registry rejects nondefault companions without URL on every backend', () => {
  for (const backend of ['kafka', 'postgres', 'pulsar']) {
    assert.doesNotThrow(() => helmTemplate(yaml.dump({ transcriptStore: { backend } })));
    for (const schemaRegistry of [
      { subject: 'custom.Subject' },
      { autoRegister: false },
      { requestTimeoutMs: 1234 },
    ]) {
      const valuesPath = writeValues(
        `${minimalValues}\n${yaml.dump({ transcriptStore: { backend, kafka: { schemaRegistry } } })}`,
      );
      const result = spawnSync(
        'helm',
        ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
        { encoding: 'utf8' },
      );
      assert.notEqual(result.status, 0, JSON.stringify({ backend, schemaRegistry }));
      assert.match(result.stderr, /transcriptStore.kafka.*(requires url|requires backend=kafka)/);
    }
  }
});

test('non-Kafka backends reject enabled transcript encoding and Registry URL', () => {
  for (const backend of ['postgres', 'pulsar']) {
    for (const kafka of [
      { schemaRegistry: { url: 'https://schemas.example' } },
      { encoding: 'avro', schemaRegistry: { url: 'https://schemas.example' } },
    ]) {
      const valuesPath = writeValues(
        `${minimalValues}\n${yaml.dump({ transcriptStore: { backend, kafka } })}`,
      );
      const result = spawnSync(
        'helm',
        ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
        { encoding: 'utf8' },
      );
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, /requires backend=kafka/);
    }
  }
});

test('chart-managed Registry password rotation rolls all three services on upgrade', () => {
  const renderUpgrade = (password, existingSecret = '') => {
    const valuesPath = writeValues(
      `${minimalValues}\n${yaml.dump({
        transcriptStore: {
          kafka: { schemaRegistry: { url: 'https://schemas.example', authMode: 'basic' } },
        },
        secrets: {
          existingSecret,
          values: {
            sessionJwtPrivateKeyPem: 'test-private-key',
            sessionJwtPublicKeyPem: 'test-public-key',
            kafkaSchemaRegistryUsername: 'schema-user',
            kafkaSchemaRegistryPassword: password,
          },
        },
        observabilityExporter: { enabled: true },
      })}`,
    );
    return execFileSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '--is-upgrade', '-f', valuesPath],
      { encoding: 'utf8' },
    );
  };
  const before = renderUpgrade('before-password');
  const after = renderUpgrade('after-password');
  const external = renderUpgrade('unused-password', 'external-auth');
  for (const component of ['registry', 'harness', 'observability-exporter']) {
    const pod = (rendered) =>
      yaml.load(document(rendered, 'Deployment', `orca-managed-agents-${component}`)).spec.template;
    const oldAnnotations = pod(before).metadata.annotations;
    const newAnnotations = pod(after).metadata.annotations;
    assert.equal(oldAnnotations['checksum/config'], newAnnotations['checksum/config']);
    assert.match(oldAnnotations['checksum/secret'] ?? '', /^[0-9a-f]{64}$/);
    assert.notEqual(
      oldAnnotations['checksum/secret'],
      newAnnotations['checksum/secret'],
      component,
    );
    assert.equal(pod(external).metadata.annotations['checksum/secret'], undefined);
  }
});

test('Schema Registry inline credentials and explicit TLS mounts stay on host services', () => {
  const mount = {
    extraVolumes: [{ name: 'schema-tls', secret: { secretName: 'schema-tls' } }],
    extraVolumeMounts: [{ name: 'schema-tls', mountPath: '/etc/schema-tls', readOnly: true }],
  };
  const rendered = helmTemplate(
    yaml.dump({
      transcriptStore: {
        kafka: { schemaRegistry: { url: 'https://schemas.example', authMode: 'basic' } },
      },
      secrets: {
        values: {
          sessionJwtPrivateKeyPem: 'test-private-key',
          sessionJwtPublicKeyPem: 'test-public-key',
          kafkaSchemaRegistryUsername: 'schema-user',
          kafkaSchemaRegistryPassword: 'schema-password',
        },
      },
      registry: mount,
      harness: mount,
      observabilityExporter: { enabled: true, ...mount },
    }),
  );
  const docs = yaml.loadAll(rendered).filter(Boolean);
  const secret = docs.find(
    (doc) => doc.kind === 'Secret' && doc.stringData?.KAFKA_SCHEMA_REGISTRY_USERNAME,
  );
  assert.equal(secret.stringData.KAFKA_SCHEMA_REGISTRY_USERNAME, 'schema-user');
  assert.equal(secret.stringData.KAFKA_SCHEMA_REGISTRY_PASSWORD, 'schema-password');
  for (const component of ['registry', 'harness', 'observability-exporter']) {
    const pod = yaml.load(document(rendered, 'Deployment', `orca-managed-agents-${component}`)).spec
      .template.spec;
    assert.deepEqual(
      pod.volumes.find((entry) => entry.name === 'schema-tls'),
      mount.extraVolumes[0],
    );
    assert.deepEqual(
      pod.containers[0].volumeMounts.find((entry) => entry.name === 'schema-tls'),
      mount.extraVolumeMounts[0],
    );
  }
  for (const doc of docs.filter(
    (entry) =>
      entry.kind === 'Deployment' &&
      !/-(registry|harness|observability-exporter)$/.test(entry.metadata.name),
  )) {
    assert.doesNotMatch(
      JSON.stringify(doc.spec.template.spec),
      /KAFKA_SCHEMA_REGISTRY|schema-tls|schema-password/,
    );
  }
});

test('Schema Registry rejects unsafe or incomplete chart configuration', () => {
  for (const kafka of [
    { encoding: 'avro' },
    { encoding: 'json' },
    { schemaRegistry: { url: 'http://schemas.example', authMode: 'basic' } },
    { schemaRegistry: { url: 'https://schemas.example', certFile: '/cert.pem' } },
    { schemaRegistry: { url: 'https://user:password@schemas.example' } },
    { schemaRegistry: { url: 'https://schemas.example?token=secret' } },
    { schemaRegistry: { url: 'https://schemas.example', authMode: 'oauth' } },
  ]) {
    const valuesPath = writeValues(
      `${minimalValues}\n${yaml.dump({ transcriptStore: { kafka } })}`,
    );
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { encoding: 'utf8' },
    );
    assert.notEqual(result.status, 0, JSON.stringify(kafka));
    assert.match(result.stderr, /transcriptStore.kafka/);
  }
});

test('transcript Avro configuration reaches only the three Kafka services', () => {
  for (const encoding of ['raw', 'avro']) {
    const rendered = helmTemplate(
      yaml.dump({
        transcriptStore: {
          kafka: {
            encoding,
            schemaRegistry: {
              url: 'https://schemas.example/prefix',
              subject: 'test.TranscriptEvent',
              autoRegister: false,
              authMode: 'basic',
              caFile: '/etc/schema/ca.pem',
              certFile: '/etc/schema/cert.pem',
              keyFile: '/etc/schema/key.pem',
              requestTimeoutMs: 7000,
            },
          },
        },
        observabilityExporter: { enabled: true },
      }),
    );
    for (const component of ['registry', 'harness', 'observability-exporter']) {
      const config = yaml.load(
        document(rendered, 'ConfigMap', `orca-managed-agents-${component}-config`),
      ).data;
      assert.equal(config.KAFKA_TRANSCRIPT_ENCODING, encoding);
      assert.equal(config.KAFKA_SCHEMA_REGISTRY_URL, 'https://schemas.example/prefix');
      assert.equal(config.KAFKA_SCHEMA_REGISTRY_SUBJECT, 'test.TranscriptEvent');
      assert.equal(config.KAFKA_SCHEMA_REGISTRY_AUTO_REGISTER, 'false');
      assert.equal(config.KAFKA_SCHEMA_REGISTRY_AUTH_MODE, 'basic');
      assert.equal(config.KAFKA_SCHEMA_REGISTRY_CA_FILE, '/etc/schema/ca.pem');
      assert.equal(config.KAFKA_SCHEMA_REGISTRY_CERT_FILE, '/etc/schema/cert.pem');
      assert.equal(config.KAFKA_SCHEMA_REGISTRY_KEY_FILE, '/etc/schema/key.pem');
      assert.equal(config.KAFKA_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS, '7000');
      const pod = yaml.load(document(rendered, 'Deployment', `orca-managed-agents-${component}`))
        .spec.template.spec;
      assert.ok(
        !JSON.stringify(pod.volumes ?? []).includes('/etc/schema'),
        'file paths do not implicitly create mounts',
      );
    }
    for (const doc of yaml.loadAll(rendered).filter(Boolean)) {
      if (
        doc.kind !== 'ConfigMap' ||
        /-(registry|harness|observability-exporter)-config$/.test(doc.metadata.name)
      )
        continue;
      assert.doesNotMatch(JSON.stringify(doc.data), /KAFKA_SCHEMA_REGISTRY/);
    }
  }
});

test('default raw transcript omits every explicit Schema Registry option', () => {
  const rendered = helmTemplate('observabilityExporter: {enabled: true}');
  for (const component of ['registry', 'harness', 'observability-exporter']) {
    const config = document(rendered, 'ConfigMap', `orca-managed-agents-${component}-config`);
    assert.match(config, /KAFKA_TRANSCRIPT_ENCODING: "raw"/);
    assert.doesNotMatch(config, /KAFKA_SCHEMA_REGISTRY_/);
  }
});

test('Schema Registry credentials use component refs before existingSecret and never broker credentials', () => {
  for (const override of [false, true]) {
    const refs = override
      ? {
          KAFKA_SCHEMA_REGISTRY_USERNAME: { name: 'schema-auth', key: 'user' },
          KAFKA_SCHEMA_REGISTRY_PASSWORD: { name: 'schema-auth', key: 'password' },
        }
      : {};
    const rendered = helmTemplate(
      yaml.dump({
        secrets: { existingSecret: 'shared-auth' },
        transcriptStore: {
          kafka: { schemaRegistry: { url: 'https://schemas.example', authMode: 'basic' } },
        },
        registry: { secretKeyRefs: refs },
        harness: { secretKeyRefs: refs },
        observabilityExporter: { enabled: true, secretKeyRefs: refs },
      }),
    );
    for (const component of ['registry', 'harness', 'observability-exporter']) {
      const container = yaml.load(
        document(rendered, 'Deployment', `orca-managed-agents-${component}`),
      ).spec.template.spec.containers[0];
      for (const suffix of ['USERNAME', 'PASSWORD']) {
        const key = `KAFKA_SCHEMA_REGISTRY_${suffix}`;
        const env = (container.env ?? []).filter((entry) => entry.name === key);
        if (component === 'registry' && !override) {
          assert.ok(container.envFrom.some((entry) => entry.secretRef?.name === 'shared-auth'));
        } else {
          assert.equal(env.length, 1);
          assert.equal(
            env[0].valueFrom.secretKeyRef.name,
            override ? 'schema-auth' : 'shared-auth',
          );
          assert.equal(
            env[0].valueFrom.secretKeyRef.key,
            override ? (suffix === 'USERNAME' ? 'user' : 'password') : key,
          );
        }
      }
    }
    assert.ok(
      !yaml
        .loadAll(rendered)
        .some((doc) => doc?.kind === 'Secret' && doc.stringData?.KAFKA_SCHEMA_REGISTRY_PASSWORD),
    );
  }
});

test('enabled exporter defaults to Kafka state without DB credentials and uses projected workload identity', () => {
  const rendered = helmTemplate(`
transcriptStore:
  backend: kafka
observabilityExporter:
  enabled: true
  secretKeyRefs:
    KAFKA_AUTH_TOKEN: {name: kafka-auth, key: token}
images:
  observabilityExporter:
    repository: docker.io/example/orca-observability-exporter
    tag: test-1
`);
  const deployment = document(rendered, 'Deployment', 'orca-managed-agents-observability-exporter');
  assert.match(deployment, /image: "docker.io\/example\/orca-observability-exporter:test-1"/);
  assert.match(deployment, /serviceAccountName: orca-managed-agents-observability-exporter/);
  assert.match(deployment, /automountServiceAccountToken: false/);
  assert.match(deployment, /audience: "orca-registry-internal"/);
  const config = document(
    rendered,
    'ConfigMap',
    'orca-managed-agents-observability-exporter-config',
  );
  assert.match(config, /OBSERVABILITY_EXPORTER_STATE_BACKEND: "kafka"/);
  assert.match(config, /TRANSCRIPT_STORE_BACKEND: "kafka"/);
  assert.doesNotMatch(config, /DATABASE_URL/);
  assert.match(deployment, /name: kafka-auth/);
  assert.match(deployment, /path: \/readyz/);
  assert.match(deployment, /path: \/healthz/);
  assert.doesNotMatch(deployment, /DATABASE_URL|ANTHROPIC_API_KEY/);
  assert.doesNotMatch(deployment, /secretRef:/);
});

test('token authentication and exporter topic aliases are configured independently', () => {
  for (const listingMode of [undefined, 'bare-alias']) {
    const rendered = helmTemplate(
      yaml.dump({
        transcriptStore: { kafka: { connectionMode: 'sasl-plain-token-tls' } },
        observabilityExporter: {
          enabled: true,
          extraEnv: listingMode ? [{ name: 'KAFKA_TOPIC_LISTING_MODE', value: listingMode }] : [],
        },
      }),
    );
    for (const component of ['registry', 'harness', 'observability-exporter']) {
      const config = yaml.load(
        document(rendered, 'ConfigMap', `orca-managed-agents-${component}-config`),
      );
      assert.equal(config.data.KAFKA_CONNECTION_MODE, 'sasl-plain-token-tls');
      assert.equal(config.data.KAFKA_TOPIC_LISTING_MODE, undefined);
      const deployment = yaml.load(
        document(rendered, 'Deployment', `orca-managed-agents-${component}`),
      );
      const env = deployment.spec.template.spec.containers[0].env ?? [];
      assert.equal(
        env.find((entry) => entry.name === 'KAFKA_TOPIC_LISTING_MODE')?.value,
        component === 'observability-exporter' ? listingMode : undefined,
      );
    }
  }
});

test('Kafka exporter scratch is disk-backed, bounded, nonroot writable and uses Recreate', () => {
  for (const kafkaStateSizeLimit of [undefined, null, '4Gi']) {
    const rendered = helmTemplate(
      yaml.dump({
        observabilityExporter: { enabled: true, kafkaStateSizeLimit },
      }),
    );
    const deployment = yaml.load(
      document(rendered, 'Deployment', 'orca-managed-agents-observability-exporter'),
    );
    const pod = deployment.spec.template.spec;
    const container = pod.containers[0];
    assert.deepEqual(deployment.spec.strategy, { type: 'Recreate', rollingUpdate: null });
    assert.deepEqual(
      pod.volumes.find((v) => v.name === 'exporter-state'),
      {
        name: 'exporter-state',
        emptyDir: { sizeLimit: kafkaStateSizeLimit || '2Gi' },
      },
    );
    assert.deepEqual(
      container.volumeMounts.find((v) => v.name === 'exporter-state'),
      {
        name: 'exporter-state',
        mountPath: '/var/run/orca/exporter-state',
      },
    );
    assert.equal(pod.securityContext.runAsNonRoot, true);
    assert.equal(pod.securityContext.runAsUser, 1000);
    assert.equal(pod.securityContext.fsGroup, 1000);
    const config = yaml.load(
      document(rendered, 'ConfigMap', 'orca-managed-agents-observability-exporter-config'),
    );
    assert.equal(config.data.OBSERVABILITY_KAFKA_STATE_DIRECTORY, '/var/run/orca/exporter-state');
    assert.equal(config.data.OBSERVABILITY_KAFKA_STATE_MAX_BYTES, '1073741824');
  }
});

test('Kafka exporter scratch permits an explicit index budget without leaking secret metadata', () => {
  const extraEnv = [{ name: 'OBSERVABILITY_KAFKA_STATE_MAX_BYTES', value: '536870912' }];
  const rendered = helmTemplate(
    yaml.dump({
      observabilityExporter: {
        enabled: true,
        extraEnv,
        secretKeyRefs: {
          KAFKA_AUTH_TOKEN: { name: 'private-kafka-credential', key: 'private-token-key' },
        },
      },
    }),
  );
  const deployment = yaml.load(
    document(rendered, 'Deployment', 'orca-managed-agents-observability-exporter'),
  );
  assert.deepEqual(
    deployment.spec.template.spec.containers[0].env.find((v) => v.name === extraEnv[0].name),
    extraEnv[0],
  );
  const config = document(
    rendered,
    'ConfigMap',
    'orca-managed-agents-observability-exporter-config',
  );
  assert.doesNotMatch(config, /private-kafka-credential|private-token-key|KAFKA_AUTH_TOKEN/);
  assert.doesNotMatch(
    JSON.stringify(deployment.spec.template.metadata),
    /private-kafka-credential|private-token-key/,
  );
});

test('exporter scratch and Recreate are absent for postgres and disabled workloads', () => {
  for (const enabled of [true, false]) {
    const rendered = helmTemplate(
      yaml.dump({
        observabilityExporter: {
          enabled,
          stateBackend: 'postgres',
          secretKeyRefs: { OBSERVABILITY_EXPORTER_DATABASE_URL: { name: 'legacy-db', key: 'url' } },
        },
      }),
    );
    assert.doesNotMatch(rendered, /OBSERVABILITY_KAFKA_STATE_|name: exporter-state/);
    assert.ok(!rendered.includes('/var/run/orca/exporter-state'));
    if (enabled) {
      const deployment = yaml.load(
        document(rendered, 'Deployment', 'orca-managed-agents-observability-exporter'),
      );
      assert.equal(deployment.spec.strategy, undefined);
    } else {
      assert.doesNotMatch(rendered, /name: orca-managed-agents-observability-exporter-config/);
    }
  }
  assert.doesNotMatch(
    helmTemplate('observabilityExporter: {enabled: false}'),
    /OBSERVABILITY_KAFKA_STATE_|name: exporter-state/,
  );
});

test('Kafka exporter scratch rejects reserved volume names and overlapping mount paths', () => {
  for (const overlay of [
    { extraVolumes: [{ name: 'exporter-state', emptyDir: {} }] },
    { extraVolumeMounts: [{ name: 'exporter-state', mountPath: '/other' }] },
    ...[
      '/var/run/orca/exporter-state',
      '/var/run/orca/exporter-state/',
      '/var/run/orca/exporter-state/db',
      '/var/run/orca',
      '/',
    ].map((mountPath) => ({ extraVolumeMounts: [{ name: 'custom', mountPath }] })),
  ]) {
    assert.throws(
      () => helmTemplate(yaml.dump({ observabilityExporter: { enabled: true, ...overlay } })),
      /observabilityExporter.*reserves.*exporter-state/,
    );
    assert.doesNotThrow(() =>
      helmTemplate(yaml.dump({ observabilityExporter: { enabled: false, ...overlay } })),
    );
  }
  assert.doesNotThrow(() =>
    helmTemplate(
      yaml.dump({
        observabilityExporter: {
          enabled: true,
          extraVolumes: [{ name: 'custom', emptyDir: {} }],
          extraVolumeMounts: [{ name: 'custom', mountPath: '/var/run/orca/exporter-state-extra' }],
        },
      }),
    ),
  );
});

test('exporter rejects reserved selector labels and non-map pod labels', () => {
  for (const labels of [
    { 'app.kubernetes.io/name': 'other' },
    { 'app.kubernetes.io/instance': 'other' },
    { 'app.kubernetes.io/component': 'other' },
    ['team'],
    'team',
  ]) {
    const valuesPath = writeValues(`${minimalValues}
${yaml.dump({ observabilityExporter: { enabled: true, podLabels: labels } })}`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { encoding: 'utf8' },
    );
    assert.notEqual(result.status, 0);
    assert.match(
      result.stderr,
      typeof labels === 'string' || Array.isArray(labels)
        ? /observabilityExporter.podLabels must be a map/
        : /observabilityExporter.podLabels must not set reserved selector label/,
    );
  }
});

test('exporter permits custom pod labels without changing selector labels', () => {
  const rendered = helmTemplate(`
observabilityExporter:
  enabled: true
  podLabels:
    team: observability
`);
  const deployment = yaml.load(
    document(rendered, 'Deployment', 'orca-managed-agents-observability-exporter'),
  );
  const labels = deployment.spec.template.metadata.labels;
  assert.equal(labels.team, 'observability');
  for (const [key, value] of Object.entries(deployment.spec.selector.matchLabels)) {
    assert.equal(labels[key], value);
  }
});

test('enabling exporter with reused pre-workload values requires reset-values, even with an image override', () => {
  const dir = mkdtempSync(join(tmpdir(), 'orca-exporter-reused-values-'));
  try {
    cpSync(chartDir, dir, { recursive: true });
    const defaults = yaml.load(readFileSync(join(chartDir, 'values.yaml'), 'utf8'));
    // Helm --reuse-values replaces the new chart defaults with the old release values.
    // Before this workload shipped, only the exporter ServiceAccount existed.
    defaults.observabilityExporter = {
      serviceAccount: defaults.observabilityExporter.serviceAccount,
    };
    delete defaults.images.observabilityExporter;
    writeFileSync(join(dir, 'values.yaml'), yaml.dump(defaults));
    const valuesPath = writeValues(`${minimalValues}
observabilityExporter:
  enabled: true
`);
    for (const extraArgs of [
      [],
      [
        '--set',
        'images.observabilityExporter.repository=example/exporter,images.observabilityExporter.tag=test,images.observabilityExporter.pullPolicy=IfNotPresent',
      ],
    ]) {
      const result = spawnSync(
        'helm',
        ['template', 'orca-managed-agents', dir, '-f', valuesPath, ...extraArgs],
        { encoding: 'utf8' },
      );
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, /exporter workload defaults are missing/);
      assert.match(result.stderr, /--reset-values/);
      assert.doesNotMatch(result.stderr, /nil pointer/);
    }
    const disabled = execFileSync(
      'helm',
      [
        'template',
        'orca-managed-agents',
        dir,
        '-f',
        valuesPath,
        '--set',
        'observabilityExporter.enabled=false',
      ],
      { encoding: 'utf8' },
    );
    assert.doesNotMatch(disabled, /name: orca-managed-agents-observability-exporter-config/);
    // Resetting to the new defaults and reapplying the operator overlay restores every probe.
    const reset = execFileSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { encoding: 'utf8' },
    );
    const deployment = document(reset, 'Deployment', 'orca-managed-agents-observability-exporter');
    for (const probe of ['startupProbe', 'readinessProbe', 'livenessProbe']) {
      assert.match(deployment, new RegExp(`${probe}:`));
    }
    assert.match(deployment, /replicas: 1/);
    assert.match(deployment, /imagePullPolicy: IfNotPresent/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('exporter deployment docs specify both Kafka group prefixes and upgrade migration', () => {
  const doc = readFileSync('services/observability-exporter/README.md', 'utf8');
  assert.match(doc, /observability-exporter-inbox-/);
  assert.match(doc, /transcript-store-/);
  assert.match(doc, /--reset-values/);
});

test('exporter rejects non-Kafka Transcript backends', () => {
  assert.throws(
    () =>
      helmTemplate(`
transcriptStore:
  backend: postgres
observabilityExporter:
  enabled: true
`),
    /requires transcriptStore.backend=kafka/,
  );
});

test('exporter accepts explicit Kafka state without a database reference', () => {
  const rendered = helmTemplate(`
observabilityExporter:
  enabled: true
  stateBackend: kafka
`);
  assert.match(
    document(rendered, 'ConfigMap', 'orca-managed-agents-observability-exporter-config'),
    /OBSERVABILITY_EXPORTER_STATE_BACKEND: "kafka"/,
  );
  assert.doesNotMatch(
    document(rendered, 'Deployment', 'orca-managed-agents-observability-exporter'),
    /DATABASE_URL|secretRef:/,
  );
});

test('exporter rejects legacy DB overlays without an explicit postgres state backend', () => {
  for (const stateBackend of [undefined, 'kafka']) {
    assert.throws(
      () =>
        helmTemplate(
          yaml.dump({
            observabilityExporter: {
              enabled: true,
              ...(stateBackend === undefined ? {} : { stateBackend }),
              secretKeyRefs: {
                OBSERVABILITY_EXPORTER_DATABASE_URL: { name: 'exporter-state', key: 'url' },
              },
            },
          }),
        ),
      /stateBackend=kafka rejects secretKeyRefs\.OBSERVABILITY_EXPORTER_DATABASE_URL; choose stateBackend=postgres to preserve existing progress, or remove the DB reference only after a controlled cutover/,
    );
  }
});

test('Kafka exporter rejects legacy DSNs supplied through extraEnv', () => {
  for (const stateBackend of [undefined, 'kafka']) {
    for (const source of [
      { value: 'postgres://exporter@db/state' },
      { valueFrom: { secretKeyRef: { name: 'exporter-state', key: 'url' } } },
      { valueFrom: { configMapKeyRef: { name: 'exporter-state', key: 'url', optional: true } } },
    ]) {
      assert.throws(
        () =>
          helmTemplate(
            yaml.dump({
              observabilityExporter: {
                enabled: true,
                ...(stateBackend === undefined ? {} : { stateBackend }),
                extraEnv: [{ name: 'OBSERVABILITY_EXPORTER_DATABASE_URL', ...source }],
              },
            }),
          ),
        /stateBackend=kafka rejects extraEnv\.OBSERVABILITY_EXPORTER_DATABASE_URL; choose stateBackend=postgres to preserve existing progress, or remove the DB reference only after a controlled cutover/,
      );
    }
  }
});

test('Kafka exporter permits empty DSNs and unrelated extraEnv entries', () => {
  for (const emptyValue of [{ value: '' }, {}]) {
    const extraEnv = [
      { name: 'OBSERVABILITY_EXPORTER_DATABASE_URL', ...emptyValue },
      {
        name: 'KAFKA_AUTH_TOKEN',
        valueFrom: { secretKeyRef: { name: 'kafka-auth', key: 'token' } },
      },
    ];
    const rendered = helmTemplate(
      yaml.dump({ observabilityExporter: { enabled: true, extraEnv } }),
    );
    const deployment = yaml.load(
      document(rendered, 'Deployment', 'orca-managed-agents-observability-exporter'),
    );
    assert.deepEqual(deployment.spec.template.spec.containers[0].env, extraEnv);
  }
});

test('Kafka exporter permits opaque extraEnvFrom sources without assuming they contain a DSN', () => {
  const extraEnvFrom = [
    { secretRef: { name: 'kafka-auth' } },
    { configMapRef: { name: 'exporter-settings' } },
    { prefix: 'KAFKA_', secretRef: { name: 'kafka-settings' } },
  ];
  const rendered = helmTemplate(
    yaml.dump({ observabilityExporter: { enabled: true, extraEnvFrom } }),
  );
  const deployment = yaml.load(
    document(rendered, 'Deployment', 'orca-managed-agents-observability-exporter'),
  );
  assert.deepEqual(deployment.spec.template.spec.containers[0].envFrom.slice(1), extraEnvFrom);
  assert.match(
    document(rendered, 'ConfigMap', 'orca-managed-agents-observability-exporter-config'),
    /OBSERVABILITY_EXPORTER_STATE_BACKEND: "kafka"/,
  );
});

test('exporter extraEnv DSN guard does not apply to postgres or disabled workloads', () => {
  for (const settings of [{ enabled: true, stateBackend: 'postgres' }, { enabled: false }]) {
    const extraEnv = [
      { name: 'OBSERVABILITY_EXPORTER_DATABASE_URL', value: 'postgres://db/state' },
    ];
    const rendered = helmTemplate(
      yaml.dump({
        observabilityExporter: {
          ...settings,
          extraEnv,
          secretKeyRefs: {
            OBSERVABILITY_EXPORTER_DATABASE_URL: { name: 'exporter-state', key: 'url' },
          },
        },
      }),
    );
    if (settings.enabled) {
      const deployment = yaml.load(
        document(rendered, 'Deployment', 'orca-managed-agents-observability-exporter'),
      );
      assert.deepEqual(deployment.spec.template.spec.containers[0].env.slice(-1), extraEnv);
    } else {
      assert.doesNotMatch(rendered, /name: orca-managed-agents-observability-exporter-config/);
    }
  }
});

test('exporter validates the state backend enum', () => {
  for (const stateBackend of ['redis', '', true]) {
    assert.throws(
      () => helmTemplate(yaml.dump({ observabilityExporter: { enabled: true, stateBackend } })),
      /observabilityExporter.stateBackend must be kafka or postgres/,
    );
  }
});

test('legacy postgres exporter requires a dedicated non-optional database secret', () => {
  for (const reference of [
    undefined,
    {},
    { name: 'db' },
    { key: 'url' },
    { name: 'db', key: 'url', optional: true },
  ]) {
    assert.throws(
      () =>
        helmTemplate(
          yaml.dump({
            observabilityExporter: {
              enabled: true,
              stateBackend: 'postgres',
              secretKeyRefs:
                reference === undefined ? {} : { OBSERVABILITY_EXPORTER_DATABASE_URL: reference },
            },
          }),
        ),
      /OBSERVABILITY_EXPORTER_DATABASE_URL requires name and key and must not be optional/,
    );
  }
  const rendered = helmTemplate(`
observabilityExporter:
  enabled: true
  stateBackend: postgres
  secretKeyRefs:
    OBSERVABILITY_EXPORTER_DATABASE_URL: {name: exporter-state, key: url, optional: false}
`);
  assert.match(
    document(rendered, 'ConfigMap', 'orca-managed-agents-observability-exporter-config'),
    /OBSERVABILITY_EXPORTER_STATE_BACKEND: "postgres"/,
  );
  const deployment = document(rendered, 'Deployment', 'orca-managed-agents-observability-exporter');
  assert.match(deployment, /name: OBSERVABILITY_EXPORTER_DATABASE_URL/);
  assert.match(deployment, /name: exporter-state/);
  assert.match(deployment, /key: url/);
  assert.doesNotMatch(deployment, /name: (DATABASE_URL|FILESTORE_DATABASE_URL)|secretRef:/);
});

function writeValues(contents) {
  const dir = mkdtempSync(join(tmpdir(), 'orca-managed-agents-chart-'));
  const path = join(dir, 'values.yaml');
  writeFileSync(path, contents);
  return path;
}

function helmTemplate(extraValues = '', namespace = 'orca-test') {
  const valuesPath = writeValues(`${minimalValues}\n${extraValues}`);
  return execFileSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '--namespace', namespace, '-f', valuesPath],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
    },
  );
}

const minimalValues = `
external:
  databases:
    registryUrl: postgres://orca:secret@postgres.example:5432/registry
    filestoreUrl: postgres://orca:secret@postgres.example:5432/filestore
    memorystoreUrl: postgres://orca:secret@postgres.example:5432/memorystore
secrets:
  values:
    sessionJwtPrivateKeyPem: test-private-key
    sessionJwtPublicKeyPem: test-public-key
objectStorage:
  stsRoleArn: arn:aws:iam::123456789012:role/orca-session-s3
`;

test('chart-created Secret rejects missing required values', () => {
  const result = spawnSync(
    'helm',
    [
      'template',
      'orca-managed-agents',
      chartDir,
      '--set',
      'objectStorage.stsRoleArn=arn:aws:iam::123456789012:role/orca-session-s3',
      '--set',
      'toolset.enabled=false',
    ],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /external\.databases\.registryUrl or secrets\.values\.databaseUrl is required/,
  );
});

test('rendered Secret and ai-gateway volume use env-var-safe JWT public key name', () => {
  const rendered = helmTemplate();

  assert.match(rendered, /SESSION_JWT_PUBLIC_KEY_PEM: "test-public-key"/);
  assert.doesNotMatch(rendered, /session-jwt-public-key-pem:/);
  assert.match(rendered, /- key: SESSION_JWT_PUBLIC_KEY_PEM\n\s+path: session-jwt-public-key-pem/);
});

test('chart-managed JWT public-key changes roll ai-gateway', () => {
  const first = helmTemplate();
  const second = helmTemplate(`
secrets:
  values:
    sessionJwtPrivateKeyPem: rotated-private-key
    sessionJwtPublicKeyPem: rotated-public-key
`);
  const checksum = (rendered) =>
    document(rendered, 'Deployment', 'orca-managed-agents-ai-gateway').match(
      /checksum\/session-jwt-public-key: ([0-9a-f]+)/,
    )?.[1];

  assert.ok(checksum(first));
  assert.notEqual(checksum(first), checksum(second));
});

test('registry renders configured session JWT LLM policy', () => {
  const rendered = helmTemplate(`
sessionJwt:
  llmRoutes: [llm-messages, managed-openai]
  llmModels: [claude-*, gpt-4o*]
`);

  assert.match(rendered, /SESSION_JWT_LLM_ROUTES: "llm-messages,managed-openai"/);
  assert.match(rendered, /SESSION_JWT_LLM_MODELS: "claude-\*,gpt-4o\*"/);
});

test('registry renders cron Trigger worker configuration', () => {
  const rendered = helmTemplate(`
registry:
  triggerScheduler:
    enabled: false
    reconcileIntervalMs: '2500'
    batchSize: '25'
`);

  assert.match(rendered, /TRIGGER_SCHEDULER_ENABLED: "false"/);
  assert.match(rendered, /TRIGGER_RECONCILE_INTERVAL_MS: "2500"/);
  assert.match(rendered, /TRIGGER_RECONCILE_BATCH_SIZE: "25"/);
});

test('registry rejects a partial session JWT LLM policy', () => {
  const valuesPath = writeValues(`${minimalValues}
sessionJwt:
  llmRoutes: [llm-messages]
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /llmRoutes and sessionJwt\.llmModels must be configured together/);
});

test('registry public, internal, and admin listeners render as separate services', () => {
  const rendered = helmTemplate();
  const publicService = rendered
    .split(/\n---\n/)
    .find(
      (document) =>
        document.includes('kind: Service\n') &&
        document.includes('  name: orca-managed-agents-registry\n'),
    );

  assert.ok(publicService);
  assert.match(rendered, /INTERNAL_HTTP_PORT: "8081"/);
  assert.match(rendered, /ADMIN_HTTP_PORT: "8082"/);
  assert.match(rendered, /PLATFORM_OIDC_ALLOWED_ISSUERS: ""/);
  assert.match(rendered, /PLATFORM_OIDC_AUDIENCE: "orca-managed-agents-platform"/);
  // The `metadata` claim fallback is opt-in per plane and ships off, so a
  // default install never treats requester-supplied claims as authorization.
  assert.match(rendered, /OIDC_METADATA_CLAIMS: "false"/);
  assert.match(rendered, /ADMIN_OIDC_METADATA_CLAIMS: "false"/);
  assert.match(rendered, /PLATFORM_OIDC_METADATA_CLAIMS: "false"/);
  // Organization-audience resolution ships off as well. With it on the
  // workspace plane stops verifying `oidcAudience`, so a default install must
  // never arrive in that mode by omission.
  assert.match(rendered, /OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE: "false"/);
  assert.match(rendered, /name: orca-managed-agents-registry-internal/);
  assert.match(rendered, /port: 8081\n\s+targetPort: internal-http/);
  assert.match(rendered, /name: orca-managed-agents-registry-admin/);
  assert.match(rendered, /port: 8082\n\s+targetPort: admin-http/);
  assert.match(
    rendered,
    /REGISTRY_INTERNAL_BASE_URL: "http:\/\/orca-managed-agents-registry-internal:8081"/,
  );
  assert.doesNotMatch(rendered, /REGISTRY_BASE_URL:/);
  assert.doesNotMatch(rendered, /HARNESS_REGISTRY_API_KEY/);
  assert.doesNotMatch(publicService, /internal-http|port: 8081/);
});

test('registry ingress requires an explicit trusted-proxy allowlist', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  ingress:
    enabled: true
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /registry\.trustedProxyCidrs is required/);
});

test('registry passes only configured ingress proxy ranges to Fastify', () => {
  const rendered = helmTemplate(`
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
    - 2001:db8::10
  ingress:
    enabled: true
`);

  assert.match(rendered, /TRUST_PROXY_CIDRS: "10\.42\.0\.0\/16,2001:db8::10"/);
  assert.match(rendered, /kind: Ingress/);
});

test('registry ingress forwards the whole public listener, not just /v1', () => {
  // The public listener serves more than `/v1`: `/api` and `/apis` answer
  // group/version discovery, `/api/v1/*` is rewritten to `/v1/*` inside Fastify,
  // and the probes sit outside `/v1` entirely. The chart shipped `path: /v1`,
  // which meant an Ingress-enabled deployment 404ed every one of those at the
  // controller — before any of it reached the process. Asserted on the rendered
  // Ingress rather than on values.yaml so the template, not the default, is what
  // has to hold.
  const rendered = helmTemplate(`
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  ingress:
    enabled: true
`);
  const ingress = document(rendered, 'Ingress', 'orca-managed-agents-registry');

  assert.match(ingress, /- path: \/\n/, 'the Ingress must forward the root prefix');
  assert.match(ingress, /pathType: Prefix/);
  assert.doesNotMatch(
    ingress,
    /path: \/v1/,
    'a `/v1` path would exclude /api, /apis and the /api/v1 alias',
  );

  // The backend is the public listener, not the internal or admin one.
  assert.match(ingress, /port:\n\s+name: http(?:\n|$)/);
});

test('registry ingress rejects trust-all proxy ranges', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  trustedProxyCidrs:
    - 0.0.0.0/0
  ingress:
    enabled: true
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /must not trust all forwarded addresses/);
});

test('registry rejects enabling both Ingress and Istio exposure', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  ingress:
    enabled: true
  istio:
    enabled: true
    host: agents.example.com
    gateway:
      tls:
        credentialName: agents-example-com-tls
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /registry\.ingress\.enabled and registry\.istio\.enabled cannot both be true/,
  );
});

test('registry Istio exposure requires an explicit trusted-proxy allowlist', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  istio:
    enabled: true
    host: agents.example.com
    gateway:
      tls:
        credentialName: agents-example-com-tls
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /registry\.trustedProxyCidrs is required when registry\.istio/);
});

test('registry Istio exposure requires a host', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  istio:
    enabled: true
    gateway:
      tls:
        credentialName: agents-example-com-tls
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /registry\.istio\.host is required when registry\.istio\.enabled=true/,
  );
});

test('registry Istio exposure requires a TLS credential', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  istio:
    enabled: true
    host: agents.example.com
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /registry\.istio\.gateway\.tls\.credentialName is required when registry\.istio\.enabled=true/,
  );
});

test('registry renders Istio Gateway and VirtualService with trusted proxy ranges', () => {
  const rendered = helmTemplate(`
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  istio:
    enabled: true
    host: agents.example.com
    gateway:
      selector:
        istio: edge-ingressgateway
      tls:
        credentialName: agents-example-com-tls
`);
  const gateway = document(rendered, 'Gateway', 'orca-managed-agents-registry');
  const virtualService = document(rendered, 'VirtualService', 'orca-managed-agents-registry');

  assert.match(rendered, /TRUST_PROXY_CIDRS: "10\.42\.0\.0\/16"/);
  assert.match(gateway, /apiVersion: networking\.istio\.io\/v1beta1/);
  // The Gateway must land next to the gateway workload: the TLS credential is
  // resolved there and PILOT_SCOPE_GATEWAY_TO_NAMESPACE ignores other
  // namespaces.
  assert.match(gateway, /namespace: istio-system/);
  assert.match(gateway, /selector:\n\s+istio: edge-ingressgateway/);
  assert.match(gateway, /- "agents\.example\.com"/);
  assert.match(gateway, /credentialName: "agents-example-com-tls"/);
  assert.match(
    virtualService,
    /gateways:\n\s+(?:#[^\n]*\n\s+)*- istio-system\/orca-managed-agents-registry/,
  );
  assert.match(virtualService, /host: orca-managed-agents-registry/);
  assert.match(virtualService, /number: 8080/);
  assert.doesNotMatch(rendered, /kind: Ingress/);
});

test('registry Istio Gateway and policy follow a custom gateway namespace', () => {
  const rendered = helmTemplate(`
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  istio:
    enabled: true
    host: agents.example.com
    gatewayNamespace: edge-gateways
    gateway:
      selector:
        istio: edge-ingressgateway
      tls:
        credentialName: agents-example-com-tls
`);
  const gateway = document(rendered, 'Gateway', 'orca-managed-agents-registry');
  const virtualService = document(rendered, 'VirtualService', 'orca-managed-agents-registry');
  const policy = document(rendered, 'AuthorizationPolicy', 'orca-managed-agents-registry-external');

  assert.match(gateway, /namespace: edge-gateways/);
  assert.match(virtualService, /- edge-gateways\/orca-managed-agents-registry/);
  assert.match(policy, /namespace: edge-gateways/);
});

test('registry Istio exposure rejects the moved authorizationPolicy.gatewayNamespace key', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  istio:
    authorizationPolicy:
      gatewayNamespace: istio-system
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /registry\.istio\.authorizationPolicy\.gatewayNamespace moved to registry\.istio\.gatewayNamespace/,
  );
});

test('registry, harness and exporter image tags default to the chart appVersion', () => {
  const appVersion = readFileSync(join(chartDir, 'Chart.yaml'), 'utf8').match(
    /^appVersion: '([^']+)'$/m,
  )?.[1];
  assert.ok(appVersion, 'Chart.yaml must declare a quoted appVersion');

  // A tagged release packages the chart with --app-version <version>, so the
  // default image tags must track appVersion for the released chart to pin
  // the images the same tag built.
  const rendered = helmTemplate(`
observabilityExporter:
  enabled: true
`);
  const escaped = appVersion.replaceAll('.', '\\.');
  assert.match(
    rendered,
    new RegExp(`image: "ghcr\\.io/orca-ae/orca-registry-service-ts:${escaped}"`),
  );
  assert.match(rendered, new RegExp(`image: "ghcr\\.io/orca-ae/orca-harness-server:${escaped}"`));
  assert.match(
    rendered,
    new RegExp(`image: "ghcr\\.io/orca-ae/orca-observability-exporter:${escaped}"`),
  );
  assert.doesNotMatch(rendered, /image: "[^"]*:latest"/);

  const pinned = helmTemplate(`
images:
  registry:
    tag: v9.9.9-test
`);
  assert.match(pinned, /image: "ghcr\.io\/orca-ae\/orca-registry-service-ts:v9\.9\.9-test"/);
});

test('sandbox-harness default pins by digest when set and falls back to a tag', () => {
  const digest = `sha256:${'0123456789abcdef'.repeat(4)}`;

  // Digest set: render the immutable reference form the OpenSandbox trust
  // validation requires. The tag is deliberately dropped because the
  // ORCA_TRUSTED_SANDBOX_WORKLOADS allowlist matches exact image strings and
  // the documented entries use the bare repository@digest form.
  const pinned = helmTemplate(`
images:
  sandboxHarness:
    claudeCode:
      tag: ignored-when-digest-set
      digest: ${digest}
`);
  assert.match(
    pinned,
    new RegExp(
      `SANDBOX_HARNESS_CLAUDE_CODE_IMAGE: "ghcr\\.io/orca-ae/sandbox-harness-claude-code@${digest}"`,
    ),
  );
  assert.doesNotMatch(pinned, /SANDBOX_HARNESS_CLAUDE_CODE_IMAGE: "[^"]*ignored-when-digest-set/);

  // Digest empty: the local/dev-only tag form (usable only with the
  // OpenSandbox server's ORCA_ALLOW_TAGGED_SANDBOX_IMAGES=true escape hatch),
  // with the tag defaulting to appVersion like registry/harness images.
  const appVersion = readFileSync(join(chartDir, 'Chart.yaml'), 'utf8').match(
    /^appVersion: '([^']+)'$/m,
  )?.[1];
  assert.ok(appVersion, 'Chart.yaml must declare a quoted appVersion');
  const rendered = helmTemplate();
  assert.match(
    rendered,
    new RegExp(
      `SANDBOX_HARNESS_CLAUDE_CODE_IMAGE: "ghcr\\.io/orca-ae/sandbox-harness-claude-code:${appVersion.replaceAll('.', '\\.')}"`,
    ),
  );

  // A malformed digest would render a reference OpenSandbox rejects at
  // session time, so it must fail at render time instead.
  const valuesPath = writeValues(`${minimalValues}
images:
  sandboxHarness:
    claudeCode:
      digest: sha256:not-a-digest
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
    },
  );
  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /images\.sandboxHarness\.claudeCode\.digest must be a full sha256:<64-hex-digit> manifest digest/,
  );
});

test('Orca CLI toolset is enabled by default and can be disabled', () => {
  const rendered = helmTemplate();
  const deployment = document(rendered, 'Deployment', 'orca-managed-agents-toolset');

  assert.match(rendered, /app\.kubernetes\.io\/component: toolset/);
  assert.match(rendered, /ghcr\.io\/orca-ae\/orca-cli/);
  assert.match(
    deployment,
    /- name: ORCA_REGISTRY_URL\n\s+value: "http:\/\/orca-managed-agents-registry:8080"/,
  );
  assert.doesNotMatch(deployment, /ORCA_API_KEY|ORCA_ACCESS_TOKEN/);
  assert.doesNotMatch(deployment, /secretKeyRef:/);

  const tagFallback = helmTemplate(`
images:
  toolset:
    digest: ''
`);
  assert.match(
    document(tagFallback, 'Deployment', 'orca-managed-agents-toolset'),
    /image: "ghcr\.io\/orca-ae\/orca-cli:0\.2\.0"/,
  );

  const disabled = helmTemplate(`
toolset:
  enabled: false
`);
  assert.doesNotMatch(disabled, /app\.kubernetes\.io\/component: toolset/);
  assert.doesNotMatch(disabled, /orca-ae\/orca-cli/);
});

test('Orca CLI toolset supports Bearer auth against the in-cluster Registry', () => {
  const digest = 'sha256:e60de0b9de6c09fba7ea8e01b21b95a92f37e306ee00305d4bc8ac9c9b511df1';
  const rendered = helmTemplate(`
imagePullSecrets:
  - name: dockerhub-pull
registry:
  oidcAllowedIssuers: https://issuer.example.com
toolset:
  registryUrl: ''
  accessToken:
    secretKeyRef:
      name: orca-toolset-access
      key: token
  podAnnotations:
    orca.ai/access-token-version: '2026-08-15-1'
  podLabels:
    orca.ai/purpose: interactive-tooling
  extraEnvFrom:
    - secretRef:
        name: orca-toolset-provider-credentials
        optional: true
`);
  const deployment = document(rendered, 'Deployment', 'orca-managed-agents-toolset');
  const serviceAccount = document(rendered, 'ServiceAccount', 'orca-managed-agents-toolset');

  assert.match(serviceAccount, /app\.kubernetes\.io\/component: toolset/);
  assert.match(deployment, /replicas: 1/);
  assert.match(deployment, /serviceAccountName: orca-managed-agents-toolset/);
  assert.match(deployment, /automountServiceAccountToken: false/);
  assert.match(deployment, /imagePullSecrets:\n\s+- name: dockerhub-pull/);
  assert.match(deployment, new RegExp(`image: "ghcr\\.io/orca-ae/orca-cli@${digest}"`));
  assert.match(deployment, /runAsNonRoot: true/);
  assert.match(deployment, /runAsUser: 1000/);
  assert.match(deployment, /allowPrivilegeEscalation: false/);
  assert.match(deployment, /capabilities:\n\s+drop:\n\s+- ALL/);
  assert.match(deployment, /command:\n\s+- \/bin\/sh\n\s+- -c/);
  assert.match(deployment, /trap shutdown TERM INT/);
  assert.match(deployment, /sleep infinity &/);
  assert.match(
    deployment,
    /- name: ORCA_REGISTRY_URL\n\s+value: "http:\/\/orca-managed-agents-registry:8080"/,
  );
  assert.doesNotMatch(deployment, /ORCA_REGISTRY_URL[\s\S]*?\/v1/);
  assert.match(
    deployment,
    /- name: ORCA_ACCESS_TOKEN\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: "orca-toolset-access"\n\s+key: "token"/,
  );
  assert.doesNotMatch(deployment, /ORCA_API_KEY/);
  assert.match(
    deployment,
    /envFrom:\n\s+- secretRef:\n\s+name: orca-toolset-provider-credentials\n\s+optional: true/,
  );
  assert.match(deployment, /orca\.ai\/access-token-version: 2026-08-15-1/);
  assert.match(deployment, /orca\.ai\/purpose: interactive-tooling/);
});

test('Orca CLI toolset supports workspace API-key auth without public OIDC', () => {
  const rendered = helmTemplate(`
registry:
  oidcAllowedIssuers: ''
toolset:
  registryUrl: ''
  apiKey:
    secretKeyRef:
      name: orca-toolset-access
      key: api-key
`);
  const deployment = document(rendered, 'Deployment', 'orca-managed-agents-toolset');

  assert.match(
    deployment,
    /- name: ORCA_API_KEY\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: "orca-toolset-access"\n\s+key: "api-key"/,
  );
  assert.doesNotMatch(deployment, /ORCA_ACCESS_TOKEN/);
});

test('Orca CLI toolset supports an explicit Registry URL and tag fallback', () => {
  const rendered = helmTemplate(`
images:
  toolset:
    repository: example.com/tools/orca-cli
    tag: test-build
    digest: ''
toolset:
  nameOverride: operations
  registryUrl: https://agents.example.com
  accessToken:
    secretKeyRef:
      name: orca-toolset-access
      key: access-token
`);
  const deployment = document(rendered, 'Deployment', 'operations-toolset');
  document(rendered, 'ServiceAccount', 'operations-toolset');

  assert.match(deployment, /image: "example\.com\/tools\/orca-cli:test-build"/);
  assert.match(deployment, /value: "https:\/\/agents\.example\.com"/);
  assert.match(deployment, /serviceAccountName: operations-toolset/);
});

test('Orca CLI toolset rejects selector label overrides', () => {
  for (const reservedLabel of [
    'app.kubernetes.io/name',
    'app.kubernetes.io/instance',
    'app.kubernetes.io/component',
  ]) {
    const valuesPath = writeValues(`${minimalValues}
toolset:
  registryUrl: https://registry.example.com
  accessToken:
    secretKeyRef:
      name: orca-toolset-access
      key: access-token
  podLabels:
    ${reservedLabel}: wrong
`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );

    assert.notEqual(result.status, 0, `${reservedLabel} unexpectedly rendered`);
    assert.match(result.stderr, /toolset\.podLabels must not set reserved selector label/);
    assert.match(result.stderr, new RegExp(reservedLabel.replaceAll('.', '\\.')));
  }
});

test('Orca CLI toolset keeps its suffix after a long name override', () => {
  const baseName = 't'.repeat(80);
  const resourceName = `${'t'.repeat(55)}-toolset`;
  const rendered = helmTemplate(`
toolset:
  nameOverride: ${baseName}
  registryUrl: https://registry.example.com
  accessToken:
    secretKeyRef:
      name: orca-toolset-access
      key: access-token
`);
  const deployment = document(rendered, 'Deployment', resourceName);

  document(rendered, 'ServiceAccount', resourceName);
  assert.equal(resourceName.length, 63);
  assert.match(deployment, new RegExp(`serviceAccountName: ${resourceName}`));
});

test('Orca CLI toolset preserves explicit ServiceAccount names', () => {
  const serviceAccountName = `toolset-${'a'.repeat(72)}`;
  const baseValues = `
toolset:
  registryUrl: https://registry.example.com
  accessToken:
    secretKeyRef:
      name: orca-toolset-access
      key: access-token
  serviceAccount:
    name: ${serviceAccountName}
`;

  for (const create of [true, false]) {
    const rendered = helmTemplate(`${baseValues}
    create: ${create}
`);
    const deployment = document(rendered, 'Deployment', 'orca-managed-agents-toolset');

    assert.match(deployment, new RegExp(`serviceAccountName: ${serviceAccountName}`));
    if (create) {
      document(rendered, 'ServiceAccount', serviceAccountName);
    } else {
      assert.doesNotMatch(
        rendered,
        new RegExp(`kind: ServiceAccount[\\s\\S]*?name: ${serviceAccountName}`),
      );
    }
  }
});

test('Orca CLI toolset docs use commands supported by the self-hosted Registry', () => {
  const kubernetesDoc = readFileSync('docs/managed-agents/kubernetes.md', 'utf8');

  assert.match(kubernetesDoc, /orca agent list/);
  assert.doesNotMatch(kubernetesDoc, /orca agent providers list/);
  assert.match(kubernetesDoc, /deployment\/orca-managed-agents-toolset/);
  assert.doesNotMatch(kubernetesDoc, /deployment\/orca-managed-agents-orca-cli/);
  assert.match(kubernetesDoc, /Do \*\*not\*\* use `--reuse-values`/);
  assert.match(kubernetesDoc, /--reset-values/);
  assert.match(kubernetesDoc, /ORCA_API_KEY/);
  assert.match(kubernetesDoc, /orca-cli:0\.2\.0/);
  assert.doesNotMatch(kubernetesDoc, /orca-cli:0\.1\.0/);
  assert.match(kubernetesDoc, /Credentialless is\s+the default/);
  assert.match(kubernetesDoc, /No empty or placeholder Secret is needed/);
  assert.match(kubernetesDoc, /--create-namespace/);
  assert.match(kubernetesDoc, /orca healthz/);
  assert.ok(
    kubernetesDoc.indexOf('helm upgrade --install orca-managed-agents') <
      kubernetesDoc.indexOf('create secret generic orca-toolset-access'),
    'credentialless install must precede the optional persistent toolset Secret',
  );
});

test('legacy Orca CLI values fail with reset-values migration guidance', () => {
  for (const legacyValues of [
    `orcaCli:\n  enabled: false`,
    `images:\n  orcaCli:\n    repository: ghcr.io/orca-ae/orca-cli`,
  ]) {
    const valuesPath = writeValues(`${minimalValues}\n${legacyValues}\n`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );

    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /orcaCli values were renamed to toolset/);
    assert.match(result.stderr, /--reset-values/);
    assert.match(result.stderr, /--reuse-values is unsupported/);
  }
});

test('Orca CLI toolset rejects invalid credential configuration or image digest', () => {
  for (const [extraValues, expected] of [
    [
      `toolset:\n  registryUrl: https://registry.example.com\n  accessToken:\n    secretKeyRef:\n      name: orca-toolset-access\n      key: ''`,
      /toolset\.accessToken\.secretKeyRef\.key is required/,
    ],
    [
      `toolset:\n  registryUrl: https://registry.example.com\n  apiKey:\n    secretKeyRef:\n      name: orca-toolset-api-key\n      key: ''`,
      /toolset\.apiKey\.secretKeyRef\.key is required/,
    ],
    [
      `toolset:\n  registryUrl: https://registry.example.com\n  apiKey:\n    secretKeyRef:\n      name: orca-toolset-api-key\n  accessToken:\n    secretKeyRef:\n      name: orca-toolset-access`,
      /toolset API-key and Bearer credentials are mutually exclusive/,
    ],
    [
      `toolset:\n  registryUrl: https://registry.example.com\n  accessToken:\n    secretKeyRef:\n      name: orca-toolset-access\n      key: token\n  extraEnv:\n    - name: ORCA_ACCESS_TOKEN\n      value: forbidden`,
      /toolset\.extraEnv must not override ORCA_REGISTRY_URL, ORCA_API_KEY, or ORCA_ACCESS_TOKEN/,
    ],
    [
      `toolset:\n  registryUrl: https://registry.example.com\n  apiKey:\n    secretKeyRef:\n      name: orca-toolset-api-key\n  extraEnv:\n    - name: ORCA_API_KEY\n      value: forbidden`,
      /toolset\.extraEnv must not override ORCA_REGISTRY_URL, ORCA_API_KEY, or ORCA_ACCESS_TOKEN/,
    ],
    [
      `images:\n  toolset:\n    digest: sha256:not-a-digest`,
      /images\.toolset\.digest must be a full sha256:<64-hex-digit> manifest digest/,
    ],
  ]) {
    const valuesPath = writeValues(`${minimalValues}\n${extraValues}\n`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );

    assert.notEqual(result.status, 0);
    assert.match(result.stderr, expected);
  }
});

test('in-chart Registry toolset requires public OIDC for Bearer auth', () => {
  for (const issuers of [`''`, `' ,  , '`]) {
    const valuesPath = writeValues(`${minimalValues}
registry:
  oidcAllowedIssuers: ${issuers}
toolset:
  registryUrl: ''
  accessToken:
    secretKeyRef:
      name: orca-toolset-access
      key: access-token
`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );

    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /registry\.oidcAllowedIssuers must contain at least one issuer/);
  }
});

test('registry workspace audience and audience mode must agree', () => {
  // The two keys are one setting with two states. Both mismatches are refused
  // by the registry at startup, so a release that rendered one would replace a
  // running pod with a crash-looping one; failing here rejects it first.
  const cases = [
    {
      // The dangerous half. jose reads an empty audience option as
      // presence-only — `aud` must exist and nothing is compared — so a pod in
      // this state accepts every token an allowed issuer minted for any relying
      // party. A Secret key that exists but is blank looks identical to a
      // configured one, which is why the value is checked rather than assumed.
      values: `
registry:
  oidcAllowedIssuers: https://issuer.example.com
  oidcAudience: ''
  oidcResolveWorkspaceByAudience: false
`,
      expected: /registry\.oidcAudience is required when registry\.oidcAllowedIssuers is set/,
    },
    {
      // The other direction: with resolution on the static audience is never
      // read, so a value left behind is a key that silently means nothing.
      values: `
registry:
  oidcAllowedIssuers: https://issuer.example.com
  oidcAudience: orca-managed-agents
  oidcResolveWorkspaceByAudience: true
`,
      expected:
        /registry\.oidcAudience must be empty when registry\.oidcResolveWorkspaceByAudience=true/,
    },
  ];

  for (const { values, expected } of cases) {
    const valuesPath = writeValues(`${minimalValues}\n${values}`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );

    assert.notEqual(result.status, 0);
    assert.match(result.stderr, expected);
  }

  // The matching pair of accepted configurations, so the failures above pin the
  // combination and not the keys themselves.
  assert.match(
    helmTemplate(`
registry:
  oidcAllowedIssuers: https://issuer.example.com
  oidcAudience: orca-managed-agents
`),
    /OIDC_AUDIENCE: "orca-managed-agents"/,
  );
  assert.match(
    helmTemplate(`
registry:
  oidcAllowedIssuers: https://issuer.example.com
  oidcAudience: ''
  oidcResolveWorkspaceByAudience: true
`),
    /OIDC_RESOLVE_WORKSPACE_BY_AUDIENCE: "true"/,
  );
});

test('registry admin and platform audiences are required alongside their issuers', () => {
  // Neither plane resolves workspaces, so each has only the dangerous half of
  // the rule above: issuers configured with an empty audience is a plane that
  // accepts every token an allowed issuer minted for any relying party. The
  // registry refuses to start on it, so rendering it would swap a running pod
  // for a crash-looping one.
  for (const [values, expected] of [
    [
      `
registry:
  adminOidcAllowedIssuers: https://admin-issuer.example.com
  adminOidcAudience: ''
`,
      /registry\.adminOidcAudience is required when registry\.adminOidcAllowedIssuers is set/,
    ],
    [
      `
registry:
  platformOidcAllowedIssuers: https://platform-issuer.example.com
  platformOidcAudience: ''
`,
      /registry\.platformOidcAudience is required when registry\.platformOidcAllowedIssuers is set/,
    ],
  ]) {
    const valuesPath = writeValues(`${minimalValues}\n${values}`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );

    assert.notEqual(result.status, 0);
    assert.match(result.stderr, expected);
  }

  // Each plane's issuers gate only its own audience, so the accepted pairing
  // renders on both.
  const configured = helmTemplate(`
registry:
  adminOidcAllowedIssuers: https://admin-issuer.example.com
  adminOidcAudience: orca-managed-agents-admin
  platformOidcAllowedIssuers: https://platform-issuer.example.com
  platformOidcAudience: orca-managed-agents-platform
`);
  assert.match(configured, /ADMIN_OIDC_AUDIENCE: "orca-managed-agents-admin"/);
  assert.match(configured, /PLATFORM_OIDC_AUDIENCE: "orca-managed-agents-platform"/);

  // A deployment that delivers every plane's issuers and audience from a Secret
  // spells none of them out in the values, and Helm cannot read what it renders
  // a secretKeyRef to. That shape must render: the render-time check is gated on
  // the LITERAL issuers, and the startup assertion covers the Secret contents.
  const secretSourced = helmTemplate(`
registry:
  oidcAllowedIssuers: ''
  oidcAudience: ''
  adminOidcAllowedIssuers: ''
  adminOidcAudience: ''
  platformOidcAllowedIssuers: ''
  platformOidcAudience: ''
  secretKeyRefs:
    OIDC_ALLOWED_ISSUERS:
      name: orca-registry-oidc
      key: OIDC_ALLOWED_ISSUERS
    OIDC_AUDIENCE:
      name: orca-registry-oidc
      key: OIDC_AUDIENCE
    ADMIN_OIDC_ALLOWED_ISSUERS:
      name: orca-registry-oidc
      key: ADMIN_OIDC_ALLOWED_ISSUERS
    ADMIN_OIDC_AUDIENCE:
      name: orca-registry-oidc
      key: ADMIN_OIDC_AUDIENCE
    PLATFORM_OIDC_ALLOWED_ISSUERS:
      name: orca-registry-oidc
      key: PLATFORM_OIDC_ALLOWED_ISSUERS
    PLATFORM_OIDC_AUDIENCE:
      name: orca-registry-oidc
      key: PLATFORM_OIDC_AUDIENCE
`);
  const registry = document(secretSourced, 'Deployment', 'orca-managed-agents-registry');
  for (const key of [
    'OIDC_AUDIENCE',
    'ADMIN_OIDC_ALLOWED_ISSUERS',
    'ADMIN_OIDC_AUDIENCE',
    'PLATFORM_OIDC_ALLOWED_ISSUERS',
    'PLATFORM_OIDC_AUDIENCE',
  ]) {
    assert.match(
      registry,
      new RegExp(
        `- name: ${key}\\n\\s+valueFrom:\\n\\s+secretKeyRef:\\n\\s+name: orca-registry-oidc\\n\\s+key: ${key}`,
      ),
    );
  }
  // The env entries win over the ConfigMap the chart still renders from the
  // (empty) values, which is why an empty literal is not the running config.
  assert.match(secretSourced, /ADMIN_OIDC_AUDIENCE: ""/);

  // Mixed sources render too: a literal issuer with a Secret-backed audience is
  // a valid deployment (Kubernetes resolves the explicit env entry from the
  // Secret, overriding the ConfigMap's empty literal), so the empty literal
  // audience must not fail the render — the startup assertion reads the real
  // value. The same deferral applies when extraEnvFrom can override the
  // ConfigMap, and to the workspace plane's check.
  const mixed = helmTemplate(`
registry:
  oidcAllowedIssuers: https://issuer.example.com
  oidcAudience: ''
  adminOidcAllowedIssuers: https://admin-issuer.example.com
  adminOidcAudience: ''
  secretKeyRefs:
    OIDC_AUDIENCE:
      name: orca-registry-oidc
      key: OIDC_AUDIENCE
    ADMIN_OIDC_AUDIENCE:
      name: orca-registry-oidc
      key: ADMIN_OIDC_AUDIENCE
`);
  const mixedRegistry = document(mixed, 'Deployment', 'orca-managed-agents-registry');
  assert.match(
    mixedRegistry,
    /- name: ADMIN_OIDC_AUDIENCE\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-registry-oidc/,
  );
  assert.match(
    mixedRegistry,
    /- name: OIDC_AUDIENCE\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-registry-oidc/,
  );

  const envFromOverride = helmTemplate(`
registry:
  oidcAllowedIssuers: https://issuer.example.com
  oidcAudience: ''
  adminOidcAllowedIssuers: https://admin-issuer.example.com
  adminOidcAudience: ''
  extraEnvFrom:
    - secretRef:
        name: orca-registry-oidc
`);
  document(envFromOverride, 'Deployment', 'orca-managed-agents-registry');

  // But an override Helm CAN see through must not weaken the check: an extraEnv
  // entry whose literal value is empty is the exact misconfiguration the
  // render-time rule exists for — it renders, it wins over the ConfigMap, and
  // the startup assertion would crash-loop on it.
  const extraEnvEmpty = spawnSync(
    'helm',
    [
      'template',
      'orca-managed-agents',
      chartDir,
      '-f',
      writeValues(`${minimalValues}
registry:
  adminOidcAllowedIssuers: https://admin-issuer.example.com
  extraEnv:
    - name: ADMIN_OIDC_AUDIENCE
      value: ''
`),
    ],
    { cwd: process.cwd(), encoding: 'utf8' },
  );
  assert.notEqual(extraEnvEmpty.status, 0);
  assert.match(
    extraEnvEmpty.stderr,
    /registry\.extraEnv sets ADMIN_OIDC_AUDIENCE to an empty string/,
  );
  // and the same on the workspace plane
  const extraEnvEmptyWorkspace = spawnSync(
    'helm',
    [
      'template',
      'orca-managed-agents',
      chartDir,
      '-f',
      writeValues(`${minimalValues}
registry:
  oidcAllowedIssuers: https://issuer.example.com
  extraEnv:
    - name: OIDC_AUDIENCE
      value: ''
`),
    ],
    { cwd: process.cwd(), encoding: 'utf8' },
  );
  assert.notEqual(extraEnvEmptyWorkspace.status, 0);
  assert.match(
    extraEnvEmptyWorkspace.stderr,
    /registry\.extraEnv sets OIDC_AUDIENCE to an empty string/,
  );

  // A prefixed extraEnvFrom source cannot override the bare keys (it writes
  // `<prefix><KEY>` instead), so it must not disable the literal check either —
  // the literal audience stays empty and the render rejects it. The prefix sits
  // beside secretRef on the EnvFromSource, per the Kubernetes schema.
  const prefixedEnvFrom = spawnSync(
    'helm',
    [
      'template',
      'orca-managed-agents',
      chartDir,
      '-f',
      writeValues(`${minimalValues}
registry:
  adminOidcAllowedIssuers: https://admin-issuer.example.com
  adminOidcAudience: ''
  extraEnvFrom:
    - prefix: oidc_
      secretRef:
        name: orca-registry-oidc
`),
    ],
    { cwd: process.cwd(), encoding: 'utf8' },
  );
  assert.notEqual(prefixedEnvFrom.status, 0);
  assert.match(
    prefixedEnvFrom.stderr,
    /registry\.adminOidcAudience is required when registry\.adminOidcAllowedIssuers is set/,
  );
});

test('all object-store consumers receive one canonical workspace root', () => {
  const rendered = helmTemplate();

  assert.match(rendered, /S3_KEY_PREFIX: "managed-agents\/"/);
  assert.doesNotMatch(rendered, /MEMORY_KEY_PREFIX:/);
  assert.doesNotMatch(rendered, /OUTPUTS_KEY_PREFIX:/);
  assert.match(rendered, /S3_STS_ROLE_ARN: "arn:aws:iam::123456789012:role\/orca-session-s3"/);
});

test('OpenSandbox exposes one endpoint and no FUSE opt-out', () => {
  const rendered = helmTemplate();
  assert.match(rendered, /OPEN_SANDBOX_DOMAIN:/);
  assert.doesNotMatch(rendered, /OPEN_SANDBOX_ENABLE_FUSE:/);
});

test('AgentENV runtime settings and API key are wired into the harness', () => {
  const rendered = helmTemplate(`
harness:
  sandboxRuntime: agentenv
  agentEnv:
    baseUrl: http://agentenv-gateway.agentenv-system.svc:8080
    image: ghcr.io/orca-ae/orca-agentenv@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    timeoutSeconds: 900
    requestTimeoutSeconds: 120
    cpuCount: 2
    memoryMB: 2048
    diskSizeMB: 65536
secrets:
  values:
    sessionJwtPrivateKeyPem: test-private-key
    sessionJwtPublicKeyPem: test-public-key
    agentEnvApiKey: agentenv-secret
`);

  assert.match(rendered, /SANDBOX_RUNTIME: "agentenv"/);
  assert.match(
    rendered,
    /AGENTENV_BASE_URL: "http:\/\/agentenv-gateway\.agentenv-system\.svc:8080"/,
  );
  assert.match(rendered, /AGENTENV_IMAGE: "ghcr\.io\/orca-ae\/orca-agentenv@sha256:a{64}"/);
  assert.match(rendered, /AGENTENV_TIMEOUT_SECONDS: "900"/);
  assert.match(rendered, /AGENTENV_REQUEST_TIMEOUT_SECONDS: "120"/);
  assert.match(rendered, /AGENTENV_CPU_COUNT: "2"/);
  assert.match(rendered, /AGENTENV_MEMORY_MB: "2048"/);
  assert.match(rendered, /AGENTENV_DISK_SIZE_MB: "65536"/);
  assert.match(rendered, /AGENTENV_API_KEY: "agentenv-secret"/);
  assert.match(rendered, /name: AGENTENV_API_KEY/);
});

test('STS endpoint override is independent from the S3 data-plane endpoint', () => {
  const defaultRendered = helmTemplate();
  assert.match(defaultRendered, /S3_ENDPOINT: "http:\/\/minio\.minio\.svc\.cluster\.local:9000"/);
  assert.match(defaultRendered, /S3_STS_ENDPOINT: ""/);

  const overridden = helmTemplate(`
objectStorage:
  stsEndpoint: http://minio-sts.minio.svc.cluster.local:9000
  stsRoleArn: arn:aws:iam::123456789012:role/orca-session-s3
`);
  assert.match(overridden, /S3_ENDPOINT: "http:\/\/minio\.minio\.svc\.cluster\.local:9000"/);
  assert.match(
    overridden,
    /S3_STS_ENDPOINT: "http:\/\/minio-sts\.minio\.svc\.cluster\.local:9000"/,
  );
});

test('AWS IRSA deployment omits static S3 credentials', () => {
  const rendered = helmTemplate(`
objectStorage:
  endpoint: https://s3.us-west-1.amazonaws.com
  forcePathStyle: false
  region: us-west-1
  stsRoleArn: arn:aws:iam::123456789012:role/orca-session-s3
registry:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/orca-registry
harness:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/orca-harness
`);
  const documents = rendered.split(/\n---\n/);
  const appSecret = documents.find(
    (document) =>
      document.includes('kind: Secret\n') && document.includes('SESSION_JWT_PRIVATE_KEY_PEM:'),
  );

  assert.ok(appSecret);
  assert.match(rendered, /S3_ENDPOINT: "https:\/\/s3\.us-west-1\.amazonaws\.com"/);
  assert.match(rendered, /S3_FORCE_PATH_STYLE: "false"/);
  assert.match(rendered, /NODE_ENV: "production"/);
  assert.match(rendered, /ALLOW_INSECURE_STATIC_S3_CREDS: "false"/);
  assert.doesNotMatch(appSecret, /S3_ACCESS_KEY|S3_SECRET/);
  assert.match(
    rendered,
    /eks\.amazonaws\.com\/role-arn: arn:aws:iam::123456789012:role\/orca-registry/,
  );
  assert.match(
    rendered,
    /eks\.amazonaws\.com\/role-arn: arn:aws:iam::123456789012:role\/orca-harness/,
  );
});

test('chart requires STS for sandbox object-store credentials', () => {
  const valuesPath = writeValues(`${minimalValues}
objectStorage:
  stsRoleArn: ""
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /objectStorage\.stsRoleArn is required/);
});

test('chart rejects partial inline static S3 credentials even when STS is configured', () => {
  for (const partialCredential of [
    '    s3AccessKeyId: partial-access',
    '    s3SecretAccessKey: partial-secret',
  ]) {
    const valuesPath = writeValues(`${minimalValues}
secrets:
  values:
    sessionJwtPrivateKeyPem: test-private-key
    sessionJwtPublicKeyPem: test-public-key
${partialCredential}
`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );

    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /s3AccessKeyId and secrets\.values\.s3SecretAccessKey/);
  }
});

test('chart permits explicitly configured static S3 credentials only as a development fallback', () => {
  const rendered = helmTemplate(`
objectStorage:
  stsRoleArn: ""
secrets:
  values:
    sessionJwtPrivateKeyPem: test-private-key
    sessionJwtPublicKeyPem: test-public-key
    s3AccessKeyId: minioadmin
    s3SecretAccessKey: minioadmin
`);

  assert.match(rendered, /NODE_ENV: "development"/);
  assert.match(rendered, /ALLOW_INSECURE_STATIC_S3_CREDS: "true"/);
  assert.match(rendered, /S3_STS_ROLE_ARN: ""/);
});

test('harness receives only its allowlisted secret keys', () => {
  const rendered = helmTemplate();
  const harness = rendered.match(
    /kind: Deployment[\s\S]*?name: orca-managed-agents-harness[\s\S]*?(?=\n---|$)/,
  )?.[0];

  assert.ok(harness);
  assert.doesNotMatch(harness, /secretRef:/);
  assert.doesNotMatch(harness, /SESSION_JWT_PRIVATE_KEY_PEM/);
  assert.doesNotMatch(harness, /MEMORYSTORE_DATABASE_URL/);
  assert.doesNotMatch(harness, /\n\s+- name: DATABASE_URL/);
  assert.match(harness, /key: TRANSCRIPT_STORE_DATABASE_URL/);
  assert.match(harness, /key: FILESTORE_DATABASE_URL/);
});

test('postgres transcript backend requires its mounted Secret key', () => {
  const rendered = helmTemplate(`
transcriptStore:
  backend: postgres
`);
  const harness = rendered.match(
    /kind: Deployment[\s\S]*?name: orca-managed-agents-harness[\s\S]*?(?=\n---|$)/,
  )?.[0];

  assert.ok(harness);
  assert.match(harness, /key: TRANSCRIPT_STORE_DATABASE_URL\n\s+optional: false/);
  assert.match(
    rendered,
    /TRANSCRIPT_STORE_DATABASE_URL: "postgres:\/\/orca:secret@postgres\.example:5432\/registry"/,
  );
});

test('registry uses a persistent Kubernetes SecretStore with least-privilege RBAC', () => {
  const rendered = helmTemplate();

  assert.match(rendered, /ORCA_SECRET_STORE_MODE: "kubernetes"/);
  assert.match(rendered, /ORCA_SECRET_STORE_K8S_NAMESPACE: "orca-test"/);
  assert.match(rendered, /ORCA_SECRET_STORE_K8S_SECRET_NAME: "orca-managed-agents-secret-store"/);
  assert.match(
    rendered,
    /kind: Role[\s\S]*resourceNames: \["orca-managed-agents-secret-store"\][\s\S]*verbs: \["get", "patch"\]/,
  );
  assert.match(
    rendered,
    /kind: Deployment[\s\S]*name: orca-managed-agents-registry[\s\S]*serviceAccountName: orca-managed-agents-registry/,
  );
});

test('registry, harness, ai-gateway, dormant exporter, and toolset use distinct workload identities', () => {
  const rendered = helmTemplate(`
registry:
  serviceAccount:
    name: registry-identity
    annotations:
      example.com/workload: registry
harness:
  serviceAccount:
    name: harness-identity
    annotations:
      example.com/workload: harness
aiGateway:
  serviceAccount:
    name: ai-gateway-identity
    annotations:
      example.com/workload: ai-gateway
observabilityExporter:
  serviceAccount:
    name: observability-exporter-identity
    annotations:
      example.com/workload: observability-exporter
toolset:
  registryUrl: https://registry.example.com
  accessToken:
    secretKeyRef:
      name: orca-toolset-access
      key: access-token
  serviceAccount:
    name: toolset-identity
    annotations:
      example.com/workload: toolset
migrations:
  registry:
    initContainer:
      enabled: false
    job:
      enabled: true
`);

  const documents = rendered.split(/\n---\n/);
  const resource = (kind, name) => {
    const document = documents.find(
      (candidate) =>
        candidate.includes(`kind: ${kind}\n`) && candidate.includes(`  name: ${name}\n`),
    );
    assert.ok(document, `${kind} ${name} was not rendered`);
    return document;
  };
  const serviceAccountNames = [
    ...rendered.matchAll(/kind: ServiceAccount\nmetadata:\n  name: ([^\n]+)/g),
  ]
    .map((match) => match[1])
    .sort();
  assert.deepEqual(serviceAccountNames, [
    'ai-gateway-identity',
    'harness-identity',
    'observability-exporter-identity',
    'registry-identity',
    'toolset-identity',
  ]);
  assert.match(resource('ServiceAccount', 'registry-identity'), /example\.com\/workload: registry/);
  assert.match(resource('ServiceAccount', 'harness-identity'), /example\.com\/workload: harness/);
  assert.match(
    resource('ServiceAccount', 'ai-gateway-identity'),
    /example\.com\/workload: ai-gateway/,
  );
  assert.match(
    resource('ServiceAccount', 'observability-exporter-identity'),
    /example\.com\/workload: observability-exporter/,
  );
  assert.match(resource('ServiceAccount', 'toolset-identity'), /example\.com\/workload: toolset/);
  assert.match(
    resource('Deployment', 'orca-managed-agents-registry'),
    /serviceAccountName: registry-identity/,
  );
  assert.match(
    resource('Deployment', 'orca-managed-agents-harness'),
    /serviceAccountName: harness-identity/,
  );
  assert.match(
    resource('Deployment', 'orca-managed-agents-ai-gateway'),
    /serviceAccountName: ai-gateway-identity/,
  );
  assert.match(
    resource('Deployment', 'orca-managed-agents-toolset'),
    /serviceAccountName: toolset-identity/,
  );
  assert.match(
    resource('Job', 'orca-managed-agents-registry-migrate'),
    /serviceAccountName: registry-identity/,
  );
  assert.match(
    rendered,
    /INTERNAL_AUTH_HARNESS_SUBJECT: "system:serviceaccount:orca-test:harness-identity"/,
  );
  assert.match(
    rendered,
    /INTERNAL_AUTH_AI_GATEWAY_SUBJECT: "system:serviceaccount:orca-test:ai-gateway-identity"/,
  );
  assert.match(
    rendered,
    /INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT: "system:serviceaccount:orca-test:observability-exporter-identity"/,
  );
  assert.equal(
    documents.some(
      (candidate) =>
        candidate.includes('kind: Deployment\n') && candidate.includes('observability-exporter'),
    ),
    false,
  );
  assert.equal(
    documents.some(
      (candidate) =>
        candidate.includes('kind: Service\n') && candidate.includes('observability-exporter'),
    ),
    false,
  );
  assert.equal(
    documents.some(
      (candidate) =>
        /kind: (?:RoleBinding|ClusterRoleBinding)\n/.test(candidate) &&
        candidate.includes('observability-exporter-identity'),
    ),
    false,
  );
});

test('old reused values default a dormant exporter identity without a workload or RBAC', () => {
  for (const extraValues of [
    '',
    `observabilityExporter: null`,
    `observabilityExporter:
  serviceAccount: null`,
  ]) {
    const rendered = helmTemplate(extraValues);
    const documents = rendered.split(/\n---\n/);

    document(rendered, 'ServiceAccount', 'orca-managed-agents-observability-exporter');
    assert.match(
      rendered,
      /INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT: "system:serviceaccount:orca-test:orca-managed-agents-observability-exporter"/,
    );
    assert.equal(
      documents.some(
        (candidate) =>
          candidate.includes('kind: Deployment\n') && candidate.includes('observability-exporter'),
      ),
      false,
    );
    assert.equal(
      documents.some(
        (candidate) =>
          /kind: (?:RoleBinding|ClusterRoleBinding)\n/.test(candidate) &&
          candidate.includes('observability-exporter'),
      ),
      false,
    );
  }
});

test('internal auth projects audience-bound ServiceAccount tokens and grants TokenReview', () => {
  const rendered = helmTemplate();

  assert.match(rendered, /INTERNAL_AUTH_MODE: "kubernetes_service_account"/);
  assert.match(rendered, /INTERNAL_AUTH_AUDIENCE: "orca-registry-internal"/);
  assert.match(
    rendered,
    /INTERNAL_AUTH_OBSERVABILITY_EXPORTER_SUBJECT: "system:serviceaccount:orca-test:orca-managed-agents-observability-exporter"/,
  );
  assert.match(
    rendered,
    /INTERNAL_SERVICE_TOKEN_FILE: "\/var\/run\/secrets\/orca\/registry-internal\/token"/,
  );
  assert.match(
    rendered,
    /bearer_token_file: "\/var\/run\/secrets\/orca\/registry-internal\/token"/,
  );
  assert.equal(
    [
      ...rendered.matchAll(
        /serviceAccountToken:\n\s+path: token\n\s+audience: "orca-registry-internal"\n\s+expirationSeconds: 600/g,
      ),
    ].length,
    2,
  );
  assert.equal(
    [...rendered.matchAll(/mountPath: "\/var\/run\/secrets\/orca\/registry-internal"/g)].length,
    2,
  );
  assert.match(
    rendered,
    /kind: ClusterRoleBinding[\s\S]*name: orca-managed-agents-orca-test-registry-tokenreview[\s\S]*name: orca-managed-agents-registry[\s\S]*namespace: orca-test[\s\S]*name: system:auth-delegator/,
  );
});

test('cluster-scoped TokenReview bindings are unique across release namespaces', () => {
  const bindingName = (rendered) =>
    rendered.match(/kind: ClusterRoleBinding\nmetadata:\n  name: ([^\n]+)/)?.[1];
  const first = bindingName(helmTemplate('', 'namespace-a'));
  const second = bindingName(helmTemplate('', 'namespace-b'));

  assert.equal(first, 'orca-managed-agents-namespace-a-registry-tokenreview');
  assert.equal(second, 'orca-managed-agents-namespace-b-registry-tokenreview');
  assert.notEqual(first, second);
});

test('chart rejects unsafe internal auth settings', () => {
  for (const extraValues of [
    `internalAuth:\n  mode: static_token`,
    `internalAuth:\n  tokenExpirationSeconds: 599`,
    `internalAuth:\n  tokenExpirationSeconds: 3601`,
    `podSecurityContext: null`,
    `podSecurityContext:\n  fsGroup: null`,
  ]) {
    const valuesPath = writeValues(`${minimalValues}\n${extraValues}\n`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );
    assert.notEqual(result.status, 0);
    assert.match(
      result.stderr,
      /internalAuth\.(mode|tokenExpirationSeconds)|podSecurityContext\.fsGroup/,
    );
  }
});

test('chart rejects a colliding external dormant exporter identity', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  serviceAccount:
    name: shared-identity
observabilityExporter:
  serviceAccount:
    create: false
    name: shared-identity
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /registry, harness, aiGateway, and observabilityExporter service account names must be distinct/,
  );
});

test('chart requires an explicit dormant exporter identity when ServiceAccount creation is disabled', () => {
  const valuesPath = writeValues(`${minimalValues}
observabilityExporter:
  serviceAccount:
    create: false
    name: ''
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /observabilityExporter\.serviceAccount\.name is required when create=false/,
  );
});

test('chart rejects the in-process LocalSecretStore', () => {
  const valuesPath = writeValues(`${minimalValues}
secretStore:
  mode: local
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /secretStore\.mode=local is development-only/);
});

test('chart rejects unsupported SecretStore modes', () => {
  const valuesPath = writeValues(`${minimalValues}
secretStore:
  mode: vault
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /secretStore\.mode must be one of: none, kubernetes/);
});

test('ai-gateway renders exact MCP overrides before the dynamic Registry wildcard', () => {
  const rendered = helmTemplate();

  assert.match(rendered, /destinations:\n\s+"\*":/);
  assert.doesNotMatch(rendered, /base_url: "https:\/\/api\.githubcopilot\.com/);
  assert.match(rendered, /targets:\n\s+- destination: "\*"/);
  assert.match(
    rendered,
    /scope_dims: \["workspace_id","session_id","org_id","agent_id","llm_routes","llm_models","mcp_server_names","vault_ids","credential_ids"\]/,
  );
  assert.match(rendered, /org_id: org_id/);
  assert.match(rendered, /llm_routes: llm_routes/);
  assert.match(rendered, /llm_models: llm_models/);
  assert.match(rendered, /resource_name_in_scope_list:\n\s+scope: llm_models/);
  assert.match(rendered, /resource_name_in_scope_list:\n\s+scope: llm_routes/);
  assert.match(rendered, /destination_resolver:\n\s+kind: http/);
  assert.match(
    rendered,
    /\/internal\/v1\/workspaces\/\{scope\.workspace_id\}\/sessions\/\{scope\.session_id\}\/mcp-destination\/resolve/,
  );
  assert.match(rendered, /allowed_private_hosts: \[\]/);
  assert.match(rendered, /dns_timeout_ms: 2000/);
  assert.match(rendered, /connect_timeout_ms: 5000/);
  assert.match(rendered, /credential_ids: credential_ids/);
  assert.match(
    rendered,
    /header: X-Orca-Credential-Id\n\s+scope: credential_ids\n\s+if_missing: allow/,
  );
  assert.match(rendered, /AI_GATEWAY_URL: "http:\/\/orca-managed-agents-ai-gateway:8090"/);
  assert.match(
    rendered,
    /\/internal\/v1\/workspaces\/\{scope\.workspace_id\}\/sessions\/\{scope\.session_id\}\/vault-credentials\/\{credential_id\}\/resolve/,
  );
  assert.match(
    rendered,
    /bearer_token_file: "\/var\/run\/secrets\/orca\/registry-internal\/token"/,
  );
});

test('ai-gateway exact MCP overrides precede wildcard route target', () => {
  const rendered = helmTemplate(`
aiGateway:
  destinationResolver:
    timeoutMs: 7000
    egress:
      allowedPrivateHosts: [mcp.internal.example]
      dnsTimeoutMs: 1500
      connectTimeoutMs: 2500
  destinations:
    internal-mcp:
      baseUrl: https://mcp.internal.example/mcp
      credentialsVault: registry-vaults
`);

  assert.match(
    rendered,
    /internal-mcp:[\s\S]*?base_url: "https:\/\/mcp\.internal\.example\/mcp"[\s\S]*?"\*":[\s\S]*?timeout_ms: 7000/,
  );
  assert.match(rendered, /allowed_private_hosts: \["mcp.internal.example"\]/);
  assert.match(rendered, /dns_timeout_ms: 1500/);
  assert.match(rendered, /connect_timeout_ms: 2500/);
  assert.match(rendered, /targets:\n\s+- destination: internal-mcp\n\s+- destination: "\*"/);
});

test('ai-gateway resolver defaults survive reused values without the new block', () => {
  const rendered = helmTemplate(`
aiGateway:
  destinationResolver: null
  registryUsage: null
`);

  assert.match(rendered, /timeout_ms: 5000/);
  assert.match(rendered, /allowed_private_hosts: \[\]/);
  assert.match(rendered, /dns_timeout_ms: 2000/);
  assert.match(rendered, /connect_timeout_ms: 5000/);
  assert.match(rendered, /AI_GATEWAY_REGISTRY_USAGE_ENABLED: "false"/);
  assert.doesNotMatch(rendered, /usage_sinks:/);
});

test('ai-gateway scope dimensions retain every required resolver and ACL scope', () => {
  const valuesPath = writeValues(`${minimalValues}
aiGateway:
  scopeDims:
    - workspace_id
    - session_id
    - org_id
    - mcp_server_names
    - vault_ids
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /aiGateway\.scopeDims must include required dimension "credential_ids"/,
  );

  const extended = helmTemplate(`
aiGateway:
  scopeDims:
    - workspace_id
    - session_id
    - org_id
    - mcp_server_names
    - vault_ids
    - credential_ids
    - llm_routes
`);
  assert.match(extended, /llm_routes: llm_routes/);
});

test('ai-gateway reserves literal wildcard destination name', () => {
  const valuesPath = writeValues(`${minimalValues}
aiGateway:
  destinations:
    "*":
      baseUrl: https://invalid.example/mcp
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /'\*' is reserved/);
});

test('ai-gateway audit Kafka sink renders TLS, SASL, and timeout config', () => {
  const rendered = helmTemplate(`
aiGateway:
  audit:
    kafka:
      messageTimeoutMs: 12000
      ssl: true
      sslCaLocation: /etc/kafka/ca.pem
      sasl:
        enabled: true
        mechanism: PLAIN
        username: public
        password: token:jwt
`);

  assert.match(rendered, /message_timeout_ms: 12000/);
  assert.match(rendered, /ssl: true/);
  assert.match(rendered, /ssl_ca_location: "\/etc\/kafka\/ca\.pem"/);
  assert.match(
    rendered,
    /sasl:\n\s+mechanism: "PLAIN"\n\s+username: "public"\n\s+password: "token:jwt"/,
  );
});

test('Registry usage authority switches only with the rendered ai-gateway sink', () => {
  const disabled = helmTemplate();
  const disabledRegistry = document(disabled, 'ConfigMap', 'orca-managed-agents-registry-config');
  const disabledGateway = document(disabled, 'ConfigMap', 'orca-managed-agents-ai-gateway-config');
  assert.match(disabledRegistry, /AI_GATEWAY_REGISTRY_USAGE_ENABLED: "false"/);
  assert.doesNotMatch(disabledGateway, /usage_sinks:/);

  const enabled = helmTemplate(`
aiGateway:
  registryUsage:
    enabled: true
    baseUrl: https://registry-internal.example
    timeoutMs: 4500
    buffer: 2048
`);
  const enabledRegistry = document(enabled, 'ConfigMap', 'orca-managed-agents-registry-config');
  const enabledGateway = document(enabled, 'ConfigMap', 'orca-managed-agents-ai-gateway-config');
  const enabledGatewayDeployment = document(
    enabled,
    'Deployment',
    'orca-managed-agents-ai-gateway',
  );
  assert.match(enabledRegistry, /AI_GATEWAY_REGISTRY_USAGE_ENABLED: "true"/);
  assert.match(enabledGateway, /usage_sinks:/);
  assert.match(enabledGateway, /kind: registry/);
  assert.match(enabledGateway, /base_url: "https:\/\/registry-internal\.example"/);
  assert.match(enabledGateway, /timeout_ms: 4500/);
  assert.match(enabledGateway, /buffer: 2048/);
  assert.match(
    enabledGatewayDeployment,
    /image: "ghcr\.io\/orca-ae\/orca-ai-gateway:v0\.4\.3-rc\.3"/,
  );
});

test('ai-gateway can resolve guardrail bundles from Registry without a file mount', () => {
  const disabled = document(helmTemplate(), 'ConfigMap', 'orca-managed-agents-ai-gateway-config');
  assert.doesNotMatch(disabled, /guardrail_source:/);

  const enabled = document(
    helmTemplate(`
aiGateway:
  registryGuardrails:
    enabled: true
    cacheTtlSecs: 15
    failureMode:
      llm_request: deny
    state:
      name: policy-state
      kind: memory
`),
    'ConfigMap',
    'orca-managed-agents-ai-gateway-config',
  );
  assert.match(enabled, /guardrail_source:\n\s+name: registry-policy\n\s+kind: registry/);
  assert.match(enabled, /cache_ttl_secs: 15/);
  assert.match(enabled, /on_backend_error: "deny"/);
  assert.match(enabled, /guardrails_policy:\n\s+mode: "enforce"/);
  assert.match(enabled, /failure_mode: \{"llm_request":"deny"\}/);
  assert.match(enabled, /guardrail_state:\n\s+kind: memory\n\s+name: policy-state/);
  assert.match(enabled, /agent_id: agent_id/);
  assert.doesNotMatch(enabled, /kind: file/);
});

test('registry guardrails require agent_id in gateway scope dimensions', () => {
  const valuesPath = writeValues(`${minimalValues}
aiGateway:
  registryGuardrails:
    enabled: true
  scopeDims:
    - workspace_id
    - session_id
    - org_id
    - mcp_server_names
    - vault_ids
    - credential_ids
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
    },
  );
  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /aiGateway\.scopeDims must include agent_id when registryGuardrails is enabled/,
  );
});

test('standalone registry migration Job has a cleanup TTL', () => {
  const rendered = helmTemplate(`
migrations:
  registry:
    initContainer:
      enabled: false
    job:
      enabled: true
`);

  assert.match(rendered, /kind: Job[\s\S]*ttlSecondsAfterFinished: 600[\s\S]*backoffLimit: 3/);
});

test('chart rejects enabling registry migration initContainer and Job together', () => {
  const valuesPath = writeValues(`${minimalValues}
migrations:
  registry:
    initContainer:
      enabled: true
    job:
      enabled: true
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    {
      cwd: process.cwd(),
      encoding: 'utf8',
    },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /only one of initContainer or job may be enabled/);
});

// --- externally delivered credentials + deployment-shape overrides ----------
// A bootstrap pipeline owns the Secrets; the chart must be able to consume them
// under their own names without creating any Secret of its own.

/** One rendered document, split on the YAML separator rather than by regex
 *  span: a `kind: Deployment[\s\S]*?name: X` match happily swallows every
 *  document between the first Deployment and X. */
function document(rendered, kind, name) {
  const found = rendered
    .split(/\n---\n/)
    .find(
      (candidate) =>
        candidate.includes(`kind: ${kind}\n`) && candidate.includes(`  name: ${name}\n`),
    );
  assert.ok(found, `${kind} ${name} was not rendered`);
  return found;
}

const requiredRegistryCredentialKeys = [
  'DATABASE_URL',
  'FILESTORE_DATABASE_URL',
  'MEMORYSTORE_DATABASE_URL',
  'SESSION_JWT_PRIVATE_KEY_PEM',
];

function externalPerKeyCredentialValues(keys) {
  const secretKeyRefs = keys
    .map(
      (key) => `    ${key}:
      name: orca-registry-credentials
      key: ${key}`,
    )
    .join('\n');
  return `
secrets:
  create: false
  existingSecret: ''
sessionJwt:
  publicKeySecret:
    name: orca-session-jwt
    key: public.pem
registry:
  secretKeyRefs:
${secretKeyRefs}
harness:
  extraEnvFrom:
    - secretRef:
        name: orca-harness-credentials
`;
}

const externalCredentialValues = `
secrets:
  create: false
  existingSecret: ''
objectStorage:
  stsRoleArn: arn:aws:iam::123456789012:role/orca-session-s3
sessionJwt:
  publicKeySecret:
    name: orca-session-jwt
    key: public.pem
registry:
  secretKeyRefs:
    DATABASE_URL:
      name: orca-postgres
      key: registry-url
  extraEnvFrom:
    - secretRef:
        name: orca-postgres
harness:
  extraEnvFrom:
    - secretRef:
        name: orca-harness-credentials
`;

test('externally delivered credentials replace the chart Secret entirely', () => {
  const rendered = helmTemplate(externalCredentialValues);

  assert.doesNotMatch(rendered, /name: orca-managed-agents-secrets/);
  assert.match(
    rendered,
    /- name: DATABASE_URL\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-postgres\n\s+key: registry-url/,
  );
  assert.match(rendered, /- secretRef:\n\s+name: orca-harness-credentials/);
  assert.match(rendered, /secretName: orca-session-jwt\n\s+items:\n\s+- key: public\.pem/);

  // Every harness credential arrives through the bundle, so no secretKeyRef is
  // left to render and an empty `env:` key would be invalid.
  const harness = document(rendered, 'Deployment', 'orca-managed-agents-harness');
  assert.doesNotMatch(harness, /\n\s+env:\n\s+volumeMounts:/);
  assert.doesNotMatch(harness, /secretKeyRef:/);
});

test('a separate database Secret satisfies DSN validation without a chart Secret', () => {
  const rendered = helmTemplate(`
external:
  databases:
    existingSecret: orca-postgres
transcriptStore:
  backend: postgres
secrets:
  create: false
  existingSecret: ''
sessionJwt:
  publicKeySecret:
    name: orca-session-jwt
    key: public.pem
registry:
  secretKeyRefs:
    SESSION_JWT_PRIVATE_KEY_PEM:
      name: orca-session-jwt
      key: private.pem
`);
  const registry = document(rendered, 'Deployment', 'orca-managed-agents-registry');
  const harness = document(rendered, 'Deployment', 'orca-managed-agents-harness');

  assert.doesNotMatch(rendered, /name: orca-managed-agents-secrets/);
  assert.match(registry, /- secretRef:\n\s+name: orca-postgres/);
  assert.match(
    registry,
    /- name: DATABASE_URL\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-postgres\n\s+key: DATABASE_URL/,
  );
  assert.match(
    registry,
    /- name: SESSION_JWT_PRIVATE_KEY_PEM\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-session-jwt\n\s+key: private.pem/,
  );
  assert.match(
    harness,
    /- name: TRANSCRIPT_STORE_DATABASE_URL\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-postgres\n\s+key: TRANSCRIPT_STORE_DATABASE_URL/,
  );
  assert.match(
    harness,
    /- name: FILESTORE_DATABASE_URL\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-postgres\n\s+key: FILESTORE_DATABASE_URL/,
  );
});

test('chart rejects a credential-less install instead of booting on localhost DSNs', () => {
  for (const [extraValues, expected] of [
    [`secrets:\n  create: false\n  existingSecret: ''`, /no credential source for the registry/],
    [
      `secrets:\n  create: false\n  existingSecret: ''\nregistry:\n  extraEnvFrom:\n    - secretRef:\n        name: orca-postgres`,
      /no credential source for the harness/,
    ],
  ]) {
    const valuesPath = writeValues(`${minimalValues}\n${extraValues}\n`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );

    assert.notEqual(result.status, 0);
    assert.match(result.stderr, expected);
  }
});

test('Postgres transcript backend requires a Harness transcript DSN source', () => {
  const valuesPath = writeValues(`${minimalValues}
secrets:
  create: false
  existingSecret: ''
sessionJwt:
  publicKeySecret:
    name: orca-session-jwt
    key: public.pem
transcriptStore:
  backend: postgres
registry:
  extraEnvFrom:
    - secretRef:
        name: orca-registry-credentials
harness:
  secretKeyRefs:
    FILESTORE_DATABASE_URL:
      name: orca-harness-credentials
      key: FILESTORE_DATABASE_URL
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /TRANSCRIPT_STORE_DATABASE_URL/);
});

test('registry rejects every missing per-key credential that would otherwise fall back locally', () => {
  for (const missingKey of requiredRegistryCredentialKeys) {
    const valuesPath = writeValues(
      `${minimalValues}${externalPerKeyCredentialValues(
        requiredRegistryCredentialKeys.filter((key) => key !== missingKey),
      )}`,
    );
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );

    assert.notEqual(result.status, 0, `${missingKey} unexpectedly passed validation`);
    assert.match(result.stderr, new RegExp(missingKey));
  }
});

test('registry renders every required per-key credential reference', () => {
  const rendered = helmTemplate(externalPerKeyCredentialValues(requiredRegistryCredentialKeys));
  const registry = document(rendered, 'Deployment', 'orca-managed-agents-registry');

  for (const key of requiredRegistryCredentialKeys) {
    assert.match(
      registry,
      new RegExp(
        `- name: ${key}\\n\\s+valueFrom:\\n\\s+secretKeyRef:\\n\\s+name: orca-registry-credentials\\n\\s+key: ${key}`,
      ),
    );
  }
});

test('empty per-key Secret references are rejected', () => {
  const valuesPath = writeValues(`${minimalValues}
secrets:
  create: false
  existingSecret: ''
sessionJwt:
  publicKeySecret:
    name: orca-session-jwt
    key: public.pem
registry:
  secretKeyRefs:
    DATABASE_URL: {}
  extraEnvFrom:
    - secretRef:
        name: orca-registry-credentials
harness:
  extraEnvFrom:
    - secretRef:
        name: orca-harness-credentials
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /secretKeyRefs\.DATABASE_URL requires a Secret name/);
});

test('operator-supplied envFrom bundles are appended after the chart ConfigMap', () => {
  const rendered = helmTemplate(`
registry:
  extraEnvFrom:
    - secretRef:
        name: transcript-store-config
`);
  // Kubernetes resolves duplicate keys in envFrom order, so the bundle has to
  // come last for a Secret-supplied backend selection to win.
  assert.match(
    rendered,
    /envFrom:\n\s+- configMapRef:\n\s+name: orca-managed-agents-registry-config\n\s+- secretRef:\n\s+name: orca-managed-agents-secrets\n\s+- secretRef:\n\s+name: transcript-store-config/,
  );
});

test('external Postgres TLS material mounts into both services', () => {
  const rendered = helmTemplate(`
registry:
  extraVolumes:
    - name: pg-ca
      secret:
        secretName: pg-ca
        optional: true
  extraVolumeMounts:
    - name: pg-ca
      mountPath: /etc/orca/pg-ca
      readOnly: true
harness:
  extraVolumes:
    - name: pg-ca
      secret:
        secretName: pg-ca
        optional: true
  extraVolumeMounts:
    - name: pg-ca
      mountPath: /etc/orca/pg-ca
      readOnly: true
`);

  assert.equal([...rendered.matchAll(/mountPath: \/etc\/orca\/pg-ca/g)].length, 3);
  assert.equal([...rendered.matchAll(/secretName: pg-ca/g)].length, 2);
});

test('built-in Postgres CA mount reserves the pg-ca volume name', () => {
  for (const [component, field] of [
    ['registry', 'extraVolumes'],
    ['registry', 'extraVolumeMounts'],
    ['harness', 'extraVolumes'],
    ['harness', 'extraVolumeMounts'],
  ]) {
    const item =
      field === 'extraVolumes'
        ? '      secret:\n        secretName: duplicate-pg-ca'
        : '      mountPath: /tmp/duplicate-pg-ca';
    const valuesPath = writeValues(`${minimalValues}
external:
  databases:
    registryUrl: postgres://orca:secret@postgres.example:5432/registry
    filestoreUrl: postgres://orca:secret@postgres.example:5432/filestore
    memorystoreUrl: postgres://orca:secret@postgres.example:5432/memorystore
    tls:
      caSecretName: orca-pg-ca
${component}:
  ${field}:
    - name: pg-ca
${item}
`);
    const result = spawnSync(
      'helm',
      ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
      { cwd: process.cwd(), encoding: 'utf8' },
    );

    assert.notEqual(result.status, 0, `${component}.${field} unexpectedly rendered`);
    assert.match(result.stderr, new RegExp(`${component}\\.${field}`));
  }
});

test('boot-time migrations are covered by a startup probe, not the liveness probe', () => {
  const rendered = helmTemplate();

  assert.equal(
    [...rendered.matchAll(/startupProbe:\n\s+failureThreshold: 30\n\s+httpGet:/g)].length,
    2,
  );

  const relaxed = helmTemplate(`
registry:
  startupProbe: null
`);
  assert.doesNotMatch(
    document(relaxed, 'Deployment', 'orca-managed-agents-registry'),
    /startupProbe:/,
  );
});

test('config and credential changes roll the fleet', () => {
  const first = helmTemplate();
  const second = helmTemplate(`
harness:
  sessionIdleTimeoutMs: '120000'
`);
  const checksum = (rendered, workload) =>
    document(rendered, 'Deployment', workload).match(/checksum\/config: (\w+)/)?.[1];

  assert.ok(checksum(first, 'orca-managed-agents-harness'));
  assert.notEqual(
    checksum(first, 'orca-managed-agents-harness'),
    checksum(second, 'orca-managed-agents-harness'),
  );
  assert.equal(
    checksum(first, 'orca-managed-agents-registry'),
    checksum(second, 'orca-managed-agents-registry'),
  );
});

test('an empty ANTHROPIC_BASE_URL is omitted rather than blanking the provider endpoint', () => {
  const rendered = helmTemplate();
  assert.doesNotMatch(rendered, /ANTHROPIC_BASE_URL/);

  const overridden = helmTemplate(`
harness:
  anthropicBaseUrl: http://ai-gateway:8090/v1
`);
  assert.match(overridden, /ANTHROPIC_BASE_URL: "http:\/\/ai-gateway:8090\/v1"/);
});

test('sandbox callbacks use a namespace-qualified Registry URL', () => {
  const rendered = helmTemplate('', 'orca-system');

  // OpenSandbox workloads run outside the release namespace, so the short
  // Service name would resolve somewhere else.
  assert.match(
    rendered,
    /GIT_CREDS_PUBLIC_URL: "http:\/\/orca-managed-agents-registry\.orca-system\.svc\.cluster\.local:8080\/v1\/git-creds"/,
  );
});

test('component names can be pinned to inherited in-cluster DNS names', () => {
  const rendered = helmTemplate(`
registry:
  nameOverride: registry
harness:
  nameOverride: harness
aiGateway:
  nameOverride: ai-gateway
`);

  assert.match(rendered, /kind: Service\nmetadata:\n  name: registry\n/);
  assert.match(rendered, /kind: Service\nmetadata:\n  name: registry-internal\n/);
  assert.match(rendered, /kind: Service\nmetadata:\n  name: registry-admin\n/);
  assert.match(rendered, /kind: Service\nmetadata:\n  name: harness\n/);
  assert.match(rendered, /kind: Service\nmetadata:\n  name: ai-gateway\n/);
  assert.match(rendered, /REGISTRY_INTERNAL_BASE_URL: "http:\/\/registry-internal:8081"/);
  assert.match(rendered, /AI_GATEWAY_URL: "http:\/\/ai-gateway:8090"/);

  const collision = writeValues(`${minimalValues}
registry:
  nameOverride: orca
harness:
  nameOverride: orca
`);
  const result = spawnSync('helm', ['template', 'orca-managed-agents', chartDir, '-f', collision], {
    cwd: process.cwd(),
    encoding: 'utf8',
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /nameOverride values must resolve to distinct names/);
});

test('long Registry overrides preserve distinct internal and admin suffixes', () => {
  const registryName = 'r'.repeat(63);
  const internalName = `${'r'.repeat(54)}-internal`;
  const adminName = `${'r'.repeat(57)}-admin`;
  const rendered = helmTemplate(`
registry:
  nameOverride: ${registryName}
`);

  document(rendered, 'Service', registryName);
  document(rendered, 'Service', internalName);
  document(rendered, 'Service', adminName);
  assert.notEqual(registryName, internalName);
  assert.notEqual(registryName, adminName);
  assert.match(
    rendered,
    new RegExp(`REGISTRY_INTERNAL_BASE_URL: "http:\\/\\/${internalName}:8081"`),
  );
});

test('component names cannot collide with Registry derived Service names', () => {
  const collision = writeValues(`${minimalValues}
registry:
  nameOverride: registry
harness:
  nameOverride: registry-internal
`);
  const result = spawnSync('helm', ['template', 'orca-managed-agents', chartDir, '-f', collision], {
    cwd: process.cwd(),
    encoding: 'utf8',
  });

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /nameOverride values must resolve to distinct names/);
});

test('an out-of-chart ai-gateway stays authorized without being deployed', () => {
  const rendered = helmTemplate(`
harness:
  aiGatewayUrl: http://ai-gateway:8080/v1/mcp
aiGateway:
  enabled: false
  serviceAccount:
    create: false
    name: ai-gateway
`);

  assert.doesNotMatch(rendered, /name: orca-managed-agents-ai-gateway\n/);
  assert.doesNotMatch(rendered, /ai-gateway-config/);
  assert.match(
    rendered,
    /INTERNAL_AUTH_AI_GATEWAY_SUBJECT: "system:serviceaccount:orca-test:ai-gateway"/,
  );
  assert.match(rendered, /AI_GATEWAY_URL: "http:\/\/ai-gateway:8080\/v1\/mcp"/);

  const valuesPath = writeValues(`${minimalValues}
aiGateway:
  enabled: false
  serviceAccount:
    create: false
    name: ai-gateway
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /harness\.aiGatewayUrl is required when aiGateway\.enabled=false/);
});

test('harness NODE_ENV override cannot contradict the static-credential guard', () => {
  const rendered = helmTemplate(`
objectStorage:
  stsRoleArn: ''
  allowStaticCredentials: true
harness:
  nodeEnv: test
`);
  assert.match(rendered, /NODE_ENV: "test"/);
  assert.match(rendered, /ALLOW_INSECURE_STATIC_S3_CREDS: "true"/);

  const valuesPath = writeValues(`${minimalValues}
objectStorage:
  stsRoleArn: ''
  allowStaticCredentials: true
harness:
  nodeEnv: production
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /harness\.nodeEnv must be development or test/);
});

test('sandbox pod sizing and MCP round-trip budget are configurable', () => {
  const rendered = helmTemplate(`
harness:
  openSandbox:
    resourceCpu: 250m
    resourceMemory: 512Mi
aiGateway:
  destinationResolver:
    egress:
      responseTimeoutMs: 120000
`);

  assert.match(rendered, /OPEN_SANDBOX_RESOURCE_CPU: "250m"/);
  assert.match(rendered, /OPEN_SANDBOX_RESOURCE_MEMORY: "512Mi"/);
  assert.match(rendered, /response_timeout_ms: 120000/);

  const defaults = helmTemplate();
  assert.doesNotMatch(defaults, /OPEN_SANDBOX_RESOURCE_/);
  assert.doesNotMatch(defaults, /response_timeout_ms/);
});

test('Chart accepts vendor-suffixed Kubernetes versions with networking.k8s.io/v1 Ingress', () => {
  const chart = readFileSync(join(chartDir, 'Chart.yaml'), 'utf8');

  assert.match(chart, /^kubeVersion:\s*(["']?)>=1\.19\.0-0\1\s*$/m);
});

test('helm test hook probes Registry and Harness without assuming an in-chart gateway', () => {
  const rendered = helmTemplate(`
imagePullSecrets:
  - name: private-registry
aiGateway:
  enabled: false
  serviceAccount:
    create: false
    name: external-gateway
harness:
  aiGatewayUrl: http://external-gateway:8080/v1/mcp
`);
  const hook = document(rendered, 'Pod', 'orca-managed-agents-test-connection');

  assert.match(hook, /"helm\.sh\/hook": test/);
  assert.match(hook, /"helm\.sh\/hook-delete-policy": before-hook-creation/);
  assert.doesNotMatch(hook, /hook-succeeded/);
  assert.match(hook, /image: "busybox:1\.36"/);
  assert.match(hook, /imagePullSecrets:\n\s+- name: private-registry/);
  assert.match(hook, /activeDeadlineSeconds: 180/);
  assert.match(hook, /automountServiceAccountToken: false/);
  assert.match(hook, /runAsNonRoot: true/);
  assert.match(hook, /runAsUser: 1000/);
  assert.match(hook, /runAsGroup: 1000/);
  assert.match(hook, /readOnlyRootFilesystem: true/);
  assert.match(hook, /allowPrivilegeEscalation: false/);
  assert.match(hook, /capabilities:\n\s+drop:\n\s+- ALL/);
  assert.match(hook, /wget -T 5 -q -O -/);
  assert.match(hook, /registry_pid=\$!/);
  assert.match(hook, /harness_pid=\$!/);
  assert.doesNotMatch(hook, /--tries|--timeout/);
  assert.match(hook, /http:\/\/orca-managed-agents-registry:8080\/readyz/);
  assert.match(hook, /http:\/\/orca-managed-agents-harness:9094\/readyz/);
  assert.doesNotMatch(hook, /ai-gateway/);

  const disabled = helmTemplate(`
tests:
  enabled: false
`);
  assert.doesNotMatch(disabled, /test-connection/);
});

test('per-pool connection ceilings render for both registry and harness', () => {
  const rendered = helmTemplate(`
databasePools:
  registryMax: 12
  transcriptStoreMax: 4
  fileStoreMax: 6
  memoryStoreMax: 3
`);
  const registryConfig = rendered.match(
    /kind: ConfigMap[\s\S]*?name: orca-managed-agents-registry-config[\s\S]*?(?=\n---|$)/,
  )?.[0];
  const harnessConfig = rendered.match(
    /kind: ConfigMap[\s\S]*?name: orca-managed-agents-harness-config[\s\S]*?(?=\n---|$)/,
  )?.[0];

  assert.ok(registryConfig);
  assert.match(registryConfig, /DATABASE_POOL_MAX: "12"/);
  assert.match(registryConfig, /TRANSCRIPT_STORE_POOL_MAX: "4"/);
  assert.match(registryConfig, /FILESTORE_POOL_MAX: "6"/);
  assert.match(registryConfig, /MEMORYSTORE_POOL_MAX: "3"/);

  assert.ok(harnessConfig);
  assert.match(harnessConfig, /TRANSCRIPT_STORE_POOL_MAX: "4"/);
  assert.match(harnessConfig, /FILESTORE_POOL_MAX: "6"/);
  // Harness never opens the registry main DB or the memory store.
  assert.doesNotMatch(harnessConfig, /DATABASE_POOL_MAX:/);
  assert.doesNotMatch(harnessConfig, /MEMORYSTORE_POOL_MAX:/);
});

test('default install mounts no Postgres CA volume', () => {
  const rendered = helmTemplate();
  assert.doesNotMatch(rendered, /name: pg-ca/);
});

test('Postgres CA bundle mounts into every component that opens a PG connection', () => {
  // Re-declare the full external.databases block: a second top-level `external:`
  // key would otherwise shadow minimalValues and drop the required DSNs.
  const rendered = helmTemplate(`
external:
  databases:
    registryUrl: postgres://orca:secret@postgres.example:5432/registry
    filestoreUrl: postgres://orca:secret@postgres.example:5432/filestore
    memorystoreUrl: postgres://orca:secret@postgres.example:5432/memorystore
    tls:
      caSecretName: orca-pg-ca
      caKey: root-ca.pem
migrations:
  registry:
    initContainer:
      enabled: false
    job:
      enabled: true
`);
  const registry = rendered.match(
    /kind: Deployment[\s\S]*?name: orca-managed-agents-registry[\s\S]*?(?=\n---|$)/,
  )?.[0];
  const harness = rendered.match(
    /kind: Deployment[\s\S]*?name: orca-managed-agents-harness[\s\S]*?(?=\n---|$)/,
  )?.[0];
  const migrateJob = rendered.match(
    /kind: Job[\s\S]*?name: orca-managed-agents-registry-migrate[\s\S]*?(?=\n---|$)/,
  )?.[0];

  for (const [name, doc] of [
    ['registry', registry],
    ['harness', harness],
    ['migrate job', migrateJob],
  ]) {
    assert.ok(doc, `${name} was not rendered`);
    assert.match(doc, /mountPath: "\/etc\/orca\/pg-ca"/, `${name} missing CA mount`);
    assert.match(doc, /secretName: "orca-pg-ca"/, `${name} missing CA volume`);
    assert.match(doc, /key: "root-ca\.pem"/, `${name} missing configured CA key`);
    assert.match(doc, /path: "root-ca\.pem"/, `${name} missing configured CA path`);
  }
});

test('a separate database Secret supplies registry, init migration, and harness DSNs', () => {
  const rendered = helmTemplate(`
external:
  databases:
    existingSecret: orca-postgres
registry:
  extraEnvFrom:
    - secretRef:
        name: conflicting-database-bundle
`);
  const applicationSecret = rendered
    .split(/\n---\n/)
    .find(
      (document) =>
        document.includes('kind: Secret\n') &&
        document.includes('  name: orca-managed-agents-secrets\n'),
    );
  const registry = rendered.match(
    /kind: Deployment[\s\S]*?name: orca-managed-agents-registry[\s\S]*?(?=\n---|$)/,
  )?.[0];
  const harness = rendered.match(
    /kind: Deployment[\s\S]*?name: orca-managed-agents-harness[\s\S]*?(?=\n---|$)/,
  )?.[0];

  assert.ok(applicationSecret);
  assert.ok(registry);
  assert.ok(harness);
  assert.doesNotMatch(
    applicationSecret,
    /^\s{2}(?:DATABASE_URL|TRANSCRIPT_STORE_DATABASE_URL|FILESTORE_DATABASE_URL|MEMORYSTORE_DATABASE_URL):/m,
  );
  assert.match(registry, /envFrom:[\s\S]*?- secretRef:\n\s+name: orca-postgres/);
  assert.match(registry, /- secretRef:\n\s+name: conflicting-database-bundle/);
  assert.equal(
    [
      ...registry.matchAll(
        /- name: DATABASE_URL\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-postgres\n\s+key: DATABASE_URL/g,
      ),
    ].length,
    2,
    'Registry app and init migration must use the selected database Secret',
  );
  assert.match(
    harness,
    /- name: TRANSCRIPT_STORE_DATABASE_URL\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-postgres\n\s+key: TRANSCRIPT_STORE_DATABASE_URL/,
  );
  assert.match(
    harness,
    /- name: FILESTORE_DATABASE_URL\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-postgres\n\s+key: FILESTORE_DATABASE_URL/,
  );
  assert.doesNotMatch(harness, /- secretRef:\n\s+name: orca-postgres/);
});

test('standalone registry migration uses the selected database Secret', () => {
  const rendered = helmTemplate(`
external:
  databases:
    existingSecret: orca-postgres
registry:
  extraEnvFrom:
    - secretRef:
        name: conflicting-database-bundle
migrations:
  registry:
    initContainer:
      enabled: false
    job:
      enabled: true
`);
  const migrateJob = rendered.match(
    /kind: Job[\s\S]*?name: orca-managed-agents-registry-migrate[\s\S]*?(?=\n---|$)/,
  )?.[0];

  assert.ok(migrateJob);
  assert.match(migrateJob, /- secretRef:\n\s+name: conflicting-database-bundle/);
  assert.match(
    migrateJob,
    /- name: DATABASE_URL\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: orca-postgres\n\s+key: DATABASE_URL/,
  );
});

test('registry migration uses a fail-fast Pool and one dedicated lock client', () => {
  const migrate = readFileSync('services/registry-service-ts/src/migrate.ts', 'utf8');

  assert.match(migrate, /connectionTimeoutMillis:\s*10_000/);
  assert.match(migrate, /const client = await pool\.connect\(\)/);
  assert.match(migrate, /migrate\(drizzle\(client\)/);
});

test('registry Istio exposure requires a gateway selector', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  istio:
    enabled: true
    host: agents.example.com
    gateway:
      tls:
        credentialName: agents-example-com-tls
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /registry\.istio\.gateway\.selector is required when registry\.istio\.enabled=true/,
  );
});

test('registry Istio exposure renders a path-scoping AuthorizationPolicy by default', () => {
  const rendered = helmTemplate(`
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  istio:
    enabled: true
    host: agents.example.com
    gateway:
      selector:
        istio: edge-ingressgateway
      tls:
        credentialName: agents-example-com-tls
`);
  const policy = document(rendered, 'AuthorizationPolicy', 'orca-managed-agents-registry-external');

  assert.match(policy, /apiVersion: security\.istio\.io\/v1beta1/);
  assert.match(policy, /namespace: istio-system/);
  assert.match(policy, /matchLabels:\n\s+istio: edge-ingressgateway/);
  assert.match(policy, /action: DENY/);
  assert.match(policy, /- "agents\.example\.com"/);
  assert.match(policy, /- "agents\.example\.com:\*"/);
  // DENY rules with HTTP-only attributes match all plain-TCP traffic, so the
  // rule must stay scoped by HTTPS port and SNI or it would black-hole
  // unrelated listeners on a shared gateway.
  assert.match(policy, /ports:\n\s+- "443"\n\s+- "8443"/);
  const notPaths = yaml.load(policy).spec.rules[0].to[0].operation.notPaths;
  assert.deepEqual(notPaths, ['/v1/*', '/api', '/api/v1/*', '/apis', '/apis/*']);
  assert.match(policy, /when:\n\s+- key: connection\.sni\n\s+values:\n\s+- "agents\.example\.com"/);
});

test('registry Istio AuthorizationPolicy rejects an empty gateway port scope', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  istio:
    enabled: true
    host: agents.example.com
    gateway:
      selector:
        istio: ingressgateway
      tls:
        credentialName: agents-example-com-tls
    authorizationPolicy:
      gatewayHttpsPorts: []
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /registry\.istio\.authorizationPolicy\.gatewayHttpsPorts must not be empty/,
  );
});

test('registry Istio AuthorizationPolicy can be disabled', () => {
  const rendered = helmTemplate(`
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  istio:
    enabled: true
    host: agents.example.com
    gateway:
      selector:
        istio: ingressgateway
      tls:
        credentialName: agents-example-com-tls
    authorizationPolicy:
      enabled: false
`);

  assert.ok(
    !rendered.includes('kind: AuthorizationPolicy'),
    'AuthorizationPolicy must not be rendered when disabled',
  );
});

test('registry Istio AuthorizationPolicy rejects an empty path allowlist', () => {
  const valuesPath = writeValues(`${minimalValues}
registry:
  trustedProxyCidrs:
    - 10.42.0.0/16
  istio:
    enabled: true
    host: agents.example.com
    gateway:
      selector:
        istio: ingressgateway
      tls:
        credentialName: agents-example-com-tls
    authorizationPolicy:
      allowedPaths: []
`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /registry\.istio\.authorizationPolicy\.allowedPaths must not be empty/,
  );
});

test('Codex Responses gateway is opt-in with a vault key and scoped route', () => {
  const rendered = helmTemplate(
    yaml.dump({
      aiGateway: { openai: { enabled: true } },
      sessionJwt: { llmRoutes: ['llm-responses'], llmModels: ['gpt-5.4'] },
    }),
  );
  const gatewayMap = yaml.load(
    document(rendered, 'ConfigMap', 'orca-managed-agents-ai-gateway-config'),
  );
  const config = yaml.load(gatewayMap.data['config.yaml']);
  assert.equal(config.destinations['openai-responses'].kind, 'openai');
  assert.equal(config.destinations['openai-responses'].credentials.vault, 'openai_api_key');
  assert.equal(config.vaults.find((v) => v.name === 'openai_api_key').env_var, 'OPENAI_API_KEY');
  assert.equal(config.routes.find((r) => r.name === 'llm-responses').match.path, '/v1/responses');
  const registry = yaml.load(
    document(rendered, 'ConfigMap', 'orca-managed-agents-registry-config'),
  );
  assert.equal(registry.data.AI_GATEWAY_LLM_URL, 'http://orca-managed-agents-ai-gateway:8090/v1');
  const harness = yaml.load(document(rendered, 'ConfigMap', 'orca-managed-agents-harness-config'));
  assert.equal(harness.data.LLM_GATEWAY_URL, registry.data.AI_GATEWAY_LLM_URL);
  assert.equal(registry.data.SESSION_JWT_LLM_MODELS, 'gpt-5.4');
  assert.doesNotMatch(helmTemplate(''), /openai-responses/);
});

test('Registry and separate harness share an explicit LLM gateway endpoint', () => {
  const rendered = helmTemplate(
    yaml.dump({ registry: { aiGatewayLlmUrl: 'https://gateway.example/v1' } }),
  );
  const registry = yaml.load(
    document(rendered, 'ConfigMap', 'orca-managed-agents-registry-config'),
  );
  const harness = yaml.load(document(rendered, 'ConfigMap', 'orca-managed-agents-harness-config'));
  assert.equal(registry.data.AI_GATEWAY_LLM_URL, 'https://gateway.example/v1');
  assert.equal(harness.data.LLM_GATEWAY_URL, registry.data.AI_GATEWAY_LLM_URL);
  const defaults = yaml.load(
    document(helmTemplate(''), 'ConfigMap', 'orca-managed-agents-harness-config'),
  );
  assert.equal(defaults.data.LLM_GATEWAY_URL, undefined);
});

test('Harness renders and validates the separate Session LLM egress default', () => {
  const defaults = yaml.load(
    document(helmTemplate(''), 'ConfigMap', 'orca-managed-agents-harness-config'),
  );
  assert.equal(defaults.data.LLM_EGRESS_DEFAULT, 'direct');

  const gateway = yaml.load(
    document(
      helmTemplate(
        yaml.dump({
          harness: { llmEgressDefault: 'gateway' },
          registry: { aiGatewayLlmUrl: 'https://gateway.example/v1' },
        }),
      ),
      'ConfigMap',
      'orca-managed-agents-harness-config',
    ),
  );
  assert.equal(gateway.data.LLM_EGRESS_DEFAULT, 'gateway');

  for (const aiGateway of [
    { anthropic: { enabled: true } },
    { deepseek: { enabled: true } },
    {
      piProviders: [
        {
          provider: 'anthropic',
          api: 'anthropic-messages',
          baseUrl: 'https://api.anthropic.com',
          apiKeyEnv: 'ANTHROPIC_API_KEY',
          auth: { type: 'header', name: 'x-api-key' },
          allowedPaths: ['/v1/messages'],
        },
      ],
    },
  ]) {
    const rendered = helmTemplate(
      yaml.dump({ harness: { llmEgressDefault: 'gateway' }, aiGateway }),
    );
    const registry = yaml.load(
      document(rendered, 'ConfigMap', 'orca-managed-agents-registry-config'),
    );
    const harness = yaml.load(
      document(rendered, 'ConfigMap', 'orca-managed-agents-harness-config'),
    );
    assert.equal(harness.data.LLM_EGRESS_DEFAULT, 'gateway');
    assert.equal(harness.data.LLM_GATEWAY_URL, registry.data.AI_GATEWAY_LLM_URL);
  }

  const valuesPath = writeValues(`${minimalValues}\nharness:\n  llmEgressDefault: invalid\n`);
  const result = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', valuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /harness\.llmEgressDefault must be one of: direct, gateway/);

  const missingGatewayValuesPath = writeValues(
    `${minimalValues}\nharness:\n  llmEgressDefault: gateway\n`,
  );
  const missingGateway = spawnSync(
    'helm',
    ['template', 'orca-managed-agents', chartDir, '-f', missingGatewayValuesPath],
    { cwd: process.cwd(), encoding: 'utf8' },
  );
  assert.notEqual(missingGateway.status, 0);
  assert.match(
    missingGateway.stderr,
    /harness\.llmEgressDefault=gateway requires registry\.aiGatewayLlmUrl or an enabled aiGateway LLM provider/,
  );
});

test('Pi Messages and Chat gateway routes are opt-in and use provider vaults', () => {
  const rendered = helmTemplate(
    yaml.dump({
      aiGateway: {
        anthropic: { enabled: true },
        deepseek: { enabled: true },
      },
    }),
  );
  const config = yaml.load(
    yaml.load(document(rendered, 'ConfigMap', 'orca-managed-agents-ai-gateway-config')).data[
      'config.yaml'
    ],
  );
  for (const [provider, destination, kind, route, path, baseUrl] of [
    [
      'anthropic',
      'anthropic-proxy',
      'anthropic',
      'llm-messages',
      '/v1/messages',
      'https://api.anthropic.com',
    ],
    [
      'deepseek',
      'deepseek-chat',
      'openai_compatible',
      'llm-chat',
      '/v1/chat/completions',
      'https://api.deepseek.com',
    ],
  ]) {
    assert.deepEqual(config.destinations[destination], {
      kind,
      base_url: baseUrl,
      credentials: { vault: `${provider}_api_key` },
    });
    assert.equal(
      config.vaults.find((v) => v.name === `${provider}_api_key`).env_var,
      `${provider.toUpperCase()}_API_KEY`,
    );
    assert.equal(config.routes.find((r) => r.name === route).match.path, path);
    assert.equal(
      config.routes.find((r) => r.name === route).strategy.targets[0].destination,
      destination,
    );
    assert.doesNotMatch(helmTemplate(''), new RegExp(destination));
  }
});

test('both dev gateway configs route DeepSeek Chat through a credential vault and model ACL', () => {
  for (const file of [
    'services/dev/ai-gateway-config.yaml',
    'services/dev/ai-gateway-llm-config.yaml',
  ]) {
    const config = yaml.load(readFileSync(file, 'utf8'));
    assert.equal(
      config.vaults.find((v) => v.name === 'deepseek_api_key').env_var,
      'DEEPSEEK_API_KEY',
    );
    assert.equal(config.destinations['deepseek-chat'].base_url, 'https://api.deepseek.com');
    assert.equal(
      config.routes.find((r) => r.name === 'llm-chat').strategy.targets[0].destination,
      'deepseek-chat',
    );
    assert(
      config.plugins.authorizers[0].rules.some(
        (r) =>
          r.resource?.provider === 'openai_compatible' &&
          r.conditions[0].resource_name_in_scope_list.scope === 'llm_models',
      ),
    );
  }
});

test('each Pi gateway provider configures Registry and harness LLM endpoints on its own', () => {
  for (const provider of ['anthropic', 'deepseek']) {
    const rendered = helmTemplate(yaml.dump({ aiGateway: { [provider]: { enabled: true } } }));
    const registry = yaml.load(
      document(rendered, 'ConfigMap', 'orca-managed-agents-registry-config'),
    );
    const harness = yaml.load(
      document(rendered, 'ConfigMap', 'orca-managed-agents-harness-config'),
    );
    assert.equal(registry.data.AI_GATEWAY_LLM_URL, 'http://orca-managed-agents-ai-gateway:8090/v1');
    assert.equal(harness.data.LLM_GATEWAY_URL, registry.data.AI_GATEWAY_LLM_URL);
  }
});

test('Pi native bindings leave legacy SDK routes intact and keep SDK usage authoritative', () => {
  const rendered = helmTemplate(
    yaml.dump({
      aiGateway: {
        openai: { enabled: true },
        anthropic: { enabled: true },
        piProviders: [
          {
            provider: 'anthropic',
            api: 'anthropic-messages',
            baseUrl: 'https://api.anthropic.com',
            apiKeyEnv: 'ANTHROPIC_API_KEY',
            auth: { type: 'header', name: 'x-api-key' },
            allowedPaths: ['/v1/messages'],
          },
        ],
      },
    }),
  );
  const config = yaml.load(
    yaml.load(document(rendered, 'ConfigMap', 'orca-managed-agents-ai-gateway-config')).data[
      'config.yaml'
    ],
  );
  const native = config.destinations['pi-anthropic-anthropic-messages'];
  assert.equal(native.kind, 'native_api_key');
  assert.equal(native.emit_usage, false);
  assert.deepEqual(native.allowed_paths, ['/v1/messages']);
  assert.deepEqual(native.allowed_query_params, ['beta']);
  assert.equal(
    config.vaults.find((v) => v.name === 'pi-anthropic-anthropic-messages').scheme,
    'api_key',
  );
  assert.equal(
    config.routes.find((r) => r.name === 'llm-pi-anthropic-anthropic-messages').match.path,
    '/v1/proxy/anthropic/anthropic-messages',
  );
  assert.equal(config.routes.find((r) => r.name === 'llm-messages').match.path, '/v1/messages');
  assert.equal(config.routes.find((r) => r.name === 'llm-responses').match.path, '/v1/responses');
  const onlyPi = helmTemplate(
    yaml.dump({
      aiGateway: {
        piProviders: [
          {
            provider: 'zai',
            api: 'openai-completions',
            baseUrl: 'https://api.z.ai',
            apiKeyEnv: 'ZAI_API_KEY',
            auth: { type: 'bearer' },
            allowedPaths: ['/api/coding/paas/v4/chat/completions'],
          },
        ],
      },
    }),
  );
  assert.equal(
    yaml.load(document(onlyPi, 'ConfigMap', 'orca-managed-agents-registry-config')).data
      .AI_GATEWAY_LLM_URL,
    'http://orca-managed-agents-ai-gateway:8090/v1',
  );
  assert.doesNotMatch(helmTemplate(''), /native_api_key/);
});
