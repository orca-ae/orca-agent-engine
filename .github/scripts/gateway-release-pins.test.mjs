// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import yaml from 'js-yaml';

// This release publishes both the container image and the matching OCI chart.
const version = '0.4.3-rc.3';
const repository = 'ghcr.io/orca-ae/orca-ai-gateway';
const image = `${repository}:v${version}`;
const read = (path) => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');

test('managed chart and E2E workflows pin the published Gateway image/chart pair', () => {
  const values = yaml.load(read('charts/orca-managed-agents/values.yaml'));
  assert.equal(values.images.aiGateway.repository, repository);
  assert.equal(values.images.aiGateway.tag, `v${version}`);
  for (const name of ['e2e-stack', 'e2e-kind-helm']) {
    const workflow = yaml.load(read(`.github/workflows/${name}.yml`));
    assert.equal(workflow.env.AI_GATEWAY_IMAGE, image, name);
    if (name === 'e2e-kind-helm') {
      assert.equal(workflow.env.AI_GATEWAY_CHART, 'oci://ghcr.io/orca-ae/charts/orca-ai-gateway');
      assert.equal(workflow.env.AI_GATEWAY_CHART_VERSION, version);
    }
  }
});

test('local stack and compatibility wrapper use the same Gateway release', () => {
  const compose = yaml.load(read('services/dev/docker-compose.yml'));
  assert.equal(compose.services['ai-gateway'].image, '${AI_GATEWAY_IMAGE:-' + image + '}');
  for (const [path, declaration] of [
    [
      'services/dev/scripts/prepare-ai-gateway-image.sh',
      'SOURCE_IMAGE="${AI_GATEWAY_IMAGE:-' + image + '}"',
    ],
    ['services/dev/scripts/start-services.sh', ': "${AI_GATEWAY_IMAGE:=' + image + '}"'],
    ['services/dev/ai-gateway-compat.Dockerfile', 'ARG AI_GATEWAY_SOURCE_IMAGE=' + image],
  ]) {
    assert.ok(read(path).split('\n').includes(declaration), path);
  }
});
