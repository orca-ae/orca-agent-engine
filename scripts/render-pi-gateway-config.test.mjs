// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import yaml from 'js-yaml';
import { withPiGatewayRoutes, piGatewayBindings } from './render-pi-gateway-config.mjs';

test('adds isolated Pi routes without changing existing endpoints or sinks', () => {
  for (const path of [
    'services/dev/ai-gateway-config.yaml',
    'services/dev/ai-gateway-llm-config.yaml',
  ]) {
    const original = yaml.load(readFileSync(path, 'utf8'));
    const config = withPiGatewayRoutes(structuredClone(original));
    for (const [name, destination] of Object.entries(original.destinations))
      assert.deepEqual(config.destinations[name], destination);
    assert.deepEqual(config.routes.slice(0, original.routes.length), original.routes);
    assert.deepEqual(config.plugins.usage_sinks, original.plugins.usage_sinks);
    assert.deepEqual(config.destinations['pi-anthropic-anthropic-messages'].allowed_query_params, [
      'beta',
    ]);
    for (const binding of piGatewayBindings) {
      const destination = config.destinations[`pi-${binding.provider}-${binding.api}`];
      assert.equal(destination.emit_usage, false);
      assert.equal(destination.provider, binding.provider);
      assert.equal(
        config.routes.find((r) => r.name === `llm-pi-${binding.provider}-${binding.api}`).match
          .path,
        `/v1/proxy/${binding.provider}/${binding.api}`,
      );
    }
  }
});

test('Compose passes every generated native vault key to the Gateway container', () => {
  const compose = yaml.load(readFileSync('services/dev/docker-compose.yml', 'utf8'));
  for (const { env } of piGatewayBindings) {
    assert.equal(compose.services['ai-gateway'].environment[env], '${' + env + ':-}');
  }
});
