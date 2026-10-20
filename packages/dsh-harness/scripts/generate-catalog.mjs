// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Read the official plugin's dependency instance, never the Orca Pi harness catalog.
import { createRequire } from 'node:module';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const require = createRequire(import.meta.url);
const plugin = createRequire(require.resolve('@deepseek-ai/dsh-llm-pi-ai'));
const piRoot = plugin.resolve
  .paths('@earendil-works/pi-ai')
  .map((path) => join(path, '@earendil-works/pi-ai'))
  .find((path) => existsSync(join(path, 'package.json')));
if (!piRoot) throw new Error('Official plugin pi-ai dependency is missing');
const { builtinProviders } = await import(pathToFileURL(join(piRoot, 'dist/providers/all.js')));
const { getSupportedThinkingLevels } = await import(pathToFileURL(join(piRoot, 'dist/index.js')));
const piVersion = JSON.parse(readFileSync(join(piRoot, 'package.json'), 'utf8')).version;
const runtimeVersion = JSON.parse(
  readFileSync(
    new URL('../node_modules/@earendil-works/pi-ai/package.json', import.meta.url),
    'utf8',
  ),
).version;
if (piVersion !== runtimeVersion)
  throw new Error('DSH runtime and official plugin pi-ai versions differ');
const providers = {};
for (const provider of builtinProviders()) {
  const models = {};
  for (const model of provider.getModels()) {
    models[model.id] = {
      api: model.api,
      efforts: model.reasoning ? getSupportedThinkingLevels(model) : [],
    };
  }
  if (Object.keys(models).length) providers[provider.id] = models;
}
const pluginVersion = JSON.parse(
  readFileSync(require.resolve('@deepseek-ai/dsh-llm-pi-ai/package.json'), 'utf8'),
).version;
const catalog = { pluginVersion, piVersion, providers };
const url = new URL('../../harness-catalog/src/dsh-models.json', import.meta.url);
if (process.argv.includes('--check')) {
  if (JSON.stringify(JSON.parse(readFileSync(url, 'utf8'))) !== JSON.stringify(catalog))
    throw new Error('DSH catalog differs from the pinned official plugin');
} else writeFileSync(url, JSON.stringify(catalog, null, 2) + String.fromCharCode(10));
