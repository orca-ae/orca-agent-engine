// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Pinned Pi static API-key providers whose wire protocols Orca validates.
// Run from this package: node scripts/generate-catalog.mjs [--check]
import { builtinModels } from '@earendil-works/pi-ai/providers/all';
import { getSupportedThinkingLevels } from '@earendil-works/pi-ai';
import { readFileSync, writeFileSync } from 'node:fs';
const apis = new Set([
  'openai-responses',
  'openai-completions',
  'anthropic-messages',
  'google-generative-ai',
]);
// These require account/deployment discovery, token exchange, or another protocol.
const excluded = new Set([
  'amazon-bedrock',
  'azure-openai-responses',
  'cloudflare-ai-gateway',
  'cloudflare-workers-ai',
  'github-copilot',
  'google-vertex',
  'openai-codex',
  'radius',
  'mistral',
]);
const models = {},
  protocols = {};
for (const provider of builtinModels().getProviders()) {
  if (excluded.has(provider.id) || !provider.auth.apiKey) continue;
  models[provider.id] = {};
  protocols[provider.id] = {};
  for (const model of provider.getModels()) {
    if (!apis.has(model.api)) continue;
    models[provider.id][model.id] = getSupportedThinkingLevels(model).filter(
      (l) => !['off', 'minimal'].includes(l),
    );
    protocols[provider.id][model.id] = model.api;
  }
  if (!Object.keys(models[provider.id]).length) {
    delete models[provider.id];
    delete protocols[provider.id];
  }
}
for (const [name, data] of Object.entries({
  'pi-models.json': models,
  'pi-model-apis.json': protocols,
})) {
  const url = new URL(`../../harness-catalog/src/${name}`, import.meta.url);
  // Compact each model row, matching the catalog's checked-in formatting.
  const value =
    JSON.stringify(data, null, 2).replace(
      /\[\n\s+([^\]]*?)\n\s*\]/g,
      (_, s) => `[${s.replace(/\n\s*/g, ' ')}]`,
    ) + '\n';
  if (process.argv.includes('--check')) {
    if (JSON.stringify(JSON.parse(readFileSync(url, 'utf8'))) !== JSON.stringify(data))
      throw new Error(`${name} differs from installed Pi`);
  } else writeFileSync(url, value);
}
