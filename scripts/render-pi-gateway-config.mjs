// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync, writeFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import yaml from 'js-yaml';

// Static API-key examples; existing Messages/Responses routes remain intact.
export const piGatewayBindings = [
  {
    provider: 'openai',
    api: 'openai-responses',
    origin: 'https://api.openai.com',
    path: '/v1/responses',
    env: 'OPENAI_API_KEY',
    auth: { type: 'bearer' },
  },
  {
    provider: 'anthropic',
    api: 'anthropic-messages',
    origin: 'https://api.anthropic.com',
    path: '/v1/messages',
    env: 'ANTHROPIC_API_KEY',
    auth: { type: 'header', name: 'x-api-key' },
    headers: { 'anthropic-version': '2023-06-01' },
    forward: ['anthropic-beta'],
    // Pi calls Anthropic's beta.messages API, which adds ?beta=true.
    query: ['beta'],
  },
  {
    provider: 'deepseek',
    api: 'openai-completions',
    origin: 'https://api.deepseek.com',
    path: '/chat/completions',
    env: 'DEEPSEEK_API_KEY',
    auth: { type: 'bearer' },
  },
  {
    provider: 'zai',
    api: 'openai-completions',
    origin: 'https://api.z.ai',
    path: '/api/coding/paas/v4/chat/completions',
    env: 'ZAI_API_KEY',
    auth: { type: 'bearer' },
  },
  {
    provider: 'google',
    api: 'google-generative-ai',
    origin: 'https://generativelanguage.googleapis.com',
    path: '/v1beta/models/{model}:streamGenerateContent',
    env: 'GEMINI_API_KEY',
    auth: { type: 'header', name: 'x-goog-api-key' },
    query: ['alt'],
  },
];
export function withPiGatewayRoutes(config) {
  for (const binding of piGatewayBindings) {
    const name = `pi-${binding.provider}-${binding.api}`;
    config.vaults.push({
      name,
      resolver: 'env',
      env_var: binding.env,
      scheme: binding.auth.type === 'bearer' ? 'bearer' : 'api_key',
    });
    config.destinations[name] = {
      kind: 'native_api_key',
      emit_usage: false,
      provider: binding.provider,
      api: binding.api,
      base_url: binding.origin,
      allowed_paths: [binding.path],
      allowed_query_params: binding.query ?? [],
      forward_headers: binding.forward ?? [],
      headers: binding.headers ?? {},
      auth: binding.auth,
      credentials: { vault: name },
    };
    config.routes.push({
      name: `llm-${name}`,
      match: { path: `/v1/proxy/${binding.provider}/${binding.api}` },
      strategy: { mode: 'fallback', targets: [{ destination: name }] },
    });
    const acl = config.plugins.authorizers.find((a) => a.kind === 'yaml_acl');
    acl.rules.push({
      effect: 'allow',
      action: 'invoke',
      resource: { kind: 'model', provider: binding.provider },
      conditions: [{ resource_name_in_scope_list: { scope: 'llm_models' } }],
    });
  }
  return config;
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [, , source, output] = process.argv;
  if (!source || !output)
    throw new Error('Usage: node scripts/render-pi-gateway-config.mjs SOURCE OUTPUT');
  writeFileSync(
    output,
    yaml.dump(withPiGatewayRoutes(yaml.load(readFileSync(source, 'utf8'))), {
      lineWidth: 120,
      noRefs: true,
    }),
  );
}
