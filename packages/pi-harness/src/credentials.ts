// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { PI_SDK_MODELS } from '@orca/harness-catalog';
import { readSdkProviderCredentials, type SdkProviderCredentials } from '@orca/sdk-harness';

export type PiProviderCredentials = Record<string, SdkProviderCredentials | undefined>;
/** Operator-owned env only. Values in PI_SDK_PROVIDER_CREDENTIALS are env references,
 * not secret literals or paths to workspace configuration. */
export function readPiProviderCredentials(
  env: Record<string, string | undefined>,
): PiProviderCredentials {
  const result: PiProviderCredentials = { ...readSdkProviderCredentials(env) };
  const aliases: Record<string, string> = {
    google: 'gemini',
    moonshotai: 'moonshot',
    'moonshotai-cn': 'moonshot',
    'minimax-cn': 'minimax',
    'zai-coding-cn': 'zai_coding_cn',
    'kimi-coding': 'kimi',
    together: 'together',
    'opencode-go': 'opencode',
  };
  for (const provider of Object.keys(PI_SDK_MODELS)) {
    const prefix = (aliases[provider] ?? provider.replaceAll('-', '_')).toUpperCase();
    const apiKey = env[`${prefix}_API_KEY`]?.trim() ?? '';
    const baseUrl = env[`${prefix}_BASE_URL`]?.trim();
    if (apiKey || baseUrl) result[provider] = { apiKey, ...(baseUrl ? { baseUrl } : {}) };
  }
  for (const [provider, key, url] of [
    ['google', env.GEMINI_API_KEY, env.GEMINI_BASE_URL],
    ['zai', env.ZAI_API_KEY, env.ZAI_BASE_URL],
  ] as const) {
    const apiKey = key?.trim() ?? '';
    const baseUrl = url?.trim();
    if (apiKey || baseUrl) result[provider] = { apiKey, ...(baseUrl ? { baseUrl } : {}) };
  }
  if (env.PI_SDK_PROVIDER_CREDENTIALS) {
    let config: unknown;
    try {
      config = JSON.parse(env.PI_SDK_PROVIDER_CREDENTIALS);
    } catch {
      throw new Error('PI_SDK_PROVIDER_CREDENTIALS must be a JSON provider-to-env-reference map');
    }
    if (!config || typeof config !== 'object' || Array.isArray(config))
      throw new Error('Invalid Pi credential configuration');
    for (const [provider, value] of Object.entries(config)) {
      if (
        !Object.hasOwn(PI_SDK_MODELS, provider) ||
        !value ||
        typeof value !== 'object' ||
        Array.isArray(value)
      )
        throw new Error(`Unsupported Pi API-key provider: ${provider}`);
      const entry = value as Record<string, unknown>;
      if (
        Object.keys(entry).some((key) => !['apiKeyEnv', 'baseUrlEnv'].includes(key)) ||
        typeof entry.apiKeyEnv !== 'string' ||
        !/^[A-Z_][A-Z0-9_]*$/.test(entry.apiKeyEnv) ||
        (entry.baseUrlEnv !== undefined &&
          (typeof entry.baseUrlEnv !== 'string' || !/^[A-Z_][A-Z0-9_]*$/.test(entry.baseUrlEnv)))
      )
        throw new Error(`Invalid Pi credential env references for ${provider}`);
      const apiKey = env[entry.apiKeyEnv]?.trim() ?? '';
      const baseUrl = entry.baseUrlEnv ? env[entry.baseUrlEnv as string]?.trim() : undefined;
      result[provider] = { apiKey, ...(baseUrl ? { baseUrl } : {}) };
    }
  }
  return result;
}
