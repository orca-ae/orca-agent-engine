// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Explicit operator credentials; never resolve keys inside the model's workspace. */
export interface SdkProviderCredentials {
  apiKey: string;
  baseUrl?: string;
}
export type SdkProviderCredentialsMap = Partial<
  Record<'openai' | 'anthropic' | 'deepseek', SdkProviderCredentials>
>;
export function readSdkProviderCredentials(
  env: Record<string, string | undefined>,
): SdkProviderCredentialsMap {
  const result: SdkProviderCredentialsMap = {};
  for (const [provider, key, url] of [
    ['openai', env.OPENAI_API_KEY, env.OPENAI_BASE_URL],
    ['anthropic', env.ANTHROPIC_API_KEY, env.ANTHROPIC_BASE_URL],
    ['deepseek', env.DEEPSEEK_API_KEY, env.DEEPSEEK_BASE_URL],
  ] as const) {
    const apiKey = key?.trim() ?? '';
    const baseUrl = url?.trim();
    if (apiKey || baseUrl) result[provider] = { apiKey, ...(baseUrl ? { baseUrl } : {}) };
  }
  return result;
}
