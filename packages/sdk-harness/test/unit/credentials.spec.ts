// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { expect, it } from 'vitest';
import { readSdkProviderCredentials } from '../../src/credentials.js';

it('keeps explicit provider keys and endpoints separate and excludes ambient providers', () => {
  expect(
    readSdkProviderCredentials({
      OPENAI_API_KEY: ' openai-test ',
      OPENAI_BASE_URL: ' https://openai.invalid/v1 ',
      ANTHROPIC_API_KEY: 'anthropic-test',
      DEEPSEEK_API_KEY: 'deepseek-test',
      DEEPSEEK_BASE_URL: 'https://deepseek.invalid/v1',
      GEMINI_API_KEY: 'excluded',
    }),
  ).toEqual({
    openai: { apiKey: 'openai-test', baseUrl: 'https://openai.invalid/v1' },
    anthropic: { apiKey: 'anthropic-test' },
    deepseek: { apiKey: 'deepseek-test', baseUrl: 'https://deepseek.invalid/v1' },
  });
  expect(readSdkProviderCredentials({ OPENAI_API_KEY: 'only-openai' })).toEqual({
    openai: { apiKey: 'only-openai' },
  });
});
