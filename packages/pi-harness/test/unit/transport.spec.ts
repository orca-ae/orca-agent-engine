// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterEach, expect, it, vi } from 'vitest';
import { piProviderFetch } from '../../src/transport.js';
import { readPiProviderCredentials } from '../../src/credentials.js';

afterEach(() => vi.unstubAllGlobals());
it('rewrites only transport, removes provider auth, and preserves opaque bytes', async () => {
  const requests: Request[] = [];
  vi.stubGlobal('fetch', async (request: Request, init: RequestInit) => {
    requests.push(new Request(request, init));
    return new Response('denied', { status: 403 });
  });
  const body = '{ "model":"glm-4.7", "reasoning_content":"opaque", "extra":{"unknown":true} }';
  await piProviderFetch({
    provider: 'zai',
    api: 'openai-completions',
    apiKey: 'scoped-jwt',
    sessionId: 'ses',
    gatewayUrl: 'http://gateway/v1',
  })('https://api.z.ai/api/coding/paas/v4/chat/completions?key=secret&alt=sse', {
    method: 'POST',
    body,
    headers: {
      authorization: 'Bearer upstream',
      'x-api-key': 'key',
      'x-goog-api-key': 'google',
      cookie: 'session=secret',
      'anthropic-beta': 'feature',
    },
  });
  expect(requests[0]!.url).toBe(
    'http://gateway/v1/proxy/zai/api/coding/paas/v4/chat/completions?alt=sse',
  );
  expect(await requests[0]!.text()).toBe(body);
  expect(requests[0]!.headers.get('authorization')).toBe('Bearer scoped-jwt');
  for (const header of ['x-api-key', 'x-goog-api-key', 'cookie'])
    expect(requests[0]!.headers.has(header)).toBe(false);
  expect(requests[0]!.headers.get('anthropic-beta')).toBe('feature');
  expect(requests[0]!.redirect).toBe('error');
});
it('rejects unsupported protocols instead of treating their usage as Responses', () => {
  expect(() =>
    piProviderFetch({
      provider: 'mistral',
      api: 'mistral-conversations',
      apiKey: 'key',
      sessionId: 'ses',
    }),
  ).toThrow('Unsupported Pi usage protocol');
});
it('reads only explicit API-key environment references and refuses OAuth configuration', () => {
  expect(
    readPiProviderCredentials({ ZAI_API_KEY: 'zai-key', GEMINI_API_KEY: 'google-key' }),
  ).toMatchObject({ zai: { apiKey: 'zai-key' }, google: { apiKey: 'google-key' } });
  expect(
    readPiProviderCredentials({
      PI_SDK_PROVIDER_CREDENTIALS: '{"zai":{"apiKeyEnv":"MY_KEY","baseUrlEnv":"MY_URL"}}',
      MY_KEY: 'custom',
      MY_URL: 'https://proxy/v4',
    }),
  ).toMatchObject({ zai: { apiKey: 'custom', baseUrl: 'https://proxy/v4' } });
  expect(() =>
    readPiProviderCredentials({
      PI_SDK_PROVIDER_CREDENTIALS: '{"openai-codex":{"apiKeyEnv":"MY_KEY"}}',
    }),
  ).toThrow('Unsupported');
  expect(() =>
    readPiProviderCredentials({ PI_SDK_PROVIDER_CREDENTIALS: '{"openai":{"oauth":"token"}}' }),
  ).toThrow('Invalid');
});
