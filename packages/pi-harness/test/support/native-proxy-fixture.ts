// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer } from 'node:http';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createProviderFixture, type TestProvider, MODELS } from './provider-fixture.js';
/** Local fixture of the native Gateway contract, with a distinct upstream key. */
export async function createNativeProxyFixture(
  provider: TestProvider,
  options: Parameters<typeof createProviderFixture>[1] = {},
) {
  const api = await createProviderFixture(provider, { ...options, apiKey: 'upstream-secret' });
  const paths: string[] = [];
  const server = createServer(async (req, res) => {
    try {
      const requestUrl = new URL(req.url ?? '/', 'http://native-gateway');
      if (
        req.headers.authorization !== 'Bearer scoped-pi-token' ||
        req.headers['x-api-key'] ||
        req.headers['x-goog-api-key'] ||
        !requestUrl.pathname.startsWith(`/v1/proxy/${provider}/`) ||
        (provider === 'anthropic' &&
          (requestUrl.pathname !== '/v1/proxy/anthropic/v1/messages' ||
            requestUrl.search !== '?beta=true'))
      ) {
        res.writeHead(403).end();
        return;
      }
      paths.push(req.url);
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = Buffer.concat(chunks);
      const data = JSON.parse(body.toString());
      if (provider !== 'google' && data.model !== MODELS[provider]) {
        res.writeHead(403).end();
        return;
      }
      const upstreamPath =
        provider === 'openai'
          ? '/v1/responses'
          : provider === 'anthropic'
            ? '/v1/messages'
            : provider === 'google'
              ? `/v1beta/models/${MODELS.google}:streamGenerateContent?alt=sse`
              : '/v1/chat/completions';
      const headers = new Headers({
        'content-type': 'application/json',
        'x-orca-session-id': String(req.headers['x-orca-session-id']),
      });
      headers.set(
        provider === 'anthropic'
          ? 'x-api-key'
          : provider === 'google'
            ? 'x-goog-api-key'
            : 'authorization',
        provider === 'anthropic' || provider === 'google' ? api.apiKey : `Bearer ${api.apiKey}`,
      );
      const response = await fetch(new URL(upstreamPath, api.url), {
        method: 'POST',
        headers,
        body,
      });
      res.writeHead(response.status, { 'content-type': response.headers.get('content-type')! });
      if (response.body)
        await pipeline(
          Readable.fromWeb(response.body as import('node:stream/web').ReadableStream),
          res,
        );
      else res.end();
    } catch {
      res.destroy();
    }
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    ...api,
    apiKey: 'scoped-pi-token',
    paths,
    url: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await api.close();
    },
  };
}
