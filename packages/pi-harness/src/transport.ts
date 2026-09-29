// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer, type Server } from 'node:http';
import { randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { validatedProviderFetch } from './responses-fetch.js';

const AUTH_HEADERS = [
  'authorization',
  'proxy-authorization',
  'x-api-key',
  'api-key',
  'x-goog-api-key',
  'cookie',
];
const SECRET_QUERY = new Set(['key', 'api_key', 'api-key', 'access_token', 'token']);
export interface PiTransportOptions {
  provider: string;
  api: string;
  apiKey: string;
  sessionId: string;
  gatewayUrl?: string | undefined;
}

/** The SDK sees its original URL until it has finished provider-specific encoding. */
export function piProviderFetch(options: PiTransportOptions): typeof fetch {
  const send: typeof fetch = async (input, init) => {
    const request = new Request(input, init);
    const headers = new Headers(request.headers);
    headers.set('X-Orca-Session-Id', options.sessionId);
    let url = new URL(request.url);
    if (options.gatewayUrl) {
      const gateway = new URL(options.gatewayUrl);
      if (!['http:', 'https:'].includes(gateway.protocol) || gateway.username || gateway.password)
        throw new Error('Invalid Pi gateway URL');
      if (!/^[a-z0-9][a-z0-9-]*$/.test(options.provider)) throw new Error('Invalid Pi provider');
      for (const key of [...url.searchParams.keys()]) {
        if (SECRET_QUERY.has(key.toLowerCase())) url.searchParams.delete(key);
      }
      for (const name of AUTH_HEADERS) headers.delete(name);
      headers.delete('host');
      headers.set('authorization', `Bearer ${options.apiKey}`);
      url = new URL(`/v1/proxy/${options.provider}${url.pathname}${url.search}`, gateway.origin);
    }
    return fetch(new Request(url, request), { headers, redirect: 'error' });
  };
  return validatedProviderFetch(options.api, send);
}

/** Pi 0.87's Google SDK rejects custom fetch. This private loopback bridge gives
 * it the same transport and wire-usage validation without global fetch mutation. */
export class PiGoogleTransport {
  private readonly token = randomUUID();
  private readonly active = new Set<AbortController>();
  private server: Server | undefined;
  private endpoint?: string;
  constructor(private readonly options: () => PiTransportOptions & { baseUrl: string }) {}
  async start(): Promise<void> {
    const server = createServer(async (req, res) => {
      const abort = new AbortController();
      this.active.add(abort);
      res.on('close', () => abort.abort());
      try {
        if (req.method !== 'POST' || req.headers['x-goog-api-key'] !== this.token) {
          res.writeHead(403).end();
          return;
        }
        const opts = this.options();
        const url = new URL(req.url!, 'http://loopback');
        const upstream = new URL(opts.baseUrl);
        if (
          !url.pathname.startsWith(`${upstream.pathname.replace(/\/$/, '')}/models/`) ||
          !url.pathname.endsWith(':streamGenerateContent')
        ) {
          res.writeHead(404).end();
          return;
        }
        const chunks: Buffer[] = [];
        let size = 0;
        for await (const chunk of req) {
          size += chunk.length;
          if (size > 32 * 1024 * 1024) {
            res.writeHead(413).end();
            return;
          }
          chunks.push(chunk);
        }
        const headers = new Headers();
        for (const [name, value] of Object.entries(req.headers)) {
          if (typeof value === 'string' && !['host', 'connection', 'content-length'].includes(name))
            headers.set(name, value);
        }
        headers.set('x-goog-api-key', opts.gatewayUrl ? 'orca-managed' : opts.apiKey);
        const response = await piProviderFetch(opts)(
          new URL(`${url.pathname}${url.search}`, upstream.origin),
          {
            method: 'POST',
            headers,
            body: Buffer.concat(chunks),
            signal: abort.signal,
          },
        );
        res.writeHead(response.status, {
          'content-type': response.headers.get('content-type') ?? 'application/json',
        });
        if (response.body)
          await pipeline(
            Readable.fromWeb(response.body as import('node:stream/web').ReadableStream),
            res,
          );
        else res.end();
      } catch {
        // Do not expose upstream credentials/URLs or accept a truncated successful stream.
        if (!res.headersSent) res.writeHead(502).end('Pi model transport failed');
        else res.destroy();
      } finally {
        this.active.delete(abort);
      }
    });
    this.server = server;
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', () => resolve());
    });
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Pi transport unavailable');
    this.endpoint = `http://127.0.0.1:${address.port}`;
  }
  modelBaseUrl(): string {
    return this.endpoint! + new URL(this.options().baseUrl).pathname.replace(/\/$/, '');
  }
  get apiKey(): string {
    return this.token;
  }
  abort(): void {
    for (const controller of this.active) controller.abort();
  }
  async close(): Promise<void> {
    this.abort();
    const server = this.server;
    if (!server) return;
    this.server = undefined;
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}
