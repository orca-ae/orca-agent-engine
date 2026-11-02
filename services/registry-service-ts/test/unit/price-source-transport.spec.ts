// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import type { lookup } from 'node:dns/promises';
import { EventEmitter } from 'node:events';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { createServer, request, type RequestOptions } from 'node:https';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { InMemoryPricingCatalogStore } from '../../src/pricing/catalog-store.js';
import { createConditionalPriceRefresher } from '../../src/pricing/conditional-refresher.js';
import { PriceSourceConfigSchema } from '../../src/pricing/provider-sources.js';
import { createPriceSourceRequest } from '../../src/pricing/source-transport.js';

const execute = promisify(execFile);
const input = () => ({
  headers: { accept: 'application/json' },
  signal: new AbortController().signal,
  maxBytes: 1024,
});

describe('price source socket DNS guard', () => {
  it.each(
    [
      [{ address: '10.0.0.1', family: 4 }],
      [{ address: 'fd00::1', family: 6 }],
      [{ address: '::ffff:127.0.0.1', family: 6 }],
      [{ address: '169.254.169.254', family: 4 }],
      [
        { address: '8.8.8.8', family: 4 },
        { address: 'fd00::1', family: 6 },
      ],
    ].map((addresses) => ({ addresses })),
  )('refuses private DNS results: $addresses', async ({ addresses }) => {
    const dns = vi.fn().mockResolvedValue(addresses);
    // Exercise the lookup passed to the connector without dialing any address.
    const connector = vi.fn((url: URL, options: RequestOptions) => {
      const req = new EventEmitter();
      return Object.assign(req, {
        end() {
          options.lookup!(url.hostname, { all: true }, (error) => {
            req.emit('error', error ?? new Error('unexpected DNS acceptance'));
          });
        },
      });
    });
    const transport = createPriceSourceRequest({
      request: connector as unknown as typeof request,
      resolve: dns as typeof lookup,
    });
    await expect(transport('https://prices.example/catalog', input())).rejects.toThrow(/blocked/);
    expect(dns).toHaveBeenCalledWith('prices.example', { all: true, verbatim: true });
    expect(dns).toHaveBeenCalledTimes(1);
    expect(connector).toHaveBeenCalledTimes(1);
  });
});

describe('price source bounded HTTPS body', () => {
  let directory: string;
  let server: ReturnType<typeof createServer>;
  let origin: string;
  let handler: (req: IncomingMessage, res: ServerResponse) => void;
  const requests: Array<{ url: URL; options: RequestOptions }> = [];
  // Only the fixture dial destination and certificate check differ from production.
  const fixtureRequest = ((
    url: URL,
    options: RequestOptions,
    callback: (response: IncomingMessage) => void,
  ) => {
    requests.push({ url, options });
    return request(
      new URL(url.pathname + url.search, origin),
      {
        ...options,
        rejectUnauthorized: false,
      },
      callback,
    );
  }) as typeof request;
  const transport = createPriceSourceRequest({ request: fixtureRequest });

  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), 'price-source-tls-'));
    const key = join(directory, 'key.pem');
    const cert = join(directory, 'cert.pem');
    await execute('openssl', [
      'req',
      '-x509',
      '-newkey',
      'rsa:2048',
      '-nodes',
      '-keyout',
      key,
      '-out',
      cert,
      '-days',
      '1',
      '-subj',
      '/CN=localhost',
    ]);
    server = createServer({ key: await readFile(key), cert: await readFile(cert) }, (req, res) =>
      handler(req, res),
    );
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('fixture did not listen');
    origin = `https://127.0.0.1:${address.port}`;
  });
  beforeEach(() => {
    requests.length = 0;
    handler = (_req, res) => {
      res.writeHead(200, { etag: 'etag-1', 'last-modified': 'Thu, 29 Oct 2026 00:00:00 GMT' });
      res.end('{"prices":[]}');
    };
  });
  afterAll(async () => {
    server?.closeAllConnections();
    await new Promise<void>((resolve) => server?.close(() => resolve()));
    if (directory) await rm(directory, { recursive: true, force: true });
  });

  it('reads the complete body and validators through the guarded connector', async () => {
    await expect(transport('https://prices.example/catalog', input())).resolves.toEqual({
      status: 200,
      body: '{"prices":[]}',
      etag: 'etag-1',
      last_modified: 'Thu, 29 Oct 2026 00:00:00 GMT',
    });
    expect(requests[0]!.options).toMatchObject({ agent: false, lookup: expect.any(Function) });
    expect(requests[0]!.options.headers).toMatchObject({ 'accept-encoding': 'identity' });
    expect(requests[0]!.options.rejectUnauthorized).toBeUndefined();
  });

  it('uses the configured refresh deadline even when headers arrive and the body keeps trickling', async () => {
    let chunks = 0;
    let closed!: () => void;
    const bodyClosed = new Promise<void>((resolve) => {
      closed = resolve;
    });
    handler = (_req, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.write('{');
      const timer = setInterval(() => {
        chunks++;
        res.write(' ');
      }, 10);
      res.on('close', () => {
        clearInterval(timer);
        closed();
      });
    };
    const store = new InMemoryPricingCatalogStore();
    const refresher = createConditionalPriceRefresher({
      store,
      sources: [
        PriceSourceConfigSchema.parse({
          id: 'slow-body',
          provider: 'anthropic',
          adapter: 'normalized-v1',
          url: 'https://prices.example/catalog',
          timeout_ms: 500,
        }),
      ],
      request: transport,
    });
    try {
      expect(await refresher.refreshNow('slow-body')).toBe('failed');
      await bodyClosed;
      expect(chunks).toBeGreaterThan(1);
      expect((await store.read('')).rules).toEqual([]);
    } finally {
      refresher.stop();
    }
  });

  it('refuses compression and oversized streamed bodies', async () => {
    handler = (_req, res) => {
      res.writeHead(200, { 'content-encoding': 'gzip' });
      res.end('data');
    };
    await expect(transport('https://prices.example/catalog', input())).rejects.toThrow(
      /compressed/,
    );
    handler = (_req, res) => {
      res.write(Buffer.alloc(16));
      res.end(Buffer.alloc(16));
    };
    await expect(
      transport('https://prices.example/catalog', { ...input(), maxBytes: 20 }),
    ).rejects.toThrow(/byte limit/);
  });

  it('returns redirect status without following the target', async () => {
    handler = (_req, res) => {
      res.writeHead(302, { location: 'https://127.0.0.1/private' });
      res.end();
    };
    expect((await transport('https://prices.example/catalog', input())).status).toBe(302);
    expect(requests).toHaveLength(1);
  });

  it.each([
    'http://prices.example/catalog',
    'https://user:password@prices.example/catalog',
    'https://prices.example/catalog#fragment',
    'https://127.0.0.1/catalog',
    'https://[::1]/catalog',
  ])('refuses unsafe origin %s before connecting', async (url) => {
    await expect(transport(url, input())).rejects.toThrow(/origin refused/);
    expect(requests).toHaveLength(0);
  });
});
