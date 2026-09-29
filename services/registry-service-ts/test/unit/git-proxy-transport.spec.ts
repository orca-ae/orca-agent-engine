// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from 'node:child_process';
import { lookup } from 'node:dns/promises';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { createServer, request, type RequestOptions } from 'node:https';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { LookupFunction } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  createGitProxyLookup,
  createGitProxyRequest,
  type GitProxyRequestInput,
} from '../../src/domain/git-proxy-transport.js';

const execute = promisify(execFile);
const PAT = 'secret-fixture-PAT';
const input = (): GitProxyRequestInput => ({
  repoUrl: 'https://github.com/owner/private.git',
  pat: PAT,
  method: 'GET',
  gitProtocol: 'version=2',
  signal: new AbortController().signal,
});

function resolveAddress(fn: LookupFunction, all = false) {
  return new Promise((resolve, reject) =>
    fn('example.com', { all }, (error, ...value) => (error ? reject(error) : resolve(value))),
  );
}

describe('Git proxy connector DNS guard', () => {
  it('rejects any blocked address among all DNS results at connection lookup', async () => {
    const dns = vi.fn().mockResolvedValue([
      { address: '8.8.8.8', family: 4 },
      { address: '127.0.0.1', family: 4 },
    ]);
    await expect(resolveAddress(createGitProxyLookup(dns as typeof lookup))).rejects.toThrow(
      'blocked',
    );
    expect(dns).toHaveBeenCalledTimes(1);
    expect(dns).toHaveBeenCalledWith('example.com', { all: true, verbatim: true });
  });

  it.each(['10.0.0.1', '169.254.169.254', '::1', '::ffff:127.0.0.1', '64:ff9b::7f00:1'])(
    'rejects %s',
    async (address) => {
      const dns = vi.fn().mockResolvedValue([{ address, family: address.includes(':') ? 6 : 4 }]);
      await expect(resolveAddress(createGitProxyLookup(dns as typeof lookup))).rejects.toThrow(
        'blocked',
      );
    },
  );

  it('supplies only the inspected results without doing another DNS lookup', async () => {
    const addresses = [
      { address: '8.8.8.8', family: 4 },
      { address: '2001:4860:4860::8888', family: 6 },
    ];
    const dns = vi
      .fn()
      .mockResolvedValueOnce(addresses)
      .mockResolvedValue([{ address: '127.0.0.1', family: 4 }]);
    await expect(resolveAddress(createGitProxyLookup(dns as typeof lookup), true)).resolves.toEqual(
      [addresses],
    );
    expect(dns).toHaveBeenCalledTimes(1);
  });

  it('rejects empty and failed resolution', async () => {
    await expect(
      resolveAddress(createGitProxyLookup(vi.fn().mockResolvedValue([]) as typeof lookup)),
    ).rejects.toThrow('blocked');
    await expect(
      resolveAddress(
        createGitProxyLookup(vi.fn().mockRejectedValue(new Error(PAT)) as typeof lookup),
      ),
    ).rejects.toThrow('resolution failed');
  });
});

describe('Git proxy bounded HTTPS transport', () => {
  let directory: string;
  let server: ReturnType<typeof createServer>;
  let origin: string;
  let handler: (req: IncomingMessage, res: ServerResponse) => void;
  const requests: Array<{ url: URL; options: RequestOptions }> = [];
  // Rewrite only the test dial destination to a local TLS fixture. Production
  // still supplies its connector lookup; its DNS behavior is exercised above.
  const fixtureRequest = ((
    url: URL,
    options: RequestOptions,
    callback: (response: IncomingMessage) => void,
  ) => {
    requests.push({ url, options });
    return request(
      new URL(url.pathname + url.search, origin),
      { ...options, rejectUnauthorized: false },
      callback,
    );
  }) as typeof request;

  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), 'git-proxy-tls-'));
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
      res.setHeader('content-type', 'application/x-git-upload-pack-advertisement');
      res.end(Buffer.from([0, 255, 128, 1]));
    };
  });

  afterAll(async () => {
    server?.closeAllConnections();
    await new Promise<void>((resolve) => server?.close(() => resolve()));
    if (directory) await rm(directory, { recursive: true, force: true });
  });

  it('uses HTTPS, guarded connector lookup, minimal headers and exact read paths', async () => {
    const transport = createGitProxyRequest({ request: fixtureRequest });
    expect(await transport(input())).toEqual(Buffer.from([0, 255, 128, 1]));
    expect(requests[0]!.url.href).toBe(
      'https://github.com/owner/private.git/info/refs?service=git-upload-pack',
    );
    expect(requests[0]!.options).toMatchObject({
      agent: false,
      lookup: expect.any(Function),
      maxHeaderSize: 16384,
    });
    expect(requests[0]!.options.headers).toEqual({
      authorization: `Basic ${Buffer.from(`x-access-token:${PAT}`).toString('base64')}`,
      accept: 'application/x-git-upload-pack-advertisement',
      'git-protocol': 'version=2',
    });
  });

  it('round-trips binary upload-pack requests and results', async () => {
    const bytes = Buffer.from('0014command=ls-refs\n00010009peel\n0000');
    handler = (req, res) => {
      expect(req.method).toBe('POST');
      expect(req.url).toBe('/owner/private.git/git-upload-pack');
      expect(req.headers['content-type']).toBe('application/x-git-upload-pack-request');
      const chunks: Buffer[] = [];
      req.on('data', (chunk: Buffer) => chunks.push(chunk));
      req.on('end', () => {
        res.setHeader('content-type', 'application/x-git-upload-pack-result');
        res.end(Buffer.concat(chunks));
      });
    };
    expect(
      await createGitProxyRequest({ request: fixtureRequest })({
        ...input(),
        method: 'POST',
        body: bytes,
      }),
    ).toEqual(bytes);
  });

  it.each([302, 401, 500])(
    'rejects %i without following redirects or exposing response bytes/headers',
    async (status) => {
      handler = (_req, res) => {
        res.writeHead(status, {
          'content-type': 'application/x-git-upload-pack-advertisement',
          location: 'http://169.254.169.254/secret',
          'set-cookie': PAT,
        });
        res.end(PAT);
      };
      await expect(createGitProxyRequest({ request: fixtureRequest })(input())).rejects.toThrow(
        'Git upstream request failed',
      );
      expect(requests).toHaveLength(1);
    },
  );

  it('rejects unexpected content type and compressed payload', async () => {
    handler = (_req, res) => {
      res.end(PAT);
    };
    await expect(createGitProxyRequest({ request: fixtureRequest })(input())).rejects.toThrow(
      'Git upstream request failed',
    );
    handler = (_req, res) => {
      res.writeHead(200, {
        'content-type': 'application/x-git-upload-pack-advertisement',
        'content-encoding': 'gzip',
      });
      res.end(PAT);
    };
    await expect(createGitProxyRequest({ request: fixtureRequest })(input())).rejects.toThrow(
      'Git upstream request failed',
    );
  });

  it.each([true, false])('bounds responses with content length declared=%s', async (declared) => {
    handler = (_req, res) => {
      res.setHeader('content-type', 'application/x-git-upload-pack-advertisement');
      if (declared) res.setHeader('content-length', '50');
      res.write(Buffer.alloc(25));
      res.end(Buffer.alloc(25));
    };
    await expect(
      createGitProxyRequest({ request: fixtureRequest, maxResponseBytes: 32 })(input()),
    ).rejects.toThrow('Git upstream request failed');
  });

  it('aborts stalled upstream requests on timeout and caller disconnect', async () => {
    handler = () => {};
    await expect(
      createGitProxyRequest({ request: fixtureRequest, timeoutMs: 50 })(input()),
    ).rejects.toThrow('Git upstream request failed');
    const abort = new AbortController();
    const pending = createGitProxyRequest({ request: fixtureRequest })({
      ...input(),
      signal: abort.signal,
    });
    abort.abort();
    await expect(pending).rejects.toThrow('Git upstream request failed');
  });

  it.each([
    'http://github.com/owner/repo',
    'https://127.0.0.1/owner/repo',
    'https://[::1]/owner/repo',
    'https://user:password@github.com/owner/repo',
  ])('refuses unsafe target %s before connecting', async (repoUrl) => {
    await expect(
      createGitProxyRequest({ request: fixtureRequest })({ ...input(), repoUrl }),
    ).rejects.toThrow();
    expect(requests).toHaveLength(0);
  });
});
