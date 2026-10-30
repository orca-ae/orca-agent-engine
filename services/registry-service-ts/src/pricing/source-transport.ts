// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { request as httpsRequest } from 'node:https';
import { isIP } from 'node:net';
import { isBlockedAddress } from '../api/egress-guard.js';
import { createGitProxyLookup } from '../domain/git-proxy-transport.js';

export interface PriceSourceResponse {
  status: number;
  body: string;
  etag: string | null;
  last_modified: string | null;
}
export type PriceSourceRequest = (
  url: string,
  options: {
    headers: Record<string, string>;
    signal: AbortSignal;
    maxBytes: number;
  },
) => Promise<PriceSourceResponse>;

/** The caller's signal bounds DNS, TLS and the entire body, including slow trickles. */
export function createPriceSourceRequest(
  transport: {
    request?: typeof httpsRequest;
    resolve?: Parameters<typeof createGitProxyLookup>[0];
  } = {},
): PriceSourceRequest {
  const request = transport.request ?? httpsRequest;
  // The resolver is always wrapped by the same guard used at socket connection.
  const lookup = createGitProxyLookup(transport.resolve);
  return async (rawUrl, options) => {
    const url = new URL(rawUrl);
    const host = url.hostname.replace(/^\[|\]$/g, '');
    if (
      url.protocol !== 'https:' ||
      url.username ||
      url.password ||
      url.hash ||
      (isIP(host) && isBlockedAddress(host))
    )
      throw new Error('price source origin refused');
    return new Promise((resolve, reject) => {
      const req = request(
        url,
        {
          method: 'GET',
          agent: false,
          lookup,
          signal: options.signal,
          headers: { ...options.headers, 'accept-encoding': 'identity' },
        },
        (res) => {
          res.on('error', reject);
          const chunks: Buffer[] = [];
          let bytes = 0;
          const encoding = res.headers['content-encoding'];
          if (encoding && encoding !== 'identity') {
            res.destroy(new Error('compressed price source refused'));
            return;
          }
          res.on('data', (chunk: Buffer) => {
            bytes += chunk.length;
            if (bytes > options.maxBytes) {
              res.destroy(new Error('price source byte limit exceeded'));
              return;
            }
            chunks.push(chunk);
          });
          res.on('end', () =>
            resolve({
              status: res.statusCode ?? 0,
              body: Buffer.concat(chunks).toString('utf8'),
              etag: typeof res.headers.etag === 'string' ? res.headers.etag : null,
              last_modified:
                typeof res.headers['last-modified'] === 'string'
                  ? res.headers['last-modified']
                  : null,
            }),
          );
        },
      );
      req.on('error', reject);
      req.end();
    });
  };
}

/** DNS is checked by the socket's lookup. No redirects, credentials or compressed bombs. */
export const requestPriceSource = createPriceSourceRequest();
