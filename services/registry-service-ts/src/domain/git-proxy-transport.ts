// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { validateGitUploadPack } from './git-upload-pack.js';
import { lookup } from 'node:dns/promises';
import { request as httpsRequest } from 'node:https';
import type { LookupFunction } from 'node:net';
import { isIP } from 'node:net';
import { isBlockedAddress } from '../api/egress-guard.js';
import {
  GIT_ADVERTISEMENT_TYPE,
  GIT_UPLOAD_REQUEST_TYPE,
  GIT_UPLOAD_RESULT_TYPE,
} from '../contracts/git-proxy.contract.js';
import { validateGitRepositoryUrl } from './git-credentials.js';

export const GIT_PROXY_REQUEST_MAX_BYTES = 1024 * 1024;
export const GIT_PROXY_RESPONSE_MAX_BYTES = 64 * 1024 * 1024;
export const GIT_PROXY_TIMEOUT_MS = 60_000;

export interface GitProxyRequestInput {
  repoUrl: string;
  pat: string;
  method: 'GET' | 'POST';
  body?: Buffer;
  gitProtocol?: string;
  signal: AbortSignal;
}
export type GitProxyRequest = (input: GitProxyRequestInput) => Promise<Buffer>;

/** This lookup IS the connector's lookup: there is no second resolution after validation. */
export function createGitProxyLookup(resolve = lookup): LookupFunction {
  return (hostname, options, callback) => {
    void resolve(hostname, { all: true, verbatim: true }).then(
      (addresses) => {
        if (addresses.length === 0 || addresses.some(({ address }) => isBlockedAddress(address))) {
          callback(new Error('Git upstream address blocked'), '', 4);
          return;
        }
        const family = typeof options === 'number' ? options : options.family;
        const selected =
          family === 4 || family === 6
            ? addresses.filter((item) => item.family === family)
            : addresses;
        if (!selected.length) {
          callback(new Error('Git upstream address unavailable'), '', 4);
          return;
        }
        if (typeof options === 'object' && options.all) {
          // Node's callback overload depends on lookup options.all.
          (callback as unknown as (error: null, addresses: typeof selected) => void)(
            null,
            selected,
          );
        } else callback(null, selected[0]!.address, selected[0]!.family);
      },
      () => callback(new Error('Git upstream resolution failed'), '', 4),
    );
  };
}

/** Only HTTPS upload-pack traffic leaves Registry; neither redirects nor upstream errors escape. */
export function createGitProxyRequest(
  options: {
    request?: typeof httpsRequest;
    lookup?: LookupFunction;
    maxResponseBytes?: number;
    timeoutMs?: number;
  } = {},
): GitProxyRequest {
  const request = options.request ?? httpsRequest;
  return async (input) => {
    if (validateGitRepositoryUrl(input.repoUrl) || !input.pat || /[\r\n\0]/.test(input.pat))
      throw new Error('Git upstream unavailable');
    if ((input.body?.length ?? 0) > GIT_PROXY_REQUEST_MAX_BYTES)
      throw new Error('Git request too large');
    if (input.method === 'POST')
      validateGitUploadPack(input.body ?? Buffer.alloc(0), input.gitProtocol);
    const url = new URL(input.repoUrl);
    const host = url.hostname.replace(/^\[|\]$/g, '');
    if (isIP(host) && isBlockedAddress(host)) throw new Error('Git upstream address blocked');
    url.pathname = `${url.pathname.replace(/\/+$/, '')}/${input.method === 'GET' ? 'info/refs' : 'git-upload-pack'}`;
    if (input.method === 'GET') url.search = '?service=git-upload-pack';
    const expectedType = input.method === 'GET' ? GIT_ADVERTISEMENT_TYPE : GIT_UPLOAD_RESULT_TYPE;
    const headers: Record<string, string> = {
      authorization: `Basic ${Buffer.from(`x-access-token:${input.pat}`).toString('base64')}`,
      accept: expectedType,
    };
    if (input.gitProtocol) {
      if (!/^version=[012]$/.test(input.gitProtocol)) throw new Error('invalid Git protocol');
      headers['git-protocol'] = input.gitProtocol;
    }
    if (input.method === 'POST') {
      headers['content-type'] = GIT_UPLOAD_REQUEST_TYPE;
      headers['content-length'] = String(input.body?.length ?? 0);
    }
    return new Promise<Buffer>((resolve, reject) => {
      // An independent timer also bounds DNS/TLS and slow trickles. agent:false
      // ensures each connection uses the guarded lookup rather than a pooled socket.
      const controller = new AbortController();
      const abort = () => controller.abort();
      const timeout = setTimeout(abort, options.timeoutMs ?? GIT_PROXY_TIMEOUT_MS);
      input.signal.addEventListener('abort', abort, { once: true });
      if (input.signal.aborted) abort();
      const cleanup = () => {
        clearTimeout(timeout);
        input.signal.removeEventListener('abort', abort);
      };
      const fail = () => {
        cleanup();
        reject(new Error('Git upstream request failed'));
      };
      try {
        const req = request(
          url,
          {
            method: input.method,
            headers,
            agent: false,
            lookup: options.lookup ?? createGitProxyLookup(),
            signal: controller.signal,
            timeout: 15_000,
            maxHeaderSize: 16 * 1024,
          },
          (response) => {
            if (
              response.statusCode !== 200 ||
              response.headers['content-type']?.split(';')[0] !== expectedType ||
              (response.headers['content-encoding'] &&
                response.headers['content-encoding'] !== 'identity')
            ) {
              response.destroy();
              fail();
              return;
            }
            const maxBytes = options.maxResponseBytes ?? GIT_PROXY_RESPONSE_MAX_BYTES;
            if (Number(response.headers['content-length']) > maxBytes) {
              response.destroy();
              fail();
              return;
            }
            const chunks: Buffer[] = [];
            let size = 0;
            response.on('data', (bytes: Buffer) => {
              size += bytes.length;
              if (size > maxBytes) {
                response.destroy();
                fail();
                return;
              }
              chunks.push(bytes);
            });
            response.on('error', fail);
            response.on('aborted', fail);
            response.on('end', () => {
              cleanup();
              resolve(Buffer.concat(chunks, size));
            });
          },
        );
        req.on('timeout', () => req.destroy());
        req.on('error', fail);
        req.end(input.body);
      } catch {
        fail();
      }
    });
  };
}
