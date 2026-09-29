// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { lookup as lookupDns } from 'node:dns/promises';
import { request as httpsRequest } from 'node:https';
import { isIP, type LookupFunction } from 'node:net';
import { Readable } from 'node:stream';
import ip from '@bybrave/ip2';

export interface OtlpResolvedAddress {
  address: string;
  family: 4 | 6;
}

export type OtlpDnsResolver = (hostname: string) => Promise<readonly OtlpResolvedAddress[]>;

export class OtlpEgressPolicyError extends Error {
  constructor() {
    super('OTLP endpoint is not admitted by exporter egress policy');
    this.name = 'OtlpEgressPolicyError';
  }
}

/** Public-only address admission for the current endpoint_class slice. */
export function isPublicOtlpAddress(address: string): boolean {
  try {
    return ip.isValid(address) && ip.isPublic(address);
  } catch {
    return false;
  }
}

/**
 * Fetch-compatible OTLP transport that resolves once, admits every answer,
 * then pins the actual socket lookup to one approved address. Node's direct
 * request path ignores ambient HTTP proxy variables and never follows redirects.
 */
export function createHardenedOtlpFetch(
  options: {
    resolve?: OtlpDnsResolver;
  } = {},
): typeof fetch {
  const resolve = options.resolve ?? defaultResolver;
  return (async (input: string | URL | Request, init: RequestInit = {}): Promise<Response> => {
    const url = input instanceof Request ? new URL(input.url) : new URL(input);
    if (url.protocol !== 'https:') throw new OtlpEgressPolicyError();
    if (init.redirect !== undefined && init.redirect !== 'manual') {
      throw new OtlpEgressPolicyError();
    }

    const hostname = url.hostname.startsWith('[') ? url.hostname.slice(1, -1) : url.hostname;
    const literalFamily = isIP(hostname);
    const addresses = literalFamily
      ? [{ address: hostname, family: literalFamily as 4 | 6 }]
      : await withAbort(resolve(hostname), init.signal);
    if (
      addresses.length === 0 ||
      addresses.some(
        ({ address, family }) => isIP(address) !== family || !isPublicOtlpAddress(address),
      )
    ) {
      throw new OtlpEgressPolicyError();
    }
    const pinned = addresses[0]!;
    const body = requestBody(init.body);
    const headers = Object.fromEntries(new Headers(init.headers).entries());
    const pinnedLookup = createPinnedLookup(pinned);

    return await new Promise<Response>((resolveResponse, reject) => {
      const outgoing = httpsRequest(
        url,
        {
          method: init.method ?? 'GET',
          headers,
          ...(init.signal === undefined || init.signal === null ? {} : { signal: init.signal }),
          lookup: pinnedLookup,
        },
        (incoming) => {
          const status = incoming.statusCode;
          if (status === undefined || status < 200 || status > 599) {
            incoming.destroy();
            reject(new Error('OTLP endpoint returned an invalid HTTP status'));
            return;
          }
          const responseHeaders = new Headers();
          for (const [name, value] of Object.entries(incoming.headers)) {
            if (Array.isArray(value)) {
              value.forEach((entry) => responseHeaders.append(name, entry));
            } else if (value !== undefined) {
              responseHeaders.set(name, value);
            }
          }
          const responseBody =
            status === 204 || status === 205 || status === 304
              ? null
              : (Readable.toWeb(incoming) as ReadableStream<Uint8Array>);
          if (responseBody === null) incoming.destroy();
          try {
            resolveResponse(
              new Response(responseBody, {
                status,
                ...(incoming.statusMessage === undefined
                  ? {}
                  : { statusText: incoming.statusMessage }),
                headers: responseHeaders,
              }),
            );
          } catch (error) {
            incoming.destroy();
            reject(error);
          }
        },
      );
      outgoing.once('error', reject);
      outgoing.end(body);
    });
  }) as typeof fetch;
}

/** Node 22 may request an all-address result even for a custom lookup. */
export function createPinnedLookup(pinned: OtlpResolvedAddress): LookupFunction {
  return (_hostname, options, callback) => {
    if (options.all) {
      callback(null, [{ address: pinned.address, family: pinned.family }]);
      return;
    }
    callback(null, pinned.address, pinned.family);
  };
}

async function defaultResolver(hostname: string): Promise<readonly OtlpResolvedAddress[]> {
  const addresses = await lookupDns(hostname, { all: true, verbatim: true });
  return addresses.flatMap(({ address, family }) =>
    family === 4 || family === 6 ? [{ address, family }] : [],
  );
}

function requestBody(body: BodyInit | null | undefined): string | Uint8Array | undefined {
  if (body === undefined || body === null) return undefined;
  if (typeof body === 'string' || body instanceof Uint8Array) return body;
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  throw new OtlpEgressPolicyError();
}

async function withAbort<T>(
  promise: Promise<T>,
  signal: AbortSignal | null | undefined,
): Promise<T> {
  if (signal === undefined || signal === null) return await promise;
  if (signal.aborted) throw signal.reason;
  return await new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort));
  });
}
