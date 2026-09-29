// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { MAX_DELIVERY_RETRY_DELAY_MS } from '../../src/delivery-retry.js';
import {
  LitefuseOtlpHttpClient,
  OtlpHttpResponseError,
  OtlpHttpStatusError,
  OtlpPartialSuccessError,
} from '../../src/litefuse-client.js';
import { encodeLangfuseOtlpJson } from '../../src/otlp-json.js';
import { TRANSCRIPT_SECRET, completedProjectedTrace } from '../support/events.js';

interface CapturedRequest {
  url: string | undefined;
  headers: Record<string, string | string[] | undefined>;
  body: string;
}

interface MockServer {
  endpoint: string;
  requests: CapturedRequest[];
  close(): Promise<void>;
}

const servers: MockServer[] = [];

afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(servers.splice(0).map((server) => server.close()));
});

describe('LitefuseOtlpHttpClient', () => {
  it('posts OTLP JSON to exact traces path with Litefuse headers', async () => {
    const server = await startMockServer(200);
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
      timeoutMs: 1_000,
    });

    await client.send(encodeLangfuseOtlpJson(completedProjectedTrace()));

    expect(server.requests).toHaveLength(1);
    const request = server.requests[0]!;
    expect(request.url).toBe('/api/public/otel/v1/traces');
    expect(request.headers.authorization).toBe(
      `Basic ${Buffer.from('pk-test:sk-test').toString('base64')}`,
    );
    expect(request.headers.accept).toBe('application/json');
    expect(request.headers['content-type']).toBe('application/json');
    expect(request.headers['x-langfuse-ingestion-version']).toBe('4');
    expect(request.body).not.toContain(TRANSCRIPT_SECRET);
    expect(JSON.parse(request.body)).toHaveProperty('resourceSpans');
  });

  it('throws typed error for non-2xx without retaining upstream body', async () => {
    const server = await startMockServer(503, 'sensitive upstream response');
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    });

    const error = await client
      .send(encodeLangfuseOtlpJson(completedProjectedTrace()))
      .catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(OtlpHttpStatusError);
    expect(error).toMatchObject({ status: 503 });
    expect((error as Error).message).not.toContain('sensitive upstream response');
  });

  it('parses Retry-After seconds and HTTP-date without retaining the raw header', async () => {
    vi.useFakeTimers();
    const now = Date.parse('2026-09-05T12:00:00.000Z');
    vi.setSystemTime(now);
    const invalidHeader = 'invalid-retry-after-sensitive-marker';
    const cases = [
      { header: '45', expected: 45_000 },
      { header: new Date(now + 90_000).toUTCString(), expected: 90_000 },
      { header: invalidHeader, expected: undefined },
      { header: '999999999999999999999999', expected: MAX_DELIVERY_RETRY_DELAY_MS },
    ];

    for (const testCase of cases) {
      const client = new LitefuseOtlpHttpClient({
        endpoint: 'https://litefuse.example/api/public/otel/v1/traces',
        publicKey: 'pk-test',
        secretKey: 'sk-test',
        fetchImpl: async () =>
          new Response('', {
            status: 503,
            headers: { 'retry-after': testCase.header },
          }),
      });
      const error = await client
        .send(encodeLangfuseOtlpJson(completedProjectedTrace()))
        .catch((failure: unknown) => failure);

      expect(error).toBeInstanceOf(OtlpHttpStatusError);
      expect(error).toMatchObject({ status: 503, retryAfterMs: testCase.expected });
      if (testCase.header === invalidHeader) {
        expect(JSON.stringify(error)).not.toContain(invalidHeader);
        expect((error as Error).message).not.toContain(invalidHeader);
      }
    }
  });

  it('rejects base endpoints rather than expanding or redirecting them', () => {
    expect(
      () =>
        new LitefuseOtlpHttpClient({
          endpoint: 'https://litefuse.example/api/public/otel',
          publicKey: 'pk-test',
          secretKey: 'sk-test',
        }),
    ).toThrow('must be exact /api/public/otel/v1/traces');
  });

  it('uses manual redirect policy and a timeout signal', async () => {
    const fetchImpl = async (
      _url: string | URL | Request,
      init?: RequestInit,
    ): Promise<Response> => {
      expect(init?.redirect).toBe('manual');
      expect(init?.signal).toBeInstanceOf(AbortSignal);
      return new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } });
    };
    const client = new LitefuseOtlpHttpClient({
      endpoint: 'https://litefuse.example/api/public/otel/v1/traces',
      publicKey: 'pk-test',
      secretKey: 'sk-test',
      timeoutMs: 1_000,
      fetchImpl,
    });

    await expect(client.send(encodeLangfuseOtlpJson(completedProjectedTrace()))).resolves.toEqual({
      kind: 'accepted',
    });
  });

  it('rejects non-200 2xx statuses', async () => {
    const server = await startMockServer(204, '');
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    });

    await expect(
      client.send(encodeLangfuseOtlpJson(completedProjectedTrace())),
    ).rejects.toMatchObject({ status: 204 });
  });

  it.each([
    ['missing', null],
    ['wrong', 'text/plain'],
  ])('rejects HTTP 200 responses with %s Content-Type', async (_kind, contentType) => {
    const server = await startMockServer(200, '{}', contentType);
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    });

    await expect(
      client.send(encodeLangfuseOtlpJson(completedProjectedTrace())),
    ).rejects.toBeInstanceOf(OtlpHttpResponseError);
  });

  it('accepts application/json Content-Type parameters', async () => {
    const server = await startMockServer(200, '{}', 'Application/JSON; charset=utf-8');
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    });

    await expect(client.send(encodeLangfuseOtlpJson(completedProjectedTrace()))).resolves.toEqual({
      kind: 'accepted',
    });
  });

  it.each([
    ['zero-byte', ''],
    ['whitespace-only', ' \t\r\n'],
    ['JSON array', '[]'],
    ['JSON null', 'null'],
  ])('rejects HTTP 200 responses with %s bodies', async (_kind, responseBody) => {
    const server = await startMockServer(200, responseBody);
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    });

    await expect(
      client.send(encodeLangfuseOtlpJson(completedProjectedTrace())),
    ).rejects.toBeInstanceOf(OtlpHttpResponseError);
  });

  it('rejects partial success with rejected spans', async () => {
    const upstreamMessage = 'not retained';
    const server = await startMockServer(
      200,
      JSON.stringify({
        partialSuccess: { rejectedSpans: '1', errorMessage: upstreamMessage },
      }),
    );
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    });

    const error = await client
      .send(encodeLangfuseOtlpJson(completedProjectedTrace()))
      .catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(OtlpPartialSuccessError);
    expect(error).toMatchObject({
      rejectedSpans: '1',
      messageBytes: Buffer.byteLength(upstreamMessage),
      messageSha256: expect.stringMatching(/^[0-9a-f]{64}$/),
    });
    expect(JSON.stringify(error)).not.toContain(upstreamMessage);
    expect((error as Error).message).not.toContain(upstreamMessage);
  });

  it.each([
    ['unquoted exponent', '{"partialSuccess":{"rejectedSpans":1e2}}', '100'],
    ['quoted exponent', '{"partialSuccess":{"rejectedSpans":"1200E-2"}}', '12'],
    ['zero fractional decimal', '{"partialSuccess":{"rejectedSpans":"1.0"}}', '1'],
    [
      'signed-int64 maximum decimal string',
      '{"partialSuccess":{"rejectedSpans":"9223372036854775807"}}',
      '9223372036854775807',
    ],
    [
      'signed-int64 maximum unquoted decimal',
      '{"partialSuccess":{"rejectedSpans":9223372036854775807}}',
      '9223372036854775807',
    ],
    [
      'signed-int64 maximum exponent string',
      '{"partialSuccess":{"rejectedSpans":"9.223372036854775807e18"}}',
      '9223372036854775807',
    ],
  ])('parses %s without losing rejected span precision', async (_kind, body, rejectedSpans) => {
    const error = await sendMockResponse(body).catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(OtlpPartialSuccessError);
    expect(error).toMatchObject({ rejectedSpans });
  });

  it.each([
    ['message null', '{"partialSuccess":null}'],
    ['scalar nulls', '{"partialSuccess":{"rejectedSpans":null,"errorMessage":null}}'],
  ])('treats ProtoJSON %s as unset', async (_kind, body) => {
    await expect(sendMockResponse(body)).resolves.toEqual({ kind: 'accepted' });
  });

  it.each([
    ['unknown top-level field', '{"unknown":{"rejectedSpans":1}}'],
    ['unknown partial-success field', '{"partialSuccess":{"unknown":{"nested":true}}}'],
    ['snake_case top-level field', '{"partial_success":{"rejectedSpans":"1"}}'],
    [
      'snake_case partial-success fields',
      '{"partialSuccess":{"rejected_spans":"1","error_message":"ignored"}}',
    ],
    [
      'unknown and snake_case siblings of known zero',
      '{"other":false,"partialSuccess":{"rejectedSpans":0,"rejected_spans":"1","unknown":null}}',
    ],
    ['unknown escaped lone surrogate', '{"unknown":"\\ud800"}'],
  ])('ignores OTLP/JSON %s', async (_kind, body) => {
    await expect(sendMockResponse(body)).resolves.toEqual({ kind: 'accepted' });
  });

  it('reads known fields only from own properties under Object.prototype pollution', async () => {
    const pollution: Record<string, unknown> = {
      partialSuccess: { rejectedSpans: '9', errorMessage: 'inherited warning' },
      rejectedSpans: '8',
      errorMessage: 'inherited warning',
    };
    const originals = new Map(
      Object.keys(pollution).map((key) => [
        key,
        Object.getOwnPropertyDescriptor(Object.prototype, key),
      ]),
    );

    try {
      for (const [key, value] of Object.entries(pollution)) {
        Object.defineProperty(Object.prototype, key, { configurable: true, value, writable: true });
      }

      await expect(sendMockResponse('{}')).resolves.toEqual({ kind: 'accepted' });
      await expect(sendMockResponse('{"partialSuccess":{}}')).resolves.toEqual({
        kind: 'accepted',
      });
      const ownRejection = await sendMockResponse('{"partialSuccess":{"rejectedSpans":"2"}}').catch(
        (failure: unknown) => failure,
      );
      expect(ownRejection).toBeInstanceOf(OtlpPartialSuccessError);
      expect(ownRejection).toMatchObject({ rejectedSpans: '2' });
    } finally {
      for (const [key, descriptor] of originals) {
        if (descriptor === undefined) Reflect.deleteProperty(Object.prototype, key);
        else Object.defineProperty(Object.prototype, key, descriptor);
      }
    }
  });

  it('processes a known partial rejection when unknown fields are siblings', async () => {
    const error = await sendMockResponse(
      '{"unknown":true,"partialSuccess":{"unknownNested":{},"rejectedSpans":"2"}}',
    ).catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(OtlpPartialSuccessError);
    expect(error).toMatchObject({ rejectedSpans: '2' });
  });

  it.each([
    ['array partial success', '{"partialSuccess":[]}'],
    ['negative rejected spans', '{"partialSuccess":{"rejectedSpans":-1}}'],
    ['fractional rejected spans', '{"partialSuccess":{"rejectedSpans":1.5}}'],
    ['non-integral exponent', '{"partialSuccess":{"rejectedSpans":"1e-1"}}'],
    ['signed-int64 overflow', '{"partialSuccess":{"rejectedSpans":"9223372036854775808"}}'],
    ['numeric string whitespace', '{"partialSuccess":{"rejectedSpans":" 1"}}'],
    [
      'known invalid field with unknown siblings',
      '{"partialSuccess":{"rejectedSpans":"invalid","rejected_spans":"1","unknown":null}}',
    ],
  ])('rejects invalid ExportTraceServiceResponse ProtoJSON: %s', async (_kind, body) => {
    await expect(sendMockResponse(body)).rejects.toBeInstanceOf(OtlpHttpResponseError);
  });

  it('accepts explicit zero-rejection partial success', async () => {
    const server = await startMockServer(
      200,
      JSON.stringify({ partialSuccess: { rejectedSpans: 0 } }),
    );
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    });

    await expect(client.send(encodeLangfuseOtlpJson(completedProjectedTrace()))).resolves.toEqual({
      kind: 'accepted',
    });
  });

  it('requires a complete bounded response body under the request timeout', async () => {
    const server = await startStallingServer();
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
      timeoutMs: 25,
    });

    await expect(
      client.send(encodeLangfuseOtlpJson(completedProjectedTrace())),
    ).rejects.toMatchObject({ kind: 'timeout' });
  });

  it('classifies non-200 before consuming an untrusted stalled body', async () => {
    const server = await startStallingServer(503);
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
      timeoutMs: 1_000,
    });

    await expect(
      client.send(encodeLangfuseOtlpJson(completedProjectedTrace())),
    ).rejects.toMatchObject({ status: 503 });
  });

  it('classifies redirects as permanent status failures without forwarding credentials', async () => {
    const target = await startMockServer(200);
    const redirect = await startRedirectServer(target.endpoint);
    const client = new LitefuseOtlpHttpClient({
      endpoint: redirect.endpoint,
      publicKey: 'pk-redirect-source',
      secretKey: 'sk-redirect-source',
    });

    await expect(
      client.send(encodeLangfuseOtlpJson(completedProjectedTrace())),
    ).rejects.toMatchObject({ status: 302 });
    expect(target.requests).toEqual([]);
  });

  it('distinguishes transport failure from complete protocol failure', async () => {
    const client = new LitefuseOtlpHttpClient({
      endpoint: 'https://litefuse.example/api/public/otel/v1/traces',
      publicKey: 'pk-test',
      secretKey: 'sk-test',
      fetchImpl: async () => {
        throw new TypeError('socket disconnected');
      },
    });

    await expect(client.send(encodeLangfuseOtlpJson(completedProjectedTrace()))).rejects.toEqual(
      expect.objectContaining({ kind: 'transport' }),
    );
  });

  it.each([undefined, null, 0, '0'])(
    'returns only safe warning metadata with rejectedSpans=%s',
    async (rejectedSpans) => {
      const warning = 'collector warning that must not escape';
      const server = await startMockServer(
        200,
        JSON.stringify({ partialSuccess: { rejectedSpans, errorMessage: warning } }),
      );
      const client = new LitefuseOtlpHttpClient({
        endpoint: server.endpoint,
        publicKey: 'pk-test',
        secretKey: 'sk-test',
      });

      const outcome = await client.send(encodeLangfuseOtlpJson(completedProjectedTrace()));
      expect(outcome).toEqual({
        kind: 'accepted_with_warning',
        rejectedSpans: '0',
        messageBytes: 38,
        messageSha256: 'b68a6b79b62c91ae5da675a96c3f38d74f4a41ec27870536f2ff7174b5cbe707',
      });
      expect(JSON.stringify(outcome)).not.toContain(warning);
      expect(server.requests).toHaveLength(1);
    },
  );

  it('returns plain acceptance for an explicitly empty warning', async () => {
    await expect(
      sendMockResponse('{"partialSuccess":{"rejectedSpans":"0","errorMessage":""}}'),
    ).resolves.toEqual({ kind: 'accepted' });
  });

  it('rejects malformed UTF-8 before producing warning metadata', async () => {
    const invalidUtf8 = Buffer.concat([
      Buffer.from('{"partialSuccess":{"errorMessage":"'),
      Buffer.from([0xc3, 0x28]),
      Buffer.from('"}}'),
    ]);
    const error = await sendMockResponse(invalidUtf8).catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(OtlpHttpResponseError);
    expect(error).not.toHaveProperty('warningSha256');
    expect(JSON.stringify(error)).not.toContain('\uFFFD');
  });

  it.each([
    ['lone high surrogate', '{"partialSuccess":{"errorMessage":"\\ud800"}}'],
    ['lone low surrogate', '{"partialSuccess":{"errorMessage":"\\udc00"}}'],
  ])('rejects escaped %s before producing warning metadata', async (_kind, responseBody) => {
    const error = await sendMockResponse(responseBody).catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(OtlpHttpResponseError);
    expect(error).not.toHaveProperty('warningSha256');
  });

  it('accepts an escaped valid surrogate pair in errorMessage', async () => {
    await expect(
      sendMockResponse('{"partialSuccess":{"errorMessage":"\\ud83d\\ude00"}}'),
    ).resolves.toEqual({
      kind: 'accepted_with_warning',
      rejectedSpans: '0',
      messageBytes: 4,
      messageSha256: 'f0443a342c5ef54783a111b51ba56c938e474c32324d90c3a60c9c8e3a37e2d9',
    });
  });

  it('rejects invalid known partial-success fields', async () => {
    const server = await startMockServer(
      200,
      JSON.stringify({ partialSuccess: { rejectedSpans: 0, errorMessage: 42 } }),
    );
    const client = new LitefuseOtlpHttpClient({
      endpoint: server.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    });

    await expect(
      client.send(encodeLangfuseOtlpJson(completedProjectedTrace())),
    ).rejects.toBeInstanceOf(OtlpHttpResponseError);
  });

  it('rejects malformed or oversized HTTP 200 responses', async () => {
    const malformed = await startMockServer(200, '{');
    const malformedClient = new LitefuseOtlpHttpClient({
      endpoint: malformed.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    });
    await expect(
      malformedClient.send(encodeLangfuseOtlpJson(completedProjectedTrace())),
    ).rejects.toBeInstanceOf(OtlpHttpResponseError);

    const oversized = await startMockServer(200, 'x'.repeat(64 * 1024 + 1));
    const oversizedClient = new LitefuseOtlpHttpClient({
      endpoint: oversized.endpoint,
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    });
    await expect(
      oversizedClient.send(encodeLangfuseOtlpJson(completedProjectedTrace())),
    ).rejects.toBeInstanceOf(OtlpHttpResponseError);
  });

  it('aligns endpoint, credential, and timeout bounds with Registry', () => {
    const base = {
      endpoint: 'https://litefuse.example/api/public/otel/v1/traces',
      publicKey: 'pk-test',
      secretKey: 'sk-test',
    };
    expect(() => new LitefuseOtlpHttpClient({ ...base, endpoint: ` ${base.endpoint}` })).toThrow(
      'canonical',
    );
    expect(
      () => new LitefuseOtlpHttpClient({ ...base, secretKey: 'x'.repeat(16 * 1024 + 1) }),
    ).toThrow('secretKey');
    expect(() => new LitefuseOtlpHttpClient({ ...base, publicKey: 'pk:ambiguous' })).toThrow(
      'must not contain a colon',
    );
    expect(() => new LitefuseOtlpHttpClient({ ...base, timeoutMs: 120_001 })).toThrow(
      'between 1 and 120000',
    );
  });
});

async function sendMockResponse(responseBody: string | Uint8Array) {
  const server = await startMockServer(200, responseBody);
  const client = new LitefuseOtlpHttpClient({
    endpoint: server.endpoint,
    publicKey: 'pk-test',
    secretKey: 'sk-test',
  });
  return client.send(encodeLangfuseOtlpJson(completedProjectedTrace()));
}

async function startMockServer(
  status: number,
  responseBody: string | Uint8Array = '{}',
  responseContentType: string | null = 'application/json',
): Promise<MockServer> {
  const requests: CapturedRequest[] = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request)
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    requests.push({
      url: request.url,
      headers: request.headers,
      body: Buffer.concat(chunks).toString('utf8'),
    });
    response.statusCode = status;
    if (responseContentType !== null) response.setHeader('content-type', responseContentType);
    response.end(responseBody);
  });
  await listen(server);
  const address = server.address() as AddressInfo;
  const mock: MockServer = {
    endpoint: `http://127.0.0.1:${address.port}/api/public/otel/v1/traces`,
    requests,
    close: () => close(server),
  };
  servers.push(mock);
  return mock;
}

async function startStallingServer(status = 200): Promise<MockServer> {
  const requests: CapturedRequest[] = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request)
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    requests.push({
      url: request.url,
      headers: request.headers,
      body: Buffer.concat(chunks).toString('utf8'),
    });
    response.writeHead(status, { 'content-type': 'application/json' });
    response.write('{');
    setTimeout(() => response.end('}'), 250).unref();
  });
  await listen(server);
  const address = server.address() as AddressInfo;
  const mock: MockServer = {
    endpoint: `http://127.0.0.1:${address.port}/api/public/otel/v1/traces`,
    requests,
    close: () => close(server),
  };
  servers.push(mock);
  return mock;
}

async function startRedirectServer(location: string): Promise<MockServer> {
  const requests: CapturedRequest[] = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request)
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    requests.push({
      url: request.url,
      headers: request.headers,
      body: Buffer.concat(chunks).toString('utf8'),
    });
    response.writeHead(302, { location });
    response.end();
  });
  await listen(server);
  const address = server.address() as AddressInfo;
  const mock: MockServer = {
    endpoint: `http://127.0.0.1:${address.port}/api/public/otel/v1/traces`,
    requests,
    close: () => close(server),
  };
  servers.push(mock);
  return mock;
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });
}

function close(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => (error === undefined ? resolve() : reject(error)));
  });
}
