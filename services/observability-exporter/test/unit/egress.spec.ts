// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { EventEmitter } from 'node:events';
import type { IncomingMessage } from 'node:http';
import { Readable } from 'node:stream';
import { afterEach, describe, expect, it, vi } from 'vitest';

const { httpsRequestMock } = vi.hoisted(() => ({ httpsRequestMock: vi.fn() }));
vi.mock('node:https', () => ({ request: httpsRequestMock }));

import {
  createHardenedOtlpFetch,
  createPinnedLookup,
  isPublicOtlpAddress,
  OtlpEgressPolicyError,
} from '../../src/egress.js';

afterEach(() => httpsRequestMock.mockReset());

describe('OTLP egress admission', () => {
  it('rejects loopback, private, link-local, metadata, and IPv6 local ranges', () => {
    for (const address of [
      '0.0.0.0',
      '10.0.0.1',
      '100.64.0.1',
      '127.0.0.1',
      '169.254.169.254',
      '172.16.0.1',
      '192.168.0.1',
      '::1',
      'fc00::1',
      'fe80::1',
      '::127.0.0.1',
      '64:ff9b:1::a9fe:a9fe',
      '100::1',
      '2001:2::1',
      '3fff::1',
      '5f00::1',
      '64:ff9b::a9fe:a9fe',
      '2002:7f00:1::1',
    ]) {
      expect(isPublicOtlpAddress(address), address).toBe(false);
    }
    expect(isPublicOtlpAddress('8.8.8.8')).toBe(true);
    expect(isPublicOtlpAddress('2606:4700:4700::1111')).toBe(true);
    expect(isPublicOtlpAddress('::ffff:8.8.8.8')).toBe(true);
    expect(isPublicOtlpAddress('64:ff9b::808:808')).toBe(true);
  });

  it('rejects direct loopback before opening a socket', async () => {
    const guarded = createHardenedOtlpFetch();
    await expect(
      guarded('https://127.0.0.1:4318/api/public/otel/v1/traces', {
        method: 'POST',
        redirect: 'manual',
        body: '{}',
      }),
    ).rejects.toBeInstanceOf(OtlpEgressPolicyError);
  });

  it('returns the Node 22 all-address lookup shape for a pinned hostname', async () => {
    const lookup = createPinnedLookup({ address: '8.8.8.8', family: 4 });
    const result = await new Promise<unknown>((resolve, reject) => {
      lookup('collector.example', { all: true }, (error, address, family) => {
        if (error) reject(error);
        else resolve({ address, family });
      });
    });
    expect(result).toEqual({ address: [{ address: '8.8.8.8', family: 4 }], family: undefined });
  });

  it('rejects cleartext public OTLP endpoints before DNS resolution', async () => {
    const resolve = vi.fn(async () => [{ address: '8.8.8.8', family: 4 as const }]);
    const guarded = createHardenedOtlpFetch({ resolve });
    await expect(
      guarded('http://collector.example/api/public/otel/v1/traces', {
        method: 'POST',
        redirect: 'manual',
        body: '{}',
      }),
    ).rejects.toBeInstanceOf(OtlpEgressPolicyError);
    expect(resolve).not.toHaveBeenCalled();
  });

  it('rejects a DNS answer set containing any private address', async () => {
    const resolve = vi.fn(async () => [
      { address: '8.8.8.8', family: 4 as const },
      { address: '169.254.169.254', family: 4 as const },
    ]);
    const guarded = createHardenedOtlpFetch({ resolve });
    await expect(
      guarded('https://collector.example/api/public/otel/v1/traces', {
        method: 'POST',
        redirect: 'manual',
        body: '{}',
      }),
    ).rejects.toBeInstanceOf(OtlpEgressPolicyError);
    expect(resolve).toHaveBeenCalledWith('collector.example');
  });

  it('keeps DNS resolution inside the caller timeout boundary', async () => {
    const guarded = createHardenedOtlpFetch({ resolve: async () => await new Promise(() => {}) });
    await expect(
      guarded('https://collector.example/api/public/otel/v1/traces', {
        method: 'POST',
        redirect: 'manual',
        body: '{}',
        signal: AbortSignal.timeout(10),
      }),
    ).rejects.toBeDefined();
  });

  it.each([204, 205, 304])('constructs body-forbidden HTTP %s responses safely', async (status) => {
    httpsRequestMock.mockImplementation((...args: unknown[]) => {
      const onResponse = args[2] as (incoming: IncomingMessage) => void;
      const incoming = Readable.from(['must be discarded']) as IncomingMessage;
      incoming.statusCode = status;
      incoming.statusMessage = 'No Body';
      incoming.headers = { 'content-type': 'application/json' };
      const outgoing = new EventEmitter() as EventEmitter & { end(): void };
      outgoing.end = () => onResponse(incoming);
      return outgoing;
    });
    const guarded = createHardenedOtlpFetch({
      resolve: async () => [{ address: '8.8.8.8', family: 4 }],
    });

    const response = await guarded('https://collector.example/api/public/otel/v1/traces', {
      method: 'POST',
      redirect: 'manual',
      body: '{}',
    });

    expect(response.status).toBe(status);
    expect(response.body).toBeNull();
  });
});
