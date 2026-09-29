// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @orca/e2e-tests — unit coverage for `apiCall`.
 *
 * Layer A black-box specs hit the real registry, but a non-JSON 5xx body
 * (Fastify's default plain-text "Internal Server Error" or an upstream
 * proxy's HTML 502) needs to surface a useful error — not the cryptic
 * `SyntaxError: Unexpected token I in JSON at position 0` you get when the
 * lazy `.json()` accessor blindly calls `JSON.parse`.
 *
 * This spec stubs `globalThis.fetch` so it doesn't touch the network and
 * exercises the JSON-error context path in `apiCall(...)`. It runs as part
 * of `pnpm test:wire` because the wire spec file uses the same helper —
 * keeping the unit next to the integration coverage avoids a separate
 * vitest config file.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import Anthropic from '@anthropic-ai/sdk';
import { apiCall, type OrcaClientConfig } from '../src/client.js';

function buildTestConfig(): OrcaClientConfig {
  // The SDK is constructed but never called — apiCall uses the bare
  // baseURL+apiKey fields, not the SDK methods.
  const sdk = new Anthropic({ baseURL: 'http://test.invalid', apiKey: 'orca_test_key' });
  return { baseURL: 'http://test.invalid', apiKey: 'orca_test_key', sdk };
}

describe('apiCall — JSON error context', () => {
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    // Replace fetch in-scope per test; afterEach restores the original.
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    globalThis.fetch = originalFetch;
  });

  it('annotates the error with method + path + status when the body is not JSON', async () => {
    // Simulate Fastify's default 500 envelope when an unhandled error escapes
    // a route handler before it hits the error handler — text/plain body.
    (globalThis.fetch as unknown as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      new Response('Internal Server Error', {
        status: 500,
        headers: { 'content-type': 'text/plain' },
      }),
    );

    const cfg = buildTestConfig();
    const res = await apiCall(cfg, '/v1/agents', { method: 'POST', body: '{}' });

    // The text body is always populated even when it's not JSON.
    expect(res.status).toBe(500);
    expect(res.text).toBe('Internal Server Error');

    // Calling .json() must throw with a useful, named error — not the raw
    // SyntaxError from JSON.parse.
    expect(() => res.json()).toThrow(/POST \/v1\/agents/);
    expect(() => res.json()).toThrow(/status=500/);
    expect(() => res.json()).toThrow(/Internal Server Error/);
  });

  it('returns parsed JSON on the happy path', async () => {
    (globalThis.fetch as unknown as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      new Response(JSON.stringify({ ok: true, id: 'agt_x' }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    );

    const cfg = buildTestConfig();
    const res = await apiCall(cfg, '/v1/agents/agt_x', { method: 'GET' });
    expect(res.status).toBe(200);
    expect(res.json<{ ok: boolean; id: string }>()).toEqual({ ok: true, id: 'agt_x' });
  });

  it('returns undefined for empty bodies (e.g. 204 DELETE)', async () => {
    // The Response constructor in Node 20 forbids non-null body for status 204
    // (matches the WHATWG Fetch spec). Stub a minimal Response-shape object
    // instead — apiCall only reads `.status`, `.headers`, and `.text()`.
    const stub = {
      status: 204,
      headers: new Headers(),
      text: async () => '',
    };
    (globalThis.fetch as unknown as ReturnType<typeof vi.fn>).mockResolvedValueOnce(stub);

    const cfg = buildTestConfig();
    const res = await apiCall(cfg, '/v1/agents/agt_x', { method: 'DELETE', body: '{}' });
    expect(res.status).toBe(204);
    expect(res.text).toBe('');
    expect(res.json()).toBeUndefined();
  });

  it('defaults the method to GET in the error message when init.method is omitted', async () => {
    (globalThis.fetch as unknown as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      new Response('<html>502 Bad Gateway</html>', {
        status: 502,
        headers: { 'content-type': 'text/html' },
      }),
    );

    const cfg = buildTestConfig();
    const res = await apiCall(cfg, '/v1/agents');
    expect(() => res.json()).toThrow(/GET \/v1\/agents/);
    expect(() => res.json()).toThrow(/status=502/);
  });
});
