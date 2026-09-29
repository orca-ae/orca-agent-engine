// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import Anthropic from '@anthropic-ai/sdk';

/**
 * Resolved configuration for the e2e-tests client. `baseURL` defaults to the
 * `make stack-up` registry port (`http://localhost:8080`); override with
 * `ORCA_BASE_URL` to point at a remote stack.
 */
export interface OrcaClientConfig {
  baseURL: string;
  apiKey: string;
  /**
   * Official SDK 0.113.0 instance configured against the local Registry.
   * Layer A uses it for Managed Agents round-trips and keeps raw fetch for
   * exact-key, error-envelope, and extension assertions.
   */
  sdk: Anthropic;
}

export function buildClient(): OrcaClientConfig {
  const baseURL = process.env['ORCA_BASE_URL'] ?? 'http://localhost:8080';
  const apiKey = process.env['ORCA_API_KEY'] ?? '';
  if (!apiKey) {
    throw new Error(
      'buildClient: ORCA_API_KEY env var is required. Specs typically set this in beforeAll() ' +
        "via the seedWorkspaceApiKey() helper — pass the result's apiKey through.",
    );
  }
  const sdk = new Anthropic({
    baseURL,
    apiKey,
    // The Anthropic SDK guards against missing API keys with an environment
    // check at construction time even when explicitly passed; setting this
    // allows the SDK to be instantiated in tests that don't ship a real
    // ANTHROPIC_API_KEY env var (Layer A doesn't need one).
    dangerouslyAllowBrowser: false,
  });
  return { baseURL, apiKey, sdk };
}

/**
 * Build a `OrcaClientConfig` directly from already-resolved values. Useful in
 * `beforeAll` where the spec just received a fresh api key from
 * `seedWorkspaceApiKey()` and doesn't want to round-trip through process.env.
 */
export function buildClientFromConfig(opts: {
  baseURL?: string;
  apiKey: string;
}): OrcaClientConfig {
  const baseURL = opts.baseURL ?? process.env['ORCA_BASE_URL'] ?? 'http://localhost:8080';
  const sdk = new Anthropic({
    baseURL,
    apiKey: opts.apiKey,
    dangerouslyAllowBrowser: false,
  });
  return { baseURL, apiKey: opts.apiKey, sdk };
}

/**
 * Standard request headers the registry's api-key middleware accepts.
 * `x-api-key` is the Anthropic-canonical header (matches their hosted API);
 * the registry's `buildApiKeyAuth` looks at that header specifically and
 * rejects anything else with a 401. Header-level equivalence is outside the
 * compatibility milestone; raw calls use the Managed Agents beta token and
 * the Registry permissively accepts the family-specific values sent by SDK
 * methods.
 */
export function authHeaders(cfg: OrcaClientConfig): Record<string, string> {
  return {
    'x-api-key': cfg.apiKey,
    'anthropic-beta': 'managed-agents-2026-04-01',
  };
}

/**
 * Pre-flight that the local stack is reachable. Calls `/healthz` (the
 * registry's unauthenticated health probe) with a short timeout. If the stack
 * isn't up, the spec should fail FAST with a clear instruction, not after
 * a 60s connection-refused timeout chain.
 *
 * Throws with a guidance message when the stack is unreachable. This is NOT
 * a skip — Layer A is unconditional. The throw bubbles up and shows the user
 * exactly what to do (`make stack-up`) instead of a cryptic ECONNREFUSED.
 */
export async function ensureStackReachable(cfg: OrcaClientConfig): Promise<void> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 5_000);
  try {
    const res = await fetch(`${cfg.baseURL}/healthz`, { signal: ac.signal });
    if (!res.ok) {
      throw new Error(
        `Orca registry at ${cfg.baseURL}/healthz responded ${res.status}. Is the stack healthy? ` +
          `Try \`make stack-status\`.`,
      );
    }
    const body = (await res.json()) as { status?: string };
    if (body.status !== 'ok') {
      throw new Error(
        `Orca registry /healthz returned status=${body.status}; expected 'ok'. Stack is degraded.`,
      );
    }
  } catch (err) {
    const cause = (err as { cause?: { code?: string } }).cause;
    const code = cause?.code ?? (err as { code?: string }).code;
    if (
      code === 'ECONNREFUSED' ||
      code === 'UND_ERR_SOCKET' ||
      (err as Error).name === 'AbortError'
    ) {
      throw new Error(
        `Orca registry not reachable at ${cfg.baseURL}. Start the stack with \`make stack-up\` ` +
          `(or set ORCA_BASE_URL to a remote stack). Original error: ${(err as Error).message}`,
      );
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Thin wrapper around fetch that injects auth headers, normalizes JSON
 * encoding, and surfaces parsed JSON when the response is application/json.
 *
 * Returns the raw `Response` along with a lazy `.json()` accessor — specs
 * inspect both `res.status` and the parsed body to assert wire-protocol
 * conformance. Errors bubble unchanged; transport failures (ECONNREFUSED)
 * are NOT swallowed.
 */
export interface ApiResponse {
  status: number;
  headers: Headers;
  /** Raw text body. Always populated. */
  text: string;
  /** Parsed JSON body. Throws if the response isn't valid JSON. */
  json: <T = unknown>() => T;
}

export async function apiCall(
  cfg: OrcaClientConfig,
  path: string,
  init: RequestInit = {},
): Promise<ApiResponse> {
  const headers: Record<string, string> = { ...authHeaders(cfg) };
  if (init.body && !(init.body instanceof FormData)) {
    headers['content-type'] = 'application/json';
  }
  // Merge caller-provided headers last so they can override defaults
  // (e.g. omitting x-api-key for the 401 test case).
  if (init.headers) {
    for (const [k, v] of Object.entries(init.headers as Record<string, string>)) {
      headers[k.toLowerCase()] = v;
    }
  }
  const method = (init.method ?? 'GET').toUpperCase();
  const res = await fetch(`${cfg.baseURL}${path}`, { ...init, headers });
  const text = await res.text();
  const status = res.status;
  return {
    status,
    headers: res.headers,
    text,
    json<T = unknown>(): T {
      if (text === '') return undefined as unknown as T;
      try {
        return JSON.parse(text) as T;
      } catch {
        // Servers that crash mid-handler often return text/plain bodies like
        // "Internal Server Error" or upstream proxy HTML. Surfacing the raw
        // SyntaxError ("Unexpected token I in JSON…") buries the actual
        // failure; produce a useful message naming the request that produced
        // the non-JSON body so the offending route is obvious.
        const preview = text.slice(0, 200);
        throw new Error(
          `apiCall: response body for ${method} ${path} (status=${status}) is not JSON: ${preview}`,
        );
      }
    },
  };
}
