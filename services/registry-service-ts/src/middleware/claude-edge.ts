// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { FastifyInstance } from 'fastify';
import type { ClaudeErrorType } from '../contracts/common.js';

interface ErrorEnvelope {
  type: 'error';
  error: {
    type: string;
    message: string;
    [key: string]: unknown;
  };
  request_id: string | null;
}

/**
 * The **resource and discovery** surface of the public listener — the paths an
 * API client calls. A client that parses `{type, error, request_id}` on `/v1`
 * and a bare `{"error": "..."}` on `/apis/...` has to special-case us, which is
 * the whole thing we are avoiding.
 *
 * `/healthz` and `/readyz` are deliberately absent. They are probe endpoints for
 * load balancers and Kubernetes, not for API clients, and nothing parses an
 * error envelope off them. Adding them would be harmless but would widen the
 * rule past what it is for — and a comment claiming "anything reachable by a
 * public client belongs here" invites someone to "fix" the list to match the
 * sentence, so the rule is stated as what it actually is.
 *
 * `/api/v1/*` needs no entry of its own — `rewriteApiV1Alias`
 * (`middleware/api-v1-alias.ts`) is a Fastify `rewriteUrl`, so it rewrites
 * `req.url` before routing and this hook only ever sees `/v1/...`. That is
 * asserted directly in `test/unit/claude-edge.spec.ts` rather than assumed.
 * Bare `/api` is *not* rewritten, so it is matched explicitly.
 */
const ENVELOPE_PREFIXES = ['/v1', '/api', '/apis'];

/** Install response-only compatibility at the public API edge. */
export function registerClaudePublicEdge(app: FastifyInstance): void {
  // This adapter is entirely synchronous. Use Fastify's callback hook form so
  // handlers that call `reply.send()` without returning it cannot race a
  // pending async onSend promise and attempt a second response.
  app.addHook('onSend', (req, reply, payload, done) => {
    if (!isClaudeEnvelopePath(req.url) || reply.statusCode < 400) {
      done(null, payload);
      return;
    }

    const body = errorEnvelope(reply.statusCode, req.id, payload);
    reply.type('application/json; charset=utf-8');
    reply.removeHeader('content-length');
    done(null, JSON.stringify(body));
  });
}

/**
 * Match on whole path segments, so `/apis` is the group tree rather than a
 * suffix of `/api`, and `/v1x` is neither.
 */
export function isClaudeEnvelopePath(url: string): boolean {
  const path = url.split('?')[0] ?? '';
  return ENVELOPE_PREFIXES.some((prefix) => path === prefix || path.startsWith(`${prefix}/`));
}

export function errorTypeForStatus(statusCode: number): ClaudeErrorType {
  switch (statusCode) {
    case 401:
      return 'authentication_error';
    case 402:
      return 'billing_error';
    case 403:
      return 'permission_error';
    case 404:
      return 'not_found_error';
    case 409:
      return 'conflict_error';
    case 413:
      return 'request_too_large';
    case 408:
    case 504:
      return 'timeout_error';
    case 429:
      return 'rate_limit_error';
    case 503:
    case 529:
      return 'overloaded_error';
    case 500:
    case 502:
      return 'api_error';
    default:
      return statusCode >= 500 ? 'api_error' : 'invalid_request_error';
  }
}

export function errorEnvelope(
  statusCode: number,
  requestId: string | null,
  payload: unknown,
): ErrorEnvelope {
  const parsed = parsePayload(payload);
  const canonical = canonicalError(parsed);
  return {
    type: 'error',
    error: canonical ?? {
      type: errorTypeForStatus(statusCode),
      message: errorMessage(statusCode, parsed),
    },
    request_id: requestId,
  };
}

function parsePayload(payload: unknown): unknown {
  if (Buffer.isBuffer(payload)) return parseJson(payload.toString('utf8'));
  if (typeof payload === 'string') return parseJson(payload);
  return payload;
}

function parseJson(value: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

function canonicalError(value: unknown): ErrorEnvelope['error'] | null {
  if (!isRecord(value) || value.type !== 'error' || !isRecord(value.error)) return null;
  if (typeof value.error.type !== 'string' || typeof value.error.message !== 'string') return null;
  return { ...value.error, type: value.error.type, message: value.error.message };
}

function errorMessage(statusCode: number, value: unknown): string {
  if (isRecord(value)) {
    if (statusCode >= 500 && typeof value.statusCode === 'number') {
      return 'Internal server error';
    }
    if (isRecord(value.error) && typeof value.error.message === 'string') {
      return value.error.message;
    }
    // Do not expose the original exception message from Fastify's generated
    // 5xx payload. Explicit route errors use `{error: string}` above.
    if (statusCode < 500 && typeof value.message === 'string' && value.message.length > 0) {
      return value.message;
    }
    if (typeof value.error === 'string' && value.error.length > 0) return value.error;
  }
  if (typeof value === 'string' && value.length > 0 && statusCode < 500) return value;
  return statusCode >= 500 ? 'Internal server error' : `Request failed with status ${statusCode}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
