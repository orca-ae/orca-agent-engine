// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { parseRetryAfterMs } from './delivery-retry.js';
import type { OtlpExportRequest } from './otlp-json.js';
import type { OtlpDeliveryOutcome } from './types.js';

const LITEFUSE_TRACES_PATH = '/api/public/otel/v1/traces';
const DEFAULT_TIMEOUT_MS = 10_000;
const MAX_ENDPOINT_LENGTH = 4_096;
const MAX_SECRET_LENGTH = 16 * 1024;
const MAX_TIMEOUT_MS = 120_000;
const MAX_RESPONSE_BYTES = 64 * 1024;
const SIGNED_INT64_MAX = '9223372036854775807';

export interface LitefuseOtlpHttpClientOptions {
  endpoint: string;
  publicKey: string;
  secretKey: string;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
}

/** Non-2xx response. Body and raw headers are never retained. */
export class OtlpHttpStatusError extends Error {
  constructor(
    readonly status: number,
    readonly retryAfterMs: number | undefined = undefined,
  ) {
    super(`Litefuse OTLP endpoint returned HTTP ${status}`);
    this.name = 'OtlpHttpStatusError';
  }
}

/** An invalid, malformed, or oversized HTTP 200 response. */
export class OtlpHttpResponseError extends Error {
  constructor() {
    super('Litefuse OTLP endpoint returned an invalid response');
    this.name = 'OtlpHttpResponseError';
  }
}

/** HTTP 200 accepted only part of the submitted batch; rejectedSpans is canonical int64 decimal. */
export class OtlpPartialSuccessError extends Error {
  constructor(
    readonly rejectedSpans: string,
    readonly messageBytes: number,
    readonly messageSha256: string,
  ) {
    super(`Litefuse OTLP endpoint rejected ${rejectedSpans} spans`);
    this.name = 'OtlpPartialSuccessError';
  }
}

/** Network failure, timeout, or caller cancellation before a complete response. */
export class OtlpHttpTransportError extends Error {
  constructor(readonly kind: 'cancelled' | 'timeout' | 'transport') {
    super(`Litefuse OTLP transport failed (${kind})`);
    this.name = 'OtlpHttpTransportError';
  }
}

/**
 * Fixed-endpoint, fixed-credential Litefuse OTLP HTTP/JSON client.
 *
 * This slice intentionally has no retry, batching, persistence, or routing.
 */
export class LitefuseOtlpHttpClient {
  private readonly endpoint: string;
  private readonly authorization: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;

  constructor(options: LitefuseOtlpHttpClientOptions) {
    this.endpoint = validateTracesEndpoint(options.endpoint);
    validateBasicUsername(options.publicKey, 'publicKey');
    validateCredential(options.secretKey, 'secretKey');
    this.authorization = `Basic ${Buffer.from(`${options.publicKey}:${options.secretKey}`).toString('base64')}`;
    this.timeoutMs = validateTimeout(options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  async send(
    payload: OtlpExportRequest,
    signal?: AbortSignal,
  ): Promise<Exclude<OtlpDeliveryOutcome, { kind: 'partial_rejection' }>> {
    const timeout = AbortSignal.timeout(this.timeoutMs);
    const requestSignal = signal === undefined ? timeout : AbortSignal.any([signal, timeout]);
    let response: Response;
    try {
      response = await this.fetchImpl(this.endpoint, {
        method: 'POST',
        headers: {
          accept: 'application/json',
          authorization: this.authorization,
          'content-type': 'application/json',
          'x-langfuse-ingestion-version': '4',
        },
        body: JSON.stringify(payload),
        signal: requestSignal,
        redirect: 'manual',
      });
    } catch (error) {
      if (error instanceof Error && error.name === 'OtlpEgressPolicyError') throw error;
      throw transportError(signal, timeout);
    }
    if (response.status !== 200) {
      const retryAfterMs = parseRetryAfterMs(response.headers.get('retry-after'));
      await response.body?.cancel().catch(() => undefined);
      throw new OtlpHttpStatusError(response.status, retryAfterMs);
    }
    if (!isJsonContentType(response.headers.get('content-type'))) {
      await response.body?.cancel().catch(() => undefined);
      throw new OtlpHttpResponseError();
    }
    const responseBody = await readBoundedResponseBody(response, signal, timeout);
    return validateOtlpResponse(responseBody);
  }
}

function validateTracesEndpoint(raw: string): string {
  if (
    raw.length === 0 ||
    raw.length > MAX_ENDPOINT_LENGTH ||
    raw !== raw.trim() ||
    raw.includes('?') ||
    raw.includes('#')
  ) {
    throw new Error('Litefuse OTLP endpoint must be a canonical HTTP(S) URL');
  }
  let endpoint: URL;
  try {
    endpoint = new URL(raw);
  } catch {
    throw new Error('Litefuse OTLP endpoint must be an absolute HTTP(S) URL');
  }
  if (endpoint.protocol !== 'http:' && endpoint.protocol !== 'https:') {
    throw new Error('Litefuse OTLP endpoint must use HTTP(S)');
  }
  if (endpoint.username || endpoint.password) {
    throw new Error('Litefuse OTLP endpoint must not contain credentials');
  }
  if (endpoint.pathname !== LITEFUSE_TRACES_PATH) {
    throw new Error(`Litefuse OTLP endpoint must be exact ${LITEFUSE_TRACES_PATH}`);
  }
  const canonical = `${endpoint.protocol}//${endpoint.host}${endpoint.pathname.replace(/\/+$/u, '')}`;
  if (canonical !== raw) {
    throw new Error('Litefuse OTLP endpoint must be canonical');
  }
  return canonical;
}

function validateCredential(value: string, name: string): void {
  if (value.length === 0 || value.length > MAX_SECRET_LENGTH || hasControlCharacter(value)) {
    throw new Error(`${name} must be non-empty and contain no control characters`);
  }
}

function validateBasicUsername(value: string, name: string): void {
  validateCredential(value, name);
  if (value.includes(':')) throw new Error(`${name} must not contain a colon`);
}

function hasControlCharacter(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x1f || code === 0x7f) return true;
  }
  return false;
}

function validateTimeout(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0 || value > MAX_TIMEOUT_MS) {
    throw new Error(`timeoutMs must be an integer between 1 and ${MAX_TIMEOUT_MS}`);
  }
  return value;
}

function isJsonContentType(contentType: string | null): boolean {
  const mediaType = contentType?.split(';', 1)[0]?.trim().toLowerCase();
  return mediaType === 'application/json';
}

async function readBoundedResponseBody(
  response: Response,
  callerSignal: AbortSignal | undefined,
  timeoutSignal: AbortSignal,
): Promise<string> {
  if (response.body === null) return '';
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > MAX_RESPONSE_BYTES) {
        await reader.cancel().catch(() => undefined);
        throw new OtlpHttpResponseError();
      }
      chunks.push(next.value);
    }
  } catch (error) {
    if (error instanceof OtlpHttpResponseError) throw error;
    throw transportError(callerSignal, timeoutSignal);
  }
  const bytes = Buffer.concat(
    chunks.map((chunk) => Buffer.from(chunk)),
    size,
  );
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  } catch {
    throw new OtlpHttpResponseError();
  }
}

function validateOtlpResponse(
  body: string,
): Exclude<OtlpDeliveryOutcome, { kind: 'partial_rejection' }> {
  if (body.trim() === '') throw new OtlpHttpResponseError();
  let parsed: unknown;
  try {
    parsed = parseJsonPreservingRejectedSpans(body);
  } catch {
    throw new OtlpHttpResponseError();
  }
  if (!isRecord(parsed)) throw new OtlpHttpResponseError();
  const partial = readOwnField(parsed, 'partialSuccess');
  if (partial === undefined || partial === null) return { kind: 'accepted' };
  if (!isRecord(partial)) throw new OtlpHttpResponseError();
  const rejectedSpansValue = readOwnField(partial, 'rejectedSpans');
  const warning = readOwnField(partial, 'errorMessage');
  const rejectedSpans =
    rejectedSpansValue === undefined || rejectedSpansValue === null
      ? '0'
      : parseNonNegativeInt64(rejectedSpansValue);
  if (rejectedSpans === undefined) throw new OtlpHttpResponseError();
  if (warning !== undefined && warning !== null && typeof warning !== 'string') {
    throw new OtlpHttpResponseError();
  }
  const warningValue = warning ?? '';
  if (hasUnpairedUtf16Surrogate(warningValue)) throw new OtlpHttpResponseError();
  if (rejectedSpans !== '0') {
    throw new OtlpPartialSuccessError(
      rejectedSpans,
      Buffer.byteLength(warningValue, 'utf8'),
      createHash('sha256').update(warningValue).digest('hex'),
    );
  }
  if (warningValue !== '') {
    return {
      kind: 'accepted_with_warning',
      rejectedSpans,
      messageBytes: Buffer.byteLength(warningValue, 'utf8'),
      messageSha256: createHash('sha256').update(warningValue).digest('hex'),
    };
  }
  return { kind: 'accepted' };
}

class JsonNumberLiteral {
  constructor(readonly source: string) {}
}

function parseJsonPreservingRejectedSpans(body: string): unknown {
  // Node 22 exposes each primitive's original token before JSON numbers lose
  // int64 precision through IEEE-754 conversion.
  const parseWithSource = JSON.parse as unknown as (
    text: string,
    reviver: (key: string, value: unknown, context?: { readonly source?: string }) => unknown,
  ) => unknown;
  return parseWithSource(body, (key, value, context) => {
    if (key === 'rejectedSpans' && typeof value === 'number') {
      if (context?.source === undefined) throw new Error('JSON number source is unavailable');
      return new JsonNumberLiteral(context.source);
    }
    return value;
  });
}

function parseNonNegativeInt64(value: unknown): string | undefined {
  const source = value instanceof JsonNumberLiteral ? value.source : value;
  if (typeof source !== 'string') return undefined;
  const match = /^(-?)(0|[1-9][0-9]*)(?:\.([0-9]+))?(?:[eE]([+-]?[0-9]+))?$/u.exec(source);
  if (match === null) return undefined;

  const integerDigits = match[2]!;
  const fractionDigits = match[3] ?? '';
  const coefficient = `${integerDigits}${fractionDigits}`;
  const firstNonZero = coefficient.search(/[1-9]/u);
  if (firstNonZero === -1) return '0';
  if (match[1] === '-') return undefined;

  const exponent = Number(match[4] ?? '0');
  if (!Number.isSafeInteger(exponent)) return undefined;
  const decimalPoint = integerDigits.length + exponent;
  if (!Number.isSafeInteger(decimalPoint) || decimalPoint <= 0) return undefined;

  let canonical: string;
  if (decimalPoint < coefficient.length) {
    if (/[^0]/u.test(coefficient.slice(decimalPoint))) return undefined;
    canonical = coefficient.slice(0, decimalPoint).replace(/^0+/u, '');
  } else {
    const significant = coefficient.slice(firstNonZero);
    const appendedZeros = decimalPoint - coefficient.length;
    if (significant.length + appendedZeros > SIGNED_INT64_MAX.length) return undefined;
    canonical = `${significant}${'0'.repeat(appendedZeros)}`;
  }

  if (
    canonical.length > SIGNED_INT64_MAX.length ||
    (canonical.length === SIGNED_INT64_MAX.length && canonical > SIGNED_INT64_MAX)
  ) {
    return undefined;
  }
  return canonical;
}

function readOwnField(value: Record<string, unknown>, field: string): unknown {
  return Object.hasOwn(value, field) ? value[field] : undefined;
}

function hasUnpairedUtf16Surrogate(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return true;
      index += 1;
    } else if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      return true;
    }
  }
  return false;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function transportError(
  callerSignal: AbortSignal | undefined,
  timeoutSignal: AbortSignal,
): OtlpHttpTransportError {
  if (callerSignal?.aborted) return new OtlpHttpTransportError('cancelled');
  if (timeoutSignal.aborted) return new OtlpHttpTransportError('timeout');
  return new OtlpHttpTransportError('transport');
}
