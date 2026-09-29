// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export const AGENT_OBSERVABILITY_ENDPOINT_MAX_LENGTH = 4096;
export const AGENT_OBSERVABILITY_TIMEOUT_MIN_MS = 1;
export const AGENT_OBSERVABILITY_TIMEOUT_MAX_MS = 120_000;
export const AGENT_OBSERVABILITY_KEY_HINT_MAX_LENGTH = 128;
export const AGENT_OBSERVABILITY_EXTERNAL_PROJECT_ID_MAX_LENGTH = 512;
export const AGENT_OBSERVABILITY_IDEMPOTENCY_KEY_MAX_LENGTH = 255;
export const AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH = 16 * 1024;
export const AGENT_OBSERVABILITY_CUSTOM_HEADER_MAX_COUNT = 32;
export const AGENT_OBSERVABILITY_CUSTOM_HEADER_NAME_MAX_LENGTH = 128;
export const AGENT_OBSERVABILITY_CUSTOM_HEADER_VALUE_MAX_LENGTH = 4096;
export const AGENT_OBSERVABILITY_CUSTOM_HEADER_TOTAL_MAX_LENGTH = 16 * 1024;

const ABSOLUTE_AUTHORITY = /^([A-Za-z][A-Za-z0-9+.-]*):\/\/([^/?#]*)/u;
const PRINTABLE_ASCII = /^[\x20-\x7E]+$/u;
const OTLP_HEADER_NAME_PATTERN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/u;
const FORBIDDEN_OTLP_HEADER_NAMES = new Set([
  'authorization',
  'connection',
  'content-encoding',
  'content-length',
  'content-type',
  'expect',
  'host',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'traceparent',
  'tracestate',
  'baggage',
  'trailer',
  'transfer-encoding',
  'upgrade',
]);

/** Write-only OTLP credential input after validation and canonicalization. */
export type AgentObservabilityOtlpHttpCredentials =
  | { type: 'basic'; username: string; password: string }
  | { type: 'bearer'; token: string }
  | { type: 'custom_headers'; headers: Readonly<Record<string, string>> };

/**
 * Parse only a safe, absolute HTTP(S) endpoint identity and return its single
 * canonical serialized form. This parser performs no DNS or network I/O.
 */
export function normalizeAgentObservabilityEndpoint(value: unknown): string | null {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > AGENT_OBSERVABILITY_ENDPOINT_MAX_LENGTH ||
    value !== value.trim() ||
    value.includes('?') ||
    value.includes('#')
  ) {
    return null;
  }

  // URL normalizes empty userinfo (`https://@host`) away, so reject every raw
  // authority `@` before parsing. This also rejects `https://:@host`.
  const authority = ABSOLUTE_AUTHORITY.exec(value);
  if (!authority || authority[2]?.length === 0 || authority[2]?.includes('@')) return null;

  try {
    const endpoint = new URL(value);
    if (
      (endpoint.protocol !== 'http:' && endpoint.protocol !== 'https:') ||
      endpoint.hostname.length === 0 ||
      endpoint.username.length !== 0 ||
      endpoint.password.length !== 0 ||
      endpoint.search.length !== 0 ||
      endpoint.hash.length !== 0
    ) {
      return null;
    }
    const path = endpoint.pathname.replace(/\/+$/u, '');
    return `${endpoint.protocol}//${endpoint.host}${path}`;
  } catch {
    return null;
  }
}

export function isCanonicalAgentObservabilityEndpoint(value: unknown): value is string {
  const normalized = normalizeAgentObservabilityEndpoint(value);
  return normalized !== null && normalized === value;
}

export function isAgentObservabilityTimeoutMs(value: unknown): value is number {
  return (
    typeof value === 'number' &&
    Number.isSafeInteger(value) &&
    value >= AGENT_OBSERVABILITY_TIMEOUT_MIN_MS &&
    value <= AGENT_OBSERVABILITY_TIMEOUT_MAX_MS
  );
}

/** Non-secret display suffix only; never accept whitespace or control characters. */
export function isAgentObservabilityKeyHint(value: unknown): value is string | null {
  return (
    value === null ||
    (typeof value === 'string' &&
      value.length > 0 &&
      value.length <= AGENT_OBSERVABILITY_KEY_HINT_MAX_LENGTH &&
      value === value.trim() &&
      PRINTABLE_ASCII.test(value))
  );
}

/** Public target identity, bounded before it reaches a response or ETag. */
export function isAgentObservabilityExternalProjectId(value: unknown): value is string | null {
  return (
    value === null ||
    (typeof value === 'string' &&
      value.length > 0 &&
      value.length <= AGENT_OBSERVABILITY_EXTERNAL_PROJECT_ID_MAX_LENGTH &&
      value === value.trim() &&
      !hasAgentObservabilityControlCharacter(value))
  );
}

/** ASCII C0 controls and DEL are unsafe in HTTP headers and opaque identifiers. */
export function hasAgentObservabilityControlCharacter(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x1f || code === 0x7f) return true;
  }
  return false;
}

/** Reject strings that cannot be encoded as Unicode scalar values without replacement. */
export function hasAgentObservabilityUnpairedUtf16Surrogate(value: string): boolean {
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

/** Trim and bound an opaque idempotency identity before it reaches storage. */
export function normalizeAgentObservabilityIdempotencyKey(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const normalized = value.trim();
  if (
    normalized.length === 0 ||
    normalized.length > AGENT_OBSERVABILITY_IDEMPOTENCY_KEY_MAX_LENGTH ||
    hasAgentObservabilityControlCharacter(normalized)
  ) {
    return null;
  }
  return normalized;
}

/** Exactly one strong entity tag. Weak tags, lists, wildcards, and spaces are invalid. */
export function isSingleStrongEntityTag(value: unknown): value is string {
  if (typeof value !== 'string' || value.length < 2 || value[0] !== '"' || value.at(-1) !== '"') {
    return false;
  }
  for (let index = 1; index < value.length - 1; index += 1) {
    const code = value.charCodeAt(index);
    if (code < 0x21 || code === 0x22 || code > 0x7e) return false;
  }
  return true;
}

/** Non-empty secret material shared by Basic and bearer OTLP credentials. */
export function isAgentObservabilitySecretValue(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value.length <= AGENT_OBSERVABILITY_SECRET_VALUE_MAX_LENGTH &&
    !hasAgentObservabilityControlCharacter(value) &&
    !hasAgentObservabilityUnpairedUtf16Surrogate(value)
  );
}

/** HTTP Basic uses the first colon as its username/password delimiter. */
export function isAgentObservabilityBasicUsername(value: unknown): value is string {
  return isAgentObservabilitySecretValue(value) && !value.includes(':');
}

export function isAgentObservabilityCustomHeaderValue(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length <= AGENT_OBSERVABILITY_CUSTOM_HEADER_VALUE_MAX_LENGTH &&
    !hasAgentObservabilityControlCharacter(value) &&
    !hasAgentObservabilityUnpairedUtf16Surrogate(value)
  );
}

/**
 * Validate custom HTTP headers and return their canonical lowercase, sorted
 * representation. The caller retains the original input only until this
 * normalizer is applied before hashing or persistence.
 */
export function normalizeAgentObservabilityCustomHeaders(
  value: unknown,
): Record<string, string> | null {
  if (!isPlainObject(value)) return null;
  const entries = Object.entries(value);
  if (entries.length === 0 || entries.length > AGENT_OBSERVABILITY_CUSTOM_HEADER_MAX_COUNT) {
    return null;
  }

  const normalized = new Map<string, string>();
  let totalLength = 0;
  for (const [rawName, rawValue] of entries) {
    const name = rawName.toLowerCase();
    if (
      rawName.length === 0 ||
      rawName.length > AGENT_OBSERVABILITY_CUSTOM_HEADER_NAME_MAX_LENGTH ||
      !OTLP_HEADER_NAME_PATTERN.test(rawName) ||
      FORBIDDEN_OTLP_HEADER_NAMES.has(name) ||
      normalized.has(name) ||
      !isAgentObservabilityCustomHeaderValue(rawValue)
    ) {
      return null;
    }
    totalLength += rawName.length + rawValue.length;
    if (totalLength > AGENT_OBSERVABILITY_CUSTOM_HEADER_TOTAL_MAX_LENGTH) return null;
    normalized.set(name, rawValue);
  }
  return Object.fromEntries(
    [...normalized.entries()].sort(([left], [right]) => left.localeCompare(right)),
  );
}

/**
 * Validate and canonicalize an OTLP credential shape before body hashing,
 * staging, or persistence. It has no SecretStore dependency so contracts and
 * domain code share one accepted input set.
 */
export function normalizeAgentObservabilityOtlpHttpCredentialInput(
  value: unknown,
): AgentObservabilityOtlpHttpCredentials | null {
  if (!isPlainObject(value) || typeof value.type !== 'string') return null;
  if (value.type === 'basic') {
    if (
      !hasExactKeys(value, ['password', 'type', 'username']) ||
      !isAgentObservabilityBasicUsername(value.username) ||
      !isAgentObservabilitySecretValue(value.password)
    ) {
      return null;
    }
    return { type: 'basic', username: value.username, password: value.password };
  }
  if (value.type === 'bearer') {
    if (!hasExactKeys(value, ['token', 'type']) || !isAgentObservabilitySecretValue(value.token)) {
      return null;
    }
    return { type: 'bearer', token: value.token };
  }
  if (value.type === 'custom_headers') {
    if (!hasExactKeys(value, ['headers', 'type'])) return null;
    const headers = normalizeAgentObservabilityCustomHeaders(value.headers);
    return headers === null ? null : { type: 'custom_headers', headers };
  }
  return null;
}

export function isAgentObservabilityOtlpHttpCredentials(
  value: unknown,
): value is AgentObservabilityOtlpHttpCredentials {
  return normalizeAgentObservabilityOtlpHttpCredentialInput(value) !== null;
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const keys = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return (
    keys.length === sortedExpected.length &&
    keys.every((key, index) => key === sortedExpected[index])
  );
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
