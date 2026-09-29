// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import {
  hasAgentObservabilityControlCharacter,
  isAgentObservabilityBasicUsername,
  isAgentObservabilitySecretValue,
  normalizeAgentObservabilityCustomHeaders,
  normalizeAgentObservabilityOtlpHttpCredentialInput,
  type AgentObservabilityOtlpHttpCredentials,
} from './agent-observability-validation.js';
import { newId } from './versioning.js';

export {
  AGENT_OBSERVABILITY_CUSTOM_HEADER_MAX_COUNT,
  AGENT_OBSERVABILITY_CUSTOM_HEADER_NAME_MAX_LENGTH,
  AGENT_OBSERVABILITY_CUSTOM_HEADER_TOTAL_MAX_LENGTH,
  AGENT_OBSERVABILITY_CUSTOM_HEADER_VALUE_MAX_LENGTH,
} from './agent-observability-validation.js';
export type { AgentObservabilityOtlpHttpCredentials } from './agent-observability-validation.js';

export const AGENT_OBSERVABILITY_SECRET_BUNDLE_VERSION = 1;
export const AGENT_OBSERVABILITY_SECRET_BUNDLE_MAX_BYTES = 64 * 1024;

const BINDING_ID_MAX_LENGTH = 512;
const SECRET_REFERENCE_PREFIX = 'local:agent_observability/';
const SECRET_REFERENCE_PATTERN = /^local:agent_observability\/obssec_[0-9A-HJ-NP-TV-Z]{20}$/u;

declare const agentObservabilitySecretReferenceBrand: unique symbol;

/** Opaque, Registry-minted SecretStore reference; never caller input. */
export type AgentObservabilitySecretReference = string & {
  readonly [agentObservabilitySecretReferenceBrand]: true;
};

/** Non-secret identity bound into every opaque SecretStore bundle. */
export interface AgentObservabilityCredentialBundleIdentity {
  bindingId: string;
  credentialVersion: number;
}

export interface OtlpHttpBasicSecretBundle extends AgentObservabilityCredentialBundleIdentity {
  adapterType: 'otlp_http';
  auth: {
    type: 'basic';
    username: string;
    password: string;
  };
}

export interface OtlpHttpBearerSecretBundle extends AgentObservabilityCredentialBundleIdentity {
  adapterType: 'otlp_http';
  auth: {
    type: 'bearer';
    token: string;
  };
}

export interface OtlpHttpCustomHeadersSecretBundle extends AgentObservabilityCredentialBundleIdentity {
  adapterType: 'otlp_http';
  auth: {
    type: 'custom_headers';
    headers: Readonly<Record<string, string>>;
  };
}

export interface LangfuseSdkSecretBundle extends AgentObservabilityCredentialBundleIdentity {
  adapterType: 'langfuse_sdk';
  publicKey: string;
  secretKey: string;
}

export type AgentObservabilitySecretBundle =
  | OtlpHttpBasicSecretBundle
  | OtlpHttpBearerSecretBundle
  | OtlpHttpCustomHeadersSecretBundle
  | LangfuseSdkSecretBundle;

/** Invalid values never include credential material in their error message. */
export class AgentObservabilitySecretBundleError extends Error {
  override readonly name = 'AgentObservabilitySecretBundleError';

  constructor(message = 'invalid agent observability credential bundle') {
    super(message);
  }
}

/**
 * Generate the reference internally, before the durable staging-intent write.
 * Its random suffix has no tenant, binding, endpoint, or secret bytes.
 */
export function newAgentObservabilitySecretReference(): AgentObservabilitySecretReference {
  return `${SECRET_REFERENCE_PREFIX}${newId('obssec')}` as AgentObservabilitySecretReference;
}

export function isAgentObservabilitySecretReference(
  value: unknown,
): value is AgentObservabilitySecretReference {
  return typeof value === 'string' && SECRET_REFERENCE_PATTERN.test(value);
}

/** Validate and canonicalize an OTLP credential shape before it is hashed or staged. */
export function normalizeAgentObservabilityOtlpHttpCredentials(
  value: AgentObservabilityOtlpHttpCredentials,
): AgentObservabilityOtlpHttpCredentials {
  const normalized = normalizeAgentObservabilityOtlpHttpCredentialInput(value);
  if (normalized === null) throw new AgentObservabilitySecretBundleError();
  return normalized;
}

/**
 * Encode a credential generation into exactly one canonical JSON payload.
 * Endpoint and Authorization header construction belong to the adapter, not
 * this bundle, so neither can be supplied or persisted here.
 */
export function encodeAgentObservabilitySecretBundle(
  bundle: AgentObservabilitySecretBundle,
): string {
  const normalized = normalizeBundle(bundle);
  const encoded = canonicalAgentObservabilityJson(bundleWire(normalized));
  if (Buffer.byteLength(encoded, 'utf8') > AGENT_OBSERVABILITY_SECRET_BUNDLE_MAX_BYTES) {
    throw new AgentObservabilitySecretBundleError();
  }
  return encoded;
}

/**
 * Decode only the exact current canonical wire form. Re-encoding equality
 * rejects unknown fields, duplicate JSON keys, alternate ordering, and future
 * schema versions instead of quietly accepting a lossy representation.
 */
export function decodeAgentObservabilitySecretBundle(
  value: string,
  expectedIdentity: AgentObservabilityCredentialBundleIdentity,
): AgentObservabilitySecretBundle {
  if (
    typeof value !== 'string' ||
    Buffer.byteLength(value, 'utf8') > AGENT_OBSERVABILITY_SECRET_BUNDLE_MAX_BYTES
  ) {
    throw new AgentObservabilitySecretBundleError();
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new AgentObservabilitySecretBundleError();
  }

  const expected = normalizeBundleIdentity(expectedIdentity);
  const bundle = bundleFromWire(parsed);
  if (
    bundle.bindingId !== expected.bindingId ||
    bundle.credentialVersion !== expected.credentialVersion
  ) {
    throw new AgentObservabilitySecretBundleError();
  }
  if (encodeAgentObservabilitySecretBundle(bundle) !== value) {
    throw new AgentObservabilitySecretBundleError(
      'agent observability credential bundle must use canonical JSON',
    );
  }
  return bundle;
}

/** Canonical JSON used by secret bundles and non-secret idempotency body hashes. */
export function canonicalAgentObservabilityJson(value: unknown): string {
  return JSON.stringify(canonicalJsonValue(value, new Set<object>()));
}

function normalizeBundle(bundle: AgentObservabilitySecretBundle): AgentObservabilitySecretBundle {
  if (!isPlainObject(bundle) || typeof bundle.adapterType !== 'string') {
    throw new AgentObservabilitySecretBundleError();
  }

  const identity = normalizeBundleIdentity(bundle);
  if (bundle.adapterType === 'otlp_http') {
    if (!isPlainObject(bundle.auth) || typeof bundle.auth.type !== 'string') {
      throw new AgentObservabilitySecretBundleError();
    }
    if (bundle.auth.type === 'basic') {
      requireExactKeys(bundle, ['adapterType', 'auth', 'bindingId', 'credentialVersion']);
      requireExactKeys(bundle.auth, ['password', 'type', 'username']);
      return {
        ...identity,
        adapterType: 'otlp_http',
        auth: {
          type: 'basic',
          username: validateBasicUsername(bundle.auth.username),
          password: validateSecretValue(bundle.auth.password),
        },
      };
    }
    if (bundle.auth.type === 'bearer') {
      requireExactKeys(bundle, ['adapterType', 'auth', 'bindingId', 'credentialVersion']);
      requireExactKeys(bundle.auth, ['token', 'type']);
      return {
        ...identity,
        adapterType: 'otlp_http',
        auth: { type: 'bearer', token: validateSecretValue(bundle.auth.token) },
      };
    }
    if (bundle.auth.type === 'custom_headers') {
      requireExactKeys(bundle, ['adapterType', 'auth', 'bindingId', 'credentialVersion']);
      requireExactKeys(bundle.auth, ['headers', 'type']);
      return {
        ...identity,
        adapterType: 'otlp_http',
        auth: { type: 'custom_headers', headers: normalizeCustomHeaders(bundle.auth.headers) },
      };
    }
    throw new AgentObservabilitySecretBundleError();
  }

  if (bundle.adapterType === 'langfuse_sdk') {
    requireExactKeys(bundle, [
      'adapterType',
      'bindingId',
      'credentialVersion',
      'publicKey',
      'secretKey',
    ]);
    return {
      ...identity,
      adapterType: 'langfuse_sdk',
      publicKey: validateSecretValue(bundle.publicKey),
      secretKey: validateSecretValue(bundle.secretKey),
    };
  }

  throw new AgentObservabilitySecretBundleError();
}

function bundleWire(bundle: AgentObservabilitySecretBundle): Record<string, unknown> {
  const identity = {
    binding_id: bundle.bindingId,
    credential_version: bundle.credentialVersion,
  };
  if (bundle.adapterType === 'langfuse_sdk') {
    return {
      ...identity,
      adapter_type: 'langfuse_sdk',
      public_key: bundle.publicKey,
      secret_key: bundle.secretKey,
      version: AGENT_OBSERVABILITY_SECRET_BUNDLE_VERSION,
    };
  }
  if (bundle.auth.type === 'basic') {
    return {
      ...identity,
      adapter_type: 'otlp_http',
      auth: { password: bundle.auth.password, type: 'basic', username: bundle.auth.username },
      version: AGENT_OBSERVABILITY_SECRET_BUNDLE_VERSION,
    };
  }
  if (bundle.auth.type === 'bearer') {
    return {
      ...identity,
      adapter_type: 'otlp_http',
      auth: { token: bundle.auth.token, type: 'bearer' },
      version: AGENT_OBSERVABILITY_SECRET_BUNDLE_VERSION,
    };
  }
  return {
    ...identity,
    adapter_type: 'otlp_http',
    auth: { headers: bundle.auth.headers, type: 'custom_headers' },
    version: AGENT_OBSERVABILITY_SECRET_BUNDLE_VERSION,
  };
}

function bundleFromWire(value: unknown): AgentObservabilitySecretBundle {
  if (!isPlainObject(value)) throw new AgentObservabilitySecretBundleError();
  const identity = bundleIdentityFromWire(value);
  if (value.adapter_type === 'otlp_http') {
    requireExactKeys(value, [
      'adapter_type',
      'auth',
      'binding_id',
      'credential_version',
      'version',
    ]);
    requireBundleVersion(value.version);
    if (!isPlainObject(value.auth) || typeof value.auth.type !== 'string') {
      throw new AgentObservabilitySecretBundleError();
    }
    if (value.auth.type === 'basic') {
      requireExactKeys(value.auth, ['password', 'type', 'username']);
      return normalizeBundle({
        ...identity,
        adapterType: 'otlp_http',
        auth: {
          type: 'basic',
          username: value.auth.username as string,
          password: value.auth.password as string,
        },
      });
    }
    if (value.auth.type === 'bearer') {
      requireExactKeys(value.auth, ['token', 'type']);
      return normalizeBundle({
        ...identity,
        adapterType: 'otlp_http',
        auth: { type: 'bearer', token: value.auth.token as string },
      });
    }
    if (value.auth.type === 'custom_headers') {
      requireExactKeys(value.auth, ['headers', 'type']);
      return normalizeBundle({
        ...identity,
        adapterType: 'otlp_http',
        auth: {
          type: 'custom_headers',
          headers: value.auth.headers as Record<string, string>,
        },
      });
    }
    throw new AgentObservabilitySecretBundleError();
  }
  if (value.adapter_type === 'langfuse_sdk') {
    requireExactKeys(value, [
      'adapter_type',
      'binding_id',
      'credential_version',
      'public_key',
      'secret_key',
      'version',
    ]);
    requireBundleVersion(value.version);
    return normalizeBundle({
      ...identity,
      adapterType: 'langfuse_sdk',
      publicKey: value.public_key as string,
      secretKey: value.secret_key as string,
    });
  }
  throw new AgentObservabilitySecretBundleError();
}

function normalizeBundleIdentity(
  value: AgentObservabilityCredentialBundleIdentity,
): AgentObservabilityCredentialBundleIdentity {
  if (!isPlainObject(value)) throw new AgentObservabilitySecretBundleError();
  if (
    typeof value.bindingId !== 'string' ||
    value.bindingId.length === 0 ||
    value.bindingId.length > BINDING_ID_MAX_LENGTH ||
    value.bindingId !== value.bindingId.trim() ||
    hasAgentObservabilityControlCharacter(value.bindingId) ||
    typeof value.credentialVersion !== 'number' ||
    !Number.isSafeInteger(value.credentialVersion) ||
    value.credentialVersion <= 0
  ) {
    throw new AgentObservabilitySecretBundleError();
  }
  return { bindingId: value.bindingId, credentialVersion: value.credentialVersion };
}

function bundleIdentityFromWire(
  value: Record<string, unknown>,
): AgentObservabilityCredentialBundleIdentity {
  return normalizeBundleIdentity({
    bindingId: value.binding_id as string,
    credentialVersion: value.credential_version as number,
  });
}

function requireBundleVersion(value: unknown): void {
  if (value !== AGENT_OBSERVABILITY_SECRET_BUNDLE_VERSION) {
    throw new AgentObservabilitySecretBundleError();
  }
}

function normalizeCustomHeaders(value: unknown): Record<string, string> {
  const normalized = normalizeAgentObservabilityCustomHeaders(value);
  if (normalized === null) throw new AgentObservabilitySecretBundleError();
  return normalized;
}

function validateSecretValue(value: unknown): string {
  if (!isAgentObservabilitySecretValue(value)) throw new AgentObservabilitySecretBundleError();
  return value;
}

function validateBasicUsername(value: unknown): string {
  if (!isAgentObservabilityBasicUsername(value)) throw new AgentObservabilitySecretBundleError();
  return value;
}

function requireExactKeys(value: Record<string, unknown>, expected: readonly string[]): void {
  const keys = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  if (
    keys.length !== sortedExpected.length ||
    keys.some((key, index) => key !== sortedExpected[index])
  ) {
    throw new AgentObservabilitySecretBundleError();
  }
}

function canonicalJsonValue(value: unknown, ancestors: Set<object>): unknown {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value))
      throw new AgentObservabilitySecretBundleError('invalid canonical JSON');
    return value;
  }
  if (Array.isArray(value)) {
    if (ancestors.has(value))
      throw new AgentObservabilitySecretBundleError('invalid canonical JSON');
    ancestors.add(value);
    try {
      return value.map((item) => canonicalJsonValue(item, ancestors));
    } finally {
      ancestors.delete(value);
    }
  }
  if (!isPlainObject(value))
    throw new AgentObservabilitySecretBundleError('invalid canonical JSON');
  if (ancestors.has(value)) throw new AgentObservabilitySecretBundleError('invalid canonical JSON');
  ancestors.add(value);
  try {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonicalJsonValue(value[key], ancestors)]),
    );
  } finally {
    ancestors.delete(value);
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
