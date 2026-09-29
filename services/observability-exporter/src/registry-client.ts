// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { PinnedDeliveryContext } from './types.js';
import { isSafeAttributionLabel } from './canonical-validation.js';

const WORKSPACE_ID_RE = /^[A-Za-z0-9_-]{1,128}$/u;
const SESSION_ID_RE = /^ses_[A-Za-z0-9_-]+$/u;
const SESSION_ID_MAX_LENGTH = 128;
const AUTHORIZATION_ID_RE = /^obsauth_[0-9A-HJ-NP-TV-Z]{20}$/u;
const SECRET_VALUE_MAX_LENGTH = 16 * 1024;
/**
 * One legal secret value has at most 16,384 UTF-16 code units. Once unpaired
 * surrogates are rejected, three-byte BMP scalars are the worst UTF-8 case per
 * code unit, so Basic/SDK's two values need at most 98,304 bytes. 128 KiB leaves
 * more than 32 KiB for the fixed resolver envelope; parity tests serialize that
 * maximum Basic response through Registry's actual contract.
 */
export const MAX_REGISTRY_RESPONSE_BYTES = 128 * 1024;

type AuthorityCaptureMode = 'metadata_only' | 'redacted_io' | 'raw_io';
const CAPTURE_MODES: ReadonlySet<AuthorityCaptureMode> = new Set([
  'metadata_only',
  'redacted_io',
  'raw_io',
]);
const SELECTION_SOURCES = new Set(['organization_default', 'workspace_custom', 'disabled']);
const BINDING_SCOPES = new Set(['organization', 'workspace']);
const BINDING_STATUSES = new Set(['active', 'draining', 'disabled', 'archived']);
const ADAPTER_TYPES = new Set(['otlp_http', 'langfuse_sdk']);
const ENDPOINT_KINDS = new Set(['traces_endpoint', 'base_endpoint']);
const ENDPOINT_CLASSES = new Set(['public', 'private']);
const SEMANTIC_PROFILES = new Set(['otel_genai', 'langfuse']);
const PROTOCOLS = new Set(['http/protobuf', 'http/json', 'sdk']);
const COMPRESSIONS = new Set(['none', 'gzip']);
const SUPPRESSION_REASONS = new Set([
  'session_archived',
  'session_deleted',
  'session_revoked',
  'organization_archived',
  'workspace_archived',
  'organization_default_revoked',
  'organization_revoked',
  'workspace_revoked',
  'binding_revoked',
  'binding_disabled',
  'binding_archived',
  'binding_configuration_invalid',
  'platform_adapter_disallowed',
  'platform_endpoint_class_disallowed',
  'credential_not_configured',
]);
const FORBIDDEN_CUSTOM_HEADER_NAMES = new Set([
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
const CONTEXT_KEYS = [
  'agent',
  'binding',
  'capture',
  'epochs',
  'harness',
  'harness_mode',
  'organization_id',
  'reason',
  'schema_version',
  'selection_source',
  'session_id',
  'status',
  'workspace_id',
] as const;
const EPOCH_KEYS = [
  'binding_revocation_epoch',
  'organization_capture_restriction_epoch',
  'organization_default_revocation_epoch',
  'organization_revocation_epoch',
  'organization_selection_epoch',
  'platform_capture_restriction_epoch',
  'session_revocation_epoch',
  'workspace_capture_restriction_epoch',
  'workspace_revocation_epoch',
  'workspace_selection_epoch',
] as const;

/** Token source is supplied by the workload bootstrap, never persisted here. */
export type InternalServiceTokenProvider = () => Promise<string>;

export interface RegistryEnabledObservabilityContext {
  status: 'enabled';
  deliveryContext: PinnedDeliveryContext;
}

export interface RegistrySuppressedObservabilityContext {
  status: 'disabled' | 'suppressed';
}

export type RegistryObservabilityContext =
  | RegistryEnabledObservabilityContext
  | RegistrySuppressedObservabilityContext;

export interface RegistryObservabilitySecret {
  bindingId: string;
  bindingVersion: number;
  effectiveCaptureMode: AuthorityCaptureMode;
  auth: { type: 'basic'; username: string; password: string } | { type: 'unsupported' };
}

/** Safe status-only failure. Registry response bodies never enter this error. */
export class RegistryResolverHttpError extends Error {
  constructor(
    readonly resolver: 'context' | 'secret',
    readonly status: number,
  ) {
    super(`Registry observability ${resolver} resolver returned HTTP ${status}`);
    this.name = 'RegistryResolverHttpError';
  }
}

/** Registry returned a 200 response outside its narrow internal contract. */
export class RegistryResolverResponseError extends Error {
  constructor(readonly resolver: 'context' | 'secret') {
    super(`Registry observability ${resolver} resolver returned an invalid response`);
    this.name = 'RegistryResolverResponseError';
  }
}

/** Local workspace/Session selector cannot be represented by Registry's path contract. */
export class RegistryResolverScopeError extends Error {
  constructor() {
    super('Registry observability resolver scope is invalid');
    this.name = 'RegistryResolverScopeError';
  }
}

/**
 * Small client for Registry's exporter-only internal resolver pair. Both
 * methods derive scope solely from the workspace/Session path and send a fresh
 * supplied bearer token on every request.
 */
export class RegistryObservabilityClient {
  private readonly internalBaseUrl: string;
  private readonly fetchImpl: typeof fetch;

  constructor(options: {
    internalBaseUrl: string;
    tokenProvider: InternalServiceTokenProvider;
    fetchImpl?: typeof fetch;
  }) {
    this.internalBaseUrl = validateInternalBaseUrl(options.internalBaseUrl);
    this.tokenProvider = options.tokenProvider;
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  private readonly tokenProvider: InternalServiceTokenProvider;

  async resolveContext(input: {
    workspaceId: string;
    sessionId: string;
    signal?: AbortSignal;
  }): Promise<RegistryObservabilityContext> {
    const raw = await this.resolve('context', input);
    return parseContext(raw, input);
  }

  async resolveSecret(input: {
    workspaceId: string;
    sessionId: string;
    signal?: AbortSignal;
  }): Promise<RegistryObservabilitySecret> {
    const raw = await this.resolve('secret', input);
    return parseSecret(raw);
  }

  private async resolve(
    resolver: 'context' | 'secret',
    input: { workspaceId: string; sessionId: string; signal?: AbortSignal },
  ): Promise<unknown> {
    const sessionBase = this.sessionBase(input);
    const token = await this.tokenProvider();
    if (typeof token !== 'string' || token.trim().length === 0 || /\s/.test(token)) {
      throw new Error('Registry observability token provider returned an invalid token');
    }
    let response: Response;
    try {
      response = await this.fetchImpl(`${sessionBase}/agent-observability/${resolver}/resolve`, {
        method: 'POST',
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${token}`,
          'content-type': 'application/json',
        },
        body: '{}',
        redirect: 'error',
        ...(input.signal === undefined ? {} : { signal: input.signal }),
      });
    } catch {
      throw new RegistryResolverHttpError(resolver, 503);
    }
    if (response.status !== 200) {
      await response.body?.cancel().catch(() => undefined);
      throw new RegistryResolverHttpError(resolver, response.status);
    }
    return readBoundedRegistryResponse(response, resolver);
  }

  private sessionBase(input: { workspaceId: string; sessionId: string }): string {
    if (!WORKSPACE_ID_RE.test(input.workspaceId)) {
      throw new RegistryResolverScopeError();
    }
    if (input.sessionId.length > SESSION_ID_MAX_LENGTH || !SESSION_ID_RE.test(input.sessionId)) {
      throw new RegistryResolverScopeError();
    }
    return (
      `${this.internalBaseUrl}/internal/v1/workspaces/${encodeURIComponent(input.workspaceId)}` +
      `/sessions/${encodeURIComponent(input.sessionId)}`
    );
  }
}

async function readBoundedRegistryResponse(
  response: Response,
  resolver: 'context' | 'secret',
): Promise<unknown> {
  if (response.body === null) throw new RegistryResolverResponseError(resolver);
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > MAX_REGISTRY_RESPONSE_BYTES) {
        await reader.cancel().catch(() => undefined);
        throw new RegistryResolverResponseError(resolver);
      }
      chunks.push(next.value);
    }
  } catch (error) {
    if (error instanceof RegistryResolverResponseError) throw error;
    throw new RegistryResolverResponseError(resolver);
  }

  try {
    const body = new TextDecoder('utf-8', { fatal: true }).decode(
      Buffer.concat(
        chunks.map((chunk) => Buffer.from(chunk)),
        size,
      ),
    );
    return JSON.parse(body) as unknown;
  } catch {
    throw new RegistryResolverResponseError(resolver);
  }
}

function parseContext(
  value: unknown,
  expected: { workspaceId: string; sessionId: string },
): RegistryObservabilityContext {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, CONTEXT_KEYS) ||
    value.schema_version !== 1 ||
    !matchesScope(value, expected) ||
    !isNonEmptyString(value.organization_id) ||
    !isAgent(value.agent) ||
    !isNullableString(value.harness) ||
    !isNullableString(value.harness_mode) ||
    !isMember(SELECTION_SOURCES, value.selection_source) ||
    !isContextEpochs(value.epochs)
  ) {
    throw new RegistryResolverResponseError('context');
  }

  const effectiveCaptureMode = parseCapture(value.capture);
  const deliveryContext =
    value.binding === null
      ? null
      : parseBinding(
          value.binding,
          effectiveCaptureMode,
          value.organization_id,
          expected.workspaceId,
        );
  const status = value.status;
  if (status === 'enabled') {
    if (value.reason !== null || deliveryContext === null) {
      throw new RegistryResolverResponseError('context');
    }
    return {
      status,
      deliveryContext: {
        ...deliveryContext,
        ...(isSafeAttributionLabel(value.agent.id) ? { agentId: value.agent.id } : {}),
        agentVersion: value.agent.version,
        ...(isSafeAttributionLabel(value.harness) ? { harness: value.harness } : {}),
        ...(isSafeAttributionLabel(value.harness_mode) ? { harnessMode: value.harness_mode } : {}),
      },
    };
  }
  if (status === 'disabled' && value.reason === 'session_pin_disabled') {
    return { status };
  }
  if (status === 'suppressed' && isMember(SUPPRESSION_REASONS, value.reason)) {
    return { status };
  }
  throw new RegistryResolverResponseError('context');
}

function parseBinding(
  value: unknown,
  effectiveCaptureMode: AuthorityCaptureMode,
  organizationId: string,
  expectedWorkspaceId: string,
): PinnedDeliveryContext {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, [
      'config',
      'current_credential_version',
      'id',
      'lifecycle_status',
      'scope',
      'target',
      'version',
      'workspace_id',
    ]) ||
    !isNonEmptyString(value.id) ||
    !isPositiveInteger(value.version) ||
    !isMember(BINDING_SCOPES, value.scope) ||
    !isMember(BINDING_STATUSES, value.lifecycle_status) ||
    !isNullablePositiveInteger(value.current_credential_version) ||
    (value.scope === 'organization' && value.workspace_id !== null) ||
    (value.scope === 'workspace' && value.workspace_id !== expectedWorkspaceId)
  ) {
    throw new RegistryResolverResponseError('context');
  }
  const target = value.target;
  const config = value.config;
  if (
    !isRecord(target) ||
    !hasExactKeys(target, [
      'adapter_type',
      'endpoint_class',
      'endpoint_kind',
      'endpoint_url',
      'external_project_id',
    ]) ||
    !isMember(ADAPTER_TYPES, target.adapter_type) ||
    !isMember(ENDPOINT_KINDS, target.endpoint_kind) ||
    !isMember(ENDPOINT_CLASSES, target.endpoint_class) ||
    !isUrlString(target.endpoint_url) ||
    !isNullableNonEmptyString(target.external_project_id) ||
    !isRecord(config) ||
    !hasExactKeys(config, [
      'capture_mode',
      'compression',
      'config_schema_version',
      'environment',
      'protocol',
      'release',
      'sample_rate',
      'semantic_profile',
      'timeout_ms',
    ]) ||
    !isMember(SEMANTIC_PROFILES, config.semantic_profile) ||
    !isMember(PROTOCOLS, config.protocol) ||
    !isMember(COMPRESSIONS, config.compression) ||
    !isPositiveInteger(config.timeout_ms) ||
    !isNullableString(config.environment) ||
    !isNullableString(config.release) ||
    !isMember(CAPTURE_MODES, config.capture_mode) ||
    typeof config.sample_rate !== 'number' ||
    !Number.isFinite(config.sample_rate) ||
    config.sample_rate < 0 ||
    config.sample_rate > 1 ||
    !isPositiveInteger(config.config_schema_version)
  ) {
    throw new RegistryResolverResponseError('context');
  }
  return {
    organizationId,
    bindingId: value.id,
    bindingVersion: value.version,
    adapterType: target.adapter_type,
    endpointKind: target.endpoint_kind,
    endpointClass: target.endpoint_class,
    endpointUrl: target.endpoint_url,
    semanticProfile: config.semantic_profile,
    protocol: config.protocol,
    compression: config.compression,
    timeoutMs: config.timeout_ms,
    captureMode: effectiveCaptureMode,
    sampleRate: config.sample_rate,
    configSchemaVersion: config.config_schema_version,
    ...(isSafeAttributionLabel(config.environment) ? { environment: config.environment } : {}),
    ...(isSafeAttributionLabel(config.release) ? { release: config.release } : {}),
  };
}

function parseCapture(value: unknown): AuthorityCaptureMode {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, ['current_ceilings', 'effective_mode', 'pinned_mode']) ||
    !isMember(CAPTURE_MODES, value.pinned_mode) ||
    !isMember(CAPTURE_MODES, value.effective_mode) ||
    !isRecord(value.current_ceilings) ||
    !hasExactKeys(value.current_ceilings, ['organization', 'platform', 'workspace']) ||
    !isMember(CAPTURE_MODES, value.current_ceilings.platform) ||
    !isMember(CAPTURE_MODES, value.current_ceilings.organization) ||
    !isMember(CAPTURE_MODES, value.current_ceilings.workspace)
  ) {
    throw new RegistryResolverResponseError('context');
  }
  const rank = { metadata_only: 0, redacted_io: 1, raw_io: 2 };
  if (
    [value.pinned_mode, ...Object.values(value.current_ceilings)].some(
      (mode) =>
        rank[value.effective_mode as AuthorityCaptureMode] > rank[mode as AuthorityCaptureMode],
    )
  ) {
    throw new RegistryResolverResponseError('context');
  }
  return value.effective_mode;
}

function parseSecret(value: unknown): RegistryObservabilitySecret {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, [
      'authorization_id',
      'binding_id',
      'binding_version',
      'bundle',
      'credential_version',
      'effective_capture_mode',
      'schema_version',
    ]) ||
    value.schema_version !== 1 ||
    typeof value.authorization_id !== 'string' ||
    !AUTHORIZATION_ID_RE.test(value.authorization_id) ||
    !isNonEmptyString(value.binding_id) ||
    !isPositiveInteger(value.binding_version) ||
    !isPositiveInteger(value.credential_version) ||
    !isMember(CAPTURE_MODES, value.effective_capture_mode)
  ) {
    throw new RegistryResolverResponseError('secret');
  }
  const auth = parseBundle(value.bundle);
  return {
    bindingId: value.binding_id,
    bindingVersion: value.binding_version,
    effectiveCaptureMode: value.effective_capture_mode,
    auth,
  };
}

function parseBundle(value: unknown): RegistryObservabilitySecret['auth'] {
  if (!isRecord(value) || typeof value.adapter_type !== 'string') {
    throw new RegistryResolverResponseError('secret');
  }
  if (value.adapter_type === 'langfuse_sdk') {
    if (
      !hasExactKeys(value, ['adapter_type', 'public_key', 'secret_key']) ||
      !isSecretValue(value.public_key) ||
      !isSecretValue(value.secret_key)
    ) {
      throw new RegistryResolverResponseError('secret');
    }
    return { type: 'unsupported' };
  }
  if (
    value.adapter_type !== 'otlp_http' ||
    !hasExactKeys(value, ['adapter_type', 'auth']) ||
    !isRecord(value.auth)
  ) {
    throw new RegistryResolverResponseError('secret');
  }
  const auth = value.auth;
  if (auth.type === 'basic') {
    if (
      !hasExactKeys(auth, ['password', 'type', 'username']) ||
      !isBasicUsername(auth.username) ||
      !isSecretValue(auth.password)
    ) {
      throw new RegistryResolverResponseError('secret');
    }
    return { type: 'basic', username: auth.username, password: auth.password };
  }
  if (auth.type === 'bearer') {
    if (!hasExactKeys(auth, ['token', 'type']) || !isSecretValue(auth.token)) {
      throw new RegistryResolverResponseError('secret');
    }
    return { type: 'unsupported' };
  }
  if (auth.type === 'custom_headers') {
    if (!hasExactKeys(auth, ['headers', 'type']) || !isCanonicalCustomHeaders(auth.headers)) {
      throw new RegistryResolverResponseError('secret');
    }
    return { type: 'unsupported' };
  }
  throw new RegistryResolverResponseError('secret');
}

function validateInternalBaseUrl(value: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error('RegistryObservabilityClient: internal base URL must be absolute HTTP(S)');
  }
  if (
    (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') ||
    parsed.username ||
    parsed.password ||
    parsed.search ||
    parsed.hash ||
    (parsed.pathname !== '' && parsed.pathname !== '/')
  ) {
    throw new Error('RegistryObservabilityClient: internal base URL must be an HTTP(S) origin');
  }
  return parsed.origin;
}

function matchesScope(
  value: Record<string, unknown>,
  expected: { workspaceId: string; sessionId: string },
): boolean {
  return value.workspace_id === expected.workspaceId && value.session_id === expected.sessionId;
}

function isAgent(value: unknown): value is { id: string; version: number } {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['id', 'version']) &&
    typeof value.id === 'string' &&
    /^(?:agt|agent)_[A-Za-z0-9_-]+$/u.test(value.id) &&
    isPositiveInteger(value.version)
  );
}

function isContextEpochs(value: unknown): boolean {
  if (!isRecord(value) || !hasExactKeys(value, ['current', 'pinned'])) return false;
  return isEpochSet(value.pinned) && isEpochSet(value.current);
}

function isEpochSet(value: unknown): boolean {
  return (
    isRecord(value) &&
    hasExactKeys(value, EPOCH_KEYS) &&
    EPOCH_KEYS.every((key) => isNonNegativeInteger(value[key]))
  );
}

function isCanonicalCustomHeaders(value: unknown): boolean {
  if (!isRecord(value)) return false;
  const entries = Object.entries(value);
  if (entries.length === 0 || entries.length > 32) return false;
  const normalized = new Map<string, string>();
  let totalLength = 0;
  for (const [rawName, headerValue] of entries) {
    const name = rawName.toLowerCase();
    if (
      rawName.length === 0 ||
      rawName.length > 128 ||
      !/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/u.test(rawName) ||
      FORBIDDEN_CUSTOM_HEADER_NAMES.has(name) ||
      normalized.has(name) ||
      typeof headerValue !== 'string' ||
      headerValue.length > 4_096 ||
      hasControlCharacter(headerValue) ||
      hasUnpairedUtf16Surrogate(headerValue)
    ) {
      return false;
    }
    totalLength += rawName.length + headerValue.length;
    if (totalLength > SECRET_VALUE_MAX_LENGTH) return false;
    normalized.set(name, headerValue);
  }
  const canonical = Object.fromEntries(
    [...normalized.entries()].sort(([left], [right]) => left.localeCompare(right)),
  );
  return JSON.stringify(canonical) === JSON.stringify(value);
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const keys = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return (
    keys.length === sortedExpected.length &&
    keys.every((key, index) => key === sortedExpected[index])
  );
}

function isMember<T extends string>(values: ReadonlySet<T>, value: unknown): value is T {
  return typeof value === 'string' && values.has(value as T);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === 'string';
}

function isNullableNonEmptyString(value: unknown): value is string | null {
  return value === null || isNonEmptyString(value);
}

function isSecretValue(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value.length <= SECRET_VALUE_MAX_LENGTH &&
    !hasControlCharacter(value) &&
    !hasUnpairedUtf16Surrogate(value)
  );
}

function isBasicUsername(value: unknown): value is string {
  return isSecretValue(value) && !value.includes(':');
}

function hasControlCharacter(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x1f || code === 0x7f) return true;
  }
  return false;
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

function isUrlString(value: unknown): value is string {
  if (!isNonEmptyString(value)) return false;
  try {
    new URL(value);
    return true;
  } catch {
    return false;
  }
}

function isNullablePositiveInteger(value: unknown): value is number | null {
  return value === null || isPositiveInteger(value);
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}
