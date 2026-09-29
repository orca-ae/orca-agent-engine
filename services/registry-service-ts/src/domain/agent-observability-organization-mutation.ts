// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull, sql } from 'drizzle-orm';
import {
  OrganizationAgentObservabilityDisableRequestSchema,
  type OrganizationAgentObservabilityDisableRequest,
  OrganizationAgentObservabilityCredentialRotationRequestSchema,
  type OrganizationAgentObservabilityCredentialRotationRequest,
  OrganizationAgentObservabilityPutRequestSchema,
  type OrganizationAgentObservabilityPutRequest,
} from '../contracts/agent-observability.contract.js';
import type { DbTransaction } from '../persistence/postgres/client.js';
import {
  agentObservabilityBindingCredentials,
  agentObservabilityBindingVersions,
  agentObservabilityBindings,
  agentObservabilityOrganizationSettings,
  agentObservabilityPlatformPolicy,
  organizations,
} from '../persistence/postgres/schema.js';
import {
  AgentObservabilitySecretBundleError,
  encodeAgentObservabilitySecretBundle,
  isAgentObservabilitySecretReference,
  type AgentObservabilitySecretBundle,
  type AgentObservabilitySecretReference,
} from './agent-observability-secrets.js';
import { agentObservabilityStateEtag } from './agent-observability-etag.js';
import {
  isAgentObservabilityCaptureMode,
  captureRestrictionEpochAfterReplacement,
  type AgentObservabilityCaptureMode,
} from './agent-observability-policy.js';
import {
  AgentObservabilityStateAvailabilityError,
  AgentObservabilityStateNotFoundError,
  loadOrganizationAgentObservabilityStateInTransaction,
  type LoadedOrganizationAgentObservabilityState,
} from './agent-observability-state.js';
import {
  isSingleStrongEntityTag,
  normalizeAgentObservabilityEndpoint,
  normalizeAgentObservabilityIdempotencyKey,
  normalizeAgentObservabilityOtlpHttpCredentialInput,
  type AgentObservabilityOtlpHttpCredentials,
} from './agent-observability-validation.js';
import {
  AGENT_OBSERVABILITY_CREDENTIAL_VERSION_MAX,
  type AgentObservabilityStagedActivation,
  type OrganizationAgentObservabilityPreemptionTarget,
} from './agent-observability-mutations.js';

export class OrganizationAgentObservabilityMutationRequestError extends Error {
  override readonly name = 'OrganizationAgentObservabilityMutationRequestError';

  constructor() {
    super('invalid agent observability request');
  }
}

export class OrganizationAgentObservabilityMutationNotFoundError extends Error {
  override readonly name = 'OrganizationAgentObservabilityMutationNotFoundError';

  constructor() {
    super('organization not found');
  }
}

export class OrganizationAgentObservabilityMutationUnavailableError extends Error {
  override readonly name = 'OrganizationAgentObservabilityMutationUnavailableError';

  constructor() {
    super('agent observability unavailable');
  }
}

/** Stored authority and credential-head state disagree. Never treat this as absence. */
export class OrganizationAgentObservabilityMutationInvariantError extends Error {
  override readonly name = 'OrganizationAgentObservabilityMutationInvariantError';

  constructor() {
    super('agent observability authoritative state is corrupt');
  }
}

/** A valid configured default exists, but it is not an OTLP credential rotation target. */
export class OrganizationAgentObservabilityCredentialRotationNotRotatableError extends Error {
  override readonly name = 'OrganizationAgentObservabilityCredentialRotationNotRotatableError';

  constructor() {
    super('agent observability credential head is not rotatable');
  }
}

export type NormalizedOrganizationAgentObservabilityPutRequest = {
  target: {
    adapterType: 'otlp_http';
    endpointKind: 'traces_endpoint' | 'base_endpoint';
    endpointClass: 'public' | 'private';
    endpoint: string;
    externalProjectId: string | null;
  };
  config: {
    semanticProfile: 'otel_genai' | 'langfuse';
    protocol: 'http/protobuf' | 'http/json';
    compression: 'none' | 'gzip';
    timeoutMs: number;
    environment: string | null;
    release: string | null;
    captureMode: AgentObservabilityCaptureMode;
    sampleRate: number;
  };
  captureCeiling: AgentObservabilityCaptureMode;
  credentials: AgentObservabilityOtlpHttpCredentials | undefined;
};

export type NormalizedOrganizationAgentObservabilityCredentialRotationRequest = {
  credentials: AgentObservabilityOtlpHttpCredentials;
};

/** Empty by contract: target selection remains Registry authority only. */
export type NormalizedOrganizationAgentObservabilityDisableRequest = Record<never, never>;

export interface OrganizationAgentObservabilityMutationBinding {
  id: string;
  status: 'active' | 'draining' | 'disabled' | 'archived';
  target: {
    adapterType: 'otlp_http' | 'langfuse_sdk';
    endpointKind: 'traces_endpoint' | 'base_endpoint';
    endpointClass: 'public' | 'private';
    endpoint: string;
    externalProjectId: string | null;
  };
  configVersion: number;
  credentialVersion: number | null;
}

export interface OrganizationAgentObservabilityMutationAuthority {
  organizationId: string;
  state: LoadedOrganizationAgentObservabilityState;
  stateVersion: string;
  currentBinding: OrganizationAgentObservabilityMutationBinding | null;
  captureCeiling: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
  selectionEpoch: number;
  defaultRevocationEpoch: number;
  organizationRevocationEpoch: number;
  allowedAdapters: readonly string[];
  allowedEndpointClasses: readonly string[];
}

export type OrganizationAgentObservabilityMutationKind =
  | { type: 'initial' }
  | { type: 'same_target'; bindingId: string }
  | { type: 'replacement'; previousActiveBindingId: string | null };

export function parseOrganizationAgentObservabilityPutRequest(
  value: unknown,
): NormalizedOrganizationAgentObservabilityPutRequest {
  const parsed = OrganizationAgentObservabilityPutRequestSchema.safeParse(value);
  if (!parsed.success) throw new OrganizationAgentObservabilityMutationRequestError();
  return normalizePutRequest(parsed.data);
}

export function parseOrganizationAgentObservabilityCredentialRotationRequest(
  value: unknown,
): NormalizedOrganizationAgentObservabilityCredentialRotationRequest {
  const parsed = OrganizationAgentObservabilityCredentialRotationRequestSchema.safeParse(value);
  if (!parsed.success) throw new OrganizationAgentObservabilityMutationRequestError();
  return normalizeCredentialRotationRequest(parsed.data);
}

export function parseOrganizationAgentObservabilityDisableRequest(
  value: unknown,
): NormalizedOrganizationAgentObservabilityDisableRequest {
  const parsed = OrganizationAgentObservabilityDisableRequestSchema.safeParse(value);
  if (!parsed.success) throw new OrganizationAgentObservabilityMutationRequestError();
  return normalizeDisableRequest(parsed.data);
}

/** The accepted, canonical request shape used for the idempotency body hash. */
export function organizationAgentObservabilityMutationBody(
  request: NormalizedOrganizationAgentObservabilityPutRequest,
): Record<string, unknown> {
  return {
    target: {
      adapter_type: request.target.adapterType,
      endpoint_kind: request.target.endpointKind,
      endpoint_class: request.target.endpointClass,
      endpoint_url: request.target.endpoint,
      external_project_id: request.target.externalProjectId,
    },
    config: {
      semantic_profile: request.config.semanticProfile,
      protocol: request.config.protocol,
      compression: request.config.compression,
      timeout_ms: request.config.timeoutMs,
      environment: request.config.environment,
      release: request.config.release,
      capture_mode: request.config.captureMode,
      sample_rate: request.config.sampleRate,
    },
    capture_ceiling: request.captureCeiling,
    ...(request.credentials === undefined ? {} : { credentials: request.credentials }),
  };
}

/** Canonical rotation body has no caller-selected binding or target identity. */
export function organizationAgentObservabilityCredentialRotationBody(
  request: NormalizedOrganizationAgentObservabilityCredentialRotationRequest,
): Record<string, unknown> {
  return { credentials: request.credentials };
}

/** Canonical durable idempotency body for the target-free disable route. */
export function organizationAgentObservabilityDisableBody(
  _request: NormalizedOrganizationAgentObservabilityDisableRequest,
): Record<never, never> {
  return {};
}

export function parseOrganizationAgentObservabilityIdempotencyKey(value: unknown): string {
  const key = normalizeAgentObservabilityIdempotencyKey(value);
  if (key === null) throw new OrganizationAgentObservabilityMutationRequestError();
  return key;
}

/** Parse exactly one strong ETag. Weak, list, wildcard, and malformed forms are invalid. */
export function parseOrganizationAgentObservabilityIfMatch(value: unknown): string | null {
  if (value === undefined) return null;
  if (!isSingleStrongEntityTag(value)) {
    throw new OrganizationAgentObservabilityMutationRequestError();
  }
  return value;
}

export function classifyOrganizationAgentObservabilityMutation(
  currentBinding: OrganizationAgentObservabilityMutationBinding | null,
  request: NormalizedOrganizationAgentObservabilityPutRequest,
): OrganizationAgentObservabilityMutationKind {
  if (currentBinding === null) return { type: 'initial' };
  if (
    currentBinding.status === 'active' &&
    targetIdentityEquals(currentBinding.target, request.target)
  ) {
    return { type: 'same_target', bindingId: currentBinding.id };
  }
  return {
    type: 'replacement',
    previousActiveBindingId: currentBinding.status === 'active' ? currentBinding.id : null,
  };
}

export function assertOrganizationAgentObservabilityMutationRequest(
  request: NormalizedOrganizationAgentObservabilityPutRequest,
  kind: OrganizationAgentObservabilityMutationKind,
): void {
  if (request.config.semanticProfile === 'langfuse' && request.target.externalProjectId === null) {
    throw new OrganizationAgentObservabilityMutationRequestError();
  }
  if (kind.type === 'same_target') {
    if (request.credentials !== undefined) {
      throw new OrganizationAgentObservabilityMutationRequestError();
    }
    return;
  }
  if (request.credentials === undefined) {
    throw new OrganizationAgentObservabilityMutationRequestError();
  }
}

export function assertOrganizationAgentObservabilityTargetAdmission(
  authority: OrganizationAgentObservabilityMutationAuthority,
  request: NormalizedOrganizationAgentObservabilityPutRequest,
): void {
  if (
    !authority.allowedAdapters.includes(request.target.adapterType) ||
    !authority.allowedEndpointClasses.includes(request.target.endpointClass)
  ) {
    throw new OrganizationAgentObservabilityMutationRequestError();
  }
}

export function assertOrganizationAgentObservabilityPrecondition(input: {
  currentBinding: OrganizationAgentObservabilityMutationBinding | null;
  currentEtag: string;
  ifMatch: string | null;
}): 'ok' | 'missing' | 'stale' {
  if (input.ifMatch !== null && input.ifMatch !== input.currentEtag) return 'stale';
  if (input.currentBinding !== null && input.ifMatch === null) return 'missing';
  return 'ok';
}

/** Credential rotation always requires an exact current state ETag. */
export function assertOrganizationAgentObservabilityCredentialRotationPrecondition(input: {
  currentEtag: string;
  ifMatch: string | null;
}): 'ok' | 'missing' | 'stale' {
  if (input.ifMatch === null) return 'missing';
  return input.ifMatch === input.currentEtag ? 'ok' : 'stale';
}

/** Emergency disable permits no precondition but never accepts a stale supplied one. */
export function assertOrganizationAgentObservabilityDisablePrecondition(input: {
  currentEtag: string;
  ifMatch: string | null;
}): 'ok' | 'stale' {
  return input.ifMatch === null || input.ifMatch === input.currentEtag ? 'ok' : 'stale';
}

export function encodeOrganizationAgentObservabilityCredentials(input: {
  request: NormalizedOrganizationAgentObservabilityPutRequest;
  candidateBindingId: string;
}): string {
  const credentials = input.request.credentials;
  if (credentials === undefined) throw new OrganizationAgentObservabilityMutationRequestError();
  return encodeOrganizationAgentObservabilityCredentialBundle({
    credentials,
    bindingId: input.candidateBindingId,
    credentialVersion: 1,
  });
}

export function encodeOrganizationAgentObservabilityCredentialRotation(input: {
  request: NormalizedOrganizationAgentObservabilityCredentialRotationRequest;
  bindingId: string;
  credentialVersion: number;
}): string {
  return encodeOrganizationAgentObservabilityCredentialBundle({
    credentials: input.request.credentials,
    bindingId: input.bindingId,
    credentialVersion: input.credentialVersion,
  });
}

function encodeOrganizationAgentObservabilityCredentialBundle(input: {
  credentials: AgentObservabilityOtlpHttpCredentials;
  bindingId: string;
  credentialVersion: number;
}): string {
  const { credentials } = input;
  let bundle: AgentObservabilitySecretBundle;
  if (credentials.type === 'basic') {
    bundle = {
      bindingId: input.bindingId,
      credentialVersion: input.credentialVersion,
      adapterType: 'otlp_http',
      auth: {
        type: 'basic',
        username: credentials.username,
        password: credentials.password,
      },
    };
  } else if (credentials.type === 'bearer') {
    bundle = {
      bindingId: input.bindingId,
      credentialVersion: input.credentialVersion,
      adapterType: 'otlp_http',
      auth: { type: 'bearer', token: credentials.token },
    };
  } else {
    bundle = {
      bindingId: input.bindingId,
      credentialVersion: input.credentialVersion,
      adapterType: 'otlp_http',
      auth: { type: 'custom_headers', headers: credentials.headers },
    };
  }
  try {
    return encodeAgentObservabilitySecretBundle(bundle);
  } catch (error) {
    if (error instanceof AgentObservabilitySecretBundleError) {
      throw new OrganizationAgentObservabilityMutationRequestError();
    }
    throw error;
  }
}

/**
 * Locks exactly the authority rows Session selection locks in platform policy
 * -> organization parent -> setting -> binding -> current version/credential
 * order. Setting and binding are mutable selection authority and take UPDATE;
 * immutable parents and heads take SHARE, so independent organizations do not
 * serialize behind a global platform-policy UPDATE lock. Every write path calls
 * this before reservation acquisition and again before finalization.
 */
export async function lockOrganizationAgentObservabilityMutationAuthority(
  tx: DbTransaction,
  organizationId: string,
): Promise<OrganizationAgentObservabilityMutationAuthority> {
  const platform = (
    await tx
      .select()
      .from(agentObservabilityPlatformPolicy)
      .where(eq(agentObservabilityPlatformPolicy.id, 'default'))
      .for('share')
      .limit(1)
  )[0];
  if (!platform) throw new OrganizationAgentObservabilityMutationUnavailableError();

  const organization = (
    await tx
      .select({ id: organizations.id, status: organizations.status })
      .from(organizations)
      .where(eq(organizations.id, organizationId))
      .for('share')
      .limit(1)
  )[0];
  if (!organization || organization.status !== 'active') {
    throw new OrganizationAgentObservabilityMutationNotFoundError();
  }

  const setting = (
    await tx
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId))
      .for('update')
      .limit(1)
  )[0];
  if (!setting) throw new OrganizationAgentObservabilityMutationUnavailableError();

  if (setting.activeDefaultBindingId !== null) {
    const binding = (
      await tx
        .select()
        .from(agentObservabilityBindings)
        .where(
          and(
            eq(agentObservabilityBindings.id, setting.activeDefaultBindingId),
            eq(agentObservabilityBindings.organizationId, organizationId),
            eq(agentObservabilityBindings.scopeType, 'organization'),
            isNull(agentObservabilityBindings.workspaceId),
          ),
        )
        .for('update')
        .limit(1)
    )[0];
    if (!binding) throw new OrganizationAgentObservabilityMutationInvariantError();
    const version = (
      await tx
        .select({ bindingId: agentObservabilityBindingVersions.bindingId })
        .from(agentObservabilityBindingVersions)
        .where(
          and(
            eq(agentObservabilityBindingVersions.bindingId, binding.id),
            eq(agentObservabilityBindingVersions.version, binding.currentVersion),
          ),
        )
        .for('share')
        .limit(1)
    )[0];
    if (!version) throw new OrganizationAgentObservabilityMutationInvariantError();
    // Do not select secret_ref. The lock serializes credential-head changes
    // while preserving the route's no-secret memory boundary.
    await tx
      .select({ version: agentObservabilityBindingCredentials.credentialVersion })
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, binding.id))
      .for('share')
      .limit(1);
  }

  let state: LoadedOrganizationAgentObservabilityState;
  try {
    state = await loadOrganizationAgentObservabilityStateInTransaction({ db: tx, organizationId });
  } catch (error) {
    if (error instanceof AgentObservabilityStateNotFoundError) {
      throw new OrganizationAgentObservabilityMutationNotFoundError();
    }
    if (error instanceof AgentObservabilityStateAvailabilityError) {
      throw new OrganizationAgentObservabilityMutationUnavailableError();
    }
    throw error;
  }

  const configured = state.response.configured.default_binding;
  const currentBinding = configured
    ? {
        id: configured.id,
        status: configured.status,
        target: {
          adapterType: configured.target.adapter_type,
          endpointKind: configured.target.endpoint_kind,
          endpointClass: configured.target.endpoint_class,
          endpoint: configured.target.endpoint_url,
          externalProjectId: configured.target.external_project_id,
        },
        configVersion: configured.config.version,
        credentialVersion: configured.credential.version,
      }
    : null;
  const [allowedAdapters, allowedEndpointClasses] = state.etagInput.platformPolicy;
  const [
    ,
    ,
    selectionEpoch,
    defaultRevocationEpoch,
    organizationRevocationEpoch,
    captureCeiling,
    captureRestrictionEpoch,
  ] = state.etagInput.organizationSetting;
  if (
    !isAgentObservabilityCaptureMode(captureCeiling) ||
    !Number.isSafeInteger(selectionEpoch) ||
    !Number.isSafeInteger(defaultRevocationEpoch) ||
    !Number.isSafeInteger(organizationRevocationEpoch) ||
    !Number.isSafeInteger(captureRestrictionEpoch)
  ) {
    throw new OrganizationAgentObservabilityMutationUnavailableError();
  }
  return {
    organizationId,
    state,
    stateVersion: agentObservabilityStateEtag(state.etagInput),
    currentBinding,
    captureCeiling,
    captureRestrictionEpoch,
    selectionEpoch,
    defaultRevocationEpoch,
    organizationRevocationEpoch,
    allowedAdapters,
    allowedEndpointClasses,
  };
}

/**
 * Reads an exact credential head only after the organization setting and its
 * selected binding are locked by {@link lockOrganizationAgentObservabilityMutationAuthority}.
 * PUT deliberately never calls this loader: SecretStore references remain in
 * rotation's transient application memory only.
 */
export interface OrganizationAgentObservabilityCredentialRotationHead {
  bindingId: string;
  configVersion: number;
  credentialVersion: number;
  secretRef: AgentObservabilitySecretReference;
}

export async function loadOrganizationAgentObservabilityCredentialRotationHead(
  tx: DbTransaction,
  authority: OrganizationAgentObservabilityMutationAuthority,
): Promise<OrganizationAgentObservabilityCredentialRotationHead | null> {
  const configured = authority.state.response.configured.default_binding;
  const binding = authority.currentBinding;
  // `null` means only that no credential head is configured. Every other
  // missing or divergent head shape is corrupt authoritative state, not an
  // ordinary non-rotatable outcome.
  if (configured === null && binding === null) return null;
  if (configured === null || binding === null) {
    throw new OrganizationAgentObservabilityMutationInvariantError();
  }
  if (
    configured.id !== binding.id ||
    configured.scope !== 'organization' ||
    configured.organization_id !== authority.organizationId ||
    configured.workspace_id !== null ||
    configured.config.version !== binding.configVersion ||
    configured.credential.version !== binding.credentialVersion
  ) {
    throw new OrganizationAgentObservabilityMutationInvariantError();
  }
  if (binding.status !== 'active' || binding.target.adapterType !== 'otlp_http') {
    throw new OrganizationAgentObservabilityCredentialRotationNotRotatableError();
  }
  if (configured.credential.configured === false && binding.credentialVersion === null) {
    return null;
  }
  if (
    configured.credential.configured !== true ||
    !Number.isSafeInteger(binding.configVersion) ||
    binding.configVersion <= 0 ||
    binding.credentialVersion === null ||
    !Number.isSafeInteger(binding.credentialVersion) ||
    binding.credentialVersion <= 0 ||
    binding.credentialVersion >= AGENT_OBSERVABILITY_CREDENTIAL_VERSION_MAX
  ) {
    throw new OrganizationAgentObservabilityMutationInvariantError();
  }

  const head = (
    await tx
      .select({
        bindingId: agentObservabilityBindingCredentials.bindingId,
        credentialVersion: agentObservabilityBindingCredentials.credentialVersion,
        secretRef: agentObservabilityBindingCredentials.secretRef,
        organizationId: agentObservabilityBindings.organizationId,
        scopeType: agentObservabilityBindings.scopeType,
        workspaceId: agentObservabilityBindings.workspaceId,
      })
      .from(agentObservabilityBindingCredentials)
      .innerJoin(
        agentObservabilityBindings,
        eq(agentObservabilityBindingCredentials.bindingId, agentObservabilityBindings.id),
      )
      .where(eq(agentObservabilityBindingCredentials.bindingId, binding.id))
      .for('update')
      .limit(1)
  )[0];
  if (!head) throw new OrganizationAgentObservabilityMutationInvariantError();
  if (
    head.bindingId !== binding.id ||
    head.organizationId !== authority.organizationId ||
    head.scopeType !== 'organization' ||
    head.workspaceId !== null ||
    head.credentialVersion !== binding.credentialVersion ||
    head.credentialVersion !== configured.credential.version ||
    !isAgentObservabilitySecretReference(head.secretRef)
  ) {
    throw new OrganizationAgentObservabilityMutationInvariantError();
  }
  return {
    bindingId: binding.id,
    configVersion: binding.configVersion,
    credentialVersion: head.credentialVersion,
    secretRef: head.secretRef,
  };
}

export async function applyOrganizationAgentObservabilityMutation(input: {
  db: Pick<DbTransaction, 'insert' | 'update'>;
  authority: OrganizationAgentObservabilityMutationAuthority;
  request: NormalizedOrganizationAgentObservabilityPutRequest;
  kind: OrganizationAgentObservabilityMutationKind;
  actor: string;
  now: Date;
  stagedActivation: AgentObservabilityStagedActivation | null;
}): Promise<{ applied: boolean }> {
  const nextCaptureRestrictionEpoch = captureRestrictionEpochAfterReplacement(
    input.authority.captureCeiling,
    input.request.captureCeiling,
    input.authority.captureRestrictionEpoch,
  );
  if (input.kind.type === 'same_target') {
    return applySameTargetPolicyReplacement({
      db: input.db,
      authority: input.authority,
      request: input.request,
      kind: input.kind,
      actor: input.actor,
      now: input.now,
      stagedActivation: input.stagedActivation,
      nextCaptureRestrictionEpoch,
    });
  }
  return applyTargetReplacement({
    db: input.db,
    authority: input.authority,
    request: input.request,
    kind: input.kind,
    actor: input.actor,
    now: input.now,
    stagedActivation: input.stagedActivation,
    nextCaptureRestrictionEpoch,
  });
}

/** Mutates only one credential head; config, selection, and epochs stay intact. */
export async function applyOrganizationAgentObservabilityCredentialRotation(input: {
  db: Pick<DbTransaction, 'update'>;
  authority: OrganizationAgentObservabilityMutationAuthority;
  head: OrganizationAgentObservabilityCredentialRotationHead;
  actor: string;
  now: Date;
  stagedActivation: AgentObservabilityStagedActivation | null;
}): Promise<{ applied: boolean }> {
  const binding = input.authority.currentBinding;
  const activation = input.stagedActivation;
  if (
    binding === null ||
    binding.id !== input.head.bindingId ||
    binding.status !== 'active' ||
    binding.target.adapterType !== 'otlp_http' ||
    binding.configVersion !== input.head.configVersion ||
    binding.credentialVersion !== input.head.credentialVersion ||
    activation === null ||
    activation.candidateBindingId !== input.head.bindingId ||
    activation.credentialVersion !== input.head.credentialVersion + 1
  ) {
    return { applied: false };
  }
  const updated = await input.db
    .update(agentObservabilityBindingCredentials)
    .set({
      secretRef: activation.secretRef,
      credentialVersion: activation.credentialVersion,
      keyHint: null,
      rotatedAt: input.now,
      updatedBy: input.actor,
      updatedAt: input.now,
    })
    .where(
      and(
        eq(agentObservabilityBindingCredentials.bindingId, input.head.bindingId),
        eq(agentObservabilityBindingCredentials.credentialVersion, input.head.credentialVersion),
        eq(agentObservabilityBindingCredentials.secretRef, input.head.secretRef),
      ),
    )
    .returning({ bindingId: agentObservabilityBindingCredentials.bindingId });
  return { applied: updated.length === 1 };
}

/**
 * Clear the selected organization default and advance its durable
 * default-revocation fence. Existing Session pin rows stay unchanged; later
 * resolver/pre-send enforcement compares their recorded epoch to this fence.
 * It deliberately retains the old target, config, credential head, and binding
 * revocation epoch so pin/reference retention remains valid. A degraded
 * selected binding is removable: only active becomes draining.
 */
export async function applyOrganizationAgentObservabilityDisable(input: {
  db: Pick<DbTransaction, 'update'>;
  authority: OrganizationAgentObservabilityMutationAuthority;
  actor: string;
  now: Date;
}): Promise<{ applied: boolean }> {
  assertOrganizationAgentObservabilityDisableAuthority(input.authority);
  const binding = input.authority.currentBinding;
  if (binding === null) {
    return { applied: true };
  }

  if (binding.status === 'active') {
    const drained = await input.db
      .update(agentObservabilityBindings)
      .set({ status: 'draining', updatedBy: input.actor, updatedAt: input.now })
      .where(
        and(
          eq(agentObservabilityBindings.id, binding.id),
          eq(agentObservabilityBindings.organizationId, input.authority.organizationId),
          isNull(agentObservabilityBindings.workspaceId),
          eq(agentObservabilityBindings.scopeType, 'organization'),
          eq(agentObservabilityBindings.status, 'active'),
          eq(agentObservabilityBindings.currentVersion, binding.configVersion),
        ),
      )
      .returning({ id: agentObservabilityBindings.id });
    if (drained.length !== 1) return { applied: false };
  }

  const cleared = await input.db
    .update(agentObservabilityOrganizationSettings)
    .set({
      activeDefaultBindingId: null,
      activeDefaultBindingScope: null,
      selectionEpoch: sql`${agentObservabilityOrganizationSettings.selectionEpoch} + 1`,
      defaultRevocationEpoch: sql`${agentObservabilityOrganizationSettings.defaultRevocationEpoch} + 1`,
      updatedAt: input.now,
    })
    .where(
      and(
        eq(agentObservabilityOrganizationSettings.organizationId, input.authority.organizationId),
        eq(agentObservabilityOrganizationSettings.activeDefaultBindingId, binding.id),
        eq(agentObservabilityOrganizationSettings.activeDefaultBindingScope, 'organization'),
        eq(agentObservabilityOrganizationSettings.selectionEpoch, input.authority.selectionEpoch),
        eq(
          agentObservabilityOrganizationSettings.defaultRevocationEpoch,
          input.authority.defaultRevocationEpoch,
        ),
        eq(
          agentObservabilityOrganizationSettings.organizationRevocationEpoch,
          input.authority.organizationRevocationEpoch,
        ),
        eq(
          agentObservabilityOrganizationSettings.captureRestrictionEpoch,
          input.authority.captureRestrictionEpoch,
        ),
        eq(agentObservabilityOrganizationSettings.captureCeiling, input.authority.captureCeiling),
      ),
    )
    .returning({ organizationId: agentObservabilityOrganizationSettings.organizationId });
  return { applied: cleared.length === 1 };
}

/** Exact authority-owned targets an emergency default disable may preempt. */
export function organizationAgentObservabilityDisablePreemptionTargets(
  authority: OrganizationAgentObservabilityMutationAuthority,
): OrganizationAgentObservabilityPreemptionTarget[] {
  assertOrganizationAgentObservabilityDisableAuthority(authority);
  const setting = {
    type: 'organization_setting',
    organizationId: authority.organizationId,
  } as const;
  if (authority.currentBinding === null) return [setting];
  return [
    setting,
    {
      type: 'binding',
      organizationId: authority.organizationId,
      bindingId: authority.currentBinding.id,
      bindingScope: 'organization',
    },
  ];
}

function normalizePutRequest(
  request: OrganizationAgentObservabilityPutRequest,
): NormalizedOrganizationAgentObservabilityPutRequest {
  const endpoint = normalizeAgentObservabilityEndpoint(request.target.endpoint_url);
  if (endpoint === null) throw new OrganizationAgentObservabilityMutationRequestError();
  const credentials =
    request.credentials === undefined
      ? undefined
      : normalizeAgentObservabilityOtlpHttpCredentialInput(request.credentials);
  if (credentials === null) throw new OrganizationAgentObservabilityMutationRequestError();
  return {
    target: {
      adapterType: request.target.adapter_type,
      endpointKind: request.target.endpoint_kind,
      endpointClass: request.target.endpoint_class,
      endpoint,
      externalProjectId: request.target.external_project_id ?? null,
    },
    config: {
      semanticProfile: request.config.semantic_profile,
      protocol: request.config.protocol,
      compression: request.config.compression,
      timeoutMs: request.config.timeout_ms,
      environment: request.config.environment ?? null,
      release: request.config.release ?? null,
      captureMode: request.config.capture_mode,
      sampleRate: request.config.sample_rate,
    },
    captureCeiling: request.capture_ceiling,
    credentials,
  };
}

function normalizeCredentialRotationRequest(
  request: OrganizationAgentObservabilityCredentialRotationRequest,
): NormalizedOrganizationAgentObservabilityCredentialRotationRequest {
  const credentials = normalizeAgentObservabilityOtlpHttpCredentialInput(request.credentials);
  if (credentials === null) throw new OrganizationAgentObservabilityMutationRequestError();
  return { credentials };
}

function normalizeDisableRequest(
  _request: OrganizationAgentObservabilityDisableRequest,
): NormalizedOrganizationAgentObservabilityDisableRequest {
  return {};
}

function assertOrganizationAgentObservabilityDisableAuthority(
  authority: OrganizationAgentObservabilityMutationAuthority,
): void {
  const configured = authority.state.response.configured.default_binding;
  const binding = authority.currentBinding;
  if (binding === null) {
    if (configured !== null) throw new OrganizationAgentObservabilityMutationInvariantError();
    return;
  }
  if (
    configured === null ||
    configured.id !== binding.id ||
    configured.scope !== 'organization' ||
    configured.organization_id !== authority.organizationId ||
    configured.workspace_id !== null ||
    configured.status !== binding.status ||
    configured.config.version !== binding.configVersion ||
    configured.credential.version !== binding.credentialVersion ||
    configured.credential.configured !== (binding.credentialVersion !== null) ||
    !Number.isSafeInteger(binding.configVersion) ||
    binding.configVersion <= 0 ||
    (binding.credentialVersion !== null &&
      (!Number.isSafeInteger(binding.credentialVersion) || binding.credentialVersion <= 0)) ||
    authority.selectionEpoch >= Number.MAX_SAFE_INTEGER ||
    authority.defaultRevocationEpoch >= Number.MAX_SAFE_INTEGER
  ) {
    throw new OrganizationAgentObservabilityMutationInvariantError();
  }
}

function targetIdentityEquals(
  left: OrganizationAgentObservabilityMutationBinding['target'],
  right: NormalizedOrganizationAgentObservabilityPutRequest['target'],
): boolean {
  return (
    left.adapterType === right.adapterType &&
    left.endpointKind === right.endpointKind &&
    left.endpointClass === right.endpointClass &&
    left.endpoint === right.endpoint &&
    left.externalProjectId === right.externalProjectId
  );
}

async function applySameTargetPolicyReplacement(input: {
  db: Pick<DbTransaction, 'insert' | 'update'>;
  authority: OrganizationAgentObservabilityMutationAuthority;
  request: NormalizedOrganizationAgentObservabilityPutRequest;
  kind: Extract<OrganizationAgentObservabilityMutationKind, { type: 'same_target' }>;
  actor: string;
  now: Date;
  stagedActivation: AgentObservabilityStagedActivation | null;
  nextCaptureRestrictionEpoch: number;
}): Promise<{ applied: boolean }> {
  const binding = input.authority.currentBinding;
  if (
    binding === null ||
    binding.id !== input.kind.bindingId ||
    binding.status !== 'active' ||
    input.stagedActivation !== null ||
    binding.configVersion >= Number.MAX_SAFE_INTEGER
  ) {
    return { applied: false };
  }
  const nextVersion = binding.configVersion + 1;
  await input.db.insert(agentObservabilityBindingVersions).values({
    bindingId: binding.id,
    version: nextVersion,
    adapterType: input.request.target.adapterType,
    semanticProfile: input.request.config.semanticProfile,
    protocol: input.request.config.protocol,
    compression: input.request.config.compression,
    timeoutMs: input.request.config.timeoutMs,
    environment: input.request.config.environment,
    release: input.request.config.release,
    captureMode: input.request.config.captureMode,
    sampleRate: String(input.request.config.sampleRate),
    configSchemaVersion: 1,
    createdBy: input.actor,
    createdAt: input.now,
  });
  const bindingUpdated = await input.db
    .update(agentObservabilityBindings)
    .set({ currentVersion: nextVersion, updatedBy: input.actor, updatedAt: input.now })
    .where(
      and(
        eq(agentObservabilityBindings.id, binding.id),
        eq(agentObservabilityBindings.organizationId, input.authority.organizationId),
        eq(agentObservabilityBindings.currentVersion, binding.configVersion),
        eq(agentObservabilityBindings.status, 'active'),
      ),
    )
    .returning({ id: agentObservabilityBindings.id });
  if (bindingUpdated.length !== 1) return { applied: false };

  const settingUpdated = await updateOrganizationSetting(input, binding.id);
  return { applied: settingUpdated };
}

async function applyTargetReplacement(input: {
  db: Pick<DbTransaction, 'insert' | 'update'>;
  authority: OrganizationAgentObservabilityMutationAuthority;
  request: NormalizedOrganizationAgentObservabilityPutRequest;
  kind: Exclude<OrganizationAgentObservabilityMutationKind, { type: 'same_target' }>;
  actor: string;
  now: Date;
  stagedActivation: AgentObservabilityStagedActivation | null;
  nextCaptureRestrictionEpoch: number;
}): Promise<{ applied: boolean }> {
  const activation = input.stagedActivation;
  if (
    activation === null ||
    activation.credentialVersion !== 1 ||
    activation.candidateBindingId.length === 0
  ) {
    return { applied: false };
  }
  await input.db.insert(agentObservabilityBindings).values({
    id: activation.candidateBindingId,
    organizationId: input.authority.organizationId,
    workspaceId: null,
    scopeType: 'organization',
    adapterType: input.request.target.adapterType,
    endpointKind: input.request.target.endpointKind,
    endpointClass: input.request.target.endpointClass,
    endpoint: input.request.target.endpoint,
    externalProjectId: input.request.target.externalProjectId,
    currentVersion: 1,
    status: 'active',
    revocationEpoch: 0,
    createdBy: input.actor,
    updatedBy: input.actor,
    archivedAt: null,
    createdAt: input.now,
    updatedAt: input.now,
  });
  await input.db.insert(agentObservabilityBindingVersions).values({
    bindingId: activation.candidateBindingId,
    version: 1,
    adapterType: input.request.target.adapterType,
    semanticProfile: input.request.config.semanticProfile,
    protocol: input.request.config.protocol,
    compression: input.request.config.compression,
    timeoutMs: input.request.config.timeoutMs,
    environment: input.request.config.environment,
    release: input.request.config.release,
    captureMode: input.request.config.captureMode,
    sampleRate: String(input.request.config.sampleRate),
    configSchemaVersion: 1,
    createdBy: input.actor,
    createdAt: input.now,
  });
  await input.db.insert(agentObservabilityBindingCredentials).values({
    bindingId: activation.candidateBindingId,
    secretRef: activation.secretRef,
    credentialVersion: 1,
    // Safe hint remains null until a separate non-secret hint design exists.
    keyHint: null,
    rotatedAt: input.now,
    updatedBy: input.actor,
    createdAt: input.now,
    updatedAt: input.now,
  });

  if (input.kind.type === 'replacement' && input.kind.previousActiveBindingId !== null) {
    const drained = await input.db
      .update(agentObservabilityBindings)
      .set({ status: 'draining', updatedBy: input.actor, updatedAt: input.now })
      .where(
        and(
          eq(agentObservabilityBindings.id, input.kind.previousActiveBindingId),
          eq(agentObservabilityBindings.organizationId, input.authority.organizationId),
          eq(agentObservabilityBindings.status, 'active'),
        ),
      )
      .returning({ id: agentObservabilityBindings.id });
    if (drained.length !== 1) return { applied: false };
  }

  const settingUpdated = await updateOrganizationSetting(
    input,
    activation.candidateBindingId,
    true,
  );
  return { applied: settingUpdated };
}

async function updateOrganizationSetting(
  input: {
    db: Pick<DbTransaction, 'update'>;
    authority: OrganizationAgentObservabilityMutationAuthority;
    request: NormalizedOrganizationAgentObservabilityPutRequest;
    now: Date;
    nextCaptureRestrictionEpoch: number;
  },
  nextBindingId: string,
  selectionChanged = false,
): Promise<boolean> {
  const currentBindingId = input.authority.currentBinding?.id ?? null;
  const bindingCondition =
    currentBindingId === null
      ? isNull(agentObservabilityOrganizationSettings.activeDefaultBindingId)
      : eq(agentObservabilityOrganizationSettings.activeDefaultBindingId, currentBindingId);
  const updated = await input.db
    .update(agentObservabilityOrganizationSettings)
    .set({
      activeDefaultBindingId: nextBindingId,
      activeDefaultBindingScope: 'organization',
      selectionEpoch: selectionChanged
        ? sql`${agentObservabilityOrganizationSettings.selectionEpoch} + 1`
        : input.authority.selectionEpoch,
      captureCeiling: input.request.captureCeiling,
      captureRestrictionEpoch: input.nextCaptureRestrictionEpoch,
      updatedAt: input.now,
    })
    .where(
      and(
        eq(agentObservabilityOrganizationSettings.organizationId, input.authority.organizationId),
        bindingCondition,
        eq(agentObservabilityOrganizationSettings.selectionEpoch, input.authority.selectionEpoch),
        eq(
          agentObservabilityOrganizationSettings.captureRestrictionEpoch,
          input.authority.captureRestrictionEpoch,
        ),
      ),
    )
    .returning({ organizationId: agentObservabilityOrganizationSettings.organizationId });
  return updated.length === 1;
}
