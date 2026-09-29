// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull } from 'drizzle-orm';
import {
  WorkspaceAgentObservabilityCredentialRotationRequestSchema,
  WorkspaceAgentObservabilityPutRequestSchema,
  type WorkspaceAgentObservabilityCredentialRotationRequest,
} from '../contracts/agent-observability.contract.js';
import type { DbTransaction } from '../persistence/postgres/client.js';
import {
  agentObservabilityBindingCredentials,
  agentObservabilityBindingVersions,
  agentObservabilityBindings,
  agentObservabilityOrganizationSettings,
  agentObservabilityPlatformPolicy,
  agentObservabilityWorkspaceSettings,
  organizations,
  workspaces,
} from '../persistence/postgres/schema.js';
import { agentObservabilityStateEtag } from './agent-observability-etag.js';
import {
  AGENT_OBSERVABILITY_CONFIG_VERSION_MAX,
  AGENT_OBSERVABILITY_CREDENTIAL_VERSION_MAX,
  type AgentObservabilityStagedActivation,
  type WorkspaceAgentObservabilityPreemptionTarget,
} from './agent-observability-mutations.js';
import {
  isAgentObservabilityCaptureMode,
  captureRestrictionEpochAfterReplacement,
  isAgentObservabilityCaptureModeAtMost,
  isValidAgentObservabilityAdapterConfiguration,
  isWorkspaceObservabilityMode,
  type AgentObservabilityCaptureMode,
  type WorkspaceObservabilityMode,
} from './agent-observability-policy.js';
import {
  AgentObservabilitySecretBundleError,
  encodeAgentObservabilitySecretBundle,
  isAgentObservabilitySecretReference,
  type AgentObservabilitySecretBundle,
  type AgentObservabilitySecretReference,
} from './agent-observability-secrets.js';
import {
  AgentObservabilityStateAvailabilityError,
  AgentObservabilityStateNotFoundError,
  loadWorkspaceAgentObservabilityStateInTransaction,
  type LoadedWorkspaceAgentObservabilityState,
} from './agent-observability-state.js';
import {
  isSingleStrongEntityTag,
  normalizeAgentObservabilityEndpoint,
  normalizeAgentObservabilityIdempotencyKey,
  normalizeAgentObservabilityOtlpHttpCredentialInput,
  type AgentObservabilityOtlpHttpCredentials,
} from './agent-observability-validation.js';

export class WorkspaceAgentObservabilityMutationRequestError extends Error {
  override readonly name = 'WorkspaceAgentObservabilityMutationRequestError';

  constructor() {
    super('invalid agent observability request');
  }
}

export class WorkspaceAgentObservabilityMutationNotFoundError extends Error {
  override readonly name = 'WorkspaceAgentObservabilityMutationNotFoundError';

  constructor() {
    super('workspace not found');
  }
}

export class WorkspaceAgentObservabilityMutationUnavailableError extends Error {
  override readonly name = 'WorkspaceAgentObservabilityMutationUnavailableError';

  constructor() {
    super('agent observability unavailable');
  }
}

/** Stored workspace selection, version, or epoch state disagrees with itself. */
export class WorkspaceAgentObservabilityMutationInvariantError extends Error {
  override readonly name = 'WorkspaceAgentObservabilityMutationInvariantError';

  constructor() {
    super('agent observability authoritative state is corrupt');
  }
}

/** A valid configured custom binding exists, but is not an OTLP rotation target. */
export class WorkspaceAgentObservabilityCredentialRotationNotRotatableError extends Error {
  override readonly name = 'WorkspaceAgentObservabilityCredentialRotationNotRotatableError';

  constructor() {
    super('workspace agent observability credential head is not rotatable');
  }
}

type NormalizedWorkspaceTarget = {
  adapterType: 'otlp_http';
  endpointKind: 'traces_endpoint' | 'base_endpoint';
  endpointClass: 'public' | 'private';
  endpoint: string;
  externalProjectId: string | null;
};

type WorkspaceBindingTarget = Omit<NormalizedWorkspaceTarget, 'adapterType'> & {
  adapterType: 'otlp_http' | 'langfuse_sdk';
};

type NormalizedWorkspaceConfig = {
  semanticProfile: 'otel_genai' | 'langfuse';
  protocol: 'http/protobuf' | 'http/json';
  compression: 'none' | 'gzip';
  timeoutMs: number;
  environment: string | null;
  release: string | null;
  captureMode: AgentObservabilityCaptureMode;
  sampleRate: number;
};

export type NormalizedWorkspaceAgentObservabilityPutRequest =
  | {
      mode: 'inherit';
      captureCeiling: AgentObservabilityCaptureMode;
    }
  | {
      mode: 'disabled';
      captureCeiling: AgentObservabilityCaptureMode;
    }
  | {
      mode: 'custom';
      target: NormalizedWorkspaceTarget;
      config: NormalizedWorkspaceConfig;
      captureCeiling: AgentObservabilityCaptureMode;
      credentials: AgentObservabilityOtlpHttpCredentials | undefined;
    };

export type NormalizedWorkspaceAgentObservabilityCredentialRotationRequest = {
  credentials: AgentObservabilityOtlpHttpCredentials;
};

export interface WorkspaceAgentObservabilityMutationBinding {
  id: string;
  status: 'active' | 'draining' | 'disabled' | 'archived';
  target: WorkspaceBindingTarget;
  configVersion: number;
  credentialVersion: number | null;
}

export interface WorkspaceAgentObservabilityMutationAuthority {
  organizationId: string;
  workspaceId: string;
  state: LoadedWorkspaceAgentObservabilityState;
  stateVersion: string;
  mode: WorkspaceObservabilityMode;
  currentBinding: WorkspaceAgentObservabilityMutationBinding | null;
  captureCeiling: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
  selectionEpoch: number;
  revocationEpoch: number;
  allowedAdapters: readonly string[];
  allowedEndpointClasses: readonly string[];
}

export type WorkspaceAgentObservabilityMutationKind =
  | {
      type: 'mode_only';
      selectionChanged: boolean;
      entersDisabled: boolean;
      previousActiveBindingId: string | null;
    }
  | { type: 'initial' }
  | { type: 'same_target'; bindingId: string }
  | { type: 'replacement'; previousActiveBindingId: string | null };

export function parseWorkspaceAgentObservabilityPutRequest(
  value: unknown,
): NormalizedWorkspaceAgentObservabilityPutRequest {
  const parsed = WorkspaceAgentObservabilityPutRequestSchema.safeParse(value);
  if (!parsed.success) throw new WorkspaceAgentObservabilityMutationRequestError();
  if (parsed.data.mode !== 'custom') {
    return { mode: parsed.data.mode, captureCeiling: parsed.data.capture_ceiling };
  }
  const endpoint = normalizeAgentObservabilityEndpoint(parsed.data.target.endpoint_url);
  const credentials =
    parsed.data.credentials === undefined
      ? undefined
      : normalizeAgentObservabilityOtlpHttpCredentialInput(parsed.data.credentials);
  if (endpoint === null || credentials === null) {
    throw new WorkspaceAgentObservabilityMutationRequestError();
  }
  return {
    mode: 'custom',
    target: {
      adapterType: parsed.data.target.adapter_type,
      endpointKind: parsed.data.target.endpoint_kind,
      endpointClass: parsed.data.target.endpoint_class,
      endpoint,
      externalProjectId: parsed.data.target.external_project_id ?? null,
    },
    config: {
      semanticProfile: parsed.data.config.semantic_profile,
      protocol: parsed.data.config.protocol,
      compression: parsed.data.config.compression,
      timeoutMs: parsed.data.config.timeout_ms,
      environment: parsed.data.config.environment ?? null,
      release: parsed.data.config.release ?? null,
      captureMode: parsed.data.config.capture_mode,
      sampleRate: parsed.data.config.sample_rate,
    },
    captureCeiling: parsed.data.capture_ceiling,
    credentials,
  };
}

export function parseWorkspaceAgentObservabilityCredentialRotationRequest(
  value: unknown,
): NormalizedWorkspaceAgentObservabilityCredentialRotationRequest {
  const parsed = WorkspaceAgentObservabilityCredentialRotationRequestSchema.safeParse(value);
  if (!parsed.success) throw new WorkspaceAgentObservabilityMutationRequestError();
  return normalizeWorkspaceCredentialRotationRequest(parsed.data);
}

/** Canonical body hash input contains no Registry-owned target or secret ref. */
export function workspaceAgentObservabilityMutationBody(
  request: NormalizedWorkspaceAgentObservabilityPutRequest,
): Record<string, unknown> {
  if (request.mode !== 'custom') {
    return { mode: request.mode, capture_ceiling: request.captureCeiling };
  }
  return {
    mode: request.mode,
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

/** Canonical rotation body intentionally has no caller-selected target identity. */
export function workspaceAgentObservabilityCredentialRotationBody(
  request: NormalizedWorkspaceAgentObservabilityCredentialRotationRequest,
): Record<string, unknown> {
  return { credentials: request.credentials };
}

export function parseWorkspaceAgentObservabilityIdempotencyKey(value: unknown): string {
  const key = normalizeAgentObservabilityIdempotencyKey(value);
  if (key === null) throw new WorkspaceAgentObservabilityMutationRequestError();
  return key;
}

/** Parse one strong ETag; absence is a typed 428 precondition outcome. */
export function parseWorkspaceAgentObservabilityIfMatch(value: unknown): string | null {
  if (value === undefined) return null;
  if (!isSingleStrongEntityTag(value)) throw new WorkspaceAgentObservabilityMutationRequestError();
  return value;
}

export function assertWorkspaceAgentObservabilityPrecondition(input: {
  currentEtag: string;
  ifMatch: string | null;
}): 'ok' | 'missing' | 'stale' {
  if (input.ifMatch === null) return 'missing';
  return input.ifMatch === input.currentEtag ? 'ok' : 'stale';
}

/** Credential rotation always requires an exact current workspace state ETag. */
export function assertWorkspaceAgentObservabilityCredentialRotationPrecondition(input: {
  currentEtag: string;
  ifMatch: string | null;
}): 'ok' | 'missing' | 'stale' {
  if (input.ifMatch === null) return 'missing';
  return input.ifMatch === input.currentEtag ? 'ok' : 'stale';
}

export function classifyWorkspaceAgentObservabilityMutation(
  authority: Pick<WorkspaceAgentObservabilityMutationAuthority, 'mode' | 'currentBinding'>,
  request: NormalizedWorkspaceAgentObservabilityPutRequest,
): WorkspaceAgentObservabilityMutationKind {
  const binding = authority.currentBinding;
  if (request.mode !== 'custom') {
    return {
      type: 'mode_only',
      selectionChanged: authority.mode !== request.mode,
      entersDisabled: authority.mode !== 'disabled' && request.mode === 'disabled',
      previousActiveBindingId:
        authority.mode === 'custom' && binding?.status === 'active' ? binding.id : null,
    };
  }
  if (authority.mode !== 'custom') return { type: 'initial' };
  if (binding === null) throw new WorkspaceAgentObservabilityMutationInvariantError();
  if (binding.status === 'active' && targetIdentityEquals(binding.target, request.target)) {
    return { type: 'same_target', bindingId: binding.id };
  }
  return {
    type: 'replacement',
    previousActiveBindingId: binding.status === 'active' ? binding.id : null,
  };
}

export function assertWorkspaceAgentObservabilityMutationRequest(
  request: NormalizedWorkspaceAgentObservabilityPutRequest,
  kind: WorkspaceAgentObservabilityMutationKind,
): void {
  if (request.mode !== 'custom') return;
  if (request.config.semanticProfile === 'langfuse' && request.target.externalProjectId === null) {
    throw new WorkspaceAgentObservabilityMutationRequestError();
  }
  if (kind.type === 'same_target') {
    if (request.credentials !== undefined)
      throw new WorkspaceAgentObservabilityMutationRequestError();
    return;
  }
  if (request.credentials === undefined)
    throw new WorkspaceAgentObservabilityMutationRequestError();
}

export function assertWorkspaceAgentObservabilityTargetAdmission(
  authority: WorkspaceAgentObservabilityMutationAuthority,
  request: NormalizedWorkspaceAgentObservabilityPutRequest,
): void {
  if (request.mode !== 'custom') return;
  if (
    !authority.allowedAdapters.includes(request.target.adapterType) ||
    !authority.allowedEndpointClasses.includes(request.target.endpointClass)
  ) {
    throw new WorkspaceAgentObservabilityMutationRequestError();
  }
}

/** Reject overflow before staging/reservation; no database integer error leaks through. */
export function assertWorkspaceAgentObservabilityMutationCapacity(
  authority: WorkspaceAgentObservabilityMutationAuthority,
  request: NormalizedWorkspaceAgentObservabilityPutRequest,
  kind: WorkspaceAgentObservabilityMutationKind,
): void {
  const selectionChanged =
    kind.type === 'initial' ||
    kind.type === 'replacement' ||
    (kind.type === 'mode_only' && kind.selectionChanged);
  const restrictionChanged = !isAgentObservabilityCaptureModeAtMost(
    authority.captureCeiling,
    request.captureCeiling,
  );
  const revocationChanged = kind.type === 'mode_only' && kind.entersDisabled;
  if (
    (selectionChanged && authority.selectionEpoch >= Number.MAX_SAFE_INTEGER) ||
    (restrictionChanged && authority.captureRestrictionEpoch >= Number.MAX_SAFE_INTEGER) ||
    (revocationChanged && authority.revocationEpoch >= Number.MAX_SAFE_INTEGER) ||
    (kind.type === 'same_target' &&
      (authority.currentBinding === null ||
        authority.currentBinding.credentialVersion === null ||
        authority.currentBinding.configVersion >= AGENT_OBSERVABILITY_CONFIG_VERSION_MAX))
  ) {
    throw new WorkspaceAgentObservabilityMutationInvariantError();
  }
}

export function encodeWorkspaceAgentObservabilityCredentials(input: {
  request: Extract<NormalizedWorkspaceAgentObservabilityPutRequest, { mode: 'custom' }>;
  candidateBindingId: string;
}): string {
  const credentials = input.request.credentials;
  if (credentials === undefined) throw new WorkspaceAgentObservabilityMutationRequestError();
  return encodeWorkspaceAgentObservabilityCredentialBundle({
    credentials,
    bindingId: input.candidateBindingId,
    credentialVersion: 1,
  });
}

export function encodeWorkspaceAgentObservabilityCredentialRotation(input: {
  request: NormalizedWorkspaceAgentObservabilityCredentialRotationRequest;
  bindingId: string;
  credentialVersion: number;
}): string {
  return encodeWorkspaceAgentObservabilityCredentialBundle({
    credentials: input.request.credentials,
    bindingId: input.bindingId,
    credentialVersion: input.credentialVersion,
  });
}

function encodeWorkspaceAgentObservabilityCredentialBundle(input: {
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
      auth: { type: 'basic', username: credentials.username, password: credentials.password },
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
      throw new WorkspaceAgentObservabilityMutationRequestError();
    }
    throw error;
  }
}

/**
 * Lock in Session-selection-compatible order. The state loader then reads the
 * same transaction snapshot only after every mutable authority row is held.
 */
export async function lockWorkspaceAgentObservabilityMutationAuthority(
  tx: DbTransaction,
  organizationId: string,
  workspaceId: string,
  options: { credentialHeadLock?: 'share' | 'update' } = {},
): Promise<WorkspaceAgentObservabilityMutationAuthority> {
  const platform = (
    await tx
      .select()
      .from(agentObservabilityPlatformPolicy)
      .where(eq(agentObservabilityPlatformPolicy.id, 'default'))
      .for('share')
      .limit(1)
  )[0];
  if (!platform) throw new WorkspaceAgentObservabilityMutationUnavailableError();

  const organization = (
    await tx
      .select({ id: organizations.id, status: organizations.status })
      .from(organizations)
      .where(eq(organizations.id, organizationId))
      .for('share')
      .limit(1)
  )[0];
  if (!organization || organization.status !== 'active') {
    throw new WorkspaceAgentObservabilityMutationNotFoundError();
  }

  const organizationSetting = (
    await tx
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId))
      .for('share')
      .limit(1)
  )[0];
  if (!organizationSetting || !hasValidOrganizationSetting(organizationSetting, organizationId)) {
    throw new WorkspaceAgentObservabilityMutationUnavailableError();
  }

  const workspace = (
    await tx
      .select({
        id: workspaces.id,
        organizationId: workspaces.organizationId,
        status: workspaces.status,
      })
      .from(workspaces)
      .where(and(eq(workspaces.id, workspaceId), eq(workspaces.organizationId, organizationId)))
      .for('update')
      .limit(1)
  )[0];
  if (!workspace || workspace.status !== 'active') {
    throw new WorkspaceAgentObservabilityMutationNotFoundError();
  }

  const workspaceSetting = (
    await tx
      .select()
      .from(agentObservabilityWorkspaceSettings)
      .where(
        and(
          eq(agentObservabilityWorkspaceSettings.organizationId, organizationId),
          eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId),
        ),
      )
      .for('update')
      .limit(1)
  )[0];
  if (
    !workspaceSetting ||
    !hasValidWorkspaceSetting(workspaceSetting, organizationId, workspaceId)
  ) {
    throw new WorkspaceAgentObservabilityMutationUnavailableError();
  }

  if (workspaceSetting.mode === 'custom') {
    await lockSelectedWorkspaceMutationBinding(tx, {
      bindingId: workspaceSetting.bindingId!,
      organizationId,
      workspaceId,
      scopeType: 'workspace',
      lock: 'update',
      credentialHeadLock: options.credentialHeadLock ?? 'share',
    });
  } else if (
    workspaceSetting.mode === 'inherit' &&
    organizationSetting.activeDefaultBindingId !== null
  ) {
    await lockSelectedWorkspaceMutationBinding(tx, {
      bindingId: organizationSetting.activeDefaultBindingId,
      organizationId,
      workspaceId: null,
      scopeType: 'organization',
      lock: 'share',
      credentialHeadLock: 'share',
    });
  }

  let state: LoadedWorkspaceAgentObservabilityState;
  try {
    state = await loadWorkspaceAgentObservabilityStateInTransaction({
      db: tx,
      organizationId,
      workspaceId,
    });
  } catch (error) {
    if (error instanceof AgentObservabilityStateNotFoundError) {
      throw new WorkspaceAgentObservabilityMutationNotFoundError();
    }
    if (error instanceof AgentObservabilityStateAvailabilityError) {
      throw new WorkspaceAgentObservabilityMutationUnavailableError();
    }
    throw error;
  }

  const configured = state.response.configured;
  if (configured.mode !== workspaceSetting.mode) {
    throw new WorkspaceAgentObservabilityMutationInvariantError();
  }
  const currentBinding = configured.binding
    ? {
        id: configured.binding.id,
        status: configured.binding.status,
        target: {
          adapterType: configured.binding.target.adapter_type,
          endpointKind: configured.binding.target.endpoint_kind,
          endpointClass: configured.binding.target.endpoint_class,
          endpoint: configured.binding.target.endpoint_url,
          externalProjectId: configured.binding.target.external_project_id,
        },
        configVersion: configured.binding.config.version,
        credentialVersion: configured.binding.credential.version,
      }
    : null;
  if (
    (workspaceSetting.mode === 'custom' && currentBinding === null) ||
    (workspaceSetting.mode !== 'custom' && currentBinding !== null)
  ) {
    throw new WorkspaceAgentObservabilityMutationInvariantError();
  }

  const [allowedAdapters, allowedEndpointClasses] = state.etagInput.platformPolicy;
  const [, , selectionEpoch, revocationEpoch, captureCeiling, captureRestrictionEpoch] =
    state.etagInput.workspaceSetting ?? [];
  if (
    !isAgentObservabilityCaptureMode(captureCeiling) ||
    !isNonnegativeSafeInteger(selectionEpoch) ||
    !isNonnegativeSafeInteger(revocationEpoch) ||
    !isNonnegativeSafeInteger(captureRestrictionEpoch)
  ) {
    throw new WorkspaceAgentObservabilityMutationUnavailableError();
  }
  return {
    organizationId,
    workspaceId,
    state,
    stateVersion: agentObservabilityStateEtag(state.etagInput),
    mode: workspaceSetting.mode as WorkspaceObservabilityMode,
    currentBinding,
    captureCeiling,
    captureRestrictionEpoch,
    selectionEpoch,
    revocationEpoch,
    allowedAdapters,
    allowedEndpointClasses,
  };
}

/**
 * Exact current custom credential head, read only after workspace authority
 * rows are locked. The opaque reference is transient rotation-executor data.
 */
export interface WorkspaceAgentObservabilityCredentialRotationHead {
  bindingId: string;
  configVersion: number;
  credentialVersion: number;
  secretRef: AgentObservabilitySecretReference;
}

export async function loadWorkspaceAgentObservabilityCredentialRotationHead(
  tx: DbTransaction,
  authority: WorkspaceAgentObservabilityMutationAuthority,
): Promise<WorkspaceAgentObservabilityCredentialRotationHead | null> {
  const configured = authority.state.response.configured;
  const binding = authority.currentBinding;

  // Inherit and explicit disable are ordinary non-eligible states. A missing
  // custom binding would be an authority invariant, not an implicit absence.
  if (configured.mode !== 'custom') {
    if (configured.binding !== null || binding !== null) {
      throw new WorkspaceAgentObservabilityMutationInvariantError();
    }
    return null;
  }
  if (
    binding === null ||
    configured.binding === null ||
    configured.binding.id !== binding.id ||
    configured.binding.scope !== 'workspace' ||
    configured.binding.organization_id !== authority.organizationId ||
    configured.binding.workspace_id !== authority.workspaceId ||
    configured.binding.status !== binding.status ||
    configured.binding.config.version !== binding.configVersion ||
    configured.binding.credential.version !== binding.credentialVersion ||
    configured.binding.credential.configured !== (binding.credentialVersion !== null)
  ) {
    throw new WorkspaceAgentObservabilityMutationInvariantError();
  }
  // Non-active or non-OTLP heads are ordinary ineligible states. Their
  // effective disabled reasons describe selection status, not corruption.
  if (binding.status !== 'active' || binding.target.adapterType !== 'otlp_http') {
    throw new WorkspaceAgentObservabilityCredentialRotationNotRotatableError();
  }
  if (!hasCoherentWorkspaceCredentialRotationEffectiveState(authority)) {
    throw new WorkspaceAgentObservabilityMutationInvariantError();
  }
  if (configured.binding.credential.configured === false && binding.credentialVersion === null) {
    // Resolver priority may surface platform or configuration availability
    // before credential_not_configured. Full configured/effective coherence
    // above proves this is still an ordinary absent head, never corruption.
    return null;
  }
  if (
    configured.binding.credential.configured !== true ||
    !isPositiveSafeInteger(binding.configVersion) ||
    binding.configVersion > AGENT_OBSERVABILITY_CONFIG_VERSION_MAX ||
    binding.credentialVersion === null ||
    !isPositiveSafeInteger(binding.credentialVersion) ||
    binding.credentialVersion >= AGENT_OBSERVABILITY_CREDENTIAL_VERSION_MAX
  ) {
    throw new WorkspaceAgentObservabilityMutationInvariantError();
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
  if (!head) throw new WorkspaceAgentObservabilityMutationInvariantError();
  if (
    head.bindingId !== binding.id ||
    head.organizationId !== authority.organizationId ||
    head.scopeType !== 'workspace' ||
    head.workspaceId !== authority.workspaceId ||
    head.credentialVersion !== binding.credentialVersion ||
    head.credentialVersion !== configured.binding.credential.version ||
    !isAgentObservabilitySecretReference(head.secretRef)
  ) {
    throw new WorkspaceAgentObservabilityMutationInvariantError();
  }
  return {
    bindingId: binding.id,
    configVersion: binding.configVersion,
    credentialVersion: head.credentialVersion,
    secretRef: head.secretRef,
  };
}

export async function applyWorkspaceAgentObservabilityMutation(input: {
  db: Pick<DbTransaction, 'insert' | 'update'>;
  authority: WorkspaceAgentObservabilityMutationAuthority;
  request: NormalizedWorkspaceAgentObservabilityPutRequest;
  kind: WorkspaceAgentObservabilityMutationKind;
  actor: string;
  now: Date;
  stagedActivation: AgentObservabilityStagedActivation | null;
}): Promise<{ applied: boolean }> {
  const nextCaptureRestrictionEpoch = captureRestrictionEpochAfterReplacement(
    input.authority.captureCeiling,
    input.request.captureCeiling,
    input.authority.captureRestrictionEpoch,
  );
  if (input.kind.type === 'mode_only') {
    if (input.request.mode === 'custom') return { applied: false };
    return applyModeOnlyReplacement({
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
  if (input.request.mode !== 'custom') return { applied: false };
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

/**
 * CAS one current workspace-custom credential head. Configuration, selection,
 * revocation/capture epochs, and Session pins intentionally remain untouched.
 */
export async function applyWorkspaceAgentObservabilityCredentialRotation(input: {
  db: Pick<DbTransaction, 'update'>;
  authority: WorkspaceAgentObservabilityMutationAuthority;
  head: WorkspaceAgentObservabilityCredentialRotationHead;
  actor: string;
  now: Date;
  stagedActivation: AgentObservabilityStagedActivation | null;
}): Promise<{ applied: boolean }> {
  const binding = input.authority.currentBinding;
  const activation = input.stagedActivation;
  if (
    input.authority.mode !== 'custom' ||
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

/** Exact current workspace targets an explicit disabled PUT may supersede. */
export function workspaceAgentObservabilityDisablePreemptionTargets(
  authority: WorkspaceAgentObservabilityMutationAuthority,
): WorkspaceAgentObservabilityPreemptionTarget[] {
  assertWorkspaceAgentObservabilityDisableAuthority(authority);
  const setting = {
    type: 'workspace_setting',
    organizationId: authority.organizationId,
    workspaceId: authority.workspaceId,
  } as const;
  if (authority.currentBinding === null) return [setting];
  return [
    setting,
    {
      type: 'binding',
      organizationId: authority.organizationId,
      workspaceId: authority.workspaceId,
      bindingId: authority.currentBinding.id,
      bindingScope: 'workspace',
    },
  ];
}

function assertWorkspaceAgentObservabilityDisableAuthority(
  authority: WorkspaceAgentObservabilityMutationAuthority,
): void {
  const configured = authority.state.response.configured;
  const binding = authority.currentBinding;
  if (
    configured.mode !== authority.mode ||
    configured.capture_ceiling !== authority.captureCeiling
  ) {
    throw new WorkspaceAgentObservabilityMutationInvariantError();
  }
  if (authority.mode !== 'custom') {
    if (configured.binding !== null || binding !== null) {
      throw new WorkspaceAgentObservabilityMutationInvariantError();
    }
    return;
  }
  if (
    configured.binding === null ||
    binding === null ||
    configured.binding.id !== binding.id ||
    configured.binding.scope !== 'workspace' ||
    configured.binding.organization_id !== authority.organizationId ||
    configured.binding.workspace_id !== authority.workspaceId ||
    configured.binding.status !== binding.status ||
    configured.binding.config.version !== binding.configVersion ||
    configured.binding.credential.version !== binding.credentialVersion ||
    configured.binding.credential.configured !== (binding.credentialVersion !== null)
  ) {
    throw new WorkspaceAgentObservabilityMutationInvariantError();
  }
}

/** Current configured/effective state must describe one exact custom binding. */
function hasCoherentWorkspaceCredentialRotationEffectiveState(
  authority: WorkspaceAgentObservabilityMutationAuthority,
): boolean {
  const configured = authority.state.response.configured;
  const effective = authority.state.response.effective;
  if (
    configured.mode !== 'custom' ||
    configured.binding === null ||
    effective.source !== 'workspace_custom' ||
    effective.binding === null
  ) {
    return false;
  }
  const configuredBinding = configured.binding;
  const effectiveBinding = effective.binding;
  if (
    configuredBinding.id !== effectiveBinding.id ||
    configuredBinding.scope !== effectiveBinding.scope ||
    configuredBinding.organization_id !== effectiveBinding.organization_id ||
    configuredBinding.workspace_id !== effectiveBinding.workspace_id ||
    configuredBinding.status !== effectiveBinding.status ||
    configuredBinding.target.adapter_type !== effectiveBinding.target.adapter_type ||
    configuredBinding.target.external_project_id !== effectiveBinding.target.external_project_id ||
    configuredBinding.target.endpoint_kind !== effectiveBinding.target.endpoint_kind ||
    configuredBinding.target.endpoint_class !== effectiveBinding.target.endpoint_class ||
    configuredBinding.target.endpoint_url !== effectiveBinding.target.endpoint_url ||
    configuredBinding.config.version !== effectiveBinding.config.version ||
    configuredBinding.config.semantic_profile !== effectiveBinding.config.semantic_profile ||
    configuredBinding.config.protocol !== effectiveBinding.config.protocol ||
    configuredBinding.config.compression !== effectiveBinding.config.compression ||
    configuredBinding.config.timeout_ms !== effectiveBinding.config.timeout_ms ||
    configuredBinding.config.environment !== effectiveBinding.config.environment ||
    configuredBinding.config.release !== effectiveBinding.config.release ||
    configuredBinding.config.capture_mode !== effectiveBinding.config.capture_mode ||
    configuredBinding.config.sample_rate !== effectiveBinding.config.sample_rate ||
    configuredBinding.config.config_schema_version !==
      effectiveBinding.config.config_schema_version ||
    configuredBinding.credential.configured !== effectiveBinding.credential.configured ||
    configuredBinding.credential.version !== effectiveBinding.credential.version ||
    configuredBinding.credential.key_hint !== effectiveBinding.credential.key_hint ||
    configuredBinding.credential.rotated_at !== effectiveBinding.credential.rotated_at
  ) {
    return false;
  }
  if (configuredBinding.status !== 'active') return false;
  const configurationIsValid = isValidAgentObservabilityAdapterConfiguration(
    configuredBinding.target.adapter_type,
    configuredBinding.target.endpoint_kind,
    configuredBinding.target.external_project_id,
    configuredBinding.config.semantic_profile,
    configuredBinding.config.protocol,
  );
  if (effective.status === 'enabled') {
    return (
      effective.disabled_reason === null &&
      configuredBinding.credential.configured === true &&
      configurationIsValid &&
      isAgentObservabilityCaptureModeAtMost(effective.capture_mode, configured.capture_ceiling) &&
      isAgentObservabilityCaptureModeAtMost(
        effective.capture_mode,
        configuredBinding.config.capture_mode,
      )
    );
  }
  if (effective.capture_mode !== 'metadata_only') return false;
  if (effective.disabled_reason === 'binding_configuration_invalid') {
    return !configurationIsValid;
  }
  if (
    effective.disabled_reason === 'platform_adapter_disallowed' ||
    effective.disabled_reason === 'platform_endpoint_class_disallowed'
  ) {
    // A historical/effective view cannot prove the policy snapshot that made
    // this active binding unavailable, so only require resolver-compatible
    // binding identity here.
    return true;
  }
  return (
    effective.disabled_reason === 'credential_not_configured' &&
    configurationIsValid &&
    configuredBinding.credential.configured === false &&
    configuredBinding.credential.version === null
  );
}

function targetIdentityEquals(
  left: WorkspaceBindingTarget,
  right: NormalizedWorkspaceTarget,
): boolean {
  return (
    left.adapterType === right.adapterType &&
    left.endpointKind === right.endpointKind &&
    left.endpointClass === right.endpointClass &&
    left.endpoint === right.endpoint &&
    left.externalProjectId === right.externalProjectId
  );
}

function normalizeWorkspaceCredentialRotationRequest(
  request: WorkspaceAgentObservabilityCredentialRotationRequest,
): NormalizedWorkspaceAgentObservabilityCredentialRotationRequest {
  const credentials = normalizeAgentObservabilityOtlpHttpCredentialInput(request.credentials);
  if (credentials === null) throw new WorkspaceAgentObservabilityMutationRequestError();
  return { credentials };
}

async function applyModeOnlyReplacement(input: {
  db: Pick<DbTransaction, 'update'>;
  authority: WorkspaceAgentObservabilityMutationAuthority;
  request: Exclude<NormalizedWorkspaceAgentObservabilityPutRequest, { mode: 'custom' }>;
  kind: Extract<WorkspaceAgentObservabilityMutationKind, { type: 'mode_only' }>;
  actor: string;
  now: Date;
  stagedActivation: AgentObservabilityStagedActivation | null;
  nextCaptureRestrictionEpoch: number;
}): Promise<{ applied: boolean }> {
  if (input.stagedActivation !== null) return { applied: false };
  if (input.kind.previousActiveBindingId !== null) {
    const drained = await input.db
      .update(agentObservabilityBindings)
      .set({ status: 'draining', updatedBy: input.actor, updatedAt: input.now })
      .where(
        and(
          eq(agentObservabilityBindings.id, input.kind.previousActiveBindingId),
          eq(agentObservabilityBindings.organizationId, input.authority.organizationId),
          eq(agentObservabilityBindings.workspaceId, input.authority.workspaceId),
          eq(agentObservabilityBindings.scopeType, 'workspace'),
          eq(agentObservabilityBindings.status, 'active'),
          eq(
            agentObservabilityBindings.currentVersion,
            input.authority.currentBinding?.configVersion ?? -1,
          ),
        ),
      )
      .returning({ id: agentObservabilityBindings.id });
    if (drained.length !== 1) return { applied: false };
  }
  return {
    applied: await updateWorkspaceSetting({
      db: input.db,
      authority: input.authority,
      nextMode: input.request.mode,
      nextBindingId: null,
      selectionChanged: input.kind.selectionChanged,
      revocationChanged: input.kind.entersDisabled,
      captureCeiling: input.request.captureCeiling,
      captureRestrictionEpoch: input.nextCaptureRestrictionEpoch,
      now: input.now,
    }),
  };
}

async function applySameTargetPolicyReplacement(input: {
  db: Pick<DbTransaction, 'insert' | 'update'>;
  authority: WorkspaceAgentObservabilityMutationAuthority;
  request: Extract<NormalizedWorkspaceAgentObservabilityPutRequest, { mode: 'custom' }>;
  kind: Extract<WorkspaceAgentObservabilityMutationKind, { type: 'same_target' }>;
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
    binding.configVersion >= AGENT_OBSERVABILITY_CONFIG_VERSION_MAX ||
    input.stagedActivation !== null
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
        eq(agentObservabilityBindings.workspaceId, input.authority.workspaceId),
        eq(agentObservabilityBindings.scopeType, 'workspace'),
        eq(agentObservabilityBindings.currentVersion, binding.configVersion),
        eq(agentObservabilityBindings.status, 'active'),
      ),
    )
    .returning({ id: agentObservabilityBindings.id });
  if (bindingUpdated.length !== 1) return { applied: false };
  return {
    applied: await updateWorkspaceSetting({
      db: input.db,
      authority: input.authority,
      nextMode: 'custom',
      nextBindingId: binding.id,
      selectionChanged: false,
      revocationChanged: false,
      captureCeiling: input.request.captureCeiling,
      captureRestrictionEpoch: input.nextCaptureRestrictionEpoch,
      now: input.now,
    }),
  };
}

async function applyTargetReplacement(input: {
  db: Pick<DbTransaction, 'insert' | 'update'>;
  authority: WorkspaceAgentObservabilityMutationAuthority;
  request: Extract<NormalizedWorkspaceAgentObservabilityPutRequest, { mode: 'custom' }>;
  kind: Exclude<WorkspaceAgentObservabilityMutationKind, { type: 'same_target' | 'mode_only' }>;
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
    workspaceId: input.authority.workspaceId,
    scopeType: 'workspace',
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
          eq(agentObservabilityBindings.workspaceId, input.authority.workspaceId),
          eq(agentObservabilityBindings.scopeType, 'workspace'),
          eq(agentObservabilityBindings.status, 'active'),
        ),
      )
      .returning({ id: agentObservabilityBindings.id });
    if (drained.length !== 1) return { applied: false };
  }
  return {
    applied: await updateWorkspaceSetting({
      db: input.db,
      authority: input.authority,
      nextMode: 'custom',
      nextBindingId: activation.candidateBindingId,
      selectionChanged: true,
      revocationChanged: false,
      captureCeiling: input.request.captureCeiling,
      captureRestrictionEpoch: input.nextCaptureRestrictionEpoch,
      now: input.now,
    }),
  };
}

async function updateWorkspaceSetting(input: {
  db: Pick<DbTransaction, 'update'>;
  authority: WorkspaceAgentObservabilityMutationAuthority;
  nextMode: WorkspaceObservabilityMode;
  nextBindingId: string | null;
  selectionChanged: boolean;
  revocationChanged: boolean;
  captureCeiling: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
  now: Date;
}): Promise<boolean> {
  const currentBindingId = input.authority.currentBinding?.id ?? null;
  const bindingCondition =
    currentBindingId === null
      ? isNull(agentObservabilityWorkspaceSettings.bindingId)
      : eq(agentObservabilityWorkspaceSettings.bindingId, currentBindingId);
  const updated = await input.db
    .update(agentObservabilityWorkspaceSettings)
    .set({
      mode: input.nextMode,
      bindingId: input.nextBindingId,
      selectionEpoch: input.authority.selectionEpoch + (input.selectionChanged ? 1 : 0),
      revocationEpoch: input.authority.revocationEpoch + (input.revocationChanged ? 1 : 0),
      captureCeiling: input.captureCeiling,
      captureRestrictionEpoch: input.captureRestrictionEpoch,
      updatedAt: input.now,
    })
    .where(
      and(
        eq(agentObservabilityWorkspaceSettings.organizationId, input.authority.organizationId),
        eq(agentObservabilityWorkspaceSettings.workspaceId, input.authority.workspaceId),
        eq(agentObservabilityWorkspaceSettings.mode, input.authority.mode),
        bindingCondition,
        eq(agentObservabilityWorkspaceSettings.selectionEpoch, input.authority.selectionEpoch),
        eq(agentObservabilityWorkspaceSettings.revocationEpoch, input.authority.revocationEpoch),
        eq(
          agentObservabilityWorkspaceSettings.captureRestrictionEpoch,
          input.authority.captureRestrictionEpoch,
        ),
        eq(agentObservabilityWorkspaceSettings.captureCeiling, input.authority.captureCeiling),
      ),
    )
    .returning({ workspaceId: agentObservabilityWorkspaceSettings.workspaceId });
  return updated.length === 1;
}

async function lockSelectedWorkspaceMutationBinding(
  tx: DbTransaction,
  input: {
    bindingId: string;
    organizationId: string;
    workspaceId: string | null;
    scopeType: 'organization' | 'workspace';
    lock: 'share' | 'update';
    credentialHeadLock: 'share' | 'update';
  },
): Promise<void> {
  const ownership = [
    eq(agentObservabilityBindings.id, input.bindingId),
    eq(agentObservabilityBindings.organizationId, input.organizationId),
    eq(agentObservabilityBindings.scopeType, input.scopeType),
  ];
  if (input.workspaceId === null) ownership.push(isNull(agentObservabilityBindings.workspaceId));
  else ownership.push(eq(agentObservabilityBindings.workspaceId, input.workspaceId));
  const binding = (
    await tx
      .select()
      .from(agentObservabilityBindings)
      .where(and(...ownership))
      .for(input.lock)
      .limit(1)
  )[0];
  if (!binding) throw new WorkspaceAgentObservabilityMutationInvariantError();
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
  if (!version) throw new WorkspaceAgentObservabilityMutationInvariantError();
  // Intentionally no `secret_ref` projection. The lock stabilizes the head;
  // state loading separately validates its public metadata without copying refs.
  await tx
    .select({ version: agentObservabilityBindingCredentials.credentialVersion })
    .from(agentObservabilityBindingCredentials)
    .where(eq(agentObservabilityBindingCredentials.bindingId, binding.id))
    .for(input.credentialHeadLock)
    .limit(1);
}

function hasValidOrganizationSetting(
  setting: typeof agentObservabilityOrganizationSettings.$inferSelect,
  organizationId: string,
): boolean {
  const validDefault =
    (setting.activeDefaultBindingId === null && setting.activeDefaultBindingScope === null) ||
    (isNonEmptyString(setting.activeDefaultBindingId) &&
      setting.activeDefaultBindingScope === 'organization');
  return (
    setting.organizationId === organizationId &&
    validDefault &&
    isAgentObservabilityCaptureMode(setting.captureCeiling) &&
    isNonnegativeSafeInteger(setting.selectionEpoch) &&
    isNonnegativeSafeInteger(setting.defaultRevocationEpoch) &&
    isNonnegativeSafeInteger(setting.organizationRevocationEpoch) &&
    isNonnegativeSafeInteger(setting.captureRestrictionEpoch)
  );
}

function hasValidWorkspaceSetting(
  setting: typeof agentObservabilityWorkspaceSettings.$inferSelect,
  organizationId: string,
  workspaceId: string,
): boolean {
  const validMode =
    (setting.mode === 'custom' && isNonEmptyString(setting.bindingId)) ||
    ((setting.mode === 'inherit' || setting.mode === 'disabled') && setting.bindingId === null);
  return (
    setting.organizationId === organizationId &&
    setting.workspaceId === workspaceId &&
    isWorkspaceObservabilityMode(setting.mode) &&
    validMode &&
    isAgentObservabilityCaptureMode(setting.captureCeiling) &&
    isNonnegativeSafeInteger(setting.selectionEpoch) &&
    isNonnegativeSafeInteger(setting.revocationEpoch) &&
    isNonnegativeSafeInteger(setting.captureRestrictionEpoch)
  );
}

function isNonnegativeSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isPositiveSafeInteger(value: unknown): value is number {
  return isNonnegativeSafeInteger(value) && value > 0;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}
