// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull, sql } from 'drizzle-orm';
import type {
  AgentObservabilityBindingView as ContractAgentObservabilityBindingView,
  OrganizationAgentObservabilityState as ContractOrganizationAgentObservabilityState,
  WorkspaceAgentObservabilityState as ContractWorkspaceAgentObservabilityState,
} from '../contracts/agent-observability.contract.js';
import {
  disabledReasonForBindingStatus,
  effectiveCaptureMode,
  isAgentObservabilityAdapter as isAdapter,
  isAgentObservabilityBindingStatus as isBindingStatus,
  isAgentObservabilityCaptureMode as isCaptureMode,
  isAgentObservabilityCompression as isCompression,
  isAgentObservabilityEndpointClass as isEndpointClass,
  isAgentObservabilityEndpointKind as isEndpointKind,
  isAgentObservabilityProtocol as isProtocol,
  isAgentObservabilitySemanticProfile as isSemanticProfile,
  isValidAgentObservabilityAdapterConfiguration,
  type AgentObservabilityAdapter,
  type AgentObservabilityBindingStatus,
  type AgentObservabilityCaptureMode,
  type AgentObservabilityCompression,
  type AgentObservabilityDisabledReason,
  type AgentObservabilityEndpointClass,
  type AgentObservabilityEndpointKind,
  type AgentObservabilityProtocol,
  type AgentObservabilitySemanticProfile,
  type WorkspaceObservabilityMode,
} from './agent-observability-policy.js';
import {
  isAgentObservabilityExternalProjectId,
  isAgentObservabilityKeyHint,
  isAgentObservabilityTimeoutMs,
  isCanonicalAgentObservabilityEndpoint,
} from './agent-observability-validation.js';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
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
import type { AgentObservabilityStateEtagInput } from './agent-observability-etag.js';

export type AgentObservabilitySource = 'none' | 'organization_default' | 'workspace_custom';
export {
  effectiveCaptureMode,
  type AgentObservabilityAdapter,
  type AgentObservabilityBindingStatus,
  type AgentObservabilityCaptureMode,
  type AgentObservabilityCompression,
  type AgentObservabilityDisabledReason,
  type AgentObservabilityEndpointClass,
  type AgentObservabilityEndpointKind,
  type AgentObservabilityProtocol,
  type AgentObservabilitySemanticProfile,
  type WorkspaceObservabilityMode,
};

type AgentObservabilityAvailabilityReason =
  | 'missing_platform_policy'
  | 'invalid_platform_policy'
  | 'missing_organization_setting'
  | 'invalid_organization_setting'
  | 'missing_workspace_setting'
  | 'invalid_workspace_setting'
  | 'missing_selected_binding'
  | 'invalid_selected_binding'
  | 'missing_selected_binding_version'
  | 'invalid_selected_binding_version'
  | 'invalid_selected_credential';

/**
 * One fail-closed error class for non-retryable-looking but retryable control
 * plane availability faults. Its public message intentionally carries no
 * database identifiers, endpoint values, or SecretStore references.
 */
export class AgentObservabilityStateAvailabilityError extends Error {
  override readonly name = 'AgentObservabilityStateAvailabilityError';

  constructor(readonly reason: AgentObservabilityAvailabilityReason) {
    super('agent observability state unavailable');
  }
}

/** Missing, archived, or cross-organization admin resources all map to 404. */
export class AgentObservabilityStateNotFoundError extends Error {
  override readonly name = 'AgentObservabilityStateNotFoundError';

  constructor() {
    super('agent observability state not found');
  }
}

export type AgentObservabilityBindingView = ContractAgentObservabilityBindingView;
export type OrganizationAgentObservabilityState = ContractOrganizationAgentObservabilityState;
export type WorkspaceAgentObservabilityState = ContractWorkspaceAgentObservabilityState;
type AgentObservabilityEffectiveState = OrganizationAgentObservabilityState['effective'];

export interface LoadedOrganizationAgentObservabilityState {
  response: OrganizationAgentObservabilityState;
  etagInput: AgentObservabilityStateEtagInput;
}

export interface LoadedWorkspaceAgentObservabilityState {
  response: WorkspaceAgentObservabilityState;
  etagInput: AgentObservabilityStateEtagInput;
}

export interface AgentObservabilityPlatformPolicyState {
  allowedAdapters: AgentObservabilityAdapter[];
  allowedEndpointClasses: AgentObservabilityEndpointClass[];
  maxCaptureMode: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
}

export interface AgentObservabilityOrganizationSettingState {
  organizationId: string;
  activeDefaultBindingId: string | null;
  activeDefaultBindingScope: 'organization' | null;
  selectionEpoch: number;
  defaultRevocationEpoch: number;
  organizationRevocationEpoch: number;
  captureCeiling: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
}

export interface AgentObservabilityWorkspaceSettingState {
  workspaceId: string;
  organizationId: string;
  mode: WorkspaceObservabilityMode;
  bindingId: string | null;
  selectionEpoch: number;
  revocationEpoch: number;
  captureCeiling: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
}

export interface AgentObservabilityBindingState {
  id: string;
  organizationId: string;
  workspaceId: string | null;
  scopeType: 'organization' | 'workspace';
  adapterType: AgentObservabilityAdapter;
  endpointKind: AgentObservabilityEndpointKind;
  endpointClass: AgentObservabilityEndpointClass;
  endpoint: string;
  externalProjectId: string | null;
  currentVersion: number;
  status: AgentObservabilityBindingStatus;
  revocationEpoch: number;
  archivedAt: string | null;
  version: {
    version: number;
    adapterType: AgentObservabilityAdapter;
    semanticProfile: AgentObservabilitySemanticProfile;
    protocol: AgentObservabilityProtocol;
    compression: AgentObservabilityCompression;
    timeoutMs: number;
    environment: string | null;
    release: string | null;
    captureMode: AgentObservabilityCaptureMode;
    sampleRate: number;
    configSchemaVersion: number;
  };
  credential:
    | {
        configured: true;
        credentialVersion: number;
        keyHint: string | null;
        rotatedAt: string;
      }
    | {
        configured: false;
        credentialVersion: null;
        keyHint: null;
        rotatedAt: null;
      };
}

export interface OrganizationAgentObservabilityResolutionInput {
  organizationStatus: 'active' | 'archived';
  organizationSetting: AgentObservabilityOrganizationSettingState;
  platformPolicy: AgentObservabilityPlatformPolicyState;
  organizationBinding: AgentObservabilityBindingState | null;
}

export interface WorkspaceAgentObservabilityResolutionInput extends OrganizationAgentObservabilityResolutionInput {
  workspaceStatus: 'active' | 'archived';
  workspaceSetting: AgentObservabilityWorkspaceSettingState;
  workspaceBinding: AgentObservabilityBindingState | null;
}

/** Resolve current organization configuration without selecting any fallback. */
export function resolveOrganizationAgentObservabilityState(
  input: OrganizationAgentObservabilityResolutionInput,
): OrganizationAgentObservabilityState {
  const { organizationSetting } = input;
  const configuredBinding = organizationSetting.activeDefaultBindingId
    ? requireOrganizationBinding(input)
    : null;
  const effective =
    input.organizationStatus !== 'active'
      ? disabledState('none', 'organization_archived')
      : configuredBinding
        ? resolveSelectedBinding(input, 'organization_default', configuredBinding)
        : disabledState('none', 'no_default_binding');

  return {
    type: 'agent_observability',
    scope: 'organization',
    organization_id: organizationSetting.organizationId,
    workspace_id: null,
    configured: {
      capture_ceiling: organizationSetting.captureCeiling,
      default_binding: configuredBinding ? bindingView(configuredBinding) : null,
    },
    effective,
  };
}

/** Resolve workspace precedence. A custom target never falls back to default. */
export function resolveWorkspaceAgentObservabilityState(
  input: WorkspaceAgentObservabilityResolutionInput,
): WorkspaceAgentObservabilityState {
  const { organizationSetting, workspaceSetting } = input;
  const configuredBinding =
    workspaceSetting.mode === 'custom' ? requireWorkspaceBinding(input) : null;
  let effective: AgentObservabilityEffectiveState;

  if (input.organizationStatus !== 'active') {
    effective = disabledState('none', 'organization_archived');
  } else if (input.workspaceStatus !== 'active') {
    effective = disabledState('none', 'workspace_archived');
  } else if (workspaceSetting.mode === 'disabled') {
    effective = disabledState('none', 'workspace_disabled');
  } else if (workspaceSetting.mode === 'custom') {
    effective = resolveSelectedBinding(input, 'workspace_custom', configuredBinding!);
  } else if (organizationSetting.activeDefaultBindingId === null) {
    effective = disabledState('none', 'no_organization_default');
  } else {
    effective = resolveSelectedBinding(
      input,
      'organization_default',
      requireOrganizationBinding(input),
    );
  }

  return {
    type: 'agent_observability',
    scope: 'workspace',
    organization_id: organizationSetting.organizationId,
    workspace_id: workspaceSetting.workspaceId,
    configured: {
      mode: workspaceSetting.mode,
      capture_ceiling: workspaceSetting.captureCeiling,
      binding: configuredBinding ? bindingView(configuredBinding) : null,
    },
    effective,
  };
}

/** Read a repeatable organization state snapshot from Registry authority. */
export async function loadOrganizationAgentObservabilityState(input: {
  db: DbClient;
  organizationId: string;
}): Promise<LoadedOrganizationAgentObservabilityState> {
  return input.db.transaction(
    (tx) =>
      loadOrganizationAgentObservabilityStateInTransaction({
        db: tx as DbTransaction,
        organizationId: input.organizationId,
      }),
    { isolationLevel: 'repeatable read' },
  );
}

/**
 * Same authority snapshot as the public GET loader, but never opens a nested
 * transaction. Mutation finalization calls this after its apply callback so
 * cache/replay state comes from committed-in-savepoint Registry rows only.
 */
export async function loadOrganizationAgentObservabilityStateInTransaction(input: {
  db: DbTransaction;
  organizationId: string;
}): Promise<LoadedOrganizationAgentObservabilityState> {
  const db = input.db as unknown as DbClient;
  const organization = await loadOrganization(db, input.organizationId);
  if (!organization || organization.status !== 'active') {
    throw new AgentObservabilityStateNotFoundError();
  }

  const platformPolicy = await loadPlatformPolicy(db);
  const organizationSetting = await loadOrganizationSetting(db, input.organizationId);
  const organizationBinding = organizationSetting.activeDefaultBindingId
    ? await loadSelectedBinding(db, {
        bindingId: organizationSetting.activeDefaultBindingId,
        organizationId: input.organizationId,
        scopeType: 'organization',
        workspaceId: null,
      })
    : null;
  const response = resolveOrganizationAgentObservabilityState({
    organizationStatus: organization.status,
    platformPolicy,
    organizationSetting,
    organizationBinding,
  });
  return {
    response,
    etagInput: etagInput({
      scope: 'organization',
      organizationId: input.organizationId,
      workspaceId: null,
      platformPolicy,
      organizationSetting,
      workspaceSetting: null,
      binding: organizationBinding,
    }),
  };
}

/** Read a repeatable workspace state snapshot through exact organization ownership. */
export async function loadWorkspaceAgentObservabilityState(input: {
  db: DbClient;
  organizationId: string;
  workspaceId: string;
}): Promise<LoadedWorkspaceAgentObservabilityState> {
  return input.db.transaction(
    (tx) =>
      loadWorkspaceAgentObservabilityStateInTransaction({
        db: tx as DbTransaction,
        organizationId: input.organizationId,
        workspaceId: input.workspaceId,
      }),
    { isolationLevel: 'repeatable read' },
  );
}

/** State loader for callers already holding the mutation authority transaction. */
export async function loadWorkspaceAgentObservabilityStateInTransaction(input: {
  db: DbTransaction;
  organizationId: string;
  workspaceId: string;
}): Promise<LoadedWorkspaceAgentObservabilityState> {
  const db = input.db as unknown as DbClient;
  const organization = await loadOrganization(db, input.organizationId);
  if (!organization || organization.status !== 'active') {
    throw new AgentObservabilityStateNotFoundError();
  }
  const workspace = await loadWorkspace(db, input.organizationId, input.workspaceId);
  if (!workspace || workspace.status !== 'active') {
    throw new AgentObservabilityStateNotFoundError();
  }

  const platformPolicy = await loadPlatformPolicy(db);
  const organizationSetting = await loadOrganizationSetting(db, input.organizationId);
  const workspaceSetting = await loadWorkspaceSetting(db, input.organizationId, input.workspaceId);
  const organizationBinding =
    workspaceSetting.mode === 'inherit' && organizationSetting.activeDefaultBindingId
      ? await loadSelectedBinding(db, {
          bindingId: organizationSetting.activeDefaultBindingId,
          organizationId: input.organizationId,
          scopeType: 'organization',
          workspaceId: null,
        })
      : null;
  const workspaceBinding =
    workspaceSetting.mode === 'custom' && workspaceSetting.bindingId
      ? await loadSelectedBinding(db, {
          bindingId: workspaceSetting.bindingId,
          organizationId: input.organizationId,
          scopeType: 'workspace',
          workspaceId: input.workspaceId,
        })
      : null;
  const response = resolveWorkspaceAgentObservabilityState({
    organizationStatus: organization.status,
    workspaceStatus: workspace.status,
    platformPolicy,
    organizationSetting,
    workspaceSetting,
    organizationBinding,
    workspaceBinding,
  });
  const selectedBinding = workspaceBinding ?? organizationBinding;
  return {
    response,
    etagInput: etagInput({
      scope: 'workspace',
      organizationId: input.organizationId,
      workspaceId: input.workspaceId,
      platformPolicy,
      organizationSetting,
      workspaceSetting,
      binding: selectedBinding,
    }),
  };
}

function resolveSelectedBinding(
  input: {
    platformPolicy: AgentObservabilityPlatformPolicyState;
    organizationSetting: AgentObservabilityOrganizationSettingState;
    workspaceSetting?: AgentObservabilityWorkspaceSettingState;
  },
  source: Exclude<AgentObservabilitySource, 'none'>,
  binding: AgentObservabilityBindingState,
): AgentObservabilityEffectiveState {
  const view = bindingView(binding);
  if (binding.status !== 'active') {
    return disabledState(source, disabledReasonForBindingStatus(binding.status), view);
  }
  if (!input.platformPolicy.allowedAdapters.includes(binding.adapterType)) {
    return disabledState(source, 'platform_adapter_disallowed', view);
  }
  if (!input.platformPolicy.allowedEndpointClasses.includes(binding.endpointClass)) {
    return disabledState(source, 'platform_endpoint_class_disallowed', view);
  }
  if (
    !isValidAgentObservabilityAdapterConfiguration(
      binding.adapterType,
      binding.endpointKind,
      binding.externalProjectId,
      binding.version.semanticProfile,
      binding.version.protocol,
    )
  ) {
    return disabledState(source, 'binding_configuration_invalid', view);
  }
  if (!binding.credential.configured) {
    return disabledState(source, 'credential_not_configured', view);
  }
  const captureMode = effectiveCaptureMode(
    binding.version.captureMode,
    input.platformPolicy.maxCaptureMode,
    input.organizationSetting.captureCeiling,
    input.workspaceSetting?.captureCeiling,
  );
  return {
    source,
    status: 'enabled',
    disabled_reason: null,
    capture_mode: captureMode,
    binding: view,
  };
}

function disabledState(
  source: AgentObservabilitySource,
  reason: AgentObservabilityDisabledReason,
  binding: AgentObservabilityBindingView | null = null,
): AgentObservabilityEffectiveState {
  return {
    source,
    status: 'disabled',
    disabled_reason: reason,
    capture_mode: 'metadata_only',
    binding,
  };
}

function requireOrganizationBinding(input: OrganizationAgentObservabilityResolutionInput) {
  const binding = input.organizationBinding;
  if (
    input.organizationSetting.activeDefaultBindingId === null ||
    input.organizationSetting.activeDefaultBindingScope !== 'organization' ||
    !binding ||
    binding.id !== input.organizationSetting.activeDefaultBindingId ||
    binding.organizationId !== input.organizationSetting.organizationId ||
    binding.scopeType !== 'organization' ||
    binding.workspaceId !== null
  ) {
    throw unavailable('invalid_organization_setting');
  }
  return binding;
}

function requireWorkspaceBinding(input: WorkspaceAgentObservabilityResolutionInput) {
  const binding = input.workspaceBinding;
  if (
    input.workspaceSetting.mode !== 'custom' ||
    input.workspaceSetting.bindingId === null ||
    !binding ||
    binding.id !== input.workspaceSetting.bindingId ||
    binding.organizationId !== input.workspaceSetting.organizationId ||
    binding.scopeType !== 'workspace' ||
    binding.workspaceId !== input.workspaceSetting.workspaceId
  ) {
    throw unavailable('invalid_workspace_setting');
  }
  return binding;
}

function bindingView(binding: AgentObservabilityBindingState): AgentObservabilityBindingView {
  return {
    id: binding.id,
    scope: binding.scopeType,
    organization_id: binding.organizationId,
    workspace_id: binding.workspaceId,
    target: {
      adapter_type: binding.adapterType,
      external_project_id: binding.externalProjectId,
      endpoint_kind: binding.endpointKind,
      endpoint_class: binding.endpointClass,
      endpoint_url: binding.endpoint,
    },
    status: binding.status,
    config: {
      version: binding.version.version,
      semantic_profile: binding.version.semanticProfile,
      protocol: binding.version.protocol,
      compression: binding.version.compression,
      timeout_ms: binding.version.timeoutMs,
      environment: binding.version.environment,
      release: binding.version.release,
      capture_mode: binding.version.captureMode,
      sample_rate: binding.version.sampleRate,
      config_schema_version: binding.version.configSchemaVersion,
    },
    credential: binding.credential.configured
      ? {
          configured: true,
          version: binding.credential.credentialVersion,
          key_hint: binding.credential.keyHint,
          rotated_at: binding.credential.rotatedAt,
        }
      : { configured: false, version: null, key_hint: null, rotated_at: null },
  };
}

function etagInput(input: {
  scope: 'organization' | 'workspace';
  organizationId: string;
  workspaceId: string | null;
  platformPolicy: AgentObservabilityPlatformPolicyState;
  organizationSetting: AgentObservabilityOrganizationSettingState;
  workspaceSetting: AgentObservabilityWorkspaceSettingState | null;
  binding: AgentObservabilityBindingState | null;
}): AgentObservabilityStateEtagInput {
  const { platformPolicy, organizationSetting, workspaceSetting, binding } = input;
  return {
    scope: input.scope,
    organizationId: input.organizationId,
    workspaceId: input.workspaceId,
    platformPolicy: [
      platformPolicy.allowedAdapters,
      platformPolicy.allowedEndpointClasses,
      platformPolicy.maxCaptureMode,
      platformPolicy.captureRestrictionEpoch,
    ],
    organizationSetting: [
      organizationSetting.activeDefaultBindingId,
      organizationSetting.activeDefaultBindingScope,
      organizationSetting.selectionEpoch,
      organizationSetting.defaultRevocationEpoch,
      organizationSetting.organizationRevocationEpoch,
      organizationSetting.captureCeiling,
      organizationSetting.captureRestrictionEpoch,
    ],
    workspaceSetting: workspaceSetting
      ? [
          workspaceSetting.mode,
          workspaceSetting.bindingId,
          workspaceSetting.selectionEpoch,
          workspaceSetting.revocationEpoch,
          workspaceSetting.captureCeiling,
          workspaceSetting.captureRestrictionEpoch,
        ]
      : null,
    binding: binding
      ? [
          binding.id,
          binding.organizationId,
          binding.workspaceId,
          binding.scopeType,
          binding.adapterType,
          binding.endpointKind,
          binding.endpointClass,
          binding.endpoint,
          binding.externalProjectId,
          binding.currentVersion,
          binding.status,
          binding.revocationEpoch,
          binding.archivedAt,
        ]
      : null,
    version: binding
      ? [
          binding.version.version,
          binding.version.adapterType,
          binding.version.semanticProfile,
          binding.version.protocol,
          binding.version.compression,
          binding.version.timeoutMs,
          binding.version.environment,
          binding.version.release,
          binding.version.captureMode,
          binding.version.sampleRate,
          binding.version.configSchemaVersion,
        ]
      : null,
    credential: binding
      ? binding.credential.configured
        ? [
            true,
            binding.credential.credentialVersion,
            binding.credential.keyHint,
            binding.credential.rotatedAt,
          ]
        : [false, null, null, null]
      : null,
  };
}

async function loadOrganization(db: DbClient, organizationId: string) {
  return (
    await db
      .select({ id: organizations.id, status: organizations.status })
      .from(organizations)
      .where(eq(organizations.id, organizationId))
      .limit(1)
  )[0];
}

async function loadWorkspace(db: DbClient, organizationId: string, workspaceId: string) {
  return (
    await db
      .select({ id: workspaces.id, status: workspaces.status })
      .from(workspaces)
      .where(and(eq(workspaces.id, workspaceId), eq(workspaces.organizationId, organizationId)))
      .limit(1)
  )[0];
}

async function loadPlatformPolicy(db: DbClient): Promise<AgentObservabilityPlatformPolicyState> {
  const row = (
    await db
      .select()
      .from(agentObservabilityPlatformPolicy)
      .where(eq(agentObservabilityPlatformPolicy.id, 'default'))
      .limit(1)
  )[0];
  if (!row) throw unavailable('missing_platform_policy');
  if (
    !arrayEvery(row.allowedAdapters, isAdapter) ||
    row.allowedAdapters.length === 0 ||
    !arrayEvery(row.allowedEndpointClasses, isEndpointClass) ||
    row.allowedEndpointClasses.length === 0 ||
    !isCaptureMode(row.maxCaptureMode) ||
    !isNonnegativeSafeInteger(row.captureRestrictionEpoch)
  ) {
    throw unavailable('invalid_platform_policy');
  }
  return {
    allowedAdapters: row.allowedAdapters,
    allowedEndpointClasses: row.allowedEndpointClasses,
    maxCaptureMode: row.maxCaptureMode,
    captureRestrictionEpoch: row.captureRestrictionEpoch,
  };
}

async function loadOrganizationSetting(
  db: DbClient,
  organizationId: string,
): Promise<AgentObservabilityOrganizationSettingState> {
  const row = (
    await db
      .select()
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId))
      .limit(1)
  )[0];
  if (!row) throw unavailable('missing_organization_setting');
  const validDefault =
    (row.activeDefaultBindingId === null && row.activeDefaultBindingScope === null) ||
    (isNonEmptyString(row.activeDefaultBindingId) &&
      row.activeDefaultBindingScope === 'organization');
  if (
    row.organizationId !== organizationId ||
    !validDefault ||
    !isCaptureMode(row.captureCeiling) ||
    !isNonnegativeSafeInteger(row.selectionEpoch) ||
    !isNonnegativeSafeInteger(row.defaultRevocationEpoch) ||
    !isNonnegativeSafeInteger(row.organizationRevocationEpoch) ||
    !isNonnegativeSafeInteger(row.captureRestrictionEpoch)
  ) {
    throw unavailable('invalid_organization_setting');
  }
  return {
    organizationId: row.organizationId,
    activeDefaultBindingId: row.activeDefaultBindingId,
    activeDefaultBindingScope: row.activeDefaultBindingScope as 'organization' | null,
    selectionEpoch: row.selectionEpoch,
    defaultRevocationEpoch: row.defaultRevocationEpoch,
    organizationRevocationEpoch: row.organizationRevocationEpoch,
    captureCeiling: row.captureCeiling,
    captureRestrictionEpoch: row.captureRestrictionEpoch,
  };
}

async function loadWorkspaceSetting(
  db: DbClient,
  organizationId: string,
  workspaceId: string,
): Promise<AgentObservabilityWorkspaceSettingState> {
  const row = (
    await db
      .select()
      .from(agentObservabilityWorkspaceSettings)
      .where(
        and(
          eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId),
          eq(agentObservabilityWorkspaceSettings.organizationId, organizationId),
        ),
      )
      .limit(1)
  )[0];
  if (!row) throw unavailable('missing_workspace_setting');
  const validMode =
    (row.mode === 'custom' && isNonEmptyString(row.bindingId)) ||
    ((row.mode === 'inherit' || row.mode === 'disabled') && row.bindingId === null);
  if (
    row.organizationId !== organizationId ||
    row.workspaceId !== workspaceId ||
    !validMode ||
    !isCaptureMode(row.captureCeiling) ||
    !isNonnegativeSafeInteger(row.selectionEpoch) ||
    !isNonnegativeSafeInteger(row.revocationEpoch) ||
    !isNonnegativeSafeInteger(row.captureRestrictionEpoch)
  ) {
    throw unavailable('invalid_workspace_setting');
  }
  return {
    workspaceId: row.workspaceId,
    organizationId: row.organizationId,
    mode: row.mode as WorkspaceObservabilityMode,
    bindingId: row.bindingId,
    selectionEpoch: row.selectionEpoch,
    revocationEpoch: row.revocationEpoch,
    captureCeiling: row.captureCeiling,
    captureRestrictionEpoch: row.captureRestrictionEpoch,
  };
}

async function loadSelectedBinding(
  db: DbClient,
  input: {
    bindingId: string;
    organizationId: string;
    scopeType: 'organization' | 'workspace';
    workspaceId: string | null;
  },
): Promise<AgentObservabilityBindingState> {
  const ownership = [
    eq(agentObservabilityBindings.id, input.bindingId),
    eq(agentObservabilityBindings.organizationId, input.organizationId),
    eq(agentObservabilityBindings.scopeType, input.scopeType),
  ];
  if (input.workspaceId === null) ownership.push(isNull(agentObservabilityBindings.workspaceId));
  else ownership.push(eq(agentObservabilityBindings.workspaceId, input.workspaceId));
  const binding = (
    await db
      .select()
      .from(agentObservabilityBindings)
      .where(and(...ownership))
      .limit(1)
  )[0];
  if (!binding) throw unavailable('missing_selected_binding');

  const version = (
    await db
      .select()
      .from(agentObservabilityBindingVersions)
      .where(
        and(
          eq(agentObservabilityBindingVersions.bindingId, binding.id),
          eq(agentObservabilityBindingVersions.version, binding.currentVersion),
          eq(agentObservabilityBindingVersions.adapterType, binding.adapterType),
        ),
      )
      .limit(1)
  )[0];
  if (!version) throw unavailable('missing_selected_binding_version');

  // Do not select `secret_ref`: this endpoint needs head existence and public
  // metadata only. SQL checks reference shape without copying it into memory.
  const credential = (
    await db
      .select({
        credentialVersion: agentObservabilityBindingCredentials.credentialVersion,
        keyHint: agentObservabilityBindingCredentials.keyHint,
        rotatedAt: agentObservabilityBindingCredentials.rotatedAt,
        referenceValid: sql<boolean>`char_length(btrim(${agentObservabilityBindingCredentials.secretRef})) > 0`,
      })
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, binding.id))
      .limit(1)
  )[0];

  return validateSelectedBinding(binding, version, credential, input);
}

function validateSelectedBinding(
  binding: typeof agentObservabilityBindings.$inferSelect,
  version: typeof agentObservabilityBindingVersions.$inferSelect,
  credential:
    | {
        credentialVersion: number;
        keyHint: string | null;
        rotatedAt: Date;
        referenceValid: boolean;
      }
    | undefined,
  expected: {
    bindingId: string;
    organizationId: string;
    scopeType: 'organization' | 'workspace';
    workspaceId: string | null;
  },
): AgentObservabilityBindingState {
  const archivedAt = nullableIsoTimestamp(binding.archivedAt);
  if (
    binding.id !== expected.bindingId ||
    binding.organizationId !== expected.organizationId ||
    binding.scopeType !== expected.scopeType ||
    binding.workspaceId !== expected.workspaceId ||
    !isNonEmptyString(binding.id) ||
    !isAdapter(binding.adapterType) ||
    !isEndpointKind(binding.endpointKind) ||
    !isEndpointClass(binding.endpointClass) ||
    !isCanonicalAgentObservabilityEndpoint(binding.endpoint) ||
    !isAgentObservabilityExternalProjectId(binding.externalProjectId) ||
    !isBindingStatus(binding.status) ||
    !isPositiveSafeInteger(binding.currentVersion) ||
    !isNonnegativeSafeInteger(binding.revocationEpoch) ||
    (binding.status === 'archived') !== (archivedAt !== null)
  ) {
    throw unavailable('invalid_selected_binding');
  }
  const sampleRate = numberInRange(version.sampleRate, 0, 1);
  if (
    version.bindingId !== binding.id ||
    version.version !== binding.currentVersion ||
    version.adapterType !== binding.adapterType ||
    !isPositiveSafeInteger(version.version) ||
    !isSemanticProfile(version.semanticProfile) ||
    !isProtocol(version.protocol) ||
    !isCompression(version.compression) ||
    !isAgentObservabilityTimeoutMs(version.timeoutMs) ||
    sampleRate === null ||
    !isCaptureMode(version.captureMode) ||
    !isPositiveSafeInteger(version.configSchemaVersion) ||
    !isNullableString(version.environment) ||
    !isNullableString(version.release)
  ) {
    throw unavailable('invalid_selected_binding_version');
  }

  let resolvedCredential: AgentObservabilityBindingState['credential'];
  if (!credential) {
    resolvedCredential = {
      configured: false,
      credentialVersion: null,
      keyHint: null,
      rotatedAt: null,
    };
  } else {
    const rotatedAt = nullableIsoTimestamp(credential.rotatedAt);
    if (
      credential.referenceValid !== true ||
      !isPositiveSafeInteger(credential.credentialVersion) ||
      !isAgentObservabilityKeyHint(credential.keyHint) ||
      rotatedAt === null
    ) {
      throw unavailable('invalid_selected_credential');
    }
    resolvedCredential = {
      configured: true,
      credentialVersion: credential.credentialVersion,
      keyHint: credential.keyHint,
      rotatedAt,
    };
  }

  return {
    id: binding.id,
    organizationId: binding.organizationId,
    workspaceId: binding.workspaceId,
    scopeType: binding.scopeType,
    adapterType: binding.adapterType,
    endpointKind: binding.endpointKind,
    endpointClass: binding.endpointClass,
    endpoint: binding.endpoint,
    externalProjectId: binding.externalProjectId,
    currentVersion: binding.currentVersion,
    status: binding.status,
    revocationEpoch: binding.revocationEpoch,
    archivedAt,
    version: {
      version: version.version,
      adapterType: version.adapterType,
      semanticProfile: version.semanticProfile,
      protocol: version.protocol,
      compression: version.compression,
      timeoutMs: version.timeoutMs,
      environment: version.environment,
      release: version.release,
      captureMode: version.captureMode,
      sampleRate,
      configSchemaVersion: version.configSchemaVersion,
    },
    credential: resolvedCredential,
  };
}

function nullableIsoTimestamp(value: unknown): string | null {
  if (value === null) return null;
  if (!(value instanceof Date) || !Number.isFinite(value.getTime())) return null;
  return value.toISOString();
}

function numberInRange(value: unknown, min: number, max: number): number | null {
  const number =
    typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
  return Number.isFinite(number) && number >= min && number <= max ? number : null;
}

function isNonnegativeSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === 'string';
}

function arrayEvery<T>(value: unknown, predicate: (item: unknown) => item is T): value is T[] {
  return Array.isArray(value) && value.every(predicate);
}

function unavailable(
  reason: AgentObservabilityAvailabilityReason,
): AgentObservabilityStateAvailabilityError {
  return new AgentObservabilityStateAvailabilityError(reason);
}
