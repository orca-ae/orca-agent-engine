// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull, sql } from 'drizzle-orm';
import {
  disabledReasonForBindingStatus,
  effectiveCaptureMode,
  isAgentObservabilityAdapter,
  isAgentObservabilityBindingStatus,
  isAgentObservabilityCaptureMode,
  isAgentObservabilityCompression,
  isAgentObservabilityEndpointClass,
  isAgentObservabilityEndpointKind,
  isAgentObservabilityProtocol,
  isAgentObservabilitySemanticProfile,
  isValidAgentObservabilityAdapterConfiguration,
  isWorkspaceObservabilityMode,
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
  isAgentObservabilityTimeoutMs,
  isCanonicalAgentObservabilityEndpoint,
} from './agent-observability-validation.js';
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

type SessionObservabilitySelectionAvailabilityReason =
  | 'missing_platform_policy'
  | 'invalid_platform_policy'
  | 'missing_organization_parent'
  | 'invalid_organization_parent'
  | 'missing_organization_setting'
  | 'invalid_organization_setting'
  | 'missing_workspace_parent'
  | 'invalid_workspace_parent'
  | 'missing_workspace_setting'
  | 'invalid_workspace_setting'
  | 'missing_selected_binding'
  | 'invalid_selected_binding'
  | 'missing_selected_binding_version'
  | 'invalid_selected_binding_version';

export type SessionObservabilitySelectionDisabledReason = Extract<
  AgentObservabilityDisabledReason,
  | 'no_organization_default'
  | 'workspace_disabled'
  | `binding_${string}`
  | 'platform_adapter_disallowed'
  | 'platform_endpoint_class_disallowed'
  | 'credential_not_configured'
>;

/** Retryable fail-closed control-plane availability or corruption outcome. */
export class SessionObservabilitySelectionAvailabilityError extends Error {
  override readonly name = 'SessionObservabilitySelectionAvailabilityError';

  constructor(readonly reason: SessionObservabilitySelectionAvailabilityReason) {
    super('session observability selection unavailable');
  }
}

/** Active Session creation must stop when an authoritative parent is inactive. */
export class SessionObservabilitySelectionResourceUnavailableError extends Error {
  override readonly name = 'SessionObservabilitySelectionResourceUnavailableError';

  constructor(readonly resource: 'organization' | 'workspace') {
    super('session observability resource unavailable');
  }
}

/**
 * Authority snapshots selected before Session insertion. The later insert
 * builder supplies `sessionRevocationEpoch: 0`, its own fresh lifecycle state.
 */
export interface SessionObservabilitySelectionEpochs {
  organizationSelectionEpoch: number;
  workspaceSelectionEpoch: number;
  organizationDefaultRevocationEpoch: number;
  organizationRevocationEpoch: number;
  workspaceRevocationEpoch: number;
  bindingRevocationEpoch: number;
  platformCaptureRestrictionEpoch: number;
  organizationCaptureRestrictionEpoch: number;
  workspaceCaptureRestrictionEpoch: number;
}

interface SessionObservabilitySelectionBase extends SessionObservabilitySelectionEpochs {
  organizationId: string;
  workspaceId: string;
}

/** Shape directly consumable by a later `session_observability_bindings` insert. */
export interface ActiveSessionObservabilitySelection extends SessionObservabilitySelectionBase {
  selectionSource: 'organization_default' | 'workspace_custom';
  status: 'active';
  bindingId: string;
  bindingVersion: number;
  bindingScope: 'organization' | 'workspace';
  bindingWorkspaceId: string | null;
  effectiveCaptureMode: AgentObservabilityCaptureMode;
  /** Internal diagnostic only. Never expose this selection over an API. */
  disabledReason: null;
}

/** Disabled selections intentionally carry no target identity or credential state. */
export interface DisabledSessionObservabilitySelection extends SessionObservabilitySelectionBase {
  selectionSource: 'disabled';
  status: 'disabled';
  bindingId: null;
  bindingVersion: null;
  bindingScope: null;
  bindingWorkspaceId: null;
  effectiveCaptureMode: 'metadata_only';
  /** Internal diagnostic only. Never expose this selection over an API. */
  disabledReason: SessionObservabilitySelectionDisabledReason;
}

export type SessionObservabilitySelection =
  | ActiveSessionObservabilitySelection
  | DisabledSessionObservabilitySelection;

export interface SessionObservabilityPlatformPolicy {
  allowedAdapters: AgentObservabilityAdapter[];
  allowedEndpointClasses: AgentObservabilityEndpointClass[];
  maxCaptureMode: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
}

export interface SessionObservabilityOrganizationSetting {
  organizationId: string;
  activeDefaultBindingId: string | null;
  activeDefaultBindingScope: 'organization' | null;
  selectionEpoch: number;
  defaultRevocationEpoch: number;
  organizationRevocationEpoch: number;
  captureCeiling: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
}

export interface SessionObservabilityWorkspaceSetting {
  organizationId: string;
  workspaceId: string;
  mode: WorkspaceObservabilityMode;
  bindingId: string | null;
  selectionEpoch: number;
  revocationEpoch: number;
  captureCeiling: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
}

/** Non-secret current binding state used only by pure Session selection. */
export interface SessionObservabilitySelectedBinding {
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
  /** Credential-head existence only. No ref or generation is present. */
  credentialConfigured: boolean;
}

export interface SessionObservabilitySelectionPolicyInput {
  organization: { id: string; status: 'active' | 'archived' };
  workspace: { id: string; organizationId: string; status: 'active' | 'archived' };
  platformPolicy: SessionObservabilityPlatformPolicy;
  organizationSetting: SessionObservabilityOrganizationSetting;
  workspaceSetting: SessionObservabilityWorkspaceSetting;
  organizationBinding: SessionObservabilitySelectedBinding | null;
  workspaceBinding: SessionObservabilitySelectedBinding | null;
}

/**
 * Pure precedence and eligibility policy. Call only with rows locked by
 * `selectSessionObservabilityBindingInTransaction` or equivalent authority.
 */
export function resolveSessionObservabilitySelection(
  input: SessionObservabilitySelectionPolicyInput,
): SessionObservabilitySelection {
  assertParentOwnership(input);
  if (input.organization.status !== 'active') {
    throw new SessionObservabilitySelectionResourceUnavailableError('organization');
  }
  if (input.workspace.status !== 'active') {
    throw new SessionObservabilitySelectionResourceUnavailableError('workspace');
  }

  const epochs = selectionEpochs(input);
  if (input.workspaceSetting.mode === 'disabled') {
    return disabledSelection(input, epochs, 'workspace_disabled');
  }

  if (input.workspaceSetting.mode === 'custom') {
    return resolveSelectedBinding(
      input,
      epochs,
      'workspace_custom',
      requireWorkspaceBinding(input),
    );
  }

  if (input.organizationSetting.activeDefaultBindingId === null) {
    return disabledSelection(input, epochs, 'no_organization_default');
  }

  return resolveSelectedBinding(
    input,
    epochs,
    'organization_default',
    requireOrganizationBinding(input),
  );
}

/**
 * Lock and select one non-secret observability pin inside an existing Session
 * creation transaction. `workspaceId` is re-authorized under locks; its first
 * lookup is advisory and only discovers the candidate organization.
 *
 * Admin snapshots own repeatable-read transactions; Session selection uses a
 * caller-owned transaction for lock orchestration. Shared policy/validation is
 * the common layer, not the loaders.
 */
export async function selectSessionObservabilityBindingInTransaction(
  tx: DbTransaction,
  workspaceId: string,
): Promise<SessionObservabilitySelection> {
  const preliminaryWorkspace = (
    await tx
      .select({ organizationId: workspaces.organizationId })
      .from(workspaces)
      .where(eq(workspaces.id, workspaceId))
      .limit(1)
  )[0];
  if (!preliminaryWorkspace) throw unavailable('missing_workspace_parent');

  // Lock order is authority order. Do not add Session/Skill/Vault locks here:
  // this helper must run before later Session-create resolution layers.
  const platformPolicy = await loadLockedPlatformPolicy(tx);
  const organization = await loadLockedOrganization(tx, preliminaryWorkspace.organizationId);
  const organizationSetting = await loadLockedOrganizationSetting(tx, organization.id);
  const workspace = await loadLockedWorkspace(tx, organization.id, workspaceId);
  const workspaceSetting = await loadLockedWorkspaceSetting(tx, organization.id, workspace.id);

  let organizationBinding: SessionObservabilitySelectedBinding | null = null;
  let workspaceBinding: SessionObservabilitySelectedBinding | null = null;
  if (workspaceSetting.mode === 'custom') {
    workspaceBinding = await loadLockedSelectedBinding(tx, {
      bindingId: workspaceSetting.bindingId!,
      organizationId: organization.id,
      scopeType: 'workspace',
      workspaceId: workspace.id,
    });
  } else if (
    workspaceSetting.mode === 'inherit' &&
    organizationSetting.activeDefaultBindingId !== null
  ) {
    organizationBinding = await loadLockedSelectedBinding(tx, {
      bindingId: organizationSetting.activeDefaultBindingId,
      organizationId: organization.id,
      scopeType: 'organization',
      workspaceId: null,
    });
  }

  return resolveSessionObservabilitySelection({
    organization,
    workspace,
    platformPolicy,
    organizationSetting,
    workspaceSetting,
    organizationBinding,
    workspaceBinding,
  });
}

function resolveSelectedBinding(
  input: SessionObservabilitySelectionPolicyInput,
  epochs: SessionObservabilitySelectionEpochs,
  source: 'organization_default' | 'workspace_custom',
  binding: SessionObservabilitySelectedBinding,
): SessionObservabilitySelection {
  assertCurrentBindingVersion(binding);
  if (binding.status !== 'active') {
    return disabledSelection(input, epochs, disabledReasonForBindingStatus(binding.status));
  }
  if (!input.platformPolicy.allowedAdapters.includes(binding.adapterType)) {
    return disabledSelection(input, epochs, 'platform_adapter_disallowed');
  }
  if (!input.platformPolicy.allowedEndpointClasses.includes(binding.endpointClass)) {
    return disabledSelection(input, epochs, 'platform_endpoint_class_disallowed');
  }
  if (!hasValidBindingConfiguration(binding)) {
    return disabledSelection(input, epochs, 'binding_configuration_invalid');
  }
  if (!binding.credentialConfigured) {
    return disabledSelection(input, epochs, 'credential_not_configured');
  }
  return {
    organizationId: input.organization.id,
    workspaceId: input.workspace.id,
    selectionSource: source,
    status: 'active',
    bindingId: binding.id,
    bindingVersion: binding.currentVersion,
    bindingScope: binding.scopeType,
    bindingWorkspaceId: binding.workspaceId,
    ...epochs,
    bindingRevocationEpoch: binding.revocationEpoch,
    effectiveCaptureMode: effectiveCaptureMode(
      binding.version.captureMode,
      input.platformPolicy.maxCaptureMode,
      input.organizationSetting.captureCeiling,
      input.workspaceSetting.captureCeiling,
    ),
    disabledReason: null,
  };
}

function assertCurrentBindingVersion(binding: SessionObservabilitySelectedBinding): void {
  if (
    !isPositiveSafeInteger(binding.currentVersion) ||
    !isPositiveSafeInteger(binding.version.version) ||
    binding.currentVersion !== binding.version.version
  ) {
    throw unavailable('invalid_selected_binding_version');
  }
}

function disabledSelection(
  input: SessionObservabilitySelectionPolicyInput,
  epochs: SessionObservabilitySelectionEpochs,
  reason: SessionObservabilitySelectionDisabledReason,
): DisabledSessionObservabilitySelection {
  return {
    organizationId: input.organization.id,
    workspaceId: input.workspace.id,
    selectionSource: 'disabled',
    status: 'disabled',
    bindingId: null,
    bindingVersion: null,
    bindingScope: null,
    bindingWorkspaceId: null,
    ...epochs,
    bindingRevocationEpoch: 0,
    effectiveCaptureMode: 'metadata_only',
    disabledReason: reason,
  };
}

function selectionEpochs(
  input: SessionObservabilitySelectionPolicyInput,
): SessionObservabilitySelectionEpochs {
  return {
    organizationSelectionEpoch: input.organizationSetting.selectionEpoch,
    workspaceSelectionEpoch: input.workspaceSetting.selectionEpoch,
    organizationDefaultRevocationEpoch: input.organizationSetting.defaultRevocationEpoch,
    organizationRevocationEpoch: input.organizationSetting.organizationRevocationEpoch,
    workspaceRevocationEpoch: input.workspaceSetting.revocationEpoch,
    bindingRevocationEpoch: 0,
    platformCaptureRestrictionEpoch: input.platformPolicy.captureRestrictionEpoch,
    organizationCaptureRestrictionEpoch: input.organizationSetting.captureRestrictionEpoch,
    workspaceCaptureRestrictionEpoch: input.workspaceSetting.captureRestrictionEpoch,
  };
}

function requireOrganizationBinding(
  input: SessionObservabilitySelectionPolicyInput,
): SessionObservabilitySelectedBinding {
  const binding = input.organizationBinding;
  if (
    input.organizationSetting.activeDefaultBindingId === null ||
    input.organizationSetting.activeDefaultBindingScope !== 'organization' ||
    !binding ||
    binding.id !== input.organizationSetting.activeDefaultBindingId ||
    binding.organizationId !== input.organization.id ||
    binding.scopeType !== 'organization' ||
    binding.workspaceId !== null
  ) {
    throw unavailable('invalid_organization_setting');
  }
  return binding;
}

function requireWorkspaceBinding(
  input: SessionObservabilitySelectionPolicyInput,
): SessionObservabilitySelectedBinding {
  const binding = input.workspaceBinding;
  if (
    input.workspaceSetting.mode !== 'custom' ||
    input.workspaceSetting.bindingId === null ||
    !binding ||
    binding.id !== input.workspaceSetting.bindingId ||
    binding.organizationId !== input.organization.id ||
    binding.scopeType !== 'workspace' ||
    binding.workspaceId !== input.workspace.id
  ) {
    throw unavailable('invalid_workspace_setting');
  }
  return binding;
}

function assertParentOwnership(input: SessionObservabilitySelectionPolicyInput): void {
  if (
    !isNonEmptyString(input.organization.id) ||
    !isNonEmptyString(input.workspace.id) ||
    input.workspace.organizationId !== input.organization.id ||
    input.organizationSetting.organizationId !== input.organization.id ||
    input.workspaceSetting.organizationId !== input.organization.id ||
    input.workspaceSetting.workspaceId !== input.workspace.id
  ) {
    throw unavailable('invalid_workspace_parent');
  }
}

function hasValidBindingConfiguration(binding: SessionObservabilitySelectedBinding): boolean {
  const version = binding.version;
  return (
    isAgentObservabilityAdapter(binding.adapterType) &&
    isAgentObservabilityEndpointKind(binding.endpointKind) &&
    isAgentObservabilityEndpointClass(binding.endpointClass) &&
    isCanonicalAgentObservabilityEndpoint(binding.endpoint) &&
    isAgentObservabilityExternalProjectId(binding.externalProjectId) &&
    isAgentObservabilityAdapter(version.adapterType) &&
    version.adapterType === binding.adapterType &&
    isAgentObservabilitySemanticProfile(version.semanticProfile) &&
    isAgentObservabilityProtocol(version.protocol) &&
    isAgentObservabilityCompression(version.compression) &&
    isAgentObservabilityTimeoutMs(version.timeoutMs) &&
    isFiniteNumberInRange(version.sampleRate, 0, 1) &&
    isAgentObservabilityCaptureMode(version.captureMode) &&
    isPositiveSafeInteger(version.configSchemaVersion) &&
    isNullableString(version.environment) &&
    isNullableString(version.release) &&
    isValidAgentObservabilityAdapterConfiguration(
      binding.adapterType,
      binding.endpointKind,
      binding.externalProjectId,
      version.semanticProfile,
      version.protocol,
    )
  );
}

async function loadLockedPlatformPolicy(
  db: DbTransaction,
): Promise<SessionObservabilityPlatformPolicy> {
  const row = (
    await db
      .select({
        allowedAdapters: agentObservabilityPlatformPolicy.allowedAdapters,
        allowedEndpointClasses: agentObservabilityPlatformPolicy.allowedEndpointClasses,
        maxCaptureMode: agentObservabilityPlatformPolicy.maxCaptureMode,
        captureRestrictionEpoch: agentObservabilityPlatformPolicy.captureRestrictionEpoch,
      })
      .from(agentObservabilityPlatformPolicy)
      .where(eq(agentObservabilityPlatformPolicy.id, 'default'))
      .for('share')
      .limit(1)
  )[0];
  if (!row) throw unavailable('missing_platform_policy');
  if (
    !arrayEvery(row.allowedAdapters, isAgentObservabilityAdapter) ||
    row.allowedAdapters.length === 0 ||
    !arrayEvery(row.allowedEndpointClasses, isAgentObservabilityEndpointClass) ||
    row.allowedEndpointClasses.length === 0 ||
    !isAgentObservabilityCaptureMode(row.maxCaptureMode) ||
    !isNonnegativeSafeInteger(row.captureRestrictionEpoch)
  ) {
    throw unavailable('invalid_platform_policy');
  }
  return {
    allowedAdapters: row.allowedAdapters as AgentObservabilityAdapter[],
    allowedEndpointClasses: row.allowedEndpointClasses as AgentObservabilityEndpointClass[],
    maxCaptureMode: row.maxCaptureMode as AgentObservabilityCaptureMode,
    captureRestrictionEpoch: row.captureRestrictionEpoch,
  };
}

async function loadLockedOrganization(
  db: DbTransaction,
  organizationId: string,
): Promise<{ id: string; status: 'active' | 'archived' }> {
  const row = (
    await db
      .select({ id: organizations.id, status: organizations.status })
      .from(organizations)
      .where(eq(organizations.id, organizationId))
      .for('share')
      .limit(1)
  )[0];
  if (!row) throw unavailable('missing_organization_parent');
  if (!isNonEmptyString(row.id) || (row.status !== 'active' && row.status !== 'archived')) {
    throw unavailable('invalid_organization_parent');
  }
  if (row.status !== 'active')
    throw new SessionObservabilitySelectionResourceUnavailableError('organization');
  return { id: row.id, status: row.status as 'active' };
}

async function loadLockedOrganizationSetting(
  db: DbTransaction,
  organizationId: string,
): Promise<SessionObservabilityOrganizationSetting> {
  const row = (
    await db
      .select({
        organizationId: agentObservabilityOrganizationSettings.organizationId,
        activeDefaultBindingId: agentObservabilityOrganizationSettings.activeDefaultBindingId,
        activeDefaultBindingScope: agentObservabilityOrganizationSettings.activeDefaultBindingScope,
        selectionEpoch: agentObservabilityOrganizationSettings.selectionEpoch,
        defaultRevocationEpoch: agentObservabilityOrganizationSettings.defaultRevocationEpoch,
        organizationRevocationEpoch:
          agentObservabilityOrganizationSettings.organizationRevocationEpoch,
        captureCeiling: agentObservabilityOrganizationSettings.captureCeiling,
        captureRestrictionEpoch: agentObservabilityOrganizationSettings.captureRestrictionEpoch,
      })
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId))
      .for('share')
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
    !isAgentObservabilityCaptureMode(row.captureCeiling) ||
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
    captureCeiling: row.captureCeiling as AgentObservabilityCaptureMode,
    captureRestrictionEpoch: row.captureRestrictionEpoch,
  };
}

async function loadLockedWorkspace(
  db: DbTransaction,
  organizationId: string,
  workspaceId: string,
): Promise<{ id: string; organizationId: string; status: 'active' | 'archived' }> {
  const row = (
    await db
      .select({
        id: workspaces.id,
        organizationId: workspaces.organizationId,
        status: workspaces.status,
      })
      .from(workspaces)
      .where(and(eq(workspaces.id, workspaceId), eq(workspaces.organizationId, organizationId)))
      .for('share')
      .limit(1)
  )[0];
  if (!row) throw unavailable('missing_workspace_parent');
  if (
    !isNonEmptyString(row.id) ||
    row.organizationId !== organizationId ||
    (row.status !== 'active' && row.status !== 'archived')
  ) {
    throw unavailable('invalid_workspace_parent');
  }
  if (row.status !== 'active')
    throw new SessionObservabilitySelectionResourceUnavailableError('workspace');
  return {
    id: row.id,
    organizationId: row.organizationId,
    status: row.status as 'active',
  };
}

async function loadLockedWorkspaceSetting(
  db: DbTransaction,
  organizationId: string,
  workspaceId: string,
): Promise<SessionObservabilityWorkspaceSetting> {
  const row = (
    await db
      .select({
        organizationId: agentObservabilityWorkspaceSettings.organizationId,
        workspaceId: agentObservabilityWorkspaceSettings.workspaceId,
        mode: agentObservabilityWorkspaceSettings.mode,
        bindingId: agentObservabilityWorkspaceSettings.bindingId,
        selectionEpoch: agentObservabilityWorkspaceSettings.selectionEpoch,
        revocationEpoch: agentObservabilityWorkspaceSettings.revocationEpoch,
        captureCeiling: agentObservabilityWorkspaceSettings.captureCeiling,
        captureRestrictionEpoch: agentObservabilityWorkspaceSettings.captureRestrictionEpoch,
      })
      .from(agentObservabilityWorkspaceSettings)
      .where(
        and(
          eq(agentObservabilityWorkspaceSettings.organizationId, organizationId),
          eq(agentObservabilityWorkspaceSettings.workspaceId, workspaceId),
        ),
      )
      .for('share')
      .limit(1)
  )[0];
  if (!row) throw unavailable('missing_workspace_setting');
  const validMode =
    (row.mode === 'custom' && isNonEmptyString(row.bindingId)) ||
    ((row.mode === 'inherit' || row.mode === 'disabled') && row.bindingId === null);
  if (
    row.organizationId !== organizationId ||
    row.workspaceId !== workspaceId ||
    !isWorkspaceObservabilityMode(row.mode) ||
    !validMode ||
    !isAgentObservabilityCaptureMode(row.captureCeiling) ||
    !isNonnegativeSafeInteger(row.selectionEpoch) ||
    !isNonnegativeSafeInteger(row.revocationEpoch) ||
    !isNonnegativeSafeInteger(row.captureRestrictionEpoch)
  ) {
    throw unavailable('invalid_workspace_setting');
  }
  return {
    organizationId: row.organizationId,
    workspaceId: row.workspaceId,
    mode: row.mode as WorkspaceObservabilityMode,
    bindingId: row.bindingId,
    selectionEpoch: row.selectionEpoch,
    revocationEpoch: row.revocationEpoch,
    captureCeiling: row.captureCeiling as AgentObservabilityCaptureMode,
    captureRestrictionEpoch: row.captureRestrictionEpoch,
  };
}

async function loadLockedSelectedBinding(
  db: DbTransaction,
  expected: {
    bindingId: string;
    organizationId: string;
    scopeType: 'organization' | 'workspace';
    workspaceId: string | null;
  },
): Promise<SessionObservabilitySelectedBinding> {
  const ownership = [
    eq(agentObservabilityBindings.id, expected.bindingId),
    eq(agentObservabilityBindings.organizationId, expected.organizationId),
    eq(agentObservabilityBindings.scopeType, expected.scopeType),
  ];
  if (expected.workspaceId === null) ownership.push(isNull(agentObservabilityBindings.workspaceId));
  else ownership.push(eq(agentObservabilityBindings.workspaceId, expected.workspaceId));
  const binding = (
    await db
      .select({
        id: agentObservabilityBindings.id,
        organizationId: agentObservabilityBindings.organizationId,
        workspaceId: agentObservabilityBindings.workspaceId,
        scopeType: agentObservabilityBindings.scopeType,
        adapterType: agentObservabilityBindings.adapterType,
        endpointKind: agentObservabilityBindings.endpointKind,
        endpointClass: agentObservabilityBindings.endpointClass,
        endpoint: agentObservabilityBindings.endpoint,
        externalProjectId: agentObservabilityBindings.externalProjectId,
        currentVersion: agentObservabilityBindings.currentVersion,
        status: agentObservabilityBindings.status,
        revocationEpoch: agentObservabilityBindings.revocationEpoch,
        archivedAt: agentObservabilityBindings.archivedAt,
      })
      .from(agentObservabilityBindings)
      .where(and(...ownership))
      .for('share')
      .limit(1)
  )[0];
  if (!binding) throw unavailable('missing_selected_binding');
  if (
    binding.id !== expected.bindingId ||
    binding.organizationId !== expected.organizationId ||
    binding.scopeType !== expected.scopeType ||
    binding.workspaceId !== expected.workspaceId ||
    !isNonEmptyString(binding.id) ||
    !isAgentObservabilityAdapter(binding.adapterType) ||
    !isAgentObservabilityEndpointKind(binding.endpointKind) ||
    !isAgentObservabilityEndpointClass(binding.endpointClass) ||
    !isAgentObservabilityBindingStatus(binding.status) ||
    !isPositiveSafeInteger(binding.currentVersion) ||
    !isNonnegativeSafeInteger(binding.revocationEpoch) ||
    !hasValidArchiveShape(binding.status, binding.archivedAt)
  ) {
    throw unavailable('invalid_selected_binding');
  }

  const version = (
    await db
      .select({
        bindingId: agentObservabilityBindingVersions.bindingId,
        version: agentObservabilityBindingVersions.version,
        adapterType: agentObservabilityBindingVersions.adapterType,
        semanticProfile: agentObservabilityBindingVersions.semanticProfile,
        protocol: agentObservabilityBindingVersions.protocol,
        compression: agentObservabilityBindingVersions.compression,
        timeoutMs: agentObservabilityBindingVersions.timeoutMs,
        environment: agentObservabilityBindingVersions.environment,
        release: agentObservabilityBindingVersions.release,
        captureMode: agentObservabilityBindingVersions.captureMode,
        sampleRate: agentObservabilityBindingVersions.sampleRate,
        configSchemaVersion: agentObservabilityBindingVersions.configSchemaVersion,
      })
      .from(agentObservabilityBindingVersions)
      .where(
        and(
          eq(agentObservabilityBindingVersions.bindingId, binding.id),
          eq(agentObservabilityBindingVersions.version, binding.currentVersion),
          eq(agentObservabilityBindingVersions.adapterType, binding.adapterType),
        ),
      )
      .for('share')
      .limit(1)
  )[0];
  if (!version) throw unavailable('missing_selected_binding_version');
  if (
    version.bindingId !== binding.id ||
    version.version !== binding.currentVersion ||
    version.adapterType !== binding.adapterType ||
    !isPositiveSafeInteger(version.version)
  ) {
    throw unavailable('invalid_selected_binding_version');
  }

  // Existence is all Session pinning needs. Do not select secret_ref or a
  // credential generation: pre-send resolution owns current credential state.
  const credentialHead = (
    await db
      .select({ configured: sql<boolean>`true` })
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, binding.id))
      .for('share')
      .limit(1)
  )[0];

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
    version: {
      version: version.version,
      adapterType: version.adapterType as AgentObservabilityAdapter,
      semanticProfile: version.semanticProfile as AgentObservabilitySemanticProfile,
      protocol: version.protocol as AgentObservabilityProtocol,
      compression: version.compression as AgentObservabilityCompression,
      timeoutMs: version.timeoutMs,
      environment: version.environment,
      release: version.release,
      captureMode: version.captureMode as AgentObservabilityCaptureMode,
      sampleRate: finiteNumber(version.sampleRate),
      configSchemaVersion: version.configSchemaVersion,
    },
    credentialConfigured: credentialHead?.configured === true,
  };
}

function hasValidArchiveShape(
  status: AgentObservabilityBindingStatus,
  archivedAt: unknown,
): boolean {
  const validTimestamp =
    archivedAt === null || (archivedAt instanceof Date && Number.isFinite(archivedAt.getTime()));
  return validTimestamp && (status === 'archived') === (archivedAt !== null);
}

function finiteNumber(value: unknown): number {
  if (typeof value === 'number') return value;
  if (typeof value === 'string') return Number(value);
  return Number.NaN;
}

function isFiniteNumberInRange(value: unknown, min: number, max: number): boolean {
  const number = finiteNumber(value);
  return Number.isFinite(number) && number >= min && number <= max;
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
  reason: SessionObservabilitySelectionAvailabilityReason,
): SessionObservabilitySelectionAvailabilityError {
  return new SessionObservabilitySelectionAvailabilityError(reason);
}
