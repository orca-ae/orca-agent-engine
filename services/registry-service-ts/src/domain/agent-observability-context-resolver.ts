// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, isNull } from 'drizzle-orm';
import type { AgentObservabilitySessionContext } from '../contracts/internal.contract.js';
import { isWorkspaceId } from '../auth/workspace-id.js';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import {
  agentObservabilityBindingCredentials,
  agentObservabilityBindingVersions,
  agentObservabilityBindings,
  agentObservabilityOrganizationSettings,
  agentObservabilityPlatformPolicy,
  agentObservabilityWorkspaceSettings,
  organizations,
  sessionObservabilityBindings,
  workspaces,
} from '../persistence/postgres/schema.js';
import {
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

const SESSION_ID_RE = /^ses_[A-Za-z0-9_-]+$/;
const AGENT_ID_RE = /^(?:agt|agent)_[A-Za-z0-9_-]+$/;

type ContextUnavailableReason =
  | 'invalid_advisory_pin'
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
  | 'missing_pinned_binding'
  | 'invalid_pinned_binding'
  | 'regressed_binding_version'
  | 'missing_pinned_binding_version'
  | 'invalid_pinned_binding_version'
  | 'invalid_credential_head'
  | 'missing_session_pin'
  | 'invalid_session_pin'
  | 'changed_session_pin'
  | 'regressed_epoch';

export class AgentObservabilitySessionContextNotFoundError extends Error {
  override readonly name = 'AgentObservabilitySessionContextNotFoundError';

  constructor() {
    super('agent observability session context not found');
  }
}

/** Safe, retryable authority absence or corruption. Never expose `reason` over HTTP. */
export class AgentObservabilitySessionContextUnavailableError extends Error {
  override readonly name = 'AgentObservabilitySessionContextUnavailableError';

  constructor(readonly reason: ContextUnavailableReason) {
    super('agent observability session context unavailable');
  }
}

type SessionObservabilityContextSuppressionReason =
  | 'session_archived'
  | 'session_deleted'
  | 'session_revoked'
  | 'organization_archived'
  | 'workspace_archived'
  | 'organization_default_revoked'
  | 'organization_revoked'
  | 'workspace_revoked'
  | 'binding_revoked'
  | 'binding_disabled'
  | 'binding_archived'
  | 'binding_configuration_invalid'
  | 'platform_adapter_disallowed'
  | 'platform_endpoint_class_disallowed'
  | 'credential_not_configured';

export type SessionObservabilityContextDecision =
  | { status: 'enabled'; reason: null }
  | { status: 'disabled'; reason: 'session_pin_disabled' }
  | { status: 'suppressed'; reason: SessionObservabilityContextSuppressionReason };

interface AdvisoryPin {
  organizationId: string;
  bindingId: string | null;
  bindingVersion: number | null;
  bindingScope: string | null;
  bindingWorkspaceId: string | null;
  selectionSource: string;
}

interface ContextPin {
  workspaceId: string;
  sessionId: string;
  organizationId: string;
  bindingId: string | null;
  bindingVersion: number | null;
  bindingScope: 'organization' | 'workspace' | null;
  bindingWorkspaceId: string | null;
  selectionSource: 'organization_default' | 'workspace_custom' | 'disabled';
  status: 'active' | 'disabled' | 'archived' | 'deleted';
  organizationSelectionEpoch: number;
  workspaceSelectionEpoch: number;
  organizationDefaultRevocationEpoch: number;
  organizationRevocationEpoch: number;
  workspaceRevocationEpoch: number;
  bindingRevocationEpoch: number;
  platformCaptureRestrictionEpoch: number;
  organizationCaptureRestrictionEpoch: number;
  workspaceCaptureRestrictionEpoch: number;
  effectiveCaptureMode: AgentObservabilityCaptureMode;
  sessionRevocationEpoch: number;
  agentId: string;
  agentVersion: number;
  harness: string | null;
  harnessMode: string | null;
  archivedAt: Date | null;
  deletedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

interface ContextPlatformPolicy {
  allowedAdapters: AgentObservabilityAdapter[];
  allowedEndpointClasses: AgentObservabilityEndpointClass[];
  maxCaptureMode: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
}

interface ContextOrganization {
  id: string;
  status: 'active' | 'archived';
}

interface ContextOrganizationSetting {
  organizationId: string;
  selectionEpoch: number;
  defaultRevocationEpoch: number;
  organizationRevocationEpoch: number;
  captureCeiling: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
}

interface ContextWorkspace {
  id: string;
  organizationId: string;
  status: 'active' | 'archived';
}

interface ContextWorkspaceSetting {
  organizationId: string;
  workspaceId: string;
  mode: WorkspaceObservabilityMode;
  selectionEpoch: number;
  revocationEpoch: number;
  captureCeiling: AgentObservabilityCaptureMode;
  captureRestrictionEpoch: number;
}

interface ContextBinding {
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
}

export interface SessionObservabilityContextClassificationInput {
  pin: ContextPin;
  platformPolicy: ContextPlatformPolicy;
  organization: ContextOrganization;
  organizationSetting: ContextOrganizationSetting;
  workspace: ContextWorkspace;
  workspaceSetting: ContextWorkspaceSetting;
  binding: ContextBinding | null;
  currentCredentialVersion: number | null;
}

/**
 * Delivery status is pure: selection-pointer changes are intentionally absent.
 * The secret resolver added later must re-run this policy before every release.
 */
export function classifySessionObservabilityContext(
  input: SessionObservabilityContextClassificationInput,
): SessionObservabilityContextDecision {
  const { pin, platformPolicy, organization, organizationSetting, workspace, workspaceSetting } =
    input;

  if (pin.status === 'archived') return { status: 'suppressed', reason: 'session_archived' };
  if (pin.status === 'deleted') return { status: 'suppressed', reason: 'session_deleted' };
  if (organization.status === 'archived') {
    return { status: 'suppressed', reason: 'organization_archived' };
  }
  if (workspace.status === 'archived')
    return { status: 'suppressed', reason: 'workspace_archived' };
  if (pin.sessionRevocationEpoch > 0) {
    return { status: 'suppressed', reason: 'session_revoked' };
  }
  if (organizationSetting.organizationRevocationEpoch > pin.organizationRevocationEpoch) {
    return { status: 'suppressed', reason: 'organization_revoked' };
  }
  if (workspaceSetting.revocationEpoch > pin.workspaceRevocationEpoch) {
    return { status: 'suppressed', reason: 'workspace_revoked' };
  }
  if (
    pin.selectionSource === 'organization_default' &&
    organizationSetting.defaultRevocationEpoch > pin.organizationDefaultRevocationEpoch
  ) {
    return { status: 'suppressed', reason: 'organization_default_revoked' };
  }

  if (input.binding !== null) {
    if (input.binding.revocationEpoch > pin.bindingRevocationEpoch) {
      return { status: 'suppressed', reason: 'binding_revoked' };
    }
    if (input.binding.status === 'disabled') {
      return { status: 'suppressed', reason: 'binding_disabled' };
    }
    if (input.binding.status === 'archived') {
      return { status: 'suppressed', reason: 'binding_archived' };
    }
    if (!platformPolicy.allowedAdapters.includes(input.binding.adapterType)) {
      return { status: 'suppressed', reason: 'platform_adapter_disallowed' };
    }
    if (!platformPolicy.allowedEndpointClasses.includes(input.binding.endpointClass)) {
      return { status: 'suppressed', reason: 'platform_endpoint_class_disallowed' };
    }
    if (!hasValidBindingConfiguration(input.binding)) {
      return { status: 'suppressed', reason: 'binding_configuration_invalid' };
    }
    if (input.currentCredentialVersion === null) {
      return { status: 'suppressed', reason: 'credential_not_configured' };
    }
  }

  if (pin.status === 'disabled') return { status: 'disabled', reason: 'session_pin_disabled' };

  // `validatePin` proves an active pin always has a complete binding reference.
  if (input.binding === null) throw unavailable('invalid_session_pin');
  return { status: 'enabled', reason: null };
}

/**
 * A capture restriction is sticky for every pin that predates it. Current
 * ceilings still clamp normally, but a later expansion cannot undo a
 * platform, organization, or workspace restriction epoch advance.
 */
export function effectiveSessionObservabilityContextCaptureMode(
  input: Pick<
    SessionObservabilityContextClassificationInput,
    'pin' | 'platformPolicy' | 'organizationSetting' | 'workspaceSetting'
  >,
): AgentObservabilityCaptureMode {
  const { pin, platformPolicy, organizationSetting, workspaceSetting } = input;
  if (
    platformPolicy.captureRestrictionEpoch > pin.platformCaptureRestrictionEpoch ||
    organizationSetting.captureRestrictionEpoch > pin.organizationCaptureRestrictionEpoch ||
    workspaceSetting.captureRestrictionEpoch > pin.workspaceCaptureRestrictionEpoch
  ) {
    return 'metadata_only';
  }
  return effectiveCaptureMode(
    pin.effectiveCaptureMode,
    platformPolicy.maxCaptureMode,
    organizationSetting.captureCeiling,
    workspaceSetting.captureCeiling,
  );
}

/** Load a repeatable, non-secret Session-pinned context snapshot. */
export async function loadAgentObservabilitySessionContext(input: {
  db: DbClient;
  workspaceId: string;
  sessionId: string;
}): Promise<AgentObservabilitySessionContext> {
  return input.db.transaction(
    (tx) =>
      loadAgentObservabilitySessionContextInTransaction({
        tx: tx as DbTransaction,
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
      }),
    { isolationLevel: 'repeatable read' },
  );
}

/**
 * Caller-owned variant for future authorization paths. It deliberately has no
 * SecretStore dependency and never selects a secret reference.
 */
export async function loadAgentObservabilitySessionContextInTransaction(input: {
  tx: DbTransaction;
  workspaceId: string;
  sessionId: string;
}): Promise<AgentObservabilitySessionContext> {
  const advisoryPin = await loadAdvisoryPin(input.tx, input.workspaceId, input.sessionId);
  if (!advisoryPin) throw new AgentObservabilitySessionContextNotFoundError();
  validateAdvisoryPin(advisoryPin, input.workspaceId, input.sessionId);

  // Keep this order in sync with Session creation and observability mutation
  // writers. The initial pin lookup is advisory only; the final locked reread
  // is the authoritative lifecycle/tombstone row.
  const platformPolicy = await loadLockedPlatformPolicy(input.tx);
  const organization = await loadLockedOrganization(input.tx, advisoryPin.organizationId);
  const organizationSetting = await loadLockedOrganizationSetting(input.tx, organization.id);
  const workspace = await loadLockedWorkspace(input.tx, organization.id, input.workspaceId);
  const workspaceSetting = await loadLockedWorkspaceSetting(
    input.tx,
    organization.id,
    workspace.id,
  );

  let binding: ContextBinding | null = null;
  let currentCredentialVersion: number | null = null;
  if (advisoryPin.bindingId !== null) {
    binding = await loadLockedPinnedBinding(input.tx, {
      bindingId: advisoryPin.bindingId,
      bindingVersion: advisoryPin.bindingVersion!,
      organizationId: organization.id,
      scopeType: advisoryPin.bindingScope as 'organization' | 'workspace',
      workspaceId: advisoryPin.bindingWorkspaceId,
    });
    currentCredentialVersion = await loadLockedCredentialHead(input.tx, binding.id);
  }

  const pin = await loadLockedPin(input.tx, input.workspaceId, input.sessionId);
  if (!pin) throw unavailable('missing_session_pin');
  validatePin(pin, input.workspaceId, input.sessionId);
  assertAdvisoryPinMatches(advisoryPin, pin);
  assertEpochsDoNotRegress({
    pin,
    platformPolicy,
    organizationSetting,
    workspaceSetting,
    binding,
  });

  const decision = classifySessionObservabilityContext({
    pin,
    platformPolicy,
    organization,
    organizationSetting,
    workspace,
    workspaceSetting,
    binding,
    currentCredentialVersion,
  });
  return contextResponse({
    decision,
    pin,
    platformPolicy,
    organizationSetting,
    workspaceSetting,
    binding,
    currentCredentialVersion,
  });
}

async function loadAdvisoryPin(
  tx: DbTransaction,
  workspaceId: string,
  sessionId: string,
): Promise<AdvisoryPin | undefined> {
  return (
    await tx
      .select({
        organizationId: sessionObservabilityBindings.organizationId,
        bindingId: sessionObservabilityBindings.bindingId,
        bindingVersion: sessionObservabilityBindings.bindingVersion,
        bindingScope: sessionObservabilityBindings.bindingScope,
        bindingWorkspaceId: sessionObservabilityBindings.bindingWorkspaceId,
        selectionSource: sessionObservabilityBindings.selectionSource,
      })
      .from(sessionObservabilityBindings)
      .where(
        and(
          eq(sessionObservabilityBindings.workspaceId, workspaceId),
          eq(sessionObservabilityBindings.sessionId, sessionId),
        ),
      )
      .limit(1)
  )[0];
}

async function loadLockedPlatformPolicy(tx: DbTransaction): Promise<ContextPlatformPolicy> {
  const row = (
    await tx
      .select({
        allowedAdapters: agentObservabilityPlatformPolicy.allowedAdapters,
        allowedEndpointClasses: agentObservabilityPlatformPolicy.allowedEndpointClasses,
        maxCaptureMode: agentObservabilityPlatformPolicy.maxCaptureMode,
        captureRestrictionEpoch: agentObservabilityPlatformPolicy.captureRestrictionEpoch,
        createdAt: agentObservabilityPlatformPolicy.createdAt,
        updatedAt: agentObservabilityPlatformPolicy.updatedAt,
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
    !isNonnegativeSafeInteger(row.captureRestrictionEpoch) ||
    !hasValidTimestampPair(row.createdAt, row.updatedAt)
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

async function loadLockedOrganization(
  tx: DbTransaction,
  organizationId: string,
): Promise<ContextOrganization> {
  const row = (
    await tx
      .select({
        id: organizations.id,
        status: organizations.status,
        createdAt: organizations.createdAt,
        updatedAt: organizations.updatedAt,
      })
      .from(organizations)
      .where(eq(organizations.id, organizationId))
      .for('share')
      .limit(1)
  )[0];
  if (!row) throw unavailable('missing_organization_parent');
  if (
    row.id !== organizationId ||
    !isNonEmptyString(row.id) ||
    !isOrganizationStatus(row.status) ||
    !hasValidTimestampPair(row.createdAt, row.updatedAt)
  ) {
    throw unavailable('invalid_organization_parent');
  }
  return { id: row.id, status: row.status };
}

async function loadLockedOrganizationSetting(
  tx: DbTransaction,
  organizationId: string,
): Promise<ContextOrganizationSetting> {
  const row = (
    await tx
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
        createdAt: agentObservabilityOrganizationSettings.createdAt,
        updatedAt: agentObservabilityOrganizationSettings.updatedAt,
      })
      .from(agentObservabilityOrganizationSettings)
      .where(eq(agentObservabilityOrganizationSettings.organizationId, organizationId))
      .for('share')
      .limit(1)
  )[0];
  if (!row) throw unavailable('missing_organization_setting');
  const validDefaultPointer =
    (row.activeDefaultBindingId === null && row.activeDefaultBindingScope === null) ||
    (isNonEmptyString(row.activeDefaultBindingId) &&
      row.activeDefaultBindingScope === 'organization');
  if (
    row.organizationId !== organizationId ||
    !validDefaultPointer ||
    !isNonnegativeSafeInteger(row.selectionEpoch) ||
    !isNonnegativeSafeInteger(row.defaultRevocationEpoch) ||
    !isNonnegativeSafeInteger(row.organizationRevocationEpoch) ||
    !isAgentObservabilityCaptureMode(row.captureCeiling) ||
    !isNonnegativeSafeInteger(row.captureRestrictionEpoch) ||
    !hasValidTimestampPair(row.createdAt, row.updatedAt)
  ) {
    throw unavailable('invalid_organization_setting');
  }
  return {
    organizationId: row.organizationId,
    selectionEpoch: row.selectionEpoch,
    defaultRevocationEpoch: row.defaultRevocationEpoch,
    organizationRevocationEpoch: row.organizationRevocationEpoch,
    captureCeiling: row.captureCeiling,
    captureRestrictionEpoch: row.captureRestrictionEpoch,
  };
}

async function loadLockedWorkspace(
  tx: DbTransaction,
  organizationId: string,
  workspaceId: string,
): Promise<ContextWorkspace> {
  const row = (
    await tx
      .select({
        id: workspaces.id,
        organizationId: workspaces.organizationId,
        status: workspaces.status,
        archivedAt: workspaces.archivedAt,
        createdAt: workspaces.createdAt,
        updatedAt: workspaces.updatedAt,
      })
      .from(workspaces)
      .where(and(eq(workspaces.organizationId, organizationId), eq(workspaces.id, workspaceId)))
      .for('share')
      .limit(1)
  )[0];
  if (!row) throw unavailable('missing_workspace_parent');
  if (
    row.id !== workspaceId ||
    row.organizationId !== organizationId ||
    !isWorkspaceId(row.id) ||
    !isWorkspaceStatus(row.status) ||
    !hasValidWorkspaceArchiveShape(row.status, row.archivedAt) ||
    !hasValidTimestampPair(row.createdAt, row.updatedAt)
  ) {
    throw unavailable('invalid_workspace_parent');
  }
  return { id: row.id, organizationId: row.organizationId, status: row.status };
}

async function loadLockedWorkspaceSetting(
  tx: DbTransaction,
  organizationId: string,
  workspaceId: string,
): Promise<ContextWorkspaceSetting> {
  const row = (
    await tx
      .select({
        organizationId: agentObservabilityWorkspaceSettings.organizationId,
        workspaceId: agentObservabilityWorkspaceSettings.workspaceId,
        mode: agentObservabilityWorkspaceSettings.mode,
        bindingId: agentObservabilityWorkspaceSettings.bindingId,
        selectionEpoch: agentObservabilityWorkspaceSettings.selectionEpoch,
        revocationEpoch: agentObservabilityWorkspaceSettings.revocationEpoch,
        captureCeiling: agentObservabilityWorkspaceSettings.captureCeiling,
        captureRestrictionEpoch: agentObservabilityWorkspaceSettings.captureRestrictionEpoch,
        createdAt: agentObservabilityWorkspaceSettings.createdAt,
        updatedAt: agentObservabilityWorkspaceSettings.updatedAt,
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
  const validModeBinding =
    (row.mode === 'custom' && isNonEmptyString(row.bindingId)) ||
    ((row.mode === 'inherit' || row.mode === 'disabled') && row.bindingId === null);
  if (
    row.organizationId !== organizationId ||
    row.workspaceId !== workspaceId ||
    !isWorkspaceObservabilityMode(row.mode) ||
    !validModeBinding ||
    !isNonnegativeSafeInteger(row.selectionEpoch) ||
    !isNonnegativeSafeInteger(row.revocationEpoch) ||
    !isAgentObservabilityCaptureMode(row.captureCeiling) ||
    !isNonnegativeSafeInteger(row.captureRestrictionEpoch) ||
    !hasValidTimestampPair(row.createdAt, row.updatedAt)
  ) {
    throw unavailable('invalid_workspace_setting');
  }
  return {
    organizationId: row.organizationId,
    workspaceId: row.workspaceId,
    mode: row.mode,
    selectionEpoch: row.selectionEpoch,
    revocationEpoch: row.revocationEpoch,
    captureCeiling: row.captureCeiling,
    captureRestrictionEpoch: row.captureRestrictionEpoch,
  };
}

async function loadLockedPinnedBinding(
  tx: DbTransaction,
  expected: {
    bindingId: string;
    bindingVersion: number;
    organizationId: string;
    scopeType: 'organization' | 'workspace';
    workspaceId: string | null;
  },
): Promise<ContextBinding> {
  const ownership = [
    eq(agentObservabilityBindings.id, expected.bindingId),
    eq(agentObservabilityBindings.organizationId, expected.organizationId),
    eq(agentObservabilityBindings.scopeType, expected.scopeType),
  ];
  if (expected.workspaceId === null) ownership.push(isNull(agentObservabilityBindings.workspaceId));
  else ownership.push(eq(agentObservabilityBindings.workspaceId, expected.workspaceId));

  const binding = (
    await tx
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
        createdAt: agentObservabilityBindings.createdAt,
        updatedAt: agentObservabilityBindings.updatedAt,
      })
      .from(agentObservabilityBindings)
      .where(and(...ownership))
      .for('share')
      .limit(1)
  )[0];
  if (!binding) throw unavailable('missing_pinned_binding');
  if (
    binding.id !== expected.bindingId ||
    binding.organizationId !== expected.organizationId ||
    binding.scopeType !== expected.scopeType ||
    binding.workspaceId !== expected.workspaceId ||
    !isNonEmptyString(binding.id) ||
    !isAgentObservabilityAdapter(binding.adapterType) ||
    !isAgentObservabilityEndpointKind(binding.endpointKind) ||
    !isAgentObservabilityEndpointClass(binding.endpointClass) ||
    !isCanonicalAgentObservabilityEndpoint(binding.endpoint) ||
    !isAgentObservabilityExternalProjectId(binding.externalProjectId) ||
    !isAgentObservabilityBindingStatus(binding.status) ||
    !isPositiveSafeInteger(binding.currentVersion) ||
    !isNonnegativeSafeInteger(binding.revocationEpoch) ||
    !hasValidBindingArchiveShape(binding.status, binding.archivedAt) ||
    !hasValidTimestampPair(binding.createdAt, binding.updatedAt)
  ) {
    throw unavailable('invalid_pinned_binding');
  }
  if (binding.currentVersion < expected.bindingVersion) {
    throw unavailable('regressed_binding_version');
  }

  const version = (
    await tx
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
        createdAt: agentObservabilityBindingVersions.createdAt,
      })
      .from(agentObservabilityBindingVersions)
      .where(
        and(
          eq(agentObservabilityBindingVersions.bindingId, binding.id),
          eq(agentObservabilityBindingVersions.version, expected.bindingVersion),
          eq(agentObservabilityBindingVersions.adapterType, binding.adapterType),
        ),
      )
      .for('share')
      .limit(1)
  )[0];
  if (!version) throw unavailable('missing_pinned_binding_version');
  const sampleRate = finiteNumberInRange(version.sampleRate, 0, 1);
  if (
    version.bindingId !== binding.id ||
    version.version !== expected.bindingVersion ||
    version.adapterType !== binding.adapterType ||
    !isPositiveSafeInteger(version.version) ||
    !isAgentObservabilitySemanticProfile(version.semanticProfile) ||
    !isAgentObservabilityProtocol(version.protocol) ||
    !isAgentObservabilityCompression(version.compression) ||
    !isAgentObservabilityTimeoutMs(version.timeoutMs) ||
    sampleRate === null ||
    !isAgentObservabilityCaptureMode(version.captureMode) ||
    !isPositiveSafeInteger(version.configSchemaVersion) ||
    !isNullableString(version.environment) ||
    !isNullableString(version.release) ||
    !isValidDate(version.createdAt)
  ) {
    throw unavailable('invalid_pinned_binding_version');
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
  };
}

/** Reads head metadata only; this resolver never queries a SecretStore reference. */
async function loadLockedCredentialHead(
  tx: DbTransaction,
  bindingId: string,
): Promise<number | null> {
  const row = (
    await tx
      .select({
        bindingId: agentObservabilityBindingCredentials.bindingId,
        credentialVersion: agentObservabilityBindingCredentials.credentialVersion,
        keyHint: agentObservabilityBindingCredentials.keyHint,
        rotatedAt: agentObservabilityBindingCredentials.rotatedAt,
        createdAt: agentObservabilityBindingCredentials.createdAt,
        updatedAt: agentObservabilityBindingCredentials.updatedAt,
      })
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingId))
      .for('share')
      .limit(1)
  )[0];
  if (!row) return null;
  if (
    row.bindingId !== bindingId ||
    !isPositiveSafeInteger(row.credentialVersion) ||
    !isAgentObservabilityKeyHint(row.keyHint) ||
    !isValidDate(row.rotatedAt) ||
    !hasValidTimestampPair(row.createdAt, row.updatedAt)
  ) {
    throw unavailable('invalid_credential_head');
  }
  return row.credentialVersion;
}

async function loadLockedPin(
  tx: DbTransaction,
  workspaceId: string,
  sessionId: string,
): Promise<ContextPin | undefined> {
  const row = (
    await tx
      .select({
        workspaceId: sessionObservabilityBindings.workspaceId,
        sessionId: sessionObservabilityBindings.sessionId,
        organizationId: sessionObservabilityBindings.organizationId,
        bindingId: sessionObservabilityBindings.bindingId,
        bindingVersion: sessionObservabilityBindings.bindingVersion,
        bindingScope: sessionObservabilityBindings.bindingScope,
        bindingWorkspaceId: sessionObservabilityBindings.bindingWorkspaceId,
        selectionSource: sessionObservabilityBindings.selectionSource,
        status: sessionObservabilityBindings.status,
        organizationSelectionEpoch: sessionObservabilityBindings.organizationSelectionEpoch,
        workspaceSelectionEpoch: sessionObservabilityBindings.workspaceSelectionEpoch,
        organizationDefaultRevocationEpoch:
          sessionObservabilityBindings.organizationDefaultRevocationEpoch,
        organizationRevocationEpoch: sessionObservabilityBindings.organizationRevocationEpoch,
        workspaceRevocationEpoch: sessionObservabilityBindings.workspaceRevocationEpoch,
        bindingRevocationEpoch: sessionObservabilityBindings.bindingRevocationEpoch,
        platformCaptureRestrictionEpoch:
          sessionObservabilityBindings.platformCaptureRestrictionEpoch,
        organizationCaptureRestrictionEpoch:
          sessionObservabilityBindings.organizationCaptureRestrictionEpoch,
        workspaceCaptureRestrictionEpoch:
          sessionObservabilityBindings.workspaceCaptureRestrictionEpoch,
        effectiveCaptureMode: sessionObservabilityBindings.effectiveCaptureMode,
        sessionRevocationEpoch: sessionObservabilityBindings.sessionRevocationEpoch,
        agentId: sessionObservabilityBindings.agentId,
        agentVersion: sessionObservabilityBindings.agentVersion,
        harness: sessionObservabilityBindings.harness,
        harnessMode: sessionObservabilityBindings.harnessMode,
        archivedAt: sessionObservabilityBindings.archivedAt,
        deletedAt: sessionObservabilityBindings.deletedAt,
        createdAt: sessionObservabilityBindings.createdAt,
        updatedAt: sessionObservabilityBindings.updatedAt,
      })
      .from(sessionObservabilityBindings)
      .where(
        and(
          eq(sessionObservabilityBindings.workspaceId, workspaceId),
          eq(sessionObservabilityBindings.sessionId, sessionId),
        ),
      )
      .for('share')
      .limit(1)
  )[0];
  return row as ContextPin | undefined;
}

function validateAdvisoryPin(pin: AdvisoryPin, workspaceId: string, sessionId: string): void {
  if (!isNonEmptyString(pin.organizationId) || !isSelectionSource(pin.selectionSource)) {
    throw unavailable('invalid_advisory_pin');
  }
  const hasBinding = pin.bindingId !== null;
  if (
    (pin.selectionSource === 'disabled' &&
      (hasBinding ||
        pin.bindingVersion !== null ||
        pin.bindingScope !== null ||
        pin.bindingWorkspaceId !== null)) ||
    (pin.selectionSource === 'organization_default' &&
      (!isNonEmptyString(pin.bindingId) ||
        !isPositiveSafeInteger(pin.bindingVersion) ||
        pin.bindingScope !== 'organization' ||
        pin.bindingWorkspaceId !== null)) ||
    (pin.selectionSource === 'workspace_custom' &&
      (!isNonEmptyString(pin.bindingId) ||
        !isPositiveSafeInteger(pin.bindingVersion) ||
        pin.bindingScope !== 'workspace' ||
        pin.bindingWorkspaceId !== workspaceId)) ||
    !isWorkspaceId(workspaceId) ||
    !SESSION_ID_RE.test(sessionId)
  ) {
    throw unavailable('invalid_advisory_pin');
  }
}

function validatePin(pin: ContextPin, workspaceId: string, sessionId: string): void {
  const validEpochs = [
    pin.organizationSelectionEpoch,
    pin.workspaceSelectionEpoch,
    pin.organizationDefaultRevocationEpoch,
    pin.organizationRevocationEpoch,
    pin.workspaceRevocationEpoch,
    pin.bindingRevocationEpoch,
    pin.platformCaptureRestrictionEpoch,
    pin.organizationCaptureRestrictionEpoch,
    pin.workspaceCaptureRestrictionEpoch,
    pin.sessionRevocationEpoch,
  ].every(isNonnegativeSafeInteger);
  const disabledShape =
    pin.selectionSource === 'disabled' &&
    ['disabled', 'archived', 'deleted'].includes(pin.status) &&
    pin.bindingId === null &&
    pin.bindingVersion === null &&
    pin.bindingScope === null &&
    pin.bindingWorkspaceId === null &&
    pin.bindingRevocationEpoch === 0 &&
    pin.effectiveCaptureMode === 'metadata_only';
  const organizationDefaultShape =
    pin.selectionSource === 'organization_default' &&
    ['active', 'archived', 'deleted'].includes(pin.status) &&
    isNonEmptyString(pin.bindingId) &&
    isPositiveSafeInteger(pin.bindingVersion) &&
    pin.bindingScope === 'organization' &&
    pin.bindingWorkspaceId === null;
  const workspaceCustomShape =
    pin.selectionSource === 'workspace_custom' &&
    ['active', 'archived', 'deleted'].includes(pin.status) &&
    isNonEmptyString(pin.bindingId) &&
    isPositiveSafeInteger(pin.bindingVersion) &&
    pin.bindingScope === 'workspace' &&
    pin.bindingWorkspaceId === workspaceId;
  if (
    pin.workspaceId !== workspaceId ||
    pin.sessionId !== sessionId ||
    !isWorkspaceId(pin.workspaceId) ||
    !SESSION_ID_RE.test(pin.sessionId) ||
    !isNonEmptyString(pin.organizationId) ||
    !isSelectionSource(pin.selectionSource) ||
    !isPinStatus(pin.status) ||
    !validEpochs ||
    !isAgentObservabilityCaptureMode(pin.effectiveCaptureMode) ||
    !AGENT_ID_RE.test(pin.agentId) ||
    !isPositiveSafeInteger(pin.agentVersion) ||
    !isNullableString(pin.harness) ||
    !isNullableString(pin.harnessMode) ||
    !hasValidPinLifecycleShape(pin.status, pin.archivedAt, pin.deletedAt) ||
    !hasValidTimestampPair(pin.createdAt, pin.updatedAt) ||
    (!disabledShape && !organizationDefaultShape && !workspaceCustomShape)
  ) {
    throw unavailable('invalid_session_pin');
  }
}

function assertAdvisoryPinMatches(advisory: AdvisoryPin, pin: ContextPin): void {
  if (
    advisory.organizationId !== pin.organizationId ||
    advisory.bindingId !== pin.bindingId ||
    advisory.bindingVersion !== pin.bindingVersion ||
    advisory.bindingScope !== pin.bindingScope ||
    advisory.bindingWorkspaceId !== pin.bindingWorkspaceId ||
    advisory.selectionSource !== pin.selectionSource
  ) {
    throw unavailable('changed_session_pin');
  }
}

function assertEpochsDoNotRegress(input: {
  pin: ContextPin;
  platformPolicy: ContextPlatformPolicy;
  organizationSetting: ContextOrganizationSetting;
  workspaceSetting: ContextWorkspaceSetting;
  binding: ContextBinding | null;
}): void {
  const { pin, platformPolicy, organizationSetting, workspaceSetting, binding } = input;
  const nonRegressing =
    organizationSetting.selectionEpoch >= pin.organizationSelectionEpoch &&
    workspaceSetting.selectionEpoch >= pin.workspaceSelectionEpoch &&
    organizationSetting.defaultRevocationEpoch >= pin.organizationDefaultRevocationEpoch &&
    organizationSetting.organizationRevocationEpoch >= pin.organizationRevocationEpoch &&
    workspaceSetting.revocationEpoch >= pin.workspaceRevocationEpoch &&
    platformPolicy.captureRestrictionEpoch >= pin.platformCaptureRestrictionEpoch &&
    organizationSetting.captureRestrictionEpoch >= pin.organizationCaptureRestrictionEpoch &&
    workspaceSetting.captureRestrictionEpoch >= pin.workspaceCaptureRestrictionEpoch &&
    (binding === null
      ? pin.bindingRevocationEpoch === 0
      : binding.revocationEpoch >= pin.bindingRevocationEpoch);
  if (!nonRegressing) throw unavailable('regressed_epoch');
}

function contextResponse(input: {
  decision: SessionObservabilityContextDecision;
  pin: ContextPin;
  platformPolicy: ContextPlatformPolicy;
  organizationSetting: ContextOrganizationSetting;
  workspaceSetting: ContextWorkspaceSetting;
  binding: ContextBinding | null;
  currentCredentialVersion: number | null;
}): AgentObservabilitySessionContext {
  const { pin, platformPolicy, organizationSetting, workspaceSetting, binding } = input;
  const common = {
    schema_version: 1 as const,
    organization_id: pin.organizationId,
    workspace_id: pin.workspaceId,
    session_id: pin.sessionId,
    agent: { id: pin.agentId, version: pin.agentVersion },
    harness: pin.harness,
    harness_mode: pin.harnessMode,
    selection_source: pin.selectionSource,
    binding:
      binding === null
        ? null
        : {
            id: binding.id,
            version: binding.version.version,
            scope: binding.scopeType,
            workspace_id: binding.workspaceId,
            target: {
              adapter_type: binding.adapterType,
              endpoint_kind: binding.endpointKind,
              endpoint_class: binding.endpointClass,
              endpoint_url: binding.endpoint,
              external_project_id: binding.externalProjectId,
            },
            lifecycle_status: binding.status,
            config: {
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
            current_credential_version: input.currentCredentialVersion,
          },
    capture: {
      pinned_mode: pin.effectiveCaptureMode,
      effective_mode: effectiveSessionObservabilityContextCaptureMode({
        pin,
        platformPolicy,
        organizationSetting,
        workspaceSetting,
      }),
      current_ceilings: {
        platform: platformPolicy.maxCaptureMode,
        organization: organizationSetting.captureCeiling,
        workspace: workspaceSetting.captureCeiling,
      },
    },
    epochs: {
      pinned: {
        organization_selection_epoch: pin.organizationSelectionEpoch,
        workspace_selection_epoch: pin.workspaceSelectionEpoch,
        organization_default_revocation_epoch: pin.organizationDefaultRevocationEpoch,
        organization_revocation_epoch: pin.organizationRevocationEpoch,
        workspace_revocation_epoch: pin.workspaceRevocationEpoch,
        binding_revocation_epoch: pin.bindingRevocationEpoch,
        platform_capture_restriction_epoch: pin.platformCaptureRestrictionEpoch,
        organization_capture_restriction_epoch: pin.organizationCaptureRestrictionEpoch,
        workspace_capture_restriction_epoch: pin.workspaceCaptureRestrictionEpoch,
        // Session create always writes zero. The row is mutable lifecycle state,
        // so returning its present value as pinned history would be false.
        session_revocation_epoch: 0,
      },
      current: {
        organization_selection_epoch: organizationSetting.selectionEpoch,
        workspace_selection_epoch: workspaceSetting.selectionEpoch,
        organization_default_revocation_epoch: organizationSetting.defaultRevocationEpoch,
        organization_revocation_epoch: organizationSetting.organizationRevocationEpoch,
        workspace_revocation_epoch: workspaceSetting.revocationEpoch,
        binding_revocation_epoch: binding?.revocationEpoch ?? 0,
        platform_capture_restriction_epoch: platformPolicy.captureRestrictionEpoch,
        organization_capture_restriction_epoch: organizationSetting.captureRestrictionEpoch,
        workspace_capture_restriction_epoch: workspaceSetting.captureRestrictionEpoch,
        session_revocation_epoch: pin.sessionRevocationEpoch,
      },
    },
  };
  switch (input.decision.status) {
    case 'enabled':
      return { ...common, status: 'enabled', reason: null };
    case 'disabled':
      return { ...common, status: 'disabled', reason: 'session_pin_disabled' };
    case 'suppressed':
      return { ...common, status: 'suppressed', reason: input.decision.reason };
  }
}

function hasValidBindingConfiguration(binding: ContextBinding): boolean {
  const version = binding.version;
  return isValidAgentObservabilityAdapterConfiguration(
    binding.adapterType,
    binding.endpointKind,
    binding.externalProjectId,
    version.semanticProfile,
    version.protocol,
  );
}

function isOrganizationStatus(value: unknown): value is 'active' | 'archived' {
  return value === 'active' || value === 'archived';
}

function isWorkspaceStatus(value: unknown): value is 'active' | 'archived' {
  return value === 'active' || value === 'archived';
}

function isSelectionSource(
  value: unknown,
): value is 'organization_default' | 'workspace_custom' | 'disabled' {
  return value === 'organization_default' || value === 'workspace_custom' || value === 'disabled';
}

function isPinStatus(value: unknown): value is 'active' | 'disabled' | 'archived' | 'deleted' {
  return value === 'active' || value === 'disabled' || value === 'archived' || value === 'deleted';
}

function hasValidWorkspaceArchiveShape(
  status: 'active' | 'archived',
  archivedAt: unknown,
): boolean {
  return (status === 'archived') === isValidDate(archivedAt);
}

function hasValidBindingArchiveShape(
  status: AgentObservabilityBindingStatus,
  archivedAt: unknown,
): boolean {
  return (status === 'archived') === isValidDate(archivedAt);
}

function hasValidPinLifecycleShape(
  status: 'active' | 'disabled' | 'archived' | 'deleted',
  archivedAt: unknown,
  deletedAt: unknown,
): boolean {
  if (status === 'active' || status === 'disabled')
    return archivedAt === null && deletedAt === null;
  if (status === 'archived') return isValidDate(archivedAt) && deletedAt === null;
  return isNullableDate(archivedAt) && isValidDate(deletedAt);
}

function hasValidTimestampPair(createdAt: unknown, updatedAt: unknown): boolean {
  return (
    isValidDate(createdAt) && isValidDate(updatedAt) && updatedAt.getTime() >= createdAt.getTime()
  );
}

function isValidDate(value: unknown): value is Date {
  return value instanceof Date && Number.isFinite(value.getTime());
}

function isNullableDate(value: unknown): value is Date | null {
  return value === null || isValidDate(value);
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

function finiteNumberInRange(value: unknown, minimum: number, maximum: number): number | null {
  const number =
    typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
  return Number.isFinite(number) && number >= minimum && number <= maximum ? number : null;
}

function arrayEvery<T>(value: unknown, predicate: (item: unknown) => item is T): value is T[] {
  return Array.isArray(value) && value.every(predicate);
}

function unavailable(
  reason: ContextUnavailableReason,
): AgentObservabilitySessionContextUnavailableError {
  return new AgentObservabilitySessionContextUnavailableError(reason);
}
