// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type {
  AgentObservabilityBindingView,
  WorkspaceAgentObservabilityState,
} from '../contracts/agent-observability.contract.js';
import { and, eq } from 'drizzle-orm';
import type { DbTransaction } from '../persistence/postgres/client.js';
import {
  agentObservabilityBindingVersions,
  agentObservabilityBindings,
} from '../persistence/postgres/schema.js';
import {
  AGENT_OBSERVABILITY_CONFIG_VERSION_MAX,
  AGENT_OBSERVABILITY_CREDENTIAL_VERSION_MAX,
  AgentObservabilityMutationValidationError,
  acquireAgentObservabilityMutationReservation,
  agentObservabilityMutationBodyHash,
  finalizeAgentObservabilityMutation,
  lockAgentObservabilityAdminIdempotencyPartition,
  lookupAgentObservabilityMutationIdempotencyByIdentity,
  type AgentObservabilityExpectedVersions,
  type AgentObservabilityMutationResponse,
  type AgentObservabilityMutationTarget,
  type AgentObservabilityReservationReference,
} from './agent-observability-mutations.js';
import {
  WorkspaceAgentObservabilityCredentialRotationNotRotatableError,
  WorkspaceAgentObservabilityMutationInvariantError,
  WorkspaceAgentObservabilityMutationNotFoundError,
  WorkspaceAgentObservabilityMutationRequestError,
  WorkspaceAgentObservabilityMutationUnavailableError,
  applyWorkspaceAgentObservabilityCredentialRotation,
  assertWorkspaceAgentObservabilityCredentialRotationPrecondition,
  encodeWorkspaceAgentObservabilityCredentialRotation,
  loadWorkspaceAgentObservabilityCredentialRotationHead,
  lockWorkspaceAgentObservabilityMutationAuthority,
  workspaceAgentObservabilityCredentialRotationBody,
  type NormalizedWorkspaceAgentObservabilityCredentialRotationRequest,
} from './agent-observability-workspace-mutation.js';
import {
  isAgentObservabilityCaptureModeAtMost,
  isValidAgentObservabilityAdapterConfiguration,
} from './agent-observability-policy.js';
import { newAgentObservabilitySecretReference } from './agent-observability-secrets.js';
import {
  reportWorkspaceAgentObservabilityMutationUnexpected,
  runWorkspaceAgentObservabilityAcquisitionWithRetry,
  runWorkspaceAgentObservabilityFinalizationWithRetry,
  settleWorkspaceAgentObservabilityFinalizationLoser,
  workspaceAgentObservabilityFinalizationFailureCode,
  writeWorkspaceAgentObservabilityStagedBundle,
  type WorkspaceAgentObservabilityMutationExecutionInput,
  type WorkspaceAgentObservabilityMutationUnexpectedPhase,
} from './agent-observability-workspace-service-common.js';

const WORKSPACE_AGENT_OBSERVABILITY_CREDENTIAL_ROTATION_SCOPE =
  'workspace_agent_observability.rotate_credentials' as const;

type WorkspaceCredentialRotationTarget = Extract<
  AgentObservabilityMutationTarget,
  { type: 'binding'; bindingScope: 'workspace' }
>;

type WorkspaceCredentialRotationAcquisitionPlan = {
  target: WorkspaceCredentialRotationTarget;
  expectedVersions: AgentObservabilityExpectedVersions;
  reservation: AgentObservabilityReservationReference;
  bindingId: string;
  candidate: {
    bundle: string;
    supersededSecretRef: ReturnType<typeof newAgentObservabilitySecretReference>;
  };
};

/** Route replay has durable target/version metadata; generic acquire never does. */
type WorkspaceCredentialRotationAcquisitionOutcome =
  | { type: 'precondition_missing' }
  | { type: 'precondition_stale' }
  | { type: 'not_rotatable' }
  | { type: 'corrupt_cache' }
  | { type: 'lookup_conflict' }
  | { type: 'live_pending' }
  | {
      type: 'route_replay';
      target: WorkspaceCredentialRotationTarget;
      expectedVersions: AgentObservabilityExpectedVersions;
      response: { status: number; body: AgentObservabilityMutationResponse };
    }
  | { type: 'kernel_acquired'; plan: WorkspaceCredentialRotationAcquisitionPlan }
  | { type: 'kernel_conflict' };

export interface ExecuteWorkspaceAgentObservabilityCredentialRotationInput extends WorkspaceAgentObservabilityMutationExecutionInput {
  request: NormalizedWorkspaceAgentObservabilityCredentialRotationRequest;
}

export type ExecuteWorkspaceAgentObservabilityCredentialRotationResult =
  | { kind: 'success'; status: 200; body: WorkspaceAgentObservabilityState }
  | { kind: 'bad_request' }
  | { kind: 'not_found' }
  | { kind: 'conflict' }
  | { kind: 'stale' }
  | { kind: 'precondition_required' }
  | { kind: 'unavailable' };

/**
 * Rotates one current workspace-custom OTLP credential head. Target/config,
 * selection/revocation/capture epochs, and Session pins remain unchanged.
 */
export async function executeWorkspaceAgentObservabilityCredentialRotation(
  input: ExecuteWorkspaceAgentObservabilityCredentialRotationInput,
): Promise<ExecuteWorkspaceAgentObservabilityCredentialRotationResult> {
  const idempotency = {
    organizationId: input.organizationId,
    principal: input.principal,
    scope: `${WORKSPACE_AGENT_OBSERVABILITY_CREDENTIAL_ROTATION_SCOPE}:${input.workspaceId}`,
    key: input.idempotencyKey,
  };
  let bodyHash: string;
  try {
    bodyHash = agentObservabilityMutationBodyHash(
      workspaceAgentObservabilityCredentialRotationBody(input.request),
    );
  } catch (error) {
    reportWorkspaceAgentObservabilityMutationUnexpected(
      input,
      'acquisition',
      error instanceof AgentObservabilityMutationValidationError
        ? 'invariant_violation'
        : 'unexpected_database_or_programmer',
    );
    return { kind: 'unavailable' };
  }

  let acquired: WorkspaceCredentialRotationAcquisitionOutcome;
  try {
    acquired =
      await runWorkspaceAgentObservabilityAcquisitionWithRetry<WorkspaceCredentialRotationAcquisitionOutcome>(
        input,
        async (tx) => {
          await input.hooks?.beforeAcquisitionAuthorityLock?.();
          // Validate exact active workspace ownership before replay or pending
          // idempotency can take precedence over current preconditions.
          const authority = await lockWorkspaceAgentObservabilityMutationAuthority(
            tx,
            input.organizationId,
            input.workspaceId,
            { credentialHeadLock: 'update' },
          );
          await input.hooks?.afterAcquisitionAuthorityLocked?.();

          await lockAgentObservabilityAdminIdempotencyPartition(tx, idempotency);
          let replay: Awaited<
            ReturnType<typeof lookupAgentObservabilityMutationIdempotencyByIdentity>
          >;
          try {
            replay = await lookupAgentObservabilityMutationIdempotencyByIdentity(tx, {
              idempotency,
              bodyHash,
            });
            await assertWorkspaceCredentialRotationIdempotency(
              tx,
              replay,
              input.organizationId,
              input.workspaceId,
            );
          } catch (error) {
            if (error instanceof AgentObservabilityMutationValidationError) {
              return { type: 'corrupt_cache' } as const;
            }
            throw error;
          }
          if (replay.kind === 'replay') {
            assertWorkspaceCredentialRotationTarget(
              replay.target,
              input.organizationId,
              input.workspaceId,
            );
            return {
              type: 'route_replay',
              target: replay.target,
              expectedVersions: replay.expectedVersions,
              response: replay.response,
            };
          }
          if (replay.kind === 'conflict') return { type: 'lookup_conflict' };
          if (replay.kind === 'pending' && replay.reservationLive) {
            return { type: 'live_pending' };
          }

          const precondition = assertWorkspaceAgentObservabilityCredentialRotationPrecondition({
            currentEtag: authority.stateVersion,
            ifMatch: input.ifMatch,
          });
          if (precondition === 'missing') return { type: 'precondition_missing' };
          if (precondition === 'stale') return { type: 'precondition_stale' };

          const head = await loadWorkspaceAgentObservabilityCredentialRotationHead(tx, authority);
          if (head === null) return { type: 'not_rotatable' };
          if (input.secretStore === undefined) {
            throw new WorkspaceAgentObservabilityMutationUnavailableError();
          }
          const nextCredentialVersion = head.credentialVersion + 1;
          const target = {
            type: 'binding',
            organizationId: input.organizationId,
            workspaceId: input.workspaceId,
            bindingId: head.bindingId,
            bindingScope: 'workspace',
          } as const;
          const candidateSecretRef = newAgentObservabilitySecretReference();
          const candidate = {
            bundle: encodeWorkspaceAgentObservabilityCredentialRotation({
              request: input.request,
              bindingId: head.bindingId,
              credentialVersion: nextCredentialVersion,
            }),
            supersededSecretRef: head.secretRef,
          };
          const expectedVersions: AgentObservabilityExpectedVersions = {
            stateVersion: authority.stateVersion,
            configVersion: head.configVersion,
            credentialVersion: head.credentialVersion,
          };
          const result = await acquireAgentObservabilityMutationReservation(tx, {
            target,
            idempotency,
            bodyHash,
            expectedVersions,
            staging: {
              candidateBindingId: head.bindingId,
              proposedCredentialVersion: nextCredentialVersion,
              secretRef: candidateSecretRef,
            },
          });
          // The route-scoped lookup holds this idempotency partition. A generic
          // replay has no historic target/version metadata, so it is an
          // impossible durable transition rather than a replayable result.
          if (result.kind === 'replay') {
            throw new WorkspaceAgentObservabilityMutationInvariantError();
          }
          if (result.kind !== 'acquired') return { type: 'kernel_conflict' };
          return {
            type: 'kernel_acquired',
            plan: {
              target,
              expectedVersions,
              reservation: result.reservation,
              bindingId: head.bindingId,
              candidate,
            },
          };
        },
      );
  } catch (error) {
    if (error instanceof WorkspaceAgentObservabilityMutationRequestError) {
      return { kind: 'bad_request' };
    }
    if (error instanceof WorkspaceAgentObservabilityMutationNotFoundError) {
      return { kind: 'not_found' };
    }
    if (error instanceof WorkspaceAgentObservabilityCredentialRotationNotRotatableError) {
      return { kind: 'conflict' };
    }
    if (error instanceof WorkspaceAgentObservabilityMutationUnavailableError) {
      return { kind: 'unavailable' };
    }
    reportWorkspaceAgentObservabilityMutationUnexpected(
      input,
      'acquisition',
      error instanceof AgentObservabilityMutationValidationError ||
        error instanceof WorkspaceAgentObservabilityMutationInvariantError
        ? 'invariant_violation'
        : 'unexpected_database_or_programmer',
    );
    return { kind: 'unavailable' };
  }

  if (acquired.type === 'precondition_missing') return { kind: 'precondition_required' };
  if (acquired.type === 'precondition_stale') return { kind: 'stale' };
  if (acquired.type === 'not_rotatable') return { kind: 'conflict' };
  if (acquired.type === 'live_pending') return { kind: 'conflict' };
  if (acquired.type === 'lookup_conflict' || acquired.type === 'kernel_conflict') {
    return { kind: 'conflict' };
  }
  if (acquired.type === 'corrupt_cache') {
    reportWorkspaceAgentObservabilityMutationUnexpected(input, 'acquisition', 'corrupt_cache');
    return { kind: 'unavailable' };
  }
  if (acquired.type === 'route_replay') {
    return successfulWorkspaceCredentialRotationResponse(
      input,
      acquired.target,
      acquired.expectedVersions,
      acquired.response,
      'acquisition',
    );
  }
  const plan = acquired.plan;

  const staged = await writeWorkspaceAgentObservabilityStagedBundle(
    input,
    plan.reservation,
    plan.candidate.bundle,
  );
  if (staged === 'preempted') return { kind: 'conflict' };
  if (staged === 'failed') return { kind: 'unavailable' };

  let finalized: Awaited<ReturnType<typeof finalizeAgentObservabilityMutation>>;
  try {
    await input.hooks?.beforeFinalizationAuthorityLock?.();
    finalized = await runWorkspaceAgentObservabilityFinalizationWithRetry(input, async (tx) => {
      const authority = await lockWorkspaceAgentObservabilityMutationAuthority(
        tx,
        input.organizationId,
        input.workspaceId,
        { credentialHeadLock: 'update' },
      );
      await input.hooks?.afterFinalizationAuthorityLocked?.();
      if (authority.stateVersion !== plan.expectedVersions.stateVersion) {
        return { kind: 'expected_versions_mismatch' } as const;
      }
      const head = await loadWorkspaceAgentObservabilityCredentialRotationHead(tx, authority);
      if (
        head === null ||
        head.bindingId !== plan.bindingId ||
        head.configVersion !== plan.expectedVersions.configVersion ||
        head.credentialVersion !== plan.expectedVersions.credentialVersion ||
        head.secretRef !== plan.candidate.supersededSecretRef
      ) {
        return { kind: 'expected_versions_mismatch' } as const;
      }
      return finalizeAgentObservabilityMutation(tx, {
        reservation: plan.reservation,
        expectedVersions: plan.expectedVersions,
        supersededSecretRef: plan.candidate.supersededSecretRef,
        responseStatus: 200,
        audit: {
          action: 'workspace.agent_observability.credentials_rotated',
          authMethod: input.authMethod,
          requestId: input.requestId,
        },
        apply: async ({ db: mutationDb, stagedActivation }) =>
          applyWorkspaceAgentObservabilityCredentialRotation({
            db: mutationDb,
            authority,
            head,
            actor: input.principal,
            now: new Date(),
            stagedActivation,
          }),
      });
    });
  } catch (error) {
    // Archive can win after the staged write. It is a non-leaking workspace
    // outcome once the exact loser reservation has handed off cleanup.
    if (error instanceof WorkspaceAgentObservabilityMutationNotFoundError) {
      const settlement = await settleWorkspaceAgentObservabilityFinalizationLoser(
        input,
        plan.reservation,
      );
      return settlement === 'conflict' ? { kind: 'not_found' } : { kind: 'unavailable' };
    }
    reportWorkspaceAgentObservabilityMutationUnexpected(
      input,
      'finalization',
      error instanceof AgentObservabilityMutationValidationError ||
        error instanceof WorkspaceAgentObservabilityMutationInvariantError
        ? 'invariant_violation'
        : 'unexpected_database_or_programmer',
    );
    await settleWorkspaceAgentObservabilityFinalizationLoser(input, plan.reservation);
    return { kind: 'unavailable' };
  }

  if (finalized.kind === 'committed') {
    return successfulWorkspaceCredentialRotationResponse(
      input,
      plan.target,
      plan.expectedVersions,
      finalized.response,
      'finalization',
    );
  }
  const finalizationFailureCode = workspaceAgentObservabilityFinalizationFailureCode(finalized);
  if (finalizationFailureCode !== undefined) {
    reportWorkspaceAgentObservabilityMutationUnexpected(
      input,
      'finalization',
      finalizationFailureCode,
    );
  }
  const settlement = await settleWorkspaceAgentObservabilityFinalizationLoser(
    input,
    plan.reservation,
  );
  if (finalizationFailureCode !== undefined) return { kind: 'unavailable' };
  return settlement === 'conflict' ? { kind: 'conflict' } : { kind: 'unavailable' };
}

async function assertWorkspaceCredentialRotationIdempotency(
  tx: DbTransaction,
  result: Awaited<ReturnType<typeof lookupAgentObservabilityMutationIdempotencyByIdentity>>,
  organizationId: string,
  workspaceId: string,
): Promise<void> {
  if (result.kind === 'absent' || result.kind === 'conflict') return;
  assertWorkspaceCredentialRotationTarget(result.target, organizationId, workspaceId);
  if (
    result.kind === 'replay' &&
    !(await hasExactWorkspaceCredentialRotationReplayBinding(
      tx,
      result.response,
      result.target,
      result.expectedVersions,
      organizationId,
      workspaceId,
    ))
  ) {
    throw new AgentObservabilityMutationValidationError(
      'workspace agent observability credential rotation replay is corrupt',
    );
  }
}

/**
 * Reservation metadata identifies only a binding. Re-read its immutable target
 * and exact historical config generation so a syntactically valid cached state
 * cannot substitute another endpoint or policy under that binding ID.
 */
async function hasExactWorkspaceCredentialRotationReplayBinding(
  tx: DbTransaction,
  response: { status: number; body: AgentObservabilityMutationResponse },
  target: Extract<AgentObservabilityMutationTarget, { type: 'binding'; bindingScope: 'workspace' }>,
  expectedVersions: AgentObservabilityExpectedVersions,
  organizationId: string,
  workspaceId: string,
): Promise<boolean> {
  if (
    !isWorkspaceCredentialRotationResponse(
      response,
      target,
      expectedVersions,
      organizationId,
      workspaceId,
    )
  ) {
    return false;
  }
  const binding = response.body.configured.binding;
  if (binding === null || expectedVersions.configVersion === null) return false;
  const persisted = (
    await tx
      .select({
        adapterType: agentObservabilityBindings.adapterType,
        endpointKind: agentObservabilityBindings.endpointKind,
        endpointClass: agentObservabilityBindings.endpointClass,
        endpoint: agentObservabilityBindings.endpoint,
        externalProjectId: agentObservabilityBindings.externalProjectId,
        version: agentObservabilityBindingVersions.version,
        versionAdapterType: agentObservabilityBindingVersions.adapterType,
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
      .from(agentObservabilityBindings)
      .innerJoin(
        agentObservabilityBindingVersions,
        and(
          eq(agentObservabilityBindingVersions.bindingId, agentObservabilityBindings.id),
          eq(agentObservabilityBindingVersions.version, expectedVersions.configVersion),
        ),
      )
      .where(
        and(
          eq(agentObservabilityBindings.id, target.bindingId),
          eq(agentObservabilityBindings.organizationId, organizationId),
          eq(agentObservabilityBindings.workspaceId, workspaceId),
          eq(agentObservabilityBindings.scopeType, 'workspace'),
        ),
      )
      .for('share')
      .limit(1)
  )[0];
  if (!persisted || !Number.isFinite(Number(persisted.sampleRate))) return false;
  return (
    persisted.adapterType === binding.target.adapter_type &&
    persisted.endpointKind === binding.target.endpoint_kind &&
    persisted.endpointClass === binding.target.endpoint_class &&
    persisted.endpoint === binding.target.endpoint_url &&
    persisted.externalProjectId === binding.target.external_project_id &&
    persisted.version === binding.config.version &&
    persisted.versionAdapterType === binding.target.adapter_type &&
    persisted.semanticProfile === binding.config.semantic_profile &&
    persisted.protocol === binding.config.protocol &&
    persisted.compression === binding.config.compression &&
    persisted.timeoutMs === binding.config.timeout_ms &&
    persisted.environment === binding.config.environment &&
    persisted.release === binding.config.release &&
    persisted.captureMode === binding.config.capture_mode &&
    Number(persisted.sampleRate) === binding.config.sample_rate &&
    persisted.configSchemaVersion === binding.config.config_schema_version
  );
}

function assertWorkspaceCredentialRotationTarget(
  target: AgentObservabilityMutationTarget,
  organizationId: string,
  workspaceId: string,
): asserts target is Extract<
  AgentObservabilityMutationTarget,
  { type: 'binding'; bindingScope: 'workspace' }
> {
  if (
    target.type !== 'binding' ||
    target.bindingScope !== 'workspace' ||
    target.organizationId !== organizationId ||
    target.workspaceId !== workspaceId
  ) {
    throw new AgentObservabilityMutationValidationError(
      'workspace agent observability credential rotation target is corrupt',
    );
  }
}

function successfulWorkspaceCredentialRotationResponse(
  input: Pick<
    ExecuteWorkspaceAgentObservabilityCredentialRotationInput,
    'organizationId' | 'workspaceId' | 'requestId' | 'reporter'
  >,
  target: AgentObservabilityMutationTarget,
  expectedVersions: AgentObservabilityExpectedVersions,
  response: { status: number; body: AgentObservabilityMutationResponse },
  phase: WorkspaceAgentObservabilityMutationUnexpectedPhase,
): ExecuteWorkspaceAgentObservabilityCredentialRotationResult {
  if (
    !isWorkspaceCredentialRotationResponse(
      response,
      target,
      expectedVersions,
      input.organizationId,
      input.workspaceId,
    )
  ) {
    reportWorkspaceAgentObservabilityMutationUnexpected(
      input,
      phase,
      'invalid_authoritative_response',
    );
    return { kind: 'unavailable' };
  }
  return { kind: 'success', status: 200, body: response.body };
}

function isWorkspaceCredentialRotationResponse(
  response: { status: number; body: AgentObservabilityMutationResponse },
  target: AgentObservabilityMutationTarget,
  expectedVersions: AgentObservabilityExpectedVersions,
  organizationId: string,
  workspaceId: string,
): response is { status: 200; body: WorkspaceAgentObservabilityState } {
  if (
    response.status !== 200 ||
    response.body.scope !== 'workspace' ||
    response.body.organization_id !== organizationId ||
    response.body.workspace_id !== workspaceId
  ) {
    return false;
  }
  try {
    assertWorkspaceCredentialRotationTarget(target, organizationId, workspaceId);
  } catch {
    return false;
  }
  const configVersion = expectedVersions.configVersion;
  const credentialVersion = expectedVersions.credentialVersion;
  if (
    !isPositiveSafeInteger(configVersion) ||
    configVersion > AGENT_OBSERVABILITY_CONFIG_VERSION_MAX ||
    !isPositiveSafeInteger(credentialVersion) ||
    credentialVersion >= AGENT_OBSERVABILITY_CREDENTIAL_VERSION_MAX
  ) {
    return false;
  }
  const configured = response.body.configured;
  const binding = configured.binding;
  if (
    configured.mode !== 'custom' ||
    binding === null ||
    binding.id !== target.bindingId ||
    binding.scope !== 'workspace' ||
    binding.organization_id !== organizationId ||
    binding.workspace_id !== workspaceId ||
    binding.status !== 'active' ||
    binding.target.adapter_type !== 'otlp_http' ||
    binding.config.version !== configVersion ||
    binding.credential.configured !== true ||
    binding.credential.version !== credentialVersion + 1
  ) {
    return false;
  }
  const configurationIsValid = isValidAgentObservabilityAdapterConfiguration(
    binding.target.adapter_type,
    binding.target.endpoint_kind,
    binding.target.external_project_id,
    binding.config.semantic_profile,
    binding.config.protocol,
  );
  const effective = response.body.effective;
  if (
    effective.source !== 'workspace_custom' ||
    effective.binding === null ||
    !sameWorkspaceCredentialRotationBinding(effective.binding, binding)
  ) {
    return false;
  }
  if (effective.status === 'enabled') {
    return (
      effective.disabled_reason === null &&
      configurationIsValid &&
      isAgentObservabilityCaptureModeAtMost(effective.capture_mode, configured.capture_ceiling) &&
      isAgentObservabilityCaptureModeAtMost(effective.capture_mode, binding.config.capture_mode)
    );
  }
  if (effective.capture_mode !== 'metadata_only') return false;
  if (effective.disabled_reason === 'binding_configuration_invalid') {
    return !configurationIsValid;
  }
  // Platform policy is not included in durable route replay metadata. Both
  // policy-disabled reasons remain coherent for an otherwise active binding.
  return (
    effective.disabled_reason === 'platform_adapter_disallowed' ||
    effective.disabled_reason === 'platform_endpoint_class_disallowed'
  );
}

function sameWorkspaceCredentialRotationBinding(
  left: AgentObservabilityBindingView,
  right: AgentObservabilityBindingView,
): boolean {
  return (
    left.id === right.id &&
    left.scope === right.scope &&
    left.organization_id === right.organization_id &&
    left.workspace_id === right.workspace_id &&
    left.status === right.status &&
    left.target.adapter_type === right.target.adapter_type &&
    left.target.external_project_id === right.target.external_project_id &&
    left.target.endpoint_kind === right.target.endpoint_kind &&
    left.target.endpoint_class === right.target.endpoint_class &&
    left.target.endpoint_url === right.target.endpoint_url &&
    left.config.version === right.config.version &&
    left.config.semantic_profile === right.config.semantic_profile &&
    left.config.protocol === right.config.protocol &&
    left.config.compression === right.config.compression &&
    left.config.timeout_ms === right.config.timeout_ms &&
    left.config.environment === right.config.environment &&
    left.config.release === right.config.release &&
    left.config.capture_mode === right.config.capture_mode &&
    left.config.sample_rate === right.config.sample_rate &&
    left.config.config_schema_version === right.config.config_schema_version &&
    left.credential.configured === right.credential.configured &&
    left.credential.version === right.credential.version &&
    left.credential.key_hint === right.credential.key_hint &&
    left.credential.rotated_at === right.credential.rotated_at
  );
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}
