// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type {
  AgentObservabilityBindingView,
  WorkspaceAgentObservabilityState,
} from '../contracts/agent-observability.contract.js';
import {
  AGENT_OBSERVABILITY_CONFIG_VERSION_MAX,
  acquireAgentObservabilityMutationReservation,
  agentObservabilityMutationBodyHash,
  AgentObservabilityMutationValidationError,
  finalizeAgentObservabilityMutation,
  lockAgentObservabilityAdminIdempotencyPartition,
  lookupAgentObservabilityMutationIdempotencyByIdentity,
  preemptWorkspaceAgentObservabilityMutationReservations,
  type AgentObservabilityExpectedVersions,
  type AgentObservabilityMutationResponse,
  type AgentObservabilityMutationTarget,
  type AgentObservabilityReservationReference,
} from './agent-observability-mutations.js';
import {
  WorkspaceAgentObservabilityMutationInvariantError,
  WorkspaceAgentObservabilityMutationNotFoundError,
  WorkspaceAgentObservabilityMutationRequestError,
  WorkspaceAgentObservabilityMutationUnavailableError,
  applyWorkspaceAgentObservabilityMutation,
  assertWorkspaceAgentObservabilityMutationCapacity,
  assertWorkspaceAgentObservabilityMutationRequest,
  assertWorkspaceAgentObservabilityPrecondition,
  assertWorkspaceAgentObservabilityTargetAdmission,
  classifyWorkspaceAgentObservabilityMutation,
  encodeWorkspaceAgentObservabilityCredentials,
  lockWorkspaceAgentObservabilityMutationAuthority,
  workspaceAgentObservabilityDisablePreemptionTargets,
  workspaceAgentObservabilityMutationBody,
  type NormalizedWorkspaceAgentObservabilityPutRequest,
  type WorkspaceAgentObservabilityMutationKind,
} from './agent-observability-workspace-mutation.js';
import {
  isAgentObservabilityCaptureModeAtMost,
  isValidAgentObservabilityAdapterConfiguration,
} from './agent-observability-policy.js';
import {
  reportWorkspaceAgentObservabilityMutationUnexpected,
  runWorkspaceAgentObservabilityFinalizationWithRetry,
  settleWorkspaceAgentObservabilityFinalizationLoser,
  workspaceAgentObservabilityFinalizationFailureCode,
  writeWorkspaceAgentObservabilityStagedBundle,
  type WorkspaceAgentObservabilityMutationExecutionInput,
  type WorkspaceAgentObservabilityMutationUnexpectedPhase,
} from './agent-observability-workspace-service-common.js';
import { newAgentObservabilitySecretReference } from './agent-observability-secrets.js';
import { newId } from './versioning.js';

const WORKSPACE_AGENT_OBSERVABILITY_PUT_SCOPE = 'workspace_agent_observability.put' as const;

export interface ExecuteWorkspaceAgentObservabilityPutInput extends WorkspaceAgentObservabilityMutationExecutionInput {
  request: NormalizedWorkspaceAgentObservabilityPutRequest;
}

export type ExecuteWorkspaceAgentObservabilityPutResult =
  | { kind: 'success'; status: 200 | 201; body: WorkspaceAgentObservabilityState }
  | { kind: 'bad_request' }
  | { kind: 'not_found' }
  | { kind: 'conflict' }
  | { kind: 'stale' }
  | { kind: 'precondition_required' }
  | { kind: 'unavailable' };

/**
 * Workspace PUT application boundary. Its route supplies trusted path/auth
 * metadata only; all authority locks, staging, preemption, audit, and cached
 * state response behavior stays below this boundary.
 */
export async function executeWorkspaceAgentObservabilityPut(
  input: ExecuteWorkspaceAgentObservabilityPutInput,
): Promise<ExecuteWorkspaceAgentObservabilityPutResult> {
  let bodyHash: string;
  try {
    bodyHash = agentObservabilityMutationBodyHash(
      workspaceAgentObservabilityMutationBody(input.request),
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
  if (input.request.mode === 'disabled') {
    return executeWorkspaceAgentObservabilityExplicitDisable(input, bodyHash);
  }
  return executeOrdinaryWorkspaceAgentObservabilityPut(input, bodyHash);
}

async function executeOrdinaryWorkspaceAgentObservabilityPut(
  input: ExecuteWorkspaceAgentObservabilityPutInput,
  bodyHash: string,
): Promise<ExecuteWorkspaceAgentObservabilityPutResult> {
  const target = workspaceSettingTarget(input);
  const idempotency = workspaceIdempotency(input);

  type AcquisitionPlan = {
    kind: WorkspaceAgentObservabilityMutationKind;
    expectedVersions: AgentObservabilityExpectedVersions;
    reservation: AgentObservabilityReservationReference;
    candidate?: {
      bindingId: string;
      secretRef: ReturnType<typeof newAgentObservabilitySecretReference>;
      bundle: string;
    };
  };
  type AcquisitionOutcome =
    | { type: 'precondition_missing' }
    | { type: 'precondition_stale' }
    | { type: 'live_pending' }
    | { type: 'corrupt_cache' }
    | {
        type: 'kernel';
        result: Awaited<ReturnType<typeof acquireAgentObservabilityMutationReservation>>;
        plan?: AcquisitionPlan;
      };

  let acquired: AcquisitionOutcome;
  try {
    acquired = await input.db.transaction(async (tx) => {
      await input.hooks?.beforeAcquisitionAuthorityLock?.();
      const authority = await lockWorkspaceAgentObservabilityMutationAuthority(
        tx,
        input.organizationId,
        input.workspaceId,
      );
      await input.hooks?.afterAcquisitionAuthorityLocked?.();

      // The route scope carries the path workspace ID. Lock it before reading
      // idempotency so a live same-key retry wins 409 before precondition work.
      await lockAgentObservabilityAdminIdempotencyPartition(tx, idempotency);
      let replay: Awaited<ReturnType<typeof lookupAgentObservabilityMutationIdempotencyByIdentity>>;
      try {
        replay = await lookupAgentObservabilityMutationIdempotencyByIdentity(tx, {
          idempotency,
          bodyHash,
        });
        assertWorkspacePutIdempotencyTarget(
          replay,
          target,
          input.request,
          input.organizationId,
          input.workspaceId,
        );
      } catch (error) {
        if (error instanceof AgentObservabilityMutationValidationError) {
          return { type: 'corrupt_cache' } as const;
        }
        throw error;
      }
      if (replay.kind === 'replay' || replay.kind === 'conflict') {
        return { type: 'kernel', result: replay };
      }
      if (replay.kind === 'pending' && replay.reservationLive) {
        return { type: 'live_pending' };
      }

      const precondition = assertWorkspaceAgentObservabilityPrecondition({
        currentEtag: authority.stateVersion,
        ifMatch: input.ifMatch,
      });
      if (precondition === 'missing') return { type: 'precondition_missing' };
      if (precondition === 'stale') return { type: 'precondition_stale' };

      const kind = classifyWorkspaceAgentObservabilityMutation(authority, input.request);
      assertWorkspaceAgentObservabilityMutationRequest(input.request, kind);
      assertWorkspaceAgentObservabilityTargetAdmission(authority, input.request);
      assertWorkspaceAgentObservabilityMutationCapacity(authority, input.request, kind);
      const needsCredentials = kind.type === 'initial' || kind.type === 'replacement';
      if (needsCredentials && input.secretStore === undefined) {
        throw new WorkspaceAgentObservabilityMutationUnavailableError();
      }

      let candidate: AcquisitionPlan['candidate'];
      if (needsCredentials) {
        if (input.request.mode !== 'custom') {
          throw new WorkspaceAgentObservabilityMutationInvariantError();
        }
        const bindingId = newId('aob');
        const secretRef = newAgentObservabilitySecretReference();
        candidate = {
          bindingId,
          secretRef,
          bundle: encodeWorkspaceAgentObservabilityCredentials({
            request: input.request,
            candidateBindingId: bindingId,
          }),
        };
      }
      const expectedVersions: AgentObservabilityExpectedVersions = {
        stateVersion: authority.stateVersion,
        configVersion: authority.currentBinding?.configVersion ?? null,
        credentialVersion: authority.currentBinding?.credentialVersion ?? null,
      };
      const result = await acquireAgentObservabilityMutationReservation(tx, {
        target,
        idempotency,
        bodyHash,
        expectedVersions,
        ...(candidate === undefined
          ? {}
          : {
              staging: {
                candidateBindingId: candidate.bindingId,
                proposedCredentialVersion: 1,
                secretRef: candidate.secretRef,
              },
            }),
      });
      if (result.kind !== 'acquired') return { type: 'kernel', result };
      return {
        type: 'kernel',
        result,
        plan: {
          kind,
          expectedVersions,
          reservation: result.reservation,
          ...(candidate === undefined ? {} : { candidate }),
        },
      };
    });
  } catch (error) {
    return workspacePutAcquisitionError(input, error);
  }

  if (acquired.type === 'precondition_missing') return { kind: 'precondition_required' };
  if (acquired.type === 'precondition_stale') return { kind: 'stale' };
  if (acquired.type === 'live_pending') return { kind: 'conflict' };
  if (acquired.type === 'corrupt_cache') {
    reportWorkspaceAgentObservabilityMutationUnexpected(input, 'acquisition', 'corrupt_cache');
    return { kind: 'unavailable' };
  }
  if (acquired.result.kind === 'replay') {
    return successfulWorkspaceResponse(input, acquired.result.response, 'acquisition');
  }
  if (acquired.result.kind === 'conflict' || acquired.result.kind === 'in_progress') {
    return { kind: 'conflict' };
  }
  const plan = acquired.plan;
  if (plan === undefined) {
    reportWorkspaceAgentObservabilityMutationUnexpected(
      input,
      'acquisition',
      'invariant_violation',
    );
    return { kind: 'unavailable' };
  }

  if (plan.candidate !== undefined) {
    const staged = await writeWorkspaceAgentObservabilityStagedBundle(
      input,
      plan.reservation,
      plan.candidate.bundle,
    );
    if (staged === 'preempted') return { kind: 'conflict' };
    if (staged === 'failed') return { kind: 'unavailable' };
  }

  let finalized: Awaited<ReturnType<typeof finalizeAgentObservabilityMutation>>;
  try {
    await input.hooks?.beforeFinalizationAuthorityLock?.();
    finalized = await runWorkspaceAgentObservabilityFinalizationWithRetry(input, async (tx) => {
      const authority = await lockWorkspaceAgentObservabilityMutationAuthority(
        tx,
        input.organizationId,
        input.workspaceId,
      );
      await input.hooks?.afterFinalizationAuthorityLocked?.();
      if (authority.stateVersion !== plan.expectedVersions.stateVersion) {
        return { kind: 'expected_versions_mismatch' } as const;
      }
      const revalidatedKind = classifyWorkspaceAgentObservabilityMutation(authority, input.request);
      if (!sameWorkspaceAgentObservabilityMutationKind(plan.kind, revalidatedKind)) {
        return { kind: 'expected_versions_mismatch' } as const;
      }
      assertWorkspaceAgentObservabilityMutationRequest(input.request, revalidatedKind);
      assertWorkspaceAgentObservabilityTargetAdmission(authority, input.request);
      assertWorkspaceAgentObservabilityMutationCapacity(authority, input.request, revalidatedKind);
      return finalizeAgentObservabilityMutation(tx, {
        reservation: plan.reservation,
        expectedVersions: plan.expectedVersions,
        responseStatus: plan.kind.type === 'initial' ? 201 : 200,
        audit: {
          action: 'workspace.agent_observability.replaced',
          authMethod: input.authMethod,
          requestId: input.requestId,
        },
        apply: async ({ db: mutationDb, stagedActivation }) =>
          applyWorkspaceAgentObservabilityMutation({
            db: mutationDb,
            authority,
            request: input.request,
            kind: plan.kind,
            actor: input.principal,
            now: new Date(),
            stagedActivation,
          }),
      });
    });
  } catch (error) {
    // Archive can win after an ordinary request has staged credentials but
    // before its finalizer reacquires workspace authority. Exact loser
    // settlement owns both reservation and staged bundle; this is a normal
    // resource-not-found outcome once that handoff succeeds.
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
    return successfulWorkspaceResponse(input, finalized.response, 'finalization');
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

/**
 * Explicit disabled is the workspace emergency path. It retains authority locks
 * through ETag check, exact target fencing, acquisition, finalization, audit,
 * and cached response completion so no ordinary PUT can reopen the window.
 */
async function executeWorkspaceAgentObservabilityExplicitDisable(
  input: ExecuteWorkspaceAgentObservabilityPutInput,
  bodyHash: string,
): Promise<ExecuteWorkspaceAgentObservabilityPutResult> {
  const target = workspaceSettingTarget(input);
  const idempotency = workspaceIdempotency(input);
  type Outcome =
    | { type: 'precondition_missing' }
    | { type: 'precondition_stale' }
    | { type: 'live_pending' }
    | { type: 'corrupt_cache' }
    | {
        type: 'kernel';
        result: Awaited<ReturnType<typeof acquireAgentObservabilityMutationReservation>>;
      }
    | {
        type: 'finalized';
        result: Awaited<ReturnType<typeof finalizeAgentObservabilityMutation>>;
      };

  let phase: WorkspaceAgentObservabilityMutationUnexpectedPhase = 'acquisition';
  let outcome: Outcome;
  try {
    outcome = await runWorkspaceAgentObservabilityFinalizationWithRetry(
      input,
      async (tx) => {
        await input.hooks?.beforeAcquisitionAuthorityLock?.();
        const authority = await lockWorkspaceAgentObservabilityMutationAuthority(
          tx,
          input.organizationId,
          input.workspaceId,
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
          assertWorkspacePutIdempotencyTarget(
            replay,
            target,
            input.request,
            input.organizationId,
            input.workspaceId,
          );
        } catch (error) {
          if (error instanceof AgentObservabilityMutationValidationError) {
            return { type: 'corrupt_cache' } as const;
          }
          throw error;
        }
        if (replay.kind === 'replay' || replay.kind === 'conflict') {
          return { type: 'kernel', result: replay };
        }
        if (replay.kind === 'pending' && replay.reservationLive) {
          return { type: 'live_pending' };
        }

        const precondition = assertWorkspaceAgentObservabilityPrecondition({
          currentEtag: authority.stateVersion,
          ifMatch: input.ifMatch,
        });
        if (precondition === 'missing') return { type: 'precondition_missing' };
        if (precondition === 'stale') return { type: 'precondition_stale' };

        const kind = classifyWorkspaceAgentObservabilityMutation(authority, input.request);
        assertWorkspaceAgentObservabilityMutationRequest(input.request, kind);
        assertWorkspaceAgentObservabilityMutationCapacity(authority, input.request, kind);
        await preemptWorkspaceAgentObservabilityMutationReservations(tx, {
          organizationId: input.organizationId,
          workspaceId: input.workspaceId,
          targets: workspaceAgentObservabilityDisablePreemptionTargets(authority),
        });

        const expectedVersions: AgentObservabilityExpectedVersions = {
          stateVersion: authority.stateVersion,
          configVersion: authority.currentBinding?.configVersion ?? null,
          credentialVersion: authority.currentBinding?.credentialVersion ?? null,
        };
        const acquired = await acquireAgentObservabilityMutationReservation(tx, {
          target,
          idempotency,
          bodyHash,
          expectedVersions,
        });
        if (acquired.kind !== 'acquired') return { type: 'kernel', result: acquired };

        await input.hooks?.afterFinalizationAuthorityLocked?.();
        phase = 'finalization';
        return {
          type: 'finalized',
          result: await finalizeAgentObservabilityMutation(tx, {
            reservation: acquired.reservation,
            expectedVersions,
            responseStatus: 200,
            audit: {
              action: 'workspace.agent_observability.replaced',
              authMethod: input.authMethod,
              requestId: input.requestId,
            },
            apply: async ({ db: mutationDb, stagedActivation }) =>
              applyWorkspaceAgentObservabilityMutation({
                db: mutationDb,
                authority,
                request: input.request,
                kind,
                actor: input.principal,
                now: new Date(),
                stagedActivation,
              }),
          }),
        };
      },
      () => {
        // A retry is a fresh whole-transaction acquisition. Reset before any
        // per-attempt transaction hook can throw.
        phase = 'acquisition';
      },
    );
  } catch (error) {
    if (error instanceof WorkspaceAgentObservabilityMutationRequestError)
      return { kind: 'bad_request' };
    if (error instanceof WorkspaceAgentObservabilityMutationNotFoundError)
      return { kind: 'not_found' };
    if (error instanceof WorkspaceAgentObservabilityMutationUnavailableError) {
      return { kind: 'unavailable' };
    }
    reportWorkspaceAgentObservabilityMutationUnexpected(
      input,
      phase,
      error instanceof AgentObservabilityMutationValidationError ||
        error instanceof WorkspaceAgentObservabilityMutationInvariantError
        ? 'invariant_violation'
        : 'unexpected_database_or_programmer',
    );
    return { kind: 'unavailable' };
  }

  if (outcome.type === 'precondition_missing') return { kind: 'precondition_required' };
  if (outcome.type === 'precondition_stale') return { kind: 'stale' };
  if (outcome.type === 'live_pending') return { kind: 'conflict' };
  if (outcome.type === 'corrupt_cache') {
    reportWorkspaceAgentObservabilityMutationUnexpected(input, 'acquisition', 'corrupt_cache');
    return { kind: 'unavailable' };
  }
  if (outcome.type === 'kernel') {
    if (outcome.result.kind === 'replay') {
      return successfulWorkspaceResponse(input, outcome.result.response, 'acquisition');
    }
    return { kind: 'conflict' };
  }
  if (outcome.result.kind === 'committed') {
    return successfulWorkspaceResponse(input, outcome.result.response, 'finalization');
  }
  const finalizationFailureCode = workspaceAgentObservabilityFinalizationFailureCode(
    outcome.result,
  );
  if (finalizationFailureCode !== undefined) {
    reportWorkspaceAgentObservabilityMutationUnexpected(
      input,
      'finalization',
      finalizationFailureCode,
    );
    return { kind: 'unavailable' };
  }
  return { kind: 'conflict' };
}

function workspaceSettingTarget(
  input: Pick<ExecuteWorkspaceAgentObservabilityPutInput, 'organizationId' | 'workspaceId'>,
): Extract<AgentObservabilityMutationTarget, { type: 'workspace_setting' }> {
  return {
    type: 'workspace_setting',
    organizationId: input.organizationId,
    workspaceId: input.workspaceId,
  };
}

function workspaceIdempotency(
  input: Pick<
    ExecuteWorkspaceAgentObservabilityPutInput,
    'organizationId' | 'workspaceId' | 'principal' | 'idempotencyKey'
  >,
) {
  return {
    organizationId: input.organizationId,
    principal: input.principal,
    scope: `${WORKSPACE_AGENT_OBSERVABILITY_PUT_SCOPE}:${input.workspaceId}`,
    key: input.idempotencyKey,
  };
}

function assertWorkspacePutIdempotencyTarget(
  result: Awaited<ReturnType<typeof lookupAgentObservabilityMutationIdempotencyByIdentity>>,
  expected: Extract<AgentObservabilityMutationTarget, { type: 'workspace_setting' }>,
  request: NormalizedWorkspaceAgentObservabilityPutRequest,
  organizationId: string,
  workspaceId: string,
): void {
  if (result.kind === 'absent' || result.kind === 'conflict') return;
  if (
    result.target.type !== 'workspace_setting' ||
    result.target.organizationId !== expected.organizationId ||
    result.target.workspaceId !== expected.workspaceId
  ) {
    throw new AgentObservabilityMutationValidationError(
      'workspace agent observability idempotency target is corrupt',
    );
  }
  if (
    result.kind === 'replay' &&
    !isWorkspacePutReplayForRequest(
      result.response,
      request,
      organizationId,
      workspaceId,
      result.expectedVersions,
    )
  ) {
    throw new AgentObservabilityMutationValidationError(
      'workspace agent observability idempotency response is corrupt',
    );
  }
}

function isWorkspacePutReplayForRequest(
  response: { status: number; body: AgentObservabilityMutationResponse },
  request: NormalizedWorkspaceAgentObservabilityPutRequest,
  organizationId: string,
  workspaceId: string,
  expectedVersions: AgentObservabilityExpectedVersions,
): boolean {
  if (response.status !== 200 && response.status !== 201) return false;
  const body = response.body;
  if (
    body.scope !== 'workspace' ||
    body.organization_id !== organizationId ||
    body.workspace_id !== workspaceId
  ) {
    return false;
  }
  if (
    body.configured.mode !== request.mode ||
    body.configured.capture_ceiling !== request.captureCeiling
  ) {
    return false;
  }
  if (request.mode === 'disabled') {
    return (
      response.status === 200 &&
      body.configured.binding === null &&
      body.effective.source === 'none' &&
      body.effective.status === 'disabled' &&
      body.effective.disabled_reason === 'workspace_disabled' &&
      body.effective.binding === null &&
      body.effective.capture_mode === 'metadata_only'
    );
  }
  if (request.mode === 'inherit') {
    if (response.status !== 200 || body.configured.binding !== null) return false;
    if (
      body.effective.source === 'organization_default' &&
      body.effective.binding !== null &&
      hasOrganizationOwnedWorkspaceReplayBinding(body.effective.binding, organizationId)
    ) {
      if (body.effective.status === 'enabled') {
        return (
          body.effective.binding.status === 'active' &&
          isValidAgentObservabilityAdapterConfiguration(
            body.effective.binding.target.adapter_type,
            body.effective.binding.target.endpoint_kind,
            body.effective.binding.target.external_project_id,
            body.effective.binding.config.semantic_profile,
            body.effective.binding.config.protocol,
          ) &&
          body.effective.binding.credential.configured === true &&
          body.effective.binding.credential.version > 0 &&
          body.effective.disabled_reason === null &&
          isAgentObservabilityCaptureModeAtMost(
            body.effective.capture_mode,
            request.captureCeiling,
          ) &&
          isAgentObservabilityCaptureModeAtMost(
            body.effective.capture_mode,
            body.effective.binding.config.capture_mode,
          )
        );
      }
      return (
        body.effective.status === 'disabled' &&
        body.effective.capture_mode === 'metadata_only' &&
        isCoherentHistoricalOrganizationBindingDisabledState(
          body.effective.binding,
          body.effective.disabled_reason,
        )
      );
    }
    return (
      body.effective.source === 'none' &&
      body.effective.status === 'disabled' &&
      body.effective.disabled_reason === 'no_organization_default' &&
      body.effective.binding === null &&
      body.effective.capture_mode === 'metadata_only'
    );
  }

  const binding = body.configured.binding;
  return (
    binding !== null &&
    hasCoherentWorkspaceCustomReplayMutation(response.status, request, expectedVersions, binding) &&
    binding.scope === 'workspace' &&
    binding.organization_id === organizationId &&
    binding.workspace_id === workspaceId &&
    binding.status === 'active' &&
    binding.credential.configured === true &&
    binding.credential.version > 0 &&
    binding.target.adapter_type === request.target.adapterType &&
    binding.target.endpoint_kind === request.target.endpointKind &&
    binding.target.endpoint_class === request.target.endpointClass &&
    binding.target.endpoint_url === request.target.endpoint &&
    binding.target.external_project_id === request.target.externalProjectId &&
    binding.config.semantic_profile === request.config.semanticProfile &&
    binding.config.protocol === request.config.protocol &&
    binding.config.compression === request.config.compression &&
    binding.config.timeout_ms === request.config.timeoutMs &&
    binding.config.environment === request.config.environment &&
    binding.config.release === request.config.release &&
    binding.config.capture_mode === request.config.captureMode &&
    binding.config.sample_rate === request.config.sampleRate &&
    body.effective.source === 'workspace_custom' &&
    body.effective.status === 'enabled' &&
    body.effective.disabled_reason === null &&
    body.effective.binding !== null &&
    sameWorkspaceReplayBinding(body.effective.binding, binding) &&
    isAgentObservabilityCaptureModeAtMost(body.effective.capture_mode, request.captureCeiling) &&
    isAgentObservabilityCaptureModeAtMost(
      body.effective.capture_mode,
      body.effective.binding.config.capture_mode,
    )
  );
}

function hasCoherentWorkspaceCustomReplayMutation(
  status: number,
  request: Extract<NormalizedWorkspaceAgentObservabilityPutRequest, { mode: 'custom' }>,
  expectedVersions: AgentObservabilityExpectedVersions,
  binding: AgentObservabilityBindingView,
): boolean {
  const { configVersion, credentialVersion } = expectedVersions;
  if (request.credentials === undefined) {
    return (
      status === 200 &&
      configVersion !== null &&
      credentialVersion !== null &&
      configVersion < AGENT_OBSERVABILITY_CONFIG_VERSION_MAX &&
      binding.config.version === configVersion + 1 &&
      binding.credential.configured === true &&
      binding.credential.version === credentialVersion
    );
  }
  if (configVersion === null) {
    return (
      credentialVersion === null &&
      status === 201 &&
      binding.config.version === 1 &&
      binding.credential.configured === true &&
      binding.credential.version === 1
    );
  }
  return (
    status === 200 &&
    binding.config.version === 1 &&
    binding.credential.configured === true &&
    binding.credential.version === 1
  );
}

function hasOrganizationOwnedWorkspaceReplayBinding(
  binding: AgentObservabilityBindingView,
  organizationId: string,
): boolean {
  return (
    binding.scope === 'organization' &&
    binding.organization_id === organizationId &&
    binding.workspace_id === null
  );
}

function isCoherentHistoricalOrganizationBindingDisabledState(
  binding: AgentObservabilityBindingView,
  reason: string | null,
): boolean {
  if (reason === 'binding_draining') return binding.status === 'draining';
  if (reason === 'binding_disabled') return binding.status === 'disabled';
  if (reason === 'binding_archived') return binding.status === 'archived';
  if (reason === 'credential_not_configured') {
    return (
      binding.status === 'active' &&
      binding.credential.configured === false &&
      binding.credential.version === null &&
      isValidAgentObservabilityAdapterConfiguration(
        binding.target.adapter_type,
        binding.target.endpoint_kind,
        binding.target.external_project_id,
        binding.config.semantic_profile,
        binding.config.protocol,
      )
    );
  }
  if (reason === 'binding_configuration_invalid') {
    return (
      binding.status === 'active' &&
      !isValidAgentObservabilityAdapterConfiguration(
        binding.target.adapter_type,
        binding.target.endpoint_kind,
        binding.target.external_project_id,
        binding.config.semantic_profile,
        binding.config.protocol,
      )
    );
  }
  return (
    binding.status === 'active' &&
    (reason === 'platform_adapter_disallowed' || reason === 'platform_endpoint_class_disallowed')
  );
}

/** Cached configured/effective views must name exact same workspace binding. */
function sameWorkspaceReplayBinding(
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

function workspacePutAcquisitionError(
  input: ExecuteWorkspaceAgentObservabilityPutInput,
  error: unknown,
): ExecuteWorkspaceAgentObservabilityPutResult {
  if (error instanceof WorkspaceAgentObservabilityMutationRequestError)
    return { kind: 'bad_request' };
  if (error instanceof WorkspaceAgentObservabilityMutationNotFoundError)
    return { kind: 'not_found' };
  if (error instanceof WorkspaceAgentObservabilityMutationUnavailableError) {
    return { kind: 'unavailable' };
  }
  reportWorkspaceAgentObservabilityMutationUnexpected(
    input,
    'acquisition',
    error instanceof AgentObservabilityMutationValidationError
      ? 'corrupt_cache'
      : error instanceof WorkspaceAgentObservabilityMutationInvariantError
        ? 'invariant_violation'
        : 'unexpected_database_or_programmer',
  );
  return { kind: 'unavailable' };
}

function successfulWorkspaceResponse(
  input: Pick<
    ExecuteWorkspaceAgentObservabilityPutInput,
    'organizationId' | 'workspaceId' | 'requestId' | 'reporter'
  >,
  response: { status: number; body: AgentObservabilityMutationResponse },
  phase: WorkspaceAgentObservabilityMutationUnexpectedPhase,
): ExecuteWorkspaceAgentObservabilityPutResult {
  if (
    (response.status !== 200 && response.status !== 201) ||
    response.body.scope !== 'workspace' ||
    response.body.organization_id !== input.organizationId ||
    response.body.workspace_id !== input.workspaceId
  ) {
    reportWorkspaceAgentObservabilityMutationUnexpected(
      input,
      phase,
      'invalid_authoritative_response',
    );
    return { kind: 'unavailable' };
  }
  return { kind: 'success', status: response.status, body: response.body };
}

function sameWorkspaceAgentObservabilityMutationKind(
  left: WorkspaceAgentObservabilityMutationKind,
  right: WorkspaceAgentObservabilityMutationKind,
): boolean {
  if (left.type !== right.type) return false;
  if (left.type === 'same_target' && right.type === 'same_target') {
    return left.bindingId === right.bindingId;
  }
  if (left.type === 'replacement' && right.type === 'replacement') {
    return left.previousActiveBindingId === right.previousActiveBindingId;
  }
  if (left.type === 'mode_only' && right.type === 'mode_only') {
    return (
      left.selectionChanged === right.selectionChanged &&
      left.entersDisabled === right.entersDisabled &&
      left.previousActiveBindingId === right.previousActiveBindingId
    );
  }
  return true;
}
