// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { OrganizationAgentObservabilityState } from '../contracts/agent-observability.contract.js';
import {
  OrganizationAgentObservabilityCredentialRotationNotRotatableError,
  OrganizationAgentObservabilityMutationInvariantError,
  OrganizationAgentObservabilityMutationNotFoundError,
  OrganizationAgentObservabilityMutationRequestError,
  OrganizationAgentObservabilityMutationUnavailableError,
  applyOrganizationAgentObservabilityCredentialRotation,
  assertOrganizationAgentObservabilityCredentialRotationPrecondition,
  encodeOrganizationAgentObservabilityCredentialRotation,
  loadOrganizationAgentObservabilityCredentialRotationHead,
  lockOrganizationAgentObservabilityMutationAuthority,
  organizationAgentObservabilityCredentialRotationBody,
  type NormalizedOrganizationAgentObservabilityCredentialRotationRequest,
} from './agent-observability-organization-mutation.js';
import {
  acquireAgentObservabilityMutationReservation,
  agentObservabilityMutationBodyHash,
  AgentObservabilityMutationValidationError,
  finalizeAgentObservabilityMutation,
  lookupAgentObservabilityMutationIdempotencyByIdentity,
  type AgentObservabilityExpectedVersions,
  type AgentObservabilityMutationResponse,
  type AgentObservabilityMutationTarget,
  type AgentObservabilityReservationReference,
} from './agent-observability-mutations.js';
import { newAgentObservabilitySecretReference } from './agent-observability-secrets.js';
import {
  organizationAgentObservabilityFinalizationFailureCode,
  reportOrganizationAgentObservabilityMutationUnexpected,
  runOrganizationAgentObservabilityFinalizationWithRetry,
  settleOrganizationAgentObservabilityFinalizationLoser,
  writeOrganizationAgentObservabilityStagedBundle,
  type OrganizationAgentObservabilityMutationExecutionInput,
  type OrganizationAgentObservabilityMutationUnexpectedPhase,
} from './agent-observability-organization-service-common.js';

const ORGANIZATION_AGENT_OBSERVABILITY_CREDENTIAL_ROTATION_SCOPE =
  'organization_agent_observability.rotate_credentials' as const;

export interface ExecuteOrganizationAgentObservabilityCredentialRotationInput extends OrganizationAgentObservabilityMutationExecutionInput {
  request: NormalizedOrganizationAgentObservabilityCredentialRotationRequest;
}

export type ExecuteOrganizationAgentObservabilityCredentialRotationResult =
  | { kind: 'success'; status: 200; body: OrganizationAgentObservabilityState }
  | { kind: 'bad_request' }
  | { kind: 'not_found' }
  | { kind: 'conflict' }
  | { kind: 'stale' }
  | { kind: 'precondition_required' }
  | { kind: 'unavailable' };

/**
 * Rotates the current organization-default OTLP credential head without
 * changing its target, config generation, selection, revocation, or capture
 * epochs. Credential references stay inside this executor until finalization.
 */
export async function executeOrganizationAgentObservabilityCredentialRotation(
  input: ExecuteOrganizationAgentObservabilityCredentialRotationInput,
): Promise<ExecuteOrganizationAgentObservabilityCredentialRotationResult> {
  const idempotency = {
    organizationId: input.organizationId,
    principal: input.principal,
    scope: ORGANIZATION_AGENT_OBSERVABILITY_CREDENTIAL_ROTATION_SCOPE,
    key: input.idempotencyKey,
  };
  let bodyHash: string;
  try {
    bodyHash = agentObservabilityMutationBodyHash(
      organizationAgentObservabilityCredentialRotationBody(input.request),
    );
  } catch (error) {
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      'acquisition',
      error instanceof AgentObservabilityMutationValidationError
        ? 'invariant_violation'
        : 'unexpected_database_or_programmer',
    );
    return { kind: 'unavailable' };
  }

  // A successful rotation can outlive a later PUT that replaces the selected
  // binding. This lookup reconstructs the historic binding only from durable
  // idempotency/reservation state; it never accepts a target from the caller.
  try {
    const replay = await input.db.transaction(async (tx) => {
      const result = await lookupAgentObservabilityMutationIdempotencyByIdentity(tx, {
        idempotency,
        bodyHash,
      });
      assertOrganizationAgentObservabilityCredentialRotationIdempotency(
        result,
        input.organizationId,
      );
      return result;
    });
    if (replay.kind === 'replay') {
      return successfulCredentialRotationResponse(input, replay.response, 'acquisition');
    }
    if (replay.kind === 'conflict' || (replay.kind === 'pending' && replay.reservationLive)) {
      return { kind: 'conflict' };
    }
  } catch (error) {
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      'acquisition',
      error instanceof AgentObservabilityMutationValidationError
        ? 'corrupt_cache'
        : 'unexpected_database_or_programmer',
    );
    return { kind: 'unavailable' };
  }

  type AcquisitionPlan = {
    expectedVersions: AgentObservabilityExpectedVersions;
    reservation: AgentObservabilityReservationReference;
    bindingId: string;
    candidate: {
      bundle: string;
      supersededSecretRef: ReturnType<typeof newAgentObservabilitySecretReference>;
    };
  };
  type AcquisitionOutcome =
    | { type: 'precondition_missing' }
    | { type: 'precondition_stale' }
    | { type: 'not_rotatable' }
    | { type: 'corrupt_cache' }
    | { type: 'live_pending' }
    | {
        type: 'kernel';
        result: Awaited<ReturnType<typeof acquireAgentObservabilityMutationReservation>>;
        plan?: AcquisitionPlan;
      };

  let acquired: AcquisitionOutcome;
  try {
    acquired = await input.db.transaction(async (tx) => {
      await input.hooks?.beforeAcquisitionAuthorityLock?.();
      const authority = await lockOrganizationAgentObservabilityMutationAuthority(
        tx,
        input.organizationId,
      );
      await input.hooks?.afterAcquisitionAuthorityLocked?.();

      // Repeat route-scoped lookup after the authority lock. A finalizer can
      // commit between the optimistic replay probe above and this transaction.
      let replay: Awaited<ReturnType<typeof lookupAgentObservabilityMutationIdempotencyByIdentity>>;
      try {
        replay = await lookupAgentObservabilityMutationIdempotencyByIdentity(tx, {
          idempotency,
          bodyHash,
        });
        assertOrganizationAgentObservabilityCredentialRotationIdempotency(
          replay,
          input.organizationId,
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
      if (replay.kind === 'pending' && replay.reservationLive) return { type: 'live_pending' };

      const precondition = assertOrganizationAgentObservabilityCredentialRotationPrecondition({
        currentEtag: authority.stateVersion,
        ifMatch: input.ifMatch,
      });
      if (precondition === 'missing') return { type: 'precondition_missing' };
      if (precondition === 'stale') return { type: 'precondition_stale' };

      const head = await loadOrganizationAgentObservabilityCredentialRotationHead(tx, authority);
      if (head === null) return { type: 'not_rotatable' };
      if (input.secretStore === undefined) {
        throw new OrganizationAgentObservabilityMutationUnavailableError();
      }
      const nextCredentialVersion = head.credentialVersion + 1;
      const target = {
        type: 'binding',
        organizationId: input.organizationId,
        bindingId: head.bindingId,
        bindingScope: 'organization',
      } as const;
      const candidateSecretRef = newAgentObservabilitySecretReference();
      const candidate = {
        bundle: encodeOrganizationAgentObservabilityCredentialRotation({
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
      if (result.kind !== 'acquired') return { type: 'kernel', result };
      return {
        type: 'kernel',
        result,
        plan: {
          expectedVersions,
          reservation: result.reservation,
          bindingId: head.bindingId,
          candidate,
        },
      };
    });
  } catch (error) {
    if (error instanceof OrganizationAgentObservabilityMutationRequestError) {
      return { kind: 'bad_request' };
    }
    if (error instanceof OrganizationAgentObservabilityMutationNotFoundError) {
      return { kind: 'not_found' };
    }
    if (error instanceof OrganizationAgentObservabilityCredentialRotationNotRotatableError) {
      return { kind: 'conflict' };
    }
    if (error instanceof OrganizationAgentObservabilityMutationUnavailableError) {
      return { kind: 'unavailable' };
    }
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      'acquisition',
      error instanceof AgentObservabilityMutationValidationError ||
        error instanceof OrganizationAgentObservabilityMutationInvariantError
        ? 'invariant_violation'
        : 'unexpected_database_or_programmer',
    );
    return { kind: 'unavailable' };
  }

  if (acquired.type === 'precondition_missing') return { kind: 'precondition_required' };
  if (acquired.type === 'precondition_stale') return { kind: 'stale' };
  if (acquired.type === 'not_rotatable') return { kind: 'conflict' };
  if (acquired.type === 'live_pending') return { kind: 'conflict' };
  if (acquired.type === 'corrupt_cache') {
    reportOrganizationAgentObservabilityMutationUnexpected(input, 'acquisition', 'corrupt_cache');
    return { kind: 'unavailable' };
  }
  if (acquired.result.kind === 'replay') {
    return successfulCredentialRotationResponse(input, acquired.result.response, 'acquisition');
  }
  if (acquired.result.kind === 'conflict' || acquired.result.kind === 'in_progress') {
    return { kind: 'conflict' };
  }
  const plan = acquired.plan;
  if (plan === undefined) {
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      'acquisition',
      'invariant_violation',
    );
    return { kind: 'unavailable' };
  }

  const staged = await writeOrganizationAgentObservabilityStagedBundle(
    input,
    plan.reservation,
    plan.candidate.bundle,
  );
  if (staged === 'preempted') return { kind: 'conflict' };
  if (staged === 'failed') {
    return { kind: 'unavailable' };
  }

  let finalized: Awaited<ReturnType<typeof finalizeAgentObservabilityMutation>>;
  try {
    await input.hooks?.beforeFinalizationAuthorityLock?.();
    finalized = await runOrganizationAgentObservabilityFinalizationWithRetry(input, async (tx) => {
      const authority = await lockOrganizationAgentObservabilityMutationAuthority(
        tx,
        input.organizationId,
      );
      await input.hooks?.afterFinalizationAuthorityLocked?.();
      if (authority.stateVersion !== plan.expectedVersions.stateVersion) {
        return { kind: 'expected_versions_mismatch' } as const;
      }
      const head = await loadOrganizationAgentObservabilityCredentialRotationHead(tx, authority);
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
          action: 'organization.agent_observability.credentials_rotated',
          authMethod: input.authMethod,
          requestId: input.requestId,
        },
        apply: async ({ db: mutationDb, stagedActivation }) =>
          applyOrganizationAgentObservabilityCredentialRotation({
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
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      'finalization',
      error instanceof AgentObservabilityMutationValidationError ||
        error instanceof OrganizationAgentObservabilityMutationInvariantError
        ? 'invariant_violation'
        : 'unexpected_database_or_programmer',
    );
    await settleOrganizationAgentObservabilityFinalizationLoser(input, plan.reservation);
    return { kind: 'unavailable' };
  }

  if (finalized.kind === 'committed') {
    return successfulCredentialRotationResponse(input, finalized.response, 'finalization');
  }

  const finalizationFailureCode = organizationAgentObservabilityFinalizationFailureCode(finalized);
  if (finalizationFailureCode !== undefined) {
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      'finalization',
      finalizationFailureCode,
    );
  }
  const settlement = await settleOrganizationAgentObservabilityFinalizationLoser(
    input,
    plan.reservation,
  );
  if (finalizationFailureCode !== undefined) return { kind: 'unavailable' };
  return settlement === 'conflict' ? { kind: 'conflict' } : { kind: 'unavailable' };
}

function assertOrganizationAgentObservabilityCredentialRotationIdempotency(
  result: Awaited<ReturnType<typeof lookupAgentObservabilityMutationIdempotencyByIdentity>>,
  organizationId: string,
): void {
  if (result.kind === 'absent' || result.kind === 'conflict') return;
  assertOrganizationAgentObservabilityCredentialRotationTarget(result.target, organizationId);
  if (result.kind !== 'replay') return;
  const { response } = result;
  if (
    response.status !== 200 ||
    response.body.scope !== 'organization' ||
    response.body.organization_id !== organizationId ||
    response.body.workspace_id !== null ||
    response.body.configured.default_binding?.id !== result.target.bindingId
  ) {
    throw new AgentObservabilityMutationValidationError(
      'agent observability credential rotation replay is corrupt',
    );
  }
}

function assertOrganizationAgentObservabilityCredentialRotationTarget(
  target: AgentObservabilityMutationTarget,
  organizationId: string,
): asserts target is Extract<AgentObservabilityMutationTarget, { type: 'binding' }> {
  if (
    target.type !== 'binding' ||
    target.bindingScope !== 'organization' ||
    target.organizationId !== organizationId
  ) {
    throw new AgentObservabilityMutationValidationError(
      'agent observability credential rotation target is corrupt',
    );
  }
}

function successfulCredentialRotationResponse(
  input: Pick<
    ExecuteOrganizationAgentObservabilityCredentialRotationInput,
    'organizationId' | 'requestId' | 'reporter'
  >,
  response: { status: number; body: AgentObservabilityMutationResponse },
  phase: OrganizationAgentObservabilityMutationUnexpectedPhase,
): ExecuteOrganizationAgentObservabilityCredentialRotationResult {
  if (
    response.status !== 200 ||
    response.body.scope !== 'organization' ||
    response.body.organization_id !== input.organizationId ||
    response.body.workspace_id !== null ||
    response.body.configured.default_binding === null
  ) {
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      phase,
      'invalid_authoritative_response',
    );
    return { kind: 'unavailable' };
  }
  return { kind: 'success', status: 200, body: response.body };
}
