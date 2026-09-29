// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { OrganizationAgentObservabilityState } from '../contracts/agent-observability.contract.js';
import {
  OrganizationAgentObservabilityMutationNotFoundError,
  OrganizationAgentObservabilityMutationInvariantError,
  OrganizationAgentObservabilityMutationRequestError,
  OrganizationAgentObservabilityMutationUnavailableError,
  applyOrganizationAgentObservabilityMutation,
  assertOrganizationAgentObservabilityMutationRequest,
  assertOrganizationAgentObservabilityPrecondition,
  assertOrganizationAgentObservabilityTargetAdmission,
  classifyOrganizationAgentObservabilityMutation,
  encodeOrganizationAgentObservabilityCredentials,
  lockOrganizationAgentObservabilityMutationAuthority,
  organizationAgentObservabilityMutationBody,
  type NormalizedOrganizationAgentObservabilityPutRequest,
  type OrganizationAgentObservabilityMutationKind,
} from './agent-observability-organization-mutation.js';
import {
  acquireAgentObservabilityMutationReservation,
  agentObservabilityMutationBodyHash,
  AgentObservabilityMutationValidationError,
  finalizeAgentObservabilityMutation,
  lookupAgentObservabilityMutationIdempotency,
  type AgentObservabilityExpectedVersions,
  type AgentObservabilityMutationResponse,
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
import { newId } from './versioning.js';

const ORGANIZATION_AGENT_OBSERVABILITY_PUT_SCOPE = 'organization_agent_observability.put' as const;

export interface ExecuteOrganizationAgentObservabilityPutInput extends OrganizationAgentObservabilityMutationExecutionInput {
  request: NormalizedOrganizationAgentObservabilityPutRequest;
}

export type ExecuteOrganizationAgentObservabilityPutResult =
  | { kind: 'success'; status: 200 | 201; body: OrganizationAgentObservabilityState }
  | { kind: 'bad_request' }
  | { kind: 'not_found' }
  | { kind: 'conflict' }
  | { kind: 'stale' }
  | { kind: 'precondition_required' }
  | { kind: 'unavailable' };

/**
 * Application boundary for an organization-default mutation. It owns all DB /
 * SecretStore lifecycle work; HTTP code supplies only trusted metadata and maps
 * the typed result to its wire response.
 */
export async function executeOrganizationAgentObservabilityPut(
  input: ExecuteOrganizationAgentObservabilityPutInput,
): Promise<ExecuteOrganizationAgentObservabilityPutResult> {
  const target = { type: 'organization_setting', organizationId: input.organizationId } as const;
  const idempotency = {
    organizationId: input.organizationId,
    principal: input.principal,
    scope: ORGANIZATION_AGENT_OBSERVABILITY_PUT_SCOPE,
    key: input.idempotencyKey,
  };

  type AcquisitionPlan = {
    kind: OrganizationAgentObservabilityMutationKind;
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
    | { type: 'corrupt_cache' }
    | {
        type: 'kernel';
        result: Awaited<ReturnType<typeof acquireAgentObservabilityMutationReservation>>;
        plan?: AcquisitionPlan;
      };

  let acquired: AcquisitionOutcome;
  try {
    const bodyHash = agentObservabilityMutationBodyHash(
      organizationAgentObservabilityMutationBody(input.request),
    );
    acquired = await input.db.transaction(async (tx) => {
      await input.hooks?.beforeAcquisitionAuthorityLock?.();
      const authority = await lockOrganizationAgentObservabilityMutationAuthority(
        tx,
        input.organizationId,
      );
      await input.hooks?.afterAcquisitionAuthorityLocked?.();
      // Authority locks serialize finalization with retries. Looking up a
      // completed record only after they are held prevents a retry from seeing
      // pending, blocking behind the finalizer, then evaluating a now-stale
      // precondition instead of replaying the committed response.
      let replay: Awaited<ReturnType<typeof lookupAgentObservabilityMutationIdempotency>>;
      try {
        replay = await lookupAgentObservabilityMutationIdempotency(tx, {
          target,
          idempotency,
          bodyHash,
        });
      } catch (error) {
        if (error instanceof AgentObservabilityMutationValidationError) {
          return { type: 'corrupt_cache' } as const;
        }
        throw error;
      }
      if (replay.kind === 'replay' || replay.kind === 'conflict') {
        return { type: 'kernel', result: replay };
      }
      const precondition = assertOrganizationAgentObservabilityPrecondition({
        currentBinding: authority.currentBinding,
        currentEtag: authority.stateVersion,
        ifMatch: input.ifMatch,
      });
      if (precondition === 'missing') return { type: 'precondition_missing' };
      if (precondition === 'stale') return { type: 'precondition_stale' };

      const kind = classifyOrganizationAgentObservabilityMutation(
        authority.currentBinding,
        input.request,
      );
      assertOrganizationAgentObservabilityMutationRequest(input.request, kind);
      assertOrganizationAgentObservabilityTargetAdmission(authority, input.request);
      if (kind.type !== 'same_target' && input.secretStore === undefined) {
        throw new OrganizationAgentObservabilityMutationUnavailableError();
      }

      let candidate: AcquisitionPlan['candidate'];
      if (kind.type !== 'same_target') {
        const bindingId = newId('aob');
        const secretRef = newAgentObservabilitySecretReference();
        candidate = {
          bindingId,
          secretRef,
          bundle: encodeOrganizationAgentObservabilityCredentials({
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
    if (error instanceof OrganizationAgentObservabilityMutationRequestError) {
      return { kind: 'bad_request' };
    }
    if (error instanceof OrganizationAgentObservabilityMutationNotFoundError) {
      return { kind: 'not_found' };
    }
    if (error instanceof OrganizationAgentObservabilityMutationUnavailableError) {
      return { kind: 'unavailable' };
    }
    // Kernel validation at this boundary indicates an invariant/cache problem,
    // not a wire request failure. Do not turn corrupt stored state into a 400.
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
  if (acquired.type === 'corrupt_cache') {
    reportOrganizationAgentObservabilityMutationUnexpected(input, 'acquisition', 'corrupt_cache');
    return { kind: 'unavailable' };
  }
  if (acquired.result.kind === 'replay') {
    return successfulResponse(input, acquired.result.response, 'acquisition');
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

  if (plan.candidate !== undefined) {
    const staged = await writeOrganizationAgentObservabilityStagedBundle(
      input,
      plan.reservation,
      plan.candidate.bundle,
    );
    if (staged === 'preempted') return { kind: 'conflict' };
    if (staged === 'failed') {
      return { kind: 'unavailable' };
    }
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
      const revalidatedKind = classifyOrganizationAgentObservabilityMutation(
        authority.currentBinding,
        input.request,
      );
      if (!sameOrganizationAgentObservabilityMutationKind(plan.kind, revalidatedKind)) {
        return { kind: 'expected_versions_mismatch' } as const;
      }
      assertOrganizationAgentObservabilityMutationRequest(input.request, revalidatedKind);
      assertOrganizationAgentObservabilityTargetAdmission(authority, input.request);
      return finalizeAgentObservabilityMutation(tx, {
        reservation: plan.reservation,
        expectedVersions: plan.expectedVersions,
        responseStatus: plan.kind.type === 'initial' ? 201 : 200,
        audit: {
          action: 'organization.agent_observability.replaced',
          authMethod: input.authMethod,
          requestId: input.requestId,
        },
        apply: async ({ db: mutationDb, stagedActivation }) =>
          applyOrganizationAgentObservabilityMutation({
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
    return successfulResponse(input, finalized.response, 'finalization');
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

function successfulResponse(
  input: Pick<
    ExecuteOrganizationAgentObservabilityPutInput,
    'organizationId' | 'requestId' | 'reporter'
  >,
  response: { status: number; body: AgentObservabilityMutationResponse },
  phase: OrganizationAgentObservabilityMutationUnexpectedPhase,
): ExecuteOrganizationAgentObservabilityPutResult {
  if (
    (response.status !== 200 && response.status !== 201) ||
    response.body.scope !== 'organization' ||
    response.body.organization_id !== input.organizationId ||
    response.body.workspace_id !== null
  ) {
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      phase,
      'invalid_authoritative_response',
    );
    return { kind: 'unavailable' };
  }
  return { kind: 'success', status: response.status, body: response.body };
}

function sameOrganizationAgentObservabilityMutationKind(
  left: OrganizationAgentObservabilityMutationKind,
  right: OrganizationAgentObservabilityMutationKind,
): boolean {
  if (left.type !== right.type) return false;
  if (left.type === 'same_target' && right.type === 'same_target') {
    return left.bindingId === right.bindingId;
  }
  if (left.type === 'replacement' && right.type === 'replacement') {
    return left.previousActiveBindingId === right.previousActiveBindingId;
  }
  return true;
}
