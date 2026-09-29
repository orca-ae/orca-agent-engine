// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { OrganizationAgentObservabilityState } from '../contracts/agent-observability.contract.js';
import {
  OrganizationAgentObservabilityMutationInvariantError,
  OrganizationAgentObservabilityMutationNotFoundError,
  OrganizationAgentObservabilityMutationRequestError,
  OrganizationAgentObservabilityMutationUnavailableError,
  applyOrganizationAgentObservabilityDisable,
  assertOrganizationAgentObservabilityDisablePrecondition,
  lockOrganizationAgentObservabilityMutationAuthority,
  organizationAgentObservabilityDisableBody,
  organizationAgentObservabilityDisablePreemptionTargets,
  type NormalizedOrganizationAgentObservabilityDisableRequest,
} from './agent-observability-organization-mutation.js';
import {
  acquireAgentObservabilityMutationReservation,
  agentObservabilityMutationBodyHash,
  AgentObservabilityMutationValidationError,
  finalizeAgentObservabilityMutation,
  lockAgentObservabilityAdminIdempotencyPartition,
  lookupAgentObservabilityMutationIdempotencyByIdentity,
  preemptOrganizationAgentObservabilityMutationReservations,
  type AgentObservabilityExpectedVersions,
  type AgentObservabilityMutationResponse,
  type AgentObservabilityMutationTarget,
} from './agent-observability-mutations.js';
import {
  organizationAgentObservabilityFinalizationFailureCode,
  reportOrganizationAgentObservabilityMutationUnexpected,
  runOrganizationAgentObservabilityRepeatableReadTransactionWithRetry,
  type OrganizationAgentObservabilityMutationExecutionInput,
  type OrganizationAgentObservabilityMutationUnexpectedPhase,
} from './agent-observability-organization-service-common.js';

const ORGANIZATION_AGENT_OBSERVABILITY_DISABLE_SCOPE =
  'organization_agent_observability.disable' as const;

export interface ExecuteOrganizationAgentObservabilityDisableInput extends Pick<
  OrganizationAgentObservabilityMutationExecutionInput,
  | 'db'
  | 'organizationId'
  | 'principal'
  | 'authMethod'
  | 'requestId'
  | 'idempotencyKey'
  | 'ifMatch'
  | 'reporter'
  | 'hooks'
> {
  request: NormalizedOrganizationAgentObservabilityDisableRequest;
}

export type ExecuteOrganizationAgentObservabilityDisableResult =
  | { kind: 'success'; status: 200; body: OrganizationAgentObservabilityState }
  | { kind: 'bad_request' }
  | { kind: 'not_found' }
  | { kind: 'conflict' }
  | { kind: 'stale' }
  | { kind: 'unavailable' };

/**
 * Emergency disable retries its DB-only repeatable-read authority transaction
 * from a fresh snapshot on PostgreSQL serialization failure. Completed replays
 * precede current-state checks, a supplied stale ETag cannot preempt, and a
 * successful mutation fences only the current default's two targets.
 */
export async function executeOrganizationAgentObservabilityDisable(
  input: ExecuteOrganizationAgentObservabilityDisableInput,
): Promise<ExecuteOrganizationAgentObservabilityDisableResult> {
  const target = { type: 'organization_setting', organizationId: input.organizationId } as const;
  const idempotency = {
    organizationId: input.organizationId,
    principal: input.principal,
    scope: ORGANIZATION_AGENT_OBSERVABILITY_DISABLE_SCOPE,
    key: input.idempotencyKey,
  };
  let bodyHash: string;
  try {
    bodyHash = agentObservabilityMutationBodyHash(
      organizationAgentObservabilityDisableBody(input.request),
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

  // The durable target is restored only from the route-scoped record. This
  // must happen before current authority, If-Match, or emergency preemption so
  // a historical completed disable remains replayable after a later PUT.
  try {
    const replay = await input.db.transaction(async (tx) => {
      const result = await lookupAgentObservabilityMutationIdempotencyByIdentity(tx, {
        idempotency,
        bodyHash,
      });
      assertOrganizationAgentObservabilityDisableIdempotency(result, input.organizationId);
      return result;
    });
    if (replay.kind === 'replay') {
      return successfulDisableResponse(input, replay.response, 'acquisition');
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

  type TransactionOutcome =
    | { type: 'precondition_stale' }
    | { type: 'live_pending' }
    | {
        type: 'kernel';
        result: Awaited<ReturnType<typeof acquireAgentObservabilityMutationReservation>>;
      }
    | {
        type: 'finalized';
        result: Awaited<ReturnType<typeof finalizeAgentObservabilityMutation>>;
      };

  let outcome: TransactionOutcome;
  let failurePhase: OrganizationAgentObservabilityMutationUnexpectedPhase = 'acquisition';
  try {
    // Disable does no SecretStore work, so its full authority transaction can
    // restart from a fresh snapshot after an ordinary PostgreSQL 40001.
    outcome = await runOrganizationAgentObservabilityRepeatableReadTransactionWithRetry(
      input,
      async (tx, attempt) => {
        failurePhase = 'acquisition';
        await input.hooks?.beforeDisableTransaction?.({ attempt, tx });
        await input.hooks?.beforeAcquisitionAuthorityLock?.();
        const authority = await lockOrganizationAgentObservabilityMutationAuthority(
          tx,
          input.organizationId,
        );
        await input.hooks?.afterAcquisitionAuthorityLocked?.();

        // Preserve kernel acquisition's advisory -> idempotency lock order
        // while this transaction keeps the authority rows locked through
        // preemption, reservation, finalization, audit, and replay caching.
        await lockAgentObservabilityAdminIdempotencyPartition(tx, idempotency);
        const replay = await lookupAgentObservabilityMutationIdempotencyByIdentity(tx, {
          idempotency,
          bodyHash,
        });
        assertOrganizationAgentObservabilityDisableIdempotency(replay, input.organizationId);
        if (replay.kind === 'replay' || replay.kind === 'conflict') {
          return { type: 'kernel', result: replay };
        }
        if (replay.kind === 'pending' && replay.reservationLive) {
          return { type: 'live_pending' };
        }

        if (
          assertOrganizationAgentObservabilityDisablePrecondition({
            currentEtag: authority.stateVersion,
            ifMatch: input.ifMatch,
          }) === 'stale'
        ) {
          return { type: 'precondition_stale' };
        }

        await preemptOrganizationAgentObservabilityMutationReservations(tx, {
          organizationId: input.organizationId,
          targets: organizationAgentObservabilityDisablePreemptionTargets(authority),
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

        failurePhase = 'finalization';
        await input.hooks?.afterFinalizationAuthorityLocked?.();
        return {
          type: 'finalized',
          result: await finalizeAgentObservabilityMutation(tx, {
            reservation: acquired.reservation,
            expectedVersions,
            responseStatus: 200,
            audit: {
              action: 'organization.agent_observability.default_disabled',
              authMethod: input.authMethod,
              requestId: input.requestId,
            },
            apply: async ({ db: mutationDb }) =>
              applyOrganizationAgentObservabilityDisable({
                db: mutationDb,
                authority,
                actor: input.principal,
                now: new Date(),
              }),
          }),
        };
      },
    );
  } catch (error) {
    if (failurePhase === 'acquisition') {
      if (error instanceof OrganizationAgentObservabilityMutationRequestError) {
        return { kind: 'bad_request' };
      }
      if (error instanceof OrganizationAgentObservabilityMutationNotFoundError) {
        return { kind: 'not_found' };
      }
      if (error instanceof OrganizationAgentObservabilityMutationUnavailableError) {
        return { kind: 'unavailable' };
      }
    }
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      failurePhase,
      error instanceof AgentObservabilityMutationValidationError ||
        error instanceof OrganizationAgentObservabilityMutationInvariantError
        ? 'invariant_violation'
        : 'unexpected_database_or_programmer',
    );
    return { kind: 'unavailable' };
  }

  if (outcome.type === 'precondition_stale') return { kind: 'stale' };
  if (outcome.type === 'live_pending') return { kind: 'conflict' };
  if (outcome.type === 'kernel') {
    if (outcome.result.kind === 'replay') {
      return successfulDisableResponse(input, outcome.result.response, 'acquisition');
    }
    return { kind: 'conflict' };
  }
  if (outcome.result.kind === 'committed') {
    return successfulDisableResponse(input, outcome.result.response, 'finalization');
  }

  const finalizationFailureCode = organizationAgentObservabilityFinalizationFailureCode(
    outcome.result,
  );
  if (finalizationFailureCode !== undefined) {
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      'finalization',
      finalizationFailureCode,
    );
    return { kind: 'unavailable' };
  }
  return { kind: 'conflict' };
}

function assertOrganizationAgentObservabilityDisableIdempotency(
  result: Awaited<ReturnType<typeof lookupAgentObservabilityMutationIdempotencyByIdentity>>,
  organizationId: string,
): void {
  if (result.kind === 'absent' || result.kind === 'conflict') return;
  assertOrganizationAgentObservabilityDisableTarget(result.target, organizationId);
  if (result.kind !== 'replay') return;
  const { response } = result;
  if (
    response.status !== 200 ||
    response.body.scope !== 'organization' ||
    response.body.organization_id !== organizationId ||
    response.body.workspace_id !== null ||
    response.body.configured.default_binding !== null ||
    response.body.effective.source !== 'none' ||
    response.body.effective.status !== 'disabled' ||
    response.body.effective.disabled_reason !== 'no_default_binding' ||
    response.body.effective.binding !== null
  ) {
    throw new AgentObservabilityMutationValidationError(
      'agent observability disable replay is corrupt',
    );
  }
}

function assertOrganizationAgentObservabilityDisableTarget(
  target: AgentObservabilityMutationTarget,
  organizationId: string,
): asserts target is Extract<AgentObservabilityMutationTarget, { type: 'organization_setting' }> {
  if (target.type !== 'organization_setting' || target.organizationId !== organizationId) {
    throw new AgentObservabilityMutationValidationError(
      'agent observability disable target is corrupt',
    );
  }
}

function successfulDisableResponse(
  input: Pick<
    ExecuteOrganizationAgentObservabilityDisableInput,
    'organizationId' | 'requestId' | 'reporter'
  >,
  response: { status: number; body: AgentObservabilityMutationResponse },
  phase: OrganizationAgentObservabilityMutationUnexpectedPhase,
): ExecuteOrganizationAgentObservabilityDisableResult {
  if (
    response.status !== 200 ||
    response.body.scope !== 'organization' ||
    response.body.organization_id !== input.organizationId ||
    response.body.workspace_id !== null ||
    response.body.configured.default_binding !== null ||
    response.body.effective.source !== 'none' ||
    response.body.effective.status !== 'disabled' ||
    response.body.effective.disabled_reason !== 'no_default_binding' ||
    response.body.effective.binding !== null
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
