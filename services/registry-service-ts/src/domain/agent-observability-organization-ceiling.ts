// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq } from 'drizzle-orm';
import { OrganizationAgentObservabilityStateSchema } from '../contracts/agent-observability.contract.js';
import type { DbClient } from '../persistence/postgres/client.js';
import { agentObservabilityOrganizationSettings } from '../persistence/postgres/schema.js';
import {
  lockOrganizationAgentObservabilityMutationAuthority,
  OrganizationAgentObservabilityMutationNotFoundError,
} from './agent-observability-organization-mutation.js';
import {
  acquireAgentObservabilityMutationReservation,
  agentObservabilityMutationBodyHash,
  finalizeAgentObservabilityMutation,
  lookupAgentObservabilityMutationIdempotency,
} from './agent-observability-mutations.js';
import {
  captureRestrictionEpochAfterReplacement,
  type AgentObservabilityCaptureMode,
} from './agent-observability-policy.js';
import type { ExecuteOrganizationAgentObservabilityPutResult } from './agent-observability-organization-service.js';
import { runOrganizationAgentObservabilityFinalizationWithRetry } from './agent-observability-organization-service-common.js';

// DB-only mutation: acquire and finalize under the same authority locks and transaction.
// No target selection, credential staging, binding versions, or Session pins are touched.
export async function executeOrganizationAgentObservabilityCaptureCeiling(input: {
  db: DbClient;
  organizationId: string;
  principal: string;
  authMethod: string;
  requestId: string;
  captureCeiling: AgentObservabilityCaptureMode;
  ifMatch: string;
  idempotencyKey: string;
}): Promise<ExecuteOrganizationAgentObservabilityPutResult> {
  try {
    return await runOrganizationAgentObservabilityFinalizationWithRetry(
      input,
      async (tx): Promise<ExecuteOrganizationAgentObservabilityPutResult> => {
        const authority = await lockOrganizationAgentObservabilityMutationAuthority(
          tx,
          input.organizationId,
        );
        const target = {
          type: 'organization_setting',
          organizationId: input.organizationId,
        } as const;
        const idempotency = {
          organizationId: input.organizationId,
          principal: input.principal,
          scope: 'organization_agent_observability.capture_ceiling',
          key: input.idempotencyKey,
        };
        const bodyHash = agentObservabilityMutationBodyHash({
          capture_ceiling: input.captureCeiling,
          if_match: input.ifMatch,
        });
        const replay = await lookupAgentObservabilityMutationIdempotency(tx, {
          target,
          idempotency,
          bodyHash,
        });
        if (replay.kind === 'conflict') return { kind: 'conflict' };
        if (replay.kind === 'replay') {
          const body = OrganizationAgentObservabilityStateSchema.parse(replay.response.body);
          if (
            replay.response.status !== 200 ||
            body.organization_id !== input.organizationId ||
            body.configured.capture_ceiling !== input.captureCeiling
          )
            throw new Error('invalid ceiling replay');
          return { kind: 'success', status: 200, body };
        }
        if (authority.stateVersion !== input.ifMatch) return { kind: 'stale' };
        const expectedVersions = {
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
        if (acquired.kind !== 'acquired') return { kind: 'conflict' };
        const epoch = captureRestrictionEpochAfterReplacement(
          authority.captureCeiling,
          input.captureCeiling,
          authority.captureRestrictionEpoch,
        );
        if (!Number.isSafeInteger(epoch)) throw new Error('invalid capture epoch');
        const finalized = await finalizeAgentObservabilityMutation(tx, {
          reservation: acquired.reservation,
          expectedVersions,
          responseStatus: 200,
          audit: {
            action: 'organization.agent_observability.capture_ceiling.updated',
            authMethod: input.authMethod,
            requestId: input.requestId,
          },
          apply: async ({ db }) => {
            if (authority.captureCeiling === input.captureCeiling) return { applied: true };
            const rows = await db
              .update(agentObservabilityOrganizationSettings)
              .set({
                captureCeiling: input.captureCeiling,
                captureRestrictionEpoch: epoch,
                updatedAt: new Date(),
              })
              .where(
                and(
                  eq(agentObservabilityOrganizationSettings.organizationId, input.organizationId),
                  eq(
                    agentObservabilityOrganizationSettings.captureCeiling,
                    authority.captureCeiling,
                  ),
                  eq(
                    agentObservabilityOrganizationSettings.captureRestrictionEpoch,
                    authority.captureRestrictionEpoch,
                  ),
                ),
              )
              .returning({ id: agentObservabilityOrganizationSettings.organizationId });
            return { applied: rows.length === 1 };
          },
        });
        // Roll back the acquisition too if finalization fails; do not strand a reservation.
        if (finalized.kind !== 'committed') throw new Error('ceiling mutation not committed');
        const body = OrganizationAgentObservabilityStateSchema.parse(finalized.response.body);
        if (
          body.organization_id !== input.organizationId ||
          body.configured.capture_ceiling !== input.captureCeiling
        )
          throw new Error('invalid ceiling result');
        return { kind: 'success', status: 200, body };
      },
    );
  } catch (error) {
    if (error instanceof OrganizationAgentObservabilityMutationNotFoundError)
      return { kind: 'not_found' };
    return { kind: 'unavailable' };
  }
}
