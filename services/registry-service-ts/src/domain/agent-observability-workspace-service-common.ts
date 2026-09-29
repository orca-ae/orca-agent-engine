// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import type { SecretStore } from '../secrets/secret-provider.js';
import type {
  AgentObservabilityReservationReference,
  FinalizeAgentObservabilityMutationResult,
} from './agent-observability-mutations.js';
import {
  organizationAgentObservabilityFinalizationFailureCode,
  runOrganizationAgentObservabilityFinalizationWithRetry,
  settleOrganizationAgentObservabilityFinalizationLoser,
  writeOrganizationAgentObservabilityStagedBundle,
  type OrganizationAgentObservabilityMutationExecutionHooks,
  type OrganizationAgentObservabilityMutationExecutionInput,
  type OrganizationAgentObservabilityMutationFailureCode,
  type OrganizationAgentObservabilityMutationReporter,
  type OrganizationAgentObservabilityMutationUnexpectedPhase,
} from './agent-observability-organization-service-common.js';

const UNEXPECTED_FAILURE_MESSAGE = {
  acquisition: 'registry-service-ts failed workspace agent observability mutation acquisition',
  staging_write: 'registry-service-ts failed workspace agent observability staging write',
  finalization: 'registry-service-ts failed workspace agent observability mutation finalization',
  fence: 'registry-service-ts failed workspace agent observability mutation fence',
} as const;

export type WorkspaceAgentObservabilityMutationUnexpectedPhase =
  OrganizationAgentObservabilityMutationUnexpectedPhase;
export type WorkspaceAgentObservabilityMutationFailureCode =
  OrganizationAgentObservabilityMutationFailureCode;

/** Fixed, non-secret workspace mutation operator signal. */
export interface WorkspaceAgentObservabilityMutationFailureEvent {
  phase: WorkspaceAgentObservabilityMutationUnexpectedPhase;
  code: WorkspaceAgentObservabilityMutationFailureCode;
  requestId: string;
  organizationId: string;
  workspaceId: string;
  message: string;
}

export interface WorkspaceAgentObservabilityMutationReporter {
  report(event: WorkspaceAgentObservabilityMutationFailureEvent): void;
}

const defaultWorkspaceAgentObservabilityMutationReporter: WorkspaceAgentObservabilityMutationReporter =
  {
    report(event) {
      console.error(JSON.stringify(event));
    },
  };

/** Workspace-only acquisition seam; finalization seams retain organization behavior. */
export interface WorkspaceAgentObservabilityMutationExecutionHooks extends OrganizationAgentObservabilityMutationExecutionHooks {
  /** Runs in every fresh acquisition transaction before workspace authority locks. */
  beforeAcquisitionTransaction?: (input: { attempt: number; tx: DbTransaction }) => Promise<void>;
}

export interface WorkspaceAgentObservabilityMutationExecutionInput {
  db: DbClient;
  secretStore?: SecretStore | undefined;
  organizationId: string;
  workspaceId: string;
  principal: string;
  authMethod: string;
  requestId: string;
  idempotencyKey: string;
  ifMatch: string | null;
  reporter?: WorkspaceAgentObservabilityMutationReporter;
  hooks?: WorkspaceAgentObservabilityMutationExecutionHooks;
  stagingWriteTimeoutMs?: number;
}

/** Acquisition retries only PostgreSQL's stable serialization SQLSTATE. */
export const WORKSPACE_AGENT_OBSERVABILITY_ACQUISITION_MAX_ATTEMPTS = 3;

/**
 * Retry a DB-only acquisition in fresh repeatable-read transactions. SecretStore
 * staging deliberately happens after this returns and is never retried here.
 */
export async function runWorkspaceAgentObservabilityAcquisitionWithRetry<T>(
  input: Pick<WorkspaceAgentObservabilityMutationExecutionInput, 'db' | 'hooks'>,
  acquire: (tx: DbTransaction) => Promise<T>,
  onAttemptStart?: (attempt: number) => void,
): Promise<T> {
  let lastSerializationError: unknown;
  for (
    let attempt = 1;
    attempt <= WORKSPACE_AGENT_OBSERVABILITY_ACQUISITION_MAX_ATTEMPTS;
    attempt += 1
  ) {
    try {
      onAttemptStart?.(attempt);
      return await input.db.transaction(
        async (tx) => {
          await input.hooks?.beforeAcquisitionTransaction?.({
            attempt,
            tx: tx as DbTransaction,
          });
          return acquire(tx as DbTransaction);
        },
        { isolationLevel: 'repeatable read' },
      );
    } catch (error) {
      if (!isPostgresSerializationFailure(error)) throw error;
      lastSerializationError = error;
    }
  }
  throw lastSerializationError;
}

/** Fresh repeatable-read retry; staged SecretStore writes remain outside it. */
export async function runWorkspaceAgentObservabilityFinalizationWithRetry<T>(
  input: Pick<WorkspaceAgentObservabilityMutationExecutionInput, 'db' | 'hooks'>,
  finalize: (tx: DbTransaction) => Promise<T>,
  onAttemptStart?: (attempt: number) => void,
): Promise<T> {
  return runOrganizationAgentObservabilityFinalizationWithRetry(
    { db: input.db, ...(input.hooks === undefined ? {} : { hooks: input.hooks }) },
    finalize,
    onAttemptStart,
  );
}

function isPostgresSerializationFailure(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    (error as { code?: unknown }).code === '40001'
  );
}

export async function writeWorkspaceAgentObservabilityStagedBundle(
  input: WorkspaceAgentObservabilityMutationExecutionInput,
  reservation: AgentObservabilityReservationReference,
  bundle: string,
): Promise<'written' | 'preempted' | 'failed'> {
  return writeOrganizationAgentObservabilityStagedBundle(
    asOrganizationInput(input),
    reservation,
    bundle,
  );
}

export async function settleWorkspaceAgentObservabilityFinalizationLoser(
  input: Pick<
    WorkspaceAgentObservabilityMutationExecutionInput,
    'db' | 'organizationId' | 'workspaceId' | 'requestId' | 'reporter' | 'hooks'
  >,
  reservation: AgentObservabilityReservationReference,
): Promise<'conflict' | 'unavailable'> {
  return settleOrganizationAgentObservabilityFinalizationLoser(
    asOrganizationReportingInput(input),
    reservation,
  );
}

export function workspaceAgentObservabilityFinalizationFailureCode(
  finalized: FinalizeAgentObservabilityMutationResult,
): WorkspaceAgentObservabilityMutationFailureCode | undefined {
  return organizationAgentObservabilityFinalizationFailureCode(finalized);
}

/** Reporting never carries caught errors, bodies, credential bundles, or refs. */
export function reportWorkspaceAgentObservabilityMutationUnexpected(
  input: Pick<
    WorkspaceAgentObservabilityMutationExecutionInput,
    'organizationId' | 'workspaceId' | 'requestId' | 'reporter'
  >,
  phase: WorkspaceAgentObservabilityMutationUnexpectedPhase,
  code: WorkspaceAgentObservabilityMutationFailureCode,
): void {
  try {
    (input.reporter ?? defaultWorkspaceAgentObservabilityMutationReporter).report({
      phase,
      code,
      requestId: input.requestId,
      organizationId: input.organizationId,
      workspaceId: input.workspaceId,
      message: UNEXPECTED_FAILURE_MESSAGE[phase],
    });
  } catch {
    // Reporting cannot alter a sanitized API result.
  }
}

/**
 * Reuse the established staging/finalization mechanics while translating every
 * operator event to a workspace-owned fixed shape before it reaches a reporter.
 */
function asOrganizationInput(
  input: WorkspaceAgentObservabilityMutationExecutionInput,
): OrganizationAgentObservabilityMutationExecutionInput {
  const reporter: OrganizationAgentObservabilityMutationReporter = {
    report(event) {
      reportWorkspaceAgentObservabilityMutationUnexpected(input, event.phase, event.code);
    },
  };
  return {
    db: input.db,
    secretStore: input.secretStore,
    organizationId: input.organizationId,
    principal: input.principal,
    authMethod: input.authMethod,
    requestId: input.requestId,
    idempotencyKey: input.idempotencyKey,
    ifMatch: input.ifMatch,
    reporter,
    ...(input.hooks === undefined ? {} : { hooks: input.hooks }),
    ...(input.stagingWriteTimeoutMs === undefined
      ? {}
      : { stagingWriteTimeoutMs: input.stagingWriteTimeoutMs }),
  };
}

function asOrganizationReportingInput(
  input: Pick<
    WorkspaceAgentObservabilityMutationExecutionInput,
    'db' | 'organizationId' | 'workspaceId' | 'requestId' | 'reporter' | 'hooks'
  >,
): Pick<
  OrganizationAgentObservabilityMutationExecutionInput,
  'db' | 'organizationId' | 'requestId' | 'reporter' | 'hooks'
> {
  const reporter: OrganizationAgentObservabilityMutationReporter = {
    report(event) {
      reportWorkspaceAgentObservabilityMutationUnexpected(input, event.phase, event.code);
    },
  };
  return {
    db: input.db,
    organizationId: input.organizationId,
    requestId: input.requestId,
    reporter,
    ...(input.hooks === undefined ? {} : { hooks: input.hooks }),
  };
}
