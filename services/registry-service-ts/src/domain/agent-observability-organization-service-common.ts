// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import type { SecretStore } from '../secrets/secret-provider.js';
import {
  AgentObservabilityMutationValidationError,
  fenceAgentObservabilityMutationReservation,
  settleAgentObservabilityMutationReservation,
  writeAgentObservabilityStagingIntent,
  type AgentObservabilityReservationReference,
  type FinalizeAgentObservabilityMutationResult,
} from './agent-observability-mutations.js';

const UNEXPECTED_FAILURE_MESSAGE = {
  acquisition: 'registry-service-ts failed organization agent observability mutation acquisition',
  staging_write: 'registry-service-ts failed organization agent observability staging write',
  finalization: 'registry-service-ts failed organization agent observability mutation finalization',
  fence: 'registry-service-ts failed organization agent observability mutation fence',
} as const;

export type OrganizationAgentObservabilityMutationUnexpectedPhase =
  keyof typeof UNEXPECTED_FAILURE_MESSAGE;

/** Stable, non-secret operator classifications for unavailable mutation outcomes. */
export type OrganizationAgentObservabilityMutationFailureCode =
  | 'corrupt_cache'
  | 'invariant_violation'
  | 'staging_timeout'
  | 'staging_failure'
  | 'unexpected_database_or_programmer'
  | 'fence_failure'
  | 'invalid_authoritative_response';

/** Operator signal only. It never carries an Error, request body, or secret-bearing value. */
export interface OrganizationAgentObservabilityMutationFailureEvent {
  phase: OrganizationAgentObservabilityMutationUnexpectedPhase;
  code: OrganizationAgentObservabilityMutationFailureCode;
  requestId: string;
  organizationId: string;
  message: string;
}

export interface OrganizationAgentObservabilityMutationReporter {
  report(event: OrganizationAgentObservabilityMutationFailureEvent): void;
}

const defaultOrganizationAgentObservabilityMutationReporter: OrganizationAgentObservabilityMutationReporter =
  {
    report(event) {
      console.error(JSON.stringify(event));
    },
  };

/** Test-only timing seams. They are deliberately not part of a listener builder. */
export interface OrganizationAgentObservabilityMutationExecutionHooks {
  beforeAcquisitionAuthorityLock?: () => Promise<void>;
  afterAcquisitionAuthorityLocked?: () => Promise<void>;
  /** Runs inside each fresh emergency-disable transaction, before authority locks. */
  beforeDisableTransaction?: (input: { attempt: number; tx: DbTransaction }) => Promise<void>;
  beforeStagingWrite?: () => Promise<void>;
  beforeFinalizationAuthorityLock?: () => Promise<void>;
  /** Runs inside each fresh finalization transaction, before authority locks. */
  beforeFinalizationTransaction?: (input: { attempt: number; tx: DbTransaction }) => Promise<void>;
  afterFinalizationAuthorityLocked?: () => Promise<void>;
  beforeFencing?: () => Promise<void>;
}

export interface OrganizationAgentObservabilityMutationExecutionInput {
  db: DbClient;
  secretStore?: SecretStore | undefined;
  organizationId: string;
  principal: string;
  authMethod: string;
  requestId: string;
  idempotencyKey: string;
  ifMatch: string | null;
  reporter?: OrganizationAgentObservabilityMutationReporter;
  hooks?: OrganizationAgentObservabilityMutationExecutionHooks;
  /** Test-only override for bounded staging-write coverage. */
  stagingWriteTimeoutMs?: number;
}

/** Bound DB-only retries for PostgreSQL's stable serialization-failure SQLSTATE. */
export const ORGANIZATION_AGENT_OBSERVABILITY_DB_TRANSACTION_MAX_ATTEMPTS = 3;

/**
 * Run a DB-only operation in fresh repeatable-read transactions. Only the
 * stable PostgreSQL serialization SQLSTATE is retried.
 */
export async function runOrganizationAgentObservabilityRepeatableReadTransactionWithRetry<T>(
  input: Pick<OrganizationAgentObservabilityMutationExecutionInput, 'db'>,
  operation: (tx: DbTransaction, attempt: number) => Promise<T>,
): Promise<T> {
  let lastSerializationError: unknown;
  for (
    let attempt = 1;
    attempt <= ORGANIZATION_AGENT_OBSERVABILITY_DB_TRANSACTION_MAX_ATTEMPTS;
    attempt += 1
  ) {
    try {
      return await input.db.transaction((tx) => operation(tx as DbTransaction, attempt), {
        isolationLevel: 'repeatable read',
      });
    } catch (error) {
      if (!isPostgresSerializationFailure(error)) throw error;
      lastSerializationError = error;
    }
  }
  throw lastSerializationError;
}

/**
 * Run DB-only finalization in fresh repeatable-read transactions. SecretStore
 * staging is intentionally outside this helper and can never be repeated.
 */
export async function runOrganizationAgentObservabilityFinalizationWithRetry<T>(
  input: Pick<OrganizationAgentObservabilityMutationExecutionInput, 'db' | 'hooks'>,
  finalize: (tx: DbTransaction) => Promise<T>,
  onAttemptStart?: (attempt: number) => void,
): Promise<T> {
  return runOrganizationAgentObservabilityRepeatableReadTransactionWithRetry(
    input,
    async (tx, attempt) => {
      onAttemptStart?.(attempt);
      await input.hooks?.beforeFinalizationTransaction?.({ attempt, tx });
      return finalize(tx);
    },
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

/** Staging outcome at an organization executor boundary. */
export type OrganizationAgentObservabilityStagedBundleWriteResult =
  | 'written'
  | 'preempted'
  | 'failed';

export async function writeOrganizationAgentObservabilityStagedBundle(
  input: OrganizationAgentObservabilityMutationExecutionInput,
  reservation: AgentObservabilityReservationReference,
  bundle: string,
): Promise<OrganizationAgentObservabilityStagedBundleWriteResult> {
  if (input.secretStore === undefined) {
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      'staging_write',
      'invariant_violation',
    );
    await fenceOrganizationAgentObservabilityMutation(input, reservation);
    return 'failed';
  }
  try {
    await input.hooks?.beforeStagingWrite?.();
    const writeInput: Parameters<typeof writeAgentObservabilityStagingIntent>[1] = {
      reservation,
      put: async ({ secretRef, signal }) => input.secretStore!.put(secretRef, bundle, { signal }),
    };
    if (input.stagingWriteTimeoutMs !== undefined) {
      writeInput.timeoutMs = input.stagingWriteTimeoutMs;
    }
    const write = await writeAgentObservabilityStagingIntent(input.db, writeInput);
    if (write.kind === 'written') return 'written';
    // A fence or elapsed reservation already owns this writer's durable cleanup
    // handoff. Both outcomes are normal API conflicts, not staging failures.
    if (
      (write.kind === 'tombstoned' &&
        (write.reason === 'preempted' || write.reason === 'expired')) ||
      (write.kind === 'not_claimed' && (write.reason === 'preempted' || write.reason === 'expired'))
    ) {
      return 'preempted';
    }
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      'staging_write',
      write.kind === 'tombstoned' ? 'staging_timeout' : 'staging_failure',
    );
  } catch (error) {
    reportOrganizationAgentObservabilityMutationUnexpected(
      input,
      'staging_write',
      error instanceof AgentObservabilityMutationValidationError
        ? 'invariant_violation'
        : 'unexpected_database_or_programmer',
    );
  }
  await fenceOrganizationAgentObservabilityMutation(input, reservation);
  return 'failed';
}

export function organizationAgentObservabilityFinalizationFailureCode(
  finalized: FinalizeAgentObservabilityMutationResult,
): OrganizationAgentObservabilityMutationFailureCode | undefined {
  if (finalized.kind === 'staging_not_written') return 'staging_failure';
  if (
    finalized.kind === 'invalid_superseded_secret' ||
    finalized.kind === 'invalid_credential_generation'
  ) {
    return 'invariant_violation';
  }
  if (
    finalized.kind === 'activation_not_proven' ||
    finalized.kind === 'authoritative_state_mismatch'
  ) {
    return 'invalid_authoritative_response';
  }
  return undefined;
}

export async function fenceOrganizationAgentObservabilityMutation(
  input: Pick<
    OrganizationAgentObservabilityMutationExecutionInput,
    'db' | 'organizationId' | 'requestId' | 'reporter' | 'hooks'
  >,
  reservation: AgentObservabilityReservationReference,
): Promise<boolean> {
  try {
    await input.hooks?.beforeFencing?.();
    const fenced = await input.db.transaction((tx) =>
      fenceAgentObservabilityMutationReservation(tx, reservation),
    );
    if (!fenced) {
      reportOrganizationAgentObservabilityMutationUnexpected(input, 'fence', 'fence_failure');
    }
    return fenced;
  } catch {
    reportOrganizationAgentObservabilityMutationUnexpected(input, 'fence', 'fence_failure');
    return false;
  }
}

/**
 * Settle a non-committed finalizer only after its authority transaction has
 * ended. An emergency fence or maintenance expiry already owns the loser and
 * is a normal conflict; every other durable result fails closed.
 */
export async function settleOrganizationAgentObservabilityFinalizationLoser(
  input: Pick<
    OrganizationAgentObservabilityMutationExecutionInput,
    'db' | 'organizationId' | 'requestId' | 'reporter' | 'hooks'
  >,
  reservation: AgentObservabilityReservationReference,
): Promise<'conflict' | 'unavailable'> {
  try {
    await input.hooks?.beforeFencing?.();
    const settled = await input.db.transaction((tx) =>
      settleAgentObservabilityMutationReservation(tx, reservation),
    );
    if (
      settled.kind === 'fenced' ||
      settled.kind === 'expired' ||
      settled.kind === 'already_terminal'
    ) {
      return 'conflict';
    }
  } catch {
    // Falls through to one sanitized fence failure below.
  }
  reportOrganizationAgentObservabilityMutationUnexpected(input, 'fence', 'fence_failure');
  return 'unavailable';
}

export function reportOrganizationAgentObservabilityMutationUnexpected(
  input: Pick<
    OrganizationAgentObservabilityMutationExecutionInput,
    'organizationId' | 'requestId' | 'reporter'
  >,
  phase: OrganizationAgentObservabilityMutationUnexpectedPhase,
  code: OrganizationAgentObservabilityMutationFailureCode,
): void {
  try {
    (input.reporter ?? defaultOrganizationAgentObservabilityMutationReporter).report({
      phase,
      code,
      requestId: input.requestId,
      organizationId: input.organizationId,
      message: UNEXPECTED_FAILURE_MESSAGE[phase],
    });
  } catch {
    // Reporting cannot alter a sanitized API result.
  }
}
