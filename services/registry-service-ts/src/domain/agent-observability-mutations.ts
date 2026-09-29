// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { and, asc, desc, eq, gt, isNull, lte, or, sql } from 'drizzle-orm';
import {
  OrganizationAgentObservabilityStateSchema,
  WorkspaceAgentObservabilityStateSchema,
  type OrganizationAgentObservabilityState,
  type WorkspaceAgentObservabilityState,
} from '../contracts/agent-observability.contract.js';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import {
  adminAuditEvents,
  agentObservabilityBindingCredentials,
  agentObservabilityBindings,
  agentObservabilityCredentialStagingIntents,
  agentObservabilityIdempotencyKeys,
  agentObservabilityMutationReservations,
  agentObservabilitySecretCleanupOutbox,
} from '../persistence/postgres/schema.js';
import type { SecretStore } from '../secrets/secret-provider.js';
import {
  canonicalAgentObservabilityJson,
  isAgentObservabilitySecretReference,
  type AgentObservabilitySecretReference,
} from './agent-observability-secrets.js';
import {
  AgentObservabilityStateAvailabilityError,
  AgentObservabilityStateNotFoundError,
  loadOrganizationAgentObservabilityStateInTransaction,
  loadWorkspaceAgentObservabilityStateInTransaction,
} from './agent-observability-state.js';
import { hasAgentObservabilityControlCharacter } from './agent-observability-validation.js';
import { newId } from './versioning.js';

export const AGENT_OBSERVABILITY_MUTATION_RESERVATION_TTL_MS = 15 * 60 * 1_000;
export const AGENT_OBSERVABILITY_IDEMPOTENCY_TTL_MS = 24 * 60 * 60 * 1_000;
/** PostgreSQL `integer` ceiling for immutable binding configuration versions. */
export const AGENT_OBSERVABILITY_CONFIG_VERSION_MAX = 2_147_483_647;
/** PostgreSQL `integer` ceiling; one more credential generation must remain storable. */
export const AGENT_OBSERVABILITY_CREDENTIAL_VERSION_MAX = 2_147_483_647;
export const AGENT_OBSERVABILITY_STAGING_CLEANUP_GRACE_MS = 5 * 60 * 1_000;
export const AGENT_OBSERVABILITY_STAGING_WRITER_LEASE_MS = 60 * 1_000;
export const AGENT_OBSERVABILITY_STAGING_WRITER_HEARTBEAT_MS = 20 * 1_000;
/** Bound a request-facing SecretStore wait far below the 15-minute reservation TTL. */
export const AGENT_OBSERVABILITY_STAGING_WRITER_TIMEOUT_MS = 45 * 1_000;
export const AGENT_OBSERVABILITY_STAGING_CLEANUP_LEASE_MS = 60 * 1_000;
export const AGENT_OBSERVABILITY_STAGING_CLEANUP_INTERVAL_MS = 30 * 1_000;
/** Keep every delete wait below the default 60-second cleanup lease. */
export const AGENT_OBSERVABILITY_STAGING_CLEANUP_DELETE_TIMEOUT_MS = 45 * 1_000;
export const AGENT_OBSERVABILITY_SECRET_CLEANUP_LEASE_MS = 60 * 1_000;
/** Keep every delete wait below the default 60-second cleanup lease. */
export const AGENT_OBSERVABILITY_SECRET_CLEANUP_DELETE_TIMEOUT_MS = 45 * 1_000;
export const AGENT_OBSERVABILITY_SECRET_CLEANUP_BASE_BACKOFF_MS = 30 * 1_000;
export const AGENT_OBSERVABILITY_SECRET_CLEANUP_MAX_BACKOFF_MS = 60 * 60 * 1_000;

const IDENTIFIER_MAX_LENGTH = 512;
const IDEMPOTENCY_SCOPE_MAX_LENGTH = 512;
const IDEMPOTENCY_KEY_MAX_LENGTH = 255;
const STATE_VERSION_MAX_LENGTH = 512;
const BODY_HASH_PATTERN = /^[0-9a-f]{64}$/u;
const SAFE_AUDIT_FIELD_PATTERN = /^[A-Za-z0-9._:-]{1,200}$/u;
const STAGING_HEARTBEAT_DRAIN_MAX_MS = 100;
const STAGING_COMPLETION_SERIALIZATION_RETRIES = 2;

export type AgentObservabilityMutationTarget =
  | {
      type: 'organization_setting';
      organizationId: string;
    }
  | {
      type: 'workspace_setting';
      organizationId: string;
      workspaceId: string;
    }
  | {
      type: 'binding';
      organizationId: string;
      bindingId: string;
      bindingScope: 'organization';
      workspaceId?: never;
    }
  | {
      type: 'binding';
      organizationId: string;
      bindingId: string;
      bindingScope: 'workspace';
      workspaceId: string;
    };

export interface AgentObservabilityExpectedVersions {
  /** Opaque current-state/ETag value; the caller owns its actual-state CAS. */
  stateVersion: string;
  configVersion: number | null;
  credentialVersion: number | null;
}

export interface AgentObservabilityAdminIdempotencyIdentity {
  organizationId: string;
  principal: string;
  scope: string;
  key: string;
}

/** Targets an emergency authority operation may preempt under its held locks. */
export type AgentObservabilityAuthorityPreemptionTarget = Extract<
  AgentObservabilityMutationTarget,
  | { type: 'organization_setting' }
  | { type: 'workspace_setting' }
  | { type: 'binding'; bindingScope: 'organization' }
  | { type: 'binding'; bindingScope: 'workspace' }
>;

/** Targets an organization-default emergency operation may preempt. */
export type OrganizationAgentObservabilityPreemptionTarget = Extract<
  AgentObservabilityAuthorityPreemptionTarget,
  { type: 'organization_setting' } | { type: 'binding'; bindingScope: 'organization' }
>;

/** Targets a workspace explicit-disable operation may preempt. */
export type WorkspaceAgentObservabilityPreemptionTarget = Extract<
  AgentObservabilityAuthorityPreemptionTarget,
  { type: 'workspace_setting' } | { type: 'binding'; bindingScope: 'workspace' }
>;

/** Metadata-only identity required to bind a staged bundle to one credential head. */
export interface AgentObservabilityCredentialStagingInput {
  secretRef: AgentObservabilitySecretReference;
  /** No FK: replacement may create this binding only in finalization. */
  candidateBindingId: string;
  proposedCredentialVersion: number;
}

export interface AcquireAgentObservabilityMutationReservationInput {
  target: AgentObservabilityMutationTarget;
  idempotency: AgentObservabilityAdminIdempotencyIdentity;
  /** Hash raw body before this DB-only API. */
  bodyHash: string;
  expectedVersions: AgentObservabilityExpectedVersions;
  staging?: AgentObservabilityCredentialStagingInput;
  now?: Date;
  ttlMs?: number;
  idempotencyTtlMs?: number;
  stagingCleanupGraceMs?: number;
}

export interface AgentObservabilityReservationReference {
  organizationId: string;
  reservationId: string;
  ownerPrincipal: string;
  generation: number;
}

export interface AgentObservabilityReservedMutation extends AgentObservabilityReservationReference {
  target: AgentObservabilityMutationTarget;
  expectedVersions: AgentObservabilityExpectedVersions;
  expiresAt: Date;
}

export type AgentObservabilityMutationResponse =
  | OrganizationAgentObservabilityState
  | WorkspaceAgentObservabilityState;

export type AcquireAgentObservabilityMutationReservationResult =
  | { kind: 'acquired'; reservation: AgentObservabilityReservedMutation }
  | { kind: 'replay'; response: { status: number; body: AgentObservabilityMutationResponse } }
  | {
      kind: 'in_progress';
      reservation: AgentObservabilityReservationReference & { status: 'pending'; expiresAt: Date };
    }
  | {
      kind: 'conflict';
      reason:
        | 'idempotency_key_reused_with_different_body'
        | 'idempotency_key_reused_with_different_target'
        | 'target_pending';
    };

/**
 * Read-only idempotency lookup under caller-held authority locks, before an
 * `If-Match` check. A successful first create changes the current ETag, but an
 * identical retry must still replay its authoritative cached response instead
 * of becoming a false 428 after waiting behind finalization. Reservation
 * acquisition remains the only mutating idempotency operation.
 */
export type LookupAgentObservabilityMutationIdempotencyResult =
  | { kind: 'absent' }
  | { kind: 'replay'; response: { status: number; body: AgentObservabilityMutationResponse } }
  | {
      kind: 'conflict';
      reason:
        | 'idempotency_key_reused_with_different_body'
        | 'idempotency_key_reused_with_different_target';
    };

export async function lookupAgentObservabilityMutationIdempotency(
  db: DbTransaction,
  input: {
    target: AgentObservabilityMutationTarget;
    idempotency: AgentObservabilityAdminIdempotencyIdentity;
    bodyHash: string;
    now?: Date;
  },
): Promise<LookupAgentObservabilityMutationIdempotencyResult> {
  const target = parseAgentObservabilityMutationTarget(input.target);
  const identity = normalizeIdempotencyIdentity(input.idempotency);
  if (target.organizationId !== identity.organizationId) {
    throw new AgentObservabilityMutationValidationError();
  }
  const bodyHash = requiredBodyHash(input.bodyHash);
  const now = validDate(input.now ?? new Date());
  const existing = (
    await db
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(idempotencyWhere(identity))
      .limit(1)
  )[0];
  if (!existing) return { kind: 'absent' };
  // Expired records are rebindable, so neither their prior body nor target can
  // conflict with a new attempt. This exactly matches reservation acquisition.
  if (existing.expiresAt.getTime() <= now.getTime()) return { kind: 'absent' };
  const targetKey = agentObservabilityMutationTargetKey(target);
  if (existing.targetKey !== targetKey) {
    return { kind: 'conflict', reason: 'idempotency_key_reused_with_different_target' };
  }
  if (existing.bodyHash !== bodyHash) {
    return { kind: 'conflict', reason: 'idempotency_key_reused_with_different_body' };
  }
  if (existing.status !== 'completed') return { kind: 'absent' };
  if (existing.responseStatus === null || existing.responseBody === null) {
    throw new AgentObservabilityMutationValidationError();
  }
  return {
    kind: 'replay',
    response: {
      status: existing.responseStatus,
      body: parseAgentObservabilityMutationResponse(target, existing.responseBody),
    },
  };
}

/**
 * Route-scoped lookup for mutations whose durable target is derived from
 * current authority rather than supplied by a caller. It restores that target
 * from the linked reservation and validates the cached response against it,
 * so a completed retry can replay after a later selection change.
 */
export type LookupAgentObservabilityMutationIdempotencyByIdentityResult =
  | { kind: 'absent' }
  | {
      kind: 'conflict';
      reason: 'idempotency_key_reused_with_different_body';
    }
  | {
      kind: 'pending';
      target: AgentObservabilityMutationTarget;
      expectedVersions: AgentObservabilityExpectedVersions;
      reservationLive: boolean;
    }
  | {
      kind: 'replay';
      target: AgentObservabilityMutationTarget;
      expectedVersions: AgentObservabilityExpectedVersions;
      response: { status: number; body: AgentObservabilityMutationResponse };
    };

export async function lookupAgentObservabilityMutationIdempotencyByIdentity(
  tx: DbTransaction,
  input: {
    idempotency: AgentObservabilityAdminIdempotencyIdentity;
    bodyHash: string;
    now?: Date;
  },
): Promise<LookupAgentObservabilityMutationIdempotencyByIdentityResult> {
  const identity = normalizeIdempotencyIdentity(input.idempotency);
  const bodyHash = requiredBodyHash(input.bodyHash);
  const now = validDate(input.now ?? new Date());
  const existing = (
    await tx
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(idempotencyWhere(identity))
      .for('update')
      .limit(1)
  )[0];
  if (!existing || existing.expiresAt.getTime() <= now.getTime()) return { kind: 'absent' };
  if (existing.bodyHash !== bodyHash) {
    return { kind: 'conflict', reason: 'idempotency_key_reused_with_different_body' };
  }

  const reservation = await lockedReservationForIdempotency(
    tx,
    identity.organizationId,
    existing.reservationId,
  );
  const target = targetFromReservation(reservation);
  const targetKey = agentObservabilityMutationTargetKey(target);
  if (
    reservation.id !== existing.reservationId ||
    reservation.organizationId !== identity.organizationId ||
    target.organizationId !== identity.organizationId ||
    reservation.ownerPrincipal !== identity.principal ||
    reservation.bodyHash !== existing.bodyHash ||
    reservation.targetKey !== targetKey ||
    existing.targetKey !== targetKey
  ) {
    throw new AgentObservabilityMutationValidationError(
      'agent observability route idempotency record is corrupt',
    );
  }
  const expectedVersions = expectedVersionsFromReservation(reservation);

  if (existing.status === 'pending') {
    if (
      reservation.status !== 'pending' &&
      reservation.status !== 'fenced' &&
      reservation.status !== 'expired'
    ) {
      throw new AgentObservabilityMutationValidationError(
        'agent observability route idempotency record is corrupt',
      );
    }
    return {
      kind: 'pending',
      target,
      expectedVersions,
      reservationLive: reservationIsLive(reservation, now),
    };
  }
  if (
    existing.status !== 'completed' ||
    reservation.status !== 'committed' ||
    existing.responseStatus === null ||
    existing.responseBody === null
  ) {
    throw new AgentObservabilityMutationValidationError(
      'agent observability route idempotency record is corrupt',
    );
  }
  return {
    kind: 'replay',
    target,
    expectedVersions,
    response: {
      status: existing.responseStatus,
      body: parseAgentObservabilityMutationResponse(target, existing.responseBody),
    },
  };
}

/** Inputs are malformed, not authorization failures; routes map them to wire errors. */
export class AgentObservabilityMutationValidationError extends Error {
  override readonly name = 'AgentObservabilityMutationValidationError';

  constructor(message = 'invalid agent observability mutation input') {
    super(message);
  }
}

/** Strict target shape for the caller-owned stable authority lock. */
export function parseAgentObservabilityMutationTarget(
  value: unknown,
): AgentObservabilityMutationTarget {
  if (!isPlainObject(value) || typeof value.type !== 'string') {
    throw new AgentObservabilityMutationValidationError();
  }
  const organizationId = requiredIdentifier(value.organizationId);
  if (value.type === 'organization_setting') {
    requireExactKeys(value, ['organizationId', 'type']);
    return { type: 'organization_setting', organizationId };
  }
  if (value.type === 'workspace_setting') {
    requireExactKeys(value, ['organizationId', 'type', 'workspaceId']);
    return {
      type: 'workspace_setting',
      organizationId,
      workspaceId: requiredIdentifier(value.workspaceId),
    };
  }
  if (value.type === 'binding') {
    const bindingId = requiredIdentifier(value.bindingId);
    if (value.bindingScope === 'organization') {
      requireExactKeys(value, ['bindingId', 'bindingScope', 'organizationId', 'type']);
      return { type: 'binding', organizationId, bindingId, bindingScope: 'organization' };
    }
    if (value.bindingScope === 'workspace') {
      requireExactKeys(value, [
        'bindingId',
        'bindingScope',
        'organizationId',
        'type',
        'workspaceId',
      ]);
      return {
        type: 'binding',
        organizationId,
        bindingId,
        bindingScope: 'workspace',
        workspaceId: requiredIdentifier(value.workspaceId),
      };
    }
  }
  throw new AgentObservabilityMutationValidationError();
}

/** Stable non-secret target identity used by the partial unique index. */
export function agentObservabilityMutationTargetKey(
  target: AgentObservabilityMutationTarget,
): string {
  const normalized = parseAgentObservabilityMutationTarget(target);
  return canonicalAgentObservabilityJson({
    binding_id: normalized.type === 'binding' ? normalized.bindingId : null,
    binding_scope: normalized.type === 'binding' ? normalized.bindingScope : null,
    organization_id: normalized.organizationId,
    target_type: normalized.type,
    workspace_id: 'workspaceId' in normalized ? (normalized.workspaceId ?? null) : null,
  });
}

/** Hash caller-owned JSON before persistence; semantic normalization belongs to the caller. */
export function agentObservabilityMutationBodyHash(value: unknown): string {
  let canonical: string;
  try {
    canonical = canonicalAgentObservabilityJson(value);
  } catch {
    throw new AgentObservabilityMutationValidationError('invalid canonical mutation body');
  }
  return createHash('sha256').update(canonical).digest('hex');
}

/** Advisory-lock identity for one organization-admin idempotency partition. */
export function agentObservabilityAdminIdempotencyPartition(
  identity: AgentObservabilityAdminIdempotencyIdentity,
): string {
  const normalized = normalizeIdempotencyIdentity(identity);
  return createHash('sha256')
    .update(
      canonicalAgentObservabilityJson({
        key: normalized.key,
        organization_id: normalized.organizationId,
        principal: normalized.principal,
        scope: normalized.scope,
      }),
    )
    .digest('hex');
}

/**
 * Serialize a route's idempotency identity before taking its durable record.
 * Callers that already own authority locks use this to preserve acquire's
 * advisory-lock -> idempotency-row order on a replay/rebind path.
 */
export async function lockAgentObservabilityAdminIdempotencyPartition(
  tx: DbTransaction,
  identity: AgentObservabilityAdminIdempotencyIdentity,
): Promise<void> {
  const normalized = normalizeIdempotencyIdentity(identity);
  await tx.execute(
    sql`select pg_advisory_xact_lock(hashtextextended(${agentObservabilityAdminIdempotencyPartition(normalized)}, 0))`,
  );
}

/**
 * Acquire or safely recover one mutation attempt inside a caller-owned
 * authority transaction. No SecretStore call is reachable from this path.
 */
export async function acquireAgentObservabilityMutationReservation(
  tx: DbTransaction,
  input: AcquireAgentObservabilityMutationReservationInput,
): Promise<AcquireAgentObservabilityMutationReservationResult> {
  const target = parseAgentObservabilityMutationTarget(input.target);
  const identity = normalizeIdempotencyIdentity(input.idempotency);
  if (target.organizationId !== identity.organizationId) {
    throw new AgentObservabilityMutationValidationError();
  }
  const expectedVersions = normalizeExpectedVersions(input.expectedVersions);
  const bodyHash = requiredBodyHash(input.bodyHash);
  const staging = normalizeStaging(input.staging);
  const now = validDate(input.now ?? new Date());
  const reservationTtlMs = validPositiveDuration(
    input.ttlMs ?? AGENT_OBSERVABILITY_MUTATION_RESERVATION_TTL_MS,
  );
  const idempotencyTtlMs = validPositiveDuration(
    input.idempotencyTtlMs ?? AGENT_OBSERVABILITY_IDEMPOTENCY_TTL_MS,
  );
  const stagingCleanupGraceMs = validPositiveDuration(
    input.stagingCleanupGraceMs ?? AGENT_OBSERVABILITY_STAGING_CLEANUP_GRACE_MS,
  );
  const targetKey = agentObservabilityMutationTargetKey(target);

  await lockAgentObservabilityAdminIdempotencyPartition(tx, identity);

  const existing = (
    await tx
      .select()
      .from(agentObservabilityIdempotencyKeys)
      .where(idempotencyWhere(identity))
      .for('update')
      .limit(1)
  )[0];
  if (!existing) {
    return createReservationAttempt(tx, {
      identity,
      target,
      targetKey,
      bodyHash,
      expectedVersions,
      staging,
      now,
      reservationTtlMs,
      idempotencyTtlMs,
      stagingCleanupGraceMs,
      rebind: null,
    });
  }

  const linkedReservation = await lockedReservationForIdempotency(
    tx,
    identity.organizationId,
    existing.reservationId,
  );
  const idempotencyLive = existing.expiresAt.getTime() > now.getTime();
  const reservationLive =
    linkedReservation.status === 'pending' && linkedReservation.expiresAt.getTime() > now.getTime();

  // TTL expiry discards target/body equivalence, except a still-live linked
  // attempt. That attempt owns the key until it becomes terminal even if its
  // idempotency cache lifetime has elapsed.
  if (!idempotencyLive) {
    if (reservationLive) {
      if (existing.status !== 'pending') {
        throw new Error(
          'completed agent observability idempotency row points at a live reservation',
        );
      }
      return { kind: 'in_progress', reservation: reservationInProgress(linkedReservation) };
    }
    if (linkedReservation.status === 'pending') {
      const expired = await expireLockedReservation(tx, linkedReservation, now);
      if (!expired)
        return { kind: 'in_progress', reservation: reservationInProgress(linkedReservation) };
      await markStagingCleanupPendingForTerminalReservation(tx, {
        reservationId: linkedReservation.id,
        generation: linkedReservation.generation,
        now,
      });
    }
    return createReservationAttempt(tx, {
      identity,
      target,
      targetKey,
      bodyHash,
      expectedVersions,
      staging,
      now,
      reservationTtlMs,
      idempotencyTtlMs,
      stagingCleanupGraceMs,
      rebind: { reservationId: existing.reservationId },
    });
  }

  if (existing.targetKey !== targetKey) {
    return { kind: 'conflict', reason: 'idempotency_key_reused_with_different_target' };
  }
  if (existing.bodyHash !== bodyHash) {
    return { kind: 'conflict', reason: 'idempotency_key_reused_with_different_body' };
  }
  if (existing.status === 'completed') {
    if (existing.responseStatus === null || existing.responseBody === null) {
      throw new Error('completed agent observability idempotency row is corrupt');
    }
    return {
      kind: 'replay',
      response: {
        status: existing.responseStatus,
        body: parseAgentObservabilityMutationResponse(target, existing.responseBody),
      },
    };
  }

  if (reservationLive) {
    return {
      kind: 'in_progress',
      reservation: reservationInProgress(linkedReservation),
    };
  }

  if (linkedReservation.status === 'pending') {
    const expired = await expireLockedReservation(tx, linkedReservation, now);
    if (!expired) {
      return { kind: 'in_progress', reservation: reservationInProgress(linkedReservation) };
    }
    await markStagingCleanupPendingForTerminalReservation(tx, {
      reservationId: linkedReservation.id,
      generation: linkedReservation.generation,
      now,
    });
  }

  return createReservationAttempt(tx, {
    identity,
    target,
    targetKey,
    bodyHash,
    expectedVersions,
    staging,
    now,
    reservationTtlMs,
    idempotencyTtlMs,
    stagingCleanupGraceMs,
    rebind: { reservationId: existing.reservationId },
  });
}

/** Owner + generation CAS. Fencing never schedules external cleanup itself. */
export async function fenceAgentObservabilityMutationReservation(
  tx: DbTransaction,
  input: AgentObservabilityReservationReference & { now?: Date },
): Promise<boolean> {
  const reference = normalizeReservationReference(input);
  const now = validDate(input.now ?? new Date());
  const updated = await tx
    .update(agentObservabilityMutationReservations)
    .set({ status: 'fenced', fencedAt: now, updatedAt: now })
    .where(reservationCasWhere(reference, 'pending'))
    .returning({ id: agentObservabilityMutationReservations.id });
  if (updated.length !== 1) return false;
  await markStagingCleanupPendingForTerminalReservation(tx, {
    reservationId: reference.reservationId,
    generation: reference.generation,
    now,
  });
  return true;
}

/**
 * Exact finalization-loser settlement. Unlike the compatibility fence API,
 * this distinguishes an authority-owned terminal transition from an invalid
 * reference or durable inconsistency so callers never turn corruption into a
 * normal conflict.
 */
export type SettleAgentObservabilityMutationReservationResult =
  | { kind: 'fenced' }
  | { kind: 'expired' }
  | { kind: 'already_terminal'; status: 'fenced' | 'expired' }
  | { kind: 'committed' }
  | { kind: 'missing' }
  | { kind: 'mismatch' }
  | { kind: 'cas_anomaly' }
  | { kind: 'invalid' };

/**
 * Lock and settle exactly one reservation after its finalizer loses. A live
 * pending reservation is fenced, an elapsed pending reservation is expired,
 * and both get the normal staging-cleanup handoff. An authority fence or prior
 * expiry is a legitimate conflict. All other outcomes are intentionally
 * explicit for the service boundary to fail closed.
 */
export async function settleAgentObservabilityMutationReservation(
  tx: DbTransaction,
  input: AgentObservabilityReservationReference & { now?: Date },
): Promise<SettleAgentObservabilityMutationReservationResult> {
  const reference = normalizeReservationReference(input);
  const now = validDate(input.now ?? new Date());
  // Match finalization's idempotency -> reservation -> staging lock order.
  // `reservationId` is unique, but the exact organization/owner predicate
  // still proves this caller has not crossed a tenant or owner boundary.
  const idempotencies = await tx
    .select()
    .from(agentObservabilityIdempotencyKeys)
    .where(
      and(
        eq(agentObservabilityIdempotencyKeys.organizationId, reference.organizationId),
        eq(agentObservabilityIdempotencyKeys.principal, reference.ownerPrincipal),
        eq(agentObservabilityIdempotencyKeys.reservationId, reference.reservationId),
      ),
    )
    .for('update')
    .limit(2);
  const reservation = (
    await tx
      .select()
      .from(agentObservabilityMutationReservations)
      .where(reservationReferenceWhere(reference))
      .for('update')
      .limit(1)
  )[0];
  if (!reservation) {
    // `id` is globally unique. Lock a same-id row if present so a stale owner,
    // generation, or organization token remains distinguishable from absence.
    const sameId = (
      await tx
        .select({ id: agentObservabilityMutationReservations.id })
        .from(agentObservabilityMutationReservations)
        .where(eq(agentObservabilityMutationReservations.id, reference.reservationId))
        .for('update')
        .limit(1)
    )[0];
    return sameId ? { kind: 'mismatch' } : { kind: 'missing' };
  }

  const state = inspectReservationSettlementState(reservation);
  if (state === 'invalid') return { kind: 'invalid' };
  const idempotency = idempotencies.length === 1 ? idempotencies[0] : undefined;
  if (
    idempotency === undefined ||
    (state === 'committed'
      ? !hasValidCommittedReservationIdempotency(reservation, idempotency)
      : !hasValidPendingReservationIdempotency(reservation, idempotency))
  ) {
    return { kind: 'invalid' };
  }
  const staging = (
    await tx
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(stagingReferenceWhere(reservation.id, reservation.generation))
      .for('update')
      .limit(1)
  )[0];
  if (staging) {
    try {
      assertStagingWriteRows(reference, { reservation, staging });
    } catch (error) {
      if (error instanceof AgentObservabilityMutationValidationError) {
        return { kind: 'invalid' };
      }
      throw error;
    }
    if (
      state === 'committed' ||
      ((state === 'fenced' || state === 'expired') &&
        (staging.status === 'pending' || staging.status === 'written'))
    ) {
      return { kind: 'invalid' };
    }
  }
  if (state === 'fenced' || state === 'expired') {
    return { kind: 'already_terminal', status: state };
  }
  if (state === 'committed') return { kind: 'committed' };

  if (reservation.expiresAt.getTime() <= now.getTime()) {
    const expired = await tx
      .update(agentObservabilityMutationReservations)
      .set({ status: 'expired', expiredAt: now, updatedAt: now })
      .where(
        and(
          reservationCasWhere(reference, 'pending'),
          lte(agentObservabilityMutationReservations.expiresAt, now),
        ),
      )
      .returning({ id: agentObservabilityMutationReservations.id });
    if (expired.length !== 1) return { kind: 'cas_anomaly' };
    await markStagingCleanupPendingForTerminalReservation(tx, {
      reservationId: reference.reservationId,
      generation: reference.generation,
      now,
    });
    return { kind: 'expired' };
  }

  const fenced = await tx
    .update(agentObservabilityMutationReservations)
    .set({ status: 'fenced', fencedAt: now, updatedAt: now })
    .where(reservationCasWhere(reference, 'pending'))
    .returning({ id: agentObservabilityMutationReservations.id });
  if (fenced.length !== 1) return { kind: 'cas_anomaly' };
  await markStagingCleanupPendingForTerminalReservation(tx, {
    reservationId: reference.reservationId,
    generation: reference.generation,
    now,
  });
  return { kind: 'fenced' };
}

/**
 * Emergency-only fencing under caller-held authority locks. Scope validation
 * is deliberately part of this kernel boundary: a workspace disable can fence
 * only its exact setting and current workspace binding, while an organization
 * disable cannot cross into any workspace target. The caller has no owner
 * token because this operation deliberately supersedes a live reservation.
 */
export async function preemptAgentObservabilityMutationReservations(
  tx: DbTransaction,
  input:
    | {
        authority: 'organization';
        organizationId: string;
        targets: readonly OrganizationAgentObservabilityPreemptionTarget[];
        now?: Date;
      }
    | {
        authority: 'workspace';
        organizationId: string;
        workspaceId: string;
        targets: readonly WorkspaceAgentObservabilityPreemptionTarget[];
        now?: Date;
      },
): Promise<{ fenced: number }> {
  const organizationId = requiredIdentifier(input.organizationId);
  const workspaceId =
    input.authority === 'workspace' ? requiredIdentifier(input.workspaceId) : null;
  const now = validDate(input.now ?? new Date());
  const targetsByKey = new Map<string, AgentObservabilityAuthorityPreemptionTarget>();
  for (const value of input.targets) {
    const target = parseAgentObservabilityMutationTarget(value);
    if (!isExactAuthorityPreemptionTarget(target, organizationId, workspaceId)) {
      throw new AgentObservabilityMutationValidationError(
        'invalid agent observability authority preemption target',
      );
    }
    targetsByKey.set(agentObservabilityMutationTargetKey(target), target);
  }
  if (targetsByKey.size === 0) {
    throw new AgentObservabilityMutationValidationError(
      'agent observability authority preemption requires a target',
    );
  }

  let fenced = 0;
  for (const [targetKey, expectedTarget] of [...targetsByKey.entries()].sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    const workspaceCondition =
      workspaceId === null
        ? isNull(agentObservabilityMutationReservations.workspaceId)
        : eq(agentObservabilityMutationReservations.workspaceId, workspaceId);
    const reservations = await tx
      .select()
      .from(agentObservabilityMutationReservations)
      .where(
        and(
          eq(agentObservabilityMutationReservations.organizationId, organizationId),
          workspaceCondition,
          eq(agentObservabilityMutationReservations.targetKey, targetKey),
          eq(agentObservabilityMutationReservations.status, 'pending'),
        ),
      )
      .orderBy(asc(agentObservabilityMutationReservations.id))
      .for('update');
    for (const reservation of reservations) {
      const actualTarget = targetFromReservation(reservation);
      if (
        reservation.organizationId !== organizationId ||
        !isExactAuthorityPreemptionTarget(actualTarget, organizationId, workspaceId) ||
        agentObservabilityMutationTargetKey(actualTarget) !== targetKey ||
        agentObservabilityMutationTargetKey(expectedTarget) !== targetKey
      ) {
        throw new AgentObservabilityMutationValidationError(
          'agent observability authority preemption reservation is corrupt',
        );
      }
      const updated = await tx
        .update(agentObservabilityMutationReservations)
        .set({ status: 'fenced', fencedAt: now, updatedAt: now })
        .where(
          and(
            eq(agentObservabilityMutationReservations.id, reservation.id),
            eq(agentObservabilityMutationReservations.organizationId, organizationId),
            workspaceCondition,
            eq(agentObservabilityMutationReservations.targetKey, targetKey),
            eq(agentObservabilityMutationReservations.generation, reservation.generation),
            eq(agentObservabilityMutationReservations.status, 'pending'),
          ),
        )
        .returning({ id: agentObservabilityMutationReservations.id });
      if (updated.length !== 1) {
        throw new AgentObservabilityMutationValidationError(
          'agent observability authority preemption lost pending reservation',
        );
      }
      await markStagingCleanupPendingForTerminalReservation(tx, {
        reservationId: reservation.id,
        generation: reservation.generation,
        now,
      });
      fenced += 1;
    }
  }
  return { fenced };
}

/** Compatibility wrapper for organization-default emergency disable. */
export async function preemptOrganizationAgentObservabilityMutationReservations(
  tx: DbTransaction,
  input: {
    organizationId: string;
    targets: readonly OrganizationAgentObservabilityPreemptionTarget[];
    now?: Date;
  },
): Promise<{ fenced: number }> {
  return preemptAgentObservabilityMutationReservations(tx, {
    authority: 'organization',
    ...input,
  });
}

/** Workspace explicit-disable fence. It cannot address organization targets. */
export async function preemptWorkspaceAgentObservabilityMutationReservations(
  tx: DbTransaction,
  input: {
    organizationId: string;
    workspaceId: string;
    targets: readonly WorkspaceAgentObservabilityPreemptionTarget[];
    now?: Date;
  },
): Promise<{ fenced: number }> {
  return preemptAgentObservabilityMutationReservations(tx, {
    authority: 'workspace',
    ...input,
  });
}

function isExactAuthorityPreemptionTarget(
  target: AgentObservabilityMutationTarget,
  organizationId: string,
  workspaceId: string | null,
): target is AgentObservabilityAuthorityPreemptionTarget {
  if (target.organizationId !== organizationId) return false;
  if (workspaceId === null) {
    return (
      target.type === 'organization_setting' ||
      (target.type === 'binding' && target.bindingScope === 'organization')
    );
  }
  return (
    (target.type === 'workspace_setting' && target.workspaceId === workspaceId) ||
    (target.type === 'binding' &&
      target.bindingScope === 'workspace' &&
      target.workspaceId === workspaceId)
  );
}

/** Expiry uses the same owner + generation fence. */
export async function expireAgentObservabilityMutationReservation(
  tx: DbTransaction,
  input: AgentObservabilityReservationReference & { now?: Date },
): Promise<boolean> {
  const reference = normalizeReservationReference(input);
  const now = validDate(input.now ?? new Date());
  const updated = await tx
    .update(agentObservabilityMutationReservations)
    .set({ status: 'expired', expiredAt: now, updatedAt: now })
    .where(
      and(
        reservationCasWhere(reference, 'pending'),
        lte(agentObservabilityMutationReservations.expiresAt, now),
      ),
    )
    .returning({ id: agentObservabilityMutationReservations.id });
  if (updated.length !== 1) return false;
  await markStagingCleanupPendingForTerminalReservation(tx, {
    reservationId: reference.reservationId,
    generation: reference.generation,
    now,
  });
  return true;
}

export interface AgentObservabilityStagedActivation {
  candidateBindingId: string;
  credentialVersion: number;
  secretRef: AgentObservabilitySecretReference;
}

export interface FinalizeAgentObservabilityMutationInput {
  reservation: AgentObservabilityReservationReference;
  expectedVersions: AgentObservabilityExpectedVersions;
  /** Old exact head for a same-binding rotation only. */
  supersededSecretRef?: AgentObservabilitySecretReference;
  /** Cache status only. Body always comes from same-transaction Registry state. */
  responseStatus: number;
  audit: {
    action: string;
    authMethod: string;
    requestId: string;
  };
  /** DB-only. Staged activation is metadata/ref only; never bundle bytes. */
  apply: (input: AgentObservabilityMutationApplyInput) => Promise<{ applied: boolean }>;
  now?: Date;
}

export interface AgentObservabilityMutationApplyInput {
  db: Pick<DbTransaction, 'delete' | 'execute' | 'insert' | 'select' | 'update'>;
  reservation: AgentObservabilityReservedMutation;
  stagedActivation: AgentObservabilityStagedActivation | null;
}

export type FinalizeAgentObservabilityMutationResult =
  | { kind: 'committed'; response: { status: number; body: AgentObservabilityMutationResponse } }
  | {
      kind:
        | 'reservation_not_live'
        | 'reservation_expired'
        | 'expected_versions_mismatch'
        | 'staging_not_written'
        | 'invalid_superseded_secret'
        | 'invalid_credential_generation'
        | 'activation_not_proven'
        | 'authoritative_state_mismatch'
        | 'version_conflict';
    };

/**
 * Finalize under a savepoint. Callback writes, credential-head proof, audit,
 * staging consumption, outbox, reservation, and idempotency completion either
 * all commit or all roll back. No external SecretStore operation occurs here.
 */
export async function finalizeAgentObservabilityMutation(
  tx: DbTransaction,
  input: FinalizeAgentObservabilityMutationInput,
): Promise<FinalizeAgentObservabilityMutationResult> {
  const reference = normalizeReservationReference(input.reservation);
  const expectedVersions = normalizeExpectedVersions(input.expectedVersions);
  const status = safeResponseStatus(input.responseStatus);
  const audit = normalizeAudit(input.audit);
  const now = validDate(input.now ?? new Date());
  if (
    input.supersededSecretRef !== undefined &&
    !isAgentObservabilitySecretReference(input.supersededSecretRef)
  ) {
    throw new AgentObservabilityMutationValidationError();
  }
  await requireRepeatableReadMutationTransaction(tx);

  try {
    return await tx.transaction(async (savepoint) => {
      const db = savepoint as unknown as DbTransaction;
      // Preserve idempotency -> reservation lock order used by recovery and
      // retention. This prevents a finalizer from deadlocking a stale-key
      // rebind on the same attempt.
      const idempotency = (
        await db
          .select()
          .from(agentObservabilityIdempotencyKeys)
          .where(
            and(
              eq(agentObservabilityIdempotencyKeys.organizationId, reference.organizationId),
              eq(agentObservabilityIdempotencyKeys.principal, reference.ownerPrincipal),
              eq(agentObservabilityIdempotencyKeys.reservationId, reference.reservationId),
            ),
          )
          .for('update')
          .limit(1)
      )[0];
      if (!idempotency || idempotency.status !== 'pending') {
        throw new FinalizationStopped('reservation_not_live');
      }
      const reservation = (
        await db
          .select()
          .from(agentObservabilityMutationReservations)
          .where(reservationReferenceWhere(reference))
          .for('update')
          .limit(1)
      )[0];
      if (!reservation || reservation.status !== 'pending') {
        throw new FinalizationStopped('reservation_not_live');
      }
      if (reservation.expiresAt.getTime() <= now.getTime()) {
        throw new FinalizationStopped('reservation_expired');
      }
      if (!storedExpectedVersionsEqual(reservation, expectedVersions)) {
        throw new FinalizationStopped('expected_versions_mismatch');
      }

      const target = targetFromReservation(reservation);
      if (
        idempotency.bodyHash !== reservation.bodyHash ||
        idempotency.targetKey !== reservation.targetKey
      ) {
        throw new FinalizationStopped('reservation_not_live');
      }

      const staging = (
        await db
          .select()
          .from(agentObservabilityCredentialStagingIntents)
          .where(
            and(
              eq(agentObservabilityCredentialStagingIntents.reservationId, reservation.id),
              eq(agentObservabilityCredentialStagingIntents.generation, reservation.generation),
            ),
          )
          .for('update')
          .limit(1)
      )[0];
      if (staging && staging.status !== 'written') {
        throw new FinalizationStopped('staging_not_written');
      }
      const stagedActivation = staging ? activationFromStaging(staging) : null;

      await verifyStagedCredentialGenerationBeforeApply(
        db,
        target,
        expectedVersions,
        stagedActivation,
        input.supersededSecretRef,
      );

      if (input.supersededSecretRef !== undefined) {
        await verifySupersededRotationHead(
          db,
          target,
          expectedVersions,
          stagedActivation,
          input.supersededSecretRef,
        );
      }

      const reservedMutation: AgentObservabilityReservedMutation = {
        organizationId: reservation.organizationId,
        reservationId: reservation.id,
        ownerPrincipal: reservation.ownerPrincipal,
        generation: reservation.generation,
        target,
        expectedVersions,
        expiresAt: reservation.expiresAt,
      };
      const applyResult = await input.apply({
        db: db as Pick<DbTransaction, 'delete' | 'execute' | 'insert' | 'select' | 'update'>,
        reservation: reservedMutation,
        stagedActivation,
      });
      if (!applyResult || applyResult.applied !== true) {
        throw new FinalizationStopped('version_conflict');
      }

      if (stagedActivation && !(await hasExactStagedCredentialHead(db, target, stagedActivation))) {
        throw new FinalizationStopped('activation_not_proven');
      }

      const response = await loadAuthoritativeMutationResponse(db, target, stagedActivation);
      if (staging) {
        const consumed = await db
          .delete(agentObservabilityCredentialStagingIntents)
          .where(
            and(
              eq(agentObservabilityCredentialStagingIntents.reservationId, reservation.id),
              eq(agentObservabilityCredentialStagingIntents.generation, reservation.generation),
              eq(agentObservabilityCredentialStagingIntents.status, 'written'),
            ),
          )
          .returning({ reservationId: agentObservabilityCredentialStagingIntents.reservationId });
        if (consumed.length !== 1) throw new FinalizationStopped('staging_not_written');
      }

      if (input.supersededSecretRef !== undefined) {
        const bindingTarget = bindingTargetForRotation(target);
        await db
          .insert(agentObservabilitySecretCleanupOutbox)
          .values({
            id: newId('aomclean'),
            organizationId: reservation.organizationId,
            bindingId: bindingTarget.bindingId,
            bindingScope: bindingTarget.bindingScope,
            secretRef: input.supersededSecretRef,
            status: 'pending',
            claimToken: null,
            leaseExpiresAt: null,
            attemptCount: 0,
            nextAttemptAt: now,
            createdAt: now,
            updatedAt: now,
          })
          .onConflictDoNothing();
      }

      await db.insert(adminAuditEvents).values({
        id: newId('audit'),
        organizationId: reservation.organizationId,
        workspaceId: targetWorkspaceId(target),
        actor: reservation.ownerPrincipal,
        authMethod: audit.authMethod,
        action: audit.action,
        targetType: target.type,
        targetId: targetId(target),
        requestId: audit.requestId,
        result: 'success',
        metadata: agentObservabilityMutationAuditMetadata(reservation),
        createdAt: now,
      });

      const committed = await db
        .update(agentObservabilityMutationReservations)
        .set({ status: 'committed', committedAt: now, updatedAt: now })
        .where(reservationCasWhere(reference, 'pending'))
        .returning({ id: agentObservabilityMutationReservations.id });
      if (committed.length !== 1) throw new FinalizationStopped('reservation_not_live');

      const completed = await db
        .update(agentObservabilityIdempotencyKeys)
        .set({
          status: 'completed',
          responseStatus: status,
          responseBody: response,
          completedAt: now,
          updatedAt: now,
        })
        .where(
          and(
            eq(agentObservabilityIdempotencyKeys.organizationId, reservation.organizationId),
            eq(agentObservabilityIdempotencyKeys.principal, reservation.ownerPrincipal),
            eq(agentObservabilityIdempotencyKeys.reservationId, reservation.id),
            eq(agentObservabilityIdempotencyKeys.targetKey, reservation.targetKey),
            eq(agentObservabilityIdempotencyKeys.status, 'pending'),
          ),
        )
        .returning({ reservationId: agentObservabilityIdempotencyKeys.reservationId });
      if (completed.length !== 1) throw new FinalizationStopped('reservation_not_live');
      return { kind: 'committed', response: { status, body: response } };
    });
  } catch (error) {
    if (error instanceof FinalizationStopped) return { kind: error.kind };
    throw error;
  }
}

/** Exact contract allowlist for cached/replayed admin mutation responses. */
export function parseAgentObservabilityMutationResponse(
  target: AgentObservabilityMutationTarget,
  value: unknown,
): AgentObservabilityMutationResponse {
  const parsed =
    expectedResponseScope(target) === 'organization'
      ? OrganizationAgentObservabilityStateSchema.safeParse(value)
      : WorkspaceAgentObservabilityStateSchema.safeParse(value);
  if (!parsed.success) throw new AgentObservabilityMutationValidationError();
  if (parsed.data.organization_id !== target.organizationId) {
    throw new AgentObservabilityMutationValidationError();
  }
  const workspaceId = targetWorkspaceId(target);
  if (workspaceId === null) {
    if (parsed.data.workspace_id !== null) throw new AgentObservabilityMutationValidationError();
  } else if (parsed.data.workspace_id !== workspaceId) {
    throw new AgentObservabilityMutationValidationError();
  }
  return parsed.data;
}

/** Fixed audit metadata. No caller-controlled metadata, refs, or candidates. */
export function agentObservabilityMutationAuditMetadata(
  reservation: Pick<
    typeof agentObservabilityMutationReservations.$inferSelect,
    | 'expectedConfigVersion'
    | 'expectedCredentialVersion'
    | 'expectedStateVersion'
    | 'generation'
    | 'targetType'
  >,
): Record<string, string | number | null> {
  return {
    expected_config_version: reservation.expectedConfigVersion,
    expected_credential_version: reservation.expectedCredentialVersion,
    expected_state_version: reservation.expectedStateVersion,
    reservation_generation: reservation.generation,
    target_type: reservation.targetType,
  };
}

export interface AgentObservabilityStagingWriteClaim {
  reservation: AgentObservabilityReservationReference;
  secretRef: AgentObservabilitySecretReference;
  writerToken: string;
  writerLeaseExpiresAt: Date;
}

export type AgentObservabilityStagingWriteCompletion =
  | { kind: 'written' }
  | { kind: 'retryable' }
  | {
      kind: 'tombstoned';
      /** Local abandonment differs from an emergency fence by another writer. */
      reason: 'abandoned' | 'preempted' | 'expired';
    }
  | { kind: 'stale' };

/** Durable reason why a writer could not acquire its staging intent. */
export type AgentObservabilityStagingWriteNotClaimed = {
  kind: 'not_claimed';
  reason: 'preempted' | 'expired' | 'busy' | 'invalid';
};

export type AgentObservabilityStagingWriteResult =
  | AgentObservabilityStagingWriteCompletion
  | AgentObservabilityStagingWriteNotClaimed;

/**
 * Short-transaction writer claim. The actual put is deliberately not part of
 * this API: callers pass a closure to writeAgentObservabilityStagingIntent so
 * bundle bytes never enter any DB-facing kernel input.
 */
export async function claimAgentObservabilityStagingWrite(
  db: DbClient,
  input: {
    reservation: AgentObservabilityReservationReference;
    now?: Date;
    writerLeaseMs?: number;
  },
): Promise<AgentObservabilityStagingWriteClaim | null> {
  const reference = normalizeReservationReference(input.reservation);
  const now = validDate(input.now ?? new Date());
  const writerLeaseMs = validPositiveDuration(
    input.writerLeaseMs ?? AGENT_OBSERVABILITY_STAGING_WRITER_LEASE_MS,
  );
  return db.transaction(async (tx) => {
    const locked = await lockReservationAndStaging(tx as DbTransaction, reference);
    if (!locked) return null;
    const { reservation, staging } = locked;
    // Lease expiry does not prove an external put has stopped. While its
    // reservation is live, retain that writer rather than risking a second put
    // of the same active ref. A known failed completion returns pending; an
    // unacknowledged writer becomes a tombstone after terminal transition.
    if (!reservationIsLive(reservation, now) || staging.status !== 'pending') return null;
    if (!isAgentObservabilitySecretReference(staging.secretRef)) {
      throw new Error('agent observability staging secret reference is corrupt');
    }
    const writerToken = newId('aomwrite');
    const writerLeaseExpiresAt = new Date(now.getTime() + writerLeaseMs);
    const claimed = await tx
      .update(agentObservabilityCredentialStagingIntents)
      .set({
        status: 'writing',
        writerToken,
        writerLeaseExpiresAt,
        updatedAt: now,
      })
      .where(
        and(
          stagingReferenceWhere(staging.reservationId, staging.generation),
          eq(agentObservabilityCredentialStagingIntents.status, 'pending'),
        ),
      )
      .returning({ reservationId: agentObservabilityCredentialStagingIntents.reservationId });
    if (claimed.length !== 1) return null;
    return {
      reservation: reservationReference(reservation),
      secretRef: staging.secretRef,
      writerToken,
      writerLeaseExpiresAt,
    };
  });
}

/** Extend only a current writer that still owns a live reservation. */
export async function heartbeatAgentObservabilityStagingWrite(
  db: DbClient,
  input: Pick<AgentObservabilityStagingWriteClaim, 'reservation' | 'writerToken'> & {
    now?: Date;
    writerLeaseMs?: number;
  },
): Promise<boolean> {
  const reference = normalizeReservationReference(input.reservation);
  const writerToken = requiredOpaqueId(input.writerToken);
  const now = validDate(input.now ?? new Date());
  const writerLeaseMs = validPositiveDuration(
    input.writerLeaseMs ?? AGENT_OBSERVABILITY_STAGING_WRITER_LEASE_MS,
  );
  return db.transaction(async (tx) => {
    const locked = await lockReservationAndStaging(tx as DbTransaction, reference);
    if (
      !locked ||
      !reservationIsLive(locked.reservation, now) ||
      locked.staging.status !== 'writing' ||
      locked.staging.writerToken !== writerToken ||
      locked.staging.writerLeaseExpiresAt === null ||
      locked.staging.writerLeaseExpiresAt.getTime() <= now.getTime()
    ) {
      return false;
    }
    const updated = await tx
      .update(agentObservabilityCredentialStagingIntents)
      .set({ writerLeaseExpiresAt: new Date(now.getTime() + writerLeaseMs), updatedAt: now })
      .where(
        and(
          stagingReferenceWhere(reference.reservationId, reference.generation),
          eq(agentObservabilityCredentialStagingIntents.status, 'writing'),
          eq(agentObservabilityCredentialStagingIntents.writerToken, writerToken),
          gt(agentObservabilityCredentialStagingIntents.writerLeaseExpiresAt, now),
        ),
      )
      .returning({ reservationId: agentObservabilityCredentialStagingIntents.reservationId });
    return updated.length === 1;
  });
}

/**
 * Complete a writer token. Terminal/stale attempts are converted to a durable
 * cleanup tombstone rather than being allowed to mark a late put as written.
 */
export async function completeAgentObservabilityStagingWrite(
  db: DbClient,
  input: Pick<AgentObservabilityStagingWriteClaim, 'reservation' | 'writerToken'> & {
    outcome: 'written' | 'failed' | 'abandoned';
    now?: Date;
  },
): Promise<AgentObservabilityStagingWriteCompletion> {
  const reference = normalizeReservationReference(input.reservation);
  const writerToken = requiredOpaqueId(input.writerToken);
  const now = validDate(input.now ?? new Date());
  return db.transaction(async (tx) => {
    const locked = await lockReservationAndStaging(tx as DbTransaction, reference);
    if (
      !locked ||
      locked.staging.status !== 'writing' ||
      locked.staging.writerToken !== writerToken
    ) {
      return { kind: 'stale' };
    }
    const reservationLive = await ensureReservationLiveOrExpired(
      tx as DbTransaction,
      locked.reservation,
      now,
    );
    // A timeout or lost heartbeat cannot prove that a provider has stopped a
    // put. Revoke the writer and retain its tombstone rather than permitting a
    // retry to race a late write at this opaque reference.
    if (!reservationLive) {
      await tombstoneStagingWriter(tx as DbTransaction, locked.staging, now);
      return {
        kind: 'tombstoned',
        reason: locked.reservation.status === 'fenced' ? 'preempted' : 'expired',
      };
    }
    if (input.outcome === 'abandoned') {
      await tombstoneStagingWriter(tx as DbTransaction, locked.staging, now);
      return { kind: 'tombstoned', reason: 'abandoned' };
    }
    if (
      locked.staging.writerLeaseExpiresAt === null ||
      locked.staging.writerLeaseExpiresAt.getTime() <= now.getTime()
    ) {
      return { kind: 'stale' };
    }
    const updated = await tx
      .update(agentObservabilityCredentialStagingIntents)
      .set(
        input.outcome === 'written'
          ? {
              status: 'written',
              writerToken: null,
              writerLeaseExpiresAt: null,
              putCompletedAt: now,
              updatedAt: now,
            }
          : {
              status: 'pending',
              writerToken: null,
              writerLeaseExpiresAt: null,
              updatedAt: now,
            },
      )
      .where(
        and(
          stagingReferenceWhere(reference.reservationId, reference.generation),
          eq(agentObservabilityCredentialStagingIntents.status, 'writing'),
          eq(agentObservabilityCredentialStagingIntents.writerToken, writerToken),
          gt(agentObservabilityCredentialStagingIntents.writerLeaseExpiresAt, now),
        ),
      )
      .returning({ reservationId: agentObservabilityCredentialStagingIntents.reservationId });
    if (updated.length !== 1) return { kind: 'stale' };
    return { kind: input.outcome === 'written' ? 'written' : 'retryable' };
  });
}

/**
 * Claim -> put outside transaction -> token-CAS completion. The supplied put
 * closure owns raw bundle bytes and receives a bounded cancellation signal.
 * Automatic heartbeats keep a healthy write live only until its deadline. A
 * timeout or failed heartbeat tombstones the claim because cancellation cannot
 * prove that a provider did not complete a late put.
 */
export async function writeAgentObservabilityStagingIntent(
  db: DbClient,
  input: {
    reservation: AgentObservabilityReservationReference;
    put: (input: {
      secretRef: AgentObservabilitySecretReference;
      heartbeat: () => Promise<boolean>;
      signal: AbortSignal;
    }) => Promise<void>;
    now?: Date;
    writerLeaseMs?: number;
    heartbeatMs?: number;
    timeoutMs?: number;
  },
): Promise<AgentObservabilityStagingWriteResult> {
  const writerLeaseMs = validPositiveDuration(
    input.writerLeaseMs ?? AGENT_OBSERVABILITY_STAGING_WRITER_LEASE_MS,
  );
  const heartbeatMs = validPositiveDuration(
    input.heartbeatMs ?? Math.max(1, Math.floor(writerLeaseMs / 3)),
  );
  if (heartbeatMs >= writerLeaseMs) throw new AgentObservabilityMutationValidationError();
  const timeoutMs = validPositiveDuration(
    input.timeoutMs ?? Math.min(AGENT_OBSERVABILITY_STAGING_WRITER_TIMEOUT_MS, writerLeaseMs - 1),
  );
  if (timeoutMs >= writerLeaseMs) throw new AgentObservabilityMutationValidationError();
  const now = validDate(input.now ?? new Date());
  const claimInput: Parameters<typeof claimAgentObservabilityStagingWrite>[1] = {
    reservation: input.reservation,
    writerLeaseMs,
    now,
  };
  const claim = await claimAgentObservabilityStagingWrite(db, claimInput);
  if (!claim) {
    return classifyAgentObservabilityStagingWriteNotClaimed(db, {
      reservation: input.reservation,
      now,
    });
  }

  const abortController = new AbortController();
  let active = true;
  let heartbeatInFlight = false;
  const heartbeatTasks = new Set<Promise<boolean>>();
  const abort = () => {
    if (!abortController.signal.aborted) abortController.abort();
  };
  const heartbeat = (): Promise<boolean> => {
    if (!active || abortController.signal.aborted) return Promise.resolve(false);
    const task = heartbeatAgentObservabilityStagingWrite(db, {
      reservation: claim.reservation,
      writerToken: claim.writerToken,
      writerLeaseMs,
    }).then(
      (renewed) => {
        // An in-flight heartbeat can finish after timeout. Its DB CAS is
        // harmless once completion tombstones the writer; never report it as
        // healthy to the caller after local ownership has ended.
        if (!active || abortController.signal.aborted) return false;
        if (!renewed) abort();
        return renewed;
      },
      () => {
        abort();
        return false;
      },
    );
    heartbeatTasks.add(task);
    void task.then(
      () => heartbeatTasks.delete(task),
      () => heartbeatTasks.delete(task),
    );
    return task;
  };
  const timer = setInterval(() => {
    if (!active || heartbeatInFlight || abortController.signal.aborted) return;
    heartbeatInFlight = true;
    void heartbeat().finally(() => {
      heartbeatInFlight = false;
    });
  }, heartbeatMs);
  const timeout = setTimeout(abort, timeoutMs);
  let outcome: 'written' | 'failed' | 'abandoned';
  try {
    outcome = await awaitStagingPutOrAbort(
      Promise.resolve().then(() =>
        input.put({
          secretRef: claim.secretRef,
          heartbeat,
          signal: abortController.signal,
        }),
      ),
      abortController.signal,
    );
  } finally {
    active = false;
    clearInterval(timer);
    clearTimeout(timeout);
    // Do not let a stuck DB heartbeat defeat the bounded SecretStore wait.
    // Give ordinary in-flight heartbeats a short chance to finish before the
    // completion CAS, then detach them with their rejection handlers intact.
    await waitForStagingHeartbeatDrain(heartbeatTasks, heartbeatMs);
    heartbeatTasks.clear();
  }
  return completeAgentObservabilityStagingWriteWithRetry(db, {
    reservation: claim.reservation,
    writerToken: claim.writerToken,
    outcome,
  });
}

async function waitForStagingHeartbeatDrain(
  tasks: ReadonlySet<Promise<boolean>>,
  heartbeatMs: number,
): Promise<void> {
  if (tasks.size === 0) return;
  const drainMs = Math.min(STAGING_HEARTBEAT_DRAIN_MAX_MS, heartbeatMs);
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, drainMs);
    void Promise.allSettled([...tasks]).then(() => {
      clearTimeout(timer);
      resolve();
    });
  });
}

async function completeAgentObservabilityStagingWriteWithRetry(
  db: DbClient,
  input: Parameters<typeof completeAgentObservabilityStagingWrite>[1],
): Promise<AgentObservabilityStagingWriteCompletion> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await completeAgentObservabilityStagingWrite(db, input);
    } catch (error) {
      if (!isSerializationFailure(error) || attempt >= STAGING_COMPLETION_SERIALIZATION_RETRIES) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, attempt + 1));
    }
  }
}

/**
 * A failed claim is not self-describing: a disable may have fenced the
 * reservation, it may have expired, or another durable writer may own it.
 * Re-read the exact rows under lock instead of guessing from the null claim.
 */
async function classifyAgentObservabilityStagingWriteNotClaimed(
  db: DbClient,
  input: {
    reservation: AgentObservabilityReservationReference;
    now: Date;
  },
): Promise<AgentObservabilityStagingWriteNotClaimed> {
  const reference = normalizeReservationReference(input.reservation);
  const now = validDate(input.now);
  return db.transaction(async (tx) => {
    const locked = await lockReservationAndStaging(tx as DbTransaction, reference);
    if (!locked) return { kind: 'not_claimed', reason: 'invalid' };
    assertStagingWriteRows(reference, locked);
    const { reservation, staging } = locked;
    switch (reservation.status) {
      case 'fenced':
        return { kind: 'not_claimed', reason: 'preempted' };
      case 'expired':
        return { kind: 'not_claimed', reason: 'expired' };
      case 'committed':
        return { kind: 'not_claimed', reason: 'invalid' };
      case 'pending':
        if (!reservationIsLive(reservation, now)) {
          await ensureReservationLiveOrExpired(tx as DbTransaction, reservation, now);
          return { kind: 'not_claimed', reason: 'expired' };
        }
        switch (staging.status) {
          case 'pending':
          case 'writing':
          case 'written':
            return { kind: 'not_claimed', reason: 'busy' };
          case 'cleanup_pending':
          case 'cleaning':
            return { kind: 'not_claimed', reason: 'invalid' };
          default:
            throw new AgentObservabilityMutationValidationError();
        }
      default:
        throw new AgentObservabilityMutationValidationError();
    }
  });
}

export interface AgentObservabilityStagingCleanupClaim {
  reservation: AgentObservabilityReservationReference;
  secretRef: AgentObservabilitySecretReference;
  cleanupToken: string;
}

export interface ReconcileAgentObservabilityStagingIntentsResult {
  claimed: number;
  deleteSucceeded: number;
  tombstonesRetained: number;
  failed: number;
  stale: number;
}

/**
 * Claim terminal tombstones only. A current writer lease blocks cleanup even
 * after reservation fence/expiry; once stale, its token is revoked and every
 * later put completion becomes stale instead of reviving the intent.
 */
export async function claimAgentObservabilityStagingCleanup(
  db: DbClient,
  options: { now?: Date; batchSize?: number; cleanupLeaseMs?: number } = {},
): Promise<AgentObservabilityStagingCleanupClaim[]> {
  const now = validDate(options.now ?? new Date());
  const batchSize = validBatchSize(options.batchSize ?? 100);
  const cleanupLeaseMs = validPositiveDuration(
    options.cleanupLeaseMs ?? AGENT_OBSERVABILITY_STAGING_CLEANUP_LEASE_MS,
  );
  const candidates = await db
    .select({ reservationId: agentObservabilityCredentialStagingIntents.reservationId })
    .from(agentObservabilityCredentialStagingIntents)
    .where(
      or(
        and(
          eq(agentObservabilityCredentialStagingIntents.status, 'cleanup_pending'),
          lte(agentObservabilityCredentialStagingIntents.nextCleanupAt, now),
        ),
        and(
          eq(agentObservabilityCredentialStagingIntents.status, 'cleaning'),
          lte(agentObservabilityCredentialStagingIntents.cleanupLeaseExpiresAt, now),
        ),
        and(
          eq(agentObservabilityCredentialStagingIntents.status, 'writing'),
          lte(agentObservabilityCredentialStagingIntents.writerLeaseExpiresAt, now),
        ),
        and(
          eq(agentObservabilityCredentialStagingIntents.status, 'pending'),
          lte(agentObservabilityCredentialStagingIntents.nextCleanupAt, now),
        ),
        and(
          eq(agentObservabilityCredentialStagingIntents.status, 'written'),
          lte(agentObservabilityCredentialStagingIntents.nextCleanupAt, now),
        ),
      ),
    )
    .orderBy(asc(agentObservabilityCredentialStagingIntents.nextCleanupAt))
    .limit(batchSize);

  const claims: AgentObservabilityStagingCleanupClaim[] = [];
  for (const candidate of candidates) {
    const claim = await db.transaction(async (tx) => {
      const reservation = (
        await tx
          .select()
          .from(agentObservabilityMutationReservations)
          .where(eq(agentObservabilityMutationReservations.id, candidate.reservationId))
          .for('update', { skipLocked: true })
          .limit(1)
      )[0];
      if (!reservation) return null;
      const staging = (
        await tx
          .select()
          .from(agentObservabilityCredentialStagingIntents)
          .where(stagingReferenceWhere(reservation.id, reservation.generation))
          .for('update')
          .limit(1)
      )[0];
      if (!staging) return null;
      const terminal = !(await ensureReservationLiveOrExpired(
        tx as DbTransaction,
        reservation,
        now,
      ));
      if (!terminal) return null;
      const terminalStatus =
        reservation.status === 'pending' && reservation.expiresAt.getTime() <= now.getTime()
          ? 'expired'
          : reservation.status;
      if (terminalStatus !== 'fenced' && terminalStatus !== 'expired') return null;
      if (!isAgentObservabilitySecretReference(staging.secretRef)) {
        throw new Error('agent observability staging secret reference is corrupt');
      }

      let status = staging.status;
      if (status === 'writing') {
        if (
          staging.writerLeaseExpiresAt !== null &&
          staging.writerLeaseExpiresAt.getTime() > now.getTime()
        ) {
          return null;
        }
        await tombstoneStagingWriter(tx as DbTransaction, staging, now);
        status = 'cleanup_pending';
      } else if (status === 'pending' || status === 'written') {
        await markStagingCleanupPendingForTerminalReservation(tx as DbTransaction, {
          reservationId: staging.reservationId,
          generation: staging.generation,
          now,
        });
        status = 'cleanup_pending';
      }

      const retryable =
        (status === 'cleanup_pending' && staging.nextCleanupAt.getTime() <= now.getTime()) ||
        (status === 'cleaning' &&
          staging.cleanupLeaseExpiresAt !== null &&
          staging.cleanupLeaseExpiresAt.getTime() <= now.getTime());
      if (!retryable) return null;
      const cleanupToken = newId('aomstageclean');
      const claimed = await tx
        .update(agentObservabilityCredentialStagingIntents)
        .set({
          status: 'cleaning',
          cleanupToken,
          cleanupLeaseExpiresAt: new Date(now.getTime() + cleanupLeaseMs),
          updatedAt: now,
        })
        .where(
          and(
            stagingReferenceWhere(staging.reservationId, staging.generation),
            eq(agentObservabilityCredentialStagingIntents.status, status),
          ),
        )
        .returning({ reservationId: agentObservabilityCredentialStagingIntents.reservationId });
      if (claimed.length !== 1) return null;
      return {
        reservation: reservationReference(reservation),
        secretRef: staging.secretRef,
        cleanupToken,
      } satisfies AgentObservabilityStagingCleanupClaim;
    });
    if (claim) claims.push(claim);
  }
  return claims;
}

/** Complete only a current cleanup token. Tombstones always reschedule after delete. */
export async function completeAgentObservabilityStagingCleanup(
  db: DbClient,
  input: AgentObservabilityStagingCleanupClaim & {
    outcome: 'deleted' | 'failed';
    now?: Date;
    cleanupIntervalMs?: number;
  },
): Promise<'rescheduled' | 'failed' | 'stale'> {
  const reference = normalizeReservationReference(input.reservation);
  const cleanupToken = requiredOpaqueId(input.cleanupToken);
  const now = validDate(input.now ?? new Date());
  const cleanupIntervalMs = validPositiveDuration(
    input.cleanupIntervalMs ?? AGENT_OBSERVABILITY_STAGING_CLEANUP_INTERVAL_MS,
  );
  return db.transaction(async (tx) => {
    const staging = (
      await tx
        .select()
        .from(agentObservabilityCredentialStagingIntents)
        .where(
          and(
            stagingReferenceWhere(reference.reservationId, reference.generation),
            eq(agentObservabilityCredentialStagingIntents.status, 'cleaning'),
            eq(agentObservabilityCredentialStagingIntents.cleanupToken, cleanupToken),
          ),
        )
        .for('update')
        .limit(1)
    )[0];
    if (!staging) return 'stale';
    const nextCleanupAt = new Date(now.getTime() + cleanupIntervalMs);
    const updated = await tx
      .update(agentObservabilityCredentialStagingIntents)
      .set({
        status: 'cleanup_pending',
        cleanupToken: null,
        cleanupLeaseExpiresAt: null,
        nextCleanupAt,
        updatedAt: now,
      })
      .where(
        and(
          stagingReferenceWhere(reference.reservationId, reference.generation),
          eq(agentObservabilityCredentialStagingIntents.status, 'cleaning'),
          eq(agentObservabilityCredentialStagingIntents.cleanupToken, cleanupToken),
        ),
      )
      .returning({ reservationId: agentObservabilityCredentialStagingIntents.reservationId });
    if (updated.length !== 1) return 'stale';
    return input.outcome === 'deleted' ? 'rescheduled' : 'failed';
  });
}

/** DB claim -> SecretStore.delete outside tx -> token-CAS completion. */
export async function reconcileAgentObservabilityStagingIntents(
  db: DbClient,
  secretStore: SecretStore,
  options: {
    now?: Date;
    batchSize?: number;
    cleanupLeaseMs?: number;
    cleanupIntervalMs?: number;
    /** Bound one provider delete; must remain shorter than its cleanup lease. */
    deleteTimeoutMs?: number;
    /** Stop current and unstarted deletes while retaining their tombstones. */
    signal?: AbortSignal;
  } = {},
): Promise<ReconcileAgentObservabilityStagingIntentsResult> {
  const now = validDate(options.now ?? new Date());
  const batchSize = validBatchSize(options.batchSize ?? 100);
  const cleanupLeaseMs = validPositiveDuration(
    options.cleanupLeaseMs ?? AGENT_OBSERVABILITY_STAGING_CLEANUP_LEASE_MS,
  );
  const deleteTimeoutMs = boundedCleanupDeleteTimeoutMs(
    options.deleteTimeoutMs,
    cleanupLeaseMs,
    AGENT_OBSERVABILITY_STAGING_CLEANUP_DELETE_TIMEOUT_MS,
  );
  const result: ReconcileAgentObservabilityStagingIntentsResult = {
    claimed: 0,
    deleteSucceeded: 0,
    tombstonesRetained: 0,
    failed: 0,
    stale: 0,
  };
  if (options.signal?.aborted) return result;

  const claims = await claimAgentObservabilityStagingCleanup(db, {
    now,
    batchSize,
    cleanupLeaseMs,
  });
  result.claimed = claims.length;
  for (const claim of claims) {
    const outcome = await awaitSecretStoreDeleteOrAbort(secretStore, claim.secretRef, {
      parentSignal: options.signal,
      timeoutMs: deleteTimeoutMs,
    });
    const completionInput: Parameters<typeof completeAgentObservabilityStagingCleanup>[1] = {
      ...claim,
      outcome,
      now,
    };
    if (options.cleanupIntervalMs !== undefined) {
      completionInput.cleanupIntervalMs = options.cleanupIntervalMs;
    }
    const completion = await completeAgentObservabilityStagingCleanup(db, completionInput);
    if (outcome === 'deleted') {
      if (completion === 'rescheduled') {
        result.deleteSucceeded += 1;
        result.tombstonesRetained += 1;
      } else if (completion === 'stale') {
        result.stale += 1;
      }
    } else if (completion === 'failed') {
      result.failed += 1;
    } else if (completion === 'stale') {
      result.stale += 1;
    }
  }
  return result;
}

export interface AgentObservabilitySecretCleanupClaim {
  id: string;
  claimToken: string;
  secretRef: AgentObservabilitySecretReference;
  attemptCount: number;
}

export interface ReconcileAgentObservabilitySecretCleanupResult {
  claimed: number;
  deleted: number;
  failed: number;
  stale: number;
}

/** Claim pending work or an expired lease in a short database transaction. */
export async function claimAgentObservabilitySecretCleanup(
  db: DbClient,
  options: { now?: Date; batchSize?: number; leaseMs?: number } = {},
): Promise<AgentObservabilitySecretCleanupClaim[]> {
  const now = validDate(options.now ?? new Date());
  const batchSize = validBatchSize(options.batchSize ?? 100);
  const leaseMs = validPositiveDuration(
    options.leaseMs ?? AGENT_OBSERVABILITY_SECRET_CLEANUP_LEASE_MS,
  );
  const candidates = await db
    .select({ id: agentObservabilitySecretCleanupOutbox.id })
    .from(agentObservabilitySecretCleanupOutbox)
    .where(
      or(
        and(
          eq(agentObservabilitySecretCleanupOutbox.status, 'pending'),
          lte(agentObservabilitySecretCleanupOutbox.nextAttemptAt, now),
        ),
        and(
          eq(agentObservabilitySecretCleanupOutbox.status, 'deleting'),
          lte(agentObservabilitySecretCleanupOutbox.leaseExpiresAt, now),
        ),
      ),
    )
    .orderBy(asc(agentObservabilitySecretCleanupOutbox.nextAttemptAt))
    .limit(batchSize);

  const claims: AgentObservabilitySecretCleanupClaim[] = [];
  for (const candidate of candidates) {
    const claim = await db.transaction(async (tx) => {
      const row = (
        await tx
          .select()
          .from(agentObservabilitySecretCleanupOutbox)
          .where(eq(agentObservabilitySecretCleanupOutbox.id, candidate.id))
          .for('update', { skipLocked: true })
          .limit(1)
      )[0];
      if (!row) return null;
      const eligible =
        (row.status === 'pending' && row.nextAttemptAt.getTime() <= now.getTime()) ||
        (row.status === 'deleting' &&
          row.leaseExpiresAt !== null &&
          row.leaseExpiresAt.getTime() <= now.getTime());
      if (!eligible) return null;
      if (!isAgentObservabilitySecretReference(row.secretRef)) {
        throw new Error('agent observability cleanup secret reference is corrupt');
      }
      const claimToken = newId('aomclaim');
      const attemptCount = row.attemptCount + 1;
      await tx
        .update(agentObservabilitySecretCleanupOutbox)
        .set({
          status: 'deleting',
          claimToken,
          leaseExpiresAt: new Date(now.getTime() + leaseMs),
          attemptCount,
          updatedAt: now,
        })
        .where(eq(agentObservabilitySecretCleanupOutbox.id, row.id));
      return {
        id: row.id,
        claimToken,
        secretRef: row.secretRef,
        attemptCount,
      } satisfies AgentObservabilitySecretCleanupClaim;
    });
    if (claim) claims.push(claim);
  }
  return claims;
}

/** Complete only a current claim token; failures persist bounded backoff. */
export async function completeAgentObservabilitySecretCleanup(
  db: DbClient,
  input: AgentObservabilitySecretCleanupClaim & {
    outcome: 'deleted' | 'failed';
    now?: Date;
    baseBackoffMs?: number;
    maxBackoffMs?: number;
  },
): Promise<boolean> {
  const now = validDate(input.now ?? new Date());
  if (
    !requiredOpaqueId(input.id) ||
    !requiredOpaqueId(input.claimToken) ||
    !isPositiveInteger(input.attemptCount)
  ) {
    throw new AgentObservabilityMutationValidationError();
  }
  if (input.outcome === 'deleted') {
    const deleted = await db.transaction(async (tx) =>
      tx
        .delete(agentObservabilitySecretCleanupOutbox)
        .where(
          and(
            eq(agentObservabilitySecretCleanupOutbox.id, input.id),
            eq(agentObservabilitySecretCleanupOutbox.status, 'deleting'),
            eq(agentObservabilitySecretCleanupOutbox.claimToken, input.claimToken),
          ),
        )
        .returning({ id: agentObservabilitySecretCleanupOutbox.id }),
    );
    return deleted.length === 1;
  }

  const delayMs = agentObservabilitySecretCleanupBackoffMs(
    input.attemptCount,
    input.baseBackoffMs,
    input.maxBackoffMs,
  );
  const updated = await db.transaction(async (tx) =>
    tx
      .update(agentObservabilitySecretCleanupOutbox)
      .set({
        status: 'pending',
        claimToken: null,
        leaseExpiresAt: null,
        nextAttemptAt: new Date(now.getTime() + delayMs),
        updatedAt: now,
      })
      .where(
        and(
          eq(agentObservabilitySecretCleanupOutbox.id, input.id),
          eq(agentObservabilitySecretCleanupOutbox.status, 'deleting'),
          eq(agentObservabilitySecretCleanupOutbox.claimToken, input.claimToken),
        ),
      )
      .returning({ id: agentObservabilitySecretCleanupOutbox.id }),
  );
  return updated.length === 1;
}

export function agentObservabilitySecretCleanupBackoffMs(
  attemptCount: number,
  baseBackoffMs = AGENT_OBSERVABILITY_SECRET_CLEANUP_BASE_BACKOFF_MS,
  maxBackoffMs = AGENT_OBSERVABILITY_SECRET_CLEANUP_MAX_BACKOFF_MS,
): number {
  if (!isPositiveInteger(attemptCount)) throw new AgentObservabilityMutationValidationError();
  const base = validPositiveDuration(baseBackoffMs);
  const max = validPositiveDuration(maxBackoffMs);
  return Math.min(max, base * 2 ** Math.min(attemptCount - 1, 30));
}

/** Claim -> external delete -> completion token CAS. */
export async function reconcileAgentObservabilitySecretCleanup(
  db: DbClient,
  secretStore: SecretStore,
  options: {
    now?: Date;
    batchSize?: number;
    leaseMs?: number;
    baseBackoffMs?: number;
    maxBackoffMs?: number;
    /** Bound one provider delete; must remain shorter than its cleanup lease. */
    deleteTimeoutMs?: number;
    /** Stop current and unstarted deletes while retaining failed cleanup work. */
    signal?: AbortSignal;
  } = {},
): Promise<ReconcileAgentObservabilitySecretCleanupResult> {
  const now = validDate(options.now ?? new Date());
  const batchSize = validBatchSize(options.batchSize ?? 100);
  const leaseMs = validPositiveDuration(
    options.leaseMs ?? AGENT_OBSERVABILITY_SECRET_CLEANUP_LEASE_MS,
  );
  const deleteTimeoutMs = boundedCleanupDeleteTimeoutMs(
    options.deleteTimeoutMs,
    leaseMs,
    AGENT_OBSERVABILITY_SECRET_CLEANUP_DELETE_TIMEOUT_MS,
  );
  const result: ReconcileAgentObservabilitySecretCleanupResult = {
    claimed: 0,
    deleted: 0,
    failed: 0,
    stale: 0,
  };
  if (options.signal?.aborted) return result;

  const claims = await claimAgentObservabilitySecretCleanup(db, { now, batchSize, leaseMs });
  result.claimed = claims.length;
  for (const claim of claims) {
    const outcome = await awaitSecretStoreDeleteOrAbort(secretStore, claim.secretRef, {
      parentSignal: options.signal,
      timeoutMs: deleteTimeoutMs,
    });
    const completion: Parameters<typeof completeAgentObservabilitySecretCleanup>[1] = {
      ...claim,
      outcome,
      now,
    };
    if (outcome === 'failed') {
      if (options.baseBackoffMs !== undefined) completion.baseBackoffMs = options.baseBackoffMs;
      if (options.maxBackoffMs !== undefined) completion.maxBackoffMs = options.maxBackoffMs;
    }
    const completed = await completeAgentObservabilitySecretCleanup(db, completion);
    if (completed) {
      if (outcome === 'deleted') result.deleted += 1;
      else result.failed += 1;
    } else result.stale += 1;
  }
  return result;
}

function boundedCleanupDeleteTimeoutMs(
  configuredTimeoutMs: number | undefined,
  leaseMs: number,
  defaultTimeoutMs: number,
): number {
  const timeoutMs = validPositiveDuration(
    configuredTimeoutMs ?? Math.min(defaultTimeoutMs, leaseMs - 1),
  );
  if (timeoutMs >= leaseMs) throw new AgentObservabilityMutationValidationError();
  return timeoutMs;
}

/**
 * Stop awaiting one provider delete on timeout or parent cancellation. The
 * provider may ignore AbortSignal, so both late completion and rejection keep
 * handlers attached after this function returns.
 */
function awaitSecretStoreDeleteOrAbort(
  secretStore: SecretStore,
  secretRef: AgentObservabilitySecretReference,
  input: { parentSignal?: AbortSignal | undefined; timeoutMs: number },
): Promise<'deleted' | 'failed'> {
  const parentSignal = input.parentSignal;
  if (parentSignal?.aborted) return Promise.resolve('failed');

  const abortController = new AbortController();
  const abort = (reason?: unknown) => {
    if (!abortController.signal.aborted) abortController.abort(reason);
  };
  const onParentAbort = () => abort(parentSignal?.reason);
  parentSignal?.addEventListener('abort', onParentAbort, { once: true });
  const timeout = setTimeout(
    () => abort(new Error('agent observability SecretStore delete timed out')),
    input.timeoutMs,
  );
  const operation = Promise.resolve().then(() => {
    if (abortController.signal.aborted) {
      throw (
        abortController.signal.reason ?? new Error('agent observability SecretStore delete aborted')
      );
    }
    return secretStore.delete(secretRef, { signal: abortController.signal });
  });

  return new Promise<'deleted' | 'failed'>((resolve) => {
    let settled = false;
    const finish = (outcome: 'deleted' | 'failed') => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      parentSignal?.removeEventListener('abort', onParentAbort);
      abortController.signal.removeEventListener('abort', onAbort);
      resolve(outcome);
    };
    const onAbort = () => finish('failed');
    abortController.signal.addEventListener('abort', onAbort, { once: true });
    void operation.then(
      () => finish(abortController.signal.aborted ? 'failed' : 'deleted'),
      () => finish('failed'),
    );
  });
}

/**
 * Bounded expiry cleanup for idempotency rows. An abandoned expired attempt is
 * first CAS-transitioned to terminal state so it cannot retain the live-target
 * index or starve later retention candidates. Reservations deliberately stay
 * append-only: deleting a target's final row would reset its generation on a
 * later mutation and weaken its fence. Staging rows remain reconciler-owned.
 */
export async function pruneAgentObservabilityMutationRetention(
  db: DbClient,
  options: { now?: Date; batchSize?: number } = {},
): Promise<{ idempotencyDeleted: number; reservationsDeleted: number }> {
  const now = validDate(options.now ?? new Date());
  const batchSize = validBatchSize(options.batchSize ?? 100);
  const idempotencyCandidates = await db
    .select({
      organizationId: agentObservabilityIdempotencyKeys.organizationId,
      principal: agentObservabilityIdempotencyKeys.principal,
      scope: agentObservabilityIdempotencyKeys.scope,
      key: agentObservabilityIdempotencyKeys.key,
    })
    .from(agentObservabilityIdempotencyKeys)
    .where(lte(agentObservabilityIdempotencyKeys.expiresAt, now))
    .orderBy(asc(agentObservabilityIdempotencyKeys.expiresAt))
    .limit(batchSize);
  let idempotencyDeleted = 0;
  for (const candidate of idempotencyCandidates) {
    const deleted = await db.transaction(async (tx) => {
      const row = (
        await tx
          .select()
          .from(agentObservabilityIdempotencyKeys)
          .where(
            and(
              eq(agentObservabilityIdempotencyKeys.organizationId, candidate.organizationId),
              eq(agentObservabilityIdempotencyKeys.principal, candidate.principal),
              eq(agentObservabilityIdempotencyKeys.scope, candidate.scope),
              eq(agentObservabilityIdempotencyKeys.key, candidate.key),
            ),
          )
          .for('update', { skipLocked: true })
          .limit(1)
      )[0];
      if (!row || row.expiresAt.getTime() > now.getTime()) return false;
      const reservation = await lockedReservationForIdempotency(
        tx,
        row.organizationId,
        row.reservationId,
      );
      let reservationStatus = reservation.status;
      if (
        row.status === 'pending' &&
        reservationStatus === 'pending' &&
        reservation.expiresAt.getTime() <= now.getTime()
      ) {
        const expired = await expireLockedReservation(tx as DbTransaction, reservation, now);
        if (!expired) return false;
        await markStagingCleanupPendingForTerminalReservation(tx as DbTransaction, {
          reservationId: reservation.id,
          generation: reservation.generation,
          now,
        });
        reservationStatus = 'expired';
      }
      const completedTerminal = row.status === 'completed' && reservationStatus === 'committed';
      const terminalPending =
        row.status === 'pending' &&
        (reservationStatus === 'fenced' || reservationStatus === 'expired');
      if (!completedTerminal && !terminalPending) return false;
      const removed = await tx
        .delete(agentObservabilityIdempotencyKeys)
        .where(
          and(
            eq(agentObservabilityIdempotencyKeys.organizationId, row.organizationId),
            eq(agentObservabilityIdempotencyKeys.principal, row.principal),
            eq(agentObservabilityIdempotencyKeys.scope, row.scope),
            eq(agentObservabilityIdempotencyKeys.key, row.key),
            eq(agentObservabilityIdempotencyKeys.reservationId, row.reservationId),
          ),
        )
        .returning({ key: agentObservabilityIdempotencyKeys.key });
      return removed.length === 1;
    });
    if (deleted) idempotencyDeleted += 1;
  }

  return { idempotencyDeleted, reservationsDeleted: 0 };
}

class FinalizationStopped extends Error {
  constructor(
    readonly kind: Exclude<FinalizeAgentObservabilityMutationResult['kind'], 'committed'>,
  ) {
    super('agent observability mutation finalization stopped');
  }
}

async function requireRepeatableReadMutationTransaction(tx: DbTransaction): Promise<void> {
  const result = await tx.execute<{ isolationLevel: string }>(
    sql`select current_setting('transaction_isolation') as "isolationLevel"`,
  );
  const isolationLevel = result.rows[0]?.isolationLevel;
  if (isolationLevel !== 'repeatable read' && isolationLevel !== 'serializable') {
    throw new AgentObservabilityMutationValidationError(
      'agent observability mutation finalization requires repeatable-read transaction',
    );
  }
}

async function createReservationAttempt(
  tx: DbTransaction,
  input: {
    identity: AgentObservabilityAdminIdempotencyIdentity;
    target: AgentObservabilityMutationTarget;
    targetKey: string;
    bodyHash: string;
    expectedVersions: AgentObservabilityExpectedVersions;
    staging: AgentObservabilityCredentialStagingInput | undefined;
    now: Date;
    reservationTtlMs: number;
    idempotencyTtlMs: number;
    stagingCleanupGraceMs: number;
    rebind: { reservationId: string } | null;
  },
): Promise<AcquireAgentObservabilityMutationReservationResult> {
  const expiredReservations = await tx
    .update(agentObservabilityMutationReservations)
    .set({ status: 'expired', expiredAt: input.now, updatedAt: input.now })
    .where(
      and(
        eq(agentObservabilityMutationReservations.targetKey, input.targetKey),
        eq(agentObservabilityMutationReservations.status, 'pending'),
        lte(agentObservabilityMutationReservations.expiresAt, input.now),
      ),
    )
    .returning({
      id: agentObservabilityMutationReservations.id,
      generation: agentObservabilityMutationReservations.generation,
    });
  for (const reservation of expiredReservations) {
    await markStagingCleanupPendingForTerminalReservation(tx, {
      reservationId: reservation.id,
      generation: reservation.generation,
      now: input.now,
    });
  }
  const live = (
    await tx
      .select({ id: agentObservabilityMutationReservations.id })
      .from(agentObservabilityMutationReservations)
      .where(
        and(
          eq(agentObservabilityMutationReservations.targetKey, input.targetKey),
          eq(agentObservabilityMutationReservations.status, 'pending'),
        ),
      )
      .limit(1)
  )[0];
  if (live) return { kind: 'conflict', reason: 'target_pending' };

  const previous = (
    await tx
      .select({ generation: agentObservabilityMutationReservations.generation })
      .from(agentObservabilityMutationReservations)
      .where(eq(agentObservabilityMutationReservations.targetKey, input.targetKey))
      .orderBy(desc(agentObservabilityMutationReservations.generation))
      .limit(1)
  )[0];
  const generation = (previous?.generation ?? 0) + 1;
  const reservationId = newId('aomr');
  const expiresAt = new Date(input.now.getTime() + input.reservationTtlMs);
  const storageTarget = targetStorage(input.target);
  const inserted = await tx
    .insert(agentObservabilityMutationReservations)
    .values({
      id: reservationId,
      organizationId: input.target.organizationId,
      workspaceId: storageTarget.workspaceId,
      targetType: input.target.type,
      targetKey: input.targetKey,
      targetBindingId: storageTarget.bindingId,
      targetBindingScope: storageTarget.bindingScope,
      ownerPrincipal: input.identity.principal,
      bodyHash: input.bodyHash,
      expectedStateVersion: input.expectedVersions.stateVersion,
      expectedConfigVersion: input.expectedVersions.configVersion,
      expectedCredentialVersion: input.expectedVersions.credentialVersion,
      generation,
      status: 'pending',
      expiresAt,
      fencedAt: null,
      expiredAt: null,
      committedAt: null,
      createdAt: input.now,
      updatedAt: input.now,
    })
    .onConflictDoNothing()
    .returning({ id: agentObservabilityMutationReservations.id });
  if (!inserted[0]) return { kind: 'conflict', reason: 'target_pending' };

  const idempotencyExpiresAt = new Date(input.now.getTime() + input.idempotencyTtlMs);
  if (input.rebind) {
    const rebound = await tx
      .update(agentObservabilityIdempotencyKeys)
      .set({
        targetKey: input.targetKey,
        bodyHash: input.bodyHash,
        status: 'pending',
        reservationId,
        responseStatus: null,
        responseBody: null,
        completedAt: null,
        expiresAt: idempotencyExpiresAt,
        updatedAt: input.now,
      })
      .where(
        and(
          idempotencyWhere(input.identity),
          eq(agentObservabilityIdempotencyKeys.reservationId, input.rebind.reservationId),
        ),
      )
      .returning({ reservationId: agentObservabilityIdempotencyKeys.reservationId });
    if (rebound.length !== 1) throw new Error('agent observability idempotency rebind lost');
  } else {
    await tx.insert(agentObservabilityIdempotencyKeys).values({
      organizationId: input.identity.organizationId,
      principal: input.identity.principal,
      scope: input.identity.scope,
      key: input.identity.key,
      targetKey: input.targetKey,
      bodyHash: input.bodyHash,
      status: 'pending',
      reservationId,
      responseStatus: null,
      responseBody: null,
      completedAt: null,
      expiresAt: idempotencyExpiresAt,
      createdAt: input.now,
      updatedAt: input.now,
    });
  }
  if (input.staging) {
    const nextCleanupAt = new Date(expiresAt.getTime() + input.stagingCleanupGraceMs);
    await tx.insert(agentObservabilityCredentialStagingIntents).values({
      reservationId,
      generation,
      candidateBindingId: input.staging.candidateBindingId,
      proposedCredentialVersion: input.staging.proposedCredentialVersion,
      secretRef: input.staging.secretRef,
      status: 'pending',
      writerToken: null,
      writerLeaseExpiresAt: null,
      putCompletedAt: null,
      cleanupToken: null,
      cleanupLeaseExpiresAt: null,
      nextCleanupAt,
      createdAt: input.now,
      updatedAt: input.now,
    });
  }
  return {
    kind: 'acquired',
    reservation: {
      organizationId: input.target.organizationId,
      reservationId,
      ownerPrincipal: input.identity.principal,
      generation,
      target: input.target,
      expectedVersions: input.expectedVersions,
      expiresAt,
    },
  };
}

async function lockedReservationForIdempotency(
  tx: DbTransaction,
  organizationId: string,
  reservationId: string,
) {
  const row = (
    await tx
      .select()
      .from(agentObservabilityMutationReservations)
      .where(
        and(
          eq(agentObservabilityMutationReservations.organizationId, organizationId),
          eq(agentObservabilityMutationReservations.id, reservationId),
        ),
      )
      .for('update')
      .limit(1)
  )[0];
  if (!row) {
    throw new AgentObservabilityMutationValidationError(
      'agent observability idempotency reservation is corrupt',
    );
  }
  return row;
}

async function lockReservationAndStaging(
  tx: DbTransaction,
  reference: AgentObservabilityReservationReference,
): Promise<
  | {
      reservation: typeof agentObservabilityMutationReservations.$inferSelect;
      staging: typeof agentObservabilityCredentialStagingIntents.$inferSelect;
    }
  | undefined
> {
  const reservation = (
    await tx
      .select()
      .from(agentObservabilityMutationReservations)
      .where(reservationReferenceWhere(reference))
      .for('update')
      .limit(1)
  )[0];
  if (!reservation) return undefined;
  const staging = (
    await tx
      .select()
      .from(agentObservabilityCredentialStagingIntents)
      .where(stagingReferenceWhere(reservation.id, reservation.generation))
      .for('update')
      .limit(1)
  )[0];
  if (!staging) throw new AgentObservabilityMutationValidationError();
  return { reservation, staging };
}

/** Validate durable rows before deriving a terminal writer outcome from them. */
function assertStagingWriteRows(
  reference: AgentObservabilityReservationReference,
  locked: {
    reservation: typeof agentObservabilityMutationReservations.$inferSelect;
    staging: typeof agentObservabilityCredentialStagingIntents.$inferSelect;
  },
): void {
  const { reservation, staging } = locked;
  if (
    reservation.id !== reference.reservationId ||
    reservation.organizationId !== reference.organizationId ||
    reservation.ownerPrincipal !== reference.ownerPrincipal ||
    reservation.generation !== reference.generation ||
    staging.reservationId !== reservation.id ||
    staging.generation !== reservation.generation ||
    !isAgentObservabilitySecretReference(staging.secretRef) ||
    !isPositiveInteger(staging.proposedCredentialVersion)
  ) {
    throw new AgentObservabilityMutationValidationError();
  }
  requiredIdentifier(staging.candidateBindingId);
  const createdAt = validDate(reservation.createdAt);
  const expiresAt = validDate(reservation.expiresAt);
  validDate(reservation.updatedAt);
  if (expiresAt.getTime() <= createdAt.getTime()) {
    throw new AgentObservabilityMutationValidationError();
  }
  const requireNull = (value: unknown) => {
    if (value !== null) throw new AgentObservabilityMutationValidationError();
  };
  const requireDate = (value: unknown) => validDate(value);
  const requireNullableDate = (value: unknown) => {
    if (value !== null) validDate(value);
  };
  switch (reservation.status) {
    case 'pending':
      requireNull(reservation.fencedAt);
      requireNull(reservation.expiredAt);
      requireNull(reservation.committedAt);
      break;
    case 'fenced':
      requireDate(reservation.fencedAt);
      requireNull(reservation.expiredAt);
      requireNull(reservation.committedAt);
      break;
    case 'expired':
      requireNull(reservation.fencedAt);
      requireDate(reservation.expiredAt);
      requireNull(reservation.committedAt);
      break;
    case 'committed':
      requireNull(reservation.fencedAt);
      requireNull(reservation.expiredAt);
      requireDate(reservation.committedAt);
      break;
    default:
      throw new AgentObservabilityMutationValidationError();
  }

  const stagingCreatedAt = validDate(staging.createdAt);
  if (validDate(staging.nextCleanupAt).getTime() < stagingCreatedAt.getTime()) {
    throw new AgentObservabilityMutationValidationError();
  }
  validDate(staging.updatedAt);
  const requireNoWriter = () => {
    requireNull(staging.writerToken);
    requireNull(staging.writerLeaseExpiresAt);
  };
  const requireWriter = () => {
    requiredOpaqueId(staging.writerToken);
    requireDate(staging.writerLeaseExpiresAt);
  };
  const requireNoCleanup = () => {
    requireNull(staging.cleanupToken);
    requireNull(staging.cleanupLeaseExpiresAt);
  };
  const requireCleanup = () => {
    requiredOpaqueId(staging.cleanupToken);
    requireDate(staging.cleanupLeaseExpiresAt);
  };
  switch (staging.status) {
    case 'pending':
      requireNoWriter();
      requireNull(staging.putCompletedAt);
      requireNoCleanup();
      break;
    case 'writing':
      requireWriter();
      requireNull(staging.putCompletedAt);
      requireNoCleanup();
      break;
    case 'written':
      requireNoWriter();
      requireDate(staging.putCompletedAt);
      requireNoCleanup();
      break;
    case 'cleanup_pending':
      requireNoWriter();
      requireNullableDate(staging.putCompletedAt);
      requireNoCleanup();
      break;
    case 'cleaning':
      requireNoWriter();
      requireNullableDate(staging.putCompletedAt);
      requireCleanup();
      break;
    default:
      throw new AgentObservabilityMutationValidationError();
  }
  if (
    (reservation.status === 'pending' &&
      (staging.status === 'cleanup_pending' || staging.status === 'cleaning')) ||
    (reservation.status === 'committed' && staging.status !== 'written')
  ) {
    throw new AgentObservabilityMutationValidationError();
  }
}

function stagingReferenceWhere(reservationId: string, generation: number) {
  return and(
    eq(agentObservabilityCredentialStagingIntents.reservationId, reservationId),
    eq(agentObservabilityCredentialStagingIntents.generation, generation),
  );
}

function reservationReference(
  reservation: typeof agentObservabilityMutationReservations.$inferSelect,
): AgentObservabilityReservationReference {
  return {
    organizationId: reservation.organizationId,
    reservationId: reservation.id,
    ownerPrincipal: reservation.ownerPrincipal,
    generation: reservation.generation,
  };
}

function reservationIsLive(
  reservation: typeof agentObservabilityMutationReservations.$inferSelect,
  now: Date,
): boolean {
  return reservation.status === 'pending' && reservation.expiresAt.getTime() > now.getTime();
}

/** Validate terminal shape before treating a durable row as a normal loser. */
function inspectReservationSettlementState(
  reservation: typeof agentObservabilityMutationReservations.$inferSelect,
): 'pending' | 'fenced' | 'expired' | 'committed' | 'invalid' {
  if (
    !hasValidReservationSettlementMetadata(reservation) ||
    !isValidStoredDate(reservation.createdAt) ||
    !isValidStoredDate(reservation.updatedAt) ||
    !isValidStoredDate(reservation.expiresAt) ||
    reservation.expiresAt.getTime() <= reservation.createdAt.getTime()
  ) {
    return 'invalid';
  }
  switch (reservation.status) {
    case 'pending':
      return reservation.fencedAt === null &&
        reservation.expiredAt === null &&
        reservation.committedAt === null
        ? 'pending'
        : 'invalid';
    case 'fenced':
      return isValidStoredDate(reservation.fencedAt) &&
        reservation.expiredAt === null &&
        reservation.committedAt === null
        ? 'fenced'
        : 'invalid';
    case 'expired':
      return reservation.fencedAt === null &&
        isValidStoredDate(reservation.expiredAt) &&
        reservation.committedAt === null
        ? 'expired'
        : 'invalid';
    case 'committed':
      return reservation.fencedAt === null &&
        reservation.expiredAt === null &&
        isValidStoredDate(reservation.committedAt)
        ? 'committed'
        : 'invalid';
    default:
      return 'invalid';
  }
}

function hasValidReservationSettlementMetadata(
  reservation: typeof agentObservabilityMutationReservations.$inferSelect,
): boolean {
  try {
    requiredOpaqueId(reservation.id);
    requiredIdentifier(reservation.organizationId);
    requiredString(reservation.ownerPrincipal, IDENTIFIER_MAX_LENGTH);
    positiveInteger(reservation.generation);
    requiredBodyHash(reservation.bodyHash);
    normalizeExpectedVersions({
      stateVersion: reservation.expectedStateVersion,
      configVersion: reservation.expectedConfigVersion,
      credentialVersion: reservation.expectedCredentialVersion,
    });
    const target = targetFromReservation(reservation);
    return reservation.targetKey === agentObservabilityMutationTargetKey(target);
  } catch {
    return false;
  }
}

function hasValidPendingReservationIdempotency(
  reservation: typeof agentObservabilityMutationReservations.$inferSelect,
  idempotency: typeof agentObservabilityIdempotencyKeys.$inferSelect,
): boolean {
  return (
    hasValidReservationIdempotencyLink(reservation, idempotency) &&
    idempotency.status === 'pending' &&
    idempotency.responseStatus === null &&
    idempotency.responseBody === null &&
    idempotency.completedAt === null
  );
}

function hasValidCommittedReservationIdempotency(
  reservation: typeof agentObservabilityMutationReservations.$inferSelect,
  idempotency: typeof agentObservabilityIdempotencyKeys.$inferSelect,
): boolean {
  return (
    hasValidReservationIdempotencyLink(reservation, idempotency) &&
    idempotency.status === 'completed' &&
    idempotency.responseStatus !== null &&
    Number.isSafeInteger(idempotency.responseStatus) &&
    idempotency.responseStatus >= 200 &&
    idempotency.responseStatus <= 299 &&
    idempotency.responseBody !== null &&
    isValidStoredDate(idempotency.completedAt) &&
    idempotency.completedAt.getTime() >= idempotency.createdAt.getTime()
  );
}

function hasValidReservationIdempotencyLink(
  reservation: typeof agentObservabilityMutationReservations.$inferSelect,
  idempotency: typeof agentObservabilityIdempotencyKeys.$inferSelect,
): boolean {
  try {
    normalizeIdempotencyIdentity({
      organizationId: idempotency.organizationId,
      principal: idempotency.principal,
      scope: idempotency.scope,
      key: idempotency.key,
    });
    if (
      !isValidStoredDate(idempotency.createdAt) ||
      !isValidStoredDate(idempotency.updatedAt) ||
      !isValidStoredDate(idempotency.expiresAt) ||
      idempotency.expiresAt.getTime() <= idempotency.createdAt.getTime()
    ) {
      return false;
    }
    return (
      idempotency.organizationId === reservation.organizationId &&
      idempotency.principal === reservation.ownerPrincipal &&
      idempotency.reservationId === reservation.id &&
      idempotency.bodyHash === reservation.bodyHash &&
      idempotency.targetKey === reservation.targetKey
    );
  } catch {
    return false;
  }
}

function isValidStoredDate(value: unknown): value is Date {
  return value instanceof Date && Number.isFinite(value.getTime());
}

async function expireLockedReservation(
  tx: DbTransaction,
  reservation: typeof agentObservabilityMutationReservations.$inferSelect,
  now: Date,
): Promise<boolean> {
  const expired = await tx
    .update(agentObservabilityMutationReservations)
    .set({ status: 'expired', expiredAt: now, updatedAt: now })
    .where(
      and(
        eq(agentObservabilityMutationReservations.id, reservation.id),
        eq(agentObservabilityMutationReservations.ownerPrincipal, reservation.ownerPrincipal),
        eq(agentObservabilityMutationReservations.generation, reservation.generation),
        eq(agentObservabilityMutationReservations.status, 'pending'),
        lte(agentObservabilityMutationReservations.expiresAt, now),
      ),
    )
    .returning({ id: agentObservabilityMutationReservations.id });
  return expired.length === 1;
}

/** Transition only writer-free rows; a live writer keeps its durable lease. */
async function markStagingCleanupPendingForTerminalReservation(
  tx: DbTransaction,
  input: { reservationId: string; generation: number; now: Date },
): Promise<void> {
  await tx
    .update(agentObservabilityCredentialStagingIntents)
    .set({
      status: 'cleanup_pending',
      cleanupToken: null,
      cleanupLeaseExpiresAt: null,
      updatedAt: input.now,
    })
    .where(
      and(
        stagingReferenceWhere(input.reservationId, input.generation),
        or(
          eq(agentObservabilityCredentialStagingIntents.status, 'pending'),
          eq(agentObservabilityCredentialStagingIntents.status, 'written'),
        ),
      ),
    );
}

async function ensureReservationLiveOrExpired(
  tx: DbTransaction,
  reservation: typeof agentObservabilityMutationReservations.$inferSelect,
  now: Date,
): Promise<boolean> {
  if (reservationIsLive(reservation, now)) return true;
  if (reservation.status === 'pending' && (await expireLockedReservation(tx, reservation, now))) {
    await markStagingCleanupPendingForTerminalReservation(tx, {
      reservationId: reservation.id,
      generation: reservation.generation,
      now,
    });
  }
  return false;
}

async function tombstoneStagingWriter(
  tx: DbTransaction,
  staging: typeof agentObservabilityCredentialStagingIntents.$inferSelect,
  now: Date,
): Promise<void> {
  if (staging.writerToken === null) return;
  await tx
    .update(agentObservabilityCredentialStagingIntents)
    .set({
      status: 'cleanup_pending',
      writerToken: null,
      writerLeaseExpiresAt: null,
      cleanupToken: null,
      cleanupLeaseExpiresAt: null,
      updatedAt: now,
    })
    .where(
      and(
        stagingReferenceWhere(staging.reservationId, staging.generation),
        eq(agentObservabilityCredentialStagingIntents.status, 'writing'),
        eq(agentObservabilityCredentialStagingIntents.writerToken, staging.writerToken),
      ),
    );
}

/**
 * Return promptly when cancellation fires while retaining handlers on the
 * provider promise. A provider that ignores AbortSignal can still finish late;
 * its abandoned writer token has already been converted to a tombstone.
 */
function awaitStagingPutOrAbort(
  operation: Promise<void>,
  signal: AbortSignal,
): Promise<'written' | 'failed' | 'abandoned'> {
  if (signal.aborted) return Promise.resolve('abandoned');
  return new Promise((resolve) => {
    let settled = false;
    const finish = (outcome: 'written' | 'failed' | 'abandoned') => {
      if (settled) return;
      settled = true;
      signal.removeEventListener('abort', onAbort);
      resolve(outcome);
    };
    const onAbort = () => finish('abandoned');
    signal.addEventListener('abort', onAbort, { once: true });
    operation.then(
      () => finish(signal.aborted ? 'abandoned' : 'written'),
      () => finish(signal.aborted ? 'abandoned' : 'failed'),
    );
  });
}

function reservationInProgress(
  reservation: typeof agentObservabilityMutationReservations.$inferSelect,
): AgentObservabilityReservationReference & { status: 'pending'; expiresAt: Date } {
  return {
    organizationId: reservation.organizationId,
    reservationId: reservation.id,
    ownerPrincipal: reservation.ownerPrincipal,
    generation: reservation.generation,
    status: 'pending',
    expiresAt: reservation.expiresAt,
  };
}

function activationFromStaging(
  staging: typeof agentObservabilityCredentialStagingIntents.$inferSelect,
): AgentObservabilityStagedActivation {
  if (!isAgentObservabilitySecretReference(staging.secretRef)) {
    throw new FinalizationStopped('staging_not_written');
  }
  return {
    candidateBindingId: staging.candidateBindingId,
    credentialVersion: staging.proposedCredentialVersion,
    secretRef: staging.secretRef,
  };
}

async function verifySupersededRotationHead(
  db: DbTransaction,
  target: AgentObservabilityMutationTarget,
  expectedVersions: AgentObservabilityExpectedVersions,
  activation: AgentObservabilityStagedActivation | null,
  supersededSecretRef: AgentObservabilitySecretReference,
): Promise<void> {
  const bindingTarget = bindingTargetForRotation(target);
  if (
    activation === null ||
    activation.candidateBindingId !== bindingTarget.bindingId ||
    expectedVersions.credentialVersion === null
  ) {
    throw new FinalizationStopped('invalid_superseded_secret');
  }
  const oldHead = (
    await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, bindingTarget.bindingId))
      .for('update')
      .limit(1)
  )[0];
  if (
    !oldHead ||
    oldHead.credentialVersion !== expectedVersions.credentialVersion ||
    oldHead.secretRef !== supersededSecretRef ||
    activation.secretRef === supersededSecretRef
  ) {
    throw new FinalizationStopped('invalid_superseded_secret');
  }
}

/**
 * Prove credential generation before apply, not only after the callback has
 * written a matching head. Existing binding mutations are strict rotations;
 * setting targets create an initial candidate head only.
 */
async function verifyStagedCredentialGenerationBeforeApply(
  db: DbTransaction,
  target: AgentObservabilityMutationTarget,
  expectedVersions: AgentObservabilityExpectedVersions,
  activation: AgentObservabilityStagedActivation | null,
  supersededSecretRef: AgentObservabilitySecretReference | undefined,
): Promise<void> {
  if (activation === null) return;
  if (target.type === 'binding') {
    if (
      activation.candidateBindingId !== target.bindingId ||
      expectedVersions.credentialVersion === null ||
      expectedVersions.credentialVersion >= AGENT_OBSERVABILITY_CREDENTIAL_VERSION_MAX ||
      activation.credentialVersion !== expectedVersions.credentialVersion + 1 ||
      supersededSecretRef === undefined
    ) {
      throw new FinalizationStopped('invalid_credential_generation');
    }
    return;
  }
  if (activation.credentialVersion !== 1) {
    throw new FinalizationStopped('invalid_credential_generation');
  }
  const existingCandidate = (
    await db
      .select({ id: agentObservabilityBindings.id })
      .from(agentObservabilityBindings)
      .where(eq(agentObservabilityBindings.id, activation.candidateBindingId))
      .for('update')
      .limit(1)
  )[0];
  if (existingCandidate) throw new FinalizationStopped('invalid_credential_generation');
}

async function hasExactStagedCredentialHead(
  db: DbTransaction,
  target: AgentObservabilityMutationTarget,
  activation: AgentObservabilityStagedActivation,
): Promise<boolean> {
  if (target.type === 'binding' && activation.candidateBindingId !== target.bindingId) {
    return false;
  }
  const binding = (
    await db
      .select()
      .from(agentObservabilityBindings)
      .where(bindingOwnershipWhere(target, activation.candidateBindingId))
      .for('update')
      .limit(1)
  )[0];
  if (!binding) return false;
  const credential = (
    await db
      .select()
      .from(agentObservabilityBindingCredentials)
      .where(eq(agentObservabilityBindingCredentials.bindingId, activation.candidateBindingId))
      .for('update')
      .limit(1)
  )[0];
  return (
    credential !== undefined &&
    credential.secretRef === activation.secretRef &&
    credential.credentialVersion === activation.credentialVersion
  );
}

/** Resolve cache body from mutation-visible Registry state, never request JSON. */
async function loadAuthoritativeMutationResponse(
  db: DbTransaction,
  target: AgentObservabilityMutationTarget,
  stagedActivation: AgentObservabilityStagedActivation | null,
): Promise<AgentObservabilityMutationResponse> {
  try {
    const response =
      expectedResponseScope(target) === 'organization'
        ? (
            await loadOrganizationAgentObservabilityStateInTransaction({
              db,
              organizationId: target.organizationId,
            })
          ).response
        : (
            await loadWorkspaceAgentObservabilityStateInTransaction({
              db,
              organizationId: target.organizationId,
              workspaceId: targetWorkspaceId(target)!,
            })
          ).response;
    const parsed = parseAgentObservabilityMutationResponse(target, response);
    if (!authoritativeResponseMatchesStagedActivation(parsed, target, stagedActivation)) {
      throw new FinalizationStopped('authoritative_state_mismatch');
    }
    return parsed;
  } catch (error) {
    if (error instanceof FinalizationStopped) throw error;
    if (
      error instanceof AgentObservabilityStateAvailabilityError ||
      error instanceof AgentObservabilityStateNotFoundError ||
      error instanceof AgentObservabilityMutationValidationError
    ) {
      throw new FinalizationStopped('authoritative_state_mismatch');
    }
    throw error;
  }
}

function authoritativeResponseMatchesStagedActivation(
  response: AgentObservabilityMutationResponse,
  target: AgentObservabilityMutationTarget,
  stagedActivation: AgentObservabilityStagedActivation | null,
): boolean {
  if (stagedActivation === null) return true;
  if (target.type === 'organization_setting') {
    return (
      response.scope === 'organization' &&
      response.configured.default_binding?.id === stagedActivation.candidateBindingId
    );
  }
  if (target.type === 'workspace_setting') {
    return (
      response.scope === 'workspace' &&
      response.configured.mode === 'custom' &&
      response.configured.binding?.id === stagedActivation.candidateBindingId
    );
  }
  const configured =
    response.scope === 'organization'
      ? response.configured.default_binding
      : response.configured.binding;
  return (
    stagedActivation.candidateBindingId === target.bindingId &&
    (configured?.id === target.bindingId || response.effective.binding?.id === target.bindingId)
  );
}

function bindingOwnershipWhere(target: AgentObservabilityMutationTarget, bindingId: string) {
  const common = [
    eq(agentObservabilityBindings.id, bindingId),
    eq(agentObservabilityBindings.organizationId, target.organizationId),
  ];
  if (target.type === 'organization_setting') {
    return and(
      ...common,
      eq(agentObservabilityBindings.scopeType, 'organization'),
      isNull(agentObservabilityBindings.workspaceId),
    );
  }
  if (target.type === 'workspace_setting') {
    return and(
      ...common,
      eq(agentObservabilityBindings.scopeType, 'workspace'),
      eq(agentObservabilityBindings.workspaceId, target.workspaceId),
    );
  }
  if (target.bindingScope === 'organization') {
    return and(
      ...common,
      eq(agentObservabilityBindings.scopeType, 'organization'),
      isNull(agentObservabilityBindings.workspaceId),
    );
  }
  return and(
    ...common,
    eq(agentObservabilityBindings.scopeType, 'workspace'),
    eq(agentObservabilityBindings.workspaceId, target.workspaceId),
  );
}

function bindingTargetForRotation(
  target: AgentObservabilityMutationTarget,
): Extract<AgentObservabilityMutationTarget, { type: 'binding' }> {
  if (target.type !== 'binding') throw new FinalizationStopped('invalid_superseded_secret');
  return target;
}

function expectedResponseScope(
  target: AgentObservabilityMutationTarget,
): 'organization' | 'workspace' {
  if (target.type === 'organization_setting') return 'organization';
  if (target.type === 'workspace_setting') return 'workspace';
  return target.bindingScope;
}

function targetStorage(target: AgentObservabilityMutationTarget): {
  workspaceId: string | null;
  bindingId: string | null;
  bindingScope: 'organization' | 'workspace' | null;
} {
  if (target.type === 'organization_setting') {
    return { workspaceId: null, bindingId: null, bindingScope: null };
  }
  if (target.type === 'workspace_setting') {
    return { workspaceId: target.workspaceId, bindingId: null, bindingScope: null };
  }
  return {
    workspaceId: target.bindingScope === 'workspace' ? target.workspaceId : null,
    bindingId: target.bindingId,
    bindingScope: target.bindingScope,
  };
}

function targetFromReservation(
  row: Pick<
    typeof agentObservabilityMutationReservations.$inferSelect,
    'organizationId' | 'targetBindingId' | 'targetBindingScope' | 'targetType' | 'workspaceId'
  >,
): AgentObservabilityMutationTarget {
  if (row.targetType === 'organization_setting') {
    return parseAgentObservabilityMutationTarget({
      type: 'organization_setting',
      organizationId: row.organizationId,
    });
  }
  if (row.targetType === 'workspace_setting' && row.workspaceId !== null) {
    return parseAgentObservabilityMutationTarget({
      type: 'workspace_setting',
      organizationId: row.organizationId,
      workspaceId: row.workspaceId,
    });
  }
  if (
    row.targetType === 'binding' &&
    row.targetBindingId !== null &&
    row.targetBindingScope === 'organization' &&
    row.workspaceId === null
  ) {
    return parseAgentObservabilityMutationTarget({
      type: 'binding',
      organizationId: row.organizationId,
      bindingId: row.targetBindingId,
      bindingScope: 'organization',
    });
  }
  if (
    row.targetType === 'binding' &&
    row.targetBindingId !== null &&
    row.targetBindingScope === 'workspace' &&
    row.workspaceId !== null
  ) {
    return parseAgentObservabilityMutationTarget({
      type: 'binding',
      organizationId: row.organizationId,
      bindingId: row.targetBindingId,
      bindingScope: 'workspace',
      workspaceId: row.workspaceId,
    });
  }
  throw new AgentObservabilityMutationValidationError(
    'agent observability reservation target is corrupt',
  );
}

function targetWorkspaceId(target: AgentObservabilityMutationTarget): string | null {
  if (target.type === 'workspace_setting') return target.workspaceId;
  if (target.type === 'binding' && target.bindingScope === 'workspace') return target.workspaceId;
  return null;
}

function targetId(target: AgentObservabilityMutationTarget): string {
  if (target.type === 'organization_setting') return target.organizationId;
  if (target.type === 'workspace_setting') return target.workspaceId;
  return target.bindingId;
}

function idempotencyWhere(identity: AgentObservabilityAdminIdempotencyIdentity) {
  return and(
    eq(agentObservabilityIdempotencyKeys.organizationId, identity.organizationId),
    eq(agentObservabilityIdempotencyKeys.principal, identity.principal),
    eq(agentObservabilityIdempotencyKeys.scope, identity.scope),
    eq(agentObservabilityIdempotencyKeys.key, identity.key),
  );
}

function reservationReferenceWhere(reference: AgentObservabilityReservationReference) {
  return and(
    eq(agentObservabilityMutationReservations.id, reference.reservationId),
    eq(agentObservabilityMutationReservations.organizationId, reference.organizationId),
    eq(agentObservabilityMutationReservations.ownerPrincipal, reference.ownerPrincipal),
    eq(agentObservabilityMutationReservations.generation, reference.generation),
  );
}

function reservationCasWhere(reference: AgentObservabilityReservationReference, status: 'pending') {
  return and(
    reservationReferenceWhere(reference),
    eq(agentObservabilityMutationReservations.status, status),
  );
}

function normalizeIdempotencyIdentity(
  identity: AgentObservabilityAdminIdempotencyIdentity,
): AgentObservabilityAdminIdempotencyIdentity {
  if (!isPlainObject(identity)) throw new AgentObservabilityMutationValidationError();
  return {
    organizationId: requiredIdentifier(identity.organizationId),
    principal: requiredString(identity.principal, IDENTIFIER_MAX_LENGTH),
    scope: requiredString(identity.scope, IDEMPOTENCY_SCOPE_MAX_LENGTH),
    key: requiredString(identity.key, IDEMPOTENCY_KEY_MAX_LENGTH),
  };
}

function normalizeExpectedVersions(
  versions: AgentObservabilityExpectedVersions,
): AgentObservabilityExpectedVersions {
  if (!isPlainObject(versions)) throw new AgentObservabilityMutationValidationError();
  return {
    stateVersion: requiredString(versions.stateVersion, STATE_VERSION_MAX_LENGTH),
    configVersion: nullablePositiveInteger(versions.configVersion),
    credentialVersion: nullablePositiveInteger(versions.credentialVersion),
  };
}

function expectedVersionsFromReservation(
  reservation: Pick<
    typeof agentObservabilityMutationReservations.$inferSelect,
    'expectedConfigVersion' | 'expectedCredentialVersion' | 'expectedStateVersion'
  >,
): AgentObservabilityExpectedVersions {
  return normalizeExpectedVersions({
    stateVersion: reservation.expectedStateVersion,
    configVersion: reservation.expectedConfigVersion,
    credentialVersion: reservation.expectedCredentialVersion,
  });
}

function normalizeStaging(
  staging: AgentObservabilityCredentialStagingInput | undefined,
): AgentObservabilityCredentialStagingInput | undefined {
  if (staging === undefined) return undefined;
  if (!isPlainObject(staging) || !isAgentObservabilitySecretReference(staging.secretRef)) {
    throw new AgentObservabilityMutationValidationError();
  }
  return {
    secretRef: staging.secretRef,
    candidateBindingId: requiredIdentifier(staging.candidateBindingId),
    proposedCredentialVersion: positiveInteger(staging.proposedCredentialVersion),
  };
}

function normalizeReservationReference(
  value: AgentObservabilityReservationReference,
): AgentObservabilityReservationReference {
  if (!isPlainObject(value)) throw new AgentObservabilityMutationValidationError();
  return {
    organizationId: requiredIdentifier(value.organizationId),
    reservationId: requiredOpaqueId(value.reservationId),
    ownerPrincipal: requiredString(value.ownerPrincipal, IDENTIFIER_MAX_LENGTH),
    generation: positiveInteger(value.generation),
  };
}

function normalizeAudit(value: FinalizeAgentObservabilityMutationInput['audit']): {
  action: string;
  authMethod: string;
  requestId: string;
} {
  if (!isPlainObject(value)) throw new AgentObservabilityMutationValidationError();
  return {
    action: requiredAuditField(value.action),
    authMethod: requiredAuditField(value.authMethod),
    requestId: requiredAuditField(value.requestId),
  };
}

function storedExpectedVersionsEqual(
  row: Pick<
    typeof agentObservabilityMutationReservations.$inferSelect,
    'expectedConfigVersion' | 'expectedCredentialVersion' | 'expectedStateVersion'
  >,
  expected: AgentObservabilityExpectedVersions,
): boolean {
  return (
    row.expectedStateVersion === expected.stateVersion &&
    row.expectedConfigVersion === expected.configVersion &&
    row.expectedCredentialVersion === expected.credentialVersion
  );
}

function safeResponseStatus(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 200 || value > 299) {
    throw new AgentObservabilityMutationValidationError();
  }
  return value;
}

function requiredIdentifier(value: unknown): string {
  return requiredString(value, IDENTIFIER_MAX_LENGTH);
}

function requiredOpaqueId(value: unknown): string {
  return requiredString(value, IDENTIFIER_MAX_LENGTH);
}

function requiredString(value: unknown, maxLength: number): string {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > maxLength ||
    value !== value.trim() ||
    hasAgentObservabilityControlCharacter(value)
  ) {
    throw new AgentObservabilityMutationValidationError();
  }
  return value;
}

function requiredAuditField(value: unknown): string {
  if (typeof value !== 'string' || !SAFE_AUDIT_FIELD_PATTERN.test(value)) {
    throw new AgentObservabilityMutationValidationError();
  }
  return value;
}

function requiredBodyHash(value: unknown): string {
  if (typeof value !== 'string' || !BODY_HASH_PATTERN.test(value)) {
    throw new AgentObservabilityMutationValidationError();
  }
  return value;
}

function nullablePositiveInteger(value: unknown): number | null {
  if (value === null) return null;
  return positiveInteger(value);
}

function positiveInteger(value: unknown): number {
  if (!isPositiveInteger(value)) throw new AgentObservabilityMutationValidationError();
  return value;
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}

function validPositiveDuration(value: unknown): number {
  if (
    typeof value !== 'number' ||
    !Number.isSafeInteger(value) ||
    value <= 0 ||
    value > 30 * 24 * 60 * 60 * 1_000
  ) {
    throw new AgentObservabilityMutationValidationError();
  }
  return value;
}

function validBatchSize(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0 || value > 1_000) {
    throw new AgentObservabilityMutationValidationError();
  }
  return value;
}

function validDate(value: unknown): Date {
  if (!(value instanceof Date) || !Number.isFinite(value.getTime())) {
    throw new AgentObservabilityMutationValidationError();
  }
  return value;
}

function requireExactKeys(value: Record<string, unknown>, expected: readonly string[]): void {
  const keys = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  if (
    keys.length !== sortedExpected.length ||
    keys.some((key, index) => key !== sortedExpected[index])
  ) {
    throw new AgentObservabilityMutationValidationError();
  }
}

function isSerializationFailure(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    (error as { code?: unknown }).code === '40001'
  );
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
