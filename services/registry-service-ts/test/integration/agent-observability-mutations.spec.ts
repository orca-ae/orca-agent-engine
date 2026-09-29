// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { and, eq, sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  AGENT_OBSERVABILITY_SECRET_CLEANUP_LEASE_MS,
  AgentObservabilityMutationValidationError,
  acquireAgentObservabilityMutationReservation,
  agentObservabilityMutationBodyHash,
  agentObservabilityMutationTargetKey,
  claimAgentObservabilitySecretCleanup,
  claimAgentObservabilityStagingWrite,
  completeAgentObservabilitySecretCleanup,
  completeAgentObservabilityStagingWrite,
  fenceAgentObservabilityMutationReservation,
  finalizeAgentObservabilityMutation,
  heartbeatAgentObservabilityStagingWrite,
  pruneAgentObservabilityMutationRetention,
  reconcileAgentObservabilitySecretCleanup,
  reconcileAgentObservabilityStagingIntents,
  settleAgentObservabilityMutationReservation,
  writeAgentObservabilityStagingIntent,
  type AcquireAgentObservabilityMutationReservationInput,
  type AgentObservabilityExpectedVersions,
  type AgentObservabilityMutationApplyInput,
  type AgentObservabilityMutationTarget,
  type AgentObservabilityReservedMutation,
} from '../../src/domain/agent-observability-mutations.js';
import {
  newOrganizationObservabilitySettings,
  newWorkspaceObservabilitySettings,
} from '../../src/domain/agent-observability-settings.js';
import {
  encodeAgentObservabilitySecretBundle,
  newAgentObservabilitySecretReference,
  type AgentObservabilitySecretReference,
} from '../../src/domain/agent-observability-secrets.js';
import { loadOrganizationAgentObservabilityStateInTransaction } from '../../src/domain/agent-observability-state.js';
import { newId } from '../../src/domain/versioning.js';
import type { DbClient, DbTransaction } from '../../src/persistence/postgres/client.js';
import * as schema from '../../src/persistence/postgres/schema.js';
import { LocalSecretStore } from '../../src/secrets/local-store.js';

const REGISTRY_DATABASE_URL =
  process.env['DATABASE_URL'] ?? 'postgres://orca:orca@localhost:5432/registry';
const ADMIN_DATABASE_URL = (() => {
  const url = new URL(REGISTRY_DATABASE_URL);
  url.pathname = '/postgres';
  return url.toString();
})();
const MIGRATIONS_FOLDER = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../src/persistence/postgres/migrations',
);

describe('agent observability mutation persistence kernel (fresh PostgreSQL)', () => {
  let pool: Pool | undefined;
  let adminPool: Pool | undefined;
  let db: DbClient | undefined;
  const suffix = `${process.pid}_${randomBytes(4).toString('hex')}`;
  const databaseName = `registry_agent_observability_mutations_${suffix}`;
  const organizationA = `org_obs_mutation_a_${suffix}`;
  const organizationB = `org_obs_mutation_b_${suffix}`;
  const workspaceA = `ws_obs_mutation_a_${suffix}`;
  const workspaceB = `ws_obs_mutation_b_${suffix}`;

  beforeAll(async () => {
    adminPool = new Pool({ connectionString: ADMIN_DATABASE_URL, max: 1 });
    const databaseUrl = new URL(REGISTRY_DATABASE_URL);
    databaseUrl.pathname = `/${databaseName}`;
    await adminPool.query(`CREATE DATABASE "${databaseName}"`);
    await adminPool.query(
      `ALTER DATABASE "${databaseName}" SET default_transaction_isolation TO 'repeatable read'`,
    );
    pool = new Pool({ connectionString: databaseUrl.toString(), max: 8 });
    db = drizzle(pool, { schema });
    await migrate(drizzle(pool), { migrationsFolder: MIGRATIONS_FOLDER });
    const now = new Date();
    await requireDb()
      .insert(schema.agentObservabilityPlatformPolicy)
      .values({
        id: 'default',
        allowedAdapters: ['otlp_http'],
        allowedEndpointClasses: ['public'],
        maxCaptureMode: 'metadata_only',
        captureRestrictionEpoch: 0,
        createdAt: now,
        updatedAt: now,
      })
      .onConflictDoNothing();
    await seedOrganization(requireDb(), organizationA, workspaceA);
    await seedOrganization(requireDb(), organizationB, workspaceB);
  });

  afterAll(async () => {
    await pool?.end().catch(() => {});
    await adminPool
      ?.query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1', [
        databaseName,
      ])
      .catch(() => {});
    await adminPool?.query(`DROP DATABASE IF EXISTS "${databaseName}"`).catch(() => {});
    await adminPool?.end().catch(() => {});
  });

  it('migrates 0055 idempotently with durable writer and cleanup tombstone columns', async () => {
    const currentPool = requirePool();
    const columns = await currentPool.query<{ column_name: string }>(
      `select column_name
       from information_schema.columns
       where table_schema = 'public'
         and table_name = 'agent_observability_credential_staging_intents'
         and column_name in (
           'proposed_credential_version', 'writer_token', 'writer_lease_expires_at',
           'put_completed_at', 'cleanup_token', 'cleanup_lease_expires_at',
           'next_cleanup_at'
         )`,
    );
    expect(columns.rows.map((row) => row.column_name).sort()).toEqual([
      'cleanup_lease_expires_at',
      'cleanup_token',
      'next_cleanup_at',
      'proposed_credential_version',
      'put_completed_at',
      'writer_lease_expires_at',
      'writer_token',
    ]);
    const constraints = await currentPool.query<{ conname: string }>(
      `select conname from pg_constraint
       where conrelid = 'agent_observability_credential_staging_intents'::regclass`,
    );
    expect(constraints.rows.map((row) => row.conname)).toEqual(
      expect.arrayContaining([
        'agent_observability_credential_staging_intents_reservation_fk',
        'agent_observability_credential_staging_intents_status_check',
      ]),
    );
    expect(constraints.rows.map((row) => row.conname)).not.toContain(
      'agent_observability_credential_staging_intents_candidate_binding_fk',
    );
    await migrate(drizzle(currentPool), { migrationsFolder: MIGRATIONS_FOLDER });
  });

  it('retains database target uniqueness and cross-organization binding ownership', async () => {
    const currentDb = requireDb();
    const foreignBinding = await createBinding(currentDb, organizationA, null);
    await expect(
      requirePool().query(
        `insert into agent_observability_mutation_reservations (
          id, organization_id, workspace_id, target_type, target_key, target_binding_id,
          target_binding_scope, owner_principal, body_hash, expected_state_version,
          expected_config_version, expected_credential_version, generation, status, expires_at,
          fenced_at, expired_at, committed_at, created_at, updated_at
        ) values ($1, $2, null, 'binding', $3, $4, 'organization', 'admin', $5,
                  'state', 1, 1, 1, 'pending', now() + interval '1 minute',
                  null, null, null, now(), now())`,
        [
          newId('aomr'),
          organizationB,
          `cross-org-${suffix}`,
          foreignBinding.id,
          agentObservabilityMutationBodyHash({ op: 'cross-org' }),
        ],
      ),
    ).rejects.toMatchObject({ code: '23503' });

    const target: AgentObservabilityMutationTarget = {
      type: 'workspace_setting',
      organizationId: organizationA,
      workspaceId: workspaceA,
    };
    const first = await acquireReservation(currentDb, {
      target,
      key: 'live-target-a',
      body: { op: 'live-target' },
      expectedVersions: versions(1),
    });
    const competing = await withAuthority(currentDb, target, (tx) =>
      acquireAgentObservabilityMutationReservation(tx, {
        target,
        idempotency: idempotency(organizationA, 'live-target-b'),
        bodyHash: agentObservabilityMutationBodyHash({ op: 'live-target' }),
        expectedVersions: versions(1),
      }),
    );
    expect(competing).toEqual({ kind: 'conflict', reason: 'target_pending' });
    await fence(currentDb, first);
  });

  it('keeps TTL-live requests bound, then atomically rebinds changed body and target', async () => {
    const currentDb = requireDb();
    const now = new Date();
    const bodyTarget: AgentObservabilityMutationTarget = {
      type: 'workspace_setting',
      organizationId: organizationA,
      workspaceId: workspaceA,
    };
    const first = await acquireReservation(currentDb, {
      target: bodyTarget,
      key: 'ttl-body',
      body: { operation: 'old' },
      expectedVersions: versions(1),
      now,
      ttlMs: 100,
      idempotencyTtlMs: 100,
    });
    const withinTtl = await withAuthority(currentDb, bodyTarget, (tx) =>
      acquireAgentObservabilityMutationReservation(tx, {
        target: bodyTarget,
        idempotency: idempotency(organizationA, 'ttl-body'),
        bodyHash: agentObservabilityMutationBodyHash({ operation: 'changed' }),
        expectedVersions: versions(1),
        now: new Date(now.getTime() + 1),
      }),
    );
    expect(withinTtl).toEqual({
      kind: 'conflict',
      reason: 'idempotency_key_reused_with_different_body',
    });
    const reboundBody = await acquireReservation(currentDb, {
      target: bodyTarget,
      key: 'ttl-body',
      body: { operation: 'changed' },
      expectedVersions: versions(1),
      now: new Date(now.getTime() + 101),
    });
    expect(reboundBody.generation).toBe(first.generation + 1);
    const reboundBodyRow = (
      await currentDb
        .select()
        .from(schema.agentObservabilityIdempotencyKeys)
        .where(
          eq(schema.agentObservabilityIdempotencyKeys.reservationId, reboundBody.reservationId),
        )
    )[0];
    expect(reboundBodyRow?.bodyHash).toBe(
      agentObservabilityMutationBodyHash({ operation: 'changed' }),
    );
    await fence(currentDb, reboundBody);

    const oldTarget: AgentObservabilityMutationTarget = {
      type: 'organization_setting',
      organizationId: organizationA,
    };
    const newTarget: AgentObservabilityMutationTarget = {
      type: 'workspace_setting',
      organizationId: organizationA,
      workspaceId: workspaceA,
    };
    const targetStart = await acquireReservation(currentDb, {
      target: oldTarget,
      key: 'ttl-target',
      body: { operation: 'same' },
      expectedVersions: versions(1),
      now,
      ttlMs: 100,
      idempotencyTtlMs: 100,
    });
    const reboundTarget = await acquireReservation(currentDb, {
      target: newTarget,
      key: 'ttl-target',
      body: { operation: 'changed-target' },
      expectedVersions: versions(1),
      now: new Date(now.getTime() + 101),
    });
    expect(reboundTarget.generation).toBeGreaterThan(0);
    expect(reboundTarget.reservationId).not.toBe(targetStart.reservationId);
    const reboundTargetRow = (
      await currentDb
        .select()
        .from(schema.agentObservabilityIdempotencyKeys)
        .where(
          eq(schema.agentObservabilityIdempotencyKeys.reservationId, reboundTarget.reservationId),
        )
    )[0];
    expect(reboundTargetRow?.bodyHash).toBe(
      agentObservabilityMutationBodyHash({ operation: 'changed-target' }),
    );
    await fence(currentDb, reboundTarget);

    const liveBinding = await createBinding(currentDb, organizationA, null);
    const liveTarget = bindingTarget(organizationA, liveBinding.id);
    const live = await acquireReservation(currentDb, {
      target: liveTarget,
      key: 'ttl-still-live',
      body: { operation: 'old' },
      expectedVersions: versions(1),
      now,
      ttlMs: 1_000,
      idempotencyTtlMs: 100,
    });
    const expiredCacheButLive = await withAuthority(currentDb, newTarget, (tx) =>
      acquireAgentObservabilityMutationReservation(tx, {
        target: newTarget,
        idempotency: idempotency(organizationA, 'ttl-still-live'),
        bodyHash: agentObservabilityMutationBodyHash({ operation: 'changed' }),
        expectedVersions: versions(1),
        now: new Date(now.getTime() + 101),
      }),
    );
    expect(expiredCacheButLive).toMatchObject({
      kind: 'in_progress',
      reservation: { reservationId: live.reservationId },
    });
    await fence(currentDb, live);
  });

  it('requires repeatable-read finalization and keeps one authority snapshot', async () => {
    const currentDb = requireDb();
    const target: AgentObservabilityMutationTarget = {
      type: 'organization_setting',
      organizationId: organizationA,
    };
    const reservation = await acquireReservation(currentDb, {
      target,
      key: 'repeatable-read-required',
      body: { operation: 'repeatable-read-required' },
      expectedVersions: versions(null),
    });

    await expect(
      currentDb.transaction(
        (tx) =>
          finalizeAgentObservabilityMutation(tx as DbTransaction, {
            reservation,
            expectedVersions: reservation.expectedVersions,
            responseStatus: 200,
            audit: audit('read-committed-rejected'),
            apply: async () => ({ applied: true }),
          }),
        { isolationLevel: 'read committed' },
      ),
    ).rejects.toThrow(AgentObservabilityMutationValidationError);

    const originalPolicy = (
      await currentDb.select().from(schema.agentObservabilityPlatformPolicy).limit(1)
    )[0];
    if (!originalPolicy) throw new Error('missing platform policy');
    let firstSnapshot:
      | Awaited<ReturnType<typeof loadOrganizationAgentObservabilityStateInTransaction>>
      | undefined;
    try {
      await currentDb.transaction(
        async (tx) => {
          firstSnapshot = await loadOrganizationAgentObservabilityStateInTransaction({
            db: tx as DbTransaction,
            organizationId: organizationA,
          });
          await currentDb
            .update(schema.agentObservabilityPlatformPolicy)
            .set({
              maxCaptureMode:
                originalPolicy.maxCaptureMode === 'metadata_only' ? 'redacted_io' : 'metadata_only',
              captureRestrictionEpoch: originalPolicy.captureRestrictionEpoch + 1,
              updatedAt: new Date(),
            })
            .where(eq(schema.agentObservabilityPlatformPolicy.id, 'default'));
          const secondSnapshot = await loadOrganizationAgentObservabilityStateInTransaction({
            db: tx as DbTransaction,
            organizationId: organizationA,
          });
          expect(secondSnapshot).toEqual(firstSnapshot);
        },
        { isolationLevel: 'repeatable read' },
      );
      if (!firstSnapshot) throw new Error('missing repeatable-read snapshot');
      const changedSnapshot = await currentDb.transaction(
        (tx) =>
          loadOrganizationAgentObservabilityStateInTransaction({
            db: tx as DbTransaction,
            organizationId: organizationA,
          }),
        { isolationLevel: 'repeatable read' },
      );
      expect(changedSnapshot.etagInput).not.toEqual(firstSnapshot.etagInput);
    } finally {
      await currentDb
        .update(schema.agentObservabilityPlatformPolicy)
        .set({
          maxCaptureMode: originalPolicy.maxCaptureMode,
          captureRestrictionEpoch: originalPolicy.captureRestrictionEpoch,
          updatedAt: originalPolicy.updatedAt,
        })
        .where(eq(schema.agentObservabilityPlatformPolicy.id, 'default'));
    }
    await fence(currentDb, reservation);
  });

  it('settles exact finalization losers without treating invalid references as conflicts', async () => {
    const currentDb = requireDb();
    const target: AgentObservabilityMutationTarget = {
      type: 'workspace_setting',
      organizationId: organizationA,
      workspaceId: workspaceA,
    };
    const staged = await acquireReservation(currentDb, {
      target,
      key: 'settlement-pending',
      body: { operation: 'settlement-pending' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
    });
    expect(
      await currentDb.transaction((tx) =>
        settleAgentObservabilityMutationReservation(tx as DbTransaction, staged),
      ),
    ).toEqual({ kind: 'fenced' });
    const [staging] = await currentDb
      .select()
      .from(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(schema.agentObservabilityCredentialStagingIntents.reservationId, staged.reservationId),
      );
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
    expect(
      await currentDb.transaction((tx) =>
        settleAgentObservabilityMutationReservation(tx as DbTransaction, staged),
      ),
    ).toEqual({ kind: 'already_terminal', status: 'fenced' });

    const expiredAt = new Date();
    const expired = await acquireReservation(currentDb, {
      target,
      key: 'settlement-expired',
      body: { operation: 'settlement-expired' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
      now: expiredAt,
      ttlMs: 1,
    });
    expect(
      await currentDb.transaction((tx) =>
        settleAgentObservabilityMutationReservation(tx as DbTransaction, {
          ...expired,
          now: new Date(expiredAt.getTime() + 2),
        }),
      ),
    ).toEqual({ kind: 'expired' });
    const [expiredReservation, expiredStaging] = await Promise.all([
      currentDb
        .select()
        .from(schema.agentObservabilityMutationReservations)
        .where(eq(schema.agentObservabilityMutationReservations.id, expired.reservationId)),
      currentDb
        .select()
        .from(schema.agentObservabilityCredentialStagingIntents)
        .where(
          eq(
            schema.agentObservabilityCredentialStagingIntents.reservationId,
            expired.reservationId,
          ),
        ),
    ]);
    expect(expiredReservation[0]).toMatchObject({ status: 'expired' });
    expect(expiredStaging[0]).toMatchObject({ status: 'cleanup_pending' });
    expect(
      await currentDb.transaction((tx) =>
        settleAgentObservabilityMutationReservation(tx as DbTransaction, expired),
      ),
    ).toEqual({ kind: 'already_terminal', status: 'expired' });

    const committed = await acquireReservation(currentDb, {
      target,
      key: 'settlement-committed',
      body: { operation: 'settlement-committed' },
      expectedVersions: versions(1),
    });
    await currentDb
      .update(schema.agentObservabilityMutationReservations)
      .set({ status: 'committed', committedAt: new Date(), updatedAt: new Date() })
      .where(eq(schema.agentObservabilityMutationReservations.id, committed.reservationId));
    await currentDb
      .update(schema.agentObservabilityIdempotencyKeys)
      .set({
        status: 'completed',
        responseStatus: 200,
        responseBody: {},
        completedAt: new Date(),
        updatedAt: new Date(),
      })
      .where(eq(schema.agentObservabilityIdempotencyKeys.reservationId, committed.reservationId));
    expect(
      await currentDb.transaction((tx) =>
        settleAgentObservabilityMutationReservation(tx as DbTransaction, committed),
      ),
    ).toEqual({ kind: 'committed' });

    const mismatched = await acquireReservation(currentDb, {
      target,
      key: 'settlement-mismatch',
      body: { operation: 'settlement-mismatch' },
      expectedVersions: versions(1),
    });
    expect(
      await currentDb.transaction((tx) =>
        settleAgentObservabilityMutationReservation(tx as DbTransaction, {
          ...mismatched,
          ownerPrincipal: 'different-owner',
        }),
      ),
    ).toEqual({ kind: 'mismatch' });
    expect(
      await currentDb.transaction((tx) =>
        settleAgentObservabilityMutationReservation(tx as DbTransaction, {
          ...mismatched,
          reservationId: `aomr_missing_${mismatched.reservationId}`,
        }),
      ),
    ).toEqual({ kind: 'missing' });
    await fence(currentDb, mismatched);
  });

  it.each(['missing', 'completed', 'wrong body', 'wrong target'] as const)(
    'does not settle a pending reservation with a %s idempotency lifecycle',
    async (failure) => {
      const currentDb = requireDb();
      const binding = await createBinding(currentDb, organizationA, null);
      const target = bindingTarget(organizationA, binding.id);
      const reservation = await acquireReservation(currentDb, {
        target,
        key: `settlement-idempotency-${failure.replaceAll(' ', '-')}`,
        body: { operation: `settlement-idempotency-${failure}` },
        expectedVersions: versions(1),
      });
      const idempotencyWhere = eq(
        schema.agentObservabilityIdempotencyKeys.reservationId,
        reservation.reservationId,
      );
      if (failure === 'missing') {
        await currentDb.delete(schema.agentObservabilityIdempotencyKeys).where(idempotencyWhere);
      } else if (failure === 'completed') {
        await currentDb
          .update(schema.agentObservabilityIdempotencyKeys)
          .set({
            status: 'completed',
            responseStatus: 200,
            responseBody: {},
            completedAt: new Date(),
            updatedAt: new Date(),
          })
          .where(idempotencyWhere);
      } else if (failure === 'wrong body') {
        await currentDb
          .update(schema.agentObservabilityIdempotencyKeys)
          .set({
            bodyHash: agentObservabilityMutationBodyHash({ operation: 'wrong-idempotency-body' }),
            updatedAt: new Date(),
          })
          .where(idempotencyWhere);
      } else {
        await currentDb
          .update(schema.agentObservabilityIdempotencyKeys)
          .set({
            targetKey: agentObservabilityMutationTargetKey({
              type: 'organization_setting',
              organizationId: organizationA,
            }),
            updatedAt: new Date(),
          })
          .where(idempotencyWhere);
      }

      expect(
        await currentDb.transaction((tx) =>
          settleAgentObservabilityMutationReservation(tx as DbTransaction, reservation),
        ),
      ).toEqual({ kind: 'invalid' });
      const [stillPending] = await currentDb
        .select()
        .from(schema.agentObservabilityMutationReservations)
        .where(eq(schema.agentObservabilityMutationReservations.id, reservation.reservationId));
      expect(stillPending).toMatchObject({ status: 'pending' });
    },
  );

  it('keeps durable writer tombstone through arbitrary late puts and repeated delete', async () => {
    const currentDb = requireDb();
    const store = new LocalSecretStore();
    const now = new Date();
    const target: AgentObservabilityMutationTarget = {
      type: 'workspace_setting',
      organizationId: organizationA,
      workspaceId: workspaceA,
    };
    const candidateBindingId = newId('aob');
    const reservation = await acquireReservation(currentDb, {
      target,
      key: 'durable-staging',
      body: { operation: 'durable-staging' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId,
        proposedCredentialVersion: 1,
      },
      now,
      ttlMs: 1_000,
      stagingCleanupGraceMs: 1_000,
    });
    let applyCalls = 0;
    const beforeWrite = await currentDb.transaction((tx) =>
      finalizeAgentObservabilityMutation(tx as DbTransaction, {
        reservation,
        expectedVersions: reservation.expectedVersions,
        responseStatus: 200,
        audit: audit('before-write'),
        apply: async () => {
          applyCalls += 1;
          return { applied: true };
        },
      }),
    );
    expect(beforeWrite).toEqual({ kind: 'staging_not_written' });
    expect(applyCalls).toBe(0);

    const writer = await claimAgentObservabilityStagingWrite(currentDb, {
      reservation,
      now,
      writerLeaseMs: 3_000,
    });
    if (!writer) throw new Error('expected writer claim');
    expect(
      await heartbeatAgentObservabilityStagingWrite(currentDb, {
        reservation,
        writerToken: writer.writerToken,
        now: new Date(now.getTime() + 500),
        writerLeaseMs: 3_000,
      }),
    ).toBe(true);
    await fence(currentDb, reservation, new Date(now.getTime() + 600));

    const originalCleanupAfter = new Date(now.getTime() + 2_000);
    expect(
      await reconcileAgentObservabilityStagingIntents(currentDb, store, {
        now: new Date(originalCleanupAfter.getTime() + 500),
        cleanupIntervalMs: 500,
      }),
    ).toEqual({ claimed: 0, deleteSucceeded: 0, tombstonesRetained: 0, failed: 0, stale: 0 });

    const staleCleanupAt = new Date(now.getTime() + 3_600);
    expect(
      await reconcileAgentObservabilityStagingIntents(currentDb, store, {
        now: staleCleanupAt,
        cleanupIntervalMs: 500,
      }),
    ).toEqual({ claimed: 1, deleteSucceeded: 1, tombstonesRetained: 1, failed: 0, stale: 0 });
    expect(await store.resolve(writer.secretRef)).toBeNull();

    const rawSecret = `raw-observability-secret-${suffix}-late`;
    await store.put(
      writer.secretRef,
      encodeAgentObservabilitySecretBundle({
        bindingId: candidateBindingId,
        credentialVersion: 1,
        adapterType: 'otlp_http',
        auth: { type: 'bearer', token: rawSecret },
      }),
    );
    expect(
      await completeAgentObservabilityStagingWrite(currentDb, {
        reservation,
        writerToken: writer.writerToken,
        outcome: 'written',
        now: new Date(now.getTime() + 3_700),
      }),
    ).toEqual({ kind: 'stale' });
    expect(await store.resolve(writer.secretRef)).not.toBeNull();
    const staleFinalize = await currentDb.transaction((tx) =>
      finalizeAgentObservabilityMutation(tx as DbTransaction, {
        reservation,
        expectedVersions: reservation.expectedVersions,
        responseStatus: 200,
        audit: audit('stale-writer-finalize'),
        apply: async () => ({ applied: true }),
      }),
    );
    expect(staleFinalize).toEqual({ kind: 'reservation_not_live' });

    expect(
      await reconcileAgentObservabilityStagingIntents(currentDb, store, {
        now: new Date(now.getTime() + 4_200),
        cleanupIntervalMs: 500,
      }),
    ).toEqual({ claimed: 1, deleteSucceeded: 1, tombstonesRetained: 1, failed: 0, stale: 0 });
    expect(await store.resolve(writer.secretRef)).toBeNull();

    const arbitraryLaterCleanup = new Date(now.getTime() + 10_000);
    expect(
      await reconcileAgentObservabilityStagingIntents(currentDb, store, {
        now: arbitraryLaterCleanup,
        cleanupIntervalMs: 500,
      }),
    ).toEqual({ claimed: 1, deleteSucceeded: 1, tombstonesRetained: 1, failed: 0, stale: 0 });
    await store.put(
      writer.secretRef,
      encodeAgentObservabilitySecretBundle({
        bindingId: candidateBindingId,
        credentialVersion: 1,
        adapterType: 'otlp_http',
        auth: { type: 'bearer', token: `${rawSecret}-again` },
      }),
    );
    expect(
      await reconcileAgentObservabilityStagingIntents(currentDb, store, {
        now: new Date(now.getTime() + 10_500),
        cleanupIntervalMs: 500,
      }),
    ).toEqual({ claimed: 1, deleteSucceeded: 1, tombstonesRetained: 1, failed: 0, stale: 0 });
    const intent = await currentDb
      .select()
      .from(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(
          schema.agentObservabilityCredentialStagingIntents.reservationId,
          reservation.reservationId,
        ),
      );
    expect(intent).toHaveLength(1);
    expect(intent[0]).toMatchObject({ status: 'cleanup_pending' });
    expect(await store.resolve(writer.secretRef)).toBeNull();
  });

  it('bounds a never-settling staging cleanup delete and retains its tombstone', async () => {
    const currentDb = requireDb();
    const claimedAt = new Date(0);
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'bounded-staging-cleanup-delete',
      body: { operation: 'bounded-staging-cleanup-delete' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
      now: claimedAt,
      ttlMs: 1,
      stagingCleanupGraceMs: 1,
    });
    await fence(currentDb, reservation, claimedAt);

    const store = new HangingDeleteSecretStore();
    const reconciliation = reconcileAgentObservabilityStagingIntents(currentDb, store, {
      now: new Date(claimedAt.getTime() + 2),
      batchSize: 1,
      cleanupLeaseMs: 100,
      cleanupIntervalMs: 1,
      deleteTimeoutMs: 25,
    });
    await store.started.promise;

    await expect(reconciliation).resolves.toEqual({
      claimed: 1,
      deleteSucceeded: 0,
      tombstonesRetained: 0,
      failed: 1,
      stale: 0,
    });
    expect(store.deleteCalls).toBe(1);
    expect(store.signal?.aborted).toBe(true);
    const [intent] = await currentDb
      .select()
      .from(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(
          schema.agentObservabilityCredentialStagingIntents.reservationId,
          reservation.reservationId,
        ),
      );
    expect(intent).toMatchObject({
      status: 'cleanup_pending',
      cleanupToken: null,
      cleanupLeaseExpiresAt: null,
    });

    // Rejection after timeout must stay observed by the detached delete handler.
    store.deleteResult.reject(new Error('late staging cleanup delete failure'));
    await store.deleteSettled.promise;
    await currentDb
      .delete(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(
          schema.agentObservabilityCredentialStagingIntents.reservationId,
          reservation.reservationId,
        ),
      );
  });

  it('reschedules unstarted staging cleanup claims when its parent signal aborts', async () => {
    const currentDb = requireDb();
    const claimedAt = new Date(10);
    const first = await acquireReservation(currentDb, {
      target: { type: 'organization_setting', organizationId: organizationA },
      key: 'abort-staging-cleanup-first',
      body: { operation: 'abort-staging-cleanup-first' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
      now: claimedAt,
      ttlMs: 1,
      stagingCleanupGraceMs: 1,
    });
    const second = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'abort-staging-cleanup-second',
      body: { operation: 'abort-staging-cleanup-second' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
      now: claimedAt,
      ttlMs: 1,
      stagingCleanupGraceMs: 1,
    });
    await Promise.all([fence(currentDb, first, claimedAt), fence(currentDb, second, claimedAt)]);

    const store = new HangingDeleteSecretStore();
    const controller = new AbortController();
    const reconciliation = reconcileAgentObservabilityStagingIntents(currentDb, store, {
      now: new Date(claimedAt.getTime() + 2),
      batchSize: 2,
      cleanupLeaseMs: 1_000,
      cleanupIntervalMs: 1,
      deleteTimeoutMs: 500,
      signal: controller.signal,
    });
    await store.started.promise;
    controller.abort(new Error('maintenance stopped'));

    await expect(reconciliation).resolves.toEqual({
      claimed: 2,
      deleteSucceeded: 0,
      tombstonesRetained: 0,
      failed: 2,
      stale: 0,
    });
    expect(store.deleteCalls).toBe(1);
    expect(store.signal?.aborted).toBe(true);
    const intents = await Promise.all(
      [first, second].map(async (reservation) =>
        currentDb
          .select({
            status: schema.agentObservabilityCredentialStagingIntents.status,
            cleanupToken: schema.agentObservabilityCredentialStagingIntents.cleanupToken,
          })
          .from(schema.agentObservabilityCredentialStagingIntents)
          .where(
            eq(
              schema.agentObservabilityCredentialStagingIntents.reservationId,
              reservation.reservationId,
            ),
          ),
      ),
    );
    expect(intents.flat()).toEqual([
      { status: 'cleanup_pending', cleanupToken: null },
      { status: 'cleanup_pending', cleanupToken: null },
    ]);
    store.deleteResult.resolve();
    await store.deleteSettled.promise;
    await Promise.all(
      [first, second].map((reservation) =>
        currentDb
          .delete(schema.agentObservabilityCredentialStagingIntents)
          .where(
            eq(
              schema.agentObservabilityCredentialStagingIntents.reservationId,
              reservation.reservationId,
            ),
          ),
      ),
    );
  });

  it('bounds a stalled writer, aborts it, and retains its late-put tombstone', async () => {
    const currentDb = requireDb();
    const store = new LocalSecretStore();
    const claimedAt = new Date();
    const candidateBindingId = newId('aob');
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'bounded-staging-writer',
      body: { operation: 'bounded-staging-writer' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId,
        proposedCredentialVersion: 1,
      },
      now: claimedAt,
      ttlMs: 1_000,
      stagingCleanupGraceMs: 1,
    });
    let beginLatePut!: () => void;
    const latePutBlocked = new Promise<void>((resolve) => {
      beginLatePut = resolve;
    });
    let putStarted!: () => void;
    const putStartedPromise = new Promise<void>((resolve) => {
      putStarted = resolve;
    });
    let latePutFinished!: () => void;
    const latePutFinishedPromise = new Promise<void>((resolve) => {
      latePutFinished = resolve;
    });
    let secretRef: AgentObservabilitySecretReference | undefined;
    let signal: AbortSignal | undefined;
    const heartbeatCommitted = deferred<void>();
    let transactionCalls = 0;
    const observedDb = new Proxy(currentDb, {
      get(target, property, receiver) {
        if (property === 'transaction') {
          return (...args: unknown[]) => {
            const call = ++transactionCalls;
            return Promise.resolve(Reflect.apply(target.transaction, target, args)).then(
              (result) => {
                if (call === 2) heartbeatCommitted.resolve();
                return result;
              },
              (error: unknown) => {
                if (call === 2) heartbeatCommitted.reject(error);
                throw error;
              },
            );
          };
        }
        return Reflect.get(target, property, receiver);
      },
    }) as DbClient;

    const write = writeAgentObservabilityStagingIntent(observedDb, {
      reservation,
      now: claimedAt,
      writerLeaseMs: 1_000,
      heartbeatMs: 20,
      timeoutMs: 250,
      put: async (input) => {
        secretRef = input.secretRef;
        signal = input.signal;
        putStarted();
        await latePutBlocked;
        await store.put(input.secretRef, 'late bundle');
        latePutFinished();
      },
    });
    await putStartedPromise;
    await heartbeatCommitted.promise;
    const [writing] = await currentDb
      .select()
      .from(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(
          schema.agentObservabilityCredentialStagingIntents.reservationId,
          reservation.reservationId,
        ),
      );
    expect(writing?.writerLeaseExpiresAt?.getTime()).toBeGreaterThan(claimedAt.getTime() + 1_000);

    await expect(write).resolves.toEqual({ kind: 'tombstoned', reason: 'abandoned' });
    expect(signal?.aborted).toBe(true);
    const [tombstone] = await currentDb
      .select()
      .from(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(
          schema.agentObservabilityCredentialStagingIntents.reservationId,
          reservation.reservationId,
        ),
      );
    expect(tombstone).toMatchObject({
      status: 'cleanup_pending',
      writerToken: null,
      writerLeaseExpiresAt: null,
    });

    beginLatePut();
    await latePutFinishedPromise;
    if (!secretRef) throw new Error('staging put did not receive a secret reference');
    expect(await store.resolve(secretRef)).toBe('late bundle');

    await fence(currentDb, reservation);
    await expect(
      reconcileAgentObservabilityStagingIntents(currentDb, store, {
        now: new Date(claimedAt.getTime() + 2_000),
        cleanupIntervalMs: 1,
      }),
    ).resolves.toMatchObject({ claimed: 1, deleteSucceeded: 1, tombstonesRetained: 1 });
    expect(await store.resolve(secretRef)).toBeNull();
  });

  it('returns from a never-settling put and stops automatic heartbeats after timeout', async () => {
    const currentDb = requireDb();
    const claimedAt = new Date();
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'never-settling-staging-writer',
      body: { operation: 'never-settling-staging-writer' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
      now: claimedAt,
      ttlMs: 1_000,
      stagingCleanupGraceMs: 1,
    });
    const heartbeatCommitted = deferred<void>();
    let transactionCalls = 0;
    const observedDb = new Proxy(currentDb, {
      get(target, property, receiver) {
        if (property === 'transaction') {
          return (...args: unknown[]) => {
            const call = ++transactionCalls;
            return Promise.resolve(Reflect.apply(target.transaction, target, args)).then(
              (result) => {
                if (call === 2) heartbeatCommitted.resolve();
                return result;
              },
              (error: unknown) => {
                if (call === 2) heartbeatCommitted.reject(error);
                throw error;
              },
            );
          };
        }
        return Reflect.get(target, property, receiver);
      },
    }) as DbClient;
    const putStarted = deferred<void>();
    let signal: AbortSignal | undefined;
    let heartbeat: (() => Promise<boolean>) | undefined;

    const write = writeAgentObservabilityStagingIntent(observedDb, {
      reservation,
      writerLeaseMs: 500,
      heartbeatMs: 10,
      timeoutMs: 250,
      put: async (input) => {
        signal = input.signal;
        heartbeat = input.heartbeat;
        putStarted.resolve();
        await new Promise<void>(() => {});
      },
    });
    await putStarted.promise;
    await heartbeatCommitted.promise;
    expect(signal?.aborted).not.toBe(true);
    await expect(write).resolves.toEqual({ kind: 'tombstoned', reason: 'abandoned' });
    expect(signal?.aborted).toBe(true);

    const callsAfterTimeout = transactionCalls;
    if (heartbeat === undefined) throw new Error('staging put did not receive a heartbeat');
    expect(await heartbeat()).toBe(false);
    expect(transactionCalls).toBe(callsAfterTimeout);
    await fence(currentDb, reservation);
  });

  it('does not let a never-settling heartbeat defeat the staging-write deadline', async () => {
    const currentDb = requireDb();
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'never-settling-staging-heartbeat',
      body: { operation: 'never-settling-staging-heartbeat' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
      ttlMs: 1_000,
      stagingCleanupGraceMs: 1,
    });
    const heartbeatStarted = deferred<void>();
    let transactions = 0;
    const heartbeatHangingDb = new Proxy(currentDb, {
      get(target, property, receiver) {
        if (property === 'transaction') {
          return (...args: unknown[]) => {
            transactions += 1;
            if (transactions === 2) {
              heartbeatStarted.resolve();
              return new Promise<never>(() => {});
            }
            return Reflect.apply(target.transaction, target, args);
          };
        }
        return Reflect.get(target, property, receiver);
      },
    }) as DbClient;
    const putStarted = deferred<void>();
    let signal: AbortSignal | undefined;
    const write = writeAgentObservabilityStagingIntent(heartbeatHangingDb, {
      reservation,
      writerLeaseMs: 500,
      heartbeatMs: 10,
      timeoutMs: 100,
      put: async (input) => {
        signal = input.signal;
        putStarted.resolve();
        await new Promise<void>(() => {});
      },
    });
    await putStarted.promise;
    await heartbeatStarted.promise;

    await expect(write).resolves.toEqual({ kind: 'tombstoned', reason: 'abandoned' });
    expect(signal?.aborted).toBe(true);
    await fence(currentDb, reservation);
  });

  it('tombstones a writer immediately when its in-flight heartbeat loses authority', async () => {
    const currentDb = requireDb();
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'lost-heartbeat-staging-writer',
      body: { operation: 'lost-heartbeat-staging-writer' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
      ttlMs: 1_000,
      stagingCleanupGraceMs: 1,
    });
    let heartbeatResult: boolean | undefined;
    let signal: AbortSignal | undefined;

    await expect(
      writeAgentObservabilityStagingIntent(currentDb, {
        reservation,
        writerLeaseMs: 500,
        heartbeatMs: 100,
        timeoutMs: 250,
        put: async (input) => {
          signal = input.signal;
          await fence(currentDb, reservation);
          heartbeatResult = await input.heartbeat();
          await new Promise<void>(() => {});
        },
      }),
    ).resolves.toEqual({ kind: 'tombstoned', reason: 'preempted' });
    expect(heartbeatResult).toBe(false);
    expect(signal?.aborted).toBe(true);
  });

  it('returns a durable preempted reason when disable fences before writer claim', async () => {
    const currentDb = requireDb();
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'preempted-before-staging-claim',
      body: { operation: 'preempted-before-staging-claim' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
    });
    await fence(currentDb, reservation);
    let putCalls = 0;

    await expect(
      writeAgentObservabilityStagingIntent(currentDb, {
        reservation,
        put: async () => {
          putCalls += 1;
        },
      }),
    ).resolves.toEqual({ kind: 'not_claimed', reason: 'preempted' });
    expect(putCalls).toBe(0);
  });

  it('expires an unclaimed staging intent before reporting its durable terminal reason', async () => {
    const currentDb = requireDb();
    const createdAt = new Date();
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'expired-before-staging-claim',
      body: { operation: 'expired-before-staging-claim' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
      now: createdAt,
      ttlMs: 1,
      stagingCleanupGraceMs: 1,
    });
    let putCalls = 0;

    await expect(
      writeAgentObservabilityStagingIntent(currentDb, {
        reservation,
        now: new Date(createdAt.getTime() + 2),
        put: async () => {
          putCalls += 1;
        },
      }),
    ).resolves.toEqual({ kind: 'not_claimed', reason: 'expired' });
    expect(putCalls).toBe(0);
    const [staging] = await currentDb
      .select()
      .from(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(
          schema.agentObservabilityCredentialStagingIntents.reservationId,
          reservation.reservationId,
        ),
      );
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
  });

  it('tombstones a claimed writer as expired when its put crosses reservation expiry', async () => {
    const currentDb = requireDb();
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'expired-during-staging-write',
      body: { operation: 'expired-during-staging-write' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
    });
    const putStarted = deferred<void>();
    const releasePut = deferred<void>();
    const write = writeAgentObservabilityStagingIntent(currentDb, {
      reservation,
      put: async () => {
        putStarted.resolve();
        await releasePut.promise;
      },
    });
    await putStarted.promise;

    const [writing] = await currentDb
      .select()
      .from(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(
          schema.agentObservabilityCredentialStagingIntents.reservationId,
          reservation.reservationId,
        ),
      );
    expect(writing).toMatchObject({ status: 'writing' });
    await currentDb
      .update(schema.agentObservabilityMutationReservations)
      .set({ expiresAt: new Date(Date.now() - 1), updatedAt: new Date() })
      .where(eq(schema.agentObservabilityMutationReservations.id, reservation.reservationId));

    releasePut.resolve();
    await expect(write).resolves.toEqual({ kind: 'tombstoned', reason: 'expired' });

    const [[expired], [staging]] = await Promise.all([
      currentDb
        .select()
        .from(schema.agentObservabilityMutationReservations)
        .where(eq(schema.agentObservabilityMutationReservations.id, reservation.reservationId)),
      currentDb
        .select()
        .from(schema.agentObservabilityCredentialStagingIntents)
        .where(
          eq(
            schema.agentObservabilityCredentialStagingIntents.reservationId,
            reservation.reservationId,
          ),
        ),
    ]);
    expect(expired).toMatchObject({ status: 'expired' });
    expect(staging).toMatchObject({ status: 'cleanup_pending' });
  });

  it('distinguishes an active writer from an invalid reservation reference', async () => {
    const currentDb = requireDb();
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'busy-or-invalid-staging-claim',
      body: { operation: 'busy-or-invalid-staging-claim' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
    });
    const claim = await claimAgentObservabilityStagingWrite(currentDb, { reservation });
    if (!claim) throw new Error('expected staging writer claim');

    await expect(
      writeAgentObservabilityStagingIntent(currentDb, {
        reservation,
        put: async () => {
          throw new Error('must not write while another writer owns the intent');
        },
      }),
    ).resolves.toEqual({ kind: 'not_claimed', reason: 'busy' });
    await expect(
      writeAgentObservabilityStagingIntent(currentDb, {
        reservation: { ...reservation, reservationId: `aomr_missing_${reservation.reservationId}` },
        put: async () => {
          throw new Error('must not write an invalid reservation');
        },
      }),
    ).resolves.toEqual({ kind: 'not_claimed', reason: 'invalid' });
    await fence(currentDb, reservation);
  });

  it('rejects a persisted reservation missing its required staging intent', async () => {
    const currentDb = requireDb();
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'missing-staging-intent',
      body: { operation: 'missing-staging-intent' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
    });
    await currentDb
      .delete(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(
          schema.agentObservabilityCredentialStagingIntents.reservationId,
          reservation.reservationId,
        ),
      );

    await expect(
      writeAgentObservabilityStagingIntent(currentDb, {
        reservation,
        put: async () => {
          throw new Error('must not write a corrupt staging intent');
        },
      }),
    ).rejects.toBeInstanceOf(AgentObservabilityMutationValidationError);
    await fence(currentDb, reservation);
  });

  it('completes a bounded writer when its SecretStore put succeeds', async () => {
    const currentDb = requireDb();
    const store = new LocalSecretStore();
    const reservation = await acquireReservation(currentDb, {
      target: {
        type: 'workspace_setting',
        organizationId: organizationA,
        workspaceId: workspaceA,
      },
      key: 'successful-bounded-staging-writer',
      body: { operation: 'successful-bounded-staging-writer' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 1,
      },
    });
    let signal: AbortSignal | undefined;

    await expect(
      writeAgentObservabilityStagingIntent(currentDb, {
        reservation,
        writerLeaseMs: 1_000,
        heartbeatMs: 20,
        timeoutMs: 250,
        put: async (input) => {
          signal = input.signal;
          await store.put(input.secretRef, 'bundle', { signal: input.signal });
        },
      }),
    ).resolves.toEqual({ kind: 'written' });
    expect(signal?.aborted).toBe(false);
    const [written] = await currentDb
      .select()
      .from(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(
          schema.agentObservabilityCredentialStagingIntents.reservationId,
          reservation.reservationId,
        ),
      );
    expect(written).toMatchObject({
      status: 'written',
      writerToken: null,
      writerLeaseExpiresAt: null,
    });
    await fence(currentDb, reservation);
  });

  it('rejects same, lower, skipped, and non-initial credential generations before apply', async () => {
    const currentDb = requireDb();
    const store = new LocalSecretStore();
    for (const proposedCredentialVersion of [2, 1, 4]) {
      const oldRef = newAgentObservabilitySecretReference();
      const binding = await createBinding(currentDb, organizationA, null, oldRef, 2);
      const target = bindingTarget(organizationA, binding.id);
      const reservation = await acquireReservation(currentDb, {
        target,
        key: `invalid-rotation-${proposedCredentialVersion}`,
        body: { operation: 'invalid-rotation', proposedCredentialVersion },
        expectedVersions: versions(2),
        staging: {
          secretRef: newAgentObservabilitySecretReference(),
          candidateBindingId: binding.id,
          proposedCredentialVersion,
        },
      });
      await stageWritten(currentDb, reservation, store);
      let applyCalls = 0;
      const result = await currentDb.transaction((tx) =>
        finalizeAgentObservabilityMutation(tx as DbTransaction, {
          reservation,
          expectedVersions: reservation.expectedVersions,
          supersededSecretRef: oldRef,
          responseStatus: 200,
          audit: audit(`invalid-rotation-${proposedCredentialVersion}`),
          apply: async () => {
            applyCalls += 1;
            return { applied: true };
          },
        }),
      );
      expect(result).toEqual({ kind: 'invalid_credential_generation' });
      expect(applyCalls).toBe(0);
    }

    const nullExpectedOldRef = newAgentObservabilitySecretReference();
    const nullExpectedBinding = await createBinding(
      currentDb,
      organizationA,
      null,
      nullExpectedOldRef,
      1,
    );
    const nullExpectedReservation = await acquireReservation(currentDb, {
      target: bindingTarget(organizationA, nullExpectedBinding.id),
      key: 'rotation-null-expected-version',
      body: { operation: 'rotation-null-expected-version' },
      expectedVersions: versions(null),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: nullExpectedBinding.id,
        proposedCredentialVersion: 1,
      },
    });
    await stageWritten(currentDb, nullExpectedReservation, store);
    const nullExpectedResult = await currentDb.transaction((tx) =>
      finalizeAgentObservabilityMutation(tx as DbTransaction, {
        reservation: nullExpectedReservation,
        expectedVersions: nullExpectedReservation.expectedVersions,
        supersededSecretRef: nullExpectedOldRef,
        responseStatus: 200,
        audit: audit('rotation-null-expected-version'),
        apply: async () => ({ applied: true }),
      }),
    );
    expect(nullExpectedResult).toEqual({ kind: 'invalid_credential_generation' });

    const settingTarget: AgentObservabilityMutationTarget = {
      type: 'organization_setting',
      organizationId: organizationB,
    };
    const nonInitial = await acquireReservation(currentDb, {
      target: settingTarget,
      key: 'setting-non-initial',
      body: { operation: 'setting-non-initial' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: newId('aob'),
        proposedCredentialVersion: 2,
      },
    });
    await stageWritten(currentDb, nonInitial, store);
    const nonInitialResult = await currentDb.transaction((tx) =>
      finalizeAgentObservabilityMutation(tx as DbTransaction, {
        reservation: nonInitial,
        expectedVersions: nonInitial.expectedVersions,
        responseStatus: 200,
        audit: audit('setting-non-initial'),
        apply: async () => ({ applied: true }),
      }),
    );
    expect(nonInitialResult).toEqual({ kind: 'invalid_credential_generation' });
    await fence(currentDb, nonInitial);

    const existingCandidate = await createBinding(currentDb, organizationB, null);
    const reusedCandidate = await acquireReservation(currentDb, {
      target: settingTarget,
      key: 'setting-existing-candidate',
      body: { operation: 'setting-existing-candidate' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: existingCandidate.id,
        proposedCredentialVersion: 1,
      },
    });
    await stageWritten(currentDb, reusedCandidate, store);
    const reusedCandidateResult = await currentDb.transaction((tx) =>
      finalizeAgentObservabilityMutation(tx as DbTransaction, {
        reservation: reusedCandidate,
        expectedVersions: reusedCandidate.expectedVersions,
        responseStatus: 200,
        audit: audit('setting-existing-candidate'),
        apply: async () => ({ applied: true }),
      }),
    );
    expect(reusedCandidateResult).toEqual({ kind: 'invalid_credential_generation' });
    await fence(currentDb, reusedCandidate);
  });

  it('caches only authoritative state loaded after apply and rejects unselected binding cache', async () => {
    const currentDb = requireDb();
    const store = new LocalSecretStore();
    const settingTarget: AgentObservabilityMutationTarget = {
      type: 'organization_setting',
      organizationId: organizationB,
    };
    const candidateBindingId = newId('aob');
    const secretRef = newAgentObservabilitySecretReference();
    const settingReservation = await acquireReservation(currentDb, {
      target: settingTarget,
      key: 'authoritative-setting',
      body: { operation: 'authoritative-setting' },
      expectedVersions: versions(1),
      staging: { secretRef, candidateBindingId, proposedCredentialVersion: 1 },
    });
    await stageWritten(currentDb, settingReservation, store);
    const forgedRequestStateValue = `https://forged.example/${suffix}`;
    const settingResult = await currentDb.transaction((tx) =>
      finalizeAgentObservabilityMutation(tx as DbTransaction, {
        reservation: settingReservation,
        expectedVersions: settingReservation.expectedVersions,
        responseStatus: 200,
        audit: audit('authoritative-setting'),
        apply: async ({ db: applyDb, stagedActivation }) => {
          if (!stagedActivation) return { applied: false };
          await insertBindingGraph(applyDb, {
            id: stagedActivation.candidateBindingId,
            organizationId: organizationB,
            workspaceId: null,
            secretRef: stagedActivation.secretRef,
            credentialVersion: stagedActivation.credentialVersion,
          });
          await applyDb
            .update(schema.agentObservabilityOrganizationSettings)
            .set({
              activeDefaultBindingId: stagedActivation.candidateBindingId,
              activeDefaultBindingScope: 'organization',
              selectionEpoch: 1,
              updatedAt: new Date(),
            })
            .where(eq(schema.agentObservabilityOrganizationSettings.organizationId, organizationB));
          return { applied: true };
        },
      }),
    );
    expect(settingResult).toMatchObject({
      kind: 'committed',
      response: {
        body: {
          configured: { default_binding: { id: candidateBindingId } },
          effective: { binding: { id: candidateBindingId } },
        },
      },
    });
    const replay = await withAuthority(currentDb, settingTarget, (tx) =>
      acquireAgentObservabilityMutationReservation(tx, {
        target: settingTarget,
        idempotency: idempotency(organizationB, 'authoritative-setting'),
        bodyHash: agentObservabilityMutationBodyHash({ operation: 'authoritative-setting' }),
        expectedVersions: settingReservation.expectedVersions,
      }),
    );
    expect(replay).toMatchObject({
      kind: 'replay',
      response: {
        status: 200,
        body: { configured: { default_binding: { id: candidateBindingId } } },
      },
    });
    const cache = (
      await currentDb
        .select()
        .from(schema.agentObservabilityIdempotencyKeys)
        .where(
          eq(
            schema.agentObservabilityIdempotencyKeys.reservationId,
            settingReservation.reservationId,
          ),
        )
    )[0];
    expect(cache?.responseBody).toEqual(
      settingResult.kind === 'committed' ? settingResult.response.body : null,
    );
    expect(JSON.stringify(cache?.responseBody)).not.toContain(forgedRequestStateValue);
    expect(JSON.stringify(cache?.responseBody)).not.toContain(secretRef);

    const unselectedSettingCandidate = newId('aob');
    const unselectedSettingReservation = await acquireReservation(currentDb, {
      target: settingTarget,
      key: 'authoritative-setting-not-selected',
      body: { operation: 'authoritative-setting-not-selected' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: unselectedSettingCandidate,
        proposedCredentialVersion: 1,
      },
    });
    await stageWritten(currentDb, unselectedSettingReservation, store);
    const unselectedSettingResult = await currentDb.transaction((tx) =>
      finalizeAgentObservabilityMutation(tx as DbTransaction, {
        reservation: unselectedSettingReservation,
        expectedVersions: unselectedSettingReservation.expectedVersions,
        responseStatus: 200,
        audit: audit('authoritative-setting-not-selected'),
        apply: async ({ db: applyDb, stagedActivation }) => {
          if (!stagedActivation) return { applied: false };
          await insertBindingGraph(applyDb, {
            id: stagedActivation.candidateBindingId,
            organizationId: organizationB,
            workspaceId: null,
            secretRef: stagedActivation.secretRef,
            credentialVersion: stagedActivation.credentialVersion,
          });
          return { applied: true };
        },
      }),
    );
    expect(unselectedSettingResult).toEqual({ kind: 'authoritative_state_mismatch' });
    await expectCandidateSelectionRollback(
      currentDb,
      unselectedSettingReservation,
      unselectedSettingCandidate,
      'req_authoritative-setting-not-selected',
    );

    const workspaceTarget: AgentObservabilityMutationTarget = {
      type: 'workspace_setting',
      organizationId: organizationA,
      workspaceId: workspaceA,
    };
    const unselectedWorkspaceCandidate = newId('aob');
    const unselectedWorkspaceReservation = await acquireReservation(currentDb, {
      target: workspaceTarget,
      key: 'authoritative-workspace-not-selected',
      body: { operation: 'authoritative-workspace-not-selected' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: unselectedWorkspaceCandidate,
        proposedCredentialVersion: 1,
      },
    });
    await stageWritten(currentDb, unselectedWorkspaceReservation, store);
    const unselectedWorkspaceResult = await currentDb.transaction((tx) =>
      finalizeAgentObservabilityMutation(tx as DbTransaction, {
        reservation: unselectedWorkspaceReservation,
        expectedVersions: unselectedWorkspaceReservation.expectedVersions,
        responseStatus: 200,
        audit: audit('authoritative-workspace-not-selected'),
        apply: async ({ db: applyDb, stagedActivation }) => {
          if (!stagedActivation) return { applied: false };
          await insertBindingGraph(applyDb, {
            id: stagedActivation.candidateBindingId,
            organizationId: organizationA,
            workspaceId: workspaceA,
            secretRef: stagedActivation.secretRef,
            credentialVersion: stagedActivation.credentialVersion,
          });
          return { applied: true };
        },
      }),
    );
    expect(unselectedWorkspaceResult).toEqual({ kind: 'authoritative_state_mismatch' });
    await expectCandidateSelectionRollback(
      currentDb,
      unselectedWorkspaceReservation,
      unselectedWorkspaceCandidate,
      'req_authoritative-workspace-not-selected',
    );

    const unselectedOldRef = newAgentObservabilitySecretReference();
    const unselected = await createBinding(currentDb, organizationA, null, unselectedOldRef, 1);
    const unselectedTarget = bindingTarget(organizationA, unselected.id);
    const unselectedReservation = await acquireReservation(currentDb, {
      target: unselectedTarget,
      key: 'unselected-binding',
      body: { operation: 'unselected-binding' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: unselected.id,
        proposedCredentialVersion: 2,
      },
    });
    await stageWritten(currentDb, unselectedReservation, store);
    const unselectedResult = await finalizeBindingRotation(
      currentDb,
      unselectedReservation,
      unselectedTarget,
      unselectedOldRef,
      'unselected-binding',
    );
    expect(unselectedResult).toEqual({ kind: 'authoritative_state_mismatch' });
    const unselectedHead = (
      await currentDb
        .select()
        .from(schema.agentObservabilityBindingCredentials)
        .where(eq(schema.agentObservabilityBindingCredentials.bindingId, unselected.id))
    )[0];
    expect(unselectedHead).toMatchObject({ secretRef: unselectedOldRef, credentialVersion: 1 });

    const selectedOldRef = newAgentObservabilitySecretReference();
    const selected = await createBinding(currentDb, organizationA, null, selectedOldRef, 1);
    await configureOrganizationDefaultBinding(currentDb, organizationA, selected.id);
    const selectedTarget = bindingTarget(organizationA, selected.id);
    const selectedReservation = await acquireReservation(currentDb, {
      target: selectedTarget,
      key: 'selected-binding',
      body: { operation: 'selected-binding' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: selected.id,
        proposedCredentialVersion: 2,
      },
    });
    await stageWritten(currentDb, selectedReservation, store);
    const selectedResult = await finalizeBindingRotation(
      currentDb,
      selectedReservation,
      selectedTarget,
      selectedOldRef,
      'selected-binding',
    );
    expect(selectedResult).toMatchObject({
      kind: 'committed',
      response: { body: { configured: { default_binding: { id: selected.id } } } },
    });
    const cleanup = await currentDb
      .select()
      .from(schema.agentObservabilitySecretCleanupOutbox)
      .where(eq(schema.agentObservabilitySecretCleanupOutbox.bindingId, selected.id));
    expect(cleanup).toHaveLength(1);
    expect(cleanup[0]?.secretRef).toBe(selectedOldRef);
  });

  it('propagates unexpected authoritative state loader database failures', async () => {
    const currentDb = requireDb();
    const target: AgentObservabilityMutationTarget = {
      type: 'organization_setting',
      organizationId: organizationA,
    };
    const reservation = await acquireReservation(currentDb, {
      target,
      key: 'authoritative-loader-sql-failure',
      body: { operation: 'authoritative-loader-sql-failure' },
      expectedVersions: versions(1),
    });
    const temporaryTableName = `agent_observability_platform_policy_broken_${suffix}`;

    await expect(
      currentDb.transaction((tx) =>
        finalizeAgentObservabilityMutation(tx as DbTransaction, {
          reservation,
          expectedVersions: reservation.expectedVersions,
          responseStatus: 200,
          audit: audit('authoritative-loader-sql-failure'),
          apply: async ({ db: applyDb }) => {
            await applyDb.execute(
              sql.raw(
                `ALTER TABLE agent_observability_platform_policy RENAME TO "${temporaryTableName}"`,
              ),
            );
            return { applied: true };
          },
        }),
      ),
    ).rejects.toMatchObject({ code: '42P01' });

    await expect(
      currentDb.select().from(schema.agentObservabilityPlatformPolicy).limit(1),
    ).resolves.toHaveLength(1);
    const rolledBackReservation = (
      await currentDb
        .select()
        .from(schema.agentObservabilityMutationReservations)
        .where(eq(schema.agentObservabilityMutationReservations.id, reservation.reservationId))
    )[0];
    expect(rolledBackReservation?.status).toBe('pending');
    await fence(currentDb, reservation);
  });

  it('expires abandoned settings attempts while pruning idempotency without deleting reservations', async () => {
    const currentDb = requireDb();
    const store = new LocalSecretStore();
    const abandonedNow = new Date();
    const abandonedTarget: AgentObservabilityMutationTarget = {
      type: 'workspace_setting',
      organizationId: organizationB,
      workspaceId: workspaceB,
    };
    const abandoned = await acquireReservation(currentDb, {
      target: abandonedTarget,
      key: 'retention-abandoned-setting',
      body: { operation: 'retention-abandoned-setting' },
      expectedVersions: versions(null),
      now: abandonedNow,
      ttlMs: 100,
      idempotencyTtlMs: 100,
    });
    const abandonedPruned = await pruneAgentObservabilityMutationRetention(currentDb, {
      now: new Date(abandonedNow.getTime() + 101),
      batchSize: 1_000,
    });
    expect(abandonedPruned.idempotencyDeleted).toBeGreaterThanOrEqual(1);
    const abandonedReservation = (
      await currentDb
        .select()
        .from(schema.agentObservabilityMutationReservations)
        .where(eq(schema.agentObservabilityMutationReservations.id, abandoned.reservationId))
    )[0];
    expect(abandonedReservation?.status).toBe('expired');
    const abandonedKeyRows = await currentDb
      .select()
      .from(schema.agentObservabilityIdempotencyKeys)
      .where(eq(schema.agentObservabilityIdempotencyKeys.key, 'retention-abandoned-setting'));
    expect(abandonedKeyRows).toHaveLength(0);
    const recovered = await acquireReservation(currentDb, {
      target: abandonedTarget,
      key: 'retention-abandoned-setting',
      body: { operation: 'retention-abandoned-setting' },
      expectedVersions: versions(null),
      now: new Date(abandonedNow.getTime() + 102),
    });
    expect(recovered.generation).toBe(abandoned.generation + 1);
    await fence(currentDb, recovered);

    const oldRef = newAgentObservabilitySecretReference();
    const binding = await createBinding(currentDb, organizationB, null, oldRef, 1);
    await configureOrganizationDefaultBinding(currentDb, organizationB, binding.id);
    const target = bindingTarget(organizationB, binding.id);
    const now = new Date();
    const reservation = await acquireReservation(currentDb, {
      target,
      key: 'retention',
      body: { operation: 'retention' },
      expectedVersions: versions(1),
      staging: {
        secretRef: newAgentObservabilitySecretReference(),
        candidateBindingId: binding.id,
        proposedCredentialVersion: 2,
      },
      now,
      idempotencyTtlMs: 100,
    });
    await stageWritten(currentDb, reservation, store, now);
    expect(
      await finalizeBindingRotation(currentDb, reservation, target, oldRef, 'retention', now),
    ).toMatchObject({ kind: 'committed' });
    const pruned = await pruneAgentObservabilityMutationRetention(currentDb, {
      now: new Date(now.getTime() + 101),
    });
    expect(pruned.idempotencyDeleted).toBeGreaterThanOrEqual(1);
    expect(pruned.reservationsDeleted).toBe(0);
    const next = await acquireReservation(currentDb, {
      target,
      key: 'retention',
      body: { operation: 'retention' },
      expectedVersions: versions(2),
    });
    expect(next.generation).toBe(reservation.generation + 1);
    await fence(currentDb, next);
  });

  it('bounds a never-settling superseded-secret cleanup delete and backs it off', async () => {
    const currentDb = requireDb();
    const binding = await createBinding(currentDb, organizationA, null);
    const now = new Date(0);
    const cleanupId = await enqueueSecretCleanup(
      currentDb,
      organizationA,
      binding.id,
      newAgentObservabilitySecretReference(),
      now,
    );
    const store = new HangingDeleteSecretStore();
    const reconciliation = reconcileAgentObservabilitySecretCleanup(currentDb, store, {
      now,
      batchSize: 1,
      leaseMs: 100,
      baseBackoffMs: 1,
      maxBackoffMs: 1,
      deleteTimeoutMs: 25,
    });
    await store.started.promise;

    await expect(reconciliation).resolves.toEqual({ claimed: 1, deleted: 0, failed: 1, stale: 0 });
    expect(store.deleteCalls).toBe(1);
    expect(store.signal?.aborted).toBe(true);
    const [cleanup] = await currentDb
      .select()
      .from(schema.agentObservabilitySecretCleanupOutbox)
      .where(eq(schema.agentObservabilitySecretCleanupOutbox.id, cleanupId));
    expect(cleanup).toMatchObject({
      status: 'pending',
      claimToken: null,
      leaseExpiresAt: null,
      attemptCount: 1,
    });

    // Rejection after timeout must stay observed by the detached delete handler.
    store.deleteResult.reject(new Error('late superseded-secret cleanup delete failure'));
    await store.deleteSettled.promise;
    await currentDb
      .delete(schema.agentObservabilitySecretCleanupOutbox)
      .where(eq(schema.agentObservabilitySecretCleanupOutbox.id, cleanupId));
  });

  it('backs off unstarted superseded-secret cleanup claims when its parent signal aborts', async () => {
    const currentDb = requireDb();
    const binding = await createBinding(currentDb, organizationA, null);
    const now = new Date(10);
    const cleanupIds = await Promise.all(
      [newAgentObservabilitySecretReference(), newAgentObservabilitySecretReference()].map(
        (secretRef) => enqueueSecretCleanup(currentDb, organizationA, binding.id, secretRef, now),
      ),
    );
    const store = new HangingDeleteSecretStore();
    const controller = new AbortController();
    const reconciliation = reconcileAgentObservabilitySecretCleanup(currentDb, store, {
      now,
      batchSize: 2,
      leaseMs: 1_000,
      baseBackoffMs: 1,
      maxBackoffMs: 1,
      deleteTimeoutMs: 500,
      signal: controller.signal,
    });
    await store.started.promise;
    controller.abort(new Error('maintenance stopped'));

    await expect(reconciliation).resolves.toEqual({ claimed: 2, deleted: 0, failed: 2, stale: 0 });
    expect(store.deleteCalls).toBe(1);
    expect(store.signal?.aborted).toBe(true);
    const cleanupRows = await Promise.all(
      cleanupIds.map((id) =>
        currentDb
          .select({
            status: schema.agentObservabilitySecretCleanupOutbox.status,
            claimToken: schema.agentObservabilitySecretCleanupOutbox.claimToken,
            attemptCount: schema.agentObservabilitySecretCleanupOutbox.attemptCount,
          })
          .from(schema.agentObservabilitySecretCleanupOutbox)
          .where(eq(schema.agentObservabilitySecretCleanupOutbox.id, id)),
      ),
    );
    expect(cleanupRows.flat()).toEqual([
      { status: 'pending', claimToken: null, attemptCount: 1 },
      { status: 'pending', claimToken: null, attemptCount: 1 },
    ]);
    store.deleteResult.resolve();
    await store.deleteSettled.promise;
    await Promise.all(
      cleanupIds.map((id) =>
        currentDb
          .delete(schema.agentObservabilitySecretCleanupOutbox)
          .where(eq(schema.agentObservabilitySecretCleanupOutbox.id, id)),
      ),
    );
  });

  it('keeps cleanup outbox token/backoff stale-safe and DB free of raw credential bytes', async () => {
    const currentDb = requireDb();
    const binding = await createBinding(currentDb, organizationA, null);
    const ref = newAgentObservabilitySecretReference();
    const now = new Date();
    await currentDb.insert(schema.agentObservabilitySecretCleanupOutbox).values({
      id: newId('aomclean'),
      organizationId: organizationA,
      bindingId: binding.id,
      bindingScope: 'organization',
      secretRef: ref,
      status: 'pending',
      claimToken: null,
      leaseExpiresAt: null,
      attemptCount: 0,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    });
    const [first] = await claimAgentObservabilitySecretCleanup(currentDb, {
      now,
      leaseMs: AGENT_OBSERVABILITY_SECRET_CLEANUP_LEASE_MS,
    });
    if (!first) throw new Error('expected cleanup claim');
    expect(
      await completeAgentObservabilitySecretCleanup(currentDb, {
        ...first,
        outcome: 'failed',
        now,
        baseBackoffMs: 10,
        maxBackoffMs: 10,
      }),
    ).toBe(true);
    const [second] = await claimAgentObservabilitySecretCleanup(currentDb, {
      now: new Date(now.getTime() + 10),
      leaseMs: 10,
    });
    if (!second) throw new Error('expected retry claim');
    const [replacement] = await claimAgentObservabilitySecretCleanup(currentDb, {
      now: new Date(now.getTime() + 21),
      leaseMs: 10,
    });
    if (!replacement) throw new Error('expected replacement claim');
    expect(
      await completeAgentObservabilitySecretCleanup(currentDb, {
        ...second,
        outcome: 'deleted',
        now: new Date(now.getTime() + 21),
      }),
    ).toBe(false);
    expect(
      await completeAgentObservabilitySecretCleanup(currentDb, {
        ...replacement,
        outcome: 'deleted',
        now: new Date(now.getTime() + 21),
      }),
    ).toBe(true);

    await reconcileAgentObservabilitySecretCleanup(currentDb, new LocalSecretStore());
    const rawSecretFixture = `raw-observability-secret-${suffix}`;
    const rows = await requirePool().query<{ payload: unknown }>(`
      select jsonb_agg(payload) as payload from (
        select to_jsonb(r) as payload from agent_observability_mutation_reservations r
        union all select to_jsonb(s) from agent_observability_credential_staging_intents s
        union all select to_jsonb(o) from agent_observability_secret_cleanup_outbox o
        union all select to_jsonb(i) from agent_observability_idempotency_keys i
        union all select to_jsonb(a) from admin_audit_events a
      ) collected
    `);
    expect(JSON.stringify(rows.rows[0]?.payload ?? [])).not.toContain(rawSecretFixture);
  });

  function requireDb(): DbClient {
    if (!db) throw new Error('mutation test database is not initialized');
    return db;
  }

  function requirePool(): Pool {
    if (!pool) throw new Error('mutation test pool is not initialized');
    return pool;
  }
});

function versions(credentialVersion: number | null): AgentObservabilityExpectedVersions {
  return {
    stateVersion: '"obs-state-v1"',
    configVersion: 1,
    credentialVersion,
  };
}

function idempotency(organizationId: string, key: string) {
  return {
    organizationId,
    principal: 'admin-observability-test',
    scope: 'PUT /admin/agent_observability',
    key,
  };
}

function audit(action: string) {
  return {
    action: `agent_observability.${action}`,
    authMethod: 'admin_api_key',
    requestId: `req_${action}`,
  };
}

function bindingTarget(
  organizationId: string,
  bindingId: string,
): AgentObservabilityMutationTarget {
  return { type: 'binding', organizationId, bindingId, bindingScope: 'organization' };
}

async function acquireReservation(
  db: DbClient,
  input: {
    target: AgentObservabilityMutationTarget;
    key: string;
    body: unknown;
    expectedVersions: AgentObservabilityExpectedVersions;
    staging?: {
      secretRef: AgentObservabilitySecretReference;
      candidateBindingId: string;
      proposedCredentialVersion: number;
    };
    now?: Date;
    ttlMs?: number;
    idempotencyTtlMs?: number;
    stagingCleanupGraceMs?: number;
  },
): Promise<AgentObservabilityReservedMutation> {
  const request: AcquireAgentObservabilityMutationReservationInput = {
    target: input.target,
    idempotency: idempotency(input.target.organizationId, input.key),
    bodyHash: agentObservabilityMutationBodyHash(input.body),
    expectedVersions: input.expectedVersions,
  };
  if (input.staging !== undefined) request.staging = input.staging;
  if (input.now !== undefined) request.now = input.now;
  if (input.ttlMs !== undefined) request.ttlMs = input.ttlMs;
  if (input.idempotencyTtlMs !== undefined) request.idempotencyTtlMs = input.idempotencyTtlMs;
  if (input.stagingCleanupGraceMs !== undefined) {
    request.stagingCleanupGraceMs = input.stagingCleanupGraceMs;
  }
  const result = await withAuthority(db, input.target, (tx) =>
    acquireAgentObservabilityMutationReservation(tx, request),
  );
  if (result.kind !== 'acquired')
    throw new Error(`expected acquired reservation, got ${result.kind}`);
  return result.reservation;
}

async function stageWritten(
  db: DbClient,
  reservation: AgentObservabilityReservedMutation,
  store: LocalSecretStore,
  now = new Date(),
): Promise<void> {
  const claim = await claimAgentObservabilityStagingWrite(db, { reservation, now });
  if (!claim) throw new Error('expected staging writer claim');
  const intent = (
    await db
      .select()
      .from(schema.agentObservabilityCredentialStagingIntents)
      .where(
        eq(
          schema.agentObservabilityCredentialStagingIntents.reservationId,
          reservation.reservationId,
        ),
      )
  )[0];
  if (!intent) throw new Error('missing staging intent');
  await store.put(
    claim.secretRef,
    encodeAgentObservabilitySecretBundle({
      bindingId: intent.candidateBindingId,
      credentialVersion: intent.proposedCredentialVersion,
      adapterType: 'otlp_http',
      auth: {
        type: 'bearer',
        token: `raw-observability-secret-${reservation.reservationId}`,
      },
    }),
  );
  expect(
    await completeAgentObservabilityStagingWrite(db, {
      reservation,
      writerToken: claim.writerToken,
      outcome: 'written',
      now,
    }),
  ).toEqual({ kind: 'written' });
}

async function finalizeBindingRotation(
  db: DbClient,
  reservation: AgentObservabilityReservedMutation,
  target: AgentObservabilityMutationTarget,
  oldRef: AgentObservabilitySecretReference,
  action: string,
  now?: Date,
) {
  if (target.type !== 'binding') throw new Error('binding target required');
  const input = {
    reservation,
    expectedVersions: reservation.expectedVersions,
    supersededSecretRef: oldRef,
    responseStatus: 200,
    audit: audit(action),
    apply: async ({ db: applyDb, stagedActivation }: AgentObservabilityMutationApplyInput) => {
      if (!stagedActivation) return { applied: false };
      const updated = await applyDb
        .update(schema.agentObservabilityBindingCredentials)
        .set({
          secretRef: stagedActivation.secretRef,
          credentialVersion: stagedActivation.credentialVersion,
          updatedBy: 'integration-test',
          updatedAt: new Date(),
        })
        .where(
          and(
            eq(schema.agentObservabilityBindingCredentials.bindingId, target.bindingId),
            eq(
              schema.agentObservabilityBindingCredentials.credentialVersion,
              reservation.expectedVersions.credentialVersion!,
            ),
          ),
        )
        .returning({ bindingId: schema.agentObservabilityBindingCredentials.bindingId });
      return { applied: updated.length === 1 };
    },
  };
  if (now !== undefined) {
    return db.transaction((tx) =>
      finalizeAgentObservabilityMutation(tx as DbTransaction, { ...input, now }),
    );
  }
  return db.transaction((tx) => finalizeAgentObservabilityMutation(tx as DbTransaction, input));
}

async function fence(
  db: DbClient,
  reservation: AgentObservabilityReservedMutation,
  now?: Date,
): Promise<void> {
  const reference = now === undefined ? reservation : { ...reservation, now };
  expect(
    await db.transaction((tx) =>
      fenceAgentObservabilityMutationReservation(tx as DbTransaction, reference),
    ),
  ).toBe(true);
}

async function configureOrganizationDefaultBinding(
  db: DbClient,
  organizationId: string,
  bindingId: string,
): Promise<void> {
  await db
    .update(schema.agentObservabilityOrganizationSettings)
    .set({
      activeDefaultBindingId: bindingId,
      activeDefaultBindingScope: 'organization',
      selectionEpoch: 1,
      updatedAt: new Date(),
    })
    .where(eq(schema.agentObservabilityOrganizationSettings.organizationId, organizationId));
}

async function expectCandidateSelectionRollback(
  db: DbClient,
  reservation: AgentObservabilityReservedMutation,
  candidateBindingId: string,
  requestId: string,
): Promise<void> {
  const [binding, idempotency] = await Promise.all([
    db
      .select({ id: schema.agentObservabilityBindings.id })
      .from(schema.agentObservabilityBindings)
      .where(eq(schema.agentObservabilityBindings.id, candidateBindingId)),
    db
      .select({
        status: schema.agentObservabilityIdempotencyKeys.status,
        responseBody: schema.agentObservabilityIdempotencyKeys.responseBody,
      })
      .from(schema.agentObservabilityIdempotencyKeys)
      .where(eq(schema.agentObservabilityIdempotencyKeys.reservationId, reservation.reservationId)),
  ]);
  expect(binding).toHaveLength(0);
  expect(idempotency).toEqual([{ status: 'pending', responseBody: null }]);
  const auditRows = await db
    .select({ id: schema.adminAuditEvents.id })
    .from(schema.adminAuditEvents)
    .where(eq(schema.adminAuditEvents.requestId, requestId));
  expect(auditRows).toHaveLength(0);
}

async function seedOrganization(
  db: DbClient,
  organizationId: string,
  workspaceId: string,
): Promise<void> {
  const now = new Date();
  await db
    .insert(schema.organizations)
    .values({ id: organizationId, name: organizationId, status: 'active' });
  await db
    .insert(schema.agentObservabilityOrganizationSettings)
    .values(newOrganizationObservabilitySettings(organizationId, now))
    .onConflictDoNothing();
  await db.insert(schema.workspaces).values({
    id: workspaceId,
    organizationId,
    name: workspaceId,
    status: 'active',
    createdBy: 'integration-test',
  });
  await db
    .insert(schema.agentObservabilityWorkspaceSettings)
    .values(newWorkspaceObservabilitySettings(organizationId, workspaceId, now))
    .onConflictDoNothing();
}

async function createBinding(
  db: DbClient,
  organizationId: string,
  workspaceId: string | null,
  secretRef = newAgentObservabilitySecretReference(),
  credentialVersion = 1,
): Promise<{ id: string; secretRef: AgentObservabilitySecretReference }> {
  const id = newId('aob');
  await db.transaction((tx) =>
    insertBindingGraph(tx as DbTransaction, {
      id,
      organizationId,
      workspaceId,
      secretRef,
      credentialVersion,
    }),
  );
  return { id, secretRef };
}

async function enqueueSecretCleanup(
  db: DbClient,
  organizationId: string,
  bindingId: string,
  secretRef: AgentObservabilitySecretReference,
  now: Date,
): Promise<string> {
  const id = newId('aomclean');
  await db.insert(schema.agentObservabilitySecretCleanupOutbox).values({
    id,
    organizationId,
    bindingId,
    bindingScope: 'organization',
    secretRef,
    status: 'pending',
    claimToken: null,
    leaseExpiresAt: null,
    attemptCount: 0,
    nextAttemptAt: now,
    createdAt: now,
    updatedAt: now,
  });
  return id;
}

async function insertBindingGraph(
  db: Pick<DbTransaction, 'insert'>,
  input: {
    id: string;
    organizationId: string;
    workspaceId: string | null;
    secretRef: AgentObservabilitySecretReference;
    credentialVersion: number;
  },
): Promise<void> {
  const now = new Date();
  await db.insert(schema.agentObservabilityBindings).values({
    id: input.id,
    organizationId: input.organizationId,
    workspaceId: input.workspaceId,
    scopeType: input.workspaceId === null ? 'organization' : 'workspace',
    adapterType: 'otlp_http',
    endpointKind: 'traces_endpoint',
    endpointClass: 'public',
    endpoint: 'https://collector.example/v1/traces',
    externalProjectId: null,
    currentVersion: 1,
    status: 'active',
    revocationEpoch: 0,
    createdBy: 'integration-test',
    updatedBy: 'integration-test',
    archivedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(schema.agentObservabilityBindingVersions).values({
    bindingId: input.id,
    version: 1,
    adapterType: 'otlp_http',
    semanticProfile: 'otel_genai',
    protocol: 'http/protobuf',
    compression: 'none',
    timeoutMs: 5_000,
    environment: null,
    release: null,
    captureMode: 'metadata_only',
    sampleRate: '1',
    configSchemaVersion: 1,
    createdBy: 'integration-test',
    createdAt: now,
  });
  await db.insert(schema.agentObservabilityBindingCredentials).values({
    bindingId: input.id,
    secretRef: input.secretRef,
    credentialVersion: input.credentialVersion,
    keyHint: null,
    rotatedAt: now,
    updatedBy: 'integration-test',
    createdAt: now,
    updatedAt: now,
  });
}

async function withAuthority<T>(
  db: DbClient,
  target: AgentObservabilityMutationTarget,
  callback: (tx: DbTransaction) => Promise<T>,
): Promise<T> {
  return db.transaction(async (tx) => {
    if (target.type === 'organization_setting') {
      await tx
        .select({ organizationId: schema.agentObservabilityOrganizationSettings.organizationId })
        .from(schema.agentObservabilityOrganizationSettings)
        .where(
          eq(schema.agentObservabilityOrganizationSettings.organizationId, target.organizationId),
        )
        .for('update')
        .limit(1);
    } else if (target.type === 'workspace_setting') {
      await tx
        .select({ workspaceId: schema.agentObservabilityWorkspaceSettings.workspaceId })
        .from(schema.agentObservabilityWorkspaceSettings)
        .where(
          and(
            eq(schema.agentObservabilityWorkspaceSettings.organizationId, target.organizationId),
            eq(schema.agentObservabilityWorkspaceSettings.workspaceId, target.workspaceId),
          ),
        )
        .for('update')
        .limit(1);
    } else {
      await tx
        .select({ id: schema.agentObservabilityBindings.id })
        .from(schema.agentObservabilityBindings)
        .where(
          and(
            eq(schema.agentObservabilityBindings.organizationId, target.organizationId),
            eq(schema.agentObservabilityBindings.id, target.bindingId),
          ),
        )
        .for('update')
        .limit(1);
    }
    return callback(tx as DbTransaction);
  });
}

class HangingDeleteSecretStore extends LocalSecretStore {
  readonly started = deferred<void>();
  readonly deleteResult = deferred<void>();
  readonly deleteSettled = deferred<void>();
  deleteCalls = 0;
  signal: AbortSignal | undefined;

  override delete(_reference: string, options?: { signal?: AbortSignal }): Promise<void> {
    this.deleteCalls += 1;
    this.signal = options?.signal;
    this.started.resolve();
    void this.deleteResult.promise.then(
      () => this.deleteSettled.resolve(),
      () => this.deleteSettled.resolve(),
    );
    return this.deleteResult.promise;
  }
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((next, fail) => {
    resolve = next;
    reject = fail;
  });
  return { promise, resolve, reject };
}
