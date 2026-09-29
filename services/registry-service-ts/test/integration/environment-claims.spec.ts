// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterEach, afterAll } from 'vitest';
import { eq } from 'drizzle-orm';
import { getTestDb, closeTestDb } from './setup.js';
import { uniqueWorkspace, createTestWorkspace } from './fixtures.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { environments, environmentClaims } from '../../src/persistence/postgres/schema.js';
import { newId } from '../../src/domain/versioning.js';
import { EnvironmentClaimStore, isClaimStale } from '../../src/domain/environment-claims.js';

/**
 * Durable-claim behavior against the real Postgres schema: exclusivity (one row
 * per environment), newest-wins replacement, the heartbeat connection guard,
 * staleness measured over a persisted `last_ping`, connection-scoped release
 * (including the takeover-then-release race where a superseded worker must NOT
 * delete the new live owner), the unconditional operator release, the bulk
 * staleness reaper, and the FK cascade. The pure decision logic is covered in
 * the unit suite; this asserts the Drizzle wrappers and the schema invariants
 * that only a DB can prove.
 */

describe('Environment claims durable model (integration)', () => {
  let db: DbClient;
  let store: EnvironmentClaimStore;
  let workspaceId: string;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    store = new EnvironmentClaimStore(db);
    workspaceId = uniqueWorkspace('env_claims');
    // `environments.workspace_id` is a real FK. This spec talks to the DB
    // directly (no HTTP path, so no `createTestApiKey` to seed for it), so the
    // workspace row has to exist before the first `createEnvironment`.
    await createTestWorkspace(db, workspaceId);
  });

  // `reapStale` is a GLOBAL bulk delete (`DELETE FROM environment_claims WHERE
  // last_ping < now - ttl`) by design — it sweeps every replica's abandoned
  // claims, not just one environment's. Tests in this file therefore share the
  // `environment_claims` table: a claim left behind by one test sits in the
  // sweep window of another, so the table-wide `reaped` count is only
  // deterministic when each test starts from an empty claims table. Clear it
  // after every test (the parent `environments` rows are harmless — each test
  // mints a fresh unique env id — and the FK cascades anyway).
  afterEach(async () => {
    await db.delete(environmentClaims);
  });

  afterAll(async () => {
    await closeTestDb();
  });

  /** Insert a parent environment the claim can FK to, returning its id. */
  async function createEnvironment(): Promise<string> {
    const id = newId('env');
    const now = new Date();
    await db.insert(environments).values({
      id,
      workspaceId,
      name: `claim-env-${id}`,
      createdAt: now,
      updatedAt: now,
    });
    return id;
  }

  it('claims an unclaimed environment and getOwner reads it back', async () => {
    const envId = await createEnvironment();
    const claim = await store.claim(envId, 'registry-0', 'conn-1');
    expect(claim.environmentId).toBe(envId);
    expect(claim.ownerPod).toBe('registry-0');
    expect(claim.workerConnId).toBe('conn-1');
    // A fresh claim stamps both timestamps to the same instant.
    expect(claim.lastPing.getTime()).toBe(claim.claimedAt.getTime());

    const owner = await store.getOwner(envId);
    expect(owner).not.toBeNull();
    expect(owner?.ownerPod).toBe('registry-0');
    expect(owner?.workerConnId).toBe('conn-1');
  });

  it('getOwner returns null for an unclaimed environment', async () => {
    const envId = await createEnvironment();
    expect(await store.getOwner(envId)).toBeNull();
  });

  it('enforces exclusivity: a second claim replaces the first (newest-wins), one row total', async () => {
    const envId = await createEnvironment();
    await store.claim(envId, 'registry-0', 'conn-1');
    // A relocated worker on a different pod + connection takes over.
    const replaced = await store.claim(envId, 'registry-1', 'conn-2');
    expect(replaced.ownerPod).toBe('registry-1');
    expect(replaced.workerConnId).toBe('conn-2');

    const owner = await store.getOwner(envId);
    expect(owner?.ownerPod).toBe('registry-1');
    expect(owner?.workerConnId).toBe('conn-2');

    // Exclusive: exactly one claim row exists for this environment.
    const rows = await db
      .select()
      .from(environmentClaims)
      .where(eq(environmentClaims.environmentId, envId));
    expect(rows.length).toBe(1);
  });

  it('re-claiming refreshes claimed_at on the same environment', async () => {
    const envId = await createEnvironment();
    const first = await store.claim(
      envId,
      'registry-0',
      'conn-1',
      new Date('2026-06-18T00:00:00.000Z'),
    );
    const second = await store.claim(
      envId,
      'registry-0',
      'conn-1',
      new Date('2026-06-18T00:05:00.000Z'),
    );
    expect(second.claimedAt.getTime()).toBeGreaterThan(first.claimedAt.getTime());
    const owner = await store.getOwner(envId);
    expect(owner?.claimedAt.getTime()).toBe(second.claimedAt.getTime());
  });

  it('heartbeat advances last_ping for the owning connection', async () => {
    const envId = await createEnvironment();
    const claimedAt = new Date('2026-06-18T00:00:00.000Z');
    await store.claim(envId, 'registry-0', 'conn-1', claimedAt);

    const pingedAt = new Date('2026-06-18T00:00:20.000Z');
    const ok = await store.heartbeat(envId, 'conn-1', pingedAt);
    expect(ok).toBe(true);

    const owner = await store.getOwner(envId);
    expect(owner?.lastPing.getTime()).toBe(pingedAt.getTime());
    // claimed_at is untouched by a heartbeat.
    expect(owner?.claimedAt.getTime()).toBe(claimedAt.getTime());
  });

  it('heartbeat from a stale (taken-over) connection is a no-op', async () => {
    const envId = await createEnvironment();
    await store.claim(envId, 'registry-0', 'conn-1', new Date('2026-06-18T00:00:00.000Z'));
    // Newest-wins: a new connection takes the claim.
    const takenOverAt = new Date('2026-06-18T00:00:10.000Z');
    await store.claim(envId, 'registry-1', 'conn-2', takenOverAt);

    // The old connection's heartbeat must not resurrect it.
    const ok = await store.heartbeat(envId, 'conn-1', new Date('2026-06-18T00:00:30.000Z'));
    expect(ok).toBe(false);

    const owner = await store.getOwner(envId);
    expect(owner?.workerConnId).toBe('conn-2');
    // last_ping stays at the takeover instant — the stale ping was rejected.
    expect(owner?.lastPing.getTime()).toBe(takenOverAt.getTime());
  });

  it('heartbeat on an unclaimed environment returns false', async () => {
    const envId = await createEnvironment();
    expect(await store.heartbeat(envId, 'conn-1')).toBe(false);
  });

  it('isClaimStale reflects the persisted last_ping across a heartbeat', async () => {
    const envId = await createEnvironment();
    const ttlMs = 30_000;
    await store.claim(envId, 'registry-0', 'conn-1', new Date('2026-06-18T00:00:00.000Z'));

    const before = await store.getOwner(envId);
    // 31s after the claim with no heartbeat — stale.
    expect(isClaimStale(before!, ttlMs, new Date('2026-06-18T00:00:31.000Z'))).toBe(true);

    await store.heartbeat(envId, 'conn-1', new Date('2026-06-18T00:00:31.000Z'));
    const after = await store.getOwner(envId);
    // Heartbeat moved the watermark forward — no longer stale at the same instant.
    expect(isClaimStale(after!, ttlMs, new Date('2026-06-18T00:00:31.000Z'))).toBe(false);
  });

  it('release drops the claim (connection-scoped) and is idempotent', async () => {
    const envId = await createEnvironment();
    await store.claim(envId, 'registry-0', 'conn-1');
    // The owning connection releases its own claim.
    expect(await store.release(envId, 'conn-1')).toBe(true);
    expect(await store.getOwner(envId)).toBeNull();
    // Releasing again (or an unclaimed env) is a safe no-op.
    expect(await store.release(envId, 'conn-1')).toBe(false);
  });

  it('release is connection-scoped: a non-owning connection cannot release', async () => {
    const envId = await createEnvironment();
    await store.claim(envId, 'registry-0', 'conn-1');
    // A different connection's release must not touch this claim.
    expect(await store.release(envId, 'conn-other')).toBe(false);
    const owner = await store.getOwner(envId);
    expect(owner?.workerConnId).toBe('conn-1');
  });

  it('takeover-then-release: a superseded worker cannot delete the new live owner (one-claim invariant)', async () => {
    const envId = await createEnvironment();
    // Worker A claims, then worker B takes over via newest-wins.
    await store.claim(envId, 'registry-0', 'conn-1', new Date('2026-06-18T00:00:00.000Z'));
    await store.claim(envId, 'registry-1', 'conn-2', new Date('2026-06-18T00:00:10.000Z'));

    // Worker A's teardown now releases — connection-scoped, so it is a no-op and
    // must NOT delete worker B's live claim.
    expect(await store.release(envId, 'conn-1')).toBe(false);

    // B still owns the environment: the invariant holds from B's perspective.
    const owner = await store.getOwner(envId);
    expect(owner).not.toBeNull();
    expect(owner?.ownerPod).toBe('registry-1');
    expect(owner?.workerConnId).toBe('conn-2');

    // And B can still release its own claim.
    expect(await store.release(envId, 'conn-2')).toBe(true);
    expect(await store.getOwner(envId)).toBeNull();
  });

  it('releaseUnconditional drops any connection’s claim (operator escape hatch)', async () => {
    const envId = await createEnvironment();
    await store.claim(envId, 'registry-0', 'conn-1');
    // Operator path ignores ownership — wipes whatever holds the environment.
    expect(await store.releaseUnconditional(envId)).toBe(true);
    expect(await store.getOwner(envId)).toBeNull();
    expect(await store.releaseUnconditional(envId)).toBe(false);
  });

  it('reapStale bulk-deletes only claims past the TTL, keeping fresh + boundary claims', async () => {
    const ttlMs = 30_000;
    const sweepAt = new Date('2026-06-18T01:00:00.000Z');

    const staleEnv = await createEnvironment();
    const boundaryEnv = await createEnvironment();
    const freshEnv = await createEnvironment();

    // Stale: last_ping 31s before the sweep (> ttl) → reaped.
    await store.claim(staleEnv, 'registry-0', 'c-stale', new Date('2026-06-18T00:59:29.000Z'));
    // Boundary: last_ping exactly ttl before the sweep (== ttl, not >) → kept.
    await store.claim(boundaryEnv, 'registry-0', 'c-bound', new Date('2026-06-18T00:59:30.000Z'));
    // Fresh: last_ping 5s before the sweep → kept.
    await store.claim(freshEnv, 'registry-0', 'c-fresh', new Date('2026-06-18T00:59:55.000Z'));

    // Exactly one row swept: only `c-stale` is past the TTL. This count is
    // table-wide (reapStale is a global delete), so it holds because afterEach
    // clears claims between tests — the per-env outcomes below are what pin the
    // boundary semantics regardless of the count.
    const reaped = await store.reapStale(ttlMs, sweepAt);
    expect(reaped).toBe(1);

    expect(await store.getOwner(staleEnv)).toBeNull();
    expect(await store.getOwner(boundaryEnv)).not.toBeNull();
    expect(await store.getOwner(freshEnv)).not.toBeNull();
  });

  it('reapStale spares a claim whose heartbeat moved the watermark forward', async () => {
    const ttlMs = 30_000;
    const envId = await createEnvironment();
    await store.claim(envId, 'registry-0', 'conn-1', new Date('2026-06-18T00:00:00.000Z'));
    // A live heartbeat just before the sweep keeps it fresh.
    await store.heartbeat(envId, 'conn-1', new Date('2026-06-18T00:00:50.000Z'));

    const reaped = await store.reapStale(ttlMs, new Date('2026-06-18T00:01:00.000Z'));
    expect(reaped).toBe(0);
    expect(await store.getOwner(envId)).not.toBeNull();
  });

  it('cascades the claim away when the environment is deleted', async () => {
    const envId = await createEnvironment();
    await store.claim(envId, 'registry-0', 'conn-1');
    await db.delete(environments).where(eq(environments.id, envId));
    // FK onDelete: cascade removed the orphaned claim.
    expect(await store.getOwner(envId)).toBeNull();
  });
});
