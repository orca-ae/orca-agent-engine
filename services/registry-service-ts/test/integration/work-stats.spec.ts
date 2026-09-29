// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterEach, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import type { AddressInfo } from 'node:net';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildApp } from '../../src/server.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  environments,
  environmentClaims,
  sessions,
} from '../../src/persistence/postgres/schema.js';
import { newId } from '../../src/domain/versioning.js';
import {
  DISTRIBUTION_PENDING,
  DISTRIBUTION_ASSIGNED,
  DISTRIBUTION_FAILED,
} from '../../src/tunnel/session-distributor.js';
import { EnvironmentClaimStore } from '../../src/domain/environment-claims.js';

/**
 * End-to-end coverage of the public work-queue-stats route
 * (`GET /v1/environments/:id/work_stats`). The pure partition + claim-liveness
 * logic is unit-tested in `test/unit/work-stats.spec.ts`; this asserts the HTTP
 * request path that makes it reachable in production: workspace-scoped auth, the
 * Drizzle counting over the `sessions` table keyed on `distribution_state` +
 * `runner_id`, and the `environment_claims` liveness wiring. The app uses a short
 * claim TTL so a back-dated heartbeat reads as a disconnected worker.
 */

const CLAIM_TTL_MS = 60_000;

describe('GET /v1/environments/:id/work_stats (integration)', () => {
  let app: FastifyInstance;
  let baseURL: string;
  let db: DbClient;
  let apiKey: string;
  let workspaceId: string;
  let agentId: string;
  let claims: EnvironmentClaimStore;

  beforeAll(async () => {
    ({ db } = await getTestDb());
    claims = new EnvironmentClaimStore(db);
    app = buildApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      environmentClaimTtlMs: CLAIM_TTL_MS,
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    baseURL = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
    workspaceId = uniqueWorkspace('work_stats');
    apiKey = await createTestApiKey(db, workspaceId);
    agentId = await createAgentRow(db, workspaceId);
  }, 30000);

  afterEach(async () => {
    await db.delete(environmentClaims);
  });

  afterAll(async () => {
    if (app) await app.close();
    await closeTestDb();
  });

  async function createEnvironment(
    target: 'self_hosted' | 'cloud' = 'self_hosted',
  ): Promise<string> {
    const id = newId('env');
    const now = new Date();
    await db.insert(environments).values({
      id,
      workspaceId,
      name: `ws-env-${id}`,
      target,
      createdAt: now,
      updatedAt: now,
    });
    return id;
  }

  /**
   * Insert a session row directly in a chosen distribution state for an
   * environment, bypassing the create path so the test can pin every bucket
   * (the create path only ever leaves a freshly-created session PENDING).
   */
  async function insertSession(args: {
    environmentId: string | null;
    distributionState: string | null;
    runnerId?: string | null;
    archived?: boolean;
    ws?: string;
    // `sessions_workspace_agent_version_fk` is composite, so a session in
    // another workspace must reference THAT workspace's own agent — the schema
    // makes a cross-workspace agent reference unconstructible.
    agent?: string;
  }): Promise<string> {
    const id = newId('ses');
    const now = new Date();
    await db.insert(sessions).values({
      id,
      workspaceId: args.ws ?? workspaceId,
      agentId: args.agent ?? agentId,
      agentVersion: 1,
      environmentId: args.environmentId,
      status: 'idle',
      runnerId: args.runnerId ?? null,
      hostEnvironmentId: args.environmentId,
      distributionState: args.distributionState,
      archivedAt: args.archived ? now : null,
      createdAt: now,
      updatedAt: now,
    });
    return id;
  }

  function getStats(id: string, key = apiKey): Promise<Response> {
    return fetch(`${baseURL}/v1/environments/${id}/work_stats`, {
      headers: { 'x-api-key': key },
    });
  }

  interface WorkStatsBody {
    depth: number;
    in_flight: number;
    worker_connected: boolean;
  }

  it('reports an empty queue with no connected worker for a fresh environment', async () => {
    const envId = await createEnvironment();
    const res = await getStats(envId);
    expect(res.status).toBe(200);
    expect((await res.json()) as WorkStatsBody).toEqual({
      depth: 0,
      in_flight: 0,
      worker_connected: false,
    });
  });

  it('counts pending-unassigned sessions as depth', async () => {
    const envId = await createEnvironment();
    await insertSession({ environmentId: envId, distributionState: DISTRIBUTION_PENDING });
    await insertSession({ environmentId: envId, distributionState: DISTRIBUTION_PENDING });
    const body = (await (await getStats(envId)).json()) as WorkStatsBody;
    expect(body.depth).toBe(2);
    expect(body.in_flight).toBe(0);
  });

  it('counts pending-with-runner (launch in flight) and assigned sessions as in_flight', async () => {
    const envId = await createEnvironment();
    // PENDING with a runner binding — a launch is in flight, a worker picked it up.
    await insertSession({
      environmentId: envId,
      distributionState: DISTRIBUTION_PENDING,
      runnerId: 'runner_launch_1',
    });
    // ASSIGNED — the runner connected.
    await insertSession({
      environmentId: envId,
      distributionState: DISTRIBUTION_ASSIGNED,
      runnerId: 'runner_conn_1',
    });
    const body = (await (await getStats(envId)).json()) as WorkStatsBody;
    expect(body.depth).toBe(0);
    expect(body.in_flight).toBe(2);
  });

  it('partitions a mixed queue and ignores failed + cloud + archived sessions', async () => {
    const envId = await createEnvironment();
    // depth: two pending-unassigned.
    await insertSession({ environmentId: envId, distributionState: DISTRIBUTION_PENDING });
    await insertSession({ environmentId: envId, distributionState: DISTRIBUTION_PENDING });
    // in_flight: one launch-in-flight + one assigned.
    await insertSession({
      environmentId: envId,
      distributionState: DISTRIBUTION_PENDING,
      runnerId: 'runner_a',
    });
    await insertSession({
      environmentId: envId,
      distributionState: DISTRIBUTION_ASSIGNED,
      runnerId: 'runner_b',
    });
    // Excluded: FAILED is terminal.
    await insertSession({
      environmentId: envId,
      distributionState: DISTRIBUTION_FAILED,
      runnerId: 'runner_dead',
    });
    // Excluded: a never-distributed (cloud) session for this env has null state.
    await insertSession({ environmentId: envId, distributionState: null });
    // Excluded: an archived pending session is not live work.
    await insertSession({
      environmentId: envId,
      distributionState: DISTRIBUTION_PENDING,
      archived: true,
    });

    const body = (await (await getStats(envId)).json()) as WorkStatsBody;
    expect(body.depth).toBe(2);
    expect(body.in_flight).toBe(2);
  });

  it('scopes counts to the requested environment (a sibling env does not bleed in)', async () => {
    const envA = await createEnvironment();
    const envB = await createEnvironment();
    await insertSession({ environmentId: envA, distributionState: DISTRIBUTION_PENDING });
    await insertSession({ environmentId: envB, distributionState: DISTRIBUTION_PENDING });
    await insertSession({ environmentId: envB, distributionState: DISTRIBUTION_PENDING });
    const body = (await (await getStats(envA)).json()) as WorkStatsBody;
    expect(body.depth).toBe(1);
  });

  it('reports worker_connected true while the claim heartbeat is live, false once stale', async () => {
    const envId = await createEnvironment();
    // A fresh claim (last_ping == now) → connected.
    await claims.claim(envId, 'registry-0', 'conn-1');
    let body = (await (await getStats(envId)).json()) as WorkStatsBody;
    expect(body.worker_connected).toBe(true);

    // Back-date the heartbeat past the app's TTL → stale → disconnected.
    await claims.heartbeat(envId, 'conn-1', new Date(Date.now() - CLAIM_TTL_MS - 5_000));
    body = (await (await getStats(envId)).json()) as WorkStatsBody;
    expect(body.worker_connected).toBe(false);
  });

  it('reflects depth and worker_connected together (a worker connected to a non-empty queue)', async () => {
    const envId = await createEnvironment();
    await insertSession({ environmentId: envId, distributionState: DISTRIBUTION_PENDING });
    await claims.claim(envId, 'registry-0', 'conn-1');
    const body = (await (await getStats(envId)).json()) as WorkStatsBody;
    expect(body).toEqual({ depth: 1, in_flight: 0, worker_connected: true });
  });

  it('404s for an unknown environment id', async () => {
    const res = await getStats('env_doesnotexist');
    expect(res.status).toBe(404);
    expect(((await res.json()) as { error: { message: string } }).error.message).toMatch(
      /not found/i,
    );
  });

  it('404s for an environment owned by a different workspace (no cross-tenant read)', async () => {
    const envId = await createEnvironment();
    const otherWs = uniqueWorkspace('work_stats_other');
    const otherKey = await createTestApiKey(db, otherWs);
    const res = await getStats(envId, otherKey);
    expect(res.status).toBe(404);
  });

  it('401s without an api key (workspace-scoped auth)', async () => {
    const envId = await createEnvironment();
    const res = await fetch(`${baseURL}/v1/environments/${envId}/work_stats`);
    expect(res.status).toBe(401);
  });

  it('does not count another workspace’s queued sessions', async () => {
    // This originally seeded a session in workspace B pointing at workspace A's
    // environment. `sessions_workspace_environment_fk` (migration 0024) is
    // composite over `(workspace_id, environment_id)`, so that row cannot be
    // inserted at all — the same class of unconstructible fixture 60f8f6a0
    // removed from `snapshot-loader.spec.ts`. The behaviour it meant to pin is
    // real, so it is asserted the way the schema permits: the other tenant's
    // queue lives at the other tenant's own environment, and neither key sees
    // the other's depth. The positive control (each side sees its own 1) is
    // what makes the zero a scoping result rather than an empty database.
    const envId = await createEnvironment();
    await insertSession({ environmentId: envId, distributionState: DISTRIBUTION_PENDING });

    const otherWs = uniqueWorkspace('work_stats_leak');
    const otherKey = await createTestApiKey(db, otherWs);
    const otherAgentId = await createAgentRow(db, otherWs);
    const otherEnvId = newId('env');
    const now = new Date();
    await db.insert(environments).values({
      id: otherEnvId,
      workspaceId: otherWs,
      name: `ws-env-${otherEnvId}`,
      target: 'self_hosted',
      createdAt: now,
      updatedAt: now,
    });
    await insertSession({
      environmentId: otherEnvId,
      distributionState: DISTRIBUTION_PENDING,
      ws: otherWs,
      agent: otherAgentId,
    });

    // Each workspace sees exactly its own one queued session, never the sum.
    expect(((await (await getStats(envId)).json()) as WorkStatsBody).depth).toBe(1);
    expect(((await (await getStats(otherEnvId, otherKey)).json()) as WorkStatsBody).depth).toBe(1);
    // And neither environment is even readable from the other's key.
    expect((await getStats(otherEnvId)).status).toBe(404);
    expect((await getStats(envId, otherKey)).status).toBe(404);
  });

  it('404s on a malformed environment id (mirrors GET /v1/environments/:id; the contract has no 400)', async () => {
    // The published contract declares only 200 / 404 for this route, and the
    // sibling environment reads 404 on a not-found id rather than 400 — a
    // malformed id is simply an environment that does not exist in the workspace.
    const res = await getStats('not-an-env-id');
    expect(res.status).toBe(404);
  });
});

/**
 * Insert a minimal agent row **and its v1 snapshot** so sessions can satisfy
 * both FKs the schema puts on them: `agents_workspace_fk`
 * (`agents.workspace_id` -> `workspaces.id`, migration 0025) and
 * `sessions_workspace_agent_version_fk`
 * (`sessions(workspace_id, agent_id, agent_version)` ->
 * `agent_versions(workspace_id, agent_id, version)`, migration 0024). The
 * caller must have seeded the workspace first (`createTestWorkspace`).
 */
async function createAgentRow(db: DbClient, workspaceId: string): Promise<string> {
  const id = newId('agt');
  const now = new Date();
  await db.insert(agents).values({
    id,
    workspaceId,
    name: `work-stats-agent-${id}`,
    modelProvider: 'anthropic',
    modelId: 'claude-3-5-sonnet-20240620',
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(agentVersions).values({
    id: newId('agtv'),
    workspaceId,
    agentId: id,
    version: 1,
    snapshot: {
      id,
      name: `work-stats-agent-${id}`,
      version: 1,
      model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
      system: null,
      tools: [],
      mcp_servers: [],
      skills: [],
      metadata: {},
      multiagent: null,
    },
    createdAt: now,
  });
  return id;
}
