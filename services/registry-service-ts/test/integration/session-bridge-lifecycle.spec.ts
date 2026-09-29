// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Server-boundary lifecycle coverage for the owner-pod session event bridge.
//
//   1. RESOLVER FILTER. `buildBoundSessionResolver` resolves a runner's bound
//      (workspace, session) for the {@link SessionEventBridgeManager}, but EXCLUDES
//      a distribution_state=`failed` binding so the manager no-ops for it.
//   2. COMPOSED CONNECT HOOK. server.ts wires the runner-connect hook as the
//      distributor's `onRunnerConnect` FIRST (which flips the bound session
//      ASSIGNED / FAILED by the connect-time capability-match), THEN the bridge
//      manager's `onRunnerConnect`. When a runner connects whose advertised
//      harnesses EXCLUDE the session's provider, the distributor flips the session
//      FAILED; the manager (over the real resolver) must then start NO bridge and
//      deliver NO snapshot. A capable runner is the positive control: its session
//      flips ASSIGNED and exactly one bridge starts + one snapshot is delivered.
//      Reverting the resolver's FAILED filter makes the failed session resolve,
//      starting a redundant bridge + snapshot — these assertions then fail.
//   3. SHUTDOWN DRAIN. `buildApp` registers an `onClose` hook that calls the
//      manager's `stopAll`, so the SIGINT/SIGTERM shutdown (main.ts `app.close()`)
//      stops every live bridge instead of leaking it past process teardown.
//
// DB-touching (the resolvers read the sessions/agents tables), so it lives under
// test/integration and is gated on the dev Postgres stack. The pure distributor +
// bridge-manager decision logic is unit-tested in
// test/unit/session-distributor.spec.ts and test/unit/session-event-bridge.spec.ts;
// this spec proves the SERVER-level composition + the real Drizzle-backed resolver.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { FrameKind, decodeFrame, type Frame, type HelloFrame } from '@orca/harness-tunnel';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestWorkspace } from './fixtures.js';
import { buildApp, getSessionEventBridges, getTunnelRegistry } from '../../src/server.js';
import {
  buildBoundSessionResolver,
  buildDistributionSessionStore,
  buildEnvironmentTargetLookup,
} from '../../src/api/sessions.routes.js';
import {
  SessionDistributor,
  DISTRIBUTION_PENDING,
  DISTRIBUTION_ASSIGNED,
  DISTRIBUTION_FAILED,
} from '../../src/tunnel/session-distributor.js';
import { SessionEventBridgeManager } from '../../src/tunnel/session-event-bridge.js';
import type { SnapshotProvider } from '../../src/tunnel/session-snapshot-delivery.js';
import type { AgentSnapshot } from '../../src/domain/agent-snapshot.js';
import { TunnelRegistry, type RegistryWebSocketLike } from '../../src/tunnel/tunnel-registry.js';
import { WorkerRegistry } from '../../src/tunnel/worker-registry.js';
import {
  agents,
  agentVersions,
  environments,
  sessions,
} from '../../src/persistence/postgres/schema.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { newId } from '../../src/domain/versioning.js';
import { eq } from 'drizzle-orm';
import { runnerResourceBindingIsCurrent } from '../../src/domain/runner-resources-factory.js';

// A runner-tunnel socket the registry never reads from; the registry queues
// outbound frames and the connecting ROUTE drains them, which `connectRunner`
// below stands in for.
const SILENT_RUNNER_WS: RegistryWebSocketLike = {
  sendText: async () => {},
  receiveText: () => new Promise<string>(() => {}),
};

/**
 * Register a runner advertising `harnesses` in its hello, and drain its outbound
 * queue, answering every request frame with an empty `200` the way a real runner
 * acks one.
 *
 * The ack loop is load-bearing, not scenery. `SessionEventBridgeManager` recovers
 * on connect by default (`recoverOnConnect ?? true`, and `server.ts` leaves the
 * default), and `chunkEvents` returns ONE EMPTY CHUNK for an empty transcript —
 * so a connect sends exactly one replay POST even over the stub store, and a
 * socket that never answers it leaves `onRunnerConnect` awaiting the ack forever.
 *
 * Returns a stop function for the drain loop.
 */
function connectRunner(
  registry: TunnelRegistry,
  runnerId: string,
  owner: string,
  harnesses: string[],
): () => void {
  const hello: HelloFrame = {
    kind: FrameKind.Hello,
    runnerVersion: '0.1.0-test',
    frameProtocolVersion: 1,
    harnesses,
    envs: [],
  };
  const session = registry.register(runnerId, SILENT_RUNNER_WS, hello, { owner });
  let stopped = false;
  void (async () => {
    for (;;) {
      const raw = await session.outboundQueue.get();
      if (raw === null || stopped) return;
      let frame: Frame;
      try {
        frame = decodeFrame(raw);
      } catch {
        continue;
      }
      if (frame.kind !== FrameKind.Request) continue;
      registry.routeResponseFrame(runnerId, {
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status: 200,
        headers: [['content-type', 'application/json']],
      });
      registry.routeResponseFrame(runnerId, { kind: FrameKind.ResponseEnd, id: frame.id });
    }
  })();
  return () => {
    stopped = true;
    session.outboundQueue.put(null);
  };
}

/**
 * Snapshot provider spy: records each session id it was asked to resolve and
 * returns `null` (nothing to deliver → delivery is skipped, no tunnel push), so a
 * recorded call means the manager REACHED snapshot delivery for that session.
 */
class CountingSnapshotProvider implements SnapshotProvider {
  readonly calls: string[] = [];
  async resolve(sessionId: string): Promise<AgentSnapshot | null> {
    this.calls.push(sessionId);
    return null;
  }
}

let runnerSeq = 0;
/**
 * A globally-unique runner id. `findBoundSessionId` / `resolveBoundSession` key on
 * `runner_id` across ALL workspaces (no tenant scope), so the seeded binding must
 * not collide with a leftover row from a prior run in the shared dev database.
 */
function uniqueRunnerId(): string {
  return `runner_token_${Date.now().toString(36)}${(runnerSeq++).toString(36)}${Math.random()
    .toString(36)
    .slice(2, 8)}`;
}

/**
 * Seed an agent whose `metadata` harness annotation resolves to a provider.
 *
 * Also seeds the workspace and the agent's v1 snapshot, because the rows this
 * spec inserts are FK-constrained: `agents_workspace_fk` (`agents.workspace_id`
 * -> `workspaces.id`, migration 0025) and, for the sessions seeded below,
 * `sessions_workspace_agent_version_fk` (`(workspace_id, agent_id,
 * agent_version)` -> `agent_versions`, migration 0024). `createTestWorkspace`
 * is idempotent, so calling it per agent is safe for the two-agent test.
 */
async function seedAgent(
  db: DbClient,
  workspaceId: string,
  metadata: Record<string, unknown>,
): Promise<string> {
  await createTestWorkspace(db, workspaceId);
  const id = newId('agt');
  const name = `agent-${id}`;
  await db.insert(agents).values({
    id,
    workspaceId,
    name,
    modelProvider: 'anthropic',
    modelId: 'claude-3-5-sonnet-20240620',
    metadata,
  });
  await db.insert(agentVersions).values({
    id: newId('agtv'),
    workspaceId,
    agentId: id,
    version: 1,
    snapshot: {
      id,
      name,
      version: 1,
      model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
      system: null,
      tools: [],
      mcp_servers: [],
      skills: [],
      metadata,
      multiagent: null,
    },
  });
  return id;
}

/** Seed a session bound to `runnerId` at the given distribution_state (default PENDING). */
async function seedBoundSession(
  db: DbClient,
  workspaceId: string,
  agentId: string,
  runnerId: string,
  distributionState: string = DISTRIBUTION_PENDING,
): Promise<string> {
  const id = newId('ses');
  const environmentId = newId('env');
  await db
    .insert(environments)
    .values({ id: environmentId, workspaceId, name: environmentId, target: 'self_hosted' });
  await db.insert(sessions).values({
    id,
    workspaceId,
    agentId,
    agentVersion: 1,
    environmentId,
    runnerId,
    hostEnvironmentId: null,
    distributionState,
  });
  return id;
}

/** Read a session's current distribution_state. */
async function loadDistributionState(db: DbClient, sessionId: string): Promise<string | null> {
  const rows = await db
    .select({ distributionState: sessions.distributionState })
    .from(sessions)
    .where(eq(sessions.id, sessionId))
    .limit(1);
  return rows[0]?.distributionState ?? null;
}

describe('owner-pod bridge lifecycle at the server boundary (integration)', () => {
  let db: DbClient;

  beforeAll(async () => {
    ({ db } = await getTestDb());
  });
  afterAll(async () => {
    await closeTestDb();
  });

  it('buildBoundSessionResolver resolves PENDING/ASSIGNED bindings but EXCLUDES a FAILED one', async () => {
    const workspaceId = uniqueWorkspace('resolvefilter');
    const agentId = await seedAgent(db, workspaceId, {});
    const pendingRunner = uniqueRunnerId();
    const assignedRunner = uniqueRunnerId();
    const failedRunner = uniqueRunnerId();
    const pendingSession = await seedBoundSession(db, workspaceId, agentId, pendingRunner);
    const assignedSession = await seedBoundSession(
      db,
      workspaceId,
      agentId,
      assignedRunner,
      DISTRIBUTION_ASSIGNED,
    );
    await seedBoundSession(db, workspaceId, agentId, failedRunner, DISTRIBUTION_FAILED);

    const resolver = buildBoundSessionResolver(db);
    expect(await resolver.resolveBoundSession(pendingRunner)).toEqual({
      workspaceId,
      sessionId: pendingSession,
    });
    expect(await resolver.resolveBoundSession(assignedRunner)).toEqual({
      workspaceId,
      sessionId: assignedSession,
    });
    // The FAILED session is filtered out — the manager no-ops for its runner. This
    // is the assertion that fails if the `ne(distributionState, FAILED)` filter is
    // reverted (a FAILED binding would then resolve non-null).
    expect(await resolver.resolveBoundSession(failedRunner)).toBeNull();
  });

  it('composed connect hook: a capability-mismatch FAILED session starts no bridge + no snapshot; a capable one does', async () => {
    const workspaceId = uniqueWorkspace('bridgenoop');
    const store = buildStubStore();

    // metadata {} → the platform default provider 'claude'; { harness:'codex' } →
    // 'codex' (a real annotation the runner below does NOT advertise).
    const claudeAgentId = await seedAgent(db, workspaceId, {});
    const codexAgentId = await seedAgent(db, workspaceId, { harness: 'codex', mode: 'colocated' });

    const capableRunner = uniqueRunnerId();
    const mismatchRunner = uniqueRunnerId();
    const capableSession = await seedBoundSession(db, workspaceId, claudeAgentId, capableRunner);
    const mismatchSession = await seedBoundSession(db, workspaceId, codexAgentId, mismatchRunner);

    // Compose exactly as server.ts wires the runner-connect hook: the distributor's
    // onRunnerConnect FIRST (flips ASSIGNED / FAILED by the connect-time capability
    // match), then the owner-pod bridge manager over the REAL resolvers.
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const snapshotSpy = new CountingSnapshotProvider();
    const distributor = new SessionDistributor({
      sessions: buildDistributionSessionStore(db),
      environments: buildEnvironmentTargetLookup(db),
      tunnelRegistry,
      workerRegistry,
    });
    const bridges = new SessionEventBridgeManager({
      store,
      registry: tunnelRegistry,
      resolver: buildBoundSessionResolver(db),
      snapshotProvider: snapshotSpy,
    });
    const onRunnerConnect = async (runnerId: string): Promise<void> => {
      await distributor.onRunnerConnect(runnerId);
      await bridges.onRunnerConnect(runnerId, {});
    };

    // Both runners advertise ['claude','mock'] — includes 'claude' (capable),
    // excludes 'codex' (mismatch).
    const stopCapable = connectRunner(tunnelRegistry, capableRunner, workspaceId, [
      'claude',
      'mock',
    ]);
    const stopMismatch = connectRunner(tunnelRegistry, mismatchRunner, workspaceId, [
      'claude',
      'mock',
    ]);

    try {
      await onRunnerConnect(capableRunner);
      await onRunnerConnect(mismatchRunner);

      // The capable session flipped ASSIGNED; the mismatch session flipped FAILED.
      expect(await loadDistributionState(db, capableSession)).toBe(DISTRIBUTION_ASSIGNED);
      expect(await loadDistributionState(db, mismatchSession)).toBe(DISTRIBUTION_FAILED);

      // Exactly ONE bridge — the capable runner's. The FAILED session resolves to
      // null via the resolver filter, so its connect no-ops: no bridge started AND
      // the snapshot spy was never asked for it (delivery is never reached).
      expect(bridges.size).toBe(1);
      expect(snapshotSpy.calls).toEqual([capableSession]);
    } finally {
      await bridges.stopAll();
      stopCapable();
      stopMismatch();
    }
  });

  it('app.close() drains every live owner-pod bridge (onClose stopAll)', async () => {
    const workspaceId = uniqueWorkspace('bridgeclose');
    const agentId = await seedAgent(db, workspaceId, {});
    const runnerId = uniqueRunnerId();
    // A PENDING binding resolves through the manager's resolver, so onRunnerConnect
    // starts a real bridge.
    await seedBoundSession(db, workspaceId, agentId, runnerId);

    const app = buildApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
    });
    await app.ready();

    // A bridge pins a real tunnel generation, including its recovery acknowledgement.
    const manager = getSessionEventBridges(app);
    const stopRunner = connectRunner(getTunnelRegistry(app), runnerId, workspaceId, ['claude']);
    await manager.onRunnerConnect(runnerId, {});
    expect(manager.size).toBe(1);

    // The SIGINT/SIGTERM shutdown calls app.close(); its onClose hook drains the
    // manager. Without the hook the bridge would leak past shutdown (size stays 1).
    await app.close();
    stopRunner();
    expect(manager.size).toBe(0);
  });

  it('requires the exact assigned active runner binding before resource persistence', async () => {
    const workspaceId = uniqueWorkspace('resourcefence');
    const agentId = await seedAgent(db, workspaceId, {});
    const runnerId = uniqueRunnerId();
    const sessionId = await seedBoundSession(
      db,
      workspaceId,
      agentId,
      runnerId,
      DISTRIBUTION_ASSIGNED,
    );
    const current = () => runnerResourceBindingIsCurrent(db, workspaceId, sessionId, runnerId);
    expect(await current()).toBe(true);
    expect(
      await runnerResourceBindingIsCurrent(db, uniqueWorkspace('foreign'), sessionId, runnerId),
    ).toBe(false);
    expect(await runnerResourceBindingIsCurrent(db, workspaceId, newId('ses'), runnerId)).toBe(
      false,
    );
    for (const change of [
      { runnerId: uniqueRunnerId() },
      { distributionState: DISTRIBUTION_PENDING },
      { distributionState: DISTRIBUTION_FAILED },
      { archivedAt: new Date() },
      { deletedAt: new Date() },
      { status: 'terminated' },
    ]) {
      await db.update(sessions).set(change).where(eq(sessions.id, sessionId));
      expect(await current()).toBe(false);
      await db
        .update(sessions)
        .set({
          runnerId,
          distributionState: DISTRIBUTION_ASSIGNED,
          archivedAt: null,
          deletedAt: null,
          status: 'idle',
        })
        .where(eq(sessions.id, sessionId));
      expect(await current()).toBe(true);
    }
  });
});
