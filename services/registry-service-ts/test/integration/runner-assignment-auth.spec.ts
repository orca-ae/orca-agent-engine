// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { afterAll, afterEach, beforeAll, expect, it } from 'vitest';
import Fastify, { type FastifyInstance } from 'fastify';
import websocket from '@fastify/websocket';
import { WebSocket } from 'ws';
import { eq, sql } from 'drizzle-orm';
import {
  Deferred,
  FrameKind,
  INTERNAL_WS_ORIGIN,
  RUNNER_TUNNEL_TOKEN_HEADER,
  encodeFrame,
  tokenBoundRunnerId,
} from '@orca/harness-tunnel';
import { buildRunnerTunnelAuth } from '../../src/auth/tunnel-auth.js';
import { buildRunnerAssignmentAuth } from '../../src/auth/runner-assignment-auth.js';
import {
  registerRunnerTunnelRoutes,
  type RunnerTunnelRouteOptions,
} from '../../src/api/runner-tunnel.routes.js';
import {
  buildBoundSessionResolver,
  buildDistributionSessionStore,
} from '../../src/api/sessions.routes.js';
import { TunnelRegistry } from '../../src/tunnel/tunnel-registry.js';
import { mintBindingToken } from '../../src/tunnel/session-distributor.js';
import {
  agents,
  agentVersions,
  environments,
  sessions,
  workspaces,
} from '../../src/persistence/postgres/schema.js';
import { newId } from '../../src/domain/versioning.js';
import { closeTestDb, getTestDb } from './setup.js';
import { createTestWorkspace, uniqueWorkspace } from './fixtures.js';

let db: Awaited<ReturnType<typeof getTestDb>>['db'];
const apps: FastifyInstance[] = [];
const sockets: WebSocket[] = [];
beforeAll(async () => {
  ({ db } = await getTestDb());
});
afterEach(async () => {
  for (const socket of sockets.splice(0)) socket.terminate();
  for (const app of apps.splice(0)) await app.close();
});
afterAll(closeTestDb);

async function fixture() {
  const workspaceId = uniqueWorkspace('runner_auth');
  await createTestWorkspace(db, workspaceId);
  const agentId = newId('agt'),
    environmentId = newId('env'),
    sessionId = newId('ses');
  const token = mintBindingToken(),
    runnerId = tokenBoundRunnerId(token);
  await db.insert(agents).values({
    id: agentId,
    workspaceId,
    name: agentId,
    harnessType: 'pi',
    modelProvider: 'anthropic',
    modelId: 'test',
  });
  await db.insert(agentVersions).values({
    id: newId('agtv'),
    workspaceId,
    agentId,
    version: 1,
    snapshot: { harness_type: 'pi', metadata: { harness: 'pi', mode: 'colocated' } },
  });
  await db
    .insert(environments)
    .values({ id: environmentId, workspaceId, name: environmentId, target: 'self_hosted' });
  await db.insert(sessions).values({
    id: sessionId,
    workspaceId,
    agentId,
    agentVersion: 1,
    environmentId,
    hostEnvironmentId: environmentId,
    runnerId,
    distributionState: 'pending',
  });
  return { workspaceId, agentId, environmentId, sessionId, token, runnerId };
}

async function server(options: Partial<RunnerTunnelRouteOptions> = {}, tokens: string[] = []) {
  const registry = new TunnelRegistry();
  const resolvePeerHost = () => '203.0.113.9';
  const auth = buildRunnerTunnelAuth({
    runnerTunnelTokens: new Set(tokens),
    runnerTunnelAllowedOrigins: new Set(),
    runnerTunnelLocalMode: true,
    resolvePeerHost,
  });
  const app = Fastify();
  apps.push(app);
  await app.register(websocket);
  registerRunnerTunnelRoutes(app, {
    registry,
    ...auth,
    resolvePeerHost,
    authorizeAssignedRunner: buildRunnerAssignmentAuth(db),
    ...options,
  });
  await app.listen({ host: '127.0.0.1', port: 0 });
  const address = app.server.address();
  if (!address || typeof address === 'string') throw new Error('no listener');
  return { registry, url: `ws://127.0.0.1:${address.port}` };
}
const hello = encodeFrame({
  kind: FrameKind.Hello,
  frameProtocolVersion: 1,
  runnerVersion: 'test',
  harnesses: ['pi-sdk'],
  envs: [],
});
type TestServer = Awaited<ReturnType<typeof server>>;
async function dial(
  server: TestServer,
  token: string,
  runnerId = tokenBoundRunnerId(token),
  sendHello = true,
) {
  const socket = new WebSocket(`${server.url}/v1/tunnels/runners/${runnerId}`, {
    headers: { origin: INTERNAL_WS_ORIGIN, [RUNNER_TUNNEL_TOKEN_HEADER]: token },
  });
  sockets.push(socket);
  const closed = new Promise<number>((resolve) => socket.once('close', resolve));
  socket.on('error', () => {});
  await new Promise<void>((resolve) => socket.once('open', resolve));
  if (sendHello) socket.send(hello);
  return { socket, closed };
}
async function connected(server: TestServer, token: string, runnerId = tokenBoundRunnerId(token)) {
  const peer = await dial(server, token, runnerId);
  await expect.poll(() => server.registry.has(runnerId)).toBe(true);
  return peer;
}

it('admits a persisted assignment on a fresh replica with no worker or pending waiter', async () => {
  const f = await fixture();
  const s = await server();
  expect(s.registry.connectWaiterCount()).toBe(0);
  await connected(s, f.token);
  expect(s.registry.runnerOwner(f.runnerId)).toBe(f.workspaceId);
});

it('indexes the admission lookup across all Session states without requiring unique runner ids', async () => {
  const result = await db.execute(sql`
    SELECT i.indisunique, i.indisvalid, pg_get_expr(i.indpred, i.indrelid) AS predicate,
      pg_get_indexdef(i.indexrelid) AS definition
    FROM pg_index i
    WHERE i.indexrelid = to_regclass('public.sessions_runner_id_idx')
  `);
  expect(result.rows).toEqual([
    {
      indisunique: false,
      indisvalid: true,
      predicate: null,
      definition: 'CREATE INDEX sessions_runner_id_idx ON public.sessions USING btree (runner_id)',
    },
  ]);
});

it.each([
  { runnerId: null },
  { runnerId: 'replacement' },
  { archivedAt: new Date() },
  { status: 'terminated' },
])(
  'rejects new admissions after retirement without closing the registered transport: %j',
  async (patch) => {
    const f = await fixture(),
      s = await server();
    const peer = await connected(s, f.token);
    const original = s.registry.get(f.runnerId);
    await db.update(sessions).set(patch).where(eq(sessions.id, f.sessionId));
    expect(await (await dial(s, f.token)).closed).toBe(4004);
    expect(peer.socket.readyState).toBe(WebSocket.OPEN);
    expect(s.registry.get(f.runnerId)).toBe(original);
  },
);

it('admits assigned runners with a static allowlist configured, without granting static tokens their identity', async () => {
  const f = await fixture();
  const s = await server({}, ['operator-token']);
  await connected(s, f.token);
  expect(await (await dial(s, 'operator-token', f.runnerId)).closed).toBe(4004);
  expect(s.registry.runnerOwner(f.runnerId)).toBe(f.workspaceId);
  await connected(s, 'operator-token', 'operator-runner');
  expect(s.registry.runnerOwner('operator-runner')).toBe('local');
});

it('rejects unissued tokens and wrong paths without evicting the legitimate connection', async () => {
  const f = await fixture(),
    s = await server();
  await connected(s, f.token);
  const original = s.registry.get(f.runnerId);
  expect(await (await dial(s, mintBindingToken())).closed).toBe(4004);
  expect(await (await dial(s, mintBindingToken(), f.runnerId)).closed).toBe(4004);
  expect(await (await dial(s, f.token, 'wrong-runner')).closed).toBe(4004);
  expect(s.registry.get(f.runnerId)).toBe(original);
});

it('rejects an unissued binding without waiting for hello', async () => {
  const s = await server();
  const token = mintBindingToken();
  const peer = await dial(s, token, tokenBoundRunnerId(token), false);
  await expect.poll(() => peer.socket.readyState).toBe(WebSocket.CLOSED);
  expect(await peer.closed).toBe(4004);
  expect(s.registry.onlineRunnerIds()).toEqual([]);
});

it.each([
  'deleted',
  'archived',
  'terminated',
  'unbound',
  'reassigned',
  'wrong-environment',
  'environment-deleted',
  'environment-archived',
  'workspace-inactive',
] as const)('rejects %s assignments', async (change) => {
  const f = await fixture();
  if (change === 'deleted')
    await db.update(sessions).set({ deletedAt: new Date() }).where(eq(sessions.id, f.sessionId));
  if (change === 'archived')
    await db.update(sessions).set({ archivedAt: new Date() }).where(eq(sessions.id, f.sessionId));
  if (change === 'terminated')
    await db.update(sessions).set({ status: 'terminated' }).where(eq(sessions.id, f.sessionId));
  if (change === 'unbound')
    await db.update(sessions).set({ runnerId: null }).where(eq(sessions.id, f.sessionId));
  if (change === 'reassigned')
    await db
      .update(sessions)
      .set({ runnerId: tokenBoundRunnerId(mintBindingToken()) })
      .where(eq(sessions.id, f.sessionId));
  if (change === 'wrong-environment')
    await db
      .update(sessions)
      .set({ hostEnvironmentId: newId('env') })
      .where(eq(sessions.id, f.sessionId));
  if (change === 'environment-deleted')
    await db
      .update(environments)
      .set({ deletedAt: new Date() })
      .where(eq(environments.id, f.environmentId));
  if (change === 'environment-archived')
    await db
      .update(environments)
      .set({ archivedAt: new Date() })
      .where(eq(environments.id, f.environmentId));
  if (change === 'workspace-inactive')
    await db
      .update(workspaces)
      .set({ status: 'archived', archivedAt: new Date() })
      .where(eq(workspaces.id, f.workspaceId));
  const s = await server();
  expect(await (await dial(s, f.token)).closed).toBe(4004);
  expect(s.registry.onlineRunnerIds()).toEqual([]);
  if (['deleted', 'archived', 'terminated'].includes(change)) {
    expect(await buildBoundSessionResolver(db).resolveBoundSession(f.runnerId)).toBeNull();
  }
});

it.each([{}, { deletedAt: new Date() }, { archivedAt: new Date() }, { status: 'terminated' }])(
  'rejects a cross-workspace host binding and ambiguous runner ids including retired rows: %j',
  async (patch) => {
    const a = await fixture(),
      b = await fixture();
    const s = await server();
    await db
      .update(sessions)
      .set({ hostEnvironmentId: b.environmentId })
      .where(eq(sessions.id, a.sessionId));
    expect(await (await dial(s, a.token)).closed).toBe(4004);
    await db
      .update(sessions)
      .set({ hostEnvironmentId: a.environmentId })
      .where(eq(sessions.id, a.sessionId));
    await db
      .update(sessions)
      .set({ runnerId: a.runnerId, ...patch })
      .where(eq(sessions.id, b.sessionId));
    expect(await (await dial(s, a.token)).closed).toBe(4004);
  },
);

it.each([null, 'pending', 'failed', 'assigned'])(
  'allows a current %s binding to reconnect after a turn and fences the local replaced connection',
  async (distributionState) => {
    const f = await fixture(),
      s = await server();
    await db
      .update(sessions)
      .set({ distributionState, status: 'idle' })
      .where(eq(sessions.id, f.sessionId));
    const first = await connected(s, f.token);
    const old = s.registry.get(f.runnerId)!;
    await dial(s, f.token);
    expect(await first.closed).toBe(4000);
    await expect.poll(() => s.registry.get(f.runnerId) !== old).toBe(true);
    s.registry.deregister(f.runnerId, old);
    expect(s.registry.runnerOwner(f.runnerId)).toBe(f.workspaceId);
  },
);

it('rechecks revocation after hello instead of trusting the initial authentication', async () => {
  const f = await fixture();
  const checked = new Deferred<void>();
  const authorize = buildRunnerAssignmentAuth(db);
  const s = await server({
    authorizeAssignedRunner: async (...args) => {
      const result = await authorize(...args);
      if (!checked.done) checked.resolve();
      return result;
    },
  });
  const peer = await dial(s, f.token, f.runnerId, false);
  await checked.promise;
  await db.update(sessions).set({ runnerId: null }).where(eq(sessions.id, f.sessionId));
  peer.socket.send(hello);
  await expect.poll(() => peer.socket.readyState).toBe(WebSocket.CLOSED);
  expect(await peer.closed).toBe(4004);
  expect(s.registry.onlineRunnerIds()).toEqual([]);
});

it('orders admission behind an in-flight reassignment transaction', async () => {
  const f = await fixture(),
    s = await server();
  const locked = new Deferred<void>(),
    release = new Deferred<void>();
  const reassign = db.transaction(async (tx) => {
    await tx
      .update(sessions)
      .set({ runnerId: tokenBoundRunnerId(mintBindingToken()) })
      .where(eq(sessions.id, f.sessionId));
    locked.resolve();
    await release.promise;
  });
  await locked.promise;
  try {
    const peer = await dial(s, f.token);
    release.resolve();
    await reassign;
    expect(await peer.closed).toBe(4004);
    expect(s.registry.onlineRunnerIds()).toEqual([]);
  } finally {
    if (!release.done) release.resolve();
    await reassign;
  }
});

it.each([{ status: 'terminated' }, { archivedAt: new Date() }])(
  'does not let stale distributor callbacks update a replacement or retired session: %j',
  async (retirement) => {
    const f = await fixture();
    const store = buildDistributionSessionStore(db);
    await db
      .update(sessions)
      .set({ runnerId: 'replacement', distributionState: 'pending' })
      .where(eq(sessions.id, f.sessionId));
    expect(
      await store.applyDistributionPatch(
        f.sessionId,
        { distributionState: 'assigned' },
        f.runnerId,
      ),
    ).toBe(false);
    expect(
      await store.applyDistributionPatch(f.sessionId, { distributionState: 'failed' }, f.runnerId),
    ).toBe(false);
    await db.update(sessions).set(retirement).where(eq(sessions.id, f.sessionId));
    for (const patch of [
      { distributionState: 'assigned' },
      { distributionState: 'failed' },
      { distributionState: 'pending', runnerId: null, hostEnvironmentId: null },
    ] as const) {
      expect(await store.applyDistributionPatch(f.sessionId, patch, 'replacement')).toBe(false);
    }
    const [row] = await db.select().from(sessions).where(eq(sessions.id, f.sessionId));
    expect(row).toMatchObject({ runnerId: 'replacement', distributionState: 'pending' });
  },
);

it('fails closed on a database error without deregistering an existing connection', async () => {
  const f = await fixture();
  let fail = false;
  const authorize = buildRunnerAssignmentAuth(db);
  const s = await server({
    authorizeAssignedRunner: (...args) => {
      if (fail) throw new Error('database unavailable');
      return authorize(...args);
    },
  });
  await connected(s, f.token);
  const original = s.registry.get(f.runnerId);
  fail = true;
  expect(await (await dial(s, f.token)).closed).toBe(1011);
  expect(s.registry.get(f.runnerId)).toBe(original);
});

it('does not start a superseded connection hook after its admission await completes', async () => {
  const f = await fixture();
  const registered = new Deferred<void>(),
    release = new Deferred<void>();
  const authorize = buildRunnerAssignmentAuth(db);
  let calls = 0,
    connects = 0;
  const s = await server({
    authorizeAssignedRunner: async (...args) => {
      const call = ++calls;
      const result = await authorize(...args);
      if (call === 2) {
        registered.resolve();
        await release.promise;
      }
      return result;
    },
    onRunnerConnect: async () => {
      connects++;
    },
  });
  try {
    const first = await dial(s, f.token);
    await registered.promise;
    await dial(s, f.token);
    expect(await first.closed).toBe(4000);
    await expect.poll(() => connects).toBe(1);
    release.resolve();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(connects).toBe(1);
    expect(s.registry.runnerOwner(f.runnerId)).toBe(f.workspaceId);
  } finally {
    if (!release.done) release.resolve();
  }
});
