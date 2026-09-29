// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the worker tunnel engine (WorkerTunnelServer).
//
// WorkerTunnelServer drives one worker's outbound WebSocket from the handshake auth
// gate through the hello + version check and the three concurrent loops (sender,
// receive, ping) to a guarded teardown. It is the consumer of the worker-frame
// schema: the receive loop decodes every `worker.*` result kind and routes it to a
// pending request waiter (or records a `worker.runner_exited` report), and the
// ping loop multiplexes runner-tunnel pings on the same socket.
//
// These tests drive the engine with faithful in-memory collaborators — a fake
// accepted WebSocket (an inbound message queue + captured sends/closes), a fake
// persistent worker store (records upsert / offline / heartbeat and resolves
// launch tokens), a fake live-worker registry whose connection is a plain record,
// and a fake runner-exit sink — exactly as the transport suite drives its
// registry. No real sockets; `sleep` is injected so the ping cadence is
// deterministic. They exercise:
//
//   * auth: launch-token resolve / mismatch / fail-closed; auth-provider
//     identity / fail-closed; no-provider -> reserved local owner;
//   * hello handshake: happy path register + upsert ordering; non-hello first
//     frame (4001); version-skew (4002); socket closed before hello;
//   * receive routing: every result kind resolves its pending waiter with the
//     full payload; runner_exited is recorded + fires the hook; pong tolerance;
//     malformed-frame drop; unexpected-kind drop;
//   * teardown guards: a successful connection deregisters + sets offline + fires
//     the disconnect hook exactly once; an upsert owner-conflict before register
//     never touches the existing owner's worker (no deregister / no setOffline);
//     in-flight pending waiters are rejected (WorkerTunnelClosedError) so callers
//     never hang; a setOffline failure still fires the disconnect hook; a
//     connection superseded by a newest-wins replacement skips the shared
//     teardown (no deregister / offline / hook) but settles its own waiters;
//   * ping: a heartbeat + ping each live interval; a silent worker past the miss
//     threshold is closed (4003); a heartbeat store failure does NOT tear down a
//     healthy tunnel.

import { describe, it, expect } from 'vitest';
import {
  WorkerFrameKind,
  encodeWorkerFrame,
  type WorkerHelloFrame,
  type WorkerLaunchRunnerResultFrame,
  type WorkerStopRunnerResultFrame,
  type WorkerRunnerExitedFrame,
  type WorkerStatResultFrame,
  type WorkerListDirResultFrame,
  type WorkerCreateWorktreeResultFrame,
  type WorkerRemoveWorktreeResultFrame,
  type WorkerCreateDirResultFrame,
} from '../../src/worker-frames.js';
import { FrameKind, encodeFrame } from '../../src/frames.js';
import { HOST_TUNNEL_TOKEN_HEADER } from '../../src/identity.js';
import { AsyncQueue } from '../../src/transport.js';
import {
  WorkerTunnelServer,
  WorkerTunnelClosedError,
  LOCAL_TUNNEL_OWNER,
  EXPECTED_HELLO_CLOSE_CODE,
  VERSION_MISMATCH_CLOSE_CODE,
  PING_TIMEOUT_CLOSE_CODE,
  UNAUTHENTICATED_CLOSE_CODE,
  PING_INTERVAL_MS,
  PING_MISS_THRESHOLD,
  type WorkerSocketMessage,
  type WorkerWebSocket,
  type WorkerHandshake,
  type WorkerStore,
  type WorkerUpsertOnConnect,
  type WorkerConnection,
  type WorkerRegistry,
  type ResolvedLaunchToken,
  type RunnerExitSink,
  type WorkerRunnerExitContext,
} from '../../src/worker-tunnel.js';

// ── Fakes ────────────────────────────────────────────────

/**
 * Fake accepted WebSocket: tests push inbound messages onto `inbound`; the engine
 * pulls them via `receive`. Sent text frames are captured in `sent`. `close`
 * records the code/reason and (once) pushes a `close` message onto `inbound` so a
 * receive loop parked on `receive` wakes and the whole `handle()` settles — the
 * same effect a real socket close has.
 */
class FakeWorkerSocket implements WorkerWebSocket {
  readonly inbound = new AsyncQueue<WorkerSocketMessage>();
  readonly sent: string[] = [];
  closed: { code?: number; reason?: string } | undefined;

  receive(): Promise<WorkerSocketMessage> {
    return this.inbound.get();
  }

  async sendText(data: string): Promise<void> {
    this.sent.push(data);
  }

  close(code?: number, reason?: string): void {
    if (this.closed === undefined) {
      this.closed = { code, reason };
      // Wake a parked receive loop so the lifecycle can finish.
      this.inbound.put({ type: 'close', code, reason });
    }
  }

  /** Push a worker/runner frame text as an inbound message. */
  pushText(text: string): void {
    this.inbound.put({ type: 'text', data: text });
  }

  /** Push a socket-close message (no close code recorded on `closed`). */
  pushClose(code = 1000, reason?: string): void {
    this.inbound.put(
      reason === undefined ? { type: 'close', code } : { type: 'close', code, reason },
    );
  }
}

/** A simple handshake backed by a header map (case-insensitive lookup). */
class FakeHandshake implements WorkerHandshake {
  private readonly map = new Map<string, string>();
  constructor(headers: Record<string, string> = {}) {
    for (const [k, v] of Object.entries(headers)) {
      this.map.set(k.toLowerCase(), v);
    }
  }

  header(name: string): string | undefined {
    return this.map.get(name.toLowerCase());
  }
}

interface UpsertCall {
  workerId: string;
  name: string;
  owner: string | undefined;
  allowWorkerIdReown: boolean;
  configuredHarnesses: Record<string, boolean> | null;
}

/** Fake persistent worker store: records every call; resolves seeded launch tokens. */
class FakeWorkerStore implements WorkerStore {
  readonly upserts: UpsertCall[] = [];
  readonly offlineCalls: string[] = [];
  heartbeats = 0;
  /** When set, `upsertOnConnect` throws it (simulates an owner-conflict). */
  upsertError: Error | undefined;
  /** When set, `heartbeat` throws it (simulates a store blip mid-tunnel). */
  heartbeatError: Error | undefined;
  /** When set, `setOffline` throws it (simulates a store failure at teardown). */
  offlineError: Error | undefined;
  private readonly tokens = new Map<string, ResolvedLaunchToken>();

  seedToken(token: string, resolved: ResolvedLaunchToken): void {
    this.tokens.set(token, resolved);
  }

  resolveLaunchToken(token: string): ResolvedLaunchToken | null {
    return this.tokens.get(token) ?? null;
  }

  async upsertOnConnect(args: WorkerUpsertOnConnect): Promise<void> {
    if (this.upsertError !== undefined) {
      throw this.upsertError;
    }
    this.upserts.push({ ...args });
  }

  async setOffline(workerId: string): Promise<void> {
    if (this.offlineError !== undefined) {
      throw this.offlineError;
    }
    this.offlineCalls.push(workerId);
  }

  async heartbeat(_hostId: string): Promise<void> {
    this.heartbeats += 1;
    if (this.heartbeatError !== undefined) {
      throw this.heartbeatError;
    }
  }
}

/** Build a fresh, empty worker connection record (the registry hands these out). */
function makeConnection(owner: string | undefined): WorkerConnection {
  return {
    owner,
    outboundQueue: new AsyncQueue<string | null>(),
    lastFrameAt: 0,
    pendingLaunches: new Map(),
    pendingStops: new Map(),
    pendingStats: new Map(),
    pendingListDirs: new Map(),
    pendingCreateWorktrees: new Map(),
    pendingRemoveWorktrees: new Map(),
    pendingCreateDirs: new Map(),
  };
}

/** Fake live-worker registry: records register/deregister; exposes the connection. */
class FakeWorkerRegistry implements WorkerRegistry {
  /** Current live connection (`undefined` once deregistered), as `get` reports. */
  conn: WorkerConnection | undefined;
  /** Most recently registered connection, retained for post-teardown assertions. */
  lastConn: WorkerConnection | undefined;
  registeredHello: WorkerHelloFrame | undefined;
  registerCalls = 0;
  deregisterCalls = 0;

  register(
    _hostId: string,
    _ws: WorkerWebSocket,
    hello: WorkerHelloFrame,
    opts: { owner?: string },
  ): WorkerConnection {
    this.registerCalls += 1;
    this.registeredHello = hello;
    const conn = makeConnection(opts.owner);
    this.conn = conn;
    this.lastConn = conn;
    return conn;
  }

  get(_hostId: string): WorkerConnection | undefined {
    return this.conn;
  }

  deregister(_hostId: string): void {
    this.deregisterCalls += 1;
    // Mirror the real registry: pushing the stop sentinel unblocks a parked
    // sender loop so teardown never deadlocks, and the entry is removed.
    this.conn?.outboundQueue.put(null);
    this.conn = undefined;
  }
}

/** Fake runner-exit sink: records every report with its authenticated context. */
class FakeRunnerExitSink implements RunnerExitSink {
  readonly records: Array<{
    runnerId: string;
    error: string;
    owner: string | undefined;
    workerId: string;
  }> = [];
  record(runnerId: string, error: string, ctx: WorkerRunnerExitContext): void {
    this.records.push({ runnerId, error, owner: ctx.owner, workerId: ctx.workerId });
  }
}

// ── Helpers ──────────────────────────────────────────────

const HELLO_PROTO = 1;

function helloText(over: Partial<WorkerHelloFrame> = {}): string {
  const hello: WorkerHelloFrame = {
    kind: WorkerFrameKind.Hello,
    version: '0.1.0',
    frameProtocolVersion: HELLO_PROTO,
    name: 'workstation-01',
    runners: [],
    ...over,
  };
  return encodeWorkerFrame(hello);
}

/**
 * A `sleep` that never resolves on its own — the ping loop parks forever, so the
 * tunnel ends only via an inbound close. Used by every test that isn't about ping
 * timing. Returns the bare options needed to construct a server.
 */
function neverSleep(): (ms: number) => Promise<void> {
  return () =>
    new Promise<void>(() => {
      // never resolves
    });
}

interface Harness {
  server: WorkerTunnelServer;
  socket: FakeWorkerSocket;
  store: FakeWorkerStore;
  registry: FakeWorkerRegistry;
  sink: FakeRunnerExitSink;
}

function makeHarness(
  over: Partial<ConstructorParameters<typeof WorkerTunnelServer>[0]> = {},
): Harness {
  const socket = new FakeWorkerSocket();
  const store = new FakeWorkerStore();
  const registry = new FakeWorkerRegistry();
  const sink = new FakeRunnerExitSink();
  const server = new WorkerTunnelServer({
    registry,
    store,
    runnerExitReports: sink,
    sleep: neverSleep(),
    now: () => 1_000,
    ...over,
  });
  return { server, socket, store, registry, sink };
}

/** Run the tunnel to completion, driving the hello + a follow-up via `drive`. */
async function runTunnel(
  h: Harness,
  handshake: WorkerHandshake,
  drive: (socket: FakeWorkerSocket) => void | Promise<void>,
  workerId = 'host_abc',
): Promise<void> {
  const done = h.server.handle(h.socket, handshake, workerId);
  await Promise.resolve();
  await drive(h.socket);
  await done;
}

/**
 * Yield microtasks until the registry has handed out a live connection, then
 * return it. The hello → upsert (async) → register path crosses several
 * microtask turns, so a single `await` is not enough; this polls deterministically
 * (no timers) and fails fast if the connection never appears.
 */
async function waitForConnection(registry: FakeWorkerRegistry): Promise<WorkerConnection> {
  for (let i = 0; i < 50; i += 1) {
    if (registry.lastConn !== undefined) {
      return registry.lastConn;
    }
    await Promise.resolve();
  }
  throw new Error('connection was never registered');
}

// ── Auth gate ────────────────────────────────────────────

describe('WorkerTunnelServer auth gate', () => {
  it('launch token that resolves to this worker authenticates with the token owner', async () => {
    const h = makeHarness();
    h.store.seedToken('tok_managed', { workerId: 'host_abc', owner: 'alice' });
    const handshake = new FakeHandshake({ [HOST_TUNNEL_TOKEN_HEADER]: 'tok_managed' });

    await runTunnel(h, handshake, (s) => {
      s.pushText(helloText());
      s.pushClose();
    });

    expect(h.registry.registerCalls).toBe(1);
    // The token owner is recorded on the connection + the upsert.
    expect(h.registry.lastConn?.owner).toBe('alice');
    expect(h.store.upserts[0]?.owner).toBe('alice');
    // Teardown's best-effort close carries no protocol error code — the peer
    // initiated the close; the server did not refuse the handshake.
    expect(h.socket.closed?.code).toBeUndefined();
  });

  it('launch token scoped to a different worker id is refused (4004), never registers', async () => {
    const h = makeHarness();
    h.store.seedToken('tok_managed', { workerId: 'host_other', owner: 'alice' });
    const handshake = new FakeHandshake({ [HOST_TUNNEL_TOKEN_HEADER]: 'tok_managed' });

    await h.server.handle(h.socket, handshake, 'host_abc');

    expect(h.socket.closed).toEqual({
      code: UNAUTHENTICATED_CLOSE_CODE,
      reason: 'unauthenticated',
    });
    expect(h.registry.registerCalls).toBe(0);
    expect(h.store.upserts).toHaveLength(0);
  });

  it('an unknown launch token is refused (4004) and never falls back to user auth', async () => {
    let userLookups = 0;
    const h = makeHarness({
      authProvider: {
        getUserId() {
          userLookups += 1;
          return 'should-not-be-used';
        },
      },
    });
    const handshake = new FakeHandshake({ [HOST_TUNNEL_TOKEN_HEADER]: 'tok_unknown' });

    await h.server.handle(h.socket, handshake, 'host_abc');

    expect(h.socket.closed?.code).toBe(UNAUTHENTICATED_CLOSE_CODE);
    expect(h.registry.registerCalls).toBe(0);
    // Presenting the managed header must NOT downgrade into the user auth path.
    expect(userLookups).toBe(0);
  });

  it('with an auth provider, an authenticated peer registers under its user id', async () => {
    const h = makeHarness({ authProvider: { getUserId: () => 'bob' } });

    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
      s.pushClose();
    });

    expect(h.registry.lastConn?.owner).toBe('bob');
    expect(h.store.upserts[0]?.owner).toBe('bob');
  });

  it('with an auth provider, an unauthenticated peer is refused (4004) and fails closed', async () => {
    const h = makeHarness({ authProvider: { getUserId: () => null } });

    await h.server.handle(h.socket, new FakeHandshake(), 'host_abc');

    expect(h.socket.closed?.code).toBe(UNAUTHENTICATED_CLOSE_CODE);
    expect(h.registry.registerCalls).toBe(0);
    expect(h.store.upserts).toHaveLength(0);
  });

  it('with no auth provider, the peer registers under the reserved local owner', async () => {
    const h = makeHarness(); // no authProvider

    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
      s.pushClose();
    });

    expect(h.registry.lastConn?.owner).toBe(LOCAL_TUNNEL_OWNER);
    expect(h.store.upserts[0]?.owner).toBe(LOCAL_TUNNEL_OWNER);
  });

  it('a launch-token store error fails closed (4004) rather than falling through', async () => {
    const h = makeHarness({ authProvider: { getUserId: () => 'bob' } });
    // Override resolveLaunchToken to throw.
    h.store.resolveLaunchToken = () => {
      throw new Error('store down');
    };
    const handshake = new FakeHandshake({ [HOST_TUNNEL_TOKEN_HEADER]: 'tok_x' });

    await h.server.handle(h.socket, handshake, 'host_abc');

    expect(h.socket.closed?.code).toBe(UNAUTHENTICATED_CLOSE_CODE);
    expect(h.registry.registerCalls).toBe(0);
  });
});

// ── Hello handshake ──────────────────────────────────────

describe('WorkerTunnelServer hello handshake', () => {
  it('upserts the worker online BEFORE registering the live connection', async () => {
    const order: string[] = [];
    const h = makeHarness();
    // Wrap the store + registry to record call order.
    const origUpsert = h.store.upsertOnConnect.bind(h.store);
    h.store.upsertOnConnect = async (args) => {
      order.push('upsert');
      return origUpsert(args);
    };
    const origRegister = h.registry.register.bind(h.registry);
    h.registry.register = (id, ws, hello, opts) => {
      order.push('register');
      return origRegister(id, ws, hello, opts);
    };

    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(
        helloText({
          name: 'box',
          runners: ['runner_a'],
          configuredHarnesses: { 'claude-sdk': true },
        }),
      );
      s.pushClose();
    });

    expect(order).toEqual(['upsert', 'register']);
    // Hello fields flow into the upsert.
    expect(h.store.upserts[0]).toMatchObject({
      workerId: 'host_abc',
      name: 'box',
      configuredHarnesses: { 'claude-sdk': true },
    });
    expect(h.registry.registeredHello?.runners).toEqual(['runner_a']);
  });

  it('a non-hello first frame closes with 4001 and never registers', async () => {
    const h = makeHarness();
    const stop: WorkerStopRunnerResultFrame = {
      kind: WorkerFrameKind.StopRunnerResult,
      requestId: 'r',
      status: 'stopped',
    };

    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(encodeWorkerFrame(stop));
    });

    expect(h.socket.closed?.code).toBe(EXPECTED_HELLO_CLOSE_CODE);
    expect(h.registry.registerCalls).toBe(0);
    expect(h.store.upserts).toHaveLength(0);
  });

  it('a malformed first frame closes with 4001', async () => {
    const h = makeHarness();
    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText('not json at all');
    });
    expect(h.socket.closed?.code).toBe(EXPECTED_HELLO_CLOSE_CODE);
    expect(h.registry.registerCalls).toBe(0);
  });

  it('a frame_protocol_version mismatch closes with 4002 and never registers', async () => {
    const h = makeHarness();
    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText({ frameProtocolVersion: 2 }));
    });
    expect(h.socket.closed?.code).toBe(VERSION_MISMATCH_CLOSE_CODE);
    expect(h.socket.closed?.reason).toContain('frame_protocol_version mismatch');
    expect(h.registry.registerCalls).toBe(0);
    expect(h.store.upserts).toHaveLength(0);
  });

  it('a socket that closes before the hello arrives is a clean no-op (no register, no close)', async () => {
    const h = makeHarness();
    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushClose();
    });
    expect(h.registry.registerCalls).toBe(0);
    expect(h.store.upserts).toHaveLength(0);
    // We did not initiate a close — the peer did.
    expect(h.socket.closed).toBeUndefined();
  });
});

// ── Receive routing ──────────────────────────────────────

describe('WorkerTunnelServer result routing', () => {
  it('a launch result resolves the matching pending launch waiter with the full payload', async () => {
    const h = makeHarness();
    let resolved: unknown;
    const result: WorkerLaunchRunnerResultFrame = {
      kind: WorkerFrameKind.LaunchRunnerResult,
      requestId: 'req_1',
      status: 'launched',
      runnerId: 'runner_token_abc',
    };

    await runTunnel(h, new FakeHandshake(), async (s) => {
      s.pushText(helloText());
      // Register a waiter on the connection the registry just created.
      const conn = await waitForConnection(h.registry);
      conn.pendingLaunches.set('req_1', { resolve: (r) => (resolved = r), reject: () => {} });
      s.pushText(encodeWorkerFrame(result));
      s.pushClose();
    });

    expect(resolved).toEqual({
      requestId: 'req_1',
      status: 'launched',
      runnerId: 'runner_token_abc',
      error: null,
      errorCode: null,
    });
    // The waiter is consumed exactly once.
    expect(h.registry.lastConn!.pendingLaunches.size).toBe(0);
  });

  it('a stop result resolves the matching pending stop waiter', async () => {
    const h = makeHarness();
    let resolved: unknown;
    const result: WorkerStopRunnerResultFrame = {
      kind: WorkerFrameKind.StopRunnerResult,
      requestId: 'req_2',
      status: 'stopped',
    };
    await runTunnel(h, new FakeHandshake(), async (s) => {
      s.pushText(helloText());
      const conn = await waitForConnection(h.registry);
      conn.pendingStops.set('req_2', { resolve: (r) => (resolved = r), reject: () => {} });
      s.pushText(encodeWorkerFrame(result));
      s.pushClose();
    });
    expect(resolved).toEqual({ requestId: 'req_2', status: 'stopped', error: null });
  });

  it('a stat result resolves with exists/type/canonical_path/error preserved', async () => {
    const h = makeHarness();
    let resolved: unknown;
    const result: WorkerStatResultFrame = {
      kind: WorkerFrameKind.StatResult,
      requestId: 'req_stat',
      status: 'ok',
      exists: true,
      type: 'directory',
      canonicalPath: '/srv/work',
    };
    await runTunnel(h, new FakeHandshake(), async (s) => {
      s.pushText(helloText());
      const conn = await waitForConnection(h.registry);
      conn.pendingStats.set('req_stat', { resolve: (r) => (resolved = r), reject: () => {} });
      s.pushText(encodeWorkerFrame(result));
      s.pushClose();
    });
    expect(resolved).toEqual({
      requestId: 'req_stat',
      status: 'ok',
      exists: true,
      type: 'directory',
      canonicalPath: '/srv/work',
      error: null,
    });
  });

  it('a list_dir result resolves with entries + has_more preserved', async () => {
    const h = makeHarness();
    let resolved: { entries: unknown[]; hasMore: boolean } | undefined;
    const result: WorkerListDirResultFrame = {
      kind: WorkerFrameKind.ListDirResult,
      requestId: 'req_list',
      status: 'ok',
      entries: [
        {
          name: 'src',
          path: '/srv/work/src',
          type: 'directory',
          bytes: null,
          modifiedAt: 1779980000,
        },
      ],
      hasMore: true,
    };
    await runTunnel(h, new FakeHandshake(), async (s) => {
      s.pushText(helloText());
      const conn = await waitForConnection(h.registry);
      conn.pendingListDirs.set('req_list', { resolve: (r) => (resolved = r), reject: () => {} });
      s.pushText(encodeWorkerFrame(result));
      s.pushClose();
    });
    expect(resolved?.entries).toHaveLength(1);
    expect(resolved?.hasMore).toBe(true);
  });

  it('create_worktree, remove_worktree, and create_dir results each resolve their waiter', async () => {
    const h = makeHarness();
    const got: Record<string, unknown> = {};
    const cw: WorkerCreateWorktreeResultFrame = {
      kind: WorkerFrameKind.CreateWorktreeResult,
      requestId: 'req_cw',
      status: 'ok',
      worktreePath: '/srv/wt',
      branch: 'feature/x',
    };
    const rw: WorkerRemoveWorktreeResultFrame = {
      kind: WorkerFrameKind.RemoveWorktreeResult,
      requestId: 'req_rw',
      status: 'ok',
    };
    const cd: WorkerCreateDirResultFrame = {
      kind: WorkerFrameKind.CreateDirResult,
      requestId: 'req_cd',
      status: 'ok',
      path: '/srv/new',
    };
    await runTunnel(h, new FakeHandshake(), async (s) => {
      s.pushText(helloText());
      const conn = await waitForConnection(h.registry);
      conn.pendingCreateWorktrees.set('req_cw', { resolve: (r) => (got.cw = r), reject: () => {} });
      conn.pendingRemoveWorktrees.set('req_rw', { resolve: (r) => (got.rw = r), reject: () => {} });
      conn.pendingCreateDirs.set('req_cd', { resolve: (r) => (got.cd = r), reject: () => {} });
      s.pushText(encodeWorkerFrame(cw));
      s.pushText(encodeWorkerFrame(rw));
      s.pushText(encodeWorkerFrame(cd));
      s.pushClose();
    });
    expect(got.cw).toMatchObject({
      requestId: 'req_cw',
      worktreePath: '/srv/wt',
      branch: 'feature/x',
    });
    expect(got.rw).toEqual({ requestId: 'req_rw', status: 'ok', error: null });
    expect(got.cd).toMatchObject({ requestId: 'req_cd', path: '/srv/new' });
  });

  it('a result with no matching waiter is dropped without error', async () => {
    const h = makeHarness();
    const result: WorkerStopRunnerResultFrame = {
      kind: WorkerFrameKind.StopRunnerResult,
      requestId: 'orphan',
      status: 'stopped',
    };
    // No waiter registered. Must not throw; tunnel still ends cleanly.
    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
      s.pushText(encodeWorkerFrame(result));
      s.pushClose();
    });
    expect(h.registry.registerCalls).toBe(1);
    expect(h.store.offlineCalls).toEqual(['host_abc']);
  });
});

// ── runner_exited recording ──────────────────────────────

describe('WorkerTunnelServer worker.runner_exited', () => {
  it('records the exit report (owner-scoped) and fires the onRunnerExited hook', async () => {
    const fired: Array<[string, string, WorkerRunnerExitContext]> = [];
    const h = makeHarness({
      authProvider: { getUserId: () => 'carol' },
      onRunnerExited: async (runnerId, error, ctx) => {
        fired.push([runnerId, error, ctx]);
      },
    });
    const exited: WorkerRunnerExitedFrame = {
      kind: WorkerFrameKind.RunnerExited,
      runnerId: 'runner_dead',
      error: 'runner process exited with code 1 (log tail: boom)',
    };

    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
      s.pushText(encodeWorkerFrame(exited));
      s.pushClose();
    });

    // Recorded into the sink with the AUTHENTICATED identity next to the
    // worker-supplied claim: frame.runnerId is unverified wire input, and the
    // consumer must be able to check the assignment (cross-tenant spoof guard,
    // review [P1] runner-exited provenance).
    expect(h.sink.records).toEqual([
      {
        runnerId: 'runner_dead',
        error: 'runner process exited with code 1 (log tail: boom)',
        owner: 'carol',
        workerId: 'host_abc',
      },
    ]);
    // And the hook fired with (runnerId, error, ctx) — same provenance.
    expect(fired).toEqual([
      [
        'runner_dead',
        'runner process exited with code 1 (log tail: boom)',
        { workerId: 'host_abc', owner: 'carol' },
      ],
    ]);
  });

  it('a failing onRunnerExited hook is swallowed; the tunnel still tears down cleanly', async () => {
    const h = makeHarness({
      onRunnerExited: async () => {
        throw new Error('hook boom');
      },
    });
    const exited: WorkerRunnerExitedFrame = {
      kind: WorkerFrameKind.RunnerExited,
      runnerId: 'runner_dead',
      error: 'cause',
    };
    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
      s.pushText(encodeWorkerFrame(exited));
      s.pushClose();
    });
    // Report still recorded; teardown still ran setOffline.
    expect(h.sink.records).toHaveLength(1);
    expect(h.store.offlineCalls).toEqual(['host_abc']);
  });

  it('records the report even with no sink wired (no onRunnerExited)', async () => {
    const h = makeHarness({ runnerExitReports: undefined });
    const exited: WorkerRunnerExitedFrame = {
      kind: WorkerFrameKind.RunnerExited,
      runnerId: 'runner_dead',
      error: 'cause',
    };
    // Must not throw when the sink is absent.
    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
      s.pushText(encodeWorkerFrame(exited));
      s.pushClose();
    });
    expect(h.registry.deregisterCalls).toBe(1);
  });
});

// ── Keepalive multiplexing + malformed frames ────────────

describe('WorkerTunnelServer frame multiplexing', () => {
  it('a runner-tunnel pong on the same socket is tolerated and dropped', async () => {
    const h = makeHarness();
    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
      s.pushText(encodeFrame({ kind: FrameKind.Pong, ts: 123 }));
      s.pushClose();
    });
    // No crash; the connection still tore down normally.
    expect(h.registry.registerCalls).toBe(1);
    expect(h.store.offlineCalls).toEqual(['host_abc']);
  });

  it('a malformed (non-worker, non-runner) frame is dropped and the tunnel survives', async () => {
    const h = makeHarness();
    let resolved = false;
    const ok: WorkerStopRunnerResultFrame = {
      kind: WorkerFrameKind.StopRunnerResult,
      requestId: 'after',
      status: 'stopped',
    };
    await runTunnel(h, new FakeHandshake(), async (s) => {
      s.pushText(helloText());
      const conn = await waitForConnection(h.registry);
      s.pushText('}{ not json');
      conn.pendingStops.set('after', { resolve: () => (resolved = true), reject: () => {} });
      // A valid frame after the garbage still routes — proving the loop survived.
      s.pushText(encodeWorkerFrame(ok));
      s.pushClose();
    });
    expect(resolved).toBe(true);
  });
});

// ── Teardown guards ──────────────────────────────────────

describe('WorkerTunnelServer teardown', () => {
  it('a successful connection deregisters, sets offline, and fires onWorkerDisconnect exactly once', async () => {
    let disconnects = 0;
    const h = makeHarness({
      onWorkerDisconnect: async () => {
        disconnects += 1;
      },
    });

    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
      s.pushClose();
    });

    expect(h.registry.deregisterCalls).toBe(1);
    expect(h.store.offlineCalls).toEqual(['host_abc']);
    expect(disconnects).toBe(1);
  });

  it('an upsert owner-conflict before register never touches the existing owner worker', async () => {
    let disconnects = 0;
    const h = makeHarness({
      onWorkerDisconnect: async () => {
        disconnects += 1;
      },
    });
    h.store.upsertError = new Error('owner conflict: host_abc already owned by someone else');

    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
    });

    // Never registered this connection -> cleanup must NOT run (cross-user DoS guard).
    expect(h.registry.registerCalls).toBe(0);
    expect(h.registry.deregisterCalls).toBe(0);
    expect(h.store.offlineCalls).toEqual([]);
    expect(disconnects).toBe(0);
    // The engine still best-effort closes its own socket.
    expect(h.socket.closed).toBeDefined();
  });

  it('a post-register loop rejection runs teardown exactly once (no double-fire)', async () => {
    // Regression for the disconnect double-fire on the post-register loop-error
    // path. When a loop REJECTS after register (here: the sender loop's
    // `sendText` rejects on a write-after-vanish), `Promise.race(loops)` rejects.
    // The inner `finally` already runs the full teardown (deregister + setOffline
    // + fireDisconnect); the rejection then unwinds into the outer `catch`, which
    // must NOT re-run it. Each of deregister / setOffline / the disconnect hook
    // must fire exactly once — otherwise reconnect reconciliation / session-
    // failure marking is double-processed.
    let disconnects = 0;

    // A socket whose first `sendText` rejects (simulating the underlying ws
    // vanishing mid-write). Everything else mirrors `FakeWorkerSocket`.
    class SendFailingWorkerSocket extends FakeWorkerSocket {
      override sendText(_data: string): Promise<void> {
        return Promise.reject(new Error('write after socket vanished'));
      }
    }

    const socket = new SendFailingWorkerSocket();
    const store = new FakeWorkerStore();
    const registry = new FakeWorkerRegistry();
    let resolveSleep: (() => void) | undefined;
    let sleepCount = 0;
    const server = new WorkerTunnelServer({
      registry,
      store,
      now: () => 1_000,
      onWorkerDisconnect: async () => {
        disconnects += 1;
      },
      // First ping tick enqueues a ping frame (within the liveness window, since
      // now === lastFrameAt); the sender then drains it and its `sendText`
      // rejects, taking down the loop race. Later sleeps park forever.
      sleep: () => {
        sleepCount += 1;
        if (sleepCount === 1) {
          return new Promise<void>((resolve) => {
            resolveSleep = resolve;
          });
        }
        return new Promise<void>(() => {});
      },
    });

    const done = server.handle(socket, new FakeHandshake(), 'host_abc');
    await Promise.resolve();
    socket.pushText(helloText());
    // Let the loops spin up and the ping loop park on the first sleep.
    for (let i = 0; i < 4; i += 1) {
      await Promise.resolve();
    }
    // Fire one ping interval: the heartbeat enqueues a ping, the sender drains it,
    // and its rejecting `sendText` collapses the loop race -> teardown.
    resolveSleep?.();
    await done;

    // Exactly-once on every teardown step, despite the rejection unwinding through
    // both the inner finally and the outer catch.
    expect(registry.deregisterCalls).toBe(1);
    expect(store.offlineCalls).toEqual(['host_abc']);
    expect(disconnects).toBe(1);
  });

  it('a post-register synchronous throw runs deregister + setOffline but NOT the disconnect hook', async () => {
    // Pins the unexpected-failure branch. The post-register inner `finally` is
    // the sole disconnect-hook source; the outer `catch` (an unexpected failure)
    // must NOT fire it, running deregister + setOffline ONLY.
    //
    // We force a throw in the narrow window AFTER `register` but BEFORE the inner
    // `try`: the first `now()` call in `handle` is the `conn.lastFrameAt =
    // this.now()` immediately after register, so a clock that throws on its first
    // call unwinds straight into the outer catch with `conn` set and
    // `teardownRan` still false — the exact residual window. The connection IS
    // registered, so cleanup runs; but the hook must stay silent.
    let disconnects = 0;
    let nowCalls = 0;
    const socket = new FakeWorkerSocket();
    const store = new FakeWorkerStore();
    const registry = new FakeWorkerRegistry();
    const server = new WorkerTunnelServer({
      registry,
      store,
      sleep: neverSleep(),
      now: () => {
        nowCalls += 1;
        if (nowCalls === 1) {
          throw new Error('clock exploded just after register');
        }
        return 1_000;
      },
      onWorkerDisconnect: async () => {
        disconnects += 1;
      },
    });

    const done = server.handle(socket, new FakeHandshake(), 'host_abc');
    await Promise.resolve();
    socket.pushText(helloText());
    await done;

    // The connection registered, so the outer catch's pre-register guard does not
    // apply: deregister + setOffline run.
    expect(registry.registerCalls).toBe(1);
    expect(registry.deregisterCalls).toBe(1);
    expect(store.offlineCalls).toEqual(['host_abc']);
    // But the disconnect hook is NEVER fired from the generic-exception branch.
    expect(disconnects).toBe(0);
    // The engine still best-effort closes its own socket.
    expect(socket.closed).toBeDefined();
  });

  it('the connect hook timing out does not block teardown', async () => {
    const h = makeHarness({
      onWorkerConnect: () =>
        new Promise<void>(() => {
          // never resolves -> times out
        }),
      onWorkerConnectTimeoutMs: 5,
    });

    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
      s.pushClose();
    });

    // Despite the hung connect hook, the tunnel reached teardown.
    expect(h.registry.deregisterCalls).toBe(1);
    expect(h.store.offlineCalls).toEqual(['host_abc']);
  });

  it('teardown rejects every in-flight pending waiter with WorkerTunnelClosedError', async () => {
    const h = makeHarness();
    let launchRejection: unknown;
    let statRejection: unknown;

    await runTunnel(h, new FakeHandshake(), async (s) => {
      s.pushText(helloText());
      const conn = await waitForConnection(h.registry);
      // Two waiters the worker never answers before the socket dies.
      conn.pendingLaunches.set('req_hang', {
        resolve: () => {},
        reject: (reason) => (launchRejection = reason),
      });
      conn.pendingStats.set('req_stat_hang', {
        resolve: () => {},
        reject: (reason) => (statRejection = reason),
      });
      s.pushClose();
    });

    // Both waiters were settled (rejected), not left hanging, and the maps are
    // drained so nothing leaks.
    expect(launchRejection).toBeInstanceOf(WorkerTunnelClosedError);
    expect(statRejection).toBeInstanceOf(WorkerTunnelClosedError);
    expect(h.registry.lastConn!.pendingLaunches.size).toBe(0);
    expect(h.registry.lastConn!.pendingStats.size).toBe(0);
  });

  it('a setOffline failure at teardown still fires the disconnect hook', async () => {
    const disconnects: string[] = [];
    const h = makeHarness({
      onWorkerDisconnect: async (workerId) => {
        disconnects.push(workerId);
      },
    });
    h.store.offlineError = new Error('store down');

    await runTunnel(h, new FakeHandshake(), (s) => {
      s.pushText(helloText());
      s.pushClose();
    });

    // setOffline threw, but reconnect reconciliation still ran exactly once.
    expect(disconnects).toEqual(['host_abc']);
    expect(h.registry.deregisterCalls).toBe(1);
  });

  it('a superseded connection skips deregister/setOffline/hook but settles its own waiters', async () => {
    const disconnects: string[] = [];
    const h = makeHarness({
      onWorkerDisconnect: async (workerId) => {
        disconnects.push(workerId);
      },
    });
    let rejection: unknown;

    await runTunnel(h, new FakeHandshake(), async (s) => {
      s.pushText(helloText());
      const conn = await waitForConnection(h.registry);
      conn.pendingLaunches.set('req_orphan', {
        resolve: () => {},
        reject: (reason) => (rejection = reason),
      });
      // Simulate a newest-wins replacement: the worker re-dialed and a second
      // handle registered a fresh connection, superseding this one.
      h.registry.conn = makeConnection('replacement-owner');
      // Now the stale connection's socket dies.
      s.pushClose();
    });

    // The stale teardown must not touch shared state keyed by workerId…
    expect(h.registry.deregisterCalls).toBe(0);
    expect(h.store.offlineCalls).toEqual([]);
    expect(disconnects).toEqual([]);
    // …but it still settles its own waiters and closes its own socket.
    expect(rejection).toBeInstanceOf(WorkerTunnelClosedError);
    expect(h.registry.lastConn!.pendingLaunches.size).toBe(0);
    expect(h.socket.closed).toBeDefined();
  });
});

// ── Ping watchdog ────────────────────────────────────────

describe('WorkerTunnelServer ping loop', () => {
  it('each live interval persists a heartbeat and enqueues a ping frame', async () => {
    // A controllable clock + a sleep we resolve exactly once. After the first
    // tick the worker is still "live" (now == lastFrameAt), so the loop heartbeats
    // and enqueues a ping; the registry's sender drains it to the socket.
    const nowMs = 1_000;
    let resolveSleep: (() => void) | undefined;
    let sleepCount = 0;
    const socket = new FakeWorkerSocket();
    const store = new FakeWorkerStore();
    const registry = new FakeWorkerRegistry();
    const server = new WorkerTunnelServer({
      registry,
      store,
      now: () => nowMs,
      sleep: () => {
        sleepCount += 1;
        if (sleepCount === 1) {
          return new Promise<void>((resolve) => {
            resolveSleep = resolve;
          });
        }
        // Subsequent sleeps park forever so only one tick fires.
        return new Promise<void>(() => {});
      },
    });

    const done = server.handle(socket, new FakeHandshake(), 'host_abc');
    await Promise.resolve();
    socket.pushText(helloText());
    // Let the loops spin up and park on the first sleep.
    await Promise.resolve();
    await Promise.resolve();
    // Fire one ping interval while still within the liveness window.
    resolveSleep?.();
    // Allow the ping loop to run heartbeat + enqueue, and the sender to drain.
    for (let i = 0; i < 5; i += 1) {
      await Promise.resolve();
    }

    expect(store.heartbeats).toBeGreaterThanOrEqual(1);
    // The ping frame reached the socket via the sender loop.
    const pinged = socket.sent.some((t) => t.includes('"kind":"ping"'));
    expect(pinged).toBe(true);

    // Close to tear down.
    socket.pushClose();
    await done;
  });

  it('a worker silent past the miss threshold is closed with 4003', async () => {
    // The clock advances past PING_MISS_THRESHOLD intervals between hello and the
    // tick, so the loop declares the worker dead and closes 4003.
    let nowMs = 1_000;
    let resolveSleep: (() => void) | undefined;
    let sleepCount = 0;
    const socket = new FakeWorkerSocket();
    const store = new FakeWorkerStore();
    const registry = new FakeWorkerRegistry();
    const server = new WorkerTunnelServer({
      registry,
      store,
      now: () => nowMs,
      sleep: () => {
        sleepCount += 1;
        if (sleepCount === 1) {
          return new Promise<void>((resolve) => {
            resolveSleep = resolve;
          });
        }
        return new Promise<void>(() => {});
      },
    });

    const done = server.handle(socket, new FakeHandshake(), 'host_abc');
    await Promise.resolve();
    socket.pushText(helloText());
    await Promise.resolve();
    await Promise.resolve();
    // Advance the clock well past the miss window, then fire the tick.
    nowMs = 1_000 + PING_INTERVAL_MS * (PING_MISS_THRESHOLD + 1);
    resolveSleep?.();
    await done;

    expect(socket.closed?.code).toBe(PING_TIMEOUT_CLOSE_CODE);
    expect(socket.closed?.reason).toBe('ping timeout');
    // No heartbeat was persisted on the dead tick.
    expect(store.heartbeats).toBe(0);
  });

  it('a heartbeat store failure does NOT tear down a healthy tunnel', async () => {
    // A store blip on the last-seen update must not end the ping loop: the loop
    // is in the teardown race, so ending it would deregister + offline a worker
    // whose socket is perfectly healthy (and every tunnel on the replica shares
    // the store, so one DB hiccup would wedge them all).
    const nowMs = 1_000;
    let resolveSleep: (() => void) | undefined;
    let sleepCount = 0;
    const socket = new FakeWorkerSocket();
    const store = new FakeWorkerStore();
    store.heartbeatError = new Error('postgres blip');
    const registry = new FakeWorkerRegistry();
    const server = new WorkerTunnelServer({
      registry,
      store,
      now: () => nowMs,
      sleep: () => {
        sleepCount += 1;
        if (sleepCount === 1) {
          return new Promise<void>((resolve) => {
            resolveSleep = resolve;
          });
        }
        return new Promise<void>(() => {});
      },
    });

    const done = server.handle(socket, new FakeHandshake(), 'host_abc');
    await Promise.resolve();
    socket.pushText(helloText());
    await Promise.resolve();
    await Promise.resolve();
    // Fire one live tick whose heartbeat throws.
    resolveSleep?.();
    for (let i = 0; i < 5; i += 1) {
      await Promise.resolve();
    }

    // The heartbeat was attempted and failed — yet no teardown ran…
    expect(store.heartbeats).toBe(1);
    expect(registry.deregisterCalls).toBe(0);
    expect(store.offlineCalls).toEqual([]);
    // …and the loop even proceeded to enqueue its ping for the live worker.
    const pinged = socket.sent.some((t) => t.includes('"kind":"ping"'));
    expect(pinged).toBe(true);

    // The tunnel ends only when the peer actually closes.
    socket.pushClose();
    await done;
    expect(registry.deregisterCalls).toBe(1);
    expect(store.offlineCalls).toEqual(['host_abc']);
  });
});
