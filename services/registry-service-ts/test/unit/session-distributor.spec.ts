// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the claim-based session distributor.
//
// The distributor is the single distributor that ties a client session, the
// worker (environment-worker) tunnel, and the runner tunnel together for a
// `target=self_hosted` environment. This spec covers it WITHOUT a DB or real
// sockets: the `TunnelRegistry` + `WorkerRegistry` are the real in-memory
// registries (Node single-loop, network-free), the session store is an
// in-memory fake implementing the small `DistributionSessionStore` seam, and a
// fake worker connection stands in for a live worker tunnel — exactly the way the
// worker-tunnel engine's own unit tests drive a `WorkerConnection`.
//
// Two layers are exercised:
//   1. PURE decision logic (no I/O): `mintBindingToken` + `expectedRunnerIdFor`
//      (the token-bound runner-id derivation both sides reproduce),
//      `isDispatchable` (self_hosted + a live claimed worker), and the
//      pending/assigned/failed state-field transitions.
//   2. The `SessionDistributor` orchestration: a self_hosted create with a
//      connected worker mints a binding token, derives the expected runner id,
//      PRE-REGISTERS it in the TunnelRegistry, sends a `worker.launch_runner`
//      frame down the worker's worker tunnel, and persists the session PENDING ->
//      (runner connects) ASSIGNED, or (no worker / launch refusal / runner
//      exited) the pending/failed branches.

import { describe, it, expect } from 'vitest';
import {
  FrameKind,
  WorkerFrameKind,
  decodeWorkerFrame,
  tokenBoundRunnerId,
  type WorkerFrame,
  type WorkerHelloFrame,
  type WorkerLaunchRunnerFrame,
  type WorkerLaunchResult,
  type HelloFrame,
} from '@orca/harness-tunnel';
import { TunnelRegistry, type RegistryWebSocketLike } from '../../src/tunnel/tunnel-registry.js';
import {
  WorkerRegistry,
  WorkerConnectionReplacedError,
  type WorkerConnection,
} from '../../src/tunnel/worker-registry.js';
import {
  SessionDistributor,
  mintBindingToken,
  expectedRunnerIdFor,
  isDispatchable,
  providerServedByWorker,
  providerAdvertisedByRunner,
  pendingDistributionPatch,
  assignedDistributionPatch,
  failedDistributionPatch,
  DISTRIBUTION_PENDING,
  DISTRIBUTION_ASSIGNED,
  DISTRIBUTION_FAILED,
  CAPABILITY_MISMATCH_ERROR_CODE,
  type DispatchableEnvironment,
  type DistributionSessionRow,
  type DistributionSessionStore,
  type DistributionSessionPatch,
  type EnvironmentTargetLookup,
} from '../../src/tunnel/session-distributor.js';
import { resolveAgentMode } from '../../src/domain/agent-snapshot-resolver.js';

// ── In-memory session store implementing the distributor seam ──

/**
 * A tiny in-memory `DistributionSessionStore`. Holds one row per session id and
 * records every patch the distributor writes, so the spec can assert BOTH the
 * final persisted row and the ordered sequence of transitions (PENDING before a
 * later ASSIGNED / FAILED) without a DB. `replaceRunnerId` is the connection-
 * scoped binding rewrite the distributor uses when it re-mints a token.
 */
class FakeSessionStore implements DistributionSessionStore {
  readonly patches: Array<{ id: string; patch: DistributionSessionPatch }> = [];
  private readonly rows = new Map<string, DistributionSessionRow>();

  seed(row: DistributionSessionRow): this {
    this.rows.set(row.id, { ...row });
    return this;
  }

  get(id: string): DistributionSessionRow | undefined {
    const row = this.rows.get(id);
    return row === undefined ? undefined : { ...row };
  }

  async load(id: string): Promise<DistributionSessionRow | null> {
    const row = this.rows.get(id);
    return row === undefined ? null : { ...row };
  }

  async applyDistributionPatch(id: string, patch: DistributionSessionPatch): Promise<boolean> {
    const row = this.rows.get(id);
    if (row === undefined) {
      return false;
    }
    this.patches.push({ id, patch: { ...patch } });
    this.rows.set(id, { ...row, ...patch });
    return true;
  }

  async findBoundSessionId(runnerId: string): Promise<string | null> {
    for (const row of this.rows.values()) {
      if (row.runnerId === runnerId) {
        return row.id;
      }
    }
    return null;
  }

  async loadPendingForEnvironment(environmentId: string): Promise<Array<{ id: string }>> {
    // Mirror the Drizzle predicate: PENDING with NO runner binding, scoped to the
    // environment — the create-time-stranded set a worker connect must drive.
    const refs: Array<{ id: string }> = [];
    for (const row of this.rows.values()) {
      if (
        row.environmentId === environmentId &&
        row.distributionState === DISTRIBUTION_PENDING &&
        row.runnerId === null
      ) {
        refs.push({ id: row.id });
      }
    }
    return refs;
  }
}

// ── A live worker connection (captures the launch frame) ──

/**
 * Build a `WorkerConnection` record the way the `WorkerRegistry` hands one out, plus
 * a helper that drains the outbound queue so a test can read the
 * `worker.launch_runner` frame the distributor enqueued and answer it with a
 * launch-result frame routed through the connection's pending waiter (the same
 * machinery `WorkerTunnelServer`'s receive loop drives in production).
 */
function makeWorkerConn(
  registry: WorkerRegistry,
  environmentId: string,
  owner = 'ws_acme',
  configuredHarnesses: Record<string, boolean> | null = null,
): {
  conn: WorkerConnection;
  drainLaunch: () => Promise<WorkerLaunchRunnerFrame>;
} {
  const hello: WorkerHelloFrame = {
    kind: WorkerFrameKind.Hello,
    version: '0.1.0-test',
    frameProtocolVersion: 1,
    name: 'worker-01',
    runners: [],
    configuredHarnesses,
  };
  // The WorkerRegistry holds no socket ref; a placeholder satisfies the signature.
  const conn = registry.register(environmentId, {} as never, hello, { owner });

  async function drainLaunch(): Promise<WorkerLaunchRunnerFrame> {
    const text = await conn.outboundQueue.get();
    if (text === null) {
      throw new Error('outbound queue was poisoned before a launch frame arrived');
    }
    const frame: WorkerFrame = decodeWorkerFrame(text);
    if (frame.kind !== WorkerFrameKind.LaunchRunner) {
      throw new Error(`expected worker.launch_runner, got ${frame.kind}`);
    }
    return frame;
  }

  return { conn, drainLaunch };
}

/** Resolve the worker's launch-result frame into the matching pending waiter. */
function answerLaunch(
  conn: WorkerConnection,
  requestId: string,
  result: Omit<WorkerLaunchResult, 'requestId'>,
): void {
  const waiter = conn.pendingLaunches.get(requestId);
  if (waiter === undefined) {
    throw new Error(`no pending launch waiter for ${requestId}`);
  }
  conn.pendingLaunches.delete(requestId);
  waiter.resolve({ requestId, ...result } as WorkerLaunchResult);
}

/** A no-op runner-tunnel socket: the registry only ever drains its outbound queue. */
const NOOP_RUNNER_WS: RegistryWebSocketLike = {
  sendText: async () => {},
  receiveText: () => new Promise<string>(() => {}),
};

/** Register a runner session against the tunnel registry (the dialing runner). */
function connectRunner(
  registry: TunnelRegistry,
  runnerId: string,
  owner = 'ws_acme',
  harnesses: string[] = [],
): void {
  const hello: HelloFrame = {
    kind: FrameKind.Hello,
    runnerVersion: '0.1.0-test',
    frameProtocolVersion: 1,
    harnesses,
    envs: [],
  };
  registry.register(runnerId, NOOP_RUNNER_WS, hello, { owner });
}

const ENV_ID = 'env_self_hosted_1';
const SESSION_ID = 'ses_dist_1';
const WORKSPACE_ID = 'ws_acme';

function selfHostedEnv(over: Partial<DispatchableEnvironment> = {}): DispatchableEnvironment {
  return { id: ENV_ID, target: 'self_hosted', ...over };
}

/**
 * Fake `EnvironmentTargetLookup` resolving every id to `self_hosted` — matches
 * `selfHostedEnv()`'s default and every existing spec in this file, which
 * predates the environments seam entirely. Cloud-specific specs build their
 * own lookup so the resolved target is explicit at the call site.
 */
const SELF_HOSTED_LOOKUP: EnvironmentTargetLookup = {
  async loadTarget() {
    return 'self_hosted';
  },
};

/**
 * Build a fake `EnvironmentTargetLookup` that resolves a fixed map of id ->
 * target and records every id it was asked to resolve, so a test can assert
 * the distributor actually CONSULTS the lookup (rather than hardcoding a
 * target) — the two dispatchable targets behave identically once resolved, so
 * `calls` is the only way to observe that the lookup was the source of truth.
 */
function targetLookup(map: Record<string, string | null>): EnvironmentTargetLookup & {
  calls: string[];
} {
  const calls: string[] = [];
  return {
    calls,
    async loadTarget(environmentId: string) {
      calls.push(environmentId);
      return environmentId in map ? map[environmentId]! : null;
    },
  };
}

/**
 * Seed the default session row. `agentMode` defaults to `'colocated'` — the
 * pre-existing self_hosted tests in this file are mode-AGNOSTIC (self_hosted
 * never reads `agentMode`, so any value works), and the pre-existing cloud
 * tests below all exercise the "worker connected -> dispatch succeeds" path,
 * which now additionally requires `colocated`. Tests that specifically probe
 * the cloud mode gate override it explicitly via `over`.
 */
function seededSession(
  store: FakeSessionStore,
  over: Partial<DistributionSessionRow> = {},
): FakeSessionStore {
  return store.seed({
    id: SESSION_ID,
    workspaceId: WORKSPACE_ID,
    environmentId: ENV_ID,
    runnerId: null,
    hostEnvironmentId: null,
    distributionState: null,
    agentHarness: 'claude-sdk',
    agentMode: 'colocated',
    agentHarnessType: 'mock',
    workspace: '/workspace',
    ...over,
  });
}

// ── 1. Pure decision logic ───────────────────────────────

describe('session-distributor — binding-token mint + expected runner id', () => {
  it('mints a non-empty url-safe binding token each call (unique)', () => {
    const a = mintBindingToken();
    const b = mintBindingToken();
    expect(a.length).toBeGreaterThan(0);
    expect(b.length).toBeGreaterThan(0);
    expect(a).not.toBe(b);
    // url-safe base64 alphabet only (no `+`/`/`/`=`), so it rides a header / env.
    expect(/^[A-Za-z0-9_-]+$/.test(a)).toBe(true);
  });

  it('derives the expected runner id byte-for-byte from the binding token', () => {
    const token = 'a-fixed-binding-token-value';
    // The distributor's derivation MUST equal the identity helper both the
    // runner and the runner-tunnel route reproduce — otherwise the dialing
    // runner is never matched to the pre-registered id.
    expect(expectedRunnerIdFor(token)).toBe(tokenBoundRunnerId(token));
    expect(expectedRunnerIdFor(token)).toMatch(/^runner_token_[0-9a-f]{32}$/);
  });

  it('throws on an empty binding token (matches the identity helper)', () => {
    expect(() => expectedRunnerIdFor('   ')).toThrow();
  });
});

describe('session-distributor — isDispatchable', () => {
  it('is dispatchable for a self_hosted environment with a live connected worker', () => {
    const registry = new WorkerRegistry();
    makeWorkerConn(registry, ENV_ID);
    expect(isDispatchable(selfHostedEnv(), registry)).toBe(true);
  });

  it('is dispatchable for a cloud environment with a live connected worker (un-gated)', () => {
    // A cloud environment's worker is provisioned + started by the
    // environment-launch lifecycle rather than an operator, but once it is
    // connected, dispatchability is identical to self_hosted — the distributor
    // does not care who started the worker.
    const registry = new WorkerRegistry();
    makeWorkerConn(registry, ENV_ID);
    expect(isDispatchable(selfHostedEnv({ target: 'cloud' }), registry)).toBe(true);
  });

  it('is NOT dispatchable for an unrecognized target even with a connected worker', () => {
    const registry = new WorkerRegistry();
    makeWorkerConn(registry, ENV_ID);
    expect(isDispatchable(selfHostedEnv({ target: 'some_future_target' }), registry)).toBe(false);
    expect(isDispatchable(selfHostedEnv({ target: null }), registry)).toBe(false);
  });

  it('is NOT dispatchable for a cloud environment with no connected worker', () => {
    const registry = new WorkerRegistry();
    expect(isDispatchable(selfHostedEnv({ target: 'cloud' }), registry)).toBe(false);
  });

  it('is NOT dispatchable for a self_hosted environment with no connected worker', () => {
    const registry = new WorkerRegistry();
    expect(isDispatchable(selfHostedEnv(), registry)).toBe(false);
  });

  it('is NOT dispatchable for a null/absent environment', () => {
    const registry = new WorkerRegistry();
    makeWorkerConn(registry, ENV_ID);
    expect(isDispatchable(null, registry)).toBe(false);
  });
});

describe('session-distributor — providerServedByWorker (capability-match decision)', () => {
  it('serves a provider that is advertised ready (true) in the worker map', () => {
    expect(providerServedByWorker('claude-sdk', { 'claude-sdk': true })).toBe(true);
  });

  it('does NOT serve a provider missing from the worker map', () => {
    expect(providerServedByWorker('claude-sdk', { 'codex-cli': true })).toBe(false);
  });

  it('does NOT serve a provider present but NOT ready (false) in the worker map', () => {
    expect(providerServedByWorker('claude-sdk', { 'claude-sdk': false })).toBe(false);
  });

  it('fails OPEN (served) when the worker advertises no map (null)', () => {
    // An older worker that does not advertise its configured set: the pre-spawn
    // check is skipped and the worker's own launch-result guards instead.
    expect(providerServedByWorker('claude-sdk', null)).toBe(true);
  });

  it('fails OPEN (served) when the session has no resolvable provider (null)', () => {
    // Nothing to check — the launch frame carries harness=null and the worker
    // fails open on its own configured-harness check too.
    expect(providerServedByWorker(null, { 'claude-sdk': true })).toBe(true);
    expect(providerServedByWorker(null, null)).toBe(true);
  });
});

describe('session-distributor — providerAdvertisedByRunner (connected-runner capability-match)', () => {
  it('serves a provider present in the runner advertised set', () => {
    expect(providerAdvertisedByRunner('claude-sdk', ['claude-sdk', 'codex-cli'])).toBe(true);
  });

  it('does NOT serve a provider absent from a non-empty advertised set', () => {
    expect(providerAdvertisedByRunner('claude-sdk', ['codex-cli'])).toBe(false);
  });

  it('fails OPEN (served) when the runner advertised an EMPTY set', () => {
    expect(providerAdvertisedByRunner('claude-sdk', [])).toBe(true);
  });

  it('fails OPEN (served) when the advertised set is undefined', () => {
    expect(providerAdvertisedByRunner('claude-sdk', undefined)).toBe(true);
  });

  it('fails OPEN (served) when the session has no resolvable provider', () => {
    expect(providerAdvertisedByRunner(null, ['codex-cli'])).toBe(true);
  });
});

describe('session-distributor — state-field transitions (pure patches)', () => {
  it('pending patch records the runner binding + environment + PENDING state', () => {
    const patch = pendingDistributionPatch({
      runnerId: 'runner_token_abc',
      hostEnvironmentId: ENV_ID,
    });
    expect(patch).toEqual({
      runnerId: 'runner_token_abc',
      hostEnvironmentId: ENV_ID,
      distributionState: DISTRIBUTION_PENDING,
    });
    expect(DISTRIBUTION_PENDING).toBe('pending');
  });

  it('assigned patch flips only the distribution_state to ASSIGNED', () => {
    const patch = assignedDistributionPatch();
    expect(patch).toEqual({ distributionState: DISTRIBUTION_ASSIGNED });
    expect(DISTRIBUTION_ASSIGNED).toBe('assigned');
  });

  it('failed patch flips the distribution_state to FAILED', () => {
    const patch = failedDistributionPatch();
    expect(patch).toEqual({ distributionState: DISTRIBUTION_FAILED });
    expect(DISTRIBUTION_FAILED).toBe('failed');
  });
});

// ── 2. SessionDistributor orchestration ───────────────────

describe('SessionDistributor.dispatch — connected worker (happy path)', () => {
  it('mints a token, pre-registers the runner id, sends launch_runner, persists PENDING', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);

    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;

    // The session is PENDING with a bound runner id + worker environment id.
    expect(outcome.state).toBe(DISTRIBUTION_PENDING);
    const expectedRunnerId = expectedRunnerIdFor(launch.bindingToken);
    expect(outcome.runnerId).toBe(expectedRunnerId);
    const row = store.get(SESSION_ID)!;
    expect(row.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(row.runnerId).toBe(expectedRunnerId);
    expect(row.hostEnvironmentId).toBe(ENV_ID);

    // The expected runner was PRE-REGISTERED as a connect-waiter so the dialing
    // runner is matched: a waiter exists for the derived id.
    expect(tunnelRegistry.connectWaiterCount(expectedRunnerId)).toBe(1);

    // The launch frame carries the session's workspace + canonical harness.
    expect(launch.workspace).toBe('/workspace');
    expect(launch.harness).toBe('claude-sdk');
    expect(launch.bindingToken.length).toBeGreaterThan(0);

    // A pending launch waiter is registered against the worker connection, so
    // the worker's launch-result frame can resolve it. Answer it to release the
    // bounded launch-result wait the dispatch started.
    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId: expectedRunnerId });
    await outcome.launchSettled;
  });

  it('reaches ASSIGNED once the matched runner connects after launch', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    const expectedRunnerId = expectedRunnerIdFor(launch.bindingToken);
    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId: expectedRunnerId });
    await outcome.launchSettled;

    // The runner dials its tunnel and registers under the derived id. The
    // distributor's connect hook flips the session ASSIGNED.
    connectRunner(tunnelRegistry, expectedRunnerId);
    await distributor.onRunnerConnect(expectedRunnerId);

    const row = store.get(SESSION_ID)!;
    expect(row.distributionState).toBe(DISTRIBUTION_ASSIGNED);
    // The transition order is PENDING then ASSIGNED.
    const states = store.patches.map((p) => p.patch.distributionState).filter(Boolean);
    expect(states).toEqual([DISTRIBUTION_PENDING, DISTRIBUTION_ASSIGNED]);
  });

  it('passes harness=null in the launch frame when the agent harness is unknown', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = new FakeSessionStore().seed({
      id: SESSION_ID,
      workspaceId: WORKSPACE_ID,
      environmentId: ENV_ID,
      runnerId: null,
      hostEnvironmentId: null,
      distributionState: null,
      agentHarness: null,
      agentMode: 'colocated',
      agentHarnessType: 'mock',
      workspace: null,
    });
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    // No agent harness resolvable -> the worker-side configured-harness check is
    // skipped (fail open). Workspace also null -> empty-string sentinel so the
    // wire frame's required field stays total.
    expect(launch.harness).toBeNull();
    expect(launch.workspace).toBe('');
    answerLaunch(conn, launch.requestId, {
      status: 'launched',
      runnerId: expectedRunnerIdFor(launch.bindingToken),
    });
    await outcome.launchSettled;
  });
});

describe('SessionDistributor.dispatch — capability-match (provider vs worker harnesses)', () => {
  it('refuses BEFORE launch when the session provider is not in the worker configured harnesses', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore()); // agentHarness: 'claude-sdk'
    // The worker advertises a DIFFERENT harness set — it cannot serve 'claude-sdk'.
    const { conn } = makeWorkerConn(workerRegistry, ENV_ID, WORKSPACE_ID, { 'codex-cli': true });
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });

    // Fail-fast: no launch frame was enqueued, no runner id minted, no connect
    // waiter pre-registered — the runner that can't serve the provider is never
    // spawned.
    expect(outcome.runnerId).toBeNull();
    const settlement = await outcome.launchSettled;
    expect(settlement.status).toBe('capability_mismatch');
    expect(settlement.errorCode).toBe(CAPABILITY_MISMATCH_ERROR_CODE);
    // The worker connection's outbound queue is empty (no launch sent). A
    // non-blocking drain: put a sentinel and confirm it is the FIRST item out.
    conn.outboundQueue.put(null);
    expect(await conn.outboundQueue.get()).toBeNull();
    // No connect waiter was registered for any runner.
    expect(tunnelRegistry.connectWaiterCount()).toBe(0);
    // The session is marked FAILED (a clear capability-mismatch failure).
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_FAILED);
    expect(store.get(SESSION_ID)!.runnerId).toBeNull();
  });

  it('refuses when the provider is present in the map but NOT ready (false)', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    // 'claude-sdk' is known to the worker but not configured/ready.
    const { conn } = makeWorkerConn(workerRegistry, ENV_ID, WORKSPACE_ID, { 'claude-sdk': false });
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const settlement = await outcome.launchSettled;
    expect(settlement.status).toBe('capability_mismatch');
    expect(outcome.runnerId).toBeNull();
    conn.outboundQueue.put(null);
    expect(await conn.outboundQueue.get()).toBeNull();
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_FAILED);
  });

  it('dispatches normally when the provider IS in the worker configured harnesses (ready)', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID, WORKSPACE_ID, {
      'claude-sdk': true,
      'codex-cli': true,
    });
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    // The advertised, ready provider is dispatched exactly as the happy path.
    expect(outcome.runnerId).toBe(expectedRunnerIdFor(launch.bindingToken));
    expect(launch.harness).toBe('claude-sdk');
    answerLaunch(conn, launch.requestId, {
      status: 'launched',
      runnerId: expectedRunnerIdFor(launch.bindingToken),
    });
    await outcome.launchSettled;
  });

  it('fails OPEN when the worker advertises NO configured-harness map (null)', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    // configuredHarnesses defaults to null — an older worker that does not
    // advertise its set. The pre-spawn check is skipped; the worker's own
    // launch-result still guards, so dispatch proceeds.
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    expect(outcome.runnerId).toBe(expectedRunnerIdFor(launch.bindingToken));
    answerLaunch(conn, launch.requestId, {
      status: 'launched',
      runnerId: expectedRunnerIdFor(launch.bindingToken),
    });
    await outcome.launchSettled;
  });

  it('fails OPEN when the session has no resolvable provider (agentHarness null)', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = new FakeSessionStore().seed({
      id: SESSION_ID,
      workspaceId: WORKSPACE_ID,
      environmentId: ENV_ID,
      runnerId: null,
      hostEnvironmentId: null,
      distributionState: null,
      agentHarness: null,
      agentMode: 'colocated',
      agentHarnessType: 'mock',
      workspace: null,
    });
    // Even a strict worker set does not refuse: there is no provider to check.
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID, WORKSPACE_ID, {
      'claude-sdk': true,
    });
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    expect(outcome.runnerId).toBe(expectedRunnerIdFor(launch.bindingToken));
    answerLaunch(conn, launch.requestId, {
      status: 'launched',
      runnerId: expectedRunnerIdFor(launch.bindingToken),
    });
    await outcome.launchSettled;
  });
});

describe('SessionDistributor.dispatch — no connected worker', () => {
  it('leaves the session PENDING without minting a binding or sending a frame', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry(); // no worker connected
    const store = seededSession(new FakeSessionStore());
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });

    expect(outcome.state).toBe(DISTRIBUTION_PENDING);
    expect(outcome.runnerId).toBeNull();
    const row = store.get(SESSION_ID)!;
    // PENDING with no runner binding — the work depth surfaces it; a later
    // worker connect drives the launch (out of this unit's scope).
    expect(row.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(row.runnerId).toBeNull();
    expect(row.hostEnvironmentId).toBeNull();
    // Nothing was pre-registered (no token was minted).
    expect(tunnelRegistry.connectWaiterCount()).toBe(0);
  });
});

describe('SessionDistributor.dispatch — launch refusal + send failure', () => {
  it('marks the session FAILED when the worker refuses the launch (harness not configured)', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    answerLaunch(conn, launch.requestId, {
      status: 'failed',
      runnerId: null,
      error: "harness 'claude-sdk' is not configured on the worker",
      errorCode: 'harness_not_configured',
    });
    const settled = await outcome.launchSettled;

    expect(settled.status).toBe('failed');
    const row = store.get(SESSION_ID)!;
    expect(row.distributionState).toBe(DISTRIBUTION_FAILED);
    // The pre-registered connect-waiter was cleaned up on the refusal (the
    // runner will never dial).
    expect(tunnelRegistry.connectWaiterCount(outcome.runnerId!)).toBe(0);
  });

  it('leaves the session PENDING (clean binding rollback) when the worker connection was replaced at send', async () => {
    const tunnelRegistry = new TunnelRegistry();
    // A registry whose `sendText` always throws the documented replaced-connection
    // error — the newest-wins race where the worker tunnel was superseded between
    // the distributor resolving the connection and enqueuing the launch frame.
    class ReplacedSendRegistry extends WorkerRegistry {
      override sendText(conn: WorkerConnection): never {
        throw new WorkerConnectionReplacedError(conn.workerId);
      }
    }
    const workerRegistry = new ReplacedSendRegistry();
    const store = seededSession(new FakeSessionStore());
    makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });

    // The frame never left: the binding is rolled back to a clean PENDING (no
    // runner id, no worker environment) so a retry — a later worker reconnect —
    // starts from a clean slate, and the pre-registered connect-waiter is gone.
    expect(outcome.state).toBe(DISTRIBUTION_PENDING);
    expect(outcome.runnerId).toBeNull();
    const row = store.get(SESSION_ID)!;
    expect(row.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(row.runnerId).toBeNull();
    expect(row.hostEnvironmentId).toBeNull();
    expect(tunnelRegistry.connectWaiterCount()).toBe(0);
  });

  it('leaves the session PENDING (clean binding rollback) on an UNEXPECTED send error', async () => {
    const tunnelRegistry = new TunnelRegistry();
    // A registry whose `sendText` throws a generic (NOT WorkerConnectionReplaced)
    // error: an unexpected fault enqueuing the frame. The launch frame still never
    // left the registry, so the runner will never dial the derived id — the
    // connect hook cannot assign the binding. A non-null runner_id would also make
    // the worker-reconnect drive (scoped on a NULL runner_id) skip the session
    // forever. So this path must roll back to a CLEAN PENDING exactly like the
    // replaced-connection one, leaving the session recoverable on reconnect.
    class FaultySendRegistry extends WorkerRegistry {
      override sendText(): never {
        throw new Error('unexpected socket write failure');
      }
    }
    const workerRegistry = new FaultySendRegistry();
    const store = seededSession(new FakeSessionStore());
    makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const settled = await outcome.launchSettled;

    // Same recoverable outcome as the replaced-connection branch: no runner id, no
    // worker environment, no stranded connect-waiter — so the reconnect drive picks
    // it up (it filters on a NULL runner_id).
    expect(settled.status).toBe('send_failed');
    expect(outcome.state).toBe(DISTRIBUTION_PENDING);
    expect(outcome.runnerId).toBeNull();
    const row = store.get(SESSION_ID)!;
    expect(row.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(row.runnerId).toBeNull();
    expect(row.hostEnvironmentId).toBeNull();
    expect(tunnelRegistry.connectWaiterCount()).toBe(0);
  });
});

describe('SessionDistributor.markRunnerExited — worker.runner_exited', () => {
  it('flips the bound session to FAILED on a runner-exited report', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    const runnerId = expectedRunnerIdFor(launch.bindingToken);
    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId });
    await outcome.launchSettled;

    // The worker reports the runner died before it ever connected its own tunnel.
    await distributor.markRunnerExited(runnerId, 'runner process exited with code 1');

    const row = store.get(SESSION_ID)!;
    expect(row.distributionState).toBe(DISTRIBUTION_FAILED);
  });

  it('is a no-op when the runner id matches no bound session', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    await distributor.markRunnerExited('runner_token_unrelated', 'boom');
    // The seeded session was never bound to that runner, so nothing changed.
    expect(store.patches).toEqual([]);
  });
});

describe('SessionDistributor.onRunnerConnect — unrelated runner', () => {
  it('is a no-op when the connecting runner matches no bound session', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    connectRunner(tunnelRegistry, 'runner_token_unrelated');
    await distributor.onRunnerConnect('runner_token_unrelated');
    expect(store.patches).toEqual([]);
  });
});

describe('SessionDistributor.onRunnerConnect — capability-match against the connected runner', () => {
  /** Bind a session to a runner id directly (skip the dispatch machinery). */
  function bindSessionToRunner(store: FakeSessionStore, runnerId: string): void {
    store.seed({
      id: SESSION_ID,
      workspaceId: WORKSPACE_ID,
      environmentId: ENV_ID,
      runnerId,
      hostEnvironmentId: ENV_ID,
      distributionState: DISTRIBUTION_PENDING,
      agentHarness: 'claude-sdk',
      agentMode: 'colocated',
      agentHarnessType: 'mock',
      workspace: '/workspace',
    });
  }

  it('flips ASSIGNED when the connected runner advertises the session provider', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = new FakeSessionStore();
    const runnerId = 'runner_token_capable';
    bindSessionToRunner(store, runnerId);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    // The runner's hello advertises 'claude-sdk' — it CAN serve the session.
    connectRunner(tunnelRegistry, runnerId, WORKSPACE_ID, ['claude-sdk', 'codex-cli']);
    await distributor.onRunnerConnect(runnerId);
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_ASSIGNED);
  });

  it('flips FAILED (not ASSIGNED) when the connected runner does NOT advertise the provider', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = new FakeSessionStore();
    const runnerId = 'runner_token_incapable';
    bindSessionToRunner(store, runnerId);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    // The runner advertises a DIFFERENT provider set — it cannot serve 'claude-sdk'.
    connectRunner(tunnelRegistry, runnerId, WORKSPACE_ID, ['codex-cli']);
    await distributor.onRunnerConnect(runnerId);
    // Capability mismatch on the ACTUAL runner: fail the session rather than
    // routing turns to a runner that cannot serve the provider.
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_FAILED);
  });

  it('fails OPEN (ASSIGNED) when the connected runner advertises an EMPTY harness set', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = new FakeSessionStore();
    const runnerId = 'runner_token_silent';
    bindSessionToRunner(store, runnerId);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    // An older runner that advertises nothing: fail open (do not refuse) — the
    // runner's own UnknownProviderError guards if it truly cannot serve it.
    connectRunner(tunnelRegistry, runnerId, WORKSPACE_ID, []);
    await distributor.onRunnerConnect(runnerId);
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_ASSIGNED);
  });
});

describe('SessionDistributor.markRunnerDisconnected — runner tunnel close', () => {
  it('flips the bound session FAILED on a post-connect runner-tunnel close (runner gone)', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    const runnerId = expectedRunnerIdFor(launch.bindingToken);
    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId });
    await outcome.launchSettled;

    // The runner connected (session ASSIGNED), then its tunnel closed for good.
    connectRunner(tunnelRegistry, runnerId);
    await distributor.onRunnerConnect(runnerId);
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_ASSIGNED);

    // The tunnel close deregisters the runner before the hook runs; with the
    // runner truly gone, the disconnect hook flips the bound session FAILED.
    tunnelRegistry.deregister(runnerId);
    await distributor.markRunnerDisconnected(runnerId);
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_FAILED);
    // The transition order is PENDING -> ASSIGNED -> FAILED (no double-FAILED).
    const states = store.patches.map((p) => p.patch.distributionState).filter(Boolean);
    expect(states).toEqual([DISTRIBUTION_PENDING, DISTRIBUTION_ASSIGNED, DISTRIBUTION_FAILED]);
  });

  it('does NOT fail the session when a newer tunnel is live (newest-wins reconnect)', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    const runnerId = expectedRunnerIdFor(launch.bindingToken);
    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId });
    await outcome.launchSettled;

    connectRunner(tunnelRegistry, runnerId);
    await distributor.onRunnerConnect(runnerId);
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_ASSIGNED);

    // The runner reconnected: the route fires the OLD generation's disconnect hook
    // while a NEWER tunnel is still registered for the runner id. The session must
    // stay ASSIGNED — the live tunnel still serves it.
    expect(tunnelRegistry.has(runnerId)).toBe(true);
    await distributor.markRunnerDisconnected(runnerId);
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_ASSIGNED);
    // No FAILED patch was written.
    const states = store.patches.map((p) => p.patch.distributionState).filter(Boolean);
    expect(states).toEqual([DISTRIBUTION_PENDING, DISTRIBUTION_ASSIGNED]);
  });

  it('is a no-op when the disconnecting runner matches no bound session', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    await distributor.markRunnerDisconnected('runner_token_unrelated');
    expect(store.patches).toEqual([]);
  });
});

describe('SessionDistributor.onWorkerConnect — drive pending sessions on (re)connect', () => {
  it('dispatches a launch for a session stranded pending while its worker was offline', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    // 1. Create-time dispatch with NO worker connected: the session is left
    //    PENDING with no runner binding (the create-time-stranded state).
    const stranded = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    expect(stranded.runnerId).toBeNull();
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(store.get(SESSION_ID)!.runnerId).toBeNull();

    // 2. The environment's worker connects. The connect hook drives the launch:
    //    it mints a binding token, pre-registers the runner, persists the
    //    binding, and sends `worker.launch_runner` down the now-live worker tunnel.
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const drained = drainLaunch();
    await distributor.onWorkerConnect(ENV_ID);
    const launch = await drained;

    const runnerId = expectedRunnerIdFor(launch.bindingToken);
    const row = store.get(SESSION_ID)!;
    expect(row.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(row.runnerId).toBe(runnerId);
    expect(row.hostEnvironmentId).toBe(ENV_ID);
    expect(launch.workspace).toBe('/workspace');
    expect(launch.harness).toBe('claude-sdk');
    expect(tunnelRegistry.connectWaiterCount(runnerId)).toBe(1);

    // The driven launch settles like a create-time one; release its waiter.
    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId });

    // The matched runner then connects and the session flips ASSIGNED.
    connectRunner(tunnelRegistry, runnerId);
    await distributor.onRunnerConnect(runnerId);
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_ASSIGNED);
  });

  it('drives every stranded session for the environment, and only those', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = new FakeSessionStore();
    // Two stranded sessions for ENV_ID, one already-bound (has a runner), one for
    // a different environment, and one already FAILED — only the two stranded
    // ENV_ID sessions must be driven.
    const base = {
      workspaceId: WORKSPACE_ID,
      hostEnvironmentId: null,
      agentHarness: 'claude-sdk',
      agentMode: 'colocated',
      agentHarnessType: 'mock',
      workspace: '/workspace',
    } as const;
    store.seed({
      id: 'ses_pending_a',
      environmentId: ENV_ID,
      runnerId: null,
      distributionState: DISTRIBUTION_PENDING,
      ...base,
    });
    store.seed({
      id: 'ses_pending_b',
      environmentId: ENV_ID,
      runnerId: null,
      distributionState: DISTRIBUTION_PENDING,
      ...base,
    });
    store.seed({
      id: 'ses_already_bound',
      environmentId: ENV_ID,
      runnerId: 'runner_token_existing',
      distributionState: DISTRIBUTION_PENDING,
      ...base,
    });
    store.seed({
      id: 'ses_other_env',
      environmentId: 'env_other',
      runnerId: null,
      distributionState: DISTRIBUTION_PENDING,
      ...base,
    });
    store.seed({
      id: 'ses_failed',
      environmentId: ENV_ID,
      runnerId: null,
      distributionState: DISTRIBUTION_FAILED,
      ...base,
    });

    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });
    const { conn } = makeWorkerConn(workerRegistry, ENV_ID);

    // Collect every launch frame the connect drive enqueues.
    const launched: string[] = [];
    const collect = (async () => {
      for (let i = 0; i < 2; i++) {
        const text = await conn.outboundQueue.get();
        if (text === null) break;
        const frame = decodeWorkerFrame(text);
        if (frame.kind === WorkerFrameKind.LaunchRunner) {
          launched.push(expectedRunnerIdFor(frame.bindingToken));
          // Release the bounded launch-result wait so the drive does not linger.
          answerLaunch(conn, frame.requestId, {
            status: 'launched',
            runnerId: expectedRunnerIdFor(frame.bindingToken),
          });
        }
      }
    })();

    await distributor.onWorkerConnect(ENV_ID);
    await collect;

    // Exactly the two stranded ENV_ID sessions got a binding; the others did not.
    expect(store.get('ses_pending_a')!.runnerId).not.toBeNull();
    expect(store.get('ses_pending_b')!.runnerId).not.toBeNull();
    expect(store.get('ses_already_bound')!.runnerId).toBe('runner_token_existing');
    expect(store.get('ses_other_env')!.runnerId).toBeNull();
    expect(store.get('ses_failed')!.runnerId).toBeNull();
    expect(launched.length).toBe(2);
  });

  it('is a no-op when no session is stranded for the environment', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    // The seeded session's distribution_state is null (never dispatched), so it is
    // not in the stranded set; the connect hook does nothing.
    await distributor.onWorkerConnect(ENV_ID);
    expect(store.patches).toEqual([]);
  });
});

// ── 3. Cloud un-gate (target=cloud, once a worker is connected) ──

describe('SessionDistributor.dispatch — cloud environment, connected worker (un-gate)', () => {
  it('mints a token, sends launch_runner, and persists PENDING exactly like self_hosted', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: targetLookup({ [ENV_ID]: 'cloud' }),
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: { id: ENV_ID, target: 'cloud' },
    });
    const launch = await drained;

    const expectedRunnerId = expectedRunnerIdFor(launch.bindingToken);
    expect(outcome.runnerId).toBe(expectedRunnerId);
    const row = store.get(SESSION_ID)!;
    expect(row.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(row.hostEnvironmentId).toBe(ENV_ID);

    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId: expectedRunnerId });
    await outcome.launchSettled;

    connectRunner(tunnelRegistry, expectedRunnerId);
    await distributor.onRunnerConnect(expectedRunnerId);
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_ASSIGNED);
  });

  it('leaves a cloud session PENDING with no binding when no worker is connected yet', async () => {
    // Mirrors the self_hosted "no connected worker" case: a cloud session
    // whose environment-launch lifecycle has not yet started (or its worker
    // has not yet dialed in) stays PENDING with no runner — this is exactly
    // the state the environment-launch lifecycle's own `dispatch` call (after
    // wait-online) will find and drive once the worker comes online.
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry(); // no worker connected
    const store = seededSession(new FakeSessionStore());
    const distributor = new SessionDistributor({
      sessions: store,
      environments: targetLookup({ [ENV_ID]: 'cloud' }),
      tunnelRegistry,
      workerRegistry,
    });

    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: { id: ENV_ID, target: 'cloud' },
    });
    expect(outcome.runnerId).toBeNull();
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(store.get(SESSION_ID)!.hostEnvironmentId).toBeNull();
  });
});

describe("SessionDistributor.onWorkerConnect — threads the environment's REAL target", () => {
  it('consults the environment lookup (by id) and drives the stranded cloud session with the resolved target', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore()); // distributionState: null initially
    const lookup = targetLookup({ [ENV_ID]: 'cloud' });
    const distributor = new SessionDistributor({
      sessions: store,
      environments: lookup,
      tunnelRegistry,
      workerRegistry,
    });

    // Strand the session PENDING with no runner binding against the cloud
    // environment (the state a cloud dispatch with no connected worker yet
    // leaves it in — see the previous describe block).
    await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: { id: ENV_ID, target: 'cloud' },
    });
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(store.get(SESSION_ID)!.runnerId).toBeNull();
    lookup.calls.length = 0; // reset — only the onWorkerConnect call below is under test

    // The worker connects. The two dispatchable targets behave identically
    // once resolved (both pass DISPATCHABLE_TARGETS), so the only way to
    // observe "the hook used the REAL resolved target, not a hardcoded
    // self_hosted" is that it actually CALLED the lookup with this
    // environment id — a hardcoded literal would never touch it. The
    // negative case below (lookup resolves null) is what proves the RESULT
    // is honored, not just requested.
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const drained = drainLaunch();
    await distributor.onWorkerConnect(ENV_ID);
    const launch = await drained;

    expect(lookup.calls).toEqual([ENV_ID]);
    const runnerId = expectedRunnerIdFor(launch.bindingToken);
    expect(store.get(SESSION_ID)!.runnerId).toBe(runnerId);
    expect(store.get(SESSION_ID)!.hostEnvironmentId).toBe(ENV_ID);
    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId });
  });

  it('does NOT drive (and does not throw) when the environment lookup resolves null (vanished)', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore());
    // The environment is unknown to the lookup (deleted/archived between the
    // pending-lookup and here) — the target-lookup map is empty.
    const distributor = new SessionDistributor({
      sessions: store,
      environments: targetLookup({}),
      tunnelRegistry,
      workerRegistry,
    });
    await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: { id: ENV_ID, target: 'cloud' },
    });

    makeWorkerConn(workerRegistry, ENV_ID);
    await expect(distributor.onWorkerConnect(ENV_ID)).resolves.toBeUndefined();
    // Still PENDING/unbound — nothing was driven.
    expect(store.get(SESSION_ID)!.runnerId).toBeNull();
  });
});

// ── 4. Cloud MODE gate (registry-driven parity with harness-server) ──
//
// harness-server's dispatcher (`isRegistryDriven`) treats a `target=cloud`
// session as registry-driven ONLY when its agent's harness mode resolves to
// `colocated` — a `cloud`+`separate` session stays with harness-server. Before
// this gate, `dispatch()`/`onWorkerConnect()` drove ANY session on a
// dispatchable-target environment with a connected worker, gating only on
// `target`, never on mode. A cloud Environment is a durable multi-session
// resource, so a `separate`-mode session can legitimately share its
// `environment_id` with a `colocated` one whose worker is already connected —
// that shared-worker case is exactly what these specs drive, proving the
// registry now agrees with harness-server EXACTLY, even when a worker IS live.

describe('SessionDistributor.dispatch — cloud MODE gate', () => {
  it.each([
    { harness: 'claude_agent_sdk', mode: 'separate' },
    { harness: 'claude_code', mode: 'colocated' },
  ] as const)(
    'does not dispatch cloud $harness/$mode even with a connected worker',
    async ({ harness, mode }) => {
      const tunnelRegistry = new TunnelRegistry();
      const workerRegistry = new WorkerRegistry();
      const store = seededSession(new FakeSessionStore(), {
        agentMode: mode,
        agentHarnessType: harness,
      });
      const { conn } = makeWorkerConn(workerRegistry, ENV_ID); // a worker IS connected
      const distributor = new SessionDistributor({
        sessions: store,
        environments: targetLookup({ [ENV_ID]: 'cloud' }),
        tunnelRegistry,
        workerRegistry,
      });

      const outcome = await distributor.dispatch({
        sessionId: SESSION_ID,
        environment: { id: ENV_ID, target: 'cloud' },
      });

      expect(outcome.state).toBe(DISTRIBUTION_PENDING);
      expect(outcome.runnerId).toBeNull();
      const settlement = await outcome.launchSettled;
      expect(settlement.status).toBe('not_registry_driven');
      const row = store.get(SESSION_ID)!;
      expect(row.distributionState).toBe(DISTRIBUTION_PENDING);
      expect(row.runnerId).toBeNull();
      expect(row.hostEnvironmentId).toBeNull();
      // No launch frame was sent — the worker's outbound queue is empty. A
      // non-blocking drain: put a sentinel and confirm it is the FIRST item out.
      conn.outboundQueue.put(null);
      expect(await conn.outboundQueue.get()).toBeNull();
      // Nothing was pre-registered (no token was minted).
      expect(tunnelRegistry.connectWaiterCount()).toBe(0);
    },
  );

  it('dispatches a cloud+colocated session normally (mints, sends launch_runner)', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore(), { agentMode: 'colocated' });
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: targetLookup({ [ENV_ID]: 'cloud' }),
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: { id: ENV_ID, target: 'cloud' },
    });
    const launch = await drained;

    const expectedRunnerId = expectedRunnerIdFor(launch.bindingToken);
    expect(outcome.runnerId).toBe(expectedRunnerId);
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_PENDING);
    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId: expectedRunnerId });
    await outcome.launchSettled;
  });

  it('self_hosted dispatches regardless of agent mode — separate', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore(), {
      agentMode: 'separate',
      agentHarnessType: 'claude_agent_sdk',
    });
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    const expectedRunnerId = expectedRunnerIdFor(launch.bindingToken);
    expect(outcome.runnerId).toBe(expectedRunnerId);
    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId: expectedRunnerId });
    await outcome.launchSettled;
  });

  it('self_hosted dispatches regardless of agent mode — colocated', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore(), { agentMode: 'colocated' });
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: SELF_HOSTED_LOOKUP,
      tunnelRegistry,
      workerRegistry,
    });

    const drained = drainLaunch();
    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: selfHostedEnv(),
    });
    const launch = await drained;
    const expectedRunnerId = expectedRunnerIdFor(launch.bindingToken);
    expect(outcome.runnerId).toBe(expectedRunnerId);
    answerLaunch(conn, launch.requestId, { status: 'launched', runnerId: expectedRunnerId });
    await outcome.launchSettled;
  });

  it('mode resolution matches resolveAgentMode: an absent annotation resolves to separate, which is NOT registry-driven for cloud', async () => {
    // Mirrors harness-server's resolveCloudSessionHarnessMode default EXACTLY
    // (an unresolvable/absent annotation -> DEFAULT_MODE 'separate') via the
    // SAME shared resolveAgentMode the production DistributionSessionStore
    // calls to populate `agentMode` — proving the value this fake seeds and the
    // real resolver's default can never disagree.
    const absentAnnotationMode = resolveAgentMode(undefined);
    expect(absentAnnotationMode).toBe('separate');

    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = seededSession(new FakeSessionStore(), {
      agentMode: absentAnnotationMode,
      agentHarnessType: 'claude_agent_sdk',
    });
    makeWorkerConn(workerRegistry, ENV_ID);
    const distributor = new SessionDistributor({
      sessions: store,
      environments: targetLookup({ [ENV_ID]: 'cloud' }),
      tunnelRegistry,
      workerRegistry,
    });

    const outcome = await distributor.dispatch({
      sessionId: SESSION_ID,
      environment: { id: ENV_ID, target: 'cloud' },
    });
    expect(outcome.runnerId).toBeNull();
    expect(store.get(SESSION_ID)!.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(store.get(SESSION_ID)!.runnerId).toBeNull();
  });
});

describe('SessionDistributor.onWorkerConnect — cloud MODE gate', () => {
  it('dispatches only the colocated pending session on connect, skipping the separate one (stays PENDING)', async () => {
    const tunnelRegistry = new TunnelRegistry();
    const workerRegistry = new WorkerRegistry();
    const store = new FakeSessionStore();
    const base = {
      workspaceId: WORKSPACE_ID,
      environmentId: ENV_ID,
      runnerId: null,
      hostEnvironmentId: null,
      distributionState: DISTRIBUTION_PENDING,
      agentHarness: 'claude-sdk',
      workspace: '/workspace',
    } as const;
    store.seed({ id: 'ses_colocated', agentMode: 'colocated', agentHarnessType: 'mock', ...base });
    store.seed({
      id: 'ses_separate',
      agentMode: 'separate',
      agentHarnessType: 'claude_agent_sdk',
      ...base,
    });

    const distributor = new SessionDistributor({
      sessions: store,
      environments: targetLookup({ [ENV_ID]: 'cloud' }),
      tunnelRegistry,
      workerRegistry,
    });
    const { conn, drainLaunch } = makeWorkerConn(workerRegistry, ENV_ID);

    const drained = drainLaunch();
    await distributor.onWorkerConnect(ENV_ID);
    const launch = await drained;

    // Only the colocated session got a binding + launch frame.
    expect(store.get('ses_colocated')!.runnerId).not.toBeNull();
    expect(store.get('ses_colocated')!.distributionState).toBe(DISTRIBUTION_PENDING);
    expect(store.get('ses_separate')!.runnerId).toBeNull();
    expect(store.get('ses_separate')!.hostEnvironmentId).toBeNull();
    expect(store.get('ses_separate')!.distributionState).toBe(DISTRIBUTION_PENDING);
    answerLaunch(conn, launch.requestId, {
      status: 'launched',
      runnerId: expectedRunnerIdFor(launch.bindingToken),
    });

    // Exactly ONE launch frame total: drain a sentinel and confirm it is the
    // NEXT (and only next) item out — no second frame was queued for
    // ses_separate.
    conn.outboundQueue.put(null);
    expect(await conn.outboundQueue.get()).toBeNull();
  });
});
