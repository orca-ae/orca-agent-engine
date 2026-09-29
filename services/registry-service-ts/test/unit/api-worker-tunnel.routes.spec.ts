// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// In-process spec for the `src/api/worker-tunnel.routes.ts` WebSocket endpoint.
//
// Mirrors the runner-tunnel route spec: every test spins up an in-process
// Fastify app on an ephemeral loopback port and connects a REAL `ws` client — no
// DB, no dev-stack, no in-memory duplex. The close/disconnect handshake and the
// pre-accept rejection paths therefore behave like production.
//
// The worker tunnel is the endpoint a self-hosted environment worker dials from
// OUTSIDE the mesh: it presents its Environment ID (in the path) + Env Key (in
// the `X-Orca-Environment-Key` header). The registry authenticates the key
// against the environment row's stored digest+expiry, registers the worker
// connection on this replica, CLAIMS the environment in the durable claim store
// (newest-wins, this pod + connection id), and holds the control channel.
//
// The environment-row loader and the claim store are injected through the route
// options as in-memory fakes — the claim semantics (newest-wins, connection-
// scoped heartbeat/release) are covered against the real `EnvironmentClaimStore`
// in the DB-backed integration suite; here we assert the route DRIVES them
// (claim on connect, release on disconnect, heartbeat on ping) and authenticates
// the Env Key, with no infrastructure.

import { describe, it, expect, afterEach } from 'vitest';
import Fastify, { type FastifyInstance } from 'fastify';
import websocket from '@fastify/websocket';
import { WebSocket } from 'ws';
import {
  WorkerFrameKind,
  INTERNAL_WS_ORIGIN,
  HOST_TUNNEL_TOKEN_HEADER,
  encodeWorkerFrame,
  FrameKind,
  encodeFrame,
  TUNNEL_MAX_MESSAGE_BYTES,
  type WorkerHelloFrame,
  type WorkerLaunchResult,
} from '@orca/harness-tunnel';
import { hashEnvKey } from '../../src/domain/environment-key.js';
import { hashEnvironmentToken } from '../../src/domain/environment-token.js';
import { verifyEnvironmentTokenForEnvironment } from '../../src/domain/environment-token-state.js';
import { WorkerRegistry } from '../../src/tunnel/worker-registry.js';
import { RunnerExitReports } from '../../src/tunnel/runner-exit-reports.js';
import {
  registerWorkerTunnelRoutes,
  WORKER_FORBIDDEN_ORIGIN_CLOSE_CODE,
  WORKER_UNAUTHENTICATED_CLOSE_CODE,
  WORKER_EXPECTED_HELLO_CLOSE_CODE,
  WORKER_VERSION_MISMATCH_CLOSE_CODE,
  WORKER_PING_TIMEOUT_CLOSE_CODE,
  ENVIRONMENT_TOKEN_HEADER,
  type WorkerTunnelRouteOptions,
  type EnvironmentKeyRow,
  type EnvironmentKeyLoader,
  type WorkerClaimStore,
  type EnvironmentTokenResolver,
} from '../../src/api/worker-tunnel.routes.js';

const ENV_ID = 'env_test_001';
const ENV_KEY = 'sk-test-env-key-aaaaaaaaaaaaaaaaaaaaaaaaaaa';
const ENV_KEY_HEADER = 'x-orca-environment-key';
const WORKSPACE_ID = 'ws_acme';
const ENVIRONMENT_TOKEN = 'et-test-environment-token-aaaaaaaaaaaaaaaaaaaaaaaaaaa';
const ENVIRONMENT_TOKEN_WORKSPACE = 'ws_sandbox_managed';

const WORKER_TUNNEL_PATH = (envId: string): string => `/v1/tunnels/environments/${envId}`;

function helloFrame(name = 'workstation-01', runners: string[] = []): WorkerHelloFrame {
  return {
    kind: WorkerFrameKind.Hello,
    version: '0.1.0-test',
    frameProtocolVersion: 1,
    name,
    runners,
    configuredHarnesses: null,
  };
}

// ── In-memory fakes injected through the route options ──────

/** A fake environment-row loader over a fixed map of `env_id` → row. */
class FakeEnvironmentStore implements EnvironmentKeyLoader {
  private readonly rows = new Map<string, EnvironmentKeyRow>();

  put(envId: string, row: EnvironmentKeyRow): this {
    this.rows.set(envId, row);
    return this;
  }

  loadEnvironmentKeyRow(envId: string): Promise<EnvironmentKeyRow | null> {
    return Promise.resolve(this.rows.get(envId) ?? null);
  }
}

/**
 * A fake environment loader whose `loadEnvironmentKeyRow` resolves only after a
 * delay — modelling the REAL route's async Env-Key auth (an argon2 verify + a DB
 * read), which opens a window between the socket accepting and the engine first
 * reading a frame. A worker that sends `worker.hello` immediately on open lands its
 * frame INSIDE that window, so this fake reproduces that timing in-process.
 */
class SlowEnvironmentStore implements EnvironmentKeyLoader {
  constructor(
    private readonly inner: FakeEnvironmentStore,
    private readonly delayMs: number,
  ) {}

  async loadEnvironmentKeyRow(envId: string): Promise<EnvironmentKeyRow | null> {
    await new Promise((r) => setTimeout(r, this.delayMs));
    return this.inner.loadEnvironmentKeyRow(envId);
  }
}

/** A claim record captured by the fake claim store. */
interface CapturedClaim {
  environmentId: string;
  ownerPod: string;
  workerConnId: string;
}

/**
 * An in-memory claim store that records every claim/heartbeat/release call and
 * enforces the same newest-wins + connection-scoped semantics the real
 * `EnvironmentClaimStore` does, so the route's wiring is asserted faithfully.
 */
class FakeClaimStore implements WorkerClaimStore {
  readonly claims: CapturedClaim[] = [];
  readonly heartbeats: { environmentId: string; workerConnId: string }[] = [];
  readonly releases: { environmentId: string; workerConnId: string; released: boolean }[] = [];
  private current = new Map<string, string>(); // envId → owning workerConnId

  claim(environmentId: string, ownerPod: string, workerConnId: string): Promise<void> {
    this.claims.push({ environmentId, ownerPod, workerConnId });
    // Newest-wins: the latest claim owns the environment.
    this.current.set(environmentId, workerConnId);
    return Promise.resolve();
  }

  heartbeat(environmentId: string, workerConnId: string): Promise<boolean> {
    const refreshed = this.current.get(environmentId) === workerConnId;
    this.heartbeats.push({ environmentId, workerConnId });
    return Promise.resolve(refreshed);
  }

  release(environmentId: string, workerConnId: string): Promise<boolean> {
    // Connection-scoped: only the current owner's release drops the claim.
    const released = this.current.get(environmentId) === workerConnId;
    if (released) {
      this.current.delete(environmentId);
    }
    this.releases.push({ environmentId, workerConnId, released });
    return Promise.resolve(released);
  }

  owner(environmentId: string): string | undefined {
    return this.current.get(environmentId);
  }
}

function liveEnvRow(overrides: Partial<EnvironmentKeyRow> = {}): EnvironmentKeyRow {
  return {
    workspaceId: WORKSPACE_ID,
    envKeyDigest: hashEnvKey(ENV_KEY),
    envKeyExpiresAt: new Date(Date.now() + 60_000),
    archived: false,
    ...overrides,
  };
}

/** A fake environment-token row, mirroring `EnvironmentKeyRow`'s shape. */
interface FakeEnvironmentTokenRow {
  workspaceId: string;
  environmentTokenDigest: string | null;
  environmentTokenExpiresAt: Date | null;
  archived: boolean;
}

/**
 * A fake per-environment token resolver over a fixed map of `env_id` → row.
 *
 * Delegates the actual digest+expiry+archive check to the real
 * `verifyEnvironmentTokenForEnvironment` (unit-tested on its own in
 * `environment-token-state.spec.ts`) so this fake proves the ROUTE's wiring
 * (present-must-resolve-or-refuse, never-falls-through, id-scoped), not a
 * re-implementation of the crypto.
 */
class FakeEnvironmentTokenStore implements EnvironmentTokenResolver {
  private readonly rows = new Map<string, FakeEnvironmentTokenRow>();

  put(envId: string, row: FakeEnvironmentTokenRow): this {
    this.rows.set(envId, row);
    return this;
  }

  resolveEnvironmentToken(envId: string, token: string): Promise<string | null> {
    const row = this.rows.get(envId);
    if (row === undefined) {
      return Promise.resolve(null);
    }
    const valid = verifyEnvironmentTokenForEnvironment(
      token,
      {
        environmentTokenDigest: row.environmentTokenDigest,
        environmentTokenExpiresAt: row.environmentTokenExpiresAt,
      },
      row.archived,
    );
    return Promise.resolve(valid ? row.workspaceId : null);
  }
}

function liveTokenRow(overrides: Partial<FakeEnvironmentTokenRow> = {}): FakeEnvironmentTokenRow {
  return {
    workspaceId: ENVIRONMENT_TOKEN_WORKSPACE,
    environmentTokenDigest: hashEnvironmentToken(ENVIRONMENT_TOKEN),
    environmentTokenExpiresAt: new Date(Date.now() + 60_000),
    archived: false,
    ...overrides,
  };
}

// Track every app + socket so a failed assertion can't leak a listener.
const openApps: FastifyInstance[] = [];
const openSockets: WebSocket[] = [];

afterEach(async () => {
  for (const ws of openSockets.splice(0)) {
    try {
      ws.terminate();
    } catch {
      // ignore
    }
  }
  for (const app of openApps.splice(0)) {
    await app.close();
  }
});

interface BuildOpts extends Omit<
  WorkerTunnelRouteOptions,
  'registry' | 'environments' | 'claims' | 'tokens'
> {
  registry?: WorkerRegistry;
  environments?: EnvironmentKeyLoader;
  claims?: WorkerClaimStore;
  tokens?: EnvironmentTokenResolver;
  /** Worker the route should treat as the peer (drives the loopback branches). */
  peerHost?: string;
}

interface BuiltApp {
  app: FastifyInstance;
  baseUrl: string;
  registry: WorkerRegistry;
  environments: FakeEnvironmentStore;
  claims: FakeClaimStore;
  tokens: FakeEnvironmentTokenStore;
}

async function buildHostTunnelApp(opts: BuildOpts = {}): Promise<BuiltApp> {
  const {
    peerHost,
    registry: regOpt,
    environments: envOpt,
    claims: claimOpt,
    tokens: tokensOpt,
    ...routeOpts
  } = opts;
  const registry = regOpt ?? new WorkerRegistry();
  const environments =
    (envOpt as FakeEnvironmentStore | undefined) ??
    new FakeEnvironmentStore().put(ENV_ID, liveEnvRow());
  const claims = (claimOpt as FakeClaimStore | undefined) ?? new FakeClaimStore();
  // Default: no environment token armed anywhere, so a test that never
  // presents the token header is byte-for-byte the pre-existing Env-Key-only
  // behavior.
  const tokens =
    (tokensOpt as FakeEnvironmentTokenStore | undefined) ?? new FakeEnvironmentTokenStore();

  const app = Fastify({ logger: false });
  openApps.push(app);
  await app.register(websocket, { options: { maxPayload: TUNNEL_MAX_MESSAGE_BYTES } });
  registerWorkerTunnelRoutes(app, {
    registry,
    environments,
    claims,
    tokens,
    ...routeOpts,
    ...(peerHost !== undefined ? { resolvePeerHost: () => peerHost } : {}),
  });
  await app.listen({ host: '127.0.0.1', port: 0 });
  const addr = app.server.address();
  if (addr === null || typeof addr === 'string') {
    throw new Error('expected a TCP address');
  }
  return { app, baseUrl: `ws://127.0.0.1:${addr.port}`, registry, environments, claims, tokens };
}

/**
 * Open a real `ws` client and resolve once it is connected OR closed.
 *
 * A pre-hello rejection closes the socket before `open` ever fires, so both
 * `open` and `close` resolve the promise — the caller then inspects the close
 * code via {@link nextClose} (attached before the first frame is sent).
 */
async function connect(
  baseUrl: string,
  envId: string,
  opts: { headers?: Record<string, string>; origin?: string | null; envKey?: string | null } = {},
): Promise<WebSocket> {
  const headers: Record<string, string> = { ...(opts.headers ?? {}) };
  // Default: present a valid Env Key in the dedicated header. Tests probing the
  // auth gate pass `envKey: <other>` or `envKey: null` to omit it.
  if (opts.envKey === undefined) {
    headers[ENV_KEY_HEADER] = ENV_KEY;
  } else if (opts.envKey !== null) {
    headers[ENV_KEY_HEADER] = opts.envKey;
  }
  // Default first-party clients announce the internal origin so the CSWSH guard
  // lets them through; tests that probe the guard pass `origin: <x>`/`null`.
  if (opts.origin === undefined) {
    headers.origin = INTERNAL_WS_ORIGIN;
  } else if (opts.origin !== null) {
    headers.origin = opts.origin;
  }
  const ws = new WebSocket(`${baseUrl}${WORKER_TUNNEL_PATH(envId)}`, { headers });
  openSockets.push(ws);
  ws.on('error', () => {
    // swallow; close-code assertions read the 'close' event
  });
  await new Promise<void>((resolve) => {
    ws.once('open', () => resolve());
    ws.once('error', () => {
      // A failed handshake surfaces as an error on some Node versions; the close
      // handler still resolves with the code.
    });
    ws.once('close', () => resolve());
  });
  return ws;
}

/** Resolve with the close code once the client socket closes. */
function nextClose(ws: WebSocket): Promise<number> {
  return new Promise<number>((resolve) => {
    ws.once('close', (code: number) => resolve(code));
  });
}

type ProbeOutcome = { upgraded: true } | { upgraded: false; status: number | undefined };

async function probePath(baseUrl: string, path: string): Promise<ProbeOutcome> {
  const ws = new WebSocket(`${baseUrl}${path}`);
  openSockets.push(ws);
  ws.on('error', () => {
    // 404 upgrade refusal surfaces as 'unexpected-response' → 'error' → 'close'.
  });
  return await new Promise<ProbeOutcome>((resolve) => {
    let outcome: ProbeOutcome | undefined;
    ws.once('open', () => {
      outcome = { upgraded: true };
      ws.close();
    });
    ws.once('unexpected-response', (_req, res: { statusCode?: number }) => {
      outcome = { upgraded: false, status: res.statusCode };
      ws.close();
    });
    ws.once('close', () => resolve(outcome ?? { upgraded: false, status: undefined }));
  });
}

function sendHost(ws: WebSocket, frame: Parameters<typeof encodeWorkerFrame>[0]): void {
  ws.send(encodeWorkerFrame(frame));
}

/** Poll until `predicate` holds or `timeoutMs` elapses. */
async function waitFor(predicate: () => boolean, timeoutMs = 1000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error('waitFor timed out');
    }
    await new Promise((r) => setTimeout(r, 5));
  }
}

/** Send hello and wait until the worker registers. */
async function sendHelloAndRegister(
  ws: WebSocket,
  registry: WorkerRegistry,
  envId: string,
  hello = helloFrame(),
): Promise<void> {
  sendHost(ws, hello);
  await waitFor(() => registry.get(envId) !== undefined);
}

// ── Accept + register + claim ───────────────────────────────

describe('api/worker-tunnel route — accept + register + claim', () => {
  it('accepts a valid Env Key, registers the worker, and claims the environment', async () => {
    const { baseUrl, registry, claims } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID, helloFrame('laptop', ['runner_x']));

    const conn = registry.get(ENV_ID);
    expect(conn).toBeDefined();
    expect(conn!.hello.name).toBe('laptop');
    // Owner recorded as the environment's workspace (the tenant scope).
    expect(conn!.owner).toBe(WORKSPACE_ID);
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);

    // The environment was claimed exactly once on connect, this pod + a conn id.
    await waitFor(() => claims.claims.length === 1);
    expect(claims.claims[0]!.environmentId).toBe(ENV_ID);
    expect(claims.claims[0]!.workerConnId.length).toBeGreaterThan(0);
    expect(claims.owner(ENV_ID)).toBe(claims.claims[0]!.workerConnId);
  });

  it('buffers a hello that arrives DURING the async Env-Key auth window (no dropped frame)', async () => {
    // Regression: the worker sends `worker.hello` the instant the socket opens, but
    // the route authenticates the Env Key asynchronously (argon2 + a DB read)
    // BEFORE it attaches the inbound pump. `ws` drops a message that lands with no
    // `message` listener, so a hello arriving inside that auth window was silently
    // lost — the engine then blocked forever on its first `receive()` and never
    // claimed the environment (the worker looked "connected" but no claim existed).
    // The fix attaches the buffering adapter BEFORE the await; this test forces the
    // race with a slow loader + a hello sent synchronously on `open`, and asserts
    // the worker still registers and claims.
    const env = new FakeEnvironmentStore().put(ENV_ID, liveEnvRow());
    const slow = new SlowEnvironmentStore(env, 50);
    const { baseUrl, registry, claims } = await buildHostTunnelApp({ environments: slow });

    const ws = new WebSocket(`${baseUrl}${WORKER_TUNNEL_PATH(ENV_ID)}`, {
      headers: { [ENV_KEY_HEADER]: ENV_KEY, origin: INTERNAL_WS_ORIGIN },
    });
    openSockets.push(ws);
    ws.on('error', () => {
      /* close-code/registration assertions read other events */
    });
    // Send the hello the moment the socket opens — i.e. while the 50ms auth is
    // still in flight, the exact window the dropped-frame bug lived in.
    ws.on('open', () => ws.send(encodeWorkerFrame(helloFrame('race-laptop'))));

    await waitFor(() => registry.get(ENV_ID) !== undefined, 2000);
    expect(registry.get(ENV_ID)!.hello.name).toBe('race-laptop');
    await waitFor(() => claims.claims.length === 1, 2000);
    expect(claims.owner(ENV_ID)).toBe(claims.claims[0]!.workerConnId);
  });

  it('claims under the configured owner-pod id', async () => {
    const { baseUrl, registry, claims } = await buildHostTunnelApp({ ownerPod: 'registry-pod-7' });
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);
    await waitFor(() => claims.claims.length === 1);
    expect(claims.claims[0]!.ownerPod).toBe('registry-pod-7');
  });

  it('fires the connect hook after registration', async () => {
    const connects: string[] = [];
    const { baseUrl, registry } = await buildHostTunnelApp({
      onWorkerConnect: async (id) => {
        connects.push(id);
      },
    });
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);
    await waitFor(() => connects.includes(ENV_ID));
    expect(connects).toEqual([ENV_ID]);
  });
});

// ── Disconnect cleanup: deregister + release ────────────────

describe('api/worker-tunnel route — disconnect cleanup', () => {
  it('deregisters the worker and releases the claim when the socket closes', async () => {
    const { baseUrl, registry, claims } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);
    await waitFor(() => claims.claims.length === 1);
    const connId = claims.claims[0]!.workerConnId;

    ws.close();
    await waitFor(() => registry.get(ENV_ID) === undefined);
    // The claim is released, connection-scoped (same conn id it claimed with).
    await waitFor(() => claims.releases.length === 1);
    expect(claims.releases[0]).toEqual({
      environmentId: ENV_ID,
      workerConnId: connId,
      released: true,
    });
    expect(claims.owner(ENV_ID)).toBeUndefined();
  });

  it('fires the disconnect hook exactly once on a clean close', async () => {
    const disconnects: string[] = [];
    const { baseUrl, registry } = await buildHostTunnelApp({
      onWorkerDisconnect: async (id) => {
        disconnects.push(id);
      },
    });
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);
    ws.close();
    await waitFor(() => registry.get(ENV_ID) === undefined);
    await waitFor(() => disconnects.length > 0);
    // Settle any (incorrect) second fire before asserting.
    await new Promise((r) => setTimeout(r, 50));
    expect(disconnects).toEqual([ENV_ID]);
  });

  it('fires the disconnect hook exactly once when the socket is abruptly terminated', async () => {
    const disconnects: string[] = [];
    const { baseUrl, registry } = await buildHostTunnelApp({
      onWorkerDisconnect: async (id) => {
        disconnects.push(id);
      },
    });
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);
    ws.terminate();
    await waitFor(() => registry.get(ENV_ID) === undefined);
    await waitFor(() => disconnects.length > 0);
    await new Promise((r) => setTimeout(r, 50));
    expect(disconnects).toEqual([ENV_ID]);
  });

  it('never releases a claim it never took when the claim itself fails (register guard)', async () => {
    // The "do not touch a worker we never registered" guard: the claim runs
    // BEFORE the connection is registered, so a claim that throws must leave the
    // worker unregistered AND must NOT issue a release for a claim that was never
    // taken (the guard against deregistering/offlining a worker on a pre-register
    // failure — here, a claim error rather than a duplicate-owner reject, since
    // claims are newest-wins).
    const disconnects: string[] = [];
    class ThrowingClaimStore extends FakeClaimStore {
      override claim(): Promise<void> {
        return Promise.reject(new Error('claim store unavailable'));
      }
    }
    const claims = new ThrowingClaimStore();
    const { baseUrl, registry } = await buildHostTunnelApp({
      claims,
      onWorkerDisconnect: async (id) => {
        disconnects.push(id);
      },
    });
    const ws = await connect(baseUrl, ENV_ID);
    const closed = nextClose(ws);
    // Hello is sent, the engine attempts the claim, it throws → the engine's
    // pre-register catch closes the socket without registering or releasing.
    sendHost(ws, helloFrame());
    await closed;
    await new Promise((r) => setTimeout(r, 50));
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(registry.onlineWorkerIds()).toEqual([]);
    // No release was attempted for a claim that was never taken, and the
    // disconnect hook never fired (we never registered this connection).
    expect(claims.releases).toEqual([]);
    expect(disconnects).toEqual([]);
  });
});

// ── Env Key auth — refused before accept (4004) ─────────────

describe('api/worker-tunnel route — Env Key auth', () => {
  it('refuses a missing Env Key before accept (4004) and never claims', async () => {
    const { baseUrl, registry, claims } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID, { envKey: null });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(claims.claims).toEqual([]);
  });

  it('refuses a wrong Env Key before accept (4004)', async () => {
    const { baseUrl, registry, claims } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID, { envKey: 'sk-not-the-real-key' });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(claims.claims).toEqual([]);
  });

  it('refuses an expired Env Key before accept (4004)', async () => {
    const environments = new FakeEnvironmentStore().put(
      ENV_ID,
      liveEnvRow({ envKeyExpiresAt: new Date(Date.now() - 1) }),
    );
    const { baseUrl, registry, claims } = await buildHostTunnelApp({ environments });
    const ws = await connect(baseUrl, ENV_ID);
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(claims.claims).toEqual([]);
  });

  it('refuses an archived environment before accept (4004)', async () => {
    const environments = new FakeEnvironmentStore().put(ENV_ID, liveEnvRow({ archived: true }));
    const { baseUrl, registry } = await buildHostTunnelApp({ environments });
    const ws = await connect(baseUrl, ENV_ID);
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
  });

  it('refuses an unknown environment id before accept (4004) without leaking existence', async () => {
    // Empty store: no row for the id. The verify path fails closed, identical to
    // a wrong key, so the 404-vs-401 distinction never leaks which ids exist.
    const environments = new FakeEnvironmentStore();
    const { baseUrl, registry } = await buildHostTunnelApp({ environments });
    const ws = await connect(baseUrl, ENV_ID);
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
  });

  it('refuses a key armed for a DIFFERENT environment (capability scoping)', async () => {
    // The presented key authenticates only against the path env's row. A key
    // valid for env A must not open env B's tunnel.
    const otherKey = 'sk-other-env-key-bbbbbbbbbbbbbbbbbbbbbbbbbbb';
    const environments = new FakeEnvironmentStore()
      .put(ENV_ID, liveEnvRow()) // armed for ENV_KEY
      .put('env_other', liveEnvRow({ envKeyDigest: hashEnvKey(otherKey) }));
    const { baseUrl, registry } = await buildHostTunnelApp({ environments });
    // Present env_other's key on ENV_ID's path → fails closed.
    const ws = await connect(baseUrl, ENV_ID, { envKey: otherKey });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
  });
});

// ── Launch-token path is inert + fail-closed on this route ────────────
//
// The reusable engine also supports a sandbox-worker launch-token credential
// (`X-Orca-Host-Token` -> resolve to worker id + owner). This registry route makes
// that path deliberately INERT and FAIL-CLOSED: the Env Key is the only
// credential, verified in the route before the engine runs, and the route's
// worker-store seam (`ClaimBackedWorkerStore.resolveLaunchToken`) always returns
// `null`. The engine treats a present-but-unresolvable launch token as an auth
// refusal (it never falls through to the route's resolved-owner provider), so a
// peer that presents this header is closed with 4004 even when it ALSO presents a
// valid Env Key — the inert token path cannot be downgraded into, and a stray
// token header cannot ride a good Env Key into an authenticated tunnel. The
// engine's token resolve / mismatch / unknown-fail-closed matrix is covered at
// the engine layer (`packages/harness-tunnel`); these two cases pin the route's
// own auth-model choice so the inert path is exercised here too.

describe('api/worker-tunnel route — launch-token path is inert + fail-closed', () => {
  it('refuses a launch token with no Env Key before accept (4004), never claims', async () => {
    // A peer that presents only the engine's launch token — and NO Env Key —
    // is refused exactly like any unauthenticated peer: the route's token resolver
    // returns null, the engine refuses, and nothing registers or claims.
    const { baseUrl, registry, claims } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID, {
      envKey: null,
      headers: { [HOST_TUNNEL_TOKEN_HEADER]: 'tok-launch-should-be-ignored' },
    });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(claims.claims).toEqual([]);
  });

  it('refuses a launch token even alongside a VALID Env Key (4004), never claims', async () => {
    // The Env Key here is valid (the default). A launch-token header is still
    // refused: the engine resolves the inert token to null and fails closed BEFORE
    // it would ever consult the route's resolved-owner provider, so the good Env
    // Key does not rescue a present-but-unresolvable token. The worker never
    // registers and the environment is never claimed.
    const { baseUrl, registry, claims } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID, {
      headers: { [HOST_TUNNEL_TOKEN_HEADER]: 'tok-launch-should-be-ignored' },
    });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(claims.claims).toEqual([]);
  });
});

// ── Environment Token auth — the managed-launch fork ────────
//
// A per-launch Environment Token (`X-Orca-Environment-Token`) is the
// managed-auth alternative to the Env Key: a registry-launched worker (a
// server-managed sandbox with no operator to provision an Env Key) presents
// this instead. When the header is present it MUST resolve or the connection
// is refused — it never falls through to the Env-Key path, exactly the same
// posture the Env Key itself enforces relative to the (separate, inert)
// engine-level launch-token header above. When the header is absent, the
// pre-existing Env-Key path is completely unaffected — proven here by arming
// a token for the SAME environment id and confirming it changes nothing
// unless the header is actually presented.

describe('api/worker-tunnel route — Environment Token auth (managed)', () => {
  it('accepts a valid Environment Token, attributes the resolved owner, with no Env Key presented', async () => {
    const tokens = new FakeEnvironmentTokenStore().put(ENV_ID, liveTokenRow());
    const { baseUrl, registry, claims } = await buildHostTunnelApp({ tokens });
    const ws = await connect(baseUrl, ENV_ID, {
      envKey: null,
      headers: { [ENVIRONMENT_TOKEN_HEADER]: ENVIRONMENT_TOKEN },
    });
    await sendHelloAndRegister(ws, registry, ENV_ID);

    const conn = registry.get(ENV_ID);
    expect(conn).toBeDefined();
    // Owner resolved via the token path's workspace — distinct from the Env
    // Key fixture's WORKSPACE_ID, so this proves which path authenticated.
    expect(conn!.owner).toBe(ENVIRONMENT_TOKEN_WORKSPACE);
    await waitFor(() => claims.claims.length === 1);
    expect(claims.claims[0]!.environmentId).toBe(ENV_ID);
  });

  it('refuses a wrong Environment Token (4004) even alongside a VALID Env Key — no fallthrough', async () => {
    const tokens = new FakeEnvironmentTokenStore().put(ENV_ID, liveTokenRow());
    const { baseUrl, registry, claims } = await buildHostTunnelApp({ tokens });
    // The default `connect()` also presents a VALID Env Key here; a bad token
    // must still refuse rather than fall back to it.
    const ws = await connect(baseUrl, ENV_ID, {
      headers: { [ENVIRONMENT_TOKEN_HEADER]: 'et-not-the-real-token' },
    });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(claims.claims).toEqual([]);
  });

  it('refuses an expired Environment Token (4004) even alongside a VALID Env Key — no fallthrough', async () => {
    const tokens = new FakeEnvironmentTokenStore().put(
      ENV_ID,
      liveTokenRow({ environmentTokenExpiresAt: new Date(Date.now() - 1) }),
    );
    const { baseUrl, registry, claims } = await buildHostTunnelApp({ tokens });
    const ws = await connect(baseUrl, ENV_ID, {
      headers: { [ENVIRONMENT_TOKEN_HEADER]: ENVIRONMENT_TOKEN },
    });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(claims.claims).toEqual([]);
  });

  it('refuses a token armed for a DIFFERENT environment (id-scoped) — no fallthrough to Env Key', async () => {
    const tokens = new FakeEnvironmentTokenStore().put('env_other', liveTokenRow());
    const { baseUrl, registry, claims } = await buildHostTunnelApp({ tokens });
    // ENV_ID itself has no token armed; presenting env_other's token on ENV_ID's
    // path must fail closed, and a valid Env Key (the connect() default) must
    // not rescue it.
    const ws = await connect(baseUrl, ENV_ID, {
      headers: { [ENVIRONMENT_TOKEN_HEADER]: ENVIRONMENT_TOKEN },
    });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
    expect(claims.claims).toEqual([]);
  });

  it('refuses an unknown environment id for the token path without leaking existence', async () => {
    const tokens = new FakeEnvironmentTokenStore(); // no rows at all
    const { baseUrl, registry } = await buildHostTunnelApp({ tokens });
    const ws = await connect(baseUrl, ENV_ID, {
      envKey: null,
      headers: { [ENVIRONMENT_TOKEN_HEADER]: ENVIRONMENT_TOKEN },
    });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_UNAUTHENTICATED_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
  });

  it('absent token header: the existing Env Key path is unaffected, even when a token IS armed for this environment', async () => {
    const tokens = new FakeEnvironmentTokenStore().put(ENV_ID, liveTokenRow());
    const { baseUrl, registry, claims } = await buildHostTunnelApp({ tokens });
    // No token header at all — connect()'s default presents only the Env Key.
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);
    // Resolved via the Env Key, not the (armed but unpresented) token.
    expect(registry.get(ENV_ID)!.owner).toBe(WORKSPACE_ID);
    await waitFor(() => claims.claims.length === 1);
  });
});

// ── CSWSH origin guard (mirrors the runner tunnel) ──────────

describe('api/worker-tunnel route — CSWSH origin guard', () => {
  it('rejects a browser cross-origin handshake (4403) before accept', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp({
      localMode: true,
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, ENV_ID, { origin: 'https://evil.example.com' });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_FORBIDDEN_ORIGIN_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
  });

  it('rejects a forbidden origin BEFORE checking the Env Key (origin guard is first)', async () => {
    // A forbidden origin closes with 4403 even when the key is also bad — the
    // CSWSH guard runs before any credential I/O, exactly like the runner tunnel.
    const { baseUrl } = await buildHostTunnelApp({ localMode: true, peerHost: '127.0.0.1' });
    const ws = await connect(baseUrl, ENV_ID, {
      origin: 'https://evil.example.com',
      envKey: 'sk-bad',
    });
    expect(await nextClose(ws)).toBe(WORKER_FORBIDDEN_ORIGIN_CLOSE_CODE);
  });

  it('allows the internal sentinel origin', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp({
      localMode: true,
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, ENV_ID, { origin: INTERNAL_WS_ORIGIN });
    await sendHelloAndRegister(ws, registry, ENV_ID);
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);
  });

  it('allows a loopback browser origin in local mode', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp({
      localMode: true,
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, ENV_ID, { origin: 'http://localhost:8000' });
    await sendHelloAndRegister(ws, registry, ENV_ID);
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);
  });

  it('allows a missing Origin (non-browser client)', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp({
      localMode: true,
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, ENV_ID, { origin: null });
    await sendHelloAndRegister(ws, registry, ENV_ID);
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);
  });

  it('denies an unlisted origin when an extra-allowed list is configured (non-local)', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp({
      allowedOrigins: new Set(['https://app.example.com']),
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, ENV_ID, { origin: 'https://other.example.com' });
    const code = await nextClose(ws);
    expect(code).toBe(WORKER_FORBIDDEN_ORIGIN_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
  });

  it('honors an explicit extra-allowed origin in non-local mode', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp({
      allowedOrigins: new Set(['https://app.example.com']),
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, ENV_ID, { origin: 'https://app.example.com' });
    await sendHelloAndRegister(ws, registry, ENV_ID);
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);
  });
});

// ── Hello handshake / version skew ──────────────────────────

describe('api/worker-tunnel route — hello handshake', () => {
  it('closes with 4002 on a frame-protocol-major mismatch', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID);
    const closed = nextClose(ws);
    sendHost(ws, { ...helloFrame(), frameProtocolVersion: 99 });
    expect(await closed).toBe(WORKER_VERSION_MISMATCH_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
  });

  it('closes with 4001 when the first frame is not a worker.hello', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID);
    const closed = nextClose(ws);
    // A launch-runner-result frame is a valid worker frame but not a hello.
    sendHost(ws, {
      kind: WorkerFrameKind.LaunchRunnerResult,
      requestId: 'req_1',
      status: 'launched',
      runnerId: 'runner_x',
    });
    expect(await closed).toBe(WORKER_EXPECTED_HELLO_CLOSE_CODE);
    expect(registry.get(ENV_ID)).toBeUndefined();
  });
});

// ── Control-frame routing (launch result → pending future) ──

describe('api/worker-tunnel route — control-frame routing', () => {
  it('routes a worker.launch_runner_result to the pending launch waiter', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);

    const conn = registry.get(ENV_ID)!;
    // Server-side caller registers a pending launch waiter, exactly as the
    // launch orchestration would before sending worker.launch_runner.
    const result = await new Promise<WorkerLaunchResult>((resolve) => {
      conn.pendingLaunches.set('req_test', { resolve, reject: () => {} });
      sendHost(ws, {
        kind: WorkerFrameKind.LaunchRunnerResult,
        requestId: 'req_test',
        status: 'launched',
        runnerId: 'runner_token_xyz',
      });
    });

    expect(result.status).toBe('launched');
    expect(result.runnerId).toBe('runner_token_xyz');
    expect(result.error).toBeNull();
    // The waiter is consumed (one-shot).
    expect(conn.pendingLaunches.has('req_test')).toBe(false);
  });

  it('survives a malformed frame without deregistering the worker', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);

    ws.send('not even json');
    await new Promise((r) => setTimeout(r, 30));
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);

    // Routing still works after the malformed frame is dropped.
    const conn = registry.get(ENV_ID)!;
    const result = await new Promise<WorkerLaunchResult>((resolve) => {
      conn.pendingLaunches.set('req_after', { resolve, reject: () => {} });
      sendHost(ws, {
        kind: WorkerFrameKind.LaunchRunnerResult,
        requestId: 'req_after',
        status: 'launched',
        runnerId: 'runner_after',
      });
    });
    expect(result.runnerId).toBe('runner_after');
  });

  it('drops a runner-tunnel pong (keepalive) without deregistering the worker', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp();
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);

    // The worker answers server pings with a runner-tunnel pong on the same socket.
    ws.send(encodeFrame({ kind: FrameKind.Pong, ts: Date.now() }));
    await new Promise((r) => setTimeout(r, 30));
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);
  });
});

// ── worker.runner_exited recording ────────────────────────────

describe('api/worker-tunnel route — runner-exit reports', () => {
  it('records a worker.runner_exited report (owner-scoped) and fires the hook', async () => {
    const runnerExitReports = new RunnerExitReports();
    const exited: { runnerId: string; error: string }[] = [];
    const { baseUrl, registry } = await buildHostTunnelApp({
      runnerExitReports,
      onRunnerExited: async (runnerId, error) => {
        exited.push({ runnerId, error });
      },
    });
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);

    sendHost(ws, {
      kind: WorkerFrameKind.RunnerExited,
      runnerId: 'runner_dead_1',
      error: 'runner process exited with code 1 (log on worker: ~/x.log)',
    });

    await waitFor(() => exited.length === 1);
    expect(exited[0]).toEqual({
      runnerId: 'runner_dead_1',
      error: 'runner process exited with code 1 (log on worker: ~/x.log)',
    });
    // Recorded against the report store, scoped to the environment's workspace.
    expect(runnerExitReports.get('runner_dead_1')).toBe(
      'runner process exited with code 1 (log on worker: ~/x.log)',
    );
    expect(runnerExitReports.getVisible('runner_dead_1', WORKSPACE_ID)).toBe(
      'runner process exited with code 1 (log on worker: ~/x.log)',
    );
    // A different tenant cannot read it (owner-scoped).
    expect(runnerExitReports.getVisible('runner_dead_1', 'ws_other')).toBeUndefined();
    // The worker stays online — runner_exited is a one-way report, not a teardown.
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);
  });
});

// ── Ping loop heartbeats the claim ──────────────────────────
//
// The engine's ping cadence is driven through its injectable `sleep` (loop
// pacing) and `now` (the liveness math) seams — the same way the harness-tunnel
// engine's own unit tests drive it. We inject a fast `sleep` so the loop ticks
// in milliseconds, and a controllable clock so we choose whether the worker looks
// live (heartbeat) or silent past the miss threshold (declared dead). The engine
// constants are PING_INTERVAL_MS=30s, PING_MISS_THRESHOLD=3 → a 90s silence
// window; we never wait real seconds, we move the injected clock.

/** A fast `sleep` that resolves after a short real delay regardless of `ms`. */
function fastSleep(realMs = 8): (ms: number) => Promise<void> {
  return () => new Promise<void>((resolve) => setTimeout(resolve, realMs));
}

describe('api/worker-tunnel route — ping loop heartbeats the claim', () => {
  it('refreshes the durable claim on each ping tick while the tunnel is up', async () => {
    // Real clock + fast sleep: each tick sees only a few ms of elapsed silence,
    // far under the 90s miss window, so the loop takes the heartbeat path every
    // tick and never declares the worker dead.
    const { baseUrl, registry, claims } = await buildHostTunnelApp({
      sleep: fastSleep(8),
    });
    const ws = await connect(baseUrl, ENV_ID);
    await sendHelloAndRegister(ws, registry, ENV_ID);
    await waitFor(() => claims.claims.length === 1);
    const connId = claims.claims[0]!.workerConnId;

    // The ping loop should heartbeat the claim within a couple of ticks,
    // connection-scoped (same conn id), while the worker stays online.
    await waitFor(() => claims.heartbeats.length >= 2);
    expect(claims.heartbeats.every((h) => h.workerConnId === connId)).toBe(true);
    expect(claims.heartbeats.every((h) => h.environmentId === ENV_ID)).toBe(true);
    expect(registry.onlineWorkerIds()).toEqual([ENV_ID]);

    ws.close();
  });

  it('declares the worker dead and closes (4003) after the miss window of silence', async () => {
    // Controllable clock: `lastFrameAt` is stamped at the clock's base when the
    // worker registers. We then jump the clock past the 90s miss window, so the
    // very next ping tick computes elapsed > PING_INTERVAL_MS*PING_MISS_THRESHOLD
    // and the watchdog trips — closing with the ping-timeout code. The fast sleep
    // (30ms) reliably outlasts the synchronous post-register clock bump below, so
    // the first tick reads the jumped clock (no real seconds elapse).
    const clock = { t: 1_000_000 };
    const { baseUrl, registry } = await buildHostTunnelApp({
      now: () => clock.t,
      sleep: fastSleep(30),
    });
    const ws = await connect(baseUrl, ENV_ID);
    const closed = nextClose(ws);
    await sendHelloAndRegister(ws, registry, ENV_ID);
    // Jump past the 90s miss window (30s interval * 3 misses) before the first
    // tick fires; no further inbound frames means `lastFrameAt` stays at base.
    clock.t += 30_000 * 3 + 1;
    expect(await closed).toBe(WORKER_PING_TIMEOUT_CLOSE_CODE);
    await waitFor(() => registry.get(ENV_ID) === undefined);
  });
});

// ── Route mount: single absolute public path, no prefix indirection ──

describe('api/worker-tunnel route — mount path is exact', () => {
  it('upgrades only at the single canonical /v1/tunnels/environments path', async () => {
    const { baseUrl } = await buildHostTunnelApp();
    const canonical = await probePath(baseUrl, WORKER_TUNNEL_PATH(ENV_ID));
    expect(canonical.upgraded).toBe(true);
  });

  it('refuses a doubled /v1/tunnels prefix with 404 (nothing registers)', async () => {
    const { baseUrl, registry } = await buildHostTunnelApp();
    const doubled = await probePath(baseUrl, `/v1/tunnels/v1/tunnels/environments/${ENV_ID}`);
    expect(doubled).toEqual({ upgraded: false, status: 404 });
    expect(registry.onlineWorkerIds()).toEqual([]);
  });

  it('refuses the runner-tunnel path on the worker route table (distinct endpoints)', async () => {
    const { baseUrl } = await buildHostTunnelApp();
    // Only the worker endpoint is registered on this app; the runner path 404s.
    const runnerPath = await probePath(baseUrl, `/v1/tunnels/runners/${ENV_ID}`);
    expect(runnerPath).toEqual({ upgraded: false, status: 404 });
  });
});
