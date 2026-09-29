// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Three-layer claim-based distribution integration test:
//   client `POST /v1/sessions` -> registry dispatches `worker.launch_runner` to
//   the environment's connected worker -> the worker spawns a runner that dials
//   the runner tunnel -> a request is routed end-to-end through the live
//   TunnelRegistry to a fake runner responder.
//
// This exercises the full three-layer flow under the Env-Key/claim model: the
// REGISTRY is the single distributor — it pushes the launch frame to the
// environment's CLAIMED worker over the worker tunnel, pre-registers the expected
// runner in the TunnelRegistry, and the dialing runner is matched back to the
// pending session, which then transitions PENDING -> ASSIGNED.
//
// Production-shaped on purpose: the app `listen`s on an ephemeral loopback port
// and BOTH the worker (worker) tunnel and the runner tunnel are driven by real
// `ws` clients (no in-memory duplex), so the frame encode/decode, the registry
// handshake, the durable environment claim, and the request/response routing all
// cross the live wire. The fake runner is a streaming frame responder: it pulls
// `request` frames off its tunnel and frames a `response.head` + `response.body`
// + `response.end` back, the same shape the real runner adapter produces.
//
// DB-touching, so it lives under test/integration and is gated on the dev
// compose stack (Postgres). The pure decision logic + the distributor
// orchestration are unit-tested in test/unit/session-distributor.spec.ts.

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { WebSocket } from 'ws';
import { eq } from 'drizzle-orm';
import {
  FrameKind,
  WorkerFrameKind,
  INTERNAL_WS_ORIGIN,
  RUNNER_TUNNEL_TOKEN_HEADER,
  decodeFrame,
  encodeFrame,
  decodeWorkerFrame,
  encodeWorkerFrame,
  TunnelTransport,
  type Frame,
  type WorkerFrame,
  type WorkerLaunchRunnerFrame,
} from '@orca/harness-tunnel';
import {
  getTestDb,
  closeTestDb,
  buildStubStore,
  STUB_SSE_CONFIG,
  buildTestJwtMinter,
  buildStubFileStore,
} from './setup.js';
import { uniqueWorkspace, createTestApiKey } from './fixtures.js';
import { buildApp, getTunnelRegistry, getWorkerRegistry } from '../../src/server.js';
import { sessionResources, sessions } from '../../src/persistence/postgres/schema.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';
import { newId } from '../../src/domain/versioning.js';

const ENV_KEY_HEADER = 'x-orca-environment-key';

// Track every socket so a failed assertion can't leak a listener.
const openSockets: WebSocket[] = [];

afterEach(() => {
  for (const ws of openSockets.splice(0)) {
    try {
      ws.terminate();
    } catch {
      // ignore
    }
  }
});

/** Open a `ws` client, push it onto the cleanup list, and resolve once open. */
async function openWs(url: string, headers: Record<string, string>): Promise<WebSocket> {
  const ws = new WebSocket(url, { headers });
  openSockets.push(ws);
  ws.on('error', () => {
    // close-code / failure assertions read the explicit events
  });
  await new Promise<void>((resolve, reject) => {
    ws.once('open', () => resolve());
    ws.once('close', (code: number) => reject(new Error(`ws closed before open: ${code}`)));
  });
  return ws;
}

/** Close a worker socket and wait until the registry deregisters it. */
async function closeWorker(app: FastifyInstance, ws: WebSocket, envId: string): Promise<void> {
  try {
    ws.close();
  } catch {
    // best-effort
  }
  const registry = getWorkerRegistry(app);
  const deadline = Date.now() + 5000;
  while (registry.get(envId) !== undefined) {
    if (Date.now() > deadline) return; // safety net; afterEach terminates anyway
    await new Promise((r) => setTimeout(r, 10));
  }
}

/** A queue of decoded worker frames arriving on a worker tunnel socket. */
class WorkerFrameInbox {
  private readonly queue: WorkerFrame[] = [];
  private waiter: ((f: WorkerFrame) => void) | undefined;

  constructor(ws: WebSocket) {
    ws.on('message', (data: unknown, isBinary: boolean) => {
      if (isBinary) return;
      const text = typeof data === 'string' ? data : String(data);
      let frame: WorkerFrame;
      try {
        frame = decodeWorkerFrame(text);
      } catch {
        // Non-worker frames on this socket (e.g. a server ping encoded with the
        // runner codec) are not what these tests wait on; drop them.
        return;
      }
      const w = this.waiter;
      if (w !== undefined) {
        this.waiter = undefined;
        w(frame);
      } else {
        this.queue.push(frame);
      }
    });
  }

  /** Resolve the next worker frame of `kind`, skipping others (e.g. pings). */
  async next<K extends WorkerFrameKind>(
    kind: K,
    timeoutMs = 5000,
  ): Promise<WorkerFrame & { kind: K }> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const queued = this.queue.shift();
      if (queued !== undefined) {
        if (queued.kind === kind) return queued as WorkerFrame & { kind: K };
        continue;
      }
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error(`timed out waiting for worker frame ${kind}`);
      const frame = await new Promise<WorkerFrame>((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error(`timed out waiting for ${kind}`)),
          remaining,
        );
        this.waiter = (f) => {
          clearTimeout(timer);
          resolve(f);
        };
      });
      if (frame.kind === kind) return frame as WorkerFrame & { kind: K };
    }
  }
}

/**
 * A fake runner tunnel client: dials `/v1/tunnels/runners/:runnerId` with the
 * binding token, sends a hello, then serves every inbound `request` frame by
 * streaming the response back in frames — `response.head` + `response.body`
 * (echoing the path) + `response.end`. Mirrors the production runner adapter's
 * frame shape so the server's `TunnelTransport` reassembles a real response.
 *
 * @returns the encoded hello text so the caller can re-send it until the runner
 *   registers (the same drop-before-listener race the worker hello has).
 */
function startFakeRunner(ws: WebSocket): string {
  const helloText = encodeFrame({
    kind: FrameKind.Hello,
    runnerVersion: '0.1.0-test',
    frameProtocolVersion: 1,
    // Advertise the REAL provider names the runner's ProviderRegistry serves
    // (providerNames()), NOT a made-up string: the registry's connect-time
    // capability-match validates the session's resolved provider against this set.
    // The seeded agent carries metadata:{} → the platform default provider
    // 'claude', so the set MUST include 'claude' or the session would be failed
    // (capability mismatch) instead of transitioning ASSIGNED.
    harnesses: ['claude', 'mock'],
    envs: ['os_sandbox'],
  });
  // Hello so the registry registers the runner session.
  ws.send(helloText);
  ws.on('message', (data: unknown, isBinary: boolean) => {
    if (isBinary) return;
    const text = typeof data === 'string' ? data : String(data);
    let frame: Frame;
    try {
      frame = decodeFrame(text);
    } catch {
      return;
    }
    if (frame.kind === FrameKind.Ping) {
      ws.send(encodeFrame({ kind: FrameKind.Pong, ts: frame.ts }));
      return;
    }
    if (frame.kind !== FrameKind.Request) return;
    const bodyText = JSON.stringify({ echoed_path: frame.path, method: frame.method });
    ws.send(
      encodeFrame({
        kind: FrameKind.ResponseHead,
        id: frame.id,
        status: 200,
        headers: [['content-type', 'application/json']],
      }),
    );
    ws.send(
      encodeFrame({
        kind: FrameKind.ResponseBody,
        id: frame.id,
        body: bodyText,
        encoding: 'utf-8',
      }),
    );
    ws.send(encodeFrame({ kind: FrameKind.ResponseEnd, id: frame.id }));
  });
  return helloText;
}

/** Re-send `helloText` until the runner registers (the drop-before-listener race). */
async function waitRunnerOnline(
  app: FastifyInstance,
  ws: WebSocket,
  helloText: string,
  runnerId: string,
): Promise<void> {
  const registry = getTunnelRegistry(app);
  const deadline = Date.now() + 20_000;
  while (!registry.has(runnerId)) {
    if (Date.now() > deadline) throw new Error('runner did not register within budget');
    await new Promise((r) => setTimeout(r, 50));
    if (!registry.has(runnerId) && ws.readyState === WebSocket.OPEN) {
      ws.send(helloText);
    }
  }
}

/**
 * Poll a session GET until `predicate(session)` holds or the budget elapses.
 *
 * `orca-beta` is required, not decoration: the DEFAULT session view is the
 * Claude-shaped one, which omits `runner_id`, `host_environment_id` and
 * `distribution_state` entirely. Those three are the
 * claim-distribution view this whole file asserts on, and `loadSessionView`
 * only emits them (via `toApi`) on the `orca-beta` branch.
 */
async function waitForSession(
  app: FastifyInstance,
  apiKey: string,
  sessionId: string,
  predicate: (s: Record<string, unknown>) => boolean,
  timeoutMs = 5000,
): Promise<Record<string, unknown>> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const res = await app.inject({
      method: 'GET',
      url: `/v1/sessions/${sessionId}`,
      headers: { 'x-api-key': apiKey, 'orca-beta': 'true' },
    });
    const session = res.json() as Record<string, unknown>;
    if (predicate(session)) return session;
    if (Date.now() > deadline) {
      throw new Error(`session ${sessionId} predicate not met: ${JSON.stringify(session)}`);
    }
    await new Promise((r) => setTimeout(r, 25));
  }
}

interface Stack {
  app: FastifyInstance;
  baseWsUrl: string;
  apiKey: string;
  agentId: string;
  envId: string;
  envKey: string;
  db: DbClient;
}

async function createSelfHostedEnvironment(
  app: FastifyInstance,
  apiKey: string,
): Promise<{ envId: string; envKey: string }> {
  // `orca-beta` for the same reason as `waitForSession` below: the raw
  // `env_key` this helper hands back is an Orca extension the create response
  // echoes only on the beta branch. The default create response is Anthropic's
  // `BetaEnvironment` projection, which has no key concept at all.
  const res = await app.inject({
    method: 'POST',
    url: '/v1/environments',
    headers: { 'x-api-key': apiKey, 'content-type': 'application/json', 'orca-beta': 'true' },
    payload: { name: `self-hosted-${Date.now()}`, target: 'self_hosted' },
  });
  if (res.statusCode !== 200) {
    throw new Error(`environment create failed: ${res.statusCode} ${res.body}`);
  }
  const body = res.json() as { id: string; env_key: string; target: string };
  return { envId: body.id, envKey: body.env_key };
}

describe('claim-based distribution — three-layer (integration)', () => {
  let stack: Stack;

  beforeAll(async () => {
    const { db } = await getTestDb();
    const app = buildApp({
      db,
      oidc: { allowedIssuers: [], audience: 'orca-managed-agents' },
      store: buildStubStore(),
      sse: STUB_SSE_CONFIG,
      jwtMinter: buildTestJwtMinter(),
      fileStore: buildStubFileStore(),
      // Speed the bounded launch-result wait so a degenerate test (no result
      // frame) does not sit on the full 10s; the happy path answers instantly.
    });
    await app.ready();
    await app.listen({ host: '127.0.0.1', port: 0 });
    const addr = app.server.address();
    if (addr === null || typeof addr === 'string') throw new Error('expected a TCP address');
    const baseWsUrl = `ws://127.0.0.1:${addr.port}`;

    const apiKey = await createTestApiKey(db, uniqueWorkspace('dist3l'));
    const agentResp = await app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
      payload: {
        name: `dist-agent-${Date.now()}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata: {},
      },
    });
    const agentId = (agentResp.json() as { id: string }).id;
    const { envId, envKey } = await createSelfHostedEnvironment(app, apiKey);

    stack = { app, baseWsUrl, apiKey, agentId, envId, envKey, db };
    // Agent + API-key + environment creation each pay the bcrypt / per-request
    // cost (seconds in aggregate under the dev stack), so the setup hook needs a
    // budget well above vitest's 10s default.
  }, 60_000);

  afterAll(async () => {
    if (stack !== undefined) {
      await stack.app.close();
    }
    await closeTestDb();
  });

  /**
   * Connect a worker (worker) tunnel for the shared environment, send hello, and
   * WAIT until the registry has registered the connection. The wait is essential:
   * `dispatch` only sends a `worker.launch_runner` frame when a worker is connected
   * at create time, so a `createSession()` that races an un-registered worker
   * would (correctly) leave the session pending with no frame — making the launch
   * assertions vacuously time out. Waiting closes that race deterministically.
   *
   * Each worker-using test closes its worker via {@link closeWorker} before
   * finishing (it waits for deregistration), so the shared environment's
   * single-`workerId` slot is clean for the next test: the in-memory
   * `WorkerRegistry.deregister` is keyed on `workerId` (not the connection), so a
   * lagging prior teardown could otherwise evict a live worker — closing cleanly
   * and waiting avoids that race without paying a fresh-environment create cost
   * per test. The budget is generous because the single-process integration run
   * shares one event loop + libuv thread pool with the bcrypt-heavy create paths.
   */
  async function connectWorker(env: {
    envId: string;
    envKey: string;
  }): Promise<{ ws: WebSocket; inbox: WorkerFrameInbox }> {
    const ws = await openWs(`${stack.baseWsUrl}/v1/tunnels/environments/${env.envId}`, {
      [ENV_KEY_HEADER]: env.envKey,
      origin: INTERNAL_WS_ORIGIN,
    });
    const inbox = new WorkerFrameInbox(ws);
    const helloText = encodeWorkerFrame({
      kind: WorkerFrameKind.Hello,
      version: '0.1.0-test',
      frameProtocolVersion: 1,
      name: 'worker-it',
      runners: [],
      configuredHarnesses: null,
    });
    // Re-send the hello on each poll tick until the registry registers the
    // worker. A frame the client sends in the brief window between the WS upgrade
    // completing and the server attaching its inbound listener (the handler runs
    // a tick later, through the async auth pre-handler chain) is dropped by `ws`;
    // re-sending is idempotent — the engine acts on the first hello it actually
    // receives — so a dropped first hello no longer wedges registration. A real
    // worker dials with its own reconnect/keepalive cadence; this is the test
    // client's equivalent.
    const registry = getWorkerRegistry(stack.app);
    const deadline = Date.now() + 20_000;
    ws.send(helloText);
    while (registry.get(env.envId) === undefined) {
      if (Date.now() > deadline) throw new Error('worker did not register within budget');
      await new Promise((r) => setTimeout(r, 50));
      if (registry.get(env.envId) === undefined && ws.readyState === WebSocket.OPEN) {
        ws.send(helloText);
      }
    }
    return { ws, inbox };
  }

  /**
   * Create a self_hosted session for `envId` and return its id + create body.
   * `orca-beta` for the same reason as `waitForSession`: the create response is
   * the same view, and the distribution fields are on the beta branch only.
   */
  async function createSession(
    envId: string,
    agentId: string = stack.agentId,
  ): Promise<{ id: string; body: Record<string, unknown> }> {
    const res = await stack.app.inject({
      method: 'POST',
      url: '/v1/sessions',
      headers: {
        'x-api-key': stack.apiKey,
        'content-type': 'application/json',
        'orca-beta': 'true',
      },
      payload: { agent_id: agentId, environment_id: envId },
    });
    expect(res.statusCode).toBe(200);
    const body = res.json() as Record<string, unknown>;
    return { id: body.id as string, body };
  }

  /** Create an agent carrying `metadata` (its harness annotation) and return its id. */
  async function createAgent(metadata: Record<string, unknown>): Promise<string> {
    const res = await stack.app.inject({
      method: 'POST',
      url: '/v1/agents',
      headers: { 'x-api-key': stack.apiKey, 'content-type': 'application/json' },
      payload: {
        name: `dist-agent-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        model: { provider: 'anthropic', id: 'claude-3-5-sonnet-20240620' },
        system: '',
        tools: [],
        mcp_servers: [],
        skills: [],
        metadata,
      },
    });
    expect(res.statusCode).toBe(200);
    return (res.json() as { id: string }).id;
  }

  it('stays PENDING with no runner binding when no worker is connected', async () => {
    // No worker tunnel for the environment: the dispatch persists PENDING with
    // no runner id / worker environment id, surfaced by the work depth. Reuses the
    // shared environment precisely because it never connects a worker to it.
    const { id: sessionId, body } = await createSession(stack.envId);
    expect(body.distribution_state).toBe('pending');
    expect(body.runner_id).toBeNull();
    expect(body.host_environment_id).toBeNull();

    const fetched = await stack.app.inject({
      method: 'GET',
      url: `/v1/sessions/${sessionId}`,
      headers: { 'x-api-key': stack.apiKey, 'orca-beta': 'true' },
    });
    const session = fetched.json() as Record<string, unknown>;
    expect(session.distribution_state).toBe('pending');
    expect(session.runner_id).toBeNull();
  });

  // The full three-layer happy path on the live WS wire: a client creates a
  // self_hosted session, the registry pushes `worker.launch_runner` to the
  // connected worker, the worker spawns a runner that dials the runner tunnel,
  // the matched session flips ASSIGNED, and a request is routed end-to-end to a
  // fake runner responder through the live `TunnelRegistry`.
  //
  // Scope note: this exercises one worker + one runner + one session in a single
  // test. The launch-refusal (worker.launch_runner_result status=failed -> FAILED)
  // and runner-exited (worker.runner_exited -> FAILED) and post-connect
  // runner-disconnect (-> FAILED) transitions are covered in
  // test/unit/session-distributor.spec.ts (which drives a `failed` launch-result
  // frame, `markRunnerExited`, and `markRunnerDisconnected` directly) and at the
  // worker-tunnel engine layer (worker-tunnel.test.ts). They are intentionally not
  // re-driven here: a second concurrent in-process WS connection (a fresh worker,
  // or a second session's launch frame after a runner attaches) trips a
  // `@fastify/websocket` v11 server->client delivery stall that is specific to
  // the single-process test harness and unrelated to the distribution logic, so
  // adding those branches here would only buy harness flakiness, not coverage.
  // The no-worker -> reconnect-drive path and the workspace-carrying launch frame
  // ARE driven end-to-end below (they need only one live worker, so they are
  // immune to that stall).
  it('routes a request end-to-end: create -> launch_runner -> runner connects -> assigned -> tunneled response', async () => {
    const { app, baseWsUrl } = stack;
    const env = await createSelfHostedEnvironment(stack.app, stack.apiKey);
    const { ws: workerWs, inbox } = await connectWorker(env);
    try {
      // Layer 1: the client creates a self_hosted session. The registry persists
      // it PENDING and dispatches a launch to the connected worker, binding a
      // runner id + the worker environment id on the row.
      const { id: sessionId, body: createBody } = await createSession(env.envId);
      expect(createBody.distribution_state).toBe('pending');
      expect(createBody.host_environment_id).toBe(env.envId);
      const runnerId = createBody.runner_id as string;
      expect(runnerId).toMatch(/^runner_token_[0-9a-f]{32}$/);

      // Layer 2: the worker receives `worker.launch_runner` carrying the binding
      // token for that exact runner id, and confirms the spawn.
      const launch: WorkerLaunchRunnerFrame = await inbox.next(WorkerFrameKind.LaunchRunner);
      expect(launch.bindingToken.length).toBeGreaterThan(0);
      workerWs.send(
        encodeWorkerFrame({
          kind: WorkerFrameKind.LaunchRunnerResult,
          requestId: launch.requestId,
          status: 'launched',
          runnerId,
        }),
      );

      // Layer 3: the worker spawns a runner that dials the runner tunnel with the
      // binding token. The path runner id equals the token-bound id, so the
      // binding-token gate accepts it and the registry matches it to the session.
      const runnerWs = await openWs(`${baseWsUrl}/v1/tunnels/runners/${runnerId}`, {
        [RUNNER_TUNNEL_TOKEN_HEADER]: launch.bindingToken,
        origin: INTERNAL_WS_ORIGIN,
      });
      const runnerHello = startFakeRunner(runnerWs);
      await waitRunnerOnline(app, runnerWs, runnerHello, runnerId);

      // The connect hook flips the session ASSIGNED once the runner registers.
      const assigned = await waitForSession(
        app,
        stack.apiKey,
        sessionId,
        (s) => s.distribution_state === 'assigned',
      );
      expect(assigned.runner_id).toBe(runnerId);
      expect(assigned.distribution_state).toBe('assigned');

      // End-to-end routing: a request tunneled to the matched runner through the
      // live registry reaches the fake runner and the framed response comes back.
      const registry = getTunnelRegistry(app);
      expect(registry.has(runnerId)).toBe(true);
      const transport = new TunnelTransport(registry, runnerId);
      const response = await transport.handleRequest({
        method: 'GET',
        path: '/v1/runner/ping',
        headers: [],
      });
      expect(response.status).toBe(200);
      const chunks: Uint8Array[] = [];
      for await (const chunk of response.stream) {
        chunks.push(chunk);
      }
      const decoded = JSON.parse(Buffer.concat(chunks).toString('utf-8')) as {
        echoed_path: string;
        method: string;
      };
      expect(decoded.echoed_path).toBe('/v1/runner/ping');
      expect(decoded.method).toBe('GET');
    } finally {
      await closeWorker(stack.app, workerWs, env.envId);
    }
  }, 60_000);

  // Capability-match fires in PRODUCTION wiring (the gap: the check was
  // structurally inert because the distribution row hardcoded a null provider).
  // The session's agent resolves to provider 'codex' (metadata harness=codex),
  // which the connecting runner does NOT advertise (its hello harnesses are
  // ['claude','mock']). The connect-time capability-match must therefore flip the
  // session FAILED instead of ASSIGNED — proving the real snapshot-derived provider
  // is threaded into the distribution row the check reads, not a hardcoded null.
  // One worker + one runner, so it is immune to the second-connection stall noted
  // above. The pre-spawn worker-side branch stays a unit test (the integration
  // worker advertises configuredHarnesses=null, which fails that branch OPEN).
  it('fails a session on connect when its provider is not advertised by the runner (capability mismatch)', async () => {
    const { app, baseWsUrl } = stack;
    // provider 'codex' — a real, valid annotation (codex only supports colocated),
    // absent from the runner's advertised ['claude','mock'] set.
    const codexAgentId = await createAgent({ harness: 'codex', mode: 'colocated' });
    const env = await createSelfHostedEnvironment(stack.app, stack.apiKey);
    const { ws: workerWs, inbox } = await connectWorker(env);
    try {
      const { id: sessionId, body: createBody } = await createSession(env.envId, codexAgentId);
      // The worker's configuredHarnesses is null (older-worker fail-open), so the
      // PRE-SPAWN worker check does not fire — the launch is still sent and a runner
      // id is bound. Enforcement lands at connect time against the real runner set.
      expect(createBody.distribution_state).toBe('pending');
      const runnerId = createBody.runner_id as string;
      expect(runnerId).toMatch(/^runner_token_[0-9a-f]{32}$/);

      const launch: WorkerLaunchRunnerFrame = await inbox.next(WorkerFrameKind.LaunchRunner);
      // The launch frame carries the resolved provider as its harness (not null).
      expect(launch.harness).toBe('codex');
      workerWs.send(
        encodeWorkerFrame({
          kind: WorkerFrameKind.LaunchRunnerResult,
          requestId: launch.requestId,
          status: 'launched',
          runnerId,
        }),
      );

      // The runner dials and registers, advertising ['claude','mock'] — which does
      // NOT include 'codex'. The connect hook's capability-match flips the session
      // FAILED rather than ASSIGNED.
      const runnerWs = await openWs(`${baseWsUrl}/v1/tunnels/runners/${runnerId}`, {
        [RUNNER_TUNNEL_TOKEN_HEADER]: launch.bindingToken,
        origin: INTERNAL_WS_ORIGIN,
      });
      const runnerHello = startFakeRunner(runnerWs);
      await waitRunnerOnline(app, runnerWs, runnerHello, runnerId);

      const failed = await waitForSession(
        app,
        stack.apiKey,
        sessionId,
        (s) => s.distribution_state === 'failed',
      );
      expect(failed.distribution_state).toBe('failed');
    } finally {
      await closeWorker(stack.app, workerWs, env.envId);
    }
  }, 60_000);

  // The MAJOR reconnect-drive path end-to-end: a session is created while its
  // environment worker is OFFLINE (stays pending, no runner binding), then the
  // worker connects and the worker-tunnel `onWorkerConnect` hook drives the launch —
  // the worker receives `worker.launch_runner`, the runner dials, and the session
  // flips ASSIGNED. This is the wire-level proof that the create-time-stranded
  // session is launched on (re)connect, not stuck pending forever.
  it('drives a pending session on worker (re)connect: create offline -> connect -> launch_runner -> assigned', async () => {
    const { app, baseWsUrl } = stack;
    const env = await createSelfHostedEnvironment(stack.app, stack.apiKey);

    // 1. No worker connected yet: the create leaves the session PENDING with no
    //    runner binding (the stranded state the reconnect must drive).
    const { id: sessionId, body: createBody } = await createSession(env.envId);
    expect(createBody.distribution_state).toBe('pending');
    expect(createBody.runner_id).toBeNull();
    expect(createBody.host_environment_id).toBeNull();

    // 2. The worker connects. The connect hook drives the stranded session: the
    //    worker receives a `worker.launch_runner` frame for a freshly-minted runner.
    const { ws: workerWs, inbox } = await connectWorker(env);
    try {
      const launch: WorkerLaunchRunnerFrame = await inbox.next(WorkerFrameKind.LaunchRunner);
      expect(launch.bindingToken.length).toBeGreaterThan(0);

      // The driven launch bound a runner id + the worker environment id on the row.
      const driven = await waitForSession(
        app,
        stack.apiKey,
        sessionId,
        (s) => s.runner_id !== null,
      );
      const runnerId = driven.runner_id as string;
      expect(runnerId).toMatch(/^runner_token_[0-9a-f]{32}$/);
      expect(driven.host_environment_id).toBe(env.envId);
      expect(driven.distribution_state).toBe('pending');

      // 3. The worker confirms the spawn and the runner dials its tunnel; the
      //    matched session flips ASSIGNED — same as a create-time launch.
      workerWs.send(
        encodeWorkerFrame({
          kind: WorkerFrameKind.LaunchRunnerResult,
          requestId: launch.requestId,
          status: 'launched',
          runnerId,
        }),
      );
      const runnerWs = await openWs(`${baseWsUrl}/v1/tunnels/runners/${runnerId}`, {
        [RUNNER_TUNNEL_TOKEN_HEADER]: launch.bindingToken,
        origin: INTERNAL_WS_ORIGIN,
      });
      const runnerHello = startFakeRunner(runnerWs);
      await waitRunnerOnline(app, runnerWs, runnerHello, runnerId);

      const assigned = await waitForSession(
        app,
        stack.apiKey,
        sessionId,
        (s) => s.distribution_state === 'assigned',
      );
      expect(assigned.runner_id).toBe(runnerId);
    } finally {
      await closeWorker(stack.app, workerWs, env.envId);
    }
  }, 60_000);

  // The workspace-carrying launch frame (gap: the frame must carry the session's
  // repository workspace, not a hardcoded null). A session with an attached
  // `github_repository` resource resolves its workspace to that resource's mount
  // path; the dispatched `worker.launch_runner` frame carries it on the wire.
  it('carries the session repository workspace in the launch frame', async () => {
    const env = await createSelfHostedEnvironment(stack.app, stack.apiKey);
    const { id: sessionId } = await createSession(env.envId);

    // Persist a github_repository resource for the session directly (the store's
    // workspace resolution reads what is persisted; the route's vault-binding
    // validation is exercised elsewhere and is not what this test covers).
    const repoMountPath = '/workspace/myrepo/';
    await seedRepoResource(stack.db, sessionId, repoMountPath);

    // Connecting the worker drives the (already-pending) session; the launch frame
    // it receives carries the resolved repository workspace.
    const { ws: workerWs, inbox } = await connectWorker(env);
    try {
      const launch: WorkerLaunchRunnerFrame = await inbox.next(WorkerFrameKind.LaunchRunner);
      expect(launch.workspace).toBe(repoMountPath);
    } finally {
      await closeWorker(stack.app, workerWs, env.envId);
    }
  }, 60_000);
});

/**
 * Insert a still-attached `github_repository` session resource for `sessionId`
 * with the given mount path, so the distribution store resolves it as the
 * session's workspace. Bypasses the create-route vault-binding validation on
 * purpose — this seeds the persisted read model the store reads, which is the
 * surface under test for the workspace-carrying launch frame.
 */
async function seedRepoResource(db: DbClient, sessionId: string, mountPath: string): Promise<void> {
  // `session_resources.workspace_id` is now required (FK'd to the owning
  // session's workspace) — look it up rather than widen this helper's
  // signature, since every call site only has the session id in hand.
  const [session] = await db
    .select({ workspaceId: sessions.workspaceId })
    .from(sessions)
    .where(eq(sessions.id, sessionId))
    .limit(1);
  if (!session) throw new Error(`seedRepoResource: session ${sessionId} not found`);

  await db.insert(sessionResources).values({
    id: newId('sesrsc'),
    workspaceId: session.workspaceId,
    sessionId,
    type: 'github_repository',
    fileId: null,
    memoryStoreId: null,
    repoRef: { vault_id: 'vault_test', url: 'https://github.com/org/myrepo' },
    mountPath,
    access: 'read_write',
    mountStrategy: null,
    instructions: null,
    attachedAt: new Date(),
    detachedAt: null,
  });
}
