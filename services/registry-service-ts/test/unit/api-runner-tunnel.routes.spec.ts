// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// In-process spec for the `src/api/runner-tunnel.routes.ts` WebSocket endpoint.
//
// Every test spins up an in-process Fastify app on an ephemeral loopback port
// and connects a REAL `ws` client — no DB, no dev-stack, no in-memory duplex.
// That makes the close/disconnect handshake and the pre-accept rejection paths
// behave like production.
//
// Peer-host resolution and the handshake `Origin` are driven through the route's
// production options (`resolvePeerHost`, which defaults to the real socket
// address; the `Origin` header on the `ws` client). A real TCP client always
// connects from `127.0.0.1`, so to exercise the NON-loopback auth branches the
// app under test overrides `resolvePeerHost` to report a chosen host. This is the
// same seam production uses (the default reads `req.socket.remoteAddress`); the
// tests just supply a different trusted source.

import { describe, it, expect, afterEach } from 'vitest';
import Fastify, { type FastifyInstance, type FastifyRequest } from 'fastify';
import websocket from '@fastify/websocket';
import { WebSocket } from 'ws';
import {
  FrameKind,
  TUNNEL_MAX_MESSAGE_BYTES,
  INTERNAL_WS_ORIGIN,
  decodeFrame,
  encodeFrame,
  tokenBoundRunnerId,
  RUNNER_TUNNEL_TOKEN_HEADER,
  type HelloFrame,
} from '@orca/harness-tunnel';
import { TunnelRegistry } from '../../src/tunnel/tunnel-registry.js';
import { RunnerExitReports } from '../../src/tunnel/runner-exit-reports.js';
import {
  registerRunnerTunnelRoutes,
  LOCAL_TUNNEL_OWNER,
  FORBIDDEN_ORIGIN_CLOSE_CODE,
  RUNNER_ID_MISMATCH_CLOSE_CODE,
  type TunnelAuthProvider,
  type RunnerTunnelRouteOptions,
} from '../../src/api/runner-tunnel.routes.js';
import { buildAuth } from '../../src/auth/auth.js';
import type { DbClient } from '../../src/persistence/postgres/client.js';

const RUNNER_ID = 'runner-route-test-1';

function helloFrame(): HelloFrame {
  return {
    kind: FrameKind.Hello,
    runnerVersion: '0.1.0-test',
    frameProtocolVersion: 1,
    harnesses: ['claude-sdk'],
    envs: ['os_sandbox'],
  };
}

// ── Auth provider stub modeling the OIDC / accounts contract ──
//
// Returns the user id carried by a credential header when present, and `null`
// otherwise — exactly how a cookie/Bearer auth provider behaves when a missing
// or invalid credential yields no identity. This is deliberately not a "header
// mode" provider that falls back to the reserved local user: it must be able to
// produce the `null` that the fail-closed gate turns on. A real provider object
// (not a mock) so the route's `authProvider !== undefined` checks behave like
// production and the exact fail-closed branch is exercised.
class CredentialHeaderAuthProvider implements TunnelAuthProvider {
  constructor(private readonly credentialHeader = 'x-test-user') {}

  getUserId(req: FastifyRequest): string | null {
    const raw = req.headers[this.credentialHeader];
    const value = Array.isArray(raw) ? raw[0] : raw;
    return value ?? null;
  }
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

interface BuildOpts extends Omit<RunnerTunnelRouteOptions, 'registry'> {
  /** Host the route should treat as the peer (drives the loopback branches). */
  peerHost?: string;
}

async function buildTunnelApp(
  registry: TunnelRegistry,
  opts: BuildOpts = {},
): Promise<{ app: FastifyInstance; baseUrl: string }> {
  const { peerHost, ...routeOpts } = opts;
  const app = Fastify({ logger: false });
  openApps.push(app);
  await app.register(websocket, { options: { maxPayload: TUNNEL_MAX_MESSAGE_BYTES } });
  registerRunnerTunnelRoutes(app, {
    registry,
    ...routeOpts,
    ...(peerHost !== undefined ? { resolvePeerHost: () => peerHost } : {}),
  });
  await app.listen({ host: '127.0.0.1', port: 0 });
  const addr = app.server.address();
  if (addr === null || typeof addr === 'string') {
    throw new Error('expected a TCP address');
  }
  return { app, baseUrl: `ws://127.0.0.1:${addr.port}` };
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
  runnerId: string,
  opts: { headers?: Record<string, string>; origin?: string | null } = {},
): Promise<WebSocket> {
  const headers: Record<string, string> = { ...(opts.headers ?? {}) };
  // Default first-party clients announce the internal origin so the CSWSH guard
  // lets them through; tests that probe the guard pass `origin: <something>` or
  // `origin: null` to omit it.
  if (opts.origin === undefined) {
    headers.origin = INTERNAL_WS_ORIGIN;
  } else if (opts.origin !== null) {
    headers.origin = opts.origin;
  }
  const ws = new WebSocket(`${baseUrl}/v1/tunnels/runners/${runnerId}`, {
    headers,
  });
  openSockets.push(ws);
  // Keep a permanent no-op error listener so a rejected handshake (which emits
  // 'error' then 'close') never surfaces as an unhandled socket error after the
  // connect promise has already resolved on 'close'.
  ws.on('error', () => {
    // swallow; close-code assertions read the 'close' event
  });
  await new Promise<void>((resolve) => {
    ws.once('open', () => resolve());
    ws.once('error', () => {
      // A failed handshake (e.g. 4403 before accept) surfaces as an error on
      // some Node versions; the close handler still resolves with the code.
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

/**
 * Outcome of probing an arbitrary upgrade path: either the WS handshake
 * completed (`{ upgraded: true }`) or the server refused the upgrade with an
 * HTTP status (`{ upgraded: false, status }`).
 */
type ProbeOutcome = { upgraded: true } | { upgraded: false; status: number | undefined };

/**
 * Open a raw `ws` client against a fully-explicit upgrade `path` (NOT the
 * canonical `/internal/...` path the {@link connect} helper hardcodes) and
 * resolve with whether the route accepted the WebSocket upgrade.
 *
 * Used to pin the route's exact mount: the endpoint is registered at one
 * absolute literal path (`/v1/tunnels/runners/:runnerId`) with no prefix
 * indirection, so any other path — a doubled `/v1/tunnels/v1/tunnels/...`, the
 * old mesh-only `/internal/runners/.../tunnel` path, etc. — misses the route
 * table and is refused with an HTTP 404 on the upgrade (surfaced by `ws` as an
 * `unexpected-response`). There is no mount-prefix layer that could re-prepend a
 * segment, so a successful upgrade can only happen at the single canonical path.
 */
async function probePath(baseUrl: string, path: string): Promise<ProbeOutcome> {
  const ws = new WebSocket(`${baseUrl}${path}`);
  openSockets.push(ws);
  ws.on('error', () => {
    // A 404 upgrade refusal surfaces as 'unexpected-response' then 'error' then
    // 'close'; swallow the error so it isn't an unhandled socket error.
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

function send(ws: WebSocket, frame: Parameters<typeof encodeFrame>[0]): void {
  ws.send(encodeFrame(frame));
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

/** Send hello and wait until the runner registers. */
async function sendHelloAndRegister(
  ws: WebSocket,
  registry: TunnelRegistry,
  runnerId: string,
  helloOverride?: Partial<HelloFrame>,
): Promise<void> {
  send(ws, { ...helloFrame(), ...helloOverride });
  await waitFor(() => registry.has(runnerId));
}

// ── Registration + request routing ──────────────────────────

describe('api/runner-tunnel route — registration + request routing', () => {
  it('round-trips a request to the runner through the live FastifyWS route', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);
    const ws = await connect(baseUrl, RUNNER_ID);

    // Fake runner: reply to any request frame with head + body + end. The
    // tunneled "app" here is a minimal /health responder.
    ws.on('message', (data: unknown) => {
      const frame = decodeFrame(typeof data === 'string' ? data : String(data));
      if (frame.kind !== FrameKind.Request) {
        return;
      }
      expect(frame.method).toBe('GET');
      expect(frame.path).toBe('/health');
      send(ws, { kind: FrameKind.ResponseHead, id: frame.id, status: 200 });
      send(ws, {
        kind: FrameKind.ResponseBody,
        id: frame.id,
        body: JSON.stringify({ status: 'ok' }),
        encoding: 'utf-8',
      });
      send(ws, { kind: FrameKind.ResponseEnd, id: frame.id });
    });

    await sendHelloAndRegister(ws, registry, RUNNER_ID);
    expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);

    const session = registry.get(RUNNER_ID)!;
    const reqId = 'req-live-1';
    const state = registry.openRequest(RUNNER_ID, reqId);
    await registry.sendText(
      session,
      encodeFrame({ kind: FrameKind.Request, id: reqId, method: 'GET', path: '/health' }),
    );

    const head = await state.headFuture.promise;
    expect(head.status).toBe(200);

    const chunks: Buffer[] = [];
    for (;;) {
      const item = await state.bodyQueue.get();
      if (item === null) {
        break;
      }
      chunks.push(Buffer.from(item.body, 'utf-8'));
    }
    expect(JSON.parse(Buffer.concat(chunks).toString('utf-8'))).toEqual({ status: 'ok' });
  });

  it('flips runner status online after tunnel registration', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);

    expect(registry.has(RUNNER_ID)).toBe(false);

    const ws = await connect(baseUrl, RUNNER_ID);
    await sendHelloAndRegister(ws, registry, RUNNER_ID);

    expect(registry.has(RUNNER_ID)).toBe(true);
    expect(registry.get(RUNNER_ID)!.hello.harnesses).toEqual(['claude-sdk']);
  });

  it('exposes live runners + advertised harnesses on the registry', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);

    expect(registry.onlineRunnerIds()).toEqual([]);

    const ws = await connect(baseUrl, RUNNER_ID);
    await sendHelloAndRegister(ws, registry, RUNNER_ID);

    expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);
    const session = registry.get(RUNNER_ID)!;
    expect(session.hello.harnesses).toEqual(['claude-sdk']);
  });

  it('routes a tunneled WS-channel frame back onto the channel inbound queue', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);
    const runnerId = 'runner-live-ws';
    const ws = await connect(baseUrl, runnerId);

    ws.on('message', (data: unknown) => {
      const frame = decodeFrame(typeof data === 'string' ? data : String(data));
      if (frame.kind === FrameKind.WsFrame && frame.data === 'ping') {
        send(ws, { kind: FrameKind.WsFrame, chId: frame.chId, data: 'pong', encoding: 'utf-8' });
      }
    });

    await sendHelloAndRegister(ws, registry, runnerId);
    const session = registry.get(runnerId)!;

    const channel = registry.openWsChannel(runnerId, 'chlive01');
    await registry.sendText(
      session,
      encodeFrame({ kind: FrameKind.WsFrame, chId: 'chlive01', data: 'ping', encoding: 'utf-8' }),
    );
    const item = (await channel.inboundQueue.get()) as ['text', string];
    expect(item[0]).toBe('text');
    expect(item[1]).toBe('pong');
  });
});

// ── Route mount: single absolute public path, no prefix indirection ──
//
// The runner tunnel is the one endpoint a self-hosted runner dials from outside
// the mesh, so it lives under the PUBLIC `/v1/tunnels/*` namespace at a single
// absolute literal path (`/v1/tunnels/runners/:runnerId`) with no mount-prefix
// layer. These pin that exact mount so a future refactor (e.g. switching to a
// prefixed Fastify plugin) that reintroduces prefix indirection — or a regression
// to the old mesh-only `/internal/...` path — is caught: the canonical path
// upgrades, and every off-path variant is a hard 404 with nothing registered.

describe('api/runner-tunnel route — mount path is exact (not double-prefixed)', () => {
  it('upgrades only at the single canonical /v1/tunnels path', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);

    const canonical = await probePath(baseUrl, `/v1/tunnels/runners/${RUNNER_ID}`);
    expect(canonical.upgraded).toBe(true);
  });

  it('refuses a doubled /v1/tunnels/v1/tunnels prefix with 404 (nothing registers)', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);

    const doubled = await probePath(baseUrl, `/v1/tunnels/v1/tunnels/runners/${RUNNER_ID}`);
    expect(doubled).toEqual({ upgraded: false, status: 404 });
    expect(registry.onlineRunnerIds()).toEqual([]);
  });

  it('refuses the old mesh-only /internal tunnel path with 404 (nothing registers)', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);

    // The tunnel was relocated off the mesh-only `/internal/*` namespace onto the
    // public `/v1/tunnels/*` namespace (a self-hosted runner cannot reach
    // `/internal/*`). The old path must miss the route table entirely.
    const oldInternal = await probePath(baseUrl, `/internal/runners/${RUNNER_ID}/tunnel`);
    expect(oldInternal).toEqual({ upgraded: false, status: 404 });
    expect(registry.onlineRunnerIds()).toEqual([]);
  });
});

// ── Public mount + global-auth bypass (`/v1/tunnels/*`) ─────
//
// In production the tunnel route is registered behind the global `buildAuth`
// pre-handler (see `server.ts`), but the runner dials it from OUTSIDE the mesh
// with NO api-key / OIDC token — only its tunnel binding token. `auth.ts`
// therefore bypasses standard app-auth for `/v1/tunnels/*`, leaving the WS
// handler to self-authenticate by binding token. These tests wire the REAL
// `buildAuth` pre-handler (exactly as `buildApp` does) and prove (a) the tunnel
// upgrades + registers with no api-key, and (b) the bypass — not an absent
// pre-handler — is what admits it, by showing a non-bypassed route under the
// same pre-handler is rejected with 401 when no credential is presented.

/**
 * A `DbClient` stand-in whose `select` throws if ever called. The api-key auth
 * arm only queries the DB when an `x-api-key: orca_…` header is present; none of
 * these tests send one, so the query path is never reached — a thrown error here
 * would mean the bypass / no-credential routing regressed into the DB lookup.
 */
function unusedDb(): DbClient {
  return {
    select() {
      throw new Error('db.select must not be called: no x-api-key header was sent');
    },
  } as unknown as DbClient;
}

async function buildAuthedTunnelApp(
  registry: TunnelRegistry,
  opts: BuildOpts = {},
): Promise<{ app: FastifyInstance; baseUrl: string }> {
  const { peerHost, ...routeOpts } = opts;
  const app = Fastify({ logger: false });
  openApps.push(app);
  await app.register(websocket, { options: { maxPayload: TUNNEL_MAX_MESSAGE_BYTES } });
  // The exact global pre-handler production runs: it bypasses `/v1/tunnels/*`
  // and applies api-key / OIDC auth everywhere else.
  app.addHook(
    'preHandler',
    buildAuth({ db: unusedDb(), oidc: { allowedIssuers: [], audience: 'orca' } }),
  );
  registerRunnerTunnelRoutes(app, {
    registry,
    ...routeOpts,
    ...(peerHost !== undefined ? { resolvePeerHost: () => peerHost } : {}),
  });
  // A normal (NON-bypassed) public route under the same pre-handler, used as the
  // control: without a credential the global auth must 401 it.
  app.get('/v1/agents', async () => ({ data: [] }));
  await app.listen({ host: '127.0.0.1', port: 0 });
  const addr = app.server.address();
  if (addr === null || typeof addr === 'string') {
    throw new Error('expected a TCP address');
  }
  return { app, baseUrl: `ws://127.0.0.1:${addr.port}` };
}

describe('api/runner-tunnel route — public mount bypasses global app-auth', () => {
  it('upgrades + registers behind buildAuth with only the binding token (no api-key)', async () => {
    const registry = new TunnelRegistry();
    // Remote peer (non-loopback) + an allow-listed binding token: the ONLY
    // credential presented is the tunnel token header — no x-api-key, no Bearer.
    const token = 'remote-binding-token';
    const runnerId = 'runner_remote_no_apikey';
    const { baseUrl } = await buildAuthedTunnelApp(registry, {
      allowedTunnelTokens: new Set([token]),
      peerHost: '203.0.113.9',
    });
    const ws = await connect(baseUrl, runnerId, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: token },
    });
    // Reaching the handler and registering proves buildAuth did NOT 401 the
    // upgrade: the `/v1/tunnels/*` bypass let it through to self-authenticate.
    await sendHelloAndRegister(ws, registry, runnerId);
    expect(registry.onlineRunnerIds()).toEqual([runnerId]);
  });

  it('still rejects a non-bypassed route with 401 when no credential is presented', async () => {
    // Control: the same pre-handler enforces auth everywhere it does not bypass,
    // so the tunnel upgrade above succeeded because of the bypass — not because
    // the pre-handler was absent or inert.
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildAuthedTunnelApp(registry);
    const res = await httpGet(baseUrl, '/v1/agents');
    expect(res.status).toBe(401);
    expect(res.body).toEqual({ error: 'unauthenticated' });
  });

  it('a tunnel-token-only dial that fails the in-handler binding check is still refused (4004)', async () => {
    // The bypass admits the dial to the handler; it does NOT weaken the handler's
    // own auth. A remote peer whose token is not in the server allow-list is
    // refused by the in-handler binding gate, proving the bypass only skips the
    // api-key/OIDC layer and leaves the binding-token authentication intact.
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildAuthedTunnelApp(registry, {
      allowedTunnelTokens: new Set(['the-only-valid-token']),
      peerHost: '203.0.113.9',
    });
    const ws = await connect(baseUrl, 'runner_bad_token', {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: 'not-the-valid-token' },
    });
    const code = await nextClose(ws);
    expect(code).toBe(RUNNER_ID_MISMATCH_CLOSE_CODE);
    expect(registry.onlineRunnerIds()).toEqual([]);
  });
});

// ── Hello handshake / version skew ──────────────────────────

describe('api/runner-tunnel route — hello handshake', () => {
  it('closes with 4002 on a frame-protocol-major mismatch', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);
    const ws = await connect(baseUrl, 'runner-bad-ver');
    const closed = nextClose(ws);
    send(ws, { ...helloFrame(), frameProtocolVersion: 2 });
    expect(await closed).toBe(4002);
    expect(registry.has('runner-bad-ver')).toBe(false);
  });

  it('closes with 4001 when the first frame is not a hello', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);
    const ws = await connect(baseUrl, 'runner-no-hello');
    const closed = nextClose(ws);
    send(ws, { kind: FrameKind.Ping, ts: 1 });
    expect(await closed).toBe(4001);
    expect(registry.has('runner-no-hello')).toBe(false);
  });
});

// ── Token-binding correlation gate ──────────────────────────

describe('api/runner-tunnel route — token binding', () => {
  it('rejects a token-bound tunnel that claims an arbitrary runner id (4004)', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);
    const token = 'bind-token';
    const boundId = tokenBoundRunnerId(token);
    expect(boundId).not.toBe(RUNNER_ID);
    const ws = await connect(baseUrl, RUNNER_ID, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: token },
    });
    const closed = nextClose(ws);
    expect(await closed).toBe(RUNNER_ID_MISMATCH_CLOSE_CODE);
    expect(registry.onlineRunnerIds()).toEqual([]);
  });

  it('accepts a tunnel whose path runner id matches the binding token', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);
    const token = 'bind-token';
    const runnerId = tokenBoundRunnerId(token);
    const ws = await connect(baseUrl, runnerId, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: token },
    });
    await sendHelloAndRegister(ws, registry, runnerId);
    expect(registry.onlineRunnerIds()).toEqual([runnerId]);
  });

  it('registers concurrent remote runner ids independently', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);

    const firstToken = 'bind-token-one';
    const firstRunnerId = tokenBoundRunnerId(firstToken);
    const first = await connect(baseUrl, firstRunnerId, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: firstToken },
    });
    await sendHelloAndRegister(first, registry, firstRunnerId);

    const secondToken = 'bind-token-two';
    const secondRunnerId = tokenBoundRunnerId(secondToken);
    const second = await connect(baseUrl, secondRunnerId, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: secondToken },
    });
    await sendHelloAndRegister(second, registry, secondRunnerId);

    expect(registry.onlineRunnerIds()).toEqual([firstRunnerId, secondRunnerId]);
  });

  it('rejects an empty token header (4004)', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);
    const ws = await connect(baseUrl, RUNNER_ID, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: '   ' },
    });
    const closed = nextClose(ws);
    expect(await closed).toBe(RUNNER_ID_MISMATCH_CLOSE_CODE);
    expect(registry.onlineRunnerIds()).toEqual([]);
  });
});

// ── Loopback detection ──────────────────────────────────────

describe('api/runner-tunnel route — loopback detection', () => {
  it('accepts an IPv4-mapped IPv6 loopback client as a local runner', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, { peerHost: '::ffff:127.0.0.1' });
    const ws = await connect(baseUrl, RUNNER_ID);
    await sendHelloAndRegister(ws, registry, RUNNER_ID);
    expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);
  });

  it.each(['10.1.2.3', '::ffff:10.1.2.3'])(
    'requires a binding token for the non-loopback client %s (4004)',
    async (peerHost) => {
      const registry = new TunnelRegistry();
      const { baseUrl } = await buildTunnelApp(registry, { peerHost });
      const ws = await connect(baseUrl, RUNNER_ID);
      const closed = nextClose(ws);
      expect(await closed).toBe(RUNNER_ID_MISMATCH_CLOSE_CODE);
      expect(registry.onlineRunnerIds()).toEqual([]);
    },
  );
});

// ── Token allow-list ────────────────────────────────────────

describe('api/runner-tunnel route — token allow-list', () => {
  it('requires a token for a remote client when the server has an allow-list (4004)', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, {
      allowedTunnelTokens: new Set(['current-token']),
      peerHost: '10.0.0.1',
    });
    const ws = await connect(baseUrl, RUNNER_ID);
    const closed = nextClose(ws);
    expect(await closed).toBe(RUNNER_ID_MISMATCH_CLOSE_CODE);
    expect(registry.onlineRunnerIds()).toEqual([]);
  });

  it('rejects a stale remote token (4004)', async () => {
    const registry = new TunnelRegistry();
    const staleToken = 'stale-token';
    const staleRunnerId = tokenBoundRunnerId(staleToken);
    const { baseUrl } = await buildTunnelApp(registry, {
      allowedTunnelTokens: new Set(['current-token']),
      peerHost: '10.0.0.1',
    });
    const ws = await connect(baseUrl, staleRunnerId, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: staleToken },
    });
    const closed = nextClose(ws);
    expect(await closed).toBe(RUNNER_ID_MISMATCH_CLOSE_CODE);
    expect(registry.onlineRunnerIds()).toEqual([]);
  });

  it('accepts a stable runner id with the current server token (remote)', async () => {
    const registry = new TunnelRegistry();
    const token = 'current-token';
    const runnerId = 'runner_local_stable';
    const { baseUrl } = await buildTunnelApp(registry, {
      allowedTunnelTokens: new Set([token]),
      peerHost: '10.0.0.1',
    });
    const ws = await connect(baseUrl, runnerId, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: token },
    });
    await sendHelloAndRegister(ws, registry, runnerId);
    expect(registry.onlineRunnerIds()).toEqual([runnerId]);
  });

  it('loopback clients bypass the allow-list entirely', async () => {
    const registry = new TunnelRegistry();
    const externalToken = 'external-runner-token';
    const externalRunnerId = tokenBoundRunnerId(externalToken);
    const { baseUrl } = await buildTunnelApp(registry, {
      allowedTunnelTokens: new Set(['server-own-token']),
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, externalRunnerId, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: externalToken },
    });
    await sendHelloAndRegister(ws, registry, externalRunnerId);
    expect(registry.onlineRunnerIds()).toEqual([externalRunnerId]);
  });
});

// ── Fail-closed on null owner (auth enabled, no allow-list) ──

describe('api/runner-tunnel route — fail closed on null owner', () => {
  it.each(['203.0.113.7', '::ffff:203.0.113.7'])(
    'rejects an unauthenticated non-loopback peer %s before accept (4004 unauthenticated)',
    async (peerHost) => {
      const registry = new TunnelRegistry();
      const { baseUrl } = await buildTunnelApp(registry, {
        authProvider: new CredentialHeaderAuthProvider(),
        peerHost,
      });
      const attackerToken = 'attacker-chosen-token';
      const derivedRunnerId = tokenBoundRunnerId(attackerToken);
      const ws = await connect(baseUrl, derivedRunnerId, {
        headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: attackerToken },
      });
      // The connection is refused before accept: the socket closes with the
      // mismatch close code and the reason "unauthenticated", and nothing
      // registers, so the owner-less runner is neither visible nor bindable.
      const code = await nextClose(ws);
      expect(code).toBe(RUNNER_ID_MISMATCH_CLOSE_CODE);
      expect(registry.onlineRunnerIds()).toEqual([]);
    },
  );

  it('registers an authenticated non-loopback runner under its owner', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, {
      authProvider: new CredentialHeaderAuthProvider(),
      peerHost: '203.0.113.7',
    });
    const token = 'alice-runner-token';
    const runnerId = tokenBoundRunnerId(token);
    const ws = await connect(baseUrl, runnerId, {
      headers: {
        [RUNNER_TUNNEL_TOKEN_HEADER]: token,
        'x-test-user': 'alice@example.com',
      },
    });
    await sendHelloAndRegister(ws, registry, runnerId);
    expect(registry.onlineRunnerIds()).toEqual([runnerId]);
    // Owner recorded as the authenticated caller (not undefined, not "local").
    expect(registry.runnerOwner(runnerId)).toBe('alice@example.com');
  });

  it('accepts the local loopback runner as the reserved local owner', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, {
      authProvider: new CredentialHeaderAuthProvider(),
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, RUNNER_ID);
    await sendHelloAndRegister(ws, registry, RUNNER_ID);
    expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);
    // Loopback + no credential → reserved local identity, so single-user
    // ownership checks stay coherent.
    expect(registry.runnerOwner(RUNNER_ID)).toBe(LOCAL_TUNNEL_OWNER);
  });
});

// ── CSWSH Origin guard ──────────────────────────────────────

describe('api/runner-tunnel route — CSWSH origin guard', () => {
  it('rejects a browser cross-origin handshake (4403) before accept', async () => {
    const registry = new TunnelRegistry();
    // Local mode (no auth provider): only loopback or the sentinel origin is
    // allowed. A hostile cross-origin page presents its own http origin.
    const { baseUrl } = await buildTunnelApp(registry, {
      localMode: true,
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, RUNNER_ID, { origin: 'https://evil.example.com' });
    const code = await nextClose(ws);
    expect(code).toBe(FORBIDDEN_ORIGIN_CLOSE_CODE);
    expect(registry.onlineRunnerIds()).toEqual([]);
  });

  it('allows the internal sentinel origin', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, { localMode: true, peerHost: '127.0.0.1' });
    const ws = await connect(baseUrl, RUNNER_ID, { origin: INTERNAL_WS_ORIGIN });
    await sendHelloAndRegister(ws, registry, RUNNER_ID);
    expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);
  });

  it('allows a loopback browser origin in local mode', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, { localMode: true, peerHost: '127.0.0.1' });
    const ws = await connect(baseUrl, RUNNER_ID, { origin: 'http://localhost:8000' });
    await sendHelloAndRegister(ws, registry, RUNNER_ID);
    expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);
  });

  it('allows a missing Origin (non-browser client)', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, { localMode: true, peerHost: '127.0.0.1' });
    const ws = await connect(baseUrl, RUNNER_ID, { origin: null });
    await sendHelloAndRegister(ws, registry, RUNNER_ID);
    expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);
  });

  it('honors an explicit extra-allowed origin in non-local mode', async () => {
    const registry = new TunnelRegistry();
    // Non-local mode, but with an explicit allow-list — anything not on it is
    // denied; the listed origin passes.
    const { baseUrl } = await buildTunnelApp(registry, {
      allowedOrigins: new Set(['https://app.example.com']),
      peerHost: '127.0.0.1',
    });
    const allowed = await connect(baseUrl, RUNNER_ID, { origin: 'https://app.example.com' });
    await sendHelloAndRegister(allowed, registry, RUNNER_ID);
    expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);
  });

  it('denies an unlisted origin when an extra-allowed list is configured (non-local)', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, {
      allowedOrigins: new Set(['https://app.example.com']),
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, RUNNER_ID, { origin: 'https://other.example.com' });
    const code = await nextClose(ws);
    expect(code).toBe(FORBIDDEN_ORIGIN_CLOSE_CODE);
    expect(registry.onlineRunnerIds()).toEqual([]);
  });

  it('passes any origin through in non-local mode without an allow-list', async () => {
    const registry = new TunnelRegistry();
    // Non-local (auth provider present), no allow-list: cookie/proxy auth is the
    // gate, so the origin policy is passthrough.
    const { baseUrl } = await buildTunnelApp(registry, {
      authProvider: new CredentialHeaderAuthProvider(),
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, RUNNER_ID, { origin: 'https://anything.example.com' });
    await sendHelloAndRegister(ws, registry, RUNNER_ID);
    expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);
  });
});

// ── Resilience + teardown ───────────────────────────────────

describe('api/runner-tunnel route — resilience + teardown', () => {
  it.each([
    ['bad json', 'not even json'],
    ['bad optional field', '{"kind":"response.head","id":"r","status":200,"headers":123}'],
  ])(
    'survives a malformed frame (%s) without deregistering the runner',
    async (_label, badFrame) => {
      const registry = new TunnelRegistry();
      const { baseUrl } = await buildTunnelApp(registry);
      const ws = await connect(baseUrl, RUNNER_ID);

      // Fake runner: respond to /health so we can prove routing still works after
      // the malformed frame is dropped.
      ws.on('message', (data: unknown) => {
        const frame = decodeFrame(typeof data === 'string' ? data : String(data));
        if (frame.kind !== FrameKind.Request) {
          return;
        }
        send(ws, { kind: FrameKind.ResponseHead, id: frame.id, status: 200 });
        send(ws, {
          kind: FrameKind.ResponseBody,
          id: frame.id,
          body: JSON.stringify({ status: 'ok' }),
          encoding: 'utf-8',
        });
        send(ws, { kind: FrameKind.ResponseEnd, id: frame.id });
      });

      await sendHelloAndRegister(ws, registry, RUNNER_ID);

      // Send the malformed frame directly (not via encodeFrame).
      ws.send(badFrame);

      // The runner stays online and routing still works.
      const session = registry.get(RUNNER_ID)!;
      const reqId = 'req-after-malformed';
      const state = registry.openRequest(RUNNER_ID, reqId);
      await registry.sendText(
        session,
        encodeFrame({ kind: FrameKind.Request, id: reqId, method: 'GET', path: '/health' }),
      );
      const head = await state.headFuture.promise;
      expect(head.status).toBe(200);
      for (;;) {
        const item = await state.bodyQueue.get();
        if (item === null) {
          break;
        }
      }
      expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);
    },
  );

  it('drops a binary frame without deregistering the runner', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);
    const ws = await connect(baseUrl, RUNNER_ID);
    await sendHelloAndRegister(ws, registry, RUNNER_ID);

    ws.send(Buffer.from([0xff, 0xfe]));
    // Give the server a tick to process (and ignore) the binary frame.
    await new Promise((r) => setTimeout(r, 30));
    expect(registry.onlineRunnerIds()).toEqual([RUNNER_ID]);
  });

  it('deregisters the runner and fires the disconnect hook when the socket closes', async () => {
    const registry = new TunnelRegistry();
    const disconnects: string[] = [];
    const { baseUrl } = await buildTunnelApp(registry, {
      onRunnerDisconnect: async (id) => {
        disconnects.push(id);
      },
    });
    const runnerId = 'runner-live-close';
    const ws = await connect(baseUrl, runnerId);
    await sendHelloAndRegister(ws, registry, runnerId);
    ws.close();
    await waitFor(() => !registry.has(runnerId));
    await waitFor(() => disconnects.includes(runnerId));
    expect(registry.has(runnerId)).toBe(false);
    expect(disconnects).toEqual([runnerId]);
  });

  it('fires the connect hook after registration', async () => {
    const registry = new TunnelRegistry();
    const connects: string[] = [];
    const { baseUrl } = await buildTunnelApp(registry, {
      onRunnerConnect: async (id) => {
        connects.push(id);
      },
    });
    const ws = await connect(baseUrl, RUNNER_ID);
    await sendHelloAndRegister(ws, registry, RUNNER_ID);
    await waitFor(() => connects.includes(RUNNER_ID));
    expect(connects).toEqual([RUNNER_ID]);
  });

  it('forwards the hello resume_cursors to the connect hook (incremental-resume handshake)', async () => {
    // The runner presents its per-session last-consumed cursors in the hello; the
    // route must read them off the hello frame and pass them to onRunnerConnect so
    // the owner pod serves an incremental after={cursor} replay for the bound
    // session. (The selection of the bound session's cursor lives in the bridge
    // manager; the route's job is to thread the whole map through.)
    const registry = new TunnelRegistry();
    let seen: Readonly<Record<string, string>> | undefined;
    const { baseUrl } = await buildTunnelApp(registry, {
      onRunnerConnect: async (_id, resumeCursors) => {
        seen = resumeCursors;
      },
    });
    const ws = await connect(baseUrl, RUNNER_ID);
    await sendHelloAndRegister(ws, registry, RUNNER_ID, {
      resumeCursors: { ses_one: 'evt_42', ses_two: 'evt_7' },
    });
    await waitFor(() => seen !== undefined);
    expect(seen).toEqual({ ses_one: 'evt_42', ses_two: 'evt_7' });
  });

  it('passes an empty resume_cursors map to the connect hook for a fresh runner (no cursors)', async () => {
    // A fresh runner presents no cursors; the route defaults to {} so recovery
    // serves a full replay. Proves the no-cursor path threads a usable empty map.
    const registry = new TunnelRegistry();
    let seen: Readonly<Record<string, string>> | undefined;
    const { baseUrl } = await buildTunnelApp(registry, {
      onRunnerConnect: async (_id, resumeCursors) => {
        seen = resumeCursors;
      },
    });
    const ws = await connect(baseUrl, RUNNER_ID);
    // helloFrame() carries no resumeCursors → the route must still pass {} (the
    // decoder defaults the absent wire field to an empty map).
    await sendHelloAndRegister(ws, registry, RUNNER_ID);
    await waitFor(() => seen !== undefined);
    expect(seen).toEqual({});
  });

  it('a hung connect hook does not stall teardown — disconnect still fires on close', async () => {
    // Hang-safety guarantee: the connect hook await is bounded by a timeout so a
    // slow / hung hook can't stall WS shutdown. Here the hook NEVER resolves.
    // Without the timeout bound the
    // handler would park forever on the hook await and never reach its loop race
    // / teardown `finally`, so closing the socket would never fire the disconnect
    // hook. With the bound, the handler proceeds after the (short, test-tuned)
    // timeout, runs the race, and tears down on close.
    const registry = new TunnelRegistry();
    const disconnects: string[] = [];
    let hookEntered = false;
    const { baseUrl } = await buildTunnelApp(registry, {
      // Never resolves: models a hung hook.
      onRunnerConnect: () => {
        hookEntered = true;
        return new Promise<void>(() => {});
      },
      onRunnerConnectTimeoutMs: 40,
      onRunnerDisconnect: async (id) => {
        disconnects.push(id);
      },
    });
    const runnerId = 'runner-hung-connect-hook';
    const ws = await connect(baseUrl, runnerId);

    // The session registers (registration happens before the hook fires)…
    await sendHelloAndRegister(ws, registry, runnerId);
    await waitFor(() => hookEntered);
    expect(registry.has(runnerId)).toBe(true);

    // …and after the connect-hook timeout elapses the handler is back in its main
    // race, so a close tears the session down and fires the disconnect hook —
    // proving the hung hook never blocked teardown.
    ws.close();
    await waitFor(() => !registry.has(runnerId));
    await waitFor(() => disconnects.includes(runnerId));
    expect(disconnects).toEqual([runnerId]);
  });

  it('a connect hook that overruns then errors does not surface an unhandled rejection', async () => {
    // The timeout path must also swallow a *late* rejection from the overran hook
    // (the handler has already moved on), so it can't bubble up as an unhandled
    // rejection after the deadline fired. A late-rejecting hook here would crash
    // the process on an unhandled rejection if the timeout helper didn't attach a
    // catch to the detached promise.
    const registry = new TunnelRegistry();
    const rejections: unknown[] = [];
    const onUnhandled = (err: unknown): void => {
      rejections.push(err);
    };
    process.on('unhandledRejection', onUnhandled);
    try {
      const { baseUrl } = await buildTunnelApp(registry, {
        onRunnerConnect: () =>
          new Promise<void>((_resolve, reject) => {
            // Reject AFTER the (short) connect timeout has already elapsed.
            setTimeout(() => reject(new Error('hook failed late')), 60);
          }),
        onRunnerConnectTimeoutMs: 20,
      });
      const runnerId = 'runner-late-reject-hook';
      const ws = await connect(baseUrl, runnerId);
      await sendHelloAndRegister(ws, registry, runnerId);
      // Wait past the late rejection so any unhandled rejection would have fired.
      await new Promise((r) => setTimeout(r, 120));
      // The runner is unaffected by the hook outcome.
      expect(registry.has(runnerId)).toBe(true);
      expect(rejections).toEqual([]);
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });

  it('newest-wins: a second tunnel for the same runner replaces the first session', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);
    const runnerId = 'runner-live-replace';
    const wsA = await connect(baseUrl, runnerId);
    await sendHelloAndRegister(wsA, registry, runnerId);
    const first = registry.get(runnerId)!;

    const wsB = await connect(baseUrl, runnerId);
    send(wsB, helloFrame());
    await waitFor(() => registry.get(runnerId) !== undefined && registry.get(runnerId) !== first);
    expect(registry.get(runnerId)).not.toBe(first);
  });
});

// ── HTTP control endpoints (list + status) ──────────────────
//
// These exercise the real Fastify HTTP routing layer over the same in-process
// app the WS tests use: a plain `fetch` against the listening loopback port hits
// `GET /internal/runners` / `/internal/runners/:id/status`, so the 401 path, the
// owner-scoped listing filter, the cross-tenant enumeration-hiding, and the
// exit-report surfacing all run end-to-end (not just through the registry).

/** GET a JSON endpoint on the app under test; returns the status + parsed body. */
async function httpGet(
  baseUrl: string,
  path: string,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: unknown }> {
  const httpBase = baseUrl.replace(/^ws:/, 'http:');
  const res = await fetch(`${httpBase}${path}`, { headers });
  const text = await res.text();
  return { status: res.status, body: text.length > 0 ? JSON.parse(text) : undefined };
}

describe('api/runner-tunnel route — GET /internal/runners (list)', () => {
  it('reports online runners + advertised harnesses after registration', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);

    const before = await httpGet(baseUrl, '/internal/runners');
    expect(before).toEqual({ status: 200, body: { data: [] } });

    const ws = await connect(baseUrl, RUNNER_ID);
    await sendHelloAndRegister(ws, registry, RUNNER_ID);

    const after = await httpGet(baseUrl, '/internal/runners');
    expect(after).toEqual({
      status: 200,
      body: { data: [{ runner_id: RUNNER_ID, online: true, harnesses: ['claude-sdk'] }] },
    });
  });

  it('401s an unauthenticated caller when an auth provider is configured', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, {
      authProvider: new CredentialHeaderAuthProvider(),
      // Loopback so the runner itself registers (as the reserved local owner);
      // the HTTP caller below still presents no credential header → 401.
      peerHost: '127.0.0.1',
    });
    const ws = await connect(baseUrl, RUNNER_ID);
    await sendHelloAndRegister(ws, registry, RUNNER_ID);

    const res = await httpGet(baseUrl, '/internal/runners');
    expect(res.status).toBe(401);
    expect(res.body).toEqual({ error: 'unauthenticated' });
  });

  it('scopes the listing to the caller — another user sees none of it', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, {
      authProvider: new CredentialHeaderAuthProvider(),
      peerHost: '203.0.113.7',
    });
    // Alice's authenticated remote runner registers under her identity.
    const token = 'alice-runner-token';
    const runnerId = tokenBoundRunnerId(token);
    const ws = await connect(baseUrl, runnerId, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: token, 'x-test-user': 'alice@example.com' },
    });
    await sendHelloAndRegister(ws, registry, runnerId);

    const alice = await httpGet(baseUrl, '/internal/runners', {
      'x-test-user': 'alice@example.com',
    });
    expect(alice).toEqual({
      status: 200,
      body: { data: [{ runner_id: runnerId, online: true, harnesses: ['claude-sdk'] }] },
    });

    // A different authenticated user must not see Alice's runner.
    const bob = await httpGet(baseUrl, '/internal/runners', { 'x-test-user': 'bob@example.com' });
    expect(bob).toEqual({ status: 200, body: { data: [] } });
  });
});

describe('api/runner-tunnel route — GET /internal/runners/:id/status', () => {
  it('flips a runner from offline to online across registration', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);

    const offline = await httpGet(baseUrl, `/internal/runners/${RUNNER_ID}/status`);
    expect(offline).toEqual({ status: 200, body: { runner_id: RUNNER_ID, online: false } });

    const ws = await connect(baseUrl, RUNNER_ID);
    await sendHelloAndRegister(ws, registry, RUNNER_ID);

    const online = await httpGet(baseUrl, `/internal/runners/${RUNNER_ID}/status`);
    expect(online).toEqual({ status: 200, body: { runner_id: RUNNER_ID, online: true } });
  });

  it('401s an unauthenticated caller when an auth provider is configured', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, {
      authProvider: new CredentialHeaderAuthProvider(),
    });
    const res = await httpGet(baseUrl, `/internal/runners/${RUNNER_ID}/status`);
    expect(res.status).toBe(401);
    expect(res.body).toEqual({ error: 'unauthenticated' });
  });

  it("hides another user's runner: reports online:false to a different tenant", async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry, {
      authProvider: new CredentialHeaderAuthProvider(),
      peerHost: '203.0.113.7',
    });
    const token = 'alice-runner-token';
    const runnerId = tokenBoundRunnerId(token);
    const ws = await connect(baseUrl, runnerId, {
      headers: { [RUNNER_TUNNEL_TOKEN_HEADER]: token, 'x-test-user': 'alice@example.com' },
    });
    await sendHelloAndRegister(ws, registry, runnerId);

    // Owner sees it online…
    const alice = await httpGet(baseUrl, `/internal/runners/${runnerId}/status`, {
      'x-test-user': 'alice@example.com',
    });
    expect(alice).toEqual({ status: 200, body: { runner_id: runnerId, online: true } });

    // …a different user sees it as offline (enumeration-hiding).
    const bob = await httpGet(baseUrl, `/internal/runners/${runnerId}/status`, {
      'x-test-user': 'bob@example.com',
    });
    expect(bob).toEqual({ status: 200, body: { runner_id: runnerId, online: false } });
  });

  it('surfaces an offline runner exit cause (owner-scoped) when exit reports are wired', async () => {
    const registry = new TunnelRegistry();
    const exitReports = new RunnerExitReports();
    const deadRunner = 'runner_dead_1';
    exitReports.record(deadRunner, 'runner process exited with code 1', {
      workerId: 'wc_1',
      owner: 'alice@example.com',
    });

    const { baseUrl } = await buildTunnelApp(registry, {
      authProvider: new CredentialHeaderAuthProvider(),
      runnerExitReports: exitReports,
    });

    // Owner: offline + the failure cause so the client fails fast.
    const owner = await httpGet(baseUrl, `/internal/runners/${deadRunner}/status`, {
      'x-test-user': 'alice@example.com',
    });
    expect(owner).toEqual({
      status: 200,
      body: {
        runner_id: deadRunner,
        online: false,
        error: 'runner process exited with code 1',
      },
    });

    // Another user: offline, but the exit cause is withheld (owner-scoped).
    const other = await httpGet(baseUrl, `/internal/runners/${deadRunner}/status`, {
      'x-test-user': 'bob@example.com',
    });
    expect(other).toEqual({ status: 200, body: { runner_id: deadRunner, online: false } });
  });

  it('omits the exit cause when no exit-report store is configured', async () => {
    const registry = new TunnelRegistry();
    const { baseUrl } = await buildTunnelApp(registry);
    const res = await httpGet(baseUrl, '/internal/runners/runner_unknown/status');
    expect(res).toEqual({ status: 200, body: { runner_id: 'runner_unknown', online: false } });
  });
});

// ── Disconnect hook is fired exactly once on every close path ──
//
// A naive teardown can double-fire the disconnect hook on the exception-disconnect
// path (an inner `finally` firing once, then a re-raised disconnect error caught
// by the outer handler firing it again). This route fires exactly once on every
// path — the contract its callers rely on — and these tests pin that so a future
// refactor cannot silently reintroduce the double-fire.

describe('api/runner-tunnel route — disconnect hook fires exactly once', () => {
  it('fires once when the client closes the socket cleanly', async () => {
    const registry = new TunnelRegistry();
    const disconnects: string[] = [];
    const { baseUrl } = await buildTunnelApp(registry, {
      onRunnerDisconnect: async (id) => {
        disconnects.push(id);
      },
    });
    const runnerId = 'runner-once-clean';
    const ws = await connect(baseUrl, runnerId);
    await sendHelloAndRegister(ws, registry, runnerId);

    ws.close();
    await waitFor(() => !registry.has(runnerId));
    await waitFor(() => disconnects.length > 0);
    // Settle any (incorrect) second fire before asserting.
    await new Promise((r) => setTimeout(r, 50));
    expect(disconnects).toEqual([runnerId]);
  });

  it('fires once when the socket is abruptly terminated (error path)', async () => {
    const registry = new TunnelRegistry();
    const disconnects: string[] = [];
    const { baseUrl } = await buildTunnelApp(registry, {
      onRunnerDisconnect: async (id) => {
        disconnects.push(id);
      },
    });
    const runnerId = 'runner-once-terminate';
    const ws = await connect(baseUrl, runnerId);
    await sendHelloAndRegister(ws, registry, runnerId);

    // `terminate()` drops the TCP connection without a close handshake, driving
    // the socket 'error'/'close' path rather than a clean close frame.
    ws.terminate();
    await waitFor(() => !registry.has(runnerId));
    await waitFor(() => disconnects.length > 0);
    await new Promise((r) => setTimeout(r, 50));
    expect(disconnects).toEqual([runnerId]);
  });

  it('fires once when a newer tunnel replaces the session, then both close', async () => {
    const registry = new TunnelRegistry();
    const disconnects: string[] = [];
    const { baseUrl } = await buildTunnelApp(registry, {
      onRunnerDisconnect: async (id) => {
        disconnects.push(id);
      },
    });
    const runnerId = 'runner-once-replace';
    const wsA = await connect(baseUrl, runnerId);
    await sendHelloAndRegister(wsA, registry, runnerId);
    const first = registry.get(runnerId)!;

    // Newest-wins: a second tunnel replaces the first session. The first
    // handler tears down (its session is no longer current) and must fire its
    // hook exactly once.
    const wsB = await connect(baseUrl, runnerId);
    send(wsB, helloFrame());
    await waitFor(() => registry.get(runnerId) !== undefined && registry.get(runnerId) !== first);
    await waitFor(() => disconnects.length >= 1);

    wsB.close();
    await waitFor(() => !registry.has(runnerId));
    await waitFor(() => disconnects.length >= 2);
    await new Promise((r) => setTimeout(r, 50));
    // One fire for the replaced tunnel, one for the final close — exactly two,
    // never three or four (which a per-close-event double-fire would produce).
    expect(disconnects).toEqual([runnerId, runnerId]);
  });
});
