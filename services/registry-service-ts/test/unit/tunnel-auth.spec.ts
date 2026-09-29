// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Spec for the public runner-tunnel auth posture derived from config
// (`src/auth/tunnel-auth.ts`) and its end-to-end effect on the live route.
//
// The critical property under test is the one the exposure work has to
// guarantee: the runner tunnel is mounted on the PUBLIC `/v1/tunnels/*` path and
// the listener binds `0.0.0.0`, so the wiring derived from the SHIPPED DEFAULT
// config must fail closed for a remote peer while still admitting a loopback
// runner. We prove that against the real Fastify WS route (a real `ws` client),
// using exactly the options `buildRunnerTunnelAuth` produces — so a regression in
// the config→route wiring (the gap where the guards existed but were never wired)
// is caught here, not just in a hand-built fixture.

import { describe, it, expect, afterEach } from 'vitest';
import Fastify, { type FastifyInstance } from 'fastify';
import websocket from '@fastify/websocket';
import { WebSocket } from 'ws';
import {
  TUNNEL_MAX_MESSAGE_BYTES,
  INTERNAL_WS_ORIGIN,
  FrameKind,
  encodeFrame,
  tokenBoundRunnerId,
  RUNNER_TUNNEL_TOKEN_HEADER,
  type HelloFrame,
} from '@orca/harness-tunnel';
import { TunnelRegistry } from '../../src/tunnel/tunnel-registry.js';
import {
  registerRunnerTunnelRoutes,
  LOCAL_TUNNEL_OWNER,
  RUNNER_ID_MISMATCH_CLOSE_CODE,
} from '../../src/api/runner-tunnel.routes.js';
import {
  buildRunnerTunnelAuth,
  LoopbackOnlyTunnelAuthProvider,
  AllowlistTunnelAuthProvider,
  type RunnerTunnelAuthConfig,
} from '../../src/auth/tunnel-auth.js';

// ── buildRunnerTunnelAuth (pure) ─────────────────────────────

function cfg(over: Partial<RunnerTunnelAuthConfig> = {}): RunnerTunnelAuthConfig {
  return {
    runnerTunnelTokens: new Set<string>(),
    runnerTunnelAllowedOrigins: new Set<string>(),
    runnerTunnelLocalMode: true,
    ...over,
  };
}

describe('buildRunnerTunnelAuth', () => {
  it('with no allow-list: wires a loopback-only provider and omits allowedTunnelTokens', () => {
    const wiring = buildRunnerTunnelAuth(cfg());
    expect(wiring.authProvider).toBeInstanceOf(LoopbackOnlyTunnelAuthProvider);
    // Omitted (not an empty set) so the route's loopback-only default is selected
    // by the provider, not turned into "remote token required" by an empty list.
    expect(wiring.allowedTunnelTokens).toBeUndefined();
    expect(wiring.localMode).toBe(true);
  });

  it('with an allow-list: forwards the tokens AND keeps the route out of single-user mode', () => {
    const tokens = new Set(['tok-a', 'tok-b']);
    const wiring = buildRunnerTunnelAuth(cfg({ runnerTunnelTokens: tokens }));
    expect(wiring.authProvider).toBeInstanceOf(AllowlistTunnelAuthProvider);
    expect(wiring.allowedTunnelTokens).toBe(tokens);
  });

  it('passes the configured origins + localMode through verbatim', () => {
    const origins = new Set(['https://app.example.com']);
    const wiring = buildRunnerTunnelAuth(
      cfg({ runnerTunnelAllowedOrigins: origins, runnerTunnelLocalMode: false }),
    );
    expect(wiring.allowedOrigins).toBe(origins);
    expect(wiring.localMode).toBe(false);
  });
});

describe('LoopbackOnlyTunnelAuthProvider', () => {
  const provider = new LoopbackOnlyTunnelAuthProvider();
  const req = (remoteAddress: string | undefined): Parameters<typeof provider.getUserId>[0] =>
    ({ socket: { remoteAddress } }) as Parameters<typeof provider.getUserId>[0];

  it('returns the reserved local owner for loopback peers', () => {
    expect(provider.getUserId(req('127.0.0.1'))).toBe(LOCAL_TUNNEL_OWNER);
    expect(provider.getUserId(req('::1'))).toBe(LOCAL_TUNNEL_OWNER);
    expect(provider.getUserId(req('::ffff:127.0.0.1'))).toBe(LOCAL_TUNNEL_OWNER);
  });

  it('returns null for remote peers (so the route fail-closed gate refuses them)', () => {
    expect(provider.getUserId(req('203.0.113.9'))).toBeNull();
    expect(provider.getUserId(req('10.0.0.4'))).toBeNull();
    expect(provider.getUserId(req('::ffff:203.0.113.9'))).toBeNull();
    expect(provider.getUserId(req(undefined))).toBeNull();
  });
});

// ── End-to-end: the shipped-default wiring fails closed ──────

const openApps: FastifyInstance[] = [];
const openSockets: WebSocket[] = [];

afterEach(async () => {
  for (const ws of openSockets.splice(0)) {
    try {
      ws.terminate();
    } catch {
      /* ignore */
    }
  }
  for (const app of openApps.splice(0)) {
    await app.close();
  }
});

function helloFrame(): HelloFrame {
  return {
    kind: FrameKind.Hello,
    runnerVersion: '0.1.0-test',
    frameProtocolVersion: 1,
    harnesses: ['claude-sdk'],
    envs: ['os_sandbox'],
  };
}

/**
 * Mount the route with the options `buildRunnerTunnelAuth(config)` produces, the
 * exact spread `main.ts`/`buildApp` use. `peerHost` overrides the route's
 * loopback decision (its production `resolvePeerHost` seam) so the remote branch
 * can be exercised from a real `127.0.0.1` TCP client.
 */
async function buildConfiguredApp(
  registry: TunnelRegistry,
  config: RunnerTunnelAuthConfig,
  peerHost: string,
): Promise<string> {
  // Production passes ONE peer-host resolver to both the route and the
  // loopback-only auth provider (so their loopback decisions agree). The test
  // does the same: thread the simulated `peerHost` through the config so the
  // provider sees it too, exactly as a proxied deployment would.
  const resolvePeerHost = (): string => peerHost;
  const wiring = buildRunnerTunnelAuth({ ...config, resolvePeerHost });
  const app = Fastify({ logger: false });
  openApps.push(app);
  await app.register(websocket, { options: { maxPayload: TUNNEL_MAX_MESSAGE_BYTES } });
  registerRunnerTunnelRoutes(app, {
    registry,
    authProvider: wiring.authProvider,
    ...(wiring.allowedTunnelTokens !== undefined
      ? { allowedTunnelTokens: wiring.allowedTunnelTokens }
      : {}),
    allowedOrigins: wiring.allowedOrigins,
    localMode: wiring.localMode,
    resolvePeerHost,
  });
  await app.listen({ host: '127.0.0.1', port: 0 });
  const addr = app.server.address();
  if (addr === null || typeof addr === 'string') {
    throw new Error('expected a TCP address');
  }
  return `ws://127.0.0.1:${addr.port}`;
}

async function connect(
  baseUrl: string,
  runnerId: string,
  headers: Record<string, string> = {},
): Promise<WebSocket> {
  const ws = new WebSocket(`${baseUrl}/v1/tunnels/runners/${runnerId}`, {
    headers: { origin: INTERNAL_WS_ORIGIN, ...headers },
  });
  openSockets.push(ws);
  ws.on('error', () => {
    /* close-code assertions read 'close' */
  });
  await new Promise<void>((resolve) => {
    ws.once('open', () => resolve());
    ws.once('close', () => resolve());
  });
  return ws;
}

function nextClose(ws: WebSocket): Promise<number> {
  return new Promise<number>((resolve) => ws.once('close', (code: number) => resolve(code)));
}

async function waitFor(predicate: () => boolean, timeoutMs = 1000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 5));
  }
}

describe('runner tunnel — shipped default config fails closed for remote peers', () => {
  it('refuses a remote attacker who derives a runner id from any chosen token (4004)', async () => {
    // Default config: no RUNNER_TUNNEL_TOKENS. Pre-fix, this peer registered
    // owner-less because the owner fail-closed gate was unreachable (no provider
    // was ever wired). With the loopback-only provider wired from config, the
    // gate now refuses the dial before accept.
    const registry = new TunnelRegistry();
    const baseUrl = await buildConfiguredApp(registry, cfg(), '203.0.113.9');
    const attackerToken = 'attacker-chosen-token';
    const derivedId = tokenBoundRunnerId(attackerToken);
    const ws = await connect(baseUrl, derivedId, {
      [RUNNER_TUNNEL_TOKEN_HEADER]: attackerToken,
    });
    expect(await nextClose(ws)).toBe(RUNNER_ID_MISMATCH_CLOSE_CODE);
    expect(registry.onlineRunnerIds()).toEqual([]);
  });

  it('still admits the local loopback runner as the reserved local owner', async () => {
    const registry = new TunnelRegistry();
    const baseUrl = await buildConfiguredApp(registry, cfg(), '127.0.0.1');
    const runnerId = 'runner_local_default';
    const ws = await connect(baseUrl, runnerId);
    ws.send(encodeFrame(helloFrame()));
    await waitFor(() => registry.has(runnerId));
    expect(registry.runnerOwner(runnerId)).toBe(LOCAL_TUNNEL_OWNER);
  });
});

describe('runner tunnel — allow-list config admits provisioned remote runners', () => {
  it('registers an allow-listed remote runner and refuses a non-allow-listed token (4004)', async () => {
    const registry = new TunnelRegistry();
    const config = cfg({ runnerTunnelTokens: new Set(['provisioned-token']) });
    const baseUrl = await buildConfiguredApp(registry, config, '203.0.113.9');

    // Allow-listed token: the remote runner uses a STABLE id (allow-list mode
    // does not require a token-derived id) and registers.
    const runnerId = 'runner_remote_stable';
    const ok = await connect(baseUrl, runnerId, {
      [RUNNER_TUNNEL_TOKEN_HEADER]: 'provisioned-token',
    });
    ok.send(encodeFrame(helloFrame()));
    await waitFor(() => registry.has(runnerId));
    // Out of single-user mode: the operator owner is recorded (not undefined), so
    // the owner-scoped guards stay enforceable.
    expect(registry.runnerOwner(runnerId)).toBe(LOCAL_TUNNEL_OWNER);

    // A token NOT in the allow-list is refused by the in-handler binding gate.
    const bad = await connect(baseUrl, 'runner_intruder', {
      [RUNNER_TUNNEL_TOKEN_HEADER]: 'not-provisioned',
    });
    expect(await nextClose(bad)).toBe(RUNNER_ID_MISMATCH_CLOSE_CODE);
    expect(registry.onlineRunnerIds()).toEqual([runnerId]);
  });
});
