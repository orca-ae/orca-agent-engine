// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// In-process spec for the registry's client-facing terminal-attach route + the
// tunnel WS-channel client it proxies over.
//
// The registry exposes a client-facing WebSocket at
// `/v1/sessions/:sessionId/terminals/:terminalId/attach`. It resolves the
// session's runner, opens a TUNNELED WS channel to that runner (reusing the
// TunnelRegistry's ws-channel bookkeeping — `openWsChannel` / `routeWsInbound` /
// the session outbound queue), and PROXIES frames between the client socket and
// the runner:
//   - client → runner: client binary/text frames become tunnel `ws.frame`s;
//   - runner → client: tunnel `ws.frame`s become client binary/text frames;
//   - either close tears the channel down.
//
// No real runner + no session-runner dependency: a FAKE runner session is
// registered directly in the TunnelRegistry. Its socket adapter captures the
// registry's outbound frames (the sender loop the real route runs is replaced by
// draining that adapter here) and answers `ws.open` by ECHOING each `ws.frame`
// back through `registry.routeWsInbound` — exactly the wire the runner pty-bridge
// speaks — so the proxy is exercised end-to-end against the registry's own
// machinery.

import { describe, it, expect, afterEach } from 'vitest';
import Fastify, { type FastifyInstance } from 'fastify';
import websocket from '@fastify/websocket';
import { WebSocket } from 'ws';
import {
  FrameKind,
  RUNNER_TERMINAL_ATTACH_PATH,
  TUNNEL_MAX_MESSAGE_BYTES,
  decodeFrame,
  type Frame,
  type HelloFrame,
} from '@orca/harness-tunnel';
import {
  TunnelRegistry,
  type RegistrySession,
  type RegistryWebSocketLike,
} from '../../src/tunnel/tunnel-registry.js';
import { openTunnelWsChannel } from '../../src/tunnel/tunnel-ws-channel.js';
import {
  RUNNER_TERMINAL_ATTACH_PATH as ROUTE_RUNNER_TERMINAL_ATTACH_PATH,
  registerTerminalAttachRoutes,
} from '../../src/api/terminal-attach.routes.js';

const RUNNER_ID = 'runner-attach-1';
const SESSION_ID = 'ses_attach_1';
const TERMINAL_ID = 't1';

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
 * A fake runner-tunnel socket the registry writes to. It DECODES each outbound
 * frame the registry enqueues (drained here in place of the route's sender loop)
 * and, for a `ws.open` on the terminal-attach path, ECHOES back each subsequent
 * `ws.frame` on that channel via {@link routeInbound} — mimicking a `cat` pane:
 * bytes in, same bytes back. It also records the frames it saw for assertions.
 */
class EchoRunnerSocket implements RegistryWebSocketLike {
  readonly sent: Frame[] = [];
  private readonly openChannels = new Set<string>();
  private routeInbound: ((frame: Frame) => void) | undefined;

  /** Wire the inbound router (the registry's `routeWsInbound` bound to this session). */
  bindInbound(route: (frame: Frame) => void): void {
    this.routeInbound = route;
  }

  async sendText(data: string): Promise<void> {
    const frame = decodeFrame(data);
    this.sent.push(frame);
    if (frame.kind === FrameKind.WsOpen) {
      this.openChannels.add(frame.chId);
      return;
    }
    if (frame.kind === FrameKind.WsFrame && this.openChannels.has(frame.chId)) {
      // Echo the payload straight back on the same channel, preserving encoding.
      this.routeInbound?.({
        kind: FrameKind.WsFrame,
        chId: frame.chId,
        data: frame.data,
        encoding: frame.encoding ?? 'utf-8',
      });
      return;
    }
    if (frame.kind === FrameKind.WsClose) {
      this.openChannels.delete(frame.chId);
    }
  }

  receiveText(): Promise<string> {
    return new Promise<string>(() => {});
  }

  close(): void {
    /* no-op for the fake */
  }
}

/**
 * Register a fake runner session in `registry` whose socket echoes ws.frames.
 * Starts a tiny drain loop that pulls the session's outbound queue and feeds the
 * EchoRunnerSocket (standing in for the real route's sender loop), and binds the
 * socket's inbound router to `registry.routeWsInbound` for this session.
 */
function registerEchoRunner(registry: TunnelRegistry): {
  session: RegistrySession;
  stop: () => void;
} {
  const socket = new EchoRunnerSocket();
  const session = registry.register(RUNNER_ID, socket, helloFrame());
  socket.bindInbound((frame) => {
    registry.routeWsInbound(RUNNER_ID, frame, session);
  });
  let stopped = false;
  void (async () => {
    while (!stopped) {
      const data = await session.outboundQueue.get();
      if (data === null || stopped) {
        return;
      }
      await socket.sendText(data);
    }
  })();
  return { session, stop: () => (stopped = true) };
}

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

// ── The runner-attach wire path is the shared tunnel contract ──

describe('terminal-attach wire path (shared runner ⇄ registry contract)', () => {
  it('re-exports the shared @orca/harness-tunnel wire-path symbol', () => {
    // The registry proxy opens the tunnel channel on this exact base path; the
    // runner registers its pty-bridge on the same shared symbol, so the contract is
    // one literal rather than two copies kept in sync by convention.
    expect(ROUTE_RUNNER_TERMINAL_ATTACH_PATH).toBe(RUNNER_TERMINAL_ATTACH_PATH);
    expect(ROUTE_RUNNER_TERMINAL_ATTACH_PATH).toBe('/v1/runner/terminal/attach');
  });
});

// ── The tunnel WS-channel client (registry → runner) ──

describe('openTunnelWsChannel (registry-side tunnel WS-channel client)', () => {
  it('opens a channel, sends bytes, and reads the runner echo back', async () => {
    const registry = new TunnelRegistry();
    const { stop } = registerEchoRunner(registry);
    try {
      const channel = openTunnelWsChannel({
        registry,
        runnerId: RUNNER_ID,
        path: `/v1/runner/terminal/attach/${TERMINAL_ID}`,
      });
      const it = channel.messages()[Symbol.asyncIterator]();

      await channel.sendBytes(Uint8Array.from([1, 2, 3]));
      const first = await it.next();
      expect(first.done).toBe(false);
      expect(first.value).toEqual({ kind: 'bytes', data: Uint8Array.from([1, 2, 3]) });

      await channel.sendText('hello');
      const second = await it.next();
      expect(second.value).toEqual({ kind: 'text', data: 'hello' });

      await channel.close(1000, 'bye');
    } finally {
      stop();
    }
  });

  it('throws when the target runner is offline', () => {
    const registry = new TunnelRegistry();
    expect(() => openTunnelWsChannel({ registry, runnerId: 'nobody', path: '/x' })).toThrow();
  });

  it('surfaces a runner-initiated close through the message iterator', async () => {
    const registry = new TunnelRegistry();
    const socket = new EchoRunnerSocket();
    const session = registry.register(RUNNER_ID, socket, helloFrame());
    const channel = openTunnelWsChannel({ registry, runnerId: RUNNER_ID, path: '/x' });
    const it = channel.messages()[Symbol.asyncIterator]();
    // The runner closes its side of the channel.
    registry.routeWsInbound(
      RUNNER_ID,
      { kind: FrameKind.WsClose, chId: channel.chId, code: 4001, reason: 'done' },
      session,
    );
    const item = await it.next();
    expect(item.value).toEqual({ kind: 'close', code: 4001, reason: 'done' });
  });
});

// ── The client-facing proxy route (client WS ↔ registry ↔ runner) ──

describe('api/terminal-attach route — client WS proxied to the runner pty-bridge', () => {
  async function buildApp(
    registry: TunnelRegistry,
    resolveRunnerId: (sessionId: string) => Promise<string | null>,
  ): Promise<string> {
    // The route passes (sessionId, req); the tests only key on sessionId, so wrap
    // to drop the request argument.
    const app = Fastify({ logger: false });
    openApps.push(app);
    await app.register(websocket, { options: { maxPayload: TUNNEL_MAX_MESSAGE_BYTES } });
    registerTerminalAttachRoutes(app, {
      registry,
      resolveRunnerId: (sessionId) => resolveRunnerId(sessionId),
    });
    await app.listen({ host: '127.0.0.1', port: 0 });
    const addr = app.server.address();
    if (addr === null || typeof addr === 'string') {
      throw new Error('expected a TCP address');
    }
    return `ws://127.0.0.1:${addr.port}`;
  }

  function connect(url: string): Promise<WebSocket> {
    const ws = new WebSocket(url);
    openSockets.push(ws);
    ws.on('error', () => {
      /* close-code assertions read the 'close' event */
    });
    return new Promise<WebSocket>((resolve) => {
      ws.once('open', () => resolve(ws));
      ws.once('close', () => resolve(ws));
    });
  }

  it('round-trips client bytes through the tunnel to the echoing runner', async () => {
    const registry = new TunnelRegistry();
    const { stop } = registerEchoRunner(registry);
    try {
      const base = await buildApp(registry, async (id) => (id === SESSION_ID ? RUNNER_ID : null));
      const ws = await connect(`${base}/v1/sessions/${SESSION_ID}/terminals/${TERMINAL_ID}/attach`);
      expect(ws.readyState).toBe(WebSocket.OPEN);

      const echoed = new Promise<Buffer>((resolve) => {
        ws.on('message', (data: Buffer, isBinary: boolean) => {
          if (isBinary) {
            resolve(data);
          }
        });
      });
      ws.send(Buffer.from('client-bytes-77'), { binary: true });
      const back = await echoed;
      expect(back.toString('utf8')).toBe('client-bytes-77');

      ws.close();
    } finally {
      stop();
    }
  });

  it('forwards a resize control (text) to the runner as a ws.frame', async () => {
    const registry = new TunnelRegistry();
    const socket = new EchoRunnerSocket();
    const session = registry.register(RUNNER_ID, socket, helloFrame());
    socket.bindInbound((frame) => registry.routeWsInbound(RUNNER_ID, frame, session));
    let stopped = false;
    void (async () => {
      while (!stopped) {
        const data = await session.outboundQueue.get();
        if (data === null || stopped) return;
        await socket.sendText(data);
      }
    })();
    try {
      const base = await buildApp(registry, async () => RUNNER_ID);
      const ws = await connect(`${base}/v1/sessions/${SESSION_ID}/terminals/${TERMINAL_ID}/attach`);
      const resize = JSON.stringify({ type: 'resize', cols: 120, rows: 40 });
      ws.send(resize); // text frame
      // Wait until the runner socket saw a ws.frame carrying the resize control.
      const deadline = Date.now() + 2000;
      while (
        !socket.sent.some(
          (f) =>
            f.kind === FrameKind.WsFrame &&
            f.data === resize &&
            (f.encoding ?? 'utf-8') === 'utf-8',
        )
      ) {
        if (Date.now() >= deadline) {
          throw new Error(
            `runner never saw the resize control; saw: ${JSON.stringify(socket.sent)}`,
          );
        }
        await new Promise((r) => setTimeout(r, 10));
      }
      ws.close();
    } finally {
      stopped = true;
    }
  });

  it('rejects the client when the session has no online runner', async () => {
    const registry = new TunnelRegistry();
    const base = await buildApp(registry, async () => null);
    const ws = await connect(`${base}/v1/sessions/${SESSION_ID}/terminals/${TERMINAL_ID}/attach`);
    // The route closes the client socket (runner offline / unresolved).
    const code = await new Promise<number>((resolve) => {
      if (ws.readyState === WebSocket.CLOSED) {
        resolve(4004);
        return;
      }
      ws.once('close', (c: number) => resolve(c));
    });
    expect(code).toBeGreaterThanOrEqual(4000);
  });
});
