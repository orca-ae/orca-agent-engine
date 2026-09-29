// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner-side tunnel serve loop (the client half of Orca's
// framed-HTTP runner tunnel): the runner DIALS the registry, sends a hello, and
// serves the request/response stream the registry pushes (the runner behaves like
// a local app reached over the tunnel).
//
// Everything here is driven IN-PROCESS against a FAKE registry runner-tunnel WS
// peer — a real `ws` server that authenticates the PUBLIC path
// `/v1/tunnels/runners/:runnerId` + the `X-Orca-Runner-Tunnel-Token` header + the
// internal WS origin exactly as the production route does, receives the runner's
// hello, and then pushes request / request.cancel / ping / ws.* frames and reads
// the runner's framed responses. No registry app, no DB, no claude — the serve
// loop + dispatch seam are network-real but dependency-free.
//
// The protocol asserted here is the EXACT wire the registry side speaks (frames +
// transport from `@orca/harness-tunnel`); the fake peer below speaks the registry's
// half.

import { afterEach, beforeEach, describe, it, expect } from 'vitest';
import {
  FrameKind,
  INTERNAL_WS_ORIGIN,
  decodeFrame,
  encodeFrame,
  tokenBoundRunnerId,
} from '@orca/harness-tunnel';
import {
  MAX_CONSECUTIVE_401_REFRESHES,
  serveTunnel,
  type ServeTunnelHandle,
} from '../../src/tunnel/serve.js';
import { RouteDispatcher } from '../../src/tunnel/request-dispatch.js';
import type {
  RunnerTunnelConnector,
  RunnerTunnelMessage,
  RunnerTunnelSocket,
} from '../../src/tunnel/ws-client.js';
import {
  Deferred,
  FakeRegistryRunnerTunnel,
  ndjsonDispatcher,
  ndjsonResponse,
} from './support/fake-registry-runner-tunnel.js';

const BINDING_TOKEN = 'binding-token-fixture';
const RUNNER_ID = tokenBoundRunnerId(BINDING_TOKEN);
const RUNNER_VERSION = '0.1.0-test';

describe('serveTunnel (runner tunnel client + serve loop)', () => {
  let registry: FakeRegistryRunnerTunnel;

  beforeEach(async () => {
    registry = new FakeRegistryRunnerTunnel({ runnerId: RUNNER_ID, bindingToken: BINDING_TOKEN });
    await registry.listen();
  });

  afterEach(async () => {
    await registry.close();
  });

  function start(
    dispatcher: RouteDispatcher,
    overrides: Partial<Parameters<typeof serveTunnel>[0]> = {},
  ): ServeTunnelHandle {
    return serveTunnel({
      dispatcher,
      registryRunnerUrl: registry.baseUrl(),
      runnerId: RUNNER_ID,
      bindingToken: BINDING_TOKEN,
      runnerVersion: RUNNER_VERSION,
      ...overrides,
    });
  }

  it('dials the PUBLIC runner-tunnel path with the binding-token header + internal origin', async () => {
    const handle = start(new RouteDispatcher());
    const runner = await registry.nextRunner();
    expect(runner.path).toBe(`/v1/tunnels/runners/${RUNNER_ID}`);
    expect(runner.tokenHeader).toBe(BINDING_TOKEN);
    expect(runner.originHeader).toBe(INTERNAL_WS_ORIGIN);
    await handle.stop();
  });

  it('sends a hello with the runner version, frame protocol 1, providers, and resume cursors', async () => {
    const handle = start(new RouteDispatcher(), {
      providers: ['claude'],
      resumeCursors: { ses_1: 'evt_99' },
    });
    const runner = await registry.nextRunner();
    expect(runner.hello.runnerVersion).toBe(RUNNER_VERSION);
    expect(runner.hello.frameProtocolVersion).toBe(1);
    // The capability-advertise + recovery fields ride the hello's harnesses +
    // resume_cursors on the wire (the registry's connect hook reads them).
    expect(runner.hello.harnesses).toEqual(['claude']);
    expect(runner.hello.resumeCursors).toEqual({ ses_1: 'evt_99' });
    await handle.stop();
  });

  it('answers a pushed request via the dispatcher, framing head + body + end', async () => {
    const dispatcher = ndjsonDispatcher({
      '/v1/runner/turn': (body) => {
        const user = JSON.parse(body) as { id: string };
        return ndjsonResponse([
          { type: 'response.created', userEventId: user.id },
          { type: 'agent.turn_completed' },
        ]);
      },
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();

    const res = await runner.request({
      method: 'POST',
      path: '/v1/runner/turn',
      headers: [['x-orca-session-id', 'ses_1']],
      body: '{"id":"evt_user_1"}',
    });

    expect(res.status).toBe(200);
    const lines = res.body
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l) as { type: string });
    expect(lines.map((l) => l.type)).toEqual(['response.created', 'agent.turn_completed']);
    // The runner saw exactly head, two bodies, end (the framed response order).
    const kinds = runner.seen.map((f) => f.kind);
    expect(kinds).toEqual([
      FrameKind.ResponseHead,
      FrameKind.ResponseBody,
      FrameKind.ResponseBody,
      FrameKind.ResponseEnd,
    ]);
    await handle.stop();
  });

  it('forwards the request body + session header to the handler', async () => {
    let seenBody = '';
    let seenSession: string | undefined;
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async (req) => {
      seenBody = Buffer.from(req.body).toString('utf8');
      seenSession = req.header('X-Orca-Session-Id');
      return ndjsonResponse([]);
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    await runner.request({
      method: 'POST',
      path: '/v1/runner/turn',
      headers: [['x-orca-session-id', 'ses_77']],
      body: '{"id":"evt_1","text":"hi"}',
    });
    expect(seenBody).toBe('{"id":"evt_1","text":"hi"}');
    expect(seenSession).toBe('ses_77');
    await handle.stop();
  });

  it('runs concurrent requests independently (two reqIds in flight)', async () => {
    const release: Record<string, Deferred<void>> = {
      a: new Deferred<void>(),
      b: new Deferred<void>(),
    };
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async (req) => {
      const user = JSON.parse(Buffer.from(req.body).toString('utf8')) as { id: string };
      return {
        status: 200,
        headers: [['content-type', 'application/x-ndjson']],
        body: (async function* () {
          await release[user.id]!.promise;
          yield new TextEncoder().encode(`{"id":"${user.id}"}\n`);
        })(),
      };
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();

    const aDone = runner.request({ method: 'POST', path: '/v1/runner/turn', body: '{"id":"a"}' });
    const bDone = runner.request({ method: 'POST', path: '/v1/runner/turn', body: '{"id":"b"}' });
    // Release b first, then a — both complete regardless of order, proving the two
    // dispatch tasks are independent.
    release.b!.resolve();
    release.a!.resolve();
    const [a, b] = await Promise.all([aDone, bDone]);
    expect(a.body.trim()).toBe('{"id":"a"}');
    expect(b.body.trim()).toBe('{"id":"b"}');
    await handle.stop();
  });

  it('answers a ping with a pong echoing the ts', async () => {
    const handle = start(new RouteDispatcher());
    const runner = await registry.nextRunner();
    const ts = 1234567;
    expect(await runner.pingAndAwaitPong(ts)).toBe(ts);
    await handle.stop();
  });

  it('survives a pong write that rejects because the socket is already closing', async () => {
    // The pong is sent fire-and-forget, and a `ws` write on a non-OPEN socket
    // REJECTS. A server ping and the server's close frame can be parsed out of the
    // SAME TCP read, so on a routine recycle the pong write loses that race — and
    // the registry pings every runner tunnel on a keepalive interval. An unobserved
    // rejection there is an unhandled rejection, which Node's default
    // `--unhandled-rejections=throw` turns into a process-wide crash of the runner.
    // Driven over a scripted socket seam because a live peer cannot be made to lose
    // that race on demand; the rejection must be logged and the loop must carry on.
    const warns: string[] = [];
    const scripted: RunnerTunnelMessage[] = [
      { type: 'text', data: encodeFrame({ kind: FrameKind.Ping, ts: 7 }) },
      { type: 'close', code: 1012, reason: 'service restart' },
    ];
    let connects = 0;
    const connector: RunnerTunnelConnector = {
      connect: async (): Promise<RunnerTunnelSocket> => {
        connects += 1;
        const first = connects === 1;
        return {
          receive: (): Promise<RunnerTunnelMessage> => {
            if (!first) {
              // The reconnect parks here until stop() aborts the serve.
              return new Promise<RunnerTunnelMessage>(() => {});
            }
            const next = scripted.shift();
            return Promise.resolve(next ?? { type: 'close', code: 1012, reason: 'restart' });
          },
          sendText: (data: string): Promise<void> => {
            // The hello goes out fine; only the pong loses the race with the close.
            if (first && decodeFrame(data).kind === FrameKind.Pong) {
              return Promise.reject(new Error('WebSocket is not open: readyState 2 (CLOSING)'));
            }
            return Promise.resolve();
          },
          closeSocket: (): void => {},
        };
      },
    };

    const handle = start(new RouteDispatcher(), {
      connector,
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
      logger: {
        warn: (_obj: unknown, msg?: string) => {
          warns.push(msg ?? '');
        },
      },
    });

    // Wait for the failed pong to be observed and the loop to re-dial.
    const deadline = Date.now() + 2000;
    while (connects < 2 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 5));
    }
    expect(warns.some((m) => m.includes('pong send failed'))).toBe(true);
    // The tunnel did not die with the failed write: it reconnected.
    expect(connects).toBeGreaterThanOrEqual(2);
    await handle.stop();
  });

  it('cancels an in-flight request when the registry sends request.cancel', async () => {
    let aborted = false;
    const firstChunk = new Deferred<void>();
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async (_req, signal) => ({
      status: 200,
      headers: [['content-type', 'application/x-ndjson']],
      body: (async function* () {
        yield new TextEncoder().encode('{"type":"response.created"}\n');
        firstChunk.resolve();
        await new Promise<void>((resolve) => {
          if (signal.aborted) return resolve();
          signal.addEventListener('abort', () => resolve(), { once: true });
        });
        aborted = signal.aborted;
      })(),
    }));
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();

    const inflight = runner.startRequest({ method: 'POST', path: '/v1/runner/turn', body: '{}' });
    await inflight.headSeen();
    await firstChunk.promise;
    runner.cancel(inflight.id);
    await inflight.ended;
    expect(aborted).toBe(true);
    // The runner still ended the response so the registry's awaiter completes.
    expect(runner.bodySoFar(inflight.id)).toContain('response.created');
    await handle.stop();
  });

  it('answers an unrouted pushed request with a framed 404 (no tunnel crash)', async () => {
    const handle = start(new RouteDispatcher());
    const runner = await registry.nextRunner();
    const res = await runner.request({ method: 'POST', path: '/v1/runner/nope', body: '{}' });
    expect(res.status).toBe(404);
    // The tunnel is still alive: a follow-up request also gets answered.
    const res2 = await runner.request({ method: 'POST', path: '/v1/runner/nope', body: '{}' });
    expect(res2.status).toBe(404);
    await handle.stop();
  });

  it('surfaces a 500 + error body when a handler throws BEFORE sending head', async () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async () => {
      throw new Error('boom before head');
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    const res = await runner.request({ method: 'POST', path: '/v1/runner/turn', body: '{}' });
    expect(res.status).toBe(500);
    const body = JSON.parse(res.body) as { error: string };
    expect(body.error).toBe('runner_dispatch_failed');
    // The frames were head(500) + body + end, in order.
    expect(runner.seen.map((f) => f.kind)).toEqual([
      FrameKind.ResponseHead,
      FrameKind.ResponseBody,
      FrameKind.ResponseEnd,
    ]);
    await handle.stop();
  });

  it('ends the response (no synthetic head) when the body stream throws AFTER head', async () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async () => ({
      status: 200,
      headers: [['content-type', 'application/x-ndjson']],
      body: (async function* () {
        yield new TextEncoder().encode('{"type":"response.created"}\n');
        throw new Error('boom mid-stream');
      })(),
    }));
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    const res = await runner.request({ method: 'POST', path: '/v1/runner/turn', body: '{}' });
    // Head already went out with 200; the runner does NOT rewrite it to 500, it
    // just ends so the registry's body iterator completes.
    expect(res.status).toBe(200);
    expect(res.body).toContain('response.created');
    expect(runner.seen.map((f) => f.kind)).toEqual([
      FrameKind.ResponseHead,
      FrameKind.ResponseBody,
      FrameKind.ResponseEnd,
    ]);
    await handle.stop();
  });

  // ── Tunneled WS channels (ws.open / ws.frame / ws.close) ──

  it('dispatches a tunneled WS attach: text + binary both directions, then close', async () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.registerWebSocket('/v1/runner/attach', async (ch) => {
      await ch.accept();
      for await (const msg of ch.messages()) {
        if (msg.kind === 'text') {
          await ch.sendText(`echo:${msg.data}`);
        } else if (msg.kind === 'bytes') {
          await ch.sendBytes(Uint8Array.from([...msg.data].reverse()));
        }
      }
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();

    const channel = runner.openWsChannel('ch1', '/v1/runner/attach');
    channel.sendText('hello');
    const a = await channel.recv();
    expect(a.kind).toBe('text');
    expect(a.data).toBe('echo:hello');

    channel.sendBytes(Uint8Array.from([1, 2, 3]));
    const b = await channel.recv();
    expect(b.kind).toBe('bytes');
    expect(Array.from(b.data as Uint8Array)).toEqual([3, 2, 1]);

    channel.close(1000, 'bye');
    // The channel dispatch ends after the peer closes.
    await handle.stop();
  });

  it('surfaces a runner-side WS close to the registry when the handler closes the channel', async () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.registerWebSocket('/v1/runner/attach', async (ch) => {
      await ch.accept();
      await ch.close(4001, 'done');
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    const channel = runner.openWsChannel('ch1', '/v1/runner/attach');
    const closed = await channel.closed();
    expect(closed.code).toBe(4001);
    expect(closed.reason).toBe('done');
    await handle.stop();
  });

  it('drops a ws.frame for an unknown channel without crashing the tunnel', async () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async () => ndjsonResponse([{ type: 'ok' }]));
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    // A frame for a channel that was never opened: routed to nobody, dropped.
    runner.sendRaw({ kind: FrameKind.WsFrame, chId: 'ghost', data: 'x', encoding: 'utf-8' });
    // The tunnel still answers a normal request.
    const res = await runner.request({ method: 'POST', path: '/v1/runner/turn', body: '{}' });
    expect(res.status).toBe(200);
    await handle.stop();
  });

  it('drops a malformed inbound tunnel frame and keeps the tunnel alive', async () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async () => ndjsonResponse([{ type: 'ok' }]));
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    // Raw payloads the frame codec rejects: invalid JSON, then a well-formed JSON
    // object with an unknown kind. Both are logged + dropped, not fatal.
    runner.sendRawText('this is not json');
    runner.sendRawText('{"kind":"totally-unknown-frame"}');
    // The tunnel still answers a normal request afterwards.
    const res = await runner.request({ method: 'POST', path: '/v1/runner/turn', body: '{}' });
    expect(res.status).toBe(200);
    await handle.stop();
  });

  it('drops a ws.frame with malformed base64 without crashing the channel', async () => {
    const seen: Array<{ kind: string }> = [];
    const dispatcher = new RouteDispatcher();
    dispatcher.registerWebSocket('/v1/runner/attach', async (ch) => {
      await ch.accept();
      for await (const msg of ch.messages()) {
        seen.push({ kind: msg.kind });
        if (msg.kind === 'text') {
          await ch.sendText(`echo:${msg.data}`);
        }
      }
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    const channel = runner.openWsChannel('ch1', '/v1/runner/attach');
    // A base64-tagged frame whose data is not valid base64: dropped, never reaches
    // the handler, channel survives.
    runner.sendRaw({
      kind: FrameKind.WsFrame,
      chId: 'ch1',
      data: '!!!not-base64!!!',
      encoding: 'base64',
    });
    // A following good text frame is still delivered + echoed.
    channel.sendText('hi');
    const echoed = await channel.recv();
    expect(echoed.kind).toBe('text');
    expect(echoed.data).toBe('echo:hi');
    // The handler only ever saw the one (text) message, never the dropped binary.
    expect(seen).toEqual([{ kind: 'text' }]);
    channel.close(1000, 'bye');
    await handle.stop();
  });

  it('closes a ws attach with 1011 when no handler is registered for the path', async () => {
    // No registerWebSocket for the path ⇒ the runner has no handler and closes 1011.
    const handle = start(new RouteDispatcher());
    const runner = await registry.nextRunner();
    const channel = runner.openWsChannel('ch1', '/v1/runner/no-such-attach');
    const closed = await channel.closed();
    expect(closed.code).toBe(1011);
    expect(closed.reason).toBe('no handler for ws attach');
    await handle.stop();
  });

  it('closes a ws attach with 1011 when the handler throws', async () => {
    const dispatcher = new RouteDispatcher();
    dispatcher.registerWebSocket('/v1/runner/attach', async (ch) => {
      await ch.accept();
      throw new Error('boom in ws handler');
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    const channel = runner.openWsChannel('ch1', '/v1/runner/attach');
    const closed = await channel.closed();
    expect(closed.code).toBe(1011);
    expect(closed.reason).toBe('runner dispatch failed');
    await handle.stop();
  });

  it('fires teardownSignal AND frames a 1001 close to the registry on tunnel teardown', async () => {
    // The handler distinguishes a tunnel teardown from a peer close: it records
    // which ended its messages() loop. stop() tears the tunnel down out from under
    // the live channel, which must (a) fire teardownSignal — the capability a
    // handler with teardown-specific cleanup relies on — and (b) best-effort frame
    // a 1001 "runner shutdown" close to the registry over the still-open socket
    // BEFORE the socket closes.
    let teardownFired = false;
    const handlerReady = new Deferred<void>();
    const handlerExited = new Deferred<void>();
    const dispatcher = new RouteDispatcher();
    dispatcher.registerWebSocket('/v1/runner/attach', async (ch) => {
      await ch.accept();
      ch.teardownSignal.addEventListener('abort', () => {
        teardownFired = true;
      });
      // The listener is now wired: it is safe to tear the tunnel down.
      handlerReady.resolve();
      // Park on messages() until the tunnel tears the channel down.
      for await (const _msg of ch.messages()) {
        // no-op: this channel only observes the teardown.
      }
      handlerExited.resolve();
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    const channel = runner.openWsChannel('ch1', '/v1/runner/attach');
    // Make the handler actually start (its first send implies accept on the peer),
    // then wait until it has wired the teardown listener.
    channel.sendText('ping');
    await handlerReady.promise;
    const closed = channel.closed();
    // Tear the whole tunnel down; the live channel must see teardownSignal fire and
    // emit the 1001 close.
    await handle.stop();
    await handlerExited.promise;
    expect(teardownFired).toBe(true);
    // The registry received the teardown close frame, code 1001.
    const close = await closed;
    expect(close.code).toBe(1001);
    expect(close.reason).toBe('runner shutdown');
  });

  it('frames a 1001 teardown close even when the handler NEVER iterates messages()', async () => {
    // Pins the UNCONDITIONAL teardown 1001: the runner frames a 1001 "runner
    // shutdown" on the per-attach cancellation regardless of where the handler was
    // parked. A handler that reacts to teardown SOLELY via teardownSignal — never
    // calling messages() — must still get the wire-level 1001, not just the
    // AbortSignal. (Before the wasTornDown() fix this handler
    // shape closed with nothing, because tornDown is only set inside messages().)
    const handlerReady = new Deferred<void>();
    const handlerExited = new Deferred<void>();
    const dispatcher = new RouteDispatcher();
    dispatcher.registerWebSocket('/v1/runner/attach', async (ch) => {
      await ch.accept();
      // Send one frame so the fake peer sees the attach accepted, then park on the
      // teardown signal ALONE — deliberately never touching ch.messages().
      await ch.sendText('ready');
      handlerReady.resolve();
      await new Promise<void>((resolve) => {
        if (ch.teardownSignal.aborted) {
          resolve();
          return;
        }
        ch.teardownSignal.addEventListener('abort', () => resolve(), { once: true });
      });
      handlerExited.resolve();
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    const channel = runner.openWsChannel('ch1', '/v1/runner/attach');
    // Wait for the handler's first send (proves it accepted + is parked on teardown).
    const first = await channel.recv();
    expect(first.kind).toBe('text');
    expect(first.data).toBe('ready');
    await handlerReady.promise;
    const closed = channel.closed();
    // Tear the whole tunnel down. The non-iterating handler unblocks via
    // teardownSignal and returns; run() must STILL frame the 1001 over the
    // still-open socket before it closes.
    await handle.stop();
    await handlerExited.promise;
    const close = await closed;
    expect(close.code).toBe(1001);
    expect(close.reason).toBe('runner shutdown');
  });

  it('does NOT fire teardownSignal when the PEER closes the channel (clean peer close)', async () => {
    // A peer-initiated ws.close ends messages() WITHOUT firing teardownSignal — the
    // close came from the registry, not a tunnel teardown.
    let teardownFired = false;
    const handlerReady = new Deferred<void>();
    const handlerExited = new Deferred<void>();
    const dispatcher = new RouteDispatcher();
    dispatcher.registerWebSocket('/v1/runner/attach', async (ch) => {
      await ch.accept();
      ch.teardownSignal.addEventListener('abort', () => {
        teardownFired = true;
      });
      handlerReady.resolve();
      for await (const _msg of ch.messages()) {
        // drain until the peer closes
      }
      handlerExited.resolve();
    });
    const handle = start(dispatcher);
    const runner = await registry.nextRunner();
    const channel = runner.openWsChannel('ch1', '/v1/runner/attach');
    channel.sendText('hi');
    await handlerReady.promise;
    // Peer closes the channel: messages() ends, but this is NOT a tunnel teardown.
    channel.close(1000, 'peer done');
    await handlerExited.promise;
    expect(teardownFired).toBe(false);
    await handle.stop();
  });

  it('touches onActivity for work frames but NOT for ping keepalives', async () => {
    let activity = 0;
    const dispatcher = new RouteDispatcher();
    dispatcher.register('POST', '/v1/runner/turn', async () => ndjsonResponse([{ type: 'ok' }]));
    const handle = start(dispatcher, {
      onActivity: () => {
        activity += 1;
      },
    });
    const runner = await registry.nextRunner();

    // A ping must NOT count as activity (keepalives don't keep an idle runner up).
    expect(await runner.pingAndAwaitPong(42)).toBe(42);
    expect(activity).toBe(0);

    // A real request frame DOES count.
    const res = await runner.request({ method: 'POST', path: '/v1/runner/turn', body: '{}' });
    expect(res.status).toBe(200);
    expect(activity).toBe(1);

    // Another ping still does not bump the counter.
    expect(await runner.pingAndAwaitPong(43)).toBe(43);
    expect(activity).toBe(1);
    await handle.stop();
  });

  // ── Reconnect + lifecycle ──

  it('reconnects after the tunnel drops and re-sends hello', async () => {
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
    });
    const first = await registry.nextRunner();
    // Drop the socket from the server side; the runner should reconnect.
    first.dropSocket(1012, 'service restart');
    const second = await registry.nextRunner();
    expect(second.hello.runnerVersion).toBe(RUNNER_VERSION);
    expect(registry.handshakes.filter((h) => h.accepted).length).toBeGreaterThanOrEqual(2);
    await handle.stop();
  });

  it('reconnects (does not exit fatally) on a non-special server close code like 4003', async () => {
    // The registry route can close an accepted runner socket with 4003 (ping
    // watchdog) or 4403 (forbidden origin). Neither is in the client's fatal set
    // {4001,4002,4004,4500} nor its recycle set {1001,1012}, and neither is
    // special-cased. The correct posture for any such non-special close is a plain
    // reconnect-with-backoff (kind:'closed'), NOT a fatal exit: a healthy runner
    // never earns 4003/4403, but a misconfiguration/starvation that produced one
    // recovers by reconnecting rather than tearing the runner down. This locks
    // that behavior so the fatal/recycle sets can never silently absorb 4003/4403.
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
    });
    const first = await registry.nextRunner();
    // Server closes with 4003 "ping timeout" — a non-special application code.
    first.dropSocket(4003, 'ping timeout');
    // The client must dial again rather than reject `done`.
    const second = await registry.nextRunner();
    expect(second.hello.runnerVersion).toBe(RUNNER_VERSION);
    expect(registry.handshakes.filter((h) => h.accepted).length).toBeGreaterThanOrEqual(2);
    await handle.stop();
  });

  it('resets the reconnect backoff to base after a successful serve that then closes', async () => {
    // Pins the post-serve backoff reset: the delay returns to its initial value
    // immediately after the serve loop returns, just as the worker-tunnel worker
    // resets `backoff = base` after connectAndServe.
    // A connection that actually SERVED proves the registry reachable, so the NEXT
    // dial must start from the base delay — even when the close that ended it was a
    // clean 1000 / abrupt drop ({kind:'closed'}), not a recycle. Without the reset, a
    // runner that cycles connect→serve→clean-close ratchets `delay` toward the cap
    // (BASE → 2·BASE → 4·BASE …) across independent healthy sessions instead of
    // reconnecting promptly. We drive several such cycles and capture the un-jittered
    // backoff each time; an escalating delay fails this on the 2nd cycle onward.
    //
    // `Math.random` is pinned to 0.5 so the ±50% jitter factor collapses to exactly
    // 1.0 (`1 + (0.5*2 - 1)*0.5 = 1`) and `sleep` receives the raw `delay` — making
    // escalation observable without jitter noise. (Pinned for THIS test only and
    // restored in finally; nothing else here depends on Math.random.)
    const BASE = 7;
    const CAP = 5000;
    const delays: number[] = [];
    const realRandom = Math.random;
    Math.random = () => 0.5;
    try {
      const handle = start(new RouteDispatcher(), {
        initialReconnectDelayMs: BASE,
        maxReconnectDelayMs: CAP,
        // Capture each backoff request and resolve immediately so the loop spins fast.
        sleep: async (ms: number) => {
          delays.push(ms);
        },
      });

      // Three connect→serve→clean-close cycles. Each close is a plain 1000 (the first
      // two) / abrupt drop (the third) — all {kind:'closed'}, NOT a recycle — so the
      // only thing that can keep the backoff at base is the post-serve reset.
      const first = await registry.nextRunner();
      first.dropSocket(1000, 'normal closure');
      const second = await registry.nextRunner();
      second.dropSocket(1000, 'normal closure');
      const third = await registry.nextRunner();
      third.dropSocket(); // abrupt drop, no code
      await registry.nextRunner();

      // Each disconnect produced exactly one backoff before the next dial. With the
      // post-serve reset every captured backoff is exactly BASE; without it the
      // 2nd/3rd would be 2·BASE / 4·BASE. Assert the first three are all BASE.
      expect(delays.length).toBeGreaterThanOrEqual(3);
      expect(delays.slice(0, 3)).toEqual([BASE, BASE, BASE]);
      // And we genuinely reconnected each time (>=4 accepted handshakes across cycles).
      expect(registry.handshakes.filter((h) => h.accepted).length).toBeGreaterThanOrEqual(4);
      await handle.stop();
    } finally {
      Math.random = realRandom;
    }
  });

  it('fires onReconnect on a re-dial but NOT on the first connect', async () => {
    const reconnects: number[] = [];
    let n = 0;
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
      onReconnect: async () => {
        reconnects.push(++n);
      },
    });
    const first = await registry.nextRunner();
    // No reconnect yet on first connect.
    expect(reconnects).toHaveLength(0);
    first.dropSocket(1012, 'service restart');
    await registry.nextRunner();
    // Give the reconnect hook a beat to fire.
    await new Promise((r) => setTimeout(r, 20));
    expect(reconnects.length).toBeGreaterThanOrEqual(1);
    await handle.stop();
  });

  it('does NOT fire onReconnect when the FIRST successful connect follows a failed one', async () => {
    // The test above only covers connect-succeeds-then-recycles. Starting the runner
    // BEFORE the registry is reachable is an explicitly supported case (see the
    // module header), so a failed first attempt does not make the second one a
    // re-dial: onReconnect is documented to fire "after a successful RE-dial (not
    // the first connect)", and a catch-up scan on a runner that never served has
    // nothing to catch up on. That requires the re-dial flag to flip on a SUCCESSFUL
    // connect rather than on entering the attempt.
    registry.rejectNextUpgrades(503, 1);
    const reconnects: string[] = [];
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
      onReconnect: async () => {
        reconnects.push('fired');
      },
    });

    // Attempt #1 was rejected with 503, so this is attempt #2 — and it is still the
    // FIRST connect that ever succeeded. The hook must not have run.
    const first = await registry.nextRunner();
    expect(registry.handshakes.filter((h) => !h.accepted).length).toBe(1);
    expect(reconnects).toHaveLength(0);

    // A genuine re-dial after that connection DOES fire it — so the flag is set on
    // success, not simply never set.
    first.dropSocket(1012, 'service restart');
    await registry.nextRunner();
    await new Promise((r) => setTimeout(r, 20));
    expect(reconnects.length).toBeGreaterThanOrEqual(1);
    await handle.stop();
  });

  it('backs off between 401 refresh retries and stops fatally once the budget is spent', async () => {
    // A 401 the token factory can answer is retried like every other failure: through
    // the shared bottom-of-loop backoff. Jumping straight back to the top of the loop
    // instead SPINS — against a registry that always 401s the runner re-dialed
    // thousands of times a second with no sleep between attempts and `done` never
    // settled. And since refreshing can never fix a permanently mis-bound runner, a
    // bounded run of consecutive 401s is fatal rather than infinite.
    registry.rejectNextUpgrades(401, MAX_CONSECUTIVE_401_REFRESHES + 10);
    const delays: number[] = [];
    let refreshes = 0;
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
      tokenFactory: () => {
        refreshes += 1;
        return BINDING_TOKEN;
      },
      // Capture every backoff and resolve immediately so the loop runs fast; a
      // 401 path that skips the backoff records nothing here.
      sleep: async (ms: number) => {
        delays.push(ms);
      },
    });

    await expect(handle.done).rejects.toThrow(/runner tunnel rejected by server/);
    // One backoff per absorbed 401 (none for the fatal one that ends the loop).
    expect(delays.length).toBe(MAX_CONSECUTIVE_401_REFRESHES);
    // The dials are bounded by the budget instead of unbounded.
    expect(registry.handshakes.length).toBe(MAX_CONSECUTIVE_401_REFRESHES + 1);
    expect(registry.handshakes.every((h) => !h.accepted)).toBe(true);
    // The factory was consulted on every attempt (pre-connect + post-401).
    expect(refreshes).toBeGreaterThanOrEqual(MAX_CONSECUTIVE_401_REFRESHES);
    await handle.stop();
  });

  it('refreshes the binding token via tokenFactory before each connect', async () => {
    let calls = 0;
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
      // The runner id is derived from BINDING_TOKEN; the factory returns the same
      // token so the registry still authorizes, but we can prove it was consulted.
      tokenFactory: () => {
        calls += 1;
        return BINDING_TOKEN;
      },
    });
    const first = await registry.nextRunner();
    expect(calls).toBeGreaterThanOrEqual(1);
    expect(first.tokenHeader).toBe(BINDING_TOKEN);
    await handle.stop();
  });

  it('keeps retrying after a transient upgrade rejection (e.g. 503) then connects', async () => {
    registry.rejectNextUpgrades(503, 1);
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
    });
    const runner = await registry.nextRunner();
    expect(runner.hello.runnerVersion).toBe(RUNNER_VERSION);
    // One rejected upgrade was recorded before the accepted one.
    expect(registry.handshakes.some((h) => !h.accepted)).toBe(true);
    await handle.stop();
  });

  it('treats a 502 upgrade rejection as a routine recycle and reconnects promptly', async () => {
    // 502 is how an ingress bounces an upgrade mid-recycle. Unlike a transient 5xx
    // (which escalates the backoff), a 502 resets the backoff to base and
    // reconnects promptly — exercising the upgrade-recycle classification.
    registry.rejectNextUpgrades(502, 1);
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
    });
    const runner = await registry.nextRunner();
    expect(runner.hello.runnerVersion).toBe(RUNNER_VERSION);
    // The 502 was recorded as a rejected handshake before the accepted reconnect.
    const rejected = registry.handshakes.filter((h) => !h.accepted);
    expect(rejected.length).toBe(1);
    expect(registry.handshakes.some((h) => h.accepted)).toBe(true);
    await handle.stop();
  });

  it('stops fatally on a 401 upgrade rejection when no token factory is configured', async () => {
    // A 401 with no way to obtain a fresh token can never succeed → fatal.
    registry.rejectNextUpgrades(401, 100);
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
    });
    await expect(handle.done).rejects.toThrow(/runner tunnel rejected by server/);
    // It did not loop: exactly one (rejected) handshake attempt.
    expect(registry.handshakes.length).toBe(1);
    await handle.stop();
  });

  it('refreshes the token after a 401 (via tokenFactory) and reconnects', async () => {
    // The registry rejects the first upgrade with 401; the factory hands back a
    // fresh token (still the binding token, so the second upgrade authorizes) and
    // the loop retries promptly instead of giving up.
    registry.rejectNextUpgrades(401, 1);
    let calls = 0;
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
      tokenFactory: () => {
        calls += 1;
        return BINDING_TOKEN;
      },
    });
    const runner = await registry.nextRunner();
    expect(runner.hello.runnerVersion).toBe(RUNNER_VERSION);
    // The factory was consulted at least once for the pre-connect refresh + once
    // more on the 401.
    expect(calls).toBeGreaterThanOrEqual(2);
    expect(registry.handshakes.some((h) => !h.accepted)).toBe(true);
    await handle.stop();
  });

  it('stop() ends the loop and does not reconnect afterwards', async () => {
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
    });
    const first = await registry.nextRunner();
    await handle.stop();
    const acceptedBefore = registry.handshakes.filter((h) => h.accepted).length;
    // Drop the (now-closed) socket and confirm no NEW runner appears.
    first.dropSocket();
    await new Promise((r) => setTimeout(r, 30));
    expect(registry.handshakes.filter((h) => h.accepted).length).toBe(acceptedBefore);
  });

  it('stops fatally (no reconnect) on a 403 upgrade rejection', async () => {
    // 403 means the credential authenticated but the registry refused the tunnel:
    // retrying can never succeed, so the loop exits with a rejection.
    registry.rejectNextUpgrades(403, 100);
    const handle = start(new RouteDispatcher(), {
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
    });
    // Match the HTTP-status detail, not just the shared prefix, so this cannot
    // pass on the close-code rejection the 4004 sibling below covers.
    await expect(handle.done).rejects.toThrow(/HTTP 403/);
    // Exactly one handshake attempt — it did not loop.
    expect(registry.handshakes.length).toBe(1);
    await handle.stop();
  });

  it('stops fatally (no reconnect) on a runner-binding close code (4004)', async () => {
    // The server accepted the upgrade but closed with a binding-refusal code;
    // 4001/4002/4004/4500 are fatal frame-protocol/binding faults.
    const handle = serveTunnel({
      dispatcher: new RouteDispatcher(),
      registryRunnerUrl: registry.baseUrl(),
      // A runner id that does NOT match the token binding ⇒ the fake route closes
      // the accepted socket with 4004 right after the upgrade — synchronously and
      // before any hello, exactly as `runner-tunnel.routes.ts` refuses a mis-bound
      // peer. This is the ONLY 4004 the real registry can emit: post-hello it
      // closes 4001 (not a hello) or 4002 (version skew), never 4004.
      runnerId: 'runner_token_deadbeefdeadbeefdeadbeefdeadbeef',
      bindingToken: BINDING_TOKEN,
      runnerVersion: RUNNER_VERSION,
      initialReconnectDelayMs: 1,
      maxReconnectDelayMs: 5,
    });
    // Match the CLOSE-CODE detail, not just the shared prefix: the 403 sibling
    // above raises the same `runner tunnel rejected by server (...)` message, so
    // the prefix alone cannot tell the close-code path from the upgrade path.
    await expect(handle.done).rejects.toThrow(/close code 4004/);
    // Exactly one handshake attempt — a fatal close does not reconnect.
    expect(registry.handshakes.length).toBe(1);
    await handle.stop();
  });
});
