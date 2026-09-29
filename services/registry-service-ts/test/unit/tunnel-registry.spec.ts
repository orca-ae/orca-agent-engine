// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Behavioural spec for the server-side tunnel registry.
//
// The registry multiplexes HTTP-like request/response cycles AND tunneled
// WebSocket channels onto one runner WebSocket. These tests exercise the
// registry surface directly through a fake in-memory `WebSocketLike` — no real
// sockets, no infra:
//
//   * register / deregister, newest-wins replacement + in-flight abort;
//   * get / onlineRunnerIds / runnerOwner / length / has;
//   * connect-waiter machinery: waitForRunner resolution on register, timeout,
//     immediate-return-when-online, per-runner + global overflow caps,
//     connectWaiterCount / connectWaitStartedAt diagnostics;
//   * markFrameSeen / secondsSinceLastFrame generation guards;
//   * per-request lifecycle: openRequest (RunnerOfflineError when offline, dup
//     reqId Error), closeRequest, requestIsOpen;
//   * routeResponseFrame head/body/end reassembly into the per-req queues,
//     orphan + stale-session + wrong-kind branches, lastFrameAt bump;
//   * WS-channel lifecycle: openWsChannel (RunnerOfflineError when offline/stale,
//     dup chId Error), closeWsChannel (idempotent), routeWsInbound text/base64/
//     close branches, malformed-base64 + unknown-encoding drops, orphan channel;
//   * sendText enqueue onto the outbound queue + replaced-session ConnectionError;
//   * the full request round-trip + ws-channel round-trip driven through a fake
//     runner loop end to end.
//
// The wire `id` / `ch_id` correlation and the stable abort-message prefixes
// (`"tunnel closed before request completed"`,
// `"tunnel replaced by newer connection"`) are part of the cross-component
// contract; the tests assert those prefixes (the trailing newest-wins
// qualifier is informational, so it is matched by prefix, not verbatim).

import { describe, it, expect } from 'vitest';
import {
  FrameKind,
  decodeBody,
  decodeFrame,
  encodeBody,
  type HelloFrame,
  type ResponseBodyFrame,
} from '@orca/harness-tunnel';
import {
  TunnelRegistry,
  type RegistrySession,
  type RegistryWebSocketLike,
  type WsInboundItem,
} from '../../src/tunnel/tunnel-registry.js';

// ── Fake WebSocket pair ──────────────────────────────────

/** One half of a bidirectional fake WebSocket. `sendText` pushes onto the peer. */
class FakeWS implements RegistryWebSocketLike {
  private readonly recvQueue: string[] = [];
  private readonly recvWaiters: Array<(value: string) => void> = [];
  private peer: FakeWS | undefined;
  closed: { code: number; reason: string } | undefined;

  link(peer: FakeWS): void {
    this.peer = peer;
    peer.peer = this;
  }

  async sendText(data: string): Promise<void> {
    if (this.peer === undefined) {
      throw new Error('FakeWS not linked to a peer');
    }
    this.peer.push(data);
  }

  receiveText(): Promise<string> {
    const ready = this.recvQueue.shift();
    if (ready !== undefined) {
      return Promise.resolve(ready);
    }
    return new Promise<string>((resolve) => {
      this.recvWaiters.push(resolve);
    });
  }

  async close(opts?: { code?: number; reason?: string }): Promise<void> {
    this.closed = { code: opts?.code ?? 1000, reason: opts?.reason ?? '' };
  }

  private push(data: string): void {
    const waiter = this.recvWaiters.shift();
    if (waiter !== undefined) {
      waiter(data);
      return;
    }
    this.recvQueue.push(data);
  }
}

function makeWsPair(): [FakeWS, FakeWS] {
  const a = new FakeWS();
  const b = new FakeWS();
  a.link(b);
  return [a, b];
}

/** A send-only fake; `receiveText` is never used by the registry. */
class SinkWS implements RegistryWebSocketLike {
  readonly sent: string[] = [];
  closed: { code: number; reason: string } | undefined;

  async sendText(data: string): Promise<void> {
    this.sent.push(data);
  }

  receiveText(): Promise<string> {
    return new Promise<string>(() => {
      // never resolves
    });
  }

  async close(opts?: { code?: number; reason?: string }): Promise<void> {
    this.closed = { code: opts?.code ?? 1000, reason: opts?.reason ?? '' };
  }
}

function hello(harnesses: string[] = ['claude-sdk'], envs: string[] = ['os_sandbox']): HelloFrame {
  return {
    kind: FrameKind.Hello,
    runnerVersion: '0.1.0-test',
    frameProtocolVersion: 1,
    harnesses,
    envs,
  };
}

// A tick small enough to let queued microtasks/timers run — a synchronization
// point for places where the registry hands work to a later turn of the loop.
function tick(ms = 5): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Drain a session's outbound queue to its WebSocket, like the route sender task. */
function startOutboundDrain(session: RegistrySession): { stop: () => void } {
  let running = true;
  void (async () => {
    while (running) {
      const data = await session.outboundQueue.get();
      if (data === null) {
        return;
      }
      await session.ws.sendText(data);
    }
  })();
  return {
    stop: () => {
      running = false;
      session.outboundQueue.put(null);
    },
  };
}

async function drainResponseBody(state: {
  bodyQueue: { get(): Promise<ResponseBodyFrame | null> };
  abortedWith: Error | undefined;
}): Promise<{ chunks: Uint8Array[]; error: Error | undefined }> {
  const chunks: Uint8Array[] = [];
  for (;;) {
    const item = await state.bodyQueue.get();
    if (state.abortedWith !== undefined) {
      return { chunks, error: state.abortedWith };
    }
    if (item === null) {
      return { chunks, error: undefined };
    }
    chunks.push(decodeBody(item.body, item.encoding ?? 'utf-8'));
  }
}

function concatBytes(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

// ── construction guards ──────────────────────────────────

describe('TunnelRegistry — construction', () => {
  it('rejects a per-runner waiter cap below one', () => {
    expect(() => new TunnelRegistry({ maxConnectWaitersPerRunner: 0 })).toThrow(
      /maxConnectWaitersPerRunner must be at least 1/,
    );
  });

  it('rejects a total waiter cap below one', () => {
    expect(() => new TunnelRegistry({ maxConnectWaitersTotal: 0 })).toThrow(
      /maxConnectWaitersTotal must be at least 1/,
    );
  });

  it('starts empty', () => {
    const reg = new TunnelRegistry();
    expect(reg.length).toBe(0);
    expect(reg.has('nope')).toBe(false);
    expect(reg.onlineRunnerIds()).toEqual([]);
  });
});

// ── register / get / deregister ──────────────────────────

describe('TunnelRegistry — register / get / deregister', () => {
  it('registers a session and exposes it via get / length / has / onlineRunnerIds', () => {
    const reg = new TunnelRegistry();
    const [serverWs] = makeWsPair();
    const session = reg.register('r1', serverWs, hello());

    expect(session.runnerId).toBe('r1');
    expect(session.ws).toBe(serverWs);
    expect(session.hello).toEqual(hello());
    expect(session.owner).toBeUndefined();
    expect(session.inFlight.size).toBe(0);
    expect(session.wsChannels.size).toBe(0);
    expect(session.connectedAt).toBeGreaterThan(0);
    expect(session.lastFrameAt).toBeGreaterThan(0);

    expect(reg.get('r1')).toBe(session);
    expect(reg.length).toBe(1);
    expect(reg.has('r1')).toBe(true);
    expect(reg.onlineRunnerIds()).toEqual(['r1']);
  });

  it('records the owner when supplied and surfaces it via runnerOwner', () => {
    const reg = new TunnelRegistry();
    const [serverWs] = makeWsPair();
    reg.register('r1', serverWs, hello(), { owner: 'alice@example.com' });
    expect(reg.runnerOwner('r1')).toBe('alice@example.com');
  });

  it('runnerOwner returns undefined when the runner is offline or had no owner', () => {
    const reg = new TunnelRegistry();
    expect(reg.runnerOwner('ghost')).toBeUndefined();
    reg.register('r1', makeWsPair()[0], hello());
    expect(reg.runnerOwner('r1')).toBeUndefined();
  });

  it('onlineRunnerIds preserves insertion order', () => {
    const reg = new TunnelRegistry();
    reg.register('r3', makeWsPair()[0], hello());
    reg.register('r1', makeWsPair()[0], hello());
    reg.register('r2', makeWsPair()[0], hello());
    expect(reg.onlineRunnerIds()).toEqual(['r3', 'r1', 'r2']);
  });

  it('get returns undefined for an unknown runner', () => {
    const reg = new TunnelRegistry();
    expect(reg.get('nope')).toBeUndefined();
  });

  it('newest-wins: a re-register replaces the session and aborts old in-flight requests', async () => {
    const reg = new TunnelRegistry();
    const oldWs = new SinkWS();
    const oldSession = reg.register('r1', oldWs, hello());
    const drain = startOutboundDrain(oldSession);

    // Open a request on the old session and start draining its body.
    const state = reg.openRequest('r1', 'req1');
    const headPromise = state.headFuture.promise;
    const bodyPromise = drainResponseBody(state);

    // Re-register: newest wins.
    const newWs = new SinkWS();
    const newSession = reg.register('r1', newWs, hello());
    expect(newSession).not.toBe(oldSession);
    expect(reg.get('r1')).toBe(newSession);

    // The old in-flight head future rejects with the newest-wins ConnectionError.
    await expect(headPromise).rejects.toThrow(/tunnel replaced by newer connection/);
    const { chunks, error } = await bodyPromise;
    expect(chunks).toEqual([]);
    expect(error?.message).toMatch(/tunnel replaced by newer connection/);

    // The old session's in-flight map is cleared and its writer retired (sentinel
    // + best-effort close at code 4000).
    expect(oldSession.inFlight.size).toBe(0);
    await tick();
    expect(oldWs.closed?.code).toBe(4000);
    drain.stop();
  });

  it('newest-wins also aborts old open ws channels', async () => {
    const reg = new TunnelRegistry();
    const oldSession = reg.register('r1', new SinkWS(), hello());
    const channel = reg.openWsChannel('r1', 'ch01ch01');
    const inboundPromise = channel.inboundQueue.get();

    reg.register('r1', new SinkWS(), hello());

    // The channel's inbound queue receives the local-abort sentinel (null).
    expect(await inboundPromise).toBeNull();
    expect(oldSession.wsChannels.size).toBe(0);
  });

  it('deregister removes the session, aborts in-flight requests, and retires the writer', async () => {
    const reg = new TunnelRegistry();
    const ws = new SinkWS();
    const session = reg.register('r1', ws, hello());
    const drain = startOutboundDrain(session);

    const state = reg.openRequest('r1', 'req1');
    const headPromise = state.headFuture.promise;
    const bodyPromise = drainResponseBody(state);

    const removed = reg.deregister('r1');
    expect(removed).toBe(session);
    expect(reg.get('r1')).toBeUndefined();
    expect(reg.length).toBe(0);

    await expect(headPromise).rejects.toThrow(/tunnel closed before request completed/);
    const { error } = await bodyPromise;
    expect(error?.message).toMatch(/tunnel closed before request completed/);

    await tick();
    expect(ws.closed?.code).toBe(4003);
    drain.stop();
  });

  it('deregister returns undefined when the runner is already offline', () => {
    const reg = new TunnelRegistry();
    expect(reg.deregister('ghost')).toBeUndefined();
  });

  it('deregister with a generation guard only removes the matching session', () => {
    const reg = new TunnelRegistry();
    const stale = reg.register('r1', new SinkWS(), hello());
    const current = reg.register('r1', new SinkWS(), hello());
    // A stale route handler passing the OLD session must not delete the new one.
    expect(reg.deregister('r1', stale)).toBeUndefined();
    expect(reg.get('r1')).toBe(current);
    // The current handler's guarded deregister succeeds.
    expect(reg.deregister('r1', current)).toBe(current);
    expect(reg.get('r1')).toBeUndefined();
  });

  it('deregister aborts open ws channels too', async () => {
    const reg = new TunnelRegistry();
    const session = reg.register('r1', new SinkWS(), hello());
    const channel = reg.openWsChannel('r1', 'abcdef01');
    const inboundPromise = channel.inboundQueue.get();
    reg.deregister('r1');
    expect(await inboundPromise).toBeNull();
    expect(session.wsChannels.size).toBe(0);
  });
});

// ── connect waiters ──────────────────────────────────────

describe('TunnelRegistry — waitForRunner', () => {
  it('returns immediately when the runner is already online', async () => {
    const reg = new TunnelRegistry();
    const session = reg.register('r1', new SinkWS(), hello());
    await expect(reg.waitForRunner('r1', { timeoutS: 1 })).resolves.toBe(session);
    expect(reg.connectWaiterCount()).toBe(0);
  });

  it('resolves a waiting future when the runner registers', async () => {
    const reg = new TunnelRegistry();
    const waiting = reg.waitForRunner('r1', { timeoutS: 1 });
    await tick();
    expect(reg.connectWaiterCount('r1')).toBe(1);
    expect(reg.connectWaiterCount()).toBe(1);
    expect(reg.connectWaitStartedAt('r1')).toBeGreaterThan(0);

    const session = reg.register('r1', new SinkWS(), hello());
    await expect(waiting).resolves.toBe(session);
    // Registering drains the whole wait state.
    expect(reg.connectWaiterCount('r1')).toBe(0);
    expect(reg.connectWaiterCount()).toBe(0);
    expect(reg.connectWaitStartedAt('r1')).toBeUndefined();
  });

  it('resolves all waiters for a runner on register', async () => {
    const reg = new TunnelRegistry();
    const w1 = reg.waitForRunner('r1', { timeoutS: 1 });
    const w2 = reg.waitForRunner('r1', { timeoutS: 1 });
    await tick();
    expect(reg.connectWaiterCount('r1')).toBe(2);
    const session = reg.register('r1', new SinkWS(), hello());
    await expect(w1).resolves.toBe(session);
    await expect(w2).resolves.toBe(session);
    expect(reg.connectWaiterCount()).toBe(0);
  });

  it('returns undefined on timeout and cleans up its waiter', async () => {
    const reg = new TunnelRegistry();
    await expect(reg.waitForRunner('r1', { timeoutS: 0.02 })).resolves.toBeUndefined();
    expect(reg.connectWaiterCount('r1')).toBe(0);
    expect(reg.connectWaiterCount()).toBe(0);
    expect(reg.connectWaitStartedAt('r1')).toBeUndefined();
  });

  it('a non-positive timeout returns the current registry snapshot without waiting', async () => {
    const reg = new TunnelRegistry();
    await expect(reg.waitForRunner('r1', { timeoutS: 0 })).resolves.toBeUndefined();
    const session = reg.register('r1', new SinkWS(), hello());
    await expect(reg.waitForRunner('r1', { timeoutS: 0 })).resolves.toBe(session);
    expect(reg.connectWaiterCount()).toBe(0);
  });

  it('shares one wait state across concurrent waiters and reports its started_at', async () => {
    const reg = new TunnelRegistry();
    const w1 = reg.waitForRunner('r1', { timeoutS: 0.05 });
    await tick();
    const started = reg.connectWaitStartedAt('r1');
    const w2 = reg.waitForRunner('r1', { timeoutS: 0.05 });
    await tick();
    // Same wait state → same started_at; both waiters counted.
    expect(reg.connectWaitStartedAt('r1')).toBe(started);
    expect(reg.connectWaiterCount('r1')).toBe(2);
    await Promise.all([w1, w2]);
    expect(reg.connectWaiterCount()).toBe(0);
  });

  it('per-runner overflow: callers beyond the cap wait on timeout without registering a waiter', async () => {
    const reg = new TunnelRegistry({ maxConnectWaitersPerRunner: 1 });
    const w1 = reg.waitForRunner('r1', { timeoutS: 0.05 });
    await tick();
    expect(reg.connectWaiterCount('r1')).toBe(1);

    // The 2nd caller overflows the per-runner cap; it is NOT registered as a
    // waiter — it just times out and does one final registry check.
    const overflow = reg.waitForRunner('r1', { timeoutS: 0.05 });
    await tick();
    expect(reg.connectWaiterCount('r1')).toBe(1);
    await expect(overflow).resolves.toBeUndefined();
    await expect(w1).resolves.toBeUndefined();
  });

  it('per-runner overflow caller still observes a runner that registers during its wait', async () => {
    const reg = new TunnelRegistry({ maxConnectWaitersPerRunner: 1 });
    reg.waitForRunner('r1', { timeoutS: 0.2 });
    await tick();
    const overflow = reg.waitForRunner('r1', { timeoutS: 0.2 });
    await tick();
    expect(reg.connectWaiterCount('r1')).toBe(1);
    const session = reg.register('r1', new SinkWS(), hello());
    // The overflow caller's final registry check sees the now-online runner.
    await expect(overflow).resolves.toBe(session);
  });

  it('global overflow: callers beyond the global cap also use the bounded wait path', async () => {
    const reg = new TunnelRegistry({ maxConnectWaitersTotal: 1 });
    const w1 = reg.waitForRunner('r1', { timeoutS: 0.05 });
    await tick();
    expect(reg.connectWaiterCount()).toBe(1);

    // A different runner id, but the global cap is already hit.
    const overflow = reg.waitForRunner('r2', { timeoutS: 0.05 });
    await tick();
    expect(reg.connectWaiterCount()).toBe(1);
    expect(reg.connectWaiterCount('r2')).toBe(0);
    await expect(overflow).resolves.toBeUndefined();
    await expect(w1).resolves.toBeUndefined();
  });

  it('connectWaitStartedAt returns undefined when no one is waiting', () => {
    const reg = new TunnelRegistry();
    expect(reg.connectWaitStartedAt('r1')).toBeUndefined();
  });

  it('cancelConnectWaiters resolves every waiter with undefined and clears the state', async () => {
    const reg = new TunnelRegistry();
    const w1 = reg.waitForRunner('r1', { timeoutS: 5 });
    const w2 = reg.waitForRunner('r1', { timeoutS: 5 });
    await tick();
    expect(reg.connectWaiterCount('r1')).toBe(2);
    expect(reg.connectWaiterCount()).toBe(2);

    // The session distributor calls this when a launch it pre-registered a waiter
    // for can no longer produce a runner (the worker refused / its tunnel was
    // replaced). Each waiter resolves with `undefined` (the timeout value) and
    // the total is decremented exactly once.
    expect(reg.cancelConnectWaiters('r1')).toBe(2);
    await expect(w1).resolves.toBeUndefined();
    await expect(w2).resolves.toBeUndefined();
    expect(reg.connectWaiterCount('r1')).toBe(0);
    expect(reg.connectWaiterCount()).toBe(0);
    expect(reg.connectWaitStartedAt('r1')).toBeUndefined();
  });

  it('cancelConnectWaiters is a no-op (returns 0) when no waiter is registered', () => {
    const reg = new TunnelRegistry();
    expect(reg.cancelConnectWaiters('r1')).toBe(0);
    expect(reg.connectWaiterCount()).toBe(0);
  });

  it('cancelConnectWaiters leaves other runner ids untouched', async () => {
    const reg = new TunnelRegistry();
    const w1 = reg.waitForRunner('r1', { timeoutS: 5 });
    const w2 = reg.waitForRunner('r2', { timeoutS: 5 });
    await tick();
    expect(reg.connectWaiterCount()).toBe(2);

    expect(reg.cancelConnectWaiters('r1')).toBe(1);
    await expect(w1).resolves.toBeUndefined();
    // r2's waiter is still live until it registers.
    expect(reg.connectWaiterCount('r2')).toBe(1);
    const session = reg.register('r2', new SinkWS(), hello());
    await expect(w2).resolves.toBe(session);
    expect(reg.connectWaiterCount()).toBe(0);
  });
});

// ── frame-seen tracking ──────────────────────────────────

describe('TunnelRegistry — markFrameSeen / secondsSinceLastFrame', () => {
  it('marks a current session and reports idle seconds', async () => {
    const reg = new TunnelRegistry();
    const session = reg.register('r1', new SinkWS(), hello());
    await tick();
    expect(reg.markFrameSeen(session)).toBe(true);
    const idle = reg.secondsSinceLastFrame(session);
    expect(idle).not.toBeNull();
    expect(idle!).toBeGreaterThanOrEqual(0);
    expect(idle!).toBeLessThan(1);
  });

  it('returns false / null for a replaced (stale) session', () => {
    const reg = new TunnelRegistry();
    const stale = reg.register('r1', new SinkWS(), hello());
    reg.register('r1', new SinkWS(), hello());
    expect(reg.markFrameSeen(stale)).toBe(false);
    expect(reg.secondsSinceLastFrame(stale)).toBeNull();
  });

  it('returns false / null for a deregistered session', () => {
    const reg = new TunnelRegistry();
    const session = reg.register('r1', new SinkWS(), hello());
    reg.deregister('r1');
    expect(reg.markFrameSeen(session)).toBe(false);
    expect(reg.secondsSinceLastFrame(session)).toBeNull();
  });
});

// ── per-request lifecycle ────────────────────────────────

describe('TunnelRegistry — openRequest / closeRequest / requestIsOpen', () => {
  it('openRequest allocates fresh reassembly state', () => {
    const reg = new TunnelRegistry();
    const session = reg.register('r1', new SinkWS(), hello());
    const state = reg.openRequest('r1', 'req1');
    expect(state.session).toBe(session);
    expect(reg.requestIsOpen(session, 'req1')).toBe(true);
    expect(session.inFlight.get('req1')).toBe(state);
    expect(state.abortedWith).toBeUndefined();
    expect(state.headFuture.done).toBe(false);
  });

  it('openRequest throws when the runner is offline', () => {
    const reg = new TunnelRegistry();
    expect(() => reg.openRequest('ghost', 'req1')).toThrow(/ghost/);
  });

  it('openRequest rejects a duplicate req_id on the same session', () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    reg.openRequest('r1', 'req1');
    expect(() => reg.openRequest('r1', 'req1')).toThrow(/already in flight/);
  });

  it('closeRequest drops the reassembly slot', () => {
    const reg = new TunnelRegistry();
    const session = reg.register('r1', new SinkWS(), hello());
    reg.openRequest('r1', 'req1');
    reg.closeRequest('r1', 'req1');
    expect(reg.requestIsOpen(session, 'req1')).toBe(false);
  });

  it('closeRequest is a no-op for an unknown runner / req', () => {
    const reg = new TunnelRegistry();
    expect(() => reg.closeRequest('ghost', 'req1')).not.toThrow();
    reg.register('r1', new SinkWS(), hello());
    expect(() => reg.closeRequest('r1', 'no-such-req')).not.toThrow();
  });

  it('closeRequest accepts a stale session for post-replacement cleanup', () => {
    const reg = new TunnelRegistry();
    const stale = reg.register('r1', new SinkWS(), hello());
    const state = reg.openRequest('r1', 'req1');
    // Manually re-add to the stale session to simulate lingering state after a
    // newest-wins replacement (register already cleared it).
    stale.inFlight.set('req1', state);
    reg.register('r1', new SinkWS(), hello());
    reg.closeRequest('r1', 'req1', stale);
    expect(stale.inFlight.has('req1')).toBe(false);
  });
});

// ── routeResponseFrame ───────────────────────────────────

describe('TunnelRegistry — routeResponseFrame', () => {
  it('routes head / body / end into the per-req reassembly state and bumps lastFrameAt', async () => {
    const reg = new TunnelRegistry();
    const session = reg.register('r1', new SinkWS(), hello());
    const before = session.lastFrameAt;
    const state = reg.openRequest('r1', 'req1');
    const bodyPromise = drainResponseBody(state);
    await tick();

    expect(
      reg.routeResponseFrame('r1', {
        kind: FrameKind.ResponseHead,
        id: 'req1',
        status: 200,
        headers: [['content-type', 'text/plain']],
      }),
    ).toBe(true);
    const head = await state.headFuture.promise;
    expect(head.status).toBe(200);
    expect(head.headers).toEqual([['content-type', 'text/plain']]);

    expect(
      reg.routeResponseFrame('r1', {
        kind: FrameKind.ResponseBody,
        id: 'req1',
        body: 'first:',
        encoding: 'utf-8',
      }),
    ).toBe(true);
    const [bodyB64, enc] = encodeBody(
      new Uint8Array([0x00, 0xff, 0x10]),
      'application/octet-stream',
    );
    expect(enc).toBe('base64');
    expect(
      reg.routeResponseFrame('r1', {
        kind: FrameKind.ResponseBody,
        id: 'req1',
        body: bodyB64,
        encoding: 'base64',
      }),
    ).toBe(true);
    expect(reg.routeResponseFrame('r1', { kind: FrameKind.ResponseEnd, id: 'req1' })).toBe(true);

    const { chunks, error } = await bodyPromise;
    expect(error).toBeUndefined();
    const expected = concatBytes([
      new TextEncoder().encode('first:'),
      new Uint8Array([0x00, 0xff, 0x10]),
    ]);
    expect(Array.from(concatBytes(chunks))).toEqual(Array.from(expected));
    expect(session.lastFrameAt).toBeGreaterThanOrEqual(before);
    void session;
  });

  it('returns false for a frame whose req_id has no in-flight request (orphan)', () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    expect(reg.routeResponseFrame('r1', { kind: FrameKind.ResponseEnd, id: 'ghost-req' })).toBe(
      false,
    );
  });

  it('returns false when the runner is offline', () => {
    const reg = new TunnelRegistry();
    expect(
      reg.routeResponseFrame('ghost', { kind: FrameKind.ResponseHead, id: 'req1', status: 200 }),
    ).toBe(false);
  });

  it('returns false for a non-response frame kind', () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    reg.openRequest('r1', 'req1');
    expect(reg.routeResponseFrame('r1', { kind: FrameKind.Ping, ts: 1 })).toBe(false);
  });

  it('ignores frames from a stale session generation when guarded', () => {
    const reg = new TunnelRegistry();
    const stale = reg.register('r1', new SinkWS(), hello());
    reg.register('r1', new SinkWS(), hello());
    // Even though req1 is not tracked on the new session, the guard short-circuits
    // before any in-flight lookup: stale route handlers can't touch the new gen.
    expect(reg.routeResponseFrame('r1', { kind: FrameKind.ResponseEnd, id: 'req1' }, stale)).toBe(
      false,
    );
  });

  it('a second response.head is ignored once the head future is resolved', async () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    const state = reg.openRequest('r1', 'req1');
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: 'req1', status: 200 });
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: 'req1', status: 500 });
    const head = await state.headFuture.promise;
    expect(head.status).toBe(200);
  });
});

// ── sendText ─────────────────────────────────────────────

describe('TunnelRegistry — sendText', () => {
  it('enqueues an outbound frame onto the session outbound queue', async () => {
    const reg = new TunnelRegistry();
    const ws = new SinkWS();
    const session = reg.register('r1', ws, hello());
    const drain = startOutboundDrain(session);

    await reg.sendText(session, 'frame-1');
    await tick();
    expect(ws.sent).toEqual(['frame-1']);
    drain.stop();
  });

  it('rejects with ConnectionError once the session has been replaced', async () => {
    const reg = new TunnelRegistry();
    const stale = reg.register('r1', new SinkWS(), hello());
    reg.register('r1', new SinkWS(), hello());
    await expect(reg.sendText(stale, 'frame-1')).rejects.toThrow(/tunnel was replaced/);
  });
});

// ── WS channel lifecycle ─────────────────────────────────

describe('TunnelRegistry — WS channel lifecycle', () => {
  it('openWsChannel allocates a per-channel state on the session', () => {
    const reg = new TunnelRegistry();
    const session = reg.register('r1', new SinkWS(), hello());
    const channel = reg.openWsChannel('r1', 'ch00ch00');
    expect(channel.session).toBe(session);
    expect(session.wsChannels.get('ch00ch00')).toBe(channel);
  });

  it('openWsChannel throws when the runner is offline', () => {
    const reg = new TunnelRegistry();
    expect(() => reg.openWsChannel('ghost', 'ch00ch00')).toThrow(/ghost/);
  });

  it('openWsChannel throws when the generation guard is stale', () => {
    const reg = new TunnelRegistry();
    const stale = reg.register('r1', new SinkWS(), hello());
    reg.register('r1', new SinkWS(), hello());
    expect(() => reg.openWsChannel('r1', 'ch00ch00', { session: stale })).toThrow(/r1/);
  });

  it('openWsChannel rejects a duplicate ch_id', () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    reg.openWsChannel('r1', 'dupdup00');
    expect(() => reg.openWsChannel('r1', 'dupdup00')).toThrow(/already open/);
  });

  it('closeWsChannel removes the channel and is idempotent', () => {
    const reg = new TunnelRegistry();
    const session = reg.register('r1', new SinkWS(), hello());
    reg.openWsChannel('r1', 'ch00ch00');
    reg.closeWsChannel('r1', 'ch00ch00');
    expect(session.wsChannels.has('ch00ch00')).toBe(false);
    // Closing again, or an unknown channel / runner, is a no-op.
    expect(() => reg.closeWsChannel('r1', 'ch00ch00')).not.toThrow();
    expect(() => reg.closeWsChannel('ghost', 'whatever0')).not.toThrow();
  });

  it('closeWsChannel accepts a stale session for post-replacement cleanup', () => {
    const reg = new TunnelRegistry();
    const stale = reg.register('r1', new SinkWS(), hello());
    const channel = reg.openWsChannel('r1', 'ch00ch00');
    stale.wsChannels.set('ch00ch00', channel);
    reg.register('r1', new SinkWS(), hello());
    reg.closeWsChannel('r1', 'ch00ch00', stale);
    expect(stale.wsChannels.has('ch00ch00')).toBe(false);
  });
});

// ── routeWsInbound ───────────────────────────────────────

describe('TunnelRegistry — routeWsInbound', () => {
  it('delivers a utf-8 ws.frame as a ("text", str) inbound item', async () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    const channel = reg.openWsChannel('r1', 'ch00ch00');
    const next = channel.inboundQueue.get();

    expect(
      reg.routeWsInbound('r1', {
        kind: FrameKind.WsFrame,
        chId: 'ch00ch00',
        data: 'resize 80x24',
        encoding: 'utf-8',
      }),
    ).toBe(true);

    const item = (await next) as Exclude<WsInboundItem, null>;
    expect(item[0]).toBe('text');
    expect(item[1]).toBe('resize 80x24');
  });

  it('decodes a base64 ws.frame to a ("data", bytes) inbound item', async () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    const channel = reg.openWsChannel('r1', 'ch00ch00');
    const next = channel.inboundQueue.get();

    const raw = new Uint8Array([0x00, 0xff, 0x10]);
    expect(
      reg.routeWsInbound('r1', {
        kind: FrameKind.WsFrame,
        chId: 'ch00ch00',
        data: Buffer.from(raw).toString('base64'),
        encoding: 'base64',
      }),
    ).toBe(true);

    const item = (await next) as Exclude<WsInboundItem, null>;
    expect(item[0]).toBe('data');
    expect(Array.from(item[1] as Uint8Array)).toEqual([0x00, 0xff, 0x10]);
  });

  it('delivers a ws.close as a ("close", [code, reason]) inbound item', async () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    const channel = reg.openWsChannel('r1', 'ch00ch00');
    const next = channel.inboundQueue.get();

    expect(
      reg.routeWsInbound('r1', {
        kind: FrameKind.WsClose,
        chId: 'ch00ch00',
        code: 1011,
        reason: 'server error',
      }),
    ).toBe(true);

    const item = (await next) as Exclude<WsInboundItem, null>;
    expect(item[0]).toBe('close');
    expect(item[1]).toEqual([1011, 'server error']);
  });

  it('bumps lastFrameAt even for ws frames', async () => {
    const reg = new TunnelRegistry();
    const session = reg.register('r1', new SinkWS(), hello());
    reg.openWsChannel('r1', 'ch00ch00');
    await tick();
    const before = session.lastFrameAt;
    reg.routeWsInbound('r1', {
      kind: FrameKind.WsFrame,
      chId: 'ch00ch00',
      data: 'x',
      encoding: 'utf-8',
    });
    expect(session.lastFrameAt).toBeGreaterThanOrEqual(before);
  });

  it('drops a frame with malformed base64', () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    reg.openWsChannel('r1', 'ch00ch00');
    expect(
      reg.routeWsInbound('r1', {
        kind: FrameKind.WsFrame,
        chId: 'ch00ch00',
        data: '!!!not base64!!!',
        encoding: 'base64',
      }),
    ).toBe(false);
  });

  // Strict, fully-padded RFC 4648 base64 validation. Unpadded-but-otherwise-
  // canonical input (e.g. "QQ" / "AAE") and any input with whitespace, an
  // out-of-alphabet character, or misplaced/excess padding must be DROPPED, not
  // silently repaired-and-delivered the way a bare `Buffer.from(.., 'base64')`
  // would.
  it('drops non-canonical base64 (unpadded / whitespace / bad padding)', () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    reg.openWsChannel('r1', 'ch00ch00');
    const rejected = [
      'QQ', // unpadded "A" — incorrect padding
      'AAE', // unpadded two bytes — incorrect padding
      'ABC', // unpadded — incorrect padding
      'A', // 1 mod 4 data chars — impossible quantum
      'AB=', // total length not a multiple of 4
      'AB=A', // discontinuous padding
      'QQ===', // excess padding
      'AB-_', // url-safe alphabet, not standard base64
      'aa\naa', // embedded newline (whitespace not skipped)
      ' QUFB', // leading space
    ];
    for (const data of rejected) {
      expect(
        reg.routeWsInbound('r1', {
          kind: FrameKind.WsFrame,
          chId: 'ch00ch00',
          data,
          encoding: 'base64',
        }),
      ).toBe(false);
    }
  });

  it('accepts properly padded base64 and decodes it to bytes', async () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    const channel = reg.openWsChannel('r1', 'ch00ch00');
    const next = channel.inboundQueue.get();
    // "AAE=" is the *padded* form of the rejected "AAE"; it decodes to 0x00 0x01.
    expect(
      reg.routeWsInbound('r1', {
        kind: FrameKind.WsFrame,
        chId: 'ch00ch00',
        data: 'AAE=',
        encoding: 'base64',
      }),
    ).toBe(true);
    const item = (await next) as Exclude<WsInboundItem, null>;
    expect(item[0]).toBe('data');
    expect(Array.from(item[1] as Uint8Array)).toEqual([0x00, 0x01]);
  });

  it('returns false for an orphan channel id', () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    expect(reg.routeWsInbound('r1', { kind: FrameKind.WsFrame, chId: 'noChanXX', data: 'x' })).toBe(
      false,
    );
  });

  it('returns false for a non-ws frame kind', () => {
    const reg = new TunnelRegistry();
    reg.register('r1', new SinkWS(), hello());
    reg.openWsChannel('r1', 'ch00ch00');
    expect(reg.routeWsInbound('r1', { kind: FrameKind.Ping, ts: 1 })).toBe(false);
  });

  it('returns false when the runner is offline or the session guard is stale', () => {
    const reg = new TunnelRegistry();
    const stale = reg.register('r1', new SinkWS(), hello());
    reg.openWsChannel('r1', 'ch00ch00');
    expect(
      reg.routeWsInbound('ghost', { kind: FrameKind.WsFrame, chId: 'ch00ch00', data: 'x' }),
    ).toBe(false);
    reg.register('r1', new SinkWS(), hello());
    expect(
      reg.routeWsInbound('r1', { kind: FrameKind.WsFrame, chId: 'ch00ch00', data: 'x' }, stale),
    ).toBe(false);
  });
});

// ── End-to-end: full request round trip via a fake runner ──
//
// Exercises the WS-tunnel round trip at the registry level: a request frame is
// enqueued on the session, drained to a fake runner that replies with
// head/body/end, and the registry reassembles the response. No transport, no
// real sockets — the registry IS the unit under test.

describe('TunnelRegistry — request round trip through a fake runner', () => {
  it('reassembles a head + body + end response routed back through the registry', async () => {
    const reg = new TunnelRegistry();
    const [serverWs, runnerWs] = makeWsPair();
    const session = reg.register('runner-test-1', serverWs, hello());
    const drain = startOutboundDrain(session);

    // Fake runner: on a request frame, reply with a 200 + body + end.
    let running = true;
    void (async () => {
      while (running) {
        const text = await runnerWs.receiveText();
        const frame = decodeFrame(text);
        if (frame.kind !== FrameKind.Request) {
          continue;
        }
        await runnerWs.sendText(
          JSON.stringify({
            kind: FrameKind.ResponseHead,
            id: frame.id,
            status: 200,
            headers: [['content-type', 'application/json']],
          }),
        );
        await runnerWs.sendText(
          JSON.stringify({
            kind: FrameKind.ResponseBody,
            id: frame.id,
            body: '{"status":"ok"}',
            encoding: 'utf-8',
          }),
        );
        await runnerWs.sendText(JSON.stringify({ kind: FrameKind.ResponseEnd, id: frame.id }));
      }
    })();

    // Server side: drain runner→server frames into the registry.
    void (async () => {
      while (running) {
        const text = await serverWs.receiveText();
        reg.routeResponseFrame('runner-test-1', decodeFrame(text));
      }
    })();

    // Open a request, send the request frame, await the reassembled response.
    const reqId = 'req-roundtrip-1';
    const state = reg.openRequest('runner-test-1', reqId);
    const bodyPromise = drainResponseBody(state);
    await reg.sendText(
      session,
      JSON.stringify({ kind: FrameKind.Request, id: reqId, method: 'GET', path: '/health' }),
    );

    const head = await state.headFuture.promise;
    expect(head.status).toBe(200);
    const { chunks, error } = await bodyPromise;
    expect(error).toBeUndefined();
    expect(new TextDecoder().decode(concatBytes(chunks))).toBe('{"status":"ok"}');

    running = false;
    drain.stop();
  });

  it('keeps concurrent requests on separate reassembly states', async () => {
    const reg = new TunnelRegistry();
    const [serverWs, runnerWs] = makeWsPair();
    const session = reg.register('runner-test-1', serverWs, hello());
    const drain = startOutboundDrain(session);

    let running = true;
    void (async () => {
      while (running) {
        const text = await runnerWs.receiveText();
        const frame = decodeFrame(text);
        if (frame.kind !== FrameKind.Request) {
          continue;
        }
        // Echo the request id back in the body so we can prove no cross-talk.
        await runnerWs.sendText(
          JSON.stringify({ kind: FrameKind.ResponseHead, id: frame.id, status: 200 }),
        );
        await runnerWs.sendText(
          JSON.stringify({
            kind: FrameKind.ResponseBody,
            id: frame.id,
            body: frame.id,
            encoding: 'utf-8',
          }),
        );
        await runnerWs.sendText(JSON.stringify({ kind: FrameKind.ResponseEnd, id: frame.id }));
      }
    })();
    void (async () => {
      while (running) {
        const text = await serverWs.receiveText();
        reg.routeResponseFrame('runner-test-1', decodeFrame(text));
      }
    })();

    const ids = ['req-a', 'req-b', 'req-c', 'req-d', 'req-e'];
    const results = await Promise.all(
      ids.map(async (reqId) => {
        const state = reg.openRequest('runner-test-1', reqId);
        const bodyPromise = drainResponseBody(state);
        await reg.sendText(
          session,
          JSON.stringify({ kind: FrameKind.Request, id: reqId, method: 'GET', path: '/health' }),
        );
        await state.headFuture.promise;
        const { chunks } = await bodyPromise;
        return new TextDecoder().decode(concatBytes(chunks));
      }),
    );
    expect(results).toEqual(ids);
    running = false;
    drain.stop();
  });
});

// ── End-to-end: WS channel round trip through a fake runner ──

describe('TunnelRegistry — ws-channel round trip through a fake runner', () => {
  it('routes ws.open + ws.frame echo + ws.close back onto the channel inbound queue', async () => {
    const reg = new TunnelRegistry();
    const [serverWs, runnerWs] = makeWsPair();
    const session = reg.register('runner-test-1', serverWs, hello());
    const drain = startOutboundDrain(session);

    const chId = 'checho00';

    // Fake runner: echo each ws.frame back, then close when it sees a "bye".
    let running = true;
    void (async () => {
      while (running) {
        const text = await runnerWs.receiveText();
        const frame = decodeFrame(text);
        if (frame.kind === FrameKind.WsFrame && frame.chId === chId) {
          if (frame.data === 'bye') {
            await runnerWs.sendText(
              JSON.stringify({ kind: FrameKind.WsClose, ch_id: chId, code: 1000, reason: 'done' }),
            );
          } else {
            await runnerWs.sendText(
              JSON.stringify({
                kind: FrameKind.WsFrame,
                ch_id: chId,
                data: `echo:${frame.data}`,
                encoding: 'utf-8',
              }),
            );
          }
        }
      }
    })();

    // Server side: route runner→server ws frames onto the channel inbound queue.
    void (async () => {
      while (running) {
        const text = await serverWs.receiveText();
        reg.routeWsInbound('runner-test-1', decodeFrame(text));
      }
    })();

    const channel = reg.openWsChannel('runner-test-1', chId);

    // Open + first data frame.
    await reg.sendText(
      session,
      JSON.stringify({ kind: FrameKind.WsOpen, ch_id: chId, path: '/term' }),
    );
    await reg.sendText(
      session,
      JSON.stringify({ kind: FrameKind.WsFrame, ch_id: chId, data: 'hello', encoding: 'utf-8' }),
    );

    const echoed = (await channel.inboundQueue.get()) as Exclude<WsInboundItem, null>;
    expect(echoed[0]).toBe('text');
    expect(echoed[1]).toBe('echo:hello');

    // Now ask the runner to close.
    await reg.sendText(
      session,
      JSON.stringify({ kind: FrameKind.WsFrame, ch_id: chId, data: 'bye', encoding: 'utf-8' }),
    );
    const closed = (await channel.inboundQueue.get()) as Exclude<WsInboundItem, null>;
    expect(closed[0]).toBe('close');
    expect(closed[1]).toEqual([1000, 'done']);

    reg.closeWsChannel('runner-test-1', chId);
    running = false;
    drain.stop();
  });
});
