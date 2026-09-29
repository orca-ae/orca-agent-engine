// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the tunnel HTTP transport adapter.
//
// The transport
// sends a `request` frame over a runner's tunnel, awaits the `response.head`,
// then streams `response.body` chunks out of a per-request queue until the
// `response.end` sentinel (or a tunnel abort). These tests exercise:
//
//   * handleRequest: offline runner -> ConnectError, including the
//     register-then-deregister race;
//   * a full request/response cycle with a streamed body;
//   * POST body encoding into the request frame;
//   * TunneledByteStream abort propagation (a mid-stream tunnel disconnect
//     surfaces the abort error and discards any already-queued chunk);
//   * TunneledByteStream.close() request cleanup;
//   * transport.close() being a safe no-op.
//
// The registry is the transport's collaborator. A
// faithful in-memory registry drives the transport here — no real sockets. It
// reproduces exactly the queue / deferred / abort semantics the transport
// depends on: register / deregister / openRequest / closeRequest /
// requestIsOpen / sendText / routeResponseFrame, plus get(). The wire `id`
// correlation and the `ConnectionError("tunnel closed before request
// completed")` abort message are part of the cross-component contract and are
// asserted verbatim.

import { describe, it, expect } from 'vitest';
import {
  FrameKind,
  decodeBody,
  decodeFrame,
  type Frame,
  type HelloFrame,
  type ResponseBodyFrame,
  type ResponseHeadFrame,
} from '../../src/frames.js';
import {
  Deferred,
  AsyncQueue,
  TunnelTransport,
  TunneledByteStream,
  type RequestState,
  BoundedResponseBodyQueue,
  ResponseBufferOverflowError,
  TunnelRequestAbortedError,
  pushResponseBody,
  type TunnelSession,
  type TransportRegistry,
  type TunnelRequest,
  type WebSocketLike,
} from '../../src/transport.js';

// ── Fakes ────────────────────────────────────────────────

/** Minimal WebSocket fake. `receiveText` never resolves (transport never reads). */
class NoopWS implements WebSocketLike {
  async sendText(_data: string): Promise<void> {
    // no-op
  }

  receiveText(): Promise<string> {
    return new Promise<string>(() => {
      // never resolves
    });
  }
}

function hello(): HelloFrame {
  return {
    kind: FrameKind.Hello,
    runnerVersion: '0.1.0',
    frameProtocolVersion: 1,
    harnesses: [],
    envs: [],
  };
}

/** Build a minimal tunnel request for testing. A GET with no body by default. */
function makeRequest(method = 'GET', path = '/health'): TunnelRequest {
  return { method, path };
}

// ── Faithful in-memory registry (the transport's collaborator) ──
//
// Implements the slice of the registry the transport touches. The abort message
// and `request.cancel` routing match the real registry exactly so the
// transport's behavior is observable end-to-end.

class FakeRegistry implements TransportRegistry {
  private readonly sessions = new Map<string, TunnelSession>();

  register(runnerId: string, ws: WebSocketLike, helloFrame: HelloFrame): TunnelSession {
    const session: TunnelSession = {
      runnerId,
      ws,
      hello: helloFrame,
      inFlight: new Map<string, RequestState>(),
    };
    this.sessions.set(runnerId, session);
    return session;
  }

  deregister(runnerId: string, session?: TunnelSession): TunnelSession | undefined {
    const current = this.sessions.get(runnerId);
    if (current === undefined || (session !== undefined && current !== session)) {
      return undefined;
    }
    this.sessions.delete(runnerId);
    // Abort all in-flight requests so awaiters get a clean failure.
    for (const state of current.inFlight.values()) {
      abortRequestState(state, new ConnectionError('tunnel closed before request completed'));
    }
    current.inFlight.clear();
    return current;
  }

  get(runnerId: string): TunnelSession | undefined {
    return this.sessions.get(runnerId);
  }

  openRequest(runnerId: string, reqId: string): RequestState {
    const session = this.sessions.get(runnerId);
    if (session === undefined) {
      throw new RunnerOfflineError(runnerId);
    }
    if (session.inFlight.has(reqId)) {
      throw new Error(
        `req_id ${JSON.stringify(reqId)} already in flight on runner ${JSON.stringify(runnerId)}`,
      );
    }
    const state: RequestState = {
      session,
      reqId,
      headFuture: new Deferred<ResponseHeadFrame>(),
      bodyQueue: new BoundedResponseBodyQueue(),
      endEvent: new Deferred<void>(),
      abortedWith: undefined,
    };
    session.inFlight.set(reqId, state);
    return state;
  }

  closeRequest(runnerId: string, reqId: string, session?: TunnelSession): void {
    const target = session ?? this.sessions.get(runnerId);
    if (target === undefined) {
      return;
    }
    target.inFlight.delete(reqId);
  }

  requestIsOpen(session: TunnelSession, reqId: string): boolean {
    return session.inFlight.has(reqId);
  }

  async sendText(session: TunnelSession, data: string): Promise<void> {
    if (this.sessions.get(session.runnerId) !== session) {
      throw new ConnectionError(`runner ${JSON.stringify(session.runnerId)} tunnel was replaced`);
    }
    await session.ws.sendText(data);
  }

  /** Route an incoming response frame into the right per-req reassembly slot. */
  routeResponseFrame(runnerId: string, frame: Frame): boolean {
    const current = this.sessions.get(runnerId);
    if (current === undefined) {
      return false;
    }
    if (
      frame.kind !== FrameKind.ResponseHead &&
      frame.kind !== FrameKind.ResponseBody &&
      frame.kind !== FrameKind.ResponseEnd
    ) {
      return false;
    }
    const state = current.inFlight.get(frame.id);
    if (state === undefined) {
      return false;
    }
    if (frame.kind === FrameKind.ResponseHead) {
      if (!state.headFuture.done) {
        state.headFuture.resolve(frame as ResponseHeadFrame);
      }
      return true;
    }
    if (frame.kind === FrameKind.ResponseBody) {
      // The mandated delivery path: pushResponseBody enforces the per-request
      // buffered-byte cap and owns the overflow abort + cancel.
      pushResponseBody(state, frame as ResponseBodyFrame);
      return true;
    }
    // ResponseEnd: signal completion + push the end-of-stream sentinel.
    state.endEvent.resolve();
    state.bodyQueue.end();
    return true;
  }
}

/** Error raised when a request is opened against an offline runner. */
class RunnerOfflineError extends Error {
  constructor(runnerId: string) {
    super(`runner ${JSON.stringify(runnerId)} is offline`);
    this.name = 'RunnerOfflineError';
  }
}

/** The `ConnectionError` the registry raises for tunnel aborts. */
class ConnectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

function abortRequestState(state: RequestState, error: Error): void {
  state.abortedWith = error;
  if (!state.headFuture.done) {
    state.headFuture.reject(error);
  }
  state.endEvent.resolve();
  state.bodyQueue.end();
}

// A tick small enough to let queued microtasks/timers run — a short-sleep
// synchronization point (~10ms).
function tick(ms = 10): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function drainStream(stream: AsyncIterable<Uint8Array>): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let total = 0;
  for await (const chunk of stream) {
    parts.push(chunk);
    total += chunk.length;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function utf8(s: string): string {
  return new TextDecoder().decode(new TextEncoder().encode(s));
}

// ── handleRequest: offline runner ────────────────────────

describe('TunnelTransport.handleRequest — offline runner', () => {
  it('raises ConnectError when the runner is offline', async () => {
    const reg = new FakeRegistry();
    const transport = new TunnelTransport(reg, 'r1');

    await expect(transport.handleRequest(makeRequest())).rejects.toMatchObject({
      name: 'ConnectError',
    });
    await expect(transport.handleRequest(makeRequest())).rejects.toThrow(/offline/);
  });

  it('raises ConnectError when the runner goes offline between get() and openRequest()', async () => {
    const reg = new FakeRegistry();
    reg.register('r1', new NoopWS(), hello());
    const transport = new TunnelTransport(reg, 'r1');

    // Deregister between get and openRequest — simulate a race. We force the
    // race deterministically by hooking the registry's get() to deregister
    // right after the transport has fetched the (now-doomed) session.
    const realGet = reg.get.bind(reg);
    let raced = false;
    reg.get = (runnerId: string): TunnelSession | undefined => {
      const session = realGet(runnerId);
      if (!raced) {
        raced = true;
        reg.deregister('r1');
      }
      return session;
    };

    await expect(transport.handleRequest(makeRequest())).rejects.toThrow(/offline/);
  });
});

// ── handleRequest: successful response ───────────────────

describe('TunnelTransport.handleRequest — successful response', () => {
  it('completes a full request/response cycle through the transport', async () => {
    const reg = new FakeRegistry();
    const session0 = reg.register('r1', new NoopWS(), hello());
    const transport = new TunnelTransport(reg, 'r1');

    // Capture frames the transport sends so we can assert the emitted request
    // frame's headers (a bodyless GET with no caller headers emits headers: []).
    const sent: string[] = [];
    const realSend = session0.ws.sendText.bind(session0.ws);
    session0.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
      await realSend(data);
    };

    // Start the request without awaiting it yet.
    const request = makeRequest();
    const pending = transport.handleRequest(request);

    // Wait for the request to be opened in the registry.
    await tick();

    const session = reg.get('r1');
    expect(session).not.toBeUndefined();
    expect(session!.inFlight.size).toBe(1);
    const reqId = session!.inFlight.keys().next().value as string;

    // The GET request frame carries no body and no headers.
    expect(sent.length).toBe(1);
    const reqFrame = decodeFrame(sent[0]);
    expect(reqFrame.kind).toBe(FrameKind.Request);
    if (reqFrame.kind === FrameKind.Request) {
      expect(reqFrame.id).toBe(reqId);
      expect(reqFrame.method).toBe('GET');
      expect(reqFrame.path).toBe('/health');
      expect(reqFrame.headers).toEqual([]);
      expect(reqFrame.body).toBeNull();
    }

    reg.routeResponseFrame('r1', {
      kind: FrameKind.ResponseHead,
      id: reqId,
      status: 200,
      headers: [['content-type', 'text/plain']],
    });
    reg.routeResponseFrame('r1', {
      kind: FrameKind.ResponseBody,
      id: reqId,
      body: 'hello',
      encoding: 'utf-8',
    });
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseEnd, id: reqId });

    const response = await pending;
    expect(response.status).toBe(200);

    // Drain the streaming body.
    const body = await drainStream(response.stream);
    expect(new TextDecoder().decode(body)).toBe('hello');

    // After iteration, the request should be closed.
    expect(session!.inFlight.has(reqId)).toBe(false);
  });

  it('encodes the POST body into the request frame', async () => {
    const reg = new FakeRegistry();
    reg.register('r1', new NoopWS(), hello());
    const transport = new TunnelTransport(reg, 'r1');

    // Capture frames the transport sends.
    const sent: string[] = [];
    const session0 = reg.get('r1')!;
    const realSend = session0.ws.sendText.bind(session0.ws);
    session0.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
      await realSend(data);
    };

    const request: TunnelRequest = {
      method: 'POST',
      path: '/v1/sessions/s1/events',
      body: new TextEncoder().encode('{"role":"user"}'),
      contentType: 'application/json',
    };
    const pending = transport.handleRequest(request);
    await tick();

    const session = reg.get('r1');
    expect(session).not.toBeUndefined();
    const reqId = session!.inFlight.keys().next().value as string;

    // The request frame carried the body inline, utf-8, byte-compatible keys.
    expect(sent.length).toBe(1);
    const frame = decodeFrame(sent[0]);
    expect(frame.kind).toBe(FrameKind.Request);
    if (frame.kind === FrameKind.Request) {
      expect(frame.id).toBe(reqId);
      expect(frame.method).toBe('POST');
      expect(frame.path).toBe('/v1/sessions/s1/events');
      expect(frame.body).toBe('{"role":"user"}');
      expect(frame.encoding).toBe('utf-8');
      expect(frame.stream).toBe(true);
      // The `contentType` convenience field rides in the frame's `headers` so
      // the runner (which rebuilds the request scope from `frame.headers`) sees
      // it — content-type travels in the forwarded header set rather than as an
      // out-of-band hint.
      expect(frame.headers).toEqual([['content-type', 'application/json']]);
    }

    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: reqId, status: 201 });
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseEnd, id: reqId });

    const response = await pending;
    expect(response.status).toBe(201);
  });

  it('forwards caller headers verbatim and does not duplicate content-type', async () => {
    const reg = new FakeRegistry();
    const session0 = reg.register('r1', new NoopWS(), hello());
    const transport = new TunnelTransport(reg, 'r1');

    const sent: string[] = [];
    const realSend = session0.ws.sendText.bind(session0.ws);
    session0.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
      await realSend(data);
    };

    // A caller-supplied content-type header plus a (redundant) contentType field:
    // the explicit header wins and is not duplicated, and other headers are
    // forwarded in order — the runner rebuilds the request scope from these.
    const request: TunnelRequest = {
      method: 'POST',
      path: '/v1/sessions/s1/events',
      headers: [
        ['x-trace-id', 'abc123'],
        ['content-type', 'application/json; charset=utf-8'],
      ],
      contentType: 'application/octet-stream',
      body: new TextEncoder().encode('{"role":"user"}'),
    };
    const pending = transport.handleRequest(request);
    await tick();

    const session = reg.get('r1');
    const reqId = session!.inFlight.keys().next().value as string;

    expect(sent.length).toBe(1);
    const frame = decodeFrame(sent[0]);
    expect(frame.kind).toBe(FrameKind.Request);
    if (frame.kind === FrameKind.Request) {
      expect(frame.headers).toEqual([
        ['x-trace-id', 'abc123'],
        ['content-type', 'application/json; charset=utf-8'],
      ]);
      // The explicit header (not the contentType field) also drives encoding.
      expect(frame.encoding).toBe('utf-8');
    }

    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: reqId, status: 201 });
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseEnd, id: reqId });

    const response = await pending;
    expect(response.status).toBe(201);
  });
});

// ── TunneledByteStream: abort propagation ────────────────

describe('TunneledByteStream — abort propagation', () => {
  it('propagates a mid-stream tunnel disconnect as the abort error', async () => {
    // The stream checks abortedWith after each get(), so even a queued body
    // chunk that arrived before the abort is not yielded once the abort flag
    // is set — the ConnectionError surfaces immediately.
    const reg = new FakeRegistry();
    reg.register('r1', new NoopWS(), hello());
    const state = reg.openRequest('r1', 'req1');

    const stream = new TunneledByteStream(reg, 'r1', 'req1', state);

    // Simulate head arriving then a body chunk, then the tunnel aborting.
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: 'req1', status: 200 });
    reg.routeResponseFrame('r1', {
      kind: FrameKind.ResponseBody,
      id: 'req1',
      body: 'chunk1',
      encoding: 'utf-8',
    });

    // Now deregister to abort.
    reg.deregister('r1');

    const chunks: Uint8Array[] = [];
    await expect(
      (async () => {
        for await (const chunk of stream) {
          chunks.push(chunk);
        }
      })(),
    ).rejects.toThrow(/tunnel closed/);

    // The abort flag is checked after get() returns, so the queued chunk is
    // discarded and the error raises before any yield.
    expect(chunks).toEqual([]);
  });
});

// ── TunneledByteStream: close cleans up ──────────────────

describe('TunneledByteStream — close', () => {
  it('closes the request in the registry on close()', async () => {
    const reg = new FakeRegistry();
    const session = reg.register('r1', new NoopWS(), hello());
    const state = reg.openRequest('r1', 'req1');

    const stream = new TunneledByteStream(reg, 'r1', 'req1', state);
    await stream.close();

    expect(session.inFlight.has('req1')).toBe(false);
  });

  it('sends a request.cancel frame when the request is still open on close()', async () => {
    const reg = new FakeRegistry();
    const session = reg.register('r1', new NoopWS(), hello());
    const state = reg.openRequest('r1', 'req1');

    const sent: string[] = [];
    const realSend = session.ws.sendText.bind(session.ws);
    session.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
      await realSend(data);
    };

    const stream = new TunneledByteStream(reg, 'r1', 'req1', state);
    await stream.close();

    // A request.cancel frame went out before the slot was closed.
    expect(sent.length).toBe(1);
    const frame = decodeFrame(sent[0]);
    expect(frame.kind).toBe(FrameKind.RequestCancel);
    if (frame.kind === FrameKind.RequestCancel) {
      expect(frame.id).toBe('req1');
      expect(frame.reason).toBe('client_disconnected');
    }
    expect(session.inFlight.has('req1')).toBe(false);
  });

  it('does not send a cancel frame when the request is already closed', async () => {
    const reg = new FakeRegistry();
    const session = reg.register('r1', new NoopWS(), hello());
    const state = reg.openRequest('r1', 'req1');
    // Close the slot first, so close() must NOT emit a cancel frame.
    reg.closeRequest('r1', 'req1');

    const sent: string[] = [];
    session.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
    };

    const stream = new TunneledByteStream(reg, 'r1', 'req1', state);
    await stream.close();

    expect(sent).toEqual([]);
  });
});

// ── full body streaming over multiple chunks ─────────────

describe('TunneledByteStream — multi-chunk body', () => {
  it('yields each body frame decoded per its encoding, in order, then ends on the sentinel', async () => {
    const reg = new FakeRegistry();
    reg.register('r1', new NoopWS(), hello());
    const state = reg.openRequest('r1', 'req1');
    const stream = new TunneledByteStream(reg, 'r1', 'req1', state);

    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: 'req1', status: 200 });
    // A utf-8 chunk and a base64 binary chunk both reassemble correctly.
    reg.routeResponseFrame('r1', {
      kind: FrameKind.ResponseBody,
      id: 'req1',
      body: 'first:',
      encoding: 'utf-8',
    });
    reg.routeResponseFrame('r1', {
      kind: FrameKind.ResponseBody,
      id: 'req1',
      body: Buffer.from(new Uint8Array([0x00, 0xff, 0x10])).toString('base64'),
      encoding: 'base64',
    });
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseEnd, id: 'req1' });

    const out = await drainStream(stream);
    const expectedHead = new TextEncoder().encode('first:');
    const expected = new Uint8Array(expectedHead.length + 3);
    expected.set(expectedHead, 0);
    expected.set(new Uint8Array([0x00, 0xff, 0x10]), expectedHead.length);
    expect(Array.from(out)).toEqual(Array.from(expected));
    expect(state.session.inFlight.has('req1')).toBe(false);
  });
});

// ── TunnelTransport.close ────────────────────────────────

describe('TunnelTransport.handleRequest — caller abort (AbortSignal)', () => {
  it('an abort while awaiting the head cancels the runner, rejects, and closes the slot', async () => {
    // Review [P1] head-wait cancellation: without the signal, a request whose
    // head never arrives can settle only when the tunnel deregisters — a live
    // runner answering pings pins the slot forever, and the caller holds
    // neither the stream nor the private reqId with which to cancel.
    const reg = new FakeRegistry();
    const session0 = reg.register('r1', new NoopWS(), hello());
    const sent: string[] = [];
    const realSend = session0.ws.sendText.bind(session0.ws);
    session0.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
      await realSend(data);
    };
    const transport = new TunnelTransport(reg, 'r1');

    const ac = new AbortController();
    const pending = transport.handleRequest({ ...makeRequest(), signal: ac.signal });
    await tick();
    const reqId = session0.inFlight.keys().next().value as string;

    ac.abort();
    await expect(pending).rejects.toBeInstanceOf(TunnelRequestAbortedError);
    await tick();

    // The runner was told to stop, and the slot is gone.
    const cancels = sent
      .map((t) => decodeFrame(t))
      .filter((f) => f.kind === FrameKind.RequestCancel);
    expect(cancels).toHaveLength(1);
    expect(cancels[0]).toMatchObject({ id: reqId, reason: 'client_disconnected' });
    expect(session0.inFlight.size).toBe(0);
  });

  it('a pre-aborted signal short-circuits before any frame is sent', async () => {
    const reg = new FakeRegistry();
    const session0 = reg.register('r1', new NoopWS(), hello());
    const sent: string[] = [];
    session0.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
    };
    const transport = new TunnelTransport(reg, 'r1');

    const ac = new AbortController();
    ac.abort();
    await expect(
      transport.handleRequest({ ...makeRequest(), signal: ac.signal }),
    ).rejects.toBeInstanceOf(TunnelRequestAbortedError);
    expect(sent).toHaveLength(0);
    expect(session0.inFlight.size).toBe(0);
  });
});

describe('TunneledByteStream — early consumer exit', () => {
  it('an ordinary `for await … break` sends request.cancel before closing the slot', async () => {
    // Review [P2]: the finally used to close the slot WITHOUT cancelling, and
    // a later stream.close() could not repair it (requestIsOpen already
    // false) — the runner kept executing after correlation state was gone.
    const reg = new FakeRegistry();
    const session0 = reg.register('r1', new NoopWS(), hello());
    const sent: string[] = [];
    const realSend = session0.ws.sendText.bind(session0.ws);
    session0.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
      await realSend(data);
    };
    const transport = new TunnelTransport(reg, 'r1');

    const pending = transport.handleRequest(makeRequest());
    await tick();
    const reqId = session0.inFlight.keys().next().value as string;
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: reqId, status: 200 });
    reg.routeResponseFrame('r1', {
      kind: FrameKind.ResponseBody,
      id: reqId,
      body: 'chunk-1',
      encoding: 'utf-8',
    });
    const response = await pending;

    for await (const chunk of response.stream) {
      void chunk;
      break; // SSE client disconnected mid-stream.
    }

    const cancels = sent
      .map((t) => decodeFrame(t))
      .filter((f) => f.kind === FrameKind.RequestCancel);
    expect(cancels).toHaveLength(1);
    expect(cancels[0]).toMatchObject({ id: reqId, reason: 'client_disconnected' });
    expect(session0.inFlight.size).toBe(0);
  });

  it('normal completion (end sentinel) sends no cancel frame', async () => {
    const reg = new FakeRegistry();
    const session0 = reg.register('r1', new NoopWS(), hello());
    const sent: string[] = [];
    const realSend = session0.ws.sendText.bind(session0.ws);
    session0.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
      await realSend(data);
    };
    const transport = new TunnelTransport(reg, 'r1');

    const pending = transport.handleRequest(makeRequest());
    await tick();
    const reqId = session0.inFlight.keys().next().value as string;
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: reqId, status: 200 });
    reg.routeResponseFrame('r1', {
      kind: FrameKind.ResponseBody,
      id: reqId,
      body: 'all of it',
      encoding: 'utf-8',
    });
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseEnd, id: reqId });
    const response = await pending;
    await drainStream(response.stream);

    const cancels = sent
      .map((t) => decodeFrame(t))
      .filter((f) => f.kind === FrameKind.RequestCancel);
    expect(cancels).toHaveLength(0);
    expect(session0.inFlight.size).toBe(0);
  });
});

describe('TunneledByteStream — mid-stream abort and release idempotency', () => {
  it('a signal abort AFTER the head surfaces on the next dequeue', async () => {
    // The source documents "a mid-stream abort also surfaces on the next
    // dequeue" — pin it: consumer takes one chunk, the caller aborts, and the
    // following dequeue throws TunnelRequestAbortedError (with exactly one
    // cancel frame; the iterator finally must not add a duplicate).
    const reg = new FakeRegistry();
    const session0 = reg.register('r1', new NoopWS(), hello());
    const sent: string[] = [];
    const realSend = session0.ws.sendText.bind(session0.ws);
    session0.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
      await realSend(data);
    };
    const transport = new TunnelTransport(reg, 'r1');

    const ac = new AbortController();
    const pending = transport.handleRequest({ ...makeRequest(), signal: ac.signal });
    await tick();
    const reqId = session0.inFlight.keys().next().value as string;
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: reqId, status: 200 });
    reg.routeResponseFrame('r1', {
      kind: FrameKind.ResponseBody,
      id: reqId,
      body: 'first',
      encoding: 'utf-8',
    });
    const response = await pending;

    const chunks: string[] = [];
    await expect(
      (async () => {
        for await (const chunk of response.stream) {
          chunks.push(new TextDecoder().decode(chunk));
          ac.abort();
        }
      })(),
    ).rejects.toBeInstanceOf(TunnelRequestAbortedError);
    expect(chunks).toEqual(['first']);
    const cancels = sent
      .map((t) => decodeFrame(t))
      .filter((f) => f.kind === FrameKind.RequestCancel);
    expect(cancels).toHaveLength(1);
    expect(session0.inFlight.size).toBe(0);
  });

  it('close() after a fully drained stream is an inert no-op (release idempotency)', async () => {
    const reg = new FakeRegistry();
    const session0 = reg.register('r1', new NoopWS(), hello());
    const sent: string[] = [];
    const realSend = session0.ws.sendText.bind(session0.ws);
    session0.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
      await realSend(data);
    };
    const transport = new TunnelTransport(reg, 'r1');

    const pending = transport.handleRequest(makeRequest());
    await tick();
    const reqId = session0.inFlight.keys().next().value as string;
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: reqId, status: 200 });
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseEnd, id: reqId });
    const response = await pending;
    await drainStream(response.stream);

    await response.stream.close();
    await response.stream.close();
    const cancels = sent
      .map((t) => decodeFrame(t))
      .filter((f) => f.kind === FrameKind.RequestCancel);
    expect(cancels).toHaveLength(0);
    expect(session0.inFlight.size).toBe(0);
  });
});

describe('BoundedResponseBodyQueue + pushResponseBody — buffered-byte cap', () => {
  it('overflow WAKES a consumer already blocked in get() — the production slow-consumer shape', async () => {
    // Deleting the end() wake inside pushResponseBody would pass the rest of
    // the suite but hang this consumer forever: it is parked in get() with an
    // empty queue when a single over-cap frame arrives.
    const reg = new FakeRegistry();
    const session0 = reg.register('r1', new NoopWS(), hello());
    const transport = new TunnelTransport(reg, 'r1');
    const pending = transport.handleRequest(makeRequest());
    await tick();
    const reqId = session0.inFlight.keys().next().value as string;
    const state = session0.inFlight.get(reqId)!;
    state.bodyQueue = new BoundedResponseBodyQueue(4);
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: reqId, status: 200 });
    const response = await pending;

    // Park the consumer FIRST (empty queue), then overflow.
    const consuming = (async () => {
      for await (const chunk of response.stream) {
        void chunk;
      }
    })();
    await tick();
    expect(
      pushResponseBody(state, { kind: FrameKind.ResponseBody, id: reqId, body: 'toolarge' }),
    ).toBe('overflow');
    await expect(consuming).rejects.toBeInstanceOf(ResponseBufferOverflowError);
    expect(session0.inFlight.size).toBe(0);
  });

  it('accounting is on BUFFERED bytes: a draining consumer can stream past the cap', async () => {
    const q = new BoundedResponseBodyQueue(10);
    const frame = (body: string) => ({ kind: FrameKind.ResponseBody, id: 'r', body }) as never;
    expect(q.tryPut(frame('123456'))).toBe(true);
    await q.get(); // releases 6 bytes
    expect(q.tryPut(frame('123456'))).toBe(true);
    expect(q.didOverflow).toBe(false);
  });

  it('latches on overflow and drops every later frame', () => {
    const q = new BoundedResponseBodyQueue(4);
    const frame = (body: string) => ({ kind: FrameKind.ResponseBody, id: 'r', body }) as never;
    expect(q.tryPut(frame('12345'))).toBe(false);
    expect(q.didOverflow).toBe(true);
    expect(q.tryPut(frame('1'))).toBe(false);
  });

  it('overflow aborts the one request with ResponseBufferOverflowError and cancels the runner', async () => {
    // Review [P1] unbounded buffering: put() never blocks, so without the cap
    // a runner streaming faster than the HTTP consumer grows the shared
    // registry heap without bound. Overflow must degrade ONE request, not the
    // replica — and must tell the runner to stop producing.
    const reg = new FakeRegistry();
    const session0 = reg.register('r1', new NoopWS(), hello());
    const sent: string[] = [];
    const realSend = session0.ws.sendText.bind(session0.ws);
    session0.ws.sendText = async (data: string): Promise<void> => {
      sent.push(data);
      await realSend(data);
    };
    const transport = new TunnelTransport(reg, 'r1');

    const pending = transport.handleRequest(makeRequest());
    await tick();
    const reqId = session0.inFlight.keys().next().value as string;
    const state = session0.inFlight.get(reqId)!;
    // Shrink the cap for the test; production uses TUNNEL_MAX_BUFFERED_RESPONSE_BYTES.
    state.bodyQueue = new BoundedResponseBodyQueue(8);
    reg.routeResponseFrame('r1', { kind: FrameKind.ResponseHead, id: reqId, status: 200 });
    const response = await pending;

    expect(
      pushResponseBody(state, { kind: FrameKind.ResponseBody, id: reqId, body: 'sixchr' }),
    ).toBe('buffered');
    expect(
      pushResponseBody(state, { kind: FrameKind.ResponseBody, id: reqId, body: 'toolarge' }),
    ).toBe('overflow');
    // A delivery AFTER the latch is inert: still 'overflow', and the cancel
    // count asserted below stays exactly 1 — a registry that keeps routing
    // must not re-fire cancels or queue extra end sentinels per frame.
    expect(pushResponseBody(state, { kind: FrameKind.ResponseBody, id: reqId, body: 'more' })).toBe(
      'overflow',
    );
    await tick();

    // The consumer sees the overflow as a thrown abort. Per the iterator's
    // documented contract, a chunk already queued when the abort lands is
    // DISCARDED rather than delivered — so no chunks arrive here. The runner
    // received a buffer_overflow cancel.
    const chunks: string[] = [];
    await expect(
      (async () => {
        for await (const chunk of response.stream) {
          chunks.push(new TextDecoder().decode(chunk));
        }
      })(),
    ).rejects.toBeInstanceOf(ResponseBufferOverflowError);
    expect(chunks).toEqual([]);
    const cancels = sent
      .map((t) => decodeFrame(t))
      .filter((f) => f.kind === FrameKind.RequestCancel);
    expect(cancels).toHaveLength(1);
    expect(cancels[0]).toMatchObject({ id: reqId, reason: 'buffer_overflow' });
    expect(session0.inFlight.size).toBe(0);
  });
});

describe('TunnelTransport.close', () => {
  it('is a safe no-op', async () => {
    const reg = new FakeRegistry();
    const transport = new TunnelTransport(reg, 'r1');
    await expect(transport.close()).resolves.toBeUndefined();
  });
});

// ── async primitive sanity (Deferred / AsyncQueue) ───────
//
// These small real primitives are a one-shot result slot and an async FIFO.
// They underpin the framing semantics, so the framing tests above already
// exercise them; these focused cases pin their core contract.

describe('Deferred', () => {
  it('resolves once and reports done', async () => {
    const d = new Deferred<number>();
    expect(d.done).toBe(false);
    d.resolve(7);
    expect(d.done).toBe(true);
    await expect(d.promise).resolves.toBe(7);
    // Second resolve is ignored (mirrors "if not future.done()").
    d.resolve(9);
    await expect(d.promise).resolves.toBe(7);
  });

  it('rejects and reports done', async () => {
    const d = new Deferred<number>();
    const err = new Error('boom');
    d.reject(err);
    expect(d.done).toBe(true);
    await expect(d.promise).rejects.toBe(err);
  });
});

describe('AsyncQueue', () => {
  it('delivers items put before get (FIFO)', async () => {
    const q = new AsyncQueue<number>();
    q.put(1);
    q.put(2);
    expect(await q.get()).toBe(1);
    expect(await q.get()).toBe(2);
  });

  it('wakes a waiting get() when an item is put later', async () => {
    const q = new AsyncQueue<string>();
    const pending = q.get();
    q.put('late');
    expect(await pending).toBe('late');
  });

  it('preserves order across interleaved waiting getters', async () => {
    const q = new AsyncQueue<number>();
    const a = q.get();
    const b = q.get();
    q.put(1);
    q.put(2);
    expect(await a).toBe(1);
    expect(await b).toBe(2);
  });
});

describe('body decode parity', () => {
  it('decodeBody round-trips utf-8 bodies the transport streams', () => {
    expect(new TextDecoder().decode(decodeBody('hello', 'utf-8'))).toBe(utf8('hello'));
  });
});
