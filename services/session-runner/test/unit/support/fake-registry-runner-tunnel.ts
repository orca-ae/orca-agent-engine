// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// In-process fake of the registry runner-tunnel server, for runner-client specs.
//
// The runner is the CLIENT that dials `/v1/tunnels/runners/:runnerId`; this is the
// SERVER it must satisfy. It speaks the EXACT runner-tunnel wire the real registry
// route speaks (frames from `@orca/harness-tunnel`): it authenticates the PUBLIC
// path + the `X-Orca-Runner-Tunnel-Token` header (token-binding correlation) + the
// internal WS origin (CSWSH guard) exactly as the production route does, receives
// the runner's hello, and then lets a test PUSH request / request.cancel / ping /
// ws.* frames and reassemble the runner's framed responses. It does NOT reproduce
// the registry's transcript/claim bookkeeping — those are the registry's own
// concern (covered in registry-service specs); here the contract under test is the
// runner's serve loop.

import { createServer, type Server } from 'node:http';
import { WebSocketServer, type WebSocket as WsServerSocket } from 'ws';
import {
  FrameKind,
  decodeFrame,
  encodeFrame,
  decodeBody,
  RUNNER_TUNNEL_TOKEN_HEADER,
  INTERNAL_WS_ORIGIN,
  tokenBoundRunnerId,
  type Frame,
  type HelloFrame,
} from '@orca/harness-tunnel';
import { RouteDispatcher, type DispatchResponse } from '../../../src/tunnel/request-dispatch.js';

/** A one-shot promise whose resolve/reject is captured for external settling. */
export class Deferred<T> {
  readonly promise: Promise<T>;
  settled = false;
  private settle!: (value: T) => void;
  private fail!: (reason: unknown) => void;
  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.settle = resolve;
      this.fail = reject;
    });
  }
  resolve = (value: T): void => {
    if (this.settled) return;
    this.settled = true;
    this.settle(value);
  };
  reject = (reason: unknown): void => {
    if (this.settled) return;
    this.settled = true;
    this.fail(reason);
  };
}

/** A frame the runner sent back, decoded. */
type SeenFrame = Frame;

/** One inbound message on a tunneled WS channel as the fake registry peer sees it. */
type WsChannelItem = { kind: 'text' | 'bytes' | 'close'; data: string | Uint8Array; code?: number };

type WsRawData = string | Buffer;

/**
 * One accepted runner tunnel as seen by the fake registry peer.
 *
 * Captures the handshake (path / token header / origin) + the runner's hello, and
 * exposes helpers to PUSH a request (and reassemble the framed response), push a
 * ping, open a tunneled WS channel, and read every frame the runner sent.
 */
export class LiveRunner {
  readonly path: string;
  readonly tokenHeader: string | undefined;
  readonly originHeader: string | undefined;
  readonly hello: HelloFrame;
  private readonly socket: WsServerSocket;
  /** Per-reqId response reassembly. */
  private readonly responses = new Map<
    string,
    {
      head?: { status: number; headers: [string, string][] };
      chunks: Uint8Array[];
      done: Deferred<void>;
    }
  >();
  /** Per-chId WS-channel inbound. */
  private readonly wsChannels = new Map<
    string,
    {
      accepted: Deferred<void>;
      inbound: WsChannelItem[];
      waiters: Array<(item: WsChannelItem) => void>;
      closed: Deferred<{ code: number; reason: string }>;
    }
  >();
  /** Every decoded frame the runner sent, in order. */
  readonly seen: SeenFrame[] = [];
  private nextReqId = 0;

  constructor(args: {
    socket: WsServerSocket;
    path: string;
    tokenHeader: string | undefined;
    originHeader: string | undefined;
    hello: HelloFrame;
  }) {
    this.socket = args.socket;
    this.path = args.path;
    this.tokenHeader = args.tokenHeader;
    this.originHeader = args.originHeader;
    this.hello = args.hello;
    this.socket.on('message', (data, isBinary) => {
      if (isBinary) return;
      this.onFrame(typeof data === 'string' ? data : data.toString());
    });
  }

  private onFrame(raw: string): void {
    let frame: Frame;
    try {
      frame = decodeFrame(raw);
    } catch {
      return;
    }
    this.seen.push(frame);
    switch (frame.kind) {
      case FrameKind.ResponseHead: {
        const slot = this.responses.get(frame.id);
        if (slot !== undefined) {
          slot.head = { status: frame.status, headers: [...(frame.headers ?? [])] };
        }
        return;
      }
      case FrameKind.ResponseBody: {
        const slot = this.responses.get(frame.id);
        if (slot !== undefined) {
          slot.chunks.push(decodeBody(frame.body, frame.encoding ?? 'utf-8'));
        }
        return;
      }
      case FrameKind.ResponseEnd: {
        const slot = this.responses.get(frame.id);
        if (slot !== undefined) {
          slot.done.resolve();
        }
        return;
      }
      case FrameKind.WsFrame: {
        const ch = this.wsChannels.get(frame.chId);
        if (ch === undefined) return;
        const item: WsChannelItem =
          (frame.encoding ?? 'utf-8') === 'base64'
            ? { kind: 'bytes', data: new Uint8Array(Buffer.from(frame.data, 'base64')) }
            : { kind: 'text', data: frame.data };
        const waiter = ch.waiters.shift();
        if (waiter !== undefined) waiter(item);
        else ch.inbound.push(item);
        // The runner's first send on a channel implies it accepted the attach.
        if (!ch.accepted.settled) ch.accepted.resolve();
        return;
      }
      case FrameKind.WsClose: {
        const ch = this.wsChannels.get(frame.chId);
        if (ch === undefined) return;
        ch.closed.resolve({ code: frame.code ?? 1000, reason: frame.reason ?? '' });
        return;
      }
      default:
        return;
    }
  }

  private send(frame: Frame): void {
    this.socket.send(encodeFrame(frame));
  }

  /** Push a framed request to the runner and reassemble its response. */
  async request(args: {
    method: string;
    path: string;
    headers?: [string, string][];
    body?: string | null;
    queryString?: string;
  }): Promise<{ status: number; headers: [string, string][]; body: string }> {
    const id = `req_${this.nextReqId++}`;
    const slot = { chunks: [] as Uint8Array[], done: new Deferred<void>() } as {
      head?: { status: number; headers: [string, string][] };
      chunks: Uint8Array[];
      done: Deferred<void>;
    };
    this.responses.set(id, slot);
    this.send({
      kind: FrameKind.Request,
      id,
      method: args.method,
      path: args.path,
      queryString: args.queryString ?? '',
      headers: args.headers ?? [],
      body: args.body ?? null,
      encoding: 'utf-8',
      stream: true,
    });
    await slot.done.promise;
    const head = slot.head ?? { status: 0, headers: [] };
    return {
      status: head.status,
      headers: head.headers,
      body: Buffer.concat(slot.chunks.map((c) => Buffer.from(c))).toString('utf8'),
    };
  }

  /** Send a request frame WITHOUT awaiting; returns the reqId + a head waiter. */
  startRequest(args: {
    method: string;
    path: string;
    headers?: [string, string][];
    body?: string | null;
  }): { id: string; headSeen: () => Promise<void>; ended: Promise<void> } {
    const id = `req_${this.nextReqId++}`;
    const headDeferred = new Deferred<void>();
    const slot = {
      chunks: [] as Uint8Array[],
      done: new Deferred<void>(),
    } as {
      head?: { status: number; headers: [string, string][] };
      chunks: Uint8Array[];
      done: Deferred<void>;
    };
    this.responses.set(id, slot);
    const poll = setInterval(() => {
      if (slot.head !== undefined && !headDeferred.settled) {
        headDeferred.resolve();
      }
    }, 2);
    void slot.done.promise.then(() => clearInterval(poll));
    this.send({
      kind: FrameKind.Request,
      id,
      method: args.method,
      path: args.path,
      queryString: '',
      headers: args.headers ?? [],
      body: args.body ?? null,
      encoding: 'utf-8',
      stream: true,
    });
    return { id, headSeen: () => headDeferred.promise, ended: slot.done.promise };
  }

  /** Read the reassembled body for an in-flight request id so far. */
  bodySoFar(id: string): string {
    const slot = this.responses.get(id);
    if (slot === undefined) return '';
    return Buffer.concat(slot.chunks.map((c) => Buffer.from(c))).toString('utf8');
  }

  /** Send a request.cancel for an in-flight request. */
  cancel(id: string): void {
    this.send({ kind: FrameKind.RequestCancel, id, reason: 'client_disconnected' });
  }

  /** Send a ping and resolve with the echoed pong ts. */
  pingAndAwaitPong(ts: number): Promise<number> {
    const deferred = new Deferred<number>();
    const onPong = (raw: WsRawData, isBinary: boolean): void => {
      if (isBinary) return;
      try {
        const frame = decodeFrame(typeof raw === 'string' ? raw : (raw as Buffer).toString());
        if (frame.kind === FrameKind.Pong && frame.ts === ts) {
          this.socket.off('message', onPong);
          deferred.resolve(frame.ts);
        }
      } catch {
        // ignore
      }
    };
    this.socket.on('message', onPong);
    this.send({ kind: FrameKind.Ping, ts });
    return deferred.promise;
  }

  /** Push a raw frame the registry would never normally send (negative tests). */
  sendRaw(frame: Frame): void {
    this.send(frame);
  }

  /**
   * Push an arbitrary raw text payload onto the socket WITHOUT frame-encoding it —
   * for exercising the runner's malformed-frame drop path (bad JSON, an unknown
   * `kind`, a missing required field). The real registry never sends this.
   */
  sendRawText(text: string): void {
    this.socket.send(text);
  }

  /** Open a tunneled WS channel and return a small driver for it. */
  openWsChannel(
    chId: string,
    path: string,
    queryString = '',
  ): {
    waitAccepted: () => Promise<void>;
    recv: () => Promise<WsChannelItem>;
    sendText: (text: string) => void;
    sendBytes: (bytes: Uint8Array) => void;
    close: (code?: number, reason?: string) => void;
    closed: () => Promise<{ code: number; reason: string }>;
  } {
    const ch = {
      accepted: new Deferred<void>(),
      inbound: [] as WsChannelItem[],
      waiters: [] as Array<(item: WsChannelItem) => void>,
      closed: new Deferred<{ code: number; reason: string }>(),
    };
    this.wsChannels.set(chId, ch);
    this.send({ kind: FrameKind.WsOpen, chId, path, queryString });
    return {
      waitAccepted: () => ch.accepted.promise,
      recv: () => {
        const queued = ch.inbound.shift();
        if (queued !== undefined) return Promise.resolve(queued);
        return new Promise<WsChannelItem>((resolve) => ch.waiters.push(resolve));
      },
      sendText: (text: string) =>
        this.send({ kind: FrameKind.WsFrame, chId, data: text, encoding: 'utf-8' }),
      sendBytes: (bytes: Uint8Array) =>
        this.send({
          kind: FrameKind.WsFrame,
          chId,
          data: Buffer.from(bytes).toString('base64'),
          encoding: 'base64',
        }),
      close: (code = 1000, reason = '') =>
        this.send({ kind: FrameKind.WsClose, chId, code, reason }),
      closed: () => ch.closed.promise,
    };
  }

  /** Force-close the underlying socket (simulate an ingress recycle / drop). */
  dropSocket(code?: number, reason?: string): void {
    this.socket.close(code, reason);
  }
}

/** One handshake the fake registry peer SAW (even rejected ones), for assertions. */
export interface SeenHandshake {
  path: string;
  token: string | undefined;
  origin: string | undefined;
  accepted: boolean;
}

/** A self-contained `ws` fake of the registry runner-tunnel endpoint. */
export class FakeRegistryRunnerTunnel {
  private readonly http: Server;
  private readonly wss: WebSocketServer;
  private port = 0;
  private readonly runnerQueue: LiveRunner[] = [];
  private readonly runnerWaiters: Array<Deferred<LiveRunner>> = [];
  /** Records each handshake the server SAW, even rejected ones (for assertions). */
  readonly handshakes: SeenHandshake[] = [];
  /** When set, the next N upgrades are rejected with this HTTP status. */
  private rejectStatus: number | undefined;
  private rejectCount = 0;

  constructor(
    private readonly opts: {
      runnerId: string;
      bindingToken: string;
    },
  ) {
    this.http = createServer((_req, res) => {
      // A non-upgrade plain HTTP response path (unused by the WS client).
      res.statusCode = 426;
      res.end();
    });
    this.wss = new WebSocketServer({ noServer: true });
    this.http.on('upgrade', (req, socket, head) => {
      const path = (req.url ?? '').split('?')[0] ?? '';
      const token = headerValue(req.headers[RUNNER_TUNNEL_TOKEN_HEADER.toLowerCase()]);
      const origin = headerValue(req.headers.origin);

      if (this.rejectCount > 0 && this.rejectStatus !== undefined) {
        this.rejectCount -= 1;
        const status = this.rejectStatus;
        this.handshakes.push({ path, token, origin, accepted: false });
        socket.write(`HTTP/1.1 ${status} ${statusText(status)}\r\nConnection: close\r\n\r\n`);
        socket.destroy();
        return;
      }

      // Authenticate EXACTLY as the production route: the path must be the public
      // runner-tunnel path for this runner id, the token must derive the runner id
      // (token-binding correlation), and the origin must be the internal WS origin
      // (CSWSH guard for a non-browser client).
      const expectedPath = `/v1/tunnels/runners/${this.opts.runnerId}`;
      const tokenBinds = token !== undefined && tokenBoundRunnerId(token) === this.opts.runnerId;
      const originOk = origin === INTERNAL_WS_ORIGIN;
      const ok = path === expectedPath && tokenBinds && originOk;
      this.handshakes.push({ path, token, origin, accepted: ok });
      this.wss.handleUpgrade(req, socket, head, (ws) => {
        if (!ok) {
          // Refuse EXACTLY as the production route does: close IMMEDIATELY with
          // the runner-binding 4004, before any tunnel protocol I/O and without
          // ever exchanging a hello. Every pre-hello refusal in
          // `registry-service-ts/src/api/runner-tunnel.routes.ts` — the CSWSH
          // origin guard, the token-binding correlation gate, and the owner
          // fail-closed gate, the last two of which close 4004 — runs
          // synchronously in the handler, ahead of its `await inbound.get()` for
          // the hello. The Fastify socket is already upgraded by then, so that
          // route's documented "refuse before accept" means exactly "close
          // immediately, never exchange a hello".
          //
          // Deferring the close until the client's hello would test a transition
          // the real registry can never produce: post-hello it closes 4001 (not a
          // hello frame) or 4002 (version skew), never 4004.
          ws.close(4004, 'runner_id does not match tunnel token');
          return;
        }
        this.onAccepted(ws, { path, token, origin });
      });
    });
  }

  private onAccepted(
    ws: WsServerSocket,
    handshake: { path: string; token: string | undefined; origin: string | undefined },
  ): void {
    ws.once('message', (data, isBinary) => {
      if (isBinary) {
        ws.close(4001, 'expected hello frame');
        return;
      }
      let hello: HelloFrame;
      try {
        const frame = decodeFrame(typeof data === 'string' ? data : data.toString());
        if (frame.kind !== FrameKind.Hello) {
          ws.close(4001, 'expected hello frame');
          return;
        }
        hello = frame;
      } catch {
        ws.close(4001, 'expected hello frame');
        return;
      }
      const runner = new LiveRunner({
        socket: ws,
        path: handshake.path,
        tokenHeader: handshake.token,
        originHeader: handshake.origin,
        hello,
      });
      const waiter = this.runnerWaiters.shift();
      if (waiter !== undefined) waiter.resolve(runner);
      else this.runnerQueue.push(runner);
    });
  }

  /** Reject the next `count` upgrades with `status` (a non-101 response). */
  rejectNextUpgrades(status: number, count: number): void {
    this.rejectStatus = status;
    this.rejectCount = count;
  }

  listen(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.http.listen(0, '127.0.0.1', () => {
        const addr = this.http.address();
        if (addr !== null && typeof addr !== 'string') this.port = addr.port;
        resolve();
      });
    });
  }

  /** The `ws://127.0.0.1:<port>` BASE url the runner dials (no tunnel path). */
  baseUrl(): string {
    return `ws://127.0.0.1:${this.port}`;
  }

  /** Resolve with the next runner that connects + sends hello. */
  nextRunner(): Promise<LiveRunner> {
    const queued = this.runnerQueue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    const deferred = new Deferred<LiveRunner>();
    this.runnerWaiters.push(deferred);
    return deferred.promise;
  }

  close(): Promise<void> {
    return new Promise<void>((resolve) => {
      for (const client of this.wss.clients) {
        try {
          client.terminate();
        } catch {
          // ignore
        }
      }
      this.wss.close(() => this.http.close(() => resolve()));
    });
  }
}

function headerValue(raw: string | string[] | undefined): string | undefined {
  return Array.isArray(raw) ? raw[0] : raw;
}

function statusText(status: number): string {
  if (status === 401) return 'Unauthorized';
  if (status === 403) return 'Forbidden';
  if (status === 502) return 'Bad Gateway';
  if (status === 503) return 'Service Unavailable';
  return 'Error';
}

/** A dispatcher with an NDJSON streaming handler scripted per request path. */
export function ndjsonDispatcher(
  routes: Record<string, (body: string, signal: AbortSignal) => DispatchResponse>,
): RouteDispatcher {
  const dispatcher = new RouteDispatcher();
  for (const [path, make] of Object.entries(routes)) {
    dispatcher.register('POST', path, async (req, signal) =>
      make(Buffer.from(req.body).toString('utf8'), signal),
    );
  }
  return dispatcher;
}

/** A 200 NDJSON response that yields the given lines (one per body frame) then ends. */
export function ndjsonResponse(lines: unknown[]): DispatchResponse {
  return {
    status: 200,
    headers: [['content-type', 'application/x-ndjson']],
    body: (async function* () {
      for (const line of lines) {
        yield new TextEncoder().encode(`${JSON.stringify(line)}\n`);
      }
    })(),
  };
}
