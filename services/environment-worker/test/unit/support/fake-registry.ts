// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// In-process fake of the registry worker-tunnel server, for worker client specs.
//
// Speaks the EXACT worker-frame protocol the real registry speaks (frames from the
// shared `@orca/harness-tunnel` contract), so a worker dialing this server is
// exercised against the same wire shape production uses. It is intentionally
// thin: it authenticates the Env Key handshake header + path (the worker is a
// CLIENT, so this is the server it must satisfy), receives the worker.hello, and
// then lets a test push launch/stop/ping frames and observe results + the
// one-way runner_exited report. It does NOT reproduce the registry's claim store
// or durable bookkeeping — those are the registry's own concern and are covered
// in registry-service specs; here the contract under test is the worker.

import { createServer, type Server } from 'node:http';
import { WebSocketServer, type WebSocket as WsSocket } from 'ws';
import {
  FrameKind,
  WorkerFrameKind,
  decodeFrame,
  decodeWorkerFrame,
  encodeWorkerFrame,
  type WorkerCreateDirResult,
  type WorkerCreateWorktreeResult,
  type WorkerFrame,
  type WorkerHelloFrame,
  type WorkerLaunchResult,
  type WorkerListDirResult,
  type WorkerRemoveWorktreeResult,
  type WorkerRunnerExitedFrame,
  type WorkerStatResult,
  type WorkerStopResult,
} from '@orca/harness-tunnel';

/** The dedicated handshake header carrying the worker's Env Key. */
const ENVIRONMENT_KEY_HEADER = 'x-orca-environment-key';
/** WS close code for an Env-Key / origin auth refusal (mirrors the real route). */
const UNAUTHENTICATED_CLOSE_CODE = 4004;

export interface FakeRegistryOptions {
  /** Environment id the server accepts on its tunnel path. */
  environmentId: string;
  /** Env Key the worker must present in the dedicated header. */
  environmentKey: string;
}

/** A one-shot promise whose resolve is captured for external settling. */
class Deferred<T> {
  readonly promise: Promise<T>;
  resolve!: (value: T) => void;
  constructor() {
    this.promise = new Promise<T>((resolve) => {
      this.resolve = resolve;
    });
  }
}

/**
 * A connected worker as seen by the fake server: its handshake headers + hello,
 * plus helpers to drive launch / stop / ping over the live socket and to read
 * the worker's one-way runner_exited reports.
 */
export class LiveWorker {
  readonly envKeyHeader: string | undefined;
  readonly originHeader: string | undefined;
  readonly path: string;
  readonly hello: WorkerHelloFrame;
  private readonly socket: WsSocket;
  private readonly pendingLaunches = new Map<string, Deferred<WorkerLaunchResult>>();
  private readonly pendingStops = new Map<string, Deferred<WorkerStopResult>>();
  private readonly pendingStats = new Map<string, Deferred<WorkerStatResult>>();
  private readonly pendingListDirs = new Map<string, Deferred<WorkerListDirResult>>();
  private readonly pendingCreateDirs = new Map<string, Deferred<WorkerCreateDirResult>>();
  private readonly pendingCreateWorktrees = new Map<string, Deferred<WorkerCreateWorktreeResult>>();
  private readonly pendingRemoveWorktrees = new Map<string, Deferred<WorkerRemoveWorktreeResult>>();
  private readonly pendingPongs: Array<{ ts: number; deferred: Deferred<number> }> = [];
  private readonly exitReports: WorkerRunnerExitedFrame[] = [];
  private readonly exitWaiters: Array<Deferred<WorkerRunnerExitedFrame>> = [];
  /** Every worker-frame kind the worker has sent back, in order (for assertions). */
  private readonly seenWorkerFrameKinds: string[] = [];

  constructor(args: {
    socket: WsSocket;
    envKeyHeader: string | undefined;
    originHeader: string | undefined;
    path: string;
    hello: WorkerHelloFrame;
  }) {
    this.socket = args.socket;
    this.envKeyHeader = args.envKeyHeader;
    this.originHeader = args.originHeader;
    this.path = args.path;
    this.hello = args.hello;
    this.socket.on('message', (data, isBinary) => {
      if (isBinary) {
        return;
      }
      this.onText(typeof data === 'string' ? data : data.toString());
    });
  }

  private onText(raw: string): void {
    let frame: WorkerFrame | undefined;
    try {
      frame = decodeWorkerFrame(raw);
    } catch {
      frame = undefined;
    }
    if (frame !== undefined) {
      this.seenWorkerFrameKinds.push(frame.kind);
      this.routeHostFrame(frame);
      return;
    }
    // Not a worker frame: the worker answers registry pings with a runner-tunnel
    // pong on the same socket. Resolve the matching ping waiter.
    try {
      const runnerFrame = decodeFrame(raw);
      if (runnerFrame.kind === FrameKind.Pong) {
        const waiter = this.pendingPongs.shift();
        if (waiter !== undefined) {
          waiter.deferred.resolve(runnerFrame.ts);
        }
      }
    } catch {
      // ignore non-protocol noise
    }
  }

  private routeHostFrame(frame: WorkerFrame): void {
    switch (frame.kind) {
      case WorkerFrameKind.LaunchRunnerResult: {
        const waiter = this.pendingLaunches.get(frame.requestId);
        if (waiter !== undefined) {
          this.pendingLaunches.delete(frame.requestId);
          waiter.resolve({
            requestId: frame.requestId,
            status: frame.status,
            runnerId: frame.runnerId ?? null,
            error: frame.error ?? null,
            errorCode: frame.errorCode ?? null,
          });
        }
        return;
      }
      case WorkerFrameKind.StopRunnerResult: {
        const waiter = this.pendingStops.get(frame.requestId);
        if (waiter !== undefined) {
          this.pendingStops.delete(frame.requestId);
          waiter.resolve({
            requestId: frame.requestId,
            status: frame.status,
            error: frame.error ?? null,
          });
        }
        return;
      }
      case WorkerFrameKind.RunnerExited: {
        this.exitReports.push(frame);
        const waiter = this.exitWaiters.shift();
        if (waiter !== undefined) {
          waiter.resolve(frame);
        }
        return;
      }
      case WorkerFrameKind.StatResult: {
        const waiter = this.pendingStats.get(frame.requestId);
        if (waiter !== undefined) {
          this.pendingStats.delete(frame.requestId);
          waiter.resolve({
            requestId: frame.requestId,
            status: frame.status,
            exists: frame.exists ?? false,
            type: frame.type ?? null,
            canonicalPath: frame.canonicalPath ?? null,
            error: frame.error ?? null,
          });
        }
        return;
      }
      case WorkerFrameKind.ListDirResult: {
        const waiter = this.pendingListDirs.get(frame.requestId);
        if (waiter !== undefined) {
          this.pendingListDirs.delete(frame.requestId);
          waiter.resolve({
            requestId: frame.requestId,
            status: frame.status,
            entries: frame.entries ?? [],
            hasMore: frame.hasMore ?? false,
            error: frame.error ?? null,
          });
        }
        return;
      }
      case WorkerFrameKind.CreateDirResult: {
        const waiter = this.pendingCreateDirs.get(frame.requestId);
        if (waiter !== undefined) {
          this.pendingCreateDirs.delete(frame.requestId);
          waiter.resolve({
            requestId: frame.requestId,
            status: frame.status,
            path: frame.path ?? null,
            error: frame.error ?? null,
          });
        }
        return;
      }
      case WorkerFrameKind.CreateWorktreeResult: {
        const waiter = this.pendingCreateWorktrees.get(frame.requestId);
        if (waiter !== undefined) {
          this.pendingCreateWorktrees.delete(frame.requestId);
          waiter.resolve({
            requestId: frame.requestId,
            status: frame.status,
            worktreePath: frame.worktreePath ?? null,
            branch: frame.branch ?? null,
            error: frame.error ?? null,
          });
        }
        return;
      }
      case WorkerFrameKind.RemoveWorktreeResult: {
        const waiter = this.pendingRemoveWorktrees.get(frame.requestId);
        if (waiter !== undefined) {
          this.pendingRemoveWorktrees.delete(frame.requestId);
          waiter.resolve({
            requestId: frame.requestId,
            status: frame.status,
            error: frame.error ?? null,
          });
        }
        return;
      }
      default:
        // The worker only ever sends results + the one-way runner_exited report;
        // anything else is ignored.
        return;
    }
  }

  private send(text: string): void {
    this.socket.send(text);
  }

  /** Send a worker.launch_runner and await the worker's launch result. */
  launch(frame: {
    requestId: string;
    bindingToken: string;
    workspace: string;
    harness?: string | null;
  }): Promise<WorkerLaunchResult> {
    const deferred = new Deferred<WorkerLaunchResult>();
    this.pendingLaunches.set(frame.requestId, deferred);
    this.send(
      encodeWorkerFrame({
        kind: WorkerFrameKind.LaunchRunner,
        requestId: frame.requestId,
        bindingToken: frame.bindingToken,
        workspace: frame.workspace,
        harness: frame.harness ?? null,
      }),
    );
    return deferred.promise;
  }

  /** Send a worker.stop_runner and await the worker's stop result. */
  stop(requestId: string, runnerId: string): Promise<WorkerStopResult> {
    const deferred = new Deferred<WorkerStopResult>();
    this.pendingStops.set(requestId, deferred);
    this.send(encodeWorkerFrame({ kind: WorkerFrameKind.StopRunner, requestId, runnerId }));
    return deferred.promise;
  }

  /** Send a worker.stat and await the worker's stat result. */
  stat(requestId: string, path: string): Promise<WorkerStatResult> {
    const deferred = new Deferred<WorkerStatResult>();
    this.pendingStats.set(requestId, deferred);
    this.send(encodeWorkerFrame({ kind: WorkerFrameKind.Stat, requestId, path }));
    return deferred.promise;
  }

  /** Send a worker.list_dir and await the worker's list-dir result. */
  listDir(frame: {
    requestId: string;
    path: string;
    limit?: number;
    after?: string | null;
    before?: string | null;
  }): Promise<WorkerListDirResult> {
    const deferred = new Deferred<WorkerListDirResult>();
    this.pendingListDirs.set(frame.requestId, deferred);
    this.send(
      encodeWorkerFrame({
        kind: WorkerFrameKind.ListDir,
        requestId: frame.requestId,
        path: frame.path,
        limit: frame.limit ?? 20,
        after: frame.after ?? null,
        before: frame.before ?? null,
      }),
    );
    return deferred.promise;
  }

  /** Send a worker.create_dir and await the worker's create-dir result. */
  createDir(requestId: string, path: string): Promise<WorkerCreateDirResult> {
    const deferred = new Deferred<WorkerCreateDirResult>();
    this.pendingCreateDirs.set(requestId, deferred);
    this.send(encodeWorkerFrame({ kind: WorkerFrameKind.CreateDir, requestId, path }));
    return deferred.promise;
  }

  /** Send a worker.create_worktree and await the worker's create-worktree result. */
  createWorktree(frame: {
    requestId: string;
    repoPath: string;
    branchName: string;
    baseBranch?: string | null;
  }): Promise<WorkerCreateWorktreeResult> {
    const deferred = new Deferred<WorkerCreateWorktreeResult>();
    this.pendingCreateWorktrees.set(frame.requestId, deferred);
    this.send(
      encodeWorkerFrame({
        kind: WorkerFrameKind.CreateWorktree,
        requestId: frame.requestId,
        repoPath: frame.repoPath,
        branchName: frame.branchName,
        baseBranch: frame.baseBranch ?? null,
      }),
    );
    return deferred.promise;
  }

  /** Send a worker.remove_worktree and await the worker's remove-worktree result. */
  removeWorktree(frame: {
    requestId: string;
    worktreePath: string;
    branch?: string | null;
    deleteBranch?: boolean;
  }): Promise<WorkerRemoveWorktreeResult> {
    const deferred = new Deferred<WorkerRemoveWorktreeResult>();
    this.pendingRemoveWorktrees.set(frame.requestId, deferred);
    this.send(
      encodeWorkerFrame({
        kind: WorkerFrameKind.RemoveWorktree,
        requestId: frame.requestId,
        worktreePath: frame.worktreePath,
        branch: frame.branch ?? null,
        deleteBranch: frame.deleteBranch ?? false,
      }),
    );
    return deferred.promise;
  }

  /**
   * Push a frame the worker's dispatch does not act on — a RESULT frame (here a
   * `worker.stat_result`), which only ever flows worker→registry. The worker's
   * forward-compat contract is to drop an inbound frame it has no handler for:
   * no reply frame, no crash. Drives the dispatch else-fall-through branch now
   * that every request kind is serviced.
   */
  sendUnhandledFrame(requestId: string): void {
    this.send(
      encodeWorkerFrame({
        kind: WorkerFrameKind.StatResult,
        requestId,
        status: 'ok',
        exists: false,
        type: null,
        canonicalPath: null,
        error: null,
      }),
    );
  }

  /** Snapshot of every worker-frame kind the worker has sent back so far. */
  sentWorkerFrameKinds(): string[] {
    return [...this.seenWorkerFrameKinds];
  }

  /** Send a runner-tunnel ping and resolve with the echoed pong `ts`. */
  pingAndAwaitPong(ts: number): Promise<number> {
    const deferred = new Deferred<number>();
    this.pendingPongs.push({ ts, deferred });
    this.send(JSON.stringify({ kind: FrameKind.Ping, ts }));
    return deferred.promise;
  }

  /** Resolve with the next worker.runner_exited report (or the next already seen). */
  nextRunnerExited(): Promise<WorkerRunnerExitedFrame> {
    const queued = this.exitReports.shift();
    if (queued !== undefined) {
      return Promise.resolve(queued);
    }
    const deferred = new Deferred<WorkerRunnerExitedFrame>();
    this.exitWaiters.push(deferred);
    return deferred.promise;
  }

  /** All runner_exited reports seen so far (snapshot). */
  runnerExitedReports(): WorkerRunnerExitedFrame[] {
    return [...this.exitReports];
  }
}

/** A self-contained `ws`-based fake of the registry worker-tunnel endpoint. */
export class FakeRegistryWorkerTunnel {
  private readonly http: Server;
  private readonly wss: WebSocketServer;
  private readonly opts: FakeRegistryOptions;
  private port = 0;
  private readonly workerQueue: LiveWorker[] = [];
  private readonly workerWaiters: Array<Deferred<LiveWorker>> = [];

  constructor(opts: FakeRegistryOptions) {
    this.opts = opts;
    this.http = createServer();
    this.wss = new WebSocketServer({ noServer: true });
    this.http.on('upgrade', (req, socket, head) => {
      const path = (req.url ?? '').split('?')[0] ?? '';
      const envKey = headerValue(req.headers[ENVIRONMENT_KEY_HEADER]);
      const origin = headerValue(req.headers.origin);
      // Authenticate the Env Key + exact path BEFORE accepting, exactly as the
      // real route does (fail closed with the 4004 close code on a mismatch).
      const ok =
        path === `/v1/tunnels/environments/${this.opts.environmentId}` &&
        envKey === this.opts.environmentKey;
      this.wss.handleUpgrade(req, socket, head, (ws) => {
        if (!ok) {
          ws.close(UNAUTHENTICATED_CLOSE_CODE, 'unauthenticated');
          return;
        }
        this.onAccepted(ws, { path, envKey, origin });
      });
    });
  }

  private onAccepted(
    ws: WsSocket,
    handshake: { path: string; envKey: string | undefined; origin: string | undefined },
  ): void {
    // First frame must be the worker.hello; capture it and surface the LiveWorker.
    ws.once('message', (data, isBinary) => {
      if (isBinary) {
        ws.close();
        return;
      }
      let hello: WorkerHelloFrame;
      try {
        const frame = decodeWorkerFrame(typeof data === 'string' ? data : data.toString());
        if (frame.kind !== WorkerFrameKind.Hello) {
          ws.close();
          return;
        }
        hello = frame;
      } catch {
        ws.close();
        return;
      }
      const worker = new LiveWorker({
        socket: ws,
        envKeyHeader: handshake.envKey,
        originHeader: handshake.origin,
        path: handshake.path,
        hello,
      });
      const waiter = this.workerWaiters.shift();
      if (waiter !== undefined) {
        waiter.resolve(worker);
      } else {
        this.workerQueue.push(worker);
      }
    });
  }

  /** Bind on an ephemeral loopback port. */
  listen(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.http.listen(0, '127.0.0.1', () => {
        const addr = this.http.address();
        if (addr !== null && typeof addr !== 'string') {
          this.port = addr.port;
        }
        resolve();
      });
    });
  }

  /** The `ws://127.0.0.1:<port>` base URL the worker should dial. */
  baseUrl(): string {
    return `ws://127.0.0.1:${this.port}`;
  }

  /** Resolve with the next worker that connects + sends hello. */
  nextWorker(): Promise<LiveWorker> {
    const queued = this.workerQueue.shift();
    if (queued !== undefined) {
      return Promise.resolve(queued);
    }
    const deferred = new Deferred<LiveWorker>();
    this.workerWaiters.push(deferred);
    return deferred.promise;
  }

  /** Tear down every socket + the HTTP server. */
  close(): Promise<void> {
    return new Promise<void>((resolve) => {
      for (const client of this.wss.clients) {
        try {
          client.terminate();
        } catch {
          // ignore
        }
      }
      this.wss.close(() => {
        this.http.close(() => resolve());
      });
    });
  }
}

function headerValue(raw: string | string[] | undefined): string | undefined {
  return Array.isArray(raw) ? raw[0] : raw;
}
