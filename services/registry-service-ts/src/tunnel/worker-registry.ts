// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// In-memory, per-replica registry of live worker (environment-worker) tunnels.
//
// Each registry replica maintains one WorkerRegistry tracking environment workers
// with an active worker tunnel on THIS replica. The durable, cross-replica source
// of truth for which environment is claimed by which replica is the
// `environment_claims` table (see `src/domain/environment-claims.ts`); this
// registry only tracks which workers are live *here* and owns the per-connection
// frame plumbing.
//
// It is simpler than the runner `TunnelRegistry` because the worker tunnel carries
// only control frames (launch/stop a runner) and the filesystem ops that back
// workspace selection — never HTTP request/response traffic, so there are no
// per-request reassembly queues. A connection holds the outbound send queue
// (drained by the route's sender loop), the per-`request_id` pending-result
// waiter maps the receive loop resolves into, the resolved owner (the tenant
// workspace, used to scope a `worker.runner_exited` report), and the last-frame
// timestamp the ping watchdog reads.
//
// Concurrency model: Node is single-threaded with one event loop, so registry
// mutations need no lock and the outbound queue is loop-safe — every caller (the
// route's loops, a server-side launch caller) runs on the one loop. The surface
// is: register (newest-wins with queue poison), deregister, get, online tracking,
// and a send-text guard that refuses a replaced connection.

import {
  AsyncQueue,
  type WorkerConnection as WorkerConnectionSeam,
  type WorkerCreateDirResult,
  type WorkerCreateWorktreeResult,
  type WorkerHelloFrame,
  type WorkerLaunchResult,
  type WorkerListDirResult,
  type WorkerPendingRequest,
  type WorkerRegistry as WorkerRegistrySeam,
  type WorkerRemoveWorktreeResult,
  type WorkerStatResult,
  type WorkerStopResult,
  type WorkerWebSocket,
} from '@orca/harness-tunnel';

/**
 * Raised by {@link WorkerRegistry.sendText} when the target connection is no longer
 * the registry's current generation for its worker id — it was replaced by a newer
 * tunnel (newest-wins) or deregistered. A server-side caller that still holds a
 * stale {@link WorkerConnection} reference gets a clean failure rather than
 * enqueuing onto a poisoned queue whose sender has already stopped.
 *
 * The message names the worker id so the failure is diagnosable as a replaced-worker
 * send.
 */
export class WorkerConnectionReplacedError extends Error {
  constructor(workerId: string) {
    super(`worker ${JSON.stringify(workerId)} connection was replaced`);
    this.name = 'WorkerConnectionReplacedError';
  }
}

/**
 * Per-worker state while the tunnel is open.
 *
 * Satisfies the `@orca/harness-tunnel` {@link WorkerConnectionSeam} the
 * `WorkerTunnelServer` engine drives (owner / outboundQueue / lastFrameAt + the
 * seven pending-result maps) and adds the registry-owned bookkeeping: the stable
 * `workerId`, the live `hello`, and the `connectedAt` timestamp.
 */
export interface WorkerConnection extends WorkerConnectionSeam {
  /** Stable worker identifier — the environment id from the tunnel path, e.g. `"env_a1b2..."`. */
  readonly workerId: string;
  /** The hello frame the worker sent on connect (version / name / runners / readiness). */
  readonly hello: WorkerHelloFrame;
  /** Epoch-ms of connect time. */
  readonly connectedAt: number;
}

/**
 * Thread-of-control-safe registry of live worker connections.
 *
 * Implements the engine's {@link WorkerRegistrySeam} (`register` / `deregister`)
 * and adds the server-side registry surface: `get`, `onlineWorkerIds`, and a guarded
 * `sendText`. Newest-wins replacement poisons the superseded connection's
 * outbound queue so its sender loop exits.
 */
export class WorkerRegistry implements WorkerRegistrySeam {
  private readonly hosts = new Map<string, WorkerConnection>();

  /**
   * Register a worker connection (newest wins).
   *
   * If `workerId` is already registered (a stale connection that lagged on
   * cleanup), the old connection is replaced and its outbound queue is poisoned
   * with the `null` stop sentinel so its sender loop returns. Insertion order in
   * {@link onlineWorkerIds} is preserved for a fresh id and unchanged for a
   * same-id replacement.
   *
   * @param workerId Stable worker identifier (the environment id).
   * @param ws The accepted WebSocket (kept so the registry can retire a
   *   superseded connection's socket — the engine retires it on replacement).
   * @param hello The validated hello frame from the worker.
   * @param opts.owner Resolved tunnel owner (the tenant workspace), omitted in
   *   single-user / no-auth mode.
   * @returns The new {@link WorkerConnection}.
   */
  register(
    workerId: string,
    ws: WorkerWebSocket,
    hello: WorkerHelloFrame,
    opts: { owner?: string } = {},
  ): WorkerConnection {
    void ws; // The route owns the socket lifecycle; the registry holds no ref.
    const now = Date.now();
    const conn: WorkerConnection = {
      workerId,
      hello,
      owner: opts.owner,
      outboundQueue: new AsyncQueue<string | null>(),
      connectedAt: now,
      lastFrameAt: now,
      pendingLaunches: new Map<string, WorkerPendingRequest<WorkerLaunchResult>>(),
      pendingStops: new Map<string, WorkerPendingRequest<WorkerStopResult>>(),
      pendingStats: new Map<string, WorkerPendingRequest<WorkerStatResult>>(),
      pendingListDirs: new Map<string, WorkerPendingRequest<WorkerListDirResult>>(),
      pendingCreateWorktrees: new Map<string, WorkerPendingRequest<WorkerCreateWorktreeResult>>(),
      pendingRemoveWorktrees: new Map<string, WorkerPendingRequest<WorkerRemoveWorktreeResult>>(),
      pendingCreateDirs: new Map<string, WorkerPendingRequest<WorkerCreateDirResult>>(),
    };
    const old = this.hosts.get(workerId);
    if (old !== undefined) {
      // Poison the superseded connection's queue so its sender loop exits.
      old.outboundQueue.put(null);
    }
    this.hosts.set(workerId, conn);
    return conn;
  }

  /**
   * Remove a worker connection and unblock its sender loop.
   *
   * Pushes the `null` stop sentinel onto the outbound queue so a sender parked on
   * `outboundQueue.get()` wakes and returns, then drops the registry entry.
   * No-op if `workerId` is not registered.
   *
   * @param workerId Worker identifier to remove.
   */
  deregister(workerId: string): void {
    const conn = this.hosts.get(workerId);
    if (conn === undefined) {
      return;
    }
    this.hosts.delete(workerId);
    conn.outboundQueue.put(null);
  }

  /**
   * Look up a live worker connection.
   *
   * @param workerId Worker identifier (the environment id).
   * @returns The {@link WorkerConnection} if online, otherwise `undefined`.
   */
  get(workerId: string): WorkerConnection | undefined {
    return this.hosts.get(workerId);
  }

  /**
   * Insertion-ordered list of all currently-connected worker ids.
   *
   * @returns Array of host_id (environment id) strings.
   */
  onlineWorkerIds(): string[] {
    return [...this.hosts.keys()];
  }

  /**
   * Enqueue a text frame for sending to the worker.
   *
   * Control frames are enqueued here rather than written to the socket directly,
   * since a server-side caller (e.g. the launch orchestration) is decoupled from
   * the route's sender loop — every outbound frame serializes through the one
   * sender draining the queue.
   *
   * @param conn The target worker connection.
   * @param data JSON-encoded frame text.
   * @throws {WorkerConnectionReplacedError} If the connection has been replaced
   *   (newest-wins) or deregistered — its queue is poisoned and the sender has
   *   stopped, so a stale-reference caller must not enqueue onto it.
   */
  sendText(conn: WorkerConnection, data: string): void {
    if (this.hosts.get(conn.workerId) !== conn) {
      throw new WorkerConnectionReplacedError(conn.workerId);
    }
    conn.outboundQueue.put(data);
  }
}
