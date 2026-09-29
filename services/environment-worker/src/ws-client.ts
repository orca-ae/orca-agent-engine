// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The worker's outbound WebSocket seam.
//
// The worker is a CLIENT: it dials the registry host tunnel. This module is the
// network seam — a `RegistryConnector` that opens one tunnel and the
// `WorkerSocket` surface the worker drives (receive the next inbound message,
// send one text frame, close). The worker depends only on the seam, so tests
// inject a scripted connector with no network.
//
// The production connector is `ws`-backed. An upgrade rejection (a non-101
// response, which `ws` surfaces as the `unexpected-response` event) is raised as
// an {@link UpgradeRejectedError} carrying the HTTP status, so the worker's
// reconnect loop can classify permanent vs transient failures by the
// rejected-upgrade HTTP status.

import { WebSocket } from 'ws';

/**
 * A received message off the worker's tunnel socket: a text frame, or the socket
 * closing (carrying the close code + reason when the peer sent them, so the
 * worker's recycle heuristic can read them).
 */
export type WorkerSocketMessage =
  | { readonly type: 'text'; readonly data: string }
  | { readonly type: 'close'; readonly code?: number; readonly reason?: string };

/**
 * The accepted-tunnel surface the worker drives.
 *
 * `receive` resolves the next inbound message (a text frame or a close); the
 * serve loop treats a close as end-of-tunnel. `sendText` writes one text frame.
 * `closeSocket` is best-effort (idempotent).
 */
export interface WorkerSocket {
  receive(): Promise<WorkerSocketMessage>;
  sendText(data: string): Promise<void>;
  closeSocket(code?: number, reason?: string): void;
}

/**
 * Opens one registry host tunnel.
 *
 * Resolves with an accepted {@link WorkerSocket}, or rejects: an
 * {@link UpgradeRejectedError} for a non-101 upgrade response (so the status can
 * be classified), or any other error for a transport-level failure (DNS, connect
 * refused, abrupt drop during the handshake).
 */
export interface RegistryConnector {
  connect(url: string, headers: Record<string, string>): Promise<WorkerSocket>;
}

/**
 * The registry rejected the WebSocket upgrade with a non-101 status.
 *
 * Carries the HTTP status so the worker can decide whether reconnecting could
 * ever succeed (permanent 4xx) or the failure is transient (408/429, any 5xx).
 * Surfaced so the reconnect loop can classify the rejected upgrade by status.
 */
export class UpgradeRejectedError extends Error {
  constructor(readonly status: number) {
    super(`registry rejected the host tunnel upgrade with HTTP ${status}`);
    this.name = 'UpgradeRejectedError';
  }
}

/** Max bytes for a single tunnel message (100 MiB), matching the engine limit. */
const MAX_PAYLOAD_BYTES = 100 * 1024 * 1024;

/**
 * A `ws`-backed {@link WorkerSocket}.
 *
 * Inbound `message` / `close` / `error` events funnel into an in-order queue the
 * worker drains via {@link receive}, so no frame is missed between the accepted
 * upgrade and the first `receive()`. A socket error surfaces to the worker as a
 * close (the serve loop ends and the reconnect loop takes over).
 */
class WsWorkerSocket implements WorkerSocket {
  private readonly inbound: WorkerSocketMessage[] = [];
  private waiter: ((msg: WorkerSocketMessage) => void) | undefined;
  private closedMessage: WorkerSocketMessage | undefined;

  constructor(private readonly socket: WebSocket) {
    socket.on('message', (data: unknown, isBinary: boolean) => {
      if (isBinary) {
        // The tunnel protocol is text-only JSON; drop binary frames.
        return;
      }
      this.push({ type: 'text', data: typeof data === 'string' ? data : String(data) });
    });
    socket.on('close', (code: number, reason: Buffer) => {
      this.push({ type: 'close', code, reason: reason.toString() });
    });
    socket.on('error', (err: Error) => {
      // Surface a socket error to the worker as a close carrying the message, so
      // the recycle heuristic can read e.g. an abrupt "no close frame" drop.
      this.push({ type: 'close', reason: err.message });
    });
  }

  private push(msg: WorkerSocketMessage): void {
    // Once closed, every later receive() keeps returning the sticky close.
    if (msg.type === 'close' && this.closedMessage === undefined) {
      this.closedMessage = msg;
    }
    const waiter = this.waiter;
    if (waiter !== undefined) {
      this.waiter = undefined;
      waiter(msg);
      return;
    }
    this.inbound.push(msg);
  }

  receive(): Promise<WorkerSocketMessage> {
    const queued = this.inbound.shift();
    if (queued !== undefined) {
      return Promise.resolve(queued);
    }
    if (this.closedMessage !== undefined) {
      return Promise.resolve(this.closedMessage);
    }
    return new Promise<WorkerSocketMessage>((resolve) => {
      this.waiter = resolve;
    });
  }

  sendText(data: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.socket.send(data, (err) => (err ? reject(err) : resolve()));
    });
  }

  closeSocket(code?: number, reason?: string): void {
    try {
      this.socket.close(code, reason);
    } catch {
      // best-effort close; swallow
    }
  }
}

/** The production `ws`-backed {@link RegistryConnector}. */
export class WsRegistryConnector implements RegistryConnector {
  connect(url: string, headers: Record<string, string>): Promise<WorkerSocket> {
    return new Promise<WorkerSocket>((resolve, reject) => {
      const socket = new WebSocket(url, { headers, maxPayload: MAX_PAYLOAD_BYTES });
      let settled = false;
      socket.once('open', () => {
        if (settled) {
          return;
        }
        settled = true;
        resolve(new WsWorkerSocket(socket));
      });
      // A non-101 upgrade response: classify by HTTP status (permanent vs
      // transient) in the worker's reconnect loop.
      socket.once('unexpected-response', (_req, res: { statusCode?: number }) => {
        if (settled) {
          return;
        }
        settled = true;
        socket.terminate();
        reject(new UpgradeRejectedError(res.statusCode ?? 0));
      });
      socket.once('error', (err: Error) => {
        if (settled) {
          return;
        }
        settled = true;
        reject(err);
      });
    });
  }
}
