// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The runner's outbound WebSocket seam.
//
// The runner is a CLIENT: it dials the registry's PUBLIC runner-tunnel endpoint
// `/v1/tunnels/runners/:runnerId`. This module is the network seam — a
// {@link RunnerTunnelConnector} that opens one tunnel and the
// {@link RunnerTunnelSocket} surface the serve loop drives (receive the next
// inbound message, send one text frame, close). The serve loop depends only on
// the seam, so tests inject a scripted connector (or run a real in-process `ws`
// peer) with no production network.
//
// The production connector is `ws`-backed. An upgrade rejection (a non-101
// response, which `ws` surfaces as the `unexpected-response` event) is raised as
// an {@link UpgradeRejectedError} carrying the HTTP status, so the serve loop's
// reconnect loop can classify permanent (e.g. 403) vs transient (any 5xx, 408,
// 429) failures by the rejected-upgrade HTTP status. Mirrors the worker-tunnel
// client seam (`environment-worker/src/ws-client.ts`) so both runner-side and
// worker-side tunnel clients share one shape.

import { WebSocket } from 'ws';
import { TUNNEL_MAX_MESSAGE_BYTES } from '@orca/harness-tunnel';

/**
 * A received message off the runner's tunnel socket: a text frame, or the socket
 * closing (carrying the close code + reason when the peer sent them, so the serve
 * loop's recycle / fatal classification can read them).
 */
export type RunnerTunnelMessage =
  | { readonly type: 'text'; readonly data: string }
  | { readonly type: 'close'; readonly code?: number; readonly reason?: string };

/**
 * The accepted-tunnel surface the serve loop drives.
 *
 * `receive` resolves the next inbound message (a text frame or a close); the
 * serve loop treats a close as end-of-tunnel. `sendText` writes one text frame.
 * `closeSocket` is best-effort (idempotent).
 */
export interface RunnerTunnelSocket {
  receive(): Promise<RunnerTunnelMessage>;
  sendText(data: string): Promise<void>;
  closeSocket(code?: number, reason?: string): void;
}

/**
 * Opens one registry runner tunnel.
 *
 * Resolves with an accepted {@link RunnerTunnelSocket}, or rejects: an
 * {@link UpgradeRejectedError} for a non-101 upgrade response (so the status can
 * be classified), or any other error for a transport-level failure (DNS, connect
 * refused, abrupt drop during the handshake).
 */
export interface RunnerTunnelConnector {
  connect(url: string, headers: Record<string, string>): Promise<RunnerTunnelSocket>;
}

/**
 * The registry rejected the WebSocket upgrade with a non-101 status.
 *
 * Carries the HTTP status so the serve loop can decide whether reconnecting could
 * ever succeed (permanent 4xx like 403) or the failure is transient (408/429, any
 * 5xx). Surfaced so the reconnect loop can classify the rejected upgrade by
 * status.
 */
export class UpgradeRejectedError extends Error {
  constructor(readonly status: number) {
    super(`registry rejected the runner tunnel upgrade with HTTP ${status}`);
    this.name = 'UpgradeRejectedError';
  }
}

/**
 * A `ws`-backed {@link RunnerTunnelSocket}.
 *
 * Inbound `message` / `close` / `error` events funnel into an in-order queue the
 * serve loop drains via {@link receive}, so no frame is missed between the
 * accepted upgrade and the first `receive()`. A socket error surfaces to the
 * serve loop as a close (the serve loop ends and the reconnect loop takes over).
 *
 * Exported so its event-ordering contract — `close` owns the exit CODE, `error`
 * only supplies a fallback reason — is pinned directly, over a socket whose
 * `error`/`close` order the test controls. Reaching it only through
 * {@link WsRunnerTunnelConnector} cannot express that ordering, so the
 * error-before-close case would go unasserted.
 */
export class WsRunnerTunnelSocket implements RunnerTunnelSocket {
  private readonly inbound: RunnerTunnelMessage[] = [];
  private waiter: ((msg: RunnerTunnelMessage) => void) | undefined;
  private closedMessage: RunnerTunnelMessage | undefined;
  private lastError: string | undefined;

  constructor(private readonly socket: WebSocket) {
    socket.on('message', (data: unknown, isBinary: boolean) => {
      if (isBinary) {
        // The tunnel protocol is text-only JSON; drop binary frames.
        return;
      }
      this.push({ type: 'text', data: typeof data === 'string' ? data : String(data) });
    });
    socket.on('close', (code: number, reason: Buffer) => {
      // The single source of truth for the exit. `ws` always emits `close` after
      // `error` — with the server's code (e.g. a fatal 4004 binding refusal) or
      // 1006 for an abrupt "no close frame" drop — so the serve loop's recycle /
      // fatal heuristic reads the code here. `reason` falls back to a preceding
      // error's message when the frame carried none.
      const text = reason.toString();
      this.push({ type: 'close', code, reason: text.length > 0 ? text : (this.lastError ?? '') });
    });
    socket.on('error', (err: Error) => {
      // Do NOT synthesize a close here. A codeless close pushed on `error` becomes
      // the sticky close and PRE-EMPTS the real `close` frame's code — under load
      // an immediate server 4004 arrived `error`-before-`close`, so the fatal code
      // was lost, the loop reconnected instead of exiting, and `done` never
      // rejected. Record the message for the imminent `close` to carry instead.
      this.lastError = err.message;
    });
  }

  private push(msg: RunnerTunnelMessage): void {
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

  receive(): Promise<RunnerTunnelMessage> {
    const queued = this.inbound.shift();
    if (queued !== undefined) {
      return Promise.resolve(queued);
    }
    if (this.closedMessage !== undefined) {
      return Promise.resolve(this.closedMessage);
    }
    return new Promise<RunnerTunnelMessage>((resolve) => {
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

/** The production `ws`-backed {@link RunnerTunnelConnector}. */
export class WsRunnerTunnelConnector implements RunnerTunnelConnector {
  connect(url: string, headers: Record<string, string>): Promise<RunnerTunnelSocket> {
    return new Promise<RunnerTunnelSocket>((resolve, reject) => {
      const socket = new WebSocket(url, { headers, maxPayload: TUNNEL_MAX_MESSAGE_BYTES });
      let settled = false;
      socket.once('open', () => {
        if (settled) {
          return;
        }
        settled = true;
        resolve(new WsRunnerTunnelSocket(socket));
      });
      // A non-101 upgrade response: classify by HTTP status (permanent vs
      // transient) in the serve loop's reconnect loop.
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
