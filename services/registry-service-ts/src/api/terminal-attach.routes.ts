// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Client-facing WebSocket route for a remote terminal-attach.
//
// A client (an operator UI / CLI) opens a WebSocket to
// `/v1/sessions/:sessionId/terminals/:terminalId/attach`. The registry resolves
// the session's runner, opens a TUNNELED WS channel to that runner over the runner
// tunnel (via {@link openTunnelWsChannel} — reusing the TunnelRegistry ws-channel
// bookkeeping), and PROXIES frames between the client socket and the runner's
// pty-bridge:
//
//   - client → runner: a client BINARY frame is forwarded as tunnel input bytes; a
//     client TEXT frame is forwarded verbatim (the runner reads a
//     `{ "type": "resize", ... }` control text there);
//   - runner → client: tunnel `ws.frame` bytes are delivered to the client as
//     BINARY frames (the live pane pty stream) and any text as a client TEXT frame;
//   - either side closing tears the channel down.
//
// The registry is a pure BYTE PROXY here: it never interprets the pty stream, so
// any terminal client renders the runner's bytes directly and the runner types the
// client's bytes into the pane. Resolution of `sessionId → runnerId` is injected as
// a seam so the route is unit-testable without a DB (the production wiring reads
// the session row's bound runner id).

import type { FastifyInstance, FastifyRequest } from 'fastify';
import type { WebSocket as WsWebSocket } from 'ws';
import { RUNNER_TERMINAL_ATTACH_PATH } from '@orca/harness-tunnel';
import { TunnelRegistry } from '../tunnel/tunnel-registry.js';
import { openTunnelWsChannel, type TunnelWsChannel } from '../tunnel/tunnel-ws-channel.js';

/**
 * The runner-side path a terminal-attach channel opens on — the SHARED wire-path
 * symbol from `@orca/harness-tunnel`, the same constant the runner's pty-bridge
 * registers its handler on (`session-runner`'s `TERMINAL_ATTACH_PATH`, sourced from
 * the same symbol). Re-exported here so existing importers keep their name while
 * the literal lives in exactly one place; neither service depends on the other
 * (both already depend on the tunnel package).
 */
export { RUNNER_TERMINAL_ATTACH_PATH };

/** WS close code used when the attach cannot be established (no runner / resolve error). */
export const ATTACH_UNAVAILABLE_CLOSE_CODE = 4004;
/** WS close code used when the tunnel channel drops or the proxy faults. */
const ATTACH_TUNNEL_CLOSED_CLOSE_CODE = 1011;

/** Structured logger seam (a subset of the usual `req.log`). All optional. */
export interface TerminalAttachRouteLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/** Options for {@link registerTerminalAttachRoutes}. */
export interface TerminalAttachRouteOptions {
  /** Shared runner-tunnel registry the proxied channel rides. */
  registry: TunnelRegistry;
  /**
   * Resolve the online runner id serving `sessionId` for the authenticated
   * caller, or `null` when the session has no bound runner visible to this caller
   * (never distributed, still pending, its runner is offline, or the session is
   * owned by another workspace). Receives the upgrade `req` so the production
   * wiring can OWNER-SCOPE the lookup on `req.auth.workspaceId` — a client may only
   * attach to a terminal in a session it owns. A `null` closes the client with
   * {@link ATTACH_UNAVAILABLE_CLOSE_CODE}.
   */
  resolveRunnerId: (sessionId: string, req: FastifyRequest) => Promise<string | null>;
  /** Optional structured logger. */
  logger?: TerminalAttachRouteLogger;
}

/** Path params carried on the attach route. */
interface AttachParams {
  sessionId: string;
  terminalId: string;
}

/**
 * Register the client-facing terminal-attach WebSocket route.
 *
 * Requires the `@fastify/websocket` plugin on the same instance (as the
 * runner-tunnel route does).
 */
export function registerTerminalAttachRoutes(
  app: FastifyInstance,
  opts: TerminalAttachRouteOptions,
): void {
  app.get<{ Params: AttachParams }>(
    '/v1/sessions/:sessionId/terminals/:terminalId/attach',
    { websocket: true },
    (socket: WsWebSocket, req) => {
      const params = req.params as AttachParams | undefined;
      const sessionId = params?.sessionId ?? attachPathParam(req, 'sessionId');
      const terminalId = params?.terminalId ?? attachPathParam(req, 'terminalId');
      if (
        sessionId === undefined ||
        sessionId.length === 0 ||
        terminalId === undefined ||
        terminalId.length === 0
      ) {
        socket.close(ATTACH_UNAVAILABLE_CLOSE_CODE, 'missing session or terminal id');
        return;
      }
      void proxyAttach(socket, req, sessionId, terminalId, opts);
    },
  );
}

/**
 * Resolve the runner, open the tunnel channel, and proxy the client socket to it
 * for the whole attach lifetime. Any pre-open failure closes the client with a
 * 4xxx code; a mid-stream tunnel drop closes it with 1011.
 */
async function proxyAttach(
  socket: WsWebSocket,
  req: FastifyRequest,
  sessionId: string,
  terminalId: string,
  opts: TerminalAttachRouteOptions,
): Promise<void> {
  const { registry, resolveRunnerId, logger } = opts;

  let runnerId: string | null;
  try {
    runnerId = await resolveRunnerId(sessionId, req);
  } catch (err) {
    logger?.error?.({ err, sessionId }, 'terminal attach: runner resolution failed');
    safeClose(socket, ATTACH_UNAVAILABLE_CLOSE_CODE, 'runner resolution failed');
    return;
  }
  if (runnerId === null || !registry.has(runnerId)) {
    logger?.warn?.({ sessionId, terminalId }, 'terminal attach: no online runner for session');
    safeClose(socket, ATTACH_UNAVAILABLE_CLOSE_CODE, 'no online runner for session');
    return;
  }

  let channel: TunnelWsChannel;
  try {
    channel = openTunnelWsChannel({
      registry,
      runnerId,
      path: `${RUNNER_TERMINAL_ATTACH_PATH}/${encodeURIComponent(terminalId)}`,
    });
  } catch (err) {
    // The runner went offline between the has() check and the open, or the session
    // was replaced: close the client cleanly.
    logger?.warn?.({ err, sessionId, runnerId }, 'terminal attach: could not open tunnel channel');
    safeClose(socket, ATTACH_UNAVAILABLE_CLOSE_CODE, 'runner unavailable');
    return;
  }

  let clientClosed = false;
  const closeClient = (code: number, reason: string): void => {
    if (clientClosed) {
      return;
    }
    clientClosed = true;
    safeClose(socket, code, reason);
  };

  // Client → runner. A binary frame is input bytes; a text frame (a resize
  // control) is forwarded verbatim. Fire-and-forget: a send onto a torn-down
  // channel is swallowed by the channel, so the client is never blocked.
  socket.on('message', (data: Buffer, isBinary: boolean) => {
    if (isBinary) {
      void channel.sendBytes(new Uint8Array(data)).catch(() => {});
    } else {
      void channel.sendText(data.toString('utf8')).catch(() => {});
    }
  });

  // Client closed / errored: close the tunnel channel toward the runner (which
  // detaches the pane without killing the terminal).
  socket.on('close', () => {
    clientClosed = true;
    void channel.close(1000, 'client closed');
  });
  socket.on('error', () => {
    clientClosed = true;
    void channel.close(1011, 'client error');
  });

  // Runner → client: pump the channel's inbound messages to the client socket.
  try {
    for await (const msg of channel.messages()) {
      if (clientClosed) {
        break;
      }
      if (msg.kind === 'bytes') {
        socket.send(data(msg.data), { binary: true });
      } else if (msg.kind === 'text') {
        socket.send(msg.data);
      } else {
        // The runner closed its side of the channel — mirror the close to the client.
        closeClient(normalizeCloseCode(msg.code), msg.reason || 'runner closed terminal');
        return;
      }
    }
    // The iterator ended without a runner close (local close / session teardown).
    closeClient(ATTACH_TUNNEL_CLOSED_CLOSE_CODE, 'terminal tunnel closed');
  } catch (err) {
    logger?.warn?.({ err, sessionId, runnerId }, 'terminal attach: proxy pump failed');
    void channel.close(1011, 'proxy error');
    closeClient(ATTACH_TUNNEL_CLOSED_CLOSE_CODE, 'terminal tunnel error');
  }
}

/** Best-effort close of a client socket (swallow a double-close). */
function safeClose(socket: WsWebSocket, code: number, reason: string): void {
  try {
    socket.close(code, reason);
  } catch {
    // already closing/closed
  }
}

/** A `Buffer` view over the channel's bytes for a `ws` binary send. */
function data(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes);
}

/**
 * A WebSocket close code the client is allowed to receive. The runner's channel
 * close code may be an application code (e.g. 1011) that is valid on the wire;
 * 1000/`>=3000` pass through, anything else is normalized to 1011 so the client
 * close never carries a reserved code the `ws` layer would reject.
 */
function normalizeCloseCode(code: number): number {
  if (code === 1000 || (code >= 3000 && code <= 4999)) {
    return code;
  }
  return ATTACH_TUNNEL_CLOSED_CLOSE_CODE;
}

/**
 * Resolve an attach path param from the raw upgrade URL when Fastify's parsed
 * `req.params` is unavailable on the WS upgrade lifecycle. The canonical path is
 * `/v1/sessions/:sessionId/terminals/:terminalId/attach`, so `sessionId` is the
 * segment after `sessions` and `terminalId` the segment after `terminals`.
 */
function attachPathParam(req: FastifyRequest, key: 'sessionId' | 'terminalId'): string | undefined {
  const path = (req.url ?? '').split('?')[0] ?? '';
  const segments = path.split('/').filter((s) => s.length > 0);
  const anchor = key === 'sessionId' ? 'sessions' : 'terminals';
  const idx = segments.indexOf(anchor);
  if (idx === -1 || idx + 1 >= segments.length) {
    return undefined;
  }
  const raw = segments[idx + 1];
  if (raw === undefined || raw.length === 0) {
    return undefined;
  }
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}
