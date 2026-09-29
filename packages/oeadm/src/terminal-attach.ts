// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// `oeadm attach --session <id> --terminal <id>` support: a byte proxy between the
// operator's terminal and a running agent terminal, over the registry's
// terminal-attach WebSocket route
// (`/v1/sessions/:sessionId/terminals/:terminalId/attach`, from the registry's
// `terminal-attach.routes.ts`). The registry is a pure byte proxy on that route,
// so this client simply pipes: stdin bytes → WS binary frames (pane input) and
// WS binary/text frames → stdout (the live pty stream).
//
// To stay dependency-free (node built-ins preferred over `ws`), the WebSocket is
// taken from `globalThis.WebSocket` (stable on the Node 22 target). It is
// injectable via {@link WebSocketCtor} so the proxy is unit tested against a fake
// socket — asserting the dialed URL + auth header + that stdin is forwarded —
// with no real connection.

import type { Readable, Writable } from 'node:stream';
import { authHeaders, type ClientConfig } from './client.js';

/**
 * The minimal WebSocket surface the proxy drives. Matches the WHATWG
 * `WebSocket` the Node 22 global provides (and the `ws` package). `send` takes
 * bytes; events are dispatched via `addEventListener`.
 */
export interface MinimalWebSocket {
  binaryType: string;
  send(data: Uint8Array): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: 'open', listener: () => void): void;
  addEventListener(type: 'close', listener: (ev: { code?: number; reason?: string }) => void): void;
  addEventListener(type: 'error', listener: (ev: unknown) => void): void;
  addEventListener(type: 'message', listener: (ev: { data: unknown }) => void): void;
}

/** Constructs a {@link MinimalWebSocket} for a URL with headers (undici/ws shape). */
export type WebSocketCtor = (
  url: string,
  options: { headers: Record<string, string> },
) => MinimalWebSocket;

/**
 * How a terminal attach ended.
 *
 * The close CODE alone is not the outcome. A registry that refuses the attach
 * closes with `1008` and a `reason` naming the cause ("terminal not found"), and
 * a mid-stream socket fault carries an `Error` the code cannot express — both of
 * which the caller must be able to distinguish from a clean `1000` detach. An
 * earlier version resolved a bare number and dropped the reason the `close`
 * listener already receives and the error object entirely, so both failures
 * reached the operator as an optimistic banner and exit 0.
 */
export interface TerminalAttachResult {
  /** WebSocket close code. `1000` is a clean detach; anything else is a failure. */
  code: number;
  /** Close reason the registry sent, when it sent one. */
  reason?: string;
  /** The socket fault, when the attach ended because the socket errored. */
  error?: Error;
}

/** Options for {@link attachTerminal}. */
export interface AttachTerminalOptions {
  config: ClientConfig;
  sessionId: string;
  terminalId: string;
  /** Terminal input (defaults to process.stdin). */
  input?: Readable;
  /** Terminal output (defaults to process.stdout). */
  output?: Writable;
  /** Injected WebSocket constructor; defaults to the Node global. */
  webSocket?: WebSocketCtor;
  /** Called once the socket closes, with the full outcome. */
  onClose?: (result: TerminalAttachResult) => void;
}

/**
 * Compute the WebSocket URL for a terminal attach from an HTTP(S) base URL:
 * `http` → `ws`, `https` → `wss`, path is the registry's attach route with both
 * ids percent-encoded.
 */
export function terminalAttachUrl(baseURL: string, sessionId: string, terminalId: string): string {
  const wsBase = baseURL.replace(/^http/, 'ws');
  return (
    `${wsBase}/v1/sessions/${encodeURIComponent(sessionId)}` +
    `/terminals/${encodeURIComponent(terminalId)}/attach`
  );
}

/** WebSocket close code for a clean, intentional detach. */
export const NORMAL_CLOSURE = 1000;

/**
 * RFC 6455 §7.4.1 `1006`: the connection ended without a close frame, so no code
 * was received.
 *
 * The seam types `code` as optional because a close event is not obliged to
 * carry one. Defaulting that to {@link NORMAL_CLOSURE} made the ABSENCE of
 * evidence read as evidence of a clean detach — `attach.ts` treats any code
 * other than 1000 as a failure, so a socket that simply vanished would have
 * printed the optimistic banner and exited 0, which is the exact fault the
 * widened result type exists to prevent.
 */
export const ABNORMAL_CLOSURE = 1006;

/**
 * Open the terminal-attach WebSocket and proxy bytes both ways until the socket
 * closes. Resolves with a {@link TerminalAttachResult} carrying the close code,
 * the registry's close reason, and the socket fault when there was one. Rejects
 * only if the socket errors before opening (a mid-stream error resolves with
 * `code: 1011` and the `Error` attached, so the caller can report the cause).
 * stdin bytes are forwarded as binary frames; inbound frames (binary or text)
 * are written to stdout verbatim.
 */
export function attachTerminal(opts: AttachTerminalOptions): Promise<TerminalAttachResult> {
  const input = opts.input ?? process.stdin;
  const output = opts.output ?? process.stdout;
  const ctor = opts.webSocket ?? defaultWebSocketCtor();

  const url = terminalAttachUrl(opts.config.baseURL, opts.sessionId, opts.terminalId);
  const socket = ctor(url, { headers: authHeaders(opts.config) });
  socket.binaryType = 'arraybuffer';

  return new Promise<TerminalAttachResult>((resolve, reject) => {
    let opened = false;
    let closed = false;

    const onStdin = (chunk: Buffer): void => {
      if (!closed) socket.send(new Uint8Array(chunk));
    };

    const finish = (result: TerminalAttachResult): void => {
      if (closed) return;
      closed = true;
      input.off('data', onStdin);
      opts.onClose?.(result);
      resolve(result);
    };

    socket.addEventListener('open', () => {
      opened = true;
      input.on('data', onStdin);
    });
    socket.addEventListener('message', (ev) => {
      output.write(toBuffer(ev.data));
    });
    socket.addEventListener('close', (ev) => {
      // The `reason` is the registry's own words for WHY it closed — "terminal
      // not found" on a 1008. It arrives here and used to be dropped on the
      // floor, leaving the operator a bare code they could not act on.
      const code = typeof ev.code === 'number' ? ev.code : ABNORMAL_CLOSURE;
      const reason = typeof ev.reason === 'string' && ev.reason.length > 0 ? ev.reason : undefined;
      finish(reason !== undefined ? { code, reason } : { code });
    });
    socket.addEventListener('error', (err) => {
      const error = err instanceof Error ? err : new Error('terminal attach socket error');
      if (!opened) {
        reject(error);
      } else {
        // A mid-stream fault (an ECONNRESET on a live pty) is a FAILED attach.
        // Carrying the Error is what lets the caller say so; discarding it is
        // how a reset connection came to print the optimistic banner and exit 0.
        finish({ code: 1011, error });
      }
    });
  });
}

/** Coerce a WS message payload (ArrayBuffer / view / string) to a Buffer for stdout. */
function toBuffer(data: unknown): Buffer {
  if (typeof data === 'string') return Buffer.from(data, 'utf8');
  if (data instanceof ArrayBuffer) return Buffer.from(data);
  if (ArrayBuffer.isView(data)) {
    return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  }
  return Buffer.from(String(data), 'utf8');
}

/**
 * Resolve the Node global `WebSocket` (stable on the Node 22 target) into a
 * {@link WebSocketCtor}. Throws a clear error on a runtime that lacks it so the
 * operator gets an actionable message instead of a `ReferenceError`.
 */
function defaultWebSocketCtor(): WebSocketCtor {
  const globalWs = (globalThis as { WebSocket?: unknown }).WebSocket;
  if (typeof globalWs !== 'function') {
    throw new Error(
      'terminal attach requires a global WebSocket (Node 22+). Upgrade Node, or run ' +
        '`oeadm attach` without --terminal for the event co-drive loop.',
    );
  }
  const Ctor = globalWs as new (url: string, protocols?: string | string[]) => MinimalWebSocket;
  // The WHATWG global ignores a headers option; undici's dispatcher path and the
  // `ws` package both accept it. We pass it through the options bag for runtimes
  // that honor it (the registry authenticates the upgrade via `x-api-key`).
  return (url, options) =>
    new (Ctor as unknown as new (
      u: string,
      o: { headers: Record<string, string> },
    ) => MinimalWebSocket)(url, options);
}
