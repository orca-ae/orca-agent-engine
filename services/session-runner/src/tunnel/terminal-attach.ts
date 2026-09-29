// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Runner-side remote terminal-attach: a tunneled WS-channel handler that bridges
// a live tmux pane's pty to the WS channel.
//
// The registry proxies a client's WebSocket to the runner over the runner tunnel
// as a WS channel opened on `attach/:terminalId` (see the registry's client-facing
// attach route). This module registers the runner half of that channel:
//
//   - server → client: the terminal's RAW pane output bytes (the live pty stream,
//     escape sequences and all — NOT the capture-pane rendered grid) are pushed as
//     BINARY WS frames, so a client terminal renders them directly;
//   - client → server: BINARY WS frames are input bytes typed into the pane, and a
//     `{ "type": "resize", "cols": <n>, "rows": <n> }` TEXT control resizes it.
//
// The bridge is backed by {@link TerminalHost.attachTerminal} (the tmux-backed
// {@link SandboxHandle} implements it): it streams pane bytes, types input, resizes
// the pane, and detaches WITHOUT killing the terminal on channel close/teardown.
// The terminal must already exist (launched via sys_terminal_launch / the native
// CLI); an unknown id closes the channel with 1011 so the client sees a clean
// refusal.
//
// This is deliberately framework-free: it speaks only the {@link WsChannel} surface
// the serve loop translates to/from `ws.*` frames, so the whole bridge is
// unit-testable in-process against the fake registry runner-tunnel peer with a real
// tmux pane and no registry app.

import { RUNNER_TERMINAL_ATTACH_PATH } from '@orca/harness-tunnel';
import { matchWsPathParams, type RouteDispatcher, type WsChannel } from './request-dispatch.js';
import type { AttachedTerminal, TerminalHost } from '../tools/sys-terminal.js';

/**
 * The runner path a terminal-attach WS channel opens on. The registry proxy opens
 * `${TERMINAL_ATTACH_PATH}/<terminalId>`; the handler is registered on the
 * `:terminalId` template of the same base so the concrete id is captured. Sourced
 * from the SHARED `@orca/harness-tunnel` wire-path symbol (the registry proxy opens
 * on the same constant), so the base path is one literal across both services.
 */
export const TERMINAL_ATTACH_PATH = RUNNER_TERMINAL_ATTACH_PATH;

/** The `:terminalId`-templated route the attach handler registers on. */
export const TERMINAL_ATTACH_ROUTE = `${TERMINAL_ATTACH_PATH}/:terminalId`;

/** WS close code when the attach cannot be served (unknown terminal / attach error). */
const ATTACH_REFUSED_CLOSE_CODE = 1011;

/**
 * WS close code when pane output can no longer be delivered (a `sendBytes` that failed
 * for a reason other than the channel closing). Same 1011 "internal error" class as a
 * refused attach: the client is told the bridge broke rather than left on a live-looking
 * channel that silently drops every byte.
 */
const SEND_FAILED_CLOSE_CODE = 1011;

/** Structured logger seam (a subset of the usual structured logger). All optional. */
export interface TerminalAttachLogger {
  info?(obj: unknown, msg?: string): void;
  warn?(obj: unknown, msg?: string): void;
  error?(obj: unknown, msg?: string): void;
}

/**
 * Resolve the {@link TerminalHost} to attach to for a fresh channel, or `null`
 * when none is available right now (no terminal-capable sandbox). Consulted PER
 * ATTACH so the handler always binds the runner's LIVE per-session sandbox, which
 * the loop rebuilds on each snapshot-apply — a fixed capture would attach to a
 * stale (destroyed) sandbox after a re-snapshot.
 */
export type TerminalHostResolver = () => TerminalHost | null;

/** Options for {@link registerTerminalAttach}. */
export interface RegisterTerminalAttachOptions {
  /** Optional structured logger threaded into the attach handler. */
  logger?: TerminalAttachLogger;
}

/**
 * Register the terminal-attach WS handler on `dispatcher`.
 *
 * `host` is either a fixed {@link TerminalHost} or a {@link TerminalHostResolver}
 * consulted per attach (the real runner passes the latter, bound to the loop's
 * live per-session sandbox). Returns the same `dispatcher` so callers can chain.
 * Registering twice throws (the dispatcher rejects a duplicate route).
 */
export function registerTerminalAttach(
  dispatcher: RouteDispatcher,
  host: TerminalHost | TerminalHostResolver,
  opts: RegisterTerminalAttachOptions = {},
): RouteDispatcher {
  const logger = opts.logger;
  const resolve: TerminalHostResolver = typeof host === 'function' ? host : () => host;
  dispatcher.registerWebSocket(TERMINAL_ATTACH_ROUTE, async (channel) => {
    const resolved = resolve();
    if (resolved === null) {
      // No terminal-capable sandbox for this session (none acquired, or a
      // cloud-only handle): refuse the attach cleanly rather than hang.
      logger?.warn?.({ path: channel.path }, 'terminal attach: no terminal host available');
      await channel.close(ATTACH_REFUSED_CLOSE_CODE, 'no terminal host');
      return;
    }
    await bridgeTerminalAttach(channel, resolved, logger);
  });
  return dispatcher;
}

/**
 * Bridge one tunneled WS channel to a terminal's pty for its whole lifetime.
 *
 * Resolves the terminal id from the channel path, attaches (streaming pane bytes
 * to `channel.sendBytes`), then pumps inbound messages — binary → input bytes,
 * a resize-control text → pane resize — until the peer closes or the tunnel tears
 * the channel down, always detaching (never killing the terminal) on the way out.
 */
async function bridgeTerminalAttach(
  channel: WsChannel,
  host: TerminalHost,
  logger: TerminalAttachLogger | undefined,
): Promise<void> {
  const terminalId = terminalIdFromPath(channel.path);
  if (terminalId === undefined) {
    logger?.warn?.({ path: channel.path }, 'terminal attach: no terminal id in path');
    await channel.close(ATTACH_REFUSED_CLOSE_CODE, 'missing terminal id');
    return;
  }

  await channel.accept();

  // Attach to the LIVE pane: raw output bytes flow straight out as binary frames.
  // A backpressure-free fire-and-forget send keeps the byte pump off the tmux read path.
  //
  // A send CAN still fail, and the two reasons need different handling. On TEARDOWN the
  // tunnel is going away and the `messages()` loop is already ending, so the `finally`'s
  // detach stops the stream — nothing to do but note it. Any OTHER failure is the silent
  // freeze: `messages()` does NOT end on a failed send, so without intervention the detach
  // never runs, the pane keeps producing, and every chunk is dropped with no trace. So the
  // first such failure is logged (once — a broken channel fails on every chunk, and a log
  // per byte would be its own outage) and the channel is CLOSED, which ends the message
  // loop and unwinds the bridge through its detach.
  let attached: AttachedTerminal;
  let sendFailed = false;
  const onSendFailure = (err: unknown): void => {
    if (sendFailed) {
      return; // already reported + closing; stay quiet for the rest of the stream.
    }
    sendFailed = true;
    if (channel.teardownSignal.aborted) {
      logger?.warn?.({ err, terminalId }, 'terminal attach: send failed on a closing channel');
      return;
    }
    logger?.warn?.({ err, terminalId }, 'terminal attach: output send failed, closing channel');
    void channel.close(SEND_FAILED_CLOSE_CODE, 'terminal output send failed').catch(() => {
      /* the channel is already gone; the detach in `finally` still stops the stream */
    });
  };
  try {
    attached = await host.attachTerminal(terminalId, {
      onData: (bytes) => {
        void channel.sendBytes(bytes).catch(onSendFailure);
      },
    });
  } catch (err) {
    // Unknown terminal id (or a pane that vanished mid-attach): refuse cleanly so
    // the proxied client sees the attach fail instead of a silent hang.
    logger?.warn?.({ err, terminalId }, 'terminal attach failed');
    await channel.close(ATTACH_REFUSED_CLOSE_CODE, 'terminal attach failed');
    return;
  }

  try {
    for await (const msg of channel.messages()) {
      if (msg.kind === 'bytes') {
        // Client → server input bytes: type them into the pane verbatim.
        attached.write(msg.data);
        continue;
      }
      // A TEXT frame is a control message; the only one is a resize.
      const resize = parseResizeControl(msg.data);
      if (resize !== undefined) {
        await attached.resize(resize.cols, resize.rows);
        continue;
      }
      logger?.warn?.(
        { terminalId, text: msg.data },
        'terminal attach: dropping unrecognized control frame',
      );
    }
  } finally {
    // Peer close OR tunnel teardown: detach (stop streaming), NEVER kill the
    // terminal — a later attach re-attaches to the same live pane.
    await attached.detach();
  }
}

/** A parsed resize control message from a channel text frame. */
interface ResizeControl {
  cols: number;
  rows: number;
}

/**
 * Parse a `{ "type": "resize", "cols": <n>, "rows": <n> }` control out of a
 * channel text frame, or `undefined` when the text is not a well-formed resize
 * (bad JSON, wrong type, or non-positive-integer dimensions). Defensive: a
 * malformed control is dropped, never crashing the bridge.
 */
export function parseResizeControl(text: string): ResizeControl | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return undefined;
  }
  if (parsed === null || typeof parsed !== 'object') {
    return undefined;
  }
  const obj = parsed as { type?: unknown; cols?: unknown; rows?: unknown };
  if (obj.type !== 'resize') {
    return undefined;
  }
  const cols = obj.cols;
  const rows = obj.rows;
  if (!isPositiveInt(cols) || !isPositiveInt(rows)) {
    return undefined;
  }
  return { cols, rows };
}

/** Whether `value` is a positive integer (a valid terminal dimension). */
function isPositiveInt(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value > 0;
}

/** Extract the `:terminalId` segment from a concrete attach channel path. */
function terminalIdFromPath(path: string): string | undefined {
  const params = matchWsPathParams(TERMINAL_ATTACH_ROUTE, path);
  const id = params?.terminalId;
  return id !== undefined && id.length > 0 ? id : undefined;
}
