// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Registry-side tunnel WebSocket-channel client.
//
// The registry PUSHES framed HTTP requests to a runner over the runner tunnel;
// this is the OTHER direction of the same socket — a tunneled WebSocket channel
// the registry OPENS on a runner (e.g. a remote terminal-attach). It reuses the
// {@link TunnelRegistry}'s ws-channel bookkeeping end-to-end:
//
//   - {@link TunnelRegistry.openWsChannel} allocates the per-`chId` inbound queue;
//   - {@link TunnelRegistry.sendText} enqueues an outbound `ws.open` / `ws.frame` /
//     `ws.close` frame on the session's outbound queue (drained to the socket by
//     the runner-tunnel route's sender loop — the ONE writer);
//   - the runner-tunnel route's receive loop calls
//     {@link TunnelRegistry.routeWsInbound} for each inbound `ws.frame` / `ws.close`,
//     which pushes onto this channel's inbound queue;
//   - {@link TunnelRegistry.closeWsChannel} drops the channel on teardown.
//
// So this module owns NO socket: it is a thin, in-process driver over the registry
// seam, which makes it unit-testable against a `TunnelRegistry` holding a fake
// runner session with no live socket. A client-facing route (the terminal-attach
// proxy) opens one of these per client WebSocket and pumps frames between the two.

import { randomBytes } from 'node:crypto';
import { FrameKind, encodeFrame } from '@orca/harness-tunnel';
import { TunnelRegistry, type RegistrySession, type WsInboundItem } from './tunnel-registry.js';

/** One message read from a tunneled WS channel (runner → registry). */
export type TunnelWsMessage =
  | { kind: 'bytes'; data: Uint8Array }
  | { kind: 'text'; data: string }
  | { kind: 'close'; code: number; reason: string };

/** Options for {@link openTunnelWsChannel}. */
export interface OpenTunnelWsChannelOptions {
  /** The shared runner-tunnel registry the channel rides. */
  registry: TunnelRegistry;
  /** The runner to open the channel on (must be online). */
  runnerId: string;
  /** The runner-side path the `ws.open` targets, e.g. `attach/<terminalId>`. */
  path: string;
  /** URL-encoded query string for the attach (without the leading `?`). Defaults to `""`. */
  queryString?: string;
  /** Explicit channel id; defaults to a fresh random 8-byte hex id. */
  chId?: string;
}

/**
 * An open tunneled WS channel from the registry to a runner. {@link sendBytes} /
 * {@link sendText} push frames to the runner; {@link messages} is an async-iterable
 * of inbound runner frames (ending after a runner close or a local {@link close} /
 * session teardown); {@link close} closes the channel toward the runner.
 */
export interface TunnelWsChannel {
  /** This channel's id (correlates frames on the runner tunnel). */
  readonly chId: string;
  /** Send binary bytes to the runner as a base64 `ws.frame`. */
  sendBytes(bytes: Uint8Array): Promise<void>;
  /** Send a text payload to the runner as a utf-8 `ws.frame`. */
  sendText(text: string): Promise<void>;
  /**
   * Inbound runner messages until the runner closes the channel, the local side
   * {@link close}s it, or the session is torn down (a `null` sentinel the registry
   * pushes on abort ends the iterator without a synthetic close).
   */
  messages(): AsyncIterable<TunnelWsMessage>;
  /** Close the channel toward the runner (idempotent) and drop its registry state. */
  close(code?: number, reason?: string): Promise<void>;
}

/**
 * Open a tunneled WS channel on `runnerId` and return a driver for it.
 *
 * Allocates the channel's registry state, sends the `ws.open` frame, and hands
 * back a {@link TunnelWsChannel}. The runner's serve loop dispatches the `ws.open`
 * to its registered handler for `path` (e.g. the terminal-attach pty-bridge).
 *
 * @throws {RunnerOfflineError} If the runner is offline (via
 *   {@link TunnelRegistry.openWsChannel}).
 * @throws {ConnectionError} If the session was replaced before the `ws.open` left
 *   (via {@link TunnelRegistry.sendText}).
 */
export function openTunnelWsChannel(opts: OpenTunnelWsChannelOptions): TunnelWsChannel {
  const { registry, runnerId, path } = opts;
  const queryString = opts.queryString ?? '';
  const chId = opts.chId ?? randomBytes(8).toString('hex');

  // Bind to THIS session generation so a newest-wins reconnect can't misroute the
  // channel onto a newer tunnel: openWsChannel returns the current session, and we
  // pass it as the generation guard to every later send / route.
  const state = registry.openWsChannel(runnerId, chId);
  const session: RegistrySession = state.session;

  let closed = false;

  const sendFrame = async (frame: Parameters<typeof encodeFrame>[0]): Promise<void> => {
    await registry.sendText(session, encodeFrame(frame));
  };

  const open = sendFrame({ kind: FrameKind.WsOpen, chId, path, queryString });
  // Surface an open-send failure through the first send/iterate rather than as an
  // unhandled rejection; a replaced session throws synchronously above anyway.
  open.catch(() => {
    /* observed via subsequent sends / the message iterator */
  });

  async function* messages(): AsyncIterable<TunnelWsMessage> {
    for (;;) {
      const item: WsInboundItem = await state.inboundQueue.get();
      if (item === null) {
        // Local abort / session teardown sentinel — end without a synthetic close.
        return;
      }
      if (item[0] === 'data') {
        yield { kind: 'bytes', data: item[1] };
        continue;
      }
      if (item[0] === 'text') {
        yield { kind: 'text', data: item[1] };
        continue;
      }
      // 'close' — the runner closed its side; surface it then end the iterator.
      yield { kind: 'close', code: item[1][0], reason: item[1][1] };
      return;
    }
  }

  return {
    chId,
    async sendBytes(bytes: Uint8Array): Promise<void> {
      if (closed) {
        return;
      }
      await sendFrame({
        kind: FrameKind.WsFrame,
        chId,
        data: Buffer.from(bytes).toString('base64'),
        encoding: 'base64',
      });
    },
    async sendText(text: string): Promise<void> {
      if (closed) {
        return;
      }
      await sendFrame({ kind: FrameKind.WsFrame, chId, data: text, encoding: 'utf-8' });
    },
    messages,
    async close(code = 1000, reason = ''): Promise<void> {
      if (closed) {
        return;
      }
      closed = true;
      try {
        await sendFrame({ kind: FrameKind.WsClose, chId, code, reason });
      } catch {
        // best-effort close; the session may already be gone.
      }
      registry.closeWsChannel(runnerId, chId, session);
    },
  };
}
