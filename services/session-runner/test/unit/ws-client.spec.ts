// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner's outbound WebSocket seam — the `ws`-backed
// {@link WsRunnerTunnelSocket} the serve loop drives.
//
// The contract under test is the socket's EVENT ORDERING, which the serve loop
// depends on but cannot itself express: `ws` may emit `error` immediately before
// `close`, and the serve loop's fatal / recycle classification keys on the close
// CODE. So `close` is the single source of truth for the exit code, and `error`
// only records a message for the imminent close to carry as its reason. A close
// synthesized on `error` carries no code, becomes the sticky close, and pre-empts
// the real one — which is exactly how a fatal 4004 binding refusal was lost under
// load, leaving the runner reconnecting forever instead of exiting.
//
// Driven against a minimal EventEmitter standing in for the `ws` socket: the
// ordering is the thing being asserted, so the test has to drive the events
// itself rather than hope a live socket produces the interleaving.

import { EventEmitter } from 'node:events';
import { describe, it, expect } from 'vitest';
import type { WebSocket } from 'ws';
import { WsRunnerTunnelSocket } from '../../src/tunnel/ws-client.js';

/** The slice of the `ws` WebSocket surface {@link WsRunnerTunnelSocket} uses. */
class FakeWs extends EventEmitter {
  readonly sent: string[] = [];
  closedWith: { code?: number | undefined; reason?: string | undefined } | undefined;
  /** When set, the next `send` fails with this error (the write-after-close path). */
  sendError: Error | undefined;

  send(data: string, cb: (err?: Error) => void): void {
    if (this.sendError !== undefined) {
      cb(this.sendError);
      return;
    }
    this.sent.push(data);
    cb();
  }

  close(code?: number, reason?: string): void {
    this.closedWith = { code, reason };
  }
}

function makeSocket(): { fake: FakeWs; socket: WsRunnerTunnelSocket } {
  const fake = new FakeWs();
  return { fake, socket: new WsRunnerTunnelSocket(fake as unknown as WebSocket) };
}

describe('WsRunnerTunnelSocket', () => {
  it('carries the server close CODE when `error` fires immediately before `close`', async () => {
    // THE regression. `ws` emits `error` then `close` for a server that refuses an
    // accepted socket, and both can be parsed out of one TCP read — so the serve
    // loop sees whichever message this seam pushes FIRST. It must be the real
    // close, code and all: 4004 is in the serve loop's fatal set, and losing the
    // code downgrades a permanent binding refusal to a generic reconnect.
    const { fake, socket } = makeSocket();

    fake.emit('error', new Error('WebSocket was closed before the connection was established'));
    fake.emit('close', 4004, Buffer.from(''));

    const msg = await socket.receive();
    expect(msg.type).toBe('close');
    expect(msg).toMatchObject({ type: 'close', code: 4004 });
    // And the sticky close keeps reporting the same code on every later receive(),
    // so a serve loop that re-reads after teardown still classifies it as fatal.
    expect(await socket.receive()).toMatchObject({ type: 'close', code: 4004 });
  });

  it('falls back to the preceding error message as the close reason', async () => {
    // The other half of the same fix: `error` is not discarded, it is DEFERRED.
    // An abrupt 1006 drop carries no reason frame, so the recorded error message
    // is what makes the disconnect diagnosable in the reconnect log.
    const { fake, socket } = makeSocket();

    fake.emit('error', new Error('boom'));
    fake.emit('close', 1006, Buffer.from(''));

    expect(await socket.receive()).toEqual({ type: 'close', code: 1006, reason: 'boom' });
  });

  it('prefers the close frame reason over a preceding error message', async () => {
    const { fake, socket } = makeSocket();

    fake.emit('error', new Error('boom'));
    fake.emit('close', 4004, Buffer.from('runner_id does not match tunnel token'));

    expect(await socket.receive()).toEqual({
      type: 'close',
      code: 4004,
      reason: 'runner_id does not match tunnel token',
    });
  });

  it('queues inbound text frames in order and drops binary ones', async () => {
    // The tunnel protocol is text-only JSON; a binary frame is not a message the
    // serve loop can decode, so it never reaches the queue.
    const { fake, socket } = makeSocket();

    fake.emit('message', 'first', false);
    fake.emit('message', Buffer.from([0x00, 0x01]), true);
    fake.emit('message', 'second', false);

    expect(await socket.receive()).toEqual({ type: 'text', data: 'first' });
    expect(await socket.receive()).toEqual({ type: 'text', data: 'second' });
  });

  it('drains queued frames received before the close, then sticks on the close', async () => {
    const { fake, socket } = makeSocket();

    fake.emit('message', 'hello-ack', false);
    fake.emit('close', 1000, Buffer.from('normal closure'));

    expect(await socket.receive()).toEqual({ type: 'text', data: 'hello-ack' });
    expect(await socket.receive()).toEqual({
      type: 'close',
      code: 1000,
      reason: 'normal closure',
    });
    expect(await socket.receive()).toEqual({
      type: 'close',
      code: 1000,
      reason: 'normal closure',
    });
  });

  it('resolves a waiter parked on receive() when a message arrives', async () => {
    const { fake, socket } = makeSocket();

    const pending = socket.receive();
    fake.emit('message', 'late', false);

    expect(await pending).toEqual({ type: 'text', data: 'late' });
  });

  it('rejects sendText when the underlying write fails (a non-OPEN socket)', async () => {
    // The rejection the serve loop's pong path must catch: `ws.send` on a socket
    // that is no longer OPEN routes to sendAfterClose and calls back with an error.
    const { fake, socket } = makeSocket();
    fake.sendError = new Error('WebSocket is not open: readyState 3 (CLOSED)');

    await expect(socket.sendText('{"kind":"pong"}')).rejects.toThrow(/WebSocket is not open/);
  });

  it('closes best-effort: forwards the code, and swallows a throwing close', () => {
    const { fake, socket } = makeSocket();

    socket.closeSocket(1000, 'runner shutdown');
    expect(fake.closedWith).toEqual({ code: 1000, reason: 'runner shutdown' });

    // Teardown runs from several places (serve-loop finally, stop()'s safety net),
    // so a close on an already-destroyed socket must not throw into the caller.
    fake.close = () => {
      throw new Error('already destroyed');
    };
    expect(() => socket.closeSocket(1000, 'runner shutdown')).not.toThrow();
  });
});
