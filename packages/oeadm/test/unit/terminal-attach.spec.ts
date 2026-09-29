// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import {
  attachTerminal,
  terminalAttachUrl,
  ABNORMAL_CLOSURE,
  NORMAL_CLOSURE,
  type MinimalWebSocket,
} from '../../src/terminal-attach.js';

/**
 * A fake WebSocket that records sent bytes and lets a test drive its events.
 *
 * The single `addEventListener(type, listener)` here bridges the WHATWG surface
 * onto {@link EventEmitter}, so it is deliberately looser than
 * {@link MinimalWebSocket}'s per-event overloads. Use {@link FakeSocket.asWebSocket}
 * to hand it to the proxy as a {@link MinimalWebSocket} — that cast is the one
 * place the WHATWG-vs-EventEmitter impedance is acknowledged, keeping `tsc`
 * (via the `typecheck` script) honest about the production `MinimalWebSocket`
 * contract everywhere else.
 */
class FakeSocket extends EventEmitter {
  binaryType = 'blob';
  sent: Uint8Array[] = [];
  closed = false;

  send(data: Uint8Array): void {
    this.sent.push(data);
  }
  close(): void {
    this.closed = true;
  }
  addEventListener(type: string, listener: (ev: unknown) => void): void {
    this.on(type, listener);
  }
  fire(type: 'open' | 'close' | 'error' | 'message', ev?: unknown): void {
    this.emit(type, ev);
  }

  /** View this fake as the {@link MinimalWebSocket} the proxy consumes. */
  asWebSocket(): MinimalWebSocket {
    return this as unknown as MinimalWebSocket;
  }
}

describe('terminalAttachUrl', () => {
  it('maps http → ws and builds the registry attach path', () => {
    expect(terminalAttachUrl('http://localhost:8080', 'ses_1', 'term_1')).toBe(
      'ws://localhost:8080/v1/sessions/ses_1/terminals/term_1/attach',
    );
  });

  it('maps https → wss and percent-encodes ids', () => {
    expect(terminalAttachUrl('https://reg.example.com', 'ses/1', 'term 2')).toBe(
      'wss://reg.example.com/v1/sessions/ses%2F1/terminals/term%202/attach',
    );
  });
});

describe('attachTerminal', () => {
  it('dials the attach URL with the api-key header and proxies bytes both ways', async () => {
    const socket = new FakeSocket();
    let dialedUrl = '';
    let dialedHeaders: Record<string, string> = {};
    const input = new PassThrough();
    const output = new PassThrough();
    const outChunks: Buffer[] = [];
    output.on('data', (c: Buffer) => outChunks.push(c));

    const done = attachTerminal({
      config: { baseURL: 'http://localhost:8080', apiKey: 'sk-term' },
      sessionId: 'ses_1',
      terminalId: 'term_1',
      input,
      output,
      webSocket: (url, options) => {
        dialedUrl = url;
        dialedHeaders = options.headers;
        return socket.asWebSocket();
      },
    });

    // Open, then the loop should wire stdin → socket.send.
    socket.fire('open');
    input.write(Buffer.from('ls\n'));
    // Inbound pty bytes → stdout.
    socket.fire('message', { data: new TextEncoder().encode('file-a\n').buffer });
    socket.fire('close', { code: 1000 });

    const result = await done;

    expect(result).toEqual({ code: 1000 });
    expect(dialedUrl).toBe('ws://localhost:8080/v1/sessions/ses_1/terminals/term_1/attach');
    expect(dialedHeaders['x-api-key']).toBe('sk-term');
    expect(Buffer.concat(socket.sent.map((u) => Buffer.from(u))).toString()).toBe('ls\n');
    expect(Buffer.concat(outChunks).toString()).toBe('file-a\n');
  });

  it('rejects when the socket errors before opening', async () => {
    const socket = new FakeSocket();
    const done = attachTerminal({
      config: { baseURL: 'http://localhost:8080', apiKey: 'sk-1' },
      sessionId: 'ses_1',
      terminalId: 'term_1',
      input: new PassThrough(),
      output: new PassThrough(),
      webSocket: () => socket.asWebSocket(),
    });
    socket.fire('error', new Error('connection refused'));
    await expect(done).rejects.toThrow(/connection refused/);
  });

  // The registry's own words for why it refused. `attachTerminal` already
  // received this on the close event and dropped it, leaving `attachCommand` a
  // bare 1008 it could not explain — and the operator an optimistic banner.
  it('carries the registry close reason on a refused attach', async () => {
    const socket = new FakeSocket();
    const done = attachTerminal({
      config: { baseURL: 'http://localhost:8080', apiKey: 'sk-1' },
      sessionId: 'ses_1',
      terminalId: 'term_missing',
      input: new PassThrough(),
      output: new PassThrough(),
      webSocket: () => socket.asWebSocket(),
    });

    socket.fire('open');
    socket.fire('close', { code: 1008, reason: 'terminal not found' });

    expect(await done).toEqual({ code: 1008, reason: 'terminal not found' });
  });

  // A mid-stream fault resolves rather than rejects (the pty bytes already
  // written are real), but it must resolve as a FAILURE carrying the cause.
  it('carries the Error when the socket faults mid-stream', async () => {
    const socket = new FakeSocket();
    const done = attachTerminal({
      config: { baseURL: 'http://localhost:8080', apiKey: 'sk-1' },
      sessionId: 'ses_1',
      terminalId: 'term_1',
      input: new PassThrough(),
      output: new PassThrough(),
      webSocket: () => socket.asWebSocket(),
    });

    socket.fire('open');
    socket.fire('error', new Error('read ECONNRESET'));

    const result = await done;
    expect(result.code).toBe(1011);
    expect(result.error?.message).toMatch(/ECONNRESET/);
  });

  // The seam types `code` as optional, so a close with none is reachable. It
  // used to default to NORMAL_CLOSURE, which `attachCommand` reads as success —
  // no evidence of a clean detach became evidence of one, and a socket that
  // simply vanished exited 0 under the optimistic banner.
  it('treats a close carrying no code as abnormal, not as a clean detach', async () => {
    const socket = new FakeSocket();
    const done = attachTerminal({
      config: { baseURL: 'http://localhost:8080', apiKey: 'sk-1' },
      sessionId: 'ses_1',
      terminalId: 'term_1',
      input: new PassThrough(),
      output: new PassThrough(),
      webSocket: () => socket.asWebSocket(),
    });

    socket.fire('open');
    socket.fire('close', {});

    const result = await done;
    expect(result.code).toBe(ABNORMAL_CLOSURE);
    expect(result.code).not.toBe(NORMAL_CLOSURE);
  });

  it('reports the whole outcome, not just a code, to onClose', async () => {
    const socket = new FakeSocket();
    const seen: Array<{ code: number; reason?: string }> = [];
    const done = attachTerminal({
      config: { baseURL: 'http://localhost:8080', apiKey: 'sk-1' },
      sessionId: 'ses_1',
      terminalId: 'term_1',
      input: new PassThrough(),
      output: new PassThrough(),
      webSocket: () => socket.asWebSocket(),
      onClose: (result) => seen.push(result),
    });

    socket.fire('open');
    socket.fire('close', { code: 1006, reason: 'abnormal closure' });
    await done;

    expect(seen).toEqual([{ code: 1006, reason: 'abnormal closure' }]);
  });
});
