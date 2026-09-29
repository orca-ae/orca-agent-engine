// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner-side remote terminal-attach.
//
// The runner exposes a tunneled WS-channel handler at `attach/:terminalId` that
// BRIDGES a live tmux pane's pty to the WS channel:
//   - server → client: raw pane output bytes are pushed as binary WS frames;
//   - client → server: binary WS frames are typed into the pane (input bytes),
//     and a `{ "type": "resize", "cols", "rows" }` TEXT control resizes the pane.
//
// Two layers are exercised:
//   1. The low-level {@link TmuxSandboxHandle.attachTerminal} pty primitive against
//      a real tmux pane (self-skips when `tmux` is not on PATH).
//   2. The full runner path: the attach handler registered on a RouteDispatcher,
//      served through the real serve loop against the in-process fake registry
//      runner-tunnel peer, driven by a tunneled WS channel to `attach/<id>`. A
//      byte written on the channel round-trips back through the pane's `cat` echo,
//      proving the bidirectional pty bridge end-to-end with no registry app.

import { afterEach, beforeEach, describe, it, expect } from 'vitest';
import { execFileSync } from 'node:child_process';
import { RUNNER_TERMINAL_ATTACH_PATH, tokenBoundRunnerId } from '@orca/harness-tunnel';
import { serveTunnel, type ServeTunnelHandle } from '../../src/tunnel/serve.js';
import {
  RouteDispatcher,
  type WsChannel,
  type WsChannelMessage,
} from '../../src/tunnel/request-dispatch.js';
import { TmuxSandboxRuntime, type SandboxHandle } from '../../src/sandbox/tmux-sandbox.js';
import { asTerminalHost } from '../../src/tools/sys-terminal.js';
import type {
  AttachedTerminal,
  AttachTerminalOptions,
  TerminalHost,
} from '../../src/tools/sys-terminal.js';
import {
  parseResizeControl,
  registerTerminalAttach,
  TERMINAL_ATTACH_PATH,
  TERMINAL_ATTACH_ROUTE,
} from '../../src/tunnel/terminal-attach.js';
import { FakeRegistryRunnerTunnel } from './support/fake-registry-runner-tunnel.js';

const BINDING_TOKEN = 'binding-token-attach';
const RUNNER_ID = tokenBoundRunnerId(BINDING_TOKEN);

/** True when a usable `tmux` binary is on PATH (the suite self-skips otherwise). */
function tmuxAvailable(): boolean {
  try {
    execFileSync('tmux', ['-V'], { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const describeTmux = tmuxAvailable() ? describe : describe.skip;

describeTmux('TmuxSandboxHandle.attachTerminal (live pty bridge)', () => {
  async function withHandle(fn: (handle: SandboxHandle) => Promise<void>): Promise<void> {
    const runtime = new TmuxSandboxRuntime();
    const sandbox = await runtime.acquire({});
    try {
      await fn(sandbox);
    } finally {
      await sandbox.destroy();
    }
  }

  it('streams raw pane output and types input bytes into the pane (cat echo)', async () => {
    await withHandle(async (sandbox) => {
      const host = asTerminalHost(sandbox);
      expect(host).not.toBeNull();
      // `cat` echoes its stdin back to stdout — a clean bidirectional round-trip
      // over the pty with no rendering assumptions beyond the echoed bytes.
      const { terminalId } = await host!.launchTerminal({ command: 'cat' });

      const chunks: Buffer[] = [];
      const attached = await host!.attachTerminal(terminalId, {
        onData: (bytes) => chunks.push(Buffer.from(bytes)),
      });
      try {
        // Give pipe-pane a moment to attach, then type a line: cat echoes it (the
        // TTY also echoes the typed characters, so the marker appears in the
        // streamed pane bytes regardless of cat's own echo).
        await delay(150);
        attached.write(Buffer.from('orca-attach-marker\n'));
        const deadline = Date.now() + 8000;
        while (!Buffer.concat(chunks).toString('utf8').includes('orca-attach-marker')) {
          if (Date.now() >= deadline) {
            throw new Error(
              `no marker in streamed bytes; saw: ${Buffer.concat(chunks).toString('utf8')}`,
            );
          }
          await delay(50);
        }
        expect(Buffer.concat(chunks).toString('utf8')).toContain('orca-attach-marker');
      } finally {
        await attached.detach();
      }
    });
  }, 20_000);

  it('resize does not throw and the pane accepts the new dimensions', async () => {
    await withHandle(async (sandbox) => {
      const host = asTerminalHost(sandbox);
      const { terminalId } = await host!.launchTerminal({ command: 'cat', cols: 80, rows: 24 });
      const attached = await host!.attachTerminal(terminalId, { onData: () => {} });
      try {
        await attached.resize(120, 40);
        // A second resize + a write still works (the pane is live).
        await attached.resize(100, 30);
        attached.write(Buffer.from('x'));
      } finally {
        await attached.detach();
      }
    });
  }, 20_000);

  it('detach is idempotent and stops delivering pane bytes', async () => {
    await withHandle(async (sandbox) => {
      const host = asTerminalHost(sandbox);
      const { terminalId } = await host!.launchTerminal({ command: 'cat' });
      let count = 0;
      const attached = await host!.attachTerminal(terminalId, {
        onData: () => {
          count += 1;
        },
      });
      await delay(100);
      await attached.detach();
      await attached.detach(); // idempotent — no throw
      const afterDetach = count;
      attached.write(Buffer.from('ignored\n')); // best-effort no-op after detach
      await delay(200);
      expect(count).toBe(afterDetach);
    });
  }, 20_000);
});

describeTmux('remote terminal-attach over the runner tunnel (end-to-end)', () => {
  let registry: FakeRegistryRunnerTunnel;

  beforeEach(async () => {
    registry = new FakeRegistryRunnerTunnel({ runnerId: RUNNER_ID, bindingToken: BINDING_TOKEN });
    await registry.listen();
  });

  afterEach(async () => {
    await registry.close();
  });

  it('bridges a client WS channel through the tunnel to a tmux pane (bidirectional bytes)', async () => {
    const runtime = new TmuxSandboxRuntime();
    const sandbox = await runtime.acquire({});
    const host = asTerminalHost(sandbox);
    expect(host).not.toBeNull();
    const { terminalId } = await host!.launchTerminal({ command: 'cat' });

    const dispatcher = new RouteDispatcher();
    registerTerminalAttach(dispatcher, host!);

    const handle: ServeTunnelHandle = serveTunnel({
      dispatcher,
      registryRunnerUrl: registry.baseUrl(),
      runnerId: RUNNER_ID,
      bindingToken: BINDING_TOKEN,
      runnerVersion: '0.1.0-test',
    });

    try {
      const runner = await registry.nextRunner();
      // Open the tunneled attach channel exactly the way the registry proxy will:
      // ws.open on `attach/<terminalId>`.
      const channel = runner.openWsChannel('ch-term-1', `${TERMINAL_ATTACH_PATH}/${terminalId}`);

      // A resize control (text) must be accepted without disrupting the byte pipe.
      channel.sendText(JSON.stringify({ type: 'resize', cols: 132, rows: 43 }));
      // Input bytes: `cat` echoes them back; they arrive as binary WS frames.
      await delay(150);
      channel.sendBytes(Buffer.from('tunnel-echo-42\n'));

      const seen: Buffer[] = [];
      const deadline = Date.now() + 8000;
      while (!Buffer.concat(seen).toString('utf8').includes('tunnel-echo-42')) {
        if (Date.now() >= deadline) {
          throw new Error(`no echo in channel bytes; saw: ${Buffer.concat(seen).toString('utf8')}`);
        }
        const item = await Promise.race([channel.recv(), delay(300).then(() => null)]);
        if (item !== null && item.kind === 'bytes') {
          seen.push(Buffer.from(item.data as Uint8Array));
        }
      }
      expect(Buffer.concat(seen).toString('utf8')).toContain('tunnel-echo-42');

      channel.close(1000, 'bye');
    } finally {
      await handle.stop();
      await sandbox.destroy();
    }
  }, 30_000);

  it('closes the channel when the target terminal id is unknown', async () => {
    const runtime = new TmuxSandboxRuntime();
    const sandbox = await runtime.acquire({});
    const host = asTerminalHost(sandbox);
    const dispatcher = new RouteDispatcher();
    registerTerminalAttach(dispatcher, host!);
    const handle = serveTunnel({
      dispatcher,
      registryRunnerUrl: registry.baseUrl(),
      runnerId: RUNNER_ID,
      bindingToken: BINDING_TOKEN,
      runnerVersion: '0.1.0-test',
    });
    try {
      const runner = await registry.nextRunner();
      const channel = runner.openWsChannel('ch-term-x', `${TERMINAL_ATTACH_PATH}/does-not-exist`);
      const closed = await channel.closed();
      // A non-1000 close signals the attach was refused (unknown terminal).
      expect(closed.code).not.toBe(1000);
    } finally {
      await handle.stop();
      await sandbox.destroy();
    }
  }, 20_000);
});

// ── Pure resize-control parser (no tmux, always runs) ────────────────────────
//
// parseResizeControl is the exported, pure text→control parser the bridge uses to
// turn a channel TEXT frame into a pane resize. Its defensive rejection branches
// (bad JSON, wrong/absent type, non-positive / non-integer dimensions) are what
// keep a malformed control from crashing the byte pump, and they were previously
// only exercised indirectly through the tmux-gated end-to-end path (one
// well-formed resize). These cover the rejection branches directly.

describe('parseResizeControl', () => {
  it('parses a well-formed resize control', () => {
    expect(parseResizeControl(JSON.stringify({ type: 'resize', cols: 132, rows: 43 }))).toEqual({
      cols: 132,
      rows: 43,
    });
  });

  it('ignores extra fields on an otherwise well-formed control', () => {
    expect(
      parseResizeControl(JSON.stringify({ type: 'resize', cols: 80, rows: 24, extra: 'x' })),
    ).toEqual({ cols: 80, rows: 24 });
  });

  it('returns undefined for non-JSON text', () => {
    expect(parseResizeControl('not json at all')).toBeUndefined();
    expect(parseResizeControl('')).toBeUndefined();
  });

  it('returns undefined for JSON that is not an object', () => {
    expect(parseResizeControl('42')).toBeUndefined();
    expect(parseResizeControl('"resize"')).toBeUndefined();
    expect(parseResizeControl('null')).toBeUndefined();
    expect(parseResizeControl('[1,2,3]')).toBeUndefined();
  });

  it('returns undefined when the type is missing or not "resize"', () => {
    expect(parseResizeControl(JSON.stringify({ cols: 80, rows: 24 }))).toBeUndefined();
    expect(
      parseResizeControl(JSON.stringify({ type: 'input', cols: 80, rows: 24 })),
    ).toBeUndefined();
  });

  it('returns undefined for non-positive dimensions', () => {
    expect(
      parseResizeControl(JSON.stringify({ type: 'resize', cols: 0, rows: 24 })),
    ).toBeUndefined();
    expect(
      parseResizeControl(JSON.stringify({ type: 'resize', cols: 80, rows: -1 })),
    ).toBeUndefined();
  });

  it('returns undefined for non-integer dimensions', () => {
    expect(
      parseResizeControl(JSON.stringify({ type: 'resize', cols: 80.5, rows: 24 })),
    ).toBeUndefined();
  });

  it('returns undefined for non-number dimensions', () => {
    expect(
      parseResizeControl(JSON.stringify({ type: 'resize', cols: '80', rows: 24 })),
    ).toBeUndefined();
    expect(parseResizeControl(JSON.stringify({ type: 'resize', cols: 80 }))).toBeUndefined();
  });
});

// ── Runner pty-bridge handler against fakes (no tmux, always runs) ───────────
//
// The tmux-gated suites above self-skip on a CI host without a `tmux` binary,
// leaving the runner half of terminal-attach (bridgeTerminalAttach: the live-pane
// byte pump, the unknown-terminal 1011 refusal, resize + input over the channel,
// detach-without-kill) with ZERO executed coverage there. These tmux-free tests
// drive `registerTerminalAttach`'s handler directly against a fake TerminalHost +
// fake WsChannel, so the handler logic is guarded regardless of tmux.

/** A controllable in-memory {@link WsChannel} the test feeds inbound messages to. */
class FakeWsChannel implements WsChannel {
  private readonly teardownController = new AbortController();
  readonly teardownSignal = this.teardownController.signal;
  accepted = false;
  readonly sentBytes: Uint8Array[] = [];
  readonly sentText: string[] = [];
  closeCode: number | undefined;
  closeReason: string | undefined;
  closed = false;

  private readonly queue: WsChannelMessage[] = [];
  private resolveNext: ((r: IteratorResult<WsChannelMessage>) => void) | null = null;
  private ended = false;

  constructor(
    readonly path: string,
    readonly queryString = '',
  ) {}

  async accept(): Promise<void> {
    this.accepted = true;
  }

  /** Enqueue an inbound message the handler's `messages()` loop will observe. */
  push(msg: WsChannelMessage): void {
    if (this.resolveNext !== null) {
      const r = this.resolveNext;
      this.resolveNext = null;
      r({ value: msg, done: false });
      return;
    }
    this.queue.push(msg);
  }

  /** Signal end-of-stream (the peer closed): ends the handler's `messages()` loop. */
  end(): void {
    this.ended = true;
    if (this.resolveNext !== null) {
      const r = this.resolveNext;
      this.resolveNext = null;
      r({ value: undefined, done: true });
    }
  }

  messages(): AsyncIterable<WsChannelMessage> {
    // Arrow `next` closes over `this` directly (no `this` alias), so the async
    // iterator reads the same queue/ended/resolveNext the `push`/`end` calls drive.
    const next = (): Promise<IteratorResult<WsChannelMessage>> => {
      const queued = this.queue.shift();
      if (queued !== undefined) {
        return Promise.resolve({ value: queued, done: false });
      }
      if (this.ended) {
        return Promise.resolve({ value: undefined, done: true });
      }
      return new Promise((resolve) => {
        this.resolveNext = resolve;
      });
    };
    return {
      [Symbol.asyncIterator](): AsyncIterator<WsChannelMessage> {
        return { next };
      },
    };
  }

  async sendText(text: string): Promise<void> {
    this.sentText.push(text);
  }

  async sendBytes(bytes: Uint8Array): Promise<void> {
    this.sentBytes.push(bytes);
  }

  async close(code?: number, reason?: string): Promise<void> {
    this.closed = true;
    this.closeCode = code;
    this.closeReason = reason;
    // A peer/handler close also ends any in-flight `messages()` iteration.
    this.end();
  }

  /** Fire the TUNNEL-teardown signal (the serve loop's socket dropped under the channel). */
  tearDown(): void {
    this.teardownController.abort();
  }
}

/**
 * A {@link FakeWsChannel} whose BINARY sends always fail — the tunnel frame path is broken
 * while the pane keeps producing. Text sends and the inbound side are untouched, so the
 * only thing under test is what the bridge does about undeliverable output.
 */
class FailingSendWsChannel extends FakeWsChannel {
  override sendBytes(_bytes: Uint8Array): Promise<void> {
    return Promise.reject(new Error('socket write failed'));
  }
}

/** A fake {@link AttachedTerminal} recording writes/resizes/detaches. */
class FakeAttachedTerminal implements AttachedTerminal {
  readonly writes: Uint8Array[] = [];
  readonly resizes: Array<{ cols: number; rows: number }> = [];
  detachCount = 0;

  write(bytes: Uint8Array): void {
    this.writes.push(bytes);
  }

  async resize(cols: number, rows: number): Promise<void> {
    this.resizes.push({ cols, rows });
  }

  async detach(): Promise<void> {
    this.detachCount += 1;
  }
}

/**
 * A minimal {@link TerminalHost} whose `attachTerminal` succeeds only for a known
 * id (throwing otherwise, exactly as the tmux host does for an unknown pane) and
 * hands back a {@link FakeAttachedTerminal} whose `onData` sink the test can pump.
 */
class FakeTerminalHost implements TerminalHost {
  lastOnData: ((bytes: Uint8Array) => void) | null = null;
  lastAttached: FakeAttachedTerminal | null = null;

  constructor(private readonly knownId: string) {}

  async launchTerminal(): Promise<{ terminalId: string }> {
    return { terminalId: this.knownId };
  }
  async sendTerminalKeys(): Promise<void> {}
  async readTerminal(): Promise<string> {
    return '';
  }
  async listTerminals(): Promise<never[]> {
    return [];
  }
  async closeTerminal(): Promise<void> {}

  async attachTerminal(terminalId: string, opts: AttachTerminalOptions): Promise<AttachedTerminal> {
    if (terminalId !== this.knownId) {
      throw new Error(`unknown terminal ${terminalId}`);
    }
    this.lastOnData = opts.onData;
    const attached = new FakeAttachedTerminal();
    this.lastAttached = attached;
    return attached;
  }
}

/** Resolve the WS handler `registerTerminalAttach` registered for the route. */
function terminalAttachHandler(host: TerminalHost) {
  const dispatcher = new RouteDispatcher();
  registerTerminalAttach(dispatcher, host);
  const handler = dispatcher.wsHandlerFor(`${TERMINAL_ATTACH_PATH}/whatever`);
  expect(handler).toBeDefined();
  return handler!;
}

describe('bridgeTerminalAttach (runner pty-bridge, tmux-free)', () => {
  it('streams pane bytes out and pumps input bytes + resize into the pane', async () => {
    const host = new FakeTerminalHost('term-1');
    const handler = terminalAttachHandler(host);
    const channel = new FakeWsChannel(`${TERMINAL_ATTACH_PATH}/term-1`);

    const done = handler(channel);

    // Give the handler a tick to accept + attach so the onData sink is wired.
    await new Promise((r) => setImmediate(r));
    expect(channel.accepted).toBe(true);
    expect(host.lastOnData).not.toBeNull();

    // server → client: raw pane bytes are pushed as binary frames verbatim.
    host.lastOnData!(Buffer.from('pane-output'));

    // client → server: a resize control (text) resizes the pane; input bytes are
    // typed in verbatim; an unrecognized control frame is dropped (not an error).
    channel.push({ kind: 'text', data: JSON.stringify({ type: 'resize', cols: 120, rows: 40 }) });
    channel.push({ kind: 'text', data: JSON.stringify({ type: 'not-a-resize' }) });
    channel.push({ kind: 'bytes', data: Buffer.from('typed-input') });
    channel.end();

    await done;

    expect(Buffer.concat(channel.sentBytes).toString('utf8')).toBe('pane-output');
    expect(host.lastAttached!.resizes).toEqual([{ cols: 120, rows: 40 }]);
    expect(host.lastAttached!.writes.map((w) => Buffer.from(w).toString('utf8'))).toEqual([
      'typed-input',
    ]);
    // Peer close (end of messages) always detaches, never kills the terminal.
    expect(host.lastAttached!.detachCount).toBe(1);
    // A clean end-of-stream is not a refusal — the channel was not force-closed.
    expect(channel.closeCode).toBeUndefined();
  });

  it('reports the first undeliverable pane chunk and closes the channel', async () => {
    // A failed `sendBytes` used to be swallowed whole. That is only harmless while the
    // channel is CLOSING: for any other failure the `messages()` loop does not end, so the
    // `finally`'s detach never runs, the pane keeps producing, and every chunk is dropped
    // with no trace — a terminal that looks live and is frozen. The bridge must say so
    // (once — a broken channel fails on every chunk, and a log per byte is its own outage)
    // and close, which ends the loop and unwinds through the detach.
    const host = new FakeTerminalHost('term-1');
    const warnings: Array<{ obj: unknown; msg: string | undefined }> = [];
    const dispatcher = new RouteDispatcher();
    registerTerminalAttach(dispatcher, host, {
      logger: {
        warn: (obj: unknown, msg?: string) => warnings.push({ obj, msg }),
      },
    });
    const handler = dispatcher.wsHandlerFor(`${TERMINAL_ATTACH_PATH}/term-1`)!;
    const channel = new FailingSendWsChannel(`${TERMINAL_ATTACH_PATH}/term-1`);

    const done = handler(channel);
    await new Promise((r) => setImmediate(r));
    expect(host.lastOnData).not.toBeNull();

    host.lastOnData!(Buffer.from('one'));
    host.lastOnData!(Buffer.from('two'));
    host.lastOnData!(Buffer.from('three'));

    await done;

    // Exactly ONE report, naming the terminal…
    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.msg).toMatch(/output send failed/);
    expect((warnings[0]!.obj as { terminalId?: string }).terminalId).toBe('term-1');
    // …the channel was force-closed, which is what ended the message loop…
    expect(channel.closed).toBe(true);
    expect(channel.closeCode).toBe(1011);
    // …and the pane was DETACHED on the way out, never killed.
    expect(host.lastAttached!.detachCount).toBe(1);
  });

  it('does not force-close a send failure that is just the tunnel tearing down', async () => {
    // Teardown is the benign case the old empty catch was written for: `messages()` is
    // already ending and the `finally`'s detach stops the stream, so the failure is noted
    // but the channel is left alone — synthesizing a close here would race the serve loop's
    // own teardown close.
    const host = new FakeTerminalHost('term-1');
    const warnings: Array<{ obj: unknown; msg: string | undefined }> = [];
    const dispatcher = new RouteDispatcher();
    registerTerminalAttach(dispatcher, host, {
      logger: {
        warn: (obj: unknown, msg?: string) => warnings.push({ obj, msg }),
      },
    });
    const handler = dispatcher.wsHandlerFor(`${TERMINAL_ATTACH_PATH}/term-1`)!;
    const channel = new FailingSendWsChannel(`${TERMINAL_ATTACH_PATH}/term-1`);

    const done = handler(channel);
    await new Promise((r) => setImmediate(r));
    channel.tearDown();
    host.lastOnData!(Buffer.from('one'));
    await new Promise((r) => setImmediate(r));

    // Noted, but NOT closed by the bridge.
    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.msg).toMatch(/on a closing channel/);
    expect(channel.closed).toBe(false);

    // The serve loop ends the message stream on teardown; the bridge then detaches.
    channel.end();
    await done;
    expect(host.lastAttached!.detachCount).toBe(1);
  });

  it('refuses with 1011 when no terminal host is available (resolver returns null)', async () => {
    const dispatcher = new RouteDispatcher();
    registerTerminalAttach(dispatcher, () => null);
    const handler = dispatcher.wsHandlerFor(`${TERMINAL_ATTACH_PATH}/term-1`)!;
    const channel = new FakeWsChannel(`${TERMINAL_ATTACH_PATH}/term-1`);

    await handler(channel);

    expect(channel.closed).toBe(true);
    expect(channel.closeCode).toBe(1011);
    // The attach never got as far as accepting the channel.
    expect(channel.accepted).toBe(false);
  });

  it('refuses with 1011 when the terminal id is unknown (attach throws)', async () => {
    const host = new FakeTerminalHost('the-only-known-id');
    const handler = terminalAttachHandler(host);
    const channel = new FakeWsChannel(`${TERMINAL_ATTACH_PATH}/does-not-exist`);

    await handler(channel);

    // The channel was accepted (the id was present) but the attach failed → 1011.
    expect(channel.accepted).toBe(true);
    expect(channel.closed).toBe(true);
    expect(channel.closeCode).toBe(1011);
  });

  it('refuses with 1011 when the path carries no terminal id', async () => {
    const host = new FakeTerminalHost('term-1');
    // Register on the concrete route and drive the handler with the bare base path
    // (no :terminalId segment) so terminalIdFromPath yields undefined.
    const dispatcher = new RouteDispatcher();
    registerTerminalAttach(dispatcher, host);
    const handler = dispatcher.wsHandlerFor(`${TERMINAL_ATTACH_PATH}/x`)!;
    const channel = new FakeWsChannel(TERMINAL_ATTACH_PATH);

    await handler(channel);

    expect(channel.closed).toBe(true);
    expect(channel.closeCode).toBe(1011);
  });

  it('registers the handler on the :terminalId-templated route', () => {
    const host = new FakeTerminalHost('term-1');
    const dispatcher = new RouteDispatcher();
    registerTerminalAttach(dispatcher, host);
    // The concrete attach path resolves to the templated route's handler.
    expect(dispatcher.wsHandlerFor(`${TERMINAL_ATTACH_PATH}/abc123`)).toBeDefined();
    // Registering twice throws (duplicate route) — proving the fixed route key.
    expect(() => registerTerminalAttach(dispatcher, host)).toThrow();
    // The route constant is the base path with the :terminalId template segment.
    expect(TERMINAL_ATTACH_ROUTE).toBe(`${TERMINAL_ATTACH_PATH}/:terminalId`);
  });

  it('sources the attach base path from the shared @orca/harness-tunnel wire symbol', () => {
    // The runner's base path IS the shared wire-path constant — the registry proxy
    // opens on the very same symbol, so the wire contract is one literal, not two
    // copies kept in sync by convention.
    expect(TERMINAL_ATTACH_PATH).toBe(RUNNER_TERMINAL_ATTACH_PATH);
  });
});
