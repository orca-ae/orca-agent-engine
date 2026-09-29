// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// `oeadm attach --session <id>` — join an EXISTING session and co-drive it. A
// pure post-only client: it never creates or binds a runner (binding is
// owner-only, server-side), it just validates the session exists, then posts
// turns to `POST /v1/sessions/:id/events` and renders the shared SSE tail — the
// same loop as `run`.
//
// With `--terminal <id>` it instead proxies the operator's terminal to a running
// agent terminal over the registry terminal-attach WebSocket route.

import { parseArgs, requireString, optionalString } from '../args.js';
import type { Palette } from '../colors.js';
import type { OrcaClient, ClientConfig } from '../client.js';
import { runSessionChat, type ChatIo } from '../session-chat.js';
import {
  attachTerminal,
  NORMAL_CLOSURE,
  type TerminalAttachResult,
  type WebSocketCtor,
} from '../terminal-attach.js';

/** Options for {@link attachCommand}. */
export interface AttachCommandOptions {
  args: readonly string[];
  client: OrcaClient;
  io: ChatIo;
  colors: Palette;
  /**
   * Config used only for the `--terminal` WebSocket dial (base URL + auth). When
   * omitted, `--terminal` is unavailable (the event co-drive loop still works).
   */
  config?: ClientConfig;
  /** Injected WebSocket constructor for `--terminal` (defaults to the Node global). */
  webSocket?: WebSocketCtor;
}

/**
 * Attach to `--session`. Without `--terminal`, runs the interactive co-drive
 * loop. With `--terminal <id>`, proxies the terminal over the attach WS. Throws
 * when `--session` is missing.
 */
export async function attachCommand(opts: AttachCommandOptions): Promise<void> {
  const { options } = parseArgs(opts.args);
  const sessionId = requireString(options, 'session');
  const terminalId = optionalString(options, 'terminal');

  if (terminalId !== undefined) {
    if (opts.config === undefined) {
      throw new Error('--terminal requires a resolved client config');
    }
    opts.io.write(
      opts.colors.dim(`attaching to terminal ${opts.colors.bold(terminalId)} — Ctrl-C to detach`),
    );
    const attachOpts = {
      config: opts.config,
      sessionId,
      terminalId,
      ...(opts.webSocket !== undefined ? { webSocket: opts.webSocket } : {}),
    };
    const result = await attachTerminal(attachOpts);
    // Anything but a clean detach is a FAILED attach and must reach the shell as
    // one. The banner above is optimistic — it is printed before the dial — so
    // returning here on a `1008 terminal not found` or a mid-stream reset left
    // the operator reading "attaching to terminal …" and an exit status of 0.
    if (result.code !== NORMAL_CLOSURE) {
      throw new Error(describeAttachFailure(terminalId, result));
    }
    return;
  }

  // Validate the session exists (and is visible to this api key) before entering
  // the loop, so a bad id fails fast with a clean error instead of a silent
  // no-op stream.
  const session = await opts.client.getSession(sessionId);
  opts.io.write(
    opts.colors.dim(`attached to session ${opts.colors.bold(session.id)} — Ctrl-D to exit`),
  );

  await runSessionChat({
    sessionId: session.id,
    client: opts.client,
    io: opts.io,
    colors: opts.colors,
  });
}

/**
 * Describe a failed terminal attach for the operator.
 *
 * Every fact the socket gave us is surfaced: the close code, the registry's own
 * `reason` when it sent one (that is where "terminal not found" lives), and the
 * socket fault's message on a mid-stream error. Reporting only the code left the
 * operator guessing between a missing terminal, a revoked key, and a dropped
 * connection — three different next actions behind one number.
 */
function describeAttachFailure(terminalId: string, result: TerminalAttachResult): string {
  const detail = result.reason ?? result.error?.message;
  const suffix = detail !== undefined && detail.length > 0 ? `: ${detail}` : '';
  return `terminal attach to ${terminalId} failed (close ${result.code})${suffix}`;
}
