// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// `oeadm run --agent <id> --environment <id>` — create a fresh session, then
// enter the interactive chat loop against it. A pure client flow: create the
// session over the existing `POST /v1/sessions` route, surface its id, and hand
// off to {@link runSessionChat}. The client + IO are injected so the whole
// command is unit tested against a fake registry with scripted stdin.

import { parseArgs, requireString, optionalString } from '../args.js';
import type { Palette } from '../colors.js';
import type { OrcaClient } from '../client.js';
import { runSessionChat, type ChatIo } from '../session-chat.js';

/** Options for {@link runCommand}. */
export interface RunCommandOptions {
  args: readonly string[];
  client: OrcaClient;
  io: ChatIo;
  colors: Palette;
}

/**
 * Create a session for `--agent` (optionally targeting `--environment`) and run
 * the interactive loop. Throws when `--agent` is missing (the caller maps that
 * to a usage error + non-zero exit).
 */
export async function runCommand(opts: RunCommandOptions): Promise<void> {
  const { options } = parseArgs(opts.args);
  const agentId = requireString(options, 'agent');
  const environmentId = optionalString(options, 'environment');

  const session = await opts.client.createSession(
    environmentId !== undefined ? { agentId, environmentId } : { agentId },
  );
  opts.io.write(opts.colors.dim(`session ${opts.colors.bold(session.id)} — Ctrl-D to exit`));

  await runSessionChat({
    sessionId: session.id,
    client: opts.client,
    io: opts.io,
    colors: opts.colors,
  });
}
