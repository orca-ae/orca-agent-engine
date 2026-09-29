// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `codex` bridge entrypoint — the child process the `codex app-server` spawns as its
// `orca` MCP server.
//
// The native-CLI tool-bridge is provider-agnostic: it reattaches a {@link SandboxHandle} to
// the session's work-dir root and serves the orca built-ins (bash/read/write/edit/glob/grep
// + sys_terminal_*) over MCP-over-stdio, so the CLI's tool calls execute INSIDE the session
// sandbox rather than on the bridge process's cwd. The `claude-code` provider already
// authored that entrypoint ({@link runBridgeEntry}); codex wires the SAME command (via its
// `-c mcp_servers.orca.command/.args` overrides) so there is ONE bridge implementation
// across every native-CLI provider.
//
// This module is codex's own concrete entry SCRIPT (so the codex provider points at a stable
// per-provider path, and so a test can run it under `tsx`): it re-exports the bridge flag
// names and delegates to the shared {@link runBridgeEntry}. It self-invokes when run as the
// entry script and stays inert when imported (vitest).

import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import {
  runBridgeEntry,
  BRIDGE_ROOT_FLAG,
  BRIDGE_TMUX_SOCKET_FLAG,
  BRIDGE_ALLOWED_TOOLS_FLAG,
} from '../claude-code/bridge-runtime.js';

// Re-export the bridge flag names so the codex launch-arg builder + the codex provider
// reference them from the codex module (one import site per provider).
export {
  runBridgeEntry,
  BRIDGE_ROOT_FLAG,
  BRIDGE_TMUX_SOCKET_FLAG,
  BRIDGE_ALLOWED_TOOLS_FLAG,
} from '../claude-code/bridge-runtime.js';

/**
 * Whether this module is the process entry point (vs imported by a test). The bridge entry
 * is launched as `node <this module> …` by codex; when imported (vitest) it must NOT
 * auto-run. Compares the resolved entry script against this module's own file path.
 */
function isEntryModule(): boolean {
  if (typeof process === 'undefined' || !Array.isArray(process.argv)) {
    return false;
  }
  const entry = process.argv[1];
  if (entry === undefined) {
    return false;
  }
  return resolve(entry) === fileURLToPath(import.meta.url);
}

// Reference the re-exported flags so they are retained even if a downstream consumer only
// imports them transitively — keeps the codex module the single flag import site.
void BRIDGE_ROOT_FLAG;
void BRIDGE_TMUX_SOCKET_FLAG;
void BRIDGE_ALLOWED_TOOLS_FLAG;

if (isEntryModule()) {
  runBridgeEntry().catch((err: unknown) => {
    process.stderr.write(`codex bridge entry failed: ${String(err)}\n`);
    process.exit(1);
  });
}
