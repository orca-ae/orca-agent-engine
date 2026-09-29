// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The native-CLI bridge RUNTIME — the reusable core every provider's bridge entrypoint
// delegates to, and every provider's harness module (`claude-code/index.ts` and the sibling
// `codex`/`cursor`/`pi`/`custom` `index.ts` files) imports the flag constants from.
//
// This module is deliberately GUARD-FREE: no "am I the process entry point" check, no
// self-invoking top-level code. It exists ONLY so a provider harness module can import the
// flag constants (to build the bridge child's argv) WITHOUT also dragging in the self-invoking
// entry guard that `bridge-entry.ts` adds on top of this. See `bridge-entry.ts`'s module doc for
// why that split is load-bearing: `main.ts`'s import graph reaches every provider's `index.ts`
// (it registers all providers on every runner boot), and tsup's multi-entry `--no-splitting`
// build bundles each entry point (`main.ts`, `bridge-entry.ts`, …) fully self-contained — so
// importing a guarded module transitively embeds its guard into `main.ts`'s bundle too, where it
// misfires (see `bridge-entry.ts`).
//
// The actual bridge behavior, unchanged from before the split:
//   1. reads the session sandbox work-dir `--root` (and, on the local-attach transport, the
//      `--tmux-socket`) from argv — the coordinates of the sandbox tree the runner acquired for
//      the session,
//   2. REATTACHES a {@link SandboxHandle} to that same root (see `reattach-sandbox.ts`),
//   3. serves the native-CLI tool-bridge ({@link serveNativeCliBridge}) over the CALLING
//      process's stdin/stdout, so the CLI's tool calls (bash/read/write/edit/glob/grep +
//      sys_terminal_*) execute INSIDE the session sandbox rather than on that process's cwd.
//
// It exits cleanly when the CLI closes the stdio transport (the parent disconnected) or on a
// signal.

import { serveNativeCliBridge } from '../../mcp/native-cli-bridge.js';
import { reattachSessionSandbox } from './reattach-sandbox.js';

/** The `--root` flag: the session sandbox work-dir the bridge binds its tools to. */
export const BRIDGE_ROOT_FLAG = '--root';
/** The `--tmux-socket` flag: the socket for a tmux-backed (local-attach) session. */
export const BRIDGE_TMUX_SOCKET_FLAG = '--tmux-socket';
/**
 * The `--allowed-tools` flag: a comma-separated skill-allowlist restricting the surface.
 *
 * PRESENT-BUT-EMPTY IS NOT ABSENT. `allowed_tool_names: []` means "the composition
 * left this agent no runnable orca tool" (an mcp-only agent is the ordinary case),
 * and must deny every tool — the same thing `[]` means to the claude provider's
 * `buildOrcaSdkTools`. ABSENT means "no restriction". So the flag is passed with an
 * empty value for the empty allowlist and omitted only when the snapshot carries no
 * allowlist at all, and {@link parseAllowedToolsFlag} keeps the two apart.
 */
export const BRIDGE_ALLOWED_TOOLS_FLAG = '--allowed-tools';

/** Read the string value following `flag` in `argv`, or `undefined` when absent. */
function argValue(argv: string[], flag: string): string | undefined {
  const i = argv.indexOf(flag);
  return i >= 0 && i + 1 < argv.length ? argv[i + 1] : undefined;
}

/**
 * Parse {@link BRIDGE_ALLOWED_TOOLS_FLAG} out of `argv`: `undefined` when the flag
 * is absent (no restriction), otherwise the parsed list — `[]` for a present-but-
 * empty value, which denies every tool.
 */
export function parseAllowedToolsFlag(argv: string[]): string[] | undefined {
  const raw = argValue(argv, BRIDGE_ALLOWED_TOOLS_FLAG);
  if (raw === undefined) return undefined;
  return raw.split(',').filter((s) => s.length > 0);
}

/**
 * Append {@link BRIDGE_ALLOWED_TOOLS_FLAG} to a bridge child's `args` for the
 * snapshot's `allowed_tool_names`. The inverse of {@link parseAllowedToolsFlag}:
 * an absent allowlist omits the flag, and an EMPTY allowlist emits it with an
 * empty value (deny-all).
 *
 * Shared by all five native-CLI providers rather than repeated in each: the same
 * two-line guard written five times acquired the same defect five times, and a
 * fix in one copy is invisible to the other four.
 */
export function pushAllowedToolsFlag(args: string[], allowed: readonly string[] | undefined): void {
  if (allowed === undefined) return;
  args.push(BRIDGE_ALLOWED_TOOLS_FLAG, allowed.join(','));
}

/**
 * Run the bridge entrypoint against `argv` (defaults to `process.argv.slice(2)`). A plain
 * function with NO top-level side effect, so it is both unit-testable and safe for a provider
 * harness / entry module to import without triggering anything — each concrete `bridge-entry.ts`
 * self-invokes it below its own "am I the entry script" guard.
 */
export async function runBridgeEntry(argv: string[] = process.argv.slice(2)): Promise<void> {
  const root = argValue(argv, BRIDGE_ROOT_FLAG);
  if (root === undefined || root.length === 0) {
    throw new Error(`${BRIDGE_ROOT_FLAG} is required for the claude-code bridge entrypoint`);
  }
  const tmuxSocket = argValue(argv, BRIDGE_TMUX_SOCKET_FLAG);
  const allowedLogicalNames = parseAllowedToolsFlag(argv);

  const sandbox = reattachSessionSandbox({
    root,
    ...(tmuxSocket !== undefined && tmuxSocket.length > 0 ? { tmuxSocket } : {}),
  });

  const bridge = await serveNativeCliBridge(sandbox, {
    ...(allowedLogicalNames !== undefined ? { allowedLogicalNames } : {}),
  });

  const shutdown = async (): Promise<void> => {
    await bridge.close().catch(() => {});
    await sandbox.destroy().catch(() => {});
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
  // When the CLI closes the stdio transport, end this process cleanly.
  bridge.onClose(() => void shutdown());
}
