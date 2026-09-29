// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `cursor` native-CLI launch-arg builder.
//
// The `cursor` provider boots the real `cursor-agent` binary HEADLESS and speaks its
// newline-delimited-JSON ("stream-json") protocol over stdio — the same transport shape the
// `claude-code` provider uses for the headless `claude` binary. This module turns a session's
// boot inputs into the {@link NativeCliLaunchConfig} the 4a native-CLI launcher
// ({@link launchNativeCli}) runs — the argument grammar plus the credential-free env. The
// flags mirror a headless `cursor-agent` launch so a session here behaves identically to an
// interactive one:
//
//   - `--print --output-format stream-json` — headless print mode: the harness drives each
//     turn by writing the user text to stdin and reads the CLI's stream-json stdout live
//     (assistant text / thinking / tool calls / a terminal result), one JSON object per line.
//   - `--model <id>` when the snapshot pinned one (else the CLI's default / `auto` select).
//   - `--mcp-config <json>` wiring the runner's native-CLI tool-bridge as an MCP server under
//     the reserved `orca` name, so the model's built-ins (bash/read/write/edit/glob/grep +
//     sys_terminal_*) resolve INSIDE the per-session sandbox rather than on the runner host.
//   - the permission wiring, which is EITHER/OR:
//       · gated (the default) when the runner wired a confirmation gate — cursor-agent raises
//         a tool-permission request over the stream-json control channel for each gated tool
//         call, which the harness routes to the uniform transcript approval and answers over
//         stdin;
//       · `--force` when NO gate is wired — the OS sandbox + the registry-composed tool
//         policies are the guardrail, so the headless CLI must not block on an interactive
//         prompt it can never satisfy.
//   - `--system-prompt <s>` for the snapshot's agent system prompt, when non-empty.
//
// Cursor authenticates against its OWN backend (there is no per-provider gateway routing the
// way the claude/codex CLIs have): the scoped session credential rides the process env as
// `CURSOR_API_KEY`, and a base-URL override (when the deployment fronts Cursor's API with a
// gateway) rides `CURSOR_API_BASE_URL`. Both are credential-free from the runner's view — the
// gateway swaps the scoped JWT for the real upstream secret. Everything is omitted when the
// boot context supplies nothing, so the CLI then uses its own ambient auth.
//
// The builder is PURE (no I/O): the harness calls it once per session and hands the result to
// {@link launchNativeCli}.

import type { NativeCliLaunchConfig } from '../../sandbox/native-cli-launcher.js';

/**
 * The reserved MCP server name the native-CLI tool-bridge is registered under. The CLI
 * exposes its tools as `mcp__<name>__<tool>`, so the model sees the orca built-ins as
 * `mcp__orca__*` — the same identity the in-process claude surface, the STDIO bridge, and the
 * claude-code / codex native-CLI providers advertise.
 */
export const CURSOR_MCP_SERVER_NAME = 'orca';

/** The default binary name resolved on the sandbox PATH when no path is pinned. */
export const DEFAULT_CURSOR_CLI = 'cursor-agent';

/** How the CLI reaches the runner's native-CLI tool-bridge (its MCP-over-stdio server). */
export interface CursorBridgeCommand {
  /** The bridge entrypoint executable the CLI spawns (e.g. `node`). */
  command: string;
  /** Arguments to the bridge entrypoint (e.g. `[<bridge-entry.js>, '--root', <root>]`). */
  args: string[];
}

/** Inputs to {@link buildCursorLaunchConfig}. */
export interface CursorLaunchInput {
  /** Binary path/name for the CLI. Defaults to {@link DEFAULT_CURSOR_CLI} on the PATH. */
  cliPath?: string;
  /**
   * Extra prefix args inserted BEFORE the generated flags (e.g. a fake CLI script path under
   * `node` in tests). Empty in production.
   */
  prefixArgs?: string[];
  /** Extra suffix args appended AFTER the generated flags (e.g. a test probe flag). */
  suffixArgs?: string[];
  /** The effective model id, or `undefined` to let the CLI pick its default / `auto`. */
  model?: string;
  /** The native-CLI tool-bridge command the CLI wires as its `orca` MCP server. */
  bridge: CursorBridgeCommand;
  /**
   * Whether a confirmation gate is wired. `true` → the CLI prompts for each gated tool call
   * over the stream-json control channel (routed to the approval gate); `false` → `--force`
   * (the OS sandbox + policies are the guardrail; the headless CLI never blocks on a prompt).
   */
  gateEnabled: boolean;
  /** The snapshot's agent system prompt to append, when non-empty. */
  system?: string;
  /** The gateway/base-URL override → `CURSOR_API_BASE_URL`, when set. */
  baseURL?: string;
  /** The scoped session credential → `CURSOR_API_KEY` (credential-free), when set. */
  apiKey?: string;
  /** Sandbox-visible working directory for the CLI. */
  cwd?: string;
}

/**
 * Build the {@link NativeCliLaunchConfig} that boots the headless `cursor-agent` binary for a
 * session. See the module header for the full argument grammar.
 */
export function buildCursorLaunchConfig(input: CursorLaunchInput): NativeCliLaunchConfig {
  const args: string[] = [...(input.prefixArgs ?? [])];

  // Headless print mode + stream-json output: one JSON object per stdout line.
  args.push('--print', '--output-format', 'stream-json');

  if (input.model !== undefined && input.model.length > 0) {
    args.push('--model', input.model);
  }

  // Wire the native-CLI tool-bridge as the `orca` MCP server so the model's built-ins resolve
  // inside the sandbox.
  const mcpConfig = {
    mcpServers: {
      [CURSOR_MCP_SERVER_NAME]: {
        command: input.bridge.command,
        args: input.bridge.args,
      },
    },
  };
  args.push('--mcp-config', JSON.stringify(mcpConfig));

  // Permission wiring: with a gate, cursor prompts over the stream-json control channel (the
  // harness answers); with no gate, `--force` auto-approves so the headless CLI never blocks.
  if (!input.gateEnabled) {
    args.push('--force');
  }

  if (input.system !== undefined && input.system.trim().length > 0) {
    args.push('--system-prompt', input.system);
  }

  args.push(...(input.suffixArgs ?? []));

  const config: NativeCliLaunchConfig = {
    cmd:
      input.cliPath !== undefined && input.cliPath.length > 0 ? input.cliPath : DEFAULT_CURSOR_CLI,
    args,
  };

  const env = buildEnv(input);
  if (Object.keys(env).length > 0) {
    config.env = env;
  }
  if (input.cwd !== undefined && input.cwd.length > 0) {
    config.cwd = input.cwd;
  }
  return config;
}

/**
 * Build the credential-free CLI env: the scoped Cursor API key + base-URL override, only when
 * present. Both are omitted entirely when no egress is supplied (the CLI then uses its own
 * ambient auth).
 */
function buildEnv(input: CursorLaunchInput): Record<string, string> {
  const env: Record<string, string> = {};
  if (input.apiKey !== undefined && input.apiKey.length > 0) {
    env['CURSOR_API_KEY'] = input.apiKey;
  }
  if (input.baseURL !== undefined && input.baseURL.length > 0) {
    env['CURSOR_API_BASE_URL'] = input.baseURL;
  }
  return env;
}
