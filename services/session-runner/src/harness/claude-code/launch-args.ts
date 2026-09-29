// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `claude-code` native-CLI launch-arg builder.
//
// The `claude-code` provider boots the real `claude` binary HEADLESS and speaks its
// newline-delimited-JSON ("stream-json") protocol over stdio. This module turns a
// session's boot inputs into the {@link NativeCliLaunchConfig} the 4a native-CLI
// launcher ({@link launchNativeCli}) runs — the argument grammar plus the credential-
// free env. The flags mirror the CANONICAL set the Claude Agent SDK's own process
// transport passes to the binary, so a headless launch here behaves identically to an
// in-process SDK `query()`:
//
//   - `--output-format stream-json --verbose --input-format stream-json
//     --include-partial-messages` — the harness drives each turn by writing a
//     stream-json user frame to stdin and reads the CLI's stream-json stdout live
//     (assistant text / thinking deltas / tool_use / tool_result / result). `--verbose`
//     is required alongside stream-json output.
//   - `--model <id>` when the snapshot pinned one (else the CLI's default).
//   - `--mcp-config <json>` wiring the runner's native-CLI tool-bridge as an MCP server
//     under the reserved `orca` name, so the model's built-ins (bash/read/write/edit/
//     glob/grep + sys_terminal_*) resolve INSIDE the per-session sandbox. Any wired MCP
//     server also sets `--strict-mcp-config` so the CLI does not ALSO read an ambient
//     `.mcp.json` off the runner host.
//   - the permission wiring, which is EITHER/OR:
//       · `--permission-prompt-tool stdio` when the runner wired a confirmation gate —
//         the CLI then raises a `can_use_tool` control_request over stream-json for each
//         gated tool call, which the harness routes to the uniform transcript approval
//         and answers over stdin;
//       · `--dangerously-skip-permissions` when NO gate is wired — the OS sandbox + the
//         registry-composed tool policies are the guardrail, so the headless CLI must
//         not block on an interactive TTY prompt it can never satisfy.
//   - `--plugin-dir <dir>` when the snapshot carries a Skills plugin dir — the runner
//     fills it in after materializing the pushed Skill bundles — so the CLI discovers
//     the bundled skills natively, and `--append-system-prompt <s>` for the snapshot's
//     agent system prompt (Skills add nothing to it).
//
// The gateway LLM egress (base URL + scoped session JWT) rides the process env as
// `ANTHROPIC_BASE_URL` / `ANTHROPIC_API_KEY` (credential-free — the gateway swaps the
// JWT for the real upstream secret). `CLAUDE_CODE_ENTRYPOINT` is pinned so a headless
// child launched from within another claude session is still a clean top-level session.
//
// The builder is PURE (no I/O): the harness calls it once per session and hands the
// result to {@link launchNativeCli}.

import type { NativeCliLaunchConfig } from '../../sandbox/native-cli-launcher.js';

/**
 * The reserved MCP server name the native-CLI tool-bridge is registered under. The CLI
 * exposes its tools as `mcp__<name>__<tool>`, so the model sees the orca built-ins as
 * `mcp__orca__*` — the same identity the in-process claude surface + the STDIO bridge
 * advertise. Matches {@link ORCA_MCP_SERVER_NAME} in `harness/claude/mcp-tools.ts`.
 */
export const CLAUDE_CODE_MCP_SERVER_NAME = 'orca';

/** The default binary name resolved on the sandbox PATH when no path is pinned. */
export const DEFAULT_CLAUDE_CLI = 'claude';

/**
 * The entrypoint tag stamped on the CLI's env. Distinguishes a runner-launched
 * headless session from an interactive one and from an in-process SDK launch, and (by
 * being non-empty + non-`sdk`) keeps a child launched from within another claude
 * session a clean top-level session.
 */
export const CLAUDE_CODE_ENTRYPOINT = 'orca-session-runner';

/** How the CLI reaches the runner's native-CLI tool-bridge (its MCP-over-stdio server). */
export interface ClaudeCodeBridgeCommand {
  /** The bridge entrypoint executable the CLI spawns (e.g. `node`). */
  command: string;
  /** Arguments to the bridge entrypoint (e.g. `[<bridge-entry.js>, '--root', <root>]`). */
  args: string[];
}

/** Inputs to {@link buildClaudeCodeLaunchConfig}. */
export interface ClaudeCodeLaunchInput {
  /** Binary path/name for the CLI. Defaults to {@link DEFAULT_CLAUDE_CLI} on the PATH. */
  cliPath?: string;
  /**
   * Extra prefix args inserted BEFORE the generated flags (e.g. a fake CLI script path
   * under `node` in tests). Empty in production.
   */
  prefixArgs?: string[];
  /** Extra suffix args appended AFTER the generated flags (e.g. a test probe flag). */
  suffixArgs?: string[];
  /** The effective model id, or `undefined` to let the CLI pick its default. */
  model?: string;
  /** The native-CLI tool-bridge command the CLI wires as its `orca` MCP server. */
  bridge: ClaudeCodeBridgeCommand;
  /**
   * Whether a confirmation gate is wired. `true` → `--permission-prompt-tool stdio`
   * (route permissions to the approval gate); `false` → `--dangerously-skip-permissions`
   * (the OS sandbox + policies are the guardrail).
   */
  gateEnabled: boolean;
  /** A skills snapshot dir to load via `--plugin-dir`, when the snapshot carries one. */
  pluginDir?: string;
  /** The snapshot's agent system prompt to append, when non-empty. */
  system?: string;
  /** The gateway LLM-proxy base URL → `ANTHROPIC_BASE_URL`, when set. */
  baseURL?: string;
  /** The scoped session JWT → `ANTHROPIC_API_KEY` (credential-free), when set. */
  apiKey?: string;
  /** Sandbox-visible working directory for the CLI. */
  cwd?: string;
}

/**
 * Build the {@link NativeCliLaunchConfig} that boots the headless `claude` binary for a
 * session. See the module header for the full argument grammar.
 */
export function buildClaudeCodeLaunchConfig(input: ClaudeCodeLaunchInput): NativeCliLaunchConfig {
  const args: string[] = [...(input.prefixArgs ?? [])];

  // Stream-json in BOTH directions + partial messages. `--verbose` is required for
  // stream-json output.
  args.push(
    '--output-format',
    'stream-json',
    '--verbose',
    '--input-format',
    'stream-json',
    '--include-partial-messages',
  );

  if (input.model !== undefined && input.model.length > 0) {
    args.push('--model', input.model);
  }

  // Wire the native-CLI tool-bridge as the `orca` MCP server so the model's built-ins
  // resolve inside the sandbox; strict so the CLI ignores an ambient `.mcp.json`.
  const mcpConfig = {
    mcpServers: {
      [CLAUDE_CODE_MCP_SERVER_NAME]: {
        command: input.bridge.command,
        args: input.bridge.args,
      },
    },
  };
  args.push('--mcp-config', JSON.stringify(mcpConfig), '--strict-mcp-config');

  // Permission wiring: route to the approval gate over stdio, OR skip permissions.
  if (input.gateEnabled) {
    args.push('--permission-prompt-tool', 'stdio');
  } else {
    args.push('--dangerously-skip-permissions');
  }

  if (input.pluginDir !== undefined && input.pluginDir.length > 0) {
    args.push('--plugin-dir', input.pluginDir);
  }

  if (input.system !== undefined && input.system.trim().length > 0) {
    args.push('--append-system-prompt', input.system);
  }

  args.push(...(input.suffixArgs ?? []));

  const config: NativeCliLaunchConfig = {
    cmd:
      input.cliPath !== undefined && input.cliPath.length > 0 ? input.cliPath : DEFAULT_CLAUDE_CLI,
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
 * Build the credential-free CLI env: the gateway LLM base URL + scoped JWT (only when
 * present) plus the pinned entrypoint tag. The tag is always set so a headless child is
 * a clean top-level session; the `ANTHROPIC_*` vars are omitted entirely when no egress
 * is supplied (the CLI then uses its own ambient auth).
 */
function buildEnv(input: ClaudeCodeLaunchInput): Record<string, string> {
  const env: Record<string, string> = { CLAUDE_CODE_ENTRYPOINT: CLAUDE_CODE_ENTRYPOINT };
  if (input.apiKey !== undefined && input.apiKey.length > 0) {
    env['ANTHROPIC_API_KEY'] = input.apiKey;
  }
  if (input.baseURL !== undefined && input.baseURL.length > 0) {
    env['ANTHROPIC_BASE_URL'] = input.baseURL;
  }
  return env;
}
