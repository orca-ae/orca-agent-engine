// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `codex` native-CLI launch-arg builder.
//
// The `codex` provider boots the real `codex` binary as an APP-SERVER — `codex app-server`
// — and drives it over its JSON-RPC-over-stdio protocol (initialize → thread/start →
// turn/start; the app-server streams `item/*` and `turn/*` notifications back, and raises
// `on-request` approval REQUESTS to the client). This module turns a session's boot inputs
// into the {@link NativeCliLaunchConfig} the 4a native-CLI launcher ({@link launchNativeCli})
// runs — the argument grammar plus the credential-free env. The flags mirror the CANONICAL
// set codex's own app-server launch uses so a headless launch here behaves identically:
//
//   - `app-server` — the JSON-RPC stdio server subcommand. The harness is the JSON-RPC
//     CLIENT: it writes one request/response/notification JSON object per stdin line and
//     reads the app-server's own JSON objects one per stdout line.
//   - `-c approval_policy="on-request"` — codex asks the CLIENT to approve each risky
//     built-in action (shell exec / apply-patch) via a server→client approval request, so
//     the harness can route it to the uniform transcript approval gate (`confirmTool`) and
//     answer with the codex-native decision. (With NO gate wired, the policy is set to
//     `never` so the headless app-server never blocks on an approval nobody can answer.)
//   - `-c mcp_servers.orca.command=<cmd>` + `-c mcp_servers.orca.args=[...]` — wire the
//     runner's native-CLI tool-bridge as an MCP server under the reserved `orca` name, so
//     the model's orca built-ins (bash/read/write/edit/glob/grep + sys_terminal_*) resolve
//     INSIDE the per-session sandbox. Codex accepts nested config via dotted `-c` keys.
//   - `-c model="<id>"` when the snapshot pinned one (else the CLI's default).
//
// The gateway LLM egress (base URL + scoped session JWT) rides the process env as
// `OPENAI_BASE_URL` / `OPENAI_API_KEY` (credential-free — the gateway swaps the JWT for the
// real upstream secret). A private `CODEX_HOME` keeps the app-server off the operator's
// `~/.codex`, so a self-hosted session never pollutes host state.
//
// The builder is PURE (no I/O): the harness calls it once per session and hands the result
// to {@link launchNativeCli}.

import type { NativeCliLaunchConfig } from '../../sandbox/native-cli-launcher.js';

/**
 * The reserved MCP server name the native-CLI tool-bridge is registered under. The model
 * sees the orca built-ins as `mcp__orca__<tool>` — the same identity the in-process claude
 * surface, the STDIO bridge, and the claude-code native-CLI provider advertise.
 */
export const CODEX_MCP_SERVER_NAME = 'orca';

/** The default binary name resolved on the sandbox PATH when no path is pinned. */
export const DEFAULT_CODEX_CLI = 'codex';

/**
 * The `on-request` approval policy: codex raises a server→client approval REQUEST for each
 * risky built-in action, which the harness routes to the uniform transcript approval gate.
 */
export const CODEX_APPROVAL_POLICY_ON_REQUEST = 'on-request';

/**
 * The `never` approval policy: codex auto-approves its own built-in actions. Used when NO
 * confirmation gate is wired, so the headless app-server never blocks on an approval no one
 * can answer (the OS sandbox + composed tool policies are the guardrail).
 */
export const CODEX_APPROVAL_POLICY_NEVER = 'never';

/** How the app-server reaches the runner's native-CLI tool-bridge (its MCP-over-stdio server). */
export interface CodexBridgeCommand {
  /** The bridge entrypoint executable codex spawns (e.g. `node`). */
  command: string;
  /** Arguments to the bridge entrypoint (e.g. `[<bridge-entry.js>, '--root', <root>]`). */
  args: string[];
}

/** Inputs to {@link buildCodexLaunchConfig}. */
export interface CodexLaunchInput {
  /** Binary path/name for the CLI. Defaults to {@link DEFAULT_CODEX_CLI} on the PATH. */
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
  /** The native-CLI tool-bridge command codex wires as its `orca` MCP server. */
  bridge: CodexBridgeCommand;
  /**
   * Whether a confirmation gate is wired. `true` → `approval_policy=on-request` (codex
   * raises approval requests the harness routes to the gate); `false` →
   * `approval_policy=never` (the OS sandbox + policies are the guardrail; the headless
   * app-server never blocks on an approval no one can answer).
   */
  gateEnabled: boolean;
  /** The gateway LLM-proxy base URL → `OPENAI_BASE_URL`, when set. */
  baseURL?: string;
  /** The scoped session JWT → `OPENAI_API_KEY` (credential-free), when set. */
  apiKey?: string;
  /**
   * A private `CODEX_HOME` for the app-server, when set — keeps codex's config/state off
   * the operator's `~/.codex`. Absent → codex uses its ambient home.
   */
  codexHome?: string;
}

/**
 * Build the {@link NativeCliLaunchConfig} that boots `codex app-server` for a session. See
 * the module header for the full argument grammar.
 */
export function buildCodexLaunchConfig(input: CodexLaunchInput): NativeCliLaunchConfig {
  const args: string[] = [...(input.prefixArgs ?? [])];

  // The JSON-RPC stdio app-server subcommand.
  args.push('app-server');

  // Approval policy: route codex's own built-in-action approvals to the gate, OR auto-approve.
  const approvalPolicy = input.gateEnabled
    ? CODEX_APPROVAL_POLICY_ON_REQUEST
    : CODEX_APPROVAL_POLICY_NEVER;
  args.push('-c', `approval_policy=${JSON.stringify(approvalPolicy)}`);

  // Wire the native-CLI tool-bridge as the `orca` MCP server (dotted-key TOML fragments,
  // which codex accepts via `-c`). The values are JSON — a valid TOML basic string / array.
  args.push(
    '-c',
    `mcp_servers.${CODEX_MCP_SERVER_NAME}.command=${JSON.stringify(input.bridge.command)}`,
  );
  args.push('-c', `mcp_servers.${CODEX_MCP_SERVER_NAME}.args=${JSON.stringify(input.bridge.args)}`);

  if (input.model !== undefined && input.model.length > 0) {
    args.push('-c', `model=${JSON.stringify(input.model)}`);
  }

  args.push(...(input.suffixArgs ?? []));

  const config: NativeCliLaunchConfig = {
    cmd:
      input.cliPath !== undefined && input.cliPath.length > 0 ? input.cliPath : DEFAULT_CODEX_CLI,
    args,
  };

  const env = buildEnv(input);
  if (Object.keys(env).length > 0) {
    config.env = env;
  }
  return config;
}

/**
 * Build the credential-free CLI env: the gateway LLM base URL + scoped JWT (only when
 * present) plus the private CODEX_HOME (only when set). Everything is omitted when the boot
 * context supplies nothing, so the CLI then uses its own ambient auth + home.
 */
function buildEnv(input: CodexLaunchInput): Record<string, string> {
  const env: Record<string, string> = {};
  if (input.apiKey !== undefined && input.apiKey.length > 0) {
    env['OPENAI_API_KEY'] = input.apiKey;
  }
  if (input.baseURL !== undefined && input.baseURL.length > 0) {
    env['OPENAI_BASE_URL'] = input.baseURL;
  }
  if (input.codexHome !== undefined && input.codexHome.length > 0) {
    env['CODEX_HOME'] = input.codexHome;
  }
  return env;
}
