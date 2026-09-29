// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The generic custom-provider LAUNCH-ARG builder.
//
// The `custom` provider boots ANY operator-declared CLI via the 4a native-CLI launcher
// ({@link launchNativeCli}). This module turns a parsed {@link CustomAgentSpec} + a session's boot
// context into the {@link NativeCliLaunchConfig} the launcher runs: it substitutes the spec's argv
// and env `{...}` placeholders from the boot context and layers the spec env over the credential-
// free gateway LLM egress (exposed under neutral `ORCA_LLM_*` names a generic CLI can read).
//
// Supported placeholders (in argv strings AND env values):
//   - `{sessionId}` / `{workspaceId}` — the session identity;
//   - `{model}` — the effective model id (snapshot's, else the configured default);
//   - `{systemPrompt}` — the composed agent/skills system prompt;
//   - `{sandboxRoot}` — the session sandbox work-dir root (a host path);
//   - `{bridgeCommand}` — the native-CLI tool-bridge executable the CLI should spawn as its `orca`
//     MCP server (so the model's built-ins resolve inside the sandbox);
//   - `{bridgeArgsJson}` — the bridge's argv as a JSON array string (the CLI parses + passes it on).
// An UNKNOWN `{token}` is left intact (a literal the CLI may want), so the substitution never
// corrupts a value it does not own.
//
// The builder is PURE (no I/O): the harness calls it once per session and hands the result to
// {@link launchNativeCli}.

import type { NativeCliLaunchConfig } from '../../sandbox/native-cli-launcher.js';
import type { CustomAgentSpec } from './spec.js';

/** The neutral env var carrying the gateway LLM base URL for a generic CLI to read. */
export const ORCA_LLM_BASE_URL_ENV = 'ORCA_LLM_BASE_URL';
/** The neutral env var carrying the scoped session JWT (credential-free) for a generic CLI. */
export const ORCA_LLM_API_KEY_ENV = 'ORCA_LLM_API_KEY';

/** How the CLI reaches the runner's native-CLI tool-bridge (its MCP-over-stdio server). */
export interface CustomBridgeCommand {
  /** The bridge entrypoint executable the CLI spawns (e.g. `node`). */
  command: string;
  /** Arguments to the bridge entrypoint (e.g. `[<bridge-entry.js>, '--root', <root>]`). */
  args: string[];
}

/** Inputs to {@link buildCustomLaunchConfig}. */
export interface CustomLaunchInput {
  /** The parsed operator spec (command + argv template + env + cwd + stdout mapping). */
  spec: CustomAgentSpec;
  /** The native-CLI tool-bridge command the CLI wires as its `orca` MCP server. */
  bridge: CustomBridgeCommand;
  /** Orca session id (`{sessionId}`). */
  sessionId: string;
  /** Owning workspace (`{workspaceId}`). */
  workspaceId: string;
  /** The effective model id (`{model}`), when set. */
  model?: string;
  /** The composed system prompt (`{systemPrompt}`), when set. */
  system?: string;
  /** The session sandbox work-dir root (`{sandboxRoot}`), when set. */
  sandboxRoot?: string;
  /**
   * Override the spec's `command` with this binary path/name (a test injects a fake CLI here).
   * When absent the spec's `command` is used.
   */
  cliPath?: string;
  /** Extra prefix args inserted BEFORE the spec argv (e.g. a fake CLI script path under `node`). */
  prefixArgs?: string[];
  /** Extra suffix args appended AFTER the spec argv (e.g. a test probe flag). */
  suffixArgs?: string[];
  /** The gateway LLM-proxy base URL → `ORCA_LLM_BASE_URL`, when set. */
  baseURL?: string;
  /** The scoped session JWT → `ORCA_LLM_API_KEY` (credential-free), when set. */
  apiKey?: string;
}

/**
 * Build the {@link NativeCliLaunchConfig} that boots the operator's CLI for a session. See the
 * module header for the placeholder grammar.
 */
export function buildCustomLaunchConfig(input: CustomLaunchInput): NativeCliLaunchConfig {
  const subs = buildSubstitutions(input);

  const args: string[] = [
    ...(input.prefixArgs ?? []),
    ...input.spec.argv.map((arg) => substitute(arg, subs)),
    ...(input.suffixArgs ?? []),
  ];

  const cmd =
    input.cliPath !== undefined && input.cliPath.length > 0 ? input.cliPath : input.spec.command;
  const config: NativeCliLaunchConfig = { cmd, args };

  const env = buildEnv(input, subs);
  if (Object.keys(env).length > 0) {
    config.env = env;
  }
  if (input.spec.cwd !== undefined && input.spec.cwd.length > 0) {
    config.cwd = input.spec.cwd;
  }
  return config;
}

/** The `{token}` → value substitution map for this session. */
function buildSubstitutions(input: CustomLaunchInput): Record<string, string> {
  const subs: Record<string, string> = {
    sessionId: input.sessionId,
    workspaceId: input.workspaceId,
    bridgeCommand: input.bridge.command,
    bridgeArgsJson: JSON.stringify(input.bridge.args),
  };
  if (input.model !== undefined && input.model.length > 0) {
    subs['model'] = input.model;
  }
  if (input.system !== undefined) {
    subs['systemPrompt'] = input.system;
  }
  if (input.sandboxRoot !== undefined && input.sandboxRoot.length > 0) {
    subs['sandboxRoot'] = input.sandboxRoot;
  }
  return subs;
}

/**
 * Substitute every `{token}` in `value` from `subs`. A `{token}` with no matching key is left
 * INTACT (a literal the CLI may want), so substitution never corrupts a value it does not own.
 */
function substitute(value: string, subs: Record<string, string>): string {
  return value.replace(/\{([a-zA-Z0-9_]+)\}/g, (match, token: string) =>
    Object.prototype.hasOwnProperty.call(subs, token) ? (subs[token] as string) : match,
  );
}

/**
 * Build the credential-free CLI env: the spec's env (placeholders substituted) plus the gateway
 * LLM base URL + scoped JWT under the neutral `ORCA_LLM_*` names (only when present). Everything is
 * omitted when the boot context supplies nothing, so the CLI then uses its own ambient auth.
 */
function buildEnv(input: CustomLaunchInput, subs: Record<string, string>): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(input.spec.env)) {
    env[key] = substitute(value, subs);
  }
  if (input.apiKey !== undefined && input.apiKey.length > 0) {
    env[ORCA_LLM_API_KEY_ENV] = input.apiKey;
  }
  if (input.baseURL !== undefined && input.baseURL.length > 0) {
    env[ORCA_LLM_BASE_URL_ENV] = input.baseURL;
  }
  return env;
}
