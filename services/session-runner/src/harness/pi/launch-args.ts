// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `pi` native-CLI launch-arg builder.
//
// The `pi` provider boots the real `pi` binary HEADLESS in its JSON RPC mode (`pi --mode rpc`)
// and speaks pi's newline-delimited-JSON command/event protocol over stdio. This module turns a
// session's boot inputs into the {@link NativeCliLaunchConfig} the 4a native-CLI launcher
// ({@link launchNativeCli}) runs — the argument grammar plus the credential-free env. The flags
// mirror the CANONICAL set pi's own headless launch uses, so a headless launch here behaves
// identically to an interactive `pi` session driven programmatically:
//
//   - `--mode rpc` — pi's headless JSON stdio protocol: the harness (the CLIENT) writes one
//     command object per stdin line (`{type:"prompt",message,id}`) and reads pi's own responses
//     + streamed session events one JSON object per stdout line.
//   - `--no-session` — pi does not persist a session file on the runner host (the durable
//     transcript is Orca's, streamed up the tunnel by the runner; pi's conversation lives in its
//     process for the session's lifetime).
//   - `--provider <id> --model <id>` — pi authenticates from the per-session `models.json` the
//     harness renders into the managed agent dir (see {@link PI_PROVIDER_ID}); the provider id is
//     stable so `--provider` selects that entry. The model is pinned when the snapshot carried
//     one (else pi picks its default from the managed config).
//   - `--no-extensions --extension <path>` — wire the runner's native-CLI tool-bridge as a pi
//     EXTENSION, so the model's orca built-ins (bash/read/write/edit/glob/grep + sys_terminal_*)
//     resolve INSIDE the per-session sandbox. Pi has no MCP-config surface in RPC mode, so the
//     bridge is surfaced as an extension that spawns the shared bridge entry and registers each
//     tool onto pi. `--no-extensions` FIRST suppresses pi's ambient extension discovery (the
//     operator's `settings.json` on the runner host), then the explicit `--extension` loads ONLY
//     the orca bridge — the documented `--no-extensions --extension <ext>` idiom — so a self-hosted
//     session never inherits host extensions. The flag is singular (`--extension` / `-e`, repeatable);
//     the plural does not exist and is silently dropped by pi's arg parser.
//   - `--no-builtin-tools` — DISABLE pi's default in-process tools (`read`/`bash`/`edit`/`write`),
//     which are ENABLED by default. They would otherwise run un-gated by the approval gate, outside
//     the sandbox-scoped bridge, and shadow/duplicate the bridged orca tools. Extension-registered
//     tools (the orca bridge tools) stay enabled, so the model sees ONLY the sandbox-scoped,
//     approval-gated orca tools.
//   - `--append-system-prompt <s>` — the snapshot's agent system prompt, when non-empty.
//
// The gateway LLM egress (base URL + scoped session JWT) rides the process env as
// `ORCA_PI_LLM_BASE_URL` / `ORCA_PI_LLM_API_KEY` (credential-free — the harness renders them into
// the managed `models.json`, and the gateway swaps the JWT for the real upstream secret). A
// private `PI_CODING_AGENT_DIR` keeps pi's config/state off the operator's `~/.pi`, so a
// self-hosted session never pollutes host state.
//
// The builder is PURE (no I/O): the harness calls it once per session and hands the result to
// {@link launchNativeCli}.

import type { NativeCliLaunchConfig } from '../../sandbox/native-cli-launcher.js';

/** The default binary name resolved on the sandbox PATH when no path is pinned. */
export const DEFAULT_PI_CLI = 'pi';

/** Pi's headless JSON stdio mode: the harness drives commands + reads events over stdio. */
export const PI_RPC_MODE = 'rpc';

/**
 * The provider id the harness registers in the managed `models.json` and selects with
 * `--provider`. Stable (Orca-native) so the launch selection and the rendered credential entry
 * agree — the analog of codex routing through the gateway under a fixed profile.
 */
export const PI_PROVIDER_ID = 'orca';

/**
 * The gateway LLM-proxy base URL, carried on the pi process env (credential-free). The harness
 * reads it back to render the managed `models.json` `baseUrl`.
 */
export const PI_LLM_BASE_URL_ENV = 'ORCA_PI_LLM_BASE_URL';

/**
 * The scoped session JWT, carried on the pi process env (credential-free). The harness reads it
 * back to render the managed `models.json` credential (the gateway swaps it for the real secret).
 */
export const PI_LLM_API_KEY_ENV = 'ORCA_PI_LLM_API_KEY';

/**
 * Pi's own env var to relocate its config/state dir (default `~/.pi`). A private per-session dir
 * keeps pi's config/state off the operator's `~/.pi`, so a self-hosted session never pollutes host
 * state (the same isolation the codex provider gets from a scoped `CODEX_HOME`).
 */
export const PI_AGENT_DIR_ENV = 'PI_CODING_AGENT_DIR';

/** How pi reaches the runner's native-CLI tool-bridge (spawned by the pi extension over stdio). */
export interface PiBridgeCommand {
  /** The pi extension module path wired via `--extension` (spawns the bridge entry). */
  extension: string;
  /** The bridge entrypoint executable the extension spawns (e.g. `node`). */
  command: string;
  /** Arguments to the bridge entrypoint (e.g. `[<bridge-entry.js>, '--root', <root>]`). */
  args: string[];
}

/** Inputs to {@link buildPiLaunchConfig}. */
export interface PiLaunchInput {
  /** Binary path/name for the CLI. Defaults to {@link DEFAULT_PI_CLI} on the PATH. */
  cliPath?: string;
  /**
   * Extra prefix args inserted BEFORE the generated flags (e.g. a fake CLI script path under
   * `node` in tests). Empty in production.
   */
  prefixArgs?: string[];
  /** Extra suffix args appended AFTER the generated flags (e.g. a test probe flag). */
  suffixArgs?: string[];
  /** The effective model id, or `undefined` to let pi pick its default. */
  model?: string;
  /** The native-CLI tool-bridge wiring pi surfaces as its orca extension. */
  bridge: PiBridgeCommand;
  /** The snapshot's agent system prompt to append, when non-empty. */
  system?: string;
  /** The gateway LLM-proxy base URL → the pi env, when set. */
  baseURL?: string;
  /** The scoped session JWT → the pi env (credential-free), when set. */
  apiKey?: string;
  /**
   * A private `PI_CODING_AGENT_DIR` for pi, when set — keeps pi's config/state off the operator's
   * `~/.pi`. Absent → pi uses its ambient home.
   */
  agentDir?: string;
}

/**
 * Build the {@link NativeCliLaunchConfig} that boots `pi --mode rpc` for a session. See the
 * module header for the full argument grammar.
 */
export function buildPiLaunchConfig(input: PiLaunchInput): NativeCliLaunchConfig {
  const args: string[] = [...(input.prefixArgs ?? [])];

  // The headless JSON stdio protocol + no host-side session persistence.
  args.push('--mode', PI_RPC_MODE, '--no-session');

  // Select the managed provider the harness rendered into models.json; pin the model when set.
  args.push('--provider', PI_PROVIDER_ID);
  if (input.model !== undefined && input.model.length > 0) {
    args.push('--model', input.model);
  }

  // Disable pi's default in-process tools (read/bash/edit/write). They are ENABLED by default and
  // would run un-gated + outside the sandbox-scoped bridge; extension-registered tools (the orca
  // bridge) stay enabled, so the model sees ONLY the approval-gated, sandbox-scoped orca tools.
  args.push('--no-builtin-tools');

  // Wire the native-CLI tool-bridge as a pi extension so the model's orca tools resolve inside the
  // sandbox (pi has no MCP-config surface in RPC mode). `--no-extensions` first suppresses pi's
  // ambient host extension discovery; the explicit `--extension` (SINGULAR — the plural flag does
  // not exist and is silently ignored) then loads ONLY the orca bridge.
  args.push('--no-extensions', '--extension', input.bridge.extension);

  if (input.system !== undefined && input.system.trim().length > 0) {
    args.push('--append-system-prompt', input.system);
  }

  args.push(...(input.suffixArgs ?? []));

  const config: NativeCliLaunchConfig = {
    cmd: input.cliPath !== undefined && input.cliPath.length > 0 ? input.cliPath : DEFAULT_PI_CLI,
    args,
  };

  const env = buildEnv(input);
  if (Object.keys(env).length > 0) {
    config.env = env;
  }
  return config;
}

/**
 * Build the credential-free CLI env: the gateway LLM base URL + scoped JWT (only when present)
 * plus the private PI_CODING_AGENT_DIR (only when set). Everything is omitted when the boot
 * context supplies nothing, so pi then uses its own ambient auth + home.
 */
function buildEnv(input: PiLaunchInput): Record<string, string> {
  const env: Record<string, string> = {};
  if (input.apiKey !== undefined && input.apiKey.length > 0) {
    env[PI_LLM_API_KEY_ENV] = input.apiKey;
  }
  if (input.baseURL !== undefined && input.baseURL.length > 0) {
    env[PI_LLM_BASE_URL_ENV] = input.baseURL;
  }
  if (input.agentDir !== undefined && input.agentDir.length > 0) {
    env[PI_AGENT_DIR_ENV] = input.agentDir;
  }
  return env;
}
