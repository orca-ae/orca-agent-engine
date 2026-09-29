// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the `codex` native-CLI launch-arg builder.
//
// The `codex` provider boots the real `codex` binary as an APP-SERVER (`codex app-server`)
// and speaks its JSON-RPC-over-stdio protocol. This builder turns a session's boot inputs
// into the {@link NativeCliLaunchConfig} the native-CLI launcher runs — the argument
// grammar plus the credential-free env — mirroring the CANONICAL set codex's own app-server
// launch uses. This suite pins that argv/env contract:
//   1. the `app-server` subcommand + the `on-request` approval policy `-c` override,
//   2. the `orca` MCP-server `-c` overrides pointing at the native-CLI tool-bridge command,
//   3. the credential-free gateway env (base URL + scoped JWT) + the pinned CODEX_HOME,
//   4. a pinned model `-c` override when set (omitted otherwise),
//   5. test prefix/suffix args + a custom binary path threading through.
//
// No real codex binary is launched here — the builder is PURE (no I/O).

import { describe, it, expect } from 'vitest';
import {
  buildCodexLaunchConfig,
  CODEX_MCP_SERVER_NAME,
  DEFAULT_CODEX_CLI,
  CODEX_APPROVAL_POLICY_ON_REQUEST,
} from '../../src/harness/codex/launch-args.js';

/** Find the `-c` override value at index i (the arg AFTER each `-c`). */
function configOverrides(argv: string[]): string[] {
  const out: string[] = [];
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '-c' && i + 1 < argv.length) {
      out.push(argv[i + 1] as string);
    }
  }
  return out;
}

const bridge = { command: '/usr/bin/node', args: ['/pkg/bridge-entry.js', '--root', '/work'] };

describe('codex launch-args builder', () => {
  it('boots `codex app-server` with the on-request approval policy override', () => {
    const config = buildCodexLaunchConfig({ bridge, gateEnabled: true });
    expect(config.cmd).toBe(DEFAULT_CODEX_CLI);
    expect(config.args?.[0]).toBe('app-server');
    const overrides = configOverrides(config.args ?? []);
    // approval_policy=on-request routes codex's own exec/apply-patch approvals to our gate.
    expect(overrides).toContain(`approval_policy="${CODEX_APPROVAL_POLICY_ON_REQUEST}"`);
    expect(CODEX_APPROVAL_POLICY_ON_REQUEST).toBe('on-request');
  });

  it('wires the native-CLI tool-bridge as the `orca` MCP server via -c overrides', () => {
    const config = buildCodexLaunchConfig({ bridge, gateEnabled: true });
    const overrides = configOverrides(config.args ?? []);
    // The bridge command + args become the `mcp_servers.orca` config (dotted TOML fragment).
    const cmdOverride = overrides.find((o) =>
      o.startsWith(`mcp_servers.${CODEX_MCP_SERVER_NAME}.command=`),
    );
    const argsOverride = overrides.find((o) =>
      o.startsWith(`mcp_servers.${CODEX_MCP_SERVER_NAME}.args=`),
    );
    expect(cmdOverride).toBeDefined();
    expect(argsOverride).toBeDefined();
    expect(CODEX_MCP_SERVER_NAME).toBe('orca');
    // The command is emitted as a TOML/JSON string; the args as a JSON array — both parseable.
    const cmdValue = JSON.parse((cmdOverride as string).split('=').slice(1).join('=')) as string;
    expect(cmdValue).toBe(bridge.command);
    const argsValue = JSON.parse(
      (argsOverride as string).split('=').slice(1).join('='),
    ) as string[];
    expect(argsValue).toEqual(bridge.args);
  });

  it('pins a model `-c` override when a model is set, and omits it otherwise', () => {
    const withModel = buildCodexLaunchConfig({ bridge, gateEnabled: false, model: 'gpt-x' });
    expect(configOverrides(withModel.args ?? [])).toContain('model="gpt-x"');
    const noModel = buildCodexLaunchConfig({ bridge, gateEnabled: false });
    expect(configOverrides(noModel.args ?? []).some((o) => o.startsWith('model='))).toBe(false);
  });

  it('carries the gateway LLM egress on the env credential-free + pins CODEX_HOME', () => {
    const config = buildCodexLaunchConfig({
      bridge,
      gateEnabled: true,
      baseURL: 'https://gw/openai',
      apiKey: 'scoped-jwt',
      codexHome: '/work/.codex-home',
    });
    // OpenAI-family env the codex CLI reads (the gateway swaps the JWT for the real secret).
    expect(config.env?.['OPENAI_BASE_URL']).toBe('https://gw/openai');
    expect(config.env?.['OPENAI_API_KEY']).toBe('scoped-jwt');
    // A private CODEX_HOME keeps the app-server off the operator's ~/.codex.
    expect(config.env?.['CODEX_HOME']).toBe('/work/.codex-home');
  });

  it('omits the LLM env entirely when no egress is supplied', () => {
    const config = buildCodexLaunchConfig({ bridge, gateEnabled: false });
    expect(config.env?.['OPENAI_API_KEY']).toBeUndefined();
    expect(config.env?.['OPENAI_BASE_URL']).toBeUndefined();
  });

  it('threads prefix/suffix args + a custom binary path through', () => {
    const config = buildCodexLaunchConfig({
      bridge,
      gateEnabled: false,
      cliPath: '/opt/codex',
      prefixArgs: ['/fake/codex.mjs'],
      suffixArgs: ['--probe', 'x'],
    });
    expect(config.cmd).toBe('/opt/codex');
    // Prefix args precede the generated flags; suffix args follow.
    expect(config.args?.[0]).toBe('/fake/codex.mjs');
    expect(config.args?.slice(-2)).toEqual(['--probe', 'x']);
    // `app-server` is still present after the prefix.
    expect(config.args).toContain('app-server');
  });
});
