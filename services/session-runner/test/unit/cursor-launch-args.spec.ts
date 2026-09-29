// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the `cursor` native-CLI launch-arg builder.
//
// The `cursor` provider boots the real `cursor-agent` binary HEADLESS (`--print
// --output-format stream-json`) and speaks its newline-delimited-JSON stream over stdio.
// This builder turns a session's boot inputs into the {@link NativeCliLaunchConfig} the
// native-CLI launcher runs — the argument grammar plus the credential-free env — mirroring
// the flags a headless `cursor-agent` launch uses. This suite pins that argv/env contract:
//   1. the headless `--print` + `--output-format stream-json` (+ `--force`) flags,
//   2. the `orca` MCP-server `--mcp-config` override pointing at the native-CLI tool-bridge,
//      plus a matching allow/deny permission-mode flag,
//   3. the credential-free auth env (the Cursor API key) + base URL,
//   4. a pinned model flag when set (omitted otherwise),
//   5. test prefix/suffix args + a custom binary path threading through.
//
// No real cursor-agent binary is launched here — the builder is PURE (no I/O).

import { describe, it, expect } from 'vitest';
import {
  buildCursorLaunchConfig,
  CURSOR_MCP_SERVER_NAME,
  DEFAULT_CURSOR_CLI,
} from '../../src/harness/cursor/launch-args.js';

/** The `--mcp-config` value (the JSON arg after the `--mcp-config` flag), parsed. */
function mcpConfigOf(argv: string[]): { mcpServers?: Record<string, unknown> } {
  const i = argv.indexOf('--mcp-config');
  if (i < 0 || i + 1 >= argv.length) {
    return {};
  }
  return JSON.parse(argv[i + 1] as string) as { mcpServers?: Record<string, unknown> };
}

const bridge = { command: '/usr/bin/node', args: ['/pkg/bridge-entry.js', '--root', '/work'] };

describe('cursor launch-args builder', () => {
  it('boots `cursor-agent` headless: --print + stream-json output', () => {
    const config = buildCursorLaunchConfig({ bridge, gateEnabled: true });
    expect(config.cmd).toBe(DEFAULT_CURSOR_CLI);
    // Headless print mode driving one turn per stdin line, streamed as newline-delim JSON.
    expect(config.args).toContain('--print');
    const oi = config.args?.indexOf('--output-format') ?? -1;
    expect(oi).toBeGreaterThanOrEqual(0);
    expect(config.args?.[oi + 1]).toBe('stream-json');
  });

  it('wires the native-CLI tool-bridge as the `orca` MCP server via --mcp-config', () => {
    const config = buildCursorLaunchConfig({ bridge, gateEnabled: true });
    const parsed = mcpConfigOf(config.args ?? []);
    const server = parsed.mcpServers?.[CURSOR_MCP_SERVER_NAME] as
      | { command?: string; args?: string[] }
      | undefined;
    expect(CURSOR_MCP_SERVER_NAME).toBe('orca');
    expect(server?.command).toBe(bridge.command);
    expect(server?.args).toEqual(bridge.args);
  });

  it('routes tool permissions to the gate when a gate is wired', () => {
    // With a gate, cursor must prompt over the stream (the harness answers) — NOT auto-run
    // every tool. The builder emits the interactive/gated permission mode, and does NOT emit
    // the force/bypass flag.
    const config = buildCursorLaunchConfig({ bridge, gateEnabled: true });
    expect(config.args).not.toContain('--force');
  });

  it('bypasses permissions (--force) when NO gate is wired', () => {
    // With no gate the OS sandbox + composed tool policies are the guardrail, so the headless
    // CLI must not block on an interactive prompt it can never satisfy.
    const config = buildCursorLaunchConfig({ bridge, gateEnabled: false });
    expect(config.args).toContain('--force');
  });

  it('pins a model flag when a model is set, and omits it otherwise', () => {
    const withModel = buildCursorLaunchConfig({ bridge, gateEnabled: false, model: 'gpt-5' });
    const mi = withModel.args?.indexOf('--model') ?? -1;
    expect(mi).toBeGreaterThanOrEqual(0);
    expect(withModel.args?.[mi + 1]).toBe('gpt-5');
    const noModel = buildCursorLaunchConfig({ bridge, gateEnabled: false });
    expect(noModel.args).not.toContain('--model');
  });

  it('carries the Cursor auth key + base URL on the env credential-free', () => {
    const config = buildCursorLaunchConfig({
      bridge,
      gateEnabled: true,
      baseURL: 'https://gw/cursor',
      apiKey: 'scoped-jwt',
    });
    // The Cursor CLI reads its API key from CURSOR_API_KEY (the gateway swaps the scoped JWT
    // for the real upstream secret); the base URL rides the CLI's own override var.
    expect(config.env?.['CURSOR_API_KEY']).toBe('scoped-jwt');
    expect(config.env?.['CURSOR_API_BASE_URL']).toBe('https://gw/cursor');
  });

  it('omits the auth env entirely when no egress is supplied', () => {
    const config = buildCursorLaunchConfig({ bridge, gateEnabled: false });
    expect(config.env?.['CURSOR_API_KEY']).toBeUndefined();
    expect(config.env?.['CURSOR_API_BASE_URL']).toBeUndefined();
  });

  it('appends the composed system prompt when non-empty, and omits it otherwise', () => {
    const withSystem = buildCursorLaunchConfig({
      bridge,
      gateEnabled: false,
      system: 'You are Orca.',
    });
    const si = withSystem.args?.indexOf('--system-prompt') ?? -1;
    expect(si).toBeGreaterThanOrEqual(0);
    expect(withSystem.args?.[si + 1]).toBe('You are Orca.');
    const noSystem = buildCursorLaunchConfig({ bridge, gateEnabled: false });
    expect(noSystem.args).not.toContain('--system-prompt');
  });

  it('threads prefix/suffix args + a custom binary path through', () => {
    const config = buildCursorLaunchConfig({
      bridge,
      gateEnabled: false,
      cliPath: '/opt/cursor-agent',
      prefixArgs: ['/fake/cursor.mjs'],
      suffixArgs: ['--probe', 'x'],
    });
    expect(config.cmd).toBe('/opt/cursor-agent');
    // Prefix args precede the generated flags; suffix args follow.
    expect(config.args?.[0]).toBe('/fake/cursor.mjs');
    expect(config.args?.slice(-2)).toEqual(['--probe', 'x']);
    // `--print` is still present after the prefix.
    expect(config.args).toContain('--print');
  });
});
