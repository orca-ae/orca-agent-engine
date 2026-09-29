// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the `pi` native-CLI launch-arg builder.
//
// The `pi` provider boots the real `pi` binary HEADLESS in its JSON RPC mode (`pi --mode rpc`)
// and speaks pi's newline-delimited-JSON command/event protocol over stdio. This builder turns
// a session's boot inputs into the {@link NativeCliLaunchConfig} the native-CLI launcher runs
// — the argument grammar plus the credential-free env — mirroring the CANONICAL set pi's own
// headless launch uses. This suite pins that argv/env contract:
//   1. the `--mode rpc` + `--no-session` headless flags,
//   2. the managed provider selection (`--provider orca --model <id>`) that pairs with the
//      per-session `models.json` the harness writes into the managed agent dir,
//   3. the `--no-extensions --extension <bridge-extension>` wiring that surfaces the native-CLI
//      tool-bridge's orca tools (bash/read/write/edit/glob/grep + sys_terminal_*) to pi while
//      suppressing host extension discovery, plus `--no-builtin-tools` so pi's default in-process
//      tools do not run un-gated / shadow the bridged orca tools,
//   4. the credential-free gateway env (base URL + scoped JWT) carried as pi-native env vars +
//      the pinned `PI_CODING_AGENT_DIR` managed config dir,
//   5. a pinned model when set (omitted otherwise, letting pi pick its default),
//   6. test prefix/suffix args + a custom binary path threading through.
//
// No real pi binary is launched here — the builder is PURE (no I/O).

import { describe, it, expect } from 'vitest';
import {
  buildPiLaunchConfig,
  DEFAULT_PI_CLI,
  PI_PROVIDER_ID,
  PI_RPC_MODE,
} from '../../src/harness/pi/launch-args.js';

const bridge = {
  extension: '/pkg/orca-extension.mjs',
  command: '/usr/bin/node',
  args: ['/pkg/bridge-entry.js', '--root', '/work'],
};

describe('pi launch-args builder', () => {
  it('boots `pi --mode rpc` headless with sessions disabled', () => {
    const config = buildPiLaunchConfig({ bridge });
    expect(config.cmd).toBe(DEFAULT_PI_CLI);
    // The headless RPC mode + no-session flags: the harness drives pi over stdin/stdout JSON,
    // and pi must not persist a session file on the runner host.
    expect(config.args).toContain('--mode');
    const modeIdx = (config.args ?? []).indexOf('--mode');
    expect(config.args?.[modeIdx + 1]).toBe(PI_RPC_MODE);
    expect(PI_RPC_MODE).toBe('rpc');
    expect(config.args).toContain('--no-session');
  });

  it('selects the managed `orca` provider so pi authenticates via the written models.json', () => {
    const config = buildPiLaunchConfig({ bridge, model: 'claude-sonnet-4-5' });
    const args = config.args ?? [];
    // The provider id is stable so `--provider` selects the entry the harness renders into
    // the managed agent dir's models.json (the analog of codex routing through the gateway).
    expect(args).toContain('--provider');
    expect(args[args.indexOf('--provider') + 1]).toBe(PI_PROVIDER_ID);
    expect(PI_PROVIDER_ID).toBe('orca');
    expect(args).toContain('--model');
    expect(args[args.indexOf('--model') + 1]).toBe('claude-sonnet-4-5');
  });

  it('omits `--model` when no model is pinned (pi picks its default)', () => {
    const config = buildPiLaunchConfig({ bridge });
    expect(config.args).not.toContain('--model');
  });

  it('wires the native-CLI tool-bridge as a pi extension via the SINGULAR --extension flag', () => {
    const config = buildPiLaunchConfig({ bridge });
    const args = config.args ?? [];
    // The real pi flag is `--extension` (singular, `-e`); the plural does not exist and would be
    // silently dropped by pi's arg parser, so the orca bridge would never load. Pin the singular.
    expect(args).not.toContain('--extensions');
    expect(args).toContain('--extension');
    expect(args[args.indexOf('--extension') + 1]).toBe(bridge.extension);
    // `--no-extensions` precedes it so pi ignores the operator's host `settings.json` extensions
    // and loads ONLY the orca bridge (the documented `--no-extensions --extension <ext>` idiom).
    expect(args).toContain('--no-extensions');
    expect(args.indexOf('--no-extensions')).toBeLessThan(args.indexOf('--extension'));
  });

  it('disables pi built-in tools with --no-builtin-tools (bridged orca tools stay enabled)', () => {
    // Pi's read/bash/edit/write are ENABLED by default; without this flag they run un-gated by the
    // approval gate, outside the sandbox-scoped bridge, and shadow the bridged orca tools.
    const config = buildPiLaunchConfig({ bridge });
    expect(config.args).toContain('--no-builtin-tools');
  });

  it('appends the composed system prompt when non-empty, omits it otherwise', () => {
    const withSystem = buildPiLaunchConfig({ bridge, system: 'You are Orca.' });
    const args = withSystem.args ?? [];
    expect(args).toContain('--append-system-prompt');
    expect(args[args.indexOf('--append-system-prompt') + 1]).toBe('You are Orca.');

    const noSystem = buildPiLaunchConfig({ bridge, system: '   ' });
    expect(noSystem.args).not.toContain('--append-system-prompt');
  });

  it('carries the gateway LLM egress on the env credential-free + pins PI_CODING_AGENT_DIR', () => {
    const config = buildPiLaunchConfig({
      bridge,
      baseURL: 'https://gw/anthropic',
      apiKey: 'scoped-jwt',
      agentDir: '/work/.pi-agent-home',
    });
    // Pi reads its credentials from the managed agent dir; the base URL + scoped JWT ride the
    // env credential-free so the harness can render a models.json entry that points at them.
    expect(config.env?.['PI_CODING_AGENT_DIR']).toBe('/work/.pi-agent-home');
    expect(config.env?.['ORCA_PI_LLM_BASE_URL']).toBe('https://gw/anthropic');
    expect(config.env?.['ORCA_PI_LLM_API_KEY']).toBe('scoped-jwt');
  });

  it('omits the LLM env entirely when no egress is supplied', () => {
    const config = buildPiLaunchConfig({ bridge });
    expect(config.env?.['ORCA_PI_LLM_API_KEY']).toBeUndefined();
    expect(config.env?.['ORCA_PI_LLM_BASE_URL']).toBeUndefined();
  });

  it('threads prefix/suffix args + a custom binary path through', () => {
    const config = buildPiLaunchConfig({
      bridge,
      cliPath: '/opt/pi',
      prefixArgs: ['/fake/pi.mjs'],
      suffixArgs: ['--probe', 'x'],
    });
    expect(config.cmd).toBe('/opt/pi');
    expect(config.args?.[0]).toBe('/fake/pi.mjs');
    expect(config.args?.slice(-2)).toEqual(['--probe', 'x']);
    // `--mode rpc` is still present after the prefix.
    expect(config.args).toContain('--mode');
  });
});
