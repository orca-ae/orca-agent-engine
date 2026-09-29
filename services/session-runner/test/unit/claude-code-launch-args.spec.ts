// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the claude-code native-CLI launch-arg builder.
//
// The `claude-code` provider boots the real `claude` binary HEADLESS and speaks its
// newline-delimited-JSON ("stream-json") protocol over stdio. This spec pins the
// argument grammar the builder emits — the SAME canonical flags the Claude Agent
// SDK's own process transport passes to the binary, adapted to the runner's needs:
//
//   - stream-json in BOTH directions + partial messages (so the harness drives turns
//     over stdin and reads assistant/thinking/tool events off stdout live),
//   - the model (when the snapshot pinned one, else the runner default),
//   - the native-CLI tool-bridge wired as an MCP server via `--mcp-config` so the
//     model's orca tools (bash/read/write/edit/glob/grep + sys_terminal_*) resolve
//     INSIDE the per-session sandbox,
//   - the permission wiring: `--permission-prompt-tool stdio` when the runner wired a
//     confirmation gate (so the CLI routes each tool permission over the stream-json
//     control channel to the uniform transcript approval), else
//     `--dangerously-skip-permissions` (the OS sandbox + policies are the guardrail),
//   - `--plugin-dir` for a skills snapshot dir, and `--append-system-prompt` for the
//     composed agent/skills system prompt.
//
// The builder is pure (no I/O), so every assertion reads the returned launch config
// directly.

import { describe, it, expect } from 'vitest';
import {
  buildClaudeCodeLaunchConfig,
  CLAUDE_CODE_MCP_SERVER_NAME,
  type ClaudeCodeLaunchInput,
} from '../../src/harness/claude-code/launch-args.js';

/** The bridge command a launched CLI runs to reach the runner's stdio tool-bridge. */
const BRIDGE = { command: '/usr/bin/node', args: ['/opt/orca/bridge.js', '--session', 'ses_x'] };

/** A minimal, valid launch input (a gate wired, a model, the bridge). */
function baseInput(overrides: Partial<ClaudeCodeLaunchInput> = {}): ClaudeCodeLaunchInput {
  return {
    model: 'claude-sonnet-4-5',
    bridge: BRIDGE,
    gateEnabled: true,
    ...overrides,
  };
}

/** The value that follows `flag` in an argv, or undefined when the flag is absent. */
function valueAfter(args: string[], flag: string): string | undefined {
  const i = args.indexOf(flag);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : undefined;
}

/** Count how many times `flag` appears in an argv. */
function countFlag(args: string[], flag: string): number {
  return args.filter((a) => a === flag).length;
}

describe('buildClaudeCodeLaunchConfig — headless claude stream-json argv', () => {
  it('runs the `claude` binary and requests stream-json in both directions + partial messages', () => {
    const cfg = buildClaudeCodeLaunchConfig(baseInput());
    expect(cfg.cmd).toBe('claude');
    const args = cfg.args ?? [];
    // Output + input are BOTH stream-json (the harness drives turns over stdin and
    // parses stream-json off stdout). `--verbose` is required for stream-json output.
    expect(valueAfter(args, '--output-format')).toBe('stream-json');
    expect(valueAfter(args, '--input-format')).toBe('stream-json');
    expect(args).toContain('--verbose');
    // Partial messages so extended-thinking deltas stream live.
    expect(args).toContain('--include-partial-messages');
  });

  it('honors a custom binary path (self-hosted operators may pin an absolute path)', () => {
    const cfg = buildClaudeCodeLaunchConfig(baseInput({ cliPath: '/opt/claude/bin/claude' }));
    expect(cfg.cmd).toBe('/opt/claude/bin/claude');
  });

  it('passes the model when set', () => {
    const cfg = buildClaudeCodeLaunchConfig(baseInput({ model: 'claude-opus-4-1' }));
    expect(valueAfter(cfg.args ?? [], '--model')).toBe('claude-opus-4-1');
  });

  it('omits --model when no model is set (the binary picks its default)', () => {
    // Build without a `model` key at all (under exactOptionalPropertyTypes an explicit
    // `undefined` is not assignable to the optional `model?: string`).
    const cfg = buildClaudeCodeLaunchConfig({ bridge: BRIDGE, gateEnabled: true });
    expect(cfg.args ?? []).not.toContain('--model');
  });

  it('wires the native-CLI bridge as an MCP server via --mcp-config under the reserved orca name', () => {
    const cfg = buildClaudeCodeLaunchConfig(baseInput());
    const raw = valueAfter(cfg.args ?? [], '--mcp-config');
    expect(raw).toBeDefined();
    const parsed = JSON.parse(raw as string) as {
      mcpServers: Record<string, { command: string; args: string[] }>;
    };
    // The bridge is registered under the reserved `orca` server name (the same
    // identity the in-process claude surface + the STDIO bridge advertise), so the
    // model sees the built-ins as `mcp__orca__*`.
    expect(CLAUDE_CODE_MCP_SERVER_NAME).toBe('orca');
    const server = parsed.mcpServers[CLAUDE_CODE_MCP_SERVER_NAME];
    expect(server).toBeDefined();
    // It launches the runner-provided bridge entrypoint verbatim (the CLI spawns it
    // as a child and speaks MCP-over-stdio to it).
    expect(server?.command).toBe(BRIDGE.command);
    expect(server?.args).toEqual(BRIDGE.args);
    // With any MCP server wired, the config is strict so the CLI does not ALSO read an
    // ambient .mcp.json off the runner host.
    expect(cfg.args ?? []).toContain('--strict-mcp-config');
  });

  it('routes tool permissions to the stdio control channel when a gate is wired (NOT skip-permissions)', () => {
    const cfg = buildClaudeCodeLaunchConfig(baseInput({ gateEnabled: true }));
    const args = cfg.args ?? [];
    // `--permission-prompt-tool stdio` makes the CLI emit a `can_use_tool`
    // control_request over stream-json for each gated tool call, which the harness
    // routes to the uniform transcript approval and answers over stdin.
    expect(valueAfter(args, '--permission-prompt-tool')).toBe('stdio');
    // The two are mutually exclusive — a gate means we do NOT bypass permissions.
    expect(args).not.toContain('--dangerously-skip-permissions');
  });

  it('skips permissions when NO gate is wired (the OS sandbox + policies are the guardrail)', () => {
    const cfg = buildClaudeCodeLaunchConfig(baseInput({ gateEnabled: false }));
    const args = cfg.args ?? [];
    expect(args).toContain('--dangerously-skip-permissions');
    expect(args).not.toContain('--permission-prompt-tool');
  });

  it('wires --plugin-dir for a skills snapshot dir, and omits it otherwise', () => {
    const withSkills = buildClaudeCodeLaunchConfig(baseInput({ pluginDir: '/snap/skills' }));
    expect(valueAfter(withSkills.args ?? [], '--plugin-dir')).toBe('/snap/skills');

    const without = buildClaudeCodeLaunchConfig(baseInput());
    expect(without.args ?? []).not.toContain('--plugin-dir');
  });

  it('appends the composed system prompt via --append-system-prompt, and omits it when empty', () => {
    const withSystem = buildClaudeCodeLaunchConfig(baseInput({ system: 'You are Orca.' }));
    expect(valueAfter(withSystem.args ?? [], '--append-system-prompt')).toBe('You are Orca.');

    const emptySystem = buildClaudeCodeLaunchConfig(baseInput({ system: '   ' }));
    expect(emptySystem.args ?? []).not.toContain('--append-system-prompt');

    const noSystem = buildClaudeCodeLaunchConfig(baseInput());
    expect(noSystem.args ?? []).not.toContain('--append-system-prompt');
  });

  it('forwards the gateway LLM egress as ANTHROPIC_* env (credential-free), and cwd', () => {
    const cfg = buildClaudeCodeLaunchConfig(
      baseInput({
        apiKey: 'scoped-session-jwt',
        baseURL: 'https://gw.example/anthropic',
        cwd: '/session/root',
      }),
    );
    expect(cfg.env?.['ANTHROPIC_API_KEY']).toBe('scoped-session-jwt');
    expect(cfg.env?.['ANTHROPIC_BASE_URL']).toBe('https://gw.example/anthropic');
    expect(cfg.cwd).toBe('/session/root');
  });

  it('omits ANTHROPIC_* env entirely when no egress is supplied (ambient CLI auth)', () => {
    const cfg = buildClaudeCodeLaunchConfig(baseInput());
    expect(cfg.env?.['ANTHROPIC_API_KEY']).toBeUndefined();
    expect(cfg.env?.['ANTHROPIC_BASE_URL']).toBeUndefined();
  });

  it('marks itself as an SDK-independent headless entrypoint (never nests inside another claude)', () => {
    // A headless claude launched inside another claude session would inherit
    // CLAUDECODE=1 and mis-behave; the builder pins the entrypoint env so the child
    // is a clean top-level session.
    const cfg = buildClaudeCodeLaunchConfig(baseInput());
    expect(cfg.env?.['CLAUDE_CODE_ENTRYPOINT']).toBe('orca-session-runner');
  });

  it('never emits a bare/duplicated stream-json toggle (argv is well-formed)', () => {
    const args = buildClaudeCodeLaunchConfig(baseInput()).args ?? [];
    expect(countFlag(args, '--output-format')).toBe(1);
    expect(countFlag(args, '--input-format')).toBe(1);
    // No dangling flag with a missing value at the tail.
    for (const flag of ['--model', '--mcp-config', '--append-system-prompt', '--plugin-dir']) {
      const i = args.indexOf(flag);
      if (i >= 0) {
        expect(i + 1).toBeLessThan(args.length);
      }
    }
  });
});
