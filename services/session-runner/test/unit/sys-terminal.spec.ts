// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// sys_terminal_* toolset — the Orca-superset interactive-terminal tools.
//
// Where the `orca` built-ins (bash/read/write/…) are run-to-completion, the
// sys_terminal_* tools drive a LONG-LIVED interactive program in a tmux pane:
// launch it, type keystrokes (literal text AND key chords like Ctrl-C), read the
// RENDERED pane + scrollback, list the live panes, and close them. They are
// backed by `TmuxSandboxHandle` panes (each pane is a real terminal on the
// handle's private tmux socket), so this suite drives a REAL tmux pane end to
// end against a tiny fake interactive program — no cloud sandbox, no real REPL.
//
// The suite self-skips when the `tmux` binary is not on PATH so the unit run
// stays green on a box without tmux (CI installs it).

import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { describe, it, expect } from 'vitest';
import { TmuxSandboxRuntime } from '../../src/sandbox/tmux-sandbox.js';
import type { SandboxHandle } from '../../src/sandbox/seam.js';
import {
  buildSysTerminalTools,
  SYS_TERMINAL_TOOL_NAMES,
  asTerminalHost,
  type SysTerminalTool,
} from '../../src/tools/sys-terminal.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const FAKE_CLI = join(HERE, 'support', 'fake-interactive-cli.mjs');

/** True when a usable `tmux` binary is on PATH (the suite self-skips otherwise). */
function tmuxAvailable(): boolean {
  try {
    execFileSync('tmux', ['-V'], { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

/** Await `ms` milliseconds (used to let keystrokes render in the pane). */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Poll a tool `read` until `predicate` holds over the captured pane text, then
 * return that text. Asserts *rendered* pane output without racing a keystroke's
 * round-trip. Fails the test with the last capture on timeout.
 */
async function readUntil(
  tools: SysTerminalTool[],
  id: string,
  predicate: (text: string) => boolean,
  timeoutMs = 8000,
): Promise<string> {
  const read = toolFor(tools, 'sys_terminal_read');
  const deadline = Date.now() + timeoutMs;
  let last = '';
  for (;;) {
    const r = await read.handler({ terminal_id: id }, {});
    last = textOf(r);
    if (predicate(last)) {
      return last;
    }
    if (Date.now() >= deadline) {
      throw new Error(`readUntil timed out; last capture:\n${last}`);
    }
    await delay(100);
  }
}

/** The single text block a sys_terminal tool returns (they are text-only). */
function textOf(r: unknown): string {
  const obj = r as { content?: Array<{ text?: unknown }> };
  const first = obj.content?.[0];
  return typeof first?.text === 'string' ? first.text : '';
}

function toolFor(tools: SysTerminalTool[], name: string): SysTerminalTool {
  const bare = name.replace(/^mcp__orca__/, '');
  const t = tools.find((x) => x.name === bare);
  if (!t) {
    throw new Error(`tool not found: ${name}`);
  }
  return t;
}

const describeTmux = tmuxAvailable() ? describe : describe.skip;

describeTmux('sys_terminal_* tools over TmuxSandboxHandle panes', () => {
  async function withHostTools(
    fn: (tools: SysTerminalTool[], sandbox: SandboxHandle) => Promise<void>,
  ): Promise<void> {
    const runtime = new TmuxSandboxRuntime();
    const sandbox = await runtime.acquire({});
    try {
      const host = asTerminalHost(sandbox);
      expect(host).not.toBeNull();
      const tools = buildSysTerminalTools(host!);
      await fn(tools, sandbox);
    } finally {
      await sandbox.destroy();
    }
  }

  it('exposes exactly the five sys_terminal tool names', async () => {
    await withHostTools(async (tools) => {
      const bare = tools.map((t) => t.name).sort();
      expect(bare).toEqual(
        [
          'sys_terminal_close',
          'sys_terminal_launch',
          'sys_terminal_list',
          'sys_terminal_read',
          'sys_terminal_send',
        ].sort(),
      );
      // The MCP-qualified names carry the `mcp__orca__` prefix the bridge exposes.
      expect([...SYS_TERMINAL_TOOL_NAMES].sort()).toEqual(
        bare.map((n) => `mcp__orca__${n}`).sort(),
      );
    });
  });

  it('launches an interactive program and reads its rendered banner', async () => {
    await withHostTools(async (tools) => {
      const launch = toolFor(tools, 'sys_terminal_launch');
      const res = await launch.handler({ command: `${process.execPath} ${FAKE_CLI}` }, {});
      const id = terminalIdOf(res);
      expect(id).toBeTruthy();
      // The program prints READY + a prompt on boot; the pane render carries it.
      const text = await readUntil(tools, id, (t) => t.includes('READY'));
      expect(text).toContain('READY');
    });
  }, 20_000);

  it('sends literal text + Enter and reads the program echo (a real submit)', async () => {
    await withHostTools(async (tools) => {
      const launch = toolFor(tools, 'sys_terminal_launch');
      const send = toolFor(tools, 'sys_terminal_send');
      const { value: id } = await launchReady(tools, launch);

      // Type a line and press Enter: the fake program echoes `you said: <line>`.
      await send.handler({ terminal_id: id, text: 'hello-orca', enter: true }, {});
      const text = await readUntil(tools, id, (t) => t.includes('you said: hello-orca'));
      expect(text).toContain('you said: hello-orca');
    });
  }, 20_000);

  it('sends a key chord (Ctrl-C) that the foreground program reacts to', async () => {
    await withHostTools(async (tools) => {
      const launch = toolFor(tools, 'sys_terminal_launch');
      const send = toolFor(tools, 'sys_terminal_send');
      const { value: id } = await launchReady(tools, launch);

      // Send the Ctrl-C chord as a KEY (not literal text). The program's SIGINT
      // handler prints `interrupted!` — proof the chord reached the foreground.
      await send.handler({ terminal_id: id, keys: ['C-c'] }, {});
      const text = await readUntil(tools, id, (t) => t.includes('interrupted!'));
      expect(text).toContain('interrupted!');
    });
  }, 20_000);

  it('read returns scrollback beyond the visible pane when requested', async () => {
    await withHostTools(async (tools) => {
      const launch = toolFor(tools, 'sys_terminal_launch');
      const send = toolFor(tools, 'sys_terminal_send');
      const read = toolFor(tools, 'sys_terminal_read');
      const { value: id } = await launchReady(tools, launch);

      // Emit many lines so the earliest ones scroll off the small visible pane.
      for (let i = 0; i < 60; i++) {
        await send.handler({ terminal_id: id, text: `line-${i}`, enter: true }, {});
      }
      await readUntil(tools, id, (t) => t.includes('you said: line-59'));

      // A large scrollback window recovers an early line that the default
      // visible-only capture would have dropped.
      const deep = await read.handler({ terminal_id: id, scrollback_lines: 500 }, {});
      expect(textOf(deep)).toContain('you said: line-0');
    });
  }, 30_000);

  it('lists the live terminals with their launch command', async () => {
    await withHostTools(async (tools) => {
      const launch = toolFor(tools, 'sys_terminal_launch');
      const list = toolFor(tools, 'sys_terminal_list');
      const a = await launchReady(tools, launch);
      const b = await launchReady(tools, launch);

      const res = await list.handler({}, {});
      const text = textOf(res);
      expect(text).toContain(a.value);
      expect(text).toContain(b.value);
      // The list is machine-readable JSON so a caller can parse ids + liveness.
      const parsed = JSON.parse(text) as Array<{ terminal_id: string; alive: boolean }>;
      const ids = parsed.map((e) => e.terminal_id);
      expect(ids).toContain(a.value);
      expect(ids).toContain(b.value);
      expect(parsed.every((e) => e.alive)).toBe(true);
    });
  }, 20_000);

  it('closes a terminal so it drops out of the list and further reads error', async () => {
    await withHostTools(async (tools) => {
      const launch = toolFor(tools, 'sys_terminal_launch');
      const list = toolFor(tools, 'sys_terminal_list');
      const close = toolFor(tools, 'sys_terminal_close');
      const read = toolFor(tools, 'sys_terminal_read');
      const { value: id } = await launchReady(tools, launch);

      const closed = await close.handler({ terminal_id: id }, {});
      expect(isError(closed)).toBe(false);

      // The closed id no longer appears in the live list.
      const listed = JSON.parse(textOf(await list.handler({}, {}))) as Array<{
        terminal_id: string;
      }>;
      expect(listed.map((e) => e.terminal_id)).not.toContain(id);

      // A read against a gone terminal is a surfaced error, not a throw.
      const r = await read.handler({ terminal_id: id }, {});
      expect(isError(r)).toBe(true);
    });
  }, 20_000);

  it('close is idempotent — a second close of the same id is not an error', async () => {
    await withHostTools(async (tools) => {
      const launch = toolFor(tools, 'sys_terminal_launch');
      const close = toolFor(tools, 'sys_terminal_close');
      const { value: id } = await launchReady(tools, launch);
      expect(isError(await close.handler({ terminal_id: id }, {}))).toBe(false);
      expect(isError(await close.handler({ terminal_id: id }, {}))).toBe(false);
    });
  }, 20_000);

  it('send/read against an unknown terminal id surface an error result', async () => {
    await withHostTools(async (tools) => {
      const send = toolFor(tools, 'sys_terminal_send');
      const read = toolFor(tools, 'sys_terminal_read');
      expect(isError(await send.handler({ terminal_id: 'nope', text: 'x' }, {}))).toBe(true);
      expect(isError(await read.handler({ terminal_id: 'nope' }, {}))).toBe(true);
    });
  }, 20_000);

  // --- helpers bound to the tool surface ---

  /** Launch the fake CLI and wait for its READY banner; return the terminal id. */
  async function launchReady(
    tools: SysTerminalTool[],
    launch: SysTerminalTool,
  ): Promise<{ value: string }> {
    const res = await launch.handler({ command: `${process.execPath} ${FAKE_CLI}` }, {});
    const id = terminalIdOf(res);
    await readUntil(tools, id, (t) => t.includes('READY'));
    return { value: id };
  }
});

/** True when a tool result is flagged `isError`. */
function isError(r: unknown): boolean {
  return (r as { isError?: unknown }).isError === true;
}

/** Extract the launched terminal id out of a sys_terminal_launch result. */
function terminalIdOf(r: unknown): string {
  const text = textOf(r);
  const parsed = JSON.parse(text) as { terminal_id?: unknown };
  if (typeof parsed.terminal_id !== 'string') {
    throw new Error(`launch result had no terminal_id: ${text}`);
  }
  return parsed.terminal_id;
}

describe('sys_terminal_* — host capability detection', () => {
  it('asTerminalHost returns null for a handle without terminal support', () => {
    // A cloud-only SandboxHandle omits the terminal capability (like `spawn`);
    // the tools must feature-detect it rather than assume every handle has panes.
    const bare = { id: 'sbx_cloud' } as unknown as SandboxHandle;
    expect(asTerminalHost(bare)).toBeNull();
  });
});
