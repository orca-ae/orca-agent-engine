// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// sys_terminal_* — the Orca-superset interactive-terminal toolset.
//
// The `orca` built-ins (bash/read/write/edit/glob/grep/list/delete) are all
// RUN-TO-COMPLETION: dispatch a command, get its buffered result. That is the
// wrong shape for a program you must interact with over time — a REPL, a pager,
// an installer prompt, a long-lived `top`. The sys_terminal_* tools cover that
// gap: they drive a LONG-LIVED interactive program living in a real terminal
// (a tmux pane), so a caller can
//
//   - {@link SYS_TERMINAL_LAUNCH}  launch a program in a fresh pane,
//   - {@link SYS_TERMINAL_SEND}    type into it — literal text AND key chords
//                                  (`C-c`, `Enter`, `Up`, `Escape`, …),
//   - {@link SYS_TERMINAL_READ}    read the RENDERED pane + optional scrollback,
//   - {@link SYS_TERMINAL_LIST}    enumerate the live panes,
//   - {@link SYS_TERMINAL_CLOSE}   tear a pane down.
//
// They are backed by {@link TerminalHost} — the pane-management capability a
// {@link SandboxHandle} may expose (the tmux-backed handle does; a cloud-only
// handle need not, so callers feature-detect with {@link asTerminalHost}, the
// same duck-typed pattern the sandbox boundary uses for `spawn?`/`endpoint?`).
// Every session keeps its terminals inside its own sandbox, so the interactive
// programs run under the SAME isolation as the run-to-completion tools.
//
// The tool definitions are produced with the SDK's `tool()` helper so they carry
// the exact `{ name, description, inputSchema, handler }` shape the runner's
// `orca` MCP tools use. That single shape lets them be exposed BOTH through the
// native-CLI STDIO bridge (a native CLI's tool calls) AND on the in-process
// claude provider's tool surface, with no per-surface adapter.

import { z } from 'zod';
import { tool, type SdkMcpToolDefinition } from '@anthropic-ai/claude-agent-sdk';

/** Cap a rendered-pane capture so a runaway scrollback can't blow the context. */
const MAX_CAPTURE = 100_000;
function truncate(s: string, max = MAX_CAPTURE): string {
  return s.length > max ? s.slice(0, max) + `\n…[truncated, total ${s.length} chars]` : s;
}

/** Logical (unqualified) names of the five sys_terminal tools. */
export const SYS_TERMINAL_LAUNCH = 'sys_terminal_launch';
export const SYS_TERMINAL_SEND = 'sys_terminal_send';
export const SYS_TERMINAL_READ = 'sys_terminal_read';
export const SYS_TERMINAL_LIST = 'sys_terminal_list';
export const SYS_TERMINAL_CLOSE = 'sys_terminal_close';

/** The logical names, in a stable order (single source of truth for tests + docs). */
export const SYS_TERMINAL_TOOL_LOGICAL_NAMES: readonly string[] = [
  SYS_TERMINAL_LAUNCH,
  SYS_TERMINAL_SEND,
  SYS_TERMINAL_READ,
  SYS_TERMINAL_LIST,
  SYS_TERMINAL_CLOSE,
] as const;

/**
 * The MCP-qualified names as exposed by the native-CLI bridge. The bridge serves
 * every built-in (orca + sys_terminal) under one server whose reserved name is
 * `orca`, so a caller comparing against a tool_use name uses these `mcp__orca__*`
 * forms — the same convention the in-process `orca` server follows.
 */
export const SYS_TERMINAL_TOOL_NAMES: readonly string[] = SYS_TERMINAL_TOOL_LOGICAL_NAMES.map(
  (name) => `mcp__orca__${name}`,
);

/** One launched interactive terminal, as reported by {@link TerminalHost.listTerminals}. */
export interface TerminalDescriptor {
  /** Opaque id the tools address this terminal by (also its tmux session name). */
  terminalId: string;
  /** The command the terminal was launched with. */
  command: string;
  /** Whether the underlying pane/process is still alive. */
  alive: boolean;
}

/** Options accepted by {@link TerminalHost.launchTerminal}. */
export interface LaunchTerminalOptions {
  /** The program to run in the pane (a shell command line). */
  command: string;
  /** Sandbox-visible working directory for the program. */
  cwd?: string;
  /** Extra environment for the program (merged over the sandbox environment). */
  env?: Record<string, string>;
  /** Pane width in columns (default sized for a generous capture). */
  cols?: number;
  /** Pane height in rows. */
  rows?: number;
}

/** Options accepted by {@link TerminalHost.sendTerminalKeys}. */
export interface SendTerminalOptions {
  /** Literal text to type into the pane (characters, not key names). */
  text?: string;
  /**
   * Key chords / named keys to send, e.g. `['C-c']`, `['Enter']`, `['Up','Up']`,
   * `['Escape']`. Each entry is one tmux key. Sent AFTER {@link text}, so a caller
   * can type then chord in a single call.
   */
  keys?: string[];
  /** Convenience: press Enter after {@link text} (a real line submit). */
  enter?: boolean;
}

/** Options accepted by {@link TerminalHost.readTerminal}. */
export interface ReadTerminalOptions {
  /**
   * How many lines of scrollback (history above the visible pane) to include. `0`
   * / omitted captures only the visible pane; a positive value reaches back that
   * many lines so output that scrolled off-screen is recovered.
   */
  scrollbackLines?: number;
}

/** Options accepted by {@link TerminalHost.attachTerminal}. */
export interface AttachTerminalOptions {
  /**
   * Sink for the terminal's LIVE raw output bytes. Called with each chunk of the
   * pane's pty output as it is produced — the exact bytes a locally-attached
   * operator's terminal would render (escape sequences and all), NOT the
   * capture-pane rendered grid {@link TerminalHost.readTerminal} returns. This is
   * the server→client half of a remote terminal-attach.
   */
  onData: (bytes: Uint8Array) => void;
}

/**
 * A live attachment to a terminal's pty — the runner side of a remote
 * terminal-attach. While attached, the terminal's output bytes flow to the
 * {@link AttachTerminalOptions.onData} sink; {@link write} types input bytes into
 * the pane, {@link resize} changes its dimensions, and {@link detach} tears the
 * attachment down (stopping the byte stream) WITHOUT killing the terminal.
 */
export interface AttachedTerminal {
  /**
   * Type raw input bytes into the terminal (the client→server half). Fire-and-
   * forget: delivery is best-effort so a caller pumping a WS channel never blocks
   * on a `send-keys` round-trip, and a write after {@link detach} (or against a
   * dead pane) is a silent no-op rather than a throw.
   */
  write(bytes: Uint8Array): void;
  /** Resize the terminal to `cols`×`rows`. Best-effort; a dead pane is a no-op. */
  resize(cols: number, rows: number): Promise<void>;
  /**
   * Detach: stop streaming output bytes and release the attachment. Idempotent
   * and does NOT terminate the terminal (a later attach re-attaches to the same
   * live pane). Never throws.
   */
  detach(): Promise<void>;
}

/**
 * The pane-management capability the sys_terminal_* tools drive. A
 * {@link SandboxHandle} MAY implement it (the tmux-backed handle does); detect it
 * with {@link asTerminalHost}. Each method addresses a terminal by the id
 * {@link launchTerminal} returned.
 */
export interface TerminalHost {
  /** Launch a program in a fresh pane; returns the new terminal's id. */
  launchTerminal(opts: LaunchTerminalOptions): Promise<{ terminalId: string }>;
  /** Deliver text and/or key chords to a terminal's pane. Throws if unknown. */
  sendTerminalKeys(terminalId: string, opts: SendTerminalOptions): Promise<void>;
  /** Capture the rendered pane (+ optional scrollback) as plain text. Throws if unknown. */
  readTerminal(terminalId: string, opts?: ReadTerminalOptions): Promise<string>;
  /** List the terminals this host has launched (with liveness). */
  listTerminals(): Promise<TerminalDescriptor[]>;
  /** Tear a terminal down. Idempotent — closing an unknown/gone id is a no-op. */
  closeTerminal(terminalId: string): Promise<void>;
  /**
   * Attach to a terminal's LIVE pty for a remote terminal-attach: stream its raw
   * output bytes to {@link AttachTerminalOptions.onData} and hand back an
   * {@link AttachedTerminal} to type input, resize, and detach. Throws if the
   * terminal id is unknown.
   */
  attachTerminal(terminalId: string, opts: AttachTerminalOptions): Promise<AttachedTerminal>;
}

/**
 * Feature-detect a {@link TerminalHost} on a {@link SandboxHandle}. Returns the
 * handle typed as a host when it implements the pane-management methods, else
 * `null` — so a caller (the bridge, a provider) exposes the sys_terminal_* tools
 * only for a sandbox that can actually back them, and stays silent for a
 * cloud-only handle. Mirrors the `if (handle.spawn) …` feature-detection the
 * sandbox boundary already uses.
 */
export function asTerminalHost(handle: unknown): TerminalHost | null {
  if (handle === null || typeof handle !== 'object') {
    return null;
  }
  const h = handle as Partial<TerminalHost>;
  if (
    typeof h.launchTerminal === 'function' &&
    typeof h.sendTerminalKeys === 'function' &&
    typeof h.readTerminal === 'function' &&
    typeof h.listTerminals === 'function' &&
    typeof h.closeTerminal === 'function' &&
    typeof h.attachTerminal === 'function'
  ) {
    return handle as TerminalHost;
  }
  return null;
}

/**
 * One sys_terminal tool definition. Structurally the SDK's `SdkMcpToolDefinition`
 * (`{ name, description, inputSchema, handler }`) — the SAME shape the `orca` MCP
 * tools use — so both surfaces register them uniformly. Re-exported under a local
 * name so consumers need not import the SDK type directly.
 */
export type SysTerminalTool = SdkMcpToolDefinition;

/**
 * Build the five sys_terminal_* tool definitions bound to `host`. Each handler
 * dispatches into the {@link TerminalHost}, so the interactive programs live in
 * the per-session sandbox. Errors are returned as `isError` CallToolResults (not
 * thrown) so a tool-level failure — an unknown terminal id, a dead pane — is
 * reported to the model/caller rather than tearing the MCP transport down.
 *
 * When `allowedLogicalNames` is supplied, the returned set is filtered to it (the
 * same skill-allowlist intersection the `orca` tools honor), so a snapshot can
 * withhold individual sys_terminal_* tools.
 */
export function buildSysTerminalTools(
  host: TerminalHost,
  allowedLogicalNames?: readonly string[],
): SysTerminalTool[] {
  const allowed = allowedLogicalNames ? new Set(allowedLogicalNames) : null;
  // Build with inferred per-tool schema types, then widen to the default-schema
  // `SysTerminalTool` via `unknown` — the same shape-preserving cast the `orca`
  // tools use. A direct `SysTerminalTool[]` annotation would (under
  // exactOptionalPropertyTypes) reject each concrete-schema handler against the
  // widened `AnyZodRawShape` handler signature.
  const tools = [
    tool(
      SYS_TERMINAL_LAUNCH,
      'Launch an interactive program in a fresh terminal (a tmux pane) inside the ' +
        'per-session sandbox. Use this for programs you must interact with over time ' +
        '(a REPL, a pager, an installer prompt) — for run-to-completion commands use bash. ' +
        'Returns a JSON object `{ "terminal_id": "…" }`; address the terminal by that id.',
      {
        command: z
          .string()
          .describe('The program/command line to run in the pane, e.g. `python3`.'),
        cwd: z.string().optional().describe('Sandbox-visible working directory for the program.'),
        cols: z.number().int().positive().optional().describe('Pane width in columns.'),
        rows: z.number().int().positive().optional().describe('Pane height in rows.'),
      },
      async ({ command, cwd, cols, rows }) => {
        try {
          const opts: LaunchTerminalOptions = { command };
          if (cwd !== undefined) opts.cwd = cwd;
          if (cols !== undefined) opts.cols = cols;
          if (rows !== undefined) opts.rows = rows;
          const { terminalId } = await host.launchTerminal(opts);
          return okJson({ terminal_id: terminalId });
        } catch (e) {
          return errText(`launch failed: ${(e as Error).message}`);
        }
      },
    ),

    tool(
      SYS_TERMINAL_SEND,
      'Send input to a running terminal: literal `text` to type, and/or `keys` — named ' +
        'key chords such as ["C-c"] (Ctrl-C), ["Enter"], ["Up","Up"], ["Escape"], ["Tab"]. ' +
        'Text is typed first, then the keys; set `enter: true` to submit a typed line. ' +
        'Read the result afterwards with sys_terminal_read.',
      {
        terminal_id: z.string().describe('The id returned by sys_terminal_launch.'),
        text: z.string().optional().describe('Literal characters to type into the pane.'),
        keys: z
          .array(z.string())
          .optional()
          .describe('Named keys / chords to send after the text, e.g. ["C-c"], ["Enter"].'),
        enter: z.boolean().optional().describe('Press Enter after `text` (submit the line).'),
      },
      async ({ terminal_id, text, keys, enter }) => {
        try {
          const opts: SendTerminalOptions = {};
          if (text !== undefined) opts.text = text;
          if (keys !== undefined) opts.keys = keys;
          if (enter !== undefined) opts.enter = enter;
          await host.sendTerminalKeys(terminal_id, opts);
          return okText(`sent to ${terminal_id}`);
        } catch (e) {
          return errText(`send failed: ${(e as Error).message}`);
        }
      },
    ),

    tool(
      SYS_TERMINAL_READ,
      'Read the current rendered contents of a terminal pane as plain text. By default ' +
        'captures only the visible pane; pass `scrollback_lines` to also include that many ' +
        'lines of history that scrolled off-screen.',
      {
        terminal_id: z.string().describe('The id returned by sys_terminal_launch.'),
        scrollback_lines: z
          .number()
          .int()
          .nonnegative()
          .optional()
          .describe('Lines of scrollback to include above the visible pane (0 = visible only).'),
      },
      async ({ terminal_id, scrollback_lines }) => {
        try {
          const opts: ReadTerminalOptions = {};
          if (scrollback_lines !== undefined) opts.scrollbackLines = scrollback_lines;
          const text = await host.readTerminal(terminal_id, opts);
          return okText(truncate(text));
        } catch (e) {
          return errText(`read failed: ${(e as Error).message}`);
        }
      },
    ),

    tool(
      SYS_TERMINAL_LIST,
      'List the interactive terminals currently open in the sandbox. Returns a JSON array ' +
        'of `{ "terminal_id", "command", "alive" }` entries.',
      {},
      async () => {
        try {
          const terminals = await host.listTerminals();
          return okJson(
            terminals.map((t) => ({
              terminal_id: t.terminalId,
              command: t.command,
              alive: t.alive,
            })),
          );
        } catch (e) {
          return errText(`list failed: ${(e as Error).message}`);
        }
      },
    ),

    tool(
      SYS_TERMINAL_CLOSE,
      'Close an interactive terminal, terminating its program. Idempotent — closing an ' +
        'already-gone terminal is not an error.',
      {
        terminal_id: z.string().describe('The id returned by sys_terminal_launch.'),
      },
      async ({ terminal_id }) => {
        try {
          await host.closeTerminal(terminal_id);
          return okText(`closed ${terminal_id}`);
        } catch (e) {
          return errText(`close failed: ${(e as Error).message}`);
        }
      },
    ),
  ];
  const widened = tools as unknown as SysTerminalTool[];
  return allowed ? widened.filter((t) => allowed.has(t.name)) : widened;
}

/** A non-error CallToolResult carrying a plain-text block. */
function okText(text: string): {
  content: Array<{ type: 'text'; text: string }>;
  isError: boolean;
} {
  return { content: [{ type: 'text', text }], isError: false };
}

/** A non-error CallToolResult carrying a JSON-encoded value as its text block. */
function okJson(value: unknown): {
  content: Array<{ type: 'text'; text: string }>;
  isError: boolean;
} {
  return okText(JSON.stringify(value));
}

/** An error CallToolResult carrying a plain-text message. */
function errText(text: string): {
  content: Array<{ type: 'text'; text: string }>;
  isError: boolean;
} {
  return { content: [{ type: 'text', text }], isError: true };
}
