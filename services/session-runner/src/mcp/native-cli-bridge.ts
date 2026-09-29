// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// native-CLI tool-bridge — an MCP server over STDIO that binds the runner's
// built-in tools to a per-session sandbox.
//
// **Why this exists.** A native coding CLI launched by the framework (the
// native-CLI provider path) runs as a long-lived child. On its own, that CLI's
// built-in file/exec tools would execute on the CLI's OWN host — with no
// connection to the per-session {@link SandboxHandle} the runner acquired. The
// fix mirrors what the claude provider does with its in-process `orca` MCP server
// (`harness/claude/mcp-tools.ts`), but for an OUT-OF-PROCESS consumer: this module
// stands up an MCP server that speaks the stdio transport, so a native CLI given
// this bridge as its MCP server has its tool calls execute INSIDE the Orca sandbox.
//
// **What it exposes.** Exactly the same built-in surface the claude provider's
// `orca` server exposes — `bash`/`read`/`write`/`edit`/`glob`/`grep`/`list`/
// `delete`, produced by the shared {@link buildOrcaSdkTools} so there is ONE
// definition of each sandbox-bound tool — PLUS the Orca-superset sys_terminal_*
// tools when the sandbox can host interactive panes ({@link asTerminalHost}). The
// tool definitions carry the SDK's `{ name, description, inputSchema, handler }`
// shape, which registers directly onto the MCP `McpServer` via `registerTool`, so
// the same tool objects flow to both the in-process claude surface and this
// out-of-process bridge without a per-surface adapter.
//
// **Naming.** The server is named `orca` (the reserved brand name the in-process
// server also uses), so a native CLI sees the built-ins under the same identity
// the rest of the harness advertises. Over the bridge a consumer typically calls
// tools by their bare logical name (`bash`, `sys_terminal_launch`); when a caller
// needs the MCP-qualified form it is `mcp__orca__<tool>`, matching the in-process
// convention.
//
// **Consumer wiring.** This module is a standalone, transport-complete bridge,
// exercised end-to-end by its spec (a real MCP `Client` over a stdio subprocess
// against a real sandbox). Composing it with {@link launchNativeCli} (the sibling
// launch framework) is per-CLI: each native-CLI provider (Claude Code / Codex /
// Cursor / Pi / custom) owns its launch config and advertises this bridge as the
// CLI's `orca` MCP server by pointing the CLI at its own `bridge-entry.ts`, a small
// entrypoint that delegates to the shared `runBridgeEntry`
// (`harness/claude-code/bridge-runtime.ts`), which calls
// {@link serveNativeCliBridge} over that child's stdio. Keeping the composition
// per-provider is deliberate — see `docs/managed-agents/session-runner-scope.md`
// (Native-CLI providers). The in-process claude providers expose the same surface
// (sys_terminal_* included) through their in-process `orca` server
// (`harness/claude/mcp-tools.ts`) instead.

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import type { Readable, Writable } from 'node:stream';
import type { SdkMcpToolDefinition } from '@anthropic-ai/claude-agent-sdk';
import type { SandboxHandle } from '../sandbox/seam.js';
import {
  buildOrcaSdkTools,
  ORCA_MCP_SERVER_NAME,
  ORCA_MCP_TOOL_LOGICAL_NAMES,
} from '../harness/claude/mcp-tools.js';
import {
  asTerminalHost,
  buildSysTerminalTools,
  SYS_TERMINAL_TOOL_LOGICAL_NAMES,
} from '../tools/sys-terminal.js';

/** Version reported by the bridge's MCP server handshake. */
const BRIDGE_VERSION = '1.0.0';

/** Options for building/serving the bridge. */
export interface NativeCliBridgeOptions {
  /**
   * Restrict the built-in tools to these logical names (the same skill-allowlist
   * intersection the snapshot carries for the claude surface). Applies to BOTH
   * the `orca` built-ins and the sys_terminal_* tools; omitted → every available
   * tool is exposed.
   */
  allowedLogicalNames?: readonly string[];
}

/**
 * Build the bridge's {@link McpServer} bound to `sandbox`, with every built-in
 * tool registered: the `orca` file/exec tools (always) and the sys_terminal_*
 * tools (only when `sandbox` can host interactive panes). The returned server is
 * not yet connected to a transport — {@link serveNativeCliBridge} does that;
 * exposing the build step separately keeps it unit-testable (link it to an
 * in-memory transport) without spawning a subprocess.
 */
export function buildNativeCliBridgeServer(
  sandbox: SandboxHandle,
  opts: NativeCliBridgeOptions = {},
): McpServer {
  const server = new McpServer({ name: ORCA_MCP_SERVER_NAME, version: BRIDGE_VERSION });
  for (const def of collectBridgeTools(sandbox, opts.allowedLogicalNames)) {
    registerTool(server, def);
  }
  return server;
}

/**
 * The live bridge returned by {@link serveNativeCliBridge}: the connected server
 * plus lifecycle hooks the caller (or the fake host in tests) drives.
 */
export interface NativeCliBridge {
  /** The connected MCP server (for advanced use — e.g. sending notifications). */
  readonly server: McpServer;
  /** Register a callback fired when the stdio transport closes (client disconnected). */
  onClose(cb: () => void): void;
  /** Close the server + its transport. Idempotent. */
  close(): Promise<void>;
}

/**
 * Build the bridge server for `sandbox` and connect it over a
 * {@link StdioServerTransport} (this process's stdin/stdout by default). Once this
 * resolves, a native CLI that spawned this process as its MCP server can complete
 * the MCP handshake and drive the sandbox-bound tools.
 *
 * The transport is injectable (`stdin`/`stdout`) so a test can drive the bridge
 * over an explicit pipe; the defaults are the current process's std streams,
 * which is exactly the wiring a framework-launched native CLI expects.
 */
export async function serveNativeCliBridge(
  sandbox: SandboxHandle,
  opts: NativeCliBridgeOptions & { stdin?: Readable; stdout?: Writable } = {},
): Promise<NativeCliBridge> {
  const server = buildNativeCliBridgeServer(sandbox, opts);
  const transport = new StdioServerTransport(opts.stdin, opts.stdout);
  const closeCallbacks: Array<() => void> = [];
  transport.onclose = (): void => {
    for (const cb of closeCallbacks) {
      cb();
    }
  };
  await server.connect(transport);
  let closed = false;
  return {
    server,
    onClose(cb: () => void): void {
      closeCallbacks.push(cb);
    },
    async close(): Promise<void> {
      if (closed) {
        return;
      }
      closed = true;
      await server.close();
    },
  };
}

/**
 * The logical (unqualified) tool names the bridge will expose for `sandbox`: the
 * `orca` built-ins always, plus the sys_terminal_* names when the sandbox hosts
 * panes. Exported so a provider advertising the bridge's surface, and tests, share
 * one derivation rather than hard-coding the union.
 */
export function nativeCliBridgeToolLogicalNames(sandbox: SandboxHandle): string[] {
  const names = [...ORCA_MCP_TOOL_LOGICAL_NAMES];
  if (asTerminalHost(sandbox) !== null) {
    names.push(...SYS_TERMINAL_TOOL_LOGICAL_NAMES);
  }
  return names;
}

/**
 * Gather the tool definitions to register for `sandbox`: the `orca` file/exec
 * tools bound to the handle, and — when the handle exposes the {@link TerminalHost}
 * capability — the sys_terminal_* tools bound to it too. A cloud-only handle
 * without pane support simply contributes no terminal tools (the built-ins still
 * bind), so the bridge degrades gracefully rather than advertising tools it cannot
 * back.
 */
function collectBridgeTools(
  sandbox: SandboxHandle,
  allowedLogicalNames?: readonly string[],
): SdkMcpToolDefinition[] {
  const defs: SdkMcpToolDefinition[] = [...buildOrcaSdkTools(sandbox, allowedLogicalNames)];
  const terminalHost = asTerminalHost(sandbox);
  if (terminalHost !== null) {
    defs.push(...buildSysTerminalTools(terminalHost, allowedLogicalNames));
  }
  return defs;
}

/**
 * Register one {@link SdkMcpToolDefinition} onto the MCP server. The SDK's tool
 * shape (`name`/`description`/`inputSchema` as a zod raw shape/`handler`) maps
 * 1:1 onto `McpServer.registerTool`, and the handler's `CallToolResult` return is
 * exactly what the MCP wire expects — so the same tool object serves the
 * in-process claude MCP server and this bridge unchanged.
 */
function registerTool(server: McpServer, def: SdkMcpToolDefinition): void {
  server.registerTool(
    def.name,
    { description: def.description, inputSchema: def.inputSchema },
    def.handler,
  );
}
