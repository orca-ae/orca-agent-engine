// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The Orca native-CLI tool-bridge, surfaced to `pi` as an EXTENSION.
//
// Pi's headless RPC mode has no MCP-config surface, so the runner's native-CLI tool-bridge — the
// provider-agnostic MCP server that binds the orca built-ins (bash/read/write/edit/glob/grep +
// sys_terminal_*) to the per-session sandbox — is wired into pi as a pi extension instead. Pi
// loads this module via `--extension <this file>` (SINGULAR — the real pi flag) and invokes the
// exported factory once at session start; the factory:
//
//   1. reads the bridge child command + args the harness stamped on the process env
//      (ORCA_PI_BRIDGE_COMMAND / ORCA_PI_BRIDGE_ARGS — a JSON-encoded argv), which point at the
//      shared bridge entry (`node <bridge-entry.js> --root <sandbox-root> …`),
//   2. spawns that bridge entry as an MCP-over-stdio child and lists its tools,
//   3. registers EACH bridge tool onto pi via `pi.registerTool`, with an `execute` that proxies
//      the call to the bridge — so pi's model calls the orca tools and they execute INSIDE the
//      session sandbox (the bridge reattaches to the same `--root`), exactly like the codex /
//      claude-code providers wire the SAME bridge as their MCP server, and
//   4. registers a `tool_call` HOOK — pi's PRE-EXECUTION gate. Pi's RPC mode has no per-tool
//      server→client approval request, so the sound gate is this hook (fires after
//      `tool_execution_start`, BEFORE the tool runs; can BLOCK). The hook asks the client to
//      approve via `ctx.ui.select(...)` — which pi serializes as an `extension_ui_request` on
//      stdout and blocks the tool until the client replies with an `extension_ui_response` — and
//      returns `{ block: true, reason }` on a `Block`/cancelled verdict. This is a genuine
//      per-tool, pre-exec block, NOT a whole-turn abort. The tool identity rides the request
//      `title` as a tagged marker (`ORCA_TOOL_APPROVAL\t<toolCallId>\t<toolName>`) because pi's
//      `ctx.ui.select(title, options)` forwards no structured metadata; the harness parses it back.
//      The hook only gates when `ctx.hasUI` is true (RPC/TUI modes) — with no UI channel wired
//      there is nothing to answer the request, so tools proceed (the OS sandbox + composed tool
//      policies are the guardrail), matching the other providers' no-gate behavior.
//
// This is a REAL shipped module — it runs for every `pi` session — not a test fake. It is a
// `.mjs` (loaded directly by pi's Node runtime, not compiled by the runner build), and depends
// only on `@modelcontextprotocol/sdk`, which the runner already vendors.

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

/** Env var carrying the bridge child executable the extension spawns (e.g. `node`). */
const BRIDGE_COMMAND_ENV = 'ORCA_PI_BRIDGE_COMMAND';
/** Env var carrying the bridge child args as a JSON array (e.g. `["<entry.js>","--root","/work"]`). */
const BRIDGE_ARGS_ENV = 'ORCA_PI_BRIDGE_ARGS';

/**
 * The tagged prefix stamped on the approval `ctx.ui.select` title so the harness recovers the tool
 * identity (pi forwards no structured metadata on a UI request). MUST match the harness protocol
 * constant `ORCA_TOOL_APPROVAL_TITLE_PREFIX`. Fields are TAB-separated: `PREFIX\t<id>\t<name>`.
 */
const ORCA_TOOL_APPROVAL_TITLE_PREFIX = 'ORCA_TOOL_APPROVAL';
/** The `Block` option — the harness replies with this option string to DENY (pre-exec block). */
const ORCA_APPROVAL_BLOCK = 'Block';
/** The `Allow` option — the harness replies with this option string to permit the tool. */
const ORCA_APPROVAL_ALLOW = 'Allow';

/** Parse the bridge child launch (command + args) from the process env, or return null. */
function readBridgeLaunch(env) {
  const command = typeof env[BRIDGE_COMMAND_ENV] === 'string' ? env[BRIDGE_COMMAND_ENV] : '';
  if (command.length === 0) {
    return null;
  }
  let args = [];
  const rawArgs = env[BRIDGE_ARGS_ENV];
  if (typeof rawArgs === 'string' && rawArgs.length > 0) {
    try {
      const parsed = JSON.parse(rawArgs);
      if (Array.isArray(parsed)) {
        args = parsed.filter((a) => typeof a === 'string');
      }
    } catch {
      args = [];
    }
  }
  return { command, args };
}

/**
 * A permissive JSON schema fallback for a bridge tool that advertised no input schema. Pi
 * validates tool arguments with AJV, which accepts a plain JSON schema; an object with no
 * declared properties admits whatever the model passes.
 */
const PERMISSIVE_SCHEMA = { type: 'object', properties: {}, additionalProperties: true };

/** Normalize a bridge tool's advertised input schema to a plain JSON schema pi/AJV accepts. */
function toParameters(inputSchema) {
  if (inputSchema && typeof inputSchema === 'object' && inputSchema.type === 'object') {
    return inputSchema;
  }
  return PERMISSIVE_SCHEMA;
}

/**
 * The pi extension factory. Connects to the native-CLI tool-bridge and registers each of its
 * tools onto pi. Exported as the module default so `--extension` loads it.
 *
 * @param {{ registerTool: (tool: object) => void }} pi The pi extension API.
 */
export default async function orcaBridgeExtension(pi) {
  const launch = readBridgeLaunch(process.env);
  if (launch === null) {
    // No bridge wired (a chat-only launch) — nothing to register. Pi keeps its built-in tools.
    return;
  }

  const transport = new StdioClientTransport({
    command: launch.command,
    args: launch.args,
    env: { ...process.env },
  });
  const client = new Client({ name: 'orca-pi-bridge', version: '1.0.0' });
  await client.connect(transport);

  const listed = await client.listTools();
  const tools = Array.isArray(listed?.tools) ? listed.tools : [];
  for (const tool of tools) {
    const name = typeof tool?.name === 'string' ? tool.name : '';
    if (name.length === 0) {
      continue;
    }
    const description = typeof tool?.description === 'string' ? tool.description : name;
    pi.registerTool({
      name,
      label: name,
      description,
      parameters: toParameters(tool?.inputSchema),
      // Proxy the call to the bridge — it executes inside the session sandbox (the bridge child
      // reattached to the session `--root`). The bridge is a long-lived child; the single client
      // connection above is reused across calls for the session's lifetime.
      async execute(_toolCallId, params) {
        const res = await client.callTool({ name, arguments: params ?? {} });
        const content = Array.isArray(res?.content) ? res.content : [{ type: 'text', text: '' }];
        return { content, details: { isError: res?.isError === true } };
      },
    });
  }

  // The PRE-EXECUTION approval gate. Pi's RPC mode has no per-tool server→client approval request,
  // so the sound gate is the `tool_call` hook: it fires BEFORE the tool executes and can BLOCK.
  // The hook asks the client to approve via `ctx.ui.select` (serialized by pi as an
  // `extension_ui_request` the harness answers with an `extension_ui_response`); a `Block`/cancelled
  // verdict returns `{ block: true, reason }`, so pi never runs the tool and surfaces an error
  // tool_result. The tool identity rides the request title as a tagged marker the harness parses.
  if (typeof pi.on === 'function') {
    pi.on('tool_call', async (event, ctx) => {
      // No UI channel to answer the request (print/JSON mode) → no gate; the OS sandbox + composed
      // tool policies are the guardrail, matching the other providers' no-gate behavior.
      if (!ctx || ctx.hasUI !== true || !ctx.ui || typeof ctx.ui.select !== 'function') {
        return undefined;
      }
      const toolCallId = typeof event?.toolCallId === 'string' ? event.toolCallId : '';
      const toolName = typeof event?.toolName === 'string' ? event.toolName : '';
      // The tagged title is the metadata carrier (pi's select forwards no structured fields).
      const title = `${ORCA_TOOL_APPROVAL_TITLE_PREFIX}\t${toolCallId}\t${toolName}`;
      let choice;
      try {
        choice = await ctx.ui.select(title, [ORCA_APPROVAL_ALLOW, ORCA_APPROVAL_BLOCK]);
      } catch {
        // A failed/cancelled request is a fail-closed DENY (never run un-approved).
        return { block: true, reason: 'Tool use denied: approval channel closed.' };
      }
      if (choice === ORCA_APPROVAL_ALLOW) {
        return undefined; // approved — let the tool run.
      }
      // `Block`, undefined (cancelled), or anything else → fail-closed block.
      return { block: true, reason: 'Tool use denied by approval policy.' };
    });
  }

  // Best-effort teardown when pi shuts the session down, so the bridge child is not orphaned.
  if (typeof pi.on === 'function') {
    pi.on('session_shutdown', async () => {
      await client.close().catch(() => {});
    });
  }
}
