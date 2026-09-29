#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A FAKE headless `cursor-agent`-like CLI for the cursor provider spec.
//
// It stands in for the REAL `cursor-agent` binary run headless (`--print --output-format
// stream-json`): a long-lived process that speaks newline-delimited JSON ("stream-json") over
// stdio. No real binary is needed to prove the provider — this script emits the SAME cursor
// stream-json shapes the real binary does and honors the SAME control protocol the harness
// drives:
//
//   - on boot it prints a `system`/`init` line (carrying the MCP server status),
//   - it reads stdin as stream-json; each `{type:"user", message:{...}}` line drives one
//     scripted "turn",
//   - it drives a turn by the FIRST word of the user text:
//       · "say <rest>"   → emit an `assistant` text block then a `result` frame,
//       · "run <cmd>"    → emit a `tool_call` started for `mcp__orca__bash`; when launched
//                          gated (no `--force`) it FIRST emits a `can_use_tool` control_request
//                          and WAITS for the matching `control_response` on stdin (the
//                          provider's approval reply). On ALLOW it actually CALLS the bridge's
//                          `bash` tool (proving orca tools resolve in the sandbox) and emits the
//                          real completed `tool_call`; on DENY it emits a failed `tool_call`.
//                          Either way it closes with a `result` frame.
//       · "cancelrun <cmd>" → like "run" but the CLI WITHDRAWS the permission right after
//                          raising it (a `control_cancel_request` for the same request_id),
//                          then waits for the harness to release the gate as a deny.
//   - it exits 0 on a user line whose text is "bye".
//
// The bridge it calls is read from its own `--mcp-config` (exactly as the real binary would):
// it spawns that MCP server as a child and speaks MCP-over-stdio to it. This makes the spec
// prove the native-CLI tool-bridge wiring end to end, with NO real cursor binary anywhere.
//
// It ALSO writes each argv it was launched with to the path in `--argv-out` (when given) so the
// parent test can assert the launch flags the provider emitted actually reached the CLI.

import { createInterface } from 'node:readline';
import { writeFileSync } from 'node:fs';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

function emit(obj) {
  process.stdout.write(`${JSON.stringify(obj)}\n`);
}

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

// Publish the raw argv so the parent can assert the launch flags reached the CLI.
const argvOut = argValue('--argv-out');
if (argvOut) {
  writeFileSync(argvOut, JSON.stringify(process.argv.slice(2)), 'utf8');
}

// Gated iff `--force` is ABSENT (with a gate the harness routes permissions over the stream).
const gated = !hasFlag('--force');

// Parse the bridge MCP server config out of `--mcp-config` so a tool call can be dispatched
// through it (the real binary launches these servers itself).
let bridgeServer;
const mcpConfigRaw = argValue('--mcp-config');
if (mcpConfigRaw) {
  try {
    const parsed = JSON.parse(mcpConfigRaw);
    bridgeServer = parsed?.mcpServers?.orca;
  } catch {
    bridgeServer = undefined;
  }
}

// Boot line: the init frame the real binary emits first.
emit({
  type: 'system',
  subtype: 'init',
  session_id: 'fake-cursor-session',
  mcp_servers: bridgeServer ? [{ name: 'orca', status: 'connected' }] : [],
});

// Pending control_responses from stdin, keyed by request_id — resolved by the reader.
const pendingPermissions = new Map();

/** Ask the caller (the provider) for permission to use a tool; resolves the verdict. */
function requestPermission(requestId, toolName, input, toolUseId) {
  return new Promise((resolve) => {
    pendingPermissions.set(requestId, resolve);
    emit({
      type: 'control_request',
      request_id: requestId,
      request: {
        subtype: 'can_use_tool',
        tool_name: toolName,
        input,
        tool_use_id: toolUseId,
      },
    });
  });
}

/** Call a tool on the bridge MCP server (spawns it as a child, MCP-over-stdio). */
async function callBridgeTool(name, args) {
  if (!bridgeServer) {
    throw new Error('no bridge server configured');
  }
  const transport = new StdioClientTransport({
    command: bridgeServer.command,
    args: bridgeServer.args ?? [],
    env: { ...process.env, ...(bridgeServer.env ?? {}) },
  });
  const client = new Client({ name: 'fake-cursor', version: '1.0.0' });
  await client.connect(transport);
  try {
    return await client.callTool({ name, arguments: args });
  } finally {
    await client.close();
  }
}

let toolSeq = 0;

/** Drive one "turn" for a user message text. */
async function runTurn(text) {
  const [verb, ...rest] = text.split(' ');

  if (verb === 'say') {
    emit({ type: 'assistant', message: { content: [{ type: 'text', text: rest.join(' ') }] } });
    emit({ type: 'result', subtype: 'success', usage: { input_tokens: 3, output_tokens: 2 } });
    return;
  }

  if (verb === 'cancelrun') {
    // Like `run`, but the CLI WITHDRAWS the permission right after raising it (emitting a
    // `control_cancel_request` for the same request_id) instead of waiting for a verdict. The
    // turn completes only once the harness observes the cancel and replies with a
    // `control_response` (a deny), so this proves the cancel path is functional end to end.
    const command = rest.join(' ');
    const callId = `call_${++toolSeq}`;
    const requestId = `req_${toolSeq}`;
    emit({
      type: 'tool_call',
      subtype: 'started',
      callId,
      name: 'mcp__orca__bash',
      args: { command },
    });
    const verdict = await new Promise((resolve) => {
      pendingPermissions.set(requestId, resolve);
      emit({
        type: 'control_request',
        request_id: requestId,
        request: {
          subtype: 'can_use_tool',
          tool_name: 'mcp__orca__bash',
          input: { command },
          tool_use_id: callId,
        },
      });
      // Immediately withdraw the request — the harness must release the parked gate.
      emit({ type: 'control_cancel_request', request_id: requestId });
    });
    const isDeny = verdict?.behavior === 'deny';
    emit({
      type: 'tool_call',
      subtype: 'completed',
      callId,
      name: 'mcp__orca__bash',
      args: { command },
      status: isDeny ? 'error' : 'completed',
      result: { content: [{ type: 'text', text: isDeny ? 'cancelled' : 'unexpectedly allowed' }] },
    });
    emit({ type: 'result', subtype: 'success', usage: { input_tokens: 2, output_tokens: 1 } });
    return;
  }

  if (verb === 'run') {
    const command = rest.join(' ');
    const callId = `call_${++toolSeq}`;
    // Announce the tool_call start (as the real binary does before running the tool).
    emit({
      type: 'tool_call',
      subtype: 'started',
      callId,
      name: 'mcp__orca__bash',
      args: { command },
    });

    let allowed = true;
    // The real binary surfaces the harness's deny MESSAGE to the model as the tool_call
    // result, so the fake does too — that message is the only channel a faulted approval
    // GATE has on this protocol, and a fixture that dropped it would let the cause vanish
    // untested.
    let denyMessage = 'denied by user';
    if (gated) {
      const verdict = await requestPermission(
        `req_${toolSeq}`,
        'mcp__orca__bash',
        { command },
        callId,
      );
      allowed = verdict?.behavior === 'allow';
      if (typeof verdict?.message === 'string' && verdict.message.length > 0) {
        denyMessage = verdict.message;
      }
    }

    if (allowed) {
      // Actually run the tool through the bridge — proving orca tools resolve INSIDE the
      // sandbox. The result text is echoed back as the completed tool_call result.
      let resultContent;
      try {
        const res = await callBridgeTool('bash', { command });
        resultContent = res?.content ?? [{ type: 'text', text: '' }];
      } catch (err) {
        resultContent = [{ type: 'text', text: `bridge call failed: ${String(err)}` }];
      }
      emit({
        type: 'tool_call',
        subtype: 'completed',
        callId,
        name: 'mcp__orca__bash',
        args: { command },
        status: 'completed',
        result: { content: resultContent },
      });
    } else {
      emit({
        type: 'tool_call',
        subtype: 'completed',
        callId,
        name: 'mcp__orca__bash',
        args: { command },
        status: 'error',
        result: { content: [{ type: 'text', text: denyMessage }] },
      });
    }
    emit({ type: 'result', subtype: 'success', usage: { input_tokens: 5, output_tokens: 4 } });
    return;
  }

  // Unknown verb: just complete the turn.
  emit({ type: 'result', subtype: 'success' });
}

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });

// Serialize turns so overlapping stdin lines don't interleave stdout.
let chain = Promise.resolve();

rl.on('line', (line) => {
  const trimmed = line.trim();
  if (trimmed.length === 0) {
    return;
  }
  let msg;
  try {
    msg = JSON.parse(trimmed);
  } catch {
    return; // ignore non-JSON
  }

  // A control_response answers a pending permission request.
  if (msg?.type === 'control_response') {
    const requestId = msg?.response?.request_id;
    const resolver = pendingPermissions.get(requestId);
    if (resolver) {
      pendingPermissions.delete(requestId);
      resolver(msg?.response?.response);
    }
    return;
  }

  // A control_request from the caller (e.g. interrupt) — acknowledge + abort any in-flight turn
  // by resolving its pending permission as a deny.
  if (msg?.type === 'control_request' && msg?.request?.subtype === 'interrupt') {
    for (const [requestId, resolver] of pendingPermissions) {
      pendingPermissions.delete(requestId);
      resolver({ behavior: 'deny', message: 'interrupted' });
    }
    emit({
      type: 'control_response',
      response: { subtype: 'success', request_id: msg.request_id, response: {} },
    });
    return;
  }

  if (msg?.type !== 'user') {
    return;
  }
  const content = msg?.message?.content;
  const text =
    typeof content === 'string'
      ? content
      : Array.isArray(content)
        ? (content.find((b) => b?.type === 'text')?.text ?? '')
        : '';

  if (text === 'bye') {
    chain = chain.then(() => {
      rl.close();
    });
    return;
  }
  chain = chain.then(() => runTurn(text));
});

rl.on('close', () => {
  process.exit(0);
});
