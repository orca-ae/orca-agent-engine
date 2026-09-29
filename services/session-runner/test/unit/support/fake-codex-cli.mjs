#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A FAKE `codex app-server`-like CLI for the codex provider spec.
//
// It stands in for the REAL `codex` binary run as `codex app-server`: a long-lived process
// that speaks JSON-RPC over stdio. No real binary is needed to prove the provider — this
// script speaks the SAME JSON-RPC shapes the real app-server does and honors the SAME
// protocol the harness drives:
//
//   - it reads stdin as JSON-RPC (one object per line): `initialize`, `thread/start`,
//     `turn/start` REQUESTS get a matching `{id,result}` response; the harness's RESPONSES
//     to server→client approval requests (`{id,result:{decision}}`) resolve a pending
//     approval; a `turn/interrupt` request fails the in-flight turn.
//   - it drives one "turn" per `turn/start`, dispatching on the FIRST word of the turn text:
//       · "say <rest>"  → emit an `item/completed` agentMessage then `turn/completed`,
//       · "run <cmd>"   → emit an `item/completed` mcpToolCall for the `orca`/`bash` tool
//                         (actually CALLING the bridge MCP server, proving orca tools
//                         resolve in the sandbox) then `turn/completed`,
//       · "exec <cmd>"  → codex's BUILT-IN shell path: when launched with
//                         `approval_policy=on-request` it FIRST raises a server→client
//                         `item/commandExecution/requestApproval` and WAITS for the
//                         harness's decision. On accept it runs the command via the bridge's
//                         `bash` tool and emits a `commandExecution` item; on decline it
//                         emits a failed `commandExecution` item. Either way it closes with
//                         `turn/completed`.
//   - it publishes a `thread/tokenUsage/updated` before each `turn/completed` so the harness
//     can attribute usage to the turn.
//
// The bridge it calls is read from its own `-c mcp_servers.orca.command` / `.args` overrides
// (exactly as the real binary would from its config): it spawns that MCP server as a child
// and speaks MCP-over-stdio to it. This makes the spec prove the native-CLI tool-bridge
// wiring end to end, with NO real codex binary anywhere.
//
// It ALSO writes each argv it was launched with to the path in `--argv-out` (when given).

import { createInterface } from 'node:readline';
import { writeFileSync } from 'node:fs';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

function send(obj) {
  process.stdout.write(`${JSON.stringify(obj)}\n`);
}

/** Read the value after a repeated `-c key=value` codex override whose key is `key`. */
function configValue(key) {
  for (let i = 0; i < process.argv.length; i += 1) {
    if (process.argv[i] === '-c' && typeof process.argv[i + 1] === 'string') {
      const eq = process.argv[i + 1].indexOf('=');
      if (eq > 0 && process.argv[i + 1].slice(0, eq) === key) {
        return process.argv[i + 1].slice(eq + 1);
      }
    }
  }
  return undefined;
}

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

const argvOut = argValue('--argv-out');
if (argvOut) {
  writeFileSync(argvOut, JSON.stringify(process.argv.slice(2)), 'utf8');
}

// The approval policy the harness launched us with (on-request routes exec to the gate).
let approvalPolicy = 'never';
const rawPolicy = configValue('approval_policy');
if (rawPolicy) {
  try {
    approvalPolicy = JSON.parse(rawPolicy);
  } catch {
    approvalPolicy = rawPolicy;
  }
}

// The bridge MCP server config, parsed out of the `-c mcp_servers.orca.command/.args`.
let bridgeCommand;
let bridgeArgs = [];
try {
  const cmd = configValue('mcp_servers.orca.command');
  if (cmd) {
    bridgeCommand = JSON.parse(cmd);
  }
  const args = configValue('mcp_servers.orca.args');
  if (args) {
    bridgeArgs = JSON.parse(args);
  }
} catch {
  bridgeCommand = undefined;
}

/** Call a tool on the bridge MCP server (spawns it as a child, MCP-over-stdio). */
async function callBridgeTool(name, args) {
  if (!bridgeCommand) {
    throw new Error('no bridge server configured');
  }
  const transport = new StdioClientTransport({
    command: bridgeCommand,
    args: bridgeArgs,
    env: { ...process.env },
  });
  const client = new Client({ name: 'fake-codex', version: '1.0.0' });
  await client.connect(transport);
  try {
    return await client.callTool({ name, arguments: args });
  } finally {
    await client.close();
  }
}

let threadSeq = 0;
let turnSeq = 0;
let itemSeq = 0;
let reqSeq = 1000;
// Pending server→client approval requests, keyed by request id — resolved by stdin RESULTs.
const pendingApprovals = new Map();
// The active turn id, so a `turn/interrupt` can fail exactly the in-flight turn.
let activeTurnId = null;
let interrupted = false;

/** Raise a server→client approval request and resolve with the harness's decision. */
function requestApproval(method, params) {
  const id = ++reqSeq;
  return new Promise((resolve) => {
    pendingApprovals.set(id, resolve);
    send({ id, method, params });
  });
}

function emitUsage(turnId) {
  send({
    method: 'thread/tokenUsage/updated',
    params: { turnId, usage: { last: { input_tokens: 7, output_tokens: 3 } } },
  });
}

/** Drive one turn for the given text, on the given turn id. */
async function runTurn(turnId, text) {
  const [verb, ...rest] = text.split(' ');

  if (verb === 'say') {
    send({
      method: 'item/completed',
      params: {
        turnId,
        item: {
          id: `item_${++itemSeq}`,
          type: 'agentMessage',
          text: rest.join(' '),
          phase: 'final_answer',
        },
      },
    });
    emitUsage(turnId);
    send({ method: 'turn/completed', params: { turn: { id: turnId } } });
    return;
  }

  if (verb === 'run') {
    // MCP tool call through the bridge (proves the orca tools resolve in the sandbox).
    const command = rest.join(' ');
    let resultContent;
    let isError = false;
    try {
      const res = await callBridgeTool('bash', { command });
      resultContent = res?.content ?? [{ type: 'text', text: '' }];
    } catch (err) {
      resultContent = [{ type: 'text', text: `bridge call failed: ${String(err)}` }];
      isError = true;
    }
    send({
      method: 'item/completed',
      params: {
        turnId,
        item: {
          id: `item_${++itemSeq}`,
          type: 'mcpToolCall',
          server: 'orca',
          tool: 'bash',
          arguments: { command },
          status: isError ? 'failed' : 'completed',
          result: { content: resultContent },
        },
      },
    });
    emitUsage(turnId);
    send({ method: 'turn/completed', params: { turn: { id: turnId } } });
    return;
  }

  if (verb === 'exec') {
    // codex's BUILT-IN shell path — gated by approval_policy=on-request.
    const command = rest.join(' ');
    let approved = true;
    if (approvalPolicy === 'on-request') {
      const decision = await requestApproval('item/commandExecution/requestApproval', {
        turnId,
        command: command.split(' '),
        cwd: '/work',
        callId: `call_${itemSeq + 1}`,
        reason: 'requires approval',
      });
      approved = decision?.decision === 'accept' || decision?.decision === 'approved';
    }
    if (interrupted) {
      return;
    }
    if (approved) {
      let output = '';
      let exitCode = 0;
      try {
        const res = await callBridgeTool('bash', { command });
        output = res?.content?.[0]?.text ?? '';
      } catch (err) {
        output = `bridge call failed: ${String(err)}`;
        exitCode = 1;
      }
      send({
        method: 'item/completed',
        params: {
          turnId,
          item: {
            id: `item_${++itemSeq}`,
            type: 'commandExecution',
            command,
            aggregatedOutput: output,
            exitCode,
            status: exitCode === 0 ? 'completed' : 'failed',
          },
        },
      });
    } else {
      send({
        method: 'item/completed',
        params: {
          turnId,
          item: {
            id: `item_${++itemSeq}`,
            type: 'commandExecution',
            command,
            aggregatedOutput: 'rejected by approval policy',
            exitCode: 1,
            status: 'failed',
          },
        },
      });
    }
    emitUsage(turnId);
    send({ method: 'turn/completed', params: { turn: { id: turnId } } });
    return;
  }

  // Unknown verb: just complete the turn.
  send({ method: 'turn/completed', params: { turn: { id: turnId } } });
}

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
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
    return;
  }

  // A RESULT (id + result, no method) answers a pending server→client approval request.
  if (msg?.id !== undefined && msg?.method === undefined && msg?.result !== undefined) {
    const resolver = pendingApprovals.get(msg.id);
    if (resolver) {
      pendingApprovals.delete(msg.id);
      resolver(msg.result);
    }
    return;
  }

  const method = msg?.method;
  const id = msg?.id;

  if (method === 'initialize') {
    send({ id, result: { userAgent: 'fake-codex/0.1', capabilities: {} } });
    return;
  }
  if (method === 'initialized') {
    return; // notification, no reply
  }
  if (method === 'thread/start') {
    const threadId = `thr_${++threadSeq}`;
    send({ id, result: { thread: { id: threadId } } });
    return;
  }
  if (method === 'turn/start') {
    const turnId = `turn_${++turnSeq}`;
    activeTurnId = turnId;
    interrupted = false;
    // The turn/start response returns the turn id; notifications then stream the turn.
    send({ id, result: { turn: { id: turnId } } });
    const input = Array.isArray(msg?.params?.input) ? msg.params.input : [];
    const text = input
      .map((b) => (typeof b?.text === 'string' ? b.text : ''))
      .join(' ')
      .trim();
    chain = chain.then(() => runTurn(turnId, text));
    return;
  }
  if (method === 'turn/interrupt') {
    interrupted = true;
    // Release any parked approval as a decline so a blocked turn unwinds.
    for (const [reqId, resolver] of pendingApprovals) {
      pendingApprovals.delete(reqId);
      resolver({ decision: 'decline' });
    }
    send({ id, result: {} });
    // Emit turn/failed for the active turn so the harness settles submit.
    if (activeTurnId) {
      send({
        method: 'turn/failed',
        params: { turn: { id: activeTurnId }, message: 'interrupted' },
      });
    }
    return;
  }
  // Unknown request with an id: reply with an empty result so the client is not stranded.
  if (id !== undefined) {
    send({ id, result: {} });
  }
});

rl.on('close', () => {
  process.exit(0);
});
