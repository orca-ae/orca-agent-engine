#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A FAKE JSON-LINE custom CLI for the generic `custom` provider spec.
//
// It stands in for an operator-registered CLI that speaks newline-delimited JSON on BOTH ends: it
// reads a user prompt as a JSON line on stdin and emits assistant text / tool calls / tool results /
// usage / a turn-end marker (and, when gated, an approval request) as JSON lines on stdout. This
// proves the `custom` provider's JSON-LINE stdout mapping + the optional approval routing end to
// end, with NO real binary anywhere.
//
// stdin  (per turn): `{ "prompt": "<verb> <rest>" }` (the spec's stdin template).
// stdout (per turn), dispatching on the FIRST word of the prompt:
//   · "say <rest>"  → { type:"assistant", text:"<rest>" }, { type:"usage", in, out }, { type:"done" }
//   · "run <cmd>"   → CALL the orca bridge `bash` tool (proving orca tools resolve in the sandbox),
//                     emit { type:"tool_call", ... } + { type:"tool_result", ... } + done.
//   · "exec <cmd>"  → the GATED built-in path: emit { type:"approval", request_id, tool:"bash", args }
//                     and WAIT for the harness's `{ type:"approval_response", request_id, decision }`
//                     on stdin. On allow: run the command via the bridge + emit a completed
//                     tool_result; on deny: emit an error tool_result. Either way close with done.
//
// The bridge child launch is read from `--bridge-cmd` / `--bridge-args` (the placeholders the
// provider substituted). Each launched argv is written to `--argv-out` when given.

import { createInterface } from 'node:readline';
import { writeFileSync } from 'node:fs';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

const argvOut = argValue('--argv-out');
if (argvOut) {
  writeFileSync(argvOut, JSON.stringify(process.argv.slice(2)), 'utf8');
}

const bridgeCommand = argValue('--bridge-cmd');
let bridgeArgs = [];
try {
  const raw = argValue('--bridge-args');
  if (raw) {
    bridgeArgs = JSON.parse(raw);
  }
} catch {
  bridgeArgs = [];
}

function send(obj) {
  process.stdout.write(`${JSON.stringify(obj)}\n`);
}

/** Call a tool on the orca bridge MCP server (spawns it as a child, MCP-over-stdio). */
async function callBridgeTool(name, args) {
  if (!bridgeCommand) {
    throw new Error('no bridge server configured');
  }
  const transport = new StdioClientTransport({
    command: bridgeCommand,
    args: bridgeArgs,
    env: { ...process.env },
  });
  const client = new Client({ name: 'fake-custom-json', version: '1.0.0' });
  await client.connect(transport);
  try {
    return await client.callTool({ name, arguments: args });
  } finally {
    await client.close();
  }
}

let reqSeq = 0;
let callSeq = 0;
// Pending approval requests, keyed by request id — resolved by stdin approval_response frames.
const pendingApprovals = new Map();

function requestApproval(toolName, args) {
  const requestId = `req_${++reqSeq}`;
  return new Promise((resolve) => {
    pendingApprovals.set(requestId, resolve);
    send({ type: 'approval', request_id: requestId, tool: toolName, args });
  });
}

async function runTurn(prompt) {
  const [verb, ...rest] = prompt.split(' ');

  if (verb === 'run') {
    const command = rest.join(' ');
    const id = `call_${++callSeq}`;
    send({ type: 'tool_call', tool: 'bash', args: { command }, id });
    let text = '';
    let isError = false;
    try {
      const res = await callBridgeTool('bash', { command });
      text = res?.content?.[0]?.text ?? '';
    } catch (err) {
      text = `bridge call failed: ${String(err)}`;
      isError = true;
    }
    send({ type: 'tool_result', id, output: text, is_error: isError });
    send({ type: 'usage', in: 7, out: 3 });
    send({ type: 'done' });
    return;
  }

  if (verb === 'exec') {
    const command = rest.join(' ');
    const id = `call_${++callSeq}`;
    const decision = await requestApproval('bash', { command });
    const approved = decision === 'allow' || decision === 'approved';
    if (approved) {
      send({ type: 'tool_call', tool: 'bash', args: { command }, id });
      let text = '';
      let isError = false;
      try {
        const res = await callBridgeTool('bash', { command });
        text = res?.content?.[0]?.text ?? '';
      } catch (err) {
        text = `bridge call failed: ${String(err)}`;
        isError = true;
      }
      send({ type: 'tool_result', id, output: text, is_error: isError });
    } else {
      send({ type: 'tool_result', id, output: 'blocked by approval policy', is_error: true });
    }
    send({ type: 'usage', in: 7, out: 3 });
    send({ type: 'done' });
    return;
  }

  // Default ("say" or anything else): one assistant text line + usage + done.
  send({ type: 'assistant', text: rest.join(' ') || verb });
  send({ type: 'usage', in: 7, out: 3 });
  send({ type: 'done' });
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
  // An approval_response answers a pending approval request.
  if (msg?.type === 'approval_response' && msg?.request_id !== undefined) {
    const resolver = pendingApprovals.get(msg.request_id);
    if (resolver) {
      pendingApprovals.delete(msg.request_id);
      resolver(msg.decision);
    }
    return;
  }
  // A prompt frame drives one turn.
  if (typeof msg?.prompt === 'string') {
    const prompt = msg.prompt.trim();
    chain = chain.then(() => runTurn(prompt));
  }
});

rl.on('close', () => {
  process.exit(0);
});
