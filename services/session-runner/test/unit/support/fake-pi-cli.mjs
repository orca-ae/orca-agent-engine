#!/usr/bin/env node
// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// A FAKE `pi --mode rpc`-like CLI for the pi provider spec.
//
// It stands in for the REAL `pi` binary run headless (`pi --mode rpc`): a long-lived process
// that speaks pi's newline-delimited JSON command/event protocol over stdio. No real binary is
// needed to prove the provider — this script speaks the SAME JSON shapes the real RPC mode does
// and honors the SAME protocol the harness drives:
//
//   - it reads stdin as pi RPC COMMANDS (one object per line): a `{type:"prompt",message,id}`
//     drives one turn and gets an immediate `{id,type:"response",command:"prompt",success:true}`;
//     an `{type:"abort",id}` aborts the in-flight turn and replies with a response; a
//     `{type:"get_state",id}` replies with a minimal state; an `{type:"extension_ui_response",id,
//     value}` resolves a pending extension-UI request (the approval sub-protocol).
//   - it loads the orca tool-bridge EXTENSION passed via `--extension` (SINGULAR — the real pi
//     flag; the plural `--extensions` does not exist) using the REAL shipped module, exactly as pi
//     would: it invokes the extension factory with a minimal pi API, collecting the tools the
//     extension registers (each backed by the native-CLI bridge) AND the `tool_call` HOOK the
//     extension registers. The hook is invoked with a real `ctx` whose `ctx.hasUI` is true and whose
//     `ctx.ui.select` drives the REAL extension_ui_request/response sub-protocol over stdio — so the
//     spec proves the ACTUAL pre-exec approval mechanism (the extension's `tool_call` hook +
//     `ctx.ui.select` → the harness's `extension_ui_response` reply), not a fabricated abort window.
//   - it drives one "turn" per prompt, dispatching on the FIRST word of the message text:
//       · "say <rest>"  → emit an assistant `message_end` (text) then `agent_end`,
//       · "run <cmd>"   → invoke the extension's `bash` tool (calling the bridge), emit an
//         assistant `message_end` carrying a `toolCall` block + `tool_execution_end`, then
//         `agent_end`,
//       · "exec <cmd>"  → the SAME as run, but FIRST emits `tool_execution_start` and runs the
//         extension's `tool_call` HOOK (which asks the client via `ctx.ui.select`) BEFORE executing:
//         a `{block:true}` return blocks the tool PRE-EXECUTION (an error tool_result, the bridge is
//         never touched); otherwise the tool runs. This mirrors the real binary exactly.
//   - it publishes a terminal `agent_end` at the end of every turn — the harness's turn boundary.
//
// It ALSO writes each argv it was launched with to the path in `--argv-out` (when given).

import { writeFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';

function send(obj) {
  process.stdout.write(`${JSON.stringify(obj)}\n`);
}

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

const argvOut = argValue('--argv-out');
if (argvOut) {
  writeFileSync(argvOut, JSON.stringify(process.argv.slice(2)), 'utf8');
}

// ── the extension-UI sub-protocol: pending `ctx.ui.*` requests keyed by their id ──
// `ctx.ui.select` emits an `extension_ui_request` and parks until the harness writes the matching
// `extension_ui_response` back on stdin, exactly as the real binary does.
const pendingUi = new Map();

/** The `ctx` the extension's `tool_call` hook receives — a real UI channel over stdio. */
const ctx = {
  hasUI: true,
  ui: {
    select(title, options) {
      const id = randomUUID();
      send({ type: 'extension_ui_request', id, method: 'select', title, options });
      return new Promise((resolve) => {
        pendingUi.set(id, resolve);
      });
    },
    confirm(title, message) {
      const id = randomUUID();
      send({ type: 'extension_ui_request', id, method: 'confirm', title, message });
      return new Promise((resolve) => {
        pendingUi.set(id, (value) => resolve(value === true || value === 'true'));
      });
    },
  },
};

// Load the orca bridge extension the harness wired via `--extension`, exactly as pi would:
// call the factory with a minimal pi API and collect the tools + the tool_call hook it registers.
const registeredTools = new Map();
const toolCallHooks = [];
const extensionPath = argValue('--extension');
const piApi = {
  registerTool(tool) {
    registeredTools.set(tool.name, tool);
  },
  registerCommand() {},
  registerShortcut() {},
  registerFlag() {},
  getFlag() {
    return undefined;
  },
  registerMessageRenderer() {},
  on(eventName, handler) {
    if (eventName === 'tool_call' && typeof handler === 'function') {
      toolCallHooks.push(handler);
    }
  },
};

async function loadExtension() {
  if (!extensionPath) {
    return;
  }
  const mod = await import(pathToFileURL(extensionPath).href);
  const factory = mod.default ?? mod;
  if (typeof factory === 'function') {
    await factory(piApi);
  }
}

let itemSeq = 0;

/** Invoke a registered bridge tool by name; returns {output, isError}. */
async function callTool(name, args) {
  const tool = registeredTools.get(name);
  if (!tool) {
    return { output: `no such tool: ${name}`, isError: true };
  }
  try {
    const res = await tool.execute(`call_${++itemSeq}`, args, undefined, undefined, {});
    const first = Array.isArray(res?.content)
      ? res.content.find((c) => c?.type === 'text')
      : undefined;
    return { output: typeof first?.text === 'string' ? first.text : '', isError: false };
  } catch (err) {
    return { output: `tool call failed: ${String(err)}`, isError: true };
  }
}

/**
 * Run the extension's `tool_call` hooks BEFORE executing a tool (pi fires them after
 * `tool_execution_start`, before the tool runs; a hook may block). Returns the first
 * `{block:true,...}` a hook returns, or null when none blocks.
 */
async function runToolCallHooks(event) {
  for (const hook of toolCallHooks) {
    const verdict = await hook(event, ctx);
    if (verdict && verdict.block === true) {
      return verdict;
    }
  }
  return null;
}

/** Emit the assistant message + tool_result events for a completed tool call. */
function emitToolTurn(toolCallId, name, args, output, isError) {
  send({
    type: 'message_end',
    message: {
      role: 'assistant',
      content: [{ type: 'toolCall', id: toolCallId, name, arguments: args }],
    },
  });
  send({
    type: 'tool_execution_end',
    toolCallId,
    toolName: name,
    result: { content: [{ type: 'text', text: output }], details: {} },
    isError,
  });
}

/** Drive one turn for the given prompt text. */
async function runTurn(text) {
  const [verb, ...rest] = text.split(' ');

  if (verb === 'say') {
    send({ type: 'agent_start' });
    send({
      type: 'message_end',
      message: { role: 'assistant', content: [{ type: 'text', text: rest.join(' ') }] },
    });
    send({ type: 'agent_end', messages: [] });
    return;
  }

  if (verb === 'run') {
    const command = rest.join(' ');
    const toolCallId = `tc_${++itemSeq}`;
    send({ type: 'agent_start' });
    const { output, isError } = await callTool('bash', { command });
    emitToolTurn(toolCallId, 'bash', { command }, output, isError);
    send({ type: 'agent_end', messages: [] });
    return;
  }

  if (verb === 'exec') {
    const command = rest.join(' ');
    const toolCallId = `tc_${++itemSeq}`;
    send({ type: 'agent_start' });
    // Surface tool_execution_start (informational), then run the REAL `tool_call` hook — which asks
    // the client via `ctx.ui.select` and can BLOCK pre-execution — exactly as the real binary does.
    send({ type: 'tool_execution_start', toolCallId, toolName: 'bash', args: { command } });
    const blocked = await runToolCallHooks({ toolCallId, toolName: 'bash', input: { command } });
    if (blocked) {
      // Blocked before running: emit a failed tool_result and never touch the bridge.
      const reason =
        typeof blocked.reason === 'string' ? blocked.reason : 'blocked by approval policy';
      emitToolTurn(toolCallId, 'bash', { command }, reason, true);
      send({ type: 'agent_end', messages: [] });
      return;
    }
    const { output, isError } = await callTool('bash', { command });
    emitToolTurn(toolCallId, 'bash', { command }, output, isError);
    send({ type: 'agent_end', messages: [] });
    return;
  }

  // Unknown verb: just end the turn.
  send({ type: 'agent_start' });
  send({ type: 'agent_end', messages: [] });
}

// ── RPC framing: split stdin on `\n` ONLY (strip an optional trailing `\r`), never on U+2028/U+2029
// (the pi RPC contract — a generic line reader would corrupt frames carrying those code points). ──
let stdinBuf = '';
let ready = loadExtension();
let chain = Promise.resolve();

function handleLine(line) {
  const trimmed = line.replace(/\r$/, '').trim();
  if (trimmed.length === 0) {
    return;
  }
  let msg;
  try {
    msg = JSON.parse(trimmed);
  } catch {
    return;
  }
  const id = msg?.id;
  const type = msg?.type;

  if (type === 'prompt') {
    const text = typeof msg?.message === 'string' ? msg.message.trim() : '';
    send({ id, type: 'response', command: 'prompt', success: true });
    chain = chain.then(() => ready).then(() => runTurn(text));
    return;
  }
  if (type === 'extension_ui_response') {
    // Resolve the parked `ctx.ui.*` request (the approval verdict) — the option string, or the
    // confirm boolean, or undefined on `cancelled`.
    const resolve = id !== undefined ? pendingUi.get(id) : undefined;
    if (resolve) {
      pendingUi.delete(id);
      const value = msg?.cancelled === true ? undefined : (msg?.value ?? msg?.confirmed);
      resolve(value);
    }
    return;
  }
  if (type === 'abort') {
    send({ id, type: 'response', command: 'abort', success: true });
    return;
  }
  if (type === 'get_state') {
    send({
      id,
      type: 'response',
      command: 'get_state',
      success: true,
      data: { sessionId: 'fake', thinkingLevel: 'off', isStreaming: false },
    });
    return;
  }
  // Unknown command with an id: reply so the client is not stranded.
  if (id !== undefined && typeof type === 'string') {
    send({ id, type: 'response', command: type, success: true });
  }
}

process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => {
  stdinBuf += chunk;
  let nl;
  while ((nl = stdinBuf.indexOf('\n')) >= 0) {
    const line = stdinBuf.slice(0, nl);
    stdinBuf = stdinBuf.slice(nl + 1);
    handleLine(line);
  }
});
process.stdin.on('end', () => {
  if (stdinBuf.length > 0) {
    handleLine(stdinBuf);
    stdinBuf = '';
  }
  process.exit(0);
});
