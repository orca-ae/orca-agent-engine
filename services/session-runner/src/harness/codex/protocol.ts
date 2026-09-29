// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `codex` app-server protocol normalizer — one stdout line → a matched JSON-RPC
// response, Orca-native events, an approval request, or ignore.
//
// Codex's `app-server` speaks JSON-RPC over stdio: the harness (the CLIENT) writes
// request/response objects to stdin and reads the app-server's responses + notifications +
// server→client approval requests off stdout, one JSON object per line. Two concerns live
// here, kept separate:
//
//   1. JSON-RPC FRAMING. A line with an `id` and a `result`/`error` but NO `method` is the
//      RESPONSE to an earlier request — routed to its pending caller by `id` (see
//      {@link CodexProtocol.expectResponse}). A line WITH a `method` is either a
//      notification (an event) or, when it also carries an `id`, a server→client REQUEST
//      (an `on-request` approval the client must answer).
//   2. EVENT NORMALIZATION. Codex `item/*` + `turn/*` notifications map to Orca-native
//      Anthropic-shaped {@link AgentEvent}s — the SAME shapes every other Orca harness
//      emits:
//        · `item/completed` agentMessage      → `agent.message` (a `text` content block);
//        · `item/completed` commandExecution  → a paired `agent.tool_use` + `agent.tool_result`
//          for codex's built-in shell (keyed by the item id; error iff non-zero exit);
//        · `item/completed` mcpToolCall        → a paired `agent.tool_use` + `agent.tool_result`
//          under the `mcp__<server>__<tool>` identity;
//        · `item/completed` fileChange/patch  → a paired tool_use + tool_result;
//        · `thread/tokenUsage/updated`         → buffered as the pending turn usage;
//        · `turn/completed`                    → a `turn_end` carrying the buffered usage;
//        · `turn/failed` / `error`             → a `turn_end` (the harness settles the turn);
//        · a server→client approval request    → an `approval_request` (routed to the gate);
//        · streaming deltas / unknown methods  → `ignore` (the completed item carries text).
//
// The normalizer is a small stateful object (it buffers the latest token usage across the
// notification that carries it and the `turn/completed` that consumes it, and tracks the
// pending request ids for response routing). It performs no I/O.

import type { AgentEvent } from '../agent-harness.js';

/** The internal token-usage shape forwarded on `agent.usage` (mirrors the claude mapper). */
interface AgentUsage {
  cache_creation: { ephemeral_1h_input_tokens: number; ephemeral_5m_input_tokens: number };
  cache_read_input_tokens: number;
  input_tokens: number;
  output_tokens: number;
}

/**
 * An `on-request` approval the codex app-server raised as a server→client REQUEST. The
 * harness answers it with a JSON-RPC RESULT ({@link CodexProtocol.approvalResult}) keyed by
 * {@link requestId}, after routing it through the uniform transcript approval gate.
 */
export interface CodexApprovalRequest {
  /** The JSON-RPC request id — the routing key of the reply the harness sends back. */
  requestId: number | string;
  /** The codex approval method (decides the reply's decision vocabulary). */
  method: string;
  /** A displayable command string, when the request named one (shell / patch approvals). */
  command?: string;
  /** The codex `callId` the approval is scoped to, when present. */
  callId?: string;
  /** The tool name to surface on the approval signal (a codex built-in, e.g. `bash`). */
  toolName: string;
  /** The proposed tool input (the command / patch), for the gate + the signal. */
  input: Record<string, unknown>;
}

/**
 * The outcome of normalizing one stdout line — a discriminated union the harness dispatches
 * on:
 *   - `response`: a JSON-RPC response to an earlier request (routed by `id`);
 *   - `events`: a notification that produced zero-or-more agent events (emit them);
 *   - `turn_end`: a `turn/completed` / `turn/failed` / `error` — carries any usage event AND
 *     ends the turn;
 *   - `approval_request`: a server→client `on-request` approval to route to the gate;
 *   - `ignore`: housekeeping / unknown (drop).
 */
export type NormalizedLine =
  | { kind: 'response'; id: number | string; result?: unknown; error?: unknown }
  | { kind: 'events'; events: AgentEvent[] }
  | { kind: 'turn_end'; events: AgentEvent[] }
  | { kind: 'approval_request'; request: CodexApprovalRequest }
  | { kind: 'ignore' };

/** An allow/deny decision the harness resolved for an approval request. */
export type CodexApprovalDecision = 'allow' | 'deny';

/** The reserved MCP server name the native-CLI tool-bridge is registered under. */
const ORCA_MCP_SERVER_NAME = 'orca';

/** The codex server→client approval request methods the harness routes to the gate. */
const CODEX_APPROVAL_METHODS = new Set<string>([
  'execCommandApproval',
  'applyPatchApproval',
  'item/commandExecution/requestApproval',
  'item/fileChange/requestApproval',
]);

/**
 * Normalizes codex app-server stdout lines to {@link NormalizedLine}s and builds the
 * JSON-RPC replies for `on-request` approvals. One per session (it buffers the per-turn
 * token usage and the set of pending request ids).
 */
export class CodexProtocol {
  /** Request ids the harness has sent and is awaiting a response for (framing disambiguation). */
  private readonly pendingRequestIds = new Set<number | string>();
  /** The most recent token usage, buffered until the next `turn/completed` consumes it. */
  private bufferedUsage: AgentUsage | null = null;

  /** Register a request id the harness just sent, so its response line routes back by id. */
  expectResponse(id: number | string): void {
    this.pendingRequestIds.add(id);
  }

  /** Normalize one parsed stdout line. */
  map(line: unknown): NormalizedLine {
    if (line === null || typeof line !== 'object') {
      return { kind: 'ignore' };
    }
    const obj = line as { id?: unknown; method?: unknown; result?: unknown; error?: unknown };
    const method = typeof obj.method === 'string' ? obj.method : undefined;
    const hasId =
      obj.id !== undefined && (typeof obj.id === 'number' || typeof obj.id === 'string');

    // A frame with an id + NO method is the response to an earlier request.
    if (method === undefined && hasId && ('result' in obj || 'error' in obj)) {
      const id = obj.id as number | string;
      this.pendingRequestIds.delete(id);
      const out: NormalizedLine = { kind: 'response', id };
      if ('result' in obj) {
        out.result = obj.result;
      }
      if ('error' in obj && obj.error !== undefined && obj.error !== null) {
        out.error = obj.error;
      }
      return out;
    }

    // A frame with a method + an id is a server→client request (an approval to answer).
    if (method !== undefined && hasId && CODEX_APPROVAL_METHODS.has(method)) {
      const request = readApprovalRequest(
        obj.id as number | string,
        method,
        (obj as { params?: unknown }).params,
      );
      return { kind: 'approval_request', request };
    }

    // Otherwise it is a notification (an event).
    if (method !== undefined) {
      return this.mapNotification(method, paramsOf((obj as { params?: unknown }).params));
    }
    return { kind: 'ignore' };
  }

  /** Map one codex notification (`item/*` / `turn/*` / usage) to a {@link NormalizedLine}. */
  private mapNotification(method: string, params: Record<string, unknown>): NormalizedLine {
    switch (method) {
      case 'item/completed': {
        const events = mapCompletedItem(params);
        return events.length > 0 ? { kind: 'events', events } : { kind: 'ignore' };
      }
      case 'thread/tokenUsage/updated': {
        this.bufferedUsage = usageFromTokenUpdate(params) ?? this.bufferedUsage;
        return { kind: 'ignore' };
      }
      case 'turn/completed': {
        const events: AgentEvent[] = [];
        if (this.bufferedUsage !== null) {
          events.push({ kind: 'agent.usage', payload: { usage: this.bufferedUsage } });
          this.bufferedUsage = null;
        }
        return { kind: 'turn_end', events };
      }
      case 'turn/failed':
      case 'error': {
        // A turn-level failure ends the turn; the harness settles `submit`. The failure text
        // is surfaced as an assistant message so a client sees WHY the turn ended.
        const events: AgentEvent[] = [];
        const text = failureTextOf(params);
        if (text !== undefined) {
          events.push({ kind: 'agent.message', payload: { content: [{ type: 'text', text }] } });
        }
        return { kind: 'turn_end', events };
      }
      default:
        // Streaming deltas + everything else are housekeeping (the completed item carries
        // the whole text), so they drop.
        return { kind: 'ignore' };
    }
  }

  /**
   * Build the JSON-RPC RESULT payload for an approval request, translating the harness's
   * allow/deny into the codex decision vocabulary the request's method expects:
   *   - the legacy `execCommandApproval` / `applyPatchApproval` want `approved` / `abort`;
   *   - the current `item/*` approvals want `accept` / `decline`.
   */
  approvalResult(method: string, decision: CodexApprovalDecision): Record<string, unknown> {
    const legacy = method === 'execCommandApproval' || method === 'applyPatchApproval';
    if (legacy) {
      return { decision: decision === 'allow' ? 'approved' : 'abort' };
    }
    return { decision: decision === 'allow' ? 'accept' : 'decline' };
  }
}

// ── module-private helpers ──────────────────────────────────────────────────────

/** The `params` object of a notification, or `{}` when absent/malformed. */
function paramsOf(params: unknown): Record<string, unknown> {
  return isRecord(params) ? params : {};
}

/**
 * Map one `item/completed` item to the agent events it produces. Dispatches on the item
 * `type`: an `agentMessage` becomes a `text` `agent.message`; a `commandExecution` (codex's
 * built-in shell), an `mcpToolCall`, or a `fileChange`/`patchApply` become a paired
 * `agent.tool_use` + `agent.tool_result`.
 */
function mapCompletedItem(params: Record<string, unknown>): AgentEvent[] {
  const item = params['item'];
  if (!isRecord(item)) {
    return [];
  }
  const type = typeof item['type'] === 'string' ? item['type'] : '';
  switch (type) {
    case 'agentMessage':
      return mapAgentMessage(item);
    case 'commandExecution':
      return mapCommandExecution(item);
    case 'mcpToolCall':
      return mapMcpToolCall(item);
    case 'fileChange':
    case 'patchApply':
      return mapFileChange(item);
    default:
      return [];
  }
}

/** `agentMessage` → a single `text` `agent.message` (the settled assistant text). */
function mapAgentMessage(item: Record<string, unknown>): AgentEvent[] {
  const text = typeof item['text'] === 'string' ? item['text'] : '';
  if (text.length === 0) {
    return [];
  }
  return [{ kind: 'agent.message', payload: { content: [{ type: 'text', text }] } }];
}

/**
 * `commandExecution` (codex's built-in shell) → a paired `agent.tool_use` + `agent.tool_result`.
 * The call is surfaced under the bare codex built-in name (`bash`), the input carries the
 * command, and the result pairs back by the item id and is an error iff the exit was non-zero.
 */
function mapCommandExecution(item: Record<string, unknown>): AgentEvent[] {
  const id = typeof item['id'] === 'string' ? item['id'] : '';
  if (id.length === 0) {
    return [];
  }
  const command = typeof item['command'] === 'string' ? item['command'] : '';
  const output = typeof item['aggregatedOutput'] === 'string' ? item['aggregatedOutput'] : '';
  const exitCode = typeof item['exitCode'] === 'number' ? item['exitCode'] : 0;
  return pairedToolEvents({
    id,
    name: 'bash',
    input: { command },
    content: output,
    isError: exitCode !== 0 || item['status'] === 'failed',
  });
}

/**
 * `mcpToolCall` → a paired tool_use + tool_result under the `mcp__<server>__<tool>` identity
 * (the same convention the rest of Orca uses). The result is an error iff the call's status
 * is a failure.
 */
function mapMcpToolCall(item: Record<string, unknown>): AgentEvent[] {
  const id = typeof item['id'] === 'string' ? item['id'] : '';
  if (id.length === 0) {
    return [];
  }
  const server = typeof item['server'] === 'string' ? item['server'] : ORCA_MCP_SERVER_NAME;
  const tool = typeof item['tool'] === 'string' ? item['tool'] : '';
  const name = `mcp__${server}__${tool}`;
  const input = isRecord(item['arguments']) ? item['arguments'] : {};
  return pairedToolEvents({
    id,
    name,
    input,
    content: item['result'],
    isError: item['status'] === 'failed' || item['status'] === 'error',
  });
}

/** `fileChange`/`patchApply` → a paired tool_use + tool_result under the `apply_patch` name. */
function mapFileChange(item: Record<string, unknown>): AgentEvent[] {
  const id = typeof item['id'] === 'string' ? item['id'] : '';
  if (id.length === 0) {
    return [];
  }
  const input = isRecord(item['changes']) ? { changes: item['changes'] } : {};
  return pairedToolEvents({
    id,
    name: 'apply_patch',
    input,
    content: typeof item['status'] === 'string' ? item['status'] : 'applied',
    isError: item['status'] === 'failed',
  });
}

/** Build the paired `agent.tool_use` + `agent.tool_result` events for a completed tool item. */
function pairedToolEvents(args: {
  id: string;
  name: string;
  input: Record<string, unknown>;
  content: unknown;
  isError: boolean;
}): AgentEvent[] {
  return [
    {
      kind: 'agent.tool_use',
      id: args.id,
      payload: { name: args.name, input: args.input, tool_use_id: args.id },
    },
    {
      kind: 'agent.tool_result',
      id: args.id,
      payload: { tool_use_id: args.id, content: args.content, is_error: args.isError },
    },
  ];
}

/**
 * Read a server→client approval request into a {@link CodexApprovalRequest}. Extracts a
 * displayable command (a string or a joined argv array), the `callId`, and a proposed input
 * for the gate + signal. Defensive about the payload shape (the app-server's stdout is
 * treated as untrusted bytes).
 */
function readApprovalRequest(
  requestId: number | string,
  method: string,
  rawParams: unknown,
): CodexApprovalRequest {
  const params = isRecord(rawParams) ? rawParams : {};
  const command = commandPreview(params['command']);
  const isPatch = method.includes('fileChange') || method === 'applyPatchApproval';
  const toolName = isPatch ? 'apply_patch' : 'bash';
  const input: Record<string, unknown> = {};
  if (command !== undefined) {
    input['command'] = command;
  }
  if (isRecord(params['changes'])) {
    input['changes'] = params['changes'];
  }
  const request: CodexApprovalRequest = { requestId, method, toolName, input };
  if (command !== undefined) {
    request.command = command;
  }
  if (typeof params['callId'] === 'string' && params['callId'].length > 0) {
    request.callId = params['callId'];
  }
  return request;
}

/** Extract a displayable command string from a codex approval param (string or argv array). */
function commandPreview(command: unknown): string | undefined {
  if (typeof command === 'string' && command.length > 0) {
    return command;
  }
  if (Array.isArray(command)) {
    const parts = command.filter((p): p is string => typeof p === 'string');
    if (parts.length > 0) {
      return parts.join(' ');
    }
  }
  return undefined;
}

/** The failure text of a `turn/failed` / `error` notification, or `undefined`. */
function failureTextOf(params: Record<string, unknown>): string | undefined {
  const message = params['message'];
  if (typeof message === 'string' && message.length > 0) {
    return message;
  }
  const turn = params['turn'];
  if (isRecord(turn) && typeof turn['error'] === 'string' && turn['error'].length > 0) {
    return turn['error'];
  }
  return undefined;
}

/**
 * Map a `thread/tokenUsage/updated` payload's `last` (the just-finished turn's breakdown) to
 * the internal usage accounting shape. Absent / malformed → `null`.
 */
function usageFromTokenUpdate(params: Record<string, unknown>): AgentUsage | null {
  const usage = params['usage'];
  if (!isRecord(usage)) {
    return null;
  }
  const last = isRecord(usage['last']) ? usage['last'] : usage;
  return {
    cache_creation: {
      ephemeral_1h_input_tokens: 0,
      ephemeral_5m_input_tokens: numberField(last['cache_creation_input_tokens']),
    },
    cache_read_input_tokens: numberField(
      last['cached_input_tokens'] ?? last['cache_read_input_tokens'],
    ),
    input_tokens: numberField(last['input_tokens']),
    output_tokens: numberField(last['output_tokens']),
  };
}

function numberField(value: unknown): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    return 0;
  }
  return Math.floor(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
