// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `cursor` stream-json normalizer — one stdout line → Orca-native events + control
// frames.
//
// The headless `cursor-agent` binary emits one JSON object per stdout line ("stream-json").
// Two concerns live here, kept separate:
//
//   1. EVENT NORMALIZATION. Cursor emits its OWN event vocabulary; this normalizer maps each
//      to the SAME Anthropic-shaped {@link AgentEvent}s every other Orca harness emits, so a
//      turn driven by cursor streams identically to one driven by the in-process claude
//      provider:
//        · `system`/`init`               → a `system` event (carries MCP connection status);
//        · `assistant` w/ a `text` block → `agent.message` `{ content:[{type:'text',text}] }`;
//        · `assistant` w/ a `thinking`
//          block, or a bare `thinking`
//          event                          → `agent.message` `{ content:[{type:'thinking',…}] }`
//          (extended reasoning surfaced as the Anthropic-native `thinking` content block);
//        · `tool_call` (started)          → an `agent.tool_use` (keyed by the call id);
//        · `tool_call` (completed)        → a paired `agent.tool_use` + `agent.tool_result`
//          (the tool_use is idempotent — a completion that follows a `started` re-emits it so
//          a client that missed the start still pairs the result), an error result iff the
//          call's status is a failure. Cursor wraps a HOST custom tool under an `mcp`
//          envelope (`name:"mcp"`, `args:{providerIdentifier,toolName,args}`) — it is unwrapped
//          to the real `mcp__<server>__<tool>` identity so the observed events (and any
//          name-keyed policy / UI) see the actual tool, not `"mcp"`;
//        · `result`                       → a `turn_end` carrying any `agent.usage`.
//   2. CONTROL frames the headless stdio protocol adds (the harness drives the other side):
//        · a `control_request` of subtype `can_use_tool` — cursor asking whether a tool call
//          may proceed. Surfaced as a {@link PermissionRequest} carrying the `request_id` (the
//          reply's routing key), the tool name + input, and the `tool_use_id`, so the harness
//          routes it to the uniform transcript approval and answers over stdin;
//        · a `control_cancel_request` — cursor WITHDRAWING a permission it previously raised.
//          Surfaced as a {@link PermissionCancel} so the harness releases the matching parked
//          gate as a deny rather than leaving it awaiting a verdict cursor no longer wants.
//      Housekeeping frames (`control_response`, `keep_alive`, unknown/blank) are ignored.
//
// NOTE ON THE STREAM SHAPE. Cursor's headless `--output-format stream-json` event set is
// modeled from the observable cursor-agent stream (the same underlying event stream a Cursor
// agent surfaces: assistant text, thinking, tool-call started/completed with a per-call id and
// the `mcp` custom-tool envelope, and a terminal result). The permission control channel
// mirrors the newline-delimited request/withdraw handshake a headless coding CLI uses to gate
// a tool call over its stdio (the harness answers with a `control_response`) — kept structurally
// aligned with the sibling native-CLI providers so one uniform approval path serves them all.
// Every reader is DEFENSIVE about the payload (the CLI's stdout is treated as untrusted bytes):
// a missing field degrades to a benign default, and an unrecognized frame is dropped.
//
// The normalizer is a small stateful object (one per turn) — it tracks the tool_use ids it has
// already emitted so a completion following its own `started` re-emits the call idempotently
// without double-counting. It performs no I/O.

import type { AgentEvent } from '../agent-harness.js';

/** The internal token-usage shape forwarded on `agent.usage` (mirrors the claude mapper). */
interface AgentUsage {
  cache_creation: { ephemeral_1h_input_tokens: number; ephemeral_5m_input_tokens: number };
  cache_read_input_tokens: number;
  input_tokens: number;
  output_tokens: number;
}

/**
 * A tool-permission request cursor raised over the stream-json control channel (a
 * `can_use_tool` control_request). The harness answers it with a `control_response` written to
 * stdin, keyed by {@link requestId}.
 */
export interface PermissionRequest {
  /** The control request id — the routing key of the `control_response` reply. */
  requestId: string;
  /** The (MCP-qualified) tool name the model wants to call, e.g. `mcp__orca__bash`. */
  toolName: string;
  /** The tool input the model proposed (defaults to `{}` when the frame omits it). */
  input: Record<string, unknown>;
  /** The call's `tool_use_id` (the approval signal's routing key); `''` when absent. */
  toolUseId: string;
}

/**
 * A permission WITHDRAWAL cursor raised over the stream-json control channel (a
 * `control_cancel_request`). It retracts an earlier `can_use_tool` request keyed by
 * {@link requestId}, so the harness releases that parked gate as a deny.
 */
export interface PermissionCancel {
  /** The `request_id` of the `control_request` being withdrawn (the park's key). */
  requestId: string;
}

/**
 * The outcome of normalizing one stdout line — a discriminated union the harness dispatches
 * on:
 *   - `events`: a data frame that produced zero-or-more agent events (emit them);
 *   - `turn_end`: the `result` frame — carries any usage event AND ends the turn;
 *   - `permission_request`: a `can_use_tool` control_request to route to the gate;
 *   - `permission_cancel`: a `control_cancel_request` withdrawing an earlier request;
 *   - `ignore`: housekeeping / unknown (drop).
 */
export type NormalizedLine =
  | { kind: 'events'; events: AgentEvent[] }
  | { kind: 'turn_end'; events: AgentEvent[] }
  | { kind: 'permission_request'; request: PermissionRequest }
  | { kind: 'permission_cancel'; cancel: PermissionCancel }
  | { kind: 'ignore' };

/** The reserved MCP server name the native-CLI tool-bridge is registered under. */
const ORCA_MCP_SERVER_NAME = 'orca';

/** Maps cursor stream-json stdout lines to {@link NormalizedLine}s for one turn. */
export class CursorStreamNormalizer {
  /**
   * The tool_use ids this turn has already emitted an `agent.tool_use` for. A `tool_call`
   * completion that FOLLOWED its own `started` frame re-emits the tool_use (so a client that
   * missed the start still pairs the result) — the set is bookkeeping for the pairing model.
   */
  private readonly emittedToolUseIds = new Set<string>();

  /** Normalize one parsed stdout line. */
  map(line: unknown): NormalizedLine {
    if (line === null || typeof line !== 'object') {
      return { kind: 'ignore' };
    }
    const type = (line as { type?: unknown }).type;

    // Control frames the headless protocol adds.
    if (type === 'control_request') {
      const request = readPermissionRequest(line);
      return request !== undefined ? { kind: 'permission_request', request } : { kind: 'ignore' };
    }
    if (type === 'control_cancel_request') {
      const cancel = readPermissionCancel(line);
      return cancel !== undefined ? { kind: 'permission_cancel', cancel } : { kind: 'ignore' };
    }
    if (type === 'control_response' || type === 'keep_alive') {
      return { kind: 'ignore' };
    }

    // The result frame ends the turn (and may carry usage).
    if (type === 'result') {
      const events: AgentEvent[] = [];
      const usage = usageFromResult(line);
      if (usage !== null) {
        events.push({ kind: 'agent.usage', payload: { usage } });
      }
      return { kind: 'turn_end', events };
    }

    // Data frames.
    const events = this.mapDataFrame(type, line);
    return events.length > 0 ? { kind: 'events', events } : { kind: 'ignore' };
  }

  /** Map a non-control, non-result data frame to the agent events it produces. */
  private mapDataFrame(type: unknown, line: object): AgentEvent[] {
    switch (type) {
      case 'system':
        return mapSystem(line);
      case 'assistant':
        return mapAssistant(line);
      case 'thinking':
        return mapThinking(line);
      case 'tool_call':
        return this.mapToolCall(line);
      default:
        return [];
    }
  }

  /**
   * `tool_call` → an `agent.tool_use` (on `started`) and a paired `agent.tool_use` +
   * `agent.tool_result` (on `completed`). The `mcp` custom-tool envelope is unwrapped to the
   * real `mcp__<server>__<tool>` name. A completion re-emits the tool_use idempotently so a
   * client that missed the `started` still pairs the result.
   */
  private mapToolCall(line: object): AgentEvent[] {
    const obj = line as {
      subtype?: unknown;
      callId?: unknown;
      call_id?: unknown;
      status?: unknown;
      result?: unknown;
    };
    const callId =
      typeof obj.callId === 'string' && obj.callId.length > 0
        ? obj.callId
        : typeof obj.call_id === 'string'
          ? obj.call_id
          : '';
    if (callId.length === 0) {
      return []; // no id → cannot pair a use to a result; drop.
    }
    const { name, input } = readToolNameAndInput(line);
    const events: AgentEvent[] = [];
    // Emit the tool_use once per call id (idempotent across a started→completed pair).
    if (!this.emittedToolUseIds.has(callId)) {
      this.emittedToolUseIds.add(callId);
      events.push({
        kind: 'agent.tool_use',
        id: callId,
        payload: { name, input, tool_use_id: callId },
      });
    }
    // A completion carries the result; a started frame does not.
    const status = typeof obj.status === 'string' ? obj.status : '';
    const isCompletion = obj.subtype === 'completed' || status.length > 0;
    if (isCompletion) {
      const isError = status === 'error' || status === 'failed';
      events.push({
        kind: 'agent.tool_result',
        id: callId,
        payload: {
          tool_use_id: callId,
          content: toolResultContent(obj.result),
          is_error: isError,
        },
      });
    }
    return events;
  }
}

// ── module-private helpers ──────────────────────────────────────────────────────

/** `system`/`init` → a `system` event; other system subtypes drop. */
function mapSystem(line: object): AgentEvent[] {
  const candidate = line as { type?: string; subtype?: string };
  if (candidate.subtype === 'init') {
    return [{ kind: 'system', payload: line }];
  }
  return [];
}

/**
 * `assistant` → one event per content block: a `text` block → a text `agent.message`; a
 * `thinking` block → a thinking `agent.message` (with the signature when present). Cursor
 * carries the settled assistant content under `message.content` (the same envelope the claude
 * surface uses), so text and reasoning are surfaced faithfully.
 */
function mapAssistant(line: object): AgentEvent[] {
  const content = contentBlocksOf(line);
  if (content === undefined) {
    return [];
  }
  const events: AgentEvent[] = [];
  for (const raw of content) {
    if (raw === null || typeof raw !== 'object') {
      continue;
    }
    const block = raw as Record<string, unknown>;
    if (block['type'] === 'text' && typeof block['text'] === 'string' && block['text'].length > 0) {
      events.push({
        kind: 'agent.message',
        payload: { content: [{ type: 'text', text: block['text'] }] },
      });
    } else if (block['type'] === 'thinking' && typeof block['thinking'] === 'string') {
      const thinkingBlock: Record<string, unknown> = {
        type: 'thinking',
        thinking: block['thinking'],
      };
      if (typeof block['signature'] === 'string') {
        thinkingBlock['signature'] = block['signature'];
      }
      events.push({ kind: 'agent.message', payload: { content: [thinkingBlock] } });
    }
  }
  return events;
}

/**
 * A bare `thinking` event → a thinking `agent.message`. Cursor surfaces streamed reasoning
 * either inside an `assistant` message's content or as a standalone `{type:'thinking',text}`
 * frame; both land as the Anthropic-native `thinking` content block.
 */
function mapThinking(line: object): AgentEvent[] {
  const text = (line as { text?: unknown }).text;
  if (typeof text !== 'string' || text.length === 0) {
    return [];
  }
  return [{ kind: 'agent.message', payload: { content: [{ type: 'thinking', thinking: text }] } }];
}

/**
 * Read the (MCP-qualified) tool name + input from a `tool_call` frame, unwrapping cursor's
 * `mcp` custom-tool envelope. When `name` is `"mcp"`, the real tool lives under
 * `args.{providerIdentifier|server, toolName}` with the real input under `args.args`; otherwise
 * `name` is the tool and `args`/`input` is the input.
 */
function readToolNameAndInput(line: object): { name: string; input: Record<string, unknown> } {
  const obj = line as { name?: unknown; args?: unknown; input?: unknown };
  const rawName = typeof obj.name === 'string' ? obj.name : '';
  const rawArgs = isRecord(obj.args) ? obj.args : isRecord(obj.input) ? obj.input : {};
  if (rawName === 'mcp' && typeof rawArgs['toolName'] === 'string') {
    const server =
      typeof rawArgs['providerIdentifier'] === 'string'
        ? rawArgs['providerIdentifier']
        : typeof rawArgs['server'] === 'string'
          ? (rawArgs['server'] as string)
          : ORCA_MCP_SERVER_NAME;
    const inner = isRecord(rawArgs['args']) ? rawArgs['args'] : {};
    return { name: `mcp__${server}__${rawArgs['toolName']}`, input: inner };
  }
  return { name: rawName, input: rawArgs };
}

/**
 * The content payload of a completed `tool_call`'s result, normalized to what the tool result
 * carries. Cursor returns either an MCP result envelope (`{content:[...]}`) or a bare value;
 * the envelope's `content` array is forwarded when present, else the raw result value.
 */
function toolResultContent(result: unknown): unknown {
  if (isRecord(result) && Array.isArray(result['content'])) {
    return result['content'];
  }
  return result ?? '';
}

/**
 * Read a `can_use_tool` control_request into a {@link PermissionRequest}, or `undefined` for
 * any other control_request subtype (interrupt / …), which the harness ignores. Defensive: a
 * missing `input` defaults to `{}`, a missing `tool_use_id` to `''`, and a request with no
 * string `request_id` is dropped (it could not be answered anyway).
 */
function readPermissionRequest(line: unknown): PermissionRequest | undefined {
  const obj = line as { request_id?: unknown; request?: unknown };
  if (typeof obj.request_id !== 'string' || obj.request_id.length === 0) {
    return undefined;
  }
  if (obj.request === null || typeof obj.request !== 'object') {
    return undefined;
  }
  const request = obj.request as {
    subtype?: unknown;
    tool_name?: unknown;
    input?: unknown;
    tool_use_id?: unknown;
  };
  if (request.subtype !== 'can_use_tool') {
    return undefined;
  }
  return {
    requestId: obj.request_id,
    toolName: typeof request.tool_name === 'string' ? request.tool_name : '',
    input: isRecord(request.input) ? request.input : {},
    toolUseId: typeof request.tool_use_id === 'string' ? request.tool_use_id : '',
  };
}

/**
 * Read a `control_cancel_request` into a {@link PermissionCancel}, or `undefined` when it
 * carries no string `request_id` (nothing to release). The cancel's `request_id` names the
 * `control_request` being withdrawn — the same key the harness parked the permission under.
 */
function readPermissionCancel(line: unknown): PermissionCancel | undefined {
  const obj = line as { request_id?: unknown };
  if (typeof obj.request_id !== 'string' || obj.request_id.length === 0) {
    return undefined;
  }
  return { requestId: obj.request_id };
}

/** The `message.content` array of an assistant frame, or `undefined`. */
function contentBlocksOf(line: unknown): unknown[] | undefined {
  const inner = (line as { message?: unknown }).message;
  if (inner === null || typeof inner !== 'object') {
    return undefined;
  }
  const content = (inner as { content?: unknown }).content;
  return Array.isArray(content) ? content : undefined;
}

/** Map a cursor `result` frame's usage to the internal accounting shape; `null` when absent. */
function usageFromResult(line: unknown): AgentUsage | null {
  const usage = (line as { usage?: unknown }).usage;
  if (!isRecord(usage)) {
    return null;
  }
  return {
    cache_creation: {
      ephemeral_1h_input_tokens: 0,
      ephemeral_5m_input_tokens: numberField(usage['cache_creation_input_tokens']),
    },
    cache_read_input_tokens: numberField(usage['cache_read_input_tokens']),
    input_tokens: numberField(usage['input_tokens']),
    output_tokens: numberField(usage['output_tokens']),
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
