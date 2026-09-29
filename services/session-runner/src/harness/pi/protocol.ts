// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `pi` RPC protocol normalizer — one stdout line → a matched command response, Orca-native
// events, a tool-approval, a turn end, or ignore.
//
// Pi's headless `--mode rpc` speaks newline-delimited JSON over stdio: the harness (the CLIENT)
// writes command objects to stdin and reads pi's responses + streamed session events off stdout,
// one JSON object per line. Two concerns live here, kept separate:
//
//   1. FRAMING. A line with `type: "response"` is the RESPONSE to a command the harness sent —
//      routed to its pending caller by `id` (see {@link PiProtocol.map}). Every other typed line
//      is a streamed `AgentSessionEvent` or an extension-UI request.
//   2. EVENT NORMALIZATION. Pi's session events map to Orca-native Anthropic-shaped
//      {@link AgentEvent}s — the SAME shapes every other Orca harness emits (NO pi dialect):
//        · `message_end` (assistant)  → an `agent.message` (its `text` content blocks) PLUS a
//          paired `agent.tool_use` for each `toolCall` content block (keyed by the block id),
//          PLUS an `agent.usage` when the assistant message carried a `usage` breakdown — the
//          same internal token-accounting event the in-process claude + codex providers emit
//          (pi reports usage PER assistant message, so each `message_end`'s usage is a per-message
//          DELTA the runner's usage sink sums into the session total, exactly like codex's per-turn
//          delta; the runner records it and does NOT persist it as a public transcript event);
//        · `tool_execution_end`       → an `agent.tool_result` paired back by the tool-call id
//          (error iff pi flagged the execution as an error — including a tool BLOCKED pre-exec by
//          the orca extension's `tool_call` hook, which pi surfaces as an error tool_result);
//        · `agent_end`                → a `turn_end` (the terminal turn boundary);
//        · `tool_execution_start` / `message_update` / `agent_start` / streaming deltas / unknown →
//          `ignore` (the completed `message_end` carries the whole assistant text, and the terminal
//          `agent_end` closes the turn). `tool_execution_start` is now purely informational — the
//          approval GATE is the extension-UI request below, NOT this after-the-fact start event.
//   3. APPROVAL FRAMING. Pi's RPC mode has NO per-tool server→client approval request. The correct
//      pre-exec gate is the orca extension's `tool_call` hook, which asks the client to approve via
//      `ctx.ui.select(...)`; pi serializes that as an `extension_ui_request` on stdout and blocks
//      the tool until the client replies with an `extension_ui_response` on stdin. This normalizer
//      maps `extension_ui_request` → a {@link PiUiRequest} the harness routes to the uniform
//      transcript approval; the harness writes the reply, and the hook returns `{block}` accordingly
//      — a genuine PRE-EXECUTION, PER-TOOL block (not a whole-turn abort). The orca extension encodes
//      the tool identity into the request `title` (a tagged marker) so the harness recovers the tool
//      name + call id for the approval signal + the per-tool policy lookup.
//
// The normalizer is a small stateful object (it tracks the tool-call ids it has surfaced so a
// `message_end` that repeats a toolCall block does not double-emit its `tool_use`). It performs
// no I/O.

import type { AgentEvent } from '../agent-harness.js';

/**
 * The internal token-usage shape forwarded on `agent.usage` (mirrors the claude + codex mappers
 * exactly). Internal accounting only — the harness-server's event pump records it into the session
 * usage sink and does NOT append it to the public transcript.
 */
interface AgentUsage {
  cache_creation: { ephemeral_1h_input_tokens: number; ephemeral_5m_input_tokens: number };
  cache_read_input_tokens: number;
  input_tokens: number;
  output_tokens: number;
}

/**
 * The tagged prefix the orca extension stamps on a `ctx.ui.select` approval title so the harness
 * can recover the tool identity from the otherwise free-form request. Fields are TAB-separated:
 * `ORCA_TOOL_APPROVAL\t<toolCallId>\t<toolName>`. Owned end-to-end by our code (the extension emits
 * it; the harness parses it) — pi's `ctx.ui.select(title, options)` forwards no structured metadata,
 * so the title is the carrier. A request whose title lacks this prefix is a NON-approval UI request.
 */
export const ORCA_TOOL_APPROVAL_TITLE_PREFIX = 'ORCA_TOOL_APPROVAL';

/** The two options the orca approval `select` offers; the reply is the chosen option STRING. */
export const ORCA_APPROVAL_ALLOW = 'Allow';
export const ORCA_APPROVAL_BLOCK = 'Block';

/**
 * A pi extension-UI approval request the harness must route to the uniform transcript approval
 * before pi lets the tool proceed. Surfaced from an `extension_ui_request` whose title carries the
 * {@link ORCA_TOOL_APPROVAL_TITLE_PREFIX}. The harness answers with an `extension_ui_response`
 * carrying the chosen option (`Allow`/`Block`), keyed by {@link requestId}.
 */
export interface PiUiRequest {
  /** The extension-UI request id — the routing key of the `extension_ui_response` reply. */
  requestId: string;
  /** The pi tool-call id parsed from the tagged title — the approval signal's routing key. */
  toolCallId: string;
  /** The tool name parsed from the tagged title (a pi built-in / orca tool, e.g. `bash`). */
  toolName: string;
}

/**
 * The outcome of normalizing one stdout line — a discriminated union the harness dispatches on:
 *   - `response`: a command response (routed by `id`);
 *   - `events`: a session event that produced zero-or-more agent events (emit them);
 *   - `ui_request`: an `extension_ui_request` approval to route to the gate;
 *   - `turn_end`: an `agent_end` — the turn boundary;
 *   - `ignore`: housekeeping / unknown (drop).
 */
export type NormalizedLine =
  | {
      kind: 'response';
      id: string | undefined;
      command: string;
      success: boolean;
      data?: unknown;
      error?: string;
    }
  | { kind: 'events'; events: AgentEvent[] }
  | { kind: 'ui_request'; request: PiUiRequest }
  | { kind: 'turn_end' }
  | { kind: 'ignore' };

/**
 * Normalizes pi RPC stdout lines to {@link NormalizedLine}s. One per session (it tracks the set
 * of tool-call ids whose `tool_use` it has already emitted, so a `message_end` echoing the same
 * toolCall block does not double-emit).
 */
export class PiProtocol {
  /** Tool-call ids whose `agent.tool_use` was already emitted (dedup across message_end echoes). */
  private readonly emittedToolUses = new Set<string>();

  /** Normalize one parsed stdout line. */
  map(line: unknown): NormalizedLine {
    if (!isRecord(line)) {
      return { kind: 'ignore' };
    }
    const type = typeof line['type'] === 'string' ? line['type'] : undefined;
    if (type === undefined) {
      return { kind: 'ignore' };
    }

    // An extension-UI request (the orca approval gate). Route it to the harness when its title
    // carries the tagged approval prefix; a non-approval UI request is dropped.
    if (type === 'extension_ui_request') {
      const request = readUiApproval(line);
      return request !== undefined ? { kind: 'ui_request', request } : { kind: 'ignore' };
    }

    // A command response (framing) — routed by id.
    if (type === 'response') {
      const command = typeof line['command'] === 'string' ? line['command'] : '';
      const out: NormalizedLine = {
        kind: 'response',
        id: typeof line['id'] === 'string' ? line['id'] : undefined,
        command,
        success: line['success'] === true,
      };
      if ('data' in line) {
        out.data = line['data'];
      }
      if (typeof line['error'] === 'string') {
        out.error = line['error'];
      }
      return out;
    }

    return this.mapEvent(type, line);
  }

  /** Map one pi session event (`message_end` / `tool_execution_*` / `agent_end` / …). */
  private mapEvent(type: string, event: Record<string, unknown>): NormalizedLine {
    switch (type) {
      case 'message_end': {
        const events = this.mapMessageEnd(event);
        return events.length > 0 ? { kind: 'events', events } : { kind: 'ignore' };
      }
      case 'tool_execution_end': {
        const events = mapToolExecutionEnd(event);
        return events.length > 0 ? { kind: 'events', events } : { kind: 'ignore' };
      }
      case 'agent_end':
        // The terminal turn boundary — pi finished streaming this turn's events.
        return { kind: 'turn_end' };
      default:
        // tool_execution_start (informational — the gate is the extension_ui_request) / agent_start
        // / turn_start / message_start / message_update (deltas) / non-approval extension UI /
        // everything else is housekeeping (the completed message_end + agent_end carry the turn).
        return { kind: 'ignore' };
    }
  }

  /**
   * Map an assistant `message_end` to its agent events: an `agent.message` carrying the message's
   * `text` content blocks, plus a paired `agent.tool_use` for each `toolCall` block (deduped by
   * the block id so a repeated block does not re-emit), plus an `agent.usage` when the message
   * carried a `usage` breakdown (the internal token-accounting event; pi reports usage per
   * assistant message, so each is a per-message delta the runner's usage sink sums). A
   * non-assistant message (a user echo) maps to nothing.
   */
  private mapMessageEnd(event: Record<string, unknown>): AgentEvent[] {
    const message = event['message'];
    if (!isRecord(message) || message['role'] !== 'assistant') {
      return [];
    }
    const content = Array.isArray(message['content']) ? message['content'] : [];
    const out: AgentEvent[] = [];

    const textBlocks: Array<{ type: 'text'; text: string }> = [];
    for (const block of content) {
      if (isRecord(block) && block['type'] === 'text' && typeof block['text'] === 'string') {
        textBlocks.push({ type: 'text', text: block['text'] });
      }
    }
    if (textBlocks.length > 0) {
      out.push({ kind: 'agent.message', payload: { content: textBlocks } });
    }

    for (const block of content) {
      if (!isRecord(block) || block['type'] !== 'toolCall') {
        continue;
      }
      const id = toolCallIdOf(block);
      if (id === undefined || this.emittedToolUses.has(id)) {
        continue;
      }
      this.emittedToolUses.add(id);
      const name = typeof block['name'] === 'string' ? block['name'] : '';
      const input = isRecord(block['arguments'])
        ? block['arguments']
        : isRecord(block['input'])
          ? block['input']
          : {};
      out.push({
        kind: 'agent.tool_use',
        id,
        payload: { name, input, tool_use_id: id },
      });
    }

    // Internal token accounting: pi carries the just-finished assistant message's token usage on
    // `message.usage`, so surface it as an `agent.usage` — the SAME internal event the in-process
    // claude + codex providers emit. The runner's usage sink records each as a delta (pi reports
    // per message, not cumulatively), so per-`message_end` emission sums to the turn + session
    // total. Absent / malformed usage → no event.
    const usage = usageFromPiMessage(message['usage']);
    if (usage !== null) {
      out.push({ kind: 'agent.usage', payload: { usage } });
    }
    return out;
  }
}

// ── module-private helpers ──────────────────────────────────────────────────────

/**
 * Read an `extension_ui_request` into a {@link PiUiRequest} when it is an orca approval request
 * (its `title` carries the {@link ORCA_TOOL_APPROVAL_TITLE_PREFIX} tagged marker), or `undefined`
 * for any other UI request. The marker is TAB-separated `PREFIX\t<toolCallId>\t<toolName>`.
 */
function readUiApproval(line: Record<string, unknown>): PiUiRequest | undefined {
  const requestId = typeof line['id'] === 'string' ? line['id'] : '';
  if (requestId.length === 0) {
    return undefined;
  }
  const title = typeof line['title'] === 'string' ? line['title'] : '';
  const parts = title.split('\t');
  if (parts[0] !== ORCA_TOOL_APPROVAL_TITLE_PREFIX) {
    return undefined; // a non-approval extension UI request — not ours to gate.
  }
  const toolCallId = parts[1] ?? '';
  const toolName = parts[2] ?? '';
  if (toolCallId.length === 0) {
    return undefined;
  }
  return { requestId, toolCallId, toolName };
}

/**
 * Map a `tool_execution_end` event to a paired `agent.tool_result`. Pi's `result` is an
 * `AgentToolResult` (`{content:[{type:"text",text}|…], details}`); the result is an error iff pi
 * flagged `isError`.
 */
function mapToolExecutionEnd(event: Record<string, unknown>): AgentEvent[] {
  const toolCallId = typeof event['toolCallId'] === 'string' ? event['toolCallId'] : '';
  if (toolCallId.length === 0) {
    return [];
  }
  const content = resultContentOf(event['result']);
  return [
    {
      kind: 'agent.tool_result',
      id: toolCallId,
      payload: { tool_use_id: toolCallId, content, is_error: event['isError'] === true },
    },
  ];
}

/** Extract the content payload from a pi tool result (its `content` array, or the raw value). */
function resultContentOf(result: unknown): unknown {
  if (isRecord(result) && Array.isArray(result['content'])) {
    return result['content'];
  }
  return result ?? '';
}

/**
 * Map a pi assistant message's `usage` to the internal {@link AgentUsage} accounting shape, or
 * `null` when absent/malformed. Pi's usage uses camelCase counters
 * (`{ input, output, cacheRead, cacheWrite }`); this projects them onto the SAME internal shape
 * the claude + codex mappers produce. Pi reports a FLAT cache-creation total (`cacheWrite`) with
 * no per-TTL breakdown, so — exactly like the codex mapper — it lands in the 5m bucket (1h = 0).
 */
function usageFromPiMessage(usage: unknown): AgentUsage | null {
  if (!isRecord(usage)) {
    return null;
  }
  return {
    cache_creation: {
      ephemeral_1h_input_tokens: 0,
      ephemeral_5m_input_tokens: numberField(usage['cacheWrite']),
    },
    cache_read_input_tokens: numberField(usage['cacheRead']),
    input_tokens: numberField(usage['input']),
    output_tokens: numberField(usage['output']),
  };
}

/** Coerce an untrusted usage counter to a non-negative integer (mirrors the claude/codex mappers). */
function numberField(value: unknown): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    return 0;
  }
  return Math.floor(value);
}

/** The tool-call id of a pi `toolCall` content block (accepts `id` or `toolCallId`). */
function toolCallIdOf(block: Record<string, unknown>): string | undefined {
  if (typeof block['id'] === 'string' && block['id'].length > 0) {
    return block['id'];
  }
  if (typeof block['toolCallId'] === 'string' && block['toolCallId'].length > 0) {
    return block['toolCallId'];
  }
  return undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
