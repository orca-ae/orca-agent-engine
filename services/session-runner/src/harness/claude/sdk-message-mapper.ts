// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Translate Claude Agent SDK messages into Orca-native {@link AgentEvent}s.
//
// This is the streaming heart of the lean `claude` provider: each SDK message the
// `query()` loop yields is mapped here into zero-or-more agent events in the
// vocabulary the rest of Orca already speaks (the same shapes the in-sandbox path
// and the registry's SSE bridge use), so a turn driven by this runner streams
// identically to every other Orca harness. Anthropic-native events ONLY.
//
// The mapping, per SDK message kind:
//
//   - `assistant` (`SDKAssistantMessage`): one event per content block of the
//     settled message —
//       · `text`     → `agent.message` `{ content: [{ type:'text', text }] }`
//       · `thinking` → `agent.message` `{ content: [{ type:'thinking', thinking,
//                       signature }] }` (extended-thinking surfaced as the
//                       Anthropic-native `thinking` content block)
//       · `tool_use` → `agent.tool_use` `{ name, input, tool_use_id }`, with the
//                       event `id` set to the SDK `tool_use_id` so a re-push dedups
//                       on it and downstream pairs the call to its result by it.
//   - `stream_event` (`SDKPartialAssistantMessage`): the live thinking stream — a
//     `content_block_delta` carrying a `thinking_delta` becomes a PARTIAL
//     `agent.message` thinking block (`{ content:[{type:'thinking',thinking:<delta>}],
//     partial:true }`) so the thinking panel populates as the model reasons. Text
//     deltas are NOT streamed (the settled assistant message carries the whole text,
//     so the persisted `agent.message` is one clean block); other stream events are
//     housekeeping and drop.
//   - `user` (`SDKUserMessage`): tool results come back as user messages whose
//     content holds `tool_result` blocks — each becomes `agent.tool_result`
//     `{ tool_use_id, content, is_error }`, the event `id` again the `tool_use_id`
//     (the call_id that pairs it to its `agent.tool_use`).
//   - `system` (init): forwarded as a `system` event (carries MCP connection status).
//   - `system` (`mirror_error`, `SDKMirrorErrorMessage`): the SDK's transcript-mirror
//     DATA-LOSS signal — `SessionStore.append()` rejected/timed out for a batch after
//     bounded retry and the batch was DROPPED. Mapped to `agent.error` `{ message, key }`
//     so a store/Kafka outage is visible on the wire instead of silently losing history.
//   - `result`: its cumulative usage becomes an `agent.usage` event, PRECEDED by an
//     `agent.error` `{ message, subtype }` when the frame is an `SDKResultError`
//     (`is_error`, or any non-`success` subtype: max-turns / max-budget / execution
//     error) — the turn ended badly, and every consumer treats a result frame as the
//     turn's end, so the reason has to ride out with it.
//
// The mapper is a small object (not a bare function) so it can carry the per-turn
// pairing bookkeeping the call_id model needs (the set of in-flight tool-use ids),
// while staying pure: it performs no I/O and emits only plain events.

import { diagnosticError, terminalError, type AgentEvent } from '../agent-harness.js';

/** The internal token-usage shape forwarded on `agent.usage` (internal accounting). */
interface AgentUsage {
  cache_creation: { ephemeral_1h_input_tokens: number; ephemeral_5m_input_tokens: number };
  cache_read_input_tokens: number;
  input_tokens: number;
  output_tokens: number;
}

/**
 * Maps SDK messages to {@link AgentEvent}s for one turn.
 *
 * One per turn (cheap to allocate): the {@link map} call is invoked for each SDK
 * message in stream order and returns the events that message produces. The mapper
 * tracks the in-flight tool-use ids it has emitted a `agent.tool_use` for so a
 * `tool_result` can be paired back to its call by the SDK `tool_use_id`.
 */
export class SdkMessageMapper {
  /**
   * The tool-use ids this turn has emitted an `agent.tool_use` for, awaiting their
   * `tool_result`. Drives the call_id pairing: a `tool_result` whose `tool_use_id`
   * is here is the paired completion of an earlier call. (Kept for the pairing
   * model; a result for an unknown id is still emitted — the id is the pairing key
   * downstream regardless of whether this mapper observed the matching call.)
   */
  private readonly pendingToolUseIds = new Set<string>();

  /** Map one SDK message to the agent events it produces (possibly none). */
  map(message: unknown): AgentEvent[] {
    if (message === null || typeof message !== 'object') {
      return [];
    }
    const type = (message as { type?: unknown }).type;
    switch (type) {
      case 'assistant':
        return this.mapAssistant(message);
      case 'stream_event':
        return mapStreamEvent(message);
      case 'user':
        return this.mapUser(message);
      case 'system':
        return mapSystem(message);
      case 'result':
        return mapResult(message);
      default:
        return [];
    }
  }

  /** `assistant` → one event per content block (text / thinking → message, tool_use → tool_use). */
  private mapAssistant(message: unknown): AgentEvent[] {
    const content = contentBlocksOf(message);
    if (content === undefined) {
      return [];
    }
    const events: AgentEvent[] = [];
    for (const raw of content) {
      if (raw === null || typeof raw !== 'object') {
        continue;
      }
      const block = raw as Record<string, unknown>;
      if (block['type'] === 'text' && typeof block['text'] === 'string') {
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
      } else if (block['type'] === 'tool_use') {
        const toolUseId = block['id'];
        // The SDK always carries a string `id` on a tool_use block; skip a malformed
        // block rather than bucketing it under "" (it could not be paired anyway).
        if (typeof toolUseId !== 'string' || toolUseId.length === 0) {
          continue;
        }
        this.pendingToolUseIds.add(toolUseId);
        events.push({
          kind: 'agent.tool_use',
          id: toolUseId,
          payload: {
            name: typeof block['name'] === 'string' ? block['name'] : '',
            input: isRecord(block['input']) ? block['input'] : {},
            tool_use_id: toolUseId,
          },
        });
      }
    }
    return events;
  }

  /** `user` → one `agent.tool_result` per tool_result block, paired by tool_use_id. */
  private mapUser(message: unknown): AgentEvent[] {
    const content = contentBlocksOf(message);
    if (content === undefined) {
      return [];
    }
    const events: AgentEvent[] = [];
    for (const raw of content) {
      if (raw === null || typeof raw !== 'object') {
        continue;
      }
      const block = raw as Record<string, unknown>;
      if (block['type'] !== 'tool_result') {
        continue;
      }
      const toolUseId = block['tool_use_id'];
      if (typeof toolUseId !== 'string' || toolUseId.length === 0) {
        continue; // cannot be paired back to a call — drop it.
      }
      this.pendingToolUseIds.delete(toolUseId);
      events.push({
        kind: 'agent.tool_result',
        id: toolUseId,
        payload: {
          tool_use_id: toolUseId,
          content: block['content'],
          is_error: Boolean(block['is_error']),
        },
      });
    }
    return events;
  }
}

/** `stream_event` → a partial thinking `agent.message` for a thinking_delta; else none. */
function mapStreamEvent(message: unknown): AgentEvent[] {
  const event = (message as { event?: unknown }).event;
  if (event === null || typeof event !== 'object') {
    return [];
  }
  const evt = event as { type?: unknown; delta?: unknown };
  if (evt.type !== 'content_block_delta' || evt.delta === null || typeof evt.delta !== 'object') {
    return [];
  }
  const delta = evt.delta as { type?: unknown; thinking?: unknown };
  // Only thinking is streamed live (text rides the settled assistant message); a
  // thinking_delta with a non-empty string becomes a partial thinking block.
  if (
    delta.type === 'thinking_delta' &&
    typeof delta.thinking === 'string' &&
    delta.thinking.length > 0
  ) {
    return [
      {
        kind: 'agent.message',
        payload: { content: [{ type: 'thinking', thinking: delta.thinking }], partial: true },
      },
    ];
  }
  return [];
}

/**
 * `system` → a `system` event for `init`, an `agent.error` for `mirror_error`; other
 * system subtypes drop.
 *
 * `mirror_error` (`SDKMirrorErrorMessage`) is the SDK's own DATA-LOSS signal: a
 * transcript-mirror batch that `SessionStore.append()` could not persist after bounded
 * retry, and therefore DROPPED. The session adapter wires `append` straight at the
 * `TranscriptStore`, so a store/Kafka outage produces exactly this frame — dropping it
 * would leave the loss silent, which is the one thing the SDK emits it to prevent. It
 * carries the batch's `key` (`{ projectKey, sessionId, subpath? }`), forwarded verbatim
 * so the loss is attributable to a session/thread.
 */
function mapSystem(message: unknown): AgentEvent[] {
  const candidate = message as { type?: string; subtype?: string; error?: unknown; key?: unknown };
  if (candidate.type !== 'system') {
    return [];
  }
  if (candidate.subtype === 'init') {
    return [{ kind: 'system', payload: message }];
  }
  if (candidate.subtype === 'mirror_error') {
    const message = `transcript mirror append failed (batch dropped): ${
      typeof candidate.error === 'string' ? candidate.error : 'unknown error'
    }`;
    // DIAGNOSTIC, deliberately — the one `agent.error` this codebase emits that is NOT why a
    // turn ended. A dropped mirror batch must not disarm the loop's own terminal fault:
    // suppression keyed on the KIND let one of these erase "harness event stream ended before
    // the turn completed" from a truncated turn's transcript.
    const extra = isRecord(candidate.key) ? { key: candidate.key } : undefined;
    return [diagnosticError(message, extra)];
  }
  return [];
}

/**
 * `result` → the frame's terminal events: an `agent.error` when it is an
 * `SDKResultError`, then an `agent.usage` when it carried usage.
 *
 * Every consumer of a result frame treats it as the turn's END (the persistent
 * harness returns on it; the native-CLI stream reader maps it to `turn_end`), so a
 * FAILED result — `is_error`, or any non-`success` subtype (`error_during_execution`,
 * `error_max_turns`, `error_max_budget_usd`, `error_max_structured_output_retries`) —
 * would otherwise reach the client as an ordinary clean turn end with its reason
 * discarded. The error rides FIRST so the reason precedes the turn's accounting.
 */
function mapResult(message: unknown): AgentEvent[] {
  const events: AgentEvent[] = [];
  const failure = resultFailureOf(message);
  if (failure !== null) {
    // TERMINAL: every consumer treats a result frame as the turn's END, so this error is
    // the turn's own final explanation — not a mid-turn diagnostic like `mirror_error`
    // below. The runner loop reads the flag to avoid burying it under a generic one.
    const { message, ...rest } = failure;
    events.push(terminalError(String(message), rest));
  }
  const usage = usageFromSdkResult(message);
  if (usage !== null) {
    events.push({ kind: 'agent.usage', payload: { usage } });
  }
  return events;
}

/**
 * The `{ message, subtype }` payload for a FAILED result frame, or `null` when the
 * frame is a clean `success`.
 *
 * A frame is failed when `is_error` is true OR its subtype is a string other than
 * `'success'` (the four `SDKResultError` subtypes). A frame with NO subtype at all is
 * malformed, not failed — treating it as failed would invent errors for garbage input,
 * so only `is_error` can flag one. The message is the SDK's `errors` array joined; with
 * no usable strings there it falls back to the subtype, which is itself the reason —
 * EXCEPT for `'success'`, which is not a reason for anything. An `is_error` frame whose
 * subtype is `'success'` and whose `errors` are empty used to report the literal message
 * `"success"`: a failed turn described to the operator as a success, and (because this is
 * a TERMINAL error) one that would also disarm the loop's own terminal fault.
 */
function resultFailureOf(message: unknown): Record<string, unknown> | null {
  const frame = message as { subtype?: unknown; is_error?: unknown; errors?: unknown };
  const subtype = typeof frame.subtype === 'string' ? frame.subtype : undefined;
  const failed = frame.is_error === true || (subtype !== undefined && subtype !== 'success');
  if (!failed) {
    return null;
  }
  const errors = Array.isArray(frame.errors)
    ? frame.errors.filter((e): e is string => typeof e === 'string' && e.length > 0)
    : [];
  const payload: Record<string, unknown> = {
    message:
      errors.length > 0
        ? errors.join('; ')
        : subtype !== undefined && subtype !== 'success'
          ? subtype
          : 'result reported is_error',
  };
  if (subtype !== undefined) {
    payload['subtype'] = subtype;
  }
  return payload;
}

/** The `message.content` array of an assistant/user SDK message, or `undefined`. */
function contentBlocksOf(message: unknown): unknown[] | undefined {
  const inner = (message as { message?: unknown }).message;
  if (inner === null || typeof inner !== 'object') {
    return undefined;
  }
  const content = (inner as { content?: unknown }).content;
  return Array.isArray(content) ? content : undefined;
}

function usageFromSdkResult(m: unknown): AgentUsage | null {
  const usage = (m as { usage?: unknown }).usage;
  if (!usage || typeof usage !== 'object') {
    return null;
  }
  const u = usage as Record<string, unknown>;
  return {
    cache_creation: cacheCreationFromUsage(u),
    cache_read_input_tokens: numberField(u['cache_read_input_tokens']),
    input_tokens: numberField(u['input_tokens']),
    output_tokens: numberField(u['output_tokens']),
  };
}

/**
 * Map the SDK usage's cache-creation tokens to the per-TTL accounting buckets.
 *
 * The SDK's result `usage` carries a STRUCTURED `cache_creation` object — the same
 * `{ ephemeral_1h_input_tokens, ephemeral_5m_input_tokens }` breakdown the Messages
 * API returns — so when it is present we forward each TTL bucket faithfully rather
 * than collapsing everything into the 5m bucket. We FALL BACK to the flat
 * `cache_creation_input_tokens` total mapped to the 5m bucket (1h = 0) only when the
 * structured object is absent (an older payload, or a partial usage that omits it),
 * so a turn that used 1h-TTL prompt caching is no longer misattributed to the 5m
 * bucket in internal accounting whenever the SDK surfaces the breakdown.
 */
function cacheCreationFromUsage(u: Record<string, unknown>): AgentUsage['cache_creation'] {
  const structured = u['cache_creation'];
  if (isRecord(structured)) {
    return {
      ephemeral_1h_input_tokens: numberField(structured['ephemeral_1h_input_tokens']),
      ephemeral_5m_input_tokens: numberField(structured['ephemeral_5m_input_tokens']),
    };
  }
  // No TTL breakdown on this payload: fall back to the flat total in the 5m bucket.
  return {
    ephemeral_1h_input_tokens: 0,
    ephemeral_5m_input_tokens: numberField(u['cache_creation_input_tokens']),
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
