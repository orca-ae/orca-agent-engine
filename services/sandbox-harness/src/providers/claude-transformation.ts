// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// PURE: @anthropic-ai/claude-agent-sdk message -> canonical stream-json frame(s).
//
// The Claude Agent SDK already emits the canonical wire (it IS the claude CLI's
// stream-json), so this is mostly a forward + normalize:
//   - `system`       -> dropped (the session emits its own init line)
//   - `assistant`    -> forwarded (content blocks pass through unchanged)
//   - `user`         -> forwarded (tool-result echoes threaded back)
//   - `stream_event` -> forwarded (partial deltas)
//   - `result`       -> forwarded with canonical fields
//   - anything else  -> dropped (forward-compatible)
//
// `session_id` is rewritten to OUR session id so every frame in a turn agrees.
// This is the exact shape `session-manager.translateFrame` consumes on the
// parent side, so the assistant content-block passthrough (text + tool_use with
// block.id) and the result-frame fields must stay byte-for-byte stable.

import type {
  SDKAssistantMessage,
  SDKMessage,
  SDKPartialAssistantMessage,
  SDKResultMessage,
  SDKUserMessage,
} from '@anthropic-ai/claude-agent-sdk';

// ---------------------------------------------------------------------------
// Derived payload types.
//
// `BetaMessage`, `BetaRawMessageStreamEvent`, and `MessageParam` are internal
// aliases of the SDK and are not re-exported from its entrypoint, so we reach
// them through indexed access on the public message types instead of importing
// the transitive `@anthropic-ai/sdk` paths directly.
// ---------------------------------------------------------------------------

/** A single assistant content block (text, tool_use, ...) — passed through unchanged. */
export type AssistantContentBlock = SDKAssistantMessage['message']['content'][number];

/** A Raw Messages streaming event (`message_start`, `content_block_delta`, ...). */
export type StreamEvent = SDKPartialAssistantMessage['event'];

/** The echoed user message (Anthropic Messages `MessageParam`). */
export type UserMessage = SDKUserMessage['message'];

/** Token-usage accounting carried on the terminal result frame. */
export type ResultUsage = Extract<SDKResultMessage, { subtype: 'success' }>['usage'];

// ---------------------------------------------------------------------------
// Canonical wire frames — the same shapes the builders in ../protocol.ts
// (`assistantFrame`, `streamEventFrame`, `resultFrame`) produce; the field set
// and ordering are load-bearing for the parent translator.
// ---------------------------------------------------------------------------

export interface AssistantFrame {
  type: 'assistant';
  message: {
    model: string | undefined;
    content: AssistantContentBlock[];
    usage: SDKAssistantMessage['message']['usage'] | undefined;
    id?: string;
  };
  parent_tool_use_id: string | null;
}

export interface UserFrame {
  type: 'user';
  message: UserMessage;
}

export interface StreamEventFrame {
  type: 'stream_event';
  session_id: string;
  event: StreamEvent;
  parent_tool_use_id?: string;
}

export interface ResultFrame {
  type: 'result';
  subtype: string;
  session_id: string;
  duration_ms: number;
  duration_api_ms: number;
  is_error: boolean;
  num_turns: number;
  total_cost_usd: number;
  usage: ResultUsage | Record<string, never>;
  result: string;
}

/** Any frame `toFrames` may emit. The `default` branch emits none of them. */
export type CanonicalFrame = AssistantFrame | UserFrame | StreamEventFrame | ResultFrame;

export interface ToFramesContext {
  /** Our session id; every frame in a turn is rewritten to agree on it. */
  sessionId: string;
}

/**
 * Map one Claude Agent SDK message to zero or more canonical stream-json frames.
 *
 * Pure: no I/O, no shared state — output depends only on `(msg, ctx)`.
 *
 * Stays defensive about non-object input so a forward-compatible / malformed
 * message degrades to `[]` rather than throwing.
 */
export function toFrames(msg: SDKMessage, { sessionId }: ToFramesContext): CanonicalFrame[] {
  if (!msg || typeof msg !== 'object') return [];

  switch (msg.type) {
    case 'system':
      // The session emits its own `system: init`; drop the SDK's.
      return [];

    case 'assistant':
      return [toAssistantFrame(msg)];

    case 'user':
      return [{ type: 'user', message: msg.message }];

    case 'stream_event':
      return [
        {
          type: 'stream_event',
          session_id: sessionId,
          event: msg.event,
          ...(typeof msg.parent_tool_use_id === 'string'
            ? { parent_tool_use_id: msg.parent_tool_use_id }
            : {}),
        },
      ];

    case 'result':
      return [toResultFrame(msg, sessionId)];

    default:
      // Forward-compatible: silently drop message kinds we do not translate.
      return [];
  }
}

function toAssistantFrame(msg: SDKAssistantMessage): AssistantFrame {
  const message = msg.message as Partial<SDKAssistantMessage['message']> | undefined;
  return {
    type: 'assistant',
    message: {
      model: message?.model,
      content: message?.content ?? [],
      usage: message?.usage,
      ...(typeof message?.id === 'string' ? { id: message.id } : {}),
    },
    parent_tool_use_id: msg.parent_tool_use_id ?? null,
  };
}

function toResultFrame(msg: SDKResultMessage, sessionId: string): ResultFrame {
  const isError = Boolean(msg.is_error);
  return {
    type: 'result',
    subtype: msg.subtype ?? (isError ? 'error_during_execution' : 'success'),
    session_id: sessionId,
    duration_ms: msg.duration_ms ?? 0,
    duration_api_ms: msg.duration_api_ms ?? 0,
    is_error: isError,
    num_turns: msg.num_turns ?? 1,
    total_cost_usd: msg.total_cost_usd ?? 0,
    usage: msg.usage ?? {},
    result: resultText(msg),
  };
}

// `result` (the assistant's final text) lives only on the success variant; the
// error variant carries `errors[]` instead. This never branches on `subtype`: it
// forwards a `result` string whenever the frame carries one and falls back to ""
// otherwise. The `"result" in msg` guard narrows to the member holding `result`
// without imposing a `subtype === "success"` constraint, so an error frame still
// yields "" while a malformed error-subtype frame bearing a `result` is
// forwarded verbatim.
function resultText(msg: SDKResultMessage): string {
  return 'result' in msg ? (msg.result ?? '') : '';
}
