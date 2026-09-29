// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The `claude-code` stream-json normalizer — one stdout line → Orca-native events +
// control frames.
//
// The headless `claude` binary emits one JSON object per stdout line ("stream-json").
// Two concerns live here, kept separate:
//
//   1. The DATA frames are the SAME SDK message shapes the in-process claude provider
//      already normalizes — `assistant` (text / thinking / tool_use), `user`
//      (tool_result), `system` (init), `result` (usage), and the partial-thinking
//      `stream_event`. Rather than re-implement that mapping, this normalizer DELEGATES
//      those to the shared {@link SdkMessageMapper}, so there is ONE definition of the
//      SDK-message → {@link AgentEvent} projection across the in-process and native-CLI
//      providers, and a turn driven either way streams identically.
//   2. The CONTROL frames are what the headless stdio protocol adds that an in-process
//      SDK consumer never sees on its message stream:
//        · a `control_request` of subtype `can_use_tool` — the CLI asking whether a
//          tool call may proceed. Surfaced as a {@link PermissionRequest} carrying the
//          `request_id` (the reply's routing key), the tool name + input, and the
//          `tool_use_id`, so the harness can route it to the uniform transcript
//          approval and answer over stdin;
//        · a `control_cancel_request` — the CLI WITHDRAWING a permission it previously
//          raised (per the SDK's `SDKControlRequestInner` union, a cancel carries the
//          `request_id` of the `control_request` it retracts). Surfaced as a
//          {@link PermissionCancel} so the harness releases the matching parked gate as
//          a deny rather than leaving it awaiting a verdict the CLI no longer wants;
//        · the `result` frame additionally marks the TURN END (headless: the CLI stays
//          alive for the next turn), so the harness settles the current `submit`.
//      Housekeeping frames (`control_response`, `keep_alive`, unknown/blank) are ignored.
//
// The normalizer is a small stateful object (one per turn) because the delegated
// {@link SdkMessageMapper} carries the per-turn tool-use ↔ tool-result pairing
// bookkeeping. It performs no I/O.

import type { AgentEvent } from '../agent-harness.js';
import { SdkMessageMapper } from '../claude/sdk-message-mapper.js';

/**
 * A tool-permission request the CLI raised over the stream-json control channel (a
 * `can_use_tool` control_request). The harness answers it with a `control_response`
 * written to stdin, keyed by {@link requestId}.
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
 * A permission WITHDRAWAL the CLI raised over the stream-json control channel (a
 * `control_cancel_request`). It retracts an earlier `can_use_tool` request keyed by
 * {@link requestId}, so the harness releases that parked gate as a deny.
 */
export interface PermissionCancel {
  /** The `request_id` of the `control_request` being withdrawn (the park's key). */
  requestId: string;
}

/**
 * The outcome of normalizing one stdout line — a discriminated union the harness
 * dispatches on:
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

/** Maps stream-json stdout lines to {@link NormalizedLine}s for one turn. */
export class ClaudeCodeStreamNormalizer {
  /** The shared SDK-message mapper (carries the per-turn tool-use ↔ result pairing). */
  private readonly mapper = new SdkMessageMapper();

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
      // The CLI is withdrawing an earlier request. Surface it as a cancel keyed by the
      // retracted `request_id` so the harness releases the matching parked gate; a
      // cancel with no string `request_id` is dropped (there is nothing to release).
      const cancel = readPermissionCancel(line);
      return cancel !== undefined ? { kind: 'permission_cancel', cancel } : { kind: 'ignore' };
    }
    if (type === 'control_response' || type === 'keep_alive') {
      return { kind: 'ignore' };
    }

    // The result frame ends the turn (and may carry a usage event).
    if (type === 'result') {
      return { kind: 'turn_end', events: this.mapper.map(line) };
    }

    // Every other data frame → the shared SDK-message projection. A frame that the
    // mapper produced no events for (an unknown type, or a system subtype it drops) is
    // `ignore` rather than an empty `events` result — so the caller only handles frames
    // that actually carried agent events, and an unknown line is uniformly dropped.
    const events = this.mapper.map(line);
    return events.length > 0 ? { kind: 'events', events } : { kind: 'ignore' };
  }
}

/**
 * Read a `can_use_tool` control_request into a {@link PermissionRequest}, or `undefined`
 * for any other control_request subtype (interrupt / mcp_message / set_model / …), which
 * the harness ignores. Defensive about the payload shape (the runner treats the CLI's
 * stdout as untrusted bytes): a missing `input` defaults to `{}`, a missing `tool_use_id`
 * to `''`, and a control_request with no string `request_id` is dropped (it could not be
 * answered anyway).
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
 * Read a `control_cancel_request` into a {@link PermissionCancel}, or `undefined` when
 * it carries no string `request_id` (nothing to release). The cancel's `request_id`
 * names the `control_request` being withdrawn — the same key the harness parked the
 * permission under. Defensive about the payload shape (the CLI's stdout is untrusted).
 */
function readPermissionCancel(line: unknown): PermissionCancel | undefined {
  const obj = line as { request_id?: unknown };
  if (typeof obj.request_id !== 'string' || obj.request_id.length === 0) {
    return undefined;
  }
  return { requestId: obj.request_id };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
