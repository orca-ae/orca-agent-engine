// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The generic custom-provider STDOUT NORMALIZER — one stdout line → Orca-native events.
//
// The `custom` provider maps a CLI's stdout to Orca-native Anthropic-shaped {@link AgentEvent}s per
// the operator's declarative stdout mapping (see `spec.ts`). Two modes:
//
//   - `text` — the simplest CLI: it prints assistant text. Every NON-BLANK stdout line becomes an
//     `agent.message` text event; a configured `end_sentinel` line ends the turn (else the turn
//     ends on stream EOF, which the harness detects separately).
//   - `jsonLine` — each stdout line is a JSON object matched against the spec's field rules, in a
//     fixed precedence (turn_completed → approval_request → tool_use → tool_result → usage → text).
//     A matched rule maps the frame to the corresponding Anthropic-native event(s) / a turn-end / an
//     approval request the harness routes to the gate.
//
// Both modes emit the SAME Anthropic-native shapes every other provider emits (no dialect):
//   - `agent.message`     → `{ content: [{ type: 'text', text }] }`
//   - `agent.tool_use`    → `{ name, input, tool_use_id }` (+ the event `id`)
//   - `agent.tool_result` → `{ tool_use_id, content, is_error }`
//   - `agent.usage`       → `{ usage: { input_tokens, output_tokens, ... } }`
//
// The normalizer is a small stateful object (one per session); it performs no I/O.

import type { AgentEvent } from '../agent-harness.js';
import type {
  CustomAgentSpec,
  CustomApprovalRule,
  CustomJsonLineStdout,
  CustomToolResultRule,
  CustomToolUseRule,
  CustomUsageRule,
} from './spec.js';

/**
 * An approval request the CLI raised on stdout (matched by the spec's `approval_request` rule).
 * The harness routes it to the human gate and writes the spec's stdin response frame back, keyed
 * by {@link requestId}.
 */
export interface CustomApprovalRequest {
  /** The approval request id — the routing key of the stdin response frame. */
  requestId: string;
  /** The tool name being approved (for the requires_action signal + policy lookup). */
  toolName: string;
  /** The tool input the CLI proposed (defaults to `{}` when the frame omits it). */
  input: Record<string, unknown>;
}

/**
 * The outcome of normalizing one stdout line — a discriminated union the harness dispatches on:
 *   - `events`: a frame that produced zero-or-more agent events (emit them);
 *   - `turn_end`: a frame that ends the turn (the sentinel / the `turn_completed` rule);
 *   - `approval_request`: an approval frame to route to the gate;
 *   - `ignore`: a blank / unmatched / non-object line (drop).
 */
export type NormalizedLine =
  | { kind: 'events'; events: AgentEvent[] }
  | { kind: 'turn_end'; events: AgentEvent[] }
  | { kind: 'approval_request'; request: CustomApprovalRequest }
  | { kind: 'ignore' };

/** Maps stdout lines to {@link NormalizedLine}s per a {@link CustomAgentSpec}'s stdout mapping. */
export class CustomStreamNormalizer {
  constructor(private readonly spec: CustomAgentSpec) {}

  /**
   * Normalize one stdout line. Accepts either the raw string line (the harness feeds raw lines) or
   * an already-parsed frame (the jsonLine tests feed objects directly). In `text` mode a non-string
   * value is stringified; in `jsonLine` mode a string is JSON-parsed (a non-JSON string is ignored).
   */
  map(line: unknown): NormalizedLine {
    if (this.spec.stdout.mode === 'text') {
      return this.mapText(typeof line === 'string' ? line : String(line));
    }
    return this.mapJsonLine(this.spec.stdout, coerceFrame(line));
  }

  /** `text` mode: a non-blank line is agent text; the end sentinel ends the turn. */
  private mapText(line: string): NormalizedLine {
    const stdout = this.spec.stdout;
    if (stdout.mode !== 'text') {
      return { kind: 'ignore' };
    }
    if (stdout.end_sentinel !== undefined && line.trim() === stdout.end_sentinel) {
      return { kind: 'turn_end', events: [] };
    }
    if (line.trim().length === 0) {
      return { kind: 'ignore' };
    }
    return { kind: 'events', events: [textEvent(line)] };
  }

  /** `jsonLine` mode: match `frame` against the field rules in precedence order. */
  private mapJsonLine(
    stdout: CustomJsonLineStdout,
    frame: Record<string, unknown> | undefined,
  ): NormalizedLine {
    if (frame === undefined) {
      return { kind: 'ignore' };
    }
    // Turn end first — an end frame may also carry a type that would match another rule.
    if (
      stdout.turn_completed !== undefined &&
      frame['type'] === stdout.turn_completed.type_equals
    ) {
      return { kind: 'turn_end', events: [] };
    }
    if (stdout.approval_request !== undefined && ruleMatches(stdout.approval_request, frame)) {
      const request = readApproval(stdout.approval_request, frame);
      return request !== undefined ? { kind: 'approval_request', request } : { kind: 'ignore' };
    }
    if (stdout.tool_use !== undefined && ruleMatches(stdout.tool_use, frame)) {
      return { kind: 'events', events: [toolUseEvent(stdout.tool_use, frame)] };
    }
    if (stdout.tool_result !== undefined && ruleMatches(stdout.tool_result, frame)) {
      return { kind: 'events', events: [toolResultEvent(stdout.tool_result, frame)] };
    }
    if (stdout.usage !== undefined && ruleMatches(stdout.usage, frame)) {
      return { kind: 'events', events: [usageEvent(stdout.usage, frame)] };
    }
    if (stdout.text !== undefined && ruleMatches(stdout.text, frame)) {
      const text = frame[stdout.text.text_field];
      if (typeof text === 'string' && text.length > 0) {
        return { kind: 'events', events: [textEvent(text)] };
      }
    }
    return { kind: 'ignore' };
  }
}

// ── frame → event mappers ─────────────────────────────────────────────────────────

/** An `agent.message` text event. */
function textEvent(text: string): AgentEvent {
  return { kind: 'agent.message', payload: { content: [{ type: 'text', text }] } };
}

/** An `agent.tool_use` event from a matched tool-call frame. */
function toolUseEvent(rule: CustomToolUseRule, frame: Record<string, unknown>): AgentEvent {
  const id = stringOf(frame[rule.id_field]);
  const name = stringOf(frame[rule.name_field]);
  const input = recordOf(frame[rule.input_field]);
  const event: AgentEvent = {
    kind: 'agent.tool_use',
    payload: { name, input, tool_use_id: id },
  };
  if (id.length > 0) {
    event.id = id;
  }
  return event;
}

/** An `agent.tool_result` event from a matched tool-result frame. */
function toolResultEvent(rule: CustomToolResultRule, frame: Record<string, unknown>): AgentEvent {
  const id = stringOf(frame[rule.id_field]);
  return {
    kind: 'agent.tool_result',
    payload: {
      tool_use_id: id,
      content: contentBlocks(frame[rule.content_field]),
      is_error: frame[rule.error_field] === true,
    },
  };
}

/** An internal `agent.usage` event from a matched usage frame. */
function usageEvent(rule: CustomUsageRule, frame: Record<string, unknown>): AgentEvent {
  return {
    kind: 'agent.usage',
    payload: {
      usage: {
        input_tokens: numberOf(frame[rule.input_tokens_field]),
        output_tokens: numberOf(frame[rule.output_tokens_field]),
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
      },
    },
  };
}

/** Read a matched approval frame into a {@link CustomApprovalRequest}, or `undefined` when it has no id. */
function readApproval(
  rule: CustomApprovalRule,
  frame: Record<string, unknown>,
): CustomApprovalRequest | undefined {
  const requestId = stringOf(frame[rule.id_field]);
  if (requestId.length === 0) {
    return undefined; // nothing to answer.
  }
  return {
    requestId,
    toolName: stringOf(frame[rule.name_field]),
    input: recordOf(frame[rule.input_field]),
  };
}

// ── helpers ─────────────────────────────────────────────────────────────────────

/** Coerce a stdout line to a JSON frame: a string is parsed; an object is used as-is. */
function coerceFrame(line: unknown): Record<string, unknown> | undefined {
  if (typeof line === 'string') {
    try {
      const parsed: unknown = JSON.parse(line);
      return isRecord(parsed) ? parsed : undefined;
    } catch {
      return undefined;
    }
  }
  return isRecord(line) ? line : undefined;
}

/** Whether a rule with an optional `type_equals` matches `frame` (a rule with none matches any). */
function ruleMatches(rule: { type_equals?: string }, frame: Record<string, unknown>): boolean {
  return rule.type_equals === undefined || frame['type'] === rule.type_equals;
}

/**
 * Normalize a tool-result content value to the Anthropic content-block array shape. A string
 * becomes one text block; an already-array value is passed through; anything else is JSON-encoded
 * into a single text block so a downstream consumer always sees `[{ type: 'text', text }]`-shaped
 * content.
 */
function contentBlocks(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value;
  }
  if (typeof value === 'string') {
    return [{ type: 'text', text: value }];
  }
  if (value === undefined || value === null) {
    return [{ type: 'text', text: '' }];
  }
  return [{ type: 'text', text: JSON.stringify(value) }];
}

function stringOf(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

function numberOf(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function recordOf(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
