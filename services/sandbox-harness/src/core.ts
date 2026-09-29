// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// core.ts — shared primitives: ids, time, event factories, content normalization,
// and HTTP error/response helpers. Pure Node, no deps beyond node:crypto / node:http.
//
// These types are the single source of truth for the rest of the package: the
// event store stamps bare events, routes serialize stamped events, and the
// session manager / providers construct bare events through the factories here.
import { randomUUID } from 'node:crypto';
import type { ServerResponse } from 'node:http';

// ── ids / time ────────────────────────────────────────────────────────────────

/** `${prefix}_<32 hex>` — stable id format for sessions and events. */
export function genId(prefix: string): string {
  return `${prefix}_${randomUUID().replace(/-/g, '')}`;
}

/** Current time as an ISO-8601 / RFC-3339 string. */
export function nowIso(): string {
  return new Date().toISOString();
}

// ── content blocks ──────────────────────────────────────────────────────────

/**
 * A single content block. We model the two shapes this package actually emits
 * (text and image) precisely, and keep the union open for provider-specific
 * block kinds we pass through verbatim (e.g. tool-result payloads) without
 * losing type-safety on the common path.
 */
export type TextBlock = { type: 'text'; text: string };

export type ImageBlock = {
  type: 'image';
  source: { type: 'base64'; media_type: string; data: string } | { type: 'url'; url: string };
};

/** An unrecognized block kind — preserved as-is so nothing is silently dropped. */
export type UnknownBlock = { type: string; [key: string]: unknown };

export type ContentBlock = TextBlock | ImageBlock | UnknownBlock;

/**
 * Tool results stream back from the provider verbatim: a tool may return a
 * plain string, a list of content blocks, or (rarely) some other JSON value.
 */
export type ToolResultContent = string | ContentBlock[] | unknown;

/** Accepted input to {@link normalizeContent}: a plain string or pre-built blocks. */
export type MessageContent = string | ContentBlock[];

// ── event payloads ────────────────────────────────────────────────────────────
// Each *bare* event is `{ type, ...payload }`. The event store stamps
// `id` / `session_id` / `created_at` at publish time (see EventStamp).

/** Prompt-cache creation token usage split by cache lifetime. */
export interface CacheCreationUsage {
  ephemeral_1h_input_tokens?: number;
  ephemeral_5m_input_tokens?: number;
}

/** Token/cost accounting reported by the provider on turn completion. */
export interface Usage {
  input_tokens?: number;
  output_tokens?: number;
  cache_creation_input_tokens?: number;
  cache_creation?: CacheCreationUsage | null;
  cache_read_input_tokens?: number;
  [key: string]: unknown;
}

export interface UserMessageEvent {
  type: 'user.message';
  content: ContentBlock[];
}

export interface AgentMessageEvent {
  type: 'agent.message';
  content: ContentBlock[];
}

export interface AgentThinkingEvent {
  type: 'agent.thinking';
  content: ContentBlock[];
}

export interface AgentToolUseEvent {
  type: 'agent.tool_use';
  name: string;
  input: Record<string, unknown>;
  tool_use_id: string;
}

export interface AgentToolResultEvent {
  type: 'agent.tool_result';
  tool_use_id: string;
  content: ToolResultContent;
  is_error: boolean;
}

export interface AgentCustomToolUseEvent {
  type: 'agent.custom_tool_use';
  id: string;
  name: string;
  input: Record<string, unknown>;
}

/** Internal accounting event consumed by harness-server and never streamed publicly. */
export interface AgentUsageEvent {
  type: 'agent.usage';
  usage: Usage;
  model?: string;
  subagent_id?: string;
}

export interface SessionIdleEvent {
  type: 'session.status_idle';
  usage: Usage;
  total_cost_usd: number;
  usage_already_reported?: boolean;
}

export interface SessionErrorEvent {
  type: 'session.status_error';
  error: string;
}

/**
 * The discriminated union of every event this package produces, in its bare
 * (un-stamped) form. Discriminate on `type`.
 */
export type BareEvent =
  | { type: 'harness.sdk_event'; event: unknown; sequence: number }
  | UserMessageEvent
  | AgentMessageEvent
  | AgentThinkingEvent
  | AgentToolUseEvent
  | AgentToolResultEvent
  | AgentCustomToolUseEvent
  | AgentUsageEvent
  | SessionIdleEvent
  | SessionErrorEvent;

/** All event `type` literals, useful for narrowing. */
export type EventType = BareEvent['type'];

/** Fields the event store stamps onto a bare event at publish time. */
export interface EventStamp {
  id: string;
  session_id: string;
  created_at: string;
}

/** A bare event of kind `T` after the store has stamped it. */
export type Stamped<T extends BareEvent> = T & EventStamp;

/**
 * The canonical persisted/streamed event: a {@link BareEvent} plus the store's
 * stamp. This is what routes list and stream back to clients. Discriminating on
 * `type` narrows to the matching stamped variant.
 */
export type Event = Stamped<BareEvent>;

// ── event factories ──────────────────────────────────────────────────────────
// Return BARE events; the store stamps id / session_id / created_at.

export function userMessageEvent(content: MessageContent): UserMessageEvent {
  return { type: 'user.message', content: normalizeContent(content) };
}

export function agentMessageEvent(content: MessageContent): AgentMessageEvent {
  return { type: 'agent.message', content: normalizeContent(content) };
}

export function agentThinkingEvent(content: MessageContent): AgentThinkingEvent {
  return { type: 'agent.thinking', content: normalizeContent(content) };
}

export interface AgentToolUseInput {
  name: string;
  input?: Record<string, unknown> | null;
  tool_use_id: string;
}

export function agentToolUseEvent({
  name,
  input,
  tool_use_id,
}: AgentToolUseInput): AgentToolUseEvent {
  return { type: 'agent.tool_use', name, input: input ?? {}, tool_use_id };
}

export interface AgentToolResultInput {
  tool_use_id: string;
  content: ToolResultContent;
  is_error?: boolean;
}

export function agentToolResultEvent({
  tool_use_id,
  content,
  is_error = false,
}: AgentToolResultInput): AgentToolResultEvent {
  return { type: 'agent.tool_result', tool_use_id, content, is_error: Boolean(is_error) };
}

export interface AgentCustomToolUseInput {
  id: string;
  name: string;
  input?: Record<string, unknown> | null;
}

export function agentCustomToolUseEvent({
  id,
  name,
  input,
}: AgentCustomToolUseInput): AgentCustomToolUseEvent {
  return { type: 'agent.custom_tool_use', id, name, input: input ?? {} };
}

export function agentUsageEvent(input: {
  usage: Usage;
  model?: string;
  subagent_id?: string;
}): AgentUsageEvent {
  return {
    type: 'agent.usage',
    usage: input.usage,
    ...(input.model ? { model: input.model } : {}),
    ...(input.subagent_id ? { subagent_id: input.subagent_id } : {}),
  };
}

export interface SessionIdleInput {
  usage?: Usage;
  total_cost_usd?: number;
  usage_already_reported?: boolean;
}

export function sessionIdleEvent({
  usage = {},
  total_cost_usd = 0,
  usage_already_reported = false,
}: SessionIdleInput = {}): SessionIdleEvent {
  return {
    type: 'session.status_idle',
    usage,
    total_cost_usd,
    ...(usage_already_reported ? { usage_already_reported: true } : {}),
  };
}

export function sessionErrorEvent(message: unknown): SessionErrorEvent {
  return { type: 'session.status_error', error: String(message ?? 'unknown error') };
}

/**
 * Accept a plain string or an array of content blocks; always return blocks.
 * - string -> `[{ type: 'text', text }]`
 * - array  -> returned as-is
 * - anything else -> `[]`
 */
export function normalizeContent(content: MessageContent | unknown): ContentBlock[] {
  if (typeof content === 'string') return [{ type: 'text', text: content }];
  if (Array.isArray(content)) return content as ContentBlock[];
  return [];
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

/** An error carrying an HTTP status code; thrown in routes, caught by the router. */
export class HttpError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
  }
}

/** Write a JSON response with the given status, setting content-type/length. */
export function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(payload),
  });
  res.end(payload);
}

/** Write a `{ error: { message } }` JSON response. */
export function sendError(res: ServerResponse, status: number, message: unknown): void {
  sendJson(res, status, { error: { message: String(message) } });
}
