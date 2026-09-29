// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { v5 as uuidv5, v7 as uuidv7 } from 'uuid';
import type { Event } from '@orca/transcript-store';
import {
  AgentEventKind,
  AgentThreadEventKind,
  InternalTranscriptEventKind,
  isAgentEventId,
  SessionEventKind,
  SessionThreadEventKind,
  SpanEventKind,
} from '@orca/agent-event-contract';

export interface HttpEventInput {
  id?: string;
  type: string;
  subpath?: string;
  content?: unknown;
  [k: string]: unknown;
}

export interface HttpEventOutput {
  id: string;
  type: string;
  processed_at: string | null;
  produced_at: string;
  produced_by: string;
  subpath?: string;
  seq: string;
  content?: unknown;
  [k: string]: unknown;
}

export interface ToProtoInput {
  workspaceId: string;
  sessionId: string;
  producedBy: 'client' | 'harness';
  /** Trusted opaque OIDC attribution; never derive this from a public event input or request headers. */
  userId?: string | undefined;
  idempotencyKey: string;
  input: HttpEventInput;
  now?: () => Date;
  /** Public Claude requests use the documented seven-event input union. */
  validationMode?: 'claude' | 'orca';
}

export type TranscriptEventVisibility = 'public' | 'internal';

export const INTERNAL_TRANSCRIPT_EVENT_PREFIX = 'harness.';
export const CLAUDE_SESSION_EVENT_TYPES = new Set<string>([
  'user.message',
  'user.interrupt',
  'user.tool_confirmation',
  'user.custom_tool_result',
  'user.define_outcome',
  'user.tool_result',
  'system.message',
  AgentEventKind.customToolUse,
  AgentEventKind.message,
  AgentEventKind.thinking,
  AgentEventKind.mcpToolUse,
  AgentEventKind.mcpToolResult,
  AgentEventKind.toolUse,
  AgentEventKind.toolResult,
  AgentThreadEventKind.messageReceived,
  AgentThreadEventKind.messageSent,
  AgentEventKind.threadContextCompacted,
  SessionEventKind.error,
  SessionEventKind.statusRescheduled,
  SessionEventKind.statusRunning,
  SessionEventKind.statusIdle,
  SessionEventKind.statusTerminated,
  SessionThreadEventKind.created,
  SessionThreadEventKind.statusRunning,
  SessionThreadEventKind.statusIdle,
  SessionThreadEventKind.statusTerminated,
  SessionThreadEventKind.statusRescheduled,
  'session.deleted',
  'session.updated',
  SpanEventKind.modelRequestStart,
  SpanEventKind.modelRequestEnd,
  SpanEventKind.outcomeEvaluationStart,
  SpanEventKind.outcomeEvaluationOngoing,
  SpanEventKind.outcomeEvaluationEnd,
]);

export function eventBatchIdempotencyKeys(
  requestId: string | undefined,
  eventCount: number,
): string[] {
  if (!requestId) return Array.from({ length: eventCount }, () => '');
  const requestIdHash = createHash('sha256').update(requestId).digest('hex');
  return Array.from({ length: eventCount }, (_, index) => `sha256:${requestIdHash}:${index}`);
}

const OIDC_TRANSCRIPT_USER_ID_DOMAIN = 'orca.transcript.oidc_user.v1';
const OIDC_TRANSCRIPT_USER_ID_SEPARATOR = '\0';

/**
 * Derive an opaque, versioned Transcript attribution from the verified OIDC
 * issuer and standard subject. This is deliberately distinct from the raw
 * subject retained for public Memory `user_actor` attribution.
 */
export function oidcTranscriptUserId(
  issuer: string | undefined,
  subject: string | undefined,
): string | undefined {
  if (
    typeof issuer !== 'string' ||
    issuer.length === 0 ||
    typeof subject !== 'string' ||
    subject.length === 0
  ) {
    return undefined;
  }
  const hash = createHash('sha256');
  hash.update(OIDC_TRANSCRIPT_USER_ID_DOMAIN, 'utf8');
  hash.update(OIDC_TRANSCRIPT_USER_ID_SEPARATOR, 'utf8');
  hash.update(issuer, 'utf8');
  hash.update(OIDC_TRANSCRIPT_USER_ID_SEPARATOR, 'utf8');
  hash.update(subject, 'utf8');
  return `oidc_user_${hash.digest('hex')}`;
}

const TOOL_EVENT_ID_NAMESPACE = 'fb36df80-c16a-4b8e-b226-8a6400cc4048';
const INTERNAL_TRANSCRIPT_EVENT_KINDS = new Set([
  'session.deferred_user_message',
  'session.deferred_user_message_submitted',
  InternalTranscriptEventKind.userEventProcessed,
  InternalTranscriptEventKind.userEventCompleted,
]);
const HARNESS_QUEUED_CLIENT_EVENT_KINDS = new Set([
  'user.message',
  'user.interrupt',
  'user.tool_confirmation',
  'user.custom_tool_result',
  'user.tool_result',
  'system.message',
]);

export function transcriptEventVisibility(type: string): TranscriptEventVisibility {
  if (INTERNAL_TRANSCRIPT_EVENT_KINDS.has(type)) return 'internal';
  return type.startsWith(INTERNAL_TRANSCRIPT_EVENT_PREFIX) ? 'internal' : 'public';
}

export function isPublicTranscriptEvent(e: Pick<Event, 'kind'>): boolean {
  return transcriptEventVisibility(e.kind) === 'public';
}

export function validateClientEvent(
  input: HttpEventInput,
  validationMode: 'claude' | 'orca' = 'orca',
): string | null {
  if (input.type.startsWith('session.thread_')) {
    return 'reserved event.type prefix: session.thread_';
  }
  switch (input.type) {
    case 'user.message':
      return Array.isArray(input.content) && input.content.length > 0
        ? null
        : 'user.message content must be a non-empty array';
    case 'user.interrupt':
      return null;
    case 'user.tool_confirmation': {
      if (typeof input.tool_use_id !== 'string') {
        return 'user.tool_confirmation requires tool_use_id and result (allow|deny)';
      }
      // Authoritative managed-agents-2026-04-01 field is `result`; `approved`
      // is retained as a deprecated boolean alias.
      const hasResult = input.result === 'allow' || input.result === 'deny';
      const hasApproved = typeof input.approved === 'boolean';
      if (!hasResult && !hasApproved) {
        return 'user.tool_confirmation requires tool_use_id and result (allow|deny)';
      }
      if (
        input.deny_message !== undefined &&
        input.deny_message !== null &&
        typeof input.deny_message !== 'string'
      ) {
        return 'user.tool_confirmation deny_message must be a string';
      }
      return null;
    }
    case 'user.custom_tool_result':
      return typeof input.custom_tool_use_id === 'string' || typeof input.tool_use_id === 'string'
        ? null
        : 'user.custom_tool_result requires custom_tool_use_id';
    case 'user.define_outcome':
      return typeof input.description === 'string' &&
        input.description.trim().length > 0 &&
        input.rubric !== undefined &&
        (input.max_iterations == null ||
          (Number.isInteger(input.max_iterations) &&
            typeof input.max_iterations === 'number' &&
            input.max_iterations >= 1 &&
            input.max_iterations <= 20))
        ? null
        : 'user.define_outcome requires description, rubric, and max_iterations 1..20 when set';
    case 'user.tool_result':
      return validationMode === 'claude' && typeof input.tool_use_id === 'string'
        ? null
        : validationMode === 'claude'
          ? 'user.tool_result requires tool_use_id'
          : `unsupported client event.type: ${input.type}`;
    case 'system.message':
      return validationMode === 'claude' && Array.isArray(input.content) && input.content.length > 0
        ? null
        : validationMode === 'claude'
          ? 'system.message content must be a non-empty array'
          : `unsupported client event.type: ${input.type}`;
    default:
      if (validationMode === 'claude' || input.type.startsWith('user.')) {
        return `unsupported client event.type: ${input.type}`;
      }
      return null;
  }
}

export function httpEventToProto(args: ToProtoInput): Event {
  const { type } = args.input;
  if (!type || typeof type !== 'string') {
    throw new Error('event.type is required');
  }
  if (args.producedBy === 'client') {
    if (type.startsWith(INTERNAL_TRANSCRIPT_EVENT_PREFIX)) {
      throw new Error(`reserved event.type prefix: ${INTERNAL_TRANSCRIPT_EVENT_PREFIX}`);
    }
    if (transcriptEventVisibility(type) === 'internal') {
      throw new Error(`reserved event.type: ${type}`);
    }
    const validationError = validateClientEvent(args.input, args.validationMode);
    if (validationError) throw new Error(validationError);
  }
  const id = isAgentEventId(args.input.id) ? args.input.id : `evt_${uuidv7()}`;
  const subpath = typeof args.input.subpath === 'string' ? args.input.subpath : '';
  const producedAt = (args.now?.() ?? new Date()).toISOString();
  let payloadInput: HttpEventInput = args.input;
  if (
    type === 'user.tool_confirmation' &&
    args.input.result == null &&
    typeof args.input.approved === 'boolean'
  ) {
    // Normalize the deprecated `approved` alias to the authoritative Claude
    // `result` field so the data plane always sees the managed-agents shape.
    // The original `approved` is preserved for backward-compatible readers.
    payloadInput = { ...args.input, result: args.input.approved ? 'allow' : 'deny' };
  }
  if (
    type === 'user.custom_tool_result' &&
    typeof args.input.custom_tool_use_id !== 'string' &&
    typeof args.input.tool_use_id === 'string'
  ) {
    payloadInput = { ...args.input, custom_tool_use_id: args.input.tool_use_id };
  }
  if (args.producedBy === 'client') {
    // Harness-driving events enter the transcript before submission and stay
    // queued until the dispatcher appends a processing marker. Passive Orca
    // extension events are complete at append time. Never trust a caller's
    // processed_at value for either path.
    payloadInput = {
      ...payloadInput,
      processed_at: HARNESS_QUEUED_CLIENT_EVENT_KINDS.has(type) ? null : producedAt,
    };
  }
  const payload = Buffer.from(JSON.stringify(payloadInput), 'utf8');
  return {
    id,
    workspaceId: args.workspaceId,
    sessionId: args.sessionId,
    subpath,
    seq: 0,
    producedAt,
    producedBy: args.producedBy,
    kind: type,
    payload,
    idempotencyKey: args.idempotencyKey,
    ...(args.producedBy === 'client' && typeof args.userId === 'string' && args.userId.length > 0
      ? { userId: args.userId }
      : {}),
  };
}

export function protoToHttpEvent(e: Event): HttpEventOutput {
  let parsed: HttpEventInput;
  try {
    const value = JSON.parse(Buffer.from(e.payload).toString('utf8')) as unknown;
    parsed = isRecord(value) ? (value as HttpEventInput) : ({ type: e.kind } as HttpEventInput);
  } catch {
    parsed = { type: e.kind } as HttpEventInput;
  }
  return serializeHttpEvent(parsed, {
    id: e.id,
    type: e.kind,
    producedAt: e.producedAt,
    producedBy: e.producedBy,
    processedAt: eventProcessedAt(e, parsed),
    seq: String(e.seq),
    ...(e.subpath ? { subpath: e.subpath } : {}),
  });
}

/**
 * Remove Orca transcript-envelope fields from the default Claude wire view.
 * The event payload itself remains forward compatible with new documented
 * event variants. `orca-beta` callers retain the full transcript envelope.
 */
export function toPublicHttpEvent(
  event: HttpEventOutput,
  orcaBeta: boolean,
): HttpEventOutput | Record<string, unknown> {
  if (orcaBeta) return event;
  const {
    produced_at: _producedAt,
    produced_by: _producedBy,
    seq: _seq,
    subpath: _subpath,
    request_id: _requestId,
    approved: _approved,
    status: _status,
    ...publicEvent
  } = event;
  delete publicEvent.source_event_id;
  delete publicEvent.source_event_type;
  delete publicEvent.source_subpath;
  if (publicEvent.type === 'event_start') {
    const preview = isRecord(publicEvent.event) ? publicEvent.event : {};
    return {
      type: 'event_start',
      event: {
        id: preview.id,
        type: preview.type,
      },
    };
  }
  if (publicEvent.type === 'event_delta') {
    return {
      type: 'event_delta',
      event_id: publicEvent.event_id,
      delta: publicEvent.delta,
    };
  }
  if (publicEvent.type === SessionEventKind.error) {
    return canonicalSessionErrorEvent(publicEvent);
  }
  if (
    publicEvent.type === AgentEventKind.thinking ||
    publicEvent.type === AgentEventKind.threadContextCompacted ||
    publicEvent.type === SessionEventKind.statusRescheduled ||
    publicEvent.type === SessionEventKind.statusRunning ||
    publicEvent.type === SessionEventKind.statusTerminated ||
    publicEvent.type === 'session.deleted' ||
    publicEvent.type === SpanEventKind.modelRequestStart
  ) {
    return publicEventBase(publicEvent);
  }
  if (publicEvent.type === AgentEventKind.message) {
    delete publicEvent.parent_tool_use_id;
  }
  if (
    (publicEvent.type === AgentEventKind.toolUse ||
      publicEvent.type === AgentEventKind.mcpToolUse) &&
    typeof publicEvent.tool_use_id === 'string'
  ) {
    publicEvent.id = canonicalToolEventId(publicEvent.tool_use_id);
    delete publicEvent.tool_use_id;
  }
  if (
    publicEvent.type === AgentEventKind.toolResult &&
    typeof publicEvent.tool_use_id === 'string'
  ) {
    publicEvent.tool_use_id = canonicalToolEventId(publicEvent.tool_use_id);
  }
  if (
    publicEvent.type === AgentEventKind.mcpToolResult &&
    publicEvent.mcp_tool_use_id === undefined &&
    typeof publicEvent.tool_use_id === 'string'
  ) {
    publicEvent.mcp_tool_use_id = publicEvent.tool_use_id;
    delete publicEvent.tool_use_id;
  }
  if (
    publicEvent.type === AgentEventKind.mcpToolResult &&
    typeof publicEvent.mcp_tool_use_id === 'string'
  ) {
    publicEvent.mcp_tool_use_id = canonicalToolEventId(publicEvent.mcp_tool_use_id);
  }
  if (
    (publicEvent.type === AgentEventKind.toolResult ||
      publicEvent.type === AgentEventKind.mcpToolResult) &&
    typeof publicEvent.content === 'string'
  ) {
    publicEvent.content = [{ type: 'text', text: publicEvent.content }];
  }
  if (publicEvent.type === AgentThreadEventKind.messageSent) {
    if (
      publicEvent.to_session_thread_id === undefined &&
      typeof publicEvent.session_thread_id === 'string'
    ) {
      publicEvent.to_session_thread_id = publicEvent.session_thread_id;
    }
    delete publicEvent.session_thread_id;
  }
  if (publicEvent.type === AgentThreadEventKind.messageReceived) {
    if (
      publicEvent.from_session_thread_id === undefined &&
      typeof publicEvent.session_thread_id === 'string'
    ) {
      publicEvent.from_session_thread_id = publicEvent.session_thread_id;
    }
    delete publicEvent.session_thread_id;
  }
  if (publicEvent.type === AgentEventKind.toolUse) {
    return publicEventFields(publicEvent, [
      'name',
      'input',
      'evaluated_permission',
      'session_thread_id',
    ]);
  }
  if (publicEvent.type === AgentEventKind.mcpToolUse) {
    return publicEventFields(publicEvent, [
      'name',
      'input',
      'mcp_server_name',
      'evaluated_permission',
      'session_thread_id',
    ]);
  }
  if (publicEvent.type === AgentEventKind.toolResult) {
    return publicEventFields(publicEvent, ['tool_use_id', 'content', 'is_error']);
  }
  if (publicEvent.type === AgentEventKind.mcpToolResult) {
    return publicEventFields(publicEvent, ['mcp_tool_use_id', 'content', 'is_error']);
  }
  if (publicEvent.type === AgentThreadEventKind.messageSent) {
    return publicEventFields(publicEvent, ['content', 'to_session_thread_id', 'to_agent_name']);
  }
  if (publicEvent.type === AgentThreadEventKind.messageReceived) {
    return publicEventFields(publicEvent, ['content', 'from_session_thread_id', 'from_agent_name']);
  }
  if (publicEvent.type === SpanEventKind.modelRequestEnd) {
    delete publicEvent.model_observation_kind;
    delete publicEvent.provider;
    delete publicEvent.model;
    delete publicEvent.total_cost_usd;
  }
  if (publicEvent.type === SessionThreadEventKind.statusTerminated) {
    delete publicEvent.stop_reason;
  }
  if (publicEvent.type === 'user.define_outcome' && publicEvent.max_iterations === undefined) {
    publicEvent.max_iterations = 3;
  }
  return publicEvent;
}

const CLAUDE_SESSION_ERROR_TYPES = new Set([
  'unknown_error',
  'model_overloaded_error',
  'model_rate_limited_error',
  'model_request_failed_error',
  'mcp_connection_failed_error',
  'mcp_authentication_failed_error',
  'billing_error',
  'credential_host_unreachable_error',
]);

function publicEventBase(event: Record<string, unknown>): Record<string, unknown> {
  return {
    id: event.id,
    type: event.type,
    processed_at: event.processed_at,
  };
}

function publicEventFields(
  event: Record<string, unknown>,
  fields: string[],
): Record<string, unknown> {
  const output = publicEventBase(event);
  for (const field of fields) {
    if (event[field] !== undefined) output[field] = event[field];
  }
  return output;
}

function canonicalSessionErrorEvent(event: Record<string, unknown>): Record<string, unknown> {
  const rawError = isRecord(event.error) ? event.error : {};
  const rawRetry = isRecord(rawError.retry_status)
    ? rawError.retry_status
    : isRecord(event.retry_status)
      ? event.retry_status
      : {};
  const rawType = typeof rawError.type === 'string' ? rawError.type : '';
  let errorType = CLAUDE_SESSION_ERROR_TYPES.has(rawType) ? rawType : 'unknown_error';
  if (
    (errorType === 'mcp_connection_failed_error' ||
      errorType === 'mcp_authentication_failed_error') &&
    typeof rawError.mcp_server_name !== 'string'
  ) {
    errorType = 'unknown_error';
  }
  if (
    errorType === 'credential_host_unreachable_error' &&
    (typeof rawError.credential_id !== 'string' || typeof rawError.vault_id !== 'string')
  ) {
    errorType = 'unknown_error';
  }
  const retryType =
    rawRetry.type === 'retrying' || rawRetry.type === 'exhausted' || rawRetry.type === 'terminal'
      ? rawRetry.type
      : rawRetry.will_retry === true
        ? 'retrying'
        : 'exhausted';
  const variantFields: Record<string, unknown> = {};
  if (
    (errorType === 'mcp_connection_failed_error' ||
      errorType === 'mcp_authentication_failed_error') &&
    typeof rawError.mcp_server_name === 'string'
  ) {
    variantFields.mcp_server_name = rawError.mcp_server_name;
  }
  if (errorType === 'credential_host_unreachable_error') {
    if (typeof rawError.credential_id === 'string') {
      variantFields.credential_id = rawError.credential_id;
    }
    if (typeof rawError.vault_id === 'string') {
      variantFields.vault_id = rawError.vault_id;
    }
  }
  return {
    ...publicEventBase(event),
    error: {
      type: errorType,
      message: typeof rawError.message === 'string' ? rawError.message : 'Unknown session error',
      retry_status: { type: retryType },
      ...variantFields,
    },
  };
}

function canonicalToolEventId(toolUseId: string): string {
  return toolUseId.startsWith('evt_')
    ? toolUseId
    : `evt_${uuidv5(toolUseId, TOOL_EVENT_ID_NAMESPACE)}`;
}

export function serializeHttpEvent(
  payload: HttpEventInput,
  metadata: {
    id: string;
    type: string;
    producedAt: string;
    producedBy: string;
    processedAt: string | null;
    seq: string;
    subpath?: string;
    filterAgentMessageContent?: boolean;
  },
): HttpEventOutput {
  return {
    ...canonicalizePublicMessagePayload(
      payload,
      metadata.type,
      metadata.filterAgentMessageContent ?? true,
    ),
    id: metadata.id,
    type: metadata.type,
    processed_at: metadata.processedAt,
    produced_at: metadata.producedAt,
    produced_by: metadata.producedBy,
    seq: metadata.seq,
    ...(metadata.subpath ? { subpath: metadata.subpath } : {}),
  };
}

export function eventProcessedAt(
  event: Pick<Event, 'producedAt' | 'producedBy'>,
  payload: HttpEventInput,
): string | null {
  if (event.producedBy !== 'client') return event.producedAt;
  if (payload.processed_at === null) return null;
  return typeof payload.processed_at === 'string' ? payload.processed_at : event.producedAt;
}

const PUBLIC_AGENT_MESSAGE_EVENT_TYPES = new Set([
  AgentEventKind.message,
  AgentThreadEventKind.messageSent,
  AgentThreadEventKind.messageReceived,
]);

const AGENT_TEXT_MESSAGE_EVENT_TYPES = new Set([AgentEventKind.message]);

function canonicalizePublicMessagePayload(
  payload: HttpEventInput,
  type: string,
  filterAgentMessageContent: boolean,
): HttpEventInput {
  const publicPayload = { ...payload };
  delete publicPayload._orca_companion_system_event_id;
  if (type === 'user.message') {
    if (Array.isArray(publicPayload.content)) return publicPayload;
    if (isRecord(publicPayload.message) && Array.isArray(publicPayload.message.content)) {
      return { ...publicPayload, content: publicPayload.message.content };
    }
    return publicPayload;
  }
  if (!PUBLIC_AGENT_MESSAGE_EVENT_TYPES.has(type)) return publicPayload;

  const content = Array.isArray(publicPayload.content)
    ? publicPayload.content
    : isRecord(publicPayload.message) && Array.isArray(publicPayload.message.content)
      ? publicPayload.message.content
      : null;

  const {
    message: _message,
    uuid: _uuid,
    session_id: _sessionId,
    ...canonicalPayload
  } = publicPayload;
  if (!content) return canonicalPayload;
  return {
    ...canonicalPayload,
    content:
      filterAgentMessageContent && AGENT_TEXT_MESSAGE_EVENT_TYPES.has(type)
        ? content.filter((block) => isRecord(block) && block.type === 'text')
        : content,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
