// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { v5 as uuidv5 } from 'uuid';
import type { Event } from '@orca/transcript-store';
import {
  AgentEventKind,
  AgentThreadEventKind,
  SessionThreadEventKind,
} from '@orca/agent-event-contract';
import { isPublicTranscriptEvent, type HttpEventInput } from './events.js';

const PRIMARY_PROJECTION_NAMESPACE = 'cb278008-bd9d-4f6e-b655-b43f8f6331ea';
const THREAD_PUBLIC_PROJECTION_NAMESPACE = 'd3d7a30e-90bd-4834-b167-7374690768ae';
const LEGACY_AGENT_MESSAGE_TOOL_PROJECTION_NAMESPACE = '3114be80-4700-47d8-a6c7-d757b3932376';
export const SESSION_THREAD_ID_NAMESPACE = 'ea82f5c2-b759-4717-9727-51b3ef079bef';

export interface IndexablePublicViewEvent {
  event: Event;
  projectionOrdinal: number;
}

export function indexableEventsForPublicViews(event: Event): IndexablePublicViewEvent[] {
  return [
    event,
    ...threadPublicProjectionEvents(event),
    ...primaryThreadProjectionEvents(event),
  ].map((projectedEvent, projectionOrdinal) => ({ event: projectedEvent, projectionOrdinal }));
}

export function streamableEventsForSubpath(event: Event, requestedSubpath: string): Event[] {
  const eventSubpath = event.subpath ?? '';
  // Deletion terminates every open session and thread stream, regardless of
  // which transcript subpath the client subscribed to.
  if (event.kind === 'session.deleted') return isPublicTranscriptEvent(event) ? [event] : [];
  if (requestedSubpath === '') {
    if (eventSubpath === '') return isPublicTranscriptEvent(event) ? [event] : [];
    return primaryThreadProjectionEvents(event);
  }
  if (eventSubpath !== requestedSubpath) return [];
  if (isPublicTranscriptEvent(event)) return [event];
  return threadPublicProjectionEvents(event);
}

export function primaryThreadProjectionEvents(event: Event): Event[] {
  const subpath = event.subpath ?? '';
  if (!subpath) return [];

  const source = sourcePayloadForProjection(event);
  if (!source) return [];

  const projectedKind = primaryProjectionKind(event.kind, source.type, event.producedBy);
  if (!projectedKind) return [];

  const payload: HttpEventInput = {
    ...source.payload,
    type: projectedKind,
    session_thread_id: threadIdForEvent(event, source.payload),
    source_event_id: event.id,
    source_event_type: event.kind,
    source_subpath: subpath,
  };

  return [
    {
      ...event,
      id: projectedEventId(event, projectedKind, PRIMARY_PROJECTION_NAMESPACE),
      subpath: '',
      kind: projectedKind,
      payload: Buffer.from(JSON.stringify(payload), 'utf8'),
      idempotencyKey: `${event.id}:primary:${projectedKind}`,
    },
  ];
}

export function threadPublicProjectionEvents(event: Event): Event[] {
  const subpath = event.subpath ?? '';
  if (!subpath || event.kind !== 'harness.claude.session_entry') return [];

  const entry = sdkEntryPayload(event);
  if (!entry) return [];
  const projections = publicSdkEntryProjections(entry);
  const sessionThreadId = threadIdForEvent(event, entry);
  return projections.map(({ kind, payload: projectedPayload, discriminator }) => {
    const payload: HttpEventInput = {
      ...projectedPayload,
      type: kind,
      session_thread_id: sessionThreadId,
      source_event_id: event.id,
      source_event_type: event.kind,
    };
    return {
      ...event,
      id: projectedEventId(event, kind, THREAD_PUBLIC_PROJECTION_NAMESPACE, discriminator),
      kind,
      payload: Buffer.from(JSON.stringify(payload), 'utf8'),
      idempotencyKey: `${event.id}:thread:${kind}:${discriminator}`,
    };
  });
}

/**
 * Reconstruct standalone tool events from pre-canonicalization agent.message
 * rows already stored in session_events_index. New harness events already
 * emit tool calls separately; this only serves the projection backfill.
 */
export function legacyAgentMessageToolProjectionEvents(event: Event): Event[] {
  if (event.kind !== AgentEventKind.message) return [];
  const payload = parsePayload(event);
  const content = entryContent(payload);
  if (!content) return [];

  const parentToolUseId = payload.parent_tool_use_id;
  const correlation =
    typeof parentToolUseId === 'string' || parentToolUseId === null
      ? { parent_tool_use_id: parentToolUseId }
      : {};
  const sourceEventId =
    typeof payload.source_event_id === 'string' ? payload.source_event_id : event.id;
  const sourceEventType =
    typeof payload.source_event_type === 'string' ? payload.source_event_type : event.kind;
  const isThreadProjection =
    sourceEventId !== event.id && sourceEventType === 'harness.claude.session_entry';
  const projectionSource = isThreadProjection ? { ...event, id: sourceEventId } : event;
  const namespace = isThreadProjection
    ? THREAD_PUBLIC_PROJECTION_NAMESPACE
    : LEGACY_AGENT_MESSAGE_TOOL_PROJECTION_NAMESPACE;

  return publicToolProjections(content, correlation).map(
    ({ kind, payload: projectedPayload, discriminator }) => {
      const projected: HttpEventInput = {
        ...projectedPayload,
        type: kind,
        ...(typeof payload.session_thread_id === 'string'
          ? { session_thread_id: payload.session_thread_id }
          : {}),
        source_event_id: sourceEventId,
        source_event_type: sourceEventType,
      };
      return {
        ...event,
        id: projectedEventId(projectionSource, kind, namespace, discriminator),
        kind,
        payload: Buffer.from(JSON.stringify(projected), 'utf8'),
        idempotencyKey: isThreadProjection
          ? `${sourceEventId}:thread:${kind}:${discriminator}`
          : `${event.id}:legacy:${kind}:${discriminator}`,
      };
    },
  );
}

function sourcePayloadForProjection(
  event: Event,
): { type: string; payload: HttpEventInput } | null {
  if (event.kind === 'harness.claude.session_entry') {
    const entry = sdkEntryPayload(event);
    if (!entry) return null;
    const payload = publicSdkMessagePayload(entry);
    if (!payload) return null;
    return { type: entry.type, payload };
  }

  const payload = parsePayload(event);
  return { type: event.kind, payload };
}

function primaryProjectionKind(
  eventKind: string,
  sourceType: string,
  producedBy: Event['producedBy'],
): string | null {
  if (isSessionThreadLifecycleKind(eventKind)) return eventKind;
  if (eventKind === AgentEventKind.message) return AgentThreadEventKind.messageReceived;
  if (eventKind === 'user.message') return AgentThreadEventKind.messageSent;
  if (eventKind === 'harness.claude.session_entry') {
    if (sourceType === 'assistant') return AgentThreadEventKind.messageReceived;
    if (sourceType === 'user') return AgentThreadEventKind.messageSent;
  }
  if (producedBy === 'harness' && eventKind === AgentEventKind.customToolUse) {
    return AgentThreadEventKind.messageSent;
  }
  return null;
}

function isSessionThreadLifecycleKind(kind: string): boolean {
  return (
    kind === SessionThreadEventKind.created ||
    kind === SessionThreadEventKind.statusRunning ||
    kind === SessionThreadEventKind.statusIdle ||
    kind === SessionThreadEventKind.statusRescheduled ||
    kind === SessionThreadEventKind.statusTerminated
  );
}

function sdkEntryPayload(event: Event): HttpEventInput | null {
  const payload = parsePayload(event);
  const sdkEntry = payload.sdk_entry;
  if (!isRecord(sdkEntry)) return null;
  const type = sdkEntry['type'];
  if (typeof type !== 'string') return null;
  return sdkEntry as HttpEventInput;
}

function publicSdkMessagePayload(entry: HttpEventInput): HttpEventInput | null {
  const content = entryContent(entry);
  if (!content) return null;
  const parentToolUseId = entry.parent_tool_use_id;
  const correlation =
    typeof parentToolUseId === 'string' || parentToolUseId === null
      ? { parent_tool_use_id: parentToolUseId }
      : {};
  if (entry.type === 'assistant') {
    const textBlocks = content.filter((block) => isRecord(block) && block.type === 'text');
    return textBlocks.length > 0 ? { type: entry.type, content: textBlocks, ...correlation } : null;
  }
  if (entry.type === 'user') return { type: entry.type, content, ...correlation };
  return null;
}

function publicSdkEntryProjections(
  entry: HttpEventInput,
): Array<{ kind: string; payload: HttpEventInput; discriminator: string }> {
  const content = entryContent(entry);
  if (!content) return [];
  const parentToolUseId = entry.parent_tool_use_id;
  const correlation =
    typeof parentToolUseId === 'string' || parentToolUseId === null
      ? { parent_tool_use_id: parentToolUseId }
      : {};
  if (entry.type === 'user') {
    return [
      {
        kind: 'user.message',
        payload: { type: entry.type, content, ...correlation },
        discriminator: '',
      },
    ];
  }
  if (entry.type !== 'assistant') return [];

  const projections: Array<{ kind: string; payload: HttpEventInput; discriminator: string }> = [];
  const textBlocks = content.filter((block) => isRecord(block) && block.type === 'text');
  if (textBlocks.length > 0) {
    projections.push({
      kind: AgentEventKind.message,
      payload: { type: entry.type, content: textBlocks, ...correlation },
      discriminator: '',
    });
  }
  projections.push(...publicToolProjections(content, correlation));
  return projections;
}

function publicToolProjections(
  content: unknown[],
  correlation: Record<string, unknown>,
): Array<{ kind: string; payload: HttpEventInput; discriminator: string }> {
  const projections: Array<{ kind: string; payload: HttpEventInput; discriminator: string }> = [];
  content.forEach((block, index) => {
    if (!isRecord(block)) return;
    const blockType = block.type;
    if (
      blockType !== 'tool_use' &&
      blockType !== 'server_tool_use' &&
      blockType !== 'mcp_tool_use'
    ) {
      return;
    }
    const toolUseId = block.id;
    const rawName = block.name;
    if (typeof toolUseId !== 'string' || typeof rawName !== 'string') return;
    const remote = remoteMcpToolParts(rawName);
    const explicitServerName =
      typeof block.mcp_server_name === 'string'
        ? block.mcp_server_name
        : typeof block.server_name === 'string'
          ? block.server_name
          : null;
    const mcpServerName = remote?.serverName ?? explicitServerName;
    const isMcp = mcpServerName ? mcpServerName !== 'orca' : blockType === 'mcp_tool_use';
    projections.push({
      kind: isMcp ? AgentEventKind.mcpToolUse : AgentEventKind.toolUse,
      payload: {
        type: blockType,
        name: isMcp ? (remote?.toolName ?? rawName) : rawName,
        input: isRecord(block.input) ? block.input : {},
        tool_use_id: toolUseId,
        ...(isMcp && mcpServerName ? { mcp_server_name: mcpServerName } : {}),
        ...correlation,
      },
      discriminator: `tool:${index}:${toolUseId}`,
    });
  });
  return projections;
}

function remoteMcpToolParts(name: string): { serverName: string; toolName: string } | null {
  if (!name.startsWith('mcp__')) return null;
  const separator = name.indexOf('__', 'mcp__'.length);
  if (separator < 0) return null;
  const serverName = name.slice('mcp__'.length, separator);
  const toolName = name.slice(separator + 2);
  return serverName && toolName ? { serverName, toolName } : null;
}

function entryContent(entry: HttpEventInput): unknown[] | null {
  const message = entry.message;
  if (isRecord(message)) {
    const messageContent = contentBlocks(message.content);
    if (messageContent) return messageContent;
  }
  return contentBlocks(entry.content);
}

function contentBlocks(content: unknown): unknown[] | null {
  if (Array.isArray(content)) return content;
  if (typeof content === 'string') return [{ type: 'text', text: content }];
  return null;
}

function parsePayload(event: Event): HttpEventInput {
  try {
    const parsed = JSON.parse(Buffer.from(event.payload).toString('utf8')) as unknown;
    if (isRecord(parsed)) return parsed as HttpEventInput;
  } catch {
    // Fall through to the minimal envelope below.
  }
  return { type: event.kind };
}

function threadIdForEvent(event: Event, payload: Record<string, unknown>): string {
  const thread = isRecord(payload.thread) ? payload.thread : {};
  const subpath =
    readString(payload, ['subpath']) ?? readString(thread, ['subpath']) ?? event.subpath ?? '';
  if (subpath) return stableSessionThreadId(event.workspaceId, event.sessionId, subpath);
  return (
    readString(payload, ['session_thread_id', 'thread_id']) ??
    readString(thread, ['id']) ??
    stableSessionThreadId(event.workspaceId, event.sessionId, event.subpath ?? '')
  );
}

function projectedEventId(
  event: Event,
  kind: string,
  namespace: string,
  discriminator = '',
): string {
  const source = `${event.workspaceId}:${event.sessionId}:${event.id}:${kind}`;
  return `evt_${uuidv5(discriminator ? `${source}:${discriminator}` : source, namespace)}`;
}

export function stableSessionThreadId(
  workspaceId: string,
  sessionId: string,
  subpath: string,
): string {
  return `sth_${uuidv5(`${workspaceId}:${sessionId}:${subpath}`, SESSION_THREAD_ID_NAMESPACE)}`;
}

function readString(record: Record<string, unknown>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === 'string' && value.length > 0) return value;
  }
  return undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
