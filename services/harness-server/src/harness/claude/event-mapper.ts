// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { v7 as uuidv7 } from 'uuid';
import { isAgentEventId, isAgentEventSubpath } from '@orca/agent-event-contract';
import type { AgentEventId, AgentEventSubpath } from '@orca/agent-event-contract';
import type { Event } from '@orca/transcript-store';

export interface SessionStoreEntry {
  type: string;
  uuid?: string;
  [k: string]: unknown;
}

export const CLAUDE_SESSION_ENTRY_EVENT_KIND = 'harness.claude.session_entry';

interface ClaudeSessionEntryEnvelope {
  type: typeof CLAUDE_SESSION_ENTRY_EVENT_KIND;
  sdk_entry: SessionStoreEntry;
}

export interface ToEventInput {
  workspaceId: string;
  sessionId: string;
  /** Native SDK subpaths remain open for legacy resume records. */
  subpath: string;
  producedBy: 'harness' | 'client';
  entry: SessionStoreEntry;
  idempotencyKey?: string;
  now?: () => Date;
}

/** Input for client-visible events produced by the local AgentEvent bridge. */
export interface PublicToEventInput extends Omit<ToEventInput, 'subpath'> {
  subpath: AgentEventSubpath;
  /** Explicit canonical AgentEvent envelope identity, separate from payload IDs. */
  eventId: AgentEventId;
}

export function entryToEvent(args: ToEventInput): Event {
  const id = (typeof args.entry.uuid === 'string' && args.entry.uuid) || uuidv7();
  const envelope: ClaudeSessionEntryEnvelope = {
    type: CLAUDE_SESSION_ENTRY_EVENT_KIND,
    sdk_entry: args.entry,
  };
  return {
    id,
    workspaceId: args.workspaceId,
    sessionId: args.sessionId,
    subpath: args.subpath,
    seq: 0,
    producedAt: (args.now?.() ?? new Date()).toISOString(),
    producedBy: args.producedBy,
    kind: CLAUDE_SESSION_ENTRY_EVENT_KIND,
    payload: Buffer.from(JSON.stringify(envelope), 'utf8'),
    idempotencyKey: args.idempotencyKey ?? '',
  };
}

export function publicEntryToEvent(args: PublicToEventInput): Event {
  if (!isAgentEventSubpath(args.subpath)) {
    throw new Error(`invalid public AgentEvent subpath: ${String(args.subpath)}`);
  }
  if (!isAgentEventId(args.eventId)) {
    throw new Error(`invalid public AgentEvent id: ${String(args.eventId)}`);
  }
  return {
    id: args.eventId,
    workspaceId: args.workspaceId,
    sessionId: args.sessionId,
    subpath: args.subpath,
    seq: 0,
    producedAt: (args.now?.() ?? new Date()).toISOString(),
    producedBy: args.producedBy,
    kind: args.entry.type,
    payload: Buffer.from(JSON.stringify(args.entry), 'utf8'),
    idempotencyKey: args.idempotencyKey ?? '',
  };
}

export function eventToEntry(e: Event): SessionStoreEntry {
  const parsed = JSON.parse(Buffer.from(e.payload).toString('utf8')) as unknown;
  if (isClaudeSessionEntryEnvelope(parsed)) return parsed.sdk_entry;
  return parsed as SessionStoreEntry;
}

export function isClaudeSessionEntryEvent(e: Event): boolean {
  if (e.kind !== CLAUDE_SESSION_ENTRY_EVENT_KIND) return false;
  try {
    return isClaudeSessionEntryEnvelope(
      JSON.parse(Buffer.from(e.payload).toString('utf8')) as unknown,
    );
  } catch {
    return false;
  }
}

function isClaudeSessionEntryEnvelope(value: unknown): value is ClaudeSessionEntryEnvelope {
  if (!value || typeof value !== 'object') return false;
  const obj = value as { type?: unknown; sdk_entry?: unknown };
  if (obj.type !== CLAUDE_SESSION_ENTRY_EVENT_KIND) return false;
  if (!obj.sdk_entry || typeof obj.sdk_entry !== 'object') return false;
  return typeof (obj.sdk_entry as { type?: unknown }).type === 'string';
}
