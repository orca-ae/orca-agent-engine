// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Map the Claude Agent SDK's `SessionStore` entries to and from transcript-store
// `Event`s, so the SDK's per-turn conversation history persists in (and reloads
// from) the same transcript the rest of Orca reads. On a self-hosted runner that
// substrate is the in-memory, tunnel-fed store (fed by the registry-pushed recovery
// replay); on the harness-server it is the Kafka/Postgres/Pulsar backend. The mapper
// is backend-agnostic — it only translates `SessionStore` entries <-> transcript `Event`s.
//
// The SDK persists each conversation entry as an opaque JSON object (`{ type,
// uuid?, ... }`); we wrap it in a stable envelope event (`harness.claude.
// session_entry`) keyed by the entry's own `uuid` (so a re-append dedups on the
// store's idempotency key) and unwrap it on load. This mirrors the harness-server's
// mapping exactly so a session started on either side reloads identically — the SDK
// reads back the entries verbatim regardless of which component wrote them.

import { v7 as uuidv7 } from 'uuid';
import type { Event } from '@orca/transcript-store-types';

/** One Claude Agent SDK conversation entry (opaque to us apart from `type`/`uuid`). */
export interface SessionStoreEntry {
  type: string;
  uuid?: string;
  [k: string]: unknown;
}

/** The transcript event kind a wrapped SDK session entry is stored under. */
export const CLAUDE_SESSION_ENTRY_EVENT_KIND = 'harness.claude.session_entry';

/** The envelope payload for a stored SDK session entry. */
interface ClaudeSessionEntryEnvelope {
  type: typeof CLAUDE_SESSION_ENTRY_EVENT_KIND;
  sdk_entry: SessionStoreEntry;
}

/** Inputs for {@link entryToEvent}. */
export interface ToEventInput {
  workspaceId: string;
  sessionId: string;
  subpath: string;
  producedBy: 'harness' | 'client';
  entry: SessionStoreEntry;
  idempotencyKey?: string;
  now?: () => Date;
}

/**
 * Wrap one SDK session entry as a transcript {@link Event}. The event id is the
 * entry's own `uuid` when present (so the store dedups a re-append by it), else a
 * fresh uuid v7. `seq` is left 0 — the store stamps the real offset on append.
 */
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

/** Unwrap a stored event back to the SDK session entry it carried. */
export function eventToEntry(e: Event): SessionStoreEntry {
  const parsed = JSON.parse(Buffer.from(e.payload).toString('utf8')) as unknown;
  if (isClaudeSessionEntryEnvelope(parsed)) {
    return parsed.sdk_entry;
  }
  return parsed as SessionStoreEntry;
}

/** Whether an event is a wrapped SDK session entry (the adapter reads only these). */
export function isClaudeSessionEntryEvent(e: Event): boolean {
  if (e.kind !== CLAUDE_SESSION_ENTRY_EVENT_KIND) {
    return false;
  }
  try {
    return isClaudeSessionEntryEnvelope(
      JSON.parse(Buffer.from(e.payload).toString('utf8')) as unknown,
    );
  } catch {
    return false;
  }
}

function isClaudeSessionEntryEnvelope(value: unknown): value is ClaudeSessionEntryEnvelope {
  if (!value || typeof value !== 'object') {
    return false;
  }
  const obj = value as { type?: unknown; sdk_entry?: unknown };
  if (obj.type !== CLAUDE_SESSION_ENTRY_EVENT_KIND) {
    return false;
  }
  if (!obj.sdk_entry || typeof obj.sdk_entry !== 'object') {
    return false;
  }
  return typeof (obj.sdk_entry as { type?: unknown }).type === 'string';
}
