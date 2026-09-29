// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { isNull, and, asc, desc, eq, gt, gte, inArray, lt, lte, or, sql } from 'drizzle-orm';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import {
  AgentEventKind,
  InternalTranscriptEventKind,
  SessionThreadEventKind,
} from '@orca/agent-event-contract';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  agentVersions,
  sessions,
  sessionEventsIndex,
  sessionThreads,
} from '../persistence/postgres/schema.js';
import {
  type HttpEventInput,
  type HttpEventOutput,
  eventProcessedAt,
  isPublicTranscriptEvent,
  serializeHttpEvent,
  transcriptEventVisibility,
} from '../domain/events.js';
import {
  indexableEventsForPublicViews,
  legacyAgentMessageToolProjectionEvents,
  stableSessionThreadId,
} from '../domain/thread-projection.js';

const CATCH_UP_BATCH_SIZE = 1_000;
const DEFAULT_CATCH_UP_MAX_EVENTS = CATCH_UP_BATCH_SIZE;
const HARD_CATCH_UP_MAX_EVENTS = 10_000;
const PROJECTION_BACKFILL_BATCH_SIZE = 500;
export const SESSION_EVENT_PROJECTION_VERSION = 1;
const MAX_SAFE_CURSOR = BigInt(Number.MAX_SAFE_INTEGER);
const USER_EVENT_PROCESSED_KIND_SQL = sql.raw(
  `'${InternalTranscriptEventKind.userEventProcessed}'`,
);

interface CatchUpSessionEventsIndexOptions {
  maxEvents?: number;
}

interface IndexTranscriptEventsFromStoreOptions {
  fromCursor: string;
  eventIds?: readonly string[];
  maxEvents?: number;
  subpath?: string;
}

export async function indexTranscriptEvents(
  db: DbClient,
  events: Event[],
  options: { guardrailSubject?: string } = {},
): Promise<void> {
  if (events.length === 0) return;
  const indexRows = events.flatMap((event) =>
    indexableEventsForPublicViews(event).map(({ event: projectedEvent, projectionOrdinal }) =>
      eventToIndexRow(projectedEvent, projectionOrdinal, options.guardrailSubject),
    ),
  );
  await db
    .insert(sessionEventsIndex)
    .values(indexRows)
    .onConflictDoUpdate({
      target: [
        sessionEventsIndex.workspaceId,
        sessionEventsIndex.sessionId,
        sessionEventsIndex.eventId,
      ],
      // The transcript consumer can race the public request path and index the
      // event before that path attaches its authenticated subject. Fill that
      // Registry-owned field on replay without letting a subject-less replay
      // erase one that is already present.
      set: {
        guardrailSubject: sql`coalesce(${sessionEventsIndex.guardrailSubject}, excluded.guardrail_subject)`,
      },
    });
  await applyUserEventProcessedMarkers(db, events);
  await upsertSessionThreadsFromEvents(db, events);
}

/**
 * Reprojects legacy agent.message rows that embedded tool_use blocks instead
 * of storing standalone agent.tool_use / agent.mcp_tool_use events. Rows stay
 * on projection_version=0 until their derived events are inserted atomically,
 * so readers can preserve the legacy blocks while a backfill is pending.
 */
export async function backfillSessionEventProjections(db: DbClient): Promise<number> {
  let total = 0;
  while (true) {
    const processed = await db.transaction(async (tx) => {
      const rows = await tx
        .select()
        .from(sessionEventsIndex)
        .where(
          and(
            eq(sessionEventsIndex.kind, AgentEventKind.message),
            lt(sessionEventsIndex.projectionVersion, SESSION_EVENT_PROJECTION_VERSION),
          ),
        )
        .orderBy(
          asc(sessionEventsIndex.workspaceId),
          asc(sessionEventsIndex.sessionId),
          asc(sessionEventsIndex.eventId),
        )
        .limit(PROJECTION_BACKFILL_BATCH_SIZE)
        .for('update', { skipLocked: true });
      if (rows.length === 0) return 0;

      const projectedRows = rows.flatMap((row) =>
        legacyAgentMessageToolProjectionEvents(indexRowToEvent(row)).map((event, index) =>
          eventToIndexRow(event, index + 1),
        ),
      );
      if (projectedRows.length > 0) {
        await tx.insert(sessionEventsIndex).values(projectedRows).onConflictDoNothing();
      }

      const rowsBySession = new Map<string, typeof rows>();
      for (const row of rows) {
        const key = `${row.workspaceId}\u0000${row.sessionId}`;
        const group = rowsBySession.get(key);
        if (group) group.push(row);
        else rowsBySession.set(key, [row]);
      }
      for (const group of rowsBySession.values()) {
        const first = group[0]!;
        await tx
          .update(sessionEventsIndex)
          .set({ projectionVersion: SESSION_EVENT_PROJECTION_VERSION })
          .where(
            and(
              eq(sessionEventsIndex.workspaceId, first.workspaceId),
              eq(sessionEventsIndex.sessionId, first.sessionId),
              inArray(
                sessionEventsIndex.eventId,
                group.map((row) => row.eventId),
              ),
            ),
          );
      }
      return rows.length;
    });
    if (processed === 0) return total;
    total += processed;
  }
}

export async function catchUpSessionEventsIndex(
  db: DbClient,
  store: TranscriptStore,
  workspaceId: string,
  sessionId: string,
  opts: CatchUpSessionEventsIndexOptions = {},
): Promise<number> {
  const maxEvents = normalizeCatchUpMaxEvents(opts.maxEvents);
  const fromCursor = await nextSessionEventsIndexCursor(db, workspaceId, sessionId);

  return indexTranscriptEventsFromStore(db, store, workspaceId, sessionId, {
    fromCursor,
    maxEvents,
    subpath: '*',
  });
}

export async function indexTranscriptEventsFromStore(
  db: DbClient,
  store: TranscriptStore,
  workspaceId: string,
  sessionId: string,
  opts: IndexTranscriptEventsFromStoreOptions,
): Promise<number> {
  const batch: Event[] = [];
  const wantedIds = opts.eventIds === undefined ? null : new Set(opts.eventIds);
  if (wantedIds !== null && wantedIds.size === 0) return 0;
  for await (const event of store.read(workspaceId, sessionId, {
    fromCursor: opts.fromCursor,
    maxEvents: opts.maxEvents ?? 0,
    subpath: opts.subpath ?? '*',
  })) {
    if (wantedIds !== null && !wantedIds.has(event.id)) continue;
    batch.push(event);
    if (wantedIds !== null) {
      wantedIds.delete(event.id);
      if (wantedIds.size === 0) break;
    }
  }
  await indexTranscriptEvents(db, batch);
  return batch.length;
}

export async function nextSessionEventsIndexCursor(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
): Promise<string> {
  const rows = await db
    .select({ seq: sessionEventsIndex.seq })
    .from(sessionEventsIndex)
    .where(
      and(
        eq(sessionEventsIndex.workspaceId, workspaceId),
        eq(sessionEventsIndex.sessionId, sessionId),
      ),
    )
    .orderBy(desc(sessionEventsIndex.seq))
    .limit(1);
  if (!rows[0]) return '';
  return String(rows[0].seq + 1);
}

export async function listSessionEventsFromIndex(
  db: DbClient,
  input: {
    workspaceId: string;
    sessionId: string;
    fromCursor: string;
    limit: number;
    subpath: string;
    order?: 'asc' | 'desc';
    createdAtGt?: string;
    createdAtGte?: string;
    createdAtLt?: string;
    createdAtLte?: string;
    types?: readonly string[];
  },
): Promise<{ events: HttpEventOutput[]; nextCursor: string | null }> {
  const cursor = parseCursor(input.fromCursor);
  const order = input.order ?? 'asc';
  const cursorCondition =
    input.fromCursor === ''
      ? undefined
      : cursor.eventId === null
        ? order === 'asc'
          ? gte(sessionEventsIndex.seq, cursor.seq)
          : lte(sessionEventsIndex.seq, cursor.seq)
        : order === 'asc'
          ? or(
              gt(sessionEventsIndex.seq, cursor.seq),
              and(
                eq(sessionEventsIndex.seq, cursor.seq),
                or(
                  gt(sessionEventsIndex.projectionOrdinal, cursor.projectionOrdinal),
                  and(
                    eq(sessionEventsIndex.projectionOrdinal, cursor.projectionOrdinal),
                    gt(sessionEventsIndex.eventId, cursor.eventId),
                  ),
                ),
              ),
            )
          : or(
              lt(sessionEventsIndex.seq, cursor.seq),
              and(
                eq(sessionEventsIndex.seq, cursor.seq),
                or(
                  lt(sessionEventsIndex.projectionOrdinal, cursor.projectionOrdinal),
                  and(
                    eq(sessionEventsIndex.projectionOrdinal, cursor.projectionOrdinal),
                    lt(sessionEventsIndex.eventId, cursor.eventId),
                  ),
                ),
              ),
            );
  const conditions = [
    eq(sessionEventsIndex.workspaceId, input.workspaceId),
    eq(sessionEventsIndex.sessionId, input.sessionId),
    eq(sessionEventsIndex.visibility, 'public'),
    cursorCondition,
    input.createdAtGt ? gt(sessionEventsIndex.producedAt, input.createdAtGt) : undefined,
    input.createdAtGte ? gte(sessionEventsIndex.producedAt, input.createdAtGte) : undefined,
    input.createdAtLt ? lt(sessionEventsIndex.producedAt, input.createdAtLt) : undefined,
    input.createdAtLte ? lte(sessionEventsIndex.producedAt, input.createdAtLte) : undefined,
    input.types && input.types.length > 0
      ? inArray(sessionEventsIndex.kind, [...input.types])
      : undefined,
  ];
  if (input.subpath !== '*') {
    conditions.push(eq(sessionEventsIndex.subpath, input.subpath));
  }

  const rows = await db
    .select()
    .from(sessionEventsIndex)
    .where(and(...conditions))
    .orderBy(
      order === 'asc' ? asc(sessionEventsIndex.seq) : desc(sessionEventsIndex.seq),
      order === 'asc'
        ? asc(sessionEventsIndex.projectionOrdinal)
        : desc(sessionEventsIndex.projectionOrdinal),
      order === 'asc' ? asc(sessionEventsIndex.eventId) : desc(sessionEventsIndex.eventId),
    )
    .limit(input.limit + 1);

  const page = rows.slice(0, input.limit);
  const last = page[page.length - 1] ?? null;
  return {
    events: page.map(indexRowToHttpEvent),
    nextCursor:
      rows.length > input.limit && last
        ? encodeCursor(last.seq, last.projectionOrdinal, last.eventId)
        : null,
  };
}

export function indexRowToHttpEvent(row: typeof sessionEventsIndex.$inferSelect): HttpEventOutput {
  const payload = isRecord(row.payload) ? (row.payload as HttpEventInput) : { type: row.kind };
  return serializeHttpEvent(payload, {
    id: row.eventId,
    type: row.kind,
    // Omit `subpath` for the parent stream ("") rather than setting `undefined`
    // (exactOptionalPropertyTypes); a subagent trace keeps its `subagents/<id>`.
    ...(row.subpath ? { subpath: row.subpath } : {}),
    producedAt: row.producedAt,
    producedBy: row.producedBy,
    filterAgentMessageContent:
      row.kind !== AgentEventKind.message ||
      row.projectionVersion === undefined ||
      row.projectionVersion >= SESSION_EVENT_PROJECTION_VERSION,
    processedAt:
      row.processedAt ??
      eventProcessedAt(
        { producedAt: row.producedAt, producedBy: row.producedBy as Event['producedBy'] },
        payload,
      ),
    seq: String(row.seq),
  });
}

function eventToIndexRow(
  event: Event,
  projectionOrdinal: number,
  guardrailSubject?: string,
): typeof sessionEventsIndex.$inferInsert {
  const payload = parsePayload(event);
  return {
    workspaceId: event.workspaceId,
    sessionId: event.sessionId,
    seq: event.seq,
    projectionOrdinal,
    projectionVersion: SESSION_EVENT_PROJECTION_VERSION,
    eventId: event.id,
    subpath: event.subpath ?? '',
    processedAt: eventProcessedAt(event, payload),
    producedAt: event.producedAt,
    producedBy: event.producedBy,
    kind: event.kind,
    visibility: isPublicTranscriptEvent(event) ? 'public' : transcriptEventVisibility(event.kind),
    payload,
    ...(guardrailSubject && event.producedBy === 'client' && event.kind.startsWith('user.')
      ? { guardrailSubject }
      : {}),
  };
}

function indexRowToEvent(row: typeof sessionEventsIndex.$inferSelect): Event {
  const payload = isRecord(row.payload) ? row.payload : { type: row.kind };
  return {
    id: row.eventId,
    workspaceId: row.workspaceId,
    sessionId: row.sessionId,
    subpath: row.subpath,
    seq: row.seq,
    producedAt: row.producedAt,
    producedBy: row.producedBy as Event['producedBy'],
    kind: row.kind,
    payload: Buffer.from(JSON.stringify(payload), 'utf8'),
    idempotencyKey: '',
  };
}

async function applyUserEventProcessedMarkers(db: DbClient, events: Event[]): Promise<void> {
  const targets = new Map<
    string,
    { workspaceId: string; sessionId: string; userEventId: string }
  >();
  for (const event of events) {
    if (
      event.kind === InternalTranscriptEventKind.userEventProcessed ||
      event.kind === 'session.deferred_user_message_submitted'
    ) {
      const payload = parsePayload(event);
      const userEventId = payload.user_event_id;
      if (typeof userEventId !== 'string' || userEventId.length === 0) continue;
      targets.set(`${event.workspaceId}\u0000${event.sessionId}\u0000${userEventId}`, {
        workspaceId: event.workspaceId,
        sessionId: event.sessionId,
        userEventId,
      });
      continue;
    }

    // A marker can be projected before its target row on Postgres/Pulsar when
    // multiple projector replicas work concurrently. Reconcile from both
    // directions so projecting the target later still finds the durable
    // marker already stored in this read model.
    if (event.producedBy === 'client' && event.kind.startsWith('user.')) {
      targets.set(`${event.workspaceId}\u0000${event.sessionId}\u0000${event.id}`, {
        workspaceId: event.workspaceId,
        sessionId: event.sessionId,
        userEventId: event.id,
      });
    }
  }

  if (targets.size === 0) return;

  // Derive from the earliest marker in transcript order, not whichever
  // projector replica happens to run first. Compute every requested marker
  // once in a materialized CTE and update the batch in one statement. The
  // legacy deferred-submitted marker remains a rollout fallback; a new
  // acceptance marker has the lower seq and therefore wins once both exist.
  const requestedRows = [...targets.values()].map(
    (target) => sql`(${target.workspaceId}, ${target.sessionId}, ${target.userEventId})`,
  );
  await db.execute(sql`
    WITH requested(workspace_id, session_id, user_event_id) AS (
      VALUES ${sql.join(requestedRows, sql`, `)}
    ), earliest_marker AS MATERIALIZED (
      SELECT DISTINCT ON (
        requested.workspace_id,
        requested.session_id,
        requested.user_event_id
      )
        requested.workspace_id,
        requested.session_id,
        requested.user_event_id,
        marker.seq,
        marker.produced_at
      FROM requested
      JOIN session_events_index AS marker
        ON marker.workspace_id = requested.workspace_id
       AND marker.session_id = requested.session_id
       AND marker.payload->>'user_event_id' = requested.user_event_id
      WHERE marker.kind IN (
        ${USER_EVENT_PROCESSED_KIND_SQL},
        'session.deferred_user_message_submitted'
      )
      ORDER BY
        requested.workspace_id,
        requested.session_id,
        requested.user_event_id,
        marker.seq ASC,
        marker.projection_ordinal ASC,
        marker.event_id ASC
    )
    UPDATE session_events_index AS target
    SET
      processed_at = earliest_marker.produced_at,
      processed_marker_seq = earliest_marker.seq
    FROM earliest_marker
    WHERE target.workspace_id = earliest_marker.workspace_id
      AND target.session_id = earliest_marker.session_id
      AND target.produced_by = 'client'
      AND (
        target.event_id = earliest_marker.user_event_id
        OR (
          target.projection_ordinal > 0
          AND target.payload->>'source_event_id' = earliest_marker.user_event_id
        )
      )
      AND (
        target.processed_marker_seq IS NULL
        OR earliest_marker.seq < target.processed_marker_seq
      )
  `);
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

interface SessionThreadMutation {
  source: 'lifecycle' | 'subpath';
  id: string;
  workspaceId: string;
  sessionId: string;
  subpath: string;
  agentId?: string | undefined;
  agentVersion?: number | undefined;
  agentName?: string | undefined;
  agentType?: string | undefined;
  status: 'idle' | 'running' | 'rescheduling' | 'terminated';
  stopReason?: string | null;
  updatedAt: Date;
}

interface ThreadAgentMetadata {
  agentId: string;
  agentVersion: number;
  agentName: string;
}

async function upsertSessionThreadsFromEvents(db: DbClient, events: Event[]): Promise<void> {
  const sessionDefaults = new Map<string, { agentId: string; agentVersion: number } | null>();
  const sessionSubagentRosters = new Map<string, Map<string, ThreadAgentMetadata>>();
  for (const event of events) {
    const mutation = extractSessionThreadMutation(event) ?? extractSubpathThreadMutation(event);
    if (!mutation) continue;

    const cacheKey = `${mutation.workspaceId}:${mutation.sessionId}`;
    let defaults = sessionDefaults.get(cacheKey);
    if (defaults === undefined) {
      defaults = await loadSessionAgentDefaults(db, mutation.workspaceId, mutation.sessionId);
      sessionDefaults.set(cacheKey, defaults);
    }

    let rosterMatch: ThreadAgentMetadata | undefined;
    if (mutation.agentType) {
      let roster = sessionSubagentRosters.get(cacheKey);
      if (!roster) {
        roster = await loadSessionSubagentRoster(
          db,
          mutation.workspaceId,
          mutation.sessionId,
          defaults,
        );
        sessionSubagentRosters.set(cacheKey, roster);
      }
      rosterMatch = roster.get(mutation.agentType);
    }

    const explicitAgentId = mutation.agentId ?? rosterMatch?.agentId;
    const explicitAgentVersion = mutation.agentVersion ?? rosterMatch?.agentVersion;
    const explicitAgentName = mutation.agentName ?? rosterMatch?.agentName;
    const agentId = explicitAgentId ?? defaults?.agentId;
    const agentVersion = explicitAgentVersion ?? defaults?.agentVersion;
    if (!agentId || !agentVersion) continue;
    const agentName = explicitAgentName ?? agentId;
    const hasExplicitAgent = Boolean(explicitAgentId && explicitAgentVersion);

    await db
      .insert(sessionThreads)
      .values({
        id: mutation.id,
        workspaceId: mutation.workspaceId,
        sessionId: mutation.sessionId,
        subpath: mutation.subpath,
        agentId,
        agentVersion,
        agentName,
        parentThreadId:
          mutation.subpath === ''
            ? null
            : stableSessionThreadId(mutation.workspaceId, mutation.sessionId, ''),
        status: mutation.status,
        stopReason: mutation.stopReason ?? null,
        archivedAt: null,
        createdAt: mutation.updatedAt,
        updatedAt: mutation.updatedAt,
      })
      .onConflictDoUpdate({
        target: [sessionThreads.workspaceId, sessionThreads.sessionId, sessionThreads.subpath],
        set:
          mutation.source === 'lifecycle'
            ? {
                id: mutation.id,
                parentThreadId:
                  mutation.subpath === ''
                    ? null
                    : stableSessionThreadId(mutation.workspaceId, mutation.sessionId, ''),
                status: mutation.status,
                stopReason: mutation.stopReason ?? null,
                updatedAt: mutation.updatedAt,
              }
            : hasExplicitAgent
              ? {
                  agentId,
                  agentVersion,
                  agentName,
                  updatedAt: mutation.updatedAt,
                }
              : {
                  updatedAt: mutation.updatedAt,
                },
      });
  }
}

function extractSessionThreadMutation(event: Event): SessionThreadMutation | null {
  const status = sessionThreadStatusForEventKind(event.kind);
  if (!status) return null;
  if (event.producedBy !== 'registry' && event.producedBy !== 'harness') return null;

  const payload = parsePayload(event);
  const thread = isRecord(payload.thread) ? payload.thread : {};
  const explicitId =
    readString(payload, ['session_thread_id', 'thread_id', 'id']) ?? readString(thread, ['id']);
  const subpath =
    readString(payload, ['subpath']) ??
    readString(thread, ['subpath']) ??
    event.subpath ??
    (explicitId ? `threads/${explicitId}` : '');
  const id = subpath
    ? stableSessionThreadId(event.workspaceId, event.sessionId, subpath)
    : explicitId;
  if (!id) return null;
  const agent = isRecord(thread.agent) ? thread.agent : {};
  const agentId =
    readString(payload, ['agent_id']) ??
    readString(thread, ['agent_id']) ??
    readString(agent, ['id']);
  const agentVersion =
    readPositiveInteger(payload, ['agent_version']) ??
    readPositiveInteger(thread, ['agent_version']) ??
    readPositiveInteger(agent, ['version']);
  const agentName =
    readString(payload, ['agent_name']) ??
    readString(thread, ['agent_name', 'name']) ??
    readString(agent, ['name']);
  return {
    source: 'lifecycle',
    id,
    workspaceId: event.workspaceId,
    sessionId: event.sessionId,
    subpath,
    agentId,
    agentVersion,
    agentName,
    status,
    stopReason: readString(payload, ['stop_reason']) ?? readString(thread, ['stop_reason']) ?? null,
    updatedAt: parseEventDate(event.producedAt),
  };
}

function extractSubpathThreadMutation(event: Event): SessionThreadMutation | null {
  if (!event.subpath) return null;
  const payload = parsePayload(event);
  const sdkEntry = isRecord(payload.sdk_entry) ? payload.sdk_entry : {};
  return {
    source: 'subpath',
    id: stableSessionThreadId(event.workspaceId, event.sessionId, event.subpath),
    workspaceId: event.workspaceId,
    sessionId: event.sessionId,
    subpath: event.subpath,
    agentType: readString(sdkEntry, ['agentType', 'agent_type', 'subagent_type']),
    status: 'idle',
    stopReason: null,
    updatedAt: parseEventDate(event.producedAt),
  };
}

function sessionThreadStatusForEventKind(kind: string): SessionThreadMutation['status'] | null {
  if (kind === SessionThreadEventKind.created) return 'idle';
  if (kind === SessionThreadEventKind.statusRunning) return 'running';
  if (kind === SessionThreadEventKind.statusRescheduled) return 'rescheduling';
  if (kind === SessionThreadEventKind.statusIdle) return 'idle';
  if (kind === SessionThreadEventKind.statusTerminated) return 'terminated';
  return null;
}

async function loadSessionAgentDefaults(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
): Promise<{ agentId: string; agentVersion: number } | null> {
  const rows = await db
    .select({ agentId: sessions.agentId, agentVersion: sessions.agentVersion })
    .from(sessions)
    .where(
      and(
        isNull(sessions.deletedAt),
        eq(sessions.workspaceId, workspaceId),
        eq(sessions.id, sessionId),
      ),
    )
    .limit(1);
  return rows[0] ?? null;
}

async function loadSessionSubagentRoster(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  defaults: { agentId: string; agentVersion: number } | null,
): Promise<Map<string, ThreadAgentMetadata>> {
  const out = new Map<string, ThreadAgentMetadata>();
  const sessionAgent = defaults ?? (await loadSessionAgentDefaults(db, workspaceId, sessionId));
  if (!sessionAgent) return out;

  const coordinatorSnapshot = await loadAgentVersionSnapshot(
    db,
    workspaceId,
    sessionAgent.agentId,
    sessionAgent.agentVersion,
  );
  const multiagent = isRecord(coordinatorSnapshot?.multiagent)
    ? coordinatorSnapshot.multiagent
    : null;
  const refs =
    multiagent?.type === 'coordinator' && Array.isArray(multiagent.agents) ? multiagent.agents : [];

  const used = new Set<string>();
  for (const ref of refs) {
    if (!isRecord(ref) || ref.type !== 'agent' || typeof ref.id !== 'string') continue;
    const version =
      typeof ref.version === 'number' && Number.isInteger(ref.version) ? ref.version : null;
    if (version === null) continue;
    const snapshot = await loadAgentVersionSnapshot(db, workspaceId, ref.id, version);
    const name =
      typeof snapshot?.name === 'string' && snapshot.name.length > 0 ? snapshot.name : ref.id;
    out.set(uniqueSubagentKey(name, ref.id, used), {
      agentId: ref.id,
      agentVersion: version,
      agentName: name,
    });
  }
  return out;
}

async function loadAgentVersionSnapshot(
  db: DbClient,
  workspaceId: string,
  agentId: string,
  version: number,
): Promise<Record<string, unknown> | null> {
  const rows = await db
    .select({ snapshot: agentVersions.snapshot })
    .from(agentVersions)
    .where(
      and(
        eq(agentVersions.workspaceId, workspaceId),
        eq(agentVersions.agentId, agentId),
        eq(agentVersions.version, version),
      ),
    )
    .limit(1);
  return (rows[0]?.snapshot as Record<string, unknown> | undefined) ?? null;
}

function uniqueSubagentKey(name: string, id: string, used: Set<string>): string {
  const base =
    name
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, '-')
      .replace(/^-+|-+$/g, '') || id;
  let key = base;
  let counter = 2;
  while (used.has(key)) {
    key = `${base}-${counter}`;
    counter += 1;
  }
  used.add(key);
  return key;
}

function readString(record: Record<string, unknown>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === 'string' && value.length > 0) return value;
  }
  return undefined;
}

function readPositiveInteger(record: Record<string, unknown>, keys: string[]): number | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === 'number' && Number.isInteger(value) && value > 0) return value;
  }
  return undefined;
}

function parseEventDate(value: string): Date {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? new Date() : date;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function parseCursor(cursor: string): {
  seq: number;
  projectionOrdinal: number;
  eventId: string | null;
} {
  if (cursor === '') return { seq: 0, projectionOrdinal: 0, eventId: null };
  if (!isValidSessionEventsCursor(cursor)) throw new Error(`invalid cursor: ${cursor}`);
  const parts = cursor.split(':');
  if (parts.length === 1) {
    return { seq: Number(parts[0]), projectionOrdinal: 0, eventId: null };
  }
  if (parts.length === 2) {
    return { seq: Number(parts[0]), projectionOrdinal: 0, eventId: parts[1]! };
  }
  return {
    seq: Number(parts[0]),
    projectionOrdinal: Number(parts[1]),
    eventId: parts[2]!,
  };
}

export function isValidSessionEventsCursor(cursor: string): boolean {
  if (cursor === '') return true;
  const parts = cursor.split(':');
  if (parts.length < 1 || parts.length > 3) return false;
  const [seq] = parts;
  if (!seq || !/^\d+$/.test(seq)) return false;
  if (BigInt(seq) > MAX_SAFE_CURSOR) return false;
  if (parts.length === 2) return parts[1]!.length > 0;
  if (parts.length === 3) {
    const projectionOrdinal = parts[1]!;
    const eventId = parts[2]!;
    if (!/^\d+$/.test(projectionOrdinal)) return false;
    if (BigInt(projectionOrdinal) > MAX_SAFE_CURSOR) return false;
    return eventId.length > 0;
  }
  return true;
}

function encodeCursor(seq: number, projectionOrdinal: number, eventId: string): string {
  return `${seq}:${projectionOrdinal}:${eventId}`;
}

function normalizeCatchUpMaxEvents(maxEvents: number | undefined): number {
  if (maxEvents === undefined) return DEFAULT_CATCH_UP_MAX_EVENTS;
  if (!Number.isInteger(maxEvents) || maxEvents <= 0) return DEFAULT_CATCH_UP_MAX_EVENTS;
  return Math.min(maxEvents, HARD_CATCH_UP_MAX_EVENTS);
}
