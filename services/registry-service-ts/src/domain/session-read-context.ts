// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, asc, eq, inArray, isNull, or } from 'drizzle-orm';
import { SpanEventKind } from '@orca/agent-event-contract';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  agents,
  agentVersions,
  sessionEventsIndex,
  sessionResources,
  sessionSkillBindings,
  skills,
  skillVersions,
} from '../persistence/postgres/schema.js';
import { createRequestBatch } from './request-batch.js';

interface AgentKey {
  agentId: string;
  version: number;
}

interface BindingKey extends AgentKey {
  sessionId: string;
}

export interface BoundSkillRow {
  ordinal: number;
  source: string;
  skillId: string;
  versionIdentifier: string;
}

export interface OutcomeIndexRow {
  eventId: string;
  kind: string;
  payload: unknown;
  producedAt: string;
}

export interface SessionReadContext {
  readonly workspaceId: string;
  readonly now: Date;
  resources(sessionId: string): Promise<Array<typeof sessionResources.$inferSelect>>;
  agentVersion(key: AgentKey): Promise<{ snapshot: unknown } | null>;
  agent(agentId: string): Promise<typeof agents.$inferSelect | null>;
  bindings(key: BindingKey): Promise<BoundSkillRow[]>;
  outcomes(sessionId: string): Promise<OutcomeIndexRow[]>;
}

const agentKey = (key: AgentKey): string => JSON.stringify([key.agentId, key.version]);
const bindingKey = (key: BindingKey): string =>
  JSON.stringify([key.sessionId, key.agentId, key.version]);

function groupBy<Row>(rows: readonly Row[], keyOf: (row: Row) => string): Map<string, Row[]> {
  const groups = new Map<string, Row[]>();
  for (const row of rows) {
    const key = keyOf(row);
    const group = groups.get(key);
    if (group) group.push(row);
    else groups.set(key, [row]);
  }
  return groups;
}

/**
 * Create once per HTTP read, never globally or across a mutation. Every query
 * carries the authenticated workspace, and associations remain keyed by their
 * full identity (not just the shared Agent version). Separate collections
 * avoid a resources × skills × outcomes join fan-out.
 */
export function createSessionReadContext(
  db: DbClient,
  workspaceId: string,
  orcaBeta: boolean,
  signal?: AbortSignal,
): SessionReadContext {
  return {
    workspaceId,
    now: new Date(),
    resources: createRequestBatch(
      (id: string) => id,
      async (ids) => {
        const rows = await db
          .select()
          .from(sessionResources)
          .where(
            and(
              isNull(sessionResources.deletedAt),
              eq(sessionResources.workspaceId, workspaceId),
              inArray(sessionResources.sessionId, [...ids]),
              isNull(sessionResources.detachedAt),
            ),
          );
        return groupBy(rows, (row) => row.sessionId);
      },
      () => [],
      100,
      signal,
    ),
    agentVersion: createRequestBatch<AgentKey, { snapshot: unknown } | null>(
      agentKey,
      async (keys) => {
        const rows = await db
          .select({
            agentId: agentVersions.agentId,
            version: agentVersions.version,
            snapshot: agentVersions.snapshot,
          })
          .from(agentVersions)
          .where(
            and(
              eq(agentVersions.workspaceId, workspaceId),
              or(
                ...keys.map((key) =>
                  and(
                    eq(agentVersions.agentId, key.agentId),
                    eq(agentVersions.version, key.version),
                  ),
                ),
              ),
            ),
          );
        return new Map(rows.map((row) => [agentKey(row), row]));
      },
      () => null,
      100,
      signal,
    ),
    agent: createRequestBatch<string, typeof agents.$inferSelect | null>(
      (id) => id,
      async (ids) => {
        const rows = await db
          .select()
          .from(agents)
          .where(
            and(
              isNull(agents.deletedAt),
              eq(agents.workspaceId, workspaceId),
              inArray(agents.id, [...ids]),
            ),
          );
        return new Map(rows.map((row) => [row.id, row]));
      },
      () => null,
      100,
      signal,
    ),
    bindings: createRequestBatch<BindingKey, BoundSkillRow[]>(
      bindingKey,
      async (keys) => {
        const rows = await db
          .select({
            sessionId: sessionSkillBindings.sessionId,
            agentId: sessionSkillBindings.agentId,
            version: sessionSkillBindings.agentVersion,
            ordinal: sessionSkillBindings.ordinal,
            source: skills.type,
            skillId: skillVersions.skillId,
            versionIdentifier: skillVersions.versionIdentifier,
          })
          .from(sessionSkillBindings)
          .innerJoin(
            skillVersions,
            and(
              eq(sessionSkillBindings.workspaceId, skillVersions.workspaceId),
              eq(sessionSkillBindings.skillVersionId, skillVersions.id),
              eq(sessionSkillBindings.bundleSha256, skillVersions.packageSha256),
            ),
          )
          .innerJoin(
            skills,
            and(
              eq(skillVersions.workspaceId, skills.workspaceId),
              eq(skillVersions.skillId, skills.id),
            ),
          )
          .where(
            and(
              eq(sessionSkillBindings.workspaceId, workspaceId),
              or(
                ...keys.map((key) =>
                  and(
                    eq(sessionSkillBindings.sessionId, key.sessionId),
                    eq(sessionSkillBindings.agentId, key.agentId),
                    eq(sessionSkillBindings.agentVersion, key.version),
                  ),
                ),
              ),
            ),
          )
          .orderBy(asc(sessionSkillBindings.ordinal));
        return groupBy(rows, bindingKey);
      },
      () => [],
      100,
      signal,
    ),
    outcomes: createRequestBatch<string, OutcomeIndexRow[]>(
      (id) => id,
      async (ids) => {
        const rows = await db
          .select({
            sessionId: sessionEventsIndex.sessionId,
            eventId: sessionEventsIndex.eventId,
            kind: sessionEventsIndex.kind,
            payload: sessionEventsIndex.payload,
            producedAt: sessionEventsIndex.producedAt,
          })
          .from(sessionEventsIndex)
          .where(
            and(
              eq(sessionEventsIndex.workspaceId, workspaceId),
              inArray(sessionEventsIndex.sessionId, [...ids]),
              orcaBeta
                ? eq(sessionEventsIndex.kind, SpanEventKind.outcomeEvaluationEnd)
                : inArray(sessionEventsIndex.kind, [
                    'user.define_outcome',
                    SpanEventKind.outcomeEvaluationEnd,
                  ]),
            ),
          )
          // Preserve each dialect's existing fold order. Grouping does not
          // disturb the relative order of rows within a Session.
          .orderBy(
            asc(sessionEventsIndex.seq),
            ...(orcaBeta ? [] : [asc(sessionEventsIndex.projectionOrdinal)]),
          );
        return groupBy(rows, (row) => row.sessionId);
      },
      () => [],
      100,
      signal,
    ),
  };
}
