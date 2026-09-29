// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, asc, eq } from 'drizzle-orm';
import { SpanEventKind, type OutcomeEvaluationResult } from '@orca/agent-event-contract';
import type { DbClient } from '../persistence/postgres/client.js';
import { sessionEventsIndex } from '../persistence/postgres/schema.js';

/**
 * Session outcome projection.
 *
 * The harness evaluates every defined outcome after each turn and emits a
 * `span.outcome_evaluation_end { outcome_id, result, explanation, iteration, usage }`
 * event. This module folds those span events (already indexed in
 * `session_events_index`) into the session's `outcome` + `outcome_evaluations`
 * views served by `GET /v1/sessions/:id` and `GET /v1/sessions/:id/outcome`.
 */

const OUTCOME_EVAL_END_KIND = SpanEventKind.outcomeEvaluationEnd;

export interface OutcomeEvaluationRecord {
  outcome_id: string;
  result: OutcomeEvaluationResult;
  explanation: string;
  iteration: number;
  usage?: unknown;
}

export interface SessionOutcomeView {
  /** The most recent evaluation across all outcomes, or null if none. */
  outcome: OutcomeEvaluationRecord | null;
  /** Latest evaluation per defined outcome, in first-seen order. */
  outcome_evaluations: OutcomeEvaluationRecord[];
}

export async function getSessionOutcome(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
): Promise<SessionOutcomeView> {
  const rows = await db
    .select({ payload: sessionEventsIndex.payload, seq: sessionEventsIndex.seq })
    .from(sessionEventsIndex)
    .where(
      and(
        eq(sessionEventsIndex.workspaceId, workspaceId),
        eq(sessionEventsIndex.sessionId, sessionId),
        eq(sessionEventsIndex.kind, OUTCOME_EVAL_END_KIND),
      ),
    )
    .orderBy(asc(sessionEventsIndex.seq));

  return foldOutcomeEvaluations(rows.map((row) => row.payload));
}

/**
 * Fold ordered `span.outcome_evaluation_end` payloads into the session outcome
 * view: latest verdict per outcome (later overrides earlier), plus the single
 * most-recent verdict overall as `outcome`. Pure so it is unit-testable.
 */
export function foldOutcomeEvaluations(payloads: unknown[]): SessionOutcomeView {
  const latest = new Map<string, OutcomeEvaluationRecord>();
  let mostRecent: OutcomeEvaluationRecord | null = null;
  for (const payload of payloads) {
    const record = parseOutcomeRecord(payload);
    if (!record) continue;
    latest.set(record.outcome_id, record);
    mostRecent = record;
  }
  return {
    outcome: mostRecent,
    outcome_evaluations: [...latest.values()],
  };
}

function parseOutcomeRecord(payload: unknown): OutcomeEvaluationRecord | null {
  if (!payload || typeof payload !== 'object') return null;
  const p = payload as Record<string, unknown>;
  const outcomeId = typeof p['outcome_id'] === 'string' ? p['outcome_id'] : null;
  if (!outcomeId) return null;
  const result = isOutcomeResult(p['result']) ? p['result'] : 'failed';
  const iteration =
    typeof p['iteration'] === 'number' && Number.isInteger(p['iteration']) && p['iteration'] >= 0
      ? p['iteration']
      : 0;
  return {
    outcome_id: outcomeId,
    result,
    explanation: typeof p['explanation'] === 'string' ? p['explanation'] : '',
    iteration,
    ...(p['usage'] !== undefined ? { usage: p['usage'] } : {}),
  };
}

function isOutcomeResult(value: unknown): value is OutcomeEvaluationRecord['result'] {
  return (
    value === 'satisfied' ||
    value === 'needs_revision' ||
    value === 'max_iterations_reached' ||
    value === 'failed' ||
    value === 'interrupted'
  );
}
