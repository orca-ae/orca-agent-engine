// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, asc, count, eq, isNull, lte, sql } from 'drizzle-orm';
import type { DbClient, DbTransaction } from '../persistence/postgres/client.js';
import { agentTriggerFires, agentTriggers, workspaces } from '../persistence/postgres/schema.js';
import { httpEventToProto } from './events.js';
import { createSessionInTransaction } from './session-creation.js';
import { SessionSkillBindingError } from './session-skill-bindings.js';
import {
  nextTriggerOccurrence,
  renderCronTriggerTitle,
  TRIGGER_MISFIRE_GRACE_MS,
  TriggerScheduleError,
  TriggerSessionTemplateError,
} from './trigger-cron.js';
import { newId } from './versioning.js';

export const TRIGGER_RECONCILE_INTERVAL_MS = 5_000;
export const TRIGGER_RECONCILE_BATCH_SIZE = 100;
export const TRIGGER_DISPATCH_MAX_ATTEMPTS = 5;
export const TRIGGER_DISPATCH_RETRY_BASE_MS = 5_000;
export const TRIGGER_DISPATCH_RETRY_MAX_MS = 60_000;

export interface TriggerPlannerResult {
  processed: number;
  created: number;
  deduplicated: number;
  misfired: number;
  paused: number;
  dueBacklog: number;
  oldestDueAgeSeconds: number;
}

export interface TriggerDispatcherResult {
  processed: number;
  enqueued: number;
  retried: number;
  canceled: number;
  failed: number;
}

export async function reconcileDueAgentTriggers(
  db: DbClient,
  options: { now?: Date; batchSize?: number } = {},
): Promise<TriggerPlannerResult> {
  const now = options.now ?? new Date();
  const dueFilter = and(
    eq(agentTriggers.status, 'active'),
    isNull(agentTriggers.archivedAt),
    lte(agentTriggers.nextFireAt, now),
    eq(workspaces.status, 'active'),
  );
  const [backlog] = await db
    .select({ value: count() })
    .from(agentTriggers)
    .innerJoin(workspaces, eq(agentTriggers.workspaceId, workspaces.id))
    .where(and(isNull(agentTriggers.deletedAt), dueFilter));
  const candidates = await db
    .select({
      id: agentTriggers.id,
      workspaceId: agentTriggers.workspaceId,
      nextFireAt: agentTriggers.nextFireAt,
    })
    .from(agentTriggers)
    .innerJoin(workspaces, eq(agentTriggers.workspaceId, workspaces.id))
    .where(and(isNull(agentTriggers.deletedAt), dueFilter))
    .orderBy(asc(agentTriggers.nextFireAt), asc(agentTriggers.id))
    .limit(options.batchSize ?? TRIGGER_RECONCILE_BATCH_SIZE);
  const result: TriggerPlannerResult = {
    processed: 0,
    created: 0,
    deduplicated: 0,
    misfired: 0,
    paused: 0,
    dueBacklog: backlog?.value ?? 0,
    oldestDueAgeSeconds: candidates[0]?.nextFireAt
      ? Math.max(0, (now.getTime() - candidates[0].nextFireAt.getTime()) / 1_000)
      : 0,
  };

  for (const candidate of candidates) {
    const outcome = await db.transaction(async (rawTx) => {
      const tx = rawTx as unknown as DbClient;
      const [trigger] = await tx
        .select()
        .from(agentTriggers)
        .where(
          and(
            isNull(agentTriggers.deletedAt),
            eq(agentTriggers.workspaceId, candidate.workspaceId),
            eq(agentTriggers.id, candidate.id),
            eq(agentTriggers.status, 'active'),
            isNull(agentTriggers.archivedAt),
            lte(agentTriggers.nextFireAt, now),
          ),
        )
        .for('update', { skipLocked: true })
        .limit(1);
      if (!trigger?.nextFireAt) return 'skipped' as const;

      const [workspace] = await tx
        .select({ status: workspaces.status })
        .from(workspaces)
        .where(eq(workspaces.id, trigger.workspaceId))
        .limit(1);
      if (workspace?.status !== 'active') return 'skipped' as const;

      let nextFireAt: Date;
      try {
        nextFireAt = nextTriggerOccurrence(trigger.cronExpression, trigger.timezone, now);
      } catch (error) {
        if (!(error instanceof TriggerScheduleError)) throw error;
        await tx
          .update(agentTriggers)
          .set({
            status: 'paused',
            generation: trigger.generation + 1,
            nextFireAt: null,
            lastError: error.message,
            updatedAt: now,
          })
          .where(
            and(
              isNull(agentTriggers.deletedAt),
              eq(agentTriggers.workspaceId, trigger.workspaceId),
              eq(agentTriggers.id, trigger.id),
            ),
          );
        return 'paused' as const;
      }

      const isMisfire = now.getTime() - trigger.nextFireAt.getTime() > TRIGGER_MISFIRE_GRACE_MS;
      let inserted = false;
      if (!isMisfire) {
        const insertedRows = await tx
          .insert(agentTriggerFires)
          .values({
            id: newId('trgfire'),
            workspaceId: trigger.workspaceId,
            triggerId: trigger.id,
            generation: trigger.generation,
            scheduledFor: trigger.nextFireAt,
            status: 'pending',
            plannedSessionId: newId('ses'),
            sessionId: null,
            eventId: newId('evt'),
            attemptCount: 0,
            lastError: null,
            nextAttemptAt: now,
            enqueuedAt: null,
            createdAt: now,
            updatedAt: now,
          })
          .onConflictDoNothing()
          .returning({ id: agentTriggerFires.id });
        inserted = insertedRows.length === 1;
      }
      await tx
        .update(agentTriggers)
        .set({
          nextFireAt,
          ...(isMisfire ? {} : { lastFiredAt: trigger.nextFireAt }),
          lastError: null,
          updatedAt: now,
        })
        .where(
          and(
            isNull(agentTriggers.deletedAt),
            eq(agentTriggers.workspaceId, trigger.workspaceId),
            eq(agentTriggers.id, trigger.id),
          ),
        );
      return isMisfire
        ? ('misfired' as const)
        : inserted
          ? ('created' as const)
          : ('deduplicated' as const);
    });
    if (outcome === 'skipped') continue;
    result.processed += 1;
    result[outcome] += 1;
  }
  return result;
}

export async function dispatchPendingAgentTriggerFires(
  db: DbClient,
  options: { now?: Date; batchSize?: number } = {},
): Promise<TriggerDispatcherResult> {
  const now = options.now ?? new Date();
  const result: TriggerDispatcherResult = {
    processed: 0,
    enqueued: 0,
    retried: 0,
    canceled: 0,
    failed: 0,
  };
  const candidates = await db
    .select({
      id: agentTriggerFires.id,
      workspaceId: agentTriggerFires.workspaceId,
      triggerId: agentTriggerFires.triggerId,
    })
    .from(agentTriggerFires)
    .where(and(eq(agentTriggerFires.status, 'pending'), lte(agentTriggerFires.nextAttemptAt, now)))
    .orderBy(asc(agentTriggerFires.scheduledFor), asc(agentTriggerFires.id))
    .limit(options.batchSize ?? TRIGGER_RECONCILE_BATCH_SIZE);

  for (const candidate of candidates) {
    const transaction = db.transaction(async (rawTx) => {
      const tx = rawTx;
      // Lock Trigger first. Lifecycle routes use the same lock order, so pause,
      // update, and delete cannot deadlock against fire dispatch.
      const [trigger] = await tx
        .select()
        .from(agentTriggers)
        .where(
          and(
            isNull(agentTriggers.deletedAt),
            eq(agentTriggers.workspaceId, candidate.workspaceId),
            eq(agentTriggers.id, candidate.triggerId),
          ),
        )
        .for('update', { skipLocked: true })
        .limit(1);
      if (!trigger) return 'skipped' as const;

      const [fire] = await tx
        .select()
        .from(agentTriggerFires)
        .where(
          and(
            eq(agentTriggerFires.workspaceId, candidate.workspaceId),
            eq(agentTriggerFires.id, candidate.id),
            eq(agentTriggerFires.status, 'pending'),
            lte(agentTriggerFires.nextAttemptAt, now),
          ),
        )
        .for('update', { skipLocked: true })
        .limit(1);
      if (!fire) return 'skipped' as const;

      const [workspace] = await tx
        .select({ status: workspaces.status })
        .from(workspaces)
        .where(eq(workspaces.id, trigger.workspaceId))
        .limit(1);
      if (
        workspace?.status !== 'active' ||
        trigger.status !== 'active' ||
        trigger.archivedAt !== null ||
        trigger.generation !== fire.generation
      ) {
        await markFire(tx, fire.id, now, 'canceled', null);
        return 'canceled' as const;
      }

      const initialEvent = httpEventToProto({
        workspaceId: trigger.workspaceId,
        sessionId: fire.plannedSessionId,
        producedBy: 'client',
        idempotencyKey: fire.eventId,
        input: {
          id: fire.eventId,
          type: 'user.message',
          content: [{ type: 'text', text: trigger.payload }],
        },
        validationMode: 'claude',
        now: () => now,
      });

      try {
        const creation = await createSessionInTransaction(tx, {
          id: fire.plannedSessionId,
          workspaceId: trigger.workspaceId,
          agentId: trigger.agentId,
          agentVersion: trigger.agentVersion,
          environmentId: trigger.environmentId,
          title: renderCronTriggerTitle(trigger.titleTemplate, trigger.name, trigger.payload),
          metadata: trigger.metadata as Record<string, string>,
          vaultIds: trigger.vaultIds,
          tools: null,
          mcpServers: null,
          agentOverrides: null,
          initialEvents: [initialEvent],
          now,
        });
        if (!creation.ok) {
          await pauseFailedTrigger(tx, trigger, fire.id, now, creation.message);
          return 'failed' as const;
        }
      } catch (error) {
        if (
          !(error instanceof SessionSkillBindingError) &&
          !(error instanceof TriggerSessionTemplateError)
        ) {
          throw error;
        }
        await pauseFailedTrigger(tx, trigger, fire.id, now, error.message);
        return 'failed' as const;
      }

      await tx
        .update(agentTriggerFires)
        .set({
          status: 'enqueued',
          sessionId: fire.plannedSessionId,
          attemptCount: sql`${agentTriggerFires.attemptCount} + 1`,
          lastError: null,
          enqueuedAt: now,
          updatedAt: now,
        })
        .where(
          and(
            eq(agentTriggerFires.workspaceId, trigger.workspaceId),
            eq(agentTriggerFires.id, fire.id),
            eq(agentTriggerFires.status, 'pending'),
          ),
        );
      return 'enqueued' as const;
    });
    const outcome = await transaction.catch((error) =>
      recordTransientDispatchFailure(db, candidate, now, error),
    );
    if (outcome === 'skipped') continue;
    result.processed += 1;
    if (outcome === 'retry') result.retried += 1;
    else result[outcome] += 1;
  }
  return result;
}

async function recordTransientDispatchFailure(
  db: DbClient,
  candidate: { id: string; workspaceId: string; triggerId: string },
  now: Date,
  error: unknown,
): Promise<'skipped' | 'retry' | 'failed'> {
  return db.transaction(async (rawTx) => {
    const tx = rawTx as unknown as DbClient;
    const [fire] = await tx
      .select({ attemptCount: agentTriggerFires.attemptCount })
      .from(agentTriggerFires)
      .where(
        and(
          eq(agentTriggerFires.workspaceId, candidate.workspaceId),
          eq(agentTriggerFires.triggerId, candidate.triggerId),
          eq(agentTriggerFires.id, candidate.id),
          eq(agentTriggerFires.status, 'pending'),
        ),
      )
      .for('update')
      .limit(1);
    if (!fire) return 'skipped' as const;

    const attemptCount = fire.attemptCount + 1;
    const exhausted = attemptCount >= TRIGGER_DISPATCH_MAX_ATTEMPTS;
    const retryDelayMs = Math.min(
      TRIGGER_DISPATCH_RETRY_BASE_MS * 2 ** (attemptCount - 1),
      TRIGGER_DISPATCH_RETRY_MAX_MS,
    );
    await tx
      .update(agentTriggerFires)
      .set({
        status: exhausted ? 'failed' : 'pending',
        attemptCount,
        lastError: dispatchErrorMessage(error),
        nextAttemptAt: exhausted ? now : new Date(now.getTime() + retryDelayMs),
        updatedAt: now,
      })
      .where(
        and(
          eq(agentTriggerFires.workspaceId, candidate.workspaceId),
          eq(agentTriggerFires.id, candidate.id),
          eq(agentTriggerFires.status, 'pending'),
        ),
      );
    return exhausted ? ('failed' as const) : ('retry' as const);
  });
}

function dispatchErrorMessage(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.slice(0, 4_096);
}

async function pauseFailedTrigger(
  tx: DbTransaction,
  trigger: typeof agentTriggers.$inferSelect,
  fireId: string,
  now: Date,
  message: string,
): Promise<void> {
  await markFire(tx, fireId, now, 'failed', message);
  await tx
    .update(agentTriggers)
    .set({
      status: 'paused',
      generation: trigger.generation + 1,
      nextFireAt: null,
      lastError: message,
      updatedAt: now,
    })
    .where(
      and(
        isNull(agentTriggers.deletedAt),
        eq(agentTriggers.workspaceId, trigger.workspaceId),
        eq(agentTriggers.id, trigger.id),
        eq(agentTriggers.generation, trigger.generation),
      ),
    );
}

async function markFire(
  tx: DbTransaction,
  fireId: string,
  now: Date,
  status: 'failed' | 'canceled',
  error: string | null,
): Promise<void> {
  await tx
    .update(agentTriggerFires)
    .set({
      status,
      attemptCount: sql`${agentTriggerFires.attemptCount} + 1`,
      lastError: error,
      updatedAt: now,
    })
    .where(and(eq(agentTriggerFires.id, fireId), eq(agentTriggerFires.status, 'pending')));
}
