// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash, randomUUID } from 'node:crypto';
import { sessionErrorPayload, userEventProcessedPayload } from '@orca/agent-event-contract';
import { and, eq, isNull, ne, inArray, sql } from 'drizzle-orm';
import {
  NATIVE_USAGE_SOURCE,
  nativeCompletionIsComplete,
  parseResourceCheckpoint,
  resourceCheckpointDigest,
  parseNativeCompletion,
  nativeCompletionDigest,
  type NativeCompletion,
  type ResourceCheckpoint,
} from '@orca/sandbox-runtime';
import type { Event, TranscriptStore } from '@orca/transcript-store';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  sessionNativeCompletions as receipts,
  sessions,
  workspaces,
} from '../persistence/postgres/schema.js';
import { loadSessionExecutionOwner } from './session-harness-binding.js';
import {
  loadGuardrailTurnSubject,
  parseUsageDelta,
  persistUsage,
  priceUsageDelta,
  type DbTransaction,
} from './usage-accounting.js';

const AUTHORITY_ID = 'authority';
const NATIVE_APPEND_BATCH_BYTES = 512 * 1024;
interface NativeBinding {
  executionId: string;
  placementId: string;
  revision: string;
}
interface ReceiptRecord extends NativeBinding {
  source: typeof NATIVE_USAGE_SOURCE;
  producedAt: string;
  checkpoint?: ResourceCheckpoint;
  digest?: string;
  incompleteCompletion?: NativeCompletion;
}
export type NativeAcknowledgement = (phase: 'prepared' | 'completed') => Promise<void>;

function stableId(...parts: string[]): string {
  return 'evt_' + createHash('sha256').update(JSON.stringify(parts)).digest('hex').slice(0, 32);
}

/** This persisted choice survives reconnect; a legacy shared token cannot become a second writer. */
export async function hasNativeUsageAuthority(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
): Promise<boolean> {
  const [row] = await db
    .select({ record: receipts.record })
    .from(receipts)
    .where(
      and(
        eq(receipts.workspaceId, workspaceId),
        eq(receipts.sessionId, sessionId),
        eq(receipts.turnId, AUTHORITY_ID),
        eq(receipts.phase, 'authority'),
      ),
    );
  if (!row) return false;
  const record = row.record as { source?: unknown } | null;
  if (record?.source !== NATIVE_USAGE_SOURCE)
    throw new Error('Unknown persisted native usage authority');
  return true;
}

/** Ordered durable work; File/event persistence remains in SessionResourcesDelivery. */
export class NativeCompletionCoordinator {
  private readonly token = randomUUID();
  private finishQueue: Promise<void> = Promise.resolve();
  private knownEvents: { turnId: string; ids: Set<string> } | undefined;
  constructor(
    private readonly options: {
      db: DbClient;
      workspaceId: string;
      sessionId: string;
      runnerId: string;
      store: TranscriptStore;
      isCurrent(revision: string): Promise<boolean>;
    },
  ) {}

  private scope(turnId?: string) {
    const o = this.options;
    return and(
      eq(receipts.workspaceId, o.workspaceId),
      eq(receipts.sessionId, o.sessionId),
      ...(turnId === undefined ? [] : [eq(receipts.turnId, turnId)]),
    );
  }

  private async locked<T>(
    revision: string,
    action: (tx: DbTransaction) => Promise<T>,
    claiming = false,
  ): Promise<T> {
    const o = this.options;
    if (!(await o.isCurrent(revision)))
      throw new Error('Native completion generation or revision retired');
    return o.db.transaction(async (tx) => {
      const [session] = await tx
        .select({ id: sessions.id })
        .from(sessions)
        .where(
          and(
            eq(sessions.workspaceId, o.workspaceId),
            eq(sessions.id, o.sessionId),
            eq(sessions.runnerId, o.runnerId),
            eq(sessions.distributionState, 'assigned'),
            isNull(sessions.deletedAt),
            isNull(sessions.archivedAt),
            ne(sessions.status, 'terminated'),
          ),
        )
        .for('update');
      const [workspace] = await tx
        .select({ status: workspaces.status })
        .from(workspaces)
        .where(eq(workspaces.id, o.workspaceId));
      if (
        !session ||
        workspace?.status !== 'active' ||
        (await loadSessionExecutionOwner(tx as unknown as DbClient, o.workspaceId, o.sessionId)) !==
          'registry'
      )
        throw new Error('Native completion lost session ownership');
      if (!claiming) {
        const [authority] = await tx
          .select({ record: receipts.record })
          .from(receipts)
          .where(this.scope(AUTHORITY_ID));
        if ((authority?.record as { token?: unknown } | undefined)?.token !== this.token)
          throw new Error('Native completion owner was superseded');
      }
      const result = await action(tx);
      if (!(await o.isCurrent(revision))) throw new Error('Native completion generation retired');
      return result;
    });
  }

  hasAuthority(): Promise<boolean> {
    return hasNativeUsageAuthority(
      this.options.db,
      this.options.workspaceId,
      this.options.sessionId,
    );
  }

  async claim(binding: NativeBinding): Promise<void> {
    await this.locked(
      binding.revision,
      async (tx) => {
        await tx
          .insert(receipts)
          .values({
            workspaceId: this.options.workspaceId,
            sessionId: this.options.sessionId,
            turnId: AUTHORITY_ID,
            phase: 'authority',
            record: { ...binding, source: NATIVE_USAGE_SOURCE, token: this.token },
          })
          .onConflictDoUpdate({
            target: [receipts.workspaceId, receipts.sessionId, receipts.turnId],
            set: {
              phase: 'authority',
              record: { ...binding, source: NATIVE_USAGE_SOURCE, token: this.token },
            },
          });
      },
      true,
    );
  }

  async begin(event: Event, binding: NativeBinding): Promise<void> {
    const o = this.options;
    if (
      event.workspaceId !== o.workspaceId ||
      event.sessionId !== o.sessionId ||
      event.subpath ||
      event.kind !== 'user.message'
    )
      throw new Error('Unsupported native driving event');
    await this.locked(binding.revision, async (tx) => {
      const unfinished = await tx
        .select()
        .from(receipts)
        .where(and(this.scope(), ne(receipts.phase, 'completed'), ne(receipts.phase, 'authority')));
      if (unfinished.some((row) => row.turnId !== event.id || row.phase !== 'running'))
        throw new Error('Native completion blocks the next turn');
      const [existing] = await tx.select().from(receipts).where(this.scope(event.id));
      if (existing) {
        const record = existing.record as ReceiptRecord;
        if (
          existing.phase !== 'running' ||
          record.executionId !== binding.executionId ||
          record.placementId !== binding.placementId ||
          record.revision !== binding.revision
        )
          throw new Error('Native turn cannot be replayed on a new execution');
        return;
      }
      await tx.insert(receipts).values({
        workspaceId: o.workspaceId,
        sessionId: o.sessionId,
        turnId: event.id,
        phase: 'running',
        record: { ...binding, source: NATIVE_USAGE_SOURCE, producedAt: new Date().toISOString() },
      });
    });
  }

  async stage(checkpoint: ResourceCheckpoint, digest: string): Promise<void> {
    const completion = checkpoint.completion;
    if (!completion || resourceCheckpointDigest(checkpoint) !== digest)
      throw new Error('Native checkpoint evidence differs');
    await this.locked(checkpoint.revision, async (tx) => {
      const [row] = await tx.select().from(receipts).where(this.scope(completion.turn_id));
      if (!row) throw new Error('Native completion has no accepted turn');
      const record = row.record as ReceiptRecord;
      if (
        record.source !== NATIVE_USAGE_SOURCE ||
        record.revision !== checkpoint.revision ||
        record.executionId !== completion.execution_id ||
        record.placementId !== completion.placement_id ||
        (record.digest !== undefined && record.digest !== digest)
      )
        throw new Error('Native receipt identity or payload differs');
      if (record.digest === digest) return;
      if (row.phase !== 'running') throw new Error('Native receipt cannot accept a checkpoint');
      await tx
        .update(receipts)
        .set({ record: { ...record, checkpoint, digest } })
        .where(this.scope(completion.turn_id));
    });
  }

  async prepare(checkpoint: ResourceCheckpoint, digest: string): Promise<void> {
    await this.stage(checkpoint, digest);
    const completion = checkpoint.completion;
    if (!completion || resourceCheckpointDigest(checkpoint) !== digest)
      throw new Error('Native checkpoint evidence differs');
    let incomplete = false;
    await this.locked(checkpoint.revision, async (tx) => {
      const [row] = await tx.select().from(receipts).where(this.scope(completion.turn_id));
      if (!row) throw new Error('Native completion has no accepted turn');
      const record = row.record as ReceiptRecord;
      if (
        record.source !== NATIVE_USAGE_SOURCE ||
        record.revision !== checkpoint.revision ||
        record.executionId !== completion.execution_id ||
        record.placementId !== completion.placement_id ||
        (record.digest !== undefined && record.digest !== digest)
      )
        throw new Error('Native receipt identity or payload differs');
      if (row.phase === 'completed' || row.phase === 'prepared') return;
      if (row.phase !== 'running') throw new Error('Native turn is incomplete');
      await this.persistReports(tx, completion);
      incomplete = !nativeCompletionIsComplete(completion);
      await tx
        .update(receipts)
        .set({
          phase: incomplete ? 'incomplete' : 'prepared',
          record: { ...record, checkpoint, digest },
        })
        .where(this.scope(completion.turn_id));
    });
    if (incomplete) {
      await this.replayEvents(completion, checkpoint.revision, new Date().toISOString());
      await this.appendIncomplete(completion.turn_id, checkpoint.revision);
      throw new Error('Native usage evidence is incomplete');
    }
  }

  finish(turnId: string, revision: string, acknowledge?: NativeAcknowledgement): Promise<void> {
    const work = this.finishQueue.then(() => this.finishTurn(turnId, revision, acknowledge));
    this.finishQueue = work.catch(() => {});
    return work;
  }

  private async finishTurn(
    turnId: string,
    revision: string,
    acknowledge?: NativeAcknowledgement,
  ): Promise<void> {
    const [row] = await this.options.db.select().from(receipts).where(this.scope(turnId));
    if (!row || !['prepared', 'completed'].includes(row.phase))
      throw new Error('Native completion is not prepared');
    const record = row.record as ReceiptRecord;
    const checkpoint = parseResourceCheckpoint(record.checkpoint);
    const completion = checkpoint.completion;
    if (
      !completion ||
      !nativeCompletionIsComplete(completion) ||
      record.digest !== resourceCheckpointDigest(checkpoint) ||
      record.revision !== revision
    )
      throw new Error('Native prepared receipt is invalid');
    await this.locked(revision, async () => {});
    await this.replayEvents(completion, revision, record.producedAt);
    await acknowledge?.('prepared');
    if (row.phase !== 'completed') {
      if (completion.outcome !== 'success')
        await this.appendOnce(turnId, revision, record.producedAt, 'agent.error', {
          message:
            completion.error ??
            (completion.outcome === 'interrupted' ? 'Turn interrupted' : 'Turn failed'),
        });
      await this.appendOnce(turnId, revision, record.producedAt, 'agent.turn_completed', {
        turn_event_id: turnId,
      });
      await this.appendOnce(turnId, revision, record.producedAt, 'session.user_event_processed', {
        ...userEventProcessedPayload(turnId),
      });
      await this.appendOnce(turnId, revision, record.producedAt, 'session.user_event_completed', {
        ...userEventProcessedPayload(turnId),
      });
      await this.locked(revision, async (tx) => {
        await tx.update(receipts).set({ phase: 'completed' }).where(this.scope(turnId));
      });
    }
    await acknowledge?.('completed');
  }

  async finishAcknowledgedCheckpoint(
    checkpointId: string,
    digest: string,
    revision: string,
    acknowledge: NativeAcknowledgement,
  ): Promise<boolean> {
    const [row] = await this.options.db
      .select()
      .from(receipts)
      .where(
        and(
          this.scope(),
          inArray(receipts.phase, ['prepared', 'completed']),
          sql`${receipts.record}->>'digest' = ${digest}`,
          sql`${receipts.record}->'checkpoint'->>'checkpoint_id' = ${checkpointId}`,
        ),
      )
      .limit(1);
    if (!row) return false;
    await this.finish(row.turnId, revision, acknowledge);
    return true;
  }

  async pending(): Promise<Array<{ turnId: string; phase: string; record: ReceiptRecord }>> {
    return (
      await this.options.db
        .select()
        .from(receipts)
        .where(and(this.scope(), ne(receipts.phase, 'authority'), ne(receipts.phase, 'completed')))
    ).map((row) => ({ turnId: row.turnId, phase: row.phase, record: row.record as ReceiptRecord }));
  }

  async markLost(turnId: string, revision: string, evidence?: unknown): Promise<never> {
    let completion: NativeCompletion | undefined;
    await this.locked(revision, async (tx) => {
      const [row] = await tx.select().from(receipts).where(this.scope(turnId));
      if (!row || !['running', 'incomplete'].includes(row.phase))
        throw new Error('Native lost turn is not recoverable');
      const record = row.record as ReceiptRecord;
      completion =
        evidence === undefined
          ? (record.incompleteCompletion ?? record.checkpoint?.completion)
          : parseNativeCompletion(evidence);
      if (completion) {
        if (
          completion.turn_id !== turnId ||
          completion.execution_id !== record.executionId ||
          completion.placement_id !== record.placementId ||
          record.revision !== revision
        )
          throw new Error('Native retained report ownership differs');
        if (
          record.incompleteCompletion &&
          nativeCompletionDigest(record.incompleteCompletion) !== nativeCompletionDigest(completion)
        )
          throw new Error('Native retained reports changed');
        await this.persistReports(tx, completion);
      }
      await tx
        .update(receipts)
        .set({
          phase: 'incomplete',
          record: { ...record, ...(completion ? { incompleteCompletion: completion } : {}) },
        })
        .where(this.scope(turnId));
    });
    if (completion) await this.replayEvents(completion, revision, new Date().toISOString());
    await this.appendIncomplete(turnId, revision);
    throw new Error('Native process lost unacknowledged turn evidence; operator recovery required');
  }

  private async persistReports(tx: DbTransaction, completion: NativeCompletion): Promise<void> {
    const o = this.options;
    const subject = await loadGuardrailTurnSubject(
      tx as unknown as DbClient,
      o.workspaceId,
      o.sessionId,
      completion.turn_id,
    );
    for (const report of completion.reports) {
      if (!report.usage) continue;
      const usage = parseUsageDelta(report.usage);
      if (!usage) throw new Error('Invalid normalized native report');
      const costNanoUsd = report.model
        ? await priceUsageDelta(
            tx as unknown as DbClient,
            o.workspaceId,
            report.provider,
            report.model,
            usage,
          )
        : null;
      const result = await persistUsage(tx, {
        workspaceId: o.workspaceId,
        sessionId: o.sessionId,
        usage,
        costNanoUsd,
        now: new Date(),
        turnSubject: subject,
        usageEventId: stableId(
          o.workspaceId,
          o.sessionId,
          completion.turn_id,
          completion.execution_id,
          report.id,
        ),
      });
      if (result !== 'applied' && result !== 'replayed')
        throw new Error('Native usage did not commit');
    }
  }

  private async replayEvents(
    completion: NativeCompletion,
    revision: string,
    producedAt: string,
  ): Promise<void> {
    await this.appendEvents(
      completion.turn_id,
      revision,
      (completion.events ?? []).map((event) =>
        this.toEvent(
          completion.turn_id,
          producedAt,
          event.type as string,
          event,
          event.id as string,
        ),
      ),
    );
  }

  /** Shared by the live bridge and receipt replay. The session lock fences broker appends too. */
  async appendEvents(turnId: string, revision: string, events: Event[]): Promise<void> {
    if (!events.length) return;
    const o = this.options;
    try {
      await this.locked(revision, async () => {
        if (this.knownEvents?.turnId !== turnId) {
          const ids = new Set<string>();
          for await (const event of o.store.read(o.workspaceId, o.sessionId, {
            fromCursor: '',
            maxEvents: 0,
            subpath: '',
          })) {
            const payload = JSON.parse(Buffer.from(event.payload).toString()) as {
              source_event_id?: unknown;
            };
            if (payload.source_event_id === turnId) {
              ids.add(event.id);
              if (ids.size > 10004) throw new Error('Native event replay exceeds limits');
            }
          }
          this.knownEvents = { turnId, ids };
        }
        const missing = events.filter((event) => !this.knownEvents!.ids.has(event.id));
        if (this.knownEvents.ids.size + missing.length > 10004)
          throw new Error('Native event replay exceeds limits');
        let batch: Event[] = [];
        let bytes = 0;
        const append = async () => {
          if (!batch.length) return;
          if (!(await o.isCurrent(revision)))
            throw new Error('Native completion generation retired');
          await o.store.append(o.workspaceId, o.sessionId, batch);
          for (const event of batch) this.knownEvents!.ids.add(event.id);
          batch = [];
          bytes = 0;
        };
        for (const event of missing) {
          // Budget duplicated raw/Avro metadata, the Kafka key and record framing with headroom.
          const size =
            event.payload.byteLength +
            2 * Buffer.byteLength(JSON.stringify({ ...event, payload: undefined })) +
            Buffer.byteLength(event.id) +
            256;
          if (batch.length && bytes + size > NATIVE_APPEND_BATCH_BYTES) await append();
          // A single large event stays subject to the store's existing limit, without truncation.
          batch.push(event);
          bytes += size;
        }
        await append();
      });
    } catch (error) {
      // The broker may have committed before an ACK or transaction failed; reconcile on retry.
      this.knownEvents = undefined;
      throw error;
    }
  }

  private async appendIncomplete(turnId: string, revision: string): Promise<void> {
    await this.appendOnce(
      turnId,
      revision,
      new Date().toISOString(),
      'session.error',
      sessionErrorPayload({
        type: 'api_error',
        message: 'Turn incomplete: required output or usage evidence is missing',
        willRetry: false,
      }),
    );
  }

  private async appendOnce(
    turnId: string,
    revision: string,
    producedAt: string,
    kind: string,
    payload: Record<string, unknown>,
    eventId?: string,
  ): Promise<void> {
    await this.appendEvents(turnId, revision, [
      this.toEvent(turnId, producedAt, kind, payload, eventId),
    ]);
  }

  private toEvent(
    turnId: string,
    producedAt: string,
    kind: string,
    payload: Record<string, unknown>,
    eventId?: string,
  ): Event {
    const o = this.options;
    const id = eventId ?? stableId(o.workspaceId, o.sessionId, turnId, 'native-completion', kind);
    return {
      id,
      workspaceId: o.workspaceId,
      sessionId: o.sessionId,
      subpath: '',
      seq: 0,
      producedAt,
      producedBy: 'harness',
      kind,
      idempotencyKey: id,
      payload: Buffer.from(JSON.stringify({ ...payload, type: kind, source_event_id: turnId })),
    };
  }
}
