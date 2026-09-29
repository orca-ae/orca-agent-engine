// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { HarnessTurnReceiptSchema } from '../contracts/internal.contract.js';
import { createHash } from 'node:crypto';
import { and, eq, isNull, ne } from 'drizzle-orm';
import {
  resolveExecutionOwner,
  validateSdkCheckpoint,
  type HarnessTurnReceipt,
  type HarnessTurnRequest,
  type HarnessTurnSnapshot,
} from '@orca/harness-catalog';
import type { DbClient } from '../persistence/postgres/client.js';
import {
  environments,
  sessionHarnessStates,
  sessionUsageEvents,
  guardrailState,
  sessions,
  workspaces,
} from '../persistence/postgres/schema.js';
import {
  loadSessionHarnessBinding,
  loadSessionExecutionOwner,
  SessionHarnessBindingError,
} from './session-harness-binding.js';

type CodexCheckpoint = {
  version: 1;
  threadId: string;
  files: Record<string, string>;
  instructionsSha256?: string;
  format?: 'pi_sdk';
  sdkVersion?: string;
};

/** Canonicalize only the validated native state, never arbitrary request properties. */
export function normalizeHarnessState(state: unknown): CodexCheckpoint {
  validateSdkCheckpoint(state);
  const checkpoint = state as CodexCheckpoint;
  return {
    version: 1,
    ...(checkpoint.format ? { format: checkpoint.format, sdkVersion: checkpoint.sdkVersion! } : {}),
    threadId: checkpoint.threadId,
    files: Object.fromEntries(
      Object.entries(checkpoint.files).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
    ),
    ...(checkpoint.instructionsSha256 !== undefined
      ? { instructionsSha256: checkpoint.instructionsSha256 }
      : {}),
  };
}

/** JSONB changes key ordering, so hash a canonical projection rather than wire bytes. */
export function harnessStateRevision(state: unknown): string | null {
  if (state == null) return null;
  return createHash('sha256')
    .update(JSON.stringify(normalizeHarnessState(state)))
    .digest('hex');
}

/** Stored data is invalid; database failures remain transient. */
export class HarnessTurnInvalidBindingError extends Error {
  constructor(readonly resourceType: 'harness_state' | 'harness') {
    super(`invalid runtime binding: ${resourceType}`);
  }
}

export class HarnessStateWriteError extends Error {
  constructor(
    readonly status: 404 | 409,
    message: string,
  ) {
    super(message);
  }
}

/** Replace state only while the prepared runtime and previous checkpoint still match. */
export async function saveHarnessState(input: {
  db: DbClient;
  workspaceId: string;
  sessionId: string;
  runtimeRevision: number;
  expectedCheckpointRevision: string | null;
  state: unknown;
}): Promise<string> {
  const state = normalizeHarnessState(input.state);
  const revision = harnessStateRevision(state)!;
  return input.db.transaction(async (tx) => {
    // Serializes checkpoint writes with each other and Session runtime/lifecycle changes.
    const [session] = await tx
      .select()
      .from(sessions)
      .where(
        and(
          eq(sessions.workspaceId, input.workspaceId),
          eq(sessions.id, input.sessionId),
          isNull(sessions.deletedAt),
        ),
      )
      .for('update');
    if (!session) throw new HarnessStateWriteError(404, 'session not found');
    const [workspace] = await tx
      .select({ status: workspaces.status })
      .from(workspaces)
      .where(eq(workspaces.id, input.workspaceId));
    if (workspace?.status !== 'active') throw new HarnessStateWriteError(404, 'session not found');
    if (
      session.archivedAt !== null ||
      session.status === 'terminated' ||
      session.runtimeRevision !== input.runtimeRevision
    ) {
      throw new HarnessStateWriteError(409, 'session runtime is no longer current');
    }
    let target: string | null = null;
    if (session.environmentId) {
      const [environment] = await tx
        .select({ target: environments.target })
        .from(environments)
        .where(
          and(
            eq(environments.workspaceId, input.workspaceId),
            eq(environments.id, session.environmentId),
          ),
        );
      if (!environment) throw new HarnessStateWriteError(409, 'session environment is unavailable');
      target = environment.target;
    }
    try {
      const selection = await loadSessionHarnessBinding(
        tx as unknown as DbClient,
        input.workspaceId,
        session.agentId,
        session.agentVersion,
      );
      if (
        (selection.harness !== 'codex_sdk' && selection.harness !== 'pi_sdk') ||
        resolveExecutionOwner(target ?? 'cloud', selection) !== 'harness-server'
      ) {
        throw new Error('not a harness-server managed SDK session');
      }
      validateSdkCheckpoint(state, selection.harness);
    } catch {
      throw new HarnessStateWriteError(
        409,
        'session does not use a harness-server native SDK harness',
      );
    }
    const stored = await loadHarnessTurnState(
      tx as unknown as DbClient,
      input.workspaceId,
      input.sessionId,
    ).catch(() => {
      throw new HarnessStateWriteError(409, 'stored harness checkpoint is invalid');
    });
    if (stored.ownershipRevision)
      throw new HarnessStateWriteError(409, 'harness turn ownership has been claimed');
    const currentState = await loadHarnessState(
      tx as unknown as DbClient,
      input.workspaceId,
      input.sessionId,
    );
    let currentRevision: string | null;
    try {
      currentRevision = harnessStateRevision(currentState);
    } catch {
      throw new HarnessStateWriteError(409, 'stored harness checkpoint is invalid');
    }
    // Repeating an acknowledged or ambiguously acknowledged write is harmless.
    // Check lifecycle/runtime first so retries cannot resurrect a retired runtime.
    if (currentRevision === revision) return revision;
    if (currentRevision !== input.expectedCheckpointRevision) {
      throw new HarnessStateWriteError(409, 'harness checkpoint has changed');
    }
    await writeHarnessState(tx as unknown as DbClient, input.workspaceId, input.sessionId, state);
    return revision;
  });
}

/** Load native history only at private execution/preparation boundaries. */
export async function loadHarnessState(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
): Promise<unknown> {
  const [row] = await db
    .select({ state: sessionHarnessStates.state })
    .from(sessionHarnessStates)
    .where(
      and(
        eq(sessionHarnessStates.workspaceId, workspaceId),
        eq(sessionHarnessStates.sessionId, sessionId),
      ),
    );
  return nativeHarnessState(row?.state ?? null);
}

async function writeHarnessState(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  state: unknown,
): Promise<void> {
  await db
    .insert(sessionHarnessStates)
    .values({ workspaceId, sessionId, state })
    .onConflictDoUpdate({
      target: [sessionHarnessStates.workspaceId, sessionHarnessStates.sessionId],
      set: { state },
    });
}

/** Serialize colocated writes with runner rebinding and Session lifecycle changes. */
export async function saveRunnerHarnessState(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
  runnerId: string,
  checkpoint: unknown,
  isCurrent: () => boolean = () => true,
): Promise<void> {
  const state = normalizeHarnessState(checkpoint);
  await db.transaction(async (tx) => {
    if (!isCurrent()) throw new HarnessStateWriteError(409, 'runner generation retired');
    const [session] = await tx
      .select({ id: sessions.id, agentId: sessions.agentId, agentVersion: sessions.agentVersion })
      .from(sessions)
      .where(
        and(
          eq(sessions.workspaceId, workspaceId),
          eq(sessions.id, sessionId),
          eq(sessions.runnerId, runnerId),
          eq(sessions.distributionState, 'assigned'),
          isNull(sessions.archivedAt),
          ne(sessions.status, 'terminated'),
          isNull(sessions.deletedAt),
        ),
      )
      .for('update');
    if (!session || !isCurrent())
      throw new HarnessStateWriteError(409, 'harness checkpoint lost session ownership');
    const [workspace] = await tx
      .select({ status: workspaces.status })
      .from(workspaces)
      .where(eq(workspaces.id, workspaceId));
    if (workspace?.status !== 'active')
      throw new HarnessStateWriteError(409, 'workspace is not active');
    if (
      (await loadSessionExecutionOwner(tx as unknown as DbClient, workspaceId, sessionId)) !==
      'registry'
    )
      throw new HarnessStateWriteError(409, 'session is no longer Registry-owned');
    const selection = await loadSessionHarnessBinding(
      tx as unknown as DbClient,
      workspaceId,
      session.agentId,
      session.agentVersion,
    );
    validateSdkCheckpoint(state, selection.harness);
    const stored = await loadHarnessTurnState(tx as unknown as DbClient, workspaceId, sessionId);
    if (stored.ownershipRevision)
      throw new HarnessStateWriteError(409, 'harness turn ownership has been claimed');
    await writeHarnessState(tx as unknown as DbClient, workspaceId, sessionId, state);
    if (!isCurrent()) throw new HarnessStateWriteError(409, 'runner generation retired');
  });
}

interface HarnessTurnEnvelope {
  format: 'orca.separate_codex_turn';
  version: 1;
  ownershipRevision: number;
  ownerToken: string | null;
  state: CodexCheckpoint | null;
  receipt: HarnessTurnReceipt | null;
}

/** Both preparation and the colocated snapshot join expose native state only. */
export function nativeHarnessState(stored: unknown): CodexCheckpoint | null {
  if (stored == null) return null;
  const value = stored as Partial<HarnessTurnEnvelope>;
  if (value.format === 'orca.separate_codex_turn') {
    if (value.version !== 1) throw new Error('unsupported harness turn envelope');
    return value.state == null ? null : normalizeHarnessState(value.state);
  }
  return normalizeHarnessState(stored);
}

export async function loadHarnessTurnState(
  db: DbClient,
  workspaceId: string,
  sessionId: string,
): Promise<HarnessTurnEnvelope> {
  const [row] = await db
    .select({ state: sessionHarnessStates.state })
    .from(sessionHarnessStates)
    .where(
      and(
        eq(sessionHarnessStates.workspaceId, workspaceId),
        eq(sessionHarnessStates.sessionId, sessionId),
      ),
    );
  try {
    const value = row?.state as Partial<HarnessTurnEnvelope> | undefined;
    const state = nativeHarnessState(value ?? null);
    if (value?.format === 'orca.separate_codex_turn') {
      if (
        !Number.isSafeInteger(value.ownershipRevision) ||
        value.ownershipRevision! < 1 ||
        typeof value.ownerToken !== 'string' ||
        !('receipt' in value)
      )
        throw new Error('invalid harness turn envelope');
      if (!('state' in value)) throw new Error('invalid harness turn envelope');
      const receipt = value.receipt === null ? null : HarnessTurnReceiptSchema.parse(value.receipt);
      return { ...value, state, receipt } as HarnessTurnEnvelope;
    }
    return {
      format: 'orca.separate_codex_turn',
      version: 1,
      ownershipRevision: 0,
      ownerToken: null,
      state,
      receipt: null,
    };
  } catch {
    throw new HarnessTurnInvalidBindingError('harness_state');
  }
}

/** One bounded receipt and one owner; all transitions serialize with Session retirement. */
export async function transitionHarnessTurn(input: {
  db: DbClient;
  workspaceId: string;
  sessionId: string;
  request: HarnessTurnRequest;
}): Promise<HarnessTurnSnapshot | null> {
  return input.db.transaction(async (tx) => {
    const db = tx as unknown as DbClient;
    const [session] = await tx
      .select()
      .from(sessions)
      .where(
        and(
          eq(sessions.workspaceId, input.workspaceId),
          eq(sessions.id, input.sessionId),
          isNull(sessions.deletedAt),
        ),
      )
      .for('update');
    if (!session) throw new HarnessStateWriteError(404, 'session not found');
    const [workspace] = await tx
      .select({ status: workspaces.status })
      .from(workspaces)
      .where(eq(workspaces.id, input.workspaceId));
    const action = input.request.action;
    if (workspace?.status !== 'active') {
      if (action.type === 'inspect') return null;
      throw new HarnessStateWriteError(404, 'session not found');
    }
    // An inactive Session has no recoverable owner. Preserve the dispatcher's
    // normal preparation/drop path for delayed source deliveries.
    if (
      action.type === 'inspect' &&
      (session.archivedAt !== null || session.status === 'terminated')
    )
      return null;
    const envelope = await loadHarnessTurnState(db, input.workspaceId, input.sessionId);
    // Cold interrupts can inspect absent receipts without preparing an archived Agent.
    if (action.type === 'inspect' && !envelope.ownershipRevision) return null;
    if (
      session.archivedAt !== null ||
      session.status === 'terminated' ||
      (action.type !== 'inspect' && session.runtimeRevision !== input.request.runtimeRevision)
    )
      throw new HarnessStateWriteError(409, 'session runtime is no longer current');
    let target: string | null = null;
    if (session.environmentId) {
      const [environment] = await tx
        .select({ target: environments.target })
        .from(environments)
        .where(
          and(
            eq(environments.workspaceId, input.workspaceId),
            eq(environments.id, session.environmentId),
          ),
        );
      if (!environment) throw new HarnessTurnInvalidBindingError('harness');
      target = environment.target;
    }
    let selectedHarness: string;
    try {
      const selection = await loadSessionHarnessBinding(
        db,
        input.workspaceId,
        session.agentId,
        session.agentVersion,
      );
      if (
        (selection.harness !== 'codex_sdk' && selection.harness !== 'pi_sdk') ||
        resolveExecutionOwner(target ?? 'cloud', selection) !== 'harness-server'
      )
        throw new SessionHarnessBindingError('wrong harness');
      selectedHarness = selection.harness;
    } catch (error) {
      if (error instanceof SessionHarnessBindingError)
        throw new HarnessTurnInvalidBindingError('harness');
      throw error;
    }
    const snapshot = async (): Promise<HarnessTurnSnapshot> => {
      const rows = await tx
        .select()
        .from(guardrailState)
        .where(
          and(
            eq(guardrailState.workspaceId, input.workspaceId),
            eq(guardrailState.sessionId, input.sessionId),
          ),
        );
      return {
        runtimeRevision: session.runtimeRevision,
        ownershipRevision: envelope.ownershipRevision,
        state: envelope.state,
        receipt: envelope.receipt,
        guardrailState: Object.fromEntries(
          rows.map((row) => [row.key, row.valueNum ?? row.valueJson]),
        ),
      };
    };
    if (action.type === 'inspect') return snapshot();
    const conflict = (message: string): never => {
      throw new HarnessStateWriteError(409, message);
    };
    if (!input.request.ownerToken) return conflict('harness owner token required');
    if (action.type === 'claim') {
      // A lost claim ACK can retry its original expected revision with the same token.
      if (envelope.ownerToken === input.request.ownerToken) return snapshot();
      if (envelope.ownershipRevision !== action.expectedOwnershipRevision)
        return conflict('harness ownership has changed');
      envelope.ownerToken = input.request.ownerToken;
      envelope.ownershipRevision += 1;
    } else {
      if (
        envelope.ownerToken !== input.request.ownerToken ||
        envelope.ownershipRevision !== input.request.ownershipRevision
      )
        return conflict('harness owner is no longer current');
      const receipt = envelope.receipt;
      if (action.type === 'begin') {
        if (receipt?.turnId === action.receipt.turnId) {
          if (
            receipt.usageEventId !== action.receipt.usageEventId ||
            receipt.terminalEventId !== action.receipt.terminalEventId ||
            receipt.errorEventId !== action.receipt.errorEventId ||
            receipt.producedAt !== action.receipt.producedAt ||
            receipt.guarded !== action.receipt.guarded ||
            action.receipt.sourceIds.length !== 1 ||
            action.receipt.sourceIds[0] !== receipt.turnId
          )
            return conflict('turn receipt identity has changed');
          return snapshot();
        }
        if (receipt && receipt.phase !== 'settled')
          return conflict('previous harness turn is not settled');
        if (
          action.receipt.phase !== 'pending' ||
          action.receipt.error !== null ||
          action.receipt.sourceIds.length !== 1 ||
          action.receipt.sourceIds[0] !== action.receipt.turnId
        )
          return conflict('invalid initial turn receipt');
        envelope.receipt = { ...action.receipt, sourceIds: [...action.receipt.sourceIds] };
      } else {
        if (!receipt || receipt.turnId !== action.turnId)
          return conflict('harness turn has changed');
        if (action.type === 'accept_source') {
          if (receipt.sourceIds.includes(action.sourceId)) return snapshot();
          if (receipt.phase !== 'pending' || receipt.sourceIds.length >= 256)
            return conflict('harness turn does not accept sources');
          receipt.sourceIds.push(action.sourceId);
        } else if (action.type === 'settle') {
          if (receipt.phase === 'pending') return conflict('harness turn is not ready');
          receipt.phase = 'settled';
        } else if (action.type === 'abandon') {
          if (receipt.phase !== 'pending') return snapshot();
          receipt.phase = 'ready';
          receipt.error =
            action.error ??
            'Codex SDK turn interrupted by runtime recovery; the unfinished turn was abandoned without replay.';
        } else {
          validateSdkCheckpoint(action.state, selectedHarness);
          const state = normalizeHarnessState(action.state);
          if (receipt.phase !== 'pending') {
            if (
              receipt.error !== action.error ||
              harnessStateRevision(envelope.state) !== harnessStateRevision(state)
            )
              return conflict('harness turn commit has changed');
            return snapshot();
          }
          envelope.state = state;
          receipt.error = action.error;
          receipt.phase = 'ready';
          if (receipt.guarded && !receipt.error) {
            const key = `codex_sdk_usage_pending:${receipt.turnId}`;
            const [marker] = await tx
              .select()
              .from(guardrailState)
              .where(
                and(
                  eq(guardrailState.workspaceId, input.workspaceId),
                  eq(guardrailState.sessionId, input.sessionId),
                  eq(guardrailState.key, key),
                ),
              );
            const value = marker?.valueJson as
              | { usageEventId?: string; turnEventId?: string }
              | undefined;
            const [usage] = await tx
              .select({ eventId: sessionUsageEvents.eventId })
              .from(sessionUsageEvents)
              .where(
                and(
                  eq(sessionUsageEvents.workspaceId, input.workspaceId),
                  eq(sessionUsageEvents.sessionId, input.sessionId),
                  eq(sessionUsageEvents.eventId, receipt.usageEventId),
                ),
              );
            if (
              !usage ||
              value?.usageEventId !== receipt.usageEventId ||
              value.turnEventId !== receipt.turnId
            )
              return conflict('Codex terminal usage is not durable');
            await tx
              .delete(guardrailState)
              .where(
                and(
                  eq(guardrailState.workspaceId, input.workspaceId),
                  eq(guardrailState.sessionId, input.sessionId),
                  eq(guardrailState.key, key),
                ),
              );
          }
        }
      }
    }
    await writeHarnessState(db, input.workspaceId, input.sessionId, envelope);
    return snapshot();
  });
}
