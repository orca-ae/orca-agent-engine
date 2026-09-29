// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { and, eq, sql } from 'drizzle-orm';
import type { DbTransaction } from '../persistence/postgres/client.js';
import { sessionObservabilityBindings } from '../persistence/postgres/schema.js';

export type SessionObservabilityLifecycleAction = 'archive' | 'delete';

type SessionObservabilityBindingLifecycleErrorReason = 'missing_pin' | 'invalid_state';

export class SessionObservabilityBindingLifecycleError extends Error {
  override readonly name = 'SessionObservabilityBindingLifecycleError';

  constructor(readonly reason: SessionObservabilityBindingLifecycleErrorReason) {
    super('session observability binding lifecycle unavailable');
  }
}

export interface SessionObservabilityBindingLifecycleState {
  status: string;
  archivedAt: Date | null;
  deletedAt: Date | null;
  sessionRevocationEpoch: number;
}

interface SessionObservabilityBindingLifecycleUpdate {
  status: 'archived' | 'deleted';
  archivedAt: Date | null;
  deletedAt: Date | null;
}

/**
 * Compute one valid pin lifecycle transition. A null result is an idempotent
 * re-archive, which deliberately leaves its first tombstone untouched.
 */
export function nextSessionObservabilityBindingLifecycle(
  state: SessionObservabilityBindingLifecycleState,
  action: SessionObservabilityLifecycleAction,
  now: Date,
): SessionObservabilityBindingLifecycleUpdate | null {
  if (!isNonnegativeSafeInteger(state.sessionRevocationEpoch) || !isValidDate(now)) {
    throw invalidState();
  }

  const activeOrDisabled = state.status === 'active' || state.status === 'disabled';
  const archived = state.status === 'archived';
  if (activeOrDisabled) {
    if (state.archivedAt !== null || state.deletedAt !== null) throw invalidState();
  } else if (archived) {
    if (!isValidDate(state.archivedAt) || state.deletedAt !== null) throw invalidState();
  } else {
    throw invalidState();
  }

  if (action === 'archive') {
    if (archived) return null;
    return { status: 'archived', archivedAt: now, deletedAt: null };
  }

  return {
    status: 'deleted',
    archivedAt: state.archivedAt,
    deletedAt: now,
  };
}

/**
 * Lock and tombstone one exact Session pin inside its caller-owned lifecycle
 * transaction. A missing or malformed pin aborts that transaction rather than
 * permitting a Session lifecycle write without its revocation fence.
 */
export async function transitionSessionObservabilityBindingInTransaction(
  tx: DbTransaction,
  input: {
    workspaceId: string;
    sessionId: string;
    action: SessionObservabilityLifecycleAction;
    now: Date;
  },
): Promise<void> {
  const pin = (
    await tx
      .select({
        status: sessionObservabilityBindings.status,
        archivedAt: sessionObservabilityBindings.archivedAt,
        deletedAt: sessionObservabilityBindings.deletedAt,
        sessionRevocationEpoch: sessionObservabilityBindings.sessionRevocationEpoch,
      })
      .from(sessionObservabilityBindings)
      .where(
        and(
          eq(sessionObservabilityBindings.workspaceId, input.workspaceId),
          eq(sessionObservabilityBindings.sessionId, input.sessionId),
        ),
      )
      .for('update')
      .limit(1)
  )[0];
  if (!pin) {
    throw new SessionObservabilityBindingLifecycleError('missing_pin');
  }

  const transition = nextSessionObservabilityBindingLifecycle(pin, input.action, input.now);
  if (transition === null) return;

  await tx
    .update(sessionObservabilityBindings)
    .set({
      status: transition.status,
      archivedAt: transition.archivedAt,
      deletedAt: transition.deletedAt,
      sessionRevocationEpoch:
        sql`${sessionObservabilityBindings.sessionRevocationEpoch} + 1` as unknown as number,
      updatedAt: input.now,
    })
    .where(
      and(
        eq(sessionObservabilityBindings.workspaceId, input.workspaceId),
        eq(sessionObservabilityBindings.sessionId, input.sessionId),
      ),
    );
}

function invalidState(): SessionObservabilityBindingLifecycleError {
  return new SessionObservabilityBindingLifecycleError('invalid_state');
}

function isNonnegativeSafeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isValidDate(value: unknown): value is Date {
  return value instanceof Date && Number.isFinite(value.getTime());
}
