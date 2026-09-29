// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type {
  HarnessTurnAction,
  HarnessTurnReceipt,
  HarnessTurnSnapshot,
} from '@orca/harness-catalog';
import {
  SessionEventKind,
  sessionErrorPayload,
  sessionIdlePayload,
  withCanonicalAgentEventEnvelope,
  type AgentEvent,
} from '../agent-harness.js';

export interface CodexTurnStore {
  claim(): Promise<HarnessTurnSnapshot>;
  update(
    action: Exclude<HarnessTurnAction, { type: 'inspect' | 'claim' }>,
  ): Promise<HarnessTurnSnapshot>;
  recover(receipt: HarnessTurnReceipt): Promise<void>;
}

export function receiptTerminalEvents(receipt: HarnessTurnReceipt): AgentEvent[] {
  const events: AgentEvent[] = [];
  if (receipt.error)
    events.push(
      withCanonicalAgentEventEnvelope({
        id: receipt.errorEventId,
        kind: SessionEventKind.error,
        producedAt: receipt.producedAt,
        payload: sessionErrorPayload({
          type: 'processing_error',
          message: receipt.error,
          willRetry: false,
        }),
      }),
    );
  events.push(
    withCanonicalAgentEventEnvelope({
      id: receipt.terminalEventId,
      kind: SessionEventKind.statusIdle,
      producedAt: receipt.producedAt,
      payload: sessionIdlePayload(receipt.error ? 'retries_exhausted' : 'end_turn'),
      completionPolicy: { sourceIds: [...receipt.sourceIds] },
    }),
  );
  return events;
}

/** Startup recovery performs no SDK work and preserves the previous native checkpoint. */
export async function recoverCodexTurn(
  store: CodexTurnStore,
  snapshot: HarnessTurnSnapshot,
): Promise<HarnessTurnSnapshot> {
  if (!snapshot.receipt || snapshot.receipt.phase === 'settled') return snapshot;
  if (snapshot.receipt.phase === 'pending')
    snapshot = await store.update({ type: 'abandon', turnId: snapshot.receipt.turnId });
  await store.recover(snapshot.receipt!);
  return await store.update({ type: 'settle', turnId: snapshot.receipt!.turnId });
}
