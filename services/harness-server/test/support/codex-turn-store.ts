// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { CodexCheckpoint } from '@orca/codex-harness';
import type { HarnessTurnReceipt, HarnessTurnSnapshot } from '@orca/harness-catalog';
import type { CodexTurnStore } from '../../src/harness/codex-sdk/turn-receipt.js';

/** Durable-receipt fixture; transport failures can be injected on either side of commit. */
export function turnStoreFixture(
  options: {
    checkpoint?: CodexCheckpoint;
    onCommit?: (state: CodexCheckpoint, receipt: HarnessTurnReceipt) => Promise<void>;
    afterCommit?: () => Promise<void>;
    recover?: CodexTurnStore['recover'];
  } = {},
) {
  const snapshot: HarnessTurnSnapshot = {
    runtimeRevision: 1,
    ownershipRevision: 1,
    state: options.checkpoint ?? null,
    receipt: null,
  };
  const store: CodexTurnStore = {
    claim: async () => structuredClone(snapshot),
    recover: options.recover ?? (async () => {}),
    update: async (action) => {
      if (action.type === 'begin') snapshot.receipt = structuredClone(action.receipt);
      else {
        const receipt = snapshot.receipt;
        if (!receipt || receipt.turnId !== action.turnId) throw new Error('wrong fixture turn');
        if (action.type === 'accept_source') {
          if (!receipt.sourceIds.includes(action.sourceId)) receipt.sourceIds.push(action.sourceId);
        } else if (action.type === 'settle') receipt.phase = 'settled';
        else if (action.type === 'abandon') {
          receipt.phase = 'ready';
          receipt.error = action.error ?? 'Codex SDK unfinished turn abandoned without replay';
        } else {
          await options.onCommit?.(action.state as CodexCheckpoint, {
            ...receipt,
            error: action.error,
          });
          snapshot.state = action.state;
          receipt.phase = 'ready';
          receipt.error = action.error;
          await options.afterCommit?.();
        }
      }
      return structuredClone(snapshot);
    },
  };
  return { store, snapshot };
}
