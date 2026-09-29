// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Private separate-runtime recovery metadata; never part of the native checkpoint. */
export interface HarnessTurnReceipt {
  turnId: string;
  sourceIds: string[];
  usageEventId: string;
  guarded: boolean;
  phase: 'pending' | 'ready' | 'settled';
  terminalEventId: string;
  errorEventId: string;
  producedAt: string;
  error: string | null;
}
export interface HarnessTurnSnapshot {
  runtimeRevision: number;
  ownershipRevision: number;
  state: unknown;
  receipt: HarnessTurnReceipt | null;
  guardrailState?: Record<string, unknown>;
}
export type HarnessTurnAction =
  | { type: 'inspect' }
  | { type: 'claim'; expectedOwnershipRevision: number }
  | { type: 'begin'; receipt: HarnessTurnReceipt }
  | { type: 'accept_source'; turnId: string; sourceId: string }
  | {
      type: 'commit';
      turnId: string;
      state: unknown;
      responsePersisted: true;
      error: string | null;
    }
  | { type: 'abandon'; turnId: string; error?: string }
  | { type: 'settle'; turnId: string };
export interface HarnessTurnRequest {
  runtimeRevision?: number;
  ownershipRevision?: number;
  ownerToken?: string;
  action: HarnessTurnAction;
}
