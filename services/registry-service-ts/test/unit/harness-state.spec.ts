// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { DbClient } from '../../src/persistence/postgres/client.js';
import { describe, expect, it } from 'vitest';
import {
  harnessStateRevision,
  normalizeHarnessState,
  loadHarnessTurnState,
  HarnessTurnInvalidBindingError,
} from '../../src/domain/harness-state.js';

describe('native checkpoint revisions', () => {
  const a = 'sessions/2026/09/20/rollout-a-thread.jsonl';
  const b = 'sessions/2026/09/20/rollout-b-thread.jsonl';
  const state = { version: 1, threadId: 'thread', files: { [a]: 'YQ==', [b]: 'Yg==' } };
  it('remains stable across JSONB object ordering and omits unsupported properties', () => {
    expect(
      harnessStateRevision({ files: { [b]: 'Yg==', [a]: 'YQ==' }, threadId: 'thread', version: 1 }),
    ).toBe(harnessStateRevision(state));
    expect(normalizeHarnessState({ ...state, secret: 'not native state' })).toEqual(state);
    expect(harnessStateRevision(null)).toBeNull();
  });
  it('changes when native history changes and validates before hashing', () => {
    expect(harnessStateRevision({ ...state, files: { [a]: 'Yw==', [b]: 'Yg==' } })).not.toBe(
      harnessStateRevision(state),
    );
    expect(() => harnessStateRevision({ ...state, files: { '../escape': 'YQ==' } })).toThrow();
  });
  it('preserves the instruction fingerprint and includes it in checkpoint revisions', () => {
    const fingerprinted = { ...state, instructionsSha256: 'a'.repeat(64) };
    expect(normalizeHarnessState(fingerprinted)).toEqual(fingerprinted);
    expect(harnessStateRevision(fingerprinted)).not.toBe(harnessStateRevision(state));
    expect(harnessStateRevision({ ...fingerprinted, instructionsSha256: 'b'.repeat(64) })).not.toBe(
      harnessStateRevision(fingerprinted),
    );
  });
  it('distinguishes database read failures from malformed stored state', async () => {
    const failure = new Error('database unavailable');
    const failingDb = {
      select: () => ({
        from: () => ({
          where: async () => {
            throw failure;
          },
        }),
      }),
    } as unknown as DbClient;
    await expect(loadHarnessTurnState(failingDb, 'ws_test', 'ses_test')).rejects.toBe(failure);
    const corruptDb = {
      select: () => ({ from: () => ({ where: async () => [{ state: { invalid: true } }] }) }),
    } as unknown as DbClient;
    await expect(loadHarnessTurnState(corruptDb, 'ws_test', 'ses_test')).rejects.toBeInstanceOf(
      HarnessTurnInvalidBindingError,
    );
  });
});
