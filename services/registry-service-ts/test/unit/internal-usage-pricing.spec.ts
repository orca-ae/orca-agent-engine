// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { SEED_PRICE_PROVIDER, type ModelPriceEntry } from '@orca/harness-catalog';
import { internalContract } from '../../src/contracts/internal.contract.js';
import { usageCostNanoUsd, type UsageDelta } from '../../src/api/internal.routes.js';

const EMPTY: UsageDelta = {
  inputTokens: 0,
  outputTokens: 0,
  cacheReadInputTokens: 0,
  cacheCreationEphemeral1hInputTokens: 0,
  cacheCreationEphemeral5mInputTokens: 0,
};

function priced(modelId: string, input: number, output: number): ModelPriceEntry {
  return {
    provider: SEED_PRICE_PROVIDER,
    modelId,
    source: 'seed',
    inputPerMillionTokens: input,
    outputPerMillionTokens: output,
  };
}

/**
 * Price against the default provider. These cases are about the arithmetic and
 * the unpriced/zero distinction, not about which vendor served the model; the
 * provider dimension itself is covered separately below.
 */
function cost(usage: UsageDelta, modelId: string, entries: readonly ModelPriceEntry[]) {
  return usageCostNanoUsd(usage, SEED_PRICE_PROVIDER, modelId, entries);
}

describe('usage delta pricing', () => {
  it('prices a delta into exact nano-USD', () => {
    const nanoUsd = cost(
      { ...EMPTY, inputTokens: 1_000_000, outputTokens: 200_000 },
      'claude-test-4',
      [priced('claude-test-4', 3, 15)],
    );
    // 1M input at $3/M + 200k output at $15/M = $6.00
    expect(nanoUsd).toBe(6_000_000_000n);
  });

  it('returns no cost for a model with no price data', () => {
    expect(cost({ ...EMPTY, inputTokens: 5_000 }, 'claude-test-4', [])).toBeNull();
    expect(
      cost({ ...EMPTY, inputTokens: 5_000 }, 'some-other-vendor-model', [
        priced('claude-test-4', 3, 15),
      ]),
    ).toBeNull();
  });

  it('keeps a genuine zero distinct from unpriced', () => {
    // A priced model that consumed tokens yields an exact integer, distinct
    // from the `null` above, which means unmeasurable.
    expect(
      cost({ ...EMPTY, inputTokens: 1 }, 'nearly-free-1', [priced('nearly-free-1', 0.001, 0.001)]),
    ).toBe(1n);
  });

  it('resolves a zero-rate row to unpriced, not to a measured $0', () => {
    expect(
      cost({ ...EMPTY, inputTokens: 1_000 }, 'free-model-1', [priced('free-model-1', 0, 0)]),
    ).toBeNull();
  });

  it('treats a delta that consumed nothing as nothing to price', () => {
    // An empty flush must not turn an unpriced session into a $0.00 one.
    expect(cost(EMPTY, 'claude-test-4', [priced('claude-test-4', 3, 15)])).toBeNull();
  });

  it('preserves sub-micro costs exactly', () => {
    expect(
      cost({ ...EMPTY, inputTokens: 1 }, 'claude-test-4', [priced('claude-test-4', 3, 15)]),
    ).toBe(3_000n);
    expect(
      cost({ ...EMPTY, inputTokens: 1 }, 'cheap-model-1', [priced('cheap-model-1', 0.4, 1)]),
    ).toBe(400n);
  });

  it('prices cache buckets additively', () => {
    const entry: ModelPriceEntry = {
      ...priced('claude-test-4', 3, 15),
      cacheReadPerMillionTokens: 0.3,
      cacheWritePerMillionTokens: 3.75,
    };
    const nanoUsd = cost(
      {
        ...EMPTY,
        cacheReadInputTokens: 1_000_000,
        cacheCreationEphemeral5mInputTokens: 1_000_000,
      },
      'claude-test-4',
      [entry],
    );
    // $0.30 cache read + $3.75 cache write
    expect(nanoUsd).toBe(4_050_000_000n);
  });

  it('resolves a dated snapshot through its base model', () => {
    expect(
      cost({ ...EMPTY, inputTokens: 1_000_000 }, 'claude-test-4-20260801', [
        priced('claude-test-4', 3, 15),
      ]),
    ).toBe(3_000_000_000n);
  });
});

describe('usage report contract', () => {
  const route = internalContract.recordSessionUsage;

  it('accepts the model that produced the delta and the thread it belongs to', () => {
    expect(
      route.body.parse({
        usage: { input_tokens: 10, output_tokens: 2 },
        model: 'claude-test-4',
        thread_id: 'sth_11111111-2222-3333-4444-555555555555',
      }),
    ).toMatchObject({
      model: 'claude-test-4',
      thread_id: 'sth_11111111-2222-3333-4444-555555555555',
    });
  });

  it('keeps both fields optional so an unattributed delta still records', () => {
    expect(route.body.parse({ usage: { input_tokens: 10 } })).toEqual({
      usage: { input_tokens: 10 },
    });
  });

  it('rejects an empty model and a thread id that is not a session thread', () => {
    expect(() => route.body.parse({ usage: { input_tokens: 1 }, model: '' })).toThrow();
    expect(() => route.body.parse({ usage: { input_tokens: 1 }, thread_id: 'ses_abc' })).toThrow();
  });
});
