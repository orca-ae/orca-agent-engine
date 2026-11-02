// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { internalContract } from '../../../src/contracts/internal.contract.js';

describe('internal usage pricing facts contract', () => {
  const usage = { input_tokens: 1, output_tokens: 0 };
  const facts = {
    version: 1,
    request_id: 'response-contract',
    service_tier: 'standard',
    request_input_tokens: 1,
    usage_profile: 'anthropic-cache-v1',
    evidence_source: 'provider_response',
  };

  it('requires a model for pricing facts before accepting the report identity', () => {
    const report = { usage, pricing_facts: facts, usage_event_id: 'evt_contract' };
    expect(internalContract.recordSessionUsage.body.safeParse(report).success).toBe(false);
    expect(
      internalContract.recordSessionUsage.body.safeParse({ ...report, model: 'example-model' })
        .success,
    ).toBe(true);
  });

  it('continues to accept legacy usage without a model or pricing facts', () => {
    expect(internalContract.recordSessionUsage.body.safeParse({ usage }).success).toBe(true);
  });
});
