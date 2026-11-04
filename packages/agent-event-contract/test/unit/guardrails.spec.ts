// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { guardrailDecisionPayload } from '../../src/index.js';

describe('guardrail decision evidence', () => {
  it('records only non-allow observers beside the enforced result', () => {
    expect(
      guardrailDecisionPayload({
        phase: 'tool_call',
        toolUseId: 'toolu_one',
        decision: {
          verdict: 'deny',
          observed: [
            { guardrailId: 'grd_one', mode: 'observe', verdict: 'ask' },
            { guardrailId: 'grd_two', mode: 'observe', verdict: 'allow' },
          ],
        },
      }),
    ).toEqual({
      phase: 'tool_call',
      decision: 'deny',
      tool_use_id: 'toolu_one',
      guardrails: [{ guardrail_id: 'grd_one', mode: 'observe', verdict: 'ask' }],
    });
    expect(
      guardrailDecisionPayload({ phase: 'request', decision: { verdict: 'allow' } }),
    ).toBeUndefined();
  });
  it('retains the observer reason while preserving an allowed enforcement result', () => {
    expect(
      guardrailDecisionPayload({
        phase: 'request',
        decision: {
          verdict: 'allow',
          observed: [
            {
              guardrailId: 'grd_budget',
              mode: 'observe',
              verdict: 'deny',
              reason: 'Monthly budget would deny this request',
            },
          ],
        },
      }),
    ).toEqual({
      phase: 'request',
      decision: 'allow',
      guardrails: [
        {
          guardrail_id: 'grd_budget',
          mode: 'observe',
          verdict: 'deny',
          reason: 'Monthly budget would deny this request',
        },
      ],
    });
  });
  it('records exposure names instead of an invocation id', () => {
    expect(
      guardrailDecisionPayload({
        phase: 'tool_call',
        tools: ['Bash'],
        decision: {
          verdict: 'allow',
          observed: [{ guardrailId: 'grd_one', mode: 'observe', verdict: 'deny' }],
        },
      }),
    ).toMatchObject({ guardrails: [{ tools: ['Bash'] }] });
  });
});
