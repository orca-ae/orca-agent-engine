// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  assertGuardrailReaderCapabilities,
  GUARDRAIL_READER_CAPABILITIES,
} from '../../src/capabilities.js';

describe('reader capability declarations', () => {
  it('retains legacy delivery but refuses old readers for each new semantic', () => {
    expect(() =>
      assertGuardrailReaderCapabilities(
        [{ rule: { kind: 'builtin', builtin: 'block_tools' } }],
        [],
      ),
    ).not.toThrow();
    expect(() => assertGuardrailReaderCapabilities([{ mode: 'observe', rule: {} }], [])).toThrow(
      /observe/,
    );
    expect(() =>
      assertGuardrailReaderCapabilities(
        [{ rule: { kind: 'builtin', builtin: 'workspace_monthly_cost_budget' } }],
        [],
      ),
    ).toThrow(/monthly_budgets/);
    expect(() =>
      assertGuardrailReaderCapabilities(
        [{ mode: 'observe', rule: { kind: 'builtin', builtin: 'user_monthly_cost_budget' } }],
        GUARDRAIL_READER_CAPABILITIES,
      ),
    ).not.toThrow();
  });
});
