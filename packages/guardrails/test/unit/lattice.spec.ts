// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { VERDICTS, atLeastAsStrict, maxVerdict, type Verdict } from '../../src/lattice.js';

/**
 * The lattice is the whole safety argument: composition takes a maximum, so
 * "a guardrail can tighten a verdict but never loosen one" is a property of
 * this function rather than a rule every call site has to remember. These
 * tests pin the algebra that argument depends on.
 */
describe('verdict lattice', () => {
  it('orders allow < ask < deny', () => {
    expect(VERDICTS).toEqual(['allow', 'ask', 'deny']);
  });

  it('is a total order: every pair is comparable', () => {
    for (const a of VERDICTS) {
      for (const b of VERDICTS) {
        expect(atLeastAsStrict(a, b) || atLeastAsStrict(b, a)).toBe(true);
      }
    }
  });

  it.each([
    ['allow', 'allow', 'allow'],
    ['allow', 'ask', 'ask'],
    ['allow', 'deny', 'deny'],
    ['ask', 'allow', 'ask'],
    ['ask', 'ask', 'ask'],
    ['ask', 'deny', 'deny'],
    ['deny', 'allow', 'deny'],
    ['deny', 'ask', 'deny'],
    ['deny', 'deny', 'deny'],
  ] as const)('max(%s, %s) = %s', (a, b, expected) => {
    expect(maxVerdict(a, b)).toBe(expected);
  });

  it('is commutative', () => {
    for (const a of VERDICTS) {
      for (const b of VERDICTS) {
        expect(maxVerdict(a, b)).toBe(maxVerdict(b, a));
      }
    }
  });

  it('is associative', () => {
    for (const a of VERDICTS) {
      for (const b of VERDICTS) {
        for (const c of VERDICTS) {
          expect(maxVerdict(maxVerdict(a, b), c)).toBe(maxVerdict(a, maxVerdict(b, c)));
        }
      }
    }
  });

  it('treats allow as the identity element, so abstaining never changes a verdict', () => {
    for (const v of VERDICTS) {
      expect(maxVerdict(v, 'allow')).toBe(v);
    }
  });

  it('never loosens: the fold result is at least as strict as every input', () => {
    const inputs: Verdict[] = ['deny', 'allow', 'ask', 'allow'];
    const folded = inputs.reduce<Verdict>((acc, v) => maxVerdict(acc, v), 'allow');
    for (const v of inputs) {
      expect(atLeastAsStrict(folded, v)).toBe(true);
    }
  });
});
