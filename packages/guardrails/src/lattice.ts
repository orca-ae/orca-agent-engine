// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The verdict lattice.
 *
 * Every composition in this package — across guardrails, across authority
 * tiers, and between a guardrail and the permission policy that seeds the fold
 * — is `maxVerdict`. Monotonicity is therefore a property of this module rather
 * than a rule each call site must remember: there is no operation here that can
 * turn a `deny` back into an `allow`.
 */

/** Ordered least to most restrictive. The order is the semantics. */
export const VERDICTS = ['allow', 'ask', 'deny'] as const;

export type Verdict = (typeof VERDICTS)[number];

const RANK: Readonly<Record<Verdict, number>> = { allow: 0, ask: 1, deny: 2 };

/** `true` when `a` is at least as restrictive as `b`. */
export function atLeastAsStrict(a: Verdict, b: Verdict): boolean {
  return RANK[a] >= RANK[b];
}

/**
 * The composition operator: the stricter of two verdicts.
 *
 * `allow` is the identity, which is why a guardrail that abstains is
 * indistinguishable from one that allows — abstention costs nothing and needs
 * no separate representation.
 */
export function maxVerdict(a: Verdict, b: Verdict): Verdict {
  return RANK[a] >= RANK[b] ? a : b;
}
