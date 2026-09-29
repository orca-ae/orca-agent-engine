// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The CEL evaluator prepared guardrails are evaluated with.
 *
 * This was a byte-for-byte copy of the library's evaluator with its own cache.
 * Two implementations of "compile, activate, map false to the rule's verdict"
 * is one too many: a fix to compilation or activation semantics would have had
 * to be made twice, and the copy that was missed would have failed only at
 * runtime, on a fail-closed phase, as a denied tool call.
 *
 * The cache is still process-global and never evicted — it is keyed by raw
 * expression source, so a process that sees N distinct expressions retains N
 * compiled programs for its lifetime. `createExpressionEvaluator()` exists for
 * a caller that wants to scope one to a session instead; adopting it is a
 * lifecycle decision, not a re-export.
 */
export { evaluateGuardrailExpression } from '@orca/guardrails';
