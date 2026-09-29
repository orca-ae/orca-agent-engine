// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { buildActivation, compileExpression, type CompiledExpression } from './cel.js';
import type { ExpressionEvaluator } from './engine.js';

/**
 * Build the standard expression evaluator used by every runtime.
 *
 * The cache belongs to the returned evaluator, so a caller can choose its
 * lifecycle while sharing the compilation, activation, and verdict semantics
 * with every other consumer of this package.
 */
export function createExpressionEvaluator(): ExpressionEvaluator {
  const compiledExpressions = new Map<string, CompiledExpression>();

  return (rule, context) => {
    let compiled = compiledExpressions.get(rule.expression);
    if (!compiled) {
      const result = compileExpression(rule.expression);
      if (!result.ok) {
        throw new Error(`prepared guardrail expression is invalid: ${result.error.message}`);
      }
      compiled = result.compiled;
      compiledExpressions.set(rule.expression, compiled);
    }

    if (compiled.evaluate(buildActivation(context.event, context.state))) {
      return { verdict: 'allow' };
    }
    return {
      verdict: rule.onFalse,
      ...(rule.reason ? { reason: rule.reason } : {}),
    };
  };
}

/** Ready-made evaluator for callers that do not need a separate cache lifecycle. */
export const evaluateGuardrailExpression = createExpressionEvaluator();
