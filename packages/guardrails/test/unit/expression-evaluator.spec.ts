// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  createExpressionEvaluator,
  evaluateGuardrailExpression,
  type EvaluatorContext,
  type ExpressionRule,
} from '../../src/index.js';

function context(rule: ExpressionRule, state: Record<string, unknown> = {}): EvaluatorContext {
  return {
    event: {
      phase: 'tool_call',
      sessionId: 'ses_1',
      tool: { name: 'Bash', input: { command: 'ls' } },
    },
    params: {},
    state,
    guardrail: {
      id: 'grd_1',
      name: 'expression',
      enabled: true,
      phases: ['tool_call'],
      scope: 'explicit',
      rule,
    },
  };
}

describe('standard expression evaluator', () => {
  it('maps true to allow and false to the configured verdict and reason', () => {
    const allowRule: ExpressionRule = {
      kind: 'expression',
      expression: "event.tool.name == 'Bash'",
      onFalse: 'deny',
    };
    expect(evaluateGuardrailExpression(allowRule, context(allowRule))).toEqual({
      verdict: 'allow',
    });

    const denyRule: ExpressionRule = {
      kind: 'expression',
      expression: "event.tool.name == 'Read'",
      onFalse: 'deny',
      reason: 'Bash is not permitted',
    };
    expect(evaluateGuardrailExpression(denyRule, context(denyRule))).toEqual({
      verdict: 'deny',
      reason: 'Bash is not permitted',
    });
  });

  it('builds the activation with the evaluator state', () => {
    const rule: ExpressionRule = {
      kind: 'expression',
      expression: 'event.state.calls < 2',
      onFalse: 'ask',
    };
    const evaluator = createExpressionEvaluator();
    expect(evaluator(rule, context(rule, { calls: 1 }))).toEqual({ verdict: 'allow' });
    expect(evaluator(rule, context(rule, { calls: 2 }))).toEqual({ verdict: 'ask' });
  });

  it('surfaces invalid prepared source for the engine to fail closed', () => {
    const rule: ExpressionRule = {
      kind: 'expression',
      expression: 'event.tool.name ===',
      onFalse: 'deny',
    };
    expect(() => createExpressionEvaluator()(rule, context(rule))).toThrow(/invalid/i);
  });
});
