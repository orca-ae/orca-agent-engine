// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  ACTIVATION_ROOT,
  DEFAULT_EVAL_TIMEOUT_MS,
  ExpressionEvaluationError,
  MAX_EXPRESSION_LENGTH,
  buildActivation,
  compileExpression,
  type CompiledExpression,
} from '../../src/cel.js';
import type { GuardrailEvent } from '../../src/types.js';

/**
 * Expressions are the escape hatch from the builtin catalog, so the properties
 * that matter are the ones an operator cannot check by reading the rule: that a
 * bad expression is refused when it is written rather than when it fires, that
 * statefulness is a fact derived from the expression rather than a claim made
 * about it, and that an expression which does not answer the question is an
 * error instead of a coincidence.
 */

const toolCall = (name: string, input: Record<string, unknown>): GuardrailEvent => ({
  phase: 'tool_call',
  sessionId: 'sess_1',
  agentId: 'agent_1',
  turnIndex: 4,
  modelId: 'model-a',
  tool: { name, input },
});

/** Unwraps a compile that is expected to succeed, failing loudly if it did not. */
function compiled(source: string): CompiledExpression {
  const result = compileExpression(source);
  if (!result.ok) {
    throw new Error(`expected "${source}" to compile, got: ${result.error.message}`);
  }
  return result.compiled;
}

describe('compiling expressions', () => {
  it('evaluates a well-formed predicate to the boolean it denotes', () => {
    const expr = compiled("event.tool.name == 'Bash'");

    expect(expr.evaluate(buildActivation(toolCall('Bash', {})))).toBe(true);
    expect(expr.evaluate(buildActivation(toolCall('Read', {})))).toBe(false);
  });

  it('rejects a syntactically invalid expression when it is written, not when it fires', () => {
    const result = compileExpression('event.tool.name ==');

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.message).toBeTruthy();
    }
  });

  it('rejects an expression longer than the size cap', () => {
    const padding = " || event.tool.name == 'x'";
    const repeats = Math.ceil(MAX_EXPRESSION_LENGTH / padding.length) + 1;
    const source = `event.tool.name == 'Bash'${padding.repeat(repeats)}`;
    expect(source.length).toBeGreaterThan(MAX_EXPRESSION_LENGTH);

    const result = compileExpression(source);

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.message).toMatch(/length|long|cap|limit/i);
    }
  });

  it('accepts an expression exactly at the size cap', () => {
    const head = "event.tool.name == '";
    const tail = "'";
    const source = head + 'a'.repeat(MAX_EXPRESSION_LENGTH - head.length - tail.length) + tail;
    expect(source.length).toBe(MAX_EXPRESSION_LENGTH);

    expect(compileExpression(source).ok).toBe(true);
  });

  it('rejects an empty expression', () => {
    expect(compileExpression('').ok).toBe(false);
    expect(compileExpression('   ').ok).toBe(false);
  });

  it('rejects a reference to an activation field that does not exist', () => {
    const result = compileExpression('event.temperature > 3');

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.message).toContain('temperature');
    }
  });

  it('rejects a reference to a variable outside the activation', () => {
    const result = compileExpression("os.getenv('SECRET') == 'x'");

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.message).toContain('os');
    }
  });

  it('never throws, whatever it is handed', () => {
    const nasty = ['(((', '"unterminated', '\u0000', 'event.', '1 +', '}{', 'event.state[', '\\'];
    for (const source of nasty) {
      expect(() => compileExpression(source)).not.toThrow();
      expect(compileExpression(source).ok).toBe(false);
    }
  });

  it('produces a compiled expression that is reusable across events', () => {
    const expr = compiled("event.tool.name == 'Bash'");

    for (const [name, expected] of [
      ['Bash', true],
      ['Read', false],
      ['Bash', true],
    ] as const) {
      expect(expr.evaluate(buildActivation(toolCall(name, {})))).toBe(expected);
    }
  });
});

describe('deriving statefulness', () => {
  it('marks an expression that reads a state key stateful and names the key', () => {
    const expr = compiled('event.state.foo > 2');

    expect(expr.stateful).toBe(true);
    expect([...expr.stateKeys]).toEqual(['foo']);
  });

  it('marks an expression that reads no state stateless', () => {
    const expr = compiled("event.tool.name == 'Bash'");

    expect(expr.stateful).toBe(false);
    expect([...expr.stateKeys]).toEqual([]);
  });

  it('keeps literal root indexing stateless when it does not select state', () => {
    const expr = compiled("event['tool']['name'] == 'Bash'");

    expect(expr.stateful).toBe(false);
    expect([...expr.stateKeys]).toEqual([]);
    expect(expr.evaluate(buildActivation(toolCall('Bash', {})))).toBe(true);
  });

  it('still treats indexed access to the state map as stateful', () => {
    const expr = compiled("size(event['state']) > 0");

    expect(expr.stateful).toBe(true);
    expect([...expr.stateKeys]).toEqual([]);
  });

  it('collects every state key an expression reads, once each', () => {
    const expr = compiled('event.state.a > 1 && event.state.b < 2 && event.state.a < 9');

    expect(expr.stateful).toBe(true);
    expect([...expr.stateKeys].sort()).toEqual(['a', 'b']);
  });

  it('reads a state key written as an index with a literal', () => {
    const expr = compiled("event.state['tool_calls'] > 5");

    expect(expr.stateful).toBe(true);
    expect([...expr.stateKeys]).toEqual(['tool_calls']);
  });

  it('counts a presence test as a state read', () => {
    const expr = compiled('has(event.state.approved) && event.state.approved');

    expect(expr.stateful).toBe(true);
    expect([...expr.stateKeys]).toEqual(['approved']);
  });

  it('is stateful with no determinable keys when the key is computed', () => {
    const expr = compiled('event.state[event.tool.name] > 1');

    expect(expr.stateful).toBe(true);
    expect([...expr.stateKeys]).toEqual([]);
  });

  it('is stateful when the whole state map is read at once', () => {
    const expr = compiled('size(event.state) > 0');

    expect(expr.stateful).toBe(true);
    expect([...expr.stateKeys]).toEqual([]);
  });

  it('is stateful when the whole event is read at once, because state is inside it', () => {
    const expr = compiled('size(event) > 0');

    expect(expr.stateful).toBe(true);
    expect([...expr.stateKeys]).toEqual([]);
  });

  it('does not mistake a field named state elsewhere in the event for guardrail state', () => {
    const expr = compiled("event.tool.input.state == 'open'");

    expect(expr.stateful).toBe(false);
    expect([...expr.stateKeys]).toEqual([]);
  });

  it('does not mistake a comprehension variable for the activation root', () => {
    const expr = compiled("['a', 'b'].exists(event, event == 'a')");

    expect(expr.stateful).toBe(false);
    expect([...expr.stateKeys]).toEqual([]);
    expect(expr.evaluate(buildActivation(toolCall('Bash', {})))).toBe(true);
  });

  it('sees a state read from inside a comprehension body', () => {
    const expr = compiled("['a'].exists(t, event.state.seen == t)");

    expect(expr.stateful).toBe(true);
    expect([...expr.stateKeys]).toEqual(['seen']);
  });
});

describe('building the activation', () => {
  it('exposes the tool name and input of the event', () => {
    const activation = buildActivation(toolCall('Bash', { command: 'ls -la' }));

    expect(activation).toEqual({
      [ACTIVATION_ROOT]: expect.objectContaining({
        tool: { name: 'Bash', input: { command: 'ls -la' } },
      }),
    });
  });

  it('exposes session, model, usage and phase under the documented names', () => {
    const activation = buildActivation({
      phase: 'request',
      sessionId: 'sess_9',
      agentId: 'agent_9',
      turnIndex: 2,
      modelId: 'model-b',
      usage: { totalTokens: 1200, totalCostUsd: 0.42, inputTokens: 1000, outputTokens: 200 },
    });

    expect(activation[ACTIVATION_ROOT]).toEqual({
      phase: 'request',
      session: { id: 'sess_9', agent_id: 'agent_9', turn_index: 2 },
      model: { id: 'model-b' },
      usage: {
        total_tokens: 1200,
        total_cost_usd: 0.42,
        input_tokens: 1000,
        output_tokens: 200,
      },
      state: {},
    });
  });

  it('omits usage fields the runtime could not measure rather than reporting them as zero', () => {
    const activation = buildActivation({
      phase: 'request',
      sessionId: 'sess_9',
      usage: { totalTokens: 10 },
    });
    const usage = (activation[ACTIVATION_ROOT] as { usage: Record<string, unknown> }).usage;

    expect(usage).toEqual({ total_tokens: 10 });
    expect('total_cost_usd' in usage).toBe(false);
  });

  it('exposes supplied state, and an empty map when there is none', () => {
    const event = toolCall('Bash', {});

    expect(buildActivation(event, { denials: 3 })[ACTIVATION_ROOT]).toMatchObject({
      state: { denials: 3 },
    });
    expect(buildActivation(event)[ACTIVATION_ROOT]).toMatchObject({ state: {} });
  });

  it('exposes the tool result at tool_result', () => {
    const activation = buildActivation({
      phase: 'tool_result',
      sessionId: 'sess_1',
      result: { exit_code: 0 },
    });

    expect(activation[ACTIVATION_ROOT]).toMatchObject({ result: { exit_code: 0 } });
  });
});

describe('evaluating expressions', () => {
  it('blocks a destructive shell command and leaves other commands alone', () => {
    // The rule reads: deny unless the command is safe. `on_false: deny` in the
    // guardrail turns a false here into a denial.
    const expr = compiled(
      "!(event.tool.name == 'Bash' && event.tool.input.command.contains('rm -rf /'))",
    );

    expect(expr.stateful).toBe(false);
    expect(
      expr.evaluate(buildActivation(toolCall('Bash', { command: 'rm -rf / --no-preserve-root' }))),
    ).toBe(false);
    expect(expr.evaluate(buildActivation(toolCall('Bash', { command: 'ls -la' })))).toBe(true);
    expect(expr.evaluate(buildActivation(toolCall('Read', { file_path: '/etc/passwd' })))).toBe(
      true,
    );
  });

  it('counts tool calls against a threshold held in state', () => {
    const expr = compiled('event.state.tool_calls < 10');
    const event = toolCall('Bash', { command: 'ls' });

    expect(expr.stateful).toBe(true);
    expect(expr.evaluate(buildActivation(event, { tool_calls: 9 }))).toBe(true);
    expect(expr.evaluate(buildActivation(event, { tool_calls: 10 }))).toBe(false);
  });

  it('raises an error when the expression does not produce a boolean', () => {
    const expr = compiled('event.tool.name');

    let thrown: unknown;
    try {
      expr.evaluate(buildActivation(toolCall('Bash', {})));
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(ExpressionEvaluationError);
    expect((thrown as ExpressionEvaluationError).kind).toBe('non_boolean');
  });

  it('does not coerce a truthy non-boolean into a passing verdict', () => {
    // A bare string is truthy in JavaScript. Returning `true` here would let a
    // malformed rule silently allow everything it was written to stop.
    expect(() =>
      compiled('event.tool.input.command').evaluate(
        buildActivation(toolCall('Bash', { command: 'rm -rf /' })),
      ),
    ).toThrow(ExpressionEvaluationError);
  });

  it('raises an error when a field the expression needs is absent from the event', () => {
    const expr = compiled("event.tool.input.command.contains('rm')");

    let thrown: unknown;
    try {
      expr.evaluate(buildActivation(toolCall('Read', { file_path: '/tmp/x' })));
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(ExpressionEvaluationError);
    expect((thrown as ExpressionEvaluationError).kind).toBe('runtime');
  });

  it('raises an error rather than a verdict when a state key it reads was never written', () => {
    const expr = compiled('event.state.tool_calls < 10');

    expect(() => expr.evaluate(buildActivation(toolCall('Bash', {})))).toThrow(
      ExpressionEvaluationError,
    );
  });

  it('reports an evaluation that overruns its time budget', () => {
    // A single comprehension over a sizeable collection; with a 0 ms budget any
    // non-instant evaluation overruns.
    const expr = compiled('event.tool.input.rows.all(i, i >= 0)');
    const rows = Array.from({ length: 200 }, (_unused, index) => index);

    let thrown: unknown;
    try {
      expr.evaluate(buildActivation(toolCall('Bash', { rows })), { timeoutMs: 0 });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(ExpressionEvaluationError);
    expect((thrown as ExpressionEvaluationError).kind).toBe('timeout');
  });

  it('does not report a timeout for an expression that finishes inside the budget', () => {
    const expr = compiled("event.tool.name == 'Bash'");

    expect(
      expr.evaluate(buildActivation(toolCall('Bash', {})), { timeoutMs: DEFAULT_EVAL_TIMEOUT_MS }),
    ).toBe(true);
  });
});

describe('limits', () => {
  it('caps expression source length', () => {
    expect(MAX_EXPRESSION_LENGTH).toBe(4096);
  });

  it('carries a default evaluation time budget', () => {
    expect(DEFAULT_EVAL_TIMEOUT_MS).toBe(50);
  });
});

describe('resource bounds', () => {
  it('rejects nested comprehensions, whose cost multiplies over model-controlled input', () => {
    // A depth-2 nest over an event-derived list is N², seconds of work for a
    // large N — and N is not known at authoring time, so even a shallow nest is
    // refused.
    const nested = 'event.tool.input.paths.all(a, event.tool.input.paths.all(b, a != b))';
    expect(compileExpression(nested).ok).toBe(false);

    // Deeper literal nests are refused the same way, well under the length cap.
    let expr = 'true';
    for (let i = 0; i < 8; i += 1) expr = `[0,1,2,3,4,5,6,7,8,9].all(v${i}, ${expr})`;
    expect(expr.length).toBeLessThan(MAX_EXPRESSION_LENGTH);
    expect(compileExpression(expr).ok).toBe(false);
  });

  it('rejects a very deep operator chain instead of throwing a stack overflow', () => {
    const expr = `${Array.from({ length: 1500 }, () => '1').join('+')} == 1`;
    expect(expr.length).toBeLessThan(MAX_EXPRESSION_LENGTH);
    // Must be a returned failure, never an uncaught RangeError out of compile.
    const result = compileExpression(expr);
    expect(result.ok).toBe(false);
  });

  it('still accepts an ordinary shallow expression and a single comprehension', () => {
    expect(compileExpression('event.usage.total_cost_usd > 5').ok).toBe(true);
    expect(compileExpression("event.tool.input.files.all(f, f != '')").ok).toBe(true);
  });
});
