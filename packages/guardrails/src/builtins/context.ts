// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { BuiltinEvaluator, EvaluatorContext } from '../engine.js';
import type { GuardrailOutcome, StateUpdate } from '../types.js';
import { numberParam, stringParam } from './helpers.js';

/**
 * Progress detectors.
 *
 * Both watch a rolling window of recent activity for the signature of an agent
 * that is repeating itself rather than advancing. Neither can be certain, which
 * is why both default to asking rather than denying: the cost of interrupting
 * real work is higher than the cost of one confirmation.
 */

const LOOP_WINDOW_KEY = 'recent_tool_calls';
const THRASH_WINDOW_KEY = 'recent_results';

function windowOf(state: Readonly<Record<string, unknown>>, key: string): string[] {
  const value = state[key];
  return Array.isArray(value) ? value.filter((v): v is string => typeof v === 'string') : [];
}

/** A stable signature for a call, so an identical repeat is recognisable. */
function canonicalJsonValue(value: unknown, ancestors: WeakSet<object>): unknown {
  if (value === null || typeof value !== 'object') return value;
  if (ancestors.has(value)) throw new TypeError('circular tool input');

  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return value.map((item) => canonicalJsonValue(item, ancestors));
    }

    const canonical: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      canonical[key] = canonicalJsonValue((value as Record<string, unknown>)[key], ancestors);
    }
    return canonical;
  } finally {
    ancestors.delete(value);
  }
}

function callSignature(ctx: EvaluatorContext): string | undefined {
  const tool = ctx.event.tool;
  if (!tool) return undefined;
  let input: string;
  try {
    input = JSON.stringify(canonicalJsonValue(tool.input ?? {}, new WeakSet<object>()));
  } catch {
    // Circular or unserialisable input: fall back to the tool alone. Coarser,
    // so it may over-match — which costs a confirmation, not a missed loop.
    input = '';
  }
  return `${tool.name}:${input}`;
}

function verdictFrom(params: Record<string, unknown>, reason: string): GuardrailOutcome {
  const action = stringParam(params, 'action') === 'deny' ? 'deny' : 'ask';
  return { verdict: action, reason };
}

function recordWindow(key: string, next: string[], limit: number): StateUpdate[] {
  return [{ scope: 'session', key, action: 'set', value: next.slice(-limit) }];
}

export const detectLoop: BuiltinEvaluator = (ctx) => {
  const signature = callSignature(ctx);
  if (!signature) return undefined;

  const threshold = numberParam(ctx.params, 'threshold', 3);
  const windowSize = numberParam(ctx.params, 'window', 10);
  const recent = windowOf(ctx.state, LOOP_WINDOW_KEY);
  const next = [...recent, signature].slice(-windowSize);

  const repeats = next.filter((entry) => entry === signature).length;
  if (repeats >= threshold) {
    return {
      ...verdictFrom(
        ctx.params,
        `This tool has been called with identical arguments ${repeats} times; the agent may be looping.`,
      ),
      // Reset the window on report, so an approved continuation does not
      // re-trigger on the very next call. This applies to the `ask` action,
      // where the reset rides in the intended updates and is persisted on
      // approval; with `action: deny` the reset is withheld like any deny write,
      // so the identical call stays blocked until a different one clears the
      // window — a looping call staying blocked is the intended behaviour.
      stateUpdates: [{ scope: 'session', key: LOOP_WINDOW_KEY, action: 'set', value: [] }],
    };
  }

  return { verdict: 'allow', stateUpdates: recordWindow(LOOP_WINDOW_KEY, next, windowSize) };
};

/** Bounds recursion into a nested result, and with it any reference cycle. */
const MAX_RESULT_DEPTH = 8;

/**
 * Whether a tool result reads as a failure.
 *
 * Heuristic by necessity: tool results are free-form. It looks for the shapes
 * failures usually take rather than trying to be exhaustive.
 */
function looksLikeFailure(result: unknown, depth = 0): boolean {
  if (result === null || result === undefined || depth > MAX_RESULT_DEPTH) return false;
  // The dominant Anthropic result shape is a `content` array of blocks; without
  // this a failure block inside it reads as success.
  if (Array.isArray(result)) return result.some((item) => looksLikeFailure(item, depth + 1));
  if (typeof result === 'object') {
    const record = result as Record<string, unknown>;
    // `error: null` and `error: false` are "no error" envelopes; only a present,
    // meaningful error counts.
    const error = record['error'];
    if (record['is_error'] === true) return true;
    if (error !== undefined && error !== null && error !== false && error !== '') return true;
    return looksLikeFailure(record['content'] ?? record['output'] ?? record['text'], depth + 1);
  }
  if (typeof result !== 'string') return false;
  return /\b(error|exception|traceback|failed|failure|not found|denied|refused)\b/i.test(result);
}

export const detectThrashing: BuiltinEvaluator = (ctx) => {
  const failed = looksLikeFailure(ctx.event.result);
  const consecutiveThreshold = numberParam(ctx.params, 'consecutive_threshold', 3);
  const windowSize = numberParam(ctx.params, 'window', 10);

  const recent = windowOf(ctx.state, THRASH_WINDOW_KEY);
  const next = [...recent, failed ? 'fail' : 'ok'].slice(-windowSize);

  let streak = 0;
  for (let i = next.length - 1; i >= 0 && next[i] === 'fail'; i -= 1) streak += 1;

  if (streak >= consecutiveThreshold) {
    // Only deny is reachable here: this fires after the tool has already run, so
    // there is nothing left to approve — and a deny persists nothing, so there is
    // no point emitting a window reset that would be withheld. The window clears
    // on the next non-failing result, through the allow path below.
    return {
      verdict: 'deny',
      reason: `The last ${streak} tool results were failures; the agent appears to be retrying rather than progressing.`,
    };
  }

  return { verdict: 'allow', stateUpdates: recordWindow(THRASH_WINDOW_KEY, next, windowSize) };
};
