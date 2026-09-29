// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { EvaluatorContext } from '../engine.js';
import { matchesAnyToolPattern } from '../tool-names.js';
import type { GuardrailOutcome, StateScope, StateUpdate } from '../types.js';

/** Read a string-array parameter, tolerating absence. */
export function stringList(params: Record<string, unknown>, key: string): string[] {
  const value = params[key];
  return Array.isArray(value) ? value.filter((v): v is string => typeof v === 'string') : [];
}

export function numberParam(
  params: Record<string, unknown>,
  key: string,
  fallback: number,
): number {
  const value = params[key];
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

export function stringParam(params: Record<string, unknown>, key: string): string | undefined {
  const value = params[key];
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

/** A counter's current value, treating anything non-numeric as zero. */
export function counter(state: Readonly<Record<string, unknown>>, key: string): number {
  const value = state[key];
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

/**
 * A boolean-ish state flag, true for either representation the two state
 * layers can hold. Session state is jsonb and stores `true`;
 * `guardrail_counters.value_num` is `double precision NOT NULL` and stores 1,
 * because the internal route refuses a non-numeric `set` at that scope. A
 * reader that checked `=== true` would silently never see the counter form.
 */
export function flagged(state: Readonly<Record<string, unknown>>, key: string): boolean {
  const value = state[key];
  if (value === true) return true;
  return typeof value === 'number' && Number.isFinite(value) && value !== 0;
}

export function increment(scope: StateScope, key: string, by = 1): StateUpdate {
  return { scope, key, action: 'increment', value: by };
}

/** The tool this event concerns, or undefined on a non-tool phase. */
export function toolName(ctx: EvaluatorContext): string | undefined {
  return ctx.event.tool?.name;
}

export function toolMatches(ctx: EvaluatorContext, patterns: readonly string[]): boolean {
  const name = toolName(ctx);
  return name !== undefined && matchesAnyToolPattern(name, patterns);
}

export function deny(reason: string): GuardrailOutcome {
  return { verdict: 'deny', reason };
}

export function ask(reason: string): GuardrailOutcome {
  return { verdict: 'ask', reason };
}

export function allowWith(updates: readonly StateUpdate[]): GuardrailOutcome {
  return { verdict: 'allow', stateUpdates: updates };
}
