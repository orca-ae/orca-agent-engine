// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { Verdict } from './lattice.js';

/**
 * Core guardrail types. See `docs/managed-agents/guardrails.md`.
 */

/**
 * Evaluation points. All six are modelled so a guardrail can declare any of
 * them; a runtime wires the subset its enforcement points can actually reach,
 * and `compose` never hands a guardrail to a phase the runtime does not fire.
 */
export const PHASES = [
  'request',
  'tool_call',
  'tool_result',
  'response',
  'llm_request',
  'llm_response',
] as const;

export type Phase = (typeof PHASES)[number];

/**
 * Phases that gate an action *before* it happens fail closed: an evaluation
 * error denies. The rest observe after the fact, where suppressing output on an
 * internal error is worse than the risk it mitigates.
 */
const FAIL_CLOSED_PHASES: ReadonlySet<Phase> = new Set<Phase>([
  'request',
  'tool_call',
  'llm_request',
]);

export function failsClosed(phase: Phase): boolean {
  return FAIL_CLOSED_PHASES.has(phase);
}

/**
 * `ask` needs a client round trip to resolve. Only `tool_call` has one today
 * (the tool-confirmation exchange), which is what lets guardrails ship without
 * adding any new client protocol. `tool_result` is excluded on its own merits:
 * the tool has already run, so there is nothing left to approve.
 */
const ASK_CAPABLE_PHASES: ReadonlySet<Phase> = new Set<Phase>(['tool_call']);

export function supportsAsk(phase: Phase): boolean {
  return ASK_CAPABLE_PHASES.has(phase);
}

/**
 * Phases some enforcement point in this tree actually fires. `response` and
 * `llm_response` are modelled but reach no runtime, and `llm_request` awaits
 * the model-endpoint interceptor, so a rule authored onto one of them would be
 * accepted and then never evaluated.
 *
 * A builtin whose catalog entry declares an unfired phase keeps it — the
 * catalog describes the rule, not this tree's wiring, so the rule starts
 * working when the interceptor lands with no stored row to migrate. What is
 * refused is *asking* for such a phase explicitly, which can only produce a
 * guardrail that silently does nothing.
 */
const ENFORCED_PHASES: ReadonlySet<Phase> = new Set<Phase>([
  'request',
  'tool_call',
  'tool_result',
]);

export function isEnforced(phase: Phase): boolean {
  return ENFORCED_PHASES.has(phase);
}

/** Where a guardrail's authority comes from. Also the composition order. */
export const TIERS = ['session', 'agent', 'workspace', 'organization'] as const;

export type Tier = (typeof TIERS)[number];

/**
 * How a guardrail attaches. `explicit` applies only where an agent or session
 * names it; the other two apply to everything in their scope.
 */
export const SCOPES = ['organization', 'workspace', 'explicit'] as const;

export type Scope = (typeof SCOPES)[number];

export interface BuiltinRule {
  kind: 'builtin';
  builtin: string;
  params?: Record<string, unknown>;
}

export interface ExpressionRule {
  kind: 'expression';
  expression: string;
  /** Verdict when the expression evaluates false. `allow` would be a no-op. */
  onFalse: Exclude<Verdict, 'allow'>;
  reason?: string;
}

export type GuardrailRule = BuiltinRule | ExpressionRule;

export interface Guardrail {
  id: string;
  name: string;
  enabled: boolean;
  phases: readonly Phase[];
  scope: Scope;
  rule: GuardrailRule;
}

/** A guardrail paired with the tier it was resolved from. */
export interface TieredGuardrail {
  guardrail: Guardrail;
  tier: Tier;
  /**
   * Set when the guardrail came from a subagent's own agent record rather than
   * the coordinator's, so per-subagent state can be namespaced apart.
   */
  subagentId?: string;
}

export interface ToolEvent {
  name: string;
  input: Record<string, unknown>;
}

export interface UsageSnapshot {
  inputTokens?: number;
  outputTokens?: number;
  cacheReadInputTokens?: number;
  cacheCreationEphemeral5mInputTokens?: number;
  cacheCreationEphemeral1hInputTokens?: number;
  totalTokens?: number;
  /**
   * Absent when the session's model has no price data. Absence means
   * *unpriced*, which is not the same as zero — a guardrail that cannot
   * measure spend must say so rather than pass at $0.
   */
  totalCostUsd?: number;
}

export interface GuardrailEvent {
  phase: Phase;
  sessionId: string;
  agentId?: string;
  /** Present when a dispatched subagent is acting rather than the coordinator. */
  subagentId?: string;
  turnIndex?: number;
  modelId?: string;
  tool?: ToolEvent;
  /** Present at `tool_result`. */
  result?: unknown;
  /** Present at `request`. */
  userText?: string;
  /**
   * Exact serialized request bytes presented to the model endpoint. Present at
   * `llm_request`; the harness evaluates and forwards this same string so a
   * guardrail cannot inspect a lossy reconstruction.
   */
  serializedRequest?: string;
  usage?: UsageSnapshot;
}

/** What one guardrail returns. `undefined` state updates means it wrote nothing. */
export interface GuardrailOutcome {
  verdict: Verdict;
  reason?: string;
  stateUpdates?: readonly StateUpdate[];
}

/** The engine's answer for one event. */
export interface Decision {
  verdict: Verdict;
  /** In evaluation order. Empty when nothing had an opinion. */
  reasons: readonly string[];
  /**
   * Updates the caller should persist. Empty when the verdict is `ask` — those
   * writes are withheld until the client approves, which is what makes a
   * refused approval re-ask instead of silently arming itself.
   */
  stateUpdates: readonly StateUpdate[];
  /**
   * Every update the guardrails asked for, including ones withheld because the
   * verdict was `ask` or the run was read-only. Lets a caller show what a rule
   * *would* do without arming it.
   */
  intendedStateUpdates: readonly StateUpdate[];
  /** The guardrail that produced a terminal `deny`, for attribution. */
  deniedBy?: string;
  /**
   * Evaluation errors the engine folded into a verdict rather than throwing.
   * Empty in the normal case. A rule that errors on a fail-open phase is
   * swallowed to an abstain, which is otherwise indistinguishable from a rule
   * that had no opinion — so a rule that has silently stopped enforcing shows up
   * only here. Reporting is the caller's to do; the engine stays pure.
   */
  errors: readonly GuardrailEvaluationError[];
}

/** One guardrail that could not be evaluated, and what the engine did about it. */
export interface GuardrailEvaluationError {
  guardrailId: string;
  /**
   * True when the phase failed closed and this became a deny; false when it
   * failed open and was swallowed to an abstain — the silent case worth alerting
   * on.
   */
  failedClosed: boolean;
  message: string;
}

export const STATE_ACTIONS = ['set', 'increment', 'delete', 'append'] as const;

export type StateAction = (typeof STATE_ACTIONS)[number];

export const STATE_SCOPES = ['turn', 'session', 'subject_window'] as const;

export type StateScope = (typeof STATE_SCOPES)[number];

export interface StateUpdate {
  scope: StateScope;
  key: string;
  action: StateAction;
  value?: unknown;
}

export type { Verdict };
