// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { maxVerdict, type Verdict } from './lattice.js';
import type { GuardrailStateStore, StateSnapshot } from './state.js';
import {
  TIERS,
  failsClosed,
  supportsAsk,
  type Decision,
  type ExpressionRule,
  type Guardrail,
  type GuardrailEvaluationError,
  type GuardrailEvent,
  type GuardrailOutcome,
  type StateScope,
  type StateUpdate,
  type Tier,
} from './types.js';

/**
 * The composition engine.
 *
 * Every verdict this module produces comes from `maxVerdict`, so the guarantee
 * that a guardrail can tighten but never loosen is structural rather than
 * something each branch has to be careful about. The engine reads state but
 * never writes it: it returns the updates a caller should persist, which keeps
 * persistence policy — and the transaction it happens in — with the caller.
 */

/** What one guardrail evaluator receives. */
export interface EvaluatorContext {
  event: GuardrailEvent;
  params: Record<string, unknown>;
  /** Empty in the stateless pass. */
  state: StateSnapshot;
  guardrail: Guardrail;
}

/** Returning `undefined` abstains, which is indistinguishable from allowing. */
export type BuiltinEvaluator = (ctx: EvaluatorContext) => GuardrailOutcome | undefined;

export type ExpressionEvaluator = (
  rule: ExpressionRule,
  ctx: EvaluatorContext,
) => GuardrailOutcome | undefined;

/**
 * A guardrail with the facts the engine needs that the record itself does not
 * carry: which tier's authority it holds, and whether evaluating it requires
 * state. Both are resolved upstream — statefulness from the type catalog for a
 * builtin, or from the compiled expression for an expression rule.
 */
export interface PreparedGuardrail {
  guardrail: Guardrail;
  tier: Tier;
  stateful: boolean;
  /**
   * Which state scope this guardrail reads. Declared by its type — a per-turn
   * dispatch cap and a per-principal daily budget are both stateful but read
   * entirely different stores, so defaulting every guardrail to session scope
   * would silently hand two of them the wrong snapshot.
   */
  stateScope?: StateScope;
  /**
   * Binds a guardrail resolved from a dispatched subagent's own record to that
   * subagent: it applies only to events carrying the same `subagentId`, and its
   * private state is namespaced apart from every other dispatch's.
   */
  subagentId?: string;
}

export interface EngineOptions {
  builtins: ReadonlyMap<string, BuiltinEvaluator>;
  expression?: ExpressionEvaluator;
  /**
   * Absent means state is unreachable here — the caller gets the stateless pass
   * only. That is the supported mode for enforcement points that decide tool
   * exposure before any tool runs.
   */
  store?: GuardrailStateStore;
  /**
   * The verdict the fold starts from, normally the agent's permission policy
   * for this tool. Guardrails compose on top of it; they cannot undo it.
   */
  seed?: Verdict;
  /** Evaluate and report intent without producing anything to persist. */
  readOnly?: boolean;
}

const TIER_ORDER: Readonly<Record<Tier, number>> = TIERS.reduce(
  (acc, tier, index) => ({ ...acc, [tier]: index }),
  {} as Record<Tier, number>,
);

export function evaluateGuardrails(
  guardrails: readonly PreparedGuardrail[],
  event: GuardrailEvent,
  opts: EngineOptions,
): Decision {
  // A deny seed is terminal: the permission policy already refused this action,
  // and guardrails can only tighten. Returning before any rule runs is what
  // guarantees a call that can never execute cannot advance a counter — or
  // read state at all.
  if (opts.seed === 'deny') {
    return { verdict: 'deny', reasons: [], stateUpdates: [], intendedStateUpdates: [], errors: [] };
  }

  const applicable = guardrails
    .filter((p) => p.guardrail.enabled && p.guardrail.phases.includes(event.phase))
    // A guardrail resolved from a dispatched subagent holds authority over that
    // subagent alone; rules with no subagent binding apply to every event.
    .filter((p) => p.subagentId === undefined || p.subagentId === event.subagentId)
    // Stateful guardrails cannot run without a store. Dropping them here rather
    // than failing keeps a stateless caller usable; the caller knows it asked
    // for a partial evaluation because it supplied no store.
    .filter((p) => !p.stateful || opts.store !== undefined);

  // Stateless first: it is cheap, needs no I/O, and can short-circuit before any
  // state is read. Within each pass, authority order decides.
  const ordered = [
    ...sortByTier(applicable.filter((p) => !p.stateful)),
    ...sortByTier(applicable.filter((p) => p.stateful)),
  ];

  // The seed runs through the same phase degradation as any verdict: a permission
  // policy of `ask` on a phase that cannot ask degrades to deny rather than fall
  // through unanswerable.
  let verdict: Verdict = resolveVerdict(opts.seed ?? 'allow', event);
  const reasons: string[] = [];
  const intended: StateUpdate[] = [];
  let persistable: StateUpdate[] = [];
  let deniedBy: string | undefined;
  const errors: GuardrailEvaluationError[] = [];

  for (const prep of ordered) {
    const outcome = evaluateOne(prep, event, opts, errors);
    if (!outcome) continue;

    const resolved = resolveVerdict(outcome.verdict, event);
    if (outcome.reason && resolved !== 'allow') reasons.push(outcome.reason);

    // Namespaced on the way out, so the keys a caller persists are the keys a
    // later read view resolves. See `stateNamespace`.
    const updates = (outcome.stateUpdates ?? []).map((u) => ({
      ...u,
      key: stateNamespace(prep) + u.key,
    }));
    intended.push(...updates);

    if (resolved === 'deny') {
      verdict = 'deny';
      deniedBy = prep.guardrail.id;
      // Writes from guardrails that already allowed stand; this one's do not.
      break;
    }

    verdict = maxVerdict(verdict, resolved);
    // Only an allow may persist. An ask is unresolved until the client answers,
    // so its writes stay out of the persistable set entirely — otherwise a later
    // deny (which does not clear an earlier ask's writes) would hand back a
    // threshold recorded as approved that nobody approved.
    if (resolved === 'allow') persistable.push(...updates);
  }

  // When to withhold the allow writes gathered above. An `ask` is unresolved and
  // readOnly persists nothing by definition. A `deny` withholds only where it
  // blocks the action before it happens — a tool call the budget denied must not
  // advance the tool-call cap. At an observational phase (`tool_result` and
  // later) the tool has already run, so a deny there only suppresses output: an
  // allowing guardrail's record of what happened — a confidential read the DLP
  // check will need next turn — still stands.
  if (opts.readOnly || verdict === 'ask' || (verdict === 'deny' && failsClosed(event.phase))) {
    persistable = [];
  }

  return {
    verdict,
    reasons,
    stateUpdates: persistable,
    ...(deniedBy !== undefined ? { deniedBy } : {}),
    intendedStateUpdates: intended,
    errors,
  };
}

function sortByTier(items: readonly PreparedGuardrail[]): PreparedGuardrail[] {
  return [...items].sort((a, b) => TIER_ORDER[a.tier] - TIER_ORDER[b.tier]);
}

/** Marks a persisted key as belonging to one guardrail's private state. */
const PRIVATE_STATE_PREFIX = 'g:';

/**
 * The key namespace one guardrail's private state lives under, including the
 * dispatch it was resolved from. Independent rules read the same scope record,
 * so without this a `token_budget` approval would satisfy a `cost_budget`
 * threshold and two counters with one name would advance each other.
 *
 * Keys without the private prefix belong to the runtime — usage counters like
 * `daily_cost_usd` that many rules legitimately read (`SHARED_USAGE_KEYS`).
 * Evaluators write through this namespace unconditionally, so a rule cannot
 * clobber a shared counter even by writing its exact name.
 */
function stateNamespace(prep: PreparedGuardrail): string {
  const dispatch = prep.subagentId === undefined ? '' : `@${prep.subagentId}`;
  return `${PRIVATE_STATE_PREFIX}${prep.guardrail.id}${dispatch}:`;
}

/**
 * What one guardrail sees: every shared runtime-written key, plus its own
 * private keys with the namespace stripped — so an evaluator reads and writes
 * bare names and never handles the prefix itself. A private key shadows a
 * shared key of the same name; the rule's own state is the more specific fact.
 */
function namespacedView(snapshot: StateSnapshot, ns: string): StateSnapshot {
  const view: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(snapshot)) {
    if (!key.startsWith(PRIVATE_STATE_PREFIX)) view[key] = value;
  }
  for (const [key, value] of Object.entries(snapshot)) {
    if (key.startsWith(ns)) view[key.slice(ns.length)] = value;
  }
  return view;
}

/**
 * `ask` needs somewhere to ask. On a phase with no approval round trip the
 * honest answer is the safe one — degrade to deny rather than let an
 * unanswerable question fall through to allow.
 */
function resolveVerdict(verdict: Verdict, event: GuardrailEvent): Verdict {
  if (verdict === 'ask' && !supportsAsk(event.phase)) return 'deny';
  return verdict;
}

function evaluateOne(
  prep: PreparedGuardrail,
  event: GuardrailEvent,
  opts: EngineOptions,
  errors: GuardrailEvaluationError[],
): GuardrailOutcome | undefined {
  const { guardrail } = prep;
  try {
    const ctx: EvaluatorContext = {
      event,
      params: guardrail.rule.kind === 'builtin' ? (guardrail.rule.params ?? {}) : {},
      state:
        prep.stateful && opts.store
          ? namespacedView(opts.store.read(prep.stateScope ?? 'session'), stateNamespace(prep))
          : {},
      guardrail,
    };

    if (guardrail.rule.kind === 'expression') {
      if (!opts.expression) throw new Error('no expression evaluator configured');
      return opts.expression(guardrail.rule, ctx);
    }

    const evaluator = opts.builtins.get(guardrail.rule.builtin);
    if (!evaluator) throw new Error(`unknown guardrail type: ${guardrail.rule.builtin}`);
    return evaluator(ctx);
  } catch (error) {
    // A guardrail that cannot be evaluated is not a guardrail that passed. On a
    // phase that gates an action before it happens, an unevaluable rule denies;
    // on an observational phase, suppressing output over an internal error
    // would cost more than it protects. Either way the error is recorded, so a
    // rule that has silently stopped enforcing is visible to a caller.
    const failedClosed = failsClosed(event.phase);
    errors.push({
      guardrailId: guardrail.id,
      failedClosed,
      message: error instanceof Error ? error.message : String(error),
    });
    return failedClosed
      ? {
          verdict: 'deny',
          reason: `Guardrail "${guardrail.name}" could not be evaluated.`,
        }
      : undefined;
  }
}
