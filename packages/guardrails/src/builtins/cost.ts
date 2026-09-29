// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { BuiltinEvaluator, EvaluatorContext } from '../engine.js';
import { SHARED_USAGE_KEYS } from '../state.js';
import { supportsAsk, type GuardrailOutcome, type StateScope, type StateUpdate } from '../types.js';
import { allowWith, counter, deny, flagged, stringList, stringParam } from './helpers.js';

/**
 * Budget guardrails.
 *
 * All three share an evaluation order, and the order is load-bearing:
 *
 *   1. an unmeasurable session asks before anything else
 *   2. the hard cap, which may be a downgrade gate rather than a stop
 *   3. soft thresholds, which never run once over the cap
 *
 * Checking the cap before establishing that spend is even measurable would let
 * an unpriced session past every budget at an apparent zero.
 */

/** Highest soft threshold the client has already approved. */
const APPROVED_KEY = 'budget_ask_approved';
/** Set once the client has accepted running without measurable spend. */
const UNPRICED_APPROVED_KEY = 'budget_unpriced_approved';
/** Set after the first unpriced request, while the approval is still pending. */
const UNPRICED_PENDING_KEY = 'budget_unpriced_pending';

/**
 * An approval answers a question asked about one dispatch. A rule whose
 * threshold is measured per subagent keys its approvals by that dispatch, so
 * approving subagent A's spend cannot silence the prompt about subagent B's.
 * Session-wide budgets keep the bare key: their question is about the session
 * total, and one answer legitimately covers every actor in it.
 */
function dispatchKey(base: string, ctx: EvaluatorContext): string {
  return ctx.event.subagentId === undefined ? base : `${base}@${ctx.event.subagentId}`;
}

const numbers = (params: Record<string, unknown>, key: string): number[] => {
  const value = params[key];
  return Array.isArray(value)
    ? value
        .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
        .sort((a, b) => a - b)
    : [];
};

/**
 * Spend is unmeasurable when the session has consumed tokens but no cost was
 * computed for them — the model has no price data.
 *
 * Absence of a cost is not zero cost. A budget that cannot measure has to say
 * so; passing at an apparent $0 would silently disable every cap that depends
 * on it.
 */
function isUnpriced(ctx: EvaluatorContext): boolean {
  const usage = ctx.event.usage;
  if (!usage) return false;
  // Every token field, including the cache buckets `computeCost` prices — a
  // cache-only snapshot is billable and must not read as "consumed nothing". A
  // `NaN` field counts as consumed too (`!== 0`), so the malformation that
  // produces a NaN cost cannot slip past this gate.
  const tokenFields = [
    usage.inputTokens,
    usage.outputTokens,
    usage.totalTokens,
    usage.cacheReadInputTokens,
    usage.cacheCreationEphemeral5mInputTokens,
    usage.cacheCreationEphemeral1hInputTokens,
  ];
  const consumed = tokenFields.some((value) => typeof value === 'number' && value !== 0);
  if (!consumed) return false;
  // Undefined is unpriced, and so is any value that is not a finite, non-negative
  // number: a `NaN` or negative cost (a malformed usage through `computeCost`) is
  // unmeasurable, not a measured $0, and must not read as within budget.
  const cost = usage.totalCostUsd;
  return cost === undefined || !Number.isFinite(cost) || cost < 0;
}

/**
 * The verdict when spend cannot be measured. If already approved, the session
 * runs unbudgeted. Otherwise it asks where it can, and where it cannot — a
 * `request` turn — it denies rather than run unmetered behind a fabricated $0.
 */
function unpricedOutcome(
  ctx: EvaluatorContext,
  scope: StateScope,
  perDispatch: boolean,
  unpriced: boolean,
): GuardrailOutcome | undefined {
  if (!unpriced) return undefined;
  const key = perDispatch ? dispatchKey(UNPRICED_APPROVED_KEY, ctx) : UNPRICED_APPROVED_KEY;
  if (ctx.state[key]) return undefined;

  const configured = stringParam(ctx.params, 'on_unpriced');
  if (configured === 'allow') return undefined;
  if (configured === 'deny') {
    return deny(
      'This session is using a model with no price data, so its budget cannot be enforced.',
    );
  }

  if (supportsAsk(ctx.event.phase)) {
    return {
      verdict: 'ask',
      reason:
        'This session is running on a model with no price data, so spend cannot be tracked and ' +
        'the budget cannot be enforced. Continue without budget enforcement?',
      stateUpdates: [{ scope, key, action: 'set', value: true }],
    };
  }

  // `request` has no approval round trip. Let the first unpriced request reach
  // the next tool-call confirmation and remember that the question is pending;
  // if another request arrives without an approval, fail closed so a text-only
  // session cannot run unmetered forever.
  const pendingKey = perDispatch ? dispatchKey(UNPRICED_PENDING_KEY, ctx) : UNPRICED_PENDING_KEY;
  if (ctx.state[pendingKey]) {
    return deny(
      'This session is still using a model with no price data and no approval to run unmetered.',
    );
  }
  return {
    verdict: 'allow',
    stateUpdates: [{ scope, key: pendingKey, action: 'set', value: true }],
  };
}

/**
 * Whether the active model is one the budget blocks once over its cap.
 *
 * An empty or absent list blocks every model, making the cap a hard stop. A
 * non-empty list makes it a downgrade gate: over budget, expensive models are
 * refused and cheaper ones still run, so the session degrades instead of dying.
 * An indeterminable model counts as blocked — otherwise a budget could be
 * evaded by making the model unreadable.
 */
function modelIsBlocked(ctx: EvaluatorContext): boolean {
  // Blank tokens are dropped: `''` is a substring of every id (an unconditional
  // hard stop), and `' '` is a substring of none (a gate that never blocks). A
  // token is meaningful only once trimmed.
  const expensive = stringList(ctx.params, 'expensive_models')
    .map((token) => token.trim())
    .filter((token) => token.length > 0);
  if (expensive.length === 0) return true;
  const model = ctx.event.modelId;
  if (!model) return true;
  const lower = model.toLowerCase();
  return expensive.some((token) => lower.includes(token.toLowerCase()));
}

function budgetOutcome(
  ctx: EvaluatorContext,
  spent: number,
  scope: StateScope,
  label: string,
  perDispatch = false,
  unpriced = isUnpriced(ctx),
): GuardrailOutcome | undefined {
  // Whether this phase has an approval round trip. A budget that would `ask`
  // where nothing can answer must hold the question instead: the engine would
  // otherwise degrade it to a deny the client cannot clear, and the deny would
  // withhold the approval write and so repeat on every event. Only the hard cap
  // (a deny in its own right) fires where asking is impossible.
  const canAsk = supportsAsk(ctx.event.phase);

  // An unmeasurable session is judged before the cap, at every phase — a
  // fabricated $0 must never slip past the budget at `request`.
  const unpricedVerdict = unpricedOutcome(ctx, scope, perDispatch, unpriced);
  if (unpricedVerdict) return unpricedVerdict;

  const cap = ctx.params['max_cost_usd'];
  if (typeof cap === 'number' && spent >= cap) {
    if (modelIsBlocked(ctx)) {
      return deny(
        `${label} has reached its $${cap.toFixed(2)} budget (spent $${spent.toFixed(2)}). ` +
          'Switch to a less expensive model to continue.',
      );
    }
    // Already on a permitted model: the downgrade the gate exists to encourage
    // has happened, so allow — and skip the soft thresholds, which are all
    // below a cap that has been passed.
    return { verdict: 'allow' };
  }

  if (!canAsk) return undefined;

  const thresholds = numbers(ctx.params, 'ask_thresholds_usd');
  const crossed = thresholds.filter((t) => spent >= t).pop();
  if (crossed === undefined) return undefined;

  const approvedKey = perDispatch ? dispatchKey(APPROVED_KEY, ctx) : APPROVED_KEY;
  const approved = counter(ctx.state, approvedKey);
  if (crossed <= approved) return undefined;

  return {
    verdict: 'ask',
    reason: `${label} has passed $${crossed.toFixed(2)} (spent $${spent.toFixed(2)}). Continue?`,
    // Written only if the client approves — the engine withholds state on an
    // ask. That is what makes a refusal re-ask rather than silently arm the
    // threshold as accepted.
    stateUpdates: [{ scope, key: approvedKey, action: 'set', value: crossed }],
  };
}

const SESSION_COST_KEY = SHARED_USAGE_KEYS.sessionCostUsd;

export const costBudget: BuiltinEvaluator = (ctx) => {
  // The larger of the event's reported cost and the runtime's accumulated spend,
  // mirroring `tokenBudget`. The counter is maintained by the runtime independent
  // of any verdict, so a malformed `0` reported after a breach cannot fall below
  // the spend already recorded and buy a free call. `??` would not help here: a
  // present-but-zero cost is not nullish, so only `Math.max` against the counter
  // holds the line.
  const reported = ctx.event.usage?.totalCostUsd;
  const spent = Math.max(
    typeof reported === 'number' && Number.isFinite(reported) ? reported : 0,
    counter(ctx.state, SESSION_COST_KEY),
  );
  return budgetOutcome(
    ctx,
    spent,
    'session',
    'This session',
    false,
    // A later event carrying no usage payload must not read as priced again:
    // Registry marks the session once it reports usage it could not price, and
    // that marker outlives the event that set it.
    isUnpriced(ctx) || flagged(ctx.state, SHARED_USAGE_KEYS.sessionHasUnpriced),
  );
};

export const userDailyCostBudget: BuiltinEvaluator = (ctx) =>
  budgetOutcome(
    ctx,
    counter(ctx.state, SHARED_USAGE_KEYS.dailyCostUsd),
    'subject_window',
    "Today's spend",
    false,
    isUnpriced(ctx) || flagged(ctx.state, SHARED_USAGE_KEYS.dailyCostUnpriced),
  );

export const subagentCostBudget: BuiltinEvaluator = (ctx) => {
  // Only meaningful for a dispatched subagent; the coordinator's own spend is
  // the session budget's concern.
  if (!ctx.event.subagentId) return undefined;
  return budgetOutcome(
    ctx,
    counter(ctx.state, `${SHARED_USAGE_KEYS.subagentCostPrefix}${ctx.event.subagentId}`),
    'session',
    'This subagent',
    // The threshold is measured per dispatch, so its approvals are too.
    true,
    // Namespaced by dispatch for the same reason the counter is: two concurrent
    // dispatches of one subagent type must not inherit each other's marker.
    isUnpriced(ctx) ||
      flagged(ctx.state, `${SHARED_USAGE_KEYS.subagentHasUnpricedPrefix}${ctx.event.subagentId}`),
  );
};

const TOKENS_KEY = SHARED_USAGE_KEYS.totalTokens;

/**
 * A budget in tokens rather than currency.
 *
 * Exists so a deployment with no price data can still cap a session. Tokens are
 * always known; money is not.
 */
export const tokenBudget: BuiltinEvaluator = (ctx) => {
  const max = ctx.params['max_total_tokens'];
  // The larger of the reported total and the accumulated counter. `??` would let
  // a reported `0` shadow a non-zero counter, since only nullish falls back — a
  // single zero-valued field must not reset the budget.
  const reported = ctx.event.usage?.totalTokens;
  const used = Math.max(
    typeof reported === 'number' && Number.isFinite(reported) ? reported : 0,
    counter(ctx.state, TOKENS_KEY),
  );

  if (typeof max === 'number' && used >= max) {
    return deny(`This session has reached its budget of ${max} tokens (used ${used}).`);
  }

  // Like the cost budgets: a soft threshold only surfaces where it can be
  // answered, so it does not degrade to a self-repeating deny at `request`.
  if (!supportsAsk(ctx.event.phase)) return undefined;

  const thresholds = numbers(ctx.params, 'ask_thresholds');
  const crossed = thresholds.filter((t) => used >= t).pop();
  if (crossed === undefined) return undefined;

  const approved = counter(ctx.state, APPROVED_KEY);
  if (crossed <= approved) return undefined;

  return {
    verdict: 'ask',
    reason: `This session has passed ${crossed} tokens (used ${used}). Continue?`,
    stateUpdates: [{ scope: 'session', key: APPROVED_KEY, action: 'set', value: crossed }],
  };
};

/** Re-exported for the detectors, which share the counter helper. */
export { allowWith, counter, type StateUpdate };
