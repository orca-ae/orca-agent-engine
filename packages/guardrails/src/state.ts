// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { STATE_SCOPES, type StateScope, type StateUpdate } from './types.js';

/**
 * Guardrail state.
 *
 * A guardrail never writes state directly — it returns updates and the runtime
 * applies them. That indirection is what makes write timing enforceable: an
 * `ask` verdict can withhold every update in a batch, so a refused approval
 * leaves nothing behind to arm itself with.
 *
 * The rules here are deliberately total: every update, however malformed, has a
 * defined effect and none of them throws. A guardrail that writes nonsense must
 * degrade its own counter, not crash the evaluation that other guardrails
 * depend on. See `docs/managed-agents/guardrails.md`.
 */

/** A snapshot handed to a guardrail. Always a copy, never live store memory. */
export type StateSnapshot = Readonly<Record<string, unknown>>;

/** Initial values per scope, for restoring a session's state on resume. */
export type SeedState = Partial<Record<StateScope, Readonly<Record<string, unknown>>>>;

export interface GuardrailStateStore {
  /** Current values for a scope. Never returns a reference the caller can mutate into the store. */
  read(scope: StateScope): StateSnapshot;
  /** Apply updates in order. */
  apply(updates: readonly StateUpdate[]): void;
  /** Replace one scope atomically, used when Registry refreshes a shared window. */
  replace(scope: StateScope, values: Readonly<Record<string, unknown>>): void;
  /** Clear turn-scoped state at a turn boundary. Other scopes untouched. */
  resetTurn(): void;
}

/**
 * The shared usage vocabulary: keys the runtime writes bare, for facts many
 * rules legitimately read — measured spend, not rule bookkeeping. Everything a
 * guardrail itself writes is namespaced by the engine under its own identity,
 * so no rule can collide with these or with another rule's private state.
 */
export const SHARED_USAGE_KEYS = Object.freeze({
  /** Rolling per-principal spend, in the `subject_window` scope. */
  dailyCostUsd: 'daily_cost_usd',
  /** Whether that rolling window contains usage Registry could not price. */
  dailyCostUnpriced: 'daily_cost_unpriced',
  /** Accumulated session spend, so a malformed per-event cost cannot fall below it. */
  sessionCostUsd: 'session_cost_usd',
  /** Session token consumption, for deployments with no price data. */
  totalTokens: 'total_tokens',
  /** Registry observed at least one session usage delta it could not price. */
  sessionHasUnpriced: 'session_usage_has_unpriced',
  /** Per-dispatch spend: `subagent_cost_<subagentId>`. */
  subagentCostPrefix: 'subagent_cost_',
  /** Per-dispatch unpriced marker: `subagent_usage_has_unpriced_<subagentId>`. */
  subagentHasUnpricedPrefix: 'subagent_usage_has_unpriced_',
});

/** The default delta when an `increment` carries no value. */
const DEFAULT_INCREMENT = 1;

function isCountable(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * The delta an `increment` contributes. An absent value means one; a value that
 * is not a finite number contributes nothing, which still normalizes the key to
 * a number so later comparisons stay meaningful.
 */
function incrementDelta(value: unknown): number {
  if (value === undefined) return DEFAULT_INCREMENT;
  return isCountable(value) ? value : 0;
}

/**
 * Clone state without ever returning a mutable reference into the store.
 * `structuredClone` handles deep values, cycles, and built-in containers. The
 * fallback covers data containing an unsupported member (most commonly a
 * function) by copying its enumerable data and dropping only that member.
 */
function copyValue(value: unknown): unknown {
  try {
    return structuredClone(value);
  } catch {
    return copyUnsupportedValue(value, new WeakMap<object, unknown>());
  }
}

function copyUnsupportedValue(value: unknown, seen: WeakMap<object, unknown>): unknown {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function')) return value;
  if (typeof value === 'function') return undefined;

  const previous = seen.get(value);
  if (previous !== undefined) return previous;

  const copy: unknown[] | Record<string, unknown> = Array.isArray(value) ? [] : {};
  seen.set(value, copy);
  for (const key of Object.keys(value)) {
    try {
      (copy as Record<string, unknown>)[key] = copyUnsupportedValue(
        (value as Record<string, unknown>)[key],
        seen,
      );
    } catch {
      // A throwing accessor is not state data. Omitting it preserves totality
      // without exposing the original object by reference.
    }
  }
  return copy;
}

/**
 * Apply one update to a plain record, in place. Exported so a persistent store
 * applies identical semantics.
 *
 * - `set` — replace the value. With no value the key is removed instead, so no
 *   key ever holds `undefined` and in-memory state round trips through JSON
 *   unchanged.
 * - `increment` — numeric add. A missing key counts as zero and a value that is
 *   not a finite number is overwritten by the delta rather than throwing.
 * - `delete` — remove the key.
 * - `append` — push onto an array. A missing key starts empty and a non-array
 *   value is replaced by a single-element array. With no value there is nothing
 *   to append, so the key is left alone.
 *
 * An action outside the vocabulary is ignored.
 */
export function applyStateUpdate(target: Record<string, unknown>, update: StateUpdate): void {
  const { key, value } = update;

  switch (update.action) {
    case 'set':
      if (value === undefined) delete target[key];
      else {
        const copied = copyValue(value);
        if (copied === undefined) delete target[key];
        else target[key] = copied;
      }
      return;

    case 'increment': {
      const current = target[key];
      target[key] = (isCountable(current) ? current : 0) + incrementDelta(value);
      return;
    }

    case 'delete':
      delete target[key];
      return;

    case 'append': {
      if (value === undefined) return;
      const copied = copyValue(value);
      if (copied === undefined) return;
      const current = target[key];
      target[key] = Array.isArray(current) ? [...current, copied] : [copied];
      return;
    }

    default:
      return;
  }
}

function copyRecord(source: Readonly<Record<string, unknown>>): Record<string, unknown> {
  return copyValue(source) as Record<string, unknown>;
}

function isKnownScope(scope: string): scope is StateScope {
  return (STATE_SCOPES as readonly string[]).includes(scope);
}

/**
 * The state a session holds while it runs. A durable store keeps this as its
 * synchronous layer — a running session always reads its own most recent write,
 * and flushes happen behind it — so the semantics here are the ones that matter
 * to a guardrail regardless of what backs them.
 */
export class InMemoryGuardrailStateStore implements GuardrailStateStore {
  readonly #scopes: Record<StateScope, Record<string, unknown>>;

  constructor(seed: SeedState = {}) {
    this.#scopes = {
      turn: {},
      session: {},
      subject_window: {},
    };
    for (const scope of STATE_SCOPES) {
      const seeded = seed[scope];
      if (seeded) this.#scopes[scope] = copyRecord(seeded);
    }
  }

  read(scope: StateScope): StateSnapshot {
    const values = this.#scopes[scope];
    return values ? copyRecord(values) : {};
  }

  apply(updates: readonly StateUpdate[]): void {
    for (const update of updates) {
      if (!isKnownScope(update.scope)) continue;
      applyStateUpdate(this.#scopes[update.scope], update);
    }
  }

  replace(scope: StateScope, values: Readonly<Record<string, unknown>>): void {
    if (!isKnownScope(scope)) return;
    this.#scopes[scope] = copyRecord(values);
  }

  resetTurn(): void {
    this.#scopes.turn = {};
  }
}
