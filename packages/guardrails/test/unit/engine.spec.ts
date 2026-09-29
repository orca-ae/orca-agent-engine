// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { BUILTIN_EVALUATORS } from '../../src/builtins/index.js';
import {
  evaluateGuardrails,
  type BuiltinEvaluator,
  type PreparedGuardrail,
} from '../../src/engine.js';
import { InMemoryGuardrailStateStore } from '../../src/state.js';
import type { GuardrailEvent, Tier, Verdict } from '../../src/types.js';

const EVENT: GuardrailEvent = {
  phase: 'tool_call',
  sessionId: 'ses_1',
  tool: { name: 'Bash', input: { command: 'ls' } },
};

function prepared(
  id: string,
  tier: Tier,
  opts: {
    stateful?: boolean;
    phases?: GuardrailEvent['phase'][];
    enabled?: boolean;
    builtin?: string;
    params?: Record<string, unknown>;
    subagentId?: string;
  } = {},
): PreparedGuardrail {
  return {
    tier,
    stateful: opts.stateful ?? false,
    ...(opts.subagentId !== undefined ? { subagentId: opts.subagentId } : {}),
    guardrail: {
      id,
      name: id,
      enabled: opts.enabled ?? true,
      phases: opts.phases ?? ['tool_call'],
      scope: 'explicit',
      rule: { kind: 'builtin', builtin: opts.builtin ?? id, params: opts.params ?? {} },
    },
  };
}

/** A builtin that always returns the given verdict, optionally writing state. */
function fixed(verdict: Verdict, reason: string, writes = false): BuiltinEvaluator {
  return () => ({
    verdict,
    reason,
    ...(writes
      ? { stateUpdates: [{ scope: 'session' as const, key: reason, action: 'increment' as const }] }
      : {}),
  });
}

const abstain: BuiltinEvaluator = () => undefined;

function run(
  guardrails: readonly PreparedGuardrail[],
  builtins: Record<string, BuiltinEvaluator>,
  extra: Partial<Parameters<typeof evaluateGuardrails>[2]> = {},
) {
  return evaluateGuardrails(guardrails, EVENT, {
    builtins: new Map(Object.entries(builtins)),
    ...extra,
  });
}

describe('verdict composition', () => {
  it('allows when nothing has an opinion', () => {
    const d = run([prepared('a', 'agent')], { a: abstain });
    expect(d.verdict).toBe('allow');
    expect(d.reasons).toEqual([]);
  });

  it('takes the strictest verdict across guardrails', () => {
    const d = run([prepared('a', 'agent'), prepared('b', 'workspace')], {
      a: fixed('allow', 'a ok'),
      b: fixed('ask', 'b asks'),
    });
    expect(d.verdict).toBe('ask');
  });

  it('starts from the seed verdict, so a permission policy can only be tightened', () => {
    const d = run([prepared('a', 'agent')], { a: fixed('allow', 'a ok') }, { seed: 'ask' });
    expect(d.verdict).toBe('ask');
  });

  it('lets a guardrail tighten the seed', () => {
    const d = run([prepared('a', 'agent')], { a: fixed('deny', 'blocked') }, { seed: 'ask' });
    expect(d.verdict).toBe('deny');
  });

  it('never lets a later allow loosen an earlier ask', () => {
    const d = run([prepared('a', 'session'), prepared('b', 'organization')], {
      a: fixed('ask', 'a asks'),
      b: fixed('allow', 'b fine'),
    });
    expect(d.verdict).toBe('ask');
  });

  it('never lets a lower tier loosen a higher one', () => {
    // session runs first and allows; organization denies. Order must not matter.
    const d = run([prepared('a', 'session'), prepared('b', 'organization')], {
      a: fixed('allow', 'session ok'),
      b: fixed('deny', 'org blocks'),
    });
    expect(d.verdict).toBe('deny');
  });
});

describe('tier ordering', () => {
  it('evaluates session, then agent, then workspace, then organization', () => {
    const seen: string[] = [];
    const record =
      (id: string): BuiltinEvaluator =>
      () => {
        seen.push(id);
        return { verdict: 'allow', reason: id };
      };
    run(
      [
        prepared('org', 'organization'),
        prepared('sess', 'session'),
        prepared('ws', 'workspace'),
        prepared('agent', 'agent'),
      ],
      { org: record('org'), sess: record('sess'), ws: record('ws'), agent: record('agent') },
    );
    expect(seen).toEqual(['sess', 'agent', 'ws', 'org']);
  });
});

describe('short-circuit', () => {
  it('stops at the first deny', () => {
    const later = vi.fn(fixed('allow', 'later'));
    const d = run([prepared('a', 'session'), prepared('b', 'organization')], {
      a: fixed('deny', 'blocked'),
      b: later,
    });
    expect(d.verdict).toBe('deny');
    expect(later).not.toHaveBeenCalled();
  });

  it('attributes the deny to the guardrail that produced it', () => {
    const d = run([prepared('a', 'session')], { a: fixed('deny', 'blocked') });
    expect(d.deniedBy).toBe('a');
  });

  it('does not advance counters of guardrails that never ran', () => {
    const d = run([prepared('a', 'session'), prepared('b', 'organization', { stateful: true })], {
      a: fixed('deny', 'blocked'),
      b: fixed('allow', 'counted', true),
    });
    expect(d.intendedStateUpdates.some((u) => u.key === 'counted')).toBe(false);
  });
});

describe('write timing', () => {
  it('persists writes from guardrails that allowed', () => {
    const d = run(
      [prepared('a', 'session', { stateful: true })],
      { a: fixed('allow', 'counted', true) },
      {
        store: new InMemoryGuardrailStateStore(),
      },
    );
    expect(d.stateUpdates).toHaveLength(1);
  });

  it('withholds every write when the verdict is ask, so a refusal cannot arm the guardrail', () => {
    const d = run(
      [
        prepared('a', 'session', { stateful: true }),
        prepared('b', 'organization', { stateful: true }),
      ],
      { a: fixed('allow', 'counted', true), b: fixed('ask', 'needs approval', true) },
      { store: new InMemoryGuardrailStateStore() },
    );
    expect(d.verdict).toBe('ask');
    expect(d.stateUpdates).toEqual([]);
    // …but the caller can still see what would have been written.
    expect(d.intendedStateUpdates.length).toBeGreaterThan(0);
  });

  it('withholds every write when the action is denied, even an earlier allow', () => {
    // The denied action does not happen, so a counter an allowing guardrail
    // advanced for it must not persist — otherwise a tool call the budget blocked
    // would still consume the tool-call cap.
    const d = run(
      [
        prepared('a', 'session', { stateful: true }),
        prepared('b', 'organization', { stateful: true }),
      ],
      { a: fixed('allow', 'counted', true), b: fixed('deny', 'blocked', true) },
      { store: new InMemoryGuardrailStateStore() },
    );
    expect(d.verdict).toBe('deny');
    expect(d.stateUpdates).toEqual([]);
    // Intent still records what the allow would have written, namespaced.
    expect(d.intendedStateUpdates.map((u) => u.key)).toContain('g:a:counted');
  });

  it("withholds an ask's writes even when a later deny supersedes it", () => {
    // The ask resolved first and would have recorded an approval threshold; a
    // later deny then set the final verdict. Only an allow may persist, so the
    // ask's write must not survive to arm the threshold on the next call.
    const d = run(
      [
        prepared('a', 'session', { stateful: true }),
        prepared('b', 'organization', { stateful: true }),
      ],
      { a: fixed('ask', 'threshold', true), b: fixed('deny', 'blocked', true) },
      { store: new InMemoryGuardrailStateStore() },
    );
    expect(d.verdict).toBe('deny');
    expect(d.stateUpdates).toEqual([]);
    // Intent still records what the ask would have written.
    expect(d.intendedStateUpdates.some((u) => u.key.endsWith('threshold'))).toBe(true);
  });

  it('persists nothing in readOnly mode but still reports intent', () => {
    const d = run(
      [prepared('a', 'session', { stateful: true })],
      { a: fixed('allow', 'counted', true) },
      {
        readOnly: true,
        store: new InMemoryGuardrailStateStore(),
      },
    );
    expect(d.stateUpdates).toEqual([]);
    expect(d.intendedStateUpdates).toHaveLength(1);
  });
});

describe('two-pass evaluation', () => {
  it('runs the stateless pass with no store at all', () => {
    const d = run([prepared('a', 'agent')], { a: fixed('deny', 'blocked') });
    expect(d.verdict).toBe('deny');
  });

  it('skips stateful guardrails entirely when no store is supplied', () => {
    const stateful = vi.fn(fixed('deny', 'over budget'));
    const d = run([prepared('a', 'agent', { stateful: true })], { a: stateful });
    expect(stateful).not.toHaveBeenCalled();
    expect(d.verdict).toBe('allow');
  });

  it('evaluates stateless guardrails before stateful ones regardless of tier', () => {
    const seen: string[] = [];
    const record =
      (id: string): BuiltinEvaluator =>
      () => {
        seen.push(id);
        return { verdict: 'allow', reason: id };
      };
    run(
      [
        prepared('statefulSession', 'session', { stateful: true }),
        prepared('statelessOrg', 'organization'),
      ],
      { statefulSession: record('statefulSession'), statelessOrg: record('statelessOrg') },
      { store: new InMemoryGuardrailStateStore() },
    );
    expect(seen).toEqual(['statelessOrg', 'statefulSession']);
  });

  it('hands stateful guardrails the session state snapshot', () => {
    const store = new InMemoryGuardrailStateStore({ session: { calls: 7 } });
    let observed: unknown;
    const d = run(
      [prepared('a', 'agent', { stateful: true })],
      {
        a: (ctx) => {
          observed = ctx.state['calls'];
          return { verdict: 'allow', reason: 'ok' };
        },
      },
      { store },
    );
    expect(observed).toBe(7);
    expect(d.verdict).toBe('allow');
  });
});

describe('filtering', () => {
  it('ignores disabled guardrails', () => {
    const d = run([prepared('a', 'session', { enabled: false })], { a: fixed('deny', 'blocked') });
    expect(d.verdict).toBe('allow');
  });

  it('ignores guardrails that do not declare the event phase', () => {
    const d = run([prepared('a', 'session', { phases: ['tool_result'] })], {
      a: fixed('deny', 'blocked'),
    });
    expect(d.verdict).toBe('allow');
  });
});

describe('failure handling', () => {
  it('denies when a guardrail throws on a fail-closed phase', () => {
    const d = run([prepared('a', 'session')], {
      a: () => {
        throw new Error('boom');
      },
    });
    expect(d.verdict).toBe('deny');
    expect(d.reasons.join(' ')).toMatch(/could not be evaluated/i);
  });

  it('allows when a guardrail throws on a fail-open phase', () => {
    const event: GuardrailEvent = { phase: 'tool_result', sessionId: 'ses_1', result: 'x' };
    const d = evaluateGuardrails([prepared('a', 'session', { phases: ['tool_result'] })], event, {
      builtins: new Map<string, BuiltinEvaluator>([
        [
          'a',
          () => {
            throw new Error('boom');
          },
        ],
      ]),
    });
    expect(d.verdict).toBe('allow');
  });

  it('records a swallowed error so a silently-disarmed rule is visible', () => {
    const event: GuardrailEvent = { phase: 'tool_result', sessionId: 'ses_1', result: 'x' };
    const d = evaluateGuardrails([prepared('a', 'session', { phases: ['tool_result'] })], event, {
      builtins: new Map<string, BuiltinEvaluator>([
        [
          'a',
          () => {
            throw new Error('boom');
          },
        ],
      ]),
    });
    expect(d.verdict).toBe('allow');
    expect(d.errors).toEqual([{ guardrailId: 'a', failedClosed: false, message: 'boom' }]);
  });

  it('records the error even when the phase failed closed', () => {
    const d = run([prepared('a', 'session')], {
      a: () => {
        throw new Error('kaboom');
      },
    });
    expect(d.verdict).toBe('deny');
    expect(d.errors).toEqual([{ guardrailId: 'a', failedClosed: true, message: 'kaboom' }]);
  });

  it('denies on a fail-closed phase when a builtin is not registered at all', () => {
    const d = run([prepared('missing', 'session')], {});
    expect(d.verdict).toBe('deny');
  });
});

describe('ask downgrade on phases without an approval round trip', () => {
  it('escalates ask to deny where the phase cannot ask', () => {
    const event: GuardrailEvent = { phase: 'tool_result', sessionId: 'ses_1', result: 'x' };
    const d = evaluateGuardrails([prepared('a', 'session', { phases: ['tool_result'] })], event, {
      builtins: new Map<string, BuiltinEvaluator>([['a', fixed('ask', 'needs approval')]]),
    });
    // The tool already ran; there is nothing left to approve, so asking degrades
    // to the safe answer rather than silently allowing.
    expect(d.verdict).toBe('deny');
  });
});

describe('state scope selection', () => {
  it('reads the scope the guardrail type declares, not always session scope', () => {
    // A per-turn dispatch cap and a per-principal daily budget are both
    // stateful but read different stores. Reading the wrong one is silent:
    // the guardrail sees an empty snapshot and never fires.
    const store = new InMemoryGuardrailStateStore({
      turn: { dispatches: 4 },
      session: { dispatches: 999 },
    });
    let observed: unknown;
    evaluateGuardrails(
      [{ ...prepared('a', 'agent', { stateful: true }), stateScope: 'turn' as const }],
      EVENT,
      {
        builtins: new Map<string, BuiltinEvaluator>([
          [
            'a',
            (ctx) => {
              observed = ctx.state['dispatches'];
              return { verdict: 'allow', reason: 'ok' };
            },
          ],
        ]),
        store,
      },
    );
    expect(observed).toBe(4);
  });

  it('defaults to session scope when the type declares none', () => {
    const store = new InMemoryGuardrailStateStore({ session: { calls: 3 } });
    let observed: unknown;
    evaluateGuardrails([prepared('a', 'agent', { stateful: true })], EVENT, {
      builtins: new Map<string, BuiltinEvaluator>([
        [
          'a',
          (ctx) => {
            observed = ctx.state['calls'];
            return { verdict: 'allow', reason: 'ok' };
          },
        ],
      ]),
      store,
    });
    expect(observed).toBe(3);
  });
});

describe('subagent applicability', () => {
  it('does not run a rule resolved from a subagent for the coordinator', () => {
    const d = run([prepared('a', 'agent', { subagentId: 'sub_a' })], {
      a: fixed('deny', 'blocked'),
    });
    expect(d.verdict).toBe('allow');
  });

  it('does not run a rule resolved from subagent A for subagent B', () => {
    const d = evaluateGuardrails(
      [prepared('a', 'agent', { subagentId: 'sub_a' })],
      { ...EVENT, subagentId: 'sub_b' },
      { builtins: new Map<string, BuiltinEvaluator>([['a', fixed('deny', 'blocked')]]) },
    );
    expect(d.verdict).toBe('allow');
  });

  it('runs a rule resolved from a subagent for that subagent', () => {
    const d = evaluateGuardrails(
      [prepared('a', 'agent', { subagentId: 'sub_a' })],
      { ...EVENT, subagentId: 'sub_a' },
      { builtins: new Map<string, BuiltinEvaluator>([['a', fixed('deny', 'blocked')]]) },
    );
    expect(d.verdict).toBe('deny');
    expect(d.deniedBy).toBe('a');
  });

  it('still runs rules with no subagent binding for a subagent event', () => {
    const d = evaluateGuardrails(
      [prepared('a', 'agent')],
      { ...EVENT, subagentId: 'sub_b' },
      { builtins: new Map<string, BuiltinEvaluator>([['a', fixed('deny', 'blocked')]]) },
    );
    expect(d.verdict).toBe('deny');
  });
});

describe('deny seed', () => {
  it('is terminal: no rule evaluates and no state is read or written', () => {
    const store = new InMemoryGuardrailStateStore({ session: { tool_calls: 1 } });
    const read = vi.spyOn(store, 'read');
    const evaluator = vi.fn(fixed('allow', 'counted', true));
    const d = run(
      [prepared('a', 'session', { stateful: true })],
      { a: evaluator },
      {
        store,
        seed: 'deny',
      },
    );
    expect(d.verdict).toBe('deny');
    expect(evaluator).not.toHaveBeenCalled();
    expect(read).not.toHaveBeenCalled();
    expect(d.stateUpdates).toEqual([]);
    expect(d.intendedStateUpdates).toEqual([]);
  });
});

describe('state namespacing', () => {
  it("hides one rule's writes from another rule's reads", () => {
    const store = new InMemoryGuardrailStateStore();
    const first = run(
      [prepared('a', 'session', { stateful: true })],
      { a: fixed('allow', 'counted', true) },
      { store },
    );
    store.apply(first.stateUpdates);

    let observed: unknown = 'unset';
    run(
      [prepared('b', 'session', { stateful: true })],
      {
        b: (ctx) => {
          observed = ctx.state['counted'];
          return { verdict: 'allow' };
        },
      },
      { store },
    );
    expect(observed).toBeUndefined();
  });

  it('lets a rule read back its own write under the bare key it wrote', () => {
    const store = new InMemoryGuardrailStateStore();
    const first = run(
      [prepared('a', 'session', { stateful: true })],
      { a: fixed('allow', 'counted', true) },
      { store },
    );
    store.apply(first.stateUpdates);

    let observed: unknown;
    run(
      [prepared('a', 'session', { stateful: true })],
      {
        a: (ctx) => {
          observed = ctx.state['counted'];
          return { verdict: 'allow' };
        },
      },
      { store },
    );
    expect(observed).toBe(1);
  });

  it('separates state for the same rule resolved from different subagents', () => {
    const store = new InMemoryGuardrailStateStore();
    const forA = evaluateGuardrails(
      [prepared('cap', 'agent', { stateful: true, subagentId: 'sub_a' })],
      { ...EVENT, subagentId: 'sub_a' },
      {
        builtins: new Map<string, BuiltinEvaluator>([['cap', fixed('allow', 'counted', true)]]),
        store,
      },
    );
    store.apply(forA.stateUpdates);

    let observed: unknown = 'unset';
    evaluateGuardrails(
      [prepared('cap', 'agent', { stateful: true, subagentId: 'sub_b' })],
      { ...EVENT, subagentId: 'sub_b' },
      {
        builtins: new Map<string, BuiltinEvaluator>([
          [
            'cap',
            (ctx) => {
              observed = ctx.state['counted'];
              return { verdict: 'allow' };
            },
          ],
        ]),
        store,
      },
    );
    expect(observed).toBeUndefined();
  });

  it('exposes runtime-written usage keys to every rule', () => {
    const store = new InMemoryGuardrailStateStore({ session: { total_tokens: 900 } });
    let observed: unknown;
    run(
      [prepared('a', 'session', { stateful: true })],
      {
        a: (ctx) => {
          observed = ctx.state['total_tokens'];
          return { verdict: 'allow' };
        },
      },
      { store },
    );
    expect(observed).toBe(900);
  });

  it("lets a rule's own state shadow a shared key of the same name", () => {
    const store = new InMemoryGuardrailStateStore({ session: { counted: 999 } });
    const first = run(
      [prepared('a', 'session', { stateful: true })],
      { a: fixed('allow', 'counted', true) },
      { store },
    );
    store.apply(first.stateUpdates);

    let observed: unknown;
    run(
      [prepared('a', 'session', { stateful: true })],
      {
        a: (ctx) => {
          observed = ctx.state['counted'];
          return { verdict: 'allow' };
        },
      },
      { store },
    );
    expect(observed).toBe(1);
  });
});

describe('independent rules do not interfere', () => {
  const realBuiltins = new Map<string, BuiltinEvaluator>([
    ['token_budget', BUILTIN_EVALUATORS.get('token_budget')!],
    ['cost_budget', BUILTIN_EVALUATORS.get('cost_budget')!],
    ['max_tool_calls_per_session', BUILTIN_EVALUATORS.get('max_tool_calls_per_session')!],
  ]);

  it('a token-budget approval does not suppress a cost-budget prompt', () => {
    const store = new InMemoryGuardrailStateStore();
    const tokens = prepared('tokens', 'session', {
      stateful: true,
      builtin: 'token_budget',
      params: { ask_thresholds: [500] },
    });
    const dollars = prepared('dollars', 'session', {
      stateful: true,
      builtin: 'cost_budget',
      params: { ask_thresholds_usd: [25] },
    });
    const event: GuardrailEvent = { ...EVENT, usage: { totalTokens: 600, totalCostUsd: 26 } };

    // Round 1: the token budget asks and the client approves, which persists
    // the approval the rule intended to write.
    const first = evaluateGuardrails([tokens], event, { builtins: realBuiltins, store });
    expect(first.verdict).toBe('ask');
    store.apply(first.intendedStateUpdates);

    // Round 2: the cost budget crosses its own $25 threshold. If the two rules
    // shared state, the token approval of 500 would satisfy 25 <= 500 and this
    // prompt would silently never happen.
    const second = evaluateGuardrails([tokens, dollars], event, {
      builtins: realBuiltins,
      store,
    });
    expect(second.verdict).toBe('ask');
    expect(second.reasons.join(' ')).toContain('$25.00');
  });

  it('two max-tool-call rules each count one call per action', () => {
    const store = new InMemoryGuardrailStateStore();
    const caps = [
      prepared('cap1', 'session', {
        stateful: true,
        builtin: 'max_tool_calls_per_session',
        params: { limit: 2 },
      }),
      prepared('cap2', 'session', {
        stateful: true,
        builtin: 'max_tool_calls_per_session',
        params: { limit: 2 },
      }),
    ];

    const first = evaluateGuardrails(caps, EVENT, { builtins: realBuiltins, store });
    expect(first.verdict).toBe('allow');
    store.apply(first.stateUpdates);

    // With a shared counter one action counts twice, so a limit of 2 would
    // already refuse the second call. Independent counters allow it.
    const second = evaluateGuardrails(caps, EVENT, { builtins: realBuiltins, store });
    expect(second.verdict).toBe('allow');
    store.apply(second.stateUpdates);

    const third = evaluateGuardrails(caps, EVENT, { builtins: realBuiltins, store });
    expect(third.verdict).toBe('deny');
  });
});

describe('seed degradation', () => {
  it('degrades a seed of ask on a phase that cannot ask', () => {
    const event: GuardrailEvent = { phase: 'tool_result', sessionId: 'ses_1', result: 'x' };
    const d = evaluateGuardrails([], event, {
      builtins: new Map<string, BuiltinEvaluator>(),
      seed: 'ask',
    });
    expect(d.verdict).toBe('deny');
  });
});

describe('state at observational phases', () => {
  it('keeps an allowing guardrail observation when a co-firing deny suppresses output', () => {
    // At tool_result the tool has already run, so a deny only suppresses output;
    // an allowing guardrail's record of what happened (a confidential read the
    // DLP check needs next turn) must still persist.
    const event: GuardrailEvent = { phase: 'tool_result', sessionId: 'ses_1', result: 'x' };
    const d = evaluateGuardrails(
      [
        prepared('a', 'session', { stateful: true, phases: ['tool_result'] }),
        prepared('b', 'organization', { stateful: true, phases: ['tool_result'] }),
      ],
      event,
      {
        builtins: new Map<string, BuiltinEvaluator>([
          ['a', fixed('allow', 'observed', true)],
          ['b', fixed('deny', 'blocked', true)],
        ]),
        store: new InMemoryGuardrailStateStore(),
      },
    );
    expect(d.verdict).toBe('deny');
    expect(d.stateUpdates.map((u) => u.key)).toContain('g:a:observed');
  });
});
