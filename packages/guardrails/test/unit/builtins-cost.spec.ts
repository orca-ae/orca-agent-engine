// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { BUILTIN_EVALUATORS } from '../../src/builtins/index.js';
import type { EvaluatorContext } from '../../src/engine.js';
import { applyStateUpdate } from '../../src/state.js';
import type { GuardrailEvent } from '../../src/types.js';

function run(
  name: string,
  params: Record<string, unknown>,
  event: Partial<GuardrailEvent> = {},
  state: Record<string, unknown> = {},
) {
  const evaluator = BUILTIN_EVALUATORS.get(name);
  if (!evaluator) throw new Error(`no evaluator for ${name}`);
  const ctx: EvaluatorContext = {
    params,
    state,
    event: { phase: 'tool_call', sessionId: 'ses_1', ...event },
    guardrail: {
      id: 'grd_1',
      name: 'test',
      enabled: true,
      phases: ['tool_call'],
      scope: 'explicit',
      rule: { kind: 'builtin', builtin: name, params },
    },
  };
  return evaluator(ctx);
}

describe('cost_budget', () => {
  it('abstains below the cap with no thresholds crossed', () => {
    expect(
      run('cost_budget', { max_cost_usd: 25 }, { usage: { totalCostUsd: 1 } }),
    ).toBeUndefined();
  });

  it('denies at the cap when every model is blocked', () => {
    // No expensive_models list means the cap is a hard stop.
    const out = run(
      'cost_budget',
      { max_cost_usd: 25 },
      { usage: { totalCostUsd: 25 }, modelId: 'cheap-model' },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('allows over the cap once the session is on a permitted model', () => {
    // The downgrade the gate exists to encourage has happened.
    const out = run(
      'cost_budget',
      { max_cost_usd: 25, expensive_models: ['opus'] },
      { usage: { totalCostUsd: 30 }, modelId: 'claude-sonnet-5' },
    );
    expect(out?.verdict).toBe('allow');
  });

  it('denies over the cap while still on an expensive model', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 25, expensive_models: ['opus'] },
      { usage: { totalCostUsd: 30 }, modelId: 'claude-opus-5' },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('normalizes expensive-model tokens before matching', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 25, expensive_models: [' opus '] },
      { usage: { totalCostUsd: 30 }, modelId: 'claude-opus-5' },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('treats an unknown model as blocked, so a budget cannot be evaded by hiding it', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 25, expensive_models: ['opus'] },
      { usage: { totalCostUsd: 30 } },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('asks the first time a threshold is crossed', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 25, ask_thresholds_usd: [10] },
      { usage: { totalCostUsd: 12 } },
    );
    expect(out?.verdict).toBe('ask');
    expect(out?.stateUpdates?.[0]).toMatchObject({ key: 'budget_ask_approved', value: 10 });
  });

  it('does not re-ask a threshold already approved', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 25, ask_thresholds_usd: [10] },
      { usage: { totalCostUsd: 12 } },
      { budget_ask_approved: 10 },
    );
    expect(out).toBeUndefined();
  });

  it('still asks a higher threshold after a lower one was approved', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 25, ask_thresholds_usd: [10, 20] },
      { usage: { totalCostUsd: 21 } },
      { budget_ask_approved: 10 },
    );
    expect(out?.verdict).toBe('ask');
  });

  it('skips soft thresholds entirely once over the cap', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 25, ask_thresholds_usd: [10], expensive_models: ['opus'] },
      { usage: { totalCostUsd: 30 }, modelId: 'sonnet' },
    );
    expect(out?.verdict).toBe('allow');
  });

  it('holds a breached cap against a later malformed zero cost', () => {
    // The runtime accumulates spend independent of any verdict. After a $30 event
    // past a $25 cap, a following event that reports $0 must not buy a free call:
    // the accumulated counter, not the single bad event, sets the floor.
    const out = run(
      'cost_budget',
      { max_cost_usd: 25 },
      { usage: { totalCostUsd: 0 } },
      { session_cost_usd: 30 },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('does not fabricate spend when the counter is empty', () => {
    // A first-ever event reporting $0 with no accumulated spend is genuinely free.
    expect(
      run('cost_budget', { max_cost_usd: 25 }, { usage: { totalCostUsd: 0, inputTokens: 10 } }),
    ).toBeUndefined();
  });
});

describe('an unmeasurable session is not a free one', () => {
  it('asks when tokens were consumed but no cost was computed', () => {
    // Absence of a cost is not zero cost. Passing here would silently disable
    // every budget that depends on measuring spend.
    const out = run('cost_budget', { max_cost_usd: 25 }, { usage: { inputTokens: 5000 } });
    expect(out?.verdict).toBe('ask');
    expect(out?.reason).toMatch(/price data/i);
  });

  it('is checked before the cap, not after', () => {
    const out = run('cost_budget', { max_cost_usd: 1 }, { usage: { inputTokens: 5000 } });
    expect(out?.verdict).toBe('ask');
  });

  it('stops asking once the client has accepted running unmeasured', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 25 },
      { usage: { inputTokens: 5000 } },
      { budget_unpriced_approved: true },
    );
    expect(out).toBeUndefined();
  });

  it('does not fire for a session that has consumed nothing yet', () => {
    expect(run('cost_budget', { max_cost_usd: 25 }, { usage: {} })).toBeUndefined();
  });

  it('does not fire when a cost of exactly zero was genuinely computed', () => {
    // A priced $0.00 and an unpriced session must stay distinguishable.
    const out = run(
      'cost_budget',
      { max_cost_usd: 25 },
      { usage: { inputTokens: 10, totalCostUsd: 0 } },
    );
    expect(out).toBeUndefined();
  });

  it('keeps a daily budget unpriced after a later session reports priced usage', () => {
    const out = run(
      'user_daily_cost_budget',
      { max_cost_usd: 25 },
      { usage: { inputTokens: 10, totalCostUsd: 1 } },
      { daily_cost_usd: 1, daily_cost_unpriced: true },
    );
    expect(out?.verdict).toBe('ask');
    expect(out?.stateUpdates?.[0]).toMatchObject({
      scope: 'subject_window',
      key: 'budget_unpriced_approved',
    });
  });

  it('honours the daily budget policy for an earlier unpriced session', () => {
    const state = { daily_cost_usd: 1, daily_cost_unpriced: true };
    const event = { usage: { inputTokens: 10, totalCostUsd: 1 } };
    expect(
      run('user_daily_cost_budget', { max_cost_usd: 25, on_unpriced: 'deny' }, event, state)
        ?.verdict,
    ).toBe('deny');
    expect(
      run('user_daily_cost_budget', { max_cost_usd: 25, on_unpriced: 'allow' }, event, state),
    ).toBeUndefined();
  });

  it('reads the numeric daily unpriced flag the counter table actually stores', () => {
    // The cases above seed `true`, which `guardrail_counters` cannot hold:
    // `value_num` is `double precision NOT NULL` and the internal route
    // refuses a non-numeric `set` at `subject_window`. The route writes 1, and
    // a reader comparing `=== true` saw nothing — so a day whose earlier
    // session ran unpriced looked identical to one that cost nothing.
    const state = { daily_cost_usd: 1, daily_cost_unpriced: 1 };
    const event = { usage: { inputTokens: 10, totalCostUsd: 1 } };
    expect(
      run('user_daily_cost_budget', { max_cost_usd: 25, on_unpriced: 'deny' }, event, state)
        ?.verdict,
    ).toBe('deny');
    // 0 is the flag's absent form and must not read as set.
    expect(
      run(
        'user_daily_cost_budget',
        { max_cost_usd: 25, on_unpriced: 'deny' },
        event,
        { daily_cost_usd: 1, daily_cost_unpriced: 0 },
      ),
    ).toBeUndefined();
  });
});

describe('budgets on a phase that cannot ask', () => {
  // At `request` there is no approval round trip, so an `ask` would degrade to a
  // deny the client can never clear — and, because a deny withholds the rule's
  // own approval write, it would repeat on every message. A soft threshold must
  // wait for the tool call where it can actually be answered; only the hard cap
  // fires at `request`.
  it('does not surface a soft threshold at the request phase', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 100, ask_thresholds_usd: [10] },
      { phase: 'request', usage: { totalCostUsd: 12 } },
    );
    expect(out).toBeUndefined();
  });

  it('allows one unpriced request to reach an ask, then denies if it remains unapproved', () => {
    const first = run(
      'cost_budget',
      { max_cost_usd: 100 },
      { phase: 'request', usage: { inputTokens: 5000 } },
    );
    expect(first?.verdict).toBe('allow');
    expect(first?.stateUpdates).toEqual([
      {
        scope: 'session',
        key: 'budget_unpriced_pending',
        action: 'set',
        value: true,
      },
    ]);

    const second = run(
      'cost_budget',
      { max_cost_usd: 100 },
      { phase: 'request', usage: { inputTokens: 5000 } },
      { budget_unpriced_pending: true },
    );
    expect(second?.verdict).toBe('deny');
  });

  it('honours explicit allow and deny behavior for unpriced usage', () => {
    const event = { phase: 'request' as const, usage: { inputTokens: 5000 } };
    expect(run('cost_budget', { max_cost_usd: 100, on_unpriced: 'allow' }, event)).toBeUndefined();
    expect(run('cost_budget', { max_cost_usd: 100, on_unpriced: 'deny' }, event)?.verdict).toBe(
      'deny',
    );
  });

  it('asks an unpriced session at the tool_call phase', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 100 },
      { phase: 'tool_call', usage: { inputTokens: 5000 } },
    );
    expect(out?.verdict).toBe('ask');
  });

  it('detects an unpriced cache-only snapshot, and a NaN cost', () => {
    // computeCost prices the cache buckets, so a cache-only snapshot is billable
    // and must not read as "consumed nothing"; a NaN cost is unmeasurable.
    expect(
      run('cost_budget', { max_cost_usd: 1 }, { usage: { cacheReadInputTokens: 1e9 } })?.verdict,
    ).toBe('ask');
    expect(
      run('cost_budget', { max_cost_usd: 1 }, { usage: { inputTokens: NaN, totalCostUsd: NaN } })
        ?.verdict,
    ).toBe('ask');
  });

  it('still enforces the hard cap at the request phase', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 100 },
      { phase: 'request', usage: { totalCostUsd: 100 }, modelId: 'x' },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('still asks the same soft threshold at the tool_call phase', () => {
    const out = run(
      'cost_budget',
      { max_cost_usd: 100, ask_thresholds_usd: [10] },
      { phase: 'tool_call', usage: { totalCostUsd: 12 } },
    );
    expect(out?.verdict).toBe('ask');
  });

  it('holds token-budget soft thresholds until a phase that can ask', () => {
    expect(
      run(
        'token_budget',
        { max_total_tokens: 1000, ask_thresholds: [500] },
        { phase: 'request', usage: { totalTokens: 600 } },
      ),
    ).toBeUndefined();
    expect(
      run(
        'token_budget',
        { max_total_tokens: 1000, ask_thresholds: [500] },
        { phase: 'tool_call', usage: { totalTokens: 600 } },
      )?.verdict,
    ).toBe('ask');
  });
});

describe('token_budget', () => {
  it('denies at the cap', () => {
    expect(
      run('token_budget', { max_total_tokens: 1000 }, { usage: { totalTokens: 1000 } })?.verdict,
    ).toBe('deny');
  });

  it('abstains below it', () => {
    expect(
      run('token_budget', { max_total_tokens: 1000 }, { usage: { totalTokens: 10 } }),
    ).toBeUndefined();
  });

  it('works with no price data at all, which is why it exists', () => {
    const out = run('token_budget', { max_total_tokens: 100 }, { usage: { totalTokens: 200 } });
    expect(out?.verdict).toBe('deny');
  });
});

describe('subagent_cost_budget', () => {
  it('abstains when the coordinator is acting', () => {
    expect(run('subagent_cost_budget', { max_cost_usd: 5 }, {})).toBeUndefined();
  });

  it('applies per dispatched subagent', () => {
    const out = run(
      'subagent_cost_budget',
      { max_cost_usd: 5 },
      { subagentId: 'sub_1' },
      { subagent_cost_sub_1: 6 },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('keeps two concurrent dispatches independent', () => {
    const out = run(
      'subagent_cost_budget',
      { max_cost_usd: 5 },
      { subagentId: 'sub_2' },
      { subagent_cost_sub_1: 6 },
    );
    expect(out).toBeUndefined();
  });

  it('keys ask approvals by dispatch, so approving one subagent does not silence another', () => {
    const params = { ask_thresholds_usd: [5] };
    const asked = run(
      'subagent_cost_budget',
      params,
      { subagentId: 'sub_1' },
      {
        subagent_cost_sub_1: 6,
      },
    );
    expect(asked?.verdict).toBe('ask');

    // The client approves sub_1: persist exactly what the rule asked to write.
    const state: Record<string, unknown> = { subagent_cost_sub_1: 6, subagent_cost_sub_2: 6 };
    for (const u of asked?.stateUpdates ?? []) applyStateUpdate(state, u);

    // sub_1 is satisfied…
    expect(run('subagent_cost_budget', params, { subagentId: 'sub_1' }, state)).toBeUndefined();
    // …but sub_2 crossing its own threshold still asks.
    expect(run('subagent_cost_budget', params, { subagentId: 'sub_2' }, state)?.verdict).toBe(
      'ask',
    );
  });

  it('keys the unpriced-session approval by dispatch as well', () => {
    const params = { max_cost_usd: 5 };
    const unpriced = { usage: { totalTokens: 10 } };
    const asked = run('subagent_cost_budget', params, { subagentId: 'sub_1', ...unpriced }, {});
    expect(asked?.verdict).toBe('ask');

    const state: Record<string, unknown> = {};
    for (const u of asked?.stateUpdates ?? []) applyStateUpdate(state, u);

    expect(
      run('subagent_cost_budget', params, { subagentId: 'sub_2', ...unpriced }, state)?.verdict,
    ).toBe('ask');
  });
});

describe('detect_loop', () => {
  const call = { tool: { name: 'Bash', input: { command: 'ls' } } };

  it('records a call and allows below the threshold', () => {
    const out = run('detect_loop', { threshold: 3 }, call, {});
    expect(out?.verdict).toBe('allow');
    expect(out?.stateUpdates?.[0]?.key).toBe('recent_tool_calls');
  });

  it('reports once identical calls reach the threshold', () => {
    const sig = 'Bash:{"command":"ls"}';
    const out = run('detect_loop', { threshold: 3 }, call, { recent_tool_calls: [sig, sig] });
    expect(out?.verdict).toBe('ask');
  });

  it('honours a deny action', () => {
    const sig = 'Bash:{"command":"ls"}';
    const out = run('detect_loop', { threshold: 2, action: 'deny' }, call, {
      recent_tool_calls: [sig],
    });
    expect(out?.verdict).toBe('deny');
  });

  it('does not confuse different arguments for a repeat', () => {
    const out = run('detect_loop', { threshold: 2 }, call, {
      recent_tool_calls: ['Bash:{"command":"pwd"}'],
    });
    expect(out?.verdict).toBe('allow');
  });

  it('treats recursively reordered object keys as the same call', () => {
    const first = run(
      'detect_loop',
      { threshold: 2 },
      { tool: { name: 'Repo', input: { owner: 'a', nested: { repo: 'b', ref: 'main' } } } },
      {},
    );
    const signature = first?.stateUpdates?.[0]?.value as string[];
    const reordered = run(
      'detect_loop',
      { threshold: 2 },
      { tool: { name: 'Repo', input: { nested: { ref: 'main', repo: 'b' }, owner: 'a' } } },
      { recent_tool_calls: signature },
    );
    expect(reordered?.verdict).toBe('ask');
  });

  it('preserves array order in call signatures', () => {
    const first = run(
      'detect_loop',
      { threshold: 2 },
      { tool: { name: 'Batch', input: { ids: ['a', 'b'] } } },
      {},
    );
    const signature = first?.stateUpdates?.[0]?.value as string[];
    const reversed = run(
      'detect_loop',
      { threshold: 2 },
      { tool: { name: 'Batch', input: { ids: ['b', 'a'] } } },
      { recent_tool_calls: signature },
    );
    expect(reversed?.verdict).toBe('allow');
  });

  it('counts repeats only inside the configured rolling window', () => {
    const sig = 'Bash:{"command":"ls"}';
    const out = run('detect_loop', { threshold: 2, window: 2 }, call, {
      recent_tool_calls: [sig, 'Bash:{"command":"pwd"}'],
    });
    expect(out?.verdict).toBe('allow');
  });
});

describe('detect_thrashing', () => {
  it('reports a run of consecutive failures', () => {
    const out = run(
      'detect_thrashing',
      { consecutive_threshold: 3 },
      { phase: 'tool_result', result: 'Error: command not found' },
      { recent_results: ['fail', 'fail'] },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('measures the failure streak only inside the configured window', () => {
    const out = run(
      'detect_thrashing',
      { consecutive_threshold: 3, window: 2 },
      { phase: 'tool_result', result: 'Error: command not found' },
      { recent_results: ['fail', 'fail'] },
    );
    expect(out?.verdict).toBe('allow');
    expect(out?.stateUpdates?.[0]?.value).toEqual(['fail', 'fail']);
  });

  it('only denies, because the tool has already run', () => {
    const out = run(
      'detect_thrashing',
      { consecutive_threshold: 1 },
      { phase: 'tool_result', result: { error: 'nope' } },
      {},
    );
    expect(out?.verdict).toBe('deny');
  });

  it('a success breaks the streak', () => {
    const out = run(
      'detect_thrashing',
      { consecutive_threshold: 2 },
      { phase: 'tool_result', result: 'done' },
      { recent_results: ['fail'] },
    );
    expect(out?.verdict).toBe('allow');
  });

  it('reads a failure nested inside a structured result', () => {
    const out = run(
      'detect_thrashing',
      { consecutive_threshold: 1 },
      { phase: 'tool_result', result: { content: 'Traceback (most recent call last)' } },
      {},
    );
    expect(out?.verdict).toBe('deny');
  });

  it('does not read an explicit `error: null` envelope as a failure', () => {
    // A common "no error" shape. Treating it as a failure would deny successful
    // output after two prior failures.
    const out = run(
      'detect_thrashing',
      { consecutive_threshold: 3 },
      { phase: 'tool_result', result: { error: null, data: 'ok' } },
      { recent_results: ['fail', 'fail'] },
    );
    expect(out?.verdict).toBe('allow');
  });

  it('reads a failure nested inside a content array', () => {
    // The dominant Anthropic tool-result shape: content is a list of blocks.
    const out = run(
      'detect_thrashing',
      { consecutive_threshold: 1 },
      { phase: 'tool_result', result: { content: [{ type: 'text', text: 'Error: not found' }] } },
      {},
    );
    expect(out?.verdict).toBe('deny');
  });

  it('does not overflow on a circular result', () => {
    const cyclic: Record<string, unknown> = {};
    cyclic['self'] = cyclic;
    const out = run(
      'detect_thrashing',
      { consecutive_threshold: 1 },
      { phase: 'tool_result', result: cyclic },
      {},
    );
    // No throw; a cycle with no failure marker reads as success.
    expect(out?.verdict).toBe('allow');
  });
});

describe('malformed and zero usage', () => {
  it('preserves Registry unpriced markers when the next event has no usage payload', () => {
    expect(
      run(
        'cost_budget',
        { max_cost_usd: 1 },
        {},
        { total_tokens: 10, session_usage_has_unpriced: true },
      )?.verdict,
    ).toBe('ask');
    expect(
      run(
        'subagent_cost_budget',
        { max_cost_usd: 1 },
        { subagentId: 'agt_child' },
        { subagent_usage_has_unpriced_agt_child: true },
      )?.verdict,
    ).toBe('ask');
  });

  it('treats a NaN or negative computed cost as unpriced, not a measured amount', () => {
    expect(
      run('cost_budget', { max_cost_usd: 1 }, { usage: { totalTokens: 2e9, totalCostUsd: NaN } })
        ?.verdict,
    ).toBe('ask');
    expect(
      run('cost_budget', { max_cost_usd: 1 }, { usage: { totalTokens: 2e9, totalCostUsd: -5 } })
        ?.verdict,
    ).toBe('ask');
  });

  it('does not let a reported zero token count shadow the accumulated counter', () => {
    const out = run(
      'token_budget',
      { max_total_tokens: 1000 },
      { usage: { totalTokens: 0 } },
      { total_tokens: 999999 },
    );
    expect(out?.verdict).toBe('deny');
  });
});
