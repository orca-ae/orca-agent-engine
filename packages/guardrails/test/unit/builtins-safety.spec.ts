// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { BUILTIN_EVALUATORS } from '../../src/builtins/index.js';
import type { EvaluatorContext } from '../../src/engine.js';
import type { GuardrailEvent } from '../../src/types.js';

function ctx(
  params: Record<string, unknown>,
  event: Partial<GuardrailEvent> = {},
  state: Record<string, unknown> = {},
): EvaluatorContext {
  return {
    params,
    state,
    event: {
      phase: 'tool_call',
      sessionId: 'ses_1',
      tool: { name: 'Bash', input: { command: 'ls' } },
      ...event,
    },
    guardrail: {
      id: 'grd_1',
      name: 'test',
      enabled: true,
      phases: ['tool_call'],
      scope: 'explicit',
      rule: { kind: 'builtin', builtin: 'x', params },
    },
  };
}

function run(
  name: string,
  params: Record<string, unknown>,
  event?: Partial<GuardrailEvent>,
  state?: Record<string, unknown>,
) {
  const evaluator = BUILTIN_EVALUATORS.get(name);
  if (!evaluator) throw new Error(`no evaluator registered for ${name}`);
  return evaluator(ctx(params, event, state));
}

describe('block_tools', () => {
  it('denies a named tool', () => {
    expect(run('block_tools', { tools: ['Bash'] })?.verdict).toBe('deny');
  });

  it('abstains on a tool it does not name', () => {
    expect(run('block_tools', { tools: ['Write'] })).toBeUndefined();
  });

  it('matches a wildcard pattern', () => {
    const out = run(
      'block_tools',
      { tools: ['mcp__github__*'] },
      {
        tool: { name: 'mcp__github__create_issue', input: {} },
      },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('carries the configured reason', () => {
    const out = run('block_tools', { tools: ['Bash'], reason: 'no shell here' });
    expect(out?.reason).toContain('no shell here');
  });
});

describe('require_approval_for_tools', () => {
  it('escalates a named tool to ask', () => {
    expect(run('require_approval_for_tools', { tools: ['Bash'] })?.verdict).toBe('ask');
  });

  it('abstains on other tools', () => {
    expect(run('require_approval_for_tools', { tools: ['Write'] })).toBeUndefined();
  });
});

describe('ask_on_os_tools', () => {
  it('asks before a shell tool', () => {
    expect(run('ask_on_os_tools', {})?.verdict).toBe('ask');
  });

  it('asks before a filesystem write', () => {
    expect(run('ask_on_os_tools', {}, { tool: { name: 'Write', input: {} } })?.verdict).toBe('ask');
  });

  it('covers the server-qualified spelling of the same capability', () => {
    // The same capability arrives under a different name depending on how the
    // session was placed. A preset covering one shape and not the other is a
    // guardrail that silently never fires.
    expect(
      run('ask_on_os_tools', {}, { tool: { name: 'mcp__orca__bash', input: {} } })?.verdict,
    ).toBe('ask');
  });

  it('abstains on a tool that does not touch the machine', () => {
    expect(run('ask_on_os_tools', {}, { tool: { name: 'WebFetch', input: {} } })).toBeUndefined();
  });
});

describe('read_only_os', () => {
  it('denies a file mutation', () => {
    expect(run('read_only_os', {}, { tool: { name: 'Write', input: {} } })?.verdict).toBe('deny');
  });

  it('allows a read', () => {
    expect(run('read_only_os', {}, { tool: { name: 'Read', input: {} } })).toBeUndefined();
  });

  it('denies a shell, which can mutate regardless of the command', () => {
    expect(
      run('read_only_os', {}, { tool: { name: 'Bash', input: { command: 'ls' } } })?.verdict,
    ).toBe('deny');
  });

  it('returns the configured reason under the param the catalog declares', () => {
    const out = run(
      'read_only_os',
      { reason: 'this agent is read-only' },
      { tool: { name: 'Write', input: {} } },
    );
    expect(out?.reason).toBe('this agent is read-only');
  });
});

describe('block_skills', () => {
  it('denies loading a blocked skill named under `skill`, matching case-insensitively', () => {
    const out = run(
      'block_skills',
      { blocked: ['deploy'] },
      {
        tool: { name: 'Skill', input: { skill: 'Deploy' } },
      },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('abstains on a skill that is not blocked', () => {
    const out = run(
      'block_skills',
      { blocked: ['deploy'] },
      {
        tool: { name: 'Skill', input: { skill: 'lint' } },
      },
    );
    expect(out).toBeUndefined();
  });

  it('does not fire on a non-skill tool whose input carries a matching name', () => {
    // The tool gate is what stops an unrelated create call from being denied as
    // a blocked skill.
    const out = run(
      'block_skills',
      { blocked: ['deploy'] },
      {
        tool: { name: 'mcp__github__create_repository', input: { name: 'deploy' } },
      },
    );
    expect(out).toBeUndefined();
  });

  it('blocks a plugin-qualified or directory-scoped name for the same skill', () => {
    // `gstack:deploy-prod` and `apps/web:deploy-prod` denote the skill `deploy-prod`;
    // a glob on the bare name must reach both, and leading/trailing space must not
    // launder past it.
    for (const skill of [
      'gstack:deploy-prod',
      'apps/web:deploy-prod',
      './deploy-prod',
      ' deploy-prod ',
    ]) {
      const out = run(
        'block_skills',
        { blocked: ['deploy-*'] },
        { tool: { name: 'Skill', input: { skill } } },
      );
      expect(out?.verdict, skill).toBe('deny');
    }
  });
});

describe('max_tool_calls_per_session', () => {
  it('counts a call and allows below the limit', () => {
    const out = run('max_tool_calls_per_session', { limit: 3 }, {}, { tool_calls: 1 });
    expect(out?.verdict).toBe('allow');
    expect(out?.stateUpdates?.[0]).toMatchObject({ action: 'increment', scope: 'session' });
  });

  it('denies once the limit is reached', () => {
    expect(run('max_tool_calls_per_session', { limit: 3 }, {}, { tool_calls: 3 })?.verdict).toBe(
      'deny',
    );
  });

  it('does not advance the counter on the call it denies', () => {
    const out = run('max_tool_calls_per_session', { limit: 3 }, {}, { tool_calls: 3 });
    expect(out?.stateUpdates ?? []).toHaveLength(0);
  });

  it('treats missing state as zero rather than failing', () => {
    expect(run('max_tool_calls_per_session', { limit: 1 }, {}, {})?.verdict).toBe('allow');
  });
});

describe('spawn_bounds', () => {
  it('counts a dispatch and allows below the cap', () => {
    const out = run(
      'spawn_bounds',
      { max_dispatches_per_turn: 2 },
      {
        tool: { name: 'Agent', input: {} },
      },
      { dispatches: 1 },
    );
    expect(out?.verdict).toBe('allow');
    expect(out?.stateUpdates?.[0]).toMatchObject({ scope: 'turn' });
  });

  it('denies past the cap', () => {
    const out = run(
      'spawn_bounds',
      { max_dispatches_per_turn: 2 },
      {
        tool: { name: 'Agent', input: {} },
      },
      { dispatches: 2 },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('abstains on a tool that is not a dispatch', () => {
    expect(
      run('spawn_bounds', { max_dispatches_per_turn: 1 }, {}, { dispatches: 5 }),
    ).toBeUndefined();
  });
});

describe('deny_pii_in_llm_request', () => {
  it('denies a message containing something shaped like a national id', () => {
    const out = run(
      'deny_pii_in_llm_request',
      { pii_types: ['ssn'] },
      {
        phase: 'request',
        userText: 'my number is 123-45-6789',
        tool: undefined as never,
      },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('denies a message containing an email when that type is selected', () => {
    const out = run(
      'deny_pii_in_llm_request',
      { pii_types: ['email'] },
      {
        phase: 'request',
        userText: 'reach me at someone@example.com',
        tool: undefined as never,
      },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('abstains when the selected types are absent', () => {
    const out = run(
      'deny_pii_in_llm_request',
      { pii_types: ['ssn'] },
      {
        phase: 'request',
        userText: 'nothing sensitive here',
        tool: undefined as never,
      },
    );
    expect(out).toBeUndefined();
  });

  it('only scans for the types it was configured with', () => {
    const out = run(
      'deny_pii_in_llm_request',
      { pii_types: ['ssn'] },
      {
        phase: 'request',
        userText: 'reach me at someone@example.com',
        tool: undefined as never,
      },
    );
    expect(out).toBeUndefined();
  });
});

describe('headless_subagent_purpose_guard', () => {
  const dispatch = (purpose?: unknown) => ({
    tool: { name: 'Agent', input: purpose === undefined ? {} : { purpose } },
  });

  it('denies a dispatch with an undeclared purpose', () => {
    const out = run(
      'headless_subagent_purpose_guard',
      { allowed_purposes: ['review'] },
      dispatch('demolish'),
    );
    expect(out?.verdict).toBe('deny');
  });

  it('abstains when the declared purpose is allowed', () => {
    expect(
      run('headless_subagent_purpose_guard', { allowed_purposes: ['review'] }, dispatch('review')),
    ).toBeUndefined();
  });

  it('enforces the catalog default when the parameter is omitted', () => {
    // The catalog advertises a default purpose list. Omitting the parameter
    // must enforce that default, not disarm the guard.
    const out = run('headless_subagent_purpose_guard', {}, dispatch('demolish'));
    expect(out?.verdict).toBe('deny');
  });

  it('accepts a default purpose when the parameter is omitted', () => {
    expect(run('headless_subagent_purpose_guard', {}, dispatch('review'))).toBeUndefined();
  });

  it('treats an explicit empty list as declining to constrain', () => {
    expect(
      run('headless_subagent_purpose_guard', { allowed_purposes: [] }, dispatch('demolish')),
    ).toBeUndefined();
  });

  it('abstains on a non-dispatch tool', () => {
    expect(
      run('headless_subagent_purpose_guard', {}, { tool: { name: 'Bash', input: {} } }),
    ).toBeUndefined();
  });
});

describe('every catalog entry has an evaluator', () => {
  it('registers one evaluator per non-internal builtin', async () => {
    const { listGuardrailTypes } = await import('../../src/catalog.js');
    const missing = listGuardrailTypes()
      .map((t) => t.name)
      .filter((name) => !BUILTIN_EVALUATORS.has(name));
    expect(missing, `catalog entries with no evaluator: ${missing.join(', ')}`).toEqual([]);
  });
});

describe('round-5 safety fixes', () => {
  it('block_skills matches glob patterns, not just exact names', () => {
    const out = run(
      'block_skills',
      { blocked: ['deploy-*'] },
      { tool: { name: 'Skill', input: { skill: 'deploy-prod' } } },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('deny_pii scans tool input even when user text is also present', () => {
    const out = run(
      'deny_pii_in_llm_request',
      {},
      { userText: 'hello there', tool: { name: 'x', input: { note: 'my ssn is 123-45-6789' } } },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('deny_pii scans the exact serialized outbound model request', () => {
    const out = run(
      'deny_pii_in_llm_request',
      {},
      {
        phase: 'llm_request',
        userText: 'safe final user message',
        serializedRequest: JSON.stringify({
          system: 'safe system prompt',
          messages: [{ role: 'tool', content: 'customer ssn: 123-45-6789' }],
        }),
      },
    );
    expect(out?.verdict).toBe('deny');
  });

  it('credit_card matches a 19-digit PAN', () => {
    const out = run(
      'deny_pii_in_llm_request',
      { pii_types: ['credit_card'] },
      { userText: 'card 1234567890123456789 here' },
    );
    expect(out?.verdict).toBe('deny');
  });
});
