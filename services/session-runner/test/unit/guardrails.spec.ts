// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// Unit spec for the runner's guardrail wiring.
//
// This increment proves stateless request enforcement only. This spec pins that
// every other phase and every stateful rule is refused rather than accepted as
// configured-but-inert policy.

import { describe, it, expect } from 'vitest';
import {
  GuardrailUnsupportedError,
  evaluateRequestPhase,
  prepareRunnerGuardrails,
} from '../../src/guardrails.js';
import type { RunnerSnapshot, SnapshotGuardrail } from '../../src/snapshot.js';

function snapshot(
  guardrails: SnapshotGuardrail[],
  state?: Record<string, unknown>,
): RunnerSnapshot {
  return {
    model: { provider: 'anthropic', id: 'claude-sonnet-4' },
    provider: 'claude',
    system: '',
    allowed_tool_names: [],
    allowed_mcp_server_names: [],
    tool_permissions: {},
    egress: null,
    guardrails,
    ...(state ? { guardrail_state: state } : {}),
  };
}

const blockShell: SnapshotGuardrail = {
  id: 'grd_shell',
  name: 'No shells',
  tier: 'workspace',
  phases: ['tool_call'],
  rule: { kind: 'builtin', builtin: 'block_tools', params: { tools: ['Bash'] } },
  stateful: false,
};

describe('prepareRunnerGuardrails', () => {
  it('refuses every phase the runner has not wired', () => {
    for (const phase of ['tool_call', 'tool_result', 'llm_request', 'llm_response', 'response']) {
      expect(() => prepareRunnerGuardrails(snapshot([{ ...blockShell, phases: [phase] }]))).toThrow(
        GuardrailUnsupportedError,
      );
    }
  });

  it('names the guardrail and the phase in the refusal', () => {
    // An operator reading this has to know which rule to change.
    expect(() =>
      prepareRunnerGuardrails(snapshot([{ ...blockShell, phases: ['llm_request'] }])),
    ).toThrow(/No shells \(grd_shell\).*llm_request/);
  });

  it('prepares a stateless request-only rule', () => {
    const prepared = prepareRunnerGuardrails(
      snapshot([{ ...blockShell, id: 'grd_req', phases: ['request'] }]),
    );
    expect(prepared.prepared).toHaveLength(1);
  });

  it('seeds the store from the restored session state', () => {
    const prepared = prepareRunnerGuardrails(
      snapshot([{ ...blockShell, phases: ['request'] }], { tool_calls: 7 }),
    );
    expect(prepared.store.read('session')).toMatchObject({ tool_calls: 7 });
  });

  it('refuses stateful rules until Registry write-through exists', () => {
    expect(() =>
      prepareRunnerGuardrails(
        snapshot([
          {
            ...blockShell,
            phases: ['request'],
            stateful: true,
            state_scope: 'session',
          },
        ]),
      ),
    ).toThrow(/stateful rules require durable Registry write-through/);
  });

  it('refuses subagent-scoped rules until dispatch enforcement is wired', () => {
    expect(() =>
      prepareRunnerGuardrails(
        snapshot([
          {
            ...blockShell,
            phases: ['request'],
            subagent_id: 'agt_researcher',
          },
        ]),
      ),
    ).toThrow(/subagent-scoped request enforcement is not wired/);
  });

  it('delegates only managed Codex request rules to Registry', () => {
    const guarded = {
      ...snapshot([{ ...blockShell, phases: ['request'], stateful: true }]),
      provider: 'codex-sdk',
      managed_resources: { version: 1 as const, revision: 'a'.repeat(64) },
      request_guardrails_owner: 'registry' as const,
    };
    expect(prepareRunnerGuardrails(guarded).prepared).toEqual([]);
    const { managed_resources: _resources, ...unmanaged } = guarded;
    expect(() => prepareRunnerGuardrails(unmanaged)).toThrow(/invalid Registry/);
    expect(() => prepareRunnerGuardrails({ ...guarded, guardrails: [blockShell] })).toThrow(
      /tool_call/,
    );
    expect(() =>
      prepareRunnerGuardrails({
        ...guarded,
        guardrails: [{ ...blockShell, phases: ['request'], subagent_id: 'agt_child' }],
      }),
    ).toThrow(/subagent/);
  });

  it('carries an empty snapshot through as no guardrails', () => {
    const prepared = prepareRunnerGuardrails(snapshot([]));
    expect(prepared.prepared).toEqual([]);
  });
});

describe('evaluateRequestPhase', () => {
  const denyEmpty: SnapshotGuardrail = {
    id: 'grd_req',
    name: 'No empty asks',
    tier: 'workspace',
    phases: ['request'],
    rule: {
      kind: 'expression',
      expression: `event.session.id == 'ses_allowed'`,
      onFalse: 'deny',
      reason: 'This session may not start a turn.',
    },
    stateful: false,
  };

  it('denies with the rule reason', () => {
    const { prepared, store } = prepareRunnerGuardrails(snapshot([denyEmpty]));
    const decision = evaluateRequestPhase(prepared, store, {
      phase: 'request',
      sessionId: 'ses_blocked',
      userText: 'hello',
    });
    expect(decision.verdict).toBe('deny');
    expect(decision.reasons).toContain('This session may not start a turn.');
  });

  it('allows when the rule is satisfied', () => {
    const { prepared, store } = prepareRunnerGuardrails(snapshot([denyEmpty]));
    const decision = evaluateRequestPhase(prepared, store, {
      phase: 'request',
      sessionId: 'ses_allowed',
      userText: 'hello',
    });
    expect(decision.verdict).toBe('allow');
  });
});
