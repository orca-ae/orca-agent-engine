// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { HARNESS_CATALOG, resolveHarnessAnnotation, type HarnessType } from '../src/catalog.js';
import {
  HARNESS_CAPABILITIES,
  resolveHarnessCapabilities,
  validateHarnessFeatures,
  validateHarnessGuardrails,
  validateHarnessDeployment,
} from '../src/capabilities.js';
import { resolveExecutionOwner } from '../src/execution-owner.js';

describe('harness execution contract', () => {
  it('requires a capability declaration for every catalog entry', () => {
    expect(Object.keys(HARNESS_CAPABILITIES).sort()).toEqual(Object.keys(HARNESS_CATALOG).sort());
  });

  for (const harness of Object.keys(HARNESS_CATALOG) as HarnessType[]) {
    for (const mode of HARNESS_CATALOG[harness].supportedModes) {
      it(`routes ${harness}/${mode} to exactly one owner for each target`, () => {
        const selection = resolveHarnessAnnotation({ harness, mode });
        expect(selection).toEqual({ harness, mode });
        expect(resolveExecutionOwner('self_hosted', { harness, mode })).toBe('registry');
        expect(resolveExecutionOwner('cloud', { harness, mode })).toBe(
          ['claude_agent_sdk', 'claude_agent_sdk_persistent', 'claude_code'].includes(harness) ||
            harness === 'codex_sdk' ||
            harness === 'pi_sdk'
            ? 'harness-server'
            : 'registry',
        );
      });
    }
  }

  it('normalizes the persisted legacy mode before resolving ownership', () => {
    const selection = resolveHarnessAnnotation({ harness: 'codex', mode: 'in_sandbox' });
    if ('error' in selection) throw new Error(selection.error);
    expect(resolveExecutionOwner('cloud', selection)).toBe('registry');
  });

  it('never assigns an unknown topology to either service', () => {
    expect(() =>
      resolveExecutionOwner('other', { harness: 'claude_agent_sdk', mode: 'separate' }),
    ).toThrow(/target/);
    expect(() =>
      resolveExecutionOwner('cloud', { harness: 'claude_agent_sdk', mode: 'other' as 'separate' }),
    ).toThrow(/mode/);
  });

  it('admits Codex Skills only in the mode with managed catalog and file delivery', () => {
    expect(validateHarnessFeatures({ harness: 'codex_sdk', mode: 'other' }, {})).toMatch(/mode/);
    expect(validateHarnessFeatures({ harness: 'codex_sdk' }, { skills: ['skill'] })).toBeNull();
    for (const mode of HARNESS_CATALOG.codex_sdk.supportedModes) {
      expect(validateHarnessFeatures({ harness: 'codex_sdk', mode }, {})).toBeNull();
      const skills = validateHarnessFeatures({ harness: 'codex_sdk', mode }, { skills: ['skill'] });
      expect(skills).toBeNull();
      expect(validateHarnessFeatures({ harness: 'codex_sdk', mode }, { multiagent: {} })).toMatch(
        /multiagent/,
      );
    }
    expect(
      validateHarnessFeatures(
        { harness: 'claude_agent_sdk' },
        { skills: ['skill'], multiagent: {} },
      ),
    ).toBeNull();
    expect(validateHarnessFeatures({ harness: 'custom' }, { multiagent: {} })).toBeNull();
  });

  it('defaults both managed SDKs to separate and retains explicit Codex colocated setup', () => {
    const defaultSelection = resolveHarnessAnnotation({ harness: 'codex_sdk' });
    if ('error' in defaultSelection) throw new Error(defaultSelection.error);
    expect(defaultSelection.mode).toBe('separate');
    expect(resolveHarnessAnnotation({ harness: 'claude_agent_sdk' })).toEqual({
      harness: 'claude_agent_sdk',
      mode: defaultSelection.mode,
    });
    expect(resolveExecutionOwner('cloud', defaultSelection)).toBe('harness-server');
    const colocated = { harness: 'codex_sdk', mode: 'colocated' } as const;
    expect(resolveExecutionOwner('cloud', colocated)).toBe('harness-server');
    expect(resolveHarnessCapabilities(colocated)).toMatchObject({
      needsSandboxToolset: true,
      needsMcpRewrite: true,
      nativeResume: true,
      supportsClientToolExecution: false,
      managedFeatures: { skills: true, multiagent: false },
    });
    expect(resolveHarnessCapabilities(defaultSelection)).toMatchObject({
      cloudExecutionOwner: 'harness-server',
      needsSandboxToolset: true,
      needsMcpRewrite: true,
      nativeResume: true,
      supportsClientToolExecution: false,
      managedFeatures: { skills: true, multiagent: false },
    });
    // Resolving one Session must not change the defaults of the next Session.
    expect(HARNESS_CAPABILITIES.codex_sdk.cloudExecutionOwner).toBe('harness-server');
    expect(resolveHarnessCapabilities(defaultSelection)).toEqual(HARNESS_CAPABILITIES.codex_sdk);
    expect(resolveHarnessCapabilities(colocated).needsSandboxToolset).toBe(true);
  });

  it('preserves mode-independent capabilities and rejects undeclared selections', () => {
    expect(resolveHarnessCapabilities({ harness: 'claude_agent_sdk', mode: 'separate' })).toEqual(
      HARNESS_CAPABILITIES.claude_agent_sdk,
    );
    expect(() =>
      resolveHarnessCapabilities({ harness: 'claude_agent_sdk', mode: 'colocated' }),
    ).toThrow(/does not support mode/);
  });

  it('admits only implemented target and mode combinations', () => {
    expect(
      validateHarnessDeployment('self_hosted', { harness: 'codex_sdk', mode: 'separate' }),
    ).toMatch(/codex_sdk\/separate.*self_hosted.*supported: cloud/);
    for (const target of ['cloud', 'self_hosted']) {
      expect(
        validateHarnessDeployment(target, { harness: 'codex_sdk', mode: 'colocated' }),
      ).toBeNull();
      expect(
        validateHarnessDeployment(target, { harness: 'claude_agent_sdk', mode: 'separate' }),
      ).toBeNull();
    }
    expect(
      validateHarnessDeployment('cloud', { harness: 'codex_sdk', mode: 'separate' }),
    ).toBeNull();
    expect(
      validateHarnessDeployment('unknown', { harness: 'codex_sdk', mode: 'separate' }),
    ).toMatch(/environment target 'unknown'/);
  });
});

describe('guardrail admission by deployment mode', () => {
  const stateless = { id: 'gr_rule', name: 'Rule', phases: ['request'], stateful: false };

  it('admits only the enforced Codex phases in each mode', () => {
    for (const mode of HARNESS_CATALOG.codex_sdk.supportedModes) {
      const selection = { harness: 'codex_sdk', mode } as const;
      expect(validateHarnessGuardrails(selection, [])).toBeNull();
      expect(validateHarnessGuardrails(selection, [stateless])).toBeNull();
      const tools = { ...stateless, phases: ['request', 'tool_call', 'tool_result'] };
      const result = validateHarnessGuardrails(selection, [tools]);
      if (mode === 'separate') expect(result).toBeNull();
      else expect(result).toMatch(/phase tool_call.*phase tool_result/);
      for (const phase of ['response', 'llm_request', 'llm_response', 'unknown']) {
        expect(
          validateHarnessGuardrails(selection, [{ ...stateless, phases: ['request', phase] }]),
        ).toContain(`phase ${phase} is not supported`);
      }
    }
  });

  it('admits stateful requests in both Codex modes and rejects all subagent rules', () => {
    for (const mode of HARNESS_CATALOG.codex_sdk.supportedModes) {
      const selection = { harness: 'codex_sdk', mode } as const;
      const request = validateHarnessGuardrails(selection, [{ ...stateless, stateful: true }]);
      expect(request).toBeNull();
      expect(
        validateHarnessGuardrails(selection, [
          { ...stateless, stateful: true, phases: ['request', 'tool_call'] },
        ]),
      ).toMatch(/stateful/);
      for (const scope of [{ subagent_id: 'agent' }, { subagentId: 'agent' }]) {
        expect(validateHarnessGuardrails(selection, [{ ...stateless, ...scope }])).toMatch(
          /subagent-scoped/,
        );
      }
    }
  });

  it('reports all unsupported rules and tolerates an absent display name', () => {
    expect(
      validateHarnessGuardrails({ harness: 'codex_sdk', mode: 'separate' }, [
        { id: 'gr_budget', phases: ['request', 'tool_call'], stateful: true },
        { ...stateless, phases: ['llm_request'] },
      ]),
    ).toMatch(/gr_budget \(gr_budget\): stateful.*Rule \(gr_rule\): phase llm_request/);
  });

  it('leaves existing Claude and operator-defined policy enforcement with their adapters', () => {
    for (const selection of [
      { harness: 'claude_agent_sdk', mode: 'separate' },
      { harness: 'custom', mode: 'colocated' },
    ] as const) {
      expect(validateHarnessGuardrails(selection, [{ ...stateless, stateful: true }])).toBeNull();
    }
  });
});

it('admits managed Skill materialization denial without admitting general tool policies', () => {
  const selection = { harness: 'codex_sdk', mode: 'colocated' } as const;
  const denySkills = {
    id: 'gr_skills',
    stateful: false,
    phases: ['tool_call'],
    rule: { kind: 'builtin', builtin: 'block_skills', params: { blocked: ['alpha'] } },
  };
  expect(validateHarnessGuardrails(selection, [denySkills])).toBeNull();
  expect(validateHarnessGuardrails(selection, [{ ...denySkills, stateful: true }])).toMatch(
    /stateful/,
  );
  expect(
    validateHarnessGuardrails(selection, [{ ...denySkills, subagent_id: 'agent_child' }]),
  ).toMatch(/subagent/);
  expect(
    validateHarnessGuardrails(selection, [{ ...denySkills, phases: ['tool_call', 'tool_result'] }]),
  ).toMatch(/tool_result/);
  expect(
    validateHarnessGuardrails(selection, [
      { ...denySkills, rule: { kind: 'cel', expression: 'true' } },
    ]),
  ).toMatch(/tool_call/);
});
