// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0
import { describe, it, expect } from 'vitest';
import {
  compileGuardrailRule,
  evaluateDogwood,
  evaluateDogwoodDecision,
  mergeDogwoodEvents,
} from '../../src/index.js';
import { evaluate } from '@runorca/dogwood';
const policy = '@id("baseline") permit(principal,action,resource);';
const input = {
  guardrail_id: 'grd-a',
  rule: { kind: 'dogwood' as const, policy },
  now_ms: 10000,
  history: [],
  call: {
    action: 'bash',
    principal: { type: 'Unattributed' as const, id: 's' },
    resource: {
      type: 'Agent' as const,
      id: 'a',
      attributes: { name: 'agent' },
      parents: [{ type: 'Workspace' as const, id: 'w' }],
    },
    request_id: 'tool',
    session_id: 's',
    input: { command: 'ls' },
  },
};
describe('actual pinned Dogwood', () => {
  it('validates authoring, default deny warnings, and annotation errors', () => {
    expect(compileGuardrailRule({ kind: 'dogwood', policy }, 'workspace').ok).toBe(true);
    const forbid = compileGuardrailRule(
      { kind: 'dogwood', policy: '@id("block") forbid(principal,action,resource);' },
      'explicit',
    );
    expect(forbid.ok && forbid.warnings?.length).toBeGreaterThan(0);
    expect(
      compileGuardrailRule(
        {
          kind: 'dogwood',
          policy:
            '@id("duplicate") permit(principal,action,resource); @id("duplicate") forbid(principal,action,resource);',
        },
        'workspace',
      ).ok,
    ).toBe(false);
  });
  it('uses the same WASM semantics in the bounded worker', async () => {
    expect(await evaluateDogwood(input)).toEqual(evaluate(input));
    expect(await evaluateDogwoodDecision(input)).toEqual(evaluate(input));
  });
  it('fails closed without events when a request cannot cross the worker boundary', async () => {
    const unserializable = { ...input, uncloneable: () => 'not wire data' };
    expect(await evaluateDogwoodDecision(unserializable)).toMatchObject({
      verdict: 'deny',
      errors: ['Dogwood evaluator unavailable'],
      valid_before: null,
      events: { allow: [], deny: [] },
    });
  });
  it('unions bounded projections without reserving the same request twice', () => {
    const event = evaluate(input).events.allow[0]!;
    const command = { ...event, input: { command: 'ls' } };
    const directory = { ...event, input: { cwd: '/workspace' } };
    expect(mergeDogwoodEvents([])).toEqual([]);
    const merged = mergeDogwoodEvents([command, directory, command]);
    expect(merged).toHaveLength(1);
    expect(merged[0]).toMatchObject({
      kind: 'request',
      request_id: input.call.request_id,
      input: { command: 'ls', cwd: '/workspace' },
    });
  });
  it('rejects conflicting projections for one authorization', () => {
    const event = evaluate(input).events.allow[0]!;
    expect(() =>
      mergeDogwoodEvents([
        { ...event, input: { command: 'ls' } },
        { ...event, input: { command: 'rm' } },
      ]),
    ).toThrow();
  });
});
