// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  GUARDRAIL_CATALOG,
  getGuardrailType,
  listGuardrailTypes,
  type GuardrailTypeEntry,
  type JsonSchema,
} from '../../src/catalog.js';
import { VERDICTS } from '../../src/lattice.js';
import { PHASES, STATE_SCOPES, supportsAsk } from '../../src/types.js';

/** Every builtin the catalog is expected to describe, internal ones included. */
const EXPECTED_NAMES = [
  'ask_on_os_tools',
  'blast_radius',
  'block_skills',
  'block_tools',
  'block_working_dir_changes',
  'cost_budget',
  'deny_pii_in_llm_request',
  'detect_loop',
  'detect_thrashing',
  'gcalendar_policy',
  'gdrive_policy',
  'github_policy',
  'gmail_policy',
  'headless_subagent_purpose_guard',
  'max_tool_calls_per_session',
  'read_only_os',
  'require_approval_for_tools',
  'spawn_bounds',
  'subagent_cost_budget',
  'token_budget',
  'tool_permission_policy',
  'user_daily_cost_budget',
  'worktree_guard',
];

function propertiesOf(schema: JsonSchema): Record<string, JsonSchema> {
  return schema.properties ?? {};
}

function entry(name: string): GuardrailTypeEntry {
  const found = getGuardrailType(name);
  if (!found) throw new Error(`catalog is missing ${name}`);
  return found;
}

describe('catalog coverage', () => {
  it('describes every builtin the model defines, and nothing else', () => {
    const names = GUARDRAIL_CATALOG.map((e) => e.name).sort();
    expect(names).toEqual([...EXPECTED_NAMES].sort());
  });

  it('names each builtin exactly once', () => {
    const names = GUARDRAIL_CATALOG.map((e) => e.name);
    expect(new Set(names).size).toBe(names.length);
  });

  it('has no entry for expressions, which are a separate rule kind rather than a builtin', () => {
    expect(getGuardrailType('cel_policy')).toBeUndefined();
    expect(getGuardrailType('expression')).toBeUndefined();
  });
});

describe('entry shape', () => {
  it('gives every builtin a name, a human label, and a description', () => {
    for (const e of GUARDRAIL_CATALOG) {
      expect(e.name.length, e.name).toBeGreaterThan(0);
      expect(e.title.length, e.name).toBeGreaterThan(0);
      expect(e.description.length, e.name).toBeGreaterThan(0);
    }
  });

  it('names builtins in the snake_case the wire format uses', () => {
    for (const e of GUARDRAIL_CATALOG) {
      expect(e.name, e.name).toMatch(/^[a-z][a-z0-9_]*$/);
    }
  });

  it('fires every builtin on at least one phase drawn from the phase vocabulary', () => {
    for (const e of GUARDRAIL_CATALOG) {
      expect(e.phases.length, e.name).toBeGreaterThan(0);
      for (const phase of e.phases) expect(PHASES, e.name).toContain(phase);
    }
  });

  it('lists each phase at most once per builtin', () => {
    for (const e of GUARDRAIL_CATALOG) {
      expect(new Set(e.phases).size, e.name).toBe(e.phases.length);
    }
  });

  it('gives every builtin at least one verdict drawn from the verdict lattice', () => {
    for (const e of GUARDRAIL_CATALOG) {
      expect(e.verdicts.length, e.name).toBeGreaterThan(0);
      for (const verdict of e.verdicts) expect(VERDICTS, e.name).toContain(verdict);
    }
  });

  it('never declares allow, which is the default rather than an outcome a rule produces', () => {
    for (const e of GUARDRAIL_CATALOG) {
      expect(e.verdicts, e.name).not.toContain('allow');
    }
  });
});

describe('state declarations', () => {
  it('declares a state scope exactly when the builtin is stateful', () => {
    for (const e of GUARDRAIL_CATALOG) {
      expect(e.stateScope !== undefined, e.name).toBe(e.stateful);
    }
  });

  it('draws every state scope from the state scope vocabulary', () => {
    for (const e of GUARDRAIL_CATALOG) {
      if (e.stateScope !== undefined) expect(STATE_SCOPES, e.name).toContain(e.stateScope);
    }
  });

  it('keeps a stateless partition large enough to gate tool exposure without a state store', () => {
    const stateless = GUARDRAIL_CATALOG.filter((e) => !e.stateful);
    expect(stateless.length).toBeGreaterThan(0);
  });
});

describe('parameter schemas', () => {
  it('describes parameters as a JSON Schema object', () => {
    for (const e of GUARDRAIL_CATALOG) {
      expect(e.paramsSchema.type, e.name).toBe('object');
      expect(e.paramsSchema.properties, e.name).toBeTypeOf('object');
    }
  });

  it('closes every schema, so an unrecognised parameter is rejected at authoring time', () => {
    for (const e of GUARDRAIL_CATALOG) {
      expect(e.paramsSchema.additionalProperties, e.name).toBe(false);
    }
  });

  it('declares every required parameter among the properties', () => {
    for (const e of GUARDRAIL_CATALOG) {
      const declared = Object.keys(propertiesOf(e.paramsSchema));
      for (const key of e.paramsSchema.required ?? []) {
        expect(declared, `${e.name}.${key}`).toContain(key);
      }
    }
  });

  it('types and documents every parameter', () => {
    for (const e of GUARDRAIL_CATALOG) {
      for (const [key, schema] of Object.entries(propertiesOf(e.paramsSchema))) {
        expect(schema.type, `${e.name}.${key}`).toBeTypeOf('string');
        expect(schema.description?.length ?? 0, `${e.name}.${key}`).toBeGreaterThan(0);
      }
    }
  });

  it('gives every array parameter an item schema', () => {
    for (const e of GUARDRAIL_CATALOG) {
      for (const [key, schema] of Object.entries(propertiesOf(e.paramsSchema))) {
        if (schema.type === 'array') expect(schema.items, `${e.name}.${key}`).toBeTypeOf('object');
      }
    }
  });

  it('keeps every default inside the enumeration it belongs to', () => {
    for (const e of GUARDRAIL_CATALOG) {
      for (const [key, schema] of Object.entries(propertiesOf(e.paramsSchema))) {
        if (schema.enum && schema.default !== undefined) {
          expect(schema.enum, `${e.name}.${key}`).toContain(schema.default);
        }
        const items = schema.items;
        if (schema.type === 'array' && items?.enum && Array.isArray(schema.default)) {
          for (const value of schema.default) {
            expect(items.enum, `${e.name}.${key}`).toContain(value);
          }
        }
      }
    }
  });

  it('never marks a parameter required twice', () => {
    for (const e of GUARDRAIL_CATALOG) {
      const required = e.paramsSchema.required ?? [];
      expect(new Set(required).size, e.name).toBe(required.length);
    }
  });
});

describe('public listing', () => {
  it('hides builtins that are not authored by users', () => {
    const listed = listGuardrailTypes().map((e) => e.name);
    expect(listed).not.toContain('tool_permission_policy');
  });

  it('offers every user-authored builtin', () => {
    const listed = listGuardrailTypes()
      .map((e) => e.name)
      .sort();
    const expected = GUARDRAIL_CATALOG.filter((e) => !e.internal)
      .map((e) => e.name)
      .sort();
    expect(listed).toEqual(expected);
    expect(listed).toHaveLength(GUARDRAIL_CATALOG.length - 1);
  });
});

describe('lookup', () => {
  it('finds a builtin by name', () => {
    expect(getGuardrailType('block_tools')?.name).toBe('block_tools');
  });

  it('resolves internal builtins the public listing hides, because the engine still needs them', () => {
    expect(getGuardrailType('tool_permission_policy')?.internal).toBe(true);
  });

  it('returns undefined for a name the catalog does not know', () => {
    expect(getGuardrailType('no_such_builtin')).toBeUndefined();
    expect(getGuardrailType('')).toBeUndefined();
  });
});

describe('individual builtins', () => {
  it('seeds the fold from the permission policy without offering it for authoring', () => {
    const e = entry('tool_permission_policy');
    expect(e.internal).toBe(true);
    expect(e.phases).toEqual(['tool_call']);
    expect(e.stateful).toBe(false);
    expect(e.verdicts).toEqual(['ask', 'deny']);
    expect(Object.keys(propertiesOf(e.paramsSchema))).toEqual([]);
  });

  it('escalates named tools to ask without ever denying them', () => {
    const e = entry('require_approval_for_tools');
    expect(e.verdicts).toEqual(['ask']);
    expect(e.paramsSchema.required).toEqual(['tools']);
  });

  it('denies named tools outright', () => {
    const e = entry('block_tools');
    expect(e.verdicts).toEqual(['deny']);
    expect(e.paramsSchema.required).toEqual(['tools']);
  });

  it('scans for personal data before a prompt reaches the model', () => {
    const e = entry('deny_pii_in_llm_request');
    expect(e.phases).toEqual(['request', 'llm_request']);
    expect(e.verdicts).toEqual(['deny']);
    expect(e.stateful).toBe(false);
    expect(propertiesOf(e.paramsSchema).pii_types?.default).toEqual([
      'ssn',
      'credit_card',
      'email',
      'phone',
    ]);
  });

  it('caps total tool calls against session state', () => {
    const e = entry('max_tool_calls_per_session');
    expect(e.stateful).toBe(true);
    expect(e.stateScope).toBe('session');
    expect(propertiesOf(e.paramsSchema).limit?.default).toBe(100);
  });

  it('caps tokens at both the request and the tool call, needing no price data', () => {
    const e = entry('token_budget');
    expect(e.phases).toEqual(['request', 'tool_call']);
    expect(e.stateScope).toBe('session');
    expect(e.paramsSchema.required).toEqual(['max_total_tokens']);
  });

  it('bounds subagent dispatches per turn rather than for the whole session', () => {
    const e = entry('spawn_bounds');
    expect(e.stateful).toBe(true);
    expect(e.stateScope).toBe('turn');
    expect(e.verdicts).toEqual(['deny']);
    expect(propertiesOf(e.paramsSchema).max_dispatches_per_turn?.default).toBe(5);
  });

  it('publishes the unpriced behavior shared by every cost budget', () => {
    for (const name of ['cost_budget', 'user_daily_cost_budget', 'subagent_cost_budget']) {
      const onUnpriced = propertiesOf(entry(name).paramsSchema).on_unpriced;
      expect(onUnpriced?.enum).toEqual(['ask', 'deny', 'allow']);
      expect(onUnpriced?.default).toBe('ask');
    }
  });

  it('watches a repeating tool-and-argument cycle as calls are made', () => {
    const e = entry('detect_loop');
    expect(e.phases).toEqual(['tool_call']);
    expect(e.stateScope).toBe('session');
    expect(propertiesOf(e.paramsSchema).threshold?.minimum).toBe(2);
  });

  it('watches a run of failing results, which is only visible after a tool has run', () => {
    const e = entry('detect_thrashing');
    expect(e.phases).toEqual(['tool_result']);
    expect(e.stateful).toBe(true);
    expect(e.stateScope).toBe('session');
  });

  it('contains Drive writes by also observing what the session read', () => {
    const e = entry('gdrive_policy');
    expect(e.phases).toEqual(['tool_call', 'tool_result']);
    expect(e.stateful).toBe(true);
    expect(e.stateScope).toBe('session');
  });

  it('keeps mail and calendar read-only until a parameter says otherwise', () => {
    const mail = propertiesOf(entry('gmail_policy').paramsSchema);
    expect(mail.allow_read?.default).toBe(true);
    expect(mail.allow_send?.default).toBe(false);

    const calendar = propertiesOf(entry('gcalendar_policy').paramsSchema);
    expect(calendar.allow_read?.default).toBe(true);
    expect(calendar.allow_create_events?.default).toBe(false);
    expect(calendar.allow_modify_events?.default).toBe(false);
  });

  it('confines writes to an allowed root by default', () => {
    const e = entry('worktree_guard');
    expect(e.verdicts).toEqual(['deny']);
    expect(propertiesOf(e.paramsSchema).allowed_root?.default).toBe('.worktrees');
  });

  it('spends against session state for a session budget and a subagent budget', () => {
    for (const name of ['cost_budget', 'subagent_cost_budget']) {
      const e = entry(name);
      expect(e.phases, name).toEqual(['request', 'tool_call']);
      expect(e.stateScope, name).toBe('session');
      expect(e.verdicts, name).toEqual(['ask', 'deny']);
    }
  });

  it('tracks a principal daily spend in a window that outlives any one session', () => {
    const e = entry('user_daily_cost_budget');
    expect(e.stateful).toBe(true);
    expect(e.stateScope).toBe('subject_window');
    expect(e.allowedScopes).toEqual(['workspace', 'organization']);
    expect(e.paramsSchema.required).toEqual(['max_cost_usd']);
  });

  it('bounds every cost budget above zero, since a zero cap would deny everything silently', () => {
    for (const name of ['cost_budget', 'subagent_cost_budget', 'user_daily_cost_budget']) {
      const max = propertiesOf(entry(name).paramsSchema).max_cost_usd;
      expect(max?.type, name).toBe('number');
      expect(max?.exclusiveMinimum, name).toBe(0);
    }
  });
});

describe('verdicts are reachable on the phases a type declares', () => {
  it('never advertises ask on a type whose phases cannot ask', () => {
    // A type declaring `ask` on a phase with no approval round trip advertises
    // a verdict the engine is obliged to degrade to deny. A client rendering
    // this catalog would offer a choice that does not exist.
    for (const entry of GUARDRAIL_CATALOG) {
      if (!entry.verdicts.includes('ask')) continue;
      const askable = entry.phases.some(supportsAsk);
      expect(askable, `${entry.name} declares ask but none of its phases can ask`).toBe(true);
    }
  });

  it('never advertises a params enum value the type cannot actually return', () => {
    for (const entry of GUARDRAIL_CATALOG) {
      const props = (entry.paramsSchema.properties ?? {}) as Record<
        string,
        { enum?: readonly unknown[] }
      >;
      for (const [param, schema] of Object.entries(props)) {
        if (!schema.enum) continue;
        const verdictValued = schema.enum.every((v) => v === 'ask' || v === 'deny');
        if (!verdictValued) continue;
        for (const value of schema.enum) {
          expect(
            entry.verdicts.includes(value as 'ask' | 'deny'),
            `${entry.name}.${param} offers "${String(value)}" but the type does not declare it`,
          ).toBe(true);
        }
      }
    }
  });
});
