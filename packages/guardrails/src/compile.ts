// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { getGuardrailType, type JsonSchema } from './catalog.js';
import { compileExpression, type CompiledExpression } from './cel.js';
import { SCOPES, type GuardrailRule, type Scope, type StateScope } from './types.js';

/**
 * Authoring-time validation.
 *
 * Everything here runs when a guardrail is written, never when it fires. A rule
 * that cannot be evaluated should be a 400 at the API, not a surprise partway
 * through a session — by which point the author is gone and the agent is the
 * one holding the error.
 */

export interface RuleCompileError {
  /** Parameter name, when the problem is attributable to one. */
  path?: string;
  message: string;
}

export interface CompiledRule {
  ok: true;
  /** Whether evaluating this rule needs state, and from which scope. */
  stateful: boolean;
  stateScope?: StateScope;
  /** Validated parameters, unchanged — no defaults are materialised. */
  params: Record<string, unknown>;
  /** Present for expression rules, so the expression is parsed once. */
  compiled?: CompiledExpression;
}

export type RuleCompileResult = CompiledRule | { ok: false; errors: readonly RuleCompileError[] };

export function compileGuardrailRule(rule: GuardrailRule, scope: Scope): RuleCompileResult {
  if (!SCOPES.some((candidate) => candidate === scope)) {
    return {
      ok: false,
      errors: [{ path: 'scope', message: `scope must be one of ${SCOPES.join(', ')}` }],
    };
  }
  if (rule.kind === 'expression') return compileExpressionRule(rule);
  return compileBuiltinRule(rule.builtin, rule.params ?? {}, scope);
}

function compileExpressionRule(
  rule: Extract<GuardrailRule, { kind: 'expression' }>,
): RuleCompileResult {
  if (rule.onFalse !== 'ask' && rule.onFalse !== 'deny') {
    return {
      ok: false,
      errors: [{ path: 'onFalse', message: 'onFalse must be one of "ask", "deny"' }],
    };
  }

  const result = compileExpression(rule.expression);
  if (!result.ok)
    return { ok: false, errors: [{ path: 'expression', message: result.error.message }] };
  return {
    ok: true,
    stateful: result.compiled.stateful,
    ...(result.compiled.stateful ? { stateScope: 'session' as const } : {}),
    params: {},
    compiled: result.compiled,
  };
}

function compileBuiltinRule(
  name: string,
  rawParams: Record<string, unknown>,
  scope: Scope,
): RuleCompileResult {
  const type = getGuardrailType(name);
  // An internal builtin is machinery the engine seeds itself; an author may not
  // reference it, and saying so in the same words as a missing name keeps the
  // internal vocabulary from leaking through the authoring endpoint.
  if (!type || type.internal) {
    return { ok: false, errors: [{ message: `unknown guardrail type: ${name}` }] };
  }

  // A shallow own-property copy. A params object carrying values on its prototype
  // (`Object.create({max_cost_usd: 25})`, or a `__proto__` key an API layer let
  // through) would otherwise satisfy validation by reading an inherited value,
  // then serialise to `{}` and persist as a rule enforcing nothing. Everything
  // below reads and stores this copy, so only own properties count.
  const params: Record<string, unknown> = { ...rawParams };

  const errors = validateParams(params, type.paramsSchema);
  if (type.allowedScopes && !type.allowedScopes.includes(scope)) {
    errors.push({
      message: `${name} may only be authored at ${type.allowedScopes.join(' or ')} scope`,
    });
  }
  // Some types are valid only once at least one of a set of parameters is
  // configured — a budget with neither a cap nor a threshold enforces nothing,
  // and a flat schema cannot express "one of these". An empty array counts as
  // absent: `{ask_thresholds_usd: []}` is a threshold list that lists nothing.
  if (type.requireAtLeastOneOf && !type.requireAtLeastOneOf.some((k) => isConfigured(params[k]))) {
    errors.push({
      message: `at least one of ${type.requireAtLeastOneOf.join(', ')} must be set`,
    });
  }
  if (errors.length > 0) return { ok: false, errors };

  return {
    ok: true,
    stateful: type.stateful,
    ...(type.stateScope ? { stateScope: type.stateScope } : {}),
    // Returned as written. Materialising declared defaults here would put
    // values in the stored record the author never chose, and for at least one
    // parameter an absent value and an explicit empty one mean the same thing —
    // writing the default in would turn an omission into an assertion.
    params,
  };
}

/** A parameter that carries a real value: present, and not an empty array. */
function isConfigured(value: unknown): boolean {
  if (value === undefined) return false;
  if (Array.isArray(value)) return value.length > 0;
  return true;
}

function validateParams(params: Record<string, unknown>, schema: JsonSchema): RuleCompileError[] {
  const errors: RuleCompileError[] = [];
  const properties = schema.properties ?? {};

  for (const key of Object.keys(params)) {
    if (!Object.hasOwn(properties, key)) {
      // Closed by design: a misspelled parameter that is quietly dropped leaves
      // an author believing a limit is enforced that never was. `hasOwn`, not
      // `in`, so a parameter named after a prototype member (`toString`,
      // `__proto__`) is still rejected rather than mistaken for a known one.
      errors.push({ path: key, message: `unknown parameter: ${key}` });
    }
  }

  for (const required of schema.required ?? []) {
    if (params[required] === undefined) {
      errors.push({ path: required, message: `missing required parameter: ${required}` });
    }
  }

  for (const [key, propSchema] of Object.entries(properties)) {
    const value = params[key];
    if (value === undefined) continue;
    errors.push(...validateValue(value, propSchema, key));
  }

  return errors;
}

function validateValue(value: unknown, schema: JsonSchema, path: string): RuleCompileError[] {
  const errors: RuleCompileError[] = [];

  if (schema.enum && !schema.enum.includes(value)) {
    const allowed = schema.enum.map((v) => JSON.stringify(v)).join(', ');
    errors.push({ path, message: `${path} must be one of ${allowed}` });
    return errors;
  }

  switch (schema.type) {
    case 'array': {
      if (!Array.isArray(value)) {
        errors.push({ path, message: `${path} must be an array` });
        break;
      }
      if (schema.minItems !== undefined && value.length < schema.minItems) {
        errors.push({ path, message: `${path} must have at least ${schema.minItems} item(s)` });
      }
      if (schema.items) {
        const items = schema.items;
        value.forEach((item, index) => {
          errors.push(...validateValue(item, items, `${path}[${index}]`));
        });
      }
      break;
    }
    case 'integer':
      if (typeof value !== 'number' || !Number.isInteger(value)) {
        errors.push({ path, message: `${path} must be an integer` });
        break;
      }
      errors.push(...validateRange(value, schema, path));
      break;
    case 'number':
      // `Number.isFinite`, not `!Number.isNaN`: `1e999` parses from JSON to
      // `Infinity`, which is a `number` and not `NaN`, and as a budget cap would
      // never be reached by any finite spend.
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        errors.push({ path, message: `${path} must be a finite number` });
        break;
      }
      errors.push(...validateRange(value, schema, path));
      break;
    case 'string':
      if (typeof value !== 'string') errors.push({ path, message: `${path} must be a string` });
      break;
    case 'boolean':
      if (typeof value !== 'boolean') errors.push({ path, message: `${path} must be a boolean` });
      break;
    case 'object':
      if (typeof value !== 'object' || value === null || Array.isArray(value)) {
        errors.push({ path, message: `${path} must be an object` });
      }
      break;
    default:
      break;
  }

  return errors;
}

function validateRange(value: number, schema: JsonSchema, path: string): RuleCompileError[] {
  const errors: RuleCompileError[] = [];
  if (schema.minimum !== undefined && value < schema.minimum) {
    errors.push({ path, message: `${path} must be at least ${schema.minimum}` });
  }
  // A spend cap of zero or below is not a budget, so the catalog declares an
  // exclusive bound. Ignoring it here would accept `max_cost_usd: 0`, which
  // reads as "no spend allowed" but evaluates as a cap already exceeded.
  if (schema.exclusiveMinimum !== undefined && value <= schema.exclusiveMinimum) {
    errors.push({ path, message: `${path} must be greater than ${schema.exclusiveMinimum}` });
  }
  if (schema.maximum !== undefined && value > schema.maximum) {
    errors.push({ path, message: `${path} must be at most ${schema.maximum}` });
  }
  return errors;
}
