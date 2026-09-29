// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import Ajv from 'ajv';
import { load } from 'js-yaml';
import { describe, expect, it } from 'vitest';

/**
 * Does the published document accept the values this service accepts?
 *
 * `openapi-document.spec.ts` asks whether the document is well-formed.
 * Well-formed is not the same as correct, and the gap is not academic: the
 * natural repair for `nullable` on a union — hoist a `type` beside it so the
 * keyword becomes legal — produces a document that validates, compiles, and
 * **rejects `null`**, because the sibling `oneOf` still applies and no branch
 * matches. Only running values through it can tell the two apart.
 *
 * Every schema in the document is compiled, and the values below are ones a
 * caller really sends. Three of them failed against the committed artifact:
 * `{ provider, id }` matched two `ModelInput` branches, an ordinary metadata
 * string matched two branches of its patch value, and a 600-character one
 * escaped `maxLength` entirely through the branch `z.null()` rendered into.
 */

const document = load(
  readFileSync(new URL('../../openapi/managed-agents.yaml', import.meta.url), 'utf8'),
) as { paths: Record<string, Record<string, Operation>> };

interface Operation {
  requestBody?: { content?: Record<string, { schema?: unknown }> };
  responses?: Record<string, { content?: Record<string, { schema?: unknown }> }>;
}

const HTTP_METHODS = ['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace'];

/**
 * Restate exclusive bounds in the numeric form AJV's dialect uses.
 *
 * Not a defect being papered over: `exclusiveMinimum: true` beside `minimum` is
 * the **correct** spelling for the OAS 3.0.2 document we publish, and
 * `openapi-document.spec.ts` asserts it. AJV 8 speaks a later draft where the
 * keyword is numeric. The conversion belongs here, in the reader, and never in
 * the document — "make AJV compile everything" applied the other way round
 * would undo the fix that put these in 3.0 form.
 */
function toAjvDialect<T>(node: T): T {
  if (Array.isArray(node)) return node.map(toAjvDialect) as unknown as T;
  if (!node || typeof node !== 'object') return node;
  const schema = node as Record<string, unknown>;
  for (const [keyword, bound] of [
    ['exclusiveMinimum', 'minimum'],
    ['exclusiveMaximum', 'maximum'],
  ]) {
    if (schema[keyword!] === true && typeof schema[bound!] === 'number') {
      schema[keyword!] = schema[bound!];
      delete schema[bound!];
    }
  }
  for (const key of Object.keys(schema)) schema[key] = toAjvDialect(schema[key]);
  return schema as T;
}

const ajv = new Ajv({ strict: false, allErrors: true, validateFormats: false, logger: false });
const compile = (schema: unknown) => ajv.compile(toAjvDialect(structuredClone(schema)) as object);

const mediaSchemas = () =>
  Object.entries(document.paths).flatMap(([path, item]) =>
    Object.entries(item)
      .filter(([method]) => HTTP_METHODS.includes(method))
      .flatMap(([method, operation]) => {
        const label = `${method.toUpperCase()} ${path}`;
        const bodies = Object.entries(operation.requestBody?.content ?? {}).map(
          ([media, entry]) => ({ label: `${label} request (${media})`, schema: entry.schema }),
        );
        const responses = Object.entries(operation.responses ?? {}).flatMap(([code, response]) =>
          Object.entries(response.content ?? {}).map(([media, entry]) => ({
            label: `${label} response ${code} (${media})`,
            schema: entry.schema,
          })),
        );
        return [...bodies, ...responses].filter((entry) => entry.schema !== undefined);
      }),
  );

const agentBody = () =>
  document.paths['/v1/agents']!.post!.requestBody!.content!['application/json']!.schema;
const patchBody = () =>
  document.paths['/v1/agents/{id}']!.post!.requestBody!.content!['application/json']!.schema;
const propertyOf = (schema: unknown, name: string) =>
  (schema as { properties: Record<string, unknown> }).properties[name];

describe('the published document, run against real values', () => {
  it('compiles every published schema', () => {
    const failures: string[] = [];
    for (const { label, schema } of mediaSchemas()) {
      try {
        compile(schema);
      } catch (error) {
        failures.push(`${label}: ${(error as Error).message.split('\n')[0]}`);
      }
    }

    expect(failures).toEqual([]);
  });

  it.each([
    ['a bare model id', 'claude-opus-5'],
    ['an object model', { id: 'claude-opus-5' }],
    // The reported case. `{ provider, id }` matched the `{id}` branch and the
    // `{provider, id}` branch, so a `oneOf` rejected a value `AgentCreate`
    // parses without complaint.
    ['a model with a provider', { provider: 'anthropic', id: 'claude-opus-5' }],
    ['a model with an effort enum', { id: 'claude-opus-5', effort: 'high' }],
    ['a model with an effort object', { id: 'claude-opus-5', effort: { type: 'high' } }],
    ['a model with a null effort', { id: 'claude-opus-5', effort: null }],
    ['a model with a null speed', { id: 'claude-opus-5', speed: null }],
  ])('accepts %s', (_label, model) => {
    const validate = compile(propertyOf(agentBody(), 'model'));

    expect({ valid: validate(model), errors: validate.errors }).toEqual({
      valid: true,
      errors: null,
    });
  });

  it.each([
    ['an ordinary value', 'production'],
    ['a cleared value', null],
    ['a value at the bound', 'y'.repeat(512)],
  ])('accepts %s in a metadata patch', (_label, value) => {
    const validate = compile(propertyOf(patchBody(), 'metadata'));

    expect({ valid: validate({ env: value }), errors: validate.errors }).toEqual({
      valid: true,
      errors: null,
    });
  });

  it('still enforces the metadata value bound', () => {
    // The bound was unenforceable while `z.null()` rendered as an unconstrained
    // string: an over-length value simply matched that branch instead.
    const validate = compile(propertyOf(patchBody(), 'metadata'));

    expect(validate({ env: 'y'.repeat(513) })).toBe(false);
  });

  it('accepts null wherever it says a value may be null', () => {
    // The one assertion covering all three ways the generator states nullability
    // — `nullable` on a type, an explicit null branch in a union, and `null` as
    // an enum member — and the only one that can tell a fix from its
    // look-alike. Hoisting a `type` beside `nullable` on a union satisfies every
    // structural check in `openapi-document.spec.ts` and fails here; a
    // `nullable` enum that omits `null` passes every check ever written for this
    // document and rejected `speed: null` on 22 schemas.
    const claimed: Record<string, unknown>[] = [];
    const collect = (node: unknown): void => {
      if (Array.isArray(node)) return node.forEach(collect);
      if (!node || typeof node !== 'object') return;
      const schema = node as Record<string, unknown>;
      const saysNullable =
        schema.nullable === true ||
        (Array.isArray(schema.oneOf) && schema.oneOf.some(isNullBranch)) ||
        (Array.isArray(schema.enum) && schema.enum.includes(null));
      if (saysNullable) claimed.push(schema);
      for (const value of Object.values(schema)) collect(value);
    };
    collect(document.paths);

    expect(claimed.length).toBeGreaterThan(0);
    const rejecting = claimed.filter((schema) => !compile(schema)(null));
    expect(rejecting.map((schema) => JSON.stringify(schema).slice(0, 120))).toEqual([]);
  });
});

function isNullBranch(branch: unknown): boolean {
  if (!branch || typeof branch !== 'object') return false;
  const { enum: values } = branch as { enum?: unknown[] };
  return Array.isArray(values) && values.length === 1 && values[0] === null;
}
