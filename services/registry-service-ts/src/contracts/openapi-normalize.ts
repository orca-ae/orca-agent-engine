// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The one place the generated document is rewritten after the fact.
 *
 * Two rules, and both are the same kind: the contract is correct and the
 * renderer emits a dialect the document does not declare. Anything that can be
 * fixed at its source is fixed there instead — a normalizer that quietly fixes
 * whatever it finds becomes the next artifact nobody can trust, because you can
 * no longer tell which parts of the published spec came from the contract and
 * which were invented on the way out. Both rules below throw or leave the
 * document unchanged rather than guess.
 */

/**
 * Keywords that describe a schema without constraining any value it accepts.
 *
 * Exported because two callers must agree on it: the nullable rule below, which
 * treats a schema carrying nothing else as already accepting null, and the
 * vacuous-schema gate in `scripts/generate-openapi.mjs`, which refuses to
 * publish one. They were separate lists for one round, and `readOnly` was in
 * neither — which is how three SSE endpoints published `{ readOnly: true }` as
 * their whole response through a gate written to catch exactly that.
 */
const ANNOTATIONS = new Set([
  'nullable',
  'description',
  'readOnly',
  'writeOnly',
  'deprecated',
  'example',
  'examples',
  'title',
  'default',
  'externalDocs',
  'xml',
]);

/** True when nothing in `schema` rules any value out. */
export function schemaConstrainsNothing(schema: unknown): boolean {
  if (!schema || typeof schema !== 'object' || Array.isArray(schema)) return true;
  return Object.keys(schema).every((key) => ANNOTATIONS.has(key));
}

/**
 * Rewrite OpenAPI 3.1's numeric `exclusiveMinimum` / `exclusiveMaximum` into the
 * boolean form 3.0 requires.
 *
 * zod's `.positive()` renders as `exclusiveMinimum: 0`, which is how 3.1 spells
 * it. We publish `openapi: 3.0.2`, where the keyword is a boolean modifier on
 * `minimum`, so a validator reads the numeric form as the wrong type and rejects
 * the document. This cannot be fixed in the contract: the schema is correct and
 * the renderer simply emits the wrong dialect.
 *
 * Mutates in place and returns the same object, since it runs over a document
 * that was just built and is not shared.
 */
export function normalizeExclusiveBounds<T>(node: T): T {
  visitBounds(node);
  return node;
}

function visitBounds(node: unknown): void {
  if (Array.isArray(node)) {
    for (const item of node) visitBounds(item);
    return;
  }
  if (!node || typeof node !== 'object') return;

  const schema = node as Record<string, unknown>;
  rewrite(schema, 'exclusiveMinimum', 'minimum');
  rewrite(schema, 'exclusiveMaximum', 'maximum');

  for (const value of Object.values(schema)) visitBounds(value);
}

function rewrite(schema: Record<string, unknown>, keyword: string, bound: string): void {
  const value = schema[keyword];
  if (typeof value !== 'number') return;
  // 3.1 states the bound as the keyword's value; 3.0 states it on `minimum` /
  // `maximum` and uses the keyword as a flag. Both express the same constraint.
  schema[bound] = value;
  schema[keyword] = true;
}

/**
 * Make every `nullable: true` actually admit `null` under OpenAPI 3.0.
 *
 * `nullable` is not a standalone keyword there: it widens a declared `type`, and
 * it widens nothing else. The renderer produces three shapes where that leaves
 * the document saying something other than `.nullable()` says in zod, and all
 * three published a schema that rejects a `null` this service accepts.
 *
 *   - **On a union.** `z.union([...]).nullable()` has branches, not a type. 3.0
 *     gives `nullable` no meaning without one and AJV refuses to compile it —
 *     the published `ModelInput` could not be compiled at all. Gains an explicit
 *     `{ enum: [null] }` branch.
 *   - **On an unconstrained value.** `z.unknown().nullable()` renders as
 *     `{ nullable: true }` and nothing else. Loses the keyword; a schema that
 *     rules nothing out already admits `null`.
 *   - **Beside an `enum`.** `z.enum([...]).nullable()` keeps the enum, and an
 *     enum that does not list `null` excludes it however `nullable` is set —
 *     22 sites, on `speed`, resource `access`, environment `scope` and `target`.
 *     Gains `null` as a member.
 *
 * The obvious repair for the first is wrong and silently so: hoisting a `type`
 * beside the `nullable` makes the keyword legal and the document valid, and the
 * schema still **rejects `null`**, because the sibling `oneOf` applies and no
 * branch matches. Structure cannot distinguish that from a fix, which is why
 * `openapi-instances.spec.ts` validates values.
 *
 * Anything else carrying `nullable` without a `type` throws — a fourth shape
 * must be decided about rather than absorbed.
 */
export function normalizeNullableForOas30<T>(node: T): T {
  visitNullable(node, '#');
  return node;
}

function visitNullable(node: unknown, path: string): void {
  if (Array.isArray(node)) {
    node.forEach((item, index) => visitNullable(item, `${path}/${String(index)}`));
    return;
  }
  if (!node || typeof node !== 'object') return;

  const schema = node as Record<string, unknown>;
  if (schema.nullable === true) rewriteNullable(schema, path);

  for (const [key, value] of Object.entries(schema)) visitNullable(value, `${path}/${key}`);
}

function rewriteNullable(schema: Record<string, unknown>, path: string): void {
  if (Array.isArray(schema.enum)) {
    if (!schema.enum.includes(null)) schema.enum.push(null);
    return;
  }

  // Everything below is about the missing `type`; with one present the keyword
  // already means what it says.
  if (schema.type !== undefined) return;

  if (Array.isArray(schema.oneOf)) {
    delete schema.nullable;
    // A branch that already accepts null makes the added one a second match, and
    // a `oneOf` demands exactly one — the very bug this rule exists to remove.
    if (!schema.oneOf.some(branchAcceptsNull)) schema.oneOf.push({ enum: [null] });
    return;
  }

  if (schemaConstrainsNothing(schema)) {
    // Nothing here rules any value out, `null` included. The keyword was only
    // ever restating that.
    delete schema.nullable;
    return;
  }

  throw new Error(
    `${path}: \`nullable: true\` with no sibling \`type\`, and neither a union nor an ` +
      `unconstrained schema (keys: ${Object.keys(schema).join(', ')}). OpenAPI 3.0 does not ` +
      'define this form; decide how it should read rather than publishing it.',
  );
}

function branchAcceptsNull(branch: unknown): boolean {
  if (!branch || typeof branch !== 'object') return false;
  const schema = branch as Record<string, unknown>;
  if (schema.nullable === true) return true;
  if (Array.isArray(schema.enum) && schema.enum.includes(null)) return true;
  return schemaConstrainsNothing(schema);
}
