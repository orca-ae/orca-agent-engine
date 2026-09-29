// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Ajv2020 } from 'ajv/dist/2020.js';
import type { ErrorObject, ValidateFunction } from 'ajv';

interface OpenApiDocument {
  components?: { schemas?: Record<string, unknown> };
}

interface CompilerOptions {
  allowAdditionalProperties: boolean;
}

const COMPONENT_REF = '#/components/schemas/';
const SCHEMA_ROOT = 'https://orca.invalid/openapi-response-components';

/**
 * Compile OpenAPI response schemas with JSON Schema semantics.
 *
 * AJV resolves refs and composition. This adapter only bridges OpenAPI's
 * `nullable` extension and its 3.0 spelling of exclusive numeric bounds. For a
 * directional compatibility check, `additionalProperties: false` can be
 * dropped so Orca may add response fields while still satisfying every field
 * Anthropic defines.
 */
export function createResponseSchemaCompiler(
  document: OpenApiDocument,
  options: CompilerOptions,
): (schema: unknown) => ValidateFunction<unknown> {
  const transform = (node: unknown): unknown => {
    if (Array.isArray(node)) return node.map(transform);
    if (!node || typeof node !== 'object') return node;

    const source = node as Record<string, unknown>;
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(source)) {
      if (key === 'nullable') continue;
      if (
        options.allowAdditionalProperties &&
        (key === 'additionalProperties' || key === 'unevaluatedProperties') &&
        value === false
      ) {
        continue;
      }
      if (
        (key === 'exclusiveMinimum' || key === 'exclusiveMaximum') &&
        typeof value === 'boolean'
      ) {
        continue;
      }
      if (
        (key === 'minimum' && source.exclusiveMinimum === true) ||
        (key === 'maximum' && source.exclusiveMaximum === true)
      ) {
        continue;
      }
      if (key === '$ref' && typeof value === 'string' && value.startsWith(COMPONENT_REF)) {
        result[key] = `${SCHEMA_ROOT}#/$defs/${value.slice(COMPONENT_REF.length)}`;
        continue;
      }
      result[key] = transform(value);
    }

    if (source.exclusiveMinimum === true && typeof source.minimum === 'number') {
      result.exclusiveMinimum = source.minimum;
    }
    if (source.exclusiveMaximum === true && typeof source.maximum === 'number') {
      result.exclusiveMaximum = source.maximum;
    }

    return source.nullable === true ? { anyOf: [result, { type: 'null' }] } : result;
  };

  const definitions = Object.fromEntries(
    Object.entries(document.components?.schemas ?? {}).map(([name, schema]) => [
      name,
      transform(schema),
    ]),
  );
  const ajv = new Ajv2020({
    strict: false,
    allErrors: true,
    validateFormats: false,
    logger: false,
  });
  ajv.addSchema({
    $id: SCHEMA_ROOT,
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    $defs: definitions,
  });

  return (schema: unknown) => ajv.compile(transform(schema) as object);
}

export function describeSchemaErrors(errors: ErrorObject[] | null | undefined): string {
  return (errors ?? [])
    .slice(0, 8)
    .map((error) => `${error.instancePath || '/'} ${error.message ?? error.keyword}`)
    .join('; ');
}
