// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { createResponseSchemaCompiler } from '../helpers/openapi-response-validation.js';

const document = {
  components: {
    schemas: {
      Envelope: {
        type: 'object',
        additionalProperties: false,
        required: ['result', 'note'],
        properties: {
          result: { $ref: '#/components/schemas/Result' },
          note: { type: 'string', nullable: true },
        },
      },
      Result: {
        oneOf: [
          { $ref: '#/components/schemas/CountResult' },
          { $ref: '#/components/schemas/TextResult' },
        ],
      },
      CountResult: {
        type: 'object',
        additionalProperties: false,
        required: ['type', 'count'],
        properties: {
          type: { type: 'string', const: 'count' },
          count: { type: 'integer' },
        },
      },
      TextResult: {
        type: 'object',
        additionalProperties: false,
        required: ['type', 'text'],
        properties: {
          type: { type: 'string', const: 'text' },
          text: { type: 'string' },
        },
      },
    },
  },
};

const envelopeSchema = { $ref: '#/components/schemas/Envelope' };

describe('directional OpenAPI response validation', () => {
  const compile = createResponseSchemaCompiler(document, { allowAdditionalProperties: true });
  const validate = compile(envelopeSchema);

  it('resolves refs and composition while allowing additive response fields', () => {
    expect(
      validate({
        result: { type: 'count', count: 3, orca_nested: true },
        note: null,
        orca_top_level: true,
      }),
    ).toBe(true);
  });

  it.each([
    ['a missing nested field', { result: { type: 'count' }, note: null }],
    ['a wrong nested type', { result: { type: 'count', count: '3' }, note: null }],
    ['a wrong discriminator', { result: { type: 'other', count: 3 }, note: null }],
    ['an invalid null', { result: null, note: null }],
  ])('rejects %s', (_label, value) => {
    expect(validate(value)).toBe(false);
  });
});

describe('exact OpenAPI response validation', () => {
  const compile = createResponseSchemaCompiler(document, { allowAdditionalProperties: false });

  it('retains additionalProperties constraints for Orca wire checks', () => {
    expect(
      compile(envelopeSchema)({
        result: { type: 'text', text: 'ok' },
        note: null,
        unexpected: true,
      }),
    ).toBe(false);
  });
});
