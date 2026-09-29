// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { CallToolResultSchema } from '@modelcontextprotocol/sdk/types.js';
import { customToolResultToMcp, CustomToolResultConversionError } from '../../src/index.js';
import { customResultCases, pngBase64 } from '../support/custom-tool-results.js';

describe('public custom tool result to MCP conversion', () => {
  it.each(customResultCases)(
    'makes $name acceptable to MCP without losing its content',
    ({ content, proof, name }) => {
      const result = customToolResultToMcp({ content, is_error: true });
      expect(CallToolResultSchema.safeParse(result).success).toBe(true);
      expect(result.isError).toBe(true);
      if (name === 'inline image') {
        expect(result.content).toEqual([{ type: 'image', data: pngBase64, mimeType: 'image/png' }]);
      } else {
        expect(JSON.stringify(result.content)).toContain(proof);
        if (name !== 'text') {
          const converted = JSON.parse((result.content[0] as { text: string }).text);
          if (name === 'base64 UTF-8 document') {
            expect(converted).toEqual({
              ...content[0],
              source: {
                media_type: 'text/plain',
                type: 'text',
                original_encoding: 'base64',
                data: proof,
              },
            });
          } else expect(converted).toEqual(content[0]);
        } else expect(result.content).toEqual(content);
      }
    },
  );

  it('retains image metadata as additional text and defaults nullable is_error to false', () => {
    const result = customToolResultToMcp({
      content: [
        {
          type: 'image',
          title: 'Proof image',
          source: { type: 'base64', media_type: 'image/png', data: pngBase64 },
        },
      ],
      is_error: null,
    });
    expect(result).toEqual({
      content: [
        { type: 'image', data: pngBase64, mimeType: 'image/png' },
        { type: 'text', text: JSON.stringify({ type: 'image_metadata', title: 'Proof image' }) },
      ],
      isError: false,
    });
    expect(customToolResultToMcp({})).toEqual({ content: [], isError: false });
  });

  it.each([
    { type: 'image', source: { type: 'url', url: 'https://example.test/proof.png' } },
    { type: 'document', source: { type: 'file', file_id: 'file_123' } },
    { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: 'cGRm' } },
    { type: 'document', source: { type: 'base64', media_type: 'text/plain', data: '/w==' } },
    { type: 'image', source: { type: 'base64', media_type: 'image/svg+xml', data: 'eA==' } },
    { type: 'image', source: { type: 'base64', media_type: 'image/png', data: '!not base64!' } },
    { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'a' } },
    { type: 'image', source: { type: 'base64', media_type: 'image/png', data: '' } },
  ])('rejects unsupported or corrupt content explicitly: %j', (block) => {
    expect(() => customToolResultToMcp({ content: [block] })).toThrow(
      CustomToolResultConversionError,
    );
  });
});
