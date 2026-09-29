// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { describe, expect, it, vi } from 'vitest';
import { buildCustomToolsMcpServer } from '../src/providers/custom-tools.js';
import type { CustomToolDefinition, CustomToolResultPayload } from '../src/providers/types.js';

const complexTool: CustomToolDefinition = {
  name: 'inspect_payload',
  description: 'Inspect a structured payload.',
  input_schema: {
    type: 'object',
    properties: {
      required_text: { type: 'string' },
      optional_count: { type: 'number' },
      nested: {
        type: 'object',
        properties: {
          enabled: { type: 'boolean' },
          tags: { type: 'array', items: { type: 'string' } },
        },
        required: ['enabled', 'tags'],
      },
      mode: { enum: ['fast', 'safe'] },
      selector: { oneOf: [{ type: 'string' }, { type: 'number' }] },
    },
    required: ['required_text', 'nested', 'mode', 'selector'],
  },
};

async function connectCustomToolClient(
  requestCustomToolUse: (
    name: string,
    input: Record<string, unknown>,
  ) => Promise<CustomToolResultPayload>,
) {
  const config = buildCustomToolsMcpServer([complexTool], requestCustomToolUse);
  const client = new Client({ name: 'custom-tools-zod4-test', version: '1.0.0' });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();

  await Promise.all([config.instance.connect(serverTransport), client.connect(clientTransport)]);

  return {
    client,
    async close() {
      await client.close();
      await config.instance.close();
    },
  };
}

function successfulResult(): Promise<CustomToolResultPayload> {
  return Promise.resolve({
    custom_tool_use_id: 'tool_use_1',
    result: { accepted: true },
  });
}

describe('custom tool Zod v4 SDK integration', () => {
  it('publishes required, optional, nested object, array, enum, and union schemas', async () => {
    const connection = await connectCustomToolClient(successfulResult);

    try {
      const listed = await connection.client.listTools();
      const sdkTool = listed.tools.find((candidate) => candidate.name === complexTool.name);

      expect(sdkTool?.inputSchema).toMatchObject({
        type: 'object',
        properties: {
          required_text: { type: 'string' },
          optional_count: { type: 'number' },
          nested: {
            type: 'object',
            properties: {
              enabled: { type: 'boolean' },
              tags: { type: 'array', items: { type: 'string' } },
            },
            required: ['enabled', 'tags'],
          },
          mode: {
            anyOf: [
              { type: 'string', const: 'fast' },
              { type: 'string', const: 'safe' },
            ],
          },
          selector: {
            anyOf: [{ type: 'string' }, { type: 'number' }],
          },
        },
        required: ['required_text', 'nested', 'mode', 'selector'],
      });
      expect(sdkTool?.inputSchema.required).not.toContain('optional_count');
    } finally {
      await connection.close();
    }
  });

  it('accepts omitted optional fields and validates nested objects and arrays', async () => {
    const requestCustomToolUse = vi.fn(successfulResult);
    const connection = await connectCustomToolClient(requestCustomToolUse);
    const validInput = {
      required_text: 'payload',
      nested: { enabled: true, tags: ['one', 'two'] },
      mode: 'fast',
      selector: 7,
    };

    try {
      await expect(
        connection.client.callTool({ name: complexTool.name, arguments: validInput }),
      ).resolves.toMatchObject({ isError: false });
      expect(requestCustomToolUse).toHaveBeenCalledWith(complexTool.name, validInput);

      const { required_text: _requiredText, ...missingRequired } = validInput;
      await expect(
        connection.client.callTool({ name: complexTool.name, arguments: missingRequired }),
      ).resolves.toMatchObject({ isError: true });
      await expect(
        connection.client.callTool({
          name: complexTool.name,
          arguments: {
            ...validInput,
            nested: { enabled: true, tags: [123] },
          },
        }),
      ).resolves.toMatchObject({ isError: true });
      expect(requestCustomToolUse).toHaveBeenCalledTimes(1);
    } finally {
      await connection.close();
    }
  });

  it('validates enum and union variants through the real SDK server', async () => {
    const requestCustomToolUse = vi.fn(successfulResult);
    const connection = await connectCustomToolClient(requestCustomToolUse);
    const baseInput = {
      required_text: 'payload',
      nested: { enabled: true, tags: ['one'] },
    };

    try {
      await expect(
        connection.client.callTool({
          name: complexTool.name,
          arguments: { ...baseInput, mode: 'fast', selector: 7 },
        }),
      ).resolves.toMatchObject({ isError: false });
      await expect(
        connection.client.callTool({
          name: complexTool.name,
          arguments: { ...baseInput, mode: 'safe', selector: 'ticket-1' },
        }),
      ).resolves.toMatchObject({ isError: false });
      await expect(
        connection.client.callTool({
          name: complexTool.name,
          arguments: { ...baseInput, mode: 'slow', selector: 7 },
        }),
      ).resolves.toMatchObject({ isError: true });
      await expect(
        connection.client.callTool({
          name: complexTool.name,
          arguments: { ...baseInput, mode: 'fast', selector: false },
        }),
      ).resolves.toMatchObject({ isError: true });
      expect(requestCustomToolUse).toHaveBeenCalledTimes(2);
    } finally {
      await connection.close();
    }
  });
});
