// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { z, type ZodTypeAny } from 'zod';
import {
  createSdkMcpServer,
  tool,
  type McpSdkServerConfigWithInstance,
  type SdkMcpToolDefinition,
} from '@anthropic-ai/claude-agent-sdk';
import type { CustomToolDefinition, CustomToolResultPayload } from './types.js';
import { DEFAULT_READ_LIMIT_BYTES, MAX_READ_LIMIT_BYTES, readUtf8FilePage } from './read-page.js';
import type { SandboxWritePolicy } from '../write-policy.js';

export const ORCA_MCP_SERVER_NAME = 'orca';

export type CustomToolRequestHandler = (
  name: string,
  input: Record<string, unknown>,
) => Promise<CustomToolResultPayload>;

interface CustomToolCallResult {
  content: Array<{ type: 'text'; text: string }>;
  isError: boolean;
  structuredContent?: Record<string, unknown>;
  [key: string]: unknown;
}

export function buildCustomToolsMcpServer(
  customTools: readonly CustomToolDefinition[],
  requestCustomToolUse: CustomToolRequestHandler,
  localToolNames: readonly string[] = [],
  readPolicy?: SandboxWritePolicy,
): McpSdkServerConfigWithInstance {
  return createSdkMcpServer({
    name: ORCA_MCP_SERVER_NAME,
    version: '1.0.0',
    tools: [
      ...buildLocalSdkTools(localToolNames, readPolicy),
      ...buildCustomSdkTools(
        customTools.filter((customTool) => !localToolNames.includes(customTool.name)),
        requestCustomToolUse,
      ),
    ],
  });
}

export function customToolAllowedToolNames(customTools: readonly CustomToolDefinition[]): string[] {
  return customTools.map((customTool) => `mcp__${ORCA_MCP_SERVER_NAME}__${customTool.name}`);
}

export const LOCAL_ORCA_READ_TOOL_NAME = 'read';
export const LOCAL_ORCA_READ_QUALIFIED_NAME = `mcp__${ORCA_MCP_SERVER_NAME}__${LOCAL_ORCA_READ_TOOL_NAME}`;

function buildLocalSdkTools(
  localToolNames: readonly string[],
  readPolicy: SandboxWritePolicy | undefined,
): Array<SdkMcpToolDefinition> {
  if (!localToolNames.includes(LOCAL_ORCA_READ_TOOL_NAME)) return [];
  if (!readPolicy) {
    throw new Error('local Orca read requires a sandbox filesystem policy');
  }
  return [
    tool(
      LOCAL_ORCA_READ_TOOL_NAME,
      'Read a bounded UTF-8 page from a per-session sandbox file. offset and limit are UTF-8 ' +
        'byte positions; follow next_offset while truncation is true.',
      {
        path: z.string().describe('Absolute path inside the sandbox, e.g. `/mnt/lorem.txt`.'),
        offset: z
          .number()
          .int()
          .nonnegative()
          .optional()
          .describe('Optional UTF-8 byte offset. Defaults to 0.'),
        limit: z
          .number()
          .int()
          .positive()
          .max(MAX_READ_LIMIT_BYTES)
          .optional()
          .describe(
            `Optional maximum UTF-8 bytes to return. Defaults to ${DEFAULT_READ_LIMIT_BYTES}.`,
          ),
      },
      async ({ path, offset, limit }) => {
        try {
          const page = await readUtf8FilePage(path, readPolicy, {
            ...(offset !== undefined ? { offset } : {}),
            ...(limit !== undefined ? { limit } : {}),
          });
          return {
            content: [{ type: 'text', text: page.content }],
            isError: false,
          };
        } catch (error) {
          return {
            content: [{ type: 'text', text: `read failed: ${errorMessage(error)}` }],
            isError: true,
          };
        }
      },
      { alwaysLoad: true },
    ),
  ];
}

function buildCustomSdkTools(
  customTools: readonly CustomToolDefinition[],
  requestCustomToolUse: CustomToolRequestHandler,
): Array<SdkMcpToolDefinition> {
  // Keep the custom-tool schema/result helpers below in sync with
  // services/harness-server/src/harness/claude/mcp-tools.ts. This package stays
  // dependency-light for in-sandbox use, so the pure conversion code is copied.
  return customTools.map((customTool) =>
    tool(
      customTool.name,
      customTool.description ?? `Call custom tool ${customTool.name}.`,
      customToolInputShape(customTool.input_schema),
      async (input: unknown) => {
        const args = input && typeof input === 'object' ? (input as Record<string, unknown>) : {};
        const result = await requestCustomToolUse(customTool.name, args);
        return customToolResultToCallToolResult(result);
      },
      { alwaysLoad: true },
    ),
  );
}

function customToolInputShape(
  schema: Record<string, unknown> | undefined,
): Record<string, ZodTypeAny> {
  if (!schema || typeof schema !== 'object') return {};
  if (schema.type !== undefined && schema.type !== 'object') {
    return { input: jsonSchemaToZod(schema) };
  }
  const properties =
    schema.properties && typeof schema.properties === 'object' && !Array.isArray(schema.properties)
      ? (schema.properties as Record<string, unknown>)
      : {};
  const required = new Set(
    Array.isArray(schema.required)
      ? schema.required.filter((name): name is string => typeof name === 'string')
      : [],
  );
  const shape: Record<string, ZodTypeAny> = {};
  for (const [name, propertySchema] of Object.entries(properties)) {
    const field = jsonSchemaToZod(propertySchema);
    shape[name] = required.has(name) ? field : field.optional();
  }
  return shape;
}

function jsonSchemaToZod(schema: unknown): ZodTypeAny {
  if (!schema || typeof schema !== 'object') return z.unknown();
  const s = schema as Record<string, unknown>;
  let out: ZodTypeAny;
  if (Array.isArray(s.enum) && s.enum.length > 0) {
    if (s.enum.some((value) => !isPrimitiveJsonLiteral(value))) {
      out = z.unknown();
    } else {
      const literals = s.enum.map((value) => z.literal(value));
      out =
        literals.length === 1
          ? literals[0]!
          : (z.union(
              literals as [
                (typeof literals)[number],
                (typeof literals)[number],
                ...typeof literals,
              ],
            ) as ZodTypeAny);
    }
  } else if (Array.isArray(s.oneOf) && s.oneOf.length > 0) {
    out = jsonSchemaUnion(s.oneOf);
  } else if (Array.isArray(s.anyOf) && s.anyOf.length > 0) {
    out = jsonSchemaUnion(s.anyOf);
  } else {
    const type = Array.isArray(s.type) ? s.type[0] : s.type;
    switch (type) {
      case 'string':
        out = z.string();
        break;
      case 'integer':
        out = z.number().int();
        break;
      case 'number':
        out = z.number();
        break;
      case 'boolean':
        out = z.boolean();
        break;
      case 'array':
        out = z.array(jsonSchemaToZod(s.items));
        break;
      case 'object':
        out = z.object(customToolInputShape(s)).passthrough();
        break;
      case 'null':
        out = z.null();
        break;
      default:
        out = z.unknown();
        break;
    }
  }
  return typeof s.description === 'string' ? out.describe(s.description) : out;
}

type PrimitiveJsonLiteral = string | number | boolean | null;

function isPrimitiveJsonLiteral(value: unknown): value is PrimitiveJsonLiteral {
  return value === null || ['string', 'number', 'boolean'].includes(typeof value);
}

function jsonSchemaUnion(schemas: unknown[]): ZodTypeAny {
  const variants = schemas.map((variant) => jsonSchemaToZod(variant));
  if (variants.length === 1) return variants[0]!;
  return z.union(
    variants as [(typeof variants)[number], (typeof variants)[number], ...typeof variants],
  ) as ZodTypeAny;
}

function customToolResultToCallToolResult(result: CustomToolResultPayload): CustomToolCallResult {
  const isError = customToolResultIsError(result);
  const content = customToolContentBlocks(result);
  const out: CustomToolCallResult = { content, isError };
  if (
    !isError &&
    result.result &&
    typeof result.result === 'object' &&
    !Array.isArray(result.result)
  ) {
    out.structuredContent = result.result as Record<string, unknown>;
  }
  return out;
}

function customToolResultIsError(result: CustomToolResultPayload): boolean {
  if (result.is_error === true) return true;
  if (result.result && typeof result.result === 'object' && !Array.isArray(result.result)) {
    return typeof (result.result as { error?: unknown }).error === 'string';
  }
  return false;
}

function customToolContentBlocks(
  result: CustomToolResultPayload,
): Array<{ type: 'text'; text: string }> {
  if (Array.isArray(result.content)) {
    const textBlocks = result.content.flatMap((block) => {
      if (
        block &&
        typeof block === 'object' &&
        (block as { type?: unknown }).type === 'text' &&
        typeof (block as { text?: unknown }).text === 'string'
      ) {
        return [{ type: 'text' as const, text: (block as { text: string }).text }];
      }
      return [{ type: 'text' as const, text: stringifyToolContent(block) }];
    });
    if (textBlocks.length > 0) return textBlocks;
  }
  if (typeof result.content === 'string') return [{ type: 'text', text: result.content }];
  if (result.result !== undefined)
    return [{ type: 'text', text: stringifyToolContent(result.result) }];
  return [{ type: 'text', text: '' }];
}

function stringifyToolContent(value: unknown): string {
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
