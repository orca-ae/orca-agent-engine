// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { z } from 'zod';

// The credential-free callback definition on the Registry snapshot wire. Keep in
// sync with contracts/agents.contract.ts; the round-trip spec pins this boundary.
const reservedNames = new Set([
  'agent_toolset',
  'agent_toolset_20260401',
  'mcp_toolset',
  'bash',
  'read',
  'write',
  'edit',
  'list',
  'delete',
  'glob',
  'grep',
  'web_fetch',
  'web_search',
]);
export const CustomToolSchema = z
  .object({
    name: z
      .string()
      .min(1)
      .refine(
        (name) =>
          !reservedNames.has(name) &&
          !name.startsWith('mcp__') &&
          !name.startsWith('sys_terminal_'),
      ),
    description: z.string().min(1).max(4096),
    input_schema: z
      .object({
        type: z.literal('object'),
        properties: z.record(z.string(), z.unknown()).nullable().optional(),
        required: z.array(z.string()).nullable().optional(),
      })
      .passthrough(),
  })
  .strict();
export type CustomTool = z.infer<typeof CustomToolSchema>;

export function parseCustomTools(value: unknown, allowedNames: readonly string[]): CustomTool[] {
  const tools = z.array(CustomToolSchema).parse(value);
  const names = new Set<string>();
  for (const tool of tools) {
    if (names.has(tool.name) || !allowedNames.includes(tool.name))
      throw new Error(`duplicate or disallowed custom tool: ${tool.name}`);
    names.add(tool.name);
  }
  return tools;
}

// Mirrors the PUBLIC result blocks in Registry contracts/sessions.contract.ts.
// Preserve complete blocks, including document/search metadata and image sources.
const fileId = z.string().regex(/^(?:file)_[A-Za-z0-9_-]+$/);
const base64 = z
  .object({ type: z.literal('base64'), media_type: z.string(), data: z.string() })
  .strict();
const url = z.object({ type: z.literal('url'), url: z.string().url() }).strict();
const file = z.object({ type: z.literal('file'), file_id: fileId }).strict();
const text = z.object({ type: z.literal('text'), text: z.string() }).strict();
const image = z
  .object({ type: z.literal('image'), source: z.discriminatedUnion('type', [base64, url, file]) })
  .passthrough();
const document = z
  .object({
    type: z.literal('document'),
    source: z.discriminatedUnion('type', [
      base64,
      url,
      file,
      z
        .object({ type: z.literal('text'), data: z.string(), media_type: z.string().optional() })
        .passthrough(),
    ]),
    context: z.string().nullable().optional(),
    title: z.string().nullable().optional(),
  })
  .strict();
export const CustomToolResultSchema = z
  .object({
    type: z.literal('user.custom_tool_result'),
    custom_tool_use_id: z.string().min(1),
    content: z
      .array(
        z.union([
          text,
          image,
          document,
          z.object({ type: z.literal('search_result') }).passthrough(),
        ]),
      )
      .optional(),
    is_error: z.boolean().nullable().optional(),
  })
  .strict();
export type CustomToolResult = z.infer<typeof CustomToolResultSchema>;

export function parseCustomToolResult(body: Uint8Array): CustomToolResult {
  const {
    id: _id,
    processed_at: _processedAt,
    ...result
  } = CustomToolResultSchema.extend({
    id: z.string().optional(),
    processed_at: z.string().nullable().optional(),
  }).parse(JSON.parse(Buffer.from(body).toString('utf8')));
  return result;
}
