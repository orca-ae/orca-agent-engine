// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { CallToolResultSchema, type CallToolResult } from '@modelcontextprotocol/sdk/types.js';

/** A valid public callback block that cannot be represented safely for native Codex. */
export class CustomToolResultConversionError extends Error {
  constructor(message: string) {
    super(`Codex custom tool result: ${message}`);
    this.name = 'CustomToolResultConversionError';
  }
}

/**
 * Public Managed Agents blocks are not MCP blocks. Convert once, in the library
 * shared by both adapters, before native Codex consumes the MCP response. This
 * is pure: no URL fetching, File-store lookup, or ambient credential access.
 */
export function customToolResultToMcp(result: {
  content?: unknown;
  is_error?: unknown;
}): CallToolResult {
  if (result.content !== undefined && !Array.isArray(result.content))
    throw new CustomToolResultConversionError('content must be an array');
  if (result.is_error != null && typeof result.is_error !== 'boolean')
    throw new CustomToolResultConversionError('is_error must be a boolean');
  const content = ((result.content ?? []) as unknown[]).flatMap(convertBlock);
  return CallToolResultSchema.parse({ content, isError: result.is_error === true });
}

type McpBlock = CallToolResult['content'][number];
const supportedImages = new Set(['image/png', 'image/jpeg', 'image/webp', 'image/gif']);

function convertBlock(value: unknown): McpBlock[] {
  const block = record(value, 'content block');
  if (block.type === 'text') {
    if (typeof block.text !== 'string')
      throw new CustomToolResultConversionError('text block requires text');
    return [{ type: 'text', text: block.text }];
  }
  if (block.type === 'search_result') return [jsonText(block)];
  if (block.type !== 'document' && block.type !== 'image')
    throw new CustomToolResultConversionError(`unsupported block type '${String(block.type)}'`);
  const source = record(block.source, `${block.type} source`);
  if (source.type === 'url' || source.type === 'file')
    throw new CustomToolResultConversionError(
      `${block.type} source '${source.type}' is not supported; supply inline text or a base64 image`,
    );
  if (block.type === 'document' && source.type === 'text') {
    if (typeof source.data !== 'string')
      throw new CustomToolResultConversionError('text document requires string data');
    // JSON text retains document content and every title/context/metadata field.
    return [jsonText(block)];
  }
  if (source.type !== 'base64')
    throw new CustomToolResultConversionError(
      `unsupported ${block.type} source '${String(source.type)}'`,
    );
  const bytes = decodeBase64(source.data);
  const mimeType = source.media_type;
  if (typeof mimeType !== 'string')
    throw new CustomToolResultConversionError('base64 content requires media_type');
  if (block.type === 'image') {
    if (!bytes.length) throw new CustomToolResultConversionError('image data is empty');
    if (!supportedImages.has(mimeType))
      throw new CustomToolResultConversionError(`unsupported image media type '${mimeType}'`);
    const blocks: McpBlock[] = [{ type: 'image', data: bytes.toString('base64'), mimeType }];
    const { type: _type, source: _source, ...metadata } = block;
    if (Object.keys(metadata).length)
      blocks.push(jsonText({ type: 'image_metadata', ...metadata }));
    return blocks;
  }
  if (!/^text\/[a-z0-9.+-]+(?:\s*;\s*charset=utf-8)?$/i.test(mimeType))
    throw new CustomToolResultConversionError(
      `unsupported document media type '${mimeType}'; supply inline text`,
    );
  let text: string;
  try {
    text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  } catch {
    throw new CustomToolResultConversionError('text document is not valid UTF-8');
  }
  return [
    jsonText({
      ...block,
      source: { ...source, type: 'text', original_encoding: 'base64', data: text },
    }),
  ];
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new CustomToolResultConversionError(`${label} must be an object`);
  return value as Record<string, unknown>;
}

function jsonText(block: Record<string, unknown>): McpBlock {
  return { type: 'text', text: JSON.stringify(block) };
}

function decodeBase64(value: unknown): Buffer {
  if (typeof value !== 'string' || !/^[A-Za-z0-9+/]*={0,2}$/.test(value))
    throw new CustomToolResultConversionError('invalid base64 content');
  const bytes = Buffer.from(value, 'base64');
  const canonical = bytes.toString('base64');
  if (value !== canonical && value !== canonical.replace(/=+$/, ''))
    throw new CustomToolResultConversionError('invalid base64 content');
  return bytes;
}
