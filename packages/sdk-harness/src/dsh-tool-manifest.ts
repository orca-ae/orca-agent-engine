// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { ToolSchema, type Tool } from '@modelcontextprotocol/sdk/types.js';

export function parseDshToolManifest(value: unknown): Tool[] {
  if (
    !Array.isArray(value) ||
    value.length > 256 ||
    Buffer.byteLength(JSON.stringify(value)) > 1024 * 1024
  )
    throw new Error('Invalid bounded DSH tool manifest');
  const names = new Set<string>();
  return value.map((raw) => {
    if (
      !raw ||
      typeof raw !== 'object' ||
      Array.isArray(raw) ||
      Object.keys(raw).some(
        (key) => !['name', 'description', 'inputSchema', 'deferLoading'].includes(key),
      ) ||
      (raw.deferLoading !== undefined && raw.deferLoading !== true)
    )
      throw new Error('Invalid DSH tool schema');
    const tool = ToolSchema.parse(raw);
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(tool.name) || names.has(tool.name))
      throw new Error('Invalid or colliding DSH tool schema');
    names.add(tool.name);
    return { ...tool, ...(raw.deferLoading === true ? { deferLoading: true } : {}) };
  });
}

export function dshToolManifestSha256(
  tools: readonly (Tool & { deferLoading?: boolean })[],
): string {
  const schemas = tools
    .map((tool) => ({
      name: tool.name,
      description: tool.description,
      parameters: tool.inputSchema,
      ...(tool.deferLoading === true ? { deferLoading: true } : {}),
    }))
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  const canonical = JSON.stringify(schemas, (_key, item) =>
    item && typeof item === 'object' && !Array.isArray(item)
      ? Object.fromEntries(
          Object.keys(item)
            .sort()
            .map((key) => [key, item[key]]),
        )
      : item,
  );
  return createHash('sha256').update(canonical).digest('hex');
}
