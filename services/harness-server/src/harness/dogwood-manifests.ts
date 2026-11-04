// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
/** Read only the live catalog through the existing credential-free Gateway route. */
export async function readMcpTools(
  url: string,
  headers: Record<string, string>,
  overallSignal?: AbortSignal,
): Promise<Record<string, unknown>[]> {
  const signal = overallSignal
    ? AbortSignal.any([overallSignal, AbortSignal.timeout(5000)])
    : AbortSignal.timeout(5000);
  const client = new Client({ name: 'orca-dogwood-manifests', version: '0.1.0' });
  const transport = new StreamableHTTPClientTransport(new URL(url), {
    requestInit: { headers, redirect: 'error', signal },
    reconnectionOptions: {
      maxRetries: 0,
      maxReconnectionDelay: 0,
      initialReconnectionDelay: 0,
      reconnectionDelayGrowFactor: 1,
    },
  });
  try {
    await client.connect(transport as Parameters<Client['connect']>[0], { timeout: 5000, signal });
    const tools: Record<string, unknown>[] = [];
    let cursor: string | undefined;
    do {
      const page = await client.listTools(cursor ? { cursor } : undefined, {
        timeout: 5000,
        signal,
      });
      tools.push(...page.tools);
      if (tools.length > 1000) throw new Error('MCP catalog limit exceeded');
      cursor = page.nextCursor;
    } while (cursor);
    return tools;
  } finally {
    await client.close();
  }
}
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object')
    return `{${Object.entries(value)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`)
      .join(',')}}`;
  return JSON.stringify(value);
}
export function manifestDrift(
  expected: Record<string, unknown>[],
  live: Record<string, unknown>[],
): boolean {
  const fields = (rows: Record<string, unknown>[]) =>
    rows
      .map((r) => ({ name: r.name, inputSchema: r.inputSchema, outputSchema: r.outputSchema }))
      .sort((a, b) => {
        const left = String(a.name),
          right = String(b.name);
        return left < right ? -1 : left > right ? 1 : 0;
      });
  return canonical(fields(expected)) !== canonical(fields(live));
}
