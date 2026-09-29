// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

import type { SandboxHandle } from './sandbox-runtime.js';
import { MAX_READ_LIMIT_BYTES } from './read-page.js';
import { AGENT_READ_LIMIT_DESCRIPTION, agentReadError, readAgentPage } from './agent-read.js';

/**
 * Tool definition shape compatible with the Claude Agent SDK's custom-tool
 * option. The exact field names are adapter-defined; we use the documented
 * `name / description / input_schema / execute` shape and cast to the SDK's
 * `tools[]` parameter at registration time.
 */
export interface ToolDefinition {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
  execute: (input: unknown) => Promise<ToolExecutionResult>;
}

export interface ToolExecutionResult {
  /** Stringified content the agent sees as the tool result. */
  content?: string;
  /** Optional structured output (for tools that return JSON). */
  output?: unknown;
  /** Set if the tool failed; non-empty `error` triggers an error tool result. */
  error?: string;
}

const truncate = (s: string, max = 100_000): string =>
  s.length > max ? s.slice(0, max) + `\n…[truncated, total ${s.length} chars]` : s;

/**
 * Build the agent_toolset for a given session sandbox. Returns the 9 tool
 * definitions the SDK should register: bash / read / write / edit / glob /
 * grep / list / delete / web_fetch.
 *
 * - bash/read/write/edit/glob/grep/list/delete dispatch through SandboxHandle.
 * - web_fetch runs OUTSIDE the sandbox (harness host's outbound). The
 *   ai-gateway covers MCP egress; web_fetch fetches directly from the host.
 */
export function buildAgentToolset(sandbox: SandboxHandle): ToolDefinition[] {
  return [
    {
      name: 'bash',
      description: 'Run a shell command in the sandbox.',
      input_schema: {
        type: 'object',
        properties: {
          command: { type: 'string', description: 'Shell command to run.' },
          timeout_ms: { type: 'number', description: 'Optional timeout in ms.' },
        },
        required: ['command'],
        additionalProperties: false,
      },
      execute: async (input) => {
        const args = input as { command: string; timeout_ms?: number };
        const result = await sandbox.run({ tool: 'bash', args });
        const out = `${result.stdout ?? ''}${result.stderr ? `\n[stderr]\n${result.stderr}` : ''}`;
        if (result.exit_code !== 0) {
          return { content: truncate(out), error: `exit code ${result.exit_code}` };
        }
        return { content: truncate(out) };
      },
    },
    {
      name: 'read',
      description:
        'Read a bounded UTF-8 page from a sandbox file. offset and limit are UTF-8 byte positions; ' +
        'follow next_offset while truncation is true.',
      input_schema: {
        type: 'object',
        properties: {
          path: { type: 'string' },
          offset: {
            type: 'integer',
            minimum: 0,
            description: 'Optional UTF-8 byte offset. Defaults to 0.',
          },
          limit: {
            type: 'integer',
            minimum: 1,
            maximum: MAX_READ_LIMIT_BYTES,
            description: AGENT_READ_LIMIT_DESCRIPTION,
          },
        },
        required: ['path'],
        additionalProperties: false,
      },
      execute: async (input) => {
        const args = input as { path: string; offset?: number; limit?: number };
        try {
          return await readAgentPage(sandbox.files, args.path, args, (page) => ({
            content: page.content,
            output: page.metadata,
          }));
        } catch (e) {
          return { error: agentReadError(e) };
        }
      },
    },
    {
      name: 'write',
      description: 'Write a file in the sandbox; creates parent dirs; overwrites.',
      input_schema: {
        type: 'object',
        properties: {
          path: { type: 'string' },
          content: { type: 'string' },
        },
        required: ['path', 'content'],
        additionalProperties: false,
      },
      execute: async (input) => {
        const args = input as { path: string; content: string };
        try {
          await sandbox.files.write(args.path, Buffer.from(args.content, 'utf8'));
          return { content: `wrote ${args.path} (${args.content.length} bytes)` };
        } catch (e) {
          return { error: `write failed: ${(e as Error).message}` };
        }
      },
    },
    {
      name: 'edit',
      description: 'Find-and-replace within a file. Replaces ALL occurrences.',
      input_schema: {
        type: 'object',
        properties: {
          path: { type: 'string' },
          find: { type: 'string' },
          replace: { type: 'string' },
        },
        required: ['path', 'find', 'replace'],
        additionalProperties: false,
      },
      execute: async (input) => {
        const args = input as { path: string; find: string; replace: string };
        try {
          const page = await sandbox.files.readUtf8Page(args.path, {});
          if (page.metadata.truncation) {
            return {
              error: `edit failed: ${args.path} exceeds the ${MAX_READ_LIMIT_BYTES}-byte edit limit`,
            };
          }
          const original = page.content;
          const updated = original.split(args.find).join(args.replace);
          if (updated === original) {
            return { content: `no occurrences of pattern in ${args.path}` };
          }
          await sandbox.files.write(args.path, Buffer.from(updated, 'utf8'));
          const count = (original.match(new RegExp(escapeRegExp(args.find), 'g')) ?? []).length;
          return { content: `replaced ${count} occurrence(s) in ${args.path}` };
        } catch (e) {
          return { error: `edit failed: ${(e as Error).message}` };
        }
      },
    },
    {
      name: 'glob',
      description: 'List paths matching a shell glob pattern under the optional root.',
      input_schema: {
        type: 'object',
        properties: {
          pattern: { type: 'string' },
          root: { type: 'string' },
        },
        required: ['pattern'],
        additionalProperties: false,
      },
      execute: async (input) => {
        const args = input as { pattern: string; root?: string };
        const result = await sandbox.run({ tool: 'glob', args });
        return { output: result.output, content: JSON.stringify(result.output ?? []) };
      },
    },
    {
      name: 'grep',
      description: 'Recursively search for a regex pattern under the optional root.',
      input_schema: {
        type: 'object',
        properties: {
          pattern: { type: 'string' },
          root: { type: 'string' },
        },
        required: ['pattern'],
        additionalProperties: false,
      },
      execute: async (input) => {
        const args = input as { pattern: string; root?: string };
        const result = await sandbox.run({ tool: 'grep', args });
        return { content: truncate((result.output as string) ?? '') };
      },
    },
    ...(['list', 'delete'] as const).map(
      (name): ToolDefinition => ({
        name,
        description:
          name === 'list'
            ? 'List entries one level deep inside a sandbox directory.'
            : 'Delete a file or directory recursively inside the sandbox; missing paths are allowed.',
        input_schema: {
          type: 'object',
          properties: { path: { type: 'string' } },
          required: ['path'],
          additionalProperties: false,
        },
        execute: async (input) => {
          const { path } = input as { path: string };
          try {
            if (name === 'list')
              return { content: (await sandbox.files.list(path)).join('\n') || '(empty)' };
            await sandbox.files.delete(path);
            return { content: `deleted ${path}` };
          } catch (error) {
            return { error: `${name} failed: ${(error as Error).message}` };
          }
        },
      }),
    ),
    {
      name: 'web_fetch',
      description:
        'Fetch the content of a URL. Runs from the harness host (NOT inside the sandbox).',
      input_schema: {
        type: 'object',
        properties: {
          url: { type: 'string' },
          method: { type: 'string', enum: ['GET', 'POST'] },
          headers: { type: 'object' },
        },
        required: ['url'],
        additionalProperties: false,
      },
      execute: async (input) => {
        const args = input as { url: string; method?: string; headers?: Record<string, string> };
        try {
          const resp = await fetch(args.url, {
            method: args.method ?? 'GET',
            headers: args.headers,
          });
          const body = await resp.text();
          if (!resp.ok) {
            return { error: `http ${resp.status}`, content: truncate(body, 10_000) };
          }
          return { content: truncate(body) };
        } catch (e) {
          return { error: `web_fetch failed: ${(e as Error).message}` };
        }
      },
    },
  ];
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
