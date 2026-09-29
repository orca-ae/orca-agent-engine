// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * In-process MCP server that exposes the per-session sandbox toolset to the
 * Claude Agent SDK.
 *
 * **Why this exists.** The Claude Agent SDK 0.2.x ships built-in tools
 * (`Bash`, `Read`, `Edit`, `Write`, `Glob`, `Grep`, `WebFetch`, …) that the
 * model is happy to call. These tools execute on the harness host's
 * filesystem, in `process.cwd()`, with the harness's network access — they
 * have ZERO connection to the per-session `SandboxHandle` that
 * `LocalSandboxRuntime` (or `E2BSandboxRuntime`) materialized for the turn.
 *
 * Without this wrapper, `materializeResources` happily writes a
 * file_resource's bytes into the work-dir at `/mnt/<file>`, the SDK's `Read`
 * tool happily ignores that and reads from the harness host's own filesystem
 * instead, and the user's "summarize /mnt/lorem.txt" prompt fails with ENOENT
 * because the SDK's `Read` doesn't know about the sandbox at all.
 *
 * The fix: bind every tool the model might dispatch to the sandbox via the
 * SDK's `createSdkMcpServer({...})` API. The harness:
 *
 *   1. Builds an `McpSdkServerConfigWithInstance` named `orca` whose tools
 *      close over the session's `SandboxHandle`.
 *   2. Passes it as `Options.mcpServers = { orca: <server> }`.
 *   3. Sets `Options.tools = []` to disable every built-in. The model now
 *      has access to ONLY the `mcp__orca__*` tools, so every file/exec call
 *      lands in the sandbox.
 *
 * The naming convention `mcp__<server>__<tool>` is established by the SDK
 * (see `sdk.d.ts` line 2621 in 0.2.126). For our `orca` server the tools
 * surface as:
 *
 *   - `mcp__orca__bash`   — runs via `SandboxHandle.run({tool:'bash', …})`
 *   - `mcp__orca__read`   — `SandboxHandle.files.read`
 *   - `mcp__orca__write`  — `SandboxHandle.files.write`
 *   - `mcp__orca__edit`   — read-modify-write through `SandboxHandle.files`
 *   - `mcp__orca__list`   — `SandboxHandle.files.list`
 *   - `mcp__orca__delete` — `SandboxHandle.files.delete`
 *   - `mcp__orca__glob`   — `SandboxHandle.run({tool:'glob', …})`
 *   - `mcp__orca__grep`   — `SandboxHandle.run({tool:'grep', …})`
 *
 * `web_fetch` from the legacy `agent-toolset.ts` is **not** exposed here. It
 * runs from the harness host (NOT inside the sandbox) and is reachable through
 * the SDK's `WebFetch` built-in if a session whitelists it; the locked
 * default for managed-agents sessions is no general-purpose web access.
 *
 * **Lifecycle.** A fresh `McpSdkServerConfigWithInstance` is built per session
 * (the SandboxHandle is per-session). The SDK's `query()` keeps a reference
 * to the `instance` for the duration of the turn; teardown is handled by the
 * SDK + node GC when the turn ends. We do not need to explicitly close the
 * server — the underlying transport is in-process.
 */
import { z, type ZodTypeAny } from 'zod';
import {
  createSdkMcpServer,
  tool,
  type McpSdkServerConfigWithInstance,
  type SdkMcpToolDefinition,
} from '@anthropic-ai/claude-agent-sdk';
import type { SandboxHandle } from '../../sandbox/sandbox-runtime.js';
import { MAX_READ_LIMIT_BYTES } from '../../sandbox/read-page.js';
import {
  AGENT_READ_LIMIT_DESCRIPTION,
  agentReadError,
  readAgentPage,
} from '../../sandbox/agent-read.js';
import type { CustomToolDefinition } from '../agent-harness.js';
import { ORCA_MCP_SERVER_NAME } from '@orca/harness-catalog';

/** Truncate large tool outputs so a runaway `cat` doesn't blow the context. */
const MAX_OUTPUT = 100_000;
function truncate(s: string, max = MAX_OUTPUT): string {
  return s.length > max ? s.slice(0, max) + `\n…[truncated, total ${s.length} chars]` : s;
}

/**
 * The MCP server name. The SDK exposes tools as `mcp__<name>__<tool>`. We pin
 * `orca` because it's the brand visible in the system prompt and shows up in
 * SSE frames (`tool_use.name === 'mcp__orca__bash'`) — useful for asserting
 * "the model dispatched OUR tools, not the SDK's built-ins".
 *
 * Re-exported from `@orca/harness-catalog` rather than redeclared: the snapshot
 * composition excludes this reserved name from `allowed_mcp_server_names`, so
 * the two must be the same string by construction, not by coincidence.
 */
export { ORCA_MCP_SERVER_NAME };

interface ToolResultPayload {
  content?: unknown;
  result?: unknown;
  is_error?: boolean | null;
  [key: string]: unknown;
}

export interface CustomToolResultPayload extends ToolResultPayload {
  custom_tool_use_id: string;
}

export interface AgentToolResultPayload extends ToolResultPayload {
  tool_use_id: string;
}

export type CustomToolRequestHandler = (
  name: string,
  input: Record<string, unknown>,
) => Promise<CustomToolResultPayload>;

export type AgentToolRequestHandler = (
  name: string,
  input: Record<string, unknown>,
) => Promise<AgentToolResultPayload>;

interface ToolCallResult {
  content: Array<{ type: 'text'; text: string }>;
  isError: boolean;
  structuredContent?: Record<string, unknown>;
  [key: string]: unknown;
}

/**
 * Build the SDK MCP server for a given sandbox handle. Each tool's handler
 * closes over `sandbox` so dispatch lands in the per-session work-dir.
 *
 * The list of tool names returned by this server matches what the model can
 * see; tests assert against this list to prove the SDK saw exactly the
 * sandbox-bound surface (no built-in leakage).
 */
export function buildOrcaSdkMcpServer(
  sandbox: SandboxHandle | undefined,
  allowedLogicalNames?: readonly string[],
  customTools: readonly CustomToolDefinition[] = [],
  requestCustomToolUse?: CustomToolRequestHandler,
  requestAgentToolUse?: AgentToolRequestHandler,
): McpSdkServerConfigWithInstance {
  const hasAgentTools = Boolean(sandbox || requestAgentToolUse);
  const safeCustomTools = hasAgentTools
    ? customTools.filter((customTool) => !ORCA_MCP_TOOL_LOGICAL_NAME_SET.has(customTool.name))
    : customTools;
  const tools = [
    ...(requestAgentToolUse
      ? buildClientExecutedSdkTools(allowedLogicalNames, requestAgentToolUse)
      : sandbox
        ? buildOrcaSdkTools(sandbox, allowedLogicalNames)
        : []),
    ...buildCustomSdkTools(safeCustomTools, requestCustomToolUse),
  ];
  return createSdkMcpServer({
    name: ORCA_MCP_SERVER_NAME,
    version: '1.0.0',
    tools,
  });
}

/**
 * Build the same logical agent-tool surface as the sandbox path, but pause
 * each call until the self-hosted client answers with user.tool_result.
 */
export function buildClientExecutedSdkTools(
  allowedLogicalNames: readonly string[] | undefined,
  requestAgentToolUse: AgentToolRequestHandler,
): Array<SdkMcpToolDefinition> {
  // buildOrcaSdkTools owns the descriptions and input schemas. Its handlers
  // are replaced below before the definitions can be invoked, so no sandbox
  // operation is reachable through this schema-only placeholder.
  const schemaDefinitions = buildOrcaSdkTools({} as SandboxHandle, allowedLogicalNames);
  return schemaDefinitions.map((definition) => ({
    ...definition,
    handler: async (input: unknown) => {
      const args = input && typeof input === 'object' ? (input as Record<string, unknown>) : {};
      const result = await requestAgentToolUse(definition.name, args);
      return toolResultToCallToolResult(result);
    },
  }));
}

/**
 * Internal helper exported for unit tests so we can drive each tool's handler
 * without standing up a full SDK MCP server. Returns the tool definitions in
 * a stable order (alphabetical by name) so name-based lookups in tests are
 * deterministic.
 */
export function buildOrcaSdkTools(
  sandbox: SandboxHandle,
  allowedLogicalNames?: readonly string[],
): Array<SdkMcpToolDefinition> {
  const allowed = allowedLogicalNames ? new Set(allowedLogicalNames) : null;
  const tools = [
    /**
     * `bash` — executes a shell command inside the sandbox.
     *
     * On `LocalSandboxRuntime` this command is wrapped by
     * `SandboxManager.wrapWithSandbox` so `srt` enforces the OS-level
     * filesystem + network allow-lists. A read of `/etc/shadow` or a write
     * outside the work-dir is rejected by the kernel, not by Node code in the
     * harness.
     */
    tool(
      'bash',
      'Run a shell command in the per-session sandbox. Returns stdout/stderr/exit_code. ' +
        'The sandbox cwd is the per-session work-dir; reads outside the allow-list are blocked by srt.',
      {
        command: z.string().describe('Shell command to execute (e.g. `pwd && ls /tmp`).'),
        timeout_ms: z
          .number()
          .int()
          .positive()
          .optional()
          .describe('Optional timeout in milliseconds; SIGKILL after expiry.'),
      },
      async ({ command, timeout_ms }) => {
        const args: { command: string; timeout_ms?: number } = { command };
        if (timeout_ms !== undefined) args.timeout_ms = timeout_ms;
        const result = await sandbox.run({ tool: 'bash', args });
        const out =
          (result.stdout ?? '') +
          (result.stderr ? `\n[stderr]\n${result.stderr}` : '') +
          `\n[exit_code] ${result.exit_code ?? 0}`;
        return {
          content: [{ type: 'text', text: truncate(out) }],
          isError: (result.exit_code ?? 0) !== 0,
        };
      },
    ),

    /**
     * `read` — read a file from the sandbox FS. Path is interpreted relative
     * to the per-session work-dir; absolute paths re-anchor under it (e.g.
     * `/mnt/lorem.txt` resolves to `<work-dir>/mnt/lorem.txt` — this matches
     * how `materializeResources` lays bytes down for file_resources).
     */
    tool(
      'read',
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
          .describe(AGENT_READ_LIMIT_DESCRIPTION),
      },
      async ({ path, offset, limit }) => {
        try {
          return await readAgentPage(
            sandbox.files,
            path,
            {
              ...(offset !== undefined ? { offset } : {}),
              ...(limit !== undefined ? { limit } : {}),
            },
            (page) => ({
              content: [{ type: 'text' as const, text: page.content }],
              isError: false,
            }),
          );
        } catch (e) {
          return {
            content: [{ type: 'text', text: agentReadError(e) }],
            isError: true,
          };
        }
      },
    ),

    /**
     * `write` — write a file to the sandbox FS, creating parent dirs as
     * needed. Used for memory_store mounts: a write to `/mnt/mem/foo.txt`
     * lands in the local memory mount root and the version-watcher picks it
     * up on the next poll.
     */
    tool(
      'write',
      'Write a file in the per-session sandbox; creates parent dirs; overwrites existing.',
      {
        path: z.string().describe('Absolute path inside the sandbox.'),
        content: z.string().describe('UTF-8 content to write.'),
      },
      async ({ path, content }) => {
        try {
          await sandbox.files.write(path, Buffer.from(content, 'utf8'));
          return {
            content: [{ type: 'text', text: `wrote ${path} (${content.length} bytes)` }],
            isError: false,
          };
        } catch (e) {
          return {
            content: [{ type: 'text', text: `write failed: ${(e as Error).message}` }],
            isError: true,
          };
        }
      },
    ),

    /**
     * `edit` — find-and-replace within an existing file. Replaces ALL
     * occurrences (not just the first) — matches the legacy
     * `agent-toolset.ts` semantics. A no-match returns a plain content
     * message rather than `isError`, so the model can plan a follow-up.
     */
    tool(
      'edit',
      'Find-and-replace within a file. Replaces ALL occurrences. Returns the count.',
      {
        path: z.string().describe('Absolute path inside the sandbox.'),
        find: z.string().describe('Literal pattern to find.'),
        replace: z.string().describe('Literal replacement.'),
      },
      async ({ path, find, replace }) => {
        try {
          const page = await sandbox.files.readUtf8Page(path, {});
          if (page.metadata.truncation) {
            return {
              content: [
                {
                  type: 'text',
                  text: `edit failed: ${path} exceeds the ${MAX_READ_LIMIT_BYTES}-byte edit limit`,
                },
              ],
              isError: true,
            };
          }
          const original = page.content;
          const updated = original.split(find).join(replace);
          if (updated === original) {
            return {
              content: [{ type: 'text', text: `no occurrences of pattern in ${path}` }],
              isError: false,
            };
          }
          await sandbox.files.write(path, Buffer.from(updated, 'utf8'));
          // count = number of times the find string appeared in the original
          // (split-join produces N+1 parts when there are N occurrences).
          const count = original.split(find).length - 1;
          return {
            content: [{ type: 'text', text: `replaced ${count} occurrence(s) in ${path}` }],
            isError: false,
          };
        } catch (e) {
          return {
            content: [{ type: 'text', text: `edit failed: ${(e as Error).message}` }],
            isError: true,
          };
        }
      },
    ),

    /**
     * `list` — list immediate entries (one level deep) under a directory.
     * Matches `SandboxFiles.list` semantics. Returns plain newline-separated
     * names so the model gets a readable text block.
     */
    tool(
      'list',
      'List entries one level deep under a directory inside the sandbox.',
      { path: z.string().describe('Absolute directory path inside the sandbox.') },
      async ({ path }) => {
        try {
          const entries = await sandbox.files.list(path);
          return {
            content: [{ type: 'text', text: entries.length ? entries.join('\n') : '(empty)' }],
            isError: false,
          };
        } catch (e) {
          return {
            content: [{ type: 'text', text: `list failed: ${(e as Error).message}` }],
            isError: true,
          };
        }
      },
    ),

    /**
     * `delete` — remove a file or directory tree. Idempotent (matches
     * `rm -rf` semantics: a missing path is not an error).
     */
    tool(
      'delete',
      'Delete a file or directory (recursive, idempotent) inside the sandbox.',
      { path: z.string().describe('Absolute path inside the sandbox.') },
      async ({ path }) => {
        try {
          await sandbox.files.delete(path);
          return { content: [{ type: 'text', text: `deleted ${path}` }], isError: false };
        } catch (e) {
          return {
            content: [{ type: 'text', text: `delete failed: ${(e as Error).message}` }],
            isError: true,
          };
        }
      },
    ),

    /**
     * `glob` — list paths matching a shell glob, dispatched via
     * `SandboxHandle.run({tool:'glob'})`. Behavior is bit-identical to the
     * legacy `agent-toolset.ts` glob tool because both wrap the same sandbox
     * primitive.
     */
    tool(
      'glob',
      'List paths matching a shell glob pattern under the optional root directory.',
      {
        pattern: z.string().describe('Shell glob pattern, e.g. `*.txt`.'),
        root: z.string().optional().describe('Optional root directory; defaults to /.'),
      },
      async ({ pattern, root }) => {
        try {
          const args: { pattern: string; root?: string } = { pattern };
          if (root !== undefined) args.root = root;
          const result = await sandbox.run({ tool: 'glob', args });
          const matches = (result.output as string[] | undefined) ?? [];
          return {
            content: [{ type: 'text', text: matches.join('\n') || '(no matches)' }],
            isError: false,
          };
        } catch (e) {
          return {
            content: [{ type: 'text', text: `glob failed: ${(e as Error).message}` }],
            isError: true,
          };
        }
      },
    ),

    /**
     * `grep` — recursive regex search. Same dispatch path as `glob`. Output
     * is the raw `grep -rn` text, truncated.
     */
    tool(
      'grep',
      'Recursively search for a regex pattern under the optional root directory.',
      {
        pattern: z.string().describe('Regex pattern.'),
        root: z.string().optional().describe('Optional root directory; defaults to /.'),
      },
      async ({ pattern, root }) => {
        try {
          const args: { pattern: string; root?: string } = { pattern };
          if (root !== undefined) args.root = root;
          const result = await sandbox.run({ tool: 'grep', args });
          const text = typeof result.output === 'string' ? result.output : '';
          return {
            content: [{ type: 'text', text: truncate(text) || '(no matches)' }],
            isError: false,
          };
        } catch (e) {
          return {
            content: [{ type: 'text', text: `grep failed: ${(e as Error).message}` }],
            isError: true,
          };
        }
      },
    ),
  ];
  return (allowed
    ? tools.filter((t) => allowed.has(t.name))
    : tools) as unknown as Array<SdkMcpToolDefinition>;
}

export function buildCustomSdkTools(
  customTools: readonly CustomToolDefinition[] = [],
  requestCustomToolUse?: CustomToolRequestHandler,
): Array<SdkMcpToolDefinition> {
  // Keep the custom-tool schema/result helpers below in sync with
  // services/sandbox-harness/src/providers/custom-tools.ts. The sandbox harness
  // intentionally avoids importing harness-server at runtime.
  if (!requestCustomToolUse) return [];
  return customTools.map((customTool) =>
    tool(
      customTool.name,
      customTool.description ?? `Call custom tool ${customTool.name}.`,
      customToolInputShape(customTool.input_schema),
      async (input: unknown) => {
        const args = input && typeof input === 'object' ? (input as Record<string, unknown>) : {};
        const result = await requestCustomToolUse(customTool.name, args);
        return toolResultToCallToolResult(result);
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

function toolResultToCallToolResult(result: ToolResultPayload): ToolCallResult {
  const isError = customToolResultIsError(result);
  const content = customToolContentBlocks(result);
  const out: ToolCallResult = { content, isError };
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

function customToolResultIsError(result: ToolResultPayload): boolean {
  if (result.is_error === true) return true;
  if (result.result && typeof result.result === 'object' && !Array.isArray(result.result)) {
    return typeof (result.result as { error?: unknown }).error === 'string';
  }
  return false;
}

function customToolContentBlocks(result: ToolResultPayload): Array<{ type: 'text'; text: string }> {
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

/** Logical names exposed by the per-session `orca` MCP server, before SDK qualification. */
export const ORCA_MCP_TOOL_LOGICAL_NAMES: readonly string[] = [
  'bash',
  'read',
  'write',
  'edit',
  'list',
  'delete',
  'glob',
  'grep',
] as const;

const ORCA_MCP_TOOL_LOGICAL_NAME_SET = new Set<string>(ORCA_MCP_TOOL_LOGICAL_NAMES);

/**
 * Stable list of MCP-qualified tool names exposed by `buildOrcaSdkMcpServer`.
 * The SDK's runtime prepends `mcp__<server-name>__` to each tool's logical
 * name, so callers comparing against an SSE `tool_use.name` field should use
 * THESE names. Kept as a constant array (vs. derived) so test assertions get
 * a single source of truth.
 */
export const ORCA_MCP_TOOL_NAMES: readonly string[] = ORCA_MCP_TOOL_LOGICAL_NAMES.map(
  (name) => `mcp__orca__${name}`,
);
