// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * In-process MCP server that exposes the per-session sandbox toolset to the
 * Claude Agent SDK — the runner's OWN copy of the sandbox tool binding.
 *
 * **Why this exists.** The Claude Agent SDK ships built-in tools (`Bash`,
 * `Read`, `Edit`, `Write`, `Glob`, `Grep`, …) the model is happy to call. On
 * their own those tools execute on the RUNNER HOST's filesystem, in
 * `process.cwd()`, with the runner's network access — they have ZERO
 * connection to the per-session `SandboxHandle` the runner acquired for the
 * turn. That is why the runner's claude providers were LLM-only: there was no
 * bash/read/write bound to the sandbox at all.
 *
 * The fix: bind every file/exec tool the model might dispatch to the sandbox
 * via the SDK's `createSdkMcpServer({...})` API. The provider:
 *
 *   1. Builds an `McpSdkServerConfigWithInstance` named `orca` whose tools
 *      close over the session's {@link SandboxHandle}.
 *   2. Passes it as one entry of `Options.mcpServers` (merged with the
 *      snapshot's gateway MCP servers).
 *   3. Sets `Options.cwd` to the sandbox root so the SDK's own subprocess +
 *      any built-ins it runs also land inside the sandbox tree.
 *
 * The naming convention `mcp__<server>__<tool>` is established by the SDK. For
 * the `orca` server the tools surface as:
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
 * On `LocalSandboxRuntime` (a self_hosted runner) each ORCA `bash`/`glob`/`grep`
 * is wrapped by `srt` so the OS-level filesystem + network allow-lists are
 * enforced by the kernel; on `InMemorySandboxRuntime` (tests/dev) it runs in a
 * per-session tmpdir. Above this boundary the provider is sandbox-agnostic — it
 * composes against {@link SandboxHandle}, not the runtime impl.
 *
 * **Scope of this binding.** Unlike the harness-server, this provider does NOT
 * set `Options.tools = []`: the claude provider keeps the SDK's real built-in
 * tools, so the SDK's native `Bash`/`Read`/… stay AVAILABLE alongside
 * the `mcp__orca__*` tools rather than being suppressed. Only the orca tools
 * dispatch through {@link SandboxHandle} (and thus through `srt` on
 * `LocalSandboxRuntime`); the SDK's own built-ins execute on the host at the
 * `cwd` set here, un-`srt`-wrapped — anchoring them at the sandbox root is what
 * keeps their file/exec work inside the sandbox tree.
 *
 * **Lifecycle.** A fresh `McpSdkServerConfigWithInstance` is built per session
 * (the {@link SandboxHandle} is per-session). The SDK's `query()` keeps a
 * reference to the `instance` for the duration of the turn; teardown is handled
 * by the SDK + node GC when the turn ends — the underlying transport is
 * in-process, so there is nothing to explicitly close.
 */
import { z } from 'zod';
import {
  createSdkMcpServer,
  tool,
  type McpSdkServerConfigWithInstance,
  type Options,
  type SdkMcpToolDefinition,
} from '@anthropic-ai/claude-agent-sdk';
import type { SandboxHandle } from '../../sandbox/seam.js';
import type { DelegateToAgent } from '../agent-harness.js';
import { asTerminalHost, buildSysTerminalTools } from '../../tools/sys-terminal.js';

/** Truncate large tool outputs so a runaway `cat` doesn't blow the context. */
const MAX_OUTPUT = 100_000;
function truncate(s: string, max = MAX_OUTPUT): string {
  return s.length > max ? s.slice(0, max) + `\n…[truncated, total ${s.length} chars]` : s;
}

/**
 * The MCP server name. The SDK exposes tools as `mcp__<name>__<tool>`. We pin
 * `orca` because it is the brand visible in the system prompt and shows up in
 * agent events (`tool_use.name === 'mcp__orca__bash'`) — the `mcp__orca__`
 * prefix is what lets a caller tell an ORCA sandbox-bound dispatch apart from
 * the SDK's native built-ins (which stay available; see the module header on
 * why `Options.tools` is not zeroed).
 */
export const ORCA_MCP_SERVER_NAME = 'orca';

/**
 * Build the SDK MCP server for a given sandbox handle. Each tool's handler
 * closes over `sandbox` so dispatch lands in the per-session work-dir.
 *
 * The list of tool names returned by this server is the sandbox-bound surface
 * the model gains ON TOP OF the SDK's native built-ins (this provider does not
 * zero `Options.tools`); tests assert against this list to prove the `orca`
 * server carries exactly the expected `mcp__orca__*` tools and that it is merged
 * into `Options.mcpServers` alongside the gateway servers.
 *
 * DELEGATION. When `delegate` is present — the harness is a coordinator (its
 * snapshot carried a `multiagent` roster and the loop injected the seam onto the
 * start input) — the {@link DELEGATE_TO_AGENT_TOOL_NAME} tool is added to the SAME
 * `orca` server, so the model gains `mcp__orca__delegate_to_agent`. That tool is
 * the provider-side delegation surface Anthropic's thread model needs: calling it
 * invokes the delegate seam (spawn a roster subagent's thread, run its turn,
 * return its result). A single-agent harness passes no `delegate`, so the tool is
 * absent and every non-coordinator surface is byte-for-byte unchanged.
 */
export function buildOrcaSdkMcpServer(
  sandbox: SandboxHandle,
  allowedLogicalNames?: readonly string[],
  delegate?: DelegateToAgent,
): McpSdkServerConfigWithInstance {
  // The file/exec tools always bind. The Orca-superset sys_terminal_* tools are
  // ADDED to the same `orca` server when the sandbox can host interactive panes
  // (a {@link TerminalHost}, e.g. the tmux-backed handle) — so the claude provider
  // gains `mcp__orca__sys_terminal_*` for driving a REPL/pager/installer, and a
  // sandbox that cannot back panes (a cloud-only handle) simply omits them. The
  // exact same tool objects are exposed out-of-process by the native-CLI bridge.
  const tools: Array<SdkMcpToolDefinition> = [...buildOrcaSdkTools(sandbox, allowedLogicalNames)];
  const terminalHost = asTerminalHost(sandbox);
  if (terminalHost !== null) {
    tools.push(...buildSysTerminalTools(terminalHost, allowedLogicalNames));
  }
  // The delegation tool is NOT gated by `allowedLogicalNames`: the roster is a
  // coordinator-level capability the registry composed, not a per-skill sandbox
  // tool the allow-list narrows, and the model must always be able to reach it
  // when the harness is a coordinator.
  if (delegate !== undefined) {
    tools.push(buildDelegateSdkTool(delegate));
  }
  return createSdkMcpServer({
    name: ORCA_MCP_SERVER_NAME,
    version: '1.0.0',
    tools,
  });
}

/**
 * Build a delegation-only `orca` MCP server — the `mcp__orca__delegate_to_agent`
 * tool with NO sandbox-bound file/exec tools. Used when a coordinator harness has
 * NO per-session sandbox (a chat-only coordinator): the model still needs the
 * delegation tool even though there is no filesystem to bind. A sandboxed
 * coordinator instead folds the delegation tool INTO its full `orca` server via
 * {@link buildOrcaSdkMcpServer}'s `delegate` param, so the two paths converge on
 * one server named `orca` carrying the delegation tool.
 */
export function buildDelegateOnlySdkMcpServer(
  delegate: DelegateToAgent,
): McpSdkServerConfigWithInstance {
  return createSdkMcpServer({
    name: ORCA_MCP_SERVER_NAME,
    version: '1.0.0',
    tools: [buildDelegateSdkTool(delegate)],
  });
}

/** The logical name of the delegate-to-agent tool (surfaces as `mcp__orca__delegate_to_agent`). */
export const DELEGATE_TO_AGENT_TOOL_NAME = 'delegate_to_agent';

/**
 * Build the delegate-to-agent SDK tool — the provider-side delegation surface for
 * Anthropic's thread model. The coordinator model calls it with a roster
 * `agent_name` + a `prompt`; the handler invokes the {@link DelegateToAgent} seam,
 * which spawns that roster agent's subagent THREAD (a fresh harness from the roster
 * agent's snapshot, sharing the coordinator's ONE sandbox), runs the delegated
 * turn, and resolves with the subagent's final text — returned here to the model as
 * the tool result.
 *
 * A refusal the seam ENFORCES — an unknown roster agent or the concurrency cap
 * (a {@link DelegationError}, or any other throw) — is surfaced to the model as a
 * tool error (`isError: true`) rather than raised, so a bad delegation is a
 * recoverable tool result the model can react to, not a turn-killing exception.
 */
export function buildDelegateSdkTool(delegate: DelegateToAgent): SdkMcpToolDefinition {
  return tool(
    DELEGATE_TO_AGENT_TOOL_NAME,
    'Delegate a task to a roster agent. The named agent runs the prompt in its own ' +
      'session thread (its own model/system/tools, sharing this session sandbox + files) ' +
      'and its final result is returned. Use for sub-tasks a specialist roster agent handles.',
    {
      agent_name: z
        .string()
        .describe('The roster agent to delegate to (a name from this coordinator’s roster).'),
      prompt: z.string().describe('The task/prompt to hand the roster agent.'),
    },
    async ({ agent_name, prompt }) => {
      try {
        const outcome = await delegate({ agentName: agent_name, prompt });
        return {
          content: [{ type: 'text', text: outcome.result }],
          isError: false,
        };
      } catch (e) {
        // A delegation refusal (unknown roster agent / concurrency cap / build
        // failure) is a clean tool error the model can react to, not a thrown fault.
        return {
          content: [{ type: 'text', text: `delegation failed: ${(e as Error).message}` }],
          isError: true,
        };
      }
    },
  ) as unknown as SdkMcpToolDefinition;
}

/**
 * Internal helper exported for unit tests so we can drive each tool's handler
 * without standing up a full SDK MCP server. Returns the tool definitions in a
 * stable order so name-based lookups in tests are deterministic. When
 * `allowedLogicalNames` is given, the returned set is filtered to it (the
 * skill-allowlist intersection the snapshot carries).
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
     * On `LocalSandboxRuntime` the command is wrapped by `srt` so the OS-level
     * filesystem + network allow-lists are enforced by the kernel, not by Node
     * code in the runner: a read of `/etc/shadow` or a write outside the
     * work-dir is rejected by the OS.
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
     * `read` — read a file from the sandbox FS. The path is interpreted
     * relative to the per-session work-dir; absolute paths re-anchor under it
     * (e.g. `/mnt/lorem.txt` resolves to `<work-dir>/mnt/lorem.txt`).
     */
    tool(
      'read',
      'Read a file from the per-session sandbox at the given absolute path. Returns the file contents as text.',
      { path: z.string().describe('Absolute path inside the sandbox, e.g. `/mnt/lorem.txt`.') },
      async ({ path }) => {
        try {
          const buf = await sandbox.files.read(path);
          return {
            content: [{ type: 'text', text: truncate(buf.toString('utf8')) }],
            isError: false,
          };
        } catch (e) {
          return {
            content: [{ type: 'text', text: `read failed: ${(e as Error).message}` }],
            isError: true,
          };
        }
      },
    ),

    /**
     * `write` — write a file to the sandbox FS, creating parent dirs as needed
     * and overwriting an existing file.
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
     * occurrences (not just the first). A no-match returns a plain content
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
          const buf = await sandbox.files.read(path);
          const original = buf.toString('utf8');
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
     * Returns plain newline-separated names so the model gets a readable block.
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
     * `delete` — remove a file or directory tree. Idempotent (matches `rm -rf`
     * semantics: a missing path is not an error).
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
     * `SandboxHandle.run({tool:'glob'})`.
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
     * `grep` — recursive regex search. Same dispatch path as `glob`. Output is
     * the raw `grep -rn` text, truncated.
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

/**
 * Stable list of MCP-qualified tool names exposed by {@link buildOrcaSdkMcpServer}.
 * The SDK's runtime prepends `mcp__<server-name>__` to each tool's logical
 * name, so callers comparing against an agent event's `tool_use.name` field
 * should use THESE names. Kept as a constant array (vs. derived) so test
 * assertions get a single source of truth.
 */
export const ORCA_MCP_TOOL_NAMES: readonly string[] = ORCA_MCP_TOOL_LOGICAL_NAMES.map(
  (name) => `mcp__orca__${name}`,
);

/** The MCP-qualified name of the delegate-to-agent tool (present only on a coordinator). */
export const DELEGATE_TO_AGENT_MCP_TOOL_NAME = `mcp__orca__${DELEGATE_TO_AGENT_TOOL_NAME}`;

/** The gateway MCP server map shape the snapshot carries (rewritten to the ai-gateway). */
export type GatewayMcpServers = Record<
  string,
  { type: 'http'; url: string; headers: Record<string, string> }
>;

/**
 * Build the SDK `Options` fragment that gives a claude provider its real built-in
 * tools bound to the per-session sandbox: the merged `mcpServers` map (the in-process
 * `orca` sandbox tool server plus the snapshot's rewritten gateway servers) and the
 * `cwd` anchored at the sandbox root.
 *
 * Shared by BOTH claude providers (lean + persistent) so the merge is identical and in
 * one place — the same shape the harness-server composes inline:
 *
 *   - the `orca` server (built once per session from {@link buildOrcaSdkMcpServer},
 *     passed in so the provider can reuse the same instance across turns) is added
 *     first; a gateway entry that collides with the reserved `orca` name is skipped;
 *   - `strictMcpConfig` is set whenever any server is wired, so the SDK does not also
 *     read an ambient `.mcp.json` off the runner host;
 *   - `cwd` is the sandbox root, so the SDK's own subprocess + any built-ins it runs
 *     land inside the sandbox tree. The root is read via the runtime's `rootDir()`
 *     helper (present on the two reusable runtimes' handles); a handle without one
 *     (a cloud runtime) simply yields no `cwd` — the `orca` tools still bind.
 *
 * Returns only the fields it owns (`mcpServers`, `strictMcpConfig`, `cwd`), spread by
 * the caller onto its `Options`, so nothing else in the provider's option-building is
 * disturbed. With neither an orca server nor a gateway server, returns an empty
 * fragment (chat-only) so the provider's options are unchanged.
 */
export function buildSandboxSdkOptions(input: {
  orcaMcpServer?: McpSdkServerConfigWithInstance;
  sandbox?: SandboxHandle;
  gatewayMcpServers?: GatewayMcpServers;
}): Pick<Options, 'mcpServers' | 'strictMcpConfig' | 'cwd'> {
  const mergedMcpServers: Record<string, unknown> = {};
  if (input.orcaMcpServer !== undefined) {
    mergedMcpServers[ORCA_MCP_SERVER_NAME] = input.orcaMcpServer;
  }
  if (input.gatewayMcpServers !== undefined) {
    for (const [name, cfg] of Object.entries(input.gatewayMcpServers)) {
      if (name === ORCA_MCP_SERVER_NAME) {
        continue; // reserved for the in-process sandbox tool server.
      }
      mergedMcpServers[name] = cfg;
    }
  }
  const out: Pick<Options, 'mcpServers' | 'strictMcpConfig' | 'cwd'> = {};
  if (Object.keys(mergedMcpServers).length > 0) {
    out.mcpServers = mergedMcpServers as NonNullable<Options['mcpServers']>;
    out.strictMcpConfig = true;
  }
  const cwd = input.sandbox !== undefined ? sandboxRootDir(input.sandbox) : undefined;
  if (cwd !== undefined) {
    out.cwd = cwd;
  }
  return out;
}

/**
 * The sandbox's host-side work-dir root, or `undefined` when the handle does not
 * expose one. The two reusable runtimes' handles (`InMemory`, `Local`) expose a
 * `rootDir()` accessor; a cloud runtime handle may not, in which case the SDK `cwd`
 * is left unset (the `orca` tools still bind — they dispatch through the handle, not
 * `cwd`). Duck-typed rather than a hard interface method so the sandbox boundary type
 * ({@link SandboxHandle}) stays minimal and cloud-runtime-friendly.
 */
function sandboxRootDir(sandbox: SandboxHandle): string | undefined {
  const rootDir = (sandbox as { rootDir?: unknown }).rootDir;
  if (typeof rootDir !== 'function') {
    return undefined;
  }
  const value = (rootDir as () => unknown).call(sandbox);
  return typeof value === 'string' ? value : undefined;
}
