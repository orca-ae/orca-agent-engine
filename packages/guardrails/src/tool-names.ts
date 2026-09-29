// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The tool vocabulary guardrails are written against.
 *
 * A guardrail is a list of tool names plus a verdict. If a preset names a tool
 * that no session ever calls, the guardrail still compiles, still evaluates,
 * and simply never fires — a security control that is silently absent. The
 * presets below therefore live in one place, next to tests that pin them to the
 * literal strings tools carry at call time.
 *
 * Two naming shapes are in play, and a preset that covers only one of them is
 * exactly the silent no-op described above:
 *
 *  - Direct names (`Bash`, `Read`, ...) for tools the session runs in-process.
 *  - Server-qualified keys (`mcp__orca__bash`, ...) for the same capabilities
 *    when they are served over MCP. `orca` is this runtime's own server.
 *
 * Which shape a given capability arrives under depends on how the session was
 * placed, and a guardrail author does not — and should not have to — know that.
 * Every preset therefore lists both.
 */

/** Filesystem read tools. */
export const OS_READ_TOOLS: readonly string[] = [
  'Read',
  'Glob',
  'Grep',
  'mcp__orca__read',
  'mcp__orca__glob',
  'mcp__orca__grep',
  'mcp__orca__list',
] as const;

/** Filesystem mutation tools. */
export const OS_WRITE_TOOLS: readonly string[] = [
  'Write',
  'Edit',
  'NotebookEdit',
  'mcp__orca__write',
  'mcp__orca__edit',
  'mcp__orca__delete',
] as const;

/** Shell execution tools. */
export const OS_SHELL_TOOLS: readonly string[] = ['Bash', 'mcp__orca__bash'] as const;

/** Union of the three — everything that touches the machine. */
export const OS_TOOLS: readonly string[] = [
  ...new Set([...OS_READ_TOOLS, ...OS_WRITE_TOOLS, ...OS_SHELL_TOOLS]),
];

/**
 * Tools that dispatch a subagent.
 *
 * Separate from {@link OS_TOOLS}: dispatching does not itself touch the
 * machine, but it hands the turn to a context a guardrail written for the
 * parent may not have been reasoned about.
 */
export const SUBAGENT_DISPATCH_TOOLS: readonly string[] = ['Agent'] as const;

/**
 * The tool that loads a Skill into a session.
 *
 * Exported because it is also the key a caller needs to ask "would a guardrail
 * block this Skill?" outside a running turn — a runtime that stages Skills as
 * files rather than exposing a load tool has to gate them at materialization,
 * and both sides must agree on the name a `block_skills` rule keys on.
 */
export const SKILL_LOAD_TOOL = 'Skill';

const MCP_PREFIX = 'mcp__';
const MCP_SEPARATOR = '__';

/** Parse `mcp__<server>__<tool>`; returns null for a non-MCP name. */
export function parseMcpToolName(name: string): { serverName: string; toolName: string } | null {
  if (!name.startsWith(MCP_PREFIX)) return null;
  const rest = name.slice(MCP_PREFIX.length);
  // The first separator wins: server names never contain `__`, so everything
  // after it belongs to the tool. `mcp__srv__my_tool` is server `srv`, tool
  // `my_tool` — splitting on the last separator would silently rename it.
  const boundary = rest.indexOf(MCP_SEPARATOR);
  if (boundary <= 0) return null;
  const serverName = rest.slice(0, boundary);
  const toolName = rest.slice(boundary + MCP_SEPARATOR.length);
  if (toolName.length === 0) return null;
  return { serverName, toolName };
}

/** Build the canonical key. */
export function mcpToolName(serverName: string, toolName: string): string {
  return `${MCP_PREFIX}${serverName}${MCP_SEPARATOR}${toolName}`;
}

/** Wildcard key for a whole server. */
export function mcpServerWildcard(serverName: string): string {
  return mcpToolName(serverName, '*');
}

const REGEXP_METACHARACTERS = /[.*+?^${}()|[\]\\]/g;

function escapeLiteral(literal: string): string {
  return literal.replace(REGEXP_METACHARACTERS, '\\$&');
}

/**
 * Glob match used by every `tools[]` parameter: exact, `*` wildcard, and
 * `mcp__server__*`.
 *
 * The match is anchored and case sensitive. Both matter: an unanchored match
 * would let `read` cover `mcp__orca__read`, and a case-insensitive one would
 * conflate the two distinct tools `Bash` and `mcp__orca__bash` — in each case
 * widening a guardrail past what its author wrote.
 */
export function matchesToolPattern(toolName: string, pattern: string): boolean {
  if (!pattern.includes('*')) return toolName === pattern;
  const source = pattern.split('*').map(escapeLiteral).join('.*');
  return new RegExp(`^${source}$`).test(toolName);
}

/** True when any pattern matches. */
export function matchesAnyToolPattern(toolName: string, patterns: readonly string[]): boolean {
  return patterns.some((pattern) => matchesToolPattern(toolName, pattern));
}
