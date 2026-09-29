// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Shared snapshot-composition primitives — the SINGLE source of truth for how an
 * (agent, skills) pair composes into the runtime tool/system/MCP selection.
 *
 * Both the harness-server Dispatcher and the Registry runner snapshot builder use
 * these primitives, including the same explicit toolset permission composition.
 *
 * The two snapshot SHAPES differ (the registry's carries an egress config + the
 * resolved provider; the harness's carries the model fields the in-process Claude
 * SDK reads), but the COMPOSITION CORE — compose the system prompt, intersect the
 * tool allowlists, canonicalize + expand the tool names, and surface the enabled
 * `mcp_toolset` servers — is identical and MUST NOT drift. It lives here so both
 * builders call exactly one implementation: the registry "owns" this composition
 * (the snapshot build was relocated into the registry), and the harness consumes
 * the same primitives rather than maintaining a parallel copy.
 *
 * Everything here is PURE: no I/O, no secrets, no environment access.
 */

/** A skill's contribution to the composition: a system-prompt fragment + an
 *  optional tool allowlist (a `null`/absent allowlist does not narrow). */
export interface SkillSlice {
  systemPrompt?: string;
  toolAllowlist?: string[] | null;
}

/**
 * The agent tool entries the composition reads over. A discriminated set in the
 * agent contract (`agent_toolset`, `agent_toolset_20260401`, `mcp_toolset`,
 * `custom`); modeled here as the open shape the builder reads (`type` plus an
 * optional `mcp_server_name` for `mcp_toolset` and an optional `name` for
 * `custom`).
 */
export interface AgentToolEntry {
  type: string;
  mcp_server_name?: string;
  [key: string]: unknown;
}

// ── Tool-name aliasing ───────────────────────────────────

const ANTHROPIC_DATED_TO_CANONICAL: Record<string, string> = {
  agent_toolset_20260401: 'agent_toolset',
};
const CANONICAL_TO_ANTHROPIC_DATED: Record<string, string> = Object.fromEntries(
  Object.entries(ANTHROPIC_DATED_TO_CANONICAL).map(([k, v]) => [v, k]),
);

/** Canonicalize a tool name (the dated `agent_toolset_20260401` alias maps to the
 *  canonical `agent_toolset`); any other name passes through unchanged. */
export function toCanonicalToolName(name: string): string {
  return ANTHROPIC_DATED_TO_CANONICAL[name] ?? name;
}

/** Map a canonical tool name back to the Anthropic wire alias when `orcaBeta` is
 *  off (the dated form); the orca-beta wire keeps the canonical name. */
export function toAnthropicWireToolName(name: string, orcaBeta: boolean): string {
  if (orcaBeta) return name;
  return CANONICAL_TO_ANTHROPIC_DATED[name] ?? name;
}

// ── System-prompt composition ────────────────────────────

/**
 * Compose the agent system prompt with each skill's system prompt, in
 * agent-declared skill order, dropping blank parts and joining with a blank line.
 */
export function composeSystemPrompt(agentSystem: string, skills: SkillSlice[]): string {
  const parts: string[] = [];
  if (agentSystem.length > 0) parts.push(agentSystem);
  for (const s of skills) {
    if (s.systemPrompt && s.systemPrompt.length > 0) parts.push(s.systemPrompt);
  }
  return parts.join('\n\n');
}

// ── Tool-allowlist intersection ──────────────────────────

/**
 * Intersect the agent's tool TOKENS with each skill's tool allowlist, cumulatively
 * (each skill narrows further). A `null`/`undefined` allowlist does not narrow.
 * The output preserves the agent's declared order. The intersection operates on
 * TOKENS (see {@link canonicalAgentToolNames}) — `agent_toolset` is kept atomic
 * here and expanded to its concrete tools only afterwards, so a skill grants or
 * denies the whole file/shell toolset by naming `agent_toolset`.
 */
export function intersectToolAllowlists(agentTools: string[], skills: SkillSlice[]): string[] {
  let allowed = new Set(agentTools);
  for (const s of skills) {
    if (s.toolAllowlist === null || s.toolAllowlist === undefined) continue;
    const narrowed = new Set<string>();
    for (const t of s.toolAllowlist) {
      if (allowed.has(t)) narrowed.add(t);
    }
    allowed = narrowed;
  }
  return agentTools.filter((t) => allowed.has(t));
}

// ── Tool-name canonicalization + expansion ───────────────

/**
 * The logical agent-toolset tools `agent_toolset` expands to. The dated
 * `agent_toolset_20260401` is canonicalized to `agent_toolset` first, so both
 * expand the same way.
 */
export const AGENT_TOOLSET_LOGICAL_TOOLS = [
  'bash',
  'read',
  'write',
  'edit',
  'glob',
  'grep',
  'list',
  'delete',
] as const;

/**
 * The intersection TOKEN for each agent tool — the vocabulary the skill
 * allowlists are expressed in.
 *   - `agent_toolset` (and its dated alias) → the canonical `agent_toolset` token
 *     (kept atomic; expansion to the logical file/shell tools happens AFTER the
 *     intersection so a skill grants/denies the whole toolset by naming it);
 *   - `mcp_toolset` → the `mcp_toolset` token (surfaced later as MCP servers);
 *   - `custom` → the tool's own `name` (a custom tool is granted/denied by name);
 *   - anything else → its canonical type, defensively (the contract only allows
 *     the four types above, but an unknown token is carried through rather than
 *     silently dropped so the intersection still constrains it).
 */
export function canonicalAgentToolNames(tools: AgentToolEntry[]): string[] {
  const tokens: string[] = [];
  for (const tool of tools) {
    const canonical = toCanonicalToolName(tool.type);
    if (canonical === 'custom') {
      const name = tool.name;
      if (typeof name === 'string' && name.length > 0) tokens.push(name);
      continue;
    }
    tokens.push(canonical);
  }
  return tokens;
}

/**
 * Expand the intersection tokens that survived into the concrete runnable tool
 * names the runner executes:
 *   - `agent_toolset` → the file/shell logical tools (bash/read/write/…);
 *   - `mcp_toolset` → dropped here (surfaced separately as MCP server names — it
 *     is not a directly-runnable tool name);
 *   - any other token (a custom tool name, or a logical file/shell tool a skill
 *     allowlist named directly) → kept verbatim.
 *
 * CUSTOM-TOOL CARRIAGE (intentional, registry-native behavior). A surviving custom
 * tool name is kept in the output. This is a DELIBERATE divergence from the
 * earlier cloud-only expansion, which filtered the output to ONLY the eight
 * file/shell logical tools and therefore silently dropped custom tool names. That
 * filter was sound only because the cloud in-process harness surfaced custom tools
 * through a separate channel and `allowed_tool_names` gated just the built-in
 * file/shell tools. The snapshot delivered to a GENERIC self-hosted runner has no
 * such side channel: dropping a declared custom tool here would silently strip a
 * capability the agent declared. Keeping it is the correct, single behavior for
 * both the relocated registry build and the harness build, which now share this
 * function.
 *
 * EMPTY OUTPUT MEANS DENY-ALL. `[]` is an ordinary result, not a degenerate one:
 * an mcp-only agent (`tools: [{ type: 'mcp_toolset', … }]`) canonicalizes to
 * `['mcp_toolset']` and expands to `[]`, because it declared no directly-runnable
 * tool. Every consumer of the resulting `allowed_tool_names` MUST read `[]` as
 * "no tool is permitted" and only an ABSENT allowlist as "no restriction" — the
 * two are one boolean apart and read oppositely, so a consumer that
 * length-guards the value hands that agent the full built-in tool surface.
 */
export function expandAllowedToolNames(toolNames: string[]): string[] {
  if (toolNames.length === 0) return [];
  const allowed = new Set<string>();
  for (const toolName of toolNames) {
    if (toCanonicalToolName(toolName) === 'agent_toolset') {
      for (const logical of AGENT_TOOLSET_LOGICAL_TOOLS) allowed.add(logical);
      continue;
    }
    if (toCanonicalToolName(toolName) === 'mcp_toolset') continue;
    allowed.add(toolName);
  }
  return [...allowed];
}

// ── Toolset configuration ────────────────────────────────

/**
 * The reserved MCP server name the runtime binds its OWN in-process tool server
 * to (`mcp__orca__bash`, …). It is not a REMOTE server: an agent that declares
 * an `mcp_toolset` named `orca` is configuring permission policies on the
 * built-in surface, not enabling an outbound MCP destination, so
 * {@link enabledMcpToolsetServerNames} excludes it.
 */
export const ORCA_MCP_SERVER_NAME = 'orca';

/** The three managed tool-permission policies the agent contract accepts. */
export type ManagedToolPermissionPolicy = 'always_allow' | 'always_ask' | 'always_deny';

export function isManagedToolPermissionPolicy(
  value: unknown,
): value is ManagedToolPermissionPolicy {
  return value === 'always_allow' || value === 'always_ask' || value === 'always_deny';
}

/**
 * Read a `permission_policy` field, which the contract accepts either as the
 * bare policy string or as an object with a `type` discriminant. Anything else
 * is `null` (no explicit policy).
 */
export function managedToolPermissionPolicyName(
  value: unknown,
): ManagedToolPermissionPolicy | null {
  if (isManagedToolPermissionPolicy(value)) return value;
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const type = (value as Record<string, unknown>)['type'];
  return isManagedToolPermissionPolicy(type) ? type : null;
}

export interface ResolvedToolsetConfig {
  enabled: boolean;
  permissionPolicy: ManagedToolPermissionPolicy;
}

/**
 * Resolve one toolset config block (`default_config`, or an entry of `configs`)
 * against a fallback — the toolset default for a per-tool entry, or the
 * platform default (`enabled`, `always_allow`) for the toolset default itself.
 */
export function resolvedToolsetConfig(
  value: Record<string, unknown>,
  fallback: ResolvedToolsetConfig = {
    enabled: true,
    permissionPolicy: 'always_allow',
  },
): ResolvedToolsetConfig {
  return {
    enabled: typeof value['enabled'] === 'boolean' ? value['enabled'] : fallback.enabled,
    permissionPolicy:
      managedToolPermissionPolicyName(value['permission_policy']) ?? fallback.permissionPolicy,
  };
}

/** Coerce a config block to a plain object (a non-object block configures nothing). */
export function toolsetConfigValue(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

/**
 * Normalize a toolset's `configs` — accepted either as an array of
 * `{ name, … }` entries or as a `{ [name]: { … } }` map — to a uniform
 * `{ name, value }` list. Malformed entries are dropped.
 */
export function toolsetConfigEntries(
  configs: unknown,
): Array<{ name: string; value: Record<string, unknown> }> {
  if (Array.isArray(configs)) {
    return configs.flatMap((config) => {
      if (!config || typeof config !== 'object' || Array.isArray(config)) return [];
      const value = config as Record<string, unknown>;
      return typeof value['name'] === 'string' ? [{ name: value['name'], value }] : [];
    });
  }
  if (!configs || typeof configs !== 'object') return [];
  return Object.entries(configs as Record<string, unknown>).flatMap(([name, value]) => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return [];
    return [{ name, value: value as Record<string, unknown> }];
  });
}

/**
 * Whether a toolset entry is enabled at all: its `default_config` is enabled, or
 * at least one per-tool `configs` entry re-enables a tool the default disabled.
 * A toolset with everything disabled contributes nothing to the runtime.
 */
export function toolsetEnabled(tool: AgentToolEntry): boolean {
  const defaultConfig = resolvedToolsetConfig(toolsetConfigValue(tool['default_config']));
  return (
    defaultConfig.enabled ||
    toolsetConfigEntries(tool['configs']).some(
      (config) => resolvedToolsetConfig(config.value, defaultConfig).enabled,
    )
  );
}

// ── Enabled MCP-toolset servers ──────────────────────────

/**
 * The REMOTE `mcp_toolset` server names enabled in the snapshot. Three filters,
 * all of which are load-bearing security controls — this is the whole gate on
 * `allowed_mcp_server_names`, and nothing downstream re-applies any of them:
 *
 *   1. the declared server name is a non-empty string;
 *   2. the toolset is ENABLED ({@link toolsetEnabled}) — a toolset whose
 *      `default_config.enabled` is `false` with no per-tool override is off, and
 *      must not reach the runtime as a permitted destination; and
 *   3. the name is not the reserved {@link ORCA_MCP_SERVER_NAME} — that names
 *      the runtime's own in-process tool server, never an outbound destination.
 *
 * `intersectedToolNames`, when supplied, adds the skill-allowlist gate: servers
 * surface ONLY when the `mcp_toolset` token itself survived the intersection, so
 * a skill that narrowed the tools to a set excluding `mcp_toolset` drops the
 * capability for the session. Omit it (the harness-server snapshot build, which
 * applies no skill intersection here) to skip that gate — filters 1-3 always run.
 */
export function enabledMcpToolsetServerNames(
  tools: AgentToolEntry[] | undefined,
  intersectedToolNames?: readonly string[],
): Set<string> {
  if (intersectedToolNames !== undefined) {
    const survived = new Set(intersectedToolNames.map((n) => toCanonicalToolName(n)));
    if (!survived.has('mcp_toolset')) return new Set();
  }
  const out = new Set<string>();
  for (const tool of tools ?? []) {
    if (toCanonicalToolName(tool.type) !== 'mcp_toolset') continue;
    const name = tool.mcp_server_name;
    if (typeof name !== 'string' || name.length === 0) continue;
    if (name === ORCA_MCP_SERVER_NAME) continue;
    if (!toolsetEnabled(tool)) continue;
    out.add(name);
  }
  return out;
}

/** Compose explicit toolset defaults and enabled/disabled overrides for either execution path. */
export function toolsetPermissionPolicies(
  tools: AgentToolEntry[],
  serverName: string,
  isTargetToolset: (tool: AgentToolEntry) => boolean,
): Record<string, 'always_allow' | 'always_ask' | 'always_deny'> {
  const policies: Record<string, 'always_allow' | 'always_ask' | 'always_deny'> = {};
  for (const tool of tools) {
    if (!isTargetToolset(tool)) continue;
    const defaultValue = toolsetConfigValue(tool['default_config']);
    const defaultConfig = resolvedToolsetConfig(defaultValue);
    const explicitDefaultPolicy = managedToolPermissionPolicyName(
      defaultValue['permission_policy'],
    );
    if (!defaultConfig.enabled) {
      policies[`mcp__${serverName}__*`] ??= 'always_deny';
    } else if (explicitDefaultPolicy) {
      policies[`mcp__${serverName}__*`] ??= explicitDefaultPolicy;
    }
    for (const config of toolsetConfigEntries(tool['configs'])) {
      const resolved = resolvedToolsetConfig(config.value, defaultConfig);
      const explicitPolicy = managedToolPermissionPolicyName(config.value['permission_policy']);
      const explicitlyEnabled = config.value['enabled'] === true;
      const policyName = !resolved.enabled
        ? 'always_deny'
        : (explicitPolicy ??
          (!defaultConfig.enabled && explicitlyEnabled ? resolved.permissionPolicy : null));
      if (!policyName) continue;
      const toolName = mcpPolicyToolName(serverName, config.name);
      if (toolName) policies[toolName] = policyName;
    }
  }
  return policies;
}

function mcpPolicyToolName(serverName: string, configuredName: string): string | null {
  if (configuredName.length === 0) return null;
  if (configuredName.startsWith('mcp__')) {
    const expectedPrefix = `mcp__${serverName}__`;
    return configuredName.startsWith(expectedPrefix) ? configuredName : null;
  }
  return `mcp__${serverName}__${configuredName}`;
}
