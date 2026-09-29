// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

// The provider-dispatch seam — construct the right {@link AgentHarness} for a
// snapshot, keyed on `snapshot.provider`.
//
// The self-hosted launch frame omits the provider by design (the worker spawns a
// generic runner shell), so the SNAPSHOT carries the harness/provider selection.
// This module turns that selection into a constructed harness: a registry of
// {@link ProviderFactory}s keyed by provider name. The built-in providers are
// registered in `main.ts` (`defaultRegisterProviders`), not here, so the core loop
// stays testable without the SDKs. The seam is the extension point (register a
// factory under a new key); the loop itself never branches on provider.
//
// The factory receives the parsed {@link RunnerSnapshot} (so a provider can read
// the egress base URL / JWT it needs) plus the (workspace, session) ids, and
// returns an {@link AgentHarness} the loop will `start` + drive. Separating the
// SHAPE mapping ({@link buildSessionStartInput}) from construction keeps the
// snapshot→SDK projection in one place and identical to the harness-server's.

import type {
  AgentHarness,
  SessionStartInput,
  ToolPermissionPolicy,
  ToolPermissionResolver,
} from './agent-harness.js';
import type { SandboxHandle, SandboxRuntime } from '../sandbox/seam.js';
import type { RunnerSnapshot } from '../snapshot.js';

/** Identity of the session a provider is built for. */
export interface ProviderSessionContext {
  /** Owning workspace (tenant scope). */
  workspaceId: string;
  /** Orca session id the harness serves. */
  sessionId: string;
  /**
   * The per-session {@link SandboxHandle} the runner ACQUIRED for this session
   * (from the runner's {@link SandboxRuntime}), or `undefined` when the runner
   * wired no runtime / did not acquire one (e.g. the LLM-free `mock` provider). The
   * in-process claude providers bind their `orca` MCP tool server + anchor the SDK
   * `cwd` at this handle's root, so the model's bash/read/write/edit/glob/grep — and
   * the SDK's own built-ins — execute INSIDE the sandbox rather than on the runner
   * host. Optional so the surface stays additive: a provider with no handle stays
   * LLM-only (its prior behavior), and a caller that builds a context without one
   * keeps type-checking unchanged. The loop owns the handle's lifecycle (acquire on
   * snapshot-apply, destroy on teardown) — a provider never destroys it.
   */
  sandbox?: SandboxHandle;
  toolSandbox?: SandboxHandle;
  runToolWithResources?: SessionStartInput['runToolWithResources'];
  /**
   * The runner's per-process {@link SandboxRuntime} (from `@orca/sandbox-runtime`),
   * or `undefined` when the runner wired none. A provider that launches a real
   * child process — the native-CLI providers (Codex / Claude Code / …) — calls
   * `sandboxRuntime.acquire(env)` and then `SandboxHandle.spawn(cmd)` (the streaming
   * spawn primitive) to run + attach the CLI; the in-process claude/mock providers
   * ignore it (they run the model in-process, never a sandboxed child). Optional so
   * the surface stays additive: an in-process provider and any caller that builds a
   * context without it type-check and behave unchanged. A provider that needs a
   * sandbox but finds none must fail-fast rather than silently run unsandboxed.
   */
  sandboxRuntime?: SandboxRuntime;
}

/**
 * Builds an {@link AgentHarness} for one session from its snapshot. Registered
 * under a provider name (e.g. `"claude"`). A factory may throw — the snapshot
 * handler maps a construction failure to a non-2xx ack so the owner pod retries
 * on the next reconnect.
 */
export type ProviderFactory = (
  snapshot: RunnerSnapshot,
  ctx: ProviderSessionContext,
) => AgentHarness;

/** Thrown when no provider is registered for a snapshot's provider name. */
export class UnknownProviderError extends Error {
  constructor(readonly provider: string) {
    super(`no harness provider registered for '${provider}'`);
    this.name = 'UnknownProviderError';
  }
}

/**
 * A provider-name → {@link ProviderFactory} registry. `main.ts` registers the
 * built-in providers, one key each. The loop resolves a factory by
 * `snapshot.provider` at snapshot-apply time.
 */
export class ProviderRegistry {
  private readonly managedProviders = new Set<string>();
  private readonly customToolProviders = new Set<string>();

  supportsManagedResources(provider: string): boolean {
    return this.managedProviders.has(provider);
  }

  private readonly factories = new Map<string, ProviderFactory>();

  /**
   * Register a factory for a provider name.
   *
   * @throws Error if a factory is already registered for the same name — a
   *   double-registration is a wiring bug, not a runtime condition.
   */
  register(
    provider: string,
    factory: ProviderFactory,
    capabilities: { managedResources?: boolean; customTools?: boolean } = {},
  ): this {
    if (this.factories.has(provider)) {
      throw new Error(`a harness provider is already registered for '${provider}'`);
    }
    this.factories.set(provider, factory);
    if (capabilities.managedResources) this.managedProviders.add(provider);
    if (capabilities.customTools) this.customToolProviders.add(provider);
    return this;
  }

  /** Whether a factory is registered for `provider`. */
  has(provider: string): boolean {
    return this.factories.has(provider);
  }

  /** The provider names this runner can serve (for the tunnel hello advertise). */
  providerNames(): string[] {
    return [...this.factories.keys()];
  }

  /**
   * Construct the harness for `snapshot`, dispatching on `snapshot.provider`.
   *
   * @throws UnknownProviderError when no factory is registered for the snapshot's
   *   provider — capability mismatch (the registry validates the agent's provider
   *   against the runner's advertised set at dispatch; this is the runner-side
   *   fail-fast).
   */
  build(snapshot: RunnerSnapshot, ctx: ProviderSessionContext): AgentHarness {
    const factory = this.factories.get(snapshot.provider);
    if (factory === undefined) {
      throw new UnknownProviderError(snapshot.provider);
    }
    if (snapshot.custom_tools?.length && !this.customToolProviders.has(snapshot.provider))
      throw new Error(`harness provider '${snapshot.provider}' does not support custom tools`);
    return factory(snapshot, ctx);
  }
}

/**
 * Project a {@link RunnerSnapshot} + session identity into the
 * {@link SessionStartInput} a provider's `start` consumes.
 *
 * Mirrors the harness-server's snapshot→`SessionStartInput` mapping so a provider
 * authored against either side reads the same fields:
 *   - the model / system / tool-allowlist ride `agentSnapshot`;
 *   - the enabled `mcp_toolset` server names become `remoteMcpToolsets` (a provider
 *     grants `mcp__<server>__*` for each, while the SDK only learns the concrete
 *     upstream tool names after connecting);
 *   - the rewritten gateway MCP server map (each entry already pointing at the
 *     ai-gateway, carrying the scoped session JWT + routing headers) becomes
 *     `mcpServers`.
 *
 * The rewritten map is carried inside the snapshot's `egress` block (only the
 * `gateway` mode has one; the registry composed it credential-free); this projection
 * lifts it onto the typed `SessionStartInput.mcpServers` field so the generic
 * mapping is complete — every provider reads the rewritten servers uniformly from
 * `SessionStartInput`, the same way it reads `remoteMcpToolsets`, rather than each
 * provider re-reading the opaque egress to recover them. A provider may still read
 * `snapshot.egress` directly for provider-specific concerns the typed shape does not
 * carry (e.g. the claude provider reads the LLM base URL / JWT for its SDK env).
 */
export function buildSessionStartInput(
  snapshot: RunnerSnapshot,
  ctx: ProviderSessionContext,
): SessionStartInput {
  const input: SessionStartInput = {
    workspaceId: ctx.workspaceId,
    sessionId: ctx.sessionId,
    agentSnapshot: {
      model_provider: snapshot.model.provider,
      model_id: snapshot.model.id,
      ...(snapshot.model.effort ? { model_effort: snapshot.model.effort } : {}),
      system: snapshot.system,
      ...(snapshot.custom_tools !== undefined ? { custom_tools: snapshot.custom_tools } : {}),
      allowed_tool_names: snapshot.allowed_tool_names,
      // The skills plugin-dir rides the boot context alongside the system prompt, so a
      // native-CLI provider surfaces it as `--plugin-dir`. Spread in only when the
      // snapshot carried one — under `exactOptionalPropertyTypes` an explicit
      // `undefined` is not assignable to the optional `skills_plugin_dir?` slot, and a
      // snapshot with no bundled skills leaves it absent (the provider then gets no
      // plugin dir).
      ...(snapshot.skills_plugin_dir !== undefined
        ? { skills_plugin_dir: snapshot.skills_plugin_dir }
        : {}),
    },
  };
  // Carry the per-session sandbox the runner acquired onto the boot context so a
  // provider binds its `orca` MCP tool server + anchors the SDK `cwd` to it. Spread
  // in only when present — under `exactOptionalPropertyTypes` an explicit `undefined`
  // is not assignable to the optional `sandbox?` slot, and a provider with no sandbox
  // stays LLM-only.
  if (ctx.sandbox !== undefined) {
    input.sandbox = ctx.sandbox;
  }
  if (ctx.toolSandbox !== undefined) input.toolSandbox = ctx.toolSandbox;
  if (ctx.runToolWithResources !== undefined) input.runToolWithResources = ctx.runToolWithResources;
  const mcpServers = readGatewayMcpServers(snapshot);
  if (mcpServers !== undefined) {
    input.mcpServers = mcpServers;
  }
  if (snapshot.allowed_mcp_server_names.length > 0) {
    input.remoteMcpToolsets = snapshot.allowed_mcp_server_names.map((serverName) => ({
      serverName,
    }));
  }
  const toolPermissions = buildToolPermissionResolver(snapshot);
  if (toolPermissions !== undefined) {
    input.toolPermissions = toolPermissions;
  }
  return input;
}

/**
 * Build the {@link ToolPermissionResolver} from a snapshot's per-tool policy, or
 * `undefined` when the snapshot carries NO policy at all (no per-tool entries and no
 * default). Returning `undefined` is load-bearing: a provider with no resolver is
 * fail-closed (parks every gated tool call), so a snapshot that stamps no policy
 * keeps the human gate engaged for everything — the conservative default for a
 * self-hosted harness. When the snapshot DOES carry a policy, the resolver looks up
 * the per-tool entry first, then the snapshot default, then falls back to fail-closed
 * `always_ask` for a tool the snapshot did not classify and gave no default for.
 */
function buildToolPermissionResolver(snapshot: RunnerSnapshot): ToolPermissionResolver | undefined {
  const perTool = snapshot.tool_permissions;
  const defaultPolicy = snapshot.default_tool_permission;
  if (Object.keys(perTool).length === 0 && defaultPolicy === undefined) {
    return undefined;
  }
  return {
    policyFor(toolName: string): ToolPermissionPolicy {
      const wildcard = Object.keys(perTool)
        .filter(
          (key) =>
            key.startsWith('mcp__') && key.endsWith('__*') && toolName.startsWith(key.slice(0, -1)),
        )
        .sort((a, b) => b.length - a.length)[0];
      const serverPolicy = wildcard === undefined ? undefined : perTool[wildcard];
      return perTool[toolName] ?? serverPolicy ?? defaultPolicy ?? 'always_ask';
    },
  };
}

/** One rewritten gateway MCP server entry, in the shape `SessionStartInput.mcpServers` carries. */
type GatewayMcpServer = { type: 'http'; url: string; headers: Record<string, string> };

/**
 * Read the rewritten gateway MCP server map out of a snapshot's opaque `egress`
 * block. Only the `gateway` egress mode carries one (under `egress.gateway.mcp_servers`,
 * keyed by logical server name); a `sidecar` / malformed / absent egress, or a gateway
 * with no rewritten servers, yields `undefined` (the projection then omits `mcpServers`,
 * matching chat-only / no-MCP sessions). Defensive because the runner treats `egress`
 * as opaque and the registry owns its structural guarantees: each candidate entry is
 * validated to the `{ type: 'http', url, headers }` shape before it is admitted, so a
 * malformed entry never reaches a provider's SDK. Extra fields the registry stamps on a
 * rewritten entry (e.g. `alwaysLoad`) are preserved via the cast — only the load-bearing
 * fields are checked.
 */
function readGatewayMcpServers(
  snapshot: RunnerSnapshot,
): Record<string, GatewayMcpServer> | undefined {
  const egress = snapshot.egress;
  if (egress === null || typeof egress !== 'object') {
    return undefined;
  }
  const obj = egress as { mode?: unknown; gateway?: unknown };
  if (obj.mode !== 'gateway' || obj.gateway === null || typeof obj.gateway !== 'object') {
    return undefined;
  }
  const rawServers = (obj.gateway as { mcp_servers?: unknown }).mcp_servers;
  if (rawServers === null || typeof rawServers !== 'object' || Array.isArray(rawServers)) {
    return undefined;
  }
  const out: Record<string, GatewayMcpServer> = {};
  for (const [name, value] of Object.entries(rawServers as Record<string, unknown>)) {
    if (isGatewayMcpServer(value)) {
      out[name] = value;
    }
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/** Whether `value` is a well-formed rewritten gateway MCP server entry. */
function isGatewayMcpServer(value: unknown): value is GatewayMcpServer {
  if (value === null || typeof value !== 'object') {
    return false;
  }
  const obj = value as { type?: unknown; url?: unknown; headers?: unknown };
  return (
    obj.type === 'http' &&
    typeof obj.url === 'string' &&
    obj.headers !== null &&
    typeof obj.headers === 'object' &&
    !Array.isArray(obj.headers)
  );
}
